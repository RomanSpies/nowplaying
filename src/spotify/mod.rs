pub mod cluster;
pub mod lyrics;
pub mod metadata;
pub mod pending;
pub mod reconnect;
pub mod session;
pub mod sink;

use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime};

use anyhow::{Context, anyhow};
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use librespot::connect::Spirc;
use librespot::core::dealer::manager::BoxedStreamResult;
use librespot::core::dealer::protocol::Message;
use librespot::core::error::ErrorKind;
use librespot::protocol::connect::ClusterUpdate;
use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Gauge, Meter, ObservableGauge};
use sqlx::PgPool;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{Instrument, debug, error, info, info_span, instrument, warn};

use crate::db;
use crate::events::{MediaKind, PlayEvent, PlaybackState};
use crate::spotify::cluster::{
    ClockSample, ClusterSnapshot, LiveStateTracker, NoSnapshot, PlayCause, PlaybackTracker,
    ServerClock, StatePatch, Suppressed,
};
use crate::spotify::metadata::{FetchTrack, MetadataResolver, TrackMeta, media_link};
use crate::spotify::pending::{DiscardReason, PendingPersist, PersistFn};
use crate::spotify::reconnect::ReconnectBudget;
use crate::spotify::session::MissingCredentials;
use crate::state::{AppState, cluster_update_age};
use crate::web::top_cache::TopCache;

const CLUSTER_URI: &str = "hm://connect-state/v1/cluster";
const MAX_RECONNECTS: usize = 5;
const RECONNECT_WINDOW: Duration = Duration::from_secs(600);
/// Reconnect replay guard: a play of the same track starting within this
/// window of the last persisted row is considered the same playback.
const REPLAY_GUARD_SECS: f64 = 10.0;
/// Insert attempts per qualified play; backoff doubles from
/// [`PERSIST_BACKOFF`] (1+2+4+8 s ≈ 15 s of Postgres outage tolerated).
const PERSIST_ATTEMPTS: u32 = 5;
const PERSIST_BACKOFF: Duration = Duration::from_secs(1);
/// How often a parked (credential-less) spotify task checks whether the
/// credentials file changed.
const CREDENTIALS_POLL: Duration = Duration::from_secs(60);
const REPAIR_INTERVAL: Duration = Duration::from_secs(3600);
const REPAIR_BATCH: i64 = 50;
/// Consecutive unavailable or timed-out fetches after which a repair run
/// gives up (circuit breaker: Spotify is having trouble, retry next
/// interval).
const REPAIR_MAX_CONSECUTIVE_FAILURES: usize = 3;

struct Metrics {
    plays: Counter<u64>,
    suppressed: Counter<u64>,
    persisted: Counter<u64>,
    discarded: Counter<u64>,
    replay_suppressed: Counter<u64>,
    state_updates: Counter<u64>,
    reconnects: Counter<u64>,
    session_invalid: Counter<u64>,
    db_errors: Counter<u64>,
    persist_failed: Counter<u64>,
    /// Kept alive so the observable callbacks stay registered.
    _connected: ObservableGauge<u64>,
    _auth_failed: ObservableGauge<u64>,
    _cluster_update_age: ObservableGauge<f64>,
}

impl Metrics {
    /// `meter` is injected so tests can observe the instruments through a
    /// private provider instead of the process-global one.
    ///
    /// `spotify_connected` alone cannot tell a self-healing reconnect from a
    /// parked credential failure; `spotify_auth_failed` is the alertable
    /// "needs `--login`" signal. `spotify_cluster_update_age` exposes a
    /// dealer that stays connected but stops delivering — long silence is
    /// also normal while nothing plays, so read it together with the
    /// playback state rather than alerting on it alone.
    fn new(meter: &Meter, state: &AppState) -> Self {
        let connected = state.spotify_connected.clone();
        let auth_failed = state.spotify_auth_failed.clone();
        let last_update = state.last_cluster_update_ms.clone();
        Self {
            plays: meter
                .u64_counter("plays_total")
                .with_description("New plays detected on the Connect cluster")
                .build(),
            suppressed: meter
                .u64_counter("play_events_suppressed_total")
                .with_description("Cluster updates that did not produce a play")
                .build(),
            persisted: meter
                .u64_counter("plays_persisted_total")
                .with_description("Plays written to Postgres after the min-play threshold")
                .build(),
            discarded: meter
                .u64_counter("plays_discarded_total")
                .with_description("Detected plays dropped before the min-play threshold")
                .build(),
            replay_suppressed: meter
                .u64_counter("plays_replay_suppressed_total")
                .with_description("Qualified plays swallowed by the DB replay guard")
                .build(),
            state_updates: meter
                .u64_counter("playback_state_updates_total")
                .with_description("Live playback state frames published to the WS")
                .build(),
            reconnects: meter
                .u64_counter("dealer_reconnects_total")
                .with_description("Spotify session/dealer reconnect attempts")
                .build(),
            session_invalid: meter
                .u64_counter("spotify_session_invalid_total")
                .with_description("Sessions torn down after being invalidated")
                .build(),
            db_errors: meter
                .u64_counter("db_errors_total")
                .with_description("Failed Postgres operations")
                .build(),
            persist_failed: meter
                .u64_counter("plays_persist_failed_total")
                .with_description("Qualified plays lost after every insert attempt failed")
                .build(),
            _auth_failed: meter
                .u64_observable_gauge("spotify_auth_failed")
                .with_description("1 while parked on missing or rejected Spotify credentials")
                .with_callback(move |o| {
                    o.observe(auth_failed.load(Ordering::Relaxed) as u64, &[]);
                })
                .build(),
            _cluster_update_age: meter
                .f64_observable_gauge("spotify_cluster_update_age")
                .with_unit("s")
                .with_description("Time since the last message on the dealer cluster stream")
                .with_callback(move |o| {
                    if let Some(age) = cluster_update_age(
                        last_update.load(Ordering::Relaxed),
                        Utc::now().timestamp_millis(),
                    ) {
                        o.observe(age.as_secs_f64(), &[]);
                    }
                })
                .build(),
            _connected: meter
                .u64_observable_gauge("spotify_connected")
                .with_description("1 while the Spotify session is up")
                .with_callback(move |o| {
                    o.observe(connected.load(Ordering::Relaxed) as u64, &[]);
                })
                .build(),
        }
    }
}

pub fn spawn(
    state: AppState,
    shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    tokio::spawn(run(state, shutdown))
}

/// Control over this service's own Connect device. Split out so
/// `handle_update` is testable without a live `Spirc`.
pub trait DeviceControl: Send + Sync {
    fn device_id(&self) -> &str;
    /// Give up the active-device role and pause: someone picked this silent
    /// device as the playback target.
    fn release(&self);
}

struct SpircDevice<'a> {
    spirc: &'a Spirc,
    device_id: String,
}

impl DeviceControl for SpircDevice<'_> {
    fn device_id(&self) -> &str {
        &self.device_id
    }

    fn release(&self) {
        if let Err(e) = self.spirc.disconnect(true) {
            warn!(error = %e, "releasing own device failed");
        }
    }
}

/// Why a connection attempt or lifetime ended with an error. `Auth` needs an
/// operator (`nowplaying --login`), so it is never retried blindly.
enum ConnectError {
    Auth(anyhow::Error),
    Transient(anyhow::Error),
}

impl ConnectError {
    fn kind(&self) -> &'static str {
        match self {
            Self::Auth(_) => "auth",
            Self::Transient(_) => "transient",
        }
    }
}

/// Aborts the wrapped task when dropped, tying its lifetime to a scope.
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Owns the per-process playback [`Pipeline`] across reconnects and discards
/// a still-unqualified play exactly once, when the task ends.
async fn run(state: AppState, mut shutdown: watch::Receiver<bool>) -> anyhow::Result<()> {
    let metrics = Metrics::new(&global::meter("nowplaying"), &state);
    let mut pipeline = Pipeline::default();
    let result = supervise(&state, &metrics, &mut pipeline, &mut shutdown).await;
    if let Some(p) = pipeline.pending.take() {
        p.discard(DiscardReason::Disconnected);
    }
    result
}

/// Supervision loop: (re)build the whole session/Spirc/dealer stack on any
/// transient failure, rate-limited by [`ReconnectBudget`]; exhausting it
/// exits the process and hands recovery to systemd. Credential problems do
/// not burn the budget — the task parks until the credentials file changes,
/// since restarting cannot fix them.
///
/// A pending play is suspended (not discarded) whenever a connection ends:
/// the reconnect's first update usually re-observes the same track and
/// resumes its listening countdown.
async fn supervise(
    state: &AppState,
    metrics: &Metrics,
    pipeline: &mut Pipeline,
    shutdown: &mut watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut budget = ReconnectBudget::new(MAX_RECONNECTS, RECONNECT_WINDOW);
    let mut attempt: u64 = 0;

    loop {
        attempt += 1;
        let started = Instant::now();
        let outcome = run_once(state, metrics, pipeline, shutdown, attempt).await;
        state.set_spotify_connected(false);
        if let Some(p) = pipeline.pending.as_mut() {
            p.suspend();
        }

        match outcome {
            Ok(()) => {
                info!("spotify task shutting down");
                return Ok(());
            }
            Err(ConnectError::Auth(e)) => match park_on_auth_failure(state, shutdown, &e).await {
                Parked::Shutdown => return Ok(()),
                Parked::CredentialsChanged => {
                    budget.reset();
                    attempt = 0;
                }
            },
            Err(ConnectError::Transient(e)) => {
                warn!(error = %format!("{e:#}"), "spotify connection failed");
                if *shutdown.borrow() {
                    return Ok(());
                }
                let backoff = budget.on_failure(Instant::now(), started.elapsed())?;
                metrics.reconnects.add(1, &[]);
                info!(
                    reconnect.backoff_ms = backoff.as_millis() as u64,
                    reconnect.budget_remaining = budget.remaining(),
                    "reconnecting to Spotify"
                );
                tokio::select! {
                    _ = tokio::time::sleep(backoff) => {}
                    _ = shutdown.changed() => return Ok(()),
                }
            }
        }
    }
}

enum Parked {
    Shutdown,
    CredentialsChanged,
}

fn modified(path: &std::path::Path) -> Option<SystemTime> {
    std::fs::metadata(path).and_then(|m| m.modified()).ok()
}

/// Wait out a credential failure: flag it for /healthz, then poll the
/// credentials file's mtime until an operator replaces it (or shutdown).
/// A half-written file simply fails the next attempt and parks again.
async fn park_on_auth_failure(
    state: &AppState,
    shutdown: &mut watch::Receiver<bool>,
    cause: &anyhow::Error,
) -> Parked {
    state.set_spotify_auth_failed(true);
    let path = session::credentials_path(&state.cfg);
    let span = info_span!("spotify.auth_parked", credentials.path = %path.display());
    async {
        error!(
            error = %format!("{cause:#}"),
            "Spotify credentials missing or rejected; parked until they change — run `nowplaying --login`"
        );
        let baseline = modified(&path);
        let mut poll = tokio::time::interval(CREDENTIALS_POLL);
        poll.tick().await;
        loop {
            if *shutdown.borrow() {
                return Parked::Shutdown;
            }
            tokio::select! {
                _ = shutdown.changed() => return Parked::Shutdown,
                _ = poll.tick() => {
                    if modified(&path) != baseline {
                        info!("credentials_changed: retrying Spotify login");
                        return Parked::CredentialsChanged;
                    }
                }
            }
        }
    }
    .instrument(span)
    .await
}

/// One connection lifetime. Returns Ok(()) only on requested shutdown; any
/// other exit is an error that triggers a reconnect (or parking, for
/// credential failures).
///
/// The setup order is load-bearing: the cluster subscription is registered
/// **before** `Spirc::new`, because librespot connects the session only once
/// all dealer listeners are in place (subscriptions to the same URI fan out —
/// each subscriber gets its own copy). Session build, subscription and Spirc
/// registration run under a single `spotify.connect` span so slow or failed
/// connects are visible as one unit. A login rejected by the access point
/// surfaces as `ErrorKind::PermissionDenied` from `Spirc::new`.
///
/// The metadata repair pass runs as a sibling task bound to this connection
/// (it needs the session) and never blocks cluster handling.
async fn run_once(
    state: &AppState,
    metrics: &Metrics,
    pipeline: &mut Pipeline,
    shutdown: &mut watch::Receiver<bool>,
    attempt: u64,
) -> Result<(), ConnectError> {
    let connect_span = info_span!(
        "spotify.connect",
        device_name = %state.cfg.device_name,
        reconnect.attempt = attempt,
        connect.error_kind = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
    );
    let connected = async {
        let bundle = session::build(&state.cfg).map_err(|e| {
            if e.is::<MissingCredentials>() {
                ConnectError::Auth(e)
            } else {
                ConnectError::Transient(e)
            }
        })?;
        let session = bundle.session.clone();

        let cluster_stream: BoxedStreamResult<ClusterUpdate> = session
            .dealer()
            .listen_for(CLUSTER_URI, Message::from_raw)
            .map_err(|e| ConnectError::Transient(anyhow!("subscribing to cluster updates: {e}")))?;

        let (spirc, spirc_task) = Spirc::new(
            bundle.connect_config,
            session.clone(),
            bundle.credentials,
            bundle.player,
            bundle.mixer,
        )
        .await
        .map_err(|e| {
            let rejected = e.kind == ErrorKind::PermissionDenied;
            let err = anyhow!("starting Spirc: {e}");
            if rejected {
                ConnectError::Auth(err)
            } else {
                ConnectError::Transient(err)
            }
        })?;
        Ok::<_, ConnectError>((session, cluster_stream, spirc, spirc_task))
    }
    .instrument(connect_span.clone())
    .await;
    let (session, mut cluster_stream, spirc, spirc_task) = match connected {
        Ok(parts) => parts,
        Err(e) => {
            connect_span.record("connect.error_kind", e.kind());
            connect_span.record("otel.status_code", "ERROR");
            return Err(e);
        }
    };
    let mut spirc_task = std::pin::pin!(spirc_task);

    if state.is_spotify_auth_failed() {
        info!("auth_recovered: Spotify accepted the credentials again");
        state.set_spotify_auth_failed(false);
    }
    state.set_spotify_connected(true);
    info!(device_name = %state.cfg.device_name, "connected as Spotify Connect device");

    let device = SpircDevice {
        spirc: &spirc,
        device_id: session.device_id().to_owned(),
    };
    let resolver = Arc::new(MetadataResolver::new(session.clone()));
    let persist = make_persist_fn(state, metrics);
    let _repair = AbortOnDrop(tokio::spawn(repair_loop(
        state.db.clone(),
        resolver.clone(),
    )));
    let mut invalid_check = tokio::time::interval(Duration::from_secs(5));

    loop {
        tokio::select! {
            _ = &mut spirc_task => {
                break Err(ConnectError::Transient(anyhow!("spirc task ended unexpectedly")));
            }
            _ = shutdown.changed() => {
                let _ = spirc.shutdown();
                let _ = tokio::time::timeout(Duration::from_secs(5), &mut spirc_task).await;
                break Ok(());
            }
            _ = invalid_check.tick() => {
                if session.is_invalid() {
                    metrics.session_invalid.add(1, &[]);
                    break Err(ConnectError::Transient(anyhow!("session invalidated")));
                }
            }
            item = cluster_stream.next() => {
                if item.is_some() {
                    state.mark_cluster_update();
                }
                match item {
                    None => break Err(ConnectError::Transient(anyhow!("cluster update stream ended"))),
                    Some(Err(e)) => warn!(error = %e, "undecodable cluster update"),
                    Some(Ok(update)) => {
                        handle_update(state, metrics, &resolver, pipeline, &persist, &device, update)
                            .await;
                    }
                }
            }
        }
    }
}

/// Playback pipeline state. Owned by the supervision loop and kept across
/// reconnects: after a reconnect the tracker recognises the still-running
/// track (no duplicate play, no duplicate frames), and a suspended pending
/// play resumes its countdown.
#[derive(Default)]
struct Pipeline {
    tracker: PlaybackTracker,
    live: LiveStateTracker,
    pending: Option<PendingPersist>,
    clock: ServerClock,
    /// Whether the last update had this service's own device active, so the
    /// device is released once per activation, not on every update.
    own_device_active: bool,
}

/// Single entry point for every dealer cluster update; scrobbling and
/// live-state replication both branch off here.
///
/// Time: the local clock is corrected onto Spotify's server clock
/// ([`ServerClock`]) before any extrapolation. Then, in order:
/// - no usable snapshot (no track, or an unsupported item such as an ad):
///   the pending play is *held* (paused, not discarded) and the last known
///   track is reported stopped;
/// - own device active or private session: nothing about the item is
///   looked at or published — the pending play is discarded, the tracker
///   forgets the playback, and the last *public* track is reported stopped.
///   Taking over the own (silent) device is additionally released once;
/// - otherwise the [`PlaybackTracker`] decides: a new play (or restart)
///   supersedes any not-yet-qualified pending persist and carries its
///   playback state inside the `play` frame, computed before the metadata
///   fetch so its latency cannot skew the position. Local files are
///   published but never persisted. Updates suppressed for scrobbling still
///   feed the [`LiveStateTracker`] and become `state` frames.
///
/// Tracing: one `cluster.handle_update` span per update; declared fields are
/// recorded as soon as they are known. `outcome` is one of new_play |
/// restart | no_track | unsupported_media | private_session | own_device |
/// not_playing | same_track | seek | metadata_failed; `update.reason` is the
/// bare enum variant name (not the protobuf `Result` wrapper), so span
/// filters match; failures set `otel.status_code = ERROR` so trace UIs
/// surface them.
#[instrument(
    name = "cluster.handle_update",
    skip_all,
    fields(
        update.reason = tracing::field::Empty,
        active_device = tracing::field::Empty,
        track.uri = tracing::field::Empty,
        track.title = tracing::field::Empty,
        media.kind = tracing::field::Empty,
        playing = tracing::field::Empty,
        paused = tracing::field::Empty,
        position_ms = tracing::field::Empty,
        private_session = tracing::field::Empty,
        own_device = tracing::field::Empty,
        clock.offset_ms = tracing::field::Empty,
        pending.resumed = tracing::field::Empty,
        outcome = tracing::field::Empty,
        state_published = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
    )
)]
async fn handle_update<F: FetchTrack>(
    state: &AppState,
    metrics: &Metrics,
    resolver: &MetadataResolver<F>,
    pipeline: &mut Pipeline,
    persist: &PersistFn,
    device: &dyn DeviceControl,
    update: ClusterUpdate,
) {
    let span = tracing::Span::current();
    let reason = update
        .update_reason
        .enum_value()
        .map(|r| format!("{r:?}"))
        .unwrap_or_else(|n| format!("UNKNOWN({n})"));
    span.record("update.reason", reason.as_str());

    let local_now_ms = Utc::now().timestamp_millis();
    if let ClockSample::Rejected(offset_ms) = pipeline
        .clock
        .observe(cluster::server_timestamp_ms(&update), local_now_ms)
    {
        warn!(offset_ms, "ignoring implausible server clock offset");
    }
    let now_ms = pipeline.clock.now_ms(local_now_ms);
    span.record("clock.offset_ms", pipeline.clock.offset_ms());

    let snapshot = match ClusterSnapshot::from_update(update) {
        Ok(snapshot) => snapshot,
        Err(no) => {
            let label = match &no {
                NoSnapshot::NoTrack => "no_track",
                NoSnapshot::Unsupported(kind) => {
                    span.record("media.kind", kind.as_str());
                    "unsupported_media"
                }
            };
            span.record("outcome", label);
            metrics.suppressed.add(1, &[KeyValue::new("reason", label)]);
            if let Some(p) = pipeline.pending.as_mut() {
                p.set_paused(true);
            }
            if let Some(patch) = pipeline.live.observe(None, now_ms) {
                publish_live(state, metrics, resolver, None, patch).await;
            }
            return;
        }
    };
    let own_device = snapshot.active_device_id == device.device_id();
    span.record("active_device", snapshot.active_device_id.as_str());
    span.record("own_device", own_device);
    span.record("private_session", snapshot.private_session);

    if own_device && !pipeline.own_device_active {
        warn!("playback was transferred to this silent device; releasing it");
        device.release();
    }
    pipeline.own_device_active = own_device;
    if own_device || snapshot.private_session {
        let (label, discard) = if snapshot.private_session {
            ("private_session", DiscardReason::Private)
        } else {
            ("own_device", DiscardReason::OwnDevice)
        };
        span.record("outcome", label);
        metrics.suppressed.add(1, &[KeyValue::new("reason", label)]);
        pipeline.tracker.clear();
        if let Some(p) = pipeline.pending.take() {
            p.discard(discard);
        }
        if let Some(patch) = pipeline.live.observe(None, now_ms) {
            publish_live(state, metrics, resolver, None, patch).await;
        }
        return;
    }

    span.record("track.uri", snapshot.track_uri.as_str());
    span.record("media.kind", snapshot.kind.as_str());
    span.record("playing", snapshot.is_playing);
    span.record("paused", snapshot.is_paused);
    span.record("position_ms", snapshot.position_ms);

    match pipeline.tracker.observe(&snapshot, now_ms) {
        Ok(new_play) => {
            metrics.plays.add(1, &[]);
            if let Some(p) = pipeline.pending.take() {
                p.discard(DiscardReason::Superseded);
            }
            let playback = snapshot.playback(now_ms);
            let _ = pipeline.live.observe(Some(&snapshot), now_ms);
            let built = async {
                let meta = resolver
                    .resolve(
                        &new_play.track_uri,
                        new_play.kind,
                        &new_play.metadata,
                        new_play.duration_ms,
                    )
                    .await?;
                play_event(
                    &new_play.track_uri,
                    new_play.kind,
                    meta,
                    new_play.started_at_ms,
                    snapshot.duration_ms,
                )
            }
            .await;
            match built {
                Ok(event) => {
                    span.record(
                        "outcome",
                        match new_play.cause {
                            PlayCause::NewTrack => "new_play",
                            PlayCause::Restart => "restart",
                        },
                    );
                    span.record("track.title", event.title.as_str());
                    info!(
                        title = %event.title,
                        artists = ?event.artists,
                        kind = event.kind.as_str(),
                        device = %snapshot.active_device_id,
                        "new play"
                    );
                    let persisted = event.kind.is_persisted();
                    state.publish_play(event.clone(), playback).await;
                    if persisted {
                        pipeline.pending = Some(PendingPersist::new(
                            event,
                            snapshot.is_paused,
                            Duration::from_millis(state.cfg.min_play_ms),
                            persist.clone(),
                            metrics.discarded.clone(),
                        ));
                    }
                }
                Err(e) => {
                    span.record("outcome", "metadata_failed");
                    span.record("otel.status_code", "ERROR");
                    error!(error = %format!("{e:#}"), "dropping play, metadata unresolvable")
                }
            }
        }
        Err(reason) => {
            let label = match reason {
                Suppressed::NotPlaying => {
                    if let Some(p) = pipeline.pending.take() {
                        p.discard(DiscardReason::Stopped);
                    }
                    "not_playing"
                }
                Suppressed::SameTrack | Suppressed::Seek => {
                    if let Some(p) = pipeline.pending.as_mut()
                        && p.set_paused(snapshot.is_paused)
                    {
                        span.record("pending.resumed", true);
                    }
                    if reason == Suppressed::Seek {
                        "seek"
                    } else {
                        "same_track"
                    }
                }
            };
            span.record("outcome", label);
            metrics.suppressed.add(1, &[KeyValue::new("reason", label)]);
            if let Some(patch) = pipeline.live.observe(Some(&snapshot), now_ms) {
                publish_live(state, metrics, resolver, Some(&snapshot), patch).await;
            }
        }
    }
}

/// Broadcast a live-state patch, upholding the invariant that clients only
/// ever get `state` frames for the track they hold metadata for (the cached
/// play). A patch for any other track — e.g. startup into a stopped or
/// paused track, which never becomes a play — first introduces the track
/// with a `now_playing` frame built from `snapshot`; without a matching
/// snapshot or resolvable metadata the patch is dropped. Records metric,
/// correlated log line and the `state_published` span field only for
/// frames actually sent.
async fn publish_live<F: FetchTrack>(
    state: &AppState,
    metrics: &Metrics,
    resolver: &MetadataResolver<F>,
    snapshot: Option<&ClusterSnapshot>,
    patch: StatePatch,
) {
    let track_id = match media_link(&patch.track_uri) {
        Ok((id, _)) => id,
        Err(e) => {
            warn!(error = %format!("{e:#}"), "dropping state update, unparsable track uri");
            return;
        }
    };
    let label = match patch.playback.state {
        PlaybackState::Playing => "playing",
        PlaybackState::Paused => "paused",
        PlaybackState::Stopped => "stopped",
    };

    let published = if state.publish_state(&track_id, patch.playback).await {
        label
    } else {
        let Some(snap) = snapshot.filter(|s| s.track_uri == patch.track_uri) else {
            debug!(track_id = %track_id, "dropping state frame for a track without metadata");
            return;
        };
        let event = async {
            let meta = resolver
                .resolve(&snap.track_uri, snap.kind, &snap.metadata, snap.duration_ms)
                .await?;
            play_event(
                &snap.track_uri,
                snap.kind,
                meta,
                snap.started_at_ms(),
                snap.duration_ms,
            )
        }
        .await;
        match event {
            Ok(event) => {
                state.publish_now_playing(event, patch.playback).await;
                "now_playing"
            }
            Err(e) => {
                debug!(
                    track_id = %track_id,
                    error = %format!("{e:#}"),
                    "dropping state frame, metadata unresolvable"
                );
                return;
            }
        }
    };

    tracing::Span::current().record("state_published", published);
    metrics
        .state_updates
        .add(1, &[KeyValue::new("state", label)]);
    info!(
        track_id = %track_id,
        state = label,
        frame = published,
        position_ms = patch.playback.position_ms,
        "playback state update"
    );
}

fn play_event(
    track_uri: &str,
    kind: MediaKind,
    meta: TrackMeta,
    started_at_ms: i64,
    duration_hint_ms: i64,
) -> anyhow::Result<PlayEvent> {
    let (track_id, track_url) = media_link(track_uri)?;
    let started_at =
        DateTime::<Utc>::from_timestamp_millis(started_at_ms).context("started_at out of range")?;
    Ok(PlayEvent {
        track_id,
        kind,
        track_url,
        title: meta.title,
        artists: meta.artists,
        album: meta.album,
        cover_url: meta.cover_url,
        duration_ms: if meta.duration_ms > 0 {
            meta.duration_ms
        } else {
            duration_hint_ms.max(0) as u32
        },
        started_at,
        lyrics: meta.lyrics,
        metadata_source: meta.source,
    })
}

/// Result of [`persist_with_retry`], recorded as the `play.persist` span's
/// `outcome`.
#[derive(Debug)]
enum PersistOutcome<E> {
    Persisted,
    /// The replay guard found the play already stored.
    ReplaySuppressed,
    Failed(E),
}

impl<E> PersistOutcome<E> {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Persisted => "persisted",
            Self::ReplaySuppressed => "replay_suppressed",
            Self::Failed(_) => "failed",
        }
    }
}

/// Run `insert` up to `attempts` times with doubling backoff from `base`.
/// Safe to repeat because the insert is idempotent and replay-guarded — a
/// retry after a commit whose acknowledgement was lost reports
/// `ReplaySuppressed`, not a duplicate. `on_error` sees every failure with
/// the backoff before the next attempt (`None` for the final one). Returns
/// the outcome and the number of attempts made.
async fn persist_with_retry<E, Op, Fut>(
    attempts: u32,
    base: Duration,
    mut insert: Op,
    mut on_error: impl FnMut(u32, &E, Option<Duration>),
) -> (PersistOutcome<E>, u32)
where
    Op: FnMut() -> Fut,
    Fut: Future<Output = Result<bool, E>>,
{
    let mut attempt = 1;
    loop {
        match insert().await {
            Ok(true) => return (PersistOutcome::Persisted, attempt),
            Ok(false) => return (PersistOutcome::ReplaySuppressed, attempt),
            Err(e) if attempt >= attempts => {
                on_error(attempt, &e, None);
                return (PersistOutcome::Failed(e), attempt);
            }
            Err(e) => {
                let backoff = base * 2u32.pow(attempt - 1);
                on_error(attempt, &e, Some(backoff));
                tokio::time::sleep(backoff).await;
                attempt += 1;
            }
        }
    }
}

/// Everything that happens after the insert attempts of one play: metrics,
/// cache invalidation, span fields and — for a lost play — an error log
/// carrying the full play so it can be restored by hand.
#[derive(Clone)]
struct PersistSink {
    persisted: Counter<u64>,
    replay_suppressed: Counter<u64>,
    persist_failed: Counter<u64>,
    top_cache: Arc<TopCache>,
}

impl PersistSink {
    fn new(state: &AppState, metrics: &Metrics) -> Self {
        Self {
            persisted: metrics.persisted.clone(),
            replay_suppressed: metrics.replay_suppressed.clone(),
            persist_failed: metrics.persist_failed.clone(),
            top_cache: state.top_cache.clone(),
        }
    }

    fn settle(&self, event: &PlayEvent, outcome: PersistOutcome<sqlx::Error>, attempts: u32) {
        let span = tracing::Span::current();
        span.record("persist.attempts", attempts);
        span.record("outcome", outcome.as_str());
        match outcome {
            PersistOutcome::Persisted => {
                self.persisted.add(1, &[]);
                self.top_cache.invalidate();
            }
            PersistOutcome::ReplaySuppressed => {
                self.replay_suppressed.add(1, &[]);
                info!(track_id = %event.track_id, "duplicate or replayed play suppressed");
            }
            PersistOutcome::Failed(e) => {
                self.persist_failed.add(1, &[]);
                span.record("otel.status_code", "ERROR");
                error!(
                    track_id = %event.track_id,
                    kind = event.kind.as_str(),
                    title = %event.title,
                    artists = ?event.artists,
                    album = %event.album,
                    cover_url = ?event.cover_url,
                    duration_ms = event.duration_ms,
                    started_at = %event.started_at,
                    metadata_source = event.metadata_source.as_str(),
                    attempts,
                    error = %e,
                    "persisting play failed permanently; play lost"
                );
            }
        }
    }
}

/// Build the action `PendingPersist` runs once a play has accumulated enough
/// listening: a guarded, idempotent insert (see `db::insert_play_guarded`)
/// with bounded retries, so a short Postgres outage does not lose plays.
///
/// `ReplaySuppressed` means the DB replay guard swallowed a reconnect
/// duplicate — counted in `plays_replay_suppressed_total`; a rising rate
/// there means reconnects re-detect running playbacks more often than
/// expected. `db_errors_total` counts every failed attempt, while
/// `plays_persist_failed_total` counts only plays actually lost. The future
/// runs instrumented with the `play.persist` span: every failed attempt is a
/// span event, the final outcome is settled by [`PersistSink`].
fn make_persist_fn(state: &AppState, metrics: &Metrics) -> PersistFn {
    let pool = state.db.clone();
    let sink = PersistSink::new(state, metrics);
    let db_errors = metrics.db_errors.clone();
    Arc::new(move |event: PlayEvent| {
        let pool = pool.clone();
        let sink = sink.clone();
        let db_errors = db_errors.clone();
        Box::pin(async move {
            let (outcome, attempts) = persist_with_retry(
                PERSIST_ATTEMPTS,
                PERSIST_BACKOFF,
                || db::insert_play_guarded(&pool, &event, REPLAY_GUARD_SECS),
                |attempt, e: &sqlx::Error, backoff| {
                    db_errors.add(1, &[KeyValue::new("op", "insert_play")]);
                    if let Some(backoff) = backoff {
                        warn!(
                            attempt,
                            backoff_ms = backoff.as_millis() as u64,
                            error = %e,
                            "persisting play failed; retrying"
                        );
                    }
                },
            )
            .await;
            sink.settle(&event, outcome, attempts);
        })
    })
}

/// Repairs degraded rows now (the interval's first tick is immediate) and
/// then every [`REPAIR_INTERVAL`], for as long as the connection lives.
async fn repair_loop<F: FetchTrack>(pool: PgPool, resolver: Arc<MetadataResolver<F>>) {
    let mut interval = tokio::time::interval(REPAIR_INTERVAL);
    loop {
        interval.tick().await;
        repair_degraded(&pool, &resolver).await;
    }
}

/// Degraded-row backlog, refreshed after every repair pass. Shows whether
/// the backlog actually shrinks; `unresolvable` rows are retired for good
/// and only ever grow.
fn degraded_gauge() -> &'static Gauge<u64> {
    static GAUGE: OnceLock<Gauge<u64>> = OnceLock::new();
    GAUGE.get_or_init(|| {
        global::meter("nowplaying")
            .u64_gauge("plays_degraded")
            .with_description("Persisted plays without full metadata, by metadata_source")
            .build()
    })
}

/// Per-run tally of [`repair_degraded`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RepairSummary {
    pub checked: usize,
    pub repaired: usize,
    pub unresolvable: usize,
    pub failed: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RepairOutcome {
    Repaired,
    Unresolvable,
    Unavailable,
    DbError,
}

impl RepairOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Repaired => "repaired",
            Self::Unresolvable => "unresolvable",
            Self::Unavailable => "unavailable",
            Self::DbError => "db_error",
        }
    }
}

/// One repair pass over up to [`REPAIR_BATCH`] degraded tracks: re-fetch the
/// full record and overwrite the rows. A track stored as `track` that
/// Spotify does not know as one is retried as an episode (rows from before
/// media kinds were modelled); unknown under both kinds, it is retired as
/// `unresolvable`. Gives up for this run after
/// [`REPAIR_MAX_CONSECUTIVE_FAILURES`] unavailable fetches in a row.
#[instrument(
    name = "metadata.repair",
    skip_all,
    fields(
        tracks.checked = tracing::field::Empty,
        tracks.repaired = tracing::field::Empty,
        tracks.failed = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
    )
)]
pub async fn repair_degraded<F: FetchTrack>(
    pool: &PgPool,
    resolver: &MetadataResolver<F>,
) -> RepairSummary {
    let span = tracing::Span::current();
    let mut summary = RepairSummary::default();
    let tracks = match db::degraded_tracks(pool, REPAIR_BATCH).await {
        Ok(tracks) => tracks,
        Err(e) => {
            span.record("otel.status_code", "ERROR");
            warn!(error = %e, "listing degraded tracks failed");
            return summary;
        }
    };
    let mut consecutive_unavailable = 0;
    for track in &tracks {
        summary.checked += 1;
        match repair_track(pool, resolver, track).await {
            RepairOutcome::Repaired => {
                summary.repaired += 1;
                consecutive_unavailable = 0;
            }
            RepairOutcome::Unresolvable => {
                summary.unresolvable += 1;
                consecutive_unavailable = 0;
            }
            RepairOutcome::DbError => summary.failed += 1,
            RepairOutcome::Unavailable => {
                summary.failed += 1;
                consecutive_unavailable += 1;
                if consecutive_unavailable >= REPAIR_MAX_CONSECUTIVE_FAILURES {
                    warn!(
                        consecutive_unavailable,
                        "metadata repair paused: Spotify unavailable, retrying next interval"
                    );
                    break;
                }
            }
        }
    }
    span.record("tracks.checked", summary.checked);
    span.record("tracks.repaired", summary.repaired);
    span.record("tracks.failed", summary.failed);
    match db::degraded_counts(pool).await {
        Ok(counts) => {
            for (source, rows) in counts {
                degraded_gauge().record(
                    rows.max(0) as u64,
                    &[KeyValue::new("metadata_source", source)],
                );
            }
        }
        Err(e) => warn!(error = %e, "counting degraded rows failed"),
    }
    if summary.checked > 0 {
        info!(
            checked = summary.checked,
            repaired = summary.repaired,
            unresolvable = summary.unresolvable,
            failed = summary.failed,
            "metadata repair pass finished"
        );
    }
    summary
}

#[instrument(
    name = "metadata.repair_track",
    skip(pool, resolver, track),
    fields(
        track_id = %track.track_id,
        media.kind = track.kind.as_str(),
        fetch.timeout = tracing::field::Empty,
        outcome = tracing::field::Empty,
    )
)]
async fn repair_track<F: FetchTrack>(
    pool: &PgPool,
    resolver: &MetadataResolver<F>,
    track: &db::DegradedTrack,
) -> RepairOutcome {
    let fetch_as = |kind: MediaKind| async move {
        let uri = format!("spotify:{}:{}", kind.as_str(), track.track_id);
        resolver
            .fetch_fresh(&uri, kind)
            .await
            .map(|meta| (kind, meta))
    };
    let mut fetched = fetch_as(track.kind).await;
    if track.kind == MediaKind::Track && fetched.as_ref().is_err_and(|e| e.is_not_found()) {
        fetched = fetch_as(MediaKind::Episode).await;
    }
    let outcome = match fetched {
        Ok((kind, meta)) => match db::repair_metadata(pool, &track.track_id, kind, &meta).await {
            Ok(_) => RepairOutcome::Repaired,
            Err(e) => {
                warn!(error = %e, "writing repaired metadata failed");
                RepairOutcome::DbError
            }
        },
        Err(e) if e.is_not_found() => match db::mark_unresolvable(pool, &track.track_id).await {
            Ok(_) => RepairOutcome::Unresolvable,
            Err(e) => {
                warn!(error = %e, "retiring unresolvable track failed");
                RepairOutcome::DbError
            }
        },
        Err(e) => {
            debug!(error = %e, "repair fetch failed");
            RepairOutcome::Unavailable
        }
    };
    tracing::Span::current().record("outcome", outcome.as_str());
    outcome
}

/// Integration tests for `handle_update`: the glue between tracker, metadata
/// resolution, broadcast and deferred persistence. The tracker extrapolates
/// against `Utc::now()` (real wall clock, keeps running under the paused test
/// clock); only `PendingPersist`'s timer runs on tokio time, so the 30s
/// threshold is driven with `tokio::time::advance`.
#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::AtomicUsize;

    use clap::Parser;
    use librespot::protocol::connect::Cluster;
    use librespot::protocol::player::{PlayerState, ProvidedTrack};

    use super::*;
    use crate::config::Config;
    use crate::spotify::metadata::test_support::{
        ScriptedFetcher, full_meta, full_meta_with_lyrics,
    };

    const TRACK_A: &str = "spotify:track:4uLU6hMCjMI75M1A2tKUQC";
    const ID_A: &str = "4uLU6hMCjMI75M1A2tKUQC";
    const TRACK_B: &str = "spotify:track:3dxiWIBVJRlqh9xk144rf4";
    const ID_B: &str = "3dxiWIBVJRlqh9xk144rf4";

    /// Every env-backed field is pinned via CLI flag (CLI beats env in
    /// clap), so exported NP_* vars cannot leak into the tests.
    fn test_state() -> AppState {
        let cfg = Config::parse_from([
            "nowplaying",
            "--bind-addr",
            "127.0.0.1:0",
            "--device-name",
            "test",
            "--cache-dir",
            "/tmp/unused",
            "--otlp-endpoint",
            "http://127.0.0.1:1",
            "--service-name",
            "test",
            "--allowed-origins",
            "https://rospies.dev",
            "--min-play-ms",
            "30000",
            "--top-default-limit",
            "10",
            "--ws-max-connections",
            "1000",
            "--ws-max-per-ip",
            "8",
            "--log-filter",
            "info",
        ]);
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused@127.0.0.1:1/unused")
            .expect("lazy pool");
        AppState::new(cfg, pool)
    }

    fn update(uri: &str, position_ms: i64, playing: bool, paused: bool) -> ClusterUpdate {
        let mut track = ProvidedTrack {
            uri: uri.to_string(),
            ..Default::default()
        };
        track
            .metadata
            .insert("title".to_string(), "Map Title".to_string());
        let mut ps = PlayerState {
            timestamp: Utc::now().timestamp_millis(),
            position_as_of_timestamp: position_ms,
            duration: 200_000,
            is_playing: playing,
            is_paused: paused,
            ..Default::default()
        };
        ps.track.0 = Some(Box::new(track));
        let mut cluster = Cluster {
            active_device_id: "test-device".to_string(),
            ..Default::default()
        };
        cluster.player_state.0 = Some(Box::new(ps));
        let mut u = ClusterUpdate::default();
        u.cluster.0 = Some(Box::new(cluster));
        u
    }

    const OWN_DEVICE: &str = "widget-device";

    #[derive(Default)]
    struct FakeDevice {
        releases: AtomicUsize,
    }

    impl DeviceControl for FakeDevice {
        fn device_id(&self) -> &str {
            OWN_DEVICE
        }

        fn release(&self) {
            self.releases.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Everything handle_update needs, plus a persist recorder instead of a DB.
    struct Harness {
        state: AppState,
        metrics: Metrics,
        pipeline: Pipeline,
        persist: PersistFn,
        device: FakeDevice,
        seen: Arc<StdMutex<Vec<String>>>,
    }

    impl Harness {
        fn new() -> Self {
            let state = test_state();
            let metrics = Metrics::new(&global::meter("nowplaying"), &state);
            let seen = Arc::new(StdMutex::new(Vec::new()));
            let sink = seen.clone();
            let persist: PersistFn = Arc::new(move |event: PlayEvent| {
                let sink = sink.clone();
                Box::pin(async move {
                    sink.lock().unwrap().push(event.track_id);
                })
            });
            Self {
                state,
                metrics,
                pipeline: Pipeline::default(),
                persist,
                device: FakeDevice::default(),
                seen,
            }
        }

        async fn feed<F: FetchTrack>(&mut self, resolver: &MetadataResolver<F>, u: ClusterUpdate) {
            handle_update(
                &self.state,
                &self.metrics,
                resolver,
                &mut self.pipeline,
                &self.persist,
                &self.device,
                u,
            )
            .await;
        }

        fn persisted(&self) -> Vec<String> {
            self.seen.lock().unwrap().clone()
        }
    }

    async fn settle() {
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn new_play_broadcasts_immediately_and_persists_after_threshold() {
        let mut h = Harness::new();
        let resolver =
            MetadataResolver::with_fetcher(ScriptedFetcher::new(vec![Some(full_meta("Song A"))]));
        let mut rx = h.state.tx.subscribe();

        h.feed(&resolver, update(TRACK_A, 0, true, false)).await;

        let frame = rx.try_recv().expect("play must broadcast immediately");
        assert_eq!(frame.key.track_id, ID_A);
        assert!(h.pipeline.pending.is_some());
        assert!(h.persisted().is_empty(), "persist must be deferred");

        tokio::time::advance(Duration::from_millis(30_001)).await;
        settle().await;
        assert_eq!(h.persisted(), [ID_A.to_string()]);
    }

    #[tokio::test(start_paused = true)]
    async fn skip_supersedes_the_pending_play() {
        let mut h = Harness::new();
        let resolver = MetadataResolver::with_fetcher(ScriptedFetcher::new(vec![
            Some(full_meta("Song A")),
            Some(full_meta("Song B")),
        ]));
        let mut rx = h.state.tx.subscribe();

        h.feed(&resolver, update(TRACK_A, 0, true, false)).await;
        tokio::time::advance(Duration::from_millis(5_000)).await;
        h.feed(&resolver, update(TRACK_B, 0, true, false)).await;

        assert_eq!(rx.try_recv().unwrap().key.track_id, ID_A);
        assert_eq!(rx.try_recv().unwrap().key.track_id, ID_B);

        tokio::time::advance(Duration::from_millis(31_000)).await;
        settle().await;
        assert_eq!(h.persisted(), [ID_B.to_string()]);
    }

    #[tokio::test(start_paused = true)]
    async fn stop_discards_the_pending_play() {
        let mut h = Harness::new();
        let resolver =
            MetadataResolver::with_fetcher(ScriptedFetcher::new(vec![Some(full_meta("Song A"))]));

        h.feed(&resolver, update(TRACK_A, 0, true, false)).await;
        assert!(h.pipeline.pending.is_some());

        h.feed(&resolver, update(TRACK_A, 5_000, false, false))
            .await;
        assert!(
            h.pipeline.pending.is_none(),
            "stop must drop the pending play"
        );

        tokio::time::advance(Duration::from_millis(60_000)).await;
        settle().await;
        assert!(h.persisted().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn pause_and_resume_drive_the_listen_clock() {
        let mut h = Harness::new();
        let resolver =
            MetadataResolver::with_fetcher(ScriptedFetcher::new(vec![Some(full_meta("Song A"))]));

        h.feed(&resolver, update(TRACK_A, 0, true, false)).await;
        h.feed(&resolver, update(TRACK_A, 100, true, true)).await;
        assert!(
            h.pipeline.pending.is_some(),
            "pause must not discard the play"
        );

        tokio::time::advance(Duration::from_millis(300_000)).await;
        settle().await;
        assert!(
            h.persisted().is_empty(),
            "paused play must not persist on wall clock"
        );

        h.feed(&resolver, update(TRACK_A, 100, true, false)).await;
        tokio::time::advance(Duration::from_millis(30_001)).await;
        settle().await;
        assert_eq!(h.persisted(), [ID_A.to_string()]);
    }

    /// Fetch fails AND the cluster map has no title, so build_event errors.
    #[tokio::test(start_paused = true)]
    async fn unresolvable_metadata_drops_the_play() {
        let mut h = Harness::new();
        let resolver = MetadataResolver::with_fetcher(ScriptedFetcher::new(vec![None]));
        let mut rx = h.state.tx.subscribe();

        let mut u = update(TRACK_A, 0, true, false);
        u.cluster
            .0
            .as_mut()
            .unwrap()
            .player_state
            .0
            .as_mut()
            .unwrap()
            .track
            .0
            .as_mut()
            .unwrap()
            .metadata
            .clear();
        h.feed(&resolver, u).await;

        assert!(rx.try_recv().is_err(), "dropped play must not broadcast");
        assert!(h.pipeline.pending.is_none());
    }

    use crate::events::FrameKind;

    fn json(frame: &crate::events::WsFrame) -> serde_json::Value {
        serde_json::from_slice(&frame.bytes).unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn pause_seek_resume_emit_state_frames() {
        let mut h = Harness::new();
        let resolver =
            MetadataResolver::with_fetcher(ScriptedFetcher::new(vec![Some(full_meta("Song A"))]));
        let mut rx = h.state.tx.subscribe();

        h.feed(&resolver, update(TRACK_A, 0, true, false)).await;
        let frame = rx.try_recv().unwrap();
        assert_eq!(frame.kind, FrameKind::Play);
        assert_eq!(json(&frame)["playback"]["state"], "playing");

        h.feed(&resolver, update(TRACK_A, 100, true, true)).await;
        let frame = rx.try_recv().expect("pause must broadcast a state frame");
        assert_eq!(frame.kind, FrameKind::State);
        let v = json(&frame);
        assert_eq!(v["type"], "state");
        assert_eq!(v["track_id"], ID_A);
        assert_eq!(v["playback"]["state"], "paused");
        assert!(
            h.pipeline.pending.is_some(),
            "pause must not discard the play"
        );

        h.feed(&resolver, update(TRACK_A, 100, true, true)).await;
        assert!(rx.try_recv().is_err(), "duplicate must not re-publish");

        h.feed(&resolver, update(TRACK_A, 100, true, false)).await;
        assert_eq!(
            json(&rx.try_recv().unwrap())["playback"]["state"],
            "playing"
        );

        h.feed(&resolver, update(TRACK_A, 120_000, true, false))
            .await;
        let v = json(&rx.try_recv().expect("seek must broadcast"));
        assert!(v["playback"]["position_ms"].as_u64().unwrap() >= 120_000);
        assert!(h.pipeline.pending.is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn stop_emits_stopped_state_once() {
        let mut h = Harness::new();
        let resolver =
            MetadataResolver::with_fetcher(ScriptedFetcher::new(vec![Some(full_meta("Song A"))]));
        let mut rx = h.state.tx.subscribe();

        h.feed(&resolver, update(TRACK_A, 0, true, false)).await;
        rx.try_recv().unwrap();

        let stop = update(TRACK_A, 5_000, false, false);
        h.feed(&resolver, stop.clone()).await;
        let frame = rx.try_recv().expect("stop must broadcast");
        assert_eq!(frame.kind, FrameKind::State);
        assert_eq!(json(&frame)["playback"]["state"], "stopped");

        h.feed(&resolver, stop).await;
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn play_frame_carries_scrambled_lyrics() {
        let mut h = Harness::new();
        let resolver = MetadataResolver::with_fetcher(ScriptedFetcher::new(vec![Some(
            full_meta_with_lyrics("Song A"),
        )]));
        let mut rx = h.state.tx.subscribe();

        h.feed(&resolver, update(TRACK_A, 0, true, false)).await;
        let frame = rx.try_recv().unwrap();
        assert_eq!(frame.kind, FrameKind::Play);
        let v = json(&frame);
        assert_eq!(v["lyrics"]["lines"].as_array().unwrap().len(), 2);
        assert_eq!(v["lyrics"]["lines"][0]["start_ms"], 1_000);

        h.feed(&resolver, update(TRACK_A, 100, true, true)).await;
        let state_frame = json(&rx.try_recv().unwrap());
        assert_eq!(state_frame["type"], "state");
        assert!(
            state_frame.get("lyrics").is_none(),
            "state frames must stay light"
        );
    }

    fn cluster_of(u: &mut ClusterUpdate) -> &mut Cluster {
        u.cluster.0.as_mut().unwrap()
    }

    fn clear_metadata(u: &mut ClusterUpdate) {
        cluster_of(u)
            .player_state
            .0
            .as_mut()
            .unwrap()
            .track
            .0
            .as_mut()
            .unwrap()
            .metadata
            .clear();
    }

    fn private(mut u: ClusterUpdate) -> ClusterUpdate {
        use librespot::protocol::connect::DeviceInfo;
        cluster_of(&mut u).device.insert(
            "test-device".to_string(),
            DeviceInfo {
                is_private_session: true,
                ..Default::default()
            },
        );
        u
    }

    fn on_own_device(mut u: ClusterUpdate) -> ClusterUpdate {
        cluster_of(&mut u).active_device_id = OWN_DEVICE.to_string();
        u
    }

    /// Startup into a stopped track: it never becomes a play, but clients
    /// must learn its metadata before any state for it — one `now_playing`
    /// frame (metadata from the cluster-map fallback here), no `state`
    /// frame, nothing pending.
    #[tokio::test(start_paused = true)]
    async fn startup_into_stopped_track_introduces_it_with_now_playing() {
        let mut h = Harness::new();
        let resolver = MetadataResolver::with_fetcher(ScriptedFetcher::new(vec![]));
        let mut rx = h.state.tx.subscribe();

        h.feed(&resolver, update(TRACK_A, 5_000, false, false))
            .await;
        let frame = rx.try_recv().expect("stopped startup must broadcast");
        assert_eq!(frame.kind, FrameKind::Play);
        let v = json(&frame);
        assert_eq!(v["type"], "now_playing");
        assert_eq!(v["title"], "Map Title");
        assert_eq!(v["playback"]["state"], "stopped");
        assert_eq!(v["track_id"], ID_A);
        assert!(rx.try_recv().is_err(), "exactly one frame");
        assert!(h.pipeline.pending.is_none());
        assert!(h.persisted().is_empty());
        assert!(h.state.last_play.read().await.is_some());
    }

    /// No metadata anywhere: clients must not get a `state` frame for a
    /// track they cannot render.
    #[tokio::test(start_paused = true)]
    async fn state_for_an_unresolvable_track_is_dropped() {
        let mut h = Harness::new();
        let resolver = MetadataResolver::with_fetcher(ScriptedFetcher::new(vec![]));
        let mut rx = h.state.tx.subscribe();

        let mut u = update(TRACK_A, 5_000, false, false);
        clear_metadata(&mut u);
        h.feed(&resolver, u).await;
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn private_session_stops_publicly_and_leaks_nothing() {
        let mut h = Harness::new();
        let resolver =
            MetadataResolver::with_fetcher(ScriptedFetcher::new(vec![Some(full_meta("Song A"))]));
        let mut rx = h.state.tx.subscribe();

        h.feed(&resolver, update(TRACK_A, 0, true, false)).await;
        rx.try_recv().unwrap();

        h.feed(&resolver, private(update(TRACK_B, 0, true, false)))
            .await;
        let frame = rx
            .try_recv()
            .expect("public track must be reported stopped");
        let v = json(&frame);
        assert_eq!(v["type"], "state");
        assert_eq!(
            v["track_id"], ID_A,
            "the private track id must never appear"
        );
        assert_eq!(v["playback"]["state"], "stopped");
        assert!(h.pipeline.pending.is_none(), "pending play is discarded");

        h.feed(&resolver, private(update(TRACK_B, 5_000, true, false)))
            .await;
        assert!(rx.try_recv().is_err());
        tokio::time::advance(Duration::from_secs(120)).await;
        settle().await;
        assert!(h.persisted().is_empty());
        assert_eq!(
            resolver_calls(&resolver),
            1,
            "no metadata fetch for private items"
        );
    }

    fn resolver_calls(resolver: &MetadataResolver<ScriptedFetcher>) -> usize {
        resolver.fetcher_for_tests().call_count()
    }

    #[tokio::test(start_paused = true)]
    async fn own_device_is_released_once_per_activation() {
        let mut h = Harness::new();
        let resolver = MetadataResolver::with_fetcher(ScriptedFetcher::new(vec![]));

        h.feed(&resolver, on_own_device(update(TRACK_A, 0, true, false)))
            .await;
        h.feed(&resolver, on_own_device(update(TRACK_A, 500, true, false)))
            .await;
        assert_eq!(h.device.releases.load(Ordering::SeqCst), 1);
        assert!(h.pipeline.pending.is_none());

        h.feed(&resolver, update(TRACK_A, 1_000, false, false))
            .await;
        h.feed(&resolver, on_own_device(update(TRACK_A, 0, true, false)))
            .await;
        assert_eq!(h.device.releases.load(Ordering::SeqCst), 2);
    }

    /// Reconnect: the pipeline survives, the pending play is suspended, and
    /// the first same-track update after the reconnect resumes the countdown
    /// — the listening before the disconnect still counts.
    #[tokio::test(start_paused = true)]
    async fn pending_play_survives_a_reconnect() {
        let mut h = Harness::new();
        let resolver =
            MetadataResolver::with_fetcher(ScriptedFetcher::new(vec![Some(full_meta("Song A"))]));
        let mut rx = h.state.tx.subscribe();

        h.feed(&resolver, update(TRACK_A, 0, true, false)).await;
        rx.try_recv().unwrap();
        tokio::time::advance(Duration::from_millis(20_000)).await;
        h.pipeline.pending.as_mut().unwrap().suspend();
        tokio::time::advance(Duration::from_millis(60_000)).await;
        settle().await;
        assert!(h.persisted().is_empty());

        h.feed(&resolver, update(TRACK_A, 80_000, true, false))
            .await;
        assert!(
            !matches!(rx.try_recv(), Ok(f) if f.kind == FrameKind::Play),
            "the running track must not be announced as a new play"
        );
        tokio::time::advance(Duration::from_millis(10_001)).await;
        settle().await;
        assert_eq!(h.persisted(), [ID_A.to_string()]);
    }

    /// Ads and other unsupported items hold (not discard) the pending play;
    /// the listener returning to the same track continues its countdown.
    #[tokio::test(start_paused = true)]
    async fn unsupported_media_holds_the_pending_play() {
        let mut h = Harness::new();
        let resolver =
            MetadataResolver::with_fetcher(ScriptedFetcher::new(vec![Some(full_meta("Song A"))]));

        h.feed(&resolver, update(TRACK_A, 0, true, false)).await;
        tokio::time::advance(Duration::from_millis(10_000)).await;
        h.feed(
            &resolver,
            update("spotify:ad:5sWHDYs0csV6RS48xBl0tH", 0, true, false),
        )
        .await;
        tokio::time::advance(Duration::from_millis(60_000)).await;
        settle().await;
        assert!(
            h.persisted().is_empty(),
            "no listening accrues during the ad"
        );
        assert!(h.pipeline.pending.is_some());

        h.feed(&resolver, update(TRACK_A, 10_000, true, false))
            .await;
        tokio::time::advance(Duration::from_millis(20_001)).await;
        settle().await;
        assert_eq!(h.persisted(), [ID_A.to_string()]);
    }

    #[tokio::test(start_paused = true)]
    async fn local_files_are_published_but_never_persisted() {
        let mut h = Harness::new();
        let resolver = MetadataResolver::with_fetcher(ScriptedFetcher::new(vec![]));
        let mut rx = h.state.tx.subscribe();

        h.feed(
            &resolver,
            update(
                "spotify:local:Some+Artist:Some+Album:Home+Demo:180",
                0,
                true,
                false,
            ),
        )
        .await;
        let v = json(&rx.try_recv().expect("local play must broadcast"));
        assert_eq!(v["kind"], "local");
        assert_eq!(v["title"], "Home Demo");
        assert!(v["track_url"].is_null());
        assert!(h.pipeline.pending.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn episodes_link_to_the_episode_page_and_persist() {
        let mut h = Harness::new();
        let resolver =
            MetadataResolver::with_fetcher(ScriptedFetcher::new(vec![Some(full_meta("Pod"))]));
        let mut rx = h.state.tx.subscribe();

        h.feed(
            &resolver,
            update("spotify:episode:512ojhOuo1ktJprKbVcKyQ", 0, true, false),
        )
        .await;
        let v = json(&rx.try_recv().unwrap());
        assert_eq!(v["kind"], "episode");
        assert_eq!(
            v["track_url"],
            "https://open.spotify.com/episode/512ojhOuo1ktJprKbVcKyQ"
        );
        tokio::time::advance(Duration::from_millis(30_001)).await;
        settle().await;
        assert_eq!(h.persisted(), ["512ojhOuo1ktJprKbVcKyQ".to_string()]);
    }

    /// Server-clock correction: a server clock ahead of ours shifts the
    /// published `as_of`.
    #[tokio::test(start_paused = true)]
    async fn server_clock_offset_shifts_published_timestamps() {
        let mut h = Harness::new();
        let resolver =
            MetadataResolver::with_fetcher(ScriptedFetcher::new(vec![Some(full_meta("Song A"))]));
        let mut rx = h.state.tx.subscribe();

        let mut u = update(TRACK_A, 0, true, false);
        cluster_of(&mut u).server_timestamp_ms = Utc::now().timestamp_millis() + 120_000;
        h.feed(&resolver, u).await;
        let v = json(&rx.try_recv().unwrap());
        let as_of: DateTime<Utc> = v["playback"]["as_of"].as_str().unwrap().parse().unwrap();
        let skew = (as_of - Utc::now()).num_milliseconds();
        assert!((110_000..=130_000).contains(&skew), "as_of skew {skew} ms");
    }

    #[tokio::test(start_paused = true)]
    async fn persist_retries_with_doubling_backoff_then_succeeds() {
        let calls = AtomicUsize::new(0);
        let mut backoffs = Vec::new();
        let started = tokio::time::Instant::now();
        let (outcome, attempts) = persist_with_retry(
            5,
            Duration::from_secs(1),
            || async {
                if calls.fetch_add(1, Ordering::SeqCst) < 2 {
                    Err("db down")
                } else {
                    Ok(true)
                }
            },
            |_, _, backoff| backoffs.push(backoff),
        )
        .await;
        assert!(matches!(outcome, PersistOutcome::Persisted));
        assert_eq!(attempts, 3);
        assert_eq!(
            backoffs,
            [Some(Duration::from_secs(1)), Some(Duration::from_secs(2))]
        );
        assert_eq!(started.elapsed(), Duration::from_secs(3));
    }

    #[tokio::test(start_paused = true)]
    async fn persist_gives_up_after_the_last_attempt() {
        let mut errors = 0;
        let (outcome, attempts) = persist_with_retry(
            3,
            Duration::from_secs(1),
            || async { Err::<bool, _>("db down") },
            |_, _, _| errors += 1,
        )
        .await;
        assert!(matches!(outcome, PersistOutcome::Failed("db down")));
        assert_eq!((attempts, errors), (3, 3));

        let (outcome, _) = persist_with_retry(
            3,
            Duration::from_secs(1),
            || async { Ok::<_, &str>(false) },
            |_, _, _| {},
        )
        .await;
        assert!(matches!(outcome, PersistOutcome::ReplaySuppressed));
    }

    #[tokio::test(start_paused = true)]
    async fn repeat_one_restart_is_a_new_play_frame() {
        let mut h = Harness::new();
        let resolver =
            MetadataResolver::with_fetcher(ScriptedFetcher::new(vec![Some(full_meta("Song A"))]));
        let mut rx = h.state.tx.subscribe();

        let mut first = update(TRACK_A, 0, true, false);
        let t0 = Utc::now().timestamp_millis() - 200_000;
        cluster_of(&mut first)
            .player_state
            .0
            .as_mut()
            .unwrap()
            .timestamp = t0;
        h.feed(&resolver, first).await;
        rx.try_recv().unwrap();

        h.feed(&resolver, update(TRACK_A, 0, true, false)).await;
        let frame = rx.try_recv().expect("restart must broadcast a play");
        assert_eq!(frame.kind, FrameKind::Play);
        assert_eq!(json(&frame)["type"], "play");
    }

    /// The alerting instruments record through a private provider: a lost
    /// play counts once in `plays_persist_failed_total` (not per attempt),
    /// and both gauges report live state — the cluster age only once an
    /// update was seen.
    #[tokio::test]
    async fn alerting_metrics_record_lost_plays_auth_state_and_cluster_age() {
        use opentelemetry::metrics::MeterProvider as _;
        use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};

        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();
        let state = test_state();
        let metrics = Metrics::new(&provider.meter("nowplaying"), &state);
        let dump = || {
            provider.force_flush().unwrap();
            format!("{:?}", exporter.get_finished_metrics().unwrap())
        };

        assert!(!dump().contains("spotify_cluster_update_age"));

        state.set_spotify_auth_failed(true);
        state.mark_cluster_update();
        let event = play_event(
            TRACK_A,
            MediaKind::Track,
            full_meta("Song A"),
            Utc::now().timestamp_millis(),
            0,
        )
        .unwrap();
        PersistSink::new(&state, &metrics).settle(
            &event,
            PersistOutcome::Failed(sqlx::Error::PoolTimedOut),
            PERSIST_ATTEMPTS,
        );

        let dump = dump();
        for needle in [
            "plays_persist_failed_total",
            "spotify_auth_failed",
            "spotify_cluster_update_age",
        ] {
            assert!(dump.contains(needle), "{needle} missing from export");
        }
    }
}
