pub mod cluster;
pub mod metadata;
pub mod pending;
pub mod session;
pub mod sink;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::{Context, anyhow, bail};
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use librespot::connect::Spirc;
use librespot::core::dealer::manager::BoxedStreamResult;
use librespot::core::dealer::protocol::Message;
use librespot::protocol::connect::ClusterUpdate;
use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, ObservableGauge};
use tokio::sync::watch;
use tracing::{Instrument, error, info, info_span, instrument, warn};

use crate::db;
use crate::events::{PlayEvent, PlaybackState};
use crate::spotify::cluster::{
    ClusterSnapshot, LiveStateTracker, NewPlay, PlaybackTracker, StatePatch, Suppressed,
};
use crate::spotify::metadata::{FetchTrack, MetadataResolver};
use crate::spotify::pending::{DiscardReason, PendingPersist, PersistFn};
use crate::state::AppState;

const CLUSTER_URI: &str = "hm://connect-state/v1/cluster";
const MAX_RECONNECTS: usize = 5;
const RECONNECT_WINDOW: Duration = Duration::from_secs(600);
/// Reconnect replay guard: a play of the same track starting within this
/// window of the last persisted row is considered the same playback.
const REPLAY_GUARD_SECS: f64 = 10.0;

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
    /// Kept alive so the observable callback stays registered.
    _connected: ObservableGauge<u64>,
}

impl Metrics {
    fn new(state: &AppState) -> Self {
        let meter = global::meter("nowplaying");
        let connected = state.spotify_connected.clone();
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

/// Supervision loop: (re)build the whole session/Spirc/dealer stack on any
/// failure, mirroring the librespot CLI's rate-limited reconnect behaviour.
/// A connection that survives [`RECONNECT_WINDOW`] resets the failure budget
/// and the backoff; exceeding [`MAX_RECONNECTS`] within the window exits the
/// process and hands recovery to systemd.
async fn run(state: AppState, mut shutdown: watch::Receiver<bool>) -> anyhow::Result<()> {
    let metrics = Metrics::new(&state);
    let mut recent_failures: Vec<tokio::time::Instant> = Vec::new();
    let mut backoff = Duration::from_secs(1);

    loop {
        let started = tokio::time::Instant::now();
        match run_once(&state, &metrics, &mut shutdown).await {
            Ok(()) => {
                info!("spotify task shutting down");
                return Ok(());
            }
            Err(e) => {
                state.set_spotify_connected(false);
                error!("spotify connection failed: {e:#}");
            }
        }
        if *shutdown.borrow() {
            return Ok(());
        }

        if started.elapsed() > RECONNECT_WINDOW {
            recent_failures.clear();
            backoff = Duration::from_secs(1);
        }
        let now = tokio::time::Instant::now();
        recent_failures.retain(|t| now.duration_since(*t) < RECONNECT_WINDOW);
        recent_failures.push(now);
        if recent_failures.len() > MAX_RECONNECTS {
            bail!(
                "spotify reconnected too often ({MAX_RECONNECTS}x within {RECONNECT_WINDOW:?}); giving up"
            );
        }

        metrics.reconnects.add(1, &[]);
        info!("reconnecting to Spotify in {backoff:?}");
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = shutdown.changed() => return Ok(()),
        }
        backoff = (backoff * 2).min(Duration::from_secs(60));
    }
}

/// One connection lifetime. Returns Ok(()) only on requested shutdown; any
/// other exit is an error that triggers a reconnect.
///
/// The setup order is load-bearing: the cluster subscription is registered
/// **before** `Spirc::new`, because librespot connects the session only once
/// all dealer listeners are in place (subscriptions to the same URI fan out —
/// each subscriber gets its own copy). Session build, subscription and Spirc
/// registration run under a single `spotify.connect` span so slow or failed
/// connects are visible as one unit.
///
/// Every select arm exits by breaking out of the loop, funnelling into the
/// single cleanup point that discards a pending, not-yet-fired persist: a
/// timer outliving the connection would double-persist once the reconnect
/// re-detects the current track.
async fn run_once(
    state: &AppState,
    metrics: &Metrics,
    shutdown: &mut watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let (session, mut cluster_stream, spirc, spirc_task) = async {
        let bundle = session::build(&state.cfg)?;
        let session = bundle.session.clone();

        let cluster_stream: BoxedStreamResult<ClusterUpdate> = session
            .dealer()
            .listen_for(CLUSTER_URI, Message::from_raw)
            .map_err(|e| anyhow::anyhow!("subscribing to cluster updates: {e}"))?;

        let (spirc, spirc_task) = Spirc::new(
            bundle.connect_config,
            session.clone(),
            bundle.credentials,
            bundle.player,
            bundle.mixer,
        )
        .await
        .map_err(|e| anyhow::anyhow!("starting Spirc: {e}"))?;
        anyhow::Ok((session, cluster_stream, spirc, spirc_task))
    }
    .instrument(info_span!(
        "spotify.connect",
        device_name = %state.cfg.device_name,
    ))
    .await?;
    let mut spirc_task = std::pin::pin!(spirc_task);

    state.set_spotify_connected(true);
    info!(device_name = %state.cfg.device_name, "connected as Spotify Connect device");

    let resolver = MetadataResolver::new(session.clone());
    let persist = make_persist_fn(state, metrics);
    let mut pipeline = Pipeline::default();
    let mut invalid_check = tokio::time::interval(Duration::from_secs(5));

    let result = loop {
        tokio::select! {
            _ = &mut spirc_task => {
                break Err(anyhow!("spirc task ended unexpectedly"));
            }
            _ = shutdown.changed() => {
                let _ = spirc.shutdown();
                let _ = tokio::time::timeout(Duration::from_secs(5), &mut spirc_task).await;
                break Ok(());
            }
            _ = invalid_check.tick() => {
                if session.is_invalid() {
                    metrics.session_invalid.add(1, &[]);
                    break Err(anyhow!("session invalidated"));
                }
            }
            item = cluster_stream.next() => {
                match item {
                    None => break Err(anyhow!("cluster update stream ended")),
                    Some(Err(e)) => warn!("undecodable cluster update: {e}"),
                    Some(Ok(update)) => {
                        handle_update(state, metrics, &resolver, &mut pipeline, &persist, update).await;
                    }
                }
            }
        }
    };

    if let Some(p) = pipeline.pending.take() {
        p.discard(DiscardReason::Disconnected);
    }
    result
}

/// Per-connection playback pipeline state, rebuilt fresh on every
/// (re)connect: the first update then re-detects the current play and
/// re-publishes the live state once — idempotent for both DB and clients.
#[derive(Default)]
struct Pipeline {
    tracker: PlaybackTracker,
    live: LiveStateTracker,
    pending: Option<PendingPersist>,
}

/// Single entry point for every dealer cluster update; scrobbling and
/// live-state replication both branch off here.
///
/// A new play supersedes any not-yet-qualified pending persist (skips below
/// the listen threshold never reach the database) and carries its playback
/// state inside the `play` frame, computed before the metadata fetch so its
/// latency cannot skew the position. Updates suppressed for scrobbling —
/// pause/resume/seek/stop, including trackless `no_track` updates — still
/// feed the [`LiveStateTracker`] and become `state` frames. `pending` always
/// belongs to the current track: a new track replaces it, a metadata failure
/// clears it.
///
/// Tracing: one `cluster.handle_update` span per update; declared fields are
/// recorded as soon as they are known. `outcome` is one of
/// new_play | no_track | not_playing | same_track | seek | metadata_failed;
/// `update.reason` is the bare enum variant name (not the protobuf `Result`
/// wrapper), so span filters match; failures set `otel.status_code = ERROR`
/// so trace UIs surface them.
#[instrument(
    name = "cluster.handle_update",
    skip_all,
    fields(
        update.reason = tracing::field::Empty,
        active_device = tracing::field::Empty,
        track.uri = tracing::field::Empty,
        track.title = tracing::field::Empty,
        playing = tracing::field::Empty,
        paused = tracing::field::Empty,
        position_ms = tracing::field::Empty,
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
    update: ClusterUpdate,
) {
    let span = tracing::Span::current();
    let reason = update
        .update_reason
        .enum_value()
        .map(|r| format!("{r:?}"))
        .unwrap_or_else(|n| format!("UNKNOWN({n})"));
    span.record("update.reason", reason.as_str());

    let now_ms = Utc::now().timestamp_millis();
    let Some(snapshot) = ClusterSnapshot::from_update(update) else {
        span.record("outcome", "no_track");
        metrics
            .suppressed
            .add(1, &[KeyValue::new("reason", "no_track")]);
        if let Some(patch) = pipeline.live.observe(None, now_ms) {
            publish_state_patch(state, metrics, patch).await;
        }
        return;
    };
    span.record("active_device", snapshot.active_device_id.as_str());
    span.record("track.uri", snapshot.track_uri.as_str());
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
            match build_event(resolver, &snapshot, &new_play).await {
                Ok(event) => {
                    span.record("outcome", "new_play");
                    span.record("track.title", event.title.as_str());
                    info!(
                        title = %event.title,
                        artists = ?event.artists,
                        device = %snapshot.active_device_id,
                        "new play"
                    );
                    state.publish_play(event.clone(), playback).await;
                    pipeline.pending = Some(PendingPersist::new(
                        event,
                        snapshot.is_paused,
                        Duration::from_millis(state.cfg.min_play_ms),
                        persist.clone(),
                        metrics.discarded.clone(),
                    ));
                }
                Err(e) => {
                    span.record("outcome", "metadata_failed");
                    span.record("otel.status_code", "ERROR");
                    error!("dropping play, metadata unresolvable: {e:#}")
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
                    if let Some(p) = pipeline.pending.as_mut() {
                        p.set_paused(snapshot.is_paused);
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
                publish_state_patch(state, metrics, patch).await;
            }
        }
    }
}

/// Broadcast a live-state patch: metric, correlated log line and the
/// `state_published` span field, then the actual publish. `track_url` is a
/// local URI parse (no network).
async fn publish_state_patch(state: &AppState, metrics: &Metrics, patch: StatePatch) {
    let track_id = match metadata::track_url(&patch.track_uri) {
        Ok((id, _)) => id,
        Err(e) => {
            warn!("dropping state update, unparsable track uri: {e:#}");
            return;
        }
    };
    let label = match patch.playback.state {
        PlaybackState::Playing => "playing",
        PlaybackState::Paused => "paused",
        PlaybackState::Stopped => "stopped",
    };
    tracing::Span::current().record("state_published", label);
    metrics
        .state_updates
        .add(1, &[KeyValue::new("state", label)]);
    info!(
        track_id = %track_id,
        state = label,
        position_ms = patch.playback.position_ms,
        "playback state update"
    );
    state.publish_state(track_id, patch.playback).await;
}

async fn build_event<F: FetchTrack>(
    resolver: &MetadataResolver<F>,
    snapshot: &ClusterSnapshot,
    new_play: &NewPlay,
) -> anyhow::Result<PlayEvent> {
    let (track_id, track_url) = metadata::track_url(&new_play.track_uri)?;
    let meta = resolver
        .resolve(
            &new_play.track_uri,
            &new_play.metadata,
            new_play.duration_ms,
        )
        .await?;
    let started_at = DateTime::<Utc>::from_timestamp_millis(new_play.started_at_ms)
        .context("started_at out of range")?;
    Ok(PlayEvent {
        track_id,
        track_url,
        title: meta.title,
        artists: meta.artists,
        album: meta.album,
        cover_url: meta.cover_url,
        duration_ms: if meta.duration_ms > 0 {
            meta.duration_ms
        } else {
            snapshot.duration_ms.max(0) as u32
        },
        started_at,
    })
}

/// Build the action `PendingPersist` runs once a play has accumulated enough
/// listening: a guarded, idempotent insert (see `db::insert_play_guarded`).
///
/// `Ok(false)` means the DB replay guard swallowed a reconnect duplicate —
/// counted in `plays_replay_suppressed_total`; a rising rate there means
/// reconnects re-detect running playbacks more often than expected. The
/// future runs instrumented with the `play.persist` span, which receives
/// `otel.status_code = ERROR` on insert failure.
fn make_persist_fn(state: &AppState, metrics: &Metrics) -> PersistFn {
    let pool = state.db.clone();
    let persisted = metrics.persisted.clone();
    let replay_suppressed = metrics.replay_suppressed.clone();
    let db_errors = metrics.db_errors.clone();
    Arc::new(move |event: PlayEvent| {
        let pool = pool.clone();
        let persisted = persisted.clone();
        let replay_suppressed = replay_suppressed.clone();
        let db_errors = db_errors.clone();
        Box::pin(async move {
            match db::insert_play_guarded(&pool, &event, REPLAY_GUARD_SECS).await {
                Ok(true) => persisted.add(1, &[]),
                Ok(false) => {
                    replay_suppressed.add(1, &[]);
                    info!(track_id = %event.track_id, "duplicate or replayed play suppressed");
                }
                Err(e) => {
                    db_errors.add(1, &[KeyValue::new("op", "insert_play")]);
                    tracing::Span::current().record("otel.status_code", "ERROR");
                    error!("persisting play failed: {e}");
                }
            }
        })
    })
}

/// Integration tests for `handle_update`: the glue between tracker, metadata
/// resolution, broadcast and deferred persistence. The tracker extrapolates
/// against `Utc::now()` (real wall clock, keeps running under the paused test
/// clock); only `PendingPersist`'s timer runs on tokio time, so the 30s
/// threshold is driven with `tokio::time::advance`.
#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use clap::Parser;
    use librespot::protocol::connect::Cluster;
    use librespot::protocol::player::{PlayerState, ProvidedTrack};

    use super::*;
    use crate::config::Config;
    use crate::spotify::metadata::test_support::{ScriptedFetcher, full_meta};

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

    /// Everything handle_update needs, plus a persist recorder instead of a DB.
    struct Harness {
        state: AppState,
        metrics: Metrics,
        pipeline: Pipeline,
        persist: PersistFn,
        seen: Arc<StdMutex<Vec<String>>>,
    }

    impl Harness {
        fn new() -> Self {
            let state = test_state();
            let metrics = Metrics::new(&state);
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

    /// A stopped track never becomes a play, so no fetch is scripted.
    #[tokio::test(start_paused = true)]
    async fn startup_into_stopped_track_emits_single_state_frame() {
        let mut h = Harness::new();
        let resolver = MetadataResolver::with_fetcher(ScriptedFetcher::new(vec![]));
        let mut rx = h.state.tx.subscribe();

        h.feed(&resolver, update(TRACK_A, 5_000, false, false))
            .await;
        let frame = rx.try_recv().expect("stopped startup state must broadcast");
        assert_eq!(frame.kind, FrameKind::State);
        let v = json(&frame);
        assert_eq!(v["playback"]["state"], "stopped");
        assert_eq!(v["track_id"], ID_A);
        assert!(rx.try_recv().is_err(), "exactly one frame");
        assert!(h.pipeline.pending.is_none());
        assert!(h.persisted().is_empty());
    }
}
