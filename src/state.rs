use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use sqlx::PgPool;
use tokio::sync::{RwLock, broadcast};
use tracing::info;

use crate::config::Config;
use crate::events::{FrameKind, PlayEvent, Playback, PlaybackState, WsFrame, state_ws_bytes};
use crate::web::top_cache::TopCache;
use crate::web::ws_limits::WsLimits;

/// Everything cached about the most recent play: the enriched event, its
/// current live playback, and the pre-serialized `now_playing` replay frame —
/// re-serialized on every matching state change so fresh clients always see
/// the true state (paused/position), not the state at play time.
#[derive(Debug, Clone)]
pub struct LastPlay {
    pub event: PlayEvent,
    pub playback: Playback,
    pub now_playing_frame: Bytes,
}

#[derive(Clone)]
pub struct AppState {
    pub cfg: Arc<Config>,
    pub db: PgPool,
    /// Fanout channel; payloads are pre-serialized frames plus identity +
    /// kind (for replay/broadcast dedupe on the WS path).
    pub tx: broadcast::Sender<WsFrame>,
    /// Last play incl. live playback, replayed to fresh WS clients and served
    /// on /api/now-playing. Invariant: every broadcast `state` frame refers
    /// to this play's track.
    pub last_play: Arc<RwLock<Option<LastPlay>>>,
    /// Whether the Spotify session is currently up (for /healthz).
    pub spotify_connected: Arc<AtomicBool>,
    /// Spotify rejected the stored credentials; the spotify task is parked
    /// until they change. Degrades /healthz to 503.
    pub spotify_auth_failed: Arc<AtomicBool>,
    /// Local wall clock (Unix ms) of the last message on the dealer cluster
    /// stream; 0 until the first one. Feeds `spotify_cluster_update_age`,
    /// the only signal for a dealer that stays connected but falls silent.
    pub last_cluster_update_ms: Arc<AtomicI64>,
    pub top_cache: Arc<TopCache>,
    pub ws_limits: Arc<WsLimits>,
}

impl AppState {
    pub fn new(cfg: Config, db: PgPool) -> Self {
        let (tx, _) = broadcast::channel(32);
        let ws_limits = Arc::new(WsLimits::new(cfg.ws_max_connections, cfg.ws_max_per_ip));
        Self {
            cfg: Arc::new(cfg),
            db,
            tx,
            last_play: Arc::new(RwLock::new(None)),
            spotify_connected: Arc::new(AtomicBool::new(false)),
            spotify_auth_failed: Arc::new(AtomicBool::new(false)),
            last_cluster_update_ms: Arc::new(AtomicI64::new(0)),
            top_cache: Arc::new(TopCache::default()),
            ws_limits,
        }
    }

    /// Publish a new play: cache it for replay and broadcast to subscribers.
    /// A send error only means no subscriber is currently connected.
    pub async fn publish_play(&self, event: PlayEvent, playback: Playback) {
        self.publish_full(event, playback, false).await;
    }

    /// Broadcast a track that is current but did not start as an observed
    /// play (e.g. startup into a stopped or paused track), so clients get
    /// its metadata before any `state` frame for it. Uses the `now_playing`
    /// frame type clients already know from the connect replay.
    pub async fn publish_now_playing(&self, event: PlayEvent, playback: Playback) {
        info!(
            track_id = %event.track_id,
            started_at = %event.started_at,
            "broadcasting now_playing for a track without an observed play"
        );
        self.publish_full(event, playback, true).await;
    }

    async fn publish_full(&self, event: PlayEvent, playback: Playback, as_now_playing: bool) {
        let now_playing_frame = event.to_ws_bytes(true, playback);
        let frame = WsFrame {
            kind: FrameKind::Play,
            key: event.key(),
            bytes: if as_now_playing {
                now_playing_frame.clone()
            } else {
                event.to_ws_bytes(false, playback)
            },
        };
        *self.last_play.write().await = Some(LastPlay {
            event,
            playback,
            now_playing_frame,
        });
        let _ = self.tx.send(frame);
    }

    /// Publish a live-state delta (pause/resume/seek/stop) for the cached
    /// play: the cached playback and replay frame are refreshed so connects
    /// during a pause render the true, frozen state. Returns `false` — and
    /// broadcasts nothing — when `track_id` is not the cached play; the
    /// caller then has to introduce the track via
    /// [`AppState::publish_now_playing`] first.
    pub async fn publish_state(&self, track_id: &str, playback: Playback) -> bool {
        let key = {
            let mut guard = self.last_play.write().await;
            match guard.as_mut() {
                Some(lp) if lp.event.track_id == track_id => {
                    lp.playback = playback;
                    lp.now_playing_frame = lp.event.to_ws_bytes(true, playback);
                    lp.event.key()
                }
                _ => return false,
            }
        };
        let _ = self.tx.send(WsFrame {
            kind: FrameKind::State,
            key,
            bytes: state_ws_bytes(track_id, playback),
        });
        true
    }

    /// Seed the cache with the most recent persisted play (stopped, position
    /// 0) so /api/now-playing and the connect replay have something to show
    /// before Spotify reports anything. Never overrides a live play.
    pub async fn hydrate(&self, event: PlayEvent) {
        let mut guard = self.last_play.write().await;
        if guard.is_some() {
            return;
        }
        let playback = Playback {
            state: PlaybackState::Stopped,
            position_ms: 0,
            as_of: Utc::now(),
        };
        info!(
            track_id = %event.track_id,
            started_at = %event.started_at,
            "seeded now-playing cache from the latest persisted play"
        );
        *guard = Some(LastPlay {
            now_playing_frame: event.to_ws_bytes(true, playback),
            event,
            playback,
        });
    }

    pub fn set_spotify_connected(&self, up: bool) {
        self.spotify_connected.store(up, Ordering::Relaxed);
    }

    pub fn is_spotify_connected(&self) -> bool {
        self.spotify_connected.load(Ordering::Relaxed)
    }

    pub fn set_spotify_auth_failed(&self, failed: bool) {
        self.spotify_auth_failed.store(failed, Ordering::Relaxed);
    }

    pub fn is_spotify_auth_failed(&self) -> bool {
        self.spotify_auth_failed.load(Ordering::Relaxed)
    }

    pub fn mark_cluster_update(&self) {
        self.last_cluster_update_ms
            .store(Utc::now().timestamp_millis(), Ordering::Relaxed);
    }

    /// Time since the last cluster message; `None` before the first one.
    pub fn cluster_update_age(&self) -> Option<Duration> {
        cluster_update_age(
            self.last_cluster_update_ms.load(Ordering::Relaxed),
            Utc::now().timestamp_millis(),
        )
    }
}

/// Age of a `last_cluster_update_ms` reading; `None` for the 0 sentinel.
pub(crate) fn cluster_update_age(last_ms: i64, now_ms: i64) -> Option<Duration> {
    (last_ms > 0).then(|| Duration::from_millis(now_ms.saturating_sub(last_ms).max(0) as u64))
}

#[cfg(test)]
mod tests {
    use super::cluster_update_age;
    use std::time::Duration;

    #[test]
    fn cluster_update_age_is_absent_until_the_first_update_and_never_negative() {
        assert_eq!(cluster_update_age(0, 5_000), None);
        assert_eq!(
            cluster_update_age(1_000, 5_000),
            Some(Duration::from_millis(4_000))
        );
        assert_eq!(cluster_update_age(5_000, 1_000), Some(Duration::ZERO));
    }
}
