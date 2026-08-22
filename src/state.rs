use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use sqlx::PgPool;
use tokio::sync::{RwLock, broadcast};

use crate::config::Config;
use crate::events::{FrameKind, PlayEvent, PlayKey, Playback, WsFrame, state_ws_bytes};

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
    /// Fanout channel; payloads are pre-serialized `play`/`state` frames plus
    /// identity + kind (for replay/broadcast dedupe on the WS path).
    pub tx: broadcast::Sender<WsFrame>,
    /// Last play incl. live playback, replayed to fresh WS clients and served
    /// on /api/now-playing.
    pub last_play: Arc<RwLock<Option<LastPlay>>>,
    /// Whether the Spotify session is currently up (for /healthz).
    pub spotify_connected: Arc<AtomicBool>,
}

impl AppState {
    pub fn new(cfg: Config, db: PgPool) -> Self {
        let (tx, _) = broadcast::channel(32);
        Self {
            cfg: Arc::new(cfg),
            db,
            tx,
            last_play: Arc::new(RwLock::new(None)),
            spotify_connected: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Publish a new play: cache it for replay and broadcast to subscribers.
    /// A send error only means no subscriber is currently connected.
    pub async fn publish_play(&self, event: PlayEvent, playback: Playback) {
        let frame = WsFrame {
            kind: FrameKind::Play,
            key: event.key(),
            bytes: event.to_ws_bytes(false, playback),
        };
        let now_playing_frame = event.to_ws_bytes(true, playback);
        *self.last_play.write().await = Some(LastPlay {
            event,
            playback,
            now_playing_frame,
        });
        let _ = self.tx.send(frame);
    }

    /// Publish a live-state delta (pause/resume/seek/stop). When it belongs
    /// to the cached play, the cached playback and replay frame are refreshed
    /// so connects during a pause render the true, frozen state. A state for
    /// a track that never became a cached play (e.g. unresolvable metadata)
    /// is still broadcast under a synthetic key — the key of `State` frames
    /// is never consulted by the WS dedupe.
    pub async fn publish_state(&self, track_id: String, playback: Playback) {
        let key = {
            let mut guard = self.last_play.write().await;
            match guard.as_mut() {
                Some(lp) if lp.event.track_id == track_id => {
                    lp.playback = playback;
                    lp.now_playing_frame = lp.event.to_ws_bytes(true, playback);
                    lp.event.key()
                }
                _ => PlayKey {
                    track_id: track_id.clone(),
                    started_at: playback.as_of,
                },
            }
        };
        let _ = self.tx.send(WsFrame {
            kind: FrameKind::State,
            key,
            bytes: state_ws_bytes(&track_id, playback),
        });
    }

    pub fn set_spotify_connected(&self, up: bool) {
        self.spotify_connected.store(up, Ordering::Relaxed);
    }

    pub fn is_spotify_connected(&self) -> bool {
        self.spotify_connected.load(Ordering::Relaxed)
    }
}
