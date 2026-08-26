use axum::extract::ws::{Message, Utf8Bytes, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram, UpDownCounter};
use std::net::SocketAddr;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::broadcast::error::RecvError;
use tracing::{Instrument, debug, info, info_span, warn};

use crate::events::{FrameKind, PlayKey};
use crate::state::AppState;

/// Proxies silently drop idle connections; periodic pings keep them open and
/// surface dead peers as send errors.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);

struct WsMetrics {
    connections: UpDownCounter<i64>,
    sent: Counter<u64>,
    lagged: Counter<u64>,
    /// Coarse buckets: a spike at exactly 60s/75s is the fingerprint of a
    /// proxy idle-timeout misconfiguration killing healthy sessions.
    session_duration: Histogram<f64>,
}

fn metrics() -> &'static WsMetrics {
    static METRICS: OnceLock<WsMetrics> = OnceLock::new();
    METRICS.get_or_init(|| {
        let meter = global::meter("nowplaying");
        WsMetrics {
            connections: meter
                .i64_up_down_counter("ws_connections_active")
                .with_description("Currently connected WebSocket clients")
                .build(),
            sent: meter
                .u64_counter("ws_messages_sent_total")
                .with_description("Messages pushed to WebSocket clients")
                .build(),
            lagged: meter
                .u64_counter("ws_lagged_total")
                .with_description("Times a slow client skipped broadcast messages")
                .build(),
            session_duration: meter
                .f64_histogram("ws_session_duration")
                .with_unit("s")
                .with_description("WebSocket session lifetime from accept to disconnect")
                .with_boundaries(vec![
                    1.0, 5.0, 15.0, 60.0, 300.0, 900.0, 1800.0, 3600.0, 7200.0,
                ])
                .build(),
        }
    })
}

/// WS upgrade endpoint. CORS does not govern WebSocket handshakes, so the
/// origin allowlist is enforced here: browsers always send `Origin` on WS
/// upgrades, while requests without one (websocat, native clients) are
/// allowed. An accepted session runs under a `ws.session` span carrying the
/// visitor address until disconnect.
pub async fn handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let client = crate::web::client_ip(&headers, Some(peer.ip()));

    if let Some(origin) = headers.get(header::ORIGIN) {
        let patterns = &state.cfg.allowed_origins;
        let allowed = patterns.iter().any(|p| p == "*")
            || origin
                .to_str()
                .is_ok_and(|o| crate::web::origin_allowed(o, patterns));
        if !allowed {
            warn!(?origin, client.address = %client, "rejecting ws upgrade from disallowed origin");
            return StatusCode::FORBIDDEN.into_response();
        }
    }
    let session_span = info_span!("ws.session", "client.address" = %client);
    ws.on_failed_upgrade(move |e| warn!(client.address = %client, "websocket upgrade failed: {e}"))
        .on_upgrade(move |socket| client_loop(socket, state).instrument(session_span))
        .into_response()
}

/// Per-connection loop: subscribe, replay the cached `now_playing`, then
/// forward broadcast frames until the client disconnects.
///
/// Subscribing before reading the replay guarantees no event is missed; a
/// play published in between would arrive twice, so `skip` holds the
/// replayed identity and `should_skip` drops that one duplicate (only the
/// first play frame after a replay/resync is checked — anything older
/// self-corrects with the next event). Lagged clients are resynced with the
/// current state instead of being disconnected, since skipped events are
/// stale by definition. The keepalive interval's immediate first tick is
/// consumed before the loop; inbound frames are drained (axum answers pings
/// itself) and only `Close` or errors end the session.
async fn client_loop(mut socket: WebSocket, state: AppState) {
    let m = metrics();
    m.connections.add(1, &[]);
    let connected_at = std::time::Instant::now();
    info!("ws client connected");

    let mut rx = state.tx.subscribe();
    let mut keepalive = tokio::time::interval(KEEPALIVE_INTERVAL);
    keepalive.tick().await;

    let mut skip: Option<PlayKey> = None;
    let replay = state
        .last_play
        .read()
        .await
        .as_ref()
        .map(|lp| (lp.event.key(), lp.now_playing_frame.clone()));
    if let Some((key, frame)) = replay {
        if send_frame(&mut socket, &frame).await.is_err() {
            m.connections.add(-1, &[]);
            m.session_duration
                .record(connected_at.elapsed().as_secs_f64(), &[]);
            return;
        }
        m.sent.add(1, &[]);
        skip = Some(key);
    }

    loop {
        tokio::select! {
            received = rx.recv() => match received {
                Ok(frame) => {
                    if should_skip(&mut skip, frame.kind, &frame.key) {
                        continue;
                    }
                    if send_frame(&mut socket, &frame.bytes).await.is_err() {
                        break;
                    }
                    m.sent.add(1, &[]);
                }
                Err(RecvError::Lagged(skipped)) => {
                    m.lagged.add(1, &[]);
                    warn!("ws client lagged, skipped {skipped} messages; resyncing");
                    let resync = state
                        .last_play
                        .read()
                        .await
                        .as_ref()
                        .map(|lp| (lp.event.key(), lp.now_playing_frame.clone()));
                    if let Some((key, frame)) = resync {
                        if send_frame(&mut socket, &frame).await.is_err() {
                            break;
                        }
                        m.sent.add(1, &[]);
                        skip = Some(key);
                    }
                }
                Err(RecvError::Closed) => break,
            },
            _ = keepalive.tick() => {
                if socket.send(Message::Ping(Bytes::new())).await.is_err() {
                    break;
                }
            }
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(_)) => {}
                Some(Err(e)) => {
                    debug!("ws receive error: {e}");
                    break;
                }
            },
        }
    }

    m.connections.add(-1, &[]);
    m.session_duration
        .record(connected_at.elapsed().as_secs_f64(), &[]);
    info!(
        session_duration_s = connected_at.elapsed().as_secs(),
        "ws client disconnected"
    );
}

/// Frames are JSON produced by this service — valid UTF-8 by construction.
async fn send_frame(socket: &mut WebSocket, frame: &bytes::Bytes) -> Result<(), axum::Error> {
    let text = Utf8Bytes::try_from(frame.clone()).map_err(axum::Error::new)?;
    socket.send(Message::Text(text)).await
}

/// One-shot dedupe: consumes the pending skip marker and reports whether the
/// incoming frame is the duplicate it guards against. Only `Play` frames are
/// candidates — a `State` frame passes through AND leaves the marker intact,
/// because the sequential publisher guarantees a queued duplicate play frame
/// always precedes any state frames for the same play.
fn should_skip(skip: &mut Option<PlayKey>, kind: FrameKind, key: &PlayKey) -> bool {
    match kind {
        FrameKind::Play => skip.take().is_some_and(|k| k == *key),
        FrameKind::State => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(track_id: &str) -> PlayKey {
        PlayKey {
            track_id: track_id.into(),
            started_at: "2026-07-28T12:00:00Z".parse().unwrap(),
        }
    }

    #[test]
    fn skip_marker_is_one_shot() {
        let mut skip = Some(key("a"));
        assert!(should_skip(&mut skip, FrameKind::Play, &key("a")));
        assert!(!should_skip(&mut skip, FrameKind::Play, &key("a")));

        let mut skip = Some(key("a"));
        assert!(!should_skip(&mut skip, FrameKind::Play, &key("b")));
        assert!(!should_skip(&mut skip, FrameKind::Play, &key("a")));

        let mut skip = None;
        assert!(!should_skip(&mut skip, FrameKind::Play, &key("a")));
    }

    /// A state frame for the same play is never skipped and must not consume
    /// the marker: the queued duplicate play frame that follows still gets
    /// skipped.
    #[test]
    fn state_frames_pass_and_leave_the_marker_intact() {
        let mut skip = Some(key("a"));
        assert!(!should_skip(&mut skip, FrameKind::State, &key("a")));
        assert!(should_skip(&mut skip, FrameKind::Play, &key("a")));
    }
}
