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
use crate::web::ws_limits::Rejection;

/// Proxies silently drop idle connections; periodic pings keep them open and
/// surface dead peers as send errors.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
/// A peer that answered none of the pings in this span is gone, even if the
/// TCP connection has not noticed yet.
const PONG_TIMEOUT: Duration = Duration::from_secs(2 * KEEPALIVE_INTERVAL.as_secs());
/// Clients never need to send more than control frames; the tungstenite
/// default would buffer up to 64 MiB per inbound message.
const MAX_INBOUND_BYTES: usize = 1024;

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
/// allowed. Admission control follows (per-address 429, global 503; see
/// [`crate::web::ws_limits`]); the permit lives as long as the session. An
/// accepted session runs under a `ws.session` span carrying the visitor
/// address plus, at the end, `close.reason`, `messages.sent` and
/// `session.duration_s`.
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
            warn!(
                ?origin,
                client.address = %client,
                reason = "origin",
                "rejecting ws upgrade from disallowed origin"
            );
            return StatusCode::FORBIDDEN.into_response();
        }
    }
    let permit = match state.ws_limits.try_admit(&client) {
        Ok(permit) => permit,
        Err(rejection) => {
            warn!(
                client.address = %client,
                reason = rejection.as_str(),
                "rejecting ws upgrade over the connection limit"
            );
            let status = match rejection {
                Rejection::PerIpLimit => StatusCode::TOO_MANY_REQUESTS,
                Rejection::GlobalLimit => StatusCode::SERVICE_UNAVAILABLE,
            };
            return status.into_response();
        }
    };
    let session_span = info_span!(
        "ws.session",
        "client.address" = %client,
        close.reason = tracing::field::Empty,
        messages.sent = tracing::field::Empty,
        session.duration_s = tracing::field::Empty,
    );
    ws.max_message_size(MAX_INBOUND_BYTES)
        .max_frame_size(MAX_INBOUND_BYTES)
        .on_failed_upgrade(
            move |e| warn!(client.address = %client, "websocket upgrade failed: {e}"),
        )
        .on_upgrade(move |socket| {
            async move {
                let _permit = permit;
                client_loop(socket, state).await;
            }
            .instrument(session_span)
        })
        .into_response()
}

/// Why a session ended, recorded as `close.reason`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CloseReason {
    ClientClose,
    SendError,
    PongTimeout,
    ServerShutdown,
}

impl CloseReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::ClientClose => "client_close",
            Self::SendError => "send_error",
            Self::PongTimeout => "pong_timeout",
            Self::ServerShutdown => "server_shutdown",
        }
    }
}

/// Per-connection loop: subscribe, replay the cached `now_playing`, then
/// forward broadcast frames until the session ends.
///
/// Subscribing before reading the replay guarantees no event is missed; a
/// play published in between would arrive twice, so `skip` holds the
/// replayed identity and `should_skip` drops that one duplicate (only the
/// first play frame after a replay/resync is checked — anything older
/// self-corrects with the next event). Lagged clients are resynced with the
/// current state instead of being disconnected, since skipped events are
/// stale by definition. The keepalive interval's immediate first tick is
/// consumed before the loop; each later tick first checks that the peer
/// answered a ping within [`PONG_TIMEOUT`]. Inbound frames are drained (axum
/// answers pings itself); pongs refresh the liveness clock.
async fn client_loop(mut socket: WebSocket, state: AppState) {
    let m = metrics();
    m.connections.add(1, &[]);
    let connected_at = std::time::Instant::now();
    info!("ws client connected");

    let mut rx = state.tx.subscribe();
    let mut keepalive = tokio::time::interval(KEEPALIVE_INTERVAL);
    keepalive.tick().await;
    let mut last_pong = tokio::time::Instant::now();
    let mut sent: u64 = 0;

    let mut skip: Option<PlayKey> = None;
    let replay = state
        .last_play
        .read()
        .await
        .as_ref()
        .map(|lp| (lp.event.key(), lp.now_playing_frame.clone()));
    let reason = 'session: {
        if let Some((key, frame)) = replay {
            if send_frame(&mut socket, &frame).await.is_err() {
                break 'session CloseReason::SendError;
            }
            m.sent.add(1, &[]);
            sent += 1;
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
                            break 'session CloseReason::SendError;
                        }
                        m.sent.add(1, &[]);
                        sent += 1;
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
                                break 'session CloseReason::SendError;
                            }
                            m.sent.add(1, &[]);
                            sent += 1;
                            skip = Some(key);
                        }
                    }
                    Err(RecvError::Closed) => break 'session CloseReason::ServerShutdown,
                },
                _ = keepalive.tick() => {
                    if last_pong.elapsed() > PONG_TIMEOUT {
                        let _ = socket.send(Message::Close(None)).await;
                        break 'session CloseReason::PongTimeout;
                    }
                    if socket.send(Message::Ping(Bytes::new())).await.is_err() {
                        break 'session CloseReason::SendError;
                    }
                }
                incoming = socket.recv() => match incoming {
                    Some(Ok(Message::Close(_))) | None => break 'session CloseReason::ClientClose,
                    Some(Ok(Message::Pong(_))) => last_pong = tokio::time::Instant::now(),
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        debug!("ws receive error: {e}");
                        break 'session CloseReason::ClientClose;
                    }
                },
            }
        }
    };

    let duration = connected_at.elapsed();
    m.connections.add(-1, &[]);
    m.session_duration.record(duration.as_secs_f64(), &[]);
    let span = tracing::Span::current();
    span.record("close.reason", reason.as_str());
    span.record("messages.sent", sent);
    span.record("session.duration_s", duration.as_secs());
    info!(
        close.reason = reason.as_str(),
        messages.sent = sent,
        session_duration_s = duration.as_secs(),
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
