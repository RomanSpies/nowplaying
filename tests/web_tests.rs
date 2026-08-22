//! In-process integration tests for the HTTP/WS layer.
//!
//! No TLS (irrelevant for the logic under test) and no database: the pool is
//! created with `connect_lazy`, and none of the tested routes runs a query.

use std::net::SocketAddr;
use std::time::Duration;

use clap::Parser;
use futures_util::StreamExt;
use nowplaying::config::Config;
use nowplaying::events::PlayEvent;
use nowplaying::state::AppState;
use nowplaying::web;
use tokio_tungstenite::tungstenite::Message;

fn test_state() -> AppState {
    test_state_with_origins("https://rospies.dev,https://*.rospies.dev")
}

/// Every env-backed field is pinned via CLI flag (CLI beats env in clap), so
/// exported NP_*/RUST_LOG/OTEL_* vars cannot leak into the tests. The short
/// acquire timeout keeps the DB-down tests from sitting in sqlx's default
/// 30s wait before /healthz and /api/top report the failure.
fn test_state_with_origins(origins: &str) -> AppState {
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
        origins,
        "--min-play-ms",
        "30000",
        "--top-default-limit",
        "10",
        "--log-filter",
        "info",
    ]);
    let pool = sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(Duration::from_millis(200))
        .connect_lazy("postgres://unused@127.0.0.1:1/unused")
        .expect("lazy pool");
    AppState::new(cfg, pool)
}

/// Serves the router with connect-info, like production — the WS handler
/// extracts the peer address.
async fn spawn_server(state: AppState) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let service = web::router(state).into_make_service_with_connect_info::<SocketAddr>();
    tokio::spawn(async move {
        axum::serve(listener, service).await.unwrap();
    });
    addr
}

/// Distinct `started_at` per event: identical (track_id, started_at) pairs
/// would be treated as the same play by the WS replay/broadcast dedupe.
fn play(title: &str) -> PlayEvent {
    static SEQ: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);
    let offset = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let base: chrono::DateTime<chrono::Utc> = "2026-07-28T12:00:00Z".parse().unwrap();
    PlayEvent {
        track_id: "4uLU6hMCjMI75M1A2tKUQC".into(),
        track_url: "https://open.spotify.com/track/4uLU6hMCjMI75M1A2tKUQC".into(),
        title: title.into(),
        artists: vec!["Rick Astley".into()],
        album: "Whenever You Need Somebody".into(),
        cover_url: None,
        duration_ms: 213_000,
        started_at: base + chrono::Duration::seconds(offset),
    }
}

fn playing_now() -> nowplaying::events::Playback {
    nowplaying::events::Playback {
        state: nowplaying::events::PlaybackState::Playing,
        position_ms: 0,
        as_of: chrono::Utc::now(),
    }
}

async fn recv_json(
    ws: &mut (impl StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin),
) -> serde_json::Value {
    loop {
        let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("timed out waiting for ws message")
            .expect("stream ended")
            .expect("ws error");
        match msg {
            Message::Text(text) => return serde_json::from_str(&text).unwrap(),
            Message::Ping(_) | Message::Pong(_) => continue,
            other => panic!("unexpected ws message: {other:?}"),
        }
    }
}

#[tokio::test]
async fn now_playing_endpoint_204_then_200() {
    let state = test_state();
    let addr = spawn_server(state.clone()).await;
    let client = reqwest::Client::new();
    let url = format!("http://{addr}/api/now-playing");

    let resp = client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 204);

    state
        .publish_play(play("Never Gonna Give You Up"), playing_now())
        .await;

    let resp = client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert_eq!(body["title"], "Never Gonna Give You Up");
    assert_eq!(body["duration_ms"], 213_000);
}

#[tokio::test]
async fn ws_replays_last_play_and_fans_out() {
    let state = test_state();
    let addr = spawn_server(state.clone()).await;
    let ws_url = format!("ws://{addr}/ws");

    state.publish_play(play("First"), playing_now()).await;

    let (mut a, _) = tokio_tungstenite::connect_async(&ws_url).await.unwrap();
    let replay = recv_json(&mut a).await;
    assert_eq!(replay["type"], "now_playing");
    assert_eq!(replay["title"], "First");

    let (mut b, _) = tokio_tungstenite::connect_async(&ws_url).await.unwrap();
    let replay_b = recv_json(&mut b).await;
    assert_eq!(replay_b["type"], "now_playing");

    state.publish_play(play("Second"), playing_now()).await;
    for ws in [&mut a, &mut b] {
        let msg = recv_json(ws).await;
        assert_eq!(msg["type"], "play");
        assert_eq!(msg["title"], "Second");
    }
}

/// Publishes far more than the broadcast capacity (32) with payloads large
/// enough to fill the socket buffers while the client is not reading, so the
/// connection task hits `RecvError::Lagged` — the client must be resynced
/// (or catch up) and the connection stay usable afterwards.
#[tokio::test]
async fn ws_lagged_client_gets_resynced() {
    let state = test_state();
    let addr = spawn_server(state.clone()).await;

    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();

    let filler = "x".repeat(256 * 1024);
    for i in 0..64 {
        state
            .publish_play(play(&format!("{filler}-{i}")), playing_now())
            .await;
    }

    let mut saw_resync_or_latest = false;
    for _ in 0..80 {
        let msg = recv_json(&mut ws).await;
        let title = msg["title"].as_str().unwrap_or_default();
        if msg["type"] == "now_playing" || title.ends_with("-63") {
            saw_resync_or_latest = true;
            break;
        }
    }
    assert!(
        saw_resync_or_latest,
        "lagged client neither resynced nor caught up"
    );

    state.publish_play(play("after-lag"), playing_now()).await;
    let mut found = false;
    for _ in 0..80 {
        let msg = recv_json(&mut ws).await;
        if msg["title"] == "after-lag" {
            found = true;
            break;
        }
    }
    assert!(found, "connection unusable after lag");
    ws.close(None).await.ok();
}

#[tokio::test]
async fn healthz_reports_db_down_with_503() {
    let state = test_state();
    let addr = spawn_server(state).await;

    let resp = reqwest::get(format!("http://{addr}/healthz"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 503);
    let body: serde_json::Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert_eq!(body["db_ok"], false);
    assert_eq!(body["spotify_connected"], false);
}

#[tokio::test]
async fn top_returns_500_when_db_unreachable() {
    let state = test_state();
    let addr = spawn_server(state).await;

    let resp = reqwest::get(format!("http://{addr}/api/top"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 500);
}

#[tokio::test]
async fn wildcard_origin_mode_allows_any_origin() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let state = test_state_with_origins("*");
    let addr = spawn_server(state.clone()).await;

    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/api/now-playing"))
        .header("Origin", "https://evil.com")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.headers()
            .get("access-control-allow-origin")
            .and_then(|v| v.to_str().ok()),
        Some("*")
    );

    state.publish_play(play("Wildcard"), playing_now()).await;
    let mut req = format!("ws://{addr}/ws").into_client_request().unwrap();
    req.headers_mut()
        .insert("Origin", "https://evil.com".parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    let msg = recv_json(&mut ws).await;
    assert_eq!(msg["type"], "now_playing");
    assert_eq!(msg["title"], "Wildcard");
}

/// Disallowed browser origins are rejected with 403, allowed (wildcard
/// subdomain) origins upgrade and receive the replay. Handshakes without an
/// Origin header (non-browser clients) are allowed — covered implicitly by
/// every other WS test, since tungstenite sends none by default.
#[tokio::test]
async fn ws_upgrade_enforces_origin_allowlist() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let state = test_state();
    let addr = spawn_server(state.clone()).await;
    let url = format!("ws://{addr}/ws");

    let mut req = url.clone().into_client_request().unwrap();
    req.headers_mut()
        .insert("Origin", "https://evil.com".parse().unwrap());
    let err = tokio_tungstenite::connect_async(req).await.unwrap_err();
    match err {
        tokio_tungstenite::tungstenite::Error::Http(resp) => assert_eq!(resp.status(), 403),
        other => panic!("expected HTTP 403 rejection, got {other:?}"),
    }

    state.publish_play(play("Origin OK"), playing_now()).await;
    let mut req = url.clone().into_client_request().unwrap();
    req.headers_mut()
        .insert("Origin", "https://widget.rospies.dev".parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    let msg = recv_json(&mut ws).await;
    assert_eq!(msg["type"], "now_playing");
    assert_eq!(msg["title"], "Origin OK");
}

#[tokio::test]
async fn cors_allows_configured_origin_only() {
    let state = test_state();
    let addr = spawn_server(state).await;
    let client = reqwest::Client::new();
    let url = format!("http://{addr}/api/now-playing");

    let allowed = client
        .get(&url)
        .header("Origin", "https://rospies.dev")
        .send()
        .await
        .unwrap();
    assert_eq!(
        allowed
            .headers()
            .get("access-control-allow-origin")
            .and_then(|v| v.to_str().ok()),
        Some("https://rospies.dev")
    );

    let subdomain = client
        .get(&url)
        .header("Origin", "https://widget.rospies.dev")
        .send()
        .await
        .unwrap();
    assert_eq!(
        subdomain
            .headers()
            .get("access-control-allow-origin")
            .and_then(|v| v.to_str().ok()),
        Some("https://widget.rospies.dev")
    );

    let denied = client
        .get(&url)
        .header("Origin", "https://evil.com")
        .send()
        .await
        .unwrap();
    assert!(
        denied
            .headers()
            .get("access-control-allow-origin")
            .is_none()
    );
}

fn paused_at(position_ms: u64) -> nowplaying::events::Playback {
    nowplaying::events::Playback {
        state: nowplaying::events::PlaybackState::Paused,
        position_ms,
        as_of: chrono::Utc::now(),
    }
}

#[tokio::test]
async fn ws_clients_receive_state_frames() {
    let state = test_state();
    let addr = spawn_server(state.clone()).await;

    state.publish_play(play("Live"), playing_now()).await;
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    let replay = recv_json(&mut ws).await;
    assert_eq!(replay["type"], "now_playing");
    assert_eq!(replay["playback"]["state"], "playing");

    state
        .publish_state("4uLU6hMCjMI75M1A2tKUQC".into(), paused_at(83_000))
        .await;
    let msg = recv_json(&mut ws).await;
    assert_eq!(msg["type"], "state");
    assert_eq!(msg["track_id"], "4uLU6hMCjMI75M1A2tKUQC");
    assert_eq!(msg["playback"]["state"], "paused");
    assert_eq!(msg["playback"]["position_ms"], 83_000);
    assert!(msg.get("title").is_none(), "state frames stay light");
}

/// A client connecting AFTER a pause must get the frozen state in the replay
/// — the cached `now_playing` frame is re-serialized on state changes.
#[tokio::test]
async fn replay_and_api_reflect_state_published_after_the_play() {
    let state = test_state();
    let addr = spawn_server(state.clone()).await;

    state.publish_play(play("PauseMe"), playing_now()).await;
    state
        .publish_state("4uLU6hMCjMI75M1A2tKUQC".into(), paused_at(83_000))
        .await;

    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    let replay = recv_json(&mut ws).await;
    assert_eq!(replay["type"], "now_playing");
    assert_eq!(replay["title"], "PauseMe");
    assert_eq!(replay["playback"]["state"], "paused");
    assert_eq!(replay["playback"]["position_ms"], 83_000);

    let text = reqwest::get(format!("http://{addr}/api/now-playing"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(body["title"], "PauseMe");
    assert_eq!(body["playback"]["state"], "paused");
    assert_eq!(body["playback"]["position_ms"], 83_000);
}
