//! Asserts that the instrumented code paths actually record their OTEL
//! metrics — a forgotten `.add()`/`.record()` is invisible to every other
//! test. Own test binary on purpose: the global meter provider is
//! per-process, and the `OnceLock`-cached instruments must be created AFTER
//! the in-memory provider is installed.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use futures_util::StreamExt;
use nowplaying::config::Config;
use nowplaying::events::PlayEvent;
use nowplaying::spotify::pending::{DiscardReason, PendingPersist, PersistFn};
use nowplaying::state::AppState;
use nowplaying::web;
use opentelemetry::global;
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};

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
        .acquire_timeout(Duration::from_millis(200))
        .connect_lazy("postgres://unused@127.0.0.1:1/unused")
        .expect("lazy pool");
    AppState::new(cfg, pool)
}

fn sample_play() -> PlayEvent {
    PlayEvent {
        track_id: "4uLU6hMCjMI75M1A2tKUQC".into(),
        track_url: "https://open.spotify.com/track/4uLU6hMCjMI75M1A2tKUQC".into(),
        title: "T".into(),
        artists: vec!["A".into()],
        album: "Al".into(),
        cover_url: None,
        duration_ms: 213_000,
        started_at: "2026-07-29T12:00:00Z".parse().unwrap(),
        lyrics: None,
    }
}

/// Assertions match against the Debug dump of the exported metrics instead of
/// walking the data model: the SDK's metric-data API churns between minor
/// versions, the rendered content does not. Good enough to prove "this
/// instrument recorded with this attribute" — value-level assertions live
/// with the code paths' logic tests.
#[tokio::test]
async fn instruments_record_on_their_code_paths() {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    global::set_meter_provider(provider.clone());

    let state = test_state();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let service = web::router(state.clone()).into_make_service_with_connect_info::<SocketAddr>();
    tokio::spawn(async move {
        axum::serve(listener, service).await.unwrap();
    });

    let resp = reqwest::get(format!("http://{addr}/healthz"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 503);
    let resp = reqwest::get(format!("http://{addr}/api/top"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 500);

    let playback = nowplaying::events::Playback {
        state: nowplaying::events::PlaybackState::Playing,
        position_ms: 0,
        as_of: chrono::Utc::now(),
    };
    state.publish_play(sample_play(), playback).await;
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("replay frame")
        .unwrap()
        .unwrap();
    assert!(msg.is_text());
    ws.close(None).await.ok();

    let discarded = global::meter("nowplaying")
        .u64_counter("plays_discarded_total")
        .with_description("Detected plays dropped before the min-play threshold")
        .build();
    let persist: PersistFn = Arc::new(|_| Box::pin(async {}));
    let pending = PendingPersist::new(
        sample_play(),
        false,
        Duration::from_secs(3600),
        persist,
        discarded,
    );
    pending.discard(DiscardReason::Superseded);

    provider.force_flush().expect("flush metrics");
    let finished = exporter.get_finished_metrics().expect("exported metrics");
    let dump = format!("{finished:?}");
    for needle in [
        "http.server.request.duration",
        "/healthz",
        "db_errors_total",
        "top_songs",
        "top_artists",
        "ws_connections_active",
        "ws_messages_sent_total",
        "plays_discarded_total",
        "superseded",
    ] {
        assert!(
            dump.contains(needle),
            "expected exported metrics to contain {needle}"
        );
    }
}
