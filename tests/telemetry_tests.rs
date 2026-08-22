//! Smoke test in its own binary: `telemetry::init` installs process-global
//! providers plus the tracing subscriber, which can only happen once per
//! process. Catches wiring regressions (EnvFilter parse of the default
//! filter + export-noise suffix, double provider registration, shutdown
//! ordering) without needing a collector — the OTLP endpoint is unreachable
//! and the batch exporters fail fast on loopback connection-refused.

use clap::Parser;
use nowplaying::config::Config;
use nowplaying::telemetry;

/// Emits one event on each of the three signals, then shuts down. Flushes to
/// the unreachable endpoint must fail gracefully — errors are reported to
/// stderr by the providers, with no hang and no panic.
#[tokio::test]
async fn init_emit_and_shutdown_survive_without_collector() {
    let cfg = Config::parse_from([
        "nowplaying",
        "--bind-addr",
        "127.0.0.1:0",
        "--device-name",
        "test",
        "--cache-dir",
        "/tmp/unused",
        "--otlp-endpoint",
        "http://127.0.0.1:9",
        "--service-name",
        "telemetry-smoke",
        "--allowed-origins",
        "https://rospies.dev",
        "--min-play-ms",
        "30000",
        "--top-default-limit",
        "10",
        "--log-filter",
        "info,librespot=warn",
    ]);

    let telemetry = telemetry::init(&cfg).expect("telemetry init");

    let span = tracing::info_span!("smoke.span", value = 1);
    span.in_scope(|| tracing::info!("telemetry smoke event"));
    opentelemetry::global::meter("nowplaying")
        .u64_counter("smoke_total")
        .build()
        .add(1, &[]);

    telemetry.shutdown();
}
