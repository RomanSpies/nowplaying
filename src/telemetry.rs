use anyhow::Context;
use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{LogExporter, MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

use crate::config::Config;

/// Baked at compile time: CI sets `NP_BUILD_VERSION` to the CalVer release
/// version (see .gitlab-ci.yml); local builds fall back to the crate version.
/// Becomes the `service.version` resource attribute on all three signals.
pub const SERVICE_VERSION: &str = match option_env!("NP_BUILD_VERSION") {
    Some(v) => v,
    None => env!("CARGO_PKG_VERSION"),
};

/// Handles for the three OTEL providers; shut down in order on exit.
pub struct Telemetry {
    tracer_provider: SdkTracerProvider,
    logger_provider: SdkLoggerProvider,
    meter_provider: SdkMeterProvider,
}

/// Build the three OTLP providers and wire the tracing registry. The OTLP
/// exporters themselves speak gRPC via tonic/hyper/h2, so those crates are
/// filtered off the OTEL-bound layers — their own tracing events would
/// otherwise re-trigger exports in a feedback loop.
pub fn init(cfg: &Config) -> anyhow::Result<Telemetry> {
    let resource = Resource::builder()
        .with_service_name(cfg.service_name.clone())
        .with_attribute(KeyValue::new("service.version", SERVICE_VERSION))
        .build();

    let span_exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(cfg.otlp_endpoint.clone())
        .build()
        .context("building OTLP span exporter")?;
    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_resource(resource.clone())
        .build();

    let metric_exporter = MetricExporter::builder()
        .with_tonic()
        .with_endpoint(cfg.otlp_endpoint.clone())
        .build()
        .context("building OTLP metric exporter")?;
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(metric_exporter)
        .with_resource(resource.clone())
        .build();

    let log_exporter = LogExporter::builder()
        .with_tonic()
        .with_endpoint(cfg.otlp_endpoint.clone())
        .build()
        .context("building OTLP log exporter")?;
    let logger_provider = SdkLoggerProvider::builder()
        .with_batch_exporter(log_exporter)
        .with_resource(resource)
        .build();

    global::set_tracer_provider(tracer_provider.clone());
    global::set_meter_provider(meter_provider.clone());

    let tracer = tracer_provider.tracer("nowplaying");

    let export_noise = "hyper=off,tonic=off,h2=off,tower=off,reqwest=off,opentelemetry=off";
    let log_bridge = OpenTelemetryTracingBridge::new(&logger_provider)
        .with_filter(EnvFilter::new(format!("{},{export_noise}", cfg.log_filter)));

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(EnvFilter::new(cfg.log_filter.clone())))
        .with(
            tracing_opentelemetry::layer()
                .with_tracer(tracer)
                .with_filter(EnvFilter::new(format!("{},{export_noise}", cfg.log_filter))),
        )
        .with(log_bridge)
        .try_init()
        .context("installing tracing subscriber")?;

    Ok(Telemetry {
        tracer_provider,
        logger_provider,
        meter_provider,
    })
}

impl Telemetry {
    /// Flush and shut down all providers. Order matters: traces first, the
    /// meter last (the log batch processor emits self-diagnostic metrics).
    pub fn shutdown(self) {
        if let Err(e) = self.tracer_provider.shutdown() {
            eprintln!("tracer provider shutdown: {e}");
        }
        if let Err(e) = self.logger_provider.shutdown() {
            eprintln!("logger provider shutdown: {e}");
        }
        if let Err(e) = self.meter_provider.shutdown() {
            eprintln!("meter provider shutdown: {e}");
        }
    }
}
