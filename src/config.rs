use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;

/// Spotify now-playing backend: pushes plays over WSS, serves 30-day top lists.
#[derive(Parser, Debug, Clone)]
#[command(name = "nowplaying", version)]
pub struct Config {
    /// Address the plain-HTTP server binds to. TLS terminates at the nginx
    /// in front (deploy/nginx-nowplaying.conf); keep this on loopback so the
    /// X-Forwarded-For header can only come from that trusted proxy.
    #[arg(long, env = "NP_BIND_ADDR", default_value = "127.0.0.1:8080")]
    pub bind_addr: SocketAddr,

    /// Postgres connection string. Required except for --login.
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: Option<String>,

    /// Device name shown in the Spotify Connect device list.
    #[arg(long, env = "NP_DEVICE_NAME", default_value = "NowPlaying Widget")]
    pub device_name: String,

    /// Directory for the librespot credentials cache.
    #[arg(
        long,
        env = "NP_CACHE_DIR",
        default_value = "/var/lib/nowplaying/cache"
    )]
    pub cache_dir: PathBuf,

    /// OTLP gRPC endpoint for traces, metrics and logs.
    #[arg(
        long,
        env = "OTEL_EXPORTER_OTLP_ENDPOINT",
        default_value = "http://localhost:4317"
    )]
    pub otlp_endpoint: String,

    /// Value for the OTEL service.name resource attribute.
    #[arg(long, env = "OTEL_SERVICE_NAME", default_value = "nowplaying")]
    pub service_name: String,

    /// Comma-separated list of allowed CORS origins. `*` allows any origin.
    #[arg(
        long,
        env = "NP_ALLOWED_ORIGINS",
        default_value = "https://rospies.dev,https://*.rospies.dev",
        value_delimiter = ','
    )]
    pub allowed_origins: Vec<String>,

    /// Minimum listen time before a play is persisted for the top lists.
    #[arg(long, env = "NP_MIN_PLAY_MS", default_value_t = 30_000)]
    pub min_play_ms: u64,

    /// Default `limit` for /api/top.
    #[arg(long, env = "NP_TOP_LIMIT", default_value_t = 10)]
    pub top_default_limit: u32,

    /// Maximum concurrent WebSocket sessions; further upgrades get 503.
    #[arg(long, env = "NP_WS_MAX_CONNECTIONS", default_value_t = 1000)]
    pub ws_max_connections: usize,

    /// Maximum concurrent WebSocket sessions per client address (as
    /// forwarded by nginx); further upgrades from it get 429.
    #[arg(long, env = "NP_WS_MAX_PER_IP", default_value_t = 8)]
    pub ws_max_per_ip: usize,

    /// tracing filter directives (also honours RUST_LOG).
    #[arg(long, env = "RUST_LOG", default_value = "info,librespot=warn")]
    pub log_filter: String,

    /// Run the initial interactive OAuth flow and exit (first-run setup).
    #[arg(long, default_value_t = false)]
    pub login: bool,
}
