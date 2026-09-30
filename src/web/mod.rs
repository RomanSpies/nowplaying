pub mod api;
pub mod top_cache;
pub mod ws;
pub mod ws_limits;

use std::net::{IpAddr, SocketAddr};
use std::sync::OnceLock;
use std::time::Instant;

use anyhow::Context;
use axum::Router;
use axum::extract::{ConnectInfo, MatchedPath, Request};
use axum::http::{HeaderMap, HeaderValue, Method};
use axum::middleware::Next;
use axum::response::Response;
use axum::routing::get;
use axum_server::Handle;
use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::Histogram;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::{info, info_span};

use crate::state::AppState;

/// Visitor IP behind the nginx that terminates TLS: the RIGHTMOST
/// X-Forwarded-For entry is the one our own proxy appended and the only part
/// of the header a client cannot forge. Only a syntactically valid address
/// is accepted (it keys the per-IP WebSocket limit, so free-form strings must
/// not mint fresh buckets); anything else falls back to the socket peer, as
/// does direct access (e.g. healthz probes against the loopback bind).
pub(crate) fn client_ip(headers: &HeaderMap, peer: Option<IpAddr>) -> String {
    headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.rsplit(',').next())
        .and_then(|ip| ip.trim().parse::<IpAddr>().ok())
        .or(peer)
        .map(|ip| ip.to_string())
        .unwrap_or_default()
}

/// Boundaries are the semconv recommendation for
/// `http.server.request.duration`.
fn request_duration() -> &'static Histogram<f64> {
    static HISTOGRAM: OnceLock<Histogram<f64>> = OnceLock::new();
    HISTOGRAM.get_or_init(|| {
        global::meter("nowplaying")
            .f64_histogram("http.server.request.duration")
            .with_unit("s")
            .with_description("HTTP request duration by route/method/status")
            .with_boundaries(vec![
                0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0,
            ])
            .build()
    })
}

/// Every attribute is drawn from a bounded set — this must never become a
/// cardinality bomb: route is the matched TEMPLATE (scanner paths collapse to
/// "unmatched"), the method is normalized to the RFC set, status is numeric.
async fn track_requests(request: Request, next: Next) -> Response {
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str().to_owned())
        .unwrap_or_else(|| "unmatched".to_owned());
    let method = normalized_method(request.method());
    let started = Instant::now();

    let response = next.run(request).await;

    request_duration().record(
        started.elapsed().as_secs_f64(),
        &[
            KeyValue::new("http.route", route),
            KeyValue::new("http.request.method", method),
            KeyValue::new(
                "http.response.status_code",
                response.status().as_u16() as i64,
            ),
        ],
    );
    response
}

/// Arbitrary method tokens are valid HTTP; collapse everything non-standard
/// to `_OTHER` (semconv convention) so scanners cannot mint metric series.
fn normalized_method(method: &Method) -> &'static str {
    match *method {
        Method::GET => "GET",
        Method::HEAD => "HEAD",
        Method::POST => "POST",
        Method::PUT => "PUT",
        Method::DELETE => "DELETE",
        Method::OPTIONS => "OPTIONS",
        Method::CONNECT => "CONNECT",
        Method::PATCH => "PATCH",
        Method::TRACE => "TRACE",
        _ => "_OTHER",
    }
}

/// Routes that get metrics but no trace: health probes hit them every few
/// seconds and would bury the real traffic in identical traces.
const UNTRACED_ROUTES: &[&str] = &["/healthz"];

/// Router with CORS, request-span tracing and the duration middleware. The
/// request span carries the matched route template plus the visitor address
/// as forwarded by nginx (the socket peer is just the local proxy); per
/// semconv, only 5xx responses mark the span as ERROR — 4xx is the client's
/// problem, not a failed server span. [`UNTRACED_ROUTES`] get a disabled
/// span (recording into it is a no-op) but still feed
/// `http.server.request.duration`.
pub fn router(state: AppState) -> Router {
    let cors = CorsLayer::new()
        .allow_methods(Any)
        .allow_origin(allow_origin(&state.cfg.allowed_origins));

    let trace = TraceLayer::new_for_http()
        .make_span_with(|request: &Request<_>| {
            let route = request
                .extensions()
                .get::<MatchedPath>()
                .map(|p| p.as_str())
                .unwrap_or("unmatched");
            if UNTRACED_ROUTES.contains(&route) {
                return tracing::Span::none();
            }
            let client = client_ip(
                request.headers(),
                request
                    .extensions()
                    .get::<ConnectInfo<SocketAddr>>()
                    .map(|ConnectInfo(addr)| addr.ip()),
            );
            info_span!(
                "request",
                "http.request.method" = %request.method(),
                "url.path" = %request.uri().path(),
                "http.route" = route,
                "client.address" = %client,
                "http.response.status_code" = tracing::field::Empty,
                "otel.status_code" = tracing::field::Empty,
            )
        })
        .on_response(
            |response: &axum::http::Response<_>,
             _latency: std::time::Duration,
             span: &tracing::Span| {
                span.record("http.response.status_code", response.status().as_u16());
                if response.status().is_server_error() {
                    span.record("otel.status_code", "ERROR");
                }
            },
        );

    Router::new()
        .route("/ws", get(ws::handler))
        .route("/api/top", get(api::top))
        .route("/api/now-playing", get(api::now_playing))
        .route("/healthz", get(api::healthz))
        .layer(cors)
        .layer(trace)
        .layer(axum::middleware::from_fn(track_requests))
        .with_state(state)
}

fn allow_origin(patterns: &[String]) -> AllowOrigin {
    if patterns.iter().any(|p| p == "*") {
        return AllowOrigin::any();
    }
    let patterns = patterns.to_vec();
    AllowOrigin::predicate(move |origin: &HeaderValue, _| {
        origin
            .to_str()
            .map(|o| origin_allowed(o, &patterns))
            .unwrap_or(false)
    })
}

/// Exact match, or wildcard subdomain patterns like `https://*.rospies.dev`,
/// where the wildcard must cover at least one character and may not span past
/// the host. Callers must handle the `"*"` allow-all pattern themselves (it
/// short-circuits before this check, see `allow_origin`).
pub(crate) fn origin_allowed(origin: &str, patterns: &[String]) -> bool {
    patterns
        .iter()
        .any(|pattern| match pattern.split_once("*") {
            None => origin == pattern,
            Some((prefix, suffix)) => {
                origin.starts_with(prefix)
                    && origin.ends_with(suffix)
                    && origin.len() > prefix.len() + suffix.len()
                    && !origin[prefix.len()..origin.len() - suffix.len()].contains('/')
            }
        })
}

/// Serve plain HTTP until `handle.graceful_shutdown` is called. TLS, the
/// public hostname and cert rotation all live in the nginx in front
/// (deploy/nginx-nowplaying.conf).
pub async fn serve(state: AppState, handle: Handle<std::net::SocketAddr>) -> anyhow::Result<()> {
    let addr = state.cfg.bind_addr;
    info!(addr = %addr, "listening for HTTP behind nginx");
    axum_server::bind(addr)
        .handle(handle)
        .serve(router(state).into_make_service_with_connect_info::<SocketAddr>())
        .await
        .context("http server failed")
}

#[cfg(test)]
mod tests {
    use super::{client_ip, origin_allowed};
    use axum::http::HeaderMap;

    #[test]
    fn client_ip_trusts_only_the_proxy_appended_entry() {
        let peer = Some("127.0.0.1".parse().unwrap());

        assert_eq!(client_ip(&HeaderMap::new(), peer), "127.0.0.1");
        assert_eq!(client_ip(&HeaderMap::new(), None), "");

        let mut h = HeaderMap::new();
        h.insert("x-forwarded-for", "203.0.113.7".parse().unwrap());
        assert_eq!(client_ip(&h, peer), "203.0.113.7");

        h.insert(
            "x-forwarded-for",
            "6.6.6.6, 198.51.100.9 , 203.0.113.7".parse().unwrap(),
        );
        assert_eq!(client_ip(&h, peer), "203.0.113.7");

        h.insert("x-forwarded-for", "203.0.113.7, not-an-ip".parse().unwrap());
        assert_eq!(
            client_ip(&h, peer),
            "127.0.0.1",
            "garbage falls back to the peer"
        );

        h.insert("x-forwarded-for", "2001:db8::1".parse().unwrap());
        assert_eq!(client_ip(&h, peer), "2001:db8::1");
    }

    #[test]
    fn origin_matching() {
        let patterns = vec![
            "https://rospies.dev".to_string(),
            "https://*.rospies.dev".to_string(),
        ];
        assert!(origin_allowed("https://rospies.dev", &patterns));
        assert!(origin_allowed("https://www.rospies.dev", &patterns));
        assert!(origin_allowed("https://a.b.rospies.dev", &patterns));
        assert!(!origin_allowed("https://rospies.dev.evil.com", &patterns));
        assert!(!origin_allowed("https://evilrospies.dev", &patterns));
        assert!(!origin_allowed("http://rospies.dev", &patterns));
        assert!(!origin_allowed("https://evil.com/x.rospies.dev", &patterns));
    }
}
