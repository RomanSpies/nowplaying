use std::sync::OnceLock;

use axum::Json;
use axum::extract::{Query, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::Counter;
use serde::{Deserialize, Serialize};
use tracing::{error, instrument};

use crate::db::{self, TopArtist, TopSong};
use crate::events::{PlayEvent, Playback};
use crate::state::AppState;
use crate::web::top_cache::{TOP_CACHE_DEPTH, TOP_CACHE_TTL, TopLists};

/// Same instrument as in `spotify::Metrics` — name and description must
/// match byte-for-byte or the SDK reports a duplicate-instrument conflict.
fn db_errors() -> &'static Counter<u64> {
    static COUNTER: OnceLock<Counter<u64>> = OnceLock::new();
    COUNTER.get_or_init(|| {
        global::meter("nowplaying")
            .u64_counter("db_errors_total")
            .with_description("Failed Postgres operations")
            .build()
    })
}

#[derive(Deserialize)]
pub struct TopParams {
    limit: Option<u32>,
}

#[derive(Serialize)]
struct TopResponse<'a> {
    songs: &'a [TopSong],
    artists: &'a [TopArtist],
}

/// Most-played songs and artists of the rolling 30-day window, served from
/// the [`crate::web::top_cache::TopCache`] (one snapshot at full depth,
/// sliced per `limit`). A reload runs both queries concurrently via `join!`
/// (not `try_join!`) so failures keep their per-query attribution in the
/// `db_errors_total` metric. Browsers may reuse a response for the cache TTL.
#[instrument(
    name = "api.top",
    skip_all,
    fields(limit, cache = tracing::field::Empty, cache.age_ms = tracing::field::Empty)
)]
pub async fn top(State(state): State<AppState>, Query(params): Query<TopParams>) -> Response {
    let limit = params
        .limit
        .unwrap_or(state.cfg.top_default_limit)
        .clamp(1, TOP_CACHE_DEPTH as u32) as usize;
    let span = tracing::Span::current();
    span.record("limit", limit);

    let served = state
        .top_cache
        .get_or_load(|| async {
            let (songs, artists) = tokio::join!(
                db::top_songs(&state.db, TOP_CACHE_DEPTH),
                db::top_artists(&state.db, TOP_CACHE_DEPTH),
            );
            for (failed, op) in [
                (songs.is_err(), "top_songs"),
                (artists.is_err(), "top_artists"),
            ] {
                if failed {
                    db_errors().add(1, &[KeyValue::new("op", op)]);
                }
            }
            Ok::<_, sqlx::Error>(TopLists {
                songs: songs?,
                artists: artists?,
            })
        })
        .await;

    match served {
        Ok(served) => {
            span.record("cache", served.outcome.as_str());
            span.record("cache.age_ms", served.age.as_millis() as u64);
            let lists = &served.lists;
            let body = TopResponse {
                songs: &lists.songs[..limit.min(lists.songs.len())],
                artists: &lists.artists[..limit.min(lists.artists.len())],
            };
            let cache_control =
                HeaderValue::from_str(&format!("public, max-age={}", TOP_CACHE_TTL.as_secs()))
                    .expect("static header value");
            ([(header::CACHE_CONTROL, cache_control)], Json(body)).into_response()
        }
        Err(e) => {
            error!("top query failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

#[derive(Serialize)]
struct NowPlayingResponse<'a> {
    #[serde(flatten)]
    event: &'a PlayEvent,
    /// Same semantics as the WS frames: extrapolate `position_ms` from
    /// `as_of` while playing, frozen otherwise.
    playback: Playback,
}

/// The most recent play incl. live playback, so the widget can render the
/// true state without waiting for a WebSocket event. 204 until the first
/// play after startup.
pub async fn now_playing(State(state): State<AppState>) -> Response {
    match state.last_play.read().await.as_ref() {
        Some(lp) => Json(NowPlayingResponse {
            event: &lp.event,
            playback: lp.playback,
        })
        .into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    }
}

#[derive(Serialize)]
pub struct Health {
    spotify_connected: bool,
    spotify_auth_ok: bool,
    db_ok: bool,
}

/// 503 when the database is down or Spotify rejected the stored credentials
/// — both need an operator. A transient Spotify reconnect stays 200
/// (`spotify_connected: false`), since the service recovers on its own.
pub async fn healthz(State(state): State<AppState>) -> Response {
    let db_ok = sqlx::query("SELECT 1").execute(&state.db).await.is_ok();
    let health = Health {
        spotify_connected: state.is_spotify_connected(),
        spotify_auth_ok: !state.is_spotify_auth_failed(),
        db_ok,
    };
    let status = if health.db_ok && health.spotify_auth_ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(health)).into_response()
}
