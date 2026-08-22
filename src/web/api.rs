use std::sync::OnceLock;

use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::Counter;
use serde::{Deserialize, Serialize};
use tracing::{error, instrument};

use crate::db::{self, TopArtist, TopSong};
use crate::events::{PlayEvent, Playback};
use crate::state::AppState;

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
pub struct TopResponse {
    songs: Vec<TopSong>,
    artists: Vec<TopArtist>,
}

/// Most-played songs and artists of the rolling 30-day window. The queries
/// run concurrently via `join!` (not `try_join!`) so failures keep their
/// per-query attribution in the `db_errors_total` metric.
#[instrument(skip_all)]
pub async fn top(State(state): State<AppState>, Query(params): Query<TopParams>) -> Response {
    let limit = params
        .limit
        .unwrap_or(state.cfg.top_default_limit)
        .clamp(1, 50) as i64;

    let (songs, artists) = tokio::join!(
        db::top_songs(&state.db, limit),
        db::top_artists(&state.db, limit),
    );
    for (result, op) in [
        (songs.is_err(), "top_songs"),
        (artists.is_err(), "top_artists"),
    ] {
        if result {
            db_errors().add(1, &[KeyValue::new("op", op)]);
        }
    }
    match (songs, artists) {
        (Ok(songs), Ok(artists)) => Json(TopResponse { songs, artists }).into_response(),
        (Err(e), _) | (_, Err(e)) => {
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
    db_ok: bool,
}

pub async fn healthz(State(state): State<AppState>) -> Response {
    let db_ok = sqlx::query("SELECT 1").execute(&state.db).await.is_ok();
    let health = Health {
        spotify_connected: state.is_spotify_connected(),
        db_ok,
    };
    let status = if health.db_ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(health)).into_response()
}
