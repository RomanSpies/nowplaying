use std::sync::OnceLock;

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::ObservableGauge;
use serde::Serialize;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tracing::instrument;

use crate::events::PlayEvent;

const POOL_MAX_CONNECTIONS: u32 = 5;

/// Kept for the process lifetime so the observable callbacks stay registered
/// (same pattern as the `spotify_connected` gauge in spotify::Metrics).
static POOL_GAUGES: OnceLock<[ObservableGauge<u64>; 2]> = OnceLock::new();

/// Pool saturation is otherwise invisible: an exhausted pool (all
/// POOL_MAX_CONNECTIONS in use) shows up only as request latency. Semconv
/// names so dashboards match other services.
fn register_pool_metrics(pool: &PgPool) {
    POOL_GAUGES.get_or_init(|| {
        let meter = global::meter("nowplaying");
        let p = pool.clone();
        let count = meter
            .u64_observable_gauge("db.client.connection.count")
            .with_description("Open pool connections by state")
            .with_callback(move |o| {
                let size = p.size() as u64;
                let idle = p.num_idle() as u64;
                o.observe(
                    idle.min(size),
                    &[KeyValue::new("db.client.connection.state", "idle")],
                );
                o.observe(
                    size.saturating_sub(idle),
                    &[KeyValue::new("db.client.connection.state", "used")],
                );
            })
            .build();
        let max = meter
            .u64_observable_gauge("db.client.connection.max")
            .with_description("Configured pool connection limit")
            .with_callback(|o| o.observe(POOL_MAX_CONNECTIONS as u64, &[]))
            .build();
        [count, max]
    });
}

pub async fn connect(database_url: &str) -> anyhow::Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(POOL_MAX_CONNECTIONS)
        .connect(database_url)
        .await?;
    sqlx::migrate!("./migrations").run(&pool).await?;
    register_pool_metrics(&pool);
    Ok(pool)
}

/// Insert a play. Idempotent: the (track_id, started_at) unique constraint
/// swallows replays after reconnects. Returns whether a row was written.
#[instrument(skip_all, fields(track_id = %event.track_id))]
pub async fn insert_play(pool: &PgPool, event: &PlayEvent) -> sqlx::Result<bool> {
    let result = sqlx::query(
        "INSERT INTO plays (track_id, title, album, artists, cover_url, duration_ms, started_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (track_id, started_at) DO NOTHING",
    )
    .bind(&event.track_id)
    .bind(&event.title)
    .bind(&event.album)
    .bind(&event.artists)
    .bind(&event.cover_url)
    .bind(event.duration_ms as i32)
    .bind(event.started_at)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Insert a play unless one of the same track already exists within
/// ±`guard_secs` of `started_at` (reconnect replay guard). Guard check and
/// insert are a single statement, so concurrent persist tasks cannot race a
/// separate SELECT against the INSERT. Returns whether a row was written.
#[instrument(skip_all, fields(track_id = %event.track_id))]
pub async fn insert_play_guarded(
    pool: &PgPool,
    event: &PlayEvent,
    guard_secs: f64,
) -> sqlx::Result<bool> {
    let result = sqlx::query(
        "INSERT INTO plays (track_id, title, album, artists, cover_url, duration_ms, started_at)
         SELECT $1, $2, $3, $4, $5, $6, $7
         WHERE NOT EXISTS (
             SELECT 1 FROM plays
             WHERE track_id = $1
               AND started_at BETWEEN $7 - make_interval(secs => $8)
                                  AND $7 + make_interval(secs => $8)
         )
         ON CONFLICT (track_id, started_at) DO NOTHING",
    )
    .bind(&event.track_id)
    .bind(&event.title)
    .bind(&event.album)
    .bind(&event.artists)
    .bind(&event.cover_url)
    .bind(event.duration_ms as i32)
    .bind(event.started_at)
    .bind(guard_secs)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct TopSong {
    pub track_id: String,
    pub track_url: String,
    pub title: String,
    pub artists: Vec<String>,
    pub album: String,
    pub cover_url: Option<String>,
    pub plays: i64,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct TopArtist {
    pub artist: String,
    pub plays: i64,
}

/// Most-played tracks of the rolling window. The per-track "latest metadata"
/// lookup uses DISTINCT ON: array_agg over the TEXT[] artists column would
/// build a 2D array whose indexing yields NULL.
#[instrument(skip(pool))]
pub async fn top_songs(pool: &PgPool, limit: i64) -> sqlx::Result<Vec<TopSong>> {
    sqlx::query_as(
        "WITH windowed AS (
             SELECT * FROM plays WHERE started_at >= now() - interval '30 days'
         ),
         counts AS (
             SELECT track_id, count(*) AS plays FROM windowed GROUP BY track_id
         ),
         latest AS (
             SELECT DISTINCT ON (track_id)
                    track_id, title, artists, album, cover_url
             FROM windowed
             ORDER BY track_id, started_at DESC
         )
         SELECT c.track_id,
                'https://open.spotify.com/track/' || c.track_id AS track_url,
                l.title, l.artists, l.album, l.cover_url,
                c.plays
         FROM counts c
         JOIN latest l USING (track_id)
         ORDER BY c.plays DESC, c.track_id
         LIMIT $1",
    )
    .bind(limit)
    .fetch_all(pool)
    .await
}

#[instrument(skip(pool))]
pub async fn top_artists(pool: &PgPool, limit: i64) -> sqlx::Result<Vec<TopArtist>> {
    sqlx::query_as(
        "SELECT unnest(artists) AS artist, count(*) AS plays
         FROM plays
         WHERE started_at >= now() - interval '30 days'
         GROUP BY artist
         ORDER BY plays DESC, artist
         LIMIT $1",
    )
    .bind(limit)
    .fetch_all(pool)
    .await
}
