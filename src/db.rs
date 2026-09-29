use std::future::Future;
use std::sync::OnceLock;
use std::time::Instant;

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Histogram, ObservableGauge};
use serde::Serialize;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tracing::instrument;

use crate::events::{MediaKind, MetadataSource, PlayEvent};
use crate::spotify::metadata::TrackMeta;

const POOL_MAX_CONNECTIONS: u32 = 5;

/// Kept for the process lifetime so the observable callbacks stay registered
/// (same pattern as the `spotify_connected` gauge in spotify::Metrics).
static POOL_GAUGES: OnceLock<[ObservableGauge<u64>; 2]> = OnceLock::new();

/// Semconv boundaries for local-Postgres scale; recorded on success and
/// failure alike — a query that errors after seconds is exactly what this
/// histogram must show.
fn operation_duration() -> &'static Histogram<f64> {
    static HISTOGRAM: OnceLock<Histogram<f64>> = OnceLock::new();
    HISTOGRAM.get_or_init(|| {
        global::meter("nowplaying")
            .f64_histogram("db.client.operation.duration")
            .with_unit("s")
            .with_description("Postgres operation latency by operation name")
            .with_boundaries(vec![
                0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ])
            .build()
    })
}

async fn timed<T>(op: &'static str, fut: impl Future<Output = sqlx::Result<T>>) -> sqlx::Result<T> {
    let started = Instant::now();
    let result = fut.await;
    operation_duration().record(
        started.elapsed().as_secs_f64(),
        &[KeyValue::new("db.operation.name", op)],
    );
    result
}

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
    timed("insert_play", async {
        let result = sqlx::query(
        "INSERT INTO plays (track_id, title, album, artists, cover_url, duration_ms, started_at,
                            content_type, metadata_source)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         ON CONFLICT (track_id, started_at) DO NOTHING",
    )
    .bind(&event.track_id)
    .bind(&event.title)
    .bind(&event.album)
    .bind(&event.artists)
    .bind(&event.cover_url)
    .bind(event.duration_ms as i32)
    .bind(event.started_at)
    .bind(event.kind.as_str())
    .bind(event.metadata_source.as_str())
        .execute(pool)
        .await?;
        Ok(result.rows_affected() > 0)
    })
    .await
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
    timed("insert_play_guarded", async {
        let result = sqlx::query(
        "INSERT INTO plays (track_id, title, album, artists, cover_url, duration_ms, started_at,
                            content_type, metadata_source)
         SELECT $1, $2, $3, $4, $5, $6, $7, $9, $10
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
    .bind(event.kind.as_str())
    .bind(event.metadata_source.as_str())
        .execute(pool)
        .await?;
        Ok(result.rows_affected() > 0)
    })
    .await
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

/// Most-played tracks of the rolling window (podcast episodes excluded). The
/// per-track "latest metadata" lookup uses DISTINCT ON: array_agg over the
/// TEXT[] artists column would build a 2D array whose indexing yields NULL.
#[instrument(skip(pool))]
pub async fn top_songs(pool: &PgPool, limit: i64) -> sqlx::Result<Vec<TopSong>> {
    timed(
        "top_songs",
        sqlx::query_as(
            "WITH windowed AS (
             SELECT * FROM plays
             WHERE started_at >= now() - interval '30 days' AND content_type = 'track'
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
        .fetch_all(pool),
    )
    .await
}

/// Most-played artists of the rolling window, tracks only (an episode's
/// "artist" is its show).
#[instrument(skip(pool))]
pub async fn top_artists(pool: &PgPool, limit: i64) -> sqlx::Result<Vec<TopArtist>> {
    timed(
        "top_artists",
        sqlx::query_as(
            "SELECT unnest(artists) AS artist, count(*) AS plays
         FROM plays
         WHERE started_at >= now() - interval '30 days' AND content_type = 'track'
         GROUP BY artist
         ORDER BY plays DESC, artist
         LIMIT $1",
        )
        .bind(limit)
        .fetch_all(pool),
    )
    .await
}

#[derive(sqlx::FromRow)]
struct PlayRow {
    track_id: String,
    content_type: String,
    title: String,
    artists: Vec<String>,
    album: String,
    cover_url: Option<String>,
    duration_ms: i32,
    started_at: chrono::DateTime<chrono::Utc>,
    metadata_source: String,
}

/// The most recently started persisted play, for seeding the now-playing
/// cache at startup. Lyrics are never persisted, so the event carries none.
#[instrument(skip(pool))]
pub async fn latest_play(pool: &PgPool) -> sqlx::Result<Option<PlayEvent>> {
    let row: Option<PlayRow> = timed(
        "latest_play",
        sqlx::query_as(
            "SELECT track_id, content_type, title, artists, album, cover_url, duration_ms,
                    started_at, metadata_source
             FROM plays
             ORDER BY started_at DESC
             LIMIT 1",
        )
        .fetch_optional(pool),
    )
    .await?;
    Ok(row.map(|r| {
        let kind = MediaKind::from_db(&r.content_type).unwrap_or(MediaKind::Track);
        PlayEvent {
            track_url: kind.open_url(&r.track_id),
            track_id: r.track_id,
            kind,
            title: r.title,
            artists: r.artists,
            album: r.album,
            cover_url: r.cover_url,
            duration_ms: r.duration_ms.max(0) as u32,
            started_at: r.started_at,
            lyrics: None,
            metadata_source: if r.metadata_source == "fetch" {
                MetadataSource::Fetch
            } else {
                MetadataSource::ClusterMap
            },
        }
    }))
}

/// A track whose persisted rows carry degraded cluster-map metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DegradedTrack {
    pub track_id: String,
    pub kind: MediaKind,
}

/// Distinct degraded tracks awaiting the metadata repair pass.
#[instrument(skip(pool))]
pub async fn degraded_tracks(pool: &PgPool, limit: i64) -> sqlx::Result<Vec<DegradedTrack>> {
    let rows: Vec<(String, String)> = timed(
        "degraded_tracks",
        sqlx::query_as(
            "SELECT DISTINCT track_id, content_type
             FROM plays
             WHERE metadata_source = 'cluster_map'
             ORDER BY track_id
             LIMIT $1",
        )
        .bind(limit)
        .fetch_all(pool),
    )
    .await?;
    Ok(rows
        .into_iter()
        .map(|(track_id, content_type)| DegradedTrack {
            track_id,
            kind: MediaKind::from_db(&content_type).unwrap_or(MediaKind::Track),
        })
        .collect())
}

/// Overwrite every degraded row of a track with a full record, re-labelling
/// its kind (rows persisted before kinds existed may be episodes). Returns
/// the number of rows repaired.
#[instrument(skip(pool, meta), fields(track_id = %track_id, media.kind = kind.as_str()))]
pub async fn repair_metadata(
    pool: &PgPool,
    track_id: &str,
    kind: MediaKind,
    meta: &TrackMeta,
) -> sqlx::Result<u64> {
    timed("repair_metadata", async {
        let result = sqlx::query(
            "UPDATE plays
             SET title = $2, album = $3, artists = $4, cover_url = $5, duration_ms = $6,
                 content_type = $7, metadata_source = 'fetch'
             WHERE track_id = $1 AND metadata_source = 'cluster_map'",
        )
        .bind(track_id)
        .bind(&meta.title)
        .bind(&meta.album)
        .bind(&meta.artists)
        .bind(&meta.cover_url)
        .bind(meta.duration_ms as i32)
        .bind(kind.as_str())
        .execute(pool)
        .await?;
        Ok(result.rows_affected())
    })
    .await
}

/// Retire a degraded track that Spotify no longer knows under any kind, so
/// the repair pass stops retrying it. Its rows keep their degraded metadata.
#[instrument(skip(pool))]
pub async fn mark_unresolvable(pool: &PgPool, track_id: &str) -> sqlx::Result<u64> {
    timed("mark_unresolvable", async {
        let result = sqlx::query(
            "UPDATE plays SET metadata_source = 'unresolvable'
             WHERE track_id = $1 AND metadata_source = 'cluster_map'",
        )
        .bind(track_id)
        .execute(pool)
        .await?;
        Ok(result.rows_affected())
    })
    .await
}
