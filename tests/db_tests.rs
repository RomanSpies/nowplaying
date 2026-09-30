//! SQL tests against a real (embedded) PostgreSQL.
//!
//! `postgresql_embedded` downloads portable Postgres binaries on first run
//! (cached in ~/.theseus/postgresql-zonky) and manages a throwaway instance
//! per test — no Docker or system installation required. The zonky archives
//! are self-contained (they bundle libxml2, libicu, … in their own lib/), so
//! no system libraries are needed on the host.

use std::net::SocketAddr;
use std::sync::Arc;

use chrono::{Duration, Utc};
use clap::Parser;
use nowplaying::config::Config;
use nowplaying::db;
use nowplaying::events::{MediaKind, MetadataSource, PlayEvent};
use nowplaying::spotify::lyrics::LyricsFetch;
use nowplaying::spotify::metadata::{FetchError, FetchTrack, MetadataResolver, TrackMeta};
use nowplaying::spotify::pending::{DiscardReason, PendingPersist, PersistFn};
use nowplaying::spotify::repair_degraded;
use nowplaying::state::AppState;
use nowplaying::web;
use postgresql_archive::configuration::zonky;
use postgresql_embedded::{PostgreSQL, Settings, VersionReq};
use sqlx::PgPool;

/// Start an embedded Postgres and hand back a migrated pool. The `PostgreSQL`
/// guard must stay alive for the duration of the test (Drop stops the server).
///
/// With only the zonky feature enabled the default `releases_url` is empty
/// and must be set explicitly. The install dir is kept separate from the
/// theseus layout: both share the version-numbered structure, and a stale
/// theseus download would be reused instead of the self-contained zonky
/// build.
async fn setup() -> (PostgreSQL, PgPool) {
    let mut settings = Settings::default();
    settings.releases_url = zonky::URL.to_string();
    settings.version = VersionReq::parse("=18.4.0").expect("version req");
    settings.installation_dir = settings.installation_dir.with_file_name("postgresql-zonky");
    let mut pg = PostgreSQL::new(settings);
    pg.setup()
        .await
        .expect("postgres setup (downloads binaries on first run)");
    pg.start().await.expect("postgres start");
    pg.create_database("nowplaying_test")
        .await
        .expect("create db");
    let pool = PgPool::connect(&pg.settings().url("nowplaying_test"))
        .await
        .expect("connect");
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("migrations");
    (pg, pool)
}

/// Every env-backed field is pinned via CLI flag (CLI beats env in clap).
fn test_config() -> Config {
    Config::parse_from([
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
        "--ws-max-connections",
        "1000",
        "--ws-max-per-ip",
        "8",
        "--log-filter",
        "info",
    ])
}

async fn serve(state: AppState) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let service = web::router(state).into_make_service_with_connect_info::<SocketAddr>();
    tokio::spawn(async move {
        axum::serve(listener, service).await.unwrap();
    });
    addr
}

/// Millisecond precision, like production events (derived from cluster
/// timestamps): raw `Utc::now()` carries nanoseconds, which Postgres
/// truncates to microseconds and would break round-trip comparisons.
fn play(track_id: &str, title: &str, artists: &[&str], days_ago: i64) -> PlayEvent {
    let started_at = Utc::now() - Duration::days(days_ago);
    let started_at =
        chrono::DateTime::from_timestamp_millis(started_at.timestamp_millis()).unwrap();
    PlayEvent {
        track_id: track_id.into(),
        kind: nowplaying::events::MediaKind::Track,
        track_url: Some(format!("https://open.spotify.com/track/{track_id}")),
        title: title.into(),
        artists: artists.iter().map(|s| s.to_string()).collect(),
        album: "Album".into(),
        cover_url: Some("https://i.scdn.co/image/abc".into()),
        duration_ms: 200_000,
        started_at,
        lyrics: None,
        metadata_source: nowplaying::events::MetadataSource::Fetch,
    }
}

#[tokio::test]
async fn insert_is_idempotent_and_guarded_insert_suppresses_replays() {
    let (_pg, pool) = setup().await;

    let event = play("track_a", "Song A", &["Artist"], 0);
    assert!(db::insert_play(&pool, &event).await.unwrap());
    assert!(!db::insert_play(&pool, &event).await.unwrap());

    let mut replay = event.clone();
    replay.started_at = event.started_at + Duration::seconds(5);
    assert!(!db::insert_play_guarded(&pool, &replay, 10.0).await.unwrap());

    let mut later = event.clone();
    later.started_at = event.started_at + Duration::seconds(15);
    assert!(db::insert_play_guarded(&pool, &later, 10.0).await.unwrap());

    let other = play("track_b", "Song B", &["Artist"], 0);
    assert!(db::insert_play_guarded(&pool, &other, 10.0).await.unwrap());
}

/// End-to-end deferred persistence: PendingPersist driving real guarded
/// inserts. Real time (no paused tokio clock — the embedded Postgres does
/// real socket I/O, and paused-time auto-advance would fire timers while
/// tasks block on it), hence a generous threshold and margins.
#[tokio::test]
async fn pending_persist_writes_only_qualified_plays() {
    const MIN_PLAY: std::time::Duration = std::time::Duration::from_millis(500);
    const MARGIN: std::time::Duration = std::time::Duration::from_millis(1_000);

    let (_pg, pool) = setup().await;
    let discarded = opentelemetry::global::meter("test")
        .u64_counter("plays_discarded_total")
        .build();
    let persist: PersistFn = {
        let pool = pool.clone();
        Arc::new(move |event: PlayEvent| {
            let pool = pool.clone();
            Box::pin(async move {
                let _ = db::insert_play_guarded(&pool, &event, 10.0).await;
            })
        })
    };
    async fn rows(pool: &PgPool) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM plays")
            .fetch_one(pool)
            .await
            .unwrap()
    }

    let full = play("e2e_full", "Full", &["X"], 0);
    let p = PendingPersist::new(
        full.clone(),
        false,
        MIN_PLAY,
        persist.clone(),
        discarded.clone(),
    );
    tokio::time::sleep(MIN_PLAY + MARGIN).await;
    assert_eq!(rows(&pool).await, 1, "qualified play must be persisted");
    p.discard(DiscardReason::Stopped);

    let skipped = play("e2e_skip", "Skip", &["X"], 0);
    let p = PendingPersist::new(skipped, false, MIN_PLAY, persist.clone(), discarded.clone());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    p.discard(DiscardReason::Superseded);
    tokio::time::sleep(MIN_PLAY + MARGIN).await;
    assert_eq!(rows(&pool).await, 1, "skipped play must not be persisted");

    let paused = play("e2e_pause", "Pause", &["X"], 0);
    let mut p = PendingPersist::new(paused, false, MIN_PLAY, persist.clone(), discarded.clone());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    p.set_paused(true);
    tokio::time::sleep(MIN_PLAY + MARGIN).await;
    assert_eq!(
        rows(&pool).await,
        1,
        "paused play must not persist on wall clock"
    );
    p.set_paused(false);
    tokio::time::sleep(MIN_PLAY + MARGIN).await;
    assert_eq!(
        rows(&pool).await,
        2,
        "resumed play must persist after listening"
    );

    let mut replay = full;
    replay.started_at += Duration::seconds(3);
    let p = PendingPersist::new(replay, false, MIN_PLAY, persist.clone(), discarded.clone());
    tokio::time::sleep(MIN_PLAY + MARGIN).await;
    assert_eq!(
        rows(&pool).await,
        2,
        "replayed play must be guard-suppressed"
    );
    p.discard(DiscardReason::Disconnected);
}

#[tokio::test]
async fn top_songs_counts_window_and_uses_latest_metadata() {
    let (_pg, pool) = setup().await;

    for days_ago in [0, 1, 2, 31] {
        db::insert_play(&pool, &play("track_a", "Old Title", &["A"], days_ago))
            .await
            .unwrap();
    }
    let mut latest = play("track_a", "New Title", &["A"], 0);
    latest.started_at = Utc::now();
    db::insert_play(&pool, &latest).await.unwrap();

    for days_ago in [3, 4] {
        db::insert_play(&pool, &play("track_b", "Other", &["B"], days_ago))
            .await
            .unwrap();
    }

    let top = db::top_songs(&pool, 10).await.unwrap();
    assert_eq!(top.len(), 2);
    assert_eq!(top[0].track_id, "track_a");
    assert_eq!(top[0].plays, 4);
    assert_eq!(top[0].title, "New Title");
    assert_eq!(top[0].track_url, "https://open.spotify.com/track/track_a");
    assert_eq!(top[1].track_id, "track_b");
    assert_eq!(top[1].plays, 2);

    let top = db::top_songs(&pool, 1).await.unwrap();
    assert_eq!(top.len(), 1);
}

/// The HTTP layer against a real database: /healthz happy path and the
/// limit clamping (1..=50, default from config) of /api/top — both are
/// invisible to the DB-function tests and the no-DB web tests. 51 distinct
/// tracks make the upper clamp observable.
#[tokio::test]
async fn router_healthz_and_top_limit_clamping() {
    let (_pg, pool) = setup().await;

    for i in 0..51 {
        db::insert_play(
            &pool,
            &play(&format!("t{i:02}"), &format!("Song {i}"), &["X"], 0),
        )
        .await
        .unwrap();
    }

    let state = AppState::new(test_config(), pool.clone());
    let addr = serve(state.clone()).await;

    let health: serde_json::Value = serde_json::from_str(
        &reqwest::get(format!("http://{addr}/healthz"))
            .await
            .unwrap()
            .text()
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(health["db_ok"], true);
    assert_eq!(health["spotify_connected"], false);
    assert_eq!(health["spotify_auth_ok"], true);

    state.set_spotify_auth_failed(true);
    let resp = reqwest::get(format!("http://{addr}/healthz"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 503, "rejected credentials need an operator");
    let body: serde_json::Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert_eq!(body["spotify_auth_ok"], false);
    state.set_spotify_auth_failed(false);

    let songs_at = |query: &'static str| {
        let base = format!("http://{addr}/api/top{query}");
        async move {
            let text = reqwest::get(base).await.unwrap().text().await.unwrap();
            let body: serde_json::Value = serde_json::from_str(&text).unwrap();
            body["songs"].as_array().unwrap().len()
        }
    };
    assert_eq!(songs_at("").await, 10, "default limit from config");
    assert_eq!(songs_at("?limit=0").await, 1, "lower clamp");
    assert_eq!(songs_at("?limit=100").await, 50, "upper clamp");
    assert_eq!(
        songs_at("?limit=25").await,
        25,
        "in-range limit passes through"
    );
}

#[tokio::test]
async fn top_artists_unnests_multi_artist_plays() {
    let (_pg, pool) = setup().await;

    db::insert_play(&pool, &play("t1", "Collab", &["X", "Y"], 0))
        .await
        .unwrap();
    db::insert_play(&pool, &play("t2", "Collab 2", &["X", "Y"], 1))
        .await
        .unwrap();
    db::insert_play(&pool, &play("t3", "Solo", &["X"], 2))
        .await
        .unwrap();
    db::insert_play(&pool, &play("t4", "Ancient", &["Y"], 40))
        .await
        .unwrap();

    let top = db::top_artists(&pool, 10).await.unwrap();
    assert_eq!(top.len(), 2);
    assert_eq!((top[0].artist.as_str(), top[0].plays), ("X", 3));
    assert_eq!((top[1].artist.as_str(), top[1].plays), ("Y", 2));
}

fn episode(track_id: &str, days_ago: i64) -> PlayEvent {
    PlayEvent {
        kind: MediaKind::Episode,
        track_url: Some(format!("https://open.spotify.com/episode/{track_id}")),
        artists: vec!["Some Show".into()],
        ..play(track_id, "Episode", &["Some Show"], days_ago)
    }
}

async fn top_json(addr: SocketAddr) -> (serde_json::Value, Option<String>) {
    let resp = reqwest::get(format!("http://{addr}/api/top"))
        .await
        .unwrap();
    let cache_control = resp
        .headers()
        .get("cache-control")
        .map(|v| v.to_str().unwrap().to_owned());
    let body = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    (body, cache_control)
}

/// Podcast episodes are persisted but never enter the song or artist lists.
/// The lists are cached until a persist invalidates them; rows written
/// behind the cache's back stay invisible until then.
#[tokio::test]
async fn top_lists_exclude_episodes_and_cache_until_invalidated() {
    let (_pg, pool) = setup().await;
    db::insert_play(&pool, &play("t1", "Song", &["X"], 0))
        .await
        .unwrap();
    for day in 0..3 {
        db::insert_play(&pool, &episode("e1", day)).await.unwrap();
    }
    let state = AppState::new(test_config(), pool.clone());
    let addr = serve(state.clone()).await;

    let (body, cache_control) = top_json(addr).await;
    assert_eq!(cache_control.as_deref(), Some("public, max-age=60"));
    let songs = body["songs"].as_array().unwrap();
    assert_eq!(songs.len(), 1);
    assert_eq!(songs[0]["track_id"], "t1");
    assert_eq!(body["artists"].as_array().unwrap().len(), 1);
    assert_eq!(body["artists"][0]["artist"], "X");

    db::insert_play(&pool, &play("t2", "Other", &["Y"], 0))
        .await
        .unwrap();
    let (cached, _) = top_json(addr).await;
    assert_eq!(
        cached["songs"].as_array().unwrap().len(),
        1,
        "served from cache"
    );

    state.top_cache.invalidate();
    let (fresh, _) = top_json(addr).await;
    assert_eq!(fresh["songs"].as_array().unwrap().len(), 2);
}

/// Startup hydration reads the newest row back with its kind and link.
#[tokio::test]
async fn latest_play_round_trips_kind_and_link() {
    let (_pg, pool) = setup().await;
    assert!(db::latest_play(&pool).await.unwrap().is_none());

    db::insert_play(&pool, &play("t1", "Older", &["X"], 2))
        .await
        .unwrap();
    db::insert_play(&pool, &episode("e1", 1)).await.unwrap();
    let latest = db::latest_play(&pool).await.unwrap().unwrap();
    assert_eq!(latest.track_id, "e1");
    assert_eq!(latest.kind, MediaKind::Episode);
    assert_eq!(
        latest.track_url.as_deref(),
        Some("https://open.spotify.com/episode/e1")
    );

    let state = AppState::new(test_config(), pool.clone());
    state.hydrate(latest).await;
    let addr = serve(state).await;
    let body: serde_json::Value = serde_json::from_str(
        &reqwest::get(format!("http://{addr}/api/now-playing"))
            .await
            .unwrap()
            .text()
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(body["track_id"], "e1");
    assert_eq!(body["playback"]["state"], "stopped");
}

/// Answers per `(uri kind, track id)`; anything unlisted is unavailable.
struct MapFetcher {
    responses: std::collections::HashMap<(MediaKind, String), Result<TrackMeta, bool>>,
    calls: std::sync::atomic::AtomicUsize,
}

impl FetchTrack for MapFetcher {
    async fn fetch(&self, uri: &str, kind: MediaKind) -> Result<TrackMeta, FetchError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let id = uri.rsplit(':').next().unwrap().to_owned();
        match self.responses.get(&(kind, id)) {
            Some(Ok(meta)) => Ok(meta.clone()),
            Some(Err(true)) => Err(FetchError::NotFound(anyhow::anyhow!("404"))),
            _ => Err(FetchError::Unavailable(anyhow::anyhow!("down"))),
        }
    }

    async fn fetch_lyrics(&self, _track_id: &str) -> LyricsFetch {
        LyricsFetch::Missing
    }
}

fn full(title: &str, artist: &str) -> TrackMeta {
    TrackMeta {
        title: title.into(),
        artists: vec![artist.into()],
        album: "Full Album".into(),
        cover_url: None,
        duration_ms: 123_000,
        lyrics: None,
        source: MetadataSource::Fetch,
    }
}

fn degraded(track_id: &str) -> PlayEvent {
    PlayEvent {
        artists: Vec::new(),
        metadata_source: MetadataSource::ClusterMap,
        ..play(track_id, "Map Title", &[], 0)
    }
}

async fn row_state(pool: &PgPool, track_id: &str) -> (String, String, Vec<String>) {
    sqlx::query_as(
        "SELECT metadata_source, content_type, artists FROM plays WHERE track_id = $1 LIMIT 1",
    )
    .bind(track_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// Degraded rows are upgraded from a fresh fetch; a "track" Spotify only
/// knows as an episode (rows from before kinds existed) is re-labelled; one
/// unknown under both kinds is retired so later passes skip it.
#[tokio::test]
async fn repair_upgrades_relabels_and_retires_degraded_rows() {
    let (_pg, pool) = setup().await;
    for id in ["trk", "epi", "gone"] {
        db::insert_play(&pool, &degraded(id)).await.unwrap();
    }
    db::insert_play(&pool, &play("ok", "Fine", &["F"], 0))
        .await
        .unwrap();
    let responses = [
        (
            (MediaKind::Track, "trk".to_owned()),
            Ok(full("Real Title", "Real Artist")),
        ),
        ((MediaKind::Track, "epi".to_owned()), Err(true)),
        (
            (MediaKind::Episode, "epi".to_owned()),
            Ok(full("Episode 1", "The Show")),
        ),
        ((MediaKind::Track, "gone".to_owned()), Err(true)),
        ((MediaKind::Episode, "gone".to_owned()), Err(true)),
    ]
    .into_iter()
    .collect();
    let resolver = MetadataResolver::with_fetcher(MapFetcher {
        responses,
        calls: Default::default(),
    });

    let summary = repair_degraded(&pool, &resolver).await;
    assert_eq!(
        (
            summary.checked,
            summary.repaired,
            summary.unresolvable,
            summary.failed
        ),
        (3, 2, 1, 0)
    );
    assert_eq!(
        row_state(&pool, "trk").await,
        ("fetch".into(), "track".into(), vec!["Real Artist".into()])
    );
    assert_eq!(
        row_state(&pool, "epi").await,
        ("fetch".into(), "episode".into(), vec!["The Show".into()])
    );
    assert_eq!(row_state(&pool, "gone").await.0, "unresolvable");
    assert!(db::degraded_tracks(&pool, 50).await.unwrap().is_empty());
    assert_eq!(
        db::degraded_counts(&pool).await.unwrap(),
        [("cluster_map", 0), ("unresolvable", 1)]
    );
}

/// Circuit breaker: when Spotify is down, a pass stops after three
/// consecutive unavailable fetches instead of hammering every track.
#[tokio::test]
async fn repair_pass_stops_when_spotify_is_unavailable() {
    let (_pg, pool) = setup().await;
    for id in ["a", "b", "c", "d", "e"] {
        db::insert_play(&pool, &degraded(id)).await.unwrap();
    }
    let resolver = MetadataResolver::with_fetcher(MapFetcher {
        responses: Default::default(),
        calls: Default::default(),
    });
    let summary = repair_degraded(&pool, &resolver).await;
    assert_eq!((summary.checked, summary.failed), (3, 3));
    assert_eq!(db::degraded_tracks(&pool, 50).await.unwrap().len(), 5);
}
