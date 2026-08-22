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
use nowplaying::events::PlayEvent;
use nowplaying::spotify::pending::{DiscardReason, PendingPersist, PersistFn};
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

/// Millisecond precision, like production events (derived from cluster
/// timestamps): raw `Utc::now()` carries nanoseconds, which Postgres
/// truncates to microseconds and would break round-trip comparisons.
fn play(track_id: &str, title: &str, artists: &[&str], days_ago: i64) -> PlayEvent {
    let started_at = Utc::now() - Duration::days(days_ago);
    let started_at =
        chrono::DateTime::from_timestamp_millis(started_at.timestamp_millis()).unwrap();
    PlayEvent {
        track_id: track_id.into(),
        track_url: format!("https://open.spotify.com/track/{track_id}"),
        title: title.into(),
        artists: artists.iter().map(|s| s.to_string()).collect(),
        album: "Album".into(),
        cover_url: Some("https://i.scdn.co/image/abc".into()),
        duration_ms: 200_000,
        started_at,
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
    let state = AppState::new(cfg, pool.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let service = web::router(state).into_make_service_with_connect_info::<SocketAddr>();
    tokio::spawn(async move {
        axum::serve(listener, service).await.unwrap();
    });

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
