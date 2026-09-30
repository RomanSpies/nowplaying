//! Service bootstrap. The rustls process default is installed explicitly
//! before anything else: both aws-lc-rs (sqlx) and ring (librespot's
//! websocket stack) are in the dependency tree, so rustls cannot pick a
//! default on its own.

use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use nowplaying::config::Config;
use nowplaying::state::AppState;
use nowplaying::{db, spotify, telemetry, web};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;
use tracing::{error, info, warn};

fn main() -> anyhow::Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("installing rustls crypto provider");

    let cfg = Config::parse();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("building tokio runtime")?
        .block_on(run(cfg))
}

async fn run(cfg: Config) -> anyhow::Result<()> {
    if cfg.login {
        tracing_subscriber::fmt().init();
        return spotify::session::oauth_login(&cfg).await;
    }

    let telemetry = telemetry::init(&cfg)?;
    info!(
        service_name = %cfg.service_name,
        version = telemetry::SERVICE_VERSION,
        "starting nowplaying"
    );

    let result = run_inner(cfg).await;
    telemetry.shutdown();
    result
}

/// Wires DB, Spotify task and web server together and supervises shutdown.
/// The now-playing cache is seeded from the latest persisted play before
/// either task starts, so the widget has something to show immediately.
/// Both tasks are expected to run forever — an early exit is fatal and hands
/// recovery to systemd, after signalling the surviving task to stop cleanly.
/// Shutdown joins both tasks instead of sleeping a fixed grace period: the
/// spotify timeout covers an in-flight metadata fetch (bounded at 3s) plus
/// the 5s Spirc drain, the server's the 5s graceful window; anything slower
/// is abandoned with a warning.
async fn run_inner(cfg: Config) -> anyhow::Result<()> {
    let database_url = cfg
        .database_url
        .clone()
        .context("--database-url / DATABASE_URL is required")?;

    let pool = db::connect(&database_url)
        .await
        .context("connecting to Postgres")?;
    let state = AppState::new(cfg, pool);
    match db::latest_play(&state.db).await {
        Ok(Some(event)) => state.hydrate(event).await,
        Ok(None) => {}
        Err(e) => warn!(error = %e, "seeding now-playing from the database failed"),
    }

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut spotify_task = spotify::spawn(state.clone(), shutdown_rx);

    let server_handle = axum_server::Handle::new();
    let mut server_task = tokio::spawn(web::serve(state, server_handle.clone()));

    let mut sigterm = signal(SignalKind::terminate()).context("installing SIGTERM handler")?;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => info!("received ctrl-c"),
        _ = sigterm.recv() => info!("received SIGTERM"),
        result = &mut spotify_task => {
            let _ = shutdown_tx.send(true);
            let inner = result.context("spotify task panicked")?;
            error!(task = "spotify", error = ?inner, "task exited");
            anyhow::bail!("spotify task exited");
        }
        result = &mut server_task => {
            let _ = shutdown_tx.send(true);
            let inner = result.context("server task panicked")?;
            error!(task = "server", error = ?inner, "task exited");
            anyhow::bail!("server task exited");
        }
    }

    let _ = shutdown_tx.send(true);
    server_handle.graceful_shutdown(Some(Duration::from_secs(5)));
    let (spotify_res, server_res) = tokio::join!(
        tokio::time::timeout(Duration::from_secs(10), &mut spotify_task),
        tokio::time::timeout(Duration::from_secs(6), &mut server_task),
    );
    for (name, task, res) in [
        ("spotify", &spotify_task, spotify_res),
        ("server", &server_task, server_res),
    ] {
        match res {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(e))) => warn!(
                task = name,
                error = %format!("{e:#}"),
                "task ended with error during shutdown"
            ),
            Ok(Err(join_err)) => {
                warn!(task = name, error = %join_err, "task panicked during shutdown")
            }
            Err(_) => {
                warn!(task = name, "task did not stop in time; aborting it");
                task.abort();
            }
        }
    }
    Ok(())
}
