//! Milestone-0 spike / permanent diagnostic tool.
//!
//! Registers as a Spotify Connect device via Spirc AND subscribes to the
//! Connect cluster topic in parallel, printing every decoded update. Use it
//! to verify that playback on OTHER devices (phone, desktop) is observed
//! while this device stays visible in the Spotify app.
//!
//! Env: NP_CACHE_DIR (default ./spike-cache), NP_DEVICE_NAME, SPIKE_LOGIN=1
//! to run the interactive OAuth flow if no cached credentials exist.
//!
//! The cluster subscription is registered BEFORE `Spirc::new`; Spirc holds an
//! identical subscription and the dealer fans messages out to both. The
//! `PlayerEvent` channel is printed only as a secondary signal — it fires
//! exclusively when THIS device is the active player, which is precisely the
//! limitation that motivated the cluster-subscription architecture. The
//! rustls process default is installed explicitly for the same aws-lc-rs/ring
//! reason as in the main binary.

use std::path::PathBuf;
use std::sync::Arc;

use futures_util::StreamExt;
use librespot::connect::{ConnectConfig, Spirc};
use librespot::core::authentication::Credentials;
use librespot::core::cache::Cache;
use librespot::core::dealer::manager::BoxedStreamResult;
use librespot::core::dealer::protocol::Message;
use librespot::core::{Session, SessionConfig};
use librespot::playback::audio_backend::{Sink, SinkResult};
use librespot::playback::config::PlayerConfig;
use librespot::playback::convert::Converter;
use librespot::playback::decoder::AudioPacket;
use librespot::playback::mixer::{self, MixerConfig};
use librespot::playback::player::Player;
use librespot::protocol::connect::ClusterUpdate;

struct NoopSink;
impl Sink for NoopSink {
    fn write(&mut self, _: AudioPacket, _: &mut Converter) -> SinkResult<()> {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("installing rustls crypto provider");

    tracing_subscriber::fmt()
        .with_env_filter("info,librespot=info")
        .init();

    let cache_dir =
        PathBuf::from(std::env::var("NP_CACHE_DIR").unwrap_or_else(|_| "./spike-cache".into()));
    let device_name = std::env::var("NP_DEVICE_NAME").unwrap_or_else(|_| "NowPlaying Spike".into());
    std::fs::create_dir_all(&cache_dir)?;

    let mut session_config = SessionConfig::default();
    let device_id_path = cache_dir.join("device_id");
    match std::fs::read_to_string(&device_id_path) {
        Ok(id) if !id.trim().is_empty() => session_config.device_id = id.trim().to_string(),
        _ => std::fs::write(&device_id_path, &session_config.device_id)?,
    }

    let cache = Cache::new(Some(&cache_dir), Some(&cache_dir), None, None)?;
    let credentials = match cache.credentials() {
        Some(c) => c,
        None if std::env::var("SPIKE_LOGIN").is_ok() => {
            let client_id = session_config.client_id.clone();
            let token = tokio::task::spawn_blocking(move || {
                librespot::oauth::OAuthClientBuilder::new(
                    &client_id,
                    "http://127.0.0.1:8898/login",
                    vec!["streaming"],
                )
                .build()?
                .get_access_token()
            })
            .await??;
            Credentials::with_access_token(token.access_token)
        }
        None => {
            eprintln!("no cached credentials in {cache_dir:?}; rerun with SPIKE_LOGIN=1");
            std::process::exit(1);
        }
    };

    let session = Session::new(session_config, Some(cache));

    let mut cluster_stream: BoxedStreamResult<ClusterUpdate> = session
        .dealer()
        .listen_for("hm://connect-state/v1/cluster", Message::from_raw)?;

    let mixer = mixer::find(None).expect("softmixer")(MixerConfig::default())?;
    let player = Player::new(
        PlayerConfig::default(),
        session.clone(),
        mixer.get_soft_volume(),
        || Box::new(NoopSink),
    );
    let mut player_events = player.get_player_event_channel();

    let connect_config = ConnectConfig {
        name: device_name.clone(),
        ..ConnectConfig::default()
    };
    let (spirc, spirc_task) = Spirc::new(
        connect_config,
        session.clone(),
        credentials,
        player,
        Arc::clone(&mixer),
    )
    .await?;
    let mut spirc_task = std::pin::pin!(spirc_task);

    println!("== device '{device_name}' registered; play something on your phone ==");

    loop {
        tokio::select! {
            _ = &mut spirc_task => {
                eprintln!("!! spirc task ended");
                break;
            }
            _ = tokio::signal::ctrl_c() => {
                let _ = spirc.shutdown();
                break;
            }
            Some(event) = player_events.recv() => {
                println!("[player-event] {event:?}");
            }
            item = cluster_stream.next() => match item {
                None => {
                    eprintln!("!! cluster stream ended");
                    break;
                }
                Some(Err(e)) => eprintln!("!! undecodable cluster update: {e}"),
                Some(Ok(mut update)) => {
                    let reason = update.update_reason.enum_value();
                    let Some(cluster) = update.cluster.take() else {
                        println!("[cluster] reason={reason:?} (no cluster payload)");
                        continue;
                    };
                    let Some(ps) = cluster.player_state.0 else {
                        println!("[cluster] reason={reason:?} active={} (no player state)", cluster.active_device_id);
                        continue;
                    };
                    let track = ps.track.0;
                    println!(
                        "[cluster] reason={reason:?} active={} playing={} paused={} ts={} pos={} dur={}",
                        cluster.active_device_id, ps.is_playing, ps.is_paused,
                        ps.timestamp, ps.position_as_of_timestamp, ps.duration,
                    );
                    if let Some(track) = track {
                        println!("  track uri={} provider={}", track.uri, track.provider);
                        let mut keys: Vec<_> = track.metadata.iter().collect();
                        keys.sort_by_key(|(k, _)| (*k).clone());
                        for (k, v) in keys {
                            println!("    meta {k} = {v}");
                        }
                    }
                }
            },
        }
    }
    Ok(())
}
