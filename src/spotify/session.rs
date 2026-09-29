use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use librespot::connect::ConnectConfig;
use librespot::core::authentication::Credentials;
use librespot::core::cache::Cache;
use librespot::core::{Session, SessionConfig};
use librespot::playback::config::PlayerConfig;
use librespot::playback::mixer::{self, Mixer, MixerConfig};
use librespot::playback::player::Player;
use tracing::info;

use crate::config::Config;
use crate::spotify::sink::NoopSink;

const OAUTH_REDIRECT_URI: &str = "http://127.0.0.1:8898/login";
const OAUTH_SCOPES: &[&str] = &["streaming"];

/// `SessionConfig::default()` generates a fresh random device_id on every
/// call; persist one so we stay the same Connect device across restarts.
fn stable_device_id(cache_dir: &Path) -> anyhow::Result<String> {
    let path = cache_dir.join("device_id");
    if let Ok(id) = std::fs::read_to_string(&path) {
        let id = id.trim().to_string();
        if !id.is_empty() {
            return Ok(id);
        }
    }
    let id = SessionConfig::default().device_id;
    std::fs::write(&path, &id).context("persisting device_id")?;
    Ok(id)
}

pub fn session_config(cfg: &Config) -> anyhow::Result<SessionConfig> {
    std::fs::create_dir_all(&cfg.cache_dir).context("creating cache dir")?;
    let session_config = SessionConfig {
        device_id: stable_device_id(&cfg.cache_dir)?,
        ..SessionConfig::default()
    };
    Ok(session_config)
}

/// Where librespot's `Cache` keeps the reusable credentials.
pub fn credentials_path(cfg: &Config) -> PathBuf {
    cfg.cache_dir.join("credentials.json")
}

/// The cache holds no usable credentials: absent, or unreadable (e.g. a
/// half-written file while `--login` runs). Treated like rejected
/// credentials — the service parks until the file changes.
#[derive(Debug)]
pub struct MissingCredentials {
    pub cache_dir: PathBuf,
}

impl std::fmt::Display for MissingCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "no usable cached Spotify credentials in {} — run `nowplaying --login` once",
            self.cache_dir.display()
        )
    }
}

impl std::error::Error for MissingCredentials {}

pub fn open_cache(cfg: &Config) -> anyhow::Result<Cache> {
    Cache::new(
        Some(cfg.cache_dir.as_path()),
        Some(cfg.cache_dir.as_path()),
        None,
        None,
    )
    .map_err(|e| anyhow::anyhow!("opening credentials cache: {e}"))
}

/// Everything needed for one connection attempt. The Spirc consumes the
/// credentials; on reconnect a fresh bundle is built.
pub struct SessionBundle {
    pub session: Session,
    pub credentials: Credentials,
    pub player: Arc<Player>,
    pub mixer: Arc<dyn Mixer>,
    pub connect_config: ConnectConfig,
}

pub fn build(cfg: &Config) -> anyhow::Result<SessionBundle> {
    let cache = open_cache(cfg)?;
    let Some(credentials) = cache.credentials() else {
        return Err(MissingCredentials {
            cache_dir: cfg.cache_dir.clone(),
        }
        .into());
    };

    let session = Session::new(session_config(cfg)?, Some(cache));

    let mixer = mixer::find(None).context("softmixer not found")?(MixerConfig::default())
        .map_err(|e| anyhow::anyhow!("opening mixer: {e}"))?;
    let player = Player::new(
        PlayerConfig::default(),
        session.clone(),
        mixer.get_soft_volume(),
        || Box::new(NoopSink),
    );

    let connect_config = ConnectConfig {
        name: cfg.device_name.clone(),
        ..ConnectConfig::default()
    };

    Ok(SessionBundle {
        session,
        credentials,
        player,
        mixer,
        connect_config,
    })
}

/// One-time interactive OAuth: fetch an access token, connect once with
/// `store_credentials = true` so the AP-returned reusable credentials land in
/// the cache, then exit.
pub async fn oauth_login(cfg: &Config) -> anyhow::Result<()> {
    let session_config = session_config(cfg)?;
    let cache = open_cache(cfg)?;

    let client_id = session_config.client_id.clone();
    let token = tokio::task::spawn_blocking(move || {
        librespot::oauth::OAuthClientBuilder::new(
            &client_id,
            OAUTH_REDIRECT_URI,
            OAUTH_SCOPES.to_vec(),
        )
        .build()?
        .get_access_token()
    })
    .await
    .context("oauth task panicked")?
    .map_err(|e| anyhow::anyhow!("oauth flow failed: {e}"))?;

    let session = Session::new(session_config, Some(cache));
    session
        .connect(Credentials::with_access_token(token.access_token), true)
        .await
        .map_err(|e| anyhow::anyhow!("connecting with oauth token: {e}"))?;

    info!(
        username = %session.username(),
        cache_dir = %cfg.cache_dir.display(),
        "credentials stored; the service can now run non-interactively"
    );
    session.shutdown();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::stable_device_id;

    /// A restart must come back as the same Connect device; a whitespace-only
    /// (corrupt) file yields a fresh id that is persisted and stable again.
    #[test]
    fn device_id_is_persisted_and_reused_across_restarts() {
        let dir =
            std::env::temp_dir().join(format!("nowplaying-device-id-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();

        let first = stable_device_id(&dir).unwrap();
        assert!(!first.is_empty());
        assert_eq!(stable_device_id(&dir).unwrap(), first);

        std::fs::write(dir.join("device_id"), "  \n").unwrap();
        let regenerated = stable_device_id(&dir).unwrap();
        assert!(!regenerated.is_empty());
        assert_ne!(regenerated, first);
        assert_eq!(stable_device_id(&dir).unwrap(), regenerated);

        std::fs::remove_dir_all(&dir).ok();
    }
}
