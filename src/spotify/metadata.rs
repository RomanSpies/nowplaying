use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::OnceLock;
use std::time::Instant;

use anyhow::Context;
use librespot::core::{Session, SpotifyUri};
use librespot::metadata::{Metadata, Track};
use lru::LruCache;
use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram};
use tokio::sync::Mutex;
use tracing::{instrument, warn};

const IMAGE_URL_FALLBACK: &str = "https://i.scdn.co/image/{file_id}";

struct MetaMetrics {
    fetch_duration: Histogram<f64>,
    cache_hits: Counter<u64>,
    cache_misses: Counter<u64>,
}

/// Fetch-duration boundaries mirror the semconv `http.server.request.duration`
/// buckets — `Track::get` is a network round trip too, and the SDK defaults
/// assume multi-second scales that would dump all sub-second fetches into the
/// lowest buckets.
fn metrics() -> &'static MetaMetrics {
    static METRICS: OnceLock<MetaMetrics> = OnceLock::new();
    METRICS.get_or_init(|| {
        let meter = global::meter("nowplaying");
        MetaMetrics {
            fetch_duration: meter
                .f64_histogram("metadata_fetch_duration")
                .with_unit("s")
                .with_description("Track::get latency, labelled by outcome")
                .with_boundaries(vec![
                    0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0,
                ])
                .build(),
            cache_hits: meter
                .u64_counter("metadata_cache_hits_total")
                .with_description("Metadata served from the LRU cache")
                .build(),
            cache_misses: meter
                .u64_counter("metadata_cache_misses_total")
                .with_description("Metadata lookups that required a fetch")
                .build(),
        }
    })
}

#[derive(Debug, Clone)]
pub struct TrackMeta {
    pub title: String,
    pub artists: Vec<String>,
    pub album: String,
    pub cover_url: Option<String>,
    pub duration_ms: u32,
}

/// The one part of metadata resolution that needs a live Spotify session:
/// fetching the full track record. Split out so the resolver's cache and
/// fallback logic (and `handle_update` above it) are testable with a fake.
///
/// Desugared RPITIT instead of `async fn` so the `Send` bound is explicit —
/// the whole spotify task runs under `tokio::spawn`. Impls may still use
/// plain `async fn` (interchangeable since 1.75).
pub trait FetchTrack: Send + Sync {
    fn fetch(
        &self,
        track_uri: &str,
    ) -> impl std::future::Future<Output = anyhow::Result<TrackMeta>> + Send;
}

/// Production fetcher: `Track::get` plus the session's cover-URL template.
pub struct SessionFetcher {
    session: Session,
}

impl FetchTrack for SessionFetcher {
    async fn fetch(&self, track_uri: &str) -> anyhow::Result<TrackMeta> {
        let uri = SpotifyUri::from_uri(track_uri).context("parsing track uri")?;
        let track = Track::get(&self.session, &uri)
            .await
            .map_err(|e| anyhow::anyhow!("Track::get: {e}"))?;

        let template = self
            .session
            .get_user_attribute("image-url")
            .unwrap_or_else(|| IMAGE_URL_FALLBACK.to_string());
        let cover_url = widest_cover_url(&track, &template);

        Ok(TrackMeta {
            title: track.name,
            artists: track.artists.0.iter().map(|a| a.name.clone()).collect(),
            album: track.album.name,
            cover_url,
            duration_ms: track.duration.max(0) as u32,
        })
    }
}

/// Resolves track metadata, preferring the full fetched record (complete
/// artist list) and falling back to the cluster's metadata map if the fetch
/// fails. Results are LRU-cached per track URI.
pub struct MetadataResolver<F: FetchTrack = SessionFetcher> {
    fetcher: F,
    cache: Mutex<LruCache<String, TrackMeta>>,
}

impl MetadataResolver<SessionFetcher> {
    pub fn new(session: Session) -> Self {
        Self::with_fetcher(SessionFetcher { session })
    }
}

impl<F: FetchTrack> MetadataResolver<F> {
    pub fn with_fetcher(fetcher: F) -> Self {
        Self {
            fetcher,
            cache: Mutex::new(LruCache::new(NonZeroUsize::new(256).unwrap())),
        }
    }

    /// `source` records where the metadata came from: cache | fetch |
    /// cluster_map. Only successful fetches are cached: the cluster-map
    /// fallback lacks the artist list, and caching it would pin that degraded
    /// record for the LRU's lifetime — left uncached, the next play of the
    /// track retries the full fetch.
    #[instrument(
        name = "metadata.resolve",
        skip(self, cluster_meta),
        fields(track_uri = %track_uri, source = tracing::field::Empty)
    )]
    pub async fn resolve(
        &self,
        track_uri: &str,
        cluster_meta: &HashMap<String, String>,
        duration_hint_ms: i64,
    ) -> anyhow::Result<TrackMeta> {
        let span = tracing::Span::current();
        let m = metrics();
        if let Some(hit) = self.cache.lock().await.get(track_uri) {
            span.record("source", "cache");
            m.cache_hits.add(1, &[]);
            return Ok(hit.clone());
        }
        m.cache_misses.add(1, &[]);

        let started = Instant::now();
        let fetched = self.fetcher.fetch(track_uri).await;
        let outcome = if fetched.is_ok() { "ok" } else { "error" };
        m.fetch_duration.record(
            started.elapsed().as_secs_f64(),
            &[KeyValue::new("outcome", outcome)],
        );

        match fetched {
            Ok(meta) => {
                span.record("source", "fetch");
                self.cache
                    .lock()
                    .await
                    .put(track_uri.to_string(), meta.clone());
                Ok(meta)
            }
            Err(e) => {
                span.record("source", "cluster_map");
                warn!("metadata fetch failed, falling back to cluster map: {e:#}");
                from_cluster_map(cluster_meta, duration_hint_ms)
                    .context("cluster metadata map insufficient")
            }
        }
    }
}

fn widest_cover_url(track: &Track, template: &str) -> Option<String> {
    track
        .album
        .covers
        .0
        .iter()
        .max_by_key(|img| img.width)
        .map(|img| img.id.to_string())
        .filter(|id| !id.is_empty())
        .map(|id| template.replace("{file_id}", &id))
}

/// Build metadata from the cluster track's metadata map. Keys verified
/// against real cluster updates (M0 spike): "title", "album_title",
/// "image_url"/"image_xlarge_url" (as spotify:image: URIs), "artist_uri" —
/// notably there is NO artist name in the map. "artist_name" is still read
/// opportunistically in case a cluster ever sends it; otherwise artists stay
/// empty rather than dropping the play when this fallback is hit.
fn from_cluster_map(meta: &HashMap<String, String>, duration_hint_ms: i64) -> Option<TrackMeta> {
    let title = meta.get("title")?.clone();
    let album = meta.get("album_title").cloned().unwrap_or_default();
    let duration_ms = meta
        .get("duration")
        .and_then(|d| d.parse::<i64>().ok())
        .unwrap_or(duration_hint_ms)
        .max(0) as u32;
    let cover_url = meta
        .get("image_xlarge_url")
        .or_else(|| meta.get("image_large_url"))
        .or_else(|| meta.get("image_url"))
        .map(|u| normalize_image_url(u));
    Some(TrackMeta {
        title,
        artists: meta.get("artist_name").cloned().into_iter().collect(),
        album,
        cover_url,
        duration_ms,
    })
}

/// Cluster image references may be `spotify:image:<hex>` URIs instead of URLs.
fn normalize_image_url(value: &str) -> String {
    match value.strip_prefix("spotify:image:") {
        Some(file_id) => format!("https://i.scdn.co/image/{file_id}"),
        None => value.to_string(),
    }
}

pub fn track_url(track_uri: &str) -> anyhow::Result<(String, String)> {
    let uri = SpotifyUri::from_uri(track_uri).context("parsing track uri")?;
    let id = uri
        .to_id()
        .map_err(|e| anyhow::anyhow!("track uri has no id: {e}"))?;
    let url = format!("https://open.spotify.com/track/{id}");
    Ok((id, url))
}

/// Scripted fetcher for tests here and in `spotify::mod` (handle_update
/// integration): pops one canned response per call, `None` → fetch error.
#[cfg(test)]
pub(crate) mod test_support {
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{FetchTrack, TrackMeta};

    pub(crate) struct ScriptedFetcher {
        responses: Mutex<VecDeque<Option<TrackMeta>>>,
        pub(crate) calls: AtomicUsize,
    }

    impl ScriptedFetcher {
        pub(crate) fn new(responses: Vec<Option<TrackMeta>>) -> Self {
            Self {
                responses: Mutex::new(responses.into()),
                calls: AtomicUsize::new(0),
            }
        }

        pub(crate) fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl FetchTrack for ScriptedFetcher {
        async fn fetch(&self, _track_uri: &str) -> anyhow::Result<TrackMeta> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.responses.lock().unwrap().pop_front() {
                Some(Some(meta)) => Ok(meta),
                _ => Err(anyhow::anyhow!("scripted fetch failure")),
            }
        }
    }

    pub(crate) fn full_meta(title: &str) -> TrackMeta {
        TrackMeta {
            title: title.into(),
            artists: vec!["Artist".into()],
            album: "Album".into(),
            cover_url: Some("https://i.scdn.co/image/abc".into()),
            duration_ms: 200_000,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{ScriptedFetcher, full_meta};
    use super::*;

    #[tokio::test]
    async fn resolve_caches_successful_fetches() {
        let resolver =
            MetadataResolver::with_fetcher(ScriptedFetcher::new(vec![Some(full_meta("Hit"))]));
        let map = HashMap::new();

        let first = resolver.resolve("spotify:track:x", &map, 0).await.unwrap();
        assert_eq!(first.title, "Hit");
        assert_eq!(resolver.fetcher.call_count(), 1);

        let second = resolver.resolve("spotify:track:x", &map, 0).await.unwrap();
        assert_eq!(second.title, "Hit");
        assert_eq!(resolver.fetcher.call_count(), 1);
    }

    #[tokio::test]
    async fn failed_fetch_falls_back_uncached_and_retries() {
        let resolver = MetadataResolver::with_fetcher(ScriptedFetcher::new(vec![
            None,
            Some(full_meta("Recovered")),
        ]));
        let mut map = HashMap::new();
        map.insert("title".to_string(), "Fallback Title".to_string());

        let degraded = resolver
            .resolve("spotify:track:x", &map, 1_000)
            .await
            .unwrap();
        assert_eq!(degraded.title, "Fallback Title");
        assert!(degraded.artists.is_empty());

        let healed = resolver
            .resolve("spotify:track:x", &map, 1_000)
            .await
            .unwrap();
        assert_eq!(healed.title, "Recovered");
        assert_eq!(healed.artists, vec!["Artist".to_string()]);
        assert_eq!(resolver.fetcher.call_count(), 2);

        resolver
            .resolve("spotify:track:x", &map, 1_000)
            .await
            .unwrap();
        assert_eq!(resolver.fetcher.call_count(), 2);
    }

    #[tokio::test]
    async fn failed_fetch_without_usable_fallback_is_an_error() {
        let resolver = MetadataResolver::with_fetcher(ScriptedFetcher::new(vec![None]));
        let result = resolver
            .resolve("spotify:track:x", &HashMap::new(), 0)
            .await;
        assert!(result.is_err());
    }

    #[test]
    fn normalizes_spotify_image_uris() {
        assert_eq!(
            normalize_image_url("spotify:image:ab67616d0000b273deadbeef"),
            "https://i.scdn.co/image/ab67616d0000b273deadbeef"
        );
        assert_eq!(
            normalize_image_url("https://i.scdn.co/image/xyz"),
            "https://i.scdn.co/image/xyz"
        );
    }

    #[test]
    fn cluster_map_fallback() {
        let mut m = HashMap::new();
        m.insert("title".to_string(), "Breaking the Habit".to_string());
        m.insert("album_title".to_string(), "Meteora".to_string());
        m.insert("image_url".to_string(), "spotify:image:small".to_string());
        m.insert(
            "image_xlarge_url".to_string(),
            "spotify:image:xlarge".to_string(),
        );
        let meta = from_cluster_map(&m, 196_133).unwrap();
        assert_eq!(meta.title, "Breaking the Habit");
        assert!(meta.artists.is_empty());
        assert_eq!(meta.duration_ms, 196_133);
        assert_eq!(
            meta.cover_url.as_deref(),
            Some("https://i.scdn.co/image/xlarge")
        );

        assert!(from_cluster_map(&HashMap::new(), 0).is_none());

        m.insert("artist_name".to_string(), "Linkin Park".to_string());
        let meta = from_cluster_map(&m, 196_133).unwrap();
        assert_eq!(meta.artists, vec!["Linkin Park".to_string()]);
    }

    #[test]
    fn builds_track_urls() {
        let (id, url) = track_url("spotify:track:4uLU6hMCjMI75M1A2tKUQC").unwrap();
        assert_eq!(id, "4uLU6hMCjMI75M1A2tKUQC");
        assert_eq!(url, "https://open.spotify.com/track/4uLU6hMCjMI75M1A2tKUQC");
    }
}
