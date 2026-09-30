use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use anyhow::Context;
use librespot::core::error::ErrorKind;
use librespot::core::{Session, SpotifyUri};
use librespot::metadata::image::Images;
use librespot::metadata::{Episode, Metadata, Track};
use librespot::protocol::metadata::{Episode as EpisodeMessage, Track as TrackMessage};
use lru::LruCache;
use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram};
use protobuf::Message as _;
use tokio::sync::Mutex;
use tracing::{instrument, warn};

use crate::events::{MediaKind, MetadataSource};
use crate::spotify::cluster::classify;
use crate::spotify::lyrics::{self, LyricsFetch};

const IMAGE_URL_FALLBACK: &str = "https://i.scdn.co/image/{file_id}";

struct MetaMetrics {
    fetch_duration: Histogram<f64>,
    lyrics_duration: Histogram<f64>,
    cache_hits: Counter<u64>,
    cache_misses: Counter<u64>,
    lyrics_fetch: Counter<u64>,
}

/// Fetch-duration boundaries mirror the semconv `http.server.request.duration`
/// buckets — both fetches are network round trips, and the SDK defaults
/// assume multi-second scales that would dump all sub-second fetches into the
/// lowest buckets. Record and lyrics fetches run concurrently but are timed
/// separately, so a slow lyrics endpoint never reads as slow metadata.
const FETCH_BOUNDARIES: [f64; 14] = [
    0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0,
];

fn metrics() -> &'static MetaMetrics {
    static METRICS: OnceLock<MetaMetrics> = OnceLock::new();
    METRICS.get_or_init(|| {
        let meter = global::meter("nowplaying");
        MetaMetrics {
            fetch_duration: meter
                .f64_histogram("metadata_fetch_duration")
                .with_unit("s")
                .with_description(
                    "Track/episode record fetch latency by outcome (ok|error|timeout)",
                )
                .with_boundaries(FETCH_BOUNDARIES.to_vec())
                .build(),
            lyrics_duration: meter
                .f64_histogram("lyrics_fetch_duration")
                .with_unit("s")
                .with_description("Lyrics fetch latency by outcome (ok|unsynced|none|error)")
                .with_boundaries(FETCH_BOUNDARIES.to_vec())
                .build(),
            cache_hits: meter
                .u64_counter("metadata_cache_hits_total")
                .with_description("Metadata served from the LRU cache")
                .build(),
            cache_misses: meter
                .u64_counter("metadata_cache_misses_total")
                .with_description("Metadata lookups that required a fetch")
                .build(),
            lyrics_fetch: meter
                .u64_counter("lyrics_fetch_total")
                .with_description("Lyrics fetches by outcome (ok|unsynced|none|error)")
                .build(),
        }
    })
}

pub(crate) struct SpotifyApiMetrics {
    pub(crate) requests: Counter<u64>,
    pub(crate) response_bytes: Counter<u64>,
}

/// Usage accounting for our spclient calls. Application payload only: the
/// dealer stream, AP session traffic and TLS overhead are not visible from
/// inside librespot and are deliberately out of scope.
pub(crate) fn api_metrics() -> &'static SpotifyApiMetrics {
    static METRICS: OnceLock<SpotifyApiMetrics> = OnceLock::new();
    METRICS.get_or_init(|| {
        let meter = global::meter("nowplaying");
        SpotifyApiMetrics {
            requests: meter
                .u64_counter("spotify_api_requests_total")
                .with_description("Requests to Spotify's spclient API by endpoint")
                .build(),
            response_bytes: meter
                .u64_counter("spotify_api_response_bytes_total")
                .with_description("Payload bytes received from Spotify's spclient API by endpoint")
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
    /// Present iff line-synced, scrambled lyrics exist (see spotify::lyrics).
    pub lyrics: Option<crate::events::Lyrics>,
    pub source: MetadataSource,
}

/// Upper bound for one metadata record fetch. `Track::request` normally
/// answers in well under a second; librespot's HTTP client itself has no
/// deadline, and a hung fetch would stall the whole cluster loop.
pub const FETCH_TIMEOUT: Duration = Duration::from_secs(3);

/// A failed record fetch, split by whether retrying can help: the repair
/// pass gives up on `NotFound` records but only pauses on `Unavailable`.
#[derive(Debug)]
pub enum FetchError {
    /// Spotify does not know the item as the requested kind (404, or a URI
    /// the metadata endpoint rejects).
    NotFound(anyhow::Error),
    /// Network trouble, 5xx, undecodable responses.
    Unavailable(anyhow::Error),
    /// No answer within [`FETCH_TIMEOUT`]; retryable like `Unavailable`.
    TimedOut,
}

impl FetchError {
    fn from_librespot(context: &str, e: librespot::core::Error) -> Self {
        let err = anyhow::anyhow!("{context}: {e}");
        match e.kind {
            ErrorKind::NotFound | ErrorKind::InvalidArgument => Self::NotFound(err),
            _ => Self::Unavailable(err),
        }
    }

    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound(_))
    }

    /// Metric/span label for a failed fetch.
    pub fn label(&self) -> &'static str {
        match self {
            Self::TimedOut => "timeout",
            Self::NotFound(_) | Self::Unavailable(_) => "error",
        }
    }
}

impl std::fmt::Display for FetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(e) => write!(f, "not found: {e:#}"),
            Self::Unavailable(e) => write!(f, "unavailable: {e:#}"),
            Self::TimedOut => write!(f, "no answer within {FETCH_TIMEOUT:?}"),
        }
    }
}

impl std::error::Error for FetchError {}

/// The parts of metadata resolution that need a live Spotify session: the
/// full item record and the lyrics. Split out so the resolver's cache,
/// timeout and fallback logic (and `handle_update` above it) are testable
/// with a fake.
///
/// Desugared RPITIT instead of `async fn` so the `Send` bound is explicit —
/// the whole spotify task runs under `tokio::spawn`. Impls may still use
/// plain `async fn` (interchangeable since 1.75).
pub trait FetchTrack: Send + Sync {
    /// Fetch the record of a track or episode; lyrics are left `None`.
    fn fetch(
        &self,
        uri: &str,
        kind: MediaKind,
    ) -> impl std::future::Future<Output = Result<TrackMeta, FetchError>> + Send;

    fn fetch_lyrics(&self, track_id: &str)
    -> impl std::future::Future<Output = LyricsFetch> + Send;
}

/// Production fetcher: `Track::get`/`Episode::get` plus the session's
/// cover-URL template.
pub struct SessionFetcher {
    session: Session,
}

impl SessionFetcher {
    fn cover_template(&self) -> String {
        self.session
            .get_user_attribute("image-url")
            .unwrap_or_else(|| IMAGE_URL_FALLBACK.to_string())
    }

    async fn request(
        &self,
        endpoint: &'static str,
        request: impl std::future::Future<Output = Result<bytes::Bytes, librespot::core::Error>>,
    ) -> Result<bytes::Bytes, FetchError> {
        api_metrics()
            .requests
            .add(1, &[KeyValue::new("endpoint", endpoint)]);
        let bytes = request
            .await
            .map_err(|e| FetchError::from_librespot(endpoint, e))?;
        api_metrics()
            .response_bytes
            .add(bytes.len() as u64, &[KeyValue::new("endpoint", endpoint)]);
        Ok(bytes)
    }
}

impl FetchTrack for SessionFetcher {
    async fn fetch(&self, uri_str: &str, kind: MediaKind) -> Result<TrackMeta, FetchError> {
        let uri = SpotifyUri::from_uri(uri_str)
            .map_err(|e| FetchError::NotFound(anyhow::anyhow!("parsing uri: {e}")))?;
        let undecodable = |e: &dyn std::fmt::Display| {
            FetchError::Unavailable(anyhow::anyhow!("decoding {}: {e}", kind.as_str()))
        };
        match kind {
            MediaKind::Track => {
                let bytes = self
                    .request("metadata", Track::request(&self.session, &uri))
                    .await?;
                let msg = TrackMessage::parse_from_bytes(&bytes).map_err(|e| undecodable(&e))?;
                let track = Track::parse(&msg, &uri).map_err(|e| undecodable(&e))?;
                Ok(TrackMeta {
                    cover_url: widest_cover_url(&track.album.covers, &self.cover_template()),
                    title: track.name,
                    artists: track.artists.0.iter().map(|a| a.name.clone()).collect(),
                    album: track.album.name,
                    duration_ms: track.duration.max(0) as u32,
                    lyrics: None,
                    source: MetadataSource::Fetch,
                })
            }
            MediaKind::Episode => {
                let bytes = self
                    .request("metadata", Episode::request(&self.session, &uri))
                    .await?;
                let msg = EpisodeMessage::parse_from_bytes(&bytes).map_err(|e| undecodable(&e))?;
                let episode = Episode::parse(&msg, &uri).map_err(|e| undecodable(&e))?;
                Ok(TrackMeta {
                    cover_url: widest_cover_url(&episode.covers, &self.cover_template()),
                    title: episode.name,
                    artists: vec![episode.show_name.clone()],
                    album: episode.show_name,
                    duration_ms: episode.duration.max(0) as u32,
                    lyrics: None,
                    source: MetadataSource::Fetch,
                })
            }
            MediaKind::Local => local_meta(uri_str).ok_or_else(|| {
                FetchError::NotFound(anyhow::anyhow!("not a local file uri: {uri_str}"))
            }),
        }
    }

    async fn fetch_lyrics(&self, track_id: &str) -> LyricsFetch {
        lyrics::fetch(&self.session, track_id).await
    }
}

/// Resolves item metadata, preferring the full fetched record (complete
/// artist list) and falling back to the cluster's metadata map if the fetch
/// fails or exceeds [`FETCH_TIMEOUT`]. Local files are described by their
/// URI alone. Results are LRU-cached per URI.
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
    /// cluster_map | uri. Only successful fetches are cached: the cluster-map
    /// fallback lacks the artist list, and caching it would pin that degraded
    /// record for the LRU's lifetime — left uncached, the next play of the
    /// item retries the full fetch (and the persisted row is marked for the
    /// repair pass).
    ///
    /// The record fetch runs under [`FETCH_TIMEOUT`], concurrently with the
    /// lyrics fetch for tracks (latency = max, not sum). A failed lyrics
    /// fetch never fails the item — it only degrades to `None`.
    #[instrument(
        name = "metadata.resolve",
        skip(self, cluster_meta),
        fields(
            track_uri = %track_uri,
            media.kind = kind.as_str(),
            source = tracing::field::Empty,
            lyrics = tracing::field::Empty,
            fetch.timeout = tracing::field::Empty,
        )
    )]
    pub async fn resolve(
        &self,
        track_uri: &str,
        kind: MediaKind,
        cluster_meta: &HashMap<String, String>,
        duration_hint_ms: i64,
    ) -> anyhow::Result<TrackMeta> {
        let span = tracing::Span::current();
        if kind == MediaKind::Local {
            span.record("source", "uri");
            return local_meta(track_uri).context("local file uri without metadata");
        }
        let m = metrics();
        if let Some(hit) = self.cache.lock().await.get(track_uri) {
            span.record("source", "cache");
            m.cache_hits.add(1, &[]);
            return Ok(hit.clone());
        }
        m.cache_misses.add(1, &[]);

        let record_fut = async {
            let started = Instant::now();
            let fetched = self.fetch_bounded(track_uri, kind).await;
            let outcome = fetched.as_ref().map_or_else(FetchError::label, |_| "ok");
            m.fetch_duration.record(
                started.elapsed().as_secs_f64(),
                &[KeyValue::new("outcome", outcome)],
            );
            fetched
        };
        let lyrics_fut = async {
            let (MediaKind::Track, Ok((track_id, _))) = (kind, media_link(track_uri)) else {
                return None;
            };
            let started = Instant::now();
            let lyrics = self.fetcher.fetch_lyrics(&track_id).await;
            m.lyrics_duration.record(
                started.elapsed().as_secs_f64(),
                &[KeyValue::new("outcome", lyrics.label())],
            );
            Some(lyrics)
        };
        let (fetched, lyrics_fetch) = tokio::join!(record_fut, lyrics_fut);
        let lyrics = lyrics_fetch.and_then(|l| {
            m.lyrics_fetch
                .add(1, &[KeyValue::new("outcome", l.label())]);
            span.record("lyrics", l.label());
            l.into_option()
        });

        match fetched {
            Ok(mut meta) => {
                span.record("source", "fetch");
                if lyrics.is_some() {
                    meta.lyrics = lyrics;
                }
                self.cache
                    .lock()
                    .await
                    .put(track_uri.to_string(), meta.clone());
                Ok(meta)
            }
            Err(e) => {
                span.record("source", "cluster_map");
                warn!(error = %e, "metadata fetch failed, falling back to cluster map");
                from_cluster_map(cluster_meta, duration_hint_ms)
                    .context("cluster metadata map insufficient")
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn fetcher_for_tests(&self) -> &F {
        &self.fetcher
    }

    /// Fetch a record bypassing cache and cluster-map fallback, for the
    /// repair pass. Not cached: the result carries no lyrics, and caching it
    /// would strip them from the next live play of the track.
    pub async fn fetch_fresh(&self, uri: &str, kind: MediaKind) -> Result<TrackMeta, FetchError> {
        self.fetch_bounded(uri, kind).await
    }

    /// Records `fetch.timeout` on the current span.
    async fn fetch_bounded(&self, uri: &str, kind: MediaKind) -> Result<TrackMeta, FetchError> {
        let result = tokio::time::timeout(FETCH_TIMEOUT, self.fetcher.fetch(uri, kind)).await;
        tracing::Span::current().record("fetch.timeout", result.is_err());
        result.unwrap_or(Err(FetchError::TimedOut))
    }
}

fn widest_cover_url(covers: &Images, template: &str) -> Option<String> {
    covers
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
        lyrics: None,
        source: MetadataSource::ClusterMap,
    })
}

/// Cluster image references may be `spotify:image:<hex>` URIs instead of URLs.
fn normalize_image_url(value: &str) -> String {
    match value.strip_prefix("spotify:image:") {
        Some(file_id) => format!("https://i.scdn.co/image/{file_id}"),
        None => value.to_string(),
    }
}

/// Local file URIs carry their own metadata
/// (`spotify:local:<artist>:<album>:<title>:<seconds>`), form-encoded: `+`
/// is a space, `%XX` an escaped byte. No cover, no network.
fn local_meta(uri: &str) -> Option<TrackMeta> {
    let SpotifyUri::Local {
        artist,
        album_title,
        track_title,
        duration,
    } = SpotifyUri::from_uri(uri).ok()?
    else {
        return None;
    };
    let artist = form_decode(&artist);
    Some(TrackMeta {
        title: form_decode(&track_title),
        artists: (!artist.is_empty()).then_some(artist).into_iter().collect(),
        album: form_decode(&album_title),
        cover_url: None,
        duration_ms: u32::try_from(duration.as_millis()).unwrap_or(u32::MAX),
        lyrics: None,
        source: MetadataSource::Uri,
    })
}

/// `application/x-www-form-urlencoded` component decoding; malformed escapes
/// pass through literally, invalid UTF-8 is replaced.
fn form_decode(component: &str) -> String {
    let bytes = component.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let hex = |i: usize| bytes.get(i).and_then(|&b| (b as char).to_digit(16));
    let mut i = 0;
    while i < bytes.len() {
        match (bytes[i], hex(i + 1), hex(i + 2)) {
            (b'+', ..) => out.push(b' '),
            (b'%', Some(hi), Some(lo)) => {
                out.push((hi * 16 + lo) as u8);
                i += 2;
            }
            (b, ..) => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Item id plus its public link for a cluster URI: `/track/` or `/episode/`,
/// no link for local files (their id is the opaque URI remainder).
pub fn media_link(uri: &str) -> anyhow::Result<(String, Option<String>)> {
    let kind = classify(uri).map_err(|e| anyhow::anyhow!("unsupported media uri {uri}: {e:?}"))?;
    let id = SpotifyUri::from_uri(uri)
        .context("parsing media uri")?
        .to_id()
        .map_err(|e| anyhow::anyhow!("media uri has no id: {e}"))?;
    let url = kind.open_url(&id);
    Ok((id, url))
}

/// Scripted fetcher for tests here and in `spotify::mod` (handle_update
/// integration): pops one canned response per record fetch; lyrics fetches
/// always report `Missing`, so scripted lyrics ride the scripted record.
#[cfg(test)]
pub(crate) mod test_support {
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{FetchError, FetchTrack, TrackMeta};
    use crate::events::MediaKind;
    use crate::spotify::lyrics::LyricsFetch;

    pub(crate) enum Scripted {
        Meta(TrackMeta),
        Unavailable,
        NotFound,
        /// Never completes; exercises the fetch timeout.
        Hang,
    }

    pub(crate) struct ScriptedFetcher {
        responses: Mutex<VecDeque<Scripted>>,
        pub(crate) calls: AtomicUsize,
        pub(crate) kinds: Mutex<Vec<MediaKind>>,
    }

    impl ScriptedFetcher {
        /// `None` scripts an `Unavailable` failure.
        pub(crate) fn new(responses: Vec<Option<TrackMeta>>) -> Self {
            Self::scripted(
                responses
                    .into_iter()
                    .map(|r| r.map_or(Scripted::Unavailable, Scripted::Meta))
                    .collect(),
            )
        }

        pub(crate) fn scripted(responses: Vec<Scripted>) -> Self {
            Self {
                responses: Mutex::new(responses.into()),
                calls: AtomicUsize::new(0),
                kinds: Mutex::new(Vec::new()),
            }
        }

        pub(crate) fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl FetchTrack for ScriptedFetcher {
        async fn fetch(&self, _uri: &str, kind: MediaKind) -> Result<TrackMeta, FetchError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.kinds.lock().unwrap().push(kind);
            let next = self.responses.lock().unwrap().pop_front();
            match next {
                Some(Scripted::Meta(meta)) => Ok(meta),
                Some(Scripted::NotFound) => Err(FetchError::NotFound(anyhow::anyhow!("scripted"))),
                Some(Scripted::Hang) => std::future::pending().await,
                Some(Scripted::Unavailable) | None => {
                    Err(FetchError::Unavailable(anyhow::anyhow!("scripted")))
                }
            }
        }

        async fn fetch_lyrics(&self, _track_id: &str) -> LyricsFetch {
            LyricsFetch::Missing
        }
    }

    pub(crate) fn full_meta(title: &str) -> TrackMeta {
        TrackMeta {
            title: title.into(),
            artists: vec!["Artist".into()],
            album: "Album".into(),
            cover_url: Some("https://i.scdn.co/image/abc".into()),
            duration_ms: 200_000,
            lyrics: None,
            source: crate::events::MetadataSource::Fetch,
        }
    }

    pub(crate) fn full_meta_with_lyrics(title: &str) -> TrackMeta {
        TrackMeta {
            lyrics: Some(crate::events::Lyrics {
                lines: vec![
                    crate::events::LyricLine {
                        start_ms: 1_000,
                        end_ms: 4_200,
                        text: "Nzqmr gswby lkvv ehm tp".into(),
                    },
                    crate::events::LyricLine {
                        start_ms: 4_200,
                        end_ms: 8_000,
                        text: "Nzqmr gswby lkvv ehm dgnn".into(),
                    },
                ],
            }),
            ..full_meta(title)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{Scripted, ScriptedFetcher, full_meta, full_meta_with_lyrics};
    use super::*;

    #[tokio::test]
    async fn lyrics_ride_the_track_meta_and_its_cache() {
        let resolver = MetadataResolver::with_fetcher(ScriptedFetcher::new(vec![Some(
            full_meta_with_lyrics("Song A"),
        )]));
        let meta = resolver
            .resolve("spotify:track:a", MediaKind::Track, &HashMap::new(), 0)
            .await
            .unwrap();
        assert_eq!(meta.lyrics.as_ref().unwrap().lines.len(), 2);

        let cached = resolver
            .resolve("spotify:track:a", MediaKind::Track, &HashMap::new(), 0)
            .await
            .unwrap();
        assert_eq!(cached.lyrics, meta.lyrics);
    }

    #[tokio::test]
    async fn resolve_caches_successful_fetches() {
        let resolver =
            MetadataResolver::with_fetcher(ScriptedFetcher::new(vec![Some(full_meta("Hit"))]));
        let map = HashMap::new();

        let first = resolver
            .resolve("spotify:track:x", MediaKind::Track, &map, 0)
            .await
            .unwrap();
        assert_eq!(first.title, "Hit");
        assert_eq!(resolver.fetcher.call_count(), 1);

        let second = resolver
            .resolve("spotify:track:x", MediaKind::Track, &map, 0)
            .await
            .unwrap();
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
            .resolve("spotify:track:x", MediaKind::Track, &map, 1_000)
            .await
            .unwrap();
        assert_eq!(degraded.title, "Fallback Title");
        assert!(degraded.artists.is_empty());

        let healed = resolver
            .resolve("spotify:track:x", MediaKind::Track, &map, 1_000)
            .await
            .unwrap();
        assert_eq!(healed.title, "Recovered");
        assert_eq!(healed.artists, vec!["Artist".to_string()]);
        assert_eq!(resolver.fetcher.call_count(), 2);

        resolver
            .resolve("spotify:track:x", MediaKind::Track, &map, 1_000)
            .await
            .unwrap();
        assert_eq!(resolver.fetcher.call_count(), 2);
    }

    #[tokio::test]
    async fn failed_fetch_without_usable_fallback_is_an_error() {
        let resolver = MetadataResolver::with_fetcher(ScriptedFetcher::new(vec![None]));
        let result = resolver
            .resolve("spotify:track:x", MediaKind::Track, &HashMap::new(), 0)
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
        assert_eq!(meta.source, MetadataSource::ClusterMap);
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
    fn media_links_per_kind() {
        let (id, url) = media_link("spotify:track:4uLU6hMCjMI75M1A2tKUQC").unwrap();
        assert_eq!(id, "4uLU6hMCjMI75M1A2tKUQC");
        assert_eq!(
            url.as_deref(),
            Some("https://open.spotify.com/track/4uLU6hMCjMI75M1A2tKUQC")
        );

        let (id, url) = media_link("spotify:episode:512ojhOuo1ktJprKbVcKyQ").unwrap();
        assert_eq!(id, "512ojhOuo1ktJprKbVcKyQ");
        assert_eq!(
            url.as_deref(),
            Some("https://open.spotify.com/episode/512ojhOuo1ktJprKbVcKyQ")
        );

        let (id, url) = media_link("spotify:local:Artist:Album:Title:127").unwrap();
        assert_eq!(id, "Artist:Album:Title:127");
        assert_eq!(url, None);

        assert!(media_link("spotify:ad:5sWHDYs0csV6RS48xBl0tH").is_err());
    }

    #[test]
    fn local_file_metadata_comes_from_the_form_encoded_uri() {
        let meta = local_meta(
            "spotify:local:David+Wise:Donkey+Kong+Country%3A+Tropical+Freeze:Snomads+Island:127",
        )
        .unwrap();
        assert_eq!(meta.title, "Snomads Island");
        assert_eq!(meta.artists, ["David Wise"]);
        assert_eq!(meta.album, "Donkey Kong Country: Tropical Freeze");
        assert_eq!(meta.duration_ms, 127_000);
        assert_eq!(meta.source, MetadataSource::Uri);

        let anonymous = local_meta("spotify:local:::Untitled:5").unwrap();
        assert!(anonymous.artists.is_empty());
    }

    #[test]
    fn form_decode_is_lenient() {
        assert_eq!(form_decode("a+b%20c"), "a b c");
        assert_eq!(form_decode("100%"), "100%");
        assert_eq!(form_decode("%zz"), "%zz");
        assert_eq!(form_decode("%C3%BCber"), "über");
    }

    /// Local files never touch the network.
    #[tokio::test]
    async fn local_resolution_skips_the_fetcher() {
        let resolver = MetadataResolver::with_fetcher(ScriptedFetcher::new(vec![]));
        let meta = resolver
            .resolve(
                "spotify:local:Artist:Album:Title:60",
                MediaKind::Local,
                &HashMap::new(),
                0,
            )
            .await
            .unwrap();
        assert_eq!(meta.title, "Title");
        assert_eq!(resolver.fetcher.call_count(), 0);
    }

    /// A hung record fetch must not stall the caller beyond FETCH_TIMEOUT;
    /// the cluster-map fallback takes over and is marked degraded.
    #[tokio::test(start_paused = true)]
    async fn hung_fetch_times_out_into_the_cluster_map_fallback() {
        let resolver =
            MetadataResolver::with_fetcher(ScriptedFetcher::scripted(vec![Scripted::Hang]));
        let mut map = HashMap::new();
        map.insert("title".to_string(), "Fallback".to_string());

        let started = tokio::time::Instant::now();
        let meta = resolver
            .resolve("spotify:track:x", MediaKind::Track, &map, 1_000)
            .await
            .unwrap();
        assert_eq!(meta.title, "Fallback");
        assert_eq!(meta.source, MetadataSource::ClusterMap);
        assert_eq!(started.elapsed(), FETCH_TIMEOUT);
    }

    /// The repair pass needs the raw error class: no cluster-map fallback,
    /// no caching.
    #[tokio::test]
    async fn fetch_fresh_reports_not_found_without_fallback() {
        let resolver = MetadataResolver::with_fetcher(ScriptedFetcher::scripted(vec![
            Scripted::NotFound,
            Scripted::Meta(full_meta("Fresh")),
        ]));
        let err = resolver
            .fetch_fresh("spotify:track:x", MediaKind::Track)
            .await
            .unwrap_err();
        assert!(err.is_not_found());
        let fresh = resolver
            .fetch_fresh("spotify:track:x", MediaKind::Track)
            .await
            .unwrap();
        assert_eq!(fresh.title, "Fresh");
        assert!(resolver.cache.lock().await.is_empty());
    }

    #[test]
    fn fetch_error_labels_separate_timeouts() {
        assert_eq!(FetchError::TimedOut.label(), "timeout");
        assert!(!FetchError::TimedOut.is_not_found());
        assert_eq!(FetchError::NotFound(anyhow::anyhow!("x")).label(), "error");
        assert_eq!(
            FetchError::Unavailable(anyhow::anyhow!("x")).label(),
            "error"
        );
    }

    #[tokio::test]
    async fn episodes_are_fetched_as_episodes() {
        let resolver =
            MetadataResolver::with_fetcher(ScriptedFetcher::new(vec![Some(full_meta("Pod"))]));
        resolver
            .resolve(
                "spotify:episode:512ojhOuo1ktJprKbVcKyQ",
                MediaKind::Episode,
                &HashMap::new(),
                0,
            )
            .await
            .unwrap();
        assert_eq!(
            *resolver.fetcher.kinds.lock().unwrap(),
            [MediaKind::Episode]
        );
    }
}
