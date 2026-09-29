//! In-process cache for the 30-day top lists.
//!
//! Every `/api/top` request used to run two aggregate queries on the pool it
//! shares with the play inserts, so a request flood could starve persistence.
//! The cache holds one snapshot at the maximum depth ([`TOP_CACHE_DEPTH`]);
//! any smaller `limit` is a prefix slice of it, so a single refresh serves
//! all limits. Refreshes are single-flight, and a successful persist bumps a
//! generation counter that invalidates the snapshot immediately — including
//! one whose refresh was already in flight when the play landed.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::time::Instant;

use crate::db::{TopArtist, TopSong};

/// Maximum `limit` accepted by `/api/top`, and the depth every refresh loads.
pub const TOP_CACHE_DEPTH: i64 = 50;
/// Upper bound on staleness when no play lands (the rolling window slides).
pub const TOP_CACHE_TTL: Duration = Duration::from_secs(60);

#[derive(Debug)]
pub struct TopLists {
    pub songs: Vec<TopSong>,
    pub artists: Vec<TopArtist>,
}

/// How a request was served, for the `cache` span field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheOutcome {
    Hit,
    /// No snapshot existed yet.
    Miss,
    /// A stale or invalidated snapshot was replaced.
    Refresh,
}

impl CacheOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
            Self::Refresh => "refresh",
        }
    }
}

/// A served snapshot plus how it was obtained and how old it is.
pub struct Served {
    pub lists: Arc<TopLists>,
    pub outcome: CacheOutcome,
    pub age: Duration,
}

struct Entry {
    lists: Arc<TopLists>,
    loaded_at: Instant,
    generation: u64,
}

pub struct TopCache {
    ttl: Duration,
    entry: RwLock<Option<Entry>>,
    refresh: tokio::sync::Mutex<()>,
    generation: AtomicU64,
}

impl Default for TopCache {
    fn default() -> Self {
        Self::new(TOP_CACHE_TTL)
    }
}

impl TopCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            entry: RwLock::new(None),
            refresh: tokio::sync::Mutex::new(()),
            generation: AtomicU64::new(0),
        }
    }

    /// Mark the current snapshot stale; the next request reloads.
    pub fn invalidate(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    fn fresh(&self) -> Option<Served> {
        let guard = self.entry.read().unwrap_or_else(|e| e.into_inner());
        let entry = guard.as_ref()?;
        let age = entry.loaded_at.elapsed();
        (age < self.ttl && entry.generation == self.generation.load(Ordering::Acquire)).then(|| {
            Served {
                lists: entry.lists.clone(),
                outcome: CacheOutcome::Hit,
                age,
            }
        })
    }

    /// Serve the snapshot, reloading it via `load` when stale. Concurrent
    /// misses wait for the one in-flight load instead of issuing their own.
    /// Load errors are returned, never cached.
    pub async fn get_or_load<F, Fut, E>(&self, load: F) -> Result<Served, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<TopLists, E>>,
    {
        if let Some(hit) = self.fresh() {
            return Ok(hit);
        }
        let _single_flight = self.refresh.lock().await;
        if let Some(hit) = self.fresh() {
            return Ok(hit);
        }
        let generation = self.generation.load(Ordering::Acquire);
        let lists = Arc::new(load().await?);
        let mut guard = self.entry.write().unwrap_or_else(|e| e.into_inner());
        let outcome = if guard.is_some() {
            CacheOutcome::Refresh
        } else {
            CacheOutcome::Miss
        };
        *guard = Some(Entry {
            lists: lists.clone(),
            loaded_at: Instant::now(),
            generation,
        });
        Ok(Served {
            lists,
            outcome,
            age: Duration::ZERO,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    fn lists(n: usize) -> TopLists {
        TopLists {
            songs: Vec::new(),
            artists: (0..n)
                .map(|i| TopArtist {
                    artist: format!("a{i}"),
                    plays: 1,
                })
                .collect(),
        }
    }

    async fn serve(cache: &TopCache, loads: &AtomicUsize) -> Served {
        cache
            .get_or_load(|| async {
                let n = loads.fetch_add(1, Ordering::SeqCst) + 1;
                Ok::<_, ()>(lists(n))
            })
            .await
            .unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn hit_until_ttl_then_refresh() {
        let cache = TopCache::new(Duration::from_secs(60));
        let loads = AtomicUsize::new(0);
        assert_eq!(serve(&cache, &loads).await.outcome, CacheOutcome::Miss);
        tokio::time::advance(Duration::from_secs(30)).await;
        let hit = serve(&cache, &loads).await;
        assert_eq!(hit.outcome, CacheOutcome::Hit);
        assert_eq!(hit.age, Duration::from_secs(30));
        tokio::time::advance(Duration::from_secs(31)).await;
        assert_eq!(serve(&cache, &loads).await.outcome, CacheOutcome::Refresh);
        assert_eq!(loads.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn invalidate_forces_a_reload() {
        let cache = TopCache::default();
        let loads = AtomicUsize::new(0);
        serve(&cache, &loads).await;
        cache.invalidate();
        let served = serve(&cache, &loads).await;
        assert_eq!(served.outcome, CacheOutcome::Refresh);
        assert_eq!(served.lists.artists.len(), 2);
    }

    /// A play persisted while a refresh is in flight must not be masked by
    /// that refresh's (already outdated) result.
    #[tokio::test(start_paused = true)]
    async fn invalidation_during_a_load_leaves_the_result_stale() {
        let cache = TopCache::default();
        let loads = AtomicUsize::new(0);
        cache
            .get_or_load(|| async {
                cache.invalidate();
                Ok::<_, ()>(lists(1))
            })
            .await
            .unwrap();
        assert_eq!(serve(&cache, &loads).await.outcome, CacheOutcome::Refresh);
    }

    #[tokio::test(start_paused = true)]
    async fn load_errors_are_not_cached() {
        let cache = TopCache::default();
        let failed = cache
            .get_or_load(|| async { Err::<TopLists, _>("db down") })
            .await;
        assert!(failed.is_err());
        let loads = AtomicUsize::new(0);
        assert_eq!(serve(&cache, &loads).await.outcome, CacheOutcome::Miss);
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_misses_share_one_load() {
        let cache = Arc::new(TopCache::default());
        let loads = Arc::new(AtomicUsize::new(0));
        let tasks: Vec<_> = (0..8)
            .map(|_| {
                let cache = cache.clone();
                let loads = loads.clone();
                tokio::spawn(async move {
                    cache
                        .get_or_load(|| async {
                            loads.fetch_add(1, Ordering::SeqCst);
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            Ok::<_, ()>(lists(1))
                        })
                        .await
                        .unwrap()
                        .outcome
                })
            })
            .collect();
        for t in tasks {
            t.await.unwrap();
        }
        assert_eq!(loads.load(Ordering::SeqCst), 1);
    }
}
