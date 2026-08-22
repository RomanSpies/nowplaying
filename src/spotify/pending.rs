//! Deferred play persistence with listened-time accounting.
//!
//! A play is written to the database only after `min_play` of *actual
//! listening*: pausing suspends the countdown, resuming continues it, and a
//! stop or track change before the threshold discards the play entirely.

use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use opentelemetry::KeyValue;
use opentelemetry::metrics::Counter;
use tokio::task::JoinHandle;
use tracing::{Instrument, info, info_span};

use crate::events::PlayEvent;

/// Action run once a play qualifies. Injected so the timer logic is testable
/// without a database.
pub type PersistFn = Arc<dyn Fn(PlayEvent) -> BoxFuture<'static, ()> + Send + Sync>;

/// Why a pending play was dropped before qualifying (metric label).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DiscardReason {
    /// A new track started before the threshold.
    Superseded,
    /// Playback stopped (cluster reported not-playing).
    Stopped,
    /// The Spotify connection ended (reconnect or shutdown).
    Disconnected,
}

impl DiscardReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::Superseded => "superseded",
            Self::Stopped => "stopped",
            Self::Disconnected => "disconnected",
        }
    }
}

/// Pure listened-time bookkeeping: completed playing segments plus the
/// currently running one. Milliseconds; the clock is injected so tests are
/// deterministic.
#[derive(Debug)]
pub struct ListenClock {
    listened_ms: u64,
    /// `Some(start)` while playing.
    playing_since_ms: Option<i64>,
}

impl ListenClock {
    pub fn new(playing: bool, now_ms: i64) -> Self {
        Self {
            listened_ms: 0,
            playing_since_ms: playing.then_some(now_ms),
        }
    }

    /// Idempotent; returns whether the state actually flipped.
    pub fn set_playing(&mut self, playing: bool, now_ms: i64) -> bool {
        match (self.playing_since_ms, playing) {
            (None, true) => {
                self.playing_since_ms = Some(now_ms);
                true
            }
            (Some(since), false) => {
                self.listened_ms += (now_ms - since).max(0) as u64;
                self.playing_since_ms = None;
                true
            }
            _ => false,
        }
    }

    pub fn listened_ms(&self, now_ms: i64) -> u64 {
        let running = self
            .playing_since_ms
            .map_or(0, |since| (now_ms - since).max(0) as u64);
        self.listened_ms + running
    }

    pub fn remaining_ms(&self, min_play_ms: u64, now_ms: i64) -> u64 {
        min_play_ms.saturating_sub(self.listened_ms(now_ms))
    }
}

/// A detected play that has not (necessarily) been persisted yet. Owns the
/// timer task that fires `persist` once enough listening has accumulated.
///
/// Timer state invariant: `handle` is `Some` while a timer is armed or has
/// already fired, `None` while paused before the threshold. Segment durations
/// are measured with `tokio::time::Instant`, so tests can drive the clock via
/// `tokio::time::pause`/`advance`.
pub struct PendingPersist {
    event: PlayEvent,
    clock: ListenClock,
    min_play_ms: u64,
    epoch: tokio::time::Instant,
    handle: Option<JoinHandle<()>>,
    persist: PersistFn,
    discarded: Counter<u64>,
}

impl PendingPersist {
    /// A play detected in paused state (e.g. startup into a long-paused
    /// session) arms nothing until it actually resumes.
    pub fn new(
        event: PlayEvent,
        initially_paused: bool,
        min_play: Duration,
        persist: PersistFn,
        discarded: Counter<u64>,
    ) -> Self {
        let mut this = Self {
            event,
            clock: ListenClock::new(!initially_paused, 0),
            min_play_ms: min_play.as_millis() as u64,
            epoch: tokio::time::Instant::now(),
            handle: None,
            persist,
            discarded,
        };
        if !initially_paused {
            this.arm();
        }
        this
    }

    fn now_ms(&self) -> i64 {
        self.epoch.elapsed().as_millis() as i64
    }

    /// Feed the current paused flag from a cluster snapshot. Idempotent, so
    /// it can be called on every same-track update (including seeks, which
    /// keep wall-clock listening running and thus don't touch the clock).
    ///
    /// The abort/fire race on pause (timer fires between sleep-end and abort)
    /// and the near-zero re-arm on late resume are both absorbed by the
    /// idempotent, replay-guarded insert keyed on (track_id, started_at).
    /// A fired handle stays in place: it marks the play as persisted, so a
    /// later resume must not re-arm.
    pub fn set_paused(&mut self, paused: bool) {
        if !self.clock.set_playing(!paused, self.now_ms()) {
            return;
        }
        if paused {
            if self.handle.as_ref().is_some_and(|h| !h.is_finished())
                && let Some(h) = self.handle.take()
            {
                h.abort();
            }
        } else if self.handle.is_none() {
            self.arm();
        }
    }

    /// The deadline is fixed at arm time rather than on the spawned task's
    /// first poll: "remaining counts from the arm" must not depend on
    /// scheduler latency, and a lazily created sleep would anchor to whenever
    /// the task first runs. The task is instrumented with a `play.persist`
    /// span — `arm` executes inside `cluster.handle_update`, so the deferred
    /// insert shows up (late) in the trace of the play it belongs to instead
    /// of as an orphan root span.
    fn arm(&mut self) {
        let remaining =
            Duration::from_millis(self.clock.remaining_ms(self.min_play_ms, self.now_ms()));
        let deadline = tokio::time::Instant::now() + remaining;
        let persist = self.persist.clone();
        let event = self.event.clone();
        let span = info_span!(
            "play.persist",
            track_id = %event.track_id,
            remaining_ms = remaining.as_millis() as u64,
            otel.status_code = tracing::field::Empty,
        );
        self.handle = Some(tokio::spawn(
            async move {
                tokio::time::sleep_until(deadline).await;
                persist(event).await;
            }
            .instrument(span),
        ));
    }

    /// Drop the play before it qualified. No-op (and no metric) if the timer
    /// already fired — then the play was persisted, not discarded.
    pub fn discard(self, reason: DiscardReason) {
        if let Some(h) = &self.handle {
            if h.is_finished() {
                return;
            }
            h.abort();
        }
        self.discarded
            .add(1, &[KeyValue::new("reason", reason.as_str())]);
        info!(
            track_id = %self.event.track_id,
            reason = reason.as_str(),
            "discarding unqualified play"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use opentelemetry::global;

    use super::*;

    #[test]
    fn clock_accumulates_only_playing_segments() {
        let mut c = ListenClock::new(true, 0);
        assert!(c.set_playing(false, 10_000));
        assert_eq!(c.listened_ms(25_000), 10_000);
        assert!(c.set_playing(true, 30_000));
        assert_eq!(c.listened_ms(55_000), 35_000);
        assert_eq!(c.remaining_ms(40_000, 55_000), 5_000);
    }

    #[test]
    fn clock_starting_paused_accrues_nothing() {
        let mut c = ListenClock::new(false, 0);
        assert_eq!(c.listened_ms(3_600_000), 0);
        assert_eq!(c.remaining_ms(30_000, 3_600_000), 30_000);
        assert!(c.set_playing(true, 3_600_000));
        assert_eq!(c.listened_ms(3_610_000), 10_000);
    }

    #[test]
    fn clock_transitions_are_idempotent() {
        let mut c = ListenClock::new(true, 0);
        assert!(!c.set_playing(true, 5_000));
        assert!(c.set_playing(false, 10_000));
        assert!(!c.set_playing(false, 20_000));
        assert_eq!(c.listened_ms(20_000), 10_000);
    }

    #[test]
    fn clock_saturates_on_backwards_time() {
        let mut c = ListenClock::new(true, 1_000);
        assert_eq!(c.listened_ms(500), 0);
        assert!(c.set_playing(false, 500));
        assert_eq!(c.listened_ms(500), 0);
    }

    fn sample_event() -> PlayEvent {
        PlayEvent {
            track_id: "t".into(),
            track_url: "https://open.spotify.com/track/t".into(),
            title: "T".into(),
            artists: vec!["A".into()],
            album: "Al".into(),
            cover_url: None,
            duration_ms: 200_000,
            started_at: "2026-07-28T12:00:00Z".parse().unwrap(),
            lyrics: None,
        }
    }

    fn counting_persist() -> (PersistFn, Arc<AtomicUsize>) {
        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();
        let persist: PersistFn = Arc::new(move |_event| {
            let c = c.clone();
            Box::pin(async move {
                c.fetch_add(1, Ordering::SeqCst);
            })
        });
        (persist, count)
    }

    fn discard_counter() -> Counter<u64> {
        global::meter("test")
            .u64_counter("plays_discarded_total")
            .build()
    }

    /// Let spawned timer tasks run after advancing the paused clock.
    async fn settle() {
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
    }

    const MIN_PLAY: Duration = Duration::from_millis(30_000);

    #[tokio::test(start_paused = true)]
    async fn persists_after_uninterrupted_min_play() {
        let (persist, count) = counting_persist();
        let _p = PendingPersist::new(sample_event(), false, MIN_PLAY, persist, discard_counter());
        tokio::time::advance(Duration::from_millis(29_999)).await;
        settle().await;
        assert_eq!(count.load(Ordering::SeqCst), 0);
        tokio::time::advance(Duration::from_millis(2)).await;
        settle().await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn pause_suspends_the_countdown() {
        let (persist, count) = counting_persist();
        let mut p =
            PendingPersist::new(sample_event(), false, MIN_PLAY, persist, discard_counter());
        tokio::time::advance(Duration::from_millis(10_000)).await;
        p.set_paused(true);
        tokio::time::advance(Duration::from_millis(300_000)).await;
        settle().await;
        assert_eq!(
            count.load(Ordering::SeqCst),
            0,
            "paused play must not persist"
        );
        p.set_paused(false);
        tokio::time::advance(Duration::from_millis(19_999)).await;
        settle().await;
        assert_eq!(count.load(Ordering::SeqCst), 0);
        tokio::time::advance(Duration::from_millis(2)).await;
        settle().await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn initially_paused_arms_only_on_resume() {
        let (persist, count) = counting_persist();
        let mut p = PendingPersist::new(sample_event(), true, MIN_PLAY, persist, discard_counter());
        tokio::time::advance(Duration::from_millis(3_600_000)).await;
        settle().await;
        assert_eq!(count.load(Ordering::SeqCst), 0);
        p.set_paused(false);
        tokio::time::advance(Duration::from_millis(30_001)).await;
        settle().await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn discard_before_threshold_never_persists() {
        let (persist, count) = counting_persist();
        let p = PendingPersist::new(sample_event(), false, MIN_PLAY, persist, discard_counter());
        tokio::time::advance(Duration::from_millis(20_000)).await;
        p.discard(DiscardReason::Superseded);
        tokio::time::advance(Duration::from_millis(60_000)).await;
        settle().await;
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn no_second_persist_after_qualifying() {
        let (persist, count) = counting_persist();
        let mut p =
            PendingPersist::new(sample_event(), false, MIN_PLAY, persist, discard_counter());
        tokio::time::advance(Duration::from_millis(30_001)).await;
        settle().await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
        p.set_paused(true);
        p.set_paused(false);
        tokio::time::advance(Duration::from_millis(120_000)).await;
        settle().await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
        p.discard(DiscardReason::Stopped);
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}
