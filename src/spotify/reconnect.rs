//! Rate limiting for Spotify reconnects, mirroring the librespot CLI: at most
//! `max` failures within a sliding `window`, exponential backoff between
//! attempts, and a clean slate once a connection proves stable.

use std::collections::VecDeque;
use std::time::Duration;

use tokio::time::Instant;

const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// Too many failures within the window; the caller gives up and hands
/// recovery to systemd.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetExhausted {
    pub failures: usize,
    pub window: Duration,
}

impl std::fmt::Display for BudgetExhausted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "spotify reconnected too often ({} failures within {:?}); giving up",
            self.failures, self.window
        )
    }
}

impl std::error::Error for BudgetExhausted {}

/// Pure failure bookkeeping; time is passed in so tests need no clock.
#[derive(Debug)]
pub struct ReconnectBudget {
    max: usize,
    window: Duration,
    failures: VecDeque<Instant>,
    backoff: Duration,
}

impl ReconnectBudget {
    pub fn new(max: usize, window: Duration) -> Self {
        Self {
            max,
            window,
            failures: VecDeque::with_capacity(max + 1),
            backoff: INITIAL_BACKOFF,
        }
    }

    /// Record a failed connection that lived for `connection_lifetime` and
    /// return how long to wait before the next attempt. A connection that
    /// outlived the window resets the budget and the backoff first, so a
    /// long-stable service is not penalised for failures hours apart.
    pub fn on_failure(
        &mut self,
        now: Instant,
        connection_lifetime: Duration,
    ) -> Result<Duration, BudgetExhausted> {
        if connection_lifetime > self.window {
            self.reset();
        }
        while self
            .failures
            .front()
            .is_some_and(|t| now.duration_since(*t) >= self.window)
        {
            self.failures.pop_front();
        }
        self.failures.push_back(now);
        if self.failures.len() > self.max {
            return Err(BudgetExhausted {
                failures: self.failures.len(),
                window: self.window,
            });
        }
        let delay = self.backoff;
        self.backoff = (self.backoff * 2).min(MAX_BACKOFF);
        Ok(delay)
    }

    /// Forget all failures, e.g. after an operator fixed the credentials.
    pub fn reset(&mut self) {
        self.failures.clear();
        self.backoff = INITIAL_BACKOFF;
    }

    /// Failures still tolerated within the current window.
    pub fn remaining(&self) -> usize {
        self.max.saturating_sub(self.failures.len())
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    const WINDOW: Duration = Duration::from_secs(600);

    #[test]
    fn backoff_doubles_up_to_the_cap() {
        let mut b = ReconnectBudget::new(100, WINDOW);
        let t0 = Instant::now();
        let delays: Vec<_> = (0..8)
            .map(|i| {
                b.on_failure(t0 + Duration::from_secs(i), Duration::ZERO)
                    .unwrap()
            })
            .collect();
        assert_eq!(
            delays.iter().map(Duration::as_secs).collect::<Vec<_>>(),
            [1, 2, 4, 8, 16, 32, 60, 60]
        );
    }

    #[test]
    fn exceeding_max_within_the_window_exhausts() {
        let mut b = ReconnectBudget::new(5, WINDOW);
        let t0 = Instant::now();
        for i in 0..5 {
            b.on_failure(t0 + Duration::from_secs(i), Duration::ZERO)
                .unwrap();
        }
        assert_eq!(b.remaining(), 0);
        assert_eq!(
            b.on_failure(t0 + Duration::from_secs(5), Duration::ZERO),
            Err(BudgetExhausted {
                failures: 6,
                window: WINDOW
            })
        );
    }

    #[test]
    fn failures_age_out_of_the_window() {
        let mut b = ReconnectBudget::new(2, WINDOW);
        let t0 = Instant::now();
        b.on_failure(t0, Duration::ZERO).unwrap();
        b.on_failure(t0 + Duration::from_secs(1), Duration::ZERO)
            .unwrap();
        assert!(
            b.on_failure(t0 + WINDOW + Duration::from_secs(1), Duration::ZERO)
                .is_ok()
        );
    }

    #[test]
    fn a_stable_connection_resets_budget_and_backoff() {
        let mut b = ReconnectBudget::new(2, WINDOW);
        let t0 = Instant::now();
        b.on_failure(t0, Duration::ZERO).unwrap();
        b.on_failure(t0 + Duration::from_secs(1), Duration::ZERO)
            .unwrap();
        let delay = b
            .on_failure(t0 + Duration::from_secs(2), WINDOW + Duration::from_secs(1))
            .unwrap();
        assert_eq!(delay, INITIAL_BACKOFF);
        assert_eq!(b.remaining(), 1);
    }

    proptest! {
        /// Whatever the spacing, the budget never tolerates more than `max`
        /// failures inside any window, and it never refuses a failure while
        /// fewer than `max` earlier ones fall inside the window.
        #[test]
        fn never_more_than_max_failures_per_window(
            max in 1usize..8,
            gaps in proptest::collection::vec(0u64..400, 1..40),
        ) {
            let mut b = ReconnectBudget::new(max, WINDOW);
            let t0 = Instant::now();
            let mut t = t0;
            let mut accepted: Vec<Instant> = Vec::new();
            for gap in gaps {
                t += Duration::from_secs(gap);
                let in_window = accepted
                    .iter()
                    .filter(|a| t.duration_since(**a) < WINDOW)
                    .count();
                match b.on_failure(t, Duration::ZERO) {
                    Ok(delay) => {
                        prop_assert!(in_window < max);
                        prop_assert!(delay <= MAX_BACKOFF);
                        accepted.push(t);
                    }
                    Err(_) => {
                        prop_assert!(in_window >= max);
                        break;
                    }
                }
            }
        }
    }
}
