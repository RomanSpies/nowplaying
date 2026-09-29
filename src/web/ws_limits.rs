//! Admission control for WebSocket sessions: a global ceiling plus a
//! per-client-address ceiling. Each accepted session holds a [`WsPermit`]
//! whose drop releases both slots, so no code path can leak a slot.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Why a session was refused; `as_str` feeds the rejection log's `reason`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rejection {
    GlobalLimit,
    PerIpLimit,
}

impl Rejection {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::GlobalLimit => "global_limit",
            Self::PerIpLimit => "per_ip_limit",
        }
    }
}

pub struct WsLimits {
    global: Arc<Semaphore>,
    max_per_ip: usize,
    per_ip: Mutex<HashMap<String, usize>>,
}

impl WsLimits {
    pub fn new(max_connections: usize, max_per_ip: usize) -> Self {
        Self {
            global: Arc::new(Semaphore::new(max_connections)),
            max_per_ip,
            per_ip: Mutex::new(HashMap::new()),
        }
    }

    /// Claim a slot for `client`. The per-address check runs first so a
    /// single noisy client is reported as such even when the service is full.
    pub fn try_admit(self: &Arc<Self>, client: &str) -> Result<WsPermit, Rejection> {
        {
            let mut per_ip = self.per_ip.lock().unwrap_or_else(|e| e.into_inner());
            let count = per_ip.entry(client.to_owned()).or_default();
            if *count >= self.max_per_ip {
                return Err(Rejection::PerIpLimit);
            }
            *count += 1;
        }
        match self.global.clone().try_acquire_owned() {
            Ok(global) => Ok(WsPermit {
                limits: self.clone(),
                client: client.to_owned(),
                _global: global,
            }),
            Err(_) => {
                self.release(client);
                Err(Rejection::GlobalLimit)
            }
        }
    }

    fn release(&self, client: &str) {
        let mut per_ip = self.per_ip.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(count) = per_ip.get_mut(client) {
            *count -= 1;
            if *count == 0 {
                per_ip.remove(client);
            }
        }
    }

    #[cfg(test)]
    fn tracked_clients(&self) -> usize {
        self.per_ip.lock().unwrap().len()
    }
}

/// Held for a session's lifetime; dropping it frees the global slot and the
/// client's per-address slot.
pub struct WsPermit {
    limits: Arc<WsLimits>,
    client: String,
    _global: OwnedSemaphorePermit,
}

impl Drop for WsPermit {
    fn drop(&mut self) {
        self.limits.release(&self.client);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn per_ip_limit_applies_per_address() {
        let limits = Arc::new(WsLimits::new(10, 2));
        let a1 = limits.try_admit("198.51.100.1").unwrap();
        let _a2 = limits.try_admit("198.51.100.1").unwrap();
        assert_eq!(
            limits.try_admit("198.51.100.1").err(),
            Some(Rejection::PerIpLimit)
        );
        let _b = limits.try_admit("198.51.100.2").unwrap();
        drop(a1);
        assert!(limits.try_admit("198.51.100.1").is_ok());
    }

    #[test]
    fn global_limit_refuses_and_rolls_back_the_address_slot() {
        let limits = Arc::new(WsLimits::new(1, 5));
        let held = limits.try_admit("a").unwrap();
        assert_eq!(limits.try_admit("b").err(), Some(Rejection::GlobalLimit));
        assert_eq!(
            limits.tracked_clients(),
            1,
            "refused client leaves no entry"
        );
        drop(held);
        assert_eq!(limits.tracked_clients(), 0);
        assert!(limits.try_admit("b").is_ok());
    }
}
