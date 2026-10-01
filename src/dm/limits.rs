//! Bounded replay cache and rate limiter for inbound direct messages.
//!
//! Both tables are fed by remote peers, so both are capped: nothing a peer
//! sends can grow them past the constants below.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

/// Replay-cache retention. Must exceed twice the accepted clock skew
/// (`wire::MAX_SKEW_MS`) so a message evicted from the cache is already
/// rejected by the timestamp check.
pub const DEDUPE_TTL: Duration = Duration::from_secs(25 * 60);
/// Cap on remembered `(fromId, id)` pairs. With [`GLOBAL_PER_WINDOW`] this
/// is comfortably more than a TTL's worth of accepted messages, so the cap
/// never evicts an entry that is still inside the timestamp window.
pub const DEDUPE_MAX: usize = 16_384;
/// Rate-limit window.
pub const WINDOW: Duration = Duration::from_secs(60);
/// Messages (and acks) accepted per sender node per window.
pub const PER_SENDER_PER_WINDOW: u32 = 30;
/// Messages (and acks) accepted from all senders per window.
pub const GLOBAL_PER_WINDOW: u32 = 300;
/// Cap on per-sender counters (expired ones are pruned first).
pub const MAX_SENDERS: usize = 2048;

#[derive(Debug, Default)]
pub struct Dedupe {
    order: VecDeque<(Instant, String)>,
    seen: HashSet<String>,
}

impl Dedupe {
    fn key(from_did: &str, id: &str) -> String {
        format!("{from_did}\n{id}")
    }

    fn prune(&mut self, now: Instant) {
        while let Some((at, _)) = self.order.front() {
            if now.saturating_duration_since(*at) <= DEDUPE_TTL && self.order.len() < DEDUPE_MAX {
                break;
            }
            let (_, key) = self.order.pop_front().expect("front exists");
            self.seen.remove(&key);
        }
    }

    /// Whether `(from_did, id)` was already accepted (read-only).
    pub fn contains(&self, from_did: &str, id: &str) -> bool {
        self.seen.contains(&Self::key(from_did, id))
    }

    /// Records `(from_did, id)`; `false` when it was already present.
    pub fn insert(&mut self, from_did: &str, id: &str, now: Instant) -> bool {
        self.prune(now);
        let key = Self::key(from_did, id);
        if !self.seen.insert(key.clone()) {
            return false;
        }
        self.order.push_back((now, key));
        true
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.seen.len()
    }
}

#[derive(Debug, Clone, Copy)]
struct Window {
    start: Instant,
    count: u32,
}

impl Window {
    fn fresh(now: Instant) -> Self {
        Self {
            start: now,
            count: 0,
        }
    }

    fn expired(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.start) >= WINDOW
    }
}

/// Fixed-window limiter: [`PER_SENDER_PER_WINDOW`] per transport sender and
/// [`GLOBAL_PER_WINDOW`] overall.
#[derive(Debug)]
pub struct RateLimiter {
    senders: HashMap<String, Window>,
    global: Window,
}

impl RateLimiter {
    pub fn new(now: Instant) -> Self {
        Self {
            senders: HashMap::new(),
            global: Window::fresh(now),
        }
    }

    /// Charges one message from transport sender `from`; `false` = over limit.
    pub fn allow(&mut self, from: &str, now: Instant) -> bool {
        if self.global.expired(now) {
            self.global = Window::fresh(now);
        }
        if self.global.count >= GLOBAL_PER_WINDOW {
            return false;
        }
        if !self.senders.contains_key(from) && self.senders.len() >= MAX_SENDERS {
            self.senders.retain(|_, w| !w.expired(now));
            if self.senders.len() >= MAX_SENDERS {
                return false;
            }
        }
        let window = self
            .senders
            .entry(from.to_string())
            .or_insert_with(|| Window::fresh(now));
        if window.expired(now) {
            *window = Window::fresh(now);
        }
        if window.count >= PER_SENDER_PER_WINDOW {
            return false;
        }
        window.count += 1;
        self.global.count += 1;
        true
    }

    #[cfg(test)]
    pub fn sender_count(&self) -> usize {
        self.senders.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedupe_rejects_a_second_insert_and_expires() {
        let mut d = Dedupe::default();
        let t0 = Instant::now();
        assert!(d.insert("did-a", "id1", t0));
        assert!(d.contains("did-a", "id1"));
        assert!(!d.insert("did-a", "id1", t0));
        // Same id from another sender is a different message.
        assert!(d.insert("did-b", "id1", t0));
        let later = t0 + DEDUPE_TTL + Duration::from_secs(1);
        assert!(d.insert("did-c", "x", later));
        assert!(!d.contains("did-a", "id1"));
    }

    #[test]
    fn dedupe_is_bounded() {
        let mut d = Dedupe::default();
        let t0 = Instant::now();
        for i in 0..(DEDUPE_MAX + 100) {
            d.insert("did", &format!("{i}"), t0);
        }
        assert!(d.len() <= DEDUPE_MAX);
        assert!(d.len() >= DEDUPE_MAX - 1);
        // The newest entries survive.
        assert!(d.contains("did", &format!("{}", DEDUPE_MAX + 99)));
    }

    #[test]
    fn per_sender_limit_applies_and_resets() {
        let mut l = RateLimiter::new(Instant::now());
        let t0 = Instant::now();
        for _ in 0..PER_SENDER_PER_WINDOW {
            assert!(l.allow("node-a", t0));
        }
        assert!(!l.allow("node-a", t0));
        // Another sender is unaffected.
        assert!(l.allow("node-b", t0));
        // The window rolls over.
        assert!(l.allow("node-a", t0 + WINDOW + Duration::from_secs(1)));
    }

    #[test]
    fn global_limit_applies_across_senders() {
        let t0 = Instant::now();
        let mut l = RateLimiter::new(t0);
        let mut accepted = 0;
        for i in 0..(GLOBAL_PER_WINDOW as usize + 50) {
            if l.allow(&format!("node-{i}"), t0) {
                accepted += 1;
            }
        }
        assert_eq!(accepted, GLOBAL_PER_WINDOW);
    }

    #[test]
    fn sender_table_is_bounded() {
        let t0 = Instant::now();
        let mut l = RateLimiter::new(t0);
        // Spread across windows so the global cap doesn't mask the table cap.
        for i in 0..(MAX_SENDERS + 200) {
            let at = t0 + Duration::from_millis(i as u64);
            l.global = Window::fresh(at);
            l.allow(&format!("node-{i}"), at);
        }
        assert!(l.sender_count() <= MAX_SENDERS);
        // Once the old windows expire, new senders are admitted again.
        let later = t0 + WINDOW * 2;
        assert!(l.allow("fresh", later));
    }
}
