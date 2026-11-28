//! mistl web p2p protocol in room `mistl-web-v1`, tagged `"mweb":1` (not
//! `{v,type}`, which the mistai protocol uses):
//!
//! - `{"mweb":1,"op":"want","site":"<id>"}` (broadcast): "who has a record
//!   for this site?" A peer holding a verified record replies directly.
//! - `{"mweb":1,"op":"record","record":{...}}` (direct reply, or broadcast
//!   announce on publish). The record authenticates itself; the receiver
//!   still verifies it and keeps it only if it asked for the site or already
//!   holds it.
//!
//! Everything here is bounded: message size, per-peer rate, tracked peers,
//! and outstanding wants.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tokio::sync::watch;

use super::site;

pub const ROOM: &str = "mistl-web-v1";
/// Largest accepted message.
pub const MAX_MESSAGE_BYTES: usize = 8 * 1024;
/// Per-peer budget: this many messages per [`RATE_WINDOW`] (excess dropped).
pub const RATE_LIMIT: u32 = 20;
pub const RATE_WINDOW: Duration = Duration::from_secs(10);
/// Peers tracked by a [`RateLimiter`] (least recently seen evicted).
pub const MAX_PEERS: usize = 256;
/// Outstanding wants and their lifetime.
pub const MAX_PENDING: usize = 64;
pub const PENDING_TTL: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, PartialEq)]
pub enum Msg {
    Want { site: String },
    Record { record: Value },
}

/// Cheap pre-filter run on every inbound message of every room before any
/// JSON parsing.
pub fn looks_like_web(data: &[u8]) -> bool {
    data.len() <= MAX_MESSAGE_BYTES
        && data.starts_with(b"{")
        && data.windows(6).any(|w| w == b"\"mweb\"")
}

/// Parse an inbound message; `None` for anything that is not a well-formed
/// v1 message (the caller ignores it).
pub fn parse(data: &[u8]) -> Option<Msg> {
    if !looks_like_web(data) {
        return None;
    }
    let v: Value = serde_json::from_slice(data).ok()?;
    if v.get("mweb")?.as_u64()? != 1 {
        return None;
    }
    match v.get("op")?.as_str()? {
        "want" => {
            let site = v.get("site")?.as_str()?;
            site::is_site_id(site).then(|| Msg::Want {
                site: site.to_string(),
            })
        }
        "record" => {
            let record = v.get("record")?;
            record.is_object().then(|| Msg::Record {
                record: record.clone(),
            })
        }
        _ => None,
    }
}

pub fn want(site: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({"mweb": 1, "op": "want", "site": site})).expect("serializes")
}

pub fn record(raw: &Value) -> Vec<u8> {
    serde_json::to_vec(&json!({"mweb": 1, "op": "record", "record": raw})).expect("serializes")
}

struct PeerWindow {
    start: Instant,
    count: u32,
    last_seen: Instant,
}

/// Fixed-window per-peer rate limiter over at most [`MAX_PEERS`] peers.
#[derive(Default)]
pub struct RateLimiter {
    peers: HashMap<String, PeerWindow>,
}

impl RateLimiter {
    /// Whether `peer` may send one more message now.
    pub fn allow(&mut self, peer: &str, now: Instant) -> bool {
        if !self.peers.contains_key(peer)
            && self.peers.len() >= MAX_PEERS
            && let Some(oldest) = self
                .peers
                .iter()
                .min_by_key(|(_, w)| w.last_seen)
                .map(|(p, _)| p.clone())
        {
            self.peers.remove(&oldest);
        }
        let w = self.peers.entry(peer.to_string()).or_insert(PeerWindow {
            start: now,
            count: 0,
            last_seen: now,
        });
        w.last_seen = now;
        if now.saturating_duration_since(w.start) >= RATE_WINDOW {
            w.start = now;
            w.count = 0;
        }
        if w.count >= RATE_LIMIT {
            return false;
        }
        w.count += 1;
        true
    }

    pub fn tracked(&self) -> usize {
        self.peers.len()
    }
}

struct PendingWant {
    expires: Instant,
    done: watch::Sender<bool>,
}

/// Sites we sent a `want` for, so replies for them are accepted.
#[derive(Default)]
pub struct Pending {
    wants: HashMap<String, PendingWant>,
}

impl Pending {
    fn purge(&mut self, now: Instant) {
        self.wants.retain(|_, w| w.expires > now);
    }

    /// Start (or refresh) a want for `site`; the receiver flips to `true`
    /// when a record for it is accepted. `None` when [`MAX_PENDING`] other
    /// sites are already outstanding.
    pub fn register(&mut self, site: &str, now: Instant) -> Option<watch::Receiver<bool>> {
        self.purge(now);
        if let Some(w) = self.wants.get_mut(site) {
            w.expires = now + PENDING_TTL;
            return Some(w.done.subscribe());
        }
        if self.wants.len() >= MAX_PENDING {
            return None;
        }
        let (done, rx) = watch::channel(false);
        self.wants.insert(
            site.to_string(),
            PendingWant {
                expires: now + PENDING_TTL,
                done,
            },
        );
        Some(rx)
    }

    pub fn is_pending(&self, site: &str, now: Instant) -> bool {
        self.wants.get(site).is_some_and(|w| w.expires > now)
    }

    /// A record for `site` was accepted: wake waiters and forget the want.
    pub fn complete(&mut self, site: &str) {
        if let Some(w) = self.wants.remove(site) {
            let _ = w.done.send(true);
        }
    }

    pub fn outstanding(&self) -> usize {
        self.wants.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u8) -> String {
        let c = (b'a' + n % 26) as char;
        std::iter::repeat_n(c, site::SITE_ID_LEN).collect()
    }

    #[test]
    fn parse_round_trips_and_rejects_noise() {
        let site = id(0);
        assert_eq!(parse(&want(&site)), Some(Msg::Want { site: site.clone() }));
        let raw = json!({"v":1,"type":"mistl-web-record"});
        assert_eq!(parse(&record(&raw)), Some(Msg::Record { record: raw }));

        for bad in [
            br#"{"v":1,"type":"hello"}"#.to_vec(),
            br#"{"mweb":2,"op":"want","site":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}"#.to_vec(),
            br#"{"mweb":1,"op":"want","site":"short"}"#.to_vec(),
            br#"{"mweb":1,"op":"record","record":"x"}"#.to_vec(),
            br#"{"mweb":1,"op":"other"}"#.to_vec(),
            br#"["mweb"]"#.to_vec(),
            b"{\"mweb\":1,".to_vec(),
        ] {
            assert_eq!(parse(&bad), None, "{}", String::from_utf8_lossy(&bad));
        }
        let mut big = want(&site);
        big.pop();
        big.extend(std::iter::repeat_n(b' ', MAX_MESSAGE_BYTES));
        big.push(b'}');
        assert!(!looks_like_web(&big));
        assert_eq!(parse(&big), None);
    }

    #[test]
    fn rate_limiter_windows_and_peer_cap() {
        let mut rl = RateLimiter::default();
        let t0 = Instant::now();
        for _ in 0..RATE_LIMIT {
            assert!(rl.allow("p", t0));
        }
        assert!(!rl.allow("p", t0 + Duration::from_secs(9)));
        assert!(rl.allow("q", t0), "per peer");
        assert!(rl.allow("p", t0 + RATE_WINDOW));

        let mut rl = RateLimiter::default();
        for i in 0..(MAX_PEERS + 10) {
            rl.allow(&format!("peer{i}"), t0 + Duration::from_millis(i as u64));
        }
        assert_eq!(rl.tracked(), MAX_PEERS);
    }

    #[tokio::test]
    async fn pending_wants_bounded_expire_and_wake() {
        let mut p = Pending::default();
        let t0 = Instant::now();
        let mut rx = p.register(&id(0), t0).unwrap();
        assert!(p.is_pending(&id(0), t0));
        assert!(!p.is_pending(&id(0), t0 + PENDING_TTL));
        p.complete(&id(0));
        assert!(*rx.wait_for(|done| *done).await.unwrap());
        assert!(!p.is_pending(&id(0), t0));

        for i in 0..MAX_PENDING as u8 {
            assert!(p.register(&format!("{}{i:02}", &id(1)[..30]), t0).is_some());
        }
        assert!(p.register(&id(2), t0).is_none());
        // Re-registering an outstanding site is always fine.
        assert!(p.register(&format!("{}00", &id(1)[..30]), t0).is_some());
        // After expiry there is room again.
        assert!(p.register(&id(2), t0 + PENDING_TTL).is_some());
        assert_eq!(p.outstanding(), 1);
    }
}
