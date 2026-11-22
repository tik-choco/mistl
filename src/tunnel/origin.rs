use std::collections::{HashMap, hash_map::Entry};
use std::net::SocketAddr;
use std::sync::{LazyLock, Mutex};

static LOOPBACK_DIALS: LazyLock<Mutex<HashMap<SocketAddr, usize>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[must_use]
pub(crate) struct Guard(Option<SocketAddr>);

fn loopback_addr(addr: SocketAddr) -> Option<SocketAddr> {
    let ip = addr.ip().to_canonical();
    ip.is_loopback().then(|| SocketAddr::new(ip, addr.port()))
}

/// Track a tunnel backend socket for the lifetime of its guard.
pub(crate) fn register(local: SocketAddr) -> Guard {
    let local = loopback_addr(local);
    if let Some(addr) = local {
        *LOOPBACK_DIALS.lock().unwrap().entry(addr).or_default() += 1;
    }
    Guard(local)
}

pub(crate) fn is_tunneled(peer: SocketAddr) -> bool {
    loopback_addr(peer).is_some_and(|addr| LOOPBACK_DIALS.lock().unwrap().contains_key(&addr))
}

/// Keep an existing registration alive while the write half is in use.
pub(super) fn retain(local: SocketAddr) -> Option<Guard> {
    let addr = loopback_addr(local)?;
    *LOOPBACK_DIALS.lock().unwrap().get_mut(&addr)? += 1;
    Some(Guard(Some(addr)))
}

impl Drop for Guard {
    fn drop(&mut self) {
        if let Some(addr) = self.0
            && let Entry::Occupied(mut entry) = LOOPBACK_DIALS.lock().unwrap().entry(addr)
        {
            let count = entry.get_mut();
            *count -= 1;
            if *count == 0 {
                entry.remove();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracks_each_loopback_registration_until_its_guard_drops() {
        let local = "127.0.0.2:50001".parse().unwrap();
        let mapped = "[::ffff:127.0.0.2]:50001".parse().unwrap();
        let other_port = "127.0.0.2:50002".parse().unwrap();
        let other_ip = "127.0.0.3:50001".parse().unwrap();
        let external = "192.0.2.1:50001".parse().unwrap();
        let first = register(local);
        let second = register(mapped);
        let writer = retain(mapped).unwrap();
        let _external = register(external);

        assert!(is_tunneled(local));
        assert!(is_tunneled(mapped));
        assert!(!is_tunneled(other_port));
        assert!(!is_tunneled(other_ip));
        assert!(!is_tunneled(external));
        drop(first);
        assert!(is_tunneled(local));
        drop(second);
        assert!(is_tunneled(local));
        drop(writer);
        assert!(!is_tunneled(local));
        assert!(retain(local).is_none());
    }
}
