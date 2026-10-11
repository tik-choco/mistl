//! JSON admission counts tasks before spawn, including PUTs waiting on slot locks.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(super) const MAX_PUT_REQUESTS: usize = 20;
pub(super) const MAX_REPLY_REQUESTS: usize = 8;
pub(super) const MAX_BUSY_REQUESTS: usize = 4;
pub(super) const MAX_PEER_REQUESTS: usize = 2;

#[derive(Clone, Copy)]
pub(super) enum Class {
    Put = 0,
    Reply = 1,
    Busy = 2,
}

pub(super) struct Admission {
    pools: [Arc<Semaphore>; 3],
    peers: Mutex<HashMap<String, [usize; 3]>>,
}

impl Default for Admission {
    fn default() -> Self {
        Self {
            pools: [
                Arc::new(Semaphore::new(MAX_PUT_REQUESTS)),
                Arc::new(Semaphore::new(MAX_REPLY_REQUESTS)),
                Arc::new(Semaphore::new(MAX_BUSY_REQUESTS)),
            ],
            peers: Mutex::new(HashMap::new()),
        }
    }
}

impl Admission {
    pub fn acquire(self: &Arc<Self>, from: &str, class: Class) -> Option<Guard> {
        let i = class as usize;
        let permit = self.pools[i].clone().try_acquire_owned().ok()?;
        let mut peers = self.peers.lock().unwrap();
        let counts = peers.entry(from.into()).or_default();
        if counts[i] >= MAX_PEER_REQUESTS {
            return None;
        }
        counts[i] += 1;
        Some(Guard {
            admission: self.clone(),
            from: from.into(),
            class,
            _permit: permit,
        })
    }
}

pub(super) struct Guard {
    admission: Arc<Admission>,
    from: String,
    class: Class,
    _permit: OwnedSemaphorePermit,
}

impl Drop for Guard {
    fn drop(&mut self) {
        let mut peers = self.admission.peers.lock().unwrap();
        let counts = peers.get_mut(&self.from).unwrap();
        counts[self.class as usize] -= 1;
        if *counts == [0, 0, 0] {
            peers.remove(&self.from);
        }
    }
}
