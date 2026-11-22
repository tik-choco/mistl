//! Authenticated control packets and bounded, fragmented checkpoint responses.
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;
use tokio::sync::oneshot;

use super::ha::{self, Group};

pub(super) const CONTROL_LIMIT: usize = 16 * 1024;
const MAGIC: &[u8; 8] = b"MVM1blob";
const HEADER: usize = 8 + 16 + 8 + 4 + 4;
const TAG: usize = 32;
// Leave room for the frame and overlay envelope below the storage protocol's 16 KiB.
const FRAGMENT: usize = 15 * 1024;
const ASSEMBLY_LIMIT: usize = 64 * 1024 * 1024;
const PER_PEER: usize = 8;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum Message {
    Lease {
        committed_seq: u64,
        #[serde(default)]
        lease_ok: bool,
        #[serde(default)]
        running: bool,
    },
    LeaseAck,
    LeaseNack {
        promised_epoch: u64,
        holder: String,
    },
    VoteReq {
        have_seq: u64,
        handover: bool,
    },
    Vote {
        granted: bool,
    },
    Release {
        to: String,
        seq: u64,
    },
    CkptOffer {
        seq: u64,
    },
    CkptHave {
        seq: u64,
    },
    Get {
        seq: u64,
        chunk: Option<String>,
    },
    Retire,
    Retired,
    RetireDone,
}

#[derive(Serialize, Deserialize)]
pub(super) struct Packet {
    mvm: u8,
    pub vm: String,
    pub from: String,
    ts: i64,
    pub nonce: u64,
    pub epoch: u64,
    #[serde(flatten)]
    pub msg: Message,
}

type Body = BTreeMap<String, Value>;

fn mac(key: &[u8; 32], bytes: &[u8]) -> Hmac<Sha256> {
    let mut hmac = Hmac::<Sha256>::new_from_slice(key).expect("fixed-size HMAC key");
    hmac.update(bytes);
    hmac
}

impl Packet {
    fn encode(&self, key: &[u8; 32]) -> Result<Vec<u8>> {
        let mut body: Body = serde_json::from_value(serde_json::to_value(self)?)?;
        let tag = mac(key, &serde_json::to_vec(&body)?)
            .finalize()
            .into_bytes();
        body.insert("mac".into(), Value::String(hex::encode(tag)));
        let bytes = serde_json::to_vec(&body)?;
        ensure!(bytes.len() <= CONTROL_LIMIT, "VM control message too large");
        Ok(bytes)
    }

    fn verify(bytes: &[u8], sender: &str, member: bool, key: &[u8; 32], now: i64) -> Result<Self> {
        ensure!(
            member && bytes.len() <= CONTROL_LIMIT,
            "invalid VM sender or size"
        );
        let mut body: Body = serde_json::from_slice(bytes)?;
        let tag = body.remove("mac").context("missing VM MAC")?;
        let tag = hex::decode(tag.as_str().context("invalid VM MAC")?)?;
        mac(key, &serde_json::to_vec(&body)?).verify_slice(&tag)?;
        let p: Self = serde_json::from_value(serde_json::to_value(body)?)?;
        ensure!(
            p.mvm == 1 && p.from == sender && p.ts.abs_diff(now) <= 120_000,
            "invalid VM envelope"
        );
        Ok(p)
    }
}

pub(super) fn encode(group: &Group, epoch: u64, nonce: u64, msg: Message) -> Result<Vec<u8>> {
    let p = Packet {
        mvm: 1,
        vm: group.id.clone(),
        from: group.me.clone(),
        epoch,
        nonce,
        msg,
        ts: chrono::Utc::now().timestamp_millis(),
    };
    p.encode(&group.key)
}

pub(super) fn send(group: &Group, to: &str, epoch: u64, nonce: u64, msg: Message) -> Result<()> {
    ensure!(group.online(), "VM group is offline");
    let bytes = encode(group, epoch, nonce, msg)?;
    group
        .outbox
        .try_send((to.to_owned(), bytes))
        .map_err(|_| anyhow::anyhow!("VM send queue is full"))?;
    Ok(())
}

struct Pending {
    group: Arc<Group>,
    peer: String,
    limit: usize,
    blob: Option<Blob>,
    done: oneshot::Sender<Blob>,
}

static REQUESTS: LazyLock<Mutex<HashMap<u64, Pending>>> = LazyLock::new(Default::default);
static SERVING: LazyLock<Mutex<HashMap<String, usize>>> = LazyLock::new(Default::default);
static INBOUND: AtomicUsize = AtomicUsize::new(0);

struct Charge {
    budget: &'static AtomicUsize,
    total: usize,
}

impl Charge {
    fn reserve(budget: &'static AtomicUsize, total: usize) -> Option<Self> {
        budget
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(total).filter(|&sum| sum <= ASSEMBLY_LIMIT)
            })
            .ok()?;
        Some(Self { budget, total })
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.budget.fetch_sub(self.total, Ordering::Relaxed);
    }
}

pub(super) struct Blob {
    bytes: Vec<u8>,
    charge: Charge,
}

impl std::ops::Deref for Blob {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.bytes
    }
}

struct Request(u64);
impl Drop for Request {
    fn drop(&mut self) {
        REQUESTS.lock().unwrap().remove(&self.0);
    }
}

pub(super) async fn fetch(
    group: &Arc<Group>,
    peer: &str,
    epoch: u64,
    seq: u64,
    chunk: Option<String>,
) -> Result<Blob> {
    let limit = if chunk.is_some() {
        super::checkpoint::CHUNK_SIZE + 16
    } else {
        ASSEMBLY_LIMIT
    };
    let nonce = rand::random();
    let (done, received) = oneshot::channel();
    let request = {
        let mut pending = REQUESTS.lock().unwrap();
        ensure!(
            !pending.contains_key(&nonce)
                && pending.len() < 64
                && pending.values().filter(|p| p.peer == peer).count() < PER_PEER,
            "VM peer request limit"
        );
        pending.insert(
            nonce,
            Pending {
                group: group.clone(),
                peer: peer.into(),
                limit,
                blob: None,
                done,
            },
        );
        Request(nonce)
    };
    send(group, peer, epoch, nonce, Message::Get { seq, chunk })?;
    let bytes = tokio::time::timeout(REQUEST_TIMEOUT, received)
        .await
        .context("VM blob request timed out")??;
    drop(request);
    Ok(bytes)
}

fn receive_blob(room: &str, from: &str, data: &[u8]) {
    if !(HEADER + TAG + 1..=HEADER + FRAGMENT + TAG).contains(&data.len()) {
        return;
    }
    let nonce = u64::from_be_bytes(data[24..32].try_into().unwrap());
    let total = u32::from_be_bytes(data[32..36].try_into().unwrap()) as usize;
    let offset = u32::from_be_bytes(data[36..40].try_into().unwrap()) as usize;
    let (signed, tag) = data.split_at(data.len() - TAG);
    let part = &signed[HEADER..];
    let mut requests = REQUESTS.lock().unwrap();
    let Some(p) = requests.get_mut(&nonce) else {
        return;
    };
    let expected = p.blob.as_ref().map(|blob| blob.charge.total);
    let received = p.blob.as_ref().map_or(0, |blob| blob.len());
    if room != p.group.room
        || from != p.peer
        || data[8..24] != *p.group.id.as_bytes()
        || !p.group.online()
        || !fits_fragment(p.limit, expected, received, total, offset, part.len())
        || mac(&p.group.key, signed).verify_slice(tag).is_err()
    {
        return;
    }
    if p.blob.is_none() {
        let Some(charge) = Charge::reserve(&INBOUND, total) else {
            return;
        };
        p.blob = Some(Blob {
            bytes: Vec::with_capacity(total),
            charge,
        });
    }
    let blob = p.blob.as_mut().unwrap();
    blob.bytes.extend_from_slice(part);
    if blob.len() == total {
        let mut p = requests.remove(&nonce).unwrap();
        let _ = p.done.send(p.blob.take().unwrap());
    }
}

fn fits_fragment(
    limit: usize,
    expected: Option<usize>,
    received: usize,
    total: usize,
    offset: usize,
    len: usize,
) -> bool {
    total <= limit
        && expected.is_none_or(|n| n == total)
        && offset == received
        && (1..=FRAGMENT).contains(&len)
        && offset.checked_add(len).is_some_and(|end| end <= total)
}

pub(super) fn listen() {
    crate::net::register_room_handler(|kind, room, from, data| {
        if kind != crate::net::EVENT_RAW || !super::store::valid_server_id(from) {
            return;
        }
        if data.starts_with(MAGIC) {
            receive_blob(room, from, data);
            return;
        }
        if data.len() > CONTROL_LIMIT
            || !data.starts_with(b"{")
            || !data.windows(5).any(|w| w == b"\"mvm\"")
        {
            return;
        }
        #[derive(Deserialize)]
        struct Address<'a> {
            vm: &'a str,
        }
        let Ok(address) = serde_json::from_slice::<Address>(data) else {
            return;
        };
        let Some(g) = ha::lookup(address.vm) else {
            return;
        };
        if g.room != room || g.me == from || !g.online() {
            return;
        }
        if let Ok(packet) = Packet::verify(
            data,
            from,
            g.member(from),
            &g.key,
            chrono::Utc::now().timestamp_millis(),
        ) {
            let _ = g.inbox.try_send(packet);
        }
    });
}

struct Service(String);
impl Drop for Service {
    fn drop(&mut self) {
        let mut active = SERVING.lock().unwrap();
        let n = active.get_mut(&self.0).unwrap();
        *n -= 1;
        if *n == 0 {
            active.remove(&self.0);
        }
    }
}

pub(super) fn serve(
    g: Arc<Group>,
    peer: String,
    epoch: u64,
    seq: u64,
    chunk: Option<String>,
    nonce: u64,
) {
    let permit = {
        let mut active = SERVING.lock().unwrap();
        if active.values().sum::<usize>() >= 16
            || active.get(&peer).copied().unwrap_or(0) >= PER_PEER
        {
            return;
        }
        *active.entry(peer.clone()).or_default() += 1;
        Service(peer.clone())
    };
    tokio::spawn(async move {
        let reader = g.clone();
        let result = tokio::time::timeout(REQUEST_TIMEOUT, async {
            let (data, _permit) = tokio::task::spawn_blocking(move || {
                let permit = permit;
                ensure!(reader.may_serve(epoch), "VM holder changed");
                let store = super::CheckpointStore::open(&reader.root, &reader.id)?;
                let data = match chunk {
                    Some(id) => store.read_referenced_chunk(seq, &id),
                    None => store.read_encrypted_manifest(seq),
                }?;
                Ok::<_, anyhow::Error>((data, permit))
            })
            .await??;
            let total = (data.len() as u32).to_be_bytes();
            for (i, part) in data.chunks(FRAGMENT).enumerate() {
                ensure!(g.may_serve(epoch), "VM holder changed");
                let mut frame = Vec::with_capacity(HEADER + part.len() + TAG);
                frame.extend_from_slice(MAGIC);
                frame.extend_from_slice(g.id.as_bytes());
                frame.extend_from_slice(&nonce.to_be_bytes());
                frame.extend_from_slice(&total);
                frame.extend_from_slice(&((i * FRAGMENT) as u32).to_be_bytes());
                frame.extend_from_slice(part);
                let tag = mac(&g.key, &frame).finalize().into_bytes();
                frame.extend_from_slice(&tag);
                crate::net::send_direct_wait(&g.room, &peer, frame).await?;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await;
        if !matches!(result, Ok(Ok(()))) {
            tracing::debug!(vm = %g.id, "VM blob response interrupted");
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_authentication_binds_sender_membership_time_and_size() {
        let key = [42; 32];
        let packet = Packet {
            mvm: 1,
            vm: "a".repeat(16),
            from: "b".repeat(16),
            ts: 200_000,
            epoch: 2,
            nonce: 71,
            msg: Message::CkptHave { seq: 5 },
        };
        let bytes = packet.encode(&key).unwrap();
        let verify = |bytes: &[u8], from, member, now| {
            Packet::verify(bytes, from, member, &key, now).is_ok()
        };
        assert!(verify(&bytes, &packet.from, true, 320_000));
        assert!(!verify(&bytes, &packet.from, true, 320_001));
        assert!(!verify(&bytes, &packet.from, true, 79_999));
        assert!(!verify(&bytes, "another", true, packet.ts));
        assert!(!verify(&bytes, &packet.from, false, packet.ts));
        assert!(Packet::verify(&bytes, &packet.from, true, &[0; 32], packet.ts).is_err());
        let mut changed: Value = serde_json::from_slice(&bytes).unwrap();
        changed["seq"] = 9.into();
        assert!(!verify(
            &serde_json::to_vec(&changed).unwrap(),
            &packet.from,
            true,
            packet.ts
        ));
        assert!(!verify(
            &vec![0; CONTROL_LIMIT + 1],
            &packet.from,
            true,
            packet.ts
        ));
        const { assert!(HEADER + TAG + FRAGMENT <= 16 * 1024) };
    }

    #[test]
    fn blob_bounds_apply_before_allocating_or_appending() {
        let limit = super::super::checkpoint::CHUNK_SIZE + 16;
        assert!(fits_fragment(limit, None, 0, limit, 0, FRAGMENT));
        assert!(fits_fragment(
            limit,
            Some(limit),
            limit - 16,
            limit,
            limit - 16,
            16
        ));
        assert!(!fits_fragment(limit, None, 0, limit + 1, 0, 1));
        assert!(!fits_fragment(limit, Some(12), 4, 13, 4, 1));
        assert!(!fits_fragment(limit, None, 0, limit, 1, 1));
        assert!(!fits_fragment(limit, Some(10), 4, 10, 4, 7));
        assert!(!fits_fragment(limit, None, 0, limit, 0, FRAGMENT + 1));
        assert!(!fits_fragment(limit, None, 0, limit, 0, 0));
    }

    #[test]
    fn assembled_blob_holds_its_budget_until_the_consumer_drops_it() {
        static BUDGET: AtomicUsize = AtomicUsize::new(0);
        let (sender, mut receiver) = oneshot::channel();
        let charge = Charge::reserve(&BUDGET, ASSEMBLY_LIMIT).unwrap();
        assert!(
            sender
                .send(Blob {
                    bytes: vec![1],
                    charge
                })
                .is_ok()
        );
        assert!(Charge::reserve(&BUDGET, 1).is_none());
        let blob = receiver.try_recv().unwrap();
        assert_eq!(&*blob, &[1]);
        assert!(Charge::reserve(&BUDGET, 1).is_none());
        drop(blob);
        assert!(Charge::reserve(&BUDGET, ASSEMBLY_LIMIT).is_some());
    }
}
