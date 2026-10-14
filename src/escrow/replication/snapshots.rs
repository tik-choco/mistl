//! Bounded, resumable metadata transfers. Ciphertext generations remain in the archive.
use anyhow::{Result, ensure};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use mistlib_consensus_core::storage::{RaftMembership, RaftSnapshot};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

use super::archive::Lease;
use crate::escrow::protocol::{digest, hex_string};

pub(super) const MAX_SNAPSHOT_BYTES: usize = 64 * 1024 * 1024;
pub(super) const CHUNK_BYTES: usize = 8 * 1024;
const MAX_ENCODED_CHUNK: usize = (CHUNK_BYTES * 4).div_ceil(3);
const MAX_PEERS: usize = 18;
const TTL: Duration = Duration::from_secs(180);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Offer {
    pub ticket: String,
    pub term: u64,
    pub index: u64,
    pub snapshot_term: u64,
    pub sha: String,
    pub size: u64,
    pub membership: Option<RaftMembership>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Request {
    pub ticket: String,
    pub offset: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Chunk {
    pub ticket: String,
    pub offset: u64,
    pub data: String,
}

#[derive(Debug)]
pub(super) enum Receive {
    Request(Request),
    Complete { term: u64, snapshot: RaftSnapshot },
    Ignored,
}

struct Outgoing {
    offer: Offer,
    expires: Instant,
    _lease: Lease,
}
struct Shared {
    snapshot: Arc<RaftSnapshot>,
    sha: String,
}
struct Incoming {
    peer: String,
    offer: Offer,
    bytes: Vec<u8>,
    expires: Instant,
}

#[derive(Default)]
pub(super) struct SnapshotTransfers {
    outgoing: HashMap<String, Outgoing>,
    shared: Option<Shared>,
    incoming: Option<Incoming>,
}

impl SnapshotTransfers {
    pub(super) fn offer(
        &mut self,
        peer: &str,
        term: u64,
        snapshot: RaftSnapshot,
        lease: Lease,
        now: Instant,
    ) -> Result<Option<Offer>> {
        self.expire(now);
        let same_payload = self.shared.as_ref().filter(|shared| {
            Arc::ptr_eq(&shared.snapshot.data, &snapshot.data)
                && shared.snapshot.last_included_index == snapshot.last_included_index
                && shared.snapshot.last_included_term == snapshot.last_included_term
                && shared.snapshot.membership == snapshot.membership
        });
        let different_header = self.shared.as_ref().is_some_and(|shared| {
            shared.snapshot.last_included_index != snapshot.last_included_index
                || shared.snapshot.last_included_term != snapshot.last_included_term
                || shared.snapshot.membership != snapshot.membership
        });
        let sha = if different_header && !self.outgoing.is_empty() {
            self.shared.as_ref().unwrap().sha.clone()
        } else {
            same_payload.map_or_else(|| digest(&snapshot.data), |shared| shared.sha.clone())
        };
        let offer = Offer {
            ticket: crate::net::peer_auth::fresh_nonce(),
            term,
            index: snapshot.last_included_index,
            snapshot_term: snapshot.last_included_term,
            sha,
            size: snapshot.data.len() as u64,
            membership: snapshot.membership.clone(),
        };
        validate_offer(peer, &offer)?;
        let same = self.shared.as_ref().is_some_and(|shared| {
            shared.snapshot.last_included_index == offer.index
                && shared.snapshot.last_included_term == offer.snapshot_term
                && shared.snapshot.membership == offer.membership
                && shared.sha == offer.sha
        });
        if !same {
            // An active recipient may still fetch this snapshot's ciphertext generations.
            if !self.outgoing.is_empty() {
                return Ok(self
                    .outgoing
                    .get(peer)
                    .filter(|item| item.offer.term == term)
                    .map(|item| item.offer.clone()));
            }
            self.shared = Some(Shared {
                snapshot: Arc::new(snapshot),
                sha: offer.sha.clone(),
            });
        }
        if let Some(existing) = self.outgoing.get(peer) {
            if existing.offer.term == term {
                return Ok(Some(existing.offer.clone()));
            }
        }
        if !self.outgoing.contains_key(peer) && self.outgoing.len() >= MAX_PEERS {
            return Ok(None);
        }
        self.outgoing.insert(
            peer.into(),
            Outgoing {
                offer: offer.clone(),
                expires: now + TTL,
                _lease: lease,
            },
        );
        Ok(Some(offer))
    }

    pub(super) fn accept_offer(
        &mut self,
        peer: &str,
        offer: Offer,
        now: Instant,
    ) -> Result<Receive> {
        self.expire(now);
        validate_offer(peer, &offer)?;
        if let Some(incoming) = &self.incoming {
            if incoming.peer == peer && incoming.offer == offer {
                return Ok(Receive::Request(Request {
                    ticket: offer.ticket,
                    offset: incoming.bytes.len() as u64,
                }));
            }
            if offer.term < incoming.offer.term
                || (offer.term == incoming.offer.term
                    && (incoming.peer != peer || offer.index <= incoming.offer.index))
            {
                return Ok(Receive::Ignored);
            }
        }
        if offer.size == 0 {
            ensure!(offer.sha == digest(&[]), "snapshot hash mismatch");
            self.incoming = None;
            return Ok(complete(offer, Vec::new()));
        }
        let request = Request {
            ticket: offer.ticket.clone(),
            offset: 0,
        };
        self.incoming = Some(Incoming {
            peer: peer.into(),
            offer,
            bytes: Vec::new(),
            expires: now + TTL,
        });
        Ok(Receive::Request(request))
    }

    pub(super) fn reply_chunk(
        &mut self,
        peer: &str,
        request: &Request,
        now: Instant,
    ) -> Option<Chunk> {
        self.expire(now);
        let outgoing = self.outgoing.get_mut(peer)?;
        if outgoing.offer.ticket != request.ticket {
            return None;
        }
        let snapshot = &self.shared.as_ref()?.snapshot;
        let offset = usize::try_from(request.offset).ok()?;
        if offset >= snapshot.data.len() || !offset.is_multiple_of(CHUNK_BYTES) {
            return None;
        }
        let end = offset.saturating_add(CHUNK_BYTES).min(snapshot.data.len());
        outgoing.expires = now + TTL;
        Some(Chunk {
            ticket: request.ticket.clone(),
            offset: request.offset,
            data: URL_SAFE_NO_PAD.encode(&snapshot.data[offset..end]),
        })
    }

    pub(super) fn accept_chunk(
        &mut self,
        peer: &str,
        chunk: Chunk,
        now: Instant,
    ) -> Result<Receive> {
        self.expire(now);
        let Some(incoming) = self.incoming.as_mut() else {
            return Ok(Receive::Ignored);
        };
        if incoming.peer != peer || incoming.offer.ticket != chunk.ticket {
            return Ok(Receive::Ignored);
        }
        ensure!(
            chunk.data.len() <= MAX_ENCODED_CHUNK,
            "oversized snapshot chunk"
        );
        let bytes = URL_SAFE_NO_PAD.decode(&chunk.data)?;
        ensure!(
            !bytes.is_empty() && bytes.len() <= CHUNK_BYTES,
            "invalid snapshot chunk size"
        );
        let offset = usize::try_from(chunk.offset)?;
        let expected = incoming.bytes.len();
        if offset != expected {
            if offset < expected {
                ensure!(
                    offset
                        .checked_add(bytes.len())
                        .is_some_and(|end| end <= expected)
                        && incoming.bytes.get(offset..offset + bytes.len())
                            == Some(bytes.as_slice()),
                    "conflicting duplicate snapshot chunk"
                );
            }
            return Ok(Receive::Request(Request {
                ticket: incoming.offer.ticket.clone(),
                offset: expected as u64,
            }));
        }
        let end = expected
            .checked_add(bytes.len())
            .ok_or_else(|| anyhow::anyhow!("snapshot offset overflow"))?;
        ensure!(
            end as u64 <= incoming.offer.size,
            "snapshot chunk exceeds declared size"
        );
        ensure!(
            end as u64 == incoming.offer.size || bytes.len() == CHUNK_BYTES,
            "short nonfinal snapshot chunk"
        );
        incoming.bytes.try_reserve(bytes.len())?;
        incoming.bytes.extend_from_slice(&bytes);
        incoming.expires = now + TTL;
        if end as u64 != incoming.offer.size {
            return Ok(Receive::Request(Request {
                ticket: incoming.offer.ticket.clone(),
                offset: end as u64,
            }));
        }
        let incoming = self.incoming.take().expect("complete incoming snapshot");
        ensure!(
            digest(&incoming.bytes) == incoming.offer.sha,
            "snapshot hash mismatch"
        );
        Ok(complete(incoming.offer, incoming.bytes))
    }

    pub(super) fn acknowledged(&mut self, peer: &str, index: u64) {
        if self
            .outgoing
            .get(peer)
            .is_some_and(|item| index >= item.offer.index)
        {
            self.outgoing.remove(peer);
        }
        if self.outgoing.is_empty() {
            self.shared = None;
        }
    }

    pub(super) fn expire(&mut self, now: Instant) {
        self.outgoing.retain(|_, item| now < item.expires);
        if self.outgoing.is_empty() {
            self.shared = None;
        }
        if self
            .incoming
            .as_ref()
            .is_some_and(|item| now >= item.expires)
        {
            self.incoming = None;
        }
    }
}

fn complete(offer: Offer, data: Vec<u8>) -> Receive {
    Receive::Complete {
        term: offer.term,
        snapshot: RaftSnapshot {
            last_included_index: offer.index,
            last_included_term: offer.snapshot_term,
            membership: offer.membership,
            data: Arc::new(data),
        },
    }
}

fn validate_offer(peer: &str, offer: &Offer) -> Result<()> {
    ensure!(super::super::valid_voter(peer), "invalid snapshot sender");
    ensure!(
        hex_string(&offer.ticket, 32) && hex_string(&offer.sha, 64),
        "invalid snapshot transfer identity"
    );
    ensure!(
        offer.size <= MAX_SNAPSHOT_BYTES as u64
            && offer.term < u64::MAX
            && offer.index < u64::MAX
            && offer.snapshot_term <= offer.term
            && ((offer.index == 0) == (offer.snapshot_term == 0)),
        "invalid snapshot bounds"
    );
    if let Some(membership) = &offer.membership {
        ensure!(
            (1..=9).contains(&membership.voters.len()) && membership.learners.len() <= 9,
            "invalid snapshot membership size"
        );
        let valid_set = |ids: &[mistlib_consensus_core::NodeId]| -> bool {
            ids.iter().all(|id| super::super::valid_voter(&id.0))
                && ids.iter().map(|id| &id.0).collect::<BTreeSet<_>>().len() == ids.len()
        };
        ensure!(
            valid_set(&membership.voters) && valid_set(&membership.learners),
            "invalid snapshot members"
        );
        if let Some(joint) = &membership.joint_voters {
            ensure!(
                (1..=9).contains(&joint.len()) && valid_set(joint),
                "invalid joint snapshot members"
            );
        }
        let voters: BTreeSet<_> = membership
            .voters
            .iter()
            .chain(membership.joint_voters.iter().flatten())
            .map(|id| &id.0)
            .collect();
        ensure!(
            membership.learners.iter().all(|id| !voters.contains(&id.0)),
            "voting snapshot learner"
        );
        ensure!(
            voters.len() + membership.learners.len() <= MAX_PEERS,
            "snapshot membership exceeds peer capacity"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EscrowConfig;
    use crate::escrow::protocol::Head;
    use ed25519_dalek::{Signer, SigningKey};
    use mistlib_consensus_core::NodeId;
    use mistlib_core::storage::cid::{MULTICODEC_DAG_CBOR, MULTICODEC_RAW, compute_cid};
    use mistlib_core::storage::{CHUNK_SIZE, FileManifest};
    use std::collections::HashSet;
    use std::path::PathBuf;

    const LEADER: &str = "0000000000000001";
    const FOLLOWER: &str = "0000000000000002";
    const OTHER: &str = "0000000000000003";
    struct Fixture {
        root: PathBuf,
        archive: super::super::archive::Archive,
    }
    impl Fixture {
        fn new() -> Self {
            let root =
                std::env::temp_dir().join(format!("mistl-snapshot-{:016x}", rand::random::<u64>()));
            std::fs::create_dir_all(&root).unwrap();
            let archive =
                super::super::archive::Archive::open(root.clone(), EscrowConfig::default())
                    .unwrap();
            Self { root, archive }
        }
        fn lease(&self) -> Lease {
            self.archive.pin_heads(&[]).unwrap()
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }
    fn snapshot(data: Vec<u8>, index: u64) -> RaftSnapshot {
        RaftSnapshot {
            last_included_index: index,
            last_included_term: if index == 0 { 0 } else { 1 },
            data: Arc::new(data),
            membership: Some(RaftMembership {
                voters: vec![NodeId(LEADER.into())],
                joint_voters: None,
                learners: vec![NodeId(FOLLOWER.into())],
            }),
        }
    }
    fn request(receive: Receive) -> Request {
        match receive {
            Receive::Request(request) => request,
            other => panic!("expected request: {other:?}"),
        }
    }
    fn generation(fixture: &Fixture) -> Head {
        let blob = vec![9; 64];
        let manifest = serde_cbor::to_vec(&FileManifest {
            name: "tc-escrow.bin".into(),
            size: blob.len() as u64,
            chunks: blob
                .chunks(CHUNK_SIZE)
                .map(|bytes| compute_cid(bytes, MULTICODEC_RAW))
                .collect(),
        })
        .unwrap();
        let key = SigningKey::from_bytes(&[7; 32]);
        let mut head = Head {
            v: 1,
            kind: "tc-escrow-head".into(),
            slot: "ab".repeat(16),
            seq: 1,
            parent: None,
            cid: compute_cid(&manifest, MULTICODEC_DAG_CBOR),
            size: blob.len() as u64,
            sha256: digest(&blob),
            comp: "none".into(),
            schema: "test/1".into(),
            writer: URL_SAFE_NO_PAD.encode(key.verifying_key().as_bytes()),
            at: 1,
            sig: String::new(),
        };
        head.sig = URL_SAFE_NO_PAD.encode(key.sign(&head.signing_payload()).to_bytes());
        fixture
            .archive
            .stage("room-a", &head, &blob, &manifest)
            .unwrap();
        head
    }

    #[test]
    fn transfers_multiple_chunks_and_deduplicates_offers() {
        let fixture = Fixture::new();
        let now = Instant::now();
        let original = snapshot(vec![7; CHUNK_BYTES * 3 + 41], 31);
        let mut send = SnapshotTransfers::default();
        let offer = send
            .offer(FOLLOWER, 3, original.clone(), fixture.lease(), now)
            .unwrap()
            .unwrap();
        assert_eq!(
            send.offer(FOLLOWER, 3, original.clone(), fixture.lease(), now)
                .unwrap()
                .unwrap(),
            offer
        );
        let mut recv = SnapshotTransfers::default();
        let mut next = request(recv.accept_offer(LEADER, offer.clone(), now).unwrap());
        loop {
            let chunk = send.reply_chunk(FOLLOWER, &next, now).unwrap();
            assert!(serde_json::to_vec(&chunk).unwrap().len() < 12 * 1024);
            match recv.accept_chunk(LEADER, chunk, now).unwrap() {
                Receive::Request(want) => {
                    assert_eq!(
                        request(recv.accept_offer(LEADER, offer.clone(), now).unwrap()),
                        want
                    );
                    next = want;
                }
                Receive::Complete { term, snapshot } => {
                    assert_eq!(term, 3);
                    assert_eq!(snapshot, original);
                    break;
                }
                Receive::Ignored => panic!("unexpected ignored chunk"),
            }
        }
        assert!(recv.incoming.is_none());
    }

    #[test]
    fn duplicate_out_of_order_and_wrong_sender_chunks_do_not_advance() {
        let fixture = Fixture::new();
        let now = Instant::now();
        let mut send = SnapshotTransfers::default();
        let offer = send
            .offer(
                FOLLOWER,
                2,
                snapshot(vec![5; CHUNK_BYTES * 2 + 8], 7),
                fixture.lease(),
                now,
            )
            .unwrap()
            .unwrap();
        let mut recv = SnapshotTransfers::default();
        let start = request(recv.accept_offer(LEADER, offer, now).unwrap());
        let later = send
            .reply_chunk(
                FOLLOWER,
                &Request {
                    ticket: start.ticket.clone(),
                    offset: CHUNK_BYTES as u64,
                },
                now,
            )
            .unwrap();
        assert_eq!(
            request(recv.accept_chunk(LEADER, later, now).unwrap()),
            start
        );
        let first = send.reply_chunk(FOLLOWER, &start, now).unwrap();
        assert!(matches!(
            recv.accept_chunk(OTHER, first.clone(), now).unwrap(),
            Receive::Ignored
        ));
        let next = request(recv.accept_chunk(LEADER, first.clone(), now).unwrap());
        assert_eq!(
            request(recv.accept_chunk(LEADER, first.clone(), now).unwrap()),
            next
        );
        let bad = Chunk {
            data: URL_SAFE_NO_PAD.encode(vec![3; CHUNK_BYTES]),
            ..first
        };
        assert!(recv.accept_chunk(LEADER, bad, now).is_err());
        assert_eq!(recv.incoming.as_ref().unwrap().bytes.len(), CHUNK_BYTES);
        assert!(send.reply_chunk(OTHER, &next, now).is_none());
        assert!(
            send.reply_chunk(
                FOLLOWER,
                &Request {
                    ticket: next.ticket,
                    offset: 1
                },
                now
            )
            .is_none()
        );
    }

    #[test]
    fn malformed_oversized_and_bad_hash_snapshots_fail_closed() {
        let fixture = Fixture::new();
        let now = Instant::now();
        let mut send = SnapshotTransfers::default();
        let offer = send
            .offer(FOLLOWER, 2, snapshot(vec![5; 3], 7), fixture.lease(), now)
            .unwrap()
            .unwrap();
        let mut recv = SnapshotTransfers::default();
        let mut oversized = offer.clone();
        oversized.size = MAX_SNAPSHOT_BYTES as u64 + 1;
        assert!(recv.accept_offer(LEADER, oversized, now).is_err());
        let mut bad_members = offer.clone();
        bad_members
            .membership
            .as_mut()
            .unwrap()
            .learners
            .push(NodeId(LEADER.into()));
        assert!(recv.accept_offer(LEADER, bad_members, now).is_err());
        let first = request(recv.accept_offer(LEADER, offer.clone(), now).unwrap());
        assert!(
            recv.accept_chunk(
                LEADER,
                Chunk {
                    ticket: first.ticket.clone(),
                    offset: 0,
                    data: "a".repeat(MAX_ENCODED_CHUNK + 1)
                },
                now
            )
            .is_err()
        );
        let mut bad = send.reply_chunk(FOLLOWER, &first, now).unwrap();
        bad.data = URL_SAFE_NO_PAD.encode([3, 4, 5]);
        assert!(recv.accept_chunk(LEADER, bad, now).is_err());
        assert!(recv.incoming.is_none());
        let next = request(recv.accept_offer(LEADER, offer, now).unwrap());
        assert!(matches!(
            recv.accept_chunk(LEADER, send.reply_chunk(FOLLOWER, &next, now).unwrap(), now)
                .unwrap(),
            Receive::Complete { .. }
        ));
    }

    #[test]
    fn newer_leader_replaces_one_buffer_and_expiry_clears_both_directions() {
        let fixture = Fixture::new();
        let now = Instant::now();
        let mut send = SnapshotTransfers::default();
        let a = send
            .offer(
                FOLLOWER,
                2,
                snapshot(vec![5; CHUNK_BYTES + 1], 7),
                fixture.lease(),
                now,
            )
            .unwrap()
            .unwrap();
        let mut recv = SnapshotTransfers::default();
        recv.accept_offer(LEADER, a.clone(), now).unwrap();
        assert!(matches!(
            recv.accept_offer(OTHER, a.clone(), now).unwrap(),
            Receive::Ignored
        ));
        let mut b = a.clone();
        b.term += 1;
        b.ticket = crate::net::peer_auth::fresh_nonce();
        let next = request(recv.accept_offer(OTHER, b, now).unwrap());
        assert_eq!(next.offset, 0);
        assert_eq!(recv.incoming.as_ref().unwrap().peer, OTHER);
        recv.expire(now + TTL);
        send.expire(now + TTL);
        assert!(recv.incoming.is_none());
        assert!(send.outgoing.is_empty());
        assert!(send.shared.is_none());
    }

    #[test]
    fn outgoing_archive_pins_survive_gc_until_matching_ack_or_expiry() {
        let fixture = Fixture::new();
        let head = generation(&fixture);
        let now = Instant::now();
        let mut send = SnapshotTransfers::default();
        send.offer(
            FOLLOWER,
            2,
            snapshot(vec![5; 3], 7),
            fixture.archive.pin_heads(&[head.clone()]).unwrap(),
            now,
        )
        .unwrap()
        .unwrap();
        assert_eq!(fixture.archive.collect(&HashSet::new()).unwrap(), 0);
        send.acknowledged(OTHER, 7);
        send.acknowledged(FOLLOWER, 6);
        assert_eq!(fixture.archive.collect(&HashSet::new()).unwrap(), 0);
        send.acknowledged(FOLLOWER, 7);
        assert_eq!(fixture.archive.collect(&HashSet::new()).unwrap(), 1);
        let head = generation(&fixture);
        send.offer(
            FOLLOWER,
            2,
            snapshot(vec![5; 3], 7),
            fixture.archive.pin_heads(&[head]).unwrap(),
            now,
        )
        .unwrap()
        .unwrap();
        send.expire(now + TTL);
        assert_eq!(fixture.archive.collect(&HashSet::new()).unwrap(), 1);
    }

    #[test]
    fn different_peers_share_one_payload_until_acknowledged() {
        let fixture = Fixture::new();
        let now = Instant::now();
        let mut send = SnapshotTransfers::default();
        let old = send
            .offer(FOLLOWER, 2, snapshot(vec![5; 3], 7), fixture.lease(), now)
            .unwrap()
            .unwrap();
        let pointer = Arc::as_ptr(&send.shared.as_ref().unwrap().snapshot);
        send.offer(OTHER, 2, snapshot(vec![5; 3], 7), fixture.lease(), now)
            .unwrap()
            .unwrap();
        assert_eq!(
            pointer,
            Arc::as_ptr(&send.shared.as_ref().unwrap().snapshot)
        );
        let retained = send
            .offer(FOLLOWER, 2, snapshot(vec![5; 4], 8), fixture.lease(), now)
            .unwrap()
            .unwrap();
        assert_eq!(retained, old);
        assert!(
            send.reply_chunk(
                FOLLOWER,
                &Request {
                    ticket: old.ticket,
                    offset: 0
                },
                now
            )
            .is_some()
        );
        send.acknowledged(FOLLOWER, 7);
        send.acknowledged(OTHER, 7);
        let new = send
            .offer(OTHER, 2, snapshot(vec![5; 4], 8), fixture.lease(), now)
            .unwrap()
            .unwrap();
        assert_eq!(new.index, 8);
        send.acknowledged(OTHER, 8);
        let mut receive = SnapshotTransfers::default();
        let empty = send
            .offer(FOLLOWER, 2, snapshot(Vec::new(), 0), fixture.lease(), now)
            .unwrap()
            .unwrap();
        assert!(
            matches!(receive.accept_offer(LEADER, empty, now).unwrap(), Receive::Complete {snapshot, ..} if snapshot.data.is_empty())
        );
    }

    #[test]
    fn later_compaction_defers_new_peer_without_releasing_active_generation() {
        let fixture = Fixture::new();
        let head = generation(&fixture);
        let now = Instant::now();
        let mut send = SnapshotTransfers::default();
        let old = send
            .offer(
                FOLLOWER,
                2,
                snapshot(vec![5; 3], 7),
                fixture.archive.pin_heads(&[head.clone()]).unwrap(),
                now,
            )
            .unwrap()
            .unwrap();
        assert!(
            send.offer(OTHER, 2, snapshot(vec![5; 4], 8), fixture.lease(), now)
                .unwrap()
                .is_none()
        );
        assert!(
            send.offer(FOLLOWER, 3, snapshot(vec![5; 4], 8), fixture.lease(), now)
                .unwrap()
                .is_none()
        );
        assert_eq!(fixture.archive.collect(&HashSet::new()).unwrap(), 0);
        assert!(fixture.archive.has(&head));
        assert!(
            send.reply_chunk(
                FOLLOWER,
                &Request {
                    ticket: old.ticket,
                    offset: 0
                },
                now
            )
            .is_some()
        );
        send.acknowledged(FOLLOWER, 7);
        assert_eq!(fixture.archive.collect(&HashSet::new()).unwrap(), 1);
        assert_eq!(
            send.offer(OTHER, 3, snapshot(vec![5; 4], 8), fixture.lease(), now)
                .unwrap()
                .unwrap()
                .index,
            8
        );
        send.expire(now + TTL);
        let head = generation(&fixture);
        send.offer(
            FOLLOWER,
            3,
            snapshot(vec![5; 4], 8),
            fixture.archive.pin_heads(&[head]).unwrap(),
            now + TTL,
        )
        .unwrap()
        .unwrap();
        assert!(
            send.offer(
                OTHER,
                3,
                snapshot(vec![5; 5], 9),
                fixture.lease(),
                now + TTL
            )
            .unwrap()
            .is_none()
        );
        send.expire(now + TTL * 2);
        assert_eq!(fixture.archive.collect(&HashSet::new()).unwrap(), 1);
        assert_eq!(
            send.offer(
                OTHER,
                3,
                snapshot(vec![5; 5], 9),
                fixture.lease(),
                now + TTL * 2
            )
            .unwrap()
            .unwrap()
            .index,
            9
        );
    }
}
