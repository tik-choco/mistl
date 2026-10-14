//! Local IPC bridge for already encrypted public escrow requests.
use super::protocol::{Delete, Head, MAX_MESSAGE, hex_string};
use crate::daemon::AppState;
use anyhow::{Context, Result, bail, ensure};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::oneshot;

const MAX_PENDING: usize = 32;
const REQUEST_WAIT: Duration = Duration::from_secs(35);
const MAX_BLOB: u64 = 1024 * 1024 * 1024;
static CLIENT: OnceLock<Arc<Client>> = OnceLock::new();

#[derive(Clone)]
enum Expected {
    Get {
        slot: String,
    },
    Put {
        slot: String,
        seq: u64,
        hash: String,
    },
    Delete {
        slot: String,
    },
}
impl Expected {
    fn slot(&self) -> &str {
        match self {
            Self::Get { slot } | Self::Put { slot, .. } | Self::Delete { slot } => slot,
        }
    }

    fn accepts(&self, reply: &Value) -> bool {
        if reply["v"] != 1 {
            return false;
        }
        if reply["type"] == "escrow_error" {
            let bounded_text = |key: &str, limit: usize| {
                reply[key]
                    .as_str()
                    .is_some_and(|s| !s.is_empty() && s.len() <= limit)
            };
            if !bounded_text("code", 64) || !bounded_text("message", 512) {
                return false;
            }
            return reply
                .get("head")
                .is_none_or(|head| head.is_null() || valid_head(head, self.slot()));
        }
        if reply["slot"].as_str() != Some(self.slot()) {
            return false;
        }
        match self {
            Self::Get { slot } => {
                reply["type"] == "escrow_head"
                    && reply
                        .get("head")
                        .is_some_and(|head| head.is_null() || valid_head(head, slot))
            }
            Self::Put { seq, hash, .. } => {
                reply["type"] == "escrow_ack"
                    && reply["seq"].as_u64() == Some(*seq)
                    && reply["hash"].as_str() == Some(hash)
            }
            Self::Delete { .. } => reply["type"] == "escrow_deleted",
        }
    }
}

fn valid_head(value: &Value, slot: &str) -> bool {
    serde_json::from_value::<Head>(value.clone()).is_ok_and(|head| {
        head.slot == slot && head.validate(MAX_BLOB).is_ok() && head.verify().is_ok()
    })
}

fn prepare(request: &Value, id: &str) -> Result<(Value, Expected)> {
    ensure!(
        request["v"] == 1,
        "escrow client requires protocol version 1"
    );
    ensure!(
        serde_json::to_vec(request)?.len() <= MAX_MESSAGE,
        "oversized escrow request"
    );
    match request["type"].as_str() {
        Some("escrow_get") => {
            let slot = request["slot"]
                .as_str()
                .filter(|s| hex_string(s, 32))
                .context("invalid escrow slot")?;
            Ok((
                json!({"v":1,"type":"escrow_get","id":id,"slot":slot}),
                Expected::Get { slot: slot.into() },
            ))
        }
        Some("escrow_put") => {
            ensure!(
                request["head"].get("parent").is_some(),
                "escrow head requires parent"
            );
            let head: Head =
                serde_json::from_value(request["head"].clone()).context("invalid escrow head")?;
            head.validate(MAX_BLOB)
                .map_err(|error| anyhow::anyhow!(error.code))?;
            head.verify().map_err(|error| anyhow::anyhow!(error.code))?;
            let expected = Expected::Put {
                slot: head.slot.clone(),
                seq: head.seq,
                hash: head.hash(),
            };
            Ok((
                json!({"v":1,"type":"escrow_put","id":id,"head":head}),
                expected,
            ))
        }
        Some("escrow_delete") => {
            let request: Delete =
                serde_json::from_value(request.clone()).context("invalid escrow delete")?;
            request
                .validate()
                .map_err(|error| anyhow::anyhow!(error.code))?;
            ensure!(
                URL_SAFE_NO_PAD
                    .decode(&request.sig)
                    .is_ok_and(|bytes| bytes.len() == 64),
                "invalid delete signature"
            );
            let expected = Expected::Delete {
                slot: request.slot.clone(),
            };
            Ok((
                json!({"v":1,"type":"escrow_delete","id":id,"slot":request.slot,"seq":request.seq,"head":request.head,"sig":request.sig}),
                expected,
            ))
        }
        _ => bail!("escrow client accepts get, put, or delete requests"),
    }
}

struct Pending {
    room: String,
    peer: String,
    expected: Expected,
    tx: oneshot::Sender<Value>,
}
#[derive(Default)]
struct Client {
    pending: Mutex<HashMap<String, Pending>>,
}
struct Guard<'a> {
    client: &'a Client,
    id: String,
}
impl Drop for Guard<'_> {
    fn drop(&mut self) {
        self.client.pending.lock().unwrap().remove(&self.id);
    }
}

impl Client {
    fn reserve(
        &self,
        room: &str,
        peer: &str,
        id: String,
        expected: Expected,
    ) -> Result<(Guard<'_>, oneshot::Receiver<Value>)> {
        let mut pending = self.pending.lock().unwrap();
        ensure!(pending.len() < MAX_PENDING, "escrow client busy");
        ensure!(!pending.contains_key(&id), "escrow request id collision");
        let (tx, rx) = oneshot::channel();
        pending.insert(
            id.clone(),
            Pending {
                room: room.into(),
                peer: peer.into(),
                expected,
                tx,
            },
        );
        Ok((Guard { client: self, id }, rx))
    }

    fn receive(&self, room: &str, peer: &str, raw: &[u8]) {
        if raw.len() > MAX_MESSAGE {
            return;
        }
        let Ok(reply) = serde_json::from_slice::<Value>(raw) else {
            return;
        };
        let Some(id) = reply["id"].as_str() else {
            return;
        };
        let mut pending = self.pending.lock().unwrap();
        let Some(request) = pending.get(id) else {
            return;
        };
        if request.room != room || request.peer != peer || !request.expected.accepts(&reply) {
            return;
        }
        if let Some(request) = pending.remove(id) {
            let _ = request.tx.send(reply);
        }
    }

    async fn round_trip<F, Fut>(
        &self,
        room: &str,
        peer: &str,
        request: &Value,
        timeout: Duration,
        send: F,
    ) -> Result<Value>
    where
        F: FnOnce(Vec<u8>) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let id = format!("ec-{}", crate::net::peer_auth::fresh_nonce());
        let (request, expected) = prepare(request, &id)?;
        let (_guard, rx) = self.reserve(room, peer, id, expected)?;
        tokio::time::timeout(timeout, async {
            send(serde_json::to_vec(&request)?).await?;
            rx.await.context("escrow response channel closed")
        })
        .await
        .context("escrow request timed out; inspect remote status before retrying")?
    }
}

fn client() -> Arc<Client> {
    CLIENT
        .get_or_init(|| {
            let client = Arc::new(Client::default());
            let handler = client.clone();
            crate::net::register_room_handler(move |event, room, peer, raw| {
                if event == crate::net::EVENT_RAW {
                    handler.receive(room, peer, raw);
                }
            });
            client
        })
        .clone()
}

pub(super) async fn request(args: Value, state: &Arc<AppState>) -> Result<Value> {
    let room = args["room"]
        .as_str()
        .filter(|r| super::valid_room(r))
        .context("invalid escrow client room")?;
    let peer = args["peer"]
        .as_str()
        .filter(|p| super::valid_voter(p))
        .context("invalid escrow client peer")?;
    // Validate before starting a network session or installing a pending request.
    prepare(&args["request"], "validation")?;
    state.network.require_online()?;
    let client = client();
    let _transport = crate::net::ensure_started(state, room.into()).await?;
    client
        .round_trip(room, peer, &args["request"], REQUEST_WAIT, |bytes| {
            crate::net::send_direct(room, peer, bytes)
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};
    use mistlib_core::storage::cid::{MULTICODEC_RAW, compute_cid};

    const ROOM: &str = "room-a";
    const PEER: &str = "0123456789abcdef";
    const SLOT: &str = "abababababababababababababababab";
    fn get() -> Value {
        json!({"v":1,"type":"escrow_get","slot":SLOT})
    }
    fn fixture() -> Head {
        let key = SigningKey::from_bytes(&[7; 32]);
        let blob = b"ciphertext";
        let mut head = Head {
            v: 1,
            kind: "tc-escrow-head".into(),
            slot: SLOT.into(),
            seq: 1,
            parent: None,
            cid: compute_cid(blob, MULTICODEC_RAW),
            size: blob.len() as u64,
            sha256: super::super::protocol::digest(blob),
            comp: "none".into(),
            schema: "test/1".into(),
            writer: URL_SAFE_NO_PAD.encode(key.verifying_key().as_bytes()),
            at: 1,
            sig: String::new(),
        };
        head.sig = URL_SAFE_NO_PAD.encode(key.sign(&head.signing_payload()).to_bytes());
        head
    }
    #[test]
    fn requests_normalize_public_fields_and_reject_invalid_signatures_and_slots() {
        let head = fixture();
        let (wire, expected) = prepare(
            &json!({"v":1,"type":"escrow_put","head":head,"ignored":"extra"}),
            "generated",
        )
        .unwrap();
        assert_eq!(wire["id"], "generated");
        assert!(wire.get("ignored").is_none());
        assert!(expected.accepts(&head.ack("generated")));
        let mut invalid = head.clone();
        invalid.seq += 1;
        assert!(prepare(&json!({"v":1,"type":"escrow_put","head":invalid}), "id").is_err());
        assert!(prepare(&json!({"v":1,"type":"escrow_get","slot":"bad"}), "id").is_err());
        assert!(prepare(&json!({"v":1,"type":"other","slot":SLOT}), "id").is_err());
        assert!(prepare(&json!({"v":1,"type":"escrow_delete","slot":SLOT,"seq":1,"head":head.hash(),"sig":"!".repeat(86)}),"id").is_err());
    }

    #[test]
    fn replies_bind_room_peer_generated_id_and_signed_head() {
        let client = Client::default();
        let (_guard, mut rx) = client
            .reserve(ROOM, PEER, "id".into(), Expected::Get { slot: SLOT.into() })
            .unwrap();
        let valid = json!({"v":1,"type":"escrow_head","id":"id","slot":SLOT,"head":fixture()});
        let raw = serde_json::to_vec(&valid).unwrap();
        client.receive("other-room", PEER, &raw);
        client.receive(ROOM, "fedcba9876543210", &raw);
        let mut wrong_id = valid.clone();
        wrong_id["id"] = json!("other");
        client.receive(ROOM, PEER, &serde_json::to_vec(&wrong_id).unwrap());
        let mut invalid_head = valid.clone();
        invalid_head["head"]["seq"] = json!(2);
        client.receive(ROOM, PEER, &serde_json::to_vec(&invalid_head).unwrap());
        let mut wrong_kind = valid.clone();
        wrong_kind["type"] = json!("escrow_deleted");
        client.receive(ROOM, PEER, &serde_json::to_vec(&wrong_kind).unwrap());
        client.receive(ROOM, PEER, &vec![b' '; MAX_MESSAGE + 1]);
        assert!(matches!(
            rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        client.receive(ROOM, PEER, &raw);
        assert_eq!(rx.try_recv().unwrap(), valid);
        assert!(client.pending.lock().unwrap().is_empty());
    }

    #[test]
    fn acknowledgements_errors_and_deletes_require_expected_shape() {
        let head = fixture();
        let expected = Expected::Put {
            slot: head.slot.clone(),
            seq: head.seq,
            hash: head.hash(),
        };
        assert!(expected.accepts(&head.ack("id")));
        let mut wrong = head.ack("id");
        wrong["hash"] = json!("00".repeat(32));
        assert!(!expected.accepts(&wrong));
        let mut wrong = head.ack("id");
        wrong["seq"] = json!(2);
        assert!(!expected.accepts(&wrong));
        let error = super::super::protocol::Error::conflict(&head).reply("id");
        assert!(expected.accepts(&error));
        let mut bad = error.clone();
        bad["head"]["sig"] = json!("x".repeat(86));
        assert!(!expected.accepts(&bad));
        let deleted = json!({"v":1,"type":"escrow_deleted","id":"id","slot":SLOT});
        assert!(Expected::Delete { slot: SLOT.into() }.accepts(&deleted));
        assert!(!Expected::Get { slot: SLOT.into() }.accepts(&deleted));
    }

    #[test]
    fn outstanding_requests_are_bounded_and_released_on_guard_drop() {
        let client = Client::default();
        let mut pending = Vec::new();
        for index in 0..MAX_PENDING {
            pending.push(
                client
                    .reserve(
                        ROOM,
                        PEER,
                        index.to_string(),
                        Expected::Get { slot: SLOT.into() },
                    )
                    .unwrap(),
            );
        }
        assert!(
            client
                .reserve(
                    ROOM,
                    PEER,
                    "overflow".into(),
                    Expected::Get { slot: SLOT.into() }
                )
                .is_err()
        );
        assert_eq!(client.pending.lock().unwrap().len(), MAX_PENDING);
        drop(pending);
        assert!(client.pending.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn round_trip_handles_reply_send_failure_and_timeout_without_leaking_slots() {
        let client = Client::default();
        let receiver = &client;
        let result = client
            .round_trip(ROOM, PEER, &get(), REQUEST_WAIT, |wire| async move {
                let request: Value = serde_json::from_slice(&wire)?;
                let reply =
                    json!({"v":1,"type":"escrow_head","id":request["id"],"slot":SLOT,"head":null});
                receiver.receive(ROOM, PEER, &serde_json::to_vec(&reply)?);
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(result["type"], "escrow_head");
        assert!(
            client
                .round_trip(ROOM, PEER, &get(), REQUEST_WAIT, |_| async {
                    bail!("send failed")
                })
                .await
                .is_err()
        );
        assert!(
            client
                .round_trip(ROOM, PEER, &get(), Duration::ZERO, |_| async { Ok(()) })
                .await
                .is_err()
        );
        assert!(client.pending.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn cancelled_request_removes_its_pending_slot() {
        let client = Arc::new(Client::default());
        let worker = client.clone();
        let (sent, ready) = oneshot::channel();
        let task = tokio::spawn(async move {
            worker
                .round_trip(ROOM, PEER, &get(), REQUEST_WAIT, |_| async move {
                    let _ = sent.send(());
                    Ok(())
                })
                .await
        });
        ready.await.unwrap();
        assert_eq!(client.pending.lock().unwrap().len(), 1);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(client.pending.lock().unwrap().is_empty());
    }
}
