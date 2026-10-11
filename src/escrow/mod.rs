//! Config-driven tc-escrow v1 service. Settings take effect on daemon restart.
mod admission;
mod protocol;
mod repository;
mod serving;
#[cfg(test)]
mod tests;
mod transfer;

use crate::config::EscrowConfig;
use crate::daemon::AppState;
use admission::{Admission, Class};
use anyhow::{Context, Result, bail};
use protocol::{Error, Head, MAX_MESSAGE, hex_string};
use repository::Repository;
use serde_json::{Value, json};
use serving::Serving;
use std::collections::HashSet;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::sync::{Mutex, OnceCell};

const SLOT_WAIT: Duration = Duration::from_secs(5);
const REPLY_WAIT: Duration = Duration::from_secs(2);
use transfer::Transfers;

static SERVICE: OnceCell<Arc<Service>> = OnceCell::const_new();

pub fn valid_room(room: &str) -> bool {
    !room.is_empty()
        && room.len() <= 96
        && room
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_./:-".contains(&b))
}

struct Service {
    cfg: EscrowConfig,
    repo: Repository,
    joined: StdMutex<HashSet<String>>,
    slots: Vec<Mutex<()>>,
    transfers: Transfers,
    requests: Arc<Admission>,
    serving: Serving,
    pulling: StdMutex<HashSet<String>>,
}

impl Service {
    fn open(dir: std::path::PathBuf, cfg: EscrowConfig) -> Result<Self> {
        Ok(Self {
            repo: Repository::open(dir, cfg.clone())?,
            cfg,
            joined: StdMutex::new(HashSet::new()),
            // Fixed lock stripes serialize each (room,slot) without an unbounded lock map.
            slots: (0..256).map(|_| Mutex::new(())).collect(),
            transfers: Transfers::default(),
            requests: Arc::new(Admission::default()),
            serving: Serving::default(),
            pulling: StdMutex::new(HashSet::new()),
        })
    }

    fn provider(&self) -> Value {
        json!({"v":1,"type":"escrow_provider","maxBlob":self.cfg.max_blob_bytes,"version":1})
    }

    async fn put(&self, room: &str, from: &str, head: Head, id: &str) -> Result<Value, Error> {
        head.validate(self.cfg.max_blob_bytes)?;
        let stripe = usize::from(sha2_byte(room, &head.slot));
        let _slot = tokio::time::timeout(SLOT_WAIT, self.slots[stripe].lock())
            .await
            .map_err(|_| Error::new("busy"))?;
        let Some(_reservation) = self.repo.reserve(room, &head)? else {
            return Ok(head.ack(id));
        };
        // No waiting task queue: at most four PUTs and one per transport peer globally.
        let _pull = {
            let mut pulling = self.pulling.lock().unwrap();
            if pulling.len() >= 4 || !pulling.insert(from.to_string()) {
                return Err(Error::new("busy"));
            }
            PullGuard {
                service: self,
                from: from.into(),
            }
        };
        let (blob, manifest) = self.transfers.pull(room, from, &head).await?;
        self.repo.commit(room, &head, &blob, &manifest)?;
        Ok(head.ack(id))
    }

    async fn request(&self, room: &str, from: &str, msg: Value) -> Option<Value> {
        if msg["v"] != 1 {
            return None;
        }
        if msg["type"] == "escrow_hello" {
            return Some(self.provider());
        }
        let kind = msg["type"].as_str()?;
        if !matches!(kind, "escrow_get" | "escrow_put") {
            return None;
        }
        let Some(id) = msg["id"].as_str().filter(|s| s.len() <= 64) else {
            return Some(Error::new("bad_request").reply(""));
        };
        let result = match kind {
            "escrow_get" => {
                if let Some(slot) = msg["slot"].as_str().filter(|s| hex_string(s, 32)) {
                    Ok(
                        json!({"v":1,"type":"escrow_head","id":id,"slot":slot,"head":self.repo.head(room, slot)}),
                    )
                } else {
                    Err(Error::new("bad_request"))
                }
            }
            _ => {
                if msg["head"].get("parent").is_none() {
                    Err(Error::new("bad_request"))
                } else if let Ok(head) = serde_json::from_value::<Head>(msg["head"].clone()) {
                    self.put(room, from, head, id).await
                } else {
                    Err(Error::new("bad_request"))
                }
            }
        };
        Some(result.unwrap_or_else(|error| error.reply(id)))
    }

    // Admission happens synchronously before spawn; parked PUTs count against the
    // sender across rooms. Reply capacity cannot be occupied by a long blob pull.
    fn enqueue_request(
        self: &Arc<Self>,
        rt: &tokio::runtime::Handle,
        room: &str,
        from: &str,
        msg: Value,
    ) {
        if msg["v"] != 1 {
            return;
        }
        let class = if msg["type"] == "escrow_put" {
            Class::Put
        } else {
            Class::Reply
        };
        let (permit, busy) = match self.requests.acquire(from, class) {
            Some(permit) => (permit, false),
            None if matches!(class, Class::Put) => {
                let Some(permit) = self.requests.acquire(from, Class::Busy) else {
                    return;
                };
                (permit, true)
            }
            None => return,
        };
        let (service, room, from) = (self.clone(), room.to_string(), from.to_string());
        rt.spawn(async move {
            let _permit = permit;
            let reply = if busy {
                let id = msg["id"].as_str().filter(|s| s.len() <= 64).unwrap_or("");
                Some(Error::new("busy").reply(id))
            } else {
                service.request(&room, &from, msg).await
            };
            if let Some(reply) = reply {
                let bytes = serde_json::to_vec(&reply).unwrap();
                if bytes.len() <= MAX_MESSAGE {
                    let _ =
                        tokio::time::timeout(REPLY_WAIT, (service.serving.send)(room, from, bytes))
                            .await;
                }
            }
        });
    }

    fn register(self: &Arc<Self>) {
        let service = self.clone();
        let rt = tokio::runtime::Handle::current();
        crate::net::register_room_handler(move |event, room, from, raw| {
            if !service.cfg.rooms.iter().any(|r| r == room) {
                return;
            }
            if event == crate::net::EVENT_JOIN {
                let Some(permit) = service.requests.acquire(from, Class::Reply) else {
                    return;
                };
                let (service, room) = (service.clone(), room.to_string());
                rt.spawn(async move {
                    let _permit = permit;
                    let bytes = serde_json::to_vec(&service.provider()).unwrap();
                    let _ =
                        tokio::time::timeout(REPLY_WAIT, crate::net::send_broadcast(&room, bytes))
                            .await;
                });
                return;
            }
            if event != crate::net::EVENT_RAW {
                return;
            }
            // Transfer parsing uses borrowed bytes and enforces the pending request's cap.
            service.transfers.receive(room, from, raw);
            if service.enqueue_storage(&rt, room, from, raw) {
                return;
            }
            if raw.len() > MAX_MESSAGE
                || raw.iter().find(|b| !b.is_ascii_whitespace()) != Some(&b'{')
            {
                return;
            }
            let Ok(msg) = serde_json::from_slice::<Value>(raw) else {
                return;
            };
            if !matches!(
                msg["type"].as_str(),
                Some("escrow_hello" | "escrow_get" | "escrow_put")
            ) {
                return;
            }
            service.enqueue_request(&rt, room, from, msg);
        });
    }
}

fn sha2_byte(room: &str, slot: &str) -> u8 {
    use sha2::{Digest, Sha256};
    Sha256::digest(format!("{room}\n{slot}").as_bytes())[0]
}

struct PullGuard<'a> {
    service: &'a Service,
    from: String,
}
impl Drop for PullGuard<'_> {
    fn drop(&mut self) {
        self.service.pulling.lock().unwrap().remove(&self.from);
    }
}

async fn local(state: &Arc<AppState>) -> Result<Arc<Service>> {
    Ok(SERVICE
        .get_or_try_init(|| async {
            Service::open(
                crate::config::data_dir()?.join("escrow"),
                state.config().escrow,
            )
            .map(Arc::new)
        })
        .await?
        .clone())
}

pub fn spawn_background(state: Arc<AppState>) {
    tokio::spawn(async move {
        if let Err(error) = start(&state).await {
            tracing::warn!(%error, "escrow: failed to start");
        }
    });
}

async fn start(state: &Arc<AppState>) -> Result<()> {
    if !state.network.permitted() || !state.config().escrow.enabled {
        return Ok(());
    }
    let service = local(state).await?;
    service.register();
    for room in &service.cfg.rooms {
        match crate::net::ensure_started(state, room.clone()).await {
            Ok(_) => {
                service.joined.lock().unwrap().insert(room.clone());
            }
            Err(error) => tracing::warn!(%error, "escrow: room join failed"),
        }
    }
    // Joins are asynchronous. Hello requests and peer joins also advertise later.
    tokio::time::sleep(std::time::Duration::from_millis(700)).await;
    for room in &service.cfg.rooms {
        let _ = crate::net::send_broadcast(room, serde_json::to_vec(&service.provider())?).await;
    }
    Ok(())
}

pub async fn handle(cmd: &str, args: Value, state: &Arc<AppState>) -> Result<Value> {
    match cmd {
        "escrow.rooms.add" | "escrow.rooms.remove" => {
            let room = args["room"]
                .as_str()
                .context("escrow rooms requires room")?;
            state.set_escrow_room(room, cmd == "escrow.rooms.add")?;
            Ok(
                json!({"saved":true,"applies":"daemon restart","enabled":state.config().escrow.enabled,"rooms":state.config().escrow.rooms}),
            )
        }
        "escrow.status" | "escrow.rooms.list" => {
            let service = local(state).await?;
            let cfg = state.config().escrow;
            let mut rooms = cfg.rooms.clone();
            rooms.extend(service.repo.rooms());
            rooms.extend(service.cfg.rooms.clone());
            rooms.sort();
            rooms.dedup();
            let joined = service.joined.lock().unwrap();
            let rooms: Vec<_> = rooms.iter().map(|room| {
                let (slots, bytes) = service.repo.status(room);
                json!({"room":room,"configured":cfg.rooms.contains(room),"joined":joined.contains(room),"slots":slots,"bytes":bytes})
            }).collect();
            Ok(
                json!({"enabled":cfg.enabled,"running":!joined.is_empty(),"rooms":rooms,"bytes":service.repo.total_bytes(),"applies":"daemon restart"}),
            )
        }
        _ => bail!("unknown escrow command: {cmd}"),
    }
}
