use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tracing::{debug, error};

use crate::tunnel::auth::AuthRequest;
use crate::tunnel::rtc::{RTCManager, TunnelMessage};

use super::{
    MAX_PENDING_UDP_PACKETS, MAX_UDP_SESSIONS_PER_PEER, MAX_UDP_SIZE, PendingUdp, UdpConn,
    UdpManager,
};

impl UdpManager {
    pub async fn handle_data(self: &Arc<Self>, peer_id: &str, tm: &TunnelMessage) {
        let payload = match &tm.payload {
            Some(p) if !p.is_empty() => p,
            _ => return,
        };

        enum SendTarget {
            Connected(
                Arc<UdpSocket>,
                crate::tunnel::forward_runtime::ForwardPeerRuntime,
            ),
            Local(
                Arc<UdpSocket>,
                SocketAddr,
                crate::tunnel::forward_runtime::ForwardPeerRuntime,
            ),
        }

        let existing = {
            let conns = self.conns.read().await;
            if let Some(uc) = conns.get(&tm.conn_id) {
                // Conn ids are chosen by the sender: only the peer that owns
                // a session may feed it, otherwise any room peer could inject
                // into (or reflect through) someone else's session.
                if uc.peer_id != peer_id {
                    debug!(
                        "dropping udp data for conn {} from non-owner peer {}",
                        tm.conn_id, peer_id
                    );
                    return;
                }
                if let Some(target) = &uc.target_conn {
                    Some(SendTarget::Connected(target.clone(), uc.metrics.clone()))
                } else if let Some(addr) = uc.client_addr {
                    // Replies go only to the local client that opened this
                    // session, never to an address supplied by the remote.
                    let sock = self.local_socket.read().await.clone();
                    sock.map(|sock| SendTarget::Local(sock, addr, uc.metrics.clone()))
                } else {
                    None
                }
            } else {
                None
            }
        };

        if let Some(target) = existing {
            let sent = match &target {
                SendTarget::Connected(sock, _) => sock.send(payload).await.is_ok(),
                SendTarget::Local(sock, addr, _) => sock.send_to(payload, addr).await.is_ok(),
            };
            if sent {
                let metrics = match target {
                    SendTarget::Connected(_, metrics) | SendTarget::Local(_, _, metrics) => metrics,
                };
                metrics.record_bytes_out(payload.len());
            }
            return;
        }

        // No session for this conn id. On the connect side (no `remote_addr`)
        // there is nothing to open: sessions there are created only by local
        // client packets, so unsolicited remote data is dropped.
        if self.remote_addr.is_empty() {
            return;
        }

        // Serve side: a new session needs authorization first. That can block
        // on a human, so it runs off this message loop; packets for the conn
        // that arrive meanwhile are queued (bounded) on `pending`.
        let sessions = {
            let conns = self.conns.read().await;
            conns.values().filter(|uc| uc.peer_id == peer_id).count()
        };
        {
            let mut pending = self.pending.lock().await;
            if let Some(entry) = pending.get_mut(&tm.conn_id) {
                if entry.peer_id == peer_id && entry.queued.len() < MAX_PENDING_UDP_PACKETS {
                    entry.queued.push(payload.clone());
                }
                return;
            }
            let pending_for_peer = pending.values().filter(|p| p.peer_id == peer_id).count();
            if sessions + pending_for_peer >= MAX_UDP_SESSIONS_PER_PEER {
                debug!(
                    "udp session limit reached for {}; dropping new conn {}",
                    peer_id, tm.conn_id
                );
                return;
            }
            pending.insert(
                tm.conn_id.clone(),
                PendingUdp {
                    peer_id: peer_id.to_string(),
                    queued: vec![payload.clone()],
                },
            );
        }

        let mgr = self.clone();
        let conn_id = tm.conn_id.clone();
        let peer_id = peer_id.to_string();
        tokio::spawn(async move { mgr.authorize_and_open(conn_id, peer_id).await });
    }

    /// Resolves authorization for a pending serve-side session (spawned by
    /// `handle_data`) and, if allowed, opens the backend socket, flushes the
    /// packets queued meanwhile and starts relaying replies.
    async fn authorize_and_open(self: Arc<Self>, conn_id: String, peer_id: String) {
        let allowed = self.authorize_remote_session(&peer_id).await;
        if !allowed || self.runtime.is_cancelled() {
            self.pending.lock().await.remove(&conn_id);
            debug!(
                "denied udp tunnel session from {} to {}",
                peer_id, self.target
            );
            return;
        }

        let sock = match UdpSocket::bind("0.0.0.0:0").await {
            Ok(sock) => sock,
            Err(e) => {
                error!("Failed to bind UDP: {}", e);
                self.pending.lock().await.remove(&conn_id);
                return;
            }
        };
        if let Err(e) = sock.connect(&self.remote_addr).await {
            error!("Failed to connect UDP: {}", e);
            self.pending.lock().await.remove(&conn_id);
            return;
        }
        let sock = Arc::new(sock);
        let metrics = self.runtime.peer(&peer_id);

        // Lock order is conns -> pending (`handle_data` never holds `pending`
        // while taking `conns`), so the session becomes visible in `conns`
        // in the same step its pending entry disappears.
        let queued = {
            let mut conns = self.conns.write().await;
            let Some(entry) = self.pending.lock().await.remove(&conn_id) else {
                return;
            };
            let old = conns.insert(
                conn_id.clone(),
                UdpConn {
                    target_conn: Some(sock.clone()),
                    last_seen: Instant::now(),
                    peer_id: peer_id.clone(),
                    metrics: metrics.clone(),
                    client_addr: None,
                },
            );
            if old.is_none() {
                metrics.record_conn_open();
            }
            entry.queued
        };
        for packet in queued {
            if sock.send(&packet).await.is_ok() {
                metrics.record_bytes_out(packet.len());
            }
        }

        let mgr_conns = self.conns.clone();
        let rtc = self.rtc_manager.clone();
        let target = self.target.clone();
        let runtime = self.runtime.clone();
        tokio::spawn(async move {
            Self::forward_target_to_tunnel(sock, mgr_conns, rtc, conn_id, peer_id, target, runtime)
                .await;
        });
    }

    async fn authorize_remote_session(&self, peer_id: &str) -> bool {
        let req = AuthRequest {
            peer_id: peer_id.to_string(),
            forward_key: self.target.clone(),
            target_addr: self.remote_addr.clone(),
            proto: "udp".to_string(),
        };
        self.authorizer.authorize(&req).await.is_allowed()
    }

    async fn forward_target_to_tunnel(
        sock: Arc<UdpSocket>,
        conns: Arc<RwLock<HashMap<String, UdpConn>>>,
        rtc_manager: RTCManager,
        conn_id: String,
        peer_id: String,
        target: String,
        runtime: crate::tunnel::forward_runtime::ForwardRuntime,
    ) {
        let mut buf = vec![0u8; MAX_UDP_SIZE];
        let mut shutdown = runtime.subscribe();
        let metrics = runtime.peer(&peer_id);
        loop {
            tokio::select! {
                result = sock.recv(&mut buf) => {
                    match result {
                        Ok(n) => {
                            {
                                let mut conns = conns.write().await;
                                if let Some(uc) = conns.get_mut(&conn_id) {
                                    uc.last_seen = Instant::now();
                                }
                            }
                            let msg = TunnelMessage {
                                msg_type: "data".into(),
                                conn_id: conn_id.clone(),
                                target: target.clone(),
                                payload: Some(buf[..n].to_vec()),
                                // UDP tunnel traffic is unordered/unreliable
                                // already; sequencing is only wired up for
                                // the TCP tunnel path (see
                                // `TunnelMessage::seq`).
                                seq: None,
                            };
                            let data = match serde_json::to_vec(&msg) {
                                Ok(d) => d,
                                Err(_) => continue,
                            };
                            if rtc_manager.send_tunnel_to(&peer_id, data).await.is_ok() {
                                metrics.record_bytes_in(n);
                            }
                        }
                        Err(e) => {
                            error!("UDP target read error: {}", e);
                            return;
                        }
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
            }
        }
    }
}
