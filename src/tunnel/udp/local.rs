use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::net::UdpSocket;
use tracing::{debug, error};

use crate::tunnel::rtc::TunnelMessage;

use super::{
    CLEANUP_INTERVAL, MAX_UDP_SIZE, RETRY_INTERVAL, TUNNEL_READY_TIMEOUT, UDP_TIMEOUT, UdpConn,
    UdpManager,
};

impl UdpManager {
    pub async fn read_local_packets(self: Arc<Self>, socket: Arc<UdpSocket>) {
        let mut buf = vec![0u8; MAX_UDP_SIZE];
        let mut shutdown = self.runtime.subscribe();
        loop {
            tokio::select! {
                result = socket.recv_from(&mut buf) => {
                    match result {
                        Ok((n, addr)) => {
                            self.handle_local_packet(&buf[..n], addr).await;
                        }
                        Err(e) => {
                            error!("UDP read error: {}", e);
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

    async fn handle_local_packet(&self, payload: &[u8], addr: std::net::SocketAddr) {
        let conn_id = addr.to_string();
        let existing = {
            let conns = self.conns.read().await;
            conns
                .get(&conn_id)
                .map(|c| (c.peer_id.clone(), c.metrics.clone()))
        };

        let (peer_id, metrics) = match existing {
            Some(existing) => existing,
            None => match self.wait_for_tunnel_ready(TUNNEL_READY_TIMEOUT).await {
                Ok(id) => {
                    let metrics = self.runtime.peer(&id);
                    let mut conns = self.conns.write().await;
                    let old = conns.insert(
                        conn_id.clone(),
                        UdpConn {
                            target_conn: None,
                            last_seen: Instant::now(),
                            peer_id: id.clone(),
                            metrics: metrics.clone(),
                            client_addr: Some(addr),
                        },
                    );
                    if old.is_none() {
                        metrics.record_conn_open();
                    }
                    (id, metrics)
                }
                Err(e) => {
                    error!("Tunnel not ready for UDP: {}", e);
                    return;
                }
            },
        };

        {
            let mut conns = self.conns.write().await;
            if let Some(uc) = conns.get_mut(&conn_id) {
                uc.last_seen = Instant::now();
            }
        }

        let msg = TunnelMessage {
            msg_type: "data".into(),
            conn_id,
            target: self.target.clone(),
            payload: Some(payload.to_vec()),
            // UDP tunnel traffic is unordered/unreliable already; sequencing
            // is only wired up for the TCP tunnel path (see
            // `TunnelMessage::seq`).
            seq: None,
        };
        if let Err(e) = self.send_to(&peer_id, &msg).await {
            error!("Failed to send UDP data: {}", e);
        } else {
            metrics.record_bytes_in(payload.len());
        }
    }

    async fn wait_for_tunnel_ready(&self, timeout: Duration) -> Result<String> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(peer_id) = self.rtc_manager.select_server_peer_for(&self.target).await {
                return Ok(peer_id);
            }
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!("timeout");
            }
            tokio::time::sleep(RETRY_INTERVAL).await;
        }
    }

    pub async fn cleanup_loop(&self) {
        let mut interval = tokio::time::interval(CLEANUP_INTERVAL);
        let mut shutdown = self.runtime.subscribe();
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let mut removed_metrics = Vec::new();
                    let mut conns = self.conns.write().await;
                    conns.retain(|id, uc| {
                        if uc.last_seen.elapsed() > UDP_TIMEOUT {
                            debug!("Cleaned up UDP session: {}", id);
                            removed_metrics.push(uc.metrics.clone());
                            false
                        } else {
                            true
                        }
                    });
                    drop(conns);
                    for metrics in removed_metrics {
                        metrics.record_conn_close();
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
