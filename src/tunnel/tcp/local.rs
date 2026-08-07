use std::sync::Arc;

use anyhow::Result;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tracing::{debug, error};

use crate::tunnel::rtc::TunnelMessage;

use super::{
    MSG_TYPE_CONNECT, MSG_TYPE_DATA, RETRY_INTERVAL, TCP_BUFFER_SIZE, TUNNEL_READY_TIMEOUT,
    TcpManager, log_tcp_io_error,
};

impl TcpManager {
    pub async fn handle_local_connection(self: &Arc<Self>, stream: TcpStream) {
        let conn_id = uuid::Uuid::new_v4().to_string();

        let peer_id = match self.wait_for_tunnel_ready(TUNNEL_READY_TIMEOUT).await {
            Ok(id) => id,
            Err(e) => {
                error!("tunnel not ready: {}", e);
                return;
            }
        };

        let (read_half, write_half) = stream.into_split();
        self.track_conn(&conn_id, write_half, &peer_id, true).await;

        let connect_msg = TunnelMessage {
            msg_type: MSG_TYPE_CONNECT.into(),
            conn_id: conn_id.clone(),
            target: self.target.clone(),
            payload: None,
            seq: None,
        };
        let mut shutdown = self.runtime.subscribe();
        if let Err(e) = self
            .send_to_with_retry(&peer_id, &connect_msg, &mut shutdown)
            .await
        {
            error!("failed to send tunnel connect after retries: {}", e);
            // Best-effort notify: the retry budget is exhausted, so the
            // remote is unlikely to be reachable anyway, but this avoids
            // leaving a dangling backend socket if it is.
            self.close_conn(&conn_id, true).await;
            return;
        }

        let mgr = self.clone();
        let cid = conn_id.clone();
        let pid = peer_id.clone();
        tokio::spawn(async move { mgr.forward_tcp_to_dc(cid, pid, read_half).await });
    }

    async fn wait_for_tunnel_ready(&self, timeout: std::time::Duration) -> Result<String> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(peer_id) = self.rtc_manager.select_server_peer_for(&self.target).await {
                debug!("Selected server peer: {}", peer_id);
                return Ok(peer_id);
            }
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!("server peer not connected");
            }
            tokio::time::sleep(RETRY_INTERVAL).await;
        }
    }

    pub async fn forward_tcp_to_dc(
        self: Arc<Self>,
        conn_id: String,
        peer_id: String,
        mut read_half: tokio::net::tcp::OwnedReadHalf,
    ) {
        let mut buf = vec![0u8; TCP_BUFFER_SIZE];
        let mut shutdown = self.runtime.subscribe();
        let metrics = self.runtime.peer(&peer_id);
        // Per-conn, monotonically increasing sequence number for outbound
        // `data` messages (see `TunnelMessage::seq`). Starts at 1; a retried
        // send (`send_to_with_retry`) reuses the same `msg` (and thus the
        // same seq) rather than bumping it, so the receiver can dedupe a
        // redelivered payload instead of treating it as new data.
        let mut next_seq: u64 = 1;
        loop {
            tokio::select! {
                read = read_half.read(&mut buf) => {
                    match read {
                        Ok(0) => {
                            self.close_conn(&conn_id, true).await;
                            return;
                        }
                        Ok(n) => {
                            let seq = next_seq;
                            next_seq += 1;
                            let msg = TunnelMessage {
                                msg_type: MSG_TYPE_DATA.into(),
                                conn_id: conn_id.clone(),
                                target: self.target.clone(),
                                payload: Some(buf[..n].to_vec()),
                                seq: Some(seq),
                            };
                            // Sequential await: the next chunk isn't read
                            // until this send (including any retries) has
                            // resolved, so byte order is preserved.
                            if let Err(e) = self
                                .send_to_with_retry(&peer_id, &msg, &mut shutdown)
                                .await
                            {
                                error!("failed to send tunnel data after retries: {}", e);
                                // Retry budget exhausted: give up on this
                                // conn, but still notify the remote
                                // (best-effort) so it doesn't dangle a
                                // backend socket waiting for data that will
                                // never come.
                                self.close_conn(&conn_id, true).await;
                                return;
                            }
                            metrics.record_bytes_in(n);
                        }
                        Err(e) => {
                            log_tcp_io_error("tcp read error", &e);
                            self.close_conn(&conn_id, true).await;
                            return;
                        }
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        self.close_conn(&conn_id, false).await;
                        return;
                    }
                }
            }
        }
    }
}
