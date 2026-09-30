use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::UdpSocket;
use tokio::sync::RwLock;

use crate::tunnel::auth::allow_all;
use crate::tunnel::forward_runtime::ForwardRuntime;
use crate::tunnel::rtc::{RTCManager, TunnelMessage};

use super::*;

fn test_manager(remote_addr: &str) -> Arc<UdpManager> {
    Arc::new(UdpManager {
        rtc_manager: RTCManager::for_test("self-node"),
        conns: Arc::new(RwLock::new(HashMap::new())),
        remote_addr: remote_addr.to_string(),
        pending: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        local_socket: Arc::new(RwLock::new(None)),
        target: "udp:53".to_string(),
        runtime: ForwardRuntime::new(),
        authorizer: allow_all(),
    })
}

fn data(conn_id: &str, payload: &[u8]) -> TunnelMessage {
    TunnelMessage {
        msg_type: "data".into(),
        conn_id: conn_id.to_string(),
        target: "udp:53".to_string(),
        payload: Some(payload.to_vec()),
        seq: None,
    }
}

/// Connect-side manager with a bound local socket and one session whose
/// client is `client`, owned by `owner`.
async fn connect_side_with_session(
    conn_id: &str,
    owner: &str,
    client: std::net::SocketAddr,
) -> Arc<UdpManager> {
    let mgr = test_manager("");
    let local = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    *mgr.local_socket.write().await = Some(local);
    mgr.conns.write().await.insert(
        conn_id.to_string(),
        UdpConn {
            target_conn: None,
            last_seen: Instant::now(),
            peer_id: owner.to_string(),
            metrics: mgr.runtime.peer(owner),
            client_addr: Some(client),
        },
    );
    mgr
}

/// T-02: the conn id is never parsed as a destination address. Remote data
/// naming an arbitrary `ip:port` as its conn id must not be relayed there.
#[tokio::test]
async fn remote_conn_id_is_never_used_as_a_send_target() {
    let victim = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let victim_addr = victim.local_addr().unwrap();
    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mgr = connect_side_with_session("legit", "peer-a", client.local_addr().unwrap()).await;

    // Unknown conn id that happens to be a socket address: dropped.
    mgr.handle_data("peer-a", &data(&victim_addr.to_string(), b"payload"))
        .await;
    let mut buf = [0u8; 16];
    let res = tokio::time::timeout(Duration::from_millis(100), victim.recv_from(&mut buf)).await;
    assert!(res.is_err(), "conn_id must not be treated as an address");
}

/// A session's reply goes to its recorded client, and only when sent by the
/// owning peer.
#[tokio::test]
async fn replies_go_to_recorded_client_from_owner_only() {
    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mgr = connect_side_with_session("legit", "peer-a", client.local_addr().unwrap()).await;

    mgr.handle_data("peer-evil", &data("legit", b"spoof")).await;
    let mut buf = [0u8; 16];
    let res = tokio::time::timeout(Duration::from_millis(100), client.recv_from(&mut buf)).await;
    assert!(
        res.is_err(),
        "non-owner peer must not inject into a session"
    );

    mgr.handle_data("peer-a", &data("legit", b"reply")).await;
    let (n, _) = tokio::time::timeout(Duration::from_secs(1), client.recv_from(&mut buf))
        .await
        .expect("owner reply should arrive")
        .unwrap();
    assert_eq!(&buf[..n], b"reply");
}

/// Serve-side sessions are capped per peer (established + awaiting auth).
#[tokio::test]
async fn serve_side_sessions_are_capped_per_peer() {
    let backend = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mgr = test_manager(&backend.local_addr().unwrap().to_string());

    for i in 0..MAX_UDP_SESSIONS_PER_PEER {
        mgr.conns.write().await.insert(
            format!("c{i}"),
            UdpConn {
                target_conn: None,
                last_seen: Instant::now(),
                peer_id: "peer-a".to_string(),
                metrics: mgr.runtime.peer("peer-a"),
                client_addr: None,
            },
        );
    }
    mgr.handle_data("peer-a", &data("overflow", b"x")).await;
    assert!(mgr.pending.lock().await.is_empty());
    assert!(!mgr.conns.read().await.contains_key("overflow"));

    // Another peer can still open a session (pending until authorized).
    mgr.handle_data("peer-b", &data("fresh", b"hello")).await;
    let mut buf = [0u8; 16];
    let (n, _) = tokio::time::timeout(Duration::from_secs(2), backend.recv_from(&mut buf))
        .await
        .expect("backend should receive peer-b's queued packet")
        .unwrap();
    assert_eq!(&buf[..n], b"hello");
}
