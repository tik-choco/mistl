use super::*;

fn serve_spec(target: &str) -> ForwardSpec {
    ForwardSpec {
        direction: Direction::Serve,
        proto: Proto::Tcp,
        addr: "127.0.0.1:80".to_string(),
        listen_port: -1,
        target: target.to_string(),
    }
}

fn connect_spec(target: &str, listen_port: i32) -> ForwardSpec {
    ForwardSpec {
        direction: Direction::Connect,
        proto: Proto::Udp,
        addr: String::new(),
        listen_port,
        target: target.to_string(),
    }
}

#[tokio::test]
async fn add_forward_registers_status() {
    let controller = ForwardController::new_inert();
    let key = controller.add_forward(serve_spec("tcp:80")).await.unwrap();

    assert_eq!(key, "tcp:80");
    let statuses = controller.list_forwards().await;
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].key, "tcp:80");
    assert_eq!(statuses[0].state, ForwardState::Listening);
    assert_eq!(statuses[0].active_conns, 0);
}

#[tokio::test]
async fn list_forwards_reflects_runtime_metrics() {
    let controller = ForwardController::new_inert();
    controller.add_forward(serve_spec("tcp:80")).await.unwrap();

    {
        let forwards = controller.forwards.read().await;
        let runtime = &forwards.get("tcp:80").unwrap().runtime;
        runtime.record_conn_open();
        runtime.record_bytes_in(128);
        runtime.record_bytes_out(64);
    }

    let status = controller.list_forwards().await.into_iter().next().unwrap();
    assert_eq!(status.active_conns, 1);
    assert_eq!(status.bytes_in, 128);
    assert_eq!(status.bytes_out, 64);
}

#[tokio::test]
async fn add_forward_rejects_duplicate_key() {
    let controller = ForwardController::new_inert();

    controller.add_forward(serve_spec("tcp:80")).await.unwrap();
    let err = controller
        .add_forward(connect_spec("tcp:80", 18080))
        .await
        .unwrap_err();

    assert!(err.to_string().contains("forward already exists"));
    assert_eq!(controller.list_forwards().await.len(), 1);
}

#[tokio::test]
async fn remove_forward_deletes_registered_status() {
    let controller = ForwardController::new_inert();
    controller.add_forward(serve_spec("tcp:80")).await.unwrap();

    controller.remove_forward("tcp:80").await.unwrap();

    assert!(controller.list_forwards().await.is_empty());
}

#[tokio::test]
async fn remove_forward_rejects_unknown_key() {
    let controller = ForwardController::new_inert();

    let err = controller.remove_forward("tcp:80").await.unwrap_err();

    assert!(err.to_string().contains("forward not found"));
}

#[tokio::test]
async fn list_forwards_is_sorted_by_key() {
    let controller = ForwardController::new_inert();

    controller
        .add_forward(serve_spec("udp:9000"))
        .await
        .unwrap();
    controller.add_forward(serve_spec("tcp:80")).await.unwrap();

    let keys = controller
        .list_forwards()
        .await
        .into_iter()
        .map(|s| s.key)
        .collect::<Vec<_>>();
    assert_eq!(keys, vec!["tcp:80", "udp:9000"]);
}

#[test]
fn proto_parses_known_names() {
    assert_eq!(Proto::from_name("tcp").unwrap(), Proto::Tcp);
    assert_eq!(Proto::from_name("udp").unwrap(), Proto::Udp);
    assert!(Proto::from_name("icmp").is_err());
    assert_eq!(Proto::Tcp.as_str(), "tcp");
}
