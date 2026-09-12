//! Tests for the control-shell command parser/executor. Ported unchanged
//! (besides the import path) from `p2p/src/control_shell/tests.rs`.

use super::*;

use crate::tunnel::auth::{
    AuthAuditLog, AuthDecision, AuthEventSource, AuthRequest, TrustDecision, TrustKey, TrustStore,
};
use crate::tunnel::controller::Proto;

mod pending_commands;

fn temp_store_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "mistl-tunnel-shell-trust-{}-{}.json",
        name,
        uuid::Uuid::new_v4()
    ))
}

#[tokio::test]
async fn add_serve_registers_forward() {
    let controller = ForwardController::new_inert();

    let outcome = execute_line(&controller, "add serve tcp://127.0.0.1:80")
        .await
        .unwrap();

    assert_eq!(outcome.output, "added tcp:80\n");
    assert!(!outcome.should_quit);
    let statuses = controller.list_forwards().await;
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].spec.direction, Direction::Serve);
    assert_eq!(statuses[0].spec.addr, "127.0.0.1:80");
}

#[tokio::test]
async fn add_connect_registers_listen_and_remote_target() {
    let controller = ForwardController::new_inert();

    execute_line(&controller, "add connect udp://19000:9000")
        .await
        .unwrap();

    let statuses = controller.list_forwards().await;
    assert_eq!(statuses[0].spec.direction, Direction::Connect);
    assert_eq!(statuses[0].spec.proto, Proto::Udp);
    assert_eq!(statuses[0].spec.listen_port, 19000);
    assert_eq!(statuses[0].spec.target, "udp:9000");
}

#[tokio::test]
async fn remove_deletes_forward() {
    let controller = ForwardController::new_inert();
    execute_line(&controller, "add serve :80").await.unwrap();

    let outcome = execute_line(&controller, "remove tcp:80").await.unwrap();

    assert_eq!(outcome.output, "removed tcp:80\n");
    assert!(controller.list_forwards().await.is_empty());
}

#[tokio::test]
async fn list_renders_registered_forwards() {
    let controller = ForwardController::new_inert();
    execute_line(&controller, "add serve :80").await.unwrap();

    let outcome = execute_line(&controller, "list").await.unwrap();

    assert!(outcome.output.contains("key direction proto endpoint"));
    assert!(outcome.output.contains("tcp:80 serve tcp :80 listening"));
}

#[tokio::test]
async fn trust_allow_persists_entry() {
    let controller = ForwardController::new_inert();
    let path = temp_store_path("allow");
    let store = TrustStore::load(&path).await.unwrap();

    let outcome = execute_line_with_trust(&controller, Some(&store), "trust allow peer-a tcp:80")
        .await
        .unwrap();

    assert_eq!(outcome.output, "trusted peer-a tcp:80 allow\n");
    assert_eq!(
        store
            .get(&TrustKey {
                peer_id: "peer-a".to_string(),
                forward_key: "tcp:80".to_string(),
            })
            .await,
        Some(TrustDecision::Allow)
    );
    let _ = tokio::fs::remove_file(path).await;
}

#[tokio::test]
async fn trust_deny_and_list_render_entries() {
    let controller = ForwardController::new_inert();
    let path = temp_store_path("list");
    let store = TrustStore::load(&path).await.unwrap();
    execute_line_with_trust(&controller, Some(&store), "trust deny peer-a udp:53")
        .await
        .unwrap();

    let outcome = execute_line_with_trust(&controller, Some(&store), "trust list")
        .await
        .unwrap();

    assert!(outcome.output.contains("peer target decision"));
    assert!(outcome.output.contains("peer-a udp:53 deny"));
    let _ = tokio::fs::remove_file(path).await;
}

#[tokio::test]
async fn trust_remove_deletes_entry() {
    let controller = ForwardController::new_inert();
    let path = temp_store_path("remove");
    let store = TrustStore::load(&path).await.unwrap();
    execute_line_with_trust(&controller, Some(&store), "trust allow peer-a tcp:80")
        .await
        .unwrap();

    let outcome = execute_line_with_trust(&controller, Some(&store), "trust remove peer-a tcp:80")
        .await
        .unwrap();

    assert_eq!(outcome.output, "removed trust peer-a tcp:80\n");
    assert!(store.list().await.is_empty());
    let _ = tokio::fs::remove_file(path).await;
}

#[tokio::test]
async fn events_render_auth_audit_log() {
    let controller = ForwardController::new_inert();
    let audit_log = AuthAuditLog::default();
    audit_log
        .record(
            &AuthRequest {
                peer_id: "peer-a".to_string(),
                forward_key: "tcp:80".to_string(),
                target_addr: "127.0.0.1:80".to_string(),
                proto: "tcp".to_string(),
            },
            AuthDecision::Allow,
            AuthEventSource::Policy,
        )
        .await;

    let outcome = execute_line_with_context(&controller, None, Some(&audit_log), None, "events")
        .await
        .unwrap();

    assert!(outcome.output.contains("seq decision source peer target"));
    assert!(
        outcome
            .output
            .contains("1 allow policy peer-a tcp:80 tcp 127.0.0.1:80")
    );
}

#[tokio::test]
async fn commands_requiring_an_absent_dependency_are_rejected() {
    let controller = ForwardController::new_inert();

    let cases: &[(&str, &str)] = &[
        ("trust list", "trust commands are unavailable"),
        ("events", "events are unavailable"),
        ("pending", "pending auth commands are unavailable"),
    ];

    for (line, expected_message) in cases {
        let err = execute_line(&controller, line).await.unwrap_err();
        assert!(
            err.to_string().contains(expected_message),
            "line {line:?}: expected error containing {expected_message:?}, got {err}"
        );
    }
}

#[tokio::test]
async fn quit_marks_shell_complete() {
    let controller = ForwardController::new_inert();

    let outcome = execute_line(&controller, "quit").await.unwrap();

    assert_eq!(outcome.output, "bye\n");
    assert!(outcome.should_quit);
}

#[tokio::test]
async fn unknown_command_is_rejected() {
    let controller = ForwardController::new_inert();

    let err = execute_line(&controller, "wat").await.unwrap_err();

    assert!(err.to_string().contains("unknown command"));
}
