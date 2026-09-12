//! Tests for the `pending`/`approve`/`deny` shell commands. Ported
//! unchanged (besides the import path) from
//! `p2p/src/control_shell/tests/pending_commands.rs`.

use crate::tunnel::auth::{AuthDecision, AuthRequest, PendingAuthorizations};
use crate::tunnel::control_shell::execute_line_with_context;
use crate::tunnel::controller::ForwardController;

#[tokio::test]
async fn pending_lists_auth_requests() {
    let controller = ForwardController::new_inert();
    let pending = PendingAuthorizations::new();
    let request = AuthRequest {
        peer_id: "peer-a".to_string(),
        forward_key: "tcp:80".to_string(),
        target_addr: "127.0.0.1:80".to_string(),
        proto: "tcp".to_string(),
    };
    let receiver = pending.enqueue_for_test(request).await;

    let outcome = execute_line_with_context(&controller, None, None, Some(&pending), "pending")
        .await
        .unwrap();

    assert!(outcome.output.contains("id peer target proto addr"));
    assert!(outcome.output.contains("1 peer-a tcp:80 tcp 127.0.0.1:80"));
    drop(receiver);
}

#[tokio::test]
async fn approve_resolves_pending_auth_request() {
    let controller = ForwardController::new_inert();
    let pending = PendingAuthorizations::new();
    let receiver = pending
        .enqueue_for_test(AuthRequest {
            peer_id: "peer-a".to_string(),
            forward_key: "tcp:80".to_string(),
            target_addr: "127.0.0.1:80".to_string(),
            proto: "tcp".to_string(),
        })
        .await;

    let outcome = execute_line_with_context(&controller, None, None, Some(&pending), "approve 1")
        .await
        .unwrap();

    assert_eq!(outcome.output, "approved pending 1\n");
    assert_eq!(receiver.await.unwrap(), AuthDecision::Allow);
}

#[tokio::test]
async fn deny_always_resolves_pending_auth_request() {
    let controller = ForwardController::new_inert();
    let pending = PendingAuthorizations::new();
    let receiver = pending
        .enqueue_for_test(AuthRequest {
            peer_id: "peer-a".to_string(),
            forward_key: "tcp:80".to_string(),
            target_addr: "127.0.0.1:80".to_string(),
            proto: "tcp".to_string(),
        })
        .await;

    let outcome =
        execute_line_with_context(&controller, None, None, Some(&pending), "deny 1 always")
            .await
            .unwrap();

    assert_eq!(outcome.output, "denied pending 1\n");
    assert_eq!(receiver.await.unwrap(), AuthDecision::DenyAlways);
}
