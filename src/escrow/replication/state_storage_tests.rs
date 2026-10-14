use super::tests::{delete, fixture, put};
use super::*;

#[test]
fn snapshot_preserves_writer_tombstone_and_continuation_without_resurrection() {
    let old = fixture(1, None, b"old");
    let mut state = State::default();
    state.apply(&put("old", &old), "", 8);
    state.apply(&delete(&old), "", 8);
    let mut state = State::decode(&state.encode().unwrap(), 8).unwrap();
    assert_eq!(state.apply(&put("late", &old), "", 8)["code"], "deleted");
    let fresh = fixture(2, Some(old.hash()), b"fresh");
    assert_eq!(
        state.apply(&put("fresh", &fresh), "", 8)["type"],
        "escrow_ack"
    );
    assert_eq!(state.head(&old.slot), Some(fresh));
    assert_eq!(state.apply(&delete(&old), "", 8)["code"], "conflict");
    assert!(State::decode(&state.encode().unwrap(), 0).is_err());
    let mut wire = serde_json::from_slice::<Value>(&state.encode().unwrap()).unwrap();
    wire["heads"][&old.slot]["writer"] = json!("forged");
    assert!(State::decode(&serde_json::to_vec(&wire).unwrap(), 8).is_err());
}

#[test]
fn batched_bootstrap_is_atomic_visible_only_after_sealed_and_survives_snapshot() {
    let head = fixture(7, Some("ab".repeat(32)), b"legacy");
    let mut state = State::default();
    let begin = Command::BootstrapBegin {
        ticket: "begin".into(),
    };
    assert_eq!(state.apply(&begin, "", 8)["type"], "escrow_bootstrapped");
    let batch = Command::Bootstrap {
        ticket: "batch".into(),
        heads: vec![head.clone()],
    };
    assert_eq!(state.apply(&batch, "", 8)["type"], "escrow_bootstrapped");
    assert_eq!(
        state.apply(&batch, "retry", 8)["type"],
        "escrow_bootstrapped"
    );
    state = State::decode(&state.encode().unwrap(), 8).unwrap();
    assert!(state.migrating());
    let read = Command::ReadBarrier {
        ticket: "read".into(),
    };
    assert_eq!(state.apply(&read, "", 8)["code"], "migrating");
    assert_eq!(
        state.apply(&put("write", &head), "", 8)["code"],
        "migrating"
    );
    let finish = Command::BootstrapComplete {
        ticket: "finish".into(),
    };
    assert_eq!(state.apply(&finish, "", 8)["type"], "escrow_bootstrapped");
    assert!(!state.migrating());
    assert_eq!(state.head(&head.slot), Some(head));
    assert_eq!(state.apply(&batch, "late", 8)["code"], "conflict");
    assert_eq!(state.apply(&read, "", 8)["type"], "escrow_read_ready");
}

#[test]
fn metadata_budget_rejects_growth_and_deletion_releases_history() {
    let old = fixture(1, None, b"old");
    let next = fixture(2, Some(old.hash()), b"next");
    let mut state = State::default();
    state.apply(&put("old", &old), "", 8);
    state.apply(&put("next", &next), "", 8);
    assert_eq!(state.metadata_bytes, head_bytes(&old) + head_bytes(&next));
    let bytes = state.encode().unwrap();
    let restored = State::decode(&bytes, 8).unwrap();
    assert_eq!(restored.metadata_bytes, state.metadata_bytes);
    state.apply(&delete(&next), "", 8);
    assert_eq!(state.metadata_bytes, head_bytes(&next));
    state.metadata_bytes = SNAPSHOT_BYTES - 128;
    let fresh = fixture(3, Some(next.hash()), b"fresh ciphertext");
    assert_eq!(state.apply(&put("large", &fresh), "", 8)["code"], "quota");
    assert_eq!(state.deleted.get(&old.slot), Some(&next));
}
