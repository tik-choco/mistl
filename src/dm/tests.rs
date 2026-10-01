use super::*;
use crate::identity::for_test;
use crate::net::peer_auth::now_ms;

fn open(_: &str) -> bool {
    true
}

/// A recipient service plus a handle to its identity.
fn recipient() -> (Arc<Identity>, Service) {
    let id = Arc::new(for_test());
    (id.clone(), Service::new(id, None, History::default()))
}

fn msg(from: &Identity, to: &Identity, ts: u64, text: &str) -> (String, Vec<u8>) {
    let id = wire::fresh_id();
    let bytes = wire::build_message(from, to.did(), &id, ts, Some(text), None).unwrap();
    (id, bytes)
}

fn run(svc: &Service, from: &Identity, bytes: &[u8], now: u64) -> Outcome {
    svc.process_inbound(&from.node_id(), bytes, now, Instant::now(), &open)
}

fn sent(id: &str, now: u64) -> StoredMsg {
    StoredMsg {
        id: id.to_string(),
        mine: true,
        ts_ms: now,
        text: Some("hi".into()),
        file: None,
        status: Status::Sent,
    }
}

#[test]
fn accepted_message_is_stored_unread_and_replays_are_dropped() {
    let a = for_test();
    let (b, svc) = recipient();
    let now = now_ms();
    let (id, bytes) = msg(&a, &b, now, "hello");
    assert_eq!(
        run(&svc, &a, &bytes, now),
        Outcome::Accepted {
            from_did: a.did().to_string(),
            id: id.clone()
        }
    );
    let list = svc.history().list();
    assert_eq!(list[0]["did"], a.did());
    assert_eq!(list[0]["unread"], 1);
    assert_eq!(list[0]["last"]["text"], "hello");
    assert_eq!(list[0]["last"]["status"], "received");
    assert_eq!(list[0]["last"]["mine"], false);
    // Replay of the very same bytes: dropped, no second copy.
    assert_eq!(run(&svc, &a, &bytes, now), Outcome::Duplicate);
    assert_eq!(svc.history().message_count(a.did()), 1);
}

#[test]
fn replay_after_restart_is_caught_by_history() {
    // The in-memory replay cache is lost on restart; the persisted
    // conversation still refuses the same id.
    let a = for_test();
    let (b, svc) = recipient();
    let now = now_ms();
    let (_, bytes) = msg(&a, &b, now, "x");
    assert!(matches!(
        run(&svc, &a, &bytes, now),
        Outcome::Accepted { .. }
    ));
    svc.inbox().dedupe = Dedupe::default();
    assert_eq!(run(&svc, &a, &bytes, now), Outcome::Duplicate);
    assert_eq!(svc.history().message_count(a.did()), 1);
}

#[test]
fn forged_sender_wrong_recipient_and_stale_messages_leave_no_trace() {
    let (a, victim, c) = (for_test(), for_test(), for_test());
    let (b, svc) = recipient();
    let now = now_ms();
    let (_, good) = msg(&a, &b, now, "hi");
    // Delivered by a different transport node than the signer's.
    assert_eq!(
        svc.process_inbound(&victim.node_id(), &good, now, Instant::now(), &open),
        Outcome::Rejected(Reject::NodeMismatch)
    );
    // Addressed to someone else (intercepted / misdelivered).
    let (_, other) = msg(&a, &c, now, "hi");
    assert_eq!(
        run(&svc, &a, &other, now),
        Outcome::Rejected(Reject::WrongRecipient)
    );
    // Stale timestamp.
    let (_, old) = msg(&a, &b, now - wire::MAX_SKEW_MS - 1, "hi");
    assert_eq!(run(&svc, &a, &old, now), Outcome::Rejected(Reject::Stale));
    // Victim's DID and node id, attacker's signature.
    let mut forged = serde_json::from_slice::<Value>(&good).unwrap();
    forged["fromId"] = json!(victim.did());
    forged["node"] = json!(victim.node_id());
    let forged = serde_json::to_vec(&forged).unwrap();
    assert_eq!(
        svc.process_inbound(&victim.node_id(), &forged, now, Instant::now(), &open),
        Outcome::Rejected(Reject::BadSignature)
    );
    // Garbage and foreign traffic is ignored outright.
    assert_eq!(run(&svc, &a, b"\x00\x01\x02", now), Outcome::Ignored);
    assert_eq!(run(&svc, &a, &vec![b'a'; 20_000], now), Outcome::Ignored);
    assert_eq!(svc.history().conversation_count(), 0);
    assert_eq!(svc.inbox().dedupe.len(), 0);
}

#[test]
fn non_admitted_senders_are_dropped_before_any_state_changes() {
    let a = for_test();
    let (b, svc) = recipient();
    let now = now_ms();
    let (_, bytes) = msg(&a, &b, now, "hi");
    let deny = |_: &str| false;
    assert_eq!(
        svc.process_inbound(&a.node_id(), &bytes, now, Instant::now(), &deny),
        Outcome::NotAdmitted
    );
    assert_eq!(svc.history().conversation_count(), 0);
    assert_eq!(svc.inbox().dedupe.len(), 0);
    // Once admitted, the very same bytes are accepted (nothing was burned).
    assert!(matches!(
        run(&svc, &a, &bytes, now),
        Outcome::Accepted { .. }
    ));
}

#[test]
fn per_sender_rate_limit_applies() {
    let a = for_test();
    let (b, svc) = recipient();
    let now = now_ms();
    let t = Instant::now();
    for i in 0..limits::PER_SENDER_PER_WINDOW {
        let (_, bytes) = msg(&a, &b, now, &format!("m{i}"));
        assert!(matches!(
            svc.process_inbound(&a.node_id(), &bytes, now, t, &open),
            Outcome::Accepted { .. }
        ));
    }
    let (_, extra) = msg(&a, &b, now, "one too many");
    assert_eq!(
        svc.process_inbound(&a.node_id(), &extra, now, t, &open),
        Outcome::RateLimited
    );
    assert_eq!(
        svc.history().message_count(a.did()),
        limits::PER_SENDER_PER_WINDOW as usize
    );
    // Another sender is unaffected.
    let other = for_test();
    let (_, bytes) = msg(&other, &b, now, "hi");
    assert!(matches!(
        svc.process_inbound(&other.node_id(), &bytes, now, t, &open),
        Outcome::Accepted { .. }
    ));
}

#[test]
fn impersonation_floods_burn_nothing_of_the_victims() {
    let (a, mallory) = (for_test(), for_test());
    let (b, svc) = recipient();
    let now = now_ms();
    let t = Instant::now();
    // Mallory replays a's signed message from her own transport node: it
    // dies on the node binding, before any rate-limit token or cache slot.
    let (_, signed_by_a) = msg(&a, &b, now, "hi");
    for _ in 0..100 {
        assert_eq!(
            svc.process_inbound(&mallory.node_id(), &signed_by_a, now, t, &open),
            Outcome::Rejected(Reject::NodeMismatch)
        );
    }
    // a's own delivery still works.
    assert!(matches!(
        svc.process_inbound(&a.node_id(), &signed_by_a, now, t, &open),
        Outcome::Accepted { .. }
    ));
}

#[test]
fn ack_marks_only_the_message_sent_to_that_peer() {
    let (b, c) = (for_test(), for_test());
    let a = Arc::new(for_test());
    let svc_a = Service::new(a.clone(), None, History::default());
    let now = now_ms();
    // a sent a message to b.
    let sent_id = wire::fresh_id();
    svc_a
        .history()
        .add(b.did(), sent(&sent_id, now), now, false)
        .unwrap();
    // c (a third party) acks b's message id: no effect on b's thread.
    let forged = wire::build_ack(&c, a.did(), &sent_id, now);
    assert_eq!(
        svc_a.process_inbound(&c.node_id(), &forged, now, Instant::now(), &open),
        Outcome::AckApplied(false)
    );
    assert_eq!(
        svc_a.history().message(b.did(), &sent_id).unwrap().status,
        Status::Sent
    );
    // b's real ack upgrades it; a replayed ack changes nothing.
    let ack = wire::build_ack(&b, a.did(), &sent_id, now);
    assert_eq!(
        svc_a.process_inbound(&b.node_id(), &ack, now, Instant::now(), &open),
        Outcome::AckApplied(true)
    );
    assert_eq!(
        svc_a.process_inbound(&b.node_id(), &ack, now, Instant::now(), &open),
        Outcome::AckApplied(false)
    );
    assert_eq!(
        svc_a.history().message(b.did(), &sent_id).unwrap().status,
        Status::Delivered
    );
    // An ack addressed to someone else is refused.
    let wrong = wire::build_ack(&b, c.did(), &sent_id, now);
    assert_eq!(
        svc_a.process_inbound(&b.node_id(), &wrong, now, Instant::now(), &open),
        Outcome::Rejected(Reject::WrongRecipient)
    );
}

#[test]
fn stranger_floods_are_refused_once_all_slots_are_real_conversations() {
    let (b, svc) = recipient();
    let now = now_ms();
    for n in 0..history::MAX_CONVERSATIONS {
        let peer = for_test();
        svc.history()
            .add(
                peer.did(),
                sent(&wire::fresh_id(), now),
                now + n as u64,
                false,
            )
            .unwrap();
    }
    let stranger = for_test();
    let (_, bytes) = msg(&stranger, &b, now, "spam");
    assert_eq!(run(&svc, &stranger, &bytes, now), Outcome::HistoryFull);
    assert_eq!(
        svc.history().conversation_count(),
        history::MAX_CONVERSATIONS
    );
}

#[test]
fn file_messages_hide_the_key_from_the_ui_but_keep_it_for_download() {
    let a = for_test();
    let (b, svc) = recipient();
    let now = now_ms();
    let file = FileMeta {
        cid: "bafyexample".into(),
        name: "a.txt".into(),
        size: 4,
        mime: "text/plain".into(),
        key: "TOPSECRETKEY".into(),
    };
    let id = wire::fresh_id();
    let bytes = wire::build_message(&a, b.did(), &id, now, None, Some(&file)).unwrap();
    assert!(matches!(
        run(&svc, &a, &bytes, now),
        Outcome::Accepted { .. }
    ));
    let ui = serde_json::to_string(&svc.history().recent(a.did(), 10)).unwrap();
    assert!(!ui.contains("TOPSECRETKEY"));
    assert!(ui.contains("a.txt"));
    let kept = svc
        .history()
        .message(a.did(), &id)
        .unwrap()
        .file
        .clone()
        .unwrap();
    assert_eq!(kept.key, "TOPSECRETKEY");
}

#[test]
fn safe_file_name_confines_hostile_names_to_one_component() {
    for hostile in [
        "../../etc/passwd",
        "..\\..\\Windows\\system32\\x.dll",
        "/abs/path",
        "C:\\boot.ini",
        "a/b/c.txt",
        "name:stream",
        "  ..  ",
        "",
        "...",
        "CON",
        "nul.txt",
        "com1.log",
        "tab\tname\n.txt",
    ] {
        let safe = safe_file_name(hostile);
        assert!(
            !safe.contains('/') && !safe.contains('\\'),
            "{hostile:?} -> {safe:?}"
        );
        assert!(
            !safe.contains(':') && !safe.starts_with('.'),
            "{hostile:?} -> {safe:?}"
        );
        assert!(
            !safe.chars().any(char::is_control),
            "{hostile:?} -> {safe:?}"
        );
        assert!(!safe.is_empty());
        let p = std::path::Path::new(&safe);
        assert_eq!(p.components().count(), 1, "{hostile:?} -> {safe:?}");
        assert!(matches!(p.components().next(), Some(Component::Normal(_))));
    }
    assert_eq!(safe_file_name("report.pdf"), "report.pdf");
    assert_eq!(safe_file_name("CON"), "_CON");
    assert_eq!(safe_file_name("日本語.txt"), "日本語.txt");
    assert!(safe_file_name(&"x".repeat(500)).chars().count() <= 120);
}

#[test]
fn sandbox_file_rejects_escapes() {
    let root = Path::new("sandbox-root");
    assert!(sandbox_file(root, "ok/file.txt").is_ok());
    assert!(sandbox_file(root, "./file.txt").is_ok());
    for bad in ["", "  ", ".", "../x", "a/../../x", "/etc/passwd"] {
        assert!(sandbox_file(root, bad).is_err(), "{bad:?}");
    }
    #[cfg(windows)]
    for bad in ["C:\\x", "\\\\server\\share\\x", "..\\x"] {
        assert!(sandbox_file(root, bad).is_err(), "{bad:?}");
    }
    let joined = sandbox_file(root, "dm/abc/n.txt").unwrap();
    assert!(joined.starts_with(root));
}

#[test]
fn mime_guess_is_printable_ascii_within_limits() {
    for name in ["a.PNG", "b", "c.tar.gz", "d.mp4"] {
        let m = guess_mime(name);
        assert!(m.len() <= wire::MAX_MIME_CHARS && m.is_ascii());
    }
    assert_eq!(guess_mime("a.PNG"), "image/png");
}
