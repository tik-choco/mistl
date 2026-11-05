use super::super::{AccessMode, Period, Rule};
use super::*;
use capability::Error;

fn now() -> DateTime<Utc> {
    DateTime::from_timestamp_millis(1_793_664_000_000).unwrap()
}

fn signed(
    issuer: &Identity,
    holder: &Identity,
    service: Service,
    actions: &[Action],
    resource: &str,
) -> Value {
    let mut cap = json!({"t":"mistl-cap-v1","id":crate::net::peer_auth::fresh_nonce(),
        "fromId":issuer.did(),"aud":holder.did(),"svc":service,"act":actions,"res":resource,
        "nbf":now().timestamp_millis(),"exp":(now() + Duration::hours(1)).timestamp_millis()});
    crate::wiresign::sign_wire(&mut cap, issuer).unwrap();
    cap
}

fn ai(issuer: &Identity, holder: &Identity) -> Value {
    signed(issuer, holder, Service::Ai, &[Action::Infer], "*")
}

fn verify(
    wire: &Value,
    holder: &Identity,
    verifier: &Identity,
) -> std::result::Result<Capability, Error> {
    capability::verify(
        wire,
        &capability::Context {
            now_ms: now().timestamp_millis(),
            holder: holder.did(),
            local_did: verifier.did(),
            service: Service::Ai,
            action: Action::Infer,
            resource: "*",
            trusted_root: |did: &str| did == verifier.did(),
            is_revoked: |_: &str| false,
        },
    )
}

fn store(identity: &Identity) -> Store {
    Store {
        local_did: Some(identity.did().into()),
        ..Store::default()
    }
}

fn principal(identity: &Identity) -> Principal {
    Principal {
        node: identity.node_id(),
        did: Some(identity.did().into()),
    }
}

fn receive(store: &mut Store, cap: Value, holder: &Identity, verifier: &Identity) {
    store.receive_cap(
        &holder.node_id(),
        Some(holder.did()),
        &json!({"t":PRESENT,"cap":cap}),
        verifier,
        now(),
    );
}

fn child(issuer: &Identity, holder: &Identity, parent: Value) -> Value {
    let mut cap = ai(issuer, holder);
    cap["prf"] = parent;
    crate::wiresign::sign_wire(&mut cap, issuer).unwrap();
    cap
}

#[test]
fn sign_verify_tamper_and_wrong_audience() {
    let issuer = crate::identity::for_test();
    let holder = crate::identity::for_test();
    let other = crate::identity::for_test();
    let mut cap = ai(&issuer, &holder);
    assert!(verify(&cap, &holder, &issuer).is_ok());
    assert!(matches!(
        verify(&cap, &other, &issuer),
        Err(Error::Audience)
    ));
    cap["id"] = json!(crate::net::peer_auth::fresh_nonce());
    assert!(matches!(
        verify(&cap, &holder, &issuer),
        Err(Error::Signature)
    ));
}

#[test]
fn time_window_skew_and_lifetime_are_bounded() {
    let issuer = crate::identity::for_test();
    let holder = crate::identity::for_test();
    let mut cap = ai(&issuer, &holder);
    cap["nbf"] = json!(now().timestamp_millis() + crate::net::peer_auth::MAX_CLOCK_SKEW_MS as i64);
    crate::wiresign::sign_wire(&mut cap, &issuer).unwrap();
    assert!(verify(&cap, &holder, &issuer).is_ok());
    cap["nbf"] = json!(cap["nbf"].as_i64().unwrap() + 1);
    crate::wiresign::sign_wire(&mut cap, &issuer).unwrap();
    assert!(matches!(verify(&cap, &holder, &issuer), Err(Error::Time)));
    cap["nbf"] = json!(now().timestamp_millis() - 1);
    cap["exp"] = json!(now().timestamp_millis());
    crate::wiresign::sign_wire(&mut cap, &issuer).unwrap();
    assert!(matches!(verify(&cap, &holder, &issuer), Err(Error::Time)));
    cap["exp"] = json!(cap["nbf"].as_i64().unwrap() + capability::MAX_LIFETIME_MS + 1);
    assert!(matches!(verify(&cap, &holder, &issuer), Err(Error::Shape)));
}

#[test]
fn to_and_root_trust_are_enforced() {
    let issuer = crate::identity::for_test();
    let holder = crate::identity::for_test();
    let verifier = crate::identity::for_test();
    let mut cap = ai(&issuer, &holder);
    assert!(matches!(
        verify(&cap, &holder, &verifier),
        Err(Error::UntrustedRoot)
    ));
    let mut store = store(&verifier);
    store.policy.ai.issuers.push(issuer.did().into());
    assert!(
        store
            .verify_cap(
                &cap,
                holder.did(),
                Service::Ai,
                Action::Infer,
                "*",
                None,
                now()
            )
            .is_ok()
    );
    cap["to"] = json!([issuer.did()]);
    crate::wiresign::sign_wire(&mut cap, &issuer).unwrap();
    assert!(matches!(
        store.verify_cap(
            &cap,
            holder.did(),
            Service::Ai,
            Action::Infer,
            "*",
            None,
            now()
        ),
        Err(Error::Verifier)
    ));
    cap["to"] = json!([verifier.did()]);
    crate::wiresign::sign_wire(&mut cap, &issuer).unwrap();
    assert!(
        store
            .verify_cap(
                &cap,
                holder.did(),
                Service::Ai,
                Action::Infer,
                "*",
                None,
                now()
            )
            .is_ok()
    );
}

#[test]
fn service_action_and_resource_coverage() {
    let issuer = crate::identity::for_test();
    let holder = crate::identity::for_test();
    let slot = "a".repeat(32);
    let store = store(&issuer);
    for (res, covered) in [
        ("*".to_owned(), true),
        ("room/*".into(), true),
        (format!("room/{slot}"), true),
        ("other/*".into(), false),
    ] {
        let cap = signed(&issuer, &holder, Service::Escrow, &[Action::Read], &res);
        assert_eq!(
            store
                .verify_cap(
                    &cap,
                    holder.did(),
                    Service::Escrow,
                    Action::Read,
                    &format!("room/{slot}"),
                    None,
                    now()
                )
                .is_ok(),
            covered
        );
        assert!(matches!(
            store.verify_cap(
                &cap,
                holder.did(),
                Service::Escrow,
                Action::Write,
                &format!("room/{slot}"),
                None,
                now()
            ),
            Err(Error::Coverage)
        ));
        assert!(matches!(
            verify(&cap, &holder, &issuer),
            Err(Error::Coverage)
        ));
    }
    assert!(!capability::covers("room/*", "room-other/slot"));
    assert!(!capability::covers("room/*", "room/nested/slot"));
    assert!(!capability::covers(&format!("room/{slot}"), "room/*"));
}

#[test]
fn exact_slot_writer_authority_and_raw_writer_conversion() {
    use base64::Engine as _;
    let owner = crate::identity::for_test();
    let holder = crate::identity::for_test();
    let verifier = crate::identity::for_test();
    let writer =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(owner.verifying_key().to_bytes());
    assert_eq!(
        capability::writer_did(&writer).as_deref(),
        Some(owner.did())
    );
    assert!(capability::writer_did("invalid").is_none());
    let mut store = store(&verifier);
    store.policy.escrow.mode = AccessMode::Allowlist;
    let resource = format!("room/{}", "a".repeat(32));
    let cap = signed(&owner, &holder, Service::Escrow, &[Action::Read], &resource);
    receive(&mut store, cap.clone(), &holder, &verifier);
    assert_eq!(store.caps.presented.len(), 1);
    assert!(!store.caps.presented[0].trusted);
    let check = |store: &mut Store, owner| {
        store.authorize_peer_owned(
            Service::Escrow,
            Action::Read,
            principal(&holder),
            Some(&resource),
            owner,
            now(),
        )
    };
    assert!(check(&mut store, None).is_err());
    assert!(check(&mut store, Some(owner.did())).is_ok());
    assert!(store.caps.presented[0].trusted);
    assert!(check(&mut store, Some(verifier.did())).is_err());
    let broad = signed(&owner, &holder, Service::Escrow, &[Action::Read], "room/*");
    assert!(matches!(
        store.verify_cap(
            &broad,
            holder.did(),
            Service::Escrow,
            Action::Read,
            "room/*",
            Some(owner.did()),
            now()
        ),
        Err(Error::UntrustedRoot)
    ));
}

#[test]
fn two_level_chain_and_depth_bound() {
    let root = crate::identity::for_test();
    let alice = crate::identity::for_test();
    let bob = crate::identity::for_test();
    let carol = crate::identity::for_test();
    let dave = crate::identity::for_test();
    let level2 = child(&alice, &bob, ai(&root, &alice));
    let result = verify(&level2, &bob, &root).unwrap();
    assert_eq!(result.chain().len(), 2);
    assert_eq!(result.root().issuer, root.did());
    let level3 = child(&bob, &carol, level2);
    assert_eq!(verify(&level3, &carol, &root).unwrap().chain().len(), 3);
    let level4 = child(&carol, &dave, level3);
    assert!(matches!(verify(&level4, &dave, &root), Err(Error::Depth)));
}

#[test]
fn chain_may_not_widen_action_resource_expiry_audience_or_limits() {
    let root = crate::identity::for_test();
    let alice = crate::identity::for_test();
    let bob = crate::identity::for_test();
    let slot = format!("room/{}", "a".repeat(32));
    let mut parent = signed(&root, &alice, Service::Escrow, &[Action::Read], "room/*");
    parent["to"] = json!([root.did()]);
    parent["lim"] =
        json!({"period":"month","max_requests":5,"max_output_chars":100,"max_stored_bytes":10});
    crate::wiresign::sign_wire(&mut parent, &root).unwrap();
    let mut cap = signed(&alice, &bob, Service::Escrow, &[Action::Read], &slot);
    cap["to"] = json!([root.did()]);
    cap["lim"] =
        json!({"period":"month","max_requests":4,"max_output_chars":90,"max_stored_bytes":9});
    cap["prf"] = parent;
    crate::wiresign::sign_wire(&mut cap, &alice).unwrap();
    let store = store(&root);
    let check = |cap: &Value| {
        store.verify_cap(
            cap,
            bob.did(),
            Service::Escrow,
            Action::Read,
            &slot,
            None,
            now(),
        )
    };
    assert!(check(&cap).is_ok());
    for (key, value) in [
        ("act", json!(["read", "write"])),
        ("res", json!("*")),
        ("exp", json!(cap["exp"].as_i64().unwrap() + 1)),
        ("nbf", json!(cap["nbf"].as_i64().unwrap() - 1)),
        ("to", json!([root.did(), bob.did()])),
        (
            "lim",
            json!({"period":"month","max_requests":6,"max_output_chars":90,"max_stored_bytes":9}),
        ),
        (
            "lim",
            json!({"period":"month","max_requests":4,"max_output_chars":101,"max_stored_bytes":9}),
        ),
        (
            "lim",
            json!({"period":"day","max_requests":4,"max_output_chars":90,"max_stored_bytes":9}),
        ),
        (
            "lim",
            json!({"period":"month","max_requests":4,"max_output_chars":90,"max_stored_bytes":11}),
        ),
        ("lim", json!({})),
    ] {
        let mut bad = cap.clone();
        bad[key] = value;
        crate::wiresign::sign_wire(&mut bad, &alice).unwrap();
        assert!(
            matches!(check(&bad), Err(Error::Attenuation)),
            "{key}: {:?}",
            check(&bad)
        );
    }
    for key in ["to", "lim"] {
        let mut bad = cap.clone();
        bad.as_object_mut().unwrap().remove(key);
        crate::wiresign::sign_wire(&mut bad, &alice).unwrap();
        assert!(matches!(check(&bad), Err(Error::Attenuation)));
    }
}

#[test]
fn revoked_link_anywhere_in_chain_is_rejected() {
    let root = crate::identity::for_test();
    let alice = crate::identity::for_test();
    let bob = crate::identity::for_test();
    let cap = child(&alice, &bob, ai(&root, &alice));
    for id in capability::parse(&cap)
        .unwrap()
        .chain()
        .iter()
        .map(|c| c.id.clone())
    {
        let mut store = store(&root);
        store.caps.revoke(&id, now().timestamp_millis());
        assert!(matches!(
            store.verify_cap(
                &cap,
                bob.did(),
                Service::Ai,
                Action::Infer,
                "*",
                None,
                now()
            ),
            Err(Error::Revoked)
        ));
    }
}

#[test]
fn size_and_shape_checked_before_signature() {
    let root = crate::identity::for_test();
    let holder = crate::identity::for_test();
    let mut cap = ai(&root, &holder);
    cap["signature"] = json!("bad");
    cap["padding"] = json!("x".repeat(capability::MAX_BYTES));
    assert!(matches!(verify(&cap, &holder, &root), Err(Error::Oversize)));
    cap.as_object_mut().unwrap().remove("padding");
    cap["type"] = json!("foreign");
    assert!(matches!(verify(&cap, &holder, &root), Err(Error::Shape)));
    for cap in [Value::Null, json!([]), json!({})] {
        assert!(matches!(verify(&cap, &holder, &root), Err(Error::Shape)));
    }
}

#[test]
fn presentation_requires_verified_transport_audience() {
    let root = crate::identity::for_test();
    let holder = crate::identity::for_test();
    let impostor = crate::identity::for_test();
    let mut store = store(&root);
    let cap = ai(&root, &holder);
    let wire = json!({"t":PRESENT,"cap":cap});
    store.receive_cap(
        &impostor.node_id(),
        Some(impostor.did()),
        &wire,
        &root,
        now(),
    );
    store.receive_cap(&holder.node_id(), None, &wire, &root, now());
    assert!(store.caps.presented.is_empty());
    store.receive_cap(&holder.node_id(), Some(holder.did()), &wire, &root, now());
    assert_eq!(store.caps.presented.len(), 1);
    assert!(
        store
            .verify_cap(
                &wire["cap"],
                holder.did(),
                Service::Ai,
                Action::Infer,
                "*",
                None,
                now()
            )
            .is_ok()
    );
}

#[test]
fn presented_table_per_node_global_and_receive_bounds() {
    let root = crate::identity::for_test();
    let holder = crate::identity::for_test();
    let mut book = Book::default();
    let cap = capability::parse(&ai(&root, &holder)).unwrap();
    for i in 0..10 {
        let mut cap = cap.clone();
        cap.id = format!("{i:032x}");
        book.insert_presented(&holder.node_id(), holder.did(), cap, true);
    }
    assert_eq!(book.presented.len(), MAX_PER_NODE);
    assert!(
        !book
            .presented
            .iter()
            .any(|p| p.cap.id == format!("{:032x}", 0))
    );
    for i in 0..MAX_PRESENTED + 1 {
        book.insert_presented(&format!("{i:016x}"), holder.did(), cap.clone(), true);
    }
    assert_eq!(book.presented.len(), MAX_PRESENTED);
    let tick = Instant::now();
    assert!(book.take_receive(&holder.node_id(), tick));
    assert!(!book.take_receive(&holder.node_id(), tick + StdDuration::from_millis(999)));
    assert!(book.take_receive(&holder.node_id(), tick + StdDuration::from_secs(1)));
    for i in 0..MAX_THROTTLES {
        book.take_receive(&format!("peer-{i}"), tick + StdDuration::from_secs(1));
    }
    assert_eq!(book.receive.len(), MAX_THROTTLES);
    assert!(!book.take_receive("extra", tick + StdDuration::from_secs(1)));
}

fn revocation(issuer: &Identity, ids: &[String]) -> Value {
    let mut wire =
        json!({"t":REVOKE,"fromId":issuer.did(),"ids":ids,"ts":now().timestamp_millis()});
    crate::wiresign::sign_wire(&mut wire, issuer).unwrap();
    wire
}

#[test]
fn revocations_require_trust_signature_freshness_and_transport_binding() {
    let root = crate::identity::for_test();
    let holder = crate::identity::for_test();
    let verifier = crate::identity::for_test();
    let cap = ai(&root, &holder);
    let id = cap["id"].as_str().unwrap().to_owned();
    let wire = revocation(&root, std::slice::from_ref(&id));
    let mut store = store(&verifier);
    store.receive_cap(&root.node_id(), Some(root.did()), &wire, &verifier, now());
    assert!(store.caps.revoked.is_empty());
    store.policy.ai.issuers.push(root.did().into());
    receive(&mut store, cap.clone(), &holder, &verifier);
    for bad in [
        {
            let mut w = wire.clone();
            w["ts"] = json!(now().timestamp_millis() - 120_001);
            crate::wiresign::sign_wire(&mut w, &root).unwrap();
            w
        },
        {
            let mut w = wire.clone();
            w["ids"] = json!([crate::net::peer_auth::fresh_nonce()]);
            w
        },
    ] {
        store.receive_cap(&root.node_id(), Some(root.did()), &bad, &verifier, now());
        assert!(store.caps.revoked.is_empty());
    }
    store.receive_cap(&holder.node_id(), Some(root.did()), &wire, &verifier, now());
    assert!(store.caps.revoked.is_empty());
    store.receive_cap(&root.node_id(), Some(root.did()), &wire, &verifier, now());
    assert!(store.caps.is_revoked(&id, now().timestamp_millis()));
    assert!(store.caps.presented.is_empty());
}

#[test]
fn currently_presented_chain_issuer_may_revoke() {
    let root = crate::identity::for_test();
    let alice = crate::identity::for_test();
    let holder = crate::identity::for_test();
    let verifier = crate::identity::for_test();
    let mut store = store(&verifier);
    store.policy.ai.issuers.push(root.did().into());
    let cap = child(&alice, &holder, ai(&root, &alice));
    let id = cap["id"].as_str().unwrap().to_owned();
    receive(&mut store, cap, &holder, &verifier);
    store.receive_cap(
        &alice.node_id(),
        Some(alice.did()),
        &revocation(&alice, std::slice::from_ref(&id)),
        &verifier,
        now(),
    );
    assert!(store.caps.is_revoked(&id, now().timestamp_millis()));
    assert!(store.caps.presented.is_empty());
}

#[test]
fn allowlist_capability_charges_node_and_cap_separate_limits_and_deny_wins() {
    let root = crate::identity::for_test();
    let holder = crate::identity::for_test();
    let mut store = store(&root);
    store.policy.ai.mode = AccessMode::Allowlist;
    store.policy.ai.limits.max_requests = Some(5);
    let mut cap = ai(&root, &holder);
    cap["lim"] = json!({"period":"total","max_requests":1});
    crate::wiresign::sign_wire(&mut cap, &root).unwrap();
    let account = format!("cap:{}", cap["id"].as_str().unwrap());
    receive(&mut store, cap, &holder, &root);
    let proof = store
        .authorize_peer(Service::Ai, Action::Infer, principal(&holder), None, now())
        .unwrap();
    store.record(&proof, 1, 7, now());
    assert_eq!(store.usage.ai[&principal(&holder).account()].requests, 1);
    assert_eq!(store.usage.ai[&account].requests, 1);
    assert_eq!(store.usage.ai[&account].output_chars, 7);
    assert_eq!(proof.cap_accounts[0].1.period, Period::Total);
    assert_eq!(
        store
            .authorize_peer(Service::Ai, Action::Infer, principal(&holder), None, now())
            .unwrap_err()
            .code,
        "usage_limit_exceeded"
    );
    store.policy.ai.deny.push(Rule {
        subject: holder.node_id(),
        ..Rule::default()
    });
    assert_eq!(
        store
            .authorize_peer(Service::Ai, Action::Infer, principal(&holder), None, now())
            .unwrap_err()
            .code,
        "forbidden"
    );
    store.policy.ai.deny.clear();
    store.policy.ai.allow.push(Rule {
        subject: holder.node_id(),
        ..Rule::default()
    });
    assert!(
        store
            .authorize_peer(Service::Ai, Action::Infer, principal(&holder), None, now())
            .unwrap()
            .cap_accounts
            .is_empty()
    );
}

#[test]
fn cap_accounts_obey_overflow_bound() {
    let root = crate::identity::for_test();
    let holder = crate::identity::for_test();
    let mut store = store(&root);
    for i in 0..super::super::usage::MAX_ACCOUNTS - 1 {
        store
            .usage
            .ai
            .insert(format!("node:{i:016x}"), Default::default());
    }
    let mut cap = ai(&root, &holder);
    cap["lim"] = json!({"max_requests":1});
    crate::wiresign::sign_wire(&mut cap, &root).unwrap();
    receive(&mut store, cap, &holder, &root);
    let proof = store
        .authorize_peer(Service::Ai, Action::Infer, principal(&holder), None, now())
        .unwrap();
    assert_eq!(proof.cap_accounts[0].0, "*");
    store.record(&proof, 1, 0, now());
    assert_eq!(store.usage.ai.len(), super::super::usage::MAX_ACCOUNTS + 1);
    assert_eq!(store.usage.ai["*"].requests, 1);
    assert!(
        store
            .authorize_peer(Service::Ai, Action::Infer, principal(&holder), None, now())
            .is_err()
    );
}

#[test]
fn commands_issue_import_redelegate_revoke_and_persist() {
    let root = crate::identity::for_test();
    let holder = crate::identity::for_test();
    let bob = crate::identity::for_test();
    let dir = std::env::temp_dir().join(format!("mistl-cap-{}", uuid::Uuid::new_v4()));
    let mut issuer = Store::load(&dir, now()).unwrap();
    let result = issuer.cap_command("authz.cap.issue", &json!({"aud":holder.did(),"service":"ai","actions":["infer"],"ttl_secs":600,"to":[root.did()],"limits":{"max_requests":10}}), &root, now()).unwrap();
    let cap = result["cap"].clone();
    let id = cap["id"].as_str().unwrap();
    assert!(verify(&cap, &holder, &root).is_ok());
    let mut holder_store = store(&holder);
    holder_store
        .cap_command(
            "authz.cap.import",
            &json!({"cap":cap.to_string()}),
            &holder,
            now(),
        )
        .unwrap();
    assert!(
        holder_store
            .cap_command("authz.cap.import", &json!({"cap":cap}), &bob, now())
            .is_err()
    );
    let delegated = holder_store.cap_command("authz.cap.issue", &json!({"aud":bob.did(),"service":"ai","actions":["infer"],"ttl_secs":300,"to":[root.did()],"limits":{"max_requests":5},"parent":id}), &holder, now()).unwrap();
    assert!(verify(&delegated["cap"], &bob, &root).is_ok());
    assert_eq!(
        issuer
            .cap_command(
                "authz.cap.verify",
                &json!({"cap":delegated["cap"]}),
                &root,
                now()
            )
            .unwrap()["ok"],
        true
    );
    issuer
        .cap_command("authz.cap.revoke", &json!({"id":id}), &root, now())
        .unwrap();
    let mut loaded = Store::load(&dir, now()).unwrap();
    loaded.local_did = Some(root.did().into());
    assert!(loaded.caps.issued[0].revoked);
    assert!(loaded.caps.is_revoked(id, now().timestamp_millis()));
    assert!(matches!(
        loaded.verify_cap(
            &delegated["cap"],
            bob.did(),
            Service::Ai,
            Action::Infer,
            "*",
            None,
            now()
        ),
        Err(Error::Revoked)
    ));
    let result = holder_store
        .cap_command("authz.cap.remove", &json!({"id":id}), &holder, now())
        .unwrap();
    assert_eq!(result["removed"], true);
    assert!(holder_store.caps.held.is_empty());
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn issuer_commands_validate_dids_and_deduplicate() {
    let issuer = crate::identity::for_test();
    let mut store = Store::default();
    let args = json!({"service":"ai","did":issuer.did()});
    for _ in 0..2 {
        store.command("authz.issuer.add", &args, now()).unwrap();
    }
    assert_eq!(store.policy.ai.issuers.len(), 1);
    assert!(
        store
            .command(
                "authz.issuer.add",
                &json!({"service":"ai","did":issuer.node_id()}),
                now()
            )
            .is_err()
    );
    store.command("authz.issuer.remove", &args, now()).unwrap();
    assert!(store.policy.ai.issuers.is_empty());
}

#[test]
fn delivery_is_holder_bound_and_held_and_revoked_are_bounded() {
    let issuer = crate::identity::for_test();
    let holder = crate::identity::for_test();
    let mut store = store(&holder);
    let cap = ai(&issuer, &holder);
    assert!(!store.receive_cap(
        &holder.node_id(),
        Some(holder.did()),
        &json!({"t":DELIVER,"cap":ai(&issuer, &issuer)}),
        &holder,
        now()
    ));
    assert!(store.caps.held.is_empty());
    assert!(store.receive_cap(
        &issuer.node_id(),
        Some(issuer.did()),
        &json!({"t":DELIVER,"cap":cap}),
        &holder,
        now()
    ));
    // A valid capability relayed by someone other than its issuer is ignored.
    let stranger = crate::identity::for_test();
    store.caps.held.clear();
    assert!(!store.receive_cap(
        &stranger.node_id(),
        Some(stranger.did()),
        &json!({"t":DELIVER,"cap":ai(&issuer, &holder)}),
        &holder,
        now()
    ));
    assert!(store.caps.held.is_empty());
    // One issuer cannot push more than MAX_DELIVERED_PER_ISSUER capabilities.
    for _ in 0..MAX_DELIVERED_PER_ISSUER {
        assert!(store.receive_cap(
            &issuer.node_id(),
            Some(issuer.did()),
            &json!({"t":DELIVER,"cap":ai(&issuer, &holder)}),
            &holder,
            now()
        ));
    }
    assert!(!store.receive_cap(
        &issuer.node_id(),
        Some(issuer.did()),
        &json!({"t":DELIVER,"cap":ai(&issuer, &holder)}),
        &holder,
        now()
    ));
    assert_eq!(store.caps.held.len(), MAX_DELIVERED_PER_ISSUER);
    store.caps.held = vec![cap.clone(); MAX_HELD];
    assert!(
        store
            .import_cap(ai(&issuer, &holder), &holder, now())
            .is_err()
    );
    store.caps.held.clear();
    for i in 0..MAX_REVOKED + 1 {
        store
            .caps
            .revoke(&format!("{i:032x}"), now().timestamp_millis() + i as i64);
    }
    assert_eq!(store.caps.revoked.len(), MAX_REVOKED);
    assert!(
        !store
            .caps
            .is_revoked(&format!("{:032x}", 0), now().timestamp_millis())
    );
    store
        .caps
        .prune(now().timestamp_millis() + capability::MAX_LIFETIME_MS + MAX_REVOKED as i64 + 1);
    assert!(store.caps.revoked.is_empty());
    assert!(store.caps.held.is_empty());
}

#[test]
fn mixed_overflow_periods_do_not_reset_shared_usage() {
    use super::super::usage::UsageBook;
    let day = Limits {
        max_requests: Some(3),
        ..Limits::default()
    };
    let total = Limits {
        period: Period::Total,
        max_requests: Some(3),
        ..Limits::default()
    };
    let mut usage = UsageBook::default();
    usage.check(Service::Ai, "*", &day, now()).unwrap();
    usage.record(Service::Ai, "*", &day, 2, 0, now());
    usage.check(Service::Ai, "*", &total, now()).unwrap();
    assert_eq!(usage.ai["*"].requests, 2);
    usage.record(Service::Ai, "*", &total, 1, 0, now());
    let bytes = serde_json::to_vec(&usage).unwrap();
    let mut usage: UsageBook = serde_json::from_slice(&bytes).unwrap();
    assert!(
        usage
            .check(Service::Ai, "*", &day, now() + Duration::days(1))
            .is_err()
    );
    assert!(
        usage
            .check(Service::Ai, "*", &total, now() + Duration::days(1))
            .is_err()
    );
    assert_eq!(usage.ai["*"].requests, 3);
    let mut same_period = UsageBook::default();
    same_period.record(Service::Ai, "*", &day, 3, 0, now());
    assert!(
        same_period
            .check(Service::Ai, "*", &day, now() + Duration::days(1))
            .is_ok()
    );
}

#[test]
fn escrow_capability_limits_charge_both_accounts() {
    let root = crate::identity::for_test();
    let holder = crate::identity::for_test();
    let mut store = store(&root);
    store.policy.escrow.mode = AccessMode::Allowlist;
    let mut cap = signed(&root, &holder, Service::Escrow, &[Action::Read], "*");
    cap["lim"] = json!({"max_requests":1});
    crate::wiresign::sign_wire(&mut cap, &root).unwrap();
    let account = format!("cap:{}", cap["id"].as_str().unwrap());
    receive(&mut store, cap, &holder, &root);
    let proof = store
        .authorize_peer(
            Service::Escrow,
            Action::Read,
            principal(&holder),
            Some("room/slot"),
            now(),
        )
        .unwrap();
    store.record(&proof, 1, 0, now());
    assert_eq!(
        store.usage.escrow[&principal(&holder).account()].requests,
        1
    );
    assert_eq!(store.usage.escrow[&account].requests, 1);
    assert!(
        store
            .authorize_peer(
                Service::Escrow,
                Action::Read,
                principal(&holder),
                Some("room/slot"),
                now()
            )
            .is_err()
    );
}

#[test]
fn repeated_revocation_retains_known_expiry_and_ignores_expired_caps() {
    let root = crate::identity::for_test();
    let holder = crate::identity::for_test();
    let mut book = Book::default();
    let cap = capability::parse(&ai(&root, &holder)).unwrap();
    book.insert_presented(&holder.node_id(), holder.did(), cap.clone(), true);
    book.revoke(&cap.id, now().timestamp_millis());
    assert_eq!(book.revoked[0].exp, cap.exp);
    book.revoke(&cap.id, now().timestamp_millis() + 300_000);
    assert_eq!(book.revoked[0].exp, cap.exp);
    book.prune(cap.exp);
    assert!(book.revoked.is_empty());
    book.insert_presented(&holder.node_id(), holder.did(), cap.clone(), true);
    book.revoke(&cap.id, cap.exp + 1);
    assert!(book.revoked.is_empty());
}

#[test]
fn corrupt_capability_files_are_preserved_and_recovered() {
    let dir = std::env::temp_dir().join(format!("mistl-cap-corrupt-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(dir.join("authz")).unwrap();
    for file in ["issued.json", "held.json", "revoked.json"] {
        std::fs::write(dir.join("authz").join(file), b"{").unwrap();
    }
    let loaded = Store::load(&dir, now()).unwrap();
    assert!(
        loaded.caps.issued.is_empty()
            && loaded.caps.held.is_empty()
            && loaded.caps.revoked.is_empty()
    );
    assert_eq!(
        std::fs::read_dir(dir.join("authz"))
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.file_name().to_string_lossy().contains(".corrupt-"))
            .count(),
        3
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn public_p2p_authorization_uses_verified_presentation_and_records_cap_usage() {
    use crate::net::peer_auth;
    let _guard = super::super::TEST_LOCK.lock().await;
    super::super::test_reset();
    let root = crate::identity::for_test();
    let holder = crate::identity::for_test();
    let room = format!("authz-cap-{}", uuid::Uuid::new_v4());
    let ts = peer_auth::now_ms();
    let bytes = peer_auth::build_proof(
        peer_auth::HELLO_TAG,
        &room,
        &holder,
        ts,
        &peer_auth::fresh_nonce(),
        false,
    );
    let proof =
        peer_auth::check_proof(&bytes, peer_auth::HELLO_TAG, &room, &holder.node_id(), ts).unwrap();
    peer_auth::accept_proof(&room, &proof).unwrap();
    let account;
    {
        let mut store = STORE.lock().unwrap();
        store.policy.ai.mode = AccessMode::Allowlist;
        let cap = store.cap_command("authz.cap.issue", &json!({"aud":holder.did(),"service":"ai","actions":["infer"],"limits":{"max_requests":1}}), &root, Utc::now()).unwrap()["cap"].clone();
        account = format!("cap:{}", cap["id"].as_str().unwrap());
        store.receive_cap(
            &holder.node_id(),
            Some(holder.did()),
            &json!({"t":PRESENT,"cap":cap}),
            &root,
            Utc::now(),
        );
    }
    assert!(super::super::access_allowed(
        Service::Ai,
        Some(&room),
        &holder.node_id()
    ));
    let authorized = super::super::authorize_p2p(
        Service::Ai,
        Action::Infer,
        Some(&room),
        &holder.node_id(),
        None,
    )
    .unwrap();
    assert_eq!(
        authorized.principal.as_ref().unwrap().did.as_deref(),
        Some(holder.did())
    );
    authorized.record(1, 3);
    assert_eq!(STORE.lock().unwrap().usage.ai[&account].requests, 1);
    assert!(super::super::access_allowed(
        Service::Ai,
        Some(&room),
        &holder.node_id()
    ));
    assert_eq!(
        super::super::authorize_p2p(
            Service::Ai,
            Action::Infer,
            Some(&room),
            &holder.node_id(),
            None
        )
        .unwrap_err()
        .code,
        "usage_limit_exceeded"
    );
    super::super::test_command(
        "authz.deny.add",
        json!({"service":"ai","subject":holder.node_id()}),
    )
    .unwrap();
    assert!(!super::super::access_allowed(
        Service::Ai,
        Some(&room),
        &holder.node_id()
    ));
    super::super::test_reset();
}
