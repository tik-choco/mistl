use super::*;
use chrono::TimeZone;

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 3, 12, 0, 0).unwrap()
}

fn peer(verified: bool) -> Principal {
    let identity = crate::identity::for_test();
    Principal {
        node: identity.node_id(),
        did: verified.then(|| identity.did().to_owned()),
    }
}

fn rule(subject: &str) -> Rule {
    Rule {
        subject: subject.into(),
        ..Rule::default()
    }
}

fn allow(
    store: &mut Store,
    principal: Principal,
    when: DateTime<Utc>,
) -> std::result::Result<Authorized, Denied> {
    store.authorize_peer(Service::Ai, Action::Infer, principal, None, when)
}

fn temp_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("mistl-authz-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn create_token(store: &mut Store, args: Value) -> (String, String) {
    let result = store.command("authz.token.create", &args, now()).unwrap();
    (
        format!("Bearer {}", result["token"].as_str().unwrap()),
        result["info"]["id"].as_str().unwrap().to_owned(),
    )
}

#[test]
fn default_open_and_anonymous_proof() {
    let mut store = Store::default();
    let unverified = peer(false);
    assert!(allow(&mut store, unverified.clone(), now()).is_ok());
    assert!(
        store
            .authorize_peer(
                Service::Escrow,
                Action::Write,
                unverified,
                Some("room/slot"),
                now()
            )
            .is_ok()
    );
    assert!(
        store
            .authorize_api(None, None, now())
            .unwrap()
            .accounts
            .is_empty()
    );
    assert!(Authorized::anonymous().accounts.is_empty());
}

#[test]
fn allowlist_requires_verified_identity_for_both_subject_kinds() {
    let verified = peer(true);
    let mut unverified = verified.clone();
    unverified.did = None;
    for subject in [&verified.node, verified.did.as_ref().unwrap()] {
        let mut store = Store::default();
        store.policy.ai.mode = AccessMode::Allowlist;
        assert_eq!(
            allow(&mut store, verified.clone(), now()).unwrap_err().code,
            "forbidden"
        );
        store.policy.ai.allow.push(rule(subject));
        assert!(allow(&mut store, verified.clone(), now()).is_ok());
        assert_eq!(
            allow(&mut store, unverified.clone(), now())
                .unwrap_err()
                .code,
            "forbidden"
        );
    }
}

#[test]
fn node_deny_does_not_require_verification_and_deny_wins() {
    let verified = peer(true);
    let mut store = Store::default();
    store
        .policy
        .ai
        .allow
        .push(rule(verified.did.as_ref().unwrap()));
    store.policy.ai.deny.push(rule(&verified.node));
    assert_eq!(
        allow(&mut store, verified.clone(), now()).unwrap_err().code,
        "forbidden"
    );
    let mut unverified = verified.clone();
    unverified.did = None;
    assert_eq!(
        allow(&mut store, unverified, now()).unwrap_err().code,
        "forbidden"
    );
    store.policy.ai.deny = vec![rule(verified.did.as_ref().unwrap())];
    assert!(allow(&mut store, verified, now()).is_err());
}

#[test]
fn expiry_is_exclusive_and_expired_rules_are_pruned_on_save() {
    let principal = peer(true);
    let mut store = Store::default();
    store.policy.ai.deny.push(Rule {
        subject: principal.node.clone(),
        expires_at: Some(now()),
        note: None,
    });
    assert!(allow(&mut store, principal.clone(), now()).is_ok());
    store.policy.ai.mode = AccessMode::Allowlist;
    store.policy.ai.allow.push(Rule {
        subject: principal.node.clone(),
        expires_at: Some(now()),
        note: None,
    });
    assert!(allow(&mut store, principal, now()).is_err());
    store
        .command("authz.api.set", &json!({"require_token": false}), now())
        .unwrap();
    assert!(store.policy.ai.allow.is_empty());
    assert!(store.policy.ai.deny.is_empty());
}

#[test]
fn request_and_output_limits_are_cumulative_and_saturate() {
    let principal = peer(true);
    let mut store = Store::default();
    store.policy.ai.limits = Limits {
        max_requests: Some(2),
        max_output_chars: Some(10),
        ..Limits::default()
    };
    let proof = allow(&mut store, principal.clone(), now()).unwrap();
    store.record(&proof, 1, 4, now());
    let proof = allow(&mut store, principal.clone(), now()).unwrap();
    store.record(&proof, 1, 4, now());
    assert_eq!(
        allow(&mut store, principal.clone(), now())
            .unwrap_err()
            .code,
        "usage_limit_exceeded"
    );
    store.policy.ai.limits.max_requests = None;
    let proof = allow(&mut store, principal.clone(), now()).unwrap();
    store.record(&proof, 1, 2, now());
    assert_eq!(
        allow(&mut store, principal.clone(), now())
            .unwrap_err()
            .http_status(),
        429
    );
    store.record(&proof, u64::MAX, u64::MAX, now());
    assert_eq!(store.usage.ai[&principal.account()].requests, u64::MAX);
    assert_eq!(store.usage.ai[&principal.account()].output_chars, u64::MAX);
}

#[test]
fn overrides_replace_defaults_even_for_unverified_node_limits() {
    let mut principal = peer(true);
    let mut store = Store::default();
    store.policy.ai.limits.max_requests = Some(0);
    store.policy.ai.overrides.push(LimitOverride {
        subject: principal.node.clone(),
        limits: Limits {
            max_requests: Some(1),
            ..Limits::default()
        },
    });
    principal.did = None;
    assert!(allow(&mut store, principal.clone(), now()).is_ok());
    store.policy.ai.overrides[0].subject = peer(true).did.unwrap();
    assert!(allow(&mut store, principal.clone(), now()).is_err());
    let verified = peer(true);
    store.policy.escrow.limits.max_stored_bytes = Some(30);
    store.policy.escrow.overrides.push(LimitOverride {
        subject: verified.did.clone().unwrap(),
        limits: Limits::default(),
    });
    assert_eq!(
        store.policy.escrow.limits_for(&verified).max_stored_bytes,
        None
    );
    assert_eq!(
        store.policy.escrow.limits_for(&principal).max_stored_bytes,
        Some(30)
    );
}

#[test]
fn utc_day_month_and_total_rollover() {
    let principal = peer(false);
    let midnight = Utc.with_ymd_and_hms(2027, 1, 1, 0, 0, 0).unwrap();
    for period in [Period::Day, Period::Month, Period::Total] {
        let mut store = Store::default();
        store.policy.ai.limits = Limits {
            period,
            max_requests: Some(1),
            ..Limits::default()
        };
        let proof = allow(
            &mut store,
            principal.clone(),
            midnight - Duration::seconds(1),
        )
        .unwrap();
        store.record(&proof, 1, 0, midnight - Duration::seconds(1));
        assert!(
            allow(
                &mut store,
                principal.clone(),
                midnight - Duration::milliseconds(1)
            )
            .is_err()
        );
        assert_eq!(
            allow(&mut store, principal.clone(), midnight).is_ok(),
            period != Period::Total
        );
        assert_eq!(
            store.usage.ai[&principal.account()].period_start,
            if period == Period::Total {
                DateTime::UNIX_EPOCH
            } else {
                midnight
            }
        );
    }
    let mut book = UsageBook::default();
    let limits = Limits {
        period: Period::Month,
        max_requests: Some(1),
        ..Limits::default()
    };
    book.record(Service::Ai, "node:0000000000000001", &limits, 1, 0, now());
    assert!(
        book.check(
            Service::Ai,
            "node:0000000000000001",
            &limits,
            now() + Duration::days(1)
        )
        .is_err()
    );
}

#[test]
fn overflow_account_is_bounded_shared_and_uses_default_limits() {
    let mut store = Store::default();
    store.policy.ai.limits.max_requests = Some(1);
    for id in 0..usage::MAX_ACCOUNTS {
        let principal = Principal {
            node: format!("{id:016x}"),
            did: None,
        };
        allow(&mut store, principal, now()).unwrap();
    }
    let principal = Principal {
        node: "ffffffffffffffff".into(),
        did: None,
    };
    store.policy.ai.overrides.push(LimitOverride {
        subject: principal.node.clone(),
        limits: Limits::default(),
    });
    let proof = allow(&mut store, principal, now()).unwrap();
    assert_eq!(proof.accounts, vec!["*"]);
    store.record(&proof, 1, 0, now());
    for id in 0..32 {
        let principal = Principal {
            node: format!("ff{id:014x}"),
            did: None,
        };
        assert_eq!(
            allow(&mut store, principal, now()).unwrap_err().code,
            "usage_limit_exceeded"
        );
    }
    assert_eq!(store.usage.ai.len(), usage::MAX_ACCOUNTS + 1);
    assert!(
        allow(
            &mut store,
            Principal {
                node: "0000000000000000".into(),
                did: None
            },
            now()
        )
        .is_ok()
    );
}

#[test]
fn token_creation_lookup_model_filter_and_statuses() {
    let mut store = Store::default();
    let (header, id) = create_token(
        &mut store,
        json!({"name":"client", "models":["model-a"], "limits":{"max_requests":1}}),
    );
    assert_eq!(header.len(), 54);
    assert_eq!(store.tokens.0[0].id.len(), 12);
    assert!(
        !store.tokens.0[0]
            .hash
            .contains(header.strip_prefix("Bearer ").unwrap())
    );
    let mixed = header.replacen("Bearer", "bEaReR", 1);
    assert!(
        store
            .tokens
            .lookup(Some(&format!("  {mixed}  ")), false, now())
            .unwrap()
            .is_some()
    );
    assert_eq!(
        store.model_filter(Some(&header), now()).unwrap(),
        Some(vec!["model-a".into()])
    );
    for model in [None, Some(""), Some("model-b")] {
        let denial = store
            .authorize_api(Some(&header), model, now())
            .unwrap_err();
        assert_eq!(denial.code, "model_not_allowed");
        assert_eq!(denial.http_status(), 403);
    }
    let proof = store
        .authorize_api(Some(&header), Some("model-a"), now())
        .unwrap();
    assert_eq!(proof.token.as_deref(), Some(id.as_str()));
    store.record(&proof, 1, 3, now());
    assert_eq!(
        store
            .authorize_api(Some(&header), Some("model-a"), now())
            .unwrap_err()
            .http_status(),
        429
    );
    // Listing models is independent of the cumulative request budget.
    assert!(store.model_filter(Some(&header), now()).is_ok());
    store
        .command("authz.token.revoke", &json!({"id": id}), now())
        .unwrap();
    assert_eq!(
        store
            .authorize_api(Some(&header), Some("model-a"), now())
            .unwrap_err()
            .http_status(),
        401
    );
    assert_eq!(
        store
            .model_filter(Some(&header), now())
            .unwrap_err()
            .http_status(),
        401
    );
}

#[test]
fn token_expiry_and_optional_authorization() {
    let mut store = Store::default();
    for header in [
        None,
        Some("Bearer other-key"),
        Some("Basic abc"),
        Some(""),
        Some("Bearer"),
    ] {
        assert!(
            store
                .authorize_api(header, None, now())
                .unwrap()
                .accounts
                .is_empty()
        );
        assert_eq!(store.model_filter(header, now()).unwrap(), None);
    }
    assert_eq!(
        store
            .authorize_api(Some("Bearer mst_unknown"), None, now())
            .unwrap_err()
            .http_status(),
        401
    );
    let (header, _) = create_token(&mut store, json!({"name":"expiring", "ttl_secs":60}));
    assert!(
        store
            .authorize_api(Some(&header), None, now() + Duration::seconds(59))
            .is_ok()
    );
    assert_eq!(
        store
            .authorize_api(Some(&header), None, now() + Duration::seconds(60))
            .unwrap_err()
            .code,
        "unauthorized"
    );
    store.policy.api.require_token = true;
    for header in [
        None,
        Some("Bearer other-key"),
        Some("Basic abc"),
        Some("Bearer mst_unknown"),
    ] {
        assert_eq!(
            store
                .authorize_api(header, None, now())
                .unwrap_err()
                .http_status(),
            401
        );
        assert_eq!(
            store.model_filter(header, now()).unwrap_err().http_status(),
            401
        );
    }
    assert!(store.authorize_api(Some(&header), None, now()).is_ok());
}

#[test]
fn token_and_node_usage_are_independent() {
    let mut store = Store::default();
    store.policy.ai.limits.max_requests = Some(0);
    let (header, id) = create_token(
        &mut store,
        json!({"name":"client", "limits":{"max_requests":2,"max_output_chars":5}}),
    );
    let proof = store.authorize_api(Some(&header), None, now()).unwrap();
    store.record(&proof, 1, 5, now());
    assert_eq!(store.usage.ai[&format!("token:{id}")].output_chars, 5);
    assert!(store.authorize_api(Some(&header), None, now()).is_err());
    assert!(allow(&mut store, peer(false), now()).is_err());
}

#[test]
fn persistence_round_trip_and_hash_is_absent_from_command_output() {
    let dir = temp_dir();
    let mut store = Store::load(&dir, now()).unwrap();
    store
        .command("authz.api.set", &json!({"require_token":true}), now())
        .unwrap();
    store
        .command(
            "authz.mode.set",
            &json!({"service":"escrow","mode":"allowlist"}),
            now(),
        )
        .unwrap();
    let (header, id) = create_token(&mut store, json!({"name":"persisted"}));
    let proof = store.authorize_api(Some(&header), None, now()).unwrap();
    store.record(&proof, 1, 9, now());
    store.flush_best_effort(now());
    let mut loaded = Store::load(&dir, now()).unwrap();
    assert!(loaded.policy.api.require_token);
    assert_eq!(loaded.policy.escrow.mode, AccessMode::Allowlist);
    assert_eq!(loaded.tokens.0[0].last_used_at, Some(now()));
    assert_eq!(loaded.usage.ai[&format!("token:{id}")].output_chars, 9);
    assert!(loaded.authorize_api(Some(&header), None, now()).is_ok());
    for cmd in ["authz.show", "authz.token.list"] {
        let output = loaded.command(cmd, &json!({}), now()).unwrap().to_string();
        assert!(!output.contains("\"hash\""));
        assert!(!output.contains(&loaded.tokens.0[0].hash));
        assert!(!output.contains(header.strip_prefix("Bearer ").unwrap()));
    }
    assert!(
        std::fs::read_to_string(dir.join("authz/tokens.json"))
            .unwrap()
            .contains("\"hash\"")
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn corrupt_files_are_preserved_and_defaulted() {
    let dir = temp_dir();
    std::fs::create_dir_all(dir.join("authz")).unwrap();
    for (file, bytes) in [
        ("policy.json", b"{".as_slice()),
        ("tokens.json", b"[] trailing".as_slice()),
        ("usage.json", b"".as_slice()),
    ] {
        std::fs::write(dir.join("authz").join(file), bytes).unwrap();
    }
    let store = Store::load(&dir, now()).unwrap();
    assert_eq!(store.policy.ai.mode, AccessMode::Open);
    assert!(store.tokens.0.is_empty());
    assert!(store.usage.ai.is_empty());
    let copies = std::fs::read_dir(dir.join("authz"))
        .unwrap()
        .filter_map(|e| {
            let e = e.unwrap();
            e.file_name()
                .to_string_lossy()
                .contains(".corrupt-")
                .then_some(e.path())
        })
        .collect::<Vec<_>>();
    assert_eq!(copies.len(), 3);
    assert!(copies.iter().any(|p| std::fs::read(p).unwrap() == b"{"));
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn commands_validate_and_mutate_rules_limits_and_tokens() {
    let mut store = Store::default();
    let principal = peer(true);
    for subject in [principal.node.clone(), principal.did.clone().unwrap()] {
        let args = json!({"service":"ai","subject":subject,"ttl_secs":60,"note":"test"});
        store.command("authz.allow.add", &args, now()).unwrap();
        store.command("authz.allow.add", &args, now()).unwrap();
    }
    assert_eq!(store.policy.ai.allow.len(), 2);
    store
        .command(
            "authz.allow.remove",
            &json!({"service":"ai","subject":principal.node}),
            now(),
        )
        .unwrap();
    assert_eq!(store.policy.ai.allow.len(), 1);
    store
        .command(
            "authz.limits.set",
            &json!({"service":"escrow","subject":principal.node,"limits":{"max_stored_bytes":12}}),
            now(),
        )
        .unwrap();
    assert_eq!(
        store.policy.escrow.limits_for(&principal).max_stored_bytes,
        Some(12)
    );
    store
        .command(
            "authz.limits.set",
            &json!({"service":"escrow","subject":principal.node,"limits":null}),
            now(),
        )
        .unwrap();
    assert!(store.policy.escrow.overrides.is_empty());
    for args in [
        json!({"service":"ai","subject":"ABCDEF0123456789"}),
        json!({"service":"ai","subject":principal.node,"ttl_secs":59}),
        json!({"service":"ai","subject":principal.node,"ttl_secs":i64::MAX}),
        json!({"service":"ai","subject":principal.node,"note":"x".repeat(201)}),
    ] {
        assert!(store.command("authz.deny.add", &args, now()).is_err());
    }
    for args in [
        json!({"name":""}),
        json!({"name":"x".repeat(65)}),
        json!({"name":"x","models":[""]}),
        json!({"name":"x","expires_at":now()}),
    ] {
        assert!(store.command("authz.token.create", &args, now()).is_err());
    }
    let (header, id) = create_token(&mut store, json!({"name":"original", "ttl_secs":60}));
    let updated = store.command("authz.token.update", &json!({"id":id,"name":"updated","models":["m"],"limits":{"max_requests":2},"expires_at":null}), now()).unwrap();
    assert_eq!(updated["name"], "updated");
    assert!(updated.get("hash").is_none());
    assert!(updated["expires_at"].is_null());
    assert!(store.authorize_api(Some(&header), Some("m"), now()).is_ok());
    let result = store
        .command("authz.token.delete", &json!({"id":id}), now())
        .unwrap();
    assert_eq!(result, json!({"deleted":true}));
    assert!(
        store
            .authorize_api(Some(&header), Some("m"), now())
            .is_err()
    );
}

#[test]
fn entry_and_token_caps_and_failed_saves_leave_store_unchanged() {
    let mut store = Store::default();
    for id in 0..policy::MAX_ENTRIES {
        store.policy.ai.allow.push(rule(&format!("{id:016x}")));
    }
    assert!(
        store
            .command(
                "authz.allow.add",
                &json!({"service":"ai","subject":"ffffffffffffffff"}),
                now()
            )
            .is_err()
    );
    assert!(
        store
            .command(
                "authz.allow.add",
                &json!({"service":"ai","subject":"0000000000000000"}),
                now()
            )
            .is_ok()
    );
    for _ in 0..tokens::MAX_TOKENS {
        store
            .tokens
            .create("test".into(), Vec::new(), Limits::default(), None, now())
            .unwrap();
    }
    assert!(
        store
            .command("authz.token.create", &json!({"name":"one too many"}), now())
            .is_err()
    );
    let dir = temp_dir();
    let file = dir.join("file");
    std::fs::write(&file, "not a directory").unwrap();
    store.dir = Some(file);
    assert!(
        store
            .command("authz.api.set", &json!({"require_token":true}), now())
            .is_err()
    );
    assert!(!store.policy.api.require_token);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn usage_listing_and_reset() {
    let mut store = Store::default();
    let principal = peer(false);
    let proof = allow(&mut store, principal.clone(), now()).unwrap();
    store.record(&proof, 1, 12, now());
    let listed = store.command("authz.usage", &json!({}), now()).unwrap();
    assert_eq!(listed["ai"][0]["node"], principal.node);
    assert_eq!(listed["ai"][0]["requests"], 1);
    assert_eq!(listed["ai"][0]["output_chars"], 12);
    assert_eq!(listed["escrow"], json!([]));
    assert_eq!(
        store
            .command("authz.usage", &json!({"service":"escrow"}), now())
            .unwrap()["ai"],
        json!([])
    );
    assert_eq!(
        store
            .command(
                "authz.usage.reset",
                &json!({"service":"ai","account":principal.account()}),
                now()
            )
            .unwrap(),
        json!({"reset":1})
    );
    assert!(store.usage.ai.is_empty());
    store.record(&proof, 1, 0, now());
    assert_eq!(
        store
            .command("authz.usage.reset", &json!({"service":"ai"}), now())
            .unwrap(),
        json!({"reset":1})
    );
}
