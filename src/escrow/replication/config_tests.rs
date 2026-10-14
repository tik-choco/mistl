use crate::config::{self, Config, EscrowConfig, EscrowReplication};
use serde_json::json;

fn cluster(room: &str, count: usize) -> EscrowReplication {
    EscrowReplication {
        room: room.into(),
        voters: (0..count).map(|i| format!("{i:016x}")).collect(),
        learners: Vec::new(),
        migration_seed: None,
    }
}

fn configured(count: usize) -> EscrowConfig {
    EscrowConfig {
        rooms: vec!["room-a".into()],
        replication: vec![cluster("room-a", count)],
        ..Default::default()
    }
}

#[test]
fn replication_default_preserves_legacy_config_and_requires_explicit_opt_in() {
    let cfg = EscrowConfig::default();
    assert!(cfg.replication.is_empty());
    cfg.validate().unwrap();
    let legacy: EscrowConfig =
        serde_json::from_value(json!({"enabled":true,"rooms":["room-a"]})).unwrap();
    assert!(legacy.replication.is_empty());
    legacy.validate().unwrap();
    let roundtrip: EscrowConfig =
        toml::from_str(&toml::to_string(&configured(3)).unwrap()).unwrap();
    roundtrip.validate().unwrap();
    assert_eq!(roundtrip.replication[0].voters.len(), 3);
}

#[test]
fn replication_accepts_bounded_fixed_voter_sets_and_rejects_empty_or_large_sets() {
    for count in [1, 2, 3, 9] {
        configured(count).validate().unwrap();
    }
    for count in [0, 10, 65] {
        assert!(configured(count).validate().is_err());
    }
    let mut duplicate = configured(3);
    duplicate.replication[0].voters[2] = duplicate.replication[0].voters[0].clone();
    assert!(duplicate.validate().is_err());
}

#[test]
fn replication_voters_use_only_canonical_transport_node_ids() {
    for invalid in [
        "",
        "a",
        "a".repeat(15).as_str(),
        "a".repeat(17).as_str(),
        "ABCDEF0123456789",
        "0123456789abcdeg",
        "0123456789abcde-",
        " 123456789abcdef",
        "did:key:zExample",
        "0123456789abcdef/other",
    ] {
        let mut cfg = configured(1);
        cfg.replication[0].voters[0] = invalid.into();
        assert!(cfg.validate().is_err(), "accepted voter {invalid:?}");
    }
}

#[test]
fn replication_rooms_must_be_unique_valid_and_already_configured() {
    let mut cfg = configured(3);
    cfg.replication[0].room = "unknown-room".into();
    assert!(cfg.validate().is_err());
    let mut cfg = configured(3);
    cfg.replication.push(cluster("room-a", 1));
    assert!(cfg.validate().is_err());
    for invalid in ["", "room with spaces", "日本語", "bad?room"] {
        let cfg = EscrowConfig {
            rooms: vec![invalid.into()],
            replication: vec![cluster(invalid, 1)],
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }
    let cfg = EscrowConfig {
        rooms: vec!["room-a".into(), "room-b".into()],
        replication: vec![cluster("room-a", 3), cluster("room-b", 3)],
        ..Default::default()
    };
    cfg.validate().unwrap();
    let cfg = EscrowConfig {
        rooms: (0..65).map(|i| format!("room-{i}")).collect(),
        replication: (0..65).map(|i| cluster(&format!("room-{i}"), 1)).collect(),
        ..Default::default()
    };
    assert!(cfg.validate().is_err());
}

#[test]
fn replication_config_rejects_unknown_cluster_fields_and_malformed_shapes() {
    for value in [
        json!({"room":"room-a","voters":["0123456789abcdef"],"dynamic":true}),
        json!({"room":"room-a"}),
        json!({"room":"room-a","voters":"all"}),
        json!({"room":1,"voters":["0123456789abcdef"]}),
    ] {
        assert!(serde_json::from_value::<EscrowReplication>(value).is_err());
    }
}

#[test]
fn replication_config_updates_validate_atomically_and_require_restart() {
    let initial =
        config::set_by_path(&Config::default(), "escrow.rooms", json!(["room-a"])).unwrap();
    let value = json!([{"room":"room-a","voters":["0123456789abcdef","123456789abcdef0","23456789abcdef01"]}]);
    let updated = config::set_by_path(&initial, "escrow.replication", value).unwrap();
    assert_eq!(updated.escrow.replication.len(), 1);
    assert_eq!(config::applies_when("escrow.replication"), "daemon restart");
    assert!(config::set_by_path(&updated, "escrow.rooms", json!([])).is_err());
    assert!(
        config::set_by_path(
            &updated,
            "escrow.replication",
            json!([{"room":"room-a","voters":[]}])
        )
        .is_err()
    );
    assert!(initial.escrow.replication.is_empty());
    assert_eq!(updated.escrow.replication[0].voters.len(), 3);
}

#[test]
fn replication_preserves_blob_total_and_slot_quota_validation() {
    let mut cfg = configured(3);
    cfg.max_blob_bytes = 0;
    assert!(cfg.validate().is_err());
    cfg.max_blob_bytes = 1024 * 1024 * 1024 + 1;
    assert!(cfg.validate().is_err());
    cfg.max_blob_bytes = 1;
    cfg.max_total_bytes = 0;
    assert!(cfg.validate().is_err());
    cfg.max_total_bytes = (1u64 << 40) + 1;
    assert!(cfg.validate().is_err());
    cfg.max_total_bytes = 1;
    cfg.max_slots_per_room = 0;
    assert!(cfg.validate().is_err());
    cfg.max_slots_per_room = 65537;
    assert!(cfg.validate().is_err());
    cfg.max_slots_per_room = 1;
    cfg.validate().unwrap();
}

#[test]
fn replication_learners_are_bounded_disjoint_and_do_not_change_genesis() {
    let mut cfg = configured(3);
    cfg.replication[0].learners = (3..12).map(|i| format!("{i:016x}")).collect();
    cfg.validate().unwrap();
    let genesis = cfg.replication[0].voters.clone();
    let roundtrip: EscrowConfig =
        serde_json::from_value(serde_json::to_value(&cfg).unwrap()).unwrap();
    assert_eq!(roundtrip.replication[0].voters, genesis);
    cfg.replication[0].learners.push("000000000000000c".into());
    assert!(cfg.validate().is_err());
    for learners in [
        vec!["0000000000000000"],
        vec!["bad"],
        vec!["0000000000000003", "0000000000000003"],
    ] {
        cfg.replication[0].learners = learners.into_iter().map(String::from).collect();
        assert!(cfg.validate().is_err());
    }
}

#[test]
fn replication_migration_requires_explicit_genesis_seed() {
    let mut cfg = configured(3);
    cfg.replication[0].migration_seed = Some(cfg.replication[0].voters[0].clone());
    cfg.validate().unwrap();
    cfg.replication[0].migration_seed = Some("0000000000000003".into());
    assert!(cfg.validate().is_err());
    cfg.replication[0].migration_seed = Some("invalid".into());
    assert!(cfg.validate().is_err());
}
