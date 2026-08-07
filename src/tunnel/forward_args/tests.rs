use super::*;

fn strings(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| s.to_string()).collect()
}

#[test]
fn connect_forward_defaults_remote_port_to_listen_port() {
    assert_eq!(
        parse_connect_forward(":8080"),
        ("tcp", 8080, "tcp:8080".to_string())
    );
    assert_eq!(
        parse_connect_forward("udp://9000"),
        ("udp", 9000, "udp:9000".to_string())
    );
}

#[test]
fn connect_forward_maps_listen_port_to_remote_target() {
    assert_eq!(
        parse_connect_forward("15432:5432"),
        ("tcp", 15432, "tcp:5432".to_string())
    );
    assert_eq!(
        parse_connect_forward("udp://19000:9000"),
        ("udp", 19000, "udp:9000".to_string())
    );
}

#[test]
fn connect_forward_without_scope_is_unaffected() {
    assert_eq!(
        parse_connect_forward("15432:5432"),
        ("tcp", 15432, "tcp:5432".to_string())
    );
}

#[test]
fn connect_forward_applies_node_scope_to_target() {
    assert_eq!(
        parse_connect_forward("10022:22@node-a"),
        ("tcp", 10022, "tcp:22@node-a".to_string())
    );
    assert_eq!(
        parse_connect_forward("udp://19000:9000@node-a"),
        ("udp", 19000, "udp:9000@node-a".to_string())
    );
    assert_eq!(
        parse_connect_forward("8080@node-a"),
        ("tcp", 8080, "tcp:8080@node-a".to_string())
    );
}

#[test]
fn split_node_scope_splits_on_last_at() {
    assert_eq!(
        split_node_scope("tcp:127.0.0.1:22@node-a"),
        ("tcp:127.0.0.1:22", Some("node-a"))
    );
}

#[test]
fn split_node_scope_leaves_unscoped_target_unchanged() {
    assert_eq!(split_node_scope("tcp:22"), ("tcp:22", None));
}

#[test]
fn split_node_scope_treats_empty_scope_as_unscoped() {
    assert_eq!(split_node_scope("tcp:22@"), ("tcp:22@", None));
}

#[test]
fn split_node_scope_treats_empty_base_as_unscoped() {
    assert_eq!(split_node_scope("@x"), ("@x", None));
}

#[test]
fn node_scoped_target_appends_scope_to_plain_base() {
    assert_eq!(node_scoped_target("tcp:22", "node-a"), "tcp:22@node-a");
}

#[test]
fn node_scoped_target_replaces_existing_scope_instead_of_stacking() {
    assert_eq!(
        node_scoped_target("tcp:22@node-a", "node-b"),
        "tcp:22@node-b"
    );
}

#[test]
fn serve_args_keep_all_forwards_when_room_is_present() {
    let (room, forwards) = split_serve_args(&strings(&["my-room", ":80", "tcp://127.0.0.1:5432"]));

    assert_eq!(room, Some("my-room".to_string()));
    assert_eq!(forwards, strings(&[":80", "tcp://127.0.0.1:5432"]));
}

#[test]
fn serve_args_treat_leading_forward_as_generated_room_mode() {
    let (room, forwards) = split_serve_args(&strings(&[":80", "udp://127.0.0.1:9000"]));

    assert_eq!(room, None);
    assert_eq!(forwards, strings(&[":80", "udp://127.0.0.1:9000"]));
}

#[test]
fn forward_key_uses_protocol_and_port() {
    assert_eq!(forward_key("tcp", "127.0.0.1:80"), "tcp:80");
    assert_eq!(forward_key("udp", ":9000"), "udp:9000");
}
