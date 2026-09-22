//! The tunnel's own signaling/data payload envelope: the `data` bytes of
//! every mistlib message this manager sends or receives. `Tunnel`/`Stdio`
//! carry a further-nested payload (a serialized
//! [`crate::tunnel::wire::TunnelMessage`], or raw stdio bytes,
//! respectively).
//!
//! Ported verbatim from `p2p/src/rtc/manager/payload.rs`: the tag/field
//! names/casing and the base64 byte encoding below are wire-compatible with
//! a deployed `p2p` binary and must not change (see
//! `TUNNEL_INTEGRATION_CONTRACT.md`, non-negotiable #1). Visibility is
//! bumped to `pub` (upstream: `pub(super)`, scoped to `p2p::rtc::manager`)
//! since other tunnel workers may need to construct or inspect payloads
//! directly.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum P2pPayload {
    /// Additive extension; older p2p peers ignore this unknown envelope kind.
    Graph {
        room: String,
        message: crate::tunnel::graph::Message,
    },
    Role {
        role: String,
    },
    Capabilities {
        forwards: Vec<String>,
    },
    Chat {
        text: String,
    },
    Tunnel {
        #[serde(with = "crate::tunnel::wire::vec_base64")]
        data: Vec<u8>,
    },
    Stdio {
        #[serde(with = "crate::tunnel::wire::vec_base64")]
        data: Vec<u8>,
    },
    ForwardRequest {
        req_id: String,
        proto: String,
        remote_addr: String,
        target: String,
    },
    ForwardResponse {
        req_id: String,
        target: String,
        accepted: bool,
    },
}
