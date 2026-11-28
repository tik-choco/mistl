//! Shared endpoint policy for the local HTTP API, relay and provider tunnel.
//! Job-id and range validation remain in the job handlers.

use super::oai_tunnel;

pub(super) const MAX_BODY_BYTES: usize = 10 * 1024 * 1024;
pub(super) const MAX_JSON_BODY_BYTES: usize = 2 * 1024 * 1024;
pub(super) const MAX_GEN3D_MULTIPART_BYTES: usize = 20 * 1024 * 1024 + 128 * 1024;
pub(super) const MAX_GEN3D_JSON_BYTES: usize = 20 * 1024 * 1024 / 3 * 4 + 128 * 1024;
pub(super) const MAX_IMAGE_EDIT_MULTIPART_BYTES: usize = 32 * 1024 * 1024 + 128 * 1024;
pub(super) const MAX_IMAGE_EDIT_JSON_BYTES: usize = 32 * 1024 * 1024 / 3 * 4 + 128 * 1024;

#[derive(Clone, Copy)]
pub(super) enum BodyKind {
    None,
    Json(usize),
    Multipart(usize),
    RawMultipart(usize),
    JsonOrMultipart { json: usize, multipart: usize },
    JsonOrRawMultipart { json: usize, multipart: usize },
}

impl BodyKind {
    pub(super) fn cap(self, multipart: bool) -> usize {
        match self {
            Self::None => MAX_JSON_BODY_BYTES,
            Self::Json(cap) | Self::Multipart(cap) | Self::RawMultipart(cap) => cap,
            Self::JsonOrMultipart {
                json,
                multipart: cap,
            }
            | Self::JsonOrRawMultipart {
                json,
                multipart: cap,
            } => {
                if multipart {
                    cap
                } else {
                    json
                }
            }
        }
    }

    pub(super) fn accepts_multipart(self) -> bool {
        matches!(
            self,
            Self::Multipart(_)
                | Self::RawMultipart(_)
                | Self::JsonOrMultipart { .. }
                | Self::JsonOrRawMultipart { .. }
        )
    }

    pub(super) fn raw_multipart(self) -> bool {
        matches!(
            self,
            Self::RawMultipart(_) | Self::JsonOrRawMultipart { .. }
        )
    }

    pub(super) fn multipart_only(self) -> bool {
        matches!(self, Self::RawMultipart(_))
    }

    pub(super) fn legacy_multipart(self) -> bool {
        matches!(self, Self::JsonOrRawMultipart { .. })
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum ApiRoute {
    Models,
    ModelDetail,
    Chat,
    Responses,
    Completions,
    Speech,
    Transcription,
    Decision,
    Relay,
    RootRelay,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Model {
    None,
    Default,
    Explicit,
    /// Embeddings already accepted model-less direct v1 tunnel requests.
    ExplicitRelay,
    Catalog,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Deadline {
    Standard,
    /// Voice and lifecycle handlers use their own deadlines.
    Specialized,
    Image,
    Media,
    Gen3d,
    Motion,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Pattern {
    Exact,
    Prefix,
    Gen3dJobs,
    MediaJobs,
}

pub(super) struct Endpoint {
    pub(super) path: &'static str,
    pattern: Pattern,
    methods: &'static [&'static str],
    body: BodyKind,
    pub(super) model: Model,
    pub(super) api: ApiRoute,
    pub(super) relay: bool,
    pub(super) tunnel: bool,
    pub(super) op: &'static str,
    pub(super) tunnel_op: &'static str,
    pub(super) deadline: Deadline,
}

use {ApiRoute as A, BodyKind as B, Deadline as D, Model as M, Pattern as P};

const JSON: B = B::Json(MAX_JSON_BODY_BYTES);
const IMAGE_UPLOAD: B = B::JsonOrMultipart {
    json: MAX_IMAGE_EDIT_JSON_BYTES,
    multipart: MAX_IMAGE_EDIT_MULTIPART_BYTES,
};
const RAW_IMAGE_UPLOAD: B = B::JsonOrRawMultipart {
    json: MAX_IMAGE_EDIT_JSON_BYTES,
    multipart: MAX_IMAGE_EDIT_MULTIPART_BYTES,
};
const GEN3D_UPLOAD: B = B::JsonOrMultipart {
    json: MAX_GEN3D_JSON_BYTES,
    multipart: MAX_GEN3D_MULTIPART_BYTES,
};

// Paths are relative to /v1, except the two root lifecycle routes. The tunnel
// op differs from the API op for legacy metering; keep those names unchanged.
#[rustfmt::skip]
const ENDPOINTS: &[Endpoint] = &[
    Endpoint { path: "/models", pattern: P::Exact, methods: &["GET"], body: B::None,
        model: M::None, api: A::Models, relay: false, tunnel: true,
        op: "oai", tunnel_op: "oai", deadline: D::Standard },
    Endpoint { path: "/models/", pattern: P::Prefix, methods: &["GET"], body: B::None,
        model: M::None, api: A::ModelDetail, relay: false, tunnel: false,
        op: "oai", tunnel_op: "oai", deadline: D::Standard },
    Endpoint { path: "/chat/completions", pattern: P::Exact, methods: &["POST"], body: JSON,
        model: M::Default, api: A::Chat, relay: true, tunnel: true,
        op: "chat", tunnel_op: "oai", deadline: D::Standard },
    Endpoint { path: "/completions", pattern: P::Exact, methods: &["POST"], body: JSON,
        model: M::Explicit, api: A::Completions, relay: false, tunnel: true,
        op: "completions", tunnel_op: "completions", deadline: D::Standard },
    Endpoint { path: "/responses", pattern: P::Exact, methods: &["POST"], body: JSON,
        model: M::Explicit, api: A::Responses, relay: false, tunnel: true,
        op: "responses", tunnel_op: "responses", deadline: D::Standard },
    Endpoint { path: "/audio/speech", pattern: P::Exact, methods: &["POST"], body: JSON,
        model: M::Default, api: A::Speech, relay: false, tunnel: false,
        op: "tts", tunnel_op: "oai", deadline: D::Specialized },
    Endpoint { path: "/audio/transcriptions", pattern: P::Exact, methods: &["POST"], body: B::Multipart(MAX_BODY_BYTES),
        model: M::Default, api: A::Transcription, relay: false, tunnel: false,
        op: "stt", tunnel_op: "oai", deadline: D::Specialized },
    Endpoint { path: "/audio/translations", pattern: P::Exact, methods: &["POST"], body: B::RawMultipart(MAX_BODY_BYTES),
        model: M::Explicit, api: A::Relay, relay: true, tunnel: true,
        op: "stt", tunnel_op: "stt", deadline: D::Specialized },
    Endpoint { path: "/systemone", pattern: P::Exact, methods: &["POST"], body: JSON,
        model: M::Default, api: A::Decision, relay: false, tunnel: true,
        op: "decision", tunnel_op: "oai", deadline: D::Standard },
    Endpoint { path: "/embeddings", pattern: P::Exact, methods: &["POST"], body: JSON,
        model: M::ExplicitRelay, api: A::Relay, relay: true, tunnel: true,
        op: "embeddings", tunnel_op: "oai", deadline: D::Standard },
    Endpoint { path: "/moderations", pattern: P::Exact, methods: &["POST"], body: JSON,
        model: M::Explicit, api: A::Relay, relay: true, tunnel: true,
        op: "moderations", tunnel_op: "moderations", deadline: D::Standard },
    Endpoint { path: "/audio/voices", pattern: P::Exact, methods: &["GET"], body: B::None,
        model: M::None, api: A::Relay, relay: true, tunnel: false,
        op: "voices", tunnel_op: "oai", deadline: D::Specialized },
    Endpoint { path: "/images/edits", pattern: P::Exact, methods: &["POST"], body: RAW_IMAGE_UPLOAD,
        model: M::Explicit, api: A::Relay, relay: true, tunnel: true,
        op: "image_edit", tunnel_op: "oai", deadline: D::Image },
    Endpoint { path: "/images/variations", pattern: P::Exact, methods: &["POST"], body: B::RawMultipart(MAX_IMAGE_EDIT_MULTIPART_BYTES),
        model: M::Explicit, api: A::Relay, relay: true, tunnel: true,
        op: "image", tunnel_op: "image", deadline: D::Image },
    Endpoint { path: "/images/generations", pattern: P::Exact, methods: &["POST"], body: B::Json(MAX_IMAGE_EDIT_JSON_BYTES),
        model: M::Explicit, api: A::Relay, relay: true, tunnel: true,
        op: "image", tunnel_op: "image", deadline: D::Image },
    Endpoint { path: "/game/act", pattern: P::Exact, methods: &["POST"], body: IMAGE_UPLOAD,
        model: M::Explicit, api: A::Relay, relay: true, tunnel: true,
        op: "game", tunnel_op: "oai", deadline: D::Standard },
    Endpoint { path: "/motion/inbetween", pattern: P::Exact, methods: &["POST"], body: JSON,
        model: M::Explicit, api: A::Relay, relay: true, tunnel: true,
        op: "motion", tunnel_op: "oai", deadline: D::Motion },
    Endpoint { path: "/motion/clips", pattern: P::Exact, methods: &["GET"], body: B::None,
        model: M::Catalog, api: A::Relay, relay: true, tunnel: true,
        op: "motion", tunnel_op: "oai", deadline: D::Motion },
    Endpoint { path: "/motion/skeletons/", pattern: P::Prefix, methods: &["GET"], body: B::None,
        model: M::Catalog, api: A::Relay, relay: true, tunnel: true,
        op: "motion", tunnel_op: "oai", deadline: D::Motion },
    Endpoint { path: "/3d/generations", pattern: P::Gen3dJobs, methods: &["GET", "POST"], body: GEN3D_UPLOAD,
        model: M::Explicit, api: A::Relay, relay: true, tunnel: true,
        op: "gen3d", tunnel_op: "oai", deadline: D::Gen3d },
    Endpoint { path: "/videos", pattern: P::MediaJobs, methods: &["GET", "POST"], body: RAW_IMAGE_UPLOAD,
        model: M::Explicit, api: A::Relay, relay: true, tunnel: true,
        op: "video", tunnel_op: "video", deadline: D::Media },
    Endpoint { path: "/music", pattern: P::MediaJobs, methods: &["GET", "POST"], body: RAW_IMAGE_UPLOAD,
        model: M::Explicit, api: A::Relay, relay: true, tunnel: true,
        op: "music", tunnel_op: "music", deadline: D::Media },
    Endpoint { path: "/api/ps", pattern: P::Exact, methods: &["GET"], body: B::None,
        model: M::None, api: A::RootRelay, relay: true, tunnel: false,
        op: "status", tunnel_op: "oai", deadline: D::Specialized },
    Endpoint { path: "/health", pattern: P::Exact, methods: &["GET"], body: B::None,
        model: M::None, api: A::RootRelay, relay: true, tunnel: false,
        op: "status", tunnel_op: "oai", deadline: D::Specialized },
];

impl Endpoint {
    fn matches(&self, path: &str) -> bool {
        match self.pattern {
            P::Exact => {
                path == self.path
                    || self.model == M::Catalog && path.split('?').next() == Some(self.path)
            }
            P::Prefix => path.starts_with(self.path),
            P::Gen3dJobs | P::MediaJobs => {
                let bare = if self.pattern == P::MediaJobs {
                    path.split('?').next().unwrap_or(path)
                } else {
                    path
                };
                bare == self.path
                    || bare
                        .strip_prefix(self.path)
                        .is_some_and(|tail| tail.starts_with('/'))
            }
        }
    }

    pub(super) fn post_body(&self, path: &str) -> B {
        if path == self.path {
            self.body
        } else {
            B::None
        }
    }

    pub(super) fn model_post(&self, path: &str) -> bool {
        path == self.path && matches!(self.model, M::Explicit | M::ExplicitRelay)
    }

    pub(super) fn allows(&self, method: &str, path: &str) -> bool {
        if self.pattern == P::MediaJobs && path.split('?').next() != Some(self.path) {
            return job_method(method, oai_tunnel::media_job_key(path));
        }
        if self.pattern == P::Gen3dJobs && path != self.path {
            return job_method(method, oai_tunnel::generation_path(path));
        }
        self.methods.contains(&method)
    }

    /// Let the HTTP job handler authenticate before reporting invalid ids or
    /// methods, preserving the existing job-specific error responses.
    pub(super) fn api_allows(&self, method: &str, path: &str) -> bool {
        match self.pattern {
            P::MediaJobs => matches!(method, "GET" | "POST" | "DELETE"),
            P::Gen3dJobs if path != self.path => matches!(method, "GET" | "DELETE"),
            _ => self.allows(method, path),
        }
    }
}

fn job_method(method: &str, job: Option<(&str, bool)>) -> bool {
    match job {
        Some((_, true)) => method == "GET",
        Some((_, false)) => matches!(method, "GET" | "DELETE"),
        None => false,
    }
}

pub(super) fn lookup(path: &str) -> Option<&'static Endpoint> {
    ENDPOINTS.iter().find(|endpoint| endpoint.matches(path))
}

pub(super) fn api_lookup(path: &str) -> Option<(&'static Endpoint, &str)> {
    let relative = match path {
        "/voices" | "/v1/voices" => "/audio/voices",
        _ => path.strip_prefix("/v1").unwrap_or(path),
    };
    let endpoint = lookup(relative)?;
    let root = !path.starts_with("/v1/");
    if path != "/voices" && root != (endpoint.api == A::RootRelay) {
        return None;
    }
    Some((endpoint, relative))
}

#[cfg(test)]
pub(super) fn media_kinds() -> impl Iterator<Item = (&'static str, &'static str)> {
    ENDPOINTS
        .iter()
        .filter(|e| e.pattern == P::MediaJobs)
        .map(|e| (e.path, e.op))
}

pub(super) fn media_kind(path: &str) -> Option<(&'static str, &'static str)> {
    let endpoint = lookup(path)?;
    (endpoint.pattern == P::MediaJobs).then_some((endpoint.path, endpoint.op))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_table_lookup_and_surfaces() {
        for (path, model, op) in [
            ("/embeddings", M::ExplicitRelay, "embeddings"),
            ("/moderations", M::Explicit, "moderations"),
        ] {
            let endpoint = lookup(path).unwrap();
            assert!(endpoint.relay && endpoint.tunnel);
            assert_eq!(endpoint.api, A::Relay);
            assert!(endpoint.model == model);
            assert_eq!(endpoint.op, op);
            assert!(endpoint.model_post(path));
            assert!(endpoint.allows("POST", path));
            assert!(!endpoint.allows("GET", path));
            assert!(!endpoint.post_body(path).accepts_multipart());
            assert_eq!(endpoint.post_body(path).cap(false), MAX_JSON_BODY_BYTES);
            assert_eq!(endpoint.deadline, D::Standard);
        }
        for (path, cap) in [
            ("/audio/translations", MAX_BODY_BYTES),
            ("/images/variations", MAX_IMAGE_EDIT_MULTIPART_BYTES),
        ] {
            let endpoint = lookup(path).unwrap();
            assert_eq!(endpoint.api, A::Relay);
            assert!(endpoint.relay && endpoint.tunnel && endpoint.model_post(path));
            assert!(endpoint.allows("POST", path) && !endpoint.allows("GET", path));
            assert!(endpoint.post_body(path).multipart_only());
            assert!(endpoint.post_body(path).raw_multipart());
            assert_eq!(endpoint.post_body(path).cap(true), cap);
        }
        let image = lookup("/images/edits").unwrap();
        assert!(
            image.post_body(image.path).raw_multipart()
                && image.post_body(image.path).legacy_multipart()
        );
        assert_eq!(
            image.post_body(image.path).cap(true),
            MAX_IMAGE_EDIT_MULTIPART_BYTES
        );
        assert_eq!(
            image.post_body(image.path).cap(false),
            MAX_IMAGE_EDIT_JSON_BYTES
        );
        assert_eq!(image.deadline, D::Image);
        assert_eq!(image.op, "image_edit");
        assert_eq!(lookup("/images/generations").unwrap().tunnel_op, "image");
        assert!(lookup("/models").unwrap().tunnel);
        assert!(!lookup("/models").unwrap().relay);
        assert!(!lookup("/audio/speech").unwrap().tunnel);
        assert_eq!(api_lookup("/v1/voices").unwrap().1, "/audio/voices");
        assert_eq!(api_lookup("/voices").unwrap().1, "/audio/voices");
        assert!(api_lookup("/health").is_some());
        assert!(api_lookup("/v1/health").is_none());
        assert!(api_lookup("/embeddings").is_none());
        let completions = lookup("/completions").unwrap();
        assert_eq!(completions.api, A::Completions);
        assert!(completions.tunnel && completions.model_post("/completions"));
        assert!(completions.allows("POST", "/completions"));
        assert!(!completions.allows("GET", "/completions"));
        assert_eq!(
            completions.post_body("/completions").cap(false),
            MAX_JSON_BODY_BYTES
        );
        let responses = lookup("/responses").unwrap();
        assert_eq!(responses.api, A::Responses);
        assert!(responses.tunnel && responses.model_post("/responses"));
        assert!(responses.allows("POST", "/responses"));
        assert!(!responses.allows("GET", "/responses"));
        assert_eq!(
            responses.post_body("/responses").cap(false),
            MAX_JSON_BODY_BYTES
        );
        assert!(lookup("/responses/response-id").is_none());
        for path in [
            "/unknown",
            "/embeddings/extra",
            "/moderations-extra",
            "/videosx",
        ] {
            assert!(lookup(path).is_none(), "{path}");
        }
        for (family, op) in media_kinds() {
            let content = format!("{family}/job-1/content?variant=preview");
            let endpoint = lookup(&content).unwrap();
            assert_eq!(endpoint.op, op);
            assert!(endpoint.allows("GET", &content));
            assert!(!endpoint.allows("DELETE", &content));
            assert!(!endpoint.model_post(&content));
            assert_eq!(media_kind(&content), Some((family, op)));
        }
    }
}
