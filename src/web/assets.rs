//! Explicit allowlist of embedded feature assets. UI features own their views;
//! the dashboard shell owns routing, transport, and composition.
//! No runtime filesystem access or sibling application dependency is needed.

pub(super) fn feature_asset(path: &str) -> Option<(&'static str, &'static [u8])> {
    let (content_type, body): (&str, &[u8]) = match path {
        "/assets/features/tunnel-graph.js" => (
            "text/javascript; charset=utf-8",
            include_bytes!("assets/features/tunnel-graph.js"),
        ),
        "/assets/features/tunnel-graph.css" => (
            "text/css; charset=utf-8",
            include_bytes!("assets/features/tunnel-graph.css"),
        ),
        "/assets/features/topology.js" => (
            "text/javascript; charset=utf-8",
            include_bytes!("assets/features/topology.js"),
        ),
        "/assets/features/topology.css" => (
            "text/css; charset=utf-8",
            include_bytes!("assets/features/topology.css"),
        ),
        "/assets/features/storage.js" => (
            "text/javascript; charset=utf-8",
            include_bytes!("assets/features/storage.js"),
        ),
        "/assets/features/storage.css" => (
            "text/css; charset=utf-8",
            include_bytes!("assets/features/storage.css"),
        ),
        "/assets/features/chat.js" => (
            "text/javascript; charset=utf-8",
            include_bytes!("assets/features/chat.js"),
        ),
        "/assets/features/chat.css" => (
            "text/css; charset=utf-8",
            include_bytes!("assets/features/chat.css"),
        ),
        _ => return None,
    };
    Some((content_type, body))
}

#[cfg(test)]
mod tests {
    use super::feature_asset;

    #[test]
    fn only_embedded_features_are_public() {
        for feature in ["topology", "storage", "chat", "tunnel-graph"] {
            for extension in ["js", "css"] {
                let (mime, bytes) =
                    feature_asset(&format!("/assets/features/{feature}.{extension}")).unwrap();
                assert!(!bytes.is_empty());
                assert!(mime.contains(if extension == "js" {
                    "javascript"
                } else {
                    "css"
                }));
            }
        }
        for path in [
            "/assets/../index.html",
            "/assets/features/../../.env",
            "/assets/features/missing.js",
        ] {
            assert!(feature_asset(path).is_none());
        }
    }
}
