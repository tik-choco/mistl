//! Explicit allowlist of feature assets. UI features own their views; the
//! dashboard shell owns routing, transport, and composition. Release builds
//! use only the embedded copies.

use std::borrow::Cow;

struct FeatureAsset {
    route: &'static str,
    content_type: &'static str,
    embedded: &'static [u8],
    #[cfg(debug_assertions)]
    relative_path: &'static str,
}

// Keep serving and the development fingerprint on the same allowlist.
macro_rules! feature_assets {
    ($($file:literal => $content_type:literal),* $(,)?) => {
        const FEATURES: &[FeatureAsset] = &[$(
            FeatureAsset {
                route: concat!("/assets/features/", $file),
                content_type: $content_type,
                embedded: include_bytes!(concat!("assets/features/", $file)),
                #[cfg(debug_assertions)]
                relative_path: concat!("features/", $file),
            },
        )*];
    };
}

feature_assets! {
    "authz.js" => "text/javascript; charset=utf-8",
    "authz.css" => "text/css; charset=utf-8",
    "identity.js" => "text/javascript; charset=utf-8",
    "identity.css" => "text/css; charset=utf-8",
    "tunnel-graph.js" => "text/javascript; charset=utf-8",
    "tunnel-graph.css" => "text/css; charset=utf-8",
    "topology.js" => "text/javascript; charset=utf-8",
    "topology.css" => "text/css; charset=utf-8",
    "nostr.js" => "text/javascript; charset=utf-8",
    "nostr.css" => "text/css; charset=utf-8",
    "escrow.js" => "text/javascript; charset=utf-8",
    "escrow.css" => "text/css; charset=utf-8",
    "storage.js" => "text/javascript; charset=utf-8",
    "storage.css" => "text/css; charset=utf-8",
    "chat.js" => "text/javascript; charset=utf-8",
    "chat.css" => "text/css; charset=utf-8",
    "messenger.js" => "text/javascript; charset=utf-8",
    "messenger.css" => "text/css; charset=utf-8",
    "peers.js" => "text/javascript; charset=utf-8",
    "peers.css" => "text/css; charset=utf-8",
}

fn find_feature(path: &str) -> Option<&'static FeatureAsset> {
    FEATURES.iter().find(|asset| asset.route == path)
}

#[cfg(debug_assertions)]
const ASSET_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src/web/assets");

/// Only construct disk paths from the fixed index name or an allowlisted asset.
#[cfg(debug_assertions)]
fn resolve_asset_path(root: &std::path::Path, route: &str) -> Option<std::path::PathBuf> {
    let relative = match route {
        "/" | "/index.html" => "index.html",
        _ => find_feature(route)?.relative_path,
    };
    Some(root.join(relative))
}

#[cfg(debug_assertions)]
fn index_html_from(root: &std::path::Path) -> Cow<'static, str> {
    resolve_asset_path(root, "/")
        .and_then(|path| std::fs::read_to_string(path).ok())
        .map(Cow::Owned)
        .unwrap_or(Cow::Borrowed(super::INDEX_HTML))
}

pub(super) fn index_html() -> Cow<'static, str> {
    #[cfg(debug_assertions)]
    {
        index_html_from(std::path::Path::new(ASSET_DIR))
    }
    #[cfg(not(debug_assertions))]
    {
        Cow::Borrowed(super::INDEX_HTML)
    }
}

#[cfg(debug_assertions)]
fn feature_asset_from(
    root: &std::path::Path,
    path: &str,
) -> Option<(&'static str, Cow<'static, [u8]>)> {
    let asset = find_feature(path)?;
    let body = resolve_asset_path(root, path)
        .and_then(|path| std::fs::read(path).ok())
        .map(Cow::Owned)
        .unwrap_or(Cow::Borrowed(asset.embedded));
    Some((asset.content_type, body))
}

pub(super) fn feature_asset(path: &str) -> Option<(&'static str, Cow<'static, [u8]>)> {
    #[cfg(debug_assertions)]
    {
        feature_asset_from(std::path::Path::new(ASSET_DIR), path)
    }
    #[cfg(not(debug_assertions))]
    {
        let asset = find_feature(path)?;
        Some((asset.content_type, Cow::Borrowed(asset.embedded)))
    }
}

/// Hash each allowlisted file's metadata so changes to any one file are seen,
/// even when another file has a later timestamp or the total size is unchanged.
#[cfg(debug_assertions)]
fn fingerprint_from(root: &std::path::Path) -> String {
    use std::hash::{Hash, Hasher};

    let mut hash = std::collections::hash_map::DefaultHasher::new();
    for route in std::iter::once("/").chain(FEATURES.iter().map(|asset| asset.route)) {
        route.hash(&mut hash);
        let metadata =
            resolve_asset_path(root, route).and_then(|path| std::fs::metadata(path).ok());
        if let Some(metadata) = metadata {
            true.hash(&mut hash);
            metadata.is_file().hash(&mut hash);
            metadata.len().hash(&mut hash);
            metadata.modified().ok().hash(&mut hash);
        } else {
            false.hash(&mut hash);
        }
    }
    format!("{:016x}", hash.finish())
}

#[cfg(debug_assertions)]
pub(super) fn fingerprint() -> String {
    fingerprint_from(std::path::Path::new(ASSET_DIR))
}

#[cfg(test)]
mod tests {
    use super::feature_asset;

    #[test]
    fn only_embedded_features_are_public() {
        for feature in [
            "authz",
            "identity",
            "topology",
            "storage",
            "escrow",
            "nostr",
            "chat",
            "tunnel-graph",
            "messenger",
            "peers",
        ] {
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

    #[cfg(debug_assertions)]
    mod debug {
        use super::super::{
            FEATURES, feature_asset_from, fingerprint_from, index_html_from, resolve_asset_path,
        };
        use std::borrow::Cow;
        use std::fs;
        use std::path::PathBuf;
        use std::time::{Duration, UNIX_EPOCH};

        struct TestDir(PathBuf);

        impl TestDir {
            fn new() -> Self {
                let path = std::env::temp_dir()
                    .join(format!("mistl-web-assets-{:016x}", rand::random::<u64>()));
                fs::create_dir_all(path.join("features")).unwrap();
                Self(path)
            }
        }

        impl Drop for TestDir {
            fn drop(&mut self) {
                let _ = fs::remove_dir_all(&self.0);
            }
        }

        #[test]
        fn paths_are_resolved_only_from_the_allowlist() {
            let root = std::path::Path::new("test-assets");
            assert_eq!(resolve_asset_path(root, "/"), Some(root.join("index.html")));
            assert_eq!(
                resolve_asset_path(root, "/index.html"),
                Some(root.join("index.html"))
            );
            for asset in FEATURES {
                assert_eq!(
                    resolve_asset_path(root, asset.route),
                    Some(root.join(asset.relative_path))
                );
            }
            for path in [
                "/assets/features/../../.env",
                "/assets/features/%2e%2e/index.html",
                "/assets/features/unknown.js",
                "/assets/features/authz.js/../identity.js",
                "/favicon.png",
            ] {
                assert_eq!(resolve_asset_path(root, path), None, "{path}");
            }
        }

        #[test]
        fn disk_assets_fall_back_to_embedded_copies() {
            let root = TestDir::new();
            assert!(matches!(index_html_from(&root.0), Cow::Borrowed(_)));
            let (_, embedded) = feature_asset_from(&root.0, "/assets/features/authz.js").unwrap();
            assert!(matches!(embedded, Cow::Borrowed(_)));
            assert!(feature_asset_from(&root.0, "/assets/features/missing.js").is_none());

            fs::write(root.0.join("index.html"), "<html>disk</html>").unwrap();
            fs::write(root.0.join("features/authz.js"), b"disk feature").unwrap();
            assert_eq!(index_html_from(&root.0), "<html>disk</html>");
            let (_, body) = feature_asset_from(&root.0, "/assets/features/authz.js").unwrap();
            assert_eq!(&*body, b"disk feature");

            fs::write(root.0.join("index.html"), [0xff]).unwrap();
            assert!(matches!(index_html_from(&root.0), Cow::Borrowed(_)));
        }

        #[test]
        fn fingerprint_tracks_each_allowlisted_file_and_ignores_others() {
            let root = TestDir::new();
            let initial = fingerprint_from(&root.0);
            assert_eq!(initial, fingerprint_from(&root.0));
            fs::write(root.0.join("other.txt"), "ignored").unwrap();
            assert_eq!(initial, fingerprint_from(&root.0));

            let index = root.0.join("index.html");
            let feature = root.0.join("features/authz.js");
            fs::write(&index, "index").unwrap();
            let index_added = fingerprint_from(&root.0);
            assert_ne!(initial, index_added);
            fs::write(&feature, "one").unwrap();
            let set_mtime = |path: &std::path::Path, seconds| {
                fs::OpenOptions::new()
                    .write(true)
                    .open(path)
                    .unwrap()
                    .set_times(
                        fs::FileTimes::new()
                            .set_modified(UNIX_EPOCH + Duration::from_secs(seconds)),
                    )
                    .unwrap();
            };
            set_mtime(&index, 1_900_000_000);
            set_mtime(&feature, 1_800_000_000);
            let before_edit = fingerprint_from(&root.0);
            fs::write(&feature, "two").unwrap();
            set_mtime(&feature, 1_800_000_001);
            let after_edit = fingerprint_from(&root.0);
            assert_ne!(before_edit, after_edit);
            fs::remove_file(feature).unwrap();
            assert_ne!(after_edit, fingerprint_from(&root.0));
        }
    }
}
