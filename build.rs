use sha2::{Digest, Sha256};
use std::{env, fs, path::Path, process::Command};

fn hash_tree(path: &Path, hash: &mut Sha256) {
    if path.is_dir() {
        let mut paths: Vec<_> = fs::read_dir(path)
            .expect("reading build inputs")
            .map(|e| e.expect("build input").path())
            .collect();
        paths.sort();
        for p in paths {
            if matches!(
                p.file_name().and_then(|s| s.to_str()),
                Some(".git" | "target")
            ) {
                continue;
            }
            hash_tree(&p, hash);
        }
    } else if path.is_file() {
        hash.update(path.to_string_lossy().as_bytes());
        hash.update([0]);
        hash.update(fs::read(path).expect("reading build input"));
    }
}

fn git(args: &[&str]) -> String {
    Command::new("git")
        .args(args)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .unwrap_or_default()
}

fn main() {
    let channel = env::var("MISTL_BUILD_CHANNEL").unwrap_or_else(|_| "dev".into());
    assert!(
        matches!(channel.as_str(), "dev" | "preview" | "stable"),
        "invalid MISTL_BUILD_CHANNEL"
    );
    let target = env::var("TARGET").unwrap_or_default();
    let profile = env::var("PROFILE").unwrap_or_default();
    let commit = git(&["rev-parse", "HEAD"]);
    let dirty = !git(&["status", "--porcelain", "--untracked-files=normal"]).is_empty();
    let mut hash = Sha256::new();
    for input in [
        "src",
        "Cargo.toml",
        "Cargo.lock",
        "build.rs",
        ".cargo/config.toml",
        ".mistlib-src/mistlib-core",
        ".mistlib-src/mistlib-native",
        ".mistlib-consensus-src/mistlib-consensus-core",
        ".mistlib-consensus-src/mistlib-consensus-native",
    ] {
        println!("cargo:rerun-if-changed={input}");
        hash_tree(Path::new(input), &mut hash);
    }
    for input in [
        &channel,
        &target,
        &profile,
        &commit,
        &env::var("CARGO_ENCODED_RUSTFLAGS").unwrap_or_default(),
    ] {
        hash.update(input.as_bytes());
        hash.update([0]);
    }
    let build_id = format!("{:x}", hash.finalize());
    let workspace_id = format!(
        "{:x}",
        Sha256::digest(env::var("CARGO_MANIFEST_DIR").unwrap().as_bytes())
    );
    for (key, value) in [
        ("TARGET", target),
        ("CHANNEL", channel),
        ("PROFILE", profile),
        (
            "DIRTY",
            if commit.is_empty() {
                "unknown".into()
            } else {
                dirty.to_string()
            },
        ),
        ("COMMIT", commit),
        ("BUILD_ID", build_id),
        ("WORKSPACE_ID", workspace_id[..12].to_string()),
    ] {
        println!("cargo:rustc-env=MISTL_{key}={value}");
    }
    println!("cargo:rerun-if-env-changed=MISTL_BUILD_CHANNEL");
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/index");
}
