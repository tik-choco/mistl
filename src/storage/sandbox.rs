//! Path-jailed content sandbox: confines every read/write to files that live
//! under a single directory ("the sandbox root"), rejecting any name that is
//! absolute, `.`, `..`, or that otherwise resolves outside the root once
//! joined onto it. Callers only ever address content by a *sandbox-relative*
//! name -- returned names are forward-slash normalized on every platform,
//! and accepted with either separator on input -- so the absolute
//! filesystem root is never exposed for indexing by outside code.
//!
//! Ported from tc-storage's `storage-cli` `internal/sandbox/sandbox.go`,
//! same semantics, adapted idiomatically to Rust: [`Sandbox::resolve`]
//! validates a requested name by walking [`std::path::Component`]s and
//! rejecting any `ParentDir` (`..`), `RootDir`, or `Prefix` component,
//! rather than string-matching on the output of `filepath.Clean` the way
//! the Go original does. One behavioral consequence: this is *stricter*
//! than the Go version for a path like `sub/../file.txt` -- Go's
//! `filepath.Clean` collapses that to `file.txt` (which stays under root
//! and would be allowed), whereas here any `..` component anywhere in the
//! input is rejected outright, even one that would have lexically
//! cancelled out. That is an intentional simplification: this sandbox never
//! has a legitimate reason to accept `..` in a request, so rejecting it
//! unconditionally is both simpler and safer.

use std::fs;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};

/// Message used for every "the requested path is not under the sandbox
/// root" error, so callers can match on it via [`is_outside_sandbox`]
/// without needing a dedicated error type.
pub const OUTSIDE_SANDBOX: &str = "path escapes content sandbox";

/// True if `err` is the "path escapes the sandbox root" error produced by
/// [`Sandbox::resolve`] (and, transitively, everything built on it).
///
/// Part of the retained sandbox API (used by the module's own tests); the
/// daemon's `store.sandbox.*` commands surface the error text directly.
#[allow(dead_code)]
pub fn is_outside_sandbox(err: &anyhow::Error) -> bool {
    err.to_string() == OUTSIDE_SANDBOX
}

/// A directory that all imports/reads/removals/listings are confined to.
///
/// Construct with [`Sandbox::new`]; every other method takes a
/// sandbox-relative name and refuses to touch anything outside
/// [`Sandbox::root`].
pub struct Sandbox {
    root: PathBuf,
}

impl Sandbox {
    /// Open (creating it and any missing parents if needed) a sandbox
    /// rooted at `root`. Rejects an empty or all-whitespace root.
    ///
    /// `root` is absolutized, not canonicalized: like Go's `filepath.Abs`,
    /// this is a purely lexical operation -- it does not touch the
    /// filesystem, require the path to already exist, or resolve symlinks.
    /// The directory is created only after the absolute path is computed,
    /// mirroring the Go reference's `Abs` then `MkdirAll` order.
    pub fn new(root: impl AsRef<Path>) -> Result<Sandbox> {
        let root = root.as_ref();
        if root.to_string_lossy().trim().is_empty() {
            bail!("sandbox root is required");
        }
        let abs = std::path::absolute(root)
            .with_context(|| format!("resolving sandbox root {}", root.display()))?;
        fs::create_dir_all(&abs)
            .with_context(|| format!("creating sandbox root {}", abs.display()))?;
        Ok(Sandbox { root: abs })
    }

    /// The sandbox's absolute root directory.
    #[allow(dead_code)] // retained sandbox API (parity with storage-cli / future TUI)
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Resolve a sandbox-relative `name` to an absolute path under
    /// [`Self::root`], rejecting anything that is empty, absolute, or that
    /// escapes the root (via `..`, a bare drive/UNC prefix, or a rooted
    /// path with no prefix such as Windows `\foo`).
    ///
    /// Accepts either `/` or `\` as a separator on any platform (both are
    /// recognized as separators by [`std::path::Path`] on Windows; only
    /// `/` is a separator on Unix, so a literal backslash there is just an
    /// ordinary filename character, exactly as `std::path` already treats
    /// it).
    pub fn resolve(&self, name: &str) -> Result<PathBuf> {
        if name.trim().is_empty() {
            bail!("sandbox path is required");
        }

        let requested = Path::new(name);
        if requested.is_absolute() {
            return Err(anyhow!(OUTSIDE_SANDBOX));
        }

        let mut normalized = PathBuf::new();
        for component in requested.components() {
            match component {
                Component::Normal(part) => normalized.push(part),
                // "." contributes nothing; a bare "." (or "./") ends up
                // with an empty `normalized` and is rejected below.
                Component::CurDir => {}
                Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                    return Err(anyhow!(OUTSIDE_SANDBOX));
                }
            }
        }

        if normalized.as_os_str().is_empty() {
            return Err(anyhow!(OUTSIDE_SANDBOX));
        }

        let target = self.root.join(&normalized);
        // Final defense-in-depth guard: normalized contains only `Normal`
        // components, so this should always hold, but confirm it rather
        // than trust that invariant blindly.
        if !target.starts_with(&self.root) {
            return Err(anyhow!(OUTSIDE_SANDBOX));
        }

        Ok(target)
    }

    /// Import an external file into the sandbox root, using its base file
    /// name as the target (sandbox-relative) name. Equivalent to
    /// [`Self::import_file_to_dir`] with an empty `dir`.
    pub fn import_file(&self, source: &str) -> Result<String> {
        self.import_file_to_dir(source, "")
    }

    /// Import an external file into the sandbox under `dir` (a
    /// sandbox-relative directory; ignored if empty/whitespace), using its
    /// base file name as the target file name. Creates any missing parent
    /// directories and overwrites an existing file at the target. Returns
    /// the forward-slash sandbox-relative path the file was written to
    /// (e.g. `sub/name.txt`).
    pub fn import_file_to_dir(&self, source: &str, dir: &str) -> Result<String> {
        let source_path = Path::new(source);
        let metadata = fs::metadata(source_path).with_context(|| format!("stat {source}"))?;
        if metadata.is_dir() {
            bail!("directories cannot be imported yet: {source}");
        }
        let file_name = source_path
            .file_name()
            .ok_or_else(|| anyhow!("source has no file name: {source}"))?;

        let mut rel = PathBuf::new();
        if !dir.trim().is_empty() {
            rel.push(dir);
        }
        rel.push(file_name);
        let rel_str = rel.to_string_lossy().into_owned();

        let target = self.resolve(&rel_str)?;
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
        }
        fs::copy(source_path, &target)
            .with_context(|| format!("copying {source} to {}", target.display()))?;

        Ok(to_forward_slash(&rel))
    }

    /// Read a sandbox-relative file's full contents, returning the bytes
    /// alongside its size in bytes. Errors if `name` resolves to a
    /// directory rather than a file.
    #[allow(dead_code)] // retained sandbox API (parity with storage-cli / future TUI)
    pub fn read_file(&self, name: &str) -> Result<(Vec<u8>, u64)> {
        let path = self.resolve(name)?;
        let metadata = fs::metadata(&path).with_context(|| format!("stat {}", path.display()))?;
        if metadata.is_dir() {
            bail!("not a file: {name}");
        }
        let data = fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
        let size = metadata.len();
        Ok((data, size))
    }

    /// Remove a sandbox-relative file or directory (recursively). Unlike
    /// Go's `os.RemoveAll`, missing paths are treated as errors by
    /// `std::fs`'s remove functions, so this checks existence first and
    /// treats "already gone" as success, matching `RemoveAll`'s
    /// idempotence.
    #[allow(dead_code)] // retained sandbox API (parity with storage-cli / future TUI)
    pub fn remove(&self, name: &str) -> Result<()> {
        let path = self.resolve(name)?;
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.is_dir() => {
                fs::remove_dir_all(&path).with_context(|| format!("removing {}", path.display()))
            }
            Ok(_) => fs::remove_file(&path).with_context(|| format!("removing {}", path.display())),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err).with_context(|| format!("stat {}", path.display())),
        }
    }

    /// Recursively list every file under the sandbox root (directories and
    /// the root itself are not included), as forward-slash sandbox-relative
    /// paths sorted lexicographically.
    pub fn list(&self) -> Result<Vec<String>> {
        let mut entries = Vec::new();
        walk(&self.root, &self.root, &mut entries)?;
        entries.sort();
        Ok(entries)
    }
}

/// Recursive helper backing [`Sandbox::list`]: walks `dir` (starting at
/// `root`), appending every file's forward-slash path (relative to `root`)
/// to `entries`. Directories are recursed into but never themselves added.
fn walk(root: &Path, dir: &Path, entries: &mut Vec<String>) -> Result<()> {
    for entry in
        fs::read_dir(dir).with_context(|| format!("reading directory {}", dir.display()))?
    {
        let entry = entry.with_context(|| format!("reading entry in {}", dir.display()))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .with_context(|| format!("stat {}", path.display()))?;
        if file_type.is_dir() {
            walk(root, &path, entries)?;
        } else {
            let rel = path.strip_prefix(root).unwrap_or(path.as_path());
            entries.push(to_forward_slash(rel));
        }
    }
    Ok(())
}

/// Render a path as a forward-slash string regardless of platform, so
/// sandbox-relative names returned to callers are stable across Windows and
/// Unix (mirrors Go's `filepath.ToSlash`).
fn to_forward_slash(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal self-cleaning temp directory (mirrors the helper in
    /// `src/storage/mod.rs`; the `tempfile` crate is only a transitive
    /// dependency here, not one mistl depends on directly).
    struct TempDir(PathBuf);

    impl TempDir {
        fn new(label: &str) -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "mistl-sandbox-test-{label}-{}-{nanos}-{n}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).expect("create temp dir");
            Self(path)
        }

        fn path(&self) -> PathBuf {
            self.0.clone()
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// Write `contents` to `dir/name` (used to create files "outside" the
    /// sandbox, to be imported in) and return its path.
    fn write_source_file(dir: &Path, name: &str, contents: &[u8]) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, contents).expect("write source file");
        path
    }

    #[test]
    fn import_file_copies_source_and_lists_it() {
        let root = TempDir::new("import-root");
        let outside = TempDir::new("import-src");
        let sandbox = Sandbox::new(root.path()).expect("new sandbox");

        let source = write_source_file(&outside.path(), "name.txt", b"hello sandbox");
        let rel = sandbox
            .import_file(source.to_str().unwrap())
            .expect("import file");
        assert_eq!(rel, "name.txt");

        let copied = std::fs::read(sandbox.root().join("name.txt")).unwrap();
        assert_eq!(copied, b"hello sandbox");

        let listed = sandbox.list().expect("list");
        assert_eq!(listed, vec!["name.txt"]);
    }

    #[test]
    fn read_file_returns_bytes_and_size() {
        let root = TempDir::new("read-root");
        let outside = TempDir::new("read-src");
        let sandbox = Sandbox::new(root.path()).unwrap();

        let source = write_source_file(&outside.path(), "data.bin", b"0123456789");
        sandbox.import_file(source.to_str().unwrap()).unwrap();

        let (bytes, size) = sandbox.read_file("data.bin").unwrap();
        assert_eq!(bytes, b"0123456789");
        assert_eq!(size, 10);
    }

    #[test]
    fn resolve_rejects_absolute_traversal_dot_and_empty() {
        let root = TempDir::new("resolve-root");
        let sandbox = Sandbox::new(root.path()).unwrap();

        // Absolute path (built from the already-absolute temp dir path, so
        // this is a valid absolute path on whatever platform tests run on).
        let absolute = root.path().join("outside.txt");
        let err = sandbox.resolve(absolute.to_str().unwrap()).unwrap_err();
        assert!(is_outside_sandbox(&err), "absolute path should be rejected");

        // Parent-directory traversal.
        let err = sandbox.resolve("../evil.txt").unwrap_err();
        assert!(
            is_outside_sandbox(&err),
            "`..` traversal should be rejected"
        );

        // Bare current-directory component.
        let err = sandbox.resolve(".").unwrap_err();
        assert!(is_outside_sandbox(&err), "bare `.` should be rejected");

        // Empty name: a distinct error (not `OUTSIDE_SANDBOX`), but still an
        // error.
        let err = sandbox.resolve("").unwrap_err();
        assert!(!is_outside_sandbox(&err));
        assert!(err.to_string().contains("required"));
    }

    #[test]
    fn import_file_to_dir_places_file_under_subdir() {
        let root = TempDir::new("subdir-root");
        let outside = TempDir::new("subdir-src");
        let sandbox = Sandbox::new(root.path()).unwrap();

        let source = write_source_file(&outside.path(), "name.txt", b"nested");
        let rel = sandbox
            .import_file_to_dir(source.to_str().unwrap(), "sub")
            .unwrap();
        assert_eq!(rel, "sub/name.txt");

        let listed = sandbox.list().unwrap();
        assert_eq!(listed, vec!["sub/name.txt"]);
    }

    #[test]
    fn remove_deletes_a_previously_imported_file() {
        let root = TempDir::new("remove-root");
        let outside = TempDir::new("remove-src");
        let sandbox = Sandbox::new(root.path()).unwrap();

        let source = write_source_file(&outside.path(), "gone.txt", b"bye");
        sandbox.import_file(source.to_str().unwrap()).unwrap();
        assert_eq!(sandbox.list().unwrap(), vec!["gone.txt"]);

        sandbox.remove("gone.txt").unwrap();
        assert!(sandbox.list().unwrap().is_empty());
    }

    #[test]
    fn list_returns_entries_sorted_lexicographically() {
        let root = TempDir::new("sort-root");
        let outside = TempDir::new("sort-src");
        let sandbox = Sandbox::new(root.path()).unwrap();

        for (dir, file) in [("", "zeta.txt"), ("", "alpha.txt"), ("mid", "beta.txt")] {
            let source = write_source_file(&outside.path(), file, file.as_bytes());
            sandbox
                .import_file_to_dir(source.to_str().unwrap(), dir)
                .unwrap();
        }

        let listed = sandbox.list().unwrap();
        assert_eq!(listed, vec!["alpha.txt", "mid/beta.txt", "zeta.txt"]);

        let mut sorted = listed.clone();
        sorted.sort();
        assert_eq!(listed, sorted, "list() must already be sorted");
    }
}
