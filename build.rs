// Expose the Cargo build target triple (e.g. "x86_64-pc-windows-msvc") to the
// program as `env!("MISTL_TARGET")`. The self-updater uses it to pick the
// matching release asset (assets are named `mistl-v<ver>-<target>[.exe]`).
fn main() {
    let target = std::env::var("TARGET").unwrap_or_default();
    println!("cargo:rustc-env=MISTL_TARGET={target}");
    println!("cargo:rerun-if-changed=build.rs");
}
