# Building from source

## Requirements

- Rust (edition 2024) and git (to clone the
  [mistlib](https://github.com/tik-choco-lab/mistlib) and
  [mistlib-consensus](https://github.com/tik-choco-lab/mistlib-consensus)
  source repositories)
- [just](https://github.com/casey/just) (optional but recommended task runner)
- [cmake](https://cmake.org/) on PATH at build time (libopus is compiled from source
  for the relay's audio pipeline). With cmake ≥ 4.0, also set
  `CMAKE_POLICY_VERSION_MINIMUM=3.5` in the build environment.
- Optional: [ffmpeg](https://ffmpeg.org/) on PATH, only if you switch
  `stream.capture_backend` to `"ffmpeg"` (the default `"native"` backend has no
  external dependencies)

## Build

mistlib is a path dependency fetched into `.mistlib-src/` (a plain git clone,
not committed) by `scripts/fetch-mistlib`, configured through `.env`:

```console
$ cp .env.example .env        # defaults to the public mistlib; no credentials needed
$ just release                # fetches mistlib on first build, then cargo build --release
```

`MISTLIB_REPO`/`MISTLIB_REF` in `.env` select the source: the public
[mistlib](https://github.com/tik-choco-lab/mistlib) (`main`, the default) or
the private `mistlib-dev` (`develop`) for unreleased upstream commits — the
example file carries both, one commented out. Changing them re-points the
existing `.mistlib-src/` clone on the next fetch, so there is no need to delete
it. `MISTLIB_CONSENSUS_REPO`/`_REF` work the same way for
`.mistlib-consensus-src/`.

Without `just`: run `scripts/fetch-mistlib.sh` (or `scripts\fetch-mistlib.ps1`
on Windows) once, then `cargo build --release`. Re-run `just fetch-mistlib`
whenever you want to update to the latest `MISTLIB_REF`; the clone in
`.mistlib-src/` is a normal git checkout, so you can also edit, branch, and
push mistlib changes from there while developing against it.

### Building a Linux binary from Windows

```console
$ just release-linux           # builds via WSL2, packages to dist/mistl-linux-x86_64
```

This runs `cargo build` inside WSL2 against the same checkout rather than
true host cross-compiling, since mistlib links several C libraries
(openh264/opus via cmake, fdk-aac via cc, OpenSSL for reqwest's native-tls)
that need a real Linux toolchain/sysroot. Requires WSL2 with a distro that
has a Rust toolchain plus `build-essential cmake pkg-config libssl-dev`
(Ubuntu: `sudo apt install build-essential cmake pkg-config libssl-dev` and
[rustup](https://rustup.rs/)). The Linux build lands in
`target/x86_64-unknown-linux-gnu/release/`, kept separate from the Windows
build's `target/release/`.

## Releases

Releases are tag-driven: pushing a `v*` tag makes CI build every target and
publish a GitHub Release (a `-beta.N` tag becomes a prerelease, installed only
with `update.prerelease = true`). `just publish` computes and creates the tag:

```
$ just publish status                 # current tags and the next versions; changes nothing
$ just publish beta --push            # on develop: v0.1.16 -> v0.2.0-beta.1 -> v0.2.0-beta.2 ...
$ just publish stable --push          # on main: v0.2.0-beta.N -> v0.2.0
```
