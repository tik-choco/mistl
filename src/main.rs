mod agent;
mod ai;
mod bot;
mod chat_relay;
mod child_process;
mod cli;
mod config;
mod consensus;
mod daemon;
mod devlog;
mod dm;
mod escrow;
mod identity;
mod install;
mod net;
mod network;
mod runtime;
mod scheduler;
mod statefile;
mod storage;
mod stream;
mod topology;
mod tray;
mod tunnel;
mod update;
mod web;
mod wiresign;

use anyhow::Result;
use clap::Parser;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error:#}");
            std::process::ExitCode::FAILURE
        }
    }
}

fn run() -> Result<()> {
    let args = cli::Cli::parse();
    // `mistl agent` draws a TUI on the terminal; stray log lines on stderr
    // would corrupt it, so it logs to stderr only when RUST_LOG asks for it.
    let default_filter = if matches!(args.command, Some(cli::Command::Agent(_))) {
        "off"
    } else {
        "mistl=info"
    };
    let stderr_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| default_filter.into()));
    // Always captures debug detail into the dashboard's ring buffer,
    // independent of the stderr log's own level -- so the "developer mode"
    // toggle in the dashboard shows detail immediately with no restart.
    let devlog_layer = devlog::DevLogLayer.with_filter(EnvFilter::new(
        "mistl=debug,mistlib_core=debug,mistlib_native=debug",
    ));

    tracing_subscriber::registry()
        .with(stderr_layer)
        .with(devlog_layer)
        .init();

    let mistlib_build: serde_json::Value =
        serde_json::from_str(&mistlib::build_info::get_build_info()).unwrap_or_default();
    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        channel = runtime::CHANNEL,
        build = runtime::BUILD_ID.get(..12).unwrap_or(runtime::BUILD_ID),
        commit = env!("MISTL_COMMIT"),
        mistlib_version = mistlib::build_info::get_version(),
        mistlib_commit = mistlib_build["commit"].as_str().unwrap_or("unknown"),
        "mistl starting"
    );

    runtime::initialize(args.instance.clone(), args.state_dir.clone(), args.no_tray)?;
    cli::dispatch(args)
}
