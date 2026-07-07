mod ai;
mod cli;
mod config;
mod consensus;
mod daemon;
mod devlog;
mod identity;
mod install;
mod mailbox;
mod net;
mod storage;
mod stream;
mod update;
mod web;

use anyhow::Result;
use clap::Parser;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

fn main() -> Result<()> {
    let stderr_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| "mistl=info".into()),
        );
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

    let args = cli::Cli::parse();
    cli::dispatch(args)
}
