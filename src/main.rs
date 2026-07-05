mod ai;
mod cli;
mod config;
mod daemon;
mod identity;
mod mailbox;
mod net;
mod storage;
mod stream;
mod web;

use anyhow::Result;
use clap::Parser;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "mistl=info".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    let args = cli::Cli::parse();
    cli::dispatch(args)
}
