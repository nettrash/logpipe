//! os-driver — userspace daemon that exposes a writable FIFO (default
//! `/dev/os-driver`) and ships every line written to it into a configured
//! OpenSearch index.

mod batcher;
mod config;
mod event;
mod opensearch;
mod sources;

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use crate::config::Config;

#[derive(Parser, Debug)]
#[command(name = "os-driver", about = "Stream FIFO writes to OpenSearch")]
struct Cli {
    /// Path to the TOML configuration file.
    #[arg(short, long, default_value = "/etc/os-driver/config.toml")]
    config: PathBuf,

    /// Log level filter (overrides RUST_LOG).
    #[arg(long)]
    log_level: Option<String>,
}

fn init_tracing(level_override: Option<&str>) {
    let env_filter = match level_override {
        Some(level) => EnvFilter::new(level),
        None => EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
    };
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .init();
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.log_level.as_deref());

    info!(version = env!("CARGO_PKG_VERSION"), "os-driver starting");

    let cfg = Config::load(&cli.config)
        .with_context(|| format!("loading config from {}", cli.config.display()))?;
    info!(
        endpoint = %cfg.opensearch.endpoint,
        index    = %cfg.opensearch.index,
        fifo     = %cfg.sources.fifo.path.display(),
        "config loaded"
    );

    let host = hostname::get()
        .ok()
        .and_then(|s| s.into_string().ok())
        .unwrap_or_else(|| "unknown".to_string());

    let (tx, rx) = mpsc::channel(cfg.batch.max_events.saturating_mul(2).max(1024));
    let cancel = CancellationToken::new();

    let mut handles = Vec::new();

    // Batcher (sink).
    {
        let batch_cfg = cfg.batch.clone();
        let os_cfg = cfg.opensearch.clone();
        let cancel = cancel.clone();
        handles.push(tokio::spawn(async move {
            if let Err(err) = batcher::run(batch_cfg, os_cfg, rx, cancel).await {
                error!(error = %err, "batcher exited with error");
            }
        }));
    }

    // FIFO ingress.
    {
        let fifo_cfg = cfg.sources.fifo.clone();
        let cancel = cancel.clone();
        let host = host.clone();
        let tx = tx.clone();
        handles.push(tokio::spawn(async move {
            if let Err(err) = sources::fifo::run(fifo_cfg, host, tx, cancel).await {
                error!(error = %err, "fifo source exited with error");
            }
        }));
    }

    // Drop our local sender so the channel closes once the source ends.
    drop(tx);

    // Wait for SIGINT / SIGTERM, then cancel.
    let mut sigterm = signal(SignalKind::terminate()).context("install SIGTERM handler")?;
    let mut sigint = signal(SignalKind::interrupt()).context("install SIGINT handler")?;
    tokio::select! {
        _ = sigterm.recv() => info!("received SIGTERM, shutting down"),
        _ = sigint.recv()  => info!("received SIGINT, shutting down"),
    }
    cancel.cancel();

    // Give tasks a chance to flush gracefully.
    let shutdown_deadline = tokio::time::Duration::from_secs(10);
    let join_all = join_all_handles(handles);
    if tokio::time::timeout(shutdown_deadline, join_all)
        .await
        .is_err()
    {
        warn!("graceful shutdown timed out");
    }

    info!("os-driver stopped");
    Ok(())
}

/// Minimal `join_all` replacement so we don't pull in the `futures` crate.
async fn join_all_handles(handles: Vec<tokio::task::JoinHandle<()>>) {
    for h in handles {
        let _ = h.await;
    }
}
