//! FIFO (named-pipe) ingress.
//!
//! On startup the daemon makes sure a FIFO exists at the configured path
//! (default `/dev/os-driver`) and chmods it to the configured mode so that
//! customer processes can simply do:
//!
//! ```sh
//! echo "hello world" > /dev/os-driver
//! # or:
//! cat my.log         > /dev/os-driver
//! # or programmatically:
//! open("/dev/os-driver", O_WRONLY)
//! ```
//!
//! Each newline-terminated line becomes one OpenSearch document.
//!
//! ## Why O_RDWR
//!
//! POSIX semantics say that when the last *writer* of a FIFO closes its
//! handle, readers see EOF. That would force us to reopen the FIFO between
//! every customer write. Linux lets us open a FIFO `O_RDWR` so that the
//! daemon is itself counted as a writer; that means the read side never
//! observes EOF and we can keep a single long-lived reader regardless of
//! how many customer processes come and go.

use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::Path;

use anyhow::{Context, Result};
use serde_json::{Map, Value};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::EventTx;
use crate::config::FifoConfig;
use crate::event::Event;

pub async fn run(
    cfg: FifoConfig,
    host: String,
    tx: EventTx,
    cancel: CancellationToken,
) -> Result<()> {
    ensure_fifo(&cfg.path, cfg.mode)?;
    info!(
        path = %cfg.path.display(),
        mode = format!("{:#o}", cfg.mode),
        "fifo ingress ready"
    );

    // O_RDWR keeps the FIFO open from our side even when no customer is
    // currently writing — otherwise we'd see EOF on every disconnect.
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&cfg.path)
        .await
        .with_context(|| format!("opening fifo {}", cfg.path.display()))?;

    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    let default_tag = cfg.tag.clone();
    let fifo_path = cfg.path.clone();

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("fifo source cancelled");
                return Ok(());
            }
            line = lines.next_line() => {
                match line {
                    Ok(Some(line)) => {
                        if line.is_empty() { continue; }
                        let event = build_event(&line, default_tag.as_deref(), &host, &fifo_path);
                        if tx.send(event).await.is_err() {
                            info!("event channel closed, fifo source exiting");
                            return Ok(());
                        }
                    }
                    Ok(None) => {
                        // Should not happen because we hold the FIFO O_RDWR,
                        // but treat it as a recoverable signal: bail out.
                        warn!("fifo unexpectedly reported EOF; exiting source");
                        return Ok(());
                    }
                    Err(err) => return Err(err.into()),
                }
            }
        }
    }
}

/// Make sure the FIFO exists with the requested permissions.
///
/// * If the path doesn't exist, `mkfifo(2)` it.
/// * If something is already there and *isn't* a FIFO, bail out — the admin
///   probably pointed us at the wrong path.
/// * Always (re)apply the configured mode so the admin can adjust access by
///   editing the config and restarting.
fn ensure_fifo(path: &Path, mode: u32) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating parent dir {}", parent.display()))?;
        }
    }

    match std::fs::symlink_metadata(path) {
        Ok(meta) => {
            if !meta.file_type().is_fifo() {
                anyhow::bail!(
                    "{} exists but is not a FIFO; remove it or choose a different path",
                    path.display()
                );
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            let c_path = CString::new(path.as_os_str().as_bytes())
                .context("fifo path contains an interior NUL byte")?;
            // SAFETY: c_path is a valid NUL-terminated C string for the
            // lifetime of the call.
            let rc = unsafe { libc::mkfifo(c_path.as_ptr(), mode as libc::mode_t) };
            if rc != 0 {
                return Err(std::io::Error::last_os_error()).with_context(|| {
                    format!(
                        "mkfifo({}) failed — the daemon's user needs write access to the parent directory",
                        path.display()
                    )
                });
            }
            info!(path = %path.display(), "created fifo");
        }
        Err(err) => return Err(err).context("stat-ing fifo path"),
    }

    let perms = std::fs::Permissions::from_mode(mode);
    std::fs::set_permissions(path, perms)
        .with_context(|| format!("setting permissions on {}", path.display()))?;

    Ok(())
}

/// Build an Event from a customer-written line.
///
/// If the line happens to be a JSON object, lift its fields directly into
/// the OpenSearch document (with `message` and `tag` promoted). Otherwise
/// treat the entire line as a free-form message.
fn build_event(line: &str, default_tag: Option<&str>, host: &str, fifo: &Path) -> Event {
    let timestamp = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_default();

    let mut fields = Map::new();
    fields.insert(
        "fifo".to_string(),
        Value::String(fifo.display().to_string()),
    );

    if let Ok(Value::Object(obj)) = serde_json::from_str::<Value>(line) {
        let mut tag = default_tag.map(str::to_owned);
        let mut message: Option<String> = None;
        for (k, v) in obj {
            if k == "message" {
                if let Value::String(s) = &v {
                    message = Some(s.clone());
                }
            } else if k == "tag" {
                if let Value::String(s) = &v {
                    tag = Some(s.clone());
                }
            }
            fields.insert(k, v);
        }
        return Event {
            timestamp,
            source: "fifo",
            host: host.to_owned(),
            tag,
            message,
            fields,
        };
    }

    Event {
        timestamp,
        source: "fifo",
        host: host.to_owned(),
        tag: default_tag.map(str::to_owned),
        message: Some(line.to_owned()),
        fields,
    }
}
