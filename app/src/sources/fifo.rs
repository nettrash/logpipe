//! FIFO (named-pipe) ingress.
//!
//! On startup the daemon makes sure a FIFO exists at the configured path
//! (default `/dev/logpipe`) and chmods it to the configured mode so that
//! customer processes can simply do:
//!
//! ```sh
//! echo "hello world" > /dev/logpipe
//! # or:
//! cat my.log         > /dev/logpipe
//! # or programmatically:
//! open("/dev/logpipe", O_WRONLY)
//! ```
//!
//! Each newline-terminated line becomes one OpenSearch document.
//!
//! ## Why O_RDWR | O_NONBLOCK
//!
//! POSIX semantics say that when the last *writer* of a FIFO closes its
//! handle, readers see EOF. That would force us to reopen the FIFO between
//! every customer write. Linux lets us open a FIFO `O_RDWR` so that the
//! daemon is itself counted as a writer; that means the read side never
//! observes EOF and we can keep a single long-lived reader regardless of how
//! many customer processes come and go.
//!
//! We additionally open it `O_NONBLOCK` and drive it through epoll via
//! [`AsyncFd`] rather than going through `tokio::fs`. `tokio::fs` performs the
//! underlying `read(2)` on the runtime's blocking thread pool; since the FIFO
//! is held open `O_RDWR` that read parks forever whenever no customer is
//! writing, and cancelling the task only drops the future — the syscall keeps
//! running and wedges runtime shutdown (and, in turn, `systemctl stop`). With
//! a non-blocking fd a cancelled read is genuinely cancelled.

use std::ffi::CString;
use std::io::{self, Read};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, OpenOptionsExt, PermissionsExt};
use std::path::Path;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};

use anyhow::{Context, Result};
use serde_json::{Map, Value};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader, ReadBuf};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::EventTx;
use crate::config::FifoConfig;
use crate::event::Event;

/// Async wrapper around a non-blocking FIFO fd, driven by the Tokio reactor's
/// epoll registration rather than the blocking thread pool. See the module
/// docs for why this matters for clean shutdown.
struct AsyncFifo {
    inner: AsyncFd<std::fs::File>,
}

impl AsyncFifo {
    fn new(file: std::fs::File) -> io::Result<Self> {
        Ok(Self {
            inner: AsyncFd::new(file)?,
        })
    }
}

impl AsyncRead for AsyncFifo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            let mut guard = match this.inner.poll_read_ready(cx) {
                Poll::Ready(Ok(guard)) => guard,
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Pending => return Poll::Pending,
            };
            let unfilled = buf.initialize_unfilled();
            match guard.try_io(|inner| inner.get_ref().read(unfilled)) {
                Ok(Ok(n)) => {
                    buf.advance(n);
                    return Poll::Ready(Ok(()));
                }
                Ok(Err(err)) => return Poll::Ready(Err(err)),
                // `try_io` already cleared readiness on WouldBlock; loop back
                // to `poll_read_ready`, which will now return `Pending`.
                Err(_would_block) => continue,
            }
        }
    }
}

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
    // currently writing (otherwise we'd see EOF on every disconnect);
    // O_NONBLOCK lets the reactor poll it instead of parking a blocking thread.
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(&cfg.path)
        .with_context(|| format!("opening fifo {}", cfg.path.display()))?;

    let reader =
        BufReader::new(AsyncFifo::new(file).with_context(|| {
            format!("registering fifo {} with the reactor", cfg.path.display())
        })?);
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

/// Keys the daemon writes itself onto every document. A customer JSON line
/// that also contains one of these must not have its copy merged into the
/// flattened `fields` map — the resulting `_bulk` document would carry the
/// key twice, and OpenSearch rejects any document with duplicate keys.
const RESERVED_KEYS: &[&str] = &["@timestamp", "source", "host", "tag", "message", "fifo"];

/// Build an Event from a customer-written line.
///
/// If the line is a JSON object, its fields are merged into the OpenSearch
/// document, with a string `message` / `tag` promoted to the document's
/// top-level fields. Keys the daemon owns ([`RESERVED_KEYS`]) are dropped
/// rather than merged, so the emitted document never has a duplicate key.
/// Anything that isn't a JSON object becomes a free-form `message`.
fn build_event(line: &str, default_tag: Option<&str>, host: &str, fifo: &Path) -> Event {
    let timestamp = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_default();

    let mut fields = Map::new();

    if let Ok(Value::Object(obj)) = serde_json::from_str::<Value>(line) {
        let mut tag = default_tag.map(str::to_owned);
        let mut message: Option<String> = None;
        for (k, v) in obj {
            match k.as_str() {
                // Promote when it's a string; otherwise keep it as a field
                // (Event.message/tag stays None, so still only one key).
                "message" => match v {
                    Value::String(s) => message = Some(s),
                    other => {
                        fields.insert(k, other);
                    }
                },
                "tag" => match v {
                    Value::String(s) => tag = Some(s),
                    other => {
                        fields.insert(k, other);
                    }
                },
                // Other daemon-owned keys: ignore the customer's copy.
                _ if RESERVED_KEYS.contains(&k.as_str()) => {}
                _ => {
                    fields.insert(k, v);
                }
            }
        }
        fields.insert(
            "fifo".to_string(),
            Value::String(fifo.display().to_string()),
        );
        return Event {
            timestamp,
            source: "fifo",
            host: host.to_owned(),
            tag,
            message,
            fields,
        };
    }

    fields.insert(
        "fifo".to_string(),
        Value::String(fifo.display().to_string()),
    );
    Event {
        timestamp,
        source: "fifo",
        host: host.to_owned(),
        tag: default_tag.map(str::to_owned),
        message: Some(line.to_owned()),
        fields,
    }
}
