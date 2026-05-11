//! Configuration loading and validation.
//!
//! Minimal example:
//!
//! ```toml
//! [opensearch]
//! endpoint = "https://opensearch.example.com:9200"
//! index    = "ubuntu-logs"
//! username = "ingest"
//! password = "change-me"
//! verify_tls = true
//!
//! [batch]
//! max_events = 500
//! flush_secs = 5
//!
//! [sources.fifo]
//! path = "/dev/logpipe"
//! mode = 0o622          # rw--w--w-: everyone can write, only daemon reads
//! tag  = "customer"     # optional default tag for every line
//! ```

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

/// Top-level configuration document.
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub opensearch: OpenSearchConfig,

    #[serde(default)]
    pub batch: BatchConfig,

    pub sources: SourcesConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OpenSearchConfig {
    /// Full base URL of the OpenSearch cluster, e.g. `https://host:9200`.
    pub endpoint: String,

    /// Index (or data-stream) name that events are written to.
    pub index: String,

    /// HTTP Basic-auth username.
    pub username: String,

    /// HTTP Basic-auth password.
    pub password: String,

    /// Verify the server TLS certificate. Defaults to `true`.
    #[serde(default = "default_true")]
    pub verify_tls: bool,

    /// Per-request timeout in seconds.
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BatchConfig {
    /// Maximum events accumulated before a forced flush.
    #[serde(default = "default_batch_max_events")]
    pub max_events: usize,

    /// Maximum time between flushes, regardless of fill level.
    #[serde(default = "default_batch_flush_secs")]
    pub flush_secs: u64,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_events: default_batch_max_events(),
            flush_secs: default_batch_flush_secs(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SourcesConfig {
    pub fifo: FifoConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FifoConfig {
    /// Filesystem path where the FIFO will live. Defaults to /dev/logpipe.
    #[serde(default = "default_fifo_path")]
    pub path: PathBuf,

    /// Permission bits applied on every startup (use octal in TOML:
    /// `mode = 0o622`). 0o622 = rw--w--w-, i.e. anyone can write, only the
    /// daemon reads.
    #[serde(default = "default_fifo_mode")]
    pub mode: u32,

    /// Default tag attached to every event from this FIFO. Customer-written
    /// JSON lines can override this via a `"tag"` field.
    #[serde(default)]
    pub tag: Option<String>,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading config file {}", path.display()))?;
        let cfg: Config = toml::from_str(&raw)
            .with_context(|| format!("parsing TOML config {}", path.display()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        if self.opensearch.endpoint.is_empty() {
            anyhow::bail!("opensearch.endpoint must be set");
        }
        if self.opensearch.index.is_empty() {
            anyhow::bail!("opensearch.index must be set");
        }
        if !(self.opensearch.endpoint.starts_with("http://")
            || self.opensearch.endpoint.starts_with("https://"))
        {
            anyhow::bail!(
                "opensearch.endpoint must start with http:// or https:// (got {})",
                self.opensearch.endpoint
            );
        }
        if self.sources.fifo.path.as_os_str().is_empty() {
            anyhow::bail!("sources.fifo.path must not be empty");
        }
        if self.sources.fifo.mode & !0o7777 != 0 {
            anyhow::bail!(
                "sources.fifo.mode {:#o} has bits outside the permission mask",
                self.sources.fifo.mode
            );
        }
        Ok(())
    }
}

fn default_true() -> bool {
    true
}

fn default_request_timeout_secs() -> u64 {
    30
}

fn default_batch_max_events() -> usize {
    500
}

fn default_batch_flush_secs() -> u64 {
    5
}

fn default_fifo_path() -> PathBuf {
    PathBuf::from("/dev/logpipe")
}

fn default_fifo_mode() -> u32 {
    0o622
}
