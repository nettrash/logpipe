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

    /// Capacity of the in-process channel that connects the FIFO reader
    /// to the OpenSearch flusher. When full, the FIFO reader blocks —
    /// which then translates into kernel-pipe backpressure on customer
    /// writers. Bigger values absorb longer slow-sink stalls at the cost
    /// of extra memory (~one Event per slot). When unset, defaults to
    /// `max_events * 8`, which gives the flusher room to keep one batch
    /// in flight while several more accumulate behind it.
    #[serde(default)]
    pub channel_capacity: Option<usize>,
}

impl BatchConfig {
    /// Effective channel capacity: respects `channel_capacity` if set,
    /// otherwise derives one from `max_events`.
    pub fn effective_channel_capacity(&self) -> usize {
        self.channel_capacity
            .unwrap_or_else(|| self.max_events.saturating_mul(8))
            .max(1)
    }
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_events: default_batch_max_events(),
            flush_secs: default_batch_flush_secs(),
            channel_capacity: None,
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

    /// When `true`, every merged field value is coerced to a JSON-encoded
    /// string before it reaches OpenSearch (e.g. `42` → `"42"`,
    /// `{"a":1}` → `"{\"a\":1}"`). Defaults to `false`, which preserves the
    /// original JSON types. Turn this on if your index mapping is keyword-
    /// only and you've been hit by `mapper_parsing_exception` when the same
    /// field arrives as different types across events.
    #[serde(default)]
    pub stringify_values: bool,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_cfg(body: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().expect("tempfile");
        f.write_all(body.as_bytes()).expect("write");
        f.flush().expect("flush");
        f
    }

    const MINIMAL_CFG: &str = r#"
        [opensearch]
        endpoint = "https://opensearch.example.com:9200"
        index    = "ubuntu-logs"
        username = "ingest"
        password = "pw"

        [sources.fifo]
        path = "/tmp/logpipe-test"
    "#;

    #[test]
    fn load_minimal_applies_defaults() {
        let f = write_cfg(MINIMAL_CFG);
        let cfg = Config::load(f.path()).expect("load");
        assert!(cfg.opensearch.verify_tls, "verify_tls defaults to true");
        assert_eq!(cfg.opensearch.request_timeout_secs, 30);
        assert_eq!(cfg.batch.max_events, 500);
        assert_eq!(cfg.batch.flush_secs, 5);
        assert!(cfg.batch.channel_capacity.is_none());
        assert_eq!(cfg.sources.fifo.mode, 0o622);
        assert!(cfg.sources.fifo.tag.is_none());
        assert!(
            !cfg.sources.fifo.stringify_values,
            "stringify_values defaults to false"
        );
    }

    #[test]
    fn load_full_overrides_defaults() {
        let body = r#"
            [opensearch]
            endpoint = "http://localhost:9200"
            index = "x"
            username = "u"
            password = "p"
            verify_tls = false
            request_timeout_secs = 5

            [batch]
            max_events = 10
            flush_secs = 1
            channel_capacity = 7

            [sources.fifo]
            path = "/tmp/x"
            mode = 0o600
            tag = "t"
            stringify_values = true
        "#;
        let f = write_cfg(body);
        let cfg = Config::load(f.path()).expect("load");
        assert!(!cfg.opensearch.verify_tls);
        assert_eq!(cfg.opensearch.request_timeout_secs, 5);
        assert_eq!(cfg.batch.max_events, 10);
        assert_eq!(cfg.batch.flush_secs, 1);
        assert_eq!(cfg.batch.channel_capacity, Some(7));
        assert_eq!(cfg.sources.fifo.mode, 0o600);
        assert_eq!(cfg.sources.fifo.tag.as_deref(), Some("t"));
        assert!(cfg.sources.fifo.stringify_values);
    }

    #[test]
    fn load_missing_file_errors() {
        let err = Config::load(Path::new("/nonexistent/logpipe/cfg.toml")).unwrap_err();
        assert!(format!("{err:#}").contains("reading config file"));
    }

    #[test]
    fn load_invalid_toml_errors() {
        let f = write_cfg("this is = not [ valid toml");
        let err = Config::load(f.path()).unwrap_err();
        assert!(format!("{err:#}").contains("parsing TOML"));
    }

    #[test]
    fn validate_rejects_empty_endpoint() {
        let body = r#"
            [opensearch]
            endpoint = ""
            index = "x"
            username = "u"
            password = "p"
            [sources.fifo]
            path = "/tmp/x"
        "#;
        let f = write_cfg(body);
        let err = Config::load(f.path()).unwrap_err();
        assert!(format!("{err:#}").contains("endpoint must be set"));
    }

    #[test]
    fn validate_rejects_empty_index() {
        let body = r#"
            [opensearch]
            endpoint = "https://h:9200"
            index = ""
            username = "u"
            password = "p"
            [sources.fifo]
            path = "/tmp/x"
        "#;
        let f = write_cfg(body);
        let err = Config::load(f.path()).unwrap_err();
        assert!(format!("{err:#}").contains("index must be set"));
    }

    #[test]
    fn validate_rejects_non_http_scheme() {
        let body = r#"
            [opensearch]
            endpoint = "ftp://h:9200"
            index = "x"
            username = "u"
            password = "p"
            [sources.fifo]
            path = "/tmp/x"
        "#;
        let f = write_cfg(body);
        let err = Config::load(f.path()).unwrap_err();
        assert!(format!("{err:#}").contains("must start with http://"));
    }

    #[test]
    fn validate_rejects_empty_fifo_path() {
        let body = r#"
            [opensearch]
            endpoint = "https://h:9200"
            index = "x"
            username = "u"
            password = "p"
            [sources.fifo]
            path = ""
        "#;
        let f = write_cfg(body);
        let err = Config::load(f.path()).unwrap_err();
        assert!(format!("{err:#}").contains("fifo.path must not be empty"));
    }

    #[test]
    fn validate_rejects_mode_outside_permission_mask() {
        // 0o10000 is one bit above the S_ISVTX/setuid/setgid/perm range.
        let body = r#"
            [opensearch]
            endpoint = "https://h:9200"
            index = "x"
            username = "u"
            password = "p"
            [sources.fifo]
            path = "/tmp/x"
            mode = 0o10000
        "#;
        let f = write_cfg(body);
        let err = Config::load(f.path()).unwrap_err();
        assert!(format!("{err:#}").contains("outside the permission mask"));
    }

    #[test]
    fn effective_channel_capacity_defaults_to_max_events_times_eight() {
        let b = BatchConfig {
            max_events: 500,
            flush_secs: 5,
            channel_capacity: None,
        };
        assert_eq!(b.effective_channel_capacity(), 4000);
    }

    #[test]
    fn effective_channel_capacity_respects_explicit_value() {
        let b = BatchConfig {
            max_events: 500,
            flush_secs: 5,
            channel_capacity: Some(123),
        };
        assert_eq!(b.effective_channel_capacity(), 123);
    }

    #[test]
    fn effective_channel_capacity_is_at_least_one() {
        // max_events=0 (a misconfiguration) must not produce a 0-cap channel
        // that would deadlock the bounded mpsc.
        let b = BatchConfig {
            max_events: 0,
            flush_secs: 5,
            channel_capacity: None,
        };
        assert_eq!(b.effective_channel_capacity(), 1);

        let b = BatchConfig {
            max_events: 0,
            flush_secs: 5,
            channel_capacity: Some(0),
        };
        assert_eq!(b.effective_channel_capacity(), 1);
    }

    #[test]
    fn effective_channel_capacity_does_not_overflow_on_huge_max_events() {
        let b = BatchConfig {
            max_events: usize::MAX,
            flush_secs: 5,
            channel_capacity: None,
        };
        // saturating_mul keeps us at usize::MAX rather than wrapping to 0.
        assert_eq!(b.effective_channel_capacity(), usize::MAX);
    }
}
