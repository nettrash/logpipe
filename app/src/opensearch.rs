//! OpenSearch `_bulk` client with retry/backoff.

use std::io::Write;
use std::time::Duration;

use anyhow::{Context, Result};
use bytes::Bytes;
use flate2::write::GzEncoder;
use flate2::Compression;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_ENCODING, CONTENT_TYPE};
use reqwest::Client;
use serde::Deserialize;
use tracing::{debug, error, warn};

use crate::config::OpenSearchConfig;
use crate::event::Event;

/// Maximum number of retry attempts for a single batch before giving up.
const MAX_ATTEMPTS: u32 = 6;

/// HTTP client wrapping a configured OpenSearch endpoint.
pub struct OpenSearchClient {
    http: Client,
    bulk_url: String,
    index: String,
}

impl OpenSearchClient {
    pub fn new(cfg: &OpenSearchConfig) -> Result<Self> {
        let mut headers = HeaderMap::new();
        // OpenSearch _bulk requires application/x-ndjson; reqwest preserves
        // the explicit content-type even when we use .body().
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-ndjson"),
        );
        // We compress the body ourselves below — see `gzip_body`. reqwest's
        // `gzip` feature only decompresses *responses*, never requests.
        headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));

        let http = Client::builder()
            .danger_accept_invalid_certs(!cfg.verify_tls)
            .timeout(Duration::from_secs(cfg.request_timeout_secs))
            .default_headers(headers)
            .gzip(true)
            .build()
            .context("building reqwest client")?;

        let endpoint = cfg.endpoint.trim_end_matches('/').to_owned();
        let bulk_url = format!("{endpoint}/_bulk");

        Ok(Self {
            http,
            bulk_url,
            index: cfg.index.clone(),
        })
    }

    /// Send a batch of events to `_bulk`. Retries transient failures with
    /// exponential backoff and only returns `Err` if the batch is
    /// definitively un-deliverable.
    ///
    /// On success returns the indices (into `events`) of documents that
    /// OpenSearch accepted at the HTTP layer but rejected at the per-item
    /// layer (mapping errors, malformed docs, …). An empty `Vec` means the
    /// whole batch was indexed cleanly.
    pub async fn send(
        &self,
        events: &[Event],
        username: &str,
        password: &str,
    ) -> Result<Vec<usize>> {
        if events.is_empty() {
            return Ok(Vec::new());
        }

        // Build and gzip the bulk body exactly once. `Bytes::clone()` on each
        // retry is just a refcount bump, no memcpy of the (possibly multi-MB)
        // payload.
        let body = gzip_body(build_bulk_body(&self.index, events)?)?;

        let mut backoff = Duration::from_millis(500);
        for attempt in 1..=MAX_ATTEMPTS {
            match self
                .http
                .post(&self.bulk_url)
                .basic_auth(username, Some(password))
                .body(body.clone())
                .send()
                .await
            {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        let text = resp.text().await.unwrap_or_default();
                        let rejected = match inspect_bulk_response(&text) {
                            Some((indices, reason)) if indices.len() >= events.len() => {
                                error!(
                                    attempt,
                                    failed = indices.len(),
                                    reason = %reason,
                                    "OpenSearch rejected every document in the bulk batch (diverting to journal)"
                                );
                                indices
                            }
                            Some((indices, reason)) => {
                                warn!(
                                    attempt,
                                    failed = indices.len(),
                                    reason = %reason,
                                    "bulk indexed with item errors (failed items diverted to journal)"
                                );
                                indices
                            }
                            None => {
                                debug!(events = events.len(), "bulk indexed");
                                Vec::new()
                            }
                        };
                        return Ok(rejected);
                    }

                    if status.is_client_error() && status.as_u16() != 408 && status.as_u16() != 429
                    {
                        let body = resp.text().await.unwrap_or_default();
                        anyhow::bail!("OpenSearch rejected bulk request with {status}: {body}");
                    }

                    warn!(
                        attempt,
                        %status, "transient OpenSearch error, will retry"
                    );
                }
                Err(err) => {
                    warn!(attempt, error = %err, "OpenSearch request failed, will retry");
                }
            }

            if attempt < MAX_ATTEMPTS {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }

        error!(
            attempts = MAX_ATTEMPTS,
            "giving up on bulk batch after exhausting retries"
        );
        anyhow::bail!("opensearch bulk delivery failed after {MAX_ATTEMPTS} attempts")
    }
}

/// gzip the bulk body. Compression level 1 is plenty for NDJSON — the cost
/// of going higher is significant CPU per batch with negligible wire savings
/// on highly-repetitive log data.
fn gzip_body(raw: Vec<u8>) -> Result<Bytes> {
    let mut encoder = GzEncoder::new(Vec::with_capacity(raw.len() / 4), Compression::fast());
    encoder.write_all(&raw).context("gzip-encoding bulk body")?;
    let compressed = encoder.finish().context("finalising gzip stream")?;
    Ok(Bytes::from(compressed))
}

fn build_bulk_body(index: &str, events: &[Event]) -> Result<Vec<u8>> {
    // _bulk wants action+doc pairs separated by \n, trailing newline required.
    let action_line = serde_json::to_vec(&serde_json::json!({
        "index": { "_index": index }
    }))?;

    let mut out = Vec::with_capacity(events.len() * 256);
    for ev in events {
        out.extend_from_slice(&action_line);
        out.push(b'\n');
        let doc = serde_json::to_vec(ev)?;
        out.extend_from_slice(&doc);
        out.push(b'\n');
    }
    Ok(out)
}

#[derive(Deserialize)]
struct BulkResponse {
    errors: bool,
    #[serde(default)]
    items: Vec<serde_json::Value>,
}

/// If the bulk response indicates per-item errors, return the indices of
/// the failed items (matching the order they were submitted in) plus a
/// representative `"type: reason"` string for logging. `None` means every
/// item was accepted cleanly.
fn inspect_bulk_response(body: &str) -> Option<(Vec<usize>, String)> {
    let parsed: BulkResponse = serde_json::from_str(body).ok()?;
    if !parsed.errors {
        return None;
    }
    let mut failed_indices = Vec::new();
    let mut sample = String::new();
    for (idx, item) in parsed.items.iter().enumerate() {
        let Some(err) = item
            .as_object()
            .and_then(|m| m.values().next())
            .and_then(|v| v.get("error"))
        else {
            continue;
        };
        failed_indices.push(idx);
        if sample.is_empty() {
            let kind = err.get("type").and_then(|v| v.as_str()).unwrap_or("error");
            let reason = err.get("reason").and_then(|v| v.as_str()).unwrap_or("");
            sample = format!("{kind}: {reason}");
        }
    }
    if failed_indices.is_empty() {
        // `errors` was set but we couldn't attribute it to any specific item —
        // treat the whole batch as failed so the caller can divert it.
        failed_indices = (0..parsed.items.len().max(1)).collect();
    }
    if sample.is_empty() {
        sample = body.chars().take(400).collect();
    }
    Some((failed_indices, sample))
}
