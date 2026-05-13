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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Map};
    use std::io::Read as _;

    fn event(message: &str) -> Event {
        Event {
            timestamp: "2026-05-13T00:00:00Z".to_string(),
            source: "fifo",
            host: "h".to_string(),
            tag: None,
            message: Some(message.to_string()),
            fields: Map::new(),
        }
    }

    #[test]
    fn build_bulk_body_produces_action_doc_pairs_with_trailing_newline() {
        let events = vec![event("a"), event("b")];
        let body = build_bulk_body("idx", &events).expect("build");
        let text = String::from_utf8(body).expect("utf8");

        // _bulk requires a trailing newline; the body must end with one.
        assert!(text.ends_with('\n'));

        // Two events ⇒ 4 NDJSON rows: action, doc, action, doc.
        let lines: Vec<&str> = text.trim_end_matches('\n').split('\n').collect();
        assert_eq!(lines.len(), 4);

        let action: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(action, json!({"index": {"_index": "idx"}}));
        assert_eq!(lines[0], lines[2], "action line is the same every event");

        let doc_a: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(doc_a["message"], json!("a"));
        let doc_b: serde_json::Value = serde_json::from_str(lines[3]).unwrap();
        assert_eq!(doc_b["message"], json!("b"));
    }

    #[test]
    fn build_bulk_body_empty_events_produces_empty_body() {
        let body = build_bulk_body("idx", &[]).expect("build");
        assert!(body.is_empty());
    }

    #[test]
    fn gzip_body_roundtrips() {
        let raw = b"hello world ".repeat(200);
        let compressed = gzip_body(raw.clone()).expect("gzip");
        let mut decoded = Vec::new();
        flate2::read::GzDecoder::new(&compressed[..])
            .read_to_end(&mut decoded)
            .expect("ungzip");
        assert_eq!(decoded, raw);
    }

    #[test]
    fn inspect_bulk_response_returns_none_when_no_errors() {
        let body = r#"{"errors":false,"items":[{"index":{"status":201}}]}"#;
        assert!(inspect_bulk_response(body).is_none());
    }

    #[test]
    fn inspect_bulk_response_collects_failed_item_indices_and_sample() {
        let body = r#"{
            "errors": true,
            "items": [
                {"index": {"status": 201}},
                {"index": {"status": 400, "error": {"type":"mapper_parsing_exception","reason":"bad field"}}},
                {"index": {"status": 201}},
                {"index": {"status": 400, "error": {"type":"x","reason":"y"}}}
            ]
        }"#;
        let (indices, sample) = inspect_bulk_response(body).expect("errored response");
        assert_eq!(indices, vec![1, 3]);
        // Sample is taken from the first failing item.
        assert_eq!(sample, "mapper_parsing_exception: bad field");
    }

    #[test]
    fn inspect_bulk_response_treats_unattributable_errors_as_whole_batch_failure() {
        // `errors: true` but no per-item .error — we can't attribute, so the
        // caller diverts everything.
        let body = r#"{"errors":true,"items":[{"index":{"status":500}},{"index":{"status":500}}]}"#;
        let (indices, sample) = inspect_bulk_response(body).expect("errored response");
        assert_eq!(indices, vec![0, 1]);
        // Fallback sample is the raw body prefix.
        assert!(sample.starts_with("{\"errors\":true"));
    }

    #[test]
    fn inspect_bulk_response_unattributable_with_empty_items_marks_one_index() {
        // Defensive case: `errors: true` with no items at all. We pick at
        // least one index so the caller still treats the batch as failed.
        let body = r#"{"errors":true,"items":[]}"#;
        let (indices, _sample) = inspect_bulk_response(body).expect("errored response");
        assert_eq!(indices, vec![0]);
    }

    #[test]
    fn inspect_bulk_response_handles_missing_type_and_reason() {
        let body = r#"{
            "errors": true,
            "items": [{"index": {"status": 400, "error": {}}}]
        }"#;
        let (indices, sample) = inspect_bulk_response(body).expect("errored response");
        assert_eq!(indices, vec![0]);
        assert_eq!(sample, "error: ");
    }

    #[test]
    fn inspect_bulk_response_returns_none_on_malformed_json() {
        assert!(inspect_bulk_response("not json").is_none());
    }
}
