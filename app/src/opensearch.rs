//! OpenSearch `_bulk` client with retry/backoff.

use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
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
    pub async fn send(&self, events: &[Event], username: &str, password: &str) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let body = build_bulk_body(&self.index, events)?;

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
                        match inspect_bulk_response(&text) {
                            Some((failed, reason)) if failed >= events.len() => error!(
                                attempt,
                                failed,
                                reason = %reason,
                                "OpenSearch rejected every document in the bulk batch (dropping)"
                            ),
                            Some((failed, reason)) => warn!(
                                attempt,
                                failed,
                                reason = %reason,
                                "bulk indexed with item errors (continuing)"
                            ),
                            None => debug!(events = events.len(), "bulk indexed"),
                        }
                        return Ok(());
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

/// If the bulk response indicates per-item errors, return how many items
/// failed plus a representative `"type: reason"` string for logging. `None`
/// means everything was accepted cleanly.
fn inspect_bulk_response(body: &str) -> Option<(usize, String)> {
    let parsed: BulkResponse = serde_json::from_str(body).ok()?;
    if !parsed.errors {
        return None;
    }
    let mut failed = 0usize;
    let mut sample = String::new();
    for item in &parsed.items {
        let Some(err) = item
            .as_object()
            .and_then(|m| m.values().next())
            .and_then(|v| v.get("error"))
        else {
            continue;
        };
        failed += 1;
        if sample.is_empty() {
            let kind = err.get("type").and_then(|v| v.as_str()).unwrap_or("error");
            let reason = err.get("reason").and_then(|v| v.as_str()).unwrap_or("");
            sample = format!("{kind}: {reason}");
        }
    }
    if sample.is_empty() {
        // `errors` was set but we couldn't attribute it — surface the raw body.
        sample = body.chars().take(400).collect();
    }
    Some((failed.max(1), sample))
}
