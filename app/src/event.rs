//! Common event representation flowing from sources to the OpenSearch sink.

use serde::Serialize;
use serde_json::Value;

/// A single document destined for OpenSearch.
///
/// Sources push these into the channel; the batcher serializes them
/// into NDJSON-encoded `_bulk` requests.
#[derive(Debug, Clone, Serialize)]
pub struct Event {
    /// ISO-8601 / RFC3339 timestamp the event was observed.
    #[serde(rename = "@timestamp")]
    pub timestamp: String,

    /// Logical source name (currently always "fifo").
    pub source: &'static str,

    /// Host that produced the event.
    pub host: String,

    /// Free-form tag from the config (file tag, unit name, etc.).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,

    /// Primary message text, if available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// All remaining source-specific fields, flattened into the document.
    #[serde(flatten)]
    pub fields: serde_json::Map<String, Value>,
}
