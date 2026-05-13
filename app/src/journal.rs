//! Fallback sink for events that OpenSearch refused to accept.
//!
//! When [`crate::opensearch::OpenSearchClient::send`] exhausts its retries,
//! or when the `_bulk` response rejects individual documents, the affected
//! events would otherwise be dropped on the floor. This module re-emits
//! them on stderr as warn-level tracing lines so systemd captures them in
//! the journal of the unit, keeping the payloads recoverable with e.g.
//!
//! ```text
//! journalctl -u logpipe | grep logpipe-fallback
//! ```
//!
//! Each event becomes one line whose message body starts with the literal
//! marker `logpipe-fallback` followed by the event serialised as compact
//! JSON, so it can be sliced with `jq` after a trivial `awk`/`cut`.
//!
//! Note: the fmt subscriber in `main.rs` is configured with
//! `with_target(false)`, so we deliberately put the marker into the
//! message text rather than the tracing target.

use tracing::warn;

use crate::event::Event;

/// Marker prefix on every fallback line. Stable on purpose — operators
/// grep for it.
pub const MARKER: &str = "logpipe-fallback";

/// Emit each event in `events` as a single stderr-bound tracing line so
/// systemd ships it to the journal. `reason` describes why the events
/// could not reach OpenSearch and is attached as a structured field.
pub fn fallback<'a, I>(events: I, reason: &str)
where
    I: IntoIterator<Item = &'a Event>,
{
    for ev in events {
        match serde_json::to_string(ev) {
            Ok(json) => warn!(reason, "{MARKER} {json}"),
            Err(err) => warn!(
                reason,
                error = %err,
                "{MARKER} event could not be serialised for journal fallback"
            ),
        }
    }
}
