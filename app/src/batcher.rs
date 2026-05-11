//! Batching task: drains the events channel and flushes to OpenSearch
//! either when `max_events` is reached or `flush_secs` elapses.

use std::time::Duration;

use anyhow::Result;
use tokio::sync::mpsc::Receiver;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::config::{BatchConfig, OpenSearchConfig};
use crate::event::Event;
use crate::opensearch::OpenSearchClient;

pub async fn run(
    batch_cfg: BatchConfig,
    os_cfg: OpenSearchConfig,
    mut rx: Receiver<Event>,
    cancel: CancellationToken,
) -> Result<()> {
    let client = OpenSearchClient::new(&os_cfg)?;
    let mut buf: Vec<Event> = Vec::with_capacity(batch_cfg.max_events);
    let mut ticker = tokio::time::interval(Duration::from_secs(batch_cfg.flush_secs.max(1)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    info!(
        max_events = batch_cfg.max_events,
        flush_secs = batch_cfg.flush_secs,
        "batcher started"
    );

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("batcher draining on shutdown");
                while let Ok(ev) = rx.try_recv() {
                    buf.push(ev);
                }
                flush(&client, &os_cfg, &mut buf).await;
                return Ok(());
            }
            maybe = rx.recv() => {
                match maybe {
                    Some(ev) => {
                        buf.push(ev);
                        if buf.len() >= batch_cfg.max_events {
                            flush(&client, &os_cfg, &mut buf).await;
                        }
                    }
                    None => {
                        info!("event channel closed, batcher flushing and exiting");
                        flush(&client, &os_cfg, &mut buf).await;
                        return Ok(());
                    }
                }
            }
            _ = ticker.tick() => {
                if !buf.is_empty() {
                    flush(&client, &os_cfg, &mut buf).await;
                }
            }
        }
    }
}

async fn flush(client: &OpenSearchClient, os_cfg: &OpenSearchConfig, buf: &mut Vec<Event>) {
    if buf.is_empty() {
        return;
    }
    let n = buf.len();
    match client.send(buf, &os_cfg.username, &os_cfg.password).await {
        Ok(()) => debug!(events = n, "flushed batch"),
        Err(err) => error!(error = %err, events = n, "dropping batch after retries exhausted"),
    }
    buf.clear();
}
