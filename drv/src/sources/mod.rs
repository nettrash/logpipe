//! Event sources. Each source spawns a tokio task and pushes [`Event`]s
//! into the shared mpsc channel.

pub mod fifo;

use tokio::sync::mpsc::Sender;

use crate::event::Event;

/// Shared sender end of the events pipeline.
pub type EventTx = Sender<Event>;
