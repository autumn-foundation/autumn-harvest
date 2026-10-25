//! Live text of model turns (issue #2268).
//!
//! Two layers carry the answer of a model turn to a reader:
//!
//! 1. **Live.** While the model writes, the model-turn activity sends
//!    [`LiveEvent`] values. On Postgres they go through
//!    `ActivityContext::publish_progress` to the progress channel of the run.
//!    A [`LiveSink`] on the harness gets them on every backend. Nothing is
//!    stored.
//! 2. **Durable.** With `AgentTask::stream`, the workflow publishes the
//!    text of each finished turn with `publish_durable_progress`. A reader
//!    that reconnects then gets each whole answer once.
//!
//! A model call starts with [`LiveEvent::Start`]. A retry starts again, so a
//! reader drops the text of the call before it.

use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

/// A delta that waits this long goes out at the next push.
///
/// The coalescer has no timer. A model adapter pushes an empty piece at each
/// provider event that has no text, for example a thinking delta. So text
/// that waits goes out while the model thinks or writes a tool input.
pub const COALESCE_WINDOW: Duration = Duration::from_millis(50);

/// A delta of at least this many bytes goes out at once.
pub const COALESCE_BYTES: usize = 256;

/// One live event of a model turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LiveEvent {
    /// A model call starts. Drop the text of an earlier call of the turn.
    Start {
        /// The run id of the agent.
        run_id: String,
        /// The tool round of the turn in its segment.
        step: u32,
    },
    /// Text that the model wrote, in order.
    Delta {
        /// The run id of the agent.
        run_id: String,
        /// The tool round of the turn in its segment.
        step: u32,
        /// The text.
        text: String,
    },
}

/// The durable record of the text of one finished turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnText {
    /// Always `turn`.
    #[serde(rename = "type")]
    pub kind: String,
    /// The run id of the agent.
    pub run_id: String,
    /// The tool round of the turn in its segment.
    pub step: u32,
    /// The agent of the tree that wrote it, when the run has a mandate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    /// The whole text of the turn.
    pub text: String,
}

/// Where the live events of a harness go, on every backend.
///
/// A sink must not block. Drop an event that cannot go out.
pub trait LiveSink: Send + Sync + std::fmt::Debug {
    /// Take one event.
    fn send(&self, event: &LiveEvent);
}

/// Joins small deltas, so a reader gets fewer, larger events.
#[derive(Debug)]
pub(crate) struct Coalescer {
    pending: String,
    since: Instant,
}

impl Coalescer {
    pub(crate) fn new() -> Self {
        Self {
            pending: String::new(),
            since: Instant::now(),
        }
    }

    /// Add `text`. Returns the text to send now, if any.
    ///
    /// An empty `text` is a tick. It sends the text that waits when the
    /// window has passed, and it never sends an empty delta.
    pub(crate) fn push(&mut self, text: &str) -> Option<String> {
        if self.pending.is_empty() {
            if text.is_empty() {
                return None;
            }
            self.since = Instant::now();
        }
        self.pending.push_str(text);
        (self.pending.len() >= COALESCE_BYTES || self.since.elapsed() >= COALESCE_WINDOW)
            .then(|| std::mem::take(&mut self.pending))
    }

    /// The text that still waits.
    pub(crate) fn flush(&mut self) -> Option<String> {
        (!self.pending.is_empty()).then(|| std::mem::take(&mut self.pending))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_deltas_join_and_the_rest_flushes() {
        let mut joined = Coalescer::new();
        assert_eq!(joined.push("a"), None);
        assert_eq!(joined.push("b"), None);
        assert_eq!(joined.flush().as_deref(), Some("ab"));
        assert_eq!(joined.flush(), None);
        let big = "x".repeat(COALESCE_BYTES);
        assert_eq!(joined.push(&big).as_deref(), Some(big.as_str()));
    }

    #[test]
    fn an_empty_push_sends_the_waiting_text_after_the_window() {
        let mut joined = Coalescer::new();
        assert_eq!(joined.push(""), None, "a tick with no text sends nothing");
        assert_eq!(joined.push("a"), None);
        assert_eq!(joined.push(""), None, "the window has not passed");
        std::thread::sleep(COALESCE_WINDOW + Duration::from_millis(5));
        assert_eq!(joined.push("").as_deref(), Some("a"));
        std::thread::sleep(COALESCE_WINDOW + Duration::from_millis(5));
        assert_eq!(joined.push(""), None, "an empty delta never goes out");
    }

    #[test]
    fn a_delta_waits_at_most_the_window() {
        let mut joined = Coalescer::new();
        assert_eq!(joined.push("a"), None);
        std::thread::sleep(COALESCE_WINDOW + Duration::from_millis(5));
        assert_eq!(joined.push("b").as_deref(), Some("ab"));
    }
}
