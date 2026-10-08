//! Delivery: where a background run's answer goes.
//!
//! A run with `deliver` set, a heartbeat, and a follow-up all end by sending a
//! [`Report`] to the harness [`Delivery`]. The send is its own activity,
//! `agent_deliver`, so a restart never sends one report twice.
//!
//! The crate speaks no chat or mail protocol. The app implements
//! [`Delivery`]. [`LogDelivery`] is the default.

use serde::{Deserialize, Serialize};

use crate::error::AgentError;
use crate::message::{RunId, SessionId};
use crate::model::BoxFuture;
use crate::types::AgentStop;

/// The answer that means "nothing needs attention".
pub const HEARTBEAT_OK: &str = "HEARTBEAT_OK";

/// The most characters a silent answer may add around [`HEARTBEAT_OK`].
pub const SILENT_ACK_MAX_CHARS: usize = 300;

/// Is this answer a silent acknowledgement?
///
/// It is when it is [`HEARTBEAT_OK`], or starts or ends with it and adds at
/// most [`SILENT_ACK_MAX_CHARS`] characters.
#[must_use]
pub fn is_silent(text: &str) -> bool {
    let text = text.trim();
    let rest = text
        .strip_prefix(HEARTBEAT_OK)
        .or_else(|| text.strip_suffix(HEARTBEAT_OK));
    rest.is_some_and(|rest| rest.trim().chars().count() <= SILENT_ACK_MAX_CHARS)
}

/// What started the run that reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReportSource {
    /// An `agent_loop` run with `deliver` set.
    Run,
    /// A heartbeat tick.
    Heartbeat,
    /// A follow-up that the agent scheduled.
    Followup,
}

/// One answer to deliver.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Report {
    /// What started the run.
    pub source: ReportSource,
    /// The run id.
    pub run_id: RunId,
    /// The session of the run, if any.
    #[serde(default)]
    pub session_id: Option<SessionId>,
    /// The answer.
    pub text: String,
    /// Why the run ended.
    pub stop: AgentStop,
}

/// The text to deliver for a run, or `None` when it has nothing to say.
///
/// Only a final answer is delivered: a run that `completed`, or whose last
/// turn hit the output cap. An empty answer and a silent acknowledgement are
/// not delivered.
#[must_use]
pub fn report_text(stop: AgentStop, text: &str) -> Option<String> {
    let answered = matches!(stop, AgentStop::Completed | AgentStop::OutputCapped);
    (answered && !text.trim().is_empty() && !is_silent(text)).then(|| text.to_owned())
}

/// Sends reports to people: mail, chat, push, or an in-app inbox.
pub trait Delivery: Send + Sync + std::fmt::Debug {
    /// Send one report.
    ///
    /// # Errors
    ///
    /// Returns an error when the send fails. A retryable kind retries. The
    /// run does not fail on a failed delivery.
    fn deliver<'a>(&'a self, report: &'a Report) -> BoxFuture<'a, Result<(), AgentError>>;
}

/// Writes each report to the `tracing` log. The default delivery.
#[derive(Debug, Clone, Copy, Default)]
pub struct LogDelivery;

impl Delivery for LogDelivery {
    fn deliver<'a>(&'a self, report: &'a Report) -> BoxFuture<'a, Result<(), AgentError>> {
        tracing::info!(
            run_id = %report.run_id.as_str(),
            source = ?report.source,
            text = %report.text,
            "agent report"
        );
        Box::pin(std::future::ready(Ok(())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn silence_rules() {
        assert!(is_silent("HEARTBEAT_OK"));
        assert!(is_silent("  HEARTBEAT_OK \n"));
        assert!(is_silent("HEARTBEAT_OK - all quiet"));
        assert!(is_silent("All quiet. HEARTBEAT_OK"));
        assert!(!is_silent("The disk is 95% full."));
        assert!(!is_silent(&format!("HEARTBEAT_OK {}", "x".repeat(301))));
        assert!(!is_silent("heartbeat_ok"));
    }

    #[test]
    fn only_a_final_answer_is_delivered() {
        assert_eq!(
            report_text(AgentStop::Completed, "Disk full"),
            Some("Disk full".to_owned())
        );
        assert_eq!(report_text(AgentStop::Completed, HEARTBEAT_OK), None);
        assert_eq!(report_text(AgentStop::Completed, "  "), None);
        assert_eq!(report_text(AgentStop::StepsExhausted, "partial"), None);
        assert!(report_text(AgentStop::OutputCapped, "cut").is_some());
    }

    #[tokio::test]
    async fn the_log_delivery_accepts_reports() {
        let report = Report {
            source: ReportSource::Run,
            run_id: RunId::new("r"),
            session_id: None,
            text: "hi".into(),
            stop: AgentStop::Completed,
        };
        LogDelivery.deliver(&report).await.unwrap();
    }
}
