//! Delivery: where a background run's answer goes.
//!
//! A run with `deliver` set, a heartbeat, and a follow-up all end by sending a
//! [`Report`] to the harness [`Delivery`]. The send is its own activity,
//! `agent_deliver`. Its result is recorded, so a restart of the workflow does
//! not send a report again.
//!
//! The activity itself can retry, for example after a send that timed out.
//! Delivery is therefore at least once. A [`Delivery`] that must not repeat a
//! message dedupes on [`Report::key`].
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

/// Is this heartbeat answer a silent acknowledgement?
///
/// It is when it holds [`HEARTBEAT_OK`] and no other letter or digit. So
/// `HEARTBEAT_OK.` is silent, and `HEARTBEAT_OK, but the disk is full` is
/// not.
#[must_use]
pub fn is_silent(text: &str) -> bool {
    text.contains(HEARTBEAT_OK)
        && !text
            .replace(HEARTBEAT_OK, "")
            .chars()
            .any(char::is_alphanumeric)
}

/// What started the run that reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
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
    /// The segment of the run: 0 for the first, then one more per follow-up.
    #[serde(default)]
    pub segment: u32,
    /// The session of the run, if any.
    #[serde(default)]
    pub session_id: Option<SessionId>,
    /// The answer.
    pub text: String,
    /// Why the run ended.
    pub stop: AgentStop,
}

impl Report {
    /// A key that is unique to this report: the run id and the segment.
    #[must_use]
    pub fn key(&self) -> String {
        format!("{}:{}", self.run_id.as_str(), self.segment)
    }
}

/// The text to deliver for one segment, or `None` when it has nothing to say.
///
/// - A final answer is delivered: a segment that `completed`, or whose last
///   answer hit the output cap. An empty answer is not.
/// - A silent heartbeat acknowledgement is not delivered.
/// - A segment that a bound ended early delivers a notice, so a broken
///   unattended run does not fail in silence.
#[must_use]
pub fn report_text(source: ReportSource, stop: AgentStop, text: &str) -> Option<String> {
    let text = text.trim();
    match stop {
        AgentStop::Completed => {
            let silent = source == ReportSource::Heartbeat && is_silent(text);
            (!text.is_empty() && !silent).then(|| text.to_owned())
        }
        AgentStop::OutputCapped if !text.is_empty() => Some(text.to_owned()),
        AgentStop::OutputCapped
        | AgentStop::StepsExhausted
        | AgentStop::TokensExhausted
        | AgentStop::TranscriptFull
        | AgentStop::BudgetExceeded
        | AgentStop::LoopDetected => {
            let notice = format!("The agent stopped early: {}.", stop_name(stop));
            Some(if text.is_empty() {
                notice
            } else {
                format!("{notice}\n\n{text}")
            })
        }
    }
}

/// The recorded name of a stop.
fn stop_name(stop: AgentStop) -> String {
    serde_json::to_value(stop)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| format!("{stop:?}"))
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
        // The text can hold private data, so only its size is logged.
        tracing::info!(
            run_id = %report.run_id.as_str(),
            segment = report.segment,
            source = ?report.source,
            stop = ?report.stop,
            chars = report.text.chars().count(),
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
        assert!(is_silent("  HEARTBEAT_OK. \n"));
        assert!(is_silent("**HEARTBEAT_OK**"));
        assert!(!is_silent("HEARTBEAT_OK, but the prod DB is down"));
        assert!(!is_silent("All quiet. HEARTBEAT_OK"));
        assert!(!is_silent("The disk is 95% full."));
        assert!(!is_silent("heartbeat_ok"));
    }

    #[test]
    fn a_final_answer_is_delivered() {
        let run = ReportSource::Run;
        assert_eq!(
            report_text(run, AgentStop::Completed, "Disk full"),
            Some("Disk full".to_owned())
        );
        assert_eq!(report_text(run, AgentStop::Completed, "  "), None);
        assert!(report_text(run, AgentStop::OutputCapped, "cut").is_some());
        let capped = report_text(run, AgentStop::OutputCapped, "").unwrap();
        assert!(capped.contains("output_capped"), "{capped}");
    }

    #[test]
    fn only_a_heartbeat_can_be_silent() {
        let beat = ReportSource::Heartbeat;
        assert_eq!(report_text(beat, AgentStop::Completed, HEARTBEAT_OK), None);
        assert!(report_text(ReportSource::Run, AgentStop::Completed, HEARTBEAT_OK).is_some());
    }

    #[test]
    fn an_early_stop_delivers_a_notice() {
        let text = report_text(ReportSource::Heartbeat, AgentStop::LoopDetected, "").unwrap();
        assert_eq!(text, "The agent stopped early: loop_detected.");
        let text = report_text(ReportSource::Run, AgentStop::StepsExhausted, "partial").unwrap();
        assert!(text.ends_with("partial"), "{text}");
        // A spent LLM budget also ends the run early (issue #1997).
        let text = report_text(ReportSource::Heartbeat, AgentStop::BudgetExceeded, "").unwrap();
        assert_eq!(text, "The agent stopped early: budget_exceeded.");
    }

    #[test]
    fn the_key_names_the_run_and_the_segment() {
        let report = Report {
            source: ReportSource::Followup,
            run_id: RunId::new("r"),
            segment: 2,
            session_id: None,
            text: "hi".into(),
            stop: AgentStop::Completed,
        };
        assert_eq!(report.key(), "r:2");
    }

    #[tokio::test]
    async fn the_log_delivery_accepts_reports() {
        let report = Report {
            source: ReportSource::Run,
            run_id: RunId::new("r"),
            segment: 0,
            session_id: None,
            text: "hi".into(),
            stop: AgentStop::Completed,
        };
        LogDelivery.deliver(&report).await.unwrap();
    }
}
