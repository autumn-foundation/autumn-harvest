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
//! A gated tool call sends one [`ApprovalNotice`] before it parks. The send
//! is the activity `agent_approval_notice`, before the wait. Its result is
//! recorded too, so a restart during the wait sends no second notice. A
//! retry can send it again, so dedupe on [`ApprovalNotice::key`]. A failed
//! notice is recorded and ignored. Only a decision releases the call.
//!
//! The crate speaks no chat or mail protocol. The app implements
//! [`Delivery`]. [`LogDelivery`] is the default.

use autumn_harvest::ExecutionId;
use autumn_harvest::chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::agent::AgentId;
use crate::error::AgentError;
use crate::harness::cut_bytes;
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
    /// A message from another durable agent (issue #2269).
    Peer,
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
        | AgentStop::ReplyTooLarge
        | AgentStop::BudgetExceeded
        | AgentStop::LoopDetected
        | AgentStop::Interrupted
        | AgentStop::Failed
        | AgentStop::AnswerRejected
        | AgentStop::StepEnded => {
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

/// One gated call that waits for an approval. It is the recorded input of
/// the activity `agent_approval_notice`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ParkedCall {
    /// The run id that tools and the policy see.
    pub run_id: RunId,
    /// The run that waits. Send the decision to it.
    pub execution_id: ExecutionId,
    /// The durable agent that waits, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    /// The session of the run, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<SessionId>,
    /// The approval signal that releases the call.
    pub signal: String,
    /// The tool name.
    pub tool: String,
    /// Why the policy asks for an approval.
    #[serde(default)]
    pub reason: String,
    /// The arguments of the call. A cut leaves a JSON string.
    pub arguments: Value,
    /// The arguments were cut to the tool result cap.
    #[serde(default)]
    pub arguments_truncated: bool,
    /// The time of the wait, in seconds.
    pub timeout_secs: u64,
}

impl ParkedCall {
    /// A key that is unique to this wait: the run id and the signal name.
    #[must_use]
    pub fn key(&self) -> String {
        format!("{}:{}", self.run_id.as_str(), self.signal)
    }

    /// The notice for this call. The deadline is `now` plus the timeout.
    #[must_use]
    pub fn notice(self, now: DateTime<Utc>) -> ApprovalNotice {
        let secs = i64::try_from(self.timeout_secs).unwrap_or(i64::MAX);
        let deadline = autumn_harvest::chrono::Duration::try_seconds(secs)
            .and_then(|timeout| now.checked_add_signed(timeout))
            .unwrap_or(DateTime::<Utc>::MAX_UTC);
        ApprovalNotice {
            call: self,
            deadline,
        }
    }
}

/// The notice that a gated call waits for an approval.
///
/// Show it to a person, and send the decision with `decide`: use
/// `call.execution_id` and `call.signal`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApprovalNotice {
    /// The call that waits.
    #[serde(flatten)]
    pub call: ParkedCall,
    /// The earliest time that the wait can end with no decision. The worker
    /// reads its clock before the wait starts, so the wait ends at this time
    /// or a little later.
    pub deadline: DateTime<Utc>,
}

impl ApprovalNotice {
    /// A key that is unique to this wait. A retry keeps it. See
    /// [`ParkedCall::key`].
    #[must_use]
    pub fn key(&self) -> String {
        self.call.key()
    }
}

/// Cut `arguments` to `max` bytes of JSON text. The flag is `true` when the
/// cut drops a byte. A cut value is a JSON string.
pub(crate) fn cut_arguments(arguments: Value, max: usize) -> (Value, bool) {
    let text = arguments.to_string();
    if text.len() <= max {
        return (arguments, false);
    }
    (Value::String(cut_bytes(text, max)), true)
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

    /// Tell a person that a gated call waits for an approval.
    ///
    /// The default writes the names to the `tracing` log. It never logs the
    /// arguments or the reason.
    ///
    /// # Errors
    ///
    /// Returns an error when the send fails. A retryable kind retries. A
    /// failed notice never fails the run and never releases the call.
    fn notify_approval<'a>(
        &'a self,
        notice: &'a ApprovalNotice,
    ) -> BoxFuture<'a, Result<(), AgentError>> {
        log_notice(notice);
        Box::pin(std::future::ready(Ok(())))
    }
}

/// Write the names of `notice` to the `tracing` log. The arguments and the
/// reason can hold private data, so only the size of the arguments is
/// logged.
fn log_notice(notice: &ApprovalNotice) {
    let call = &notice.call;
    tracing::info!(
        run_id = %call.run_id.as_str(),
        agent_id = call.agent_id.as_ref().map(AgentId::as_str),
        signal = %call.signal,
        tool = %call.tool,
        deadline = %notice.deadline,
        argument_bytes = call.arguments.to_string().len(),
        "agent approval wait"
    );
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
        // A reply too large to record ends the run early too (issue #2040).
        let text = report_text(ReportSource::Run, AgentStop::ReplyTooLarge, "").unwrap();
        assert_eq!(text, "The agent stopped early: reply_too_large.");
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

    fn parked(timeout_secs: u64) -> ParkedCall {
        ParkedCall {
            run_id: RunId::new("r"),
            execution_id: ExecutionId::new(),
            agent_id: Some(AgentId::new("support", "acct-1").unwrap()),
            session_id: None,
            signal: "tool_approval:0:0:w1".into(),
            tool: "write".into(),
            reason: "a secret reason".into(),
            arguments: serde_json::json!({"card": "4111-1111"}),
            arguments_truncated: false,
            timeout_secs,
        }
    }

    #[test]
    fn the_notice_key_names_the_run_and_the_signal() {
        let notice = parked(60).notice(Utc::now());
        assert_eq!(notice.key(), "r:tool_approval:0:0:w1");
        assert_eq!(notice.key(), notice.call.key());
    }

    #[test]
    fn the_deadline_is_now_plus_the_timeout() {
        let now = Utc::now();
        let notice = parked(90).notice(now);
        assert_eq!(
            notice.deadline,
            now + autumn_harvest::chrono::Duration::seconds(90)
        );
        // A huge timeout saturates. It never wraps to an early deadline.
        let notice = parked(u64::MAX).notice(now);
        assert_eq!(notice.deadline, DateTime::<Utc>::MAX_UTC);
    }

    #[test]
    fn the_notice_round_trips_as_flat_json() {
        let notice = parked(60).notice(Utc::now());
        let value = serde_json::to_value(&notice).unwrap();
        assert_eq!(value["signal"], "tool_approval:0:0:w1");
        assert!(value.get("deadline").is_some(), "{value}");
        assert!(value.get("call").is_none(), "{value}");
        let back: ApprovalNotice = serde_json::from_value(value).unwrap();
        assert_eq!(back, notice);
    }

    #[test]
    fn arguments_over_the_cap_are_cut_to_a_string() {
        let small = serde_json::json!({"n": 1});
        assert_eq!(cut_arguments(small.clone(), 100), (small, false));
        let big = serde_json::json!({"text": "x".repeat(500)});
        let (cut, flagged) = cut_arguments(big, 100);
        assert!(flagged);
        let Value::String(text) = cut else {
            panic!("a cut value is a string: {cut}");
        };
        assert!(text.len() <= 100, "{}", text.len());
        assert!(text.ends_with(crate::harness::TRUNCATED), "{text}");
        // No cap keeps the value.
        let big = serde_json::json!({"text": "x".repeat(500)});
        assert_eq!(cut_arguments(big.clone(), usize::MAX), (big, false));
    }

    /// A `tracing` writer that keeps each line.
    #[derive(Clone, Default)]
    struct Lines(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for Lines {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn the_default_notice_logs_names_and_never_the_arguments() {
        let lines = Lines::default();
        let writer = lines.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .finish();
        let notice = parked(60).notice(Utc::now());
        let _guard = tracing::subscriber::set_default(subscriber);
        LogDelivery.notify_approval(&notice).await.unwrap();
        let text = String::from_utf8(lines.0.lock().unwrap().clone()).unwrap();
        assert!(text.contains("tool_approval:0:0:w1"), "{text}");
        assert!(text.contains("write"), "{text}");
        assert!(text.contains("support/acct-1"), "{text}");
        assert!(!text.contains("4111"), "the arguments leak: {text}");
        assert!(!text.contains("secret"), "the reason leaks: {text}");
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
