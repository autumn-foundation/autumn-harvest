//! Names for durable approval waits.
//!
//! A gated tool call waits on a signal. The signal name holds the step, the
//! position in that step, and the tool-call id. Each name is therefore unique
//! to ONE wait in the whole run.
//!
//! This matters for a decision that arrives late. A deadline that fires first
//! resolves the wait as a timeout, and the late decision stays in history
//! unread. A name that a later wait shares would let that stale decision
//! release a call nobody reviewed. A name bound to one wait cannot match again.
//!
//! A model can reuse a tool-call id across steps, so the id alone is not
//! enough.
//!
//! Send each decision once. A second decision for a wait that already ended
//! stays unread in history. It changes nothing, but a strict replay check
//! can report it as a difference.

use std::time::Duration;

use autumn_harvest::context::WorkflowContext;
use serde::de::DeserializeOwned;
use serde_json::Value;

/// The prefix of every approval signal name.
pub const SIGNAL_TOOL_APPROVAL: &str = "tool_approval";

/// The signal name that releases one tool call.
///
/// `step` and `position` identify the wait. `call_id` names the call, so an
/// operator can read it from the name.
#[must_use]
pub fn approval_signal(step: u32, position: usize, call_id: &str) -> String {
    format!("{SIGNAL_TOOL_APPROVAL}:{step}:{position}:{call_id}")
}

/// The tool-call id that one approval signal name releases.
///
/// Returns `None` for a name that is not an approval signal.
#[must_use]
pub fn approval_call_id(signal_name: &str) -> Option<&str> {
    let mut parts = signal_name.splitn(4, ':');
    if parts.next()? != SIGNAL_TOOL_APPROVAL {
        return None;
    }
    parts.next()?.parse::<u32>().ok()?;
    parts.next()?.parse::<usize>().ok()?;
    parts.next()
}

/// How one approval wait ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision<T> {
    /// A reviewer sent a payload that decodes as `T`.
    Decided(T),
    /// A reviewer sent a payload that does not decode as `T`. The text says
    /// why. Treat it as a refusal: nobody approved anything the run can read.
    Unreadable(String),
    /// No decision arrived before the deadline.
    TimedOut,
}

/// Wait on the signal `name` until `timeout` passes.
///
/// The wait is durable: a restart resumes it, and its deadline holds. The
/// payload is decoded after it arrives. A payload of the wrong shape is
/// [`Decision::Unreadable`], so one bad signal cannot fail the run.
///
/// # Errors
///
/// Returns the engine error when the wait itself fails.
pub async fn await_decision<T: DeserializeOwned>(
    ctx: &WorkflowContext,
    name: &str,
    timeout: Duration,
) -> Result<Decision<T>, String> {
    let payload: Option<Value> = ctx
        .receive_signal_timeout(name, timeout)
        .await
        .map_err(|e| e.to_string())?;
    Ok(decode(payload))
}

/// Decode one received payload. `None` means the deadline passed.
fn decode<T: DeserializeOwned>(payload: Option<Value>) -> Decision<T> {
    payload.map_or(Decision::TimedOut, |value| {
        serde_json::from_value(value).map_or_else(
            |err| Decision::Unreadable(err.to_string()),
            Decision::Decided,
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn a_name_gives_back_its_call_id() {
        let name = approval_signal(2, 0, "toolu_1");
        assert_eq!(name, "tool_approval:2:0:toolu_1");
        assert_eq!(approval_call_id(&name), Some("toolu_1"));
    }

    #[test]
    fn a_foreign_or_malformed_name_has_no_call_id() {
        assert_eq!(approval_call_id("other:1:0:x"), None);
        assert_eq!(approval_call_id("tool_approval:1:0"), None);
        assert_eq!(approval_call_id("tool_approval:x:0:id"), None);
        assert_eq!(approval_call_id("tool_approval:1:y:id"), None);
        assert_eq!(approval_call_id(""), None);
    }

    #[test]
    fn a_payload_decodes_or_is_unreadable() {
        use autumn_plugin_agent::Approval;
        let ok: Decision<Approval> = decode(Some(serde_json::json!({"verdict": "approve"})));
        assert_eq!(ok, Decision::Decided(Approval::Approve));
        let bad: Decision<Approval> = decode(Some(serde_json::json!("approve")));
        assert!(matches!(bad, Decision::Unreadable(_)));
        let none: Decision<Approval> = decode(None);
        assert_eq!(none, Decision::TimedOut);
    }

    proptest! {
        #[test]
        fn approval_signal_round_trips(step: u32, position: usize, id in ".*") {
            let name = approval_signal(step, position, &id);
            prop_assert_eq!(approval_call_id(&name), Some(id.as_str()));
        }

        #[test]
        fn two_waits_never_share_a_name(
            a in (any::<u32>(), any::<usize>()),
            b in (any::<u32>(), any::<usize>()),
            id in "[a-z0-9_]{1,8}",
        ) {
            prop_assume!(a != b);
            prop_assert_ne!(approval_signal(a.0, a.1, &id), approval_signal(b.0, b.1, &id));
        }
    }
}
