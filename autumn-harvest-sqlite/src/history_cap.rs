//! The #1804 history caps on this backend (issue #2023).
//!
//! A run that reaches a cap fails with a [`HistoryCapReason`]. A run that
//! continues as new is exempt, because its successor starts a new history.

use autumn_harvest::{ExecutionId, WorkflowHistoryPolicy};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};

use crate::error::SqliteResult;
use crate::store;

/// Why a run failed at a history cap.
///
/// The JSON form is byte-identical to the Postgres `DeadLetterReason` of the
/// same name. A test on each side pins the same literal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum HistoryCapReason {
    /// The durable event count reached the event cap.
    HistoryCapExceeded {
        /// The event count that reached the cap.
        count: u64,
        /// The configured event cap.
        cap: u64,
        /// The workflow type of the run.
        workflow_type: String,
    },
    /// The stored history bytes reached the byte cap.
    ///
    /// `bytes` is the sum of the stored event JSON lengths. Postgres measures
    /// `pg_column_size` instead, so the two counts can differ.
    HistoryBytesCapExceeded {
        /// The stored bytes that reached the cap.
        bytes: u64,
        /// The configured byte cap.
        cap: u64,
        /// The workflow type of the run.
        workflow_type: String,
    },
}

impl HistoryCapReason {
    /// The serde `type` tag. The `WorkflowFailed` event uses it as `error_type`.
    #[must_use]
    pub const fn type_tag(&self) -> &'static str {
        match self {
            Self::HistoryCapExceeded { .. } => "HistoryCapExceeded",
            Self::HistoryBytesCapExceeded { .. } => "HistoryBytesCapExceeded",
        }
    }
}

impl std::fmt::Display for HistoryCapReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match serde_json::to_string(self) {
            Ok(json) => f.write_str(&json),
            Err(_) => f.write_str(self.type_tag()),
        }
    }
}

/// What one decision checks against the caps.
pub struct CapCheck<'a> {
    /// The configured caps.
    pub policy: WorkflowHistoryPolicy,
    /// The stored history bytes when the decision started.
    pub start_bytes: u64,
    /// The workflow type of the run, for the reason.
    pub workflow_type: &'a str,
}

impl CapCheck<'_> {
    /// The cap reached once this decision's events are in `conn`, if any.
    ///
    /// Call it inside the decision transaction, after its appends and before
    /// its terminal event. The count is then exact.
    ///
    /// # Errors
    ///
    /// Returns a persistence error if the event count cannot be read.
    pub fn reached(
        &self,
        conn: &Connection,
        exec: ExecutionId,
    ) -> SqliteResult<Option<HistoryCapReason>> {
        let count = u64::try_from(store::next_seq(conn, exec)?).unwrap_or(0);
        Ok(breach(
            self.policy,
            count,
            self.start_bytes,
            self.workflow_type,
        ))
    }
}

/// The cap that `count` events or `bytes` stored bytes reach, if any.
///
/// The event cap is checked first, as on Postgres. A cap of `None` is
/// unlimited.
#[must_use]
pub fn breach(
    policy: WorkflowHistoryPolicy,
    count: u64,
    bytes: u64,
    workflow_type: &str,
) -> Option<HistoryCapReason> {
    if let Some(cap) = policy.event_hard_cap()
        && count >= cap
    {
        return Some(HistoryCapReason::HistoryCapExceeded {
            count,
            cap,
            workflow_type: workflow_type.to_string(),
        });
    }
    if let Some(cap) = policy.byte_hard_cap()
        && bytes >= cap
    {
        return Some(HistoryCapReason::HistoryBytesCapExceeded {
            bytes,
            cap,
            workflow_type: workflow_type.to_string(),
        });
    }
    None
}

#[cfg(test)]
mod tests {
    use autumn_harvest::WorkflowHistoryPolicy;

    use super::{HistoryCapReason, breach};

    fn policy(events: u64, bytes: u64) -> WorkflowHistoryPolicy {
        WorkflowHistoryPolicy::default()
            .with_event_hard_cap(events)
            .with_byte_hard_cap(bytes)
    }

    #[test]
    fn a_count_below_both_caps_is_no_breach() {
        assert_eq!(breach(policy(10, 100), 9, 99, "wf"), None);
    }

    #[test]
    fn a_count_at_the_event_cap_is_a_breach() {
        assert_eq!(
            breach(policy(10, 100), 10, 0, "wf"),
            Some(HistoryCapReason::HistoryCapExceeded {
                count: 10,
                cap: 10,
                workflow_type: "wf".into(),
            })
        );
    }

    #[test]
    fn bytes_at_the_byte_cap_are_a_breach() {
        assert_eq!(
            breach(policy(10, 100), 0, 100, "wf"),
            Some(HistoryCapReason::HistoryBytesCapExceeded {
                bytes: 100,
                cap: 100,
                workflow_type: "wf".into(),
            })
        );
    }

    #[test]
    fn the_event_cap_wins_when_both_are_reached() {
        let reason = breach(policy(10, 100), 10, 100, "wf").unwrap();
        assert_eq!(reason.type_tag(), "HistoryCapExceeded");
    }

    #[test]
    fn an_unlimited_cap_is_never_reached() {
        let unlimited = WorkflowHistoryPolicy::default()
            .without_event_hard_cap()
            .without_byte_hard_cap();
        assert_eq!(breach(unlimited, u64::MAX, u64::MAX, "wf"), None);
    }
}
