//! Serializable DTOs + pure mapping for the single-execution replay-diagnosis
//! endpoint (`POST /api/harvest/workflows/{id}/replay-diagnosis`, issue #614).
//!
//! This module is deliberately pure: it owns the response shape and the total
//! mapping from a [`ReplayReport`](autumn_harvest::testing::ReplayReport) (or the
//! two "not replayable" pre-checks) to a [`ReplayDiagnosisResponse`]. The HTTP
//! handler in `api.rs` performs the I/O (load the execution + history from the
//! owning shard, drive [`WorkflowReplayer`](autumn_harvest::testing::WorkflowReplayer)
//! against the currently-registered handler) and calls the pure functions here,
//! so the mapping is unit-testable with no database and no async.
//!
//! The endpoint is **read-only** — it appends no events and performs no writes
//! (issue #614 AC3). It returns a structured `200` verdict for every reachable
//! outcome (clean / diverged / workflow_failed / not_registered /
//! not_replayable_dag), reserving non-`200` statuses for the resource itself:
//! `400` (malformed id), `404` (unknown execution), `408` (replay exceeded the
//! bounded budget), `410` (history unavailable — pruned by retention, released on
//! reset, or PII-erased per issue #495).

use autumn_harvest::testing::{NonDeterminismKind, ReplayReport, ReplayStatus};
use autumn_harvest::types::ExecutionId;
use serde::Serialize;

/// The verdict of replaying one execution's recorded history against the
/// currently-registered `#[workflow]` handler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosisVerdict {
    /// The workflow replayed the entire recorded history with no divergence —
    /// the current code can safely resume every in-flight execution that
    /// produced this history.
    Clean,
    /// The workflow issued a command that diverged from the recorded history:
    /// the current code is NOT replay-compatible with this run. `divergence`
    /// carries the classified `{kind, event_index, expected, actual}`.
    Diverged,
    /// The workflow function returned an error during replay (not a
    /// non-determinism divergence). `failure` carries `{error, event_index}`.
    WorkflowFailed,
    /// The execution's workflow type is not registered on this node, so there is
    /// no handler to replay against (issue #614 AC5). A `200` verdict — the
    /// diagnosis is "this node cannot answer", not an error.
    NotRegistered,
    /// The execution's workflow type is a classic (non-unified) DAG, which is not
    /// on the replay path (issue #614 AC5). A `200` verdict.
    NotReplayableDag,
}

/// The classified non-determinism divergence for a [`DiagnosisVerdict::Diverged`]
/// verdict. Mirrors the fields the #480/#603 non-determinism-block incident
/// signal records (`failure_cause=non_determinism`, `expected`, `actual`,
/// `event_index`) so an operator triaging an nd-blocked run reads the same
/// vocabulary here.
#[derive(Debug, Clone, Serialize)]
pub struct DivergenceDetail {
    /// The category of mismatch (activity/timer/signal/child/side-effect/...).
    pub kind: NonDeterminismKind,
    /// Approximate index into the event list where the divergence occurred.
    pub event_index: usize,
    /// What the recorded history expected at this position.
    pub expected: String,
    /// What the current workflow code actually requested.
    pub actual: String,
}

/// The error detail for a [`DiagnosisVerdict::WorkflowFailed`] verdict.
#[derive(Debug, Clone, Serialize)]
pub struct FailureDetail {
    /// The error string the workflow function returned.
    pub error: String,
    /// Index of the last event processed before the failure.
    pub event_index: usize,
}

/// The structured verdict returned by
/// `POST /api/harvest/workflows/{id}/replay-diagnosis`.
///
/// Serialized snake_case. `divergence`, `failure`, and `summary` are omitted
/// when absent so a clean verdict carries no null clutter.
#[derive(Debug, Clone, Serialize)]
pub struct ReplayDiagnosisResponse {
    /// The diagnosed execution id.
    pub execution_id: String,
    /// The execution's workflow type name.
    pub workflow_name: String,
    /// The execution's current lifecycle state (RUNNING / COMPLETED / FAILED /
    /// ...), for context — the diagnosis is independent of terminal state
    /// (issue #614 AC4: a FAILED run can be diagnosed retroactively).
    pub state: String,
    /// The verdict.
    pub diagnosis: DiagnosisVerdict,
    /// How many recorded events were processed before the verdict was reached.
    pub events_replayed: usize,
    /// The classified divergence, present iff `diagnosis == diverged`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub divergence: Option<DivergenceDetail>,
    /// The workflow error, present iff `diagnosis == workflow_failed`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure: Option<FailureDetail>,
    /// A one-line human summary of the mismatch (present for a divergence).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    /// A human-readable one-line explanation of the verdict.
    pub message: String,
}

/// Map a completed [`ReplayReport`] into the response DTO (issue #614).
///
/// Pure and total: every [`ReplayStatus`] variant maps to a distinct verdict.
#[must_use]
pub fn report_to_response(
    exec_id: ExecutionId,
    workflow_name: String,
    state: String,
    report: ReplayReport,
) -> ReplayDiagnosisResponse {
    let events_replayed = report.events_replayed;
    let summary = report.mismatched_command_summary.clone();
    match report.status {
        ReplayStatus::ReplaySucceeded => ReplayDiagnosisResponse {
            execution_id: exec_id.to_string(),
            workflow_name,
            state,
            diagnosis: DiagnosisVerdict::Clean,
            events_replayed,
            divergence: None,
            failure: None,
            // A clean replay has no mismatch summary; suppress it.
            summary: None,
            message: "no divergence under current code".to_string(),
        },
        ReplayStatus::NonDeterminismDetected {
            kind,
            expected,
            actual,
            event_index,
        } => ReplayDiagnosisResponse {
            execution_id: exec_id.to_string(),
            workflow_name,
            state,
            diagnosis: DiagnosisVerdict::Diverged,
            events_replayed,
            divergence: Some(DivergenceDetail {
                kind,
                event_index,
                expected,
                actual,
            }),
            failure: None,
            summary,
            message: "replay diverged from recorded history under current code".to_string(),
        },
        ReplayStatus::WorkflowFailed { error, event_index } => ReplayDiagnosisResponse {
            execution_id: exec_id.to_string(),
            workflow_name,
            state,
            diagnosis: DiagnosisVerdict::WorkflowFailed,
            events_replayed,
            divergence: None,
            failure: Some(FailureDetail { error, event_index }),
            summary,
            message: "workflow returned an error during replay".to_string(),
        },
    }
}

/// Build the `not_registered` verdict (issue #614 AC5): the execution's workflow
/// type is not registered on this node, so it cannot be replayed here. A `200`
/// verdict, not an error.
#[must_use]
pub fn not_registered_response(
    exec_id: ExecutionId,
    workflow_name: String,
    state: String,
) -> ReplayDiagnosisResponse {
    let message = format!(
        "workflow type '{workflow_name}' is not registered on this node; cannot replay its history here"
    );
    ReplayDiagnosisResponse {
        execution_id: exec_id.to_string(),
        workflow_name,
        state,
        diagnosis: DiagnosisVerdict::NotRegistered,
        events_replayed: 0,
        divergence: None,
        failure: None,
        summary: None,
        message,
    }
}

/// Build the `not_replayable_dag` verdict (issue #614 AC5): the execution is a
/// classic (non-unified) DAG run, which is not on the replay path. A `200`
/// verdict.
#[must_use]
pub fn not_replayable_dag_response(
    exec_id: ExecutionId,
    workflow_name: String,
    state: String,
) -> ReplayDiagnosisResponse {
    let message =
        format!("'{workflow_name}' is a classic DAG and is not replayable via this endpoint");
    ReplayDiagnosisResponse {
        execution_id: exec_id.to_string(),
        workflow_name,
        state,
        diagnosis: DiagnosisVerdict::NotReplayableDag,
        events_replayed: 0,
        divergence: None,
        failure: None,
        summary: None,
        message,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use autumn_harvest::types::{ExecutionId, ShardId};

    fn exec() -> ExecutionId {
        ExecutionId::new_for_shard(ShardId::new(0))
    }

    #[test]
    fn maps_replay_succeeded_to_clean() {
        let id = exec();
        let report = ReplayReport {
            execution_id: id,
            events_replayed: 7,
            status: ReplayStatus::ReplaySucceeded,
            mismatched_command_summary: None,
        };
        let resp = report_to_response(id, "wf".to_string(), "COMPLETED".to_string(), report);
        assert_eq!(resp.diagnosis, DiagnosisVerdict::Clean);
        assert_eq!(resp.events_replayed, 7);
        assert_eq!(resp.message, "no divergence under current code");
        assert!(resp.divergence.is_none());
        assert!(resp.failure.is_none());
        assert!(resp.summary.is_none());
        assert_eq!(resp.state, "COMPLETED");
        assert_eq!(resp.execution_id, id.to_string());
    }

    #[test]
    fn maps_non_determinism_to_diverged_with_all_fields() {
        let id = exec();
        let report = ReplayReport {
            execution_id: id,
            events_replayed: 3,
            status: ReplayStatus::NonDeterminismDetected {
                kind: NonDeterminismKind::ActivityScheduleMismatch,
                expected: "ActivityScheduled(send_email)".to_string(),
                actual: "ActivityScheduled(charge_card)".to_string(),
                event_index: 2,
            },
            mismatched_command_summary: Some("expected send_email, got charge_card".to_string()),
        };
        let resp = report_to_response(id, "wf".to_string(), "RUNNING".to_string(), report);
        assert_eq!(resp.diagnosis, DiagnosisVerdict::Diverged);
        let div = resp.divergence.expect("divergence populated");
        assert_eq!(div.kind, NonDeterminismKind::ActivityScheduleMismatch);
        assert_eq!(div.event_index, 2);
        assert_eq!(div.expected, "ActivityScheduled(send_email)");
        assert_eq!(div.actual, "ActivityScheduled(charge_card)");
        assert_eq!(
            resp.summary.as_deref(),
            Some("expected send_email, got charge_card")
        );
        assert!(resp.failure.is_none());
        assert_eq!(
            resp.message,
            "replay diverged from recorded history under current code"
        );
    }

    #[test]
    fn maps_workflow_failed_to_workflow_failed_with_failure() {
        let id = exec();
        let report = ReplayReport {
            execution_id: id,
            events_replayed: 5,
            status: ReplayStatus::WorkflowFailed {
                error: "boom".to_string(),
                event_index: 4,
            },
            mismatched_command_summary: None,
        };
        let resp = report_to_response(id, "wf".to_string(), "FAILED".to_string(), report);
        assert_eq!(resp.diagnosis, DiagnosisVerdict::WorkflowFailed);
        let fail = resp.failure.expect("failure populated");
        assert_eq!(fail.error, "boom");
        assert_eq!(fail.event_index, 4);
        assert!(resp.divergence.is_none());
        assert_eq!(resp.message, "workflow returned an error during replay");
    }

    #[test]
    fn not_registered_builds_verdict() {
        let id = exec();
        let resp = not_registered_response(id, "ghost".to_string(), "RUNNING".to_string());
        assert_eq!(resp.diagnosis, DiagnosisVerdict::NotRegistered);
        assert_eq!(resp.events_replayed, 0);
        assert!(resp.message.contains("not registered"));
        assert!(resp.divergence.is_none());
        assert!(resp.failure.is_none());
    }

    #[test]
    fn not_replayable_dag_builds_verdict() {
        let id = exec();
        let resp = not_replayable_dag_response(id, "my_dag".to_string(), "RUNNING".to_string());
        assert_eq!(resp.diagnosis, DiagnosisVerdict::NotReplayableDag);
        assert_eq!(resp.events_replayed, 0);
        assert!(resp.message.contains("classic DAG"));
    }

    #[test]
    fn serializes_snake_case_keys() {
        let id = exec();
        let report = ReplayReport {
            execution_id: id,
            events_replayed: 1,
            status: ReplayStatus::NonDeterminismDetected {
                kind: NonDeterminismKind::TimerMismatch,
                expected: "e".to_string(),
                actual: "a".to_string(),
                event_index: 0,
            },
            mismatched_command_summary: Some("s".to_string()),
        };
        let resp = report_to_response(id, "wf".to_string(), "RUNNING".to_string(), report);
        let v = serde_json::to_value(&resp).expect("serializes");
        // snake_case top-level keys.
        assert!(v.get("execution_id").is_some());
        assert!(v.get("workflow_name").is_some());
        assert!(v.get("events_replayed").is_some());
        // The verdict enum renders snake_case.
        assert_eq!(v["diagnosis"], serde_json::json!("diverged"));
        // Divergence nested object present with its own snake_case fields.
        assert!(v["divergence"]["event_index"].is_number());
        assert_eq!(v["divergence"]["kind"], serde_json::json!("TimerMismatch"));
        // Absent optionals are omitted, not null.
        assert!(v.get("failure").is_none());
    }
}
