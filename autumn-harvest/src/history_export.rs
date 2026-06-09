//! Export workflow event histories to replay fixtures and Mermaid diagrams.

use crate::event::WorkflowEvent;
use crate::types::ExecutionId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::fmt::Write;
use std::str::FromStr;

/// Schema identifier for history export documents.
pub const HISTORY_EXPORT_SCHEMA: &str = "autumn-harvest.history-export";

/// Current history export schema version.
pub const HISTORY_EXPORT_VERSION: u32 = 1;

/// Default maximum serialized export size, in bytes.
pub const DEFAULT_HISTORY_EXPORT_MAX_BYTES: usize = 10 * 1024 * 1024;

/// How payload-bearing event fields should be emitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryPayloadPolicy {
    /// Emit raw event payloads. Use only for private replay fixtures.
    Full,
    /// Replace payload-bearing fields and tokens with deterministic summaries.
    Redacted,
}

impl HistoryPayloadPolicy {
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Redacted => "redacted",
        }
    }
}

impl std::fmt::Display for HistoryPayloadPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_wire())
    }
}

impl FromStr for HistoryPayloadPolicy {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "full" => Ok(Self::Full),
            "redacted" | "summary" | "summarized" => Ok(Self::Redacted),
            other => Err(format!(
                "unknown payload_policy '{other}'; expected 'redacted' or 'full'"
            )),
        }
    }
}

/// Runtime status metadata included with an export.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryExportStatus {
    /// Current workflow execution state.
    pub state: String,
    /// Whether `state` is terminal from Harvest's workflow-state perspective.
    pub terminal: bool,
}

/// Machine-readable export size policy and measurement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryExportSizeLimit {
    /// Caller-selected byte limit for the serialized export document.
    pub max_bytes: usize,
    /// Serialized document size after applying the selected payload policy.
    pub actual_bytes: usize,
    /// Whether the history was truncated. Harvest exports currently fail
    /// oversized histories instead of truncating them.
    pub truncated: bool,
    /// The behavior used when an export would exceed `max_bytes`.
    pub truncation_behavior: String,
}

/// Replay-compatible workflow history export document.
///
/// In [`HistoryPayloadPolicy::Full`] mode this struct serializes with the same
/// top-level `workflow_name`, `execution_id`, and `events` fields required by
/// `testing::HistorySnapshot`. Extra metadata fields are ignored by
/// `WorkflowReplayer::replay_from_json`, so full exports can be replayed
/// without transformation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryExportDocument {
    /// Stable export schema identifier.
    pub schema: String,
    /// Export schema version.
    pub version: u32,
    /// Registered workflow handler name.
    pub workflow_name: String,
    /// Workflow execution whose history was exported.
    pub execution_id: ExecutionId,
    /// Shard that owns the execution.
    pub shard_id: i32,
    /// Wall-clock time when the export document was produced.
    pub exported_at: DateTime<Utc>,
    /// Number of history events in `events`.
    pub event_count: usize,
    /// Terminal or in-flight status metadata.
    pub status: HistoryExportStatus,
    /// Payload emission policy used for `events`.
    pub payload_policy: HistoryPayloadPolicy,
    /// Export size policy and measurement.
    pub size_limit: HistoryExportSizeLimit,
    /// Ordered history event data. Full exports contain raw `WorkflowEvent`
    /// JSON. Redacted exports preserve event shape but summarize sensitive
    /// payload-bearing fields.
    pub events: Vec<Value>,
}

/// Input needed to export one workflow execution history.
#[derive(Debug, Clone)]
pub struct HistoryExportRequest {
    /// Registered workflow handler name.
    pub workflow_name: String,
    /// Workflow execution whose history should be exported.
    pub execution_id: ExecutionId,
    /// Shard that owns the execution.
    pub shard_id: i32,
    /// Current workflow execution state.
    pub state: String,
    /// Ordered event history.
    pub events: Vec<WorkflowEvent>,
    /// Wall-clock time to stamp on the export.
    pub exported_at: DateTime<Utc>,
    /// Payload emission policy.
    pub payload_policy: HistoryPayloadPolicy,
    /// Optional serialized byte limit. Defaults to
    /// [`DEFAULT_HISTORY_EXPORT_MAX_BYTES`].
    pub max_bytes: Option<usize>,
}

/// History export failure modes.
#[derive(Debug, thiserror::Error)]
pub enum HistoryExportError {
    /// Event or document JSON serialization failed.
    #[error("failed to serialize history export: {0}")]
    Serialization(#[from] serde_json::Error),
    /// The serialized export exceeded the caller-selected size limit.
    #[error("history export is {actual_bytes} bytes, exceeding max_bytes={max_bytes}")]
    SizeLimitExceeded {
        /// Serialized document size after applying the selected payload policy.
        actual_bytes: usize,
        /// Caller-selected size limit.
        max_bytes: usize,
    },
}

/// Build a replay-compatible history export document.
///
/// # Errors
///
/// Returns [`HistoryExportError::Serialization`] if event serialization fails
/// or [`HistoryExportError::SizeLimitExceeded`] when the serialized document
/// exceeds the requested limit.
pub fn export_history(
    request: HistoryExportRequest,
) -> Result<HistoryExportDocument, HistoryExportError> {
    let max_bytes = request
        .max_bytes
        .unwrap_or(DEFAULT_HISTORY_EXPORT_MAX_BYTES);
    let events = export_events(&request.events, request.payload_policy)?;
    let mut document = HistoryExportDocument {
        schema: HISTORY_EXPORT_SCHEMA.to_string(),
        version: HISTORY_EXPORT_VERSION,
        workflow_name: request.workflow_name,
        execution_id: request.execution_id,
        shard_id: request.shard_id,
        exported_at: request.exported_at,
        event_count: events.len(),
        status: HistoryExportStatus {
            terminal: history_state_is_terminal(&request.state),
            state: request.state,
        },
        payload_policy: request.payload_policy,
        size_limit: HistoryExportSizeLimit {
            max_bytes,
            actual_bytes: 0,
            truncated: false,
            truncation_behavior: "fail".to_string(),
        },
        events,
    };

    let actual_bytes = measure_export_bytes(&mut document)?;
    if actual_bytes > max_bytes {
        return Err(HistoryExportError::SizeLimitExceeded {
            actual_bytes,
            max_bytes,
        });
    }
    Ok(document)
}

fn export_events(
    events: &[WorkflowEvent],
    payload_policy: HistoryPayloadPolicy,
) -> Result<Vec<Value>, HistoryExportError> {
    events
        .iter()
        .map(|event| match payload_policy {
            HistoryPayloadPolicy::Full => serde_json::to_value(event).map_err(Into::into),
            HistoryPayloadPolicy::Redacted => redacted_event_value(event),
        })
        .collect()
}

fn redacted_event_value(event: &WorkflowEvent) -> Result<Value, HistoryExportError> {
    let mut value = serde_json::to_value(event)?;
    redact_value(&mut value)?;
    Ok(value)
}

fn redact_value(value: &mut Value) -> Result<(), HistoryExportError> {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                if is_payload_field(key) {
                    *child = redacted_summary(child, "payload")?;
                } else if is_token_field(key) {
                    *child = redacted_summary(child, "token")?;
                } else {
                    redact_value(child)?;
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                redact_value(item)?;
            }
        }
        Value::String(text) if contains_bearer_token(text) => {
            let original = Value::String(text.clone());
            *value = redacted_summary(&original, "token")?;
        }
        _ => {}
    }
    Ok(())
}

fn is_payload_field(key: &str) -> bool {
    // `value` is the arbitrary `ctx.side_effect(...)` result on a
    // `SideEffectRecorded` event (issue #384). Before #384 custom side effects
    // were stored under `MarkerRecorded.details` and redacted via "details"; the
    // new field must be redacted too so secrets/PII captured by the closure are
    // not leaked in a redacted export.
    matches!(key, "input" | "output" | "payload" | "details" | "value")
}

fn is_token_field(key: &str) -> bool {
    matches!(
        key,
        "token" | "authorization" | "auth_token" | "bearer_token"
    )
}

fn contains_bearer_token(text: &str) -> bool {
    text.to_ascii_lowercase().contains("bearer ")
}

fn redacted_summary(value: &Value, kind: &'static str) -> Result<Value, HistoryExportError> {
    let bytes = serde_json::to_vec(value)?;
    let digest = seahash::hash(&bytes);
    Ok(json!({
        "redacted": true,
        "kind": kind,
        "payload_size_bytes": bytes.len(),
        "payload_digest": format!("seahash64:{digest:016x}"),
    }))
}

fn measure_export_bytes(document: &mut HistoryExportDocument) -> Result<usize, HistoryExportError> {
    let mut last = 0;
    for _ in 0..4 {
        let actual = serde_json::to_vec(document)?.len();
        if actual == last {
            return Ok(actual);
        }
        document.size_limit.actual_bytes = actual;
        last = actual;
    }
    Ok(last)
}

fn history_state_is_terminal(state: &str) -> bool {
    matches!(
        state,
        "COMPLETED" | "FAILED" | "CANCELLED" | "TIMED_OUT" | "CONTINUED_AS_NEW" | "TERMINATED"
    )
}

/// Export a slice of workflow events into a Mermaid.js sequence diagram.
///
/// This provides an intuitive visual representation of the workflow's execution
/// lifecycle, making it easier to debug timing issues, parallel activities, and
/// system interactions.
///
/// # Errors
/// Returns `std::fmt::Error` if string formatting fails.
pub fn export_mermaid_sequence(events: &[WorkflowEvent]) -> Result<String, std::fmt::Error> {
    let mut exporter = MermaidExporter::new();
    exporter.export(events)?;
    Ok(exporter.out)
}

struct MermaidExporter {
    out: String,
    participants: std::collections::HashSet<String>,
}

impl MermaidExporter {
    fn new() -> Self {
        Self {
            out: String::new(),
            participants: std::collections::HashSet::new(),
        }
    }

    fn export(&mut self, events: &[WorkflowEvent]) -> Result<(), std::fmt::Error> {
        writeln!(self.out, "sequenceDiagram")?;
        writeln!(self.out, "    autonumber")?;
        writeln!(self.out, "    participant WF as Workflow")?;

        // We'll keep track of dynamic participants to avoid re-declaring them.
        self.participants.insert("WF".to_string());

        for event in events {
            match event {
                WorkflowEvent::WorkflowStarted { .. }
                | WorkflowEvent::WorkflowCompleted { .. }
                | WorkflowEvent::WorkflowFailed { .. }
                | WorkflowEvent::WorkflowCancelled { .. }
                | WorkflowEvent::WorkflowContinuedAsNew { .. }
                | WorkflowEvent::WorkflowResetFork { .. }
                | WorkflowEvent::WorkflowResetTerminated { .. }
                | WorkflowEvent::WorkflowExecutionTimedOut { .. }
                | WorkflowEvent::WorkflowExecutionPaused { .. }
                | WorkflowEvent::WorkflowExecutionResumed { .. } => {
                    self.handle_workflow_event(event)?;
                }
                WorkflowEvent::ActivityScheduled { .. }
                | WorkflowEvent::ActivityStarted { .. }
                | WorkflowEvent::ActivityCompleted { .. }
                | WorkflowEvent::ActivityFailed { .. }
                | WorkflowEvent::ActivityTimedOut { .. }
                | WorkflowEvent::ActivityHeartbeat { .. } => {
                    self.handle_activity_event(event)?;
                }
                WorkflowEvent::TimerStarted { .. } | WorkflowEvent::TimerFired { .. } => {
                    self.handle_timer_event(event)?;
                }
                WorkflowEvent::ChildWorkflowStarted { .. }
                | WorkflowEvent::ChildWorkflowCompleted { .. }
                | WorkflowEvent::ChildWorkflowFailed { .. }
                | WorkflowEvent::ChildWorkflowSpawnedDetached { .. }
                | WorkflowEvent::ChildWorkflowCascadeApplied { .. } => {
                    self.handle_child_workflow_event(event)?;
                }
                WorkflowEvent::SignalReceived { .. }
                | WorkflowEvent::MarkerRecorded { .. }
                | WorkflowEvent::SideEffectRecorded { .. } => {
                    self.handle_misc_event(event)?;
                }
                WorkflowEvent::ActivityAwaitingExternal { .. }
                | WorkflowEvent::ActivityCompletedExternally { .. }
                | WorkflowEvent::ActivityFailedExternally { .. }
                | WorkflowEvent::ActivityExternalDeadlineExtended { .. } => {
                    self.handle_external_activity_event(event)?;
                }
                WorkflowEvent::LocalActivityScheduled { .. }
                | WorkflowEvent::LocalActivityCompleted { .. }
                | WorkflowEvent::LocalActivityFailed { .. }
                | WorkflowEvent::LocalActivityExhausted { .. } => {
                    self.handle_local_activity_event(event)?;
                }
                WorkflowEvent::UpdateAdmitted { .. }
                | WorkflowEvent::UpdateCompleted { .. }
                | WorkflowEvent::UpdateFailed { .. } => {
                    self.handle_update_event(event)?;
                }
                WorkflowEvent::ExternalSignalRequested { .. }
                | WorkflowEvent::ExternalSignalDelivered { .. }
                | WorkflowEvent::ExternalSignalFailed { .. } => {
                    self.handle_external_signal_event(event)?;
                }
            }
        }
        Ok(())
    }

    fn handle_workflow_event(&mut self, event: &WorkflowEvent) -> Result<(), std::fmt::Error> {
        match event {
            WorkflowEvent::WorkflowStarted { .. } => {
                writeln!(self.out, "    Note over WF: Workflow Started")?;
            }
            WorkflowEvent::WorkflowCompleted { .. } => {
                writeln!(self.out, "    Note over WF: Workflow Completed")?;
            }
            WorkflowEvent::WorkflowFailed { error } => {
                let safe_error = error.replace('\n', " ").replace('"', "'");
                writeln!(self.out, "    Note over WF: Workflow Failed: {safe_error}")?;
            }
            WorkflowEvent::WorkflowCancelled { reason } => {
                let safe_reason = reason.replace('\n', " ").replace('"', "'");
                writeln!(
                    self.out,
                    "    Note over WF: Workflow Cancelled: {safe_reason}"
                )?;
            }
            WorkflowEvent::WorkflowContinuedAsNew { new_exec_id, .. } => {
                writeln!(
                    self.out,
                    "    Note over WF: Continued As New (next: {new_exec_id})"
                )?;
            }
            WorkflowEvent::WorkflowResetFork {
                reset_from_exec_id,
                reset_to_event_id,
                reason,
                ..
            } => {
                let safe_reason = reason.replace('\n', " ").replace('"', "'");
                writeln!(
                    self.out,
                    "    Note over WF: Reset Fork from {reset_from_exec_id} at event {reset_to_event_id}: {safe_reason}"
                )?;
            }
            WorkflowEvent::WorkflowResetTerminated {
                reset_to_exec_id,
                reason,
                ..
            } => {
                let safe_reason = reason.replace('\n', " ").replace('"', "'");
                writeln!(
                    self.out,
                    "    Note over WF: Reset Terminated (fork: {reset_to_exec_id}): {safe_reason}"
                )?;
            }
            WorkflowEvent::WorkflowExecutionTimedOut { deadline, .. } => {
                writeln!(
                    self.out,
                    "    Note over WF: Execution Timed Out (deadline: {deadline})"
                )?;
            }
            WorkflowEvent::WorkflowExecutionPaused { actor, reason, .. } => {
                let detail = reason
                    .as_deref()
                    .map(|r| format!(": {}", r.replace('\n', " ").replace('"', "'")))
                    .unwrap_or_default();
                writeln!(self.out, "    Note over WF: Paused by {actor}{detail}")?;
            }
            WorkflowEvent::WorkflowExecutionResumed { actor, .. } => {
                writeln!(self.out, "    Note over WF: Resumed by {actor}")?;
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn handle_activity_event(&mut self, event: &WorkflowEvent) -> Result<(), std::fmt::Error> {
        match event {
            WorkflowEvent::ActivityScheduled {
                name, activity_id, ..
            } => {
                let participant = format!("Activity_{name}");
                if self.participants.insert(participant.clone()) {
                    writeln!(
                        self.out,
                        "    participant {participant} as Activity: {name}"
                    )?;
                }
                writeln!(
                    self.out,
                    "    WF->>+{participant}: Schedule (ID: {activity_id})"
                )?;
            }
            WorkflowEvent::ActivityStarted {
                worker_id,
                activity_id,
                ..
            } => {
                // Without mapping activity_id to name, we use a generic Worker.
                // In a perfect world, we'd track activity_id -> name, but let's keep it simple.
                writeln!(
                    self.out,
                    "    Note right of WF: Activity Started (ID: {activity_id}) on {worker_id}"
                )?;
            }
            WorkflowEvent::ActivityCompleted { activity_id, .. } => {
                // Note: since we lack the activity name here, we'll draw it back to WF generally
                // or just use a note. To do an arrow, we would need to map activity_id -> participant.
                writeln!(
                    self.out,
                    "    Note right of WF: Activity Completed (ID: {activity_id})"
                )?;
            }
            WorkflowEvent::ActivityFailed {
                activity_id,
                error,
                attempt,
                ..
            } => {
                let safe_error = error.replace('\n', " ").replace('"', "'");
                writeln!(
                    self.out,
                    "    Note right of WF: Activity Failed (ID: {activity_id}, Attempt: {attempt}): {safe_error}"
                )?;
            }
            WorkflowEvent::ActivityTimedOut {
                activity_id,
                timeout_type,
            } => {
                writeln!(
                    self.out,
                    "    Note right of WF: Activity Timed Out (ID: {activity_id}, Type: {timeout_type:?})"
                )?;
            }
            WorkflowEvent::ActivityHeartbeat { activity_id, .. } => {
                writeln!(
                    self.out,
                    "    Note right of WF: Activity Heartbeat (ID: {activity_id})"
                )?;
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn handle_timer_event(&mut self, event: &WorkflowEvent) -> Result<(), std::fmt::Error> {
        match event {
            WorkflowEvent::TimerStarted {
                timer_id,
                duration_secs,
            } => {
                let participant = "Timer";
                if self.participants.insert(participant.to_string()) {
                    writeln!(self.out, "    participant {participant} as Timer")?;
                }
                writeln!(
                    self.out,
                    "    WF->>+{participant}: Start Timer {timer_id} ({duration_secs}s)"
                )?;
            }
            WorkflowEvent::TimerFired { timer_id } => {
                let participant = "Timer";
                writeln!(self.out, "    {participant}-->>-WF: Timer {timer_id} Fired")?;
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn handle_child_workflow_event(
        &mut self,
        event: &WorkflowEvent,
    ) -> Result<(), std::fmt::Error> {
        match event {
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name,
                ..
            } => {
                let participant = format!("Child_{workflow_name}");
                if self.participants.insert(participant.clone()) {
                    writeln!(
                        self.out,
                        "    participant {participant} as Child: {workflow_name}"
                    )?;
                }
                writeln!(
                    self.out,
                    "    WF->>+{participant}: Start Child (ID: {child_id})"
                )?;
            }
            WorkflowEvent::ChildWorkflowCompleted { child_id, .. } => {
                writeln!(
                    self.out,
                    "    Note right of WF: Child Workflow Completed (ID: {child_id})"
                )?;
            }
            WorkflowEvent::ChildWorkflowFailed { child_id, error } => {
                let safe_error = error.replace('\n', " ").replace('"', "'");
                writeln!(
                    self.out,
                    "    Note right of WF: Child Workflow Failed (ID: {child_id}): {safe_error}"
                )?;
            }
            WorkflowEvent::ChildWorkflowSpawnedDetached {
                child_id,
                workflow_name,
                parent_close_policy,
                ..
            } => {
                let participant = format!("Child_{workflow_name}");
                if self.participants.insert(participant.clone()) {
                    writeln!(
                        self.out,
                        "    participant {participant} as Child: {workflow_name}"
                    )?;
                }
                writeln!(
                    self.out,
                    "    WF-->>{participant}: Spawn Detached (ID: {child_id}, policy: {policy})",
                    policy = parent_close_policy.as_str(),
                )?;
            }
            WorkflowEvent::ChildWorkflowCascadeApplied {
                child_id, action, ..
            } => {
                writeln!(
                    self.out,
                    "    Note right of WF: Cascade Applied to Child {child_id}: {action}"
                )?;
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn handle_misc_event(&mut self, event: &WorkflowEvent) -> Result<(), std::fmt::Error> {
        match event {
            WorkflowEvent::SignalReceived { signal_name, .. } => {
                let participant = "External";
                if self.participants.insert(participant.to_string()) {
                    writeln!(self.out, "    participant {participant} as External")?;
                }
                writeln!(
                    self.out,
                    "    {participant}->>WF: Signal Received: {signal_name}"
                )?;
            }
            WorkflowEvent::MarkerRecorded { name, .. } => {
                writeln!(self.out, "    Note over WF: Marker: {name}")?;
            }
            WorkflowEvent::SideEffectRecorded { kind, name, .. } => {
                let label = name.as_deref().unwrap_or(kind.as_str());
                writeln!(self.out, "    Note over WF: Side Effect: {label}")?;
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn handle_local_activity_event(
        &mut self,
        event: &WorkflowEvent,
    ) -> Result<(), std::fmt::Error> {
        match event {
            WorkflowEvent::LocalActivityScheduled {
                name, activity_id, ..
            } => {
                writeln!(
                    self.out,
                    "    Note right of WF: Local Activity Scheduled: {name} (ID: {activity_id})"
                )?;
            }
            WorkflowEvent::LocalActivityCompleted { activity_id, .. } => {
                writeln!(
                    self.out,
                    "    Note right of WF: Local Activity Completed (ID: {activity_id})"
                )?;
            }
            WorkflowEvent::LocalActivityFailed {
                activity_id,
                error,
                attempt,
            } => {
                let safe_error = error.replace('\n', " ").replace('"', "'");
                writeln!(
                    self.out,
                    "    Note right of WF: Local Activity Failed (ID: {activity_id}, Attempt: {attempt}): {safe_error}"
                )?;
            }
            WorkflowEvent::LocalActivityExhausted {
                activity_id,
                error,
                attempt,
            } => {
                let safe_error = error.replace('\n', " ").replace('"', "'");
                writeln!(
                    self.out,
                    "    Note right of WF: Local Activity Exhausted (ID: {activity_id}, Attempts: {attempt}): {safe_error}"
                )?;
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn handle_external_activity_event(
        &mut self,
        event: &WorkflowEvent,
    ) -> Result<(), std::fmt::Error> {
        let participant = "ExternalSystem";
        if self.participants.insert(participant.to_string()) {
            writeln!(self.out, "    participant {participant} as External System")?;
        }
        match event {
            WorkflowEvent::ActivityAwaitingExternal { name, token, .. } => {
                writeln!(
                    self.out,
                    "    WF->>{participant}: Awaiting External: {name} (token: {token})"
                )?;
            }
            WorkflowEvent::ActivityCompletedExternally { token, .. } => {
                writeln!(
                    self.out,
                    "    {participant}->>WF: Completed Externally (token: {token})"
                )?;
            }
            WorkflowEvent::ActivityFailedExternally { token, error, .. } => {
                let safe_error = error.replace('\n', " ").replace('"', "'");
                writeln!(
                    self.out,
                    "    {participant}->>WF: Failed Externally (token: {token}): {safe_error}"
                )?;
            }
            WorkflowEvent::ActivityExternalDeadlineExtended { token, .. } => {
                writeln!(
                    self.out,
                    "    Note right of WF: Deadline Extended (token: {token})"
                )?;
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn handle_update_event(&mut self, event: &WorkflowEvent) -> Result<(), std::fmt::Error> {
        match event {
            WorkflowEvent::UpdateAdmitted {
                update_id, name, ..
            } => {
                writeln!(
                    self.out,
                    "    Note over WF: Update Admitted: {name} (id: {update_id})"
                )?;
            }
            WorkflowEvent::UpdateCompleted { update_id, .. } => {
                writeln!(
                    self.out,
                    "    Note over WF: Update Completed (id: {update_id})"
                )?;
            }
            WorkflowEvent::UpdateFailed { update_id, error } => {
                let safe_error = error.replace('\n', " ").replace('"', "'");
                writeln!(
                    self.out,
                    "    Note over WF: Update Failed (id: {update_id}): {safe_error}"
                )?;
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn handle_external_signal_event(
        &mut self,
        event: &WorkflowEvent,
    ) -> Result<(), std::fmt::Error> {
        match event {
            WorkflowEvent::ExternalSignalRequested {
                signal_id,
                target,
                signal_name,
                ..
            } => {
                writeln!(
                    self.out,
                    "    Note over WF: Signal Requested: {signal_name} → {target} (id: {signal_id})"
                )?;
            }
            WorkflowEvent::ExternalSignalDelivered { signal_id } => {
                writeln!(
                    self.out,
                    "    Note over WF: Signal Delivered (id: {signal_id})"
                )?;
            }
            WorkflowEvent::ExternalSignalFailed {
                signal_id,
                reason_code,
            } => {
                writeln!(
                    self.out,
                    "    Note over WF: Signal Failed ({reason_code}) (id: {signal_id})"
                )?;
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ActivityExecId;
    use chrono::Utc;

    #[test]
    fn test_export_mermaid_sequence_empty() {
        let events = vec![];
        let diagram = export_mermaid_sequence(&events).expect("export should succeed");
        assert!(diagram.contains("sequenceDiagram"));
        assert!(diagram.contains("participant WF as Workflow"));
    }

    #[test]
    fn test_export_mermaid_sequence_basic_workflow() {
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: serde_json::json!({}),
                timestamp: Utc::now(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: ActivityExecId::new(),
                name: "download_file".to_string(),
                input: serde_json::json!({}),
                queue: "default".to_string(),
            },
            WorkflowEvent::WorkflowCompleted {
                output: serde_json::json!({}),
            },
        ];

        let diagram = export_mermaid_sequence(&events).unwrap();
        assert!(diagram.contains("Note over WF: Workflow Started"));
        assert!(diagram.contains("participant Activity_download_file as Activity: download_file"));
        assert!(diagram.contains("WF->>+Activity_download_file: Schedule (ID: "));
        assert!(diagram.contains("Note over WF: Workflow Completed"));
    }

    #[tokio::test]
    async fn full_history_export_round_trips_through_replayer_json() {
        use crate::context::WorkflowContext;
        use crate::testing::{ReplayStatus, WorkflowReplayer};
        use crate::types::ExecutionId;
        use serde_json::Value;
        use std::future::Future;
        use std::pin::Pin;

        fn echo_workflow<'a>(
            _ctx: &'a WorkflowContext,
            input: Value,
        ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
            Box::pin(async move { Ok(input) })
        }

        let execution_id = ExecutionId::new();
        let document = export_history(HistoryExportRequest {
            workflow_name: "echo".to_string(),
            execution_id,
            shard_id: 3,
            state: "COMPLETED".to_string(),
            events: vec![
                WorkflowEvent::WorkflowStarted {
                    input: serde_json::json!({ "customer": "acme" }),
                    timestamp: Utc::now(),
                },
                WorkflowEvent::WorkflowCompleted {
                    output: serde_json::json!({ "ok": true }),
                },
            ],
            exported_at: Utc::now(),
            payload_policy: HistoryPayloadPolicy::Full,
            max_bytes: Some(64 * 1024),
        })
        .expect("full export should fit under the limit");

        let json = serde_json::to_string(&document).expect("export should serialize");
        let report = WorkflowReplayer::new()
            .register_fn("echo", echo_workflow)
            .replay_from_json(&json)
            .await
            .expect("full export must be accepted by replay_from_json");

        assert!(matches!(report.status, ReplayStatus::ReplaySucceeded));
        assert_eq!(document.schema, "autumn-harvest.history-export");
        assert_eq!(document.version, 1);
        assert_eq!(document.workflow_name, "echo");
        assert_eq!(document.execution_id, execution_id);
        assert_eq!(document.shard_id, 3);
        assert_eq!(document.event_count, 2);
        assert!(document.status.terminal);
        assert!(!document.size_limit.truncated);
    }

    #[test]
    fn redacted_history_export_preserves_names_without_raw_payloads_or_tokens() {
        use crate::types::{ActivityExecId, ExecutionId, ExternalActivityToken};

        let activity_id = ActivityExecId::new();
        let token = ExternalActivityToken::new();
        let document = export_history(HistoryExportRequest {
            workflow_name: "billing_checkout".to_string(),
            execution_id: ExecutionId::new(),
            shard_id: 1,
            state: "RUNNING".to_string(),
            events: vec![
                WorkflowEvent::WorkflowStarted {
                    input: serde_json::json!({
                        "card": "4111111111111111",
                        "authorization": "Bearer top-secret"
                    }),
                    timestamp: Utc::now(),
                },
                WorkflowEvent::ActivityScheduled {
                    activity_id,
                    name: "charge_card".to_string(),
                    input: serde_json::json!({ "cvv": "123" }),
                    queue: "payments".to_string(),
                },
                WorkflowEvent::SignalReceived {
                    signal_name: "manager_override".to_string(),
                    payload: serde_json::json!({ "approved_by": "root" }),
                },
                WorkflowEvent::ActivityAwaitingExternal {
                    activity_id,
                    token,
                    name: "settlement_webhook".to_string(),
                    input: serde_json::json!({ "settlement_secret": "raw-secret" }),
                    queue: "payments".to_string(),
                    schedule_to_close_secs: 300,
                },
                WorkflowEvent::WorkflowFailed {
                    error: "downstream returned bearer lower-secret".to_string(),
                },
            ],
            exported_at: Utc::now(),
            payload_policy: HistoryPayloadPolicy::Redacted,
            max_bytes: Some(64 * 1024),
        })
        .expect("redacted export should fit under the limit");

        let json = serde_json::to_string(&document).expect("export should serialize");
        assert!(json.contains("WorkflowStarted"));
        assert!(json.contains("ActivityScheduled"));
        assert!(json.contains("charge_card"));
        assert!(json.contains("manager_override"));
        assert!(json.contains("settlement_webhook"));
        assert!(json.contains("payload_size_bytes"));
        assert!(json.contains("payload_digest"));
        assert!(!json.contains("4111111111111111"));
        assert!(!json.contains("Bearer top-secret"));
        assert!(!json.contains("lower-secret"));
        assert!(!json.contains("raw-secret"));
        assert!(!json.contains(&token.to_string()));

        let value: serde_json::Value =
            serde_json::from_str(&json).expect("redacted export should be valid JSON");
        assert_eq!(value["payload_policy"], "redacted");
        assert_eq!(value["status"]["state"], "RUNNING");
        assert_eq!(value["status"]["terminal"], false);
        assert_eq!(value["events"][0]["data"]["input"]["redacted"], true);
        assert_eq!(value["events"][3]["data"]["token"]["redacted"], true);
    }

    #[test]
    fn redacted_history_export_redacts_side_effect_recorded_value() {
        use crate::event::SideEffectKind;
        use crate::types::ExecutionId;

        let document = export_history(HistoryExportRequest {
            workflow_name: "secret_capture".to_string(),
            execution_id: ExecutionId::new(),
            shard_id: 0,
            state: "RUNNING".to_string(),
            events: vec![
                WorkflowEvent::WorkflowStarted {
                    input: serde_json::json!({}),
                    timestamp: Utc::now(),
                },
                // A custom side_effect that captured a secret in its closure. Pre
                // #384 this lived under MarkerRecorded.details and was redacted;
                // it must remain redacted under the new SideEffectRecorded.value.
                WorkflowEvent::SideEffectRecorded {
                    kind: SideEffectKind::Custom,
                    name: Some("api_credential".to_string()),
                    value: serde_json::json!({ "token": "super-secret-credential" }),
                },
            ],
            exported_at: Utc::now(),
            payload_policy: HistoryPayloadPolicy::Redacted,
            max_bytes: Some(64 * 1024),
        })
        .expect("redacted export should fit under the limit");

        let json = serde_json::to_string(&document).expect("export should serialize");
        // Event shape and the side-effect name remain visible for debugging…
        assert!(json.contains("SideEffectRecorded"));
        assert!(json.contains("api_credential"));
        // …but the captured value must not leak.
        assert!(!json.contains("super-secret-credential"));

        let value: serde_json::Value =
            serde_json::from_str(&json).expect("redacted export should be valid JSON");
        assert_eq!(value["events"][1]["data"]["value"]["redacted"], true);
    }

    #[test]
    fn history_export_size_limit_fails_with_machine_readable_metadata() {
        use crate::types::ExecutionId;

        let error = export_history(HistoryExportRequest {
            workflow_name: "oversized".to_string(),
            execution_id: ExecutionId::new(),
            shard_id: 0,
            state: "COMPLETED".to_string(),
            events: vec![WorkflowEvent::WorkflowStarted {
                input: serde_json::json!({ "large": "x".repeat(512) }),
                timestamp: Utc::now(),
            }],
            exported_at: Utc::now(),
            payload_policy: HistoryPayloadPolicy::Full,
            max_bytes: Some(128),
        })
        .expect_err("oversized full export must fail unless limit is raised");

        match error {
            HistoryExportError::SizeLimitExceeded {
                actual_bytes,
                max_bytes,
            } => {
                assert!(actual_bytes > max_bytes);
                assert_eq!(max_bytes, 128);
            }
            HistoryExportError::Serialization(error) => {
                panic!("unexpected serialization error: {error}")
            }
        }
    }

    #[test]
    #[should_panic(expected = "internal error: entered unreachable code")]
    fn test_handle_workflow_event_unreachable_panics() {
        let mut exporter = MermaidExporter::new();
        // WorkflowCompleted is not an unreachable arm, but ActivityScheduled is (for workflow event handler)
        let event = WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "test".to_string(),
            input: serde_json::json!({}),
            queue: "default".to_string(),
        };
        let _ = exporter.handle_workflow_event(&event);
    }

    #[test]
    #[should_panic(expected = "internal error: entered unreachable code")]
    fn test_handle_activity_event_unreachable_panics() {
        let mut exporter = MermaidExporter::new();
        let event = WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({}),
            timestamp: Utc::now(),
        };
        let _ = exporter.handle_activity_event(&event);
    }

    #[test]
    #[should_panic(expected = "internal error: entered unreachable code")]
    fn test_handle_timer_event_unreachable_panics() {
        let mut exporter = MermaidExporter::new();
        let event = WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({}),
            timestamp: Utc::now(),
        };
        let _ = exporter.handle_timer_event(&event);
    }

    #[test]
    #[should_panic(expected = "internal error: entered unreachable code")]
    fn test_handle_child_workflow_event_unreachable_panics() {
        let mut exporter = MermaidExporter::new();
        let event = WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({}),
            timestamp: Utc::now(),
        };
        let _ = exporter.handle_child_workflow_event(&event);
    }

    #[test]
    #[should_panic(expected = "internal error: entered unreachable code")]
    fn test_handle_misc_event_unreachable_panics() {
        let mut exporter = MermaidExporter::new();
        let event = WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({}),
            timestamp: Utc::now(),
        };
        let _ = exporter.handle_misc_event(&event);
    }

    #[test]
    #[should_panic(expected = "internal error: entered unreachable code")]
    fn test_handle_local_activity_event_unreachable_panics() {
        let mut exporter = MermaidExporter::new();
        let event = WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({}),
            timestamp: Utc::now(),
        };
        let _ = exporter.handle_local_activity_event(&event);
    }

    #[test]
    #[should_panic(expected = "internal error: entered unreachable code")]
    fn test_handle_external_activity_event_unreachable_panics() {
        let mut exporter = MermaidExporter::new();
        let event = WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({}),
            timestamp: Utc::now(),
        };
        let _ = exporter.handle_external_activity_event(&event);
    }

    #[test]
    #[should_panic(expected = "internal error: entered unreachable code")]
    fn test_handle_update_event_unreachable_panics() {
        let mut exporter = MermaidExporter::new();
        let event = WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({}),
            timestamp: Utc::now(),
        };
        let _ = exporter.handle_update_event(&event);
    }
}
