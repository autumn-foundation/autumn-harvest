//! Export workflow event histories to replay fixtures and Mermaid diagrams.

use crate::event::WorkflowEvent;
use crate::payload_codec::{LossyDecodeOutcome, PayloadCodecs};
use crate::types::ExecutionId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::fmt::Write;
use std::str::FromStr;

/// Serde adapter for `Option<chrono::Duration>` as `Option<i64>` milliseconds
/// (issue #772). `chrono::Duration` (`TimeDelta`) has no serde impl, so the
/// deadline-aware replay metadata (`execution_timeout`) that both
/// [`HistoryExportDocument`] and `testing::HistorySnapshot` carry is serialised
/// as an integer millisecond count — lossless for the `INTERVAL`-backed
/// `execution_timeout` column and matching the millisecond granularity of the
/// deadline probe. Both structs use this module so the exported JSON round-trips
/// into a `HistorySnapshot` verbatim.
pub(crate) mod opt_duration_millis {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    // serde `with` dictates the `&Option<T>` signature — `Option<&T>` is not
    // usable here.
    #[allow(clippy::ref_option)]
    pub fn serialize<S: Serializer>(
        value: &Option<chrono::Duration>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value
            .as_ref()
            .map(chrono::Duration::num_milliseconds)
            .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<chrono::Duration>, D::Error> {
        let millis: Option<i64> = Option::deserialize(deserializer)?;
        Ok(millis.map(chrono::Duration::milliseconds))
    }
}

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
    /// The business-level workflow identifier (issue #698), sourced from the
    /// `harvest_workflow_executions.workflow_id` column. Like `parent_execution_id`
    /// / `execution_timeout`, it lives in no `WorkflowEvent`, so it must ride the
    /// top-level export document for an exported history (retention archive /
    /// offline `replay_from_json`) to replay `ctx.info().workflow_id` without
    /// false-reporting non-determinism when a workflow branches on it or embeds it
    /// in an activity input. Serialised at the same top-level name as
    /// [`testing::HistorySnapshot::workflow_id`], so an exported history round-trips
    /// into that snapshot verbatim. `None` for a run without an explicit id or a
    /// legacy export produced before this field. Non-payload operational metadata
    /// — never redacted. The workflow **type** name rides
    /// [`workflow_name`](Self::workflow_name).
    ///
    /// [`testing::HistorySnapshot::workflow_id`]: crate::testing::HistorySnapshot::workflow_id
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,
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
    /// Per-execution context headers from the original run. `None` means the
    /// export was produced before this field was introduced (legacy) or no
    /// headers were attached. Used by `WorkflowReplayer::replay_from_json` to
    /// restore the same ambient headers the workflow saw during live execution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_headers: Option<HashMap<String, String>>,
    /// The execution's `execution_timeout` budget (issue #772), serialised as
    /// integer milliseconds at the top level so it round-trips into a
    /// `testing::HistorySnapshot` and the JSON / `harvest-replay` replay path
    /// threads the deadline-aware continue-as-new budget. `None` for a workflow
    /// with no execution timeout, or a legacy export produced before this field.
    /// **Non-payload operational metadata** — never redacted.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "opt_duration_millis"
    )]
    pub execution_timeout: Option<chrono::Duration>,
    /// The execution's live (pause/resume/redrive-shifted) absolute `deadline_at`
    /// (issue #772). Serialised at the top level so the JSON replay path can
    /// compute the deadline against the value the timeout scanner enforces
    /// rather than the nominal `start + execution_timeout`. `None` when no
    /// absolute deadline was recorded, or a legacy export. Non-payload
    /// operational metadata — never redacted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_at: Option<DateTime<Utc>>,
    /// The spawning parent's execution id (issue #698), sourced from the
    /// `harvest_workflow_executions.parent_id` column. `parent_id` lives in no
    /// `WorkflowEvent`, so — exactly like `execution_timeout`/`deadline_at` — it
    /// must ride the top-level export document for a parent-aware child exported
    /// here (retention archive / offline `replay_from_json`) to replay without
    /// false-reporting non-determinism when its control flow branches on
    /// `ctx.info().parent_execution_id`. Serialised at the same top-level name
    /// as [`testing::HistorySnapshot::parent_execution_id`], so an exported
    /// history round-trips into that snapshot verbatim. `None` for a top-level
    /// run or a legacy export produced before this field. Non-payload
    /// operational metadata — never redacted.
    ///
    /// [`testing::HistorySnapshot::parent_execution_id`]: crate::testing::HistorySnapshot::parent_execution_id
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_execution_id: Option<ExecutionId>,
}

/// Input needed to export one workflow execution history.
#[derive(Debug, Clone)]
pub struct HistoryExportRequest {
    /// Registered workflow handler name.
    pub workflow_name: String,
    /// The business-level workflow identifier (issue #698), embedded in the export
    /// so the JSON / `harvest-replay` replay path threads it into the replayed
    /// `WorkflowContext`. `None` for a run without an explicit id. Mirrors
    /// `execution_timeout`/`parent_execution_id`: the value lives in no
    /// `WorkflowEvent`, so it must be carried explicitly on the request.
    pub workflow_id: Option<String>,
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
    /// Per-execution context headers attached at workflow start. When `Some`,
    /// the headers are embedded in the export document so replaying the export
    /// restores the same ambient headers the original execution saw.
    pub context_headers: Option<HashMap<String, String>>,
    /// The execution's `execution_timeout` budget (issue #772), embedded in the
    /// export so the JSON / `harvest-replay` replay path threads the
    /// deadline-aware continue-as-new budget. `None` for a workflow with no
    /// execution timeout.
    pub execution_timeout: Option<chrono::Duration>,
    /// The execution's live (pause/resume/redrive-shifted) absolute `deadline_at`
    /// (issue #772). `None` when no absolute deadline was recorded.
    pub deadline_at: Option<DateTime<Utc>>,
    /// The spawning parent's execution id (issue #698), sourced from the
    /// execution row's `parent_id` column and embedded in the export so the
    /// JSON / `harvest-replay` replay path threads the child's spawner into the
    /// replayed `WorkflowContext`. `None` for a top-level run. Mirrors
    /// `execution_timeout`/`deadline_at`: the value lives in no `WorkflowEvent`,
    /// so it must be carried explicitly on the request.
    pub parent_execution_id: Option<ExecutionId>,
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
    let mut outcome = LossyDecodeOutcome::default();
    export_history_decoded(request, None, &mut outcome)
}

/// [`export_history`] with an optional read-path payload decoder (issue #608).
///
/// Under [`HistoryPayloadPolicy::Full`] with `Some(decoder)`, every exported
/// event is tolerantly decoded ([`PayloadCodecs::decode_value_lossy`])
/// **before** the document is assembled and measured, so `max_bytes`
/// enforcement and `size_limit.actual_bytes` reflect the decoded bytes the
/// caller actually receives — never a stale pre-decode measurement (PR #936
/// review): a decoded export that fits is not rejected because its encoded
/// envelopes were over the limit, and a decoded export that exceeds the limit
/// is rejected instead of shipping oversized while reporting a smaller size.
///
/// The decoder is ignored under [`HistoryPayloadPolicy::Redacted`] — redacted
/// exports replace payload fields wholesale (envelope included) and must
/// never materialize plaintext. `None` is byte-identical to
/// [`export_history`].
///
/// `outcome` accumulates decode/marker counts (never content) across calls —
/// including calls that return [`HistoryExportError`], since decoding has
/// already happened by then — so callers can audit every decode, even one
/// whose export was subsequently rejected for size.
///
/// # Errors
///
/// Same as [`export_history`].
pub fn export_history_decoded(
    request: HistoryExportRequest,
    decoder: Option<&PayloadCodecs>,
    outcome: &mut LossyDecodeOutcome,
) -> Result<HistoryExportDocument, HistoryExportError> {
    let max_bytes = request
        .max_bytes
        .unwrap_or(DEFAULT_HISTORY_EXPORT_MAX_BYTES);
    let mut events = export_events(&request.events, request.payload_policy)?;
    if request.payload_policy == HistoryPayloadPolicy::Full
        && let Some(codecs) = decoder
    {
        for event in &mut events {
            *outcome = outcome.merged(codecs.decode_value_lossy(event));
        }
    }
    let mut document = HistoryExportDocument {
        schema: HISTORY_EXPORT_SCHEMA.to_string(),
        version: HISTORY_EXPORT_VERSION,
        workflow_name: request.workflow_name,
        // Issue #698: business `workflow_id` — non-payload operational metadata,
        // carried verbatim under BOTH policies (never redacted) so an exported
        // history round-trips its `workflow_id` into the JSON replay path, exactly
        // like the deadline/parent metadata below.
        workflow_id: request.workflow_id,
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
        // Omit header values under Redacted policy — they may contain auth
        // tokens or tenant secrets that should not appear in shared exports.
        context_headers: if request.payload_policy == HistoryPayloadPolicy::Redacted {
            None
        } else {
            request.context_headers
        },
        // Deadline-aware replay metadata (issue #772) — non-payload operational
        // fields, carried verbatim under BOTH policies (never redacted) so the
        // exported history round-trips the deadline budget into the JSON /
        // `harvest-replay` replay path.
        execution_timeout: request.execution_timeout,
        deadline_at: request.deadline_at,
        // Spawning-parent id (issue #698) — non-payload operational metadata,
        // carried verbatim under BOTH policies (never redacted) so a parent-aware
        // child's exported history round-trips its `parent_execution_id` into the
        // JSON replay path, exactly like the deadline metadata above.
        parent_execution_id: request.parent_execution_id,
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
    // `last_completion_result` is the prior run's output frozen into WorkflowStarted
    // for scheduled carryover (issue #488); redact it like any other payload copy.
    matches!(
        key,
        "input" | "output" | "payload" | "details" | "value" | "last_completion_result"
    )
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
                | WorkflowEvent::WorkflowExecutionResumed { .. }
                | WorkflowEvent::WorkflowRedriven { .. }
                | WorkflowEvent::WorkflowRetryScheduled { .. } => {
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
                WorkflowEvent::TimerStarted { .. }
                | WorkflowEvent::TimerFired { .. }
                | WorkflowEvent::TimerCancelled { .. } => {
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
                | WorkflowEvent::SideEffectRecorded { .. }
                | WorkflowEvent::MutexGranted { .. } => {
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
                WorkflowEvent::ExternalCancelRequested { .. }
                | WorkflowEvent::ExternalCancelDelivered { .. }
                | WorkflowEvent::ExternalCancelFailed { .. } => {
                    self.handle_external_cancel_event(event)?;
                }
                WorkflowEvent::ExternalAwaitRequested { .. }
                | WorkflowEvent::ExternalAwaitResolved { .. }
                | WorkflowEvent::ExternalAwaitFailed { .. } => {
                    self.handle_external_await_event(event)?;
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
            WorkflowEvent::WorkflowFailed { error, .. } => {
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
            WorkflowEvent::WorkflowRedriven {
                dead_letter_id,
                reason,
                ..
            } => {
                let detail = reason
                    .as_deref()
                    .map(|r| format!(": {}", r.replace('\n', " ").replace('"', "'")))
                    .unwrap_or_default();
                writeln!(
                    self.out,
                    "    Note over WF: Redriven (dlq: {dead_letter_id}){detail}"
                )?;
            }
            WorkflowEvent::WorkflowRetryScheduled {
                attempt,
                retry_exec_id,
                ..
            } => {
                writeln!(
                    self.out,
                    "    Note over WF: Retry Scheduled (attempt: {attempt}, next: {retry_exec_id})"
                )?;
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
            WorkflowEvent::TimerCancelled { timer_id } => {
                let participant = "Timer";
                if self.participants.insert(participant.to_string()) {
                    writeln!(self.out, "    participant {participant} as Timer")?;
                }
                writeln!(
                    self.out,
                    "    WF-->>-{participant}: Timer {timer_id} Cancelled"
                )?;
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
            WorkflowEvent::ChildWorkflowFailed {
                child_id, error, ..
            } => {
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
            WorkflowEvent::MutexGranted { key, .. } => {
                writeln!(self.out, "    Note over WF: Mutex Acquired: {key}")?;
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

    fn handle_external_cancel_event(
        &mut self,
        event: &WorkflowEvent,
    ) -> Result<(), std::fmt::Error> {
        match event {
            WorkflowEvent::ExternalCancelRequested { cancel_id, target } => {
                writeln!(
                    self.out,
                    "    Note over WF: Cancel Requested → {target} (id: {cancel_id})"
                )?;
            }
            WorkflowEvent::ExternalCancelDelivered { cancel_id } => {
                writeln!(
                    self.out,
                    "    Note over WF: Cancel Delivered (id: {cancel_id})"
                )?;
            }
            WorkflowEvent::ExternalCancelFailed {
                cancel_id,
                reason_code,
            } => {
                writeln!(
                    self.out,
                    "    Note over WF: Cancel Failed ({reason_code}) (id: {cancel_id})"
                )?;
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn handle_external_await_event(
        &mut self,
        event: &WorkflowEvent,
    ) -> Result<(), std::fmt::Error> {
        match event {
            WorkflowEvent::ExternalAwaitRequested { await_id, target } => {
                writeln!(
                    self.out,
                    "    Note over WF: Await Requested → {target} (id: {await_id})"
                )?;
            }
            WorkflowEvent::ExternalAwaitResolved { await_id, .. } => {
                writeln!(
                    self.out,
                    "    Note over WF: Await Resolved (id: {await_id})"
                )?;
            }
            WorkflowEvent::ExternalAwaitFailed {
                await_id,
                reason_code,
                ..
            } => {
                writeln!(
                    self.out,
                    "    Note over WF: Await Failed ({reason_code}) (id: {await_id})"
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
                    last_completion_result: None,
                    last_error: None,
                    scheduled_time: None,
                },
                WorkflowEvent::WorkflowCompleted {
                    output: serde_json::json!({ "ok": true }),
                },
            ],
            exported_at: Utc::now(),
            payload_policy: HistoryPayloadPolicy::Full,
            max_bytes: Some(64 * 1024),
            context_headers: None,
            execution_timeout: None,
            deadline_at: None,
            parent_execution_id: None,
            workflow_id: None,
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

    /// Issue #772 (Codex P2): a full history export must carry the deadline-aware
    /// replay metadata (`execution_timeout`/`deadline_at`) at the TOP LEVEL, in
    /// the same wire shape a `testing::HistorySnapshot` expects, so an exported
    /// history round-trips into a snapshot and the JSON / `harvest-replay`
    /// replay path threads the deadline budget.
    #[test]
    fn full_export_carries_deadline_metadata_round_tripping_into_a_snapshot() {
        use crate::types::ExecutionId;

        let exec_id = ExecutionId::new();
        let timeout = chrono::Duration::seconds(1800);
        let deadline = Utc::now() + chrono::Duration::seconds(3600);
        let document = export_history(HistoryExportRequest {
            workflow_name: "wf".to_string(),
            execution_id: exec_id,
            shard_id: 0,
            state: "RUNNING".to_string(),
            events: vec![WorkflowEvent::WorkflowStarted {
                input: serde_json::json!(null),
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            }],
            exported_at: Utc::now(),
            payload_policy: HistoryPayloadPolicy::Full,
            max_bytes: Some(64 * 1024),
            context_headers: None,
            execution_timeout: Some(timeout),
            deadline_at: Some(deadline),
            parent_execution_id: None,
            workflow_id: None,
        })
        .expect("full export should fit under the limit");

        assert_eq!(document.execution_timeout, Some(timeout));
        assert_eq!(document.deadline_at, Some(deadline));

        let json = serde_json::to_string(&document).expect("export should serialize");
        // The duration serialises as integer milliseconds at the top level.
        assert!(
            json.contains("\"execution_timeout\":1800000"),
            "execution_timeout must serialise as top-level integer millis: {json}"
        );

        // The exported JSON must deserialise into a HistorySnapshot carrying the
        // deadline metadata verbatim — this is the exact round-trip the JSON /
        // `harvest-replay` replay path relies on.
        let snapshot: crate::testing::HistorySnapshot =
            serde_json::from_str(&json).expect("export JSON parses as a HistorySnapshot");
        assert_eq!(snapshot.execution_timeout, Some(timeout));
        assert_eq!(snapshot.deadline_at, Some(deadline));
        assert_eq!(snapshot.workflow_name, "wf");
        assert_eq!(snapshot.execution_id, exec_id);
    }

    /// Issue #698 (Codex P2): a full history export must carry the spawning
    /// `parent_execution_id` at the TOP LEVEL — sourced from the row's
    /// `parent_id` column, present in no `WorkflowEvent` — in the same wire
    /// shape a `testing::HistorySnapshot` expects, so a parent-aware child
    /// exported through the DB `export_history` -> `replay_from_json` path
    /// (retention archive / offline replay) round-trips its parent instead of
    /// deserialising `None` and false-reporting non-determinism.
    #[test]
    fn full_export_carries_parent_execution_id_round_tripping_into_a_snapshot() {
        use crate::types::ExecutionId;

        let exec_id = ExecutionId::new();
        let parent = ExecutionId::new();
        let document = export_history(HistoryExportRequest {
            workflow_name: "child_wf".to_string(),
            execution_id: exec_id,
            shard_id: 0,
            state: "RUNNING".to_string(),
            events: vec![WorkflowEvent::WorkflowStarted {
                input: serde_json::json!(null),
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            }],
            exported_at: Utc::now(),
            payload_policy: HistoryPayloadPolicy::Full,
            max_bytes: Some(64 * 1024),
            context_headers: None,
            execution_timeout: None,
            deadline_at: None,
            parent_execution_id: Some(parent),
            workflow_id: None,
        })
        .expect("full export should fit under the limit");

        assert_eq!(document.parent_execution_id, Some(parent));

        let json = serde_json::to_string(&document).expect("export should serialize");
        assert!(
            json.contains("\"parent_execution_id\":"),
            "parent_execution_id must serialise at the top level: {json}"
        );

        // The exported JSON must deserialise into a HistorySnapshot carrying the
        // parent verbatim — the exact round-trip the JSON / `harvest-replay`
        // replay path relies on.
        let snapshot: crate::testing::HistorySnapshot =
            serde_json::from_str(&json).expect("export JSON parses as a HistorySnapshot");
        assert_eq!(snapshot.parent_execution_id, Some(parent));
        assert_eq!(snapshot.workflow_name, "child_wf");
        assert_eq!(snapshot.execution_id, exec_id);
    }

    /// Issue #698 back-compat: an OLD export document JSON that predates the
    /// top-level `parent_execution_id` field deserialises into a
    /// `HistorySnapshot` with `parent_execution_id == None` (serde default),
    /// so legacy archives replay unchanged.
    #[test]
    fn legacy_export_without_parent_field_deserialises_to_none() {
        use crate::types::ExecutionId;

        let exec_id = ExecutionId::new();
        // Minimal document JSON with NO parent_execution_id / execution_timeout /
        // deadline_at keys — the shape a pre-#698 / pre-#772 export produced.
        let json = format!(
            r#"{{"schema":"autumn-harvest.history-export","version":1,"workflow_name":"legacy_wf","execution_id":"{exec_id}","shard_id":0,"exported_at":"2026-01-01T00:00:00Z","event_count":0,"status":{{"terminal":true,"state":"COMPLETED"}},"payload_policy":"full","size_limit":{{"max_bytes":65536,"actual_bytes":10,"truncated":false,"truncation_behavior":"fail"}},"events":[]}}"#
        );
        let snapshot: crate::testing::HistorySnapshot =
            serde_json::from_str(&json).expect("legacy export JSON parses as a HistorySnapshot");
        assert_eq!(snapshot.parent_execution_id, None);
        assert_eq!(snapshot.execution_id, exec_id);
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
                    last_completion_result: None,
                    last_error: None,
                    scheduled_time: None,
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
                WorkflowEvent::workflow_failed(
                    "downstream returned bearer lower-secret".to_string(),
                ),
            ],
            exported_at: Utc::now(),
            payload_policy: HistoryPayloadPolicy::Redacted,
            max_bytes: Some(64 * 1024),
            context_headers: None,
            execution_timeout: None,
            deadline_at: None,
            parent_execution_id: None,
            workflow_id: None,
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
                    last_completion_result: None,
                    last_error: None,
                    scheduled_time: None,
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
            context_headers: None,
            execution_timeout: None,
            deadline_at: None,
            parent_execution_id: None,
            workflow_id: None,
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            }],
            exported_at: Utc::now(),
            payload_policy: HistoryPayloadPolicy::Full,
            max_bytes: Some(128),
            context_headers: None,
            execution_timeout: None,
            deadline_at: None,
            parent_execution_id: None,
            workflow_id: None,
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
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
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
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
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
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
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
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
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
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
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
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
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
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        };
        let _ = exporter.handle_update_event(&event);
    }

    // ── Codec-envelope-bearing histories (issue #608, PR #936 review) ─────────
    //
    // The export handlers load history *raw* (`store::load_history_undecoded`)
    // so a non-identity codec envelope reaches `export_history` verbatim as an
    // opaque `Value` inside the event's payload field. These tests pin the two
    // downstream policy legs: Full passes the envelope through byte-identical
    // (the read-path decoder — when active — decodes it afterwards), and
    // Redacted replaces the payload field wholesale, envelope included,
    // without ever needing a codec.

    fn codec_envelope_fixture() -> serde_json::Value {
        serde_json::json!({
            "_harvest_codec_envelope": 1,
            "codec_id": "aes-256-gcm",
            "data": "bm90LXJlYWwtY2lwaGVydGV4dA==",
        })
    }

    #[test]
    fn full_history_export_preserves_codec_envelopes_verbatim() {
        use crate::types::ExecutionId;

        let envelope = codec_envelope_fixture();
        let document = export_history(HistoryExportRequest {
            workflow_name: "encrypted_flow".to_string(),
            execution_id: ExecutionId::new(),
            shard_id: 0,
            state: "COMPLETED".to_string(),
            events: vec![WorkflowEvent::WorkflowCompleted {
                output: envelope.clone(),
            }],
            exported_at: Utc::now(),
            payload_policy: HistoryPayloadPolicy::Full,
            max_bytes: Some(64 * 1024),
            context_headers: None,
            execution_timeout: None,
            deadline_at: None,
            parent_execution_id: None,
            workflow_id: None,
        })
        .expect("an envelope-bearing history must export under Full");

        assert_eq!(
            document.events[0]["data"]["output"], envelope,
            "Full export must carry the stored envelope byte-identical — \
             decoding (or erroring) is the read-path decoder's job, not the export's"
        );
    }

    #[test]
    fn redacted_history_export_replaces_codec_envelopes_without_decoding() {
        use crate::types::ExecutionId;

        let document = export_history(HistoryExportRequest {
            workflow_name: "encrypted_flow".to_string(),
            execution_id: ExecutionId::new(),
            shard_id: 0,
            state: "COMPLETED".to_string(),
            events: vec![WorkflowEvent::WorkflowCompleted {
                output: codec_envelope_fixture(),
            }],
            exported_at: Utc::now(),
            payload_policy: HistoryPayloadPolicy::Redacted,
            max_bytes: Some(64 * 1024),
            context_headers: None,
            execution_timeout: None,
            deadline_at: None,
            parent_execution_id: None,
            workflow_id: None,
        })
        .expect("an envelope-bearing history must export under Redacted");

        let json = serde_json::to_string(&document).expect("serialize");
        assert!(
            !json.contains("_harvest_codec_envelope") && !json.contains("bm90LXJlYWwt"),
            "Redacted export must replace the payload field wholesale, envelope included: {json}"
        );
        assert_eq!(document.events[0]["data"]["output"]["redacted"], true);
    }

    // --- issue #608, PR #936 review: `export_history_decoded` runs the
    // read-path decoder *before* the document is measured, so `max_bytes`
    // enforcement and `size_limit.actual_bytes` always describe the decoded
    // bytes the caller actually receives.

    /// Test codec whose decode ignores the stored ciphertext and returns a
    /// fixed plaintext, so tests can independently size the encoded envelope
    /// (via the base64 `data` length) and the decoded result.
    struct FixedPlaintextCodec {
        plaintext: String,
    }

    impl crate::payload_codec::PayloadCodec for FixedPlaintextCodec {
        fn codec_id(&self) -> &'static str {
            "fixed-plaintext"
        }

        fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, crate::payload_codec::CodecError> {
            Ok(raw.to_vec())
        }

        fn decode(&self, _encoded: &[u8]) -> Result<Vec<u8>, crate::payload_codec::CodecError> {
            Ok(self.plaintext.clone().into_bytes())
        }
    }

    fn fixed_plaintext_registry(plaintext_json: &str) -> PayloadCodecs {
        let mut codecs = PayloadCodecs::default();
        codecs.register(std::sync::Arc::new(FixedPlaintextCodec {
            plaintext: plaintext_json.to_string(),
        }));
        codecs
    }

    fn fixed_plaintext_envelope(encoded_len: usize) -> Value {
        use base64::Engine as _;
        serde_json::json!({
            "_harvest_codec_envelope": 1,
            "codec_id": "fixed-plaintext",
            "data": base64::engine::general_purpose::STANDARD.encode(vec![b'x'; encoded_len]),
        })
    }

    fn envelope_export_request(envelope: Value, max_bytes: usize) -> HistoryExportRequest {
        use crate::types::ExecutionId;
        HistoryExportRequest {
            workflow_name: "encrypted_flow".to_string(),
            execution_id: ExecutionId::new(),
            shard_id: 0,
            state: "COMPLETED".to_string(),
            events: vec![WorkflowEvent::WorkflowCompleted { output: envelope }],
            exported_at: Utc::now(),
            payload_policy: HistoryPayloadPolicy::Full,
            max_bytes: Some(max_bytes),
            context_headers: None,
            execution_timeout: None,
            deadline_at: None,
            parent_execution_id: None,
            workflow_id: None,
        }
    }

    #[test]
    fn decoded_export_exceeding_max_bytes_is_rejected_with_decoded_size() {
        // Tiny envelope (fits comfortably), huge decoded plaintext (does not).
        let plaintext = format!("\"{}\"", "a".repeat(8192));
        let codecs = fixed_plaintext_registry(&plaintext);
        let request = envelope_export_request(fixed_plaintext_envelope(8), 2048);

        // Sanity: without a decoder the encoded document fits.
        let mut ignored = LossyDecodeOutcome::default();
        export_history_decoded(request.clone(), None, &mut ignored)
            .expect("encoded document must fit under max_bytes");
        assert!(!ignored.touched(), "no decoder => no decode outcome");

        let mut outcome = LossyDecodeOutcome::default();
        let error = export_history_decoded(request, Some(&codecs), &mut outcome)
            .expect_err("decoded document must exceed max_bytes");
        match error {
            HistoryExportError::SizeLimitExceeded {
                actual_bytes,
                max_bytes,
            } => {
                assert_eq!(max_bytes, 2048);
                assert!(
                    actual_bytes > 8192,
                    "actual_bytes must describe the decoded document, \
                     not the small encoded one: {actual_bytes}"
                );
            }
            other @ HistoryExportError::Serialization(_) => {
                panic!("expected SizeLimitExceeded, got {other}")
            }
        }
        // The decode happened before the rejection — the outcome survives the
        // error so callers can still audit it.
        assert_eq!(outcome.decoded, 1);
        assert_eq!(outcome.failed, 0);
    }

    #[test]
    fn oversized_envelope_with_small_decoded_form_exports_within_max_bytes() {
        // Big envelope (over the limit encoded), tiny decoded plaintext.
        let codecs = fixed_plaintext_registry("\"tiny\"");
        let request = envelope_export_request(fixed_plaintext_envelope(6000), 4096);

        // Sanity: without a decoder the encoded document is over the limit —
        // the pre-fix behavior rejected this export outright.
        let mut ignored = LossyDecodeOutcome::default();
        export_history_decoded(request.clone(), None, &mut ignored)
            .expect_err("encoded document must exceed max_bytes");

        let mut outcome = LossyDecodeOutcome::default();
        let document = export_history_decoded(request, Some(&codecs), &mut outcome)
            .expect("decoded document fits and must export");
        assert_eq!(document.events[0]["data"]["output"], "tiny");
        assert_eq!(outcome.decoded, 1);
        assert!(
            document.size_limit.actual_bytes <= 4096,
            "size accounting must reflect the decoded document: {}",
            document.size_limit.actual_bytes
        );
        // One source of truth: the reported size is the serialized size of
        // the exact document the caller receives.
        let serialized = serde_json::to_vec(&document).expect("serialize").len();
        assert_eq!(document.size_limit.actual_bytes, serialized);
    }

    #[test]
    fn redacted_export_ignores_the_decoder_entirely() {
        let codecs = fixed_plaintext_registry("\"tiny\"");
        let mut request = envelope_export_request(fixed_plaintext_envelope(64), 64 * 1024);
        request.payload_policy = HistoryPayloadPolicy::Redacted;

        let mut outcome = LossyDecodeOutcome::default();
        let document = export_history_decoded(request, Some(&codecs), &mut outcome)
            .expect("redacted export must succeed");
        assert!(
            !outcome.touched(),
            "Redacted policy must never run the decoder"
        );
        let json = serde_json::to_string(&document).expect("serialize");
        assert!(
            !json.contains("tiny") && !json.contains("_harvest_codec_envelope"),
            "Redacted export must contain neither plaintext nor the envelope: {json}"
        );
        assert_eq!(document.events[0]["data"]["output"]["redacted"], true);
    }
}
