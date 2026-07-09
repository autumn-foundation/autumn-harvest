//! DAG run graph view (issue #690).
//!
//! This module contains the **pure** projection that reconstructs a unified
//! DAG run's node topology — annotated with per-node status, timing, attempts,
//! and (on failure) the truncated error — purely from the registered
//! [`DagDefinition`] and the run's recorded [`WorkflowEvent`] history.
//!
//! It is **read-only**: it introduces no new [`WorkflowEvent`] variant, no
//! migration, and never writes to `harvest_dag_runs`. The handler
//! (`get_dag_run_graph` in `api.rs`) reads the owning shard via
//! [`ExecutionId::shard`](autumn_harvest::types::ExecutionId) routing and calls
//! [`build_run_graph`].
//!
//! ## Node identity
//!
//! A unified DAG node is identified by its **activity name** — the same
//! node-name-is-activity-name contract the #366 retry resolver relies on
//! ([`crate::dag_retry`]). The `ActivityScheduled` event records that same
//! activity name.
//!
//! ### v1 limitation: duplicate activity names
//!
//! This view assumes activity names are **unique** within a DAG. Because node
//! identity is keyed purely by activity name (there is no per-task id in the
//! recorded history), a DAG that reuses the same activity name across more than
//! one task yields **ambiguous, duplicated** node entries: two [`DagRunNode`]s
//! with the identical name, each carrying the same name-keyed status, timing,
//! and error, and a combined `attempts` count. This is not supported.
//!
//! It is the same assumption the #366 retry resolver enforces — but that
//! resolver *rejects* a duplicate-name DAG outright
//! ([`DagRetryResolveError::AmbiguousNodes`](crate::dag_retry::DagRetryResolveError)
//! → `400`). This read view deliberately does **not** reject: it is a
//! best-effort, always-render-the-graph read path, and refusing to render a
//! validly-running DAG would be worse operator UX than showing the (documented)
//! ambiguous duplicates. `DagBuilder::build()` itself does not reject duplicate
//! activity names, so such a DAG can be registered and run.
//!
//! ### v1 limitation: mapped (fan-out) tasks
//!
//! A mapped task (`DagBuilder::map_activity`) is a single `DagTask`, but at
//! runtime the unified DAG handler dispatches one activity instance per
//! input-array element, **all sharing the task's activity name** with distinct
//! `activity_id`s (no `fan_out:` marker is recorded — the handler hand-rolls the
//! fan-out via `execute_activity_raw_with_opts`). Because node identity is keyed
//! purely by activity name and both this view and [`crate::dag_retry::node_outcome`]
//! select the **latest-scheduled** `ActivityScheduled` for the name, the whole
//! mapped node collapses to that single instance's outcome. If an earlier mapped
//! element fails or times out while a later element succeeds, the node is
//! reported with the later element's status/attempts/error rather than an
//! aggregate — a `FAILED` run can show the mapped node as `succeeded` or
//! `cancelled`.
//!
//! This is **intentionally identical** to the #366 retry resolver, which
//! collapses mapped tasks the same way ([`node_outcome`](crate::dag_retry::node_outcome)
//! has no `map_upstream` awareness). Keeping the two consistent is the point
//! (AC5): aggregating here would make the graph disagree with the retry
//! endpoint — an operator could see a mapped node reported `failed` yet have the
//! retry endpoint reject it as succeeded. A map-aware aggregate outcome is
//! deferred to a #366 change that updates `node_outcome` and this view together.
//!
//! **Empty mapped fan-out** (issue #690 review, Codex). A mapped task over an
//! *empty* upstream array is a special case of the same limitation: the unified
//! DAG handler returns `TaskStatus::Succeeded` **without dispatching any
//! instance** (`autumn-harvest-macros/src/dag.rs`, the `__n_instances == 0`
//! guard), so the node appends **no** `ActivityScheduled`/activity events at all.
//! With zero events for the node, [`node_outcome`](crate::dag_retry::node_outcome)
//! returns [`NodeOutcome::NotAttempted`] and this view reports it as
//! [`DagNodeStatus::Pending`] on a completed run — not `succeeded`. From history
//! alone a zero-event, no-marker node is indistinguishable from a not-yet-reached
//! node (and from a `SkipByTriggerRule` skip, which also records no marker — see
//! the AC7 scope note below). This is **AC5-consistent** (`node_outcome` returns
//! `NotAttempted` for the same history, so the graph and the #366 retry path
//! agree); a precise `succeeded` classification is deferred to the same
//! coordinated #366 change as the instance-collapse case above.
//!
//! ## Status classification (AC5 anchor)
//!
//! Base classification comes from [`crate::dag_retry::node_outcome`], the exact
//! function the #366 retry path uses, so a node the retry path treats as
//! `Failed` reports [`DagNodeStatus::Failed`] here. Two derived statuses layer
//! on top of the base outcome:
//!
//! * A [`NodeOutcome::Cancelled`] node (scheduled, no terminal event) is
//!   reported as [`DagNodeStatus::Running`] while the run is still
//!   `RUNNING`/`SUSPENDED`, and [`DagNodeStatus::Cancelled`] once the run has
//!   reached a terminal state.
//! * A [`NodeOutcome::NotAttempted`] node is reported as
//!   [`DagNodeStatus::Skipped`] when a `dag_skip:{task_index}` marker was
//!   recorded for it (a #482 data-dependent condition branch that was not
//!   taken), and [`DagNodeStatus::Pending`] otherwise (never reached).
//!
//! ### AC7 scope note
//!
//! Only [`DagDispatchDecision::SkipByCondition`](autumn_harvest::dag::DagDispatchDecision)
//! (issue #482, data-dependent) records a `dag_skip:` marker. A
//! `SkipByTriggerRule` skip records **no** marker and is therefore
//! indistinguishable from `pending` via history alone — which is acceptable per
//! the issue's AC7 wording, which targets #482 data-dependent branches.

use autumn_harvest::dag::DagDefinition;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::types::ActivityExecId;
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::dag_retry::{NodeOutcome, node_outcome};

/// Maximum characters retained for a failed node's error message. The message
/// is truncated to its first line and then to this many characters.
const ERROR_MAX_CHARS: usize = 200;

/// The status of a single DAG node on one run, derived purely from the
/// registered topology plus recorded history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DagNodeStatus {
    /// The node's activity completed successfully.
    Succeeded,
    /// The node's activity recorded a terminal failure (no later completion).
    Failed,
    /// The node's activity timed out.
    TimedOut,
    /// The node's activity was scheduled but never recorded a terminal event,
    /// and the run has itself reached a terminal state (it was cancelled /
    /// force-failed while the activity was in flight).
    Cancelled,
    /// The node's activity is scheduled-but-not-terminal on a run that is still
    /// `RUNNING`/`SUSPENDED`.
    Running,
    /// The node was never reached on this run.
    Pending,
    /// The node was skipped by a data-dependent condition (#482): a
    /// `dag_skip:{task_index}` marker was recorded for it.
    Skipped,
}

/// One node entry in a DAG run graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DagRunNode {
    /// The node (== activity) name.
    pub node_name: String,
    /// The node's status on this run.
    pub status: DagNodeStatus,
    /// Static topology: the upstream node (activity) names this node depends on,
    /// so a UI renders the graph without a second registry lookup (AC3).
    pub depends_on: Vec<String>,
    /// When the node's latest attempt actually started running (its latest
    /// `ActivityStarted` timestamp), falling back to the schedule timestamp for
    /// a scheduled-but-not-yet-claimed node. `None` for a never-scheduled node.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    /// When the node's latest attempt reached a terminal event, if it did.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
    /// Number of execution attempts of the node's latest attempt-chain — the
    /// count of `ActivityStarted` events for its authoritative `activity_id`
    /// (one per worker claim, so activity-level retries are counted). `0` for a
    /// never-scheduled node.
    pub attempts: u32,
    /// Low-cardinality error-type name for a failed node.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
    /// First line of the failure message for a failed node, truncated.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// The full DAG run graph response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DagRunGraphResponse {
    /// The execution id of the DAG run.
    pub run_exec_id: String,
    /// The DAG name.
    pub dag_name: String,
    /// The run's execution state (e.g. `RUNNING`, `FAILED`, `COMPLETED`).
    pub state: String,
    /// When the run started.
    pub started_at: DateTime<Utc>,
    /// When the run finished, if it has (its `completed_at`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
    /// The annotated node topology.
    pub nodes: Vec<DagRunNode>,
}

/// Truncate an error message to its first line and at most [`ERROR_MAX_CHARS`]
/// characters (char-boundary safe), appending `...` when truncated. Returns
/// `None` when the first line is empty, so an empty error string is serialized
/// as an omitted field rather than `Some("")`.
fn first_line_truncated(error: &str) -> Option<String> {
    let first_line = error.lines().next().unwrap_or("");
    if first_line.is_empty() {
        return None;
    }
    let mut chars = first_line.chars();
    let truncated: String = chars.by_ref().take(ERROR_MAX_CHARS).collect();
    Some(if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    })
}

/// Whether the execution state means the run is still live (a scheduled node
/// with no terminal event is running, not cancelled).
///
/// Classified by **terminality**, not an explicit RUNNING allow-list: any
/// non-terminal state is live. This deliberately includes `PAUSED` (a real
/// non-terminal state, issue #383) — a scheduled-but-not-terminal node on a
/// paused run is still in flight and reports `running`, not `cancelled`. It
/// also future-proofs against any new non-terminal state. `SUSPENDED` is not a
/// persisted execution state (the state CHECK constraint forbids it), so it is
/// covered incidentally rather than named.
fn is_live_state(exec_state: &str) -> bool {
    !autumn_harvest::erase::is_terminal_state(exec_state)
}

/// Find the index (and activity id) of the **latest** `ActivityScheduled` for
/// `node`. Mirrors [`node_outcome`]'s latest-attempt selection so timing,
/// attempts, and error all describe the same authoritative attempt.
fn latest_scheduled(events: &[WorkflowEvent], node: &str) -> Option<(usize, ActivityExecId)> {
    events.iter().enumerate().rev().find_map(|(idx, event)| {
        if let WorkflowEvent::ActivityScheduled {
            activity_id, name, ..
        } = event
        {
            (name == node).then_some((idx, *activity_id))
        } else {
            None
        }
    })
}

/// Count the real execution attempts of the node's authoritative attempt-chain
/// and return the index of the **latest** attempt's start.
///
/// An activity-level retry (a `RetryPolicy` firing within a single dispatch)
/// **reuses the same task row and the same `activity_id`**: the engine keeps the
/// original `ActivityScheduled` and appends a fresh `ActivityStarted` on every
/// claim (`queue::requeue_for_retry` re-pends the row and appends no event; a
/// retryable failure appends no `ActivityFailed` at all — only the final,
/// retry-exhausting failure does). Counting `ActivityScheduled` would therefore
/// report `attempts: 1` for a node the engine attempted N times, and anchor the
/// timing to the original schedule instead of the latest attempt. The
/// authoritative signal for "how many times a worker ran this" is the number of
/// `ActivityStarted` events for the node's latest `activity_id` (the same
/// `activity_id` [`node_outcome`] treats as authoritative).
///
/// Returns `(started_count, latest_started_index)` scoped to `activity_id`.
fn started_attempts(events: &[WorkflowEvent], activity_id: ActivityExecId) -> (u32, Option<usize>) {
    let mut count: u32 = 0;
    let mut latest = None;
    for (idx, event) in events.iter().enumerate() {
        if let WorkflowEvent::ActivityStarted {
            activity_id: id, ..
        } = event
            && *id == activity_id
        {
            count = count.saturating_add(1);
            latest = Some(idx);
        }
    }
    (count, latest)
}

/// Find the index of the terminal event (`ActivityCompleted`/`ActivityFailed`/
/// `ActivityTimedOut`) for `activity_id`, preferring a completion, then the
/// last-recorded failure/timeout for that id.
fn terminal_index(events: &[WorkflowEvent], activity_id: ActivityExecId) -> Option<usize> {
    // A completion is authoritative for this attempt id.
    if let Some(idx) = events.iter().position(|event| {
        matches!(event, WorkflowEvent::ActivityCompleted { activity_id: id, .. } if *id == activity_id)
    }) {
        return Some(idx);
    }
    // Otherwise the last recorded failure/timeout for this id.
    events
        .iter()
        .enumerate()
        .rev()
        .find_map(|(idx, event)| match event {
            WorkflowEvent::ActivityFailed {
                activity_id: id, ..
            }
            | WorkflowEvent::ActivityTimedOut {
                activity_id: id, ..
            } if *id == activity_id => Some(idx),
            _ => None,
        })
}

/// Extract the `(error_type, error)` pair from the terminal `ActivityFailed`
/// for `activity_id`, first line truncated. The outer `Option` is `Some` when a
/// terminal `ActivityFailed` exists for this attempt; the inner fields are each
/// `None` when empty (an empty error string or empty error type serializes as an
/// omitted field rather than `Some("")`).
fn failure_detail(
    events: &[WorkflowEvent],
    activity_id: ActivityExecId,
) -> Option<(Option<String>, Option<String>)> {
    events.iter().rev().find_map(|event| match event {
        WorkflowEvent::ActivityFailed {
            activity_id: id,
            error,
            error_type,
            ..
        } if *id == activity_id => {
            let etype = if error_type.is_empty() {
                None
            } else {
                Some(error_type.clone())
            };
            Some((etype, first_line_truncated(error)))
        }
        _ => None,
    })
}

/// Whether a `dag_skip:{task_index}` marker was recorded for `task_index`
/// (and, when its recorded fingerprint is readable, whether that fingerprint
/// still identifies the current node).
///
/// A `dag_skip:{idx}` marker (#482) is keyed by the node's *position* in the
/// DAG, but the marker's `details` also record the bound `task` (activity name)
/// and `upstreams` (edges) at record time. If the DAG was reordered or a task
/// renamed after this run recorded its markers, the marker at `task_index` would
/// belong to a *different* node than the one now at that index — treating the
/// bare marker name as authoritative would mislabel the wrong current node as
/// `skipped`. Guard against that by requiring the recorded `details.task` to
/// still match the current node's activity name, and the recorded
/// `details.upstreams` fingerprint to still match its edges (old-format markers
/// without an `upstreams` field pass through for backward compatibility). This
/// mirrors the Vantage UI's condition-skip validation (`ui.rs`); a marker whose
/// fingerprint no longer matches is ignored, so the node falls through to
/// `pending` rather than being falsely reported as `skipped`.
///
/// **Codec/offload deployments (issue #690 review, Codex CLAIM D).** `details`
/// is a payload field (`PAYLOAD_FIELD_KEYS`), so on a deployment with a
/// non-identity [`PayloadCodec`](autumn_harvest::payload_codec::PayloadCodec) —
/// or, in principle, large-payload offload — it is stored as an opaque envelope
/// (`_harvest_codec_envelope` / `_harvest_offload_envelope`). This read path
/// deliberately does **not** codec-decode (matching the Vantage UI's `dag_skip`
/// detection and [`crate::store::load_history_with_timestamps`]), so the
/// recorded fingerprint is not visible. When `details.task` is unreadable the
/// fingerprint validation cannot run, so we fall back to matching by the marker
/// **name/index** alone — which is never a payload field and is always in the
/// clear — and report the node `skipped`. Reorder-safety is not available in
/// that opaque case (accepted: this equals the pre-#690 behavior, and skip
/// detection itself stays correct in every deployment).
fn has_skip_marker(
    events: &[WorkflowEvent],
    task_index: usize,
    activity_name: &str,
    upstreams: &[usize],
) -> bool {
    let marker_name = format!("dag_skip:{task_index}");
    events.iter().any(|event| {
        let WorkflowEvent::MarkerRecorded { name, details } = event else {
            return false;
        };
        if *name != marker_name {
            return false;
        }
        // When the recorded fingerprint is unreadable — an opaque codec/offload
        // envelope, or an erasure tombstone — validation cannot run, so honor
        // the skip by the (always-clear) marker name/index alone. See the doc
        // comment (issue #690 review, Codex CLAIM D).
        let Some(recorded_task) = details.get("task").and_then(|v| v.as_str()) else {
            return true;
        };
        // The recorded task name must still identify the current node at this
        // index (reorder/rename safety).
        if recorded_task != activity_name {
            return false;
        }
        // Validate the upstream fingerprint when present; old-format markers
        // without an `upstreams` field pass through for backward compat.
        if let Some(arr) = details.get("upstreams").and_then(|v| v.as_array()) {
            let recorded: Vec<usize> = arr
                .iter()
                .filter_map(|v| v.as_u64().and_then(|n| usize::try_from(n).ok()))
                .collect();
            if recorded != upstreams {
                return false;
            }
        }
        true
    })
}

/// Build the annotated node topology for a DAG run.
///
/// Pure: takes the registered [`DagDefinition`], the run's recorded events
/// paired with their `harvest_events.timestamp`, and the run's current
/// execution state. Node order matches [`DagDefinition::tasks`] order.
#[must_use]
pub fn build_run_graph(
    def: &DagDefinition,
    timestamped_events: &[(DateTime<Utc>, WorkflowEvent)],
    exec_state: &str,
) -> Vec<DagRunNode> {
    // A view of just the events, for the classification/scan helpers.
    let events: Vec<WorkflowEvent> = timestamped_events
        .iter()
        .map(|(_, event)| event.clone())
        .collect();
    let tasks = def.tasks();

    tasks
        .iter()
        .enumerate()
        .map(|(task_index, task)| {
            let node_name = task.activity_name.clone();
            let base = node_outcome(&events, &node_name);

            // `classify` is the single source of truth for status, timing,
            // attempts, and error: all four describe the node's authoritative
            // (latest-scheduled) attempt-chain, so they never disagree about
            // which attempt they are reporting.
            let (status, started_at, finished_at, error_type, error, attempts) = classify(
                base,
                task_index,
                &node_name,
                &task.upstreams,
                &events,
                timestamped_events,
                exec_state,
            );

            let depends_on: Vec<String> = task
                .upstreams
                .iter()
                .filter_map(|&i| tasks.get(i).map(|t| t.activity_name.clone()))
                .collect();

            DagRunNode {
                node_name,
                status,
                depends_on,
                started_at,
                finished_at,
                attempts,
                error_type,
                error,
            }
        })
        .collect()
}

/// Resolve the derived status and timing/error fields for one node.
///
/// `base` is the authoritative [`node_outcome`] classification (AC5 anchor).
/// The two derived statuses ([`DagNodeStatus::Running`] vs
/// [`DagNodeStatus::Cancelled`], [`DagNodeStatus::Skipped`] vs
/// [`DagNodeStatus::Pending`]) are the only places history-plus-run-state adds
/// information beyond the base outcome.
#[allow(clippy::type_complexity)]
fn classify(
    base: NodeOutcome,
    task_index: usize,
    node_name: &str,
    upstreams: &[usize],
    events: &[WorkflowEvent],
    timestamped_events: &[(DateTime<Utc>, WorkflowEvent)],
    exec_state: &str,
) -> (
    DagNodeStatus,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    Option<String>,
    Option<String>,
    u32,
) {
    let status = match base {
        NodeOutcome::Succeeded => DagNodeStatus::Succeeded,
        NodeOutcome::Failed => DagNodeStatus::Failed,
        NodeOutcome::TimedOut => DagNodeStatus::TimedOut,
        NodeOutcome::Cancelled => {
            if is_live_state(exec_state) {
                DagNodeStatus::Running
            } else {
                DagNodeStatus::Cancelled
            }
        }
        NodeOutcome::NotAttempted => {
            if has_skip_marker(events, task_index, node_name, upstreams) {
                DagNodeStatus::Skipped
            } else {
                DagNodeStatus::Pending
            }
        }
    };

    // Timing, attempts, and error all describe the node's authoritative
    // (latest-scheduled) attempt-chain, mirroring node_outcome's latest-attempt
    // selection. A never-scheduled node (Pending/Skipped) has none, and reports
    // `attempts: 0`.
    let mut started_at = None;
    let mut finished_at = None;
    let mut error_type = None;
    let mut error = None;
    let mut attempts: u32 = 0;

    if let Some((sched_idx, activity_id)) = latest_scheduled(events, node_name) {
        let (started_count, latest_started_idx) = started_attempts(events, activity_id);
        // A scheduled node has made at least one attempt: the engine appends an
        // `ActivityStarted` per claim, so `started_count` is the true attempt
        // count for a claimed node. Floor to 1 so a scheduled-but-not-yet-claimed
        // PENDING node (or an abbreviated history that omits `ActivityStarted`)
        // still reports its single pending attempt rather than 0.
        attempts = started_count.max(1);
        // `started_at` is the LATEST attempt's actual start (its `ActivityStarted`
        // timestamp), not the original schedule — so a node that backed off and
        // retried is not made to look like it ran for the whole backoff window.
        // Fall back to the schedule timestamp when no `ActivityStarted` exists
        // (not yet claimed), so a running node still shows when it was scheduled.
        started_at = latest_started_idx
            .or(Some(sched_idx))
            .and_then(|i| timestamped_events.get(i).map(|(ts, _)| *ts));
        finished_at = terminal_index(events, activity_id)
            .and_then(|i| timestamped_events.get(i).map(|(ts, _)| *ts));
        if base == NodeOutcome::Failed
            && let Some((etype, emsg)) = failure_detail(events, activity_id)
        {
            error_type = etype;
            error = emsg;
        }
    }

    (status, started_at, finished_at, error_type, error, attempts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use autumn_harvest::dag::{DagBuilder, DagDefinition};
    use autumn_harvest::types::{ActivityExecId, WorkerId};
    use chrono::{TimeZone, Utc};
    use serde_json::Value;

    // Distinct activity functions → distinct node names.
    fn a() {}
    fn b() {}
    fn c() {}
    fn d() {}
    fn e() {}

    fn linear_dag() -> DagDefinition {
        // a -> b -> c -> d
        let mut builder = DagBuilder::new();
        let na = builder.activity(a);
        let nb = builder.activity(b).upstream(&na);
        let nc = builder.activity(c).upstream(&nb);
        let _nd = builder.activity(d).upstream(&nc);
        builder.build().expect("linear dag builds")
    }

    fn fanout_dag() -> DagDefinition {
        // a -> {b, c, d} -> e
        let mut builder = DagBuilder::new();
        let na = builder.activity(a);
        let nb = builder.activity(b).upstream(&na);
        let nc = builder.activity(c).upstream(&na);
        let nd = builder.activity(d).upstream(&na);
        let _ne = builder
            .activity(e)
            .upstream(&nb)
            .upstream(&nc)
            .upstream(&nd);
        builder.build().expect("fanout dag builds")
    }

    /// Linear DAG with a `.condition()` on node `b`, so a false condition
    /// records a `dag_skip:1` marker.
    fn conditional_dag() -> DagDefinition {
        // a -> b(cond) -> c
        let mut builder = DagBuilder::new();
        let na = builder.activity(a);
        let nb = builder.activity(b).upstream(&na).condition(|_| false);
        let _nc = builder.activity(c).upstream(&nb);
        builder.build().expect("conditional dag builds")
    }

    fn ts(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000 + secs, 0).single().unwrap()
    }

    fn sched(name: &str, id: ActivityExecId) -> WorkflowEvent {
        WorkflowEvent::ActivityScheduled {
            activity_id: id,
            name: name.to_string(),
            input: Value::Null,
            queue: "default".to_string(),
        }
    }

    fn completed(id: ActivityExecId) -> WorkflowEvent {
        WorkflowEvent::ActivityCompleted {
            activity_id: id,
            output: Value::Null,
        }
    }

    fn failed(id: ActivityExecId, error: &str) -> WorkflowEvent {
        WorkflowEvent::ActivityFailed {
            activity_id: id,
            error: error.to_string(),
            attempt: 1,
            error_type: "S3Error".to_string(),
            non_retryable: false,
            details: None,
        }
    }

    fn timed_out(id: ActivityExecId) -> WorkflowEvent {
        WorkflowEvent::ActivityTimedOut {
            activity_id: id,
            timeout_type: autumn_harvest::error::TimeoutType::StartToClose,
        }
    }

    /// An `ActivityStarted` for `id` — the engine appends one per worker claim
    /// (including each activity-level retry), reusing the original `activity_id`.
    fn activity_started(id: ActivityExecId) -> WorkflowEvent {
        WorkflowEvent::ActivityStarted {
            activity_id: id,
            worker_id: WorkerId::new("worker-1"),
        }
    }

    fn started() -> WorkflowEvent {
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: ts(0),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }
    }

    fn skip_marker(task_index: usize, activity_name: &str) -> WorkflowEvent {
        WorkflowEvent::MarkerRecorded {
            name: format!("dag_skip:{task_index}"),
            details: serde_json::json!({ "task": activity_name, "reason": "condition_false" }),
        }
    }

    /// A `dag_skip` marker in the current engine's full format (task + upstream
    /// fingerprint), for exercising the rename/reorder validation.
    fn skip_marker_full(task_index: usize, task: &str, upstreams: &[usize]) -> WorkflowEvent {
        WorkflowEvent::MarkerRecorded {
            name: format!("dag_skip:{task_index}"),
            details: serde_json::json!({
                "task": task,
                "reason": "condition_false",
                "upstreams": upstreams,
            }),
        }
    }

    /// A `dag_skip` marker whose `details` has been replaced by a codec envelope,
    /// exactly as `PayloadCodecs::transform_event_data` does on write when a
    /// non-identity `PayloadCodec` is configured (`details` is a payload field).
    /// The recorded task/upstream fingerprint is opaque here — the read path does
    /// not codec-decode — so classification must fall back to the (always
    /// in-the-clear) marker name/index (issue #690 review, Codex CLAIM D).
    fn skip_marker_codec_envelope(task_index: usize) -> WorkflowEvent {
        WorkflowEvent::MarkerRecorded {
            name: format!("dag_skip:{task_index}"),
            details: serde_json::json!({
                "_harvest_codec_envelope": 1,
                "codec_id": "reverse",
                "data": "b3BhcXVl",
            }),
        }
    }

    fn node<'a>(nodes: &'a [DagRunNode], name: &str) -> &'a DagRunNode {
        nodes.iter().find(|n| n.node_name == name).expect("node")
    }

    // ── status mapping ──────────────────────────────────────────────────────

    #[test]
    fn succeeded_failed_timed_out_mapping() {
        let def = fanout_dag();
        let (ia, ib, ic, id) = (
            ActivityExecId::new(),
            ActivityExecId::new(),
            ActivityExecId::new(),
            ActivityExecId::new(),
        );
        let events = vec![
            (ts(0), started()),
            (ts(1), sched("a", ia)),
            (ts(2), completed(ia)),
            (ts(3), sched("b", ib)),
            (ts(4), completed(ib)),
            (ts(5), sched("c", ic)),
            (ts(6), failed(ic, "transient S3 500")),
            (ts(7), sched("d", id)),
            (ts(8), timed_out(id)),
        ];
        let nodes = build_run_graph(&def, &events, "FAILED");

        assert_eq!(node(&nodes, "a").status, DagNodeStatus::Succeeded);
        assert_eq!(node(&nodes, "b").status, DagNodeStatus::Succeeded);
        assert_eq!(node(&nodes, "c").status, DagNodeStatus::Failed);
        assert_eq!(node(&nodes, "d").status, DagNodeStatus::TimedOut);
        // e was never reached.
        assert_eq!(node(&nodes, "e").status, DagNodeStatus::Pending);
    }

    #[test]
    fn timing_is_populated_from_event_timestamps() {
        let def = linear_dag();
        let ia = ActivityExecId::new();
        let events = vec![
            (ts(0), started()),
            (ts(10), sched("a", ia)),
            (ts(20), completed(ia)),
        ];
        let nodes = build_run_graph(&def, &events, "RUNNING");
        let a_node = node(&nodes, "a");
        assert_eq!(a_node.started_at, Some(ts(10)));
        assert_eq!(a_node.finished_at, Some(ts(20)));
    }

    #[test]
    fn attempts_counts_activity_started_not_scheduled() {
        // Realistic engine output for an activity-level retry (issue #690
        // review, Codex CLAIM A). The node is scheduled ONCE (one
        // ActivityScheduled / activity_id); the engine appends a fresh
        // ActivityStarted on each claim. `queue::requeue_for_retry` re-pends the
        // same task row and appends no event, and a *retryable* failure appends
        // NO ActivityFailed (only a retry-exhausting terminal failure does) — so
        // a node the engine ran twice has ONE ActivityScheduled and TWO
        // ActivityStarted. Counting ActivityScheduled (the pre-fix behavior)
        // would report attempts: 1 and anchor started_at to the original
        // schedule; this test would fail against that code.
        let def = linear_dag();
        let (ia, ib, ic) = (
            ActivityExecId::new(),
            ActivityExecId::new(),
            ActivityExecId::new(),
        );
        let events = vec![
            (ts(0), started()),
            (ts(1), sched("a", ia)),
            (ts(2), activity_started(ia)),
            (ts(3), completed(ia)),
            (ts(4), sched("b", ib)),
            (ts(5), activity_started(ib)),
            (ts(6), completed(ib)),
            (ts(7), sched("c", ic)),       // scheduled ONCE
            (ts(8), activity_started(ic)), // claim 1
            // attempt 1 fails retryably → requeue_for_retry (NO event appended)
            (ts(9), activity_started(ic)), // claim 2 (the retry)
            (ts(10), completed(ic)),
        ];
        let nodes = build_run_graph(&def, &events, "RUNNING");
        let c_node = node(&nodes, "c");
        // Two execution attempts, counted from ActivityStarted (there is exactly
        // ONE ActivityScheduled for the node).
        assert_eq!(c_node.attempts, 2);
        assert_eq!(c_node.status, DagNodeStatus::Succeeded);
        // Timing describes the LATEST attempt's actual start (ts(9)), not the
        // original schedule (ts(7)).
        assert_eq!(c_node.started_at, Some(ts(9)));
        assert_eq!(c_node.finished_at, Some(ts(10)));
        // A single-run node reports 1 attempt.
        assert_eq!(node(&nodes, "a").attempts, 1);
    }

    #[test]
    fn attempts_counts_all_started_when_retries_exhaust() {
        // A node that fails all its retries: ONE ActivityScheduled, N
        // ActivityStarted, then a single terminal ActivityFailed (attempt == N).
        let def = linear_dag();
        let (ia, ic) = (ActivityExecId::new(), ActivityExecId::new());
        let events = vec![
            (ts(0), started()),
            (ts(1), sched("a", ia)),
            (ts(2), activity_started(ia)),
            (ts(3), completed(ia)),
            (ts(4), sched("c", ic)),           // scheduled ONCE
            (ts(5), activity_started(ic)),     // claim 1
            (ts(6), activity_started(ic)),     // claim 2
            (ts(7), activity_started(ic)),     // claim 3
            (ts(8), failed(ic, "still boom")), // terminal (attempt 3)
        ];
        let nodes = build_run_graph(&def, &events, "FAILED");
        let c_node = node(&nodes, "c");
        assert_eq!(c_node.attempts, 3);
        assert_eq!(c_node.status, DagNodeStatus::Failed);
        // Latest attempt's start (ts(7)) and its terminal event (ts(8)).
        assert_eq!(c_node.started_at, Some(ts(7)));
        assert_eq!(c_node.finished_at, Some(ts(8)));
    }

    #[test]
    fn attempts_are_scoped_to_the_authoritative_latest_activity_id() {
        // DAG-level re-dispatch (a #366 retry-from-node reset re-runs the node
        // with a fresh activity_id). node_outcome treats the LATEST activity_id
        // as authoritative; attempts/timing/error follow suit, so `attempts`
        // reports the latest dispatch's execution count (here: 2 for ic2), not a
        // cumulative total across the superseded first dispatch (ic1).
        let def = linear_dag();
        let (ia, ic1, ic2) = (
            ActivityExecId::new(),
            ActivityExecId::new(),
            ActivityExecId::new(),
        );
        let events = vec![
            (ts(0), started()),
            (ts(1), sched("a", ia)),
            (ts(2), activity_started(ia)),
            (ts(3), completed(ia)),
            // First dispatch (superseded by a reset): 1 attempt, failed.
            (ts(4), sched("c", ic1)),
            (ts(5), activity_started(ic1)),
            (ts(6), failed(ic1, "boom")),
            // Second dispatch (fresh activity_id): 2 attempts, succeeded.
            (ts(7), sched("c", ic2)),
            (ts(8), activity_started(ic2)),
            (ts(9), activity_started(ic2)),
            (ts(10), completed(ic2)),
        ];
        let nodes = build_run_graph(&def, &events, "COMPLETED");
        let c_node = node(&nodes, "c");
        // Scoped to the authoritative (latest) activity_id ic2 → 2, not 3.
        assert_eq!(c_node.attempts, 2);
        assert_eq!(c_node.status, DagNodeStatus::Succeeded);
        assert_eq!(c_node.started_at, Some(ts(9)));
        assert_eq!(c_node.finished_at, Some(ts(10)));
    }

    #[test]
    fn scheduled_but_unclaimed_node_reports_one_attempt() {
        // A node scheduled but not yet claimed (no ActivityStarted) still made
        // one (pending) attempt; started_at falls back to the schedule time so a
        // running node shows when it was scheduled.
        let def = linear_dag();
        let (ia, ib) = (ActivityExecId::new(), ActivityExecId::new());
        let events = vec![
            (ts(0), started()),
            (ts(1), sched("a", ia)),
            (ts(2), activity_started(ia)),
            (ts(3), completed(ia)),
            (ts(4), sched("b", ib)), // scheduled, never claimed
        ];
        let nodes = build_run_graph(&def, &events, "RUNNING");
        let b_node = node(&nodes, "b");
        assert_eq!(b_node.status, DagNodeStatus::Running);
        assert_eq!(b_node.attempts, 1);
        assert_eq!(b_node.started_at, Some(ts(4))); // falls back to schedule ts
        assert!(b_node.finished_at.is_none());
        // A never-scheduled node reports 0 attempts.
        assert_eq!(node(&nodes, "c").attempts, 0);
    }

    #[test]
    fn failed_node_carries_error_type_and_truncated_first_line() {
        let def = linear_dag();
        let (ia, ib, ic) = (
            ActivityExecId::new(),
            ActivityExecId::new(),
            ActivityExecId::new(),
        );
        let long = format!("first line {}\nsecond line", "x".repeat(500));
        let events = vec![
            (ts(0), started()),
            (ts(1), sched("a", ia)),
            (ts(2), completed(ia)),
            (ts(3), sched("b", ib)),
            (ts(4), completed(ib)),
            (ts(5), sched("c", ic)),
            (ts(6), failed(ic, &long)),
        ];
        let nodes = build_run_graph(&def, &events, "FAILED");
        let c_node = node(&nodes, "c");
        assert_eq!(c_node.status, DagNodeStatus::Failed);
        assert_eq!(c_node.error_type.as_deref(), Some("S3Error"));
        let err = c_node.error.as_deref().expect("error");
        assert!(err.starts_with("first line xxx"), "err was: {err}");
        assert!(!err.contains("second line"), "must be first line only");
        assert!(err.ends_with("..."), "long line must be truncated");
        assert!(err.chars().count() <= ERROR_MAX_CHARS + 3);
        // A succeeded node carries no error fields.
        assert!(node(&nodes, "a").error.is_none());
        assert!(node(&nodes, "a").error_type.is_none());
    }

    #[test]
    fn running_vs_cancelled_depends_on_exec_state() {
        let def = linear_dag();
        let (ia, ib) = (ActivityExecId::new(), ActivityExecId::new());
        // b scheduled, no terminal event: NodeOutcome::Cancelled base.
        let events = vec![
            (ts(0), started()),
            (ts(1), sched("a", ia)),
            (ts(2), completed(ia)),
            (ts(3), sched("b", ib)),
        ];

        let live = build_run_graph(&def, &events, "RUNNING");
        assert_eq!(node(&live, "b").status, DagNodeStatus::Running);

        // PAUSED is a real non-terminal state (issue #383): a scheduled-no-terminal
        // node is still in flight → running, not cancelled.
        let paused = build_run_graph(&def, &events, "PAUSED");
        assert_eq!(node(&paused, "b").status, DagNodeStatus::Running);

        // A terminal state → cancelled.
        let dead = build_run_graph(&def, &events, "CANCELLED");
        assert_eq!(node(&dead, "b").status, DagNodeStatus::Cancelled);
    }

    #[test]
    fn concurrent_fanout_scheduled_nodes_both_report_running() {
        let def = fanout_dag();
        // a completed; b and c both scheduled with no terminal event; run RUNNING.
        let (ia, ib, ic) = (
            ActivityExecId::new(),
            ActivityExecId::new(),
            ActivityExecId::new(),
        );
        let events = vec![
            (ts(0), started()),
            (ts(1), sched("a", ia)),
            (ts(2), completed(ia)),
            (ts(3), sched("b", ib)),
            (ts(4), sched("c", ic)),
        ];
        let nodes = build_run_graph(&def, &events, "RUNNING");
        assert_eq!(node(&nodes, "a").status, DagNodeStatus::Succeeded);
        // Two simultaneously in-flight fan-out branches both report running.
        assert_eq!(node(&nodes, "b").status, DagNodeStatus::Running);
        assert_eq!(node(&nodes, "c").status, DagNodeStatus::Running);
        // The join node d was never reached.
        assert_eq!(node(&nodes, "d").status, DagNodeStatus::Pending);
    }

    #[test]
    fn skipped_vs_pending_depends_on_marker() {
        let def = conditional_dag();
        let ia = ActivityExecId::new();
        // a succeeds; b is skipped by condition (dag_skip:1 marker); c never reached.
        let with_marker = vec![
            (ts(0), started()),
            (ts(1), sched("a", ia)),
            (ts(2), completed(ia)),
            (ts(3), skip_marker(1, "b")),
        ];
        let nodes = build_run_graph(&def, &with_marker, "RUNNING");
        assert_eq!(node(&nodes, "b").status, DagNodeStatus::Skipped);
        assert_eq!(node(&nodes, "c").status, DagNodeStatus::Pending);
        // A skipped node has no timing.
        assert!(node(&nodes, "b").started_at.is_none());
        assert!(node(&nodes, "b").finished_at.is_none());

        // Without the marker, b (never scheduled) is pending, not skipped.
        let without_marker = vec![
            (ts(0), started()),
            (ts(1), sched("a", ia)),
            (ts(2), completed(ia)),
        ];
        let nodes = build_run_graph(&def, &without_marker, "RUNNING");
        assert_eq!(node(&nodes, "b").status, DagNodeStatus::Pending);
    }

    #[test]
    fn skip_marker_with_mismatched_task_is_ignored() {
        // Issue #690 review (Codex CLAIM B): a dag_skip:{idx} marker whose
        // recorded details.task no longer identifies the current node at that
        // index — because the DAG was reordered or a task renamed after this run
        // recorded the marker — must NOT mislabel the current node as skipped.
        // It falls through to pending. (The pre-fix has_skip_marker matched on
        // the bare marker name alone and would report Skipped here.)
        let def = conditional_dag(); // a(0) -> b(1)[cond] -> c(2)
        let ia = ActivityExecId::new();
        let events = vec![
            (ts(0), started()),
            (ts(1), sched("a", ia)),
            (ts(2), completed(ia)),
            // Marker at index 1 was recorded for a node named "was_renamed",
            // not the "b" now occupying index 1.
            (ts(3), skip_marker(1, "was_renamed")),
        ];
        let nodes = build_run_graph(&def, &events, "RUNNING");
        assert_eq!(node(&nodes, "b").status, DagNodeStatus::Pending);
    }

    #[test]
    fn skip_marker_with_matching_task_and_upstreams_is_skipped() {
        // A full-format marker (task + upstream fingerprint) that still matches
        // the current node marks it skipped — the positive control for the
        // validation added in the CLAIM B fix.
        let def = conditional_dag(); // b(1) upstream is [0]
        let ia = ActivityExecId::new();
        let events = vec![
            (ts(0), started()),
            (ts(1), sched("a", ia)),
            (ts(2), completed(ia)),
            (ts(3), skip_marker_full(1, "b", &[0])),
        ];
        let nodes = build_run_graph(&def, &events, "RUNNING");
        assert_eq!(node(&nodes, "b").status, DagNodeStatus::Skipped);
    }

    #[test]
    fn skip_marker_with_mismatched_upstreams_is_ignored() {
        // A full-format marker whose recorded upstream fingerprint no longer
        // matches the current node's edges (topology changed at the same index)
        // is ignored, mirroring the Vantage UI's validation.
        let def = conditional_dag(); // b(1) upstream is [0]
        let ia = ActivityExecId::new();
        let events = vec![
            (ts(0), started()),
            (ts(1), sched("a", ia)),
            (ts(2), completed(ia)),
            // Correct task name, but recorded upstreams [2] != current [0].
            (ts(3), skip_marker_full(1, "b", &[2])),
        ];
        let nodes = build_run_graph(&def, &events, "RUNNING");
        assert_eq!(node(&nodes, "b").status, DagNodeStatus::Pending);
    }

    #[test]
    fn skip_marker_with_opaque_codec_envelope_details_is_still_skipped() {
        // Issue #690 review (Codex CLAIM D): on a deployment with a non-identity
        // PayloadCodec, a `dag_skip:{idx}` marker's `details` is stored as an
        // opaque codec envelope (`details` is a payload field), so the recorded
        // task/upstream fingerprint is not readable at this read path. The node
        // must still be reported `skipped` (falling back to the in-the-clear
        // marker name/index) rather than `pending` — the marker name is never a
        // payload field. Before the fix, the unreadable `details.task` made
        // has_skip_marker return false and the node was wrongly reported pending.
        let def = conditional_dag(); // a(0) -> b(1)[cond] -> c(2)
        let ia = ActivityExecId::new();
        let events = vec![
            (ts(0), started()),
            (ts(1), sched("a", ia)),
            (ts(2), completed(ia)),
            (ts(3), skip_marker_codec_envelope(1)),
        ];
        let nodes = build_run_graph(&def, &events, "RUNNING");
        assert_eq!(node(&nodes, "b").status, DagNodeStatus::Skipped);
        assert_eq!(node(&nodes, "c").status, DagNodeStatus::Pending);
        // A skipped node still has no timing.
        assert!(node(&nodes, "b").started_at.is_none());
    }

    // ── depends_on topology ─────────────────────────────────────────────────

    #[test]
    fn depends_on_linear() {
        let def = linear_dag();
        let nodes = build_run_graph(&def, &[(ts(0), started())], "RUNNING");
        assert!(node(&nodes, "a").depends_on.is_empty());
        assert_eq!(node(&nodes, "b").depends_on, vec!["a".to_string()]);
        assert_eq!(node(&nodes, "c").depends_on, vec!["b".to_string()]);
        assert_eq!(node(&nodes, "d").depends_on, vec!["c".to_string()]);
    }

    #[test]
    fn depends_on_fanout_join() {
        let def = fanout_dag();
        let nodes = build_run_graph(&def, &[(ts(0), started())], "RUNNING");
        assert!(node(&nodes, "a").depends_on.is_empty());
        assert_eq!(node(&nodes, "b").depends_on, vec!["a".to_string()]);
        assert_eq!(node(&nodes, "c").depends_on, vec!["a".to_string()]);
        assert_eq!(node(&nodes, "d").depends_on, vec!["a".to_string()]);
        // Join node e depends on b, c, d in declaration order.
        assert_eq!(
            node(&nodes, "e").depends_on,
            vec!["b".to_string(), "c".to_string(), "d".to_string()]
        );
    }

    // ── AC5: consistency with the #366 retry resolver ───────────────────────

    #[test]
    fn status_projects_node_outcome_for_every_node() {
        let def = fanout_dag();
        let (ia, ib, ic, id) = (
            ActivityExecId::new(),
            ActivityExecId::new(),
            ActivityExecId::new(),
            ActivityExecId::new(),
        );
        let events = vec![
            (ts(0), started()),
            (ts(1), sched("a", ia)),
            (ts(2), completed(ia)),
            (ts(3), sched("b", ib)),
            (ts(4), completed(ib)),
            (ts(5), sched("c", ic)),
            (ts(6), failed(ic, "boom")),
            (ts(7), sched("d", id)),
        ];
        let events_only: Vec<WorkflowEvent> = events.iter().map(|(_, e)| e.clone()).collect();
        let nodes = build_run_graph(&def, &events, "FAILED");
        for n in &nodes {
            let outcome = node_outcome(&events_only, &n.node_name);
            let consistent = match outcome {
                NodeOutcome::Succeeded => n.status == DagNodeStatus::Succeeded,
                NodeOutcome::Failed => n.status == DagNodeStatus::Failed,
                NodeOutcome::TimedOut => n.status == DagNodeStatus::TimedOut,
                // d is scheduled-no-terminal on a FAILED run → Cancelled.
                NodeOutcome::Cancelled => {
                    n.status == DagNodeStatus::Cancelled || n.status == DagNodeStatus::Running
                }
                NodeOutcome::NotAttempted => {
                    n.status == DagNodeStatus::Pending || n.status == DagNodeStatus::Skipped
                }
            };
            assert!(
                consistent,
                "node {} outcome {:?} inconsistent with status {:?}",
                n.node_name, outcome, n.status
            );
        }
    }

    #[test]
    fn first_line_truncated_helper() {
        assert_eq!(first_line_truncated("simple").as_deref(), Some("simple"));
        assert_eq!(
            first_line_truncated("line1\nline2").as_deref(),
            Some("line1")
        );
        let long = "y".repeat(300);
        let out = first_line_truncated(&long).expect("non-empty");
        assert_eq!(out.chars().count(), ERROR_MAX_CHARS + 3);
        assert!(out.ends_with("..."));
        // An empty first line yields None (omitted, not Some("")).
        assert_eq!(first_line_truncated(""), None);
        assert_eq!(first_line_truncated("\nsecond line"), None);
    }

    #[test]
    fn failed_node_with_empty_error_omits_error_field() {
        let def = linear_dag();
        let (ia, ib) = (ActivityExecId::new(), ActivityExecId::new());
        // A failed activity whose error message is empty (and error_type empty).
        let empty_fail = WorkflowEvent::ActivityFailed {
            activity_id: ib,
            error: String::new(),
            attempt: 1,
            error_type: String::new(),
            non_retryable: false,
            details: None,
        };
        let events = vec![
            (ts(0), started()),
            (ts(1), sched("a", ia)),
            (ts(2), completed(ia)),
            (ts(3), sched("b", ib)),
            (ts(4), empty_fail),
        ];
        let nodes = build_run_graph(&def, &events, "FAILED");
        let b_node = node(&nodes, "b");
        assert_eq!(b_node.status, DagNodeStatus::Failed);
        // Empty error / error_type are omitted (None), not Some("").
        assert!(b_node.error.is_none(), "empty error must be omitted");
        assert!(
            b_node.error_type.is_none(),
            "empty error_type must be omitted"
        );

        // Serialized JSON must not carry the keys at all.
        let value = serde_json::to_value(b_node).expect("serialize");
        assert!(value.get("error").is_none(), "json: {value}");
        assert!(value.get("error_type").is_none(), "json: {value}");
    }

    #[test]
    fn duplicate_activity_names_produce_ambiguous_duplicated_nodes() {
        // The node-name==activity-name contract assumes activity names are unique
        // within a DAG. `DagBuilder::build()` does NOT reject duplicates (only the
        // #366 retry resolver does, by rejecting the whole DAG). This read view
        // neither guards nor rejects — it always shows the graph — so two tasks
        // sharing an activity name yield two ambiguous, duplicated node entries
        // with the same name-keyed status. This test pins that documented v1
        // behavior so a future change is intentional.
        let mut builder = DagBuilder::new();
        let n1 = builder.activity(a);
        // Same activity function `a` again → same activity_name "a".
        let _n2 = builder.activity(a).upstream(&n1);
        let def = builder.build().expect("dag with duplicate names builds");

        let ia = ActivityExecId::new();
        let events = vec![
            (ts(0), started()),
            (ts(1), sched("a", ia)),
            (ts(2), completed(ia)),
        ];
        let nodes = build_run_graph(&def, &events, "RUNNING");

        // Two node entries, both named "a".
        let dupes: Vec<&DagRunNode> = nodes.iter().filter(|n| n.node_name == "a").collect();
        assert_eq!(dupes.len(), 2, "both duplicated nodes are present");
        // Both carry the same name-keyed status (ambiguous by construction).
        assert_eq!(dupes[0].status, dupes[1].status);
        assert_eq!(dupes[0].status, DagNodeStatus::Succeeded);
    }

    #[test]
    fn mapped_task_collapses_to_latest_scheduled_instance() {
        // A mapped (fan-out) task is a single DagTask, but the unified DAG
        // handler dispatches one activity instance per input-array element, all
        // sharing the task's activity name with distinct activity_ids and NO
        // `fan_out:` marker. Because node identity is keyed purely by activity
        // name and `latest_scheduled` (this view) and `node_outcome` (the #366
        // retry resolver) both select the LATEST-scheduled instance for the
        // name, the whole mapped node collapses to that single instance's
        // outcome (issue #690 review, Codex CLAIM C).
        //
        // This test PINS that documented v1 collapse — it is NOT asserting the
        // "ideal" map-aware aggregate. On a FAILED run where an EARLIER mapped
        // element failed but the LATER element succeeded, the node is reported
        // `succeeded` (the latest-scheduled element B), not `failed`. Diverging
        // from this would break AC5 consistency with the #366 retry endpoint.
        //
        // The mapped node is a single task; the collapse is purely name-keyed,
        // so a plain single-node DAG faithfully models it.
        let mut builder = DagBuilder::new();
        let _na = builder.activity(a);
        let def = builder.build().expect("single-node dag builds");

        let (id_a, id_b) = (ActivityExecId::new(), ActivityExecId::new());
        // Two same-name "a" instances (a mapped fan-out): earlier element A
        // fails, later element B succeeds; distinct timestamps so B is latest.
        let events = vec![
            (ts(0), started()),
            (ts(1), sched("a", id_a)),
            (ts(2), activity_started(id_a)),
            (ts(3), sched("a", id_b)),
            (ts(4), activity_started(id_b)),
            (ts(5), failed(id_a, "element A failed")),
            (ts(6), completed(id_b)),
        ];
        let nodes = build_run_graph(&def, &events, "FAILED");

        // The collapse reports the LATEST-scheduled instance (B → completed).
        assert_eq!(node(&nodes, "a").status, DagNodeStatus::Succeeded);

        // AC5: this exactly matches the #366 retry resolver for the same
        // history — both collapse to the latest-scheduled instance's outcome.
        let plain: Vec<WorkflowEvent> = events.into_iter().map(|(_ts, ev)| ev).collect();
        assert_eq!(node_outcome(&plain, "a"), NodeOutcome::Succeeded);
    }

    #[test]
    fn empty_mapped_fan_out_reports_pending_on_completed_run() {
        // A mapped task over an EMPTY upstream array dispatches zero instances:
        // the unified DAG handler returns TaskStatus::Succeeded without recording
        // any ActivityScheduled/activity events for the node (dag.rs
        // `__n_instances == 0` guard). With zero events, node_outcome returns
        // NotAttempted and this view reports the node `pending` on a COMPLETED
        // run — because from history alone an empty-mapped node is
        // indistinguishable from a not-yet-reached node (and from a
        // SkipByTriggerRule skip, which also records no marker).
        //
        // This test PINS that documented v1 behavior — NOT the ideal `succeeded`
        // classification. It matches node_outcome's NotAttempted for the same
        // history (AC5), and a precise fix is deferred to a coordinated #366
        // change (same rationale as the instance-collapse case). Modelled as a
        // two-node DAG where the mapped node `a` fans out over nothing while the
        // downstream node `b` runs and succeeds — the mapped node still reports
        // `pending`, not `succeeded`, even on a COMPLETED run.
        let def = linear_dag(); // a -> b -> c -> d
        let ib = ActivityExecId::new();
        // `a` (the empty mapped fan-out) appends NO events; `b` runs & succeeds.
        let events = vec![
            (ts(0), started()),
            (ts(1), sched("b", ib)),
            (ts(2), activity_started(ib)),
            (ts(3), completed(ib)),
        ];
        let nodes = build_run_graph(&def, &events, "COMPLETED");

        // The empty-mapped node reports `pending` (the documented current
        // behavior), not `succeeded`.
        assert_eq!(node(&nodes, "a").status, DagNodeStatus::Pending);
        assert_eq!(node(&nodes, "a").attempts, 0);
        assert!(node(&nodes, "a").started_at.is_none());
        // The downstream node that actually ran is succeeded.
        assert_eq!(node(&nodes, "b").status, DagNodeStatus::Succeeded);

        // AC5: node_outcome returns NotAttempted for the same zero-event history,
        // so the graph and the #366 retry path agree.
        let plain: Vec<WorkflowEvent> = events.into_iter().map(|(_ts, ev)| ev).collect();
        assert_eq!(node_outcome(&plain, "a"), NodeOutcome::NotAttempted);
    }
}
