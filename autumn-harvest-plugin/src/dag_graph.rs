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
    /// When the node's latest attempt was scheduled, if it was ever scheduled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    /// When the node's latest attempt reached a terminal event, if it did.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
    /// Number of times the node's activity was scheduled (attempts).
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
/// characters (char-boundary safe), appending `...` when truncated.
fn first_line_truncated(error: &str) -> String {
    let first_line = error.lines().next().unwrap_or("");
    let mut chars = first_line.chars();
    let truncated: String = chars.by_ref().take(ERROR_MAX_CHARS).collect();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

/// Whether the execution state means the run is still live (a scheduled node
/// with no terminal event is running, not cancelled).
fn is_live_state(exec_state: &str) -> bool {
    matches!(exec_state, "RUNNING" | "SUSPENDED")
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

/// Count how many times `node` was scheduled (attempts).
fn count_attempts(events: &[WorkflowEvent], node: &str) -> u32 {
    let count = events
        .iter()
        .filter(
            |event| matches!(event, WorkflowEvent::ActivityScheduled { name, .. } if name == node),
        )
        .count();
    u32::try_from(count).unwrap_or(u32::MAX)
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
/// for `activity_id`, first line truncated.
fn failure_detail(
    events: &[WorkflowEvent],
    activity_id: ActivityExecId,
) -> Option<(String, String)> {
    events.iter().rev().find_map(|event| match event {
        WorkflowEvent::ActivityFailed {
            activity_id: id,
            error,
            error_type,
            ..
        } if *id == activity_id => Some((error_type.clone(), first_line_truncated(error))),
        _ => None,
    })
}

/// Whether a `dag_skip:{task_index}` marker was recorded for `task_index`.
fn has_skip_marker(events: &[WorkflowEvent], task_index: usize) -> bool {
    let marker_name = format!("dag_skip:{task_index}");
    events.iter().any(
        |event| matches!(event, WorkflowEvent::MarkerRecorded { name, .. } if *name == marker_name),
    )
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

            // `classify` is the single source of truth for status, timing, and
            // error: error fields are set only for a Failed base, and timing
            // only for a node that was actually scheduled.
            let (status, started_at, finished_at, error_type, error) = classify(
                base,
                task_index,
                &node_name,
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
                attempts: count_attempts(&events, &task.activity_name),
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
    events: &[WorkflowEvent],
    timestamped_events: &[(DateTime<Utc>, WorkflowEvent)],
    exec_state: &str,
) -> (
    DagNodeStatus,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    Option<String>,
    Option<String>,
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
            if has_skip_marker(events, task_index) {
                DagNodeStatus::Skipped
            } else {
                DagNodeStatus::Pending
            }
        }
    };

    // Timing + error describe the node's latest scheduled attempt, mirroring
    // node_outcome's latest-attempt selection. A never-scheduled node
    // (Pending/Skipped) has none.
    let mut started_at = None;
    let mut finished_at = None;
    let mut error_type = None;
    let mut error = None;

    if let Some((sched_idx, activity_id)) = latest_scheduled(events, node_name) {
        started_at = timestamped_events.get(sched_idx).map(|(ts, _)| *ts);
        finished_at = terminal_index(events, activity_id)
            .and_then(|i| timestamped_events.get(i).map(|(ts, _)| *ts));
        if base == NodeOutcome::Failed
            && let Some((etype, emsg)) = failure_detail(events, activity_id)
        {
            error_type = Some(etype);
            error = Some(emsg);
        }
    }

    (status, started_at, finished_at, error_type, error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use autumn_harvest::dag::{DagBuilder, DagDefinition};
    use autumn_harvest::types::ActivityExecId;
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
    fn attempts_counts_reschedules() {
        let def = linear_dag();
        // c scheduled twice: first attempt failed, second succeeded.
        let (ia, ib, ic1, ic2) = (
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
            (ts(5), sched("c", ic1)),
            (ts(6), failed(ic1, "boom")),
            (ts(7), sched("c", ic2)),
            (ts(8), completed(ic2)),
        ];
        let nodes = build_run_graph(&def, &events, "RUNNING");
        let c_node = node(&nodes, "c");
        assert_eq!(c_node.attempts, 2);
        assert_eq!(c_node.status, DagNodeStatus::Succeeded);
        // Timing describes the latest attempt (ic2).
        assert_eq!(c_node.started_at, Some(ts(7)));
        assert_eq!(c_node.finished_at, Some(ts(8)));
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

        let dead = build_run_graph(&def, &events, "CANCELLED");
        assert_eq!(node(&dead, "b").status, DagNodeStatus::Cancelled);
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
        assert_eq!(first_line_truncated("simple"), "simple");
        assert_eq!(first_line_truncated("line1\nline2"), "line1");
        let long = "y".repeat(300);
        let out = first_line_truncated(&long);
        assert_eq!(out.chars().count(), ERROR_MAX_CHARS + 3);
        assert!(out.ends_with("..."));
    }
}
