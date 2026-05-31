//! DAG retry-from-failed-node resolver (issue #366).
//!
//! This module contains the **pure** orchestration logic that maps an operator
//! request `(dag_name, run_exec_id, from_nodes)` onto a single
//! `reset_to_event_id` that the existing workflow-reset primitive (#148) can
//! consume. It adds no new core engine primitive: it reads the registered
//! [`DagDefinition`] for topology and walks the source workflow's recorded
//! [`WorkflowEvent`] history to find the earliest [`WorkflowEvent::ActivityScheduled`]
//! whose activity binding matches the requested nodes (and any node downstream
//! of them under the DAG's declared edges).
//!
//! ## Node identity
//!
//! A unified DAG node is identified by its **activity name** — `DagTask`
//! records the bound activity name and its `upstreams` (edges). The
//! `ActivityScheduled` event records the same activity name, so node name and
//! activity name are the same string. (v1 assumes node/activity names are
//! unique within a DAG.)
//!
//! ## Level-granular semantics
//!
//! The computed `reset_to_event_id` is `earliest_reexecute_schedule - 1`. The
//! caller passes it through the #148 reset-validity validator. When the failed
//! node shares a *parallel* level with siblings that were scheduled before it,
//! that boundary lands mid-level and the validator rejects it: the management
//! API surfaces a `409 Conflict` with a remediation hint to widen `from_nodes`
//! to include the siblings. Widening moves the cut to the clean boundary before
//! the whole level, re-executing the failed node and its same-level siblings
//! together. Upstream nodes are always carried over.

use std::collections::{BTreeSet, VecDeque};

use autumn_harvest::dag::DagDefinition;
use autumn_harvest::event::WorkflowEvent;

/// Terminal (or non-terminal) outcome of a single DAG node on the source run,
/// derived purely from the recorded event history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeOutcome {
    /// The node's activity recorded an `ActivityCompleted`.
    Succeeded,
    /// The node's activity recorded a terminal `ActivityFailed` (and no later
    /// completion).
    Failed,
    /// The node's activity recorded an `ActivityTimedOut`.
    TimedOut,
    /// The node's activity was scheduled but recorded no terminal event (the
    /// run was cancelled / force-failed while the activity was in flight).
    Cancelled,
    /// The node was never scheduled on the source run (skipped by an upstream
    /// failure, or simply not reached).
    NotAttempted,
}

impl NodeOutcome {
    #[must_use]
    const fn is_attempted(self) -> bool {
        !matches!(self, Self::NotAttempted)
    }

    #[must_use]
    const fn is_succeeded(self) -> bool {
        matches!(self, Self::Succeeded)
    }
}

/// Reasons the resolver rejects a retry request before any reset is attempted.
///
/// Each maps to a `400 Bad Request` at the management API layer (the
/// reset-validity boundary `409` is produced later, by the #148 validator).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DagRetryResolveError {
    /// `from_nodes` was empty.
    EmptyFromNodes,
    /// One or more requested nodes are not declared in the DAG definition.
    UnknownNodes {
        /// The requested node names that are not declared.
        unknown: Vec<String>,
        /// The full sorted list of declared node names, for the error body.
        declared: Vec<String>,
    },
    /// One or more requested nodes were never attempted on the source run.
    NotAttempted {
        /// The requested node names with no `ActivityScheduled` event.
        nodes: Vec<String>,
    },
    /// One or more requested nodes already succeeded on the source run.
    AlreadySucceeded {
        /// The requested node names whose activity completed successfully.
        nodes: Vec<String>,
    },
    /// None of the re-execute-set nodes have an `ActivityScheduled` event, so
    /// no reset point can be derived. (Defensive — should not occur once the
    /// per-node attempted check passes.)
    NoSchedulePoint,
}

/// A resolved, dry-runnable retry plan: the reset point plus the explicit
/// enumeration of what re-executes and what is carried over.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DagRetryPlan {
    /// The 0-based event id to reset the source execution to (carry over
    /// events `0..=reset_to_event_id`).
    pub reset_to_event_id: i64,
    /// Every node that will (re-)execute on the fork, sorted. This is computed
    /// from the actual cut — it includes the failed node, its declared
    /// downstream, and any node whose scheduling falls after the cut — so the
    /// operator gets no surprises.
    pub nodes_to_re_execute: Vec<String>,
    /// Every node whose recorded result is preserved (scheduled at or before
    /// the cut), sorted.
    pub nodes_carried_over: Vec<String>,
}

/// Returns the sorted set of distinct declared node (activity) names.
#[must_use]
pub fn declared_nodes(def: &DagDefinition) -> Vec<String> {
    let mut names: BTreeSet<String> = BTreeSet::new();
    for task in def.tasks() {
        names.insert(task.activity_name.clone());
    }
    names.into_iter().collect()
}

/// Compute the downstream closure (inclusive of the requested nodes) over the
/// DAG's declared edges, returned as a set of node (activity) names.
///
/// Edges are encoded as `DagTask.upstreams` (reverse edges); this builds the
/// forward adjacency and does a breadth-first reach from every task index whose
/// activity name is in `from_nodes`.
#[must_use]
pub fn downstream_closure(def: &DagDefinition, from_nodes: &BTreeSet<String>) -> BTreeSet<String> {
    let tasks = def.tasks();
    // Forward adjacency: forward[u] = tasks that declare u as an upstream.
    let mut forward: Vec<Vec<usize>> = vec![Vec::new(); tasks.len()];
    for (idx, task) in tasks.iter().enumerate() {
        for &up in &task.upstreams {
            if up < forward.len() {
                forward[up].push(idx);
            }
        }
    }

    let mut queue: VecDeque<usize> = VecDeque::new();
    let mut seen: BTreeSet<usize> = BTreeSet::new();
    for (idx, task) in tasks.iter().enumerate() {
        if from_nodes.contains(&task.activity_name) && seen.insert(idx) {
            queue.push_back(idx);
        }
    }

    while let Some(idx) = queue.pop_front() {
        for &down in &forward[idx] {
            if seen.insert(down) {
                queue.push_back(down);
            }
        }
    }

    seen.into_iter()
        .map(|i| tasks[i].activity_name.clone())
        .collect()
}

/// Determine the outcome of a single node (by activity name) on the source run.
#[must_use]
pub fn node_outcome(events: &[WorkflowEvent], node: &str) -> NodeOutcome {
    // Find the activity_id of the *latest* ActivityScheduled with this name.
    // If a node was scheduled more than once (e.g. a re-dispatched attempt with
    // a fresh activity_id), the latest attempt is the one whose terminal state
    // is authoritative — the earlier attempts are superseded.
    let mut scheduled_id = None;
    for event in events.iter().rev() {
        if let WorkflowEvent::ActivityScheduled {
            activity_id, name, ..
        } = event
            && name == node
        {
            scheduled_id = Some(*activity_id);
            break;
        }
    }
    let Some(activity_id) = scheduled_id else {
        return NodeOutcome::NotAttempted;
    };

    // Completion wins over any earlier failed attempt for this activity_id.
    for event in events {
        if let WorkflowEvent::ActivityCompleted {
            activity_id: id, ..
        } = event
            && *id == activity_id
        {
            return NodeOutcome::Succeeded;
        }
    }
    for event in events {
        match event {
            WorkflowEvent::ActivityTimedOut {
                activity_id: id, ..
            } if *id == activity_id => {
                return NodeOutcome::TimedOut;
            }
            WorkflowEvent::ActivityFailed {
                activity_id: id, ..
            } if *id == activity_id => {
                return NodeOutcome::Failed;
            }
            _ => {}
        }
    }
    // Scheduled but no terminal event recorded: the run was cancelled/aborted
    // while the activity was in flight.
    NodeOutcome::Cancelled
}

/// Index of the earliest `ActivityScheduled` event whose activity name is in
/// `reexecute`, if any.
#[must_use]
fn earliest_schedule_index(
    events: &[WorkflowEvent],
    reexecute: &BTreeSet<String>,
) -> Option<usize> {
    events.iter().position(|event| {
        matches!(event, WorkflowEvent::ActivityScheduled { name, .. } if reexecute.contains(name))
    })
}

/// Resolve a retry request into a [`DagRetryPlan`].
///
/// Pure: takes the registered topology and the recorded history. Performs all
/// node-validity checks (declared / attempted / non-succeeded) and computes the
/// reset point as `earliest_reexecute_schedule - 1`. The caller is responsible
/// for running that point through the #148 reset-validity validator (which may
/// reject a mid-parallel-level cut with `409`).
///
/// # Errors
///
/// Returns [`DagRetryResolveError`] for empty / unknown / unattempted /
/// already-succeeded node requests.
pub fn resolve_retry_plan(
    def: &DagDefinition,
    events: &[WorkflowEvent],
    from_nodes: &[String],
) -> Result<DagRetryPlan, DagRetryResolveError> {
    if from_nodes.is_empty() {
        return Err(DagRetryResolveError::EmptyFromNodes);
    }

    let declared = declared_nodes(def);
    let declared_set: BTreeSet<&str> = declared.iter().map(String::as_str).collect();

    // (a) Every requested node must be declared.
    let unknown: Vec<String> = from_nodes
        .iter()
        .filter(|n| !declared_set.contains(n.as_str()))
        .cloned()
        .collect();
    if !unknown.is_empty() {
        // De-dup while preserving the request order.
        let mut seen = BTreeSet::new();
        let unknown = unknown
            .into_iter()
            .filter(|n| seen.insert(n.clone()))
            .collect();
        return Err(DagRetryResolveError::UnknownNodes { unknown, declared });
    }

    // (b) Every requested node must have been attempted, and (c) be in a
    // non-Succeeded state on the source run.
    let mut not_attempted = Vec::new();
    let mut already_succeeded = Vec::new();
    for node in from_nodes {
        let outcome = node_outcome(events, node);
        if !outcome.is_attempted() {
            not_attempted.push(node.clone());
        } else if outcome.is_succeeded() {
            already_succeeded.push(node.clone());
        }
    }
    if !not_attempted.is_empty() {
        return Err(DagRetryResolveError::NotAttempted {
            nodes: not_attempted,
        });
    }
    if !already_succeeded.is_empty() {
        return Err(DagRetryResolveError::AlreadySucceeded {
            nodes: already_succeeded,
        });
    }

    // Re-execute set = requested nodes + their declared downstream closure.
    let requested: BTreeSet<String> = from_nodes.iter().cloned().collect();
    let reexecute = downstream_closure(def, &requested);

    // Reset point: just before the earliest scheduling of any re-execute node.
    let first_idx =
        earliest_schedule_index(events, &reexecute).ok_or(DagRetryResolveError::NoSchedulePoint)?;
    let reset_to_event_id = i64::try_from(first_idx).unwrap_or(i64::MAX) - 1;

    // Honest enumeration computed from the actual cut: a node is "carried over"
    // iff its scheduling is at or before the cut; everything else (declared)
    // will be re-evaluated by the replayed DAG handler.
    let cut = reset_to_event_id;
    let mut carried: BTreeSet<String> = BTreeSet::new();
    for (idx, event) in events.iter().enumerate() {
        if i64::try_from(idx).unwrap_or(i64::MAX) > cut {
            break;
        }
        if let WorkflowEvent::ActivityScheduled { name, .. } = event {
            carried.insert(name.clone());
        }
    }

    let nodes_carried_over: Vec<String> = carried.iter().cloned().collect();
    let nodes_to_re_execute: Vec<String> = declared
        .iter()
        .filter(|n| !carried.contains(*n))
        .cloned()
        .collect();

    Ok(DagRetryPlan {
        reset_to_event_id,
        nodes_to_re_execute,
        nodes_carried_over,
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::iter_on_single_items)]
    use super::*;
    use autumn_harvest::dag::DagBuilder;
    use autumn_harvest::types::ActivityExecId;
    use serde_json::Value;

    // Distinct activity functions so each DAG node has a unique activity name.
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

    fn scheduled(name: &str, id: ActivityExecId) -> WorkflowEvent {
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

    fn failed(id: ActivityExecId) -> WorkflowEvent {
        WorkflowEvent::ActivityFailed {
            activity_id: id,
            error: "boom".to_string(),
            attempt: 1,
            error_type: "Error".to_string(),
            non_retryable: false,
            details: None,
        }
    }

    fn started() -> WorkflowEvent {
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: chrono::Utc::now(),
        }
    }

    // ---- downstream propagation ------------------------------------------

    #[test]
    fn downstream_closure_linear_includes_self_and_all_downstream() {
        let def = linear_dag();
        let from: BTreeSet<String> = ["c".to_string()].into_iter().collect();
        let closure = downstream_closure(&def, &from);
        assert_eq!(
            closure,
            ["c".to_string(), "d".to_string()].into_iter().collect()
        );
    }

    #[test]
    fn downstream_closure_fanout_branch_reaches_join() {
        let def = fanout_dag();
        let from: BTreeSet<String> = ["c".to_string()].into_iter().collect();
        let closure = downstream_closure(&def, &from);
        // c's only downstream is the join node e; siblings b and d are NOT
        // downstream of c.
        assert_eq!(
            closure,
            ["c".to_string(), "e".to_string()].into_iter().collect()
        );
    }

    // ---- node outcome ----------------------------------------------------

    #[test]
    fn node_outcome_classifies_succeeded_failed_and_not_attempted() {
        let id_a = ActivityExecId::new();
        let id_c = ActivityExecId::new();
        let events = vec![
            started(),
            scheduled("a", id_a),
            completed(id_a),
            scheduled("c", id_c),
            failed(id_c),
        ];
        assert_eq!(node_outcome(&events, "a"), NodeOutcome::Succeeded);
        assert_eq!(node_outcome(&events, "c"), NodeOutcome::Failed);
        assert_eq!(node_outcome(&events, "d"), NodeOutcome::NotAttempted);
    }

    #[test]
    fn node_outcome_uses_latest_attempt_when_rescheduled() {
        // A node scheduled twice: first attempt (id1) failed, a later attempt
        // (id2) succeeded. The latest attempt is authoritative -> Succeeded.
        let id1 = ActivityExecId::new();
        let id2 = ActivityExecId::new();
        let events = vec![
            started(),
            scheduled("c", id1),
            failed(id1),
            scheduled("c", id2),
            completed(id2),
        ];
        assert_eq!(node_outcome(&events, "c"), NodeOutcome::Succeeded);
    }

    // ---- resolve: linear -------------------------------------------------

    #[test]
    fn resolve_linear_retry_from_c_carries_a_b_reexecutes_c_d() {
        let def = linear_dag();
        let (ia, ib, ic) = (
            ActivityExecId::new(),
            ActivityExecId::new(),
            ActivityExecId::new(),
        );
        // 0 started, 1 schedA, 2 compA, 3 schedB, 4 compB, 5 schedC, 6 failC
        let events = vec![
            started(),
            scheduled("a", ia),
            completed(ia),
            scheduled("b", ib),
            completed(ib),
            scheduled("c", ic),
            failed(ic),
        ];
        let plan = resolve_retry_plan(&def, &events, &["c".to_string()]).expect("plan");
        // earliest reexecute schedule = schedC at index 5 -> reset to 4.
        assert_eq!(plan.reset_to_event_id, 4);
        assert_eq!(
            plan.nodes_carried_over,
            vec!["a".to_string(), "b".to_string()]
        );
        assert_eq!(
            plan.nodes_to_re_execute,
            vec!["c".to_string(), "d".to_string()]
        );
    }

    // ---- resolve: fanout (level-granular) --------------------------------

    #[test]
    fn resolve_fanout_retry_from_c_cut_lands_before_failed_node() {
        let def = fanout_dag();
        let (ia, ib, ic, id) = (
            ActivityExecId::new(),
            ActivityExecId::new(),
            ActivityExecId::new(),
            ActivityExecId::new(),
        );
        // Parallel level schedules b, c, d adjacently, then completions arrive.
        // 0 started,1 schedA,2 compA,3 schedB,4 schedC,5 schedD,6 compB,7 compD,8 failC
        let events = vec![
            started(),
            scheduled("a", ia),
            completed(ia),
            scheduled("b", ib),
            scheduled("c", ic),
            scheduled("d", id),
            completed(ib),
            completed(id),
            failed(ic),
        ];
        let plan = resolve_retry_plan(&def, &events, &["c".to_string()]).expect("plan");
        // re-execute set = {c, e}; earliest scheduling = schedC at index 4 -> reset to 3.
        // At event id 3 (schedB) b is still pending, so the management layer's
        // #148 validator will reject this with 409; the resolver itself just
        // reports the cut and the honest enumeration.
        assert_eq!(plan.reset_to_event_id, 3);
        // Carried over = nodes scheduled at idx <= 3 = {a, b}.
        assert_eq!(
            plan.nodes_carried_over,
            vec!["a".to_string(), "b".to_string()]
        );
        // Everything else may re-run.
        assert_eq!(
            plan.nodes_to_re_execute,
            vec!["c".to_string(), "d".to_string(), "e".to_string()]
        );
    }

    #[test]
    fn resolve_fanout_failed_node_first_in_level_cuts_before_whole_level() {
        // Level-granular success: when the failed node is the first scheduled in
        // its parallel level, the cut lands on the clean boundary before the
        // whole level, so the failed node and its same-level siblings re-execute
        // together (the operator's chosen semantics). Upstream `a` is preserved.
        let def = fanout_dag();
        let (ia, ib, ic, id) = (
            ActivityExecId::new(),
            ActivityExecId::new(),
            ActivityExecId::new(),
            ActivityExecId::new(),
        );
        // c is scheduled first in the level, then b and d; b and d complete; c fails.
        // 0 started,1 schedA,2 compA,3 schedC,4 schedB,5 schedD,6 compB,7 compD,8 failC
        let events = vec![
            started(),
            scheduled("a", ia),
            completed(ia),
            scheduled("c", ic),
            scheduled("b", ib),
            scheduled("d", id),
            completed(ib),
            completed(id),
            failed(ic),
        ];
        let plan = resolve_retry_plan(&def, &events, &["c".to_string()]).expect("plan");
        // earliest scheduling among {c,e} = schedC at index 3 -> reset to 2 (compA).
        assert_eq!(plan.reset_to_event_id, 2);
        assert_eq!(plan.nodes_carried_over, vec!["a".to_string()]);
        assert_eq!(
            plan.nodes_to_re_execute,
            vec![
                "b".to_string(),
                "c".to_string(),
                "d".to_string(),
                "e".to_string()
            ]
        );
    }

    // ---- resolve: validation errors --------------------------------------

    #[test]
    fn resolve_empty_from_nodes_is_rejected() {
        let def = linear_dag();
        assert_eq!(
            resolve_retry_plan(&def, &[], &[]),
            Err(DagRetryResolveError::EmptyFromNodes)
        );
    }

    #[test]
    fn resolve_unknown_node_lists_declared() {
        let def = linear_dag();
        let events = vec![started()];
        let err = resolve_retry_plan(&def, &events, &["nope".to_string()]).unwrap_err();
        match err {
            DagRetryResolveError::UnknownNodes { unknown, declared } => {
                assert_eq!(unknown, vec!["nope".to_string()]);
                assert_eq!(
                    declared,
                    vec![
                        "a".to_string(),
                        "b".to_string(),
                        "c".to_string(),
                        "d".to_string()
                    ]
                );
            }
            other => panic!("expected UnknownNodes, got {other:?}"),
        }
    }

    #[test]
    fn resolve_unattempted_node_is_rejected() {
        let def = linear_dag();
        let ia = ActivityExecId::new();
        let events = vec![started(), scheduled("a", ia), completed(ia)];
        // d was never scheduled.
        let err = resolve_retry_plan(&def, &events, &["d".to_string()]).unwrap_err();
        assert_eq!(
            err,
            DagRetryResolveError::NotAttempted {
                nodes: vec!["d".to_string()]
            }
        );
    }

    #[test]
    fn resolve_already_succeeded_node_is_rejected() {
        let def = linear_dag();
        let (ia, ib) = (ActivityExecId::new(), ActivityExecId::new());
        let events = vec![
            started(),
            scheduled("a", ia),
            completed(ia),
            scheduled("b", ib),
            completed(ib),
        ];
        let err = resolve_retry_plan(&def, &events, &["b".to_string()]).unwrap_err();
        assert_eq!(
            err,
            DagRetryResolveError::AlreadySucceeded {
                nodes: vec!["b".to_string()]
            }
        );
    }
}
