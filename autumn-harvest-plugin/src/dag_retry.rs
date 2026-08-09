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
//!
//! ## v1 interaction: signal/timer gate nodes (issue #746)
//!
//! A signal/timer gate ([`DagBuilder::signal_gate`](autumn_harvest::dag::DagBuilder::signal_gate))
//! has **no activity dispatch**, so it records no `ActivityScheduled` /
//! `ActivityCompleted` events. From this resolver's activity-name-keyed
//! perspective a gate is therefore always `NotAttempted`
//! ([`node_outcome`]), with three consequences the resolver handles safely but
//! imperfectly:
//!
//! * **Retrying a gate node directly is rejected** with
//!   [`DagRetryResolveError::NotAttempted`] (a gate has no activity to re-run;
//!   retry a downstream *activity* instead). This is a deliberately conservative
//!   rejection, not a bug.
//! * **A downstream/crossing retry computes a correct reset point.** The cut is
//!   derived purely from activity schedule events ([`earliest_schedule_index`]),
//!   and a gate contributes none, so it never moves the cut. On replay the gate
//!   re-resolves from carried-over history (its recorded `SignalReceived` /
//!   race-`TimerFired`) when the cut lands after it. A gate in the re-execute
//!   closure appears in `nodes_to_re_execute` (never `nodes_carried_over`, since
//!   it has no schedule event to be "carried") — a benign enumeration quirk that
//!   does not affect the reset point.
//! * **A gate whose signal name equals an activity name rejects the whole DAG.**
//!   Because a gate's node identity is its signal name, a gate named `approval`
//!   alongside an activity `approval` is a genuine name collision that
//!   name-based matching cannot disambiguate, so the resolver returns
//!   [`DagRetryResolveError::AmbiguousNodes`] for the whole DAG (same treatment
//!   as any duplicate activity name). Give gates signal names distinct from
//!   activity names to keep a DAG retryable.
//!
//! ## Interaction: declarative node compensation (issue #780)
//!
//! A DAG whose nodes declare compensators unwinds on terminal failure, undoing
//! every **succeeded** node's side effect. That is exactly the set of nodes this
//! resolver would CARRY OVER, so a retry of a compensated run would resume on
//! rolled-back state. Such a run is therefore rejected outright with
//! [`DagRetryResolveError::CompensatedRun`] (`409`); start a fresh DAG run
//! instead. Detection uses TWO signals — a `saga_compensat*` marker, OR an
//! `ActivityScheduled` naming one of this DAG's declared compensators — because
//! a DAG unwind at a drained signal frontier records no marker (see
//! [`ran_compensation_unwind`]). A run that failed WITHOUT compensators (and
//! every pre-#780 history) triggers neither and stays fully retryable.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

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
    /// The DAG reuses the same activity name across multiple nodes. Because the
    /// retry resolver maps node name -> task -> events purely by name, any
    /// duplicate name makes the reset-point and node enumeration ambiguous, so
    /// retry-from-node is rejected for the whole DAG in v1 (not just when the
    /// duplicated node is named directly).
    AmbiguousNodes {
        /// The activity names that are reused across more than one node.
        nodes: Vec<String>,
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
    /// The source run already executed an issue #780 compensation unwind, so
    /// its succeeded upstream nodes' side effects have been **rolled back**.
    /// Retrying from the failed node would carry those nodes over and resume on
    /// state that no longer exists. Unlike every sibling variant this maps to a
    /// `409 Conflict` — it is a state conflict about the run, not a malformed
    /// node request.
    CompensatedRun,
}

/// Marker-name prefix recorded at the start of a `Saga` unwind (issue #801),
/// mirrored here because the core helper that formats it
/// (`autumn_harvest::replay::saga_compensated_marker_name`) is `pub(crate)`.
///
/// Matching by prefix covers both `saga_compensated:{seq}` and
/// `saga_compensation_failed:{seq}`: either one proves an unwind ran, and a
/// unwind that FAILED leaves the run in an even less retryable state.
const SAGA_UNWIND_MARKER_PREFIX: &str = "saga_compensat";

/// Whether the recorded history shows an issue #780 / #801 compensation unwind.
///
/// Two independent signals, because **neither alone is sufficient**:
///
/// 1. A `saga_compensat*` [`WorkflowEvent::MarkerRecorded`]. This is the
///    general signal and the only one available for a non-DAG saga, but it is
///    NOT guaranteed for a DAG: issue #801's matcher deliberately leaves an
///    unwind uncounted when the history sits at a **drained signal frontier**,
///    so a DAG run that received an unsolicited signal records no marker even
///    though its compensators dispatched (documented in `docs/saga.md`, "a
///    stray signal silences unwind observability").
/// 2. An [`WorkflowEvent::ActivityScheduled`] whose name is a compensator
///    declared by THIS DAG. This closes the marker-less hole and is
///    unambiguous by construction:
///    `DagBuildError::CompensatorNameCollidesWithNode` rejects at build time
///    any compensator that shares a forward node's name, precisely so a
///    compensation dispatch stays distinguishable in recorded history for the
///    name-keyed run graph (#690) and this retry resolver (#366).
///
/// Pure and cheap: one pass over the definition to collect compensator names
/// (usually empty — a DAG with no compensators can never be in this state) and
/// one pass over the events.
#[must_use]
fn ran_compensation_unwind(def: &DagDefinition, events: &[WorkflowEvent]) -> bool {
    let compensators: BTreeSet<&str> = def
        .tasks()
        .iter()
        .filter_map(|task| task.compensate.as_deref())
        .collect();

    events.iter().any(|event| match event {
        WorkflowEvent::MarkerRecorded { name, .. } => name.starts_with(SAGA_UNWIND_MARKER_PREFIX),
        WorkflowEvent::ActivityScheduled { name, .. } => compensators.contains(name.as_str()),
        _ => false,
    })
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

/// Forward adjacency for the DAG: `forward[u]` lists the tasks that declare task
/// `u` as an upstream (`DagTask.upstreams` is reverse edges).
fn forward_adjacency(def: &DagDefinition) -> Vec<Vec<usize>> {
    let tasks = def.tasks();
    let mut forward: Vec<Vec<usize>> = vec![Vec::new(); tasks.len()];
    for (idx, task) in tasks.iter().enumerate() {
        for &up in &task.upstreams {
            if up < forward.len() {
                forward[up].push(idx);
            }
        }
    }
    forward
}

/// Breadth-first downstream reach over task indices, inclusive of the seeds.
fn closure_indices(def: &DagDefinition, seeds: &BTreeSet<usize>) -> BTreeSet<usize> {
    let forward = forward_adjacency(def);
    let mut queue: VecDeque<usize> = seeds.iter().copied().collect();
    let mut seen: BTreeSet<usize> = seeds.clone();
    while let Some(idx) = queue.pop_front() {
        if idx < forward.len() {
            for &down in &forward[idx] {
                if seen.insert(down) {
                    queue.push_back(down);
                }
            }
        }
    }
    seen
}

/// Compute the downstream closure (inclusive of the requested nodes) over the
/// DAG's declared edges, returned as a set of node (activity) names.
#[must_use]
pub fn downstream_closure(def: &DagDefinition, from_nodes: &BTreeSet<String>) -> BTreeSet<String> {
    let tasks = def.tasks();
    let seeds: BTreeSet<usize> = tasks
        .iter()
        .enumerate()
        .filter(|(_, t)| from_nodes.contains(&t.activity_name))
        .map(|(i, _)| i)
        .collect();
    closure_indices(def, &seeds)
        .into_iter()
        .map(|i| tasks[i].activity_name.clone())
        .collect()
}

/// Expand the requested nodes to the re-execute set under **level-granular**
/// semantics (issue #366, operator choice): every node in the same execution
/// level as a requested node, plus the downstream closure of that whole level.
///
/// This lands the reset cut on the clean boundary before the failed node's
/// parallel level, so the failed node and its same-level siblings re-run
/// together — and an operator never has to name an already-succeeded sibling to
/// move the boundary earlier.
#[must_use]
fn level_granular_reexecute_set(
    def: &DagDefinition,
    requested: &BTreeSet<String>,
) -> BTreeSet<String> {
    let tasks = def.tasks();
    let requested_indices: BTreeSet<usize> = tasks
        .iter()
        .enumerate()
        .filter(|(_, t)| requested.contains(&t.activity_name))
        .map(|(i, _)| i)
        .collect();

    // Seed with every task in any execution level that contains a requested node.
    let mut seeds: BTreeSet<usize> = BTreeSet::new();
    for level in def.execution_levels() {
        if level.iter().any(|i| requested_indices.contains(i)) {
            seeds.extend(level.iter().copied());
        }
    }
    // Fall back to the requested indices themselves if (defensively) no level
    // matched, so the set is never empty for a valid request.
    if seeds.is_empty() {
        seeds = requested_indices;
    }

    closure_indices(def, &seeds)
        .into_iter()
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
/// already-succeeded node requests, and
/// [`DagRetryResolveError::CompensatedRun`] when the source run already
/// executed an issue #780 compensation unwind.
pub fn resolve_retry_plan(
    def: &DagDefinition,
    events: &[WorkflowEvent],
    from_nodes: &[String],
) -> Result<DagRetryPlan, DagRetryResolveError> {
    if from_nodes.is_empty() {
        return Err(DagRetryResolveError::EmptyFromNodes);
    }

    // Issue #780 interaction: a run that already unwound its compensations has
    // had its succeeded upstream side effects ROLLED BACK. The level-granular
    // cut below deliberately CARRIES those nodes over, so the fork would resume
    // as if their effects still existed — a double-spend of the compensation.
    // Checked before any node validation so the operator gets the state answer
    // ("this run is not retryable") rather than a node-shaped one.
    if ran_compensation_unwind(def, events) {
        return Err(DagRetryResolveError::CompensatedRun);
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

    // (a.1) Reject DAGs that reuse an activity name across nodes. The retry
    // resolver maps node name -> task -> events purely by activity name, so any
    // duplicate name — whether named directly in `from_nodes` or reached through
    // level/downstream expansion — makes `earliest_schedule_index` and the
    // carried/re-execute enumeration ambiguous (it could match an unrelated
    // earlier occurrence and move the cut too far back). v1 requires unique node
    // names for retry; reject the whole DAG otherwise.
    let duplicated: Vec<String> = {
        let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
        for task in def.tasks() {
            *counts.entry(task.activity_name.as_str()).or_insert(0) += 1;
        }
        counts
            .into_iter()
            .filter(|(_, c)| *c > 1)
            .map(|(name, _)| name.to_string())
            .collect()
    };
    if !duplicated.is_empty() {
        return Err(DagRetryResolveError::AmbiguousNodes { nodes: duplicated });
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

    // Re-execute set = the failed node's full execution level + that level's
    // downstream closure (level-granular semantics). Landing the cut before the
    // whole level means an operator never has to name an already-succeeded
    // sibling to move the boundary earlier.
    let requested: BTreeSet<String> = from_nodes.iter().cloned().collect();
    let reexecute = level_granular_reexecute_set(def, &requested);

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

    fn undo_a() {}

    fn linear_compensating_dag() -> DagDefinition {
        // a -> b -> c -> d, where `a` declares a compensator.
        let mut builder = DagBuilder::new();
        let na = builder.activity(a).compensate(undo_a);
        let nb = builder.activity(b).upstream(&na);
        let nc = builder.activity(c).upstream(&nb);
        let _nd = builder.activity(d).upstream(&nc);
        builder.build().expect("linear compensating dag builds")
    }

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

    fn duplicate_name_dag() -> DagDefinition {
        // a -> b -> b  (activity `b` reused for two distinct nodes)
        let mut builder = DagBuilder::new();
        let na = builder.activity(a);
        let nb = builder.activity(b).upstream(&na);
        let _nb2 = builder.activity(b).upstream(&nb);
        builder.build().expect("duplicate-name dag builds")
    }

    fn duplicate_name_elsewhere_dag() -> DagDefinition {
        // a -> c (the unique node we retry) and a -> b -> b (b reused elsewhere)
        let mut builder = DagBuilder::new();
        let na = builder.activity(a);
        let _nc = builder.activity(c).upstream(&na);
        let nb = builder.activity(b).upstream(&na);
        let _nb2 = builder.activity(b).upstream(&nb);
        builder.build().expect("dag builds")
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
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
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
    fn resolve_fanout_retry_from_c_widens_to_level() {
        let def = fanout_dag();
        let (ia, ib, ic, id) = (
            ActivityExecId::new(),
            ActivityExecId::new(),
            ActivityExecId::new(),
            ActivityExecId::new(),
        );
        // Parallel level schedules b, c, d adjacently (b first), then completions.
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
        // Level-granular: retrying c widens to its whole level {b, c, d} + e, so
        // the cut lands before the level's earliest scheduling (schedB at 3) ->
        // reset to 2 (a clean boundary right after compA). No dead-end even though
        // b and d succeeded first.
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
    fn resolve_ambiguous_duplicate_node_name_is_rejected() {
        let def = duplicate_name_dag();
        let (ia, ib) = (ActivityExecId::new(), ActivityExecId::new());
        let events = vec![
            started(),
            scheduled("a", ia),
            completed(ia),
            scheduled("b", ib),
            failed(ib),
        ];
        let err = resolve_retry_plan(&def, &events, &["b".to_string()]).unwrap_err();
        assert_eq!(
            err,
            DagRetryResolveError::AmbiguousNodes {
                nodes: vec!["b".to_string()]
            }
        );
    }

    #[test]
    fn resolve_rejects_dag_with_duplicate_name_even_for_unique_target() {
        // Codex P2: retrying a *unique* failed node (c) must still be rejected
        // when the DAG reuses an activity name elsewhere (b), because name-based
        // matching could move the cut to the unrelated occurrence.
        let def = duplicate_name_elsewhere_dag();
        let (ia, ic) = (ActivityExecId::new(), ActivityExecId::new());
        let events = vec![
            started(),
            scheduled("a", ia),
            completed(ia),
            scheduled("c", ic),
            failed(ic),
        ];
        let err = resolve_retry_plan(&def, &events, &["c".to_string()]).unwrap_err();
        assert_eq!(
            err,
            DagRetryResolveError::AmbiguousNodes {
                nodes: vec!["b".to_string()]
            }
        );
    }

    #[test]
    fn resolve_empty_from_nodes_is_rejected() {
        let def = linear_dag();
        assert_eq!(
            resolve_retry_plan(&def, &[], &[]),
            Err(DagRetryResolveError::EmptyFromNodes)
        );
    }

    // ---- resolve: compensated runs (issue #780 interaction) ---------------

    fn marker(name: &str) -> WorkflowEvent {
        WorkflowEvent::MarkerRecorded {
            name: name.to_string(),
            details: Value::from(1),
        }
    }

    /// A DAG run that executed an issue #780 compensation unwind carried over
    /// side effects that were ROLLED BACK. Retrying from the failed node would
    /// preserve those (now-undone) upstream nodes and resume on state that no
    /// longer exists — a double-spend of the compensation.
    #[test]
    fn resolve_rejects_a_run_that_already_compensated() {
        let def = linear_dag();
        let (ia, ib, iu) = (
            ActivityExecId::new(),
            ActivityExecId::new(),
            ActivityExecId::new(),
        );
        let events = vec![
            started(),
            scheduled("a", ia),
            completed(ia),
            scheduled("b", ib),
            failed(ib),
            // The unwind: dedup marker + the compensator's own dispatch.
            marker("saga_compensated:1"),
            scheduled("undo_a", iu),
            completed(iu),
        ];
        assert_eq!(
            resolve_retry_plan(&def, &events, &["b".to_string()]),
            Err(DagRetryResolveError::CompensatedRun)
        );
    }

    /// The failure marker alone (a compensation that ITSELF failed) also means
    /// the unwind ran, so the run is equally unsafe to retry.
    #[test]
    fn resolve_rejects_a_run_whose_compensation_failed() {
        let def = linear_dag();
        let (ia, ib) = (ActivityExecId::new(), ActivityExecId::new());
        let events = vec![
            started(),
            scheduled("a", ia),
            completed(ia),
            scheduled("b", ib),
            failed(ib),
            marker("saga_compensated:1"),
            marker("saga_compensation_failed:1"),
        ];
        assert_eq!(
            resolve_retry_plan(&def, &events, &["b".to_string()]),
            Err(DagRetryResolveError::CompensatedRun)
        );
    }

    /// A DAG unwind does not ALWAYS leave a marker: a run that received an
    /// unsolicited signal ends its history at a drained-signal frontier, which
    /// issue #801's matcher deliberately leaves uncounted — no
    /// `saga_compensated:{seq}` marker, no counters — even though the
    /// compensators still dispatch and still replay (documented in
    /// `docs/saga.md`, "a stray signal silences unwind observability").
    ///
    /// Detecting the unwind by marker ALONE therefore leaves such a fully
    /// rolled-back run retryable, which is exactly the double-spend
    /// [`DagRetryResolveError::CompensatedRun`] exists to prevent. The
    /// definition-driven check closes it: a compensator's dispatch is
    /// unambiguous in history because
    /// `DagBuildError::CompensatorNameCollidesWithNode` forbids a compensator
    /// from sharing any forward node's name.
    #[test]
    fn resolve_rejects_a_marker_less_compensated_run() {
        let def = linear_compensating_dag();
        let (ia, ib, iu, isig) = (
            ActivityExecId::new(),
            ActivityExecId::new(),
            ActivityExecId::new(),
            ActivityExecId::new(),
        );
        let _ = isig;
        let events = vec![
            started(),
            scheduled("a", ia),
            completed(ia),
            scheduled("b", ib),
            failed(ib),
            // An unsolicited signal put the unwind at a drained-signal
            // frontier, so NO `saga_compensat*` marker was recorded...
            WorkflowEvent::SignalReceived {
                signal_name: "unsolicited".to_string(),
                payload: Value::Null,
            },
            // ...but the compensator still ran and rolled `a` back.
            scheduled("undo_a", iu),
            completed(iu),
        ];
        assert_eq!(
            resolve_retry_plan(&def, &events, &["b".to_string()]),
            Err(DagRetryResolveError::CompensatedRun)
        );
    }

    /// REGRESSION GUARD: a compensating DAG that failed BEFORE any compensator
    /// dispatched (e.g. the first node itself failed, so nothing succeeded)
    /// left nothing rolled back and stays retryable.
    #[test]
    fn resolve_allows_a_compensating_dag_whose_unwind_never_dispatched() {
        let def = linear_compensating_dag();
        let ia = ActivityExecId::new();
        let events = vec![started(), scheduled("a", ia), failed(ia)];
        let plan = resolve_retry_plan(&def, &events, &["a".to_string()]).expect("plan");
        assert!(
            plan.nodes_carried_over.is_empty(),
            "nothing succeeded, so nothing is carried over"
        );
    }

    /// REGRESSION GUARD: a failed run with NO compensation unwind (the ordinary
    /// issue #366 case, and every pre-#780 history) must still resolve.
    #[test]
    fn resolve_allows_a_failed_run_without_a_compensation_marker() {
        let def = linear_dag();
        let (ia, ib) = (ActivityExecId::new(), ActivityExecId::new());
        let events = vec![
            started(),
            scheduled("a", ia),
            completed(ia),
            scheduled("b", ib),
            failed(ib),
            // An UNRELATED marker must not be mistaken for a compensation.
            marker("dag_skip:3"),
        ];
        let plan = resolve_retry_plan(&def, &events, &["b".to_string()]).expect("plan");
        assert_eq!(plan.reset_to_event_id, 2);
        assert_eq!(plan.nodes_carried_over, vec!["a".to_string()]);
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

    // ---- resolve: signal/timer gate interaction (issue #746) -------------

    /// `a -> validate(c) -> signal_gate("approval") -> b`, gate name distinct
    /// from every activity name.
    fn gate_dag() -> DagDefinition {
        let mut builder = DagBuilder::new();
        let na = builder.activity(a);
        let nc = builder.activity(c).upstream(&na);
        let gate = builder.signal_gate("approval").upstream(&nc);
        let _nb = builder.activity(b).upstream(&gate);
        builder.build().expect("gate dag builds")
    }

    fn signal_received(name: &str) -> WorkflowEvent {
        WorkflowEvent::SignalReceived {
            signal_name: name.to_string(),
            payload: Value::Null,
        }
    }

    #[test]
    fn resolve_gate_node_directly_is_rejected_as_not_attempted() {
        // A gate records no activity events, so retrying it directly is rejected
        // as NotAttempted (documented v1: retry a downstream activity instead).
        let def = gate_dag();
        let (ia, ic) = (ActivityExecId::new(), ActivityExecId::new());
        let events = vec![
            started(),
            scheduled("a", ia),
            completed(ia),
            scheduled("c", ic),
            completed(ic),
            signal_received("approval"),
        ];
        let err = resolve_retry_plan(&def, &events, &["approval".to_string()]).unwrap_err();
        assert_eq!(
            err,
            DagRetryResolveError::NotAttempted {
                nodes: vec!["approval".to_string()]
            }
        );
    }

    #[test]
    fn resolve_upstream_retry_crossing_a_gate_computes_cut_from_activity_schedules() {
        // Retry from `c`, whose downstream closure crosses the gate ("approval")
        // and reaches `b`. `c` failed, so the gate/`b` never ran. The reset point
        // is derived purely from activity schedules — the gate contributes no
        // schedule event and does not move the cut — and the gate appears in
        // nodes_to_re_execute (never carried_over), a benign enumeration quirk.
        let def = gate_dag();
        let (ia, ic) = (ActivityExecId::new(), ActivityExecId::new());
        // 0 started, 1 schedA, 2 compA, 3 schedC, 4 failC (gate & b never reached)
        let events = vec![
            started(),
            scheduled("a", ia),
            completed(ia),
            scheduled("c", ic),
            failed(ic),
        ];
        let plan = resolve_retry_plan(&def, &events, &["c".to_string()]).expect("plan");
        // earliest reexecute schedule among {c, approval, b} = schedC at index 3
        // (the gate and b have no schedule) -> reset to 2 (compA).
        assert_eq!(plan.reset_to_event_id, 2);
        assert_eq!(plan.nodes_carried_over, vec!["a".to_string()]);
        // The gate node "approval" is in the re-execute closure of `c`.
        assert!(
            plan.nodes_to_re_execute.contains(&"approval".to_string()),
            "gate crossed by the retry closure must be listed for re-execution: {:?}",
            plan.nodes_to_re_execute
        );
        assert!(plan.nodes_to_re_execute.contains(&"c".to_string()));
        assert!(plan.nodes_to_re_execute.contains(&"b".to_string()));
    }

    #[test]
    fn resolve_rejects_gate_signal_name_colliding_with_an_activity_name() {
        // A gate whose signal name equals an activity name is a genuine node-name
        // collision the resolver cannot disambiguate → AmbiguousNodes for the
        // whole DAG, even when retrying an unrelated node.
        let mut builder = DagBuilder::new();
        let na = builder.activity(a);
        // Gate signal "a" collides with the activity `a`'s node name "a".
        let _gate = builder.signal_gate("a").upstream(&na);
        let _nc = builder.activity(c).upstream(&na);
        let def = builder.build().expect("colliding-name dag builds");

        let (ia, ic) = (ActivityExecId::new(), ActivityExecId::new());
        let events = vec![
            started(),
            scheduled("a", ia),
            completed(ia),
            scheduled("c", ic),
            failed(ic),
        ];
        let err = resolve_retry_plan(&def, &events, &["c".to_string()]).unwrap_err();
        assert_eq!(
            err,
            DagRetryResolveError::AmbiguousNodes {
                nodes: vec!["a".to_string()]
            }
        );
    }
}
