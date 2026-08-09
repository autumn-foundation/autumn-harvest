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
//! instead. Detection uses TWO signals — a `saga_compensat*` marker, or an
//! `ActivityScheduled` whose recorded input is a **compensation envelope** for a
//! node that succeeded in the same run — because a DAG unwind at a drained
//! signal frontier records no marker. Both read only the run's own history,
//! never the registered definition, so they survive a compensator being renamed
//! or removed and a later definition reusing its name (see
//! [`ran_compensation_unwind`]). The envelope signal reads a payload-bearing
//! field, so callers must supply INFLATED, codec-decoded history. A run that
//! failed WITHOUT compensators (and every pre-#780 history) triggers neither and
//! stays fully retryable.

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

/// The reserved key issue #780's compensation envelope is built around. The
/// envelope is `{"dag_compensate": <node's activity_name>, "input": …,
/// "output": …}` — written **only** by the DAG unwind, and recorded verbatim in
/// the compensator dispatch's own `ActivityScheduled.input`.
const COMPENSATION_ENVELOPE_KEY: &str = "dag_compensate";

/// Is this `ActivityScheduled.input` an issue #780 compensation envelope?
///
/// Structural, not merely key-presence: the envelope has exactly the three keys
/// `dag_compensate` / `input` / `output`, with a string `dag_compensate`. That
/// makes an accidental collision with a *forward* node's input essentially
/// impossible — a forward input is either the `{conf, dag_task}` wrapper, a raw
/// bound upstream output, or a mapped cell, none of which have this shape.
///
/// The failure direction is safe anyway: a false positive marks a retryable run
/// non-retryable (the operator starts a fresh run), whereas a false negative is
/// the double-spend this guard exists to prevent.
#[must_use]
fn is_compensation_envelope(input: &serde_json::Value) -> bool {
    let Some(object) = input.as_object() else {
        return false;
    };
    object.len() == 3
        && object
            .get(COMPENSATION_ENVELOPE_KEY)
            .is_some_and(serde_json::Value::is_string)
        && object.contains_key("input")
        && object.contains_key("output")
}

/// Names of activities that were scheduled AND recorded an `ActivityCompleted`
/// in this history — i.e. the nodes the issue #780 unwind is allowed to
/// compensate.
///
/// Correlation is by `activity_id`, because `ActivityCompleted` carries no name.
#[must_use]
pub fn succeeded_activity_names(events: &[WorkflowEvent]) -> BTreeSet<&str> {
    let completed: std::collections::HashSet<_> = events
        .iter()
        .filter_map(|event| match event {
            WorkflowEvent::ActivityCompleted { activity_id, .. } => Some(*activity_id),
            _ => None,
        })
        .collect();
    events
        .iter()
        .filter_map(|event| match event {
            WorkflowEvent::ActivityScheduled {
                activity_id, name, ..
            } if completed.contains(activity_id) => Some(name.as_str()),
            _ => None,
        })
        .collect()
}

/// Is this `ActivityScheduled.input` a compensation envelope whose compensated
/// node actually **succeeded in this run**?
///
/// The history-only corroboration for signal 2 — see [`ran_compensation_unwind`]
/// for why it is drawn from history rather than the current definition.
///
/// Doubles as the **forward-dispatch exclusion** used by every name-keyed
/// history reader ([`node_outcome`], and `dag_graph`'s `latest_scheduled`): a
/// dispatch this returns `true` for is a compensator's, never a forward node's,
/// so it must not contribute a node's outcome or timing. See
/// [`is_compensation_dispatch`].
#[must_use]
pub fn compensates_a_succeeded_node(input: &serde_json::Value, succeeded: &BTreeSet<&str>) -> bool {
    is_compensation_envelope(input)
        && input
            .get(COMPENSATION_ENVELOPE_KEY)
            .and_then(serde_json::Value::as_str)
            .is_some_and(|node| succeeded.contains(node))
}

/// Whether the recorded history shows an issue #780 / #801 compensation unwind.
///
/// **Two** signals, because neither alone is sufficient:
///
/// 1. A `saga_compensat*` [`WorkflowEvent::MarkerRecorded`]. The general signal,
///    and the only one available for a non-DAG saga — but NOT guaranteed for a
///    DAG: issue #801's matcher deliberately leaves an unwind uncounted when the
///    history sits at a **drained signal frontier**, so a DAG run that received
///    an unsolicited signal records no marker even though its compensators
///    dispatched (`docs/saga.md`, "a stray signal silences unwind
///    observability").
/// 2. An [`WorkflowEvent::ActivityScheduled`] whose recorded `input` is a
///    **compensation envelope** whose `dag_compensate` names a node that
///    **actually succeeded in this run** ([`compensates_a_succeeded_node`]).
///    This is the load-bearing signal for a marker-less unwind.
///
///    **Caller obligation:** `input` is payload-bearing, so `events` MUST be
///    inflated and codec-decoded (the endpoint uses
///    `store::load_history_inflated`). Left raw, an oversized envelope appears
///    as an `_harvest_offload_envelope` reference (issue #524) — or a codec
///    envelope (issue #608) — and this signal misses. It cannot be recovered
///    here: an offloaded input is indistinguishable from a large ordinary
///    forward input, so treating it as a compensation signal would reject every
///    legitimate retry of a big-payload compensating DAG.
///
/// Signal 2 corroborates against **recorded history, never the definition**.
/// Shape alone is reachable by accident — a mapped cell or an `input_from`
/// binding hands a node the raw upstream output, which is arbitrary user data,
/// and an `input_from_all` alias set could even be named
/// `dag_compensate`/`input`/`output` outright — so shape alone would 409 a
/// retryable run, including one whose DAG declares no compensators at all. The
/// unwind only ever compensates succeeded nodes, so a genuine envelope always
/// satisfies the corroboration.
///
/// Drawing it from history is what makes signal 2 survive every way the current
/// definition can drift from the run that produced the history:
///
/// * the compensator was **renamed**;
/// * the compensator was **removed outright**;
/// * a later definition **reuses** the old compensator's name as a forward node;
/// * an **old** definition's forward node shares a name with a compensator the
///   current definition introduces.
///
/// Those last two are why no name-keyed check against the current definition
/// survives here, in either direction.
/// `DagBuildError::CompensatorNameCollidesWithNode` is a *per-definition-version*
/// guarantee — no compensator shares a node's name in the build that declared it
/// — and nothing constrains names across versions. An earlier revision of this
/// function also accepted a dispatch whose *name* was a currently-declared
/// compensator; that was dropped because it is redundant (every unwind this
/// engine produces writes the envelope, and only succeeded nodes are
/// compensated) while being exactly the cross-version false-positive vector
/// above.
///
/// Residual, documented and safe-direction: a forward dispatch whose user input
/// is exactly the three envelope keys *and* whose `dag_compensate` string
/// happens to name a node that succeeded in the same run is still a false
/// positive. That marks a retryable run non-retryable (start a fresh run) — the
/// opposite direction from the double-spend this guard exists to prevent.
///
/// Pure and cheap: a bounded number of passes over the events.
#[must_use]
fn ran_compensation_unwind(events: &[WorkflowEvent]) -> bool {
    let succeeded = succeeded_activity_names(events);

    events.iter().any(|event| match event {
        WorkflowEvent::MarkerRecorded { name, .. } => name.starts_with(SAGA_UNWIND_MARKER_PREFIX),
        WorkflowEvent::ActivityScheduled { input, .. } => {
            compensates_a_succeeded_node(input, &succeeded)
        }
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

/// Is this recorded `ActivityScheduled` an issue #780 **compensator** dispatch
/// rather than a forward node's dispatch?
///
/// Every name-keyed history reader must exclude these. Issue #780 introduced a
/// new class of `ActivityScheduled` event, and while
/// `DagBuildError::CompensatorNameCollidesWithNode` keeps a compensator
/// distinguishable from a forward node *within one definition version*, that is
/// a per-version guarantee: a later definition may introduce a forward node
/// named after an older definition's compensator. Reading the old compensation
/// dispatch as that node's outcome or timing reports a node that never
/// executed.
///
/// `succeeded` comes from [`succeeded_activity_names`] over the same history —
/// hoist it out of per-node loops. The check is history-only, so unlike a
/// definition-derived one it cannot drift across versions in either direction.
#[must_use]
pub fn is_compensation_dispatch(input: &serde_json::Value, succeeded: &BTreeSet<&str>) -> bool {
    compensates_a_succeeded_node(input, succeeded)
}

/// Determine the outcome of a single node (by activity name) on the source run.
///
/// Compensator dispatches are excluded ([`is_compensation_dispatch`]), so a
/// forward node introduced under a name an older definition used for a
/// compensator reports `NotAttempted` rather than inheriting the compensation's
/// outcome.
#[must_use]
pub fn node_outcome(events: &[WorkflowEvent], node: &str) -> NodeOutcome {
    let succeeded = succeeded_activity_names(events);

    // Find the activity_id of the *latest* ActivityScheduled with this name.
    // If a node was scheduled more than once (e.g. a re-dispatched attempt with
    // a fresh activity_id), the latest attempt is the one whose terminal state
    // is authoritative — the earlier attempts are superseded.
    let mut scheduled_id = None;
    for event in events.iter().rev() {
        if let WorkflowEvent::ActivityScheduled {
            activity_id,
            name,
            input,
            ..
        } = event
            && name == node
            && !is_compensation_dispatch(input, &succeeded)
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
    if ran_compensation_unwind(events) {
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

    /// A compensator dispatch exactly as `unwind_dag_compensations` records it:
    /// the compensator's own activity name, with the three-key envelope naming
    /// the **forward node** being rolled back. Fixtures must use this rather than
    /// [`scheduled`] (whose `input` is `Value::Null`), because the envelope — not
    /// the name — is what the resolver reads.
    fn compensator_dispatch(
        compensator_name: &str,
        compensates_node: &str,
        id: ActivityExecId,
    ) -> WorkflowEvent {
        WorkflowEvent::ActivityScheduled {
            activity_id: id,
            name: compensator_name.to_string(),
            input: serde_json::json!({
                "dag_compensate": compensates_node,
                "input": Value::Null,
                "output": Value::Null,
            }),
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

    /// Issue #780 introduced a *new class* of `ActivityScheduled` event — the
    /// compensator dispatch — that name-keyed history readers had never seen
    /// before. Within one definition version
    /// `CompensatorNameCollidesWithNode` keeps it distinguishable, but that is a
    /// per-version guarantee: a later definition may introduce a **forward**
    /// node named after an older definition's compensator. `node_outcome` would
    /// then report that never-executed node's outcome from the old compensation
    /// dispatch — surfacing in the run-graph view (#690) and in this resolver's
    /// per-node validation.
    ///
    /// The exclusion is history-only (the same corroboration
    /// `ran_compensation_unwind` uses), so it needs no definition and cannot
    /// drift across versions.
    #[test]
    fn node_outcome_ignores_a_compensation_dispatch_named_like_a_forward_node() {
        let (ia, iu) = (ActivityExecId::new(), ActivityExecId::new());
        let events = vec![
            started(),
            scheduled("a", ia),
            completed(ia),
            // The OLD definition's unwind rolled `a` back. In the CURRENT
            // definition `undo_a` is an ordinary forward node that never ran.
            compensator_dispatch("undo_a", "a", iu),
            completed(iu),
        ];
        assert_eq!(
            node_outcome(&events, "undo_a"),
            NodeOutcome::NotAttempted,
            "a compensation dispatch must never be read as a forward node's outcome"
        );
        assert_eq!(
            node_outcome(&events, "a"),
            NodeOutcome::Succeeded,
            "the genuinely-executed forward node is unaffected"
        );
    }

    /// The exclusion must not swallow a forward dispatch whose *user input*
    /// happens to be envelope-shaped — the same false-positive vector the retry
    /// guard corroborates against. `dag_compensate` here names nothing that
    /// succeeded, so the dispatch stays a forward dispatch.
    #[test]
    fn node_outcome_still_reads_a_forward_node_whose_input_mimics_the_envelope() {
        let ia = ActivityExecId::new();
        let events = vec![
            started(),
            WorkflowEvent::ActivityScheduled {
                activity_id: ia,
                name: "a".to_string(),
                input: serde_json::json!({
                    "dag_compensate": "not_a_node_that_ran",
                    "input": Value::Null,
                    "output": Value::Null,
                }),
                queue: "default".to_string(),
            },
            completed(ia),
        ];
        assert_eq!(node_outcome(&events, "a"), NodeOutcome::Succeeded);
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
            // The unwind's dedup marker. The compensator dispatch that follows
            // deliberately carries NO envelope (`scheduled` sets `input: Null`)
            // and `linear_dag` declares no compensators, so signal 2 cannot
            // fire — this isolates signal 1.
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
    /// [`DagRetryResolveError::CompensatedRun`] exists to prevent. The envelope
    /// signal closes it, reading only the run's own history: the compensator's
    /// dispatch carries `{"dag_compensate": "a", …}` and `a` did succeed here.
    #[test]
    fn resolve_rejects_a_marker_less_compensated_run() {
        let def = linear_compensating_dag();
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
            // An unsolicited signal put the unwind at a drained-signal
            // frontier, so NO `saga_compensat*` marker was recorded...
            WorkflowEvent::SignalReceived {
                signal_name: "unsolicited".to_string(),
                payload: Value::Null,
            },
            // ...but the compensator still ran and rolled `a` back.
            compensator_dispatch("undo_a", "a", iu),
            completed(iu),
        ];
        assert_eq!(
            resolve_retry_plan(&def, &events, &["b".to_string()]),
            Err(DagRetryResolveError::CompensatedRun)
        );
    }

    /// The envelope predicate is a cross-crate coupling: it encodes the shape
    /// `autumn-harvest`'s `unwind_dag_compensations` writes. The core side pins
    /// that shape with an exact-value assertion
    /// (`dag_compensation_tests::…the_compensator_envelope…`), so a change there
    /// fails loudly; these pin THIS side's reading of it.
    #[test]
    fn compensation_envelope_predicate_accepts_only_the_real_envelope() {
        // The real thing.
        assert!(is_compensation_envelope(&serde_json::json!({
            "dag_compensate": "charge_payment",
            "input": {"order": 7},
            "output": {"charge_id": "ch_1"},
        })));
        // Null input/output are legitimate envelope values.
        assert!(is_compensation_envelope(&serde_json::json!({
            "dag_compensate": "a",
            "input": Value::Null,
            "output": Value::Null,
        })));

        // Near-misses must all be rejected.
        assert!(
            !is_compensation_envelope(&serde_json::json!({
                "conf": Value::Null, "dag_task": "a"
            })),
            "the ordinary unbound forward wrapper is not a compensation"
        );
        assert!(
            !is_compensation_envelope(&serde_json::json!({
                "dag_compensate": "a", "input": Value::Null
            })),
            "a 2-key subset is not the envelope"
        );
        assert!(
            !is_compensation_envelope(&serde_json::json!({
                "dag_compensate": "a", "input": Value::Null,
                "output": Value::Null, "extra": 1
            })),
            "a 4-key superset is not the envelope"
        );
        assert!(
            !is_compensation_envelope(&serde_json::json!({
                "dag_compensate": 42, "input": Value::Null, "output": Value::Null
            })),
            "`dag_compensate` must be the compensated node's NAME"
        );
        assert!(
            !is_compensation_envelope(&serde_json::json!([1, 2, 3])),
            "a mapped cell array is not the envelope"
        );
        assert!(!is_compensation_envelope(&Value::Null));
    }

    /// POST-PR REVIEW P1 (Codex on #1153): the retry endpoint passes the
    /// **currently registered** `DagDefinition`, not the one that produced the
    /// history. A deployment that renamed (or removed) a compensator after a
    /// marker-less unwind ran would make the name-based signal blind, and the
    /// already-rolled-back run would become retryable again.
    ///
    /// The compensation ENVELOPE is the durable, definition-independent signal:
    /// it is recorded in the dispatch's own `ActivityScheduled.input` and never
    /// consults the registry.
    #[test]
    fn resolve_rejects_a_marker_less_unwind_whose_compensator_was_since_renamed() {
        let def = linear_compensating_dag();
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
            // No `saga_compensat*` marker (drained-signal frontier)...
            WorkflowEvent::SignalReceived {
                signal_name: "unsolicited".to_string(),
                payload: Value::Null,
            },
            // ...and the compensator that actually ran has since been RENAMED,
            // so its recorded name is absent from the current definition. Only
            // the envelope shape identifies it now.
            WorkflowEvent::ActivityScheduled {
                activity_id: iu,
                name: "undo_a_v1_RENAMED".to_string(),
                input: serde_json::json!({
                    "dag_compensate": "a",
                    "input": Value::Null,
                    "output": Value::Null,
                }),
                queue: "default".to_string(),
            },
            completed(iu),
        ];
        assert_eq!(
            resolve_retry_plan(&def, &events, &["b".to_string()]),
            Err(DagRetryResolveError::CompensatedRun),
            "a rolled-back run must stay non-retryable even after the \
             compensator it dispatched was renamed out of the definition"
        );
    }

    /// **This resolver requires INFLATED history** — the caller must resolve
    /// payload offloading (issue #524) before calling it.
    ///
    /// When a compensation envelope exceeds the offload threshold,
    /// `append_events_offloaded` replaces the WHOLE `ActivityScheduled.input`
    /// with an `_harvest_offload_envelope` reference (`input` is in
    /// `PAYLOAD_FIELD_KEYS`), so the structural envelope signal cannot see the
    /// three compensation keys. That is unfixable *here*: an offloaded input is
    /// indistinguishable from a large ordinary forward input, so treating it as
    /// a compensation signal would reject every legitimate retry of a
    /// big-payload compensating DAG.
    ///
    /// This test pins the blindness so the obligation stays with the endpoint,
    /// which loads via `store::load_history_inflated`. Do not "simplify" that
    /// call site back to `store::load_history`.
    #[test]
    fn an_offloaded_compensation_envelope_is_invisible_and_must_be_inflated_by_the_caller() {
        let def = linear_compensating_dag();
        let (ia, ib, iu) = (
            ActivityExecId::new(),
            ActivityExecId::new(),
            ActivityExecId::new(),
        );
        // The exact shape `payload_store::build_offload_envelope` writes.
        let offloaded_input = serde_json::json!({
            "_harvest_offload_envelope": 1,
            "store_id": "s3",
            "key": "sha256:deadbeef",
            "len": 900_000,
            "checksum": "deadbeef",
        });
        assert!(
            !is_compensation_envelope(&offloaded_input),
            "an offload reference is not structurally a compensation envelope"
        );

        let build = |input: Value| {
            vec![
                started(),
                scheduled("a", ia),
                completed(ia),
                scheduled("b", ib),
                failed(ib),
                // Marker-less (drained-signal frontier)...
                WorkflowEvent::SignalReceived {
                    signal_name: "unsolicited".to_string(),
                    payload: Value::Null,
                },
                // ...and the compensator has since been renamed, so only the
                // envelope shape can identify this dispatch.
                WorkflowEvent::ActivityScheduled {
                    activity_id: iu,
                    name: "undo_a_v1_RENAMED".to_string(),
                    input,
                    queue: "default".to_string(),
                },
                completed(iu),
            ]
        };

        // Fed the OFFLOADED form (what plain `store::load_history` returns),
        // every definition-independent signal misses and the rolled-back run
        // looks retryable. This is the fail-open the endpoint must prevent.
        assert!(
            resolve_retry_plan(&def, &build(offloaded_input), &["b".to_string()]).is_ok(),
            "offloaded input hides the envelope from this resolver — the \
             endpoint MUST inflate before calling it"
        );

        // Fed the INFLATED form, the guard fires correctly.
        let inflated = serde_json::json!({
            "dag_compensate": "a",
            "input": Value::Null,
            "output": Value::Null,
        });
        assert_eq!(
            resolve_retry_plan(&def, &build(inflated), &["b".to_string()]),
            Err(DagRetryResolveError::CompensatedRun),
            "with inflated history the rolled-back run is correctly rejected"
        );
    }

    /// A **forward** dispatch is never an unwind, even if the user's own
    /// payload happens to have the envelope's exact three keys.
    ///
    /// A mapped cell or an `input_from` binding hands a node the raw upstream
    /// output, which is arbitrary user data — so `{"dag_compensate": <string>,
    /// "input": …, "output": …}` is reachable by accident. Shape alone would
    /// then 409 a perfectly retryable run, and (worse) would do so for a DAG
    /// that declares no compensators at all and therefore cannot have unwound.
    ///
    /// The corroboration is the dispatch's *name*: every forward dispatch is
    /// named after a declared node, and
    /// `DagBuildError::CompensatorNameCollidesWithNode` guarantees no
    /// compensator can share a node's name.
    #[test]
    fn a_forward_dispatch_mimicking_the_envelope_is_not_an_unwind() {
        // A DAG with NO compensators — an unwind is impossible by construction.
        let def = linear_dag();
        let (ia, ib) = (ActivityExecId::new(), ActivityExecId::new());
        let mimic = serde_json::json!({
            "dag_compensate": "looks_like_a_node",
            "input": 1,
            "output": 2,
        });
        assert!(
            is_compensation_envelope(&mimic),
            "the mimic is envelope-shaped — corroboration, not shape, is what \
             must reject it"
        );
        let events = vec![
            started(),
            // `a` is a real forward node; its input is user data that happens
            // to be envelope-shaped.
            WorkflowEvent::ActivityScheduled {
                activity_id: ia,
                name: "a".to_string(),
                input: mimic,
                queue: "default".to_string(),
            },
            completed(ia),
            scheduled("b", ib),
            failed(ib),
        ];
        let plan = resolve_retry_plan(&def, &events, &["b".to_string()])
            .expect("a non-compensating DAG must stay retryable");
        assert_eq!(plan.nodes_carried_over, vec!["a".to_string()]);
    }

    /// The corroboration must NOT cost the rename-resilience it was added for:
    /// a genuine compensation dispatch is still detected after **every**
    /// compensator has been removed from the definition, because its name is
    /// still absent from the forward node set.
    #[test]
    fn a_genuine_unwind_is_detected_even_after_all_compensators_were_removed() {
        // The current definition declares NO compensators — they were all
        // removed after this run unwound.
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
            // Marker-less (drained-signal frontier).
            WorkflowEvent::SignalReceived {
                signal_name: "unsolicited".to_string(),
                payload: Value::Null,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: iu,
                name: "undo_a".to_string(),
                input: serde_json::json!({
                    "dag_compensate": "a",
                    "input": Value::Null,
                    "output": Value::Null,
                }),
                queue: "default".to_string(),
            },
            completed(iu),
        ];
        assert_eq!(
            resolve_retry_plan(&def, &events, &["b".to_string()]),
            Err(DagRetryResolveError::CompensatedRun),
            "a rolled-back run must stay non-retryable even after every \
             compensator was removed from the definition"
        );
    }

    /// Name reuse in the OTHER direction must not fabricate an unwind.
    ///
    /// The mirror of the envelope case: an OLD definition's forward node can
    /// share a name with a compensator the CURRENT definition introduces. A
    /// name-only check against the current compensator set then reports
    /// `CompensatedRun` for a run that never unwound at all — blocking a
    /// legitimate retry with a 409.
    #[test]
    fn an_old_forward_node_named_like_a_current_compensator_is_not_an_unwind() {
        // Current definition declares `undo_a` as `a`'s compensator...
        let def = linear_compensating_dag();
        let (ia, iu, ib) = (
            ActivityExecId::new(),
            ActivityExecId::new(),
            ActivityExecId::new(),
        );
        let events = vec![
            started(),
            scheduled("a", ia),
            completed(ia),
            // ...but in the OLD definition `undo_a` was an ordinary forward
            // node. No envelope, no marker — this run never unwound.
            scheduled("undo_a", iu),
            completed(iu),
            scheduled("b", ib),
            failed(ib),
        ];
        let plan = resolve_retry_plan(&def, &events, &["b".to_string()])
            .expect("a run that never unwound must stay retryable");
        assert!(
            plan.nodes_carried_over.contains(&"a".to_string()),
            "the succeeded upstream is carried over as usual"
        );
    }

    /// Name **reuse** must not blind the envelope signal.
    ///
    /// `CompensatorNameCollidesWithNode` is a *per-definition-version*
    /// guarantee: it stops a compensator from sharing a node's name in the build
    /// that declared it. Nothing stops a LATER deployment from introducing a
    /// forward node named after a compensator that has since been renamed away.
    /// Corroborating against the *current* node set would then suppress a
    /// genuine envelope — so the corroboration is historical instead.
    #[test]
    fn a_genuine_unwind_is_detected_even_when_a_later_node_reuses_the_compensator_name() {
        // Current definition: `undo_a` is now an ordinary FORWARD node, and no
        // compensator is declared at all (the old one was renamed away).
        let def = {
            let mut builder = DagBuilder::new();
            let na = builder.activity(a);
            let nu = builder.activity(undo_a).upstream(&na);
            let _nc = builder.activity(c).upstream(&nu);
            builder.build().expect("name-reuse dag builds")
        };
        let (ia, ic, iu) = (
            ActivityExecId::new(),
            ActivityExecId::new(),
            ActivityExecId::new(),
        );
        let events = vec![
            started(),
            scheduled("a", ia),
            completed(ia),
            scheduled("c", ic),
            failed(ic),
            // Marker-less (drained-signal frontier).
            WorkflowEvent::SignalReceived {
                signal_name: "unsolicited".to_string(),
                payload: Value::Null,
            },
            // The genuine compensation dispatch from the OLD definition. Its
            // name is now a forward node in the CURRENT definition.
            WorkflowEvent::ActivityScheduled {
                activity_id: iu,
                name: "undo_a".to_string(),
                input: serde_json::json!({
                    "dag_compensate": "a",
                    "input": Value::Null,
                    "output": Value::Null,
                }),
                queue: "default".to_string(),
            },
            completed(iu),
        ];
        assert_eq!(
            resolve_retry_plan(&def, &events, &["c".to_string()]),
            Err(DagRetryResolveError::CompensatedRun),
            "a rolled-back run must stay non-retryable even when a later \
             definition reuses the compensator's name as a forward node"
        );
    }

    /// The envelope signal must not fire on an ordinary forward activity whose
    /// input merely happens to be a JSON object.
    #[test]
    fn resolve_allows_a_run_whose_forward_inputs_are_plain_objects() {
        let def = linear_compensating_dag();
        let (ia, ib) = (ActivityExecId::new(), ActivityExecId::new());
        let events = vec![
            started(),
            WorkflowEvent::ActivityScheduled {
                activity_id: ia,
                name: "a".to_string(),
                // The ordinary unbound-node forward envelope.
                input: serde_json::json!({"conf": Value::Null, "dag_task": "a"}),
                queue: "default".to_string(),
            },
            completed(ia),
            scheduled("b", ib),
            failed(ib),
        ];
        let plan = resolve_retry_plan(&def, &events, &["b".to_string()]).expect("plan");
        assert_eq!(
            plan.nodes_carried_over,
            vec!["a".to_string()],
            "a forward dispatch must never be mistaken for a compensation"
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
