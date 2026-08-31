//! DAG definition primitives for Harvest.
//!
//! DAGs are compiled in memory into immutable execution metadata. Runtime
//! scheduling can consume the resulting [`DagDefinition`] without rebuilding
//! edges or dependency levels.

use std::any::type_name_of_val;
use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

use crate::policy::{MapFailurePolicy, RetryPolicy, TaskStatus, TriggerRule};

/// A data-dependent node condition: a predicate over the deserialized outputs
/// of upstream nodes, evaluated at dispatch time.
///
/// Upstream outputs are passed in upstream-declaration order.  Nodes whose
/// upstream failed or was itself skipped contribute [`Value::Null`].
///
/// When the predicate returns `false` the node is skipped
/// (`DagDispatchDecision::SkipByCondition`) without ever dispatching the
/// activity.  The skip is recorded as a [`MarkerRecorded`] event so replay
/// always selects the same branch.
///
/// # Example
///
/// ```rust
/// use autumn_harvest::DagCondition;
///
/// let high_risk = DagCondition::new(|ups| {
///     ups[0]["fraud_score"].as_f64().is_some_and(|s| s > 0.8)
/// });
/// ```
#[allow(clippy::type_complexity)]
#[derive(Clone)]
pub struct DagCondition(Arc<dyn Fn(&[Value]) -> bool + Send + Sync>);

impl DagCondition {
    /// Create a new condition from a predicate closure.
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(&[Value]) -> bool + Send + Sync + 'static,
    {
        Self(Arc::new(f))
    }

    /// Evaluate the predicate against the given upstream outputs.
    #[must_use]
    pub fn evaluate(&self, upstream_outputs: &[Value]) -> bool {
        (self.0)(upstream_outputs)
    }
}

impl fmt::Debug for DagCondition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DagCondition(<fn>)")
    }
}

/// What a timed-out signal/timer gate does when its deadline fires before the
/// awaited signal arrives (issue #746).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateTimeoutAction {
    /// Fail the DAG run when the deadline fires first (the gate node's status
    /// becomes [`TaskStatus::Failed`]).
    FailRun,
    /// Continue past the gate when the deadline fires first: the gate succeeds
    /// with a `Value::Null` output so downstream nodes proceed and can branch
    /// on the null-vs-payload distinction via a `.condition(...)` / trigger
    /// rule.
    Continue,
}

/// A declarative signal/timer gate node (issue #746).
///
/// A gate has **no activity dispatch**: reaching it makes the unified DAG walk
/// wait on a named signal (optionally bounded by a timer). It lowers onto the
/// existing #476 `wait_for_signal` / `wait_for_signal_timeout` primitives, so
/// **no new `WorkflowEvent` variant and no migration** are introduced — a gate
/// composes `TimerStarted`/`TimerFired`/`SignalReceived`.
///
/// The received signal payload becomes the gate node's output, addressable by
/// downstream nodes (including a `.map_activity(...).over(&gate)` fan-out when
/// the payload is a JSON array).
///
/// # Retry-from-node interaction (issue #366)
///
/// A gate records no activity events, so the `#[366]` retry resolver treats it
/// as `NotAttempted`: retrying a gate node *directly* is rejected, but a
/// downstream/crossing retry computes a correct reset point (a gate never moves
/// the cut) and the gate re-resolves from carried-over history. Give a gate a
/// signal name **distinct from every activity name** — a collision makes the
/// whole DAG un-retryable (ambiguous node names).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DagSignalGate {
    /// The name of the signal the gate waits on. This is also the gate node's
    /// identity (its `activity_name`).
    pub signal_name: String,
    /// Optional deadline. `None` waits indefinitely for the signal.
    pub timeout: Option<Duration>,
    /// What to do when the deadline fires before the signal. Ignored when
    /// `timeout` is `None`.
    pub on_timeout: GateTimeoutAction,
}

/// The dispatch decision for a single DAG task, combining trigger-rule and
/// condition-predicate checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DagDispatchDecision {
    /// The task should be dispatched.
    Run,
    /// The task is skipped because its trigger rule evaluated to `false` over
    /// upstream statuses.  No marker is recorded (replay-compat with pre-#482
    /// histories).
    SkipByTriggerRule,
    /// The task is skipped because its condition predicate evaluated to
    /// `false` over upstream outputs.  A `dag_skip:` marker is recorded in
    /// event history to make the branch decision deterministic on replay.
    SkipByCondition,
}

/// How a DAG node's activity input is bound to upstream node outputs
/// (issue #702).
///
/// When a node has an `input_from` binding, its activity receives the raw
/// upstream output(s) directly instead of the trigger-input + `dag_task`
/// wrapper the unbound path uses. Set via [`DagTaskRef::input_from`],
/// [`DagTaskRef::input_from_all`], or [`DagTaskRef::input_from_aliased`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DagInputBinding {
    /// Single upstream: the node's input is that upstream's recorded output,
    /// verbatim.
    Single(usize),
    /// Multiple upstreams merged into a JSON object, one key per source.
    Merged(Vec<DagMergeSource>),
}

/// A single keyed source in a [`DagInputBinding::Merged`] binding (issue #702).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DagMergeSource {
    /// The JSON object key this source's output is inserted under — either an
    /// explicit alias (from [`DagTaskRef::input_from_aliased`]) or the
    /// upstream's activity name (from [`DagTaskRef::input_from_all`]).
    pub key: String,
    /// The global task index of the upstream node whose output supplies the
    /// value.
    pub upstream_index: usize,
}

#[derive(Clone)]
struct PendingDagTask {
    activity_name: String,
    upstreams: Vec<usize>,
    trigger_rule: TriggerRule,
    retry_policy: Option<RetryPolicy>,
    start_to_close: Option<Duration>,
    queue: Option<String>,
    map_upstream: Option<usize>,
    map_failure_policy: MapFailurePolicy,
    condition: Option<DagCondition>,
    signal: Option<DagSignalGate>,
    input_from: Option<DagInputBinding>,
    compensate: Option<String>,
}

impl fmt::Debug for PendingDagTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PendingDagTask")
            .field("activity_name", &self.activity_name)
            .field("upstreams", &self.upstreams)
            .field("trigger_rule", &self.trigger_rule)
            .field("retry_policy", &self.retry_policy)
            .field("start_to_close", &self.start_to_close)
            .field("queue", &self.queue)
            .field("map_upstream", &self.map_upstream)
            .field("map_failure_policy", &self.map_failure_policy)
            .field("condition", &self.condition)
            .field("signal", &self.signal)
            .field("input_from", &self.input_from)
            .field("compensate", &self.compensate)
            .finish()
    }
}

/// Immutable task definition produced by [`DagBuilder::build`].
#[derive(Debug, Clone)]
pub struct DagTask {
    /// The name of the activity.
    pub activity_name: String,
    /// Indices of upstream tasks that must complete before this task.
    pub upstreams: Vec<usize>,
    /// The trigger rule for this task.
    pub trigger_rule: TriggerRule,
    /// The retry policy for this task.
    pub retry_policy: Option<RetryPolicy>,
    /// The start-to-close timeout for this task.
    pub start_to_close: Option<Duration>,
    /// The optional specific queue to schedule this task on.
    pub queue: Option<String>,
    /// The index of the upstream task mapped over (if any).
    pub map_upstream: Option<usize>,
    /// Failure policy for mapped tasks.
    pub map_failure_policy: MapFailurePolicy,
    /// Optional data-dependent condition predicate.  When `Some`, the
    /// predicate is evaluated against upstream outputs after the trigger rule
    /// passes; `false` → `DagDispatchDecision::SkipByCondition`.
    pub condition: Option<DagCondition>,
    /// Optional signal/timer gate (issue #746). When `Some`, this node has no
    /// activity dispatch: reaching it waits on the named signal (optionally
    /// bounded by a timer). A gate always occupies its own singleton execution
    /// level so its `WaitForSignal` suspension is never batched with a level's
    /// activity `ScheduleActivity` dispatches.
    pub signal: Option<DagSignalGate>,
    /// Optional input binding (issue #702). When `Some`, this node's activity
    /// input is drawn directly from upstream node output(s) — the raw output
    /// for [`DagInputBinding::Single`], or a keyed JSON object for
    /// [`DagInputBinding::Merged`] — instead of the trigger-input + `dag_task`
    /// wrapper the unbound path uses.
    pub input_from: Option<DagInputBinding>,
    /// Optional compensator activity name (issue #780).
    ///
    /// Opt-in per node, and **unified-dag-execution only** (a classic DAG has no
    /// unwind step, so a compensator there is rejected at
    /// `HarvestBuilder::try_build`). When the DAG reaches a terminal failure
    /// (`Err("one or more DAG tasks failed")`), the engine dispatches this
    /// activity for each node that **completed successfully**, in **reverse
    /// topological order** (LIFO over the levels-forward / ascending-index push
    /// order) through the existing [`Saga`](crate::saga::Saga) unwind, using the
    /// ordinary activity lowering on the compensated node's own queue.
    ///
    /// The compensator receives the fixed envelope
    /// `{"dag_compensate": <node>, "input": <the node's resolved forward
    /// input>, "output": <the node's recorded output>}`.
    ///
    /// Set via [`DagTaskRef::compensate`] (typed, typo-proof) or
    /// [`DagTaskRef::compensate_named`] (explicit string).
    pub compensate: Option<String>,
}

impl DagTask {
    /// Compute the dispatch decision for this task.
    ///
    /// `statuses` and `outputs` are indexed by global task index.
    #[must_use]
    pub fn dispatch_decision(
        &self,
        statuses: &[TaskStatus],
        outputs: &[Value],
    ) -> DagDispatchDecision {
        // Collect upstream statuses for the trigger rule.
        let upstream_statuses: Vec<TaskStatus> = self
            .upstreams
            .iter()
            .map(|&i| statuses.get(i).copied().unwrap_or(TaskStatus::Skipped))
            .collect();
        if !self.trigger_rule.should_run(&upstream_statuses) {
            return DagDispatchDecision::SkipByTriggerRule;
        }
        // Trigger rule passed — evaluate the condition if present.
        if let Some(cond) = &self.condition {
            let upstream_outputs: Vec<Value> = self
                .upstreams
                .iter()
                .map(|&i| outputs.get(i).cloned().unwrap_or(Value::Null))
                .collect();
            if !cond.evaluate(&upstream_outputs) {
                return DagDispatchDecision::SkipByCondition;
            }
        }
        DagDispatchDecision::Run
    }
}

impl From<PendingDagTask> for DagTask {
    fn from(task: PendingDagTask) -> Self {
        Self {
            activity_name: task.activity_name,
            upstreams: task.upstreams,
            trigger_rule: task.trigger_rule,
            retry_policy: task.retry_policy,
            start_to_close: task.start_to_close,
            queue: task.queue,
            map_upstream: task.map_upstream,
            map_failure_policy: task.map_failure_policy,
            condition: task.condition,
            signal: task.signal,
            input_from: task.input_from,
            compensate: task.compensate,
        }
    }
}

/// Fully compiled DAG metadata: task definitions plus execution levels.
#[derive(Debug, Clone)]
pub struct DagDefinition {
    tasks: Vec<DagTask>,
    execution_levels: Vec<Vec<usize>>,
}

impl DagDefinition {
    /// Returns the linearised list of all tasks in the DAG.
    #[must_use]
    pub fn tasks(&self) -> &[DagTask] {
        &self.tasks
    }

    /// Returns the DAG execution levels, where each level contains indices
    /// of tasks that can be executed concurrently.
    #[must_use]
    pub fn execution_levels(&self) -> &[Vec<usize>] {
        &self.execution_levels
    }
}

/// Error returned when a DAG cannot be compiled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DagBuildError {
    /// The DAG contains a cyclic dependency, which prevents execution.
    CycleDetected,
    /// A node declares both an `input_from` binding and a `map_upstream`
    /// (issue #702) — these are contradictory input sources.
    ConflictingInputBinding {
        /// The activity name of the offending node.
        task: String,
    },
    /// A [`DagInputBinding::Merged`] binding contains the same key twice
    /// (issue #702) — either two upstreams sharing an activity name via
    /// `input_from_all`, or a repeated alias via `input_from_aliased`.
    DuplicateInputBindingKey {
        /// The activity name of the offending node.
        task: String,
        /// The duplicated merge key.
        key: String,
    },
    /// A binding references an `upstream_index` that is not one of the node's
    /// upstreams (issue #702) — unreachable via the public API (bindings
    /// auto-add the edge), validated defensively.
    InputBindingNotAnUpstream {
        /// The activity name of the offending node.
        task: String,
    },
    /// A [`DagInputBinding::Merged`] binding has no upstream sources
    /// (issue #702) — an empty `input_from_all(&[])` / `input_from_aliased(&[])`
    /// declares a binding that would deliver an empty JSON object, which is
    /// almost certainly a mistake.
    EmptyInputBinding {
        /// The activity name of the offending node.
        task: String,
    },
    /// A signal-gate node declares an `input_from*` binding (issue #702). A gate
    /// dispatches no activity, so the binding *value* is ignored — but the
    /// binding also auto-adds a dependency edge, which would silently make the
    /// gate wait for that upstream before its signal wait. Unlike the inert
    /// activity-only setters (`.queue()`, `.retry()`, `.start_to_close()`), a
    /// binding has a structural effect, so it is rejected rather than swallowed.
    /// Use [`DagTaskRef::upstream`] to add a gate dependency deliberately.
    InputBindingOnGate {
        /// The signal name (identity) of the offending gate node.
        task: String,
    },
    /// A signal-gate node declares a compensator (issue #780). A gate
    /// dispatches no activity, so it performs no side effect for a compensator
    /// to undo; declaring one is almost certainly a mistake and is rejected
    /// rather than silently ignored.
    CompensateOnGate {
        /// The signal name (identity) of the offending gate node.
        task: String,
    },
    /// A node declares an empty (or whitespace-only) compensator name
    /// (issue #780). The unwind dispatches compensators **by name**, so an
    /// empty name would schedule a nameless activity at exactly the moment the
    /// state is already dangling; reject it at build time instead.
    EmptyCompensator {
        /// The activity name of the offending node.
        task: String,
    },
    /// A compensator name collides with a **forward node's** identity
    /// (issue #780) — another node's activity name, the declaring node's own
    /// name, or a signal gate's signal name.
    ///
    /// The unwind dispatches compensators through the ordinary activity
    /// lowering, so a collision makes the compensation indistinguishable from
    /// the forward node in recorded history — corrupting the name-keyed
    /// classification the DAG run-graph (issue #690) and retry-from-node
    /// (issue #366) surfaces depend on.
    CompensatorNameCollidesWithNode {
        /// The activity name of the node declaring the compensator.
        task: String,
        /// The colliding compensator name.
        compensator: String,
    },
}

impl fmt::Display for DagBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CycleDetected => f.write_str("dag contains a dependency cycle"),
            Self::ConflictingInputBinding { task } => write!(
                f,
                "dag task '{task}' declares both an input binding and a mapped upstream (input_from and map_activity are mutually exclusive)"
            ),
            Self::DuplicateInputBindingKey { task, key } => write!(
                f,
                "dag task '{task}' has a duplicate input-binding key '{key}' (merge keys must be unique)"
            ),
            Self::InputBindingNotAnUpstream { task } => write!(
                f,
                "dag task '{task}' has an input binding referencing a node that is not an upstream"
            ),
            Self::EmptyInputBinding { task } => write!(
                f,
                "dag task '{task}' has an input binding with no upstream sources"
            ),
            Self::InputBindingOnGate { task } => write!(
                f,
                "dag task '{task}' is a signal gate and cannot have an input binding; use `.upstream()` to add a dependency"
            ),
            Self::CompensateOnGate { task } => write!(
                f,
                "dag task '{task}' is a signal gate and cannot have a compensator; a gate dispatches no activity, so it has no side effect to undo"
            ),
            Self::EmptyCompensator { task } => write!(
                f,
                "dag task '{task}' declares an empty compensator name; name the activity that undoes this node"
            ),
            Self::CompensatorNameCollidesWithNode { task, compensator } => write!(
                f,
                "dag task '{task}' declares compensator '{compensator}', which collides with a forward node's name; a compensator dispatched under a node's name would corrupt the name-keyed history classification used by the DAG run graph and retry-from-node"
            ),
        }
    }
}

impl std::error::Error for DagBuildError {}

type SharedTasks = Rc<RefCell<Vec<PendingDagTask>>>;

/// Opaque handle to a task being defined inside a [`DagBuilder`].
#[derive(Debug, Clone)]
pub struct DagTaskRef {
    tasks: SharedTasks,
    index: usize,
}

impl DagTaskRef {
    /// The numerical index of this task within the [`DagBuilder`].
    #[must_use]
    pub const fn index(&self) -> usize {
        self.index
    }

    /// Declare that this task depends on `upstream`.
    ///
    /// # Panics
    ///
    /// Panics if `self` and `upstream` were created by different
    /// [`DagBuilder`] instances.
    #[must_use]
    pub fn upstream(self, upstream: &Self) -> Self {
        assert!(
            Rc::ptr_eq(&self.tasks, &upstream.tasks),
            "cannot connect tasks from different DagBuilder instances"
        );
        self.mutate(|task| {
            if !task.upstreams.contains(&upstream.index) {
                task.upstreams.push(upstream.index);
            }
        })
    }

    /// Set a custom trigger rule for this task.
    #[must_use]
    pub fn trigger_rule(self, trigger_rule: TriggerRule) -> Self {
        self.mutate(|task| task.trigger_rule = trigger_rule)
    }

    /// Attach a specific retry policy to this task.
    #[must_use]
    pub fn retry(self, retry_policy: RetryPolicy) -> Self {
        self.mutate(|task| task.retry_policy = Some(retry_policy))
    }

    /// Set a maximum start-to-close timeout duration for this task.
    #[must_use]
    pub fn start_to_close(self, timeout: Duration) -> Self {
        self.mutate(|task| task.start_to_close = Some(timeout))
    }

    /// Assign this task to a specific task queue.
    #[must_use]
    pub fn queue(self, queue: impl Into<String>) -> Self {
        self.mutate(|task| task.queue = Some(queue.into()))
    }

    /// Set the mapped failure policy. Only applies to mapped tasks.
    #[must_use]
    pub fn map_failure_policy(self, policy: MapFailurePolicy) -> Self {
        self.mutate(|task| task.map_failure_policy = policy)
    }

    /// Attach a data-dependent condition predicate to this task.
    ///
    /// The predicate receives upstream outputs in upstream-declaration order.
    /// Upstream nodes that failed or were skipped contribute [`Value::Null`].
    /// When the predicate returns `false`, the node is skipped
    /// ([`DagDispatchDecision::SkipByCondition`]) and a `dag_skip:` marker is
    /// recorded in event history so replay selects the identical branch.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use autumn_harvest::prelude::*;
    /// # use autumn_harvest::DagCondition;
    /// # fn score_payment() {}
    /// # fn manual_review() {}
    /// # fn auto_approve() {}
    /// fn fraud_routing(dag: &mut DagBuilder) {
    ///     let score = dag.activity(score_payment);
    ///     let _review = dag
    ///         .activity(manual_review)
    ///         .upstream(&score)
    ///         .condition(|ups| ups[0]["fraud_score"].as_f64().is_some_and(|s| s > 0.8));
    ///     let _auto = dag
    ///         .activity(auto_approve)
    ///         .upstream(&score)
    ///         .condition(|ups| ups[0]["fraud_score"].as_f64().is_some_and(|s| s <= 0.8));
    /// }
    /// ```
    #[must_use]
    pub fn condition<F>(self, predicate: F) -> Self
    where
        F: Fn(&[Value]) -> bool + Send + Sync + 'static,
    {
        self.mutate(|task| task.condition = Some(DagCondition::new(predicate)))
    }

    /// Bind this task's activity input to `upstream`'s output, verbatim
    /// (issue #702).
    ///
    /// The dependency edge is added automatically (an explicit `.upstream()`
    /// is not required). The bound activity receives the raw upstream output
    /// instead of the trigger-input + `dag_task` wrapper the unbound path uses.
    ///
    /// A skipped or failed upstream contributes [`Value::Null`] (reachable when
    /// a non-default trigger rule like [`TriggerRule::AllDone`] lets the node
    /// run past a skip/failure).
    ///
    /// Calling a binding method adds a dependency edge; edges accumulate across
    /// calls, but the binding itself is last-wins — a later `input_from*`
    /// replaces the input source while keeping every prior edge as an
    /// ordering-only dependency. The bound upstream therefore also appears in
    /// the node's [`condition`](Self::condition) `ups` slice, in builder
    /// call order.
    ///
    /// # Panics
    ///
    /// Panics if `self` and `upstream` were created by different
    /// [`DagBuilder`] instances.
    #[must_use]
    pub fn input_from(self, upstream: &Self) -> Self {
        assert!(
            Rc::ptr_eq(&self.tasks, &upstream.tasks),
            "cannot connect tasks from different DagBuilder instances"
        );
        let up_index = upstream.index;
        self.mutate(|task| {
            if !task.upstreams.contains(&up_index) {
                task.upstreams.push(up_index);
            }
            task.input_from = Some(DagInputBinding::Single(up_index));
        })
    }

    /// Bind this task's activity input to a JSON object merging every
    /// `upstream`'s output, keyed by each upstream's activity name (issue #702).
    ///
    /// Dependency edges are added automatically. Keys are the upstreams'
    /// activity names in the given order; two upstreams sharing an activity
    /// name is a [`DagBuildError::DuplicateInputBindingKey`] at build time (use
    /// [`input_from_aliased`](Self::input_from_aliased) to disambiguate). An
    /// empty `upstreams` slice is a [`DagBuildError::EmptyInputBinding`].
    ///
    /// A skipped or failed upstream contributes [`Value::Null`] for its key
    /// (reachable when a non-default trigger rule like [`TriggerRule::AllDone`]
    /// lets the node run past a skip/failure) — never a missing key.
    ///
    /// Binding edges accumulate across calls, but the binding itself is
    /// last-wins — a later `input_from*` replaces the input source while
    /// keeping every prior edge as an ordering-only dependency. Every bound
    /// upstream therefore also appears in the node's
    /// [`condition`](Self::condition) `ups` slice, in builder call order.
    ///
    /// # Panics
    ///
    /// Panics if `self` and any `upstream` were created by different
    /// [`DagBuilder`] instances.
    #[must_use]
    pub fn input_from_all(self, upstreams: &[&Self]) -> Self {
        for up in upstreams {
            assert!(
                Rc::ptr_eq(&self.tasks, &up.tasks),
                "cannot connect tasks from different DagBuilder instances"
            );
        }
        let sources: Vec<DagMergeSource> = {
            let tasks = self.tasks.borrow();
            upstreams
                .iter()
                .map(|up| DagMergeSource {
                    key: tasks[up.index].activity_name.clone(),
                    upstream_index: up.index,
                })
                .collect()
        };
        self.mutate(move |task| {
            for up in upstreams {
                if !task.upstreams.contains(&up.index) {
                    task.upstreams.push(up.index);
                }
            }
            task.input_from = Some(DagInputBinding::Merged(sources));
        })
    }

    /// Bind this task's activity input to a JSON object merging every
    /// upstream's output, keyed by an explicit alias (issue #702).
    ///
    /// Dependency edges are added automatically. A duplicate alias is a
    /// [`DagBuildError::DuplicateInputBindingKey`] at build time; an empty
    /// `bindings` slice is a [`DagBuildError::EmptyInputBinding`].
    ///
    /// A skipped or failed upstream contributes [`Value::Null`] for its alias
    /// (reachable when a non-default trigger rule like [`TriggerRule::AllDone`]
    /// lets the node run past a skip/failure) — never a missing key.
    ///
    /// Binding edges accumulate across calls, but the binding itself is
    /// last-wins — a later `input_from*` replaces the input source while
    /// keeping every prior edge as an ordering-only dependency. Every bound
    /// upstream therefore also appears in the node's
    /// [`condition`](Self::condition) `ups` slice, in builder call order.
    ///
    /// # Panics
    ///
    /// Panics if `self` and any bound upstream were created by different
    /// [`DagBuilder`] instances.
    #[must_use]
    pub fn input_from_aliased(self, bindings: &[(&str, &Self)]) -> Self {
        for (_, up) in bindings {
            assert!(
                Rc::ptr_eq(&self.tasks, &up.tasks),
                "cannot connect tasks from different DagBuilder instances"
            );
        }
        let sources: Vec<DagMergeSource> = bindings
            .iter()
            .map(|(key, up)| DagMergeSource {
                key: (*key).to_owned(),
                upstream_index: up.index,
            })
            .collect();
        self.mutate(move |task| {
            for (_, up) in bindings {
                if !task.upstreams.contains(&up.index) {
                    task.upstreams.push(up.index);
                }
            }
            task.input_from = Some(DagInputBinding::Merged(sources));
        })
    }

    /// Declare the activity that **undoes** this node when the DAG fails
    /// (issue #780), derived from the activity fn item exactly like
    /// [`DagBuilder::activity`] — so a typo is a compile error, not a
    /// mid-unwind dispatch failure.
    ///
    /// If the DAG reaches a terminal failure and this node **succeeded**, the
    /// compensator is dispatched on this node's queue with the envelope
    /// `{"dag_compensate": <node>, "input": <resolved forward input>,
    /// "output": <recorded output>}`. Compensators run in reverse topological
    /// (LIFO) order through the [`Saga`](crate::saga::Saga) unwind. A node that
    /// was skipped, never reached, or itself failed is never compensated.
    ///
    /// Last call wins: repeating `compensate*` replaces the previous
    /// declaration.
    ///
    /// # Build errors
    ///
    /// * [`DagBuildError::CompensateOnGate`] — a signal gate dispatches no
    ///   activity, so there is nothing for a compensator to undo.
    /// * [`DagBuildError::CompensatorNameCollidesWithNode`] — the compensator
    ///   name matches a forward node's identity.
    /// * [`DagBuildError::EmptyCompensator`] — the name is empty/whitespace.
    ///
    /// ```rust
    /// use autumn_harvest::dag::DagBuilder;
    ///
    /// fn reserve_inventory() {}
    /// fn release_inventory() {}
    /// fn charge_payment() {}
    ///
    /// let mut dag = DagBuilder::new();
    /// let reserve = dag.activity(reserve_inventory).compensate(release_inventory);
    /// let _charge = dag.activity(charge_payment).upstream(&reserve);
    /// ```
    #[must_use]
    pub fn compensate<F>(self, activity: F) -> Self
    where
        F: Copy + 'static,
    {
        let name = short_activity_name(type_name_of_val(&activity));
        self.mutate(|task| task.compensate = Some(name))
    }

    /// Declare the compensator by **name** (issue #780) — the escape hatch for
    /// a compensator whose fn item is not in scope (a remote/polyglot worker's
    /// activity, or a name computed by a macro).
    ///
    /// Prefer [`compensate`](Self::compensate), which derives the name from the
    /// fn item and is therefore typo-proof. Semantics are otherwise identical;
    /// last call wins.
    ///
    /// The name is **trimmed** on insert, so surrounding whitespace can never
    /// produce a dispatch name that differs from the registered activity — the
    /// same normalisation [`DagBuildError::EmptyCompensator`] already applies
    /// when deciding whether a name is empty.
    ///
    /// The name must still be **registered with the builder**: the plugin's
    /// startup preflight fails the boot for a DAG compensator that resolves to
    /// no registered activity, exactly as it does for a forward node. This is a
    /// name-based dispatch escape hatch (e.g. an activity routed to a different
    /// queue, or a name computed by a macro), not a way to reference an activity
    /// that only exists on a remote/polyglot worker.
    ///
    /// # Build errors
    ///
    /// Same as [`compensate`](Self::compensate):
    /// [`DagBuildError::CompensateOnGate`],
    /// [`DagBuildError::CompensatorNameCollidesWithNode`], and
    /// [`DagBuildError::EmptyCompensator`] for an empty/whitespace-only name.
    #[must_use]
    pub fn compensate_named(self, name: impl Into<String>) -> Self {
        let name = name.into().trim().to_owned();
        self.mutate(|task| task.compensate = Some(name))
    }

    fn mutate(self, update: impl FnOnce(&mut PendingDagTask)) -> Self {
        {
            let mut tasks = self.tasks.borrow_mut();
            update(&mut tasks[self.index]);
        }
        self
    }
}

/// Opaque handle to a mapped task being defined inside a [`DagBuilder`].
#[derive(Debug, Clone)]
pub struct DagMapTaskRef {
    tasks: SharedTasks,
    index: usize,
}

impl DagMapTaskRef {
    /// Bind this mapped task to map over `upstream`.
    ///
    /// # Panics
    ///
    /// Panics if `self` and `upstream` were created by different
    /// [`DagBuilder`] instances.
    #[must_use]
    pub fn over(self, upstream: &DagTaskRef) -> DagTaskRef {
        assert!(
            Rc::ptr_eq(&self.tasks, &upstream.tasks),
            "cannot connect tasks from different DagBuilder instances"
        );
        {
            let mut tasks = self.tasks.borrow_mut();
            let task = &mut tasks[self.index];
            task.map_upstream = Some(upstream.index);
            if !task.upstreams.contains(&upstream.index) {
                task.upstreams.push(upstream.index);
            }
        }
        DagTaskRef {
            tasks: self.tasks,
            index: self.index,
        }
    }
}

/// Builder for DAG task graphs.
#[derive(Debug, Clone)]
pub struct DagBuilder {
    tasks: SharedTasks,
    default_queue: Option<String>,
}

impl Default for DagBuilder {
    fn default() -> Self {
        Self {
            tasks: Rc::new(RefCell::new(Vec::new())),
            default_queue: None,
        }
    }
}

impl DagBuilder {
    /// Create a new, empty DAG builder.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::dag::DagBuilder;
    ///
    /// let mut builder = DagBuilder::new();
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new DAG builder that schedules tasks on `queue` by default.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::dag::DagBuilder;
    ///
    /// let mut builder = DagBuilder::with_default_queue("my-queue");
    /// ```
    #[must_use]
    pub fn with_default_queue(queue: impl Into<String>) -> Self {
        Self {
            default_queue: Some(queue.into()),
            ..Self::default()
        }
    }

    /// Add an activity task to the DAG.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::dag::DagBuilder;
    ///
    /// fn my_activity() {}
    ///
    /// let mut builder = DagBuilder::new();
    /// let task = builder.activity(my_activity);
    /// ```
    #[must_use]
    pub fn activity<F>(&mut self, activity: F) -> DagTaskRef
    where
        F: Copy + 'static,
    {
        let activity_name = short_activity_name(type_name_of_val(&activity));
        let mut tasks = self.tasks.borrow_mut();
        let index = tasks.len();
        tasks.push(PendingDagTask {
            activity_name,
            upstreams: Vec::new(),
            trigger_rule: TriggerRule::AllSuccess,
            retry_policy: None,
            start_to_close: None,
            queue: self.default_queue.clone(),
            map_upstream: None,
            map_failure_policy: MapFailurePolicy::FailFast,
            condition: None,
            signal: None,
            input_from: None,
            compensate: None,
        });

        DagTaskRef {
            tasks: Rc::clone(&self.tasks),
            index,
        }
    }

    /// Add a mapped activity task to the DAG.
    #[must_use]
    pub fn map_activity<F>(&mut self, activity: F) -> DagMapTaskRef
    where
        F: Copy + 'static,
    {
        let activity_name = short_activity_name(type_name_of_val(&activity));
        let mut tasks = self.tasks.borrow_mut();
        let index = tasks.len();
        tasks.push(PendingDagTask {
            activity_name,
            upstreams: Vec::new(),
            trigger_rule: TriggerRule::AllSuccess,
            retry_policy: None,
            start_to_close: None,
            queue: self.default_queue.clone(),
            map_upstream: None,
            map_failure_policy: MapFailurePolicy::FailFast,
            condition: None,
            signal: None,
            input_from: None,
            compensate: None,
        });

        DagMapTaskRef {
            tasks: Rc::clone(&self.tasks),
            index,
        }
    }

    /// Add a signal gate node that pauses the DAG until the named signal
    /// arrives (issue #746).
    ///
    /// The gate has no activity dispatch: reaching it in the unified DAG walk
    /// waits indefinitely for `signal_name`. The signal payload becomes the
    /// gate node's output, addressable by downstream nodes. Use
    /// [`signal_gate_with_timeout`](Self::signal_gate_with_timeout) to bound the
    /// wait with a deadline.
    ///
    /// Returns a [`DagTaskRef`] so the gate composes as an upstream and as a
    /// `.map_activity(...).over(&gate)` fan-out source.
    ///
    /// ```rust
    /// use autumn_harvest::dag::DagBuilder;
    ///
    /// fn extract() {}
    /// fn load() {}
    ///
    /// let mut dag = DagBuilder::new();
    /// let e = dag.activity(extract);
    /// let gate = dag.signal_gate("approval").upstream(&e);
    /// let _l = dag.activity(load).upstream(&gate);
    /// ```
    #[must_use]
    pub fn signal_gate(&mut self, signal_name: impl Into<String>) -> DagTaskRef {
        self.push_gate(DagSignalGate {
            signal_name: signal_name.into(),
            timeout: None,
            on_timeout: GateTimeoutAction::FailRun,
        })
    }

    /// Add a signal gate node bounded by a deadline (issue #746).
    ///
    /// If `signal_name` arrives before `timeout`, the gate succeeds with the
    /// signal payload as its output. If the deadline fires first, `on_timeout`
    /// decides: [`GateTimeoutAction::FailRun`] fails the DAG run, while
    /// [`GateTimeoutAction::Continue`] proceeds past the gate with a
    /// `Value::Null` output (so downstream nodes can branch on null-vs-payload
    /// via a `.condition(...)` / trigger rule).
    ///
    /// # Edge traps
    ///
    /// * Under `Continue` the timed-out output is `Value::Null`, so a
    ///   `.condition(|ups| ups[0].is_null())` cannot tell a timeout apart from an
    ///   *approval whose signal body was literally `null`* — branch on a field
    ///   instead when a null payload is possible.
    /// * A `Continue` gate cannot feed `.map_activity(...).over(&gate)` directly:
    ///   the null output is not a JSON array (runtime error `mapped upstream
    ///   output is not a JSON array`) — guard the map with a
    ///   `.condition(|ups| ups[0].is_array())`.
    /// * Independent gates that Kahn-levelling would co-locate run **serially**
    ///   (level isolation splits each gate into its own singleton level), not as
    ///   overlapping wait windows.
    /// * A gate dispatches no activity, so `.retry()`, `.start_to_close()`,
    ///   `.queue()`, and `.map_failure_policy()` are silently ignored on a gate;
    ///   only `.upstream()`, `.condition()`, and `.trigger_rule()` apply.
    #[must_use]
    pub fn signal_gate_with_timeout(
        &mut self,
        signal_name: impl Into<String>,
        timeout: Duration,
        on_timeout: GateTimeoutAction,
    ) -> DagTaskRef {
        self.push_gate(DagSignalGate {
            signal_name: signal_name.into(),
            timeout: Some(timeout),
            on_timeout,
        })
    }

    /// Push a gate node whose identity (`activity_name`) is its signal name.
    fn push_gate(&self, gate: DagSignalGate) -> DagTaskRef {
        let mut tasks = self.tasks.borrow_mut();
        let index = tasks.len();
        tasks.push(PendingDagTask {
            activity_name: gate.signal_name.clone(),
            upstreams: Vec::new(),
            trigger_rule: TriggerRule::AllSuccess,
            retry_policy: None,
            start_to_close: None,
            queue: self.default_queue.clone(),
            map_upstream: None,
            map_failure_policy: MapFailurePolicy::FailFast,
            condition: None,
            signal: Some(gate),
            input_from: None,
            compensate: None,
        });

        DagTaskRef {
            tasks: Rc::clone(&self.tasks),
            index,
        }
    }

    /// Compile the current task graph into immutable execution metadata.
    ///
    /// # Errors
    ///
    /// Returns [`DagBuildError::CycleDetected`] if the task graph contains a
    /// cycle.
    pub fn build(&self) -> Result<DagDefinition, DagBuildError> {
        let tasks = self.tasks.borrow().clone();

        validate_compensators(&tasks)?;

        // Input-binding validation (issue #702).
        for task in &tasks {
            let Some(binding) = &task.input_from else {
                continue;
            };
            // A signal gate dispatches no activity, so a binding's value is
            // ignored — but its auto-added edge would silently make the gate wait
            // for that upstream. Reject it (like the map_upstream conflict below)
            // rather than adding a stray dependency the "ignored" contract hides.
            if task.signal.is_some() {
                return Err(DagBuildError::InputBindingOnGate {
                    task: task.activity_name.clone(),
                });
            }
            // A binding and a mapped upstream are contradictory input sources.
            if task.map_upstream.is_some() {
                return Err(DagBuildError::ConflictingInputBinding {
                    task: task.activity_name.clone(),
                });
            }
            let indices: Vec<usize> = match binding {
                DagInputBinding::Single(i) => vec![*i],
                DagInputBinding::Merged(sources) => {
                    // An empty merge binding (`input_from_all(&[])` /
                    // `input_from_aliased(&[])`) would deliver an empty object.
                    if sources.is_empty() {
                        return Err(DagBuildError::EmptyInputBinding {
                            task: task.activity_name.clone(),
                        });
                    }
                    let mut seen = std::collections::HashSet::new();
                    for source in sources {
                        if !seen.insert(source.key.as_str()) {
                            return Err(DagBuildError::DuplicateInputBindingKey {
                                task: task.activity_name.clone(),
                                key: source.key.clone(),
                            });
                        }
                    }
                    sources.iter().map(|s| s.upstream_index).collect()
                }
            };
            // Defensive: every bound source must be a declared upstream. The
            // public builder methods auto-add the edge, so this is unreachable
            // via the API, but validate anyway.
            for idx in indices {
                if !task.upstreams.contains(&idx) {
                    return Err(DagBuildError::InputBindingNotAnUpstream {
                        task: task.activity_name.clone(),
                    });
                }
            }
        }

        let mut indegree = vec![0_usize; tasks.len()];
        let mut outgoing = vec![Vec::<usize>::new(); tasks.len()];

        for (task_index, task) in tasks.iter().enumerate() {
            indegree[task_index] = task.upstreams.len();
            for &upstream_index in &task.upstreams {
                outgoing[upstream_index].push(task_index);
            }
        }

        let mut ready: Vec<usize> = indegree
            .iter()
            .enumerate()
            .filter_map(|(index, degree)| (*degree == 0).then_some(index))
            .collect();
        let mut execution_levels = Vec::with_capacity(tasks.len());
        let mut visited = 0_usize;

        while !ready.is_empty() {
            ready.sort_unstable();
            let current_level = ready;
            let mut next_level = Vec::new();

            for task_index in &current_level {
                visited += 1;
                for &downstream in &outgoing[*task_index] {
                    indegree[downstream] = indegree[downstream].saturating_sub(1);
                    if indegree[downstream] == 0 {
                        next_level.push(downstream);
                    }
                }
            }

            execution_levels.push(current_level); // ⚡ Bolt: Removed unnecessary .clone() to avoid allocating a new vector per level.
            ready = next_level;
        }

        if visited != tasks.len() {
            return Err(DagBuildError::CycleDetected);
        }

        // Signal/timer gate isolation (issue #746). Originally a hard
        // requirement: the worker could persist only a homogeneous suspension
        // batch, so a gate's `WaitForSignal` must never share a level with a
        // sibling activity's `ScheduleActivity`. Issue #950 lifted that
        // constraint (`persist_mixed_suspension_batch` persists the mixed batch
        // in one transaction), but the split is RETAINED deliberately: it is the
        // recorded execution shape of every DAG already in flight, and merging a
        // gate back into its level would change the command order those
        // histories replay against. Collapsing it is a separate, versioned
        // change, not a side effect of #950.
        // Split each Kahn level that contains a gate into
        // `[non-gate tasks] ++ [each gate as its own singleton]`. Same-level
        // tasks are mutually independent, so re-sequencing among them is safe.
        // When the DAG has no gates the level vector is left byte-for-byte
        // unchanged (zero behaviour change for existing DAGs).
        let execution_levels = if tasks.iter().any(|t| t.signal.is_some()) {
            let mut split = Vec::with_capacity(execution_levels.len());
            for level in execution_levels {
                let (gates, non_gates): (Vec<usize>, Vec<usize>) =
                    level.into_iter().partition(|&i| tasks[i].signal.is_some());
                if !non_gates.is_empty() {
                    split.push(non_gates);
                }
                for gate_idx in gates {
                    split.push(vec![gate_idx]);
                }
            }
            split
        } else {
            execution_levels
        };

        Ok(DagDefinition {
            tasks: tasks.into_iter().map(Into::into).collect(),
            execution_levels,
        })
    }
}

/// Validate every node's compensator declaration (issue #780).
///
/// A compensator must be dispatchable by name on the terminal-failure unwind,
/// and must stay distinguishable from every forward node in recorded history.
/// One compensator **shared by several nodes** is fine — the unwind envelope's
/// `dag_compensate` field disambiguates which node it is undoing.
fn validate_compensators(tasks: &[PendingDagTask]) -> Result<(), DagBuildError> {
    // The node-identity set is built once and includes gates (whose identity
    // is their signal name).
    let node_names: std::collections::HashSet<&str> = tasks
        .iter()
        .map(|task| task.activity_name.as_str())
        .collect();
    for task in tasks {
        let Some(compensator) = &task.compensate else {
            continue;
        };
        // A gate dispatches no activity, so it has no side effect to undo.
        if task.signal.is_some() {
            return Err(DagBuildError::CompensateOnGate {
                task: task.activity_name.clone(),
            });
        }
        // An empty name would dispatch a nameless activity mid-unwind.
        if compensator.trim().is_empty() {
            return Err(DagBuildError::EmptyCompensator {
                task: task.activity_name.clone(),
            });
        }
        // A compensator sharing a forward node's identity would be
        // indistinguishable from that node in recorded history, corrupting the
        // name-keyed classification the run graph (#690) and retry-from-node
        // (#366) depend on.
        if node_names.contains(compensator.as_str()) {
            return Err(DagBuildError::CompensatorNameCollidesWithNode {
                task: task.activity_name.clone(),
                compensator: compensator.clone(),
            });
        }
    }
    Ok(())
}

/// Resolve a bound node's activity input from upstream outputs (issue #702):
/// the raw output for a [`DagInputBinding::Single`], or a keyed JSON object for
/// a [`DagInputBinding::Merged`]. No `dag_task` injection, no `conf` wrapping.
fn bind_activity_input(binding: &DagInputBinding, outputs: &[Value]) -> Value {
    // Indices are validated at build time, so the happy-path behaviour is
    // unchanged; the defensive `.get(..).unwrap_or(Null)` matches the sibling
    // `dispatch_decision` convention (defence in depth against an out-of-range
    // index).
    match binding {
        DagInputBinding::Single(idx) => outputs.get(*idx).cloned().unwrap_or(Value::Null),
        DagInputBinding::Merged(sources) => {
            let mut obj = serde_json::Map::new();
            for source in sources {
                obj.insert(
                    source.key.clone(),
                    outputs
                        .get(source.upstream_index)
                        .cloned()
                        .unwrap_or(Value::Null),
                );
            }
            Value::Object(obj)
        }
    }
}

fn short_activity_name(type_name: &str) -> String {
    type_name
        .rsplit("::")
        .next()
        .unwrap_or(type_name)
        .to_string()
}

/// Execute a compiled unified DAG on the standard workflow execution path
/// (issue #256 Step 1, extended for signal gates in issue #746).
///
/// This is the single source of truth for the `#[dag]` level walker. The
/// `#[dag]` macro builds the [`DagDefinition`] in a scoped block (so the
/// non-`Send` [`DagBuilder`] is dropped before any `.await`) and hands the
/// resulting `(levels, tasks)` here. Both the inlined `DagInfo::workflow_handler`
/// and the shadow `WorkflowInfo::handler` call this function, so the walk logic
/// (dispatch decisions, mapped fan-out, signal gates) lives in exactly one
/// place.
///
/// The walk dispatches each level's activities through
/// `ctx.execute_activity_raw_with_opts`, `join_all`-ing the level, and
/// accumulates per-task statuses so trigger rules and conditions are
/// deterministic and replay-safe. A signal/timer gate node (isolated into its
/// own singleton level by [`DagBuilder::build`]) instead awaits its named
/// signal via the #476 `wait_for_signal` / `wait_for_signal_timeout` primitives.
///
/// # Errors
///
/// Returns `Err` if the workflow-context propagates a non-activity error (e.g.
/// a non-determinism divergence), or `Err("one or more DAG tasks failed")` when
/// any task reaches [`TaskStatus::Failed`].
///
/// A terminal failure additionally triggers the issue #780 compensation unwind
/// (see [`unwind_dag_compensations`]) when any **succeeded** node declares a
/// [`DagTask::compensate`]: a successful unwind still returns the original
/// `"one or more DAG tasks failed"` error unchanged, while a failing
/// compensation surfaces a stringified
/// [`HarvestError::SagaCompensationFailed`](crate::error::HarvestError::SagaCompensationFailed)
/// carrying both the original error and the compensation errors. A **cancelled**
/// run never unwinds (`docs/saga.md`: cancellation does not auto-compensate).
#[allow(clippy::too_many_lines, clippy::type_complexity)]
pub async fn run_unified_dag(
    ctx: &crate::context::WorkflowContext,
    input: Value,
    levels: Vec<Vec<usize>>,
    tasks: Vec<DagTask>,
) -> Result<Value, String> {
    use std::future::Future;
    use std::pin::Pin;

    use crate::error::HarvestError;
    use crate::futures;
    use crate::policy::{MapFailurePolicy, TaskStatus};

    /// One level-member's dispatch result:
    /// `(task_idx, status, output, dispatched_forward, shape_failure_reason)`
    /// — see `dispatched_forward` / `shape_failure` below (issue #780).
    type NodeRun = (usize, TaskStatus, Value, bool, Option<String>);

    let n = tasks.len();
    let mut statuses: Vec<TaskStatus> = vec![TaskStatus::Skipped; n];
    let mut outputs: Vec<Value> = vec![Value::Null; n];
    // Resolved forward inputs, retained ONLY for nodes that declare a
    // compensator (issue #780) so the unwind envelope can carry them. Every
    // other slot stays `Value::Null` and costs nothing.
    let mut inputs: Vec<Value> = vec![Value::Null; n];
    // Whether each node actually dispatched forward work. A mapped node over an
    // EMPTY upstream array reaches `Succeeded` without dispatching a single
    // instance, so it has NO side effect to undo — compensating it would issue
    // a refund for a charge that never happened (issue #780 post-PR review).
    let mut dispatched_forward: Vec<bool> = vec![false; n];
    // The first deterministic, pre-dispatch input-shape rejection, if any. It
    // fails the DAG like any node failure (so the unwind runs) while keeping
    // the precise diagnostic operator-visible.
    let mut shape_failure: Option<String> = None;

    for level in &levels {
        // ── Signal/timer gate (issue #746) ──────────────────────────────────
        // A gate is always alone in its level (guaranteed by DagBuilder::build),
        // so it is handled inline: it awaits a signal, not an activity, and must
        // never be batched into `activity_futs`. Detect a gate by
        // `signal.is_some()` regardless of level size (NOT `level.len() == 1`):
        // if a future refactor ever broke the singleton-isolation invariant, the
        // `debug_assert!` below fails LOUDLY here instead of silently routing the
        // gate through `activity_futs` and dispatching a phantom activity named
        // after the signal.
        let gate_opt = level
            .iter()
            .find_map(|&i| tasks[i].signal.clone().map(|gate| (i, gate)));
        if let Some((task_idx, gate)) = gate_opt {
            debug_assert_eq!(
                level.len(),
                1,
                "signal gate node must occupy its own singleton execution level, got {level:?}"
            );
            let activity_name = tasks[task_idx].activity_name.clone();
            let upstreams = tasks[task_idx].upstreams.clone();

            match tasks[task_idx].dispatch_decision(&statuses, &outputs) {
                DagDispatchDecision::SkipByTriggerRule => {
                    statuses[task_idx] = TaskStatus::Skipped;
                    continue;
                }
                DagDispatchDecision::SkipByCondition => {
                    ctx.dag_skip_marker(task_idx, &activity_name, &upstreams)
                        .map_err(|e| e.to_string())?;
                    statuses[task_idx] = TaskStatus::Skipped;
                    continue;
                }
                DagDispatchDecision::Run => {}
            }

            let (status, val) = if let Some(timeout) = gate.timeout {
                match ctx
                    .wait_for_signal_timeout(&gate.signal_name, timeout)
                    .await
                    .map_err(|e| e.to_string())?
                {
                    Some(payload) => (TaskStatus::Succeeded, payload),
                    None => match gate.on_timeout {
                        GateTimeoutAction::FailRun => (TaskStatus::Failed, Value::Null),
                        GateTimeoutAction::Continue => (TaskStatus::Succeeded, Value::Null),
                    },
                }
            } else {
                let payload = ctx
                    .wait_for_signal(&gate.signal_name)
                    .await
                    .map_err(|e| e.to_string())?;
                (TaskStatus::Succeeded, payload)
            };
            statuses[task_idx] = status;
            outputs[task_idx] = val;
            continue;
        }

        // ── Non-gate level: activity / mapped-activity dispatch ──────────────
        let mut activity_futs: Vec<
            Pin<Box<dyn Future<Output = Result<NodeRun, String>> + Send + '_>>,
        > = Vec::new();

        for &task_idx in level {
            let activity_name: String = tasks[task_idx].activity_name.clone();
            let queue_str: String = tasks[task_idx].queue.clone().unwrap_or_default();
            let upstreams: Vec<usize> = tasks[task_idx].upstreams.clone();
            let retry_override = tasks[task_idx].retry_policy.clone();
            let stc_override = tasks[task_idx].start_to_close;

            match tasks[task_idx].dispatch_decision(&statuses, &outputs) {
                DagDispatchDecision::SkipByTriggerRule => {
                    statuses[task_idx] = TaskStatus::Skipped;
                    continue;
                }
                DagDispatchDecision::SkipByCondition => {
                    ctx.dag_skip_marker(task_idx, &activity_name, &upstreams)
                        .map_err(|e| e.to_string())?;
                    statuses[task_idx] = TaskStatus::Skipped;
                    continue;
                }
                DagDispatchDecision::Run => {}
            }

            if let Some(upstream_idx) = tasks[task_idx].map_upstream {
                let upstream_val = outputs[upstream_idx].clone();
                // A mapped node is compensated at NODE granularity (issue
                // #780), so its "resolved forward input" is the whole mapped
                // upstream array, not any single cell's item.
                if tasks[task_idx].compensate.is_some() {
                    inputs[task_idx] = upstream_val.clone();
                }
                let policy = tasks[task_idx].map_failure_policy;
                let activity_name_clone = activity_name.clone();
                let queue_str_clone = queue_str.clone();
                let retry_override_clone = retry_override.clone();
                let stc_override_clone = stc_override;
                activity_futs.push(Box::pin(async move {
                    let Value::Array(array) = &upstream_val else {
                        // A deterministic, pre-dispatch input-shape rejection —
                        // NOT an engine error (issue #780 post-PR review). It
                        // used to `return Err`, which `activity_result?`
                        // propagated straight past the terminal check, so a
                        // compensable upstream that had already succeeded was
                        // left un-rolled-back. Reporting it as an ordinary node
                        // FAILURE routes it through the normal terminal path
                        // (and therefore the unwind), while `shape_failure`
                        // keeps the precise diagnostic in the returned error.
                        // Genuine replay/non-determinism errors still propagate
                        // directly via `?` and never trigger an unwind, which
                        // matters: unwinding from a diverged cursor is exactly
                        // the P1-B nd-block failure fixed above.
                        return Ok((
                            task_idx,
                            TaskStatus::Failed,
                            Value::Null,
                            false,
                            Some("mapped upstream output is not a JSON array".to_owned()),
                        ));
                    };
                    let n_instances = array.len();
                    if n_instances == 0 {
                        // Zero instances dispatched: the node succeeds
                        // vacuously, so it has nothing to compensate.
                        return Ok((
                            task_idx,
                            TaskStatus::Succeeded,
                            Value::Array(Vec::new()),
                            false,
                            None,
                        ));
                    }

                    let mut instance_futs = Vec::new();
                    for (i, item) in array.iter().enumerate() {
                        let item_input = item.clone();
                        let act_name = activity_name_clone.clone();
                        let q_str = queue_str_clone.clone();
                        let ret_override = retry_override_clone.clone();
                        let stc_over = stc_override_clone;
                        instance_futs.push(async move {
                            let res = ctx
                                .execute_activity_raw_with_opts(
                                    &act_name,
                                    item_input,
                                    &q_str,
                                    ret_override,
                                    stc_over,
                                )
                                .await;
                            (i, res)
                        });
                    }

                    let mut results = vec![Value::Null; n_instances];
                    let mut status = TaskStatus::Succeeded;
                    let mut final_val = Value::Null;
                    // Carries a deterministic pre-dispatch rejection's precise
                    // diagnostic out to the terminal error (issue #780).
                    let mut shape_reason: Option<String> = None;

                    if policy == MapFailurePolicy::FailFast {
                        use futures::StreamExt as _;
                        let mut stream = instance_futs
                            .into_iter()
                            .collect::<futures::stream::FuturesUnordered<_>>();
                        // The stream is DRAINED even after the first failure
                        // (issue #780, post-review P1-B). Abandoning it mid-way
                        // used to leave the unyielded instances unpolled, so
                        // their recorded `ActivityScheduled` events stayed
                        // unconsumed and the replay cursor was parked on one of
                        // them. That was harmless while the terminal check
                        // returned immediately, but the compensation unwind now
                        // consumes history after this loop and would dispatch
                        // its first compensator straight into the stale cursor:
                        // `match_activity` diverges, which issue #603 turns into
                        // a PERMANENT nd-block (the divergence is data-caused,
                        // so every retry replays it identically) *and* silently
                        // skips compensation.
                        //
                        // Draining is also the only cursor-clean option: polling
                        // a still-in-flight instance once would push a
                        // `WaitForActivity` command that the unwind's
                        // `ScheduleActivity` batch cannot legally share.
                        //
                        // Semantics preserved: FIRST failure wins (`status` only
                        // ever moves Succeeded -> Failed, and `final_val` is
                        // built solely from the all-succeeded case). A
                        // non-activity error (e.g. a genuine non-determinism
                        // divergence) still propagates rather than being
                        // swallowed. The one behavioural change is timing: the
                        // DAG now settles every instance of the failed mapped
                        // node before terminating, instead of abandoning the
                        // in-flight siblings — which never durably cancelled
                        // them anyway.
                        while let Some((i, res)) = stream.next().await {
                            match res {
                                Ok(v) => results[i] = v,
                                Err(err) => match &err {
                                    HarvestError::ActivityFailed { .. }
                                    | HarvestError::Timeout { .. } => {
                                        status = TaskStatus::Failed;
                                    }
                                    // A deterministic pre-dispatch rejection
                                    // fails the NODE (issue #780 post-PR
                                    // review): the DAG outcome is unchanged
                                    // (it failed before too), but it now
                                    // reaches the unwind instead of escaping
                                    // past it. The first such reason wins,
                                    // matching the fail-fast contract.
                                    e if is_deterministic_dispatch_rejection(e) => {
                                        status = TaskStatus::Failed;
                                        if shape_reason.is_none() {
                                            shape_reason = Some(e.to_string());
                                        }
                                    }
                                    _ => return Err(err.to_string()),
                                },
                            }
                        }
                        if status == TaskStatus::Succeeded {
                            final_val = Value::Array(results);
                        }
                    } else {
                        let mut collect_results = vec![Value::Null; n_instances];
                        for res_item in futures::future::join_all(instance_futs).await {
                            let (i, res) = res_item;
                            let obj = match res {
                                Ok(v) => serde_json::json!({"status": "succeeded", "value": v}),
                                // Non-activity/non-timeout errors (e.g. a
                                // non-determinism divergence) propagate rather
                                // than being swallowed into a per-item cell —
                                // consistent with the fail-fast branch above.
                                Err(err) => match &err {
                                    HarvestError::ActivityFailed { .. }
                                    | HarvestError::Timeout { .. } => {
                                        let err_str = match &err {
                                            HarvestError::ActivityFailed { source, .. } => {
                                                source.to_string()
                                            }
                                            _ => err.to_string(),
                                        };
                                        serde_json::json!({"status": "failed", "error": err_str})
                                    }
                                    // A deterministic pre-dispatch rejection is
                                    // NOT a per-cell business failure, so it is
                                    // deliberately not folded into the cells
                                    // array: it fails the NODE (issue #780
                                    // post-PR review). That keeps today's
                                    // outcome — the DAG failed before this fix
                                    // too — while routing it through the
                                    // unwind. Folding it into a cell would
                                    // instead let the DAG COMPLETE, silently
                                    // turning a cap violation into a success.
                                    e if is_deterministic_dispatch_rejection(e) => {
                                        status = TaskStatus::Failed;
                                        if shape_reason.is_none() {
                                            shape_reason = Some(e.to_string());
                                        }
                                        Value::Null
                                    }
                                    _ => return Err(err.to_string()),
                                },
                            };
                            collect_results[i] = obj;
                        }
                        if status == TaskStatus::Succeeded {
                            final_val = Value::Array(collect_results);
                        }
                    }

                    Ok::<_, String>((task_idx, status, final_val, true, shape_reason))
                }));
            } else {
                // Highest priority (issue #702): an explicit input binding
                // draws the activity input directly from upstream output(s) —
                // the RAW output, with NO `dag_task` injection and NO `conf`
                // wrapping. The unbound `||` branch below stays byte-identical.
                let activity_input = tasks[task_idx].input_from.as_ref().map_or_else(
                    || {
                        let mapped_up = upstreams
                            .iter()
                            .copied()
                            .find(|&i| tasks[i].map_upstream.is_some());
                        mapped_up.map_or_else(
                            || match input.clone() {
                                Value::Object(mut object) => {
                                    object.insert(
                                        "dag_task".to_owned(),
                                        Value::String(activity_name.clone()),
                                    );
                                    Value::Object(object)
                                }
                                conf => {
                                    let mut object = serde_json::Map::new();
                                    object.insert("conf".to_owned(), conf);
                                    object.insert(
                                        "dag_task".to_owned(),
                                        Value::String(activity_name.clone()),
                                    );
                                    Value::Object(object)
                                }
                            },
                            |mapped_up_idx| outputs[mapped_up_idx].clone(),
                        )
                    },
                    |binding| bind_activity_input(binding, &outputs),
                );

                // Retain the resolved forward input for the compensation
                // envelope (issue #780) before it is moved into the dispatch
                // future. Uncompensated nodes clone nothing.
                if tasks[task_idx].compensate.is_some() {
                    inputs[task_idx] = activity_input.clone();
                }

                activity_futs.push(Box::pin(async move {
                    let (status, val, dispatched, reason) = match ctx
                        .execute_activity_raw_with_opts(
                            &activity_name,
                            activity_input,
                            &queue_str,
                            retry_override,
                            stc_override,
                        )
                        .await
                    {
                        Ok(v) => (TaskStatus::Succeeded, v, true, None),
                        Err(HarvestError::ActivityFailed { .. } | HarvestError::Timeout { .. }) => {
                            (TaskStatus::Failed, Value::Null, true, None)
                        }
                        // A deterministic PRE-DISPATCH rejection (issue #780
                        // post-PR review) — see `is_deterministic_dispatch_rejection`.
                        // Reported as an ordinary node FAILURE so it routes
                        // through the terminal path and therefore the unwind,
                        // instead of `?`-escaping past it and stranding an
                        // already-succeeded compensable upstream.
                        Err(error) if is_deterministic_dispatch_rejection(&error) => (
                            TaskStatus::Failed,
                            Value::Null,
                            false,
                            Some(error.to_string()),
                        ),
                        Err(error) => return Err(error.to_string()),
                    };
                    Ok::<_, String>((task_idx, status, val, dispatched, reason))
                }));
            }
        }

        for activity_result in futures::future::join_all(activity_futs).await {
            let (task_idx, status, val, dispatched, reason) = activity_result?;
            statuses[task_idx] = status;
            outputs[task_idx] = val;
            dispatched_forward[task_idx] = dispatched;
            if shape_failure.is_none() {
                shape_failure = reason;
            }
        }
    }

    if statuses.iter().any(|s| matches!(s, TaskStatus::Failed)) {
        // A deterministic input-shape rejection keeps its precise message so
        // the operator still sees *why* (the generic DAG error names no node);
        // any other failure uses the generic error, since per-node detail is
        // already in the recorded `ActivityFailed` event.
        let original = shape_failure.unwrap_or_else(|| "one or more DAG tasks failed".to_owned());
        // `docs/saga.md`: cancellation does NOT auto-compensate. Skipping the
        // unwind here also avoids dispatching into an unconsumed
        // `WorkflowCancelled`, which would nd-block (issue #603) a run the
        // operator already cancelled.
        if ctx.is_cancelled() {
            return Err(original);
        }
        return unwind_dag_compensations(
            ctx,
            &levels,
            &tasks,
            &NodeUnwindState {
                statuses: &statuses,
                dispatched_forward: &dispatched_forward,
                inputs: &inputs,
                outputs: &outputs,
            },
            original,
        )
        .await;
    }

    Ok(Value::Null)
}

/// Is this error a **deterministic pre-dispatch rejection** — one the engine
/// raises *before* it allocates an activity id, pushes any `WorkflowCommand`, or
/// records any event?
///
/// Such an error leaves **no history footprint and no side effect**, and is a
/// pure function of already-recorded state (the serialised input size) plus
/// stable configuration. It is therefore safe — and, for the issue #780 unwind,
/// necessary — to report it as an ordinary node **failure**, so the DAG reaches
/// its terminal check and runs the compensation unwind instead of `?`-escaping
/// past it and stranding an already-succeeded compensable upstream.
///
/// The complement must keep propagating directly:
///
/// * [`HarvestError::NonDeterministic`](crate::error::HarvestError::NonDeterministic)
///   — unwinding from a diverged replay cursor is exactly the permanent
///   nd-block (issue #603) this slice already had to fix once.
/// * [`HarvestError::Cancelled`](crate::error::HarvestError::Cancelled) —
///   `docs/saga.md`: cancellation does not auto-compensate.
/// * Transient engine/storage errors — the workflow task is retried, so the run
///   is not terminal and must not unwind.
///
/// Deliberately narrow: mis-classifying a *transient* error as a node failure
/// would unwind a run that the engine was merely going to retry.
///
/// # Known limitation — the rejection has no history footprint
///
/// "Leaves no history footprint" is what makes routing this to the unwind safe,
/// and is also its one limitation: the decision is re-evaluated on every replay
/// against *live* configuration. Raise the cap while a compensating DAG is
/// mid-unwind and the node now dispatches, colliding with the recorded
/// compensator — a divergence, surfacing as a #603 nd-block (a stuck-but-
/// recoverable run, never a silent partial rollback).
///
/// The same class as the engine-wide `known_limitation_early_config_dependent_
/// failure_does_not_replay_cleanly` (issue #601); issue #780 enlarges the
/// surface rather than creating it. A durable fix means persisting the rejection
/// in the *engine's* dispatch path — it cannot be done here, because a level
/// dispatches concurrently through `join_all`, so a marker from inside a task
/// future has no deterministic position and one after the join is read too late
/// to gate the dispatch. See `docs/saga.md` ("raising a payload cap *during* an
/// unwind diverges") and
/// `dag_compensation_tests::known_limitation_raising_the_cap_mid_unwind_diverges`.
const fn is_deterministic_dispatch_rejection(error: &crate::error::HarvestError) -> bool {
    matches!(error, crate::error::HarvestError::PayloadTooLarge { .. })
}

/// Per-node forward-run state consulted by [`unwind_dag_compensations`] when it
/// decides which nodes have a side effect to undo (issue #780).
struct NodeUnwindState<'a> {
    /// Terminal status of each node in the forward run.
    statuses: &'a [TaskStatus],
    /// Whether the node actually dispatched forward work. A mapped node over an
    /// empty upstream array succeeds *vacuously* — nothing ran, so nothing is
    /// compensated.
    dispatched_forward: &'a [bool],
    /// Resolved forward input of each node, echoed to its compensator.
    inputs: &'a [Value],
    /// Recorded output of each node, echoed to its compensator.
    outputs: &'a [Value],
}

/// Compensate every **succeeded** node that declares a compensator, in reverse
/// topological (LIFO) order, after a terminal DAG failure (issue #780).
///
/// Compensations are pushed in levels-forward / ascending-index order — the
/// DAG's own topological order — and the [`Saga`](crate::saga::Saga) unwind
/// pops them LIFO, so an undo never runs before the undo of a node that
/// depended on it. Each compensator is dispatched through the ordinary
/// activity lowering on the compensated node's own queue, so compensation
/// rides existing `ActivityScheduled`/`ActivityCompleted` events and adds **no
/// new `WorkflowEvent` variant**.
///
/// A node that was skipped, never reached, or itself failed is never
/// compensated: only a successful forward step has an effect to undo. Nor is a
/// node that succeeded WITHOUT dispatching forward work — a mapped node over an
/// empty upstream array (`dispatched_forward`).
///
/// # Errors
///
/// Returns the caller's `original` DAG error when every compensation succeeds,
/// or [`HarvestError::SagaCompensationFailed`](crate::error::HarvestError::SagaCompensationFailed)
/// (stringified) when any compensation fails — every remaining compensation is
/// still attempted first (continue-not-abort).
async fn unwind_dag_compensations(
    ctx: &crate::context::WorkflowContext,
    levels: &[Vec<usize>],
    tasks: &[DagTask],
    state: &NodeUnwindState<'_>,
    original: String,
) -> Result<Value, String> {
    let mut saga = crate::saga::Saga::new(ctx);

    for level in levels {
        for &i in level {
            if !matches!(state.statuses[i], TaskStatus::Succeeded) {
                continue;
            }
            // A vacuous success (a mapped node over an empty array) dispatched
            // nothing, so there is no side effect to undo.
            if !state.dispatched_forward[i] {
                continue;
            }
            let Some(comp_name) = tasks[i].compensate.clone() else {
                continue;
            };
            let queue = tasks[i].queue.clone().unwrap_or_default();
            let envelope = serde_json::json!({
                "dag_compensate": tasks[i].activity_name,
                "input": state.inputs[i],
                "output": state.outputs[i],
            });
            saga.push_compensation(move || async move {
                ctx.execute_activity_raw_with_opts(&comp_name, envelope, &queue, None, None)
                    .await
                    .map(|_| ())
            });
        }
    }

    // `Vec::pop` inside the unwind ⇒ reverse-topological LIFO order.
    match saga.compensate_all_after(original.clone()).await {
        Ok(()) => Err(original),
        Err(error) => Err(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn dummy_activity() {}
    fn dummy_activity2() {}
    fn dummy_activity3() {}

    #[test]
    fn test_short_activity_name() {
        assert_eq!(short_activity_name("dummy_activity"), "dummy_activity");
        assert_eq!(
            short_activity_name("my_crate::module::dummy_activity"),
            "dummy_activity"
        );
        assert_eq!(short_activity_name("::dummy_activity"), "dummy_activity");
    }

    #[test]
    fn test_empty_dag() {
        let builder = DagBuilder::new();
        let dag = builder.build().expect("build should succeed");
        assert!(dag.tasks().is_empty());
        assert!(dag.execution_levels().is_empty());
    }

    #[test]
    fn test_single_activity() {
        let mut builder = DagBuilder::new();
        let _ = builder.activity(dummy_activity);

        let dag = builder.build().expect("build should succeed");
        let tasks = dag.tasks();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].activity_name, "dummy_activity");
        assert!(tasks[0].upstreams.is_empty());
        assert_eq!(tasks[0].trigger_rule, TriggerRule::AllSuccess);
        assert!(tasks[0].retry_policy.is_none());
        assert!(tasks[0].start_to_close.is_none());
        assert!(tasks[0].queue.is_none());

        let levels = dag.execution_levels();
        assert_eq!(levels.len(), 1);
        assert_eq!(levels[0], vec![0]);
    }

    #[test]
    fn test_with_default_queue() {
        let mut builder = DagBuilder::with_default_queue("custom_queue");
        let _ = builder.activity(dummy_activity);

        let dag = builder.build().unwrap();
        assert_eq!(dag.tasks()[0].queue.as_deref(), Some("custom_queue"));
    }

    #[test]
    fn test_modifying_task_parameters() {
        let mut builder = DagBuilder::new();
        let _ = builder
            .activity(dummy_activity)
            .trigger_rule(TriggerRule::AllDone)
            .retry(RetryPolicy::fixed(3, Duration::from_secs(1)))
            .start_to_close(Duration::from_secs(10))
            .queue("specific_queue");

        let dag = builder.build().unwrap();
        let task = &dag.tasks()[0];

        assert_eq!(task.trigger_rule, TriggerRule::AllDone);
        assert_eq!(task.start_to_close, Some(Duration::from_secs(10)));
        assert_eq!(task.queue.as_deref(), Some("specific_queue"));
        assert!(task.retry_policy.is_some());
        if let Some(rp) = &task.retry_policy {
            assert_eq!(rp.max_attempts, 3);
        }
    }

    #[test]
    fn test_simple_dependency_chaining() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity);
        let b = builder.activity(dummy_activity2).upstream(&a);
        let _c = builder.activity(dummy_activity3).upstream(&a).upstream(&b);

        let dag = builder.build().unwrap();
        let tasks = dag.tasks();

        assert_eq!(tasks.len(), 3);
        assert!(tasks[0].upstreams.is_empty());
        assert_eq!(tasks[1].upstreams, vec![0]);
        // upstreams are inserted in order
        assert_eq!(tasks[2].upstreams, vec![0, 1]);

        let levels = dag.execution_levels();
        assert_eq!(levels.len(), 3);
        assert_eq!(levels[0], vec![0]); // a runs first
        assert_eq!(levels[1], vec![1]); // b runs second
        assert_eq!(levels[2], vec![2]); // c runs third
    }

    #[test]
    fn test_fan_out_fan_in() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity); // 0
        let b1 = builder.activity(dummy_activity).upstream(&a); // 1
        let b2 = builder.activity(dummy_activity).upstream(&a); // 2
        let _c = builder.activity(dummy_activity).upstream(&b1).upstream(&b2); // 3

        let dag = builder.build().unwrap();
        let levels = dag.execution_levels();

        assert_eq!(levels.len(), 3);
        assert_eq!(levels[0], vec![0]);
        assert_eq!(levels[1], vec![1, 2]); // b1 and b2 run in parallel
        assert_eq!(levels[2], vec![3]);
    }

    #[test]
    fn test_cycle_detection() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity);
        let b = builder.activity(dummy_activity2).upstream(&a);

        // create a cycle: a depends on b
        let a_clone = a;
        let _ = a_clone.upstream(&b);

        let res = builder.build();
        assert_eq!(res.unwrap_err(), DagBuildError::CycleDetected);
    }

    #[test]
    #[should_panic(expected = "cannot connect tasks from different DagBuilder instances")]
    fn test_cross_builder_panic() {
        let mut builder1 = DagBuilder::new();
        let a = builder1.activity(dummy_activity);

        let mut builder2 = DagBuilder::new();
        let _b = builder2.activity(dummy_activity2).upstream(&a);
    }

    #[test]
    fn test_dag_build_error_display() {
        let err = DagBuildError::CycleDetected;
        assert_eq!(err.to_string(), "dag contains a dependency cycle");
    }

    #[test]
    fn should_deduplicate_identical_upstream_dependencies() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity);
        // Upstream added twice
        let _b = builder.activity(dummy_activity2).upstream(&a).upstream(&a);

        let dag = builder.build().unwrap();
        let tasks = dag.tasks();

        // b's upstreams should only contain a once
        assert_eq!(tasks[1].upstreams, vec![0]);
    }

    #[test]
    fn should_process_disjoint_subgraphs() {
        let mut builder = DagBuilder::new();

        // Subgraph 1
        let a = builder.activity(dummy_activity);
        let _b = builder.activity(dummy_activity2).upstream(&a);

        // Subgraph 2
        let c = builder.activity(dummy_activity3);
        let _d = builder.activity(dummy_activity).upstream(&c);

        let dag = builder.build().unwrap();
        let levels = dag.execution_levels();

        // Level 0: [a, c]
        // Level 1: [b, d]
        assert_eq!(levels.len(), 2);
        assert_eq!(levels[0], vec![0, 2]); // a, c
        assert_eq!(levels[1], vec![1, 3]); // b, d
    }

    #[test]
    fn should_override_default_queue() {
        let mut builder = DagBuilder::with_default_queue("default-queue");

        let _a = builder.activity(dummy_activity);
        let _b = builder.activity(dummy_activity2).queue("custom-queue");

        let dag = builder.build().unwrap();
        let tasks = dag.tasks();

        assert_eq!(tasks[0].queue.as_deref(), Some("default-queue"));
        assert_eq!(tasks[1].queue.as_deref(), Some("custom-queue"));
    }

    #[test]
    fn should_detect_self_referential_dependency_cycles() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity);

        // Self-reference cycle
        let a_clone = a.clone();
        let _ = a.upstream(&a_clone);

        let res = builder.build();
        assert_eq!(res.unwrap_err(), DagBuildError::CycleDetected);
    }

    #[test]
    fn should_support_mapped_activity_builder() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity);
        let _b = builder.map_activity(dummy_activity2).over(&a);

        let dag = builder.build().unwrap();
        let tasks = dag.tasks();

        assert_eq!(tasks.len(), 2);
        assert_eq!(tasks[1].map_upstream, Some(0));
        assert_eq!(tasks[1].map_failure_policy, MapFailurePolicy::FailFast);
        assert_eq!(tasks[1].upstreams, vec![0]);
    }

    #[test]
    fn should_support_mapped_failure_policy_override() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity);
        let _b = builder
            .map_activity(dummy_activity2)
            .over(&a)
            .map_failure_policy(MapFailurePolicy::CollectAll);

        let dag = builder.build().unwrap();
        let tasks = dag.tasks();
        assert_eq!(tasks[1].map_failure_policy, MapFailurePolicy::CollectAll);
    }

    // ── Phase 1 / Issue #482 — DagCondition + DagDispatchDecision ──────────

    #[test]
    fn condition_is_stored_on_task() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity);
        let _b = builder
            .activity(dummy_activity2)
            .upstream(&a)
            .condition(|_| true);

        let dag = builder.build().unwrap();
        let tasks = dag.tasks();
        assert!(
            tasks[0].condition.is_none(),
            "root task should have no condition"
        );
        assert!(
            tasks[1].condition.is_some(),
            "conditioned task should have condition set"
        );
    }

    #[test]
    fn dispatch_decision_run_when_no_condition() {
        let mut builder = DagBuilder::new();
        let _ = builder.activity(dummy_activity);
        let dag = builder.build().unwrap();
        let statuses = [TaskStatus::Succeeded];
        let outputs = [Value::Null];
        assert_eq!(
            dag.tasks()[0].dispatch_decision(&statuses, &outputs),
            DagDispatchDecision::Run,
        );
    }

    #[test]
    fn dispatch_decision_skip_by_trigger_rule() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity);
        let _b = builder.activity(dummy_activity2).upstream(&a); // AllSuccess default
        let dag = builder.build().unwrap();
        // upstream failed → trigger rule should block; condition must NOT run
        let statuses = [TaskStatus::Failed, TaskStatus::Succeeded];
        let outputs = [Value::Null, Value::Null];
        assert_eq!(
            dag.tasks()[1].dispatch_decision(&statuses, &outputs),
            DagDispatchDecision::SkipByTriggerRule,
        );
    }

    #[test]
    fn dispatch_decision_condition_not_invoked_when_trigger_fails() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let invoked = Arc::new(AtomicBool::new(false));
        let invoked_clone = Arc::clone(&invoked);

        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity);
        let _b = builder
            .activity(dummy_activity2)
            .upstream(&a)
            .condition(move |_| {
                invoked_clone.store(true, Ordering::SeqCst);
                true
            });
        let dag = builder.build().unwrap();
        // upstream failed → trigger rule (AllSuccess) should be false
        let statuses = [TaskStatus::Failed, TaskStatus::Succeeded];
        let outputs = [Value::Null, Value::Null];
        let decision = dag.tasks()[1].dispatch_decision(&statuses, &outputs);
        assert_eq!(decision, DagDispatchDecision::SkipByTriggerRule);
        assert!(
            !invoked.load(Ordering::SeqCst),
            "condition must NOT be invoked when trigger fails"
        );
    }

    #[test]
    fn dispatch_decision_skip_by_condition_when_predicate_false() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity);
        let _b = builder
            .activity(dummy_activity2)
            .upstream(&a)
            .condition(|ups| ups[0]["score"].as_f64().is_some_and(|s| s > 0.8));
        let dag = builder.build().unwrap();
        let statuses = [TaskStatus::Succeeded, TaskStatus::Succeeded];
        let outputs = [serde_json::json!({"score": 0.2}), Value::Null];
        assert_eq!(
            dag.tasks()[1].dispatch_decision(&statuses, &outputs),
            DagDispatchDecision::SkipByCondition,
        );
    }

    #[test]
    fn dispatch_decision_run_when_condition_true() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity);
        let _b = builder
            .activity(dummy_activity2)
            .upstream(&a)
            .condition(|ups| ups[0]["score"].as_f64().is_some_and(|s| s > 0.8));
        let dag = builder.build().unwrap();
        let statuses = [TaskStatus::Succeeded, TaskStatus::Succeeded];
        let outputs = [serde_json::json!({"score": 0.95}), Value::Null];
        assert_eq!(
            dag.tasks()[1].dispatch_decision(&statuses, &outputs),
            DagDispatchDecision::Run,
        );
    }

    #[test]
    fn dispatch_decision_upstream_outputs_in_declaration_order() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity);
        let b = builder.activity(dummy_activity2);
        // c depends on both a (index 0) and b (index 1), in that order
        let _c = builder
            .activity(dummy_activity3)
            .upstream(&a)
            .upstream(&b)
            .condition(|ups| {
                // ups[0] should be a's output, ups[1] should be b's output
                ups[0] == serde_json::json!("from_a") && ups[1] == serde_json::json!("from_b")
            });
        let dag = builder.build().unwrap();
        let statuses = [TaskStatus::Succeeded; 3];
        let outputs = [
            serde_json::json!("from_a"),
            serde_json::json!("from_b"),
            Value::Null,
        ];
        assert_eq!(
            dag.tasks()[2].dispatch_decision(&statuses, &outputs),
            DagDispatchDecision::Run,
        );
    }

    #[test]
    fn mapped_task_supports_condition() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity);
        let _b = builder
            .map_activity(dummy_activity2)
            .over(&a)
            .condition(|_| false); // always skip

        let dag = builder.build().unwrap();
        assert!(
            dag.tasks()[1].condition.is_some(),
            "mapped task should support condition"
        );
        let statuses = [TaskStatus::Succeeded, TaskStatus::Succeeded];
        let outputs = [serde_json::json!([1, 2, 3]), Value::Null];
        assert_eq!(
            dag.tasks()[1].dispatch_decision(&statuses, &outputs),
            DagDispatchDecision::SkipByCondition,
        );
    }

    // ── Issue #702 — DAG node input binding ────────────────────────────────

    #[test]
    fn input_from_stores_single_binding_and_adds_edge() {
        let mut builder = DagBuilder::new();
        let extract = builder.activity(dummy_activity); // idx 0
        let extract_idx = extract.index();
        // transform declares NO explicit `.upstream()` — the edge must be
        // auto-added by `input_from`.
        let transform = builder.activity(dummy_activity2); // idx 1
        let _ = transform.input_from(&extract);

        let dag = builder.build().unwrap();
        let tasks = dag.tasks();
        assert_eq!(
            tasks[1].input_from,
            Some(DagInputBinding::Single(extract_idx)),
            "single binding must be stored"
        );
        assert!(
            tasks[1].upstreams.contains(&extract_idx),
            "input_from must auto-add the dependency edge"
        );
    }

    #[test]
    fn input_from_all_merges_keyed_by_activity_name() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity); // 0 "dummy_activity"
        let b = builder.activity(dummy_activity2); // 1 "dummy_activity2"
        let c = builder.activity(dummy_activity3); // 2
        let _ = c.input_from_all(&[&a, &b]);

        let dag = builder.build().unwrap();
        let t = &dag.tasks()[2];
        assert_eq!(
            t.input_from,
            Some(DagInputBinding::Merged(vec![
                DagMergeSource {
                    key: "dummy_activity".to_owned(),
                    upstream_index: 0,
                },
                DagMergeSource {
                    key: "dummy_activity2".to_owned(),
                    upstream_index: 1,
                },
            ])),
            "input_from_all must key by upstream activity name in argument order"
        );
        assert!(t.upstreams.contains(&0) && t.upstreams.contains(&1));
    }

    #[test]
    fn input_from_aliased_merges_keyed_by_alias() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity); // 0
        let b = builder.activity(dummy_activity2); // 1
        let c = builder.activity(dummy_activity3); // 2
        let _ = c.input_from_aliased(&[("rows", &a), ("meta", &b)]);

        let dag = builder.build().unwrap();
        let t = &dag.tasks()[2];
        assert_eq!(
            t.input_from,
            Some(DagInputBinding::Merged(vec![
                DagMergeSource {
                    key: "rows".to_owned(),
                    upstream_index: 0,
                },
                DagMergeSource {
                    key: "meta".to_owned(),
                    upstream_index: 1,
                },
            ])),
            "input_from_aliased must key by the given alias"
        );
        assert!(t.upstreams.contains(&0) && t.upstreams.contains(&1));
    }

    #[test]
    fn input_from_all_duplicate_activity_name_is_build_error() {
        let mut builder = DagBuilder::new();
        // Two nodes using the SAME activity fn → same activity_name.
        let a = builder.activity(dummy_activity); // 0 "dummy_activity"
        let b = builder.activity(dummy_activity); // 1 "dummy_activity"
        let c = builder.activity(dummy_activity3); // 2
        let _ = c.input_from_all(&[&a, &b]);

        let err = builder.build().unwrap_err();
        assert!(
            matches!(
                err,
                DagBuildError::DuplicateInputBindingKey { ref key, .. } if key == "dummy_activity"
            ),
            "duplicate activity-name merge key must be a build error, got {err:?}"
        );
    }

    #[test]
    fn input_from_aliased_duplicate_alias_is_build_error() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity); // 0
        let b = builder.activity(dummy_activity2); // 1
        let c = builder.activity(dummy_activity3); // 2
        let _ = c.input_from_aliased(&[("k", &a), ("k", &b)]);

        let err = builder.build().unwrap_err();
        assert!(
            matches!(
                err,
                DagBuildError::DuplicateInputBindingKey { ref key, .. } if key == "k"
            ),
            "duplicate alias must be a build error, got {err:?}"
        );
    }

    #[test]
    fn input_from_on_mapped_node_is_conflict_error() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity); // 0
        let other = builder.activity(dummy_activity3); // 1
        // `.over()` sets map_upstream; `.input_from()` then sets input_from.
        let mapped = builder.map_activity(dummy_activity2).over(&a); // 2
        let _ = mapped.input_from(&other);

        let err = builder.build().unwrap_err();
        assert!(
            matches!(err, DagBuildError::ConflictingInputBinding { .. }),
            "input_from on a mapped node must conflict, got {err:?}"
        );
    }

    #[test]
    fn input_from_on_signal_gate_is_build_error() {
        let mut builder = DagBuilder::new();
        let up = builder.activity(dummy_activity); // 0
        let gate = builder.signal_gate("approval"); // 1
        // A gate dispatches no activity, so a binding would silently add a stray
        // dependency edge — reject it rather than swallowing it.
        let _ = gate.input_from(&up);

        let err = builder.build().unwrap_err();
        assert!(
            matches!(err, DagBuildError::InputBindingOnGate { .. }),
            "input_from on a signal gate must be a build error, got {err:?}"
        );
    }

    #[test]
    fn input_from_all_on_signal_gate_is_build_error() {
        let mut builder = DagBuilder::new();
        let up = builder.activity(dummy_activity); // 0
        let gate = builder.signal_gate("approval"); // 1
        let _ = gate.input_from_all(&[&up]);

        let err = builder.build().unwrap_err();
        assert!(
            matches!(err, DagBuildError::InputBindingOnGate { .. }),
            "input_from_all on a signal gate must be a build error, got {err:?}"
        );
    }

    #[test]
    fn input_from_all_empty_slice_is_build_error() {
        let mut builder = DagBuilder::new();
        let c = builder.activity(dummy_activity3); // 0
        let _ = c.input_from_all(&[]);

        let err = builder.build().unwrap_err();
        assert!(
            matches!(err, DagBuildError::EmptyInputBinding { .. }),
            "empty input_from_all must be a build error, got {err:?}"
        );
    }

    #[test]
    fn input_from_aliased_empty_slice_is_build_error() {
        let mut builder = DagBuilder::new();
        let c = builder.activity(dummy_activity3); // 0
        let _ = c.input_from_aliased(&[]);

        let err = builder.build().unwrap_err();
        assert!(
            matches!(err, DagBuildError::EmptyInputBinding { .. }),
            "empty input_from_aliased must be a build error, got {err:?}"
        );
    }

    #[test]
    fn input_from_after_upstream_preserves_condition_ups_order() {
        // Binding APPENDS its edge — it never reorders prior `.upstream()`
        // edges. Because `dispatch_decision` builds the condition `ups` slice by
        // iterating `upstreams` in order, builder call order determines the
        // `ups[..]` indexing a `.condition(|ups| ...)` sees.

        // Node built as `.upstream(&a).condition(...).input_from(&b)`:
        // a's edge is declared first, so `upstreams == [a, b]` and the
        // condition sees a's output at `ups[0]`.
        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity); // 0
        let b = builder.activity(dummy_activity2); // 1
        let _node = builder
            .activity(dummy_activity3) // 2
            .upstream(&a)
            .condition(|ups| ups[0] == serde_json::json!("a-out"))
            .input_from(&b);

        let dag = builder.build().unwrap();
        assert_eq!(
            dag.tasks()[2].upstreams,
            vec![0, 1],
            "explicit upstream(&a) precedes the binding edge to b — order preserved"
        );
        // Prove `ups[0]` is a's output via dispatch_decision.
        let statuses = [
            TaskStatus::Succeeded,
            TaskStatus::Succeeded,
            TaskStatus::Skipped,
        ];
        let outputs = [
            serde_json::json!("a-out"),
            serde_json::json!("b-out"),
            Value::Null,
        ];
        assert_eq!(
            dag.tasks()[2].dispatch_decision(&statuses, &outputs),
            DagDispatchDecision::Run,
            "condition must see a's output at ups[0] (edges: [a, b])"
        );

        // Same two edges declared in the OTHER call order:
        // `.input_from(&b).upstream(&a)` → binding edge to b is declared first,
        // so `upstreams == [b, a]` and the condition sees b's output at ups[0].
        let mut builder2 = DagBuilder::new();
        let a2 = builder2.activity(dummy_activity); // 0
        let b2 = builder2.activity(dummy_activity2); // 1
        let _node2 = builder2
            .activity(dummy_activity3) // 2
            .input_from(&b2)
            .upstream(&a2)
            .condition(|ups| ups[0] == serde_json::json!("b-out"));

        let dag2 = builder2.build().unwrap();
        assert_eq!(
            dag2.tasks()[2].upstreams,
            vec![1, 0],
            "binding edge to b precedes the explicit upstream(&a) edge — call order determines ups order"
        );
        assert_eq!(
            dag2.tasks()[2].dispatch_decision(&statuses, &outputs),
            DagDispatchDecision::Run,
            "condition must see b's output at ups[0] (edges: [b, a])"
        );
    }

    // ── Issue #780 — declarative DAG node compensation ─────────────────────

    // Compensator activity fn items — distinct fns so the typed
    // `.compensate(f)` derives distinct short names (same mechanism as
    // `DagBuilder::activity`).
    fn release_inventory() {}
    fn refund_payment() {}

    /// T1 — `.compensate(f)` (typed) derives the short activity name exactly
    /// like `DagBuilder::activity`; `.compensate_named(name)` stores the given
    /// string verbatim; an undecorated node carries `None`.
    #[test]
    fn compensate_sets_the_task_field() {
        let mut builder = DagBuilder::new();
        // Typed — name derived from the fn item.
        let _typed = builder
            .activity(dummy_activity)
            .compensate(release_inventory); // 0
        // Named — explicit string.
        let _named = builder
            .activity(dummy_activity2)
            .compensate_named("refund_payment"); // 1
        // Undecorated — no compensator declared.
        let _plain = builder.activity(dummy_activity3); // 2

        let dag = builder.build().expect("compensated DAG builds");
        let tasks = dag.tasks();
        assert_eq!(
            tasks[0].compensate.as_deref(),
            Some("release_inventory"),
            "typed `.compensate(f)` must derive the short activity name"
        );
        assert_eq!(
            tasks[1].compensate.as_deref(),
            Some("refund_payment"),
            "`.compensate_named(..)` must store the given name verbatim"
        );
        assert_eq!(
            tasks[2].compensate, None,
            "an undecorated node must carry no compensator"
        );
    }

    /// T2 — a signal gate dispatches no activity, so there is nothing for a
    /// compensator to undo; declaring one is a build error naming the gate.
    #[test]
    fn compensate_on_a_signal_gate_is_a_build_error() {
        let mut builder = DagBuilder::new();
        let root = builder.activity(dummy_activity); // 0
        let gate = builder.signal_gate("approval").upstream(&root); // 1
        let _ = gate.compensate_named("undo_approval");

        let err = builder.build().unwrap_err();
        assert!(
            matches!(
                err,
                DagBuildError::CompensateOnGate { ref task } if task == "approval"
            ),
            "compensate on a signal gate must be a build error naming the gate, got {err:?}"
        );
    }

    /// T3 — an empty compensator name would dispatch a nameless activity;
    /// reject it at build time rather than at unwind time.
    #[test]
    fn empty_compensator_name_is_a_build_error() {
        let mut builder = DagBuilder::new();
        let _ = builder.activity(dummy_activity).compensate_named("");

        let err = builder.build().unwrap_err();
        assert!(
            matches!(
                err,
                DagBuildError::EmptyCompensator { ref task } if task == "dummy_activity"
            ),
            "an empty compensator name must be a build error naming the node, got {err:?}"
        );
    }

    /// T4 — a compensator name that collides with a **forward node's** identity
    /// is rejected: the unwind dispatches compensators by name, so a collision
    /// makes the compensation indistinguishable from the forward node (and
    /// would make the DAG ambiguous for issue #366 retry-from-node).
    ///
    /// Three sub-cases: another node's name, the declaring node's own name, and
    /// a signal gate's name (a gate's identity is its signal name).
    #[test]
    fn compensator_named_after_a_forward_node_is_a_build_error() {
        // (a) Named after ANOTHER forward node.
        let mut b1 = DagBuilder::new();
        let _a = b1.activity(dummy_activity); // 0 "dummy_activity"
        let _b = b1
            .activity(dummy_activity2) // 1 "dummy_activity2"
            .compensate_named("dummy_activity");
        let err = b1.build().unwrap_err();
        assert!(
            matches!(
                err,
                DagBuildError::CompensatorNameCollidesWithNode { ref task, ref compensator }
                    if task == "dummy_activity2" && compensator == "dummy_activity"
            ),
            "a compensator named after another forward node must be rejected, got {err:?}"
        );

        // (b) Named after the DECLARING node itself.
        let mut b2 = DagBuilder::new();
        let _self_named = b2
            .activity(dummy_activity) // 0 "dummy_activity"
            .compensate_named("dummy_activity");
        let err = b2.build().unwrap_err();
        assert!(
            matches!(
                err,
                DagBuildError::CompensatorNameCollidesWithNode { ref task, ref compensator }
                    if task == "dummy_activity" && compensator == "dummy_activity"
            ),
            "a self-named compensator must be rejected, got {err:?}"
        );

        // (c) Named after a signal GATE (a gate's identity is its signal name).
        let mut b3 = DagBuilder::new();
        let _gate = b3.signal_gate("approval"); // 0, identity "approval"
        let _node = b3
            .activity(dummy_activity) // 1
            .compensate_named("approval");
        let err = b3.build().unwrap_err();
        assert!(
            matches!(
                err,
                DagBuildError::CompensatorNameCollidesWithNode { ref task, ref compensator }
                    if task == "dummy_activity" && compensator == "approval"
            ),
            "a compensator named after a signal gate must be rejected, got {err:?}"
        );
    }

    /// T5 — one compensator activity may be shared by several nodes (the
    /// envelope's `dag_compensate` field disambiguates which node it is undoing),
    /// so a shared compensator is NOT a collision.
    #[test]
    fn one_compensator_shared_by_several_nodes_builds_cleanly() {
        let mut builder = DagBuilder::new();
        let a = builder
            .activity(dummy_activity)
            .compensate(release_inventory); // 0
        let b = builder
            .activity(dummy_activity2)
            .upstream(&a)
            .compensate(release_inventory); // 1
        let _c = builder
            .activity(dummy_activity3)
            .upstream(&b)
            .compensate(release_inventory); // 2

        let dag = builder
            .build()
            .expect("one compensator may be shared by several nodes");
        for (i, task) in dag.tasks().iter().enumerate() {
            assert_eq!(
                task.compensate.as_deref(),
                Some("release_inventory"),
                "node {i} must keep the shared compensator"
            );
        }
        // A distinct compensator on a sibling DAG is likewise fine — the guard
        // is "collides with a forward NODE", not "used more than once".
        let mut other = DagBuilder::new();
        let _ = other.activity(dummy_activity).compensate(refund_payment);
        assert_eq!(
            other.build().unwrap().tasks()[0].compensate.as_deref(),
            Some("refund_payment")
        );
    }

    /// T6 (post-review P3-6) — "last call wins" is a documented contract, so pin
    /// it across all four `compensate*` orderings.
    #[test]
    fn repeated_compensate_calls_are_last_wins() {
        // typed -> typed
        let mut b1 = DagBuilder::new();
        let _ = b1
            .activity(dummy_activity)
            .compensate(release_inventory)
            .compensate(refund_payment);
        assert_eq!(
            b1.build().unwrap().tasks()[0].compensate.as_deref(),
            Some("refund_payment"),
            "a later typed `.compensate` must replace the earlier one"
        );

        // typed -> named
        let mut b2 = DagBuilder::new();
        let _ = b2
            .activity(dummy_activity)
            .compensate(release_inventory)
            .compensate_named("undo_by_name");
        assert_eq!(
            b2.build().unwrap().tasks()[0].compensate.as_deref(),
            Some("undo_by_name"),
            "`.compensate_named` must replace an earlier typed `.compensate`"
        );

        // named -> typed
        let mut b3 = DagBuilder::new();
        let _ = b3
            .activity(dummy_activity)
            .compensate_named("undo_by_name")
            .compensate(refund_payment);
        assert_eq!(
            b3.build().unwrap().tasks()[0].compensate.as_deref(),
            Some("refund_payment"),
            "a typed `.compensate` must replace an earlier `.compensate_named`"
        );

        // named -> named
        let mut b4 = DagBuilder::new();
        let _ = b4
            .activity(dummy_activity)
            .compensate_named("first_undo")
            .compensate_named("second_undo");
        assert_eq!(
            b4.build().unwrap().tasks()[0].compensate.as_deref(),
            Some("second_undo"),
            "a later `.compensate_named` must replace the earlier one"
        );
    }

    /// T7 (post-review P3-7) — `compensate_named` TRIMS. Storing a padded name
    /// verbatim would dispatch `" undo "`, which resolves to no registered
    /// activity, while `EmptyCompensator` already judges emptiness on the
    /// trimmed form — an inconsistency that only surfaced mid-unwind.
    #[test]
    fn compensate_named_trims_surrounding_whitespace() {
        let mut builder = DagBuilder::new();
        let _ = builder
            .activity(dummy_activity)
            .compensate_named("  release_inventory\t\n");
        assert_eq!(
            builder.build().unwrap().tasks()[0].compensate.as_deref(),
            Some("release_inventory"),
            "`compensate_named` must trim so the dispatched name matches the \
             registered activity"
        );

        // A whitespace-only name is still the empty-name build error, and the
        // trim must not turn it into a silently-accepted `""`.
        let mut ws = DagBuilder::new();
        let _ = ws.activity(dummy_activity).compensate_named("   ");
        assert!(
            matches!(
                ws.build().unwrap_err(),
                DagBuildError::EmptyCompensator { ref task } if task == "dummy_activity"
            ),
            "a whitespace-only compensator name must still be a build error"
        );

        // Trimming must not create a NEW collision blind spot: a padded name
        // that trims onto a forward node is still rejected.
        let mut collide = DagBuilder::new();
        let _a = collide.activity(dummy_activity);
        let _b = collide
            .activity(dummy_activity2)
            .compensate_named(" dummy_activity ");
        assert!(
            matches!(
                collide.build().unwrap_err(),
                DagBuildError::CompensatorNameCollidesWithNode { ref compensator, .. }
                    if compensator == "dummy_activity"
            ),
            "a padded name that trims onto a forward node must still collide"
        );
    }
}
