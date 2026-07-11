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
}

impl fmt::Display for DagBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CycleDetected => f.write_str("dag contains a dependency cycle"),
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

        // Signal/timer gate isolation (issue #746): the worker requires a
        // homogeneous suspension batch, so a gate's `WaitForSignal` command must
        // never share a level with a sibling activity's `ScheduleActivity`.
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

    let n = tasks.len();
    let mut statuses: Vec<TaskStatus> = vec![TaskStatus::Skipped; n];
    let mut outputs: Vec<Value> = vec![Value::Null; n];

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
            Pin<Box<dyn Future<Output = Result<(usize, TaskStatus, Value), String>> + Send + '_>>,
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
                let policy = tasks[task_idx].map_failure_policy;
                let activity_name_clone = activity_name.clone();
                let queue_str_clone = queue_str.clone();
                let retry_override_clone = retry_override.clone();
                let stc_override_clone = stc_override;
                activity_futs.push(Box::pin(async move {
                    let Value::Array(array) = &upstream_val else {
                        return Err("mapped upstream output is not a JSON array".to_owned());
                    };
                    let n_instances = array.len();
                    if n_instances == 0 {
                        return Ok((task_idx, TaskStatus::Succeeded, Value::Array(Vec::new())));
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

                    if policy == MapFailurePolicy::FailFast {
                        use futures::StreamExt as _;
                        let mut stream = instance_futs
                            .into_iter()
                            .collect::<futures::stream::FuturesUnordered<_>>();
                        while let Some((i, res)) = stream.next().await {
                            match res {
                                Ok(v) => results[i] = v,
                                Err(err) => match &err {
                                    HarvestError::ActivityFailed { .. }
                                    | HarvestError::Timeout { .. } => {
                                        status = TaskStatus::Failed;
                                        drop(stream);
                                        break;
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
                                    _ => return Err(err.to_string()),
                                },
                            };
                            collect_results[i] = obj;
                        }
                        status = TaskStatus::Succeeded;
                        final_val = Value::Array(collect_results);
                    }

                    Ok::<_, String>((task_idx, status, final_val))
                }));
            } else {
                let mapped_up = upstreams
                    .iter()
                    .copied()
                    .find(|&i| tasks[i].map_upstream.is_some());
                let activity_input = mapped_up.map_or_else(
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
                );

                activity_futs.push(Box::pin(async move {
                    let (status, val) = match ctx
                        .execute_activity_raw_with_opts(
                            &activity_name,
                            activity_input,
                            &queue_str,
                            retry_override,
                            stc_override,
                        )
                        .await
                    {
                        Ok(v) => (TaskStatus::Succeeded, v),
                        Err(
                            HarvestError::ActivityFailed { .. } | HarvestError::Timeout { .. },
                        ) => (TaskStatus::Failed, Value::Null),
                        Err(error) => return Err(error.to_string()),
                    };
                    Ok::<_, String>((task_idx, status, val))
                }));
            }
        }

        for activity_result in futures::future::join_all(activity_futs).await {
            let (task_idx, status, val) = activity_result?;
            statuses[task_idx] = status;
            outputs[task_idx] = val;
        }
    }

    if statuses.iter().any(|s| matches!(s, TaskStatus::Failed)) {
        return Err("one or more DAG tasks failed".to_owned());
    }

    Ok(Value::Null)
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
}
