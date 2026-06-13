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
        let upstream_statuses: Vec<TaskStatus> =
            self.upstreams.iter().map(|&i| statuses[i]).collect();
        if !self.trigger_rule.should_run(&upstream_statuses) {
            return DagDispatchDecision::SkipByTriggerRule;
        }
        // Trigger rule passed — evaluate the condition if present.
        if let Some(cond) = &self.condition {
            let upstream_outputs: Vec<Value> =
                self.upstreams.iter().map(|&i| outputs[i].clone()).collect();
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
        });

        DagMapTaskRef {
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
