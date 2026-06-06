//! DAG definition primitives for Harvest.
//!
//! DAGs are compiled in memory into immutable execution metadata. Runtime
//! scheduling can consume the resulting [`DagDefinition`] without rebuilding
//! edges or dependency levels.

use std::any::type_name_of_val;
use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;
use std::time::Duration;

use crate::policy::{MapFailurePolicy, RetryPolicy, TriggerRule};

#[derive(Debug, Clone)]
struct PendingDagTask {
    activity_name: String,
    upstreams: Vec<usize>,
    trigger_rule: TriggerRule,
    retry_policy: Option<RetryPolicy>,
    start_to_close: Option<Duration>,
    queue: Option<String>,
    map_upstream: Option<usize>,
    map_failure_policy: MapFailurePolicy,
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
}
