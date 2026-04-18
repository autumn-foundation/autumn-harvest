//! Fluent API for registering workflows, activities, and configuring the worker.

use std::any::{Any, TypeId};
use std::time::Duration;

use crate::context::SharedStateMap;
use crate::info::{ActivityInfo, DagInfo, WorkflowInfo};

/// Fluent builder for configuring the autumn-harvest engine.
///
/// In a full Autumn app, this is consumed by `HarvestPlugin` from the
/// `autumn-harvest-plugin` crate. In tests or standalone use, call
/// `.build()` directly.
#[derive(Default)]
pub struct HarvestBuilder {
    workflows: Vec<WorkflowInfo>,
    activities: Vec<ActivityInfo>,
    dags: Vec<DagInfo>,
    worker_config: WorkerConfig,
    state: SharedStateMap,
}

impl std::fmt::Debug for HarvestBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HarvestBuilder")
            .field("workflow_count", &self.workflows.len())
            .field("activity_count", &self.activities.len())
            .field("dag_count", &self.dags.len())
            .field("worker_config", &self.worker_config)
            .field("state_count", &self.state.len())
            .finish()
    }
}

/// Built harvest registration set produced by [`HarvestBuilder::build`].
pub struct BuiltHarvest {
    workflows: Vec<WorkflowInfo>,
    activities: Vec<ActivityInfo>,
    dags: Vec<DagInfo>,
    worker_config: WorkerConfig,
    state: SharedStateMap,
}

impl std::fmt::Debug for BuiltHarvest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BuiltHarvest")
            .field("workflow_count", &self.workflows.len())
            .field("activity_count", &self.activities.len())
            .field("dag_count", &self.dags.len())
            .field("worker_config", &self.worker_config)
            .field("state_count", &self.state.len())
            .finish()
    }
}

impl BuiltHarvest {
    /// Number of registered workflows.
    #[must_use]
    pub fn workflow_count(&self) -> usize {
        self.workflows.len()
    }

    /// Number of registered activities.
    #[must_use]
    pub fn activity_count(&self) -> usize {
        self.activities.len()
    }

    /// Number of registered DAGs.
    #[must_use]
    pub fn dag_count(&self) -> usize {
        self.dags.len()
    }

    /// Access typed shared state registered on the builder.
    #[must_use]
    pub fn state<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.state.get(&TypeId::of::<T>())?.downcast_ref::<T>()
    }

    /// Worker configuration carried through the build step.
    #[must_use]
    pub const fn worker_config(&self) -> &WorkerConfig {
        &self.worker_config
    }

    /// Registered DAG metadata.
    #[must_use]
    pub fn dags(&self) -> &[DagInfo] {
        &self.dags
    }

    /// Convert the built harvest registration into worker-ready parts.
    #[cfg(feature = "db")]
    #[must_use]
    pub fn into_worker_parts(self) -> (crate::worker::HandlerRegistry, Vec<DagInfo>, WorkerConfig) {
        (
            crate::worker::HandlerRegistry::with_state(
                self.workflows,
                self.activities,
                std::sync::Arc::new(self.state),
            ),
            self.dags,
            self.worker_config,
        )
    }

    /// Convert the built harvest registration into worker-ready parts while
    /// injecting additional typed runtime state.
    #[cfg(feature = "db")]
    #[must_use]
    pub fn into_worker_parts_with_extra_state(
        mut self,
        extra_state: SharedStateMap,
    ) -> (crate::worker::HandlerRegistry, Vec<DagInfo>, WorkerConfig) {
        self.state.extend(extra_state);
        (
            crate::worker::HandlerRegistry::with_state(
                self.workflows,
                self.activities,
                std::sync::Arc::new(self.state),
            ),
            self.dags,
            self.worker_config,
        )
    }
}

impl HarvestBuilder {
    /// Create a new empty builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register workflow definitions (output of `workflows![]` macro).
    #[must_use]
    pub fn workflows(mut self, workflows: Vec<WorkflowInfo>) -> Self {
        self.workflows.extend(workflows);
        self
    }

    /// Register activity definitions (output of `activities![]` macro).
    #[must_use]
    pub fn activities(mut self, activities: Vec<ActivityInfo>) -> Self {
        self.activities.extend(activities);
        self
    }

    /// Register DAG definitions (output of `dags![]` macro).
    #[must_use]
    pub fn dags(mut self, dags: Vec<DagInfo>) -> Self {
        self.dags.extend(dags);
        self
    }

    /// Configure the worker (concurrency, queues, timeouts).
    #[must_use]
    pub fn worker(mut self, config: WorkerConfig) -> Self {
        self.worker_config = config;
        self
    }

    /// Register typed shared state visible to workflow and activity handlers.
    ///
    /// Registering the same type more than once replaces the previous value.
    #[must_use]
    pub fn state<T: Any + Send + Sync>(mut self, value: T) -> Self {
        self.state.insert(TypeId::of::<T>(), Box::new(value));
        self
    }

    /// Number of registered workflows (used in tests and diagnostics).
    #[must_use]
    pub fn workflow_count(&self) -> usize {
        self.workflows.len()
    }

    /// Number of registered activities.
    #[must_use]
    pub fn activity_count(&self) -> usize {
        self.activities.len()
    }

    /// Number of registered DAG definitions.
    #[must_use]
    pub fn dag_count(&self) -> usize {
        self.dags.len()
    }

    /// Finalize the builder into a reusable harvest registration set.
    #[must_use]
    pub fn build(self) -> BuiltHarvest {
        BuiltHarvest {
            workflows: self.workflows,
            activities: self.activities,
            dags: self.dags,
            worker_config: self.worker_config,
            state: self.state,
        }
    }
}

/// Worker concurrency and queue configuration.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Queues this worker polls. Defaults to `["default"]`.
    pub queues: Vec<String>,
    /// Optional Postgres URL for LISTEN/NOTIFY wakeups.
    pub notification_database_url: Option<String>,
    /// Maximum concurrent workflow executions on this worker.
    pub max_concurrent_workflows: usize,
    /// Maximum concurrent activity executions on this worker.
    pub max_concurrent_activities: usize,
    /// Graceful shutdown timeout.
    pub shutdown_timeout: Duration,
    /// Maximum cached in-memory workflow states (LRU eviction).
    pub workflow_cache_size: usize,
    /// How long to offer sticky tasks to the sticky worker before fallback.
    pub sticky_timeout: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            queues: vec!["default".to_string()],
            notification_database_url: None,
            max_concurrent_workflows: 20,
            max_concurrent_activities: 50,
            shutdown_timeout: Duration::from_secs(30),
            workflow_cache_size: 1000,
            sticky_timeout: Duration::from_secs(5),
        }
    }
}

impl WorkerConfig {
    /// Replace the queue list.
    ///
    /// # Panics
    ///
    /// Panics if any of the provided queue names are empty strings.
    #[must_use]
    pub fn with_queues<'a>(mut self, queues: impl IntoIterator<Item = &'a str>) -> Self {
        self.queues = queues
            .into_iter()
            .map(|q| {
                assert!(!q.is_empty(), "queue name cannot be empty");
                q.to_owned()
            })
            .collect();
        self
    }

    /// Enable LISTEN/NOTIFY wakeups using a dedicated Postgres connection.
    #[must_use]
    pub fn with_notification_database_url(mut self, database_url: impl Into<String>) -> Self {
        self.notification_database_url = Some(database_url.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::DagBuilder;
    use crate::info::{DagInfo, WorkflowInfo};
    use crate::policy::Schedule;

    fn fake_workflow_info() -> WorkflowInfo {
        WorkflowInfo {
            name: "test",
            module: "test",
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        }
    }

    fn fake_dag_info() -> DagInfo {
        fn build(_dag: &mut DagBuilder) {}

        DagInfo {
            name: "daily_etl",
            module: "test",
            schedule: Some(Schedule::Manual),
            catchup: false,
            max_active_runs: 1,
            default_queue: Some("default"),
            builder: build,
        }
    }

    #[test]
    fn harvest_builder_collects_workflows() {
        let builder = HarvestBuilder::new().workflows(vec![fake_workflow_info()]);
        assert_eq!(builder.workflow_count(), 1);
    }

    #[test]
    fn worker_config_default_queues() {
        let config = WorkerConfig::default();
        assert!(config.queues.contains(&"default".to_string()));
        assert!(config.notification_database_url.is_none());
    }

    #[test]
    fn worker_config_builder_adds_queues() {
        let config = WorkerConfig::default().with_queues(["email-workers", "etl"]);
        assert!(config.queues.contains(&"email-workers".to_string()));
    }

    #[test]
    fn worker_config_with_empty_queues_clears_list() {
        let config = WorkerConfig::default().with_queues(Vec::<&str>::new());
        assert!(config.queues.is_empty());
    }

    #[test]
    fn worker_config_builder_sets_notification_database_url() {
        let config =
            WorkerConfig::default().with_notification_database_url("postgres://localhost/test");
        assert_eq!(
            config.notification_database_url.as_deref(),
            Some("postgres://localhost/test")
        );
    }

    #[test]
    fn harvest_builder_collects_dags() {
        let builder = HarvestBuilder::new().dags(vec![fake_dag_info()]);
        assert_eq!(builder.dag_count(), 1);
    }

    #[test]
    fn harvest_builder_build_registers_shared_state() {
        let built = HarvestBuilder::new().state(String::from("hello")).build();

        assert_eq!(built.workflow_count(), 0);
        assert_eq!(built.activity_count(), 0);
        assert_eq!(built.dag_count(), 0);
        assert_eq!(built.state::<String>(), Some(&String::from("hello")));
        assert!(built.state::<u64>().is_none());
    }

    #[cfg(feature = "db")]
    #[test]
    fn built_harvest_into_worker_parts_preserves_shared_state() {
        let built = HarvestBuilder::new()
            .workflows(vec![fake_workflow_info()])
            .activities(vec![ActivityInfo {
                name: "test_activity",
                module: "test",
                default_retry_policy: None,
                default_start_to_close: None,
                default_heartbeat_timeout: None,
                default_schedule_to_start: None,
                default_queue: None,
                handler: |_ctx, input| Box::pin(async move { Ok(input) }),
            }])
            .state(String::from("haunted"))
            .build();

        let (registry, _dags, worker_config) = built.into_worker_parts();

        assert_eq!(registry.state::<String>(), Some(&String::from("haunted")));
        assert!(worker_config.queues.contains(&"default".to_string()));
    }

    #[test]
    #[should_panic(expected = "queue name cannot be empty")]
    fn worker_config_with_empty_queue_name_panics() {
        let _config = WorkerConfig::default().with_queues(["", "default"]);
    }

    #[test]
    fn worker_config_with_empty_iterator_clears_queues() {
        let config = WorkerConfig::default().with_queues(Vec::<&str>::new());
        assert!(config.queues.is_empty());
    }
}
