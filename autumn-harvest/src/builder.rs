//! Fluent API for registering workflows, activities, and configuring the worker.

use std::time::Duration;

use crate::info::{ActivityInfo, WorkflowInfo};

/// Fluent builder for configuring the autumn-harvest engine.
///
/// In a full Autumn app, this is consumed by the `HarvestExt` trait on
/// `AppBuilder`. In tests or standalone use, call `.build()` directly.
#[derive(Default, Debug)]
pub struct HarvestBuilder {
    workflows: Vec<WorkflowInfo>,
    activities: Vec<ActivityInfo>,
    worker_config: WorkerConfig,
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

    /// Configure the worker (concurrency, queues, timeouts).
    #[must_use]
    pub fn worker(mut self, config: WorkerConfig) -> Self {
        self.worker_config = config;
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
}

/// Worker concurrency and queue configuration.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Queues this worker polls. Defaults to `["default"]`.
    pub queues: Vec<String>,
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
    #[must_use]
    pub fn with_queues<'a>(mut self, queues: impl IntoIterator<Item = &'a str>) -> Self {
        self.queues = queues.into_iter().map(str::to_owned).collect();
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::info::WorkflowInfo;

    fn fake_workflow_info() -> WorkflowInfo {
        WorkflowInfo {
            name: "test",
            module: "test",
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
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
    }

    #[test]
    fn worker_config_builder_adds_queues() {
        let config = WorkerConfig::default().with_queues(["email-workers", "etl"]);
        assert!(config.queues.contains(&"email-workers".to_string()));
    }
}
