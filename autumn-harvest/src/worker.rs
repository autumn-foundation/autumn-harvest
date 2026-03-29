//! Worker runtime — the main poll loop that claims and dispatches tasks.
//!
//! Each [`Worker`] runs a `tokio::select!`-driven loop: it either receives a
//! shutdown signal or polls the task queue for work. Claimed tasks are dispatched
//! via Tokio tasks bounded by semaphores so that at most
//! `max_concurrent_workflows` workflow tasks and `max_concurrent_activities`
//! activity tasks run concurrently on a single worker.
//!
//! The worker is deliberately "dumb" — it claims a row, looks up the handler in
//! the [`HandlerRegistry`], and spawns a task. The actual execution semantics
//! (replay, retries, heartbeats) live in the executor and context modules.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::builder::WorkerConfig;
use crate::error::{HarvestError, HarvestResult};
use crate::info::{ActivityInfo, WorkflowInfo};
use crate::models::TaskQueueItem;
use crate::queue;

/// Type alias for the deadpool-managed async Diesel connection pool.
pub type DbPool = deadpool::managed::Pool<
    diesel_async::pooled_connection::AsyncDieselConnectionManager<diesel_async::AsyncPgConnection>,
>;

// ---------------------------------------------------------------------------
// WorkerRuntimeConfig
// ---------------------------------------------------------------------------

/// Validated, runtime-ready worker configuration.
///
/// Built from [`WorkerConfig`] (the user-facing builder) via `From`, which
/// auto-generates a unique worker ID.
#[derive(Debug, Clone)]
pub struct WorkerRuntimeConfig {
    /// Unique identifier for this worker instance.
    pub worker_id: String,
    /// Queue names this worker polls.
    pub queues: Vec<String>,
    /// Maximum concurrent workflow task executions.
    pub max_concurrent_workflows: usize,
    /// Maximum concurrent activity task executions.
    pub max_concurrent_activities: usize,
    /// Interval between queue poll attempts when idle.
    pub poll_interval: Duration,
    /// Maximum time to wait for in-flight tasks during shutdown.
    pub shutdown_timeout: Duration,
}

impl WorkerRuntimeConfig {
    /// Validate this configuration.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Config`] if `queues` is empty.
    pub fn validate(&self) -> HarvestResult<()> {
        if self.queues.is_empty() {
            return Err(HarvestError::Config(
                "worker must poll at least one queue".into(),
            ));
        }
        Ok(())
    }
}

impl From<WorkerConfig> for WorkerRuntimeConfig {
    fn from(cfg: WorkerConfig) -> Self {
        Self {
            worker_id: uuid::Uuid::new_v4().to_string(),
            queues: cfg.queues,
            max_concurrent_workflows: cfg.max_concurrent_workflows,
            max_concurrent_activities: cfg.max_concurrent_activities,
            poll_interval: Duration::from_millis(500),
            shutdown_timeout: cfg.shutdown_timeout,
        }
    }
}

// ---------------------------------------------------------------------------
// HandlerRegistry
// ---------------------------------------------------------------------------

/// Fast name-to-handler lookup for workflows and activities.
///
/// Built once at startup from the vectors produced by the `workflows![]` and
/// `activities![]` macros, then shared via `Arc` across all poll iterations.
pub struct HandlerRegistry {
    /// Workflow handlers indexed by name.
    pub workflows: HashMap<String, WorkflowInfo>,
    /// Activity handlers indexed by name.
    pub activities: HashMap<String, ActivityInfo>,
}

impl HandlerRegistry {
    /// Create a new registry, indexing handlers by their `name` field.
    #[must_use]
    pub fn new(workflows: Vec<WorkflowInfo>, activities: Vec<ActivityInfo>) -> Self {
        let workflows = workflows
            .into_iter()
            .map(|w| (w.name.to_string(), w))
            .collect();
        let activities = activities
            .into_iter()
            .map(|a| (a.name.to_string(), a))
            .collect();
        Self {
            workflows,
            activities,
        }
    }
}

impl std::fmt::Debug for HandlerRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HandlerRegistry")
            .field("workflows", &self.workflows.keys().collect::<Vec<_>>())
            .field("activities", &self.activities.keys().collect::<Vec<_>>())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

/// The worker runtime that polls the task queue and dispatches work.
#[derive(Debug)]
pub struct Worker {
    /// Validated runtime configuration.
    pub config: WorkerRuntimeConfig,
    /// Shared handler registry.
    pub registry: Arc<HandlerRegistry>,
    /// Bounds concurrent workflow task executions.
    workflow_semaphore: Arc<Semaphore>,
    /// Bounds concurrent activity task executions.
    activity_semaphore: Arc<Semaphore>,
    /// Cancellation token for graceful shutdown.
    shutdown: CancellationToken,
}

impl Worker {
    /// Create a new worker from validated config and a handler registry.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Config`] if the config fails validation.
    pub fn new(config: WorkerRuntimeConfig, registry: Arc<HandlerRegistry>) -> HarvestResult<Self> {
        config.validate()?;

        let workflow_semaphore = Arc::new(Semaphore::new(config.max_concurrent_workflows));
        let activity_semaphore = Arc::new(Semaphore::new(config.max_concurrent_activities));

        Ok(Self {
            config,
            registry,
            workflow_semaphore,
            activity_semaphore,
            shutdown: CancellationToken::new(),
        })
    }

    /// Run the main poll loop until shutdown is requested.
    ///
    /// This is the worker's entry point. It alternates between polling the
    /// queue and checking the shutdown token via `tokio::select!`.
    pub async fn run(&self, pool: &DbPool) {
        tracing::info!(
            worker_id = %self.config.worker_id,
            queues = ?self.config.queues,
            "worker starting"
        );

        loop {
            tokio::select! {
                () = self.shutdown.cancelled() => {
                    tracing::info!(worker_id = %self.config.worker_id, "shutdown signal received");
                    break;
                }
                () = self.poll_once(pool) => {}
            }
        }

        tracing::info!(worker_id = %self.config.worker_id, "draining in-flight tasks");
        self.drain_in_flight().await;
        tracing::info!(worker_id = %self.config.worker_id, "worker stopped");
    }

    /// Execute a single poll iteration.
    ///
    /// Gets a connection from the pool, tries to claim a task, dispatches it
    /// if found, or sleeps for `poll_interval` if the queue was empty.
    async fn poll_once(&self, pool: &DbPool) {
        let mut conn = match pool.get().await {
            Ok(conn) => conn,
            Err(e) => {
                tracing::error!(error = %e, "failed to get connection from pool");
                tokio::time::sleep(self.config.poll_interval).await;
                return;
            }
        };

        match queue::claim_task(&mut conn, &self.config.queues, &self.config.worker_id).await {
            Ok(Some(task)) => {
                tracing::debug!(
                    task_id = %task.id,
                    task_type = %task.task_type,
                    queue = %task.queue_name,
                    "claimed task"
                );
                self.dispatch_task(task, pool);
            }
            Ok(None) => {
                tokio::time::sleep(self.config.poll_interval).await;
            }
            Err(e) => {
                tracing::error!(error = %e, "failed to claim task");
                tokio::time::sleep(self.config.poll_interval).await;
            }
        }
    }

    /// Spawn a bounded Tokio task for the claimed work item.
    ///
    /// For now this is a stub that logs and marks the task completed. The full
    /// wiring to the executor (replay engine, activity context) comes in
    /// integration tests.
    fn dispatch_task(&self, task: TaskQueueItem, pool: &DbPool) {
        let semaphore = if task.task_type == "WORKFLOW" {
            Arc::clone(&self.workflow_semaphore)
        } else {
            Arc::clone(&self.activity_semaphore)
        };

        let pool = pool.clone();
        let task_id = task.id;
        let task_type = task.task_type;
        let worker_id = self.config.worker_id.clone();

        tokio::spawn(async move {
            // Acquire semaphore permit — blocks if at concurrency limit.
            let Ok(_permit) = semaphore.acquire().await else {
                tracing::error!(task_id = %task_id, "semaphore closed");
                return;
            };

            tracing::info!(
                task_id = %task_id,
                task_type = %task_type,
                worker_id = %worker_id,
                "executing task (stub — full wiring in integration)"
            );

            // Stub: mark completed with empty output.
            let Ok(mut conn) = pool.get().await else {
                tracing::error!(task_id = %task_id, "failed to get connection for completion");
                return;
            };

            if let Err(e) = queue::complete_task(&mut conn, task_id, serde_json::json!(null)).await
            {
                tracing::error!(task_id = %task_id, error = %e, "failed to complete task");
            }
        });
    }

    /// Wait for all in-flight tasks to finish (or timeout).
    ///
    /// We wait until all semaphore permits are available again, meaning all
    /// spawned tasks have completed and dropped their permits.
    #[allow(clippy::cast_possible_truncation)] // concurrency limits are well under u32::MAX
    async fn drain_in_flight(&self) {
        let total_permits =
            self.config.max_concurrent_workflows + self.config.max_concurrent_activities;

        let drain = async {
            // Try to acquire ALL permits — when we can, all in-flight tasks are done.
            let _wf = self
                .workflow_semaphore
                .acquire_many(self.config.max_concurrent_workflows as u32)
                .await;
            let _act = self
                .activity_semaphore
                .acquire_many(self.config.max_concurrent_activities as u32)
                .await;
        };

        if tokio::time::timeout(self.config.shutdown_timeout, drain)
            .await
            .is_err()
        {
            tracing::warn!(
                worker_id = %self.config.worker_id,
                total_permits,
                "shutdown timeout elapsed — some tasks may still be running"
            );
        }
    }

    /// Request graceful shutdown of this worker.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

// ---------------------------------------------------------------------------
// Tests (unit, no DB)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn default_runtime_config() -> WorkerRuntimeConfig {
        WorkerRuntimeConfig {
            worker_id: "test-worker-1".to_string(),
            queues: vec!["default".to_string()],
            max_concurrent_workflows: 10,
            max_concurrent_activities: 20,
            poll_interval: Duration::from_millis(100),
            shutdown_timeout: Duration::from_secs(5),
        }
    }

    #[test]
    fn worker_config_validates() {
        let cfg = default_runtime_config();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn worker_config_rejects_empty_queues() {
        let cfg = WorkerRuntimeConfig {
            queues: vec![],
            ..default_runtime_config()
        };
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("queue"));
    }

    #[test]
    fn worker_config_from_builder() {
        let builder_cfg = WorkerConfig {
            queues: vec!["email".to_string(), "billing".to_string()],
            max_concurrent_workflows: 5,
            max_concurrent_activities: 15,
            shutdown_timeout: Duration::from_secs(60),
            workflow_cache_size: 500,
            sticky_timeout: Duration::from_secs(3),
        };

        let runtime_cfg: WorkerRuntimeConfig = builder_cfg.into();

        assert_eq!(runtime_cfg.queues, vec!["email", "billing"]);
        assert_eq!(runtime_cfg.max_concurrent_workflows, 5);
        assert_eq!(runtime_cfg.max_concurrent_activities, 15);
        assert_eq!(runtime_cfg.shutdown_timeout, Duration::from_secs(60));
        assert_eq!(runtime_cfg.poll_interval, Duration::from_millis(500));
        // worker_id should be a valid UUID
        assert!(uuid::Uuid::parse_str(&runtime_cfg.worker_id).is_ok());
    }

    #[test]
    fn handler_registry_indexes_by_name() {
        let wf = WorkflowInfo {
            name: "onboarding",
            module: "app::workflows",
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        };

        let act = ActivityInfo {
            name: "send_email",
            module: "app::activities",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_queue: None,
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        };

        let registry = HandlerRegistry::new(vec![wf], vec![act]);

        assert!(registry.workflows.contains_key("onboarding"));
        assert!(registry.activities.contains_key("send_email"));
        assert!(!registry.workflows.contains_key("nonexistent"));
    }

    #[test]
    fn worker_rejects_invalid_config() {
        let cfg = WorkerRuntimeConfig {
            queues: vec![],
            ..default_runtime_config()
        };
        let registry = Arc::new(HandlerRegistry::new(vec![], vec![]));
        assert!(Worker::new(cfg, registry).is_err());
    }

    #[test]
    fn worker_creates_with_valid_config() {
        let cfg = default_runtime_config();
        let registry = Arc::new(HandlerRegistry::new(vec![], vec![]));
        let worker = Worker::new(cfg, registry);
        assert!(worker.is_ok());
    }

    #[test]
    fn worker_shutdown_cancels_token() {
        let cfg = default_runtime_config();
        let registry = Arc::new(HandlerRegistry::new(vec![], vec![]));
        let worker = Worker::new(cfg, registry).unwrap();

        assert!(!worker.shutdown.is_cancelled());
        worker.shutdown();
        assert!(worker.shutdown.is_cancelled());
    }
}
