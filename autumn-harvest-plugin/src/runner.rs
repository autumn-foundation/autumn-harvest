//! Reusable Harvest runtime ownership for standalone or embedded processes.

use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use autumn_harvest::BuiltHarvest;
use autumn_harvest::batch::{BatchExecutorConfig, run_executor_once};
use autumn_harvest::context::SharedStateMap;
use autumn_harvest::policy::WorkflowSchedule;
use autumn_harvest::retention::{RetentionConfig, RetentionRuntime};
use autumn_harvest::scheduler::{
    DagCatalog, SchedulerMonitor, SchedulerRuntime, compile_dag_catalog,
};
use autumn_harvest::shard::{ShardRouter, ShardedDbPool};
use autumn_harvest::types::ShardId;
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_web::AppState;
use autumn_web::error::AutumnError;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::api::{HarvestApiRuntime, HarvestRetentionRuntime};
use crate::config::HarvestRuntimeConfig;
use crate::state::{AppDbPool, HarvestDbPool};

/// Resource bundle used to start a Harvest runtime outside `HarvestExt`.
///
/// The Harvest storage pool is required. Application state and an application
/// database pool are optional, but should be provided when activities or
/// workflows need access to app-owned state or business tables.
#[derive(Clone)]
pub struct HarvestRunnerResources {
    app_state: Option<AppState>,
    app_pool: Option<DbPool>,
    harvest_pool: DbPool,
    shard_router: Option<ShardRouter>,
    /// Pre-built multi-shard pool for multi-shard deployments (issue #522).
    ///
    /// When set, the runtime uses this pool directly instead of deriving a
    /// single-shard pool from `harvest_pool`. This allows operators to inject
    /// a `ShardedDbPool` spanning multiple databases so the worker can drain
    /// all assigned shards. When `None`, the runtime falls back to a
    /// single-shard wrapper around `harvest_pool`.
    sharded_pool: Option<ShardedDbPool>,
}

impl HarvestRunnerResources {
    /// Create a new resource bundle with the required Harvest storage pool.
    #[must_use]
    pub const fn new(harvest_pool: DbPool) -> Self {
        Self {
            app_state: None,
            app_pool: None,
            harvest_pool,
            shard_router: None,
            sharded_pool: None,
        }
    }

    /// Inject application state for workflows or activities that expect it.
    #[must_use]
    pub fn with_app_state(mut self, app_state: AppState) -> Self {
        self.app_state = Some(app_state);
        self
    }

    /// Inject the application/business database role for workflow code that
    /// touches app tables directly.
    #[must_use]
    pub fn with_app_pool(mut self, app_pool: DbPool) -> Self {
        self.app_pool = Some(app_pool);
        self
    }

    /// Inject the shard router the runtime should use for new-workflow
    /// placement and read-side routing decisions.
    ///
    /// When omitted the runtime defaults to the single-shard router.
    #[must_use]
    pub fn with_shard_router(mut self, router: ShardRouter) -> Self {
        self.shard_router = Some(router);
        self
    }

    /// Inject a pre-built multi-shard pool for multi-shard deployments.
    ///
    /// When set, this pool is used as the `storage_pool` instead of deriving
    /// a single-shard pool from `harvest_pool`. Use this when the runtime
    /// spans multiple Postgres databases and needs to drain tasks from all
    /// assigned shards (issue #522).
    #[must_use]
    pub fn with_sharded_pool(mut self, pool: ShardedDbPool) -> Self {
        self.sharded_pool = Some(pool);
        self
    }
}

struct PreparedHarvestRuntime {
    registry: Arc<HandlerRegistry>,
    dag_catalog: Arc<DagCatalog>,
    registered_dag_names: HashSet<String>,
    workflow_schedules: Arc<Vec<WorkflowSchedule>>,
    worker_runtime_config: WorkerRuntimeConfig,
    storage_pool: HarvestDbPool,
    shard_router: ShardRouter,
    retention_config: RetentionConfig,
    history_archiver: Option<Arc<dyn autumn_harvest::HistoryArchiver>>,
}

impl PreparedHarvestRuntime {
    fn build(
        built: BuiltHarvest,
        resources: HarvestRunnerResources,
    ) -> autumn_web::AutumnResult<Self> {
        let shard_router = resources.shard_router.clone().unwrap_or_default();
        let retention_config = built.retention().clone();
        let history_archiver = built.history_archiver().cloned();
        // Install the process-global completion-callback runtime config
        // (issue #605): the deliverer/secret/allowlist/defaults/retry policy
        // the core scanner (`fire_due_completion_deliveries`) and enqueue
        // path (`enqueue_completion_deliveries`) read via
        // `GLOBAL_CALLBACK_CONFIG`. Every `BuiltHarvest` consumer (the
        // `HarvestPlugin` web-app path and the standalone runner) funnels
        // through this one construction point, so this is set exactly once
        // regardless of which path started the runtime. Core ships no HTTP
        // client, so an embedder-supplied deliverer is used verbatim and a
        // `reqwest`-based default is substituted otherwise.
        {
            let callback_config = built.completion_callback_config();
            let deliverer = callback_config.deliverer.clone().unwrap_or_else(|| {
                Arc::new(crate::callback_deliverer::ReqwestCallbackDeliverer::new())
            });
            let secret = callback_config.secret.clone().unwrap_or_else(|| {
                autumn_harvest::completion_callback::CallbackSecret::new(Vec::new())
            });
            if let Ok(mut lock) =
                autumn_harvest::completion_callback::GLOBAL_CALLBACK_CONFIG.write()
            {
                *lock = Some(autumn_harvest::completion_callback::CallbackRuntimeConfig {
                    deliverer,
                    secret,
                    ssrf_policy: callback_config.ssrf_policy(),
                    default_targets: callback_config.default_targets.clone(),
                    retry_policy: callback_config.retry_policy.clone(),
                });
            }
        }
        let classic_dag_names = built
            .dags()
            .iter()
            .filter(|dag| dag.workflow_handler.is_none())
            .map(|dag| dag.name)
            .collect::<Vec<_>>();
        if !classic_dag_names.is_empty() {
            return Err(AutumnError::service_unavailable_msg(format!(
                "classic DAG execution is not supported by this runtime; \
                 rebuild with autumn-harvest/unified-dag-execution or remove classic DAGs: {}",
                classic_dag_names.join(", ")
            )));
        }
        let registered_dag_names = built
            .dags()
            .iter()
            .filter(|dag| dag.workflow_handler.is_some())
            .map(|dag| dag.name.to_string())
            .collect();
        let workflow_schedules = Arc::new(built.workflow_schedules().to_vec());
        let max_workflow_history_events = built.max_workflow_history_events;
        // Resolve the effective sharded storage pool *before* building handler
        // state so the registry receives the same sharded HarvestDbPool;
        // otherwise handlers fall back to the default shard's pool and can
        // read/write the wrong database (issue #522). Precedence:
        //   1. resources.sharded_pool — explicit runner-level override
        //   2. WorkerConfig::with_sharded_pool — carried on the built config
        //   3. single-shard wrapper of the default harvest pool
        // Honouring (2) here keeps a `HarvestBuilder::with_sharded_pool` from
        // being silently narrowed to a single shard when the runner is started
        // with only `HarvestRunnerResources::new(default_pool)` (the plugin
        // path), which would strand all non-default-shard work.
        let storage_pool = if let Some(sp) = resources.sharded_pool {
            HarvestDbPool::sharded(sp)
        } else if let Some(sp) = built.worker_config().sharded_pool.clone() {
            HarvestDbPool::sharded(sp)
        } else {
            HarvestDbPool::from(resources.harvest_pool)
        };
        // Reject a misconfigured router-vs-pool pair before any I/O.
        // `pool_for()` falls back to the default shard without warning, so a
        // missing pool for shard N silently writes shard-N `ExecutionId`s into
        // shard-0's database; those executions become permanently invisible
        // once shard N is later added.  Fail loud at startup instead.
        let mismatched = missing_router_shards(&shard_router, storage_pool.sharded_pool());
        if !mismatched.is_empty() {
            return Err(AutumnError::service_unavailable_msg(format!(
                "ShardRouter references shards {mismatched:?} that have no pool entry in the \
                 ShardedDbPool; every readable shard and the default shard must have an exact \
                 pool entry to prevent silent cross-shard writes — check your \
                 ShardedDbPool configuration"
            )));
        }
        let (registry, dags, _ws, worker_config) =
            built.into_worker_parts_with_extra_state(injected_runtime_state(
                resources.app_state,
                resources.app_pool,
                storage_pool.clone(),
                shard_router.clone(),
            ));
        let dag_catalog = Arc::new(
            compile_dag_catalog(dags)
                .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?,
        );
        let mut worker_runtime_config = WorkerRuntimeConfig::from(worker_config);
        // Builder-level ceiling takes precedence; WorkerConfig ceiling is kept
        // when the builder did not set one (avoids silently disabling a ceiling
        // that the embedder configured via WorkerConfig directly).
        if let Some(ceiling) = max_workflow_history_events {
            worker_runtime_config.max_workflow_history_events = Some(ceiling);
        }
        // Point the worker at the same resolved sharded pool so it claims from
        // every assigned shard (issue #522). `storage_pool` already honours the
        // resources/WorkerConfig precedence above, so this never narrows a
        // configured `WorkerConfig::with_sharded_pool` to a single shard.
        worker_runtime_config.sharded_pool = Some(storage_pool.sharded_pool().clone());

        Ok(Self {
            registry: Arc::new(registry),
            dag_catalog,
            registered_dag_names,
            workflow_schedules,
            worker_runtime_config,
            storage_pool,
            shard_router,
            retention_config,
            history_archiver,
        })
    }
}

/// Running Harvest runtime ownership for a process.
///
/// This owns any locally started worker and scheduler tasks while also
/// exposing the management API snapshot and Harvest storage pool needed by a
/// web app or control plane process.
pub struct HarvestRunner {
    api_runtime: HarvestApiRuntime,
    storage_pool: HarvestDbPool,
    worker: Option<Arc<Worker>>,
    worker_handle: Option<JoinHandle<()>>,
    scheduler: Option<SchedulerRuntime>,
    retention: Option<RetentionRuntime>,
    batch: Option<BatchRuntime>,
}

/// Background batch-operations executor handle (issue #102).
struct BatchRuntime {
    cancel: CancellationToken,
    handle: JoinHandle<()>,
}

impl BatchRuntime {
    /// Spawn a tick loop that drives every open batch job to terminal status.
    ///
    /// The loop sleeps `tick_interval` between scans; each tick walks every
    /// shard and dispatches per-target actions with bounded concurrency. The
    /// loop exits cleanly when the cancellation token fires.
    fn spawn(
        pool: ShardedDbPool,
        executor_config: BatchExecutorConfig,
        tick_interval: std::time::Duration,
    ) -> Self {
        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();
        let handle = tokio::spawn(async move {
            loop {
                if cancel_for_task.is_cancelled() {
                    return;
                }
                if let Err(error) = run_executor_once(&pool, &executor_config).await {
                    tracing::warn!(%error, "batch executor tick failed");
                }
                tokio::select! {
                    () = cancel_for_task.cancelled() => return,
                    () = tokio::time::sleep(tick_interval) => {}
                }
            }
        });
        Self { cancel, handle }
    }

    async fn shutdown(self) {
        self.cancel.cancel();
        if let Err(error) = self.handle.await {
            tracing::warn!(error = %error, "harvest batch executor task failed during shutdown");
        }
    }
}

impl HarvestRunner {
    /// Start a Harvest runtime from a previously built registration set.
    ///
    /// Local worker and scheduler ownership are driven by `config`.
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow/activity registrations are invalid or
    /// the worker configuration cannot be materialized.
    #[allow(clippy::too_many_lines)]
    pub async fn start(
        built: BuiltHarvest,
        config: &HarvestRuntimeConfig,
        resources: HarvestRunnerResources,
    ) -> autumn_web::AutumnResult<Self> {
        let completion_triggers = built.completion_triggers().to_vec();
        let prepared = PreparedHarvestRuntime::build(built, resources)?;
        let registry = Arc::clone(&prepared.registry);
        let dag_catalog = Arc::clone(&prepared.dag_catalog);
        let workflow_schedules = Arc::clone(&prepared.workflow_schedules);
        let queues = prepared.worker_runtime_config.queues.clone();
        let harvest_pool = prepared.storage_pool.clone_inner();
        let shard_router = prepared.shard_router.clone();
        autumn_harvest::shard::install_global_router(shard_router.clone());

        if !config.worker_enabled && !config.scheduler_enabled {
            tracing::info!(
                mode = ?config.mode,
                "harvest runtime started without local worker or scheduler ownership"
            );
        }

        // Sync static triggers before starting workers (issue #517)
        for (shard_id, shard_pool) in prepared.storage_pool.iter_shards() {
            let mut conn = shard_pool.get().await.map_err(|e| {
                AutumnError::service_unavailable_msg(format!(
                    "Failed to get DB connection to sync completion triggers for shard {shard_id}: {e}"
                ))
            })?;
            autumn_harvest::completion_trigger::sync_completion_triggers(
                &mut conn,
                &completion_triggers,
            )
            .await
            .map_err(|e| {
                AutumnError::service_unavailable_msg(format!(
                    "Failed to sync completion triggers on startup for shard {shard_id}: {e:?}"
                ))
            })?;
        }

        let worker = if config.worker_enabled {
            let worker = Worker::new(
                prepared.worker_runtime_config.clone(),
                Arc::clone(&registry),
            )
            .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?;
            // Fail the process at startup if any assigned shard is missing from
            // the sharded pool (issue #522 review). The same condition is
            // re-checked inside `Worker::run`, but that only aborts the spawned
            // task — startup would otherwise return Ok and keep serving the API
            // and scheduler with no local worker, leaving the assigned shards'
            // work unclaimed behind a coverage view that still advertises them.
            let missing = worker.missing_assigned_shard_pools();
            if !missing.is_empty() {
                return Err(AutumnError::service_unavailable_msg(format!(
                    "worker is enabled but shard_assignments {missing:?} are missing from the \
                     sharded_pool; refusing to start — check your ShardedDbPool configuration"
                )));
            }
            Some(Arc::new(worker))
        } else {
            None
        };
        let worker_id = worker
            .as_ref()
            .map(|_| prepared.worker_runtime_config.worker_id.clone());
        let worker_handle = worker.as_ref().map(|worker| {
            let worker = Arc::clone(worker);
            let pool = harvest_pool.clone();
            tokio::spawn(async move {
                worker.run(&pool).await;
            })
        });
        let scheduler = if config.scheduler_enabled {
            Some(SchedulerRuntime::spawn_sharded(
                prepared.storage_pool.sharded_pool().clone(),
                shard_router.clone(),
                Arc::clone(&registry),
                Arc::clone(&dag_catalog),
                Arc::clone(&workflow_schedules),
            ))
        } else {
            None
        };
        let scheduler_monitor = scheduler
            .as_ref()
            .map_or_else(SchedulerMonitor::offline, SchedulerRuntime::monitor);
        let retention = if prepared.retention_config.enabled() {
            RetentionRuntime::spawn(
                prepared.storage_pool.sharded_pool().clone(),
                prepared.retention_config.clone(),
                Arc::clone(&registry.telemetry().metrics),
                prepared.history_archiver,
                registry.payload_offloader_arc(),
            )
        } else {
            tracing::info!(
                mode = ?config.mode,
                "harvest retention janitor not started on this runtime (retention disabled)"
            );
            None
        };
        let retention_monitor = retention.as_ref().map(RetentionRuntime::monitor);
        let retention_trigger = retention.as_ref().map(RetentionRuntime::trigger_sender);
        // Batch operations executor (issue #102): drive open `harvest_batch_jobs`
        // rows to completion in the background. Only the worker-owning
        // process spawns it so we don't run multiple competing executors
        // against the same job rows.
        let batch = if config.worker_enabled {
            Some(BatchRuntime::spawn(
                prepared.storage_pool.sharded_pool().clone(),
                BatchExecutorConfig {
                    concurrency: config.batch.concurrency,
                    metrics: Arc::clone(&registry.telemetry().metrics),
                },
                std::time::Duration::from_millis(config.batch.tick_interval_ms),
            ))
        } else {
            None
        };
        let api_runtime = HarvestApiRuntime::new(
            registry,
            dag_catalog,
            workflow_schedules,
            worker_id,
            queues,
            scheduler_monitor,
            HarvestRetentionRuntime::new(
                prepared.retention_config,
                retention_monitor,
                retention_trigger,
            ),
            shard_router,
        )
        .with_registered_dag_names(prepared.registered_dag_names.iter().cloned());

        Ok(Self {
            api_runtime,
            storage_pool: prepared.storage_pool,
            worker,
            worker_handle,
            scheduler,
            retention,
            batch,
        })
    }

    /// Clone the API runtime snapshot for management/query routes.
    #[must_use]
    pub fn api_runtime(&self) -> HarvestApiRuntime {
        self.api_runtime.clone()
    }

    /// Clone the Harvest storage pool used by management routes.
    #[must_use]
    pub fn storage_pool(&self) -> HarvestDbPool {
        self.storage_pool.clone()
    }

    /// Stop any locally owned worker and scheduler tasks.
    pub async fn stop(self) {
        let Self {
            api_runtime: _,
            storage_pool: _,
            worker,
            worker_handle,
            scheduler,
            retention,
            batch,
        } = self;

        if let Some(worker) = worker {
            worker.shutdown();
        }
        if let Some(scheduler) = scheduler {
            scheduler.shutdown();
            if let Err(error) = scheduler.join().await {
                tracing::warn!(error = %error, "harvest scheduler task failed during shutdown");
            }
        }
        if let Some(retention) = retention {
            retention.shutdown();
            if let Err(error) = retention.join().await {
                tracing::warn!(error = %error, "harvest retention task failed during shutdown");
            }
        }
        if let Some(batch) = batch {
            batch.shutdown().await;
        }
        if let Some(worker_handle) = worker_handle
            && let Err(error) = worker_handle.await
        {
            tracing::warn!(error = %error, "harvest worker task failed during shutdown");
        }
    }
}

/// Returns shard IDs referenced by `router` that have no exact pool entry in
/// `pool`.  An empty result means the router and pool are fully consistent.
///
/// `ShardedDbPool::pool_for` silently falls back to the default shard when a
/// configured shard lacks a pool entry; if the caller never checks this gap,
/// shard-N `ExecutionId`s are written into shard-0's database and become
/// permanently invisible after shard N is provisioned later.  Call this at
/// startup and fail if the result is non-empty.
fn missing_router_shards(router: &ShardRouter, pool: &ShardedDbPool) -> Vec<ShardId> {
    let mut missing: Vec<ShardId> = router
        .readable_shards()
        .iter()
        .copied()
        .chain(std::iter::once(router.default_shard()))
        .filter(|&shard| pool.exact_pool_for(shard).is_none())
        .collect();
    missing.sort_unstable();
    missing.dedup();
    missing
}

pub(crate) fn injected_runtime_state(
    pool_state: Option<AppState>,
    app_pool: Option<DbPool>,
    harvest_pool: HarvestDbPool,
    shard_router: ShardRouter,
) -> SharedStateMap {
    let mut state: HashMap<TypeId, Box<dyn Any + Send + Sync>> = HashMap::new();
    if let Some(pool_state) = pool_state {
        state.insert(TypeId::of::<AppState>(), Box::new(pool_state));
    }
    if let Some(app_pool) = app_pool {
        state.insert(
            TypeId::of::<AppDbPool>(),
            Box::new(AppDbPool::from(app_pool)),
        );
    }
    // Inject the same (possibly sharded) HarvestDbPool the worker storage uses
    // so a handler calling `pool_for_execution` routes to the owning shard.
    // The legacy raw `DbPool` slot stays the default shard for shard-unaware
    // handlers (issue #522).
    state.insert(TypeId::of::<DbPool>(), Box::new(harvest_pool.clone_inner()));
    state.insert(TypeId::of::<HarvestDbPool>(), Box::new(harvest_pool));
    state.insert(TypeId::of::<ShardRouter>(), Box::new(shard_router));
    state
}
