//! Reusable Harvest runtime ownership for standalone or embedded processes.

use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use autumn_harvest::BuiltHarvest;
use autumn_harvest::batch::{BatchExecutorConfig, run_executor_once};
use autumn_harvest::context::SharedStateMap;
use autumn_harvest::effective_config::{
    EffectiveConfigView, PayloadCapsView, PoolConfigView, ShardedInfo,
};
use autumn_harvest::policy::WorkflowSchedule;
use autumn_harvest::retention::{RetentionConfig, RetentionRuntime};
use autumn_harvest::scheduler::{
    DagCatalog, SchedulerMonitor, SchedulerRuntime, compile_dag_catalog,
};
use autumn_harvest::shard::{ShardRouter, ShardedDbPool};
use autumn_harvest::types::ShardId;
use autumn_harvest::worker::{
    DEFAULT_WORKER_POLL_INTERVAL, DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig,
};
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

    /// The explicit runner-level sharded-pool override, if set.
    ///
    /// Used by the plugin's boot-time orphaned-workflow gate (issue #700 P2) to
    /// select the same shard-0 pool `HarvestRunner::start` will run against, by
    /// feeding this into [`select_runtime_shard0_pool`] with the exact inputs
    /// `build` uses — so the gate queries the database the workers actually poll
    /// rather than assuming `harvest_pool`. Borrowed (not cloned) so the gate's
    /// selection touches no process global.
    #[must_use]
    pub(crate) const fn sharded_pool_override(&self) -> Option<&ShardedDbPool> {
        self.sharded_pool.as_ref()
    }
}

/// Which pool source the runtime resolves to, before any installation.
///
/// Borrowed so the boot-gate can READ shard 0 without installing a process
/// global (issue #700 P4).
enum RuntimePoolSource<'a> {
    Sharded(&'a ShardedDbPool),
    Single(&'a DbPool),
}

/// **Single source of truth** for the pool-resolution PRECEDENCE (issue #700
/// P2).
///
/// A runner-level `resources.sharded_pool` override wins, then a
/// `WorkerConfig::with_sharded_pool` carried on the build, then the single
/// `harvest_pool`. Pure SELECTION — installs no process global. Both the
/// install path ([`resolve_runtime_storage_pool`]) and the boot-gate's read
/// path ([`select_runtime_shard0_pool`]) route through it, so the
/// `sharded_pool`-over-`harvest_pool` precedence can never drift between the
/// gate and the runner.
const fn pick_runtime_pool_source<'a>(
    resources_sharded_pool: Option<&'a ShardedDbPool>,
    worker_config_sharded_pool: Option<&'a ShardedDbPool>,
    harvest_pool: &'a DbPool,
) -> RuntimePoolSource<'a> {
    match (resources_sharded_pool, worker_config_sharded_pool) {
        (Some(sp), _) | (None, Some(sp)) => RuntimePoolSource::Sharded(sp),
        (None, None) => RuntimePoolSource::Single(harvest_pool),
    }
}

/// Resolve the storage pool the runtime will run against **and install it**.
///
/// For the single-shard case this goes through
/// `HarvestDbPool::from` → `ShardedDbPool::single`, which writes the process
/// global `GLOBAL_SHARDED_POOL`. Used by `PreparedHarvestRuntime::build` on the
/// normal startup path, where installing the global is intended.
///
/// The boot-time orphaned-workflow gate must NOT call this — an aborting gate
/// must mutate no process global (issue #700 P4). It uses
/// [`select_runtime_shard0_pool`] instead, which shares this function's exact
/// precedence (via [`pick_runtime_pool_source`]) but installs nothing.
#[must_use]
pub fn resolve_runtime_storage_pool(
    resources_sharded_pool: Option<&ShardedDbPool>,
    worker_config_sharded_pool: Option<&ShardedDbPool>,
    harvest_pool: &DbPool,
) -> HarvestDbPool {
    match pick_runtime_pool_source(
        resources_sharded_pool,
        worker_config_sharded_pool,
        harvest_pool,
    ) {
        RuntimePoolSource::Sharded(sp) => HarvestDbPool::sharded(sp.clone()),
        RuntimePoolSource::Single(pool) => HarvestDbPool::from(pool.clone()),
    }
}

/// Select the shard-0 `DbPool` HANDLE the runtime will run the workers against.
///
/// Honors the same precedence as [`resolve_runtime_storage_pool`] but
/// **without installing any process global** (issue #700 P2 + P4).
///
/// The boot-time orphaned-workflow gate reads through this so an `Abort` mutates
/// no `GLOBAL_SHARDED_POOL`: it reads shard 0 from an already-constructed
/// `ShardedDbPool` (`pool_for`, a read), or returns the `harvest_pool` handle
/// directly — it never calls `ShardedDbPool::single`/`from_map`. Preserves the
/// P2 guarantee (the gate queries the same database the runner will) while
/// keeping an aborted gate side-effect-free.
#[must_use]
pub fn select_runtime_shard0_pool(
    resources_sharded_pool: Option<&ShardedDbPool>,
    worker_config_sharded_pool: Option<&ShardedDbPool>,
    harvest_pool: &DbPool,
) -> DbPool {
    match pick_runtime_pool_source(
        resources_sharded_pool,
        worker_config_sharded_pool,
        harvest_pool,
    ) {
        RuntimePoolSource::Sharded(sp) => sp.pool_for(ShardId::new(0)).clone(),
        RuntimePoolSource::Single(pool) => pool.clone(),
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
    /// Secret-free effective-config snapshot (issue #695) captured here — the
    /// single seam every `BuiltHarvest` consumer (the `HarvestPlugin` web-app
    /// path and the standalone runner) funnels through — so it rides on the
    /// resulting `HarvestApiRuntime` and is served by `GET /admin/config`
    /// automatically, without a separate `set_effective_config` call the
    /// integrator must remember.
    effective_config: EffectiveConfigView,
}

/// Install the process-global completion-callback runtime config (issue #605).
///
/// Extracted from `PreparedHarvestRuntime::build` so it sits alongside its
/// audit-export sibling below; the reasoning is unchanged.
///
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
// issue #605 code review: signing with an empty key is not a
// silent no-op -- HMAC-SHA256 accepts any key length and
// produces a valid, deterministic (and trivially
// reproducible by anyone) signature, so a caller who never
// configures `completion_callback_secret(...)` gets a
// `X-Harvest-Signature` header that carries no real
// authenticity guarantee at all. This is reachable for both
// builder-default AND per-execution targets (the latter
// bypass builder config entirely), so warn unconditionally
// rather than only when default targets are configured.
fn install_completion_callback_config(built: &BuiltHarvest) {
    let callback_config = built.completion_callback_config();
    let deliverer = callback_config
        .deliverer
        .clone()
        .unwrap_or_else(|| Arc::new(crate::callback_deliverer::ReqwestCallbackDeliverer::new()));
    let secret = callback_config.secret.clone().unwrap_or_else(|| {
        // issue #605 code review: signing with an empty key is not a
        // silent no-op -- HMAC-SHA256 accepts any key length and
        // produces a valid, deterministic (and trivially
        // reproducible by anyone) signature, so a caller who never
        // configures `completion_callback_secret(...)` gets a
        // `X-Harvest-Signature` header that carries no real
        // authenticity guarantee at all. This is reachable for both
        // builder-default AND per-execution targets (the latter
        // bypass builder config entirely), so warn unconditionally
        // rather than only when default targets are configured.
        tracing::warn!(
            "completion-callback HMAC secret was never configured via \
                 HarvestBuilder::completion_callback_secret(...) -- every \
                 delivered callback will be signed with an empty key, which \
                 defeats the X-Harvest-Signature authenticity guarantee for \
                 any receiver relying on it"
        );
        autumn_harvest::completion_callback::CallbackSecret::new(Vec::new())
    });
    if let Ok(mut lock) = autumn_harvest::completion_callback::GLOBAL_CALLBACK_CONFIG.write() {
        *lock = Some(Arc::new(
            autumn_harvest::completion_callback::CallbackRuntimeConfig {
                deliverer,
                secret,
                ssrf_policy: callback_config.ssrf_policy(),
                default_targets: callback_config.default_targets.clone(),
                retry_policy: callback_config.retry_policy.clone(),
            },
        ));
    }
}

/// Install the process-global audit-export runtime config (issue #953).
///
/// Called from `PreparedHarvestRuntime::build`, the one construction point
/// every `BuiltHarvest` consumer funnels through, so the exporter's sink is
/// installed exactly once regardless of which path started the runtime. Core
/// ships no HTTP client, so an embedder-supplied `AuditSink` is used verbatim
/// and a `reqwest` signed-webhook sink is substituted when only a URL was
/// configured.
///
/// Installs `None` when neither was configured — deliberately a write, not a
/// skip: the config is a process-wide static, so a second runtime built
/// without a sink must not keep shipping audit records to the first runtime's
/// destination.
fn install_audit_export_config(built: &BuiltHarvest) {
    let audit_config = built.audit_export_config();
    let sink: Option<Arc<dyn autumn_harvest::audit_export::AuditSink>> =
        audit_config.sink.clone().or_else(|| {
            audit_config.webhook_url.as_ref().map(|url| {
                Arc::new(crate::audit_sink::ReqwestAuditSink::new(url.clone()))
                    as Arc<dyn autumn_harvest::audit_export::AuditSink>
            })
        });
    let Ok(mut lock) = autumn_harvest::audit_export::GLOBAL_AUDIT_EXPORT_CONFIG.write() else {
        return;
    };
    *lock = sink.map(|sink| {
        // `HarvestBuilder::try_build` rejects a webhook with no secret, so
        // reaching the empty-key fallback means an embedder-supplied sink that
        // authenticates some other way (IAM, mTLS, a local file). Warn rather
        // than fail: a signature is not always the relevant control there.
        let secret = audit_config.secret.clone().unwrap_or_else(|| {
            tracing::warn!(
                "no audit-export HMAC secret was configured via \
                 HarvestBuilder::audit_export_secret(...) -- exported batches will carry \
                 an X-Harvest-Signature computed with an empty key, which any third party \
                 can reproduce; it conveys no authenticity and a receiver must not treat \
                 it as tamper evidence"
            );
            autumn_harvest::completion_callback::CallbackSecret::new(Vec::new())
        });
        Arc::new(autumn_harvest::audit_export::AuditExportRuntimeConfig {
            sink,
            secret,
            batch_size: audit_config.effective_batch_size(),
            backoff: audit_config.backoff.clone(),
            lease: audit_config.effective_lease(),
        })
    });
}

impl PreparedHarvestRuntime {
    fn build(
        built: BuiltHarvest,
        resources: HarvestRunnerResources,
    ) -> autumn_web::AutumnResult<Self> {
        let shard_router = resources.shard_router.clone().unwrap_or_default();
        let retention_config = built.retention().clone();
        let history_archiver = built.history_archiver().cloned();
        install_completion_callback_config(&built);
        install_audit_export_config(&built);
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
        // Capture the runner-provided sharded-pool provenance before the move
        // below consumes `resources.sharded_pool`. The `WorkerConfig` knob is
        // still readable afterwards from `built`, but this override is not — and
        // the effective-config snapshot must report the resolved runtime pool's
        // sharded-ness, not solely the `WorkerConfig` field (issue #695 review).
        let resources_sharded_pool = resources.sharded_pool.is_some();
        // Single source of truth for the pool-resolution precedence (issue #700
        // P2): the plugin's boot-time orphan gate calls the very same
        // `resolve_runtime_storage_pool` so it queries the exact database the
        // workers will poll, and the `sharded_pool`-over-`harvest_pool`
        // precedence can never drift between the two.
        let storage_pool = resolve_runtime_storage_pool(
            resources.sharded_pool.as_ref(),
            built.worker_config().sharded_pool.as_ref(),
            &resources.harvest_pool,
        );
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
        // Capture the secret-free effective-config snapshot (issue #695) while
        // `built` is still owned — `built.into_worker_parts_*` below consumes it.
        let effective_config =
            capture_effective_config(&built, &storage_pool, &shard_router, resources_sharded_pool);
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

        // Resolve auto (empty) shard assignments now that `sharded_pool` is
        // final (issue #961, AC1). `Worker::new` runs the same idempotent pass,
        // but doing it here first means the warning below — and anything else
        // reading `worker_runtime_config.shard_assignments` — sees the
        // *effective* coverage rather than the raw "auto" sentinel.
        worker_runtime_config.resolve_shard_assignments();

        warn_uncovered_writable_shards(&shard_router, &worker_runtime_config.shard_assignments);

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
            effective_config,
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
        .with_registered_dag_names(prepared.registered_dag_names.iter().cloned())
        .with_effective_config(prepared.effective_config.clone());

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
/// Build the secret-free effective-config snapshot (issue #695) from the
/// resolved runtime parts.
///
/// Payload caps come from `built`; the pool sizing is read from the
/// already-resolved `storage_pool`, so it honours the full
/// resources/`WorkerConfig` sharded-pool precedence and always describes the
/// pool the runtime actually uses. `poll_interval` reads the side-effect-free
/// [`DEFAULT_WORKER_POLL_INTERVAL`] — the single source of truth the
/// `WorkerRuntimeConfig` conversion also uses — so this stays a pure read.
///
/// `resources_sharded_pool` reports whether a runner-provided
/// [`HarvestRunnerResources::with_sharded_pool`] override was supplied — that
/// provenance is erased once `storage_pool` is built, so the caller captures it
/// before the move. It, together with `WorkerConfig::with_sharded_pool` and the
/// resolved `storage_pool` shard count, drives the two `WorkerConfigView`
/// sharded-pool fields so they describe the resolved runtime pool, not solely
/// the `WorkerConfig` knob (issue #695 review).
fn capture_effective_config(
    built: &BuiltHarvest,
    storage_pool: &HarvestDbPool,
    router: &ShardRouter,
    resources_sharded_pool: bool,
) -> EffectiveConfigView {
    let (payload_offload_enabled, payload_offload_threshold_bytes) = built
        .payload_offloader()
        .map_or((false, 0), |o| (true, o.threshold()));
    let caps = PayloadCapsView::new(
        built.max_activity_input_bytes,
        built.max_activity_result_bytes,
        built.max_signal_payload_bytes,
        built.max_workflow_input_bytes,
        built.max_current_details_bytes,
        built.max_workflow_execution_timeout,
        built.max_workflow_attempts,
        built
            .max_workflow_history_events
            .or_else(|| built.worker_config().max_workflow_history_events),
        built.usage_window_ceiling,
        built.usage_max_groups,
        payload_offload_enabled,
        payload_offload_threshold_bytes,
    );
    let shard_count = storage_pool.iter_shards().count();
    let pool_view = PoolConfigView {
        worker_pool_max_connections: storage_pool.default_pool().status().max_size,
        shard_pool_count: shard_count,
    };
    // The runtime uses a sharded pool when either the runner supplied one or the
    // WorkerConfig carried one; the fallback single pool (a 1-shard wrapper of
    // `harvest_pool`) is not "sharded_pool_configured". Report the resolved
    // pool's actual shard count when sharded, else 0.
    let sharded_pool_configured =
        resources_sharded_pool || built.worker_config().sharded_pool.is_some();
    let resolved_sharding = ShardedInfo {
        configured: sharded_pool_configured,
        shard_count: if sharded_pool_configured {
            shard_count
        } else {
            0
        },
        // Always the resolved pool's real shard ids — including the
        // single-shard fallback wrapper — so the snapshot can report the
        // worker's *effective* shard assignments (issue #961, AC1/AC8).
        // Unlike `shard_count`, this is not zeroed for the non-sharded case:
        // resolving "auto" needs the concrete shard list either way, and for
        // the fallback wrapper it is exactly `[0]`.
        shard_ids: storage_pool
            .iter_shards()
            .map(|(shard, _)| shard.as_i32())
            .collect(),
    };
    EffectiveConfigView::capture(
        built.worker_config(),
        caps,
        router,
        pool_view,
        DEFAULT_WORKER_POLL_INTERVAL,
        Some(resolved_sharding),
    )
}

/// The writable shards `assignments` does **not** cover, ascending (issue #961).
///
/// Pure so the boot-time coverage warning is testable without a runtime. Under
/// auto-assignment this is always empty by construction — `ShardRouter::new`
/// asserts `writable ⊆ readable`, `missing_router_shards` fails startup when a
/// readable shard has no pool, and auto-derived assignments are exactly the
/// pool's shards — so a non-empty result means the worker was **explicitly**
/// narrowed.
fn uncovered_writable_shards(router: &ShardRouter, assignments: &[ShardId]) -> Vec<i32> {
    let covered: std::collections::BTreeSet<ShardId> = assignments.iter().copied().collect();
    let mut uncovered: Vec<i32> = router
        .writable_shards()
        .iter()
        .filter(|shard| !covered.contains(shard))
        .map(|shard| shard.as_i32())
        .collect();
    uncovered.sort_unstable();
    uncovered
}

/// Warn loudly about writable shards this worker will not poll (issue #961, AC6).
///
/// This is the boot-time half of shard-coverage detection; the runtime half is
/// the `harvest.shard.stranded_pending` gauge and the `harvest_shard_undrained`
/// alert. It is a warning, not a hard failure: a one-worker-process-per-shard
/// deployment deliberately narrows each process, so uncovered-here is only a
/// problem when *no* process in the fleet covers the shard — which this process
/// cannot know. The fleet-wide question is answered by
/// `GET /admin/shards/health` (`no_live_worker`) and the stranded-work gauge.
fn warn_uncovered_writable_shards(router: &ShardRouter, assignments: &[ShardId]) {
    let uncovered = uncovered_writable_shards(router, assignments);
    if !uncovered.is_empty() {
        let covered: Vec<i32> = assignments.iter().map(|s| s.as_i32()).collect();
        tracing::warn!(
            uncovered_writable_shards = ?uncovered,
            covered_shards = ?covered,
            "this worker does not poll every writable shard; work routed to the \
             uncovered shards will be stranded unless another worker process in the \
             fleet covers them — verify with GET /api/harvest/admin/shards/health and \
             the harvest.shard.stranded_pending gauge (issue #961)"
        );
    }
}

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

#[cfg(test)]
mod tests {
    use super::{
        resolve_runtime_storage_pool, select_runtime_shard0_pool, uncovered_writable_shards,
    };
    use autumn_harvest::shard::ShardRouter;
    use autumn_harvest::shard::ShardedDbPool;
    use autumn_harvest::types::ShardId;
    use autumn_harvest::worker::DbPool;
    use diesel_async::AsyncPgConnection;
    use diesel_async::pooled_connection::AsyncDieselConnectionManager;

    /// Build a pool tagged by its `max_size` (readable without connecting) so
    /// two pools are distinguishable in a DB-free test.
    fn tagged_pool(max_size: usize) -> DbPool {
        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            "postgres://unused@127.0.0.1:1/none",
        );
        deadpool::managed::Pool::builder(manager)
            .max_size(max_size)
            .build()
            .expect("build tagged pool")
    }

    /// The single source of truth for the runtime pool-resolution precedence
    /// (issue #700 P2). A `WorkerConfig::with_sharded_pool` MUST win over
    /// `harvest_pool` — otherwise the boot-gate (which calls this exact
    /// function) would validate `harvest_pool` while the workers poll the
    /// sharded pool's database. The `max_size` tag identifies which pool was
    /// resolved without opening a connection.
    #[test]
    fn resolve_runtime_storage_pool_precedence() {
        let harvest = tagged_pool(3);
        let worker_config_sharded = ShardedDbPool::single(tagged_pool(7));
        let resources_override = ShardedDbPool::single(tagged_pool(11));

        // WorkerConfig's sharded pool wins over harvest_pool (the P2 case).
        let resolved = resolve_runtime_storage_pool(None, Some(&worker_config_sharded), &harvest);
        assert_eq!(
            resolved.clone_inner().status().max_size,
            7,
            "WorkerConfig::with_sharded_pool must win over harvest_pool",
        );

        // An explicit runner-level override wins over WorkerConfig's pool.
        let resolved = resolve_runtime_storage_pool(
            Some(&resources_override),
            Some(&worker_config_sharded),
            &harvest,
        );
        assert_eq!(
            resolved.clone_inner().status().max_size,
            11,
            "runner-level sharded_pool override must win over WorkerConfig",
        );

        // No sharded pool configured -> single-shard wrapper of harvest_pool.
        let resolved = resolve_runtime_storage_pool(None, None, &harvest);
        assert_eq!(
            resolved.clone_inner().status().max_size,
            3,
            "with no sharded pool the resolver must fall back to harvest_pool",
        );
    }

    /// The read-only shard-0 selector (issue #700 P4) must honor the SAME
    /// precedence as `resolve_runtime_storage_pool` (both route through
    /// `pick_runtime_pool_source`). Asserted on the returned pool's `max_size`
    /// tag, so this reads no process global and cannot race parallel tests.
    #[test]
    fn select_runtime_shard0_pool_precedence() {
        let harvest = tagged_pool(3);
        let worker_config_sharded = ShardedDbPool::single(tagged_pool(7));
        let resources_override = ShardedDbPool::single(tagged_pool(11));

        // WorkerConfig's sharded pool wins over harvest_pool (shard 0 of it).
        assert_eq!(
            select_runtime_shard0_pool(None, Some(&worker_config_sharded), &harvest)
                .status()
                .max_size,
            7,
            "select: WorkerConfig::with_sharded_pool must win over harvest_pool",
        );

        // A runner-level override wins over WorkerConfig's pool.
        assert_eq!(
            select_runtime_shard0_pool(
                Some(&resources_override),
                Some(&worker_config_sharded),
                &harvest,
            )
            .status()
            .max_size,
            11,
            "select: runner-level sharded_pool override must win over WorkerConfig",
        );

        // No sharded pool -> harvest_pool handle directly.
        assert_eq!(
            select_runtime_shard0_pool(None, None, &harvest)
                .status()
                .max_size,
            3,
            "select: with no sharded pool must return harvest_pool directly",
        );
    }

    fn three_shard_router() -> ShardRouter {
        let ids = vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)];
        ShardRouter::new(ids.clone(), ids, ShardId::new(0))
    }

    #[test]
    fn uncovered_writable_shards_names_every_writable_shard_the_worker_skips() {
        let router = three_shard_router();
        assert_eq!(
            uncovered_writable_shards(&router, &[ShardId::new(0)]),
            vec![1, 2],
            "an explicitly narrowed worker must name every writable shard it drops",
        );
        assert_eq!(
            uncovered_writable_shards(&router, &[ShardId::new(2), ShardId::new(0)]),
            vec![1],
            "the result is ascending regardless of assignment order",
        );
    }

    #[test]
    fn uncovered_writable_shards_is_empty_for_full_coverage() {
        let router = three_shard_router();
        let all = [ShardId::new(0), ShardId::new(1), ShardId::new(2)];
        assert!(
            uncovered_writable_shards(&router, &all).is_empty(),
            "full coverage — the boot warning must stay silent",
        );
        // Auto-assignment derives from the pool, which is a superset of the
        // writable set in any startable deployment, so it never warns.
        let superset = [
            ShardId::new(0),
            ShardId::new(1),
            ShardId::new(2),
            ShardId::new(3),
        ];
        assert!(
            uncovered_writable_shards(&router, &superset).is_empty(),
            "covering more than the writable set must not warn",
        );
    }

    #[test]
    fn uncovered_writable_shards_ignores_readable_only_shards() {
        // Shard 2 is readable (draining) but not writable: a worker that skips
        // it must NOT be warned about, because no new work routes there.
        let readable = vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)];
        let writable = vec![ShardId::new(0), ShardId::new(1)];
        let router = ShardRouter::new(readable, writable, ShardId::new(0));
        assert!(
            uncovered_writable_shards(&router, &[ShardId::new(0), ShardId::new(1)]).is_empty(),
            "a drained readable-only shard is not an uncovered *writable* shard",
        );
    }
}
