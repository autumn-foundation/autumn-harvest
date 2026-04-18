//! Reusable Harvest runtime ownership for standalone or embedded processes.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use autumn_harvest::BuiltHarvest;
use autumn_harvest::context::SharedStateMap;
use autumn_harvest::scheduler::{
    DagCatalog, SchedulerMonitor, SchedulerRuntime, compile_dag_catalog,
};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_web::AppState;
use autumn_web::error::AutumnError;
use tokio::task::JoinHandle;

use crate::api::HarvestApiRuntime;
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
}

impl HarvestRunnerResources {
    /// Create a new resource bundle with the required Harvest storage pool.
    #[must_use]
    pub const fn new(harvest_pool: DbPool) -> Self {
        Self {
            app_state: None,
            app_pool: None,
            harvest_pool,
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
}

struct PreparedHarvestRuntime {
    registry: Arc<HandlerRegistry>,
    dag_catalog: Arc<DagCatalog>,
    worker_runtime_config: WorkerRuntimeConfig,
    storage_pool: HarvestDbPool,
}

impl PreparedHarvestRuntime {
    fn build(
        built: BuiltHarvest,
        resources: HarvestRunnerResources,
    ) -> autumn_web::AutumnResult<Self> {
        let (registry, dags, worker_config) =
            built.into_worker_parts_with_extra_state(injected_runtime_state(
                resources.app_state,
                resources.app_pool,
                resources.harvest_pool.clone(),
            ));
        let dag_catalog = Arc::new(
            compile_dag_catalog(dags)
                .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?,
        );

        Ok(Self {
            registry: Arc::new(registry),
            dag_catalog,
            worker_runtime_config: WorkerRuntimeConfig::from(worker_config),
            storage_pool: HarvestDbPool::from(resources.harvest_pool),
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
    pub fn start(
        built: BuiltHarvest,
        config: &HarvestRuntimeConfig,
        resources: HarvestRunnerResources,
    ) -> autumn_web::AutumnResult<Self> {
        let prepared = PreparedHarvestRuntime::build(built, resources)?;
        let registry = Arc::clone(&prepared.registry);
        let dag_catalog = Arc::clone(&prepared.dag_catalog);
        let queues = prepared.worker_runtime_config.queues.clone();
        let harvest_pool = prepared.storage_pool.clone_inner();

        if !config.worker_enabled && !config.scheduler_enabled {
            tracing::info!(
                mode = ?config.mode,
                "harvest runtime started without local worker or scheduler ownership"
            );
        }

        let worker = if config.worker_enabled {
            Some(Arc::new(
                Worker::new(
                    prepared.worker_runtime_config.clone(),
                    Arc::clone(&registry),
                )
                .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?,
            ))
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
        let scheduler = if config.scheduler_enabled && !dag_catalog.is_empty() {
            Some(SchedulerRuntime::spawn(
                harvest_pool,
                Arc::clone(&registry),
                Arc::clone(&dag_catalog),
            ))
        } else {
            None
        };
        let scheduler_monitor = scheduler
            .as_ref()
            .map_or_else(SchedulerMonitor::offline, SchedulerRuntime::monitor);
        let api_runtime =
            HarvestApiRuntime::new(registry, dag_catalog, worker_id, queues, scheduler_monitor);

        Ok(Self {
            api_runtime,
            storage_pool: prepared.storage_pool,
            worker,
            worker_handle,
            scheduler,
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
        if let Some(worker_handle) = worker_handle {
            if let Err(error) = worker_handle.await {
                tracing::warn!(error = %error, "harvest worker task failed during shutdown");
            }
        }
    }
}

pub(crate) fn injected_runtime_state(
    pool_state: Option<AppState>,
    app_pool: Option<DbPool>,
    harvest_pool: DbPool,
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
    let harvest_pool = HarvestDbPool::from(harvest_pool);
    state.insert(
        TypeId::of::<HarvestDbPool>(),
        Box::new(harvest_pool.clone()),
    );
    state.insert(TypeId::of::<DbPool>(), Box::new(harvest_pool.clone_inner()));
    state
}
