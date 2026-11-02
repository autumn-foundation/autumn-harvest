//! Per-tenant Harvest in a SQLite fleet app (issue #2472).
//!
//! `HarvestPlugin::sqlite_per_tenant()` gives each open tenant database its
//! own Harvest file and its own driver. The control Harvest keeps running
//! beside them, for platform workflows.
//!
//! ```text
//! fleet open ──► on_open ──► open acme.harvest.db, start a driver,
//!                            register it as a worker of the database
//! fleet close ─► the fleet stops the worker ──► on_close drops the handle
//! fleet delete ► the fleet aborts the worker ─► on_delete removes the file
//! ```
//!
//! The fleet decides when a tenant is open. A closed tenant runs nothing. Its
//! runs resume when it opens again. The registry copy of each handle holds no
//! lease, so the plugin never keeps a tenant open by itself. A handle given
//! to a caller holds one, so the tenant stays open while the caller works.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::{Duration, Instant};

use autumn_harvest::info::WorkflowInfo;
use autumn_web::AppState;
use autumn_web::db::fleet::{DatabaseFleet, FleetDatabase, FleetLifecycle};
use autumn_web::error::AutumnError;
use autumn_web::fleet_layout::FleetDbKey;
use autumn_web::reexports::axum;
use tokio_util::sync::CancellationToken;

use super::driver::{Driver, Registry};
use super::handle::{HarvestSqlite, HarvestSqliteError};

/// How long a caller waits for a tenant driver that another task starts.
const START_WAIT: Duration = Duration::from_secs(10);

/// The SQLite sidecars that a delete removes with a Harvest file.
///
/// The `.lock` file stays. The lock module forbids its delete while a
/// runtime can still hold it.
const SIDECARS: [&str; 3] = ["-wal", "-shm", "-journal"];

tokio::task_local! {
    /// The tenant whose driver runs the current task.
    static CURRENT: FleetDbKey;
}

/// The tenant of the running activity.
///
/// A tenant driver runs each activity inline on its own task. That task
/// carries the tenant key. Code on the control driver, or on a task that an
/// activity spawns, sees `None`.
///
/// ```rust,ignore
/// #[activity]
/// async fn purge(ctx: &ActivityContext, _: ()) -> Result<(), String> {
///     let key = HarvestTenant::current().ok_or("not a tenant run")?;
///     let fleet = ctx.state::<AppState>().and_then(|s| s.shards()?.fleet().cloned());
///     // Open `key` on the fleet and work on that tenant only.
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct HarvestTenant;

impl HarvestTenant {
    /// The key of the tenant whose driver runs this task, or `None`.
    #[must_use]
    pub fn current() -> Option<FleetDbKey> {
        CURRENT.try_with(Clone::clone).ok()
    }
}

/// Where the Harvest file of a fleet database goes: `acme.db` gives
/// `acme.harvest.db`, in the same directory.
#[must_use]
pub fn tenant_harvest_file(database: &Path) -> PathBuf {
    super::sibling_harvest_file(database)
}

/// One tenant in the registry.
enum Slot {
    /// A task opens the file now. Another start returns at once.
    Starting,
    /// The driver runs. This copy holds no lease.
    Running(HarvestSqlite),
}

/// A tenant as [`HarvestFleet::open`] sees it.
enum TenantState {
    /// No driver and no start yet.
    Absent,
    /// Another task opens the file now.
    Starting,
    /// The driver runs.
    Running(HarvestSqlite),
}

/// What every tenant driver shares.
struct Shared {
    fleet: DatabaseFleet,
    registry: Arc<Registry>,
    infos: Vec<WorkflowInfo>,
    tick: Duration,
    tenants: Mutex<HashMap<FleetDbKey, Slot>>,
}

/// The per-tenant Harvests of a fleet app.
///
/// The plugin installs one at startup when `HarvestPlugin::sqlite_per_tenant`
/// is on. Get it as an extractor, or with `state.extension::<HarvestFleet>()`.
///
/// ```rust,ignore
/// #[post("/{tenant}/orders")]
/// async fn create(fleet: HarvestFleet, Path(tenant): Path<String>) -> AutumnResult<String> {
///     let harvest = fleet.open_for(&tenant).await.map_err(to_http)?;
///     let run = harvest.start("process_order", json!({}), StartOptions::new()).await.map_err(to_http)?;
///     Ok(run.exec_id.to_string())
/// }
/// ```
#[derive(Clone)]
pub struct HarvestFleet {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for HarvestFleet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HarvestFleet")
            .field("open_tenants", &self.open_tenants())
            .finish_non_exhaustive()
    }
}

impl HarvestFleet {
    pub(crate) fn new(
        fleet: DatabaseFleet,
        registry: Arc<Registry>,
        infos: Vec<WorkflowInfo>,
        tick: Duration,
    ) -> Self {
        Self {
            shared: Arc::new(Shared {
                fleet,
                registry,
                infos,
                tick,
                tenants: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Install the lifecycle on the fleet, then start a driver for each
    /// tenant that is open already. The fleet calls no `on_open` for those.
    pub(crate) async fn install(&self) {
        self.shared
            .fleet
            .add_lifecycle(Arc::new(TenantLifecycle(Arc::downgrade(&self.shared))));
        for key in self.shared.fleet.open_keys() {
            let Some(db) = self.shared.fleet.get_open(&key) else {
                continue;
            };
            if let Err(error) = self.start_blocking(db).await {
                tracing::warn!(tenant = %key, %error, "cannot start the tenant Harvest");
            }
        }
    }

    /// The Harvest of the tenant that `routing_key` routes to. It opens the
    /// tenant database when it is closed.
    ///
    /// # Errors
    ///
    /// [`HarvestSqliteError::Fleet`] when the fleet refuses the key or the
    /// open. [`HarvestSqliteError::Tenant`] when the driver cannot start.
    pub async fn open_for(&self, routing_key: &str) -> Result<HarvestSqlite, HarvestSqliteError> {
        let key = self.shared.fleet.key_for(routing_key)?;
        self.open(&key).await
    }

    /// The Harvest of the tenant database `key`. It opens the database when
    /// it is closed.
    ///
    /// The handle holds a lease on the database. The fleet keeps the
    /// database open while any clone of the handle lives.
    ///
    /// # Errors
    ///
    /// As [`open_for`](Self::open_for).
    pub async fn open(&self, key: &FleetDbKey) -> Result<HarvestSqlite, HarvestSqliteError> {
        let db = self.shared.fleet.open(key).await?;
        let lease = db.lease();
        let deadline = Instant::now() + START_WAIT;
        loop {
            match self.state(key) {
                TenantState::Running(handle) => return Ok(handle.with_lease(lease)),
                // The fleet opened it before the lifecycle saw it.
                TenantState::Absent => self.start_blocking(db.clone()).await?,
                TenantState::Starting if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                TenantState::Starting => {
                    return Err(HarvestSqliteError::Tenant(format!(
                        "the Harvest of {key} did not start in {START_WAIT:?}"
                    )));
                }
            }
        }
    }

    /// The tenant databases that run a Harvest driver now, sorted.
    #[must_use]
    pub fn open_tenants(&self) -> Vec<FleetDbKey> {
        let tenants = self.shared.lock();
        let mut keys: Vec<FleetDbKey> = tenants
            .iter()
            .filter(|(_, slot)| matches!(slot, Slot::Running(_)))
            .map(|(key, _)| key.clone())
            .collect();
        drop(tenants);
        keys.sort();
        keys
    }

    fn state(&self, key: &FleetDbKey) -> TenantState {
        match self.shared.lock().get(key) {
            None => TenantState::Absent,
            Some(Slot::Starting) => TenantState::Starting,
            Some(Slot::Running(handle)) => TenantState::Running(handle.clone()),
        }
    }

    async fn start_blocking(&self, db: FleetDatabase) -> Result<(), HarvestSqliteError> {
        let shared = Arc::clone(&self.shared);
        tokio::task::spawn_blocking(move || shared.start(&db))
            .await
            .map_err(|error| HarvestSqliteError::Tenant(error.to_string()))?
            .map_err(HarvestSqliteError::Tenant)
    }
}

impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<FleetDbKey, Slot>> {
        self.tenants.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Open the tenant's Harvest file and start its driver.
    ///
    /// It blocks on file IO. A second call for a tenant that runs or starts
    /// returns at once, so one file never gets two runtimes.
    fn start(&self, db: &FleetDatabase) -> Result<(), String> {
        let key = db.key().clone();
        {
            let mut tenants = self.lock();
            if tenants.contains_key(&key) {
                return Ok(());
            }
            tenants.insert(key.clone(), Slot::Starting);
        }
        match self.open_driver(db) {
            Ok(handle) => {
                self.lock().insert(key.clone(), Slot::Running(handle));
                tracing::info!(tenant = %key, "tenant Harvest is running");
                Ok(())
            }
            Err(error) => {
                self.lock().remove(&key);
                Err(error)
            }
        }
    }

    fn open_driver(&self, db: &FleetDatabase) -> Result<HarvestSqlite, String> {
        let key = db.key().clone();
        let path = tenant_harvest_file(db.path());
        let tokio = tokio::runtime::Handle::try_current()
            .map_err(|error| format!("no Tokio runtime to run the Harvest of {key}: {error}"))?;
        let runtime = Driver::open(&path, &self.registry)?;
        let reader = autumn_harvest_sqlite::SqliteReader::open(&path)
            .map_err(|error| format!("cannot open a reader on {}: {error}", path.display()))?;
        let stop = CancellationToken::new();
        let _entered = tokio.enter();
        let (sender, task) = Driver::spawn_tenant(runtime, self.tick, key.clone(), stop.clone());
        // The fleet stops the driver before it closes the database.
        db.register_worker(stop, task);
        Ok(HarvestSqlite::new(sender, reader, path, self.infos.clone()).for_tenant(key))
    }

    fn forget(&self, key: &FleetDbKey) {
        self.lock().remove(key);
    }

    /// Remove the tenant's Harvest file and its sidecars.
    fn erase(&self, key: &FleetDbKey) {
        let path = tenant_harvest_file(&self.fleet.path_of(key));
        let mut files = vec![path.clone()];
        files.extend(SIDECARS.iter().map(|suffix| {
            let mut name = path.clone().into_os_string();
            name.push(suffix);
            PathBuf::from(name)
        }));
        for file in files {
            match std::fs::remove_file(&file) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => tracing::warn!(
                    tenant = %key,
                    file = %file.display(),
                    %error,
                    "cannot remove the tenant Harvest file"
                ),
            }
        }
    }
}

/// The fleet hooks. A weak link: the fleet holds the hooks, and the shared
/// state holds the fleet.
struct TenantLifecycle(Weak<Shared>);

impl FleetLifecycle for TenantLifecycle {
    fn on_open(&self, db: &FleetDatabase) -> Result<(), String> {
        self.0.upgrade().map_or(Ok(()), |shared| shared.start(db))
    }

    fn on_close(&self, db: &FleetDatabase) {
        if let Some(shared) = self.0.upgrade() {
            shared.forget(db.key());
        }
    }

    fn on_delete(&self, key: &FleetDbKey) {
        if let Some(shared) = self.0.upgrade() {
            shared.forget(key);
            shared.erase(key);
        }
    }
}

impl axum::extract::FromRequestParts<AppState> for HarvestFleet {
    type Rejection = AutumnError;

    fn from_request_parts(
        _parts: &mut axum::http::request::Parts,
        state: &AppState,
    ) -> impl std::future::Future<Output = Result<Self, Self::Rejection>> + Send {
        std::future::ready(
            state
                .extension::<Self>()
                .map(|fleet| (*fleet).clone())
                .ok_or_else(|| {
                    AutumnError::service_unavailable_msg(
                        "per-tenant Harvest is not running: call HarvestPlugin::sqlite_per_tenant",
                    )
                }),
        )
    }
}

/// Run `future` with `key` as the current tenant.
pub fn scoped<F: std::future::Future>(
    key: FleetDbKey,
    future: F,
) -> impl std::future::Future<Output = F::Output> {
    CURRENT.scope(key, future)
}
