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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::{Duration, Instant};

use autumn_web::AppState;
use autumn_web::db::fleet::{DatabaseFleet, FleetDatabase, FleetLifecycle};
use autumn_web::error::AutumnError;
use autumn_web::fleet_layout::FleetDbKey;
use autumn_web::reexports::axum;
use tokio_util::sync::CancellationToken;

use super::driver::{Driver, Registry};
use super::handle::Catalog;
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

/// The fleet database of the running activity.
///
/// A tenant driver runs each activity inline on its own task. That task
/// carries the database key. In a `slot` fleet the key is the slot, which
/// many tenants share. Code on the control driver, or on a task that an
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

/// One tenant in the registry. The number is the start generation.
///
/// Each start takes a new generation. A start or a driver exit changes only
/// the slot of its own generation, so a late one never removes a newer
/// driver.
enum Slot {
    /// A task opens the file now. Another start returns at once.
    Starting(u64),
    /// The driver runs. This copy holds no lease.
    Running(u64, HarvestSqlite),
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
    /// `None` once [`HarvestFleet::release`] ran. The registry holds a clone
    /// of `AppState`, which holds this fleet, so the release breaks a cycle.
    registry: Mutex<Option<Arc<Registry>>>,
    /// The definitions that a tenant handle reads (issue #2468).
    catalog: Arc<Catalog>,
    tick: Duration,
    tenants: Mutex<HashMap<FleetDbKey, Slot>>,
    /// The last start generation.
    generation: AtomicU64,
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
    pub(crate) fn new(fleet: DatabaseFleet, registry: Arc<Registry>, tick: Duration) -> Self {
        Self {
            shared: Arc::new(Shared {
                fleet,
                catalog: Arc::new(Catalog::of(&registry)),
                registry: Mutex::new(Some(registry)),
                tick,
                tenants: Mutex::new(HashMap::new()),
                generation: AtomicU64::new(0),
            }),
        }
    }

    /// Install the lifecycle on the fleet, then start a driver for each
    /// tenant that is open already. The fleet calls no `on_open` for those.
    pub(crate) async fn install(&self) {
        self.shared
            .fleet
            .add_lifecycle(Arc::new(TenantLifecycle(Arc::downgrade(&self.shared))));
        let fleet = &self.shared.fleet;
        for key in fleet.open_keys() {
            // A tenant in the middle of its open may have read the hooks
            // before the install. A background open waits for that open.
            let (db, opened) = match fleet.get_open(&key) {
                Some(db) => (db, false),
                None => match fleet.open_background(&key).await {
                    Ok(Some(background)) => (background.db, background.opened),
                    Ok(None) | Err(_) => continue,
                },
            };
            if let Err(error) = self.start_blocking(db).await {
                tracing::warn!(tenant = %key, %error, "cannot start the tenant Harvest");
            }
            if opened {
                // The scan opened a closed tenant. Leave it closed again.
                let _ = fleet.close_if_unused(&key);
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

    /// As [`open_for`](Self::open_for), but only for a database that exists.
    ///
    /// The management API uses it, so a request never creates a tenant,
    /// whatever the fleet's `create_on_demand`.
    ///
    /// It uses a background open. That open never creates a database, so a
    /// delete that races the request cannot bring the tenant back. It never
    /// goes past `max_open`, and it does not count as a use. So a management
    /// read never evicts a tenant that the app uses.
    pub(crate) async fn open_existing_for(
        &self,
        routing_key: &str,
    ) -> Result<HarvestSqlite, HarvestSqliteError> {
        let key = self.shared.fleet.key_for(routing_key)?;
        let Some(background) = self.shared.fleet.open_background(&key).await? else {
            return Err(HarvestSqliteError::Tenant(format!(
                "the fleet has no room to open {key} now (max_open)"
            )));
        };
        self.handle_for(background.db).await
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
        self.handle_for(db).await
    }

    /// Release the shared registry at shutdown. Later starts refuse.
    ///
    /// The registry holds a clone of `AppState`, and `AppState` holds this
    /// fleet. Without the release, an app that stops never frees its state.
    /// A running tenant driver keeps its own copy until the fleet closes it.
    pub fn release(&self) {
        *self
            .shared
            .registry
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = None;
    }

    /// The leased handle of the open database `db`.
    async fn handle_for(&self, db: FleetDatabase) -> Result<HarvestSqlite, HarvestSqliteError> {
        let key = db.key().clone();
        let lease = db.lease();
        let deadline = Instant::now() + START_WAIT;
        loop {
            match self.state(&key) {
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
            .filter(|(_, slot)| matches!(slot, Slot::Running(..)))
            .map(|(key, _)| key.clone())
            .collect();
        drop(tenants);
        keys.sort();
        keys
    }

    fn state(&self, key: &FleetDbKey) -> TenantState {
        match self.shared.lock().get(key) {
            None => TenantState::Absent,
            Some(Slot::Starting(_)) => TenantState::Starting,
            Some(Slot::Running(_, handle)) => TenantState::Running(handle.clone()),
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
    ///
    /// # Errors
    ///
    /// Returns a message when the file cannot open, or when the fleet closed
    /// the tenant while its Harvest started.
    fn start(self: &Arc<Self>, db: &FleetDatabase) -> Result<(), String> {
        let key = db.key().clone();
        let generation = {
            let mut tenants = self.lock();
            if tenants.contains_key(&key) {
                return Ok(());
            }
            let generation = self.generation.fetch_add(1, Ordering::Relaxed) + 1;
            tenants.insert(key.clone(), Slot::Starting(generation));
            generation
        };
        // A failed or a panicked start clears its `Starting` slot.
        let mut guard = StartGuard {
            shared: self,
            key: &key,
            generation,
            armed: true,
        };
        let handle = self.open_driver(db, generation)?;
        guard.armed = false;
        let mut tenants = self.lock();
        if !matches!(tenants.get(&key), Some(Slot::Starting(slot)) if *slot == generation) {
            // A close removed the slot. The fleet has stopped the driver.
            return Err(format!("{key} closed while its Harvest started"));
        }
        tenants.insert(key.clone(), Slot::Running(generation, handle));
        drop(tenants);
        tracing::info!(tenant = %key, "tenant Harvest is running");
        Ok(())
    }

    fn open_driver(
        self: &Arc<Self>,
        db: &FleetDatabase,
        generation: u64,
    ) -> Result<HarvestSqlite, String> {
        let key = db.key().clone();
        let path = tenant_harvest_file(db.path());
        let tokio = tokio::runtime::Handle::try_current()
            .map_err(|error| format!("no Tokio runtime to run the Harvest of {key}: {error}"))?;
        let registry = self
            .registry
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .ok_or_else(|| {
                format!("the per-tenant Harvest has stopped, so {key} gets no driver")
            })?;
        let runtime = Driver::open(&path, &registry)?;
        drop(registry);
        let reader = autumn_harvest_sqlite::SqliteReader::open(&path)
            .map_err(|error| format!("cannot open a reader on {}: {error}", path.display()))?;
        let stop = CancellationToken::new();
        let weak = Arc::downgrade(self);
        let exit_key = key.clone();
        // The driver clears its own slot when it ends, so a dead driver
        // never stays in the registry.
        let on_exit = Box::new(move || {
            if let Some(shared) = weak.upgrade() {
                shared.remove_generation(&exit_key, generation);
            }
        });
        let _entered = tokio.enter();
        let (sender, task) =
            Driver::spawn_tenant(runtime, self.tick, key.clone(), stop.clone(), on_exit);
        // The fleet stops the driver before it closes the database.
        db.register_worker(stop.clone(), task);
        if stop.is_cancelled() {
            // The fleet had begun to close the tenant, so it stopped the
            // driver at once.
            return Err(format!("{key} closed while its Harvest started"));
        }
        Ok(HarvestSqlite::new(sender, reader, path, Arc::clone(&self.catalog)).for_tenant(key))
    }

    /// Remove the slot of `key` when it still has `generation`.
    fn remove_generation(&self, key: &FleetDbKey, generation: u64) {
        let mut tenants = self.lock();
        let ours = match tenants.get(key) {
            Some(Slot::Starting(slot) | Slot::Running(slot, _)) => *slot == generation,
            None => false,
        };
        if ours {
            tenants.remove(key);
        }
    }

    fn released(&self) -> bool {
        self.registry
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_none()
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

/// Clears a `Starting` slot when its start does not finish.
struct StartGuard<'a> {
    shared: &'a Shared,
    key: &'a FleetDbKey,
    generation: u64,
    armed: bool,
}

impl Drop for StartGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.shared.remove_generation(self.key, self.generation);
        }
    }
}

/// The fleet hooks. A weak link: the fleet holds the hooks, and the shared
/// state holds the fleet.
struct TenantLifecycle(Weak<Shared>);

impl FleetLifecycle for TenantLifecycle {
    fn on_open(&self, db: &FleetDatabase) -> Result<(), String> {
        match self.0.upgrade() {
            // After the release, the app still opens its tenants. They only
            // get no Harvest driver.
            Some(shared) if shared.released() => Ok(()),
            Some(shared) => shared.start(db),
            None => Ok(()),
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn shared(root: &Path) -> Arc<Shared> {
        let config: autumn_web::config::DatabaseFleetConfig =
            serde_json::from_value(serde_json::json!({
                "mode": "tenant",
                "root": root.display().to_string(),
            }))
            .expect("fleet config");
        let fleet = DatabaseFleet::builder(config).build().expect("fleet");
        let registry = Arc::new(Registry {
            workflows: Vec::new(),
            activities: Vec::new(),
            updates: Vec::new(),
            signals: Vec::new(),
            queries: Vec::new(),
            log_policy: None,
            state: autumn_harvest::context::empty_shared_state(),
        });
        HarvestFleet::new(fleet, registry, Duration::from_millis(250)).shared
    }

    fn acme(shared: &Shared) -> FleetDbKey {
        shared.fleet.key_for("acme").expect("key")
    }

    /// A late exit of an old driver never removes the slot of a newer one.
    #[test]
    fn only_the_own_generation_leaves_the_registry() {
        let dir = tempfile::tempdir().unwrap();
        let shared = shared(dir.path());
        let key = acme(&shared);
        shared.lock().insert(key.clone(), Slot::Starting(2));
        shared.remove_generation(&key, 1);
        assert!(shared.lock().contains_key(&key), "an old generation");
        shared.remove_generation(&key, 2);
        assert!(!shared.lock().contains_key(&key), "its own generation");
    }

    /// A start that fails or panics never leaves its tenant `Starting`.
    #[test]
    fn an_unfinished_start_clears_its_slot() {
        let dir = tempfile::tempdir().unwrap();
        let shared = shared(dir.path());
        let key = acme(&shared);
        shared.lock().insert(key.clone(), Slot::Starting(7));
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = StartGuard {
                shared: &shared,
                key: &key,
                generation: 7,
                armed: true,
            };
            panic!("the open panicked");
        }));
        assert!(panicked.is_err());
        assert!(!shared.lock().contains_key(&key));

        shared.lock().insert(key.clone(), Slot::Starting(8));
        drop(StartGuard {
            shared: &shared,
            key: &key,
            generation: 8,
            armed: false,
        });
        assert!(shared.lock().contains_key(&key), "a finished start stays");
    }

    /// The registry holds a clone of `AppState`, and `AppState` holds the
    /// fleet handle. A release breaks that cycle at shutdown, and a later
    /// start refuses.
    #[tokio::test]
    async fn a_release_drops_the_registry() {
        let dir = tempfile::tempdir().unwrap();
        let config: autumn_web::config::DatabaseFleetConfig =
            serde_json::from_value(serde_json::json!({
                "mode": "tenant",
                "root": dir.path().display().to_string(),
                "create_on_demand": true,
            }))
            .expect("fleet config");
        let fleet = DatabaseFleet::builder(config).build().expect("fleet");
        let registry = Arc::new(Registry {
            workflows: Vec::new(),
            activities: Vec::new(),
            updates: Vec::new(),
            signals: Vec::new(),
            queries: Vec::new(),
            log_policy: None,
            state: autumn_harvest::context::empty_shared_state(),
        });
        let harvest = HarvestFleet::new(
            fleet.clone(),
            Arc::clone(&registry),
            Duration::from_millis(250),
        );
        harvest.install().await;
        assert_eq!(Arc::strong_count(&registry), 2);
        harvest.release();
        assert_eq!(Arc::strong_count(&registry), 1, "the fleet let go");
        let error = harvest.open_for("acme").await.unwrap_err();
        assert!(error.to_string().contains("stopped"), "{error}");
        fleet.close_all().await;
    }

    #[test]
    fn the_harvest_file_sits_next_to_the_tenant_file() {
        assert_eq!(
            tenant_harvest_file(Path::new("/f/00a/acme.db")),
            PathBuf::from("/f/00a/acme.harvest.db")
        );
    }
}
