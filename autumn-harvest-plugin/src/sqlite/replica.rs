//! The replica of each tenant Harvest file.
//!
//! Fleet replication ships `acme.db`. This module ships `acme.harvest.db`
//! under the same tenant prefix, with the same engine, destination and
//! epoch. A takeover then restores the runs of the tenant with its data.
//!
//! ```text
//! <base>/fleet/tenant/<id>/generations/..           the tenant database
//! <base>/fleet/tenant/<id>/harvest/generations/..   its Harvest file
//! ```
//!
//! The fleet purge lists `<base>/fleet/tenant/<id>/` and the delete marker
//! sits there. So one tombstone covers both files, and one purge removes
//! both. The fleet restore reads `generations/` and `epochs/` of the tenant
//! prefix only, so it never reads a Harvest key.
//!
//! ```text
//! on_open  ─► restore or replace the file ─► open it ─► raise its epoch
//!             ─► attach a shipper
//! loop     ─► each tick: check the ownership epoch, then ship
//! on_close ─► ship until caught up, or park the shipper and retry
//! release  ─► drop a parked shipper, restore at the next open
//! on_delete ─► forget the tenant
//! ```
//!
//! Autumn ADR 0040 gives a lifecycle hook a destination scoped to
//! `<key prefix>/<engine name>/`. This module uses that layout now, on
//! the public parts of autumn-web 0.10.

use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use autumn_web::db::fleet::DatabaseFleet;
use autumn_web::db::fleet_replication::replica_root;
use autumn_web::db::fleet_tombstones::restore_allowed;
use autumn_web::fleet_layout::FleetDbKey;
use autumn_web::replication::restore::{self, RestoreError};
use autumn_web::replication::{
    ReplicaDestination, ReplicationSettings, ReplicationStatus, Replicator, segment,
};
use autumn_web::time::ClockSource;
use chrono::{DateTime, Utc};

/// The engine name of the Harvest file under the prefix of its tenant.
pub const REPLICA_ENGINE: &str = "harvest";

/// Most ticks that the close-time ship takes. One tick ships one segment.
const FINAL_SHIP_MAX_TICKS: usize = 64;

/// The longest that a close waits for another process to free the lock.
const CLAIM_WAIT: Duration = Duration::from_secs(2);

/// The pause between two claims at a close.
const CLAIM_POLL: Duration = Duration::from_millis(25);

/// The pause of the loop between two checks of its stop flag.
const STOP_POLL: Duration = Duration::from_millis(100);

/// The SQLite sidecars of a database file.
const SIDECARS: [&str; 3] = ["-wal", "-shm", "-journal"];

/// The replica prefix of the Harvest file of `key`, under the replication
/// root `base`.
///
/// ```rust,ignore
/// // "app/prod/fleet/tenant/<id>/harvest"
/// let root = tenant_harvest_replica_root("app/prod", &key);
/// ```
#[must_use]
pub fn tenant_harvest_replica_root(base: &str, key: &FleetDbKey) -> String {
    format!("{}/{REPLICA_ENGINE}", replica_root(base, key))
}

/// Where the tenant Harvest files ship.
///
/// Give it the destination and the settings that the fleet replication
/// uses. `settings.root` is the replication root, and each tenant file
/// ships under [`tenant_harvest_replica_root`]. `database_path` and
/// `verify_interval` are not used.
///
/// The plugin builds one from `[replication]` when the fleet replicates.
/// Pass one to `HarvestPlugin::sqlite_tenant_replica` to set it in code.
#[derive(Clone)]
pub struct TenantReplica {
    destination: Arc<dyn ReplicaDestination>,
    settings: ReplicationSettings,
}

impl std::fmt::Debug for TenantReplica {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantReplica")
            .field("destination", &self.destination.describe())
            .field("root", &self.settings.root)
            .finish_non_exhaustive()
    }
}

impl TenantReplica {
    /// Ship to `destination`, under the replication root `settings.root`.
    #[must_use]
    pub fn new(destination: Arc<dyn ReplicaDestination>, settings: ReplicationSettings) -> Self {
        Self {
            destination,
            settings,
        }
    }

    /// The replication root that each tenant prefix hangs off.
    #[must_use]
    pub fn root(&self) -> &str {
        &self.settings.root
    }

    /// The replica prefix of the Harvest file of `key`.
    #[must_use]
    pub fn root_for(&self, key: &FleetDbKey) -> String {
        tenant_harvest_replica_root(&self.settings.root, key)
    }
}

/// The restore that the next open of a tenant runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pending {
    /// The fleet replaced the tenant database. Take the latest state.
    Latest,
    /// `HarvestFleet::restore` asked for this point in time.
    At(DateTime<Utc>),
}

impl Pending {
    const fn target(self) -> Option<DateTime<Utc>> {
        match self {
            Self::Latest => None,
            Self::At(at) => Some(at),
        }
    }
}

/// The replicator of one open Harvest file.
struct Shipper {
    replicator: Mutex<Replicator>,
    /// The OS lock `<file>.replicator.lock`. One process ships a file.
    lock: Mutex<Option<File>>,
    path: PathBuf,
    /// The ownership epoch that the file opened under.
    epoch: Option<u64>,
    /// Set once the shipper must ship nothing more.
    fenced: AtomicBool,
}

impl Shipper {
    /// Take the replicator lock. `true` when this process holds it.
    fn claim(&self) -> bool {
        let mut lock = self.lock.lock().unwrap_or_else(PoisonError::into_inner);
        if lock.is_none() {
            *lock = self.try_lock_file();
        }
        lock.is_some()
    }

    /// Take the OS lock, or `None` when another process holds it.
    fn try_lock_file(&self) -> Option<File> {
        let mut name = self.path.clone().into_os_string();
        name.push(".replicator.lock");
        let opened = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(PathBuf::from(name));
        match opened {
            Ok(file) => file.try_lock().is_ok().then_some(file),
            Err(error) => {
                tracing::warn!(file = %self.path.display(), %error, "cannot open the Harvest replicator lock");
                None
            }
        }
    }

    fn replicator(&self) -> MutexGuard<'_, Replicator> {
        self.replicator
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn fenced(&self) -> bool {
        self.fenced.load(Ordering::Acquire)
    }
}

/// Why a restore did not publish a file.
enum Restored {
    /// The file is in place.
    Done,
    /// The replica has nothing for the request. Keep the local file.
    Nothing(String),
}

/// The shippers of the open tenant Harvest files.
pub struct Replicas {
    replica: TenantReplica,
    clock: Arc<dyn ClockSource>,
    fleet: DatabaseFleet,
    open: Mutex<HashMap<FleetDbKey, Arc<Shipper>>>,
    /// Shippers of closed tenants whose last ship failed. Each keeps its
    /// connection, so no checkpoint drops the frames that it did not ship.
    parked: Mutex<HashMap<FleetDbKey, Arc<Shipper>>>,
    pending: Mutex<HashMap<FleetDbKey, Pending>>,
    stopped: AtomicBool,
}

impl Replicas {
    pub fn new(replica: TenantReplica, clock: Arc<dyn ClockSource>, fleet: DatabaseFleet) -> Self {
        Self {
            replica,
            clock,
            fleet,
            open: Mutex::new(HashMap::new()),
            parked: Mutex::new(HashMap::new()),
            pending: Mutex::new(HashMap::new()),
            stopped: AtomicBool::new(false),
        }
    }

    fn open_map(&self) -> MutexGuard<'_, HashMap<FleetDbKey, Arc<Shipper>>> {
        self.open.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn parked_map(&self) -> MutexGuard<'_, HashMap<FleetDbKey, Arc<Shipper>>> {
        self.parked.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn pending_map(&self) -> MutexGuard<'_, HashMap<FleetDbKey, Pending>> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn destination(&self) -> &dyn ReplicaDestination {
        self.replica.destination.as_ref()
    }

    /// Whether this host holds the range of `key` at `epoch`. Always `true`
    /// without ownership.
    fn holds(&self, key: &FleetDbKey, epoch: Option<u64>) -> bool {
        self.fleet.ownership().is_none_or(|ownership| {
            ownership
                .epoch_for(key.routing_slot())
                .map(autumn_web::lock::FencingToken::get)
                == epoch
        })
    }

    /// Make the Harvest file of `key` current before its runtime opens it.
    ///
    /// - After a restore of the tenant database, restore the file too.
    /// - A missing file restores from the replica, as the fleet restores a
    ///   missing database.
    /// - A file from an older epoch of another owner is replaced.
    ///
    /// Blocking.
    ///
    /// # Errors
    ///
    /// Returns a message when the replica cannot be read, or when a needed
    /// restore fails. The open is then refused, so no stale file serves.
    pub fn prepare(&self, key: &FleetDbKey, path: &Path, epoch: Option<u64>) -> Result<(), String> {
        let pending = self.pending_map().remove(key);
        if let Some(pending) = pending {
            self.drop_parked(key);
            return match self.restore_in_place(key, path, pending.target()) {
                Ok(Restored::Done) => {
                    tracing::info!(tenant = %key, "tenant Harvest file restored with its database");
                    Ok(())
                }
                Ok(Restored::Nothing(why)) => {
                    tracing::warn!(tenant = %key, %why, "the tenant database was restored, but its Harvest file has no replica to match; the local file stays");
                    Ok(())
                }
                Err(error) => {
                    // The next open tries again.
                    self.pending_map().entry(key.clone()).or_insert(pending);
                    Err(error)
                }
            };
        }
        if !path.exists() {
            return self.restore_missing(key, path);
        }
        epoch.map_or(Ok(()), |epoch| self.replace_if_stale(key, path, epoch))
    }

    /// Restore a missing file from its replica, under the fleet's rules.
    fn restore_missing(&self, key: &FleetDbKey, path: &Path) -> Result<(), String> {
        let Some(replication) = self.fleet.replication() else {
            return Ok(());
        };
        if !replication.restores_missing() {
            return Ok(());
        }
        let root = self.replica.root_for(key);
        let plan = match restore::plan(self.destination(), &root, None) {
            Ok(plan) => plan,
            Err(RestoreError::NoReplica { .. }) => return Ok(()),
            Err(error) => return Err(format!("cannot plan the Harvest restore of {key}: {error}")),
        };
        let row = tombstone_row(replication, key)?;
        let marker = replication.marker(key)?.map(|marker| marker.deleted_at());
        if !restore_allowed(row, marker, plan.generation_started_at) {
            tracing::info!(tenant = %key, "the tenant was deleted: its Harvest file is not restored");
            return Ok(());
        }
        let staging = staging_of(path);
        let applied = restore::apply(self.destination(), &plan, &staging);
        let result = match applied {
            Ok(_) => {
                remove_sidecars(path);
                match std::fs::hard_link(&staging, path) {
                    Ok(()) => {
                        tracing::info!(tenant = %key, "tenant Harvest file restored from its replica");
                        Ok(())
                    }
                    // Another opener published it first. Open theirs.
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
                    Err(error) => Err(format!(
                        "cannot publish the restored Harvest file of {key}: {error}"
                    )),
                }
            }
            Err(error) => Err(format!("cannot restore the Harvest file of {key}: {error}")),
        };
        discard(&staging);
        result
    }

    /// Replace the file when another owner shipped a newer epoch of it.
    fn replace_if_stale(&self, key: &FleetDbKey, path: &Path, epoch: u64) -> Result<(), String> {
        let stamp = autumn_harvest_sqlite::fleet_epoch::read(path)
            .map_err(|error| format!("cannot read the epoch of {}: {error}", path.display()))?;
        if stamp >= epoch {
            return Ok(());
        }
        let newest = self.newest_epoch(key)?;
        // An epoch belongs to one owner, so a replica at `epoch` is ours.
        if !newest.is_some_and(|newest| newest > stamp && newest != epoch) {
            return Ok(());
        }
        tracing::warn!(
            tenant = %key,
            stamp,
            newest = newest.unwrap_or_default(),
            epoch,
            "the tenant Harvest file is older than its replica: another host owned it; it is restored from the replica"
        );
        self.drop_parked(key);
        match self.restore_in_place(key, path, None)? {
            Restored::Done => Ok(()),
            Restored::Nothing(why) => Err(format!(
                "the Harvest file of {key} is stale, and its replica cannot replace it: {why}"
            )),
        }
    }

    /// The newest epoch that any host recorded at the replica of `key`.
    fn newest_epoch(&self, key: &FleetDbKey) -> Result<Option<u64>, String> {
        let root = self.replica.root_for(key);
        let listed = self
            .destination()
            .list(&segment::epochs_prefix(&root))
            .map_err(|error| format!("cannot read the Harvest replica epochs of {key}: {error}"))?;
        Ok(listed
            .iter()
            .filter_map(|marker| segment::parse_epoch_marker_key(&root, marker))
            .max())
    }

    /// Rebuild the file from its replica, as of `target`, over the local
    /// file. It holds the Harvest writer lock while it replaces the file.
    fn restore_in_place(
        &self,
        key: &FleetDbKey,
        path: &Path,
        target: Option<DateTime<Utc>>,
    ) -> Result<Restored, String> {
        let root = self.replica.root_for(key);
        let plan = match restore::plan(self.destination(), &root, target) {
            Ok(plan) => plan,
            Err(
                error @ (RestoreError::NoReplica { .. }
                | RestoreError::TargetBeforeRetention { .. }),
            ) => {
                return Ok(Restored::Nothing(error.to_string()));
            }
            Err(error) => return Err(format!("cannot plan the Harvest restore of {key}: {error}")),
        };
        let staging = staging_of(path);
        if let Err(error) = restore::apply(self.destination(), &plan, &staging) {
            discard(&staging);
            return Err(format!("cannot restore the Harvest file of {key}: {error}"));
        }
        let lock = if path.exists() {
            match autumn_harvest_sqlite::FileWriterLock::acquire(path) {
                Ok(lock) => Some(lock),
                Err(error) => {
                    discard(&staging);
                    return Err(format!(
                        "cannot lock {} to replace it: {error}",
                        path.display()
                    ));
                }
            }
        } else {
            None
        };
        // The old frames must not replay onto the restored file.
        remove_sidecars(path);
        let published = std::fs::rename(&staging, path)
            .map_err(|error| format!("cannot publish the restored Harvest file of {key}: {error}"));
        drop(lock);
        discard(&staging);
        published.map(|()| Restored::Done)
    }

    /// Ship the open Harvest file of `key`. Call it after the runtime opened
    /// the file with `SqliteRuntime::open_replicated`.
    pub fn attach(&self, key: &FleetDbKey, path: &Path, epoch: Option<u64>) {
        let parked = self.parked_map().remove(key);
        if let Some(parked) = parked
            && !parked.fenced()
            && parked.epoch == epoch
        {
            // It still holds the frames that it did not ship.
            self.open_map().insert(key.clone(), parked);
            return;
        }
        let settings = ReplicationSettings {
            database_path: path.to_path_buf(),
            root: self.replica.root_for(key),
            // A verification restores the whole file. That is the
            // operator's call, not a timer's.
            verify_interval: None,
            ..self.replica.settings.clone()
        };
        let status = Arc::new(ReplicationStatus::new(self.destination().describe()));
        let replicator = Replicator::new(settings, Arc::clone(&self.replica.destination), status)
            .with_clock(Arc::clone(&self.clock))
            .with_epoch(epoch);
        let shipper = Arc::new(Shipper {
            replicator: Mutex::new(replicator),
            lock: Mutex::new(None),
            path: path.to_path_buf(),
            epoch,
            fenced: AtomicBool::new(false),
        });
        shipper.claim();
        self.open_map().insert(key.clone(), shipper);
    }

    /// Ship each open file once, and retry each parked one. Blocking.
    pub fn tick_all(&self) {
        let open: Vec<(FleetDbKey, Arc<Shipper>)> = self
            .open_map()
            .iter()
            .map(|(key, shipper)| (key.clone(), Arc::clone(shipper)))
            .collect();
        for (key, shipper) in open {
            self.tick(&key, &shipper);
        }
        let parked: Vec<(FleetDbKey, Arc<Shipper>)> = self
            .parked_map()
            .iter()
            .map(|(key, shipper)| (key.clone(), Arc::clone(shipper)))
            .collect();
        for (key, shipper) in parked {
            self.tick_parked(&key, &shipper);
        }
    }

    /// One tick of an open file. A host that lost the range ships nothing.
    fn tick(&self, key: &FleetDbKey, shipper: &Shipper) {
        if shipper.fenced() {
            return;
        }
        if !self.holds(key, shipper.epoch) {
            fence(key, shipper, "this host lost the range");
            return;
        }
        if !shipper.claim() {
            return;
        }
        // A close or another tick has it. The next tick ships.
        let Ok(mut replicator) = shipper.replicator.try_lock() else {
            return;
        };
        if let Err(error) = replicator.tick() {
            tracing::debug!(tenant = %key, %error, "tenant Harvest replication tick failed");
        }
        if let Some(newest) = replicator.fenced_by() {
            fence(key, shipper, &format!("epoch {newest} took over"));
        }
    }

    /// Ship the backlog of a closed file. Drop its shipper once caught up.
    fn tick_parked(&self, key: &FleetDbKey, shipper: &Arc<Shipper>) {
        let caught_up = self.ship_parked(key, shipper);
        if caught_up || shipper.fenced() {
            let mut parked = self.parked_map();
            if parked
                .get(key)
                .is_some_and(|current| Arc::ptr_eq(current, shipper))
            {
                parked.remove(key);
            }
        }
    }

    /// Ship a parked file until caught up. `true` once nothing is left.
    fn ship_parked(&self, key: &FleetDbKey, shipper: &Shipper) -> bool {
        let mut replicator = shipper.replicator();
        let caught_up = !shipper.fenced()
            && self.holds(key, shipper.epoch)
            && ship_until_caught_up(&mut replicator).is_ok();
        if replicator.fenced_by().is_some() {
            shipper.fenced.store(true, Ordering::Release);
        }
        drop(replicator);
        caught_up
    }

    /// The tenant closed. Ship what its runs committed. Blocking.
    ///
    /// The fleet stopped the driver first, so no new frame comes.
    pub fn close(&self, key: &FleetDbKey) {
        let Some(shipper) = self.open_map().remove(key) else {
            return;
        };
        if shipper.fenced() {
            return;
        }
        if !self.holds(key, shipper.epoch) {
            fence(key, &shipper, "this host lost the range");
            return;
        }
        if !claim_for_close(&shipper) {
            return;
        }
        let mut replicator = shipper.replicator();
        let outcome = ship_until_caught_up(&mut replicator);
        let newest = replicator.fenced_by();
        drop(replicator);
        if let Some(newest) = newest {
            fence(key, &shipper, &format!("epoch {newest} took over"));
            return;
        }
        if let Err(error) = outcome {
            tracing::warn!(tenant = %key, %error, "the close-time ship of the tenant Harvest file did not catch up; it keeps shipping");
            self.parked_map().insert(key.clone(), shipper);
        }
    }

    /// A delete or a restore of the tenant database starts. Let go of the
    /// file. The next open restores the Harvest file, unless a delete
    /// follows: [`forget`](Self::forget) clears that.
    pub fn release(&self, key: &FleetDbKey) {
        self.drop_parked(key);
        self.pending_map()
            .entry(key.clone())
            .or_insert(Pending::Latest);
    }

    /// The tenant is deleted.
    pub fn forget(&self, key: &FleetDbKey) {
        self.drop_parked(key);
        self.pending_map().remove(key);
    }

    /// `HarvestFleet::restore` asks for `pending` at the next open.
    pub fn expect_restore(&self, key: &FleetDbKey, pending: Pending) {
        self.pending_map().insert(key.clone(), pending);
    }

    /// A restore that `expect_restore` announced failed.
    pub fn cancel_restore(&self, key: &FleetDbKey) {
        self.pending_map().remove(key);
    }

    fn drop_parked(&self, key: &FleetDbKey) {
        let parked = self.parked_map().remove(key);
        if let Some(parked) = parked {
            // A tick in flight finishes first. Then nothing ships again.
            parked.fenced.store(true, Ordering::Release);
            drop(parked.replicator());
        }
    }

    /// Tick every `interval` on a thread, until [`stop`](Self::stop).
    ///
    /// # Errors
    ///
    /// The OS refused to start the thread.
    pub fn spawn_loop(self: &Arc<Self>) -> std::io::Result<std::thread::JoinHandle<()>> {
        let this = Arc::clone(self);
        let interval = self.replica.settings.sync_interval;
        std::thread::Builder::new()
            .name("harvest-tenant-replica".to_owned())
            .spawn(move || {
                while !this.stopped.load(Ordering::Acquire) {
                    let started = std::time::Instant::now();
                    this.tick_all();
                    let pause = interval.saturating_sub(started.elapsed());
                    let mut slept = Duration::ZERO;
                    while slept < pause && !this.stopped.load(Ordering::Acquire) {
                        let step = STOP_POLL.min(pause.saturating_sub(slept));
                        std::thread::sleep(step);
                        slept += step;
                    }
                }
            })
    }

    /// Stop the loop after its current pass. A close still ships.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
    }
}

fn fence(key: &FleetDbKey, shipper: &Shipper, why: &str) {
    if !shipper.fenced.swap(true, Ordering::AcqRel) {
        tracing::warn!(tenant = %key, why, "the tenant Harvest file ships nothing more");
    }
}

/// Claim at a close. Another process can hold the lock while it ships.
fn claim_for_close(shipper: &Shipper) -> bool {
    let limit = std::time::Instant::now() + CLAIM_WAIT;
    loop {
        if shipper.claim() {
            return true;
        }
        if std::time::Instant::now() >= limit {
            return false;
        }
        std::thread::sleep(CLAIM_POLL);
    }
}

/// Tick until nothing committed is left to ship.
fn ship_until_caught_up(replicator: &mut Replicator) -> Result<(), String> {
    for _ in 0..FINAL_SHIP_MAX_TICKS {
        let report = replicator.tick().map_err(|error| error.to_string())?;
        if report.pending_bytes == 0 {
            return Ok(());
        }
        if report.segments == 0 && !report.snapshot_taken && !report.checkpointed {
            return Err(format!(
                "{} WAL bytes are pending and the last tick shipped nothing",
                report.pending_bytes
            ));
        }
    }
    Err(format!("still behind after {FINAL_SHIP_MAX_TICKS} ticks"))
}

/// Whether the control database holds a tombstone row for `key`.
fn tombstone_row(
    replication: &autumn_web::db::fleet_replication::FleetReplication,
    key: &FleetDbKey,
) -> Result<bool, String> {
    let Some(tombstones) = replication.tombstones() else {
        return Ok(false);
    };
    let runtime = tokio::runtime::Handle::try_current()
        .map_err(|error| format!("no Tokio runtime to read the tombstone of {key}: {error}"))?;
    runtime
        .block_on(tombstones.get(&key.name()))
        .map(|row| row.is_some())
        .map_err(|error| format!("cannot read the tombstone of {key}: {error}"))
}

/// The private path that a restore builds before it publishes.
fn staging_of(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".restoring");
    PathBuf::from(name)
}

/// The SQLite sidecars of `path`.
fn sidecars(path: &Path) -> impl Iterator<Item = PathBuf> + '_ {
    SIDECARS.iter().map(move |suffix| {
        let mut name = path.as_os_str().to_os_string();
        name.push(suffix);
        PathBuf::from(name)
    })
}

/// Remove `path` and its sidecars. A missing file is not an error.
fn discard(path: &Path) {
    for file in std::iter::once(path.to_path_buf()).chain(sidecars(path)) {
        let _ = std::fs::remove_file(file);
    }
}

/// Remove the sidecars of `path`, and `path` itself.
fn remove_sidecars(path: &Path) {
    discard(path);
}

#[cfg(test)]
mod tests {
    use super::*;
    use autumn_web::replication::FileDestination;

    fn fleet(root: &Path) -> DatabaseFleet {
        let config: autumn_web::config::DatabaseFleetConfig =
            serde_json::from_value(serde_json::json!({
                "mode": "tenant",
                "root": root.display().to_string(),
            }))
            .expect("fleet config");
        DatabaseFleet::builder(config).build().expect("fleet")
    }

    fn settings() -> ReplicationSettings {
        ReplicationSettings {
            database_path: PathBuf::new(),
            root: "app/test".to_owned(),
            sync_interval: Duration::from_secs(1),
            snapshot_interval: Duration::from_secs(3600),
            max_wal_bytes: 4 * 1024 * 1024,
            retention: Duration::from_secs(3600),
            verify_interval: None,
        }
    }

    fn replicas(dir: &Path) -> Replicas {
        let destination: Arc<dyn ReplicaDestination> =
            Arc::new(FileDestination::new(dir.join("replicas")).expect("destination"));
        Replicas::new(
            TenantReplica::new(destination, settings()),
            Arc::new(autumn_web::time::SystemClock),
            fleet(&dir.join("fleet")),
        )
    }

    #[test]
    fn the_harvest_replica_sits_under_the_tenant_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let fleet = fleet(dir.path());
        let key = fleet.key_for("acme").unwrap();
        let root = tenant_harvest_replica_root("app/test", &key);
        assert_eq!(root, format!("{}/harvest", replica_root("app/test", &key)));
        assert!(root.starts_with("app/test/fleet/tenant/"), "{root}");
    }

    /// A shipped file comes back byte for byte, and a release asks the
    /// next open to restore it.
    #[test]
    fn a_shipped_file_restores_after_a_release() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let key = replicas.fleet.key_for("acme").unwrap();
        let path = dir.path().join("acme.harvest.db");
        let runtime = autumn_harvest_sqlite::SqliteRuntime::open_replicated(&path).unwrap();
        runtime.raise_fleet_epoch(7).unwrap();
        replicas.attach(&key, &path, None);
        replicas.tick_all();
        drop(runtime);
        replicas.close(&key);
        assert!(replicas.open_map().is_empty());
        assert!(replicas.parked_map().is_empty(), "the close caught up");

        std::fs::remove_file(&path).unwrap();
        replicas.release(&key);
        replicas.prepare(&key, &path, None).unwrap();
        assert_eq!(autumn_harvest_sqlite::fleet_epoch::read(&path).unwrap(), 7);
        assert!(replicas.pending_map().is_empty(), "the restore ran once");
    }

    /// A delete clears the restore that its release asked for.
    #[test]
    fn a_delete_forgets_the_pending_restore() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let key = replicas.fleet.key_for("acme").unwrap();
        replicas.release(&key);
        replicas.forget(&key);
        assert!(replicas.pending_map().is_empty());
    }

    /// An explicit point in time survives the release of its own restore.
    #[test]
    fn an_explicit_target_survives_the_release() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let key = replicas.fleet.key_for("acme").unwrap();
        let at = Utc::now();
        replicas.expect_restore(&key, Pending::At(at));
        replicas.release(&key);
        assert_eq!(replicas.pending_map().get(&key), Some(&Pending::At(at)));
        replicas.cancel_restore(&key);
        assert!(replicas.pending_map().is_empty());
    }

    /// Without a replica, a missing file stays missing.
    #[test]
    fn a_missing_file_with_no_replica_stays_missing() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let key = replicas.fleet.key_for("acme").unwrap();
        let path = dir.path().join("acme.harvest.db");
        replicas.prepare(&key, &path, Some(3)).unwrap();
        assert!(!path.exists());
    }

    /// Ship a Harvest file that opened under `epoch`.
    fn ship_at(replicas: &Replicas, key: &FleetDbKey, path: &Path, epoch: u64) {
        let runtime = autumn_harvest_sqlite::SqliteRuntime::open_replicated(path).unwrap();
        runtime.raise_fleet_epoch(epoch).unwrap();
        replicas.attach(key, path, Some(epoch));
        replicas.tick_all();
        drop(runtime);
        replicas.close(key);
    }

    /// A local file from epoch 1 never opens once another owner shipped
    /// epoch 2. The open under epoch 3 replaces it.
    #[test]
    fn a_file_older_than_the_replica_epoch_is_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let key = replicas.fleet.key_for("acme").unwrap();
        std::fs::create_dir_all(dir.path().join("b")).unwrap();
        ship_at(&replicas, &key, &dir.path().join("b/acme.harvest.db"), 2);

        let local = dir.path().join("acme.harvest.db");
        let runtime = autumn_harvest_sqlite::SqliteRuntime::open_replicated(&local).unwrap();
        runtime.raise_fleet_epoch(1).unwrap();
        drop(runtime);
        replicas.prepare(&key, &local, Some(3)).unwrap();
        assert_eq!(autumn_harvest_sqlite::fleet_epoch::read(&local).unwrap(), 2);
    }

    /// A replica at the epoch of this host is its own. The file stays.
    #[test]
    fn a_replica_of_the_own_epoch_does_not_replace_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let key = replicas.fleet.key_for("acme").unwrap();
        std::fs::create_dir_all(dir.path().join("b")).unwrap();
        ship_at(&replicas, &key, &dir.path().join("b/acme.harvest.db"), 3);

        let local = dir.path().join("acme.harvest.db");
        let runtime = autumn_harvest_sqlite::SqliteRuntime::open_replicated(&local).unwrap();
        runtime.raise_fleet_epoch(1).unwrap();
        drop(runtime);
        replicas.prepare(&key, &local, Some(3)).unwrap();
        assert_eq!(autumn_harvest_sqlite::fleet_epoch::read(&local).unwrap(), 1);
    }

    /// A runtime that holds the file blocks its replacement.
    #[test]
    fn a_live_runtime_blocks_the_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let key = replicas.fleet.key_for("acme").unwrap();
        std::fs::create_dir_all(dir.path().join("b")).unwrap();
        ship_at(&replicas, &key, &dir.path().join("b/acme.harvest.db"), 2);

        let local = dir.path().join("acme.harvest.db");
        let runtime = autumn_harvest_sqlite::SqliteRuntime::open_replicated(&local).unwrap();
        runtime.raise_fleet_epoch(1).unwrap();
        let refused = replicas.prepare(&key, &local, Some(3)).unwrap_err();
        assert!(refused.contains("cannot lock"), "{refused}");
        assert_eq!(autumn_harvest_sqlite::fleet_epoch::read(&local).unwrap(), 1);
        drop(runtime);
    }

    /// A stale file with no newer epoch at the replica stays.
    #[test]
    fn a_file_with_no_newer_epoch_stays() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let key = replicas.fleet.key_for("acme").unwrap();
        let path = dir.path().join("acme.harvest.db");
        let runtime = autumn_harvest_sqlite::SqliteRuntime::open_replicated(&path).unwrap();
        runtime.raise_fleet_epoch(1).unwrap();
        drop(runtime);
        replicas.prepare(&key, &path, Some(2)).unwrap();
        assert_eq!(autumn_harvest_sqlite::fleet_epoch::read(&path).unwrap(), 1);
    }
}
