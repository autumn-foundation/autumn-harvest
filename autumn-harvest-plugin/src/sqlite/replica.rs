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
//! The Harvest file records the identity of its tenant database file. A
//! restore, a re-create or an import of the database gives a new file, and
//! so a new identity. The next open then makes the Harvest file follow it.
//!
//! ```text
//! on_open   ─► restore or replace the file ─► open it ─► raise its epoch
//!              ─► record the database identity ─► attach a shipper
//! loop      ─► each tick: check the ownership epoch, then ship
//! on_close  ─► ship until caught up, or park the shipper and retry
//! release   ─► drop a parked shipper, keep the target of a restore
//! on_delete ─► forget the tenant
//! ```
//!
//! Autumn ADR 0040 plans a destination for a lifecycle hook, scoped to
//! `<key prefix>/<engine name>/`. This module uses that layout now, on the
//! public parts of autumn-web 0.10.

use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use autumn_web::db::fleet::DatabaseFleet;
use autumn_web::db::fleet_replication::replica_root;
use autumn_web::db::fleet_tombstones::restore_allowed;
use autumn_web::fleet_layout::FleetDbKey;
use autumn_web::replication::restore::{self, RestoreError, RestorePlan};
use autumn_web::replication::{
    ReplicaDestination, ReplicationSettings, ReplicationStatus, Replicator, segment,
};
use autumn_web::time::ClockSource;
use chrono::{DateTime, Utc};

/// The engine name of the Harvest file under the prefix of its tenant.
pub const REPLICA_ENGINE: &str = "harvest";

/// Most ticks that a close-time ship takes. One tick ships one segment.
const FINAL_SHIP_MAX_TICKS: usize = 64;

/// The longest that a close waits for another process to free the lock.
const CLAIM_WAIT: Duration = Duration::from_secs(2);

/// The pause between two claims at a close.
const CLAIM_POLL: Duration = Duration::from_millis(25);

/// The pause of the loop between two checks of its stop flag.
const STOP_POLL: Duration = Duration::from_millis(100);

/// The pause between two flush passes at shutdown.
const FLUSH_RETRY: Duration = Duration::from_millis(100);

/// The SQLite sidecars of a database file.
const SIDECARS: [&str; 3] = ["-wal", "-shm", "-journal"];

/// The suffix of the file that holds the tenant database identity.
pub const IDENTITY_SUFFIX: &str = ".tenant-db";

/// The suffix of the file that holds the target of a tenant restore.
pub const TARGET_SUFFIX: &str = ".restore-target";

/// The text of a restore target that asks for the latest state.
const LATEST: &str = "latest";

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
/// uses. `settings.root` is the replication root. Each tenant file ships
/// under [`tenant_harvest_replica_root`]. The plugin ignores
/// `database_path` and `verify_interval`.
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

/// What this host knows about its range of a tenant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Hold {
    /// This host holds the range at the epoch of the file.
    Held,
    /// This host holds the range at another epoch, so another owner came.
    Lost,
    /// The local lease ran out. A renewal can bring it back.
    Unknown,
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
    /// Set when the tenant closes. A tick that took the shipper before the
    /// close then ships nothing.
    retired: AtomicBool,
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
        let opened = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(suffixed(&self.path, ".replicator.lock"));
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

/// What the replica holds for a restore request.
enum Planned {
    /// A generation to restore.
    Plan(Box<RestorePlan>),
    /// The replica has no file.
    NoReplica,
    /// The replica no longer reaches the requested time.
    TooOld(String),
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
    /// The targets of `HarvestFleet::restore` calls in flight. The `release`
    /// hook writes each one to disk, after the fleet drained the tenant.
    requested: Mutex<HashMap<FleetDbKey, Option<DateTime<Utc>>>>,
    stopped: AtomicBool,
}

impl Replicas {
    /// The shippers of the Harvest files of `fleet`, to `replica`.
    pub fn new(replica: TenantReplica, clock: Arc<dyn ClockSource>, fleet: DatabaseFleet) -> Self {
        Self {
            replica,
            clock,
            fleet,
            open: Mutex::new(HashMap::new()),
            parked: Mutex::new(HashMap::new()),
            requested: Mutex::new(HashMap::new()),
            stopped: AtomicBool::new(false),
        }
    }

    fn open_map(&self) -> MutexGuard<'_, HashMap<FleetDbKey, Arc<Shipper>>> {
        self.open.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn parked_map(&self) -> MutexGuard<'_, HashMap<FleetDbKey, Arc<Shipper>>> {
        self.parked.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn requested_map(&self) -> MutexGuard<'_, HashMap<FleetDbKey, Option<DateTime<Utc>>>> {
        self.requested
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn destination(&self) -> &dyn ReplicaDestination {
        self.replica.destination.as_ref()
    }

    /// What this host knows about its range of `key` at `epoch`.
    fn hold(&self, key: &FleetDbKey, epoch: Option<u64>) -> Hold {
        let Some(ownership) = self.fleet.ownership() else {
            return Hold::Held;
        };
        match ownership.epoch_for(key.routing_slot()) {
            Some(held) if Some(held.get()) == epoch => Hold::Held,
            Some(_) => Hold::Lost,
            None => Hold::Unknown,
        }
    }

    /// Whether a close or a parked shipper may ship.
    ///
    /// A local lease that ran out is not a lost range. The destination then
    /// decides: a newer epoch there fences the shipper.
    fn may_ship_closed(&self, key: &FleetDbKey, shipper: &Shipper) -> bool {
        if shipper.fenced() {
            return false;
        }
        match self.hold(key, shipper.epoch) {
            Hold::Held => true,
            Hold::Lost => {
                fence(key, shipper, "another owner holds the range");
                false
            }
            Hold::Unknown => match (self.newest_epoch(key), shipper.epoch) {
                (Ok(Some(newest)), Some(epoch)) if newest > epoch => {
                    fence(key, shipper, &format!("epoch {newest} took over"));
                    false
                }
                _ => true,
            },
        }
    }

    /// Make the Harvest file of `key` current before its runtime opens it.
    ///
    /// - A missing file restores from the replica, as the fleet restores a
    ///   missing database.
    /// - When the tenant database is a new file, the Harvest file follows
    ///   it. `HarvestFleet::restore` sets the point in time.
    /// - A file from an older epoch of another owner is replaced.
    ///
    /// It blocks the thread.
    ///
    /// # Errors
    ///
    /// Returns a message when the replica cannot be read, or when a needed
    /// restore fails. The open is then refused, so no stale file serves.
    pub fn prepare(
        &self,
        key: &FleetDbKey,
        path: &Path,
        database: &Path,
        epoch: Option<u64>,
    ) -> Result<(), String> {
        let current = database_identity(database)?;
        let recorded = std::fs::read_to_string(suffixed(path, IDENTITY_SUFFIX)).ok();
        let target = read_target(path)?;
        // A file with no record predates the record. Only a target then
        // shows that the database changed.
        let replaced =
            recorded.map_or_else(|| target.is_some(), |recorded| recorded.trim() != current);
        if !path.exists() || replaced {
            self.drop_parked(key);
            match target {
                Some(target) => self.restore_explicit(key, path, target.time())?,
                None if replaced => self.follow_replaced(key, path)?,
                None => self.restore_missing(key, path)?,
            }
            // Before the target goes: an open that stops after this point
            // keeps the restored file.
            record_database(path, database)?;
        } else if let Some(epoch) = epoch {
            self.replace_if_stale(key, path, epoch)?;
        }
        // The open used the target, or a failed restore left it beside an
        // unchanged database. It goes in both cases.
        remove_quietly(&suffixed(path, TARGET_SUFFIX));
        Ok(())
    }

    /// What the replica of `key` holds at `target`, or at its latest state.
    fn plan(&self, key: &FleetDbKey, target: Option<DateTime<Utc>>) -> Result<Planned, String> {
        let root = self.replica.root_for(key);
        match restore::plan(self.destination(), &root, target) {
            Ok(plan) => Ok(Planned::Plan(Box::new(plan))),
            Err(RestoreError::NoReplica { .. }) => Ok(Planned::NoReplica),
            Err(error @ RestoreError::TargetBeforeRetention { .. }) => {
                Ok(Planned::TooOld(error.to_string()))
            }
            Err(error) => Err(format!("cannot plan the Harvest restore of {key}: {error}")),
        }
    }

    /// Restore the file to `target`, as `HarvestFleet::restore` asked. The
    /// fleet already restored the tenant database.
    fn restore_explicit(
        &self,
        key: &FleetDbKey,
        path: &Path,
        target: Option<DateTime<Utc>>,
    ) -> Result<(), String> {
        match self.plan(key, target)? {
            Planned::Plan(plan) => self.publish_plan(key, path, &plan, true),
            Planned::NoReplica => {
                tracing::warn!(tenant = %key, "the tenant database was restored, but its Harvest file has no replica: the Harvest file starts empty");
                set_aside(key, path)
            }
            // The target file stays, so each open refuses until an operator
            // restores the tenant again.
            Planned::TooOld(why) => Err(format!(
                "the Harvest replica of {key} cannot reach the restore target: {why}. \
                 Restore the tenant again with a target that both replicas hold"
            )),
        }
    }

    /// The tenant database is a new file, with no requested target. A
    /// restore or a stale-epoch replacement keeps its replica, so the
    /// Harvest file takes the latest one. A re-created or a deleted tenant
    /// has a tombstone, so its old Harvest file goes aside.
    fn follow_replaced(&self, key: &FleetDbKey, path: &Path) -> Result<(), String> {
        let plan = match self.plan(key, None)? {
            Planned::Plan(plan) => plan,
            Planned::NoReplica | Planned::TooOld(_) => return set_aside(key, path),
        };
        if !self.restore_allowed_for(key, &plan)? {
            return set_aside(key, path);
        }
        tracing::warn!(tenant = %key, "the tenant database is a new file: its Harvest file is restored from the replica");
        self.publish_plan(key, path, &plan, true)
    }

    /// Restore a missing file from its replica, under the fleet's rules.
    fn restore_missing(&self, key: &FleetDbKey, path: &Path) -> Result<(), String> {
        if !self
            .fleet
            .replication()
            .is_some_and(|replication| replication.restores_missing())
        {
            return Ok(());
        }
        let Planned::Plan(plan) = self.plan(key, None)? else {
            return Ok(());
        };
        if !self.restore_allowed_for(key, &plan)? {
            tracing::info!(tenant = %key, "the tenant was deleted: its Harvest file is not restored");
            return Ok(());
        }
        self.publish_plan(key, path, &plan, false)?;
        tracing::info!(tenant = %key, "tenant Harvest file restored from its replica");
        Ok(())
    }

    /// Whether the tombstone row and the delete marker allow `plan`.
    fn restore_allowed_for(&self, key: &FleetDbKey, plan: &RestorePlan) -> Result<bool, String> {
        let Some(replication) = self.fleet.replication() else {
            return Ok(true);
        };
        let row = tombstone_row(replication, key)?;
        let marker = replication.marker(key)?.map(|marker| marker.deleted_at());
        Ok(restore_allowed(row, marker, plan.generation_started_at))
    }

    /// Replace the file when another owner shipped a newer epoch of it.
    fn replace_if_stale(&self, key: &FleetDbKey, path: &Path, epoch: u64) -> Result<(), String> {
        let stamp = autumn_harvest_sqlite::fleet_epoch::read(path)
            .map_err(|error| format!("cannot read the epoch of {}: {error}", path.display()))?;
        if stamp >= epoch {
            return Ok(());
        }
        let newest = self.newest_epoch(key)?;
        // An epoch belongs to one owner, so a replica at `epoch` comes from
        // this host.
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
        match self.plan(key, None)? {
            Planned::Plan(plan) => self.publish_plan(key, path, &plan, true),
            Planned::NoReplica | Planned::TooOld(_) => Err(format!(
                "the Harvest file of {key} is stale, and its replica has no file to replace it"
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

    /// Build `plan` in a private file, then publish it at `path`.
    ///
    /// Each publisher holds `<file>.publish.lock`. With `replace`, it also
    /// holds the Harvest writer lock, so it never replaces a file that a
    /// runtime has open. Without `replace`, a file that another opener
    /// published stays.
    fn publish_plan(
        &self,
        key: &FleetDbKey,
        path: &Path,
        plan: &RestorePlan,
        replace: bool,
    ) -> Result<(), String> {
        let staging = staging_of(path);
        if let Err(error) = restore::apply(self.destination(), plan, &staging) {
            discard(&staging);
            return Err(format!("cannot restore the Harvest file of {key}: {error}"));
        }
        let published = publish(&staging, path, replace);
        discard(&staging);
        published
            .map_err(|error| format!("cannot publish the restored Harvest file of {key}: {error}"))
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
            parked.retired.store(false, Ordering::Release);
            self.open_map().insert(key.clone(), parked);
            return;
        }
        let settings = ReplicationSettings {
            database_path: path.to_path_buf(),
            root: self.replica.root_for(key),
            // A verification restores the whole file. An operator starts
            // it. No timer starts it.
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
            retired: AtomicBool::new(false),
        });
        shipper.claim();
        self.open_map().insert(key.clone(), shipper);
    }

    /// Ship each open file once, and retry each parked one. It blocks the
    /// thread.
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
            self.tick_parked(&key, &shipper, None);
        }
    }

    /// One tick of an open file. A host that lost the range ships nothing.
    fn tick(&self, key: &FleetDbKey, shipper: &Arc<Shipper>) {
        if shipper.fenced() || shipper.retired.load(Ordering::Acquire) {
            return;
        }
        match self.hold(key, shipper.epoch) {
            Hold::Held => {}
            Hold::Lost => {
                fence(key, shipper, "another owner holds the range");
                return;
            }
            // A renewal can bring the lease back. Ship nothing until then.
            Hold::Unknown => return,
        }
        if !shipper.claim() {
            return;
        }
        // A close or another tick has it. The next tick ships.
        let Ok(mut replicator) = shipper.replicator.try_lock() else {
            return;
        };
        // The tenant closed since the snapshot of the map.
        let current = self
            .open_map()
            .get(key)
            .is_some_and(|open| Arc::ptr_eq(open, shipper));
        if !current || shipper.retired.load(Ordering::Acquire) {
            return;
        }
        if let Err(error) = replicator.tick() {
            tracing::debug!(tenant = %key, %error, "tenant Harvest replication tick failed");
        }
        if let Some(newest) = replicator.fenced_by() {
            fence(key, shipper, &format!("epoch {newest} took over"));
        }
    }

    /// Ship the backlog of a closed file. Drop its shipper once it caught
    /// up or is fenced. `deadline` bounds a shutdown flush.
    fn tick_parked(&self, key: &FleetDbKey, shipper: &Arc<Shipper>, deadline: Option<Instant>) {
        let caught_up = self.may_ship_closed(key, shipper) && {
            let mut replicator = shipper.replicator();
            let stop = || shipper.fenced() || deadline.is_some_and(|at| Instant::now() >= at);
            let caught_up = ship_until_caught_up(&mut replicator, &stop).is_ok();
            if let Some(newest) = replicator.fenced_by() {
                fence(key, shipper, &format!("epoch {newest} took over"));
            }
            drop(replicator);
            caught_up
        };
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

    /// The tenant closed. Ship what its runs committed. It blocks the
    /// thread.
    ///
    /// The fleet stopped the driver first, so no new frame comes.
    pub fn close(&self, key: &FleetDbKey) {
        let Some(shipper) = self.open_map().remove(key) else {
            return;
        };
        shipper.retired.store(true, Ordering::Release);
        if !self.may_ship_closed(key, &shipper) {
            return;
        }
        if !claim_for_close(&shipper) {
            tracing::warn!(tenant = %key, "another process holds the Harvest replicator lock: it ships the last frames");
            return;
        }
        let mut replicator = shipper.replicator();
        let outcome = ship_until_caught_up(&mut replicator, &|| shipper.fenced());
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
    /// file. For a `HarvestFleet::restore`, write its target beside the
    /// Harvest file. The next open reads it once the database is a new file.
    pub fn release(&self, key: &FleetDbKey) {
        self.drop_parked(key);
        let requested = self.requested_map().get(key).copied();
        if let Some(target) = requested {
            let path = super::fleet::tenant_harvest_file(&self.fleet.path_of(key));
            if let Err(error) = write_target(&path, target) {
                tracing::error!(tenant = %key, %error, "cannot keep the restore target of the tenant Harvest file");
            }
        }
    }

    /// The tenant is deleted.
    pub fn forget(&self, key: &FleetDbKey) {
        self.drop_parked(key);
    }

    /// `HarvestFleet::restore` starts a restore of `key` to `target`.
    pub fn request_restore(&self, key: &FleetDbKey, target: Option<DateTime<Utc>>) {
        self.requested_map().insert(key.clone(), target);
    }

    /// The restore that [`request_restore`](Self::request_restore) started
    /// ended. A failed one leaves the database as it was, so its target
    /// goes.
    pub fn end_restore(&self, key: &FleetDbKey, restored: bool) {
        self.requested_map().remove(key);
        if !restored {
            let path = super::fleet::tenant_harvest_file(&self.fleet.path_of(key));
            remove_quietly(&suffixed(&path, TARGET_SUFFIX));
        }
    }

    /// Stop the parked shipper of `key`. A tick in flight stops after its
    /// current step.
    fn drop_parked(&self, key: &FleetDbKey) {
        let parked = self.parked_map().remove(key);
        if let Some(parked) = parked {
            parked.fenced.store(true, Ordering::Release);
            drop(parked.replicator());
        }
    }

    /// Ship the parked backlog until it caught up or `within` passed. It
    /// blocks the thread. Each tenant still behind is logged.
    pub fn flush_parked(&self, within: Duration) {
        let deadline = Instant::now() + within;
        loop {
            let parked: Vec<(FleetDbKey, Arc<Shipper>)> = self
                .parked_map()
                .iter()
                .map(|(key, shipper)| (key.clone(), Arc::clone(shipper)))
                .collect();
            if parked.is_empty() || Instant::now() >= deadline {
                break;
            }
            for (key, shipper) in parked {
                self.tick_parked(&key, &shipper, Some(deadline));
            }
            std::thread::sleep(FLUSH_RETRY.min(deadline.saturating_duration_since(Instant::now())));
        }
        for key in self.parked_map().keys() {
            tracing::error!(tenant = %key, "shutdown: frames of the tenant Harvest file are not shipped and stay on this volume");
        }
    }

    /// Tick every sync interval on a thread, until [`stop`](Self::stop).
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
                    let started = Instant::now();
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

/// Record that the Harvest file at `path` belongs to `database`. Call it
/// once the runtime opened the file.
///
/// # Errors
///
/// Returns a message when the record cannot be written.
pub fn record_database(path: &Path, database: &Path) -> Result<(), String> {
    write_atomic(
        &suffixed(path, IDENTITY_SUFFIX),
        &database_identity(database)?,
    )
}

fn fence(key: &FleetDbKey, shipper: &Shipper, why: &str) {
    if !shipper.fenced.swap(true, Ordering::AcqRel) {
        tracing::warn!(tenant = %key, why, "the tenant Harvest file ships nothing more");
    }
}

/// Claim at a close. Another process can hold the lock while it ships.
fn claim_for_close(shipper: &Shipper) -> bool {
    let limit = Instant::now() + CLAIM_WAIT;
    loop {
        if shipper.claim() {
            return true;
        }
        if Instant::now() >= limit {
            return false;
        }
        std::thread::sleep(CLAIM_POLL);
    }
}

/// Tick until nothing committed is left to ship, or until `stop` is true.
fn ship_until_caught_up(
    replicator: &mut Replicator,
    stop: &dyn Fn() -> bool,
) -> Result<(), String> {
    for _ in 0..FINAL_SHIP_MAX_TICKS {
        if stop() {
            return Err("the ship stopped".to_owned());
        }
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

/// The identity of the tenant database file. A restore, a re-create or an
/// import publishes a new file, so the identity changes.
fn database_identity(database: &Path) -> Result<String, String> {
    let meta = std::fs::metadata(database)
        .map_err(|error| format!("cannot read {}: {error}", database.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        Ok(format!("{}:{}", meta.dev(), meta.ino()))
    }
    #[cfg(not(unix))]
    {
        let created = meta
            .created()
            .map_err(|error| format!("cannot read {}: {error}", database.display()))?;
        let nanos = created
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos());
        Ok(format!("{nanos}:{}", meta.len()))
    }
}

/// The point in time of a requested restore.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Target {
    /// The latest state of the replica.
    Latest,
    /// The state at this time.
    At(DateTime<Utc>),
}

impl Target {
    const fn time(self) -> Option<DateTime<Utc>> {
        match self {
            Self::Latest => None,
            Self::At(at) => Some(at),
        }
    }
}

/// The restore target beside the Harvest file at `path`, if any.
fn read_target(path: &Path) -> Result<Option<Target>, String> {
    let file = suffixed(path, TARGET_SUFFIX);
    let text = match std::fs::read_to_string(&file) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("cannot read {}: {error}", file.display())),
    };
    let text = text.trim();
    if text == LATEST {
        return Ok(Some(Target::Latest));
    }
    DateTime::parse_from_rfc3339(text)
        .map(|at| Some(Target::At(at.with_timezone(&Utc))))
        .map_err(|error| format!("unreadable restore target in {}: {error}", file.display()))
}

/// Keep the restore target beside the Harvest file at `path`.
fn write_target(path: &Path, target: Option<DateTime<Utc>>) -> Result<(), String> {
    let text = target.map_or_else(|| LATEST.to_owned(), |at| at.to_rfc3339());
    write_atomic(&suffixed(path, TARGET_SUFFIX), &text)
}

/// Write `text` to `file` through a private file and a rename.
fn write_atomic(file: &Path, text: &str) -> Result<(), String> {
    let staging = suffixed(file, &format!(".{}", unique()));
    std::fs::write(&staging, text)
        .and_then(|()| std::fs::rename(&staging, file))
        .map_err(|error| {
            remove_quietly(&staging);
            format!("cannot write {}: {error}", file.display())
        })
}

/// Move the Harvest file at `path` and its sidecars aside. The next open
/// then starts an empty file. An operator can still read the old one.
fn set_aside(key: &FleetDbKey, path: &Path) -> Result<(), String> {
    if !path.exists() {
        return Ok(());
    }
    let _publish = PublishLock::acquire(path)?;
    let _writer = autumn_harvest_sqlite::FileWriterLock::acquire(path)
        .map_err(|error| format!("cannot lock {} to set it aside: {error}", path.display()))?;
    let aside = suffixed(path, &format!(".aside-{}", unique()));
    for suffix in SIDECARS {
        let sidecar = suffixed(path, suffix);
        if sidecar.exists() {
            let _ = std::fs::rename(&sidecar, suffixed(&aside, suffix));
        }
    }
    std::fs::rename(path, &aside)
        .map_err(|error| format!("cannot set {} aside: {error}", path.display()))?;
    tracing::warn!(tenant = %key, aside = %aside.display(), "the Harvest file does not belong to the tenant database: it is set aside");
    Ok(())
}

/// Publish the restored file `staging` at `path`. See
/// [`Replicas::publish_plan`].
fn publish(staging: &Path, path: &Path, replace: bool) -> Result<(), String> {
    let _publish = PublishLock::acquire(path)?;
    let _writer = if path.exists() {
        if !replace {
            // Another opener published the file first. Open that file.
            return Ok(());
        }
        Some(
            autumn_harvest_sqlite::FileWriterLock::acquire(path).map_err(|error| {
                format!("cannot lock {} to replace it: {error}", path.display())
            })?,
        )
    } else {
        None
    };
    // The frames of the old file must not replay onto the restored one.
    for sidecar in sidecars(path) {
        remove_quietly(&sidecar);
    }
    std::fs::rename(staging, path).map_err(|error| error.to_string())
}

/// The OS lock `<file>.publish.lock`, held while a file is published.
struct PublishLock {
    _file: File,
}

impl PublishLock {
    fn acquire(path: &Path) -> Result<Self, String> {
        let name = suffixed(path, ".publish.lock");
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&name)
            .map_err(|error| format!("cannot open {}: {error}", name.display()))?;
        file.lock()
            .map_err(|error| format!("cannot lock {}: {error}", name.display()))?;
        Ok(Self { _file: file })
    }
}

/// A name part that no other staging file of this host has.
fn unique() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

/// `path` with `suffix` appended to its file name.
fn suffixed(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}

/// The private path that a restore builds before it publishes.
fn staging_of(path: &Path) -> PathBuf {
    suffixed(path, &format!(".restoring-{}", unique()))
}

/// The SQLite sidecars of `path`.
fn sidecars(path: &Path) -> impl Iterator<Item = PathBuf> + '_ {
    SIDECARS.iter().map(move |suffix| suffixed(path, suffix))
}

/// Remove a private file and its sidecars. A missing file is not an error.
fn discard(path: &Path) {
    remove_quietly(path);
    for sidecar in sidecars(path) {
        remove_quietly(&sidecar);
    }
}

/// Remove `file`. A missing file is not an error.
fn remove_quietly(file: &Path) {
    let _ = std::fs::remove_file(file);
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

    /// A tenant database file that `prepare` can read.
    fn database(dir: &Path, name: &str) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, b"tenant").unwrap();
        path
    }

    /// Replace the database file with a new one, as a restore does.
    fn replace_database(path: &Path) {
        let fresh = suffixed(path, ".fresh");
        std::fs::write(&fresh, b"restored").unwrap();
        std::fs::rename(&fresh, path).unwrap();
    }

    /// A restore through `HarvestFleet` keeps its target on disk at the
    /// release. The next open restores the shipped file to it.
    #[test]
    fn a_requested_restore_brings_the_shipped_file_back() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let key = replicas.fleet.key_for("acme").unwrap();
        let path = super::super::fleet::tenant_harvest_file(&replicas.fleet.path_of(&key));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let db = database(path.parent().unwrap(), "acme.db");
        ship_at(&replicas, &key, &path, 7);
        record_database(&path, &db).unwrap();
        assert!(replicas.open_map().is_empty());
        assert!(replicas.parked_map().is_empty(), "the close caught up");

        replicas.request_restore(&key, None);
        replicas.release(&key);
        replace_database(&db);
        replicas.end_restore(&key, true);
        std::fs::remove_file(&path).unwrap();
        replicas.prepare(&key, &path, &db, None).unwrap();
        assert_eq!(autumn_harvest_sqlite::fleet_epoch::read(&path).unwrap(), 7);
        assert!(
            !suffixed(&path, TARGET_SUFFIX).exists(),
            "the restore ran once"
        );
    }

    /// A release with no `HarvestFleet::restore`, such as a delete, keeps no
    /// target. A failed restore drops its target.
    #[test]
    fn only_a_requested_restore_keeps_a_target() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let key = replicas.fleet.key_for("acme").unwrap();
        let path = super::super::fleet::tenant_harvest_file(&replicas.fleet.path_of(&key));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let target = suffixed(&path, TARGET_SUFFIX);
        replicas.release(&key);
        assert!(!target.exists(), "a plain release");

        let at = Utc::now();
        replicas.request_restore(&key, Some(at));
        replicas.release(&key);
        assert_eq!(read_target(&path).unwrap(), Some(Target::At(at)));
        replicas.end_restore(&key, false);
        assert!(!target.exists(), "a failed restore");
    }

    /// A target beside an unchanged database is stale. The open drops it
    /// and keeps the file.
    #[test]
    fn a_target_beside_an_unchanged_database_is_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let key = replicas.fleet.key_for("acme").unwrap();
        let path = dir.path().join("acme.harvest.db");
        let db = database(dir.path(), "acme.db");
        let runtime = autumn_harvest_sqlite::SqliteRuntime::open_replicated(&path).unwrap();
        runtime.raise_fleet_epoch(4).unwrap();
        drop(runtime);
        record_database(&path, &db).unwrap();
        write_target(&path, None).unwrap();
        replicas.prepare(&key, &path, &db, None).unwrap();
        assert_eq!(autumn_harvest_sqlite::fleet_epoch::read(&path).unwrap(), 4);
        assert!(!suffixed(&path, TARGET_SUFFIX).exists());
    }

    /// A file from before the database record still follows a requested
    /// restore.
    #[test]
    fn a_target_applies_to_a_file_with_no_record() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let key = replicas.fleet.key_for("acme").unwrap();
        std::fs::create_dir_all(dir.path().join("b")).unwrap();
        ship_at(&replicas, &key, &dir.path().join("b/acme.harvest.db"), 6);

        let path = dir.path().join("acme.harvest.db");
        let db = database(dir.path(), "acme.db");
        let runtime = autumn_harvest_sqlite::SqliteRuntime::open_replicated(&path).unwrap();
        runtime.raise_fleet_epoch(1).unwrap();
        drop(runtime);
        write_target(&path, None).unwrap();
        replicas.prepare(&key, &path, &db, None).unwrap();
        assert_eq!(autumn_harvest_sqlite::fleet_epoch::read(&path).unwrap(), 6);
    }

    /// A restore records the database before its target goes. An open that
    /// stops after `prepare` then keeps the restored file.
    #[test]
    fn a_restore_records_the_database_before_the_target_goes() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let key = replicas.fleet.key_for("acme").unwrap();
        std::fs::create_dir_all(dir.path().join("b")).unwrap();
        ship_at(&replicas, &key, &dir.path().join("b/acme.harvest.db"), 6);

        let path = dir.path().join("acme.harvest.db");
        let db = database(dir.path(), "acme.db");
        drop(autumn_harvest_sqlite::SqliteRuntime::open_replicated(&path).unwrap());
        record_database(&path, &db).unwrap();
        replace_database(&db);
        write_target(&path, None).unwrap();
        replicas.prepare(&key, &path, &db, None).unwrap();
        assert_eq!(
            std::fs::read_to_string(suffixed(&path, IDENTITY_SUFFIX)).unwrap(),
            database_identity(&db).unwrap()
        );
        assert!(!suffixed(&path, TARGET_SUFFIX).exists());
    }

    /// A new tenant database with no Harvest replica sets the old Harvest
    /// file aside. A re-created tenant never reads the runs of the old one.
    #[test]
    fn a_new_database_with_no_replica_sets_the_old_file_aside() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let key = replicas.fleet.key_for("acme").unwrap();
        let path = dir.path().join("acme.harvest.db");
        let db = database(dir.path(), "acme.db");
        drop(autumn_harvest_sqlite::SqliteRuntime::open_replicated(&path).unwrap());
        record_database(&path, &db).unwrap();
        replace_database(&db);
        replicas.prepare(&key, &path, &db, None).unwrap();
        assert!(!path.exists(), "the open starts an empty file");
        let aside = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .any(|entry| entry.file_name().to_string_lossy().contains(".aside-"));
        assert!(aside, "an operator can still read the old file");
    }

    /// A new tenant database with a Harvest replica takes its latest file.
    #[test]
    fn a_new_database_takes_the_latest_harvest_replica() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let key = replicas.fleet.key_for("acme").unwrap();
        std::fs::create_dir_all(dir.path().join("b")).unwrap();
        ship_at(&replicas, &key, &dir.path().join("b/acme.harvest.db"), 5);

        let path = dir.path().join("acme.harvest.db");
        let db = database(dir.path(), "acme.db");
        let runtime = autumn_harvest_sqlite::SqliteRuntime::open_replicated(&path).unwrap();
        runtime.raise_fleet_epoch(2).unwrap();
        drop(runtime);
        record_database(&path, &db).unwrap();
        replace_database(&db);
        replicas.prepare(&key, &path, &db, None).unwrap();
        assert_eq!(autumn_harvest_sqlite::fleet_epoch::read(&path).unwrap(), 5);
    }

    /// A publish without replace keeps a file that another opener
    /// published. It never deletes a live file.
    #[test]
    fn a_publish_keeps_a_file_that_another_opener_published() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("acme.harvest.db");
        let theirs = autumn_harvest_sqlite::SqliteRuntime::open_replicated(&path).unwrap();
        theirs.raise_fleet_epoch(9).unwrap();
        let staging = dir.path().join("staging.db");
        drop(autumn_harvest_sqlite::SqliteRuntime::open_replicated(&staging).unwrap());
        publish(&staging, &path, false).unwrap();
        assert_eq!(autumn_harvest_sqlite::fleet_epoch::read(&path).unwrap(), 9);
        assert!(suffixed(&path, "-wal").exists(), "their WAL stays");
        drop(theirs);
    }

    /// Without a replica, a missing file stays missing.
    #[test]
    fn a_missing_file_with_no_replica_stays_missing() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let key = replicas.fleet.key_for("acme").unwrap();
        let path = dir.path().join("acme.harvest.db");
        let db = database(dir.path(), "acme.db");
        replicas.prepare(&key, &path, &db, Some(3)).unwrap();
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
        replicas
            .prepare(&key, &local, &database(dir.path(), "acme.db"), Some(3))
            .unwrap();
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
        replicas
            .prepare(&key, &local, &database(dir.path(), "acme.db"), Some(3))
            .unwrap();
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
        let refused = replicas
            .prepare(&key, &local, &database(dir.path(), "acme.db"), Some(3))
            .unwrap_err();
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
        replicas
            .prepare(&key, &path, &database(dir.path(), "acme.db"), Some(2))
            .unwrap();
        assert_eq!(autumn_harvest_sqlite::fleet_epoch::read(&path).unwrap(), 1);
    }
}
