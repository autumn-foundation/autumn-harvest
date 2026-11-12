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
//! A restore target also names the database file that it belongs to. It
//! applies only while that file is current, so a later restore of the
//! database supersedes it. `resolve_target` holds the rule.
//!
//! ```text
//! on_open   ─► restore or replace the file ─► open it ─► raise its epoch
//!              ─► record the database identity ─► attach a shipper
//! loop      ─► each tick: check the ownership epoch, then ship
//! on_close  ─► ship until caught up, or park the shipper and retry
//! release   ─► drop a parked shipper, write or bind the restore target
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

/// The suffix of the lock around each change of the restore target.
const TARGET_LOCK_SUFFIX: &str = ".restore-target.lock";

/// The suffix of the lock held while a file is published.
const PUBLISH_LOCK_SUFFIX: &str = ".publish.lock";

/// The text of a restore target that asks for the latest state.
const LATEST: &str = "latest";

/// The text of a missing database file, or of no previous target.
const NONE: &str = "-";

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

/// A `HarvestFleet::restore` call in flight.
struct Requested {
    target: Target,
    /// The pending entry that the last release of this restore wrote.
    written: Option<Entry>,
    /// Set once a release ran for this restore, even when its write failed.
    released: bool,
    /// Set once the fleet restore returned. A later release is not ours.
    returned: bool,
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
    requested: Mutex<HashMap<FleetDbKey, Requested>>,
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

    fn requested_map(&self) -> MutexGuard<'_, HashMap<FleetDbKey, Requested>> {
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
    ///   it. `HarvestFleet::restore` sets the point in time, until a later
    ///   restore of the database supersedes it.
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
        // The open holds this lock until it ends. So a late `end_restore`
        // never writes back a target that this open used.
        let _target_lock = SideLock::acquire(path, TARGET_LOCK_SUFFIX)?;
        let current = database_identity(database)?;
        let recorded = std::fs::read_to_string(suffixed(path, IDENTITY_SUFFIX)).ok();
        let entry = read_entry(path)?;
        // A restore of this process that did not return can still publish.
        // Its pending entry does not apply yet, so the open reads the entry
        // from before the request, and keeps the pending entry.
        let unreturned = entry
            .as_ref()
            .filter(|entry| self.unreturned(key).as_ref() == Some(*entry))
            .cloned();
        let seen = unreturned.as_ref().map_or(entry, before_request);
        let resolution = resolve_target(held(seen, &current));
        let superseded = matches!(resolution, Resolution::Superseded);
        let target = match resolution {
            Resolution::Apply(target) => Some(target),
            Resolution::Nothing | Resolution::Superseded => None,
        };
        // A file with no record predates the record. A superseded target
        // then shows that the database is a new file.
        let replaced =
            target.is_some() || recorded.map_or(superseded, |recorded| recorded.trim() != current);
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
        match unreturned {
            // The open used the earlier target, so the pending entry drops it.
            Some(Entry::Pending { target, from, .. }) => {
                let kept = Entry::Pending {
                    target,
                    from,
                    previous: None,
                };
                write_entry(path, Some(&kept))?;
                if let Some(requested) = self.requested_map().get_mut(key) {
                    requested.written = Some(kept);
                }
            }
            // The open used the target, or the target does not apply to
            // this database. It goes in both cases.
            _ => remove_quietly(&suffixed(path, TARGET_SUFFIX)),
        }
        Ok(())
    }

    /// The pending entry of a restore of `key` that did not return.
    fn unreturned(&self, key: &FleetDbKey) -> Option<Entry> {
        self.requested_map()
            .get(key)
            .filter(|requested| !requested.returned)
            .and_then(|requested| requested.written.clone())
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
    /// file, and bind the restore target to the database file.
    ///
    /// Each release before a `HarvestFleet::restore` returns writes its
    /// pending target. The last one comes from the drain of that restore, so
    /// it names the file that the restore replaces. Each other release binds
    /// the target that applies now to the current database file, or drops
    /// it. A later restore of the database then supersedes the target.
    pub fn release(&self, key: &FleetDbKey) {
        self.drop_parked(key);
        let database = self.fleet.path_of(key);
        let path = super::fleet::tenant_harvest_file(&database);
        // The fleet calls this hook from a task. The lock can wait for an
        // open that restores the Harvest file.
        if let Err(error) = blocking(|| self.rebind(key, &path, &database)) {
            tracing::error!(tenant = %key, %error, "cannot keep the restore target of the tenant Harvest file");
        }
    }

    /// Bind the target beside `path` to the current file at `database`,
    /// under the target lock. A restore of `key` that did not return writes
    /// its pending target.
    fn rebind(&self, key: &FleetDbKey, path: &Path, database: &Path) -> Result<(), String> {
        let asked = self.unreturned_target(key).is_some();
        if !asked && !suffixed(path, TARGET_SUFFIX).exists() {
            return Ok(());
        }
        if asked && let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| format!("cannot create {}: {error}", parent.display()))?;
        }
        let _lock = SideLock::acquire(path, TARGET_LOCK_SUFFIX)?;
        // Read the request under the lock, as an open can change it.
        let request = self
            .requested_map()
            .get_mut(key)
            .filter(|requested| !requested.returned)
            .map(|requested| {
                requested.released = true;
                (requested.target, requested.written.clone())
            });
        let current = identity_or_none(database)?;
        let (entry, unreadable) = match read_entry(path) {
            Ok(entry) => (entry, false),
            Err(error) => {
                tracing::warn!(tenant = %key, %error, "the release replaces an unreadable restore target");
                (None, true)
            }
        };
        let next = match &request {
            Some((target, written)) => {
                // An earlier release of this restore wrote the entry. Read
                // the entry from before the request.
                let before = match entry.clone() {
                    Some(entry) if written.as_ref() == Some(&entry) => before_request(&entry),
                    other => other,
                };
                Some(Entry::Pending {
                    target: *target,
                    from: current.clone(),
                    previous: settle(before, &current).as_ref().map(Entry::target),
                })
            }
            None => settle(entry.clone(), &current),
        };
        if unreadable || next != entry {
            write_entry(path, next.as_ref())?;
        }
        if request.is_some()
            && let Some(requested) = self.requested_map().get_mut(key)
            && !requested.returned
        {
            requested.written = next;
        }
        Ok(())
    }

    /// The target of a restore of `key` that did not return.
    fn unreturned_target(&self, key: &FleetDbKey) -> Option<Target> {
        self.requested_map()
            .get(key)
            .filter(|requested| !requested.returned)
            .map(|requested| requested.target)
    }

    /// The tenant is deleted.
    pub fn forget(&self, key: &FleetDbKey) {
        self.drop_parked(key);
    }

    /// `HarvestFleet::restore` starts a restore of `key` to `target`.
    pub fn request_restore(&self, key: &FleetDbKey, target: Option<DateTime<Utc>>) {
        self.requested_map().insert(
            key.clone(),
            Requested {
                target: Target::of(target),
                written: None,
                released: false,
                returned: false,
            },
        );
    }

    /// The fleet restore that [`request_restore`](Self::request_restore)
    /// started returned. Its drain ended, so a later release is not ours.
    pub fn restore_returned(&self, key: &FleetDbKey) {
        if let Some(requested) = self.requested_map().get_mut(key) {
            requested.returned = true;
        }
    }

    /// The restore that [`request_restore`](Self::request_restore) started
    /// ended. It blocks the thread.
    ///
    /// A finished restore binds its target to the new database file. A
    /// failed one leaves the database as it was, so the earlier target
    /// comes back. When an open or another release already took the
    /// pending target, nothing changes.
    pub fn end_restore(&self, key: &FleetDbKey, restored: bool) {
        let Some(requested) = self.requested_map().remove(key) else {
            return;
        };
        // The fleet refused the restore before its drain.
        if !requested.released {
            return;
        }
        let database = self.fleet.path_of(key);
        let path = super::fleet::tenant_harvest_file(&database);
        let ended = match &requested.written {
            Some(written) => finish(&path, &database, written, restored),
            // The release could not write the target. Keep it now.
            None if restored => commit_lost(&path, &database, requested.target),
            None => Ok(()),
        };
        if let Err(error) = ended {
            tracing::error!(tenant = %key, %error, "cannot commit the restore target of the tenant Harvest file");
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
    file_identity(database).map_err(|error| format!("cannot read {}: {error}", database.display()))
}

/// The identity of the tenant database file, or [`NONE`] when it is gone.
fn identity_or_none(database: &Path) -> Result<String, String> {
    match file_identity(database) {
        Ok(identity) => Ok(identity),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(NONE.to_owned()),
        Err(error) => Err(format!("cannot read {}: {error}", database.display())),
    }
}

fn file_identity(database: &Path) -> std::io::Result<String> {
    let meta = std::fs::metadata(database)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        Ok(format!("{}:{}", meta.dev(), meta.ino()))
    }
    #[cfg(not(unix))]
    {
        let nanos = meta
            .created()?
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos());
        // Not the length: an ordinary checkpoint changes it.
        Ok(nanos.to_string())
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
    const fn of(time: Option<DateTime<Utc>>) -> Self {
        match time {
            Some(at) => Self::At(at),
            None => Self::Latest,
        }
    }

    const fn time(self) -> Option<DateTime<Utc>> {
        match self {
            Self::Latest => None,
            Self::At(at) => Some(at),
        }
    }

    fn render(self) -> String {
        match self {
            Self::Latest => LATEST.to_owned(),
            Self::At(at) => at.to_rfc3339(),
        }
    }

    fn parse(text: &str) -> Option<Self> {
        if text == LATEST {
            return Some(Self::Latest);
        }
        DateTime::parse_from_rfc3339(text)
            .ok()
            .map(|at| Self::At(at.with_timezone(&Utc)))
    }
}

/// The entry of a restore target file. Each one names the database file
/// that it belongs to.
///
/// ```text
/// committed <of> <target>
/// pending <from> <target> <previous>
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
enum Entry {
    /// A finished restore that published the database file `of`.
    Committed { target: Target, of: String },
    /// A restore that started on the database file `from`. Its end did not
    /// run. `previous` is the target that applied to `from`.
    Pending {
        target: Target,
        from: String,
        previous: Option<Target>,
    },
}

impl Entry {
    const fn target(&self) -> Target {
        match self {
            Self::Committed { target, .. } | Self::Pending { target, .. } => *target,
        }
    }

    fn render(&self) -> String {
        match self {
            Self::Committed { target, of } => format!("committed {of} {}", target.render()),
            Self::Pending {
                target,
                from,
                previous,
            } => format!(
                "pending {from} {} {}",
                target.render(),
                previous.map_or_else(|| NONE.to_owned(), Target::render)
            ),
        }
    }

    fn parse(text: &str) -> Option<Self> {
        let words: Vec<&str> = text.split_whitespace().collect();
        match words.as_slice() {
            ["committed", of, target] => Some(Self::Committed {
                target: Target::parse(target)?,
                of: (*of).to_owned(),
            }),
            ["pending", from, target, previous] => Some(Self::Pending {
                target: Target::parse(target)?,
                from: (*from).to_owned(),
                previous: if *previous == NONE {
                    None
                } else {
                    Some(Target::parse(previous)?)
                },
            }),
            // An entry in the older format names no file. It applies at the
            // next open.
            [target] | ["committed", target] => Some(Self::Pending {
                target: Target::parse(target)?,
                from: NONE.to_owned(),
                previous: None,
            }),
            _ => None,
        }
    }
}

/// What a target file holds, seen from the current database file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Held<T> {
    /// No target.
    Nothing,
    /// A finished restore. `current` is true while its file is current.
    Committed { target: T, current: bool },
    /// A restore whose end did not run. `moved` is true when the database
    /// file changed since its release.
    Pending {
        target: T,
        moved: bool,
        previous: Option<T>,
    },
}

/// The target that applies to the current database file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Resolution<T> {
    /// No target applies.
    Nothing,
    /// The open restores the Harvest file to this target.
    Apply(T),
    /// A later restore replaced the database file of the target.
    Superseded,
}

/// Give the target that applies to the current database file.
///
/// A committed target applies only while its file is current. A pending
/// target applies once the file moved. Until then, the target that
/// applied before it still applies. `formal/verus/src/restore_target.rs`
/// proves the function for every input. It also proves that each open
/// applies the target of the last finished `HarvestFleet::restore`, and no
/// target after a successful direct restore.
fn resolve_target<T>(held: Held<T>) -> Resolution<T> {
    match held {
        Held::Nothing => Resolution::Nothing,
        Held::Committed { target, current } => {
            if current {
                Resolution::Apply(target)
            } else {
                Resolution::Superseded
            }
        }
        Held::Pending {
            target,
            moved,
            previous,
        } => match (moved, previous) {
            (true, _) => Resolution::Apply(target),
            (false, Some(previous)) => Resolution::Apply(previous),
            (false, None) => Resolution::Nothing,
        },
    }
}

/// `entry`, seen from the database file `current`.
fn held(entry: Option<Entry>, current: &str) -> Held<Target> {
    match entry {
        None => Held::Nothing,
        Some(Entry::Committed { target, of }) => Held::Committed {
            target,
            current: of == current,
        },
        Some(Entry::Pending {
            target,
            from,
            previous,
        }) => Held::Pending {
            target,
            moved: from != current,
            previous,
        },
    }
}

/// The target that applies to `current`, bound to `current`.
fn settle(entry: Option<Entry>, current: &str) -> Option<Entry> {
    match resolve_target(held(entry, current)) {
        Resolution::Apply(target) => Some(Entry::Committed {
            target,
            of: current.to_owned(),
        }),
        Resolution::Nothing | Resolution::Superseded => None,
    }
}

/// The restore target beside the Harvest file at `path`, if any.
fn read_entry(path: &Path) -> Result<Option<Entry>, String> {
    let file = suffixed(path, TARGET_SUFFIX);
    match std::fs::read_to_string(&file) {
        Ok(text) => Entry::parse(&text)
            .map(Some)
            .ok_or_else(|| format!("unreadable restore target in {}", file.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(format!("cannot read {}: {error}", file.display())),
    }
}

/// Keep `entry` beside the Harvest file at `path`, or remove the target.
fn write_entry(path: &Path, entry: Option<&Entry>) -> Result<(), String> {
    let file = suffixed(path, TARGET_SUFFIX);
    let Some(entry) = entry else {
        return match std::fs::remove_file(&file) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(format!("cannot remove {}: {error}", file.display())),
        };
    };
    write_atomic(&file, &entry.render())
}

/// The entry from before a request that wrote `entry`.
fn before_request(entry: &Entry) -> Option<Entry> {
    match entry {
        Entry::Pending { from, previous, .. } => previous.map(|previous| Entry::Committed {
            target: previous,
            of: from.clone(),
        }),
        Entry::Committed { .. } => Some(entry.clone()),
    }
}

/// Keep the target of a finished restore whose release wrote nothing.
fn commit_lost(path: &Path, database: &Path, target: Target) -> Result<(), String> {
    let _lock = SideLock::acquire(path, TARGET_LOCK_SUFFIX)?;
    if read_entry(path).ok().flatten().is_some() {
        return Ok(());
    }
    let committed = Entry::Committed {
        target,
        of: database_identity(database)?,
    };
    write_entry(path, Some(&committed))
}

/// Run `work`, which blocks, from any thread. On a worker thread of a
/// multi-thread runtime, the runtime moves its other tasks away first.
fn blocking<R>(work: impl FnOnce() -> R) -> R {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(work)
        }
        _ => work(),
    }
}

/// End the restore that wrote `written`, under the target lock. When an
/// open or another release took it, nothing changes.
fn finish(path: &Path, database: &Path, written: &Entry, restored: bool) -> Result<(), String> {
    let _lock = SideLock::acquire(path, TARGET_LOCK_SUFFIX)?;
    if read_entry(path)?.as_ref() != Some(written) {
        return Ok(());
    }
    let Entry::Pending {
        target,
        from,
        previous,
    } = written
    else {
        return Ok(());
    };
    let next = if restored {
        // An error leaves the pending entry. The open then compares `from`
        // with the database file.
        Some(Entry::Committed {
            target: *target,
            of: database_identity(database)?,
        })
    } else {
        previous.map(|previous| Entry::Committed {
            target: previous,
            of: from.clone(),
        })
    };
    write_entry(path, next.as_ref())
}

/// Write `text` to `file` through a private file and a rename. The data
/// reaches the disk before the rename.
fn write_atomic(file: &Path, text: &str) -> Result<(), String> {
    use std::io::Write as _;
    let staging = suffixed(file, &format!(".{}", unique()));
    File::create(&staging)
        .and_then(|mut created| {
            created.write_all(text.as_bytes())?;
            created.sync_all()
        })
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
    let _publish = SideLock::acquire(path, PUBLISH_LOCK_SUFFIX)?;
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
    let _publish = SideLock::acquire(path, PUBLISH_LOCK_SUFFIX)?;
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

/// An OS lock on `<file><suffix>`. It waits for another holder.
struct SideLock {
    _file: File,
}

impl SideLock {
    fn acquire(path: &Path, suffix: &str) -> Result<Self, String> {
        let name = suffixed(path, suffix);
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
        replicas.restore_returned(&key);
        replicas.end_restore(&key, true);
        std::fs::remove_file(&path).unwrap();
        replicas.prepare(&key, &path, &db, None).unwrap();
        assert_eq!(autumn_harvest_sqlite::fleet_epoch::read(&path).unwrap(), 7);
        assert!(
            !suffixed(&path, TARGET_SUFFIX).exists(),
            "the restore ran once"
        );
    }

    /// The target file beside the Harvest file at `path`.
    fn entry(path: &Path) -> Option<Entry> {
        read_entry(path).unwrap()
    }

    /// A target that no replica reaches. An open that applies it fails.
    fn unreachable() -> Option<DateTime<Utc>> {
        Some(DateTime::UNIX_EPOCH)
    }

    /// The key and the paths of the tenant `acme`. It writes a database
    /// file.
    fn acme(replicas: &Replicas) -> (FleetDbKey, PathBuf, PathBuf) {
        let key = replicas.fleet.key_for("acme").unwrap();
        let db = replicas.fleet.path_of(&key);
        let path = super::super::fleet::tenant_harvest_file(&db);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&db, b"tenant").unwrap();
        (key, path, db)
    }

    /// A Harvest file at epoch 1 beside its database, and a replica at
    /// epoch 6 that the open can restore.
    fn shipped_acme(dir: &Path, replicas: &Replicas) -> (FleetDbKey, PathBuf, PathBuf) {
        let (key, path, db) = acme(replicas);
        std::fs::create_dir_all(dir.join("b")).unwrap();
        ship_at(replicas, &key, &dir.join("b/acme.harvest.db"), 6);
        let runtime = autumn_harvest_sqlite::SqliteRuntime::open_replicated(&path).unwrap();
        runtime.raise_fleet_epoch(1).unwrap();
        drop(runtime);
        record_database(&path, &db).unwrap();
        (key, path, db)
    }

    /// A `HarvestFleet::restore` of `key` to `target` that publishes a new
    /// database file.
    fn harvest_restore(
        replicas: &Replicas,
        key: &FleetDbKey,
        db: &Path,
        target: Option<DateTime<Utc>>,
    ) {
        replicas.request_restore(key, target);
        replicas.release(key);
        replace_database(db);
        replicas.restore_returned(key);
        replicas.end_restore(key, true);
    }

    /// A release with no `HarvestFleet::restore`, such as a delete, keeps no
    /// target. A failed restore drops its target.
    #[test]
    fn only_a_requested_restore_keeps_a_target() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let (key, path, db) = acme(&replicas);
        replicas.release(&key);
        assert_eq!(entry(&path), None, "a plain release");

        let at = Utc::now();
        replicas.request_restore(&key, Some(at));
        replicas.release(&key);
        assert_eq!(
            entry(&path),
            Some(Entry::Pending {
                target: Target::At(at),
                from: database_identity(&db).unwrap(),
                previous: None,
            })
        );
        replicas.end_restore(&key, false);
        assert_eq!(entry(&path), None, "a failed restore");
    }

    /// A failed restore puts back the target of an earlier one that waits
    /// for the next open.
    #[test]
    fn a_failed_restore_keeps_the_earlier_target() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let (key, path, db) = acme(&replicas);
        let first = Utc::now();
        harvest_restore(&replicas, &key, &db, Some(first));
        let committed = Some(Entry::Committed {
            target: Target::At(first),
            of: database_identity(&db).unwrap(),
        });
        assert_eq!(entry(&path), committed);

        replicas.request_restore(&key, None);
        replicas.release(&key);
        replicas.end_restore(&key, false);
        assert_eq!(entry(&path), committed);
    }

    /// A finished restore applies its target even when the database
    /// identity did not change, as a rename on Windows can keep it.
    #[test]
    fn a_committed_target_applies_with_an_unchanged_identity() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let (key, path, db) = shipped_acme(dir.path(), &replicas);
        replicas.request_restore(&key, None);
        replicas.release(&key);
        replicas.end_restore(&key, true);
        assert!(matches!(entry(&path), Some(Entry::Committed { .. })));
        replicas.prepare(&key, &path, &db, None).unwrap();
        assert_eq!(autumn_harvest_sqlite::fleet_epoch::read(&path).unwrap(), 6);
    }

    /// A direct restore after a `HarvestFleet::restore` supersedes its
    /// target. The open follows the latest replica.
    #[test]
    fn a_direct_restore_supersedes_a_committed_target() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let (key, path, db) = shipped_acme(dir.path(), &replicas);
        harvest_restore(&replicas, &key, &db, unreachable());

        replicas.release(&key);
        replace_database(&db);
        replicas.prepare(&key, &path, &db, None).unwrap();
        assert_eq!(autumn_harvest_sqlite::fleet_epoch::read(&path).unwrap(), 6);
        assert_eq!(entry(&path), None);
    }

    /// A direct restore that fails leaves the database. The target still
    /// applies, so this open refuses as the target asks.
    #[test]
    fn a_failed_direct_restore_keeps_the_committed_target() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let (key, path, db) = shipped_acme(dir.path(), &replicas);
        harvest_restore(&replicas, &key, &db, unreachable());

        replicas.release(&key);
        let refused = replicas.prepare(&key, &path, &db, None).unwrap_err();
        assert!(
            refused.contains("cannot reach the restore target"),
            "{refused}"
        );
        assert!(matches!(entry(&path), Some(Entry::Committed { .. })));
    }

    /// A restart after the release keeps the pending target. The open
    /// applies it once the database file changed, and drops it otherwise.
    #[test]
    fn a_restart_after_the_release_keeps_the_pending_target() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let (key, path, db) = shipped_acme(dir.path(), &replicas);
        replicas.request_restore(&key, unreachable());
        replicas.release(&key);
        let restarted = self::replicas(dir.path());
        restarted.prepare(&key, &path, &db, None).unwrap();
        assert_eq!(entry(&path), None, "the restore failed before the restart");

        replicas.request_restore(&key, unreachable());
        replicas.release(&key);
        replace_database(&db);
        let restarted = self::replicas(dir.path());
        assert!(
            restarted.prepare(&key, &path, &db, None).is_err(),
            "the restore finished"
        );
    }

    /// A release after a restore returned and before its `end_restore` binds
    /// the pending target to the restored file. A later end changes nothing.
    #[test]
    fn a_later_release_binds_the_pending_target() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let (key, path, db) = shipped_acme(dir.path(), &replicas);
        replicas.request_restore(&key, unreachable());
        replicas.release(&key);
        replace_database(&db);
        replicas.restore_returned(&key);

        replicas.release(&key);
        let bound = Some(Entry::Committed {
            target: Target::of(unreachable()),
            of: database_identity(&db).unwrap(),
        });
        assert_eq!(entry(&path), bound);
        replace_database(&db);
        replicas.end_restore(&key, true);
        assert_eq!(entry(&path), bound, "the end found another entry");
        replicas.prepare(&key, &path, &db, None).unwrap();
        assert_eq!(autumn_harvest_sqlite::fleet_epoch::read(&path).unwrap(), 6);
    }

    /// An open holds the target lock. An end that waits on it never writes
    /// back a target that the open used.
    #[test]
    fn a_late_end_never_writes_back_a_used_target() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let (key, path, db) = acme(&replicas);
        replicas.request_restore(&key, None);
        replicas.release(&key);
        replace_database(&db);
        replicas.restore_returned(&key);

        let open = SideLock::acquire(&path, TARGET_LOCK_SUFFIX).unwrap();
        std::thread::scope(|scope| {
            let end = scope.spawn(|| replicas.end_restore(&key, true));
            std::thread::sleep(Duration::from_millis(100));
            assert!(
                matches!(entry(&path), Some(Entry::Pending { .. })),
                "the end waits"
            );
            remove_quietly(&suffixed(&path, TARGET_SUFFIX));
            drop(open);
            end.join().unwrap();
        });
        assert_eq!(entry(&path), None);
    }

    /// An open holds the target lock while it restores. A release or an end
    /// waits for it.
    #[test]
    fn an_open_holds_the_target_lock() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let (key, path, db) = shipped_acme(dir.path(), &replicas);
        replicas.request_restore(&key, None);
        replicas.release(&key);
        replace_database(&db);
        replicas.restore_returned(&key);

        let held = SideLock::acquire(&path, TARGET_LOCK_SUFFIX).unwrap();
        std::thread::scope(|scope| {
            let open = scope.spawn(|| replicas.prepare(&key, &path, &db, None));
            std::thread::sleep(Duration::from_millis(100));
            assert!(!open.is_finished(), "the open waits");
            assert_eq!(autumn_harvest_sqlite::fleet_epoch::read(&path).unwrap(), 1);
            drop(held);
            open.join().unwrap().unwrap();
        });
        assert_eq!(autumn_harvest_sqlite::fleet_epoch::read(&path).unwrap(), 6);
        assert_eq!(entry(&path), None);
    }

    /// A release of another operation can come between the request and the
    /// drain of a restore, as a stale-epoch open does. The last release
    /// rebuilds the entry, so the target follows the restore.
    #[test]
    fn the_last_release_before_the_return_writes_the_target() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let (key, path, db) = shipped_acme(dir.path(), &replicas);
        replicas.request_restore(&key, unreachable());
        // The stale-epoch open releases, replaces the database, then opens.
        replicas.release(&key);
        replace_database(&db);
        replicas.prepare(&key, &path, &db, None).unwrap();
        assert_eq!(autumn_harvest_sqlite::fleet_epoch::read(&path).unwrap(), 6);
        assert!(matches!(entry(&path), Some(Entry::Pending { .. })), "kept");

        // The restore drains, releases and publishes.
        replicas.release(&key);
        let from = database_identity(&db).unwrap();
        replace_database(&db);
        replicas.restore_returned(&key);
        replicas.end_restore(&key, true);
        assert_eq!(
            entry(&path),
            Some(Entry::Committed {
                target: Target::of(unreachable()),
                of: database_identity(&db).unwrap(),
            })
        );
        assert_ne!(from, database_identity(&db).unwrap());
        assert!(
            replicas.prepare(&key, &path, &db, None).is_err(),
            "the target applies"
        );
    }

    /// An open during the drain of a restore keeps the pending target. It
    /// uses the earlier target, so the pending entry drops that one.
    #[test]
    fn an_open_in_the_drain_keeps_the_pending_target() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let (key, path, db) = shipped_acme(dir.path(), &replicas);
        harvest_restore(&replicas, &key, &db, None);
        replicas.request_restore(&key, unreachable());
        replicas.release(&key);

        replicas.prepare(&key, &path, &db, None).unwrap();
        assert_eq!(autumn_harvest_sqlite::fleet_epoch::read(&path).unwrap(), 6);
        let kept = Some(Entry::Pending {
            target: Target::of(unreachable()),
            from: database_identity(&db).unwrap(),
            previous: None,
        });
        assert_eq!(entry(&path), kept);

        replace_database(&db);
        replicas.restore_returned(&key);
        replicas.end_restore(&key, true);
        assert!(matches!(entry(&path), Some(Entry::Committed { .. })));
        assert!(
            replicas.prepare(&key, &path, &db, None).is_err(),
            "the target applies"
        );
    }

    /// A release drops a target whose database file is gone. An inode number
    /// that comes back then never revives it.
    #[test]
    fn a_release_drops_a_superseded_target() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let (key, path, _db) = acme(&replicas);
        let gone = Entry::Committed {
            target: Target::Latest,
            of: "1:1".to_owned(),
        };
        write_entry(&path, Some(&gone)).unwrap();
        replicas.release(&key);
        assert_eq!(entry(&path), None);
    }

    /// A superseded target restores the Harvest file only when the file has
    /// no database record. A file that follows the database stays.
    #[test]
    fn a_superseded_target_restores_only_a_file_with_no_record() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let (key, path, db) = shipped_acme(dir.path(), &replicas);
        let gone = Entry::Committed {
            target: Target::Latest,
            of: "1:1".to_owned(),
        };
        write_entry(&path, Some(&gone)).unwrap();
        replicas.prepare(&key, &path, &db, None).unwrap();
        assert_eq!(
            autumn_harvest_sqlite::fleet_epoch::read(&path).unwrap(),
            1,
            "kept"
        );

        write_entry(&path, Some(&gone)).unwrap();
        std::fs::remove_file(suffixed(&path, IDENTITY_SUFFIX)).unwrap();
        replicas.prepare(&key, &path, &db, None).unwrap();
        assert_eq!(
            autumn_harvest_sqlite::fleet_epoch::read(&path).unwrap(),
            6,
            "restored"
        );
    }

    /// A restart after a failed restore keeps the earlier target that the
    /// pending entry holds.
    #[test]
    fn a_restart_keeps_the_earlier_target_of_a_failed_restore() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let (key, path, db) = shipped_acme(dir.path(), &replicas);
        let pending = Entry::Pending {
            target: Target::Latest,
            from: database_identity(&db).unwrap(),
            previous: Some(Target::of(unreachable())),
        };
        write_entry(&path, Some(&pending)).unwrap();
        let restarted = self::replicas(dir.path());
        assert!(
            restarted.prepare(&key, &path, &db, None).is_err(),
            "the earlier target applies"
        );
    }

    /// A restore of a deleted tenant has no database file at its release.
    /// The entry names no file, so it applies to the restored one.
    #[test]
    fn a_release_with_no_database_names_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let (key, path, db) = acme(&replicas);
        std::fs::remove_file(&db).unwrap();
        replicas.request_restore(&key, None);
        replicas.release(&key);
        assert_eq!(
            entry(&path),
            Some(Entry::Pending {
                target: Target::Latest,
                from: NONE.to_owned(),
                previous: None,
            })
        );
    }

    /// A release replaces an unreadable target, so no restore stays stuck
    /// on it.
    #[test]
    fn a_release_replaces_an_unreadable_target() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let (key, path, _db) = acme(&replicas);
        std::fs::write(suffixed(&path, TARGET_SUFFIX), "garbage text").unwrap();
        replicas.release(&key);
        assert_eq!(entry(&path), None, "a plain release drops it");

        std::fs::write(suffixed(&path, TARGET_SUFFIX), "garbage text").unwrap();
        replicas.request_restore(&key, None);
        replicas.release(&key);
        assert!(matches!(entry(&path), Some(Entry::Pending { .. })));
    }

    /// A restore whose release could not write its target still keeps it
    /// at the end. A restore that the fleet refused before its drain keeps
    /// nothing.
    #[test]
    fn the_end_keeps_a_target_that_the_release_lost() {
        let dir = tempfile::tempdir().unwrap();
        let replicas = replicas(dir.path());
        let (key, path, db) = acme(&replicas);
        replicas.request_restore(&key, None);
        replicas.end_restore(&key, false);
        assert_eq!(entry(&path), None, "refused before the drain");

        replicas.request_restore(&key, None);
        if let Some(requested) = replicas.requested_map().get_mut(&key) {
            requested.released = true;
        }
        replace_database(&db);
        replicas.restore_returned(&key);
        replicas.end_restore(&key, true);
        assert_eq!(
            entry(&path),
            Some(Entry::Committed {
                target: Target::Latest,
                of: database_identity(&db).unwrap(),
            })
        );
    }

    /// Each entry reads back unchanged. An entry in the older format names
    /// no file. The parser refuses other text.
    #[test]
    fn a_target_entry_reads_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("acme.harvest.db");
        let at = DateTime::parse_from_rfc3339("2026-11-12T05:00:00.250Z")
            .unwrap()
            .with_timezone(&Utc);
        for written in [
            Entry::Committed {
                target: Target::At(at),
                of: "64768:42".to_owned(),
            },
            Entry::Pending {
                target: Target::Latest,
                from: NONE.to_owned(),
                previous: Some(Target::At(at)),
            },
            Entry::Pending {
                target: Target::At(at),
                from: "64768:42".to_owned(),
                previous: None,
            },
        ] {
            write_entry(&path, Some(&written)).unwrap();
            assert_eq!(entry(&path), Some(written));
        }
        for (text, target) in [
            ("latest", Target::Latest),
            ("committed 2026-11-12T05:00:00.250Z", Target::At(at)),
        ] {
            std::fs::write(suffixed(&path, TARGET_SUFFIX), text).unwrap();
            let older = Entry::Pending {
                target,
                from: NONE.to_owned(),
                previous: None,
            };
            assert_eq!(entry(&path), Some(older), "{text}");
        }
        for text in [
            "",
            "soon",
            "committed 1:2 soon",
            "pending 1:2 latest",
            "pending 1:2 soon -",
        ] {
            std::fs::write(suffixed(&path, TARGET_SUFFIX), text).unwrap();
            assert!(read_entry(&path).is_err(), "{text}");
        }
        write_entry(&path, None).unwrap();
        assert_eq!(entry(&path), None);
    }

    /// The kernel gives the result of each row of the rule.
    #[test]
    fn resolve_target_gives_the_target_of_each_entry() {
        use Held::{Committed, Nothing, Pending};
        assert_eq!(resolve_target::<u8>(Nothing), Resolution::Nothing);
        assert_eq!(
            resolve_target(Committed {
                target: 1,
                current: true
            }),
            Resolution::Apply(1)
        );
        assert_eq!(
            resolve_target(Committed {
                target: 1,
                current: false
            }),
            Resolution::Superseded
        );
        for previous in [None, Some(2)] {
            assert_eq!(
                resolve_target(Pending {
                    target: 1,
                    moved: true,
                    previous
                }),
                Resolution::Apply(1)
            );
        }
        assert_eq!(
            resolve_target(Pending {
                target: 1,
                moved: false,
                previous: Some(2)
            }),
            Resolution::Apply(2)
        );
        assert_eq!(
            resolve_target(Pending {
                target: 1_u8,
                moved: false,
                previous: None
            }),
            Resolution::Nothing
        );
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
        let pending = Entry::Pending {
            target: Target::Latest,
            from: database_identity(&db).unwrap(),
            previous: None,
        };
        write_entry(&path, Some(&pending)).unwrap();
        replicas.prepare(&key, &path, &db, None).unwrap();
        assert_eq!(autumn_harvest_sqlite::fleet_epoch::read(&path).unwrap(), 4);
        assert_eq!(entry(&path), None);
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
        let pending = Entry::Pending {
            target: Target::Latest,
            from: NONE.to_owned(),
            previous: None,
        };
        write_entry(&path, Some(&pending)).unwrap();
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
        let pending = Entry::Pending {
            target: Target::Latest,
            from: database_identity(&db).unwrap(),
            previous: None,
        };
        replace_database(&db);
        write_entry(&path, Some(&pending)).unwrap();
        replicas.prepare(&key, &path, &db, None).unwrap();
        assert_eq!(
            std::fs::read_to_string(suffixed(&path, IDENTITY_SUFFIX)).unwrap(),
            database_identity(&db).unwrap()
        );
        assert_eq!(entry(&path), None);
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

    /// A slot database ships under its slot prefix.
    #[test]
    fn a_slot_replica_sits_under_the_slot_prefix() {
        let root = tenant_harvest_replica_root("app/test", &FleetDbKey::Slot(42));
        assert_eq!(root, "app/test/fleet/slot/00042/harvest");
    }

    /// A destination that refuses each upload while it is down.
    struct Flaky {
        inner: FileDestination,
        down: AtomicBool,
    }

    impl Flaky {
        fn refused(&self) -> Result<(), autumn_web::replication::DestinationError> {
            if self.down.load(Ordering::SeqCst) {
                return Err(autumn_web::replication::DestinationError::Io {
                    op: "put",
                    detail: "the destination is down".to_owned(),
                });
            }
            Ok(())
        }
    }

    impl ReplicaDestination for Flaky {
        fn describe(&self) -> String {
            self.inner.describe()
        }
        fn put(
            &self,
            key: &str,
            body: &[u8],
        ) -> Result<(), autumn_web::replication::DestinationError> {
            self.refused()?;
            self.inner.put(key, body)
        }
        fn put_file(
            &self,
            key: &str,
            path: &Path,
        ) -> Result<(), autumn_web::replication::DestinationError> {
            self.refused()?;
            self.inner.put_file(key, path)
        }
        fn get(&self, key: &str) -> Result<Vec<u8>, autumn_web::replication::DestinationError> {
            self.inner.get(key)
        }
        fn get_to_file(
            &self,
            key: &str,
            path: &Path,
        ) -> Result<(), autumn_web::replication::DestinationError> {
            self.inner.get_to_file(key, path)
        }
        fn list(
            &self,
            prefix: &str,
        ) -> Result<Vec<String>, autumn_web::replication::DestinationError> {
            self.inner.list(prefix)
        }
        fn delete(&self, key: &str) -> Result<(), autumn_web::replication::DestinationError> {
            self.inner.delete(key)
        }
    }

    /// A close that cannot ship parks its shipper. The loop ships the
    /// backlog once the destination is back, then drops the shipper.
    #[test]
    fn a_failed_close_parks_the_shipper_until_the_backlog_ships() {
        let dir = tempfile::tempdir().unwrap();
        let flaky = Arc::new(Flaky {
            inner: FileDestination::new(dir.path().join("replicas")).unwrap(),
            down: AtomicBool::new(false),
        });
        let replicas = Replicas::new(
            TenantReplica::new(
                Arc::clone(&flaky) as Arc<dyn ReplicaDestination>,
                settings(),
            ),
            Arc::new(autumn_web::time::SystemClock),
            fleet(&dir.path().join("fleet")),
        );
        let key = replicas.fleet.key_for("acme").unwrap();
        let path = super::super::fleet::tenant_harvest_file(&replicas.fleet.path_of(&key));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let runtime = autumn_harvest_sqlite::SqliteRuntime::open_replicated(&path).unwrap();
        runtime.raise_fleet_epoch(1).unwrap();
        replicas.attach(&key, &path, None);
        replicas.tick_all();

        runtime.raise_fleet_epoch(2).unwrap();
        flaky.down.store(true, Ordering::SeqCst);
        drop(runtime);
        replicas.close(&key);
        assert!(replicas.parked_map().contains_key(&key), "the close parked");

        flaky.down.store(false, Ordering::SeqCst);
        replicas.tick_all();
        assert!(replicas.parked_map().is_empty(), "the backlog shipped");

        let restored = dir.path().join("restored.harvest.db");
        let Planned::Plan(plan) = replicas.plan(&key, None).unwrap() else {
            panic!("the replica has a file");
        };
        replicas
            .publish_plan(&key, &restored, &plan, false)
            .unwrap();
        assert_eq!(
            autumn_harvest_sqlite::fleet_epoch::read(&restored).unwrap(),
            2
        );
    }

    /// A release drops a parked shipper, so a delete never waits on it.
    #[test]
    fn a_release_drops_a_parked_shipper() {
        let dir = tempfile::tempdir().unwrap();
        let flaky = Arc::new(Flaky {
            inner: FileDestination::new(dir.path().join("replicas")).unwrap(),
            down: AtomicBool::new(true),
        });
        let replicas = Replicas::new(
            TenantReplica::new(
                Arc::clone(&flaky) as Arc<dyn ReplicaDestination>,
                settings(),
            ),
            Arc::new(autumn_web::time::SystemClock),
            fleet(&dir.path().join("fleet")),
        );
        let key = replicas.fleet.key_for("acme").unwrap();
        let path = dir.path().join("acme.harvest.db");
        let runtime = autumn_harvest_sqlite::SqliteRuntime::open_replicated(&path).unwrap();
        runtime.raise_fleet_epoch(1).unwrap();
        replicas.attach(&key, &path, None);
        drop(runtime);
        replicas.close(&key);
        assert!(replicas.parked_map().contains_key(&key));
        replicas.release(&key);
        assert!(replicas.parked_map().is_empty());
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
