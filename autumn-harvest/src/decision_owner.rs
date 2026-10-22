//! The fenced decision owner of a partition (issue #2251).
//!
//! [ADR 0009](../../docs/adr/0009-decision-owner.md) gives the protocol.
//! One owner row per decision partition lives in `harvest_decision_owner`,
//! in the database that holds the history it fences.
//!
//! - An acquisition takes the row `FOR UPDATE` and adds 1 to the epoch.
//!   The lock waits for each commit in flight. That wait is the drain.
//! - A renewal changes `lease_until` only. It never changes the epoch, so
//!   it never waits for a commit.
//! - A decision commit takes the row `FOR KEY SHARE`. It compares the
//!   holder and the epoch that its acquisition pinned. A mismatch aborts
//!   the whole transaction with [`HarvestError::OwnershipLost`].
//!
//! A decision pins its tenure in a task-local with [`pin_decision`].
//! [`crate::replication::assert_fence`] then fences the owner row at each
//! event insert of that task. A write in any other task takes no owner row,
//! so a foreign writer never waits for an owner.
//!
//! Issue #2252 adds owner mode. A per-database switch turns it on. The
//! enable step installs a guard on the claim, so a legacy claim takes no
//! workflow task. Each worker then acquires partitions, renews them and
//! claims workflow tasks of its own partitions only. See
//! [`enable_owner_mode`] and [`OwnerLoop`].

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use crate::error::{HarvestError, HarvestResult};
use crate::types::ShardId;

/// The number of decision partitions in each physical database.
///
/// ADR 0009 decision 2 fixes it. It never changes for a database, because
/// each change would move executions to other partitions.
pub const DECISION_PARTITIONS: u16 = 512;

/// The hash prefix. It keeps the partition independent of shard routing.
const HASH_PREFIX: &[u8] = b"harvest.decision_partition";

/// The worker label that says a binary can run owner mode (issue #2252).
///
/// Each registration and heartbeat writes it. The enable step refuses while
/// a live worker lacks it. The value is a number, so capability matching
/// never reads it as an operator label.
pub const OWNER_MODE_CAPABILITY_LABEL: &str = "decision_owner_version";

/// The value of [`OWNER_MODE_CAPABILITY_LABEL`] for this binary.
pub const OWNER_MODE_CAPABILITY_VERSION: i64 = 1;

/// The worker label that names the workflow types an owner can run (issue
/// #2252).
///
/// An owner claims every workflow task of its partitions. So the enable step
/// refuses while live owners advertise different values. The value is
/// [`workflow_set_fingerprint`], or [`NO_OWNED_WORKFLOWS`] for a worker that
/// owns no partition.
pub const OWNER_WORKFLOWS_LABEL: &str = "decision_owner_workflows";

/// The [`OWNER_WORKFLOWS_LABEL`] value of a worker that owns no partition.
///
/// The enable step skips such a worker in the workflow type check.
pub const NO_OWNED_WORKFLOWS: &str = "none";

/// A fingerprint of a set of workflow type names (issue #2252).
///
/// Order and duplicates do not change it. Each name ends with a NUL byte, so
/// two different sets never join into the same bytes.
#[must_use]
pub fn workflow_set_fingerprint<'a>(names: impl IntoIterator<Item = &'a str>) -> String {
    let set: std::collections::BTreeSet<&str> = names.into_iter().collect();
    let mut bytes = Vec::new();
    for name in set {
        bytes.extend_from_slice(name.as_bytes());
        bytes.push(0);
    }
    format!("{:016x}", seahash::hash(&bytes))
}

/// The labels `labels` with the owner-mode capability of this binary.
///
/// The engine writes the label, so a value in `labels` never survives.
#[must_use]
pub fn advertise_owner_capability(labels: &serde_json::Value) -> serde_json::Value {
    let mut merged = match labels {
        serde_json::Value::Object(map) => serde_json::Value::Object(map.clone()),
        _ => serde_json::Value::Object(serde_json::Map::new()),
    };
    merged[OWNER_MODE_CAPABILITY_LABEL] = serde_json::Value::from(OWNER_MODE_CAPABILITY_VERSION);
    merged
}

/// A decision partition: a fixed hash bucket of executions in one database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DecisionPartition(u16);

impl DecisionPartition {
    /// The partition `n`, or `None` when `n` is not below
    /// [`DECISION_PARTITIONS`].
    #[must_use]
    pub const fn new(n: u16) -> Option<Self> {
        if n < DECISION_PARTITIONS {
            Some(Self(n))
        } else {
            None
        }
    }

    /// The partition of `workflow_id` (ADR 0009 decision 3).
    ///
    /// The hash is `seahash` of the prefix and the id. `seahash` is stable
    /// across processes and builds.
    #[must_use]
    pub fn for_workflow_id(workflow_id: &str) -> Self {
        let mut bytes = Vec::with_capacity(HASH_PREFIX.len() + workflow_id.len());
        bytes.extend_from_slice(HASH_PREFIX);
        bytes.extend_from_slice(workflow_id.as_bytes());
        let bucket = seahash::hash(&bytes) % u64::from(DECISION_PARTITIONS);
        // The bucket is below 512, so the conversion cannot fail.
        Self(u16::try_from(bucket).unwrap_or(0))
    }

    /// The partition number.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }

    /// The partition number as the `SMALLINT` of
    /// `harvest_task_queue.decision_partition` (issue #2252).
    #[must_use]
    pub const fn as_i16(self) -> i16 {
        // The number is below 512, so it fits.
        #[allow(clippy::cast_possible_wrap)]
        let n = self.0 as i16;
        n
    }
}

impl fmt::Display for DecisionPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "p{}", self.0)
    }
}

/// An owner epoch. Each acquisition adds 1. At epoch 0, no holder has
/// acquired the row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OwnerEpoch(i64);

impl OwnerEpoch {
    /// The epoch `n`.
    #[must_use]
    pub const fn new(n: i64) -> Self {
        Self(n)
    }

    /// The epoch value, as the database stores it.
    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }

    /// The epoch of the next acquisition, or `None` on overflow.
    #[must_use]
    pub const fn next(self) -> Option<Self> {
        match next_epoch(self.0) {
            Some(n) => Some(Self(n)),
            None => None,
        }
    }
}

impl fmt::Display for OwnerEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "e{}", self.0)
    }
}

/// The epoch after `epoch`, or `None` at `i64::MAX`.
///
/// `formal/verus/src/owner_fence.rs` proves that the result is greater
/// than `epoch` and that it never wraps.
#[must_use]
pub const fn next_epoch(epoch: i64) -> Option<i64> {
    if epoch < i64::MAX {
        Some(epoch + 1)
    } else {
        None
    }
}

/// Whether the fence lets a commit pass.
///
/// `pinned` is the epoch that the decision pinned. `row` is the epoch of the
/// owner row under the commit lock. `formal/verus/src/owner_fence.rs`
/// proves that no commit passes at an epoch older than the row's.
#[must_use]
pub const fn fence_passes(pinned: i64, row: i64, holder_matches: bool) -> bool {
    holder_matches && pinned == row
}

/// The holder token of one process (ADR 0009 decision 5).
///
/// It is the worker id and a start nonce. A replacement worker with the
/// same id is a new holder, so it cannot renew the old lease.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OwnerHolder(Arc<str>);

impl OwnerHolder {
    /// A new holder token for `worker_id`.
    #[must_use]
    pub fn for_worker(worker_id: &str) -> Self {
        let nonce = uuid::Uuid::new_v4().simple();
        Self(Arc::from(format!("{worker_id}/{nonce}")))
    }

    /// The token, as the database stores it.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for OwnerHolder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The lease TTL (ADR 0009 decision 8). The range is 3 s to 300 s.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OwnerLeaseTtl(Duration);

impl OwnerLeaseTtl {
    /// The shortest TTL. Issue #2254 measures whether a shorter one is safe.
    pub const MIN: Duration = Duration::from_secs(3);
    /// The longest TTL, the same as the longest scanner lease.
    pub const MAX: Duration = Duration::from_secs(300);
    /// The default TTL, the same as the scanner lease default.
    pub const DEFAULT: Self = Self(Duration::from_secs(10));

    /// A TTL of `ttl`.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Config`] when `ttl` is outside the range.
    pub fn new(ttl: Duration) -> HarvestResult<Self> {
        if (Self::MIN..=Self::MAX).contains(&ttl) {
            Ok(Self(ttl))
        } else {
            Err(HarvestError::Config(format!(
                "the owner lease TTL must be from {:?} to {:?}, not {ttl:?}",
                Self::MIN,
                Self::MAX
            )))
        }
    }

    /// The TTL.
    #[must_use]
    pub const fn get(self) -> Duration {
        self.0
    }

    /// The renewal interval: one third of the TTL.
    #[must_use]
    pub fn renew_interval(self) -> Duration {
        self.0 / 3
    }

    /// The local deadline of a lease request sent at `sent_at`.
    ///
    /// It is the send time + TTL − one renewal interval, on the monotonic
    /// clock. At that deadline the holder stops new claims and commits.
    #[must_use]
    pub fn local_deadline(self, sent_at: Instant) -> Instant {
        sent_at + self.0.saturating_sub(self.renew_interval())
    }
}

impl Default for OwnerLeaseTtl {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// The settings of the lease statements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnerLeaseConfig {
    /// The lease TTL.
    pub ttl: OwnerLeaseTtl,
    /// The longest wait for an owner row lock. A miss is a failed attempt.
    pub lock_timeout: Duration,
}

impl OwnerLeaseConfig {
    /// The default lock wait of an acquisition.
    pub const DEFAULT_LOCK_TIMEOUT: Duration = Duration::from_secs(1);

    /// This config with `ttl`.
    #[must_use]
    pub const fn with_ttl(mut self, ttl: OwnerLeaseTtl) -> Self {
        self.ttl = ttl;
        self
    }

    /// This config with `lock_timeout`.
    #[must_use]
    pub const fn with_lock_timeout(mut self, lock_timeout: Duration) -> Self {
        self.lock_timeout = lock_timeout;
        self
    }
}

impl Default for OwnerLeaseConfig {
    fn default() -> Self {
        Self {
            ttl: OwnerLeaseTtl::DEFAULT,
            lock_timeout: Self::DEFAULT_LOCK_TIMEOUT,
        }
    }
}

/// One decision partition of one database.
///
/// Each physical database has its own 512 partitions (ADR 0009 decision 2).
/// A worker can serve several databases, so it keys its tenures and its
/// cache tags by the shard and the partition. Issue #2252 maps each shard of
/// a colocated group to one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OwnerKey {
    /// The shard whose database holds the owner row.
    pub shard: ShardId,
    /// The partition.
    pub partition: DecisionPartition,
}

impl OwnerKey {
    /// The lowest key of `shard`.
    const fn first_of(shard: ShardId) -> Self {
        Self {
            shard,
            partition: DecisionPartition(0),
        }
    }

    /// The highest key of `shard`.
    const fn last_of(shard: ShardId) -> Self {
        Self {
            shard,
            partition: DecisionPartition(DECISION_PARTITIONS - 1),
        }
    }

    /// The shard that keys the tenures of a worker pool.
    ///
    /// `pool` is the shard of the pool that a task came from. A worker with
    /// one plain pool has no shard identity, so it uses
    /// [`ShardId::UNENCODED`]. The acquisition loop of issue #2252 adopts
    /// each tenure under the same shard.
    #[must_use]
    pub fn pool_shard(pool: Option<ShardId>) -> ShardId {
        pool.unwrap_or(ShardId::UNENCODED)
    }

    /// The key of an execution with `workflow_id` on `shard`.
    #[must_use]
    pub fn of(shard: ShardId, workflow_id: &str) -> Self {
        Self {
            shard,
            partition: DecisionPartition::for_workflow_id(workflow_id),
        }
    }
}

impl fmt::Display for OwnerKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.shard.as_i32(), self.partition)
    }
}

/// What an acquisition pinned: one holder at one epoch of one partition.
///
/// Only [`acquire`] makes a tenure. The holder keeps it and never adopts an
/// epoch that it reads later.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerTenure {
    shard: ShardId,
    partition: DecisionPartition,
    holder: OwnerHolder,
    epoch: OwnerEpoch,
    ttl: OwnerLeaseTtl,
}

impl OwnerTenure {
    #[cfg_attr(not(any(test, feature = "db")), allow(dead_code))]
    pub(crate) const fn pinned(
        shard: ShardId,
        partition: DecisionPartition,
        holder: OwnerHolder,
        epoch: OwnerEpoch,
        ttl: OwnerLeaseTtl,
    ) -> Self {
        Self {
            shard,
            partition,
            holder,
            epoch,
            ttl,
        }
    }

    /// The shard that the acquisition ran on.
    #[must_use]
    pub const fn shard(&self) -> ShardId {
        self.shard
    }

    /// The shard and the partition.
    #[must_use]
    pub const fn key(&self) -> OwnerKey {
        OwnerKey {
            shard: self.shard,
            partition: self.partition,
        }
    }

    /// The partition.
    #[must_use]
    pub const fn partition(&self) -> DecisionPartition {
        self.partition
    }

    /// The holder token.
    #[must_use]
    pub const fn holder(&self) -> &OwnerHolder {
        &self.holder
    }

    /// The pinned epoch.
    #[must_use]
    pub const fn epoch(&self) -> OwnerEpoch {
        self.epoch
    }

    /// The lease TTL.
    #[must_use]
    pub const fn ttl(&self) -> OwnerLeaseTtl {
        self.ttl
    }
}

/// The result of an acquisition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Acquisition {
    /// The row is free or its lease ended. The holder owns it now.
    Acquired(OwnerTenure),
    /// Another lease is live. Nothing changed.
    Held,
    /// A lock wait hit the lock timeout. Nothing changed. Try again later.
    Busy,
}

/// The rows that a renewal or a release matched.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Matched {
    /// The partitions that the statement wrote, in ascending order.
    pub matched: Vec<DecisionPartition>,
    /// The partitions whose row moved or whose lease ended, in ascending
    /// order. The holder does not own them.
    pub lost: Vec<DecisionPartition>,
}

/// The result of a renewal or a release of many rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseWrite {
    /// The statement ran.
    Done(Matched),
    /// A lock wait hit the lock timeout. Nothing changed.
    Busy,
}

impl LeaseWrite {
    /// A `Done` result. Both lists are sorted, with no duplicate.
    #[must_use]
    pub fn done(mut matched: Vec<DecisionPartition>, mut lost: Vec<DecisionPartition>) -> Self {
        matched.sort_unstable();
        matched.dedup();
        lost.sort_unstable();
        lost.dedup();
        Self::Done(Matched { matched, lost })
    }
}

tokio::task_local! {
    /// The tenure of the decision that this task runs, if any.
    static DECISION: RefCell<Option<OwnerTenure>>;

    /// The tenures of the worker that runs this task (issue #2252).
    static OWNED: Arc<OwnedPartitions>;

    /// The connection and the tenure that the current transaction fenced
    /// (issue #2252).
    static FENCED: (usize, OwnerTenure);
}

/// Run `body` as the rest of a transaction whose decision fence passed.
///
/// `conn` names the connection of that transaction, as `conn_id` gives
/// it. Inside `body`, a fence of that connection
/// for the pinned tenure sends no query. The first fence holds its row
/// locks until the transaction ends, so no takeover and no generation bump
/// can commit before then. A fence of any other connection still runs. The
/// scope ends with `body`, so wrap only the rest of the fenced transaction.
/// With no tenure pinned, this only runs `body`.
pub async fn fenced_scope<F: Future>(conn: usize, body: F) -> F::Output {
    match pinned_decision() {
        Some(tenure) => FENCED.scope((conn, tenure), body).await,
        None => body.await,
    }
}

/// Whether connection `conn` fenced `tenure` in the current transaction,
/// inside [`fenced_scope`].
#[must_use]
pub fn already_fenced(conn: usize, tenure: &OwnerTenure) -> bool {
    FENCED
        .try_with(|(fenced, held)| *fenced == conn && held == tenure)
        .unwrap_or(false)
}

/// Run `body` in a decision scope of the worker that holds `owned`.
///
/// Each worker holds its own tenures. Two workers can share one handler
/// registry, so the registry cannot hold them for both. Inside the scope,
/// [`current_owned`] returns `owned`.
pub async fn scope_decision_for<F: Future>(owned: Arc<OwnedPartitions>, body: F) -> F::Output {
    OWNED.scope(owned, scope_decision(body)).await
}

/// The tenures of the worker that runs this task, inside
/// [`scope_decision_for`].
#[must_use]
pub fn current_owned() -> Option<Arc<OwnedPartitions>> {
    OWNED.try_with(Arc::clone).ok()
}

/// Run `body` in a decision scope. [`pin_decision`] sets its tenure.
pub async fn scope_decision<F: Future>(body: F) -> F::Output {
    DECISION.scope(RefCell::new(None), body).await
}

/// Pin `tenure` for the rest of the current decision scope.
///
/// Returns `false` outside a decision scope. The call then pins nothing.
#[must_use = "a pin outside a decision scope fences nothing"]
pub fn pin_decision(tenure: Option<OwnerTenure>) -> bool {
    DECISION.try_with(|pinned| pinned.replace(tenure)).is_ok()
}

/// The tenure that the current decision pinned, if any.
#[must_use]
pub fn pinned_decision() -> Option<OwnerTenure> {
    DECISION
        .try_with(|pinned| pinned.borrow().clone())
        .ok()
        .flatten()
}

/// How a decision of one partition commits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecisionPin {
    /// Owner mode is off on this shard. The decision takes no owner row.
    Legacy,
    /// This worker owns the partition. Each history write fences this tenure.
    Owned(OwnerTenure),
    /// Owner mode is on, but this worker holds no tenure. The decision must
    /// not write. The worker re-pends the task.
    Unowned,
}

/// The tenures that one worker holds, one per shard and partition.
///
/// The acquisition loop of issue #2252 adopts each tenure and turns owner
/// mode on for each shard. The decision path reads them. A fence unloads a
/// tenure.
#[derive(Debug, Default)]
pub struct OwnedPartitions {
    state: Mutex<Owned>,
    /// A worker uses these tenures. A second worker on the same registry
    /// takes a set of its own.
    claimed: std::sync::atomic::AtomicBool,
}

#[derive(Debug, Default)]
struct Owned {
    tenures: BTreeMap<OwnerKey, OwnerTenure>,
    /// The local deadline of each tenure (ADR 0009 decision 8). A tenure
    /// with no entry has no deadline.
    deadlines: BTreeMap<OwnerKey, Instant>,
    owner_mode: std::collections::BTreeSet<ShardId>,
    /// The shards whose claims use the owned form. Only the owner loop sets
    /// it, when the switch of the database is on.
    claim_shards: std::collections::BTreeSet<ShardId>,
    holder: Option<OwnerHolder>,
}

impl Owned {
    /// The tenure of `key` when its local deadline has not passed.
    fn live(&self, key: OwnerKey, now: Instant) -> Option<&OwnerTenure> {
        let live = self.deadlines.get(&key).is_none_or(|d| now < *d);
        self.tenures.get(&key).filter(|_| live)
    }
}

impl OwnedPartitions {
    fn state(&self) -> std::sync::MutexGuard<'_, Owned> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Take these tenures for one worker. Returns `false` when another
    /// worker took them first.
    pub fn try_claim(&self) -> bool {
        !self.claimed.swap(true, std::sync::atomic::Ordering::AcqRel)
    }

    /// Turn owner mode on or off for `shard`.
    ///
    /// With owner mode on, a decision with no tenure fails closed. Issue
    /// #2252 sets it from the per-shard switch.
    pub fn set_owner_mode(&self, shard: ShardId, on: bool) {
        let mut state = self.state();
        if on {
            state.owner_mode.insert(shard);
        } else {
            state.owner_mode.remove(&shard);
        }
    }

    /// Whether owner mode is on for `shard`.
    #[must_use]
    pub fn owner_mode(&self, shard: ShardId) -> bool {
        self.state().owner_mode.contains(&shard)
    }

    /// Hold `tenure`. Returns the tenure that it replaces.
    ///
    /// The caller drops the resident state of the partition first. A new
    /// tenure starts cold (rule R4 of ADR 0010). The tenure has no local
    /// deadline. The acquisition loop uses [`Self::adopt_until`].
    pub fn adopt(&self, tenure: OwnerTenure) -> Option<OwnerTenure> {
        let mut state = self.state();
        state.deadlines.remove(&tenure.key());
        state.tenures.insert(tenure.key(), tenure)
    }

    /// Hold `tenure` until its local `deadline` (issue #2252).
    ///
    /// After the deadline, the worker claims no task and commits no decision
    /// in the partition, until a renewal moves the deadline.
    pub fn adopt_until(&self, tenure: OwnerTenure, deadline: Instant) -> Option<OwnerTenure> {
        let mut state = self.state();
        state.deadlines.insert(tenure.key(), deadline);
        state.tenures.insert(tenure.key(), tenure)
    }

    /// Move the local deadline of each tenure of `renewed` to `deadline`.
    ///
    /// A tenure that a fence or a newer acquisition replaced keeps its own
    /// deadline. So a late renewal result never revives a dropped tenure.
    pub fn extend(&self, renewed: &[OwnerTenure], deadline: Instant) {
        let mut state = self.state();
        for tenure in renewed {
            let current = state
                .tenures
                .get(&tenure.key())
                .is_some_and(|t| t.epoch == tenure.epoch && t.holder == tenure.holder);
            if current {
                state.deadlines.insert(tenure.key(), deadline);
            }
        }
    }

    /// Set the holder token of this process (issue #2252).
    pub fn set_holder(&self, holder: OwnerHolder) {
        self.state().holder = Some(holder);
    }

    /// The holder token of this process, once the acquisition loop set it.
    #[must_use]
    pub fn holder(&self) -> Option<OwnerHolder> {
        self.state().holder.clone()
    }

    /// Claim in the owned form on `shard`, or in the legacy form.
    ///
    /// The owner loop turns it on when the switch of the database is on.
    pub fn set_owned_claims(&self, shard: ShardId, on: bool) {
        let mut state = self.state();
        if on {
            state.claim_shards.insert(shard);
        } else {
            state.claim_shards.remove(&shard);
        }
    }

    /// The owned claim of `shard`, or `None` for the legacy claim.
    ///
    /// The partitions hold each live tenure of the shard. A tenure past its
    /// local deadline is left out.
    #[must_use]
    pub fn owned_claim(&self, shard: ShardId) -> Option<OwnedClaim> {
        let state = self.state();
        if !state.claim_shards.contains(&shard) {
            return None;
        }
        let holder = state.holder.as_ref()?.as_str().to_string();
        let now = Instant::now();
        let partitions = state
            .tenures
            .range(OwnerKey::first_of(shard)..=OwnerKey::last_of(shard))
            .filter(|(key, _)| state.live(**key, now).is_some())
            .map(|(key, _)| key.partition.as_i16())
            .collect();
        drop(state);
        Some(OwnedClaim { holder, partitions })
    }

    /// The tenures of `shard`, in partition order.
    #[must_use]
    pub fn tenures_of(&self, shard: ShardId) -> Vec<OwnerTenure> {
        self.state()
            .tenures
            .range(OwnerKey::first_of(shard)..=OwnerKey::last_of(shard))
            .map(|(_, t)| t.clone())
            .collect()
    }

    /// The tenure of `key`, if this worker holds one.
    #[must_use]
    pub fn tenure_for(&self, key: OwnerKey) -> Option<OwnerTenure> {
        self.state().tenures.get(&key).cloned()
    }

    /// How a decision of `key` commits.
    ///
    /// A tenure past its local deadline counts as no tenure, so the decision
    /// fails closed.
    #[must_use]
    pub fn pin_for(&self, key: OwnerKey) -> DecisionPin {
        let state = self.state();
        match state.live(key, Instant::now()) {
            Some(tenure) => DecisionPin::Owned(tenure.clone()),
            None if state.owner_mode.contains(&key.shard) => DecisionPin::Unowned,
            None => DecisionPin::Legacy,
        }
    }

    /// Drop the tenure of `key` when it still pins `epoch`.
    ///
    /// A fence of an older tenure leaves a newer one alone. Returns `true`
    /// when this call dropped the tenure.
    pub fn unload(&self, key: OwnerKey, epoch: OwnerEpoch) -> bool {
        let mut state = self.state();
        let current = state.tenures.get(&key).is_some_and(|t| t.epoch == epoch);
        if current {
            state.tenures.remove(&key);
            state.deadlines.remove(&key);
        }
        drop(state);
        current
    }

    /// Whether a decision that pinned `pinned` may still cache its state.
    ///
    /// A tenure that a fence dropped, or that a newer one replaced, may not.
    /// A decision with no tenure caches as before.
    #[must_use]
    pub fn may_cache(&self, key: OwnerKey, pinned: Option<OwnerEpoch>) -> bool {
        let state = self.state();
        pinned.map_or_else(
            || !state.owner_mode.contains(&key.shard),
            |epoch| state.tenures.get(&key).is_some_and(|t| t.epoch == epoch),
        )
    }

    /// Every tenure, in key order.
    #[must_use]
    pub fn tenures(&self) -> Vec<OwnerTenure> {
        self.state().tenures.values().cloned().collect()
    }

    /// The number of tenures.
    #[must_use]
    pub fn len(&self) -> usize {
        self.state().tenures.len()
    }

    /// Whether this worker holds no tenure.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.state().tenures.is_empty()
    }
}

/// The owned claim of a worker in owner mode (issue #2252).
///
/// ADR 0009 decision 11. The owned claim takes a workflow row only when the
/// owner-mode switch of the database is on, and only from `partitions`. It
/// also takes an unstamped row: the worker stamps it and re-pends it to its
/// owner. Activity rows are unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedClaim {
    /// The holder token of the worker. The claim guard matches it with the
    /// owner row of the claimed partition.
    pub holder: String,
    /// The partitions that the worker owns. An empty list takes no stamped
    /// workflow row.
    pub partitions: Vec<i16>,
}

impl OwnedClaim {
    /// The partitions as a 64-byte bit mask, as the claim binds them.
    ///
    /// Bit `p` is bit `p % 8` of byte `p / 8`, as Postgres `get_bit` on
    /// `bytea` reads it. A test of one bit costs the same for any number of
    /// partitions. An array test costs one comparison per element.
    #[must_use]
    pub fn mask(&self) -> Vec<u8> {
        let mut mask = vec![0u8; usize::from(DECISION_PARTITIONS / 8)];
        for p in &self.partitions {
            if let Ok(p) = usize::try_from(*p)
                && let Some(byte) = mask.get_mut(p / 8)
            {
                *byte |= 1 << (p % 8);
            }
        }
        mask
    }
}

/// What one balance step does (issue #2252, ADR 0009 decision 9).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BalancePlan {
    /// `⌈512 / live owners⌉`.
    pub target: usize,
    /// How many free partitions to acquire.
    pub acquire: usize,
    /// How many held partitions to release.
    pub release: usize,
}

/// Plan one balance step for a worker that holds `held` partitions.
///
/// - `peers` is the number of other live owners on the database.
/// - `free` is the number of partitions with no live lease.
/// - `orphans` is the number of free partitions whose lease ended more than
///   one renewal interval ago. No peer took them, so any owner may.
///
/// A worker below the target acquires up to the target. A worker above
/// `target + 1` releases down to the target. Any worker acquires orphans,
/// so a crashed peer that still looks alive cannot strand its partitions.
#[must_use]
pub fn plan_balance(held: usize, peers: usize, free: usize, orphans: usize) -> BalancePlan {
    let owners = peers.saturating_add(1);
    let target = usize::from(DECISION_PARTITIONS).div_ceil(owners);
    if held > target + 1 {
        return BalancePlan {
            target,
            acquire: 0,
            release: held - target,
        };
    }
    let below = target.saturating_sub(held).min(free);
    BalancePlan {
        target,
        acquire: below.max(orphans.min(free)),
        release: 0,
    }
}

/// The owner-mode switch of one database (issue #2252).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnerMode {
    /// Whether owner mode is on.
    pub enabled: bool,
    /// The partition count of the database. A process that reads a count
    /// other than [`DECISION_PARTITIONS`] refuses owner mode.
    pub partitions: i32,
    /// Whether the claim guard is installed.
    pub guard_installed: bool,
}

impl OwnerMode {
    /// Whether this binary can run owner mode on the database.
    #[must_use]
    pub fn count_matches(self) -> bool {
        self.partitions == i32::from(DECISION_PARTITIONS)
    }
}

/// The settings of [`enable_owner_mode`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnableOwnerMode {
    /// A worker with a heartbeat this recent is live.
    pub live_within: Duration,
    /// Enable even when a check refuses: a live worker cannot run owner
    /// mode, the fleet is mixed, or no owner loop is live.
    pub force: bool,
    /// The lock wait of each statement on `harvest_task_queue`.
    pub lock_timeout: Duration,
    /// The rows per stamp batch.
    pub batch: i64,
}

impl Default for EnableOwnerMode {
    fn default() -> Self {
        Self {
            live_within: Duration::from_secs(30),
            force: false,
            lock_timeout: Duration::from_secs(5),
            batch: 1000,
        }
    }
}

/// What [`enable_owner_mode`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EnableReport {
    /// Owner mode was already on. The call changed nothing.
    pub already_on: bool,
    /// Workflow-task rows that the call stamped with their partition.
    pub stamped: u64,
    /// Legacy claims of workers that are not live. The call re-pended them.
    /// A live worker finishes its own legacy decision.
    pub repended: u64,
    /// Live workers that cannot run owner mode. `force` was set.
    pub legacy_workers: Vec<String>,
}

#[cfg(feature = "db")]
pub use db::{
    OwnerLoop, OwnerTick, acquire, conn_id, disable_owner_mode, enable_owner_mode,
    end_upload_allowance, expire_all, fence, fence_pinned, is_lock_miss, read_owner_mode,
    release_all, renew_all, start_upload_allowance,
};

#[cfg(feature = "db")]
mod db {
    use std::time::Duration;

    use diesel::sql_types::{Array, BigInt, Bool, Double, Integer, Nullable, SmallInt, Text};
    use diesel::{OptionalExtension, QueryableByName};
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};

    use super::{
        Acquisition, DECISION_PARTITIONS, DecisionPartition, EnableOwnerMode, EnableReport,
        LeaseWrite, OWNER_MODE_CAPABILITY_VERSION, OwnerEpoch, OwnerHolder, OwnerLeaseConfig,
        OwnerMode, OwnerTenure, fence_passes, next_epoch, pinned_decision,
    };
    use crate::error::{HarvestError, HarvestResult, database_error};
    use crate::types::ShardId;

    /// Lock a takeable row for an acquisition.
    ///
    /// The `WHERE` skips a live row without a lock. A takeable row waits for
    /// each commit that holds it `FOR KEY SHARE`. Postgres checks the row
    /// again after the wait, so a renewal in between makes it live again.
    const ACQUIRE_LOCK_SQL: &str = "SELECT epoch FROM harvest_decision_owner \
         WHERE partition = $1 \
           AND (holder IS NULL OR lease_until <= clock_timestamp()) \
         FOR UPDATE";

    /// Whether the row exists at all.
    const ROW_EXISTS_SQL: &str = "SELECT epoch FROM harvest_decision_owner WHERE partition = $1";

    /// Write the new holder and epoch under the row lock.
    ///
    /// `clock_timestamp()` reads the clock after the lock wait. `NOW()`
    /// returns the start time of the transaction. After a long wait, a lease
    /// from `NOW()` can end too early.
    const ACQUIRE_WRITE_SQL: &str = "UPDATE harvest_decision_owner \
         SET holder = $2, epoch = $3, \
             lease_until = clock_timestamp() + make_interval(secs => $4) \
         WHERE partition = $1 AND epoch = $5 \
         RETURNING epoch";

    /// Lock the live rows of a renewal, in ascending partition order.
    ///
    /// `FOR NO KEY UPDATE` does not conflict with a commit's `FOR KEY SHARE`.
    /// So a renewal never waits for a commit.
    const RENEW_LOCK_SQL: &str = "SELECT o.partition FROM harvest_decision_owner o \
         JOIN unnest($1::int[], $2::bigint[], $3::text[]) AS w(partition, epoch, holder) \
           ON o.partition = w.partition AND o.epoch = w.epoch AND o.holder = w.holder \
         WHERE o.lease_until > clock_timestamp() \
         ORDER BY o.partition \
         FOR NO KEY UPDATE OF o";

    /// Extend each locked lease. The epoch stays.
    const RENEW_WRITE_SQL: &str = "UPDATE harvest_decision_owner o \
         SET lease_until = clock_timestamp() + make_interval(secs => w.ttl) \
         FROM unnest($1::int[], $2::float8[]) AS w(partition, ttl) \
         WHERE o.partition = w.partition";

    /// Lock the rows of a release, in ascending partition order.
    const RELEASE_LOCK_SQL: &str = "SELECT o.partition FROM harvest_decision_owner o \
         JOIN unnest($1::int[], $2::bigint[], $3::text[]) AS w(partition, epoch, holder) \
           ON o.partition = w.partition AND o.epoch = w.epoch AND o.holder = w.holder \
         ORDER BY o.partition \
         FOR NO KEY UPDATE OF o";

    /// End each locked lease now. The holder and the epoch stay, so a
    /// release is a lapse: the next acquisition adds 1. A commit of the
    /// releaser that holds its fence still passes, and no other owner can
    /// acquire before it ends.
    const RELEASE_WRITE_SQL: &str = "UPDATE harvest_decision_owner \
         SET lease_until = LEAST(lease_until, clock_timestamp()) \
         WHERE partition = ANY($1::int[])";

    /// Lock every live row for the DR bump, in ascending partition order.
    const EXPIRE_LOCK_SQL: &str = "SELECT partition FROM harvest_decision_owner \
         WHERE lease_until > clock_timestamp() \
         ORDER BY partition \
         FOR NO KEY UPDATE";

    /// End each locked lease now. The holder and the epoch stay.
    const EXPIRE_WRITE_SQL: &str = "UPDATE harvest_decision_owner \
         SET lease_until = clock_timestamp() \
         WHERE partition = ANY($1::int[])";

    /// The commit fence: the migration's `harvest_decision_owner_fence`.
    ///
    /// It takes the row `FOR KEY SHARE` and waits at most 250 ms. A lock miss
    /// returns `out_locked = false`. It also sets
    /// `idle_in_transaction_session_timeout` for the caller's transaction.
    const FENCE_SQL: &str = "SELECT out_holder AS holder, out_epoch AS epoch, \
                 out_locked AS locked \
         FROM harvest_decision_owner_fence($1, $2)";

    /// Set the idle bound of the current transaction.
    const IDLE_SQL: &str =
        "SELECT set_config('idle_in_transaction_session_timeout', $1, true) AS v";

    #[derive(QueryableByName)]
    struct EpochRow {
        #[diesel(sql_type = BigInt)]
        epoch: i64,
    }

    #[derive(QueryableByName)]
    struct PartitionRow {
        #[diesel(sql_type = Integer)]
        partition: i32,
    }

    #[derive(QueryableByName)]
    struct FenceRow {
        #[diesel(sql_type = Nullable<Text>)]
        holder: Option<String>,
        #[diesel(sql_type = Nullable<BigInt>)]
        epoch: Option<i64>,
        #[diesel(sql_type = Bool)]
        locked: bool,
    }

    #[derive(QueryableByName)]
    struct SettingRow {
        #[diesel(sql_type = Text)]
        #[allow(dead_code)]
        v: String,
    }

    /// Whether `msg` is a lock miss: a `lock_timeout` or a `NOWAIT` refusal
    /// (SQLSTATE `55P03`).
    ///
    /// Diesel keeps no SQLSTATE, so the check reads the English message. On
    /// a server with another `lc_messages`, a lease statement then returns a
    /// database error instead of `Busy`. That is safe: nothing changed. The
    /// commit fence does not use this check.
    #[must_use]
    pub fn is_lock_miss(msg: &str) -> bool {
        msg.contains("55P03")
            || msg.contains("lock timeout")
            || msg.contains("could not obtain lock")
    }

    fn lock_timeout_sql(wait: Duration) -> String {
        // Postgres reads 0 as no limit, so the bound is at least 1 ms.
        let ms = wait.as_millis().clamp(1, u128::from(u32::MAX));
        format!("SET LOCAL lock_timeout = '{ms}ms'")
    }

    fn partition_of(row: i32) -> HarvestResult<DecisionPartition> {
        u16::try_from(row)
            .ok()
            .and_then(DecisionPartition::new)
            .ok_or_else(|| HarvestError::Database(format!("owner row {row} is out of range")))
    }

    /// Take `partition` for `holder` when its row is free or its lease
    /// ended. Each acquisition adds 1 to the epoch.
    ///
    /// The statement runs in its own transaction. It takes the DR
    /// generation row first, then the owner row (ADR 0009 decision 6).
    ///
    /// # Errors
    ///
    /// [`HarvestError::ShardFenced`] when this process lost DR write
    /// authority. [`HarvestError::NotFound`] when the row does not exist.
    /// [`HarvestError::Database`] on a query failure or an epoch overflow.
    pub async fn acquire(
        conn: &mut AsyncPgConnection,
        shard: ShardId,
        holder: &OwnerHolder,
        partition: DecisionPartition,
        config: &OwnerLeaseConfig,
    ) -> HarvestResult<Acquisition> {
        let holder = holder.clone();
        let config = *config;
        let result = conn
            .transaction::<Acquisition, HarvestError, _>(async |conn| {
                diesel::sql_query(lock_timeout_sql(config.lock_timeout))
                    .execute(conn)
                    .await
                    .map_err(database_error)?;
                crate::replication::assert_generation_fence(conn, shard).await?;
                let p = i32::from(partition.get());
                let locked: Option<EpochRow> = diesel::sql_query(ACQUIRE_LOCK_SQL)
                    .bind::<Integer, _>(p)
                    .get_result(conn)
                    .await
                    .optional()
                    .map_err(database_error)?;
                let Some(locked) = locked else {
                    let exists: Option<EpochRow> = diesel::sql_query(ROW_EXISTS_SQL)
                        .bind::<Integer, _>(p)
                        .get_result(conn)
                        .await
                        .optional()
                        .map_err(database_error)?;
                    return match exists {
                        Some(_) => Ok(Acquisition::Held),
                        None => Err(HarvestError::NotFound(format!(
                            "no owner row for decision partition {partition}"
                        ))),
                    };
                };
                let next = next_epoch(locked.epoch).ok_or_else(|| {
                    HarvestError::Database(format!(
                        "the epoch of decision partition {partition} cannot grow"
                    ))
                })?;
                let written: EpochRow = diesel::sql_query(ACQUIRE_WRITE_SQL)
                    .bind::<Integer, _>(p)
                    .bind::<Text, _>(holder.as_str())
                    .bind::<BigInt, _>(next)
                    .bind::<Double, _>(config.ttl.get().as_secs_f64())
                    .bind::<BigInt, _>(locked.epoch)
                    .get_result(conn)
                    .await
                    .map_err(database_error)?;
                Ok(Acquisition::Acquired(OwnerTenure::pinned(
                    shard,
                    partition,
                    holder.clone(),
                    OwnerEpoch::new(written.epoch),
                    config.ttl,
                )))
            })
            .await;
        match result {
            Err(HarvestError::Database(msg)) if is_lock_miss(&msg) => Ok(Acquisition::Busy),
            other => other,
        }
    }

    /// The keys of `tenures`, as three parallel arrays.
    fn keys(tenures: &[OwnerTenure]) -> (Vec<i32>, Vec<i64>, Vec<String>) {
        let partitions = tenures
            .iter()
            .map(|t| i32::from(t.partition().get()))
            .collect();
        let epochs = tenures.iter().map(|t| t.epoch().get()).collect();
        let holders = tenures
            .iter()
            .map(|t| t.holder().as_str().to_string())
            .collect();
        (partitions, epochs, holders)
    }

    /// The write of a multi-row lease statement.
    #[derive(Clone, Copy)]
    enum RowsWrite {
        /// Extend each lease by its tenure's TTL.
        Renew,
        /// Free each row.
        Release,
    }

    /// Lock the rows of `tenures` that the statement matches, then write.
    ///
    /// The lock statement orders by partition, so two statements on the
    /// same rows lock them in one order. A plain multi-row `UPDATE` locks
    /// rows in scan order instead.
    async fn locked_write(
        conn: &mut AsyncPgConnection,
        shard: ShardId,
        tenures: &[OwnerTenure],
        lock_timeout: Duration,
        write: RowsWrite,
    ) -> HarvestResult<LeaseWrite> {
        let (partitions, epochs, holders) = keys(tenures);
        let ttls: Vec<f64> = tenures
            .iter()
            .map(|t| t.ttl().get().as_secs_f64())
            .collect();
        let lock_sql = match write {
            RowsWrite::Renew => RENEW_LOCK_SQL,
            RowsWrite::Release => RELEASE_LOCK_SQL,
        };
        let result = conn
            .transaction::<Vec<i32>, HarvestError, _>(async |conn| {
                diesel::sql_query(lock_timeout_sql(lock_timeout))
                    .execute(conn)
                    .await
                    .map_err(database_error)?;
                crate::replication::assert_generation_fence(conn, shard).await?;
                let locked: Vec<PartitionRow> = diesel::sql_query(lock_sql)
                    .bind::<Array<Integer>, _>(&partitions)
                    .bind::<Array<BigInt>, _>(&epochs)
                    .bind::<Array<Text>, _>(&holders)
                    .load(conn)
                    .await
                    .map_err(database_error)?;
                let locked: Vec<i32> = locked.into_iter().map(|r| r.partition).collect();
                if locked.is_empty() {
                    return Ok(locked);
                }
                match write {
                    RowsWrite::Renew => {
                        let locked_ttls: Vec<f64> = locked
                            .iter()
                            .map(|p| {
                                partitions
                                    .iter()
                                    .position(|q| q == p)
                                    .map_or(0.0, |at| ttls[at])
                            })
                            .collect();
                        diesel::sql_query(RENEW_WRITE_SQL)
                            .bind::<Array<Integer>, _>(&locked)
                            .bind::<Array<Double>, _>(&locked_ttls)
                            .execute(conn)
                            .await
                            .map_err(database_error)?;
                    }
                    RowsWrite::Release => {
                        diesel::sql_query(RELEASE_WRITE_SQL)
                            .bind::<Array<Integer>, _>(&locked)
                            .execute(conn)
                            .await
                            .map_err(database_error)?;
                    }
                }
                Ok(locked)
            })
            .await;
        let locked = match result {
            Err(HarvestError::Database(msg)) if is_lock_miss(&msg) => {
                return Ok(LeaseWrite::Busy);
            }
            other => other?,
        };
        let mut matched = Vec::with_capacity(locked.len());
        for p in &locked {
            matched.push(partition_of(*p)?);
        }
        let lost = tenures
            .iter()
            .map(OwnerTenure::partition)
            .filter(|p| !locked.contains(&i32::from(p.get())))
            .collect();
        Ok(LeaseWrite::done(matched, lost))
    }

    /// Renew each lease of `tenures` in one transaction.
    ///
    /// A renewal matches holder and epoch, and needs a live lease. It
    /// changes `lease_until` only, so it never changes an epoch. A lapsed
    /// holder acquires again, at a new epoch.
    ///
    /// # Errors
    ///
    /// [`HarvestError::ShardFenced`] when this process lost DR write
    /// authority. [`HarvestError::Database`] on a query failure.
    pub async fn renew_all(
        conn: &mut AsyncPgConnection,
        shard: ShardId,
        tenures: &[OwnerTenure],
        lock_timeout: Duration,
    ) -> HarvestResult<LeaseWrite> {
        locked_write(conn, shard, tenures, lock_timeout, RowsWrite::Renew).await
    }

    /// Release each row of `tenures` in one transaction (ADR 0009
    /// decision 9).
    ///
    /// A release matches holder and epoch. It ends the lease now and keeps
    /// the holder and the epoch, so any worker may acquire the row next. The
    /// holder stops new commits in each partition before it calls this.
    ///
    /// # Errors
    ///
    /// [`HarvestError::ShardFenced`] when this process lost DR write
    /// authority. [`HarvestError::Database`] on a query failure.
    pub async fn release_all(
        conn: &mut AsyncPgConnection,
        shard: ShardId,
        tenures: &[OwnerTenure],
        lock_timeout: Duration,
    ) -> HarvestResult<LeaseWrite> {
        locked_write(conn, shard, tenures, lock_timeout, RowsWrite::Release).await
    }

    /// End every live owner lease on this database (ADR 0009 decision 4).
    ///
    /// The DR bump calls this in its own transaction, after it takes the
    /// generation row. The holders and the epochs stay. Workers of the
    /// promoted region then acquire at once, at new epochs.
    ///
    /// Returns the number of leases that it ended.
    ///
    /// # Errors
    ///
    /// [`HarvestError::Database`] on a query failure.
    pub async fn expire_all(conn: &mut AsyncPgConnection) -> HarvestResult<usize> {
        let locked: Vec<PartitionRow> = diesel::sql_query(EXPIRE_LOCK_SQL)
            .load(conn)
            .await
            .map_err(database_error)?;
        let locked: Vec<i32> = locked.into_iter().map(|r| r.partition).collect();
        if locked.is_empty() {
            return Ok(0);
        }
        diesel::sql_query(EXPIRE_WRITE_SQL)
            .bind::<Array<Integer>, _>(&locked)
            .execute(conn)
            .await
            .map_err(database_error)
    }

    /// The commit fence: check `tenure` against its owner row.
    ///
    /// Call it inside the decision transaction, first after the DR
    /// generation check. The `FOR KEY SHARE` lock stays until the
    /// transaction ends, so no acquisition can commit before this
    /// transaction does. The fence also sets
    /// `idle_in_transaction_session_timeout` to the TTL for the
    /// transaction.
    ///
    /// The lock wait is at most 250 ms. Only an acquisition holds the row
    /// `FOR UPDATE`. On a live lease it holds the lock for one recheck only,
    /// so a healthy owner passes. A longer hold is a takeover, and the fence
    /// fails closed. A transaction that holds other row locks can call it
    /// too, because the wait is short and ends in a refusal.
    ///
    /// # Errors
    ///
    /// [`HarvestError::OwnershipLost`] when the row holds another holder
    /// or epoch, when the row is missing, or when the lock wait ends. The
    /// caller aborts the transaction and does not retry.
    /// [`HarvestError::Database`] on a query failure.
    /// The id of `conn` for [`super::fenced_scope`] (issue #2252).
    ///
    /// It is the address of the connection. The scope borrows the
    /// connection, so no other connection can take that address meanwhile.
    #[must_use]
    pub fn conn_id(conn: &AsyncPgConnection) -> usize {
        std::ptr::from_ref(conn).addr()
    }

    pub async fn fence(conn: &mut AsyncPgConnection, tenure: &OwnerTenure) -> HarvestResult<()> {
        let idle = format!("{}ms", tenure.ttl().get().as_millis());
        let lost = |current: Option<i64>| HarvestError::OwnershipLost {
            partition: tenure.partition(),
            pinned: tenure.epoch(),
            current: current.map(OwnerEpoch::new),
        };
        let row: Option<FenceRow> = diesel::sql_query(FENCE_SQL)
            .bind::<Integer, _>(i32::from(tenure.partition().get()))
            .bind::<Text, _>(idle)
            .get_result(conn)
            .await
            .optional()
            .map_err(database_error)?;
        let Some(row) = row else {
            return Err(lost(None));
        };
        let (true, Some(epoch)) = (row.locked, row.epoch) else {
            return Err(lost(None));
        };
        let holder_matches = row.holder.as_deref() == Some(tenure.holder().as_str());
        if fence_passes(tenure.epoch().get(), epoch, holder_matches) {
            Ok(())
        } else {
            Err(lost(Some(epoch)))
        }
    }

    /// Fence the tenure that the current decision pinned, if any.
    ///
    /// Outside a decision, or with no tenure pinned, it does nothing and
    /// sends no query.
    ///
    /// # Errors
    ///
    /// As [`fence`].
    pub async fn fence_pinned(conn: &mut AsyncPgConnection) -> HarvestResult<()> {
        match pinned_decision() {
            Some(tenure) => fence(conn, &tenure).await,
            None => Ok(()),
        }
    }

    /// Widen the idle bound of a pinned decision for an offload upload.
    ///
    /// Some callers upload a payload inside their decision transaction. The
    /// fence bounds an idle transaction by the TTL, so a slow upload would
    /// end the session. This call sets the bound to
    /// [`super::OwnerLeaseTtl::MAX`] until [`end_upload_allowance`]. With
    /// no tenure pinned, it sends no query.
    ///
    /// # Errors
    ///
    /// [`HarvestError::Database`] on a query failure.
    pub async fn start_upload_allowance(conn: &mut AsyncPgConnection) -> HarvestResult<()> {
        if pinned_decision().is_none() {
            return Ok(());
        }
        let wide = format!("{}ms", super::OwnerLeaseTtl::MAX.as_millis());
        set_idle(conn, wide).await
    }

    /// Restore the TTL idle bound after [`start_upload_allowance`].
    ///
    /// # Errors
    ///
    /// [`HarvestError::Database`] on a query failure.
    pub async fn end_upload_allowance(conn: &mut AsyncPgConnection) -> HarvestResult<()> {
        let Some(tenure) = pinned_decision() else {
            return Ok(());
        };
        set_idle(conn, format!("{}ms", tenure.ttl().get().as_millis())).await
    }

    async fn set_idle(conn: &mut AsyncPgConnection, value: String) -> HarvestResult<()> {
        diesel::sql_query(IDLE_SQL)
            .bind::<Text, _>(value)
            .get_result::<SettingRow>(conn)
            .await
            .map(|_| ())
            .map_err(database_error)
    }

    /// Read the switch row and whether the claim guard is on.
    const MODE_SQL: &str = "SELECT m.enabled, m.partitions, \
             EXISTS (SELECT 1 FROM pg_trigger t \
                     WHERE t.tgname = 'harvest_task_queue_owner_claim_guard_trg' \
                       AND t.tgrelid = 'harvest_task_queue'::regclass \
                       AND t.tgenabled <> 'D') AS guard_installed \
         FROM harvest_decision_owner_mode m WHERE m.singleton";

    /// Lock the switch row, so enable and disable run one at a time.
    const LOCK_MODE_SQL: &str =
        "SELECT enabled FROM harvest_decision_owner_mode WHERE singleton FOR UPDATE";

    /// Live workers whose labels lack the owner-mode capability.
    const LEGACY_WORKERS_SQL: &str = "SELECT worker_id FROM harvest_workers \
         WHERE status <> 'Stopped' \
           AND last_heartbeat_at > NOW() - make_interval(secs => $1) \
           AND COALESCE((labels->>'decision_owner_version')::int, 0) < $2 \
         ORDER BY worker_id";

    /// Live workers whose queues or build differ from each other.
    ///
    /// An owner claims every workflow task of its partitions. A partition of
    /// a worker that does not poll a queue, or cannot run a build, would
    /// strand those tasks. So owner mode needs one queue set and one build.
    /// [`MIXED_WORKFLOWS_SQL`] checks the workflow types.
    const MIXED_FLEET_SQL: &str = "SELECT count(DISTINCT (queues, build_id))::bigint AS n \
         FROM harvest_workers \
         WHERE status <> 'Stopped' \
           AND last_heartbeat_at > NOW() - make_interval(secs => $1)";

    /// The number of workflow type sets that live owners advertise.
    ///
    /// A worker that owns no partition is skipped. A missing label counts as
    /// its own set, so an unknown worker never passes as a match.
    const MIXED_WORKFLOWS_SQL: &str = "SELECT count(DISTINCT \
             COALESCE(labels->>'decision_owner_workflows', ''))::bigint AS n \
         FROM harvest_workers \
         WHERE status <> 'Stopped' \
           AND last_heartbeat_at > NOW() - make_interval(secs => $1) \
           AND COALESCE(labels->>'decision_owner_workflows', '') <> 'none'";

    /// Workflow-task rows with no stamp, and their workflow ids.
    const UNSTAMPED_SQL: &str = "SELECT t.id, e.workflow_id FROM harvest_task_queue t \
         JOIN harvest_workflow_executions e ON e.id = t.workflow_exec_id \
         WHERE t.task_type = 'workflow' \
           AND t.decision_partition IS NULL \
           AND t.state IN ('PENDING', 'RUNNING') \
         LIMIT $1";

    /// Stamp a batch of rows. A row stamped since the read keeps its stamp.
    const STAMP_SQL: &str = "UPDATE harvest_task_queue t SET decision_partition = u.p \
         FROM unnest($1::uuid[], $2::int2[]) AS u(id, p) \
         WHERE t.id = u.id AND t.decision_partition IS NULL";

    /// Install the claim guard, or turn it on again (issue #2252).
    ///
    /// Its `WHEN` clause runs the function only for the claim of a workflow
    /// task. `CREATE TRIGGER` and `ENABLE TRIGGER` take `SHARE ROW
    /// EXCLUSIVE`, which does not block a reader of the table.
    const INSTALL_GUARD_SQL: &str = "DO $$ BEGIN \
             IF NOT EXISTS (SELECT 1 FROM pg_trigger \
                            WHERE tgname = 'harvest_task_queue_owner_claim_guard_trg' \
                              AND tgrelid = 'harvest_task_queue'::regclass) THEN \
                 CREATE TRIGGER harvest_task_queue_owner_claim_guard_trg \
                     BEFORE UPDATE ON harvest_task_queue \
                     FOR EACH ROW \
                     WHEN (OLD.state = 'PENDING' AND NEW.state = 'RUNNING' \
                           AND NEW.task_type = 'workflow') \
                     EXECUTE FUNCTION harvest_task_queue_owner_claim_guard(); \
             END IF; \
         END $$; \
         ALTER TABLE harvest_task_queue ENABLE TRIGGER harvest_task_queue_owner_claim_guard_trg";

    /// Turn the claim guard off. `DISABLE TRIGGER` takes `SHARE ROW
    /// EXCLUSIVE`. A `DROP TRIGGER` would take `ACCESS EXCLUSIVE`.
    const DISABLE_GUARD_SQL: &str = "DO $$ BEGIN \
             IF EXISTS (SELECT 1 FROM pg_trigger \
                        WHERE tgname = 'harvest_task_queue_owner_claim_guard_trg' \
                          AND tgrelid = 'harvest_task_queue'::regclass) THEN \
                 ALTER TABLE harvest_task_queue \
                     DISABLE TRIGGER harvest_task_queue_owner_claim_guard_trg; \
             END IF; \
         END $$";

    /// Re-pend each legacy workflow claim of a worker that is not live.
    ///
    /// No owner exists while the switch is off, so each `RUNNING` claim is a
    /// legacy claim. A live worker finishes its own decision and parks the
    /// task. Then an owner claims it. A dead worker never parks it, so this
    /// statement re-pends it.
    const REPEND_DEAD_LEGACY_SQL: &str = "UPDATE harvest_task_queue t \
         SET state = 'PENDING', worker_id = NULL, started_at = NULL, \
             last_heartbeat_at = NULL, scheduled_at = NOW(), \
             sticky_worker_id = NULL, sticky_until = NULL, sticky_timeout = NULL, \
             wake_requested = FALSE \
         WHERE t.task_type = 'workflow' AND t.state = 'RUNNING' AND t.worker_id IS NOT NULL \
           AND NOT EXISTS (SELECT 1 FROM harvest_workers w \
                           WHERE w.worker_id = t.worker_id \
                             AND w.status <> 'Stopped' \
                             AND w.last_heartbeat_at > NOW() - make_interval(secs => $1))";

    const SET_MODE_SQL: &str = "UPDATE harvest_decision_owner_mode \
         SET enabled = $1, changed_at = NOW() WHERE singleton";

    /// Record this loop as a live owner candidate.
    const UPSERT_CANDIDATE_SQL: &str = "INSERT INTO harvest_decision_owner_candidates \
             (holder, seen_at) VALUES ($1, clock_timestamp()) \
         ON CONFLICT (holder) DO UPDATE SET seen_at = EXCLUDED.seen_at";

    /// Forget this loop as a candidate, and every long-dead one.
    const DELETE_CANDIDATE_SQL: &str = "DELETE FROM harvest_decision_owner_candidates \
         WHERE holder = $1 OR seen_at < clock_timestamp() - interval '10 minutes'";

    /// The live candidates of this database.
    const LIVE_CANDIDATES_SQL: &str = "SELECT holder FROM harvest_decision_owner_candidates \
         WHERE seen_at > clock_timestamp() - make_interval(secs => $1)";

    /// Free partitions, and the free partitions whose lease ended before
    /// `$1` seconds ago.
    const FREE_SQL: &str = "SELECT count(*)::bigint AS free, \
             count(*) FILTER (WHERE lease_until <= clock_timestamp() - make_interval(secs => $1))::bigint \
                 AS orphans \
         FROM harvest_decision_owner \
         WHERE holder IS NULL OR lease_until <= clock_timestamp()";

    /// Re-pend each workflow task that a dead worker holds `RUNNING` in a
    /// newly acquired partition (ADR 0009 decision 7).
    ///
    /// It runs after the acquisitions commit. A claim of the old owner that
    /// commits later fails the guard, because the holder changed. A live
    /// worker keeps its claim. A live old owner's decision fails its fence
    /// and re-pends its own claim. A live legacy worker finishes its decision
    /// and parks the task. Only a dead worker never releases its claim, so
    /// this statement re-pends it. The loop runs it before the worker adopts
    /// the tenures. The rows lock in id order, so two such statements cannot
    /// deadlock.
    const REPEND_FOREIGN_SQL: &str = "UPDATE harvest_task_queue \
         SET state = 'PENDING', worker_id = NULL, started_at = NULL, \
             last_heartbeat_at = NULL, scheduled_at = NOW(), \
             sticky_worker_id = NULL, sticky_until = NULL, sticky_timeout = NULL, \
             wake_requested = FALSE \
         WHERE id IN (SELECT t.id FROM harvest_task_queue t \
                      WHERE t.task_type = 'workflow' AND t.state = 'RUNNING' \
                        AND t.worker_id IS NOT NULL \
                        AND t.decision_partition = ANY($1::int2[]) \
                        AND NOT EXISTS ( \
                            SELECT 1 FROM harvest_workers w \
                            WHERE w.worker_id = t.worker_id \
                              AND w.status <> 'Stopped' \
                              AND w.last_heartbeat_at > NOW() - make_interval(secs => $2)) \
                      ORDER BY t.id FOR UPDATE)";

    /// Free partitions to try, in random order, so that peers spread.
    const FREE_PICK_SQL: &str = "SELECT partition FROM harvest_decision_owner \
         WHERE holder IS NULL OR lease_until <= clock_timestamp() \
         ORDER BY random() LIMIT $1";

    /// How long a candidate row counts as live: three scans.
    const CANDIDATE_WINDOW: Duration = Duration::from_secs(3);

    #[derive(QueryableByName)]
    struct ModeRow {
        #[diesel(sql_type = Bool)]
        enabled: bool,
        #[diesel(sql_type = Integer)]
        partitions: i32,
        #[diesel(sql_type = Bool)]
        guard_installed: bool,
    }

    #[derive(QueryableByName)]
    struct EnabledRow {
        #[diesel(sql_type = Bool)]
        #[allow(dead_code)]
        enabled: bool,
    }

    #[derive(QueryableByName)]
    struct WorkerRow {
        #[diesel(sql_type = Text)]
        worker_id: String,
    }

    #[derive(QueryableByName)]
    struct HolderRow {
        #[diesel(sql_type = Text)]
        holder: String,
    }

    #[derive(QueryableByName)]
    struct CountRow {
        #[diesel(sql_type = BigInt)]
        n: i64,
    }

    #[derive(QueryableByName)]
    struct UnstampedRow {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: uuid::Uuid,
        #[diesel(sql_type = Text)]
        workflow_id: String,
    }

    #[derive(QueryableByName)]
    struct FreeRow {
        #[diesel(sql_type = BigInt)]
        free: i64,
        #[diesel(sql_type = BigInt)]
        orphans: i64,
    }

    /// Read the owner-mode switch of the database of `conn` (issue #2252).
    ///
    /// # Errors
    ///
    /// [`HarvestError::Database`] on a query failure, or when the switch row
    /// is missing.
    pub async fn read_owner_mode(conn: &mut AsyncPgConnection) -> HarvestResult<OwnerMode> {
        let row: ModeRow = diesel::sql_query(MODE_SQL)
            .get_result(conn)
            .await
            .map_err(database_error)?;
        Ok(OwnerMode {
            enabled: row.enabled,
            partitions: row.partitions,
            guard_installed: row.guard_installed,
        })
    }

    const fn secs(d: Duration) -> f64 {
        d.as_secs_f64()
    }

    /// Turn owner mode on for the database of `conn` (ADR 0009 decision 11).
    ///
    /// 1. Refuse unless `force` is set. Three cases refuse: a live worker
    ///    cannot run owner mode, live workers differ in queues or build, or
    ///    no owner loop is live. These checks are a courtesy. The guard is
    ///    the structural part.
    /// 2. Stamp the partition of each live workflow task.
    /// 3. Run one transaction. It locks the switch row and installs the
    ///    claim guard. It re-pends each legacy claim of a dead worker, and
    ///    sets the switch. A live worker finishes its own legacy decision.
    /// 4. Stamp the rows that arrived since step 2.
    ///
    /// Step 3 is atomic. A failure leaves owner mode off and no guard on.
    ///
    /// # Errors
    ///
    /// [`HarvestError::Config`] when a check refuses, or when the database
    /// has another partition count. [`HarvestError::Database`] on a query
    /// failure.
    pub async fn enable_owner_mode(
        conn: &mut AsyncPgConnection,
        options: &EnableOwnerMode,
    ) -> HarvestResult<EnableReport> {
        let mode = read_owner_mode(conn).await?;
        if !mode.count_matches() {
            return Err(HarvestError::Config(format!(
                "this database has {} decision partitions; this binary needs {DECISION_PARTITIONS}",
                mode.partitions
            )));
        }
        if mode.enabled && mode.guard_installed {
            return Ok(EnableReport {
                already_on: true,
                ..EnableReport::default()
            });
        }
        let legacy = check_fleet(conn, options).await?;
        let mut stamped = stamp_unstamped(conn, options).await?;
        let lock = lock_timeout_sql(options.lock_timeout);
        let live = secs(options.live_within);
        let repended = conn
            .transaction::<usize, HarvestError, _>(async |conn| {
                diesel::sql_query(lock)
                    .execute(conn)
                    .await
                    .map_err(database_error)?;
                diesel::sql_query(LOCK_MODE_SQL)
                    .get_result::<EnabledRow>(conn)
                    .await
                    .map_err(database_error)?;
                conn.batch_execute(INSTALL_GUARD_SQL)
                    .await
                    .map_err(database_error)?;
                let repended = diesel::sql_query(REPEND_DEAD_LEGACY_SQL)
                    .bind::<Double, _>(live)
                    .execute(conn)
                    .await
                    .map_err(database_error)?;
                diesel::sql_query(SET_MODE_SQL)
                    .bind::<Bool, _>(true)
                    .execute(conn)
                    .await
                    .map_err(database_error)?;
                Ok(repended)
            })
            .await?;
        stamped += stamp_unstamped(conn, options).await?;
        Ok(EnableReport {
            already_on: false,
            stamped,
            repended: u64::try_from(repended).unwrap_or(u64::MAX),
            legacy_workers: legacy,
        })
    }

    /// The courtesy checks of [`enable_owner_mode`]. Returns the live
    /// workers that cannot run owner mode, when `force` lets them pass.
    async fn check_fleet(
        conn: &mut AsyncPgConnection,
        options: &EnableOwnerMode,
    ) -> HarvestResult<Vec<String>> {
        let live = secs(options.live_within);
        let legacy: Vec<WorkerRow> = diesel::sql_query(LEGACY_WORKERS_SQL)
            .bind::<Double, _>(live)
            .bind::<Integer, _>(i32::try_from(OWNER_MODE_CAPABILITY_VERSION).unwrap_or(i32::MAX))
            .load(conn)
            .await
            .map_err(database_error)?;
        let legacy: Vec<String> = legacy.into_iter().map(|w| w.worker_id).collect();
        if options.force {
            return Ok(legacy);
        }
        if !legacy.is_empty() {
            return Err(HarvestError::Config(format!(
                "owner mode refused: live workers cannot run it: {}",
                legacy.join(", ")
            )));
        }
        let mixed: CountRow = diesel::sql_query(MIXED_FLEET_SQL)
            .bind::<Double, _>(live)
            .get_result(conn)
            .await
            .map_err(database_error)?;
        if mixed.n > 1 {
            return Err(HarvestError::Config(
                "owner mode refused: live workers poll different queues or run different \
                 builds, so an owner could strand a workflow task"
                    .to_string(),
            ));
        }
        let types: CountRow = diesel::sql_query(MIXED_WORKFLOWS_SQL)
            .bind::<Double, _>(live)
            .get_result(conn)
            .await
            .map_err(database_error)?;
        if types.n > 1 {
            return Err(HarvestError::Config(
                "owner mode refused: live workers register different workflow types, so an \
                 owner could hold a workflow task that it cannot run"
                    .to_string(),
            ));
        }
        let candidates: Vec<HolderRow> = diesel::sql_query(LIVE_CANDIDATES_SQL)
            .bind::<Double, _>(secs(CANDIDATE_WINDOW))
            .load(conn)
            .await
            .map_err(database_error)?;
        if candidates.is_empty() {
            return Err(HarvestError::Config(
                "owner mode refused: no owner loop is live on this database, so no worker \
                 would claim a workflow task"
                    .to_string(),
            ));
        }
        Ok(legacy)
    }

    /// Stamp each live workflow-task row that has no partition.
    async fn stamp_unstamped(
        conn: &mut AsyncPgConnection,
        options: &EnableOwnerMode,
    ) -> HarvestResult<u64> {
        let mut stamped = 0u64;
        loop {
            let rows: Vec<UnstampedRow> = diesel::sql_query(UNSTAMPED_SQL)
                .bind::<BigInt, _>(options.batch.max(1))
                .load(conn)
                .await
                .map_err(database_error)?;
            if rows.is_empty() {
                return Ok(stamped);
            }
            let ids: Vec<uuid::Uuid> = rows.iter().map(|r| r.id).collect();
            let parts: Vec<i16> = rows
                .iter()
                .map(|r| DecisionPartition::for_workflow_id(&r.workflow_id).as_i16())
                .collect();
            let lock = lock_timeout_sql(options.lock_timeout);
            let n = conn
                .transaction::<usize, HarvestError, _>(async |conn| {
                    diesel::sql_query(lock)
                        .execute(conn)
                        .await
                        .map_err(database_error)?;
                    diesel::sql_query(STAMP_SQL)
                        .bind::<Array<diesel::sql_types::Uuid>, _>(&ids)
                        .bind::<Array<SmallInt>, _>(&parts)
                        .execute(conn)
                        .await
                        .map_err(database_error)
                })
                .await?;
            stamped += u64::try_from(n).unwrap_or(0);
            if i64::try_from(rows.len()).unwrap_or(i64::MAX) < options.batch.max(1) {
                return Ok(stamped);
            }
        }
    }

    /// Turn owner mode off for the database of `conn` (issue #2252).
    ///
    /// One transaction locks the switch row, clears it and turns the guard
    /// off. Owners see the switch on their next scan, release their
    /// partitions and claim as before. An owned claim selects no workflow
    /// task with the switch off, so the two claim forms never take the same
    /// task.
    ///
    /// # Errors
    ///
    /// [`HarvestError::Database`] on a query failure.
    pub async fn disable_owner_mode(
        conn: &mut AsyncPgConnection,
        lock_timeout: Duration,
    ) -> HarvestResult<()> {
        let lock = lock_timeout_sql(lock_timeout);
        conn.transaction::<(), HarvestError, _>(async |conn| {
            diesel::sql_query(lock)
                .execute(conn)
                .await
                .map_err(database_error)?;
            diesel::sql_query(LOCK_MODE_SQL)
                .get_result::<EnabledRow>(conn)
                .await
                .map_err(database_error)?;
            diesel::sql_query(SET_MODE_SQL)
                .bind::<Bool, _>(false)
                .execute(conn)
                .await
                .map_err(database_error)?;
            conn.batch_execute(DISABLE_GUARD_SQL)
                .await
                .map_err(database_error)?;
            Ok(())
        })
        .await
    }

    /// What one [`OwnerLoop::tick`] changed.
    #[derive(Debug, Default)]
    pub struct OwnerTick {
        /// Owner mode is on for the database.
        pub owner_mode: bool,
        /// Tenures that this tick dropped: lost in a renewal, past their
        /// local deadline, released, or dropped because the switch is off.
        /// The worker drops their cache entries.
        pub dropped: Vec<super::OwnerKey>,
        /// Tenures that this tick acquired, with their local deadlines. The
        /// worker drops their cache entries first, then adopts them, so a
        /// new tenure starts cold.
        pub acquired: Vec<(OwnerTenure, std::time::Instant)>,
    }

    /// The acquisition loop of one worker on one database (issue #2252).
    ///
    /// The worker calls [`Self::tick`] about once a second. Each tick records
    /// the loop as a live candidate and reads the switch. With the switch
    /// on, it renews when a renewal is due, drops each tenure past its local
    /// deadline, and balances by [`super::plan_balance`].
    ///
    /// A refused loop is a participant, not an owner. It turns the owned
    /// claim on with the switch, so it claims activities and unstamped
    /// rows. It is never a candidate and acquires nothing.
    #[derive(Debug)]
    pub struct OwnerLoop {
        shard: ShardId,
        holder: OwnerHolder,
        config: OwnerLeaseConfig,
        owned: std::sync::Arc<super::OwnedPartitions>,
        last_renewal: Option<std::time::Instant>,
        refused: bool,
        /// This loop turned owner mode on. Only then does it turn owner mode
        /// off again and release, so it never clears state that it did not
        /// set.
        active: bool,
        /// The loop stopped for good. A later tick does nothing.
        stopped: bool,
        /// Tenures that this loop stopped using, released on the next tick.
        /// The delay lets their decisions in flight commit first.
        releasing: Vec<OwnerTenure>,
        /// A worker with a heartbeat this recent is live.
        live_within: Duration,
    }

    impl OwnerLoop {
        /// A loop for `worker_id` on the database of `shard`.
        ///
        /// The loop sets the holder of `owned` when it has none.
        #[must_use]
        pub fn new(
            shard: ShardId,
            worker_id: &str,
            config: OwnerLeaseConfig,
            owned: std::sync::Arc<super::OwnedPartitions>,
        ) -> Self {
            let holder = owned.holder().unwrap_or_else(|| {
                let holder = OwnerHolder::for_worker(worker_id);
                owned.set_holder(holder.clone());
                holder
            });
            Self {
                shard,
                holder,
                config,
                owned,
                last_renewal: None,
                refused: false,
                active: false,
                stopped: false,
                releasing: Vec::new(),
                live_within: Duration::from_secs(30),
            }
        }

        /// This loop with `live_within`: a worker with a heartbeat this recent
        /// is live, so the loop never re-pends its claim.
        #[must_use]
        pub const fn with_live_within(mut self, live_within: Duration) -> Self {
            self.live_within = live_within;
            self
        }

        /// The shard that keys this loop's tenures.
        #[must_use]
        pub const fn shard(&self) -> ShardId {
            self.shard
        }

        /// Make this loop a participant, not an owner.
        ///
        /// A worker that cannot run workflow tasks, or whose shard
        /// assignments split a colocated group, must not own partitions
        /// (ADR 0009 decision 10).
        pub const fn refuse(&mut self) {
            self.refused = true;
        }

        /// Drop every tenure of this loop's shard locally.
        fn drop_all(&self, tick: &mut OwnerTick) {
            for tenure in self.owned.tenures_of(self.shard) {
                if self.owned.unload(tenure.key(), tenure.epoch()) {
                    tick.dropped.push(tenure.key());
                }
            }
        }

        /// One scan of the database of `conn`.
        ///
        /// # Errors
        ///
        /// [`HarvestError::Database`] on a query failure. The worker logs it
        /// and tries again on the next scan. A tenure keeps its local
        /// deadline, so a failing database stops the claims in time.
        pub async fn tick(&mut self, conn: &mut AsyncPgConnection) -> HarvestResult<OwnerTick> {
            let mut tick = OwnerTick::default();
            if self.stopped {
                return Ok(tick);
            }
            if !self.refused {
                diesel::sql_query(UPSERT_CANDIDATE_SQL)
                    .bind::<Text, _>(self.holder.as_str())
                    .execute(conn)
                    .await
                    .map_err(database_error)?;
            }
            let mode = read_owner_mode(conn).await?;
            let on = mode.enabled && mode.guard_installed && mode.count_matches();
            if !on {
                if self.active {
                    self.deactivate(conn, &mut tick).await;
                }
                return Ok(tick);
            }
            self.active = true;
            self.owned.set_owner_mode(self.shard, true);
            self.owned.set_owned_claims(self.shard, true);
            tick.owner_mode = true;
            if self.refused {
                return Ok(tick);
            }
            self.release_pending(conn).await;
            self.renew_if_due(conn, &mut tick).await?;
            self.drop_expired(&mut tick);
            self.balance(conn, &mut tick).await?;
            Ok(tick)
        }

        /// Owner mode went off: claim and decide as before.
        async fn deactivate(&mut self, conn: &mut AsyncPgConnection, tick: &mut OwnerTick) {
            self.active = false;
            self.owned.set_owned_claims(self.shard, false);
            self.owned.set_owner_mode(self.shard, false);
            let mut held = self.owned.tenures_of(self.shard);
            self.drop_all(tick);
            held.append(&mut self.releasing);
            if !held.is_empty() {
                let _ = release_all(conn, self.shard, &held, self.config.lock_timeout).await;
            }
        }

        /// Release the tenures that the last tick stopped using.
        async fn release_pending(&mut self, conn: &mut AsyncPgConnection) {
            if self.releasing.is_empty() {
                return;
            }
            let releasing = std::mem::take(&mut self.releasing);
            // A failed release lapses within one TTL. Peers then acquire.
            let _ = release_all(conn, self.shard, &releasing, self.config.lock_timeout).await;
        }

        async fn renew_if_due(
            &mut self,
            conn: &mut AsyncPgConnection,
            tick: &mut OwnerTick,
        ) -> HarvestResult<()> {
            let now = std::time::Instant::now();
            let interval = self.config.ttl.renew_interval();
            if self
                .last_renewal
                .is_some_and(|last| now.duration_since(last) < interval)
            {
                return Ok(());
            }
            let held = self.owned.tenures_of(self.shard);
            if held.is_empty() {
                self.last_renewal = Some(now);
                return Ok(());
            }
            let sent = std::time::Instant::now();
            match renew_all(conn, self.shard, &held, self.config.lock_timeout).await {
                Ok(LeaseWrite::Done(matched)) => {
                    self.last_renewal = Some(sent);
                    let renewed: Vec<OwnerTenure> = held
                        .iter()
                        .filter(|t| matched.matched.contains(&t.partition()))
                        .cloned()
                        .collect();
                    self.owned
                        .extend(&renewed, self.config.ttl.local_deadline(sent));
                    for tenure in held
                        .iter()
                        .filter(|t| matched.lost.contains(&t.partition()))
                    {
                        if self.owned.unload(tenure.key(), tenure.epoch()) {
                            tick.dropped.push(tenure.key());
                        }
                    }
                    Ok(())
                }
                // Nothing changed. The deadline stops the claims if this
                // keeps failing.
                Ok(LeaseWrite::Busy) => Ok(()),
                Err(HarvestError::ShardFenced { .. }) => {
                    self.drop_all(tick);
                    Ok(())
                }
                Err(e) => Err(e),
            }
        }

        fn drop_expired(&self, tick: &mut OwnerTick) {
            for tenure in self.owned.tenures_of(self.shard) {
                if matches!(
                    self.owned.pin_for(tenure.key()),
                    super::DecisionPin::Unowned
                ) && self.owned.unload(tenure.key(), tenure.epoch())
                {
                    tick.dropped.push(tenure.key());
                }
            }
        }

        async fn balance(
            &mut self,
            conn: &mut AsyncPgConnection,
            tick: &mut OwnerTick,
        ) -> HarvestResult<()> {
            let candidates: Vec<HolderRow> = diesel::sql_query(LIVE_CANDIDATES_SQL)
                .bind::<Double, _>(secs(CANDIDATE_WINDOW))
                .load(conn)
                .await
                .map_err(database_error)?;
            let peers = candidates
                .iter()
                .filter(|c| c.holder != self.holder.as_str())
                .count();
            let free: FreeRow = diesel::sql_query(FREE_SQL)
                .bind::<Double, _>(self.config.ttl.renew_interval().as_secs_f64())
                .get_result(conn)
                .await
                .map_err(database_error)?;
            let held = self.owned.tenures_of(self.shard);
            let plan = super::plan_balance(
                held.len(),
                peers,
                usize::try_from(free.free).unwrap_or(0),
                usize::try_from(free.orphans).unwrap_or(0),
            );
            if plan.release > 0 {
                self.stop_using(held, plan.release, tick);
            }
            if plan.acquire > 0 {
                self.acquire_some(conn, plan.acquire, tick).await?;
            }
            Ok(())
        }

        /// Stop claiming `count` held partitions, highest first. The next
        /// tick releases them, so their decisions in flight can commit
        /// (ADR 0009 decision 9).
        fn stop_using(&mut self, mut held: Vec<OwnerTenure>, count: usize, tick: &mut OwnerTick) {
            let keep = held.len().saturating_sub(count);
            for tenure in held.split_off(keep) {
                if self.owned.unload(tenure.key(), tenure.epoch()) {
                    tick.dropped.push(tenure.key());
                    self.releasing.push(tenure);
                }
            }
        }

        async fn acquire_some(
            &self,
            conn: &mut AsyncPgConnection,
            count: usize,
            tick: &mut OwnerTick,
        ) -> HarvestResult<()> {
            let picks: Vec<PartitionRow> = diesel::sql_query(FREE_PICK_SQL)
                .bind::<BigInt, _>(i64::try_from(count).unwrap_or(i64::MAX))
                .load(conn)
                .await
                .map_err(database_error)?;
            let mut acquired = Vec::new();
            for pick in picks {
                let partition = partition_of(pick.partition)?;
                let sent = std::time::Instant::now();
                match acquire(conn, self.shard, &self.holder, partition, &self.config).await {
                    Ok(Acquisition::Acquired(tenure)) => {
                        let deadline = tenure.ttl().local_deadline(sent);
                        acquired.push((tenure, deadline));
                    }
                    Ok(Acquisition::Held | Acquisition::Busy) => {}
                    Err(HarvestError::ShardFenced { .. }) => break,
                    Err(e) => return Err(e),
                }
            }
            if acquired.is_empty() {
                return Ok(());
            }
            // The worker has not adopted these tenures yet, so no claim of
            // its own is in them. The re-pend touches foreign claims only.
            let taken: Vec<i16> = acquired
                .iter()
                .map(|(t, _)| t.partition().as_i16())
                .collect();
            let repended = diesel::sql_query(REPEND_FOREIGN_SQL)
                .bind::<Array<SmallInt>, _>(&taken)
                .bind::<Double, _>(secs(self.live_within))
                .execute(conn)
                .await;
            if let Err(error) = repended {
                // A tenure must not serve claims before its foreign claims
                // are re-pended. Give the partitions back. A later scan
                // acquires them again, at a new epoch.
                tracing::warn!(
                    holder = %self.holder,
                    error = %error,
                    "failed to re-pend the workflow tasks of acquired partitions; releasing them"
                );
                let tenures: Vec<OwnerTenure> = acquired.into_iter().map(|(t, _)| t).collect();
                let _ = release_all(conn, self.shard, &tenures, self.config.lock_timeout).await;
                return Ok(());
            }
            tick.acquired.extend(acquired);
            Ok(())
        }

        /// Stop for good and release every partition of this loop's shard
        /// (ADR 0009 decision 9). The worker calls it once its in-flight
        /// decisions end. A later tick does nothing.
        ///
        /// # Errors
        ///
        /// [`HarvestError::Database`] on a query failure. A lease that a
        /// failed release leaves lapses within one TTL.
        pub async fn release_everything(
            &mut self,
            conn: &mut AsyncPgConnection,
        ) -> HarvestResult<Vec<super::OwnerKey>> {
            self.stopped = true;
            if !self.refused {
                let _ = diesel::sql_query(DELETE_CANDIDATE_SQL)
                    .bind::<Text, _>(self.holder.as_str())
                    .execute(conn)
                    .await;
            }
            if !self.active {
                return Ok(Vec::new());
            }
            self.active = false;
            self.owned.set_owned_claims(self.shard, false);
            let mut held = self.owned.tenures_of(self.shard);
            let mut tick = OwnerTick::default();
            self.drop_all(&mut tick);
            self.owned.set_owner_mode(self.shard, false);
            held.append(&mut self.releasing);
            if !held.is_empty() {
                release_all(conn, self.shard, &held, self.config.lock_timeout).await?;
            }
            Ok(tick.dropped)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tenure(p: u16, epoch: i64) -> OwnerTenure {
        OwnerTenure::pinned(
            ShardId::new(0),
            DecisionPartition::new(p).expect("in range"),
            OwnerHolder::for_worker("w"),
            OwnerEpoch::new(epoch),
            OwnerLeaseTtl::DEFAULT,
        )
    }

    /// A fenced scope covers one connection and one tenure, and only inside
    /// the scope (issue #2252).
    #[tokio::test]
    async fn a_fenced_scope_covers_one_connection_and_one_tenure() {
        let held = tenure(4, 7);
        let other = tenure(4, 8);
        fenced_scope(1, async {
            assert!(!already_fenced(1, &held), "no pin, no scope");
        })
        .await;
        scope_decision(async {
            assert!(pin_decision(Some(held.clone())));
            assert!(!already_fenced(1, &held), "no scope yet");
            fenced_scope(1, async {
                assert!(already_fenced(1, &held));
                assert!(!already_fenced(2, &held), "another connection fences");
                assert!(!already_fenced(1, &other), "another tenure fences");
                let spawned = tokio::spawn({
                    let held = held.clone();
                    async move { already_fenced(1, &held) }
                });
                assert!(!spawned.await.expect("join"), "a spawned task fences");
            })
            .await;
            assert!(!already_fenced(1, &held), "the scope ends with its body");
        })
        .await;
    }

    #[test]
    fn a_partition_is_below_the_count() {
        assert_eq!(
            DecisionPartition::new(0).map(DecisionPartition::get),
            Some(0)
        );
        assert_eq!(
            DecisionPartition::new(511).map(DecisionPartition::get),
            Some(511)
        );
        assert_eq!(DecisionPartition::new(512), None);
        assert_eq!(DecisionPartition::new(u16::MAX), None);
    }

    /// The hash must never change. A change would move every execution to
    /// another partition.
    #[test]
    fn the_partition_hash_is_stable() {
        for (id, golden) in [
            ("", 252),
            ("order-1", 449),
            ("order-2", 147),
            ("wf-0", 331),
            ("a", 418),
        ] {
            assert_eq!(
                DecisionPartition::for_workflow_id(id).get(),
                golden,
                "{id:?}"
            );
        }
    }

    #[test]
    fn the_partition_hash_spreads_ids() {
        let mut seen = std::collections::BTreeSet::new();
        for n in 0..4096 {
            let p = DecisionPartition::for_workflow_id(&format!("wf-{n}"));
            assert!(p.get() < DECISION_PARTITIONS);
            seen.insert(p);
        }
        assert!(seen.len() > 500, "4096 ids reach {} partitions", seen.len());
    }

    #[test]
    fn next_epoch_adds_one_and_never_wraps() {
        assert_eq!(next_epoch(0), Some(1));
        assert_eq!(next_epoch(41), Some(42));
        assert_eq!(next_epoch(i64::MAX - 1), Some(i64::MAX));
        assert_eq!(next_epoch(i64::MAX), None);
        assert_eq!(OwnerEpoch::new(i64::MAX).next(), None);
    }

    #[test]
    fn the_fence_passes_only_the_pinned_epoch_of_the_holder() {
        assert!(fence_passes(5, 5, true));
        assert!(!fence_passes(5, 5, false));
        assert!(!fence_passes(4, 5, true), "an older epoch never passes");
        assert!(!fence_passes(6, 5, true));
    }

    #[test]
    fn the_ttl_range_and_renewal_interval() {
        assert!(OwnerLeaseTtl::new(Duration::from_secs(2)).is_err());
        assert!(OwnerLeaseTtl::new(Duration::from_secs(301)).is_err());
        let ttl = OwnerLeaseTtl::new(Duration::from_secs(3)).expect("the floor");
        assert_eq!(ttl.renew_interval(), Duration::from_secs(1));
        let ttl = OwnerLeaseTtl::DEFAULT;
        assert_eq!(ttl.get(), Duration::from_secs(10));
        let sent = Instant::now();
        let deadline = ttl.local_deadline(sent);
        assert_eq!(
            deadline - sent,
            Duration::from_secs(10)
                .checked_sub(ttl.renew_interval())
                .expect("the interval is shorter than the TTL")
        );
        assert!(OwnerLeaseTtl::new(OwnerLeaseTtl::MAX).is_ok());
    }

    #[test]
    fn a_holder_token_is_new_for_each_process() {
        let a = OwnerHolder::for_worker("worker-1");
        let b = OwnerHolder::for_worker("worker-1");
        assert_ne!(a, b, "a replacement worker is a new holder");
        assert!(a.as_str().starts_with("worker-1/"));
    }

    #[test]
    fn an_unload_drops_only_the_fenced_tenure() {
        let owned = OwnedPartitions::default();
        assert!(owned.is_empty());
        let old = tenure(7, 3);
        owned.adopt(old.clone());
        assert!(
            !owned.unload(old.key(), OwnerEpoch::new(2)),
            "an older fence"
        );
        assert_eq!(owned.tenure_for(old.key()), Some(old.clone()));
        // The worker took the row back at a newer epoch.
        let new = tenure(7, 5);
        assert_eq!(owned.adopt(new.clone()), Some(old.clone()));
        assert!(
            !owned.unload(old.key(), old.epoch()),
            "a fence of the old tenure"
        );
        assert_eq!(owned.len(), 1);
        assert!(owned.unload(new.key(), new.epoch()));
        assert!(owned.tenure_for(new.key()).is_none());
        assert_eq!(owned.tenures(), Vec::<OwnerTenure>::new());
    }

    #[test]
    fn tenures_of_two_databases_never_mix() {
        let owned = OwnedPartitions::default();
        let a = tenure(7, 3);
        let b = OwnerTenure::pinned(
            ShardId::new(1),
            a.partition(),
            OwnerHolder::for_worker("w"),
            OwnerEpoch::new(9),
            OwnerLeaseTtl::DEFAULT,
        );
        owned.adopt(a.clone());
        owned.adopt(b.clone());
        assert_eq!(owned.len(), 2, "one tenure per database and partition");
        assert!(
            !owned.unload(a.key(), b.epoch()),
            "the epoch of the other database"
        );
        assert!(owned.unload(b.key(), b.epoch()));
        assert_eq!(owned.tenure_for(a.key()), Some(a));
    }

    #[test]
    fn owner_mode_fails_closed_without_a_tenure() {
        let owned = OwnedPartitions::default();
        let held = tenure(7, 3);
        let other = OwnerKey {
            shard: ShardId::new(0),
            partition: DecisionPartition::new(8).expect("in range"),
        };
        assert_eq!(owned.pin_for(other), DecisionPin::Legacy, "owner mode off");
        assert!(owned.may_cache(other, None));
        assert!(!owned.owner_mode(ShardId::new(0)));
        owned.set_owner_mode(ShardId::new(0), true);
        assert!(owned.owner_mode(ShardId::new(0)));
        assert!(!owned.owner_mode(ShardId::new(1)), "per shard");
        owned.adopt(held.clone());
        assert_eq!(owned.pin_for(held.key()), DecisionPin::Owned(held.clone()));
        assert_eq!(owned.pin_for(other), DecisionPin::Unowned);
        assert!(
            !owned.may_cache(other, None),
            "no tenure, no cache in owner mode"
        );
        assert!(owned.may_cache(held.key(), Some(held.epoch())));
        assert!(
            !owned.may_cache(held.key(), Some(OwnerEpoch::new(2))),
            "a stale epoch"
        );
        owned.set_owner_mode(ShardId::new(0), false);
        assert!(!owned.owner_mode(ShardId::new(0)));
        assert_eq!(owned.pin_for(other), DecisionPin::Legacy);
    }

    #[tokio::test]
    async fn a_pin_lives_only_inside_its_decision_scope() {
        let outside = pin_decision(Some(tenure(1, 1)));
        assert!(!outside, "no scope, no pin");
        assert_eq!(pinned_decision(), None);
        let inside = scope_decision(async {
            assert_eq!(pinned_decision(), None);
            assert!(pin_decision(Some(tenure(1, 1))));
            tokio::task::yield_now().await;
            pinned_decision().map(|t| t.epoch())
        })
        .await;
        assert_eq!(inside, Some(OwnerEpoch::new(1)));
        assert_eq!(pinned_decision(), None);
        // A spawned task does not see the pin, so a foreign write takes no
        // owner row.
        let spawned = scope_decision(async {
            let _ = pin_decision(Some(tenure(2, 1)));
            tokio::spawn(async { pinned_decision() })
                .await
                .expect("join")
        })
        .await;
        assert_eq!(spawned, None);
    }

    #[test]
    fn a_lone_owner_takes_every_partition() {
        let plan = plan_balance(0, 0, 512, 0);
        assert_eq!(plan.target, 512);
        assert_eq!(plan.acquire, 512);
        assert_eq!(plan.release, 0);
    }

    #[test]
    fn a_joiner_takes_up_to_the_target_and_an_owner_releases_down_to_it() {
        // Two owners: the target is 256.
        let full = plan_balance(512, 1, 0, 0);
        assert_eq!((full.target, full.acquire, full.release), (256, 0, 256));
        let joiner = plan_balance(0, 1, 256, 0);
        assert_eq!((joiner.acquire, joiner.release), (256, 0));
        // Three owners: the target is ⌈512 / 3⌉ = 171.
        let three = plan_balance(256, 2, 0, 0);
        assert_eq!((three.target, three.release), (171, 85));
    }

    #[test]
    fn one_partition_above_the_target_is_slack() {
        let plan = plan_balance(257, 1, 0, 0);
        assert_eq!(
            (plan.acquire, plan.release),
            (0, 0),
            "no flapping at target + 1"
        );
        let plan = plan_balance(258, 1, 0, 0);
        assert_eq!(plan.release, 2);
    }

    #[test]
    fn any_owner_takes_orphans_so_a_crashed_peer_strands_nothing() {
        // The crashed peer still looks live, so the target stays 256. Its
        // 256 partitions lapsed long ago.
        let plan = plan_balance(256, 1, 256, 256);
        assert_eq!(plan.acquire, 256);
        // A fresh release is free but not an orphan yet. A joiner takes it.
        let plan = plan_balance(256, 1, 256, 0);
        assert_eq!(plan.acquire, 0);
        // Never more than is free.
        assert_eq!(plan_balance(0, 0, 3, 9).acquire, 3);
    }

    #[test]
    fn the_owned_claim_lists_live_tenures_of_one_shard() {
        let owned = OwnedPartitions::default();
        let shard = ShardId::new(0);
        assert_eq!(owned.owned_claim(shard), None, "owned claims are off");
        owned.set_owner_mode(shard, true);
        assert_eq!(
            owned.owned_claim(shard),
            None,
            "owner mode alone keeps the legacy claim"
        );
        owned.set_owned_claims(shard, true);
        assert_eq!(owned.owned_claim(shard), None, "no holder yet");
        let holder = OwnerHolder::for_worker("w");
        owned.set_holder(holder.clone());
        let live = tenure(3, 1);
        let expired = tenure(4, 1);
        let other_shard = OwnerTenure::pinned(
            ShardId::new(1),
            DecisionPartition::new(5).expect("in range"),
            holder.clone(),
            OwnerEpoch::new(1),
            OwnerLeaseTtl::DEFAULT,
        );
        let later = Instant::now() + Duration::from_secs(60);
        owned.adopt_until(live.clone(), later);
        owned.adopt_until(expired.clone(), Instant::now());
        owned.adopt_until(other_shard, later);
        let claim = owned.owned_claim(shard).expect("owner mode is on");
        assert_eq!(claim.holder, holder.as_str());
        assert_eq!(claim.partitions, vec![3], "the expired tenure is left out");
        assert_eq!(owned.pin_for(expired.key()), DecisionPin::Unowned);
        assert_eq!(owned.pin_for(live.key()), DecisionPin::Owned(live.clone()));
        // A renewal moves the deadline of the current tenure only.
        owned.extend(std::slice::from_ref(&expired), later);
        assert_eq!(owned.pin_for(expired.key()), DecisionPin::Owned(expired));
        let stale = tenure(3, 0);
        owned.extend(&[stale], Instant::now());
        assert_eq!(owned.pin_for(live.key()), DecisionPin::Owned(live));
        assert_eq!(owned.tenures_of(shard).len(), 2);
    }

    #[test]
    fn the_capability_label_is_engine_owned() {
        let mut forged = serde_json::json!({ "gpu": "true" });
        forged[OWNER_MODE_CAPABILITY_LABEL] = serde_json::json!(0);
        let merged = advertise_owner_capability(&forged);
        assert_eq!(merged["gpu"], "true");
        assert_eq!(
            merged[OWNER_MODE_CAPABILITY_LABEL],
            OWNER_MODE_CAPABILITY_VERSION
        );
        let replaced = advertise_owner_capability(&serde_json::json!("not-an-object"));
        assert_eq!(
            replaced[OWNER_MODE_CAPABILITY_LABEL],
            OWNER_MODE_CAPABILITY_VERSION
        );
    }

    #[test]
    fn the_owned_mask_sets_one_bit_per_partition() {
        let claim = OwnedClaim {
            holder: "w".to_string(),
            partitions: vec![0, 9, 511],
        };
        let mask = claim.mask();
        assert_eq!(mask.len(), 64);
        assert_eq!(mask[0], 0b0000_0001);
        assert_eq!(mask[1], 0b0000_0010);
        assert_eq!(mask[63], 0b1000_0000);
        assert_eq!(mask.iter().map(|b| b.count_ones()).sum::<u32>(), 3);
    }

    #[test]
    fn a_partition_fits_the_task_queue_column() {
        assert_eq!(
            DecisionPartition::new(0).map(DecisionPartition::as_i16),
            Some(0)
        );
        assert_eq!(
            DecisionPartition::new(511).map(DecisionPartition::as_i16),
            Some(511)
        );
    }

    #[test]
    fn only_one_worker_takes_a_set_of_tenures() {
        let owned = OwnedPartitions::default();
        assert!(owned.try_claim(), "the first worker takes the set");
        assert!(!owned.try_claim(), "a second worker takes its own set");
    }

    #[tokio::test]
    async fn a_worker_scope_names_its_own_tenures() {
        assert!(current_owned().is_none());
        let mine = Arc::new(OwnedPartitions::default());
        mine.set_owner_mode(ShardId::new(0), true);
        let seen = scope_decision_for(Arc::clone(&mine), async {
            // A nested scope of a local activity keeps the worker's set.
            scope_decision(async { current_owned().map(|o| o.owner_mode(ShardId::new(0))) }).await
        })
        .await;
        assert_eq!(seen, Some(true));
        assert!(current_owned().is_none());
    }

    #[test]
    fn lease_write_lists_are_sorted() {
        let p = |n| DecisionPartition::new(n).expect("in range");
        assert_eq!(
            LeaseWrite::done(vec![p(9), p(2)], vec![p(5), p(1)]),
            LeaseWrite::Done(Matched {
                matched: vec![p(2), p(9)],
                lost: vec![p(1), p(5)],
            })
        );
    }
}
