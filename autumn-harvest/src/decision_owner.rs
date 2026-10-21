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
//! Owner mode is off until issue #2252 adds its switch and its claim. No
//! production path pins a tenure before then.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
    /// Counts each change of the tenure set (issue #2253). The owner wake
    /// listener reads it to know when to change its channels.
    version: AtomicU64,
    /// Fires at each change of the tenure set (issue #2253). An owner wake
    /// task with no tenure waits on it, so it does not poll.
    changed: tokio::sync::Notify,
}

/// Owner wakes are on in this process (issue #2253).
static OWNER_WAKES: AtomicBool = AtomicBool::new(false);

/// Turn owner wakes on for this process (issue #2253).
///
/// A foreign write then wakes the owner of its partition after it commits.
/// [`OwnedPartitions::set_owner_mode`] calls this. A process that only
/// writes, such as an API server, calls it itself. Issue #2252 replaces the
/// flag with the switch row of each database.
pub fn enable_owner_wakes() {
    OWNER_WAKES.store(true, Ordering::Relaxed);
}

/// Whether owner wakes are on in this process (issue #2253).
#[must_use]
pub fn owner_wakes_enabled() -> bool {
    OWNER_WAKES.load(Ordering::Relaxed)
}

#[derive(Debug, Default)]
struct Owned {
    tenures: BTreeMap<OwnerKey, OwnerTenure>,
    owner_mode: std::collections::BTreeSet<ShardId>,
}

impl OwnedPartitions {
    fn state(&self) -> std::sync::MutexGuard<'_, Owned> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Turn owner mode on or off for `shard`.
    ///
    /// With owner mode on, a decision with no tenure fails closed. Issue
    /// #2252 sets it from the per-shard switch.
    pub fn set_owner_mode(&self, shard: ShardId, on: bool) {
        let mut state = self.state();
        if on {
            enable_owner_wakes();
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
    /// tenure starts cold (rule R4 of ADR 0010).
    pub fn adopt(&self, tenure: OwnerTenure) -> Option<OwnerTenure> {
        let replaced = self.state().tenures.insert(tenure.key(), tenure);
        self.version.fetch_add(1, Ordering::Release);
        self.changed.notify_waiters();
        replaced
    }

    /// The tenure of `key`, if this worker holds one.
    #[must_use]
    pub fn tenure_for(&self, key: OwnerKey) -> Option<OwnerTenure> {
        self.state().tenures.get(&key).cloned()
    }

    /// How a decision of `key` commits.
    #[must_use]
    pub fn pin_for(&self, key: OwnerKey) -> DecisionPin {
        let state = self.state();
        match state.tenures.get(&key) {
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
        }
        drop(state);
        if current {
            self.version.fetch_add(1, Ordering::Release);
            self.changed.notify_waiters();
        }
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

    /// A count that changes with each change of the tenure set.
    #[must_use]
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// A future that completes at the next change of the tenure set.
    ///
    /// Call [`tokio::sync::futures::Notified::enable`] on it before reading
    /// [`Self::version`]. A change after the read then still completes it.
    pub fn changed(&self) -> tokio::sync::futures::Notified<'_> {
        self.changed.notified()
    }

    /// The partitions of the tenures on `shard`, in ascending order.
    #[must_use]
    pub fn partitions_on(&self, shard: ShardId) -> Vec<DecisionPartition> {
        self.state()
            .tenures
            .keys()
            .filter(|key| key.shard == shard)
            .map(|key| key.partition)
            .collect()
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

#[cfg(feature = "db")]
pub use db::{
    acquire, end_upload_allowance, expire_all, fence, fence_pinned, is_lock_miss, release_all,
    renew_all, start_upload_allowance,
};

#[cfg(feature = "db")]
mod db {
    use std::time::Duration;

    use diesel::sql_types::{Array, BigInt, Bool, Double, Integer, Nullable, Text};
    use diesel::{OptionalExtension, QueryableByName};
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

    use super::{
        Acquisition, DecisionPartition, LeaseWrite, OwnerEpoch, OwnerHolder, OwnerLeaseConfig,
        OwnerTenure, fence_passes, next_epoch, pinned_decision,
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

    /// Issue #2253: the owner wake task follows the tenure set through
    /// its version.
    #[test]
    fn the_version_changes_with_the_tenure_set() {
        let owned = OwnedPartitions::default();
        let start = owned.version();
        owned.adopt(tenure(4, 1));
        owned.adopt(tenure(2, 1));
        assert_ne!(owned.version(), start, "an adopt changes the version");
        let shard = ShardId::new(0);
        let p = |n| DecisionPartition::new(n).expect("in range");
        assert_eq!(owned.partitions_on(shard), [p(2), p(4)]);
        assert!(owned.partitions_on(ShardId::new(1)).is_empty());
        let adopted = owned.version();
        assert!(!owned.unload(tenure(4, 1).key(), OwnerEpoch::new(9)));
        assert_eq!(owned.version(), adopted, "a stale unload changes nothing");
        assert!(owned.unload(tenure(4, 1).key(), OwnerEpoch::new(1)));
        assert_ne!(owned.version(), adopted, "an unload changes the version");
        assert_eq!(owned.partitions_on(shard), [p(2)]);
    }

    /// Issue #2253: owner mode turns owner wakes on for the process.
    #[test]
    fn owner_mode_turns_owner_wakes_on() {
        OwnedPartitions::default().set_owner_mode(ShardId::new(0), true);
        assert!(owner_wakes_enabled());
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
