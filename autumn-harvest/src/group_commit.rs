//! Group commit of the decisions of one decision partition (issue #2256).
//!
//! An owner can commit the decisions of several executions of one
//! partition in one Postgres transaction. `DESIGN-2256.md` section 1 is the
//! design note. In short:
//!
//! - [`GroupCommit`] keeps one lane per shard and partition. A lane has
//!   `width` slots and a queue.
//! - A decision leads a batch of one at once while a slot is free. So a
//!   batch of two or more forms only when decisions wait for a slot.
//! - The batch fences once. The owner row stays locked `FOR KEY SHARE`
//!   until the batch commits, so no takeover commits mid-batch.
//! - Each stage of a batch of two or more runs in its own savepoint. A
//!   failing stage rolls back alone.
//! - No result goes out before the batch commits.
//!
//! The pure kernels [`plan_batch`], [`accepts`] and [`acknowledges`] carry
//! the rules. The simulator in `dst::group` calls them.
//! `formal/verus/src/group_commit.rs` proves `accepts` and `acknowledges`,
//! and the spec of `plan_batch`.
//!
//! The worker persist paths do not use the lane yet. Each path must first
//! split its post-commit work from its transaction body (issue #2318).

/// The default slots of one lane.
pub const DEFAULT_WIDTH: usize = 4;

/// The default bound of one batch.
pub const DEFAULT_MAX_BATCH: usize = 32;

/// One queued entry, as [`plan_batch`] sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Queued<G, K> {
    /// The tenure that the decision pinned.
    pub tenure: G,
    /// The execution of the decision.
    pub execution: K,
}

/// Whether a batch of `batch_len` entries accepts one more entry.
///
/// The entry must have the leader's tenure, and the batch must hold no
/// entry of its execution. `formal/verus/src/group_commit.rs` proves that a
/// plan built with this rule keeps every rule of a batch.
#[must_use]
pub const fn accepts(
    batch_len: usize,
    max_batch: usize,
    same_tenure: bool,
    execution_in_batch: bool,
) -> bool {
    batch_len < max_batch && same_tenure && !execution_in_batch
}

/// The indices into `queue` of the batch that the entry at `own` leads.
///
/// The batch starts with `own`. The other entries follow in queue order.
/// Each one has the tenure of `own`, and no two share an execution. The
/// batch holds at most `max_batch` entries. With `own` out of range, or
/// with `max_batch` at 0, the batch is empty.
#[must_use]
pub fn plan_batch<G: PartialEq, K: PartialEq>(
    queue: &[Queued<G, K>],
    own: usize,
    max_batch: usize,
) -> Vec<usize> {
    let Some(leader) = queue.get(own) else {
        return Vec::new();
    };
    if max_batch == 0 {
        return Vec::new();
    }
    let mut batch = vec![own];
    for (at, entry) in queue.iter().enumerate() {
        if at == own {
            continue;
        }
        let shared = batch
            .iter()
            .any(|&taken| queue[taken].execution == entry.execution);
        if accepts(
            batch.len(),
            max_batch,
            entry.tenure == leader.tenure,
            shared,
        ) {
            batch.push(at);
        }
    }
    batch
}

/// Whether a decision of a batch reports success.
///
/// It does only when its own stage passed and the batch committed. So no
/// result goes out before the durable write. `formal/verus/src/group_commit.rs`
/// proves that an ordinary failure of another stage cannot change it.
#[must_use]
pub const fn acknowledges(stage_passed: bool, batch_committed: bool) -> bool {
    stage_passed && batch_committed
}

/// The width and the batch bound of a lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupCommitConfig {
    width: usize,
    max_batch: usize,
}

impl GroupCommitConfig {
    /// A lane with `width` slots and at most `max_batch` decisions per
    /// batch. Returns `None` when a bound is 0.
    ///
    /// A `max_batch` of 1 turns group commit off. Each decision then runs
    /// alone, in at most `width` transactions at once.
    #[must_use]
    pub const fn new(width: usize, max_batch: usize) -> Option<Self> {
        if width == 0 || max_batch == 0 {
            return None;
        }
        Some(Self { width, max_batch })
    }

    /// The slots of a lane.
    #[must_use]
    pub const fn width(self) -> usize {
        self.width
    }

    /// The most decisions in one batch.
    #[must_use]
    pub const fn max_batch(self) -> usize {
        self.max_batch
    }
}

impl Default for GroupCommitConfig {
    fn default() -> Self {
        Self {
            width: DEFAULT_WIDTH,
            max_batch: DEFAULT_MAX_BATCH,
        }
    }
}

/// Counters of the lanes of one [`GroupCommit`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GroupCommitStats {
    /// Batches that ran, a batch of one included.
    pub batches: u64,
    /// Decisions that ran.
    pub decisions: u64,
    /// Decisions in batches of two or more.
    pub grouped: u64,
    /// The largest batch.
    pub largest: usize,
    /// Savepoints that the batches opened.
    pub savepoints: u64,
    /// Stages that rolled back to their savepoint.
    pub rolled_back: u64,
    /// Batches that committed nothing.
    pub failed_batches: u64,
}

#[cfg(feature = "db")]
pub use db::{GroupCommit, Stage, stage};

#[cfg(feature = "db")]
mod db {
    use std::collections::{HashMap, VecDeque};
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

    use diesel_async::{AsyncConnection, AsyncPgConnection};
    use futures::future::BoxFuture;
    use tokio::sync::{Semaphore, oneshot};

    use super::{GroupCommitConfig, GroupCommitStats, Queued, acknowledges, plan_batch};
    use crate::decision_owner::{self, OwnerKey, OwnerTenure};
    use crate::error::{HarvestError, HarvestResult};
    use crate::types::ExecutionId;
    use crate::worker::DbPool;

    /// The body of one decision transaction.
    ///
    /// It runs on the connection of its batch, inside the batch fence. It
    /// must not report a result or start post-commit work. Its caller does
    /// that after [`GroupCommit::commit`] returns `Ok`. It runs on a task of
    /// its batch. So it sees no task-local of its caller, and it must not
    /// pin a tenure.
    pub type Stage<T> = Box<
        dyn for<'c> FnOnce(&'c mut AsyncPgConnection) -> BoxFuture<'c, HarvestResult<T>> + Send,
    >;

    /// Box `body` as a [`Stage`].
    pub fn stage<T, F>(body: F) -> Stage<T>
    where
        F: for<'c> FnOnce(&'c mut AsyncPgConnection) -> BoxFuture<'c, HarvestResult<T>>
            + Send
            + 'static,
    {
        Box::new(body)
    }

    /// One decision in a lane.
    struct Entry<T> {
        id: u64,
        tenure: OwnerTenure,
        execution: ExecutionId,
        stage: Stage<T>,
        reply: oneshot::Sender<HarvestResult<T>>,
    }

    /// The slots and the queue of one partition.
    struct Lane<T> {
        slots: Arc<Semaphore>,
        queue: Mutex<VecDeque<Entry<T>>>,
    }

    impl<T> Lane<T> {
        fn queue(&self) -> MutexGuard<'_, VecDeque<Entry<T>>> {
            // A panic under the lock leaves a whole queue. So a poisoned
            // lock is still safe to use.
            self.queue.lock().unwrap_or_else(PoisonError::into_inner)
        }

        /// Remove the batch that entry `id` leads, or `None` when another
        /// leader took the entry.
        fn take(&self, id: u64, max_batch: usize) -> Option<Vec<Entry<T>>> {
            let mut queue = self.queue();
            let own = queue.iter().position(|entry| entry.id == id)?;
            let views: Vec<Queued<&OwnerTenure, ExecutionId>> = queue
                .iter()
                .map(|entry| Queued {
                    tenure: &entry.tenure,
                    execution: entry.execution,
                })
                .collect();
            let order = plan_batch(&views, own, max_batch);
            let mut slots: Vec<Option<Entry<T>>> = queue.drain(..).map(Some).collect();
            let batch = order.iter().filter_map(|&at| slots[at].take()).collect();
            queue.extend(slots.into_iter().flatten());
            drop(queue);
            Some(batch)
        }

        fn remove(&self, id: u64) {
            self.queue().retain(|entry| entry.id != id);
        }
    }

    /// Removes a queued entry when its caller leaves before a batch takes
    /// it. A batch never runs a decision whose caller left first.
    struct Waiting<'a, T> {
        lane: &'a Lane<T>,
        id: u64,
        armed: bool,
    }

    impl<T> Drop for Waiting<'_, T> {
        fn drop(&mut self) {
            if self.armed {
                self.lane.remove(self.id);
            }
        }
    }

    #[derive(Default)]
    struct Counters {
        batches: AtomicU64,
        decisions: AtomicU64,
        grouped: AtomicU64,
        largest: AtomicUsize,
        savepoints: AtomicU64,
        rolled_back: AtomicU64,
        failed_batches: AtomicU64,
    }

    /// What a batch task needs: the pool and the counters.
    struct Shared {
        pool: DbPool,
        counters: Counters,
    }

    /// The group commit lanes of one database (issue #2256).
    ///
    /// `DESIGN-2256.md` section 1 is the design note. Each lane has its own
    /// slots, and all lanes share one pool. So size `width` times the owned
    /// partitions against the pool. Do not call [`Self::commit`] while you
    /// hold a connection of the same pool.
    pub struct GroupCommit<T> {
        config: GroupCommitConfig,
        lanes: Mutex<HashMap<OwnerKey, Arc<Lane<T>>>>,
        next_id: AtomicU64,
        shared: Arc<Shared>,
    }

    impl<T: Send + 'static> std::fmt::Debug for GroupCommit<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("GroupCommit")
                .field("config", &self.config)
                .field("stats", &self.stats())
                .finish_non_exhaustive()
        }
    }

    impl<T: Send + 'static> GroupCommit<T> {
        /// Lanes that take their connections from `pool`.
        #[must_use]
        pub fn new(pool: DbPool, config: GroupCommitConfig) -> Self {
            Self {
                config,
                lanes: Mutex::new(HashMap::new()),
                next_id: AtomicU64::new(0),
                shared: Arc::new(Shared {
                    pool,
                    counters: Counters::default(),
                }),
            }
        }

        /// Commit one decision of `execution` under `tenure`.
        ///
        /// The decision joins the lane of the tenure's partition. While a
        /// slot is free, it commits alone at once, with no savepoint. Else
        /// it waits, and the next free slot commits it with the other
        /// waiting decisions of the same tenure, one per execution.
        ///
        /// The result comes only after the batch commits. A caller that
        /// leaves before a batch takes its decision removes it, so it never
        /// runs. A batch runs on a task of its own. So a caller that leaves
        /// later does not cancel the batch.
        ///
        /// # Errors
        ///
        /// - The stage's own error when it fails. Its writes rolled back.
        /// - [`HarvestError::OwnershipLost`] or [`HarvestError::ShardFenced`]
        ///   when the batch fence fails, or when a stage returns one.
        /// - [`HarvestError::Database`] that names the group commit when
        ///   the checkout, a savepoint or the `COMMIT` fails. A lost
        ///   connection at `COMMIT` leaves the outcome unknown, as for a lone
        ///   decision.
        pub async fn commit(
            &self,
            tenure: OwnerTenure,
            execution: ExecutionId,
            stage: Stage<T>,
        ) -> HarvestResult<T> {
            let lane = self.lane(tenure.key());
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            let (reply, mut result) = oneshot::channel();
            lane.queue().push_back(Entry {
                id,
                tenure,
                execution,
                stage,
                reply,
            });
            let mut waiting = Waiting {
                lane: &lane,
                id,
                armed: true,
            };
            tokio::select! {
                biased;
                done = &mut result => {
                    waiting.armed = false;
                    return done.unwrap_or_else(|_| Err(left()));
                }
                permit = Arc::clone(&lane.slots).acquire_owned() => {
                    // A taken entry is no longer in the queue, so the guard
                    // has nothing to remove after this point.
                    waiting.armed = false;
                    if let Some(batch) = lane.take(id, self.config.max_batch()) {
                        // The slots never close, so `permit` holds a slot.
                        let permit = permit.ok();
                        let shared = Arc::clone(&self.shared);
                        tokio::spawn(async move {
                            shared.run(batch).await;
                            drop(permit);
                        });
                    }
                }
            }
            result.await.unwrap_or_else(|_| Err(left()))
        }

        /// The decisions that wait in the lane of `tenure`.
        #[must_use]
        pub fn queued(&self, tenure: &OwnerTenure) -> usize {
            self.lanes()
                .get(&tenure.key())
                .map_or(0, |lane| lane.queue().len())
        }

        /// The counters of every lane.
        #[must_use]
        pub fn stats(&self) -> GroupCommitStats {
            let c = &self.shared.counters;
            GroupCommitStats {
                batches: c.batches.load(Ordering::Relaxed),
                decisions: c.decisions.load(Ordering::Relaxed),
                grouped: c.grouped.load(Ordering::Relaxed),
                largest: c.largest.load(Ordering::Relaxed),
                savepoints: c.savepoints.load(Ordering::Relaxed),
                rolled_back: c.rolled_back.load(Ordering::Relaxed),
                failed_batches: c.failed_batches.load(Ordering::Relaxed),
            }
        }

        fn lanes(&self) -> MutexGuard<'_, HashMap<OwnerKey, Arc<Lane<T>>>> {
            // A panic under the lock leaves a whole map.
            self.lanes.lock().unwrap_or_else(PoisonError::into_inner)
        }

        fn lane(&self, key: OwnerKey) -> Arc<Lane<T>> {
            let width = self.config.width();
            Arc::clone(self.lanes().entry(key).or_insert_with(|| {
                Arc::new(Lane {
                    slots: Arc::new(Semaphore::new(width)),
                    queue: Mutex::new(VecDeque::new()),
                })
            }))
        }
    }

    impl Shared {
        /// Run `batch` in one transaction, then send each result.
        async fn run<T: Send + 'static>(&self, batch: Vec<Entry<T>>) {
            let len = batch.len();
            let c = &self.counters;
            c.batches.fetch_add(1, Ordering::Relaxed);
            c.decisions.fetch_add(len as u64, Ordering::Relaxed);
            c.largest.fetch_max(len, Ordering::Relaxed);
            if len > 1 {
                c.grouped.fetch_add(len as u64, Ordering::Relaxed);
            }
            let Some(tenure) = batch.first().map(|entry| entry.tenure.clone()) else {
                return;
            };
            let (stages, replies): (Vec<Stage<T>>, Vec<_>) =
                batch.into_iter().map(|e| (e.stage, e.reply)).unzip();
            let outcome = match self.pool.get().await {
                Ok(mut conn) => self.transact(&mut conn, tenure, stages).await,
                Err(error) => Err(HarvestError::Database(format!(
                    "group commit: the pool checkout failed: {error}"
                ))),
            };
            if outcome.is_err() {
                c.failed_batches.fetch_add(1, Ordering::Relaxed);
            }
            deliver(replies, outcome);
        }

        /// The batch transaction: one fence, then each stage.
        ///
        /// A batch of one runs its stage with no savepoint. A batch of two
        /// or more runs each stage in a savepoint. A fence error from a
        /// stage ends the whole batch, because the tenure is in doubt.
        async fn transact<T: Send + 'static>(
            &self,
            conn: &mut AsyncPgConnection,
            tenure: OwnerTenure,
            stages: Vec<Stage<T>>,
        ) -> HarvestResult<Vec<HarvestResult<T>>> {
            let c = &self.counters;
            decision_owner::scope_decision(async move {
                let _ = decision_owner::pin_decision(Some(tenure));
                conn.transaction::<_, HarvestError, _>(async move |conn| {
                    crate::replication::assert_decision_fence(conn).await?;
                    let id = decision_owner::conn_id(conn);
                    decision_owner::fenced_scope(id, async move {
                        if stages.len() == 1 {
                            let mut outcomes = Vec::with_capacity(1);
                            for stage in stages {
                                outcomes.push(Ok(stage(conn).await?));
                            }
                            return Ok(outcomes);
                        }
                        let mut outcomes = Vec::with_capacity(stages.len());
                        for stage in stages {
                            c.savepoints.fetch_add(1, Ordering::Relaxed);
                            let outcome = conn
                                .transaction::<_, HarvestError, _>(async move |conn| {
                                    stage(conn).await
                                })
                                .await;
                            match outcome {
                                Err(error) if fences(&error) => return Err(error),
                                Err(error) => {
                                    c.rolled_back.fetch_add(1, Ordering::Relaxed);
                                    outcomes.push(Err(error));
                                }
                                Ok(value) => outcomes.push(Ok(value)),
                            }
                        }
                        if outcomes.iter().any(Result::is_err) {
                            probe(conn).await?;
                        }
                        Ok(outcomes)
                    })
                    .await
                })
                .await
            })
            .await
        }
    }

    /// Fail when the batch transaction is aborted.
    ///
    /// A failed `SAVEPOINT`, `RELEASE` or `ROLLBACK TO` statement can leave
    /// the transaction aborted. Postgres then answers `COMMIT` with a silent
    /// rollback. So after a failed stage, the batch sends one statement
    /// first. It fails on an aborted transaction, and the batch then fails.
    async fn probe(conn: &mut AsyncPgConnection) -> HarvestResult<()> {
        use diesel_async::RunQueryDsl;

        diesel::sql_query("SELECT 1")
            .execute(conn)
            .await
            .map(drop)
            .map_err(|error| {
                HarvestError::Database(format!(
                    "group commit: the batch transaction aborted: {error}"
                ))
            })
    }

    /// Send the result of each decision of a batch.
    ///
    /// `outcome` holds each stage result when the batch committed, or the
    /// error that ended the batch. A decision gets `Ok` only when its stage
    /// passed and the batch committed.
    fn deliver<T>(
        replies: Vec<oneshot::Sender<HarvestResult<T>>>,
        outcome: HarvestResult<Vec<HarvestResult<T>>>,
    ) {
        let len = replies.len();
        let (committed, stages, mut cause) = match outcome {
            Ok(stages) => (true, stages.into_iter().map(Some).collect(), None),
            Err(error) => (false, Vec::new(), Some(error)),
        };
        let mut stages: Vec<Option<HarvestResult<T>>> = stages;
        stages.resize_with(len, || None);
        for (reply, stage) in replies.into_iter().zip(stages) {
            let passed = matches!(stage, Some(Ok(_)));
            let result = match stage {
                Some(Ok(value)) if acknowledges(passed, committed) => Ok(value),
                Some(Err(error)) => Err(error),
                _ => Err(batch_error(len, &mut cause)),
            };
            let _ = reply.send(result);
        }
    }

    /// The error of one decision of a batch that committed nothing.
    ///
    /// A batch of one keeps its own error, as a lone decision does today.
    fn batch_error(len: usize, cause: &mut Option<HarvestError>) -> HarvestError {
        match (len, cause.take()) {
            (1, Some(error)) => error,
            (_, Some(error)) => {
                let copy = rolled_back(&error);
                *cause = Some(error);
                copy
            }
            (_, None) => left(),
        }
    }

    /// Whether `error` puts the tenure of the batch in doubt.
    const fn fences(error: &HarvestError) -> bool {
        matches!(
            error,
            HarvestError::OwnershipLost { .. } | HarvestError::ShardFenced { .. }
        )
    }

    /// A copy of `error` for each decision of a failed batch.
    ///
    /// A fence error keeps its variant, so the owner unloads the partition.
    /// Any other error names the group commit and its cause.
    fn rolled_back(error: &HarvestError) -> HarvestError {
        match error {
            HarvestError::OwnershipLost {
                partition,
                pinned,
                current,
            } => HarvestError::OwnershipLost {
                partition: *partition,
                pinned: *pinned,
                current: *current,
            },
            HarvestError::ShardFenced {
                shard_id,
                pinned,
                current,
            } => HarvestError::ShardFenced {
                shard_id: *shard_id,
                pinned: *pinned,
                current: *current,
            },
            other => {
                HarvestError::Database(format!("group commit: the batch rolled back: {other}"))
            }
        }
    }

    /// The error of a decision whose batch task ended with no result.
    ///
    /// Only a panic in a stage ends the task that way. The outcome is then
    /// unknown.
    fn left() -> HarvestError {
        HarvestError::Database(
            "group commit: the batch task ended with no result; the outcome is unknown".to_string(),
        )
    }
}
