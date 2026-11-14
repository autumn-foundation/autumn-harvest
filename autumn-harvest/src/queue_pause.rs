//! Task-queue pause/resume — hold dispatch during a downstream outage (issue #619).
//!
//! A downstream dependency for a whole *class* of activities can go down:
//! Stripe, `SendGrid`, the data warehouse. The operator then must **hold** the
//! affected work, not fail it. Freeze the queue, let the dependency recover,
//! thaw, and everything continues as if nothing happened.
//!
//! # How it composes with the neighbouring primitives
//!
//! | Primitive | What it does | Issue |
//! |---|---|---|
//! | Admission gate | Halts new workflow **starts**; in-flight runs keep scheduling activities | #377 / #618 |
//! | Circuit breaker | **Short-circuits** dispatch: defers it by default, or fails it and pushes workflows down error branches (`FailFast`) | #369 / #1809 |
//! | Per-execution pause | Holds **one** execution — useless when you know the queue, not the 50,000 executions | #383 / #609 |
//! | **Queue pause (this)** | **Holds dispatch** on a named queue: nothing fails, nothing retries, nothing dead-letters | **#619** |
//!
//! Gate the *door*, breaker the *short circuit*, pause the *hold*.
//!
//! # Enforcement model — anti-join, not cache
//!
//! A pause is durable queue metadata enforced on the claim path
//! ([`QUEUE_PAUSE_CLAIM_PREDICATE`]), exactly like the PAUSED-execution skip
//! (#383) and the rate-limit gate (#332). It is deliberately **not** an
//! in-process cache like the admission gate's:
//!
//! - "Durable and re-applied before the worker pool begins claiming" is true
//!   **by construction**. There is no boot window to lose a pause in, no
//!   refresh interval, and no fleet-wide staleness window.
//! - There is no global static, no fail-closed reasoning, and no write-through
//!   path that can diverge from the database.
//!
//! The target cost is one probe per claim, against a table bounded by the
//! number of distinct queue names. The query *shape* decides whether that holds.
//! `claim_task` first embedded [`queue_pause_anti_join`] as a per-row correlated
//! `NOT EXISTS`. The planner ran it once per *candidate row* on the way to
//! `LIMIT 1`, not once per claim. At a 10k-row backlog that was ~99% of the
//! query's buffer touches (`docs/performance.md`). `claim_task_query` now reads
//! `harvest_queue_pauses` once into a small array (`paused_queues`). It tests
//! membership per row and does not probe the table per row.
//! [`queue_pause_anti_join`] and [`QUEUE_PAUSE_CLAIM_PREDICATE`] stay correct
//! for a **mirror** query that loads few rows (`oldest_pending_ages`,
//! `claimable_pending_demand_by_queue`). The array rewrite only pays off on
//! the high-cardinality candidate scan of `claim_task`.
//!
//! # Replay contract
//!
//! A pause writes **nothing** to `harvest_events` and introduces **no**
//! [`crate::event::WorkflowEvent`] variant. Replay of an execution that touched
//! a paused queue is byte-identical to one that never paused.
//!
//! # Timeout interaction
//!
//! The *relative* `schedule_to_start` timer stops for held tasks, in the scan
//! predicate and in the locked re-check. A resume credits held time back to
//! `scheduled_at`, so a thaw does not time out the whole backlog. The
//! *absolute* `schedule_to_close` deadline (#378) keeps ticking: a paused queue
//! does not extend an SLA. See [`queue_pause_suppresses_timeout`].

#[cfg(feature = "db")]
use crate::error::HarvestError;
use crate::error::HarvestResult;
#[cfg(feature = "db")]
use chrono::{DateTime, Utc};
#[cfg(feature = "db")]
use diesel_async::AsyncPgConnection;

/// Build the late-pass statement. `$tail` is the optional `RETURNING` clause.
///
/// One body feeds both variants, so they cannot drift apart.
macro_rules! late_arrivals_sql {
    ($tail:literal) => {
        concat!(
            "UPDATE harvest_task_queue \
             SET scheduled_at = CASE \
                   WHEN created_at IS NULL OR created_at < $2 \
                     THEN scheduled_at + (clock_timestamp() - GREATEST(scheduled_at, $2)) \
                   ELSE clock_timestamp() \
                 END \
             WHERE queue_name = $1 \
               AND state = 'PENDING' \
               AND scheduled_at <= clock_timestamp() \
               AND NOT EXISTS ( \
                     SELECT 1 FROM unnest($3::uuid[]) AS already(id) \
                     WHERE already.id = harvest_task_queue.id \
                   )",
            $tail
        )
    };
}

/// One id returned by a resume shift pass.
///
/// The second pass excludes the ids of the first pass. The resume publishes a
/// dispatch hint for the ids of both passes after commit (issue #2066).
#[cfg(feature = "db")]
#[derive(diesel::QueryableByName)]
struct ShiftedTaskId {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: uuid::Uuid,
}

/// Domain separator for [`queue_lock_keys`] (issue #619).
///
/// Mirrors `RAMP_HASH_DOMAIN_SEP` in `crate::build_routing`. The digest is
/// this feature's alone. So a queue name never derives the same key as another
/// subsystem that hashes the same string. The `v1` marker makes a future
/// re-keying an explicit, greppable decision, not a silent change. See
/// [`queue_lock_keys`] for why a re-key is not safe in a rolling deploy.
const QUEUE_PAUSE_LOCK_DOMAIN: &str = "harvest:queue_pause:v1:";

/// Derive the `(key1, key2)` advisory-lock key that identifies `queue_name`.
///
/// Every queue-pause lock — [`lock_queue_for_pause`],
/// [`lock_queue_for_timeout_recheck`], [`try_lock_queue_for_claim`] — keys on
/// this pair, so they mutually exclude on exactly the queue they name.
///
/// # Why the whole 64 bits are spent on identity (issue #619)
///
/// Postgres exposes two *disjoint* advisory-lock keyspaces: a 64-bit one
/// (`pg_advisory_xact_lock(bigint)`) and a 2×32-bit one
/// (`pg_advisory_xact_lock(int, int)`). A key in one never collides with a key
/// in the other. **Every other advisory-lock user in this engine is in the
/// single-argument one.** These are the per-key concurrency gate on the hot
/// claim path (`crate::queue::claim_task`), `crate::mutex`,
/// `crate::wasm_store`, `crate::admission_gate` and the registration lock of
/// `crate::scheduler` (issue #1157). So the two-argument keyspace belongs to
/// this feature alone. That separation is load-bearing, and this function
/// preserves it. A source-level guard test keeps it true.
///
/// An earlier key spent the first 32 bits on a constant class id. It derived the
/// second from `hashtext(queue_name)`, which is only 32 bits wide. That left
/// **queue identity** at 32 bits. Two unrelated queue names collide with
/// probability ≈ n²/2³³. The result is the exact failure this feature exists
/// to prevent. An exclusive pause on queue `a` makes
/// [`try_lock_queue_for_claim`] fail for the colliding queue `b`. That call uses
/// the *try* variant, so the failure is silent. The workers of `b` release
/// every claim and stop dispatching on a queue that was never paused. This
/// lasts for as long as the pause or the (possibly long) resume of `a` runs.
///
/// The fix stops spending half the key on a constant. `key1` and `key2` are
/// the high and low halves of a domain-separated SHA-256 digest. So identity
/// is the full 64 bits, and a false share needs a 64-bit collision.
///
/// # Why not two locks, or the single-argument form
///
/// **Two locks** (a class-scoped pair, 32 identity bits each) make this
/// *worse*, not better. Postgres evaluates lock conflicts per key. So a claim
/// that needs both locks fails if **either** collides. That doubles the
/// false-share probability and does not square it away.
///
/// **The single-argument 64-bit form** gives the same identity width. But it
/// shares the keyspace that the concurrency gate of `claim_task` hashes raw
/// text into. That trades an intra-feature collision for a cross-feature one.
/// Its blast radius is strictly worse and it is far harder to diagnose.
///
/// # Diagnosing in `pg_locks`
///
/// The constant `619` is no longer in the key, so it no longer labels the row.
/// It does not need to: `pg_locks` reports `objsubid = 1` for the two-key form
/// (`2` for the `bigint` form), and this feature is its only user, so
///
/// ```sql
/// SELECT * FROM pg_locks WHERE locktype = 'advisory' AND objsubid = 1;
/// ```
///
/// selects exactly the queue-pause locks. Match a specific queue by computing
/// its pair with this function.
///
/// # Why SHA-256 rather than a fast hash
///
/// The rate-limit bucket keys of issue #699 use it for the same reason. A
/// collision here silently shares state between two unrelated queues. Callers
/// outside this module influence queue names. The digest runs once per lock
/// call, in-process, so it costs nothing next to the round trip it rides on.
///
/// # Compatibility
///
/// The key derivation is part of the *runtime* protocol between concurrent
/// processes. It is not a persisted format, and nothing stores it. So a change
/// to it (the domain separator included) is safe across a restart. It is
/// **not** safe across a rolling deploy. Two processes on different
/// derivations take non-conflicting locks for the same queue. They lose mutual
/// exclusion for the length of the rollout.
#[must_use]
pub fn queue_lock_keys(queue_name: &str) -> (i32, i32) {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(QUEUE_PAUSE_LOCK_DOMAIN.as_bytes());
    hasher.update(queue_name.as_bytes());
    let digest = hasher.finalize();

    let mut head = [0u8; 8];
    head.copy_from_slice(&digest[..8]);
    let key = u64::from_be_bytes(head);

    #[allow(clippy::cast_possible_wrap)]
    ((key >> 32) as u32 as i32, (key & 0xFFFF_FFFF) as u32 as i32)
}

/// SQL for [`lock_queue_for_pause`], exposed for shape tests.
///
/// `$1`/`$2` = the queue's [`queue_lock_keys`] pair.
#[must_use]
pub const fn lock_queue_for_pause_query() -> &'static str {
    "SELECT pg_advisory_xact_lock($1, $2)"
}

/// SQL for [`lock_queue_for_timeout_recheck`], exposed for shape tests.
///
/// `$1`/`$2` = the queue's [`queue_lock_keys`] pair — the same key as
/// [`lock_queue_for_pause_query`], **shared** mode.
#[must_use]
pub const fn lock_queue_for_timeout_recheck_query() -> &'static str {
    "SELECT pg_advisory_xact_lock_shared($1, $2)"
}

/// Upper bound on a pausable queue name.
///
/// Queue names are free-form `TEXT` everywhere else in the engine. Here the
/// queue name is the primary key, so an unbounded operator-supplied string is
/// an unbounded btree key. 255 matches the one other place the
/// engine bounds a queue name (`harvest_completion_triggers.queue_name`).
pub const MAX_QUEUE_NAME_LEN: usize = 255;

/// Render the queue-pause anti-join correlated against `outer_table` — the
/// single source of truth for the claim gate and every query that mirrors it.
///
/// A mirror query that forgets a paused queue reports a phantom backlog. That
/// drives a false capacity alert during a deliberate hold. So every mirror
/// comes from this one function, or is drift-locked against it.
///
/// The outer column reference **must** be qualified. Inside the subquery an
/// unqualified `queue_name` resolves against `harvest_queue_pauses` first. The
/// predicate then becomes `qp.queue_name = qp.queue_name`, which is always
/// true. That silently pauses *every* queue as soon as one queue is paused.
#[must_use]
pub fn queue_pause_anti_join(outer_table: &str) -> String {
    format!(
        "NOT EXISTS (SELECT 1 FROM harvest_queue_pauses qp \
         WHERE qp.queue_name = {outer_table}.queue_name)"
    )
}

/// The anti-join rendered for the unaliased `harvest_task_queue`, as a `const`
/// so the hot claim path can embed it without a per-claim allocation.
///
/// Drift-locked against [`queue_pause_anti_join`] by
/// `const_matches_the_renderer` — the two can never disagree.
pub const QUEUE_PAUSE_CLAIM_PREDICATE: &str = "NOT EXISTS (SELECT 1 FROM harvest_queue_pauses qp \
         WHERE qp.queue_name = harvest_task_queue.queue_name)";

/// Releases a just-claimed task back to `PENDING` when its queue turns out to
/// be paused (issue #619).
///
/// # Why a second statement is required
///
/// [`crate::queue::claim_task`] is a single autocommit statement. Under
/// `READ COMMITTED` its whole CTE evaluates against **one snapshot taken at
/// statement start**. That includes the [`QUEUE_PAUSE_CLAIM_PREDICATE`]
/// anti-join. The snapshot does not see a pause that commits after it. So a
/// claim in flight at pause time can still move its task to `RUNNING`. A
/// worker then dispatches that task into the outage. The window is normally
/// sub-millisecond, but it has no upper bound. The rate-limit debit of the
/// claim can block on a row lock that a competing claim holds. On unblock, the
/// `EvalPlanQual` re-check of Postgres only re-evaluates the *locked row*. It
/// never re-evaluates a correlated subquery over another table.
///
/// An *exclusive* queue-scoped lock inside the claim path closes the window.
/// But it serializes every claim for a queue against every other. That
/// defeats the parallel claiming of `FOR UPDATE SKIP LOCKED`, for a feature
/// that is almost always inactive. A re-check in a **fresh statement** gets a
/// fresh snapshot. It costs one indexed primary-key probe per *successful*
/// claim, never per empty poll. The contract is crisp: **a pause committed
/// before this re-check's statement begins always wins.**
///
/// That contract is only sound if the claim is *durable* by the time the pause
/// commits. The claim and this re-check run in one transaction, so a re-check
/// error cannot strand a `RUNNING` row. That puts the claim's commit *after*
/// the re-check's snapshot. It opens a new window: a pause that commits in
/// between is acknowledged to the operator, and the claim still commits
/// afterwards. [`try_lock_queue_for_claim`] closes it with a **shared** lock
/// on the key that `pause_queue` takes exclusively. See there for why the
/// *try* variant keeps this deadlock-free.
///
/// The guard `state = 'RUNNING' AND worker_id = $2` lets the release undo only
/// *this* worker's own claim. The release restores `attempt`. The task never
/// ran, so a hold must not consume retry budget (AC3). The rate-limit token
/// of the claim is not refunded. [`crate::queue::claim_task`] documents the
/// same safe-side, self-healing leak for a lost claim. It only under-dispatches.
///
/// Returns `true` when the claim was released (the caller must behave as if no
/// task was claimed).
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
#[cfg(feature = "db")]
pub async fn release_claim_if_queue_paused(
    conn: &AsyncPgConnection,
    task_id: uuid::Uuid,
    worker_id: &str,
) -> HarvestResult<bool> {
    crate::queue::release_claim_via(
        conn,
        release_claim_if_queue_paused_query(),
        task_id,
        worker_id,
    )
    .await
}

/// The shared prefix of both claim-release statements.
///
/// A macro rather than a `const`, so the two `const fn`s below can `concat!` it
/// with their own tail at compile time. Both releases must restore `attempt`
/// and use the same `state = 'RUNNING' AND worker_id = $2` guard. One copy
/// makes that structural, not a convention that two sites must remember.
macro_rules! release_claim_prefix {
    () => {
        "UPDATE harvest_task_queue \
         SET state = 'PENDING', \
             worker_id = NULL, \
             started_at = NULL, \
             attempt = GREATEST(attempt - 1, 0) \
         WHERE id = $1 \
           AND state = 'RUNNING' \
           AND worker_id = $2"
    };
}

/// SQL for [`release_claim_if_queue_paused`], exposed for shape tests.
///
/// One statement, so it takes a fresh `READ COMMITTED` snapshot. It sees any
/// pause committed before it began. See [`release_claim_if_queue_paused`].
#[must_use]
pub const fn release_claim_if_queue_paused_query() -> &'static str {
    concat!(
        release_claim_prefix!(),
        " AND EXISTS (SELECT 1 FROM harvest_queue_pauses qp \
           WHERE qp.queue_name = harvest_task_queue.queue_name)"
    )
}

/// SQL for [`release_claim`], exposed for shape tests.
#[must_use]
pub const fn release_claim_query() -> &'static str {
    release_claim_prefix!()
}

/// Release this worker's own just-made claim **unconditionally**.
///
/// The sibling of [`release_claim_if_queue_paused`] for the one case where the
/// pause table cannot be consulted. [`try_lock_queue_for_claim`] failed, so a
/// pause or resume transaction is mid-flight on this queue. Its rows are not
/// yet visible here. The guard and the `attempt` restore match the paused variant.
/// So a barrier miss costs a task at most one poll interval.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
#[cfg(feature = "db")]
pub async fn release_claim(
    conn: &AsyncPgConnection,
    task_id: uuid::Uuid,
    worker_id: &str,
) -> HarvestResult<bool> {
    crate::queue::release_claim_via(conn, release_claim_query(), task_id, worker_id).await
}

/// SQL for [`try_lock_queue_for_claim`], exposed for shape tests.
///
/// `$1`/`$2` = the queue's [`queue_lock_keys`] pair.
#[must_use]
pub const fn try_lock_queue_for_claim_query() -> &'static str {
    "SELECT pg_try_advisory_xact_lock_shared($1, $2) AS acquired"
}

/// Commit-order barrier between a claim and a concurrent pause (issue #619).
///
/// Takes the **shared** mode of the key that [`lock_queue_for_pause`] takes
/// exclusively. While a claim transaction holds it, a `pause_queue` cannot
/// commit until that claim has committed. So the pause cannot acknowledge a
/// hold to the operator first. The post-claim re-check alone cannot give this
/// order. The re-check takes its snapshot before its own transaction commits.
/// Without this lock, a pause can slot in between. A claim then beats a pause
/// that already reported the queue as held.
///
/// # Why *shared*, and why *try*
///
/// **Shared** because claims must stay parallel. Shared mode is compatible with
/// itself, so any number of workers claim concurrently. Only the rare, brief
/// exclusive pause/resume transaction is excluded. An exclusive lock here
/// serializes every claim on a queue and defeats `FOR UPDATE SKIP LOCKED`.
///
/// **Try** because of lock ordering. This call must come *after* the claim
/// query, because the queue to lock is not known until a task is in hand. So
/// the claim path holds task rows and then wants this lock. `resume_queue` does
/// the exact inverse. It holds this lock and then row-locks every `PENDING`
/// task on the queue for its `scheduled_at` shift. A *blocking* acquire here
/// creates an ABBA deadlock between a claim and a resume. Postgres then aborts
/// either the claim or the operator's resume.
/// `pg_try_advisory_xact_lock_shared` never waits, so it cannot participate in
/// a wait cycle at all.
///
/// `false` means "a pause or resume is committing on this queue right now".
/// The caller must then release its claim and not dispatch. That errs on the
/// safe side. A resume is not a hold, so a release during one costs at most
/// one poll interval. Dispatch during a *pause* is the failure this whole
/// feature exists to prevent.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
#[cfg(feature = "db")]
pub async fn try_lock_queue_for_claim(
    conn: &AsyncPgConnection,
    queue_name: &str,
) -> HarvestResult<bool> {
    use crate::cached_stmt::CachePreparedDsl as _;
    use diesel_async::RunQueryDsl;

    #[derive(diesel::QueryableByName)]
    struct AcquiredRow {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        acquired: bool,
    }

    let (key1, key2) = queue_lock_keys(queue_name);
    // A shared reference lets the claim pipeline this lock with its rechecks.
    let mut shared = conn;
    let row: AcquiredRow = diesel::sql_query(try_lock_queue_for_claim_query())
        .bind::<diesel::sql_types::Integer, _>(key1)
        .bind::<diesel::sql_types::Integer, _>(key2)
        .cache_prepared()
        .get_result(&mut shared)
        .await
        .map_err(crate::error::database_error)?;
    Ok(row.acquired)
}

/// Validate an operator-supplied queue name and return its canonical form.
///
/// Rejects blank (or whitespace-only) names and anything longer than
/// [`MAX_QUEUE_NAME_LEN`]. Pure — no database access — so the management API
/// can reject a typo with a `400` before touching a connection.
///
/// Surrounding whitespace is **rejected**, not trimmed, and the name is
/// otherwise used verbatim by every read and write in this module.
///
/// Both halves of that rule matter. The anti-join matches on exact equality.
/// `EnqueueParams` stores `queue_name` verbatim and does not normalise. So a
/// queue named `"payments "` is reachable:
///
/// - A **raw** stored name with a trimmed validated one inserts a row that
///   matches no task. It reports a successful hold (`newly_paused: true`,
///   `held_task_count: 0`) on a queue that keeps dispatching.
/// - **Trimming** is no better, just quieter. A pause of `"payments "` inserts
///   a row for `"payments"`. The hold silently targets a *different, valid*
///   queue, and the real one keeps dispatching. The API again reports success.
///
/// Rejecting is the only option that cannot lie. Almost always a stray space
/// is a copy-paste typo, and the operator gets a `400` that names it, not a
/// false green. Rarely the queue really has surrounding whitespace. Then this
/// API cannot pause it. That is an honest refusal. It is strictly safer than a
/// silent success against the wrong queue during an outage. The alternative
/// fix normalises queue names at *creation* time. That reaches across every
/// enqueue path and is out of scope here.
///
/// # Errors
///
/// [`HarvestError::Config`] when the name is blank, carries surrounding
/// whitespace, or is oversized.
pub fn validate_queue_name(queue_name: &str) -> HarvestResult<String> {
    if queue_name.trim().is_empty() {
        return Err(crate::error::HarvestError::Config(
            "queue name must not be empty".to_string(),
        ));
    }
    if queue_name != queue_name.trim() {
        return Err(crate::error::HarvestError::Config(
            "queue name must not have leading or trailing whitespace; \
             a queue name is matched exactly, so a stray space would hold a \
             different queue than the one intended"
                .to_string(),
        ));
    }
    if queue_name.len() > MAX_QUEUE_NAME_LEN {
        return Err(crate::error::HarvestError::Config(format!(
            "queue name exceeds {MAX_QUEUE_NAME_LEN} bytes"
        )));
    }
    Ok(queue_name.to_string())
}

/// Serialize pause/resume against the timeout enforcer for one queue.
///
/// The `schedule_to_start` enforcer must not fail a task on a queue that an
/// operator is concurrently pausing. Its non-locking `is_queue_paused`
/// re-check alone cannot guarantee that. Without this lock, `pause_queue`
/// shares no lock with the enforcer. So a pause can commit between the
/// re-check and the enforcer's own commit. The enforcer then fails a task
/// *because* its queue was paused. AC2/AC3/AC4 forbid exactly that outcome.
///
/// Both sides therefore take this transaction-scoped advisory lock first,
/// mirroring the `concurrency_key` and `ctx.mutex` patterns already used in the
/// engine. It is keyed on the queue name alone, so unrelated queues never
/// contend.
///
/// # Why the two-argument lock form (issue #619)
///
/// It uses the **two-argument** keyspace, which no other advisory-lock user in
/// this engine touches. Several unrelated features already share the
/// single-argument `hashtext(<some text>)` keyspace, with no namespace. The
/// most important is the per-key concurrency gate on the hot claim path
/// (`pg_try_advisory_xact_lock(hashtext(candidate.concurrency_key)::bigint)`
/// in `queue::claim_task`).
///
/// A shared keyspace causes a real cross-feature outage, not a theoretical
/// one. Take a queue name that hash-collides with the `concurrency_key` of some
/// *other* queue. The `pg_try_advisory_xact_lock` of that key then fails while
/// a large `resume_queue` holds this lock to shift its backlog. The claim path
/// uses the *try* variant, so the failure is silent: the claim yields no task.
/// Tasks on a queue that was never paused stop dispatching for the whole
/// resume of an unrelated queue. This feature exists to prevent exactly that
/// "silently stops dispatching" failure. So the namespace separation is
/// load-bearing, not hygiene.
///
/// [`queue_lock_keys`] closes the *intra*-feature half of the same hazard. See
/// it for why both key slots are spent on queue identity rather than one on a
/// constant class id.
///
/// # Errors
///
/// Database errors.
#[cfg(feature = "db")]
pub async fn lock_queue_for_pause(
    conn: &mut AsyncPgConnection,
    queue_name: &str,
) -> HarvestResult<()> {
    use diesel_async::RunQueryDsl;

    let (key1, key2) = queue_lock_keys(queue_name);
    diesel::sql_query(lock_queue_for_pause_query())
        .bind::<diesel::sql_types::Integer, _>(key1)
        .bind::<diesel::sql_types::Integer, _>(key2)
        .execute(conn)
        .await?;
    Ok(())
}

/// Serialize a `schedule_to_start` timeout enforcement against a concurrent
/// pause or resume — **without** blocking ordinary claims (issue #619).
///
/// Takes the *same* key as [`lock_queue_for_pause`] in **shared** mode, giving
/// the enforcer exactly the mutual exclusion it needs and nothing more:
///
/// | Held by | Wanted by | Compatible? |
/// |---|---|---|
/// | this (shared) | `pause_queue` / `resume_queue` (exclusive) | **no** — the serialization the re-check depends on |
/// | this (shared) | [`try_lock_queue_for_claim`] (shared) | **yes** — claims keep dispatching |
/// | this (shared) | another enforcement (shared) | yes — they serialize on task rows instead |
///
/// # Why not the exclusive lock
///
/// The enforcer must not reuse [`lock_queue_for_pause`]. Exclusive mode blocks
/// shared, and [`try_lock_queue_for_claim`] takes the shared mode of this key on
/// every claim. So an exclusive enforcement transaction stalls *all* dispatch on
/// its queue for its full duration, on a queue with **no pause at all**. The
/// workflow sibling holds the lock across `append_events`, the parent-close
/// cascade and trigger evaluation. So a backlog of expired tasks can halt
/// unrelated work again and again. Claims fail their `try` silently and yield
/// no task. The keyspace separation on [`lock_queue_for_pause`] exists to
/// prevent that same "silently stops dispatching" failure. This reaches it
/// from a different side.
///
/// Shared mode is sufficient because this lock's only job is to order the
/// enforcement against pause/resume. Two concurrent enforcements need no mutual
/// exclusion: they act on distinct tasks and already serialize on the task rows
/// they lock.
///
/// # Why blocking, not `try`
///
/// [`try_lock_queue_for_claim`] runs *after* the claim query, so it must never
/// wait. This lock is the **first** statement in the enforcement transaction,
/// before any row lock. So it cannot participate in a wait cycle. See the
/// lock-ordering note in `enforce_activity_timeout`. It must really wait. A
/// bail-out on contention skips the pause re-check. The enforcement then
/// proceeds against a stale snapshot. It terminally fails the very task that an
/// in-flight pause protects.
///
/// # Errors
///
/// Database errors.
#[cfg(feature = "db")]
pub async fn lock_queue_for_timeout_recheck(
    conn: &mut AsyncPgConnection,
    queue_name: &str,
) -> HarvestResult<()> {
    use diesel_async::RunQueryDsl;

    let (key1, key2) = queue_lock_keys(queue_name);
    diesel::sql_query(lock_queue_for_timeout_recheck_query())
        .bind::<diesel::sql_types::Integer, _>(key1)
        .bind::<diesel::sql_types::Integer, _>(key2)
        .execute(conn)
        .await?;
    Ok(())
}

/// How much of the fleet a hold is **actually** in effect on.
///
/// Distinct from the stored `scope_shard_id`. That column records the *intent*
/// of the request that wrote a row (`NULL` = "this was a fleet-wide request"),
/// not its coverage. A fleet-wide pause can reach only some shards. It then
/// stores `scope_shard_id = NULL` on the shards it reached. It stores **no row
/// at all** on the shards it missed. A read of intent alone shows such a
/// partial hold as fleet-wide. The missed shards meanwhile keep dispatching
/// (issue #619 review).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PauseCoverage {
    /// Every expected shard holds the queue.
    Fleet,
    /// A fleet-wide request wrote at least one row, but at least one expected
    /// shard does not hold the queue. So part of the fleet still dispatches.
    PartialFleet,
    /// Every row is explicitly shard-scoped, and they do not cover the fleet.
    Shard,
}

impl PauseCoverage {
    /// Operator-facing label, shared by the API and the Vantage banner so the
    /// two surfaces cannot describe the same hold differently.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Fleet => "fleet-wide",
            Self::PartialFleet => "fleet-wide (partially applied)",
            Self::Shard => "shard-scoped",
        }
    }

    /// True when part of the fleet is still dispatching despite the hold.
    #[must_use]
    pub const fn is_incomplete_fleet(self) -> bool {
        matches!(self, Self::PartialFleet)
    }
}

/// Classify a hold's real coverage from the shards that actually hold it.
///
/// `holding_shards` are the shards whose read returned a row for this queue.
/// `scope_shard_ids` are the stored scopes of those rows. `expected_shards` is
/// the shard set the fleet is *supposed* to span
/// (`shard_fanout::expected_shards`: every pool plus every shard the router
/// knows about).
///
/// **Expected** shards, not merely inspected ones, is the safe direction. A
/// shard that could not be read can still be dispatching. So a hold is `Fleet`
/// only when every shard the fleet is supposed to have is known to hold it.
///
/// An empty `expected_shards` (a degenerate configuration) falls back to the
/// holding set, so a single-shard deployment still reads `Fleet`.
#[must_use]
pub fn classify_pause_coverage(
    holding_shards: &[i32],
    scope_shard_ids: &[Option<i32>],
    expected_shards: &[i32],
) -> PauseCoverage {
    let covers_fleet = if expected_shards.is_empty() {
        true
    } else {
        expected_shards
            .iter()
            .all(|shard| holding_shards.contains(shard))
    };
    if covers_fleet {
        PauseCoverage::Fleet
    } else if scope_shard_ids.iter().any(Option::is_none) {
        PauseCoverage::PartialFleet
    } else {
        PauseCoverage::Shard
    }
}

/// Whether a queue pause suspends enforcement of `reason` for a held task.
///
/// Only the **relative** `schedule_to_start` timer is suspended (AC4): a task
/// must not be timed out merely because it waited while its queue was paused.
///
/// Deliberately *not* suspended:
///
/// - **`ScheduleToClose`** — issue #619 scopes this out explicitly: a paused
///   queue does not extend an absolute wall-clock SLA deadline (#378).
///   Crediting paused time back to `schedule_to_close` is a follow-up.
/// - **`Heartbeat` / `StartToClose`** — both apply only to `RUNNING` rows. A
///   pause holds *dispatch*; already-dispatched work runs to completion on its
///   own merits, so these stay pause-blind (mirroring the per-execution pause
///   contract in `crate::timeout`).
#[cfg(feature = "db")]
#[must_use]
pub const fn queue_pause_suppresses_timeout(
    reason: &crate::timeout::TimeoutReason,
    queue_paused: bool,
) -> bool {
    queue_paused && matches!(reason, crate::timeout::TimeoutReason::ScheduleToStart)
}

/// The resume-time `scheduled_at` shift — the single most load-bearing query in
/// this feature.
///
/// Without it, a thaw times out the *entire* held backlog at once when that
/// backlog waited past its `schedule_to_start`. AC4/AC5 exist to prevent
/// exactly that failure.
///
/// For each `PENDING` row on the queue that is actually **due**
/// (`scheduled_at <= clock_timestamp()`), it credits back only the time the row
/// was genuinely held:
///
/// ```text
/// scheduled_at += clock_timestamp() - GREATEST(scheduled_at, paused_at)
/// ```
///
/// - **Eligible before the pause** (`scheduled_at <= paused_at`): shifted by the
///   full pause span, so pre-pause waiting is preserved. A task that had waited
///   100 s still shows 100 s of wait after the thaw.
/// - **Became eligible mid-pause** (`paused_at < scheduled_at <= now`):
///   collapses to the thaw instant — it was held from the moment it became due,
///   so it accrues no wait.
/// - **Not yet due** (`scheduled_at > now`, e.g. a retry backoff): excluded
///   entirely. It was never held, so its backoff must not be extended.
///
/// # Why `clock_timestamp()` and not `NOW()`
///
/// `NOW()` is `transaction_timestamp()`, frozen at **transaction start**. Here
/// that is *before* the advisory-lock wait, the pause-row `DELETE` and this
/// bulk `UPDATE`. But the hold stays in force until this transaction
/// **commits**. So a frozen `NOW()` silently omits the resume's own duration
/// from the credit. A direct measurement: a task waited 100 s before the pause,
/// and the resume spent 3 s on the lock. With `NOW()`, the task shows 103 s of
/// wait the instant the queue thaws. So a task whose `schedule_to_start` is
/// shorter than the resume's runtime times out right after the thaw. This
/// query exists to prevent exactly that AC3/AC4 failure ("never failed *solely
/// because* of the pause"). The same frozen value also kept rows that fell due
/// *during* the transaction out of the predicate.
///
/// `clock_timestamp()` is volatile and re-read per row at execution time. So
/// it sees the real wall clock after the lock is held. It advances as the scan
/// progresses.
///
/// It is still a **lower bound**, by design. The true thaw instant is commit
/// time, and no in-statement expression can know it. So each row is
/// under-credited by at most the remaining duration of this `UPDATE`. That is
/// the *safe* direction. Over-credit past commit time pushes `scheduled_at`
/// into the future and makes the task **un**claimable, the inverse of AC5.
///
/// The result stays provably `<= clock_timestamp()` at evaluation, hence
/// `<= commit time`, in every branch. `RUNNING` rows are untouched, and
/// `schedule_to_close_at` is untouched so the absolute deadline keeps ticking.
///
/// # Why the partition is `created_at` vs `paused_at`
///
/// This pass owns the rows that had already accrued wait *before* the hold
/// began. [`resume_shift_late_arrivals_query`] owns the rows created *during*
/// it. The two predicates partition the due `PENDING` rows, so no row shifts
/// twice. A second application of this *relative* `+=` formula erases the
/// pre-pause wait that it exists to preserve.
///
/// The split is **semantic, not temporal** (issue #619). An earlier revision
/// split on `created_at < transaction_timestamp()` ("existed when this resume
/// began"). That is subtly wrong. `created_at` records when the enqueuing
/// `INSERT` *executed*. Whether this pass can *see* the row depends on when
/// that enqueuing transaction **committed**. Take a row inserted before the
/// resume began but committed after this statement took its snapshot. It
/// matched neither predicate. This pass could not see it, and the late pass
/// excluded it by its own `created_at` test. So it thawed with its entire held
/// wait uncredited. A schedule-to-start failure could then follow at once.
///
/// A comparison against `paused_at` removes visibility from the question.
/// `created_at` and `paused_at` are both fixed values on rows that already
/// exist. So the side of a row is the same, whichever statement sees it first.
/// The pass that owns a late-visible row still credits it correctly. It is
/// also the more meaningful split. It asks "did this task accrue wait before
/// the hold, or was it held from birth?". The two formulas encode exactly that.
///
/// `created_at` is **nullable**: a pre-`20260619000000` `PENDING` row has none.
/// In SQL a comparison against `NULL` is `NULL`, not `false`. Without the
/// explicit `IS NULL` arm, such a row matches *neither* predicate. Then
/// *neither* pass credits it. It belongs on this side. A row with no
/// `created_at` predates that migration, so it predates any pause.
///
/// # Why a total partition was still not enough (issue #619)
///
/// The partition above is total and visibility-independent. Every due
/// `PENDING` row matches exactly one predicate, whenever it becomes visible.
/// That is a statement about the *predicates*. An earlier revision wrongly read
/// it as a statement about **coverage**. Take a row on *this* pass's side that
/// *this* pass's snapshot cannot see. This pass never shifts it. A later pass
/// cannot pick it up either. The second pass used to test the same
/// `created_at` predicate, so it excluded the row by construction.
///
/// An example: an enqueue `INSERT` *executes* before the pause, so
/// `created_at < paused_at` puts it on this side. Its transaction *commits*
/// after this statement's snapshot. This pass cannot see it, and pass 2
/// rejected it. So it thawed with its entire held wait uncredited, and a
/// `schedule_to_start` failure could follow at once. The `paused_at` partition
/// removes the dependence on visibility for *which side a row belongs to*. It
/// cannot remove the dependence on visibility for *whether a pass sees the row
/// at all*.
///
/// The fix derives the exclusion of pass 2 from the rows this pass **actually
/// shifted**, not from a predicate. This statement now returns their ids
/// (`RETURNING id`). [`resume_shift_late_arrivals_query`] covers *both* sides
/// of the partition for every row not in that set. Coverage across the two
/// snapshots is then total by construction, for any commit order.
///
/// [`resume_shift_late_arrivals_query`] documents the irreducible residual: a
/// row that commits after the snapshot of pass 2.
///
/// # Re-pend paths
///
/// A row can also change **state** between the two snapshots. This pass cannot
/// see a `RUNNING` row re-pended after its snapshot. Pass 2 now covers every
/// such row, on either side of the partition. So this is no longer a
/// correctness argument. Still, each reachable re-pend path *refreshes*
/// `scheduled_at` to the re-pend instant, so it accrued no held time anyway.
/// These paths are `crate::queue::requeue_for_retry`, the `crate::poison_pill`
/// reclaim and `primary_repend_workflow_task`. [`release_claim_if_queue_paused`]
/// is the one path that re-pends and *preserves* a pre-pause `scheduled_at`.
/// It does so on purpose, so a released claim keeps the wait it had accrued.
/// Pass 2 now credits it correctly. It no longer relies on the path being
/// unreachable in this window.
///
/// # Why the ids round-trip through this process (issue #619)
///
/// The shifted ids load here and go back to pass 2 as an array bind. That is
/// linear in the released backlog. Application memory holds one `Uuid` per
/// shifted task. The array then goes back over the wire, and `unnest`
/// rebuilds it on the server. All of this happens while this transaction holds
/// the exclusive advisory lock of the queue.
///
/// A data-modifying CTE could capture the ids straight into a transaction-scoped
/// `CREATE TEMP TABLE ... ON COMMIT DROP` scratch table and remove that cost.
/// This module does not do that, because `CREATE TEMP TABLE` requires the
/// database-level `TEMPORARY` privilege. The migration does not grant it, and
/// preflight does not validate it. Revoking `TEMPORARY` is a recognised
/// hardening step. Such a deployment passes every current permission check.
/// It then fails EVERY resume, which rolls back the pause deletion. The queue
/// stays frozen at the exact moment an operator tries to thaw it. A rare scale
/// problem is a better failure than a deterministic, silent, config-dependent
/// break of the core operation of this feature.
///
/// The memory cost is real and is tracked as a follow-up. A proper fix needs a
/// handoff that the existing migration grants cover, such as a real table keyed
/// by a per-resume token. It must not need a privilege the deployment may lack.
#[must_use]
pub const fn resume_shift_scheduled_at_query() -> &'static str {
    "UPDATE harvest_task_queue \
     SET scheduled_at = scheduled_at + (clock_timestamp() - GREATEST(scheduled_at, $2)) \
     WHERE queue_name = $1 \
       AND state = 'PENDING' \
       AND scheduled_at <= clock_timestamp() \
       AND (created_at IS NULL OR created_at < $2) \
     RETURNING id"
}

/// Second pass of the resume shift: credit rows **enqueued during the thaw
/// itself** (issue #619 review).
///
/// The hold stays in force until the `resume_queue` transaction *commits*. The
/// snapshot of a concurrent claimer still sees the uncommitted pause row, so
/// it still skips the queue. A task enqueued after the snapshot of
/// [`resume_shift_scheduled_at_query`] is therefore really held. That
/// statement can never see it. Without this pass, the task thaws with an
/// uncredited `scheduled_at`. The `schedule_to_start` scanner can then time it
/// out for time it spent held. The primary shift exists to prevent exactly
/// that AC3/AC4 failure.
///
/// # Why this pass covers *both* sides of the partition (issue #619)
///
/// It is not enough for this pass to own only the rows created during the
/// hold. Take a row on the side of the *primary* pass (`created_at < paused_at`)
/// that its snapshot could not see. That is an enqueue that executed before the
/// pause but committed after the primary shift ran. With that narrower scope,
/// neither pass credits it, and it thaws with its whole held wait uncredited.
/// See the "total partition" note on [`resume_shift_scheduled_at_query`].
///
/// So this statement applies, via a `CASE`, the **same per-side formula** that
/// the partition defines. It derives its exclusion from the ids the primary
/// pass actually shifted (`$3`), not from a `created_at` predicate:
///
/// - `created_at IS NULL OR created_at < $2` → the *relative* `+=` delta of the
///   primary pass. It preserves the wait the row accrued before the hold.
/// - otherwise → `clock_timestamp()` (absolute). A row created during the hold
///   has no pre-pause wait to preserve. It was held from the moment it became
///   due, so it accrues none.
///
/// The exclusion by id, not by predicate, makes coverage total for *any*
/// commit order. A row the primary pass shifted is in `$3`, so this pass skips
/// it. A second application of the relative formula erases the pre-pause wait
/// it exists to preserve. This pass credits every other due `PENDING` row,
/// whichever side it belongs to. An empty `$3` means the primary pass shifted
/// nothing. It correctly leaves every row to this pass.
///
/// The exclusion is `NOT EXISTS (… unnest …)`, not `id <> ALL(<array>)`, by
/// design. The latter is a per-row linear scan of the array. The former lets
/// the planner build a hash anti-join. So the cost is O(rows + ids), not
/// O(rows × ids).
///
/// # Irreducible residual
///
/// This narrows the uncredited window from "the whole bulk `UPDATE`" to "this
/// final, small statement". It cannot close it. A row committed after *this*
/// statement's snapshot and before this transaction commits is still held. It
/// still gets no credit. The remaining duration of this statement bounds the
/// under-credit (microseconds for a queue with no late arrivals). That is the
/// same safe direction as the lower-bound credit of the primary pass.
///
/// A full close needs **enqueue** to serialize behind the advisory lock of the
/// queue. That puts a queue-scoped serialization point on the hot enqueue path
/// for every task on every queue, paused or not. The design notes of this
/// module reject that exact trade for the claim path. A sub-millisecond credit
/// is not worth that cost. So this documents the residual and does not remove
/// it.
///
/// The statement returns the ids it shifts. The resume publishes a dispatch
/// hint for each one after commit (issue #2066). A resume with no dispatch
/// channel runs [`resume_shift_late_arrivals_count_query`] instead.
///
/// Binds: `$1` = queue name, `$2` = the hold's `paused_at`, `$3` = the ids the
/// primary pass reported shifting.
#[must_use]
pub const fn resume_shift_late_arrivals_query() -> &'static str {
    late_arrivals_sql!(" RETURNING id")
}

/// The same statement as [`resume_shift_late_arrivals_query`], without the
/// `RETURNING` clause.
///
/// The late pass covers every due task created during the pause. After a long
/// pause that set can be the whole backlog. A resume with no dispatch channel
/// needs only the row count, so it must not load the ids (issue #2066).
#[must_use]
pub const fn resume_shift_late_arrivals_count_query() -> &'static str {
    late_arrivals_sql!("")
}

/// The `GET /admin/queues/paused` read, and the Vantage Workers banner's source.
///
/// # Why a correlated count rather than a grouped join (issue #619)
///
/// The obvious shape is `LEFT JOIN (SELECT queue_name, COUNT(*) … GROUP BY
/// queue_name)`. It aggregates the **entire** `PENDING` backlog of every queue
/// on the shard. Only then does it join the result to the tiny pause table.
/// Postgres cannot reliably push the paused queue names down into that grouped
/// subquery. So the cost scales with the backlog of the whole shard, not with
/// the paused queues.
///
/// That is backwards for this query. It is a **diagnostic read**. The
/// management endpoint and the Workers-page banner both run it per shard.
/// Operators read it during exactly the incident where the backlog is largest.
/// A "what is on hold?" view that slows (or times out) with unrelated pending
/// work defeats its purpose.
///
/// The correlated `COUNT(*)` runs once per **paused queue** instead. The row
/// count of `harvest_queue_pauses` bounds that set, normally to a handful. Each
/// count is a selective `(queue_name, state)` lookup, not a full aggregation.
/// It also drops the `COALESCE`. A correlated count returns `0` for a paused
/// queue with no held work. So such a queue still lists and does not vanish.
/// That was the only reason for the `LEFT JOIN`.
#[must_use]
pub const fn list_paused_queues_query() -> &'static str {
    "SELECT p.queue_name, p.reason, p.paused_by, p.paused_at, p.scope_shard_id, \
            (SELECT COUNT(*) FROM harvest_task_queue t \
             WHERE t.queue_name = p.queue_name AND t.state = 'PENDING') AS held_task_count \
     FROM harvest_queue_pauses p \
     ORDER BY p.paused_at ASC, p.queue_name ASC"
}

/// Count of `PENDING` tasks currently held on a queue (`$1` = queue name).
#[must_use]
pub const fn held_task_count_query() -> &'static str {
    "SELECT COUNT(*) AS count FROM harvest_task_queue \
     WHERE queue_name = $1 AND state = 'PENDING'"
}

/// One claimable task id on a just-resumed queue, used only as the payload for
/// the resume's wake notification (`$1` = queue name).
///
/// `LIMIT 1` because the notification is a *doorbell*, not a work assignment.
/// [`crate::notify::QueueListener`] hands the payload back to the poll loop,
/// which ignores it and re-polls the queue normally. So one indexed row is
/// enough. It deliberately does **not** take its id from the `RETURNING id`
/// output of the shift statements. That output feeds the pass 2 exclusion and
/// the dispatch hints. A resume with no dispatch channel loads no late ids.
/// So this query reads the one doorbell id by itself.
///
/// The predicate mirrors the due-row filter of the shift passes. So the id is
/// a row this resume just made claimable, not a retry that is still not due.
#[must_use]
pub const fn resumed_queue_notify_task_query() -> &'static str {
    "SELECT id FROM harvest_task_queue \
     WHERE queue_name = $1 \
       AND state = 'PENDING' \
       AND scheduled_at <= clock_timestamp() \
     LIMIT 1"
}

/// Ring the queue's `LISTEN`/`NOTIFY` doorbell so parked workers re-poll as
/// soon as this resume commits (issue #619).
///
/// Must be called **inside** the resume transaction. The call stages the wake
/// with the transaction id, and the sender sends it only after `COMMIT`
/// (issue #1796). Listeners therefore wake exactly when the hold lifts. An
/// earlier wake would still see the uncommitted pause row and skip the queue.
/// A resume that rolls back sends no wake.
///
/// Reuses [`crate::notify::notify_task_enqueued`] and adds no second channel or
/// payload shape. [`crate::notify::QueueListener`] **parses** the payload as
/// `NotifyPayload { task_id }` and surfaces a parse failure as an error. The
/// poll loop then logs and sleeps a full `poll_interval`. That is strictly
/// worse than sending nothing. A synthetic or nil id is a lie in a field typed
/// as a real task. A real id is honest and free (one indexed row). The sender
/// can still merge this wake with others on the queue into one nil-id wake
/// (issue #1796).
///
/// A queue that raced to empty between the shift and this lookup needs no
/// doorbell, so the lookup returning nothing is a silent no-op.
///
/// # Errors
///
/// Database errors.
#[cfg(feature = "db")]
pub async fn notify_resumed_queue(
    conn: &mut AsyncPgConnection,
    queue_name: &str,
) -> HarvestResult<()> {
    use diesel_async::RunQueryDsl;

    #[derive(diesel::QueryableByName)]
    struct IdRow {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: uuid::Uuid,
    }

    let rows: Vec<IdRow> = diesel::sql_query(resumed_queue_notify_task_query())
        .bind::<diesel::sql_types::Text, _>(queue_name)
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    if let Some(row) = rows.into_iter().next() {
        crate::notify::notify_task_enqueued(conn, queue_name, row.id).await?;
    }
    Ok(())
}

/// A currently-paused queue, as surfaced by the read API and the Vantage UI.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct PausedQueue {
    /// The paused task queue.
    pub queue_name: String,
    /// Operator-supplied human-readable reason for the hold.
    pub reason: String,
    /// Operator identity that applied the pause.
    pub paused_by: String,
    /// When the pause was applied.
    pub paused_at: chrono::DateTime<chrono::Utc>,
    /// `None` = fleet-wide (the default); otherwise the shard the operator
    /// scoped the pause to.
    pub scope_shard_id: Option<i32>,
    /// `PENDING` tasks currently held on this queue.
    pub held_task_count: i64,
}

/// Result of a pause request.
///
/// Idempotent: a re-pause of a paused queue is a success no-op with
/// `newly_paused == false`. It keeps the **original** pause provenance. This
/// mirrors the `newly_terminated` / `newly_resumed` convention from #504 / #609.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct PauseOutcome {
    /// The queue that is now paused.
    pub queue_name: String,
    /// `false` when the queue was already paused (no-op).
    pub newly_paused: bool,
    /// The effective reason (the original one on a re-pause).
    pub reason: String,
    /// The effective operator identity (the original one on a re-pause).
    pub paused_by: String,
    /// The effective pause instant (the original one on a re-pause).
    pub paused_at: chrono::DateTime<chrono::Utc>,
    /// `None` = fleet-wide (the default); otherwise the shard the operator
    /// scoped the pause to. The effective value (the original on a re-pause).
    pub scope_shard_id: Option<i32>,
    /// `PENDING` tasks held at the moment the pause was applied.
    pub held_task_count: i64,
}

/// Result of a resume request. Idempotent: resuming a queue that is not paused
/// is a success no-op with `newly_resumed == false`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ResumeOutcome {
    /// The queue that is now dispatching again.
    pub queue_name: String,
    /// `false` when the queue was not paused (no-op).
    pub newly_resumed: bool,
    /// Tasks whose `scheduled_at` was credited back the held time.
    ///
    /// The sum of both resume passes — the backlog that existed when the resume
    /// began plus anything enqueued while it ran. Pass 2 excludes the ids that
    /// pass 1 shifted, so this is an exact row count, never a double count.
    pub released_task_count: i64,
    /// How long the queue was held, in seconds (`0` on a no-op).
    pub paused_duration_secs: i64,
    /// The reason the hold was originally placed, echoed back on release.
    ///
    /// The resume deletes the pause row, so this is the last chance to recover
    /// the *why*. The resume response carries it, and so do the CLI output and
    /// the Vantage flash. An operator who closes an incident then sees exactly
    /// which hold was released. `None` on a no-op.
    pub released_reason: Option<String>,
    /// The operator who placed the hold being released. `None` on a no-op.
    pub released_paused_by: Option<String>,
}

#[cfg(feature = "db")]
#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

/// The wall clock read immediately after [`lock_queue_for_pause`] returns —
/// the instant the hold actually takes effect. See [`pause_clock_query`].
#[cfg(feature = "db")]
#[derive(diesel::QueryableByName)]
struct ClockRow {
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    now: DateTime<Utc>,
}

#[cfg(feature = "db")]
#[derive(diesel::QueryableByName)]
struct PauseRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    reason: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    paused_by: String,
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    paused_at: DateTime<Utc>,
}

/// The pause row as returned by the upsert, plus its own "did I insert this?"
/// signal. Carries every column the pause path echoes back, where [`PauseRow`]
/// carries only the provenance the resume path releases.
#[cfg(feature = "db")]
#[derive(diesel::QueryableByName)]
struct UpsertedPauseRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    queue_name: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    reason: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    paused_by: String,
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    paused_at: DateTime<Utc>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Integer>)]
    scope_shard_id: Option<i32>,
    /// `xmax = 0` on a freshly inserted tuple; non-zero when `DO UPDATE` fired
    /// against an existing row. The standard Postgres upsert discriminator.
    #[diesel(sql_type = diesel::sql_types::Bool)]
    newly_paused: bool,
}

/// The pause upsert.
///
/// A single upsert, not `INSERT ... DO NOTHING` + a fallback `SELECT`.
/// `DO NOTHING` does not lock the conflicting row. So a concurrent resume can
/// delete it between the two statements. The fallback `SELECT` then returns
/// zero rows. That is a `500`. Worse, the queue stays *unpaused*, and the
/// operator was told the pause failed.
///
/// The no-op `SET queue_name = ...` keeps the ORIGINAL provenance on a
/// re-pause and still takes the row lock. `xmax = 0` is the standard
/// discriminator for "this tuple was inserted, not updated".
///
/// # Why `paused_at` is stamped explicitly, not left to `DEFAULT NOW()`
///
/// The column defaults to `NOW()`, which is `transaction_timestamp()`. That
/// value is frozen at **transaction start**, *before* [`lock_queue_for_pause`]
/// waits. The hold takes effect only once that lock is held (strictly, at
/// commit). So a defaulted `paused_at` predates the effective hold by the whole
/// lock wait. That wait has no upper bound. It can queue behind a large
/// concurrent resume that shifts a big backlog.
///
/// Two consequences, both real:
///
/// - **Reported start and duration are overstated.** `GET /admin/queues/paused`
///   and the Vantage banner show `paused_at`. The resume response derives
///   `paused_duration_secs` from it. The `harvest_queue_paused_too_long` alert
///   exists so an operator can reason about *how long* a hold has been in
///   place. All of them read too long by the lock wait.
/// - **The resume shift over-credits.** `resume_shift_scheduled_at_query`
///   credits `clock_timestamp() - GREATEST(scheduled_at, paused_at)`. So an
///   understated `paused_at` credits an interval when the task was not held. It
///   erodes the pre-pause wait that the shift exists to preserve.
///
/// Note the *direction* of that second one. The `GREATEST` of the shift clamps
/// its result to `<= clock_timestamp()` in every branch. So an understated
/// `paused_at` never pushes `scheduled_at` into the future. It cannot make a
/// task **un**claimable (the AC5 inverse). It reduces the apparent wait, which
/// is the safe side for `schedule_to_start`. It still loses fidelity in
/// `paused_at`, the value the resume shift reads.
///
/// `paused_at` is therefore **bound** (`$5`). [`pause_clock_query`] reads it
/// right after the advisory lock is granted. It is not evaluated inline when
/// this statement runs; see below. It appears **only** in the `INSERT` half.
/// The `DO UPDATE` deliberately does not touch `paused_at`. So an idempotent
/// re-pause keeps the start time of the original hold.
///
/// # Why the stamp is taken at lock acquisition, not later (issue #619)
///
/// Earlier revisions read `clock_timestamp()` inline here. [`pause_queue`] ran
/// the [`held_task_count_query`] backlog scan *first*. So the stamp landed as
/// close to `COMMIT` as possible. The reasoning: claimers share neither the
/// advisory lock nor visibility of the uncommitted pause row. So they keep
/// dispatching until `COMMIT`. All time before it is then *unheld* time, which
/// a resume must not credit back.
///
/// The first half of that is no longer true. [`try_lock_queue_for_claim`] takes
/// the **shared** mode of the key that [`lock_queue_for_pause`] takes
/// exclusively. Once this transaction holds that lock, every claim on the
/// queue fails its `try` and gives the task back. So the queue is
/// **effectively held from lock acquisition**, not from `COMMIT`. A stamp after
/// the scan credits none of the scan interval. On a large `PENDING` backlog
/// that interval has no upper bound. A task with a short `schedule_to_start`
/// can then time out right after the thaw. The cause is time it spent blocked
/// by the pause transaction itself. The shift exists to prevent exactly that
/// AC3/AC4 failure.
///
/// Note the *direction*. The shift is
/// `scheduled_at += clock_timestamp() - GREATEST(scheduled_at, paused_at)`. A
/// **later** `paused_at` shrinks the credit. That under-credit causes spurious
/// timeouts, the dangerous side. An **earlier** one only grows the credit. The
/// `GREATEST` clamps the result to `<= clock_timestamp()` in every branch. So
/// it never pushes `scheduled_at` into the future or makes a task
/// **un**claimable (the AC5 inverse). A stamp at lock acquisition is thus the
/// correct instant *and* the safe direction.
///
/// One statement acquires the lock and the next reads the clock. They are not
/// together in one target list. Postgres does not guarantee evaluation order
/// within a select list. So a combined statement can read the clock *before*
/// the blocking lock call. That silently reintroduces the lock wait as
/// over-credit. Guarded by
/// `pause_stamps_paused_at_at_lock_acquisition_not_after_the_scan`.
///
/// The irreducible residue is now only `COMMIT` itself. The barrier can release
/// a claim between the stamp and commit. That claim is held but uncredited.
/// The remaining runtime of this transaction bounds it.
///
/// Binds: `$1` = queue name, `$2` = reason, `$3` = actor, `$4` = scope shard id,
/// `$5` = the lock-acquisition instant.
#[must_use]
pub const fn pause_upsert_query() -> &'static str {
    "INSERT INTO harvest_queue_pauses \
         (queue_name, reason, paused_by, scope_shard_id, paused_at) \
     VALUES ($1, $2, $3, $4, $5) \
     ON CONFLICT (queue_name) DO UPDATE \
         SET queue_name = harvest_queue_pauses.queue_name \
     RETURNING queue_name, reason, paused_by, paused_at, scope_shard_id, \
               (xmax = 0) AS newly_paused"
}

/// Read the wall clock at the instant the queue hold takes effect.
///
/// [`pause_queue`] runs it right after [`lock_queue_for_pause`] returns. It is
/// its own statement, so the blocking lock acquisition provably comes first.
/// It reads `clock_timestamp()` (volatile, real current time), not `NOW()`.
/// `NOW()` is frozen at transaction start, before the lock wait.
#[must_use]
pub const fn pause_clock_query() -> &'static str {
    "SELECT clock_timestamp() AS now"
}

/// Pause dispatch on `queue_name`.
///
/// Runs in one transaction so the insert and the held-task count agree. Held
/// tasks are left `PENDING` and untouched — a pause never mutates the queue.
///
/// # Errors
///
/// [`HarvestError::Config`] for a blank or oversized queue name or a blank
/// reason; otherwise a database error.
#[cfg(feature = "db")]
pub async fn pause_queue(
    conn: &mut AsyncPgConnection,
    queue_name: &str,
    reason: &str,
    actor: &str,
    scope_shard_id: Option<i32>,
) -> HarvestResult<PauseOutcome> {
    use diesel_async::RunQueryDsl;

    let queue_owned = validate_queue_name(queue_name)?;
    if reason.trim().is_empty() {
        return Err(HarvestError::Config(
            "a queue pause requires a non-empty reason".to_string(),
        ));
    }

    let reason_owned = reason.to_string();
    let actor_owned = actor.to_string();

    // Pinned `READ COMMITTED` for the same fresh-snapshot reason as
    // `resume_queue`; see the note there (issue #619). The advisory lock
    // establishes the snapshot of this transaction before it waits. Under
    // `REPEATABLE READ`, a pause queued behind a concurrent pause/resume then
    // reads pre-lock state for the rest of the transaction.
    let mut tx = conn.build_transaction().read_committed();
    Box::pin(tx.run::<PauseOutcome, HarvestError, _>(async |conn| {
        // Serialize against the `schedule_to_start` enforcer, so it cannot
        // fail a task on this queue (see `lock_queue_for_pause`).
        lock_queue_for_pause(conn, &queue_owned).await?;

        // The hold takes effect HERE, not at COMMIT. `try_lock_queue_for_claim`
        // takes the shared mode of the key just locked exclusively. So every
        // claim on this queue now fails its `try` and gives the task back.
        // Read the clock in its own statement, right after the lock. A resume
        // then credits back the whole rest of this transaction, including the
        // unbounded backlog scan below. See `pause_upsert_query` for why a
        // later stamp under-credits and can time a task out after the thaw.
        let clock: ClockRow = diesel::sql_query(pause_clock_query())
            .get_result(conn)
            .await?;

        let held: CountRow = diesel::sql_query(held_task_count_query())
            .bind::<diesel::sql_types::Text, _>(&queue_owned)
            .get_result(conn)
            .await?;

        // `paused_at` is bound from the lock-acquisition instant above, not
        // evaluated inline here — see `pause_upsert_query`.
        let row: UpsertedPauseRow = diesel::sql_query(pause_upsert_query())
            .bind::<diesel::sql_types::Text, _>(&queue_owned)
            .bind::<diesel::sql_types::Text, _>(&reason_owned)
            .bind::<diesel::sql_types::Text, _>(&actor_owned)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(scope_shard_id)
            .bind::<diesel::sql_types::Timestamptz, _>(clock.now)
            .get_result(conn)
            .await?;
        let newly_paused = row.newly_paused;

        Ok(PauseOutcome {
            queue_name: row.queue_name,
            newly_paused,
            reason: row.reason,
            paused_by: row.paused_by,
            paused_at: row.paused_at,
            scope_shard_id: row.scope_shard_id,
            held_task_count: held.count,
        })
    }))
    .await
}

/// Resume dispatch on `queue_name`, crediting held time back to `scheduled_at`.
///
/// Runs in one transaction, so the pause row delete and the backlog re-time are
/// atomic. No worker can see a thawed queue whose backlog still carries its
/// stale, pre-thaw `scheduled_at`. That lets the `schedule_to_start` scanner
/// kill the very work the pause protected.
///
/// # Errors
///
/// [`HarvestError::Config`] for a blank or oversized queue name; otherwise a
/// database error.
#[cfg(feature = "db")]
pub async fn resume_queue(
    conn: &mut AsyncPgConnection,
    queue_name: &str,
    actor: &str,
) -> HarvestResult<ResumeOutcome> {
    use diesel_async::RunQueryDsl;

    let queue_owned = validate_queue_name(queue_name)?;
    let _ = actor; // recorded by the caller's audit row; the table keeps no resume trail

    // Pinned `READ COMMITTED`, not the session default (issue #619).
    //
    // The advisory lock below is a `SELECT`. So it establishes the snapshot
    // of this transaction and *then* blocks on any in-flight pause. Under
    // `REPEATABLE READ` that snapshot is the only one of the transaction. The
    // pause then commits and releases the lock. The `DELETE` below still
    // cannot see the pause row it waited for. The resume reports a successful
    // `newly_resumed: false` no-op, and the queue stays held. The operator
    // believes the queue is thawed, and nothing dispatches.
    //
    // Under `READ COMMITTED` each statement takes a fresh snapshot. So the
    // post-lock `DELETE` sees the pause that committed during the wait. An
    // inherited level lets `default_transaction_isolation = repeatable read`
    // on the database or the role silently disable the guarantee from outside
    // this code. `build_transaction().read_committed()` emits the level on the
    // `BEGIN`, so it travels with the query. `queue::claim_task` pins the same
    // level for the same fresh-snapshot reason.
    // Ids both shift passes below thawed, collected so the dispatch hints for them are
    // published **after** this transaction commits (issue #1312). Publishing
    // from inside would name rows a channel reader still sees as held.
    let thawed: std::sync::Arc<std::sync::Mutex<Vec<uuid::Uuid>>> =
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let collector = std::sync::Arc::clone(&thawed);

    let mut tx = conn.build_transaction().read_committed();
    let outcome = Box::pin(tx.run::<ResumeOutcome, HarvestError, _>(async |conn| {
        // Same lock the pause path and the timeout enforcer take, so a
        // resume cannot interleave with either.
        //
        // LOCK ORDERING (load-bearing): advisory lock FIRST, then the
        // PENDING task rows the `scheduled_at` shift below updates. The
        // `schedule_to_start` enforcer takes the same two in the same order
        // (see the lock-ordering note in `timeout::enforce_activity_timeout`).
        // An inversion on either side creates an ABBA deadlock. Enforcement
        // holds a task row and waits on this advisory lock. This transaction
        // holds the advisory lock and waits on that task row. Postgres breaks
        // the deadlock by aborting one. That fails either the timeout pass or
        // the operator's resume.
        lock_queue_for_pause(conn, &queue_owned).await?;

        // Delete-and-return: one statement is both the "was it paused?" test
        // and the release. So two concurrent resumes cannot both claim to have
        // released the same hold.
        let deleted: Vec<PauseRow> = diesel::sql_query(
            "DELETE FROM harvest_queue_pauses WHERE queue_name = $1 \
                     RETURNING reason, paused_by, paused_at",
        )
        .bind::<diesel::sql_types::Text, _>(&queue_owned)
        .load(conn)
        .await?;

        let Some(row) = deleted.into_iter().next() else {
            return Ok(ResumeOutcome {
                queue_name: queue_owned,
                newly_resumed: false,
                released_task_count: 0,
                paused_duration_secs: 0,
                released_reason: None,
                released_paused_by: None,
            });
        };

        // The ids this pass shifts are what pass 2 excludes. The coverage
        // notes on both queries explain why a `created_at` predicate cannot
        // do that job.
        //
        // A server-side temp table keeps application memory O(1) in the
        // released backlog. It needs the `TEMPORARY` privilege, which a
        // hardened deployment may revoke. Then EVERY resume fails. So this code
        // does not use one. `resume_shift_scheduled_at_query` explains why.
        // A per-shard install passes `hints_wanted` but gives an unbound caller
        // no channel. Build the id set only when a publish can land.
        let publish_hints = crate::dispatch::hints_deliverable();
        let shifted: Vec<ShiftedTaskId> = diesel::sql_query(resume_shift_scheduled_at_query())
            .bind::<diesel::sql_types::Text, _>(&queue_owned)
            .bind::<diesel::sql_types::Timestamptz, _>(row.paused_at)
            .load(conn)
            .await?;
        let released = shifted.len();
        let shifted_ids: Vec<uuid::Uuid> = shifted.into_iter().map(|r| r.id).collect();
        // Only a deployment with a channel needs the thawed ids. Without one
        // the collector stays empty, so a resume of a large backlog does not
        // hold a second copy of every id it shifted.
        if publish_hints {
            collector
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .extend(shifted_ids.iter().copied());
        }

        // Second pass, LAST, so it sees the freshest snapshot. A task is still
        // held, because the DELETE has not committed and concurrent claimers
        // still see the pause row. The primary shift above may not see it.
        // The task was created during the hold, or by an enqueue before the
        // hold that only committed now. This pass covers BOTH sides of the
        // partition. It excludes exactly the rows the primary pass shifted.
        // So the two passes are disjoint by construction, for any commit
        // order: the counts sum exactly, and no row shifts twice. A
        // `created_at` test separates only some commit orders.
        // Load the ids only when a channel will publish them. The late pass can
        // cover the whole backlog created during a long pause (issue #2066).
        let late = if publish_hints {
            let late_shifted: Vec<ShiftedTaskId> =
                diesel::sql_query(resume_shift_late_arrivals_query())
                    .bind::<diesel::sql_types::Text, _>(&queue_owned)
                    .bind::<diesel::sql_types::Timestamptz, _>(row.paused_at)
                    .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(&shifted_ids)
                    .load(conn)
                    .await?;
            let count = late_shifted.len();
            // The late rows need a hint too. Their reference may sit in
            // release backoff.
            collector
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .extend(late_shifted.into_iter().map(|r| r.id));
            count
        } else {
            diesel::sql_query(resume_shift_late_arrivals_count_query())
                .bind::<diesel::sql_types::Text, _>(&queue_owned)
                .bind::<diesel::sql_types::Timestamptz, _>(row.paused_at)
                .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(&shifted_ids)
                .execute(conn)
                .await?
        };

        // Wake sleeping listeners (issue #619).
        //
        // AC5 promises held tasks are claimable *immediately* on resume. But
        // a worker with `notification_database_url` set parks in
        // `wait_for_notification(poll_interval)`. It re-polls only when a
        // `NOTIFY` arrives on the queue channel. Every held row was enqueued
        // *before* the pause. So its enqueue notification fired long ago and
        // was consumed (or missed) during the hold. A resume enqueues nothing
        // new. Without a notification here, the queue stays idle for up to a
        // full `poll_interval` after the thaw. That is invisible at the 500 ms
        // default. But `poll_interval` is configurable. A higher value is
        // exactly why an operator configures LISTEN/NOTIFY. So the
        // deployments most likely to tune it would sit idle longest.
        //
        // Staged INSIDE this transaction on purpose. The sender sends the
        // wake only after COMMIT (issue #1796), so listeners wake exactly
        // when the hold lifts. A worker woken earlier would still see the
        // uncommitted pause row and skip the queue. A rollback sends no wake.
        //
        // Skipped when nothing was released: there is no held backlog to
        // wake for, and a spurious wake would just burn a poll cycle.
        if released.saturating_add(late) > 0 {
            notify_resumed_queue(conn, &queue_owned).await?;
        }

        Ok(ResumeOutcome {
            queue_name: queue_owned,
            newly_resumed: true,
            released_task_count: i64::try_from(released.saturating_add(late)).unwrap_or(i64::MAX),
            paused_duration_secs: (Utc::now() - row.paused_at).num_seconds().max(0),
            released_reason: Some(row.reason),
            released_paused_by: Some(row.paused_by),
        })
    }))
    .await?;

    // Dispatch hints for the thawed backlog (issue #1312). The channel dedupes
    // by task id, so a row a reconcile sweep already published costs nothing.
    let ids = std::mem::take(
        &mut *thawed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    );
    crate::queue::record_pending_hints(conn, &ids).await;

    Ok(outcome)
}

/// Every currently-paused queue on this shard, with its held-task count.
///
/// # Errors
///
/// Database errors.
#[cfg(feature = "db")]
pub async fn list_paused_queues(conn: &mut AsyncPgConnection) -> HarvestResult<Vec<PausedQueue>> {
    use diesel_async::RunQueryDsl;

    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        queue_name: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        reason: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        paused_by: String,
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        paused_at: DateTime<Utc>,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Integer>)]
        scope_shard_id: Option<i32>,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        held_task_count: i64,
    }

    let rows: Vec<Row> = diesel::sql_query(list_paused_queues_query())
        .load(conn)
        .await?;

    Ok(rows
        .into_iter()
        .map(|r| PausedQueue {
            queue_name: r.queue_name,
            reason: r.reason,
            paused_by: r.paused_by,
            paused_at: r.paused_at,
            scope_shard_id: r.scope_shard_id,
            held_task_count: r.held_task_count,
        })
        .collect())
}

/// Names of every currently-paused queue on this shard.
///
/// The gauge sampler's source of truth — deliberately narrower (and cheaper)
/// than [`list_paused_queues`], which also counts held tasks.
///
/// # Errors
///
/// Database errors.
#[cfg(feature = "db")]
pub async fn paused_queue_names(conn: &mut AsyncPgConnection) -> HarvestResult<Vec<String>> {
    use diesel_async::RunQueryDsl;

    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        queue_name: String,
    }

    let rows: Vec<Row> =
        diesel::sql_query("SELECT queue_name FROM harvest_queue_pauses ORDER BY queue_name ASC")
            .load(conn)
            .await?;
    Ok(rows.into_iter().map(|r| r.queue_name).collect())
}

/// Whether `queue_name` is currently paused on this shard.
///
/// # Errors
///
/// Database errors.
#[cfg(feature = "db")]
pub async fn is_queue_paused(
    conn: &mut AsyncPgConnection,
    queue_name: &str,
) -> HarvestResult<bool> {
    use diesel_async::RunQueryDsl;

    let row: CountRow = diesel::sql_query(
        "SELECT COUNT(*) AS count FROM harvest_queue_pauses WHERE queue_name = $1",
    )
    .bind::<diesel::sql_types::Text, _>(queue_name)
    .get_result(conn)
    .await?;
    Ok(row.count > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "db")]
    use crate::timeout::TimeoutReason;

    #[test]
    fn validate_queue_name_accepts_ordinary_names() {
        assert!(validate_queue_name("payments").is_ok());
        assert!(validate_queue_name("email-workers").is_ok());
        assert!(validate_queue_name(&"q".repeat(MAX_QUEUE_NAME_LEN)).is_ok());
    }

    #[test]
    fn validate_queue_name_rejects_blank_and_oversized() {
        assert!(validate_queue_name("").is_err());
        assert!(validate_queue_name("   ").is_err());
        assert!(validate_queue_name("\t\n").is_err());
        assert!(validate_queue_name(&"q".repeat(MAX_QUEUE_NAME_LEN + 1)).is_err());
    }

    /// Issue #619 review: a partially-applied fleet-wide pause must not read as
    /// `fleet-wide`. The missed shards persist **no row**, so intent
    /// (`scope_shard_id = NULL`) alone cannot tell the two apart.
    #[test]
    fn coverage_is_derived_from_shards_that_actually_hold_the_queue() {
        // Fleet-wide request that reached both expected shards.
        assert_eq!(
            classify_pause_coverage(&[0, 1], &[None, None], &[0, 1]),
            PauseCoverage::Fleet
        );
        // Same stored intent, but shard 1 was missed -- it is still dispatching.
        assert_eq!(
            classify_pause_coverage(&[0], &[None], &[0, 1]),
            PauseCoverage::PartialFleet
        );
        // Deliberately shard-scoped holds are not "partially applied".
        assert_eq!(
            classify_pause_coverage(&[0], &[Some(0)], &[0, 1]),
            PauseCoverage::Shard
        );
        // Scoped holds that between them cover the fleet are effectively fleet.
        assert_eq!(
            classify_pause_coverage(&[0, 1], &[Some(0), Some(1)], &[0, 1]),
            PauseCoverage::Fleet
        );
        // Single-shard deployment: one row is the whole fleet.
        assert_eq!(
            classify_pause_coverage(&[0], &[None], &[0]),
            PauseCoverage::Fleet
        );
        // Degenerate empty expectation falls back to the holding set.
        assert_eq!(
            classify_pause_coverage(&[3], &[None], &[]),
            PauseCoverage::Fleet
        );
        // An unreachable shard counts as expected-but-not-holding: safe side.
        assert_eq!(
            classify_pause_coverage(&[0, 1], &[None, None], &[0, 1, 2]),
            PauseCoverage::PartialFleet
        );
    }

    #[test]
    fn coverage_labels_are_distinct_and_flag_the_incomplete_case() {
        assert_eq!(PauseCoverage::Fleet.label(), "fleet-wide");
        assert!(
            PauseCoverage::PartialFleet.label().contains("partially"),
            "an operator must be able to see the gap at a glance"
        );
        assert!(PauseCoverage::PartialFleet.is_incomplete_fleet());
        assert!(!PauseCoverage::Fleet.is_incomplete_fleet());
        assert!(!PauseCoverage::Shard.is_incomplete_fleet());
    }

    /// Surrounding whitespace is rejected, not trimmed. A queue named
    /// `"payments "` is reachable, because `EnqueueParams` stores names
    /// verbatim. A trim holds a *different, valid* queue and reports success.
    #[test]
    fn validate_queue_name_rejects_surrounding_whitespace() {
        for name in ["payments ", " payments", "\tpayments", "payments\n"] {
            let err = validate_queue_name(name)
                .expect_err("surrounding whitespace must be rejected, not trimmed");
            assert!(
                err.to_string().contains("whitespace"),
                "the message must name the problem; got {err}"
            );
        }
        // Interior whitespace is a legitimate (if unusual) queue name.
        assert!(validate_queue_name("two words").is_ok());
    }

    #[cfg(feature = "db")]
    #[test]
    fn suppression_is_scoped_to_schedule_to_start() {
        assert!(queue_pause_suppresses_timeout(
            &TimeoutReason::ScheduleToStart,
            true
        ));
        assert!(!queue_pause_suppresses_timeout(
            &TimeoutReason::ScheduleToClose,
            true
        ));
        assert!(!queue_pause_suppresses_timeout(
            &TimeoutReason::Heartbeat,
            true
        ));
        assert!(!queue_pause_suppresses_timeout(
            &TimeoutReason::StartToClose,
            true
        ));
    }

    #[cfg(feature = "db")]
    #[test]
    fn suppression_never_fires_for_an_unpaused_queue() {
        for reason in [
            TimeoutReason::ScheduleToStart,
            TimeoutReason::ScheduleToClose,
            TimeoutReason::Heartbeat,
            TimeoutReason::StartToClose,
        ] {
            assert!(!queue_pause_suppresses_timeout(&reason, false));
        }
    }

    #[test]
    fn resume_shift_sql_shape_is_bounded_and_pending_only() {
        let sql = resume_shift_scheduled_at_query();
        assert!(sql.contains("GREATEST(scheduled_at, $2)"));
        assert!(sql.contains("state = 'PENDING'"));
        assert!(sql.contains("scheduled_at <= clock_timestamp()"));
        // The absolute deadline must NOT be shifted (issue #619 out-of-scope).
        assert!(!sql.contains("schedule_to_close_at"));
    }

    /// The shift must measure the thaw with the **real** clock, not the frozen
    /// transaction timestamp.
    ///
    /// `NOW()` is `transaction_timestamp()`, fixed before the advisory-lock
    /// wait, the pause-row `DELETE`, and this `UPDATE`. Yet the hold stays in
    /// force until commit. So `NOW()` silently omits the resume's own runtime
    /// from the credit. A task whose `schedule_to_start` is shorter than the
    /// resume then times out the instant the queue thaws. This query exists to
    /// prevent that exact AC3/AC4 violation ("never failed *solely because* of
    /// the pause"). The fix is a one-word change that a later "simplification"
    /// can quietly undo, so this guard pins it.
    #[test]
    fn resume_shift_never_uses_the_frozen_transaction_clock() {
        let sql = resume_shift_scheduled_at_query();
        // The partition compares against the bound `paused_at`. So NO frozen
        // clock belongs anywhere in this query. That covers the credit
        // arithmetic, the due predicate and the partition. A frozen clock in
        // the WHERE half hides the commit-visibility bug, so the ban covers the
        // whole string.
        for frozen in ["NOW()", "transaction_timestamp()", "statement_timestamp()"] {
            assert!(
                !sql.contains(frozen),
                "the shift must read only the live clock and bound values, never \
                 the frozen clock ({frozen}); got:\n{sql}"
            );
        }
        let (_set_clause, where_clause) = sql
            .split_once(" WHERE ")
            .expect("the shift is an UPDATE ... WHERE");
        // Both the credit and the due-ness predicate must use the live clock, or
        // rows that fall due mid-transaction are silently skipped.
        assert_eq!(
            sql.matches("clock_timestamp()").count(),
            2,
            "both the SET credit and the due predicate must read the live clock; got:\n{sql}"
        );
        // The frozen clock's one legitimate use: partitioning off the rows the
        // late-arrivals pass owns, so neither pass shifts a row twice.
        assert!(
            where_clause.contains("created_at < $2)"),
            "the primary pass must exclude rows enqueued during the resume; got:\n{sql}"
        );
    }

    /// `paused_at` must be stamped from the live clock, not left to the column's
    /// `DEFAULT NOW()`.
    ///
    /// `NOW()` is frozen at transaction start, before `lock_queue_for_pause`
    /// waits. So a defaulted `paused_at` predates the effective hold by the
    /// whole lock wait. It overstates the reported start and duration, and the
    /// resume shift over-credits. It is the write-side mirror of the
    /// frozen-clock hazard in the resume shift.
    #[test]
    fn pause_upsert_stamps_paused_at_from_the_live_clock() {
        let sql = pause_upsert_query();
        let clock = pause_clock_query();
        // The clock read is its own statement, taken at lock acquisition, so
        // the live-clock requirement belongs there.
        assert!(
            clock.contains("clock_timestamp()"),
            "paused_at must be stamped from the live clock, read after the queue \
             lock is held, not left to the column's frozen DEFAULT NOW(); got:\n{clock}"
        );
        assert!(
            sql.contains("$5"),
            "the upsert must BIND that instant rather than re-reading a clock at \
             statement time; got:\n{sql}"
        );
        for frozen in ["NOW()", "transaction_timestamp()", "statement_timestamp()"] {
            assert!(
                !sql.contains(frozen) && !clock.contains(frozen),
                "neither the pause upsert nor the stamp read may use the frozen \
                 clock ({frozen}); got:\n{sql}\n{clock}"
            );
        }
        // Naming the column in the insert list is what overrides the DEFAULT.
        assert!(
            sql.contains("paused_at"),
            "paused_at must be named explicitly in the insert list; got:\n{sql}"
        );
    }

    /// An idempotent re-pause must preserve the ORIGINAL hold's start time, so
    /// the live-clock stamp above must appear only in the `INSERT` half.
    ///
    /// The runbook tells an operator to re-issue a pause to repair a
    /// `partial_fleet` hold. Without this guard, that re-pause resets
    /// `paused_at`. It erases how long the queue was really held. The resume
    /// shift then credits only from the re-pause. It silently under-credits
    /// the backlog it must protect.
    #[test]
    fn pause_upsert_does_not_restamp_paused_at_on_a_repause() {
        let sql = pause_upsert_query();
        let (insert_half, after_update) = sql
            .split_once("DO UPDATE")
            .expect("the pause upsert is an INSERT ... ON CONFLICT DO UPDATE");
        // Judge only the conflict SET clause. `RETURNING` sits after it and
        // legitimately names `paused_at` — the handler echoes the EFFECTIVE
        // (preserved) value.
        let (update_set, returning) = after_update
            .split_once("RETURNING")
            .expect("the pause upsert RETURNs the effective row");
        assert!(
            insert_half.contains("$5"),
            "the INSERT half must stamp the bound lock-acquisition instant; \
             got:\n{sql}"
        );
        assert!(
            !update_set.contains("paused_at"),
            "the DO UPDATE SET clause must not touch paused_at, or a re-pause \
             would erase the original hold's start time; got:\n{sql}"
        );
        assert!(
            !update_set.contains("clock_timestamp()") && !update_set.contains("$5"),
            "the DO UPDATE SET clause must not re-stamp the hold's start; \
             got:\n{sql}"
        );
        assert!(
            returning.contains("paused_at"),
            "the effective paused_at must still be returned so the handler \
             echoes the preserved value; got:\n{sql}"
        );
    }

    /// `paused_at` must be stamped at **lock acquisition**, before the backlog
    /// scan.
    ///
    /// A scan-first order lands the stamp as close to `COMMIT` as possible. It
    /// assumes that claimers share neither [`lock_queue_for_pause`]'s advisory
    /// lock nor visibility of the uncommitted pause row, and so keep dispatching
    /// until `COMMIT`. That premise is false. [`try_lock_queue_for_claim`] takes
    /// the **shared** mode of the very key the pause takes exclusively. So the
    /// queue is effectively held from the instant the lock is granted. Stamping
    /// after an unbounded `COUNT(*)` therefore credits none of that held
    /// interval back on resume. A task with a short `schedule_to_start` can then
    /// time out immediately after the thaw, for time that the pause transaction
    /// itself blocked it.
    ///
    /// Three things are pinned, because breaking any one of them silently
    /// restores the old behaviour:
    ///
    /// 1. the clock read comes **after** the lock (order within `pause_queue`),
    /// 2. it comes **before** the scan,
    /// 3. the upsert **binds** `paused_at` rather than evaluating
    ///    `clock_timestamp()` inline, which would re-read it at statement time.
    ///
    /// Asserted on the source because the ordering *is* the invariant. The DB
    /// test `pause_credits_the_backlog_scan_interval` covers the wall-clock
    /// consequence. It makes the scan slow deterministically with an
    /// `ACCESS EXCLUSIVE` lock on `harvest_task_queue`. Mirrors the
    /// file-reading guards in `migration_hygiene`/`ci_run_coverage`.
    #[test]
    fn pause_stamps_paused_at_at_lock_acquisition_not_after_the_scan() {
        let src = std::fs::read_to_string(
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/queue_pause.rs"),
        )
        .expect("this file must be readable");

        // Narrow to `pause_queue`'s body: the doc comments above it name every
        // query, so a whole-file search would pass vacuously.
        let from_fn = src
            .find("pub async fn pause_queue(")
            .map(|at| &src[at..])
            .expect("pause_queue must exist");
        let body = from_fn
            .find("/// Resume dispatch on `queue_name`")
            .map(|at| &from_fn[..at])
            .expect("pause_queue must be followed by the resume fn's doc comment");

        let lock_at = body
            .find("lock_queue_for_pause(conn")
            .expect("pause_queue must take the queue advisory lock");
        let clock_at = body
            .find("pause_clock_query()")
            .expect("pause_queue must read the clock at lock acquisition");
        let scan_at = body
            .find("held_task_count_query()")
            .expect("pause_queue must scan the held backlog");

        assert!(
            lock_at < clock_at,
            "the clock must be read AFTER the advisory lock is granted, or the \
             stamp predates the hold by the whole lock wait (lock at {lock_at}, \
             clock at {clock_at})"
        );
        assert!(
            clock_at < scan_at,
            "the clock must be read BEFORE the backlog COUNT(*): the queue is \
             already held once the exclusive lock is granted (round-18's shared \
             try-lock barrier declines every claim), so a stamp taken after an \
             unbounded scan credits none of that held interval back on resume and \
             can spuriously schedule-to-start-fail a task after the thaw (clock at \
             {clock_at}, scan at {scan_at})"
        );

        let upsert = pause_upsert_query();
        assert!(
            upsert.contains("VALUES ($1, $2, $3, $4, $5)"),
            "paused_at must be BOUND from the lock-acquisition instant, not \
             evaluated inline (which would re-read the clock at statement time and \
             silently restore the post-scan stamp): {upsert}"
        );
        assert!(
            !upsert.contains("clock_timestamp()"),
            "the pause upsert must not call clock_timestamp() at all: {upsert}"
        );
        assert!(
            pause_clock_query().contains("clock_timestamp()")
                && !pause_clock_query().contains("NOW()"),
            "the stamp must come from the volatile clock, not the frozen \
             transaction clock: {}",
            pause_clock_query()
        );
    }

    /// The two passes must be made disjoint by the **ids the first pass
    /// actually shifted**, never by re-testing `created_at`.
    ///
    /// A `created_at` predicate on pass 2 leaves a coverage hole. Take a row on
    /// pass 1's side that pass 1's snapshot could not see, because its enqueue
    /// committed late. That very predicate excludes it from pass 2, so neither
    /// pass credits it. Excluding by id instead makes coverage total for any
    /// commit order. It still guarantees that no row is shifted twice. Pass 1's
    /// `+=` credit is *relative*, so a second application would erase the
    /// pre-pause wait it exists to preserve and double-count
    /// `released_task_count`.
    #[test]
    fn the_second_resume_pass_excludes_by_shifted_id_not_created_at() {
        let primary = resume_shift_scheduled_at_query();
        let late = resume_shift_late_arrivals_query();

        assert!(
            primary.contains("RETURNING id"),
            "pass 1 must report the rows it shifted, or pass 2 has nothing sound \
             to exclude by; got:\n{primary}"
        );

        let (_, late_where) = late
            .split_once(" WHERE ")
            .expect("the second pass is an UPDATE ... WHERE");
        assert!(
            !late_where.contains("created_at"),
            "pass 2 must NOT re-test created_at in its WHERE clause -- that is \
             exactly what excluded the late-committing pre-pause row it now has \
             to cover (round-19 P2); got:\n{late}"
        );
        assert!(
            late.contains("unnest($3::uuid[])") && late.contains("NOT EXISTS"),
            "pass 2 must exclude by the shifted-id set; got:\n{late}"
        );
        assert!(
            !late.contains("<> ALL"),
            "`<> ALL` is a per-row linear scan; the NOT EXISTS spelling lets the \
             planner hash the id set; got:\n{late}"
        );
        // Same queue, same PENDING/due gate on both sides, or a row could leak
        // out of one pass without the other picking it up.
        for sql in [primary, late] {
            assert!(sql.contains("queue_name = $1"), "got:\n{sql}");
            assert!(sql.contains("state = 'PENDING'"), "got:\n{sql}");
            assert!(
                sql.contains("scheduled_at <= clock_timestamp()"),
                "got:\n{sql}"
            );
        }
    }

    /// Pass 2 must report the ids it shifts, so the resume can publish a
    /// dispatch hint for them after commit (issue #2066).
    #[test]
    fn the_late_resume_pass_returns_the_ids_it_shifts() {
        let late = resume_shift_late_arrivals_query();
        assert!(
            late.starts_with("UPDATE harvest_task_queue") && late.contains("RETURNING id"),
            "pass 2 must return the shifted ids; got:\n{late}"
        );
    }

    /// A resume with no dispatch channel must not load the late ids. The pass
    /// can cover the whole backlog created during a long pause (issue #2066).
    #[test]
    fn the_count_only_late_pass_returns_no_rows_and_shares_the_id_pass_body() {
        let with_ids = resume_shift_late_arrivals_query();
        let count_only = resume_shift_late_arrivals_count_query();
        assert!(
            !count_only.contains("RETURNING"),
            "the count-only pass must not return rows; got:\n{count_only}"
        );
        assert_eq!(
            with_ids.strip_suffix(" RETURNING id"),
            Some(count_only),
            "the two passes must differ only in the RETURNING clause"
        );
    }

    /// The partition must be **total**, and `created_at` is nullable (a
    /// pre-`20260619000000` `PENDING` row has none). In SQL a comparison against
    /// `NULL` yields `NULL`, not `false`. So a bare `<` / `>=` split leaves such
    /// a row matching *neither* pass, and neither pass credits it. That is a
    /// silent regression against the pre-partition query. That query had no
    /// `created_at` predicate at all and shifted the row. It belongs to the
    /// primary pass. No `created_at` means it was enqueued before this resume.
    #[test]
    fn the_partition_is_total_for_a_null_created_at() {
        let primary = resume_shift_scheduled_at_query();
        assert!(
            primary.contains("(created_at IS NULL OR created_at < $2)"),
            "a legacy row with no created_at must still be credited; got:\n{primary}"
        );
        // Pass 2 now covers both sides, so its CASE needs the same NULL arm.
        // Otherwise a legacy row that pass 1 could not see falls to the
        // absolute arm. It then gets a full fresh budget and loses the wait it
        // accrued before the hold.
        let late = resume_shift_late_arrivals_query();
        assert!(
            late.contains("WHEN created_at IS NULL OR created_at < $2"),
            "pass 2's CASE must route a NULL created_at to the relative arm, \
             same as pass 1; got:\n{late}"
        );
    }

    /// The resume's wake doorbell must pick a genuinely **claimable** row.
    ///
    /// Neither resume pass shifts a future-scheduled retry backoff, by design.
    /// As the notify payload, such a row names a task the woken worker cannot
    /// claim. So the predicate mirrors the due-row filter of the shift passes.
    /// It stays `LIMIT 1`, because this is a doorbell, not a work assignment.
    #[test]
    fn resume_notify_payload_comes_from_a_claimable_row() {
        let sql = resumed_queue_notify_task_query();
        assert!(
            sql.contains("state = 'PENDING'"),
            "must be a held row: {sql}"
        );
        assert!(
            sql.contains("scheduled_at <= clock_timestamp()"),
            "must be DUE, so a future-scheduled retry is never named: {sql}"
        );
        assert!(
            sql.contains("queue_name = $1"),
            "must be scoped to the resumed queue: {sql}"
        );
        assert!(
            sql.contains("LIMIT 1"),
            "a doorbell needs exactly one id, not the whole backlog: {sql}"
        );
    }

    /// Pass 2 applies the SAME per-side formula the partition defines. So a row
    /// that it covers on pass 1's behalf gets exactly the credit pass 1 would
    /// give it.
    ///
    /// Both arms are load-bearing and independently falsifiable. The absolute
    /// arm alone gives a pre-pause row a full fresh `schedule_to_start` budget.
    /// That erases the wait it had already accrued. The relative arm alone
    /// credits a row born during the hold for wait it never accrued.
    #[test]
    fn second_pass_applies_the_correct_formula_per_side() {
        let sql = resume_shift_late_arrivals_query();
        let (set_clause, _where_clause) = sql
            .split_once(" WHERE ")
            .expect("the second pass is an UPDATE ... WHERE");

        assert!(
            set_clause.contains("ELSE clock_timestamp()"),
            "a row created during the hold has no pre-pause wait to preserve, so \
             its arm must be an absolute assignment; got:\n{sql}"
        );
        assert!(
            set_clause
                .contains("THEN scheduled_at + (clock_timestamp() - GREATEST(scheduled_at, $2))"),
            "a row created before the hold must get the SAME relative credit \
             pass 1 would have given it, or covering it here silently changes \
             its semantics; got:\n{sql}"
        );
        // The two arms must be exactly pass 1's assignment and the absolute one
        // -- not some third formula that drifted from the primary pass.
        let primary = resume_shift_scheduled_at_query();
        assert!(
            primary.contains(
                "SET scheduled_at = scheduled_at + (clock_timestamp() - GREATEST(scheduled_at, $2))"
            ),
            "pass 1's assignment changed; pass 2's relative arm must be updated \
             in lockstep; got:\n{primary}"
        );
        // Same out-of-scope invariant as the primary pass.
        assert!(!sql.contains("schedule_to_close_at"), "got:\n{sql}");
    }

    /// The claim-side barrier must be the *shared*, *try* variant
    /// of exactly the key `pause_queue` takes exclusively.
    ///
    /// Each of the three properties is load-bearing and independently
    /// falsifiable. Exclusive mode serializes every claim on a queue and
    /// defeats `SKIP LOCKED`. A blocking acquire is an ABBA deadlock against
    /// the advisory-then-rows order of `resume_queue`. The single-argument
    /// keyspace collides with the `concurrency_key` gate of `claim_task`. A
    /// collision there fails silently, because that gate uses the *try*
    /// variant. An unpaused queue then stops dispatching during the resume of
    /// an unrelated queue.
    #[test]
    fn claim_barrier_is_a_shared_try_lock_on_the_pause_key() {
        let sql = try_lock_queue_for_claim_query();
        assert!(
            sql.contains("pg_try_advisory_xact_lock_shared($1, $2)"),
            "the claim barrier must be the shared TRY lock in the two-argument \
             keyspace; got:\n{sql}"
        );
        for wrong in [
            // Exclusive: would serialize claims against each other.
            "pg_try_advisory_xact_lock($1",
            "pg_advisory_xact_lock($1",
            // Blocking: would deadlock against resume_queue (rows-then-lock here).
            "pg_advisory_xact_lock_shared($1",
            // Single-argument keyspace: collides with the concurrency_key gate.
            "pg_try_advisory_xact_lock_shared(hashtext($1))",
            // A 32-bit key slot is not enough for queue identity.
            "hashtext($2)",
        ] {
            assert!(
                !sql.contains(wrong),
                "claim barrier must not use `{wrong}`; got:\n{sql}"
            );
        }
        assert!(
            lock_queue_for_pause_query().contains("($1, $2)"),
            "the pause side must key on the same (key1, key2) pair, \
             or the shared/exclusive pair would not actually exclude"
        );
    }

    /// The timeout re-check takes the **shared** mode of the pause
    /// key, so it orders against pause/resume without blocking claims.
    ///
    /// Three independently falsifiable properties. Exclusive mode (what this
    /// used to be) blocks the shared claim barrier, so an enforcement
    /// transaction stalls all dispatch on an unpaused queue. The `try` variant
    /// would skip the pause re-check under contention and let a stale snapshot
    /// terminally fail a held task. The single-argument keyspace would collide
    /// with `claim_task`'s `concurrency_key` gate.
    #[test]
    fn timeout_recheck_lock_is_shared_and_blocking_on_the_pause_key() {
        let sql = lock_queue_for_timeout_recheck_query();
        assert!(
            sql.contains("pg_advisory_xact_lock_shared($1, $2)"),
            "the timeout re-check must take the shared, BLOCKING lock in the \
             two-argument keyspace; got:\n{sql}"
        );
        for wrong in [
            // Exclusive: blocks every claim's shared barrier for the whole
            // enforcement transaction, on a queue that is not even paused.
            "pg_advisory_xact_lock($1, $2)",
            "pg_try_advisory_xact_lock($1",
            // Try: would silently skip the pause re-check under contention.
            "pg_try_advisory_xact_lock_shared($1",
            // Single-argument keyspace: collides with the concurrency_key gate.
            "pg_advisory_xact_lock_shared(hashtext($1))",
        ] {
            assert!(
                !sql.contains(wrong),
                "the timeout re-check must not use `{wrong}`; got:\n{sql}"
            );
        }
        // Shared and exclusive only exclude each other if they name the same
        // key, so the two spellings must agree beyond the mode.
        assert!(
            sql.contains("($1, $2)") && lock_queue_for_pause_query().contains("($1, $2)"),
            "the re-check and the pause must key on the same (key1, key2) \
             pair, or they would not actually serialize"
        );
    }

    /// Neither `schedule_to_start` enforcement path may reach for
    /// the exclusive lock.
    ///
    /// Source-level because the hazard is a *call site* that picks the wrong
    /// helper. The exclusive function is still correct, and still used, for
    /// pause and resume themselves. The workflow path is the worse of the two:
    /// it holds the lock across `append_events`, the parent-close cascade and
    /// trigger evaluation.
    #[test]
    fn timeout_enforcement_never_takes_the_exclusive_queue_lock() {
        let src = include_str!("timeout.rs");
        assert!(
            !src.contains("queue_pause::lock_queue_for_pause("),
            "a timeout enforcement path is taking the EXCLUSIVE queue lock; that \
             blocks every claim's shared barrier for the whole transaction and \
             stalls dispatch on an unpaused queue -- use \
             `lock_queue_for_timeout_recheck` (shared)"
        );
        assert_eq!(
            src.matches("queue_pause::lock_queue_for_timeout_recheck(")
                .count(),
            2,
            "both schedule_to_start enforcement paths (activity and workflow) must \
             take the shared re-check lock"
        );
    }

    /// Both claim releases share one prefix, so the guard and the attempt
    /// restoration cannot drift apart. Only the paused variant consults the
    /// pause table.
    #[test]
    fn both_claim_releases_share_the_guard_and_restore_the_attempt() {
        let unconditional = release_claim_query();
        let paused = release_claim_if_queue_paused_query();
        for sql in [unconditional, paused] {
            assert!(
                sql.contains("attempt = GREATEST(attempt - 1, 0)"),
                "a released claim must give back the attempt it consumed (AC3); got:\n{sql}"
            );
            assert!(
                sql.contains("AND state = 'RUNNING'") && sql.contains("AND worker_id = $2"),
                "a release must only ever undo this worker's own claim; got:\n{sql}"
            );
        }
        assert!(
            paused.starts_with(unconditional),
            "the paused release must be the unconditional one plus its EXISTS tail, \
             so the shared prefix cannot drift"
        );
        assert!(
            paused.contains("harvest_queue_pauses"),
            "the paused variant must still consult the pause table"
        );
        assert!(
            !unconditional.contains("harvest_queue_pauses"),
            "the unconditional variant is used precisely when the pause table cannot \
             be trusted (a pause/resume is mid-commit), so it must not consult it"
        );
    }

    #[test]
    fn queue_pause_lock_uses_its_own_advisory_namespace() {
        let sql = lock_queue_for_pause_query();
        assert!(
            sql.contains("pg_advisory_xact_lock($1, $2)"),
            "the queue-pause lock must use the two-argument (key1, key2) form \
             so it cannot collide with the single-argument hashtext keyspace; got:\n{sql}"
        );
        // The single-argument shapes that WOULD collide with concurrency_key.
        for colliding in [
            "pg_advisory_xact_lock(hashtext($1))",
            "pg_advisory_xact_lock(hashtext($1)::bigint)",
        ] {
            assert!(
                !sql.contains(colliding),
                "must not use the shared single-argument keyspace ({colliding}); got:\n{sql}"
            );
        }
    }

    /// The advisory key identifies the *queue*, deterministically.
    #[test]
    fn queue_lock_keys_are_deterministic_and_name_sensitive() {
        assert_eq!(
            queue_lock_keys("payments"),
            queue_lock_keys("payments"),
            "the key must be a pure function of the queue name -- two processes \
             deriving different keys for one queue would lose mutual exclusion"
        );
        for (a, b) in [
            ("payments", "emails"),
            ("payments", "payment"),
            ("payments", "Payments"),
            ("payments", "payments2"),
            ("", "\0"),
        ] {
            assert_ne!(
                queue_lock_keys(a),
                queue_lock_keys(b),
                "distinct queue names must not share a lock key ({a:?} vs {b:?})"
            );
        }
    }

    /// **Both** key slots carry queue identity.
    ///
    /// This is the falsifiable core of the full-width key. An older key held the
    /// constant class id `619` in slot 1, and only slot 2 (a 32-bit `hashtext`)
    /// told queues apart. Identity was then 32 bits wide, and two unrelated
    /// queues collided with probability n²/2³³. A collision silently stalls the
    /// colliding queue's dispatch for the length of an unrelated pause or resume.
    ///
    /// Asserting slot 1 *varies* is what fails against that design: a constant
    /// first slot cannot vary no matter which names are sampled.
    #[test]
    fn queue_lock_keys_spend_both_slots_on_queue_identity() {
        let names = [
            "payments",
            "emails",
            "warehouse",
            "billing",
            "search",
            "webhooks",
        ];
        let firsts: std::collections::HashSet<i32> =
            names.iter().map(|n| queue_lock_keys(n).0).collect();
        let seconds: std::collections::HashSet<i32> =
            names.iter().map(|n| queue_lock_keys(n).1).collect();

        assert!(
            firsts.len() > 1,
            "key1 must carry queue identity, not a constant class id -- a fixed \
             slot halves the key to 32 bits and reintroduces the round-23 collision"
        );
        assert!(
            seconds.len() > 1,
            "key2 must carry queue identity too; got a constant"
        );
        assert_eq!(
            firsts.len(),
            names.len(),
            "no two of these names should share key1"
        );
    }

    /// This feature owns the two-argument advisory keyspace.
    ///
    /// [`queue_lock_keys`] spends all 64 bits on queue identity and reserves
    /// none for a class id. So queue-pause locks avoid a collision with another
    /// subsystem *only* because no other subsystem uses the two-argument form.
    /// Every other advisory-lock user in the engine is in the single-argument
    /// `bigint` keyspace. These are the concurrency gate of `queue::claim_task`,
    /// `mutex`, `wasm_store`, `admission_gate` and the registration lock of
    /// `scheduler`. This test walks the crate to keep that true. It turns a
    /// documented convention into a CI-enforced one.
    #[test]
    fn queue_pause_owns_the_two_argument_advisory_keyspace() {
        /// Does `src` call an advisory-lock function with two arguments?
        fn calls_two_arg_advisory_lock(src: &str) -> Option<String> {
            // Comments describe the keyspace split at length; only code counts.
            let code: String = src
                .lines()
                .filter(|l| !l.trim_start().starts_with("//"))
                .collect::<Vec<_>>()
                .join("\n");

            let mut from = 0usize;
            while let Some(hit) = code[from..].find("advisory_xact_lock") {
                let open = from + hit;
                let Some(paren) = code[open..].find('(') else {
                    break;
                };
                let mut depth = 0usize;
                for (i, ch) in code[open + paren..].char_indices() {
                    match ch {
                        '(' => depth += 1,
                        ')' => {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        ',' if depth == 1 => {
                            let end = open + paren + i;
                            return Some(code[open..end].to_string());
                        }
                        _ => {}
                    }
                }
                from = open + paren;
            }
            None
        }

        let src_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut stack = vec![src_root];
        let mut checked = 0usize;
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).expect("read src dir") {
                let path = entry.expect("dir entry").path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                    continue;
                }
                if path.file_name().and_then(|f| f.to_str()) == Some("queue_pause.rs") {
                    continue;
                }
                let src = std::fs::read_to_string(&path).expect("read source");
                checked += 1;
                assert!(
                    calls_two_arg_advisory_lock(&src).is_none(),
                    "{} uses the two-argument advisory-lock keyspace, which \
                     queue_pause owns: its keys spend all 64 bits on the queue \
                     name and reserve no class id, so a second user there would \
                     silently stall queue dispatch. Use the single-argument \
                     bigint form (see queue_lock_keys). Offending call:\n{}",
                    path.display(),
                    calls_two_arg_advisory_lock(&src).unwrap_or_default()
                );
            }
        }
        assert!(checked > 10, "the source walk found only {checked} files");

        // The detector must actually fire, or the walk above proves nothing.
        assert!(
            calls_two_arg_advisory_lock("pg_advisory_xact_lock($1, $2)").is_some(),
            "the two-argument detector failed to match the form it looks for"
        );
        assert!(
            calls_two_arg_advisory_lock("pg_try_advisory_xact_lock(hashtext($1)::bigint)")
                .is_none(),
            "the detector must not flag the single-argument form"
        );
    }

    /// The paused-queue read must count per paused queue, not aggregate the
    /// whole shard's PENDING backlog and then join.
    ///
    /// An operator opens this diagnostic *during* a backlog incident. So its
    /// cost must scale with the number of paused queues (a handful). It must
    /// not scale with unrelated pending work (possibly millions of rows).
    #[test]
    fn list_paused_queues_counts_per_paused_queue_not_the_whole_backlog() {
        let sql = list_paused_queues_query();
        assert!(
            sql.contains("WHERE t.queue_name = p.queue_name AND t.state = 'PENDING'"),
            "the held count must be correlated to the pause row; got:\n{sql}"
        );
        assert!(
            !sql.contains("GROUP BY"),
            "an ungrouped correlated count replaces the whole-backlog aggregate; got:\n{sql}"
        );
        // A correlated COUNT(*) already yields 0, so the LEFT JOIN + COALESCE
        // that existed only to keep a zero-held paused queue listed is gone.
        assert!(
            !sql.contains("LEFT JOIN") && !sql.contains("COALESCE"),
            "got:\n{sql}"
        );
    }

    /// The drift lock: the `const` the hot claim path embeds must be exactly
    /// what the renderer produces for the unaliased table.
    #[test]
    fn const_matches_the_renderer() {
        assert_eq!(
            QUEUE_PAUSE_CLAIM_PREDICATE,
            queue_pause_anti_join("harvest_task_queue")
        );
    }

    /// Regression guard for the subtlest bug in this feature. Inside the
    /// subquery, an *unqualified* outer reference resolves against
    /// `harvest_queue_pauses`. The predicate then reads
    /// `qp.queue_name = qp.queue_name`. That pauses every queue as soon as one
    /// queue is paused.
    #[test]
    fn anti_join_always_qualifies_the_outer_column() {
        for outer in ["harvest_task_queue", "tq", "t"] {
            let sql = queue_pause_anti_join(outer);
            assert!(sql.contains(&format!("qp.queue_name = {outer}.queue_name")));
            assert!(
                !sql.contains("= queue_name)"),
                "outer column must never be unqualified; got:\n{sql}"
            );
        }
    }
}
