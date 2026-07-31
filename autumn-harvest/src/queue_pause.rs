//! Task-queue pause/resume — hold dispatch during a downstream outage (issue #619).
//!
//! When a downstream dependency for a whole *class* of activities goes down —
//! Stripe, `SendGrid`, the data warehouse — the operator's job-to-be-done is to
//! **hold** the affected work, not fail it: freeze the queue, let the dependency
//! recover, thaw, and have everything continue as if nothing happened.
//!
//! # How it composes with the neighbouring primitives
//!
//! | Primitive | What it does | Issue |
//! |---|---|---|
//! | Admission gate | Halts new workflow **starts**; in-flight runs keep scheduling activities | #377 / #618 |
//! | Circuit breaker | **Fast-fails** dispatch, burning retry budget and pushing workflows down error branches | #369 |
//! | Per-execution pause | Holds **one** execution — useless when you know the queue, not the 50,000 executions | #383 / #609 |
//! | **Queue pause (this)** | **Holds dispatch** on a named queue: nothing fails, nothing retries, nothing dead-letters | **#619** |
//!
//! Gate the *door*, breaker the *fast-fail*, pause the *hold*.
//!
//! # Enforcement model — anti-join, not cache
//!
//! A pause is durable queue metadata enforced by a `NOT EXISTS` conjunct on the
//! claim path ([`QUEUE_PAUSE_CLAIM_PREDICATE`]), exactly like the
//! PAUSED-execution skip (#383) and the rate-limit gate (#332). It is
//! deliberately **not** an in-process cache like the admission gate's:
//!
//! - "Durable and re-applied before the worker pool begins claiming" is true
//!   **by construction** — there is no boot window to lose a pause in, no
//!   refresh interval, and no fleet-wide staleness window.
//! - There is no global static, no fail-closed reasoning, and no write-through
//!   path that can diverge from the database.
//!
//! The cost is one primary-key probe per claim against a table bounded by the
//! number of distinct queue names.
//!
//! # Replay contract
//!
//! A pause writes **nothing** to `harvest_events` and introduces **no**
//! [`crate::event::WorkflowEvent`] variant. Replay of an execution that touched
//! a paused queue is byte-identical to one that never paused.
//!
//! # Timeout interaction
//!
//! The *relative* `schedule_to_start` timer is suspended for held tasks (both in
//! the scan predicate and in the locked re-check), and a resume credits held
//! time back to `scheduled_at` so a thaw does not retroactively time out the
//! whole backlog. The *absolute* `schedule_to_close` deadline (#378) keeps
//! ticking — a paused queue does not extend an SLA. See
//! [`queue_pause_suppresses_timeout`].

#[cfg(feature = "db")]
use crate::error::HarvestError;
use crate::error::HarvestResult;
#[cfg(feature = "db")]
use chrono::{DateTime, Utc};
#[cfg(feature = "db")]
use diesel_async::AsyncPgConnection;

/// One id returned by [`resume_shift_scheduled_at_query`], so the second pass
/// can exclude exactly the rows the first pass shifted rather than re-deriving
/// that set from a `created_at` predicate that cannot see commit order
/// (issue #619 round-19 review).
#[cfg(feature = "db")]
#[derive(diesel::QueryableByName)]
struct ShiftedTaskId {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: uuid::Uuid,
}

/// Domain separator for [`queue_lock_keys`] (issue #619 round-23 review).
///
/// Mirrors `crate::build_routing`'s `RAMP_HASH_DOMAIN_SEP`: the digest is this
/// feature's alone, so a queue name can never derive the same key as an
/// unrelated subsystem that happens to hash the same string. The `v1` marker
/// makes a future re-keying an explicit, greppable decision rather than a
/// silent behaviour change — see [`queue_lock_keys`] on why re-keying is not
/// a rolling-deploy-safe operation.
const QUEUE_PAUSE_LOCK_DOMAIN: &str = "harvest:queue_pause:v1:";

/// Derive the `(key1, key2)` advisory-lock key that identifies `queue_name`.
///
/// Every queue-pause lock — [`lock_queue_for_pause`],
/// [`lock_queue_for_timeout_recheck`], [`try_lock_queue_for_claim`] — keys on
/// this pair, so they mutually exclude on exactly the queue they name.
///
/// # Why the whole 64 bits are spent on identity (issue #619 round-23 review)
///
/// Postgres exposes two *disjoint* advisory-lock keyspaces: a 64-bit one
/// (`pg_advisory_xact_lock(bigint)`) and a 2×32-bit one
/// (`pg_advisory_xact_lock(int, int)`). A key in one can never collide with a
/// key in the other, and **every other advisory-lock user in this engine is in
/// the single-argument one** — the per-key concurrency gate on the hot claim
/// path (`crate::queue::claim_task`), `crate::mutex`, `crate::wasm_store` and
/// `crate::admission_gate`. So the two-argument keyspace belongs to this
/// feature alone, which is the load-bearing separation round 16 established and
/// this function preserves. A source-level guard test keeps it true.
///
/// Round 16 spent the first 32 bits on a constant class id and derived the
/// second from `hashtext(queue_name)`, which is only 32 bits wide. That left
/// **queue identity** at 32 bits: two unrelated queue names collide with
/// probability ≈ n²/2³³, and the consequence is precisely the failure this
/// feature exists to prevent. An exclusive pause on queue `a` makes
/// [`try_lock_queue_for_claim`] fail for the colliding queue `b`, and because
/// that call uses the *try* variant the failure is silent — `b`'s workers
/// release every claim and stop dispatching for as long as `a`'s pause or
/// (potentially long) resume runs, on a queue that was never paused.
///
/// The fix is to stop spending half the key on a constant: `key1` and `key2`
/// are the high and low halves of a domain-separated SHA-256 digest, so
/// identity is the full 64 bits and a false share needs a 64-bit collision.
///
/// # Why not two locks, or the single-argument form
///
/// **Two locks** (a class-scoped pair, 32 identity bits each) would make this
/// *worse*, not better: lock conflicts are evaluated per key, so a claim
/// needing both locks fails if **either** collides. That doubles the
/// false-share probability rather than squaring it away.
///
/// **The single-argument 64-bit form** would give the same identity width but
/// drop us into the keyspace `claim_task`'s concurrency gate already hashes raw
/// text into — trading an intra-feature collision for a cross-feature one whose
/// blast radius is strictly worse and far harder to diagnose.
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
/// The same reason issue #699's rate-limit bucket keys use it: a collision here
/// silently shares state between two unrelated queues, and queue names are
/// influenced from outside this module. The digest is computed once per lock
/// call, in-process, so it costs nothing next to the round trip it rides on.
///
/// # Compatibility
///
/// The key derivation is part of the *runtime* protocol between concurrently
/// running processes, not a persisted format — nothing stores it. Changing it
/// (including the domain separator) is therefore safe across a restart but
/// **not** across a rolling deploy: two processes on different derivations
/// would take non-conflicting locks for the same queue and lose mutual
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
/// Queue names are free-form `TEXT` everywhere else in the engine, but this
/// table's primary key is the queue name, so an unbounded operator-supplied
/// string would be an unbounded btree key. 255 matches the one other place the
/// engine bounds a queue name (`harvest_completion_triggers.queue_name`).
pub const MAX_QUEUE_NAME_LEN: usize = 255;

/// Render the queue-pause anti-join correlated against `outer_table` — the
/// single source of truth for the claim gate and every query that mirrors it.
///
/// A paused queue that a mirror query forgot about would report a phantom
/// backlog and drive a false capacity alert during a deliberate hold, so all of
/// them are generated from (or drift-locked against) this one function.
///
/// The outer column reference **must** be qualified: inside the subquery an
/// unqualified `queue_name` resolves against `harvest_queue_pauses` first,
/// degenerating the predicate to `qp.queue_name = qp.queue_name` — always true,
/// which would silently pause *every* queue the moment any one queue is paused.
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
/// [`crate::queue::claim_task`] is a single autocommit statement, so under
/// `READ COMMITTED` its whole CTE — including the
/// [`QUEUE_PAUSE_CLAIM_PREDICATE`] anti-join — evaluates against **one snapshot
/// taken at statement start**. A pause that commits after that snapshot is
/// invisible to it, so a claim already in flight when the operator paused can
/// still transition its task to `RUNNING` and hand it to a worker that
/// dispatches into the outage. The window is normally sub-millisecond but is
/// unbounded in principle: the claim's rate-limit debit can block on a row lock
/// held by a competing claim, and Postgres's `EvalPlanQual` re-check on unblock
/// only re-evaluates conditions on the *locked row*, never a correlated
/// subquery over another table.
///
/// Taking an *exclusive* queue-scoped lock inside the claim path would close
/// the window but serialize every claim for a queue against every other,
/// defeating `FOR UPDATE SKIP LOCKED`'s parallel claiming for a feature that is
/// inactive almost all of the time. Re-checking in a **fresh statement** gets a
/// fresh snapshot for the cost of one indexed primary-key probe per *successful*
/// claim (never per empty poll), and yields a crisp contract: **a pause
/// committed before this re-check's statement begins always wins.**
///
/// That contract is only sound if the claim is *durable* by the time the pause
/// commits. Round 17 wrapped the claim and this re-check in one transaction (so
/// a re-check error can no longer strand a `RUNNING` row), which moved the
/// claim's commit *after* the re-check's snapshot and opened a new window: a
/// pause committing in between would be acknowledged to the operator and the
/// claim would still commit afterwards. [`try_lock_queue_for_claim`] closes it
/// with a **shared** lock on the same key `pause_queue` takes exclusively — see
/// there for why the *try* variant is what keeps this deadlock-free.
///
/// The release is guarded on `state = 'RUNNING' AND worker_id = $2` so it can
/// only ever undo *this* worker's own claim, and it restores `attempt` — the
/// task never ran, so a hold must not consume retry budget (AC3). The
/// rate-limit token the claim debited is not refunded; that is the same
/// safe-side, self-healing leak already documented for a lost claim in
/// [`crate::queue::claim_task`] (it only ever under-dispatches).
///
/// Returns `true` when the claim was released (the caller must behave as if no
/// task was claimed).
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
#[cfg(feature = "db")]
pub async fn release_claim_if_queue_paused(
    conn: &mut AsyncPgConnection,
    task_id: uuid::Uuid,
    worker_id: &str,
) -> HarvestResult<bool> {
    use diesel_async::RunQueryDsl;

    let released = diesel::sql_query(release_claim_if_queue_paused_query())
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .bind::<diesel::sql_types::Text, _>(worker_id)
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(released > 0)
}

/// The shared prefix of both claim-release statements.
///
/// A macro rather than a `const` so the two `const fn`s below can `concat!` it
/// with their own tail at compile time. Both releases must restore `attempt`
/// and be guarded on `state = 'RUNNING' AND worker_id = $2` identically; keeping
/// one copy makes that structural rather than a convention two sites must
/// remember.
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
/// One statement, so it takes a fresh `READ COMMITTED` snapshot and therefore
/// sees any pause committed before it began — which is the entire point (see
/// [`release_claim_if_queue_paused`]).
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
/// pause table cannot be consulted: [`try_lock_queue_for_claim`] failed, so
/// some pause or resume transaction is mid-flight on this queue and its rows
/// are, by definition, not yet visible to us. Guarded and attempt-restoring
/// exactly like the paused variant, so a barrier miss costs a task nothing
/// beyond one poll interval.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
#[cfg(feature = "db")]
pub async fn release_claim(
    conn: &mut AsyncPgConnection,
    task_id: uuid::Uuid,
    worker_id: &str,
) -> HarvestResult<bool> {
    use diesel_async::RunQueryDsl;

    let released = diesel::sql_query(release_claim_query())
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .bind::<diesel::sql_types::Text, _>(worker_id)
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(released > 0)
}

/// SQL for [`try_lock_queue_for_claim`], exposed for shape tests.
///
/// `$1`/`$2` = the queue's [`queue_lock_keys`] pair.
#[must_use]
pub const fn try_lock_queue_for_claim_query() -> &'static str {
    "SELECT pg_try_advisory_xact_lock_shared($1, $2) AS acquired"
}

/// Commit-order barrier between a claim and a concurrent pause (issue #619
/// round-18 review).
///
/// Takes the **shared** mode of the very key [`lock_queue_for_pause`] takes
/// exclusively, so while a claim transaction holds it a `pause_queue` cannot
/// commit — and therefore cannot acknowledge a hold to the operator — until
/// that claim has committed. That is the ordering guarantee the post-claim
/// re-check alone cannot provide: the re-check's snapshot is taken before its
/// own transaction commits, so without this a pause could slot in between and
/// still be beaten by a claim it had already reported as held.
///
/// # Why *shared*, and why *try*
///
/// **Shared** because claims must stay parallel: shared mode is compatible with
/// itself, so an arbitrary number of workers claim concurrently and only the
/// (rare, brief) exclusive pause/resume transaction is excluded. An exclusive
/// lock here would serialize every claim on a queue and defeat
/// `FOR UPDATE SKIP LOCKED` — the reason the round-2 review's literal
/// suggestion was rejected.
///
/// **Try** because of lock ordering. This is necessarily called *after* the
/// claim query, since the queue to lock is not known until a task is in hand,
/// so the claim path holds task rows and then wants this lock — the exact
/// inverse of `resume_queue`, which holds this lock and then row-locks every
/// `PENDING` task on the queue for its `scheduled_at` shift. A *blocking*
/// acquire would therefore reintroduce the ABBA deadlock the round-2 review
/// found (and Postgres would abort either the claim or the operator's resume).
/// `pg_try_advisory_xact_lock_shared` never waits, so it cannot participate in
/// a wait cycle at all.
///
/// Returning `false` means "a pause or resume is committing on this queue right
/// now"; the caller must release its claim rather than dispatch. That is
/// deliberately conservative in the safe direction — a resume is not a hold, so
/// releasing during one costs at most one poll interval, while dispatching
/// during a *pause* is the failure this whole feature exists to prevent.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
#[cfg(feature = "db")]
pub async fn try_lock_queue_for_claim(
    conn: &mut AsyncPgConnection,
    queue_name: &str,
) -> HarvestResult<bool> {
    use diesel_async::RunQueryDsl;

    #[derive(diesel::QueryableByName)]
    struct AcquiredRow {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        acquired: bool,
    }

    let (key1, key2) = queue_lock_keys(queue_name);
    let row: AcquiredRow = diesel::sql_query(try_lock_queue_for_claim_query())
        .bind::<diesel::sql_types::Integer, _>(key1)
        .bind::<diesel::sql_types::Integer, _>(key2)
        .get_result(conn)
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
/// Both halves of that rule matter, because the anti-join matches on exact
/// equality and `EnqueueParams` stores `queue_name` verbatim (it does not
/// normalise), so a queue genuinely named `"payments "` is reachable:
///
/// - Storing the **raw** name while validating a trimmed one would insert a row
///   matching no task, reporting a successful hold (`newly_paused: true`,
///   `held_task_count: 0`) on a queue that keeps dispatching.
/// - **Trimming** is no better, just quieter: pausing `"payments "` would
///   insert a row for `"payments"`, so the hold would silently target a
///   *different, valid* queue while the real one kept dispatching — and the API
///   would again report success.
///
/// Rejecting is the only option that cannot lie. In the overwhelmingly common
/// case a stray space is a copy-paste typo, and the operator gets a `400`
/// naming it instead of a false green. In the pathological case the queue
/// really does carry surrounding whitespace, it is not pausable through this
/// API — an honest refusal, and strictly safer than a silent success against
/// the wrong queue during an outage. Normalising queue names at *creation* time
/// is the alternative fix, but that reaches across every enqueue path and is
/// out of scope here.
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
/// re-check alone cannot guarantee that: `pause_queue` shares no lock with the
/// enforcer, so a pause can commit in the window between the re-check and the
/// enforcer's own commit — terminally failing a task *because* its queue was
/// paused, the exact outcome AC2/AC3/AC4 forbid.
///
/// Both sides therefore take this transaction-scoped advisory lock first,
/// mirroring the `concurrency_key` and `ctx.mutex` patterns already used in the
/// engine. It is keyed on the queue name alone, so unrelated queues never
/// contend.
///
/// # Why the two-argument lock form (issue #619 round-16 review)
///
/// It uses the **two-argument** keyspace, which no other advisory-lock user in
/// this engine touches, because the single-argument `hashtext(<some text>)`
/// keyspace is already shared, unnamespaced, by several unrelated features —
/// most importantly the per-key concurrency gate on the hot claim path
/// (`queue::claim_task`'s
/// `pg_try_advisory_xact_lock(hashtext(candidate.concurrency_key)::bigint)`).
///
/// Sharing it would be a genuine cross-feature outage, not a theoretical one:
/// a queue name that merely hash-collides with some *other* queue's
/// `concurrency_key` would make that key's `pg_try_advisory_xact_lock` fail for
/// as long as a large `resume_queue` held this lock while shifting its backlog.
/// Because the claim path uses the *try* variant, the failure is silent — the
/// claim simply yields no task — so tasks on a queue that was never paused
/// would quietly stop dispatching for the duration of an unrelated queue's
/// resume. That is precisely the "silently stops dispatching" failure mode this
/// whole feature exists to prevent, so the namespace separation is load-bearing
/// rather than hygiene.
///
/// Round 23 kept that separation and closed the *intra*-feature half of the
/// same hazard: see [`queue_lock_keys`] for why both key slots are now spent on
/// queue identity rather than one on a constant class id.
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
/// pause or resume — **without** blocking ordinary claims (issue #619 round-21
/// review).
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
/// The enforcer originally reused [`lock_queue_for_pause`]. Exclusive mode
/// blocks shared, so from round 18 — when [`try_lock_queue_for_claim`] began
/// taking the shared mode of this key on every claim — an enforcement
/// transaction stalled *all* dispatch on its queue for its full duration, on a
/// queue with **no pause at all**. The workflow sibling holds the lock across
/// `append_events`, the parent-close cascade and trigger evaluation, so a
/// backlog of expired tasks could repeatedly halt unrelated work. Claims fail
/// their `try` silently and simply yield no task, which is the same
/// "silently stops dispatching" failure the class-id separation documented on
/// [`lock_queue_for_pause`] exists to prevent — reached from a different side.
///
/// Shared mode is sufficient because this lock's only job is to order the
/// enforcement against pause/resume. Two concurrent enforcements need no mutual
/// exclusion: they act on distinct tasks and already serialize on the task rows
/// they lock.
///
/// # Why blocking, not `try`
///
/// Unlike [`try_lock_queue_for_claim`] — which is called *after* the claim
/// query, so it must never wait — this is the **first** statement in the
/// enforcement transaction, before any row lock, so it cannot participate in a
/// wait cycle (see the lock-ordering note in `enforce_activity_timeout`). It
/// must actually wait: bailing out on contention would skip the pause re-check
/// and let the enforcement proceed against a stale snapshot, terminally failing
/// the very task an in-flight pause was protecting.
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
/// Distinct from the stored `scope_shard_id`, which records the *intent* of the
/// request that wrote a row (`NULL` = "this was a fleet-wide request"), not its
/// coverage. A fleet-wide pause that only reached some shards persists
/// `scope_shard_id = NULL` on the shards it reached and **no row at all** on the
/// ones it missed, so reading intent alone would present a partially-applied
/// hold as fleet-wide while the missed shards keep dispatching (issue #619
/// review).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PauseCoverage {
    /// Every expected shard holds the queue.
    Fleet,
    /// At least one row was written by a fleet-wide request, but at least one
    /// expected shard does not hold the queue — so part of the fleet is still
    /// dispatching.
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
/// `holding_shards` are the shards whose read returned a row for this queue;
/// `scope_shard_ids` are those rows' stored scopes; `expected_shards` is the
/// shard set the fleet is *supposed* to span (`shard_fanout::expected_shards`,
/// i.e. every pool plus every shard the router knows about).
///
/// Using **expected** rather than merely-inspected shards is deliberate and is
/// the safe direction: a shard that could not be read might well be
/// dispatching, so a hold is only ever reported as `Fleet` when every shard the
/// fleet is supposed to have is known to hold it.
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
/// Without it, thawing a queue whose backlog waited past its
/// `schedule_to_start` would immediately time out the *entire* held backlog:
/// exactly the failure AC4/AC5 exist to prevent.
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
///   full pause span, so pre-pause waiting is preserved (a task that had already
///   waited 100 s still shows 100 s of wait after the thaw).
/// - **Became eligible mid-pause** (`paused_at < scheduled_at <= now`):
///   collapses to the thaw instant — it was held from the moment it became due,
///   so it accrues no wait.
/// - **Not yet due** (`scheduled_at > now`, e.g. a retry backoff): excluded
///   entirely. It was never held, so its backoff must not be extended.
///
/// # Why `clock_timestamp()` and not `NOW()`
///
/// `NOW()` is `transaction_timestamp()` — frozen at **transaction start**, which
/// here is *before* the advisory-lock wait, before the pause-row `DELETE`, and
/// before this bulk `UPDATE` runs. But the hold stays in force until this
/// transaction **commits**, so a frozen `NOW()` silently omits the resume's own
/// duration from the credit. Measured directly: with a task that had genuinely
/// waited 100 s pre-pause and a resume that spent 3 s on the lock, `NOW()`
/// leaves it looking like it waited 103 s the instant the queue thaws — so any
/// task whose `schedule_to_start` is shorter than the resume's own runtime times
/// out immediately after the thaw, which is precisely the AC3/AC4 failure
/// ("never failed *solely because* of the pause") this query exists to prevent.
/// The same frozen value also excluded rows that fell due *during* the
/// transaction from the predicate.
///
/// `clock_timestamp()` is volatile and re-read per row at execution time, so it
/// sees the real wall clock after the lock is held and advances as the scan
/// progresses.
///
/// It is still a **lower bound**, deliberately: the true thaw instant is commit
/// time, which no in-statement expression can know, so each row is under-credited
/// by at most the remaining duration of this `UPDATE`. That is the *safe*
/// direction — over-crediting past commit time would push `scheduled_at` into
/// the future and make the task **un**claimable, the inverse of AC5.
///
/// The result stays provably `<= clock_timestamp()` at evaluation, hence
/// `<= commit time`, in every branch. `RUNNING` rows are untouched, and
/// `schedule_to_close_at` is untouched so the absolute deadline keeps ticking.
///
/// # Why the partition is `created_at` vs `paused_at`
///
/// This pass owns the rows that had already accrued wait *before* the hold
/// began; [`resume_shift_late_arrivals_query`] owns the rows created *during*
/// it. The two predicates partition the due `PENDING` rows so no row is shifted
/// twice — a second application of this *relative* `+=` formula would erase the
/// pre-pause wait it exists to preserve.
///
/// The split is **semantic, not temporal** (issue #619 round-16 review). An
/// earlier revision partitioned on `created_at < transaction_timestamp()`
/// ("existed when this resume began"), which is subtly wrong: `created_at`
/// records when the enqueuing `INSERT` *executed*, while whether this pass can
/// *see* the row is decided by when that enqueuing transaction **committed**. A
/// row inserted before the resume began but committed after this statement took
/// its snapshot therefore satisfied neither predicate — invisible to this pass,
/// and excluded from the late pass by its own `created_at` test — so it thawed
/// with its entire held wait uncredited and could be schedule-to-start-failed
/// immediately after the thaw.
///
/// Comparing against `paused_at` removes visibility from the question
/// altogether: `created_at` and `paused_at` are both fixed values on rows that
/// already exist, so a row's side is the same no matter which statement first
/// observes it, and a row that becomes visible late is still credited correctly
/// by whichever pass owns it. It also happens to be the more meaningful split —
/// "did this task accrue wait before the hold, or was it held from birth?" —
/// which is exactly the distinction the two formulas encode.
///
/// `created_at` is **nullable** (a pre-`20260619000000` `PENDING` row has none),
/// and in SQL a comparison against `NULL` is `NULL`, not `false` — so without the
/// explicit `IS NULL` arm such a row would satisfy *neither* predicate and be
/// credited by *neither* pass. It belongs on this side: a row with no
/// `created_at` predates that migration and therefore predates any pause.
///
/// # Why a total partition was still not enough (issue #619 round-19 review)
///
/// The partition above is total and visibility-independent — every due
/// `PENDING` row matches exactly one predicate, whenever it becomes visible.
/// That is a statement about the *predicates*, and an earlier revision wrongly
/// read it as a statement about **coverage**. It is not: a row on *this* pass's
/// side that is invisible to *this* pass's snapshot is simply never shifted,
/// and it cannot be picked up later, because the second pass used to test the
/// same `created_at` predicate and so excluded it by construction.
///
/// Concretely: an enqueue whose `INSERT` *executes* before the pause (so
/// `created_at < paused_at`, this pass's side) but whose transaction *commits*
/// after this statement's snapshot is invisible here and was rejected there —
/// thawing with its entire held wait uncredited, and so immediately
/// `schedule_to_start`-failable. Fixing the partition (round 16) removed the
/// dependence on visibility for *which side a row belongs to*; it could not
/// remove the dependence on visibility for *whether a pass sees the row at all*.
///
/// The fix is to stop deriving pass 2's exclusion from a predicate and derive
/// it from the rows this pass **actually shifted**: this statement now
/// `RETURNING id`s them, and [`resume_shift_late_arrivals_query`] covers *both*
/// sides of the partition for every row not in that set. Coverage is then total
/// across the two snapshots by construction, for any commit order.
///
/// The remaining, genuinely irreducible residual — a row committing after pass
/// 2's snapshot — is documented on [`resume_shift_late_arrivals_query`].
///
/// # Re-pend paths
///
/// A row can also change **state** between the two snapshots: a `RUNNING` row
/// re-pended after this pass's snapshot is invisible here. Every such row is
/// now covered by pass 2 regardless of which side of the partition it lands on,
/// so this is no longer a correctness argument — but it is still worth
/// recording that each reachable re-pend path *refreshes* `scheduled_at` to the
/// re-pend instant (`crate::queue::requeue_for_retry`, `crate::poison_pill`
/// reclaim, `primary_repend_workflow_task`) and so accrued no held time anyway.
/// The one path that re-pends while *preserving* a pre-pause `scheduled_at` is
/// [`release_claim_if_queue_paused`], which does so deliberately so a released
/// claim keeps the wait it had already accrued; it is now credited correctly by
/// pass 2 rather than relying on being unreachable in this window.
///
/// # Why the ids round-trip through this process (issue #619 round-24 review)
///
/// The shifted ids are loaded here and sent back to pass 2 as an array bind,
/// which is linear in the released backlog: one `Uuid` per shifted task in
/// application memory, then that array re-serialised onto the wire and
/// re-materialised server-side by `unnest`, all while this transaction holds
/// the queue's exclusive advisory lock.
///
/// Round 23 removed that cost by capturing the ids straight into a
/// transaction-scoped `CREATE TEMP TABLE ... ON COMMIT DROP` scratch table via
/// a data-modifying CTE. Round 24 reverted it: `CREATE TEMP TABLE` requires the
/// database-level `TEMPORARY` privilege. Nothing in the migration grants it and
/// nothing in preflight validates it, so a deployment that has revoked
/// `TEMPORARY` — a recognised hardening step — passes every current permission
/// check and then fails EVERY resume, rolling back the pause deletion and
/// leaving the queue frozen at precisely the moment an operator is trying to
/// thaw it. A rare scale problem is a better failure than a deterministic,
/// silent, config-dependent break of the feature's core operation.
///
/// The memory cost is real and is tracked as a follow-up. Fixing it properly
/// needs a handoff covered by the grants the migration already issues (a real
/// table keyed by a per-resume token), not a privilege the deployment may not
/// have.
///
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
/// The hold stays in force until `resume_queue`'s transaction *commits* — a
/// concurrent claimer's snapshot still sees the not-yet-committed pause row, so
/// it still skips the queue. A task enqueued after
/// [`resume_shift_scheduled_at_query`]'s snapshot is therefore genuinely held,
/// yet that statement can never see it, so without this pass it would thaw
/// carrying an uncredited `scheduled_at` and could be timed out by the
/// `schedule_to_start` scanner for time it spent held — the AC3/AC4 failure the
/// primary shift exists to prevent.
///
/// # Why this pass covers *both* sides of the partition (issue #619 round-19 review)
///
/// It is not enough for this pass to own only the rows created during the hold.
/// A row on the *primary* pass's side (`created_at < paused_at`) that was
/// invisible to that pass's snapshot — an enqueue that executed before the
/// pause but committed after the primary shift ran — would otherwise be
/// credited by neither pass and thaw with its whole held wait uncredited. See
/// the round-19 note on [`resume_shift_scheduled_at_query`].
///
/// So this statement applies the **same per-side formula** the partition
/// defines, via a `CASE`, and derives its exclusion from the ids the primary
/// pass actually shifted (`$3`) rather than from a `created_at` predicate:
///
/// - `created_at IS NULL OR created_at < $2` → the primary pass's *relative*
///   `+=` delta, preserving the wait the row accrued before the hold.
/// - otherwise → `clock_timestamp()` (absolute), because a row created during
///   the hold has no pre-pause wait to preserve: it was held from the moment it
///   became due, so it accrues none.
///
/// Excluding by id rather than by predicate is what makes coverage total for
/// *any* commit order: a row the primary pass shifted is in `$3` and is skipped
/// here (a second application of the relative formula would erase the very
/// pre-pause wait it exists to preserve), and every other due `PENDING` row is
/// credited here, whichever side it belongs to. An empty `$3` — the primary
/// pass shifted nothing — correctly leaves every row to this pass.
///
/// The exclusion is written as `NOT EXISTS (… unnest …)` rather than
/// `id <> ALL(<array>)` deliberately: the latter is a per-row linear scan of
/// the array, while the former lets the planner build a hash anti-join, so the
/// cost is O(rows + ids) rather than O(rows × ids). (Round 11 rejected this
/// exclusion on the strength of the `<> ALL` cost; the objection was to that
/// spelling, not to the approach.)
///
/// # Irreducible residual
///
/// This narrows the uncredited window from "the whole bulk `UPDATE`" to "this
/// final, small statement", but cannot close it: a row committed after *this*
/// statement's snapshot and before this transaction commits is still held and
/// still gets no credit. The under-credit is bounded by the remaining duration of
/// this statement (microseconds for a queue with no late arrivals), the same
/// safe direction as the primary pass's lower-bound credit.
///
/// Closing it completely would require serializing **enqueue** behind the queue's
/// advisory lock, putting a queue-scoped serialization point on the hot enqueue
/// path for every task on every queue, paused or not. That is the exact trade
/// this module's design notes reject for the claim path, and it is not worth
/// paying to recover a sub-millisecond credit — so the residual is documented
/// rather than eliminated.
///
/// Binds: `$1` = queue name, `$2` = the hold's `paused_at`, `$3` = the ids the
/// primary pass reported shifting.
#[must_use]
pub const fn resume_shift_late_arrivals_query() -> &'static str {
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
           )"
}

/// The `GET /admin/queues/paused` read, and the Vantage Workers banner's source.
///
/// # Why a correlated count rather than a grouped join (issue #619 round-16 review)
///
/// The obvious shape — `LEFT JOIN (SELECT queue_name, COUNT(*) … GROUP BY
/// queue_name)` — aggregates the **entire** `PENDING` backlog of every queue on
/// the shard and only then joins the result to the (tiny) pause table. Postgres
/// cannot reliably push the pause table's queue names down into that grouped
/// subquery, so the cost scales with the whole shard's backlog rather than with
/// the paused queues.
///
/// That is backwards for this query specifically: it is a **diagnostic read**,
/// executed per shard by both the management endpoint and the Workers-page
/// banner, and it is consulted during exactly the incident where the backlog is
/// largest. Making the operator's "what is on hold?" view slow (or time out) in
/// proportion to unrelated pending work defeats its purpose.
///
/// The correlated `COUNT(*)` instead runs once per **paused queue** — a set
/// bounded by the number of rows in `harvest_queue_pauses`, normally a handful —
/// and each one is a selective `(queue_name, state)` lookup rather than a full
/// aggregation. It also drops the `COALESCE`: a correlated count returns `0`
/// for a paused queue with no held work, so such a queue still lists rather
/// than disappearing, which was the only reason the `LEFT JOIN` existed.
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
/// `LIMIT 1` because the notification is a *doorbell*, not a work assignment:
/// [`crate::notify::QueueListener`] hands the payload back to the poll loop,
/// which ignores it and re-polls the queue normally. One indexed row is
/// therefore all this needs, and it deliberately does **not** use
/// `RETURNING id` on the shift statements — that would materialise an id array
/// proportional to the whole released backlog to carry a single value.
///
/// The predicate mirrors the shift passes' own due-row filter, so the id is one
/// of the rows this resume just made claimable rather than a future-scheduled
/// retry that is still not due.
#[must_use]
pub const fn resumed_queue_notify_task_query() -> &'static str {
    "SELECT id FROM harvest_task_queue \
     WHERE queue_name = $1 \
       AND state = 'PENDING' \
       AND scheduled_at <= clock_timestamp() \
     LIMIT 1"
}

/// Ring the queue's `LISTEN`/`NOTIFY` doorbell so parked workers re-poll as
/// soon as this resume commits (issue #619 round-17 review).
///
/// Must be called **inside** the resume transaction: Postgres queues `NOTIFY`
/// and delivers it at `COMMIT`, so listeners wake exactly when the hold lifts —
/// never earlier (an early wake would still see the uncommitted pause row and
/// skip the queue) and never at all if the resume rolls back.
///
/// Reuses [`crate::notify::notify_task_enqueued`] rather than introducing a
/// second channel or payload shape, because [`crate::notify::QueueListener`]
/// **parses** the payload as `NotifyPayload { task_id }` and surfaces a parse
/// failure as an error — which the poll loop handles by logging and sleeping a
/// full `poll_interval`, i.e. strictly worse than sending nothing. A synthetic
/// or nil id would be a lie in a field that is typed as a real task; a genuine
/// id is both honest and free (one indexed row).
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
/// Idempotent: re-pausing an already-paused queue is a success no-op with
/// `newly_paused == false` and the **original** pause provenance preserved
/// (mirroring the `newly_terminated` / `newly_resumed` convention from
/// #504 / #609).
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
    /// began plus anything enqueued while it ran. The two partition on
    /// `created_at`, so this is an exact row count, never a double count.
    pub released_task_count: i64,
    /// How long the queue was held, in seconds (`0` on a no-op).
    pub paused_duration_secs: i64,
    /// The reason the hold was originally placed, echoed back on release.
    ///
    /// The pause row is deleted on resume, so this is the last moment the
    /// *why* is recoverable — the resume response (and therefore the CLI
    /// output and the Vantage flash) carries it so an operator closing out an
    /// incident sees exactly which hold they released. `None` on a no-op.
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
/// A single upsert, not `INSERT ... DO NOTHING` + a fallback `SELECT`:
/// `DO NOTHING` does not lock the conflicting row, so a concurrent resume could
/// delete it between the two statements, leaving the fallback `SELECT` with zero
/// rows — a `500` and, worse, a queue left *unpaused* for an operator who was
/// told the pause failed.
///
/// The no-op `SET queue_name = ...` preserves the ORIGINAL provenance on a
/// re-pause while still taking the row lock, and `xmax = 0` is the standard
/// discriminator for "this tuple was inserted, not updated".
///
/// # Why `paused_at` is stamped explicitly, not left to `DEFAULT NOW()`
///
/// The column defaults to `NOW()`, which is `transaction_timestamp()` — frozen
/// at **transaction start**, i.e. *before* [`lock_queue_for_pause`] waits. The
/// hold does not become effective until that lock is held (and, strictly, until
/// this transaction commits), so a defaulted `paused_at` predates the effective
/// hold by the entire lock wait — which is unbounded in principle, since it can
/// queue behind a large concurrent resume shifting a big backlog.
///
/// Two consequences, both real:
///
/// - **Reported start and duration are overstated.** `GET /admin/queues/paused`
///   and the Vantage banner show `paused_at`, the resume response derives
///   `paused_duration_secs` from it, and the `harvest_queue_paused_too_long`
///   alert exists precisely so an operator reasons about *how long* a hold has
///   been in place. All of them read too long by the lock wait.
/// - **The resume shift over-credits.** `resume_shift_scheduled_at_query`
///   credits `clock_timestamp() - GREATEST(scheduled_at, paused_at)`, so an
///   understated `paused_at` credits an interval the task was not actually
///   held, eroding the pre-pause wait the shift exists to preserve.
///
/// Note the *direction* of that second one: the shift's `GREATEST` clamps its
/// result to `<= clock_timestamp()` in every branch, so an understated
/// `paused_at` can never push `scheduled_at` into the future and make a task
/// **un**claimable (the AC5 inverse). It reduces the apparent wait, which is the
/// safe side for `schedule_to_start` — but it is still lost fidelity in exactly
/// the value round 9 fixed the other half of.
///
/// `paused_at` is therefore **bound** (`$5`), read by [`pause_clock_query`]
/// immediately after the advisory lock is granted rather than evaluated inline
/// when this statement runs — see below. It appears **only** in the `INSERT`
/// half: the `DO UPDATE` deliberately does not touch `paused_at`, so an
/// idempotent re-pause still preserves the original hold's start time.
///
/// # Why the stamp is taken at lock acquisition, not later (issue #619 round-20 review)
///
/// Earlier revisions read `clock_timestamp()` inline here and had [`pause_queue`]
/// run the [`held_task_count_query`] backlog scan *first*, so the stamp landed as
/// close to `COMMIT` as possible. The reasoning was that claimers share neither
/// the advisory lock nor visibility of the uncommitted pause row, so they keep
/// dispatching until `COMMIT` — making everything before it *unheld* time that
/// must not be credited back on resume.
///
/// The first half of that is no longer true. [`try_lock_queue_for_claim`] takes
/// the **shared** mode of the very key [`lock_queue_for_pause`] takes
/// exclusively, so from the moment this transaction is granted that lock every
/// claim on the queue fails its `try` and gives the task back. The queue is
/// therefore **effectively held from lock acquisition**, not from `COMMIT` — and
/// stamping after the scan credits none of the scan interval, which is unbounded
/// in principle on a large `PENDING` backlog. A task with a short
/// `schedule_to_start` could then be timed out immediately after the thaw for
/// time it spent blocked by the pause transaction itself: the exact AC3/AC4
/// failure the shift exists to prevent.
///
/// Note the *direction*. The shift is
/// `scheduled_at += clock_timestamp() - GREATEST(scheduled_at, paused_at)`, so a
/// **later** `paused_at` shrinks the credit (under-credit → spurious timeouts,
/// the dangerous side), while an **earlier** one only grows it, and the
/// `GREATEST` clamps the result to `<= clock_timestamp()` in every branch — so
/// it can never push `scheduled_at` into the future and make a task
/// **un**claimable (the AC5 inverse). Stamping at lock acquisition is thus both
/// the correct instant *and* the safe direction.
///
/// The lock is acquired in its own statement and the clock read in the next, not
/// together in one target list: Postgres does not guarantee evaluation order
/// within a select list, so a combined statement could read the clock *before*
/// the blocking lock call and silently reintroduce the lock-wait as
/// over-credit. Guarded by
/// `pause_stamps_paused_at_at_lock_acquisition_not_after_the_scan`.
///
/// The genuinely irreducible residue is now only `COMMIT` itself: a claim
/// released by the barrier between the stamp and commit is held but uncredited,
/// bounded by this transaction's remaining runtime.
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
/// Run by [`pause_queue`] immediately after [`lock_queue_for_pause`] returns, in
/// its own statement so the blocking lock acquisition provably happens first.
/// `clock_timestamp()` (volatile, real current time) rather than `NOW()`, which
/// is frozen at transaction start — before the lock wait.
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
    // `resume_queue` — see the note there (issue #619 round-24 review). The
    // advisory lock establishes this transaction's snapshot before it waits, so
    // under `REPEATABLE READ` a pause that lands behind a concurrent
    // pause/resume would read pre-lock state for the rest of the transaction.
    let mut tx = conn.build_transaction().read_committed();
    Box::pin(tx.run::<PauseOutcome, HarvestError, _>(async |conn| {
        // Serialize against the schedule_to_start enforcer so it cannot
        // fail a task on the queue we are pausing (see
        // `lock_queue_for_pause`).
        lock_queue_for_pause(conn, &queue_owned).await?;

        // The hold takes effect HERE, not at COMMIT: `try_lock_queue_for_claim`
        // takes the shared mode of the key just locked exclusively, so every
        // claim on this queue now fails its `try` and gives the task back.
        // Read the clock in its own statement, immediately after the lock, so
        // the whole rest of this transaction — the unbounded backlog scan
        // below included — is credited back on resume. See
        // `pause_upsert_query` for why a later stamp under-credits and can
        // spuriously time a task out after the thaw.
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
/// Runs in one transaction so the pause row is deleted and the held backlog is
/// re-timed atomically: no worker can observe a thawed queue whose backlog is
/// still carrying its stale, pre-thaw `scheduled_at` (which would let the
/// `schedule_to_start` scanner kill the very work the pause protected).
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

    // Pinned `READ COMMITTED`, not the session default (issue #619 round-24 review).
    //
    // The advisory lock below is a `SELECT`, so it establishes this
    // transaction's snapshot and *then* blocks waiting for any in-flight pause.
    // Under `REPEATABLE READ` that snapshot is the transaction's only one, so
    // once the pause commits and releases the lock, the `DELETE` below still
    // cannot see the pause row it was waiting for: the resume reports a
    // successful `newly_resumed: false` no-op while the queue stays held —
    // the operator believes they thawed and nothing dispatches.
    //
    // Under `READ COMMITTED` each statement takes a fresh snapshot, so the
    // post-lock `DELETE` observes the pause that committed while we waited.
    // Inheriting the level would let a `default_transaction_isolation =
    // repeatable read` on the database or the role silently disable the
    // guarantee from outside this code; `build_transaction().read_committed()`
    // emits the level on the `BEGIN`, so it travels with the query. This
    // mirrors `queue::claim_task`, which pins the same level for the same
    // fresh-snapshot reason.
    let mut tx = conn.build_transaction().read_committed();
    Box::pin(tx.run::<ResumeOutcome, HarvestError, _>(async |conn| {
        // Same lock the pause path and the timeout enforcer take, so a
        // resume cannot interleave with either.
        //
        // LOCK ORDERING (load-bearing): advisory lock FIRST, then the
        // PENDING task rows the `scheduled_at` shift below updates. The
        // `schedule_to_start` enforcer takes the same two in the same order
        // (see the lock-ordering note in `timeout::enforce_activity_timeout`).
        // Inverting either side would let enforcement hold a task row while
        // waiting on this advisory lock and this transaction hold the
        // advisory lock while waiting on that task row — an ABBA deadlock
        // Postgres would break by aborting one, failing either the timeout
        // pass or the operator's resume.
        lock_queue_for_pause(conn, &queue_owned).await?;

        // Delete-and-return: a single statement that is both the
        // "was it paused?" test and the release, so two concurrent
        // resumes cannot both claim to have released the same hold.
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

        // The ids this pass shifts are what pass 2 excludes -- see the
        // round-19 notes on both queries for why a `created_at` predicate
        // cannot do that job.
        //
        // Round 23 briefly handed this set over server-side in a
        // `CREATE TEMP TABLE ... ON COMMIT DROP` scratch table, to keep
        // application memory O(1) in the released backlog. That was
        // reverted in round 24: `CREATE TEMP TABLE` requires the
        // database-level `TEMPORARY` privilege, which nothing in the
        // migration grants and nothing in preflight validates, so a
        // deployment that has revoked it (a recognised hardening step)
        // would pass every current permission check and then fail EVERY
        // resume -- rolling back the pause deletion and leaving the queue
        // frozen at exactly the moment an operator is trying to thaw it.
        // Trading a rare scale problem for a deterministic, silent,
        // config-dependent break of the feature's core operation is the
        // wrong trade. The memory cost is tracked as a follow-up.
        let shifted: Vec<ShiftedTaskId> = diesel::sql_query(resume_shift_scheduled_at_query())
            .bind::<diesel::sql_types::Text, _>(&queue_owned)
            .bind::<diesel::sql_types::Timestamptz, _>(row.paused_at)
            .load(conn)
            .await?;
        let released = shifted.len();
        let shifted_ids: Vec<uuid::Uuid> = shifted.into_iter().map(|r| r.id).collect();

        // Second pass, LAST so it sees the freshest snapshot: a task still
        // held (our DELETE has not committed, so concurrent claimers still
        // see the pause row) may be invisible to the primary shift above --
        // whether it was created during the hold, or created *before* it by
        // an enqueue that only committed just now. This pass covers BOTH
        // sides of the partition and excludes exactly the rows the primary
        // pass reported shifting, so the two are disjoint by construction
        // (the counts sum exactly, no row is shifted twice) for any commit
        // order rather than only for the ones a `created_at` test happens
        // to separate.
        let late = diesel::sql_query(resume_shift_late_arrivals_query())
            .bind::<diesel::sql_types::Text, _>(&queue_owned)
            .bind::<diesel::sql_types::Timestamptz, _>(row.paused_at)
            .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(&shifted_ids)
            .execute(conn)
            .await?;

        // Wake sleeping listeners (issue #619 round-17 review).
        //
        // AC5 promises held tasks are claimable *immediately* on resume,
        // but a worker with `notification_database_url` set parks in
        // `wait_for_notification(poll_interval)` and only re-polls when a
        // `NOTIFY` arrives on the queue channel. Every held row was
        // enqueued *before* the pause, so its enqueue notification fired
        // long ago and was consumed (or missed) while the queue was held —
        // and nothing about a resume enqueues anything new. Without a
        // notification here the queue therefore stays idle for up to a full
        // `poll_interval` after the thaw. That is invisible at the 500 ms
        // default, but `poll_interval` is configurable and raising it is
        // precisely why an operator configures LISTEN/NOTIFY in the first
        // place, so the deployments most likely to be tuned this way are
        // the ones that would sit idle longest.
        //
        // Emitted INSIDE this transaction on purpose: Postgres queues
        // `NOTIFY` and delivers it at COMMIT, so listeners are woken
        // exactly when the hold actually lifts — never before (a worker
        // woken early would still see the uncommitted pause row and skip
        // the queue) and never lost to a rollback.
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
    .await
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

    /// Surrounding whitespace is rejected rather than trimmed: a queue named
    /// `"payments "` is genuinely reachable (`EnqueueParams` stores names
    /// verbatim), so trimming would hold a *different, valid* queue while
    /// reporting success.
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
    /// wait, the pause-row `DELETE`, and this `UPDATE` — yet the hold stays in
    /// force until commit. Using it silently omits the resume's own runtime from
    /// the credit, so a task whose `schedule_to_start` is shorter than the
    /// resume duration times out the instant the queue thaws: the exact AC3/AC4
    /// violation ("never failed *solely because* of the pause") this query
    /// exists to prevent. This guard exists because the fix is a one-word
    /// change that a later "simplification" could quietly undo.
    #[test]
    fn resume_shift_never_uses_the_frozen_transaction_clock() {
        let sql = resume_shift_scheduled_at_query();
        // Since the partition moved off `transaction_timestamp()` onto the bound
        // `paused_at` (round 16), NO frozen clock belongs anywhere in this query
        // -- not in the credit arithmetic, not in the due predicate, and not in
        // the partition. The earlier carve-out that allowed one in the WHERE half
        // is exactly what the visibility bug hid behind, so the ban is now whole
        // -string and unconditional.
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
    /// `NOW()` is frozen at transaction start — before `lock_queue_for_pause`
    /// waits — so a defaulted `paused_at` predates the effective hold by the
    /// whole lock wait, overstating the reported start/duration and
    /// over-crediting the resume shift. The exact mirror of the round-9 finding,
    /// on the write side.
    #[test]
    fn pause_upsert_stamps_paused_at_from_the_live_clock() {
        let sql = pause_upsert_query();
        let clock = pause_clock_query();
        // Round 20 moved the read out of this statement and into its own, taken
        // at lock acquisition; the live-clock requirement now belongs there.
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
    /// Without this, re-issuing a pause (which the runbook explicitly tells an
    /// operator to do to repair a `partial_fleet` hold) would reset `paused_at`
    /// and erase how long the queue had really been held — and the resume shift
    /// would then credit only from the re-pause, silently under-crediting the
    /// backlog it was supposed to protect.
    #[test]
    fn pause_upsert_does_not_restamp_paused_at_on_a_repause() {
        let sql = pause_upsert_query();
        let (insert_half, after_update) = sql
            .split_once("DO UPDATE")
            .expect("the pause upsert is an INSERT ... ON CONFLICT DO UPDATE");
        // Judge only the conflict SET clause. `RETURNING` sits after it and
        // legitimately names `paused_at` — the handler echoes the EFFECTIVE
        // (preserved) value, which is the round-3 fix.
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

    /// Round-20 P2: `paused_at` must be stamped at **lock acquisition**, before
    /// the backlog scan — the inverse of what round 14 pinned here.
    ///
    /// Round 14 had `pause_queue` scan the backlog first so the stamp landed as
    /// close to `COMMIT` as possible, on the premise that claimers share neither
    /// [`lock_queue_for_pause`]'s advisory lock nor visibility of the uncommitted
    /// pause row, and so keep dispatching until `COMMIT`. That premise was true
    /// then and round 18 made it false: [`try_lock_queue_for_claim`] takes the
    /// **shared** mode of the very key the pause takes exclusively, so the queue
    /// is effectively held from the instant the lock is granted. Stamping after
    /// an unbounded `COUNT(*)` therefore credits none of that genuinely-held
    /// interval back on resume, and a task with a short `schedule_to_start` can
    /// be timed out immediately after the thaw for time the pause transaction
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
    /// Asserted on the source because the ordering *is* the invariant; the
    /// wall-clock consequence is covered by the DB test
    /// `pause_credits_the_backlog_scan_interval` (which makes the scan slow
    /// deterministically by holding an `ACCESS EXCLUSIVE` lock on
    /// `harvest_task_queue`). Mirrors the file-reading guards in
    /// `migration_hygiene`/`ci_run_coverage`.
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

    /// Round-19 P2: the two passes must be made disjoint by the **ids the first
    /// pass actually shifted**, never by re-testing `created_at`.
    ///
    /// A `created_at` predicate on pass 2 is what left the round-19 hole: a row
    /// on pass 1's side that pass 1's snapshot could not see (its enqueue
    /// committed late) was excluded from pass 2 by that very predicate and so
    /// credited by neither pass. Excluding by id instead makes coverage total
    /// for any commit order, while still guaranteeing no row is shifted twice —
    /// pass 1's `+=` credit is *relative*, so a second application would erase
    /// the pre-pause wait it exists to preserve and double-count
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

    /// The partition must be **total**, and `created_at` is nullable (a
    /// pre-`20260619000000` `PENDING` row has none). In SQL a comparison against
    /// `NULL` yields `NULL`, not `false`, so a bare `<` / `>=` split would leave
    /// such a row matching *neither* pass and credited by neither — a silent
    /// regression against the pre-partition query, which had no `created_at`
    /// predicate at all and shifted it. It belongs to the primary pass: no
    /// `created_at` means it was certainly enqueued before this resume began.
    #[test]
    fn the_partition_is_total_for_a_null_created_at() {
        let primary = resume_shift_scheduled_at_query();
        assert!(
            primary.contains("(created_at IS NULL OR created_at < $2)"),
            "a legacy row with no created_at must still be credited; got:\n{primary}"
        );
        // Pass 2 now covers both sides, so the same NULL arm must appear in its
        // CASE -- otherwise a legacy row pass 1 could not see would fall to the
        // absolute arm and be handed a full fresh budget instead of keeping the
        // wait it accrued before the hold.
        let late = resume_shift_late_arrivals_query();
        assert!(
            late.contains("WHEN created_at IS NULL OR created_at < $2"),
            "pass 2's CASE must route a NULL created_at to the relative arm, \
             same as pass 1; got:\n{late}"
        );
    }

    /// The late-arrivals pass must be an **absolute** assignment, never the
    /// primary pass's relative `+=` delta: a row enqueued during the hold has no
    /// pre-pause wait to preserve, and an absolute assignment is idempotent under
    /// repetition (it can only move `scheduled_at` to a later instant that is
    /// still `<= clock_timestamp() <= commit time`, never into the future).
    /// The resume's wake doorbell must pick a genuinely **claimable** row.
    ///
    /// A future-scheduled retry backoff is deliberately not shifted by either
    /// resume pass, so using one as the notify payload would name a task the
    /// woken worker cannot claim. The predicate therefore mirrors the shift
    /// passes' own due-row filter, and stays `LIMIT 1` — this is a doorbell,
    /// not a work assignment.
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

    /// Round-19 P2: pass 2 applies the SAME per-side formula the partition
    /// defines, so a row it has to cover on pass 1's behalf is credited exactly
    /// as pass 1 would have credited it.
    ///
    /// Both arms are load-bearing and independently falsifiable. Collapsing to
    /// the absolute arm alone hands a pre-pause row a full fresh
    /// `schedule_to_start` budget, erasing the wait it had already accrued;
    /// collapsing to the relative arm alone credits a row born during the hold
    /// for wait it never accrued.
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

    /// The drift lock: the `const` the hot claim path embeds must be exactly
    /// what the renderer produces for the unaliased table.
    /// The queue-pause lock must live in the two-argument advisory keyspace,
    /// which is disjoint from the single-argument `bigint` one that
    /// `claim_task`'s per-key concurrency gate hashes `concurrency_key` into.
    ///
    /// Sharing that keyspace is not hygiene: a queue name hash-colliding with
    /// some other queue's `concurrency_key` would make that key's
    /// `pg_try_advisory_xact_lock` fail -- silently, since it is the *try*
    /// variant -- for as long as a large resume held the lock, so an unpaused
    /// queue would stop dispatching during an unrelated queue's resume.
    /// Round-18 P1: the claim-side barrier must be the *shared*, *try* variant
    /// of exactly the key `pause_queue` takes exclusively.
    ///
    /// Each of the three properties is load-bearing and independently
    /// falsifiable: exclusive would serialize every claim on a queue and defeat
    /// `SKIP LOCKED`; a blocking acquire would be an ABBA deadlock against
    /// `resume_queue`'s advisory-then-rows order; and the single-argument
    /// keyspace would collide with `claim_task`'s own `concurrency_key` gate.
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
            // Round-23: a 32-bit key slot is not enough for queue identity.
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

    /// Round-21 P2: the timeout re-check takes the **shared** mode of the pause
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

    /// Round-21 P2: neither `schedule_to_start` enforcement path may reach for
    /// the exclusive lock.
    ///
    /// Source-level because the hazard is a *call site* choosing the wrong
    /// helper — the exclusive function is still correct, and still used, for
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

    /// Round-18 P1: both claim releases share one prefix, so the guard and the
    /// attempt restoration cannot drift apart — and only the paused variant
    /// consults the pause table.
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

    /// Round-23 P2: the advisory key identifies the *queue*, deterministically.
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

    /// Round-23 P2: **both** key slots carry queue identity.
    ///
    /// This is the falsifiable core of the round-23 fix. Before it, slot 1 was
    /// the constant class id `619` and only slot 2 (a 32-bit `hashtext`) told
    /// queues apart, so identity was 32 bits wide and two unrelated queues
    /// collided with probability n²/2³³ -- silently stalling the colliding
    /// queue's dispatch for the length of an unrelated pause or resume.
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

    /// Round-23 P2: this feature owns the two-argument advisory keyspace.
    ///
    /// [`queue_lock_keys`] spends all 64 bits on queue identity rather than
    /// reserving 32 for a class id, so the *only* thing keeping queue-pause
    /// locks from colliding with another subsystem is that no other subsystem
    /// uses the two-argument form. Every other advisory-lock user in the engine
    /// is in the single-argument `bigint` keyspace (`queue::claim_task`'s
    /// concurrency gate, `mutex`, `wasm_store`, `admission_gate`), and this
    /// walks the crate to keep that true -- turning a documented convention
    /// into a CI-enforced one.
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
    /// This is the diagnostic an operator opens *during* a backlog incident, so
    /// its cost must scale with the number of paused queues (a handful) rather
    /// than with the unrelated pending work (potentially millions of rows).
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

    #[test]
    fn const_matches_the_renderer() {
        assert_eq!(
            QUEUE_PAUSE_CLAIM_PREDICATE,
            queue_pause_anti_join("harvest_task_queue")
        );
    }

    /// Regression guard for the subtlest bug in this feature: an *unqualified*
    /// outer reference resolves against `harvest_queue_pauses` inside the
    /// subquery, degenerating to `qp.queue_name = qp.queue_name` — which would
    /// pause every queue the moment any one queue is paused.
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
