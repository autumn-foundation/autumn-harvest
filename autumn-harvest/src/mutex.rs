//! Durable mutual-exclusion locks for workflow code (issue #691).
//!
//! This module is the DB-helper layer behind `ctx.mutex(key).acquire()` — a
//! `WorkflowContext` author primitive that durably suspends the calling workflow
//! until it holds exclusive access to `key`. It owns the raw-SQL access to the
//! two backing tables (`harvest_mutex_locks`, `harvest_mutex_waiters`) and the
//! `harvest_mutex_lock_seq` fencing sequence; the author-facing handle/guard API
//! and the executor/worker suspension wiring land in later milestones.
//!
//! # Shard-local scope
//!
//! Mutex coordination is **shard-local**: the lock row and the FIFO waiter rows
//! live on the contending workflows' own shard, exactly like per-key concurrency
//! limits (issue #247). Two workflows that must exclude one another therefore
//! have to resolve to the **same** shard. Cross-shard global locks are out of
//! scope — there is no cross-shard coordination here.
//!
//! # Lease contract (the honest caveat)
//!
//! Crash safety rests on two mechanisms: a `lease_expires_at` TTL backstop and a
//! monotonic `lock_seq` **fencing token**. If a holder crashes (or its worker
//! stops renewing), the lease scanner reclaims the expired lease and wakes the
//! next waiter. `lock_seq` fences the lock **table**: release/reclaim match on
//! `(holder_exec_id, lock_seq)`, so a reclaimed-then-resumed stale holder's
//! release is a 0-row no-op and the table can never be corrupted.
//!
//! What the engine cannot fence is the **author's external side effects**. The
//! lease TTL must exceed the worst-case wall-clock time a workflow spends inside
//! a single held step (the interval between decision cycles that renew the
//! lease). If the TTL is too short, the scanner may reclaim a lock while the
//! original holder is still mid-critical-section, admitting a second holder that
//! runs concurrently against whatever external resource the lock was protecting.
//! This is the standard lease-lock trade-off; pick a TTL comfortably above the
//! longest expected held step. See [`DEFAULT_MUTEX_LEASE_TTL`] (60s default).
//!
//! # Advisory-first lock ordering (deadlock avoidance)
//!
//! Every acquire / release / reclaim / sweep transaction takes a
//! `pg_advisory_xact_lock(hashtext(key))` **first**, before touching any lock or
//! waiter row. This uniform ordering prevents an ABBA deadlock with the terminal
//! auto-release sweep, which runs *inside* the workflow's terminal-transition
//! commit: if the sweep took a row lock before the advisory lock while acquire
//! took them in the opposite order, a deadlock would roll the terminal commit
//! back. Advisory-first everywhere removes the inversion by construction.
//!
//! `hashtext` produces a 32-bit hash, so two distinct keys can (rarely) collide
//! onto the same advisory-lock slot and be briefly, harmlessly serialized
//! against each other. This never affects correctness: every row operation still
//! matches `lock_key` **exactly**, so a collision only costs a little needless
//! serialization, never a wrong grant or release.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::types::ExecutionId;

/// Default lease TTL for a held durable mutex (issue #691).
///
/// The scanner reclaims a lock whose lease has elapsed; the holder's own
/// decision cycles renew it forward. Must exceed the worst-case single held
/// step — see the module-level lease contract.
pub const DEFAULT_MUTEX_LEASE_TTL: Duration = Duration::from_secs(60);

/// [`DEFAULT_MUTEX_LEASE_TTL`] as `f64` seconds, for the const initializer of
/// [`MUTEX_LEASE_TTL_SECS_BITS`] (`Duration::as_secs_f64` is not `const`).
const DEFAULT_LEASE_TTL_SECS_F64: f64 = 60.0;

/// Process-global effective mutex lease TTL, in **seconds**, stored as the bit
/// pattern of an `f64` ([`f64::to_bits`]) so it carries sub-second precision
/// (mirrors `start_idempotency`'s purge-window static and the
/// `GLOBAL_DEFAULT_WORKFLOW_QUEUE` process-global idiom).
///
/// Set once at worker startup from the configured `WorkerConfig::mutex_lease_ttl`
/// via [`set_mutex_lease_ttl`] (the `From<WorkerConfig>` path), read by the
/// acquire/renewal paths via [`effective_mutex_lease_ttl`]. Defaults to
/// [`DEFAULT_MUTEX_LEASE_TTL`].
static MUTEX_LEASE_TTL_SECS_BITS: AtomicU64 = AtomicU64::new(DEFAULT_LEASE_TTL_SECS_F64.to_bits());

/// Set the process-global mutex lease TTL (issue #691).
///
/// Called at worker startup from `WorkerConfig::mutex_lease_ttl`. A degenerate
/// duration (non-positive or non-finite) is **ignored**: the previous (or
/// default) value is kept and a warning is logged, rather than storing a `0`
/// TTL that would let the scanner reclaim every lock instantly.
pub fn set_mutex_lease_ttl(ttl: Duration) {
    let secs = ttl.as_secs_f64();
    if !(secs.is_finite() && secs > 0.0) {
        tracing::warn!(
            requested_ttl = ?ttl,
            "ignoring degenerate mutex lease TTL (<= 0 or non-finite); keeping previous value"
        );
        return;
    }
    MUTEX_LEASE_TTL_SECS_BITS.store(secs.to_bits(), Ordering::Relaxed);
}

/// The current process-global mutex lease TTL (issue #691).
///
/// Defaults to [`DEFAULT_MUTEX_LEASE_TTL`] until [`set_mutex_lease_ttl`] stores a
/// value. Always finite and positive by construction (the setter rejects
/// degenerate durations), so the `f64`→`Duration` conversion never panics.
#[must_use]
pub fn effective_mutex_lease_ttl() -> Duration {
    let secs = f64::from_bits(MUTEX_LEASE_TTL_SECS_BITS.load(Ordering::Relaxed));
    Duration::from_secs_f64(secs)
}

/// Whether a lease has elapsed relative to `now` (pure; the scanner's reclaim
/// predicate). A lease is expired the instant `now` reaches `lease_expires_at`.
#[must_use]
pub fn lease_expired(lease_expires_at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    now >= lease_expires_at
}

/// Outcome of a single [`try_grant_or_enqueue`] attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MutexGrantOutcome {
    /// The caller now holds the lock; `lock_seq` is its fencing token, to be
    /// matched on the eventual release/reclaim.
    Granted {
        /// The freshly-minted fencing token for this hold.
        lock_seq: i64,
    },
    /// The caller was appended to (or is already in) the FIFO waiter queue and
    /// must park; a later release/reclaim wakes the head of line.
    Enqueued,
}

/// A pending wake target produced when a lock is released or reclaimed: the key
/// whose head-of-line waiter (if any) should be woken.
#[derive(Debug, Clone)]
pub struct MutexWakeTarget {
    /// The lock key that was freed.
    pub lock_key: String,
    /// The next FIFO waiter to wake, or `None` if the queue is now empty.
    pub next_waiter_exec_id: Option<ExecutionId>,
}

// ── SQL statement helpers (each shape-tested; advisory-first callers) ────────

/// Advisory-first serialization lock.
///
/// `$1` = `lock_key`. `hashtext` is 32-bit
/// (rare, harmless cross-key collisions — see the module docs); the cast to
/// `int8` matches `pg_advisory_xact_lock`'s `bigint` argument.
#[must_use]
pub const fn advisory_lock_stmt() -> &'static str {
    "SELECT pg_advisory_xact_lock(hashtext($1)::int8)"
}

/// Append a FIFO waiter idempotently.
///
/// `$1` = `lock_key`, `$2` = `waiter_exec_id`.
/// `ON CONFLICT DO NOTHING` preserves the original BIGSERIAL `id` (and thus FIFO
/// position) across re-parks.
#[must_use]
pub const fn enqueue_waiter_stmt() -> &'static str {
    "INSERT INTO harvest_mutex_waiters (lock_key, waiter_exec_id, requested_at) \
     VALUES ($1, $2, now()) \
     ON CONFLICT (lock_key, waiter_exec_id) DO NOTHING"
}

/// Grant the lock to `$2` **iff** it is the head of line and the key is free (or
/// its lease has expired).
///
/// `$1` = `lock_key`, `$2` = `holder_exec_id`, `$3` =
/// lease TTL seconds (double). Mints the next `lock_seq`, `RETURNING` it on
/// success; no row on failure.
///
/// Relies on the caller having run [`enqueue_waiter_stmt`] first in the same
/// transaction, so the `waiter_exec_id = $2` subquery is never `NULL`.
#[must_use]
pub const fn grant_lock_stmt() -> &'static str {
    "INSERT INTO harvest_mutex_locks (lock_key, holder_exec_id, lock_seq, acquired_at, lease_expires_at) \
     SELECT $1, $2, nextval('harvest_mutex_lock_seq'), now(), now() + make_interval(secs => $3) \
     WHERE NOT EXISTS ( \
         SELECT 1 FROM harvest_mutex_waiters w \
         WHERE w.lock_key = $1 \
           AND w.id < (SELECT id FROM harvest_mutex_waiters WHERE lock_key = $1 AND waiter_exec_id = $2) \
     ) \
     ON CONFLICT (lock_key) DO UPDATE \
         SET holder_exec_id = EXCLUDED.holder_exec_id, \
             lock_seq = EXCLUDED.lock_seq, \
             acquired_at = EXCLUDED.acquired_at, \
             lease_expires_at = EXCLUDED.lease_expires_at \
         WHERE harvest_mutex_locks.lease_expires_at < now() \
     RETURNING lock_seq"
}

/// Read-only "would a grant to `$2` succeed right now?" check: `$2` is the head
/// of line AND the key is either free or its lease has expired.
///
/// `$1` = `lock_key`, `$2` = `waiter_exec_id`. Returns one `grantable` bool row.
#[must_use]
pub const fn is_grantable_head_stmt() -> &'static str {
    "SELECT NOT EXISTS ( \
              SELECT 1 FROM harvest_mutex_waiters w \
              WHERE w.lock_key = $1 \
                AND w.id < (SELECT id FROM harvest_mutex_waiters WHERE lock_key = $1 AND waiter_exec_id = $2) \
            ) \
        AND NOT EXISTS ( \
              SELECT 1 FROM harvest_mutex_locks l \
              WHERE l.lock_key = $1 AND l.lease_expires_at >= now() \
            ) AS grantable"
}

/// The current head-of-line waiter for a key (smallest `id`). `$1` = `lock_key`.
#[must_use]
pub const fn head_of_line_stmt() -> &'static str {
    "SELECT waiter_exec_id FROM harvest_mutex_waiters WHERE lock_key = $1 ORDER BY id ASC LIMIT 1"
}

/// Fenced release: delete the lock row only if `$2`/`$3` still own it.
///
/// `$1` = `lock_key`, `$2` = `holder_exec_id`, `$3` = `lock_seq`. `RETURNING acquired_at`
/// (for the held-duration metric) — no row when fenced out.
#[must_use]
pub const fn release_lock_stmt() -> &'static str {
    "DELETE FROM harvest_mutex_locks \
     WHERE lock_key = $1 AND holder_exec_id = $2 AND lock_seq = $3 \
     RETURNING acquired_at"
}

/// Release by `(key, holder)` with **no** `lock_seq` fence — for the terminal
/// sweep / release-all path where the seq isn't threaded. `$1` = `lock_key`,
/// `$2` = `holder_exec_id`. `RETURNING acquired_at`.
#[must_use]
pub const fn release_lock_by_holder_key_stmt() -> &'static str {
    "DELETE FROM harvest_mutex_locks \
     WHERE lock_key = $1 AND holder_exec_id = $2 \
     RETURNING acquired_at"
}

/// Remove one waiter row. `$1` = `lock_key`, `$2` = `waiter_exec_id`.
#[must_use]
pub const fn delete_holder_waiter_for_key_stmt() -> &'static str {
    "DELETE FROM harvest_mutex_waiters WHERE lock_key = $1 AND waiter_exec_id = $2"
}

/// All keys currently held by `$1` (`holder_exec_id`), ordered for stability.
#[must_use]
pub const fn held_keys_for_holder_stmt() -> &'static str {
    "SELECT lock_key FROM harvest_mutex_locks WHERE holder_exec_id = $1 ORDER BY lock_key"
}

/// All distinct keys `$1` (`waiter_exec_id`) is queued on, ordered for stability.
#[must_use]
pub const fn waiter_keys_for_holder_stmt() -> &'static str {
    "SELECT DISTINCT lock_key FROM harvest_mutex_waiters WHERE waiter_exec_id = $1 ORDER BY lock_key"
}

/// Candidate expired leases whose holder is not `PAUSED` (unlocked SELECT — the
/// scanner re-checks under the advisory lock before reclaiming). No `FOR UPDATE`.
#[must_use]
pub const fn expired_leases_stmt() -> &'static str {
    "SELECT l.lock_key FROM harvest_mutex_locks l \
     WHERE l.lease_expires_at < now() \
       AND NOT EXISTS ( \
         SELECT 1 FROM harvest_workflow_executions e \
         WHERE e.id = l.holder_exec_id AND e.state = 'PAUSED' \
       ) \
     ORDER BY l.lock_key"
}

/// Fenced reclaim: under the advisory lock, delete the lock row only if the
/// lease is *still* expired and the holder is *still* not `PAUSED`.
///
/// `$1` = `lock_key`. `RETURNING holder_exec_id` (the reclaimed holder) — no row if a
/// renewal/resume won the race.
#[must_use]
pub const fn reclaim_expired_lock_stmt() -> &'static str {
    "DELETE FROM harvest_mutex_locks l \
     WHERE l.lock_key = $1 \
       AND l.lease_expires_at < now() \
       AND NOT EXISTS ( \
         SELECT 1 FROM harvest_workflow_executions e \
         WHERE e.id = l.holder_exec_id AND e.state = 'PAUSED' \
       ) \
     RETURNING holder_exec_id"
}

/// Push every lease held by `$1` (`holder_exec_id`) forward by `$2` seconds
/// (double). Single-lock-type UPDATE — no advisory lock needed (no ABBA).
#[must_use]
pub const fn renew_leases_for_holder_stmt() -> &'static str {
    "UPDATE harvest_mutex_locks SET lease_expires_at = now() + make_interval(secs => $2) \
     WHERE holder_exec_id = $1"
}

/// Whether `$1` (`holder_exec_id`) holds any lock — for the reset-reject check.
/// Returns at most one `lock_key` row.
#[must_use]
pub const fn holder_holds_any_stmt() -> &'static str {
    "SELECT lock_key FROM harvest_mutex_locks WHERE holder_exec_id = $1 ORDER BY lock_key LIMIT 1"
}

/// Best-effort grant metrics for `$2` (`waiter_exec_id`) on key `$1`.
///
/// Reads the
/// waiter's `requested_at` (for the wait-duration histogram) and the current
/// total waiter-queue depth for the key (for the contention gauge). The waiter
/// row still exists at grant time — [`grant_lock_stmt`] does not delete it, only
/// the eventual fenced release does — so this reads it directly. Returns at most
/// one row.
#[must_use]
pub const fn waiter_wait_metrics_stmt() -> &'static str {
    "SELECT requested_at, \
            (SELECT count(*) FROM harvest_mutex_waiters WHERE lock_key = $1) AS depth \
     FROM harvest_mutex_waiters WHERE lock_key = $1 AND waiter_exec_id = $2"
}

// ── DB-backed operations (advisory-first; to_regclass-guarded entry points) ──

#[cfg(feature = "db")]
mod db_ops {
    use super::{
        MutexGrantOutcome, advisory_lock_stmt, delete_holder_waiter_for_key_stmt,
        enqueue_waiter_stmt, expired_leases_stmt, grant_lock_stmt, head_of_line_stmt,
        held_keys_for_holder_stmt, holder_holds_any_stmt, is_grantable_head_stmt,
        reclaim_expired_lock_stmt, release_lock_by_holder_key_stmt, release_lock_stmt,
        renew_leases_for_holder_stmt, waiter_keys_for_holder_stmt,
    };
    use crate::error::{HarvestResult, database_error};
    use crate::types::ExecutionId;
    use chrono::{DateTime, Utc};
    use diesel::OptionalExtension;
    use diesel_async::{AsyncPgConnection, RunQueryDsl};
    use std::time::Duration;

    #[derive(diesel::QueryableByName)]
    struct GrantedSeq {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        lock_seq: i64,
    }

    #[derive(diesel::QueryableByName)]
    struct Grantable {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        grantable: bool,
    }

    #[derive(diesel::QueryableByName)]
    struct KeyRow {
        #[diesel(sql_type = diesel::sql_types::Text)]
        lock_key: String,
    }

    #[derive(diesel::QueryableByName)]
    struct WaiterRow {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        waiter_exec_id: uuid::Uuid,
    }

    #[derive(diesel::QueryableByName)]
    struct HolderRow {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        holder_exec_id: uuid::Uuid,
    }

    #[derive(diesel::QueryableByName)]
    struct AcquiredAtRow {
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        acquired_at: DateTime<Utc>,
    }

    #[derive(diesel::QueryableByName)]
    struct TableExists {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        present: bool,
    }

    #[derive(diesel::QueryableByName)]
    struct WaitMetricsRow {
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        requested_at: DateTime<Utc>,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        depth: i64,
    }

    /// Whether the `harvest_mutex_locks` table exists on this connection's
    /// database.
    ///
    /// Per-call (no caching) — a cached answer is unsound under a
    /// staggered multi-shard migration, matching `debounce`/`throttle`/
    /// `start_idempotency`. Cross-suite mutex-table access is guarded by this so
    /// a fresh schema lacking the migration no-ops instead of erroring.
    ///
    /// # Errors
    /// Returns `HarvestError` if the database query fails.
    pub async fn table_present(conn: &mut AsyncPgConnection) -> HarvestResult<bool> {
        let exists: TableExists =
            diesel::sql_query("SELECT to_regclass('harvest_mutex_locks') IS NOT NULL AS present")
                .get_result(conn)
                .await
                .map_err(database_error)?;
        Ok(exists.present)
    }

    /// Take the advisory-first serialization lock for `key`. Held until the
    /// enclosing transaction commits/rolls back.
    async fn advisory_lock(conn: &mut AsyncPgConnection, key: &str) -> HarvestResult<()> {
        diesel::sql_query(advisory_lock_stmt())
            .bind::<diesel::sql_types::Text, _>(key)
            .execute(conn)
            .await
            .map(|_| ())
            .map_err(database_error)
    }

    /// The current head-of-line waiter for `key`, if any.
    async fn head_of_line(
        conn: &mut AsyncPgConnection,
        key: &str,
    ) -> HarvestResult<Option<ExecutionId>> {
        let row: Option<WaiterRow> = diesel::sql_query(head_of_line_stmt())
            .bind::<diesel::sql_types::Text, _>(key)
            .get_result(conn)
            .await
            .optional()
            .map_err(database_error)?;
        Ok(row.map(|r| ExecutionId::from_uuid(r.waiter_exec_id)))
    }

    /// Acquire attempt: advisory-first, enqueue the waiter idempotently, then try
    /// to grant to the head of line.
    ///
    /// **Not** `table_present`-guarded — this runs
    /// only on the acquire path, which is active exactly when the feature is.
    /// Caller must hold the enclosing park transaction.
    ///
    /// # Errors
    /// Returns `HarvestError` if a database query fails.
    pub async fn try_grant_or_enqueue(
        conn: &mut AsyncPgConnection,
        key: &str,
        exec_id: ExecutionId,
        ttl: Duration,
    ) -> HarvestResult<MutexGrantOutcome> {
        advisory_lock(conn, key).await?;

        diesel::sql_query(enqueue_waiter_stmt())
            .bind::<diesel::sql_types::Text, _>(key)
            .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
            .execute(conn)
            .await
            .map_err(database_error)?;

        let granted: Option<GrantedSeq> = diesel::sql_query(grant_lock_stmt())
            .bind::<diesel::sql_types::Text, _>(key)
            .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
            .bind::<diesel::sql_types::Double, _>(ttl.as_secs_f64())
            .get_result(conn)
            .await
            .optional()
            .map_err(database_error)?;

        Ok(granted.map_or(MutexGrantOutcome::Enqueued, |g| {
            MutexGrantOutcome::Granted {
                lock_seq: g.lock_seq,
            }
        }))
    }

    /// Best-effort read of the waiter's `requested_at` and the key's current
    /// waiter-queue depth at grant time.
    ///
    /// Feeds the `harvest.mutex.wait_duration`
    /// and `harvest.mutex.contention_depth` metrics. Returns `None` if the
    /// waiter row is gone (shouldn't happen at grant time). **Not**
    /// `table_present`-guarded — called only on the acquire/grant path.
    ///
    /// # Errors
    /// Returns `HarvestError` if the database query fails.
    pub async fn waiter_wait_metrics(
        conn: &mut AsyncPgConnection,
        key: &str,
        exec_id: ExecutionId,
    ) -> HarvestResult<Option<(DateTime<Utc>, i64)>> {
        let row: Option<WaitMetricsRow> = diesel::sql_query(super::waiter_wait_metrics_stmt())
            .bind::<diesel::sql_types::Text, _>(key)
            .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
            .get_result(conn)
            .await
            .optional()
            .map_err(database_error)?;
        Ok(row.map(|r| (r.requested_at, r.depth)))
    }

    /// Would a grant to `exec_id` on `key` succeed right now? (Post-park
    /// self-wake re-check for the enqueued path.)
    ///
    /// # Errors
    /// Returns `HarvestError` if the database query fails.
    pub async fn is_grantable_head(
        conn: &mut AsyncPgConnection,
        key: &str,
        exec_id: ExecutionId,
    ) -> HarvestResult<bool> {
        let row: Grantable = diesel::sql_query(is_grantable_head_stmt())
            .bind::<diesel::sql_types::Text, _>(key)
            .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
            .get_result(conn)
            .await
            .map_err(database_error)?;
        Ok(row.grantable)
    }

    /// Fenced release of a single lock, then wake the new head of line.
    ///
    /// Returns
    /// the released lock's `acquired_at` (`Some`) — or `None` if the fence
    /// rejected it (a reclaimed-then-resumed stale holder, harmless no-op).
    ///
    /// # Errors
    /// Returns `HarvestError` if a database query fails.
    pub async fn release_lock(
        conn: &mut AsyncPgConnection,
        key: &str,
        holder: ExecutionId,
        lock_seq: i64,
    ) -> HarvestResult<Option<DateTime<Utc>>> {
        advisory_lock(conn, key).await?;

        let released: Option<AcquiredAtRow> = diesel::sql_query(release_lock_stmt())
            .bind::<diesel::sql_types::Text, _>(key)
            .bind::<diesel::sql_types::Uuid, _>(holder.as_uuid())
            .bind::<diesel::sql_types::BigInt, _>(lock_seq)
            .get_result(conn)
            .await
            .optional()
            .map_err(database_error)?;

        let Some(rel) = released else {
            return Ok(None);
        };

        diesel::sql_query(delete_holder_waiter_for_key_stmt())
            .bind::<diesel::sql_types::Text, _>(key)
            .bind::<diesel::sql_types::Uuid, _>(holder.as_uuid())
            .execute(conn)
            .await
            .map_err(database_error)?;

        if let Some(head) = head_of_line(conn, key).await? {
            crate::queue::wake_workflow_task(conn, head).await?;
        }

        Ok(Some(rel.acquired_at))
    }

    /// Release every lock `holder` currently holds and wake each freed key's new
    /// head of line.
    ///
    /// Guarded (no-op if the table is absent). Advisory-first
    /// per key; keys are selected unlocked, then each is locked and freed.
    ///
    /// # Errors
    /// Returns `HarvestError` if a database query fails.
    pub async fn release_all_locks_for_holder(
        conn: &mut AsyncPgConnection,
        holder: ExecutionId,
    ) -> HarvestResult<()> {
        if !table_present(conn).await? {
            return Ok(());
        }

        let keys: Vec<KeyRow> = diesel::sql_query(held_keys_for_holder_stmt())
            .bind::<diesel::sql_types::Uuid, _>(holder.as_uuid())
            .load(conn)
            .await
            .map_err(database_error)?;

        for k in keys {
            advisory_lock(conn, &k.lock_key).await?;

            let released: Option<AcquiredAtRow> =
                diesel::sql_query(release_lock_by_holder_key_stmt())
                    .bind::<diesel::sql_types::Text, _>(&k.lock_key)
                    .bind::<diesel::sql_types::Uuid, _>(holder.as_uuid())
                    .get_result(conn)
                    .await
                    .optional()
                    .map_err(database_error)?;

            if released.is_some() {
                diesel::sql_query(delete_holder_waiter_for_key_stmt())
                    .bind::<diesel::sql_types::Text, _>(&k.lock_key)
                    .bind::<diesel::sql_types::Uuid, _>(holder.as_uuid())
                    .execute(conn)
                    .await
                    .map_err(database_error)?;

                if let Some(head) = head_of_line(conn, &k.lock_key).await? {
                    crate::queue::wake_workflow_task(conn, head).await?;
                }
            }
        }

        Ok(())
    }

    /// Remove every waiter row for `exec_id` across all keys and wake each key's
    /// new head of line.
    ///
    /// Guarded. A spurious wake (the exec wasn't actually the
    /// head) is safe — the woken task simply re-parks.
    ///
    /// # Errors
    /// Returns `HarvestError` if a database query fails.
    pub async fn delete_waiters_for_holder(
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
    ) -> HarvestResult<()> {
        if !table_present(conn).await? {
            return Ok(());
        }

        let keys: Vec<KeyRow> = diesel::sql_query(waiter_keys_for_holder_stmt())
            .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
            .load(conn)
            .await
            .map_err(database_error)?;

        for k in keys {
            advisory_lock(conn, &k.lock_key).await?;

            diesel::sql_query(delete_holder_waiter_for_key_stmt())
                .bind::<diesel::sql_types::Text, _>(&k.lock_key)
                .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
                .execute(conn)
                .await
                .map_err(database_error)?;

            if let Some(head) = head_of_line(conn, &k.lock_key).await? {
                crate::queue::wake_workflow_task(conn, head).await?;
            }
        }

        Ok(())
    }

    /// Reclaim expired-lease locks (crash recovery) and wake each freed key's new
    /// head of line.
    ///
    /// Guarded. Candidates are selected unlocked; each is then
    /// locked (advisory-first) and re-checked under the fenced reclaim statement
    /// so a renewal/resume that won the race is skipped. Returns the number of
    /// locks actually reclaimed.
    ///
    /// # Errors
    /// Returns `HarvestError` if a database query fails.
    pub async fn reclaim_expired_leases_and_wake(
        conn: &mut AsyncPgConnection,
    ) -> HarvestResult<usize> {
        if !table_present(conn).await? {
            return Ok(0);
        }

        let keys: Vec<KeyRow> = diesel::sql_query(expired_leases_stmt())
            .load(conn)
            .await
            .map_err(database_error)?;

        let mut count = 0usize;
        for k in keys {
            advisory_lock(conn, &k.lock_key).await?;

            let reclaimed: Option<HolderRow> = diesel::sql_query(reclaim_expired_lock_stmt())
                .bind::<diesel::sql_types::Text, _>(&k.lock_key)
                .get_result(conn)
                .await
                .optional()
                .map_err(database_error)?;

            if let Some(h) = reclaimed {
                let reclaimed_holder = ExecutionId::from_uuid(h.holder_exec_id);
                diesel::sql_query(delete_holder_waiter_for_key_stmt())
                    .bind::<diesel::sql_types::Text, _>(&k.lock_key)
                    .bind::<diesel::sql_types::Uuid, _>(reclaimed_holder.as_uuid())
                    .execute(conn)
                    .await
                    .map_err(database_error)?;

                if let Some(head) = head_of_line(conn, &k.lock_key).await? {
                    crate::queue::wake_workflow_task(conn, head).await?;
                }
                count += 1;
            }
        }

        Ok(count)
    }

    /// Renew every lease `holder` holds forward by `ttl`. Guarded. No advisory
    /// lock (single-lock-type UPDATE, no ABBA).
    ///
    /// # Errors
    /// Returns `HarvestError` if the database query fails.
    pub async fn renew_leases_for_holder(
        conn: &mut AsyncPgConnection,
        holder: ExecutionId,
        ttl: Duration,
    ) -> HarvestResult<()> {
        if !table_present(conn).await? {
            return Ok(());
        }
        diesel::sql_query(renew_leases_for_holder_stmt())
            .bind::<diesel::sql_types::Uuid, _>(holder.as_uuid())
            .bind::<diesel::sql_types::Double, _>(ttl.as_secs_f64())
            .execute(conn)
            .await
            .map(|_| ())
            .map_err(database_error)
    }

    /// Terminal auto-release: free every lock `exec_id` held and drop every
    /// waiter row it owns, waking new heads of line.
    ///
    /// Guarded. Folded into the
    /// terminal-transition chokepoint in a later milestone.
    ///
    /// # Errors
    /// Returns `HarvestError` if a database query fails.
    pub async fn sweep_terminal_holder_and_wake(
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
    ) -> HarvestResult<()> {
        if !table_present(conn).await? {
            return Ok(());
        }
        release_all_locks_for_holder(conn, exec_id).await?;
        delete_waiters_for_holder(conn, exec_id).await?;
        Ok(())
    }

    /// Any lock key `exec_id` currently holds (for the reset-reject check), or
    /// `None`. Guarded — a missing table means "holds nothing".
    ///
    /// # Errors
    /// Returns `HarvestError` if the database query fails.
    pub async fn holder_holds_any_mutex(
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
    ) -> HarvestResult<Option<String>> {
        if !table_present(conn).await? {
            return Ok(None);
        }
        let row: Option<KeyRow> = diesel::sql_query(holder_holds_any_stmt())
            .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
            .get_result(conn)
            .await
            .optional()
            .map_err(database_error)?;
        Ok(row.map(|r| r.lock_key))
    }
}

#[cfg(feature = "db")]
pub use db_ops::{
    delete_waiters_for_holder, holder_holds_any_mutex, is_grantable_head,
    reclaim_expired_leases_and_wake, release_all_locks_for_holder, release_lock,
    renew_leases_for_holder, sweep_terminal_holder_and_wake, table_present, try_grant_or_enqueue,
    waiter_wait_metrics,
};

#[cfg(test)]
mod pure_tests {
    use super::*;

    /// Serializes the mutex-lease-TTL global setter tests against each other so
    /// they don't race in the shared lib-test binary (mirrors
    /// `start_idempotency::PURGE_WINDOW_TEST_LOCK`).
    static LEASE_TTL_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn default_ttl_is_sixty_seconds() {
        assert_eq!(DEFAULT_MUTEX_LEASE_TTL.as_secs(), 60);
        assert!((DEFAULT_LEASE_TTL_SECS_F64 - DEFAULT_MUTEX_LEASE_TTL.as_secs_f64()).abs() < 1e-9);
    }

    #[test]
    fn set_and_effective_ttl_roundtrips_and_rejects_degenerate() {
        let _guard = LEASE_TTL_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        // Whole and sub-second values round-trip.
        set_mutex_lease_ttl(Duration::from_secs(120));
        assert_eq!(effective_mutex_lease_ttl(), Duration::from_secs(120));

        set_mutex_lease_ttl(Duration::from_millis(1500));
        assert!((effective_mutex_lease_ttl().as_secs_f64() - 1.5).abs() < 1e-9);

        // A degenerate (zero) TTL is ignored: the previous value is kept, never
        // stored as 0.
        set_mutex_lease_ttl(Duration::ZERO);
        assert!((effective_mutex_lease_ttl().as_secs_f64() - 1.5).abs() < 1e-9);

        // Restore the default so ordering-independent test runs don't leak state.
        set_mutex_lease_ttl(DEFAULT_MUTEX_LEASE_TTL);
        assert_eq!(effective_mutex_lease_ttl(), DEFAULT_MUTEX_LEASE_TTL);
    }

    #[test]
    fn lease_expired_truth_table() {
        let base = DateTime::parse_from_rfc3339("2026-07-20T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let before = base - chrono::Duration::seconds(1);
        let after = base + chrono::Duration::seconds(1);
        // Not yet expired.
        assert!(!lease_expired(base, before));
        // Expired exactly at the deadline (>=).
        assert!(lease_expired(base, base));
        // Expired past the deadline.
        assert!(lease_expired(base, after));
    }

    #[test]
    fn grant_outcome_equality() {
        assert_eq!(
            MutexGrantOutcome::Granted { lock_seq: 7 },
            MutexGrantOutcome::Granted { lock_seq: 7 }
        );
        assert_ne!(
            MutexGrantOutcome::Granted { lock_seq: 7 },
            MutexGrantOutcome::Granted { lock_seq: 8 }
        );
        assert_ne!(
            MutexGrantOutcome::Enqueued,
            MutexGrantOutcome::Granted { lock_seq: 1 }
        );
        assert_eq!(MutexGrantOutcome::Enqueued, MutexGrantOutcome::Enqueued);
    }

    #[test]
    fn advisory_lock_stmt_serializes_on_hashtext() {
        let sql = advisory_lock_stmt();
        assert!(sql.contains("pg_advisory_xact_lock"));
        assert!(sql.contains("hashtext($1)::int8"));
    }

    #[test]
    fn enqueue_waiter_stmt_is_idempotent_insert() {
        let sql = enqueue_waiter_stmt();
        assert!(sql.contains("INSERT INTO harvest_mutex_waiters"));
        assert!(sql.contains("ON CONFLICT (lock_key, waiter_exec_id) DO NOTHING"));
        assert!(sql.contains("$1"));
        assert!(sql.contains("$2"));
    }

    #[test]
    fn grant_lock_stmt_is_head_of_line_upsert_returning_seq() {
        let sql = grant_lock_stmt();
        assert!(sql.contains("INSERT INTO harvest_mutex_locks"));
        assert!(sql.contains("nextval('harvest_mutex_lock_seq')"));
        // Head-of-line guard: no smaller-id waiter for the key.
        assert!(sql.contains("NOT EXISTS"));
        assert!(sql.contains("w.id <"));
        // Expired-lease takeover.
        assert!(sql.contains("ON CONFLICT (lock_key) DO UPDATE"));
        assert!(sql.contains("WHERE harvest_mutex_locks.lease_expires_at < now()"));
        assert!(sql.contains("RETURNING lock_seq"));
        assert!(sql.contains("make_interval(secs => $3)"));
    }

    #[test]
    fn is_grantable_head_stmt_returns_grantable_bool() {
        let sql = is_grantable_head_stmt();
        assert!(sql.contains("AS grantable"));
        assert!(sql.contains("NOT EXISTS"));
        assert!(sql.contains("l.lease_expires_at >= now()"));
    }

    #[test]
    fn head_of_line_stmt_orders_by_id_asc() {
        let sql = head_of_line_stmt();
        assert!(sql.contains("SELECT waiter_exec_id FROM harvest_mutex_waiters"));
        assert!(sql.contains("ORDER BY id ASC"));
        assert!(sql.contains("LIMIT 1"));
    }

    #[test]
    fn release_lock_stmt_is_fenced_and_returns_acquired_at() {
        let sql = release_lock_stmt();
        assert!(sql.contains("DELETE FROM harvest_mutex_locks"));
        assert!(sql.contains("holder_exec_id = $2"));
        assert!(sql.contains("lock_seq = $3"));
        assert!(sql.contains("RETURNING acquired_at"));
    }

    #[test]
    fn release_by_holder_key_stmt_has_no_seq_fence() {
        let sql = release_lock_by_holder_key_stmt();
        assert!(sql.contains("DELETE FROM harvest_mutex_locks"));
        assert!(sql.contains("holder_exec_id = $2"));
        assert!(!sql.contains("lock_seq"));
        assert!(sql.contains("RETURNING acquired_at"));
    }

    #[test]
    fn delete_holder_waiter_stmt_targets_key_and_exec() {
        let sql = delete_holder_waiter_for_key_stmt();
        assert!(sql.contains("DELETE FROM harvest_mutex_waiters"));
        assert!(sql.contains("lock_key = $1"));
        assert!(sql.contains("waiter_exec_id = $2"));
    }

    #[test]
    fn held_and_waiter_keys_stmts_order_by_key() {
        let held = held_keys_for_holder_stmt();
        assert!(held.contains("SELECT lock_key FROM harvest_mutex_locks"));
        assert!(held.contains("ORDER BY lock_key"));
        let waiting = waiter_keys_for_holder_stmt();
        assert!(waiting.contains("SELECT DISTINCT lock_key FROM harvest_mutex_waiters"));
        assert!(waiting.contains("ORDER BY lock_key"));
    }

    #[test]
    fn expired_and_reclaim_stmts_skip_paused_holders() {
        let expired = expired_leases_stmt();
        assert!(expired.contains("lease_expires_at < now()"));
        assert!(expired.contains("e.state = 'PAUSED'"));
        assert!(expired.contains("ORDER BY l.lock_key"));
        assert!(!expired.contains("FOR UPDATE"));

        let reclaim = reclaim_expired_lock_stmt();
        assert!(reclaim.contains("DELETE FROM harvest_mutex_locks"));
        assert!(reclaim.contains("lease_expires_at < now()"));
        assert!(reclaim.contains("e.state = 'PAUSED'"));
        assert!(reclaim.contains("RETURNING holder_exec_id"));
    }

    #[test]
    fn renew_and_holds_any_stmts() {
        let renew = renew_leases_for_holder_stmt();
        assert!(renew.contains("UPDATE harvest_mutex_locks SET lease_expires_at"));
        assert!(renew.contains("make_interval(secs => $2)"));
        assert!(renew.contains("holder_exec_id = $1"));

        let holds = holder_holds_any_stmt();
        assert!(holds.contains("SELECT lock_key FROM harvest_mutex_locks"));
        assert!(holds.contains("LIMIT 1"));
    }

    #[test]
    fn waiter_wait_metrics_stmt_reads_requested_at_and_depth() {
        let sql = waiter_wait_metrics_stmt();
        assert!(sql.contains("requested_at"));
        assert!(sql.contains("count(*)"));
        assert!(sql.contains("AS depth"));
        assert!(sql.contains("waiter_exec_id = $2"));
    }
}
