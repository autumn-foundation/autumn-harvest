//! Request-scoped idempotency keys for plain workflow starts (issue #808).
//!
//! # Overview
//!
//! `POST /workflows/{name}/start` accepts an optional `idempotency_key`. Two
//! starts carrying the *same* key (within a retention window, default 24h)
//! converge on exactly **one** execution: the first creates it, the second is a
//! no-op returning the *same* `execution_id` with success — no `409`, no second
//! `WorkflowStarted` event, no second enqueued task.
//!
//! # Delivery identity vs business identity
//!
//! `idempotency_key` is a **delivery-identity** dedupe (this exact request),
//! scoped to `(workflow_name, idempotency_key)` and **independent of
//! `workflow_id`** — so it converges even when `workflow_id` is auto-generated
//! per request. It is layered *in front of* the **business-identity**
//! reuse-policy matrix keyed on `workflow_id`: the idempotency check
//! short-circuits the reuse-policy matrix entirely. When a start is *not*
//! deduplicated (fresh key), the normal reuse-policy semantics apply unchanged.
//!
//! Because the dedupe is `workflow_id`-independent, a keyed start must **route
//! by the key**, not by `workflow_id`. The HTTP handler picks the shard via
//! [`crate::shard::ShardRouter::pick_for_idempotency_key`] (rendezvous over
//! `(workflow_name, idempotency_key)`), so two same-key retries — whose
//! `workflow_id` may be auto-generated per request — deterministically
//! co-locate on one shard and hit the same claim row. The claim row, its
//! execution, and the per-shard expiry sweep all share that key-derived shard.
//!
//! # Concurrency
//!
//! Safety comes from the `(workflow_name, idempotency_key)` primary key and a
//! single `INSERT ... ON CONFLICT DO UPDATE` upsert: N simultaneous same-key
//! starts race to insert one row; the winner reserves, every loser observes the
//! winner's row and returns its `execution_id`. Wrapping the reserve + start in
//! one transaction makes a start that fails *after* reserving roll the claim
//! back so a retry can start fresh.
//!
//! # Retention & reuse
//!
//! The upsert's `ON CONFLICT ... DO UPDATE ... WHERE created_at <= now() -
//! window` clause lets a key be reused once the window elapses: a stale row is
//! overwritten in place and the start proceeds as fresh. A background sweep
//! ([`purge_expired_start_idempotency`], folded into
//! `timeout::enforce_timeouts_once`) deletes expired rows so the table can't
//! grow unbounded for keys that are never reused. Even with no sweep,
//! correctness holds — an expired row is overwritten on the next same-key start.
//!
//! # Scoping
//!
//! Shard-local. The claim row and its execution live on the same shard (no
//! cross-shard coordination), consistent with per-key concurrency (#247) and the
//! start throttle (#607). There is deliberately **no** foreign key to
//! `harvest_workflow_executions`: a claim pointing at a retention-deleted
//! execution is handled by the defensive reclaim path in
//! [`reserve_start_idempotency`] plus the expiry sweep.
//!
//! # Replay safety
//!
//! This is a **pre-start admission dedupe** that runs before `WorkflowStarted`
//! is appended. It introduces **no new `WorkflowEvent` variant**, no migration
//! to the event schema, and has zero effect on replay determinism or shard
//! routing.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Default retention window within which a repeated `idempotency_key`
/// deduplicates onto the same execution. After it elapses the key is reusable.
pub const DEFAULT_START_IDEMPOTENCY_WINDOW: Duration = Duration::from_secs(24 * 60 * 60);

/// Maximum rows deleted per shard per expiry sweep tick, bounding the sweep's
/// per-tick cost on a large backlog.
pub const START_IDEMPOTENCY_PURGE_BATCH: i64 = 1000;

/// Maximum accepted length (in bytes, after trimming) for a request-scoped
/// `idempotency_key` on any start path.
///
/// Applied by both the plain-start HTTP route
/// (`autumn-harvest-plugin::api::validate_start_idempotency_key`) and
/// [`crate::handle::TransactionalStartOptions::with_idempotency_key`] (issue
/// #763's review). The key is half of the composite PRIMARY KEY
/// `(workflow_name, idempotency_key)` on `harvest_start_idempotency`; an
/// oversized key would exceed Postgres's ~2704-byte btree index tuple limit
/// and error at INSERT, leaking a raw database error on client-controlled
/// input. Capped conservatively so `workflow_name` + key stays well under
/// that limit.
pub const MAX_START_IDEMPOTENCY_KEY_LEN: usize = 512;

/// Shared trim + empty-rejection + length-cap validation for a request-scoped
/// `idempotency_key`.
///
/// Every start path should agree on what a "valid" key looks like, rather
/// than each independently (re)implementing -- or, as issue #763's review
/// found for the transactional-start client, omitting -- the same rule. An
/// unvalidated key would let an accidental empty/whitespace value become a
/// real `(workflow_name, "")` claim that silently dedupes every *other*
/// unrelated start for that workflow against each other.
///
/// `Ok(None)` = no key supplied; `Ok(Some(trimmed))` = a valid, trimmed key;
/// `Err(_)` = the key is present but empty/whitespace-only, or exceeds
/// [`MAX_START_IDEMPOTENCY_KEY_LEN`] bytes after trimming.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Config`] naming the specific
/// violation (empty vs. too long).
pub fn validate_start_idempotency_key(
    raw: Option<&str>,
) -> crate::error::HarvestResult<Option<String>> {
    match raw {
        None => Ok(None),
        Some(s) if s.trim().is_empty() => Err(crate::error::HarvestError::Config(
            "idempotency_key must not be empty".to_string(),
        )),
        Some(s) => {
            let trimmed = s.trim();
            if trimmed.len() > MAX_START_IDEMPOTENCY_KEY_LEN {
                Err(crate::error::HarvestError::Config(format!(
                    "idempotency_key too long (max {MAX_START_IDEMPOTENCY_KEY_LEN} bytes)"
                )))
            } else {
                Ok(Some(trimmed.to_string()))
            }
        }
    }
}

/// The default retention window expressed as `f64` seconds, for the const
/// initializer of [`PURGE_WINDOW_SECS_BITS`] (`Duration::as_secs_f64` is not
/// `const`).
const DEFAULT_WINDOW_SECS_F64: f64 = 24.0 * 60.0 * 60.0;

/// Process-global expiry-sweep retention window in **seconds**, stored as the
/// bit pattern of an `f64` ([`f64::to_bits`]) so it carries the **same
/// sub-second precision as the reserve path's `Duration::as_secs_f64()`**
/// (issue #808, Codex P2).
///
/// The reserve `WHERE created_at <= now() - make_interval(secs => $window)`
/// guard treats a key as live for exactly `window` seconds. If the sweep used a
/// *truncated* whole-second window (`Duration::as_secs()`), a 1500 ms config
/// would let the sweep DELETE a claim after 1.0 s while the reserve still
/// considers it live until 1.5 s — a same-key retry at 1.2 s would then find no
/// claim and start a **second** execution inside the configured window. Storing
/// the window as `f64` seconds guarantees the sweep window *equals* the reserve
/// window, so the sweep never deletes a claim the reserve still considers live.
///
/// Set once at `Plugin::build` time from the same `HarvestBuilder` config the
/// HTTP reserve path reads, via [`set_purge_window_secs`] — mirroring the
/// `GLOBAL_CALLBACK_CONFIG` pattern (issue #605) so the sweep does not require a
/// new parameter threaded through `enforce_timeouts_once` /
/// `spawn_timeout_checker` and their many call sites. Defaults to
/// [`DEFAULT_START_IDEMPOTENCY_WINDOW`].
static PURGE_WINDOW_SECS_BITS: AtomicU64 = AtomicU64::new(DEFAULT_WINDOW_SECS_F64.to_bits());

/// Set the process-global expiry-sweep retention window (issue #808).
///
/// Called at plugin/runner startup from the configured
/// `HarvestBuilder::start_idempotency_window`. Stored with full sub-second
/// precision (matching the reserve path). A non-positive or non-finite duration
/// falls back to [`DEFAULT_START_IDEMPOTENCY_WINDOW`].
pub fn set_purge_window_secs(window: Duration) {
    let secs = window.as_secs_f64();
    let effective = if secs.is_finite() && secs > 0.0 {
        secs
    } else {
        DEFAULT_START_IDEMPOTENCY_WINDOW.as_secs_f64()
    };
    PURGE_WINDOW_SECS_BITS.store(effective.to_bits(), Ordering::Relaxed);
}

/// The current process-global expiry-sweep retention window in seconds
/// (sub-second precise), matching the reserve path's window exactly.
#[must_use]
pub fn purge_window_secs() -> f64 {
    f64::from_bits(PURGE_WINDOW_SECS_BITS.load(Ordering::Relaxed))
}

/// The `INSERT ... ON CONFLICT DO UPDATE` reserve/claim statement.
///
/// Extracted as a `const fn` so a no-DB shape test can assert the exact SQL
/// string (mirroring the `*_stmt!` shape-test precedent for the backfill
/// reservation, issue #688) — editing the guard, the `SET` list, or the
/// `RETURNING` clause breaks that test rather than silently drifting.
///
/// Binds:
/// - `$1` `workflow_name`, `$2` `idempotency_key`, `$3` new `workflow_exec_id`,
///   `$4` `shard_id`, `$5` retention window in seconds.
///
/// On a conflicting row *older than* the window it overwrites the claim in place
/// (key reuse) and `RETURNING`s the new id; on a conflicting row *within* the
/// window the `WHERE` guard is false, no row is updated, and the caller resolves
/// the existing claim via [`reserve_start_idempotency`]'s duplicate branch. The
/// window is bound as a **parameter** (not string-interpolated).
#[must_use]
pub const fn reserve_start_idempotency_stmt() -> &'static str {
    "INSERT INTO harvest_start_idempotency AS s \
        (workflow_name, idempotency_key, workflow_exec_id, created_at, shard_id) \
     VALUES ($1, $2, $3, now(), $4) \
     ON CONFLICT (workflow_name, idempotency_key) DO UPDATE \
        SET workflow_exec_id = EXCLUDED.workflow_exec_id, \
            created_at = EXCLUDED.created_at, \
            shard_id = EXCLUDED.shard_id \
        WHERE s.created_at <= now() - make_interval(secs => $5) \
     RETURNING workflow_exec_id"
}

/// Outcome of a [`reserve_start_idempotency`] attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartIdempotencyReservation {
    /// This request won (or re-won, after window expiry) the claim for its key.
    /// The caller must now perform the actual workflow start with the reserved
    /// `execution_id` inside the same transaction.
    Reserved,
    /// An earlier start already claimed this key and its execution still exists.
    /// The caller must NOT start; it returns this existing run as a no-op.
    Duplicate {
        /// The already-created execution's id.
        exec_id: crate::types::ExecutionId,
        /// The already-created execution's `workflow_id`.
        workflow_id: String,
        /// The already-created execution's current state string.
        state: String,
    },
}

/// Convert whole/sub-second [`Duration`] to the `f64` seconds bound as `$5`.
#[must_use]
pub const fn window_to_secs_f64(window: Duration) -> f64 {
    window.as_secs_f64()
}

/// Serializes tests that mutate and read back the process-global
/// [`PURGE_WINDOW_SECS_BITS`] static so they don't race each other in the shared
/// lib-test binary (issue #808, Codex P2). Any test that calls
/// `set_purge_window_secs` and then asserts on `purge_window_secs()` — here or in
/// `builder.rs` — must hold this lock for the set→read span.
#[cfg(test)]
pub(crate) static PURGE_WINDOW_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
mod pure_tests {
    use super::*;

    #[test]
    fn reserve_stmt_is_a_conflict_guarded_upsert_returning_exec_id() {
        let sql = reserve_start_idempotency_stmt();
        // Upsert onto the composite PK, guarded on the retention window as a
        // bound parameter, returning the (possibly-reclaimed) exec id.
        assert!(sql.contains("INSERT INTO harvest_start_idempotency"));
        assert!(sql.contains("ON CONFLICT (workflow_name, idempotency_key) DO UPDATE"));
        assert!(sql.contains("WHERE s.created_at <= now() - make_interval(secs => $5)"));
        assert!(sql.contains("RETURNING workflow_exec_id"));
        // The window must be a bound param, never interpolated.
        assert!(sql.contains("$5"));
        assert!(sql.contains("$1"));
        assert!(sql.contains("$4"));
    }

    #[test]
    fn default_window_is_twenty_four_hours() {
        assert_eq!(DEFAULT_START_IDEMPOTENCY_WINDOW.as_secs(), 86_400);
    }

    #[test]
    fn window_to_secs_f64_roundtrips() {
        assert!((window_to_secs_f64(Duration::from_secs(3600)) - 3600.0).abs() < 1e-9);
        assert!((window_to_secs_f64(Duration::from_millis(1500)) - 1.5).abs() < 1e-9);
    }

    #[test]
    fn purge_window_precision_and_clamping() {
        let _guard = PURGE_WINDOW_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Sub-second precision (issue #808, Codex P2): the sweep window must be
        // >= the reserve window (`Duration::as_secs_f64()`) so the sweep never
        // deletes a claim the reserve still considers live. A truncating u64
        // store would make the sweep window 1.0s for a 1500ms config while the
        // reserve treats the key as live until 1.5s — a sub-second dedup gap.
        let window = Duration::from_millis(1500);
        set_purge_window_secs(window);
        let sweep_window = purge_window_secs();
        assert!(
            sweep_window >= window.as_secs_f64(),
            "sweep window {sweep_window} must not be shorter than reserve window {}",
            window.as_secs_f64()
        );
        assert!((sweep_window - 1.5).abs() < 1e-9);

        // Whole-second window round-trips exactly.
        set_purge_window_secs(Duration::from_secs(7200));
        assert!((purge_window_secs() - 7200.0).abs() < 1e-9);

        // Non-positive falls back to the default window.
        set_purge_window_secs(Duration::ZERO);
        assert!(
            (purge_window_secs() - DEFAULT_START_IDEMPOTENCY_WINDOW.as_secs_f64()).abs() < 1e-9
        );

        // Restore default so ordering-independent test runs don't leak state.
        set_purge_window_secs(DEFAULT_START_IDEMPOTENCY_WINDOW);
    }
}

// ── DB-backed reserve / purge ────────────────────────────────────────────────

#[cfg(feature = "db")]
mod db_impl {
    use super::{StartIdempotencyReservation, reserve_start_idempotency_stmt};
    use crate::error::{HarvestResult, database_error};
    use crate::types::ExecutionId;
    use diesel::OptionalExtension;
    use diesel_async::{AsyncPgConnection, RunQueryDsl};

    #[derive(diesel::QueryableByName)]
    struct ReturnedExecId {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        workflow_exec_id: uuid::Uuid,
    }

    #[derive(diesel::QueryableByName)]
    struct ExistingRun {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: uuid::Uuid,
        #[diesel(sql_type = diesel::sql_types::Text)]
        workflow_id: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
    }

    async fn run_reserve_upsert(
        conn: &mut AsyncPgConnection,
        workflow_name: &str,
        key: &str,
        new_exec_id: ExecutionId,
        shard_id: i32,
        window_secs: f64,
    ) -> HarvestResult<Option<uuid::Uuid>> {
        let row: Option<ReturnedExecId> = diesel::sql_query(reserve_start_idempotency_stmt())
            .bind::<diesel::sql_types::Text, _>(workflow_name)
            .bind::<diesel::sql_types::Text, _>(key)
            .bind::<diesel::sql_types::Uuid, _>(new_exec_id.as_uuid())
            .bind::<diesel::sql_types::Integer, _>(shard_id)
            .bind::<diesel::sql_types::Double, _>(window_secs)
            .get_result(conn)
            .await
            .optional()
            .map_err(database_error)?;
        Ok(row.map(|r| r.workflow_exec_id))
    }

    /// Load the existing execution a claim points at, joining through the claim
    /// row so a retention-deleted (dangling) pointer returns `None`.
    async fn load_claimed_execution(
        conn: &mut AsyncPgConnection,
        workflow_name: &str,
        key: &str,
    ) -> HarvestResult<Option<ExistingRun>> {
        let row: Option<ExistingRun> = diesel::sql_query(
            "SELECT we.id, we.workflow_id, we.state \
             FROM harvest_start_idempotency si \
             JOIN harvest_workflow_executions we ON we.id = si.workflow_exec_id \
             WHERE si.workflow_name = $1 AND si.idempotency_key = $2",
        )
        .bind::<diesel::sql_types::Text, _>(workflow_name)
        .bind::<diesel::sql_types::Text, _>(key)
        .get_result(conn)
        .await
        .optional()
        .map_err(database_error)?;
        Ok(row)
    }

    /// Outcome of a single upsert-and-classify pass, *without* any reclaim.
    ///
    /// The [`reserve_start_idempotency`] entry point runs this once, and — only
    /// on [`ReserveClassification::Dangling`] — performs a guarded delete and
    /// runs it exactly once more to re-classify.
    enum ReserveClassification {
        /// The upsert wrote our new id (fresh insert or window-expired reclaim).
        Reserved,
        /// A live claim within the window points at an existing execution.
        Duplicate(ExistingRun),
        /// A claim row exists but points at an execution that no longer exists.
        Dangling,
    }

    /// Run the reserve upsert once and classify the result, performing **no**
    /// reclaim. Robust to both Postgres interpretations of a `WHERE`-false
    /// `DO UPDATE` (`RETURNING` may yield the pre-existing row or nothing).
    async fn try_reserve_once(
        conn: &mut AsyncPgConnection,
        workflow_name: &str,
        key: &str,
        new_exec_id: ExecutionId,
        shard_id: i32,
        window_secs: f64,
    ) -> HarvestResult<ReserveClassification> {
        let returned =
            run_reserve_upsert(conn, workflow_name, key, new_exec_id, shard_id, window_secs)
                .await?;

        // Fresh insert OR a window-expired reclaim: the upsert wrote our new id.
        if returned == Some(new_exec_id.as_uuid()) {
            return Ok(ReserveClassification::Reserved);
        }

        // Either the conflict guard was false and RETURNING gave the old id
        // (`Some(other)`), or it gave nothing (`None`). In both cases a claim
        // exists within the window — resolve it by loading the pointed-at
        // execution through the join. A live execution → Duplicate; a dangling
        // pointer (execution retention-deleted) → Dangling.
        let existing = load_claimed_execution(conn, workflow_name, key).await?;
        Ok(existing.map_or(
            ReserveClassification::Dangling,
            ReserveClassification::Duplicate,
        ))
    }

    /// Reserve a start idempotency claim for `(workflow_name, key)`.
    ///
    /// Returns [`StartIdempotencyReservation::Reserved`] when this request should
    /// perform the start with `new_exec_id`, or
    /// [`StartIdempotencyReservation::Duplicate`] when an earlier start already
    /// claimed the key and its execution still exists (return that run as a
    /// no-op).
    ///
    /// Concurrency-safe via the composite-PK upsert. Robust to both Postgres
    /// interpretations of a `WHERE`-false `DO UPDATE` (`RETURNING` may yield the
    /// pre-existing row or nothing) and to a dangling claim whose execution was
    /// deleted by retention.
    ///
    /// # Errors
    /// Propagates database failures.
    pub async fn reserve_start_idempotency(
        conn: &mut AsyncPgConnection,
        workflow_name: &str,
        key: &str,
        new_exec_id: ExecutionId,
        shard_id: i32,
        window_secs: f64,
    ) -> HarvestResult<StartIdempotencyReservation> {
        match try_reserve_once(conn, workflow_name, key, new_exec_id, shard_id, window_secs).await?
        {
            ReserveClassification::Reserved => Ok(StartIdempotencyReservation::Reserved),
            ReserveClassification::Duplicate(existing) => {
                Ok(StartIdempotencyReservation::Duplicate {
                    exec_id: ExecutionId::from_uuid(existing.id),
                    workflow_id: existing.workflow_id,
                    state: existing.state,
                })
            }
            ReserveClassification::Dangling => {
                // Defensive reclaim path: the claim row exists but points at an
                // execution that no longer exists (a dangling pointer — only
                // reachable if the idempotency window exceeds execution retention
                // and the sweep hasn't run yet). Delete the stale claim, but ONLY
                // if it still points at a non-existent execution: the `NOT EXISTS`
                // guard makes this safe against a concurrent same-key request that
                // repointed the claim to a real new run between our classifying
                // JOIN above and this delete — that request's valid claim is
                // preserved, so we never delete a live claim and never produce a
                // duplicate execution. Binds `workflow_name`/`key` as parameters.
                diesel::sql_query(
                    "DELETE FROM harvest_start_idempotency \
                     WHERE workflow_name = $1 AND idempotency_key = $2 \
                       AND NOT EXISTS ( \
                           SELECT 1 FROM harvest_workflow_executions we \
                           WHERE we.id = harvest_start_idempotency.workflow_exec_id \
                       )",
                )
                .bind::<diesel::sql_types::Text, _>(workflow_name)
                .bind::<diesel::sql_types::Text, _>(key)
                .execute(conn)
                .await
                .map_err(database_error)?;

                // Re-classify ONCE after the guarded delete. Because the delete is
                // guarded, it may have removed 0 rows (a concurrent request already
                // repointed the claim to a valid run), so we must NOT assume
                // Reserved — re-run the classify and honour whatever now owns the
                // key.
                match try_reserve_once(conn, workflow_name, key, new_exec_id, shard_id, window_secs)
                    .await?
                {
                    // Stale row removed (or expired) — our insert won.
                    ReserveClassification::Reserved => Ok(StartIdempotencyReservation::Reserved),
                    // A concurrent valid claim now owns the key — dedup to it.
                    ReserveClassification::Duplicate(existing) => {
                        Ok(StartIdempotencyReservation::Duplicate {
                            exec_id: ExecutionId::from_uuid(existing.id),
                            workflow_id: existing.workflow_id,
                            state: existing.state,
                        })
                    }
                    // Practically unreachable (the guarded delete + upsert leaves
                    // either our row or a concurrent valid claim). Surface it
                    // rather than guessing a reservation that could duplicate.
                    ReserveClassification::Dangling => Err(database_error(
                        "start idempotency claim still dangling after guarded reclaim; \
                         refusing to reserve to avoid a duplicate execution",
                    )),
                }
            }
        }
    }

    /// Read-only probe for a live (within-window) start idempotency claim.
    ///
    /// Returns the claimed execution's id when a claim row for `(workflow_name,
    /// key)` exists, was created within `window_secs`, **and** still points at a
    /// live execution (the JOIN mirrors [`reserve_start_idempotency`]'s
    /// `Duplicate` classification — a within-window claim whose execution was
    /// retention-deleted is *not* a dedup hit, it reclaims to a fresh start).
    ///
    /// Used by the admission-gate idempotent-retry bypass (issue #808): a keyed
    /// replay of an already-successful start resolves to a `200` no-op, so it is
    /// **not** fresh admission and a raised gate must not reject it. Side-effect
    /// free (no upsert, no reclaim) — a genuinely fresh keyed start finds no
    /// claim and still passes through the gate normally.
    ///
    /// # Errors
    /// Propagates database failures.
    pub async fn lookup_live_start_idempotency_claim(
        conn: &mut AsyncPgConnection,
        workflow_name: &str,
        key: &str,
        window_secs: f64,
    ) -> HarvestResult<Option<ExecutionId>> {
        #[derive(diesel::QueryableByName)]
        struct ClaimedId {
            #[diesel(sql_type = diesel::sql_types::Uuid)]
            id: uuid::Uuid,
        }
        let row: Option<ClaimedId> = diesel::sql_query(
            "SELECT we.id \
             FROM harvest_start_idempotency si \
             JOIN harvest_workflow_executions we ON we.id = si.workflow_exec_id \
             WHERE si.workflow_name = $1 AND si.idempotency_key = $2 \
               AND si.created_at > now() - make_interval(secs => $3)",
        )
        .bind::<diesel::sql_types::Text, _>(workflow_name)
        .bind::<diesel::sql_types::Text, _>(key)
        .bind::<diesel::sql_types::Double, _>(window_secs)
        .get_result(conn)
        .await
        .optional()
        .map_err(database_error)?;
        Ok(row.map(|r| ExecutionId::from_uuid(r.id)))
    }

    /// Repoint an existing claim at the execution the start actually resolved to.
    ///
    /// The reserve upsert writes the claim pointing at the *reserved*
    /// `new_exec_id`, but when the fresh-key start's `workflow_id` collides with
    /// a prior run and the reuse policy **returns that existing run** (e.g.
    /// `AllowDuplicate`), `start_or_load_workflow_execution_collect` returns the
    /// existing execution's id rather than `new_exec_id` — so `new_exec_id` is
    /// never inserted and the claim would dangle. Repointing it at the real run
    /// (`resolved_exec_id`) means the next same-key request's join resolves to a
    /// live execution and deduplicates cleanly, instead of hitting the defensive
    /// reclaim path and re-running the whole start every time.
    ///
    /// Runs inside the caller's reserve+start transaction.
    ///
    /// # Errors
    /// Propagates database failures.
    pub async fn repoint_start_idempotency_claim(
        conn: &mut AsyncPgConnection,
        workflow_name: &str,
        key: &str,
        resolved_exec_id: ExecutionId,
    ) -> HarvestResult<()> {
        diesel::sql_query(
            "UPDATE harvest_start_idempotency \
             SET workflow_exec_id = $3 \
             WHERE workflow_name = $1 AND idempotency_key = $2",
        )
        .bind::<diesel::sql_types::Text, _>(workflow_name)
        .bind::<diesel::sql_types::Text, _>(key)
        .bind::<diesel::sql_types::Uuid, _>(resolved_exec_id.as_uuid())
        .execute(conn)
        .await
        .map_err(database_error)?;
        Ok(())
    }

    /// Delete idempotency claims older than the retention window on one shard.
    ///
    /// Bounded to `batch_limit` rows per call via a `ctid` subselect so a large
    /// backlog is drained across several sweep ticks rather than in one long
    /// statement. Returns the number of rows deleted.
    ///
    /// # Errors
    /// Propagates database failures.
    pub async fn purge_expired_start_idempotency(
        conn: &mut AsyncPgConnection,
        shard_id: i32,
        window_secs: f64,
        batch_limit: i64,
    ) -> HarvestResult<usize> {
        let deleted = diesel::sql_query(
            "DELETE FROM harvest_start_idempotency \
             WHERE ctid IN ( \
                 SELECT ctid FROM harvest_start_idempotency \
                 WHERE shard_id = $1 AND created_at <= now() - make_interval(secs => $2) \
                 LIMIT $3 \
             )",
        )
        .bind::<diesel::sql_types::Integer, _>(shard_id)
        .bind::<diesel::sql_types::Double, _>(window_secs)
        .bind::<diesel::sql_types::BigInt, _>(batch_limit)
        .execute(conn)
        .await
        .map_err(database_error)?;
        Ok(deleted)
    }

    /// Sweep expired idempotency claims across all assigned shards.
    ///
    /// Folded into `timeout::enforce_timeouts_once`, mirroring the throttle /
    /// debounce sweeps' shard-fan-out shape. Tolerates a missing table (a
    /// deployment mid-migration) via a `to_regclass` probe rather than erroring
    /// the whole timeout tick.
    ///
    /// # Errors
    /// Propagates database failures other than a missing table.
    pub async fn sweep_expired_start_idempotency(
        conn: &mut AsyncPgConnection,
        sharded_pool: &Option<crate::shard::ShardedDbPool>,
        shard_assignments: &[crate::types::ShardId],
    ) -> HarvestResult<usize> {
        async fn purge_on_conn(
            conn: &mut AsyncPgConnection,
            shard_id: i32,
            window_secs: f64,
        ) -> HarvestResult<usize> {
            // Tolerate the table not existing yet (mid-migration deploy).
            #[derive(diesel::QueryableByName)]
            struct Present {
                #[diesel(sql_type = diesel::sql_types::Bool)]
                present: bool,
            }
            let probe: Present = diesel::sql_query(
                "SELECT to_regclass('harvest_start_idempotency') IS NOT NULL AS present",
            )
            .get_result(conn)
            .await
            .map_err(database_error)?;
            if !probe.present {
                return Ok(0);
            }
            purge_expired_start_idempotency(
                conn,
                shard_id,
                window_secs,
                super::START_IDEMPOTENCY_PURGE_BATCH,
            )
            .await
        }

        // Sub-second precise (issue #808, Codex P2): use the same `f64`-seconds
        // window the reserve path binds, so the sweep never deletes a claim the
        // reserve still considers live.
        let window_secs = super::purge_window_secs();
        let mut total = 0usize;
        match sharded_pool {
            Some(sp) if !shard_assignments.is_empty() => {
                for shard in shard_assignments {
                    let Some(pool) = sp.exact_pool_for(*shard).cloned() else {
                        continue;
                    };
                    let mut shard_conn = match pool.get().await {
                        Ok(c) => c,
                        Err(e) => {
                            tracing::error!(
                                "[start_idempotency] failed to get connection to shard {shard:?}: {e:?}"
                            );
                            continue;
                        }
                    };
                    total += purge_on_conn(&mut shard_conn, shard.as_i32(), window_secs).await?;
                }
            }
            _ => {
                total += purge_on_conn(conn, 0, window_secs).await?;
            }
        }
        Ok(total)
    }
}

#[cfg(feature = "db")]
pub use db_impl::{
    lookup_live_start_idempotency_claim, purge_expired_start_idempotency,
    repoint_start_idempotency_claim, reserve_start_idempotency, sweep_expired_start_idempotency,
};
