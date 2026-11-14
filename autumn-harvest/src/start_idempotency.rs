//! Request-scoped idempotency keys for plain workflow starts (issue #808).
//!
//! # Overview
//!
//! `POST /workflows/{name}/start` accepts an optional `idempotency_key`. Two
//! starts with the *same* key converge on exactly **one** execution. The key
//! holds for a retention window, 24h by default. The first start creates the
//! execution. The second is a no-op that returns the *same* `execution_id` with
//! success: no `409`, no second `WorkflowStarted` event, no second enqueued task.
//!
//! # Delivery identity vs business identity
//!
//! `idempotency_key` is a **delivery-identity** dedupe (this exact request).
//! Its scope is `(workflow_name, idempotency_key)`, **independent of
//! `workflow_id`**. So it converges even when each request auto-generates its
//! `workflow_id`. It sits *in front of* the **business-identity** reuse-policy
//! matrix keyed on `workflow_id`: the idempotency check short-circuits that
//! matrix entirely. When a start is *not* deduplicated (fresh key), the normal
//! reuse-policy semantics apply unchanged.
//!
//! Because the dedupe is `workflow_id`-independent, a keyed start must **route
//! by the key**, not by `workflow_id`. The HTTP handler picks the shard via
//! [`crate::shard::ShardRouter::pick_for_idempotency_key`], a rendezvous over
//! `(workflow_name, idempotency_key)`. So two same-key retries deterministically
//! co-locate on one shard and hit the same claim row, even with auto-generated
//! `workflow_id` values. The claim row, its execution, and the per-shard expiry
//! sweep all share that key-derived shard.
//!
//! # Concurrency
//!
//! Safety comes from the `(workflow_name, idempotency_key)` primary key and a
//! single `INSERT ... ON CONFLICT DO UPDATE` upsert. N simultaneous same-key
//! starts race to insert one row. The winner reserves. Every loser observes the
//! winner's row and returns its `execution_id`. The reserve and the start share
//! one transaction. So a start that fails *after* reserving rolls the claim back,
//! and a retry can start fresh.
//!
//! # Retention & reuse
//!
//! The upsert's `ON CONFLICT ... DO UPDATE ... WHERE created_at <= now() -
//! window` clause makes a key reusable once the window elapses. A stale row is
//! overwritten in place and the start proceeds as fresh. A background sweep
//! ([`purge_expired_start_idempotency`], folded into
//! `timeout::enforce_timeouts_once`) deletes expired rows so the table cannot
//! grow unbounded for keys that are never reused. Even with no sweep,
//! correctness holds — an expired row is overwritten on the next same-key start.
//!
//! # Scoping
//!
//! Shard-local. The claim row and its execution live on the same shard (no
//! cross-shard coordination), consistent with per-key concurrency (#247) and the
//! start throttle (#607). There is deliberately **no** foreign key to
//! `harvest_workflow_executions`. A claim that points at a retention-deleted
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
pub const DEFAULT_START_IDEMPOTENCY_WINDOW: Duration = Duration::from_hours(24);

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
/// `(workflow_name, idempotency_key)` on `harvest_start_idempotency`. An
/// oversized key would exceed the Postgres btree index tuple limit (~2704
/// bytes) and fail at INSERT. That leaks a raw database error on
/// client-controlled input. The conservative cap keeps `workflow_name` + key
/// well under that limit.
pub const MAX_START_IDEMPOTENCY_KEY_LEN: usize = 512;

/// Key prefix **reserved for engine-derived keys**, refused on every
/// caller-facing path (issue #944 review).
///
/// # Why a reservation, and not just a convention
///
/// A broker event-source connector cannot trust a producer to send an
/// idempotency key, so it *derives* one from stable broker coordinates
/// (`autumn-harvest-plugin::connector::idempotency`). Those derived keys share
/// a durable uniqueness scope with caller-supplied ones — `(workflow_name,
/// idempotency_key)` on `harvest_start_idempotency`, and `(workflow_exec_id,
/// idempotency_key)` on `harvest_signals`.
///
/// The prefix alone does not separate them. Derived keys are *predictable*:
/// Kafka coordinates are `{topic}:{partition}:{offset}`, so anyone who knows a
/// topic name can enumerate them. A caller who lands the derived key first
/// claims that row. The broker's own delivery then reads as an idempotent
/// replay. It is acknowledged **without ever dispatching its payload**. That
/// silent loss is precisely the failure the derived key exists to prevent.
///
/// Refusing the prefix at the caller boundary is what actually makes the two
/// namespaces disjoint, so an engine-derived key can never be squatted.
///
/// # Scope
///
/// Applied to every caller-facing key that reaches one of those two scopes.
/// These are the plain start route (header and body), the in-process
/// transactional-start client, signal-with-start, and the standalone signal
/// route. Deliberately **not** applied to `update-with-start` or batch
/// operations. Their keys live in different uniqueness scopes that no connector
/// key can reach. Refusing there would reject a caller's key for no protective
/// gain. A connector target that ever dispatches into one of those scopes must
/// extend this guard.
///
/// # Case sensitivity
///
/// The comparison is case-**sensitive**, matching the Postgres text comparison
/// that backs both uniqueness scopes. `CONN:` is a genuinely different key that
/// cannot alias a derived one, so rejecting it would be over-broad.
pub const RESERVED_KEY_PREFIX: &str = "conn:";

/// Whether `key` is in the engine-reserved namespace and must be refused from
/// a caller. See [`RESERVED_KEY_PREFIX`].
#[must_use]
pub fn is_reserved_key(key: &str) -> bool {
    key.trim().starts_with(RESERVED_KEY_PREFIX)
}

/// Shared trim + empty-rejection + length-cap validation for a request-scoped
/// `idempotency_key`.
///
/// Every start path shares this one rule instead of reimplementing it. Issue
/// #763's review found that the transactional-start client omitted it. An
/// unvalidated empty or whitespace key would become a real
/// `(workflow_name, "")` claim. That claim silently dedupes every *other*
/// unrelated start for that workflow against each other.
///
/// `Ok(None)` means no key was supplied. `Ok(Some(trimmed))` is a valid,
/// trimmed key. `Err(_)` means the key is present but empty or
/// whitespace-only, exceeds [`MAX_START_IDEMPOTENCY_KEY_LEN`] bytes after
/// trimming, or is in the engine-reserved [`RESERVED_KEY_PREFIX`] namespace.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Config`] naming the specific
/// violation (empty vs. too long vs. reserved).
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
            } else if is_reserved_key(trimmed) {
                Err(crate::error::HarvestError::Config(
                    reserved_key_message().to_string(),
                ))
            } else {
                Ok(Some(trimmed.to_string()))
            }
        }
    }
}

/// The one rejection message for a reserved key on every caller-facing path.
/// The plugin's HTTP routes and this validator then never describe the same
/// refusal differently.
#[must_use]
pub const fn reserved_key_message() -> &'static str {
    "idempotency_key must not start with the reserved prefix 'conn:' \
     (it is reserved for engine-derived keys)"
}

/// The default retention window expressed as `f64` seconds, for the const
/// initializer of [`PURGE_WINDOW_SECS_BITS`] (`Duration::as_secs_f64` is not
/// `const`).
const DEFAULT_WINDOW_SECS_F64: f64 = 24.0 * 60.0 * 60.0;

/// Process-global expiry-sweep retention window in **seconds**.
///
/// It holds the bit pattern of an `f64` ([`f64::to_bits`]). So it carries the
/// **same sub-second precision as the reserve path's `Duration::as_secs_f64()`**
/// (issue #808).
///
/// The reserve `WHERE created_at <= now() - make_interval(secs => $window)`
/// guard treats a key as live for exactly `window` seconds. Suppose the sweep
/// used a *truncated* whole-second window (`Duration::as_secs()`) with a
/// 1500 ms config. The sweep would DELETE a claim after 1.0 s, while the
/// reserve still considers it live until 1.5 s. A same-key retry at 1.2 s would
/// then find no claim and start a **second** execution inside the configured
/// window. The `f64` seconds window guarantees that the sweep window *equals*
/// the reserve window. So the sweep never deletes a claim the reserve still
/// considers live.
///
/// [`set_purge_window_secs`] sets it once at `Plugin::build` time. It reads the
/// same `HarvestBuilder` config as the HTTP reserve path. This mirrors the
/// `GLOBAL_CALLBACK_CONFIG` pattern (issue #605). The sweep then needs no new
/// parameter threaded through `enforce_timeouts_once`, `spawn_timeout_checker`
/// and their many call sites. Defaults to [`DEFAULT_START_IDEMPOTENCY_WINDOW`].
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
/// It is a `const fn` so a no-DB shape test can assert the exact SQL string.
/// This mirrors the `*_stmt!` shape-test precedent for the backfill
/// reservation (issue #688). An edit to the guard, the `SET` list, or the
/// `RETURNING` clause then breaks that test instead of drifting silently.
///
/// Binds:
/// - `$1` `workflow_name`, `$2` `idempotency_key`, `$3` new `workflow_exec_id`,
///   `$4` `shard_id`, `$5` retention window in seconds.
///
/// On a conflicting row *older than* the window, it overwrites the claim in
/// place (key reuse) and `RETURNING`s the new id. On a conflicting row *within*
/// the window, the `WHERE` guard is false and no row is updated. The caller
/// then resolves the existing claim via [`reserve_start_idempotency`]'s
/// duplicate branch. The window is bound as a **parameter** (not
/// string-interpolated).
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
/// [`PURGE_WINDOW_SECS_BITS`] static. They then do not race each other in the
/// shared lib-test binary (issue #808). Any test that calls
/// `set_purge_window_secs` and then asserts on `purge_window_secs()` — here or in
/// `builder.rs` — must hold this lock for the set→read span.
#[cfg(test)]
pub(crate) static PURGE_WINDOW_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
mod pure_tests {
    use super::*;

    /// A caller must not be able to squat an engine-derived key.
    ///
    /// The connector derives predictable keys from broker coordinates. It
    /// relies on them being unclaimable from outside. If a caller lands one
    /// first, the broker's delivery reads as a replay and is acked without
    /// dispatching.
    #[test]
    fn a_reserved_prefix_key_is_refused() {
        let derived = "conn:L6:orders:L0::L10:orders:0:7";
        assert!(is_reserved_key(derived));
        let err = validate_start_idempotency_key(Some(derived))
            .expect_err("a caller must not be able to claim an engine-derived key");
        assert!(
            err.to_string().contains("reserved"),
            "the rejection must name the reservation: {err}"
        );
        // Surrounding whitespace must not smuggle it past: the validator
        // trims, so the *trimmed* value is what lands in the durable scope.
        assert!(is_reserved_key("  conn:L6:orders:L0::L10:orders:0:7  "));
        assert!(validate_start_idempotency_key(Some("  conn:anything  ")).is_err());
    }

    /// Case-sensitive on purpose: `CONN:` is a different Postgres text value
    /// and cannot alias a derived key, so refusing it would be over-broad.
    #[test]
    fn a_near_miss_of_the_reserved_prefix_is_still_accepted() {
        for ok in [
            "CONN:x",          // different case -> different durable key
            "conn",            // no separator -> never a derived key
            "connector-thing", // merely starts with the letters
            "xconn:y",
        ] {
            assert!(!is_reserved_key(ok), "{ok} must not read as reserved");
            assert_eq!(
                validate_start_idempotency_key(Some(ok)).expect("must be accepted"),
                Some(ok.to_string()),
            );
        }
    }

    #[test]
    fn reserve_stmt_is_a_conflict_guarded_upsert_returning_exec_id() {
        let sql = reserve_start_idempotency_stmt();
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
        // Sub-second precision (issue #808): the sweep window must be >= the
        // reserve window (`Duration::as_secs_f64()`). Then the sweep never
        // deletes a claim the reserve still considers live. A truncating u64
        // store would make the sweep window 1.0s for a 1500ms config. The
        // reserve treats the key as live until 1.5s, so a sub-second dedup gap
        // opens.
        let window = Duration::from_millis(1500);
        set_purge_window_secs(window);
        let sweep_window = purge_window_secs();
        assert!(
            sweep_window >= window.as_secs_f64(),
            "sweep window {sweep_window} must not be shorter than reserve window {}",
            window.as_secs_f64()
        );
        assert!((sweep_window - 1.5).abs() < 1e-9);

        set_purge_window_secs(Duration::from_secs(7200));
        assert!((purge_window_secs() - 7200.0).abs() < 1e-9);

        set_purge_window_secs(Duration::ZERO);
        assert!(
            (purge_window_secs() - DEFAULT_START_IDEMPOTENCY_WINDOW.as_secs_f64()).abs() < 1e-9
        );

        // Restore default so ordering-independent test runs do not leak state.
        set_purge_window_secs(DEFAULT_START_IDEMPOTENCY_WINDOW);
    }
}

#[cfg(feature = "db")]
mod db_impl {
    use super::{StartIdempotencyReservation, reserve_start_idempotency_stmt};
    use crate::cached_stmt::CachePreparedDsl as _;
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
    /// Returns [`StartIdempotencyReservation::Reserved`] when this request must
    /// perform the start with `new_exec_id`. Returns
    /// [`StartIdempotencyReservation::Duplicate`] when an earlier start already
    /// claimed the key and its execution still exists. The caller returns that
    /// run as a no-op.
    ///
    /// Concurrency-safe via the composite-PK upsert. Robust to both Postgres
    /// interpretations of a `WHERE`-false `DO UPDATE`: `RETURNING` may yield the
    /// pre-existing row or nothing. Also robust to a dangling claim whose
    /// execution was deleted by retention.
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
                // execution that no longer exists. That dangling pointer is only
                // reachable if the idempotency window exceeds execution retention
                // and the sweep has not run yet. Delete the stale claim ONLY if it
                // still points at a non-existent execution. The `NOT EXISTS` guard
                // makes this safe against a concurrent same-key request. That
                // request may repoint the claim to a real new run between the
                // classifying JOIN above and this delete. Its valid claim is
                // preserved, so this never deletes a live claim and never produces
                // a duplicate execution. Binds `workflow_name`/`key` as parameters.
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

                // Re-classify ONCE after the guarded delete. The guarded delete may
                // have removed 0 rows, because a concurrent request already
                // repointed the claim to a valid run. So do NOT assume Reserved.
                // Re-run the classify and honour whatever now owns the key.
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
    /// Returns the claimed execution's id when three things hold. A claim row
    /// for `(workflow_name, key)` exists, it was created within `window_secs`,
    /// **and** it still points at a live execution. The JOIN mirrors the
    /// `Duplicate` classification of [`reserve_start_idempotency`]. A
    /// within-window claim whose execution was retention-deleted is *not* a
    /// dedup hit. It reclaims to a fresh start.
    ///
    /// The admission-gate idempotent-retry bypass uses it (issue #808). A keyed
    /// replay of an already-successful start resolves to a `200` no-op. So it is
    /// **not** fresh admission, and a raised gate must not reject it.
    /// Side-effect free (no upsert, no reclaim). A genuinely fresh keyed start
    /// finds no claim and still passes through the gate normally.
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
    /// `new_exec_id`. A fresh-key start's `workflow_id` can collide with a prior
    /// run, and the reuse policy can **return that existing run** (e.g.
    /// `AllowDuplicate`). Then `start_or_load_workflow_execution_collect`
    /// returns the existing execution's id, not `new_exec_id`. So `new_exec_id`
    /// is never inserted and the claim would dangle. Repointing it at the real
    /// run (`resolved_exec_id`) lets the next same-key request's join resolve to
    /// a live execution and deduplicate cleanly. Otherwise each such request hits
    /// the defensive reclaim path and re-runs the whole start.
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
    /// A `ctid` subselect bounds each call to `batch_limit` rows. A large
    /// backlog then drains across several sweep ticks, not in one long
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
        .cache_prepared()
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
        sweep_expired_start_idempotency_on_conn_shard(
            conn,
            None,
            sharded_pool.as_ref(),
            shard_assignments,
        )
        .await
    }

    /// [`sweep_expired_start_idempotency`] for a caller that knows `conn`'s
    /// shard. See [`crate::shard::connect_or_reuse`].
    pub async fn sweep_expired_start_idempotency_on_conn_shard(
        conn: &mut AsyncPgConnection,
        conn_shard: Option<crate::types::ShardId>,
        sharded_pool: Option<&crate::shard::ShardedDbPool>,
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
            .cache_prepared()
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

        // Sub-second precise (issue #808): use the same `f64`-seconds window
        // that the reserve path binds. The sweep then never deletes a claim the
        // reserve still considers live.
        let window_secs = super::purge_window_secs();
        let mut total = 0usize;
        match sharded_pool {
            Some(sp) if !shard_assignments.is_empty() => {
                for shard in shard_assignments {
                    let Some(mut shard_conn) = crate::shard::connect_or_reuse(
                        conn,
                        conn_shard,
                        sp,
                        *shard,
                        "start_idempotency",
                        crate::shard::ShardConnectError::LogAndSkip,
                    )
                    .await?
                    else {
                        continue;
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
pub(crate) use db_impl::sweep_expired_start_idempotency_on_conn_shard;
#[cfg(feature = "db")]
pub use db_impl::{
    lookup_live_start_idempotency_claim, purge_expired_start_idempotency,
    repoint_start_idempotency_claim, reserve_start_idempotency, sweep_expired_start_idempotency,
};
