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
/// Taking a queue-scoped lock inside the claim path would close the window but
/// serialize every claim for a queue against every other, defeating
/// `FOR UPDATE SKIP LOCKED`'s parallel claiming for a feature that is inactive
/// almost all of the time. Re-checking in a **fresh statement** gets a fresh
/// snapshot for the cost of one indexed primary-key probe per *successful*
/// claim (never per empty poll), and yields a crisp contract: **a pause
/// committed before this re-check's statement begins always wins.** A pause
/// that commits after it did not beat the claim, and AC2 explicitly allows an
/// already-`RUNNING` task to finish naturally.
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

/// SQL for [`release_claim_if_queue_paused`], exposed for shape tests.
///
/// One statement, so it takes a fresh `READ COMMITTED` snapshot and therefore
/// sees any pause committed before it began — which is the entire point (see
/// [`release_claim_if_queue_paused`]).
#[must_use]
pub const fn release_claim_if_queue_paused_query() -> &'static str {
    "UPDATE harvest_task_queue \
     SET state = 'PENDING', \
         worker_id = NULL, \
         started_at = NULL, \
         attempt = GREATEST(attempt - 1, 0) \
     WHERE id = $1 \
       AND state = 'RUNNING' \
       AND worker_id = $2 \
       AND EXISTS (SELECT 1 FROM harvest_queue_pauses qp \
                   WHERE qp.queue_name = harvest_task_queue.queue_name)"
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
/// # Errors
///
/// Database errors.
#[cfg(feature = "db")]
pub async fn lock_queue_for_pause(
    conn: &mut AsyncPgConnection,
    queue_name: &str,
) -> HarvestResult<()> {
    use diesel_async::RunQueryDsl;

    diesel::sql_query("SELECT pg_advisory_xact_lock(hashtext($1))")
        .bind::<diesel::sql_types::Text, _>(queue_name)
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
/// (`scheduled_at <= NOW()`), it credits back only the time the row was
/// genuinely held:
///
/// ```text
/// scheduled_at += NOW() - GREATEST(scheduled_at, paused_at)
/// ```
///
/// - **Eligible before the pause** (`scheduled_at <= paused_at`): shifted by the
///   full pause span, so pre-pause waiting is preserved (a task that had already
///   waited 100 s still shows 100 s of wait after the thaw).
/// - **Became eligible mid-pause** (`paused_at < scheduled_at <= NOW()`):
///   collapses to `NOW()` — it was held from the instant it became due, so it
///   accrues no wait.
/// - **Not yet due** (`scheduled_at > NOW()`, e.g. a retry backoff): excluded
///   entirely. It was never held, so its backoff must not be extended.
///
/// The result is provably `<= NOW()` in every branch, so a thawed task is never
/// pushed into the future (which would make it *un*claimable — the inverse of
/// AC5). `RUNNING` rows are untouched, and `schedule_to_close_at` is untouched
/// so the absolute deadline keeps ticking.
///
/// Binds: `$1` = queue name, `$2` = `paused_at`.
#[must_use]
pub const fn resume_shift_scheduled_at_query() -> &'static str {
    "UPDATE harvest_task_queue \
     SET scheduled_at = scheduled_at + (NOW() - GREATEST(scheduled_at, $2)) \
     WHERE queue_name = $1 \
       AND state = 'PENDING' \
       AND scheduled_at <= NOW()"
}

/// Count of `PENDING` tasks currently held on a queue (`$1` = queue name).
#[must_use]
pub const fn held_task_count_query() -> &'static str {
    "SELECT COUNT(*) AS count FROM harvest_task_queue \
     WHERE queue_name = $1 AND state = 'PENDING'"
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
    use diesel_async::{AsyncConnection, RunQueryDsl};

    let queue_owned = validate_queue_name(queue_name)?;
    if reason.trim().is_empty() {
        return Err(HarvestError::Config(
            "a queue pause requires a non-empty reason".to_string(),
        ));
    }

    let reason_owned = reason.to_string();
    let actor_owned = actor.to_string();

    Box::pin(
        conn.transaction::<PauseOutcome, HarvestError, _>(async |conn| {
            // Serialize against the schedule_to_start enforcer so it cannot
            // fail a task on the queue we are pausing (see
            // `lock_queue_for_pause`).
            lock_queue_for_pause(conn, &queue_owned).await?;

            // A single upsert, not INSERT ... DO NOTHING + a fallback SELECT.
            // DO NOTHING does not lock the conflicting row, so a concurrent
            // resume could delete it between the two statements, leaving the
            // fallback SELECT with zero rows — a 500 and, worse, a queue left
            // *unpaused* for an operator who was told the pause failed.
            //
            // The no-op `SET queue_name = ...` preserves the ORIGINAL
            // provenance on a re-pause while still taking the row lock, and
            // `xmax = 0` is the standard discriminator for "this tuple was
            // inserted, not updated".
            let row: UpsertedPauseRow = diesel::sql_query(
                "INSERT INTO harvest_queue_pauses \
                         (queue_name, reason, paused_by, scope_shard_id) \
                     VALUES ($1, $2, $3, $4) \
                     ON CONFLICT (queue_name) DO UPDATE \
                         SET queue_name = harvest_queue_pauses.queue_name \
                     RETURNING queue_name, reason, paused_by, paused_at, scope_shard_id, \
                               (xmax = 0) AS newly_paused",
            )
            .bind::<diesel::sql_types::Text, _>(&queue_owned)
            .bind::<diesel::sql_types::Text, _>(&reason_owned)
            .bind::<diesel::sql_types::Text, _>(&actor_owned)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(scope_shard_id)
            .get_result(conn)
            .await?;
            let newly_paused = row.newly_paused;

            let held: CountRow = diesel::sql_query(held_task_count_query())
                .bind::<diesel::sql_types::Text, _>(&queue_owned)
                .get_result(conn)
                .await?;

            Ok(PauseOutcome {
                queue_name: row.queue_name,
                newly_paused,
                reason: row.reason,
                paused_by: row.paused_by,
                paused_at: row.paused_at,
                scope_shard_id: row.scope_shard_id,
                held_task_count: held.count,
            })
        }),
    )
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
    use diesel_async::{AsyncConnection, RunQueryDsl};

    let queue_owned = validate_queue_name(queue_name)?;
    let _ = actor; // recorded by the caller's audit row; the table keeps no resume trail

    Box::pin(
        conn.transaction::<ResumeOutcome, HarvestError, _>(async |conn| {
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

            let released = diesel::sql_query(resume_shift_scheduled_at_query())
                .bind::<diesel::sql_types::Text, _>(&queue_owned)
                .bind::<diesel::sql_types::Timestamptz, _>(row.paused_at)
                .execute(conn)
                .await?;

            Ok(ResumeOutcome {
                queue_name: queue_owned,
                newly_resumed: true,
                released_task_count: i64::try_from(released).unwrap_or(i64::MAX),
                paused_duration_secs: (Utc::now() - row.paused_at).num_seconds().max(0),
                released_reason: Some(row.reason),
                released_paused_by: Some(row.paused_by),
            })
        }),
    )
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

    // One pass: LEFT JOIN the PENDING backlog so a paused queue with no held
    // work still lists (with a zero count) rather than disappearing.
    let rows: Vec<Row> = diesel::sql_query(
        "SELECT p.queue_name, p.reason, p.paused_by, p.paused_at, p.scope_shard_id, \
                COALESCE(t.held, 0) AS held_task_count \
         FROM harvest_queue_pauses p \
         LEFT JOIN ( \
             SELECT queue_name, COUNT(*) AS held \
             FROM harvest_task_queue WHERE state = 'PENDING' GROUP BY queue_name \
         ) t ON t.queue_name = p.queue_name \
         ORDER BY p.paused_at ASC, p.queue_name ASC",
    )
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
        assert!(sql.contains("scheduled_at <= NOW()"));
        // The absolute deadline must NOT be shifted (issue #619 out-of-scope).
        assert!(!sql.contains("schedule_to_close_at"));
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
