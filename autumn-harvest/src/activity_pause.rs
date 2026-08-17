//! Per-activity-type pause/resume — surgical outage containment (issue #807).
//!
//! When **one** downstream dependency has a known incident — a payment vendor
//! returning 500s, an internal service drained for maintenance, a rate-limit
//! ban — the operator already knows which activity is broken. Their job is to
//! stop dispatching *that* activity, fleet-wide, right now, without touching
//! the healthy work that shares its queue.
//!
//! # How it composes with the neighbouring brakes
//!
//! | Primitive | Granularity | Trigger | What it does | Issue |
//! |---|---|---|---|---|
//! | Admission gate | workflow **starts** | manual | halts new starts; in-flight runs keep scheduling activities | #377 / #618 |
//! | Queue pause | a whole **task queue** | manual | holds dispatch for every activity on that queue | #619 |
//! | Circuit breaker | one **activity type** | *automatic*, reactive | **fast-fails** dispatch after failures accumulate | #369 |
//! | **Activity pause (this)** | one **activity type** | *manual*, proactive | **holds** dispatch: nothing fails, retries, or dead-letters | **#807** |
//!
//! The breaker and this feature share a granularity but not a job. The breaker
//! is a reflex — it only trips *after* retryable failures have already burned
//! retry budget, grown histories and fed the DLQ, and it only exists for
//! activities whose author declared a policy at compile time. This is the
//! operator's hand on the same valve: proactive, available for every activity,
//! and holding rather than failing.
//!
//! Queue pause is the same *shape* one level up. It is the wrong tool when many
//! activity types share the `default` queue, because holding the queue to stop
//! one broken call also halts email, inventory and notifications.
//!
//! # Enforcement model — durable, claim-layer, no cache
//!
//! A pause is durable metadata enforced on the claim path, exactly like the
//! queue pause (#619), the PAUSED-execution skip (#383) and the rate-limit gate
//! (#332). It is deliberately **not** an in-process registry like the circuit
//! breaker's:
//!
//! - "Durable, and applied on every shard and every worker" is true **by
//!   construction** — there is no boot window to lose a pause in, no refresh
//!   interval, and no fleet-wide staleness window. A worker that starts during
//!   an incident observes the hold on its very first claim.
//! - There is no global static and no write-through path that can diverge from
//!   the database.
//!
//! # Replay contract
//!
//! A pause writes **nothing** to `harvest_events` and introduces **no**
//! [`crate::event::WorkflowEvent`] variant. A workflow waiting on a paused
//! activity observes a delayed dispatch and nothing else; replay of an
//! execution that touched a paused activity is byte-identical to one that never
//! paused.
//!
//! # Timeout interaction
//!
//! The *relative* `schedule_to_start` timer is suspended for held tasks, and a
//! resume credits held time back to `scheduled_at` so a thaw does not
//! retroactively time out the whole backlog. Without both halves, AC3's
//! "remain `PENDING` (not failed, not DLQ'd)" would be violated by the very
//! scanner that exists to detect starved queues. The *absolute*
//! `schedule_to_close` deadline (#378) keeps ticking — a hold does not extend
//! an SLA — matching the explicit scope decision in #619.

#[cfg(feature = "db")]
use crate::error::HarvestError;
use crate::error::HarvestResult;
#[cfg(feature = "db")]
use chrono::{DateTime, Utc};
#[cfg(feature = "db")]
use diesel_async::AsyncPgConnection;

/// Upper bound on a pausable activity name.
///
/// Activity names are free-form `TEXT` on `harvest_task_queue`, but this
/// table's primary key is the activity name, so an unbounded operator-supplied
/// string would be an unbounded btree key. 255 matches
/// [`crate::queue_pause::MAX_QUEUE_NAME_LEN`].
pub const MAX_ACTIVITY_NAME_LEN: usize = 255;

/// Validate an operator-supplied activity name and return its canonical form.
///
/// Rejects blank (or whitespace-only) names, names carrying surrounding
/// whitespace, and anything longer than [`MAX_ACTIVITY_NAME_LEN`]. Pure — no
/// database access — so the management API can reject a typo with a `400`
/// before touching a connection.
///
/// Surrounding whitespace is **rejected, not trimmed**, for the same reason
/// [`crate::queue_pause::validate_queue_name`] rejects it: every read and write
/// here matches on exact equality, so a name that does not round-trip verbatim
/// would hold a *different* activity than the operator named while the API
/// reported success. Storing the raw name after validating a trimmed one
/// inserts a row matching no task; trimming instead silently retargets a real,
/// different activity. Only rejecting cannot lie, and during an outage a `400`
/// naming the problem beats a false green.
///
/// # Errors
///
/// [`HarvestError::Config`] when the name is blank, carries surrounding
/// whitespace, or is oversized.
pub fn validate_activity_name(activity_name: &str) -> HarvestResult<String> {
    if activity_name.trim().is_empty() {
        return Err(crate::error::HarvestError::Config(
            "activity name must not be empty".to_string(),
        ));
    }
    if activity_name != activity_name.trim() {
        return Err(crate::error::HarvestError::Config(
            "activity name must not have leading or trailing whitespace; \
             an activity name is matched exactly, so a stray space would hold a \
             different activity than the one intended"
                .to_string(),
        ));
    }
    if activity_name.len() > MAX_ACTIVITY_NAME_LEN {
        return Err(crate::error::HarvestError::Config(format!(
            "activity name exceeds {MAX_ACTIVITY_NAME_LEN} bytes"
        )));
    }
    Ok(activity_name.to_string())
}

/// Render the activity-pause anti-join correlated against `outer_table` — the
/// single source of truth for every query that mirrors the claim gate.
///
/// A paused activity that a mirror query forgot about would report a phantom
/// backlog and drive a false capacity alert during a deliberate hold, so all of
/// them are generated from this one function.
///
/// # Why the mirrors use this and the claim path does not
///
/// The anti-join is scoped by **`task_type`**, not by a NULL `activity_name`.
///
/// It is tempting to lean on NULL semantics here — `ap.activity_name = NULL` is
/// never true, so `NOT EXISTS (...)` stays `TRUE` — and conclude that a workflow
/// task can never be held. That reasoning is **wrong**, because a workflow row's
/// `activity_name` is not reliably NULL: the engine stamps the
/// `'mixed_signal_suspension'` sentinel into that very column (see
/// `queue::claim_task`'s wake handling and `queue::requeue_workflow_task_nd_blocked`,
/// which has to clear it). Leaning on NULL would therefore make the guarantee
/// hinge on no registered activity happening to share that sentinel's name —
/// and if one ever did, pausing it would silently hold *every*
/// mixed-signal-suspended workflow task in the fleet.
///
/// The explicit `task_type = 'activity'` predicate makes the same guarantee
/// structural instead, at zero cost: the pause table is empty in steady state,
/// so the subquery is a single index probe either way.
///
/// The hot claim path cannot use this correlated form at all — there it is
/// re-probed once per *candidate row* and was measured at ~99% of the claim
/// query's buffer touches (`docs/performance.md`, issue #619) — so it uses a
/// pre-filtered array with its own explicit
/// `task_type != 'activity' OR activity_name IS NULL` guard. See
/// `queue::claim_task_query` and the test
/// `claim_query_activity_pause_gate_never_holds_workflow_tasks`.
///
/// The outer column reference **must** be qualified: inside the subquery an
/// unqualified `activity_name` resolves against `harvest_activity_pauses`
/// first, degenerating the predicate to `ap.activity_name = ap.activity_name` —
/// always true, which would silently hold *every* activity the moment any one
/// is paused.
#[must_use]
pub fn activity_pause_anti_join(outer_table: &str) -> String {
    format!(
        "NOT EXISTS (SELECT 1 FROM harvest_activity_pauses ap \
         WHERE ap.activity_name = {outer_table}.activity_name \
           AND {outer_table}.task_type = '{ACTIVITY_TASK_TYPE}')"
    )
}

/// The `harvest_task_queue.task_type` value an activity pause may hold.
///
/// Every pause predicate — the SQL renderers here, the pre-filtered claim gate,
/// the plugin's eligibility explainer, and the Rust-side re-check in
/// `timeout::enforce_activity_timeout` — is scoped by this value, for the
/// sentinel reason documented on [`activity_pause_anti_join`]. Naming it once
/// keeps the Rust comparisons and the SQL literals from drifting apart; the
/// test `anti_join_scopes_by_the_named_task_type` pins that they agree.
pub const ACTIVITY_TASK_TYPE: &str = "activity";

/// Whether an activity pause suspends enforcement of `reason` for a held task.
///
/// Only the **relative** `schedule_to_start` timer is suspended: a task must
/// not be terminally failed merely because it waited while an operator held its
/// activity type (AC3 — held tasks "remain `PENDING` (not failed, not
/// DLQ'd)").
///
/// Deliberately *not* suspended:
///
/// - **`ScheduleToClose`** — a hold does not extend an absolute wall-clock SLA
///   deadline (#378). This mirrors the explicit scope decision #619 made for
///   queue pause; crediting held time back to `schedule_to_close` is a
///   follow-up for both.
/// - **`Heartbeat` / `StartToClose`** — both apply only to `RUNNING` rows. A
///   pause holds *dispatch*; already-dispatched work runs to completion on its
///   own merits (AC4), so these stay pause-blind.
#[cfg(feature = "db")]
#[must_use]
pub const fn activity_pause_suppresses_timeout(
    reason: &crate::timeout::TimeoutReason,
    activity_paused: bool,
) -> bool {
    activity_paused && matches!(reason, crate::timeout::TimeoutReason::ScheduleToStart)
}

/// SQL for [`release_claim_if_activity_paused`], exposed for shape tests.
///
/// One statement, so it takes a fresh `READ COMMITTED` snapshot and therefore
/// sees any pause committed before it began — which is the entire point (see
/// [`release_claim_if_activity_paused`]).
///
/// Restores `attempt`: the task never ran, so a hold must not consume retry
/// budget (AC3).
///
/// Carries `task_type = 'activity'` in the statement itself rather than relying
/// on the caller to pre-filter. The caller in `queue::claim_task` does check it,
/// but this function is `pub`: a second caller that omitted the check would gain
/// the ability to release a claimed *workflow* task, because a workflow row can
/// carry a non-NULL `activity_name` (the `'mixed_signal_suspension'` sentinel —
/// see [`activity_pause_anti_join`]). Making the statement safe in isolation
/// costs one conjunct on a primary-key lookup and removes the footgun entirely.
#[must_use]
pub const fn release_claim_if_activity_paused_query() -> &'static str {
    "UPDATE harvest_task_queue \
     SET state = 'PENDING', \
         worker_id = NULL, \
         started_at = NULL, \
         attempt = GREATEST(attempt - 1, 0) \
     WHERE id = $1 \
       AND state = 'RUNNING' \
       AND worker_id = $2 \
       AND task_type = 'activity' \
       AND EXISTS (SELECT 1 FROM harvest_activity_pauses ap \
           WHERE ap.activity_name = harvest_task_queue.activity_name)"
}

/// Releases a just-claimed task back to `PENDING` when its activity turns out
/// to be paused (issue #807).
///
/// # Why a second statement is required
///
/// The claim is a single CTE statement, so under `READ COMMITTED` its whole
/// body — including the `paused_activities` pre-filter — evaluates against
/// **one snapshot taken at statement start**. A pause that commits after that
/// snapshot is invisible to it, so a claim already in flight when the operator
/// paused can still transition its task to `RUNNING` and hand it to a worker
/// that dispatches into the outage. Re-checking in a **fresh statement** gets a
/// fresh snapshot for the cost of one indexed probe per *successful activity
/// claim* (never per empty poll, and never for a workflow task), and yields a
/// crisp contract: **a pause committed before this re-check's statement begins
/// always wins.**
///
/// The release is guarded on `state = 'RUNNING' AND worker_id = $2` so it can
/// only ever undo *this* worker's own claim. The rate-limit token the claim
/// debited is not refunded; that is the same safe-side, self-healing leak
/// already documented for a lost claim in [`crate::queue::claim_task`] (it only
/// ever under-dispatches).
///
/// # Residual window (deliberate — see the module note on advisory locks)
///
/// This re-check's verdict is authoritative as of *its own snapshot*, not
/// through commit. A pause that commits in the window between this statement
/// and the claim transaction's `COMMIT` — one round trip, sub-millisecond — can
/// be acknowledged to the operator while one already-claimed task still
/// dispatches. Queue pause closes that last window with a shared advisory lock
/// on the queue key; this feature deliberately does not, because
/// `queue_pause::queue_lock_keys` spends all 64 bits of the two-argument
/// advisory keyspace on queue identity and reserves no class id, and a
/// CI-enforced source walk
/// (`queue_pause::queue_pause_owns_the_two_argument_advisory_keyspace`) keeps
/// that keyspace single-user for exactly that reason. The alternative
/// single-argument keyspace is already shared, unnamespaced, by five
/// subsystems including the concurrency gate on this same hot claim path, where
/// a collision silently stops dispatch on an unrelated key — the precise
/// failure this feature exists to prevent. Paying that risk to close a
/// sub-millisecond window that can leak at most one already-claimed task per
/// racing worker, when the issue's own bar is "effective within one poll
/// interval", is the wrong trade. The leaked task's *next* attempt is held like
/// any other.
///
/// Returns `true` when the claim was released (the caller must behave as if no
/// task was claimed).
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
#[cfg(feature = "db")]
pub async fn release_claim_if_activity_paused(
    conn: &mut AsyncPgConnection,
    task_id: uuid::Uuid,
    worker_id: &str,
) -> HarvestResult<bool> {
    use diesel_async::RunQueryDsl;

    let released = diesel::sql_query(release_claim_if_activity_paused_query())
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .bind::<diesel::sql_types::Text, _>(worker_id)
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(released > 0)
}

/// The resume-time `scheduled_at` credit.
///
/// Without it, thawing an activity whose backlog waited past its
/// `schedule_to_start` would immediately time out the *entire* held backlog:
/// exactly the AC3 failure ("not failed, not DLQ'd") this feature exists to
/// prevent.
///
/// For each due `PENDING` task of the activity it credits back only the time
/// the row was genuinely held, using the same two-sided formula queue pause
/// arrived at over several review rounds (issue #619):
///
/// - **Eligible before the pause** (`created_at < paused_at`, or a pre-
///   `created_at` row): `scheduled_at += clock_timestamp() - GREATEST(scheduled_at, paused_at)`,
///   so pre-pause waiting is preserved — a task that had already waited 100 s
///   still shows 100 s of wait after the thaw, and two such tasks keep their
///   relative order.
/// - **Created during the hold**: collapses to the thaw instant. It was held
///   from the moment it became due, so it accrues no wait.
/// - **Not yet due** (`scheduled_at > now`, e.g. a retry backoff): excluded
///   entirely. It was never held, so its backoff must not be extended.
///
/// `created_at` is nullable (a pre-`20260619000000` row has none), and in SQL a
/// comparison against `NULL` is `NULL`, not `false` — hence the explicit
/// `IS NULL` arm, which puts such a row on the pre-pause side where it belongs
/// (it predates that migration and therefore predates any pause).
///
/// # Why `clock_timestamp()` and not `NOW()`
///
/// `NOW()` is `transaction_timestamp()`, frozen at **transaction start** —
/// before the pause-row `DELETE` and before this `UPDATE` runs. The hold stays
/// in force until this transaction **commits**, so a frozen `NOW()` silently
/// omits the resume's own duration from the credit, and any task whose
/// `schedule_to_start` is shorter than the resume's runtime times out
/// immediately after the thaw. `clock_timestamp()` is volatile and re-read per
/// row at execution time.
///
/// It is still a **lower bound**, deliberately: the true thaw instant is commit
/// time, which no in-statement expression can know, so each row is
/// under-credited by at most the remaining duration of this `UPDATE`. That is
/// the *safe* direction — over-crediting past commit time would push
/// `scheduled_at` into the future and make the task **un**claimable.
///
/// # Why one statement, where queue pause needs two
///
/// Queue pause splits this into a primary pass plus a late-arrivals pass
/// because its primary pass predates the discovery of rows enqueued during the
/// resume, and it must then exclude the rows it already shifted by id. A single
/// statement carrying the `CASE` has no such ordering problem: every due
/// `PENDING` row visible to this one snapshot is credited exactly once, by
/// whichever arm of the `CASE` its `created_at` selects. The irreducible
/// residual is the same one queue pause documents — a row that commits *after*
/// this statement's snapshot and before this transaction commits is still held
/// and gets no credit, under-credited by at most this statement's remaining
/// duration. Closing it completely would require serializing **enqueue** behind
/// a pause-scoped lock, putting a serialization point on the hot enqueue path
/// for every task, paused or not.
///
/// `RUNNING` rows are untouched (AC4) and `schedule_to_close_at` is untouched
/// so the absolute deadline keeps ticking.
///
/// Binds: `$1` = activity name, `$2` = the hold's `paused_at`.
#[must_use]
pub const fn resume_shift_scheduled_at_query() -> &'static str {
    "UPDATE harvest_task_queue \
     SET scheduled_at = CASE \
           WHEN created_at IS NULL OR created_at < $2 \
             THEN scheduled_at + (clock_timestamp() - GREATEST(scheduled_at, $2)) \
           ELSE clock_timestamp() \
         END \
     WHERE task_type = 'activity' \
       AND activity_name = $1 \
       AND state = 'PENDING' \
       AND scheduled_at <= clock_timestamp() \
     RETURNING id"
}

/// Second resume pass: credit every due `PENDING` task of `$1` that the primary
/// pass did **not** shift, excluded by `$3` (the ids it reported) rather than by
/// predicate.
///
/// # Why a second pass exists at all (issue #807 review, Codex P1)
///
/// Under READ COMMITTED each statement takes its own snapshot, so a task
/// enqueued *after* the primary shift began — but before this transaction's
/// pause `DELETE` commits — is invisible to that pass while still being
/// genuinely held (every other session still sees the uncommitted pause row and
/// still skips it). With a single pass such a row thaws carrying its whole held
/// wait, and the `schedule_to_start` scanner can terminally fail it the instant
/// the hold lifts — the exact failure the credit exists to prevent. The window
/// is widest exactly where it hurts: a large backlog keeps the bulk `UPDATE`
/// and the per-queue notification pass running longest.
///
/// # Why exclude by id rather than by predicate
///
/// The primary pass already handles *both* sides of the `created_at` partition
/// inline, so a predicate-based exclusion here would have to re-derive which
/// rows it saw — and "which rows a statement saw" is a question about
/// visibility, not about the row. Excluding the ids it actually reported makes
/// coverage total for **any** commit order: a row it shifted is skipped here (a
/// second application of the relative `+=` formula would erase the very
/// pre-pause wait it preserves), and every other due `PENDING` row is credited
/// here. An empty `$3` correctly leaves every row to this pass.
///
/// Spelled `NOT EXISTS (… unnest …)` rather than `id <> ALL(<array>)`
/// deliberately: the latter is a per-row linear scan of the array, the former
/// lets the planner build a hash anti-join, so cost is O(rows + ids) rather
/// than O(rows × ids). Mirrors [`crate::queue_pause::resume_shift_late_arrivals_query`].
///
/// # Irreducible residual
///
/// This narrows the uncredited window from "the whole bulk `UPDATE` plus the
/// notification pass" to "this final, small statement", but cannot close it: a
/// row committed after *this* statement's snapshot and before this transaction
/// commits is still held and still gets no credit. The under-credit is bounded
/// by the remaining duration of this statement (microseconds for an activity
/// with no late arrivals) and errs in the same safe direction as the primary
/// pass. Closing it completely would require serializing **enqueue** behind an
/// activity-scoped lock, putting a serialization point on the hot enqueue path
/// for every task, paused or not — the exact trade this module's design notes
/// reject for the claim path.
///
/// Binds: `$1` = activity name, `$2` = the hold's `paused_at`, `$3` = the ids
/// the primary pass reported shifting.
#[must_use]
pub const fn resume_shift_late_arrivals_query() -> &'static str {
    "UPDATE harvest_task_queue \
     SET scheduled_at = CASE \
           WHEN created_at IS NULL OR created_at < $2 \
             THEN scheduled_at + (clock_timestamp() - GREATEST(scheduled_at, $2)) \
           ELSE clock_timestamp() \
         END \
     WHERE task_type = 'activity' \
       AND activity_name = $1 \
       AND state = 'PENDING' \
       AND scheduled_at <= clock_timestamp() \
       AND NOT EXISTS ( \
             SELECT 1 FROM unnest($3::uuid[]) AS already(id) \
             WHERE already.id = harvest_task_queue.id \
           )"
}

/// SQL for [`is_activity_paused`], exposed for shape tests.
///
/// One statement, so it takes a fresh `READ COMMITTED` snapshot and therefore
/// sees any pause committed before it began — which is the entire point.
#[must_use]
pub const fn is_activity_paused_query() -> &'static str {
    "SELECT EXISTS ( \
         SELECT 1 FROM harvest_activity_pauses WHERE activity_name = $1 \
     ) AS paused"
}

/// Is this activity type currently held on **this shard's** database?
///
/// The authoritative re-check for [`crate::timeout::enforce_timeouts_once`]'s
/// `ScheduleToStart` path. The scan predicate that selected the task is a
/// non-locking snapshot, so a pause committing after the scan is invisible to
/// it and the enforcer would terminally fail the very task the hold exists to
/// protect — an `ActivityTimedOut` the operator's pause was one moment too late
/// to prevent. Running this inside the enforcement transaction closes all but a
/// bounded residual, described below.
///
/// # Residual window (deliberate)
///
/// This is authoritative as of **its own snapshot**, not through commit: a
/// pause that commits between this read and the enforcement transaction's
/// `COMMIT` is not observed, so one already-scanned task can still be failed.
/// The queue-pause sibling closes that window with a shared advisory lock, and
/// this path deliberately does not, because:
///
/// 1. The lock would need a **new** two-argument advisory keyspace.
///    `queue_pause` owns that keyspace by a CI-enforced guard, and the
///    single-argument space is shared by five subsystems where a silent
///    collision stalls the hot claim path.
/// 2. It would impose a fleet-wide queue-before-activity lock-ordering rule on
///    two paths that today take no lock at all, creating an ABBA hazard for the
///    first feature that touches both.
/// 3. The exposure is narrow by construction: only activities that opt into
///    `schedule_to_start` at all, only rows *already past* that deadline at the
///    instant of the pause — which were going to be failed by the very next
///    scanner tick regardless — and only inside the window above.
///
/// The complementary race (a *resume* crediting `scheduled_at` forward while
/// this enforcer holds the row) needs no lock at all: `resume_activity` does
/// the pause delete and the credit in **one** transaction, so a statement here
/// sees either (held, uncredited) or (released, credited) and never a torn view,
/// and `schedule_to_start_still_expired_unlocked` re-reads the credited deadline
/// before the enforcer trusts its scan snapshot.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
#[cfg(feature = "db")]
pub async fn is_activity_paused(
    conn: &mut AsyncPgConnection,
    activity_name: &str,
) -> HarvestResult<bool> {
    use diesel_async::RunQueryDsl;

    #[derive(diesel::QueryableByName)]
    struct PausedRow {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        paused: bool,
    }

    let row: PausedRow = diesel::sql_query(is_activity_paused_query())
        .bind::<diesel::sql_types::Text, _>(activity_name)
        .get_result(conn)
        .await
        .map_err(crate::error::database_error)?;

    Ok(row.paused)
}

/// Count of `PENDING` tasks currently held for an activity (`$1` = name).
#[must_use]
pub const fn held_task_count_query() -> &'static str {
    "SELECT COUNT(*) AS count FROM harvest_task_queue \
     WHERE task_type = 'activity' AND activity_name = $1 AND state = 'PENDING'"
}

/// The `GET /activities` read: every currently-paused activity with the size of
/// the backlog it is holding.
///
/// Uses a **correlated** `COUNT(*)` rather than a grouped join for the same
/// reason [`crate::queue_pause::list_paused_queues_query`] does: the grouped
/// form aggregates the entire `PENDING` backlog of the shard and only then
/// joins the (tiny) pause table, so its cost scales with unrelated pending work
/// — backwards for a diagnostic read consulted during exactly the incident
/// where the backlog is largest. The correlated count runs once per *paused
/// activity*, and returns `0` (rather than dropping the row) for a paused
/// activity holding nothing.
#[must_use]
pub const fn list_paused_activities_query() -> &'static str {
    "SELECT p.activity_name, p.reason, p.paused_by, p.paused_at, p.scope_shard_id, \
            (SELECT COUNT(*) FROM harvest_task_queue t \
             WHERE t.task_type = 'activity' \
               AND t.activity_name = p.activity_name \
               AND t.state = 'PENDING') AS held_task_count \
     FROM harvest_activity_pauses p \
     ORDER BY p.paused_at ASC, p.activity_name ASC"
}

/// One claimable task id **per distinct queue** for a just-resumed activity,
/// used only as the payload for the resume's wake notifications (`$1` = name).
///
/// One row per queue, not one row overall: the notify channel is per-queue, and
/// a single activity type's held backlog can span several of them (a per-call
/// `queue_override` routes the same activity onto a priority queue alongside its
/// default). Ringing only the first queue's doorbell would leave every other
/// queue's released backlog waiting for its workers' next poll tick — correct,
/// but needlessly slow during exactly the recovery the resume is driving.
///
/// One row *within* each queue because the notification is a **doorbell**, not
/// a work assignment: [`crate::notify::QueueListener`] hands the payload back to
/// the poll loop, which then re-polls that queue normally and drains it. The
/// predicate mirrors the shift's own due-row filter, so each id is a row this
/// resume actually made claimable rather than a future-scheduled retry that is
/// still not due.
#[must_use]
pub const fn resumed_activity_notify_task_query() -> &'static str {
    "SELECT DISTINCT ON (queue_name) id, queue_name FROM harvest_task_queue \
     WHERE task_type = 'activity' \
       AND activity_name = $1 \
       AND state = 'PENDING' \
       AND scheduled_at <= clock_timestamp() \
     ORDER BY queue_name, id"
}

/// A currently-paused activity type, as surfaced by the read API and the CLI.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct PausedActivity {
    /// The paused activity type.
    pub activity_name: String,
    /// Operator-supplied human-readable reason for the hold.
    pub reason: String,
    /// Operator identity that applied the pause.
    pub paused_by: String,
    /// When the pause was applied.
    pub paused_at: chrono::DateTime<chrono::Utc>,
    /// `None` = fleet-wide (the default); otherwise the shard the operator
    /// scoped the pause to.
    pub scope_shard_id: Option<i32>,
    /// `PENDING` activity tasks currently held for this activity type.
    pub held_task_count: i64,
}

/// Result of a pause request.
///
/// Idempotent: re-pausing an already-paused activity is a success no-op with
/// `newly_paused == false` and the **original** provenance preserved (mirroring
/// the `newly_terminated` / `newly_resumed` convention from #504 / #609 / #619).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ActivityPauseOutcome {
    /// The activity type that is now paused.
    pub activity_name: String,
    /// `false` when the activity was already paused (no-op).
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
    /// `PENDING` activity tasks held at the moment the pause was applied.
    pub held_task_count: i64,
}

/// Result of a resume request. Idempotent: resuming an activity that is not
/// paused is a success no-op with `newly_resumed == false`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ActivityResumeOutcome {
    /// The activity type that is dispatching again.
    pub activity_name: String,
    /// `false` when the activity was not paused (no-op).
    pub newly_resumed: bool,
    /// Tasks whose `scheduled_at` was credited back the held time.
    pub released_task_count: i64,
    /// How long the activity was held, in seconds (`0` on a no-op).
    pub paused_duration_secs: i64,
    /// The reason the hold was originally placed, echoed back on release.
    ///
    /// The pause row is deleted on resume, so this is the last moment the *why*
    /// is recoverable — the resume response (and therefore the CLI output)
    /// carries it so an operator closing out an incident sees exactly which
    /// hold they released. `None` on a no-op.
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
struct PausedActivityRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    activity_name: String,
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

/// The pause row as returned by the upsert, plus its own "did I insert this?"
/// signal.
#[cfg(feature = "db")]
#[derive(diesel::QueryableByName)]
struct UpsertedPauseRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    activity_name: String,
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
/// delete it between the two statements, leaving the fallback with zero rows —
/// a `500`, and worse, an activity left *unpaused* for an operator who was told
/// it was held. `DO UPDATE` locks the row and always returns it.
///
/// The `DO UPDATE` deliberately writes the columns back to their **existing**
/// values, so an idempotent re-pause preserves the original reason, operator and
/// instant while still returning the row (AC1: "pausing an already-paused type
/// returns success, not an error"). `xmax = 0` distinguishes the insert from the
/// no-op update.
///
/// Binds: `$1` name, `$2` reason, `$3` operator, `$4` scope shard id.
#[must_use]
pub const fn pause_activity_upsert_query() -> &'static str {
    "INSERT INTO harvest_activity_pauses \
         (activity_name, reason, paused_by, paused_at, scope_shard_id) \
     VALUES ($1, $2, $3, NOW(), $4) \
     ON CONFLICT (activity_name) DO UPDATE \
         SET reason = harvest_activity_pauses.reason, \
             paused_by = harvest_activity_pauses.paused_by, \
             paused_at = harvest_activity_pauses.paused_at, \
             scope_shard_id = harvest_activity_pauses.scope_shard_id \
     RETURNING activity_name, reason, paused_by, paused_at, scope_shard_id, \
               (xmax = 0) AS newly_paused"
}

/// Hold dispatch for one activity type on this shard's database.
///
/// Idempotent by construction: a re-pause returns the **original** provenance
/// with `newly_paused == false`. The caller is responsible for fanning the call
/// out across shards.
///
/// Validates the name itself via [`validate_activity_name`], matching
/// [`crate::queue_pause::pause_queue`]. A `pub` write path must not depend on
/// every caller remembering: an unvalidated whitespace-padded name would insert
/// a row that the claim gate — which matches exactly — can never match, giving
/// an operator a hold that *lists* as active while holding nothing. The
/// management API still validates up front so a typo is a `400` before a
/// connection is taken; this is the backstop, not a duplicate.
///
/// # Errors
///
/// [`HarvestError::Config`] when the name is blank, whitespace-padded, or
/// oversized; [`HarvestError::Database`] on query failure.
#[cfg(feature = "db")]
pub async fn pause_activity(
    conn: &mut AsyncPgConnection,
    activity_name: &str,
    reason: &str,
    paused_by: &str,
    scope_shard_id: Option<i32>,
) -> HarvestResult<ActivityPauseOutcome> {
    use diesel_async::{AsyncConnection, RunQueryDsl};

    let name = validate_activity_name(activity_name)?;
    let reason = reason.to_string();
    let paused_by = paused_by.to_string();

    // ONE transaction, matching `resume_activity`. The upsert and the held count
    // must commit or roll back together, or a failure of the *count* — a
    // best-effort observability read — inverts the reported outcome of a live
    // mutation: the pause row commits, the claim gate starts holding tasks
    // fleet-wide, and the operator sees a 500 and concludes the hold did not
    // take. That is the worst possible failure mode for an incident brake, and
    // the count is the statement most likely to be slow on a large backlog.
    Box::pin(
        conn.transaction::<ActivityPauseOutcome, HarvestError, _>(async |conn| {
            let (name, reason, paused_by) = (name, reason, paused_by);
            let row: UpsertedPauseRow = diesel::sql_query(pause_activity_upsert_query())
                .bind::<diesel::sql_types::Text, _>(&name)
                .bind::<diesel::sql_types::Text, _>(&reason)
                .bind::<diesel::sql_types::Text, _>(&paused_by)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(scope_shard_id)
                .get_result(conn)
                .await
                .map_err(crate::error::database_error)?;

            let held: CountRow = diesel::sql_query(held_task_count_query())
                .bind::<diesel::sql_types::Text, _>(&name)
                .get_result(conn)
                .await
                .map_err(crate::error::database_error)?;

            Ok(ActivityPauseOutcome {
                activity_name: row.activity_name,
                newly_paused: row.newly_paused,
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

/// SQL for the resume's pause-row delete, exposed for shape tests.
///
/// `RETURNING` the provenance is what lets [`ActivityResumeOutcome`] echo back
/// *which* hold was released: the row is gone after this statement, so this is
/// the last moment the reason is recoverable.
#[must_use]
pub const fn delete_activity_pause_query() -> &'static str {
    "DELETE FROM harvest_activity_pauses WHERE activity_name = $1 \
     RETURNING reason, paused_by, paused_at"
}

#[cfg(feature = "db")]
#[derive(diesel::QueryableByName)]
struct DeletedPauseRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    reason: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    paused_by: String,
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    paused_at: DateTime<Utc>,
}

/// One id returned by [`resume_shift_scheduled_at_query`], so the second pass
/// can exclude exactly the rows the first one shifted.
#[cfg(feature = "db")]
#[derive(diesel::QueryableByName)]
struct ShiftedTaskIdRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: uuid::Uuid,
}

#[cfg(feature = "db")]
#[derive(diesel::QueryableByName)]
struct NotifyTaskRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: uuid::Uuid,
    #[diesel(sql_type = diesel::sql_types::Text)]
    queue_name: String,
}

/// Release the hold on one activity type on this shard's database.
///
/// Deletes the pause row, credits held time back to the released backlog's
/// `scheduled_at` (see [`resume_shift_scheduled_at_query`]), and rings the
/// queue doorbell so parked workers re-poll as soon as this commits — all in
/// **one transaction**, so a partially-applied resume is impossible: either the
/// hold is lifted and the backlog is credited, or neither happened.
///
/// Idempotent: resuming an activity that is not paused is a success no-op with
/// `newly_resumed == false` and no shift.
///
/// Validates the name itself for the same reason [`pause_activity`] does — see
/// its note — so the two directions cannot disagree about what a valid name is.
///
/// # Errors
///
/// [`HarvestError::Config`] when the name is blank, whitespace-padded, or
/// oversized; [`HarvestError::Database`] on query failure.
#[cfg(feature = "db")]
pub async fn resume_activity(
    conn: &mut AsyncPgConnection,
    activity_name: &str,
) -> HarvestResult<ActivityResumeOutcome> {
    use diesel_async::{AsyncConnection, RunQueryDsl};

    let name = validate_activity_name(activity_name)?;
    Box::pin(
        conn.transaction::<ActivityResumeOutcome, HarvestError, _>(async |conn| {
            let name = name;
            let deleted: Vec<DeletedPauseRow> = diesel::sql_query(delete_activity_pause_query())
                .bind::<diesel::sql_types::Text, _>(&name)
                .load(conn)
                .await
                .map_err(crate::error::database_error)?;

            let Some(pause) = deleted.into_iter().next() else {
                return Ok(ActivityResumeOutcome {
                    activity_name: name,
                    newly_resumed: false,
                    released_task_count: 0,
                    paused_duration_secs: 0,
                    released_reason: None,
                    released_paused_by: None,
                });
            };

            let shifted: Vec<ShiftedTaskIdRow> =
                diesel::sql_query(resume_shift_scheduled_at_query())
                    .bind::<diesel::sql_types::Text, _>(&name)
                    .bind::<diesel::sql_types::Timestamptz, _>(pause.paused_at)
                    .load(conn)
                    .await
                    .map_err(crate::error::database_error)?;
            let released = shifted.len();
            let shifted_ids: Vec<uuid::Uuid> = shifted.into_iter().map(|r| r.id).collect();

            // Second pass, before the doorbell so it sees the freshest snapshot:
            // a task still held (our DELETE has not committed, so every other
            // session still sees the pause row) may be invisible to the primary
            // shift above -- whether it was created during the hold, or created
            // before it by an enqueue that only committed just now. This pass
            // covers every due row the primary pass did not report shifting, so
            // the two are disjoint by construction and no row is credited twice.
            // See `resume_shift_late_arrivals_query` for the irreducible
            // residual this narrows but cannot close.
            let late: usize = diesel::sql_query(resume_shift_late_arrivals_query())
                .bind::<diesel::sql_types::Text, _>(&name)
                .bind::<diesel::sql_types::Timestamptz, _>(pause.paused_at)
                .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(&shifted_ids)
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;

            // Ring the doorbell INSIDE the transaction: Postgres queues NOTIFY
            // and delivers it at COMMIT, so listeners wake exactly when the
            // hold lifts — never earlier (an early wake would still see the
            // uncommitted pause row and skip the task) and never at all if this
            // rolls back.
            let notify: Vec<NotifyTaskRow> =
                diesel::sql_query(resumed_activity_notify_task_query())
                    .bind::<diesel::sql_types::Text, _>(&name)
                    .load(conn)
                    .await
                    .map_err(crate::error::database_error)?;
            for row in notify {
                crate::notify::notify_task_enqueued(conn, &row.queue_name, row.id).await?;
            }

            let paused_duration_secs = (Utc::now() - pause.paused_at).num_seconds().max(0);
            Ok(ActivityResumeOutcome {
                activity_name: name,
                newly_resumed: true,
                released_task_count: i64::try_from(released.saturating_add(late))
                    .unwrap_or(i64::MAX),
                paused_duration_secs,
                released_reason: Some(pause.reason),
                released_paused_by: Some(pause.paused_by),
            })
        }),
    )
    .await
}

/// Every currently-paused activity on this shard's database.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] on query failure.
#[cfg(feature = "db")]
pub async fn list_paused_activities(
    conn: &mut AsyncPgConnection,
) -> HarvestResult<Vec<PausedActivity>> {
    use diesel_async::RunQueryDsl;

    let rows: Vec<PausedActivityRow> = diesel::sql_query(list_paused_activities_query())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    Ok(rows
        .into_iter()
        .map(|r| PausedActivity {
            activity_name: r.activity_name,
            reason: r.reason,
            paused_by: r.paused_by,
            paused_at: r.paused_at,
            scope_shard_id: r.scope_shard_id,
            held_task_count: r.held_task_count,
        })
        .collect())
}

/// Names of every currently-paused activity on this shard's database.
///
/// Deliberately narrower (and cheaper) than [`list_paused_activities`], which
/// also runs a correlated `COUNT(*)` over `harvest_task_queue` per paused
/// activity to report `held_task_count`. The eligibility explainer (issue #807)
/// only needs the name *set* to decide whether a task is held, and it is
/// consulted during exactly the incident where that backlog — and so the cost
/// of counting it — is largest. Exact mirror of
/// [`crate::queue_pause::paused_queue_names`], which exists for the same reason.
///
/// Unfiltered, matching the `paused_activities` CTE in `queue::claim_task`: an
/// activity pause is fleet-wide across queues on this shard, so there is no
/// natural bound to pre-filter by. The table holds one row per *currently
/// paused* activity — empty in steady state, a handful during an incident.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] on query failure.
#[cfg(feature = "db")]
pub async fn paused_activity_names(conn: &mut AsyncPgConnection) -> HarvestResult<Vec<String>> {
    use diesel_async::RunQueryDsl;

    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        activity_name: String,
    }

    let rows: Vec<Row> = diesel::sql_query(
        "SELECT activity_name FROM harvest_activity_pauses ORDER BY activity_name ASC",
    )
    .load(conn)
    .await
    .map_err(crate::error::database_error)?;

    Ok(rows.into_iter().map(|r| r.activity_name).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Timeout-enforcer re-check (AC3: a held task is never failed) ────────

    /// The enforcer's authoritative re-check must be a *single* statement, so it
    /// takes a fresh `READ COMMITTED` snapshot and therefore observes a pause
    /// that committed after the scan predicate ran. A multi-statement or
    /// join-based form would reintroduce the very staleness it exists to close.
    /// A **workflow** task can legitimately carry a non-NULL `activity_name`:
    /// the engine stamps the `'mixed_signal_suspension'` sentinel there (see
    /// `queue::claim_task`'s wake handling). So "workflow rows have a NULL
    /// `activity_name`" is a naming coincidence, not an invariant, and an
    /// anti-join resting on it would hold every mixed-signal-suspended workflow
    /// task fleet-wide the moment an operator paused an activity that happened
    /// to share the sentinel's name. Scope the anti-join by `task_type` so the
    /// guarantee is structural instead.
    #[test]
    fn anti_join_is_scoped_by_task_type_not_by_a_null_activity_name() {
        let sql = activity_pause_anti_join("t");
        assert!(
            sql.contains("t.task_type = 'activity'"),
            "the anti-join must be scoped to activity rows, so a workflow task \
             carrying the 'mixed_signal_suspension' sentinel can never be held \
             by an activity pause; got:\n{sql}"
        );
    }

    /// A single activity's held tasks can sit on **several** queues — a
    /// per-call `queue_override` puts the same activity type on a priority queue
    /// alongside its default one. The notify channel is per-queue, so a resume
    /// must ring one doorbell per distinct queue it actually released work on;
    /// a single `LIMIT 1` row would wake one queue's workers and leave the
    /// others' backlog sitting until their next poll tick.
    #[test]
    fn resume_notify_lookup_returns_one_row_per_distinct_queue() {
        let sql = resumed_activity_notify_task_query();
        assert!(
            sql.contains("DISTINCT ON (queue_name)"),
            "the resume doorbell must cover every queue the activity's released \
             backlog sits on, not just the first; got:\n{sql}"
        );
        assert!(
            !sql.contains("LIMIT 1"),
            "a global LIMIT 1 silently drops every queue but one; got:\n{sql}"
        );
    }

    #[test]
    fn enforcer_recheck_is_a_single_existence_statement_on_the_pause_table() {
        let sql = is_activity_paused_query();
        assert!(
            sql.contains("harvest_activity_pauses"),
            "the re-check must read the durable pause table; got:\n{sql}"
        );
        assert!(
            sql.contains("activity_name = $1"),
            "the re-check must be scoped to the task's own activity; got:\n{sql}"
        );
        assert_eq!(
            sql.matches(';').count(),
            0,
            "must be one statement so it takes a single fresh snapshot; got:\n{sql}"
        );
    }

    // ── Name validation (AC1: a typo must never report a false hold) ────────

    #[test]
    fn validate_activity_name_accepts_a_plain_name() {
        assert_eq!(
            validate_activity_name("charge_card").expect("valid"),
            "charge_card"
        );
    }

    #[test]
    fn validate_activity_name_rejects_blank_and_whitespace_only() {
        assert!(validate_activity_name("").is_err());
        assert!(validate_activity_name("   ").is_err());
        assert!(validate_activity_name("\t\n").is_err());
    }

    /// A stray space must be a `400`, never a silent hold on a different (or
    /// no) activity — the same rule, and the same reasoning, as
    /// `queue_pause::validate_queue_name`.
    #[test]
    fn validate_activity_name_rejects_surrounding_whitespace() {
        for name in ["charge_card ", " charge_card", " charge_card "] {
            let err = validate_activity_name(name).expect_err("must reject");
            assert!(
                err.to_string().contains("whitespace"),
                "the error must name the problem; got: {err}"
            );
        }
    }

    #[test]
    fn validate_activity_name_rejects_oversized() {
        let long = "a".repeat(MAX_ACTIVITY_NAME_LEN + 1);
        assert!(validate_activity_name(&long).is_err());
        let at_cap = "a".repeat(MAX_ACTIVITY_NAME_LEN);
        assert!(validate_activity_name(&at_cap).is_ok());
    }

    // ── The anti-join (mirror queries) ──────────────────────────────────────

    /// The outer reference must be qualified. An unqualified `activity_name`
    /// resolves against `harvest_activity_pauses` first, degenerating the
    /// predicate to `ap.activity_name = ap.activity_name` — always true — which
    /// would hold *every* activity the moment any one is paused.
    #[test]
    fn anti_join_qualifies_the_outer_column_reference() {
        let sql = activity_pause_anti_join("harvest_task_queue");
        assert!(
            sql.contains("ap.activity_name = harvest_task_queue.activity_name"),
            "the outer column must be table-qualified; got: {sql}"
        );
        assert!(sql.starts_with("NOT EXISTS (SELECT 1 FROM harvest_activity_pauses ap"));
    }

    #[test]
    fn anti_join_renders_for_an_aliased_outer_table() {
        assert!(
            activity_pause_anti_join("tq").contains("ap.activity_name = tq.activity_name"),
            "the renderer must correlate against whatever alias the mirror uses"
        );
    }

    /// The Rust-side re-check in `timeout::enforce_activity_timeout` compares
    /// `task.task_type` against [`ACTIVITY_TASK_TYPE`], while the SQL renderers
    /// embed it as a literal. If the constant and the literal ever disagree the
    /// enforcer would silently stop matching its own scan predicate — the
    /// sentinel hazard reopening through the back door — so pin that they agree.
    #[test]
    fn anti_join_scopes_by_the_named_task_type() {
        assert_eq!(
            ACTIVITY_TASK_TYPE, "activity",
            "the constant must equal the `harvest_task_queue.task_type` value the \
             engine actually writes for activity tasks"
        );
        assert!(
            activity_pause_anti_join("tq")
                .contains(&format!("tq.task_type = '{ACTIVITY_TASK_TYPE}'")),
            "the rendered SQL must scope by the same value the Rust re-check compares"
        );
    }

    // ── SQL shapes ──────────────────────────────────────────────────────────

    /// A hold must never consume retry budget (AC3): the release restores the
    /// attempt the claim incremented.
    #[test]
    fn release_claim_restores_the_attempt_and_is_guarded_to_this_worker() {
        let sql = release_claim_if_activity_paused_query();
        assert!(sql.contains("attempt = GREATEST(attempt - 1, 0)"));
        assert!(sql.contains("state = 'RUNNING'"));
        assert!(sql.contains("worker_id = $2"));
        assert!(
            sql.contains(&format!("task_type = '{ACTIVITY_TASK_TYPE}'")),
            "the release must be safe in isolation: a workflow row can carry the \
             `mixed_signal_suspension` sentinel in `activity_name`, so relying on \
             the caller to pre-filter by task_type would be a footgun for the next \
             caller; got: {sql}"
        );
    }

    /// The resume credit must never touch `RUNNING` rows (AC4) nor the absolute
    /// `schedule_to_close` deadline.
    #[test]
    fn resume_shift_only_touches_due_pending_activity_rows() {
        let sql = resume_shift_scheduled_at_query();
        assert!(sql.contains("state = 'PENDING'"));
        assert!(sql.contains("task_type = 'activity'"));
        assert!(sql.contains("scheduled_at <= clock_timestamp()"));
        assert!(
            !sql.contains("schedule_to_close_at"),
            "the absolute deadline must keep ticking through a hold"
        );
    }

    /// `NOW()` is frozen at transaction start, so it would omit the resume's own
    /// duration from the credit and time out the backlog the instant it thaws.
    #[test]
    fn resume_shift_uses_the_volatile_clock_not_the_frozen_transaction_clock() {
        let sql = resume_shift_scheduled_at_query();
        assert!(sql.contains("clock_timestamp()"));
        assert!(
            !sql.contains("NOW()"),
            "a frozen NOW() under-credits by the resume's own duration; got:\n{sql}"
        );
    }

    /// An idempotent re-pause must preserve the ORIGINAL provenance, or the
    /// operator's audit trail silently rewrites itself on every retry.
    #[test]
    fn pause_upsert_preserves_original_provenance_and_reports_insert_vs_update() {
        let sql = pause_activity_upsert_query();
        assert!(sql.contains("ON CONFLICT (activity_name) DO UPDATE"));
        assert!(sql.contains("reason = harvest_activity_pauses.reason"));
        assert!(sql.contains("paused_by = harvest_activity_pauses.paused_by"));
        assert!(sql.contains("paused_at = harvest_activity_pauses.paused_at"));
        assert!(sql.contains("(xmax = 0) AS newly_paused"));
    }

    /// The read must not disappear a paused activity that happens to be holding
    /// nothing — a correlated count returns `0` where an inner join drops the
    /// row.
    #[test]
    fn list_query_uses_a_correlated_count_so_an_empty_hold_still_lists() {
        let sql = list_paused_activities_query();
        assert!(sql.contains("(SELECT COUNT(*) FROM harvest_task_queue t"));
        assert!(!sql.contains("GROUP BY"));
        assert!(sql.contains("FROM harvest_activity_pauses p"));
    }

    /// The doorbell is per-queue, so the notify lookup must carry the queue the
    /// woken task actually sits on — an activity's backlog can span queues.
    #[test]
    fn notify_lookup_returns_the_tasks_own_queue() {
        let sql = resumed_activity_notify_task_query();
        assert!(
            sql.contains("id, queue_name"),
            "the doorbell payload carries the task's own queue, because the \
             notify channel is per-queue; got:\n{sql}"
        );
        // One row per queue — `resume_notify_lookup_returns_one_row_per_distinct_queue`
        // pins why a single row would strand every other queue's backlog.
        assert!(sql.contains("DISTINCT ON (queue_name)"), "got:\n{sql}");
    }

    // ── Timeout interaction (AC3) ───────────────────────────────────────────

    #[cfg(feature = "db")]
    #[test]
    fn pause_suppresses_only_the_relative_schedule_to_start_timer() {
        use crate::timeout::TimeoutReason;
        assert!(activity_pause_suppresses_timeout(
            &TimeoutReason::ScheduleToStart,
            true
        ));
        // A hold does not extend an absolute SLA deadline...
        assert!(!activity_pause_suppresses_timeout(
            &TimeoutReason::ScheduleToClose,
            true
        ));
        // ...and already-dispatched work runs to completion on its own merits.
        assert!(!activity_pause_suppresses_timeout(
            &TimeoutReason::Heartbeat,
            true
        ));
        assert!(!activity_pause_suppresses_timeout(
            &TimeoutReason::StartToClose,
            true
        ));
        // Nothing is suppressed when the activity is not paused.
        assert!(!activity_pause_suppresses_timeout(
            &TimeoutReason::ScheduleToStart,
            false
        ));
    }
}
