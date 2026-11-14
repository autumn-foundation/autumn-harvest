//! Per-activity-type pause/resume — surgical outage containment (issue #807).
//!
//! When **one** downstream dependency has an incident, the operator knows which
//! activity is broken. Examples are a payment vendor that returns 500s, a
//! drained internal service, or a rate-limit ban. The operator must stop
//! dispatch of *that* activity, fleet-wide, now. Healthy work that shares its
//! queue must keep running.
//!
//! # How it composes with the neighbouring brakes
//!
//! | Primitive | Granularity | Trigger | What it does | Issue |
//! |---|---|---|---|---|
//! | Admission gate | workflow **starts** | manual | halts new starts; in-flight runs keep scheduling activities | #377 / #618 |
//! | Queue pause | a whole **task queue** | manual | holds dispatch for every activity on that queue | #619 |
//! | Circuit breaker | one **activity type** | *automatic*, reactive | **short-circuits** dispatch after failures accumulate (defers by default, #1809) | #369 |
//! | **Activity pause (this)** | one **activity type** | *manual*, proactive | **holds** dispatch: nothing fails, retries, or dead-letters | **#807** |
//!
//! The breaker and this feature share a granularity but not a job. The breaker
//! is a reflex. It trips only *after* retryable failures burn retry budget, grow
//! histories and feed the DLQ. It exists only for activities whose author
//! declared a policy at compile time. This feature is the operator's hand on the
//! same valve: proactive, available for every activity, and holding rather than
//! failing.
//!
//! Queue pause is the same *shape* one level up. It is the wrong tool when many
//! activity types share the `default` queue. A hold on the queue to stop one
//! broken call also halts email, inventory and notifications.
//!
//! # Enforcement model — durable, claim-layer, no cache
//!
//! A pause is durable metadata enforced on the claim path, exactly like the
//! queue pause (#619), the PAUSED-execution skip (#383) and the rate-limit gate
//! (#332). It is deliberately **not** an in-process registry like the circuit
//! breaker's:
//!
//! - "Durable, and applied on every shard and every worker" is true **by
//!   construction**. There is no boot window to lose a pause in, no refresh
//!   interval, and no fleet-wide staleness window. A worker that starts during
//!   an incident sees the hold on its first claim.
//! - There is no global static and no write-through path that can diverge from
//!   the database.
//!
//! # Replay contract
//!
//! A pause writes **nothing** to `harvest_events` and adds **no**
//! [`crate::event::WorkflowEvent`] variant. A workflow that waits on a paused
//! activity sees a delayed dispatch and nothing else. Replay of an execution
//! that touched a paused activity is byte-identical to one that never paused.
//!
//! # Timeout interaction
//!
//! The *relative* `schedule_to_start` timer is suspended for held tasks. A
//! resume credits held time back to `scheduled_at`, so a thaw does not time out
//! the whole backlog. Without both halves, the scanner that detects starved
//! queues would violate AC3's "remain `PENDING` (not failed, not DLQ'd)". The
//! *absolute* `schedule_to_close` deadline (#378) keeps ticking, because a hold
//! does not extend an SLA. This matches the scope decision in #619.

#[cfg(feature = "db")]
use crate::error::HarvestError;
use crate::error::HarvestResult;
#[cfg(feature = "db")]
use chrono::{DateTime, Utc};
#[cfg(feature = "db")]
use diesel_async::AsyncPgConnection;

/// Upper bound on a pausable activity name.
///
/// Activity names are free-form `TEXT` on `harvest_task_queue`. This table's
/// primary key is the activity name, so an unbounded operator string would be
/// an unbounded btree key. 255 matches [`crate::queue_pause::MAX_QUEUE_NAME_LEN`].
pub const MAX_ACTIVITY_NAME_LEN: usize = 255;

/// Validate an operator-supplied activity name and return its canonical form.
///
/// Pure, with no database access, so the management API can reject a typo
/// with a `400` before it touches a connection.
///
/// Surrounding whitespace is **rejected, not trimmed**, for the same reason
/// as in [`crate::queue_pause::validate_queue_name`]. Every read and write here
/// matches on exact equality. A name that does not round-trip verbatim would
/// hold a *different* activity than the operator named, while the API reports
/// success. Storing the raw name after validating a trimmed one inserts a row
/// that matches no task. Trimming silently retargets a real, different
/// activity. Only rejection cannot lie. During an outage, a `400` that names
/// the problem beats a false green.
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

/// Render the activity-pause anti-join correlated against `outer_table`.
///
/// Every query that mirrors the claim gate takes its anti-join from here. A
/// mirror that forgot a paused activity would report a phantom backlog. That
/// drives a false capacity alert during a deliberate hold.
///
/// # Why the mirrors use this and the claim path does not
///
/// The anti-join is scoped by **`task_type`**, not by a NULL `activity_name`.
///
/// NULL semantics look sufficient: `ap.activity_name = NULL` is never true, so
/// `NOT EXISTS (...)` stays `TRUE` for a workflow task. That reasoning is
/// **wrong**, because a workflow row's `activity_name` is not reliably NULL.
/// The engine stamps the `'mixed_signal_suspension'` sentinel into that column.
/// See `queue::claim_task`'s wake handling and
/// `queue::requeue_workflow_task_nd_blocked`, which has to clear it. With NULL
/// semantics, the guarantee holds only while no registered activity shares the
/// sentinel's name. If one did, its pause would silently hold *every*
/// mixed-signal-suspended workflow task in the fleet.
///
/// The explicit `task_type = 'activity'` predicate makes the guarantee
/// structural at zero cost. The pause table is empty in steady state, so the
/// subquery is a single index probe either way.
///
/// The hot claim path cannot use this correlated form. There it is re-probed
/// once per *candidate row*, and it measured ~99% of the claim query's buffer
/// touches (`docs/performance.md`, issue #619). So the claim path uses a
/// pre-filtered array with its own explicit
/// `task_type != 'activity' OR activity_name IS NULL` guard. See
/// `queue::claim_task_query` and the test
/// `claim_query_activity_pause_gate_never_holds_workflow_tasks`.
///
/// The outer column reference **must** be qualified. Inside the subquery, an
/// unqualified `activity_name` resolves against `harvest_activity_pauses`
/// first. The predicate then becomes `ap.activity_name = ap.activity_name`.
/// That is always true, so one pause would silently hold *every* activity.
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
/// Every pause predicate is scoped by this value, for the sentinel reason on
/// [`activity_pause_anti_join`]. That covers the SQL renderers here, the
/// pre-filtered claim gate, the plugin's eligibility explainer, and the Rust
/// re-check in `timeout::enforce_activity_timeout`. One name keeps the Rust
/// comparisons and the SQL literals in agreement. The test
/// `anti_join_scopes_by_the_named_task_type` pins that.
pub const ACTIVITY_TASK_TYPE: &str = "activity";

/// Whether an activity pause suspends enforcement of `reason` for a held task.
///
/// Only the **relative** `schedule_to_start` timer is suspended. A task must
/// not fail terminally only because it waited while an operator held its
/// activity type. AC3 says held tasks "remain `PENDING` (not failed, not
/// DLQ'd)".
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
/// It is one statement, so it takes a fresh `READ COMMITTED` snapshot. It sees
/// any pause committed before it began, which is the entire point (see
/// [`release_claim_if_activity_paused`]).
///
/// Restores `attempt`: the task never ran, so a hold must not consume retry
/// budget (AC3).
///
/// The statement itself carries `task_type = 'activity'`. The caller in
/// `queue::claim_task` checks it too, but this function is `pub`. A second
/// caller that omitted the check could release a claimed *workflow* task. A
/// workflow row can carry a non-NULL `activity_name`: the
/// `'mixed_signal_suspension'` sentinel (see [`activity_pause_anti_join`]).
/// One conjunct on a primary-key lookup makes the statement safe in isolation.
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
/// The claim is a single CTE statement. Under `READ COMMITTED`, its whole body
/// evaluates against **one snapshot taken at statement start**, the
/// `paused_activities` pre-filter included. That snapshot cannot see a pause
/// that commits later. So a claim in flight when the operator paused can still
/// move its task to `RUNNING`. It then hands the task to a worker that
/// dispatches into the outage. A re-check in a **fresh statement** gets a
/// fresh snapshot. It costs one indexed probe per *successful activity claim*,
/// never per empty poll and never for a workflow task. The contract is crisp:
/// **a pause committed before this re-check's statement begins always wins.**
///
/// The release is guarded on `state = 'RUNNING' AND worker_id = $2`, so it can
/// undo only *this* worker's own claim. The rate-limit token that the claim
/// debited is not refunded. That is the same safe-side, self-healing leak as a
/// lost claim in [`crate::queue::claim_task`]: it only under-dispatches.
///
/// # Residual window (deliberate)
///
/// This re-check's verdict is authoritative as of *its own snapshot*, not
/// through commit. A pause can commit between this statement and the claim
/// transaction's `COMMIT`, one sub-millisecond round trip. The operator can then
/// get an acknowledgement while one already-claimed task still dispatches.
///
/// Queue pause closes that window with a shared advisory lock on the queue key.
/// This feature deliberately does not. `queue_pause::queue_lock_keys` spends
/// all 64 bits of the two-argument advisory keyspace on queue identity and
/// reserves no class id. A CI-enforced source walk
/// (`queue_pause::queue_pause_owns_the_two_argument_advisory_keyspace`) keeps
/// that keyspace single-user for that reason. Five subsystems already share
/// the single-argument keyspace without a namespace. One of them is the
/// concurrency gate on this same hot claim path. There, a collision silently
/// stops dispatch on an unrelated key: the precise failure this feature exists
/// to prevent. The window leaks at most one already-claimed task per racing
/// worker, and the issue's own bar is "effective within one poll interval". So
/// a collision risk to close a sub-millisecond window is the wrong trade. The
/// leaked task's *next* attempt is held like any other.
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
    crate::queue::release_claim_via(
        conn,
        release_claim_if_activity_paused_query(),
        task_id,
        worker_id,
    )
    .await
}

/// The resume-time `scheduled_at` credit.
///
/// Without it, a thaw of an activity whose backlog waited past its
/// `schedule_to_start` would time out the *entire* held backlog at once. That
/// is the AC3 failure ("not failed, not DLQ'd") this feature exists to prevent.
///
/// For each due `PENDING` task of the activity, it credits back only the time
/// the row was really held. It uses the two-sided formula of queue pause
/// (issue #619):
///
/// - **Eligible before the pause** (`created_at < paused_at`, or a pre-
///   `created_at` row): `scheduled_at += clock_timestamp() - GREATEST(scheduled_at, paused_at)`.
///   Pre-pause waiting is preserved. A task that waited 100 s still shows
///   100 s of wait after the thaw, and two such tasks keep their order.
/// - **Created during the hold**: collapses to the thaw instant. It was held
///   from the moment it became due, so it accrues no wait.
/// - **Not yet due** (`scheduled_at > now`, e.g. a retry backoff): excluded
///   entirely. It was never held, so its backoff must not be extended.
///
/// `created_at` is nullable: a row from before `20260619000000` has none. In
/// SQL, a comparison against `NULL` is `NULL`, not `false`. So the explicit
/// `IS NULL` arm puts such a row on the pre-pause side. The row predates that
/// migration, so it predates any pause.
///
/// # Why `clock_timestamp()` and not `NOW()`
///
/// `NOW()` is `transaction_timestamp()`, frozen at **transaction start**,
/// before the pause-row `DELETE` and before this `UPDATE` runs. The hold stays
/// in force until this transaction **commits**. A frozen `NOW()` silently
/// omits the resume's own duration from the credit. Then any task whose
/// `schedule_to_start` is shorter than the resume's runtime times out right
/// after the thaw. `clock_timestamp()` is volatile and re-read per row at
/// execution time.
///
/// It is still a **lower bound**, deliberately. The true thaw instant is commit
/// time, which no in-statement expression can know. So each row is
/// under-credited by at most the remaining duration of this `UPDATE`. That is
/// the *safe* direction. Over-crediting past commit time would push
/// `scheduled_at` into the future and make the task **un**claimable.
///
/// # Overlapping queue and activity holds over-credit, deliberately
///
/// A task can be held by BOTH its queue (#619) and its activity type at once.
/// Each resume path applies this same *relative* shift without knowledge of
/// the other, so the overlap is credited twice. Take a task scheduled at
/// `t=0`, held by both from `t=100`, and resumed at `t=200` and then `t=300`.
/// It ends with `scheduled_at = 300`, where the union-correct value is `200`.
/// That erases 100 s of real pre-pause wait and gives the task a fresh
/// `schedule_to_start` budget.
///
/// That is bounded, and it is the **safe** direction. The formula's result is
/// always `<= clock_timestamp()`: when `scheduled_at <= paused_at` it is
/// `scheduled_at + now - paused_at <= now`, and otherwise it collapses to
/// exactly `now`. So no combination of holds can push a task into the future or
/// make it un-claimable, and none can credit *less* than the task earned. The
/// cost is delayed starvation detection — never a spurious terminal failure,
/// which is the precise outcome both pause features exist to prevent.
///
/// The two obvious coordinations are worse. Each trades this safe over-credit
/// for an **unsafe** under-credit:
///
/// - *Skip a row while the other mechanism still holds it.* Correct when the
///   holds begin together. Staggered starts break it: queue from `t=100`,
///   activity from `t=150`, resumed at `200` then `300`. Only the surviving
///   resume credits, and from `t=150`. The task lands on `150` against a
///   union-correct `200`.
/// - *A per-row `credited_through` watermark.* Correct on that ordering, and
///   symmetrically wrong on its reverse (activity from `t=150` resumed first,
///   queue from `t=100` resumed second) — also `150` against `200`.
///
/// An exact union credit needs the *earliest* `paused_at` among the
/// overlapping holds to survive until the last resume. Both pause tables key
/// on the name and hard-delete on resume, so by then that value is gone.
/// Recovery needs new persistent state: a `held_since` watermark on
/// `harvest_task_queue`, the hottest table in the schema. It also needs an
/// anti-join against the other mechanism and a matching change to the shipped
/// resume path of #619. That migration-bearing cross-feature change is a
/// tracked follow-up. `overlapping_queue_and_activity_holds_over_credit_but_never_starve`
/// pins both halves of the contract, so a future coordination cannot land the
/// unsafe direction unnoticed.
///
/// # Why one `CASE` covers both sides, and a second pass still runs
///
/// This statement credits both sides of the `created_at` partition with one
/// `CASE`. It credits every due `PENDING` row visible to its snapshot exactly
/// once, by the `CASE` arm that its `created_at` selects. It returns the ids it
/// shifted (`RETURNING id`). A row that commits *after* this statement's
/// snapshot is still held, but this statement cannot see it. So the resume
/// runs [`resume_shift_late_arrivals_query`] as a second pass, as queue pause
/// does. That pass skips the ids this statement reported, so no row gets the
/// credit twice. It also documents the irreducible residual. To close that
/// residual completely, **enqueue** must serialize behind a pause-scoped lock.
/// That puts a serialization point on the hot enqueue path for every task,
/// paused or not.
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
/// pass did **not** shift.
///
/// `$3` holds the ids that the primary pass reported. The exclusion uses those
/// ids, not a predicate.
///
/// # Why a second pass exists at all (issue #807)
///
/// Under READ COMMITTED, each statement takes its own snapshot. A task can be
/// enqueued *after* the primary shift began but before this transaction's
/// pause `DELETE` commits. That pass cannot see the task, but the task is still
/// held: every other session still sees the uncommitted pause row and skips it.
/// With a single pass, such a row thaws with its whole held wait. The
/// `schedule_to_start` scanner can then fail it terminally the instant the hold
/// lifts. That is the exact failure the credit exists to prevent. The window is
/// widest where it hurts: a large backlog keeps the bulk `UPDATE` and the
/// per-queue notification pass running longest.
///
/// # Why exclude by id rather than by predicate
///
/// The primary pass handles *both* sides of the `created_at` partition inline.
/// A predicate-based exclusion here would have to re-derive which rows it saw.
/// "Which rows a statement saw" is a question about visibility, not about the
/// row. Exclusion of the ids it reported makes coverage total for **any**
/// commit order. A row it shifted is skipped here, because a second
/// application of the relative `+=` formula would erase the pre-pause wait it
/// preserves. This pass credits every other due `PENDING` row. An empty `$3`
/// leaves every row to this pass.
///
/// The SQL uses `NOT EXISTS (… unnest …)`, not `id <> ALL(<array>)`, on
/// purpose. The latter is a per-row linear scan of the array. The former lets
/// the planner build a hash anti-join, so cost is O(rows + ids), not
/// O(rows × ids). Mirrors [`crate::queue_pause::resume_shift_late_arrivals_query`].
///
/// # Irreducible residual
///
/// This pass narrows the uncredited window from "the whole bulk `UPDATE` plus
/// the notification pass" to "this final, small statement". It cannot close it.
/// A row committed after *this* statement's snapshot and before this
/// transaction commits is still held and still gets no credit. The remaining
/// duration of this statement bounds the under-credit. That is microseconds
/// for an activity with no late arrivals, and it errs in the same safe
/// direction as the primary pass. To close it completely, **enqueue** must
/// serialize behind an activity-scoped lock. That puts a serialization point on
/// the hot enqueue path for every task, paused or not. This module's design
/// notes reject that trade for the claim path.
///
/// # Why the ids round-trip through this process
///
/// The exclusion set is loaded into application memory and sent back as an
/// array bind, so it is linear in the released backlog. Each shifted task costs
/// one `Uuid`, serialised onto the wire and re-materialised server-side by
/// `unnest`. On a large incident backlog, that is real memory and real
/// connection traffic.
///
/// It is still the shipped design. The obvious database-side alternative was
/// **tried and reverted** on the queue-pause sibling (issue #619). It captured
/// the ids into a transaction-scoped `CREATE TEMP TABLE ... ON COMMIT DROP`.
/// That needs the database-level `TEMPORARY` privilege, which no migration
/// grants and no preflight check validates. A deployment that revoked it, a
/// recognised hardening step, passes every permission check. It then fails
/// **every** resume, which rolls back the pause deletion. The activity stays
/// frozen at the moment an operator tries to thaw it. A rare, observable scale
/// problem is a better failure than a deterministic, silent, config-dependent
/// break of the core operation.
///
/// A second application of the credit is not an option either. The primary
/// formula is a *relative* `+=`, so a second application over-credits. It
/// pushes `scheduled_at` past commit time, the one direction the queue-pause
/// notes call unsafe. A data-modifying CTE that folds both passes into one
/// statement defeats the purpose. Both halves would share a single snapshot,
/// so the late pass would see the same rows as the primary one and catch
/// nothing.
///
/// Exposure here is strictly smaller than in the shipped queue sibling. This
/// set is one activity type's backlog, a subset of the queue backlog that the
/// sibling bounds. The proper fix is shared with it and tracked as a follow-up:
/// a real handoff table keyed by a per-resume token. The grants that the
/// migration already issues cover such a table, so it needs no privilege that
/// the deployment may lack.
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
/// It is one statement, so it takes a fresh `READ COMMITTED` snapshot. It sees
/// any pause committed before it began, which is the entire point.
#[must_use]
pub const fn is_activity_paused_query() -> &'static str {
    "SELECT EXISTS ( \
         SELECT 1 FROM harvest_activity_pauses WHERE activity_name = $1 \
     ) AS paused"
}

/// Is this activity type currently held on **this shard's** database?
///
/// The re-check for [`crate::timeout::enforce_timeouts_once`]'s
/// `ScheduleToStart` path. The scan predicate that selected the task is a
/// non-locking snapshot, so it cannot see a pause that commits after the scan.
/// The enforcer would then fail the very task the hold exists to protect. The
/// result is an `ActivityTimedOut` that the pause came one moment too late to
/// prevent. A call inside the enforcement transaction closes all but a bounded
/// residual, described below.
///
/// # Called TWICE per enforcement, and why
///
/// `enforce_activity_timeout` calls this once near the top of its transaction
/// and again **after** it has taken the execution and task row locks. The first
/// call is an advisory fast path. It lets a held task bail before it pays for
/// the row locks and the history load. It is emphatically **not** the
/// guarantee. The statement immediately after it,
/// `lock_workflow_execution_row_and_load_history`, is a `FOR UPDATE`. It can
/// block for an **unbounded** period behind any other holder of that row.
/// `pause_activity` shares no row with that transaction. So an operator pause
/// commits freely during that wait and returns success. With only the first
/// call, the enforcer then failed the task anyway. The second call, after the
/// last blocking acquisition, is the authoritative one.
///
/// # Residual window (deliberate)
///
/// Even the authoritative call is authoritative as of **its own snapshot**, not
/// through commit. It does not see a pause that commits between it and the
/// enforcement transaction's `COMMIT`, so one already-scanned task can still
/// fail. This transaction's own remaining work bounds that window. Every
/// statement from the second call to `COMMIT` touches only rows the
/// transaction already holds, so it waits on no foreign lock. The queue-pause
/// sibling closes the window with a shared advisory lock held through commit.
/// This path deliberately does not, because:
///
/// 1. The lock would need a **new** two-argument advisory keyspace.
///    `queue_pause` owns that keyspace by a CI-enforced guard. Five subsystems
///    share the single-argument space, where a silent collision stalls the hot
///    claim path.
/// 2. It would impose a fleet-wide queue-before-activity lock-ordering rule on
///    two paths that take no lock today. That creates an ABBA hazard for the
///    first feature that touches both.
/// 3. The exposure is narrow by construction. It covers only activities that
///    opt into `schedule_to_start`, and only rows *already past* that deadline
///    at the instant of the pause. The next scanner tick would fail those rows
///    anyway. It also applies only inside the window above.
///
/// The complementary race is a *resume* that credits `scheduled_at` forward
/// while this enforcer holds the row. It needs no lock. `resume_activity` does
/// the pause delete and the credit in **one** transaction. So a statement here
/// sees either (held, uncredited) or (released, credited), never a torn view.
/// `schedule_to_start_still_expired_unlocked` also re-reads the credited
/// deadline before the enforcer trusts its scan snapshot.
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
/// Uses a **correlated** `COUNT(*)`, not a grouped join, for the same reason as
/// [`crate::queue_pause::list_paused_queues_query`]. The grouped form
/// aggregates the shard's entire `PENDING` backlog and only then joins the tiny
/// pause table. So its cost scales with unrelated pending work. That is
/// backwards for a diagnostic read used during the incident where the backlog
/// is largest. The correlated count runs once per *paused activity*. It returns
/// `0` for a paused activity that holds nothing, and does not drop the row.
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

/// One claimable task id **per distinct queue** for a just-resumed activity
/// (`$1` = name).
///
/// The ids are only the payload for the resume's wake notifications.
///
/// One row per queue, because the notify channel is per-queue. A single
/// activity type's held backlog can span several queues: a per-call
/// `queue_override` routes the same activity onto a priority queue beside its
/// default. A doorbell on the first queue only would leave every other queue's
/// released backlog to its workers' next poll tick. That is correct, but slow
/// during the recovery the resume drives.
///
/// One row *within* each queue, because the notification is a **doorbell**,
/// not a work assignment. [`crate::notify::QueueListener`] hands the payload
/// back to the poll loop, which re-polls that queue normally and drains it. The
/// predicate mirrors the shift's own due-row filter. So each id is a row this
/// resume made claimable, not a future-scheduled retry that is still not due.
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
/// Idempotent: a re-pause of an already-paused activity is a success no-op
/// with `newly_paused == false`. It keeps the **original** provenance, like the
/// `newly_terminated` / `newly_resumed` convention from #504 / #609 / #619.
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
    /// is recoverable. The resume response, and so the CLI output, carries it.
    /// An operator who closes out an incident then sees which hold was
    /// released. `None` on a no-op.
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
/// A single upsert, not `INSERT ... DO NOTHING` + a fallback `SELECT`.
/// `DO NOTHING` does not lock the conflicting row. A concurrent resume could
/// delete it between the two statements and leave the fallback with zero rows.
/// That is a `500`, and worse, an activity left *unpaused* for an operator who
/// was told it was held. `DO UPDATE` locks the row and always returns it.
///
/// The `DO UPDATE` deliberately writes the columns back to their **existing**
/// values. So an idempotent re-pause keeps the original reason, operator and
/// instant, and still returns the row. AC1: "pausing an already-paused type
/// returns success, not an error". `xmax = 0` distinguishes the insert from the
/// no-op update. A re-pause evaluates the `clock_timestamp()` in `VALUES` too,
/// and discards it. Only a fresh insert stamps it.
///
/// # Why `clock_timestamp()` and not `NOW()` (issue #807)
///
/// `NOW()` is `transaction_timestamp()`, fixed when the transaction
/// **starts**, whichever statement reads it. A pause takes effect for other
/// sessions only when this transaction **commits**. So each step after the
/// upsert widens the gap between the recorded `paused_at` and the true hold
/// start.
///
/// That gap is not cosmetic. The resume credit passes subtract from `paused_at`
/// as `$2`. A `paused_at` earlier than the true hold start credits back wait
/// the task spent **claimable and unheld**. That launders a
/// `schedule_to_start` budget the task already burned, which the pause must
/// not do. The statement most able to widen the gap is
/// [`held_task_count_query`]. Its cost scales with the backlog, which is
/// largest during the incident an operator pauses for.
///
/// Two halves fix it. This volatile clock is read when the upsert executes.
/// The count runs **before** the upsert (see [`pause_activity`]), so nothing
/// slow sits between the stamp and the commit.
///
/// The residual mirrors the resume side's, and is documented the same way. The
/// true freeze instant is commit time, which no in-statement expression can
/// know. So `paused_at` still precedes it by the upsert's own execution plus
/// the commit. It still errs toward over-crediting, but by microseconds, not by
/// an unbounded backlog scan. It never errs the other way. A stamp *later*
/// than the effective hold would under-credit and could time out a task that
/// was really held.
///
/// Binds: `$1` name, `$2` reason, `$3` operator, `$4` scope shard id.
#[must_use]
pub const fn pause_activity_upsert_query() -> &'static str {
    "INSERT INTO harvest_activity_pauses \
         (activity_name, reason, paused_by, paused_at, scope_shard_id) \
     VALUES ($1, $2, $3, clock_timestamp(), $4) \
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
/// each caller to remember. The claim gate matches exactly, so it can never
/// match a row with an unvalidated whitespace-padded name. The operator would
/// get a hold that *lists* as active while it holds nothing. The management API
/// still validates first, so a typo is a `400` before a connection is taken.
/// This check is the backstop, not a duplicate.
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
    use diesel_async::RunQueryDsl;

    let name = validate_activity_name(activity_name)?;
    let reason = reason.to_string();
    let paused_by = paused_by.to_string();

    // ONE transaction, matching `resume_activity`. The upsert and the held count
    // must commit or roll back together. Otherwise a failure of the *count*, a
    // best-effort observability read, inverts the reported outcome of a live
    // mutation. The pause row commits and the claim gate holds tasks fleet-wide.
    // But the operator sees a 500 and concludes that the hold did not take. That
    // is the worst failure mode for an incident brake. The count is also the
    // statement most likely to be slow on a large backlog.
    //
    // Pinned to READ COMMITTED for the same reason as `resume_activity`, and for
    // one specific to this path. The held count below is read *before* the
    // upsert. Under `REPEATABLE READ`, its snapshot would predate any wait on a
    // concurrent resume, so it could report a backlog that no longer exists.
    // Postgres can also abort the upsert with a serialization failure. That
    // occurs when `ON CONFLICT` lands on a row that a concurrent resume touched
    // after that snapshot. An idempotent re-pause then becomes a 500. This code
    // cannot see either hazard when the level comes from the database or the
    // role, so the level is emitted on the `BEGIN`.
    let mut tx = conn.build_transaction().read_committed();
    Box::pin(
        tx.run::<ActivityPauseOutcome, HarvestError, _>(async |conn| {
            let (name, reason, paused_by) = (name, reason, paused_by);

            // The count runs FIRST, before the row that records the hold (issue
            // #807). It reads only `harvest_task_queue`, never the pause row, so
            // its answer does not depend on the upsert. The hold is invisible to
            // other sessions until this transaction commits either way. So the
            // count is the same approximation in both orders.
            //
            // The order buys the accuracy of `paused_at`. The upsert stamps
            // `clock_timestamp()` (see `pause_activity_upsert_query` for why not
            // `NOW()`). The recorded instant is only as close to the true hold
            // start, the commit, as the work after it is short. The count is the
            // one statement here whose cost scales with the backlog, so it is
            // largest during the incident being paused for. With the count ahead
            // of the stamp, a slow count cannot back-date the hold. A back-dated
            // hold lets the resume credit back wait the tasks spent claimable and
            // unheld.
            //
            // It also shortens the hold on the pause row's lock. The upsert locks
            // it only across its own execution and the commit, not across an
            // unbounded scan. A concurrent pause or resume of the same activity
            // then queues behind it for less time.
            let held: CountRow = diesel::sql_query(held_task_count_query())
                .bind::<diesel::sql_types::Text, _>(&name)
                .get_result(conn)
                .await
                .map_err(crate::error::database_error)?;

            let row: UpsertedPauseRow = diesel::sql_query(pause_activity_upsert_query())
                .bind::<diesel::sql_types::Text, _>(&name)
                .bind::<diesel::sql_types::Text, _>(&reason)
                .bind::<diesel::sql_types::Text, _>(&paused_by)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(scope_shard_id)
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
/// `RETURNING` the provenance lets [`ActivityResumeOutcome`] echo back *which*
/// hold was released. The row is gone after this statement, so this is the last
/// moment the reason is recoverable.
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
/// Deletes the pause row and credits held time back to the released backlog's
/// `scheduled_at` (see [`resume_shift_scheduled_at_query`]). It rings the queue
/// doorbell so parked workers re-poll as soon as this commits. All of it runs
/// in **one transaction**, so a partially-applied resume is impossible. Either
/// the hold is lifted and the backlog is credited, or neither happens.
///
/// Idempotent: resuming an activity that is not paused is a success no-op with
/// `newly_resumed == false` and no shift.
///
/// Validates the name itself for the same reason as [`pause_activity`]. So the
/// two directions cannot disagree about what a valid name is.
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
    use diesel_async::RunQueryDsl;

    let name = validate_activity_name(activity_name)?;
    // Pinned to READ COMMITTED rather than inheriting the session default.
    //
    // Every statement below depends on taking a FRESH snapshot. The primary
    // shift, the doorbell and the late-arrivals credit exist to catch rows that
    // became visible after this transaction began. One is a task enqueued
    // during the hold. Another is a task enqueued before it whose insert only
    // committed just now. Under `REPEATABLE READ`, the whole transaction shares
    // the snapshot of its first statement, the `DELETE` below. Such a row is
    // then invisible to *every* later statement, and the late-arrivals pass is
    // structurally inert. That task keeps its entire held wait. The
    // `schedule_to_start` scanner can then fail it terminally the instant the
    // hold lifts. That is the exact outcome the credit exists to prevent.
    //
    // With an inherited level, a `default_transaction_isolation = repeatable
    // read` on the database or the role would silently disable that guarantee
    // from outside this code. `build_transaction().read_committed()` emits the
    // level on the `BEGIN` itself, so it travels with the query.
    // `queue_pause::resume_queue`, `queue::claim_task`, the timeout enforcer and
    // the scheduler all pin it for the same reason (issue #807). Pinned by
    // `resume_credits_a_late_arrival_under_a_repeatable_read_session_default`.
    let mut tx = conn.build_transaction().read_committed();
    Box::pin(
        tx.run::<ActivityResumeOutcome, HarvestError, _>(async |conn| {
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

            // Ring the doorbell INSIDE the transaction. The sender sends the
            // wake only after COMMIT (issue #1796), so listeners wake exactly
            // when the hold lifts. An earlier wake would still see the
            // uncommitted pause row and skip the task. A rollback sends no
            // wake.
            //
            // Ordered BEFORE the late-arrivals credit pass, so that pass is the
            // last statement this transaction runs (issue #807). Whatever runs
            // after the final credit bounds the uncredited residual. This
            // notification pass is the expensive tail: one round-trip per queue
            // that holds backlog. Ringing first shrinks that residual from "the
            // whole notification pass" to "one small UPDATE". That is what
            // `resume_shift_late_arrivals_query` documents. It did not hold with
            // the reverse order.
            //
            // The trade is asymmetric, and that settles the order. A row that
            // commits after this SELECT gets no doorbell of its own. It waits one
            // poll tick, because workers still poll: a latency cost. The same row
            // without its *credit* keeps its whole held wait. The
            // `schedule_to_start` scanner can then fail it terminally the instant
            // the hold lifts. Latency is recoverable; a terminal failure is not.
            // The doorbell also rarely misses anything. It is per-queue, so a
            // late arrival is silent only when it is the sole due row on its
            // queue.
            let notify: Vec<NotifyTaskRow> =
                diesel::sql_query(resumed_activity_notify_task_query())
                    .bind::<diesel::sql_types::Text, _>(&name)
                    .load(conn)
                    .await
                    .map_err(crate::error::database_error)?;
            for row in notify {
                crate::notify::notify_task_enqueued(conn, &row.queue_name, row.id).await?;
            }

            // Final statement of the transaction. The `DELETE` above has not
            // committed, so every other session still sees the pause row. A task
            // still held may be invisible to the primary shift above. It may be
            // created during the hold, or before it by an enqueue that only
            // committed just now. This pass covers every due row that the
            // primary pass did not report shifting. So the two are disjoint by
            // construction, and no row is credited twice. See
            // `resume_shift_late_arrivals_query` for the irreducible residual
            // this narrows but cannot close.
            let late: usize = diesel::sql_query(resume_shift_late_arrivals_query())
                .bind::<diesel::sql_types::Text, _>(&name)
                .bind::<diesel::sql_types::Timestamptz, _>(pause.paused_at)
                .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(&shifted_ids)
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;

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
/// Deliberately narrower and cheaper than [`list_paused_activities`]. That
/// function also runs a correlated `COUNT(*)` over `harvest_task_queue` per
/// paused activity to report `held_task_count`. The eligibility explainer
/// (issue #807) needs only the name *set* to decide whether a task is held. It
/// runs during the incident where that backlog, and so the cost of counting it,
/// is largest. Exact mirror of [`crate::queue_pause::paused_queue_names`],
/// which exists for the same reason.
///
/// Unfiltered, matching the `paused_activities` CTE in `queue::claim_task`. An
/// activity pause is fleet-wide across queues on this shard, so there is no
/// natural bound to pre-filter by. The table holds one row per *currently
/// paused* activity: none in steady state, a handful during an incident.
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

    /// A **workflow** task can carry a non-NULL `activity_name`: the engine
    /// stamps the `'mixed_signal_suspension'` sentinel there (see
    /// `queue::claim_task`'s wake handling). So "workflow rows have a NULL
    /// `activity_name`" is a coincidence, not an invariant. An anti-join that
    /// relied on it would hold every mixed-signal-suspended workflow task
    /// fleet-wide once an operator paused an activity with the sentinel's name.
    /// Scoping by `task_type` makes the guarantee structural.
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

    /// A single activity's held tasks can sit on **several** queues. A per-call
    /// `queue_override` puts the same activity type on a priority queue beside
    /// its default one. The notify channel is per-queue. So a resume must ring
    /// one doorbell per distinct queue it released work on. A single `LIMIT 1`
    /// row would wake one queue's workers and leave the other backlogs until
    /// their next poll tick.
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

    /// The enforcer's authoritative re-check must be a *single* statement. It
    /// then takes a fresh `READ COMMITTED` snapshot and sees a pause that
    /// committed after the scan predicate ran. A multi-statement or join-based
    /// form would bring back the staleness it exists to close.
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
    /// no) activity. The rule and its reason match
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
    /// resolves against `harvest_activity_pauses` first. The predicate then
    /// becomes `ap.activity_name = ap.activity_name`, which is always true. One
    /// pause would then hold *every* activity.
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

    /// The Rust re-check in `timeout::enforce_activity_timeout` compares
    /// `task.task_type` against [`ACTIVITY_TASK_TYPE`]. The SQL renderers embed
    /// it as a literal. If the two disagree, the enforcer silently stops
    /// matching its own scan predicate. That reopens the sentinel hazard through
    /// the back door, so this test pins that they agree.
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

    /// `NOW()` is frozen at transaction start, so it omits the resume's own
    /// duration from the credit. The backlog would time out the instant it thaws.
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

    /// `NOW()` is `transaction_timestamp()`, frozen at transaction start. So it
    /// records a hold that began before the transaction did any of its work.
    /// The resume subtracts `paused_at`, so it credits that gap back as held
    /// time. See the query's own docs for the full argument.
    #[test]
    fn pause_upsert_stamps_the_volatile_clock_not_the_frozen_transaction_clock() {
        let sql = pause_activity_upsert_query();
        assert!(sql.contains("clock_timestamp()"));
        assert!(
            !sql.contains("NOW()"),
            "a frozen NOW() back-dates the hold to transaction start, letting \
             the resume credit back wait the tasks spent unheld; got:\n{sql}"
        );
    }

    /// The read must still list a paused activity that holds nothing. A
    /// correlated count returns `0` where an inner join drops the row.
    #[test]
    fn list_query_uses_a_correlated_count_so_an_empty_hold_still_lists() {
        let sql = list_paused_activities_query();
        assert!(sql.contains("(SELECT COUNT(*) FROM harvest_task_queue t"));
        assert!(!sql.contains("GROUP BY"));
        assert!(sql.contains("FROM harvest_activity_pauses p"));
    }

    /// The doorbell is per-queue, so the notify lookup must carry the queue of
    /// the woken task. An activity's backlog can span queues.
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
        assert!(!activity_pause_suppresses_timeout(
            &TimeoutReason::ScheduleToStart,
            false
        ));
    }
}
