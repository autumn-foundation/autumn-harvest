//! Activity task queue and durable timers.
//!
//! # Single-writer is load-bearing (replaces `FOR UPDATE SKIP LOCKED`)
//!
//! The Postgres backend claims work with `SELECT ... FOR UPDATE SKIP LOCKED` so
//! many concurrent worker processes can pull disjoint rows without blocking each
//! other. `SQLite` has no row-level locking and no `SKIP LOCKED`; instead this
//! backend assumes a **single writer process**. A claim is a `BEGIN IMMEDIATE`
//! transaction (which takes `SQLite`'s database-level write lock up front) that
//! `SELECT`s the oldest ready `PENDING` row and flips it to `RUNNING`, then
//! `COMMIT`s. Under the single-writer assumption this is exactly-once by
//! construction — no two claimers ever race. **Multi-writer `SQLite` is explicitly
//! out of scope**; supporting it would need an external lease or a
//! `busy_timeout`/retry protocol layered on the `BEGIN IMMEDIATE` claim, which is
//! precisely the complexity the edge/local-first use case avoids.
//!
//! # Polling replaces `LISTEN`/`NOTIFY`
//!
//! The Postgres backend wakes idle workers with `LISTEN`/`NOTIFY`. `SQLite` has no
//! push notification, so the driver ([`SqliteRuntime`](crate::SqliteRuntime))
//! **polls**: it drains all currently-ready tasks and due timers, re-runs the
//! workflow, and repeats until the run reaches a terminal state or blocks on an
//! external input. A production edge runtime would wrap this in a
//! sleep-and-repoll loop; the tests drive the poll explicitly (and advance a
//! virtual clock) so they never sleep.
//!
//! # Orphan reclaim (single-server crash recovery)
//!
//! Because there is one server, any task left `RUNNING` when the process starts
//! is an orphan from a crash — the previous process claimed it, may have run the
//! body, but did not commit the terminal finalize (see the atomicity discussion
//! in [`worker`](crate::worker)). [`reclaim_orphaned_running`] flips every such
//! row back to `PENDING` on [`SqliteRuntime::open`](crate::SqliteRuntime::open)
//! so the body re-runs. This makes activity execution **at-least-once**.

use autumn_harvest::{ActivityExecId, ExecutionId};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::Value;

use crate::error::{SqliteError, SqliteResult};

/// A claimed activity task ready to run.
pub struct ClaimedTask {
    pub task_id: String,
    pub activity_id: ActivityExecId,
    pub name: String,
    pub input: Value,
    /// Attempts already consumed (0 before the first run).
    pub attempt: u32,
    /// Per-call retry cap carried from the scheduling command's
    /// `retry_policy_override` (issue #1069 P2). `Some` when the workflow used the
    /// typed `execute_activity`/`execute_activity_with_opts` path (which resolves
    /// the activity's declared/per-call `RetryPolicy`); `None` for the raw
    /// `execute_activity_raw` path, in which case the worker falls back to the
    /// registered [`ActivitySpec`](crate::runtime::ActivitySpec)'s default.
    pub max_attempts: Option<u32>,
}

fn next_task_seq(conn: &Connection) -> SqliteResult<i64> {
    let max: Option<i64> =
        conn.query_row("SELECT MAX(seq) FROM harvest_tasks", [], |row| row.get(0))?;
    Ok(max.map_or(0, |m| m + 1))
}

/// The next monotonic timer arm sequence (the deterministic same-`fire_at`
/// tie-breaker, issue #1069 P2). Computed as `MAX(arm_seq) + 1` over ALL timer
/// rows so it is globally monotonic within the database — two timers armed in the
/// same [`apply_commands`](crate::runtime) transaction (e.g.
/// `tokio::join!(ctx.timer("a", 1), ctx.timer("b", 1))`) get consecutive values in
/// command order, because the first `enqueue_timer` commits its row before the
/// second reads `MAX`. Robust to `VACUUM` (unlike an implicit `rowid`, which
/// `VACUUM` may renumber).
fn next_timer_arm_seq(conn: &Connection) -> SqliteResult<i64> {
    let max: Option<i64> =
        conn.query_row("SELECT MAX(arm_seq) FROM harvest_timers", [], |row| {
            row.get(0)
        })?;
    Ok(max.map_or(0, |m| m + 1))
}

/// Reclaim tasks stranded `RUNNING` by a crash, flipping them back to `PENDING`.
///
/// Safe because this backend is single-server: any `RUNNING` row observed at
/// startup was claimed by a process that has since exited without finalizing.
/// Returns the number of rows reclaimed.
pub fn reclaim_orphaned_running(conn: &Connection) -> SqliteResult<usize> {
    let n = conn.execute(
        "UPDATE harvest_tasks SET state = 'PENDING' WHERE state = 'RUNNING'",
        [],
    )?;
    Ok(n)
}

/// Enqueue a fresh activity task in the `PENDING` state.
///
/// `max_attempts` is the per-call retry cap from the scheduling command's
/// `retry_policy_override` (issue #1069 P2): `Some(n)` honors the workflow's
/// declared/per-call [`RetryPolicy`](autumn_harvest::policy::RetryPolicy), `None`
/// leaves the worker to fall back to the registered `ActivitySpec` default.
#[allow(clippy::too_many_arguments)]
pub fn enqueue_activity(
    conn: &Connection,
    exec_id: ExecutionId,
    activity_id: ActivityExecId,
    name: &str,
    input: &Value,
    queue: &str,
    run_at: i64,
    max_attempts: Option<u32>,
) -> SqliteResult<()> {
    let seq = next_task_seq(conn)?;
    conn.execute(
        "INSERT INTO harvest_tasks \
         (task_id, exec_id, activity_id, name, input_json, queue, state, attempt, run_at, seq, \
          max_attempts) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'PENDING', 0, ?7, ?8, ?9)",
        params![
            uuid::Uuid::new_v4().to_string(),
            exec_id.to_string(),
            activity_id.to_string(),
            name,
            serde_json::to_string(input)?,
            queue,
            run_at,
            seq,
            max_attempts.map(i64::from),
        ],
    )?;
    Ok(())
}

/// Claim the oldest ready (`PENDING`, `run_at <= now`) task for `exec_id`,
/// flipping it to `RUNNING` inside a `BEGIN IMMEDIATE` transaction. See the
/// module docs for why this replaces `SKIP LOCKED`.
pub fn claim_next_ready_task(
    conn: &mut Connection,
    exec_id: ExecutionId,
    now: i64,
) -> SqliteResult<Option<ClaimedTask>> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let row = tx
        .query_row(
            "SELECT task_id, activity_id, name, input_json, attempt, max_attempts \
             FROM harvest_tasks WHERE state = 'PENDING' AND exec_id = ?1 AND run_at <= ?2 \
             ORDER BY seq LIMIT 1",
            params![exec_id.to_string(), now],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                ))
            },
        )
        .optional()?;

    let Some((task_id, act_s, name, input_s, attempt, max_attempts)) = row else {
        tx.commit()?;
        return Ok(None);
    };

    // Parse the fallible fields BEFORE mutating: a `?`-return here drops `tx`
    // un-committed, rolling back the `BEGIN IMMEDIATE` transaction so the task
    // stays `PENDING` and can be re-claimed — a mutate-then-parse ordering would
    // strand it `RUNNING` on a corrupt-row error.
    let activity_id = act_s
        .parse()
        .map_err(|_| SqliteError::corrupt("activity_id"))?;
    let input: Value = serde_json::from_str(&input_s)?;

    tx.execute(
        "UPDATE harvest_tasks SET state = 'RUNNING' WHERE task_id = ?1",
        params![task_id],
    )?;
    tx.commit()?;

    Ok(Some(ClaimedTask {
        task_id,
        activity_id,
        name,
        input,
        attempt: u32::try_from(attempt).unwrap_or(0),
        max_attempts: max_attempts.and_then(|m| u32::try_from(m).ok()),
    }))
}

/// Release a just-claimed (`RUNNING`) task back to `PENDING` **without** consuming
/// an attempt or advancing `run_at` — the body never ran.
///
/// The claim-release primitive for the unregistered-activity path (Codex #1069
/// P2). [`claim_next_ready_task`] commits a task to `RUNNING` *before* the caller
/// resolves the handler; if the activity name has no registered body, leaving the
/// row `RUNNING` would strand it (invisible to a later drain, which only re-claims
/// `PENDING` rows) until a full DB close+reopen ran [`reclaim_orphaned_running`].
/// Releasing it here lets a later drain re-claim it once the body is registered,
/// with no reopen. Scoped to one task and guarded on `state = 'RUNNING'` so it is a
/// no-op against an already-finalized row; mirrors the shape of
/// [`reclaim_orphaned_running`].
pub fn release_claim(conn: &Connection, task_id: &str) -> SqliteResult<()> {
    conn.execute(
        "UPDATE harvest_tasks SET state = 'PENDING' WHERE task_id = ?1 AND state = 'RUNNING'",
        params![task_id],
    )?;
    Ok(())
}

/// Mark a task terminally done (success or exhausted retries).
pub fn finish_task(conn: &Connection, task_id: &str) -> SqliteResult<()> {
    conn.execute(
        "UPDATE harvest_tasks SET state = 'DONE' WHERE task_id = ?1",
        params![task_id],
    )?;
    Ok(())
}

/// The stored `state` of a task by id (used by the atomicity tests).
#[cfg(test)]
pub fn task_state(conn: &Connection, task_id: &str) -> SqliteResult<Option<String>> {
    Ok(conn
        .query_row(
            "SELECT state FROM harvest_tasks WHERE task_id = ?1",
            params![task_id],
            |row| row.get(0),
        )
        .optional()?)
}

/// Requeue a task for another attempt at `run_at`, recording the consumed attempt.
pub fn requeue_task(
    conn: &Connection,
    task_id: &str,
    attempt: u32,
    run_at: i64,
) -> SqliteResult<()> {
    conn.execute(
        "UPDATE harvest_tasks SET state = 'PENDING', attempt = ?2, run_at = ?3 WHERE task_id = ?1",
        params![task_id, i64::from(attempt), run_at],
    )?;
    Ok(())
}

/// Cancel a still-open (`PENDING`/`RUNNING`) activity task by its `activity_id`,
/// flipping it to `CANCELLED`. Returns `true` iff a row was actually cancelled.
///
/// The loser-cancellation primitive for a losing activity branch of a resolved
/// `ctx.race()` (issue #600), mirroring the Postgres `queue::cancel_activity_task`.
/// An activity that
/// genuinely completed first (a real completion raced the cancellation) is
/// already `DONE`, matches nothing, and returns `false` — so its real terminal
/// event is never shadowed by a synthetic one. A `CANCELLED` row is terminal for
/// this queue: [`claim_next_ready_task`] only selects `state = 'PENDING'`, so a
/// cancelled loser is never re-run.
pub fn cancel_activity_task(conn: &Connection, activity_id: ActivityExecId) -> SqliteResult<bool> {
    let n = conn.execute(
        "UPDATE harvest_tasks SET state = 'CANCELLED' \
         WHERE activity_id = ?1 AND state IN ('PENDING', 'RUNNING')",
        params![activity_id.to_string()],
    )?;
    Ok(n > 0)
}

// ── Durable timers ───────────────────────────────────────────────────────────

pub fn enqueue_timer(
    conn: &Connection,
    exec_id: ExecutionId,
    timer_id: &str,
    fire_at: i64,
) -> SqliteResult<()> {
    // `INSERT OR REPLACE` (not `OR IGNORE`) so a timer id can be RE-ARMED under the
    // same name after a prior fire (the poll-loop idiom). The caller
    // (`apply_commands`) only reaches this for a genuinely-new arm — occurrence
    // idempotency (`store::pending_timer_arms == 0`) guarantees no *pending* arm of
    // this id exists — so any conflicting `(exec_id, timer_id)` row is a spent
    // (`fired = 1`) row from a previous arm that is safe to supersede with the
    // fresh unfired arm. `OR IGNORE` would instead silently keep the spent row and
    // wedge the re-arm at `Stuck`.
    // Assign a fresh monotonic `arm_seq` so this arm sorts AFTER every timer armed
    // earlier (issue #1069 P2). A re-arm of the same id (the poll-loop idiom) is an
    // `INSERT OR REPLACE`, which deletes the spent row and inserts fresh — so it
    // correctly gets a NEW, higher `arm_seq`, sorting after any concurrently-armed
    // sibling as its re-arm implies.
    let arm_seq = next_timer_arm_seq(conn)?;
    conn.execute(
        "INSERT OR REPLACE INTO harvest_timers (timer_id, exec_id, fire_at, fired, arm_seq) \
         VALUES (?1, ?2, ?3, 0, ?4)",
        params![timer_id, exec_id.to_string(), fire_at, arm_seq],
    )?;
    Ok(())
}

/// Return the ids of all unfired timers for `exec_id` whose deadline has passed.
pub fn due_timers(conn: &Connection, exec_id: ExecutionId, now: i64) -> SqliteResult<Vec<String>> {
    // `ORDER BY fire_at, arm_seq`: the secondary `arm_seq` key makes the fire order
    // of equal-deadline timers deterministic and equal to their `TimerStarted`
    // append order, which the core matcher requires (issue #1069 P2).
    let mut stmt = conn.prepare_cached(
        "SELECT timer_id FROM harvest_timers \
         WHERE exec_id = ?1 AND fired = 0 AND fire_at <= ?2 ORDER BY fire_at, arm_seq",
    )?;
    let rows = stmt.query_map(params![exec_id.to_string(), now], |row| {
        row.get::<_, String>(0)
    })?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

/// Return the ids of all unfired timers for `exec_id` that are **both** due
/// (`fire_at <= now`) **and** whose deadline is at or before `received_at`,
/// oldest deadline first.
///
/// The "fire this timer BEFORE that signal" half of the wake-event ordering
/// (issue #476). A `wait_for_signal_timeout` deadline that expired at or before a
/// signal arrived (`fire_at <= received_at`) must be recorded as `TimerFired`
/// ahead of the `SignalReceived`, so a late signal cannot retroactively flip the
/// race to the approval branch. Ties (`fire_at == received_at`) go to the timer,
/// mirroring the Postgres `merge_wake_events` rule (signal-first iff
/// `received_at < fires_at`). A due timer whose deadline is *after* the signal
/// arrived is deliberately excluded here — it is fired *after* the signal by the
/// ordinary [`drain_ready`](crate::worker::drain_ready) pass.
pub fn due_timers_before_signal(
    conn: &Connection,
    exec_id: ExecutionId,
    now: i64,
    received_at: i64,
) -> SqliteResult<Vec<String>> {
    let mut stmt = conn.prepare_cached(
        "SELECT timer_id FROM harvest_timers \
         WHERE exec_id = ?1 AND fired = 0 AND fire_at <= ?2 AND fire_at <= ?3 \
         ORDER BY fire_at, arm_seq",
    )?;
    let rows = stmt.query_map(params![exec_id.to_string(), now, received_at], |row| {
        row.get::<_, String>(0)
    })?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

/// True if `exec_id` has any armed (unfired) durable timer — the ground-truth
/// signal a no-progress cycle is genuinely waiting on a not-yet-due timer.
pub fn has_unfired_timer(conn: &Connection, exec_id: ExecutionId) -> SqliteResult<bool> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM harvest_timers WHERE exec_id = ?1 AND fired = 0",
        params![exec_id.to_string()],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

pub fn mark_timer_fired(
    conn: &Connection,
    exec_id: ExecutionId,
    timer_id: &str,
) -> SqliteResult<()> {
    conn.execute(
        "UPDATE harvest_timers SET fired = 1 WHERE exec_id = ?1 AND timer_id = ?2",
        params![exec_id.to_string(), timer_id],
    )?;
    Ok(())
}

/// Delete a single still-pending (`fired = 0`) durable timer row by its
/// `timer_id`. A no-op if the timer already fired or does not exist.
///
/// The loser-cancellation primitive for a losing timer branch of a resolved race
/// — `ctx.race()` (issue #600) and, in particular, the deadline timer of a
/// `wait_for_signal_timeout` / `receive_signal_timeout` whose **signal wins**
/// (issue #476). Mirrors the Postgres `queue::delete_pending_timer`. Deleting
/// the unfired row is what keeps the completed workflow from being pinned by a
/// stray armed timer (and from a stray later `TimerFired`).
///
/// Returns `true` iff a row was actually removed. This lets the (idempotent)
/// re-push of the same teardown on every subsequent replay cycle report *no*
/// progress once the row is gone, so the decision loop still converges instead
/// of spinning on a repeated no-op delete.
pub fn delete_pending_timer(
    conn: &Connection,
    exec_id: ExecutionId,
    timer_id: &str,
) -> SqliteResult<bool> {
    let n = conn.execute(
        "DELETE FROM harvest_timers WHERE exec_id = ?1 AND timer_id = ?2 AND fired = 0",
        params![exec_id.to_string(), timer_id],
    )?;
    Ok(n > 0)
}

#[cfg(test)]
mod tests {
    use rusqlite::{Connection, params};

    use autumn_harvest::ExecutionId;

    use super::{due_timers, due_timers_before_signal, enqueue_timer};
    use crate::schema;

    fn open() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(schema::SCHEMA).unwrap();
        conn
    }

    // FINDING 1 (Codex #1069 P2): the deterministic `arm_seq` tie-break makes the
    // fire order of equal-`fire_at` timers well-defined and equal to arm order.
    // (SQLite's `ORDER BY fire_at`-only tie order is *unspecified* — it may happen
    // to coincide with arm order or not — so the hard-falsifiable proof that this
    // order is load-bearing lives in the end-to-end reversed-history control in
    // `tests/timer_arm_order.rs`; here we pin the guarantee.) Rows are inserted so
    // arm order (a, z) is the reverse of insertion (rowid) order (z, a).
    #[test]
    fn due_timers_break_equal_deadlines_by_arm_seq() {
        let conn = open();
        let exec = ExecutionId::new();
        conn.execute(
            "INSERT INTO harvest_timers (timer_id, exec_id, fire_at, fired, arm_seq) \
             VALUES ('z', ?1, 100, 0, 1)",
            params![exec.to_string()],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO harvest_timers (timer_id, exec_id, fire_at, fired, arm_seq) \
             VALUES ('a', ?1, 100, 0, 0)",
            params![exec.to_string()],
        )
        .unwrap();

        assert_eq!(
            due_timers(&conn, exec, 200).unwrap(),
            vec!["a".to_string(), "z".to_string()],
            "equal-deadline timers must fire in arm_seq order"
        );
        // The signal-interleave query carries the same tie-break.
        assert_eq!(
            due_timers_before_signal(&conn, exec, 200, 200).unwrap(),
            vec!["a".to_string(), "z".to_string()],
            "due_timers_before_signal must apply the same arm_seq tie-break"
        );
    }

    // `enqueue_timer` assigns a strictly increasing `arm_seq` in call order, so two
    // timers armed in one batch (the `join!` case) fire in `TimerStarted`-append
    // order — and a re-arm of the same id (INSERT OR REPLACE) gets a fresh, higher
    // `arm_seq` rather than keeping the spent row's value.
    #[test]
    fn enqueue_timer_assigns_monotonic_arm_seq_including_on_rearm() {
        let conn = open();
        let exec = ExecutionId::new();
        enqueue_timer(&conn, exec, "a", 100).unwrap();
        enqueue_timer(&conn, exec, "b", 100).unwrap();
        let seq = |id: &str| -> i64 {
            conn.query_row(
                "SELECT arm_seq FROM harvest_timers WHERE exec_id = ?1 AND timer_id = ?2",
                params![exec.to_string(), id],
                |r| r.get(0),
            )
            .unwrap()
        };
        assert!(seq("a") < seq("b"), "a armed before b sorts first");
        // Both share fire_at=100, so due order follows arm order.
        assert_eq!(
            due_timers(&conn, exec, 200).unwrap(),
            vec!["a".to_string(), "b".to_string()],
        );

        // Re-arm "a" (the poll-loop idiom) — it must get a fresh arm_seq ABOVE b's.
        enqueue_timer(&conn, exec, "a", 100).unwrap();
        assert!(
            seq("a") > seq("b"),
            "a re-armed after b must get a fresh higher arm_seq, not keep its stale one"
        );
    }
}
