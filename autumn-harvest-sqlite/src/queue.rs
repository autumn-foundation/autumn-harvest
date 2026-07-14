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
}

fn next_task_seq(conn: &Connection) -> SqliteResult<i64> {
    let max: Option<i64> =
        conn.query_row("SELECT MAX(seq) FROM harvest_tasks", [], |row| row.get(0))?;
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
pub fn enqueue_activity(
    conn: &Connection,
    exec_id: ExecutionId,
    activity_id: ActivityExecId,
    name: &str,
    input: &Value,
    queue: &str,
    run_at: i64,
) -> SqliteResult<()> {
    let seq = next_task_seq(conn)?;
    conn.execute(
        "INSERT INTO harvest_tasks \
         (task_id, exec_id, activity_id, name, input_json, queue, state, attempt, run_at, seq) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'PENDING', 0, ?7, ?8)",
        params![
            uuid::Uuid::new_v4().to_string(),
            exec_id.to_string(),
            activity_id.to_string(),
            name,
            serde_json::to_string(input)?,
            queue,
            run_at,
            seq,
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
            "SELECT task_id, activity_id, name, input_json, attempt \
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
                ))
            },
        )
        .optional()?;

    let Some((task_id, act_s, name, input_s, attempt)) = row else {
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
    }))
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
    conn.execute(
        "INSERT OR REPLACE INTO harvest_timers (timer_id, exec_id, fire_at, fired) \
         VALUES (?1, ?2, ?3, 0)",
        params![timer_id, exec_id.to_string(), fire_at],
    )?;
    Ok(())
}

/// Return the ids of all unfired timers for `exec_id` whose deadline has passed.
pub fn due_timers(conn: &Connection, exec_id: ExecutionId, now: i64) -> SqliteResult<Vec<String>> {
    let mut stmt = conn.prepare_cached(
        "SELECT timer_id FROM harvest_timers \
         WHERE exec_id = ?1 AND fired = 0 AND fire_at <= ?2 ORDER BY fire_at",
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
