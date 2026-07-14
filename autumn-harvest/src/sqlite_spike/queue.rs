//! Activity task queue and durable timers for the spike.
//!
//! # Single-writer is load-bearing (replaces `FOR UPDATE SKIP LOCKED`)
//!
//! The Postgres backend claims work with `SELECT ... FOR UPDATE SKIP LOCKED` so
//! that many concurrent worker processes can pull disjoint rows without blocking
//! each other. `SQLite` has no row-level locking and no `SKIP LOCKED`; instead this
//! spike assumes a **single writer process**. A claim is a `BEGIN IMMEDIATE`
//! transaction (which takes `SQLite`'s database-level write lock up front) that
//! `SELECT`s the oldest ready `PENDING` row and flips it to `RUNNING`, then
//! `COMMIT`s. Under the single-writer assumption this is exactly-once by
//! construction — no two claimers ever race. **Multi-writer `SQLite` is explicitly
//! out of scope for the spike**; supporting it would require an external lease or
//! a `busy_timeout`/retry protocol layered on the `BEGIN IMMEDIATE` claim, which
//! is precisely the complexity the edge/local-first use case is trying to avoid.
//!
//! # Polling replaces `LISTEN`/`NOTIFY`
//!
//! The Postgres backend wakes idle workers with `LISTEN`/`NOTIFY`. `SQLite` has no
//! push notification, so the driver ([`super::SqliteRuntime::run_until_blocked`])
//! **polls**: it drains all currently-ready tasks and due timers, re-runs the
//! workflow, and repeats until the run reaches a terminal state or blocks on an
//! external input (a not-yet-due timer or an undelivered signal). A production
//! edge runtime would wrap this in a sleep-and-repoll loop; the tests drive the
//! poll explicitly (and advance a virtual clock) so they never sleep.

use rusqlite::{Connection, TransactionBehavior, params};
use serde_json::Value;

use super::SpikeError;
use crate::types::{ActivityExecId, ExecutionId};

/// A claimed activity task ready to run.
pub(super) struct ClaimedTask {
    pub task_id: String,
    pub activity_id: ActivityExecId,
    pub name: String,
    pub input: Value,
    /// Attempts already consumed (0 before the first run).
    pub attempt: u32,
}

fn next_task_seq(conn: &Connection) -> Result<i64, SpikeError> {
    let max: Option<i64> =
        conn.query_row("SELECT MAX(seq) FROM spike_tasks", [], |row| row.get(0))?;
    Ok(max.map_or(0, |m| m + 1))
}

/// Enqueue a fresh activity task in the `PENDING` state.
pub(super) fn enqueue_activity(
    conn: &Connection,
    exec_id: ExecutionId,
    activity_id: ActivityExecId,
    name: &str,
    input: &Value,
    queue: &str,
    run_at: i64,
) -> Result<(), SpikeError> {
    let seq = next_task_seq(conn)?;
    conn.execute(
        "INSERT INTO spike_tasks \
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
pub(super) fn claim_next_ready_task(
    conn: &mut Connection,
    exec_id: ExecutionId,
    now: i64,
) -> Result<Option<ClaimedTask>, SpikeError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let row = tx
        .query_row(
            "SELECT task_id, exec_id, activity_id, name, input_json, queue, attempt \
             FROM spike_tasks WHERE state = 'PENDING' AND exec_id = ?1 AND run_at <= ?2 \
             ORDER BY seq LIMIT 1",
            params![exec_id.to_string(), now],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, i64>(6)?,
                ))
            },
        )
        .optional_spike()?;

    let Some((task_id, _exec_s, act_s, name, input_s, _queue, attempt)) = row else {
        tx.commit()?;
        return Ok(None);
    };

    tx.execute(
        "UPDATE spike_tasks SET state = 'RUNNING' WHERE task_id = ?1",
        params![task_id],
    )?;
    tx.commit()?;

    Ok(Some(ClaimedTask {
        task_id,
        activity_id: act_s
            .parse()
            .map_err(|_| SpikeError::corrupt("activity_id"))?,
        name,
        input: serde_json::from_str(&input_s)?,
        attempt: u32::try_from(attempt).unwrap_or(0),
    }))
}

/// Mark a task terminally done (success or exhausted retries).
pub(super) fn finish_task(conn: &Connection, task_id: &str) -> Result<(), SpikeError> {
    conn.execute(
        "UPDATE spike_tasks SET state = 'DONE' WHERE task_id = ?1",
        params![task_id],
    )?;
    Ok(())
}

/// Requeue a task for another attempt at `run_at`, recording the consumed attempt.
pub(super) fn requeue_task(
    conn: &Connection,
    task_id: &str,
    attempt: u32,
    run_at: i64,
) -> Result<(), SpikeError> {
    conn.execute(
        "UPDATE spike_tasks SET state = 'PENDING', attempt = ?2, run_at = ?3 WHERE task_id = ?1",
        params![task_id, i64::from(attempt), run_at],
    )?;
    Ok(())
}

// ── Durable timers ───────────────────────────────────────────────────────────

pub(super) fn enqueue_timer(
    conn: &Connection,
    exec_id: ExecutionId,
    timer_id: &str,
    fire_at: i64,
) -> Result<(), SpikeError> {
    conn.execute(
        "INSERT OR IGNORE INTO spike_timers (timer_id, exec_id, fire_at, fired) VALUES (?1, ?2, ?3, 0)",
        params![timer_id, exec_id.to_string(), fire_at],
    )?;
    Ok(())
}

/// Return the ids of all unfired timers for `exec_id` whose deadline has passed.
pub(super) fn due_timers(
    conn: &Connection,
    exec_id: ExecutionId,
    now: i64,
) -> Result<Vec<String>, SpikeError> {
    let mut stmt = conn.prepare(
        "SELECT timer_id FROM spike_timers \
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

pub(super) fn mark_timer_fired(
    conn: &Connection,
    exec_id: ExecutionId,
    timer_id: &str,
) -> Result<(), SpikeError> {
    conn.execute(
        "UPDATE spike_timers SET fired = 1 WHERE exec_id = ?1 AND timer_id = ?2",
        params![exec_id.to_string(), timer_id],
    )?;
    Ok(())
}

/// `.optional()` for a `query_row` that maps into a `Result<T, SpikeError>`.
///
/// `rusqlite`'s own `OptionalExtension` only works on `rusqlite::Result`; the
/// closure above already returns a `rusqlite::Result`, so we translate the
/// `QueryReturnedNoRows` sentinel here to keep the claim path readable.
trait OptionalSpike<T> {
    fn optional_spike(self) -> Result<Option<T>, SpikeError>;
}

impl<T> OptionalSpike<T> for rusqlite::Result<T> {
    fn optional_spike(self) -> Result<Option<T>, SpikeError> {
        match self {
            Ok(v) => Ok(Some(v)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(SpikeError::from(e)),
        }
    }
}
