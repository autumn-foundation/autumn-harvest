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
//! The same single-writer `BEGIN…COMMIT` does double duty: besides substituting
//! for `SKIP LOCKED`, it gives **atomic multi-row persistence**. A decision
//! cycle's event append and its paired task/timer row insert
//! ([`super::SqliteRuntime::apply_commands`]), and a drained activity's terminal
//! event + task-state flip ([`super::worker::drain_ready`]), each commit in one
//! transaction — so a crash never leaves an event without its row, matching the
//! Postgres engine, which persists an event and its task-queue row in a single
//! transaction.
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

    // Parse the fallible fields BEFORE mutating: a `?`-return here drops `tx`
    // un-committed, rolling back the `BEGIN IMMEDIATE` transaction so the task
    // stays `PENDING` and can be re-claimed — a mutate-then-parse ordering would
    // strand it `RUNNING` on a corrupt-row error.
    let activity_id = act_s
        .parse()
        .map_err(|_| SpikeError::corrupt("activity_id"))?;
    let input: Value = serde_json::from_str(&input_s)?;

    tx.execute(
        "UPDATE spike_tasks SET state = 'RUNNING' WHERE task_id = ?1",
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

/// Return a claimed (`RUNNING`) task to `PENDING` **without** touching its
/// `attempt` or `run_at`, so a subsequent drain re-claims and re-runs it exactly
/// as it stood before the claim.
///
/// The claim ([`claim_next_ready_task`]) commits the `RUNNING` flip in its own
/// `BEGIN IMMEDIATE` transaction (the activity body runs between the claim and the
/// post-body persistence, so the two cannot share one transaction — a `SQLite`
/// write lock can't be held across arbitrary user work). Any fallible step
/// *after* that claim — the handler lookup for an unregistered activity, or a
/// transient post-body persistence error — would otherwise strand the task
/// `RUNNING` un-reclaimable, since later claims select only `PENDING`. The worker
/// pass ([`super::worker::drain_ready`]) calls this on **any** such error so the
/// wedge is recoverable: register the missing handler / let the fault clear,
/// re-run, and the task drains. (Contrast [`requeue_task`], the retry path, which
/// also *advances* `attempt`/`run_at`.)
pub(super) fn mark_pending(conn: &Connection, task_id: &str) -> Result<(), SpikeError> {
    conn.execute(
        "UPDATE spike_tasks SET state = 'PENDING' WHERE task_id = ?1",
        params![task_id],
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

/// True if `exec_id` has any armed (unfired) durable timer — the ground-truth
/// signal a no-progress cycle is genuinely waiting on a not-yet-due timer.
pub(super) fn has_unfired_timer(
    conn: &Connection,
    exec_id: ExecutionId,
) -> Result<bool, SpikeError> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM spike_timers WHERE exec_id = ?1 AND fired = 0",
        params![exec_id.to_string()],
        |row| row.get(0),
    )?;
    Ok(count > 0)
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

/// The greatest `fire_at` among all *fired* timers (across every execution), or
/// `0` if none. A belt-and-braces lower bound for restoring the virtual clock on
/// [`open`](super::SqliteRuntime::open): a fired timer proves the clock once
/// reached its deadline, so the restored clock must never regress below it — even
/// if the explicit clock write was somehow lost. The primary restore is the
/// persisted [`store::load_clock`](super::store::load_clock); this only guards
/// against that being absent/stale.
pub(super) fn max_fired_timer_deadline(conn: &Connection) -> Result<i64, SpikeError> {
    let v: Option<i64> = conn.query_row(
        "SELECT MAX(fire_at) FROM spike_timers WHERE fired = 1",
        [],
        |row| row.get(0),
    )?;
    Ok(v.unwrap_or(0))
}

// ── Introspection (tests) ──────────────────────────────────────────────────────

/// The `activity_id`s of every task-queue row (any state) for `exec_id`, in FIFO
/// order. Exposed so the spike's tests can assert the per-cycle append+enqueue
/// atomicity: an `ActivityScheduled` event and its `spike_tasks` row are always
/// observed together (both committed, or — on a rolled-back batch — neither).
pub(super) fn all_task_activity_ids(
    conn: &Connection,
    exec_id: ExecutionId,
) -> Result<Vec<String>, SpikeError> {
    let mut stmt =
        conn.prepare("SELECT activity_id FROM spike_tasks WHERE exec_id = ?1 ORDER BY seq")?;
    let rows = stmt.query_map(params![exec_id.to_string()], |row| row.get::<_, String>(0))?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

/// The `state` of every task-queue row for `exec_id`, in FIFO (`seq`) order.
/// Exposed so a test can assert a claimed task that hit a post-claim error (an
/// unregistered handler, or a transient post-body persistence failure) was
/// returned to `PENDING` (re-drainable) via [`mark_pending`], never stranded
/// `RUNNING`.
pub(super) fn task_states(
    conn: &Connection,
    exec_id: ExecutionId,
) -> Result<Vec<String>, SpikeError> {
    let mut stmt = conn.prepare("SELECT state FROM spike_tasks WHERE exec_id = ?1 ORDER BY seq")?;
    let rows = stmt.query_map(params![exec_id.to_string()], |row| row.get::<_, String>(0))?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

/// The `timer_id`s of every armed (unfired) durable timer for `exec_id`. The
/// timer half of the append+arm atomicity invariant: every `TimerStarted` event
/// has a matching `spike_timers` row.
pub(super) fn armed_timer_ids(
    conn: &Connection,
    exec_id: ExecutionId,
) -> Result<Vec<String>, SpikeError> {
    let mut stmt = conn.prepare(
        "SELECT timer_id FROM spike_timers WHERE exec_id = ?1 AND fired = 0 ORDER BY fire_at",
    )?;
    let rows = stmt.query_map(params![exec_id.to_string()], |row| row.get::<_, String>(0))?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

/// The `timer_id`s of every *fired* durable timer for `exec_id`. Paired with the
/// history's `TimerFired` events this asserts timer-fire atomicity: the
/// `TimerFired` append + `fired = 1` flag commit in one transaction, so a
/// `TimerFired` event is never durable without its `fired = 1` row — a reload can
/// therefore never re-fire it and append a stray duplicate `TimerFired`.
pub(super) fn fired_timer_ids(
    conn: &Connection,
    exec_id: ExecutionId,
) -> Result<Vec<String>, SpikeError> {
    let mut stmt = conn.prepare(
        "SELECT timer_id FROM spike_timers WHERE exec_id = ?1 AND fired = 1 ORDER BY fire_at",
    )?;
    let rows = stmt.query_map(params![exec_id.to_string()], |row| row.get::<_, String>(0))?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
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
