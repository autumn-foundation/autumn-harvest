//! Hand-written `SQLite` DDL for the embedded backend, plus the additive
//! [`migrate`] step that brings a pre-existing database file up to the current
//! shape.
//!
//! Deliberately **not** the Postgres migration DDL: the dialect differs (TEXT for
//! UUIDs/JSON, INTEGER for sequence numbers and epoch timestamps) and this
//! backend persists only what a single-writer, embedded runtime needs. The DDL
//! is applied idempotently (`CREATE TABLE IF NOT EXISTS`) on every
//! [`SqliteRuntime::open`](crate::SqliteRuntime::open), so re-opening an existing
//! file is safe.
//!
//! Tables:
//! - `harvest_executions` — one row per workflow run (input + terminal state).
//! - `harvest_events` — the append-only
//!   [`WorkflowEvent`](autumn_harvest::WorkflowEvent) log, keyed `(exec_id, seq)`;
//!   `event_json` is `serde_json::to_string(&WorkflowEvent)`. This is the
//!   *canonical, replayable* history. Each event's JSON encoding is byte-identical
//!   to the Postgres backend's (the shared adjacently-tagged `serde` form), which
//!   is what makes a history produced here replay unchanged on the core
//!   `WorkflowReplayer` (and, in principle, a Postgres hub).
//! - `harvest_tasks` — the activity task queue (the `FOR UPDATE SKIP LOCKED`
//!   analog; single writer — see [`queue`](crate::queue)).
//! - `harvest_timers` — durable timers with an absolute epoch `fire_at` and a
//!   monotonic `arm_seq`. `arm_seq` is the deterministic tie-breaker when several
//!   timers share the same `fire_at` (e.g. `tokio::join!(ctx.timer("a", 1),
//!   ctx.timer("b", 1))`): due timers fire in `(fire_at, arm_seq)` order, so the
//!   `TimerFired` events land in the same order as the `TimerStarted` events. The
//!   core `HistoryMatcher::match_timer` scans for its own `TimerFired` but STOPS at
//!   an unconsumed sibling `TimerFired`, so a reversed fire order would wedge replay
//!   (issue #1069 P2).
//! - `harvest_signals` — staged inbound signals awaiting a matching
//!   `wait_for_signal`. Each row records the absolute epoch-millisecond the signal
//!   arrived (`received_at`, issue #1069 P2 — millisecond precision so an on-time
//!   signal cannot lose a deadline race near a second boundary), which the
//!   wake-event ingest interleaves against a deadline timer's `fire_at` so a
//!   signal delivered *after* an expired `wait_for_signal_timeout` deadline cannot
//!   retroactively win the race (issue #476; mirrors the Postgres `merge_wake_events`
//!   `received_at` vs `fires_at` ordering).
//! - `harvest_activity_attempts` — a per-attempt audit log. **Retryable** activity
//!   failures live here, not in `harvest_events`, mirroring the Postgres engine's
//!   `requeue_for_retry` (which stores the attempt error on the task-queue row,
//!   not `harvest_events`). Only terminal outcomes reach the replayable log.

/// The full schema, applied idempotently on
/// [`SqliteRuntime::open`](crate::SqliteRuntime::open).
pub const SCHEMA: &str = r"
CREATE TABLE IF NOT EXISTS harvest_executions (
    exec_id       TEXT PRIMARY KEY,
    workflow_name TEXT NOT NULL,
    workflow_id   TEXT NOT NULL DEFAULT '', -- the run-scoped BUSINESS workflow id
                                           -- (issue #698, Codex #1069 P2 runtime.rs:780):
                                           -- the value `ctx.info().workflow_id` reports,
                                           -- documented for minting idempotency keys.
                                           -- Persisted at start and re-read into the
                                           -- WorkflowContext on EVERY drive cycle, so it is
                                           -- STABLE across replays (never regenerated).
                                           -- Defaults to the `exec_id` string form when the
                                           -- caller supplies none, so it is never empty and
                                           -- distinct per run (mirrors the Postgres worker,
                                           -- which injects StartWorkflowParams.workflow_id).
    input_json    TEXT NOT NULL,
    state         TEXT NOT NULL,          -- RUNNING | COMPLETED | FAILED
    output_json   TEXT,
    error         TEXT
);

-- Non-unique lookup index for the (workflow_name, workflow_id) reuse-policy probe
-- (issue #1068, `store::find_active_execution_by_key`). Deliberately NON-unique: a
-- unique index would FAIL to build on reopen of any database file already holding
-- duplicate active ids from the pre-#1068 always-fresh start behavior, breaking
-- durability. The reuse matrix is instead enforced in the single-writer start
-- transaction (there are no concurrent writers, so a lookup-then-decide is atomic).
-- `CREATE INDEX IF NOT EXISTS` is idempotent and never fails on duplicate keys, so
-- it is applied by the base SCHEMA on every open — including pre-existing files —
-- needing no separate additive migration.
CREATE INDEX IF NOT EXISTS idx_harvest_executions_key
    ON harvest_executions (workflow_name, workflow_id);

CREATE TABLE IF NOT EXISTS harvest_events (
    exec_id    TEXT NOT NULL,
    seq        INTEGER NOT NULL,
    event_json TEXT NOT NULL,
    PRIMARY KEY (exec_id, seq)
);

CREATE TABLE IF NOT EXISTS harvest_tasks (
    task_id      TEXT PRIMARY KEY,
    exec_id      TEXT NOT NULL,
    activity_id  TEXT NOT NULL,            -- the scheduled ActivityExecId
    name         TEXT NOT NULL,
    input_json   TEXT NOT NULL,
    queue        TEXT NOT NULL,
    state        TEXT NOT NULL,            -- PENDING | RUNNING | DONE
    attempt      INTEGER NOT NULL,         -- attempts consumed so far
    run_at       INTEGER NOT NULL,         -- earliest epoch-millisecond this may run
    seq          INTEGER NOT NULL,         -- FIFO ordering within the queue
    max_attempts INTEGER,                  -- per-call retry cap from the command's
                                           -- retry_policy_override (issue #1069 P2);
                                           -- NULL = use the registered ActivitySpec default
    retry_policy_json TEXT,                -- the WHOLE serialized RetryPolicy from the
                                           -- command's retry_policy_override (issue #1069 P2,
                                           -- Codex runtime.rs:985) — initial_interval,
                                           -- backoff_coefficient, max_interval,
                                           -- non_retryable_errors, jitter — so the worker
                                           -- honors backoff timing and non-retryable
                                           -- classification, not just max_attempts.
                                           -- NULL = raw-path task (no declared policy):
                                           -- immediate requeue, no policy non-retryable list.
    start_to_close_ms INTEGER,             -- per-activity start-to-close budget in
                                           -- milliseconds from the command's
                                           -- start_to_close_override (issue #1069 P2);
                                           -- NULL = no budget (unbounded, prior behavior).
                                           -- Enforced as a post-execution outcome by the
                                           -- worker: a body whose real wall-clock runtime
                                           -- exceeds it records a terminal ActivityTimedOut
                                           -- { StartToClose } instead of ActivityCompleted,
                                           -- byte-equivalent to what the Postgres timeout
                                           -- scanner durably records.
    scheduled_at INTEGER NOT NULL,         -- ABSOLUTE epoch-millisecond the ScheduleActivity
                                           -- command was persisted (== the initial `run_at`).
                                           -- The STABLE anchor for the activity's total
                                           -- (cross-retry) schedule_to_close deadline (issue
                                           -- #378, Codex #1069 P2 runtime.rs:944): unlike
                                           -- `run_at`, it is NEVER mutated by a retry requeue,
                                           -- so it always denotes when the activity was first
                                           -- scheduled. The FROZEN absolute deadline derived from
                                           -- it lives in `schedule_to_close_at` below (issue
                                           -- #1068).
    schedule_to_close_at INTEGER,          -- The FROZEN absolute (epoch-millisecond) total
                                           -- (cross-retry) schedule_to_close deadline for this
                                           -- task (issue #1068): `scheduled_at +
                                           -- default_schedule_to_close`. NULL = no total cap
                                           -- (the activity declared no schedule_to_close, or was
                                           -- raw-registered). Resolved ONCE and frozen — at
                                           -- schedule time when the activity is already registered
                                           -- (`defaults_frozen = 1`), else at first CLAIM for a
                                           -- LATE-registered activity — so an in-flight run's
                                           -- deadline is IMMUTABLE and a crash/reopen or a
                                           -- re-registration under a changed spec cannot alter it,
                                           -- mirroring the Postgres engine (which freezes the
                                           -- deadline onto the task-queue row). Anchoring to
                                           -- `scheduled_at` (never claim time) keeps the wall-clock
                                           -- budget measured from when the activity was first
                                           -- scheduled, so a wait for late registration does not
                                           -- extend it.
    defaults_frozen INTEGER NOT NULL DEFAULT 0  -- Whether the four resolved-default scalars
                                           -- (`retry_policy_json`, `max_attempts`,
                                           -- `start_to_close_ms`, `schedule_to_close_at`) have
                                           -- been RESOLVED onto this row (issue #1068). 0 = not
                                           -- yet resolved (the activity was scheduled before its
                                           -- body/`ActivityInfo` was registered) — the CLAIM path
                                           -- resolves them from the now-present registered spec,
                                           -- persists them, and sets this to 1. 1 = frozen: read
                                           -- the four columns VERBATIM (a NULL means resolved-to-
                                           -- None) and NEVER re-consult the mutable registry for
                                           -- them. The activity BODY is always resolved from the
                                           -- registry regardless — only the policy/deadline
                                           -- scalars are frozen. `DEFAULT 0` so a row created by
                                           -- the additive migration (pre-#1068) is treated as
                                           -- unresolved and gets frozen on its next claim.
);

CREATE TABLE IF NOT EXISTS harvest_timers (
    timer_id TEXT NOT NULL,
    exec_id  TEXT NOT NULL,
    fire_at  INTEGER NOT NULL,            -- absolute epoch-millisecond (issue #1069 P2:
                                          -- sub-second precision so a timer never fires
                                          -- before its true deadline)
    fired    INTEGER NOT NULL DEFAULT 0,
    arm_seq  INTEGER NOT NULL DEFAULT 0,  -- monotonic arm order (issue #1069 P2);
                                          -- the deterministic tie-breaker for equal
                                          -- fire_at deadlines, so multiple timers armed
                                          -- in one batch (a join! of two timers) FIRE in
                                          -- TimerStarted-append order, which the core
                                          -- matcher requires to replay
    PRIMARY KEY (exec_id, timer_id)
);

CREATE TABLE IF NOT EXISTS harvest_signals (
    signal_seq   INTEGER PRIMARY KEY AUTOINCREMENT,
    exec_id      TEXT NOT NULL,
    name         TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    delivered    INTEGER NOT NULL DEFAULT 0,
    received_at  INTEGER NOT NULL DEFAULT 0   -- absolute epoch-millisecond the signal arrived
);

CREATE TABLE IF NOT EXISTS harvest_activity_attempts (
    attempt_seq INTEGER PRIMARY KEY AUTOINCREMENT,
    exec_id     TEXT NOT NULL,
    name        TEXT NOT NULL,
    attempt     INTEGER NOT NULL,
    ok          INTEGER NOT NULL,         -- 1 = success, 0 = failure
    detail      TEXT NOT NULL             -- output JSON on success, error on failure
);
";

use rusqlite::Connection;

use crate::error::SqliteResult;

/// Bring an existing database file up to the current [`SCHEMA`] shape by adding
/// any columns introduced after it was created — run on every
/// [`SqliteRuntime::open`](crate::SqliteRuntime::open), right after the idempotent
/// `CREATE TABLE IF NOT EXISTS` batch.
///
/// The base schema is applied with `CREATE TABLE IF NOT EXISTS`, so a table that
/// already exists is left with its *original* column set — a database created
/// before a column was added would otherwise be missing it, and a fresh `INSERT`
/// (which lists the new column) would fail with "no such column". This step is the
/// backend's minimal, forward-only migration: it is **strictly additive** (only
/// `ALTER TABLE … ADD COLUMN`, never `DROP`/`RENAME`), **idempotent** (each column
/// is added only when absent, so it is safe to run on every open — including a
/// fresh database that already has the column), and preserves every existing row's
/// data.
///
/// Current migrations:
/// - **issue #1068** — `harvest_tasks.schedule_to_close_at` (the frozen absolute
///   total-deadline column) and `harvest_tasks.defaults_frozen` (the
///   resolved-defaults flag). Both are nullable/defaulted, so a pre-#1068 row is
///   valid unchanged and is treated as "defaults not yet frozen" (its next claim
///   resolves and freezes them).
///
/// # Errors
///
/// Returns [`SqliteError::Sqlite`](crate::SqliteError::Sqlite) if a column probe or
/// `ALTER TABLE` fails.
pub fn migrate(conn: &Connection) -> SqliteResult<()> {
    // issue #1068: freeze resolved activity defaults onto the task row.
    add_column_if_absent(
        conn,
        "harvest_tasks",
        "schedule_to_close_at",
        "ALTER TABLE harvest_tasks ADD COLUMN schedule_to_close_at INTEGER",
    )?;
    add_column_if_absent(
        conn,
        "harvest_tasks",
        "defaults_frozen",
        "ALTER TABLE harvest_tasks ADD COLUMN defaults_frozen INTEGER NOT NULL DEFAULT 0",
    )?;
    Ok(())
}

/// Run `add_column_sql` iff `table` does not already have a column named `column`.
///
/// `table`/`column` are backend-internal string LITERALS (never caller input), so
/// interpolating them into the `PRAGMA table_info(...)` probe is safe — a `PRAGMA`
/// takes its table name as a bare identifier, not a bindable parameter.
fn add_column_if_absent(
    conn: &Connection,
    table: &str,
    column: &str,
    add_column_sql: &str,
) -> SqliteResult<()> {
    if !column_exists(conn, table, column)? {
        conn.execute_batch(add_column_sql)?;
    }
    Ok(())
}

/// True iff `table` has a column named `column`, read from `PRAGMA table_info`
/// (whose result rows are `(cid, name, type, notnull, dflt_value, pk)` — the name
/// is column index 1).
fn column_exists(conn: &Connection, table: &str, column: &str) -> SqliteResult<bool> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        if name == column {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use super::{SCHEMA, column_exists, migrate};

    // The pre-#1068 `harvest_tasks` shape (no freeze columns) — the 14 columns that
    // existed before this change.
    const OLD_HARVEST_TASKS_DDL: &str = "\
        CREATE TABLE harvest_tasks (\
            task_id TEXT PRIMARY KEY, exec_id TEXT NOT NULL, activity_id TEXT NOT NULL, \
            name TEXT NOT NULL, input_json TEXT NOT NULL, queue TEXT NOT NULL, \
            state TEXT NOT NULL, attempt INTEGER NOT NULL, run_at INTEGER NOT NULL, \
            seq INTEGER NOT NULL, max_attempts INTEGER, retry_policy_json TEXT, \
            start_to_close_ms INTEGER, scheduled_at INTEGER NOT NULL)";

    // A fresh database already has the freeze columns from `SCHEMA`, so `migrate` is
    // a pure no-op (idempotent).
    #[test]
    fn migrate_is_a_noop_on_a_fresh_database() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        assert!(column_exists(&conn, "harvest_tasks", "schedule_to_close_at").unwrap());
        assert!(column_exists(&conn, "harvest_tasks", "defaults_frozen").unwrap());
        // Running it (repeatedly) does not error and does not change the columns.
        migrate(&conn).unwrap();
        migrate(&conn).unwrap();
        assert!(column_exists(&conn, "harvest_tasks", "schedule_to_close_at").unwrap());
        assert!(column_exists(&conn, "harvest_tasks", "defaults_frozen").unwrap());
    }

    // An old-schema `harvest_tasks` (missing the freeze columns) is brought up to
    // date additively — the two columns are added, existing data is preserved.
    #[test]
    fn migrate_adds_freeze_columns_to_an_old_schema_table() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(OLD_HARVEST_TASKS_DDL).unwrap();
        // Seed a pre-existing row to prove data survives the ALTER.
        conn.execute(
            "INSERT INTO harvest_tasks \
             (task_id, exec_id, activity_id, name, input_json, queue, state, attempt, \
              run_at, seq, max_attempts, retry_policy_json, start_to_close_ms, scheduled_at) \
             VALUES ('t1', 'e1', 'a1', 'act', '{}', 'default', 'PENDING', 0, 0, 0, NULL, NULL, NULL, 0)",
            [],
        )
        .unwrap();

        assert!(!column_exists(&conn, "harvest_tasks", "schedule_to_close_at").unwrap());
        assert!(!column_exists(&conn, "harvest_tasks", "defaults_frozen").unwrap());

        migrate(&conn).unwrap();

        assert!(column_exists(&conn, "harvest_tasks", "schedule_to_close_at").unwrap());
        assert!(column_exists(&conn, "harvest_tasks", "defaults_frozen").unwrap());

        // The pre-existing row survives; the new columns default correctly (NULL
        // deadline, unresolved defaults so its next claim freezes them).
        let (deadline, frozen): (Option<i64>, i64) = conn
            .query_row(
                "SELECT schedule_to_close_at, defaults_frozen FROM harvest_tasks WHERE task_id = 't1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(deadline, None, "the migrated column defaults to NULL");
        assert_eq!(
            frozen, 0,
            "a migrated row is treated as defaults-not-yet-frozen"
        );
    }
}
