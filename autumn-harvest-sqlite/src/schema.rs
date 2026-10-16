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
//! - `harvest_signals` — staged inbound signals. The next drive appends each one
//!   to history (issue #2020). Each row records the epoch-millisecond the signal
//!   arrived (`received_at`, issue #1069 P2). Millisecond precision keeps an
//!   on-time signal from losing a deadline race near a second boundary. The
//!   ingest compares `received_at` with the `fire_at` of a deadline timer. So a
//!   signal delivered after an expired `wait_for_signal_timeout` deadline cannot
//!   win the race (issue #476). This mirrors the Postgres `merge_wake_events`
//!   order of `received_at` and `fires_at`. An optional `idempotency_key` is
//!   unique per execution (issue #2028). A seal keeps a keyed row, so the key
//!   still dedupes a retry.
//! - `harvest_activity_attempts` — a per-attempt audit log. **Retryable** activity
//!   failures live here, not in `harvest_events`, mirroring the Postgres engine's
//!   `requeue_for_retry` (which stores the attempt error on the task-queue row,
//!   not `harvest_events`). Only terminal outcomes reach the replayable log.
//! - `harvest_mutex_locks`, `harvest_mutex_waiters` and `harvest_mutex_seq` —
//!   the workflow mutex (issue #2029). A lock row holds a key. The waiter rows
//!   form a FIFO queue per key. The counter mints each fencing `lock_seq`.

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
    state         TEXT NOT NULL,          -- RUNNING | COMPLETED | FAILED | CANCELLED |
                                           -- TERMINATED | TIMED_OUT | CONTINUED_AS_NEW
    output_json   TEXT,
    error         TEXT,
    cancel_reason TEXT,                    -- a cooperative cancel request (issue #2021).
                                           -- NULL until `cancel` stores a request. The
                                           -- drive reads it only while the run is
                                           -- RUNNING. A terminal run keeps the value.
    cancel_from_seq INTEGER,               -- the history length when `cancel` stored
                                           -- the request (issue #2021). It bounds the
                                           -- events that a cleanup can add.
    parent_exec_id      TEXT,              -- the parent run of a child (issue #2026)
    parent_close_policy TEXT,              -- NULL for an awaited child; the policy of a
                                           -- detached child
    started_at    INTEGER,                 -- the `WorkflowStarted` time in epoch
                                           -- microseconds (issue #2027). NULL on a row
                                           -- written before that issue.
    closed_at     INTEGER,                 -- the time of the first terminal write in
                                           -- epoch microseconds (issue #2027). NULL while the
                                           -- run is open.
    search_attrs  TEXT,                    -- the search attributes as a JSON object
                                           -- (issue #2027). NULL until a start or an
                                           -- upsert sets one.
    memo          TEXT,                    -- the start memo as JSON (issue #2027). No
                                           -- command changes it after the start.
    execution_timeout_ms INTEGER,          -- the declared execution_timeout (issue
                                           -- #2029). NULL = no timeout. A successor
                                           -- re-anchors its deadline with it.
    deadline_at INTEGER,                   -- the epoch-millisecond deadline of this run
                                           -- (issue #2029). NULL = no deadline.
    start_at INTEGER,                      -- the earliest epoch-millisecond a drive may
                                           -- run this run (issue #2029). A retry
                                           -- successor waits for its backoff here.
    workflow_attempt INTEGER NOT NULL DEFAULT 1, -- the 1-based workflow retry attempt
                                           -- (issue #2029).
    retry_policy_json TEXT,                -- the frozen workflow-level RetryPolicy
                                           -- (issue #2029). NULL = no retry.
    retry_of_exec_id TEXT,                 -- the failed run that this retry replaces
                                           -- (issue #2029).
    local_retry_at INTEGER                 -- the epoch-millisecond end of a local
                                           -- activity's retry backoff (issue #2029).
                                           -- A drive before it waits.
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

-- The state filter of a listing (issue #2027). It indexes the same expression
-- the filter tests, so the planner can use it. The filter compares bytes, so a
-- value in the wrong storage class still matches. Without the index, a filter
-- on `state` reads past `input_json` on every row, which is slow when the
-- inputs are large. The index is non-unique and idempotent, so it builds on
-- every open of an older file.
CREATE INDEX IF NOT EXISTS idx_harvest_executions_state
    ON harvest_executions (cast(state as blob));

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
    defaults_frozen INTEGER NOT NULL DEFAULT 0, -- Whether the four resolved-default scalars
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
    heartbeat_details TEXT,                -- The latest heartbeat payload as JSON (issue
                                           -- #2024). NULL = no heartbeat yet. A retry or a
                                           -- reopen reads it back as the checkpoint.
    last_error TEXT                        -- The previous attempt's error message (issue
                                           -- #2024). `ActivityContext::previous_failure`
                                           -- reads it. NULL on the first attempt.
);

-- Non-unique index on `harvest_tasks(seq)` so `MAX(seq)` (the next-`seq` allocation
-- read in `queue`, used on every enqueue and every zero-delay requeue-to-back) is
-- satisfied from the index instead of a full-table scan (issue #1068, hardening item
-- 3). The new zero-delay requeue-to-back (which reassigns a fresh `MAX(seq) + 1` on
-- each immediate race retry) makes that read hotter, so the index keeps a busy retry
-- loop cheap. `CREATE INDEX IF NOT EXISTS` is idempotent and non-unique — it runs on
-- every open (including a pre-existing file) and never fails, so no separate additive
-- migration is needed.
CREATE INDEX IF NOT EXISTS idx_harvest_tasks_seq
    ON harvest_tasks (seq);

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
    received_at  INTEGER NOT NULL DEFAULT 0,  -- absolute epoch-millisecond the signal arrived
    idempotency_key TEXT                      -- the caller's dedupe key (issue #2028).
                                              -- NULL = no key. `migrate` builds the
                                              -- unique index on (exec_id, key).
);

-- Without this, every exec_id/delivered-scoped query here (the signal read,
-- delete_undelivered_signals_for_execution) scans the whole table, unlike
-- harvest_timers, whose (exec_id, timer_id) PRIMARY KEY already covers this.
-- The trailing columns serve the arrival order of the signal read (issue
-- #2020). Without them, the read sorts the whole backlog before its first row.
-- It replaces the (exec_id, delivered) index of earlier files. Both statements
-- are idempotent and run on every open, so no separate migration is needed.
CREATE INDEX IF NOT EXISTS idx_harvest_signals_exec_arrival
    ON harvest_signals (exec_id, delivered, received_at, signal_seq);
DROP INDEX IF EXISTS idx_harvest_signals_exec_delivered;

-- The workflow mutex (issue #2029). A lock row means the key is held. Each
-- grant and each release runs in one write transaction of the single writer.
CREATE TABLE IF NOT EXISTS harvest_mutex_locks (
    lock_key       TEXT PRIMARY KEY,
    holder_exec_id TEXT NOT NULL,
    lock_seq       INTEGER NOT NULL        -- the fencing token of the grant
);
CREATE INDEX IF NOT EXISTS idx_harvest_mutex_locks_holder
    ON harvest_mutex_locks (holder_exec_id);

-- The FIFO queue of runs that wait for a key. `waiter_seq` is the order.
CREATE TABLE IF NOT EXISTS harvest_mutex_waiters (
    waiter_seq     INTEGER PRIMARY KEY AUTOINCREMENT,
    lock_key       TEXT NOT NULL,
    waiter_exec_id TEXT NOT NULL,
    UNIQUE (lock_key, waiter_exec_id)
);
CREATE INDEX IF NOT EXISTS idx_harvest_mutex_waiters_exec
    ON harvest_mutex_waiters (waiter_exec_id);

-- One row. It mints a new `lock_seq` for each grant, so a token is never
-- reused after a release.
CREATE TABLE IF NOT EXISTS harvest_mutex_seq (
    id   INTEGER PRIMARY KEY CHECK (id = 1),
    last INTEGER NOT NULL
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
/// backend's minimal, forward-only migration. It is **additive**: it adds columns
/// and indexes, and never drops or renames. It is **idempotent**: each column is
/// added only when absent, so it is safe to run on every open, a fresh database
/// included. It keeps every existing row. The one row update is the issue #2028
/// back-fill, which only sets a NULL key.
///
/// Current migrations:
/// - **issue #2024** — `harvest_tasks.heartbeat_details` and
///   `harvest_tasks.last_error`. Both are nullable, so an old row is valid.
/// - **issue #2026** — `harvest_executions.parent_exec_id` and
///   `parent_close_policy` link a child to its parent. `harvest_signals.idempotency_key`
///   holds the dedup key of an external signal. All are nullable. Their indexes
///   are built here, after the columns exist.
/// - **issue #1068** — `harvest_tasks.schedule_to_close_at` (the frozen absolute
///   total-deadline column) and `harvest_tasks.defaults_frozen` (the
///   resolved-defaults flag). Both are nullable/defaulted, so a pre-#1068 row is
///   valid unchanged and is treated as "defaults not yet frozen" (its next claim
///   resolves and freezes them).
/// - **issue #2021** — `harvest_executions.cancel_reason`, the stored cooperative
///   cancel request, and `cancel_from_seq`, the history length at the request.
///   Both are nullable, so an old row has no pending cancel.
/// - **issue #2027** — `harvest_executions.started_at`, `closed_at`,
///   `search_attrs` and `memo`. All four are nullable. An old row has no start
///   time, no close time, no search attributes and no memo.
/// - **issue #2028** — `harvest_signals.idempotency_key` and its partial unique
///   index on `(exec_id, idempotency_key)`. The index is built here, because the
///   base [`SCHEMA`] runs before an old table has the column. When the column is
///   new, each promise settlement takes its name as its key (see
///   [`back_fill_promise_keys`]). One transaction holds the column, the index and
///   the back-fill, so a crash cannot leave the column without the back-fill.
/// - **issue #2029** — `harvest_executions.execution_timeout_ms`, `deadline_at`,
///   `start_at`, `workflow_attempt`, `retry_policy_json`, `retry_of_exec_id` and
///   `local_retry_at`.
///   Each is nullable or has a default, so an old row has no timeout and no
///   retry. The mutex tables are new, so `CREATE TABLE IF NOT EXISTS` adds them.
///
/// # Errors
///
/// Returns [`SqliteError::Sqlite`](crate::SqliteError::Sqlite) if a column probe,
/// an `ALTER TABLE`, the index build or the back-fill fails.
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
    // issue #2024: the heartbeat checkpoint and the previous attempt's error.
    add_column_if_absent(
        conn,
        "harvest_tasks",
        "heartbeat_details",
        "ALTER TABLE harvest_tasks ADD COLUMN heartbeat_details TEXT",
    )?;
    add_column_if_absent(
        conn,
        "harvest_tasks",
        "last_error",
        "ALTER TABLE harvest_tasks ADD COLUMN last_error TEXT",
    )?;
    // issue #2021: store a cooperative cancel request until the seal.
    add_column_if_absent(
        conn,
        "harvest_executions",
        "cancel_reason",
        "ALTER TABLE harvest_executions ADD COLUMN cancel_reason TEXT",
    )?;
    add_column_if_absent(
        conn,
        "harvest_executions",
        "cancel_from_seq",
        "ALTER TABLE harvest_executions ADD COLUMN cancel_from_seq INTEGER",
    )?;
    // issue #2026: the parent link of a child run.
    for column in ["parent_exec_id", "parent_close_policy"] {
        add_column_if_absent(
            conn,
            "harvest_executions",
            column,
            &format!("ALTER TABLE harvest_executions ADD COLUMN {column} TEXT"),
        )?;
    }
    // issue #2027: the columns that list and describe read.
    for (column, sql) in [
        (
            "started_at",
            "ALTER TABLE harvest_executions ADD COLUMN started_at INTEGER",
        ),
        (
            "closed_at",
            "ALTER TABLE harvest_executions ADD COLUMN closed_at INTEGER",
        ),
        (
            "search_attrs",
            "ALTER TABLE harvest_executions ADD COLUMN search_attrs TEXT",
        ),
        (
            "memo",
            "ALTER TABLE harvest_executions ADD COLUMN memo TEXT",
        ),
    ] {
        add_column_if_absent(conn, "harvest_executions", column, sql)?;
    }
    // issue #2029: the execution timeout and the workflow retry.
    for (column, ddl) in [
        ("execution_timeout_ms", "INTEGER"),
        ("deadline_at", "INTEGER"),
        ("start_at", "INTEGER"),
        ("workflow_attempt", "INTEGER NOT NULL DEFAULT 1"),
        ("retry_policy_json", "TEXT"),
        ("retry_of_exec_id", "TEXT"),
        ("local_retry_at", "INTEGER"),
    ] {
        add_column_if_absent(
            conn,
            "harvest_executions",
            column,
            &format!("ALTER TABLE harvest_executions ADD COLUMN {column} {ddl}"),
        )?;
    }
    // issue #2028: the signal idempotency key. The back-fill runs only when the
    // column is new. So the column, the index and the back-fill share one
    // transaction: a crash after the column must not skip the back-fill.
    let tx = conn.unchecked_transaction()?;
    let key_added = add_column_if_absent(
        &tx,
        "harvest_signals",
        "idempotency_key",
        "ALTER TABLE harvest_signals ADD COLUMN idempotency_key TEXT",
    )?;
    // An index on a new column must follow its `ADD COLUMN`. In `SCHEMA` it
    // would fail on an old file, which still lacks the column (issue #2026).
    tx.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_harvest_executions_parent \
             ON harvest_executions (parent_exec_id);",
    )?;
    tx.execute_batch(SIGNAL_KEY_INDEX)?;
    if key_added {
        back_fill_promise_keys(&tx)?;
    }
    tx.commit()?;
    Ok(())
}

/// The partial unique index that enforces a signal idempotency key (issue
/// #2028). A NULL key is outside the index, so an unkeyed signal always
/// stages. It matches the Postgres `uq_harvest_signals_idem`.
const SIGNAL_KEY_INDEX: &str = "\
    CREATE UNIQUE INDEX IF NOT EXISTS uq_harvest_signals_idem \
        ON harvest_signals (exec_id, idempotency_key) \
        WHERE idempotency_key IS NOT NULL;";

/// Give each stored promise settlement its name as its key (issue #2028).
///
/// Before issue #2028, this backend deduped a settlement by its name. Now the
/// key does it, so an old settlement needs the key too. Only the first row of
/// each `(exec_id, name)` takes the key. A later duplicate keeps NULL, so the
/// unique index stays valid. A row whose run already holds the key also keeps
/// NULL, so a second run of the back-fill cannot break the index.
fn back_fill_promise_keys(conn: &Connection) -> SqliteResult<()> {
    conn.execute(
        "UPDATE harvest_signals SET idempotency_key = name \
         WHERE idempotency_key IS NULL \
           AND substr(name, 1, length(?1)) = ?1 \
           AND signal_seq = (SELECT MIN(first.signal_seq) FROM harvest_signals AS first \
                             WHERE first.exec_id = harvest_signals.exec_id \
                               AND first.name = harvest_signals.name) \
           AND NOT EXISTS (SELECT 1 FROM harvest_signals AS held \
                           WHERE held.exec_id = harvest_signals.exec_id \
                             AND held.idempotency_key = harvest_signals.name)",
        [autumn_harvest::durable_promise::PROMISE_SIGNAL_PREFIX],
    )?;
    Ok(())
}

/// Run `add_column_sql` iff `table` does not already have a column named `column`.
/// Returns `true` when it added the column.
///
/// `table`/`column` are backend-internal string LITERALS (never caller input), so
/// interpolating them into the `PRAGMA table_info(...)` probe is safe — a `PRAGMA`
/// takes its table name as a bare identifier, not a bindable parameter.
fn add_column_if_absent(
    conn: &Connection,
    table: &str,
    column: &str,
    add_column_sql: &str,
) -> SqliteResult<bool> {
    if column_exists(conn, table, column)? {
        return Ok(false);
    }
    conn.execute_batch(add_column_sql)?;
    Ok(true)
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

    // The pre-#2021 `harvest_executions` shape (no `cancel_reason`).
    const OLD_HARVEST_EXECUTIONS_DDL: &str = "\
        CREATE TABLE harvest_executions (\
            exec_id TEXT PRIMARY KEY, workflow_name TEXT NOT NULL, \
            workflow_id TEXT NOT NULL DEFAULT '', input_json TEXT NOT NULL, \
            state TEXT NOT NULL, output_json TEXT, error TEXT)";

    // The pre-#2028 `harvest_signals` shape (no `idempotency_key`).
    const OLD_HARVEST_SIGNALS_DDL: &str = "\
        CREATE TABLE harvest_signals (\
            signal_seq INTEGER PRIMARY KEY AUTOINCREMENT, exec_id TEXT NOT NULL, \
            name TEXT NOT NULL, payload_json TEXT NOT NULL, \
            delivered INTEGER NOT NULL DEFAULT 0, received_at INTEGER NOT NULL DEFAULT 0)";

    fn old_database() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(OLD_HARVEST_EXECUTIONS_DDL).unwrap();
        conn.execute_batch(OLD_HARVEST_TASKS_DDL).unwrap();
        conn.execute_batch(OLD_HARVEST_SIGNALS_DDL).unwrap();
        conn
    }

    fn signal_keys(conn: &Connection) -> Vec<(String, Option<String>)> {
        let mut stmt = conn
            .prepare("SELECT name, idempotency_key FROM harvest_signals ORDER BY signal_seq")
            .unwrap();
        stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    // Issue #2028: an old table gains the key column and its unique index. A
    // promise row takes its name as its key. Other rows keep no key.
    #[test]
    fn migrate_adds_the_signal_key_and_back_fills_promises() {
        let conn = old_database();
        conn.execute_batch(
            "INSERT INTO harvest_signals (exec_id, name, payload_json) VALUES \
             ('e1', 'go', '1'), ('e1', 'harvest.promise:p', '{}'), \
             ('e1', 'harvest.promise:p', '{}'), ('e2', 'harvest.promise:p', '{}')",
        )
        .unwrap();

        migrate(&conn).unwrap();
        migrate(&conn).unwrap();

        assert_eq!(
            signal_keys(&conn),
            vec![
                ("go".to_string(), None),
                (
                    "harvest.promise:p".to_string(),
                    Some("harvest.promise:p".to_string())
                ),
                ("harvest.promise:p".to_string(), None),
                (
                    "harvest.promise:p".to_string(),
                    Some("harvest.promise:p".to_string())
                ),
            ],
            "a duplicate legacy settlement keeps no key, so the index builds"
        );
        let dup = conn.execute(
            "INSERT INTO harvest_signals (exec_id, name, payload_json, idempotency_key) \
             VALUES ('e1', 'x', '1', 'harvest.promise:p')",
            [],
        );
        assert!(dup.is_err(), "the unique index holds");
    }

    // Issue #2028: a fresh database has the key column and index at once.
    #[test]
    fn a_fresh_database_enforces_the_signal_key() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        migrate(&conn).unwrap();
        assert!(column_exists(&conn, "harvest_signals", "idempotency_key").unwrap());
        let insert = "INSERT INTO harvest_signals (exec_id, name, payload_json, idempotency_key) \
                      VALUES (?1, 'go', '1', ?2)";
        conn.execute(insert, ["e1", "k"]).unwrap();
        conn.execute(insert, ["e2", "k"]).unwrap();
        conn.execute(insert, rusqlite::params!["e1", None::<String>])
            .unwrap();
        conn.execute(insert, rusqlite::params!["e1", None::<String>])
            .unwrap();
        assert!(conn.execute(insert, ["e1", "k"]).is_err());
    }

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
        assert!(column_exists(&conn, "harvest_executions", "cancel_reason").unwrap());
        assert!(column_exists(&conn, "harvest_executions", "cancel_from_seq").unwrap());
    }

    // Issue #2021: an old `harvest_executions` gets `cancel_reason`. The old row
    // keeps its data and has no pending cancel.
    #[test]
    fn migrate_adds_cancel_reason_to_an_old_executions_table() {
        let conn = old_database();
        conn.execute(
            "INSERT INTO harvest_executions (exec_id, workflow_name, input_json, state) \
             VALUES ('e1', 'wf', '{}', 'RUNNING')",
            [],
        )
        .unwrap();
        assert!(!column_exists(&conn, "harvest_executions", "cancel_reason").unwrap());

        migrate(&conn).unwrap();

        let (state, reason, from): (String, Option<String>, Option<i64>) = conn
            .query_row(
                "SELECT state, cancel_reason, cancel_from_seq FROM harvest_executions \
                 WHERE exec_id = 'e1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(state, "RUNNING");
        assert_eq!(reason, None);
        assert_eq!(from, None);
    }

    /// The six timeout and retry columns of one row (issue #2029).
    type TimeoutRetryRow = (
        Option<i64>,
        Option<i64>,
        Option<i64>,
        i64,
        Option<String>,
        Option<String>,
    );

    // Issue #2029: an old row gains the timeout and retry columns. It has no
    // deadline, no start delay and no policy, and it is attempt 1.
    #[test]
    fn migrate_adds_the_timeout_and_retry_columns() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(OLD_HARVEST_EXECUTIONS_DDL).unwrap();
        conn.execute_batch(OLD_HARVEST_TASKS_DDL).unwrap();
        conn.execute_batch(OLD_HARVEST_SIGNALS_DDL).unwrap();
        conn.execute(
            "INSERT INTO harvest_executions (exec_id, workflow_name, input_json, state) \
             VALUES ('e1', 'wf', '{}', 'RUNNING')",
            [],
        )
        .unwrap();

        migrate(&conn).unwrap();
        migrate(&conn).unwrap();

        let row: TimeoutRetryRow = conn
            .query_row(
                "SELECT execution_timeout_ms, deadline_at, start_at, workflow_attempt, \
                 retry_policy_json, retry_of_exec_id FROM harvest_executions",
                [],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(row, (None, None, None, 1, None, None));
    }

    // Issue #2027: an old `harvest_executions` gains the four visibility
    // columns. The old row keeps its data, and each new column reads NULL.
    #[test]
    fn migrate_adds_the_visibility_columns() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(OLD_HARVEST_EXECUTIONS_DDL).unwrap();
        conn.execute_batch(OLD_HARVEST_TASKS_DDL).unwrap();
        conn.execute_batch(OLD_HARVEST_SIGNALS_DDL).unwrap();
        conn.execute(
            "INSERT INTO harvest_executions (exec_id, workflow_name, input_json, state) \
             VALUES ('e1', 'wf', '{}', 'RUNNING')",
            [],
        )
        .unwrap();

        migrate(&conn).unwrap();
        migrate(&conn).unwrap();

        let row: (
            String,
            Option<i64>,
            Option<i64>,
            Option<String>,
            Option<String>,
        ) = conn
            .query_row(
                "SELECT state, started_at, closed_at, search_attrs, memo \
                 FROM harvest_executions WHERE exec_id = 'e1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .unwrap();
        assert_eq!(row, ("RUNNING".to_string(), None, None, None, None));
        let fresh = Connection::open_in_memory().unwrap();
        fresh.execute_batch(SCHEMA).unwrap();
        for column in ["started_at", "closed_at", "search_attrs", "memo"] {
            assert!(column_exists(&fresh, "harvest_executions", column).unwrap());
        }
    }

    // An old-schema `harvest_tasks` (missing the freeze columns) is brought up to
    // date additively — the two columns are added, existing data is preserved.
    #[test]
    fn migrate_adds_freeze_columns_to_an_old_schema_table() {
        let conn = old_database();
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

        // An open applies `SCHEMA` first. It keeps the old `harvest_tasks`.
        conn.execute_batch(SCHEMA).unwrap();
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

    // Issue #2024: an old table gains the checkpoint and error columns. Both
    // read NULL on an existing row.
    #[test]
    fn migrate_adds_the_heartbeat_and_error_columns() {
        let conn = old_database();
        conn.execute(
            "INSERT INTO harvest_tasks \
             (task_id, exec_id, activity_id, name, input_json, queue, state, attempt, \
              run_at, seq, max_attempts, retry_policy_json, start_to_close_ms, scheduled_at) \
             VALUES ('t1', 'e1', 'a1', 'act', '{}', 'default', 'PENDING', 0, 0, 0, NULL, NULL, NULL, 0)",
            [],
        )
        .unwrap();

        migrate(&conn).unwrap();
        migrate(&conn).unwrap();

        let (details, error): (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT heartbeat_details, last_error FROM harvest_tasks WHERE task_id = 't1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!((details, error), (None, None));
        let fresh = Connection::open_in_memory().unwrap();
        fresh.execute_batch(SCHEMA).unwrap();
        assert!(column_exists(&fresh, "harvest_tasks", "heartbeat_details").unwrap());
        assert!(column_exists(&fresh, "harvest_tasks", "last_error").unwrap());
    }

    // The pre-#2026 `harvest_executions` and `harvest_signals` shapes.
    const OLD_COMPOSITION_DDL: &str = "\
        CREATE TABLE harvest_executions (\
            exec_id TEXT PRIMARY KEY, workflow_name TEXT NOT NULL, \
            workflow_id TEXT NOT NULL DEFAULT '', input_json TEXT NOT NULL, \
            state TEXT NOT NULL, output_json TEXT, error TEXT);\
        CREATE TABLE harvest_signals (\
            signal_seq INTEGER PRIMARY KEY AUTOINCREMENT, exec_id TEXT NOT NULL, \
            name TEXT NOT NULL, payload_json TEXT NOT NULL, \
            delivered INTEGER NOT NULL DEFAULT 0, received_at INTEGER NOT NULL DEFAULT 0);\
        INSERT INTO harvest_executions (exec_id, workflow_name, input_json, state) \
            VALUES ('e1', 'wf', '{}', 'RUNNING');";

    // Issue #2026: an old file gains the parent link and the signal key. Its
    // rows survive, and a pre-#2026 run reads as top-level.
    #[test]
    fn migrate_adds_composition_columns_to_an_old_file() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(OLD_COMPOSITION_DDL).unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        assert!(!column_exists(&conn, "harvest_executions", "parent_exec_id").unwrap());

        migrate(&conn).unwrap();
        migrate(&conn).unwrap();

        for (table, column) in [
            ("harvest_executions", "parent_exec_id"),
            ("harvest_executions", "parent_close_policy"),
            ("harvest_signals", "idempotency_key"),
        ] {
            assert!(
                column_exists(&conn, table, column).unwrap(),
                "{table}.{column}"
            );
        }
        let parent: Option<String> = conn
            .query_row(
                "SELECT parent_exec_id FROM harvest_executions WHERE exec_id = 'e1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(parent, None);
    }
}
