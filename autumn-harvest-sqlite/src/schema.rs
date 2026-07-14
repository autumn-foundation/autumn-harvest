//! Hand-written `SQLite` DDL for the embedded backend.
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
//! - `harvest_timers` — durable timers with an absolute epoch `fire_at`.
//! - `harvest_signals` — staged inbound signals awaiting a matching `wait_for_signal`.
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
    input_json    TEXT NOT NULL,
    state         TEXT NOT NULL,          -- RUNNING | COMPLETED | FAILED
    output_json   TEXT,
    error         TEXT
);

CREATE TABLE IF NOT EXISTS harvest_events (
    exec_id    TEXT NOT NULL,
    seq        INTEGER NOT NULL,
    event_json TEXT NOT NULL,
    PRIMARY KEY (exec_id, seq)
);

CREATE TABLE IF NOT EXISTS harvest_tasks (
    task_id     TEXT PRIMARY KEY,
    exec_id     TEXT NOT NULL,
    activity_id TEXT NOT NULL,            -- the scheduled ActivityExecId
    name        TEXT NOT NULL,
    input_json  TEXT NOT NULL,
    queue       TEXT NOT NULL,
    state       TEXT NOT NULL,            -- PENDING | RUNNING | DONE
    attempt     INTEGER NOT NULL,         -- attempts consumed so far
    run_at      INTEGER NOT NULL,         -- earliest epoch-second this may run
    seq         INTEGER NOT NULL          -- FIFO ordering within the queue
);

CREATE TABLE IF NOT EXISTS harvest_timers (
    timer_id TEXT NOT NULL,
    exec_id  TEXT NOT NULL,
    fire_at  INTEGER NOT NULL,            -- absolute epoch-second
    fired    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (exec_id, timer_id)
);

CREATE TABLE IF NOT EXISTS harvest_signals (
    signal_seq   INTEGER PRIMARY KEY AUTOINCREMENT,
    exec_id      TEXT NOT NULL,
    name         TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    delivered    INTEGER NOT NULL DEFAULT 0
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
