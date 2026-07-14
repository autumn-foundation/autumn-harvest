//! Hand-written `SQLite` DDL for the feasibility spike (issue #966).
//!
//! Deliberately **not** the Postgres migration DDL: the dialect differs (TEXT
//! for UUIDs/JSON, INTEGER for sequence numbers and epoch timestamps) and the
//! spike only persists the minimum a single-writer, embedded runtime needs.
//!
//! Tables:
//! - `spike_executions` — one row per workflow run (input + terminal state).
//! - `spike_events` — the append-only [`WorkflowEvent`](crate::event::WorkflowEvent)
//!   log, `(exec_id, seq)`; `event_json` is `serde_json::to_string(&WorkflowEvent)`.
//!   This is the *canonical, replayable* history — byte-identical to what the
//!   Postgres backend would write, which is what makes cross-backend replay
//!   (AC4) hold by construction.
//! - `spike_tasks` — the activity task queue (the `SKIP LOCKED` analog; single
//!   writer, see [`queue`](super::queue)).
//! - `spike_timers` — durable timers with an absolute epoch `fire_at`.
//! - `spike_signals` — staged inbound signals awaiting a matching `wait_for_signal`.
//! - `spike_activity_attempts` — a per-attempt audit log. Retryable activity
//!   failures live here, **not** in `spike_events`, mirroring the PG engine's
//!   `requeue_for_retry` (which stores the attempt error on the task-queue row,
//!   not `harvest_events`). Keeping every persisted event history a clean,
//!   terminal-only, replay-correct log is what lets AC4 pass without filtering.

/// The full schema, applied idempotently on [`SqliteRuntime::open`](super::SqliteRuntime::open).
pub(super) const SCHEMA: &str = r"
CREATE TABLE IF NOT EXISTS spike_executions (
    exec_id       TEXT PRIMARY KEY,
    workflow_name TEXT NOT NULL,
    input_json    TEXT NOT NULL,
    state         TEXT NOT NULL,          -- RUNNING | COMPLETED | FAILED
    output_json   TEXT,
    error         TEXT
);

CREATE TABLE IF NOT EXISTS spike_events (
    exec_id    TEXT NOT NULL,
    seq        INTEGER NOT NULL,
    event_json TEXT NOT NULL,
    PRIMARY KEY (exec_id, seq)
);

CREATE TABLE IF NOT EXISTS spike_tasks (
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

CREATE TABLE IF NOT EXISTS spike_timers (
    timer_id TEXT NOT NULL,
    exec_id  TEXT NOT NULL,
    fire_at  INTEGER NOT NULL,            -- absolute epoch-second
    fired    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (exec_id, timer_id)
);

CREATE TABLE IF NOT EXISTS spike_signals (
    signal_seq   INTEGER PRIMARY KEY AUTOINCREMENT,
    exec_id      TEXT NOT NULL,
    name         TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    delivered    INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS spike_activity_attempts (
    attempt_seq INTEGER PRIMARY KEY AUTOINCREMENT,
    exec_id     TEXT NOT NULL,
    name        TEXT NOT NULL,
    attempt     INTEGER NOT NULL,
    ok          INTEGER NOT NULL,         -- 1 = success, 0 = failure
    detail      TEXT NOT NULL             -- output JSON on success, error on failure
);
";
