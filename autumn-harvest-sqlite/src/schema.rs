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
    max_attempts INTEGER                   -- per-call retry cap from the command's
                                           -- retry_policy_override (issue #1069 P2);
                                           -- NULL = use the registered ActivitySpec default
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
