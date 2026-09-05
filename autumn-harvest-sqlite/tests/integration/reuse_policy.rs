//! HARDENING ITEM 2 (issue #1068): **`(workflow_name, workflow_id)` start-boundary
//! reuse-policy matrix.**
//!
//! Before this change `start_workflow_with_id` ALWAYS minted a fresh
//! [`ExecutionId`] — no `(workflow_name, workflow_id)` lookup, no uniqueness, no
//! reuse policy — so a duplicate delivery (a retried webhook) started a second,
//! independent run and repeated its side effects. This backend now mirrors the
//! Postgres core PLAIN-start matrix in the single-writer start transaction:
//! `start_workflow_with_id` is idempotent-by-default (`AllowDuplicate`), and
//! `start_workflow_with_reuse_policy` exposes the full four-policy matrix.
//!
//! The core PLAIN-start matrix (restricted to the states this backend can hold —
//! none / RUNNING / COMPLETED / FAILED, with a replaced prior SEALED to
//! `CONTINUED_AS_NEW`):
//!
//! | Prior state | AllowDuplicate | RejectDuplicate | AllowDuplicateFailedOnly | TerminateIfRunning |
//! |-------------|----------------|-----------------|--------------------------|--------------------|
//! | none        | fresh          | fresh           | fresh                    | fresh              |
//! | RUNNING     | existing       | AlreadyExists   | existing                 | cancel + fresh     |
//! | COMPLETED   | existing       | AlreadyExists   | existing                 | fresh (no cancel)  |
//! | FAILED      | existing       | AlreadyExists   | **fresh (replace)**      | fresh (no cancel)  |
//!
//! A prior in a SEALED state (`CONTINUED_AS_NEW`/`TERMINATED`) is invisible to the
//! reuse check, so ALL policies (incl. `RejectDuplicate`) create fresh over it —
//! mirroring core's uniqueness-index-excludes-terminal nuance.
//!
//! No Docker: `SQLite` is embedded (`rusqlite` `bundled`); prior runs are driven
//! to RUNNING / COMPLETED / FAILED with the in-process drivers, and the reuse
//! decision is a pure application-code branch in the start transaction — nothing
//! waits on wall time.

// The `#[workflow]` macro references its input params, so a deliberately-unused
// param reads as a "used underscore binding" through expansion.
#![allow(clippy::used_underscore_binding)]
#![allow(clippy::unused_async)]
// This module's doc comments and the matrix table describe the reuse-policy
// variants (`AllowDuplicate`, `RejectDuplicate`, …) in prose; backticking every
// occurrence in test-fixture docs is noise.
#![allow(clippy::doc_markdown)]

use autumn_harvest::WorkflowIdReusePolicy;
use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::{ExecutionOutcome, RunState, SqliteError, SqliteRuntime, StartOutcome};
use serde_json::json;

// ── Shared helpers ─────────────────────────────────────────────────────────────

fn temp_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("harvest.sqlite3");
    (dir, path)
}

/// Count of `harvest_executions` rows for a `(workflow_name, workflow_id)` key
/// (any state), read from a raw connection on the same file.
fn rows_for_key(path: &std::path::Path, name: &str, id: &str) -> i64 {
    let raw = rusqlite::Connection::open(path).unwrap();
    raw.query_row(
        "SELECT COUNT(*) FROM harvest_executions WHERE workflow_name = ?1 AND workflow_id = ?2",
        rusqlite::params![name, id],
        |row| row.get(0),
    )
    .unwrap()
}

/// Count of ACTIVE (non-sealed, non-terminal) rows for the key.
fn active_rows_for_key(path: &std::path::Path, name: &str, id: &str) -> i64 {
    let raw = rusqlite::Connection::open(path).unwrap();
    raw.query_row(
        "SELECT COUNT(*) FROM harvest_executions \
         WHERE workflow_name = ?1 AND workflow_id = ?2 \
           AND state NOT IN ('CONTINUED_AS_NEW','TERMINATED')",
        rusqlite::params![name, id],
        |row| row.get(0),
    )
    .unwrap()
}

/// The stored `state` column of a single execution, from a raw connection.
fn raw_state(path: &std::path::Path, exec: ExecutionId) -> String {
    let raw = rusqlite::Connection::open(path).unwrap();
    raw.query_row(
        "SELECT state FROM harvest_executions WHERE exec_id = ?1",
        rusqlite::params![exec.to_string()],
        |row| row.get(0),
    )
    .unwrap()
}

/// Number of unfired timer rows for an execution.
fn unfired_timer_count(path: &std::path::Path, exec: ExecutionId) -> i64 {
    let raw = rusqlite::Connection::open(path).unwrap();
    raw.query_row(
        "SELECT COUNT(*) FROM harvest_timers WHERE exec_id = ?1 AND fired = 0",
        rusqlite::params![exec.to_string()],
        |row| row.get(0),
    )
    .unwrap()
}

/// Number of PENDING task rows for an execution.
fn pending_task_count(path: &std::path::Path, exec: ExecutionId) -> i64 {
    let raw = rusqlite::Connection::open(path).unwrap();
    raw.query_row(
        "SELECT COUNT(*) FROM harvest_tasks WHERE exec_id = ?1 AND state = 'PENDING'",
        rusqlite::params![exec.to_string()],
        |row| row.get(0),
    )
    .unwrap()
}

/// Number of undelivered (`delivered = 0`) staged signal rows for an execution.
fn undelivered_signal_count(path: &std::path::Path, exec: ExecutionId) -> i64 {
    let raw = rusqlite::Connection::open(path).unwrap();
    raw.query_row(
        "SELECT COUNT(*) FROM harvest_signals WHERE exec_id = ?1 AND delivered = 0",
        rusqlite::params![exec.to_string()],
        |row| row.get(0),
    )
    .unwrap()
}

/// Directly seed a pre-#1068 RUNNING `harvest_executions` row (bypassing the reuse
/// matrix, which the public start path now enforces) for `(name, id)` on the file
/// at `path`, plus its `WorkflowStarted` event so a driver can replay/run it, plus
/// one orphan PENDING task and one unfired timer so cleanup is observable. Returns
/// the minted `exec_id`. Used to reconstruct the "multiple RUNNING duplicates for
/// one key" state a database written by the pre-#1068 always-fresh
/// `start_workflow_with_id` could already hold.
fn seed_legacy_running_row(path: &std::path::Path, name: &str, id: &str) -> ExecutionId {
    let raw = rusqlite::Connection::open(path).unwrap();
    let exec = ExecutionId::new();
    raw.execute(
        "INSERT INTO harvest_executions (exec_id, workflow_name, workflow_id, input_json, state) \
         VALUES (?1, ?2, ?3, '{}', 'RUNNING')",
        rusqlite::params![exec.to_string(), name, id],
    )
    .unwrap();
    // A WorkflowStarted event so drive_one_cycle can replay/run the handler.
    let started = WorkflowEvent::workflow_started(json!({}), chrono::Utc::now());
    raw.execute(
        "INSERT INTO harvest_events (exec_id, seq, event_json) VALUES (?1, 0, ?2)",
        rusqlite::params![exec.to_string(), serde_json::to_string(&started).unwrap()],
    )
    .unwrap();
    // An orphan PENDING task + an unfired timer, so the TerminateIfRunning cleanup
    // (`delete_pending_tasks_for_execution` / `delete_unfired_timers_for_execution`)
    // is observable per prior.
    raw.execute(
        "INSERT INTO harvest_tasks \
         (task_id, exec_id, activity_id, name, input_json, queue, state, attempt, run_at, seq, \
          scheduled_at, defaults_frozen) \
         VALUES (?1, ?2, ?3, 'orphan', '{}', 'default', 'PENDING', 0, 0, 0, 0, 0)",
        rusqlite::params![
            format!("orphan-task-{exec}"),
            exec.to_string(),
            ActivityExecId::new().to_string(),
        ],
    )
    .unwrap();
    raw.execute(
        "INSERT INTO harvest_timers (timer_id, exec_id, fire_at, fired, arm_seq) \
         VALUES ('legacy-park', ?1, 9_999_999_999_999, 0, 0)",
        rusqlite::params![exec.to_string()],
    )
    .unwrap();
    exec
}

/// True iff the execution's history contains a `WorkflowCancelled` event.
fn has_cancelled_event(rt: &SqliteRuntime, exec: ExecutionId) -> bool {
    rt.load_history(exec)
        .unwrap()
        .iter()
        .any(|e| matches!(e, WorkflowEvent::WorkflowCancelled { .. }))
}

// ── Fixture workflows ───────────────────────────────────────────────────────────

// Completes immediately with its echoed input.
#[workflow]
async fn echo_wf(
    _ctx: &WorkflowContext,
    input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    Ok(input)
}

// Fails immediately.
#[workflow]
async fn fail_wf(
    _ctx: &WorkflowContext,
    _input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    Err("boom".to_string())
}

// Blocks awaiting a signal — the run stays RUNNING.
#[workflow]
async fn wait_wf(
    ctx: &WorkflowContext,
    _input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let v = ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    Ok(v)
}

// Arms a far-future durable timer and blocks (WaitingTimer) — leaves an unfired
// `harvest_timers` row, so a TerminateIfRunning replace must clean it up.
#[workflow]
async fn timer_wait_wf(
    ctx: &WorkflowContext,
    _input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    ctx.timer("park", 3600).await.map_err(|e| e.to_string())?;
    Ok(json!("woke"))
}

/// Register all fixtures on a fresh runtime.
fn runtime_at(path: &std::path::Path) -> SqliteRuntime {
    let mut rt = SqliteRuntime::open(path).unwrap();
    rt.register_workflow(&echo_wf_info());
    rt.register_workflow(&fail_wf_info());
    rt.register_workflow(&wait_wf_info());
    rt.register_workflow(&timer_wait_wf_info());
    rt
}

/// Start a prior run under `wf` + `id` and drive it to a target state, returning
/// its `exec_id`. `wait_wf` blocks (RUNNING); `echo_wf` completes; `fail_wf` fails.
async fn seed_prior(rt: &mut SqliteRuntime, wf: &str, id: &str) -> ExecutionId {
    let exec = rt
        .start_workflow_with_id(wf, id, json!({"seed": true}))
        .unwrap();
    // Drive once; the terminal fixtures reach COMPLETED/FAILED, wait_wf blocks.
    let _ = rt.run_until_blocked(exec).await;
    exec
}

// ── RED headline: duplicate delivery attaches, does NOT duplicate ───────────────

/// The reviewer's headline scenario: two `start_workflow_with_id` calls with the
/// SAME business id return the SAME `exec_id` and leave exactly ONE row for the
/// key. RED against the current always-fresh behavior (two distinct execs, two
/// rows). This is the falsifiable test.
#[tokio::test]
async fn duplicate_delivery_attaches_not_duplicates() {
    let (_dir, path) = temp_db();
    let mut rt = runtime_at(&path);

    let first = rt
        .start_workflow_with_id("wait_wf", "biz-1", json!({"n": 1}))
        .unwrap();
    // Leave the first run RUNNING (it blocks on "go").
    let _ = rt.run_until_blocked(first).await;

    let second = rt
        .start_workflow_with_id("wait_wf", "biz-1", json!({"n": 2}))
        .unwrap();

    assert_eq!(
        first, second,
        "a duplicate delivery must ATTACH to the existing run, not start a second one"
    );
    assert_eq!(
        rows_for_key(&path, "wait_wf", "biz-1"),
        1,
        "exactly one execution row must exist for the (workflow_name, workflow_id) key"
    );
}

// ── none → create fresh (all four policies) ─────────────────────────────────────

#[tokio::test]
async fn no_prior_creates_fresh_for_every_policy() {
    for (i, policy) in [
        WorkflowIdReusePolicy::AllowDuplicate,
        WorkflowIdReusePolicy::RejectDuplicate,
        WorkflowIdReusePolicy::AllowDuplicateFailedOnly,
        WorkflowIdReusePolicy::TerminateIfRunning,
    ]
    .into_iter()
    .enumerate()
    {
        let (_dir, path) = temp_db();
        let mut rt = runtime_at(&path);
        let id = format!("fresh-{i}");
        let out: StartOutcome = rt
            .start_workflow_with_reuse_policy("wait_wf", &id, json!({}), policy)
            .unwrap();
        assert!(
            out.created,
            "{policy:?}: a start with no prior must create fresh"
        );
        assert_eq!(active_rows_for_key(&path, "wait_wf", &id), 1);
    }
}

// ── RUNNING prior ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn running_prior_allow_duplicate_returns_existing() {
    let (_dir, path) = temp_db();
    let mut rt = runtime_at(&path);
    let prior = seed_prior(&mut rt, "wait_wf", "r-1").await;
    assert_eq!(raw_state(&path, prior), "RUNNING");

    let out = rt
        .start_workflow_with_reuse_policy(
            "wait_wf",
            "r-1",
            json!({}),
            WorkflowIdReusePolicy::AllowDuplicate,
        )
        .unwrap();
    assert_eq!(out.exec_id, prior);
    assert!(
        !out.created,
        "AllowDuplicate over RUNNING prior returns existing (created=false)"
    );
    assert_eq!(rows_for_key(&path, "wait_wf", "r-1"), 1);
}

#[tokio::test]
async fn running_prior_reject_duplicate_errors() {
    let (_dir, path) = temp_db();
    let mut rt = runtime_at(&path);
    let prior = seed_prior(&mut rt, "wait_wf", "r-2").await;

    let err = rt
        .start_workflow_with_reuse_policy(
            "wait_wf",
            "r-2",
            json!({}),
            WorkflowIdReusePolicy::RejectDuplicate,
        )
        .unwrap_err();
    match err {
        SqliteError::AlreadyExists {
            existing_exec_id,
            existing_state,
        } => {
            assert_eq!(existing_exec_id, prior);
            assert_eq!(existing_state, "RUNNING");
        }
        other => panic!("expected AlreadyExists, got {other:?}"),
    }
    // No fresh row was written.
    assert_eq!(rows_for_key(&path, "wait_wf", "r-2"), 1);
}

#[tokio::test]
async fn running_prior_allow_duplicate_failed_only_returns_existing() {
    let (_dir, path) = temp_db();
    let mut rt = runtime_at(&path);
    let prior = seed_prior(&mut rt, "wait_wf", "r-3").await;

    let out = rt
        .start_workflow_with_reuse_policy(
            "wait_wf",
            "r-3",
            json!({}),
            WorkflowIdReusePolicy::AllowDuplicateFailedOnly,
        )
        .unwrap();
    assert_eq!(out.exec_id, prior);
    assert!(
        !out.created,
        "AllowDuplicateFailedOnly over RUNNING prior returns existing"
    );
    assert_eq!(rows_for_key(&path, "wait_wf", "r-3"), 1);
}

#[tokio::test]
async fn running_prior_terminate_if_running_cancels_and_starts_fresh() {
    let (_dir, path) = temp_db();
    let mut rt = runtime_at(&path);
    let prior = seed_prior(&mut rt, "wait_wf", "r-4").await;
    assert_eq!(raw_state(&path, prior), "RUNNING");

    let out = rt
        .start_workflow_with_reuse_policy(
            "wait_wf",
            "r-4",
            json!({}),
            WorkflowIdReusePolicy::TerminateIfRunning,
        )
        .unwrap();
    assert!(
        out.created,
        "TerminateIfRunning over RUNNING prior starts fresh"
    );
    assert_ne!(out.exec_id, prior, "a NEW exec is created");
    // Prior sealed to CONTINUED_AS_NEW (invisible to the active lookup).
    assert_eq!(raw_state(&path, prior), "CONTINUED_AS_NEW");
    // Prior's history records the forced cancellation.
    assert!(
        has_cancelled_event(&rt, prior),
        "the cancelled prior must record a WorkflowCancelled event"
    );
    // Exactly one ACTIVE run for the key (the new one); the sealed prior is invisible.
    assert_eq!(active_rows_for_key(&path, "wait_wf", "r-4"), 1);
    assert_eq!(raw_state(&path, out.exec_id), "RUNNING");
}

/// TerminateIfRunning over a RUNNING prior with an unfired timer cleans up the
/// orphan timer row (no leftover rows that could fire against a sealed run).
#[tokio::test]
async fn terminate_if_running_cleans_up_orphan_timer() {
    let (_dir, path) = temp_db();
    let mut rt = runtime_at(&path);
    let prior = rt
        .start_workflow_with_id("timer_wait_wf", "tt-1", json!({}))
        .unwrap();
    let state = rt.run_until_blocked(prior).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingTimer),
        "prior blocks on the timer"
    );
    assert_eq!(
        unfired_timer_count(&path, prior),
        1,
        "prior has one unfired timer"
    );

    let out = rt
        .start_workflow_with_reuse_policy(
            "timer_wait_wf",
            "tt-1",
            json!({}),
            WorkflowIdReusePolicy::TerminateIfRunning,
        )
        .unwrap();
    assert!(out.created);
    assert_eq!(raw_state(&path, prior), "CONTINUED_AS_NEW");
    assert!(has_cancelled_event(&rt, prior));
    assert_eq!(
        unfired_timer_count(&path, prior),
        0,
        "the sealed prior's unfired timer must be cleaned up"
    );
}

// 🪝 Snag: `TerminateIfRunning` cleaned up an orphan PENDING task and an orphan
// unfired timer for the sealed prior (see `terminate_if_running_cleans_up_orphan_timer`
// above), but had no `harvest_signals` counterpart — `cancel_and_seal_prior` called
// `delete_pending_tasks_for_execution` and `delete_unfired_timers_for_execution` but
// nothing for staged signals. Unlike a timer, an orphaned signal row can never fire
// against anything (the sealed prior is never driven again), so this was not a
// correctness hazard — but the sqlite backend has no retention/GC pass at all
// (README `v0.1 non-goals`: "Schedules, the management API, DAGs, worker sessions,
// retention, sharding"), unlike the Postgres core, which purges old rows via a
// separate retention subsystem. So on this backend a signal sent to a still-RUNNING
// execution that the workflow never reaches a matching
// `wait_for_signal`/`wait_for_signal_timeout` for before being replaced via
// `TerminateIfRunning` stayed in `harvest_signals` with `delivered = 0` forever, with
// no code path anywhere in this crate that ever removed it — repeating the
// send-signal-then-replace cycle against the same `(workflow_name, workflow_id)` grew
// the table without bound. Fixed by `delete_undelivered_signals_for_execution`,
// called alongside the existing task/timer cleanup in `cancel_and_seal_prior`. RED
// against the pre-fix code: `total_orphaned` was 3 (one per cycle), not 0.
#[tokio::test]
async fn terminate_if_running_orphans_an_undelivered_signal_across_repeated_cycles() {
    let (_dir, path) = temp_db();
    let mut rt = runtime_at(&path);

    let mut priors = Vec::new();
    for cycle in 0..3 {
        let prior = rt
            .start_workflow_with_id("timer_wait_wf", "sig-1", json!({}))
            .unwrap();
        let state = rt.run_until_blocked(prior).await.unwrap();
        assert!(
            matches!(state, RunState::WaitingTimer),
            "prior #{cycle} blocks on the timer, never on this signal"
        );
        // A caller sends a signal the workflow's code never awaits before it is
        // replaced — e.g. a stale webhook retry, or a signal sent to the wrong
        // step of a long-running workflow.
        rt.send_signal(prior, "never_consumed", json!({"cycle": cycle}))
            .unwrap();
        assert_eq!(
            undelivered_signal_count(&path, prior),
            1,
            "prior #{cycle} has one staged, undelivered signal"
        );

        let out = rt
            .start_workflow_with_reuse_policy(
                "timer_wait_wf",
                "sig-1",
                json!({}),
                WorkflowIdReusePolicy::TerminateIfRunning,
            )
            .unwrap();
        assert!(out.created);
        assert_eq!(raw_state(&path, prior), "CONTINUED_AS_NEW");
        priors.push(prior);
    }

    let total_orphaned: i64 = priors
        .iter()
        .map(|&exec| undelivered_signal_count(&path, exec))
        .sum();
    assert_eq!(
        total_orphaned,
        0,
        "every sealed prior's undelivered signal must be cleaned up, not left to \
         accumulate forever with no retention pass to ever reclaim it (got {total_orphaned} \
         orphaned rows across {} cycles)",
        priors.len()
    );
}

// ── FINDING (Codex #1080 P2): TerminateIfRunning seals ALL legacy active rows ─────
//
// A database written by the PRE-#1068 always-fresh `start_workflow_with_id` can
// already hold MULTIPLE `RUNNING` rows for one `(workflow_name, workflow_id)` key.
// `TerminateIfRunning` must seal/cancel EVERY active row for the key (not just the
// newest, which `find_active_execution_by_key`'s `ORDER BY rowid DESC LIMIT 1`
// returns), so no orphaned RUNNING prior survives to be driven by `run_until_idle`,
// defeating the terminate-and-restart guarantee for upgraded databases.

/// Seed THREE legacy RUNNING duplicates for one key (each with a WorkflowStarted
/// event, an orphan pending task, and an unfired timer), then run
/// `TerminateIfRunning`: EVERY prior must be sealed `CONTINUED_AS_NEW` with a
/// `WorkflowCancelled` event and its orphan task/timer released, exactly ONE
/// `RUNNING` row (the fresh run) must remain, and `run_until_idle` must drive ONLY
/// the fresh run — never re-driving the sealed priors.
///
/// RED against the pre-fix code: it seals only the NEWEST prior, leaving the two
/// older RUNNING duplicates active — so their state stays `RUNNING` (not
/// `CONTINUED_AS_NEW`) and `run_until_idle` drives them to `COMPLETED`.
#[tokio::test]
async fn terminate_if_running_seals_all_legacy_active_duplicates() {
    let (_dir, path) = temp_db();
    // Open the runtime first so the schema (and WAL) is applied to the file before
    // the raw seeding connections write to it.
    let mut rt = runtime_at(&path);

    // Reconstruct a pre-#1068 database holding THREE RUNNING rows for one key. The
    // public start path now dedups, so it cannot create duplicates — seed them
    // directly. Insertion order == rowid order, so `execs[2]` is the newest (the
    // only one the pre-fix single-row lookup would seal). `echo_wf` COMPLETES when
    // driven, so a prior left RUNNING is observably driven to COMPLETED by
    // `run_until_idle` (a sealed prior is not driven at all).
    let key_id = "legacy-dup";
    let execs: Vec<ExecutionId> = (0..3)
        .map(|_| seed_legacy_running_row(&path, "echo_wf", key_id))
        .collect();
    for &e in &execs {
        assert_eq!(raw_state(&path, e), "RUNNING");
        assert_eq!(pending_task_count(&path, e), 1);
        assert_eq!(unfired_timer_count(&path, e), 1);
    }
    assert_eq!(
        active_rows_for_key(&path, "echo_wf", key_id),
        3,
        "three legacy active duplicates exist for the key"
    );

    let out = rt
        .start_workflow_with_reuse_policy(
            "echo_wf",
            key_id,
            json!({}),
            WorkflowIdReusePolicy::TerminateIfRunning,
        )
        .unwrap();
    assert!(out.created, "TerminateIfRunning starts fresh");
    assert!(!execs.contains(&out.exec_id), "a NEW exec is created");

    // EVERY legacy duplicate is sealed, cancelled, and its orphans cleaned up.
    for &e in &execs {
        assert_eq!(
            raw_state(&path, e),
            "CONTINUED_AS_NEW",
            "every legacy RUNNING duplicate must be sealed (not just the newest)"
        );
        assert!(
            has_cancelled_event(&rt, e),
            "each formerly-RUNNING prior records a WorkflowCancelled event"
        );
        assert_eq!(
            pending_task_count(&path, e),
            0,
            "each prior's pending task is released"
        );
        assert_eq!(
            unfired_timer_count(&path, e),
            0,
            "each prior's unfired timer is released"
        );
    }
    assert_eq!(
        active_rows_for_key(&path, "echo_wf", key_id),
        1,
        "exactly one ACTIVE run (the fresh one) remains"
    );

    // `run_until_idle` drives ONLY the fresh run: the sealed priors are filtered out
    // of the running set and never re-driven (pre-fix, the two older RUNNING
    // duplicates would be driven to COMPLETED here).
    rt.run_until_idle().await.unwrap();
    for &e in &execs {
        assert_eq!(
            raw_state(&path, e),
            "CONTINUED_AS_NEW",
            "a sealed prior must never be driven by run_until_idle"
        );
    }
    assert_eq!(
        raw_state(&path, out.exec_id),
        "COMPLETED",
        "only the fresh run is driven (echo_wf completes)"
    );
}

// ── COMPLETED prior ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn completed_prior_allow_duplicate_returns_existing() {
    let (_dir, path) = temp_db();
    let mut rt = runtime_at(&path);
    let prior = seed_prior(&mut rt, "echo_wf", "c-1").await;
    assert_eq!(raw_state(&path, prior), "COMPLETED");

    let out = rt
        .start_workflow_with_reuse_policy(
            "echo_wf",
            "c-1",
            json!({}),
            WorkflowIdReusePolicy::AllowDuplicate,
        )
        .unwrap();
    assert_eq!(out.exec_id, prior);
    assert!(!out.created);
}

#[tokio::test]
async fn completed_prior_reject_duplicate_errors() {
    let (_dir, path) = temp_db();
    let mut rt = runtime_at(&path);
    let prior = seed_prior(&mut rt, "echo_wf", "c-2").await;

    let err = rt
        .start_workflow_with_reuse_policy(
            "echo_wf",
            "c-2",
            json!({}),
            WorkflowIdReusePolicy::RejectDuplicate,
        )
        .unwrap_err();
    match err {
        SqliteError::AlreadyExists { existing_state, .. } => {
            assert_eq!(existing_state, "COMPLETED");
        }
        other => panic!("expected AlreadyExists, got {other:?}"),
    }
    let _ = prior;
}

#[tokio::test]
async fn completed_prior_allow_duplicate_failed_only_returns_existing_not_replaced() {
    let (_dir, path) = temp_db();
    let mut rt = runtime_at(&path);
    let prior = seed_prior(&mut rt, "echo_wf", "c-3").await;

    let out = rt
        .start_workflow_with_reuse_policy(
            "echo_wf",
            "c-3",
            json!({}),
            WorkflowIdReusePolicy::AllowDuplicateFailedOnly,
        )
        .unwrap();
    assert_eq!(out.exec_id, prior, "a COMPLETED prior is NOT replaced");
    assert!(!out.created);
    assert_eq!(
        raw_state(&path, prior),
        "COMPLETED",
        "prior stays COMPLETED (not sealed)"
    );
}

#[tokio::test]
async fn completed_prior_terminate_if_running_starts_fresh_no_cancel_event() {
    let (_dir, path) = temp_db();
    let mut rt = runtime_at(&path);
    let prior = seed_prior(&mut rt, "echo_wf", "c-4").await;

    let out = rt
        .start_workflow_with_reuse_policy(
            "echo_wf",
            "c-4",
            json!({}),
            WorkflowIdReusePolicy::TerminateIfRunning,
        )
        .unwrap();
    assert!(out.created);
    assert_ne!(out.exec_id, prior);
    // Already-terminal prior is sealed but NOT given a cancellation event.
    assert_eq!(raw_state(&path, prior), "CONTINUED_AS_NEW");
    assert!(
        !has_cancelled_event(&rt, prior),
        "an already-terminal prior must NOT receive a WorkflowCancelled event"
    );
    assert_eq!(active_rows_for_key(&path, "echo_wf", "c-4"), 1);
}

// ── FAILED prior ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn failed_prior_allow_duplicate_returns_existing() {
    let (_dir, path) = temp_db();
    let mut rt = runtime_at(&path);
    let prior = seed_prior(&mut rt, "fail_wf", "f-1").await;
    assert_eq!(raw_state(&path, prior), "FAILED");

    let out = rt
        .start_workflow_with_reuse_policy(
            "fail_wf",
            "f-1",
            json!({}),
            WorkflowIdReusePolicy::AllowDuplicate,
        )
        .unwrap();
    assert_eq!(out.exec_id, prior);
    assert!(!out.created);
}

#[tokio::test]
async fn failed_prior_reject_duplicate_errors() {
    let (_dir, path) = temp_db();
    let mut rt = runtime_at(&path);
    let _prior = seed_prior(&mut rt, "fail_wf", "f-2").await;

    let err = rt
        .start_workflow_with_reuse_policy(
            "fail_wf",
            "f-2",
            json!({}),
            WorkflowIdReusePolicy::RejectDuplicate,
        )
        .unwrap_err();
    match err {
        SqliteError::AlreadyExists { existing_state, .. } => {
            assert_eq!(existing_state, "FAILED");
        }
        other => panic!("expected AlreadyExists, got {other:?}"),
    }
}

/// The key `AllowDuplicateFailedOnly` divergence: a FAILED prior IS replaced.
#[tokio::test]
async fn failed_prior_allow_duplicate_failed_only_replaces() {
    let (_dir, path) = temp_db();
    let mut rt = runtime_at(&path);
    let prior = seed_prior(&mut rt, "fail_wf", "f-3").await;
    assert_eq!(raw_state(&path, prior), "FAILED");

    let out = rt
        .start_workflow_with_reuse_policy(
            "fail_wf",
            "f-3",
            json!({}),
            WorkflowIdReusePolicy::AllowDuplicateFailedOnly,
        )
        .unwrap();
    assert!(
        out.created,
        "a FAILED prior is REPLACED under AllowDuplicateFailedOnly"
    );
    assert_ne!(out.exec_id, prior);
    assert_eq!(
        raw_state(&path, prior),
        "CONTINUED_AS_NEW",
        "the FAILED prior is sealed"
    );
    assert_eq!(active_rows_for_key(&path, "fail_wf", "f-3"), 1);
}

#[tokio::test]
async fn failed_prior_terminate_if_running_starts_fresh_no_cancel_event() {
    let (_dir, path) = temp_db();
    let mut rt = runtime_at(&path);
    let prior = seed_prior(&mut rt, "fail_wf", "f-4").await;

    let out = rt
        .start_workflow_with_reuse_policy(
            "fail_wf",
            "f-4",
            json!({}),
            WorkflowIdReusePolicy::TerminateIfRunning,
        )
        .unwrap();
    assert!(out.created);
    assert_ne!(out.exec_id, prior);
    assert_eq!(raw_state(&path, prior), "CONTINUED_AS_NEW");
    assert!(
        !has_cancelled_event(&rt, prior),
        "an already-terminal prior must NOT receive a WorkflowCancelled event"
    );
}

// ── Sealed-prior invisibility ───────────────────────────────────────────────────

/// After a replace, a subsequent AllowDuplicate start attaches to the NEW active
/// run, never the sealed prior.
#[tokio::test]
async fn sealed_prior_is_invisible_to_subsequent_attach() {
    let (_dir, path) = temp_db();
    let mut rt = runtime_at(&path);
    let prior = seed_prior(&mut rt, "wait_wf", "s-1").await;

    // Replace the RUNNING prior.
    let replaced = rt
        .start_workflow_with_reuse_policy(
            "wait_wf",
            "s-1",
            json!({}),
            WorkflowIdReusePolicy::TerminateIfRunning,
        )
        .unwrap();
    assert!(replaced.created);
    let _ = rt.run_until_blocked(replaced.exec_id).await; // keep it RUNNING

    // A subsequent AllowDuplicate attaches to the NEW active run, not the sealed prior.
    let out = rt
        .start_workflow_with_reuse_policy(
            "wait_wf",
            "s-1",
            json!({}),
            WorkflowIdReusePolicy::AllowDuplicate,
        )
        .unwrap();
    assert_eq!(out.exec_id, replaced.exec_id);
    assert_ne!(out.exec_id, prior);
    assert!(!out.created);
}

/// RejectDuplicate over a key whose ONLY prior is SEALED (`CONTINUED_AS_NEW`)
/// creates fresh rather than erroring — the sealed row is invisible to the reuse
/// lookup (mirrors core's uniqueness-index-excludes-terminal). The sealed-only
/// state (no active row for the key) is set up directly via a raw seal, since
/// every public "replace" path pairs a seal with a fresh start and so always leaves
/// an active row behind.
#[tokio::test]
async fn reject_duplicate_over_a_sealed_only_prior_creates_fresh() {
    let (_dir, path) = temp_db();
    let mut rt = runtime_at(&path);
    let prior = seed_prior(&mut rt, "echo_wf", "s-2").await;
    assert_eq!(raw_state(&path, prior), "COMPLETED");

    // Seal the prior directly, leaving the key with ONLY a sealed row (no active
    // run) — the precise "uniqueness-index-excludes-terminal" precondition.
    {
        let raw = rusqlite::Connection::open(&path).unwrap();
        raw.execute(
            "UPDATE harvest_executions SET state = 'CONTINUED_AS_NEW' WHERE exec_id = ?1",
            rusqlite::params![prior.to_string()],
        )
        .unwrap();
    }
    assert_eq!(
        active_rows_for_key(&path, "echo_wf", "s-2"),
        0,
        "no active row remains"
    );

    // RejectDuplicate must NOT see the sealed prior — it creates a fresh run.
    let out = rt
        .start_workflow_with_reuse_policy(
            "echo_wf",
            "s-2",
            json!({}),
            WorkflowIdReusePolicy::RejectDuplicate,
        )
        .unwrap();
    assert!(
        out.created,
        "RejectDuplicate over a sealed-only prior creates fresh"
    );
    assert_ne!(out.exec_id, prior);
    assert_eq!(active_rows_for_key(&path, "echo_wf", "s-2"), 1);
}

// ── `start_workflow` (no id) is unaffected — always distinct execs ──────────────

#[tokio::test]
async fn start_workflow_without_id_always_distinct() {
    let (_dir, path) = temp_db();
    let mut rt = runtime_at(&path);
    let a = rt.start_workflow("wait_wf", json!({})).unwrap();
    let b = rt.start_workflow("wait_wf", json!({})).unwrap();
    assert_ne!(a, b, "no-id starts always create distinct executions");
}

// ── `start_workflow_with_id` default is idempotent (AllowDuplicate) ─────────────

#[tokio::test]
async fn start_workflow_with_id_default_is_idempotent() {
    let (_dir, path) = temp_db();
    let mut rt = runtime_at(&path);
    let a = rt
        .start_workflow_with_id("wait_wf", "d-1", json!({}))
        .unwrap();
    let _ = rt.run_until_blocked(a).await;
    let b = rt
        .start_workflow_with_id("wait_wf", "d-1", json!({}))
        .unwrap();
    assert_eq!(
        a, b,
        "start_workflow_with_id defaults to AllowDuplicate (attach)"
    );
}

// ── driver safety: a sealed exec is terminal, never re-driven ───────────────────

#[tokio::test]
async fn driving_a_sealed_execution_is_terminal() {
    let (_dir, path) = temp_db();
    let mut rt = runtime_at(&path);
    let prior = seed_prior(&mut rt, "wait_wf", "seal-drive").await;
    let _ = rt
        .start_workflow_with_reuse_policy(
            "wait_wf",
            "seal-drive",
            json!({}),
            WorkflowIdReusePolicy::TerminateIfRunning,
        )
        .unwrap();
    assert_eq!(raw_state(&path, prior), "CONTINUED_AS_NEW");
    // Directly driving the sealed prior must short-circuit to a terminal RunState,
    // not attempt to re-run its handler.
    let state = rt.run_until_blocked(prior).await.unwrap();
    assert!(
        matches!(state, RunState::Failed(_)),
        "a sealed execution drives to a terminal state, got {state:?}"
    );
}

// ── FINDING 2 (Codex #1080 P2): outcome() of a superseded prior is TERMINAL ───────
//
// When a reuse-policy decision SEALS a prior execution to `CONTINUED_AS_NEW`
// (`TerminateIfRunning` over any prior; `AllowDuplicateFailedOnly` over a FAILED
// prior), the pure `outcome()` accessor must report the superseded run TERMINALLY —
// consistent with the driver's `erase::is_terminal_state` short-circuit — NOT as
// `Running`. Pre-fix `outcome()` mapped every state other than COMPLETED/FAILED to
// `Running`, so a client polling the superseded prior saw it active forever.

/// A `TerminateIfRunning` replace seals a RUNNING prior to `CONTINUED_AS_NEW`; its
/// `outcome()` must be `Terminated`, not `Running`.
#[tokio::test]
async fn outcome_of_a_superseded_prior_is_terminal_not_running() {
    let (_dir, path) = temp_db();
    let mut rt = runtime_at(&path);
    let prior = seed_prior(&mut rt, "wait_wf", "term-1").await;
    assert_eq!(raw_state(&path, prior), "RUNNING");
    // Pre-fix control: a RUNNING prior legitimately reports Running.
    assert!(
        matches!(rt.outcome(prior).unwrap(), ExecutionOutcome::Running),
        "precondition: the live prior reports Running"
    );

    let out = rt
        .start_workflow_with_reuse_policy(
            "wait_wf",
            "term-1",
            json!({}),
            WorkflowIdReusePolicy::TerminateIfRunning,
        )
        .unwrap();
    assert!(out.created);
    assert_eq!(raw_state(&path, prior), "CONTINUED_AS_NEW");

    // The superseded prior is TERMINAL, carrying the raw sealed state — NOT Running
    // (RED pre-fix: outcome() returned Running for the CONTINUED_AS_NEW prior).
    match rt.outcome(prior).unwrap() {
        ExecutionOutcome::Terminated(state) => assert_eq!(
            state, "CONTINUED_AS_NEW",
            "the sealed prior reports its raw terminal state"
        ),
        other => panic!("expected a Terminated outcome for a sealed prior, got {other:?}"),
    }
    // The fresh run is the live one.
    assert!(matches!(
        rt.outcome(out.exec_id).unwrap(),
        ExecutionOutcome::Running
    ));
}

/// `AllowDuplicateFailedOnly` over a FAILED prior seals it to `CONTINUED_AS_NEW`;
/// its `outcome()` must likewise be `Terminated`, not `Running`.
#[tokio::test]
async fn outcome_of_a_failed_only_replaced_prior_is_terminal_not_running() {
    let (_dir, path) = temp_db();
    let mut rt = runtime_at(&path);
    let prior = seed_prior(&mut rt, "fail_wf", "term-2").await;
    assert_eq!(raw_state(&path, prior), "FAILED");
    // A genuinely FAILED prior reports Failed before it is replaced.
    assert!(matches!(
        rt.outcome(prior).unwrap(),
        ExecutionOutcome::Failed(_)
    ));

    let out = rt
        .start_workflow_with_reuse_policy(
            "fail_wf",
            "term-2",
            json!({}),
            WorkflowIdReusePolicy::AllowDuplicateFailedOnly,
        )
        .unwrap();
    assert!(out.created, "a FAILED prior is replaced");
    assert_eq!(raw_state(&path, prior), "CONTINUED_AS_NEW");

    // Once SEALED, the prior is Terminated (superseded), not Running and no longer
    // Failed (RED pre-fix: outcome() returned Running for the sealed state).
    match rt.outcome(prior).unwrap() {
        ExecutionOutcome::Terminated(state) => assert_eq!(state, "CONTINUED_AS_NEW"),
        other => panic!("expected Terminated for a sealed FAILED-replaced prior, got {other:?}"),
    }
}
