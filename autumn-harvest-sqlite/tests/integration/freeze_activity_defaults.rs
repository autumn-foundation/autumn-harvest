//! HARDENING ITEM 1 (issue #1068): **freeze resolved activity defaults on the
//! task row.**
//!
//! Before this change the backend re-read an activity's registered defaults
//! (`max_attempts`, the full `RetryPolicy`, `start_to_close`, and the total
//! `schedule_to_close` deadline) from the MUTABLE in-memory registry on EVERY
//! claim. So a task scheduled under spec A could, after a crash/reopen or a
//! re-registration of the activity *between* attempts, retry under a DIFFERENT
//! spec B — silently changing its policy/deadline mid-flight. The Postgres engine
//! FREEZES these scalars onto the task row at schedule/claim time so an in-flight
//! activity's contract is immutable.
//!
//! This backend now resolves the four fields ONCE — at schedule time when the
//! activity is already registered, else at first claim for a late-registered
//! activity — and FREEZES them on the durable `harvest_tasks` row
//! (`defaults_frozen = 1`, plus the new absolute `schedule_to_close_at` column).
//! Later claims read the frozen values verbatim and NEVER re-consult the registry
//! for them (the activity BODY still always comes from the registry).
//!
//! No Docker: `SQLite` is embedded (`rusqlite` `bundled`); retry backoff and the
//! total-deadline clock are driven by the injected `*_as_of` clock, and the
//! start-to-close budget by a real (short) `std::thread::sleep`, so nothing waits
//! on wall time.

// The `#[workflow]`/`#[activity]` macros reference their input params, so a
// deliberately-unused param reads as a "used underscore binding" through expansion.
#![allow(clippy::used_underscore_binding)]
// The `#[activity]` fixture bodies are trivial and await-free — the crate runs a
// registered `ActivitySpec`, not the macro body.
#![allow(clippy::unused_async)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use autumn_harvest::HarvestError;
use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::{ActivitySpec, RunState, SqliteRuntime};
use chrono::{DateTime, Utc};
use serde_json::json;

// ── Shared helpers ─────────────────────────────────────────────────────────────

fn temp_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("harvest.sqlite3");
    (dir, path)
}

fn fixed_now() -> DateTime<Utc> {
    "2026-07-15T00:00:00Z".parse().unwrap()
}

/// The single stored `ActivityFailed` event for `exec` (panics if 0 or >1).
fn only_activity_failed(rt: &SqliteRuntime, exec: ExecutionId) -> WorkflowEvent {
    let mut it = rt
        .load_history(exec)
        .unwrap()
        .into_iter()
        .filter(|e| matches!(e, WorkflowEvent::ActivityFailed { .. }));
    let ev = it.next().expect("expected one ActivityFailed event");
    assert!(it.next().is_none(), "expected exactly one ActivityFailed");
    ev
}

fn history_has_schedule_to_close_timeout(rt: &SqliteRuntime, exec: ExecutionId) -> bool {
    rt.load_history(exec).unwrap().iter().any(|e| {
        matches!(
            e,
            WorkflowEvent::ActivityTimedOut {
                timeout_type: autumn_harvest::TimeoutType::ScheduleToClose,
                ..
            }
        )
    })
}

/// True iff `table` has a column named `column` (a `PRAGMA table_info` probe from
/// a raw connection — the additive-migration verifier).
fn column_exists(raw: &rusqlite::Connection, table: &str, column: &str) -> bool {
    let mut stmt = raw.prepare(&format!("PRAGMA table_info({table})")).unwrap();
    let mut rows = stmt.query([]).unwrap();
    while let Some(row) = rows.next().unwrap() {
        let name: String = row.get(1).unwrap();
        if name == column {
            return true;
        }
    }
    false
}

// ── HEADLINE: a frozen retry cap/policy survives a re-registration mid-retry ─────
//
// The activity is dispatched via the RAW path (`execute_activity_raw`) against an
// INFO-registered activity, so pre-fix the defaults (max_attempts + the whole
// retry policy) fell back to the registry AT CLAIM TIME — and a re-registration
// under a different spec between attempts changed them. Post-fix they are frozen
// onto the row at schedule time and are immutable for the rest of the run.

#[activity(retry = RetryPolicy::fixed(3, Duration::from_secs(60)))]
async fn flaky(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

// Calls the RAW dispatch path BY NAME — so the ScheduleActivity command carries NO
// retry_policy_override and the defaults are resolved from the registered spec.
#[workflow]
async fn flaky_wf(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let out = ctx
        .execute_activity_raw("flaky", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    Ok(out.as_i64().unwrap_or_default() * 2)
}

// An always-failing body that increments `counter` on each call.
fn failing_body(
    counter: &Arc<AtomicUsize>,
) -> impl Fn(serde_json::Value) -> Result<serde_json::Value, String> + Send + Sync + 'static {
    let counter = Arc::clone(counter);
    move |_input| {
        counter.fetch_add(1, Ordering::SeqCst);
        Err("boom".to_string())
    }
}

// RED pre-fix: the raw path re-read the registry at claim time, so re-registering
// "flaky" with `max_attempts = 1` (no policy) between attempts 1 and 2 CUT the
// in-flight run's cap to 1 — it went terminal at attempt 2 (2 body calls) instead
// of honoring the ORIGINAL max_attempts = 3 (3 body calls, terminal at attempt 3).
#[tokio::test]
async fn frozen_retry_policy_survives_re_registration() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&flaky_wf_info());

    let calls = Arc::new(AtomicUsize::new(0));
    // Spec A: the info-registered activity carries `retry = fixed(3, 60s)`, so the
    // frozen policy has max_attempts = 3 and a 60s backoff.
    rt.register_activity(&flaky_info(), failing_body(&calls));

    let t0 = fixed_now();
    let exec = rt.start_workflow("flaky_wf", json!(21)).unwrap();

    // Attempt 1 fails and is requeued 60s out under the FROZEN fixed(3, 60s) policy.
    let state = rt.run_until_blocked_as_of(exec, t0).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingTimer),
        "attempt 1 must back off under the registered policy; got {state:?}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1, "attempt 1 ran once");

    // Re-register "flaky" under a DIFFERENT, hostile spec: max_attempts = 1, NO
    // retry policy. Pre-fix this would cap the already-scheduled task at 1 attempt.
    rt.register_activity_raw("flaky", ActivitySpec::new(1, failing_body(&calls)));

    // Advance past each backoff. Under the frozen contract the run STILL gets its
    // original 3 attempts.
    let state = rt
        .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(61))
        .await
        .unwrap();
    assert!(
        matches!(state, RunState::WaitingTimer),
        "attempt 2 must back off again under the FROZEN cap of 3, not go terminal at \
         the re-registered cap of 1; got {state:?} (RED pre-fix: terminal here)"
    );

    let done = rt
        .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(122))
        .await
        .unwrap();
    assert!(
        matches!(done, RunState::Failed(_)),
        "the run fails terminally after exhausting the FROZEN 3 attempts; got {done:?}"
    );

    // The falsifiable discriminators: 3 body calls, terminal ActivityFailed at
    // attempt 3 — NOT 2 calls / attempt 2 (the re-registered cap of 1).
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "the FROZEN max_attempts = 3 must govern despite the re-registration to 1 \
         (RED pre-fix: only 2 calls, cut to the re-registered cap)"
    );
    match only_activity_failed(&rt, exec) {
        WorkflowEvent::ActivityFailed { attempt, .. } => assert_eq!(
            attempt, 3,
            "terminal at the frozen attempt cap of 3 (RED pre-fix: attempt 2)"
        ),
        other => panic!("expected ActivityFailed, got {other:?}"),
    }
}

// ── A frozen schedule_to_close deadline survives a re-registration ───────────────
//
// The total (cross-retry) deadline was resolved from the registry at CLAIM time,
// so re-registering the activity with a LONGER (or no) schedule_to_close between
// attempts let the in-flight run outlive its original deadline. Post-fix the
// absolute deadline is frozen on the row and governs regardless of later
// re-registration.

#[activity]
async fn stc_act(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

#[workflow]
async fn stc_wf(ctx: &WorkflowContext, n: i64) -> Result<String, String> {
    match ctx
        .execute_activity_raw("stc_act", json!(n), "default")
        .await
    {
        Ok(_) => Ok("completed".to_string()),
        Err(HarvestError::Timeout { .. }) => Ok("timed_out".to_string()),
        Err(other) => Err(other.to_string()),
    }
}

// The activity info with a 5m total deadline and a 4m-backoff, 10-attempt policy.
fn stc_info_with_deadline(deadline: Duration) -> ActivityInfo {
    let mut info = stc_act_info();
    info.default_schedule_to_close = Some(deadline);
    info.default_retry_policy = Some(RetryPolicy::fixed(10, Duration::from_secs(240)));
    info
}

// RED pre-fix: the schedule_to_close deadline was re-resolved from the registry at
// claim time, so re-registering `stc_act` with a 1h deadline between attempts let
// the run keep backing off past its ORIGINAL 5m deadline (never timing out). With
// the frozen absolute deadline the run times out at the original 5m mark.
#[tokio::test]
async fn frozen_schedule_to_close_deadline_survives_re_registration() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&stc_wf_info());

    let calls = Arc::new(AtomicUsize::new(0));
    // Spec A: total deadline 5m (300s), retry fixed(10, 4m). Frozen at schedule as
    // an absolute deadline of scheduled_at + 300s.
    rt.register_activity(
        &stc_info_with_deadline(Duration::from_secs(300)),
        failing_body(&calls),
    );

    let t0 = fixed_now();
    let exec = rt.start_workflow("stc_wf", json!(1)).unwrap();

    // Attempt 1 (t0) fails; the next attempt is armed 4m out (t0+240s), still under
    // the frozen 5m deadline, so it backs off rather than timing out.
    let state = rt.run_until_blocked_as_of(exec, t0).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingTimer),
        "attempt 1 backs off (4m < the 5m deadline); got {state:?}"
    );

    // Re-register under a MUCH LONGER deadline (1h). Pre-fix this would let the run
    // keep retrying past the original 5m deadline.
    rt.register_activity(
        &stc_info_with_deadline(Duration::from_secs(3600)),
        failing_body(&calls),
    );

    // Drive at t0+4m: attempt 2 fails; its next attempt would land at t0+8m, which
    // is PAST the FROZEN 5m deadline → sealed ActivityTimedOut { ScheduleToClose }.
    // Under the bug the re-registered 1h deadline would requeue it instead.
    let done = rt
        .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(240))
        .await
        .unwrap();
    assert!(
        matches!(done, RunState::Completed(ref v) if v == "timed_out"),
        "the run must time out at the FROZEN 5m deadline, not keep retrying under the \
         re-registered 1h deadline; got {done:?} (RED pre-fix: WaitingTimer)"
    );
    assert!(
        history_has_schedule_to_close_timeout(&rt, exec),
        "history must record a terminal ActivityTimedOut {{ ScheduleToClose }} at the \
         frozen deadline:\n{:?}",
        rt.load_history(exec).unwrap()
    );
}

// ── A frozen start_to_close budget survives a re-registration ────────────────────
//
// The per-attempt start-to-close budget fell back to the registry on the RAW path
// at claim time, so re-registering with a generous budget between attempts let a
// later, slow attempt escape its original tight budget. Post-fix the budget is
// frozen on the row.

#[activity]
async fn stc2_act(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

#[workflow]
async fn stc2_wf(ctx: &WorkflowContext, n: i64) -> Result<String, String> {
    match ctx
        .execute_activity_raw("stc2_act", json!(n), "default")
        .await
    {
        Ok(_) => Ok("completed".to_string()),
        Err(HarvestError::Timeout { .. }) => Ok("timed_out".to_string()),
        Err(other) => Err(other.to_string()),
    }
}

fn stc2_info(start_to_close: Duration) -> ActivityInfo {
    let mut info = stc2_act_info();
    info.default_start_to_close = Some(start_to_close);
    // A generous 60s retry policy (2 attempts) so attempt 1 backs off and attempt 2
    // runs after the re-registration.
    info.default_retry_policy = Some(RetryPolicy::fixed(2, Duration::from_secs(60)));
    info
}

// Attempt 1 fails fast (well under any budget); attempt 2 sleeps 40ms (over the
// frozen 1ms budget, under the re-registered 1h budget).
fn stc2_body(
    counter: &Arc<AtomicUsize>,
) -> impl Fn(serde_json::Value) -> Result<serde_json::Value, String> + Send + Sync + 'static {
    let counter = Arc::clone(counter);
    move |input| {
        if counter.fetch_add(1, Ordering::SeqCst) == 0 {
            Err("transient".to_string())
        } else {
            std::thread::sleep(Duration::from_millis(40));
            Ok(input)
        }
    }
}

// RED pre-fix: the raw path re-read start_to_close from the registry at claim time,
// so re-registering `stc2_act` with a 1h budget between attempts let the slow (40ms)
// attempt 2 complete instead of timing out at the ORIGINAL 1ms budget. With the
// frozen budget attempt 2 records ActivityTimedOut { StartToClose }.
#[tokio::test]
async fn frozen_start_to_close_survives_re_registration() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&stc2_wf_info());

    let calls = Arc::new(AtomicUsize::new(0));
    // Spec A: a tight 1ms per-attempt budget, frozen onto the task row at schedule.
    rt.register_activity(&stc2_info(Duration::from_millis(1)), stc2_body(&calls));

    let t0 = fixed_now();
    let exec = rt.start_workflow("stc2_wf", json!(7)).unwrap();

    // Attempt 1 fails fast (no start-to-close timeout — the body was quick) and
    // backs off 60s under the retry policy.
    let state = rt.run_until_blocked_as_of(exec, t0).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingTimer),
        "attempt 1 fails fast and backs off; got {state:?}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1, "attempt 1 ran once");

    // Re-register with a GENEROUS 1h budget. Pre-fix this would let the slow attempt
    // 2 complete instead of timing out at the original 1ms budget.
    rt.register_activity(&stc2_info(Duration::from_secs(3600)), stc2_body(&calls));

    // Drive past the backoff: attempt 2 sleeps 40ms — over the FROZEN 1ms budget →
    // ActivityTimedOut { StartToClose }. Under the bug (1h budget) it completes.
    let done = rt
        .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(61))
        .await
        .unwrap();
    assert!(
        matches!(done, RunState::Completed(ref v) if v == "timed_out"),
        "the slow attempt 2 must time out under the FROZEN 1ms budget, not complete \
         under the re-registered 1h budget; got {done:?} (RED pre-fix: \"completed\")"
    );
    assert!(
        rt.load_history(exec).unwrap().iter().any(|e| matches!(
            e,
            WorkflowEvent::ActivityTimedOut {
                timeout_type: autumn_harvest::TimeoutType::StartToClose,
                ..
            }
        )),
        "history must record a terminal ActivityTimedOut {{ StartToClose }} at the \
         frozen budget:\n{:?}",
        rt.load_history(exec).unwrap()
    );
}

// ── An old-schema DB reopens and is migrated additively ──────────────────────────
//
// Guards the additive migration: a database file created before this change (whose
// `harvest_tasks` lacks `schedule_to_close_at`/`defaults_frozen`) must reopen
// cleanly (the columns are added, never dropped/renamed) and drive a fresh workflow
// to completion.

// The pre-#1068 `harvest_tasks` DDL — the 14 columns that existed before the two
// freeze columns were added.
const OLD_HARVEST_TASKS_DDL: &str = "\
CREATE TABLE harvest_tasks (\
    task_id      TEXT PRIMARY KEY,\
    exec_id      TEXT NOT NULL,\
    activity_id  TEXT NOT NULL,\
    name         TEXT NOT NULL,\
    input_json   TEXT NOT NULL,\
    queue        TEXT NOT NULL,\
    state        TEXT NOT NULL,\
    attempt      INTEGER NOT NULL,\
    run_at       INTEGER NOT NULL,\
    seq          INTEGER NOT NULL,\
    max_attempts INTEGER,\
    retry_policy_json TEXT,\
    start_to_close_ms INTEGER,\
    scheduled_at INTEGER NOT NULL\
)";

#[activity]
async fn plain_act(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

#[workflow]
async fn plain_wf(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let out: i64 = ctx
        .execute_activity(&plain_act_info(), n)
        .await
        .map_err(|e| e.to_string())?;
    Ok(out + 1)
}

// RED (after the schema/INSERT change but before the migration is wired): a fresh
// `SqliteRuntime::open` on the old-schema file must ADD the two missing columns.
// Without the migration the columns are absent and any fresh enqueue (which now
// lists them) fails with "no such column".
#[tokio::test]
async fn old_schema_db_reopens_and_migrates() {
    let (_dir, path) = temp_db();

    // Create the file with the OLD `harvest_tasks` shape (no freeze columns). The
    // other tables are absent — `SqliteRuntime::open`'s `CREATE TABLE IF NOT EXISTS`
    // creates them, and leaves the pre-existing `harvest_tasks` untouched (so it
    // keeps the old shape until the additive migration runs).
    {
        let raw = rusqlite::Connection::open(&path).unwrap();
        raw.execute_batch(OLD_HARVEST_TASKS_DDL).unwrap();
        assert!(
            !column_exists(&raw, "harvest_tasks", "schedule_to_close_at"),
            "precondition: the old schema lacks schedule_to_close_at"
        );
        assert!(
            !column_exists(&raw, "harvest_tasks", "defaults_frozen"),
            "precondition: the old schema lacks defaults_frozen"
        );
    }

    // Open the runtime on the old-schema file — the additive migration must add the
    // two columns without dropping/renaming anything.
    let mut rt = SqliteRuntime::open(&path).unwrap();

    {
        let raw = rusqlite::Connection::open(&path).unwrap();
        assert!(
            column_exists(&raw, "harvest_tasks", "schedule_to_close_at"),
            "the additive migration must add schedule_to_close_at on reopen"
        );
        assert!(
            column_exists(&raw, "harvest_tasks", "defaults_frozen"),
            "the additive migration must add defaults_frozen on reopen"
        );
    }

    // A fresh schedule + drive works end-to-end on the migrated database.
    rt.register_workflow(&plain_wf_info());
    rt.register_activity(&plain_act_info(), |input: serde_json::Value| Ok(input));
    let exec = rt.start_workflow("plain_wf", json!(41)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v.as_i64() == Some(42)),
        "a fresh workflow must complete on the migrated old-schema DB; got {state:?}"
    );
}

// ── Regression guard: the stable (never-re-registered) path is unchanged ─────────
//
// Freezing must not alter the common case. A single, stable registration resolves
// the same values it did before (backoff timing, deadline, attempt cap), and the
// task row is frozen with the expected absolute deadline.

fn frozen_columns(path: &std::path::Path, exec: ExecutionId) -> (Option<i64>, i64) {
    let raw = rusqlite::Connection::open(path).unwrap();
    raw.query_row(
        "SELECT schedule_to_close_at, defaults_frozen FROM harvest_tasks WHERE exec_id = ?1",
        [exec.to_string()],
        |r| Ok((r.get::<_, Option<i64>>(0)?, r.get::<_, i64>(1)?)),
    )
    .unwrap()
}

#[activity(retry = RetryPolicy::fixed(3, Duration::from_secs(60)))]
async fn stable_act(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

#[workflow]
async fn stable_wf(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let out: i64 = ctx
        .execute_activity(&stable_act_info(), n)
        .await
        .map_err(|e| e.to_string())?;
    Ok(out * 10)
}

#[tokio::test]
async fn stable_registration_unchanged() {
    let (_dir, path) = temp_db();
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&stable_wf_info());

    let calls = Arc::new(AtomicUsize::new(0));
    let body_calls = Arc::clone(&calls);
    // A body that fails once then succeeds — exercises one retry under the stable
    // policy without any re-registration.
    let mut info = stable_act_info();
    info.default_schedule_to_close = Some(Duration::from_secs(600)); // 10m total cap
    rt.register_activity(&info, move |input: serde_json::Value| {
        if body_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Err("transient".to_string())
        } else {
            Ok(input)
        }
    });

    let t0 = fixed_now();
    let exec = rt.start_workflow("stable_wf", json!(3)).unwrap();

    // Attempt 1 fails, backs off 60s under the (unchanged) fixed(3, 60s) policy.
    let state = rt.run_until_blocked_as_of(exec, t0).await.unwrap();
    assert!(matches!(state, RunState::WaitingTimer), "got {state:?}");

    // The row froze the total deadline as scheduled_at (t0) + 10m, with the frozen flag set.
    let (deadline, frozen) = frozen_columns(&path, exec);
    assert_eq!(frozen, 1, "the stable path freezes the task row");
    assert_eq!(
        deadline,
        Some(t0.timestamp_millis() + 600_000),
        "the total deadline is frozen as scheduled_at + schedule_to_close"
    );

    // Past the backoff the retry runs and the run completes normally (identical to
    // pre-freeze behavior).
    let done = rt
        .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(61))
        .await
        .unwrap();
    assert!(
        matches!(done, RunState::Completed(ref v) if v.as_i64() == Some(30)),
        "the stable path completes unchanged; got {done:?}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2, "body ran twice (fail, ok)");
}
