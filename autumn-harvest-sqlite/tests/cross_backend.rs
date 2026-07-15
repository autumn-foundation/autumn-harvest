//! Cross-backend replay guarantee (AC5).
//!
//! A history written by the `SQLite` backend replays cleanly on the core engine's
//! own (Postgres-oriented) [`WorkflowReplayer`], because both backends serialize
//! the *same* [`WorkflowEvent`] via `serde_json` in the shared adjacently-tagged
//! form. The claim is **per-event encoding byte-identity + replay-equivalent
//! event SETS**, not identical event streams: the Postgres engine additionally
//! appends an `ActivityStarted` on claim that this backend never writes, and the
//! two replay identically because `HistoryMatcher::scan_activity_terminal` skips
//! `ActivityStarted` (exercised by the PG-shaped-history test below).

// The `#[workflow]` macro references the workflow's input parameter, so a
// deliberately-unused param reads as a "used underscore binding" through the
// expansion.
#![allow(clippy::used_underscore_binding)]
// The terminal-failure workflow fixtures return immediately (no `.await`).
#![allow(clippy::unused_async)]

use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest::{ActivityExecId, WorkerId};
use autumn_harvest_sqlite::{ActivitySpec, ExecutionOutcome, RunState, SqliteRuntime};
use serde_json::json;

#[workflow]
async fn single_activity(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let out = ctx
        .execute_activity_raw("work", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    Ok(out.as_i64().ok_or("bad activity output")? * 2)
}

/// Mints a deterministic UUID (a frozen side effect) BEFORE its first suspension,
/// then runs an activity. The frozen value must survive to history so replay
/// reproduces it byte-identically rather than re-minting a different UUID.
#[workflow]
async fn side_effect_then_activity(
    ctx: &WorkflowContext,
    n: i64,
) -> Result<serde_json::Value, String> {
    let id = ctx.new_uuid().to_string();
    let out = ctx
        .execute_activity_raw("work", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    Ok(json!({ "id": id, "doubled": out.as_i64().unwrap_or_default() * 2 }))
}

async fn run_to_completion_on_sqlite() -> Vec<WorkflowEvent> {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&single_activity_info());
    rt.register_activity(
        "work",
        ActivitySpec::new(1, |input: serde_json::Value| {
            Ok(json!(input.as_i64().unwrap()))
        }),
    );
    let exec = rt.start_workflow("single_activity", json!(3)).unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(ref v) if v.as_i64() == Some(6)));
    rt.load_history(exec).unwrap()
}

// ── AC5: the `SQLite` history replays cleanly on the core replayer ─────────────

#[tokio::test]
async fn sqlite_history_replays_on_core_replayer() {
    let history = run_to_completion_on_sqlite().await;

    let report = WorkflowReplayer::new()
        .register_fn("single_activity", single_activity_info().handler)
        .replay_from_events(history)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "`SQLite`-written history must replay cleanly on the core engine:\n{report}"
    );
}

// ── AC5: every event's JSON encoding is canonical (byte-stable round-trip) ───

#[tokio::test]
async fn each_event_json_roundtrips_byte_identically() {
    let history = run_to_completion_on_sqlite().await;
    assert!(!history.is_empty());

    for event in &history {
        let once = serde_json::to_string(event).unwrap();
        // Re-parse via the SAME adjacently-tagged `WorkflowEvent` derive the
        // Postgres backend uses, and re-serialize: a stable, canonical encoding
        // round-trips byte-identically. This is a *necessary condition* for
        // cross-backend byte-identity given the shared derive — not a direct
        // demonstration of identity against a PG-written stream (the golden
        // assertion below pins one concrete encoding).
        let parsed: WorkflowEvent = serde_json::from_str(&once).unwrap();
        let twice = serde_json::to_string(&parsed).unwrap();
        assert_eq!(once, twice, "event JSON must be byte-stable: {once}");
        assert!(
            once.contains("\"type\":"),
            "adjacently-tagged encoding must carry a `type` tag: {once}"
        );
    }
}

// ── AC5: a concrete GOLDEN encoding — makes "byte-identical to the core" real ──
//
// `single_activity(3)` completes with output `6`. Its terminal `WorkflowCompleted`
// event, as STORED by the SQLite backend, must equal this exact byte string — the
// same bytes the Postgres backend's shared `WorkflowEvent` serde derive produces.
// A concrete tripwire against an accidental core encoding change that a
// self-round-trip test would not catch.

#[tokio::test]
async fn stored_event_matches_golden_encoding() {
    const GOLDEN_WORKFLOW_COMPLETED: &str = r#"{"type":"WorkflowCompleted","data":{"output":6}}"#;

    let history = run_to_completion_on_sqlite().await;
    let completed = history
        .iter()
        .find(|e| matches!(e, WorkflowEvent::WorkflowCompleted { .. }))
        .expect("a completed run has a WorkflowCompleted event");

    let stored = serde_json::to_string(completed).unwrap();
    assert_eq!(
        stored, GOLDEN_WORKFLOW_COMPLETED,
        "the stored event must be byte-identical to the shared core encoding"
    );
}

// ── AC5: event-SET equivalence — a genuinely PG-shaped history replays ───────
//
// The Postgres engine appends an `ActivityStarted` on claim that this backend
// never writes, so the two backends' event *streams* differ. They are
// replay-equivalent because `HistoryMatcher::scan_activity_terminal` skips
// `ActivityStarted`. This is proven directly against the core `WorkflowReplayer`
// (in-memory — no SQLite import path needed): a PG-shaped history, including the
// `ActivityStarted` this backend omits, replays cleanly.

#[tokio::test]
async fn pg_shaped_history_with_activity_started_is_replay_equivalent() {
    let activity_id = ActivityExecId::new();
    let pg_history = vec![
        WorkflowEvent::workflow_started(json!(4), chrono::Utc::now()),
        WorkflowEvent::ActivityScheduled {
            activity_id,
            name: "work".to_string(),
            input: json!(4),
            queue: "default".to_string(),
        },
        WorkflowEvent::ActivityStarted {
            activity_id,
            worker_id: WorkerId::new("pg-worker-1"),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id,
            output: json!(4),
        },
    ];

    let report = WorkflowReplayer::new()
        .register_fn("single_activity", single_activity_info().handler)
        .replay_from_events(pg_history)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a PG-shaped history (with the ActivityStarted this backend omits) must be \
         replay-equivalent:\n{report}"
    );
}

// ── Deterministic side effect: the frozen value persists across a reopen and is
//    NOT re-minted on replay ────────────────────────────────────────────────────

#[tokio::test]
async fn deterministic_side_effect_persists_and_replays_byte_identically() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("harvest.sqlite3");

    // Run to completion on the SQLite backend, then "crash" (drop).
    let (exec, recorded_id) = {
        let mut rt = SqliteRuntime::open(&path).unwrap();
        rt.register_workflow(&side_effect_then_activity_info());
        rt.register_activity(
            "work",
            ActivitySpec::new(1, |input: serde_json::Value| {
                Ok(json!(input.as_i64().unwrap()))
            }),
        );
        let exec = rt
            .start_workflow("side_effect_then_activity", json!(4))
            .unwrap();
        let state = rt.run_until_blocked(exec).await.unwrap();
        let RunState::Completed(out) = state else {
            panic!("expected completion, got {state:?}");
        };
        let id = out["id"].as_str().unwrap().to_string();
        (exec, id)
    };

    // Reopen the file and read back the durable history: the frozen side effect
    // MUST be present (it rode the same suspending batch as the activity).
    let rt2 = SqliteRuntime::open(&path).unwrap();
    let events = rt2.load_history(exec).unwrap();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::SideEffectRecorded { .. })),
        "the deterministic side effect must be persisted, not dropped"
    );

    // Replay via the core engine: the frozen UUID is matched from history, never
    // re-minted, so replay succeeds and reproduces the same id byte-for-byte.
    let report = WorkflowReplayer::new()
        .register_fn(
            "side_effect_then_activity",
            side_effect_then_activity_info().handler,
        )
        .replay_from_events(events)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a history with a frozen side effect must replay byte-identically:\n{report}"
    );
    assert_eq!(recorded_id.len(), 36, "the recorded UUID is a real v7 UUID");
}

// ── FIX C (Codex #1069): the real workflow_name reaches ctx.info() ─────────────
//
// A workflow that reads the documented replay-safe `ctx.info().workflow_type`
// (issue #698) must observe the REAL registered handler name on this backend,
// exactly as the Postgres worker (which injects it) does. The lighter core entry
// point hard-codes an EMPTY workflow_name; driving through the caps-carrying entry
// point with the real name populates `ctx.info().workflow_type`. This is a
// cross-backend byte-identity concern: the value is recorded into an activity
// input, so a divergent (empty) name would poison the shared-history guarantee.

#[workflow]
async fn records_workflow_type(ctx: &WorkflowContext, _n: i64) -> Result<String, String> {
    // Read the run's own type and route it through an activity input (so the
    // value is recorded in history and must replay byte-identically).
    let wf_type = ctx.info().workflow_type;
    let echoed = ctx
        .execute_activity_raw("echo_str", json!(wf_type), "default")
        .await
        .map_err(|e| e.to_string())?;
    Ok(echoed.as_str().unwrap_or_default().to_string())
}

#[tokio::test]
async fn workflow_type_reaches_ctx_info_and_replays_on_core() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&records_workflow_type_info());
    rt.register_activity(
        "echo_str",
        ActivitySpec::new(1, |input: serde_json::Value| Ok(input)),
    );
    let exec = rt
        .start_workflow("records_workflow_type", json!(0))
        .unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();
    // The workflow observed its REAL registered type, NOT "" (FIX C; RED pre-fix).
    assert!(
        matches!(state, RunState::Completed(ref v) if v == "records_workflow_type"),
        "ctx.info().workflow_type must be the real registered name, not empty, got {state:?}"
    );

    // The activity input recorded the real name into history.
    let history = rt.load_history(exec).unwrap();
    assert!(
        history.iter().any(|e| matches!(
            e,
            WorkflowEvent::ActivityScheduled { input, .. }
                if input.as_str() == Some("records_workflow_type")
        )),
        "the recorded activity input must carry the real workflow type:\n{history:?}"
    );

    // Cross-backend guarantee: the history replays byte-identically on the core.
    let report = WorkflowReplayer::new()
        .register_fn(
            "records_workflow_type",
            records_workflow_type_info().handler,
        )
        .replay_from_events(history)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a history recording ctx.info().workflow_type must replay on the core:\n{report}"
    );
}

// ── FIX 1 (Codex #1069 P2, runtime.rs:660): typed workflow failures preserve
//    metadata and stay byte-equivalent to the Postgres path ─────────────────────
//
// When a `#[workflow]` returns a typed `WorkflowFailure`, the macro shim encodes it
// into a `harvest_workflow_failure_v1` envelope carried on the failure string. The
// Postgres path DECODES that envelope and writes `WorkflowFailed` with
// `error_type`/`details` (+ the human message), storing the human message in the
// execution `error` column. Pre-fix this backend stored the RAW envelope and an
// all-`None` untyped `WorkflowFailed`, losing the typed metadata and breaking the
// crate's cross-backend byte-identity guarantee (AC5). This is RED pre-fix.

#[workflow]
async fn typed_failure_workflow(ctx: &WorkflowContext, _n: i64) -> Result<i64, WorkflowFailure> {
    // Read `ctx` so the macro's expansion does not warn on an unused binding.
    let _ = ctx.info();
    Err(
        WorkflowFailure::new("BudgetExceeded", "over the monthly budget")
            .with_details(json!({ "limit": 1000, "spent": 1500 })),
    )
}

#[tokio::test]
async fn typed_workflow_failure_preserves_metadata_and_replays_on_core() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&typed_failure_workflow_info());
    let exec = rt
        .start_workflow("typed_failure_workflow", json!(0))
        .unwrap();

    // The run fails, and the SURFACED failure is the HUMAN message — not the raw
    // wire envelope (RED pre-fix: the surfaced value was the whole envelope).
    let state = rt.run_until_blocked(exec).await.unwrap();
    let RunState::Failed(msg) = state else {
        panic!("expected a Failed terminal state, got {state:?}");
    };
    assert_eq!(
        msg, "over the monthly budget",
        "the surfaced failure must be the decoded human message, not the raw envelope"
    );

    // The stored execution `error` column is the human message, never the raw
    // envelope (mirrors the Postgres `update_workflow_execution_failed(&decoded.message)`).
    match rt.outcome(exec).unwrap() {
        ExecutionOutcome::Failed(e) => {
            assert_eq!(e, "over the monthly budget");
            assert!(
                !e.contains("harvest_workflow_failure_v1"),
                "the error column must not store the raw wire envelope: {e}"
            );
        }
        other => panic!("expected a Failed outcome, got {other:?}"),
    }

    // The stored `WorkflowFailed` event carries the DECODED typed fields, exactly as
    // the Postgres path's `workflow_failed_typed` does — NOT an all-`None` untyped
    // event (RED pre-fix: `error_type`/`details` were `None` and `error` was the
    // raw envelope).
    let history = rt.load_history(exec).unwrap();
    let (error, error_type, details, non_retryable) = history
        .iter()
        .find_map(|e| match e {
            WorkflowEvent::WorkflowFailed {
                error,
                error_type,
                details,
                non_retryable,
            } => Some((
                error.clone(),
                error_type.clone(),
                details.clone(),
                *non_retryable,
            )),
            _ => None,
        })
        .expect("a failed run records a WorkflowFailed event");
    assert_eq!(
        error, "over the monthly budget",
        "the WorkflowFailed event's `error` must be the human message"
    );
    assert_eq!(
        error_type.as_deref(),
        Some("BudgetExceeded"),
        "the typed error_type must be preserved (RED pre-fix: None)"
    );
    assert_eq!(
        details,
        Some(json!({ "limit": 1000, "spent": 1500 })),
        "the typed details must be preserved (RED pre-fix: None)"
    );
    assert_eq!(
        non_retryable,
        Some(false),
        "the typed non_retryable flag must be preserved (RED pre-fix: None)"
    );

    // AC5: the typed-failure history replays cleanly on the core engine — the
    // handler deterministically re-fails (`WorkflowFailed`, the replayer's clean
    // terminal for a failing workflow), with NO non-determinism divergence. The
    // stored typed `WorkflowFailed` event is consistent with re-running the handler.
    let report = WorkflowReplayer::new()
        .register_fn(
            "typed_failure_workflow",
            typed_failure_workflow_info().handler,
        )
        .replay_from_events(history)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::WorkflowFailed { .. }),
        "a typed-workflow-failure history must replay to a clean WorkflowFailed \
         (no non-determinism divergence) on the core engine:\n{report}"
    );
}

// A legacy untyped `Err(String)` still stores its message unchanged (all typed
// fields `None`) — the decode's back-compat path, so the fix does not regress the
// common untyped failure.
#[workflow]
async fn untyped_failure_workflow(ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    let _ = ctx.info();
    Err("plain author error".to_string())
}

#[tokio::test]
async fn untyped_workflow_failure_is_unchanged() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&untyped_failure_workflow_info());
    let exec = rt
        .start_workflow("untyped_failure_workflow", json!(0))
        .unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Failed(ref m) if m == "plain author error"));

    let history = rt.load_history(exec).unwrap();
    let event = history
        .iter()
        .find(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. }))
        .expect("a failed run records a WorkflowFailed event");
    let WorkflowEvent::WorkflowFailed {
        error,
        error_type,
        details,
        non_retryable,
    } = event
    else {
        unreachable!()
    };
    assert_eq!(error, "plain author error");
    assert_eq!(
        *error_type, None,
        "an untyped failure records no error_type"
    );
    assert_eq!(*details, None);
    assert_eq!(*non_retryable, None);
}
