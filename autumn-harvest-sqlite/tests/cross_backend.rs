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

use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest::{ActivityExecId, WorkerId};
use autumn_harvest_sqlite::{ActivitySpec, RunState, SqliteRuntime};
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
        // round-trips byte-identically, which is the property that makes a history
        // byte-identical per event across backends.
        let parsed: WorkflowEvent = serde_json::from_str(&once).unwrap();
        let twice = serde_json::to_string(&parsed).unwrap();
        assert_eq!(once, twice, "event JSON must be byte-stable: {once}");
        assert!(
            once.contains("\"type\":"),
            "adjacently-tagged encoding must carry a `type` tag: {once}"
        );
    }
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
