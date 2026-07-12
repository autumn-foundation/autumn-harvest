//! Child-workflow fan-out primitive tests -- parallel child-workflow dispatch,
//! replay determinism, non-determinism detection, and cancellation propagation
//! (issue #601).
//!
//! Mirrors `tests/fanout_tests.rs` (issue #359, activity fan-out) test-for-test,
//! substituting `ChildWorkflowStarted`/`ChildWorkflowCompleted`/`ChildWorkflowFailed`
//! events for `ActivityScheduled`/`ActivityCompleted`/`ActivityFailed`.
//!
//! All tests are pure unit tests; no database required.

use std::future::Future;
use std::pin::Pin;

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::error::{HarvestError, PayloadKind};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::executor::{WorkflowOutcome, run_workflow};
use autumn_harvest::types::ExecutionId;
use chrono::Utc;
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Test workflow handlers
// ---------------------------------------------------------------------------

fn child_fan_out_three_parallel<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let children = vec![
            ("child_a".to_string(), json!("input_a")),
            ("child_b".to_string(), json!("input_b")),
            ("child_c".to_string(), json!("input_c")),
        ];
        let results = ctx
            .spawn_child_workflow_fan_out_raw(children)
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({ "results": results }))
    })
}

fn child_fan_out_fail_fast<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let children = vec![
            ("child_ok".to_string(), Value::Null),
            ("child_fail".to_string(), Value::Null),
            ("child_ok2".to_string(), Value::Null),
        ];
        let results = ctx
            .spawn_child_workflow_fan_out_raw(children)
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({ "results": results }))
    })
}

// Two failing children among three slots -- used to demonstrate the known,
// pre-existing (shared with activity fan-out) limitation that a fail-fast
// replay selects the failure at the lowest input-slot index, not necessarily
// the one whose terminal event was recorded earliest in history. See
// `child_fan_out_raw_known_limitation_replay_selects_by_slot_order_not_recorded_order`.
fn child_fan_out_two_failures<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let children = vec![
            ("child_ok".to_string(), Value::Null),
            ("child_fail_low_slot".to_string(), Value::Null),
            ("child_fail_high_slot".to_string(), Value::Null),
        ];
        let results = ctx
            .spawn_child_workflow_fan_out_raw(children)
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({ "results": results }))
    })
}

// Two children: slot 0 is still ChildInProgress (no terminal recorded), slot
// 1 has a recorded failure. Catches the fail-fast error and continues by
// scheduling an activity -- used to demonstrate the known limitation where
// try_join_all re-emits slot 0's StartChildWorkflow re-park command before
// the fail-fast error is even returned, leaving it in the *same* commands
// batch as whatever the workflow does next. See
// `child_fan_out_raw_known_limitation_stale_repark_command_leaks_into_next_suspension`.
fn child_fan_out_catch_and_continue<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let children = vec![
            ("child_pending".to_string(), Value::Null),
            ("child_fail".to_string(), Value::Null),
        ];
        let Err(_) = ctx.spawn_child_workflow_fan_out_raw(children).await else {
            panic!("expected the fan-out to fail fast");
        };
        // Caught the error and continued -- push an unrelated suspending
        // command (ScheduleActivity).
        ctx.execute_activity_raw("unrelated_activity", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({ "continued": true }))
    })
}

fn child_fan_out_collect_all<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let children = vec![
            ("child_ok".to_string(), Value::Null),
            ("child_fail".to_string(), Value::Null),
            ("child_ok2".to_string(), Value::Null),
        ];
        let results = ctx
            .spawn_child_workflow_fan_out_collect_raw(children)
            .await
            .map_err(|e| e.to_string())?;
        let serialized: Vec<Value> = results
            .into_iter()
            .map(|r| match r {
                Ok(v) => json!({ "ok": v }),
                Err(e) => json!({ "err": e }),
            })
            .collect();
        Ok(json!({ "results": serialized }))
    })
}

fn child_fan_out_empty<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let results = ctx
            .spawn_child_workflow_fan_out_raw(vec![])
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({ "count": results.len() }))
    })
}

// Fan out with N derived from a prior activity result.
fn child_fan_out_dynamic_from_prior<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let items_json = ctx
            .execute_activity_raw("list_items", json!(null), "default")
            .await
            .map_err(|e| e.to_string())?;
        let items = items_json.as_array().cloned().unwrap_or_default();

        let children: Vec<_> = items
            .into_iter()
            .map(|item| ("process_item_workflow".to_string(), item))
            .collect();
        let results = ctx
            .spawn_child_workflow_fan_out_raw(children)
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({ "processed": results }))
    })
}

// Workflow for non-determinism (count mismatch) detection test.
fn child_fan_out_count_changed<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let n = usize::try_from(input.as_u64().unwrap_or(2)).unwrap_or(2);
        let children: Vec<_> = (0..n).map(|i| (format!("child_{i}"), json!(i))).collect();
        let results = ctx
            .spawn_child_workflow_fan_out_raw(children)
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({ "count": results.len() }))
    })
}

// Two independent fan-out groups (one activity, one child) in the same workflow,
// verifying the shared `fan_out_seq` counter assigns distinct sequence numbers.
fn mixed_activity_then_child_fan_out<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let activity_results = ctx
            .execute_activity_fan_out_raw(vec![
                ("a1".to_string(), json!(null), "default".to_string()),
                ("a2".to_string(), json!(null), "default".to_string()),
            ])
            .await
            .map_err(|e| e.to_string())?;
        let child_results = ctx
            .spawn_child_workflow_fan_out_raw(vec![("b1_child".to_string(), json!(null))])
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({ "activities": activity_results, "children": child_results }))
    })
}

fn started() -> WorkflowEvent {
    WorkflowEvent::WorkflowStarted {
        input: Value::Null,
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }
}

fn child_started(child_id: ExecutionId, workflow_name: &str, input: Value) -> WorkflowEvent {
    WorkflowEvent::ChildWorkflowStarted {
        child_id,
        workflow_name: workflow_name.to_string(),
        input,
    }
}

const fn child_completed(child_id: ExecutionId, output: Value) -> WorkflowEvent {
    WorkflowEvent::ChildWorkflowCompleted { child_id, output }
}

fn child_failed(child_id: ExecutionId, error: &str) -> WorkflowEvent {
    WorkflowEvent::child_workflow_failed(child_id, error.to_string())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Basic fan-out: 3 children in parallel, all complete successfully.
/// The workflow should return results in input order (not completion order).
#[tokio::test]
async fn child_fan_out_raw_three_parallel_all_succeed() {
    let exec_id = ExecutionId::new();
    let id_a = ExecutionId::new();
    let id_b = ExecutionId::new();
    let id_c = ExecutionId::new();

    let history = vec![
        started(),
        WorkflowEvent::MarkerRecorded {
            name: "fan_out:1".into(),
            details: json!(3u64),
        },
        child_started(id_a, "child_a", json!("input_a")),
        child_started(id_b, "child_b", json!("input_b")),
        child_started(id_c, "child_c", json!("input_c")),
        // Completions out-of-order: C finishes first, then A, then B.
        child_completed(id_c, json!("result_c")),
        child_completed(id_a, json!("result_a")),
        child_completed(id_b, json!("result_b")),
    ];

    let outcome = run_workflow(exec_id, history, child_fan_out_three_parallel, Value::Null).await;

    match outcome {
        WorkflowOutcome::Completed { output, .. } => {
            let results = output["results"].as_array().unwrap();
            assert_eq!(results.len(), 3, "should have 3 results");
            assert_eq!(results[0], json!("result_a"), "slot 0 should be result_a");
            assert_eq!(results[1], json!("result_b"), "slot 1 should be result_b");
            assert_eq!(results[2], json!("result_c"), "slot 2 should be result_c");
        }
        other => panic!("expected Completed, got {other:?}"),
    }
}

/// Fan-out fail-fast: one child fails, the whole fan-out fails.
#[tokio::test]
async fn child_fan_out_raw_fail_fast_on_first_failure() {
    let exec_id = ExecutionId::new();
    let id_ok = ExecutionId::new();
    let id_fail = ExecutionId::new();
    let id_ok2 = ExecutionId::new();

    let history = vec![
        started(),
        WorkflowEvent::MarkerRecorded {
            name: "fan_out:1".into(),
            details: json!(3u64),
        },
        child_started(id_ok, "child_ok", Value::Null),
        child_started(id_fail, "child_fail", Value::Null),
        child_started(id_ok2, "child_ok2", Value::Null),
        child_completed(id_ok, json!("success")),
        child_failed(id_fail, "boom"),
        child_completed(id_ok2, json!("also_success")),
    ];

    let outcome = run_workflow(exec_id, history, child_fan_out_fail_fast, Value::Null).await;

    match outcome {
        WorkflowOutcome::Failed { error, .. } => {
            assert!(
                error.contains("boom"),
                "error should mention 'boom', got: {error}"
            );
        }
        other => panic!("expected Failed, got {other:?}"),
    }
}

/// **Known limitation** (PR #901 review, shared with the pre-existing activity
/// fan-out from issue #359 -- not introduced by child fan-out): when two or
/// more children fail and their terminals are all already recorded (a full
/// replay, e.g. crash recovery or `WorkflowReplayer`), the fail-fast fan-out
/// selects the failure at the **lowest input-slot index**, not necessarily the
/// one whose `ChildWorkflowFailed` event was recorded earliest in history.
///
/// `try_join_all` resolves every slot's future synchronously on its first poll
/// once a terminal is already in history (`match_child_workflow` is a pure,
/// synchronous history scan with no actual suspension for an already-recorded
/// `Matched`/`Failed` result), so a single poll pass finds whichever slot's
/// future is ready-with-`Err` first while iterating in **slot order** --
/// this is *usually* the same as recorded order (each terminal typically
/// arrives in its own wake/replay cycle), but diverges when two terminals
/// land in history before the same decision cycle observes either of them.
///
/// This test locks in the *current* behavior for two reasons: (1) it proves
/// the divergence is real, and (2) it guards against a silent, undetected
/// behavior change (e.g. from a future refactor of the fan-out or the
/// underlying `try_join_all` usage) going unnoticed. A workflow that branches
/// on the *content* of the returned fail-fast error (rather than treating any
/// fan-out failure identically) should be aware this selection is
/// **not** guaranteed to reproduce the live run's chronological "first
/// failure" on replay.
///
/// Fixing this properly requires threading the recorded terminal event's
/// index through `HistoryMatch::Failed` and selecting by that index rather
/// than by `try_join_all`'s poll order -- a change that would need to apply
/// equally to `execute_activity_fan_out_raw` to stay consistent between the
/// two fan-out primitives, and is out of scope for issue #601 (which mirrors
/// the pre-existing activity fan-out mechanism deliberately, defects
/// included).
#[tokio::test]
async fn child_fan_out_raw_known_limitation_replay_selects_by_slot_order_not_recorded_order() {
    let exec_id = ExecutionId::new();
    let id_ok = ExecutionId::new();
    let id_fail_low_slot = ExecutionId::new();
    let id_fail_high_slot = ExecutionId::new();

    let history = vec![
        started(),
        WorkflowEvent::MarkerRecorded {
            name: "fan_out:1".into(),
            details: json!(3u64),
        },
        // Started events must appear in slot order (0, 1, 2) -- match_child_workflow
        // matches each slot's own child by cursor position.
        child_started(id_ok, "child_ok", Value::Null),
        child_started(id_fail_low_slot, "child_fail_low_slot", Value::Null),
        child_started(id_fail_high_slot, "child_fail_high_slot", Value::Null),
        child_completed(id_ok, json!("success")),
        // The HIGH-slot child (index 2) is recorded as having failed FIRST --
        // i.e. on the original live run, this is the failure that actually
        // won the real chronological race and was surfaced to the workflow.
        child_failed(id_fail_high_slot, "high_slot_failed_first_chronologically"),
        // The LOW-slot child (index 1) failed second, chronologically.
        child_failed(id_fail_low_slot, "low_slot_failed_second_chronologically"),
    ];

    let outcome = run_workflow(exec_id, history, child_fan_out_two_failures, Value::Null).await;

    match outcome {
        WorkflowOutcome::Failed { error, .. } => {
            // Known-limitation assertion: replay returns the LOW-slot error
            // (index order) even though the HIGH-slot error was recorded
            // first (and is what the live run actually returned). If this
            // assertion ever starts failing, the underlying selection
            // mechanism has changed -- update this test and the doc comment
            // above rather than treating it as a regression.
            assert!(
                error.contains("low_slot_failed_second_chronologically"),
                "replay is expected to select the lowest-slot-index failure \
                 regardless of recorded order (known limitation) -- got: {error}"
            );
        }
        other => panic!("expected Failed, got {other:?}"),
    }
}

/// **Known limitation** (PR #901 review, shared with the pre-existing activity
/// fan-out from issue #359 -- not introduced by child fan-out): when an
/// earlier-slot child is still `ChildInProgress` (started, no terminal
/// recorded yet) while a later-slot child has a recorded failure,
/// `try_join_all` polls the earlier slot first during the same sweep that
/// discovers the fail-fast error. `spawn_child_workflow_raw`'s
/// `ChildInProgress` branch synchronously pushes a re-park
/// `StartChildWorkflow` command (carrying the existing `child_id`) *before*
/// suspending on its own oneshot -- and that push is not undone when
/// `try_join_all` subsequently short-circuits on the later slot's `Err` and
/// drops the still-pending future. If the workflow catches the fail-fast
/// error and pushes a different command (e.g. schedules an activity), both
/// commands land in the *same* suspension batch: a stale re-park alongside
/// an unrelated new command. The worker's dispatch logic (`worker.rs`,
/// `suspended_workflow_error`) only recognizes single-shape suspension
/// batches, so this mixed batch hits "workflow task suspended with
/// unsupported commands ...; this command set is not implemented yet" and
/// fails the workflow outright -- even though the user only handled the
/// fan-out error and continued normally.
///
/// `execute_activity_fan_out_raw` (issue #359) has the identical structure
/// (`ActivityInProgress` also pushes `WaitForActivity` then suspends before
/// returning), so this is a pre-existing, shared limitation, not something
/// introduced by child fan-out. Properly fixing it needs either abandoning
/// `try_join_all`'s natural poll-all-every-time semantics for a custom
/// combinator that never touches a sibling slot once the fail-fast winner is
/// known, or a side-effect-free way to peek "is this child already failed in
/// history" against the stateful, cursor-based `HistoryMatcher` without
/// consuming its cursor -- both are large, cross-cutting changes shared with
/// the activity fan-out primitive, out of scope for issue #601.
///
/// This test proves the defect at the `WorkflowOutcome` level (a full
/// worker-level reproduction of the resulting `WorkflowFailed` needs a
/// DB-backed integration test, since the "unsupported commands" check lives
/// in `worker.rs`, not the pure `executor::run_workflow` used here): the
/// commands returned alongside the fail-fast error already contain the
/// stale `StartChildWorkflow` re-park for the still-pending sibling.
#[tokio::test]
async fn child_fan_out_raw_known_limitation_stale_repark_command_leaks_into_next_suspension() {
    let exec_id = ExecutionId::new();
    let id_pending = ExecutionId::new();
    let id_fail = ExecutionId::new();

    let history = vec![
        started(),
        WorkflowEvent::MarkerRecorded {
            name: "fan_out:1".into(),
            details: json!(2u64),
        },
        // Slot 0: started, no terminal -- still ChildInProgress.
        child_started(id_pending, "child_pending", Value::Null),
        // Slot 1: started and failed.
        child_started(id_fail, "child_fail", Value::Null),
        child_failed(id_fail, "boom"),
    ];

    let outcome = run_workflow(
        exec_id,
        history,
        child_fan_out_catch_and_continue,
        Value::Null,
    )
    .await;

    match outcome {
        WorkflowOutcome::Suspended { commands } => {
            let has_stale_repark = commands.iter().any(|c| {
                matches!(
                    c,
                    autumn_harvest::context::WorkflowCommand::StartChildWorkflow {
                        child_id,
                        ..
                    } if *child_id == id_pending
                )
            });
            let has_unrelated_activity = commands.iter().any(|c| {
                matches!(
                    c,
                    autumn_harvest::context::WorkflowCommand::ScheduleActivity { name, .. }
                        if name == "unrelated_activity"
                )
            });
            assert!(
                has_stale_repark,
                "known limitation: the still-pending sibling's re-park command \
                 should leak into this suspension batch -- got: {commands:?}"
            );
            assert!(
                has_unrelated_activity,
                "the workflow's post-catch activity should also be in this batch -- \
                 got: {commands:?}"
            );
            assert_eq!(
                commands.len(),
                2,
                "batch should contain exactly the stale re-park plus the new \
                 activity -- a worker receiving this mixed batch has no single \
                 recognized shape to persist it as; got: {commands:?}"
            );
        }
        other => panic!(
            "expected Suspended with a mixed stale-repark + new-command batch \
             (known limitation), got: {other:?}"
        ),
    }
}

/// Fan-out collect-all: one child fails, others succeed.
/// All per-slot results are returned (not fail-fast).
#[tokio::test]
async fn child_fan_out_collect_all_returns_per_slot_results() {
    let exec_id = ExecutionId::new();
    let id_ok = ExecutionId::new();
    let id_fail = ExecutionId::new();
    let id_ok2 = ExecutionId::new();

    let history = vec![
        started(),
        WorkflowEvent::MarkerRecorded {
            name: "fan_out:1".into(),
            details: json!(3u64),
        },
        child_started(id_ok, "child_ok", Value::Null),
        child_started(id_fail, "child_fail", Value::Null),
        child_started(id_ok2, "child_ok2", Value::Null),
        child_completed(id_ok, json!("success")),
        child_failed(id_fail, "slot_1_failed"),
        child_completed(id_ok2, json!("also_success")),
    ];

    let outcome = run_workflow(exec_id, history, child_fan_out_collect_all, Value::Null).await;

    match outcome {
        WorkflowOutcome::Completed { output, .. } => {
            let results = output["results"].as_array().unwrap();
            assert_eq!(results.len(), 3, "collect-all should return all 3 slots");
            assert!(results[0].get("ok").is_some(), "slot 0 should be ok");
            assert_eq!(results[0]["ok"], json!("success"));
            assert!(results[1].get("err").is_some(), "slot 1 should be err");
            let err_msg = results[1]["err"].as_str().unwrap();
            assert!(
                err_msg.contains("slot_1_failed"),
                "slot 1 error message mismatch"
            );
            assert!(results[2].get("ok").is_some(), "slot 2 should be ok");
            assert_eq!(results[2]["ok"], json!("also_success"));
        }
        other => panic!("expected Completed, got {other:?}"),
    }
}

/// Empty fan-out: zero children should return immediately with empty Vec.
#[tokio::test]
async fn child_fan_out_empty_returns_empty_vec() {
    let exec_id = ExecutionId::new();
    let history = vec![started()];

    let outcome = run_workflow(exec_id, history, child_fan_out_empty, Value::Null).await;

    match outcome {
        WorkflowOutcome::Completed { output, .. } => {
            assert_eq!(
                output["count"],
                json!(0),
                "empty fan-out should return 0 results"
            );
        }
        other => panic!("expected Completed, got {other:?}"),
    }
}

/// First-time live execution: no history → fan-out suspends emitting commands.
/// Verifies all N children are scheduled *before* any is awaited (a single
/// suspension batch carries the marker + all `StartChildWorkflow` commands).
#[tokio::test]
async fn child_fan_out_live_execution_emits_marker_and_start_commands() {
    let exec_id = ExecutionId::new();
    let history = vec![started()];

    let outcome = run_workflow(exec_id, history, child_fan_out_three_parallel, Value::Null).await;

    match outcome {
        WorkflowOutcome::Suspended { commands } => {
            let has_marker = commands.iter().any(|c| {
                matches!(c, autumn_harvest::context::WorkflowCommand::RecordMarker {
                    name, ..
                } if name.starts_with("fan_out:"))
            });
            assert!(
                has_marker,
                "should emit a fan_out marker command; got: {commands:?}"
            );

            let start_count = commands
                .iter()
                .filter(|c| {
                    matches!(
                        c,
                        autumn_harvest::context::WorkflowCommand::StartChildWorkflow { .. }
                    )
                })
                .count();
            assert_eq!(
                start_count, 3,
                "should emit 3 StartChildWorkflow commands; got {commands:?}"
            );
        }
        other => panic!("expected Suspended, got {other:?}"),
    }
}

/// Non-determinism detection: fan-out called with different count than recorded.
#[tokio::test]
async fn child_fan_out_count_mismatch_returns_non_deterministic_error() {
    let exec_id = ExecutionId::new();
    let id_a = ExecutionId::new();
    let id_b = ExecutionId::new();
    let id_c = ExecutionId::new();

    let history = vec![
        started(),
        WorkflowEvent::MarkerRecorded {
            name: "fan_out:1".into(),
            details: json!(3u64), // recorded 3
        },
        child_started(id_a, "child_0", json!(0u64)),
        child_started(id_b, "child_1", json!(1u64)),
        child_started(id_c, "child_2", json!(2u64)),
        child_completed(id_a, json!("r0")),
        child_completed(id_b, json!("r1")),
        child_completed(id_c, json!("r2")),
    ];

    // New code passes input=2 which creates only 2 children (changed from 3).
    let outcome = run_workflow(exec_id, history, child_fan_out_count_changed, json!(2u64)).await;

    match outcome {
        WorkflowOutcome::Failed { error, .. } => {
            assert!(
                error.to_lowercase().contains("non-deterministic")
                    || error.to_lowercase().contains("fan_out")
                    || error.to_lowercase().contains("count"),
                "error should mention non-determinism or fan_out count; got: {error}"
            );
        }
        other => panic!("expected Failed (non-determinism), got {other:?}"),
    }
}

/// Fan-out with dynamic N derived from a prior activity output.
#[tokio::test]
async fn child_fan_out_dynamic_from_prior_activity_replays_correctly() {
    use autumn_harvest::types::ActivityExecId;

    let exec_id = ExecutionId::new();
    let list_id = ActivityExecId::new();
    let item_ids: Vec<ExecutionId> = (0..3).map(|_| ExecutionId::new()).collect();

    let history = vec![
        started(),
        WorkflowEvent::ActivityScheduled {
            activity_id: list_id,
            name: "list_items".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: list_id,
            output: json!([1, 2, 3]),
        },
        WorkflowEvent::MarkerRecorded {
            name: "fan_out:1".into(),
            details: json!(3u64),
        },
        child_started(item_ids[0], "process_item_workflow", json!(1)),
        child_started(item_ids[1], "process_item_workflow", json!(2)),
        child_started(item_ids[2], "process_item_workflow", json!(3)),
        child_completed(item_ids[0], json!("done_1")),
        child_completed(item_ids[1], json!("done_2")),
        child_completed(item_ids[2], json!("done_3")),
    ];

    let outcome = run_workflow(
        exec_id,
        history,
        child_fan_out_dynamic_from_prior,
        Value::Null,
    )
    .await;

    match outcome {
        WorkflowOutcome::Completed { output, .. } => {
            let processed = output["processed"].as_array().unwrap();
            assert_eq!(processed.len(), 3, "should process 3 items");
            assert_eq!(processed[0], json!("done_1"));
            assert_eq!(processed[1], json!("done_2"));
            assert_eq!(processed[2], json!("done_3"));
        }
        other => panic!("expected Completed, got {other:?}"),
    }
}

/// Cancellation test: workflow cancelled → fan-out returns Cancelled.
#[tokio::test]
async fn child_fan_out_cancelled_workflow_returns_cancelled_error() {
    let exec_id = ExecutionId::new();

    let history = vec![
        started(),
        WorkflowEvent::WorkflowCancelled {
            reason: "user_requested".into(),
        },
    ];

    let outcome = run_workflow(exec_id, history, child_fan_out_three_parallel, Value::Null).await;

    match outcome {
        WorkflowOutcome::Failed { error, .. } => {
            assert!(
                error.contains("cancelled") || error.contains("cancel"),
                "cancelled workflow fan-out should report cancellation; got: {error}"
            );
        }
        other => panic!("expected Failed (Cancelled), got {other:?}"),
    }
}

/// Partial history: all children started, only some terminals recorded.
/// The parent should re-park, re-emitting `StartChildWorkflow` for the
/// still-pending child carrying its *existing* `child_id` (no duplicate spawn).
#[tokio::test]
async fn child_fan_out_partial_history_re_parks_pending_children() {
    let exec_id = ExecutionId::new();
    let id_a = ExecutionId::new();
    let id_b = ExecutionId::new();
    let id_c = ExecutionId::new();

    let history = vec![
        started(),
        WorkflowEvent::MarkerRecorded {
            name: "fan_out:1".into(),
            details: json!(3u64),
        },
        child_started(id_a, "child_a", json!("input_a")),
        child_started(id_b, "child_b", json!("input_b")),
        child_started(id_c, "child_c", json!("input_c")),
        // Only child_a has completed; b and c are still in flight.
        child_completed(id_a, json!("result_a")),
    ];

    let outcome = run_workflow(exec_id, history, child_fan_out_three_parallel, Value::Null).await;

    match outcome {
        WorkflowOutcome::Suspended { commands } => {
            let reparked: Vec<_> = commands
                .iter()
                .filter_map(|c| match c {
                    autumn_harvest::context::WorkflowCommand::StartChildWorkflow {
                        child_id,
                        ..
                    } => Some(*child_id),
                    _ => None,
                })
                .collect();
            assert_eq!(
                reparked.len(),
                2,
                "should re-park exactly the 2 still-pending children; got: {commands:?}"
            );
            assert!(
                reparked.contains(&id_b),
                "should re-park child_b's existing id"
            );
            assert!(
                reparked.contains(&id_c),
                "should re-park child_c's existing id"
            );
        }
        other => panic!("expected Suspended (re-park), got {other:?}"),
    }
}

/// Shared sequence counter: an activity fan-out followed by a child fan-out
/// in the same workflow gets `fan_out:1` then `fan_out:2` -- not two `fan_out:1`s.
#[tokio::test]
async fn child_fan_out_shares_seq_counter_with_activity_fan_out() {
    use autumn_harvest::types::ActivityExecId;

    let exec_id = ExecutionId::new();
    let a1 = ActivityExecId::new();
    let a2 = ActivityExecId::new();
    let b1 = ExecutionId::new();

    let history = vec![
        started(),
        // First fan-out group (activities, seq=1, count=2)
        WorkflowEvent::MarkerRecorded {
            name: "fan_out:1".into(),
            details: json!(2u64),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: a1,
            name: "a1".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: a2,
            name: "a2".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: a1,
            output: json!("ra1"),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: a2,
            output: json!("ra2"),
        },
        // Second fan-out group (children, seq=2, count=1)
        WorkflowEvent::MarkerRecorded {
            name: "fan_out:2".into(),
            details: json!(1u64),
        },
        child_started(b1, "b1_child", Value::Null),
        child_completed(b1, json!("rb1")),
    ];

    let outcome = run_workflow(
        exec_id,
        history,
        mixed_activity_then_child_fan_out,
        Value::Null,
    )
    .await;

    match outcome {
        WorkflowOutcome::Completed { output, .. } => {
            let activities = output["activities"].as_array().unwrap();
            let children = output["children"].as_array().unwrap();
            assert_eq!(activities.len(), 2);
            assert_eq!(children.len(), 1);
            assert_eq!(activities[0], json!("ra1"));
            assert_eq!(activities[1], json!("ra2"));
            assert_eq!(children[0], json!("rb1"));
        }
        other => panic!("expected Completed, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// WorkflowContext unit tests (typed API, no executor needed)
// ---------------------------------------------------------------------------

fn make_workflow_info(name: &'static str) -> autumn_harvest::info::WorkflowInfo {
    autumn_harvest::info::WorkflowInfo {
        mcp: false,
        name,
        module: "test",
        handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        execution_timeout: None,
        sla: None,
        concurrency: None,
        debounce: None,
        batch: None,
        throttle: None,
        max_input_bytes: None,
        owner: None,
        runbook_url: None,
        severity: None,
        description: None,
        input_schema: None,
        output_schema: None,
        error_schema: None,
        retry_policy: None,
    }
}

/// Typed fan-out: `spawn_child_workflow_fan_out` accepts `(&WorkflowInfo, I)` pairs.
#[tokio::test]
async fn child_fan_out_typed_replays_completed_output() {
    let info = make_workflow_info("echo_child");

    let exec_id = ExecutionId::new();
    let id_1 = ExecutionId::new();
    let id_2 = ExecutionId::new();

    let history = vec![
        started(),
        WorkflowEvent::MarkerRecorded {
            name: "fan_out:1".into(),
            details: json!(2u64),
        },
        child_started(id_1, "echo_child", json!("hello")),
        child_started(id_2, "echo_child", json!("world")),
        child_completed(id_1, json!("hello")),
        child_completed(id_2, json!("world")),
    ];

    let ctx = WorkflowContext::for_replay(exec_id, history);
    let results: Vec<String> = ctx
        .spawn_child_workflow_fan_out(&info, vec!["hello".to_string(), "world".to_string()])
        .await
        .expect("fan-out should succeed");

    assert_eq!(results, vec!["hello".to_string(), "world".to_string()]);
}

/// Typed collect-all fan-out: mixed results returned as `Vec<Result<O, String>>`.
#[tokio::test]
async fn child_fan_out_typed_collect_all_returns_per_slot_results() {
    let info = make_workflow_info("maybe_fail_child");

    let exec_id = ExecutionId::new();
    let id_1 = ExecutionId::new();
    let id_2 = ExecutionId::new();

    let history = vec![
        started(),
        WorkflowEvent::MarkerRecorded {
            name: "fan_out:1".into(),
            details: json!(2u64),
        },
        child_started(id_1, "maybe_fail_child", json!("ok_input")),
        child_started(id_2, "maybe_fail_child", json!("fail_input")),
        child_completed(id_1, json!("ok_output")),
        child_failed(id_2, "deliberate_failure"),
    ];

    let ctx = WorkflowContext::for_replay(exec_id, history);
    let results: Vec<Result<Value, String>> = ctx
        .spawn_child_workflow_fan_out_collect(&info, vec![json!("ok_input"), json!("fail_input")])
        .await
        .expect("collect should not fail at the workflow level");

    assert_eq!(results.len(), 2);
    assert!(results[0].is_ok(), "slot 0 should be ok");
    assert_eq!(results[0].as_ref().unwrap(), &json!("ok_output"));
    assert!(results[1].is_err(), "slot 1 should be err");
    assert!(
        results[1]
            .as_ref()
            .unwrap_err()
            .contains("deliberate_failure")
    );
}

// ---------------------------------------------------------------------------
// Post-review hardening: all-or-nothing dispatch on payload-cap overflow
// ---------------------------------------------------------------------------

/// A fresh fan-out where one child's input exceeds the payload cap must fail
/// with `PayloadTooLarge` *before dispatching any child* -- not leave earlier
/// siblings' `StartChildWorkflow` commands sitting in the drained command
/// list while the batch as a whole fails. Proves the dispatch is genuinely
/// all-or-nothing: zero `StartChildWorkflow` commands are ever pushed, not a
/// partial N-of-M count that would depend on `try_join_all`'s poll order.
#[tokio::test]
async fn child_fan_out_raw_oversized_child_rejects_before_dispatching_any_sibling() {
    let exec_id = ExecutionId::new();
    let history = vec![started()];

    // max_workflow_input capped tiny (100 bytes); the small child fits, the
    // second child's input does not.
    let ctx = WorkflowContext::for_replay(exec_id, history).with_payload_caps(
        1024 * 1024,
        1024 * 1024,
        1024 * 1024,
        100,
    );

    let small = json!("small");
    let oversized = json!({ "data": "x".repeat(1000) });

    let result = ctx
        .spawn_child_workflow_fan_out_raw(vec![
            ("child_small".to_string(), small),
            ("child_big".to_string(), oversized),
        ])
        .await;

    match result {
        Err(HarvestError::PayloadTooLarge { kind, .. }) => {
            assert_eq!(kind, PayloadKind::ChildWorkflowInput);
        }
        other => panic!("expected PayloadTooLarge, got {other:?}"),
    }

    // Zero commands of ANY kind -- not even the `fan_out:{n}` marker --
    // must be pushed. If the marker were recorded but no children were, a
    // later replay of this exact terminal history would match the marker
    // (fresh_dispatch == false), skip the cap re-check, and then diverge
    // looking for `ChildWorkflowStarted` events that were never recorded
    // (see `replayer_...` coverage in tests/replayer_tests.rs).
    let commands = ctx.drain_commands();
    assert!(
        commands.is_empty(),
        "no command -- not even the fan_out marker -- should be pushed when any \
         sibling exceeds the payload cap on a fresh dispatch; got: {commands:?}"
    );
}

/// A fan-out group already fully recorded in history (both children
/// `Matched`) must replay successfully even if `payload_max_workflow_input`
/// has since been lowered below what one child's original input required --
/// the payload pre-check must never re-run on replay, only on a fresh
/// (first-time) dispatch, or a config change alone could make an
/// already-completed execution fail non-deterministically on replay.
#[tokio::test]
async fn child_fan_out_raw_replay_ignores_current_payload_cap_for_already_recorded_children() {
    let exec_id = ExecutionId::new();
    let id_a = ExecutionId::new();
    let id_b = ExecutionId::new();

    // child_b's recorded input is 500 bytes -- would exceed a 100-byte cap
    // if the pre-check ran on replay, but it must not.
    let large_input = json!({ "data": "x".repeat(500) });

    let history = vec![
        started(),
        WorkflowEvent::MarkerRecorded {
            name: "fan_out:1".into(),
            details: json!(2u64),
        },
        child_started(id_a, "child_a", json!("small")),
        child_started(id_b, "child_b", large_input.clone()),
        child_completed(id_a, json!("result_a")),
        child_completed(id_b, json!("result_b")),
    ];

    // Cap lowered to 100 bytes -- below child_b's recorded 500-byte input --
    // simulating a config change made after the original run.
    let ctx = WorkflowContext::for_replay(exec_id, history).with_payload_caps(
        1024 * 1024,
        1024 * 1024,
        1024 * 1024,
        100,
    );

    let results = ctx
        .spawn_child_workflow_fan_out_raw(vec![
            ("child_a".to_string(), json!("small")),
            ("child_b".to_string(), large_input),
        ])
        .await
        .expect("replay of an already-recorded fan-out must not re-validate the payload cap");

    assert_eq!(results, vec![json!("result_a"), json!("result_b")]);
}

/// A workflow that catches a fan-out's `PayloadTooLarge` (a normal,
/// supported "degrade gracefully" pattern -- the caller can `match`/`map_err`
/// on the returned `HarvestResult` instead of propagating with `?`) and then
/// runs a *second*, successful fan-out must have the second call's marker
/// number stay one past the first, even though the first call recorded no
/// marker at all.
///
/// This looks unintuitive but is deliberate: an earlier revision tried
/// *releasing* the failed call's number so the second call could reuse it.
/// That is a **worse** bug -- it lets an unrelated fan-out call site inherit
/// another call's number, so a later replay of the *first* call could match
/// against the *second* call's marker/count (or, in the collect-all-error
/// case, its recorded children), silently misattributing unrelated data
/// instead of cleanly diverging. Burning the number instead produces an
/// honest "expected `fan_out:1`, got `fan_out:2`" divergence on replay -- which
/// still doesn't gracefully reproduce the caught error (that's the same
/// broader, pre-existing engine limitation documented on
/// `known_limitation_early_config_dependent_failure_does_not_replay_cleanly`,
/// `tests/replayer_tests.rs`), but it is never silently wrong.
#[tokio::test]
async fn child_fan_out_raw_burns_seq_on_validation_failure_so_next_fan_out_never_reuses_it() {
    let exec_id = ExecutionId::new();
    let history = vec![started()];

    let ctx = WorkflowContext::for_replay(exec_id, history).with_payload_caps(
        1024 * 1024,
        1024 * 1024,
        1024 * 1024,
        100,
    );

    // First fan-out: one child exceeds the 100-byte cap -- fails before any
    // marker is recorded, but its sequence number (1) must still be burned.
    let oversized = json!({ "data": "x".repeat(1000) });
    let first = ctx
        .spawn_child_workflow_fan_out_raw(vec![("child_big".to_string(), oversized)])
        .await;
    assert!(
        matches!(first, Err(HarvestError::PayloadTooLarge { .. })),
        "expected the first fan-out to fail on payload cap: {first:?}"
    );

    // Second fan-out: a small, valid input. Nothing in this no-worker unit
    // test resolves the child's oneshot, so the call suspends forever
    // waiting on it -- confirmed via a short outer timeout. What matters is
    // the SYNCHRONOUS prefix (peek/validate/record) that runs before that
    // await point: the marker it records must be "fan_out:2" (seq 1 was
    // burned by the failed first call, never reused).
    let second = tokio::time::timeout(
        std::time::Duration::from_millis(50),
        ctx.spawn_child_workflow_fan_out_raw(vec![("child_small".to_string(), json!("small"))]),
    )
    .await;
    assert!(
        second.is_err(),
        "second fan-out should suspend cleanly waiting on its child's \
         (never-resolved) oneshot, not fail synchronously: {second:?}"
    );

    let commands = ctx.drain_commands();
    let marker_names: Vec<String> = commands
        .iter()
        .filter_map(|c| match c {
            autumn_harvest::context::WorkflowCommand::RecordMarker { name, .. } => {
                Some(name.clone())
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        marker_names,
        vec!["fan_out:2".to_string()],
        "the second (successful) fan-out must NOT reuse seq 1 -- the \
         failed first attempt must permanently burn it so no later fan-out \
         call site can ever be confused with it; got markers: \
         {marker_names:?}, all commands: {commands:?}"
    );
}
