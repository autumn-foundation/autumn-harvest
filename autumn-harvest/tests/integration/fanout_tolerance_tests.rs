//! Fan-out failure tolerance and result writer (issue #1986).
//!
//! Pure tests. They need no database. A driver replays each suspension's
//! commands back as recorded events, as `fanout_tests.rs` does.

use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;

use autumn_harvest::context::{WorkflowCommand, WorkflowContext};
use autumn_harvest::error::HarvestError;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::executor::{WorkflowOutcome, run_workflow, run_workflow_with_context};
use autumn_harvest::fan_out::{
    FailureTolerance, FanOutItem, FanOutOptions, FanOutResults, StoredResult,
};
use autumn_harvest::info::WorkflowHandlerFn;
use autumn_harvest::types::ExecutionId;
use chrono::Utc;
use serde_json::{Value, json};

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Read the fan-out options from the workflow input.
///
/// Input shape: `{ "n", "w"?, "count"?, "percent"?, "writer"? }`.
fn options_from(input: &Value) -> FanOutOptions {
    let mut options = FanOutOptions::new();
    if let Some(w) = input["w"].as_u64() {
        options = options.max_in_flight(usize::try_from(w).unwrap());
    }
    if let Some(k) = input["count"].as_u64() {
        options = options.tolerate(FailureTolerance::Count(usize::try_from(k).unwrap()));
    }
    if let Some(p) = input["percent"].as_u64() {
        options = options.tolerate(FailureTolerance::Percent(u8::try_from(p).unwrap()));
    }
    if input["writer"].as_bool() == Some(true) {
        options = options.write_results();
    }
    options
}

/// Fan out `n` items named `item`. The handler catches the threshold error and
/// completes, so a test can see the error fields and the #1791 drift check.
fn tolerant_handler<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let n = input["n"].as_u64().unwrap_or(0);
        let activities: Vec<_> = (0..n)
            .map(|i| ("item".to_string(), json!(i), "default".to_string()))
            .collect();
        match ctx
            .execute_activity_fan_out_raw_with(activities, &options_from(&input))
            .await
        {
            Ok(results) => Ok(json!({
                "failed": results.failed_count(),
                "succeeded": results.succeeded_count(),
                "results": results,
            })),
            Err(HarvestError::FanOutFailureThresholdExceeded { tolerated, total }) => {
                Ok(json!({ "exceeded": { "tolerated": tolerated, "total": total } }))
            }
            Err(e) => Err(e.to_string()),
        }
    })
}

/// What one drive of a workflow produced.
struct Drive {
    outcome: WorkflowOutcome,
    scheduled: usize,
    writer_flags: Vec<bool>,
    history: Vec<WorkflowEvent>,
}

fn started(input: &Value) -> WorkflowEvent {
    WorkflowEvent::WorkflowStarted {
        input: input.clone(),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }
}

/// Drive `handler` to a terminal outcome.
///
/// Each `ScheduleActivity` becomes `ActivityScheduled` plus the result that
/// `output_fn(input)` gives. `store` sets the context's offload threshold.
async fn drive<F>(
    handler: WorkflowHandlerFn,
    input: Value,
    store: bool,
    output_fn: F,
) -> Drive
where
    F: Fn(&Value) -> Result<Value, String>,
{
    let exec_id = ExecutionId::new();
    let mut history = vec![started(&input)];
    let mut scheduled = 0usize;
    let mut writer_flags = Vec::new();

    for _cycle in 0..1000 {
        let ctx = WorkflowContext::for_replay(exec_id, history.clone())
            .with_payload_offload_threshold(store.then_some(1024));
        let outcome = run_workflow_with_context(ctx, handler, input.clone()).await;
        let WorkflowOutcome::Suspended { commands } = outcome else {
            return Drive {
                outcome,
                scheduled,
                writer_flags,
                history,
            };
        };
        for cmd in &commands {
            match cmd {
                WorkflowCommand::RecordMarker { name, details } => {
                    history.push(WorkflowEvent::MarkerRecorded {
                        name: name.clone(),
                        details: details.clone(),
                    });
                }
                WorkflowCommand::ScheduleActivity {
                    activity_id,
                    name,
                    input,
                    queue,
                    result_writer,
                    ..
                } => {
                    scheduled += 1;
                    writer_flags.push(*result_writer);
                    history.push(WorkflowEvent::ActivityScheduled {
                        activity_id: *activity_id,
                        name: name.clone(),
                        input: input.clone(),
                        queue: queue.clone(),
                    });
                    history.push(match output_fn(input) {
                        Ok(output) => WorkflowEvent::ActivityCompleted {
                            activity_id: *activity_id,
                            output,
                        },
                        Err(error) => WorkflowEvent::ActivityFailed {
                            activity_id: *activity_id,
                            error,
                            attempt: 1,
                            error_type: "Error".into(),
                            non_retryable: true,
                            details: None,
                        },
                    });
                }
                other => panic!("unexpected command: {other:?}"),
            }
        }
    }
    panic!("the fan-out did not terminate within 1000 cycles");
}

/// Items in `failing` fail. Every other item `i` returns `i * 10`.
fn fail_on(failing: &[u64]) -> impl Fn(&Value) -> Result<Value, String> + '_ {
    let failing: BTreeSet<u64> = failing.iter().copied().collect();
    move |input| {
        let i = input.as_u64().unwrap();
        if failing.contains(&i) {
            Err(format!("item {i} failed"))
        } else {
            Ok(json!(i * 10))
        }
    }
}

fn completed_output(outcome: &WorkflowOutcome) -> &Value {
    match outcome {
        WorkflowOutcome::Completed { output, .. } => output,
        other => panic!("expected Completed, got {other:?}"),
    }
}

// ── FailureTolerance ─────────────────────────────────────────────────────────

#[test]
fn tolerance_limits_are_exact() {
    assert_eq!(FailureTolerance::None.max_failures(10), 0);
    assert_eq!(FailureTolerance::Count(3).max_failures(10), 3);
    assert_eq!(FailureTolerance::Count(30).max_failures(10), 30);
    // The percentage rounds down: 10% of 15 is 1.5, so 1 failure.
    assert_eq!(FailureTolerance::Percent(10).max_failures(15), 1);
    assert_eq!(FailureTolerance::Percent(0).max_failures(15), 0);
    assert_eq!(FailureTolerance::Percent(100).max_failures(15), 15);
    // A value above 100 means 100.
    assert_eq!(FailureTolerance::Percent(250).max_failures(15), 15);
    assert_eq!(FailureTolerance::default(), FailureTolerance::None);
}

// ── Done when #1: tolerance of N ─────────────────────────────────────────────

/// AC1: a tolerance of 2 completes when exactly 2 items fail.
#[tokio::test]
async fn count_tolerance_completes_at_n_failures() {
    let run = drive(
        tolerant_handler,
        json!({ "n": 6, "count": 2 }),
        false,
        fail_on(&[1, 4]),
    )
    .await;
    let output = completed_output(&run.outcome);
    assert_eq!(output["failed"], json!(2));
    assert_eq!(output["succeeded"], json!(4));
    let items = output["results"]["items"].as_array().unwrap();
    assert_eq!(items.len(), 6);
    assert_eq!(items[0], json!({ "value": 0 }));
    assert_eq!(items[1], json!({ "failed": "activity failed: item (attempt 1): item 1 failed" }));
    assert_eq!(items[5], json!({ "value": 50 }));
}

/// AC1: a tolerance of 2 fails when 3 items fail.
#[tokio::test]
async fn count_tolerance_fails_at_n_plus_one_failures() {
    let run = drive(
        tolerant_handler,
        json!({ "n": 6, "count": 2 }),
        false,
        fail_on(&[0, 3, 5]),
    )
    .await;
    let output = completed_output(&run.outcome);
    assert_eq!(
        output["exceeded"],
        json!({ "tolerated": 2, "total": 6 }),
        "3 failures exceed a tolerance of 2"
    );
}

/// The default tolerance is zero: one failure exceeds it.
#[tokio::test]
async fn default_tolerance_fails_on_the_first_failure() {
    let run = drive(
        tolerant_handler,
        json!({ "n": 3 }),
        false,
        fail_on(&[2]),
    )
    .await;
    let output = completed_output(&run.outcome);
    assert_eq!(output["exceeded"], json!({ "tolerated": 0, "total": 3 }));
}

/// A percentage rounds down: 10% of 15 tolerates 1 failure, not 2.
#[tokio::test]
async fn percent_tolerance_rounds_down() {
    let one = drive(
        tolerant_handler,
        json!({ "n": 15, "percent": 10 }),
        false,
        fail_on(&[7]),
    )
    .await;
    assert_eq!(completed_output(&one.outcome)["failed"], json!(1));

    let two = drive(
        tolerant_handler,
        json!({ "n": 15, "percent": 10 }),
        false,
        fail_on(&[7, 8]),
    )
    .await;
    assert_eq!(
        completed_output(&two.outcome)["exceeded"],
        json!({ "tolerated": 1, "total": 15 })
    );
}

/// A tolerance of 100% completes when every item fails.
#[tokio::test]
async fn full_percent_tolerance_completes_when_every_item_fails() {
    let run = drive(
        tolerant_handler,
        json!({ "n": 4, "percent": 100 }),
        false,
        fail_on(&[0, 1, 2, 3]),
    )
    .await;
    assert_eq!(completed_output(&run.outcome)["failed"], json!(4));
}

/// Windowed: when wave 1 exceeds the tolerance, no later wave is dispatched.
#[tokio::test]
async fn windowed_fan_out_stops_dispatch_when_the_tolerance_is_exceeded() {
    let run = drive(
        tolerant_handler,
        json!({ "n": 10, "w": 2, "count": 1 }),
        false,
        fail_on(&[0, 1]),
    )
    .await;
    assert_eq!(
        completed_output(&run.outcome)["exceeded"],
        json!({ "tolerated": 1, "total": 10 })
    );
    assert_eq!(run.scheduled, 2, "only wave 1 may be dispatched");
}

/// Windowed: failures add up across waves. Wave 3 holds the second failure.
#[tokio::test]
async fn windowed_fan_out_counts_failures_across_waves() {
    let run = drive(
        tolerant_handler,
        json!({ "n": 10, "w": 2, "count": 1 }),
        false,
        fail_on(&[0, 5]),
    )
    .await;
    assert_eq!(
        completed_output(&run.outcome)["exceeded"],
        json!({ "tolerated": 1, "total": 10 })
    );
    assert_eq!(run.scheduled, 6, "waves 1 to 3 run, waves 4 and 5 do not");
}

/// Windowed: a tolerated failure does not stop later waves.
#[tokio::test]
async fn windowed_fan_out_completes_within_the_tolerance() {
    let run = drive(
        tolerant_handler,
        json!({ "n": 7, "w": 3, "count": 2 }),
        false,
        fail_on(&[2, 6]),
    )
    .await;
    assert_eq!(completed_output(&run.outcome)["failed"], json!(2));
    assert_eq!(run.scheduled, 7);
}

/// Replay of the recorded history gives the same outcome (R7).
///
/// The threshold error is caught and the workflow completes. Every recorded
/// command is consumed, so issue #1791 does not block the run (R10).
#[tokio::test]
async fn replay_of_an_exceeded_fan_out_is_deterministic_and_drift_free() {
    let input = json!({ "n": 5, "count": 1 });
    let run = drive(tolerant_handler, input.clone(), false, fail_on(&[0, 1])).await;
    let first = completed_output(&run.outcome).clone();

    let replayed = run_workflow(ExecutionId::new(), run.history, tolerant_handler, input).await;
    assert_eq!(completed_output(&replayed), &first);
}

/// An engine error is not an item failure. Cancellation aborts the fan-out.
#[tokio::test]
async fn cancellation_is_not_counted_as_an_item_failure() {
    let history = vec![
        started(&json!({})),
        WorkflowEvent::WorkflowCancelled {
            reason: "user_requested".into(),
        },
    ];
    let ctx = WorkflowContext::for_replay(ExecutionId::new(), history);
    let activities = vec![("item".to_string(), json!(0), "default".to_string())];
    let options = FanOutOptions::new().tolerate(FailureTolerance::Count(5));
    let result = ctx.execute_activity_fan_out_raw_with(activities, &options).await;
    assert!(
        matches!(result, Err(HarvestError::Cancelled(_))),
        "got {result:?}"
    );
}

/// An empty fan-out completes with no items.
#[tokio::test]
async fn empty_fan_out_with_options_returns_no_items() {
    let run = drive(tolerant_handler, json!({ "n": 0 }), false, fail_on(&[])).await;
    let output = completed_output(&run.outcome);
    assert_eq!(output["results"]["items"], json!([]));
    assert_eq!(output["failed"], json!(0));
}

// ── Result writer ────────────────────────────────────────────────────────────

fn stored(i: u64) -> StoredResult {
    StoredResult::new("mem", format!("key-{i}"), 4096, format!("{i:064x}"))
}

/// Every command of a writer fan-out asks the worker to write the result.
/// Recorded references come back as `Stored` items.
#[tokio::test]
async fn writer_fan_out_flags_commands_and_returns_stored_items() {
    let run = drive(
        tolerant_handler,
        json!({ "n": 4, "writer": true, "count": 1 }),
        true,
        |input| {
            let i = input.as_u64().unwrap();
            if i == 2 {
                Err("item 2 failed".into())
            } else {
                Ok(stored(i).to_value())
            }
        },
    )
    .await;
    assert_eq!(run.writer_flags, vec![true; 4]);
    let output = completed_output(&run.outcome);
    let results: FanOutResults<Value> =
        serde_json::from_value(output["results"].clone()).unwrap();
    assert_eq!(results.failed_count(), 1);
    assert_eq!(results.items()[0], FanOutItem::Stored(stored(0)));
    assert!(matches!(results.items()[2], FanOutItem::Failed(_)));
    assert_eq!(results.items()[3], FanOutItem::Stored(stored(3)));
}

/// A plain fan-out never asks for the writer.
#[tokio::test]
async fn plain_fan_out_does_not_flag_commands() {
    let run = drive(tolerant_handler, json!({ "n": 3 }), true, fail_on(&[])).await;
    assert_eq!(run.writer_flags, vec![false; 3]);
}

/// A worker without a store records the value inline. The item is a `Value`.
#[tokio::test]
async fn writer_fan_out_accepts_an_inline_result() {
    let run = drive(
        tolerant_handler,
        json!({ "n": 2, "writer": true }),
        true,
        |input| {
            let i = input.as_u64().unwrap();
            if i == 0 {
                Ok(stored(0).to_value())
            } else {
                Ok(json!("inline"))
            }
        },
    )
    .await;
    let output = completed_output(&run.outcome);
    assert_eq!(output["results"]["items"][1], json!({ "value": "inline" }));
}

/// A fresh writer fan-out with no store fails before it records the marker.
#[tokio::test]
async fn writer_fan_out_without_a_store_is_a_config_error() {
    let input = json!({ "n": 2, "writer": true });
    let outcome = run_workflow(
        ExecutionId::new(),
        vec![started(&input)],
        tolerant_handler,
        input,
    )
    .await;
    match outcome {
        WorkflowOutcome::Failed { error, .. } => {
            assert!(error.contains("PayloadStore"), "got {error}");
        }
        other => panic!("expected Failed, got {other:?}"),
    }
}

/// Replay does not check the store again (R8). A history recorded with a
/// store replays on a context without one.
#[tokio::test]
async fn writer_fan_out_replays_without_a_store() {
    let input = json!({ "n": 2, "writer": true });
    let run = drive(tolerant_handler, input.clone(), true, |input| {
        Ok(stored(input.as_u64().unwrap()).to_value())
    })
    .await;
    let first = completed_output(&run.outcome).clone();
    let replayed = run_workflow(ExecutionId::new(), run.history, tolerant_handler, input).await;
    assert_eq!(completed_output(&replayed), &first);
}

/// The typed helper decodes values and keeps references.
#[tokio::test]
async fn typed_fan_out_with_options_decodes_values() {
    let info = autumn_harvest::info::ActivityInfo {
        name: "item",
        module: "tests",
        default_retry_policy: None,
        default_start_to_close: None,
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some("default"),
        max_concurrent: None,
        concurrency_key: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: None,
        requires: None,
        handler: |_ctx, input| Box::pin(async move { Ok(input) }),
    };
    let input = json!({ "n": 3, "count": 1 });
    let run = drive(tolerant_handler, input, false, fail_on(&[1])).await;
    let ctx = WorkflowContext::for_replay(ExecutionId::new(), run.history);
    // Consume the recorded fan-out with the typed helper instead.
    let results: FanOutResults<u64> = ctx
        .execute_activity_fan_out_with(
            &info,
            vec![0u64, 1, 2],
            &FanOutOptions::new().tolerate(FailureTolerance::Count(1)),
        )
        .await
        .unwrap();
    assert_eq!(results.items()[0], FanOutItem::Value(0));
    assert!(matches!(results.items()[1], FanOutItem::Failed(_)));
    assert_eq!(results.items()[2], FanOutItem::Value(20));
}
