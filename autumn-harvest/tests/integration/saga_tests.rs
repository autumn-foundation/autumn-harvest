use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::telemetry::{
    METRIC_SAGA_COMPENSATED, METRIC_SAGA_COMPENSATION_FAILED, MetricsRecorder,
};
use autumn_harvest::{
    ExecutionId, HarvestError, HarvestResult, Saga, WorkflowCommand, WorkflowContext, WorkflowEvent,
};
use chrono::Utc;
use serde_json::Value;

fn test_context() -> WorkflowContext {
    WorkflowContext::for_replay(
        ExecutionId::new(),
        vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
    )
}

fn cancelled_context(reason: &str) -> WorkflowContext {
    WorkflowContext::for_replay(
        ExecutionId::new(),
        vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::WorkflowCancelled {
                reason: reason.to_owned(),
            },
        ],
    )
}

fn push(log: &Arc<Mutex<Vec<String>>>, entry: impl Into<String>) {
    log.lock()
        .expect("saga test log lock should not be poisoned")
        .push(entry.into());
}

fn entries(log: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
    log.lock()
        .expect("saga test log lock should not be poisoned")
        .clone()
}

fn workflow_failed(reason: &str) -> HarvestError {
    HarvestError::WorkflowFailed {
        name: "book_trip".into(),
        reason: reason.into(),
    }
}

#[tokio::test]
async fn saga_success_does_not_run_compensations() {
    let ctx = test_context();
    let mut saga = Saga::new(&ctx);
    let log = Arc::new(Mutex::new(Vec::new()));

    let flight = saga
        .step(
            {
                let log = Arc::clone(&log);
                move || async move {
                    push(&log, "book_flight");
                    Ok::<_, HarvestError>("flight-1".to_string())
                }
            },
            {
                let log = Arc::clone(&log);
                move |flight_id| async move {
                    push(&log, format!("cancel_flight:{flight_id}"));
                    Ok::<_, HarvestError>(())
                }
            },
        )
        .await
        .expect("flight step should succeed");

    let hotel = saga
        .step(
            {
                let log = Arc::clone(&log);
                move || async move {
                    push(&log, "book_hotel");
                    Ok::<_, HarvestError>("hotel-1".to_string())
                }
            },
            {
                let log = Arc::clone(&log);
                move |hotel_id| async move {
                    push(&log, format!("cancel_hotel:{hotel_id}"));
                    Ok::<_, HarvestError>(())
                }
            },
        )
        .await
        .expect("hotel step should succeed");

    assert_eq!(flight, "flight-1");
    assert_eq!(hotel, "hotel-1");
    assert_eq!(entries(&log), vec!["book_flight", "book_hotel"]);
    assert_eq!(saga.pending_compensation_count(), 2);
}

#[tokio::test]
async fn saga_runs_compensations_in_reverse_order_when_later_step_fails() {
    let ctx = test_context();
    let mut saga = Saga::new(&ctx);
    let log = Arc::new(Mutex::new(Vec::new()));

    saga.step(
        {
            let log = Arc::clone(&log);
            move || async move {
                push(&log, "book_flight");
                Ok::<_, HarvestError>("flight-1".to_string())
            }
        },
        {
            let log = Arc::clone(&log);
            move |flight_id| async move {
                push(&log, format!("cancel_flight:{flight_id}"));
                Ok::<_, HarvestError>(())
            }
        },
    )
    .await
    .expect("flight step should succeed");

    let error = saga
        .step(
            {
                let log = Arc::clone(&log);
                move || async move {
                    push(&log, "book_hotel");
                    Err::<String, _>(workflow_failed("sold out"))
                }
            },
            {
                let log = Arc::clone(&log);
                move |hotel_id| async move {
                    push(&log, format!("cancel_hotel:{hotel_id}"));
                    Ok::<_, HarvestError>(())
                }
            },
        )
        .await
        .expect_err("hotel step should fail");

    assert!(error.to_string().contains("sold out"));
    assert_eq!(
        entries(&log),
        vec!["book_flight", "book_hotel", "cancel_flight:flight-1"]
    );
    assert_eq!(saga.pending_compensation_count(), 0);
}

#[tokio::test]
async fn saga_compensate_all_runs_pending_compensations_in_reverse_order() {
    let ctx = test_context();
    let mut saga = Saga::new(&ctx);
    let log = Arc::new(Mutex::new(Vec::new()));

    saga.step(
        {
            let log = Arc::clone(&log);
            move || async move {
                push(&log, "reserve_inventory");
                Ok::<_, HarvestError>("inventory-1".to_string())
            }
        },
        {
            let log = Arc::clone(&log);
            move |inventory_id| async move {
                push(&log, format!("release_inventory:{inventory_id}"));
                Ok::<_, HarvestError>(())
            }
        },
    )
    .await
    .expect("inventory step should succeed");

    saga.step(
        {
            let log = Arc::clone(&log);
            move || async move {
                push(&log, "hold_seat");
                Ok::<_, HarvestError>("seat-1".to_string())
            }
        },
        {
            let log = Arc::clone(&log);
            move |seat_id| async move {
                push(&log, format!("release_seat:{seat_id}"));
                Ok::<_, HarvestError>(())
            }
        },
    )
    .await
    .expect("seat step should succeed");

    saga.compensate_all()
        .await
        .expect("manual compensation should succeed");

    assert_eq!(
        entries(&log),
        vec![
            "reserve_inventory",
            "hold_seat",
            "release_seat:seat-1",
            "release_inventory:inventory-1",
        ]
    );
    assert_eq!(saga.pending_compensation_count(), 0);
}

#[tokio::test]
async fn saga_reports_compensation_failures_with_original_error_and_keeps_unwinding() {
    let ctx = test_context();
    let mut saga = Saga::new(&ctx);
    let log = Arc::new(Mutex::new(Vec::new()));

    saga.step(
        {
            let log = Arc::clone(&log);
            move || async move {
                push(&log, "reserve_inventory");
                Ok::<_, HarvestError>("inventory-1".to_string())
            }
        },
        {
            let log = Arc::clone(&log);
            move |inventory_id| async move {
                push(&log, format!("release_inventory:{inventory_id}"));
                Ok::<_, HarvestError>(())
            }
        },
    )
    .await
    .expect("inventory step should succeed");

    saga.step(
        {
            let log = Arc::clone(&log);
            move || async move {
                push(&log, "charge_payment");
                Ok::<_, HarvestError>("payment-1".to_string())
            }
        },
        {
            let log = Arc::clone(&log);
            move |payment_id| async move {
                push(&log, format!("refund_payment:{payment_id}"));
                Err::<(), _>(workflow_failed("refund gateway down"))
            }
        },
    )
    .await
    .expect("payment step should succeed");

    let error = saga
        .step(
            {
                let log = Arc::clone(&log);
                move || async move {
                    push(&log, "create_shipment");
                    Err::<String, _>(workflow_failed("carrier rejected shipment"))
                }
            },
            {
                let log = Arc::clone(&log);
                move |shipment_id| async move {
                    push(&log, format!("cancel_shipment:{shipment_id}"));
                    Ok::<_, HarvestError>(())
                }
            },
        )
        .await
        .expect_err("shipment step should fail");

    let HarvestError::SagaCompensationFailed {
        original,
        compensation_errors,
    } = error
    else {
        panic!("expected SagaCompensationFailed, got {error:?}");
    };

    assert!(original.contains("carrier rejected shipment"));
    assert_eq!(compensation_errors.len(), 1);
    assert!(compensation_errors[0].contains("refund gateway down"));
    assert_eq!(
        entries(&log),
        vec![
            "reserve_inventory",
            "charge_payment",
            "create_shipment",
            "refund_payment:payment-1",
            "release_inventory:inventory-1",
        ]
    );
    assert_eq!(saga.pending_compensation_count(), 0);
}

#[tokio::test]
async fn saga_compensate_all_reports_failures_when_compensations_fail() {
    let ctx = test_context();
    let mut saga = Saga::new(&ctx);

    saga.step(
        || async move { Ok::<_, HarvestError>("step1") },
        |_| async move { Err::<(), _>(workflow_failed("comp_fail_1")) },
    )
    .await
    .expect("step should succeed");

    saga.step(
        || async move { Ok::<_, HarvestError>("step2") },
        |_| async move { Err::<(), _>(workflow_failed("comp_fail_2")) },
    )
    .await
    .expect("step should succeed");

    let error = saga.compensate_all().await.expect_err("should fail");
    let HarvestError::SagaCompensationFailed {
        original,
        compensation_errors,
    } = error
    else {
        panic!("expected SagaCompensationFailed, got {error:?}");
    };

    assert_eq!(original, "manual compensation requested");
    assert_eq!(compensation_errors.len(), 2);
    assert!(compensation_errors[0].contains("comp_fail_2"));
    assert!(compensation_errors[1].contains("comp_fail_1"));
}

#[tokio::test]
async fn saga_context_returns_provided_context() {
    let ctx = test_context();
    let saga = Saga::new(&ctx);
    assert_eq!(saga.context().version("test", 1, 1), 1);
}

// ── RED PHASE: cancellation × saga semantics (issue #238) ─────────────────

/// Cancellation does NOT auto-trigger saga compensation.
///
/// When `cancel_workflow_execution` writes a `WorkflowCancelled` event,
/// the executor replays the workflow function with a context where
/// `ctx.is_cancelled()` returns `true`.  The `Saga` struct never observes
/// this; its compensation stack stays intact and untouched until the
/// workflow author explicitly calls `compensate_all()`.
///
/// This mirrors Temporal's documented model: the workflow function is
/// responsible for detecting cancellation and choosing whether (and how)
/// to compensate.
#[tokio::test]
async fn saga_cancellation_does_not_auto_compensate() {
    let ctx = cancelled_context("operator requested shutdown");
    let mut saga = Saga::new(&ctx);
    let log = Arc::new(Mutex::new(Vec::new()));

    saga.step(
        {
            let log = Arc::clone(&log);
            move || async move {
                push(&log, "book_flight");
                Ok::<_, HarvestError>("flight-1".to_string())
            }
        },
        {
            let log = Arc::clone(&log);
            move |flight_id| async move {
                push(&log, format!("cancel_flight:{flight_id}"));
                Ok::<_, HarvestError>(())
            }
        },
    )
    .await
    .expect("flight step should succeed");

    saga.step(
        {
            let log = Arc::clone(&log);
            move || async move {
                push(&log, "book_hotel");
                Ok::<_, HarvestError>("hotel-1".to_string())
            }
        },
        {
            let log = Arc::clone(&log);
            move |hotel_id| async move {
                push(&log, format!("cancel_hotel:{hotel_id}"));
                Ok::<_, HarvestError>(())
            }
        },
    )
    .await
    .expect("hotel step should succeed");

    assert!(
        ctx.is_cancelled(),
        "context should reflect the WorkflowCancelled event"
    );
    assert_eq!(
        entries(&log),
        vec!["book_flight", "book_hotel"],
        "cancellation must not auto-trigger saga compensation"
    );
    assert_eq!(
        saga.pending_compensation_count(),
        2,
        "compensations remain pending; the author must call compensate_all() explicitly"
    );
}

/// Recommended pattern: observe `ctx.is_cancelled()` and call `compensate_all()`.
///
/// This test is a runnable example of the idiomatic cancel-and-compensate
/// pattern.  When the workflow detects cancellation it calls
/// `saga.compensate_all()` explicitly; compensations fire in LIFO order and
/// the execution ends cleanly.
#[tokio::test]
async fn saga_compensate_all_on_cancel_pattern() {
    // Issue #801 post-review hardening (mutant k): the cancel-and-compensate
    // unwind must be COUNTED — instrumentation never consults is_cancelled().
    let recorder = Arc::new(SagaRecordingMetrics::default());
    let ctx = cancelled_context("operator shutdown")
        .with_metrics(Arc::clone(&recorder) as Arc<dyn MetricsRecorder>);
    let mut saga = Saga::new(&ctx);
    let log = Arc::new(Mutex::new(Vec::new()));

    saga.step(
        {
            let log = Arc::clone(&log);
            move || async move {
                push(&log, "book_flight");
                Ok::<_, HarvestError>("flight-1".to_string())
            }
        },
        {
            let log = Arc::clone(&log);
            move |flight_id| async move {
                push(&log, format!("cancel_flight:{flight_id}"));
                Ok::<_, HarvestError>(())
            }
        },
    )
    .await
    .expect("flight step should succeed");

    saga.step(
        {
            let log = Arc::clone(&log);
            move || async move {
                push(&log, "book_hotel");
                Ok::<_, HarvestError>("hotel-1".to_string())
            }
        },
        {
            let log = Arc::clone(&log);
            move |hotel_id| async move {
                push(&log, format!("cancel_hotel:{hotel_id}"));
                Ok::<_, HarvestError>(())
            }
        },
    )
    .await
    .expect("hotel step should succeed");

    // Recommended pattern ─────────────────────────────────────────────────
    if ctx.is_cancelled() {
        saga.compensate_all()
            .await
            .expect("compensation should succeed");
    }
    // ─────────────────────────────────────────────────────────────────────

    assert_eq!(saga.pending_compensation_count(), 0);
    assert_eq!(
        entries(&log),
        vec![
            "book_flight",
            "book_hotel",
            "cancel_hotel:hotel-1",
            "cancel_flight:flight-1",
        ],
        "compensate_all fires in LIFO order when the author invokes it explicitly"
    );

    // The cancel-and-compensate unwind is a real compensation sequence and
    // must be counted exactly once, with its durable dedup marker recorded —
    // a regression gating emission on !is_cancelled() would silently uncount
    // exactly the population sagas serve (issue #801, AC1).
    assert_eq!(
        recorder.count(METRIC_SAGA_COMPENSATED),
        1,
        "a cancel-and-compensate unwind must fire harvest.saga.compensated"
    );
    assert_eq!(recorder.count(METRIC_SAGA_COMPENSATION_FAILED), 0);
    assert_eq!(
        marker_command_names(&ctx.drain_commands()),
        vec!["saga_compensated:1".to_owned()]
    );
}

/// Simulates one complete pick-up of the workflow task for the idempotency test.
/// On a crash-then-replay, the executor calls the workflow function a second
/// time with the same history, re-registering all Saga compensation closures.
async fn run_compensated_saga_once(comp_log: Arc<Mutex<Vec<String>>>) {
    let ctx = WorkflowContext::for_replay(
        ExecutionId::new(),
        vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
    );
    let mut saga = Saga::new(&ctx);

    // Step 1: charge payment.
    saga.step(
        || async { Ok::<_, HarvestError>("charge-001".to_string()) },
        {
            let comp_log = Arc::clone(&comp_log);
            move |charge_id| async move {
                // Idempotent: refund by specific charge ID — safe to call twice.
                comp_log
                    .lock()
                    .expect("lock")
                    .push(format!("refund:{charge_id}"));
                Ok::<_, HarvestError>(())
            }
        },
    )
    .await
    .expect("charge step ok");

    // Step 2: reserve inventory.
    saga.step(
        || async { Ok::<_, HarvestError>("inventory-002".to_string()) },
        {
            let comp_log = Arc::clone(&comp_log);
            move |inv_id| async move {
                comp_log
                    .lock()
                    .expect("lock")
                    .push(format!("release_inventory:{inv_id}"));
                Ok::<_, HarvestError>(())
            }
        },
    )
    .await
    .expect("inventory step ok");

    // Step 3: reserve seat.
    saga.step(
        || async { Ok::<_, HarvestError>("seat-003".to_string()) },
        {
            let comp_log = Arc::clone(&comp_log);
            move |seat_id| async move {
                comp_log
                    .lock()
                    .expect("lock")
                    .push(format!("release_seat:{seat_id}"));
                Ok::<_, HarvestError>(())
            }
        },
    )
    .await
    .expect("seat step ok");

    saga.compensate_all()
        .await
        .expect("compensations should succeed");
}

/// Demonstrates the idempotency contract for compensation closures.
///
/// When a worker crashes mid-`compensate_all`, the next worker replays the
/// workflow function from the top.  Every `Saga::step()` call executes again
/// (re-registering its compensation closure), and `compensate_all()` runs
/// the entire stack again — including compensations that already ran before
/// the crash.
///
/// **Idempotent pattern (safe):** release a resource by its specific ID.
/// Calling `release_reservation("rsv-abc")` twice is a no-op the second time.
///
/// **Anti-pattern (dangerous):** release the *most recently created*
/// reservation.  A second call would release a *different* reservation that
/// belongs to another order.
///
/// This test simulates two complete executions to show that idempotent
/// compensations are safe while non-idempotent ones produce double-effects.
#[tokio::test]
async fn saga_compensation_idempotency_under_replay() {
    let comp_log = Arc::new(Mutex::new(Vec::<String>::new()));

    // First execution: compensations #3, #2, #1 each run once (LIFO).
    run_compensated_saga_once(Arc::clone(&comp_log)).await;
    assert_eq!(
        *comp_log.lock().expect("lock"),
        vec![
            "release_seat:seat-003",
            "release_inventory:inventory-002",
            "refund:charge-001",
        ],
        "first execution: all three compensations run in LIFO order"
    );

    // Second execution: simulates replay after a crash mid-compensate_all
    // (e.g., worker died after compensation #2 ran but before #1).
    // On replay the entire stack re-runs — idempotent by-ID compensations
    // are safe; a release-most-recent anti-pattern would release the wrong
    // resource on this second invocation.
    run_compensated_saga_once(Arc::clone(&comp_log)).await;
    assert_eq!(
        *comp_log.lock().expect("lock"),
        vec![
            "release_seat:seat-003",
            "release_inventory:inventory-002",
            "refund:charge-001",
            // Same three compensations re-run on replay:
            "release_seat:seat-003",
            "release_inventory:inventory-002",
            "refund:charge-001",
        ],
        "on replay, all compensations re-run; idempotent (by-ID) compensations tolerate this safely"
    );

    // ── Non-idempotent anti-pattern: concrete failure mode ────────────────
    // A compensation that reads from ambient state instead of the forward
    // step's T result behaves differently on replay when that state has
    // changed — e.g., a different order's reservation now occupies the
    // "most recent" slot.
    {
        // Shared mutable slot that simulates external booking state.
        let current_res: Arc<Mutex<String>> = Arc::new(Mutex::new("rsv-abc".to_string()));
        let bad_log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

        // First execution: the correct reservation is "rsv-abc".
        {
            let ctx = test_context();
            let mut saga = Saga::new(&ctx);
            let slot = Arc::clone(&current_res);
            let log = Arc::clone(&bad_log);
            saga.step(
                || async { Ok::<_, HarvestError>("rsv-abc".to_string()) },
                // Anti-pattern: ignores the forward result, reads ambient state.
                move |_ignored: String| {
                    let released = slot.lock().expect("lock").clone();
                    async move {
                        log.lock()
                            .expect("lock")
                            .push(format!("released:{released}"));
                        Ok::<_, HarvestError>(())
                    }
                },
            )
            .await
            .expect("step ok");
            saga.compensate_all().await.expect("compensate ok");
        }

        // Between executions a different order creates its own reservation,
        // replacing "rsv-abc" as the "most recent" in the shared slot.
        *current_res.lock().expect("lock") = "rsv-xyz".to_string();

        // Replay: same workflow code re-runs after the simulated crash.
        {
            let ctx = test_context();
            let mut saga = Saga::new(&ctx);
            let slot = Arc::clone(&current_res);
            let log = Arc::clone(&bad_log);
            saga.step(
                || async { Ok::<_, HarvestError>("rsv-abc".to_string()) },
                move |_ignored: String| {
                    let released = slot.lock().expect("lock").clone();
                    async move {
                        log.lock()
                            .expect("lock")
                            .push(format!("released:{released}"));
                        Ok::<_, HarvestError>(())
                    }
                },
            )
            .await
            .expect("step ok");
            saga.compensate_all().await.expect("compensate ok");
        }

        assert_eq!(
            *bad_log.lock().expect("lock"),
            vec![
                "released:rsv-abc", // first run: correct
                "released:rsv-xyz", // replay: WRONG — releases a different order's reservation
            ],
            "non-idempotent compensation reads ambient state and releases the wrong resource on replay"
        );
    }
}

// ── Saga compensation observability (issue #801) ───────────────────────────

/// Buffers the two saga counters (name + workflow/queue labels) so tests can
/// assert exact emission counts across simulated crash-replay cycles.
#[derive(Default)]
struct SagaRecordingMetrics {
    samples: Mutex<Vec<(&'static str, String, String)>>,
}

impl SagaRecordingMetrics {
    fn count(&self, metric: &str) -> usize {
        self.samples
            .lock()
            .expect("saga metrics lock should not be poisoned")
            .iter()
            .filter(|(name, _, _)| *name == metric)
            .count()
    }

    fn labels(&self, metric: &str) -> Vec<(String, String)> {
        self.samples
            .lock()
            .expect("saga metrics lock should not be poisoned")
            .iter()
            .filter(|(name, _, _)| *name == metric)
            .map(|(_, wf, q)| (wf.clone(), q.clone()))
            .collect()
    }
}

impl MetricsRecorder for SagaRecordingMetrics {
    fn record_saga_compensated(&self, workflow_name: &str, queue: &str) {
        self.samples.lock().expect("lock").push((
            METRIC_SAGA_COMPENSATED,
            workflow_name.to_owned(),
            queue.to_owned(),
        ));
    }

    fn record_saga_compensation_failed(&self, workflow_name: &str, queue: &str) {
        self.samples.lock().expect("lock").push((
            METRIC_SAGA_COMPENSATION_FAILED,
            workflow_name.to_owned(),
            queue.to_owned(),
        ));
    }
}

fn started_history() -> Vec<WorkflowEvent> {
    vec![WorkflowEvent::WorkflowStarted {
        input: Value::Null,
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }]
}

fn saga_marker(name: &str, details: u64) -> WorkflowEvent {
    WorkflowEvent::MarkerRecorded {
        name: name.to_owned(),
        details: Value::from(details),
    }
}

fn observed_context(
    history: Vec<WorkflowEvent>,
    recorder: Arc<SagaRecordingMetrics>,
) -> WorkflowContext {
    WorkflowContext::for_replay(ExecutionId::new(), history)
        .with_workflow_name("book_trip")
        .with_queue_name("default")
        .with_metrics(recorder)
}

fn marker_command_names(commands: &[WorkflowCommand]) -> Vec<String> {
    commands
        .iter()
        .filter_map(|cmd| match cmd {
            WorkflowCommand::RecordMarker { name, .. } => Some(name.clone()),
            _ => None,
        })
        .collect()
}

/// One decision cycle of the three-step in-memory compensated saga used by
/// the crash-replay test — mirrors `run_compensated_saga_once` but against a
/// caller-supplied context so metrics/commands can be observed.
async fn run_three_step_compensated_saga(
    ctx: &WorkflowContext,
    comp_log: &Arc<Mutex<Vec<String>>>,
) {
    let mut saga = Saga::new(ctx);

    saga.step(
        || async { Ok::<_, HarvestError>("charge-001".to_string()) },
        {
            let comp_log = Arc::clone(comp_log);
            move |charge_id| async move {
                comp_log
                    .lock()
                    .expect("lock")
                    .push(format!("refund:{charge_id}"));
                Ok::<_, HarvestError>(())
            }
        },
    )
    .await
    .expect("charge step ok");

    saga.step(
        || async { Ok::<_, HarvestError>("inventory-002".to_string()) },
        {
            let comp_log = Arc::clone(comp_log);
            move |inv_id| async move {
                comp_log
                    .lock()
                    .expect("lock")
                    .push(format!("release_inventory:{inv_id}"));
                Ok::<_, HarvestError>(())
            }
        },
    )
    .await
    .expect("inventory step ok");

    saga.step(
        || async { Ok::<_, HarvestError>("seat-003".to_string()) },
        {
            let comp_log = Arc::clone(comp_log);
            move |seat_id| async move {
                comp_log
                    .lock()
                    .expect("lock")
                    .push(format!("release_seat:{seat_id}"));
                Ok::<_, HarvestError>(())
            }
        },
    )
    .await
    .expect("seat step ok");

    saga.compensate_all()
        .await
        .expect("compensations should succeed");
}

/// AC3 primary — exactly-once under crash + replay.
///
/// Cycle 1 is the live unwind (history = `[WorkflowStarted]`): the counter
/// fires once and the durable `saga_compensated:{seq}` dedup marker is pushed
/// as a `RecordMarker` command in the same batch. Cycle 2 simulates the
/// crash-resume: the worker persisted cycle 1's batch, so the marker is now in
/// history; compensations re-run wholesale (the documented idempotency
/// contract) but the counter must NOT double-count.
#[tokio::test]
async fn saga_compensated_metric_emitted_once_across_crash_replay() {
    let recorder = Arc::new(SagaRecordingMetrics::default());
    let comp_log = Arc::new(Mutex::new(Vec::new()));

    // Cycle 1 — live unwind.
    let ctx = observed_context(started_history(), Arc::clone(&recorder));
    run_three_step_compensated_saga(&ctx, &comp_log).await;
    assert_eq!(entries(&comp_log).len(), 3, "all three compensations ran");
    assert_eq!(recorder.count(METRIC_SAGA_COMPENSATED), 1);
    let markers = marker_command_names(&ctx.drain_commands());
    assert_eq!(
        markers,
        vec!["saga_compensated:1".to_owned()],
        "the dedup marker must be recorded durably with the unwind's own batch"
    );

    // Cycle 2 — crash-resume replay (marker persisted in history).
    let mut history = started_history();
    history.push(saga_marker("saga_compensated:1", 3));
    let ctx2 = observed_context(history, Arc::clone(&recorder));
    run_three_step_compensated_saga(&ctx2, &comp_log).await;
    assert_eq!(
        entries(&comp_log).len(),
        6,
        "compensations re-ran on replay (documented idempotency contract)"
    );
    assert_eq!(
        recorder.count(METRIC_SAGA_COMPENSATED),
        1,
        "counter must reflect real compensation sequences, not the replay count"
    );
    assert!(
        marker_command_names(&ctx2.drain_commands()).is_empty(),
        "a replayed unwind records no fresh marker"
    );
}

/// AC2 — the dangling-state case gets a distinct counter, emitted exactly
/// once, independent of `harvest.workflow.terminal` (which this recorder
/// never observes: emission happens in-Saga, not at the worker terminal
/// boundary).
#[tokio::test]
async fn saga_compensation_failed_metric_distinct_and_once() {
    let recorder = Arc::new(SagaRecordingMetrics::default());

    // Cycle 1 — live unwind whose single compensation fails.
    let ctx = observed_context(started_history(), Arc::clone(&recorder));
    {
        let mut saga = Saga::new(&ctx);
        saga.step(
            || async { Ok::<_, HarvestError>("payment-1".to_string()) },
            |_| async { Err::<(), _>(workflow_failed("refund gateway down")) },
        )
        .await
        .expect("payment step ok");

        let error = saga.compensate_all().await.expect_err("unwind must fail");
        let HarvestError::SagaCompensationFailed {
            compensation_errors,
            ..
        } = error
        else {
            panic!("expected SagaCompensationFailed");
        };
        assert_eq!(compensation_errors.len(), 1);
    }
    assert_eq!(recorder.count(METRIC_SAGA_COMPENSATED), 1);
    assert_eq!(recorder.count(METRIC_SAGA_COMPENSATION_FAILED), 1);
    let markers = marker_command_names(&ctx.drain_commands());
    assert_eq!(
        markers,
        vec![
            "saga_compensated:1".to_owned(),
            "saga_compensation_failed:1".to_owned(),
        ],
        "start marker at unwind start, failure marker at failed-unwind end"
    );

    // Cycle 2 — replay with both markers persisted: no new samples.
    let mut history = started_history();
    history.push(saga_marker("saga_compensated:1", 1));
    history.push(saga_marker("saga_compensation_failed:1", 1));
    let ctx2 = observed_context(history, Arc::clone(&recorder));
    {
        let mut saga = Saga::new(&ctx2);
        saga.step(
            || async { Ok::<_, HarvestError>("payment-1".to_string()) },
            |_| async { Err::<(), _>(workflow_failed("refund gateway down")) },
        )
        .await
        .expect("payment step ok");
        let _ = saga.compensate_all().await.expect_err("unwind still fails");
    }
    assert_eq!(recorder.count(METRIC_SAGA_COMPENSATED), 1);
    assert_eq!(recorder.count(METRIC_SAGA_COMPENSATION_FAILED), 1);
    assert!(marker_command_names(&ctx2.drain_commands()).is_empty());

    // Labels carry workflow + queue (AC1), never an execution id — enforced
    // by construction: the recorder only ever receives the two strings.
    assert_eq!(
        recorder.labels(METRIC_SAGA_COMPENSATION_FAILED),
        vec![("book_trip".to_owned(), "default".to_owned())]
    );
}

/// The failure counter fires even when the workflow author catches
/// `SagaCompensationFailed` and completes normally — a case worker-boundary
/// emission would structurally miss.
#[tokio::test]
async fn saga_compensation_failed_emitted_even_when_author_catches_error() {
    let recorder = Arc::new(SagaRecordingMetrics::default());
    let ctx = observed_context(started_history(), Arc::clone(&recorder));

    let mut saga = Saga::new(&ctx);
    saga.step(
        || async { Ok::<_, HarvestError>("rsv-1".to_string()) },
        |_| async { Err::<(), _>(workflow_failed("release rejected")) },
    )
    .await
    .expect("step ok");

    // Author catches the dangling-state error and continues; the workflow
    // would go on to COMPLETE.
    match saga.compensate_all().await {
        Err(HarvestError::SagaCompensationFailed { .. }) => { /* logged, workflow continues */ }
        other => panic!("expected SagaCompensationFailed, got {other:?}"),
    }

    assert_eq!(
        recorder.count(METRIC_SAGA_COMPENSATION_FAILED),
        1,
        "in-Saga emission must observe an author-caught compensation failure"
    );
}

/// AC6 — a saga that never compensates emits nothing new: zero samples, zero
/// marker commands. The success path is byte-identical to pre-#801 behavior.
#[tokio::test]
async fn saga_success_path_emits_no_saga_metrics_and_no_marker_commands() {
    let recorder = Arc::new(SagaRecordingMetrics::default());
    let ctx = observed_context(started_history(), Arc::clone(&recorder));

    let mut saga = Saga::new(&ctx);
    for _ in 0..3 {
        saga.step(
            || async { Ok::<_, HarvestError>("ok") },
            |_| async { Ok::<_, HarvestError>(()) },
        )
        .await
        .expect("step ok");
    }
    assert_eq!(saga.pending_compensation_count(), 3);

    assert_eq!(recorder.count(METRIC_SAGA_COMPENSATED), 0);
    assert_eq!(recorder.count(METRIC_SAGA_COMPENSATION_FAILED), 0);
    assert!(ctx.drain_commands().is_empty());
}

/// An empty `compensate_all` (nothing pending) is not a compensation
/// sequence: no seq allocated, no marker, no metric.
#[tokio::test]
async fn saga_empty_compensate_all_emits_nothing() {
    let recorder = Arc::new(SagaRecordingMetrics::default());
    let ctx = observed_context(started_history(), Arc::clone(&recorder));

    let mut saga = Saga::new(&ctx);
    saga.compensate_all()
        .await
        .expect("empty compensation is Ok");

    assert_eq!(recorder.count(METRIC_SAGA_COMPENSATED), 0);
    assert_eq!(recorder.count(METRIC_SAGA_COMPENSATION_FAILED), 0);
    assert!(ctx.drain_commands().is_empty());
}

/// Activity-backed saga flow used by the backward-compat test: one forward
/// step whose compensation dispatches an activity, then a manual unwind.
async fn activity_backed_saga_flow(ctx: &WorkflowContext) -> HarvestResult<()> {
    let mut saga = Saga::new(ctx);
    saga.step(
        || async {
            ctx.execute_activity_raw("reserve", Value::Null, "default")
                .await
        },
        move |rsv: Value| async move {
            ctx.execute_activity_raw("release", rsv, "default")
                .await
                .map(|_| ())
        },
    )
    .await?;
    saga.compensate_all().await
}

/// AC7 backward-compat money test — a pre-#801 history (no saga markers)
/// replays untouched and uncounted:
///
/// (a) a fully recorded legacy unwind replays to completion with no
///     divergence, no metric, and no fresh marker command;
/// (b) a legacy history captured *mid-unwind* (compensation activity
///     scheduled but not yet completed) resumes by suspending on that
///     activity — never a `NonDeterministic` failure, never an emission.
#[tokio::test]
async fn pre_801_history_mid_unwind_replays_clean_and_uncounted() {
    let reserve_id = autumn_harvest::types::ActivityExecId::new();
    let release_id = autumn_harvest::types::ActivityExecId::new();

    let full_legacy_history = || {
        let mut events = started_history();
        events.extend([
            WorkflowEvent::ActivityScheduled {
                activity_id: reserve_id,
                name: "reserve".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: reserve_id,
                output: serde_json::json!("rsv-1"),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: release_id,
                name: "release".into(),
                input: serde_json::json!("rsv-1"),
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: release_id,
                output: Value::Null,
            },
        ]);
        events
    };

    // (a) Fully recorded legacy unwind — replays clean, zero emissions.
    let recorder = Arc::new(SagaRecordingMetrics::default());
    let ctx = observed_context(full_legacy_history(), Arc::clone(&recorder));
    activity_backed_saga_flow(&ctx)
        .await
        .expect("legacy marker-less history must replay without divergence");
    assert_eq!(recorder.count(METRIC_SAGA_COMPENSATED), 0);
    assert_eq!(recorder.count(METRIC_SAGA_COMPENSATION_FAILED), 0);
    assert!(
        marker_command_names(&ctx.drain_commands()).is_empty(),
        "no fresh saga marker may be recorded into a legacy history position"
    );

    // (b) Mid-unwind legacy history — the compensation activity is scheduled
    // but has no terminal yet. The resumed unwind must suspend on it (the
    // pre-#801 resume shape), not diverge and not emit.
    let mut mid_unwind = full_legacy_history();
    mid_unwind.pop(); // drop the release ActivityCompleted
    let recorder2 = Arc::new(SagaRecordingMetrics::default());
    let ctx2 = observed_context(mid_unwind, Arc::clone(&recorder2));
    let outcome =
        tokio::time::timeout(Duration::from_millis(200), activity_backed_saga_flow(&ctx2)).await;
    assert!(
        outcome.is_err(),
        "mid-unwind resume must suspend awaiting the in-flight compensation \
         activity, got {outcome:?}"
    );
    assert_eq!(recorder2.count(METRIC_SAGA_COMPENSATED), 0);
    assert_eq!(recorder2.count(METRIC_SAGA_COMPENSATION_FAILED), 0);
    assert!(
        marker_command_names(&ctx2.drain_commands()).is_empty(),
        "the suspended cycle's command batch must carry no saga marker"
    );
}

/// P2-1 (post-review hardening): a trailing un-awaited signal at the
/// failed-unwind end must NOT suppress `harvest.saga.compensation_failed`
/// for a counted unwind.
///
/// Shape (the review's empirical reproduction): the unwind was counted in an
/// earlier cycle (its `saga_compensated:1` marker is in history), its
/// activity-backed compensation terminally failed, and a duplicate/extra
/// signal (e.g. a retried cancel webhook) was ingested after the
/// compensation's terminal — landing exactly at the failed-observe cursor
/// position. The failure marker must be recorded past the drained signal and
/// the page-severity counter must fire, exactly once.
#[tokio::test]
async fn trailing_unawaited_signal_does_not_suppress_the_failure_counter() {
    // One saga step: in-memory forward step, activity-backed compensation
    // that replays to the recorded terminal failure.
    async fn run_failing_unwind(ctx: &WorkflowContext) -> HarvestError {
        let mut saga = Saga::new(ctx);
        saga.step(
            || async { Ok::<_, HarvestError>(serde_json::json!("rsv-1")) },
            move |rsv: Value| async move {
                ctx.execute_activity_raw("release", rsv, "default")
                    .await
                    .map(|_| ())
            },
        )
        .await
        .expect("forward step ok");
        saga.compensate_all()
            .await
            .expect_err("compensation replays the recorded activity failure")
    }

    let release_id = autumn_harvest::types::ActivityExecId::new();
    let failed_unwind_history = || {
        let mut events = started_history();
        events.extend([
            saga_marker("saga_compensated:1", 1),
            WorkflowEvent::ActivityScheduled {
                activity_id: release_id,
                name: "release".into(),
                input: serde_json::json!("rsv-1"),
                queue: "default".into(),
            },
            WorkflowEvent::ActivityFailed {
                activity_id: release_id,
                error: "release rejected".into(),
                attempt: 1,
                error_type: "Error".into(),
                non_retryable: false,
                details: None,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "dup_cancel".into(),
                payload: serde_json::json!({"retry": true}),
            },
        ]);
        events
    };

    // Cycle F — the cycle that observes the compensation failure.
    let recorder = Arc::new(SagaRecordingMetrics::default());
    let ctx = observed_context(failed_unwind_history(), Arc::clone(&recorder));
    run_failing_unwind(&ctx).await;
    assert_eq!(
        recorder.count(METRIC_SAGA_COMPENSATED),
        0,
        "the compensated counter was already emitted in an earlier cycle (marker dedupe)"
    );
    assert_eq!(
        recorder.count(METRIC_SAGA_COMPENSATION_FAILED),
        1,
        "a genuinely failed, counted unwind must fire the page-severity counter \
         even with a trailing un-awaited signal at the failed-unwind end"
    );
    assert_eq!(
        marker_command_names(&ctx.drain_commands()),
        vec!["saga_compensation_failed:1".to_owned()],
        "the failure marker must be recorded (past the drained trailing signal)"
    );

    // Cycle F+1 — the failure marker persisted after the signal: exactly-once.
    let mut history = failed_unwind_history();
    history.push(saga_marker("saga_compensation_failed:1", 1));
    let ctx2 = observed_context(history, Arc::clone(&recorder));
    run_failing_unwind(&ctx2).await;
    assert_eq!(
        recorder.count(METRIC_SAGA_COMPENSATION_FAILED),
        1,
        "the failure counter must not re-emit once its marker is recorded"
    );
    assert!(
        marker_command_names(&ctx2.drain_commands()).is_empty(),
        "no fresh marker on the replay cycle"
    );
}

/// P2-2 (post-review hardening): the `failed ≤ compensated` invariant.
///
/// Signal-with-start shape: the staged signal sits at the cursor when the
/// unwind starts, so the whole unwind is conservatively uncounted (the same
/// caveat `ctx.patched()` documents). The failure counter must FOLLOW that
/// disposition — an uncounted unwind's failure stays uncounted, never
/// producing `compensated=0, failed=1` (which breaks ratio dashboards and
/// records an asymmetric marker set).
#[tokio::test]
async fn signal_with_start_shape_keeps_the_failed_counter_coupled_to_compensated() {
    async fn run_failing_in_memory_unwind(ctx: &WorkflowContext) -> HarvestError {
        let mut saga = Saga::new(ctx);
        saga.step(
            || async { Ok::<_, HarvestError>("rsv-1".to_string()) },
            |_| async { Err::<(), _>(workflow_failed("release rejected")) },
        )
        .await
        .expect("forward step ok");
        saga.compensate_all()
            .await
            .expect_err("in-memory compensation fails")
    }

    let signal_with_start_history = || {
        let mut events = started_history();
        events.push(WorkflowEvent::SignalReceived {
            signal_name: "kick".into(),
            payload: serde_json::json!({"n": 1}),
        });
        events
    };

    // Every cycle resolves identically: the unwind is uncounted as a unit.
    let recorder = Arc::new(SagaRecordingMetrics::default());
    for cycle in 0..2 {
        let ctx = observed_context(signal_with_start_history(), Arc::clone(&recorder));
        run_failing_in_memory_unwind(&ctx).await;
        assert_eq!(
            recorder.count(METRIC_SAGA_COMPENSATED),
            0,
            "cycle {cycle}: an unwind entered at a drained-signal frontier is \
             conservatively uncounted"
        );
        assert_eq!(
            recorder.count(METRIC_SAGA_COMPENSATION_FAILED),
            0,
            "cycle {cycle}: the failure counter must follow the unwind's \
             disposition — never failed=1 with compensated=0"
        );
        assert!(
            marker_command_names(&ctx.drain_commands()).is_empty(),
            "cycle {cycle}: an uncounted unwind records no marker at all \
             (no asymmetric marker set)"
        );
    }
}

/// Mutant-killer for the seq counter (test-review P2-2): two compensation
/// sequences in one workflow must get distinct seq-numbered markers and count
/// independently — a frozen `next_saga_seq` would collide both unwinds on
/// `saga_compensated:1`.
#[tokio::test]
async fn two_saga_unwinds_in_one_workflow_count_independently() {
    async fn run_two_compensated_sagas(ctx: &WorkflowContext) {
        for _ in 0..2 {
            let mut saga = Saga::new(ctx);
            saga.step(
                || async { Ok::<_, HarvestError>("ok") },
                |_| async { Ok::<_, HarvestError>(()) },
            )
            .await
            .expect("step ok");
            saga.compensate_all().await.expect("compensation succeeds");
        }
    }

    // Cycle 1 — both unwinds live: two distinct markers, two samples.
    let recorder = Arc::new(SagaRecordingMetrics::default());
    let ctx = observed_context(started_history(), Arc::clone(&recorder));
    run_two_compensated_sagas(&ctx).await;
    assert_eq!(
        recorder.count(METRIC_SAGA_COMPENSATED),
        2,
        "each compensation sequence counts once"
    );
    assert_eq!(
        marker_command_names(&ctx.drain_commands()),
        vec![
            "saga_compensated:1".to_owned(),
            "saga_compensated:2".to_owned(),
        ],
        "the second unwind must allocate a fresh seq, never reuse seq 1"
    );

    // Cycle 2 — replay with both markers persisted: zero new samples.
    let mut history = started_history();
    history.push(saga_marker("saga_compensated:1", 1));
    history.push(saga_marker("saga_compensated:2", 1));
    let ctx2 = observed_context(history, Arc::clone(&recorder));
    run_two_compensated_sagas(&ctx2).await;
    assert_eq!(
        recorder.count(METRIC_SAGA_COMPENSATED),
        2,
        "a replay of two recorded unwinds emits nothing new"
    );
    assert!(marker_command_names(&ctx2.drain_commands()).is_empty());
}
