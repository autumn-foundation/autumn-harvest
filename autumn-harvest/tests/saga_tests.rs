use std::sync::{Arc, Mutex};

use autumn_harvest::{ExecutionId, HarvestError, Saga, WorkflowContext, WorkflowEvent};
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
    let ctx = cancelled_context("operator shutdown");
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
