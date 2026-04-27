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
        }],
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
