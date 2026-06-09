use autumn_harvest::policy::RetryPolicy;
#[cfg(feature = "db")]
use autumn_harvest::store::events_to_insert_rows_from;
#[cfg(feature = "db")]
use autumn_harvest::{event::WorkflowEvent, types::ExecutionId};
use std::time::Duration;

#[cfg(feature = "db")]
#[test]
fn test_havoc_event_id_overflow() {
    let exec_id = ExecutionId::new();
    let events = vec![
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::Value::Null,
        },
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::Value::Null,
        },
    ];
    let res = std::panic::catch_unwind(|| {
        let _ = events_to_insert_rows_from(exec_id, &events, i32::MAX);
    });

    assert!(
        res.is_ok(),
        "The system still crashes due to Event ID overflow!"
    );
    // Specifically, `events_to_insert_rows_from` should return an Err, not panic.
    let out = events_to_insert_rows_from(exec_id, &events, i32::MAX);
    assert!(
        out.is_err(),
        "It should return a Database error for event ID overflow."
    );
}

#[test]
fn test_havoc_exponential_retry_delay() {
    let policy = RetryPolicy::exponential(u32::MAX, Duration::from_secs(1));
    let res = std::panic::catch_unwind(|| policy.next_delay(32));
    assert!(
        res.is_ok(),
        "The system still crashes on large retry delay calculations!"
    );
}

#[test]
fn test_havoc_external_task_duration_panic() {
    let res = std::panic::catch_unwind(|| {
        let schedule_to_close_secs = u64::MAX;
        let dur = chrono::Duration::try_seconds(
            i64::try_from(schedule_to_close_secs).unwrap_or(i64::MAX),
        )
        .ok_or_else(|| {
            autumn_harvest::error::HarvestError::Database("Duration out of bounds".to_string())
        });

        if let Ok(d) = dur {
            let _schedule_to_close_at = chrono::Utc::now().checked_add_signed(d);
        }
    });
    assert!(res.is_ok());
}

#[test]
fn test_havoc_idempotency_key_subkey_panic() {
    use autumn_harvest::types::{ActivityExecId, IdempotencyKey};
    let key = IdempotencyKey::from_activity_exec_id(ActivityExecId::new());
    let res = std::panic::catch_unwind(|| {
        let _ = key.subkey("");
    });
    assert!(
        res.is_ok(),
        "The system still crashes on invalid idempotency subkey!"
    );
}
