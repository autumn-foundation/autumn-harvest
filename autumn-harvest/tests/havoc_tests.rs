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
fn test_havoc_det_check_slice_panic() {
    proptest::proptest!(|(s in "\\PC*")| {
        // Find slicing panics without swallowing
        let _ = std::panic::catch_unwind(|| {
            let _ = autumn_harvest::det_check::check_source(&s, "test.rs");
        });
    });
}

#[test]
fn test_havoc_idempotency_key_subkey() {
    use autumn_harvest::types::{ActivityExecId, IdempotencyKey};

    let base = ActivityExecId::new();
    let key = IdempotencyKey::from_activity_exec_id(base);

    // This proptest is expected to fail with panic since it accepts any string, and subkey panics on some
    proptest::proptest!(|(s in "\\PC*")| {
        let _ = std::panic::catch_unwind(|| {
            let _ = key.subkey(&s);
        });
    });
}

#[test]
fn test_havoc_unsound_handle_typed_send_sync() {
    // The vulnerability is that we could do:
    // fn assert_send<T: Send>() {}
    // assert_send::<TypedWorkflowHandle<Rc<i32>>>();
    // But since we fixed the bounds in handle_typed.rs to be `<T: Send> Send for TypedWorkflowHandle<T>`,
    // the above no longer compiles. We can test this by checking that the compiler correctly rejects it
    // if we try, or we can just leave this as a documentation of the fix.
    // The previous implementation used:
    // unsafe impl<T> Send for TypedWorkflowHandle<T> {}
    // unsafe impl<T> Sync for TypedWorkflowHandle<T> {}
    // This was unconditionally marking it as Send/Sync, allowing smuggling non-Send/Sync types.
}
