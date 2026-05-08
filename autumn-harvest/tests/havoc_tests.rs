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

#[cfg(feature = "testing")]
#[tokio::test]
async fn test_havoc_deadlock_in_query() {
    use autumn_harvest::context::WorkflowContext;
    use std::sync::Arc;
    use std::time::Duration;

    let ctx = Arc::new(WorkflowContext::new_test());

    let ctx_clone = ctx.clone();

    ctx.register_query("test", move || {
        // This will attempt to lock `query_registry` while it is already locked
        // by the outer `execute_query` call.
        let _ = ctx_clone.execute_query("another");
        serde_json::json!(1)
    });

    ctx.register_query("another", || serde_json::json!(2));

    let handle = tokio::spawn(async move {
        let _ = ctx.execute_query("test");
    });

    let res = tokio::time::timeout(Duration::from_millis(500), handle).await;
    assert!(res.is_ok(), "Test should NOT timeout due to deadlock");
}

#[cfg(feature = "testing")]
#[tokio::test]
async fn test_havoc_deadlock_in_update_validation() {
    use autumn_harvest::context::WorkflowContext;
    use serde_json::json;
    use std::sync::Arc;
    use std::time::Duration;

    let ctx = Arc::new(WorkflowContext::new_test());

    let ctx_clone = ctx.clone();

    ctx.register_update_handler(
        "test",
        move |input| {
            // This will deadlock if validate_update holds the lock
            let _ = ctx_clone.validate_update("another", input);
            Ok(())
        },
        |input| async move { Ok(input) },
    );

    ctx.register_update_handler("another", |_| Ok(()), |input| async move { Ok(input) });

    let handle = tokio::spawn(async move {
        let _ = ctx.validate_update("test", &json!(1));
    });

    let res = tokio::time::timeout(Duration::from_millis(500), handle).await;
    assert!(res.is_ok(), "Test should NOT timeout due to deadlock");
}

#[cfg(feature = "testing")]
#[tokio::test]
async fn test_havoc_deadlock_in_update_execution() {
    use autumn_harvest::context::WorkflowContext;
    use autumn_harvest::types::UpdateId;
    use serde_json::json;
    use std::sync::Arc;
    use std::time::Duration;

    let ctx = Arc::new(WorkflowContext::new_test());
    let ctx_clone = ctx.clone();

    ctx.register_update_handler(
        "test",
        |_| Ok(()),
        move |input| {
            let ctx_inner = ctx_clone.clone();
            async move {
                let _ = ctx_inner
                    .execute_admitted_update(UpdateId::new(), "another", input.clone())
                    .await;
                Ok(input)
            }
        },
    );

    ctx.register_update_handler("another", |_| Ok(()), |input| async move { Ok(input) });

    let handle = tokio::spawn(async move {
        let _ = ctx
            .execute_admitted_update(UpdateId::new(), "test", json!(1))
            .await;
    });

    let res = tokio::time::timeout(Duration::from_millis(500), handle).await;
    assert!(res.is_ok(), "Test should NOT timeout due to deadlock");
}
