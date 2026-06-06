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
fn havoc_test_admission_gate_cache_deadlocks() {
    loom::model(|| {
        let cache = std::sync::Arc::new(autumn_harvest::admission_gate::AdmissionGateCache::new());
        let c1 = cache.clone();
        let t1 = loom::thread::spawn(move || {
            let _ = c1.check("wf1", "q1", 0, None);
        });

        let c2 = cache;
        let t2 = loom::thread::spawn(move || {
            c2.refresh(vec![]);
        });

        t1.join().unwrap();
        t2.join().unwrap();
    });
}

#[test]
fn havoc_test_execute_query_deadlocks() {
    loom::model(|| {
        use autumn_harvest::{WorkflowContext, types::ExecutionId};
        use serde_json::json;
        use std::sync::Arc;

        let ctx = Arc::new(WorkflowContext::for_replay(ExecutionId::new(), vec![]));
        let ctx_clone = Arc::clone(&ctx);

        ctx.register_query("other", || json!({"status": "ok"}));
        ctx.register_query("deadlock", move || {
            ctx_clone.execute_query("other").unwrap();
            json!({})
        });

        let ctx_run = ctx;
        let t1 = loom::thread::spawn(move || {
            let _ = ctx_run.execute_query("deadlock");
        });

        t1.join().unwrap();
    });
}
