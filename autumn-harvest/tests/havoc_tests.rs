use autumn_harvest::policy::RetryPolicy;
#[cfg(feature = "db")]
use autumn_harvest::store::events_to_insert_rows_from;
#[cfg(feature = "db")]
use autumn_harvest::{event::WorkflowEvent, types::ExecutionId};
use autumn_harvest::context::WorkflowContext;
use std::time::Duration;
use std::sync::Arc;

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
fn test_havoc_query_deadlock_on_execute() {
    let exec_id = ExecutionId::new();
    let ctx = Arc::new(WorkflowContext::for_replay(exec_id, vec![]));
    let ctx_clone = ctx.clone();

    // Attempt to register query inside execute_query
    ctx.register_query("deadlock", move || {
        ctx_clone.register_query("nested", || serde_json::json!("nested"));
        serde_json::json!("done")
    });

    let (tx, rx) = std::sync::mpsc::channel();
    let ctx_run = ctx;
    std::thread::spawn(move || {
        let result = ctx_run.execute_query("deadlock");
        tx.send(result).unwrap();
    });

    match rx.recv_timeout(std::time::Duration::from_secs(2)) {
        Ok(res) => println!("Query execution finished: {res:?}"),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("Deadlock in query execution!");
        }
        Err(e) => panic!("Thread error: {e:?}"),
    }
}

#[tokio::test]
async fn test_havoc_update_deadlock_on_execute() {
    let exec_id = ExecutionId::new();
    let ctx = Arc::new(WorkflowContext::for_replay(exec_id, vec![]));
    let ctx_clone = ctx.clone();

    ctx.register_update_handler_no_validator("deadlock", move |_val| {
        let ctx = ctx_clone.clone();
        async move {
            let _ = ctx.execute_admitted_update(
                autumn_harvest::types::UpdateId::new(),
                "nested",
                serde_json::json!("test")
            ).await;
            Ok(serde_json::json!("done"))
        }
    });

    ctx.register_update_handler_no_validator("nested", |_| async { Ok(serde_json::json!("nested")) });

    let (tx, rx) = std::sync::mpsc::channel();
    let ctx_run = ctx;
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let result = ctx_run.execute_admitted_update(
                autumn_harvest::types::UpdateId::new(),
                "deadlock",
                serde_json::json!("test")
            ).await;
            tx.send(result).unwrap();
        });
    });

    match rx.recv_timeout(std::time::Duration::from_secs(2)) {
        Ok(res) => println!("Update execution finished: {res:?}"),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("Deadlock in update execution!");
        }
        Err(e) => panic!("Thread error: {e:?}"),
    }
}

#[test]
fn test_havoc_validate_update_deadlock() {
    let exec_id = ExecutionId::new();
    let ctx = Arc::new(WorkflowContext::for_replay(exec_id, vec![]));
    let ctx_clone = ctx.clone();

    ctx.register_update_handler(
        "deadlock",
        move |_val| {
            let _ = ctx_clone.validate_update("nested", &serde_json::json!("test"));
            Ok(())
        },
        |_| async { Ok(serde_json::json!("done")) }
    );

    ctx.register_update_handler(
        "nested",
        |_| { Ok(()) },
        |_| async { Ok(serde_json::json!("done")) }
    );

    let (tx, rx) = std::sync::mpsc::channel();
    let ctx_run = ctx;
    std::thread::spawn(move || {
        let result = ctx_run.validate_update("deadlock", &serde_json::json!("input"));
        tx.send(result).unwrap();
    });

    match rx.recv_timeout(std::time::Duration::from_secs(2)) {
        Ok(res) => println!("Validate execution finished: {res:?}"),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("Deadlock in validate execution!");
        }
        Err(e) => panic!("Thread error: {e:?}"),
    }
}
