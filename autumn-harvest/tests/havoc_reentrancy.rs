use autumn_harvest::WorkflowEvent;
use autumn_harvest::context::WorkflowContext;
use autumn_harvest::types::ExecutionId;
use autumn_harvest::types::UpdateId;
use serde_json::Value;
use std::sync::Arc;
use uuid::Uuid;

#[test]
fn test_update_validate_deadlock() {
    let events = vec![WorkflowEvent::WorkflowStarted {
        input: Value::Null,
        timestamp: chrono::Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    let ctx = Arc::new(WorkflowContext::for_replay(
        ExecutionId::from_uuid(Uuid::new_v4()),
        events,
    ));

    let ctx_clone = ctx.clone();

    // Validator that tries to register another update, causing a deadlock
    let validator = move |_: &Value| {
        // Attempt to register another update, which will try to lock update_registry
        ctx_clone
            .register_update_handler_no_validator("another_update", |input| async { Ok(input) });
        Ok(())
    };

    ctx.register_update_handler("my_update", validator, |input| async { Ok(input) });

    let (tx, rx) = std::sync::mpsc::channel();
    let ctx_run = ctx;
    std::thread::spawn(move || {
        let result = ctx_run.validate_update("my_update", &Value::Null);
        tx.send(result).unwrap();
    });

    match rx.recv_timeout(std::time::Duration::from_secs(2)) {
        Ok(res) => println!("Validation finished: {res:?}"),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("Deadlock!");
        }
        Err(e) => panic!("Thread error: {e:?}"),
    }
}

#[tokio::test]
async fn test_update_handler_deadlock() {
    let events = vec![WorkflowEvent::WorkflowStarted {
        input: Value::Null,
        timestamp: chrono::Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    let ctx = Arc::new(WorkflowContext::for_replay(
        ExecutionId::from_uuid(Uuid::new_v4()),
        events,
    ));

    let ctx_clone = ctx.clone();

    // Register update handler that tries to register another update handler in the outer closure
    ctx.register_update_handler_no_validator("my_update", move |input| {
        ctx_clone.register_update_handler_no_validator("another_update", |i| async { Ok(i) });
        async { Ok(input) }
    });

    let (tx, rx) = std::sync::mpsc::channel();
    let ctx_run = ctx;
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let result = ctx_run
                .execute_admitted_update(UpdateId::new(), "my_update", Value::Null)
                .await;
            tx.send(result).unwrap();
        });
    });

    match rx.recv_timeout(std::time::Duration::from_secs(2)) {
        Ok(res) => println!("Execution finished: {res:?}"),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("Deadlock!");
        }
        Err(e) => panic!("Thread error: {e:?}"),
    }
}
