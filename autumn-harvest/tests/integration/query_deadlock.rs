use autumn_harvest::{WorkflowContext, types::ExecutionId};
use serde_json::json;
use std::sync::Arc;

#[test]
fn test_query_deadlock_detonation() {
    let ctx = Arc::new(WorkflowContext::for_replay(ExecutionId::new(), vec![]));
    let ctx_clone = Arc::clone(&ctx);

    ctx.register_query("other", || json!({"status": "ok"}));

    ctx.register_query("deadlock", move || {
        // This will try to acquire the same lock that is currently held by execute_query
        ctx_clone.execute_query("other").unwrap();
        json!({})
    });

    let (tx, rx) = std::sync::mpsc::channel();

    std::thread::spawn(move || {
        let _ = ctx.execute_query("deadlock");
        tx.send(()).unwrap();
    });

    let res = rx.recv_timeout(std::time::Duration::from_secs(1));
    assert!(res.is_ok(), "The query deadlocked and timed out!");
}
