//! Input and signal schemas are checked where the runtime decides (issue
//! #2467).
//!
//! - A signal checks its `#[signal]` `arg_schema` after the committed key
//!   probe. A retry of a delivered keyed signal stays a duplicate.
//! - A signal-with-start checks the workflow's `input_schema` only when it
//!   creates a run. An attach discards the input.

// The `#[workflow]` macro needs an `async fn`.
#![allow(clippy::unused_async)]
// The macro reads the unused `_input` through its expansion.
#![allow(clippy::used_underscore_binding)]

use autumn_harvest::prelude::*;
use autumn_harvest::{SignalHandlerInfo, WorkflowIdReusePolicy};
use autumn_harvest_sqlite::{SignalWithStart, SqliteError, SqliteRuntime};
use serde_json::{Value, json};

#[workflow]
async fn waits(ctx: &WorkflowContext, _input: Value) -> Result<Value, String> {
    ctx.wait_for_signal("go")
        .await
        .map_err(|error| error.to_string())
}

fn object_schema() -> Value {
    json!({ "type": "object" })
}

fn go_handler() -> SignalHandlerInfo {
    SignalHandlerInfo {
        name: "go",
        workflow: "waits",
        module: module_path!(),
        arg_type_hint: "serde_json::Value",
        description: None,
        arg_schema: Some(object_schema),
    }
}

fn runtime(input_schema: bool) -> SqliteRuntime {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    let mut info = waits_info();
    if input_schema {
        info.input_schema = Some(object_schema);
    }
    rt.register_workflow(&info);
    rt
}

#[tokio::test]
async fn a_fresh_signal_must_meet_the_schema() {
    let mut rt = runtime(false);
    rt.register_signal_handler(&go_handler());
    let exec = rt.start_workflow("waits", json!({})).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    let refused = rt.send_signal_idempotent(exec, "go", json!(42), Some("k"));
    assert!(matches!(
        refused,
        Err(SqliteError::SignalPayloadInvalid { ref signal, .. }) if signal == "go"
    ));
    let by_id = rt
        .start_workflow_with_id("waits", "w-1", json!({}))
        .unwrap();
    rt.run_until_blocked(by_id).await.unwrap();
    assert!(matches!(
        rt.send_signal_by_workflow_id("waits", "w-1", "go", json!(42), None),
        Err(SqliteError::SignalPayloadInvalid { .. })
    ));
    assert!(
        rt.send_signal_idempotent(exec, "go", json!({}), Some("k"))
            .unwrap()
    );
}

#[tokio::test]
async fn a_delivered_keyed_signal_stays_a_duplicate_after_the_schema_tightens() {
    let mut rt = runtime(false);
    let exec = rt
        .start_workflow_with_id("waits", "w-2", json!({}))
        .unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    // Delivered before any schema existed.
    assert!(
        rt.send_signal_idempotent(exec, "go", json!(42), Some("k1"))
            .unwrap()
    );
    // A deploy adds a schema the old payload breaks.
    rt.register_signal_handler(&go_handler());
    assert!(
        !rt.send_signal_idempotent(exec, "go", json!(42), Some("k1"))
            .unwrap()
    );
    let retry = rt
        .send_signal_by_workflow_id("waits", "w-2", "go", json!(42), Some("k1"))
        .unwrap();
    assert!(!retry.delivered);
    let again = rt
        .signal_with_start(SignalWithStart {
            workflow_name: "waits",
            workflow_id: "w-2",
            input: json!({}),
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            signal_name: "go",
            signal_payload: json!(42),
            idempotency_key: Some("k1"),
        })
        .unwrap();
    assert!(!again.signal_delivered);
}

#[tokio::test]
async fn signal_with_start_checks_the_input_only_when_it_creates_a_run() {
    let mut rt = runtime(true);
    let request = |input: Value, workflow_id: &'static str| SignalWithStart {
        workflow_name: "waits",
        workflow_id,
        input,
        reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
        signal_name: "go",
        signal_payload: json!({}),
        idempotency_key: None,
    };
    // A fresh run with an input that breaks the schema is refused, and
    // nothing is written.
    assert!(matches!(
        rt.signal_with_start(request(Value::Null, "w-3")),
        Err(SqliteError::WorkflowInputInvalid { ref workflow, .. }) if workflow == "waits"
    ));
    assert!(matches!(
        rt.send_signal_by_workflow_id("waits", "w-3", "go", json!({}), None),
        Err(SqliteError::WorkflowIdNotFound { .. })
    ));
    // A valid input starts the run.
    let started = rt.signal_with_start(request(json!({}), "w-3")).unwrap();
    assert!(started.started_fresh);
    // An attach discards the input, so a missing one is fine.
    let attached = rt.signal_with_start(request(Value::Null, "w-3")).unwrap();
    assert!(!attached.started_fresh);
    assert_eq!(attached.exec_id, started.exec_id);
}
