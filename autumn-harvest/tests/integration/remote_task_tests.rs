//! Remote task calls with no database (issue #2006).
//!
//! - The MCP and A2A wire formats map to one task state.
//! - A tool result with `isError: true` is a completed result.
//! - The start activity sends the stable idempotency key.
//! - Replay returns the recorded outcome and calls nothing.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::context::{ActivityContext, WorkflowCommand, WorkflowContext};
use autumn_harvest::error::HarvestError;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::remote_task::{
    self, AWAIT_ACTIVITY, RemoteFuture, RemoteProtocol, RemoteTaskCall, RemoteTaskError,
    RemoteTaskHandle, RemoteTaskOutcome, RemoteTaskRequest, RemoteTaskStart, RemoteTaskState,
    RemoteTaskTransport, RemoteTasks, START_ACTIVITY, a2a, mcp,
};
use autumn_harvest::types::{ActivityExecId, ExecutionId, ExternalActivityToken};
use chrono::Utc;
use serde_json::{Value, json};

fn request(protocol: RemoteProtocol) -> RemoteTaskRequest {
    RemoteTaskRequest {
        server: "reports".into(),
        protocol,
        tool: "export".into(),
        arguments: json!({"year": 2026}),
    }
}

fn handle() -> RemoteTaskHandle {
    RemoteTaskHandle {
        server: "reports".into(),
        protocol: RemoteProtocol::Mcp,
        task_id: "task-7".into(),
    }
}

// ── MCP wire format ─────────────────────────────────────────────────────────

#[test]
fn mcp_tools_call_declares_the_tasks_extension_and_the_key() {
    let params = mcp::tools_call_params(&request(RemoteProtocol::Mcp), "key-1");
    assert_eq!(params["name"], "export");
    assert_eq!(params["arguments"], json!({"year": 2026}));
    let meta = &params["_meta"];
    assert_eq!(
        meta["io.modelcontextprotocol/protocolVersion"],
        "2026-07-28"
    );
    assert!(
        meta["io.modelcontextprotocol/clientCapabilities"]["extensions"]
            ["io.modelcontextprotocol/tasks"]
            .is_object()
    );
    assert_eq!(meta["io.autumn-harvest/idempotencyKey"], "key-1");
}

#[test]
fn mcp_task_result_is_a_handle() {
    let result = json!({"resultType": "task", "taskId": "task-7", "status": "working"});
    let start = mcp::parse_tools_call_result(&request(RemoteProtocol::Mcp), &result);
    assert_eq!(start, Ok(RemoteTaskStart::Task(handle())));
}

#[test]
fn mcp_complete_result_is_an_outcome() {
    let result = json!({"content": [{"type": "text", "text": "done"}], "isError": false});
    let start = mcp::parse_tools_call_result(&request(RemoteProtocol::Mcp), &result);
    assert_eq!(
        start,
        Ok(RemoteTaskStart::Completed(RemoteTaskOutcome {
            result: result.clone(),
            is_error: false,
        }))
    );
}

#[test]
fn mcp_is_error_result_is_completed_not_failed() {
    let result = json!({"content": [{"type": "text", "text": "bad year"}], "isError": true});
    let start = mcp::parse_tools_call_result(&request(RemoteProtocol::Mcp), &result);
    assert_eq!(
        start,
        Ok(RemoteTaskStart::Completed(RemoteTaskOutcome {
            result: result.clone(),
            is_error: true,
        }))
    );
}

#[test]
fn mcp_result_with_no_task_and_no_content_is_an_error() {
    let start = mcp::parse_tools_call_result(&request(RemoteProtocol::Mcp), &json!({}));
    let err = start.expect_err("no task id and no content");
    assert!(!err.retryable);
}

#[test]
fn mcp_tasks_get_names_the_task_and_declares_the_extension() {
    let params = mcp::tasks_get_params(&handle());
    assert_eq!(params["taskId"], "task-7");
    let meta = &params["_meta"];
    assert_eq!(
        meta["io.modelcontextprotocol/protocolVersion"],
        "2026-07-28"
    );
    assert!(
        meta["io.modelcontextprotocol/clientCapabilities"]["extensions"]
            ["io.modelcontextprotocol/tasks"]
            .is_object()
    );
}

#[test]
fn mcp_task_states_map_to_remote_states() {
    assert_eq!(
        mcp::parse_task(&json!({"taskId": "t", "status": "working"})),
        Ok(RemoteTaskState::Working)
    );
    assert_eq!(
        mcp::parse_task(&json!({"taskId": "t", "status": "input_required"})),
        Ok(RemoteTaskState::InputRequired)
    );
    let tool = json!({"content": [{"type": "text", "text": "no"}], "isError": true});
    assert_eq!(
        mcp::parse_task(&json!({"taskId": "t", "status": "completed", "result": tool})),
        Ok(RemoteTaskState::Completed(RemoteTaskOutcome {
            result: tool.clone(),
            is_error: true,
        }))
    );
    assert_eq!(
        mcp::parse_task(&json!({"taskId": "t", "status": "failed", "statusMessage": "boom"})),
        Ok(RemoteTaskState::Failed("boom".into()))
    );
    assert_eq!(
        mcp::parse_task(&json!({"taskId": "t", "status": "cancelled", "statusMessage": "stop"})),
        Ok(RemoteTaskState::Cancelled("stop".into()))
    );
    assert!(mcp::parse_task(&json!({"taskId": "t", "status": "odd"})).is_err());
}

#[test]
fn mcp_failed_task_reads_the_error_object() {
    let task =
        json!({"taskId": "t", "status": "failed", "error": {"code": -32603, "message": "x"}});
    assert_eq!(
        mcp::parse_task(&task),
        Ok(RemoteTaskState::Failed("x".into()))
    );
}

// ── A2A wire format ─────────────────────────────────────────────────────────

#[test]
fn a2a_message_send_uses_the_key_as_the_message_id() {
    let params = a2a::message_send_params(&request(RemoteProtocol::A2a), "key-2");
    assert_eq!(params["message"]["messageId"], "key-2");
    assert_eq!(params["message"]["role"], "user");
    assert_eq!(params["message"]["parts"][0]["kind"], "data");
    assert_eq!(
        params["message"]["parts"][0]["data"],
        json!({"skill": "export", "arguments": {"year": 2026}})
    );
}

#[test]
fn a2a_task_result_is_a_handle() {
    let result = json!({"kind": "task", "id": "a2a-1", "status": {"state": "submitted"}});
    let start = a2a::parse_send_result(&request(RemoteProtocol::A2a), &result);
    assert_eq!(
        start,
        Ok(RemoteTaskStart::Task(RemoteTaskHandle {
            server: "reports".into(),
            protocol: RemoteProtocol::A2a,
            task_id: "a2a-1".into(),
        }))
    );
}

#[test]
fn a2a_message_result_is_an_outcome() {
    let result = json!({"kind": "message", "role": "agent", "parts": []});
    let start = a2a::parse_send_result(&request(RemoteProtocol::A2a), &result);
    assert_eq!(
        start,
        Ok(RemoteTaskStart::Completed(RemoteTaskOutcome {
            result: result.clone(),
            is_error: false,
        }))
    );
}

#[test]
fn a2a_task_states_map_to_remote_states() {
    let task = |state: &str| json!({"id": "a", "status": {"state": state}});
    assert_eq!(
        a2a::parse_task(&task("working")),
        Ok(RemoteTaskState::Working)
    );
    assert_eq!(
        a2a::parse_task(&task("submitted")),
        Ok(RemoteTaskState::Working)
    );
    assert_eq!(
        a2a::parse_task(&task("input-required")),
        Ok(RemoteTaskState::InputRequired)
    );
    assert_eq!(
        a2a::parse_task(&task("TASK_STATE_AUTH_REQUIRED")),
        Ok(RemoteTaskState::InputRequired)
    );
    let done = task("completed");
    assert_eq!(
        a2a::parse_task(&done),
        Ok(RemoteTaskState::Completed(RemoteTaskOutcome {
            result: done.clone(),
            is_error: false,
        }))
    );
    assert!(matches!(
        a2a::parse_task(&task("TASK_STATE_FAILED")),
        Ok(RemoteTaskState::Failed(_))
    ));
    assert!(matches!(
        a2a::parse_task(&task("rejected")),
        Ok(RemoteTaskState::Failed(_))
    ));
    assert!(matches!(
        a2a::parse_task(&task("canceled")),
        Ok(RemoteTaskState::Cancelled(_))
    ));
    assert!(a2a::parse_task(&json!({"id": "a"})).is_err());
}

// ── The start activity ──────────────────────────────────────────────────────

/// Records each start key and answers with a fixed result.
struct RecordingTransport {
    keys: Arc<Mutex<Vec<String>>>,
    answer: Result<RemoteTaskStart, RemoteTaskError>,
}

impl RemoteTaskTransport for RecordingTransport {
    fn start<'a>(
        &'a self,
        _request: &'a RemoteTaskRequest,
        idempotency_key: &'a str,
    ) -> RemoteFuture<'a, Result<RemoteTaskStart, RemoteTaskError>> {
        self.keys
            .lock()
            .expect("keys")
            .push(idempotency_key.to_string());
        let answer = self.answer.clone();
        Box::pin(async move { answer })
    }

    fn get<'a>(
        &'a self,
        _handle: &'a RemoteTaskHandle,
    ) -> RemoteFuture<'a, Result<RemoteTaskState, RemoteTaskError>> {
        Box::pin(async { Ok(RemoteTaskState::Working) })
    }
}

fn state_with(remote: RemoteTasks) -> autumn_harvest::context::SharedState {
    let mut map: HashMap<TypeId, Box<dyn Any + Send + Sync>> = HashMap::new();
    map.insert(TypeId::of::<RemoteTasks>(), Box::new(remote));
    Arc::new(map)
}

fn start_handler() -> autumn_harvest::info::ActivityHandlerFn {
    remote_task::activities()
        .into_iter()
        .find(|info| info.name == START_ACTIVITY)
        .expect("the start activity is registered")
        .handler
}

#[test]
fn the_start_activity_is_a_queued_activity_with_retries() {
    let infos = remote_task::activities();
    let start = infos
        .iter()
        .find(|info| info.name == START_ACTIVITY)
        .expect("start activity");
    assert!(!start.is_local);
    assert!(start.default_retry_policy.is_some());
    assert!(start.default_start_to_close.is_some());
}

#[tokio::test]
async fn the_start_activity_sends_the_stable_idempotency_key() {
    let keys = Arc::new(Mutex::new(Vec::new()));
    let remote = RemoteTasks::new(RecordingTransport {
        keys: Arc::clone(&keys),
        answer: Ok(RemoteTaskStart::Task(handle())),
    });
    let ctx = ActivityContext::new_test_with_state(state_with(remote));
    let expected = ctx.idempotency_key().expect("key").as_str().to_string();
    let input = serde_json::to_value(request(RemoteProtocol::Mcp)).expect("encode");

    let output = (start_handler())(&ctx, input.clone()).await.expect("start");
    let start: RemoteTaskStart = serde_json::from_value(output).expect("decode");
    assert_eq!(start, RemoteTaskStart::Task(handle()));
    // A retry of the same activity sends the same key.
    (start_handler())(&ctx, input).await.expect("retry");
    assert_eq!(
        *keys.lock().expect("keys"),
        vec![expected.clone(), expected]
    );
}

#[tokio::test]
async fn a_non_retryable_start_error_fails_the_activity_for_good() {
    let remote = RemoteTasks::new(RecordingTransport {
        keys: Arc::new(Mutex::new(Vec::new())),
        answer: Err(RemoteTaskError {
            message: "unknown tool".into(),
            retryable: false,
        }),
    });
    let ctx = ActivityContext::new_test_with_state(state_with(remote));
    let input = serde_json::to_value(request(RemoteProtocol::Mcp)).expect("encode");
    let err = (start_handler())(&ctx, input).await.expect_err("fails");
    let failure = autumn_harvest::failure::parse_typed_payload(&err).expect("typed failure");
    assert!(failure.non_retryable, "got {err}");
    assert!(err.contains("unknown tool"));
}

#[tokio::test]
async fn the_start_activity_needs_the_worker_state() {
    let ctx = ActivityContext::new_test();
    let input = serde_json::to_value(request(RemoteProtocol::Mcp)).expect("encode");
    let err = (start_handler())(&ctx, input).await.expect_err("no state");
    assert!(err.contains("RemoteTasks"), "got {err}");
}

// ── The workflow call ───────────────────────────────────────────────────────

fn call() -> RemoteTaskCall {
    RemoteTaskCall::mcp(
        "reports",
        "export",
        json!({"year": 2026}),
        Duration::from_secs(600),
    )
}

fn started() -> WorkflowEvent {
    WorkflowEvent::WorkflowStarted {
        input: Value::Null,
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }
}

fn scheduled_start(id: ActivityExecId) -> WorkflowEvent {
    WorkflowEvent::ActivityScheduled {
        activity_id: id,
        name: START_ACTIVITY.into(),
        input: serde_json::to_value(request(RemoteProtocol::Mcp)).expect("encode"),
        queue: "default".into(),
    }
}

fn completed_start(id: ActivityExecId, start: &RemoteTaskStart) -> WorkflowEvent {
    WorkflowEvent::ActivityCompleted {
        activity_id: id,
        output: serde_json::to_value(start).expect("encode"),
    }
}

fn awaiting(id: ActivityExecId, token: ExternalActivityToken) -> WorkflowEvent {
    WorkflowEvent::ActivityAwaitingExternal {
        activity_id: id,
        token,
        name: AWAIT_ACTIVITY.into(),
        input: serde_json::to_value(handle()).expect("encode"),
        queue: "default".into(),
        schedule_to_close_secs: 600,
    }
}

#[tokio::test]
async fn the_first_call_schedules_the_start_activity() {
    let ctx = Arc::new(WorkflowContext::new_test());
    let ctx2 = Arc::clone(&ctx);
    let task = tokio::spawn(async move { remote_task::call(&ctx2, &call()).await });
    tokio::task::yield_now().await;

    let commands = ctx.drain_commands();
    assert_eq!(commands.len(), 1);
    let WorkflowCommand::ScheduleActivity {
        name, input, queue, ..
    } = &commands[0]
    else {
        panic!("expected ScheduleActivity, got {:?}", commands[0]);
    };
    assert_eq!(name, START_ACTIVITY);
    assert_eq!(queue, "default");
    let sent: RemoteTaskRequest = serde_json::from_value(input.clone()).expect("decode");
    assert_eq!(sent, request(RemoteProtocol::Mcp));
    task.abort();
}

#[tokio::test]
async fn a_recorded_handle_suspends_on_the_external_token() {
    let start_id = ActivityExecId::new();
    let events = vec![
        started(),
        scheduled_start(start_id),
        completed_start(start_id, &RemoteTaskStart::Task(handle())),
    ];
    let ctx = Arc::new(WorkflowContext::for_replay(ExecutionId::new(), events));
    let ctx2 = Arc::clone(&ctx);
    let task = tokio::spawn(async move { remote_task::call(&ctx2, &call()).await });
    tokio::task::yield_now().await;

    let commands = ctx.drain_commands();
    assert_eq!(commands.len(), 1);
    let WorkflowCommand::ScheduleExternalActivity {
        name,
        input,
        schedule_to_close_secs,
        ..
    } = &commands[0]
    else {
        panic!("expected ScheduleExternalActivity, got {:?}", commands[0]);
    };
    assert_eq!(name, AWAIT_ACTIVITY);
    assert_eq!(*schedule_to_close_secs, 600);
    let journaled: RemoteTaskHandle = serde_json::from_value(input.clone()).expect("decode");
    assert_eq!(journaled, handle(), "the remote handle is journaled");
    task.abort();
}

#[tokio::test]
async fn replay_returns_an_is_error_outcome_as_ok() {
    let start_id = ActivityExecId::new();
    let await_id = ActivityExecId::new();
    let token = ExternalActivityToken::new();
    let outcome = RemoteTaskOutcome {
        result: json!({"content": [], "isError": true}),
        is_error: true,
    };
    let events = vec![
        started(),
        scheduled_start(start_id),
        completed_start(start_id, &RemoteTaskStart::Task(handle())),
        awaiting(await_id, token),
        WorkflowEvent::ActivityCompletedExternally {
            activity_id: await_id,
            token,
            output: serde_json::to_value(&outcome).expect("encode"),
        },
    ];
    let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
    let got = remote_task::call(&ctx, &call())
        .await
        .expect("isError is Ok");
    assert_eq!(got, outcome);
    assert!(ctx.drain_commands().is_empty(), "replay calls nothing");
}

#[tokio::test]
async fn a_start_that_completes_at_once_needs_no_token() {
    let start_id = ActivityExecId::new();
    let outcome = RemoteTaskOutcome {
        result: json!({"content": [{"type": "text", "text": "ok"}], "isError": false}),
        is_error: false,
    };
    let events = vec![
        started(),
        scheduled_start(start_id),
        completed_start(start_id, &RemoteTaskStart::Completed(outcome.clone())),
    ];
    let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
    let got = remote_task::call(&ctx, &call()).await.expect("ok");
    assert_eq!(got, outcome);
    assert!(ctx.drain_commands().is_empty());
}

#[tokio::test]
async fn replay_returns_a_remote_failure_as_activity_failed() {
    let start_id = ActivityExecId::new();
    let await_id = ActivityExecId::new();
    let token = ExternalActivityToken::new();
    let events = vec![
        started(),
        scheduled_start(start_id),
        completed_start(start_id, &RemoteTaskStart::Task(handle())),
        awaiting(await_id, token),
        WorkflowEvent::ActivityFailedExternally {
            activity_id: await_id,
            token,
            error: "remote task failed: boom".into(),
            retryable: false,
        },
    ];
    let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
    let err = remote_task::call(&ctx, &call()).await.expect_err("fails");
    assert!(
        matches!(&err, HarvestError::ActivityFailed { name, .. } if name == AWAIT_ACTIVITY),
        "got {err:?}"
    );
}

#[test]
fn call_builders_set_the_protocol_and_the_queue() {
    let a2a_call =
        RemoteTaskCall::a2a("agent", "summarise", json!({}), Duration::from_secs(5)).on_queue("q");
    assert_eq!(a2a_call.request.protocol, RemoteProtocol::A2a);
    assert_eq!(a2a_call.queue, "q");
    assert_eq!(call().queue, "default");
}
