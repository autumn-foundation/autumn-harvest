//! Offline tests for the agent daemon.
//!
//! Every test runs the scripted stub model, so the suite needs no API key and
//! no network. The stub drives the same loop the live model does: one tool
//! call, one approval-gated write, then a final answer.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use autumn_harvest_sqlite::{ExecutionId, RunState, SqliteRuntime};
use serde_json::{Value, json};

use crate::claude;
use crate::daemon;
use crate::guard;
use crate::protocol::{self, Request, Response};
use crate::session::{
    self, ApprovalDecision, SIGNAL_TOOL_APPROVAL, SessionReport, SessionTask, ToolCall,
    ToolOutcome, TurnReply, TurnRequest, WORKFLOW_NAME,
};
use crate::tools;

/// The task every test submits.
fn task() -> Value {
    serde_json::to_value(SessionTask {
        goal: "summarise the workspace".to_string(),
        max_turns: 6,
        approval_timeout_secs: 300,
    })
    .expect("the task encodes")
}

/// A stub model body that counts its calls.
fn counting_model(
    calls: Arc<AtomicUsize>,
) -> impl Fn(Value) -> Result<Value, String> + Send + Sync + 'static {
    move |input| {
        let request: TurnRequest =
            serde_json::from_value(input).map_err(|e| format!("bad request: {e}"))?;
        calls.fetch_add(1, Ordering::SeqCst);
        serde_json::to_value(claude::offline::reply(&request))
            .map_err(|e| format!("bad reply: {e}"))
    }
}

/// Open a runtime with both activity bodies registered.
fn runtime(db: &Path, workspace: &Path, calls: &Arc<AtomicUsize>) -> SqliteRuntime {
    let mut rt = SqliteRuntime::open(db).expect("the database opens");
    rt.register_workflow(&session::agent_session_info());
    rt.register_activity(&session::claude_turn_info(), counting_model(calls.clone()));
    rt.register_activity(
        &session::run_tool_info(),
        tools::activity_body(workspace.to_path_buf()),
    );
    rt
}

/// Deliver an approval decision to the signal a session is waiting on.
fn approve(rt: &mut SqliteRuntime, exec: ExecutionId, signal: &str) {
    let payload = serde_json::to_value(ApprovalDecision {
        approved: true,
        note: None,
    })
    .expect("the decision encodes");
    rt.send_signal(exec, signal, payload)
        .expect("the signal is staged");
}

/// Drive to the next stop and return the approval signal the run waits on.
async fn drive_to_approval(rt: &mut SqliteRuntime, exec: ExecutionId) -> String {
    let state = rt.run_until_blocked(exec).await.expect("the run advances");
    let RunState::WaitingSignal(signal) = state else {
        panic!("expected an approval wait, got {state:?}");
    };
    assert!(
        session::approval_call_id(&signal).is_some(),
        "the wait must name the tool call it releases: {signal}"
    );
    signal
}

#[tokio::test]
async fn a_session_runs_its_tools_and_finishes_after_approval() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");
    std::fs::write(workspace.join("README.md"), "hello").expect("the fixture is written");

    let calls = Arc::new(AtomicUsize::new(0));
    let mut rt = runtime(&dir.path().join("agentd.db"), &workspace, &calls);
    let exec = rt
        .start_workflow(WORKFLOW_NAME, task())
        .expect("the session starts");

    // Turn one lists the workspace. Turn two proposes a write, which parks the
    // run on that call's own approval signal.
    let signal = drive_to_approval(&mut rt, exec).await;
    assert!(
        !workspace.join("agent-notes.md").exists(),
        "the gated write must not run before approval"
    );

    approve(&mut rt, exec, &signal);
    let state = rt.run_until_blocked(exec).await.expect("the run finishes");
    assert!(
        matches!(state, RunState::Completed(_)),
        "expected completion, got {state:?}"
    );
    assert!(
        workspace.join("agent-notes.md").exists(),
        "the approved write must run"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 3, "three model calls");
}

#[tokio::test]
async fn a_denied_call_is_reported_to_the_model_and_the_session_continues() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");

    let calls = Arc::new(AtomicUsize::new(0));
    let mut rt = runtime(&dir.path().join("agentd.db"), &workspace, &calls);
    let exec = rt
        .start_workflow(WORKFLOW_NAME, task())
        .expect("the session starts");
    let signal = drive_to_approval(&mut rt, exec).await;

    let payload = serde_json::to_value(ApprovalDecision {
        approved: false,
        note: Some("not this file".to_string()),
    })
    .expect("the decision encodes");
    rt.send_signal(exec, &signal, payload)
        .expect("the signal is staged");

    let state = rt.run_until_blocked(exec).await.expect("the run finishes");
    assert!(
        matches!(state, RunState::Completed(_)),
        "expected completion, got {state:?}"
    );
    assert!(
        !workspace.join("agent-notes.md").exists(),
        "a denied write must never run"
    );
}

#[tokio::test]
async fn a_restart_resumes_the_session_without_repeating_model_calls() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("agentd.db");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");

    // Session one drives to the approval block, then the process "crashes".
    let first_calls = Arc::new(AtomicUsize::new(0));
    let (exec, signal) = {
        let mut rt = runtime(&db, &workspace, &first_calls);
        let exec = rt
            .start_workflow(WORKFLOW_NAME, task())
            .expect("the session starts");
        let signal = drive_to_approval(&mut rt, exec).await;
        (exec, signal)
    };
    assert_eq!(first_calls.load(Ordering::SeqCst), 2, "two model calls");

    // Session two reopens the same file. The recorded turns replay, so only the
    // turn after the approval reaches the model.
    let second_calls = Arc::new(AtomicUsize::new(0));
    let mut rt = runtime(&db, &workspace, &second_calls);
    approve(&mut rt, exec, &signal);
    let state = rt.run_until_blocked(exec).await.expect("the run finishes");

    assert!(
        matches!(state, RunState::Completed(_)),
        "expected completion, got {state:?}"
    );
    assert_eq!(
        second_calls.load(Ordering::SeqCst),
        1,
        "the replayed turns must not call the model again"
    );
}

#[test]
fn the_toolbox_refuses_a_path_outside_the_workspace() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let body = tools::activity_body(dir.path().to_path_buf());
    let call = ToolCall {
        id: "toolu_test".to_string(),
        name: tools::TOOL_READ_FILE.to_string(),
        input: json!({ "path": "../../etc/passwd" }),
    };

    let raw = body(serde_json::to_value(call).expect("the call encodes"))
        .expect("a tool failure is a result, not an activity error");
    let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
    assert!(outcome.is_error, "the escape must fail");
    assert!(
        outcome.output.contains("leaves the workspace"),
        "unexpected message: {}",
        outcome.output
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_daemon_serves_one_session_over_its_socket() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let socket = dir.path().join("agentd.sock");
    let options = daemon::Options {
        db: dir.path().join("agentd.db"),
        socket: socket.clone(),
        workspace: dir.path().join("workspace"),
        model: claude::DEFAULT_MODEL.to_string(),
        max_tokens: claude::DEFAULT_MAX_TOKENS,
        tick: Duration::from_millis(50),
        api_key: None,
    };
    let daemon = tokio::spawn(daemon::serve(options));

    // The daemon binds the socket a moment after it starts.
    let mut ready = false;
    for _ in 0..100 {
        if socket.exists() {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(ready, "the daemon never bound its socket");

    let submitted = protocol::call(
        &socket,
        &Request::Submit {
            goal: "summarise the workspace".to_string(),
            max_turns: 6,
            approval_timeout_secs: 300,
        },
    )
    .await
    .expect("the submit is answered");
    let Response::Submitted { execution_id } = submitted else {
        panic!("unexpected answer: {submitted:?}");
    };

    // Approve as soon as the session parks, then wait for the terminal state.
    let mut approved = false;
    let mut final_state = String::new();
    for _ in 0..200 {
        let answer = protocol::call(
            &socket,
            &Request::Status {
                execution_id: execution_id.clone(),
            },
        )
        .await
        .expect("the status is answered");
        let Response::Session { session: view } = answer else {
            panic!("unexpected answer: {answer:?}");
        };
        if view.state != "RUNNING" {
            final_state = view.state;
            break;
        }
        if !approved && view.blocked_on.is_some() {
            // An operator approves an action, not a session, so the exact call
            // must be visible before the decision.
            let pending = view.pending.as_ref().expect("the pending call is shown");
            assert_eq!(pending.tool, tools::TOOL_WRITE_FILE);
            assert!(
                pending.input.contains("agent-notes.md"),
                "the status must show what the write does: {}",
                pending.input
            );
            protocol::call(
                &socket,
                &Request::Approve {
                    execution_id: execution_id.clone(),
                    approved: true,
                    note: None,
                },
            )
            .await
            .expect("the approval is answered");
            approved = true;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    assert!(approved, "the session never asked for approval");
    assert_eq!(final_state, "COMPLETED", "the session did not finish");

    // The list and history answers carry sequences, which an internally tagged
    // enum only encodes from a struct variant. Assert both over the socket.
    let listed = protocol::call(&socket, &Request::List)
        .await
        .expect("the list is answered");
    let Response::Sessions { sessions } = listed else {
        panic!("unexpected answer: {listed:?}");
    };
    assert_eq!(sessions.len(), 1, "one session is recorded");

    let logged = protocol::call(&socket, &Request::History { execution_id })
        .await
        .expect("the history is answered");
    let Response::History { events } = logged else {
        panic!("unexpected answer: {logged:?}");
    };
    assert!(
        events.iter().any(|event| event.contains("WorkflowStarted")),
        "the event log is missing its start: {events:?}"
    );

    daemon.abort();
}

#[tokio::test]
async fn a_truncated_turn_never_reports_a_clean_finish() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");

    // A reply cut short by the output cap: text, no tool calls, and the
    // `max_tokens` stop reason the API reports for a truncated turn.
    let mut rt = SqliteRuntime::open(dir.path().join("agentd.db")).expect("the database opens");
    rt.register_workflow(&session::agent_session_info());
    rt.register_activity(&session::claude_turn_info(), |_input| {
        serde_json::to_value(TurnReply {
            content: json!([{ "type": "text", "text": "half an ans" }]),
            stop_reason: "max_tokens".to_string(),
            text: "half an ans".to_string(),
            tool_calls: Vec::new(),
        })
        .map_err(|e| format!("bad reply: {e}"))
    });
    rt.register_activity(
        &session::run_tool_info(),
        tools::activity_body(workspace.clone()),
    );

    let exec = rt
        .start_workflow(WORKFLOW_NAME, task())
        .expect("the session starts");
    let state = rt.run_until_blocked(exec).await.expect("the run finishes");
    let RunState::Completed(output) = state else {
        panic!("expected a terminal report, got {state:?}");
    };

    let report: SessionReport = serde_json::from_value(output).expect("the report decodes");
    assert_eq!(
        report.stop, "max_tokens",
        "a truncated turn must not report as `end_turn`"
    );
}

#[test]
fn the_toolbox_refuses_a_symlink_that_escapes_the_workspace() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    let outside = dir.path().join("outside");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");
    std::fs::create_dir_all(&outside).expect("the outside directory is created");
    std::fs::write(outside.join("secret.txt"), "classified").expect("the secret is written");

    // One link to a directory outside the workspace, and one straight to the
    // file. The first escapes through the middle of a path, the second through
    // its final component.
    std::os::unix::fs::symlink(&outside, workspace.join("link")).expect("the link is created");
    std::os::unix::fs::symlink(outside.join("secret.txt"), workspace.join("direct"))
        .expect("the link is created");

    let body = tools::activity_body(workspace);
    for path in ["link/secret.txt", "direct"] {
        let call = ToolCall {
            id: "toolu_test".to_string(),
            name: tools::TOOL_READ_FILE.to_string(),
            input: json!({ "path": path }),
        };
        let raw = body(serde_json::to_value(call).expect("the call encodes"))
            .expect("a tool failure is a result, not an activity error");
        let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
        assert!(outcome.is_error, "`{path}` must not resolve");
        assert!(
            !outcome.output.contains("classified"),
            "`{path}` leaked the file outside the workspace"
        );
    }

    // A write through the escaping link must not land outside either.
    let call = ToolCall {
        id: "toolu_test".to_string(),
        name: tools::TOOL_WRITE_FILE.to_string(),
        input: json!({ "path": "link/planted.txt", "content": "planted" }),
    };
    let raw = body(serde_json::to_value(call).expect("the call encodes"))
        .expect("a tool failure is a result, not an activity error");
    let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
    assert!(outcome.is_error, "the write must not resolve");
    assert!(
        !outside.join("planted.txt").exists(),
        "the write escaped the workspace"
    );
}

#[test]
fn a_second_daemon_cannot_open_a_database_another_one_holds() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("agentd.db");

    let held = guard::acquire(&db).expect("the first daemon takes the lock");
    let refused = guard::acquire(&db);
    assert!(
        refused.is_err(),
        "a second daemon must not open a database another one holds"
    );

    // Releasing the lock is what a process exit does, so a restart succeeds.
    drop(held);
    assert!(
        guard::acquire(&db).is_ok(),
        "the lock must be free once its holder is gone"
    );
}

#[tokio::test]
async fn a_stale_approval_cannot_release_a_later_tool_call() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");

    let calls = Arc::new(AtomicUsize::new(0));
    let mut rt = runtime(&dir.path().join("agentd.db"), &workspace, &calls);
    let exec = rt
        .start_workflow(WORKFLOW_NAME, task())
        .expect("the session starts");
    let signal = drive_to_approval(&mut rt, exec).await;

    // A decision that does not name this call must not release it. The bare
    // prefix is what an early or repeated `approve` used to stage.
    let payload = serde_json::to_value(ApprovalDecision {
        approved: true,
        note: None,
    })
    .expect("the decision encodes");
    rt.send_signal(exec, SIGNAL_TOOL_APPROVAL, payload)
        .expect("the signal is staged");

    let state = rt.run_until_blocked(exec).await.expect("the run advances");
    assert!(
        matches!(&state, RunState::WaitingSignal(name) if name == &signal),
        "an unaddressed decision must leave the call parked, got {state:?}"
    );
    assert!(
        !workspace.join("agent-notes.md").exists(),
        "an unaddressed decision must not authorise the write"
    );

    // The decision that names the call does release it.
    approve(&mut rt, exec, &signal);
    let state = rt.run_until_blocked(exec).await.expect("the run finishes");
    assert!(
        matches!(state, RunState::Completed(_)),
        "expected completion, got {state:?}"
    );
}

#[test]
fn the_daemon_lock_follows_the_database_through_a_symlink() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let real = dir.path().join("real.db");
    let alias = dir.path().join("current.db");
    std::fs::write(&real, b"").expect("the database file is created");
    std::os::unix::fs::symlink(&real, &alias).expect("the alias is created");

    // Two spellings of one file must take one lock, or two daemons write it.
    let held = guard::acquire(&real).expect("the first daemon takes the lock");
    assert!(
        guard::acquire(&alias).is_err(),
        "an alias of a held database must not take a second lock"
    );

    drop(held);
    assert!(
        guard::acquire(&alias).is_ok(),
        "the lock must be free once its holder is gone"
    );
}

#[test]
fn the_toolbox_refuses_a_file_over_the_read_cap_without_reading_it() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().to_path_buf();
    let oversized = vec![b'x'; 70 * 1024];
    std::fs::write(workspace.join("big.txt"), &oversized).expect("the fixture is written");

    let body = tools::activity_body(workspace);
    let call = ToolCall {
        id: "toolu_test".to_string(),
        name: tools::TOOL_READ_FILE.to_string(),
        input: json!({ "path": "big.txt" }),
    };
    let raw = body(serde_json::to_value(call).expect("the call encodes"))
        .expect("a tool failure is a result, not an activity error");
    let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");

    assert!(outcome.is_error, "an oversized file must not be read");
    assert!(
        outcome.output.contains("the limit is"),
        "unexpected message: {}",
        outcome.output
    );
}
