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
    ToolOutcome, ToolRequest, TurnReply, TurnRequest, WORKFLOW_NAME,
};
use crate::tools;

/// The resolved workspace identity a session records.
fn workspace_id(workspace: &Path) -> String {
    workspace
        .canonicalize()
        .expect("the workspace resolves")
        .to_string_lossy()
        .into_owned()
}

/// The task every test submits. The stub model is what the tests register.
fn task(workspace: &Path) -> Value {
    task_on(workspace, claude::OFFLINE_MODEL)
}

/// A task recorded against a specific model identity.
fn task_on(workspace: &Path, model: &str) -> Value {
    serde_json::to_value(SessionTask {
        goal: "summarise the workspace".to_string(),
        max_turns: 6,
        approval_timeout_secs: 300,
        workspace: workspace_id(workspace),
        model: model.to_string(),
    })
    .expect("the task encodes")
}

/// One `run_tool` activity input, as the workflow would build it.
fn tool_request(workspace: &Path, tool: &str, input: Value) -> Value {
    serde_json::to_value(ToolRequest {
        workspace: workspace_id(workspace),
        call: ToolCall {
            id: "toolu_test".to_string(),
            name: tool.to_string(),
            input,
        },
    })
    .expect("the call encodes")
}

/// A stub model body that counts its calls.
///
/// It stands in for `claude::activity_body` with no API key, so it answers to
/// the same recorded identity.
fn counting_model(
    calls: Arc<AtomicUsize>,
) -> impl Fn(Value) -> Result<Value, String> + Send + Sync + 'static {
    move |input| {
        let request: TurnRequest =
            serde_json::from_value(input).map_err(|e| format!("bad request: {e}"))?;
        assert_eq!(
            request.model,
            claude::OFFLINE_MODEL,
            "a turn must carry the identity its session recorded"
        );
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
        .start_workflow(WORKFLOW_NAME, task(&workspace))
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
        .start_workflow(WORKFLOW_NAME, task(&workspace))
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
            .start_workflow(WORKFLOW_NAME, task(&workspace))
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
    let raw = body(tool_request(
        dir.path(),
        tools::TOOL_READ_FILE,
        json!({ "path": "../../etc/passwd" }),
    ))
    .expect("a tool failure is a result, not an activity error");
    let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
    assert!(outcome.is_error, "the escape must fail");
    assert!(
        outcome.output.contains("leaves the workspace"),
        "unexpected message: {}",
        outcome.output
    );
}

/// Poll one session over the socket until it leaves `RUNNING`, approving the
/// call it shows. Returns the terminal state it reached.
///
/// The decision names the call the status printed, and a decision naming
/// another call is asserted to be refused.
async fn settle_over_socket(socket: &Path, execution_id: &str) -> String {
    let mut approved = false;
    for _ in 0..200 {
        let answer = protocol::call(
            socket,
            &Request::Status {
                execution_id: execution_id.to_string(),
                full: false,
            },
        )
        .await
        .expect("the status is answered");
        let Response::Session { session } = answer else {
            panic!("unexpected answer: {answer:?}");
        };
        if session.state != "RUNNING" {
            assert!(approved, "the session never asked for approval");
            return session.state;
        }

        if !approved && session.blocked_on.is_some() {
            // An operator approves an action, not a session, so the exact call
            // must be visible before the decision.
            let pending = session.pending.as_ref().expect("the pending call is shown");
            assert_eq!(pending.tool, tools::TOOL_WRITE_FILE);
            assert!(
                pending.input.contains("agent-notes.md"),
                "the status must show what the write does: {}",
                pending.input
            );

            // A decision that does not name the call it saw is refused.
            let stale = protocol::call(
                socket,
                &Request::Approve {
                    execution_id: execution_id.to_string(),
                    call_id: "toolu_something_else".to_string(),
                    approved: true,
                    note: None,
                },
            )
            .await
            .expect("the daemon answers");
            assert!(
                matches!(stale, Response::Error { .. }),
                "a decision for another call must be refused: {stale:?}"
            );

            protocol::call(
                socket,
                &Request::Approve {
                    execution_id: execution_id.to_string(),
                    call_id: pending.id.clone(),
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
    panic!("the session never reached a terminal state");
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

    let final_state = settle_over_socket(&socket, &execution_id).await;

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
    // An audit trail must say what the agent did, not only which events ran.
    assert!(
        events
            .iter()
            .any(|event| event.contains("agent-notes.md") || event.contains("write_file")),
        "the event log does not record what the tools did: {events:?}"
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
        .start_workflow(WORKFLOW_NAME, task(&workspace))
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

    let body = tools::activity_body(workspace.clone());
    for path in ["link/secret.txt", "direct"] {
        let raw = body(tool_request(
            &workspace,
            tools::TOOL_READ_FILE,
            json!({ "path": path }),
        ))
        .expect("a tool failure is a result, not an activity error");
        let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
        assert!(outcome.is_error, "`{path}` must not resolve");
        assert!(
            !outcome.output.contains("classified"),
            "`{path}` leaked the file outside the workspace"
        );
    }

    // A write through the escaping link must not land outside either.
    let raw = body(tool_request(
        &workspace,
        tools::TOOL_WRITE_FILE,
        json!({ "path": "link/planted.txt", "content": "planted" }),
    ))
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
        .start_workflow(WORKFLOW_NAME, task(&workspace))
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
    let symlinked = dir.path().join("current.db");
    std::fs::write(&real, b"").expect("the database file is created");
    std::os::unix::fs::symlink(&real, &symlinked).expect("the symbolic link is created");

    // Two spellings of one file must take one lock, or two daemons write it.
    // The lock is held on the file itself, so both names reach it. A hard link
    // is refused outright instead — see
    // `a_hard_linked_database_is_refused`, because `SQLite` cannot share a
    // write-ahead log across two pathnames.
    let held = guard::acquire(&real).expect("the first daemon takes the lock");
    assert!(
        guard::acquire(&symlinked).is_err(),
        "an alias of a held database must not take a second lock"
    );

    drop(held);
    assert!(
        guard::acquire(&symlinked).is_ok(),
        "the lock must be free once its holder is gone"
    );
}

#[test]
fn the_toolbox_refuses_a_file_over_the_read_cap_without_reading_it() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().to_path_buf();
    let oversized = vec![b'x'; 70 * 1024];
    std::fs::write(workspace.join("big.txt"), &oversized).expect("the fixture is written");

    let body = tools::activity_body(workspace.clone());
    let raw = body(tool_request(
        &workspace,
        tools::TOOL_READ_FILE,
        json!({ "path": "big.txt" }),
    ))
    .expect("a tool failure is a result, not an activity error");
    let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");

    assert!(outcome.is_error, "an oversized file must not be read");
    assert!(
        outcome.output.contains("the limit is"),
        "unexpected message: {}",
        outcome.output
    );
}

#[tokio::test]
async fn a_session_refuses_to_run_in_another_workspace() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let theirs = dir.path().join("their-project");
    let ours = dir.path().join("our-project");
    std::fs::create_dir_all(&theirs).expect("the workspace is created");
    std::fs::create_dir_all(&ours).expect("the workspace is created");

    // The session belongs to one workspace; this daemon serves another. A
    // restart pointed elsewhere must not run the session's writes here.
    let calls = Arc::new(AtomicUsize::new(0));
    let mut rt = runtime(&dir.path().join("agentd.db"), &ours, &calls);
    let exec = rt
        .start_workflow(WORKFLOW_NAME, task(&theirs))
        .expect("the session starts");

    let state = rt.run_until_blocked(exec).await.expect("the run advances");
    let RunState::Failed(error) = state else {
        panic!("expected a terminal failure, got {state:?}");
    };
    assert!(
        error.contains("belongs to the workspace"),
        "unexpected failure: {error}"
    );
    assert!(
        !ours.join("agent-notes.md").exists(),
        "a session from another workspace must not write here"
    );
}

#[tokio::test]
async fn the_control_socket_is_private_and_never_deletes_another_file() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("a temporary directory");

    // A typo in `--socket` must not destroy the file it happens to name.
    let precious = dir.path().join("notes.txt");
    std::fs::write(&precious, "keep me").expect("the file is written");
    let refused = daemon::bind(&precious).await;
    assert!(refused.is_err(), "a regular file must not be bound over");
    assert_eq!(
        std::fs::read_to_string(&precious).expect("the file survives"),
        "keep me",
        "the file must not be deleted"
    );

    // Whoever can connect can spend money, so the socket is owner-only.
    let socket = dir.path().join("agentd.sock");
    let listener = daemon::bind(&socket).await.expect("the socket binds");
    let mode = std::fs::metadata(&socket)
        .expect("the socket exists")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600, "the control socket must be owner-only");
    drop(listener);
}

#[test]
fn a_zero_drive_interval_is_rejected() {
    use clap::Parser;

    // A zero period panics the timer, which would take the daemon down.
    assert!(
        crate::Cli::try_parse_from(["agentd", "serve", "--tick-ms", "0"]).is_err(),
        "a zero tick must be refused at the boundary"
    );
    assert!(
        crate::Cli::try_parse_from(["agentd", "serve", "--tick-ms", "1"]).is_ok(),
        "one millisecond is a usable period"
    );
}

#[tokio::test]
async fn the_full_view_shows_a_write_that_the_status_trims() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");

    // A write may carry up to 64 KiB, and an operator approves the WHOLE of it.
    let content = "x".repeat(5000);
    let tail = "THE-END-OF-THE-PAYLOAD";
    let payload = format!("{content}{tail}");

    let mut rt = SqliteRuntime::open(dir.path().join("agentd.db")).expect("the database opens");
    rt.register_workflow(&session::agent_session_info());
    let written = payload.clone();
    rt.register_activity(&session::claude_turn_info(), move |_input| {
        let input = json!({ "path": "notes.md", "content": written });
        serde_json::to_value(TurnReply {
            content: json!([
                { "type": "tool_use", "id": "toolu_big", "name": tools::TOOL_WRITE_FILE, "input": input },
            ]),
            stop_reason: "tool_use".to_string(),
            text: String::new(),
            tool_calls: vec![ToolCall {
                id: "toolu_big".to_string(),
                name: tools::TOOL_WRITE_FILE.to_string(),
                input: json!({ "path": "notes.md", "content": payload }),
            }],
        })
        .map_err(|e| format!("bad reply: {e}"))
    });
    rt.register_activity(
        &session::run_tool_info(),
        tools::activity_body(workspace.clone()),
    );

    let exec = rt
        .start_workflow(WORKFLOW_NAME, task(&workspace))
        .expect("the session starts");
    let signal = drive_to_approval(&mut rt, exec).await;

    let trimmed = daemon::pending_call(&rt, exec, &signal, false).expect("the status shows a call");
    assert!(
        trimmed.input.contains("truncated"),
        "the status must say when it has trimmed the payload"
    );
    assert!(
        !trimmed.input.contains(tail),
        "the trimmed view cannot hold the whole payload"
    );

    let whole = daemon::pending_call(&rt, exec, &signal, true).expect("the full view shows a call");
    assert!(
        whole.input.contains(tail),
        "the full view must show every byte an approval authorises"
    );
    assert!(
        !whole.input.contains("truncated"),
        "the full view must not be trimmed"
    );
}

#[test]
fn only_an_accepted_request_is_refused_a_retry() {
    use autumn_harvest::failure::parse_error_payload_full;
    use reqwest::StatusCode;

    // A body that fails after a 2xx means the turn was billed. Retrying buys it
    // twice, so that case is terminal.
    let accepted = parse_error_payload_full(&claude::body_failure(StatusCode::OK, "it went away"));
    assert!(
        accepted.non_retryable,
        "a lost response to an accepted request must not be retried"
    );

    // A rate limit produced no turn, so it must still back off and retry.
    for status in [StatusCode::TOO_MANY_REQUESTS, StatusCode::BAD_GATEWAY] {
        let transient = parse_error_payload_full(&claude::body_failure(status, "it went away"));
        assert!(
            !transient.non_retryable,
            "{status} must stay retryable even when its body is unreadable"
        );
    }

    // A rejected request fails the same way whether or not its body read.
    let rejected = parse_error_payload_full(&claude::body_failure(StatusCode::BAD_REQUEST, "gone"));
    assert!(
        rejected.non_retryable,
        "a rejected request must not be retried"
    );
}

#[tokio::test]
async fn a_session_refuses_to_continue_on_another_model() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");

    // The session was started on the offline stub; this daemon serves a real
    // model. Continuing would move a conversation onto another model, and an
    // offline session onto billed calls.
    let model = claude::ModelConfig::new(
        Some("sk-ant-not-a-real-key".to_string()),
        claude::DEFAULT_MODEL.to_string(),
        claude::DEFAULT_MAX_TOKENS,
    )
    .expect("the configuration builds");

    let mut rt = SqliteRuntime::open(dir.path().join("agentd.db")).expect("the database opens");
    rt.register_workflow(&session::agent_session_info());
    rt.register_activity(&session::claude_turn_info(), claude::activity_body(model));
    rt.register_activity(
        &session::run_tool_info(),
        tools::activity_body(workspace.clone()),
    );

    let exec = rt
        .start_workflow(WORKFLOW_NAME, task_on(&workspace, claude::OFFLINE_MODEL))
        .expect("the session starts");
    let state = rt.run_until_blocked(exec).await.expect("the run advances");

    let RunState::Failed(error) = state else {
        panic!("expected a terminal failure, got {state:?}");
    };
    assert!(
        error.contains("this session runs on"),
        "unexpected failure: {error}"
    );
}

#[test]
fn a_billed_response_that_is_not_a_message_is_refused() {
    use autumn_harvest::failure::parse_error_payload_full;

    // Valid JSON is not yet a message. Without the shape check these fall
    // through every default and record a clean, empty `end_turn`.
    for malformed in [
        json!({}),
        json!({ "content": [] }),
        json!({ "stop_reason": "end_turn" }),
        json!({ "content": "not an array", "stop_reason": "end_turn" }),
    ] {
        assert!(
            !claude::is_message(&malformed),
            "{malformed} must not pass as a message"
        );
    }

    let message = json!({
        "content": [{ "type": "text", "text": "hello" }],
        "stop_reason": "end_turn",
    });
    assert!(claude::is_message(&message), "a real message must pass");

    // A malformed body from an accepted request is terminal, like the others.
    let refused = parse_error_payload_full(&claude::body_failure(
        reqwest::StatusCode::OK,
        "its response was not a message",
    ));
    assert!(refused.non_retryable, "a billed malformed body is terminal");
}

#[test]
fn a_new_database_and_its_sidecars_are_owner_only() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("agentd.db");

    // The database holds every prompt and every tool result, so it is at least
    // as sensitive as the control socket.
    let held = guard::acquire(&db).expect("the lock is taken");
    let mode = std::fs::metadata(&db)
        .expect("the database exists")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600, "a new database must be owner-only");
    drop(held);

    // The `-wal` and `-shm` sidecars carry the same data, and `SQLite` creates
    // them itself, so the mask is what makes them private.
    let runtime = guard::with_private_umask(|| SqliteRuntime::open(&db));
    drop(runtime.expect("the runtime opens"));
    for sidecar in ["agentd.db-wal", "agentd.db-shm"] {
        let path = dir.path().join(sidecar);
        if let Ok(meta) = std::fs::metadata(&path) {
            assert_eq!(
                meta.permissions().mode() & 0o777,
                0o600,
                "{sidecar} must be owner-only"
            );
        }
    }
}

#[test]
fn the_toolbox_refuses_a_named_pipe_without_blocking_on_it() {
    use std::ffi::CString;

    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().to_path_buf();
    let fifo = workspace.join("pipe");

    let path = CString::new(fifo.to_string_lossy().as_bytes()).expect("a C path");
    // SAFETY: `path` is a valid, NUL-terminated C string that lives across the
    // call, and `mkfifo` only reads it.
    let made = unsafe { libc::mkfifo(path.as_ptr(), 0o600) };
    assert_eq!(made, 0, "the test fixture needs a FIFO");

    // A FIFO with no writer blocks a plain open. One runtime serves every
    // session, so a blocked body would wedge the whole daemon.
    let body = tools::activity_body(workspace.clone());
    for tool in [tools::TOOL_READ_FILE, tools::TOOL_WRITE_FILE] {
        let raw = body(tool_request(
            &workspace,
            tool,
            json!({ "path": "pipe", "content": "x" }),
        ))
        .expect("a tool failure is a result, not an activity error");
        let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
        assert!(outcome.is_error, "{tool} must refuse a FIFO");
        assert!(
            outcome.output.contains("not an ordinary file"),
            "unexpected message from {tool}: {}",
            outcome.output
        );
    }
}

#[test]
fn a_write_replaces_its_target_atomically() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().to_path_buf();
    std::fs::write(workspace.join("notes.md"), "the previous content")
        .expect("the fixture is written");

    let body = tools::activity_body(workspace.clone());
    let raw = body(tool_request(
        &workspace,
        tools::TOOL_WRITE_FILE,
        json!({ "path": "notes.md", "content": "the approved content" }),
    ))
    .expect("a tool failure is a result, not an activity error");
    let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
    assert!(
        !outcome.is_error,
        "the write must succeed: {}",
        outcome.output
    );

    assert_eq!(
        std::fs::read_to_string(workspace.join("notes.md")).expect("the target exists"),
        "the approved content"
    );

    // The scratch file is renamed, never left behind.
    let leftovers: Vec<_> = std::fs::read_dir(&workspace)
        .expect("the workspace lists")
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains("agentd-tmp"))
        .collect();
    assert!(leftovers.is_empty(), "scratch files remain: {leftovers:?}");
}

#[test]
fn a_turn_that_says_nothing_is_not_an_answer() {
    // A malformed content block, or an empty `content`, reaches the projection
    // as a clean `end_turn` with no text. Reporting that as a finished session
    // would present a billed non-answer as work.
    let empty = TurnReply {
        content: json!([]),
        stop_reason: "end_turn".to_string(),
        text: String::new(),
        tool_calls: Vec::new(),
    };
    assert!(
        !claude::is_usable(&empty),
        "an empty end_turn is not usable"
    );

    let spoken = TurnReply {
        text: "here is the summary".to_string(),
        ..empty.clone()
    };
    assert!(claude::is_usable(&spoken), "text makes a turn usable");

    let calling = TurnReply {
        stop_reason: "tool_use".to_string(),
        tool_calls: vec![ToolCall {
            id: "toolu_x".to_string(),
            name: tools::TOOL_READ_FILE.to_string(),
            input: json!({ "path": "." }),
        }],
        ..empty.clone()
    };
    assert!(
        claude::is_usable(&calling),
        "a tool call makes a turn usable"
    );

    // A stop reason that speaks for itself needs no content.
    let refused = TurnReply {
        stop_reason: "refusal".to_string(),
        ..empty.clone()
    };
    assert!(
        claude::is_usable(&refused),
        "a refusal reports itself and must not be re-classified"
    );

    // A turn that stopped TO CALL A TOOL must carry one. Otherwise the loop
    // takes its no-tool-calls branch and reports a finished session.
    let promised = TurnReply {
        content: json!([null]),
        stop_reason: "tool_use".to_string(),
        ..empty
    };
    assert!(
        !claude::is_usable(&promised),
        "a `tool_use` turn with no call is not usable"
    );
}

#[test]
fn a_hard_linked_database_is_refused() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let real = dir.path().join("real.db");
    let alias = dir.path().join("alias.db");
    std::fs::write(&real, b"").expect("the database file is created");
    std::fs::hard_link(&real, &alias).expect("the hard link is created");

    // `SQLite` derives its write-ahead log from the PATH. After an unclean
    // exit, opening `alias.db` reads `alias.db-wal` and never sees what
    // `real.db-wal` holds, so committed sessions become invisible.
    for name in [&real, &alias] {
        let refused = guard::acquire(name);
        let message = refused.err().unwrap_or_else(|| {
            panic!(
                "{} must be refused while it is multiply linked",
                name.display()
            )
        });
        assert!(
            message.contains("hard links"),
            "unexpected message: {message}"
        );
    }

    // One name again, and it opens.
    std::fs::remove_file(&alias).expect("the link is removed");
    assert!(
        guard::acquire(&real).is_ok(),
        "a single-named database must open"
    );
}

#[test]
fn a_turn_whose_tool_calls_share_an_id_is_refused() {
    let call = |id: &str| ToolCall {
        id: id.to_string(),
        name: tools::TOOL_WRITE_FILE.to_string(),
        input: json!({ "path": "notes.md", "content": "x" }),
    };
    let reply = |calls: Vec<ToolCall>| TurnReply {
        content: json!([]),
        stop_reason: "tool_use".to_string(),
        text: String::new(),
        tool_calls: calls,
    };

    // An approval is addressed by id, so a repeated one would let a single
    // decision release a call the operator never read.
    assert!(
        !claude::has_addressable_calls(&reply(vec![call("toolu_a"), call("toolu_a")])),
        "a repeated id must be refused"
    );
    assert!(
        !claude::has_addressable_calls(&reply(vec![call("")])),
        "a blank id must be refused"
    );
    assert!(
        claude::has_addressable_calls(&reply(vec![call("toolu_a"), call("toolu_b")])),
        "distinct ids are addressable"
    );
    assert!(
        claude::has_addressable_calls(&reply(Vec::new())),
        "a turn with no tool calls has nothing to address"
    );
}

#[test]
fn a_write_keeps_the_mode_of_the_file_it_replaces() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().to_path_buf();
    let secret = workspace.join("secret.txt");
    std::fs::write(&secret, "old").expect("the fixture is written");
    std::fs::set_permissions(&secret, std::fs::Permissions::from_mode(0o600))
        .expect("the fixture is made private");

    let body = tools::activity_body(workspace.clone());
    let raw = body(tool_request(
        &workspace,
        tools::TOOL_WRITE_FILE,
        json!({ "path": "secret.txt", "content": "new" }),
    ))
    .expect("a tool failure is a result, not an activity error");
    let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
    assert!(
        !outcome.is_error,
        "the write must succeed: {}",
        outcome.output
    );

    // A content change is not a permission change.
    let mode = std::fs::metadata(&secret)
        .expect("the target exists")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(
        mode, 0o600,
        "the target's mode must survive the replacement"
    );

    // A file the agent brings into being starts private.
    let raw = body(tool_request(
        &workspace,
        tools::TOOL_WRITE_FILE,
        json!({ "path": "fresh.txt", "content": "new" }),
    ))
    .expect("a tool failure is a result, not an activity error");
    let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
    assert!(
        !outcome.is_error,
        "the write must succeed: {}",
        outcome.output
    );
    let mode = std::fs::metadata(workspace.join("fresh.txt"))
        .expect("the new file exists")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600, "a new file must be owner-only");
}

#[test]
fn a_write_keeps_a_group_readable_mode_the_umask_would_strip() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().to_path_buf();
    let shared = workspace.join("shared.txt");
    std::fs::write(&shared, "old").expect("the fixture is written");
    std::fs::set_permissions(&shared, std::fs::Permissions::from_mode(0o660))
        .expect("the fixture is made group-writable");

    // A mode passed to `open` is filtered through the umask, so `0660` would
    // come back `0640` under the common one. The mode is applied to the
    // descriptor instead, where nothing filters it.
    let body = tools::activity_body(workspace.clone());
    let raw = body(tool_request(
        &workspace,
        tools::TOOL_WRITE_FILE,
        json!({ "path": "shared.txt", "content": "new" }),
    ))
    .expect("a tool failure is a result, not an activity error");
    let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
    assert!(
        !outcome.is_error,
        "the write must succeed: {}",
        outcome.output
    );

    let mode = std::fs::metadata(&shared)
        .expect("the target exists")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(
        mode, 0o660,
        "the target's mode must survive the replacement"
    );
}

#[test]
fn a_write_never_removes_a_file_that_occupies_a_scratch_name() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().to_path_buf();

    // Whatever sits on a scratch name may be residue someone wants to read.
    // The write picks another name rather than deleting it.
    let squatter = workspace.join(format!(".notes.md.agentd-{}-0.tmp", std::process::id()));
    std::fs::write(&squatter, "do not delete me").expect("the fixture is written");

    let body = tools::activity_body(workspace.clone());
    let raw = body(tool_request(
        &workspace,
        tools::TOOL_WRITE_FILE,
        json!({ "path": "notes.md", "content": "the approved content" }),
    ))
    .expect("a tool failure is a result, not an activity error");
    let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
    assert!(
        !outcome.is_error,
        "the write must succeed: {}",
        outcome.output
    );

    assert_eq!(
        std::fs::read_to_string(&squatter).expect("the occupant survives"),
        "do not delete me"
    );
    assert_eq!(
        std::fs::read_to_string(workspace.join("notes.md")).expect("the target exists"),
        "the approved content"
    );
}

#[tokio::test]
async fn a_decision_can_only_be_delivered_once() {
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

    // Wait for the gate, then send the SAME decision twice in a row. The
    // second must be refused: two staged signals would leave one queued for a
    // later call to consume without being shown.
    let mut call_id = None;
    for _ in 0..200 {
        let answer = protocol::call(
            &socket,
            &Request::Status {
                execution_id: execution_id.clone(),
                full: false,
            },
        )
        .await
        .expect("the status is answered");
        let Response::Session { session } = answer else {
            panic!("unexpected answer: {answer:?}");
        };
        if let Some(pending) = session.pending {
            call_id = Some(pending.id);
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let call_id = call_id.expect("the session never asked for approval");

    let decision = |id: String| Request::Approve {
        execution_id: execution_id.clone(),
        call_id: id,
        approved: true,
        note: None,
    };
    let first = protocol::call(&socket, &decision(call_id.clone()))
        .await
        .expect("the first decision is answered");
    assert!(
        matches!(first, Response::Ack { .. }),
        "the first decision must be accepted: {first:?}"
    );
    let second = protocol::call(&socket, &decision(call_id))
        .await
        .expect("the second decision is answered");
    assert!(
        matches!(second, Response::Error { .. }),
        "a repeated decision must be refused: {second:?}"
    );

    daemon.abort();
}

#[tokio::test]
async fn a_daemon_refuses_to_start_where_a_session_does_not_belong() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("agentd.db");
    let theirs = dir.path().join("their-project");
    let ours = dir.path().join("our-project");
    std::fs::create_dir_all(&theirs).expect("the workspace is created");
    std::fs::create_dir_all(&ours).expect("the workspace is created");

    // A session parked mid-run, recorded against one workspace.
    let calls = Arc::new(AtomicUsize::new(0));
    let exec = {
        let mut rt = runtime(&db, &theirs, &calls);
        let exec = rt
            .start_workflow(WORKFLOW_NAME, task(&theirs))
            .expect("the session starts");
        drive_to_approval(&mut rt, exec).await;
        exec
    };

    // Starting on another workspace must be refused BEFORE anything drives the
    // session. A mismatched tool call fails the run non-retryably, and only
    // `RUNNING` rows are ever driven, so a failure here could never be undone.
    let refused = daemon::serve(daemon::Options {
        db: db.clone(),
        socket: dir.path().join("agentd.sock"),
        workspace: ours,
        model: claude::DEFAULT_MODEL.to_string(),
        max_tokens: claude::DEFAULT_MAX_TOKENS,
        tick: Duration::from_millis(50),
        api_key: None,
    })
    .await;
    let message = refused.expect_err("the daemon must refuse to start");
    assert!(
        message.contains("belongs to the workspace"),
        "unexpected message: {message}"
    );

    // The session is untouched, so the operator can fix the flag and resume.
    let mut rt = runtime(&db, &theirs, &calls);
    assert!(
        matches!(
            rt.outcome(exec),
            Ok(autumn_harvest_sqlite::ExecutionOutcome::Running)
        ),
        "the session must stay resumable"
    );
    let signal = drive_to_approval(&mut rt, exec).await;
    approve(&mut rt, exec, &signal);
    let state = rt.run_until_blocked(exec).await.expect("the run finishes");
    assert!(
        matches!(state, RunState::Completed(_)),
        "the session must still complete, got {state:?}"
    );
}

#[tokio::test]
async fn a_workspace_path_that_cannot_be_written_down_is_refused() {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    let dir = tempfile::tempdir().expect("a temporary directory");

    // A path that is not valid UTF-8 cannot be recorded exactly. A lossy name
    // would never match the real path again, so every tool call in the session
    // would fail.
    let mut raw = OsString::from_vec(b"workspace-\xff".to_vec());
    let workspace = dir.path().join(&mut raw);
    std::fs::create_dir_all(&workspace).expect("the workspace is created");

    let refused = daemon::serve(daemon::Options {
        db: dir.path().join("agentd.db"),
        socket: dir.path().join("agentd.sock"),
        workspace,
        model: claude::DEFAULT_MODEL.to_string(),
        max_tokens: claude::DEFAULT_MAX_TOKENS,
        tick: Duration::from_millis(50),
        api_key: None,
    })
    .await;
    let message = refused.expect_err("the daemon must refuse the path");
    assert!(
        message.contains("not valid UTF-8"),
        "unexpected message: {message}"
    );
}
