//! Offline tests for the agent daemon.
//!
//! Every test runs the scripted stub model, so the suite needs no API key and
//! no network. The stub drives the same loop the live model does: one tool
//! call, one approval-gated write, then a final answer.

use std::path::{Path, PathBuf};
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

/// Wait for a daemon to answer on its socket.
///
/// The path existing is not enough. A dead daemon leaves its socket file
/// behind on purpose, so the test has to wait for an answer rather than for a
/// name.
async fn await_daemon(socket: &Path) {
    for _ in 0..200 {
        if let Ok(Response::Sessions { .. }) = protocol::call(socket, &Request::List).await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the daemon never answered on its socket");
}

/// Wait for a session to park on its approval.
async fn await_parked(socket: &Path, execution_id: &str) {
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
        if session.blocked_on.is_some() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the session never parked");
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

            // A decision that does not name the wait it saw is refused. The
            // bare tool-use id is exactly what must NOT be enough: the model
            // can reuse one, so an older status would release a later call.
            let stale = protocol::call(
                socket,
                &Request::Approve {
                    execution_id: execution_id.to_string(),
                    token: pending.id.clone(),
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
                    token: pending.token.clone(),
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
async fn a_second_daemon_resumes_a_session_the_first_one_left_running() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    // One database, one socket each. Both daemons run in THIS process, and the
    // accept loop of an aborted one keeps its listener, which a killed process
    // would not. The socket reclaim has its own test; this one is about the
    // session surviving in the file.
    let first_socket = dir.path().join("first.sock");
    let second_socket = dir.path().join("second.sock");
    let options = |socket: &Path| daemon::Options {
        db: dir.path().join("agentd.db"),
        socket: socket.to_path_buf(),
        workspace: dir.path().join("workspace"),
        model: claude::DEFAULT_MODEL.to_string(),
        max_tokens: claude::DEFAULT_MAX_TOKENS,
        tick: Duration::from_millis(50),
        api_key: None,
    };

    let first = tokio::spawn(daemon::serve(options(&first_socket)));
    await_daemon(&first_socket).await;
    let submitted = protocol::call(
        &first_socket,
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
    await_parked(&first_socket, &execution_id).await;

    // The process dies with the session parked. Aborting the task drops the
    // listener and the database lock, which is what a kill does.
    first.abort();
    drop(first.await);

    // The second daemon holds no memory of the session. It has to find the
    // session in the file, or nothing advances it again: this daemon is the
    // only writer.
    let second = tokio::spawn(daemon::serve(options(&second_socket)));
    await_daemon(&second_socket).await;
    let state = settle_over_socket(&second_socket, &execution_id).await;
    assert_eq!(
        state, "COMPLETED",
        "the resumed session did not finish under the second daemon"
    );
    second.abort();
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

    // A well-formed id that names no session is an error, not an empty log. A
    // mistyped audit target must not read as a session that did nothing.
    let missing = protocol::call(
        &socket,
        &Request::History {
            execution_id: "00000000-0000-4000-8000-000000000000".to_string(),
        },
    )
    .await
    .expect("the history is answered");
    let Response::Error { message } = missing else {
        panic!("an unknown session must be refused, got {missing:?}");
    };
    assert!(
        message.contains("no session"),
        "the refusal must name the missing session: {message}"
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

    // An empty stop reason is not a stop reason. It also differs from
    // `end_turn`, so the usability test would accept it, and the loop would
    // record a completed session with a blank stop reason.
    let blank = json!({ "content": [Value::Null], "stop_reason": "" });
    assert!(
        !claude::is_message(&blank),
        "a blank stop reason must be refused"
    );

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
fn a_write_flushes_the_whole_chain_below_the_workspace() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");
    let root = workspace.canonicalize().expect("the workspace resolves");
    let target = root.join("notes/day/1/log.md");
    let chain = vec![
        root.join("notes/day/1"),
        root.join("notes/day"),
        root.join("notes"),
        root.clone(),
    ];

    // Each directory between the target and the root can hold an entry this
    // write created. An entry is durable only once its own directory is
    // flushed.
    assert_eq!(
        tools::directories_to_flush(&target, &workspace),
        chain,
        "the flush must run from the target's directory up to the root"
    );

    // The same list after the directories already exist. Activity execution is
    // at-least-once, so a retry can find the directories a crashed attempt
    // created and never flushed. A list built from this attempt's own
    // creations would name nothing.
    std::fs::create_dir_all(target.parent().expect("a parent")).expect("the chain exists");
    assert_eq!(
        tools::directories_to_flush(&target, &workspace),
        chain,
        "a retry must flush the chain it did not create"
    );

    // A target in the root itself flushes the root, and nothing above it.
    assert_eq!(
        tools::directories_to_flush(&root.join("notes.md"), &workspace),
        vec![root.clone()],
        "the walk must stop at the workspace"
    );

    // The real path still writes the nested file and lands the content.
    let body = tools::activity_body(workspace.clone());
    let raw = body(tool_request(
        &workspace,
        tools::TOOL_WRITE_FILE,
        json!({ "path": "deep/er/still/notes.md", "content": "hello" }),
    ))
    .expect("a tool failure is a result, not an activity error");
    let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
    assert!(
        !outcome.is_error,
        "the nested write must succeed: {}",
        outcome.output
    );
    assert_eq!(
        std::fs::read_to_string(root.join("deep/er/still/notes.md")).expect("the file exists"),
        "hello"
    );
}

#[test]
fn a_turn_that_ends_and_still_asks_for_a_tool_is_refused() {
    let reply = |stop: &str, calls: Vec<ToolCall>| TurnReply {
        content: json!([]),
        stop_reason: stop.to_string(),
        text: String::new(),
        tool_calls: calls,
    };
    let call = vec![ToolCall {
        id: "toolu_a".to_string(),
        name: tools::TOOL_WRITE_FILE.to_string(),
        input: json!({ "path": "notes.md", "content": "x" }),
    }];

    // A turn cannot both end and ask for a tool. Running the call and then
    // reporting a clean finish, or dropping it and reporting one, both present
    // a malformed billed response as a finished session.
    assert!(
        !claude::agrees_with_its_content(&reply(claude::STOP_END_TURN, call.clone())),
        "`end_turn` with a tool call must be refused"
    );
    assert!(
        claude::agrees_with_its_content(&reply(claude::STOP_TOOL_USE, call.clone())),
        "a tool call under `tool_use` is the ordinary case"
    );
    assert!(
        claude::agrees_with_its_content(&reply(claude::STOP_END_TURN, Vec::new())),
        "a finished turn with no tool call is consistent"
    );

    // A truncated turn can carry a partial block. The loop drops it unrun and
    // reports `max_tokens`, so this must stay a report and not become a
    // refusal.
    assert!(
        claude::agrees_with_its_content(&reply("max_tokens", call)),
        "a truncated turn must still report rather than fail"
    );
}

#[tokio::test]
async fn a_tool_call_under_an_unknown_stop_reason_is_not_run() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");

    // A stop reason this example does not know about, carrying a write. The
    // pair is not a contradiction, so the turn is not refused. The call is
    // still not what the stop reason asked for, so it must not run.
    let mut rt = SqliteRuntime::open(dir.path().join("agentd.db")).expect("the database opens");
    rt.register_workflow(&session::agent_session_info());
    rt.register_activity(&session::claude_turn_info(), |_input| {
        serde_json::to_value(TurnReply {
            content: json!([]),
            stop_reason: "pause_turn".to_string(),
            text: "thinking".to_string(),
            tool_calls: vec![ToolCall {
                id: "toolu_paused".to_string(),
                name: tools::TOOL_WRITE_FILE.to_string(),
                input: json!({ "path": "unasked.md", "content": "never" }),
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
    let state = rt.run_until_blocked(exec).await.expect("the run finishes");
    let RunState::Completed(output) = state else {
        panic!("expected a terminal report, got {state:?}");
    };

    let report: SessionReport = serde_json::from_value(output).expect("the report decodes");
    assert_eq!(
        report.stop, "pause_turn",
        "the session must end under the stop reason it was given"
    );
    assert_eq!(report.tool_calls, 0, "the unasked call must not run");
    assert!(
        !workspace.join("unasked.md").exists(),
        "the unasked write must not land"
    );
}

#[test]
fn the_toolbox_stops_listing_a_directory_at_the_cap() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");

    // One entry over the cap is enough to prove the read stops. The tool bodies
    // run on the one runtime, so naming a huge directory in full would block
    // every session and every control command.
    for index in 0..=tools::MAX_ENTRIES {
        std::fs::write(workspace.join(format!("file-{index:05}.txt")), "x")
            .expect("the fixture is written");
    }

    let body = tools::activity_body(workspace.clone());
    let listed = |input: Value| -> String {
        let raw = body(input).expect("a tool failure is a result, not an activity error");
        let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
        assert!(
            !outcome.is_error,
            "the listing must succeed: {}",
            outcome.output
        );
        outcome.output
    };

    let output = listed(tool_request(
        &workspace,
        tools::TOOL_LIST_FILES,
        json!({ "path": "." }),
    ));
    let lines: Vec<&str> = output.lines().collect();
    assert_eq!(
        lines.len(),
        tools::MAX_ENTRIES + 1,
        "the listing must carry the cap and one marker"
    );
    let marker = lines.last().expect("the marker is present");
    assert!(
        marker.starts_with("... more entries"),
        "the truncation must be reported: {marker}"
    );

    // A directory inside the cap is listed whole, sorted, with no marker.
    let small = workspace.join("small");
    std::fs::create_dir(&small).expect("the directory is created");
    std::fs::write(small.join("b.txt"), "x").expect("the fixture is written");
    std::fs::write(small.join("a.txt"), "x").expect("the fixture is written");
    assert_eq!(
        listed(tool_request(
            &workspace,
            tools::TOOL_LIST_FILES,
            json!({ "path": "small" }),
        )),
        "a.txt\nb.txt",
        "a small directory must be listed whole and sorted"
    );
}

#[tokio::test]
async fn the_drive_query_reads_only_the_running_sessions() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");
    let db = dir.path().join("agentd.db");

    let calls = Arc::new(AtomicUsize::new(0));
    let mut rt = runtime(&db, &workspace, &calls);

    // One session driven to completion, and one parked on its approval.
    let done = rt
        .start_workflow(WORKFLOW_NAME, task(&workspace))
        .expect("the session starts");
    let signal = drive_to_approval(&mut rt, done).await;
    approve(&mut rt, done, &signal);
    let state = rt.run_until_blocked(done).await.expect("the run finishes");
    assert!(
        matches!(state, RunState::Completed(_)),
        "expected completion, got {state:?}"
    );

    let parked = rt
        .start_workflow(WORKFLOW_NAME, task(&workspace))
        .expect("the second session starts");
    drive_to_approval(&mut rt, parked).await;

    // The tick polls several times a second, so it must not read the rows it
    // cannot act on.
    let reader = crate::inspect::open(&db).expect("the inspector opens");
    let running = crate::inspect::running(&reader, WORKFLOW_NAME).expect("the drive query answers");
    assert_eq!(
        running,
        vec![parked.to_string()],
        "only the parked session is drivable"
    );
    assert_eq!(
        crate::inspect::executions(&reader, WORKFLOW_NAME)
            .expect("the listing answers")
            .len(),
        2,
        "both sessions are still listed for the operator"
    );
}

#[test]
fn a_write_lands_on_a_name_at_the_component_limit() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");

    // A 255-byte name is legal on the common filesystems. A scratch name that
    // copied it whole would be 20 bytes longer. Every attempt then failed with
    // `ENAMETOOLONG`, and the approved write could not land at all.
    let long = "n".repeat(255);
    // A name of multi-byte characters, to prove the cut lands on a boundary.
    let wide = "é".repeat(120);

    let body = tools::activity_body(workspace.clone());
    for name in [long.as_str(), wide.as_str()] {
        let raw = body(tool_request(
            &workspace,
            tools::TOOL_WRITE_FILE,
            json!({ "path": name, "content": "landed" }),
        ))
        .expect("a tool failure is a result, not an activity error");
        let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
        assert!(
            !outcome.is_error,
            "the write must land on a {}-byte name: {}",
            name.len(),
            outcome.output
        );
        assert_eq!(
            std::fs::read_to_string(workspace.join(name)).expect("the file exists"),
            "landed"
        );
    }

    // The scratch name fits whatever the target does, and a short target keeps
    // its whole stem so a leftover file stays identifiable.
    for name in [long.as_str(), wide.as_str(), "notes.md"] {
        let scratch = tools::scratch_name(name, 4_294_967_295, 15);
        assert!(
            scratch.len() <= name.len().max(64),
            "`{scratch}` is longer than the name it replaces"
        );
        assert!(scratch.len() <= 255, "`{scratch}` is over the limit");
    }
    assert!(
        tools::scratch_name("notes.md", 123, 0).contains("notes.md"),
        "a short target must keep its stem"
    );
}

#[test]
fn a_live_daemon_refuses_the_offline_identity() {
    // A key plus the stub's own name would make `identity` match a session
    // recorded offline. The restart would send that transcript to the API.
    // `expect_err` is not available here on purpose: `ModelConfig` holds the
    // API key, so it does not implement `Debug`.
    let Err(refusal) = claude::ModelConfig::new(
        Some("sk-not-a-real-key".to_string()),
        claude::OFFLINE_MODEL.to_string(),
        claude::DEFAULT_MAX_TOKENS,
    ) else {
        panic!("the stub's name must not be accepted as a model");
    };
    assert!(
        refusal.contains("would leave this machine"),
        "the refusal must say what is at stake: {refusal}"
    );

    // Without a key the name is what the daemon records anyway, so it is no
    // error. A real model with a key is the ordinary case.
    let offline = claude::ModelConfig::new(
        None,
        claude::OFFLINE_MODEL.to_string(),
        claude::DEFAULT_MAX_TOKENS,
    )
    .expect("the stub needs no key");
    assert_eq!(offline.identity(), claude::OFFLINE_MODEL);

    let live = claude::ModelConfig::new(
        Some("sk-not-a-real-key".to_string()),
        claude::DEFAULT_MODEL.to_string(),
        claude::DEFAULT_MAX_TOKENS,
    )
    .expect("a real model with a key is ordinary");
    assert_eq!(live.identity(), claude::DEFAULT_MODEL);
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
    let mut token = None;
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
            token = Some(pending.token);
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let token = token.expect("the session never asked for approval");

    let decision = |token: String| Request::Approve {
        execution_id: execution_id.clone(),
        token,
        approved: true,
        note: None,
    };
    let first = protocol::call(&socket, &decision(token.clone()))
        .await
        .expect("the first decision is answered");
    assert!(
        matches!(first, Response::Ack { .. }),
        "the first decision must be accepted: {first:?}"
    );
    let second = protocol::call(&socket, &decision(token))
        .await
        .expect("the second decision is answered");
    assert!(
        matches!(second, Response::Error { .. }),
        "a repeated decision must be refused: {second:?}"
    );

    daemon.abort();
}

#[tokio::test]
async fn a_daemon_flushes_the_path_above_its_workspace() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let root = dir.path().canonicalize().expect("the directory resolves");
    let workspace = root.join("projects/agent/workspace");

    // The write path flushes from a target up to the workspace root. The entry
    // that NAMES the root lives above it, so the chain above is the daemon's
    // to flush at startup.
    let chain = daemon::path_above(&workspace);
    assert_eq!(
        chain.get(..3),
        Some(
            &[
                root.join("projects/agent"),
                root.join("projects"),
                root.clone()
            ][..]
        ),
        "the chain must start in the directory just above the workspace"
    );
    assert_eq!(
        chain.last().map(PathBuf::as_path),
        Some(Path::new("/")),
        "the chain must end at the filesystem root"
    );
    assert!(
        !chain.contains(&workspace),
        "the workspace itself is flushed by every write, not here"
    );

    // A workspace several levels deep is created and served. The startup flush
    // must not stop that.
    let options = daemon::Options {
        db: root.join("agentd.db"),
        socket: root.join("agentd.sock"),
        workspace: workspace.clone(),
        model: claude::DEFAULT_MODEL.to_string(),
        max_tokens: claude::DEFAULT_MAX_TOKENS,
        tick: Duration::from_millis(50),
        api_key: None,
    };
    let daemon = tokio::spawn(daemon::serve(options));
    await_daemon(&root.join("agentd.sock")).await;
    assert!(workspace.is_dir(), "the workspace must exist");
    daemon.abort();
}

#[tokio::test]
async fn a_daemon_refuses_a_database_the_agent_could_write() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");

    // `--workspace .` with the default database name is the natural way into
    // this. The model can write any path in the workspace, and a write
    // replaces its target. The daemon would then run from a file that one
    // approved tool call could destroy.
    let inside = daemon::Options {
        db: workspace.join("agentd.db"),
        socket: dir.path().join("agentd.sock"),
        workspace: workspace.clone(),
        model: claude::DEFAULT_MODEL.to_string(),
        max_tokens: claude::DEFAULT_MAX_TOKENS,
        tick: Duration::from_millis(50),
        api_key: None,
    };
    // A timeout, because a daemon that does NOT refuse runs until `Ctrl-C`.
    // Without it a regression would hang the suite instead of failing it.
    let refusal = tokio::time::timeout(Duration::from_secs(10), daemon::serve(inside))
        .await
        .expect("the daemon must refuse rather than start")
        .expect_err("a database inside the workspace must be refused");
    assert!(
        refusal.contains("inside the workspace"),
        "the refusal must name the problem: {refusal}"
    );
    assert!(
        !dir.path().join("agentd.sock").exists(),
        "a refused daemon must not bind its socket"
    );

    // A directory below the workspace is no better: the model reaches that too.
    std::fs::create_dir_all(workspace.join("state")).expect("the directory is created");
    let nested = daemon::Options {
        db: workspace.join("state/agentd.db"),
        socket: dir.path().join("agentd.sock"),
        workspace: workspace.clone(),
        model: claude::DEFAULT_MODEL.to_string(),
        max_tokens: claude::DEFAULT_MAX_TOKENS,
        tick: Duration::from_millis(50),
        api_key: None,
    };
    assert!(
        tokio::time::timeout(Duration::from_secs(10), daemon::serve(nested))
            .await
            .expect("the daemon must refuse rather than start")
            .expect_err("a nested database must be refused")
            .contains("inside the workspace"),
        "a database below the workspace must be refused too"
    );

    // A link OUTSIDE the workspace can name a target inside it. The lock and
    // `SQLite` both follow the link. A test on the link's own path would
    // report the safe side of a rule the daemon then breaks.
    std::fs::write(workspace.join("real.db"), "").expect("the target exists");
    let link = dir.path().join("linked.db");
    std::os::unix::fs::symlink(workspace.join("real.db"), &link).expect("the link is made");
    let linked = daemon::Options {
        db: link,
        socket: dir.path().join("agentd.sock"),
        workspace: workspace.clone(),
        model: claude::DEFAULT_MODEL.to_string(),
        max_tokens: claude::DEFAULT_MAX_TOKENS,
        tick: Duration::from_millis(50),
        api_key: None,
    };
    assert!(
        tokio::time::timeout(Duration::from_secs(10), daemon::serve(linked))
            .await
            .expect("the daemon must refuse rather than start")
            .expect_err("a linked database must be refused")
            .contains("inside the workspace"),
        "a link into the workspace must be refused"
    );

    // A link that resolves to nothing would create its file wherever it
    // points, so it is refused rather than guessed at.
    let dangling = dir.path().join("dangling.db");
    std::os::unix::fs::symlink(workspace.join("absent.db"), &dangling).expect("the link is made");
    let broken = daemon::Options {
        db: dangling,
        socket: dir.path().join("agentd.sock"),
        workspace,
        model: claude::DEFAULT_MODEL.to_string(),
        max_tokens: claude::DEFAULT_MAX_TOKENS,
        tick: Duration::from_millis(50),
        api_key: None,
    };
    assert!(
        tokio::time::timeout(Duration::from_secs(10), daemon::serve(broken))
            .await
            .expect("the daemon must refuse rather than start")
            .expect_err("a dangling link must be refused")
            .contains("resolves to nothing"),
        "a link to nothing must be refused"
    );
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

#[test]
fn an_approval_signal_names_one_wait_and_only_that_wait() {
    // A late decision is recorded in history behind its expired deadline, where
    // it stays unconsumed. Under a name shared with a later wait it would
    // release a call nobody reviewed. So the name carries the turn and the
    // position as well as the tool-use id.
    let first = session::approval_signal(1, 0, "toolu_a");
    let same_call_later_turn = session::approval_signal(2, 0, "toolu_a");
    let same_turn_later_call = session::approval_signal(1, 1, "toolu_a");

    assert_ne!(
        first, same_call_later_turn,
        "a later turn must wait on its own name"
    );
    assert_ne!(
        first, same_turn_later_call,
        "a second call in one turn must wait on its own name"
    );

    // The operator still decides by tool-use id, so the name must give it back.
    for name in [&first, &same_call_later_turn, &same_turn_later_call] {
        assert_eq!(
            session::approval_call_id(name),
            Some("toolu_a"),
            "the call id must survive the round trip: {name}"
        );
    }

    // An id containing the separator still round-trips, and a name that is not
    // an approval signal is not mistaken for one.
    let odd = session::approval_signal(3, 4, "toolu:with:colons");
    assert_eq!(
        session::approval_call_id(&odd),
        Some("toolu:with:colons"),
        "the id is the remainder of the name"
    );
    assert_eq!(session::approval_call_id("something_else:1:0:x"), None);
    assert_eq!(session::approval_call_id("tool_approval"), None);
}
