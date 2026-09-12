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
use crate::inspect;
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
        if let Ok(Response::Sessions { .. }) =
            protocol::call(socket, &Request::List { before: None }).await
        {
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
    let RunState::Completed(output) = state else {
        panic!("expected completion, got {state:?}");
    };
    assert!(
        !workspace.join("agent-notes.md").exists(),
        "a denied write must never run"
    );

    // The session must say what happened. The stub proposed the write, the
    // operator denied it, and there is no file. An answer that reported the
    // note as recorded would be a false success in the one demonstration that
    // needs no key.
    let report: SessionReport = serde_json::from_value(output).expect("the report decodes");
    assert!(
        report.answer.contains("NOT recorded"),
        "a denied write must be reported as such: {}",
        report.answer
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
                matches!(stale, Response::Stale { .. }),
                "a decision for another call must be refused: {stale:?}"
            );
            // The refusal names a follow-up command, so the CLIENT renders it
            // against the socket this command reached. A command built in the
            // daemon would send the operator to `agentd.sock`.
            let told = crate::rendered_lines(&stale, Path::new("/run/agentd/project-b.sock"));
            assert!(
                told[0].contains("--socket /run/agentd/project-b.sock")
                    && told[0].contains(execution_id),
                "the recovery command must reach the same daemon: {}",
                told[0]
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

#[tokio::test]
async fn a_control_connection_is_identified_by_its_peer() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let socket = dir.path().join("probe.sock");
    let listener = tokio::net::UnixListener::bind(&socket).expect("the socket binds");

    let caller = tokio::spawn(async move {
        tokio::net::UnixStream::connect(&socket)
            .await
            .expect("the caller connects")
    });
    let (served, _) = listener.accept().await.expect("the daemon accepts");
    drop(caller.await.expect("the caller finishes"));

    // The kernel reports the caller's user, and no directory mode can forge
    // it. This is what the daemon checks, because a socket's own mode is
    // enforced on `connect` by Linux and not by macOS.
    let owner = rustix::process::geteuid().as_raw();
    assert!(
        daemon::peer_is_owner(&served, owner),
        "a caller running as the daemon's own user must be served"
    );
    assert!(
        !daemon::peer_is_owner(&served, owner.wrapping_add(1)),
        "a caller running as anyone else must be refused"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_caller_that_sends_nothing_does_not_hold_the_daemon() {
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
    await_daemon(&socket).await;

    // Connections that open and send no newline. Each one holds a permit until
    // its deadline, and there are more of them than the daemon holds at once.
    let mut silent = Vec::new();
    for _ in 0..40 {
        silent.push(
            tokio::net::UnixStream::connect(&socket)
                .await
                .expect("the caller connects"),
        );
    }

    // An honest caller is still answered. The silent ones are not holding the
    // daemon: a bounded read gives their permits back.
    let answered = tokio::time::timeout(
        Duration::from_secs(60),
        protocol::call(&socket, &Request::List { before: None }),
    )
    .await
    .expect("the honest caller must not wait on the silent ones")
    .expect("the list is answered");
    assert!(
        matches!(answered, Response::Sessions { .. }),
        "unexpected answer: {answered:?}"
    );

    drop(silent);
    daemon.abort();
}

#[tokio::test]
async fn a_decision_after_the_deadline_is_refused() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");
    let db = dir.path().join("agentd.db");

    let calls = Arc::new(AtomicUsize::new(0));
    let mut rt = runtime(&db, &workspace, &calls);
    // A deadline short enough to pass while the operator thinks.
    let task = serde_json::to_value(SessionTask {
        goal: "summarise the workspace".to_string(),
        max_turns: 6,
        approval_timeout_secs: 1,
        workspace: workspace_id(&workspace),
        model: claude::OFFLINE_MODEL.to_string(),
    })
    .expect("the task encodes");
    let exec = rt
        .start_workflow(WORKFLOW_NAME, task)
        .expect("the session starts");
    let signal = drive_to_approval(&mut rt, exec).await;

    let reader = crate::inspect::open(&db).expect("the inspector opens");
    let parked = |signal: &str| {
        let mut blocked = std::collections::HashMap::new();
        blocked.insert(
            exec,
            daemon::ParkedState {
                reason: "waiting for a tool approval".to_string(),
                signal: Some(signal.to_string()),
            },
        );
        blocked
    };

    // Before the deadline the decision is taken.
    let mut blocked = parked(&signal);
    let answer = daemon::approve(
        &mut rt,
        &reader,
        &mut blocked,
        &exec.to_string(),
        &signal,
        true,
        None,
    );
    assert!(
        matches!(answer, Response::Ack { .. }),
        "an on-time decision must be taken: {answer:?}"
    );

    // After it, the backend fires the expired timer BEFORE a late signal, so
    // the session denies the call however this answer reads. An "approved"
    // here would be a lie the operator finds only in the history.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let mut blocked = parked(&signal);
    let refused = daemon::approve(
        &mut rt,
        &reader,
        &mut blocked,
        &exec.to_string(),
        &signal,
        true,
        None,
    );
    let Response::Error { message } = refused else {
        panic!("a late decision must not be acknowledged, got {refused:?}");
    };
    assert!(
        message.contains("deadline"),
        "the refusal must say why: {message}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_daemon_answers_more_connections_than_it_holds_at_once() {
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
    await_daemon(&socket).await;

    // More callers at once than the daemon holds connections for. The bound
    // stops a polling script from spending the daemon's descriptors. This test
    // proves the bound costs no answers. The kernel queues the callers that
    // wait on the listening socket.
    let callers = 80;
    let mut answers = Vec::with_capacity(callers);
    for _ in 0..callers {
        let socket = socket.clone();
        answers.push(tokio::spawn(async move {
            protocol::call(&socket, &Request::List { before: None }).await
        }));
    }

    for answer in answers {
        let answered = answer.await.expect("the caller finishes");
        assert!(
            matches!(answered, Ok(Response::Sessions { .. })),
            "every caller must be answered: {answered:?}"
        );
    }
    daemon.abort();
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
    let listed = protocol::call(&socket, &Request::List { before: None })
        .await
        .expect("the list is answered");
    let Response::Sessions { sessions, .. } = listed else {
        panic!("unexpected answer: {listed:?}");
    };
    assert_eq!(sessions.len(), 1, "one session is recorded");

    let logged = protocol::call(
        &socket,
        &Request::History {
            execution_id,
            before: None,
        },
    )
    .await
    .expect("the history is answered");
    let Response::History { events, .. } = logged else {
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

    // A well-formed id that names no session is an error from both commands.
    // A mistyped audit target must not read as a session that did nothing.
    let unknown = protocol::call(
        &socket,
        &Request::Status {
            execution_id: "00000000-0000-4000-8000-000000000000".to_string(),
            full: false,
        },
    )
    .await
    .expect("the status is answered");
    assert!(
        matches!(unknown, Response::Error { .. }),
        "an unknown session must be refused: {unknown:?}"
    );
    let missing = protocol::call(
        &socket,
        &Request::History {
            execution_id: "00000000-0000-4000-8000-000000000000".to_string(),
            before: None,
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
    // The property is that no other local user can reach it. The owner's
    // execute bit is meaningless on a socket. The mask keeps that bit so a
    // DIRECTORY created while the mask is held stays enterable.
    assert_eq!(
        mode & 0o077,
        0,
        "the control socket must be owner-only, and has mode {mode:o}"
    );
    assert_eq!(mode & 0o700, 0o700, "the owner must reach its own socket");
    drop(listener);
}

#[test]
fn a_write_that_landed_is_never_reported_as_absent() {
    use crate::session::Message;

    // Two assistant turns put the stub on its final turn, where it reports.
    let transcript = |result: Value| TurnRequest {
        model: claude::OFFLINE_MODEL.to_string(),
        messages: vec![
            Message::user(json!([{ "type": "text", "text": "go" }])),
            Message::assistant(json!([{ "type": "text", "text": "listing" }])),
            Message::assistant(json!([{ "type": "text", "text": "writing" }])),
            Message::user(json!([result])),
        ],
    };
    let answer = |result: Value| claude::offline::reply(&transcript(result)).text;

    // A write can fail AFTER its rename: the file holds the new bytes, and
    // only the flush failed. Reporting that as "not recorded" would be false,
    // and it would contradict the reason printed beside it.
    let durable_failure = answer(json!({
        "type": "tool_result",
        "tool_use_id": "toolu_offline_write",
        "content": format!("`agent-notes.md` {} yet: no space left", tools::LANDED_UNFLUSHED),
        "is_error": true,
    }));
    assert!(
        durable_failure.contains("IS recorded"),
        "a write that landed must not be reported as absent: {durable_failure}"
    );

    // A write that never landed is still reported as absent.
    let refused = answer(json!({
        "type": "tool_result",
        "tool_use_id": "toolu_offline_write",
        "content": "the operator denied this call",
        "is_error": true,
    }));
    assert!(
        refused.contains("NOT recorded"),
        "a denied write must be reported as absent: {refused}"
    );

    // The block this daemon builds is replayed to the Messages API on the
    // next turn. It carries the fields that API defines for a tool_result,
    // and nothing else. A field invented here would travel with it.
    let outcome = session::ToolOutcome {
        output: "wrote 12 bytes".to_string(),
        is_error: false,
    };
    let block = session::tool_result_block("toolu_a", &outcome);
    let mut keys: Vec<&str> = block
        .as_object()
        .expect("the block is an object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec!["content", "is_error", "tool_use_id", "type"],
        "the tool_result block must carry no invented property"
    );
}

#[test]
fn a_blank_model_name_is_refused() {
    // A blank name is not a model. Every request would carry it, the API
    // would refuse each one, and the refusal of an accepted request is
    // terminal. The daemon would advertise readiness and fail every session.
    for blank in ["", " ", "\t\n"] {
        let (_, signal) = crate::shutdown::channel();
        let refusal = claude::ModelConfig::new(
            Some("sk-ant-example".to_string()),
            blank,
            claude::DEFAULT_MAX_TOKENS,
            signal,
        );
        let Err(message) = refusal else {
            panic!("a blank model name must be refused: {blank:?}");
        };
        assert!(
            message.contains("blank"),
            "the refusal must say what is wrong: {message}"
        );
    }

    // A real name still opens, and a padded one is stored trimmed. A check
    // that read the trimmed value while the verbatim one was sent would pass
    // this. It would then send a name with spaces to the API.
    for given in [
        claude::DEFAULT_MODEL,
        " claude-opus-5 ",
        "\tclaude-opus-5\n",
    ] {
        let (_, signal) = crate::shutdown::channel();
        let config = claude::ModelConfig::new(
            Some("sk-ant-example".to_string()),
            given,
            claude::DEFAULT_MAX_TOKENS,
            signal,
        )
        .expect("a real model name must be accepted");
        assert_eq!(
            config.identity(),
            given.trim(),
            "the stored name must carry no padding: {given:?}"
        );
    }
}

#[test]
fn a_long_history_does_not_make_one_unbounded_listing() {
    // `list` reads the whole row of every session it names, and both the goal
    // and the report are unbounded. The runtime is serialised, so an
    // unbounded listing would also block every session drive while it ran.
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("agentd.db");
    let conn = rusqlite::Connection::open(&db).expect("the database opens");
    conn.execute_batch(
        "CREATE TABLE harvest_executions (
            rowid_alias INTEGER, exec_id TEXT, workflow_name TEXT, state TEXT,
            input_json TEXT, output_json TEXT, error TEXT
        );",
    )
    .expect("the fixture schema is created");
    let rows = inspect::MAX_LISTED_SESSIONS + 25;
    for n in 0..rows {
        conn.execute(
            "INSERT INTO harvest_executions
             (exec_id, workflow_name, state, input_json, output_json, error)
             VALUES (?1, ?2, 'COMPLETED', '{}', NULL, NULL)",
            rusqlite::params![format!("exec-{n:04}"), WORKFLOW_NAME],
        )
        .expect("the fixture row is inserted");
    }
    drop(conn);

    let reader = inspect::open(&db).expect("the read-only connection opens");
    let listed = inspect::executions(&reader, WORKFLOW_NAME, None).expect("the listing reads");
    assert_eq!(
        listed.len(),
        inspect::MAX_LISTED_SESSIONS as usize + 1,
        "the listing must stop one past the cap, so the caller can say there are more"
    );

    // The newest are the ones an operator is looking for, and they read in
    // the order they were submitted.
    let last = listed.last().expect("the listing is not empty");
    assert_eq!(
        last.exec_id,
        format!("exec-{:04}", rows - 1),
        "the newest session must be in the listing"
    );
}

#[test]
fn a_listing_reads_no_whole_payload() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("agentd.db");
    let conn = rusqlite::Connection::open(&db).expect("the database opens");
    conn.execute_batch(
        "CREATE TABLE harvest_executions (
            exec_id TEXT, workflow_name TEXT, state TEXT,
            input_json TEXT, output_json TEXT, error TEXT
        );",
    )
    .expect("the fixture schema is created");

    // A recorded task and a recorded report are each written by somebody
    // else, and neither is bounded at the source. A listing that read them
    // whole would hold every byte of every session it names.
    let huge = "x".repeat(200_000);
    conn.execute(
        "INSERT INTO harvest_executions
         (exec_id, workflow_name, state, input_json, output_json, error)
         VALUES ('exec-1', ?1, 'COMPLETED', ?2, ?3, ?4)",
        rusqlite::params![
            WORKFLOW_NAME,
            json!({ "goal": huge, "workspace": "/w", "model": "m", "max_turns": 4,
                    "approval_timeout_secs": 1 })
            .to_string(),
            json!({ "answer": huge, "turns": 2, "tool_calls": 1, "stop": "end_turn" }).to_string(),
            huge,
        ],
    )
    .expect("the fixture row is inserted");
    drop(conn);

    let reader = inspect::open(&db).expect("the reader opens");
    let listed = inspect::executions(&reader, WORKFLOW_NAME, None).expect("the listing reads");
    let row = listed.first().expect("the session is listed");
    let cap = inspect::MAX_LISTED_CHARS as usize;

    for (name, field) in [
        ("goal", &row.goal),
        ("answer", &row.answer),
        ("error", &row.error),
    ] {
        let held = field.as_deref().unwrap_or_default();
        // Both halves matter. The first says the field obeys the cap. The
        // second says the cap is doing work: an assertion against the cap
        // alone would hold however large the cap became.
        assert!(
            held.len() <= cap,
            "the listing must cut `{name}` to the cap, and read {} bytes",
            held.len()
        );
        assert!(
            held.len() < huge.len(),
            "the listing must not read the whole `{name}`, and read {} of {} bytes",
            held.len(),
            huge.len()
        );
    }

    // The fields it does not cut are the ones that are already small, and the
    // listing still says what the session did.
    assert_eq!(
        row.stop.as_deref(),
        Some("end_turn"),
        "the stop reason reads"
    );
    assert_eq!(row.turns, Some(2), "the turn count reads");
    assert_eq!(row.tool_calls, Some(1), "the tool call count reads");
}

#[tokio::test]
async fn a_status_reads_a_bounded_slice_of_the_history() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("agentd.db");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");

    let calls = Arc::new(AtomicUsize::new(0));
    let mut rt = runtime(&db, &workspace, &calls);
    let exec = rt
        .start_workflow(WORKFLOW_NAME, task(&workspace))
        .expect("the session starts");
    let signal = drive_to_approval(&mut rt, exec).await;
    drop(rt);

    let reader = inspect::open(&db).expect("the reader opens");
    let exec_id = exec.to_string();

    // The status must find the awaited call in the newest events. Every model
    // activity carries the whole transcript, so reading the history entire is
    // unbounded twice over, and one status would block every session drive.
    let call = daemon::pending_call(&reader, &exec_id, &signal, false)
        .expect("the awaited call is in the newest events");
    assert_eq!(call.token, signal, "the call must be the awaited one");

    // The cap is what bounds the read. A session this short has fewer events
    // than `MAX_SCANNED_EVENTS`, so asserting against that cap would pass
    // whether or not the query carries a limit. The assertion uses a small
    // limit instead, which only holds if the limit reaches the query.
    let whole = inspect::event_lines(&reader, &exec_id, None, u32::MAX).expect("the events read");
    assert!(
        whole.len() > 3,
        "the fixture must hold more events than the limit below, and holds {}",
        whole.len()
    );
    let capped = inspect::event_lines(&reader, &exec_id, None, 3).expect("the events read");
    assert_eq!(
        capped.len(),
        3,
        "the read must stop at the limit it is given"
    );

    // Newest first, so the scan reaches the last model reply immediately.
    assert_eq!(
        capped.first().map(|line| line.seq),
        whole.first().map(|line| line.seq),
        "the bounded read must start at the newest event"
    );

    // A page is not a window. The search must reach an event that sits further
    // back than one page. A turn with many tool calls before its gated write
    // would otherwise leave the operator with no token to approve.
    let oldest_seq = whole.last().expect("the history is not empty").seq;
    let reached = (0..)
        .scan(None, |before: &mut Option<i64>, _| {
            let page = inspect::event_lines(&reader, &exec_id, *before, 1).ok()?;
            let seq = page.first()?.seq;
            *before = Some(seq);
            Some(seq)
        })
        .take(whole.len())
        .last()
        .expect("the walk reads at least one page");
    assert_eq!(
        reached, oldest_seq,
        "a page-by-page walk must reach the oldest event"
    );
}

#[tokio::test]
async fn a_status_finds_a_call_behind_more_events_than_one_page() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("agentd.db");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");

    let calls = Arc::new(AtomicUsize::new(0));
    let mut rt = runtime(&db, &workspace, &calls);
    let exec = rt
        .start_workflow(WORKFLOW_NAME, task(&workspace))
        .expect("the session starts");
    let signal = drive_to_approval(&mut rt, exec).await;
    drop(rt);

    let reader = inspect::open(&db).expect("the reader opens");
    let exec_id = exec.to_string();

    // One page of ONE event. A fixed window of this size would step over the
    // model reply that holds the awaited call. The status would then print no
    // token at all, and the operator could not approve before the deadline. A
    // page must bound the memory in hand, and nothing else.
    let mut before = None;
    let mut walked = 0;
    let found = loop {
        let page = inspect::event_lines(&reader, &exec_id, before, 1).expect("the page reads");
        let Some(line) = page.first() else {
            break None;
        };
        before = Some(line.seq);
        walked += 1;
        if line.label == "ActivityCompleted"
            && line
                .detail
                .as_deref()
                .is_some_and(|detail| detail.contains("tool_use"))
        {
            break Some(walked);
        }
    };
    let depth = found.expect("a model reply with a call is in the log");
    assert!(
        depth > 1,
        "the fixture must hide the reply behind at least one other event"
    );

    // The daemon's own lookup finds it whatever the page size.
    let call = daemon::pending_call(&reader, &exec_id, &signal, false)
        .expect("the awaited call must be found however deep it sits");
    assert_eq!(call.token, signal, "the call must be the awaited one");

    // The lookup reads the CALLS of a reply and not the reply. A reply holds
    // every earlier turn in its content, and none of that names a call. A read
    // of the whole reply would carry the transcript with it.
    let replies = inspect::reply_calls(&reader, &exec_id, None, 1).expect("the calls read");
    let (_, calls) = replies.first().expect("a reply is recorded");
    assert!(calls.is_array(), "the query must return the calls: {calls}");
    assert!(
        calls.get(0).is_some_and(|call| call.get("id").is_some()),
        "the calls must carry their ids: {calls}"
    );
    // The discriminating assertion. The whole reply is an OBJECT carrying a
    // stop reason and the replayed content blocks. The calls are an array
    // carrying neither. A tool input may hold a `content` field of its own,
    // so the stop reason is the field that tells the two shapes apart.
    let rendered = calls.to_string();
    assert!(
        !rendered.contains("stop_reason"),
        "the read must carry the calls alone: {rendered}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_history_command_reads_a_bounded_page() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("agentd.db");
    let socket = dir.path().join("agentd.sock");
    let options = daemon::Options {
        db: db.clone(),
        socket: socket.clone(),
        workspace: dir.path().join("workspace"),
        model: claude::DEFAULT_MODEL.to_string(),
        max_tokens: claude::DEFAULT_MAX_TOKENS,
        tick: Duration::from_millis(50),
        api_key: None,
    };
    let served = tokio::spawn(daemon::serve(options));
    await_daemon(&socket).await;

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
    await_parked(&socket, &execution_id).await;

    let answer = protocol::call(
        &socket,
        &Request::History {
            execution_id: execution_id.clone(),
            before: None,
        },
    )
    .await
    .expect("the history is answered");
    let Response::History { events, .. } = answer else {
        panic!("unexpected answer: {answer:?}");
    };

    // The audit trail is the point of the command, so a short session prints
    // whole. The bound is on what one command reads, not on what it may show.
    assert!(!events.is_empty(), "the history must not be empty");
    assert!(
        events.len() <= inspect::MAX_HISTORY_EVENTS as usize + 1,
        "the history must stay bounded, and printed {} lines",
        events.len()
    );
    assert!(
        !events[0].contains("the log holds more"),
        "a short session is the whole log: {}",
        events[0]
    );

    // An event's data is cut in the DATABASE. A recorded activity can approach
    // the backend's payload cap, and a page names hundreds of them. A page
    // that read them whole would hold gigabytes for one command.
    let cap = inspect::MAX_EVENT_DETAIL_CHARS as usize;
    for line in &events {
        assert!(
            line.chars().count() <= cap + 64,
            "an audit line must be cut, and printed {} characters",
            line.chars().count()
        );
    }

    // Each line carries the event's OWN sequence number, which the log counts
    // from zero. A bounded read therefore never renumbers the log it shows,
    // and a later page reads on from where this one ended.
    assert!(
        events[0].trim_start().starts_with("0  "),
        "the first line must carry the log's own first sequence number: {}",
        events[0]
    );

    // A truncated log must be reachable. The marker names the command that
    // reads the events before this page, and that command must work.
    let page = protocol::call(
        &socket,
        &Request::History {
            execution_id: execution_id.clone(),
            before: Some(2),
        },
    )
    .await
    .expect("the page is answered");
    let Response::History { events: older, .. } = page else {
        panic!("unexpected answer: {page:?}");
    };
    assert_eq!(older.len(), 2, "the page before event 2 holds two events");

    // The continuation command is rendered by the CLIENT, so it carries the
    // socket the operator asked. A command built in the daemon cannot know
    // it, and would send the operator to whatever answers the default.
    let hinted = crate::rendered_lines(
        &Response::History {
            events: vec!["0  WorkflowStarted".to_string()],
            execution_id: execution_id.clone(),
            older: Some(7),
        },
        Path::new("/run/agentd/project-b.sock"),
    );
    assert!(
        hinted[0].contains("--socket /run/agentd/project-b.sock")
            && hinted[0].contains("--before 7")
            && hinted[0].contains(&execution_id),
        "the continuation command must reach the same daemon: {}",
        hinted[0]
    );
    assert!(
        older[0].trim_start().starts_with("0  "),
        "the page must start at the log's own first event: {}",
        older[0]
    );

    served.abort();
    drop(served.await);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_empty_goal_never_starts_a_session() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let socket = dir.path().join("agentd.sock");
    let served = tokio::spawn(daemon::serve(daemon::Options {
        db: dir.path().join("agentd.db"),
        socket: socket.clone(),
        workspace: dir.path().join("workspace"),
        model: claude::DEFAULT_MODEL.to_string(),
        max_tokens: claude::DEFAULT_MAX_TOKENS,
        tick: Duration::from_millis(50),
        api_key: None,
    }));
    await_daemon(&socket).await;

    // An empty goal is sent as an empty text block, which the API refuses.
    // The refusal of an accepted request is terminal here, so the daemon
    // would acknowledge a session that could never make its first call.
    for empty in ["", "   ", "\t\n"] {
        let answer = protocol::call(
            &socket,
            &Request::Submit {
                goal: empty.to_string(),
                max_turns: 4,
                approval_timeout_secs: 60,
            },
        )
        .await
        .expect("the submit is answered");
        let Response::Error { message } = answer else {
            panic!("an empty goal must be refused, and got: {answer:?}");
        };
        assert!(
            message.contains("empty"),
            "the refusal must say what is wrong: {message}"
        );
    }

    // The field beside it takes the same argument. A protocol client can send
    // a bound of zero even though the CLI refuses it. The loop would then run
    // `1..=0` and record a COMPLETED session that never called the model.
    // The deadline is recorded and read back as a signed integer. A larger
    // one would be accepted here and would then stop every later daemon.
    let huge = protocol::call(
        &socket,
        &Request::Submit {
            goal: "a real goal".to_string(),
            max_turns: 4,
            approval_timeout_secs: u64::MAX,
        },
    )
    .await
    .expect("the submit is answered");
    let Response::Error { message } = huge else {
        panic!("an unrecordable deadline must be refused, and got: {huge:?}");
    };
    assert!(
        message.contains("approval deadline"),
        "the refusal must name the field: {message}"
    );

    let zero = protocol::call(
        &socket,
        &Request::Submit {
            goal: "a real goal".to_string(),
            max_turns: 0,
            approval_timeout_secs: 60,
        },
    )
    .await
    .expect("the submit is answered");
    let Response::Error { message } = zero else {
        panic!("a zero turn bound must be refused, and got: {zero:?}");
    };
    assert!(
        message.contains("turn"),
        "the refusal must say what is wrong: {message}"
    );

    // Nothing was recorded, so no session is left to fail.
    let listed = protocol::call(&socket, &Request::List { before: None })
        .await
        .expect("the listing is answered");
    let Response::Sessions { sessions, .. } = listed else {
        panic!("unexpected answer: {listed:?}");
    };
    assert!(
        sessions.is_empty(),
        "a refused submit must record nothing: {sessions:?}"
    );

    served.abort();
    drop(served.await);
}

#[test]
fn a_block_that_cannot_be_replayed_is_refused() {
    use autumn_harvest::failure::parse_error_payload_full;

    let reply = |content: Value| TurnReply {
        content,
        stop_reason: "tool_use".to_string(),
        text: String::new(),
        tool_calls: vec![ToolCall {
            id: "toolu_a".to_string(),
            name: tools::TOOL_WRITE_FILE.to_string(),
            input: json!({ "path": "notes.md", "content": "x" }),
        }],
    };

    // The assistant blocks are replayed VERBATIM on the next request. A block
    // the API will not accept back fails the turn AFTER this turn's tools
    // have run. A malformed billed response would therefore leave a real
    // change on the disk, and a failed session behind it.
    for malformed in [
        json!([Value::Null, { "type": "tool_use", "id": "toolu_a" }]),
        json!(["a bare string"]),
        json!([{ "text": "a block with no type" }]),
        json!([{ "type": "" }]),
        json!("not an array at all"),
    ] {
        assert!(
            !claude::has_replayable_content(&reply(malformed.clone())),
            "a block that cannot be replayed must be refused: {malformed}"
        );
    }

    // A block of a type this example KNOWS must carry that type's fields. The
    // API refuses these on replay, and `parse_reply` would quietly default
    // them here. A text block with no text reads as an empty answer. A call
    // with no name reads as a call to nothing.
    for incomplete in [
        json!([{ "type": "text" }]),
        json!([{ "type": "text", "text": 7 }]),
        // A text block is declared with a minimum length of one character,
        // so an empty one is refused on replay.
        json!([{ "type": "text", "text": "" }]),
        json!([{ "type": "tool_use", "id": "toolu_a", "input": {} }]),
        json!([{ "type": "tool_use", "id": "toolu_a", "name": " ", "input": {} }]),
        json!([{ "type": "tool_use", "name": "write_file", "input": {} }]),
        json!([{ "type": "tool_use", "id": "toolu_a", "name": "write_file" }]),
        json!([{ "type": "tool_use", "id": "toolu_a", "name": "write_file", "input": Value::Null }]),
        json!([{ "type": "tool_use", "id": "toolu_a", "name": "write_file", "input": "text" }]),
        json!([{ "type": "tool_use", "id": "toolu_a", "name": "write_file", "input": [] }]),
        json!([{ "type": "thinking", "signature": "abc" }]),
        json!([{ "type": "thinking", "thinking": Value::Null, "signature": "abc" }]),
        json!([{ "type": "thinking", "thinking": 7, "signature": "abc" }]),
        // The signature carries the encrypted reasoning, and the API reads it
        // to prove the block came from the model. It is present whatever the
        // display setting, so a block without one cannot be replayed.
        json!([{ "type": "thinking", "thinking": "reasoned" }]),
        json!([{ "type": "thinking", "thinking": "", "signature": "" }]),
        json!([{ "type": "thinking", "thinking": "", "signature": " " }]),
        json!([{ "type": "thinking", "thinking": "", "signature": 7 }]),
        json!([{ "type": "redacted_thinking" }]),
        json!([{ "type": "redacted_thinking", "data": "" }]),
        // A padded name is a CORRUPTED block of a type this example knows,
        // and not a type from a later API. `parse_reply` matches the type
        // exactly, so it would ignore the block while the API refuses it.
        json!([{ "type": " text ", "text": "hello" }]),
        json!([{ "type": "text\n", "text": "hello" }]),
        json!([{ "type": " thinking ", "thinking": "", "signature": "abc" }]),
        json!([{ "type": " tool_use ", "id": "toolu_a", "name": "write_file", "input": {} }]),
    ] {
        assert!(
            !claude::has_replayable_content(&reply(incomplete.clone())),
            "a known block missing its fields must be refused: {incomplete}"
        );
    }

    // The text of a thinking block may be EMPTY, and the block is still
    // replayed unchanged. This request asks for adaptive thinking and asks
    // for no display, and under the default display every thinking block
    // comes back with an empty text. A check for text here would refuse the
    // model's ORDINARY replies, which is a worse fault than the one above.
    for empty in [
        json!([{ "type": "thinking", "thinking": "", "signature": "abc" }]),
        json!([{ "type": "thinking", "thinking": "reasoned", "signature": "abc" }]),
        json!([{ "type": "redacted_thinking", "data": "abc" }]),
    ] {
        assert!(
            claude::has_replayable_content(&reply(empty.clone())),
            "an empty thinking block must still be replayed: {empty}"
        );
    }

    // A block type this example does not know about still passes, because the
    // API knows types this example does not. Guessing at the fields of a type
    // from a later API would refuse replies that are perfectly good.
    for fine in [
        json!([{ "type": "text", "text": "hello" }]),
        // One space is a character, so it meets the minimum. This asks for
        // length and not for content.
        json!([{ "type": "text", "text": " " }]),
        json!([{ "type": "a_type_from_a_later_api" }]),
        json!([{ "type": "tool_use", "id": "toolu_a", "name": "write_file", "input": {} }]),
        json!([]),
    ] {
        assert!(
            claude::has_replayable_content(&reply(fine.clone())),
            "a well-formed block must pass: {fine}"
        );
    }

    // The refusal is terminal, because the response was billed.
    let refused = parse_error_payload_full(&claude::body_failure(
        reqwest::StatusCode::OK,
        "its response carried a content block that cannot be replayed",
    ));
    assert!(refused.non_retryable, "a billed malformed body is terminal");
}

#[test]
fn a_zero_turn_session_is_rejected() {
    use clap::Parser;

    // Zero turns is not a session. The loop runs no iteration, and the run is
    // recorded COMPLETED with a blank answer and `max_turns` as its reason.
    assert!(
        crate::Cli::try_parse_from(["agentd", "submit", "goal", "--max-turns", "0"]).is_err(),
        "a zero turn bound must be refused at the boundary"
    );
    assert!(
        crate::Cli::try_parse_from(["agentd", "submit", "goal", "--max-turns", "1"]).is_ok(),
        "one turn is a session"
    );
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

    // The status reads the event log through the read-only connection, and it
    // reads a bounded number of the newest events rather than the whole
    // history. See `inspect::MAX_SCANNED_EVENTS`.
    let reader = inspect::open(&dir.path().join("agentd.db")).expect("the reader opens");
    let exec_id = exec.to_string();
    let trimmed =
        daemon::pending_call(&reader, &exec_id, &signal, false).expect("the status shows a call");
    assert!(
        trimmed.input.contains("truncated"),
        "the status must say when it has trimmed the payload"
    );
    assert!(
        !trimmed.input.contains(tail),
        "the trimmed view cannot hold the whole payload"
    );

    let whole =
        daemon::pending_call(&reader, &exec_id, &signal, true).expect("the full view shows a call");
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
        claude::DEFAULT_MODEL,
        claude::DEFAULT_MAX_TOKENS,
        crate::shutdown::channel().1,
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

    // A blank stop reason is not a stop reason. It also differs from
    // `end_turn`, so the usability test would accept it, and the loop would
    // record a completed session whose stop reason says nothing. Whitespace
    // is as blank as an empty string, and it takes the same path.
    for blank in [
        json!({ "content": [Value::Null], "stop_reason": "" }),
        json!({ "content": [Value::Null], "stop_reason": " " }),
        json!({ "content": [Value::Null], "stop_reason": "\t\n" }),
    ] {
        assert!(
            !claude::is_message(&blank),
            "{blank} must not pass as a message"
        );
    }

    // A malformed body from an accepted request is terminal, like the others.
    let refused = parse_error_payload_full(&claude::body_failure(
        reqwest::StatusCode::OK,
        "its response was not a message",
    ));
    assert!(refused.non_retryable, "a billed malformed body is terminal");
}

#[test]
fn a_padded_stop_reason_is_refused_rather_than_normalised() {
    // A reason with space around it has no safe normalisation. Trimming turns
    // ` tool_use ` into the reason that AUTHORISES a tool call, so a
    // malformed response would reach the approval gate and run an approved
    // write. Keeping it verbatim matches no reason this loop acts on, so
    // ` end_turn ` would record a session as complete with no answer.
    //
    // So it is refused as the malformed body it is, before either.
    for padded in [" end_turn ", " tool_use ", "tool_use\t", "\nend_turn"] {
        let payload = json!({
            "content": [{ "type": "text", "text": "done" }],
            "stop_reason": padded,
        });
        assert!(
            !claude::is_message(&payload),
            "a padded stop reason must be refused: {padded:?}"
        );
    }

    // The two reasons this loop acts on still pass, exactly as they arrive.
    for exact in [claude::STOP_END_TURN, claude::STOP_TOOL_USE, "max_tokens"] {
        let payload = json!({
            "content": [{ "type": "text", "text": "done" }],
            "stop_reason": exact,
        });
        assert!(
            claude::is_message(&payload),
            "an exact stop reason must pass: {exact}"
        );
    }

    // A blank one is still refused, which is what this check was built for.
    for blank in ["", " ", "\t"] {
        let payload = json!({
            "content": [{ "type": "text", "text": "done" }],
            "stop_reason": blank,
        });
        assert!(
            !claude::is_message(&payload),
            "a blank stop reason must be refused: {blank:?}"
        );
    }

    // The projection keeps the reason VERBATIM, so nothing downstream can
    // turn a padded one into the reason that authorises a tool call.
    let padded = claude::parse_reply(&json!({
        "content": [{ "type": "text", "text": "done" }],
        "stop_reason": " tool_use ",
    }));
    assert_eq!(
        padded.stop_reason, " tool_use ",
        "the projection must not normalise the reason"
    );
    assert_ne!(
        padded.stop_reason,
        claude::STOP_TOOL_USE,
        "a padded reason must never match the one that authorises a tool call"
    );
}

#[test]
fn a_turn_of_whitespace_is_not_an_answer() {
    // Whitespace is not an answer. A turn carrying only blank text passes an
    // emptiness test, so the session reports a clean finish with nothing in
    // it. The bytes are kept as they arrive, because indentation and line
    // breaks are part of a code answer. Only the decision reads the trim.
    let blank = claude::parse_reply(&json!({
        "content": [{ "type": "text", "text": "  \n\t " }],
        "stop_reason": "end_turn",
    }));
    assert_eq!(blank.text, "  \n\t ", "the bytes must arrive unchanged");
    assert!(
        !claude::is_usable(&blank),
        "a turn of whitespace must not pass as an answer"
    );

    let indented = claude::parse_reply(&json!({
        "content": [{ "type": "text", "text": "    let x = 1;\n" }],
        "stop_reason": "end_turn",
    }));
    assert!(
        claude::is_usable(&indented),
        "an indented answer must still pass"
    );
    assert_eq!(
        indented.text, "    let x = 1;\n",
        "an answer keeps its own layout"
    );
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

#[tokio::test(flavor = "multi_thread")]
async fn a_daemon_refuses_a_session_it_cannot_read() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("agentd.db");
    let first_socket = dir.path().join("first.sock");
    let second_socket = dir.path().join("second.sock");
    let options = |socket: &Path| daemon::Options {
        db: db.clone(),
        socket: socket.to_path_buf(),
        workspace: dir.path().join("workspace"),
        model: claude::DEFAULT_MODEL.to_string(),
        max_tokens: claude::DEFAULT_MAX_TOKENS,
        tick: Duration::from_millis(50),
        api_key: None,
    };

    // Park one session, then drop the daemon holding it.
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
    first.abort();
    drop(first.await);

    // Stand in for rows a newer daemon wrote. The fixture edits the EXECUTION
    // row, which is state rather than history: the append-only event log is
    // not touched.
    //
    // Each task below is complete APART FROM the one field named against it.
    // The startup check reads the small fields as values and the goal by its
    // type. A check that read fewer of them, or that read a number without
    // its range, would enlist the row. The first drive would then fail to
    // deserialise the task, and the runtime would seal the session FAILED
    // where no later daemon could resume it.
    let workspace = dir.path().join("workspace").to_string_lossy().to_string();
    let task = |field: &str, value: Value| {
        let mut whole = json!({
            "goal": "summarise the workspace",
            "max_turns": 6,
            "approval_timeout_secs": 300,
            "workspace": workspace,
            "model": claude::OFFLINE_MODEL,
        });
        let object = whole.as_object_mut().expect("the task is an object");
        if value.is_null() {
            object.remove(field);
        } else {
            object.insert(field.to_string(), value);
        }
        whole.to_string()
    };
    // A JSON `true` reads back from `json_extract` as the integer 1, so the
    // type guard and not the value is what refuses it.
    let broken = [
        ("goal", Value::Null),
        // A goal of no characters is refused for the reason `submit` refuses
        // one. An empty text block is below the minimum the API accepts, so
        // the first live turn would end the session terminally.
        ("goal", json!("")),
        ("goal", json!("   ")),
        ("goal", json!("\t\n ")),
        // `json_type` calls this an integer and `json_extract` returns a
        // real. The type test alone therefore admits it. Reading it as an
        // integer then fails the WHOLE query, which names no session.
        (
            "approval_timeout_secs",
            json!(9_223_372_036_854_775_808_u64),
        ),
        ("max_turns", Value::Null),
        ("max_turns", json!(-1)),
        ("max_turns", json!(0)),
        ("max_turns", json!(true)),
        ("max_turns", json!(4_294_967_296i64)),
        ("approval_timeout_secs", Value::Null),
        ("approval_timeout_secs", json!(-1)),
    ];

    for (field, value) in broken {
        let writer = rusqlite::Connection::open(&db).expect("the database opens");
        writer
            .execute(
                "UPDATE harvest_executions SET input_json = ?2 WHERE exec_id = ?1",
                rusqlite::params![&execution_id, task(field, value.clone())],
            )
            .expect("the input is replaced");
        drop(writer);

        // The daemon must say so rather than report readiness over a session
        // it silently dropped. Without the refusal `serve` runs until Ctrl-C,
        // so the timeout keeps a regression short.
        let refusal = tokio::time::timeout(
            Duration::from_secs(10),
            daemon::serve(options(&second_socket)),
        )
        .await
        .unwrap_or_else(|_| {
            panic!("the daemon must refuse rather than start on {field} = {value}")
        });
        let message = refusal.expect_err(&format!("{field} = {value} must refuse the start"));
        assert!(
            message.contains(&execution_id),
            "the refusal must name the row: {message}"
        );
        assert!(
            message.contains("cannot read"),
            "the refusal must say what is wrong: {message}"
        );
    }
}

#[test]
fn model_text_cannot_drive_the_terminal() {
    // A file in the workspace can tell the model what to answer, so the
    // answer is untrusted. The operator reads a pending call from this
    // output and approves it.
    let clearing = crate::visible("done\u{1b}[2K\u{1b}[1A");
    assert!(
        !clearing.contains('\u{1b}'),
        "an escape must not reach the terminal: {clearing}"
    );
    assert!(
        clearing.contains("\\u{001b}"),
        "the escape must be shown instead: {clearing}"
    );

    // OSC 52 writes the operator's clipboard.
    let clipboard = crate::visible("\u{1b}]52;c;cm0K\u{7}");
    assert!(
        !clipboard.contains('\u{1b}') && !clipboard.contains('\u{7}'),
        "a clipboard sequence must not reach the terminal: {clipboard}"
    );

    // A carriage return overwrites the line the operator already read.
    let overwrite = crate::visible("safe.md\rmalicious.md");
    assert!(
        !overwrite.contains('\r'),
        "a carriage return must not reach the terminal: {overwrite}"
    );

    // A bidirectional control reorders what is displayed, so one path reads
    // as another. The whole `Bidi_Control` set counts, and not only the
    // overrides: a single mark beside right-to-left text reorders it too.
    for control in [
        '\u{061c}', '\u{200e}', '\u{200f}', '\u{202a}', '\u{202b}', '\u{202c}', '\u{202d}',
        '\u{202e}', '\u{2066}', '\u{2067}', '\u{2068}', '\u{2069}',
    ] {
        let path = format!("notes{control}gnp.md");
        let reordered = crate::visible(&path);
        assert!(
            !reordered.contains(control),
            "a bidirectional control must not reach the terminal: {:04x}",
            control as u32
        );
    }

    // An answer keeps its own layout, and ordinary text is untouched.
    let answer = "line one\nline two\n\tindented 👩‍💻 done";
    assert_eq!(
        crate::visible(answer),
        answer,
        "a newline, a tab and a joiner are part of the answer"
    );

    // The status view is the thing an operator reads before approving, and
    // every field of it carries the model's own words.
    let view = protocol::SessionView {
        execution_id: "01JCEXEC".to_string(),
        goal: "tidy the notes\u{1b}[31m".to_string(),
        state: "RUNNING".to_string(),
        blocked_on: Some("a tool approval\r".to_string()),
        pending: Some(protocol::PendingCall {
            token: "tool_approval:1:0:toolu_a".to_string(),
            id: "toolu_a".to_string(),
            tool: "write_file\u{1b}[2K".to_string(),
            input: "{\"path\":\"notes\u{202e}gnp.md\"}".to_string(),
        }),
        answer: Some("done\u{1b}]52;c;cm0K\u{7}".to_string()),
        error: None,
    };
    for rendered in crate::session_lines(&view, Path::new("agentd.sock")) {
        assert!(
            !rendered
                .chars()
                .any(|c| c.is_control() && c != '\n' && c != '\t'),
            "a printed line must carry no control character: {rendered:?}"
        );
        assert!(
            !rendered.contains('\u{202e}'),
            "a printed line must carry no override: {rendered:?}"
        );
    }
}

#[test]
fn a_created_directory_can_be_entered_by_its_owner() {
    use std::os::unix::fs::PermissionsExt;

    // The umask is one value for the whole process, so the hostile mask runs
    // in a CHILD. A sibling test creating a file at the same moment would
    // otherwise see it, and the mode it asserts would be wrong. The child is
    // this same test binary, told by the marker to do the second half.
    const MARKER: &str = "AGENTD_UMASK_WORKSPACE";
    let Ok(workspace) = std::env::var(MARKER) else {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let status =
            std::process::Command::new(std::env::current_exe().expect("the running test binary"))
                .args([
                    "--exact",
                    "tests::a_created_directory_can_be_entered_by_its_owner",
                ])
                .env(MARKER, dir.path())
                .status()
                .expect("the child runs");
        assert!(status.success(), "the child must pass: {status}");
        return;
    };

    // A umask that masks every owner bit. `create_dir_all` asks for 0777, so
    // what survives is 000, and nothing can be written inside the result.
    unsafe { libc::umask(0o777) };
    let workspace = Path::new(&workspace);
    let nested = workspace.join("deep/nested");
    tools::create_enterable(&nested).expect("the directories are created");

    for level in [workspace.join("deep"), nested.clone()] {
        let mode = std::fs::metadata(&level)
            .expect("the directory exists")
            .permissions()
            .mode()
            & 0o7777;
        assert_eq!(
            mode & 0o700,
            0o700,
            "{} must stay enterable by its owner, and has mode {mode:o}",
            level.display()
        );
    }

    // The point of the owner bits: a file can be written inside.
    std::fs::write(nested.join("notes.md"), "hello").expect("a write lands inside");

    // A directory that exists and cannot be entered is refused, not widened.
    // An operator can lock one deliberately, and a daemon killed between the
    // creation and the mode leaves the same thing. The two are identical on
    // disk, so the mode is left alone and the path is named instead.
    let locked = workspace.join("locked");
    std::fs::create_dir(&locked).expect("the directory is created");
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000))
        .expect("the directory is locked");
    let refusal =
        tools::create_enterable(&locked.join("child")).expect_err("a locked parent is refused");
    assert_eq!(
        refusal.kind(),
        std::io::ErrorKind::PermissionDenied,
        "the refusal must say what is wrong: {refusal}"
    );
    assert!(
        refusal.to_string().contains("locked"),
        "the refusal must name the path: {refusal}"
    );
    let kept = std::fs::metadata(&locked)
        .expect("the directory exists")
        .permissions()
        .mode()
        & 0o7777;
    assert_eq!(
        kept, 0o000,
        "a locked directory keeps the mode it was given"
    );

    // The private mask is held around the database open and the socket bind.
    // It is one value for the whole process, so a directory created by any
    // other thread in that window takes it. A mask that hid the owner's
    // execute bit would make such a directory unusable. The refusal above
    // would then fire on a directory the daemon itself caused.
    let under_mask = guard::with_private_umask(|| {
        let held = workspace.join("held");
        std::fs::create_dir(&held).expect("the directory is created");
        std::fs::metadata(&held)
            .expect("the directory exists")
            .permissions()
            .mode()
            & 0o7777
    });
    assert_eq!(
        under_mask & 0o700,
        0o700,
        "a directory created under the private mask must stay usable, \
         and has mode {under_mask:o}"
    );
    assert_eq!(
        under_mask & 0o077,
        0,
        "a directory created under the private mask must stay private, \
         and has mode {under_mask:o}"
    );

    // A directory that is narrow but USABLE is a mode an operator can mean.
    // It is left exactly as they set it.
    let narrow = workspace.join("narrow");
    std::fs::create_dir(&narrow).expect("the directory is created");
    std::fs::set_permissions(&narrow, std::fs::Permissions::from_mode(0o500))
        .expect("the directory is made read-only");
    tools::create_enterable(&narrow).expect("an existing usable directory is accepted");
    let kept = std::fs::metadata(&narrow)
        .expect("the directory exists")
        .permissions()
        .mode()
        & 0o7777;
    assert_eq!(
        kept, 0o500,
        "a usable narrow directory keeps the mode the operator chose"
    );
}

#[test]
fn the_decide_line_reaches_the_daemon_that_printed_it() {
    let view = |token: &str| protocol::SessionView {
        execution_id: "01JCEXEC".to_string(),
        goal: "tidy the notes".to_string(),
        state: "RUNNING".to_string(),
        blocked_on: None,
        pending: Some(protocol::PendingCall {
            token: token.to_string(),
            id: "toolu_a".to_string(),
            tool: "write_file".to_string(),
            input: "{}".to_string(),
        }),
        answer: None,
        error: None,
    };
    let decide = |socket: &str| {
        crate::session_lines(&view("tool_approval:1:0:toolu_a"), Path::new(socket))
            .into_iter()
            .find(|line| line.contains("decide:"))
            .expect("the decide line is printed")
    };

    // The documented setup gives each daemon its own socket. A command copied
    // out of one daemon's status must not go to another daemon, or to none.
    let named = decide("/run/agentd/project-b.sock");
    assert!(
        named.contains("--socket /run/agentd/project-b.sock"),
        "the chosen socket must be carried: {named}"
    );

    // The submit command prints a follow-up too, and it is the same failure
    // one branch away.
    let watch = |socket: &str| {
        crate::rendered_lines(
            &Response::Submitted {
                execution_id: "01JCEXEC".to_string(),
            },
            Path::new(socket),
        )
        .into_iter()
        .find(|line| line.contains("Watch it with"))
        .expect("the watch line is printed")
    };
    let watch_named = watch("/run/agentd/project-b.sock");
    assert!(
        watch_named.contains("--socket /run/agentd/project-b.sock"),
        "the watch command must carry the socket: {watch_named}"
    );
    assert!(
        !watch("agentd.sock").contains("--socket"),
        "the default socket needs no flag on the watch command"
    );

    // The common case stays short.
    let default = decide("agentd.sock");
    assert!(
        !default.contains("--socket"),
        "the default socket needs no flag: {default}"
    );

    // A directory with a space in its name is ordinary, and the line is made
    // to be copied into a shell.
    let spaced = decide("/home/a b/agentd.sock");
    assert!(
        spaced.contains("--socket '/home/a b/agentd.sock'"),
        "a socket a shell would split must be quoted: {spaced}"
    );

    // A late decision points at the history, and that command is the same
    // failure again. The daemon sends the id and the client builds the line.
    let late = |socket: &str| {
        crate::rendered_lines(
            &Response::Ack {
                detail: "approved, and the deadline passed".to_string(),
                history_of: Some("01JCEXEC".to_string()),
            },
            Path::new(socket),
        )
        .into_iter()
        .find(|line| line.contains("history"))
        .expect("the history line is printed")
    };
    let late_named = late("/run/agentd/project-b.sock");
    assert!(
        late_named.contains("--socket /run/agentd/project-b.sock")
            && late_named.contains("01JCEXEC"),
        "the history command must carry the socket: {late_named}"
    );

    // The refusal an operator meets when no daemon answers names the socket
    // it tried, so the command that STARTS one must name it too. This line is
    // built in the protocol module rather than the renderer.
    let unreachable = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime")
        .block_on(protocol::call(
            Path::new("/run/agentd/project-b.sock"),
            &Request::List { before: None },
        ))
        .expect_err("no daemon listens there");
    assert!(
        unreachable.contains("agentd serve --socket /run/agentd/project-b.sock"),
        "the start command must name the socket that failed: {unreachable}"
    );
}

/// A goal opening with a NUL is still a goal.
///
/// Rust keeps that byte through `trim`, so `submit` accepts such a goal. The
/// database counts TEXT to the first NUL and stops, so a count of characters
/// measures zero there. The startup check would then refuse to start over a
/// task it can read perfectly well. No daemon of this version could resume
/// the session. The two checks must agree about the same value.
#[test]
fn a_goal_opening_with_a_nul_is_measured_whole() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("nul.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    writer
        .execute(
            "CREATE TABLE harvest_executions (exec_id TEXT, workflow_name TEXT, \
             state TEXT, input_json TEXT, output_json TEXT, error TEXT)",
            [],
        )
        .expect("the fixture table is created");
    let task = |goal: &str| {
        json!({
            "goal": goal,
            "max_turns": 4,
            "approval_timeout_secs": 300,
            "workspace": "/tmp/w",
            "model": claude::OFFLINE_MODEL,
        })
        .to_string()
    };
    for (exec, goal) in [
        ("nul-first", "\u{0}summarise the workspace"),
        ("nul-middle", "summarise\u{0}the workspace"),
        ("plain", "summarise the workspace"),
    ] {
        writer
            .execute(
                "INSERT INTO harvest_executions VALUES (?1, ?2, 'RUNNING', ?3, NULL, NULL)",
                rusqlite::params![exec, WORKFLOW_NAME, task(goal)],
            )
            .expect("the session is recorded");
    }
    // A goal that is only space is still refused, so the byte count did not
    // trade one fault for another.
    writer
        .execute(
            "INSERT INTO harvest_executions VALUES ('blank', ?1, 'RUNNING', ?2, NULL, NULL)",
            rusqlite::params![WORKFLOW_NAME, task("   ")],
        )
        .expect("the blank session is recorded");
    drop(writer);

    let reader = rusqlite::Connection::open(&db).expect("the database opens");
    let running = inspect::running(&reader, WORKFLOW_NAME).expect("the query runs");
    let goal_of = |exec: &str| {
        running
            .iter()
            .find(|row| row.exec_id == exec)
            .expect("the session is RUNNING")
            .has_goal
    };
    assert!(
        goal_of("nul-first"),
        "a goal opening with a NUL must count as a goal"
    );
    assert!(
        goal_of("nul-middle"),
        "a goal holding a NUL must count as a goal"
    );
    assert!(goal_of("plain"), "an ordinary goal must count as a goal");
    assert!(
        !goal_of("blank"),
        "a goal of only space must still be refused"
    );
}

/// The startup check draws the blank-goal line where `submit` draws it.
///
/// Rust's `trim` removes the whole Unicode whitespace set. `SQLite`'s removes
/// only the characters it is given. A goal of non-breaking spaces says
/// nothing, and `submit` refuses it. A narrower set in SQL would resume a
/// session the other end of this invariant calls blank.
#[test]
fn a_goal_of_unicode_space_is_refused_as_submit_refuses_it() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("space.db");
    let conn = rusqlite::Connection::open(&db).expect("the database opens");
    conn.execute(
        "CREATE TABLE harvest_executions (exec_id TEXT, workflow_name TEXT, \
         state TEXT, input_json TEXT, output_json TEXT, error TEXT)",
        [],
    )
    .expect("the fixture table is created");
    drop(conn);
    let task = |goal: &str| {
        json!({
            "goal": goal,
            "max_turns": 4,
            "approval_timeout_secs": 300,
            "workspace": "/tmp/w",
            "model": claude::OFFLINE_MODEL,
        })
        .to_string()
    };
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    for (exec, goal) in [
        ("nbsp", "\u{a0}\u{a0}"),
        ("ideographic", "\u{3000}"),
        ("thin", "\u{2009}\u{2009}"),
        ("nel", "\u{85}"),
    ] {
        writer
            .execute(
                "INSERT INTO harvest_executions VALUES (?1, ?2, 'RUNNING', ?3, NULL, NULL)",
                rusqlite::params![exec, WORKFLOW_NAME, task(goal)],
            )
            .expect("the session is recorded");
    }
    // One wrapped in that space is still a goal, so the trim is a trim.
    writer
        .execute(
            "INSERT INTO harvest_executions VALUES ('wrapped', ?1, 'RUNNING', ?2, NULL, NULL)",
            rusqlite::params![WORKFLOW_NAME, task("\u{a0}do it\u{3000}")],
        )
        .expect("the session is recorded");
    drop(writer);

    let reader = rusqlite::Connection::open(&db).expect("the database opens");
    let running = inspect::running(&reader, WORKFLOW_NAME).expect("the query runs");
    let unicode_goal = |exec: &str| {
        running
            .iter()
            .find(|row| row.exec_id == exec)
            .expect("the session is RUNNING")
            .has_goal
    };
    for blank in ["nbsp", "ideographic", "thin", "nel"] {
        assert!(
            !unicode_goal(blank),
            "a goal of only {blank} space must be refused, as `submit` refuses it"
        );
    }
    assert!(
        unicode_goal("wrapped"),
        "a real goal wrapped in that space is still a goal"
    );
}

/// A name that is not text is counted, and never rendered.
///
/// A filename is bytes on this platform and the model can only send a string,
/// so such an entry cannot be named through this tool. Rendering it with the
/// replacement character costs twice. The name addresses no file, and two
/// entries differing only in those bytes collapse into one. The second then
/// disappears from a walk that claims to reach everything.
#[test]
fn a_directory_entry_that_is_not_text_is_counted_and_not_named() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;

    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");
    std::fs::write(workspace.join("good.txt"), "x").expect("a readable name");
    // Two names that differ ONLY in a byte that is not UTF-8. Lossy rendering
    // maps both to the same string, and a set keyed on that loses one.
    for raw in [b"bad\xff.txt".as_slice(), b"bad\xfe.txt".as_slice()] {
        std::fs::write(workspace.join(OsStr::from_bytes(raw)), "x").expect("a byte name");
    }

    let body = tools::activity_body(workspace.clone());
    let raw = body(tool_request(
        &workspace,
        tools::TOOL_LIST_FILES,
        json!({ "path": "." }),
    ))
    .expect("a tool failure is a result, not an activity error");
    let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
    assert!(
        !outcome.is_error,
        "the listing must succeed: {}",
        outcome.output
    );

    assert!(
        !outcome.output.contains('\u{fffd}'),
        "a name that is not text must never be rendered: {}",
        outcome.output
    );
    assert!(
        outcome.output.lines().any(|line| line == "good.txt"),
        "a readable name is still listed: {}",
        outcome.output
    );
    // BOTH are accounted for. A lossy rendering would have collapsed them
    // into one line and reported nothing missing.
    assert!(
        outcome.output.contains("2 entries are not listed"),
        "both unnameable entries must be counted: {}",
        outcome.output
    );
}

/// A capped directory listing reaches every entry.
///
/// `read_dir` gives no order, so a truncated READ returns an arbitrary subset
/// and the same call returns that same subset again. Everything outside it is
/// unreachable to the model, which has only a path to ask with.
///
/// The selection is bounded instead: the smallest names after the cursor. The
/// page is therefore in order, and the cursor walks the whole directory.
#[test]
fn a_capped_listing_walks_the_whole_directory() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("the workspace is created");
    // Enough to overflow one page, named so that sorted order is known.
    let total = tools::MAX_ENTRIES + 5;
    for index in 0..total {
        std::fs::write(workspace.join(format!("f{index:04}.txt")), "x").expect("a file");
    }

    let body = tools::activity_body(workspace.clone());
    let call = |after: Option<&str>| -> String {
        let mut input = json!({ "path": "." });
        if let Some(after) = after {
            input["after"] = json!(after);
        }
        let raw = body(tool_request(&workspace, tools::TOOL_LIST_FILES, input))
            .expect("a tool failure is a result, not an activity error");
        let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
        assert!(
            !outcome.is_error,
            "the listing must succeed: {}",
            outcome.output
        );
        outcome.output
    };

    let first = call(None);
    let named: Vec<&str> = first.lines().filter(|line| line.starts_with('f')).collect();
    assert_eq!(
        named.len(),
        tools::MAX_ENTRIES,
        "the first page holds one page of entries"
    );
    assert_eq!(
        named[0], "f0000.txt",
        "the page is the SMALLEST names, and not an arbitrary subset"
    );
    assert!(
        first.contains("after"),
        "a truncated listing must name the cursor that continues it: {first}"
    );

    // The cursor reaches the entries the first page left out, which a capped
    // read with no cursor could never do.
    let last = named.last().expect("the page is not empty");
    let second = call(Some(last));
    let rest: Vec<&str> = second
        .lines()
        .filter(|line| line.starts_with('f'))
        .collect();
    assert_eq!(rest.len(), 5, "the rest of the directory is reachable");
    assert_eq!(
        rest[0],
        format!("f{:04}.txt", tools::MAX_ENTRIES),
        "the second page starts after the cursor"
    );
}

/// A socket path a terminal would rewrite is refused.
///
/// Being UTF-8 is not enough. Every printed line leaves through `visible`,
/// which rewrites a character a terminal would act on. A path holding one is
/// printed as an escape, and the copied command names another socket.
#[test]
fn a_socket_path_the_terminal_would_rewrite_is_refused() {
    use clap::Parser;

    for raw in [
        "/tmp/a\u{1b}[2K.sock",
        "/tmp/a\u{202e}b.sock",
        "/tmp/a\u{0}b.sock",
    ] {
        let cli = crate::Cli::try_parse_from(["agentd", "--socket", raw, "list"])
            .expect("clap accepts the text; the refusal is the daemon's own");
        let refused = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime")
            .block_on(crate::run(cli));
        let message = refused.expect_err("a path the renderer rewrites must be refused");
        assert!(
            message.contains("act on"),
            "the refusal must name the reason: {message}"
        );
    }

    // An ordinary path is still served, so the check refuses only what the
    // renderer would rewrite.
    let plain = crate::Cli::try_parse_from(["agentd", "--socket", "/tmp/plain.sock", "list"])
        .expect("clap accepts the path");
    let answered = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime")
        .block_on(crate::run(plain));
    let message = answered.expect_err("no daemon listens there");
    assert!(
        message.contains("cannot reach the daemon"),
        "an ordinary path must reach the connect attempt: {message}"
    );
}

/// A socket is replaced only when nothing is proved to be listening.
///
/// The reclaim removes a name and binds over it. Doing that to a LIVE socket
/// leaves the daemon behind it running with nothing able to reach it. Only an
/// answer that proves the name has no listener may license the removal.
///
/// A refusal and a missing entry prove it. A permission error does not, and
/// that is the reported case: another user's live socket in a shared
/// directory. Exhausted file descriptors have the same shape on a socket this
/// daemon owns.
#[test]
fn only_a_proven_absence_licenses_a_reclaim() {
    use std::io::{Error, ErrorKind};

    for proof in [ErrorKind::ConnectionRefused, ErrorKind::NotFound] {
        assert!(
            daemon::proves_nothing_listens(&Error::new(proof, "probe")),
            "{proof:?} proves the name has no listener"
        );
    }
    // Every other answer leaves the question open, so the name is not known
    // to be free and the socket must stay.
    for open in [
        ErrorKind::PermissionDenied,
        ErrorKind::ConnectionAborted,
        ErrorKind::TimedOut,
        ErrorKind::WouldBlock,
        ErrorKind::Other,
    ] {
        assert!(
            !daemon::proves_nothing_listens(&Error::new(open, "probe")),
            "{open:?} does not prove the name has no listener"
        );
    }
}

/// A restored wait is the ARMED one, and not one already answered.
///
/// Two tables decide this, and the backend's own rule is that an armed but
/// unfired timer proves a wait. A fired timer is an approval that ran out of
/// time, and a timed-out wait carries no answer either. Reading every timer
/// would therefore restore the EXPIRED call of an earlier turn.
///
/// A decision is staged in `harvest_signals` when it is sent, and its event
/// is appended later. A daemon that stopped between the two holds a decision
/// that wins on the next drive, so the wait must not come back.
#[test]
fn a_restored_wait_is_armed_and_unanswered() {
    // A fixed clock, so a deadline can be placed on either side of it.
    const NOW: i64 = 1_000;

    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("waits.db");
    let conn = rusqlite::Connection::open(&db).expect("the database opens");
    conn.execute_batch(
        "CREATE TABLE harvest_timers (timer_id TEXT, exec_id TEXT, fire_at INTEGER, \
         fired INTEGER NOT NULL DEFAULT 0, arm_seq INTEGER, \
         PRIMARY KEY (exec_id, timer_id)); \
         CREATE TABLE harvest_events (exec_id TEXT, seq INTEGER, event_json TEXT, \
         PRIMARY KEY (exec_id, seq)); \
         CREATE TABLE harvest_signals (signal_seq INTEGER PRIMARY KEY AUTOINCREMENT, \
         exec_id TEXT NOT NULL, name TEXT NOT NULL, payload_json TEXT NOT NULL, \
         delivered INTEGER NOT NULL DEFAULT 0, received_at INTEGER NOT NULL DEFAULT 0)",
    )
    .expect("the fixture tables are created");

    let expired = "tool_approval:1:0:toolu_first";
    let armed = "tool_approval:2:0:toolu_second";
    let overdue = "tool_approval:3:0:toolu_third";
    // The earlier call timed out, so its timer stays behind as FIRED. The
    // current call waits on an armed timer.
    conn.execute(
        "INSERT INTO harvest_timers VALUES (?1, 'e', 10, 1, 1)",
        [format!("__signal_timeout:1:{expired}")],
    )
    .expect("the expired timer is recorded");
    conn.execute(
        "INSERT INTO harvest_timers VALUES (?1, 'e', 9999, 0, 2)",
        [format!("__signal_timeout:2:{armed}")],
    )
    .expect("the armed timer is recorded");

    let found = inspect::outstanding_signal(&conn, "e", NOW)
        .expect("the wait query runs")
        .expect("an armed wait must be reported");
    assert_eq!(
        found, armed,
        "the armed wait must be restored, and not the timed-out one"
    );

    // A daemon stopped PAST a deadline has had no drive in which to mark the
    // timer fired, so an overdue wait still reads as armed. Restoring it
    // would print a token that `approve` refuses every time.
    conn.execute("DELETE FROM harvest_timers WHERE exec_id = 'e'", [])
        .expect("the timers are cleared");
    conn.execute(
        "INSERT INTO harvest_timers VALUES (?1, 'e', 500, 0, 3)",
        [format!("__signal_timeout:3:{overdue}")],
    )
    .expect("the overdue timer is recorded");
    let gone = inspect::outstanding_signal(&conn, "e", NOW).expect("the wait query runs");
    assert!(
        gone.is_none(),
        "an overdue wait must not be restored, and got {gone:?}"
    );

    // The same timer with its deadline ahead is restored, so the filter is
    // the deadline and not the row.
    conn.execute(
        "UPDATE harvest_timers SET fire_at = 9999 WHERE exec_id = 'e'",
        [],
    )
    .expect("the deadline is moved ahead");
    let ahead = inspect::outstanding_signal(&conn, "e", NOW)
        .expect("the wait query runs")
        .expect("a wait with time left must be restored");
    assert_eq!(
        ahead, overdue,
        "the wait with time left is the one reported"
    );

    // Back to the armed pair for the answer checks below.
    conn.execute("DELETE FROM harvest_timers WHERE exec_id = 'e'", [])
        .expect("the timers are cleared");
    conn.execute(
        "INSERT INTO harvest_timers VALUES (?1, 'e', 9999, 0, 2)",
        [format!("__signal_timeout:2:{armed}")],
    )
    .expect("the armed timer is recorded");

    // A decision sent but not yet taken up lives only in the staged table.
    // The wait must not come back over it, or a second answer would be taken
    // for a call that is already decided.
    conn.execute(
        "INSERT INTO harvest_signals (exec_id, name, payload_json) VALUES ('e', ?1, '{}')",
        [armed],
    )
    .expect("the decision is staged");
    let staged = inspect::outstanding_signal(&conn, "e", NOW).expect("the wait query runs");
    assert!(
        staged.is_none(),
        "a staged decision ends the wait, and got {staged:?}"
    );

    // Once the workflow takes the decision up, the row is marked delivered
    // and the event carries it. The wait stays closed.
    conn.execute("UPDATE harvest_signals SET delivered = 1", [])
        .expect("the decision is taken up");
    let event = json!({
        "type": "SignalReceived",
        "data": { "signal_name": armed, "payload": { "approved": true } },
    });
    conn.execute(
        "INSERT INTO harvest_events VALUES ('e', 0, ?1)",
        [event.to_string()],
    )
    .expect("the delivery is appended");
    let done = inspect::outstanding_signal(&conn, "e", NOW).expect("the wait query runs");
    assert!(
        done.is_none(),
        "a delivered decision ends the wait, and got {done:?}"
    );
}

/// A restarted daemon knows what a parked session awaits before it serves.
///
/// The parked state was empty until the first drive, while the socket already
/// accepted requests and readiness was announced. A decision that arrived in
/// that window was refused as a session that is not waiting. That is the
/// sequence the restart recipe describes.
///
/// The wait is durable, so it is read rather than driven. This drives the
/// reconstruction itself. The window it closes is a race, so an end-to-end
/// test of it would pass either way on a lucky schedule.
#[tokio::test(flavor = "multi_thread")]
async fn a_parked_wait_is_read_from_the_database() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("agentd.db");
    let socket = dir.path().join("agentd.sock");
    let served = tokio::spawn(daemon::serve(daemon::Options {
        db: db.clone(),
        socket: socket.clone(),
        workspace: dir.path().join("workspace"),
        model: claude::DEFAULT_MODEL.to_string(),
        max_tokens: claude::DEFAULT_MAX_TOKENS,
        tick: Duration::from_millis(50),
        api_key: None,
    }));
    await_daemon(&socket).await;
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
    await_parked(&socket, &execution_id).await;

    // The token the operator would approve, read while the daemon still holds
    // its own parked state.
    let view = protocol::call(
        &socket,
        &Request::Status {
            execution_id: execution_id.clone(),
            full: false,
        },
    )
    .await
    .expect("the status is answered");
    let Response::Session { session } = view else {
        panic!("unexpected answer: {view:?}");
    };
    let token = session.pending.expect("the call is pending").token;

    served.abort();
    drop(served.await);

    // A fresh reader, as a restarted daemon opens. The wait must be readable
    // with no drive at all, and it must be the SAME token.
    let reader = inspect::open(&db).expect("the database opens");
    let now_ms = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the clock is after the epoch")
            .as_millis(),
    )
    .expect("the clock is in range");
    let waited = inspect::outstanding_signal(&reader, &execution_id, now_ms)
        .expect("the wait query runs")
        .expect("a parked session must report its wait");
    assert_eq!(
        waited, token,
        "the wait read from the database must be the one the operator approves"
    );

    // A signal the previous daemon delivered before it stopped ends the wait.
    // The timer can outlive that, so a timer alone would report a wait that is
    // over, and this is an approval gate.
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    let next: i64 = writer
        .query_row(
            "SELECT COALESCE(MAX(seq), -1) + 1 FROM harvest_events WHERE exec_id = ?1",
            [&execution_id],
            |row| row.get(0),
        )
        .expect("the log is read");
    let delivered = json!({
        "type": "SignalReceived",
        "data": { "signal_name": waited, "payload": { "approved": true } },
    });
    writer
        .execute(
            "INSERT INTO harvest_events (exec_id, seq, event_json) VALUES (?1, ?2, ?3)",
            rusqlite::params![&execution_id, next, delivered.to_string()],
        )
        .expect("the delivery is appended");
    drop(writer);

    let after =
        inspect::outstanding_signal(&reader, &execution_id, now_ms).expect("the wait query runs");
    assert!(
        after.is_none(),
        "a delivered signal ends the wait, and got {after:?}"
    );
}

/// A capped listing stays reachable through its cursor.
///
/// The listing is capped so an old database cannot be read whole into memory.
/// Without a cursor that cap HIDES rows. An old session still waiting for a
/// decision becomes unreachable once enough newer sessions arrive, unless the
/// operator kept its execution id.
#[test]
fn a_listing_walks_back_through_its_cursor() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("listing.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    writer
        .execute(
            "CREATE TABLE harvest_executions (exec_id TEXT, workflow_name TEXT, \
             state TEXT, input_json TEXT, output_json TEXT, error TEXT)",
            [],
        )
        .expect("the fixture table is created");
    for index in 0..5 {
        let task = json!({
            "goal": format!("goal {index}"),
            "max_turns": 4,
            "approval_timeout_secs": 300,
            "workspace": "/tmp/w",
            "model": claude::OFFLINE_MODEL,
        });
        writer
            .execute(
                "INSERT INTO harvest_executions VALUES (?1, ?2, 'COMPLETED', ?3, NULL, NULL)",
                rusqlite::params![format!("exec-{index}"), WORKFLOW_NAME, task.to_string()],
            )
            .expect("the session is recorded");
    }
    drop(writer);

    let reader = rusqlite::Connection::open(&db).expect("the database opens");
    let all = inspect::executions(&reader, WORKFLOW_NAME, None).expect("the listing reads");
    assert_eq!(all.len(), 5, "the fixture holds five sessions");
    assert_eq!(
        all[0].exec_id, "exec-0",
        "the listing reads oldest first: {:?}",
        all[0].exec_id
    );

    // Walk back from the third row. The page before it is the first two, and
    // nothing newer.
    let cursor = all[2].row;
    let older = inspect::executions(&reader, WORKFLOW_NAME, Some(cursor)).expect("the page reads");
    let named: Vec<&str> = older.iter().map(|row| row.exec_id.as_str()).collect();
    assert_eq!(
        named,
        vec!["exec-0", "exec-1"],
        "the cursor must read the rows BEFORE it"
    );

    // The oldest row has nothing before it, which is how a walk ends.
    let none =
        inspect::executions(&reader, WORKFLOW_NAME, Some(all[0].row)).expect("the page reads");
    assert!(
        none.is_empty(),
        "the oldest row ends the walk, and got {} rows",
        none.len()
    );
}

/// A full page carries the cursor that reads the page before it.
///
/// The query and the renderer are covered above and below. This covers the
/// DAEMON deciding to send the cursor, which neither of those reaches: a
/// reverted cursor left both of them passing.
#[test]
fn a_full_page_carries_its_cursor() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("full.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    writer
        .execute(
            "CREATE TABLE harvest_executions (exec_id TEXT, workflow_name TEXT, \
             state TEXT, input_json TEXT, output_json TEXT, error TEXT)",
            [],
        )
        .expect("the fixture table is created");

    // One session past the cap, which is what makes the page full.
    let rows = inspect::MAX_LISTED_SESSIONS + 1;
    let task = json!({
        "goal": "tidy the notes",
        "max_turns": 4,
        "approval_timeout_secs": 300,
        "workspace": "/tmp/w",
        "model": claude::OFFLINE_MODEL,
    })
    .to_string();
    for index in 0..rows {
        writer
            .execute(
                "INSERT INTO harvest_executions VALUES (?1, ?2, 'COMPLETED', ?3, NULL, NULL)",
                rusqlite::params![format!("exec-{index}"), WORKFLOW_NAME, task],
            )
            .expect("the session is recorded");
    }
    drop(writer);

    let reader = rusqlite::Connection::open(&db).expect("the database opens");
    let parked = daemon::Parked::new();
    let (shown, more, older) =
        daemon::sessions(&reader, &parked, false, None).expect("the listing reads");
    assert_eq!(
        shown.len(),
        inspect::MAX_LISTED_SESSIONS as usize,
        "a full page shows the cap and no more"
    );
    assert!(more, "the table holds more than one page");
    let cursor = older.expect("a full page must carry its cursor");

    // The cursor reads the page BEFORE this one, so it must find the session
    // the page left out and not repeat one it showed.
    let before =
        inspect::executions(&reader, WORKFLOW_NAME, Some(cursor)).expect("the earlier page reads");
    assert_eq!(
        before.len(),
        1,
        "the cursor must reach the one session this page omitted"
    );
    assert_eq!(
        before[0].exec_id, "exec-0",
        "the omitted session is the oldest one: {}",
        before[0].exec_id
    );
}

/// The listing's own continuation command carries the socket.
#[test]
fn the_listing_cursor_reaches_the_daemon_that_printed_it() {
    let view = protocol::SessionView {
        execution_id: "01JCEXEC".to_string(),
        goal: "tidy the notes".to_string(),
        state: "RUNNING".to_string(),
        blocked_on: None,
        pending: None,
        answer: None,
        error: None,
    };
    let rendered = crate::rendered_lines(
        &Response::Sessions {
            sessions: vec![view],
            more: true,
            older: Some(41),
        },
        Path::new("/run/agentd/project-b.sock"),
    );
    let hint = rendered
        .iter()
        .find(|line| line.contains("--before"))
        .expect("the continuation command is printed");
    assert!(
        hint.contains("--socket /run/agentd/project-b.sock") && hint.contains("--before 41"),
        "the continuation command must reach the same daemon: {hint}"
    );
}

/// The flush chain holds every directory above a write.
///
/// An entry is durable only after the directory naming it is flushed. A write
/// that creates nested directories therefore needs each one above it flushed,
/// up to the workspace root.
///
/// The workspace may be named through a symlink. Both sides of the comparison
/// must be canonical. The chain otherwise collapses to the target's own
/// directory, and a crash loses the file while the history says the write
/// finished.
#[test]
fn the_flush_chain_reaches_the_root_through_a_symlink() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let real = dir.path().join("real");
    std::fs::create_dir_all(real.join("a/b")).expect("the tree is created");
    let link = dir.path().join("link");
    std::os::unix::fs::symlink(&real, &link).expect("the symlink is made");

    // The workspace is named through the link, which is how an operator who
    // keeps a stable path to a moving directory names it.
    let chain = tools::directories_to_flush(&link.join("a/b/notes.md"), &link);
    let canonical = real.canonicalize().expect("the real path resolves");

    assert_eq!(
        chain.len(),
        3,
        "the chain must hold b, a and the root: {chain:?}"
    );
    assert!(
        chain[0].ends_with("a/b") && chain[1].ends_with("a") && chain[2] == canonical,
        "the chain must run deepest first up to the root: {chain:?}"
    );

    // The directory holding the entry that names `b` is the one a collapsed
    // chain leaves out, so it is named here on its own.
    assert!(
        chain.iter().any(|entry| entry.ends_with("a")),
        "the parent that names the deepest directory must be flushed: {chain:?}"
    );

    // The same workspace named directly gives the same chain, so the fix is
    // about the spelling and not about the walk.
    let direct = tools::directories_to_flush(&real.join("a/b/notes.md"), &real);
    assert_eq!(
        direct, chain,
        "a link and the real path must flush the same directories"
    );
}

/// The reply search stops at the newest reply.
///
/// The `stop_reason` test is not indexed. The database reads and decodes each
/// row to know whether it matches, and `LIMIT` counts only the rows that DO.
/// A page of many therefore reads backward past older replies until it has
/// that many. On a long history that is the whole log for one `status`.
///
/// This asserts the property the page size buys: a page of one returns the
/// NEWEST reply and no older one. What it cannot assert is the row count the
/// database read to get there, which needs a progress hook this build does
/// not carry.
#[test]
fn the_reply_search_reads_no_further_than_the_newest_reply() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("replies.db");
    let writer = rusqlite::Connection::open(&db).expect("the database opens");
    writer
        .execute(
            "CREATE TABLE harvest_events (exec_id TEXT, seq INTEGER, event_json TEXT, \
             PRIMARY KEY (exec_id, seq))",
            [],
        )
        .expect("the fixture table is created");

    // Four turns, each a reply and then the events of its tool calls. The
    // newest reply sits behind the tool events of its own turn, and three
    // older replies sit behind those.
    let mut seq = 0_i64;
    for turn in 0..4 {
        let reply = json!({
            "type": "ActivityCompleted",
            "data": { "output": {
                "stop_reason": "tool_use",
                "tool_calls": [{
                    "id": format!("toolu_{turn}"),
                    "name": "write_file",
                    "input": { "path": "notes.md", "content": "x" },
                }],
            }},
        });
        writer
            .execute(
                "INSERT INTO harvest_events VALUES ('e', ?1, ?2)",
                rusqlite::params![seq, reply.to_string()],
            )
            .expect("the reply is recorded");
        seq += 1;
        for _ in 0..20 {
            let result = json!({
                "type": "ActivityCompleted",
                "data": { "output": { "ok": true } },
            });
            writer
                .execute(
                    "INSERT INTO harvest_events VALUES ('e', ?1, ?2)",
                    rusqlite::params![seq, result.to_string()],
                )
                .expect("the tool event is recorded");
            seq += 1;
        }
    }
    drop(writer);

    let reader = rusqlite::Connection::open(&db).expect("the database opens");
    let page = inspect::reply_calls(&reader, "e", None, 1).expect("the page reads");
    assert_eq!(page.len(), 1, "a page of one must hold one reply: {page:?}");
    assert_eq!(
        page[0].0, 63,
        "the page must hold the NEWEST reply and stop there"
    );

    // The whole log holds four replies, so a larger page walks back over the
    // older three. That is the reading this page size avoids.
    let wide = inspect::reply_calls(&reader, "e", None, 64).expect("the page reads");
    assert_eq!(
        wide.len(),
        4,
        "a wide page reads back to the end of the log: {wide:?}"
    );
}

#[test]
fn a_response_body_is_read_under_a_byte_cap() {
    // `text()` buffers whatever arrives. The request timeout bounds the TIME
    // a body may take, and not the BYTES it carries. One answer could
    // therefore spend the daemon's memory and stall every session.
    let cap = claude::MAX_BODY_BYTES;

    // A body that ends under the cap is read whole.
    let mut body = Vec::new();
    assert!(
        !claude::push_capped(&mut body, b"{\"ok\":true}"),
        "a small chunk must not end the read"
    );
    assert_eq!(body.len(), 11, "a small chunk is taken whole");

    // A chunk that STRADDLES the cap is cut at it, and the read ends. This is
    // the arithmetic worth testing: the body must hold exactly the cap, and
    // not the cap plus the overshoot.
    let mut body = vec![b'a'; cap - 10];
    assert!(
        claude::push_capped(&mut body, &vec![b'b'; 4096]),
        "a chunk past the cap must end the read"
    );
    assert_eq!(body.len(), cap, "the body must hold exactly the cap");

    // A body already at the cap takes nothing more and still ends.
    let mut body = vec![b'a'; cap];
    assert!(
        claude::push_capped(&mut body, b"more"),
        "a full body must end the read"
    );
    assert_eq!(body.len(), cap, "a full body grows no further");

    // The cap is above the recorded payload cap of 2 MiB, so no reply that
    // could be recorded is refused for its size.
    assert!(
        cap > 2 * 1024 * 1024,
        "the cap must not refuse a recordable reply: {cap}"
    );
}

#[test]
fn a_key_that_cannot_be_a_header_is_refused() {
    // A key read out of a file can carry an interior newline. The trim on the
    // way in removes the ends and not the middle. The value therefore counts
    // as present, and the daemon would run live against it.
    let interior = "sk-ant-aa\nbb";
    assert_eq!(
        crate::usable_key(interior),
        Some(interior.to_string()),
        "the trim leaves an interior newline, which is why this check exists"
    );

    // The key is tested as the thing it becomes. Reqwest is asked, rather
    // than this example guessing which bytes a header value accepts.
    for bad in ["sk-ant-aa\nbb", "sk-ant-aa\rbb", "sk-ant-aa\u{0}bb"] {
        let (_, signal) = crate::shutdown::channel();
        let refused = claude::ModelConfig::new(
            Some(bad.to_string()),
            claude::DEFAULT_MODEL,
            claude::DEFAULT_MAX_TOKENS,
            signal,
        );
        let Err(message) = refused else {
            panic!("{bad:?} cannot be a header value and must be refused");
        };
        assert!(
            message.contains("header"),
            "the refusal must name the reason: {message}"
        );
    }

    // A key of ordinary characters is accepted, so the check refuses only
    // what the header refuses.
    let (_, signal) = crate::shutdown::channel();
    assert!(
        claude::ModelConfig::new(
            Some("sk-ant-api03-aAbB09_-".to_string()),
            claude::DEFAULT_MODEL,
            claude::DEFAULT_MAX_TOKENS,
            signal,
        )
        .is_ok(),
        "an ordinary key must be accepted"
    );
}

#[test]
fn a_socket_path_that_cannot_be_printed_is_refused() {
    use clap::Parser;
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    // A path is bytes on this platform. Every command prints follow-up
    // commands naming the socket, and a byte that is not UTF-8 cannot be
    // written into one of those lines unchanged. The printed line would then
    // name another socket, or none.
    let raw = OsString::from_vec(vec![b'/', b't', b'm', b'p', b'/', 0xff, b'.', b's']);
    let cli = crate::Cli::try_parse_from([
        OsString::from("agentd"),
        OsString::from("--socket"),
        raw,
        OsString::from("list"),
    ])
    .expect("clap accepts the bytes; the refusal is the daemon's own");

    let refused = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime")
        .block_on(crate::run(cli));
    let message = refused.expect_err("a path that cannot be printed must be refused");
    assert!(
        message.contains("not UTF-8"),
        "the refusal must name the reason: {message}"
    );
}

/// The parked state is built before the socket accepts anything.
///
/// This is an ORDERING, and the window it closes is a race. No end-to-end
/// test fails reliably without it, because a lucky schedule lets the first
/// drive win and the answer comes out right anyway. The reconstruction query
/// has its own test. This pins the daemon using it, and using it first.
///
/// Reverting the startup rebuild left the query's own test passing, which is
/// why this guard exists rather than a comment promising the order.
#[test]
fn the_parked_state_is_rebuilt_before_the_socket_is_bound() {
    let daemon = include_str!("daemon.rs");
    let built = daemon
        .find("Parked::new()")
        .expect("the daemon builds its parked state");
    let bound = daemon
        .find("bind(&options.socket)")
        .expect("the daemon binds its socket");
    assert!(
        built < bound,
        "the parked state must be built before the socket accepts anything"
    );
    // Built early and left empty would pass the order and fix nothing.
    let read = daemon
        .find("outstanding_signal")
        .expect("the daemon reads each durable wait");
    assert!(
        read < bound,
        "each durable wait must be read before the socket accepts anything"
    );
}

/// The daemon builds no command an operator can copy.
///
/// Five separate findings were one defect: a command formatted in the daemon,
/// which cannot know which socket the client asked. Fixing them one at a time
/// left the next one to be found. This reads the source as data, so a command
/// added to the daemon fails here rather than in review.
#[test]
fn no_operator_command_is_built_in_the_daemon() {
    let daemon = include_str!("daemon.rs");
    assert!(
        !daemon.contains("`agentd "),
        "the daemon must send data and let the client render the command"
    );
    // The guard is only worth having if the pattern it looks for is the one
    // the client actually uses.
    assert!(
        include_str!("main.rs").contains("`agentd "),
        "the client is where these commands belong"
    );
}

#[test]
fn a_workspace_that_is_a_file_is_refused() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("a temporary directory");
    let file = dir.path().join("not-a-directory");
    std::fs::write(&file, "I am a file").expect("the fixture is written");

    // A file with the owner's execute bit reads as enterable by mode alone.
    // Nothing is missing above it, so nothing would be created, and the
    // daemon would start over a workspace no tool can use.
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o700))
        .expect("the fixture is made executable");
    let refusal = tools::create_enterable(&file).expect_err("a file must be refused");
    assert_eq!(
        refusal.kind(),
        std::io::ErrorKind::NotADirectory,
        "the refusal must say what is wrong: {refusal}"
    );

    // Without the owner bits the repair path would have changed the mode of
    // a file nobody asked this daemon to touch.
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600))
        .expect("the fixture is made unexecutable");
    tools::create_enterable(&file).expect_err("a file must still be refused");
    let mode = std::fs::metadata(&file)
        .expect("the file exists")
        .permissions()
        .mode()
        & 0o7777;
    assert_eq!(mode, 0o600, "the file's mode must be untouched");

    // A file BELOW the target is refused too, rather than created through.
    let under = file.join("child");
    tools::create_enterable(&under).expect_err("a path through a file must be refused");
}

#[test]
fn a_key_of_whitespace_is_not_a_key() {
    // A key of whitespace would count as present, and the daemon would run
    // live against it. Every turn would fail at the API. An absent key runs
    // the offline stub instead, which is the quieter and correct outcome.
    for blank in ["", " ", "\t\n", "   "] {
        assert_eq!(
            crate::usable_key(blank),
            None,
            "a key of whitespace must read as no key: {blank:?}"
        );
    }

    // An operator commonly reads a key out of a file and keeps the newline.
    // A header carries that byte to the API, which rejects it.
    assert_eq!(
        crate::usable_key("sk-ant-example\n").as_deref(),
        Some("sk-ant-example"),
        "a key keeps none of the whitespace around it"
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

    // The id becomes part of the approval token, and the status view prints
    // that token unquoted in an `agentd approve` command line. An operator
    // copies that line. A shell drops a trailing space and splits an inner
    // one, so the copied token no longer matches the staged name. The write
    // then stays blocked until its deadline, with no sign of why.
    // A shell reads what the operator copies. An id of `x;reboot` is not a
    // token at all under that reading: it is a command, and the model chose
    // it. Whitespace, a quote, a backtick, a pipe and a glob mangle or obey
    // the line in the same way. A leading dash reads as a flag.
    for mangled in [
        " ", "\t", "toolu_a ", " toolu_a", "toolu a", "x;reboot", "$(id)", "`id`", "a|b", "a&b",
        "a>b", "a*", "a'b", "a\"b", "-toolu_a",
    ] {
        assert!(
            !claude::has_addressable_calls(&reply(vec![call(mangled)])),
            "an id a shell would read differently must be refused: {mangled:?}"
        );
    }

    // The shape the API actually mints stays acceptable.
    for minted in ["toolu_01A09q90qw90lq917835lq9", "call-1.2_3"] {
        assert!(
            claude::has_addressable_calls(&reply(vec![call(minted)])),
            "a minted id must be addressable: {minted:?}"
        );
    }

    // An accepted id therefore always yields a token an operator can copy.
    let token = session::approval_signal(1, 0, "toolu_01A09q90qw90lq917835lq9");
    assert!(
        token
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | ':')),
        "an accepted id must give a token a shell leaves alone: {token:?}"
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
async fn the_startup_and_status_queries_read_only_what_they_need() {
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
        running
            .iter()
            .map(|session| session.exec_id.clone())
            .collect::<Vec<_>>(),
        vec![parked.to_string()],
        "only the parked session is drivable"
    );
    // The startup check compares the workspace and the model, so the query
    // carries those two FIELDS and not the whole recorded task. A goal can
    // approach the control-request cap, and a restart reads every parked
    // session. Reading the tasks whole could spend the daemon's memory before
    // it is ready.
    let first = running.first().expect("the parked session is listed");
    assert_eq!(
        first.workspace.as_deref(),
        Some(workspace.to_str().expect("the workspace path is UTF-8")),
        "the running row must carry the workspace it was recorded against"
    );
    assert_eq!(
        first.model.as_deref(),
        Some(claude::OFFLINE_MODEL),
        "the running row must carry the model it was recorded against"
    );
    assert!(
        !format!("{:?} {:?}", first.workspace, first.model).contains("summarise the workspace"),
        "the running row must not carry the goal"
    );
    assert_eq!(
        crate::inspect::executions(&reader, WORKFLOW_NAME, None)
            .expect("the listing answers")
            .len(),
        2,
        "both sessions are still listed for the operator"
    );

    // `status` names one session, so it reads one row rather than building a
    // view of every session that ever ran.
    let one = |exec: ExecutionId| {
        crate::inspect::execution(&reader, WORKFLOW_NAME, &exec.to_string())
            .expect("the single-row query answers")
    };
    assert_eq!(
        one(done).map(|row| row.state),
        Some("COMPLETED".to_string()),
        "the finished session is readable by id"
    );
    assert_eq!(
        one(parked).map(|row| row.state),
        Some("RUNNING".to_string()),
        "the parked session is readable by id"
    );
    assert!(
        crate::inspect::execution(
            &reader,
            WORKFLOW_NAME,
            "00000000-0000-4000-8000-000000000000",
        )
        .expect("the single-row query answers")
        .is_none(),
        "an id that names no session reads as nothing"
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
        let scratch = tools::scratch_name(name, 4_294_967_295, u64::MAX, 15);
        assert!(
            scratch.len() <= name.len().max(96),
            "`{scratch}` is longer than the name it replaces"
        );
        assert!(scratch.len() <= 255, "`{scratch}` is over the limit");
    }
    assert!(
        tools::scratch_name("notes.md", 123, 1, 0).contains("notes.md"),
        "a short target must keep its stem"
    );

    // The name carries a value that does not repeat across restarts. A daemon
    // that always starts as pid 1 would otherwise retry the same sixteen names
    // after a crash between the create and the rename.
    assert_ne!(
        tools::scratch_nonce(),
        tools::scratch_nonce(),
        "the scratch nonce must not repeat"
    );
    assert_ne!(
        tools::scratch_name("notes.md", 1, 1, 0),
        tools::scratch_name("notes.md", 1, 2, 0),
        "a different nonce must give a different name"
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
        claude::OFFLINE_MODEL,
        claude::DEFAULT_MAX_TOKENS,
        crate::shutdown::channel().1,
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
        claude::OFFLINE_MODEL,
        claude::DEFAULT_MAX_TOKENS,
        crate::shutdown::channel().1,
    )
    .expect("the stub needs no key");
    assert_eq!(offline.identity(), claude::OFFLINE_MODEL);

    let live = claude::ModelConfig::new(
        Some("sk-not-a-real-key".to_string()),
        claude::DEFAULT_MODEL,
        claude::DEFAULT_MAX_TOKENS,
        crate::shutdown::channel().1,
    )
    .expect("a real model with a key is ordinary");
    assert_eq!(live.identity(), claude::DEFAULT_MODEL);
}

#[tokio::test]
async fn the_shutdown_flag_is_never_missed() {
    // A waiter that starts AFTER the signal must not wait forever. The model
    // request waits on this flag from inside a blocking call, so it starts
    // late by construction.
    let (trigger, signal) = crate::shutdown::channel();
    trigger.send_replace(true);
    let mut late = signal.clone();
    tokio::time::timeout(Duration::from_secs(5), late.raised())
        .await
        .expect("a raised flag must not make a late waiter wait");

    // A waiter that starts first is released when the flag goes up.
    let (trigger, signal) = crate::shutdown::channel();
    let mut early = signal.clone();
    let waiting = tokio::spawn(async move { early.raised().await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    trigger.send_replace(true);
    tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .expect("the waiter must be released")
        .expect("the waiter finishes");

    // A trigger that is dropped reads as a stop. Its only holder is the task
    // that waits for the signal, so losing it means the daemon is going away.
    let (trigger, signal) = crate::shutdown::channel();
    let mut orphaned = signal.clone();
    drop(trigger);
    tokio::time::timeout(Duration::from_secs(5), orphaned.raised())
        .await
        .expect("a lost trigger must not make a waiter wait");
}

#[test]
fn a_written_file_never_keeps_set_id_bits() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("a temporary directory");
    let workspace = dir.path().to_path_buf();
    let target = workspace.join("helper.sh");
    std::fs::write(&target, "old").expect("the fixture is written");
    // A `setuid` script the agent is then asked to rewrite.
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o4755))
        .expect("the fixture is made set-user-id");

    let body = tools::activity_body(workspace.clone());
    let raw = body(tool_request(
        &workspace,
        tools::TOOL_WRITE_FILE,
        json!({ "path": "helper.sh", "content": "#!/bin/sh\necho mine\n" }),
    ))
    .expect("a tool failure is a result, not an activity error");
    let outcome: ToolOutcome = serde_json::from_value(raw).expect("the outcome decodes");
    assert!(
        !outcome.is_error,
        "the write must succeed: {}",
        outcome.output
    );

    // The new inode belongs to the daemon and the model chose its bytes. A
    // `setuid` file here would run as the daemon's user for anyone who could
    // execute it, and the approval never showed the mode.
    let mode = std::fs::metadata(&target)
        .expect("the target exists")
        .permissions()
        .mode()
        & 0o7777;
    assert_eq!(mode, 0o0755, "the set-user-id bit must not survive a write");
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
