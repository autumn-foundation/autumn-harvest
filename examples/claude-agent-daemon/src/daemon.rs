//! The daemon: one process, one database file, one writer.
//!
//! The process owns three things:
//!
//! 1. The [`SqliteRuntime`] — the only write handle to the database.
//! 2. A Unix socket — the control surface the CLI talks to.
//! 3. A drive tick — the poll the backend needs in place of `LISTEN`/`NOTIFY`.
//!
//! The main loop selects between a control command and the tick. Both take the
//! runtime by mutable reference, so exactly one of them runs at a time. That is
//! the single-writer contract made visible: while a model call is in flight,
//! the next command waits. A fleet that needs concurrent writers wants the
//! Postgres core instead.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use autumn_harvest_sqlite::{ExecutionId, RunState, SqliteRuntime};
use rusqlite::Connection;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot};

use crate::claude::{self, ModelConfig};
use crate::guard;
use crate::inspect::{self, ExecutionRow};
use crate::protocol::{Request, Response, SessionView};
use crate::session::{
    self, ApprovalDecision, SIGNAL_TOOL_APPROVAL, SessionReport, SessionTask, WORKFLOW_NAME,
};
use crate::tools;

/// How many control commands may queue while the runtime is busy.
const COMMAND_BACKLOG: usize = 32;

/// Everything the daemon needs to start.
pub struct Options {
    pub db: PathBuf,
    pub socket: PathBuf,
    pub workspace: PathBuf,
    pub model: String,
    pub max_tokens: u32,
    pub tick: Duration,
    /// `None` runs the offline stub model.
    pub api_key: Option<String>,
}

/// One control command plus the channel its answer goes back on.
type Job = (Request, oneshot::Sender<Response>);

/// Run the daemon until `Ctrl-C`.
///
/// # Errors
///
/// Returns an error if the database, the workspace, or the socket cannot be
/// opened, or if another daemon already holds the socket.
pub async fn serve(options: Options) -> Result<(), String> {
    std::fs::create_dir_all(&options.workspace).map_err(|e| {
        format!(
            "cannot create the workspace {}: {e}",
            options.workspace.display()
        )
    })?;

    // Take the per-database lock FIRST. The open below reclaims every task left
    // `RUNNING` by a dead process. A second daemon opening the same file would
    // reclaim a LIVE task, and its activity would run twice. The lock lives as
    // long as this call, and the kernel releases it if the process dies.
    let lock = guard::acquire(&options.db)?;

    let model = ModelConfig::new(options.api_key, options.model, options.max_tokens)?;
    let live = model.is_live();

    // Opening the file applies the schema and reclaims any task a previous
    // process left RUNNING. In-flight sessions resume by replay from here.
    let mut runtime = SqliteRuntime::open(&options.db)
        .map_err(|e| format!("cannot open {}: {e}", options.db.display()))?;
    runtime.register_workflow(&session::agent_session_info());
    runtime.register_activity(&session::claude_turn_info(), claude::activity_body(model));
    runtime.register_activity(
        &session::run_tool_info(),
        tools::activity_body(options.workspace.clone()),
    );

    let reader = inspect::open(&options.db)?;
    let listener = bind(&options.socket).await?;
    let (tx, mut rx) = mpsc::channel::<Job>(COMMAND_BACKLOG);
    tokio::spawn(accept_loop(listener, tx));

    tracing::info!(
        db = %options.db.display(),
        lock = %lock.path().display(),
        socket = %options.socket.display(),
        workspace = %options.workspace.display(),
        model = if live { "claude api" } else { "offline stub" },
        "agentd is ready",
    );
    if !live {
        tracing::warn!(
            "ANTHROPIC_API_KEY is not set, so the offline stub model is in use. \
             Set the key and restart to run against Claude."
        );
    }

    let mut blocked: HashMap<ExecutionId, String> = HashMap::new();
    let mut ticker = tokio::time::interval(options.tick);
    loop {
        tokio::select! {
            job = rx.recv() => {
                let Some((request, answer)) = job else { break };
                let response = handle(&mut runtime, &reader, &blocked, request);
                // A closed receiver means the client hung up. Nothing to do.
                drop(answer.send(response));
            }
            _ = ticker.tick() => {
                // The read is synchronous, so its borrow of the reader ends
                // here. That keeps this future `Send`. A `&Connection` is not
                // `Send`, because a SQLite connection is not `Sync`.
                let ready = running_sessions(&reader);
                for exec in ready {
                    drive_one(&mut runtime, exec, &mut blocked).await;
                }
            }
            result = tokio::signal::ctrl_c() => {
                if let Err(e) = result {
                    tracing::error!(error = %e, "cannot listen for Ctrl-C");
                }
                break;
            }
        }
    }

    tracing::info!("agentd is stopping; in-flight sessions resume on the next start");
    drop(std::fs::remove_file(&options.socket));
    Ok(())
}

/// Take the control socket, refusing to displace a live daemon.
async fn bind(socket: &Path) -> Result<UnixListener, String> {
    if socket.exists() {
        if UnixStream::connect(socket).await.is_ok() {
            return Err(format!("a daemon already listens on {}", socket.display()));
        }
        // The file outlived its process, so it is safe to replace.
        std::fs::remove_file(socket)
            .map_err(|e| format!("cannot remove the stale socket {}: {e}", socket.display()))?;
    }
    UnixListener::bind(socket).map_err(|e| format!("cannot listen on {}: {e}", socket.display()))
}

/// Accept connections and forward each request to the main loop.
async fn accept_loop(listener: UnixListener, tx: mpsc::Sender<Job>) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(serve_connection(stream, tx.clone()));
            }
            Err(e) => tracing::warn!(error = %e, "cannot accept a control connection"),
        }
    }
}

/// Read one request, wait for the answer, write it back.
async fn serve_connection(stream: UnixStream, tx: mpsc::Sender<Job>) {
    let (read_half, mut write_half) = stream.into_split();
    let mut line = String::new();
    if BufReader::new(read_half)
        .read_line(&mut line)
        .await
        .is_err()
    {
        return;
    }

    let response = match serde_json::from_str::<Request>(line.trim()) {
        Ok(request) => {
            let (answer_tx, answer_rx) = oneshot::channel();
            if tx.send((request, answer_tx)).await.is_err() {
                Response::Error {
                    message: "the daemon is shutting down".to_string(),
                }
            } else {
                answer_rx.await.unwrap_or_else(|_| Response::Error {
                    message: "the daemon dropped the request".to_string(),
                })
            }
        }
        Err(e) => Response::Error {
            message: format!("malformed request: {e}"),
        },
    };

    // An answer that cannot be encoded still gets a line, so the client reads a
    // reason instead of a closed connection.
    let mut encoded = serde_json::to_string(&response).unwrap_or_else(|e| {
        tracing::error!(error = %e, "cannot encode an answer");
        r#"{"status":"error","message":"the daemon cannot encode its answer"}"#.to_string()
    });
    encoded.push('\n');
    drop(write_half.write_all(encoded.as_bytes()).await);
    drop(write_half.flush().await);
}

/// Apply one control command.
fn handle(
    runtime: &mut SqliteRuntime,
    reader: &Connection,
    blocked: &HashMap<ExecutionId, String>,
    request: Request,
) -> Response {
    match request {
        Request::Submit {
            goal,
            max_turns,
            approval_timeout_secs,
        } => submit(runtime, goal, max_turns, approval_timeout_secs),
        Request::Status { execution_id } => match sessions(reader, blocked) {
            Ok(views) => views
                .into_iter()
                .find(|view| view.execution_id == execution_id)
                .map_or_else(
                    || Response::Error {
                        message: format!("no session {execution_id}"),
                    },
                    |session| Response::Session { session },
                ),
            Err(message) => Response::Error { message },
        },
        Request::List => match sessions(reader, blocked) {
            Ok(sessions) => Response::Sessions { sessions },
            Err(message) => Response::Error { message },
        },
        Request::History { execution_id } => history(runtime, &execution_id),
        Request::Approve {
            execution_id,
            approved,
            note,
        } => approve(runtime, &execution_id, approved, note),
    }
}

/// Start one session.
fn submit(
    runtime: &mut SqliteRuntime,
    goal: String,
    max_turns: u32,
    approval_timeout_secs: u64,
) -> Response {
    let task = SessionTask {
        goal,
        max_turns,
        approval_timeout_secs,
    };
    let input = match serde_json::to_value(task) {
        Ok(value) => value,
        Err(e) => {
            return Response::Error {
                message: format!("cannot encode the task: {e}"),
            };
        }
    };
    // The call records the start and returns at once. The drive tick runs the
    // first turn, so a slow model call never holds up the answer here.
    match runtime.start_workflow(WORKFLOW_NAME, input) {
        Ok(exec) => Response::Submitted {
            execution_id: exec.to_string(),
        },
        Err(e) => Response::Error {
            message: format!("cannot start the session: {e}"),
        },
    }
}

/// Deliver one approval decision.
fn approve(
    runtime: &mut SqliteRuntime,
    execution_id: &str,
    approved: bool,
    note: Option<String>,
) -> Response {
    let exec = match execution_id.parse::<ExecutionId>() {
        Ok(exec) => exec,
        Err(e) => {
            return Response::Error {
                message: format!("`{execution_id}` is not an execution id: {e}"),
            };
        }
    };
    let decision = ApprovalDecision { approved, note };
    let payload = match serde_json::to_value(decision) {
        Ok(value) => value,
        Err(e) => {
            return Response::Error {
                message: format!("cannot encode the decision: {e}"),
            };
        }
    };
    match runtime.send_signal(exec, SIGNAL_TOOL_APPROVAL, payload) {
        Ok(()) => Response::Ack {
            detail: if approved {
                "approved".to_string()
            } else {
                "denied".to_string()
            },
        },
        Err(e) => Response::Error {
            message: format!("cannot deliver the decision: {e}"),
        },
    }
}

/// Report the recorded event log of one session.
fn history(runtime: &SqliteRuntime, execution_id: &str) -> Response {
    let exec = match execution_id.parse::<ExecutionId>() {
        Ok(exec) => exec,
        Err(e) => {
            return Response::Error {
                message: format!("`{execution_id}` is not an execution id: {e}"),
            };
        }
    };
    match runtime.load_history(exec) {
        Ok(events) => Response::History {
            events: events
                .iter()
                .enumerate()
                .map(|(index, event)| format!("{:>3}  {}", index + 1, event_label(event)))
                .collect(),
        },
        Err(e) => Response::Error {
            message: format!("cannot read the history: {e}"),
        },
    }
}

/// The `type` tag of one recorded event.
fn event_label(event: &autumn_harvest::WorkflowEvent) -> String {
    serde_json::to_value(event)
        .ok()
        .and_then(|value| {
            value
                .get("type")
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
        .unwrap_or_else(|| "unknown".to_string())
}

/// Project every execution row into an operator view.
fn sessions(
    reader: &Connection,
    blocked: &HashMap<ExecutionId, String>,
) -> Result<Vec<SessionView>, String> {
    Ok(inspect::executions(reader, WORKFLOW_NAME)?
        .into_iter()
        .map(|row| view(&row, blocked))
        .collect())
}

/// Build one operator view.
fn view(row: &ExecutionRow, blocked: &HashMap<ExecutionId, String>) -> SessionView {
    let goal = serde_json::from_str::<SessionTask>(&row.input_json)
        .map_or_else(|_| "<unreadable task>".to_string(), |task| task.goal);
    let answer = row
        .output_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<SessionReport>(raw).ok())
        .map(|report| {
            format!(
                "[{} after {} turns, {} tool calls] {}",
                report.stop, report.turns, report.tool_calls, report.answer
            )
        });
    let blocked_on = row
        .exec_id
        .parse::<ExecutionId>()
        .ok()
        .and_then(|exec| blocked.get(&exec).cloned());

    SessionView {
        execution_id: row.exec_id.clone(),
        goal,
        state: row.state.clone(),
        blocked_on,
        answer,
        error: row.error.clone(),
    }
}

/// Every session the tick must drive.
///
/// The backend has no push wake-up, so progress comes from this poll. A daemon
/// with many sessions would track the next timer deadline and sleep until it;
/// a fixed tick keeps the example short.
fn running_sessions(reader: &Connection) -> Vec<ExecutionId> {
    match inspect::executions(reader, WORKFLOW_NAME) {
        Ok(rows) => rows
            .iter()
            .filter(|row| row.state == "RUNNING")
            .filter_map(|row| row.exec_id.parse::<ExecutionId>().ok())
            .collect(),
        Err(message) => {
            tracing::error!(error = %message, "cannot enumerate the sessions");
            Vec::new()
        }
    }
}

/// Drive one session to its next stopping point.
async fn drive_one(
    runtime: &mut SqliteRuntime,
    exec: ExecutionId,
    blocked: &mut HashMap<ExecutionId, String>,
) {
    match runtime.run_until_blocked(exec).await {
        Ok(RunState::WaitingSignal(name)) => {
            let reason = if name == SIGNAL_TOOL_APPROVAL {
                "waiting for a tool approval".to_string()
            } else {
                format!("waiting for the `{name}` signal")
            };
            note(blocked, exec, reason);
        }
        Ok(RunState::WaitingTimer) => {
            note(blocked, exec, "waiting for a durable timer".to_string());
        }
        Ok(RunState::Completed(output)) => {
            blocked.remove(&exec);
            tracing::info!(%exec, output = %output, "session completed");
        }
        Ok(RunState::Failed(error)) => {
            blocked.remove(&exec);
            tracing::error!(%exec, error = %error, "session failed");
        }
        Ok(RunState::InProgress) => {
            blocked.remove(&exec);
        }
        Err(e) => tracing::error!(%exec, error = %e, "cannot drive the session"),
    }
}

/// Record why a session is parked, logging only the changes.
fn note(blocked: &mut HashMap<ExecutionId, String>, exec: ExecutionId, reason: String) {
    if blocked.get(&exec) != Some(&reason) {
        tracing::info!(%exec, reason = %reason, "session parked");
        blocked.insert(exec, reason);
    }
}
