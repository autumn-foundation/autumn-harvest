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
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest_sqlite::{ExecutionId, RunState, SqliteRuntime};
use rusqlite::Connection;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Semaphore, mpsc, oneshot};

use crate::claude::{self, ModelConfig};
use crate::guard;
use crate::inspect::{self, ExecutionRow};
use crate::protocol::{PendingCall, Request, Response, SessionView};
use crate::session::{
    self, ApprovalDecision, SessionReport, SessionTask, TurnReply, WORKFLOW_NAME,
};
use crate::shutdown;
use crate::tools;

/// How many control commands may queue while the runtime is busy.
const COMMAND_BACKLOG: usize = 32;

/// How many control connections the daemon holds at once.
///
/// A command waits while the runtime is busy, and a model call holds it for as
/// long as the API takes. Without this bound, every connection accepted during
/// that time is a task and a descriptor parked in the channel send. A polling
/// script would spend the daemon's descriptors, and the answer to every
/// command would arrive no sooner.
///
/// The permit is taken BEFORE the accept. A connection that has nowhere to go
/// is not accepted at all, so the kernel queues it on the listening socket
/// instead. The operator sees one command wait, rather than a daemon that ran
/// out of descriptors.
const MAX_CONNECTIONS: usize = COMMAND_BACKLOG;

/// The longest control request the daemon reads.
///
/// A request is one line of JSON. A goal can be long, and a megabyte is far
/// past anything an operator types. The cap stops one caller from growing the
/// daemon's memory without end.
const MAX_REQUEST_BYTES: u64 = 1024 * 1024;

/// How long one caller may take to send its request.
///
/// The deadline covers the REQUEST only. The answer can take as long as the
/// runtime needs, because a command waits for the drive loop.
///
/// A real client writes its line as soon as it connects, so this is generous
/// by a wide margin. It is deliberately SHORT, because a connection that
/// sends nothing still holds one of the daemon's connections until it expires.
const REQUEST_DEADLINE: Duration = Duration::from_secs(5);

/// How long one caller may take to read its answer.
///
/// A client that asks for an answer and then stops reading would otherwise
/// hold a connection until it disconnected. The socket is local, so it moves
/// at memory speed: a client that has not taken its answer in this long is not
/// reading it.
const RESPONSE_DEADLINE: Duration = Duration::from_secs(5);

/// How long to wait after `accept` fails.
///
/// A descriptor limit makes `accept` fail at once, every time. Without a pause
/// the loop spins on it and fills the log.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

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

/// The largest tool input the status prints. An operator decides from it, so
/// it is generous; the marker says when there is more.
const MAX_PENDING_INPUT_CHARS: usize = 2000;

/// The largest event detail one audit line carries.
const MAX_EVENT_DETAIL_CHARS: usize = 240;

/// Why one session is parked, and what would release it.
#[derive(Clone)]
pub struct ParkedState {
    /// The operator-facing reason.
    pub reason: String,
    /// The signal name a decision must carry, when the session waits for one.
    /// It names the exact tool call, so an approval cannot release another.
    pub signal: Option<String>,
}

/// The parked sessions this daemon knows about.
type Parked = HashMap<ExecutionId, ParkedState>;

/// The sessions the drive tick advances, in the order they arrived.
///
/// The daemon is the only writer of this database, so it knows every live
/// session. It starts them, and it sees each one reach a terminal state. The
/// set is seeded once at startup and maintained in memory after that.
///
/// The alternative is a query on every tick. That query cannot use an index.
/// `harvest_executions` is indexed on `(workflow_name, workflow_id)`, so a
/// filter on `state` visits every session that ever ran under this workflow
/// name. The cost of an idle daemon would grow with its whole history, several
/// times a second. The engine owns that schema, and an example does not add an
/// index to it.
///
/// A `Vec` rather than a set, for two reasons. The order stays the submit
/// order. The length is the number of LIVE sessions, and not of the recorded
/// history.
type Live = Vec<ExecutionId>;

/// Run the daemon until `Ctrl-C`.
///
/// # Errors
///
/// Returns an error if the database, the workspace, or the socket cannot be
/// opened, or if another daemon already holds the socket.
pub async fn serve(options: Options) -> Result<(), String> {
    let workspace = prepare_workspace(&options.workspace, &options.db)?;

    // Take the per-database lock FIRST. The open below reclaims every task left
    // `RUNNING` by a dead process. A second daemon opening the same file would
    // reclaim a LIVE task, and its activity would run twice. The lock lives as
    // long as this call, and the kernel releases it if the process dies.
    let _lock = guard::acquire(&options.db)?;

    // One task waits for `Ctrl-C` and raises the flag. The drive loop cannot
    // wait for the signal itself. A model call blocks its thread, so nothing
    // else on that task is polled until the call returns. See `shutdown`.
    let (trigger, signal) = shutdown::channel();
    tokio::spawn(async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::error!(error = %e, "cannot listen for Ctrl-C");
        }
        trigger.send_replace(true);
    });

    let model = ModelConfig::new(
        options.api_key,
        options.model,
        options.max_tokens,
        signal.clone(),
    )?;
    let live = model.is_live();
    // Every session records this, and a turn is refused by a daemon serving a
    // different model. See `ModelConfig::identity`.
    let identity = model.identity();

    // Opening the file applies the schema and reclaims any task a previous
    // process left RUNNING. In-flight sessions resume by replay from here.
    // The `-wal` and `-shm` sidecars are created by `SQLite`, and they carry
    // the same data as the database. The mask makes them private too.
    let mut runtime = guard::with_private_umask(|| SqliteRuntime::open(&options.db))
        .map_err(|e| format!("cannot open {}: {e}", options.db.display()))?;
    runtime.register_workflow(&session::agent_session_info());
    runtime.register_activity(&session::claude_turn_info(), claude::activity_body(model));
    runtime.register_activity(
        &session::run_tool_info(),
        tools::activity_body(options.workspace.clone()),
    );

    let reader = inspect::open(&options.db)?;
    // Check the sessions already in the file BEFORE anything drives them. A
    // mismatched tool call fails non-retryably, and a FAILED run is terminal.
    // Only `RUNNING` rows are ever driven again, so restarting with the right
    // flags could not bring it back. An operator who mistypes `--workspace`
    // gets an error here, and every session stays resumable.
    let resumed = check_resumable(&reader, &workspace, &identity)?;
    if !resumed.is_empty() {
        tracing::info!(count = resumed.len(), "resuming the sessions left running");
    }
    let listener = bind(&options.socket).await?;
    let (tx, mut rx) = mpsc::channel::<Job>(COMMAND_BACKLOG);
    // The socket's mode is not a control surface on its own. See
    // [`peer_is_owner`].
    let owner = rustix::process::geteuid().as_raw();
    tokio::spawn(accept_loop(listener, tx, owner));

    tracing::info!(
        db = %options.db.display(),
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

    let mut blocked: Parked = Parked::new();
    // The startup check already read the RUNNING rows, so the live set is what
    // it validated. One query, not two.
    let mut live: Live = resumed;
    let mut ticker = tokio::time::interval(options.tick);
    // A model call can hold a drive for the whole HTTP timeout, which is
    // thousands of tick periods. The default behaviour fires every missed tick
    // at once when the drive returns, and each one drives every live session.
    // The poll wants the NEXT tick, not the ones it slept through.
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Every branch below waits on the same flag. It stays raised once it is
    // raised, so a fresh wait returns at once rather than missing the signal.
    let mut stop = signal.clone();
    let mut stop_during_drive = signal;
    'serve: loop {
        tokio::select! {
            job = rx.recv() => {
                let Some((request, answer)) = job else { break };
                let response = handle(
                    &mut runtime,
                    &reader,
                    &mut blocked,
                    &mut live,
                    &workspace,
                    &identity,
                    request,
                );
                // A closed receiver means the client hung up. Nothing to do.
                drop(answer.send(response));
            }
            _ = ticker.tick() => {
                // A copy, because each drive can remove its own session from
                // the set. The set holds the live sessions only, so this is
                // short whatever the recorded history holds.
                let ready = live.clone();
                for exec in ready {
                    tokio::select! {
                        () = drive_one(&mut runtime, exec, &mut blocked, &mut live) => {}
                        () = stop_during_drive.raised() => {
                            // The drive is dropped where it stands. Its task
                            // stays RUNNING in the file, and the next start
                            // reclaims it and replays the recorded history.
                            // That is the same path a crash takes.
                            break 'serve;
                        }
                    }
                }
            }
            () = stop.raised() => break,
        }
    }

    // The socket path is deliberately NOT removed here.
    //
    // Whatever occupies it at this moment may not be this daemon's socket. The
    // path can be replaced while the daemon runs. An inode number is also
    // reused as soon as it is freed. Neither the type nor the identity can
    // therefore prove ownership of a public pathname. Deleting another
    // daemon's socket is worse than leaving a stale one, and a stale one costs
    // nothing. `bind` reclaims it at the next start, once it has proved that
    // it is a socket and that nobody answers on it.
    tracing::info!(
        socket = %options.socket.display(),
        "agentd is stopping; in-flight sessions resume on the next start",
    );
    Ok(())
}

/// Refuse to start when a session in this file belongs to another daemon.
///
/// The activity-level checks stay as a backstop, but they can only fail a run.
/// This is the check that protects the work.
fn check_resumable(
    reader: &Connection,
    workspace: &str,
    model: &str,
) -> Result<Vec<ExecutionId>, String> {
    let mut live = Vec::new();
    for row in inspect::running(reader, WORKFLOW_NAME)? {
        let Ok(task) = serde_json::from_str::<SessionTask>(&row.input_json) else {
            continue;
        };
        if task.workspace != workspace {
            return Err(format!(
                "session {} belongs to the workspace `{}`, and this daemon serves \
                 `{workspace}`. Start it with `--workspace {}` so the session can \
                 resume.",
                row.exec_id, task.workspace, task.workspace
            ));
        }
        if task.model != model {
            return Err(format!(
                "session {} runs on the model `{}`, and this daemon serves `{model}`. \
                 Start it with `--model {}`, or with the key that model needs, so the \
                 session can resume.",
                row.exec_id, task.model, task.model
            ));
        }
        if let Ok(exec) = row.exec_id.parse::<ExecutionId>() {
            live.push(exec);
        }
    }
    Ok(live)
}

/// Take the control socket, refusing to displace a live daemon.
///
/// Anything already at the path is removed ONLY when it is a socket. A typo in
/// `--socket` must not delete a file, so any other kind of entry is an error.
///
/// The socket is created owner-only. Whoever can connect to it can spend money
/// and approve writes with this daemon's privileges, so a permissive umask must
/// not decide that. The mask is narrowed across the bind, which makes the
/// socket private AT CREATION: there is no window in which another local user
/// can connect.
pub async fn bind(socket: &Path) -> Result<UnixListener, String> {
    if let Ok(existing) = std::fs::symlink_metadata(socket) {
        if !existing.file_type().is_socket() {
            return Err(format!(
                "{} exists and is not a socket. Refusing to remove it.",
                socket.display()
            ));
        }
        if UnixStream::connect(socket).await.is_ok() {
            return Err(format!("a daemon already listens on {}", socket.display()));
        }
        // The socket outlived its process, so it is safe to replace.
        std::fs::remove_file(socket)
            .map_err(|e| format!("cannot remove the stale socket {}: {e}", socket.display()))?;
    }

    guard::with_private_umask(|| UnixListener::bind(socket))
        .map_err(|e| format!("cannot listen on {}: {e}", socket.display()))
}

/// Accept connections and forward each request to the main loop.
async fn accept_loop(listener: UnixListener, tx: mpsc::Sender<Job>, owner: u32) {
    let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    loop {
        // Take the permit first. See [`MAX_CONNECTIONS`].
        let Ok(permit) = Arc::clone(&permits).acquire_owned().await else {
            return;
        };
        match listener.accept().await {
            Ok((stream, _)) => {
                // Every caller is identified before it is served.
                if !peer_is_owner(&stream, owner) {
                    drop(permit);
                    continue;
                }
                let tx = tx.clone();
                tokio::spawn(async move {
                    // The permit is released when this connection is done.
                    let _permit = permit;
                    serve_connection(stream, tx).await;
                });
            }
            Err(e) => {
                drop(permit);
                tracing::warn!(error = %e, "cannot accept a control connection");
                tokio::time::sleep(ACCEPT_BACKOFF).await;
            }
        }
    }
}

/// Is the caller the user this daemon runs as?
///
/// The socket is created `0600`, and that is not enough on its own. Linux
/// enforces a socket's mode on `connect`. macOS does not, and this example
/// supports both. A socket in a directory that other users can search would
/// therefore accept them there.
///
/// The kernel reports the peer's credentials, and no directory mode can forge
/// them. The check fails CLOSED: a peer that cannot be identified is refused,
/// because this connection spends money and approves writes.
pub fn peer_is_owner(stream: &UnixStream, owner: u32) -> bool {
    match stream.peer_cred() {
        Ok(peer) if peer.uid() == owner => true,
        Ok(peer) => {
            tracing::warn!(
                uid = peer.uid(),
                "refused a control connection from another user"
            );
            false
        }
        Err(e) => {
            tracing::warn!(error = %e, "refused a control connection of unknown origin");
            false
        }
    }
}

/// Read one request, wait for the answer, write it back.
async fn serve_connection(stream: UnixStream, tx: mpsc::Sender<Job>) {
    let (read_half, mut write_half) = stream.into_split();
    let mut line = String::new();
    // The read is bounded in BOTH size and time. A caller that sends no
    // newline would otherwise grow this buffer without end, and hold one of
    // the daemon's connections while it did. Enough such callers would leave
    // no connection for the operator.
    let read = tokio::time::timeout(
        REQUEST_DEADLINE,
        BufReader::new(read_half.take(MAX_REQUEST_BYTES)).read_line(&mut line),
    )
    .await;
    let request = match read {
        Ok(Ok(_)) => line,
        // A caller that stopped mid-request gets a reason, not a closed
        // connection. The cap makes a truncated line malformed JSON, which is
        // reported the same way.
        Ok(Err(_)) => return,
        Err(_) => {
            answer(
                &mut write_half,
                &Response::Error {
                    message: format!(
                        "the request did not arrive within {} seconds",
                        REQUEST_DEADLINE.as_secs()
                    ),
                },
            )
            .await;
            return;
        }
    };

    let response = match serde_json::from_str::<Request>(request.trim()) {
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

    answer(&mut write_half, &response).await;
}

/// Write one answer back, and end the line.
///
/// An answer that cannot be encoded still gets a line, so the client reads a
/// reason instead of a closed connection.
async fn answer(write_half: &mut tokio::net::unix::OwnedWriteHalf, response: &Response) {
    let mut encoded = serde_json::to_string(response).unwrap_or_else(|e| {
        tracing::error!(error = %e, "cannot encode an answer");
        r#"{"status":"error","message":"the daemon cannot encode its answer"}"#.to_string()
    });
    encoded.push('\n');
    // Bounded, like the read. A client that stops reading cannot hold this
    // connection for longer than the deadline. See [`RESPONSE_DEADLINE`].
    let written = tokio::time::timeout(RESPONSE_DEADLINE, async {
        write_half.write_all(encoded.as_bytes()).await?;
        write_half.flush().await
    })
    .await;
    match written {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::debug!(error = %e, "a caller did not take its answer"),
        Err(_) => tracing::warn!(
            seconds = RESPONSE_DEADLINE.as_secs(),
            "a caller did not read its answer in time"
        ),
    }
}

/// Apply one control command.
fn handle(
    runtime: &mut SqliteRuntime,
    reader: &Connection,
    blocked: &mut Parked,
    live: &mut Live,
    workspace: &str,
    model: &str,
    request: Request,
) -> Response {
    match request {
        Request::Submit {
            goal,
            max_turns,
            approval_timeout_secs,
        } => submit(
            runtime,
            live,
            workspace,
            model,
            goal,
            max_turns,
            approval_timeout_secs,
        ),
        // One session is named, so one row is read. The listing would select
        // the input and the output of every session that ever ran.
        Request::Status { execution_id, full } => {
            match inspect::execution(reader, WORKFLOW_NAME, &execution_id) {
                Ok(Some(row)) => Response::Session {
                    session: Box::new(view(runtime, &row, blocked, full)),
                },
                Ok(None) => Response::Error {
                    message: format!("no session {execution_id}"),
                },
                Err(message) => Response::Error { message },
            }
        }
        Request::List => match sessions(runtime, reader, blocked, false) {
            Ok(sessions) => Response::Sessions { sessions },
            Err(message) => Response::Error { message },
        },
        Request::History { execution_id } => history(runtime, reader, &execution_id),
        Request::Approve {
            execution_id,
            token,
            approved,
            note,
        } => approve(
            runtime,
            reader,
            blocked,
            &execution_id,
            &token,
            approved,
            note,
        ),
    }
}

/// Start one session.
fn submit(
    runtime: &mut SqliteRuntime,
    live: &mut Live,
    workspace: &str,
    model: &str,
    goal: String,
    max_turns: u32,
    approval_timeout_secs: u64,
) -> Response {
    let task = SessionTask {
        goal,
        max_turns,
        approval_timeout_secs,
        workspace: workspace.to_string(),
        model: model.to_string(),
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
        Ok(exec) => {
            // The tick drives it from here. A session the set does not hold
            // would sit at its first turn forever.
            enlist(live, exec);
            Response::Submitted {
                execution_id: exec.to_string(),
            }
        }
        Err(e) => Response::Error {
            message: format!("cannot start the session: {e}"),
        },
    }
}

/// Deliver one approval decision.
///
/// The decision is addressed to the signal the session is waiting on, and that
/// name carries the tool-use id. A decision can therefore only release the call
/// the operator was shown. An early, repeated, or stale `approve` has no live
/// wait to land in, so it is refused here rather than staged for a later call.
pub fn approve(
    runtime: &mut SqliteRuntime,
    reader: &Connection,
    blocked: &mut Parked,
    execution_id: &str,
    token: &str,
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
    let Some(signal) = blocked.get(&exec).and_then(|state| state.signal.clone()) else {
        return Response::Error {
            message: format!("session {execution_id} is not waiting for a decision"),
        };
    };
    // The wait can move on between the status and the decision: a deadline can
    // expire, and the session then parks on the NEXT call. The token names one
    // wait of one run, and it is compared EXACTLY. A tool-use id would not be
    // enough. The model can reuse one across turns, so a decision read from an
    // older status would then release a call nobody reviewed.
    if signal != token {
        return Response::Error {
            message: format!(
                "session {execution_id} is now waiting on `{signal}`, not `{token}`. \
                 Read `agentd status {execution_id}` again before deciding."
            ),
        };
    }
    // A decision that arrives after the deadline cannot win. The backend fires
    // the expired race timer BEFORE a late signal, on purpose, so the session
    // reports the call as denied however this answer reads. Saying "approved"
    // here would be a lie the operator only discovers in the history.
    match expired(reader, execution_id, &signal) {
        Ok(Some(passed)) => {
            return Response::Error {
                message: format!(
                    "the deadline for this call passed {passed} seconds ago, so the \
                     session denies it on its next drive. Nothing was delivered."
                ),
            };
        }
        Ok(None) => {}
        Err(message) => return Response::Error { message },
    }

    let decision = ApprovalDecision { approved, note };
    let payload = match serde_json::to_value(decision) {
        Ok(value) => value,
        Err(e) => {
            return Response::Error {
                message: format!("cannot encode the decision: {e}"),
            };
        }
    };
    match runtime.send_signal(exec, &signal, payload) {
        Ok(()) => {
            // The deadline can pass between the check above and the moment the
            // backend stamps this signal. It records its own arrival time, so
            // the daemon cannot stage a decision AT a chosen instant. When the
            // deadline has gone by now, the answer says what is true: the
            // decision is delivered, and the session may still deny the call.
            let crossed = matches!(expired(reader, execution_id, &signal), Ok(Some(_)));
            // The wait is spent the moment a decision is staged. Without this,
            // a second `approve` before the next drive tick would stage a
            // SECOND signal. The first releases this call and the other stays
            // queued, where a later call reusing the id could consume it and
            // run without being shown. The next tick re-reads the run's real
            // state, so clearing it here loses nothing.
            if let Some(state) = blocked.get_mut(&exec) {
                state.signal = None;
                state.reason = "a decision is delivered; awaiting the next drive".to_string();
            }
            let decision = if approved { "approved" } else { "denied" };
            Response::Ack {
                detail: if crossed {
                    format!(
                        "{decision}, and the deadline passed while it was delivered. \
                         The session may report this call as denied. Read \
                         `agentd history {execution_id}` to see which won."
                    )
                } else {
                    decision.to_string()
                },
            }
        }
        Err(e) => Response::Error {
            message: format!("cannot deliver the decision: {e}"),
        },
    }
}

/// How long ago the deadline of this wait passed, in seconds.
///
/// `None` means the wait still has time, or has no deadline at all.
fn expired(reader: &Connection, execution_id: &str, signal: &str) -> Result<Option<i64>, String> {
    let Some(fire_at) = inspect::signal_deadline(reader, execution_id, signal)? else {
        return Ok(None);
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| format!("cannot read the clock: {e}"))?;
    let now =
        i64::try_from(now.as_millis()).map_err(|e| format!("the clock is out of range: {e}"))?;
    if now < fire_at {
        return Ok(None);
    }
    Ok(Some((now - fire_at) / 1000))
}

/// Report the recorded event log of one session.
///
/// The session must exist. An event read of an unknown id returns no rows, so
/// without this check a mistyped audit target prints an empty history and
/// exits clean. That reads as a session that did nothing.
fn history(runtime: &SqliteRuntime, reader: &Connection, execution_id: &str) -> Response {
    let exec = match execution_id.parse::<ExecutionId>() {
        Ok(exec) => exec,
        Err(e) => {
            return Response::Error {
                message: format!("`{execution_id}` is not an execution id: {e}"),
            };
        }
    };
    match inspect::is_session(reader, WORKFLOW_NAME, execution_id) {
        Ok(true) => {}
        Ok(false) => {
            return Response::Error {
                message: format!("no session {execution_id}"),
            };
        }
        Err(message) => return Response::Error { message },
    }
    match runtime.load_history(exec) {
        Ok(events) => Response::History {
            events: events
                .iter()
                .enumerate()
                .map(|(index, event)| format!("{:>3}  {}", index + 1, describe(event)))
                .collect(),
        },
        Err(e) => Response::Error {
            message: format!("cannot read the history: {e}"),
        },
    }
}

/// Describe one recorded event for the audit trail.
///
/// A type label alone cannot answer what the agent did, which is the whole
/// point of the command. So each event carries its own data too, rendered
/// compactly and trimmed to one readable line. The rendering is generic and
/// prints whatever the event holds. A new event variant therefore needs no
/// change here, and is never reduced to a bare name.
fn describe(event: &autumn_harvest::WorkflowEvent) -> String {
    let Ok(value) = serde_json::to_value(event) else {
        return "unreadable event".to_string();
    };
    let label = value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("unknown");

    match value.get("data") {
        Some(data) if !data.is_null() => {
            let mut rendered = data.to_string();
            if rendered.chars().count() > MAX_EVENT_DETAIL_CHARS {
                rendered = rendered.chars().take(MAX_EVENT_DETAIL_CHARS).collect();
                rendered.push('…');
            }
            format!("{label}  {rendered}")
        }
        _ => label.to_string(),
    }
}

/// Project every execution row into an operator view.
fn sessions(
    runtime: &SqliteRuntime,
    reader: &Connection,
    blocked: &Parked,
    full: bool,
) -> Result<Vec<SessionView>, String> {
    Ok(inspect::executions(reader, WORKFLOW_NAME)?
        .into_iter()
        .map(|row| view(runtime, &row, blocked, full))
        .collect())
}

/// Build one operator view.
fn view(runtime: &SqliteRuntime, row: &ExecutionRow, blocked: &Parked, full: bool) -> SessionView {
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
    let exec = row.exec_id.parse::<ExecutionId>().ok();
    let state = exec.and_then(|exec| blocked.get(&exec));
    let pending = match (exec, state.and_then(|state| state.signal.as_deref())) {
        (Some(exec), Some(signal)) => pending_call(runtime, exec, signal, full),
        _ => None,
    };

    SessionView {
        execution_id: row.exec_id.clone(),
        goal,
        state: row.state.clone(),
        blocked_on: state.map(|state| state.reason.clone()),
        pending,
        answer,
        error: row.error.clone(),
    }
}

/// Read the awaited tool call back out of the event log.
///
/// The daemon holds no copy of it. The call was recorded as the result of the
/// model activity, so the history is the source of truth here. That is true of
/// the run itself as well. The most recent model reply holds the awaited call,
/// so the scan runs backwards.
pub fn pending_call(
    runtime: &SqliteRuntime,
    exec: ExecutionId,
    signal: &str,
    full: bool,
) -> Option<PendingCall> {
    let call_id = session::approval_call_id(signal)?;
    let history = runtime.load_history(exec).ok()?;

    for event in history.iter().rev() {
        let value = serde_json::to_value(event).ok()?;
        if value.get("type").and_then(Value::as_str) != Some("ActivityCompleted") {
            continue;
        }
        let Some(output) = value.pointer("/data/output") else {
            continue;
        };
        let Ok(reply) = serde_json::from_value::<TurnReply>(output.clone()) else {
            continue;
        };
        if let Some(call) = reply.tool_calls.into_iter().find(|call| call.id == call_id) {
            let mut input = call.input.to_string();
            // A decision needs the WHOLE payload, and a write carries up to
            // 64 KiB. The status trims it to stay readable, and `--full` prints
            // every byte, so nothing is ever approved sight unseen.
            if !full && input.chars().count() > MAX_PENDING_INPUT_CHARS {
                input = input.chars().take(MAX_PENDING_INPUT_CHARS).collect();
                input.push_str(" … (truncated; read it all with `status --full`)");
            }
            return Some(PendingCall {
                token: signal.to_string(),
                id: call.id,
                tool: call.name,
                input,
            });
        }
    }
    None
}

/// Create the workspace, make it durable, and record its resolved name.
///
/// # Errors
///
/// Returns an error if the workspace cannot be created or resolved, if its
/// name is not valid UTF-8, or if the database is inside it.
fn prepare_workspace(workspace: &Path, db: &Path) -> Result<String, String> {
    std::fs::create_dir_all(workspace)
        .map_err(|e| format!("cannot create the workspace {}: {e}", workspace.display()))?;
    // Resolve it once. Every session records this value, and the tool activity
    // refuses a call whose session belongs to a different workspace.
    let resolved = workspace
        .canonicalize()
        .map_err(|e| format!("cannot resolve the workspace {}: {e}", workspace.display()))?;
    // `create_dir_all` above wrote no directory entry to disk. The write path
    // flushes from a target up to the workspace root, and the entry that NAMES
    // the root lives above it. See [`flush_workspace_path`].
    flush_workspace_path(&resolved);
    // The model can write to any path inside the workspace, and `write_file`
    // replaces its target. The database, and its `-wal` sidecar beside it,
    // must therefore not be reachable from there.
    refuse_state_in_workspace(db, &resolved)?;

    // A lossy conversion would mangle a path that is not valid UTF-8, and the
    // recorded identity would then never match the real one again. Refuse the
    // path instead of recording a name that cannot be compared.
    Ok(resolved
        .to_str()
        .ok_or_else(|| {
            format!(
                "the workspace path {} is not valid UTF-8. Each session records \
                 this path, so a name that cannot be written down exactly would \
                 never match again.",
                resolved.display()
            )
        })?
        .to_string())
}

/// The directories above the workspace, closest first.
///
/// The workspace itself is not in the list. Every write flushes it already,
/// because it is the top of the chain the write path walks.
pub fn path_above(workspace: &Path) -> Vec<PathBuf> {
    workspace
        .ancestors()
        .skip(1)
        .map(Path::to_path_buf)
        .collect()
}

/// Make the workspace's own directory entry durable.
///
/// `create_dir_all` writes no entry to disk. A power loss after a committed
/// write could therefore remove the workspace that the daemon created, and the
/// recorded file with it. The write path cannot cover this: it flushes from a
/// target up to the root, and the entry that names the root is above it.
///
/// The whole chain is flushed, and not only what this process created.
/// A daemon that created those directories and then died would leave the next
/// start with nothing to flush and the same unwritten entries.
///
/// A directory that cannot be opened or flushed is logged and skipped. This is
/// durability work on an operator's own tree. It is not a reason to refuse to
/// serve.
fn flush_workspace_path(workspace: &Path) {
    for directory in path_above(workspace) {
        let flushed = std::fs::File::open(&directory).and_then(|handle| handle.sync_all());
        if let Err(e) = flushed {
            tracing::warn!(
                path = %directory.display(),
                error = %e,
                "cannot flush a directory above the workspace"
            );
        }
    }
}

/// Refuse a database that the agent can reach.
///
/// `write_file` replaces its target through a rename. A database inside the
/// workspace is therefore one approved tool call away from replacement.
/// `SQLite` and the daemon lock still hold the old inode after that. The next
/// commits fail, and a restart opens the replacement instead of the recorded
/// history. The `-wal` and `-shm` sidecars sit beside the database and carry
/// the same data, so one containment test covers all three.
///
/// The refusal is at startup, where it costs an operator one flag. The
/// alternative is a list of reserved names in the toolbox, which has to stay
/// in step with whatever the engine writes beside its database.
fn refuse_state_in_workspace(db: &Path, workspace: &Path) -> Result<(), String> {
    let real = resolve_database(db)?;
    if !real.starts_with(workspace) {
        return Ok(());
    }

    Err(format!(
        "the database {} is inside the workspace {}. A tool call can write to \
         any path in the workspace. Replacing the database, or its `-wal` \
         sidecar, would destroy the history this daemon runs from. Keep the \
         database outside the workspace with `--db` or `--workspace`.",
        db.display(),
        workspace.display()
    ))
}

/// Where the database really is.
///
/// The final component is resolved too, and not only the directory that holds
/// it. A symbolic link outside the workspace can name a target inside it, and
/// the lock and `SQLite` both follow the link. A test on the link's own path
/// would report the safe side of a rule the daemon then breaks.
///
/// A database that does not exist yet has no target to resolve, so the
/// directory that will hold it is resolved instead. A link that resolves to
/// nothing is refused rather than guessed at. The file it creates would land
/// wherever the link points, and that is the question being asked here.
fn resolve_database(db: &Path) -> Result<PathBuf, String> {
    if let Ok(real) = db.canonicalize() {
        return Ok(real);
    }
    if db.symlink_metadata().is_ok() {
        return Err(format!(
            "the database {} is a symbolic link that resolves to nothing. \
             The daemon cannot say where it would write. Name the database \
             itself with `--db`.",
            db.display()
        ));
    }

    let directory = match db.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    let resolved = directory
        .canonicalize()
        .map_err(|e| format!("cannot resolve the directory of {}: {e}", db.display()))?;
    let name = db
        .file_name()
        .ok_or_else(|| format!("{} does not name a database file", db.display()))?;
    Ok(resolved.join(name))
}

/// Add a session to the set the tick drives.
fn enlist(live: &mut Live, exec: ExecutionId) {
    if !live.contains(&exec) {
        live.push(exec);
    }
}

/// Drop a session that reached a terminal state.
fn retire(blocked: &mut Parked, live: &mut Live, exec: ExecutionId) {
    blocked.remove(&exec);
    live.retain(|id| *id != exec);
}

/// Drive one session to its next stopping point.
///
/// A session leaves the live set only on a state that cannot be driven again.
/// An unrecognised outcome, or a drive error, keeps it. One wasted drive costs
/// a tick. A session dropped in error would never be driven again, because
/// this daemon is the only writer.
async fn drive_one(
    runtime: &mut SqliteRuntime,
    exec: ExecutionId,
    blocked: &mut Parked,
    live: &mut Live,
) {
    match runtime.run_until_blocked(exec).await {
        Ok(RunState::WaitingSignal(name)) => {
            let reason = if session::approval_call_id(&name).is_some() {
                "waiting for a tool approval".to_string()
            } else {
                format!("waiting for the `{name}` signal")
            };
            note(blocked, exec, reason, Some(name));
        }
        Ok(RunState::WaitingTimer) => {
            note(
                blocked,
                exec,
                "waiting for a durable timer".to_string(),
                None,
            );
        }
        Ok(RunState::Completed(output)) => {
            retire(blocked, live, exec);
            tracing::info!(%exec, output = %output, "session completed");
        }
        Ok(RunState::Failed(error)) => {
            retire(blocked, live, exec);
            tracing::error!(%exec, error = %error, "session failed");
        }
        Ok(RunState::InProgress) => {
            blocked.remove(&exec);
        }
        Err(e) => tracing::error!(%exec, error = %e, "cannot drive the session"),
    }
}

/// Record why a session is parked, logging only the changes.
fn note(blocked: &mut Parked, exec: ExecutionId, reason: String, signal: Option<String>) {
    if blocked.get(&exec).map(|state| state.reason.as_str()) != Some(reason.as_str()) {
        tracing::info!(%exec, reason = %reason, "session parked");
    }
    blocked.insert(exec, ParkedState { reason, signal });
}
