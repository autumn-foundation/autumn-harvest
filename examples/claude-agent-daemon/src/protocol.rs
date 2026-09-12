//! The control protocol between the `agentd` daemon and its CLI.
//!
//! One line of JSON in, one line of JSON out, over a Unix domain socket. The
//! daemon holds the only write handle to the database, so every mutation
//! travels this socket. That keeps the backend's single-writer contract intact
//! with more than one process in play.

use std::path::Path;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

/// A command the CLI sends to the daemon.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Request {
    /// Start one agent session.
    Submit {
        goal: String,
        max_turns: u32,
        approval_timeout_secs: u64,
    },
    /// Report one session.
    Status {
        execution_id: String,
        /// Print the pending call's arguments in full, however long they are.
        full: bool,
    },
    /// Report every session this database holds.
    List,
    /// Report the recorded event log of one session.
    ///
    /// `before` reads the page that ENDS just before that sequence number. An
    /// operator can then walk back through a log too long to print at once.
    /// The answer names the number to pass next.
    History {
        execution_id: String,
        #[serde(default)]
        before: Option<i64>,
    },
    /// Release or refuse one approval-gated tool call.
    ///
    /// `token` is the approval token the operator was shown. It names ONE wait
    /// of one run, so the daemon can compare it exactly. A tool-use id would
    /// not do. The model can reuse one across turns, so a decision read from
    /// an older status would then release a call nobody reviewed.
    Approve {
        execution_id: String,
        token: String,
        approved: bool,
        note: Option<String>,
    },
}

/// The daemon's answer.
///
/// Every variant is a struct variant. An internally tagged enum cannot carry a
/// bare sequence, so a newtype variant over a `Vec` fails to serialize.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Response {
    Submitted {
        execution_id: String,
    },
    /// Boxed: this variant is much larger than its siblings, and the enum is
    /// sized for its biggest one.
    Session {
        session: Box<SessionView>,
    },
    Sessions {
        sessions: Vec<SessionView>,
        /// Set when the database holds more sessions than this listing shows.
        /// The listing is capped, so an old database cannot be read whole
        /// into memory by one command. See `inspect::MAX_LISTED_SESSIONS`.
        #[serde(default)]
        more: bool,
    },
    History {
        events: Vec<String>,
        /// The session this page belongs to, so the client can name it in the
        /// command that reads the page before this one.
        #[serde(default)]
        execution_id: String,
        /// The cursor that reads the events BEFORE this page, when the log
        /// holds more. The CLIENT renders the command, because only it knows
        /// which socket it asked.
        #[serde(default)]
        older: Option<i64>,
    },
    Ack {
        detail: String,
    },
    Error {
        message: String,
    },
}

/// One session, as an operator sees it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionView {
    pub execution_id: String,
    pub goal: String,
    /// `RUNNING`, `COMPLETED`, or `FAILED`, read from the execution row.
    pub state: String,
    /// Why a running session is parked, when the daemon knows.
    pub blocked_on: Option<String>,
    /// The tool call awaiting a decision, read back from the event log.
    pub pending: Option<PendingCall>,
    /// The report of a finished session.
    pub answer: Option<String>,
    /// The error of a failed session.
    pub error: Option<String>,
}

/// The tool call one parked session is waiting on.
///
/// An operator approves an action, not a session, so the exact call is part of
/// the status. Approving what you cannot see is not approval.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingCall {
    /// The approval token: the one wait this call is parked on. A decision
    /// carries it back, and the daemon matches it exactly.
    pub token: String,
    /// The tool-use id, for the operator to read.
    pub id: String,
    pub tool: String,
    /// The call arguments as JSON, truncated for a terminal.
    pub input: String,
}

/// Send one request to the daemon and read its answer.
///
/// # Errors
///
/// Returns an error if the socket is absent, if the daemon closes the
/// connection, or if either side writes something that is not valid JSON.
pub async fn call(socket: &Path, request: &Request) -> Result<Response, String> {
    let stream = UnixStream::connect(socket).await.map_err(|e| {
        format!(
            "cannot reach the daemon at {}: {e}. Start it with `agentd serve`.",
            socket.display()
        )
    })?;
    let (read_half, mut write_half) = stream.into_split();

    let mut line =
        serde_json::to_string(request).map_err(|e| format!("cannot encode the request: {e}"))?;
    line.push('\n');
    write_half
        .write_all(line.as_bytes())
        .await
        .map_err(|e| format!("cannot send the request: {e}"))?;
    write_half
        .flush()
        .await
        .map_err(|e| format!("cannot send the request: {e}"))?;

    let mut answer = String::new();
    BufReader::new(read_half)
        .read_line(&mut answer)
        .await
        .map_err(|e| format!("cannot read the answer: {e}"))?;
    if answer.trim().is_empty() {
        return Err("the daemon closed the connection without an answer".to_string());
    }
    serde_json::from_str(answer.trim()).map_err(|e| format!("malformed answer: {e}"))
}
