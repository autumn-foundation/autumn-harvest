//! MCP tools and tasks on SQLite (issue #2471).
//!
//! The tool and task routes keep their Postgres descriptors and auth layers.
//! Only the I/O differs: each call reads and writes through the
//! [`HarvestSqlite`] handle.
//!
//! - `start_`, `signal_` and `update_` call the SQLite management handlers,
//!   so a tool answers the body of its HTTP route.
//! - `status` and `watch` follow the retry and continue-as-new chain to the
//!   live run, as on Postgres.
//! - `watch` polls the read-only connection. SQLite has no `LISTEN`.
//! - A task reads its live run, its last event and its open signal waits
//!   from the same file.

use std::convert::Infallible;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use autumn_harvest::ExecutionId;
use autumn_harvest::awaitables::AwaitableKind;
use autumn_harvest_sqlite::{ExecutionDescription, SqliteError, Stored};
use autumn_web::error::AutumnError;
use autumn_web::reexports::axum;
use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, RawQuery, State};
use axum::http::HeaderMap;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Utc};
use futures::SinkExt as _;
use serde_json::{Value, json};

use super::api::{ApiState, NameParams, SignalParams, UpdateParams, map_error};
use super::{HarvestSqlite, HarvestSqliteError};
use crate::api::HarvestApiState;

/// How often `watch` reads the file.
const WATCH_POLL: Duration = Duration::from_millis(100);

/// The state of the SQLite management handlers, on the control Harvest.
fn api_state(handle: HarvestSqlite, flags: &HarvestApiState) -> ApiState {
    let slot = OnceLock::new();
    let _ = slot.set(handle);
    ApiState {
        handle: Arc::new(slot),
        fleet: Arc::new(OnceLock::new()),
        flags: flags.clone(),
    }
}

/// One run, as a tool and a task read it.
#[derive(Debug, Clone)]
pub struct Run {
    /// The run id.
    pub exec: ExecutionId,
    /// The workflow type.
    pub workflow_name: String,
    /// The business id.
    pub workflow_id: String,
    /// The stored state.
    pub state: String,
    /// The output of a completed run.
    pub output: Option<Value>,
    /// The error of a failed run.
    pub error: Option<String>,
    /// The start time. A row from before issue #2027 reads the epoch.
    pub started_at: DateTime<Utc>,
    /// The close time of a terminal run.
    pub completed_at: Option<DateTime<Utc>>,
}

impl Run {
    fn of(exec: ExecutionId, row: &ExecutionDescription) -> Self {
        let text = |field: &Stored<String>| field.value().cloned().unwrap_or_default();
        Self {
            exec,
            workflow_name: text(&row.summary.workflow_name),
            workflow_id: text(&row.summary.workflow_id),
            state: text(&row.summary.state),
            output: row
                .output_json
                .value()
                .and_then(|raw| serde_json::from_str(raw).ok()),
            error: row.error.value().cloned(),
            started_at: row
                .summary
                .started_at
                .value()
                .copied()
                .unwrap_or(DateTime::UNIX_EPOCH),
            completed_at: row.summary.closed_at.value().copied(),
        }
    }

    /// The Postgres `McpStatusResponse` body.
    fn status_body(&self) -> Value {
        json!({
            "execution_id": self.exec.to_string(),
            "workflow_name": self.workflow_name,
            "workflow_id": self.workflow_id,
            "state": self.state,
            "is_terminal": crate::api::is_terminal_state(&self.state),
            "current_details": Value::Null,
            "output": self.output,
            "error": self.error,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
        })
    }
}

/// The HTTP status and the text of a refused call, as the SQLite routes
/// answer it. An MCP task maps a `4xx` to `INVALID_PARAMS`, as on Postgres.
pub fn refusal(error: HarvestSqliteError) -> (u16, String) {
    let text = error.to_string();
    (map_error(error).status().as_u16(), text)
}

/// Read `exec`, or `None` when it does not exist.
pub async fn read_run(
    handle: &HarvestSqlite,
    exec: ExecutionId,
) -> Result<Option<Run>, HarvestSqliteError> {
    match handle.describe(exec).await {
        Ok(row) => Ok(Some(Run::of(exec, &row))),
        Err(HarvestSqliteError::Sqlite(SqliteError::ExecutionNotFound(_))) => Ok(None),
        Err(error) => Err(error),
    }
}

/// The live run of `run`: the end of its retry and continue-as-new chain.
///
/// Only a `FAILED` or `CONTINUED_AS_NEW` run needs the walk, as on Postgres.
pub async fn live_run(handle: &HarvestSqlite, run: Run) -> Result<Run, HarvestSqliteError> {
    if !matches!(run.state.as_str(), "FAILED" | "CONTINUED_AS_NEW") {
        return Ok(run);
    }
    let last = handle.chain_result(run.exec).await?.last;
    if last == run.exec {
        return Ok(run);
    }
    Ok(read_run(handle, last).await?.unwrap_or(run))
}

/// The run of `id` when it is a run of `workflow`, then its live run.
///
/// A run of another workflow answers `404`, as an unknown id does. So one
/// workflow's tools are no existence oracle for another's runs.
#[allow(clippy::result_large_err)] // As `load_owned_execution`: the error is the reply.
async fn owned_live_run(handle: &HarvestSqlite, workflow: &str, id: &str) -> Result<Run, Response> {
    let not_found =
        || AutumnError::not_found_msg(format!("workflow execution {id}")).into_response();
    let exec = crate::api::parse_execution_id(id).map_err(IntoResponse::into_response)?;
    let run = read_run(handle, exec)
        .await
        .map_err(|error| map_error(error).into_response())?
        .ok_or_else(not_found)?;
    if run.workflow_name != workflow {
        return Err(not_found());
    }
    live_run(handle, run)
        .await
        .map_err(|error| map_error(error).into_response())
}

/// `start_{wf}`: the SQLite start route, with only the input set.
///
/// A start key in the `Idempotency-Key` header is a `400`, as on the start
/// route. SQLite has no start-idempotency table, so a retried start would
/// run twice.
pub async fn start_tool(
    handle: HarvestSqlite,
    flags: &HarvestApiState,
    workflow: &str,
    headers: HeaderMap,
    input: Value,
) -> Response {
    let body = Bytes::from(json!({ "input": input }).to_string());
    let name = NameParams {
        workflow_name: workflow.to_owned(),
    };
    super::api::start(
        State(api_state(handle, flags)),
        Path(name),
        None,
        headers,
        body,
    )
    .await
    .into_response()
}

/// `{wf}_status`: the live run, in the Postgres status shape.
pub async fn status_tool(handle: HarvestSqlite, workflow: &str, id: &str) -> Response {
    match owned_live_run(&handle, workflow, id).await {
        Ok(run) => Json(run.status_body()).into_response(),
        Err(response) => response,
    }
}

/// `signal_{wf}`: the SQLite signal route on the live run. It honours the
/// `Idempotency-Key` header, as on Postgres.
pub async fn signal_tool(
    handle: HarvestSqlite,
    flags: &HarvestApiState,
    workflow: &str,
    id: &str,
    signal_name: String,
    headers: HeaderMap,
    payload: Value,
) -> Response {
    let run = match owned_live_run(&handle, workflow, id).await {
        Ok(run) => run,
        Err(response) => return response,
    };
    let params = SignalParams {
        id: run.exec.to_string(),
        signal_name,
    };
    super::api::signal(
        State(api_state(handle, flags)),
        Path(params),
        RawQuery(None),
        headers,
        Bytes::from(payload.to_string()),
    )
    .await
    .into_response()
}

/// `{wf}_update_{name}`: the SQLite update route on the live run. It
/// completes in the request, as the Postgres default does.
pub async fn update_tool(
    handle: HarvestSqlite,
    flags: &HarvestApiState,
    workflow: &str,
    update: &str,
    id: &str,
    input: Value,
) -> Response {
    let run = match owned_live_run(&handle, workflow, id).await {
        Ok(run) => run,
        Err(response) => return response,
    };
    let params = UpdateParams {
        id: run.exec.to_string(),
        update_name: update.to_owned(),
    };
    super::api::update(
        State(api_state(handle, flags)),
        Path(params),
        RawQuery(None),
        Bytes::from(json!({ "input": input }).to_string()),
    )
    .await
    .into_response()
}

/// `{wf}_watch`: an SSE stream of progress frames, then the result frame.
///
/// The frames are the Postgres ones. A progress frame goes out on each
/// change of state, and the stream follows the chain to the live run.
pub async fn watch_tool(
    handle: HarvestSqlite,
    workflow: &str,
    id: &str,
    keepalive: Duration,
    buffer: usize,
) -> Response {
    let run = match owned_live_run(&handle, workflow, id).await {
        Ok(run) => run,
        Err(response) => return response,
    };
    let (mut tx, rx) = futures::channel::mpsc::channel::<Result<Event, Infallible>>(buffer.max(1));
    tokio::spawn(async move {
        let result = |run: &Run| {
            Event::default().event("result").data(
                json!({ "state": run.state, "output": run.output, "error": run.error }).to_string(),
            )
        };
        let progress = |n: u64, run: &Run| {
            Event::default().data(json!({ "progress": n, "message": run.state }).to_string())
        };
        if crate::api::is_terminal_state(&run.state) {
            let _ = tx.send(Ok(result(&run))).await;
            return;
        }
        let mut n = 1;
        if tx.send(Ok(progress(n, &run))).await.is_err() {
            return;
        }
        let mut current = run;
        loop {
            tokio::time::sleep(WATCH_POLL).await;
            if tx.is_closed() {
                return;
            }
            // A failed read keeps the stream. The next poll retries it.
            let Ok(Some(row)) = read_run(&handle, current.exec).await else {
                continue;
            };
            let Ok(next) = live_run(&handle, row).await else {
                continue;
            };
            if crate::api::is_terminal_state(&next.state) {
                let _ = tx.send(Ok(result(&next))).await;
                return;
            }
            if next.exec != current.exec || next.state != current.state {
                n += 1;
                if tx.send(Ok(progress(n, &next))).await.is_err() {
                    return;
                }
            }
            current = next;
        }
    });
    Sse::new(rx)
        .keep_alive(KeepAlive::new().interval(keepalive))
        .into_response()
}

/// The open signal waits of a live run, for an MCP task.
#[derive(Debug, Clone, Default)]
pub struct Waits {
    /// The id and the append time of the last history event.
    pub head: Option<(i32, DateTime<Utc>)>,
    /// The signal names that the replay finds parked, in order.
    pub names: Vec<String>,
    /// The staged signals: name and the time of the oldest.
    pub queued: Vec<(String, DateTime<Utc>)>,
}

/// The id and the append time of the last history event of `exec`.
///
/// An event from before issue #2489 has no time. It then takes the start of
/// the epoch, so it never counts as the latest change.
pub async fn history_head(
    handle: &HarvestSqlite,
    exec: ExecutionId,
) -> Result<Option<(i32, DateTime<Utc>)>, HarvestSqliteError> {
    handle
        .read(move |reader| {
            let events = reader.timed_history(exec)?;
            let Some(last) = events.last() else {
                return Ok(None);
            };
            let position = i32::try_from(events.len() - 1).unwrap_or(i32::MAX);
            Ok(Some((
                position,
                last.recorded_at.unwrap_or(DateTime::UNIX_EPOCH),
            )))
        })
        .await
}

/// The staged, undelivered signals of `exec`.
pub async fn queued_signals(
    handle: &HarvestSqlite,
    exec: ExecutionId,
) -> Result<Vec<(String, DateTime<Utc>)>, HarvestSqliteError> {
    handle
        .read(move |reader| {
            Ok(reader
                .pending_work(exec)?
                .signals
                .into_iter()
                .map(|signal| (signal.name, signal.oldest_received_at))
                .collect())
        })
        .await
}

/// The open signal waits of the live run `exec`.
///
/// The awaitables replay names each parked `wait_for_signal`, as on
/// Postgres. A replay that degrades cannot see a signal wait, so it names
/// none. An event that lands during the replay voids the read.
pub async fn open_waits(
    handle: &HarvestSqlite,
    flags: &HarvestApiState,
    exec: ExecutionId,
) -> Result<Waits, HarvestSqliteError> {
    let head = history_head(handle, exec).await?;
    if head.is_none() {
        return Ok(Waits::default());
    }
    let report = super::reads::awaitables(
        State(api_state(handle.clone(), flags)),
        Path(super::reads::IdParams {
            id: exec.to_string(),
        }),
    )
    .await
    .map_err(|error| HarvestSqliteError::Read(error.to_string()))?
    .0;
    let mut names: Vec<String> = Vec::new();
    for awaitable in report.awaitables {
        if awaitable.kind == AwaitableKind::Signal
            && let Some(name) = awaitable.name
            && !names.contains(&name)
        {
            names.push(name);
        }
    }
    if history_head(handle, exec).await? != head {
        return Ok(Waits {
            head,
            ..Waits::default()
        });
    }
    let queued = queued_signals(handle, exec).await?;
    Ok(Waits {
        head,
        names,
        queued,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_status_body_has_the_postgres_fields() {
        let exec = ExecutionId::new();
        let run = Run {
            exec,
            workflow_name: "greet".into(),
            workflow_id: "w-1".into(),
            state: "COMPLETED".into(),
            output: Some(json!(3)),
            error: None,
            started_at: DateTime::UNIX_EPOCH,
            completed_at: Some(DateTime::UNIX_EPOCH),
        };
        let body = run.status_body();
        for field in [
            "execution_id",
            "workflow_name",
            "workflow_id",
            "state",
            "is_terminal",
            "current_details",
            "output",
            "error",
            "started_at",
            "completed_at",
        ] {
            assert!(body.get(field).is_some(), "{field}: {body}");
        }
        assert_eq!(body["is_terminal"], true);
        assert_eq!(body["execution_id"], exec.to_string());
    }
}
