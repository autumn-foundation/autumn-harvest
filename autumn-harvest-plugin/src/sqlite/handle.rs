//! [`HarvestSqlite`], the app-facing client of the SQLite backend (issue
//! #2467).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use autumn_harvest::info::{SchemaViolation, WorkflowInfo};
use autumn_harvest::{ExecutionId, WorkflowEvent, WorkflowResult, WorkflowResultState};
use autumn_harvest_sqlite::{
    ExecutionDescription, ExecutionFilter, ExecutionPage, ExecutionSummary, HistoryFilter,
    HistoryPage, SignalDelivery, SignalWithStartOutcome, SqliteError, SqliteReader, StartOptions,
    StartOutcome,
};
use autumn_web::AppState;
use autumn_web::error::AutumnError;
use autumn_web::fleet_layout::FleetDbKey;
use autumn_web::reexports::axum;
use autumn_web::sharding::ShardLease;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

use super::driver::{
    Command, KeyedSignal, MAX_CHAIN_HOPS, MAX_RETRY_HOPS, RoutedSignal, RoutedUpdate,
    SignalWithStartParams, Started, StopAck,
};

/// Why a [`HarvestSqlite`] call failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum HarvestSqliteError {
    /// The backend refused the call or failed to run it.
    #[error(transparent)]
    Sqlite(#[from] SqliteError),
    /// The driver task has stopped, for example during shutdown.
    #[error("the Harvest SQLite driver has stopped")]
    Stopped,
    /// A read task failed before it returned a result.
    #[error("the Harvest SQLite read failed: {0}")]
    Read(String),
    /// The input breaks the workflow's registered `input_schema`. The
    /// Postgres start path refuses it the same way (issue #373).
    #[error("input validation failed: {}", describe_violations(.0))]
    InvalidInput(Vec<SchemaViolation>),
    /// The payload breaks the `arg_schema` of the signal's declarative
    /// `#[signal]` handler. The Postgres signal paths refuse it the same way
    /// (issue #610).
    #[error("signal payload validation failed: {}", describe_violations(.0))]
    InvalidSignal(Vec<SchemaViolation>),
    /// The fleet refused the tenant key or the open (issue #2472).
    #[error(transparent)]
    Fleet(#[from] autumn_web::db::fleet::FleetError),
    /// The tenant's Harvest cannot serve the call (issue #2472).
    #[error("tenant Harvest: {0}")]
    Tenant(String),
}

fn describe_violations(violations: &[SchemaViolation]) -> String {
    violations
        .iter()
        .map(|violation| {
            violation.field_path.as_ref().map_or_else(
                || violation.message.clone(),
                |path| format!("{path}: {}", violation.message),
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}

/// The client of the plugin's SQLite backend.
///
/// The plugin builds one at startup when it runs on SQLite. It is cheap to
/// clone. Get it in a handler as an extractor, or with
/// `state.extension::<HarvestSqlite>()`.
///
/// Writes go to the driver task, one at a time. Reads (`describe`, `list`,
/// `history`) use a read-only connection, so a long activity does not block
/// them.
///
/// ```rust,ignore
/// use autumn_harvest_plugin::sqlite::HarvestSqlite;
///
/// #[post("/orders")]
/// async fn create(harvest: HarvestSqlite) -> AutumnResult<String> {
///     let started = harvest
///         .start("process_order", serde_json::json!({"item": "widget"}), Default::default())
///         .await?;
///     Ok(started.exec_id.to_string())
/// }
/// ```
#[derive(Clone)]
pub struct HarvestSqlite {
    inner: Arc<Inner>,
    /// A tenant handle that a caller holds keeps its tenant open. The
    /// fleet's own copy has none (issue #2472).
    lease: Option<ShardLease>,
}

struct Inner {
    /// The driver's runtime holds `AppState`, and `AppState` holds this
    /// handle. So the driver stops on an explicit shutdown, never by a dropped
    /// sender: the plugin's shutdown hook, or [`HarvestSqlite::shutdown`].
    sender: mpsc::Sender<Command>,
    reader: Arc<Mutex<SqliteReader>>,
    path: PathBuf,
    workflows: Vec<String>,
    /// The registered definitions, for their input schemas.
    infos: HashMap<String, WorkflowInfo>,
    /// The fleet tenant of this Harvest. `None` for the control Harvest.
    tenant: Option<FleetDbKey>,
}

impl std::fmt::Debug for HarvestSqlite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HarvestSqlite")
            .field("path", &self.inner.path)
            .field("tenant", &self.inner.tenant)
            .finish_non_exhaustive()
    }
}

impl HarvestSqlite {
    pub(crate) fn new(
        sender: mpsc::Sender<Command>,
        reader: SqliteReader,
        path: PathBuf,
        infos: Vec<WorkflowInfo>,
    ) -> Self {
        let mut workflows: Vec<String> = infos.iter().map(|info| info.name.to_owned()).collect();
        workflows.sort();
        workflows.dedup();
        let infos = infos
            .into_iter()
            .map(|info| (info.name.to_owned(), info))
            .collect();
        Self {
            inner: Arc::new(Inner {
                sender,
                reader: Arc::new(Mutex::new(reader)),
                path,
                workflows,
                infos,
                tenant: None,
            }),
            lease: None,
        }
    }

    /// Mark a new handle as the Harvest of fleet tenant `key`.
    pub(crate) fn for_tenant(mut self, key: FleetDbKey) -> Self {
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.tenant = Some(key);
        }
        self
    }

    /// A copy that keeps its tenant database open while it lives.
    pub(crate) fn with_lease(mut self, lease: ShardLease) -> Self {
        self.lease = Some(lease);
        self
    }

    /// The fleet tenant of this Harvest, or `None` for the control Harvest.
    #[must_use]
    pub fn tenant(&self) -> Option<&FleetDbKey> {
        self.inner.tenant.as_ref()
    }

    /// The Harvest database file.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    /// Check `input` against the `input_schema` of `workflow_name`.
    ///
    /// A workflow with no schema, or an unknown name, passes. `start` and
    /// `signal_with_start` call this first.
    ///
    /// # Errors
    ///
    /// Returns every violation of the schema.
    pub fn validate_input(
        &self,
        workflow_name: &str,
        input: &Value,
    ) -> Result<(), Vec<SchemaViolation>> {
        self.inner
            .infos
            .get(workflow_name)
            .map_or(Ok(()), |info| info.validate_input(input))
    }

    /// The registered workflow names, sorted.
    #[must_use]
    pub fn workflow_names(&self) -> &[String] {
        &self.inner.workflows
    }

    /// The registered workflow definition of `name`, if any.
    #[must_use]
    pub fn workflow_info(&self, name: &str) -> Option<&WorkflowInfo> {
        self.inner.infos.get(name)
    }

    async fn call<T>(
        &self,
        command: impl FnOnce(oneshot::Sender<T>) -> Command,
    ) -> Result<T, HarvestSqliteError> {
        let (reply, response) = oneshot::channel();
        self.inner
            .sender
            .send(command(reply))
            .await
            .map_err(|_| HarvestSqliteError::Stopped)?;
        response.await.map_err(|_| HarvestSqliteError::Stopped)
    }

    /// A runtime reply, with a schema refusal lifted to its own variant.
    fn lift<T>(reply: Result<T, SqliteError>) -> Result<T, HarvestSqliteError> {
        reply.map_err(|error| match error {
            SqliteError::SignalPayloadInvalid { violations, .. } => {
                HarvestSqliteError::InvalidSignal(violations)
            }
            SqliteError::WorkflowInputInvalid { violations, .. } => {
                HarvestSqliteError::InvalidInput(violations)
            }
            other => HarvestSqliteError::Sqlite(other),
        })
    }

    async fn read<T: Send + 'static>(
        &self,
        read: impl FnOnce(&SqliteReader) -> Result<T, SqliteError> + Send + 'static,
    ) -> Result<T, HarvestSqliteError> {
        let reader = Arc::clone(&self.inner.reader);
        tokio::task::spawn_blocking(move || {
            let reader = reader.lock().unwrap_or_else(PoisonError::into_inner);
            read(&reader)
        })
        .await
        .map_err(|error| HarvestSqliteError::Read(error.to_string()))?
        .map_err(HarvestSqliteError::from)
    }

    /// Stop the driver and release the writer lock on the Harvest file.
    ///
    /// The driver finishes the command or pass in flight first. It returns
    /// once the lock is free, so another runtime can open the file. Every
    /// later call returns [`HarvestSqliteError::Stopped`]. The plugin's
    /// shutdown hook does the same, so an app does not need to call this.
    ///
    /// The fleet owns the driver of a tenant Harvest. Close the tenant
    /// database to stop it.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestSqliteError::Stopped`] when the driver had already
    /// stopped, and [`HarvestSqliteError::Tenant`] for a tenant Harvest.
    pub async fn shutdown(&self) -> Result<(), HarvestSqliteError> {
        if let Some(key) = &self.inner.tenant {
            return Err(HarvestSqliteError::Tenant(format!(
                "the fleet owns the driver of {key}: close the tenant database to stop it"
            )));
        }
        self.call(|reply| Command::Shutdown { reply: Some(reply) })
            .await
    }

    /// Start a run of `workflow_name`.
    ///
    /// # Errors
    ///
    /// Returns the backend's refusal, such as an unknown workflow or a
    /// rejected reuse policy, or [`HarvestSqliteError::Stopped`].
    pub async fn start(
        &self,
        workflow_name: &str,
        input: Value,
        options: StartOptions,
    ) -> Result<StartOutcome, HarvestSqliteError> {
        Ok(self
            .start_with_state(workflow_name, input, options)
            .await?
            .outcome)
    }

    /// [`start`](Self::start), and the state of the run right after it.
    pub(crate) async fn start_with_state(
        &self,
        workflow_name: &str,
        input: Value,
        options: StartOptions,
    ) -> Result<Started<StartOutcome>, HarvestSqliteError> {
        self.validate_input(workflow_name, &input)
            .map_err(HarvestSqliteError::InvalidInput)?;
        let workflow_name = workflow_name.to_owned();
        Ok(self
            .call(|reply| Command::Start {
                workflow_name,
                input,
                options,
                reply,
            })
            .await??)
    }

    /// Start a run and signal it, or signal the run that exists, in one
    /// transaction.
    ///
    /// # Errors
    ///
    /// Returns the backend's refusal or [`HarvestSqliteError::Stopped`].
    pub async fn signal_with_start(
        &self,
        request: SignalWithStartParams,
    ) -> Result<SignalWithStartOutcome, HarvestSqliteError> {
        Ok(self.signal_with_start_state(request).await?.outcome)
    }

    /// [`signal_with_start`](Self::signal_with_start), and the state of the
    /// run right after it.
    pub(crate) async fn signal_with_start_state(
        &self,
        request: SignalWithStartParams,
    ) -> Result<Started<SignalWithStartOutcome>, HarvestSqliteError> {
        // The runtime checks the start input only when it creates a run, and
        // the signal only after the key probe, as Postgres does (issue
        // #2467). So an attach needs no input, and a delivered keyed retry
        // stays a duplicate.
        Self::lift(
            self.call(|reply| Command::SignalWithStart { request, reply })
                .await?,
        )
    }

    /// Signal one run. Returns `false` when the idempotency key was already
    /// delivered.
    ///
    /// A signal to a `FAILED` run with a live retry reaches the retry, as on
    /// Postgres (issue #843). [`signal_routed`](Self::signal_routed) names
    /// the run that took it.
    ///
    /// # Errors
    ///
    /// Returns the backend's refusal, such as a terminal run, or
    /// [`HarvestSqliteError::Stopped`].
    pub async fn signal(
        &self,
        exec: ExecutionId,
        name: &str,
        payload: Value,
        idempotency_key: Option<String>,
    ) -> Result<bool, HarvestSqliteError> {
        Ok(self
            .signal_routed(exec, name, payload, idempotency_key)
            .await?
            .delivered)
    }

    /// [`signal`](Self::signal), and name the run that took the signal.
    ///
    /// # Errors
    ///
    /// The errors of [`signal`](Self::signal).
    pub async fn signal_routed(
        &self,
        exec: ExecutionId,
        name: &str,
        payload: Value,
        idempotency_key: Option<String>,
    ) -> Result<RoutedSignal, HarvestSqliteError> {
        let name = name.to_owned();
        Self::lift(
            self.call(|reply| Command::Signal {
                exec,
                name,
                payload,
                idempotency_key,
                reply,
            })
            .await?,
        )
    }

    /// Signal the running run of a `(workflow_name, workflow_id)` key.
    ///
    /// # Errors
    ///
    /// Returns the backend's refusal or [`HarvestSqliteError::Stopped`].
    pub async fn signal_by_workflow_id(
        &self,
        workflow_name: &str,
        workflow_id: &str,
        name: &str,
        payload: Value,
        idempotency_key: Option<String>,
    ) -> Result<SignalDelivery, HarvestSqliteError> {
        Self::lift(
            self.signal_by_key(workflow_name, workflow_id, name, payload, idempotency_key)
                .await?
                .delivery,
        )
    }

    /// [`signal_by_workflow_id`](Self::signal_by_workflow_id), with the run
    /// that the key resolved to in the same driver command.
    pub(crate) async fn signal_by_key(
        &self,
        workflow_name: &str,
        workflow_id: &str,
        name: &str,
        payload: Value,
        idempotency_key: Option<String>,
    ) -> Result<KeyedSignal, HarvestSqliteError> {
        let (workflow_name, workflow_id, name) = (
            workflow_name.to_owned(),
            workflow_id.to_owned(),
            name.to_owned(),
        );
        self.call(|reply| Command::SignalByWorkflowId {
            workflow_name,
            workflow_id,
            name,
            payload,
            idempotency_key,
            reply,
        })
        .await
    }

    /// Lift a runtime refusal, as each write does. A by-id route calls it
    /// after it reads the resolved run.
    pub(crate) fn lift_reply<T>(reply: Result<T, SqliteError>) -> Result<T, HarvestSqliteError> {
        Self::lift(reply)
    }

    /// Ask a run to cancel. The next drive replays it with the reason set.
    ///
    /// A cancelled run and a repeat request answer with `newly: false`. A
    /// `FAILED` run with a live retry hands the request to the retry.
    ///
    /// # Errors
    ///
    /// Returns the backend's refusal, such as a run that ended another way,
    /// or [`HarvestSqliteError::Stopped`].
    pub async fn cancel(
        &self,
        exec: ExecutionId,
        reason: &str,
    ) -> Result<StopAck, HarvestSqliteError> {
        let reason = reason.to_owned();
        Ok(self
            .call(|reply| Command::Cancel {
                exec,
                reason,
                reply,
            })
            .await??)
    }

    /// End a run `TERMINATED` at once.
    ///
    /// A terminal run answers with `newly: false`. A `FAILED` run with a live
    /// retry hands the request to the retry.
    ///
    /// # Errors
    ///
    /// Returns the backend's refusal or [`HarvestSqliteError::Stopped`].
    pub async fn terminate(
        &self,
        exec: ExecutionId,
        reason: &str,
    ) -> Result<StopAck, HarvestSqliteError> {
        let reason = reason.to_owned();
        Ok(self
            .call(|reply| Command::Terminate {
                exec,
                reason,
                reply,
            })
            .await??)
    }

    /// Call a query handler. The replay runs on this task, not the driver.
    ///
    /// # Errors
    ///
    /// Returns the backend's refusal or [`HarvestSqliteError::Stopped`].
    pub async fn query(
        &self,
        exec: ExecutionId,
        name: &str,
        args: Value,
    ) -> Result<Value, HarvestSqliteError> {
        let name = name.to_owned();
        let replay = self
            .call(|reply| Command::Query {
                exec,
                name,
                args,
                reply,
            })
            .await?;
        Ok(replay.await?)
    }

    /// Run an update handler and return its result.
    ///
    /// # Errors
    ///
    /// Returns the backend's refusal or [`HarvestSqliteError::Stopped`]. A
    /// handler error is [`SqliteError::UpdateFailed`].
    pub async fn update(
        &self,
        exec: ExecutionId,
        name: &str,
        input: Value,
    ) -> Result<Value, HarvestSqliteError> {
        let routed = self.update_routed(exec, name, input).await?;
        routed.update.result.map_err(|error| {
            HarvestSqliteError::Sqlite(SqliteError::UpdateFailed {
                execution_id: routed.exec,
                name: name.to_owned(),
                error,
            })
        })
    }

    /// [`update`](Self::update), and return the update id and the run that
    /// took it. A handler error is in the result, because history holds the
    /// admission.
    ///
    /// # Errors
    ///
    /// The errors of [`update`](Self::update), except
    /// [`SqliteError::UpdateFailed`].
    pub async fn update_routed(
        &self,
        exec: ExecutionId,
        name: &str,
        input: Value,
    ) -> Result<RoutedUpdate, HarvestSqliteError> {
        let name = name.to_owned();
        Ok(self
            .call(|reply| Command::Update {
                exec,
                name,
                input,
                reply,
            })
            .await??)
    }

    /// Read one run with its payloads.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::ExecutionNotFound`] for an unknown id, or a
    /// read error.
    pub async fn describe(
        &self,
        exec: ExecutionId,
    ) -> Result<ExecutionDescription, HarvestSqliteError> {
        self.read(move |reader| reader.describe(exec)).await
    }

    /// One page of runs, newest first.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::InvalidFilter`] for a bad filter, or a read
    /// error.
    pub async fn list(&self, filter: ExecutionFilter) -> Result<ExecutionPage, HarvestSqliteError> {
        self.read(move |reader| reader.list_executions(&filter))
            .await
    }

    /// The event history of one run, oldest first.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::ExecutionNotFound`] for an unknown id, or a
    /// read error.
    pub async fn history(
        &self,
        exec: ExecutionId,
    ) -> Result<Vec<WorkflowEvent>, HarvestSqliteError> {
        self.read(move |reader| reader.load_history(exec)).await
    }

    /// One page of the event history of one run, oldest first (issue
    /// #2489).
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::ExecutionNotFound`] for an unknown id, or a
    /// read error.
    pub async fn history_page(
        &self,
        exec: ExecutionId,
        filter: HistoryFilter,
    ) -> Result<HistoryPage, HarvestSqliteError> {
        self.read(move |reader| reader.history_page(exec, &filter))
            .await
    }

    /// The parent run of `exec`, if it is a child run.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::ExecutionNotFound`] for an unknown id, or a
    /// read error.
    pub async fn parent(
        &self,
        exec: ExecutionId,
    ) -> Result<Option<ExecutionId>, HarvestSqliteError> {
        self.read(move |reader| reader.parent(exec)).await
    }

    /// The result of the logical run that `exec` starts (issue #2489).
    ///
    /// The read follows the `continue_as_new` chain and the workflow retry
    /// chain to the last run, as `GET /workflows/{id}/result` does on
    /// Postgres. A run that is not terminal reads
    /// [`WorkflowResultState::Running`].
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::ExecutionNotFound`] for an unknown id,
    /// [`SqliteError::RetryChainTooDeep`] for a retry chain past its bound,
    /// or a read error.
    pub async fn result(&self, exec: ExecutionId) -> Result<WorkflowResult, HarvestSqliteError> {
        Ok(self.chain_result(exec).await?.result)
    }

    /// [`result`](Self::result), with the last run that the walk reached.
    pub(crate) async fn chain_result(
        &self,
        exec: ExecutionId,
    ) -> Result<ChainResult, HarvestSqliteError> {
        self.read(move |reader| {
            let (row, last, exhausted) = last_run_of_chain(reader, exec)?;
            Ok(ChainResult {
                result: compact_result(&row),
                last,
                exhausted,
            })
        })
        .await
    }

    /// The run that a by-id route acts on: the running run of the key, or
    /// else the newest run, as on Postgres.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::WorkflowIdNotFound`] when the key has no run,
    /// or a read error.
    pub async fn resolve_by_id(
        &self,
        workflow_name: &str,
        workflow_id: &str,
    ) -> Result<ExecutionId, HarvestSqliteError> {
        let (workflow_name, workflow_id) = (workflow_name.to_owned(), workflow_id.to_owned());
        self.read(move |reader| {
            let key = ExecutionFilter::new()
                .workflow_name(workflow_name.clone())
                .workflow_id(workflow_id.clone())
                .limit(1);
            for filter in [key.clone().state("RUNNING"), key] {
                let page = reader.list_executions(&filter)?;
                if let Some(exec) = page
                    .executions
                    .first()
                    .and_then(ExecutionSummary::execution_id)
                {
                    return Ok(exec);
                }
            }
            Err(SqliteError::WorkflowIdNotFound {
                workflow_name,
                workflow_id,
            })
        })
        .await
    }

    /// Check that the Harvest file answers a read.
    ///
    /// The check opens its own read-only connection. A busy shared reader,
    /// for example under many `/result` long polls, then cannot fail it.
    ///
    /// # Errors
    ///
    /// Returns the open or read error.
    pub async fn ping(&self) -> Result<(), HarvestSqliteError> {
        let path = self.inner.path.clone();
        tokio::task::spawn_blocking(move || {
            SqliteReader::open(&path)?.list_executions(&ExecutionFilter::new().limit(1))
        })
        .await
        .map_err(|error| HarvestSqliteError::Read(error.to_string()))?
        .map(|_| ())
        .map_err(HarvestSqliteError::from)
    }
}

/// The result of a chain walk (issue #2489).
pub struct ChainResult {
    /// The compact result of the last run that the walk reached.
    pub result: WorkflowResult,
    /// That run. A long poll resumes the walk from it.
    pub last: ExecutionId,
    /// `true` when the walk stopped at [`MAX_CHAIN_HOPS`] continue-as-new
    /// hops with more chain left.
    pub exhausted: bool,
}

/// The last run of the chain that `exec` starts, that run's id, and whether
/// the walk ran out of hops.
///
/// A `CONTINUED_AS_NEW` run hands over to its successor. A `FAILED` run hands
/// over to its workflow retry. The budgets follow Postgres: at most
/// [`MAX_CHAIN_HOPS`] continue-as-new hops, and at most [`MAX_RETRY_HOPS`]
/// retry hops in one retry walk. A successor row that is gone ends the walk
/// at the run before it.
fn last_run_of_chain(
    reader: &SqliteReader,
    exec: ExecutionId,
) -> Result<(ExecutionDescription, ExecutionId, bool), SqliteError> {
    let mut row = reader.describe(exec)?;
    let mut current = exec;
    let mut continued = 0_usize;
    let mut retried = 0_usize;
    loop {
        let Some(next) = reader.successor(current)? else {
            return Ok((row, current, false));
        };
        if row.summary.state.value().map(String::as_str) == Some("FAILED") {
            retried += 1;
            if retried > MAX_RETRY_HOPS {
                return Err(SqliteError::RetryChainTooDeep {
                    exec_id: exec,
                    max_depth: MAX_RETRY_HOPS,
                });
            }
        } else {
            if continued == MAX_CHAIN_HOPS {
                return Ok((row, current, true));
            }
            continued += 1;
            retried = 0;
        }
        match reader.describe(next) {
            Ok(next_row) => {
                row = next_row;
                current = next;
            }
            Err(SqliteError::ExecutionNotFound(_)) => return Ok((row, current, false)),
            Err(error) => return Err(error),
        }
    }
}

/// The compact Postgres result form of one run. It mirrors
/// `WorkflowResult::from_execution`.
fn compact_result(row: &ExecutionDescription) -> WorkflowResult {
    let state = WorkflowResultState::from_execution_state(
        row.summary.state.value().map_or("", String::as_str),
    );
    let completed_at = row.summary.closed_at.value().copied();
    let output = || {
        row.output_json
            .value()
            .and_then(|text| serde_json::from_str(text).ok())
    };
    match state {
        WorkflowResultState::Running => WorkflowResult::running(),
        WorkflowResultState::Completed | WorkflowResultState::ContinuedAsNew => WorkflowResult {
            state,
            output: output(),
            error: None,
            completed_at,
        },
        WorkflowResultState::Failed
        | WorkflowResultState::Cancelled
        | WorkflowResultState::TimedOut
        | WorkflowResultState::Terminated => WorkflowResult {
            state,
            output: None,
            error: row.error.value().cloned(),
            completed_at,
        },
    }
}

impl axum::extract::FromRequestParts<AppState> for HarvestSqlite {
    type Rejection = AutumnError;

    fn from_request_parts(
        _parts: &mut axum::http::request::Parts,
        state: &AppState,
    ) -> impl std::future::Future<Output = Result<Self, Self::Rejection>> + Send {
        std::future::ready(
            state
                .extension::<Self>()
                .map(|handle| (*handle).clone())
                .ok_or_else(|| {
                    AutumnError::service_unavailable_msg(
                        "the Harvest SQLite backend is not running yet",
                    )
                }),
        )
    }
}

#[cfg(test)]
mod tests {
    // The `#[workflow]` macro needs an `async fn`.
    #![allow(clippy::unused_async)]

    use super::*;
    use autumn_harvest::prelude::*;
    use autumn_harvest_sqlite::{RunState, SqliteRuntime};

    /// Continues as new until its input reaches the stop value.
    #[workflow]
    async fn counts(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
        if n < STOP {
            ctx.continue_as_new(serde_json::json!(n + 1))
                .await
                .map_err(|error| error.to_string())?;
        }
        Ok(n)
    }

    /// Two hops past the bound of a `/result` walk.
    const STOP: i64 = 130;

    #[tokio::test]
    async fn a_result_walk_stops_at_the_continue_as_new_bound() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("h.db");
        let mut runtime = SqliteRuntime::open(&path).unwrap();
        runtime.register_workflow(&counts_info());
        let first = runtime
            .start_workflow("counts", serde_json::json!(0))
            .unwrap();
        let mut runs = vec![first];
        loop {
            match runtime
                .run_until_blocked(runs[runs.len() - 1])
                .await
                .unwrap()
            {
                RunState::ContinuedAsNew(next) => runs.push(next),
                RunState::Completed(_) => break,
                other => panic!("unexpected {other:?}"),
            }
        }
        let reader = SqliteReader::open(&path).unwrap();

        let (row, last, exhausted) = last_run_of_chain(&reader, first).unwrap();
        assert!(exhausted, "the chain is longer than the bound");
        assert_eq!(last, runs[MAX_CHAIN_HOPS]);
        assert_eq!(
            compact_result(&row).state,
            WorkflowResultState::ContinuedAsNew
        );

        // A walk that starts inside the bound reaches the end.
        let (row, last, exhausted) = last_run_of_chain(&reader, runs[10]).unwrap();
        assert!(!exhausted);
        assert_eq!(last, runs[runs.len() - 1]);
        assert_eq!(compact_result(&row).output, Some(serde_json::json!(STOP)));
    }
}
