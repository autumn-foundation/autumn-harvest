//! [`HarvestSqlite`], the app-facing client of the SQLite backend (issue
//! #2467).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use autumn_harvest::info::{SchemaViolation, WorkflowInfo};
use autumn_harvest::{ExecutionId, WorkflowEvent};
use autumn_harvest_sqlite::{
    ExecutionDescription, ExecutionFilter, ExecutionPage, SignalDelivery, SignalWithStartOutcome,
    SqliteError, SqliteReader, StartOptions, StartOutcome,
};
use autumn_web::AppState;
use autumn_web::error::AutumnError;
use autumn_web::fleet_layout::FleetDbKey;
use autumn_web::reexports::axum;
use autumn_web::sharding::ShardLease;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

use super::driver::{Command, SignalWithStartParams};

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
        let (workflow_name, workflow_id, name) = (
            workflow_name.to_owned(),
            workflow_id.to_owned(),
            name.to_owned(),
        );
        Self::lift(
            self.call(|reply| Command::SignalByWorkflowId {
                workflow_name,
                workflow_id,
                name,
                payload,
                idempotency_key,
                reply,
            })
            .await?,
        )
    }

    /// Ask a run to cancel. The next drive replays it with the reason set.
    ///
    /// # Errors
    ///
    /// Returns the backend's refusal or [`HarvestSqliteError::Stopped`].
    pub async fn cancel(&self, exec: ExecutionId, reason: &str) -> Result<(), HarvestSqliteError> {
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
    /// # Errors
    ///
    /// Returns the backend's refusal or [`HarvestSqliteError::Stopped`].
    pub async fn terminate(
        &self,
        exec: ExecutionId,
        reason: &str,
    ) -> Result<(), HarvestSqliteError> {
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
    /// Returns the backend's refusal or [`HarvestSqliteError::Stopped`].
    pub async fn update(
        &self,
        exec: ExecutionId,
        name: &str,
        input: Value,
    ) -> Result<Value, HarvestSqliteError> {
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
