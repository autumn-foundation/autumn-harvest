//! In-process workflow result handles for request/response embedders.
//!
//! `WorkflowHandle` is the small client-side primitive for code that starts a
//! workflow behind an HTTP route and wants to await the terminal result without
//! polling the full event history.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use diesel::ExpressionMethods;
use diesel::OptionalExtension;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::completion_trigger::DeferredTriggerStart;
use crate::error::{HarvestError, HarvestResult, TimeoutType, database_error};
use crate::execution::{
    CancelledWorkflowExecution, StartWorkflowParams, StartedWorkflowExecution,
    cancel_workflow_execution, check_and_report_unfinished_handlers,
    start_or_load_workflow_execution, start_or_load_workflow_execution_collect,
    terminate_workflow_execution,
};
use crate::models::WorkflowExecution;
use crate::notify::{WorkflowEventListener, WorkflowEventWaitOutcome};
use crate::schema::harvest_workflow_executions;
use crate::shard::{ShardPlacement, ShardRouter, ShardedDbPool};
use crate::start_idempotency::{
    DEFAULT_START_IDEMPOTENCY_WINDOW, StartIdempotencyReservation, repoint_start_idempotency_claim,
    reserve_start_idempotency,
};
use crate::types::{
    ExecutionId, ShardId, StartSource, WorkflowIdConflictPolicy, WorkflowIdReusePolicy,
};
use crate::worker::DbPool;

/// Compact public state for a workflow result response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowResultState {
    /// Execution is still active.
    Running,
    /// Execution completed successfully and may have an output payload.
    Completed,
    /// Execution failed permanently.
    Failed,
    /// Execution was explicitly cancelled.
    Cancelled,
    /// Execution exceeded its workflow-level timeout.
    TimedOut,
    /// Execution was forcefully terminated.
    Terminated,
    /// Execution sealed itself and started a successor run.
    ContinuedAsNew,
}

impl WorkflowResultState {
    /// Convert a stored execution state into the compact API state.
    #[must_use]
    pub fn from_execution_state(state: &str) -> Self {
        match state {
            "COMPLETED" => Self::Completed,
            "FAILED" => Self::Failed,
            "CANCELLED" => Self::Cancelled,
            "TIMED_OUT" => Self::TimedOut,
            "TERMINATED" => Self::Terminated,
            "CONTINUED_AS_NEW" => Self::ContinuedAsNew,
            _ => Self::Running,
        }
    }

    /// Whether this state is terminal for the current execution.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Running)
    }
}

/// Compact workflow result payload.
///
/// This intentionally omits event history. The management API uses the same
/// shape for `GET /workflows/{id}/result`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowResult {
    /// Current compact state.
    pub state: WorkflowResultState,
    /// Present only for successful terminal states with a result payload.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    /// Present only for failed/cancelled/timed-out/terminated states.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Timestamp when the execution entered a terminal state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl WorkflowResult {
    /// Build a compact result from a workflow execution row.
    #[must_use]
    pub fn from_execution(execution: &WorkflowExecution) -> Self {
        let state = WorkflowResultState::from_execution_state(&execution.state);
        match state {
            WorkflowResultState::Completed | WorkflowResultState::ContinuedAsNew => Self {
                state,
                output: execution.output.clone(),
                error: None,
                completed_at: execution.completed_at,
            },
            WorkflowResultState::Failed
            | WorkflowResultState::Cancelled
            | WorkflowResultState::TimedOut
            | WorkflowResultState::Terminated => Self {
                state,
                output: None,
                error: execution.error.clone(),
                completed_at: execution.completed_at,
            },
            WorkflowResultState::Running => Self {
                state,
                output: None,
                error: None,
                completed_at: None,
            },
        }
    }

    /// Build a successful terminal result.
    #[must_use]
    pub const fn completed(
        state: WorkflowResultState,
        output: Value,
        completed_at: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Self {
        Self {
            state,
            output: Some(output),
            error: None,
            completed_at,
        }
    }

    /// Build a non-terminal result.
    #[must_use]
    pub const fn running() -> Self {
        Self {
            state: WorkflowResultState::Running,
            output: None,
            error: None,
            completed_at: None,
        }
    }

    /// Whether this result represents a terminal execution state.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }
}

#[derive(Clone)]
struct WorkflowHandleClientInner {
    pools: ShardedDbPool,
    router: ShardRouter,
    notification_database_urls: BTreeMap<ShardId, String>,
    payload_codecs: crate::payload_codec::PayloadCodecs,
    shared_state: crate::context::SharedState,
    update_handlers: Vec<crate::info::UpdateHandlerInfo>,
    query_handlers: Vec<crate::info::QueryHandlerInfo>,
    /// Registered workflow metadata, keyed by [`crate::info::WorkflowInfo::name`]
    /// (issue #763). Used by [`WorkflowHandleClient::start_workflow_transactional`]
    /// to resolve `WorkflowInfo` defaults (execution timeout, sla, concurrency
    /// key, input schema) the same way the HTTP start route does. Empty by
    /// default — set via [`WorkflowHandleClient::with_workflows`].
    workflows: HashMap<String, crate::info::WorkflowInfo>,
    max_workflow_input_bytes: u64,
    max_workflow_execution_timeout: Option<Duration>,
    /// Server-side ceiling on the chain-scoped lifetime cap AND fleet-wide chain
    /// default (issue #617). `None` = no chain cap applied fleet-wide.
    max_workflow_chain_timeout: Option<Duration>,
    max_workflow_start_delay: Duration,
    max_signal_payload_bytes: u64,
    query_timeout: Duration,
    history_policy: crate::context::WorkflowHistoryPolicy,
    /// Default max-wait cap for debounced starts when the policy omits `max_wait`.
    default_debounce_max_wait: Duration,
    /// Server-side ceiling on workflow retry attempts (issue #523). `None` = no ceiling.
    max_workflow_attempts: Option<u32>,
    /// Engine metrics recorder (issue #684). Wired by the runtime so the
    /// in-process update path (`execute_update_in_process`) can emit
    /// `harvest.update.admitted` at admission, matching the HTTP/UI paths.
    /// Defaults to a no-op recorder when the client is built without one.
    metrics: Arc<dyn crate::telemetry::MetricsRecorder>,
}

impl std::fmt::Debug for WorkflowHandleClientInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowHandleClientInner")
            .field("pools", &self.pools)
            .field("router", &self.router)
            .field(
                "notification_database_urls",
                &self.notification_database_urls,
            )
            .field("payload_codecs", &"<PayloadCodecs>")
            .field("shared_state", &"<SharedState>")
            .field("update_handlers_count", &self.update_handlers.len())
            .field("query_handlers_count", &self.query_handlers.len())
            .field("workflows_count", &self.workflows.len())
            .field("max_workflow_input_bytes", &self.max_workflow_input_bytes)
            .field(
                "max_workflow_execution_timeout",
                &self.max_workflow_execution_timeout,
            )
            .field(
                "max_workflow_chain_timeout",
                &self.max_workflow_chain_timeout,
            )
            .field("max_workflow_start_delay", &self.max_workflow_start_delay)
            .field("max_signal_payload_bytes", &self.max_signal_payload_bytes)
            .field("query_timeout", &self.query_timeout)
            .field("history_policy", &self.history_policy)
            .field("default_debounce_max_wait", &self.default_debounce_max_wait)
            .field("max_workflow_attempts", &self.max_workflow_attempts)
            .field("metrics", &"<MetricsRecorder>")
            .finish()
    }
}

/// Factory for shard-aware workflow handles.
///
/// The client owns the storage pools, shard router, and per-shard database URLs
/// used for LISTEN/NOTIFY. A handle cloned from this client routes reads through
/// [`ShardRouter::shard_for_execution`] using its `ExecutionId`.
#[derive(Debug, Clone)]
pub struct WorkflowHandleClient {
    inner: Arc<WorkflowHandleClientInner>,
}

impl WorkflowHandleClient {
    /// Build a single-shard handle client.
    #[must_use]
    pub fn single(pool: DbPool, notification_database_url: impl Into<String>) -> Self {
        Self::new(
            ShardedDbPool::single(pool),
            ShardRouter::single(),
            [(ShardId::new(0), notification_database_url)],
        )
    }

    /// Build a client for a sharded deployment.
    ///
    /// `notification_database_urls` must include every readable shard that may
    /// be awaited. Missing entries are reported as [`HarvestError::Config`]
    /// when a handle tries to wait on that shard.
    #[must_use]
    pub fn new<I, S>(
        pools: ShardedDbPool,
        router: ShardRouter,
        notification_database_urls: I,
    ) -> Self
    where
        I: IntoIterator<Item = (ShardId, S)>,
        S: Into<String>,
    {
        Self {
            inner: Arc::new(WorkflowHandleClientInner {
                pools,
                router,
                notification_database_urls: notification_database_urls
                    .into_iter()
                    .map(|(shard, url)| (shard, url.into()))
                    .collect(),
                payload_codecs: crate::payload_codec::PayloadCodecs::default(),
                shared_state: crate::context::empty_shared_state(),
                update_handlers: Vec::new(),
                query_handlers: Vec::new(),
                workflows: HashMap::new(),
                max_workflow_input_bytes: crate::builder::DEFAULT_MAX_WORKFLOW_INPUT_BYTES,
                max_workflow_execution_timeout: None,
                max_workflow_chain_timeout: None,
                max_workflow_start_delay: crate::builder::DEFAULT_MAX_WORKFLOW_START_DELAY,
                max_signal_payload_bytes: crate::builder::DEFAULT_MAX_SIGNAL_PAYLOAD_BYTES,
                query_timeout: Duration::from_secs(5),
                history_policy: crate::context::WorkflowHistoryPolicy::default(),
                default_debounce_max_wait: crate::builder::DEFAULT_DEBOUNCE_MAX_WAIT,
                max_workflow_attempts: None,
                metrics: Arc::new(crate::telemetry::NoOpMetrics),
            }),
        }
    }

    /// Wire the engine metrics recorder into the client (issue #684).
    ///
    /// The in-process update path emits `harvest.update.admitted` at admission,
    /// matching the HTTP and Vantage-UI paths. Defaults to a no-op recorder.
    #[must_use]
    pub fn with_metrics(self, metrics: Arc<dyn crate::telemetry::MetricsRecorder>) -> Self {
        let mut inner = (*self.inner).clone();
        inner.metrics = metrics;
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Add custom payload codecs to the client.
    #[must_use]
    pub fn with_codecs(self, codecs: crate::payload_codec::PayloadCodecs) -> Self {
        let mut inner = (*self.inner).clone();
        inner.payload_codecs = codecs;
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Add shared state to the client.
    #[must_use]
    pub fn with_shared_state(self, shared_state: crate::context::SharedState) -> Self {
        let mut inner = (*self.inner).clone();
        inner.shared_state = shared_state;
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Add query and update handlers to the client.
    #[must_use]
    pub fn with_handlers(
        self,
        query_handlers: Vec<crate::info::QueryHandlerInfo>,
        update_handlers: Vec<crate::info::UpdateHandlerInfo>,
    ) -> Self {
        let mut inner = (*self.inner).clone();
        inner.query_handlers = query_handlers;
        inner.update_handlers = update_handlers;
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Add registered workflow metadata to the client (issue #763).
    ///
    /// Used by [`Self::start_workflow_transactional`] to resolve `WorkflowInfo`
    /// defaults (execution timeout, sla, concurrency key) and published
    /// input-schema validation, the same way the HTTP `POST /workflows/{name}/start`
    /// route does. A workflow name absent from this set is rejected by
    /// `start_workflow_transactional` with [`HarvestError::Config`].
    #[must_use]
    pub fn with_workflows(
        self,
        workflows: impl IntoIterator<Item = crate::info::WorkflowInfo>,
    ) -> Self {
        let mut inner = (*self.inner).clone();
        inner.workflows = workflows
            .into_iter()
            .map(|w| (w.name.to_string(), w))
            .collect();
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Add the global max workflow input bytes to the client.
    #[must_use]
    pub fn with_max_workflow_input_bytes(self, bytes: u64) -> Self {
        let mut inner = (*self.inner).clone();
        inner.max_workflow_input_bytes = bytes;
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Add the global max workflow execution timeout ceiling to the client.
    #[must_use]
    pub fn with_max_workflow_execution_timeout(self, ceiling: Option<Duration>) -> Self {
        let mut inner = (*self.inner).clone();
        inner.max_workflow_execution_timeout = ceiling;
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Add the global chain-scoped lifetime cap ceiling to the client (issue #617).
    #[must_use]
    pub fn with_max_workflow_chain_timeout(self, ceiling: Option<Duration>) -> Self {
        let mut inner = (*self.inner).clone();
        inner.max_workflow_chain_timeout = ceiling;
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Add the global max workflow start delay to the client.
    #[must_use]
    pub fn with_max_workflow_start_delay(self, ceiling: Duration) -> Self {
        let mut inner = (*self.inner).clone();
        inner.max_workflow_start_delay = ceiling;
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Add the global max signal payload bytes to the client.
    #[must_use]
    pub fn with_max_signal_payload_bytes(self, bytes: u64) -> Self {
        let mut inner = (*self.inner).clone();
        inner.max_signal_payload_bytes = bytes;
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Add the global query timeout to the client.
    #[must_use]
    pub fn with_query_timeout(self, timeout: Duration) -> Self {
        let mut inner = (*self.inner).clone();
        inner.query_timeout = timeout;
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Add history size policies to the client.
    #[must_use]
    pub fn with_history_policy(
        self,
        history_policy: crate::context::WorkflowHistoryPolicy,
    ) -> Self {
        let mut inner = (*self.inner).clone();
        inner.history_policy = history_policy;
        Self {
            inner: Arc::new(inner),
        }
    }
    /// Set the default max-wait cap for debounced starts.
    ///
    /// Applied when a `DebouncePolicy.max_wait` is `None`. Mirrors
    /// [`WorkerConfig::with_default_debounce_max_wait`](crate::builder::WorkerConfig::with_default_debounce_max_wait).
    /// Defaults to 1 hour.
    #[must_use]
    pub fn with_default_debounce_max_wait(self, max_wait: Duration) -> Self {
        let mut inner = (*self.inner).clone();
        inner.default_debounce_max_wait = max_wait;
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Get the default debounce max-wait cap.
    #[must_use]
    pub fn default_debounce_max_wait(&self) -> Duration {
        self.inner.default_debounce_max_wait
    }

    /// Get the server-side ceiling on workflow retry attempts (issue #523).
    #[must_use]
    pub fn max_workflow_attempts(&self) -> Option<u32> {
        self.inner.max_workflow_attempts
    }

    /// Set the server-side ceiling on workflow retry attempts (issue #523).
    #[must_use]
    pub fn with_max_workflow_attempts(self, ceiling: Option<u32>) -> Self {
        let mut inner = (*self.inner).clone();
        inner.max_workflow_attempts = ceiling;
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Get the maximum allowed execution timeout.
    #[must_use]
    pub fn max_workflow_execution_timeout(&self) -> Option<Duration> {
        self.inner.max_workflow_execution_timeout
    }

    /// Get the chain-scoped lifetime cap ceiling (issue #617). Doubles as the
    /// fleet-wide chain default.
    #[must_use]
    pub fn max_workflow_chain_timeout(&self) -> Option<Duration> {
        self.inner.max_workflow_chain_timeout
    }

    /// Get the maximum allowed workflow start delay.
    #[must_use]
    pub fn max_workflow_start_delay(&self) -> Duration {
        self.inner.max_workflow_start_delay
    }

    /// Get the maximum allowed signal payload bytes.
    #[must_use]
    pub fn max_signal_payload_bytes(&self) -> u64 {
        self.inner.max_signal_payload_bytes
    }

    /// Get the query timeout.
    #[must_use]
    pub fn query_timeout(&self) -> Duration {
        self.inner.query_timeout
    }

    /// Get the effective max workflow input bytes for a workflow.
    #[must_use]
    pub fn max_workflow_input_bytes(&self, per_workflow_override: Option<u64>) -> u64 {
        per_workflow_override.map_or(self.inner.max_workflow_input_bytes, |per_wf| {
            per_wf.max(self.inner.max_workflow_input_bytes)
        })
    }

    /// Pick which shard a new workflow execution with `(name, id)` should land on.
    #[must_use]
    pub fn pick_shard_for_new_workflow(&self, workflow_name: &str, workflow_id: &str) -> ShardId {
        self.inner
            .router
            .pick_for_new_workflow(workflow_name, workflow_id)
    }

    /// Resolve an explicit [`ShardPlacement`] for a new workflow (issue #697).
    ///
    /// [`ShardPlacement::Auto`] is exactly [`Self::pick_shard_for_new_workflow`];
    /// the explicit variants pin the workflow (and, by shard inheritance, its
    /// whole descendant tree) to a residency-controlled database.
    ///
    /// # Scope
    ///
    /// This **resolves** a placement against this client's router; it does not
    /// by itself place anything. The in-process start APIs
    /// ([`StartWorkflowParams`] and the typed client stubs) carry no placement
    /// field, so they always route by hash — placement is exposed on the HTTP
    /// start route and the CLI. Use this to compute or validate a shard
    /// (e.g. to pre-flight an operator tool), not on the assumption that a
    /// subsequent in-process start will honour it.
    ///
    /// # Errors
    ///
    /// Propagates [`ShardPlacementError`] as [`HarvestError::Config`] when the
    /// requested shard is unknown or drained, or the residency key is blank or
    /// undeclared.
    pub fn resolve_shard_placement(
        &self,
        placement: &ShardPlacement,
        workflow_name: &str,
        workflow_id: &str,
    ) -> HarvestResult<ShardId> {
        self.inner
            .router
            .resolve_placement(placement, workflow_name, workflow_id)
            .map_err(HarvestError::from)
    }

    /// Create a handle for an existing workflow execution.
    #[must_use]
    pub fn handle(&self, exec_id: ExecutionId) -> WorkflowHandle {
        WorkflowHandle {
            exec_id,
            client: self.clone(),
        }
    }

    /// Start or load a workflow, returning the normal start metadata and a
    /// result handle for the resolved execution.
    ///
    /// # Errors
    ///
    /// Propagates [`start_or_load_workflow_execution`] failures.
    pub async fn start_or_load(
        &self,
        conn: &mut AsyncPgConnection,
        request: StartWorkflowParams<'_>,
    ) -> HarvestResult<StartedWorkflowHandle> {
        let started = start_or_load_workflow_execution(conn, request, None).await?;
        let handle = self.handle(started.exec_id);
        Ok(StartedWorkflowHandle { started, handle })
    }

    /// Stage a workflow start on a caller-owned Diesel connection or
    /// transaction so it commits or rolls back atomically with the caller's
    /// own domain write (issue #763).
    ///
    /// `conn` may be a plain connection or an already-open transaction on the
    /// same connection — either way, this call composes correctly via Diesel's
    /// nested-transaction (`SAVEPOINT`) semantics: it never opens a *new*
    /// top-level transaction of its own. The `WorkflowStarted` event, the
    /// execution row, and the initial dispatchable task-queue row all live only
    /// inside whatever transaction is open on `conn` at the time this call
    /// starts. If the caller later rolls that transaction back, none of them
    /// exist and no worker ever claims the run.
    ///
    /// `workflow_name` must have been registered on this client via
    /// [`Self::with_workflows`]; that registration also supplies the
    /// `WorkflowInfo` defaults (`execution_timeout`, `sla`, concurrency key,
    /// input schema) applied here, mirroring `POST /workflows/{name}/start`.
    ///
    /// Returns synchronously with the assigned `ExecutionId` so the caller can
    /// persist it on their own domain row inside the same transaction. Call
    /// [`TransactionalStartOutcome::finish`] **after** the caller's transaction
    /// has committed to eagerly dispatch any deferred completion-trigger
    /// follow-ups the start produced; this is an optional latency optimization
    /// (a periodic scanner delivers the same follow-ups if `finish` is never
    /// called), never a correctness requirement.
    ///
    /// In a sharded deployment the started workflow lands on the shard given by
    /// [`TransactionalStartOptions::with_shard`] — the shard backing `conn` —
    /// never a freshly rendezvous-hashed shard, since a cross-shard transaction
    /// is impossible. **A multi-shard client requires `.with_shard(...)`**:
    /// omitting it would mint an execution id whose *encoded* shard (used by
    /// every later exec-id-routed lookup — signal, cancel, describe, the
    /// timeout scanner, ...) has no relationship to wherever `conn` actually
    /// writes the row, silently producing an execution that commits
    /// successfully but can never be found again. This method therefore
    /// rejects with [`HarvestError::Config`] rather than guessing. A
    /// single-shard client (built via [`Self::single`]) has no such ambiguity
    /// and needs no `.with_shard(...)` call at all — the caller's connection
    /// is definitionally on the only shard there is.
    ///
    /// # Errors
    ///
    /// - [`HarvestError::Config`] when `workflow_name` is not registered on
    ///   this client; when the target workflow is configured with a debounce,
    ///   batch, or throttle policy (those require deferred admission,
    ///   possibly to a *different* `ExecutionId`, which is structurally
    ///   incompatible with returning one synchronously inside the caller's
    ///   transaction); or when this client is registered with more than one
    ///   shard and `options` did not specify one via
    ///   [`TransactionalStartOptions::with_shard`].
    /// - [`HarvestError::InputValidationFailed`] when `input` fails the
    ///   workflow's published JSON Schema (issue #373). Nothing is written —
    ///   validation runs before any database call.
    /// - [`HarvestError::AlreadyExists`] when the configured id-reuse /
    ///   conflict policy rejects a duplicate `(workflow_name, workflow_id)`.
    /// - Propagates database / queue / event-store failures.
    pub async fn start_workflow_transactional(
        &self,
        conn: &mut AsyncPgConnection,
        workflow_name: &str,
        workflow_id: &str,
        input: Value,
        options: TransactionalStartOptions,
    ) -> HarvestResult<TransactionalStartOutcome> {
        let info = self.validate_transactional_start_request(
            workflow_name,
            workflow_id,
            &input,
            &options,
        )?;

        let shard = options.shard.unwrap_or(ShardId::UNENCODED);
        let exec_id = ExecutionId::new_for_shard(shard);
        let queue_name = Self::resolve_transactional_queue_name(&options);
        let defaults = self.resolve_transactional_start_defaults(info, &input);

        let params = StartWorkflowParams {
            workflow_name,
            workflow_id,
            exec_id,
            input,
            parent_id: options.parent_id,
            queue_name: &queue_name,
            execution_timeout: defaults.execution_timeout,
            memo: options.memo.clone(),
            search_attrs: options.search_attrs.clone(),
            reuse_policy: options.reuse_policy,
            conflict_policy: options.conflict_policy,
            trace_context: None,
            max_execution_timeout_ceiling: defaults.max_workflow_execution_timeout_ceiling,
            chain_execution_timeout: defaults.chain_execution_timeout,
            max_workflow_chain_timeout_ceiling: defaults.max_workflow_chain_timeout_ceiling,
            inherited_chain_deadline_at: None,
            concurrency_key: defaults.concurrency_key,
            concurrency_limit: defaults.concurrency_limit,
            priority: crate::types::Priority::default(),
            max_workflow_input_bytes: defaults.max_workflow_input_bytes,
            start_at: None,
            delay: None,
            max_workflow_start_delay: Some(defaults.max_workflow_start_delay),
            owner: info.owner,
            runbook_url: info.runbook_url,
            severity: info.severity,
            context_headers: None,
            sla: defaults.sla,
            schedule_id: None,
            scheduled_for: None,
            // 1 = first attempt (see `StartWorkflowParams::workflow_attempt`'s doc
            // comment). Every other fresh-start call site in the workspace uses
            // `1`; using `0` here would silently grant one extra retry attempt
            // beyond a workflow's configured `max_attempts` (e.g. a
            // `max_attempts = 1` "never retry" policy would still retry once,
            // since `0 >= 1` is false on the first attempt).
            workflow_attempt: 1,
            workflow_retry_policy: info.retry_policy.clone(),
            retry_of_exec_id: None,
            max_workflow_attempts_ceiling: self.inner.max_workflow_attempts,
            origin: None,
            completion_callbacks: None,
            start_source: StartSource::Transactional,
            start_source_ref: None,
            started_by: None,
        };

        // Gated via `GateMode::CheckCached` (issue #618/#377 "halt beats pace"),
        // not `GateMode::Check` — the same choice `throttle.rs`/
        // `completion_trigger.rs` make for other in-process producers that may
        // run without a `HarvestPlugin` in the same process. `CheckCached` never
        // fails closed: `evaluate_start_gate` returns `None` immediately when no
        // process-global gate cache was ever published (i.e. no plugin/manual
        // caller has installed one), so a standalone embedder who never wires up
        // gates is completely unaffected. When a plugin *is* running in this
        // process (the dominant deployment shape — `HarvestPlugin::build()`
        // injects an already-`.with_workflows(...)`-populated
        // `WorkflowHandleClient` into `AppState` for exactly this API) an active
        // fleet-wide/queue/workflow-name gate now blocks a matching transactional
        // start and increments `harvest.admission.blocked`, exactly like every
        // other in-process producer. See `StartProducer::Transactional` in the
        // admission-gate contract (`admission_gate.rs`) for the full rationale.
        let (started, deferred_starts, deferred_checks, cancel_metrics) =
            if let Some(key) = options.idempotency_key.as_deref() {
                self.start_workflow_transactional_idempotent(conn, params, key)
                    .await?
            } else {
                start_or_load_workflow_execution_collect(
                    conn,
                    params,
                    /* in_outer_transaction = */ true,
                    /* reject_fresh_if_debounced = */ false,
                    Some(self.inner.metrics.as_ref()
                        as &(dyn crate::telemetry::MetricsRecorder + Send + Sync)),
                    Some(crate::admission_gate::GateMode::CheckCached),
                )
                .await?
            };

        Ok(TransactionalStartOutcome {
            exec_id: started.exec_id,
            workflow_name: started.workflow_name,
            workflow_id: started.workflow_id,
            state: started.state,
            created: started.created,
            client: self.clone(),
            shard,
            deferred: PendingStartFollowUps {
                starts: deferred_starts,
                checks: deferred_checks,
                cancel_metrics,
            },
        })
    }

    /// Validate a transactional-start request before touching the database
    /// (issue #763). Consolidates the five fast-fail checks
    /// [`Self::start_workflow_transactional`] must run before it can safely
    /// resolve a shard/exec-id and build [`StartWorkflowParams`]: the target
    /// workflow must be registered on this client, must not be configured
    /// with a debounce/batch/throttle policy (all three defer admission,
    /// possibly to a *different* `ExecutionId`, which cannot be returned
    /// synchronously), `input` must satisfy the workflow's published JSON
    /// Schema when one is set, — for a client spanning more than one shard —
    /// the caller must have named which shard backs their connection via
    /// [`TransactionalStartOptions::with_shard`], and an explicitly-named
    /// shard must actually be a member of this client's router (readable
    /// *and* writable), not merely *some* shard id. Split out of
    /// [`Self::start_workflow_transactional`] purely to keep that function
    /// under the workspace's line-count lint.
    fn validate_transactional_start_request(
        &self,
        workflow_name: &str,
        workflow_id: &str,
        input: &Value,
        options: &TransactionalStartOptions,
    ) -> HarvestResult<&crate::info::WorkflowInfo> {
        let info = self.inner.workflows.get(workflow_name).ok_or_else(|| {
            HarvestError::Config(format!(
                "workflow '{workflow_name}' is not registered on this \
                 WorkflowHandleClient; pass it to `.with_workflows(...)` when \
                 constructing the client (issue #763)"
            ))
        })?;

        if info.debounce.is_some() || info.batch.is_some() || info.throttle.is_some() {
            return Err(HarvestError::Config(format!(
                "workflow '{workflow_name}' is configured with a debounce, batch, or \
                 throttle policy, which defers admission (possibly to a different \
                 ExecutionId) and cannot return one synchronously; transactional start \
                 is not supported for it (issue #763)"
            )));
        }

        info.validate_input(input)
            .map_err(|violations| HarvestError::InputValidationFailed { violations })?;

        match options.shard {
            None => {
                if self.inner.pools.len() > 1 {
                    return Err(HarvestError::Config(format!(
                        "this WorkflowHandleClient is registered with {} shards, so \
                         `start_workflow_transactional` cannot infer which one the caller's \
                         connection belongs to; pass \
                         `TransactionalStartOptions::new().with_shard(...)` naming the shard \
                         that owns `conn` (issue #763). Leaving the shard unspecified would \
                         mint an execution id whose *encoded* shard disagrees with wherever \
                         `conn` actually writes the row, making the execution unreachable by \
                         any later exec-id-routed lookup (signal, cancel, describe, ...) even \
                         though the row itself committed successfully.",
                        self.inner.pools.len()
                    )));
                }
            }
            Some(shard) => {
                // A caller-supplied shard must be a real, currently-writable member
                // of this client's router — never merely "some `ShardId`". A shard
                // that is unregistered (typo, stale config, topology drift between
                // wherever the caller sourced their `ShardId` and how *this* client
                // was constructed) or has been drained out of `writable_shards`
                // would otherwise mint an exec id that writes successfully via
                // `conn` yet silently routes every later exec-id lookup (signal,
                // cancel, describe, the timeout scanner) to the *default* shard
                // instead — the row commits but becomes permanently unreachable
                // through this client. `resolve_shard_placement` is the same
                // validated primitive the HTTP start route's `shard_id` option uses
                // (issue #697); reusing it here closes exactly that gap.
                self.resolve_shard_placement(
                    &ShardPlacement::Shard(shard),
                    workflow_name,
                    workflow_id,
                )?;
            }
        }

        Ok(info)
    }

    /// Resolve the effective task-queue name for a transactional start (issue
    /// #763). Mirrors `completion_trigger::default_workflow_queue()`: the
    /// process-global registered default queue (set at plugin boot from the
    /// worker's own configured queue list), else the literal `"default"`.
    /// Shard-independent, so it is always safe to read here. Split out of
    /// [`Self::start_workflow_transactional`] purely to keep that function
    /// under the workspace's line-count lint.
    fn resolve_transactional_queue_name(options: &TransactionalStartOptions) -> String {
        options.queue_name.clone().unwrap_or_else(|| {
            crate::completion_trigger::GLOBAL_DEFAULT_WORKFLOW_QUEUE
                .read()
                .ok()
                .and_then(|lock| lock.clone())
                .unwrap_or_else(|| "default".to_string())
        })
    }

    /// Resolve every `WorkflowInfo`-derived (and client-ceiling-derived)
    /// default for a transactional start (issue #763), mirroring
    /// `POST /workflows/{name}/start`'s resolution exactly. Returns owned
    /// values only (no borrows into `info`/`input`), so it can be called
    /// independently of the [`StartWorkflowParams`] it feeds — split out of
    /// [`Self::start_workflow_transactional`] purely to keep that function
    /// under the workspace's line-count lint.
    fn resolve_transactional_start_defaults(
        &self,
        info: &crate::info::WorkflowInfo,
        input: &Value,
    ) -> ResolvedStartDefaults {
        let concurrency_key = info
            .concurrency
            .as_ref()
            .and_then(|policy| crate::concurrency::resolve_concurrency_key(policy.key_expr, input));
        let concurrency_limit = info.concurrency.as_ref().map(|policy| policy.limit);

        let execution_timeout = info
            .execution_timeout
            .and_then(|d| chrono::Duration::from_std(d).ok());
        // Effective SLA clamped to at most the hard execution timeout, mirroring
        // the HTTP start route's resolution (never a soft deadline past the hard
        // one).
        let sla = info
            .sla
            .and_then(|d| chrono::Duration::from_std(d).ok())
            .map(|sla| execution_timeout.map_or(sla, |hard| sla.min(hard)));
        let chain_execution_timeout = info
            .chain_execution_timeout
            .and_then(|d| chrono::Duration::from_std(d).ok());
        let max_workflow_input_bytes = self.max_workflow_input_bytes(info.max_input_bytes);
        let max_workflow_execution_timeout_ceiling = self
            .inner
            .max_workflow_execution_timeout
            .and_then(|d| chrono::Duration::from_std(d).ok());
        let max_workflow_chain_timeout_ceiling = self
            .inner
            .max_workflow_chain_timeout
            .and_then(|d| chrono::Duration::from_std(d).ok());
        let max_workflow_start_delay =
            chrono::Duration::from_std(self.inner.max_workflow_start_delay)
                .unwrap_or_else(|_| chrono::Duration::days(365));

        ResolvedStartDefaults {
            concurrency_key,
            concurrency_limit,
            execution_timeout,
            sla,
            chain_execution_timeout,
            max_workflow_input_bytes,
            max_workflow_execution_timeout_ceiling,
            max_workflow_chain_timeout_ceiling,
            max_workflow_start_delay,
        }
    }

    /// Idempotency-key variant of the transactional start (issue #808 semantics
    /// reimplemented for issue #763).
    ///
    /// [`start_or_load_workflow_execution_idempotent`](crate::execution::start_or_load_workflow_execution_idempotent)
    /// is not reused here: it unconditionally spawns deferred follow-ups the
    /// instant its own internal transaction returns `Ok`, assuming that
    /// transaction was the outermost one. When nested (via `SAVEPOINT`) inside
    /// a caller's own still-open outer transaction, that assumption is false —
    /// the follow-ups would be dispatched (via a fresh, independently-committed
    /// connection) even if the caller's outer transaction is later rolled back,
    /// violating the atomicity this API exists to provide. This method performs
    /// the identical reserve-then-start sequence but returns the deferred
    /// follow-ups to the caller instead, exactly like the non-keyed path above.
    async fn start_workflow_transactional_idempotent(
        &self,
        conn: &mut AsyncPgConnection,
        params: StartWorkflowParams<'_>,
        idempotency_key: &str,
    ) -> HarvestResult<(
        StartedWorkflowExecution,
        Vec<DeferredTriggerStart>,
        Vec<(ExecutionId, String)>,
        Vec<(String, String)>,
    )> {
        let new_exec_id = params.exec_id;
        let shard_id = params.shard_id();
        let metrics =
            Some(self.inner.metrics.as_ref()
                as &(dyn crate::telemetry::MetricsRecorder + Send + Sync));

        // Wrapped in its own transaction (a nested `SAVEPOINT` when `conn` is
        // already inside the caller's outer transaction) so the reservation
        // UPSERT and the start below are atomic *together*, exactly like
        // `start_or_load_workflow_execution_idempotent`: a start failure after a
        // successful reservation rolls the reservation back too, rather than
        // leaving a dangling claim inside the caller's still-open transaction.
        Box::pin(conn.transaction::<(
            StartedWorkflowExecution,
            Vec<DeferredTriggerStart>,
            Vec<(ExecutionId, String)>,
            Vec<(String, String)>,
        ), HarvestError, _>(async |conn| {
            let params = params;
            let workflow_name = params.workflow_name.to_string();
            match reserve_start_idempotency(
                conn,
                &workflow_name,
                idempotency_key,
                new_exec_id,
                shard_id,
                DEFAULT_START_IDEMPOTENCY_WINDOW.as_secs_f64(),
            )
            .await?
            {
                StartIdempotencyReservation::Duplicate {
                    exec_id,
                    workflow_id,
                    state,
                } => Ok((
                    StartedWorkflowExecution {
                        exec_id,
                        workflow_name,
                        workflow_id,
                        state,
                        created: false,
                    },
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                )),
                StartIdempotencyReservation::Reserved => {
                    let (started, deferred_starts, deferred_checks, cancel_metrics) =
                        start_or_load_workflow_execution_collect(
                            conn,
                            params,
                            true,
                            false,
                            metrics,
                            Some(crate::admission_gate::GateMode::CheckCached),
                        )
                        .await?;
                    // The reserve wrote the claim pointing at `new_exec_id`. If the
                    // reuse policy resolved this fresh-key start to an *existing*
                    // run (e.g. `AllowDuplicate` attaching to a prior `workflow_id`
                    // collision), `new_exec_id` was never inserted — repoint the
                    // claim at the real run so a later same-key request
                    // deduplicates cleanly instead of hitting the defensive
                    // reclaim path.
                    if started.exec_id != new_exec_id {
                        repoint_start_idempotency_claim(
                            conn,
                            &workflow_name,
                            idempotency_key,
                            started.exec_id,
                        )
                        .await?;
                    }
                    Ok((started, deferred_starts, deferred_checks, cancel_metrics))
                }
            }
        }))
        .await
    }
}

/// Options for [`WorkflowHandleClient::start_workflow_transactional`] (issue #763).
///
/// All fields are optional. Omitted fields fall back to the target workflow's
/// registered [`crate::info::WorkflowInfo`] defaults, mirroring
/// `POST /workflows/{name}/start`.
#[derive(Debug, Clone, Default)]
pub struct TransactionalStartOptions {
    shard: Option<ShardId>,
    reuse_policy: WorkflowIdReusePolicy,
    conflict_policy: WorkflowIdConflictPolicy,
    idempotency_key: Option<String>,
    queue_name: Option<String>,
    memo: Option<Value>,
    search_attrs: Option<Value>,
    parent_id: Option<Uuid>,
}

impl TransactionalStartOptions {
    /// The default options: default (rendezvous) shard placement,
    /// `AllowDuplicate` / `Unspecified` id-reuse semantics, no idempotency key,
    /// the `"default"` queue, no memo/search attrs/parent.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Pin the started workflow to the shard backing the connection passed to
    /// `start_workflow_transactional` (issue #763, AC4). Required in a sharded
    /// deployment to avoid a cross-shard transaction; omit in a single-shard
    /// deployment (the default shard is the only shard).
    #[must_use]
    pub const fn with_shard(mut self, shard: ShardId) -> Self {
        self.shard = Some(shard);
        self
    }

    /// Set the `WorkflowIdReusePolicy` applied to a duplicate
    /// `(workflow_name, workflow_id)` collision against a **terminal** prior run.
    #[must_use]
    pub const fn with_reuse_policy(mut self, policy: WorkflowIdReusePolicy) -> Self {
        self.reuse_policy = policy;
        self
    }

    /// Set the `WorkflowIdConflictPolicy` applied to a duplicate
    /// `(workflow_name, workflow_id)` collision against an **active** prior run.
    #[must_use]
    pub const fn with_conflict_policy(mut self, policy: WorkflowIdConflictPolicy) -> Self {
        self.conflict_policy = policy;
        self
    }

    /// Deduplicate this start against an earlier one carrying the same key
    /// (issue #808 semantics), scoped to `(workflow_name, idempotency_key)`.
    #[must_use]
    pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }

    /// Override the task-queue the workflow's activities are dispatched on.
    /// Defaults to `"default"`.
    #[must_use]
    pub fn with_queue_name(mut self, queue_name: impl Into<String>) -> Self {
        self.queue_name = Some(queue_name.into());
        self
    }

    /// Attach an operator-visible memo.
    #[must_use]
    pub fn with_memo(mut self, memo: Value) -> Self {
        self.memo = Some(memo);
        self
    }

    /// Attach filterable search attributes.
    #[must_use]
    pub fn with_search_attrs(mut self, search_attrs: Value) -> Self {
        self.search_attrs = Some(search_attrs);
        self
    }

    /// Record a parent execution id (for child-workflow style provenance).
    #[must_use]
    pub const fn with_parent_id(mut self, parent_id: Uuid) -> Self {
        self.parent_id = Some(parent_id);
        self
    }
}

/// Owned, `WorkflowInfo`-derived defaults resolved for a transactional start
/// (issue #763) — see
/// [`WorkflowHandleClient::resolve_transactional_start_defaults`].
struct ResolvedStartDefaults {
    concurrency_key: Option<String>,
    concurrency_limit: Option<u32>,
    execution_timeout: Option<chrono::Duration>,
    sla: Option<chrono::Duration>,
    chain_execution_timeout: Option<chrono::Duration>,
    max_workflow_input_bytes: u64,
    max_workflow_execution_timeout_ceiling: Option<chrono::Duration>,
    max_workflow_chain_timeout_ceiling: Option<chrono::Duration>,
    max_workflow_start_delay: chrono::Duration,
}

/// The three kinds of post-commit follow-up work a transactional start can
/// produce, bundled for [`TransactionalStartOutcome::finish`] (issue #763).
///
/// Only ever non-empty for the `WorkflowIdConflictPolicy::Terminate` case,
/// where resolving the collision cancels a prior execution that itself has
/// registered completion triggers — the overwhelming majority of fresh starts
/// produce none of these.
#[derive(Debug, Default)]
struct PendingStartFollowUps {
    starts: Vec<DeferredTriggerStart>,
    checks: Vec<(ExecutionId, String)>,
    cancel_metrics: Vec<(String, String)>,
}

/// Result of a staged, in-transaction workflow start (issue #763).
///
/// The `WorkflowStarted` event, the execution row, and the initial
/// dispatchable task-queue row exist only inside the caller's own connection's
/// transaction until it commits. If the caller rolls back, none of them exist
/// and no worker ever claims the run — no explicit action is needed here for
/// that guarantee to hold.
///
/// After the caller's transaction has committed, call [`Self::finish`] to
/// eagerly dispatch any deferred completion-trigger follow-ups the start
/// produced. This is an optional latency optimization: a periodic scanner
/// eventually delivers the same follow-ups if `finish` is never called (or the
/// process crashes before it runs), so the started workflow itself runs
/// correctly either way.
#[derive(Debug)]
#[must_use = "call `.finish().await` once your outer transaction has committed to eagerly \
              dispatch any deferred completion-trigger follow-ups; the workflow itself is \
              fully started the moment your transaction commits even if this is dropped"]
pub struct TransactionalStartOutcome {
    /// The execution id assigned to the started (or attached-to) workflow.
    pub exec_id: ExecutionId,
    /// The workflow type name.
    pub workflow_name: String,
    /// The business `workflow_id` this execution was started (or attached) under.
    pub workflow_id: String,
    /// The execution's state immediately after the start decision (e.g. `"RUNNING"`).
    pub state: String,
    /// `true` when a fresh execution was created; `false` when the call
    /// attached to an already-live execution under the configured id-reuse /
    /// conflict policy.
    pub created: bool,
    client: WorkflowHandleClient,
    shard: ShardId,
    deferred: PendingStartFollowUps,
}

impl TransactionalStartOutcome {
    /// Eagerly dispatch deferred completion-trigger follow-ups produced by
    /// this start (issue #763).
    ///
    /// Call this only **after** the caller's outer transaction has committed —
    /// calling it before commit and then rolling back would durably dispatch
    /// cascades for a start that never became durable itself. Safe (and a
    /// documented no-op in the overwhelming majority of calls, since
    /// `deferred` is empty unless a `WorkflowIdConflictPolicy::Terminate`
    /// collision was resolved) to skip entirely.
    pub async fn finish(self) {
        for start in self.deferred.starts {
            start.spawn();
        }
        if !self.deferred.checks.is_empty() {
            // `pool_for` (not `exact_pool_for`) so this works for a single-shard
            // client, where `self.shard` is `ShardId::UNENCODED` (no
            // `.with_shard(...)` call needed) and `exact_pool_for` would return
            // `None`, silently dropping this diagnostic entirely.
            // `ShardedDbPool::single`/`from_map` guarantee a default-shard entry,
            // so `pool_for` never needs its own fallible path here.
            let pool = self.client.inner.pools.pool_for(self.shard);
            match pool.get().await {
                Ok(mut fresh_conn) => {
                    for (exec_id, workflow_name) in self.deferred.checks {
                        let _ = check_and_report_unfinished_handlers(
                            &mut fresh_conn,
                            exec_id,
                            &workflow_name,
                            Some(self.client.inner.metrics.as_ref()
                                as &(dyn crate::telemetry::MetricsRecorder + Send + Sync)),
                        )
                        .await;
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "[transactional_start] failed to acquire a connection to report \
                         unfinished-handler diagnostics; this is a best-effort diagnostic \
                         only and does not affect the started workflow"
                    );
                }
            }
        }
        for (workflow_name, queue_name) in self.deferred.cancel_metrics {
            crate::telemetry::emit_workflow_terminal(
                self.client.inner.metrics.as_ref(),
                &workflow_name,
                &queue_name,
                crate::telemetry::WorkflowStatus::Cancelled,
            );
        }
    }
}

/// Result of starting/loading a workflow together with an awaitable handle.
#[derive(Debug, Clone)]
pub struct StartedWorkflowHandle {
    /// Standard start/load metadata.
    pub started: StartedWorkflowExecution,
    /// Awaitable result handle for `started.exec_id`.
    pub handle: WorkflowHandle,
}

/// Awaitable handle for one workflow execution.
#[derive(Debug, Clone)]
pub struct WorkflowHandle {
    exec_id: ExecutionId,
    client: WorkflowHandleClient,
}

impl WorkflowHandle {
    /// Execution ID this handle awaits.
    #[must_use]
    pub const fn exec_id(&self) -> ExecutionId {
        self.exec_id
    }

    /// Return the handle client associated with this handle.
    #[must_use]
    pub const fn client(&self) -> &WorkflowHandleClient {
        &self.client
    }

    /// Verify that the loaded execution belongs to the expected workflow type.
    ///
    /// # Errors
    ///
    /// Returns a [`HarvestError::Config`] mismatch error if the workflow type
    /// does not match, or database/not-found errors if the execution cannot be resolved.
    pub async fn validate_workflow_type(
        &self,
        conn: &mut AsyncPgConnection,
        expected_name: &str,
    ) -> HarvestResult<()> {
        let execution = harvest_workflow_executions::table
            .find(self.exec_id.as_uuid())
            .select(WorkflowExecution::as_select())
            .first(conn)
            .await
            .optional()
            .map_err(database_error)?
            .ok_or_else(|| {
                HarvestError::NotFound(format!("workflow execution {}", self.exec_id))
            })?;
        if execution.workflow_name != expected_name {
            return Err(HarvestError::Config(format!(
                "workflow type mismatch: execution '{}' has type '{}', but expected '{}'",
                self.exec_id, execution.workflow_name, expected_name
            )));
        }
        Ok(())
    }

    /// Gracefully cancel this workflow execution (cooperative path).
    ///
    /// Mirrors `POST /workflows/{id}/cancel`: appends a `WorkflowCancelled`
    /// event and seals the run as `CANCELLED`. The workflow body is expected
    /// to observe the cancellation via `is_cancelled`/`check_cancellation`.
    /// Idempotent against an already-cancelled run; returns
    /// [`HarvestError::Config`] for a run already terminal for another reason.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::NotFound`] when the execution does not exist,
    /// [`HarvestError::Config`] when the execution is already terminal, and
    /// [`HarvestError::Database`] for persistence failures.
    pub async fn cancel(&self, reason: &str) -> HarvestResult<CancelledWorkflowExecution> {
        let mut conn = self
            .client
            .inner
            .pools
            .pool_for(self.shard())
            .get()
            .await
            .map_err(|error| HarvestError::Database(error.to_string()))?;
        cancel_workflow_execution(
            &mut conn,
            self.exec_id,
            reason,
            &crate::telemetry::NoOpMetrics,
        )
        .await
    }

    /// Forcefully terminate this workflow execution (operator escape hatch).
    ///
    /// Mirrors `POST /workflows/{id}/terminate`: seals a live run in the
    /// `TERMINATED` state unilaterally without requiring the workflow body to
    /// cooperate, fails outstanding task rows, and runs the parent-close
    /// cascade. A result-awaiting caller observes [`HarvestError::Terminated`]
    /// (distinct from a cooperative cancel and from a failure). Idempotent
    /// no-op against any already-terminal state.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::NotFound`] when the execution does not exist and
    /// [`HarvestError::Database`] for persistence failures.
    pub async fn terminate(&self, reason: &str) -> HarvestResult<CancelledWorkflowExecution> {
        let mut conn = self
            .client
            .inner
            .pools
            .pool_for(self.shard())
            .get()
            .await
            .map_err(|error| HarvestError::Database(error.to_string()))?;
        terminate_workflow_execution(
            &mut conn,
            self.exec_id,
            reason,
            &crate::telemetry::NoOpMetrics,
        )
        .await
    }

    /// Return the current compact result snapshot without waiting.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::NotFound`] when the execution row does not exist,
    /// or [`HarvestError::Database`] for connection/query failures.
    pub async fn result_snapshot(&self) -> HarvestResult<WorkflowResult> {
        let execution = self.load_execution().await?;
        Ok(WorkflowResult::from_execution(&execution))
    }

    /// Wait up to `timeout` for a terminal compact snapshot.
    ///
    /// Returns `Ok(None)` when the execution is still running after the wait
    /// window elapses. This is the long-poll shape used by HTTP routes that
    /// need to return `204 No Content` instead of treating an elapsed wait as a
    /// workflow timeout.
    ///
    /// # Errors
    ///
    /// Returns database, listener setup, notification payload, or not-found
    /// errors.
    pub async fn result_snapshot_with_wait(
        &self,
        timeout: Duration,
    ) -> HarvestResult<Option<WorkflowResult>> {
        let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            HarvestError::Config("workflow result wait duration overflowed".to_string())
        })?;
        // Follow the workflow-level retry chain (issue #523) so a still-retrying
        // run is not reported terminal at its first FAILED attempt.
        let snapshot = WorkflowResult::from_execution(&self.load_effective_execution().await?);
        if snapshot.is_terminal() {
            return Ok(Some(snapshot));
        }
        if Instant::now() >= deadline {
            return Ok(None);
        }

        let mut listener = self.connect_listener().await?;

        loop {
            let snapshot = WorkflowResult::from_execution(&self.load_effective_execution().await?);
            if snapshot.is_terminal() {
                return Ok(Some(snapshot));
            }

            let now = Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            let remaining = deadline.saturating_duration_since(now);
            match listener.wait_for_notification_timeout(remaining).await? {
                WorkflowEventWaitOutcome::Notification(_payload) => {}
                WorkflowEventWaitOutcome::TimedOut => {
                    let snapshot =
                        WorkflowResult::from_execution(&self.load_effective_execution().await?);
                    return if snapshot.is_terminal() {
                        Ok(Some(snapshot))
                    } else {
                        Ok(None)
                    };
                }
                WorkflowEventWaitOutcome::ChannelClosed => {
                    listener = self.connect_listener().await?;
                }
            }
        }
    }

    /// Wait until the workflow reaches a terminal state and return its raw JSON
    /// output. Failure terminal states are returned as typed [`HarvestError`]
    /// variants.
    ///
    /// # Errors
    ///
    /// Returns terminal workflow errors, database errors, listener setup errors,
    /// or not-found errors.
    pub async fn result_raw(&self) -> HarvestResult<Value> {
        let execution = self.load_effective_execution().await?;
        if let Some(result) = terminal_raw_result(&execution) {
            return self
                .enrich_terminal_result(ExecutionId::from_uuid(execution.id), result)
                .await;
        }

        let mut listener = self.connect_listener().await?;

        loop {
            let execution = self.load_effective_execution().await?;
            if let Some(result) = terminal_raw_result(&execution) {
                return self
                    .enrich_terminal_result(ExecutionId::from_uuid(execution.id), result)
                    .await;
            }

            match listener.wait_for_notification().await? {
                WorkflowEventWaitOutcome::Notification(_payload) => {}
                WorkflowEventWaitOutcome::TimedOut => {}
                WorkflowEventWaitOutcome::ChannelClosed => {
                    listener = self.connect_listener().await?;
                }
            }
        }
    }

    /// Wait until the workflow reaches a terminal state, returning
    /// [`HarvestError::Timeout`] if the deadline elapses while it is still
    /// running.
    ///
    /// # Errors
    ///
    /// Returns terminal workflow errors, timeout, database errors, listener
    /// setup errors, or not-found errors.
    pub async fn result_raw_with_timeout(&self, timeout: Duration) -> HarvestResult<Value> {
        let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            HarvestError::Config("workflow result timeout duration overflowed".to_string())
        })?;
        let execution = self.load_effective_execution().await?;
        if let Some(result) = terminal_raw_result(&execution) {
            return self
                .enrich_terminal_result(ExecutionId::from_uuid(execution.id), result)
                .await;
        }
        if Instant::now() >= deadline {
            return Err(wait_timeout_error(&execution));
        }

        let mut listener = self.connect_listener().await?;

        loop {
            let execution = self.load_effective_execution().await?;
            if let Some(result) = terminal_raw_result(&execution) {
                return self
                    .enrich_terminal_result(ExecutionId::from_uuid(execution.id), result)
                    .await;
            }

            let now = Instant::now();
            if now >= deadline {
                return Err(wait_timeout_error(&execution));
            }
            let remaining = deadline.saturating_duration_since(now);
            match listener.wait_for_notification_timeout(remaining).await? {
                WorkflowEventWaitOutcome::Notification(_payload) => {}
                WorkflowEventWaitOutcome::TimedOut => {
                    let execution = self.load_effective_execution().await?;
                    if let Some(result) = terminal_raw_result(&execution) {
                        return self
                            .enrich_terminal_result(ExecutionId::from_uuid(execution.id), result)
                            .await;
                    }
                    return Err(wait_timeout_error(&execution));
                }
                WorkflowEventWaitOutcome::ChannelClosed => {
                    listener = self.connect_listener().await?;
                }
            }
        }
    }

    /// Load the typed fields of the terminal `WorkflowFailed` event for
    /// `exec_id` (issue #767).
    ///
    /// Returns `Some(DecodedWorkflowFailure)` when a `WorkflowFailed` event
    /// exists in history (carrying its `error_type`/`details`/`non_retryable`, all
    /// `None` for a legacy untyped failure), or `None` when the execution has no
    /// `WorkflowFailed` event. Callers should invoke this only on the `FAILED`
    /// branch, since it performs an extra history load.
    ///
    /// `exec_id` is an explicit parameter (rather than `self.exec_id`) because
    /// `result_raw` follows the workflow-level retry chain (issue #523) and must
    /// recover the typed failure from the *effective* (final-attempt) execution,
    /// not the original handle id. `result_snapshot` (which reports the original
    /// row, not the chain) passes `self.exec_id()` so its typed fields stay
    /// consistent with the execution row it actually reports.
    ///
    /// # Errors
    ///
    /// Returns database or history-load errors.
    pub(crate) async fn terminal_typed_failure(
        &self,
        exec_id: ExecutionId,
    ) -> HarvestResult<Option<crate::failure::DecodedWorkflowFailure>> {
        let shard = self.client.inner.router.shard_for_execution(exec_id);
        let mut conn = self
            .client
            .inner
            .pools
            .pool_for(shard)
            .get()
            .await
            .map_err(|error| HarvestError::Database(error.to_string()))?;
        let history = crate::store::load_history_with_codecs(
            &mut conn,
            exec_id,
            &self.client.inner.payload_codecs,
        )
        .await?;
        drop(conn);

        Ok(history.events.iter().rev().find_map(|event| match event {
            crate::event::WorkflowEvent::WorkflowFailed {
                error,
                error_type,
                details,
                non_retryable,
            } => Some(crate::failure::DecodedWorkflowFailure {
                message: error.clone(),
                error_type: error_type.clone(),
                details: details.clone(),
                non_retryable: *non_retryable,
            }),
            _ => None,
        }))
    }

    /// Enrich a terminal `result_raw` outcome with the typed failure fields.
    ///
    /// For a `FAILED` execution (`Err(HarvestError::WorkflowFailed { .. })`) this
    /// performs one extra history load to recover the typed
    /// `error_type`/`details`/`non_retryable` (issue #767). The `reason` stays the
    /// human message from `execution.error`. Every other outcome is passed through
    /// untouched (no extra DB round-trip).
    ///
    /// `exec_id` is the id of the execution the failure was reported from — for
    /// `result_raw` this is the *effective* execution after following the
    /// workflow-level retry chain (issue #523), so a retried run whose final
    /// attempt failed with a different typed cause surfaces that final attempt's
    /// typed fields, not the original attempt's.
    async fn enrich_terminal_result(
        &self,
        exec_id: ExecutionId,
        result: HarvestResult<Value>,
    ) -> HarvestResult<Value> {
        let Err(HarvestError::WorkflowFailed {
            name,
            reason,
            error_type,
            details,
            non_retryable,
        }) = result
        else {
            return result;
        };
        // Only reload history when the message-only branch produced an untyped
        // failure (the common terminal path); if typed fields are already present
        // there is nothing to recover.
        if error_type.is_some() || details.is_some() || non_retryable.is_some() {
            return Err(HarvestError::WorkflowFailed {
                name,
                reason,
                error_type,
                details,
                non_retryable,
            });
        }
        match self.terminal_typed_failure(exec_id).await {
            Ok(Some(decoded)) => Err(HarvestError::WorkflowFailed {
                name,
                reason,
                error_type: decoded.error_type,
                details: decoded.details,
                non_retryable: decoded.non_retryable,
            }),
            // No WorkflowFailed event or a history-load error: fall back to the
            // untyped result rather than masking the terminal failure.
            _ => Err(HarvestError::WorkflowFailed {
                name,
                reason,
                error_type: None,
                details: None,
                non_retryable: None,
            }),
        }
    }

    fn shard(&self) -> ShardId {
        self.client.inner.router.shard_for_execution(self.exec_id)
    }

    fn notification_database_url(&self) -> HarvestResult<String> {
        let shard = self.shard();
        self.client
            .inner
            .notification_database_urls
            .get(&shard)
            .cloned()
            .ok_or_else(|| {
                HarvestError::Config(format!(
                    "no workflow notification database URL configured for shard {shard}"
                ))
            })
    }

    async fn connect_listener(&self) -> HarvestResult<WorkflowEventListener> {
        WorkflowEventListener::connect(&self.notification_database_url()?).await
    }

    async fn load_execution(&self) -> HarvestResult<WorkflowExecution> {
        self.load_execution_by_id(self.exec_id).await
    }

    async fn load_execution_by_id(&self, exec_id: ExecutionId) -> HarvestResult<WorkflowExecution> {
        let shard = self.shard();
        let mut conn = self
            .client
            .inner
            .pools
            .pool_for(shard)
            .get()
            .await
            .map_err(|error| HarvestError::Database(error.to_string()))?;

        harvest_workflow_executions::table
            .find(exec_id.as_uuid())
            .select(WorkflowExecution::as_select())
            .first(&mut conn)
            .await
            .optional()
            .map_err(database_error)?
            .ok_or_else(|| HarvestError::NotFound(format!("workflow execution {exec_id}")))
    }

    /// If `execution` is `FAILED` and a workflow-level retry was scheduled for
    /// it (issue #523), return the retry successor's [`ExecutionId`]. The retry
    /// runs on the same shard with a fresh `exec_id`/`workflow_id` and links back
    /// via `retry_of_exec_id`, so a result wait that stopped at the original
    /// `FAILED` row would surface the first transient failure even though the
    /// chain may still complete. Returns `None` for non-failed states or when no
    /// retry exists (the failure is the chain's final outcome).
    async fn retry_successor_id(
        &self,
        failed: &WorkflowExecution,
    ) -> HarvestResult<Option<ExecutionId>> {
        if failed.state != "FAILED" {
            return Ok(None);
        }
        let shard = self.shard();
        let mut conn = self
            .client
            .inner
            .pools
            .pool_for(shard)
            .get()
            .await
            .map_err(|error| HarvestError::Database(error.to_string()))?;
        let next: Option<uuid::Uuid> = harvest_workflow_executions::table
            .filter(harvest_workflow_executions::retry_of_exec_id.eq(Some(failed.id)))
            .select(harvest_workflow_executions::id)
            .first(&mut conn)
            .await
            .optional()
            .map_err(database_error)?;
        Ok(next.map(ExecutionId::from_uuid))
    }

    /// Load the *effective* execution to inspect for a result wait: starting at
    /// this handle's `exec_id`, follow the workflow-level retry chain (issue
    /// #523) while each execution is `FAILED` with a scheduled retry, returning
    /// the deepest (most recent) execution. For workflows without a retry policy
    /// this is exactly `load_execution()` — the original row — so behavior is
    /// unchanged for the non-retry case.
    async fn load_effective_execution(&self) -> HarvestResult<WorkflowExecution> {
        let mut execution = self.load_execution().await?;
        // Bounded by the chain length (max_attempts); a self-referential cycle
        // is impossible because each retry has a strictly fresh exec_id.
        loop {
            match self.retry_successor_id(&execution).await? {
                Some(next) => execution = self.load_execution_by_id(next).await?,
                None => return Ok(execution),
            }
        }
    }

    /// Execute a registered query handler in-process by replaying event history.
    ///
    /// # Errors
    ///
    /// Returns query execution or hydration errors.
    // Already at the clippy line limit before #772 round 6 added the two-line
    // deadline-budget threading below; an allow is cheaper than refactoring an
    // unrelated large function for a 2-line fix.
    #[allow(clippy::too_many_lines)]
    pub async fn execute_query_in_process(
        &self,
        workflow_info: &crate::info::WorkflowInfo,
        query_info: &crate::info::QueryHandlerInfo,
        query_name: &str,
        args: Value,
    ) -> HarvestResult<Value> {
        let execution = self.load_execution().await?;
        if execution.workflow_name != workflow_info.name {
            return Err(HarvestError::Config(format!(
                "workflow type mismatch: execution '{}' has type '{}', but query stub expected '{}'",
                self.exec_id, execution.workflow_name, workflow_info.name
            )));
        }
        if WorkflowResultState::from_execution_state(&execution.state).is_terminal() {
            return Err(HarvestError::WorkflowNotRunning(self.exec_id));
        }

        let shard = self.shard();
        let mut conn = self
            .client
            .inner
            .pools
            .pool_for(shard)
            .get()
            .await
            .map_err(|error| HarvestError::Database(error.to_string()))?;
        let history = crate::store::load_history_with_codecs(
            &mut conn,
            self.exec_id,
            &self.client.inner.payload_codecs,
        )
        .await?;
        drop(conn);

        let ctx = crate::context::WorkflowContext::for_replay_with_state_and_history_policy(
            self.exec_id,
            history.events,
            self.client.inner.shared_state.clone(),
            self.client.inner.history_policy,
        )
        // #772 round 6: thread the deadline budget (see `hydrate_ctx_for_query`).
        .with_execution_timeout(execution.execution_timeout)
        .with_deadline(execution.deadline_at)
        // #698: thread the spawning-parent id (mirrors the deadline budget) so a
        // query handler running against a loaded execution reads the correct
        // `ctx.info().parent_execution_id` for a child workflow.
        .with_parent_execution_id(
            execution
                .parent_id
                .map(crate::types::ExecutionId::from_uuid),
        )
        // #698: thread the workflow type name / business `workflow_id` from the
        // loaded execution row so `ctx.info().workflow_type` / `workflow_id` are the
        // real values (not "") for a query handler running against this run.
        .with_workflow_name(execution.workflow_name.clone())
        .with_workflow_id(execution.workflow_id.clone());
        for q_info in &self.client.inner.query_handlers {
            if q_info.workflow == workflow_info.name {
                ctx.register_declarative_query_handler(q_info);
            }
        }
        for u_info in &self.client.inner.update_handlers {
            if u_info.workflow == workflow_info.name {
                ctx.register_declarative_update_handler(u_info);
            }
        }
        ctx.register_declarative_query_handler(query_info);

        let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let waker_arc = Arc::new(WakerFlag(flag.clone()));
        let waker = futures::task::waker_ref(&waker_arc);
        let mut poll_cx = std::task::Context::from_waker(&waker);

        // Issue #782 (PR #1012 review): contain a panic during future
        // *construction* (a hand-written handler doing synchronous work before
        // returning its boxed future), mirroring the poll-time containment below.
        // Query replays emit no commands and append no events — map the caught
        // construction panic to a clean `QueryHandlerPanicked` (503).
        let handler_fut = match crate::error::catch_construct(|| {
            (workflow_info.handler)(&ctx, execution.input.clone())
        }) {
            Ok(fut) => fut,
            Err(message) => return Err(HarvestError::QueryHandlerPanicked(message)),
        };
        tokio::pin!(handler_fut);

        let mut replay_result = None;
        let deadline = Instant::now() + self.client.query_timeout();
        loop {
            if Instant::now() >= deadline {
                return Err(HarvestError::QueryTimedOut {
                    query_name: query_name.to_string(),
                    timeout_ms: u64::try_from(self.client.query_timeout().as_millis())
                        .unwrap_or(u64::MAX),
                });
            }
            flag.store(false, std::sync::atomic::Ordering::Release);
            // Issue #782: contain a workflow-handler panic during the in-process
            // read-only replay drive, mirroring `executor::drive_query_replay`
            // exactly. Without this, a panicking query handler on a code-changed
            // in-flight run unwinds through the in-process
            // `TypedWorkflowHandle`/`WorkflowHandle` caller. Query replays emit no
            // commands and append no events, so there is nothing to roll back —
            // map the caught panic to a clean `QueryHandlerPanicked` (503).
            let poll = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                handler_fut.as_mut().poll(&mut poll_cx)
            })) {
                Ok(poll) => poll,
                Err(panic_payload) => {
                    return Err(HarvestError::QueryHandlerPanicked(
                        crate::error::panic_message(panic_payload),
                    ));
                }
            };
            match poll {
                std::task::Poll::Ready(res) => {
                    replay_result = Some(res);
                    break;
                }
                std::task::Poll::Pending => {
                    let was_woken = std::sync::atomic::AtomicBool::load(
                        &flag,
                        std::sync::atomic::Ordering::Acquire,
                    );
                    if !was_woken {
                        break;
                    }
                }
            }
        }

        if let Some(Err(reason)) = replay_result {
            return Err(HarvestError::workflow_failed_untyped(
                self.exec_id.to_string(),
                reason,
            ));
        }

        ctx.execute_query_with_args(query_name, args)
    }

    /// Durably admit a workflow update and poll until it completes or fails.
    ///
    /// # Errors
    ///
    /// Returns update execution, admission, or polling timeout errors.
    pub async fn execute_update_in_process(
        &self,
        conn: &mut AsyncPgConnection,
        workflow_name: &str,
        name: &str,
        input: Value,
        timeout: Duration,
    ) -> HarvestResult<Value> {
        self.validate_workflow_type(conn, workflow_name).await?;
        let update_id = crate::types::UpdateId::new();
        // Issue #684: emit harvest.update.admitted post-commit for the in-process
        // typed-client path too. The update's completion is driven by the worker
        // (woken below), which emits update.completed/failed, so admitting
        // without recording admitted would leave this path asymmetric. The
        // recorder defaults to a no-op when the client was built without one.
        crate::store::admit_update_event(
            conn,
            self.exec_id,
            update_id,
            name.to_string(),
            input,
            Some(self.client.inner.metrics.as_ref()),
        )
        .await?;
        crate::queue::wake_workflow_task(conn, self.exec_id).await?;
        let start = Instant::now();
        let poll_interval = Duration::from_millis(100);

        loop {
            let result = {
                let h = crate::store::load_history_with_codecs(
                    conn,
                    self.exec_id,
                    &self.client.inner.payload_codecs,
                )
                .await?;
                match crate::replay::HistoryMatcher::new(h.events).match_update(update_id) {
                    crate::replay::HistoryMatch::Matched { output } => Some(Ok(output)),
                    crate::replay::HistoryMatch::Failed { error, .. } => Some(Err(
                        HarvestError::workflow_failed_untyped(self.exec_id.to_string(), error),
                    )),
                    _ => None,
                }
            };

            if let Some(res) = result {
                return res;
            }

            if start.elapsed() >= timeout {
                return Err(HarvestError::Timeout {
                    timeout_type: TimeoutType::ScheduleToClose,
                    task_name: format!("update {name}"),
                });
            }

            tokio::time::sleep(poll_interval).await;
        }
    }
}

fn terminal_raw_result(execution: &WorkflowExecution) -> Option<HarvestResult<Value>> {
    match execution.state.as_str() {
        "COMPLETED" | "CONTINUED_AS_NEW" => {
            Some(Ok(execution.output.clone().unwrap_or(Value::Null)))
        }
        // Message-only here (no history access); `enrich_terminal_result`
        // augments this with the typed fields from the terminal `WorkflowFailed`
        // event on the `FAILED` branch (issue #767).
        "FAILED" => Some(Err(HarvestError::workflow_failed_untyped(
            execution.workflow_name.clone(),
            execution
                .error
                .clone()
                .unwrap_or_else(|| "workflow failed".to_string()),
        ))),
        "CANCELLED" => Some(Err(HarvestError::Cancelled(
            execution
                .error
                .clone()
                .unwrap_or_else(|| "workflow cancelled".to_string()),
        ))),
        "TIMED_OUT" => Some(Err(HarvestError::Timeout {
            timeout_type: TimeoutType::ScheduleToClose,
            task_name: execution.workflow_name.clone(),
        })),
        "TERMINATED" => Some(Err(HarvestError::Terminated(
            execution
                .error
                .clone()
                .unwrap_or_else(|| "workflow terminated".to_string()),
        ))),
        _ => None,
    }
}

fn wait_timeout_error(execution: &WorkflowExecution) -> HarvestError {
    HarvestError::Timeout {
        timeout_type: TimeoutType::ScheduleToClose,
        task_name: execution.workflow_name.clone(),
    }
}

/// Start or load a workflow and return an awaitable [`WorkflowHandle`].
///
/// This free function mirrors [`start_or_load_workflow_execution`] for callers
/// that prefer functions over [`WorkflowHandleClient::start_or_load`].
///
/// # Errors
///
/// Propagates [`start_or_load_workflow_execution`] failures.
pub async fn start_or_load_workflow_execution_with_handle(
    conn: &mut AsyncPgConnection,
    request: StartWorkflowParams<'_>,
    client: &WorkflowHandleClient,
) -> HarvestResult<StartedWorkflowHandle> {
    client.start_or_load(conn, request).await
}

struct WakerFlag(Arc<std::sync::atomic::AtomicBool>);

impl futures::task::ArcWake for WakerFlag {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.0.store(true, std::sync::atomic::Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_result_state_maps_database_states() {
        assert_eq!(
            WorkflowResultState::from_execution_state("COMPLETED"),
            WorkflowResultState::Completed
        );
        assert_eq!(
            WorkflowResultState::from_execution_state("CONTINUED_AS_NEW"),
            WorkflowResultState::ContinuedAsNew
        );
        assert_eq!(
            WorkflowResultState::from_execution_state("RUNNING"),
            WorkflowResultState::Running
        );
    }

    // ── issue #697: explicit shard pinning ───────────────────────────────────

    #[test]
    fn shard_placement_error_converts_to_a_config_harvest_error() {
        use crate::shard::ShardPlacementError;
        // SDK callers surface placement failures through the ordinary error
        // channel, which the plugin already maps to a 400-class response.
        let err = HarvestError::from(ShardPlacementError::UnknownResidencyKey {
            key: "apac".to_string(),
            known: vec!["eu".to_string()],
        });
        match err {
            HarvestError::Config(msg) => {
                assert!(msg.contains("apac"), "message must name the key: {msg}");
            }
            other => panic!("expected Config, got {other:?}"),
        }
    }

    /// Build a client whose router declares a residency map. The pools are
    /// never dialled (deadpool connects lazily), so this needs no database.
    fn residency_client() -> WorkflowHandleClient {
        use diesel_async::pooled_connection::AsyncDieselConnectionManager;

        let manager = AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(
            "postgres://unused@127.0.0.1:1/unused",
        );
        let pool: DbPool = deadpool::managed::Pool::builder(manager)
            .max_size(1)
            .build()
            .expect("lazy pool");

        let router = ShardRouter::new(
            vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)],
            vec![ShardId::new(0), ShardId::new(1)],
            ShardId::new(0),
        )
        .with_residency_map([
            ("eu".to_string(), ShardId::new(1)),
            ("us".to_string(), ShardId::new(0)),
        ]);

        WorkflowHandleClient::new(
            ShardedDbPool::single(pool),
            router,
            [(ShardId::new(0), "postgres://unused@127.0.0.1:1/unused")],
        )
    }

    /// AC1b/AC5 on the **handle** surface: the SDK's placement resolver reads
    /// the client's declared residency map, not a hash. Shard 1 is deliberately
    /// NOT the default shard, so a "always resolves to default" regression is
    /// caught rather than passing by coincidence.
    #[test]
    fn resolve_shard_placement_reads_the_declared_residency_map() {
        let client = residency_client();
        assert_eq!(
            client
                .resolve_shard_placement(&ShardPlacement::residency_key("eu"), "order_flow", "o-1")
                .expect("declared key must resolve"),
            ShardId::new(1),
            "the `eu` key must resolve through the map to shard 1 (not the default shard 0)"
        );
        assert_eq!(
            client
                .resolve_shard_placement(&ShardPlacement::residency_key("us"), "order_flow", "o-1")
                .expect("declared key must resolve"),
            ShardId::new(0)
        );
    }

    /// AC1 (no placement) on the handle surface: `Auto` is byte-for-byte
    /// today's rendezvous routing.
    #[test]
    fn resolve_shard_placement_auto_matches_rendezvous_routing() {
        let client = residency_client();
        for i in 0..25 {
            let workflow_id = format!("order-{i}");
            assert_eq!(
                client
                    .resolve_shard_placement(&ShardPlacement::Auto, "order_flow", &workflow_id)
                    .expect("auto never fails"),
                client.pick_shard_for_new_workflow("order_flow", &workflow_id),
                "Auto must be exactly the pre-#697 hash for {workflow_id}"
            );
        }
    }

    /// AC2 on the handle surface: an undeclared key and an unknown/drained
    /// shard surface as a typed error the caller can act on — never a silent
    /// fall back to the default shard.
    #[test]
    fn resolve_shard_placement_rejects_undeclared_and_unplaceable_targets() {
        let client = residency_client();

        for placement in [
            ShardPlacement::residency_key("apac"),
            ShardPlacement::residency_key("   "),
            ShardPlacement::Shard(ShardId::new(99)),
            // Readable but drained.
            ShardPlacement::Shard(ShardId::new(2)),
        ] {
            let err = client
                .resolve_shard_placement(&placement, "order_flow", "o-1")
                .expect_err("unplaceable target must be rejected, never defaulted");
            assert!(
                matches!(err, HarvestError::Config(_)),
                "expected Config for {placement:?}, got {err:?}"
            );
        }
    }
}
