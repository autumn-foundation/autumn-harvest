//! Durable routes: an Autumn route that runs its work as a keyed Harvest
//! workflow.
//!
//! The HTTP `Idempotency-Key` header keys the run. A retry with the same key
//! attaches to the same run and gets its stored result, so side effects do
//! not run again. A request that outlives its wait budget gets a pending
//! answer, and the client retries the same request to collect the result.
//!
//! ```rust,ignore
//! #[post("/orders")]
//! async fn place_order(
//!     durable: DurableRoute,
//!     user: CurrentUser,
//!     Json(order): Json<OrderRequest>,
//! ) -> DurableOutcome {
//!     durable
//!         .scope(user.id)
//!         .call("place_order", PlaceOrder { user_id: user.id, order })
//!         .success_status(StatusCode::CREATED)
//!         .send()
//!         .await
//! }
//! ```
//!
//! See `docs/durable-routes.md` for the guide and `DESIGN-durable-routes.md`
//! for the design record.

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::fmt::Display;
use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_harvest::{
    ExecutionId, HarvestError, HarvestResult, TransactionalStartOptions, WorkflowEvent,
    WorkflowHandleClient, WorkflowIdConflictPolicy, WorkflowIdReusePolicy, WorkflowResult,
    WorkflowResultState,
};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::extract::{FromRequestParts, MatchedPath};
use axum::http::request::Parts;
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::Instrument as _;

use crate::state::HarvestDbPool;

/// The request header that carries the client key.
pub const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";

/// The response header on an answer for a run that an earlier request
/// started. Autumn's idempotency layer uses the same header.
pub const REPLAYED_HEADER: &str = "x-idempotent-replayed";

/// The longest client key in bytes.
pub const MAX_KEY_BYTES: usize = 255;

/// The wait budget when the handler sets none.
pub const DEFAULT_WAIT: Duration = Duration::from_secs(5);

/// The longest wait budget. The route clamps a longer budget to it.
pub const MAX_WAIT: Duration = Duration::from_secs(30);

/// The time kept free before Autumn's request deadline.
pub const DEADLINE_HEADROOM: Duration = Duration::from_secs(2);

/// The default cap on requests that wait at the same time in one process.
pub const DEFAULT_MAX_WAITERS: usize = 32;

/// The default cap on the continue-as-new steps that one wait follows.
///
/// Each step is one successor lookup. A step follows a chain to its newest
/// run of the same type, or across one change of workflow type or one retry.
/// A chain needs at most one step more than its changes of type and retries.
pub const DEFAULT_MAX_CHAIN_STEPS: usize = 1024;

/// The prefix of the start-idempotency claim key.
pub const CLAIM_KEY_PREFIX: &str = "durable-route:v1:";

/// The prefix of a durable-route `workflow_id`.
pub const WORKFLOW_ID_PREFIX: &str = "dr1-";

/// Every problem slug that a durable route can answer with.
pub const PROBLEM_SLUGS: &[&str] = &[
    "pending",
    "idempotency-key-missing",
    "idempotency-key-invalid",
    "idempotency-key-reused",
    "input-invalid",
    "input-too-large",
    "quota-exceeded",
    "unavailable",
    "misconfigured",
    "internal",
    "failed",
    "cancelled",
    "terminated",
    "timed-out",
    "erased",
    "result-unavailable",
];

/// The domain tag of the dedupe digest. A new derivation needs a new tag.
const DEDUPE_DOMAIN: &[u8] = b"autumn-harvest/durable-route/v1";

/// The domain tag of the request fingerprint.
const FINGERPRINT_DOMAIN: &[u8] = b"autumn-harvest/durable-route/fp/v1";

/// The `Retry-After` value of a pending answer, in seconds.
const PENDING_RETRY_AFTER_SECS: u64 = 1;

/// The runtime state that the plugin installs at startup.
pub(crate) struct DurableRouteRuntime {
    client: WorkflowHandleClient,
    pool: HarvestDbPool,
    waiters: Arc<Semaphore>,
    /// The most continue-as-new steps that one wait follows.
    max_chain_steps: usize,
    /// The effective input cap of each registered workflow, in bytes.
    input_caps: HashMap<String, u64>,
}

/// Install the durable-route runtime into `state`.
///
/// The plugin calls this after the Harvest runtime starts. Returns `false`
/// when the client or the pool is absent.
pub(crate) fn install(state: &AppState, waiters: Arc<Semaphore>, max_chain_steps: usize) -> bool {
    let (Some(client), Some(pool)) = (
        state.extension::<WorkflowHandleClient>(),
        state.extension::<HarvestDbPool>(),
    ) else {
        return false;
    };
    // The route checks the input size before any I/O, with the cap that the
    // engine applies to a fresh start.
    let input_caps = state
        .extension::<Arc<autumn_harvest::worker::HandlerRegistry>>()
        .map(|registry| {
            registry
                .workflows
                .iter()
                .map(|(name, info)| {
                    (
                        name.clone(),
                        client.max_workflow_input_bytes(info.max_input_bytes),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    state.insert_extension(DurableRouteRuntime {
        client: (*client).clone(),
        pool: (*pool).clone(),
        waiters,
        max_chain_steps,
        input_caps,
    });
    true
}

/// The `Idempotency-Key` fields of a request.
#[derive(Debug, Clone, PartialEq, Eq)]
enum KeyField {
    Absent,
    Many,
    One(Vec<u8>),
}

/// An extractor that turns a route into a durable route.
///
/// It never rejects a request. [`DurableCall::send`] renders every problem,
/// so the handler signature stays simple. Put it before the body extractor.
///
/// The scope is not optional. A call needs [`Self::scope`] or
/// [`Self::unscoped`] first, so this does not compile:
///
/// ```compile_fail,E0599
/// fn handler(durable: autumn_harvest_plugin::durable_route::DurableRoute) {
///     let _ = durable.call("place_order", 1);
/// }
/// ```
pub struct DurableRoute {
    method: Method,
    route: Option<String>,
    key: KeyField,
    layer_active: bool,
    bearer: bool,
    tenant: Option<String>,
    extracted_at: Instant,
    request_timeout: Option<Duration>,
    runtime: Option<Arc<DurableRouteRuntime>>,
}

impl FromRequestParts<AppState> for DurableRoute {
    type Rejection = Infallible;

    fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        std::future::ready(Ok(Self::capture(parts, state)))
    }
}

/// The principal part of the dedupe key.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Scope {
    Unscoped,
    Principal(String),
}

impl DurableRoute {
    /// Read the request facts that a durable call needs.
    fn capture(parts: &Parts, state: &AppState) -> Self {
        let request_timeout = state
            .config_arc()
            .server
            .timeouts
            .request_timeout_ms
            .filter(|ms| *ms > 0)
            .map(Duration::from_millis);
        Self {
            method: parts.method.clone(),
            route: parts
                .extensions
                .get::<MatchedPath>()
                .map(|path| path.as_str().to_owned()),
            key: key_field(&parts.headers),
            // Autumn's idempotency layer adds this context before any
            // extractor runs. Its presence means that the layer caches 2xx.
            layer_active: parts
                .extensions
                .get::<autumn_web::idempotency::IdempotencyContext>()
                .is_some(),
            bearer: parts.headers.contains_key(header::AUTHORIZATION),
            tenant: autumn_web::tenancy::CURRENT_TENANT
                .try_with(Clone::clone)
                .ok()
                .flatten(),
            extracted_at: Instant::now(),
            request_timeout,
            runtime: state.extension::<DurableRouteRuntime>(),
        }
    }

    /// Bind the key to `principal`, for example the user id.
    ///
    /// Two principals that send the same key get two runs. The string must
    /// be unique across every route that runs the workflow. When ids of
    /// different kinds can be equal, add a prefix, such as `user:7`. An
    /// empty principal is a setup error.
    #[must_use]
    pub fn scope(self, principal: impl Display) -> ScopedDurableRoute {
        ScopedDurableRoute {
            route: self,
            scope: Scope::Principal(principal.to_string()),
        }
    }

    /// Do not bind the key to a principal.
    ///
    /// Use this only for a route with no principal. Every caller then shares
    /// one key space, so keys must be random.
    #[must_use]
    pub const fn unscoped(self) -> ScopedDurableRoute {
        ScopedDurableRoute {
            route: self,
            scope: Scope::Unscoped,
        }
    }
}

/// A durable route with its scope set.
pub struct ScopedDurableRoute {
    route: DurableRoute,
    scope: Scope,
}

impl ScopedDurableRoute {
    /// Run the registered workflow `workflow` with `input`.
    ///
    /// The input must be a pure function of the request and the principal.
    /// A retry must build the same input, or it gets a `422`.
    pub fn call(self, workflow: &str, input: impl Serialize) -> DurableCall {
        DurableCall {
            route: self.route,
            scope: self.scope,
            workflow: workflow.to_owned(),
            input: serde_json::to_value(input),
            wait: DEFAULT_WAIT,
            success: StatusCode::OK,
            failures: Vec::new(),
            expose_id: false,
        }
    }
}

/// A configured durable call. [`Self::send`] runs it.
#[must_use]
pub struct DurableCall {
    route: DurableRoute,
    scope: Scope,
    workflow: String,
    input: Result<Value, serde_json::Error>,
    wait: Duration,
    success: StatusCode,
    failures: Vec<(String, StatusCode)>,
    expose_id: bool,
}

impl DurableCall {
    /// Wait up to `budget` for the result. The default is 5 s. The route
    /// clamps a longer budget to 30 s. `Duration::ZERO` never waits.
    ///
    /// The wait also ends 2 s before Autumn's request deadline, counted from
    /// the time the route reads the request. With Autumn's idempotency layer
    /// on, the route counts half the deadline, because that layer reads the
    /// body first. A per-route `timeout_ms` is not visible here, so keep
    /// `budget` below it.
    pub const fn wait(mut self, budget: Duration) -> Self {
        self.wait = budget;
        self
    }

    /// The status of a completed run: 200 (default), 201 or 204.
    pub const fn success_status(mut self, status: StatusCode) -> Self {
        self.success = status;
        self
    }

    /// Answer `status` when the run fails with the typed `error_type`.
    ///
    /// The status must be a 4xx or 5xx status. The retry statuses 408, 409,
    /// 425, 429, 502, 503 and 504 are refused. The last mapping for an error
    /// type wins. The answer carries the `error_type` and the failure
    /// details, never the failure message.
    pub fn failure_status(mut self, error_type: &str, status: StatusCode) -> Self {
        self.failures.push((error_type.to_owned(), status));
        self
    }

    /// Add the `x-harvest-execution-id` header to each answer that reports on
    /// the run, and an `execution_id` member to a pending body.
    ///
    /// The id names the run that the key started. For a continue-as-new
    /// chain, that is the first run. The execution id is the handle of the
    /// management API. Expose it only when that API has its own
    /// authentication.
    pub const fn expose_execution_id(mut self) -> Self {
        self.expose_id = true;
        self
    }

    /// Start or attach to the run, wait, and build the answer.
    ///
    /// This never fails. Every problem is a [`DurableOutcome`].
    pub async fn send(self) -> DurableOutcome {
        let span = tracing::info_span!(
            "harvest.durable_route",
            workflow = %self.workflow,
            route = self.route.route.as_deref().unwrap_or_default(),
            created = tracing::field::Empty,
            exec_id = tracing::field::Empty,
            outcome = tracing::field::Empty,
        );
        let outcome = self.run().instrument(span.clone()).await;
        span.record("outcome", outcome.slug());
        outcome
    }

    /// The setup error of this call, if it has one.
    fn setup_problem(&self) -> Option<&'static str> {
        if matches!(&self.scope, Scope::Principal(p) if p.is_empty()) {
            return Some("the scope is empty");
        }
        if self.input.is_err() {
            return Some("the input does not serialize");
        }
        if !success_status_is_valid(self.success) {
            return Some("success_status must be 200, 201 or 204");
        }
        if self
            .failures
            .iter()
            .any(|(_, status)| !failure_status_is_valid(*status))
        {
            return Some("failure_status refuses this status");
        }
        None
    }

    /// Check the call and build the start. Every check runs before any I/O.
    fn prepare(self) -> Result<(StartRequest, Prepared), Problem> {
        if self.route.layer_active && self.route.bearer {
            warn_layer_ignores_scope();
        }
        let misconfigured = Problem::new(StatusCode::INTERNAL_SERVER_ERROR, "misconfigured");
        if let Some(cause) = self.setup_problem() {
            tracing::error!(workflow = %self.workflow, cause, "durable route setup error");
            if let Err(error) = &self.input {
                tracing::debug!(workflow = %self.workflow, error = %error, "durable route input");
            }
            return Err(misconfigured);
        }
        let key = check_key(&self.route.key)
            .map_err(|slug| Problem::new(StatusCode::BAD_REQUEST, slug))?
            .to_vec();
        let Some(runtime) = self.route.runtime.clone() else {
            return Err(Problem {
                status: StatusCode::SERVICE_UNAVAILABLE,
                slug: "unavailable",
                retry_after: Some(1),
            });
        };
        let input = self.input.map_err(|_| misconfigured)?;
        let digest = dedupe_digest(
            &self.workflow,
            self.route.tenant.as_deref(),
            &self.scope,
            &key,
        );
        let (fp, input_bytes) = canonical_fingerprint(&key, &self.workflow, &input);
        // The canonical form has the length of the compact form that the
        // engine measures. An oversized input stops here, before any I/O.
        if let Some(cap) = runtime.input_caps.get(&self.workflow)
            && *cap > 0
            && input_bytes as u64 > *cap
        {
            return Err(INPUT_TOO_LARGE);
        }
        let start = StartRequest {
            workflow_id: workflow_id(&digest, &fp),
            claim: claim_key(&digest),
            input,
            memo: json!({"durable_route": {
                "method": self.route.method.as_str(),
                "route": self.route.route,
            }}),
            workflow: self.workflow,
        };
        let prepared = Prepared {
            runtime,
            digest,
            fp,
            wait: self.wait,
            extracted_at: self.route.extracted_at,
            request_timeout: self.route.request_timeout,
            layer_active: self.route.layer_active,
            success: self.success,
            failures: self.failures,
            expose_id: self.expose_id,
        };
        Ok((start, prepared))
    }

    async fn run(self) -> DurableOutcome {
        let (start, prepared) = match self.prepare() {
            Ok(parts) => parts,
            Err(problem) => return DurableOutcome::problem(problem),
        };
        let layer_active = prepared.layer_active;
        let io_limit = prepared.request_timeout.map(|timeout| {
            time_left(
                prepared.extracted_at.elapsed(),
                timeout,
                layer_active,
                DEADLINE_HEADROOM / 2,
            )
        });
        let work = prepared.start_and_answer(start);
        // Every read after the start ends before Autumn's request deadline.
        // A dropped handler would keep Autumn's in-flight lock for its TTL.
        match io_limit {
            None => work.await,
            Some(limit) => tokio::time::timeout(limit, work).await.unwrap_or_else(|_| {
                tracing::warn!("durable route reached its deadline margin; answering pending");
                DurableOutcome::pending(layer_active)
            }),
        }
    }
}

/// Warn once that Autumn's idempotency layer does not use the scope.
///
/// The layer keys its cache by the session cookie, not by the scope. Two
/// clients whose principal is not a session, such as two token clients, that
/// send one key and one body share one cached answer. That answer skips the
/// handler.
fn warn_layer_ignores_scope() {
    static WARNED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    if !WARNED.swap(true, std::sync::atomic::Ordering::Relaxed) {
        tracing::warn!(
            "a durable route runs under Autumn's idempotency layer with an Authorization header; \
             that layer caches answers by session, not by the durable-route scope; see \
             docs/durable-routes.md"
        );
    }
}

/// A checked call, ready to start.
struct Prepared {
    runtime: Arc<DurableRouteRuntime>,
    digest: [u8; 32],
    fp: [u8; 32],
    wait: Duration,
    extracted_at: Instant,
    request_timeout: Option<Duration>,
    layer_active: bool,
    success: StatusCode,
    failures: Vec<(String, StatusCode)>,
    expose_id: bool,
}

impl Prepared {
    /// Start or attach to the run, then build the answer.
    async fn start_and_answer(self, start: StartRequest) -> DurableOutcome {
        let workflow = start.workflow.clone();
        // The start runs in its own task. A dropped request cannot cut the
        // start transaction short, and the retry then finds the claim.
        let task = start_run(Arc::clone(&self.runtime), start);
        let started = match tokio::spawn(task.in_current_span()).await {
            Ok(Ok(started)) => started,
            Ok(Err(error)) => {
                log_start_error(&workflow, &error);
                return DurableOutcome::problem(start_error_problem(&error));
            }
            Err(error) => {
                tracing::error!(error = %error, "durable route start task failed");
                return DurableOutcome::problem(Problem::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                ));
            }
        };
        let span = tracing::Span::current();
        span.record("created", started.created);
        span.record("exec_id", tracing::field::display(started.exec_id));
        if !started.created && !workflow_id_matches(&started.workflow_id, &self.digest, &self.fp) {
            return DurableOutcome::problem(KEY_REUSED);
        }
        self.answer(&started).await
    }

    /// Wait for the run and build the answer.
    async fn answer(self, started: &Started) -> DurableOutcome {
        let after_start = |kind, status, output| DurableOutcome {
            kind,
            status,
            execution_id: Some(started.exec_id),
            replayed: !started.created,
            expose_id: self.expose_id,
            output,
            retry_after: None,
        };
        let budget = wait_budget(
            self.wait,
            self.extracted_at.elapsed(),
            self.request_timeout,
            self.layer_active,
        );
        let waited = wait_for_result(
            &*self.runtime,
            &self.runtime.waiters,
            self.runtime.max_chain_steps,
            started.exec_id,
            budget,
        )
        .await;
        match waited {
            Waited::NoResult(failure) => {
                let (kind, status) = no_result_outcome(failure, self.layer_active);
                after_start(kind, status, None)
            }
            Waited::Terminal { exec_id, result } => {
                // The typed failure costs a history read, so read it only
                // when a mapping can use it.
                let typed =
                    if result.state == WorkflowResultState::Failed && !self.failures.is_empty() {
                        let read = typed_failure(&self.runtime, exec_id).await;
                        match typed_or_answer(read, self.layer_active) {
                            Ok(typed) => typed,
                            Err((kind, status)) => return after_start(kind, status, None),
                        }
                    } else {
                        None
                    };
                let (kind, status, output) = terminal_outcome(
                    result.state,
                    result.output,
                    typed,
                    self.success,
                    &self.failures,
                );
                after_start(kind, status, output)
            }
        }
    }
}

/// What a durable call ended with.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum DurableOutcomeKind {
    /// The run completed.
    Completed,
    /// The run is still live. Retry the same request.
    Pending,
    /// The run failed.
    #[non_exhaustive]
    Failed {
        /// The typed failure class, when a mapping matched it.
        error_type: Option<String>,
        /// The typed failure details, when a mapping matched it.
        details: Option<Value>,
    },
    /// The run was cancelled.
    Cancelled,
    /// The run was terminated.
    Terminated,
    /// The run exceeded its execution timeout.
    TimedOut,
    /// PII erasure removed the result.
    Erased,
    /// The call failed before or outside the run.
    #[non_exhaustive]
    Problem {
        /// The problem slug, one of [`PROBLEM_SLUGS`].
        slug: &'static str,
    },
}

/// The answer of a durable call. It implements [`IntoResponse`].
#[must_use]
#[derive(Debug, Clone)]
pub struct DurableOutcome {
    kind: DurableOutcomeKind,
    status: StatusCode,
    execution_id: Option<ExecutionId>,
    replayed: bool,
    expose_id: bool,
    output: Option<Value>,
    retry_after: Option<u64>,
}

impl DurableOutcome {
    /// A pending answer that names no run.
    fn pending(layer_active: bool) -> Self {
        let (kind, status) = pending_outcome(layer_active);
        Self {
            kind,
            status,
            execution_id: None,
            replayed: false,
            expose_id: false,
            output: None,
            retry_after: None,
        }
    }

    const fn problem(problem: Problem) -> Self {
        Self {
            kind: DurableOutcomeKind::Problem { slug: problem.slug },
            status: problem.status,
            execution_id: None,
            replayed: false,
            expose_id: false,
            output: None,
            retry_after: problem.retry_after,
        }
    }

    /// What the call ended with.
    #[must_use]
    pub const fn kind(&self) -> &DurableOutcomeKind {
        &self.kind
    }

    /// The HTTP status of the answer.
    #[must_use]
    pub const fn status(&self) -> StatusCode {
        self.status
    }

    /// The run that the key started, when the call started or found one.
    /// For a continue-as-new chain, this is the first run.
    #[must_use]
    pub const fn execution_id(&self) -> Option<ExecutionId> {
        self.execution_id
    }

    /// `true` when an earlier request started the run.
    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }

    /// The output of a completed run.
    #[must_use]
    pub const fn output(&self) -> Option<&Value> {
        self.output.as_ref()
    }

    /// The output of a completed run as `T`.
    ///
    /// # Errors
    ///
    /// Returns the decode error. A run with no output decodes from `null`.
    pub fn output_as<T: serde::de::DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        T::deserialize(self.output.as_ref().unwrap_or(&Value::Null))
    }

    /// The slug of this outcome, for logs.
    const fn slug(&self) -> &'static str {
        match &self.kind {
            DurableOutcomeKind::Completed => "completed",
            DurableOutcomeKind::Pending => "pending",
            DurableOutcomeKind::Failed { .. } => "failed",
            DurableOutcomeKind::Cancelled => "cancelled",
            DurableOutcomeKind::Terminated => "terminated",
            DurableOutcomeKind::TimedOut => "timed-out",
            DurableOutcomeKind::Erased => "erased",
            DurableOutcomeKind::Problem { slug } => slug,
        }
    }
}

impl IntoResponse for DurableOutcome {
    fn into_response(self) -> Response {
        let exposed_id = self.execution_id.filter(|_| self.expose_id);
        let mut response = match &self.kind {
            DurableOutcomeKind::Completed if self.status == StatusCode::NO_CONTENT => {
                self.status.into_response()
            }
            DurableOutcomeKind::Completed => {
                let body = self.output.as_ref().unwrap_or(&Value::Null).to_string();
                json_response(self.status, "application/json", body)
            }
            DurableOutcomeKind::Pending if self.status == StatusCode::ACCEPTED => {
                let mut body = Map::new();
                body.insert("state".to_owned(), json!("pending"));
                if let Some(id) = exposed_id {
                    body.insert("execution_id".to_owned(), json!(id.to_string()));
                }
                let mut response = json_response(
                    self.status,
                    "application/json",
                    Value::Object(body).to_string(),
                );
                set_retry_after(&mut response, PENDING_RETRY_AFTER_SECS);
                response
            }
            DurableOutcomeKind::Pending => {
                let mut extra = Map::new();
                extra.insert("state".to_owned(), json!("pending"));
                if let Some(id) = exposed_id {
                    extra.insert("execution_id".to_owned(), json!(id.to_string()));
                }
                let mut response = problem_response(self.status, "pending", extra);
                set_retry_after(&mut response, PENDING_RETRY_AFTER_SECS);
                response
            }
            DurableOutcomeKind::Failed {
                error_type,
                details,
            } => {
                let mut extra = Map::new();
                if let Some(error_type) = error_type {
                    extra.insert("error_type".to_owned(), json!(error_type));
                }
                if let Some(details) = details {
                    extra.insert("details".to_owned(), details.clone());
                }
                problem_response(self.status, "failed", extra)
            }
            kind => {
                let mut response = problem_response(self.status, self.slug(), Map::new());
                if let (DurableOutcomeKind::Problem { .. }, Some(secs)) = (kind, self.retry_after) {
                    set_retry_after(&mut response, secs);
                }
                response
            }
        };
        let headers = response.headers_mut();
        if self.execution_id.is_some() && self.replayed {
            headers.insert(
                HeaderName::from_static(REPLAYED_HEADER),
                HeaderValue::from_static("true"),
            );
        }
        if let Some(id) = exposed_id
            && let Ok(value) = HeaderValue::from_str(&id.to_string())
        {
            headers.insert(
                HeaderName::from_static(crate::api::HEADER_EXECUTION_ID),
                value,
            );
        }
        response
    }
}

fn json_response(status: StatusCode, content_type: &'static str, body: String) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, HeaderValue::from_static(content_type))],
        body,
    )
        .into_response()
}

fn set_retry_after(response: &mut Response, secs: u64) {
    response
        .headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from(secs));
}

/// The fixed title and detail of a problem slug.
const fn problem_text(slug: &str) -> (&'static str, &'static str) {
    match slug.as_bytes() {
        b"pending" => (
            "Request still running",
            "The operation is still running. Retry the same request with the same Idempotency-Key.",
        ),
        b"idempotency-key-missing" => (
            "Idempotency-Key required",
            "Send an Idempotency-Key header with a random value.",
        ),
        b"idempotency-key-invalid" => (
            "Invalid Idempotency-Key",
            "Send one Idempotency-Key header of 1 to 255 printable ASCII characters.",
        ),
        b"idempotency-key-reused" => (
            "Idempotency-Key reused",
            "This Idempotency-Key belongs to a request with another body. Use a new key.",
        ),
        b"input-invalid" => (
            "Invalid request",
            "The request does not match the input that this operation accepts.",
        ),
        b"input-too-large" => (
            "Request too large",
            "The request is larger than this operation accepts.",
        ),
        b"quota-exceeded" => (
            "Too many requests",
            "A quota for this operation is full. Retry later.",
        ),
        b"unavailable" => (
            "Service unavailable",
            "The operation cannot start now. Retry later with the same Idempotency-Key.",
        ),
        b"misconfigured" => ("Server error", "The server cannot run this operation."),
        b"failed" => ("Operation failed", "The operation failed."),
        b"cancelled" => ("Operation cancelled", "The operation was cancelled."),
        b"terminated" => ("Operation terminated", "The operation was stopped."),
        b"timed-out" => (
            "Operation timed out",
            "The operation did not finish in its time limit.",
        ),
        b"erased" => ("Result erased", "The result of this operation was erased."),
        b"result-unavailable" => (
            "Result unavailable",
            "The server cannot read the result of this operation.",
        ),
        _ => ("Server error", "The operation failed to start."),
    }
}

/// An RFC 9457 problem answer with fixed text.
fn problem_response(status: StatusCode, slug: &str, extra: Map<String, Value>) -> Response {
    let (title, detail) = problem_text(slug);
    let mut body = Map::new();
    body.insert(
        "type".to_owned(),
        json!(format!("urn:autumn-harvest:durable-route:{slug}")),
    );
    body.insert("title".to_owned(), json!(title));
    body.insert("status".to_owned(), json!(status.as_u16()));
    body.insert("detail".to_owned(), json!(detail));
    body.insert(
        "code".to_owned(),
        json!(format!("harvest.durable_route.{}", slug.replace('-', "_"))),
    );
    body.extend(extra);
    json_response(
        status,
        "application/problem+json",
        Value::Object(body).to_string(),
    )
}

// ── Start and wait ───────────────────────────────────────────────────────────

/// The inputs of one keyed start.
struct StartRequest {
    workflow: String,
    workflow_id: String,
    claim: String,
    input: Value,
    memo: Value,
}

/// The run that a keyed start created or found.
struct Started {
    exec_id: ExecutionId,
    workflow_id: String,
    created: bool,
}

/// Start the run, or find the run that the claim already holds.
///
/// The start drops its pooled connection before `finish`, so no request
/// holds a pool slot while it waits.
async fn start_run(
    runtime: Arc<DurableRouteRuntime>,
    start: StartRequest,
) -> HarvestResult<Started> {
    let shard = runtime.pool.default_shard();
    let outcome = {
        let mut conn =
            autumn_harvest::pool::acquire_within_pool_bound(runtime.pool.pool_for(shard)).await?;
        let options = TransactionalStartOptions::new()
            .with_shard(shard)
            .with_idempotency_key(start.claim)
            // Pinned for a same-input retry after the claim window. It then
            // attaches to the run with the same `workflow_id`, while that run
            // holds the business id.
            .with_reuse_policy(WorkflowIdReusePolicy::AllowDuplicate)
            .with_conflict_policy(WorkflowIdConflictPolicy::UseExisting)
            .with_memo(start.memo);
        Box::pin(runtime.client.start_workflow_transactional(
            &mut conn,
            &start.workflow,
            &start.workflow_id,
            start.input,
            options,
        ))
        .await?
    };
    let started = Started {
        exec_id: outcome.exec_id,
        workflow_id: outcome.workflow_id.clone(),
        created: outcome.created,
    };
    outcome.finish().await;
    Ok(started)
}

/// How a wait ended.
enum Waited {
    /// No result to report: the run is live, or the route cannot read it.
    NoResult(ReadFailure),
    Terminal {
        exec_id: ExecutionId,
        result: WorkflowResult,
    },
}

/// The reads of one wait. [`DurableRouteRuntime`] reads Postgres. A unit
/// test reads a fake chain.
trait RunReads: Sync {
    /// The result of the run, with no wait.
    fn snapshot(
        &self,
        exec_id: ExecutionId,
    ) -> impl Future<Output = HarvestResult<WorkflowResult>> + Send;
    /// The terminal result of the run, or `None` when `budget` ends first.
    fn wait(
        &self,
        exec_id: ExecutionId,
        budget: Duration,
    ) -> impl Future<Output = HarvestResult<Option<WorkflowResult>>> + Send;
    /// The next run of a run that continued as new.
    fn successor(
        &self,
        exec_id: ExecutionId,
    ) -> impl Future<Output = HarvestResult<Option<ExecutionId>>> + Send;
}

impl RunReads for DurableRouteRuntime {
    async fn snapshot(&self, exec_id: ExecutionId) -> HarvestResult<WorkflowResult> {
        self.client.handle(exec_id).result_snapshot().await
    }

    async fn wait(
        &self,
        exec_id: ExecutionId,
        budget: Duration,
    ) -> HarvestResult<Option<WorkflowResult>> {
        let handle = self.client.handle(exec_id);
        wait_at_most(budget, handle.result_snapshot_with_wait(budget)).await
    }

    async fn successor(&self, exec_id: ExecutionId) -> HarvestResult<Option<ExecutionId>> {
        chain_successor(self, exec_id).await
    }
}

/// Run `wait`, cut off at `budget`. A cut-off wait reads as a live run.
///
/// The engine wait can overrun its budget on a slow listener connect, so
/// this outer timeout bounds it.
async fn wait_at_most(
    budget: Duration,
    wait: impl Future<Output = HarvestResult<Option<WorkflowResult>>>,
) -> HarvestResult<Option<WorkflowResult>> {
    tokio::time::timeout(budget, wait).await.unwrap_or(Ok(None))
}

/// Wait up to `budget` for the end of the run and its continue-as-new chain.
///
/// The walk takes at most `max_steps` steps. Each step is one successor
/// lookup. Past the cap, a walk that met a live run answers pending, and a
/// walk that read only ended runs answers result-unavailable. A waiting read
/// takes a permit from `waiters`. A run that is already terminal needs no
/// permit.
async fn wait_for_result(
    reads: &impl RunReads,
    waiters: &Arc<Semaphore>,
    max_steps: usize,
    exec_id: ExecutionId,
    budget: Duration,
) -> Waited {
    let deadline = Instant::now() + budget;
    let mut current = exec_id;
    // The permit lives until the wait ends. One request holds at most one.
    let mut permit: Option<OwnedSemaphorePermit> = None;
    let mut visited = HashSet::new();
    let mut steps = 0;
    let mut saw_live = false;
    loop {
        if !visited.insert(current) {
            tracing::error!(exec_id = %current, "durable route found a cycle in a continue-as-new chain");
            return Waited::NoResult(ReadFailure::Unreadable);
        }
        // A first read with no wait opens no `LISTEN` connection, so a
        // finished run needs no permit.
        let snapshot = match reads.snapshot(current).await {
            Ok(snapshot) => snapshot,
            Err(error) => return Waited::NoResult(read_error(&error)),
        };
        let snapshot = if snapshot.state.is_terminal() {
            snapshot
        } else {
            saw_live = true;
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Waited::NoResult(ReadFailure::Pending);
            }
            if permit.is_none() {
                match Arc::clone(waiters).try_acquire_owned() {
                    Ok(acquired) => permit = Some(acquired),
                    Err(_) => return Waited::NoResult(ReadFailure::Pending),
                }
            }
            match reads.wait(current, remaining).await {
                Ok(None) => return Waited::NoResult(ReadFailure::Pending),
                Ok(Some(snapshot)) => snapshot,
                Err(error) => return Waited::NoResult(read_error(&error)),
            }
        };
        if snapshot.state != WorkflowResultState::ContinuedAsNew {
            return Waited::Terminal {
                exec_id: current,
                result: snapshot,
            };
        }
        if steps == max_steps {
            // A walk that met a live run can need fewer steps once the chain
            // ends, so a retry can succeed.
            if saw_live {
                tracing::warn!(
                    max_steps,
                    "durable route reached its chain step cap on a live chain; answering pending"
                );
                return Waited::NoResult(ReadFailure::Pending);
            }
            // A walk over ended runs only repeats itself, so the answer is final.
            tracing::error!(
                max_steps,
                "durable route reached its chain step cap; the result is unavailable"
            );
            return Waited::NoResult(ReadFailure::Unreadable);
        }
        steps += 1;
        match reads.successor(current).await {
            Ok(Some(next)) => current = next,
            Ok(None) => {
                return Waited::Terminal {
                    exec_id: current,
                    result: snapshot,
                };
            }
            Err(error) => return Waited::NoResult(read_error(&error)),
        }
    }
}

/// Classify a read error and log it.
fn read_error(error: &HarvestError) -> ReadFailure {
    let failure = read_failure(error);
    match failure {
        ReadFailure::Pending => {
            tracing::warn!(error = %error, "durable route cannot read the run; answering pending");
        }
        ReadFailure::Unreadable => {
            tracing::error!(error = %error, "durable route cannot decode the result");
        }
    }
    failure
}

/// The next run of a chain that continued as new.
///
/// A successor of the same type keeps the business id, so one lookup finds
/// the newest run of a long chain. A successor of another type comes from
/// the continue-as-new event. Each step therefore crosses a type change or
/// reaches the end, and a retry never walks the chain one run at a time.
async fn chain_successor(
    runtime: &DurableRouteRuntime,
    exec_id: ExecutionId,
) -> HarvestResult<Option<ExecutionId>> {
    let sharded = runtime.pool.sharded_pool();
    let shard = sharded.routed_shard_for_execution(exec_id);
    let mut held = autumn_harvest::pool::acquire_within_pool_bound(sharded.pool_for(shard)).await?;
    // A workflow-level retry can hold the continue-as-new event, so read the
    // live attempt first.
    let (live, live_shard) =
        autumn_harvest::execution::resolve_live_attempt(&mut held, sharded, shard, exec_id).await?;
    // A rebalance can move the live attempt to another shard. Every later
    // read runs on the shard that holds it.
    let mut resident =
        autumn_harvest::shard_rebalance::bind_to_shard(&mut held, sharded, shard, live_shard)
            .await?;
    let conn = resident.as_mut();
    let live_id = ExecutionId::from_uuid(live.id);
    // An operator rerun takes the business id but starts its own chain, so
    // the lookup keeps to this chain.
    let live_chain = live.first_exec_id.unwrap_or(live.id);
    let newest = newest_chain_run(conn, &live.workflow_name, &live.workflow_id, live_chain).await?;
    match newest {
        // The newest run of the chain stands in for the whole same-type chain.
        Some((run, state)) if run != live_id && state != "CONTINUED_AS_NEW" => Ok(Some(run)),
        // A sealed newest run leads on through its continue-as-new event. A
        // run that a rerun sealed has no such event, so it reports itself.
        Some((run, _)) if run != live_id => Ok(Some(continued_as(conn, run).await?.unwrap_or(run))),
        _ => continued_as(conn, live_id).await,
    }
}

/// The newest run of the chain that starts at `chain`, among the runs with
/// this business id.
async fn newest_chain_run(
    conn: &mut diesel_async::AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    chain: uuid::Uuid,
) -> HarvestResult<Option<(ExecutionId, String)>> {
    use autumn_harvest::schema::harvest_workflow_executions as executions;
    use diesel::{
        BoolExpressionMethods as _, ExpressionMethods as _, NullableExpressionMethods as _,
        OptionalExtension as _, QueryDsl as _,
    };
    use diesel_async::RunQueryDsl as _;
    executions::table
        .filter(executions::workflow_name.eq(workflow_name))
        .filter(executions::workflow_id.eq(workflow_id))
        .filter(
            executions::id
                .eq(chain)
                .nullable()
                .or(executions::first_exec_id.eq(chain)),
        )
        // A reconciled migration seal is a stub, not a run.
        .filter(executions::migrated_run_terminal_at.is_null())
        .order(executions::started_at.desc())
        .select((executions::id, executions::state))
        .first::<(uuid::Uuid, String)>(conn)
        .await
        .optional()
        .map(|row| row.map(|(id, state)| (ExecutionId::from_uuid(id), state)))
        .map_err(|error| HarvestError::Database(error.to_string()))
}

/// The run that `exec_id` continued as, from its continue-as-new event.
async fn continued_as(
    conn: &mut diesel_async::AsyncPgConnection,
    exec_id: ExecutionId,
) -> HarvestResult<Option<ExecutionId>> {
    let history = autumn_harvest::store::load_history_undecoded(conn, exec_id).await?;
    Ok(history.events.into_iter().find_map(|event| match event {
        WorkflowEvent::WorkflowContinuedAsNew { new_exec_id, .. } => Some(new_exec_id),
        _ => None,
    }))
}

/// The typed failure of a failed run, from its last `WorkflowFailed` event.
///
/// A read error is an error, not an untyped failure. The caller answers it as
/// any read error: pending, or `result-unavailable` for a codec error. So a
/// short outage never turns a mapped failure into a final 500.
async fn typed_failure(
    runtime: &DurableRouteRuntime,
    exec_id: ExecutionId,
) -> HarvestResult<Option<TypedFailure>> {
    let sharded = runtime.pool.sharded_pool();
    let shard = sharded.routed_shard_for_execution(exec_id);
    let mut held = autumn_harvest::pool::acquire_within_pool_bound(sharded.pool_for(shard)).await?;
    let (live, live_shard) =
        autumn_harvest::execution::resolve_live_attempt(&mut held, sharded, shard, exec_id).await?;
    // The live attempt can sit on another shard after a rebalance.
    let mut resident =
        autumn_harvest::shard_rebalance::bind_to_shard(&mut held, sharded, shard, live_shard)
            .await?;
    let history = autumn_harvest::store::load_history_with_codecs(
        resident.as_mut(),
        ExecutionId::from_uuid(live.id),
        runtime.client.payload_codecs(),
    )
    .await?;
    Ok(history
        .events
        .into_iter()
        .rev()
        .find_map(|event| match event {
            WorkflowEvent::WorkflowFailed {
                error_type,
                details,
                ..
            } => Some(TypedFailure {
                // `Error` is the untyped sentinel of `WorkflowFailure`.
                error_type: error_type.filter(|class| class != "Error"),
                details,
            }),
            _ => None,
        }))
}

/// Log a start error. The log never holds an input value.
fn log_start_error(workflow: &str, error: &HarvestError) {
    match error {
        HarvestError::InputValidationFailed { violations } => {
            // A violation message can quote an input value, so log only the
            // count and a few field paths.
            let paths: Vec<&str> = violations
                .iter()
                .filter_map(|violation| violation.field_path.as_deref())
                .take(5)
                .collect();
            tracing::debug!(
                workflow,
                count = violations.len(),
                ?paths,
                "durable route input fails the schema"
            );
        }
        // The quota key can come from the input, so the message stays out.
        HarvestError::QuotaExceeded { resource, .. } => {
            tracing::warn!(workflow, %resource, "durable route start hit a quota");
        }
        HarvestError::Config(_) => {
            tracing::error!(workflow, error = %error, "durable route cannot start its workflow");
        }
        _ => tracing::warn!(workflow, error = %error, "durable route start failed"),
    }
}

// ── Pure parts ───────────────────────────────────────────────────────────────

fn key_field(headers: &HeaderMap) -> KeyField {
    let mut values = headers.get_all(IDEMPOTENCY_KEY_HEADER).iter();
    match (values.next(), values.next()) {
        (None, _) => KeyField::Absent,
        (Some(_), Some(_)) => KeyField::Many,
        (Some(value), None) => KeyField::One(value.as_bytes().to_vec()),
    }
}

/// Check the key policy. The error is a problem slug.
fn check_key(field: &KeyField) -> Result<&[u8], &'static str> {
    match field {
        KeyField::Absent => Err("idempotency-key-missing"),
        KeyField::Many => Err("idempotency-key-invalid"),
        KeyField::One(key) if key.is_empty() => Err("idempotency-key-missing"),
        KeyField::One(key)
            if key.len() > MAX_KEY_BYTES || !key.iter().all(|b| (0x20..=0x7e).contains(b)) =>
        {
            Err("idempotency-key-invalid")
        }
        KeyField::One(key) => Ok(key),
    }
}

/// Feed `bytes` into `hasher` with a big-endian `u64` length prefix.
fn put(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

/// Feed an optional value as a tag: `0x00`, or `0x01` and the value.
fn put_tag(hasher: &mut Sha256, value: Option<&[u8]>) {
    match value {
        None => put(hasher, &[0]),
        Some(value) => {
            hasher.update((value.len() as u64 + 1).to_be_bytes());
            hasher.update([1]);
            hasher.update(value);
        }
    }
}

/// The dedupe digest. The route path is not in it, so a renamed route keeps
/// its keys.
fn dedupe_digest(workflow: &str, tenant: Option<&str>, scope: &Scope, key: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    put(&mut hasher, DEDUPE_DOMAIN);
    put(&mut hasher, workflow.as_bytes());
    put_tag(&mut hasher, tenant.map(str::as_bytes));
    let principal = match scope {
        Scope::Unscoped => None,
        Scope::Principal(principal) => Some(principal.as_bytes()),
    };
    put_tag(&mut hasher, principal);
    put(&mut hasher, key);
    hasher.finalize().into()
}

fn claim_key(digest: &[u8; 32]) -> String {
    format!("{CLAIM_KEY_PREFIX}{}", hex(digest))
}

/// Write `value` as JSON with object keys sorted at every depth.
///
/// The result does not depend on the `preserve_order` feature of
/// `serde_json`.
fn canonical_json(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Object(map) => canonical_object(map.iter().collect(), out),
        Value::Array(items) => {
            out.push(b'[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                canonical_json(item, out);
            }
            out.push(b']');
        }
        scalar => out.extend_from_slice(scalar.to_string().as_bytes()),
    }
}

/// Write the object `entries` with their names sorted.
fn canonical_object(mut entries: Vec<(&String, &Value)>, out: &mut Vec<u8>) {
    entries.sort_unstable_by(|a, b| a.0.cmp(b.0));
    out.push(b'{');
    for (i, (name, item)) in entries.into_iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.extend_from_slice(Value::from(name.as_str()).to_string().as_bytes());
        out.push(b':');
        canonical_json(item, out);
    }
    out.push(b'}');
}

/// The request fingerprint. The raw key is in it, so the `workflow_id`
/// reveals nothing about the input to a reader without the key.
#[cfg(test)]
fn fingerprint(key: &[u8], workflow: &str, input: &Value) -> [u8; 32] {
    canonical_fingerprint(key, workflow, input).0
}

/// The request fingerprint and the length of the canonical input in bytes.
fn canonical_fingerprint(key: &[u8], workflow: &str, input: &Value) -> ([u8; 32], usize) {
    let mut canonical = Vec::new();
    canonical_json(input, &mut canonical);
    let mut hasher = Sha256::new();
    put(&mut hasher, FINGERPRINT_DOMAIN);
    put(&mut hasher, key);
    put(&mut hasher, workflow.as_bytes());
    put(&mut hasher, &canonical);
    (hasher.finalize().into(), canonical.len())
}

fn workflow_id(digest: &[u8; 32], fingerprint: &[u8; 32]) -> String {
    format!(
        "{WORKFLOW_ID_PREFIX}{}-{}",
        hex(&digest[..16]),
        hex(&fingerprint[..16])
    )
}

/// `true` when `id` is the `workflow_id` of this key and this input.
fn workflow_id_matches(id: &str, digest: &[u8; 32], fingerprint: &[u8; 32]) -> bool {
    id == workflow_id(digest, fingerprint)
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, b| {
            let _ = write!(out, "{b:02x}");
            out
        })
}

/// The time left before Autumn's request deadline `timeout`, less `margin`.
///
/// The count starts when the route reads the request. Autumn's idempotency
/// layer reads the whole body before that, so with the layer on the route
/// counts only half the deadline.
fn time_left(
    elapsed: Duration,
    timeout: Duration,
    layer_active: bool,
    margin: Duration,
) -> Duration {
    let timeout = if layer_active { timeout / 2 } else { timeout };
    timeout.saturating_sub(margin).saturating_sub(elapsed)
}

/// The wait budget: the requested wait, clamped to [`MAX_WAIT`] and to the
/// request deadline less [`DEADLINE_HEADROOM`].
fn wait_budget(
    requested: Duration,
    elapsed: Duration,
    request_timeout: Option<Duration>,
    layer_active: bool,
) -> Duration {
    let budget = requested.min(MAX_WAIT);
    request_timeout.map_or(budget, |timeout| {
        budget.min(time_left(elapsed, timeout, layer_active, DEADLINE_HEADROOM))
    })
}

const fn success_status_is_valid(status: StatusCode) -> bool {
    matches!(status.as_u16(), 200 | 201 | 204)
}

/// A failure status is a final 4xx or 5xx status. A status that means
/// "retry" would make a client repeat a final failure.
fn failure_status_is_valid(status: StatusCode) -> bool {
    (status.is_client_error() || status.is_server_error())
        && !matches!(status.as_u16(), 408 | 409 | 425 | 429 | 502 | 503 | 504)
}

/// A problem before or outside the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Problem {
    status: StatusCode,
    slug: &'static str,
    retry_after: Option<u64>,
}

impl Problem {
    const fn new(status: StatusCode, slug: &'static str) -> Self {
        Self {
            status,
            slug,
            retry_after: None,
        }
    }
}

/// The answer for a key that a request with another body already used.
const KEY_REUSED: Problem =
    Problem::new(StatusCode::UNPROCESSABLE_ENTITY, "idempotency-key-reused");

/// The answer for an input over the input cap of the workflow.
const INPUT_TOO_LARGE: Problem = Problem::new(StatusCode::PAYLOAD_TOO_LARGE, "input-too-large");

/// The public answer for a start error. It never carries the error text.
fn start_error_problem(error: &HarvestError) -> Problem {
    let unavailable = |secs: u64| Problem {
        status: StatusCode::SERVICE_UNAVAILABLE,
        slug: "unavailable",
        retry_after: Some(secs.max(1)),
    };
    match error {
        HarvestError::InputValidationFailed { .. } => {
            Problem::new(StatusCode::UNPROCESSABLE_ENTITY, "input-invalid")
        }
        HarvestError::PayloadTooLarge { .. } => INPUT_TOO_LARGE,
        HarvestError::QuotaExceeded { .. } => Problem {
            status: StatusCode::TOO_MANY_REQUESTS,
            slug: "quota-exceeded",
            retry_after: Some(1),
        },
        HarvestError::LoadShed {
            retry_after_secs, ..
        } => unavailable(*retry_after_secs),
        HarvestError::AdmissionBlocked { .. }
        | HarvestError::QueueFull { .. }
        | HarvestError::PoolAcquireTimeout { .. }
        | HarvestError::PoolAcquireFailed { .. }
        | HarvestError::ShardUnavailable { .. }
        | HarvestError::ShardFenced { .. }
        | HarvestError::Database(_) => unavailable(1),
        HarvestError::Config(_) => Problem::new(StatusCode::INTERNAL_SERVER_ERROR, "misconfigured"),
        _ => Problem::new(StatusCode::INTERNAL_SERVER_ERROR, "internal"),
    }
}

/// How a read error during the wait ends the call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadFailure {
    Pending,
    Unreadable,
}

/// A codec failure needs an operator, so it is not pending. A retry chain
/// past the engine's walk depth only grows, so that error is final too. Any
/// other read error can pass, and the run exists, so the client retries.
const fn read_failure(error: &HarvestError) -> ReadFailure {
    if error.is_codec_failure() || matches!(error, HarvestError::RetryChainMaxDepthExceeded { .. })
    {
        ReadFailure::Unreadable
    } else {
        ReadFailure::Pending
    }
}

/// The typed failure of a failed run.
#[derive(Debug, Clone, Default, PartialEq)]
struct TypedFailure {
    error_type: Option<String>,
    details: Option<Value>,
}

/// The answer for a run in `state`.
fn terminal_outcome(
    state: WorkflowResultState,
    output: Option<Value>,
    failure: Option<TypedFailure>,
    success: StatusCode,
    failures: &[(String, StatusCode)],
) -> (DurableOutcomeKind, StatusCode, Option<Value>) {
    let error = StatusCode::INTERNAL_SERVER_ERROR;
    match state {
        WorkflowResultState::Running => (DurableOutcomeKind::Pending, StatusCode::ACCEPTED, None),
        WorkflowResultState::Completed => {
            if output
                .as_ref()
                .is_some_and(autumn_harvest::erase::is_erasure_tombstone)
            {
                (DurableOutcomeKind::Erased, StatusCode::GONE, None)
            } else {
                (DurableOutcomeKind::Completed, success, output)
            }
        }
        WorkflowResultState::Failed => {
            let mapped = failure.and_then(|failure| {
                let error_type = failure.error_type?;
                let status = failures
                    .iter()
                    .rev()
                    .find(|(mapped, _)| *mapped == error_type)?
                    .1;
                Some((error_type, failure.details, status))
            });
            match mapped {
                Some((_, Some(details), _))
                    if autumn_harvest::erase::is_erasure_tombstone(&details) =>
                {
                    (DurableOutcomeKind::Erased, StatusCode::GONE, None)
                }
                Some((error_type, details, status)) => (
                    DurableOutcomeKind::Failed {
                        error_type: Some(error_type),
                        details,
                    },
                    status,
                    None,
                ),
                None => (
                    DurableOutcomeKind::Failed {
                        error_type: None,
                        details: None,
                    },
                    error,
                    None,
                ),
            }
        }
        WorkflowResultState::Cancelled => (DurableOutcomeKind::Cancelled, error, None),
        WorkflowResultState::Terminated => (DurableOutcomeKind::Terminated, error, None),
        WorkflowResultState::TimedOut => (DurableOutcomeKind::TimedOut, error, None),
        // A continue-as-new row with no successor holds no final result.
        WorkflowResultState::ContinuedAsNew => (
            DurableOutcomeKind::Problem {
                slug: "result-unavailable",
            },
            error,
            None,
        ),
    }
}

/// The typed failure, or the answer when the route cannot read it.
///
/// The run failed, but its class is unknown. A passing read error answers
/// pending, so a retry reads it again, and a short outage never turns a
/// mapped failure into a final 500. A codec error answers
/// `result-unavailable`.
fn typed_or_answer(
    read: HarvestResult<Option<TypedFailure>>,
    layer_active: bool,
) -> Result<Option<TypedFailure>, (DurableOutcomeKind, StatusCode)> {
    read.map_err(|error| no_result_outcome(read_error(&error), layer_active))
}

/// The answer when the route has no result to report.
///
/// A live run, or a read error that can pass, answers pending. A result that
/// the server cannot decode answers `500` `result-unavailable`.
const fn no_result_outcome(
    failure: ReadFailure,
    layer_active: bool,
) -> (DurableOutcomeKind, StatusCode) {
    match failure {
        ReadFailure::Pending => pending_outcome(layer_active),
        ReadFailure::Unreadable => (
            DurableOutcomeKind::Problem {
                slug: "result-unavailable",
            },
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

/// The pending answer: `202`, or `409` when Autumn's idempotency layer would
/// cache a `202` and replay it in place of the result.
const fn pending_outcome(layer_active: bool) -> (DurableOutcomeKind, StatusCode) {
    let status = if layer_active {
        StatusCode::CONFLICT
    } else {
        StatusCode::ACCEPTED
    };
    (DurableOutcomeKind::Pending, status)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderValue, Request, header};
    use proptest::prelude::*;

    const KEY: &str = "3f2b6c1e-7d4a-4f0e-9b1a-2c5d8e7f6a90";

    fn parts_with(headers: &[(&str, &[u8])]) -> Parts {
        let mut builder = Request::builder().method(Method::POST).uri("/orders");
        for (name, value) in headers {
            builder = builder.header(*name, HeaderValue::from_bytes(value).expect("header value"));
        }
        builder.body(()).expect("request").into_parts().0
    }

    async fn extract(headers: &[(&str, &[u8])]) -> DurableRoute {
        let state = AppState::for_test();
        let mut parts = parts_with(headers);
        match DurableRoute::from_request_parts(&mut parts, &state).await {
            Ok(route) => route,
            Err(never) => match never {},
        }
    }

    fn keyed() -> [(&'static str, &'static [u8]); 1] {
        [(IDEMPOTENCY_KEY_HEADER, KEY.as_bytes())]
    }

    async fn body_json(response: Response) -> Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        serde_json::from_slice(&bytes).expect("json body")
    }

    // AC-01
    #[tokio::test]
    async fn extractor_never_rejects_and_captures_request_facts() {
        let route = extract(&[]).await;
        assert_eq!(route.key, KeyField::Absent);
        assert_eq!(route.method, Method::POST);
        assert!(!route.layer_active);
        assert_eq!(route.tenant, None);
        assert!(route.runtime.is_none());

        let route = extract(&[
            (IDEMPOTENCY_KEY_HEADER, b"a".as_slice()),
            (IDEMPOTENCY_KEY_HEADER, b"b".as_slice()),
        ])
        .await;
        assert_eq!(route.key, KeyField::Many);

        let route = extract(&keyed()).await;
        assert_eq!(route.key, KeyField::One(KEY.as_bytes().to_vec()));

        let route = autumn_web::tenancy::with_tenant("acme".to_owned(), extract(&keyed())).await;
        assert_eq!(route.tenant.as_deref(), Some("acme"));
    }

    // AC-02
    #[tokio::test]
    async fn missing_runtime_returns_503_with_retry_after() {
        let outcome = extract(&keyed())
            .await
            .scope("user-1")
            .call("place_order", serde_json::json!({}))
            .send()
            .await;
        assert_eq!(outcome.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            outcome.kind(),
            &DurableOutcomeKind::Problem {
                slug: "unavailable"
            }
        );
        let response = outcome.into_response();
        assert_eq!(response.headers().get(header::RETRY_AFTER).unwrap(), "1");
    }

    // AC-03
    #[tokio::test]
    async fn misconfigured_call_returns_500_before_any_io() {
        let empty_scope = extract(&keyed())
            .await
            .scope("")
            .call("place_order", 1)
            .send()
            .await;
        let bad_success = extract(&keyed())
            .await
            .scope("u")
            .call("place_order", 1)
            .success_status(StatusCode::ACCEPTED)
            .send()
            .await;
        let bad_failure = extract(&keyed())
            .await
            .scope("u")
            .call("place_order", 1)
            .failure_status("OutOfStock", StatusCode::CONFLICT)
            .send()
            .await;
        let redirect_failure = extract(&keyed())
            .await
            .scope("u")
            .call("place_order", 1)
            .failure_status("OutOfStock", StatusCode::FOUND)
            .send()
            .await;
        // A missing key with a setup error still reports the setup error.
        let no_key = extract(&[])
            .await
            .scope("")
            .call("place_order", 1)
            .send()
            .await;
        // JSON has no map with tuple keys, so this input does not serialize.
        let bad_input = extract(&keyed())
            .await
            .scope("u")
            .call("place_order", HashMap::from([((1_u8, 2_u8), 3_u8)]))
            .send()
            .await;
        for outcome in [
            empty_scope,
            bad_success,
            bad_failure,
            redirect_failure,
            no_key,
            bad_input,
        ] {
            assert_eq!(outcome.status(), StatusCode::INTERNAL_SERVER_ERROR);
            assert_eq!(
                outcome.kind(),
                &DurableOutcomeKind::Problem {
                    slug: "misconfigured"
                }
            );
        }
    }

    // AC-04
    #[tokio::test]
    async fn key_policy_table() {
        let long = vec![b'k'; MAX_KEY_BYTES + 1];
        let longest = vec![b'k'; MAX_KEY_BYTES];
        let cases: Vec<(Vec<&[u8]>, Option<&str>)> = vec![
            (vec![], Some("idempotency-key-missing")),
            (vec![b"".as_slice()], Some("idempotency-key-missing")),
            (
                vec![b"a".as_slice(), b"b".as_slice()],
                Some("idempotency-key-invalid"),
            ),
            (vec![b"a\tb".as_slice()], Some("idempotency-key-invalid")),
            (
                vec!["caf\u{e9}".as_bytes()],
                Some("idempotency-key-invalid"),
            ),
            (vec![b"a\xffb".as_slice()], Some("idempotency-key-invalid")),
            (vec![long.as_slice()], Some("idempotency-key-invalid")),
            (vec![longest.as_slice()], None),
            (vec![b"\"quoted sf-string\"".as_slice()], None),
            (vec![b"conn:looks-reserved".as_slice()], None),
        ];
        for (values, expected) in cases {
            let headers: Vec<(&str, &[u8])> = values
                .iter()
                .map(|v| (IDEMPOTENCY_KEY_HEADER, *v))
                .collect();
            let route = extract(&headers).await;
            let checked = check_key(&route.key).err();
            assert_eq!(checked, expected, "values {values:?}");
            if let Some(slug) = expected {
                let outcome = route.scope("u").call("w", 1).send().await;
                assert_eq!(outcome.status(), StatusCode::BAD_REQUEST);
                assert_eq!(outcome.kind(), &DurableOutcomeKind::Problem { slug });
                let response = outcome.into_response();
                let text = body_json(response).await.to_string();
                for value in &values {
                    if value.len() > 2 {
                        assert!(
                            !text.contains(&*String::from_utf8_lossy(value)),
                            "the key must not be echoed: {text}"
                        );
                    }
                }
            }
        }
    }

    // AC-05
    #[test]
    fn dedupe_key_golden_vector() {
        let digest = dedupe_digest(
            "place_order",
            None,
            &Scope::Principal("user-1".to_owned()),
            KEY.as_bytes(),
        );
        let claim = claim_key(&digest);
        assert_eq!(
            claim,
            "durable-route:v1:4a7b839e9245326e675f24e498ad378b5baadfedf2e4f4ad6b743c08e9cfe151"
        );
        assert_eq!(claim.len(), 81);
        let validated =
            autumn_harvest::start_idempotency::validate_start_idempotency_key(Some(claim.as_str()))
                .expect("Harvest accepts the claim key");
        assert_eq!(validated.as_deref(), Some(claim.as_str()));

        let tenant = dedupe_digest(
            "place_order",
            Some("acme"),
            &Scope::Unscoped,
            KEY.as_bytes(),
        );
        assert_eq!(
            claim_key(&tenant),
            "durable-route:v1:a717297f3d3e891076538fbcae2bddf82f20ed763b10cfa0df265d8248b909f9"
        );
    }

    // AC-06
    #[test]
    fn dedupe_key_components_never_collide() {
        let principal = |p: &str| Scope::Principal(p.to_owned());
        let digests = [
            dedupe_digest("w", None, &principal("ab"), b"c"),
            dedupe_digest("w", None, &principal("a"), b"bc"),
            dedupe_digest("w", None, &Scope::Unscoped, b"c"),
            dedupe_digest("w", None, &principal(""), b"c"),
            dedupe_digest("w", Some(""), &principal("ab"), b"c"),
            dedupe_digest("w", Some("t"), &principal("ab"), b"c"),
            dedupe_digest("w2", None, &principal("ab"), b"c"),
            dedupe_digest("w", None, &principal("ab"), b"C"),
            dedupe_digest("w", None, &principal("ab"), b"\"c\""),
            dedupe_digest("wa", None, &principal("b"), b"c"),
            // A value that moves from the tenant slot to the scope slot.
            dedupe_digest("w", Some("x"), &Scope::Unscoped, b"c"),
            dedupe_digest("w", None, &principal("x"), b"c"),
        ];
        for (i, a) in digests.iter().enumerate() {
            for b in &digests[i + 1..] {
                assert_ne!(a, b);
            }
        }
    }

    // AC-07
    #[test]
    fn fingerprint_is_order_independent_and_keyed() {
        let a: Value =
            serde_json::from_str(r#"{"user":{"name":"Ada","id":7},"sku":"A-1","qty":2}"#).unwrap();
        let b: Value =
            serde_json::from_str(r#"{"qty":2,"sku":"A-1","user":{"id":7,"name":"Ada"}}"#).unwrap();
        let mut canonical = Vec::new();
        canonical_json(&a, &mut canonical);
        assert_eq!(
            canonical,
            br#"{"qty":2,"sku":"A-1","user":{"id":7,"name":"Ada"}}"#.as_slice()
        );
        let fp = fingerprint(KEY.as_bytes(), "place_order", &a);
        assert_eq!(fp, fingerprint(KEY.as_bytes(), "place_order", &b));
        // A map type that keeps insertion order must not change the bytes.
        let (qty, sku) = ("qty".to_owned(), "sku".to_owned());
        let (two, part) = (serde_json::json!(2), serde_json::json!("A-1"));
        let mut forward = Vec::new();
        canonical_object(vec![(&qty, &two), (&sku, &part)], &mut forward);
        let mut reversed = Vec::new();
        canonical_object(vec![(&sku, &part), (&qty, &two)], &mut reversed);
        assert_eq!(forward, reversed);
        assert_eq!(forward, br#"{"qty":2,"sku":"A-1"}"#.as_slice());
        assert_eq!(
            hex(&fp),
            "a6a21a78f14e2be87440d70006fb29109a589e5b5f795d0976e6bd4f4e30ad8d"
        );
        let reordered = serde_json::json!([2, 1]);
        assert_ne!(
            fingerprint(KEY.as_bytes(), "w", &serde_json::json!([1, 2])),
            fingerprint(KEY.as_bytes(), "w", &reordered)
        );
        assert_ne!(fp, fingerprint(b"other-key", "place_order", &a));
        assert_ne!(fp, fingerprint(KEY.as_bytes(), "other_flow", &a));

        let tricky = serde_json::json!({
            "e": true, "d": 0, "c": 1.5, "a": null,
            "b": [3, 1, {"z": 1, "a": "\u{e9}\n\"q\""}],
        });
        let mut out = Vec::new();
        canonical_json(&tricky, &mut out);
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "{\"a\":null,\"b\":[3,1,{\"a\":\"\u{e9}\\n\\\"q\\\"\",\"z\":1}],\"c\":1.5,\"d\":0,\"e\":true}"
        );
    }

    fn json_value() -> impl Strategy<Value = Value> {
        let leaf = prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            any::<i64>().prop_map(Value::from),
            "[a-z\\n\"\\\\\u{e9}]{0,6}".prop_map(Value::String),
        ];
        leaf.prop_recursive(4, 32, 6, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..6).prop_map(Value::Array),
                prop::collection::vec(("[a-z]{0,4}", inner), 0..6)
                    .prop_map(|pairs| Value::Object(pairs.into_iter().collect())),
            ]
        })
    }

    proptest! {
        // AC-07 (property): the canonical form is valid JSON that equals the
        // input. Any entry order gives the same bytes. A parse round trip
        // keeps the fingerprint.
        #[test]
        fn fingerprint_ignores_object_key_order(
            value in json_value(),
            entries in prop::collection::btree_map("[a-z]{0,4}", json_value(), 0..6),
        ) {
            let mut out = Vec::new();
            canonical_json(&value, &mut out);
            let parsed: Value = serde_json::from_slice(&out).expect("canonical JSON parses");
            prop_assert_eq!(&parsed, &value);
            prop_assert_eq!(out.len(), value.to_string().len());

            let mut forward = Vec::new();
            canonical_object(entries.iter().collect(), &mut forward);
            let mut reversed = Vec::new();
            canonical_object(entries.iter().rev().collect(), &mut reversed);
            prop_assert_eq!(&forward, &reversed);

            let round_trip: Value =
                serde_json::from_str(&serde_json::to_string_pretty(&value).unwrap()).unwrap();
            prop_assert_eq!(
                fingerprint(b"k", "w", &value),
                fingerprint(b"k", "w", &round_trip)
            );
        }
    }

    // AC-08
    #[test]
    fn workflow_id_round_trips_and_rejects_foreign_ids() {
        let digest = dedupe_digest(
            "place_order",
            None,
            &Scope::Principal("user-1".to_owned()),
            KEY.as_bytes(),
        );
        let input: Value =
            serde_json::from_str(r#"{"user":{"name":"Ada","id":7},"sku":"A-1","qty":2}"#).unwrap();
        let fp = fingerprint(KEY.as_bytes(), "place_order", &input);
        let id = workflow_id(&digest, &fp);
        assert_eq!(
            id,
            "dr1-4a7b839e9245326e675f24e498ad378b-a6a21a78f14e2be87440d70006fb2910"
        );
        assert_eq!(id.len(), 69);
        assert!(workflow_id_matches(&id, &digest, &fp));
        let other_fp = fingerprint(KEY.as_bytes(), "place_order", &serde_json::json!(1));
        assert!(!workflow_id_matches(&id, &digest, &other_fp));
        for foreign in [
            "",
            "order-42",
            "dr1-",
            "dr2-4a7b839e9245326e675f24e498ad378b-a6a21a78f14e2be87440d70006fb2910",
            "DR1-4A7B839E9245326E675F24E498AD378B-A6A21A78F14E2BE87440D70006FB2910",
        ] {
            assert!(!workflow_id_matches(foreign, &digest, &fp), "{foreign}");
        }
    }

    // AC-09
    #[test]
    fn status_map_covers_every_workflow_result_state() {
        let output = Some(serde_json::json!({"ok": true}));
        for state in [
            WorkflowResultState::Running,
            WorkflowResultState::Completed,
            WorkflowResultState::Failed,
            WorkflowResultState::Cancelled,
            WorkflowResultState::TimedOut,
            WorkflowResultState::Terminated,
            WorkflowResultState::ContinuedAsNew,
        ] {
            let (kind, status, body) =
                terminal_outcome(state, output.clone(), None, StatusCode::OK, &[]);
            let expected = match state {
                WorkflowResultState::Completed => {
                    assert_eq!(body, output);
                    (DurableOutcomeKind::Completed, StatusCode::OK)
                }
                WorkflowResultState::Failed => (
                    DurableOutcomeKind::Failed {
                        error_type: None,
                        details: None,
                    },
                    StatusCode::INTERNAL_SERVER_ERROR,
                ),
                WorkflowResultState::Cancelled => (
                    DurableOutcomeKind::Cancelled,
                    StatusCode::INTERNAL_SERVER_ERROR,
                ),
                WorkflowResultState::Terminated => (
                    DurableOutcomeKind::Terminated,
                    StatusCode::INTERNAL_SERVER_ERROR,
                ),
                WorkflowResultState::TimedOut => (
                    DurableOutcomeKind::TimedOut,
                    StatusCode::INTERNAL_SERVER_ERROR,
                ),
                WorkflowResultState::Running => (DurableOutcomeKind::Pending, StatusCode::ACCEPTED),
                WorkflowResultState::ContinuedAsNew => (
                    DurableOutcomeKind::Problem {
                        slug: "result-unavailable",
                    },
                    StatusCode::INTERNAL_SERVER_ERROR,
                ),
            };
            assert_eq!((kind, status), expected, "{state:?}");
        }
    }

    // AC-09
    #[test]
    fn failure_mapping_uses_the_last_status_and_spots_erasure() {
        let mapped = [("OutOfStock".to_owned(), StatusCode::UNPROCESSABLE_ENTITY)];
        let typed = TypedFailure {
            error_type: Some("OutOfStock".to_owned()),
            details: Some(serde_json::json!({"sku": "A-1"})),
        };
        let (kind, status, _) = terminal_outcome(
            WorkflowResultState::Failed,
            None,
            Some(typed.clone()),
            StatusCode::OK,
            &mapped,
        );
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(
            kind,
            DurableOutcomeKind::Failed {
                error_type: typed.error_type.clone(),
                details: typed.details.clone(),
            }
        );
        // The last mapping for an error type wins.
        let remapped = [
            ("OutOfStock".to_owned(), StatusCode::UNPROCESSABLE_ENTITY),
            ("OutOfStock".to_owned(), StatusCode::GONE),
        ];
        let (_, status, _) = terminal_outcome(
            WorkflowResultState::Failed,
            None,
            Some(typed),
            StatusCode::OK,
            &remapped,
        );
        assert_eq!(status, StatusCode::GONE);
        // An unmapped type is a generic failure with no type and no details.
        let other = TypedFailure {
            error_type: Some("Other".to_owned()),
            details: Some(serde_json::json!(1)),
        };
        let (kind, status, _) = terminal_outcome(
            WorkflowResultState::Failed,
            None,
            Some(other),
            StatusCode::OK,
            &mapped,
        );
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            kind,
            DurableOutcomeKind::Failed {
                error_type: None,
                details: None,
            }
        );
        // An erasure tombstone is gone, as an output and as mapped details.
        let tomb = Some(autumn_harvest::erase::erasure_tombstone());
        let (kind, status, _) = terminal_outcome(
            WorkflowResultState::Completed,
            tomb.clone(),
            None,
            StatusCode::OK,
            &[],
        );
        assert_eq!(
            (kind, status),
            (DurableOutcomeKind::Erased, StatusCode::GONE)
        );
        let erased = TypedFailure {
            error_type: Some("OutOfStock".to_owned()),
            details: tomb,
        };
        let (kind, status, _) = terminal_outcome(
            WorkflowResultState::Failed,
            None,
            Some(erased),
            StatusCode::OK,
            &mapped,
        );
        assert_eq!(
            (kind, status),
            (DurableOutcomeKind::Erased, StatusCode::GONE)
        );

        assert_eq!(
            pending_outcome(false),
            (DurableOutcomeKind::Pending, StatusCode::ACCEPTED)
        );
        assert_eq!(
            pending_outcome(true),
            (DurableOutcomeKind::Pending, StatusCode::CONFLICT)
        );
    }

    /// Assert each start error's problem, and that its body holds no secret.
    async fn assert_start_problems(
        secret: &str,
        cases: Vec<(HarvestError, StatusCode, &'static str, Option<u64>)>,
    ) {
        for (error, status, slug, retry_after) in cases {
            let problem = start_error_problem(&error);
            assert_eq!(
                problem,
                Problem {
                    status,
                    slug,
                    retry_after
                },
                "{error}"
            );
            let response = DurableOutcome::problem(problem).into_response();
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("body");
            let body = String::from_utf8_lossy(&bytes);
            assert!(!body.contains(secret), "{error}: {body}");
        }
    }

    // AC-10
    #[tokio::test]
    async fn start_errors_map_to_public_statuses() {
        let secret = "secret-internal-text";
        let cases = vec![
            (
                HarvestError::InputValidationFailed {
                    violations: vec![autumn_harvest::info::SchemaViolation {
                        message: secret.to_owned(),
                        field_path: Some(format!("/{secret}")),
                    }],
                },
                StatusCode::UNPROCESSABLE_ENTITY,
                "input-invalid",
                None,
            ),
            (
                HarvestError::PayloadTooLarge {
                    kind: autumn_harvest::error::PayloadKind::WorkflowInput,
                    observed_bytes: 10,
                    cap_bytes: 1,
                    workflow_type: secret.to_owned(),
                    activity_name: None,
                },
                StatusCode::PAYLOAD_TOO_LARGE,
                "input-too-large",
                None,
            ),
            (
                HarvestError::QuotaExceeded {
                    workflow_name: secret.to_owned(),
                    key: secret.to_owned(),
                    resource: autumn_harvest::quota::QuotaResource::ActiveExecutions,
                    limit: 1,
                    current: 1,
                },
                StatusCode::TOO_MANY_REQUESTS,
                "quota-exceeded",
                Some(1),
            ),
            (
                HarvestError::Config(secret.to_owned()),
                StatusCode::INTERNAL_SERVER_ERROR,
                "misconfigured",
                None,
            ),
            (
                HarvestError::NotFound(secret.to_owned()),
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                None,
            ),
        ];
        assert_start_problems(secret, cases).await;
    }

    // AC-10
    #[tokio::test]
    async fn retryable_start_errors_answer_503_with_retry_after() {
        let secret = "secret-internal-text";
        let cases = vec![
            (
                HarvestError::LoadShed {
                    queue: secret.to_owned(),
                    oldest_pending_age_secs: 9,
                    retry_after_secs: 7,
                },
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                Some(7),
            ),
            (
                HarvestError::AdmissionBlocked {
                    gate_id: uuid::Uuid::nil(),
                    reason: secret.to_owned(),
                },
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                Some(1),
            ),
            (
                HarvestError::QueueFull {
                    queue: secret.to_owned(),
                    depth: 1,
                },
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                Some(1),
            ),
            (
                HarvestError::LoadShed {
                    queue: secret.to_owned(),
                    oldest_pending_age_secs: 9,
                    retry_after_secs: 0,
                },
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                Some(1),
            ),
            (
                HarvestError::PoolAcquireTimeout {
                    waited: Duration::from_secs(2),
                },
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                Some(1),
            ),
            (
                HarvestError::PoolAcquireFailed {
                    reason: secret.to_owned(),
                },
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                Some(1),
            ),
            (
                HarvestError::ShardUnavailable {
                    shard_id: 1,
                    reason: secret.to_owned(),
                },
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                Some(1),
            ),
            (
                HarvestError::ShardFenced {
                    shard_id: 1,
                    pinned: 2,
                    current: Some(3),
                },
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                Some(1),
            ),
            (
                HarvestError::Database(secret.to_owned()),
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                Some(1),
            ),
        ];
        assert_start_problems(secret, cases).await;
    }

    // AC-11
    #[test]
    fn read_errors_map_to_result_unavailable_or_pending() {
        let unreadable = [
            HarvestError::Codec("bad".to_owned()),
            HarvestError::UnknownPayloadCodec { id: "x".to_owned() },
            HarvestError::UnknownCodecKey {
                key_id: "k".to_owned(),
                codec_id: "c".to_owned(),
            },
            // A retry chain past the walk depth never shrinks.
            HarvestError::RetryChainMaxDepthExceeded {
                exec_id: ExecutionId::new(),
                max_depth: 256,
            },
        ];
        for error in unreadable {
            assert_eq!(read_failure(&error), ReadFailure::Unreadable, "{error}");
        }
        for error in [
            HarvestError::Database("down".to_owned()),
            HarvestError::NotFound("gone".to_owned()),
        ] {
            assert_eq!(read_failure(&error), ReadFailure::Pending, "{error}");
        }
        assert_eq!(
            read_error(&HarvestError::Codec("bad".to_owned())),
            ReadFailure::Unreadable
        );
        assert_eq!(
            read_error(&HarvestError::Database("down".to_owned())),
            ReadFailure::Pending
        );
        // The answer for each class, with Autumn's layer off and on.
        assert_eq!(
            no_result_outcome(ReadFailure::Pending, false),
            (DurableOutcomeKind::Pending, StatusCode::ACCEPTED)
        );
        assert_eq!(
            no_result_outcome(ReadFailure::Pending, true),
            (DurableOutcomeKind::Pending, StatusCode::CONFLICT)
        );
        // The typed-failure read answers the same way. A passing error never
        // turns a mapped failure into a final answer.
        let typed = TypedFailure {
            error_type: Some("OutOfStock".to_owned()),
            details: None,
        };
        assert_eq!(
            typed_or_answer(Ok(Some(typed.clone())), false),
            Ok(Some(typed))
        );
        assert_eq!(
            typed_or_answer(Err(HarvestError::Database("blip".to_owned())), false),
            Err((DurableOutcomeKind::Pending, StatusCode::ACCEPTED))
        );
        assert_eq!(
            typed_or_answer(Err(HarvestError::Database("blip".to_owned())), true),
            Err((DurableOutcomeKind::Pending, StatusCode::CONFLICT))
        );
        assert_eq!(
            typed_or_answer(Err(HarvestError::Codec("bad".to_owned())), false),
            Err((
                DurableOutcomeKind::Problem {
                    slug: "result-unavailable"
                },
                StatusCode::INTERNAL_SERVER_ERROR
            ))
        );
        for layer_active in [false, true] {
            assert_eq!(
                no_result_outcome(ReadFailure::Unreadable, layer_active),
                (
                    DurableOutcomeKind::Problem {
                        slug: "result-unavailable"
                    },
                    StatusCode::INTERNAL_SERVER_ERROR
                )
            );
        }
    }

    // AC-12
    #[tokio::test]
    async fn wait_budget_is_clamped() {
        let s = Duration::from_secs;
        let ms = Duration::from_millis;
        assert_eq!(DEFAULT_WAIT, s(5));
        assert_eq!(MAX_WAIT, s(30));
        assert_eq!(DEADLINE_HEADROOM, s(2));
        let call = extract(&keyed()).await.scope("u").call("place_order", 1);
        assert_eq!(call.wait, DEFAULT_WAIT, "a call waits 5 s by default");
        assert_eq!(wait_budget(DEFAULT_WAIT, Duration::ZERO, None, false), s(5));
        assert_eq!(wait_budget(s(60), Duration::ZERO, None, false), MAX_WAIT);
        assert_eq!(
            wait_budget(Duration::ZERO, Duration::ZERO, Some(s(30)), false),
            Duration::ZERO
        );
        // A 3 s deadline leaves 1 s after the 2 s headroom.
        assert_eq!(wait_budget(s(30), Duration::ZERO, Some(s(3)), false), s(1));
        // Time spent before the wait counts against the deadline.
        assert_eq!(wait_budget(s(30), ms(500), Some(s(3)), false), ms(500));
        // A deadline inside the headroom leaves no wait.
        assert_eq!(
            wait_budget(s(30), Duration::ZERO, Some(s(1)), false),
            Duration::ZERO
        );
        assert_eq!(wait_budget(s(30), s(5), Some(s(3)), false), Duration::ZERO);
        // With Autumn's layer on, the route counts half the deadline.
        assert_eq!(wait_budget(s(30), Duration::ZERO, Some(s(10)), true), s(3));
        assert_eq!(wait_budget(s(30), Duration::ZERO, Some(s(30)), true), s(13));
        // Every read after the start ends 1 s before the counted deadline.
        assert_eq!(
            time_left(Duration::ZERO, s(10), false, DEADLINE_HEADROOM / 2),
            s(9)
        );
        assert_eq!(
            time_left(Duration::ZERO, s(10), true, DEADLINE_HEADROOM / 2),
            s(4)
        );
        assert_eq!(
            time_left(s(9), s(10), false, DEADLINE_HEADROOM),
            Duration::ZERO
        );
    }

    fn outcome(kind: DurableOutcomeKind, status: StatusCode) -> DurableOutcome {
        DurableOutcome {
            kind,
            status,
            execution_id: Some(ExecutionId::new()),
            replayed: false,
            expose_id: false,
            output: None,
            retry_after: None,
        }
    }

    // AC-13
    #[tokio::test]
    async fn problem_bodies_never_carry_internal_text() {
        for slug in PROBLEM_SLUGS {
            let response = outcome(
                DurableOutcomeKind::Problem { slug },
                StatusCode::INTERNAL_SERVER_ERROR,
            )
            .into_response();
            assert_eq!(
                response.headers().get(header::CONTENT_TYPE).unwrap(),
                "application/problem+json"
            );
            let body = body_json(response).await;
            assert_eq!(
                body["type"],
                format!("urn:autumn-harvest:durable-route:{slug}")
            );
            assert_eq!(
                body["code"],
                format!("harvest.durable_route.{}", slug.replace('-', "_"))
            );
            assert_eq!(body["status"], 500);
            assert!(body["title"].as_str().is_some_and(|t| !t.is_empty()));
            assert!(body["detail"].as_str().is_some_and(|d| !d.is_empty()));
        }
        let failed = outcome(
            DurableOutcomeKind::Failed {
                error_type: Some("OutOfStock".to_owned()),
                details: Some(serde_json::json!({"sku": "A-1"})),
            },
            StatusCode::UNPROCESSABLE_ENTITY,
        );
        let body = body_json(failed.into_response()).await;
        assert_eq!(body["type"], "urn:autumn-harvest:durable-route:failed");
        assert_eq!(body["status"], 422);
        assert_eq!(body["error_type"], "OutOfStock");
        assert_eq!(body["details"], serde_json::json!({"sku": "A-1"}));
        for (kind, slug) in [
            (DurableOutcomeKind::Cancelled, "cancelled"),
            (DurableOutcomeKind::Terminated, "terminated"),
            (DurableOutcomeKind::TimedOut, "timed-out"),
            (DurableOutcomeKind::Erased, "erased"),
        ] {
            let response = outcome(kind, StatusCode::INTERNAL_SERVER_ERROR).into_response();
            assert!(response.headers().get(header::RETRY_AFTER).is_none());
            let body = body_json(response).await;
            assert_eq!(
                body["type"],
                format!("urn:autumn-harvest:durable-route:{slug}")
            );
        }
        let pending = outcome(DurableOutcomeKind::Pending, StatusCode::CONFLICT).into_response();
        assert_eq!(pending.headers().get(header::RETRY_AFTER).unwrap(), "1");
        let body = body_json(pending).await;
        assert_eq!(body["type"], "urn:autumn-harvest:durable-route:pending");
        assert_eq!(body["state"], "pending");
        let accepted = outcome(DurableOutcomeKind::Pending, StatusCode::ACCEPTED).into_response();
        assert_eq!(accepted.headers().get(header::RETRY_AFTER).unwrap(), "1");
        assert_eq!(
            body_json(accepted).await,
            serde_json::json!({"state": "pending"})
        );
    }

    // AC-14
    #[tokio::test]
    async fn replayed_and_execution_id_headers() {
        let mut completed = outcome(DurableOutcomeKind::Completed, StatusCode::OK);
        let id = completed.execution_id.unwrap();
        let plain = completed.clone().into_response();
        assert!(plain.headers().get(REPLAYED_HEADER).is_none());
        assert!(
            plain
                .headers()
                .get(crate::api::HEADER_EXECUTION_ID)
                .is_none()
        );

        completed.replayed = true;
        completed.expose_id = true;
        let response = completed.into_response();
        assert_eq!(response.headers().get(REPLAYED_HEADER).unwrap(), "true");
        assert_eq!(
            response
                .headers()
                .get(crate::api::HEADER_EXECUTION_ID)
                .unwrap(),
            id.to_string().as_str()
        );

        // A replay without the opt-in never shows the management handle.
        let mut replay = outcome(DurableOutcomeKind::Completed, StatusCode::OK);
        replay.replayed = true;
        let response = replay.into_response();
        assert_eq!(response.headers().get(REPLAYED_HEADER).unwrap(), "true");
        assert!(
            response
                .headers()
                .get(crate::api::HEADER_EXECUTION_ID)
                .is_none()
        );

        let mut pending = outcome(DurableOutcomeKind::Pending, StatusCode::ACCEPTED);
        pending.execution_id = Some(id);
        pending.expose_id = true;
        let body = body_json(pending.into_response()).await;
        assert_eq!(body["execution_id"], id.to_string());

        // A problem before the start carries neither header.
        let mut before = outcome(
            DurableOutcomeKind::Problem {
                slug: "idempotency-key-missing",
            },
            StatusCode::BAD_REQUEST,
        );
        before.execution_id = None;
        before.expose_id = true;
        let response = before.into_response();
        assert!(response.headers().get(REPLAYED_HEADER).is_none());
        assert!(
            response
                .headers()
                .get(crate::api::HEADER_EXECUTION_ID)
                .is_none()
        );
    }

    // AC-15
    #[tokio::test]
    async fn success_status_shapes_the_completed_response() {
        assert!(success_status_is_valid(StatusCode::OK));
        assert!(success_status_is_valid(StatusCode::CREATED));
        assert!(success_status_is_valid(StatusCode::NO_CONTENT));
        assert!(!success_status_is_valid(StatusCode::ACCEPTED));
        assert!(!success_status_is_valid(StatusCode::MULTI_STATUS));

        for status in [408, 409, 425, 429, 502, 503, 504, 302, 200] {
            assert!(
                !failure_status_is_valid(StatusCode::from_u16(status).unwrap()),
                "{status}"
            );
        }
        for status in [400, 402, 404, 410, 422, 500, 501] {
            assert!(
                failure_status_is_valid(StatusCode::from_u16(status).unwrap()),
                "{status}"
            );
        }

        let mut created = outcome(DurableOutcomeKind::Completed, StatusCode::CREATED);
        created.output = Some(serde_json::json!({"id": 1}));
        let response = created.into_response();
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/json"
        );
        assert_eq!(body_json(response).await, serde_json::json!({"id": 1}));

        let mut empty = outcome(DurableOutcomeKind::Completed, StatusCode::NO_CONTENT);
        empty.output = Some(serde_json::json!({"id": 1}));
        let response = empty.into_response();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(bytes.is_empty());

        let none = outcome(DurableOutcomeKind::Completed, StatusCode::OK);
        assert_eq!(body_json(none.into_response()).await, Value::Null);
    }

    // ── The chain walk, on a fake chain ───────────────────────────────────────

    /// One run of a fake chain.
    struct FakeRun {
        state: WorkflowResultState,
        next: Option<ExecutionId>,
        /// The state that the wait reports, when it differs from `state`.
        wait_state: Option<WorkflowResultState>,
        snapshot_error: Option<fn() -> HarvestError>,
        wait_error: Option<fn() -> HarvestError>,
        successor_error: Option<fn() -> HarvestError>,
    }

    /// Runs in memory. The chain counts its waits and its successor reads.
    #[derive(Default)]
    struct FakeChain {
        runs: HashMap<ExecutionId, FakeRun>,
        waits: std::sync::atomic::AtomicUsize,
        successors: std::sync::atomic::AtomicUsize,
    }

    impl FakeChain {
        fn run(&mut self, state: WorkflowResultState, next: Option<ExecutionId>) -> ExecutionId {
            let exec_id = ExecutionId::new();
            self.runs.insert(
                exec_id,
                FakeRun {
                    state,
                    next,
                    wait_state: None,
                    snapshot_error: None,
                    wait_error: None,
                    successor_error: None,
                },
            );
            exec_id
        }

        fn run_mut(&mut self, exec_id: ExecutionId) -> &mut FakeRun {
            self.runs.get_mut(&exec_id).expect("fake run")
        }

        fn waits(&self) -> usize {
            self.waits.load(std::sync::atomic::Ordering::Relaxed)
        }

        fn successors(&self) -> usize {
            self.successors.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    fn fake_result(state: WorkflowResultState) -> WorkflowResult {
        WorkflowResult {
            state,
            output: Some(json!({"state": format!("{state:?}")})),
            error: None,
            completed_at: None,
        }
    }

    impl RunReads for FakeChain {
        fn snapshot(
            &self,
            exec_id: ExecutionId,
        ) -> impl Future<Output = HarvestResult<WorkflowResult>> + Send {
            let run = &self.runs[&exec_id];
            std::future::ready(
                run.snapshot_error
                    .map_or_else(|| Ok(fake_result(run.state)), |error| Err(error())),
            )
        }

        fn wait(
            &self,
            exec_id: ExecutionId,
            _budget: Duration,
        ) -> impl Future<Output = HarvestResult<Option<WorkflowResult>>> + Send {
            self.waits
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let run = &self.runs[&exec_id];
            let state = run.wait_state.unwrap_or(run.state);
            std::future::ready(run.wait_error.map_or_else(
                || Ok(state.is_terminal().then(|| fake_result(state))),
                |error| Err(error()),
            ))
        }

        fn successor(
            &self,
            exec_id: ExecutionId,
        ) -> impl Future<Output = HarvestResult<Option<ExecutionId>>> + Send {
            self.successors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let run = &self.runs[&exec_id];
            std::future::ready(
                run.successor_error
                    .map_or_else(|| Ok(run.next), |error| Err(error())),
            )
        }
    }

    fn waiters(permits: usize) -> Arc<Semaphore> {
        Arc::new(Semaphore::new(permits))
    }

    /// A chain `a -> b -> c`, where `c` completed.
    fn three_runs() -> (FakeChain, ExecutionId, ExecutionId) {
        let mut chain = FakeChain::default();
        let c = chain.run(WorkflowResultState::Completed, None);
        let b = chain.run(WorkflowResultState::ContinuedAsNew, Some(c));
        let a = chain.run(WorkflowResultState::ContinuedAsNew, Some(b));
        (chain, a, c)
    }

    const ONE_SECOND: Duration = Duration::from_secs(1);

    // AC-58
    #[tokio::test]
    async fn chain_walk_follows_each_step_to_the_final_run() {
        let (chain, a, c) = three_runs();
        let waited =
            wait_for_result(&chain, &waiters(1), DEFAULT_MAX_CHAIN_STEPS, a, ONE_SECOND).await;
        assert!(
            matches!(&waited, Waited::Terminal { exec_id, result }
                if *exec_id == c && result.state == WorkflowResultState::Completed),
            "the walk ends at the completed run"
        );
        assert_eq!(chain.successors(), 2);
        assert_eq!(chain.waits(), 0, "a finished chain needs no wait");

        // A continue-as-new run with no successor is the last run.
        let mut lone = FakeChain::default();
        let x = lone.run(WorkflowResultState::ContinuedAsNew, None);
        let waited =
            wait_for_result(&lone, &waiters(1), DEFAULT_MAX_CHAIN_STEPS, x, ONE_SECOND).await;
        assert!(matches!(&waited, Waited::Terminal { exec_id, result }
            if *exec_id == x && result.state == WorkflowResultState::ContinuedAsNew));
    }

    // AC-59
    #[tokio::test]
    async fn chain_walk_answers_pending_past_the_step_cap() {
        assert_eq!(DEFAULT_MAX_CHAIN_STEPS, 1024);
        // The chain `a -> b -> c` takes two steps. The first read is free.
        let (chain, a, c) = three_runs();
        // Every run had ended, so a retry would repeat the walk: final.
        let capped = wait_for_result(&chain, &waiters(1), 1, a, ONE_SECOND).await;
        assert!(matches!(capped, Waited::NoResult(ReadFailure::Unreadable)));
        assert_eq!(chain.successors(), 1);
        let enough = wait_for_result(&chain, &waiters(1), 2, a, ONE_SECOND).await;
        assert!(matches!(enough, Waited::Terminal { exec_id, .. } if exec_id == c));
        // A walk that met a live run answers pending at the cap. Once the
        // chain ends, a retry can need fewer steps.
        let mut live = FakeChain::default();
        let end = live.run(WorkflowResultState::Completed, None);
        let middle = live.run(WorkflowResultState::ContinuedAsNew, Some(end));
        let start = live.run(WorkflowResultState::Running, Some(middle));
        live.run_mut(start).wait_state = Some(WorkflowResultState::ContinuedAsNew);
        let waited = wait_for_result(&live, &waiters(1), 1, start, ONE_SECOND).await;
        assert!(matches!(waited, Waited::NoResult(ReadFailure::Pending)));
        // A run that did not continue as new needs no step.
        let mut single = FakeChain::default();
        let done = single.run(WorkflowResultState::Completed, None);
        let waited = wait_for_result(&single, &waiters(1), 1, done, ONE_SECOND).await;
        assert!(matches!(waited, Waited::Terminal { .. }));
        assert_eq!(single.successors(), 0);
    }

    // AC-60
    #[tokio::test]
    async fn chain_walk_stops_at_a_cycle() {
        let mut chain = FakeChain::default();
        let a = chain.run(WorkflowResultState::ContinuedAsNew, None);
        let b = chain.run(WorkflowResultState::ContinuedAsNew, Some(a));
        chain.run_mut(a).next = Some(b);
        let waited =
            wait_for_result(&chain, &waiters(1), DEFAULT_MAX_CHAIN_STEPS, a, ONE_SECOND).await;
        assert!(matches!(waited, Waited::NoResult(ReadFailure::Unreadable)));
        assert_eq!(chain.successors(), 2, "the walk stops at the first repeat");
    }

    // AC-61
    #[tokio::test]
    async fn chain_walk_maps_read_errors() {
        let codec: fn() -> HarvestError = || HarvestError::UnknownPayloadCodec {
            id: "gone".to_owned(),
        };
        let database: fn() -> HarvestError = || HarvestError::Database("down".to_owned());
        for (error, expected) in [
            (codec, ReadFailure::Unreadable),
            (database, ReadFailure::Pending),
        ] {
            let (mut chain, a, _) = three_runs();
            chain.run_mut(a).snapshot_error = Some(error);
            let waited =
                wait_for_result(&chain, &waiters(1), DEFAULT_MAX_CHAIN_STEPS, a, ONE_SECOND).await;
            assert!(matches!(waited, Waited::NoResult(found) if found == expected));

            let (mut chain, a, _) = three_runs();
            chain.run_mut(a).successor_error = Some(error);
            let waited =
                wait_for_result(&chain, &waiters(1), DEFAULT_MAX_CHAIN_STEPS, a, ONE_SECOND).await;
            assert!(matches!(waited, Waited::NoResult(found) if found == expected));

            // A run that ends during the wait can fail to decode.
            let mut chain = FakeChain::default();
            let live = chain.run(WorkflowResultState::Running, None);
            chain.run_mut(live).wait_error = Some(error);
            let waited = wait_for_result(
                &chain,
                &waiters(1),
                DEFAULT_MAX_CHAIN_STEPS,
                live,
                ONE_SECOND,
            )
            .await;
            assert!(matches!(waited, Waited::NoResult(found) if found == expected));
            assert_eq!(chain.waits(), 1, "the error comes from the wait");
        }
    }

    // AC-62
    #[tokio::test]
    async fn chain_walk_takes_a_permit_only_to_wait() {
        let mut chain = FakeChain::default();
        let done = chain.run(WorkflowResultState::Completed, None);
        let live = chain.run(WorkflowResultState::Running, None);
        let none = waiters(0);
        // A finished chain needs no permit at any step.
        let (sealed, start, end) = three_runs();
        let walked =
            wait_for_result(&sealed, &none, DEFAULT_MAX_CHAIN_STEPS, start, ONE_SECOND).await;
        assert!(matches!(walked, Waited::Terminal { exec_id, .. } if exec_id == end));
        let finished =
            wait_for_result(&chain, &none, DEFAULT_MAX_CHAIN_STEPS, done, ONE_SECOND).await;
        assert!(
            matches!(finished, Waited::Terminal { .. }),
            "a finished run needs no permit"
        );
        let full = wait_for_result(&chain, &none, DEFAULT_MAX_CHAIN_STEPS, live, ONE_SECOND).await;
        assert!(matches!(full, Waited::NoResult(ReadFailure::Pending)));
        assert_eq!(chain.waits(), 0, "a full cap never waits");

        let one = waiters(1);
        let no_budget =
            wait_for_result(&chain, &one, DEFAULT_MAX_CHAIN_STEPS, live, Duration::ZERO).await;
        assert!(matches!(no_budget, Waited::NoResult(ReadFailure::Pending)));
        assert_eq!(chain.waits(), 0, "a zero budget never waits");

        let waited = wait_for_result(&chain, &one, DEFAULT_MAX_CHAIN_STEPS, live, ONE_SECOND).await;
        assert!(matches!(waited, Waited::NoResult(ReadFailure::Pending)));
        assert_eq!(chain.waits(), 1);
        assert_eq!(one.available_permits(), 1, "the wait returns its permit");
    }

    // AC-80
    #[tokio::test(start_paused = true)]
    async fn the_engine_wait_is_cut_off_at_its_budget() {
        // A listener connect that never ends reads as a live run at the budget.
        let stalled = std::future::pending::<HarvestResult<Option<WorkflowResult>>>();
        let cut = tokio::time::timeout(Duration::from_secs(60), wait_at_most(ONE_SECOND, stalled))
            .await
            .expect("the wait ends at its budget");
        assert!(matches!(cut, Ok(None)));
        // A wait that ends in time passes its result through.
        let done = std::future::ready(Ok(Some(fake_result(WorkflowResultState::Completed))));
        let passed = wait_at_most(ONE_SECOND, done).await;
        assert!(
            matches!(passed, Ok(Some(result)) if result.state == WorkflowResultState::Completed)
        );
    }

    // ── Logs ──────────────────────────────────────────────────────────────────

    /// A log sink in memory.
    #[derive(Clone, Default)]
    struct Captured(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for Captured {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl Captured {
        fn text(&self) -> String {
            let bytes = self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            String::from_utf8_lossy(&bytes).into_owned()
        }
    }

    /// A subscriber that writes every event, and each span when it closes.
    fn capture() -> (Captured, tracing::Dispatch) {
        // With one live dispatcher, tracing-core lets another test thread
        // cache a callsite as never. A second dispatcher for the whole
        // process turns that path off.
        static KEEP: std::sync::OnceLock<tracing::Dispatch> = std::sync::OnceLock::new();
        KEEP.get_or_init(|| tracing::Dispatch::new(tracing::subscriber::NoSubscriber::new()));
        let captured = Captured::default();
        let writer = captured.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::TRACE)
            .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
            .finish();
        (captured, tracing::Dispatch::new(subscriber))
    }

    // AC-63
    #[test]
    fn start_error_logs_hold_no_input_values() {
        let violations = (0..7)
            .map(|i| autumn_harvest::info::SchemaViolation {
                message: format!("secret-message-{i}"),
                field_path: Some(format!("/field{i}")),
            })
            .collect();
        let (captured, dispatch) = capture();
        tracing::dispatcher::with_default(&dispatch, || {
            log_start_error(
                "place_order",
                &HarvestError::InputValidationFailed { violations },
            );
            log_start_error(
                "place_order",
                &HarvestError::QuotaExceeded {
                    workflow_name: "place_order".to_owned(),
                    key: "secret-quota-key".to_owned(),
                    resource: autumn_harvest::quota::QuotaResource::ActiveExecutions,
                    limit: 1,
                    current: 1,
                },
            );
        });
        let text = captured.text();
        assert!(text.contains("count=7"), "{text}");
        for i in 0..5 {
            assert!(text.contains(&format!("/field{i}")), "{text}");
        }
        assert!(
            !text.contains("/field5") && !text.contains("/field6"),
            "{text}"
        );
        assert!(text.contains("durable route start hit a quota"), "{text}");
        for secret in ["secret-message", "secret-quota-key"] {
            assert!(!text.contains(secret), "{secret} in {text}");
        }
    }

    // AC-64
    #[tokio::test]
    async fn send_logs_hold_no_key_scope_or_input() {
        let (captured, dispatch) = capture();
        let guard = tracing::dispatcher::set_default(&dispatch);
        let key = [(IDEMPOTENCY_KEY_HEADER, b"secret-key-64".as_slice())];
        let unavailable = extract(&key)
            .await
            .scope("secret-scope-64")
            .call("place_order", json!({"note": "secret-input-64"}))
            .send()
            .await;
        assert_eq!(unavailable.status(), StatusCode::SERVICE_UNAVAILABLE);
        let misconfigured = extract(&key)
            .await
            .scope("secret-scope-64")
            .call(
                "place_order",
                HashMap::from([((1_u8, 2_u8), "secret-input-64")]),
            )
            .send()
            .await;
        assert_eq!(misconfigured.status(), StatusCode::INTERNAL_SERVER_ERROR);
        drop(guard);
        let text = captured.text();
        assert!(text.contains("harvest.durable_route{"), "{text}");
        assert!(text.contains("workflow=place_order"), "{text}");
        assert!(text.contains("outcome=\"unavailable\""), "{text}");
        assert!(text.contains("outcome=\"misconfigured\""), "{text}");
        assert!(text.contains("durable route setup error"), "{text}");
        assert!(text.contains("the input does not serialize"), "{text}");
        for secret in ["secret-key-64", "secret-scope-64", "secret-input-64"] {
            assert!(!text.contains(secret), "{secret} in {text}");
        }
    }

    // AC-65
    #[tokio::test]
    async fn layer_with_authorization_warns_once() {
        let (captured, dispatch) = capture();
        let guard = tracing::dispatcher::set_default(&dispatch);
        for _ in 0..2 {
            let mut route = extract(&[
                (IDEMPOTENCY_KEY_HEADER, KEY.as_bytes()),
                ("authorization", b"Bearer token".as_slice()),
            ])
            .await;
            assert!(route.bearer);
            // Only autumn-web can build the context that its layer adds.
            route.layer_active = true;
            let outcome = route.scope("u").call("place_order", 1).send().await;
            assert_eq!(outcome.status(), StatusCode::SERVICE_UNAVAILABLE);
        }
        drop(guard);
        let text = captured.text();
        assert_eq!(
            text.matches("caches answers by session").count(),
            1,
            "{text}"
        );
    }

    // ── The public outcome ────────────────────────────────────────────────────

    // AC-66
    #[test]
    fn outcome_accessors_expose_the_result() {
        #[derive(serde::Deserialize, Debug, PartialEq)]
        struct Order {
            id: u32,
        }
        let mut completed = outcome(DurableOutcomeKind::Completed, StatusCode::OK);
        completed.output = Some(json!({"id": 7}));
        completed.replayed = true;
        assert_eq!(completed.kind(), &DurableOutcomeKind::Completed);
        assert_eq!(completed.status(), StatusCode::OK);
        assert!(completed.execution_id().is_some());
        assert!(completed.replayed());
        assert_eq!(completed.output(), Some(&json!({"id": 7})));
        assert_eq!(
            completed.output_as::<Order>().expect("order"),
            Order { id: 7 }
        );

        let pending = outcome(DurableOutcomeKind::Pending, StatusCode::ACCEPTED);
        assert_eq!(pending.output(), None);
        assert!(!pending.replayed());
        assert_eq!(pending.output_as::<Option<Order>>().expect("null"), None);
        assert!(pending.output_as::<Order>().is_err());

        let problem = DurableOutcome::problem(KEY_REUSED);
        assert_eq!(problem.execution_id(), None);
        assert_eq!(problem.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    /// The status that the code gives each problem slug.
    async fn code_statuses() -> Vec<(&'static str, u16)> {
        let missing = extract(&[]).await.scope("u").call("w", 1).send().await;
        let invalid = extract(&[
            (IDEMPOTENCY_KEY_HEADER, b"a".as_slice()),
            (IDEMPOTENCY_KEY_HEADER, b"b".as_slice()),
        ])
        .await
        .scope("u")
        .call("w", 1)
        .send()
        .await;
        let misconfigured = extract(&keyed()).await.scope("").call("w", 1).send().await;
        let unavailable = extract(&keyed()).await.scope("u").call("w", 1).send().await;
        let start = |error: HarvestError| start_error_problem(&error).status.as_u16();
        let terminal = |state: WorkflowResultState, output: Option<Value>| {
            terminal_outcome(state, output, None, StatusCode::OK, &[])
                .1
                .as_u16()
        };
        vec![
            ("pending", pending_outcome(true).1.as_u16()),
            ("idempotency-key-missing", missing.status().as_u16()),
            ("idempotency-key-invalid", invalid.status().as_u16()),
            ("idempotency-key-reused", KEY_REUSED.status.as_u16()),
            (
                "input-invalid",
                start(HarvestError::InputValidationFailed { violations: vec![] }),
            ),
            ("input-too-large", INPUT_TOO_LARGE.status.as_u16()),
            (
                "quota-exceeded",
                start(HarvestError::QuotaExceeded {
                    workflow_name: "w".to_owned(),
                    key: "k".to_owned(),
                    resource: autumn_harvest::quota::QuotaResource::ActiveExecutions,
                    limit: 1,
                    current: 1,
                }),
            ),
            ("unavailable", unavailable.status().as_u16()),
            ("misconfigured", misconfigured.status().as_u16()),
            ("internal", start(HarvestError::NotFound("x".to_owned()))),
            ("failed", terminal(WorkflowResultState::Failed, None)),
            ("cancelled", terminal(WorkflowResultState::Cancelled, None)),
            (
                "terminated",
                terminal(WorkflowResultState::Terminated, None),
            ),
            ("timed-out", terminal(WorkflowResultState::TimedOut, None)),
            (
                "erased",
                terminal(
                    WorkflowResultState::Completed,
                    Some(autumn_harvest::erase::erasure_tombstone()),
                ),
            ),
            (
                "result-unavailable",
                no_result_outcome(ReadFailure::Unreadable, false).1.as_u16(),
            ),
        ]
    }

    // AC-67
    #[tokio::test]
    async fn status_table_statuses_match_the_code() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../docs/durable-routes.md");
        let page = std::fs::read_to_string(path).expect("docs page");
        let start = page.find("## Status table").expect("status table");
        let table = &page[start..];
        let table = &table[..table[3..].find("\n## ").map_or(table.len(), |i| i + 3)];
        let mut documented: HashMap<String, Vec<u16>> = HashMap::new();
        for line in table.lines().filter(|line| line.starts_with('|')) {
            let cells: Vec<&str> = line.split('|').collect();
            let (Some(status), Some(slug)) = (cells.get(2), cells.get(3)) else {
                continue;
            };
            // A row with no problem starts its cell with an em dash.
            if slug.trim_start().starts_with('\u{2014}') {
                continue;
            }
            let Some(slug) = slug.split('`').nth(1) else {
                continue;
            };
            let digits: String = status
                .chars()
                .skip_while(|c| !c.is_ascii_digit())
                .take_while(char::is_ascii_digit)
                .collect();
            let entry = documented.entry(slug.to_owned()).or_default();
            if let Ok(number) = digits.parse() {
                entry.push(number);
            }
        }
        let code = code_statuses().await;
        assert_eq!(code.len(), PROBLEM_SLUGS.len());
        for (slug, status) in code {
            assert!(PROBLEM_SLUGS.contains(&slug), "{slug}");
            let rows = documented
                .get(slug)
                .unwrap_or_else(|| panic!("no row for {slug}"));
            assert!(!rows.is_empty(), "{slug}: no status in the table");
            assert!(
                rows.iter().all(|row| *row == status),
                "{slug}: the table says {rows:?}, the code says {status}"
            );
        }
        assert_eq!(pending_outcome(false).1, StatusCode::ACCEPTED);
        assert!(page.contains("| 202, `Retry-After: 1` |"));
    }
}
