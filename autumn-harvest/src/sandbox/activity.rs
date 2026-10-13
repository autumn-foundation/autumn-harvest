//! Sandbox runs as activities.
//!
//! [`SandboxActivity`] names a runtime as an activity.
//! `HarvestBuilder::sandbox_activity` registers it: the builder adds a normal
//! [`ActivityInfo`] and puts the runtime in the [`SandboxActivities`] state.
//! The shared handler finds the runtime by the activity name.
//!
//! The registration sets the bounds that keep a run durable:
//!
//! - `start_to_close` is the wall-clock ceiling plus the backend overhead,
//!   so the activity never times out while its sandbox still runs. A retry
//!   therefore never overlaps a live run.
//! - `max_result_bytes` is the outcome bound of the policy, so a large
//!   outcome never fails the result cap.
//! - `max_input_bytes` is the request bound of the policy.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::context::ActivityContext;
use crate::info::ActivityInfo;
use crate::policy::RetryPolicy;

use super::error::SandboxError;
use super::request::SandboxRequest;
use super::runtime::SandboxRuntime;

/// The variable that carries the activity idempotency key to the code.
pub const IDEMPOTENCY_KEY_ENV: &str = "HARVEST_IDEMPOTENCY_KEY";

/// The variable that carries the attempt number to the code.
pub const ATTEMPT_ENV: &str = "HARVEST_ATTEMPT";

/// How often the handler checks the activity cancellation.
pub const CANCEL_POLL_INTERVAL: Duration = Duration::from_millis(25);

/// A sandbox runtime registered as an activity.
#[derive(Debug, Clone)]
pub struct SandboxActivity {
    /// The activity name.
    pub name: String,
    /// The runtime that runs each request.
    pub runtime: Arc<SandboxRuntime>,
    /// The task queue. `None` is the `"default"` queue.
    pub queue: Option<String>,
    /// The retry policy. Only infrastructure faults retry.
    pub retry: RetryPolicy,
    /// A bound on all attempts together.
    pub schedule_to_close: Option<Duration>,
    /// A heartbeat timeout. The handler heartbeats three times per timeout
    /// while the code runs.
    pub heartbeat_timeout: Option<Duration>,
}

impl SandboxActivity {
    /// Register `runtime` as the activity `name`.
    #[must_use]
    pub fn new(name: impl Into<String>, runtime: Arc<SandboxRuntime>) -> Self {
        Self {
            name: name.into(),
            runtime,
            queue: None,
            retry: RetryPolicy::default(),
            schedule_to_close: None,
            heartbeat_timeout: None,
        }
    }

    /// Route the tasks to `queue`.
    #[must_use]
    pub fn with_queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = Some(queue.into());
        self
    }

    /// Set the retry policy.
    #[must_use]
    pub fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Bound all attempts together.
    #[must_use]
    pub const fn with_schedule_to_close(mut self, bound: Duration) -> Self {
        self.schedule_to_close = Some(bound);
        self
    }

    /// Set a heartbeat timeout.
    #[must_use]
    pub const fn with_heartbeat_timeout(mut self, timeout: Duration) -> Self {
        self.heartbeat_timeout = Some(timeout);
        self
    }

    /// The start-to-close timeout of one attempt: the wall-clock ceiling of
    /// the policy plus the backend overhead.
    #[must_use]
    pub fn start_to_close(&self) -> Duration {
        self.runtime
            .policy()
            .max_limits
            .wall_clock()
            .saturating_add(self.runtime.backend_overhead())
    }

    /// The [`ActivityInfo`] of this activity.
    ///
    /// The name and the queue are leaked to `'static`, as for a WASM
    /// activity. A registration happens once at startup, so the leak is
    /// bounded.
    #[must_use]
    pub fn activity_info(&self) -> ActivityInfo {
        let name: &'static str = Box::leak(self.name.clone().into_boxed_str());
        let queue: Option<&'static str> = self
            .queue
            .as_ref()
            .map(|q| &*Box::leak(q.clone().into_boxed_str()));
        let policy = self.runtime.policy();
        ActivityInfo {
            name,
            module: "sandbox",
            default_retry_policy: Some(self.retry.clone()),
            default_start_to_close: Some(self.start_to_close()),
            default_heartbeat_timeout: self.heartbeat_timeout,
            default_schedule_to_start: None,
            default_queue: queue,
            max_concurrent: None,
            concurrency_key: None,
            default_schedule_to_close: self.schedule_to_close,
            is_local: false,
            max_input_bytes: Some(policy.max_request_json_bytes()),
            max_result_bytes: Some(policy.max_outcome_json_bytes()),
            rate_limit_rps: None,
            rate_limit_burst: None,
            rate_limit_key: None,
            rate_limit_key_expr: None,
            circuit_breaker: None,
            requires: None,
            handler: sandbox_activity_handler,
            input_schema: None,
            output_schema: None,
        }
    }

    /// Run one request for `ctx`, with its cancellation, idempotency key and
    /// heartbeats.
    ///
    /// # Errors
    ///
    /// Returns a typed activity error payload.
    pub async fn run(
        &self,
        ctx: &ActivityContext,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let request: SandboxRequest = serde_json::from_value(input).map_err(|e| {
            failure(SandboxError::Denied(format!(
                "the input is not a sandbox request: {e}"
            )))
        })?;
        let mut engine_env = BTreeMap::new();
        if let Ok(key) = ctx.idempotency_key() {
            engine_env.insert(IDEMPOTENCY_KEY_ENV.to_owned(), key.to_string());
        }
        engine_env.insert(ATTEMPT_ENV.to_owned(), ctx.attempt().to_string());

        let cancel = CancellationToken::new();
        let run = self
            .runtime
            .run_with_engine_env(&request, engine_env, &cancel);
        tokio::pin!(run);
        let mut heartbeat = self
            .heartbeat_timeout
            .map(|timeout| tokio::time::interval((timeout / 3).max(Duration::from_millis(100))));
        loop {
            tokio::select! {
                result = &mut run => {
                    let outcome = result.map_err(failure)?;
                    return serde_json::to_value(outcome).map_err(|e| {
                        failure(SandboxError::Unavailable(format!("cannot encode the outcome: {e}")))
                    });
                }
                () = tokio::time::sleep(CANCEL_POLL_INTERVAL) => {
                    if ctx.is_cancelled() {
                        cancel.cancel();
                    }
                }
                () = tick(heartbeat.as_mut()) => {
                    // A failed heartbeat must not stop the run. The heartbeat
                    // timeout reports a stuck worker.
                    let _ = ctx.heartbeat(serde_json::json!({"sandbox": "running"})).await;
                }
            }
        }
    }
}

/// The sandbox activities of a worker, by name. The builder keeps it as
/// shared state.
#[derive(Debug, Default)]
pub struct SandboxActivities {
    by_name: HashMap<String, SandboxActivity>,
}

impl SandboxActivities {
    /// Add or replace an activity.
    pub fn insert(&mut self, activity: SandboxActivity) {
        self.by_name.insert(activity.name.clone(), activity);
    }

    /// The activity called `name`.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&SandboxActivity> {
        self.by_name.get(name)
    }

    /// The number of activities.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_name.len()
    }

    /// Whether there is no activity.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_name.is_empty()
    }
}

/// Run a sandbox request in an activity body that you write.
///
/// Use it when one `#[activity]` wraps a runtime from shared state. It
/// gives the same cancellation and environment as a registered activity.
///
/// # Errors
///
/// Returns a typed activity error payload.
pub async fn run_sandbox_activity(
    ctx: &ActivityContext,
    runtime: Arc<SandboxRuntime>,
    input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    SandboxActivity::new(ctx.activity_type(), runtime)
        .run(ctx, input)
        .await
}

/// The handler of each registered sandbox activity.
pub(crate) fn sandbox_activity_handler(
    ctx: &ActivityContext,
    input: serde_json::Value,
) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, String>> + Send + '_>> {
    Box::pin(async move {
        let activity = ctx
            .state::<SandboxActivities>()
            .and_then(|registry| registry.get(ctx.activity_type()))
            .ok_or_else(|| {
                failure(SandboxError::Config(format!(
                    "no sandbox activity named {:?} is registered on this worker",
                    ctx.activity_type()
                )))
            })?;
        activity.run(ctx, input).await
    })
}

/// The typed error payload of a sandbox error.
fn failure(err: SandboxError) -> String {
    use crate::failure::IntoActivityErrorString as _;
    crate::failure::ActivityFailure::from(err).into_error_payload()
}

/// Wait for the next tick, or forever with no interval.
async fn tick(interval: Option<&mut tokio::time::Interval>) {
    match interval {
        Some(interval) => {
            interval.tick().await;
        }
        None => std::future::pending().await,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::any::{Any, TypeId};
    use std::sync::Mutex;

    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::failure::{
        ERROR_TYPE_SANDBOX_CANCELLED, ERROR_TYPE_SANDBOX_DENIED, ERROR_TYPE_SANDBOX_UNAVAILABLE,
        parse_error_payload_full,
    };
    use crate::sandbox::error::SandboxError;
    use crate::sandbox::policy::{IsolationLevel, SandboxPolicy};
    use crate::sandbox::request::SandboxRequest;
    use crate::sandbox::runtime::tests::FakeBackend;

    fn activity(backend: FakeBackend) -> (Arc<FakeBackend>, SandboxActivity) {
        let backend = Arc::new(backend);
        let runtime =
            SandboxRuntime::new(backend.clone(), SandboxPolicy::for_development()).unwrap();
        (backend, SandboxActivity::new("run_code", Arc::new(runtime)))
    }

    fn state_with(activity: SandboxActivity) -> crate::context::SharedState {
        let mut registry = SandboxActivities::default();
        registry.insert(activity);
        let mut map: crate::context::SharedStateMap = HashMap::new();
        map.insert(
            TypeId::of::<SandboxActivities>(),
            Box::new(registry) as Box<dyn Any + Send + Sync>,
        );
        Arc::new(map)
    }

    fn ctx_for(state: crate::context::SharedState, cancel: CancellationToken) -> ActivityContext {
        let id = crate::types::ActivityExecId::new();
        let identity = crate::context::ActivityIdentity {
            activity_id: id,
            activity_type: "run_code".into(),
            ..crate::context::ActivityIdentity::for_test()
        };
        ActivityContext::new(state, None, cancel, identity)
            .with_idempotency_key(crate::types::IdempotencyKey::from_activity_exec_id(id))
            .with_attempt(2)
    }

    #[test]
    fn the_info_sets_bounds_from_the_policy() {
        let (_, activity) = activity(FakeBackend::new(IsolationLevel::Process));
        let activity = activity.with_queue("sandboxes");
        let info = activity.activity_info();
        let policy = activity.runtime.policy();
        assert_eq!(info.name, "run_code");
        assert_eq!(info.default_queue, Some("sandboxes"));
        assert!(!info.is_local);
        let start_to_close = info.default_start_to_close.unwrap();
        assert!(start_to_close > policy.max_limits.wall_clock());
        assert_eq!(start_to_close, activity.start_to_close());
        assert_eq!(info.max_result_bytes, Some(policy.max_outcome_json_bytes()));
        assert_eq!(info.max_input_bytes, Some(policy.max_request_json_bytes()));
        assert!(info.default_retry_policy.is_some());
    }

    #[tokio::test]
    async fn the_handler_runs_the_request_and_returns_the_outcome() {
        let (backend, activity) = activity(FakeBackend::new(IsolationLevel::Process));
        let ctx = ctx_for(state_with(activity), CancellationToken::new());
        let input = serde_json::to_value(SandboxRequest::new("sh", "echo hi")).unwrap();
        let output = sandbox_activity_handler(&ctx, input).await.unwrap();
        assert_eq!(output["status"]["kind"], "exited");
        assert_eq!(output["isolation"], "process");
        assert_eq!(backend.jobs.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn the_code_gets_the_idempotency_key_and_the_attempt() {
        let (backend, activity) = activity(FakeBackend::new(IsolationLevel::Process));
        let ctx = ctx_for(state_with(activity), CancellationToken::new());
        let key = ctx.idempotency_key().unwrap().to_string();
        let input = serde_json::to_value(SandboxRequest::new("sh", "true")).unwrap();
        sandbox_activity_handler(&ctx, input).await.unwrap();
        let jobs = backend.jobs.lock().unwrap().clone();
        let env = &jobs[0].plan.env;
        assert_eq!(env.get(IDEMPOTENCY_KEY_ENV), Some(&key));
        assert_eq!(env.get(ATTEMPT_ENV).map(String::as_str), Some("2"));
    }

    #[tokio::test]
    async fn an_unregistered_name_fails_without_retry() {
        let (_, mut activity) = activity(FakeBackend::new(IsolationLevel::Process));
        activity.name = "other".into();
        let ctx = ctx_for(state_with(activity), CancellationToken::new());
        let err = sandbox_activity_handler(&ctx, serde_json::json!({}))
            .await
            .expect_err("unregistered");
        let failure = parse_error_payload_full(&err);
        assert!(failure.non_retryable, "{err}");
    }

    #[tokio::test]
    async fn bad_input_is_a_denial() {
        let (backend, activity) = activity(FakeBackend::new(IsolationLevel::Process));
        let ctx = ctx_for(state_with(activity), CancellationToken::new());
        let err = sandbox_activity_handler(&ctx, serde_json::json!({"runtime": "sh", "oops": 1}))
            .await
            .expect_err("unknown field");
        let failure = parse_error_payload_full(&err);
        assert_eq!(failure.error_type, ERROR_TYPE_SANDBOX_DENIED);
        assert!(failure.non_retryable);
        assert!(backend.jobs.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_backend_fault_is_a_retryable_failure() {
        let backend = FakeBackend::new(IsolationLevel::Process);
        *backend.answer.lock().unwrap() = Some(Err(SandboxError::Unavailable("boot".into())));
        let (_, activity) = activity(backend);
        let ctx = ctx_for(state_with(activity), CancellationToken::new());
        let input = serde_json::to_value(SandboxRequest::new("sh", "true")).unwrap();
        let err = sandbox_activity_handler(&ctx, input)
            .await
            .expect_err("fault");
        let failure = parse_error_payload_full(&err);
        assert_eq!(failure.error_type, ERROR_TYPE_SANDBOX_UNAVAILABLE);
        assert!(!failure.non_retryable);
    }

    #[tokio::test]
    async fn an_activity_cancel_stops_the_run() {
        let mut backend = FakeBackend::new(IsolationLevel::Process);
        backend.delay = Duration::from_secs(60);
        let (backend, activity) = activity(backend);
        let cancel = CancellationToken::new();
        let ctx = ctx_for(state_with(activity), cancel.clone());
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            cancel.cancel();
        });
        let input = serde_json::to_value(SandboxRequest::new("sh", "sleep 60")).unwrap();
        let started = std::time::Instant::now();
        let err = sandbox_activity_handler(&ctx, input)
            .await
            .expect_err("cancelled");
        assert!(started.elapsed() < Duration::from_secs(5));
        assert_eq!(
            parse_error_payload_full(&err).error_type,
            ERROR_TYPE_SANDBOX_CANCELLED
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(backend.running.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[test]
    fn the_builder_registers_the_activity_and_its_runtime() {
        let (_, activity) = activity(FakeBackend::new(IsolationLevel::Process));
        let builder = crate::builder::HarvestBuilder::new().sandbox_activity(activity);
        assert!(
            builder
                .activity_infos()
                .iter()
                .any(|a| a.name == "run_code")
        );
        let built = builder.build();
        assert_eq!(built.activity_count(), 1);
        let registry = built.state::<SandboxActivities>().expect("registry state");
        assert!(registry.get("run_code").is_some());
    }

    #[test]
    fn two_registrations_share_one_registry() {
        let (_, first) = activity(FakeBackend::new(IsolationLevel::Process));
        let (_, mut second) = activity(FakeBackend::new(IsolationLevel::Process));
        second.name = "run_python".into();
        let built = crate::builder::HarvestBuilder::new()
            .sandbox_activity(first)
            .sandbox_activity(second)
            .build();
        assert_eq!(built.state::<SandboxActivities>().unwrap().len(), 2);
    }

    #[test]
    fn heartbeats_are_off_unless_asked_for() {
        let (_, activity) = activity(FakeBackend::new(IsolationLevel::Process));
        assert_eq!(activity.activity_info().default_heartbeat_timeout, None);
        let info = activity
            .with_heartbeat_timeout(Duration::from_secs(9))
            .activity_info();
        assert_eq!(info.default_heartbeat_timeout, Some(Duration::from_secs(9)));
        let _ = Mutex::new(());
    }
}
