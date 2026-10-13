//! The sandbox runtime: the policy, a backend and a concurrency bound.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use super::error::SandboxError;
use super::outcome::SandboxOutcome;
use super::policy::{
    IsolationLevel, MAX_ENGINE_ENV_BYTES, MAX_ENGINE_ENV_VARS, NetworkPolicy, SandboxPolicy,
};
use super::request::{ExecPlan, RESERVED_ENV_PREFIX, SandboxRequest};

/// A boxed, sendable future.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The default number of runs that one runtime starts at one time.
pub const DEFAULT_MAX_CONCURRENT: usize = 8;

/// The time that a backend gets on top of the wall-clock limit of a run, for
/// boot, setup and teardown. The runtime stops waiting after it.
pub const DEFAULT_BACKEND_OVERHEAD: Duration = Duration::from_secs(30);

/// The time that a backend gets to tear down after the runtime cancels it.
pub const CLEANUP_GRACE: Duration = Duration::from_secs(10);

/// The longest image name, in bytes. The outcome bound has room for it.
pub const MAX_IMAGE_NAME_BYTES: usize = 256;

/// The default time that a run waits for a free slot.
pub const DEFAULT_QUEUE_WAIT: Duration = Duration::from_secs(30);

/// What the engine adds to one run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunOptions {
    /// Variables for the code, added after the request check. Each name must
    /// start with `HARVEST_`. At most [`MAX_ENGINE_ENV_VARS`] variables with
    /// [`MAX_ENGINE_ENV_BYTES`] bytes in all.
    pub engine_env: BTreeMap<String, String>,
    /// The time left in the activity attempt. `None` is no bound.
    pub time_left: Option<Duration>,
}

/// One run for a backend: a checked plan, a run id and fresh entropy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxJob {
    /// A unique id for this run.
    pub run_id: String,
    /// 32 bytes from the host CSPRNG for the guest random pool.
    pub entropy: [u8; 32],
    /// The plan to run.
    pub plan: ExecPlan,
}

/// A place that runs plans.
pub trait SandboxBackend: Send + Sync + 'static {
    /// The isolation of this backend.
    fn isolation(&self) -> IsolationLevel;

    /// The name of the guest image, for the outcome. `None` by default.
    fn image(&self) -> Option<String> {
        None
    }

    /// Whether the backend can give code a network. `false` by default. A
    /// runtime refuses a policy that allows the network on a backend
    /// without one.
    fn supports_network(&self) -> bool {
        false
    }

    /// The longest time that the backend can need on top of the wall-clock
    /// limit, such as a boot, a response grace and a teardown. The runtime
    /// never gives a run less. Zero by default.
    fn overhead(&self) -> Duration {
        Duration::ZERO
    }

    /// Run `job`. Stop and clean up when `cancel` fires.
    fn execute(
        &self,
        job: SandboxJob,
        cancel: CancellationToken,
    ) -> BoxFuture<'_, Result<SandboxOutcome, SandboxError>>;
}

/// A policy, a backend and a bound on the runs at one time.
pub struct SandboxRuntime {
    backend: Arc<dyn SandboxBackend>,
    policy: Arc<SandboxPolicy>,
    permits: Arc<Semaphore>,
    max_concurrent: usize,
    overhead: Duration,
    queue_wait: Duration,
    /// The image name that `new` checked. Each outcome carries this copy,
    /// so a backend cannot change it later.
    image: Option<String>,
}

impl std::fmt::Debug for SandboxRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SandboxRuntime")
            .field("isolation", &self.backend.isolation())
            .field("max_concurrent", &self.max_concurrent)
            .field("overhead", &self.overhead)
            .field("queue_wait", &self.queue_wait)
            .finish_non_exhaustive()
    }
}

impl SandboxRuntime {
    /// A runtime that runs `policy` on `backend`.
    ///
    /// # Errors
    ///
    /// Returns [`SandboxError::Config`] in these cases:
    ///
    /// - the policy is not valid;
    /// - the backend isolation is weaker than `policy.min_isolation`;
    /// - the policy allows the network on a backend without one.
    pub fn new(
        backend: Arc<dyn SandboxBackend>,
        policy: SandboxPolicy,
    ) -> Result<Self, SandboxError> {
        policy.validate()?;
        let isolation = backend.isolation();
        if isolation < policy.min_isolation {
            return Err(SandboxError::Config(format!(
                "the backend isolation {isolation} is weaker than the policy minimum {}",
                policy.min_isolation
            )));
        }
        let image = backend.image();
        if let Some(image) = &image
            && (image.len() > MAX_IMAGE_NAME_BYTES || image.chars().any(char::is_control))
        {
            return Err(SandboxError::Config(format!(
                "the image name must be at most {MAX_IMAGE_NAME_BYTES} bytes, with no control \
                 character"
            )));
        }
        if policy.network == NetworkPolicy::Allow && !backend.supports_network() {
            return Err(SandboxError::Config(
                "the policy allows the network, but the backend gives code no network".into(),
            ));
        }
        let overhead = DEFAULT_BACKEND_OVERHEAD.max(backend.overhead());
        Ok(Self {
            backend,
            policy: Arc::new(policy),
            permits: Arc::new(Semaphore::new(DEFAULT_MAX_CONCURRENT)),
            max_concurrent: DEFAULT_MAX_CONCURRENT,
            overhead,
            queue_wait: DEFAULT_QUEUE_WAIT,
            image,
        })
    }

    /// Bound the runs at one time. A value of 0 becomes 1.
    #[must_use]
    pub fn with_max_concurrent(mut self, max_concurrent: usize) -> Self {
        let max_concurrent = max_concurrent.max(1);
        self.permits = Arc::new(Semaphore::new(max_concurrent));
        self.max_concurrent = max_concurrent;
        self
    }

    /// Set the time that the backend gets on top of the wall-clock limit.
    /// It is never less than [`SandboxBackend::overhead`].
    #[must_use]
    pub fn with_backend_overhead(mut self, overhead: Duration) -> Self {
        self.overhead = overhead.max(self.backend.overhead());
        self
    }

    /// Set the time that a run waits for a free slot. After it, the run is
    /// [`SandboxError::Unavailable`], and the activity retries later.
    #[must_use]
    pub const fn with_queue_wait(mut self, queue_wait: Duration) -> Self {
        self.queue_wait = queue_wait;
        self
    }

    /// The time that a run waits for a free slot.
    #[must_use]
    pub const fn queue_wait(&self) -> Duration {
        self.queue_wait
    }

    /// The longest time that one run takes: the queue wait, the wall-clock
    /// ceiling of the policy, the backend overhead and the cleanup grace.
    ///
    /// An activity attempt with at least this time never times out while
    /// its sandbox still runs.
    #[must_use]
    pub fn max_attempt_time(&self) -> Duration {
        self.queue_wait
            .saturating_add(self.policy.max_limits.wall_clock())
            .saturating_add(self.overhead)
            .saturating_add(CLEANUP_GRACE)
    }

    /// The policy.
    #[must_use]
    pub fn policy(&self) -> &SandboxPolicy {
        &self.policy
    }

    /// The isolation of the backend.
    #[must_use]
    pub fn isolation(&self) -> IsolationLevel {
        self.backend.isolation()
    }

    /// The bound on the runs at one time.
    #[must_use]
    pub const fn max_concurrent(&self) -> usize {
        self.max_concurrent
    }

    /// The time that the backend gets on top of the wall-clock limit.
    #[must_use]
    pub const fn backend_overhead(&self) -> Duration {
        self.overhead
    }

    /// Check `request`, wait for a free slot, run it and check the outcome.
    ///
    /// # Errors
    ///
    /// Returns [`SandboxError::Denied`] for a request that breaks the policy,
    /// [`SandboxError::Cancelled`] when `cancel` fires, and the backend error
    /// otherwise. These are [`SandboxError::Unavailable`]:
    ///
    /// - no free slot within the queue wait;
    /// - a backend that runs past the wall-clock limit plus the overhead.
    pub async fn run(
        &self,
        request: &SandboxRequest,
        cancel: &CancellationToken,
    ) -> Result<SandboxOutcome, SandboxError> {
        self.run_with(request, RunOptions::default(), cancel).await
    }

    /// Like [`Self::run`], with engine variables and the time left in the
    /// activity attempt.
    ///
    /// The runtime keeps [`CLEANUP_GRACE`] of the time left for teardown.
    /// It does not start the code when the rest is shorter than the
    /// wall-clock limit plus the overhead.
    ///
    /// # Errors
    ///
    /// The errors of [`Self::run`]. A variable name without the `HARVEST_`
    /// prefix, or engine variables over their bounds, are a
    /// [`SandboxError::Config`]. Too little time left is
    /// [`SandboxError::Unavailable`].
    pub async fn run_with(
        &self,
        request: &SandboxRequest,
        options: RunOptions,
        cancel: &CancellationToken,
    ) -> Result<SandboxOutcome, SandboxError> {
        let deadline = options
            .time_left
            .map(|left| tokio::time::Instant::now() + left.saturating_sub(CLEANUP_GRACE));
        let mut plan = self.policy.plan(request)?;
        // The request check does not see these variables, so they have
        // bounds of their own. The request frame bound keeps room for them.
        let engine_bytes: usize = options
            .engine_env
            .iter()
            .map(|(name, value)| name.len() + value.len())
            .sum();
        if options.engine_env.len() > MAX_ENGINE_ENV_VARS || engine_bytes > MAX_ENGINE_ENV_BYTES {
            return Err(SandboxError::Config(format!(
                "the engine variables are {} with {engine_bytes} bytes, over the bound of \
                 {MAX_ENGINE_ENV_VARS} with {MAX_ENGINE_ENV_BYTES} bytes",
                options.engine_env.len()
            )));
        }
        for (name, value) in options.engine_env {
            if !name.starts_with(RESERVED_ENV_PREFIX) {
                return Err(SandboxError::Config(format!(
                    "engine variable {name:?} must start with {RESERVED_ENV_PREFIX}"
                )));
            }
            plan.env.insert(name, value);
        }
        plan.validate()?;
        let _permit = self.acquire_slot(deadline, cancel).await?;
        let budget = plan.limits.wall_clock().saturating_add(self.overhead);
        if let Some(deadline) = deadline {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            if left < budget {
                return Err(SandboxError::Unavailable(format!(
                    "the attempt has {left:?} left, under the {budget:?} that the run can take"
                )));
            }
        }
        let mut entropy = [0u8; 32];
        rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut entropy);
        let job = SandboxJob {
            run_id: uuid::Uuid::new_v4().to_string(),
            entropy,
            plan: plan.clone(),
        };
        let backend_cancel = cancel.child_token();
        let started = tokio::time::Instant::now();
        let execute = self.backend.execute(job, backend_cancel.clone());
        tokio::pin!(execute);
        let result = tokio::select! {
            result = &mut execute => result,
            () = tokio::time::sleep(budget) => {
                // Cancel the backend, and give it time to tear down. An
                // outcome that arrives in the grace is still an outcome.
                backend_cancel.cancel();
                match tokio::time::timeout(CLEANUP_GRACE, &mut execute).await {
                    Ok(Ok(outcome)) => Ok(outcome),
                    _ => Err(SandboxError::Unavailable(format!(
                        "the backend did not finish within {budget:?}"
                    ))),
                }
            }
        };
        let mut outcome = result?;
        outcome.check_bounds(&plan)?;
        // The host sets these fields. A guest duration is at most the time
        // that the host measured.
        let elapsed = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        outcome.duration_ms = outcome.duration_ms.min(elapsed);
        outcome.isolation = self.backend.isolation();
        outcome.image.clone_from(&self.image);
        Ok(outcome)
    }

    /// Wait for a free slot until the queue wait or `deadline` ends.
    async fn acquire_slot(
        &self,
        deadline: Option<tokio::time::Instant>,
        cancel: &CancellationToken,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, SandboxError> {
        let mut wait_until = tokio::time::Instant::now() + self.queue_wait;
        if let Some(deadline) = deadline {
            wait_until = wait_until.min(deadline);
        }
        tokio::select! {
            biased;
            () = cancel.cancelled() => Err(SandboxError::Cancelled),
            permit = Arc::clone(&self.permits).acquire_owned() => permit.map_err(|_| {
                SandboxError::Unavailable("the sandbox runtime is closed".into())
            }),
            () = tokio::time::sleep_until(wait_until) => Err(SandboxError::Unavailable(format!(
                "no free sandbox slot within {:?}; {} runs are active",
                self.queue_wait, self.max_concurrent
            ))),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
pub(crate) mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::sandbox::outcome::tests::outcome;
    use crate::sandbox::outcome::{OutputFile, RunStatus};

    /// A backend that records each job and answers with a fixed outcome.
    pub struct FakeBackend {
        pub isolation: IsolationLevel,
        pub jobs: Mutex<Vec<SandboxJob>>,
        pub running: AtomicUsize,
        pub peak: AtomicUsize,
        pub delay: Duration,
        pub answer: Mutex<Option<Result<SandboxOutcome, SandboxError>>>,
        pub image: Mutex<String>,
    }

    impl FakeBackend {
        pub fn new(isolation: IsolationLevel) -> Self {
            Self {
                isolation,
                jobs: Mutex::new(Vec::new()),
                running: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                delay: Duration::ZERO,
                answer: Mutex::new(None),
                image: Mutex::new("fake:1".into()),
            }
        }
    }

    impl SandboxBackend for FakeBackend {
        fn isolation(&self) -> IsolationLevel {
            self.isolation
        }

        fn image(&self) -> Option<String> {
            Some(self.image.lock().unwrap().clone())
        }

        fn supports_network(&self) -> bool {
            true
        }

        fn execute(
            &self,
            job: SandboxJob,
            cancel: CancellationToken,
        ) -> BoxFuture<'_, Result<SandboxOutcome, SandboxError>> {
            Box::pin(async move {
                self.jobs.lock().unwrap().push(job);
                let now = self.running.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak.fetch_max(now, Ordering::SeqCst);
                let result = tokio::select! {
                    () = tokio::time::sleep(self.delay) => self
                        .answer
                        .lock()
                        .unwrap()
                        .clone()
                        .unwrap_or_else(|| Ok(outcome(RunStatus::Exited { code: 0 }))),
                    () = cancel.cancelled() => Err(SandboxError::Cancelled),
                };
                self.running.fetch_sub(1, Ordering::SeqCst);
                result
            })
        }
    }

    fn runtime(backend: FakeBackend) -> (Arc<FakeBackend>, SandboxRuntime) {
        let backend = Arc::new(backend);
        let runtime = SandboxRuntime::new(backend.clone(), SandboxPolicy::for_development())
            .expect("valid runtime");
        (backend, runtime)
    }

    /// A backend with no network device.
    struct NoNetworkBackend;

    impl SandboxBackend for NoNetworkBackend {
        fn isolation(&self) -> IsolationLevel {
            IsolationLevel::MicroVm
        }

        fn execute(
            &self,
            _job: SandboxJob,
            _cancel: CancellationToken,
        ) -> BoxFuture<'_, Result<SandboxOutcome, SandboxError>> {
            Box::pin(async { Err(SandboxError::Cancelled) })
        }
    }

    #[test]
    fn a_policy_that_allows_the_network_needs_a_backend_with_one() {
        let policy = SandboxPolicy::default().with_network(NetworkPolicy::Allow);
        let err = SandboxRuntime::new(Arc::new(NoNetworkBackend), policy).expect_err("no NIC");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("network")),
            "{err:?}"
        );
        SandboxRuntime::new(Arc::new(NoNetworkBackend), SandboxPolicy::default()).unwrap();
    }

    #[test]
    fn the_default_policy_refuses_a_process_backend() {
        let backend = Arc::new(FakeBackend::new(IsolationLevel::Process));
        let err = SandboxRuntime::new(backend, SandboxPolicy::default()).expect_err("too weak");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("micro_vm")),
            "{err:?}"
        );
    }

    #[test]
    fn the_default_policy_accepts_a_microvm_backend() {
        let backend = Arc::new(FakeBackend::new(IsolationLevel::MicroVm));
        assert!(SandboxRuntime::new(backend, SandboxPolicy::default()).is_ok());
    }

    #[test]
    fn an_invalid_policy_is_refused() {
        let mut policy = SandboxPolicy::for_development();
        policy.runtimes.clear();
        let backend = Arc::new(FakeBackend::new(IsolationLevel::Process));
        assert!(matches!(
            SandboxRuntime::new(backend, policy),
            Err(SandboxError::Config(_))
        ));
    }

    #[tokio::test]
    async fn a_denied_request_never_reaches_the_backend() {
        let (backend, runtime) = runtime(FakeBackend::new(IsolationLevel::Process));
        let err = runtime
            .run(&SandboxRequest::new("cobol", ""), &CancellationToken::new())
            .await
            .expect_err("denied");
        assert!(matches!(err, SandboxError::Denied(_)));
        assert!(backend.jobs.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn each_job_gets_a_fresh_run_id_and_fresh_entropy() {
        let (backend, runtime) = runtime(FakeBackend::new(IsolationLevel::Process));
        let request = SandboxRequest::new("sh", "true");
        for _ in 0..2 {
            runtime
                .run(&request, &CancellationToken::new())
                .await
                .unwrap();
        }
        let jobs = backend.jobs.lock().unwrap();
        assert_ne!(jobs[0].run_id, jobs[1].run_id);
        assert_ne!(jobs[0].entropy, jobs[1].entropy);
        assert_ne!(jobs[0].entropy, [0; 32]);
    }

    #[tokio::test]
    async fn the_host_stamps_the_isolation_and_the_image() {
        let mut backend = FakeBackend::new(IsolationLevel::Process);
        let mut lying = outcome(RunStatus::Exited { code: 0 });
        lying.isolation = IsolationLevel::MicroVm;
        backend.answer = Mutex::new(Some(Ok(lying)));
        let (_, runtime) = runtime(backend);
        let out = runtime
            .run(
                &SandboxRequest::new("sh", "true"),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(out.isolation, IsolationLevel::Process);
        assert_eq!(out.image.as_deref(), Some("fake:1"));
    }

    #[tokio::test]
    async fn the_outcome_carries_the_image_name_that_new_checked() {
        let (backend, runtime) = runtime(FakeBackend::new(IsolationLevel::Process));
        *backend.image.lock().unwrap() = "x\0".repeat(MAX_IMAGE_NAME_BYTES);
        let out = runtime
            .run(
                &SandboxRequest::new("sh", "true"),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(out.image.as_deref(), Some("fake:1"));
    }

    #[tokio::test]
    async fn the_duration_is_never_longer_than_the_host_measured() {
        let mut backend = FakeBackend::new(IsolationLevel::Process);
        let mut lying = outcome(RunStatus::Exited { code: 0 });
        lying.duration_ms = u64::MAX;
        backend.answer = Mutex::new(Some(Ok(lying)));
        let (_, runtime) = runtime(backend);
        let out = runtime
            .run(
                &SandboxRequest::new("sh", "true"),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        assert!(out.duration_ms < 60_000, "{}", out.duration_ms);
    }

    /// A backend with an image name that history cannot hold.
    struct BadImageBackend(String);

    impl SandboxBackend for BadImageBackend {
        fn isolation(&self) -> IsolationLevel {
            IsolationLevel::Process
        }

        fn image(&self) -> Option<String> {
            Some(self.0.clone())
        }

        fn execute(
            &self,
            _job: SandboxJob,
            _cancel: CancellationToken,
        ) -> BoxFuture<'_, Result<SandboxOutcome, SandboxError>> {
            Box::pin(async { Err(SandboxError::Cancelled) })
        }
    }

    /// A backend that needs a long time on top of the wall clock.
    struct SlowBackend(Duration);

    impl SandboxBackend for SlowBackend {
        fn isolation(&self) -> IsolationLevel {
            IsolationLevel::Process
        }

        fn overhead(&self) -> Duration {
            self.0
        }

        fn execute(
            &self,
            _job: SandboxJob,
            _cancel: CancellationToken,
        ) -> BoxFuture<'_, Result<SandboxOutcome, SandboxError>> {
            Box::pin(async { Err(SandboxError::Cancelled) })
        }
    }

    #[test]
    fn the_runtime_gives_a_run_the_overhead_of_its_backend() {
        let need = DEFAULT_BACKEND_OVERHEAD + Duration::from_secs(30);
        let runtime = SandboxRuntime::new(
            Arc::new(SlowBackend(need)),
            SandboxPolicy::for_development(),
        )
        .unwrap();
        assert_eq!(runtime.backend_overhead(), need);
        // A smaller explicit overhead cannot cut the time that the backend needs.
        let runtime = runtime.with_backend_overhead(Duration::from_secs(1));
        assert_eq!(runtime.backend_overhead(), need);
        let runtime = runtime.with_backend_overhead(need * 2);
        assert_eq!(runtime.backend_overhead(), need * 2);
    }

    #[test]
    fn a_long_or_nul_image_name_is_a_config_error() {
        for name in ["x".repeat(MAX_IMAGE_NAME_BYTES + 1), "a\0b".to_owned()] {
            let backend = Arc::new(BadImageBackend(name));
            let err = SandboxRuntime::new(backend, SandboxPolicy::for_development())
                .expect_err("bad image name");
            assert!(matches!(err, SandboxError::Config(_)), "{err:?}");
        }
    }

    #[tokio::test]
    async fn an_outcome_over_its_bounds_is_a_protocol_violation() {
        let mut backend = FakeBackend::new(IsolationLevel::Process);
        let mut bad = outcome(RunStatus::Exited { code: 0 });
        bad.outputs.push(OutputFile {
            path: "never-asked".into(),
            content: Vec::new(),
        });
        backend.answer = Mutex::new(Some(Ok(bad)));
        let (_, runtime) = runtime(backend);
        let err = runtime
            .run(
                &SandboxRequest::new("sh", "true"),
                &CancellationToken::new(),
            )
            .await
            .expect_err("bounds");
        assert!(matches!(err, SandboxError::Protocol(_)));
    }

    #[tokio::test]
    async fn runs_at_one_time_never_exceed_the_bound() {
        let mut backend = FakeBackend::new(IsolationLevel::Process);
        backend.delay = Duration::from_millis(30);
        let (backend, runtime) = runtime(backend);
        let runtime = Arc::new(runtime.with_max_concurrent(2));
        let mut tasks = Vec::new();
        for _ in 0..6 {
            let runtime = Arc::clone(&runtime);
            tasks.push(tokio::spawn(async move {
                runtime
                    .run(
                        &SandboxRequest::new("sh", "true"),
                        &CancellationToken::new(),
                    )
                    .await
            }));
        }
        for task in tasks {
            task.await.unwrap().unwrap();
        }
        assert_eq!(backend.peak.load(Ordering::SeqCst), 2);
        assert_eq!(backend.jobs.lock().unwrap().len(), 6);
    }

    #[tokio::test]
    async fn a_cancel_while_waiting_for_a_slot_returns_cancelled() {
        let mut backend = FakeBackend::new(IsolationLevel::Process);
        backend.delay = Duration::from_secs(60);
        let (_, runtime) = runtime(backend);
        let runtime = Arc::new(runtime.with_max_concurrent(1));
        let busy = {
            let runtime = Arc::clone(&runtime);
            tokio::spawn(async move {
                runtime
                    .run(
                        &SandboxRequest::new("sh", "true"),
                        &CancellationToken::new(),
                    )
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        let cancel = CancellationToken::new();
        let waiting = {
            let runtime = Arc::clone(&runtime);
            let cancel = cancel.clone();
            tokio::spawn(async move {
                runtime
                    .run(&SandboxRequest::new("sh", "true"), &cancel)
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiting.is_finished(), "the run waits for the busy slot");
        cancel.cancel();
        let err = waiting.await.unwrap().expect_err("cancelled");
        assert_eq!(err, SandboxError::Cancelled);
        busy.abort();
    }

    #[tokio::test]
    async fn a_cancel_during_a_run_reaches_the_backend() {
        let mut backend = FakeBackend::new(IsolationLevel::Process);
        backend.delay = Duration::from_secs(60);
        let (_, runtime) = runtime(backend);
        let cancel = CancellationToken::new();
        let trigger = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            trigger.cancel();
        });
        let started = std::time::Instant::now();
        let err = runtime
            .run(&SandboxRequest::new("sh", "true"), &cancel)
            .await
            .expect_err("cancelled");
        assert_eq!(err, SandboxError::Cancelled);
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[tokio::test]
    async fn a_backend_that_hangs_past_the_overhead_is_unavailable() {
        let mut backend = FakeBackend::new(IsolationLevel::Process);
        backend.delay = Duration::from_secs(60);
        let (backend, runtime) = runtime(backend);
        let runtime = runtime.with_backend_overhead(Duration::from_millis(50));
        let request = SandboxRequest::new("sh", "true").with_timeout_ms(10);
        let err = runtime
            .run(&request, &CancellationToken::new())
            .await
            .expect_err("watchdog");
        assert!(matches!(err, SandboxError::Unavailable(_)), "{err:?}");
        // The watchdog cancels the backend, so the run does not leak.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(backend.running.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn a_full_runtime_is_unavailable_after_the_queue_wait() {
        let mut backend = FakeBackend::new(IsolationLevel::Process);
        backend.delay = Duration::from_secs(60);
        let (backend, runtime) = runtime(backend);
        let runtime = Arc::new(
            runtime
                .with_max_concurrent(1)
                .with_queue_wait(Duration::from_millis(50)),
        );
        let busy = {
            let runtime = Arc::clone(&runtime);
            tokio::spawn(async move {
                runtime
                    .run(
                        &SandboxRequest::new("sh", "true"),
                        &CancellationToken::new(),
                    )
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        let err = runtime
            .run(
                &SandboxRequest::new("sh", "true"),
                &CancellationToken::new(),
            )
            .await
            .expect_err("no slot");
        assert!(
            matches!(err, SandboxError::Unavailable(ref m) if m.contains("slot")),
            "{err:?}"
        );
        assert_eq!(backend.jobs.lock().unwrap().len(), 1);
        busy.abort();
    }

    #[tokio::test]
    async fn an_attempt_without_time_for_the_run_is_unavailable_before_the_code_runs() {
        let (backend, runtime) = runtime(FakeBackend::new(IsolationLevel::Process));
        let request = SandboxRequest::new("sh", "true").with_timeout_ms(10_000);
        let options = RunOptions {
            time_left: Some(CLEANUP_GRACE + Duration::from_secs(5)),
            ..RunOptions::default()
        };
        let err = runtime
            .run_with(&request, options, &CancellationToken::new())
            .await
            .expect_err("too little time");
        assert!(
            matches!(err, SandboxError::Unavailable(ref m) if m.contains("left")),
            "{err:?}"
        );
        assert_eq!(backend.jobs.lock().unwrap().len(), 0, "the code never ran");
    }

    #[tokio::test]
    async fn oversized_engine_variables_are_a_config_error() {
        let (backend, runtime) = runtime(FakeBackend::new(IsolationLevel::Process));
        let request = SandboxRequest::new("sh", "true");
        let big = RunOptions {
            engine_env: BTreeMap::from([(
                "HARVEST_BIG".to_owned(),
                "x".repeat(MAX_ENGINE_ENV_BYTES),
            )]),
            ..RunOptions::default()
        };
        let many = RunOptions {
            engine_env: (0..=MAX_ENGINE_ENV_VARS)
                .map(|i| (format!("HARVEST_V{i}"), String::new()))
                .collect(),
            ..RunOptions::default()
        };
        for options in [big, many] {
            let err = runtime
                .run_with(&request, options, &CancellationToken::new())
                .await
                .expect_err("too much engine data");
            assert!(
                matches!(err, SandboxError::Config(ref m) if m.contains("engine variables")),
                "{err:?}"
            );
        }
        assert_eq!(backend.jobs.lock().unwrap().len(), 0, "the code never ran");
    }

    #[test]
    fn the_attempt_time_covers_the_queue_the_run_and_the_cleanup() {
        let (_, runtime) = runtime(FakeBackend::new(IsolationLevel::Process));
        let runtime = runtime
            .with_queue_wait(Duration::from_secs(7))
            .with_backend_overhead(Duration::from_secs(11));
        assert_eq!(
            runtime.max_attempt_time(),
            runtime.policy().max_limits.wall_clock()
                + Duration::from_secs(7)
                + Duration::from_secs(11)
                + CLEANUP_GRACE
        );
    }

    /// A backend that answers late after a cancel.
    struct LateBackend;

    impl SandboxBackend for LateBackend {
        fn isolation(&self) -> IsolationLevel {
            IsolationLevel::Process
        }

        fn execute(
            &self,
            _job: SandboxJob,
            cancel: CancellationToken,
        ) -> BoxFuture<'_, Result<SandboxOutcome, SandboxError>> {
            Box::pin(async move {
                cancel.cancelled().await;
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok(outcome(RunStatus::TimedOut))
            })
        }
    }

    #[tokio::test]
    async fn an_outcome_inside_the_cleanup_grace_is_kept() {
        let runtime = SandboxRuntime::new(Arc::new(LateBackend), SandboxPolicy::for_development())
            .unwrap()
            .with_backend_overhead(Duration::from_millis(30));
        let request = SandboxRequest::new("sh", "true").with_timeout_ms(10);
        let out = runtime
            .run(&request, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(out.status, RunStatus::TimedOut);
    }

    #[test]
    fn a_zero_bound_becomes_one() {
        let (_, runtime) = runtime(FakeBackend::new(IsolationLevel::Process));
        assert_eq!(runtime.with_max_concurrent(0).max_concurrent(), 1);
    }
}
