//! The sandbox runtime: the policy, a backend and a concurrency bound.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use super::error::SandboxError;
use super::outcome::SandboxOutcome;
use super::policy::{IsolationLevel, SandboxPolicy};
use super::request::{ExecPlan, SandboxRequest};

/// A boxed, sendable future.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The default number of runs that one runtime starts at one time.
pub const DEFAULT_MAX_CONCURRENT: usize = 8;

/// The time that a backend gets on top of the wall-clock limit of a run, for
/// boot, setup and teardown. The runtime stops waiting after it.
pub const DEFAULT_BACKEND_OVERHEAD: Duration = Duration::from_secs(30);

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
}

impl std::fmt::Debug for SandboxRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SandboxRuntime")
            .field("isolation", &self.backend.isolation())
            .field("max_concurrent", &self.max_concurrent)
            .field("overhead", &self.overhead)
            .finish_non_exhaustive()
    }
}

impl SandboxRuntime {
    /// A runtime that runs `policy` on `backend`.
    ///
    /// # Errors
    ///
    /// Returns [`SandboxError::Config`] when the policy is not valid, or when
    /// the backend isolation is weaker than `policy.min_isolation`.
    pub fn new(
        backend: Arc<dyn SandboxBackend>,
        policy: SandboxPolicy,
    ) -> Result<Self, SandboxError> {
        let _ = (backend, policy);
        todo!("SandboxRuntime::new")
    }

    /// Bound the runs at one time. A value of 0 becomes 1.
    #[must_use]
    pub fn with_max_concurrent(self, max_concurrent: usize) -> Self {
        let _ = max_concurrent;
        todo!("SandboxRuntime::with_max_concurrent")
    }

    /// Set the time that the backend gets on top of the wall-clock limit.
    #[must_use]
    pub const fn with_backend_overhead(mut self, overhead: Duration) -> Self {
        self.overhead = overhead;
        self
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
    /// otherwise. A backend that runs past the wall-clock limit plus the
    /// overhead is [`SandboxError::Unavailable`].
    pub async fn run(
        &self,
        request: &SandboxRequest,
        cancel: &CancellationToken,
    ) -> Result<SandboxOutcome, SandboxError> {
        let _ = (request, cancel);
        todo!("SandboxRuntime::run")
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
    pub(crate) struct FakeBackend {
        pub isolation: IsolationLevel,
        pub jobs: Mutex<Vec<SandboxJob>>,
        pub running: AtomicUsize,
        pub peak: AtomicUsize,
        pub delay: Duration,
        pub answer: Mutex<Option<Result<SandboxOutcome, SandboxError>>>,
    }

    impl FakeBackend {
        pub(crate) fn new(isolation: IsolationLevel) -> Self {
            Self {
                isolation,
                jobs: Mutex::new(Vec::new()),
                running: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                delay: Duration::ZERO,
                answer: Mutex::new(None),
            }
        }
    }

    impl SandboxBackend for FakeBackend {
        fn isolation(&self) -> IsolationLevel {
            self.isolation
        }

        fn image(&self) -> Option<String> {
            Some("fake:1".into())
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

    #[test]
    fn the_default_policy_refuses_a_process_backend() {
        let backend = Arc::new(FakeBackend::new(IsolationLevel::Process));
        let err = SandboxRuntime::new(backend, SandboxPolicy::default()).expect_err("too weak");
        assert!(matches!(err, SandboxError::Config(ref m) if m.contains("micro_vm")), "{err:?}");
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
            .run(&SandboxRequest::new("sh", "true"), &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(out.isolation, IsolationLevel::Process);
        assert_eq!(out.image.as_deref(), Some("fake:1"));
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
            .run(&SandboxRequest::new("sh", "true"), &CancellationToken::new())
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
                    .run(&SandboxRequest::new("sh", "true"), &CancellationToken::new())
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
                    .run(&SandboxRequest::new("sh", "true"), &CancellationToken::new())
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        let cancel = CancellationToken::new();
        cancel.cancel();
        let err = runtime
            .run(&SandboxRequest::new("sh", "true"), &cancel)
            .await
            .expect_err("cancelled");
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

    #[test]
    fn a_zero_bound_becomes_one() {
        let (_, runtime) = runtime(FakeBackend::new(IsolationLevel::Process));
        assert_eq!(runtime.with_max_concurrent(0).max_concurrent(), 1);
    }
}
