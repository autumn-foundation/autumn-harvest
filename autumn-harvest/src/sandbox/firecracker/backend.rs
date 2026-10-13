//! The Firecracker backend: one single-use microVM per run.

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::config::GUEST_AGENT_PORT;
use super::launcher::{LaunchedVm, VmLauncher};
use super::pool::WarmPool;
use crate::sandbox::error::SandboxError;
use crate::sandbox::outcome::SandboxOutcome;
use crate::sandbox::policy::IsolationLevel;
use crate::sandbox::runtime::{BoxFuture, SandboxBackend, SandboxJob};

/// The default time from launch until the guest agent accepts.
pub const DEFAULT_BOOT_TIMEOUT: Duration = Duration::from_secs(10);

/// The default time that the guest gets on top of the wall-clock limit to
/// send its outcome.
pub const DEFAULT_RESPONSE_GRACE: Duration = Duration::from_secs(5);

/// The timing of the backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackendSettings {
    /// The vsock port of the guest agent.
    pub guest_port: u32,
    /// The time from launch until the guest agent accepts.
    pub boot_timeout: Duration,
    /// The time on top of the wall-clock limit for the outcome to arrive.
    pub response_grace: Duration,
}

impl Default for BackendSettings {
    fn default() -> Self {
        Self {
            guest_port: GUEST_AGENT_PORT,
            boot_timeout: DEFAULT_BOOT_TIMEOUT,
            response_grace: DEFAULT_RESPONSE_GRACE,
        }
    }
}

/// A backend that runs each plan in its own Firecracker microVM.
pub struct FirecrackerBackend {
    launcher: Arc<dyn VmLauncher>,
    pool: Option<WarmPool>,
    settings: BackendSettings,
}

impl std::fmt::Debug for FirecrackerBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FirecrackerBackend")
            .field("settings", &self.settings)
            .field("warm_pool", &self.pool.is_some())
            .finish_non_exhaustive()
    }
}

impl FirecrackerBackend {
    /// A backend that launches a new microVM for each run.
    #[must_use]
    pub fn new(launcher: Arc<dyn VmLauncher>, settings: BackendSettings) -> Self {
        Self {
            launcher,
            pool: None,
            settings,
        }
    }

    /// Keep `size` booted microVMs ready. Call it inside a Tokio runtime.
    #[must_use]
    pub fn with_warm_pool(self, size: usize) -> Self {
        let _ = size;
        todo!("FirecrackerBackend::with_warm_pool")
    }

    /// The warm pool, when the backend has one.
    #[must_use]
    pub const fn warm_pool(&self) -> Option<&WarmPool> {
        self.pool.as_ref()
    }

    /// The timing of the backend.
    #[must_use]
    pub const fn settings(&self) -> BackendSettings {
        self.settings
    }

    /// Stop the warm pool and tear down its microVMs.
    pub async fn shutdown(&self) {
        todo!("FirecrackerBackend::shutdown")
    }
}

impl SandboxBackend for FirecrackerBackend {
    fn isolation(&self) -> IsolationLevel {
        IsolationLevel::MicroVm
    }

    fn image(&self) -> Option<String> {
        self.launcher.image_name()
    }

    fn execute(
        &self,
        job: SandboxJob,
        cancel: CancellationToken,
    ) -> BoxFuture<'_, Result<SandboxOutcome, SandboxError>> {
        let _ = (job, cancel, std::marker::PhantomData::<Box<dyn LaunchedVm>>);
        todo!("FirecrackerBackend::execute")
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::time::Instant;

    use super::*;
    use crate::sandbox::exec::{ExecOptions, ProcessExecutor};
    use crate::sandbox::firecracker::fake::{FakeLauncher, GuestMode};
    use crate::sandbox::outcome::RunStatus;
    use crate::sandbox::policy::{NetworkPolicy, SandboxPolicy};
    use crate::sandbox::request::SandboxRequest;
    use crate::sandbox::runtime::SandboxRuntime;

    fn launcher(mode: GuestMode) -> Arc<FakeLauncher> {
        Arc::new(FakeLauncher::new(
            mode,
            Arc::new(ProcessExecutor::new(ExecOptions::default())),
        ))
    }

    fn fast() -> BackendSettings {
        BackendSettings {
            guest_port: GUEST_AGENT_PORT,
            boot_timeout: Duration::from_millis(500),
            response_grace: Duration::from_millis(500),
        }
    }

    fn job(code: &str, timeout_ms: u64) -> SandboxJob {
        // The fake microVM shares the host network, so this test plan allows
        // it. A real microVM has no network device.
        let plan = SandboxPolicy::default()
            .with_network(NetworkPolicy::Allow)
            .plan(&SandboxRequest::new("sh", code).with_timeout_ms(timeout_ms))
            .unwrap();
        SandboxJob {
            run_id: uuid::Uuid::new_v4().to_string(),
            entropy: [1; 32],
            plan,
        }
    }

    #[tokio::test]
    async fn a_run_boots_a_vm_runs_the_code_and_tears_the_vm_down() {
        let fake = launcher(GuestMode::Serve);
        let backend = FirecrackerBackend::new(fake.clone(), fast());
        let out = backend
            .execute(job("echo in the vm; exit 7", 5_000), CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(out.status, RunStatus::Exited { code: 7 });
        assert_eq!(out.stdout, "in the vm\n");
        assert_eq!(fake.launched(), 1);
        assert_eq!(fake.live(), 0, "the VM must be torn down");
    }

    #[tokio::test]
    async fn each_run_gets_a_new_vm() {
        let fake = launcher(GuestMode::Serve);
        let backend = FirecrackerBackend::new(fake.clone(), fast());
        for _ in 0..3 {
            backend
                .execute(job("true", 5_000), CancellationToken::new())
                .await
                .unwrap();
        }
        assert_eq!(fake.launched(), 3);
        assert_eq!(fake.live(), 0);
    }

    #[tokio::test]
    async fn the_backend_reports_microvm_isolation_and_the_image() {
        let backend = FirecrackerBackend::new(launcher(GuestMode::Serve), fast());
        assert_eq!(backend.isolation(), IsolationLevel::MicroVm);
        assert_eq!(backend.image().as_deref(), Some("fake-image"));
    }

    #[tokio::test]
    async fn a_guest_that_never_listens_is_unavailable_at_the_boot_deadline() {
        let fake = launcher(GuestMode::NeverListen);
        let backend = FirecrackerBackend::new(fake.clone(), fast());
        let started = Instant::now();
        let err = backend
            .execute(job("true", 5_000), CancellationToken::new())
            .await
            .expect_err("no guest");
        assert!(matches!(err, SandboxError::Unavailable(_)), "{err:?}");
        assert!(started.elapsed() < Duration::from_secs(5));
        assert_eq!(fake.live(), 0);
    }

    #[tokio::test]
    async fn a_vmm_that_exits_is_unavailable_at_once() {
        let fake = launcher(GuestMode::Exited);
        let mut settings = fast();
        settings.boot_timeout = Duration::from_secs(30);
        let backend = FirecrackerBackend::new(fake.clone(), settings);
        let started = Instant::now();
        let err = backend
            .execute(job("true", 5_000), CancellationToken::new())
            .await
            .expect_err("exited");
        assert!(matches!(err, SandboxError::Unavailable(ref m) if m.contains("status 1")), "{err:?}");
        assert!(started.elapsed() < Duration::from_secs(5));
        assert_eq!(fake.live(), 0);
    }

    #[tokio::test]
    async fn a_garbage_answer_is_a_protocol_violation() {
        let fake = launcher(GuestMode::Garbage);
        let backend = FirecrackerBackend::new(fake.clone(), fast());
        let err = backend
            .execute(job("true", 5_000), CancellationToken::new())
            .await
            .expect_err("garbage");
        assert!(matches!(err, SandboxError::Protocol(_)), "{err:?}");
        assert_eq!(fake.live(), 0);
    }

    #[tokio::test]
    async fn a_guest_that_never_answers_is_unavailable_after_the_grace() {
        let fake = launcher(GuestMode::Hang);
        let backend = FirecrackerBackend::new(fake.clone(), fast());
        let started = Instant::now();
        let err = backend
            .execute(job("true", 100), CancellationToken::new())
            .await
            .expect_err("hang");
        assert!(matches!(err, SandboxError::Unavailable(_)), "{err:?}");
        assert!(started.elapsed() < Duration::from_secs(5));
        assert_eq!(fake.live(), 0);
    }

    #[tokio::test]
    async fn a_cancel_during_the_run_tears_the_vm_down() {
        let fake = launcher(GuestMode::Serve);
        let backend = FirecrackerBackend::new(fake.clone(), fast());
        let cancel = CancellationToken::new();
        let trigger = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            trigger.cancel();
        });
        let err = backend
            .execute(job("sleep 30", 60_000), cancel)
            .await
            .expect_err("cancelled");
        assert_eq!(err, SandboxError::Cancelled);
        assert_eq!(fake.live(), 0);
    }

    #[tokio::test]
    async fn dropping_the_run_tears_the_vm_down() {
        let fake = launcher(GuestMode::Serve);
        let backend = Arc::new(FirecrackerBackend::new(fake.clone(), fast()));
        let task = {
            let backend = Arc::clone(&backend);
            tokio::spawn(async move {
                backend
                    .execute(job("sleep 30", 60_000), CancellationToken::new())
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(200)).await;
        task.abort();
        let _ = task.await;
        let deadline = Instant::now() + Duration::from_secs(5);
        while fake.live() > 0 {
            assert!(Instant::now() < deadline, "the VM leaked");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn a_launch_failure_is_unavailable() {
        let fake = launcher(GuestMode::Serve);
        fake.failures_left.store(1, std::sync::atomic::Ordering::SeqCst);
        let backend = FirecrackerBackend::new(fake.clone(), fast());
        let err = backend
            .execute(job("true", 5_000), CancellationToken::new())
            .await
            .expect_err("launch failed");
        assert!(matches!(err, SandboxError::Unavailable(_)));
    }

    #[tokio::test]
    async fn a_warm_pool_serves_a_booted_vm_and_refills() {
        let fake = launcher(GuestMode::Serve);
        let backend = FirecrackerBackend::new(fake.clone(), fast()).with_warm_pool(2);
        let pool = backend.warm_pool().unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while pool.ready_len() < 2 {
            assert!(Instant::now() < deadline, "the pool never filled");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let out = backend
            .execute(job("echo warm", 5_000), CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(out.stdout, "warm\n");
        while pool.ready_len() < 2 {
            assert!(Instant::now() < deadline, "the pool never refilled");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(fake.launched(), 3);
        backend.shutdown().await;
        assert_eq!(fake.live(), 0);
    }

    #[tokio::test]
    async fn the_runtime_accepts_the_backend_under_the_default_policy() {
        let fake = launcher(GuestMode::Serve);
        let backend = Arc::new(FirecrackerBackend::new(fake, fast()));
        let runtime = SandboxRuntime::new(backend, SandboxPolicy::default().with_network(NetworkPolicy::Allow))
            .unwrap();
        let out = runtime
            .run(&SandboxRequest::new("sh", "echo ok"), &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(out.isolation, IsolationLevel::MicroVm);
        assert_eq!(out.image.as_deref(), Some("fake-image"));
    }
}
