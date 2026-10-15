//! The Firecracker backend: one single-use microVM per run.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;

use super::config::GUEST_AGENT_PORT;
use super::launcher::{LaunchedVm, VmLauncher};
use super::pool::{MAX_WARM_POOL, WarmPool};
use super::vsock::connect_guest;
use crate::sandbox::error::SandboxError;
use crate::sandbox::guest::{read_outcome, send_run};
use crate::sandbox::outcome::SandboxOutcome;
use crate::sandbox::policy::IsolationLevel;
use crate::sandbox::runtime::{BoxFuture, SandboxBackend, SandboxJob};

/// The default time from launch until the guest agent accepts.
pub const DEFAULT_BOOT_TIMEOUT: Duration = Duration::from_secs(10);

/// The default time that the guest gets on top of the wall-clock limit to
/// send its outcome.
///
/// The guest writes the input files. After the run, it waits up to two
/// seconds for the pipes of the code to close. Then it
/// waits up to two seconds for the run cgroup to empty, reads the outputs,
/// removes the work tree and sends the outcome. The grace covers each step.
pub const DEFAULT_RESPONSE_GRACE: Duration = Duration::from_secs(15);

/// The time to move one full guest frame over vsock and to decode or encode
/// it. A frame holds at most 64 MiB, so this assumes at least 16 MiB/s.
pub const FRAME_IO_ALLOWANCE: Duration = Duration::from_secs(4);

/// The time for the guest to remove a full work tree before it answers.
///
/// The `/work` tmpfs holds at most [`WORK_MAX_INODES`] inodes. Their removal
/// took about 0.2 s on one host CPU, so this keeps a tenfold margin.
///
/// [`WORK_MAX_INODES`]: crate::sandbox::guest::WORK_MAX_INODES
pub const WORK_REMOVE_ALLOWANCE: Duration = Duration::from_secs(2);

/// The shortest response grace.
///
/// It holds the guest pipe wait, the run cgroup wait and the removal of the
/// work tree. It adds one frame allowance to write the input files, and one
/// to read the outputs and send the outcome.
pub const MIN_RESPONSE_GRACE: Duration = crate::sandbox::exec::PIPE_DRAIN_GRACE
    .saturating_add(crate::sandbox::exec::CGROUP_WAIT)
    .saturating_add(WORK_REMOVE_ALLOWANCE)
    .saturating_add(FRAME_IO_ALLOWANCE)
    .saturating_add(FRAME_IO_ALLOWANCE);

/// The timing of the backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackendSettings {
    /// The vsock port of the guest agent, when the launcher sets none.
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
    /// The vsock port of the guest agent, fixed at construction.
    port: u32,
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
    ///
    /// # Errors
    ///
    /// Returns [`SandboxError::Config`] for a boot timeout of zero, a
    /// response grace under [`MIN_RESPONSE_GRACE`], or the wildcard guest
    /// port. The port is the port of the launcher, or else of the settings.
    pub fn new(
        launcher: Arc<dyn VmLauncher>,
        settings: BackendSettings,
    ) -> Result<Self, SandboxError> {
        // Ask the launcher once, so the pool and each run use one port.
        let port = launcher.guest_port().unwrap_or(settings.guest_port);
        if port == super::vsock::VMADDR_PORT_ANY {
            return Err(SandboxError::Config(
                "the guest port u32::MAX is VMADDR_PORT_ANY, which no host can connect to".into(),
            ));
        }
        // A zero boot timeout expires before any guest can answer.
        if settings.boot_timeout.is_zero() {
            return Err(SandboxError::Config(
                "the boot timeout must be above zero".into(),
            ));
        }
        // After the wall clock, the guest can wait for the pipes before it
        // answers. A shorter grace drops a valid outcome.
        if settings.response_grace < MIN_RESPONSE_GRACE {
            return Err(SandboxError::Config(format!(
                "the response grace must be at least {MIN_RESPONSE_GRACE:?}, not {:?}",
                settings.response_grace
            )));
        }
        Ok(Self {
            launcher,
            pool: None,
            settings,
            port,
        })
    }

    /// Keep `size` booted microVMs ready. Call it inside a Tokio runtime.
    ///
    /// # Errors
    ///
    /// Returns [`SandboxError::Config`] for a size above [`MAX_WARM_POOL`].
    pub fn with_warm_pool(mut self, size: usize) -> Result<Self, SandboxError> {
        if size > MAX_WARM_POOL {
            return Err(SandboxError::Config(format!(
                "a warm pool holds at most {MAX_WARM_POOL} microVMs, not {size}"
            )));
        }
        if size > 0 {
            self.pool = Some(WarmPool::start(
                Arc::clone(&self.launcher),
                size,
                self.guest_port(),
                self.settings.boot_timeout,
            ));
        }
        Ok(self)
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
        if let Some(pool) = &self.pool {
            pool.shutdown().await;
        }
    }

    /// The vsock port of the guest agent: the port of the launcher, or the
    /// port of the settings.
    const fn guest_port(&self) -> u32 {
        self.port
    }

    /// Run `job` on `vm`: connect over vsock, send the plan, read the
    /// outcome.
    async fn run_on(
        &self,
        vm: &mut Box<dyn LaunchedVm>,
        job: &SandboxJob,
        cancel: &CancellationToken,
    ) -> Result<SandboxOutcome, SandboxError> {
        let path = vm.vsock_path().to_path_buf();
        let deadline = Instant::now() + self.settings.boot_timeout;
        let mut exited = || vm.exit_status();
        let mut stream = tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(SandboxError::Cancelled),
            stream = connect_guest(&path, self.guest_port(), deadline, &mut exited) => stream?,
        };
        let budget = job
            .plan
            .limits
            .wall_clock()
            .saturating_add(self.settings.response_grace);
        exchange(&mut stream, job, FRAME_IO_ALLOWANCE, budget, cancel).await
    }
}

/// Send `job` on `stream` and wait for its outcome.
///
/// The request has its own limit, `send_limit`. It covers the write, the
/// guest decode, the ready answer and the start message. The outcome
/// `budget` starts after the start, so a large request takes no time from
/// the code.
async fn exchange<S>(
    stream: &mut S,
    job: &SandboxJob,
    send_limit: Duration,
    budget: Duration,
    cancel: &CancellationToken,
) -> Result<SandboxOutcome, SandboxError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    tokio::select! {
        biased;
        () = cancel.cancelled() => return Err(SandboxError::Cancelled),
        sent = tokio::time::timeout(send_limit, send_run(stream, job)) => {
            sent.unwrap_or_else(|_| {
                Err(SandboxError::Unavailable(format!(
                    "the guest did not take the request within {send_limit:?}"
                )))
            })?;
        }
    }
    tokio::select! {
        biased;
        () = cancel.cancelled() => Err(SandboxError::Cancelled),
        outcome = tokio::time::timeout(budget, read_outcome(stream, job)) => {
            outcome.unwrap_or_else(|_| {
                Err(SandboxError::Unavailable(format!(
                    "the guest sent no outcome within {budget:?}"
                )))
            })
        }
    }
}

impl SandboxBackend for FirecrackerBackend {
    fn isolation(&self) -> IsolationLevel {
        IsolationLevel::MicroVm
    }

    /// The launch, a boot, the request transfer, the response grace, and
    /// the teardown of the VMM and its cgroup.
    fn overhead(&self) -> Duration {
        self.launcher
            .launch_budget()
            .saturating_add(self.settings.boot_timeout)
            .saturating_add(FRAME_IO_ALLOWANCE)
            .saturating_add(self.settings.response_grace)
            .saturating_add(super::launcher::KILL_WAIT)
            .saturating_add(super::launcher::CGROUP_EMPTY_WAIT)
    }

    fn image(&self) -> Option<String> {
        self.launcher.image_name()
    }

    fn memory_capacity(&self) -> Option<u64> {
        self.launcher.memory_capacity()
    }

    fn work_capacity(&self) -> Option<u64> {
        self.launcher.work_capacity()
    }

    fn execute(
        &self,
        job: SandboxJob,
        cancel: CancellationToken,
    ) -> BoxFuture<'_, Result<SandboxOutcome, SandboxError>> {
        Box::pin(async move {
            let mut warm = self.pool.as_ref().and_then(WarmPool::take);
            // A warm microVM can die in the pool, for example by the OOM
            // killer. Then boot a new one. The dead one is torn down in the
            // background, so its cleanup takes no time from this run.
            if warm.as_mut().is_some_and(|vm| vm.exit_status().is_some())
                && let Some(dead) = warm.take()
            {
                tokio::spawn(dead.shutdown());
            }
            let vm = if let Some(vm) = warm {
                vm
            } else {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => return Err(SandboxError::Cancelled),
                    vm = self.launcher.launch() => vm?,
                }
            };
            // Every path below ends in one teardown. A dropped future tears
            // the microVM down through the guard.
            let mut guard = VmGuard(Some(vm));
            let result = match guard.0.as_mut() {
                Some(vm) => self.run_on(vm, &job, &cancel).await,
                None => Err(SandboxError::Unavailable("the microVM is gone".into())),
            };
            guard.shutdown().await;
            result
        })
    }
}

/// Tears a microVM down on every path, the dropped future included.
struct VmGuard(Option<Box<dyn LaunchedVm>>);

impl VmGuard {
    async fn shutdown(&mut self) {
        if let Some(vm) = self.0.take() {
            vm.shutdown().await;
        }
    }
}

impl Drop for VmGuard {
    fn drop(&mut self) {
        let Some(vm) = self.0.take() else {
            return;
        };
        // Run the async teardown when a runtime exists. Otherwise the drop
        // of the microVM kills it at once.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(vm.shutdown());
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::time::Instant;

    use super::*;
    use crate::sandbox::exec::{ExecOptions, ProcessExecutor};
    use crate::sandbox::firecracker::fake::{FakeLauncher, GuestMode, HostNetworkExecutor};
    use crate::sandbox::outcome::RunStatus;
    use crate::sandbox::policy::SandboxPolicy;
    use crate::sandbox::request::SandboxRequest;
    use crate::sandbox::runtime::SandboxRuntime;

    fn launcher(mode: GuestMode) -> Arc<FakeLauncher> {
        Arc::new(FakeLauncher::new(
            mode,
            Arc::new(HostNetworkExecutor(ProcessExecutor::new(
                ExecOptions::default(),
            ))),
        ))
    }

    fn fast() -> BackendSettings {
        BackendSettings {
            guest_port: GUEST_AGENT_PORT,
            boot_timeout: Duration::from_millis(500),
            response_grace: MIN_RESPONSE_GRACE,
        }
    }

    fn job(code: &str, timeout_ms: u64) -> SandboxJob {
        let plan = SandboxPolicy::default()
            .plan(&SandboxRequest::new("sh", code).with_timeout_ms(timeout_ms))
            .unwrap();
        SandboxJob {
            run_id: uuid::Uuid::new_v4().to_string(),
            entropy: [1; 32],
            plan,
        }
    }

    #[test]
    fn a_response_grace_shorter_than_the_guest_cleanup_is_a_config_error() {
        let settings = BackendSettings {
            response_grace: crate::sandbox::exec::PIPE_DRAIN_GRACE,
            ..fast()
        };
        let err = FirecrackerBackend::new(launcher(GuestMode::Serve), settings)
            .expect_err("short response grace");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("response grace")),
            "{err:?}"
        );
        let settings = BackendSettings {
            response_grace: MIN_RESPONSE_GRACE,
            ..fast()
        };
        FirecrackerBackend::new(launcher(GuestMode::Serve), settings).unwrap();
    }

    #[test]
    fn the_overhead_covers_the_boot_the_grace_and_the_teardown() {
        let settings = BackendSettings {
            boot_timeout: Duration::from_secs(60),
            ..fast()
        };
        let backend = FirecrackerBackend::new(launcher(GuestMode::Serve), settings).unwrap();
        let need = Duration::from_secs(60)
            + FRAME_IO_ALLOWANCE
            + settings.response_grace
            + super::super::launcher::KILL_WAIT
            + super::super::launcher::CGROUP_EMPTY_WAIT;
        assert!(SandboxBackend::overhead(&backend) >= need);
    }

    #[test]
    fn the_overhead_includes_the_launch_budget() {
        let mut fake = FakeLauncher::new(
            GuestMode::Serve,
            Arc::new(HostNetworkExecutor(ProcessExecutor::new(
                ExecOptions::default(),
            ))),
        );
        fake.launch_budget = Duration::from_secs(40);
        let backend = FirecrackerBackend::new(Arc::new(fake), fast()).unwrap();
        let settings = backend.settings();
        let need = Duration::from_secs(40) + settings.boot_timeout + settings.response_grace;
        assert!(SandboxBackend::overhead(&backend) >= need);
    }

    #[test]
    fn the_minimum_grace_covers_the_guest_setup_and_cleanup() {
        // One frame for the input files, one for the outputs and the
        // outcome frame.
        let need = crate::sandbox::exec::PIPE_DRAIN_GRACE
            + crate::sandbox::exec::CGROUP_WAIT
            + WORK_REMOVE_ALLOWANCE
            + FRAME_IO_ALLOWANCE * 2;
        assert!(MIN_RESPONSE_GRACE >= need, "{MIN_RESPONSE_GRACE:?}");
        assert!(DEFAULT_RESPONSE_GRACE >= MIN_RESPONSE_GRACE);
    }

    #[test]
    fn a_launcher_with_the_wildcard_port_is_a_config_error() {
        let mut fake = FakeLauncher::new(
            GuestMode::Serve,
            Arc::new(HostNetworkExecutor(ProcessExecutor::new(
                ExecOptions::default(),
            ))),
        );
        fake.port = Some(u32::MAX);
        let err = FirecrackerBackend::new(Arc::new(fake), fast()).expect_err("VMADDR_PORT_ANY");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("port")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn the_backend_asks_the_launcher_for_its_port_once() {
        let fake = launcher(GuestMode::Serve);
        let backend = FirecrackerBackend::new(fake.clone(), fast()).unwrap();
        for _ in 0..2 {
            backend
                .execute(job("true", 5_000), CancellationToken::new())
                .await
                .unwrap();
        }
        assert_eq!(fake.port_queries.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_wildcard_guest_port_is_a_config_error() {
        let settings = BackendSettings {
            guest_port: u32::MAX,
            ..fast()
        };
        let err = FirecrackerBackend::new(launcher(GuestMode::Serve), settings)
            .expect_err("VMADDR_PORT_ANY");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("port")),
            "{err:?}"
        );
    }

    #[test]
    fn a_zero_boot_timeout_is_a_config_error() {
        let settings = BackendSettings {
            boot_timeout: Duration::ZERO,
            ..fast()
        };
        let err = FirecrackerBackend::new(launcher(GuestMode::Serve), settings)
            .expect_err("zero boot timeout");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("boot timeout")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn a_run_boots_a_vm_runs_the_code_and_tears_the_vm_down() {
        let fake = launcher(GuestMode::Serve);
        let backend = FirecrackerBackend::new(fake.clone(), fast()).unwrap();
        let out = backend
            .execute(
                job("echo in the vm; exit 7", 5_000),
                CancellationToken::new(),
            )
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
        let backend = FirecrackerBackend::new(fake.clone(), fast()).unwrap();
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
        let backend = FirecrackerBackend::new(launcher(GuestMode::Serve), fast()).unwrap();
        assert_eq!(backend.isolation(), IsolationLevel::MicroVm);
        assert_eq!(backend.image().as_deref(), Some("fake-image"));
    }

    #[tokio::test]
    async fn a_guest_that_never_listens_is_unavailable_at_the_boot_deadline() {
        let fake = launcher(GuestMode::NeverListen);
        let backend = FirecrackerBackend::new(fake.clone(), fast()).unwrap();
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
        let backend = FirecrackerBackend::new(fake.clone(), settings).unwrap();
        let started = Instant::now();
        let err = backend
            .execute(job("true", 5_000), CancellationToken::new())
            .await
            .expect_err("exited");
        assert!(
            matches!(err, SandboxError::Unavailable(ref m) if m.contains("status 1")),
            "{err:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(5));
        assert_eq!(fake.live(), 0);
    }

    #[tokio::test]
    async fn a_garbage_answer_is_a_protocol_violation() {
        let fake = launcher(GuestMode::Garbage);
        let backend = FirecrackerBackend::new(fake.clone(), fast()).unwrap();
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
        let backend = FirecrackerBackend::new(fake.clone(), fast()).unwrap();
        let started = Instant::now();
        let err = backend
            .execute(job("true", 100), CancellationToken::new())
            .await
            .expect_err("hang");
        assert!(matches!(err, SandboxError::Unavailable(_)), "{err:?}");
        // The run gets its wall clock and then the grace, no more.
        assert!(started.elapsed() < MIN_RESPONSE_GRACE + Duration::from_secs(2));
        assert_eq!(fake.live(), 0);
    }

    #[tokio::test]
    async fn a_cancel_during_the_run_tears_the_vm_down() {
        let fake = launcher(GuestMode::Serve);
        let backend = FirecrackerBackend::new(fake.clone(), fast()).unwrap();
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
        let backend = Arc::new(FirecrackerBackend::new(fake.clone(), fast()).unwrap());
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
        fake.failures_left
            .store(1, std::sync::atomic::Ordering::SeqCst);
        let backend = FirecrackerBackend::new(fake.clone(), fast()).unwrap();
        let err = backend
            .execute(job("true", 5_000), CancellationToken::new())
            .await
            .expect_err("launch failed");
        assert!(matches!(err, SandboxError::Unavailable(_)));
    }

    #[tokio::test]
    async fn a_warm_pool_too_large_is_a_config_error_not_a_panic() {
        let fake = launcher(GuestMode::Serve);
        for size in [MAX_WARM_POOL + 1, usize::MAX] {
            let backend = FirecrackerBackend::new(fake.clone(), fast()).unwrap();
            let err = backend.with_warm_pool(size).expect_err("too large");
            assert!(matches!(err, SandboxError::Config(_)), "{err:?}");
        }
        assert_eq!(fake.launched(), 0);
    }

    #[tokio::test]
    async fn a_warm_pool_serves_a_booted_vm_and_refills() {
        let fake = launcher(GuestMode::Serve);
        let backend = FirecrackerBackend::new(fake.clone(), fast())
            .unwrap()
            .with_warm_pool(2)
            .unwrap();
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
    async fn a_warm_vm_that_died_in_the_pool_is_replaced() {
        let fake = launcher(GuestMode::Serve);
        let backend = FirecrackerBackend::new(fake.clone(), fast())
            .unwrap()
            .with_warm_pool(1)
            .unwrap();
        let pool = backend.warm_pool().unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while pool.ready_len() < 1 {
            assert!(Instant::now() < deadline, "the pool never filled");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        fake.crashed.lock().unwrap().push("fake-0".into());
        let out = backend
            .execute(job("echo fresh", 5_000), CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(out.stdout, "fresh\n");
        backend.shutdown().await;
        assert_eq!(fake.live(), 0);
    }

    #[tokio::test]
    async fn the_teardown_of_a_dead_warm_vm_takes_no_run_time() {
        let mut fake = FakeLauncher::new(
            GuestMode::Serve,
            Arc::new(HostNetworkExecutor(ProcessExecutor::new(
                ExecOptions::default(),
            ))),
        );
        fake.crash_teardown = Duration::from_secs(3);
        let fake = Arc::new(fake);
        let backend = FirecrackerBackend::new(fake.clone(), fast())
            .unwrap()
            .with_warm_pool(1)
            .unwrap();
        let pool = backend.warm_pool().unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while pool.ready_len() < 1 {
            assert!(Instant::now() < deadline, "the pool never filled");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        fake.crashed.lock().unwrap().push("fake-0".into());
        let started = Instant::now();
        let out = backend
            .execute(job("echo fresh", 5_000), CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(out.stdout, "fresh\n");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the run waited for the dead VM: {:?}",
            started.elapsed()
        );
        backend.shutdown().await;
        // The dead VM still goes away, in the background.
        let deadline = Instant::now() + Duration::from_secs(10);
        while fake.live() > 0 {
            assert!(Instant::now() < deadline, "the dead VM leaked");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// A guest on `stream` that waits `before_read`, reads the request,
    /// takes the start, waits `before_reply`, and sends an outcome.
    fn slow_guest(
        mut stream: tokio::io::DuplexStream,
        before_read: Duration,
        before_reply: Duration,
    ) -> tokio::task::JoinHandle<()> {
        use crate::sandbox::protocol::{
            GuestMessage, HostMessage, MAX_CONTROL_FRAME_BYTES, MAX_FRAME_BYTES, OutcomeMessage,
            ReadyMessage, read_message, write_message,
        };
        tokio::spawn(async move {
            tokio::time::sleep(before_read).await;
            let message: HostMessage = read_message(&mut stream, MAX_FRAME_BYTES).await.unwrap();
            let HostMessage::Run(run) = message else {
                panic!("not a run");
            };
            let ready = GuestMessage::Ready(ReadyMessage {
                run_id: run.run_id.clone(),
            });
            write_message(&mut stream, ready, MAX_FRAME_BYTES)
                .await
                .unwrap();
            let start: HostMessage = read_message(&mut stream, MAX_CONTROL_FRAME_BYTES)
                .await
                .unwrap();
            assert!(matches!(start, HostMessage::Start(_)), "{start:?}");
            tokio::time::sleep(before_reply).await;
            let outcome = SandboxOutcome {
                status: RunStatus::Exited { code: 0 },
                stdout: String::new(),
                stderr: String::new(),
                stdout_bytes: 0,
                stderr_bytes: 0,
                stdout_truncated: false,
                stderr_truncated: false,
                outputs: Vec::new(),
                skipped_outputs: Vec::new(),
                duration_ms: 0,
                isolation: IsolationLevel::MicroVm,
                image: None,
            };
            let reply = GuestMessage::Outcome(OutcomeMessage {
                run_id: run.run_id,
                outcome,
            });
            write_message(&mut stream, reply, MAX_FRAME_BYTES)
                .await
                .unwrap();
        })
    }

    #[tokio::test]
    async fn a_slow_request_transfer_takes_no_time_from_the_outcome_budget() {
        // A small pipe makes the request write wait until the guest reads.
        let (mut host, guest) = tokio::io::duplex(64);
        let guest = slow_guest(
            guest,
            Duration::from_millis(800),
            Duration::from_millis(700),
        );
        let outcome = exchange(
            &mut host,
            &job("true", 1_000),
            Duration::from_secs(5),
            Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await;
        assert!(outcome.is_ok(), "{outcome:?}");
        guest.await.unwrap();
    }

    #[tokio::test]
    async fn a_guest_that_does_not_take_the_request_is_unavailable() {
        let (mut host, _guest) = tokio::io::duplex(64);
        let started = Instant::now();
        let err = exchange(
            &mut host,
            &job("true", 1_000),
            Duration::from_millis(300),
            Duration::from_secs(30),
            &CancellationToken::new(),
        )
        .await
        .expect_err("no reader");
        assert!(matches!(err, SandboxError::Unavailable(_)), "{err:?}");
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[tokio::test]
    async fn the_runtime_accepts_the_backend_under_the_default_policy() {
        let fake = launcher(GuestMode::Serve);
        let backend = Arc::new(FirecrackerBackend::new(fake, fast()).unwrap());
        let runtime = SandboxRuntime::new(backend, SandboxPolicy::default()).unwrap();
        let out = runtime
            .run(
                &SandboxRequest::new("sh", "echo ok"),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(out.isolation, IsolationLevel::MicroVm);
        assert_eq!(out.image.as_deref(), Some("fake-image"));
    }
}
