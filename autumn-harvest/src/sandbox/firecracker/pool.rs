//! A warm pool of booted, single-use microVMs.
//!
//! A boot takes about 125 ms before the guest agent listens. The pool hides
//! it: a background task keeps a number of microVMs booted and checked with a
//! ping. A run takes one microVM, and the microVM never comes back. The task
//! then boots a new one. A failed boot waits before the next try, with a
//! delay that doubles up to [`MAX_REFILL_BACKOFF`].

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use tokio::sync::Notify;
use tokio::task::JoinHandle;

use super::launcher::{LaunchedVm, VmLauncher};
use super::vsock::connect_guest;
use crate::sandbox::error::SandboxError;
use crate::sandbox::guest::ping;

/// The first wait after a failed boot.
pub const FIRST_REFILL_BACKOFF: Duration = Duration::from_millis(50);

/// The longest wait after failed boots.
pub const MAX_REFILL_BACKOFF: Duration = Duration::from_secs(5);

/// A pool of booted microVMs.
pub struct WarmPool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    ready: Mutex<VecDeque<Box<dyn LaunchedVm>>>,
    wake: Notify,
    closed: AtomicBool,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for WarmPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WarmPool")
            .field("ready", &self.ready_len())
            .finish_non_exhaustive()
    }
}

/// The most microVMs that a warm pool of a backend keeps ready. Each one can
/// hold its guest memory, so this many of the default machine can hold over
/// 2 TiB.
pub const MAX_WARM_POOL: usize = 1024;

impl WarmPool {
    /// Start a pool that keeps `target` microVMs from `launcher` ready. Each
    /// microVM must answer a ping on `port` within `boot_timeout` to count
    /// as ready. Call it inside a Tokio runtime.
    #[must_use]
    pub(crate) fn start(
        launcher: Arc<dyn VmLauncher>,
        target: usize,
        port: u32,
        boot_timeout: Duration,
    ) -> Self {
        let inner = Arc::new(PoolInner {
            ready: Mutex::new(VecDeque::new()),
            wake: Notify::new(),
            closed: AtomicBool::new(false),
            task: Mutex::new(None),
        });
        let task = tokio::spawn(refill(
            Arc::clone(&inner),
            launcher,
            target,
            port,
            boot_timeout,
        ));
        *inner.task.lock().unwrap_or_else(PoisonError::into_inner) = Some(task);
        Self { inner }
    }

    /// Take a ready microVM, if one exists. The pool then boots a new one.
    #[must_use]
    pub fn take(&self) -> Option<Box<dyn LaunchedVm>> {
        let vm = self
            .inner
            .ready
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_front();
        if vm.is_some() {
            self.inner.wake.notify_one();
        }
        vm
    }

    /// The number of ready microVMs.
    #[must_use]
    pub fn ready_len(&self) -> usize {
        self.inner
            .ready
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// Stop refilling and tear down each ready microVM.
    pub async fn shutdown(&self) {
        // The refill task checks `closed` under the `ready` lock before each
        // push, so the drain below sees each pushed microVM.
        self.inner.closed.store(true, Ordering::SeqCst);
        let task = self
            .inner
            .task
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(task) = task {
            task.abort();
            // A microVM that the task still boots is dropped with the task,
            // and its drop kills it. So wait until the task is gone.
            let _ = task.await;
        }
        let ready: Vec<_> = self
            .inner
            .ready
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .drain(..)
            .collect();
        for vm in ready {
            vm.shutdown().await;
        }
    }
}

impl Drop for WarmPool {
    fn drop(&mut self) {
        self.inner.closed.store(true, Ordering::SeqCst);
        let task = self
            .inner
            .task
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(task) = task {
            task.abort();
        }
        // Dropping a microVM kills it at once; each one cleans the rest up
        // in its own drop.
        self.inner
            .ready
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }
}

/// Keep the pool at `target` ready microVMs until it closes.
async fn refill(
    inner: Arc<PoolInner>,
    launcher: Arc<dyn VmLauncher>,
    target: usize,
    port: u32,
    boot_timeout: Duration,
) {
    let mut backoff = FIRST_REFILL_BACKOFF;
    loop {
        if inner.closed.load(Ordering::SeqCst) {
            return;
        }
        let ready = inner
            .ready
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len();
        if ready >= target {
            inner.wake.notified().await;
            continue;
        }
        match launch_ready(launcher.as_ref(), port, boot_timeout).await {
            Ok(vm) => {
                backoff = FIRST_REFILL_BACKOFF;
                // Check `closed` under the lock that `shutdown` drains with,
                // so no microVM enters the pool after the drain.
                let refused = {
                    let mut ready = inner.ready.lock().unwrap_or_else(PoisonError::into_inner);
                    if inner.closed.load(Ordering::SeqCst) {
                        Some(vm)
                    } else {
                        ready.push_back(vm);
                        None
                    }
                };
                if let Some(vm) = refused {
                    vm.shutdown().await;
                    return;
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "the sandbox warm pool could not boot a microVM");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_REFILL_BACKOFF);
            }
        }
    }
}

/// Launch a microVM and wait until its guest agent answers a ping.
async fn launch_ready(
    launcher: &dyn VmLauncher,
    port: u32,
    boot_timeout: Duration,
) -> Result<Box<dyn LaunchedVm>, SandboxError> {
    let mut vm = launcher.launch().await?;
    let path = vm.vsock_path().to_path_buf();
    let deadline = Instant::now() + boot_timeout;
    let ready = async {
        let mut stream = connect_guest(&path, port, deadline, &mut || vm.exit_status()).await?;
        // The guest kernel can finish the handshake while the agent hangs,
        // so the ping has the same deadline.
        tokio::time::timeout_at(deadline.into(), ping(&mut stream))
            .await
            .map_err(|_| {
                SandboxError::Unavailable("the guest agent did not answer a ping".into())
            })?
    }
    .await;
    match ready {
        Ok(()) => Ok(vm),
        Err(e) => {
            vm.shutdown().await;
            Err(e)
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::atomic::Ordering;
    use std::time::Instant;

    use super::*;
    use crate::sandbox::exec::{ExecOptions, ProcessExecutor};
    use crate::sandbox::firecracker::fake::{FakeLauncher, GuestMode, HostNetworkExecutor};

    fn launcher(mode: GuestMode) -> Arc<FakeLauncher> {
        Arc::new(FakeLauncher::new(
            mode,
            Arc::new(HostNetworkExecutor(ProcessExecutor::new(
                ExecOptions::default(),
            ))),
        ))
    }

    async fn wait_until(mut done: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !done() {
            assert!(Instant::now() < deadline, "condition never held");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn the_pool_fills_to_its_target() {
        let fake = launcher(GuestMode::Serve);
        let pool = WarmPool::start(fake.clone(), 3, 52, Duration::from_secs(2));
        wait_until(|| pool.ready_len() == 3).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(fake.launched(), 3, "the pool must not overfill");
        pool.shutdown().await;
    }

    #[tokio::test]
    async fn each_vm_is_handed_out_once_and_the_pool_refills() {
        let fake = launcher(GuestMode::Serve);
        let pool = WarmPool::start(fake.clone(), 2, 52, Duration::from_secs(2));
        wait_until(|| pool.ready_len() == 2).await;
        let mut ids = BTreeSet::new();
        for _ in 0..4 {
            wait_until(|| pool.ready_len() > 0).await;
            let vm = pool.take().unwrap();
            assert!(ids.insert(vm.id().to_owned()), "a VM came out twice");
            vm.shutdown().await;
        }
        wait_until(|| pool.ready_len() == 2).await;
        pool.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_tears_down_every_ready_vm() {
        let fake = launcher(GuestMode::Serve);
        let pool = WarmPool::start(fake.clone(), 3, 52, Duration::from_secs(2));
        wait_until(|| pool.ready_len() == 3).await;
        pool.shutdown().await;
        assert_eq!(pool.ready_len(), 0);
        assert_eq!(fake.live(), 0);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(fake.launched(), 3, "a stopped pool must not refill");
    }

    #[tokio::test]
    async fn a_vm_that_never_answers_the_ping_is_not_ready() {
        let fake = launcher(GuestMode::NeverListen);
        let pool = WarmPool::start(fake.clone(), 1, 52, Duration::from_millis(100));
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(pool.ready_len(), 0);
        assert!(fake.live() <= 1, "failed VMs must be torn down");
        pool.shutdown().await;
        assert_eq!(fake.live(), 0);
    }

    #[tokio::test]
    async fn a_guest_that_hangs_after_the_handshake_does_not_stop_the_refill() {
        let fake = launcher(GuestMode::Hang);
        let pool = WarmPool::start(fake.clone(), 1, 52, Duration::from_millis(100));
        tokio::time::sleep(Duration::from_millis(800)).await;
        assert_eq!(pool.ready_len(), 0);
        assert!(
            fake.launched() >= 2,
            "the ping must time out, so the pool tries again"
        );
        pool.shutdown().await;
        assert_eq!(fake.live(), 0);
    }

    #[tokio::test]
    async fn shutdown_tears_down_a_vm_that_is_still_booting() {
        let fake = launcher(GuestMode::Hang);
        let pool = WarmPool::start(fake.clone(), 1, 52, Duration::from_secs(30));
        // The refill task now holds a microVM and waits for its ping.
        wait_until(|| fake.launched() == 1 && fake.live() == 1).await;
        pool.shutdown().await;
        assert_eq!(fake.live(), 0, "the booting microVM outlived the shutdown");
    }

    #[tokio::test]
    async fn failed_launches_back_off() {
        let fake = launcher(GuestMode::Serve);
        fake.failures_left.store(usize::MAX, Ordering::SeqCst);
        let pool = WarmPool::start(fake.clone(), 1, 52, Duration::from_secs(1));
        tokio::time::sleep(Duration::from_millis(400)).await;
        let tries = usize::MAX - fake.failures_left.load(Ordering::SeqCst);
        assert!(tries <= 5, "{tries} tries in 400 ms is a hot loop");
        assert!(tries >= 2, "the pool must keep trying");
        pool.shutdown().await;
    }

    #[tokio::test]
    async fn dropping_the_pool_tears_down_its_vms() {
        let fake = launcher(GuestMode::Serve);
        let pool = WarmPool::start(fake.clone(), 2, 52, Duration::from_secs(2));
        wait_until(|| pool.ready_len() == 2).await;
        drop(pool);
        wait_until(|| fake.live() == 0).await;
    }
}
