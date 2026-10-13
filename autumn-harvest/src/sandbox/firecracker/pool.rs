//! A warm pool of booted, single-use microVMs.
//!
//! A boot takes about 125 ms before the guest agent listens. The pool hides
//! it: a background task keeps a number of microVMs booted and checked with a
//! ping. A run takes one microVM, and the microVM never comes back. The task
//! then boots a new one. A failed boot waits before the next try, with a
//! delay that doubles up to [`MAX_REFILL_BACKOFF`].

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use super::launcher::{LaunchedVm, VmLauncher};

/// The first wait after a failed boot.
pub const FIRST_REFILL_BACKOFF: Duration = Duration::from_millis(50);

/// The longest wait after failed boots.
pub const MAX_REFILL_BACKOFF: Duration = Duration::from_secs(5);

/// A pool of booted microVMs.
pub struct WarmPool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    ready: std::sync::Mutex<VecDeque<Box<dyn LaunchedVm>>>,
}

impl std::fmt::Debug for WarmPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WarmPool").finish_non_exhaustive()
    }
}

impl WarmPool {
    /// Start a pool that keeps `target` microVMs from `launcher` ready. Each
    /// microVM must answer a ping on `port` within `boot_timeout` to count
    /// as ready. Call it inside a Tokio runtime.
    #[must_use]
    pub fn start(
        launcher: Arc<dyn VmLauncher>,
        target: usize,
        port: u32,
        boot_timeout: Duration,
    ) -> Self {
        let _ = (launcher, target, port, boot_timeout);
        todo!("WarmPool::start")
    }

    /// Take a ready microVM, if one exists. The pool then boots a new one.
    #[must_use]
    pub fn take(&self) -> Option<Box<dyn LaunchedVm>> {
        let _ = &self.inner;
        todo!("WarmPool::take")
    }

    /// The number of ready microVMs.
    #[must_use]
    pub fn ready_len(&self) -> usize {
        todo!("WarmPool::ready_len")
    }

    /// Stop refilling and tear down each ready microVM.
    pub async fn shutdown(&self) {
        todo!("WarmPool::shutdown")
    }
}

impl Drop for WarmPool {
    fn drop(&mut self) {
        let _ = &self.inner;
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
    use crate::sandbox::firecracker::fake::{FakeLauncher, GuestMode};

    fn launcher(mode: GuestMode) -> Arc<FakeLauncher> {
        Arc::new(FakeLauncher::new(
            mode,
            Arc::new(ProcessExecutor::new(ExecOptions::default())),
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
