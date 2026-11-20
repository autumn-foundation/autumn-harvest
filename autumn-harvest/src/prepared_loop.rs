//! A background loop that is ready to run, but not yet spawned.
//!
//! A runtime such as the scheduler builds its loop and its monitor first.
//! The caller then picks the owner of the loop:
//!
//! - [`PreparedLoop::spawn`] puts the loop on a new Tokio task.
//! - [`PreparedLoop::run`] runs the loop in the current task until a host
//!   token is cancelled. An Autumn background task uses this form, so the
//!   Autumn drain owns the stop.
//!
//! The loop keeps its own stop token. `run` cancels it when the host token
//! is cancelled, then waits for the loop to return. So the loop stops at its
//! own safe points in both forms.

use std::future::Future;
use std::pin::Pin;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// The future of one loop.
type LoopFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// A loop with its own stop token.
#[must_use = "a prepared loop does nothing until it runs"]
pub struct PreparedLoop {
    stop: CancellationToken,
    task: LoopFuture,
}

impl std::fmt::Debug for PreparedLoop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedLoop")
            .field("stopped", &self.stop.is_cancelled())
            .finish_non_exhaustive()
    }
}

impl PreparedLoop {
    /// A loop that stops when `stop` is cancelled.
    pub(crate) fn new(
        stop: CancellationToken,
        task: impl Future<Output = ()> + Send + 'static,
    ) -> Self {
        Self {
            stop,
            task: Box::pin(task),
        }
    }

    /// Run the loop in the current task until it returns.
    ///
    /// When `host` is cancelled, the loop gets its own stop signal. This call
    /// then waits for the loop to return.
    pub async fn run(self, host: CancellationToken) {
        let Self { stop, mut task } = self;
        tokio::select! {
            // A loop that ends by itself ends the call.
            biased;
            () = &mut task => {}
            () = host.cancelled() => {
                stop.cancel();
                task.await;
            }
        }
    }

    /// Spawn the loop on a new Tokio task.
    ///
    /// Returns the stop token of the loop and the handle of the task.
    #[must_use]
    pub fn spawn(self) -> (CancellationToken, JoinHandle<()>) {
        (self.stop, tokio::spawn(self.task))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use super::*;

    /// A loop that runs until its own token is cancelled, then sets `done`.
    fn polite(done: &Arc<AtomicBool>) -> PreparedLoop {
        let stop = CancellationToken::new();
        let inner = stop.clone();
        let done = Arc::clone(done);
        PreparedLoop::new(stop, async move {
            inner.cancelled().await;
            done.store(true, Ordering::SeqCst);
        })
    }

    #[tokio::test]
    async fn run_stops_the_loop_through_its_own_token() {
        let done = Arc::new(AtomicBool::new(false));
        let host = CancellationToken::new();
        let run = tokio::spawn(polite(&done).run(host.clone()));
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!done.load(Ordering::SeqCst), "the loop ran past no stop");
        host.cancel();
        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("run returns after the host stop")
            .expect("run does not panic");
        assert!(done.load(Ordering::SeqCst), "the loop finished its stop");
    }

    #[tokio::test]
    async fn run_returns_when_the_loop_returns() {
        let host = CancellationToken::new();
        let quick = PreparedLoop::new(CancellationToken::new(), async {});
        tokio::time::timeout(Duration::from_secs(5), quick.run(host.clone()))
            .await
            .expect("a finished loop ends the run");
        assert!(!host.is_cancelled());
    }

    #[tokio::test]
    async fn spawn_gives_the_stop_token_of_the_loop() {
        let done = Arc::new(AtomicBool::new(false));
        let (stop, handle) = polite(&done).spawn();
        stop.cancel();
        handle.await.expect("the loop does not panic");
        assert!(done.load(Ordering::SeqCst));
    }
}
