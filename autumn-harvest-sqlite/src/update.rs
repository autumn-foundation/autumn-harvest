//! Update helpers for the single-writer runtime (issue #2025).
//!
//! The runtime runs an update handler to completion in one call. These helpers
//! await the handler under a deadline and build the result event.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};
use std::time::{Duration, Instant};

use autumn_harvest::{UpdateId, WorkflowEvent};
use serde_json::Value;

/// How an update handler ended.
#[derive(Debug)]
pub enum HandlerRun {
    /// The handler returned this result. A panic is an `Err`.
    Finished(Result<Value, String>),
    /// The handler did not finish before the deadline.
    TimedOut,
}

/// A thread that wakes the task at the deadline.
///
/// The handler can wait on a wake that never comes, such as a durable
/// command. The alarm wakes the task, so the deadline check runs on any async
/// runtime. Dropping the alarm stops the thread at once.
struct Alarm {
    waker: Arc<Mutex<Waker>>,
    _stop: Sender<()>,
}

impl Alarm {
    /// Start the alarm. It returns `None` if the OS cannot start a thread.
    fn start(deadline: Instant, waker: &Waker) -> Option<Self> {
        let shared = Arc::new(Mutex::new(waker.clone()));
        let (stop, stopped) = mpsc::channel::<()>();
        let alarm_waker = Arc::clone(&shared);
        std::thread::Builder::new()
            .name("harvest-update-alarm".to_string())
            .spawn(move || {
                let wait = deadline.saturating_duration_since(Instant::now());
                if stopped.recv_timeout(wait) == Err(RecvTimeoutError::Timeout) {
                    // A poisoned lock still holds a usable waker.
                    let waker = alarm_waker
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    waker.wake_by_ref();
                }
            })
            .ok()?;
        Some(Self {
            waker: shared,
            _stop: stop,
        })
    }

    /// Wake the latest task waker at the deadline.
    fn track(&self, waker: &Waker) {
        let mut current = self
            .waker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !current.will_wake(waker) {
            current.clone_from(waker);
        }
    }
}

/// Await `handler` until it returns or `timeout` ends.
///
/// The handler can await any future, such as a timer or I/O. A handler that
/// awaits a durable command never finishes, so it times out. A panic becomes
/// an `Err`, as in `WorkflowTestEnv::update`.
pub async fn run_handler(
    handler: impl Future<Output = Result<Value, String>>,
    timeout: Duration,
) -> HandlerRun {
    let deadline = Instant::now() + timeout;
    let mut handler = std::pin::pin!(handler);
    let mut alarm: Option<Alarm> = None;
    std::future::poll_fn(|cx| {
        if Instant::now() >= deadline {
            return Poll::Ready(HandlerRun::TimedOut);
        }
        match std::panic::catch_unwind(AssertUnwindSafe(|| handler.as_mut().poll(cx))) {
            Ok(Poll::Ready(result)) => Poll::Ready(HandlerRun::Finished(result)),
            Ok(Poll::Pending) => {
                match &alarm {
                    Some(alarm) => alarm.track(cx.waker()),
                    None => match Alarm::start(deadline, cx.waker()) {
                        Some(started) => alarm = Some(started),
                        // With no alarm, nothing bounds the wait. Stop now and
                        // write nothing.
                        None => return Poll::Ready(HandlerRun::TimedOut),
                    },
                }
                Poll::Pending
            }
            Err(panic) => Poll::Ready(HandlerRun::Finished(Err(format!(
                "update handler panicked: {}",
                autumn_harvest::error::panic_message(panic)
            )))),
        }
    })
    .await
}

/// The event that a `RecordUpdateResult` command records.
///
/// The Postgres worker builds the same events, so the histories match.
pub fn result_event(update_id: UpdateId, result: &Result<Value, String>) -> WorkflowEvent {
    match result {
        Ok(output) => WorkflowEvent::UpdateCompleted {
            update_id,
            output: output.clone(),
        },
        Err(error) => WorkflowEvent::UpdateFailed {
            update_id,
            error: error.clone(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const SHORT: Duration = Duration::from_millis(100);

    /// A future that a separate thread wakes after `delay`.
    fn woken_later(delay: Duration) -> impl Future<Output = Result<Value, String>> {
        let mut started = false;
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        std::future::poll_fn(move |cx| {
            if done.load(std::sync::atomic::Ordering::Acquire) {
                return Poll::Ready(Ok(json!("woken")));
            }
            if !started {
                started = true;
                let done = Arc::clone(&done);
                let waker = cx.waker().clone();
                std::thread::spawn(move || {
                    std::thread::sleep(delay);
                    done.store(true, std::sync::atomic::Ordering::Release);
                    waker.wake();
                });
            }
            Poll::Pending
        })
    }

    #[tokio::test]
    async fn a_ready_handler_finishes() {
        let run = run_handler(async { Ok(json!(1)) }, SHORT).await;
        assert!(matches!(run, HandlerRun::Finished(Ok(ref v)) if v == &json!(1)));
    }

    #[tokio::test]
    async fn an_asynchronous_wake_finishes() {
        let run = run_handler(
            woken_later(Duration::from_millis(20)),
            Duration::from_secs(5),
        )
        .await;
        assert!(
            matches!(run, HandlerRun::Finished(Ok(ref v)) if v == &json!("woken")),
            "{run:?}"
        );
    }

    #[tokio::test]
    async fn a_handler_that_never_wakes_times_out() {
        let started = Instant::now();
        let run = run_handler(std::future::pending::<Result<Value, String>>(), SHORT).await;
        assert!(matches!(run, HandlerRun::TimedOut));
        assert!(started.elapsed() >= SHORT);
    }

    #[tokio::test]
    async fn a_self_waking_handler_finishes() {
        let mut yielded = false;
        let run = run_handler(
            std::future::poll_fn(move |cx| {
                if yielded {
                    Poll::Ready(Ok(json!("done")))
                } else {
                    yielded = true;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }),
            SHORT,
        )
        .await;
        assert!(matches!(run, HandlerRun::Finished(Ok(ref v)) if v == &json!("done")));
    }

    #[tokio::test]
    async fn a_handler_that_always_wakes_itself_times_out() {
        let run = run_handler(
            std::future::poll_fn(|cx| {
                cx.waker().wake_by_ref();
                Poll::<Result<Value, String>>::Pending
            }),
            SHORT,
        )
        .await;
        assert!(matches!(run, HandlerRun::TimedOut));
    }

    #[tokio::test]
    async fn a_panic_becomes_an_error() {
        let run = run_handler(
            std::future::poll_fn(|_| -> Poll<Result<Value, String>> { panic!("kaboom") }),
            SHORT,
        )
        .await;
        assert!(
            matches!(run, HandlerRun::Finished(Err(ref e)) if e.contains("kaboom")),
            "{run:?}"
        );
    }

    #[test]
    fn result_event_matches_the_result() {
        let id = UpdateId::new();
        assert!(matches!(
            result_event(id, &Ok(json!(2))),
            WorkflowEvent::UpdateCompleted { update_id, ref output } if update_id == id && output == &json!(2)
        ));
        assert!(matches!(
            result_event(id, &Err("no".to_string())),
            WorkflowEvent::UpdateFailed { update_id, ref error } if update_id == id && error == "no"
        ));
    }
}
