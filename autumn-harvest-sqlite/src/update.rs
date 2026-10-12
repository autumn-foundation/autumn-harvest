//! Update helpers for the single-writer runtime (issue #2025).
//!
//! The runtime runs an update handler to completion in one call. These helpers
//! poll the handler and build the result event.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use autumn_harvest::{UpdateId, WorkflowEvent};
use serde_json::Value;

/// The poll budget for a handler that wakes itself, such as a `yield_now`
/// loop. A handler that uses all of it is blocked.
const MAX_HANDLER_POLLS: usize = 10_000;

/// How an update handler ended.
#[derive(Debug)]
pub enum HandlerRun {
    /// The handler returned this result. A panic is an `Err`.
    Finished(Result<Value, String>),
    /// The handler waits for a wake that this call cannot give.
    Blocked,
}

/// Record whether the handler woke itself during a poll.
struct WakeFlag(AtomicBool);

impl Wake for WakeFlag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::Release);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::Release);
    }
}

/// Poll `handler` until it returns or stops making progress.
///
/// A poll that returns `Pending` with no wake means the handler waits for a
/// durable command. No later poll in this call can resolve it, so the run is
/// `Blocked`. A panic becomes an `Err`, as in `WorkflowTestEnv::update`.
pub fn run_handler(handler: impl Future<Output = Result<Value, String>>) -> HandlerRun {
    let flag = Arc::new(WakeFlag(AtomicBool::new(false)));
    let waker = Waker::from(Arc::clone(&flag));
    let mut cx = Context::from_waker(&waker);
    let mut handler = std::pin::pin!(handler);
    for _ in 0..MAX_HANDLER_POLLS {
        flag.0.store(false, Ordering::Release);
        match std::panic::catch_unwind(AssertUnwindSafe(|| handler.as_mut().poll(&mut cx))) {
            Ok(Poll::Ready(result)) => return HandlerRun::Finished(result),
            Ok(Poll::Pending) if flag.0.load(Ordering::Acquire) => {}
            Ok(Poll::Pending) => return HandlerRun::Blocked,
            Err(panic) => {
                return HandlerRun::Finished(Err(format!(
                    "update handler panicked: {}",
                    autumn_harvest::error::panic_message(panic)
                )));
            }
        }
    }
    HandlerRun::Blocked
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

    #[test]
    fn a_ready_handler_finishes() {
        let run = run_handler(async { Ok(json!(1)) });
        assert!(matches!(run, HandlerRun::Finished(Ok(ref v)) if v == &json!(1)));
    }

    #[test]
    fn a_pending_handler_with_no_wake_is_blocked() {
        let run = run_handler(std::future::pending::<Result<Value, String>>());
        assert!(matches!(run, HandlerRun::Blocked));
    }

    #[test]
    fn a_self_waking_handler_finishes() {
        let mut yielded = false;
        let run = run_handler(std::future::poll_fn(move |cx| {
            if yielded {
                Poll::Ready(Ok(json!("done")))
            } else {
                yielded = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }));
        assert!(matches!(run, HandlerRun::Finished(Ok(ref v)) if v == &json!("done")));
    }

    #[test]
    fn a_handler_that_always_wakes_itself_is_blocked() {
        let run = run_handler(std::future::poll_fn(|cx| {
            cx.waker().wake_by_ref();
            Poll::<Result<Value, String>>::Pending
        }));
        assert!(matches!(run, HandlerRun::Blocked));
    }

    #[test]
    fn a_panic_becomes_an_error() {
        let run = run_handler(std::future::poll_fn(|_| -> Poll<Result<Value, String>> {
            panic!("kaboom")
        }));
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
