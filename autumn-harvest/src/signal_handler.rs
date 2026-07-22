//! Signal handler registry for push-based reactive signal handling (issue #546).
//!
//! Mirrors [`UpdateRegistry`](crate::update::UpdateRegistry) /
//! [`QueryRegistry`](crate::query::QueryRegistry) but for the signal
//! primitive: a `SignalHandlerRegistry` stores synchronous, type-erased
//! handlers keyed by signal name. Registration is idempotent — the first
//! registration for a given name wins, so it is safe to call at the top of
//! every `#[workflow]` function on each replay cycle.
//!
//! Handlers are **fire-and-forget** by contract (issue #546 explicitly puts
//! validators/rejection out of scope — that's the `update` primitive's job)
//! and synchronous: they run inline on the workflow's replay/live cycle, so a
//! mutation of author-captured state (e.g. `Arc<Mutex<T>>`) is visible to the
//! rest of the workflow body within the same cycle. There is no suspension
//! shape to reason about and no completion event to persist.
//!
//! Because there is no completion event, dispatch is **per-cycle, not
//! once-ever**: a fresh `SignalHandlerRegistry` is built for every workflow
//! replay/live cycle, so the same recorded `SignalReceived` event is
//! redelivered on every subsequent cycle for the life of the execution, not
//! just the first time it is seen. That is correct for reconstructing
//! in-memory state (the captured `Arc<Mutex<T>>` is rebuilt fresh each cycle
//! too), but not for a non-idempotent side effect performed directly inside a
//! handler — see `WorkflowContext::register_signal_handler_raw`'s docs for
//! the full guidance.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;

/// Type-erased signal handler: receives the JSON signal payload, no return value.
pub type BoxSignalHandler = Arc<dyn Fn(Value) + Send + Sync>;

/// In-memory registry of push-based signal handlers for a running workflow execution.
///
/// Stored on [`WorkflowContext`](crate::context::WorkflowContext) behind a
/// `Mutex`. Registration is **idempotent** — the first registration for a
/// given name wins, matching the semantics of
/// [`UpdateRegistry`](crate::update::UpdateRegistry) and
/// [`QueryRegistry`](crate::query::QueryRegistry).
#[derive(Default)]
pub struct SignalHandlerRegistry {
    handlers: HashMap<String, BoxSignalHandler>,
}

impl SignalHandlerRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a handler under `name` and return the handler that actually
    /// won registration.
    ///
    /// If `name` is already registered, this is a no-op (first registration
    /// wins) and the *existing* handler is returned rather than `handler`, so
    /// a caller that immediately needs to dispatch through the resolved
    /// handler (as `WorkflowContext::register_and_dispatch_signal_handler`
    /// does) never has to re-acquire the registry lock and look it back up.
    pub fn register(&mut self, name: &str, handler: BoxSignalHandler) -> BoxSignalHandler {
        self.handlers
            .entry(name.to_string())
            .or_insert(handler)
            .clone()
    }

    /// Returns a cloned handler for `name`, if registered.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<BoxSignalHandler> {
        self.handlers.get(name).cloned()
    }

    /// Returns the sorted names of all registered signal handlers (issue #546 AC:
    /// parity with `list_query_names`).
    #[must_use]
    pub fn list_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.handlers.keys().cloned().collect();
        names.sort();
        names
    }
}

/// Invoke a signal handler, catching a panic at this call boundary instead of
/// letting it propagate out (mirrors `QueryRegistry::execute_with_args`'s
/// `catch_unwind`).
///
/// Signals are fire-and-forget: a panicking handler's panic is logged here
/// and this one delivery is dropped, rather than propagating past this call.
/// This does **not** protect against a handler that panics while holding a
/// lock on its own captured `std::sync::Mutex`: that mutex still poisons per
/// ordinary Rust semantics regardless of this `catch_unwind`, and a *later*
/// `.lock().unwrap()` on the same mutex elsewhere in the workflow body will
/// still panic (uncaught) when it runs. Handler authors sharing a mutex with
/// other workflow code should keep critical sections trivially short/
/// infallible, or recover with
/// `.lock().unwrap_or_else(std::sync::PoisonError::into_inner)`.
pub fn invoke_signal_handler(handler: &BoxSignalHandler, signal_name: &str, payload: Value) {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler(payload)));
    if let Err(e) = result {
        let msg = crate::error::panic_message(e);
        tracing::warn!(
            signal = signal_name,
            panic = %msg,
            "signal handler panicked; delivery dropped"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_get_round_trips() {
        let mut registry = SignalHandlerRegistry::new();
        assert!(registry.get("cancel").is_none());

        let calls: Arc<std::sync::Mutex<Vec<Value>>> = Arc::new(std::sync::Mutex::new(vec![]));
        let calls_clone = calls.clone();
        registry.register(
            "cancel",
            Arc::new(move |payload| calls_clone.lock().unwrap().push(payload)),
        );

        let handler = registry.get("cancel").expect("handler must be present");
        handler(serde_json::json!({"reason": "user_requested"}));
        assert_eq!(
            *calls.lock().unwrap(),
            vec![serde_json::json!({"reason": "user_requested"})]
        );
    }

    #[test]
    fn registration_is_idempotent_first_wins() {
        let mut registry = SignalHandlerRegistry::new();
        let calls: Arc<std::sync::Mutex<Vec<&'static str>>> =
            Arc::new(std::sync::Mutex::new(vec![]));

        let calls_first = calls.clone();
        registry.register(
            "cancel",
            Arc::new(move |_| calls_first.lock().unwrap().push("first")),
        );
        let calls_second = calls.clone();
        let resolved = registry.register(
            "cancel",
            Arc::new(move |_| calls_second.lock().unwrap().push("second")),
        );

        // register() itself must return the winning (first) handler, so a
        // caller never needs a second lookup to dispatch through it.
        resolved(Value::Null);
        assert_eq!(
            *calls.lock().unwrap(),
            vec!["first"],
            "register() should return the first-registered handler"
        );

        let handler = registry.get("cancel").expect("handler must be present");
        handler(Value::Null);
        assert_eq!(
            *calls.lock().unwrap(),
            vec!["first", "first"],
            "first registration should win for get() too"
        );
    }

    #[test]
    fn get_returns_none_for_unregistered() {
        let registry = SignalHandlerRegistry::new();
        assert!(registry.get("unknown").is_none());
    }

    #[test]
    fn list_names_returns_sorted_names() {
        let mut registry = SignalHandlerRegistry::new();
        registry.register("zeta", Arc::new(|_| {}));
        registry.register("alpha", Arc::new(|_| {}));
        registry.register("mid", Arc::new(|_| {}));

        assert_eq!(
            registry.list_names(),
            vec!["alpha".to_string(), "mid".to_string(), "zeta".to_string(),]
        );
    }

    #[test]
    fn invoke_signal_handler_survives_panic() {
        let handler: BoxSignalHandler = Arc::new(|_| panic!("boom"));
        // Must not propagate the panic to the caller.
        invoke_signal_handler(&handler, "cancel", Value::Null);
    }

    #[test]
    fn invoke_signal_handler_calls_through_on_success() {
        let calls: Arc<std::sync::Mutex<Vec<Value>>> = Arc::new(std::sync::Mutex::new(vec![]));
        let calls_clone = calls.clone();
        let handler: BoxSignalHandler =
            Arc::new(move |payload| calls_clone.lock().unwrap().push(payload));
        invoke_signal_handler(&handler, "cancel", serde_json::json!(42));
        assert_eq!(*calls.lock().unwrap(), vec![serde_json::json!(42)]);
    }
}
