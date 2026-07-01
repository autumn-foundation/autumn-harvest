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

    /// Register a handler under `name`.
    ///
    /// If `name` is already registered, this is a no-op (first registration wins).
    pub fn register(&mut self, name: &str, handler: BoxSignalHandler) {
        self.handlers.entry(name.to_string()).or_insert(handler);
    }

    /// Returns `true` if a handler is registered under `name`.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.handlers.contains_key(name)
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

/// Invoke a signal handler, guarding against a panicking handler crashing the
/// workflow task (mirrors `QueryRegistry::execute_with_args`'s `catch_unwind`).
///
/// Signals are fire-and-forget: a panicking handler is logged and the
/// delivery is dropped, rather than propagating a panic up through the
/// workflow task.
pub fn invoke_signal_handler(handler: &BoxSignalHandler, signal_name: &str, payload: Value) {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler(payload)));
    if let Err(e) = result {
        let msg = e
            .downcast_ref::<&str>()
            .map(|s| (*s).to_string())
            .or_else(|| e.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "unknown panic".to_string());
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
        assert!(!registry.contains("cancel"));

        let calls: Arc<std::sync::Mutex<Vec<Value>>> = Arc::new(std::sync::Mutex::new(vec![]));
        let calls_clone = calls.clone();
        registry.register(
            "cancel",
            Arc::new(move |payload| calls_clone.lock().unwrap().push(payload)),
        );

        assert!(registry.contains("cancel"));
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
        registry.register(
            "cancel",
            Arc::new(move |_| calls_second.lock().unwrap().push("second")),
        );

        let handler = registry.get("cancel").expect("handler must be present");
        handler(Value::Null);
        assert_eq!(
            *calls.lock().unwrap(),
            vec!["first"],
            "first registration should win"
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
