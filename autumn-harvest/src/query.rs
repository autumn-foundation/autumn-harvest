//! Query registry and dispatch for read-only workflow state inspection (issue #234).
//!
//! Queries allow external systems to interrogate the internal state of a running
//! workflow without writing any event to `harvest_events`. Handlers are pure
//! synchronous functions over user-captured state registered during each
//! workflow execution cycle and looked up by name at query time.
//!
//! Key invariants:
//! - **No history footprint.** Query execution never appends to `harvest_events`.
//! - **Read-only.** Handlers receive an immutable snapshot; they cannot call any
//!   `WorkflowCommand`-emitting method on `WorkflowContext`.
//! - **No deadlock.** The registry lock is released before calling the handler.
use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;

use crate::error::{HarvestError, HarvestResult};

/// A type-erased query handler: receives JSON args, returns JSON result or error.
///
/// The `Arc` wrapper makes it cheap to clone out of the registry before invoking,
/// which is the key to avoiding deadlocks when a handler calls back into the
/// context (e.g., the deadlock regression test in `tests/query_deadlock.rs`).
pub type QueryHandler = Arc<dyn Fn(Value) -> Result<Value, String> + Send + Sync>;

/// Holds all registered query handlers for a running workflow execution.
///
/// Registration is **idempotent** — the first registration for a given name
/// wins, matching the semantics of `UpdateRegistry`.
#[derive(Default)]
pub struct QueryRegistry {
    handlers: HashMap<String, QueryHandler>,
}

impl QueryRegistry {
    /// Creates a new, empty query registry.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use autumn_harvest::query::QueryRegistry;
    ///
    /// let registry = QueryRegistry::new();
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a query handler under the given name.
    ///
    /// If `name` is already registered, this is a no-op (first registration wins).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use autumn_harvest::query::QueryRegistry;
    /// use std::sync::Arc;
    /// use serde_json::json;
    ///
    /// let mut registry = QueryRegistry::new();
    /// registry.register("health", Arc::new(|_args| Ok(json!({ "status": "ok" }))));
    /// ```
    pub fn register(&mut self, name: &str, handler: QueryHandler) {
        self.handlers.entry(name.to_string()).or_insert(handler);
    }

    /// Gets a registered query handler without executing it.
    ///
    /// Returns a cloned `Arc` so the lock can be released before the handler
    /// is invoked, preventing re-entrant deadlocks.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<QueryHandler> {
        self.handlers.get(name).cloned()
    }

    /// Returns the names of all registered query handlers, in unspecified order.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use autumn_harvest::query::QueryRegistry;
    /// use std::sync::Arc;
    /// use serde_json::json;
    ///
    /// let mut registry = QueryRegistry::new();
    /// registry.register("status", Arc::new(|_| Ok(json!("ok"))));
    /// registry.register("progress", Arc::new(|_| Ok(json!(0))));
    ///
    /// let names = registry.list_names();
    /// assert_eq!(names.len(), 2);
    /// assert!(names.contains(&"status".to_string()));
    /// ```
    #[must_use]
    pub fn list_names(&self) -> Vec<String> {
        self.handlers.keys().cloned().collect()
    }

    /// Execute a registered query handler with the given `args`.
    ///
    /// The registry lock is **not** held during handler invocation, so handlers
    /// that call back into a `WorkflowContext` (which holds its own lock around
    /// the registry) will not deadlock.
    ///
    /// # Errors
    ///
    /// - Returns [`HarvestError::QueryHandlerNotFound`] if `name` is not registered.
    /// - Returns [`HarvestError::QueryHandlerPanicked`] if the handler returns `Err`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use autumn_harvest::query::QueryRegistry;
    /// use std::sync::Arc;
    /// use serde_json::json;
    ///
    /// let mut registry = QueryRegistry::new();
    /// registry.register("echo", Arc::new(|args| Ok(args)));
    ///
    /// let result = registry.execute_with_args("echo", json!("hello")).unwrap();
    /// assert_eq!(result, json!("hello"));
    ///
    /// let missing = registry.execute_with_args("unknown", json!(null));
    /// assert!(missing.is_err());
    /// ```
    pub fn execute_with_args(&self, name: &str, args: Value) -> HarvestResult<Value> {
        let handler = self
            .handlers
            .get(name)
            .cloned()
            .ok_or_else(|| HarvestError::QueryHandlerNotFound(name.to_string()))?;

        // Lock released; call handler outside the borrow.
        // Wrap in catch_unwind so a panicking handler doesn't crash the worker.
        // Panics → QueryHandlerPanicked (503); intentional Err returns → QueryHandlerFailed (400).
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler(args)))
            .map_err(|e| {
                let msg = e
                    .downcast_ref::<&str>()
                    .map(|s| (*s).to_string())
                    .or_else(|| e.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "unknown panic".to_string());
                HarvestError::QueryHandlerPanicked(msg)
            })?
            .map_err(HarvestError::QueryHandlerFailed)
    }

    /// Execute a registered query handler with no arguments (`Value::Null`).
    ///
    /// Convenience alias for `execute_with_args(name, Value::Null)`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use autumn_harvest::query::QueryRegistry;
    /// use std::sync::Arc;
    /// use serde_json::json;
    ///
    /// let mut registry = QueryRegistry::new();
    /// registry.register("health", Arc::new(|_args| Ok(json!({ "status": "ok" }))));
    ///
    /// let result = registry.execute("health").unwrap();
    /// assert_eq!(result, json!({ "status": "ok" }));
    ///
    /// let missing = registry.execute("unknown");
    /// assert!(missing.is_err());
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::QueryHandlerNotFound`] if `name` is not registered.
    pub fn execute(&self, name: &str) -> HarvestResult<Value> {
        self.execute_with_args(name, Value::Null)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn executes_registered_query() {
        let mut reg = QueryRegistry::new();
        reg.register("status", Arc::new(|_| Ok(serde_json::json!({"ok": true}))));

        let result = reg.execute("status").expect("query must be found");
        assert_eq!(result, serde_json::json!({"ok": true}));
    }

    #[test]
    fn execute_with_args_passes_args_to_handler() {
        let mut reg = QueryRegistry::new();
        reg.register("echo", Arc::new(|args: Value| Ok(args)));

        let result = reg
            .execute_with_args("echo", serde_json::json!(42))
            .expect("echo must succeed");
        assert_eq!(result, serde_json::json!(42));
    }

    #[test]
    fn execute_returns_not_found_for_unregistered() {
        let reg = QueryRegistry::new();
        let err = reg.execute("missing").unwrap_err();
        assert!(matches!(err, HarvestError::QueryHandlerNotFound(_)));
    }

    #[test]
    fn list_names_returns_registered_names() {
        let mut reg = QueryRegistry::new();
        reg.register("a", Arc::new(|_| Ok(Value::Null)));
        reg.register("b", Arc::new(|_| Ok(Value::Null)));
        let names = reg.list_names();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"a".to_string()));
        assert!(names.contains(&"b".to_string()));
    }

    #[test]
    fn registration_is_idempotent() {
        let mut reg = QueryRegistry::new();
        reg.register("k", Arc::new(|_| Ok(serde_json::json!("first"))));
        reg.register("k", Arc::new(|_| Ok(serde_json::json!("second"))));
        let result = reg.execute("k").expect("must succeed");
        assert_eq!(
            result,
            serde_json::json!("first"),
            "first registration wins"
        );
    }
}
