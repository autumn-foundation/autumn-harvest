//! Query registry and dispatch mechanisms.
//!
//! Queries allow external systems to interrogate the internal state of a running workflow.
//! Workflows register query handlers (which are synchronous functions returning a JSON
//! value) during their execution, and clients can trigger them via the execution engine.
use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;

use crate::error::{HarvestError, HarvestResult};

/// A thread-safe, sync function that returns a JSON-serializable query result.
pub type QueryHandler = Arc<dyn Fn() -> Value + Send + Sync>;

/// Holds all registered query handlers for a running workflow.
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
    /// # Examples
    ///
    /// ```rust
    /// use autumn_harvest::query::QueryRegistry;
    /// use std::sync::Arc;
    /// use serde_json::json;
    ///
    /// let mut registry = QueryRegistry::new();
    /// registry.register("health", Arc::new(|| json!({ "status": "ok" })));
    /// ```
    pub fn register(&mut self, name: &str, handler: QueryHandler) {
        self.handlers.insert(name.to_string(), handler);
    }

    /// Execute a registered query handler.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use autumn_harvest::query::QueryRegistry;
    /// use std::sync::Arc;
    /// use serde_json::json;
    ///
    /// let mut registry = QueryRegistry::new();
    /// registry.register("health", Arc::new(|| json!({ "status": "ok" })));
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
    /// Returns [`HarvestError::NotFound`] if `name` is not registered.
    pub fn execute(&self, name: &str) -> HarvestResult<Value> {
        self.handlers
            .get(name)
            .map(|handler| handler())
            .ok_or_else(|| HarvestError::NotFound(format!("query handler '{name}'")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn executes_registered_query() {
        let mut reg = QueryRegistry::new();
        reg.register("status", Arc::new(|| serde_json::json!({"ok": true})));

        let result = reg.execute("status").expect("query must be found");
        assert_eq!(result, serde_json::json!({"ok": true}));
    }
}
