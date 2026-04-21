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
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a query handler under the given name.
    pub fn register(&mut self, name: &str, handler: QueryHandler) {
        self.handlers.insert(name.to_string(), handler);
    }

    /// Execute a registered query handler.
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
