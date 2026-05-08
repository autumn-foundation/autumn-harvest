//! Update handler registry for the workflow Update primitive (issue #140).
//!
//! An `UpdateRegistry` stores type-erased validators and async handlers keyed
//! by name. Registration is idempotent — calling `register` twice for the same
//! name is a no-op, making it safe to call at the top of every `#[workflow]`
//! function on each replay cycle.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde_json::Value;

/// Pinned boxed async future returned by an update handler invocation.
pub type UpdateHandlerFuture = Pin<Box<dyn Future<Output = Result<Value, String>> + Send>>;

/// Type-erased async update handler: takes JSON input, returns JSON result or string error.
pub type BoxUpdateHandler = Arc<dyn Fn(Value) -> UpdateHandlerFuture + Send + Sync>;

/// Type-erased synchronous validator: takes JSON input, returns Ok or a rejection reason.
pub type BoxUpdateValidator = Arc<dyn Fn(&Value) -> Result<(), String> + Send + Sync>;

/// An entry in the update registry.
struct UpdateEntry {
    validator: Option<BoxUpdateValidator>,
    handler: BoxUpdateHandler,
}

/// In-memory registry of update handlers and their optional validators.
///
/// Stored on [`WorkflowContext`](crate::context::WorkflowContext) behind a
/// `Mutex`. Registration is idempotent — the first registration wins and
/// subsequent calls with the same `name` are ignored.
#[derive(Default)]
pub struct UpdateRegistry {
    entries: HashMap<String, UpdateEntry>,
}

impl UpdateRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Register a handler (and optional validator) under `name`.
    ///
    /// If `name` is already registered, this is a no-op (first registration wins).
    pub fn register(
        &mut self,
        name: &str,
        validator: Option<BoxUpdateValidator>,
        handler: BoxUpdateHandler,
    ) {
        // Idempotent: first registration wins.
        self.entries
            .entry(name.to_string())
            .or_insert(UpdateEntry { validator, handler });
    }

    /// Returns `true` if a handler is registered under `name`.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.entries.contains_key(name)
    }

    /// Run the validator for `name` against `input`.
    ///
    /// # Errors
    ///
    /// - Returns `Err("update handler 'name' not found")` if `name` is unknown.
    /// - Returns `Err(reason)` if the validator rejects.
    pub fn validate(&self, name: &str, input: &Value) -> Result<(), String> {
        let entry = self
            .entries
            .get(name)
            .ok_or_else(|| format!("update handler '{name}' not found"))?;

        if let Some(validator) = &entry.validator {
            validator(input)?;
        }
        Ok(())
    }

    /// Invoke the handler for `name` with `input`.
    ///
    /// Returns `None` if `name` is not registered.
    #[must_use]
    pub fn invoke(&self, name: &str, input: Value) -> Option<UpdateHandlerFuture> {
        self.entries.get(name).map(|e| (e.handler)(input))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_register_and_invoke_update_handler_successfully() {
        let mut registry = UpdateRegistry::new();
        assert!(
            !registry.contains("my_update"),
            "registry should be initially empty"
        );

        let validator = Arc::new(|val: &Value| {
            if val == &serde_json::json!("valid") {
                Ok(())
            } else {
                Err("invalid input".to_string())
            }
        });

        let handler = Arc::new(|val: Value| {
            Box::pin(async move {
                if val == serde_json::json!("valid") {
                    Ok(serde_json::json!("success"))
                } else {
                    Err("handler error".to_string())
                }
            }) as UpdateHandlerFuture
        });

        registry.register("my_update", Some(validator.clone()), handler.clone());
        assert!(
            registry.contains("my_update"),
            "registry should contain my_update after registration"
        );

        // Second registration is a no-op
        registry.register("my_update", None, handler.clone());

        // Test validate
        assert_eq!(
            registry
                .validate("unknown", &serde_json::json!("valid"))
                .unwrap_err(),
            "update handler 'unknown' not found",
            "validation should fail for unknown handler"
        );
        assert_eq!(
            registry
                .validate("my_update", &serde_json::json!("invalid"))
                .unwrap_err(),
            "invalid input",
            "validation should fail when validator rejects"
        );
        assert!(
            registry
                .validate("my_update", &serde_json::json!("valid"))
                .is_ok(),
            "validation should succeed when validator accepts"
        );

        // Test invoke
        assert!(
            registry
                .invoke("unknown", serde_json::json!("valid"))
                .is_none(),
            "invocation should return None for unknown handler"
        );

        let _future = registry
            .invoke("my_update", serde_json::json!("valid"))
            .expect("handler should exist");
    }

    #[test]
    fn should_succeed_validation_when_no_validator_provided() {
        let mut registry = UpdateRegistry::new();

        let handler =
            Arc::new(|val: Value| Box::pin(async move { Ok(val) }) as UpdateHandlerFuture);

        registry.register("my_update", None, handler);
        assert!(
            registry
                .validate("my_update", &serde_json::json!("valid"))
                .is_ok(),
            "validation should trivially succeed if no validator is provided"
        );
    }
}
