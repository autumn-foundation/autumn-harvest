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

    /// Returns the validator for `name`, if registered and present.
    #[must_use]
    pub fn get_validator(&self, name: &str) -> Option<BoxUpdateValidator> {
        self.entries.get(name).and_then(|e| e.validator.clone())
    }

    /// Returns the handler for `name`, if registered.
    #[must_use]
    pub fn get_handler(&self, name: &str) -> Option<BoxUpdateHandler> {
        self.entries.get(name).map(|e| e.handler.clone())
    }
}
