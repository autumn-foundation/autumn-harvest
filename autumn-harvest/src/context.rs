//! Execution contexts passed to workflow and activity functions.
//!
//! `WorkflowContext` drives deterministic replay — it tracks the event history
//! pointer and routes commands either to real execution or to history lookup.
//!
//! `ActivityContext` provides heartbeating, state access, and a DB connection
//! to activities.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

/// Context passed to every workflow function.
///
/// In **normal mode** (no history to replay): commands generate new events.
/// In **replay mode** (resuming from Postgres history): commands are matched
/// against recorded events and return the stored result without re-executing.
pub struct WorkflowContext {
    /// When `true`, the context is replaying history rather than executing fresh.
    replaying: bool,
    /// Shared state map (same `AppState` extras as the web server).
    state: Arc<HashMap<TypeId, Box<dyn Any + Send + Sync>>>,
    // Phase 2 will add: event history, event pointer, pending commands channel
}

impl WorkflowContext {
    /// Returns `true` if currently replaying recorded event history.
    #[must_use]
    pub fn is_replaying(&self) -> bool {
        self.replaying
    }

    /// Switch replay mode on or off. Called by the worker executor.
    pub fn set_replaying(&mut self, replaying: bool) {
        self.replaying = replaying;
    }

    /// Access typed shared state (e.g., email clients, config).
    ///
    /// Returns `None` if the state type was not registered with the builder.
    #[must_use]
    pub fn state<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.state.get(&TypeId::of::<T>())?.downcast_ref::<T>()
    }

    /// Constructor for testing — creates a context in normal (non-replay) mode
    /// with empty state.
    #[cfg(any(test, feature = "testing"))]
    #[must_use]
    pub fn new_test() -> Self {
        Self {
            replaying: false,
            state: Arc::new(HashMap::new()),
        }
    }
}

/// Context passed to every activity function.
///
/// Activities may perform I/O, call external services, and interact with the
/// database. The context provides heartbeating to signal liveness and state
/// access for shared resources.
pub struct ActivityContext {
    /// Shared state map.
    state: Arc<HashMap<TypeId, Box<dyn Any + Send + Sync>>>,
    // Phase 2 will add: heartbeat sender, cancellation token, DB pool handle
}

impl ActivityContext {
    /// Access typed shared state.
    #[must_use]
    pub fn state<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.state.get(&TypeId::of::<T>())?.downcast_ref::<T>()
    }

    /// Send a heartbeat to signal the activity is still running.
    ///
    /// Phase 1 stub — full implementation in Phase 2 (worker heartbeat loop).
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow was cancelled and the activity should
    /// stop. Activities should check this return value on long operations.
    pub async fn heartbeat(&self, _details: impl serde::Serialize) -> crate::HarvestResult<()> {
        // Phase 2: send details via heartbeat channel to the worker's batch sender
        Ok(())
    }

    /// Constructor for testing.
    #[cfg(any(test, feature = "testing"))]
    #[must_use]
    pub fn new_test() -> Self {
        Self {
            state: Arc::new(HashMap::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activity_context_state_returns_none_when_not_registered() {
        let ctx = ActivityContext::new_test();
        let state: Option<&String> = ctx.state::<String>();
        assert!(state.is_none());
    }

    #[test]
    fn workflow_context_new_is_in_normal_mode() {
        let ctx = WorkflowContext::new_test();
        assert!(!ctx.is_replaying());
    }

    #[test]
    fn workflow_context_replay_mode_flag() {
        let mut ctx = WorkflowContext::new_test();
        ctx.set_replaying(true);
        assert!(ctx.is_replaying());
    }
}
