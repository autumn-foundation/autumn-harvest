//! Synthetic liveness canary (issue #796).
//!
//! A built-in, throwaway workflow that continuously probes the *live*
//! `start → dispatch → activity → durable-timer → complete` execution path so a
//! wedged pipeline (workers polling but never completing, a stalled scheduler
//! tick, a write-blocked shard) is detected within one probe interval — before
//! any customer workflow misses its SLA — with zero operator-authored workflow
//! code.
//!
//! **Distinct from the #512 replay canary.** The replay canary (`run_workflow_canary`,
//! `canary_mode`, `/admin/workflows/replay-canary`) validates *code changes* by
//! replaying in-flight histories against new workflow code. This synthetic
//! liveness canary validates *the running pipeline* by actively executing a real
//! throwaway workflow end to end. The two share only the word "canary".
//!
//! This module owns the reserved-name constants and the name-matching predicates
//! that the engine (terminal-metric emission, fleet-count exclusion) and the
//! plugin (schedule registration, `GET /admin/canary`) both key on. The canary
//! is registered as an ordinary `#[workflow]`/`#[activity]` on the standard
//! execution path, so it introduces no new `WorkflowEvent` variant, no migration,
//! and no replay-determinism change.

/// Name prefix shared by every built-in synthetic-liveness-canary workflow
/// (issue #796).
///
/// The plugin registers one canary workflow per configured probe queue, named
/// `{CANARY_WORKFLOW_NAME_PREFIX}__{queue}` (double underscore + queue name), so
/// a single wedged worker pool is distinguishable from "some queue is draining".
/// All such names share this prefix, so [`is_canary_workflow`] matches them with
/// a single `starts_with` check.
pub const CANARY_WORKFLOW_NAME_PREFIX: &str = "__harvest_canary_probe";

/// Reserved name of the built-in synthetic-liveness-canary activity (issue #796).
///
/// Unlike the two reserved worker-session internal activities (issue #606), whose
/// handlers are never invoked, this activity has a *real* working handler — the
/// canary must actually dispatch and complete a non-local activity to prove the
/// dispatch path works end to end.
pub const CANARY_ACTIVITY_NAME: &str = "__harvest_canary_probe_activity";

/// Returns `true` when `name` is a built-in synthetic-liveness-canary workflow
/// (issue #796).
///
/// Matches the per-queue naming scheme `{CANARY_WORKFLOW_NAME_PREFIX}__{queue}`
/// via a prefix check, so `__harvest_canary_probe__default` and
/// `__harvest_canary_probe__email` both match while an ordinary user workflow
/// does not.
///
/// Used at the workflow-terminal metric-emission sites to route a canary run to
/// the `harvest.canary.*` metrics (and to *skip* `harvest.workflow.terminal`),
/// and in the fleet-count exclusion so canary runs never pollute success-rate
/// dashboards.
#[must_use]
pub fn is_canary_workflow(name: &str) -> bool {
    name.starts_with(CANARY_WORKFLOW_NAME_PREFIX)
}

/// Returns `true` when `name` is any reserved synthetic-liveness-canary name — a
/// canary workflow (any probe queue) or the canary activity (issue #796).
///
/// Distinct from [`crate::context::is_reserved_session_activity_name`], which
/// guards the worker-session internal activities (issue #606).
#[must_use]
pub fn is_reserved_canary_name(name: &str) -> bool {
    is_canary_workflow(name) || name == CANARY_ACTIVITY_NAME
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constants_have_expected_values() {
        assert_eq!(CANARY_WORKFLOW_NAME_PREFIX, "__harvest_canary_probe");
        assert_eq!(CANARY_ACTIVITY_NAME, "__harvest_canary_probe_activity");
    }

    #[test]
    fn is_canary_workflow_matches_the_bare_prefix() {
        assert!(is_canary_workflow(CANARY_WORKFLOW_NAME_PREFIX));
    }

    #[test]
    fn is_canary_workflow_matches_the_per_queue_naming_scheme() {
        // The plugin registers one canary workflow per configured queue as
        // `{PREFIX}__{queue}` (double underscore + queue name).
        assert!(is_canary_workflow("__harvest_canary_probe__default"));
        assert!(is_canary_workflow("__harvest_canary_probe__email"));
        assert!(is_canary_workflow("__harvest_canary_probe__priority-email"));
    }

    #[test]
    fn is_canary_workflow_rejects_user_and_other_reserved_names() {
        assert!(!is_canary_workflow("my_workflow"));
        assert!(!is_canary_workflow("onboarding"));
        // A worker-session reserved name is NOT a canary name.
        assert!(!is_canary_workflow("__harvest_session_acquire"));
        assert!(!is_canary_workflow("__harvest_session_release"));
        // A name that merely contains the prefix later on does not match.
        assert!(!is_canary_workflow("not__harvest_canary_probe"));
        // NOTE: the canary activity name shares the workflow prefix
        // (`__harvest_canary_probe_activity` starts with `__harvest_canary_probe`),
        // so `is_canary_workflow` returns `true` for it. That is harmless:
        // `is_canary_workflow` is only ever called with *workflow* names (at the
        // terminal-metric sites and in the fleet-count SQL), never activity names
        // — activities have no `harvest_workflow_executions` rows. Use
        // `is_reserved_canary_name` when both kinds must be recognised.
    }

    #[test]
    fn is_reserved_canary_name_covers_workflows_and_the_activity() {
        assert!(is_reserved_canary_name(CANARY_WORKFLOW_NAME_PREFIX));
        assert!(is_reserved_canary_name("__harvest_canary_probe__default"));
        assert!(is_reserved_canary_name(CANARY_ACTIVITY_NAME));
    }

    #[test]
    fn is_reserved_canary_name_rejects_unrelated_names() {
        assert!(!is_reserved_canary_name("my_workflow"));
        assert!(!is_reserved_canary_name("deliver_webhook"));
        assert!(!is_reserved_canary_name("__harvest_session_acquire"));
    }
}
