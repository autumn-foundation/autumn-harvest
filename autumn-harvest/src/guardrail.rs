//! Deterministic workflow guardrail rule catalog (issue #173).
//!
//! Provides a machine-readable catalog of determinism rules that workflow
//! authors must follow. Each rule has a stable ID, severity, category,
//! explanation, and Harvest-shaped alternative guidance.
//!
//! This module intentionally contains no source-scanning logic; it is the
//! stable contract that a future checker will emit findings against.
//!
//! # Rule IDs
//!
//! All rules use the `HVGxxx` prefix (Harvest Workflow Guardrail) and are
//! permanently assigned — IDs are never reused after a rule is retired.
//!
//! | ID     | Category       | Severity     | Summary                              |
//! |--------|---------------|--------------|--------------------------------------|
//! | HVG001 | WallClock     | HardBlocker  | Wall-clock time reads                |
//! | HVG002 | Randomness    | HardBlocker  | Random number / ad-hoc UUID          |
//! | HVG003 | ProcessEnv    | HardBlocker  | Process/environment reads            |
//! | HVG004 | SleepTimer    | HardBlocker  | Direct sleep / timer primitives      |
//! | HVG005 | BackgroundTask| HardBlocker  | Background task spawning             |
//! | HVG006 | DirectIo      | HardBlocker  | Direct network / DB / filesystem I/O |
//! | HVG007 | ProcessGlobal | HardBlocker  | Process-global state mutation        |
//! | HVG008 | NonDeterministicPredicate| HardBlocker | Non-deterministic predicate closures|

/// Severity of a guardrail rule violation.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Severity {
    /// Fails CI by default. Workflow replay is at risk.
    HardBlocker,
    /// Advisory. Can be acknowledged explicitly without hiding the finding.
    Warning,
}

/// The determinism category a rule belongs to.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RuleCategory {
    /// Direct reads of wall-clock time (`SystemTime`, `Instant`, `chrono::Utc::now()`).
    WallClock,
    /// Random number generation or ad-hoc UUID creation (`rand`, `Uuid::new_v4()`).
    Randomness,
    /// Reads of process/environment state (`std::env::var`, `std::env::args`).
    ProcessEnv,
    /// Direct sleep or timer primitives outside Harvest APIs (`thread::sleep`, `tokio::time::sleep`).
    SleepTimer,
    /// Spawning background tasks from workflow code (`tokio::spawn`, `thread::spawn`).
    BackgroundTask,
    /// Direct network, database, or filesystem I/O from workflow code.
    DirectIo,
    /// Mutation of process-global state (`static mut`, shared atomics, global registries).
    ProcessGlobal,
    /// Non-deterministic predicate evaluated inside `await_condition` closures.
    NonDeterministicPredicate,
}

/// A single entry in the guardrail rule catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleEntry {
    /// Stable rule identifier (`HVGxxx`).
    pub id: &'static str,
    /// Severity of this rule.
    pub severity: Severity,
    /// Category this rule belongs to.
    pub category: RuleCategory,
    /// Short explanation of why this pattern breaks deterministic replay.
    pub explanation: &'static str,
    /// Harvest-shaped alternative: what to use instead.
    pub alternative: &'static str,
}

// ── Static catalog ────────────────────────────────────────────────────────────

static CATALOG: &[RuleEntry] = &[
    RuleEntry {
        id: "HVG001",
        severity: Severity::HardBlocker,
        category: RuleCategory::WallClock,
        explanation: "Reading wall-clock time inside a workflow (std::time::Instant::now(), \
            SystemTime::now(), chrono::Utc::now(), etc.) produces a different value on every \
            replay. The workflow engine re-executes the function body deterministically against \
            recorded history; a fresh timestamp breaks that contract and causes a \
            non-determinism error.",
        alternative: "Use ctx.now() (WorkflowContext) to read the workflow-logical clock, \
            which returns the WorkflowStarted timestamp and replays identically on every \
            subsequent run. If you need a real wall-clock timestamp as a side-effect, wrap it \
            in an activity and return it as the activity output.",
    },
    RuleEntry {
        id: "HVG002",
        severity: Severity::HardBlocker,
        category: RuleCategory::Randomness,
        explanation: "Calling rand::random(), rand::thread_rng(), Uuid::new_v4(), or any other \
            source of randomness directly in a workflow body produces a different value on each \
            replay pass. Since the workflow function is re-run from the top on every resume, the \
            random sequence diverges from what was recorded in harvest_events.",
        alternative: "Generate random values inside an activity (ActivityContext) and return them \
            as the activity result, which is durably recorded. Alternatively, pass pre-generated \
            values as workflow input. For replay-safe UUIDs, use ctx.random_uuid(id) which \
            records the generated UUID in history and replays it deterministically.",
    },
    RuleEntry {
        id: "HVG003",
        severity: Severity::HardBlocker,
        category: RuleCategory::ProcessEnv,
        explanation: "Reading std::env::var(), std::env::args(), or process environment at \
            workflow execution time couples replay correctness to the environment of whichever \
            worker process happens to run the replay. Environment variables may differ between \
            the original execution host and a replay host, causing divergence.",
        alternative: "Read configuration at worker startup (WorkerConfig) and pass it as typed \
            state via ctx.state::<T>(). For values that must vary per workflow run, pass them as \
            workflow input parameters. Signal-based reconfiguration can use ctx.wait_for_signal() \
            to update workflow-local state durably.",
    },
    RuleEntry {
        id: "HVG004",
        severity: Severity::HardBlocker,
        category: RuleCategory::SleepTimer,
        explanation: "Calling std::thread::sleep(), tokio::time::sleep(), async_std::task::sleep(), \
            or any OS/runtime sleep primitive in a workflow body does not record a durable timer. \
            The workflow worker's task is blocked but no timer event is appended to harvest_events. \
            After a worker restart the sleep is replayed as a no-op, changing observable timing.",
        alternative: "Use ctx.timer(timer_id, duration_secs) (WorkflowContext) which emits a \
            TimerStarted event into the durable history. The timer is enforced by the harvest \
            timeout scanner and survives worker restarts. For periodic scheduling, use DagBuilder \
            with an Interval or Cron schedule.",
    },
    RuleEntry {
        id: "HVG005",
        severity: Severity::HardBlocker,
        category: RuleCategory::BackgroundTask,
        explanation: "Spawning a background task with tokio::spawn(), std::thread::spawn(), \
            async_std::task::spawn(), or rayon::spawn() from inside a workflow function creates \
            untracked side-effects. The spawned task runs outside Harvest's supervision: its \
            completion is not recorded in harvest_events, it is not retried on failure, and it \
            is silently abandoned on worker restart.",
        alternative: "Model concurrent work as parallel activity branches using \
            ctx.execute_activity_raw(name, input, queue) calls combined with futures::join! or \
            futures::try_join!. Harvest records each branch's result durably and re-joins them \
            correctly on replay. For fire-and-forget side-effects, use a local activity \
            (#[activity(local = true)]) so the result is at least logged to history.",
    },
    RuleEntry {
        id: "HVG006",
        severity: Severity::HardBlocker,
        category: RuleCategory::DirectIo,
        explanation: "Performing network requests (reqwest, hyper, tonic), database queries \
            (sqlx, diesel, tokio-postgres), or filesystem I/O (std::fs, tokio::fs) directly \
            inside a workflow body creates non-idempotent side-effects that are invisible to \
            the event store. On replay the I/O is re-executed, potentially sending duplicate \
            requests, corrupting database state, or failing because external state has changed.",
        alternative: "Wrap all I/O in activities (#[activity]). Activities are the unit of \
            durable, retryable side-effect in Harvest. Their inputs and outputs are recorded in \
            harvest_events so the workflow can replay without re-executing I/O. Use \
            ctx.execute_activity_raw(name, input, queue) from the workflow body to schedule \
            the activity.",
    },
    RuleEntry {
        id: "HVG007",
        severity: Severity::HardBlocker,
        category: RuleCategory::ProcessGlobal,
        explanation: "Mutating process-global state — static mut variables, std::sync::Mutex \
            guards wrapping shared counters, lazy_static or once_cell singletons, global \
            metrics registries, or similar — from inside a workflow body creates a side-effect \
            that is not recorded in harvest_events. On replay the mutation is re-applied, \
            producing double-counting or inconsistent global state across worker processes.",
        alternative: "Keep workflow execution stateless with respect to process globals. \
            Accumulate workflow-local state in local variables across ctx.timer() and \
            ctx.execute_activity_raw() boundaries. If you need to emit metrics or update a \
            registry, do so inside an activity where the side-effect is bounded to a single \
            retryable execution unit and is not re-applied on replay.",
    },
    RuleEntry {
        id: "HVG008",
        severity: Severity::HardBlocker,
        category: RuleCategory::NonDeterministicPredicate,
        explanation: "Evaluating non-deterministic predicates inside await_condition or \
            await_condition_timeout (such as checking Instant::now(), SystemTime::now(), or \
            calling random generators inside the closure) leads to non-deterministic execution \
            paths during replay. Predicates must be pure projections of deterministic workflow \
            local state variables rehydrated by replaying events.",
        alternative: "Use durable timers (ctx.timer()) for time-based pauses, and ensure the \
            predicate closure relies purely on local variables mutated by deterministic signals or \
            activities.",
    },
];

/// Return the full guardrail rule catalog.
#[must_use]
pub fn catalog() -> &'static [RuleEntry] {
    CATALOG
}

/// Look up a catalog entry by its stable rule ID.
#[must_use]
pub fn rule_by_id(id: &str) -> Option<&'static RuleEntry> {
    CATALOG.iter().find(|e| e.id == id)
}

// ── GuardrailFinding ──────────────────────────────────────────────────────────

/// A machine-readable finding produced by a guardrail checker.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GuardrailFinding {
    /// Stable rule identifier.
    pub rule_id: String,
    /// Severity of the violation.
    pub severity: Severity,
    /// Category of the violation.
    pub category: RuleCategory,
    /// Human-readable explanation of the specific violation.
    pub message: String,
    /// Harvest-shaped alternative from the catalog.
    pub alternative: String,
    /// Workflow name if known.
    pub workflow_name: Option<String>,
    /// Source location if available (`file:line`).
    pub source_location: Option<String>,
}

impl GuardrailFinding {
    /// Construct a finding from a catalog entry plus call-site context.
    #[must_use]
    pub fn from_rule(
        entry: &'static RuleEntry,
        message: impl Into<String>,
        workflow_name: Option<String>,
        source_location: Option<String>,
    ) -> Self {
        Self {
            rule_id: entry.id.to_string(),
            severity: entry.severity.clone(),
            category: entry.category.clone(),
            message: message.into(),
            alternative: entry.alternative.to_string(),
            workflow_name,
            source_location,
        }
    }
}

// ── GuardrailSuppression ──────────────────────────────────────────────────────

/// Error returned when a suppression is constructed with invalid arguments.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GuardrailSuppressionError {
    /// The reason string was empty or contained only whitespace.
    #[error("suppression reason cannot be empty")]
    EmptyReason,
    /// The rule ID was empty or contained only whitespace.
    #[error("suppression rule ID cannot be empty")]
    EmptyRuleId,
}

/// Wire type used only for deserialization so the custom `TryFrom` path runs.
#[derive(serde::Deserialize)]
struct GuardrailSuppressionRaw {
    rule_id: String,
    reason: String,
}

impl TryFrom<GuardrailSuppressionRaw> for GuardrailSuppression {
    type Error = GuardrailSuppressionError;
    fn try_from(raw: GuardrailSuppressionRaw) -> Result<Self, Self::Error> {
        Self::new(raw.rule_id, raw.reason)
    }
}

/// An auditable suppression of a guardrail finding. Requires a non-empty reason.
///
/// Deserialization enforces the same invariants as [`GuardrailSuppression::new`]:
/// both `rule_id` and `reason` must be non-empty non-whitespace strings.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "GuardrailSuppressionRaw")]
pub struct GuardrailSuppression {
    rule_id: String,
    reason: String,
}

impl GuardrailSuppression {
    /// Create a new suppression.
    ///
    /// # Errors
    ///
    /// Returns `Err(EmptyRuleId)` if `rule_id` is empty or whitespace.
    /// Returns `Err(EmptyReason)` if `reason` is empty or whitespace.
    pub fn new(
        rule_id: impl Into<String>,
        reason: impl Into<String>,
    ) -> Result<Self, GuardrailSuppressionError> {
        let rule_id = rule_id.into();
        let reason = reason.into();
        if rule_id.trim().is_empty() {
            return Err(GuardrailSuppressionError::EmptyRuleId);
        }
        if reason.trim().is_empty() {
            return Err(GuardrailSuppressionError::EmptyReason);
        }
        Ok(Self { rule_id, reason })
    }

    /// The rule ID this suppression covers.
    #[must_use]
    pub fn rule_id(&self) -> &str {
        &self.rule_id
    }

    /// The auditable reason for the suppression.
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn catalog_is_nonempty() {
        assert!(!catalog().is_empty());
    }

    #[test]
    fn all_rule_ids_start_with_hvg_and_numeric_suffix() {
        for entry in catalog() {
            assert!(entry.id.starts_with("HVG"), "bad id: {}", entry.id);
            let suffix = &entry.id["HVG".len()..];
            assert!(
                !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit()),
                "non-numeric suffix in id: {}",
                entry.id
            );
        }
    }

    #[test]
    fn rule_ids_are_unique() {
        let mut ids = HashSet::new();
        for entry in catalog() {
            assert!(ids.insert(entry.id), "duplicate: {}", entry.id);
        }
    }

    #[test]
    fn suppression_rejects_empty_reason() {
        assert!(GuardrailSuppression::new("HVG001", "").is_err());
        assert!(GuardrailSuppression::new("HVG001", "   ").is_err());
    }

    #[test]
    fn suppression_accepts_valid_reason() {
        let s = GuardrailSuppression::new("HVG001", "replay test covers this").unwrap();
        assert_eq!(s.rule_id(), "HVG001");
        assert!(!s.reason().is_empty());
    }
}
