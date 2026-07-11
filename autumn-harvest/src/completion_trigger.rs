use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", content = "data")]
pub enum InputMapping {
    Passthrough,
    Static(Value),
    Projection(String),
    /// Assemble a structured terminal-outcome envelope from the source
    /// execution's recorded terminal state + row fields (issue #748).
    ///
    /// A source that reached a non-`Completed` terminal state has a `NULL`
    /// recorded output, so a `Passthrough`/`Projection` mapping delivers
    /// `Null` and a failure-routed handler starts blind. This variant instead
    /// projects already-recorded execution data — the terminal state, the
    /// source identity, the output (only on `COMPLETED`), and the error /
    /// cancel reason — into a fixed envelope:
    ///
    /// ```json
    /// {
    ///   "terminal_state": "FAILED",
    ///   "source_exec_id": "…",
    ///   "source_workflow_id": "…",
    ///   "source_workflow_name": "…",
    ///   "output": null,
    ///   "error": "payment declined: insufficient funds"
    /// }
    /// ```
    ///
    /// `projection = None` yields the full envelope; `projection = Some(path)`
    /// plucks a dotted field from it using the same [`project_json_path`]
    /// mechanism `Projection` uses (e.g. `Some("error")` → the bare error
    /// string). Read-side projection only — no `WorkflowEvent` variant, no
    /// migration.
    Outcome {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        projection: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum TerminalState {
    Completed,
    Failed,
    Cancelled,
    TimedOut,
    /// Force-terminated by an operator (or batch/scheduler). Distinct from
    /// `Cancelled`: a completion trigger registered for cooperative
    /// cancellations does **not** fire on a force-terminate, and vice versa
    /// (issue #504).
    Terminated,
}

impl TerminalState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
            Self::Cancelled => "CANCELLED",
            Self::TimedOut => "TIMED_OUT",
            Self::Terminated => "TERMINATED",
        }
    }

    #[allow(clippy::should_implement_trait)]
    #[must_use]
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "COMPLETED" => Some(Self::Completed),
            "FAILED" => Some(Self::Failed),
            "CANCELLED" => Some(Self::Cancelled),
            "TIMED_OUT" => Some(Self::TimedOut),
            "TERMINATED" => Some(Self::Terminated),
            _ => None,
        }
    }
}

/// Maximum nesting depth of a [`TriggerCondition`] tree (issue #810).
///
/// A leaf operator is depth 1; each `All`/`Any`/`Not` wrapper adds one level.
/// Bounds recursion in [`TriggerCondition::evaluate`] so a hostile
/// registration can never `DoS` the terminal-commit path.
pub const MAX_CONDITION_DEPTH: usize = 8;

/// Maximum total node count (leaves + combinators) of a [`TriggerCondition`] tree.
pub const MAX_CONDITION_NODES: usize = 64;

/// Maximum number of candidate values in a [`TriggerCondition::In`] set.
pub const MAX_CONDITION_IN_VALUES: usize = 64;

/// Bounded, declarative output guard for a [`CompletionTrigger`] (issue #810).
///
/// Evaluated **server-side at terminal-commit time** against the source
/// workflow's recorded output JSON — a pure, deterministic function (same
/// output → same fire/skip decision on every redelivery). This is a fixed
/// comparison AST, deliberately **not** an expression/CEL engine; an unknown
/// operator is a serde deserialization error, which the HTTP registration
/// route surfaces as a `400`.
///
/// Leaf operators resolve `path` (a dotted JSON path, the same
/// [`project_json_path`] machinery `InputMapping::Projection` uses; the empty
/// path means the whole output) and compare the projected value:
///
/// - **Missing path or explicit `null`** ⇒ every comparison operator
///   (`Eq`/`NotEq`/ordering/`In`) evaluates to `false` — never a panic or an
///   error. Test absence/nullness explicitly with `Exists` / `IsNull` /
///   `Not(Exists)`.
/// - **`Exists`** is `true` when the path is present (including an explicit
///   `null` value); **`IsNull`** is `true` only when the path is present AND
///   the value is exactly `null` — so `Exists`/`IsNull` genuinely distinguish
///   present-null from absent.
/// - **Numeric coercion rule**: for `Eq`/`NotEq`/`In` and the ordering
///   operators, two **integers** compare exactly (full `i64`/`u64` range,
///   mixed signs included — distinct integers above 2^53 never collapse);
///   when at least one side is a genuine float, both sides coerce to `f64`
///   (so `1 == 1.0`, with `f64` precision governing mixed int/float pairs
///   above 2^53). Otherwise comparison is strict `Value` equality (a number
///   never equals a numeric string; the coercion never recurses into arrays
///   or objects). The ordering operators
///   (`GreaterThan`/`GreaterThanOrEq`/`LessThan`/`LessThanOrEq`) are
///   **numeric-only**: unless both sides are numbers the result is `false`.
///   Note the deliberate asymmetry: `NotEq` on a missing/null path is
///   `false` (comparisons require presence), while `Not(Eq(..))` is `true`
///   (`Not` is pure negation).
/// - `All([])` is `true`, `Any([])` is `false` (conventional identities).
///
/// A source that reached a non-`Completed` terminal state typically has a
/// `NULL` recorded output, which evaluates as a literal JSON `null` root:
/// every member path is missing, so comparisons are `false` — but a
/// `Not(Exists(..))`-style guard can still meaningfully fire. Guards are
/// output-only by design (the failure-cause envelope is issue #748's story).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", content = "data")]
pub enum TriggerCondition {
    Eq { path: String, value: Value },
    NotEq { path: String, value: Value },
    GreaterThan { path: String, value: Value },
    GreaterThanOrEq { path: String, value: Value },
    LessThan { path: String, value: Value },
    LessThanOrEq { path: String, value: Value },
    In { path: String, values: Vec<Value> },
    Exists { path: String },
    IsNull { path: String },
    All(Vec<Self>),
    Any(Vec<Self>),
    Not(Box<Self>),
}

/// Exact integer view of a JSON number: `Some` only when the value is an
/// integer (`i64` or `u64` — both embed losslessly in `i128`). A genuine
/// float (`1.0`) or a non-number is `None`.
fn as_exact_int(v: &Value) -> Option<i128> {
    v.as_i64()
        .map(i128::from)
        .or_else(|| v.as_u64().map(i128::from))
}

/// Equality with the documented numeric-coercion rule: two integers compare
/// **exactly** (via `i128`, covering the full `i64`/`u64` range and mixed
/// signs), so distinct integers above 2^53 — snowflake-style IDs — never
/// collapse through lossy `f64` coercion. Only when at least one side is a
/// genuine float do both sides coerce to `f64` (so `1 == 1.0`); otherwise
/// strict `Value` equality.
fn values_eq(projected: &Value, candidate: &Value) -> bool {
    if let (Some(a), Some(b)) = (as_exact_int(projected), as_exact_int(candidate)) {
        return a == b;
    }
    if let (Some(a), Some(b)) = (projected.as_f64(), candidate.as_f64()) {
        // Deterministic exact f64 comparison — this branch is only reached
        // when at least one side is a genuine float; no epsilon fuzzing
        // (that would make the guard non-obvious to reason about).
        #[allow(clippy::float_cmp)]
        return a == b;
    }
    projected == candidate
}

/// Numeric-only ordering comparison; `false` unless both sides are numbers.
/// Two integers compare exactly (`i128`); mixed int/float falls back to `f64`.
fn values_cmp(projected: &Value, candidate: &Value) -> Option<std::cmp::Ordering> {
    if let (Some(a), Some(b)) = (as_exact_int(projected), as_exact_int(candidate)) {
        return Some(a.cmp(&b));
    }
    match (projected.as_f64(), candidate.as_f64()) {
        (Some(a), Some(b)) => a.partial_cmp(&b),
        _ => None,
    }
}

impl TriggerCondition {
    /// Pure, deterministic evaluation against the source workflow's recorded
    /// output JSON. Never panics; a missing path yields a defined result
    /// (comparisons `false`, `Exists` `false`, `IsNull` `false`).
    #[must_use]
    pub fn evaluate(&self, output: &Value) -> bool {
        match self {
            Self::Eq { path, value } => project_json_path_opt(output, path)
                .is_some_and(|p| !p.is_null() && values_eq(p, value)),
            Self::NotEq { path, value } => project_json_path_opt(output, path)
                .is_some_and(|p| !p.is_null() && !values_eq(p, value)),
            Self::GreaterThan { path, value } => project_json_path_opt(output, path)
                .is_some_and(|p| values_cmp(p, value) == Some(std::cmp::Ordering::Greater)),
            Self::GreaterThanOrEq { path, value } => project_json_path_opt(output, path)
                .is_some_and(|p| {
                    matches!(
                        values_cmp(p, value),
                        Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                    )
                }),
            Self::LessThan { path, value } => project_json_path_opt(output, path)
                .is_some_and(|p| values_cmp(p, value) == Some(std::cmp::Ordering::Less)),
            Self::LessThanOrEq { path, value } => {
                project_json_path_opt(output, path).is_some_and(|p| {
                    matches!(
                        values_cmp(p, value),
                        Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                    )
                })
            }
            Self::In { path, values } => project_json_path_opt(output, path)
                .is_some_and(|p| !p.is_null() && values.iter().any(|v| values_eq(p, v))),
            Self::Exists { path } => project_json_path_opt(output, path).is_some(),
            Self::IsNull { path } => {
                project_json_path_opt(output, path).is_some_and(Value::is_null)
            }
            Self::All(conds) => conds.iter().all(|c| c.evaluate(output)),
            Self::Any(conds) => conds.iter().any(|c| c.evaluate(output)),
            Self::Not(cond) => !cond.evaluate(output),
        }
    }

    /// Boundedness validation, enforced at **both** registration surfaces
    /// (`HarvestBuilder::try_build` and `POST /admin/completion-triggers`):
    /// nesting depth ≤ [`MAX_CONDITION_DEPTH`], total nodes ≤
    /// [`MAX_CONDITION_NODES`], `In` sets ≤ [`MAX_CONDITION_IN_VALUES`], and
    /// every non-empty path free of empty segments (`"a..b"`, `".a"`, `"a."`
    /// are malformed; the empty path selects the whole output and is valid).
    ///
    /// # Errors
    ///
    /// Returns a human-readable description of the first violation found.
    pub fn validate(&self) -> Result<(), String> {
        let mut nodes = 0usize;
        self.validate_inner(1, &mut nodes)?;
        if nodes > MAX_CONDITION_NODES {
            return Err(format!(
                "condition has {nodes} nodes, exceeding the maximum of {MAX_CONDITION_NODES}"
            ));
        }
        Ok(())
    }

    fn validate_inner(&self, depth: usize, nodes: &mut usize) -> Result<(), String> {
        if depth > MAX_CONDITION_DEPTH {
            return Err(format!(
                "condition nesting exceeds the maximum depth of {MAX_CONDITION_DEPTH}"
            ));
        }
        *nodes += 1;
        // Cheap early exit so a hostile wide tree fails fast.
        if *nodes > MAX_CONDITION_NODES {
            return Err(format!(
                "condition exceeds the maximum of {MAX_CONDITION_NODES} nodes"
            ));
        }
        match self {
            Self::Eq { path, .. }
            | Self::NotEq { path, .. }
            | Self::GreaterThan { path, .. }
            | Self::GreaterThanOrEq { path, .. }
            | Self::LessThan { path, .. }
            | Self::LessThanOrEq { path, .. }
            | Self::Exists { path }
            | Self::IsNull { path } => validate_condition_path(path),
            Self::In { path, values } => {
                validate_condition_path(path)?;
                if values.len() > MAX_CONDITION_IN_VALUES {
                    return Err(format!(
                        "In set has {} values, exceeding the maximum of {MAX_CONDITION_IN_VALUES}",
                        values.len()
                    ));
                }
                Ok(())
            }
            Self::All(conds) | Self::Any(conds) => {
                for c in conds {
                    c.validate_inner(depth + 1, nodes)?;
                }
                Ok(())
            }
            Self::Not(cond) => cond.validate_inner(depth + 1, nodes),
        }
    }
}

fn validate_condition_path(path: &str) -> Result<(), String> {
    if path.is_empty() {
        // Empty path selects the whole source output — valid.
        return Ok(());
    }
    if path.split('.').any(str::is_empty) {
        return Err(format!(
            "malformed condition path {path:?}: empty path segment"
        ));
    }
    Ok(())
}

/// Resolution of a stored (possibly absent or corrupt) `condition` column
/// against a source output — the pure decision the terminal-commit gate
/// applies per trigger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConditionGate {
    /// No condition, or condition evaluated `true`: fire as today.
    Pass,
    /// Condition evaluated `false`: record a resolved-skip (durable fires
    /// row), do not start.
    Unmet,
    /// Stored condition failed to deserialize or violates the boundedness
    /// caps: **fail closed** — skip with a distinct reason and a warning,
    /// never fire on an unintelligible guard, never error the terminal
    /// commit. Unlike `Unmet` (a final, correct decision over recorded
    /// output), `Invalid` means "this binary cannot understand the guard"
    /// (mixed-version deploy skew, corrupt row), so **no fires row is
    /// written** (mirroring the `validation_failed` precedent): the pair is
    /// left *unresolved* rather than durably suppressed, and a later
    /// re-entry into trigger evaluation re-evaluates it. **Recovery limit
    /// (honest contract):** trigger evaluation runs only inside a source
    /// terminal commit and no scanner re-visits unresolved pairs, so a
    /// re-entry for an already-terminal execution is rare (e.g. a
    /// parent-close-cascade race) and NOT guaranteed — without one, the
    /// fire is lost for that terminal. Treat every `condition_invalid`
    /// skip (warn log + the `harvest.completion_trigger.skipped{reason=
    /// "condition_invalid"}` counter) as a fire to re-trigger by hand, and
    /// avoid the window entirely by registering conditions that use newly
    /// added operators only after the whole fleet runs the new binary. A
    /// durable re-evaluation queue is a named follow-up (issue #810 /
    /// PR #972 review).
    Invalid,
}

/// Pure gate: `None` = unconditional (byte-identical legacy behavior).
///
/// Evaluates the stored condition against the source output **as stored** —
/// raw bytes from `harvest_workflow_executions.output`. Engine write paths
/// always store the plaintext handler output there (payload codecs/offload
/// apply only to the `harvest_events` copy), so guards see real output today.
#[must_use]
pub fn gate_stored_condition(stored: Option<&Value>, source_output: &Value) -> ConditionGate {
    let Some(raw) = stored else {
        return ConditionGate::Pass;
    };
    // `&Value` implements `serde::Deserializer`, so no clone is needed — this
    // runs inside every terminal transaction for every trigger on the source.
    TriggerCondition::deserialize(raw).map_or(ConditionGate::Invalid, |cond| {
        if cond.validate().is_err() {
            ConditionGate::Invalid
        } else if cond.evaluate(source_output) {
            ConditionGate::Pass
        } else {
            ConditionGate::Unmet
        }
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompletionTrigger {
    pub id: Uuid,
    pub source_workflow_name: String,
    pub terminal_states: Vec<TerminalState>,
    pub target_workflow_name: String,
    pub input_mapping: InputMapping,
    pub queue_name: Option<String>,
    /// Optional output guard (issue #810). `None` = unconditional — the
    /// trigger fires exactly as before #810.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition: Option<TriggerCondition>,
}

impl CompletionTrigger {
    pub fn new(
        source_workflow_name: impl Into<String>,
        target_workflow_name: impl Into<String>,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            source_workflow_name: source_workflow_name.into(),
            terminal_states: vec![TerminalState::Completed],
            target_workflow_name: target_workflow_name.into(),
            input_mapping: InputMapping::Passthrough,
            queue_name: None,
            condition: None,
        }
    }

    #[must_use]
    pub const fn with_id(mut self, id: Uuid) -> Self {
        self.id = id;
        self
    }

    #[must_use]
    pub fn with_terminal_states(mut self, states: Vec<TerminalState>) -> Self {
        self.terminal_states = states;
        self
    }

    #[must_use]
    pub fn with_input_mapping(mut self, mapping: InputMapping) -> Self {
        self.input_mapping = mapping;
        self
    }

    #[must_use]
    pub fn with_queue_name(mut self, queue_name: impl Into<String>) -> Self {
        self.queue_name = Some(queue_name.into());
        self
    }

    #[must_use]
    pub fn with_optional_queue_name(mut self, queue_name: Option<String>) -> Self {
        self.queue_name = queue_name;
        self
    }

    /// Attach an output guard (issue #810): the trigger fires only when
    /// `condition` evaluates `true` against the source workflow's recorded
    /// output JSON. Validated (boundedness caps, path shape) at
    /// `HarvestBuilder::try_build`.
    #[must_use]
    pub fn with_condition(mut self, condition: TriggerCondition) -> Self {
        self.condition = Some(condition);
        self
    }
}

#[must_use]
pub fn project_json_path(value: &Value, path: &str) -> Value {
    project_json_path_opt(value, path)
        .cloned()
        .unwrap_or(Value::Null)
}

/// Build the terminal-outcome envelope for [`InputMapping::Outcome`] (issue
/// #748). A pure read-side projection of already-recorded execution data — no
/// event variant, no migration.
///
/// Envelope semantics: `output` is non-null **only** on `COMPLETED` (the
/// caller passes `Some(&output)` only when `state == TerminalState::Completed`,
/// else `None`); `error` carries the failure message on `FAILED`/`TIMED_OUT`
/// and the cancel reason on `CANCELLED` when one was recorded (the caller just
/// forwards `execution.error`, which is naturally `None` on `COMPLETED`).
/// `terminal_state` is the state's canonical string (e.g. `"FAILED"`).
// The sole non-test caller (`evaluate_triggers_for_execution`) is `db`-gated,
// so this is genuinely unused in a `--no-default-features` lib build.
#[cfg_attr(not(feature = "db"), allow(dead_code))]
pub(crate) fn build_outcome_envelope(
    exec_id: crate::types::ExecutionId,
    state: TerminalState,
    workflow_id: &str,
    workflow_name: &str,
    output: Option<Value>,
    error: Option<&str>,
) -> Value {
    serde_json::json!({
        "terminal_state": state.as_str(),
        "source_exec_id": exec_id.to_string(),
        "source_workflow_id": workflow_id,
        "source_workflow_name": workflow_name,
        "output": output.unwrap_or(Value::Null),
        "error": error.map_or(Value::Null, |s| Value::String(s.to_string())),
    })
}

/// Borrowing sibling of [`project_json_path`] that distinguishes a **missing**
/// path (`None`) from a **present-and-null** value (`Some(&Value::Null)`) —
/// the distinction [`TriggerCondition::Exists`] / [`TriggerCondition::IsNull`]
/// are defined on (issue #810). Same walker, same segment semantics.
fn project_json_path_opt<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    if path.is_empty() {
        return Some(value);
    }
    let mut current = value;
    for part in path.split('.') {
        if part.is_empty() {
            continue;
        }
        current = current.get(part)?;
    }
    Some(current)
}

/// Synchronizes completion triggers with the database.
///
/// # Errors
///
/// Returns an error if database operations fail.
#[cfg(feature = "db")]
pub async fn sync_completion_triggers(
    conn: &mut diesel_async::AsyncPgConnection,
    triggers: &[CompletionTrigger],
) -> crate::error::HarvestResult<()> {
    use crate::models::NewCompletionTriggerDb;
    use crate::schema::harvest_completion_triggers::dsl;
    use chrono::Utc;
    use diesel::prelude::*;
    use diesel_async::{AsyncConnection, RunQueryDsl};

    let active_ids: Vec<Uuid> = triggers.iter().map(|t| t.id).collect();
    let triggers = triggers.to_vec();

    conn.transaction(|tx| {
        Box::pin(async move {
            // First, delete any static triggers that are no longer present in the builder's triggers list.
            if active_ids.is_empty() {
                diesel::delete(dsl::harvest_completion_triggers)
                    .filter(dsl::is_static.eq(true))
                    .execute(tx)
                    .await
                    .map_err(crate::error::database_error)?;
            } else {
                diesel::delete(dsl::harvest_completion_triggers)
                    .filter(dsl::is_static.eq(true))
                    .filter(dsl::id.ne_all(&active_ids))
                    .execute(tx)
                    .await
                    .map_err(crate::error::database_error)?;
            }

            for trigger in &triggers {
                let db_row = NewCompletionTriggerDb {
                    id: trigger.id,
                    source_workflow_name: trigger.source_workflow_name.clone(),
                    terminal_states: serde_json::to_value(&trigger.terminal_states)?,
                    target_workflow_name: trigger.target_workflow_name.clone(),
                    input_mapping: serde_json::to_value(&trigger.input_mapping)?,
                    queue_name: trigger.queue_name.clone(),
                    is_static: true,
                    condition: trigger
                        .condition
                        .as_ref()
                        .map(serde_json::to_value)
                        .transpose()?,
                };

                diesel::insert_into(dsl::harvest_completion_triggers)
                    .values(&db_row)
                    .on_conflict(dsl::id)
                    .do_update()
                    .set((
                        dsl::source_workflow_name.eq(&db_row.source_workflow_name),
                        dsl::terminal_states.eq(&db_row.terminal_states),
                        dsl::target_workflow_name.eq(&db_row.target_workflow_name),
                        dsl::input_mapping.eq(&db_row.input_mapping),
                        dsl::queue_name.eq(&db_row.queue_name),
                        dsl::is_static.eq(true),
                        dsl::condition.eq(&db_row.condition),
                        dsl::updated_at.eq(Utc::now()),
                    ))
                    .execute(tx)
                    .await
                    .map_err(crate::error::database_error)?;
            }

            Ok(())
        })
    })
    .await
}

#[cfg(feature = "db")]
pub struct WorkflowMetadata {
    pub concurrency: Option<crate::concurrency::ConcurrencyPolicy>,
    pub max_input_bytes: Option<u64>,
    pub owner: Option<String>,
    pub runbook_url: Option<String>,
    pub severity: Option<String>,
    pub input_schema: Option<fn() -> serde_json::Value>,
    /// Declared soft-SLA default (issue #487), resolved at completion-trigger
    /// start time into a fresh `sla_deadline_at`.
    pub sla: Option<std::time::Duration>,
    /// Declared workflow-type retry policy default (issue #523).
    pub retry_policy: Option<crate::policy::RetryPolicy>,
}

#[cfg(feature = "db")]
pub static GLOBAL_WORKFLOW_METADATA: std::sync::RwLock<
    Option<std::collections::HashMap<String, WorkflowMetadata>>,
> = std::sync::RwLock::new(None);

#[cfg(feature = "db")]
pub static GLOBAL_MAX_WORKFLOW_INPUT_BYTES: std::sync::RwLock<u64> =
    std::sync::RwLock::new(crate::builder::DEFAULT_MAX_WORKFLOW_INPUT_BYTES);

#[cfg(feature = "db")]
pub static GLOBAL_DEFAULT_WORKFLOW_QUEUE: std::sync::RwLock<Option<String>> =
    std::sync::RwLock::new(None);

/// Server-side ceiling on `workflow_attempt` for completion-trigger-started workflows (issue #523).
///
/// Mirrors the per-start ceiling applied on API/scheduler paths. Set at worker startup via
/// `HandlerRegistry::with_max_workflow_attempts_ceiling`.
#[cfg(feature = "db")]
pub static GLOBAL_MAX_WORKFLOW_ATTEMPTS_CEILING: std::sync::RwLock<Option<u32>> =
    std::sync::RwLock::new(None);

#[cfg(feature = "db")]
pub async fn resolve_target_queue(
    conn: &mut diesel_async::AsyncPgConnection,
    target_workflow_name: &str,
    target_shard: crate::types::ShardId,
) -> String {
    // If the target shard is 0 (the default shard), we can query it directly.
    if target_shard.as_i32() == 0 || target_shard.is_unencoded() {
        use crate::schema::harvest_schedules::dsl as sched_dsl;
        use diesel::prelude::*;
        use diesel_async::RunQueryDsl;
        if let Ok(Some(Some(q))) = sched_dsl::harvest_schedules
            .filter(sched_dsl::workflow_name.eq(target_workflow_name))
            .select(sched_dsl::queue_name)
            .first::<Option<String>>(conn)
            .await
            .optional()
        {
            return q;
        }
        return default_workflow_queue();
    }

    // Otherwise, acquire a connection to the configured default shard to query
    // the schedules. Schedules are created on the default shard; using a
    // hard-coded shard 0 would miss them in deployments where the default shard
    // is not 0.
    let default_pool = crate::shard::GLOBAL_SHARDED_POOL
        .read()
        .ok()
        .and_then(|p| p.clone())
        .map(|sp| {
            let shard = sp.default_shard();
            sp.pool_for(shard).clone()
        });

    if let Some(dp) = default_pool
        && let Ok(mut default_conn) = dp.get().await
    {
        use crate::schema::harvest_schedules::dsl as sched_dsl;
        use diesel::prelude::*;
        use diesel_async::RunQueryDsl;
        if let Ok(Some(Some(q))) = sched_dsl::harvest_schedules
            .filter(sched_dsl::workflow_name.eq(target_workflow_name))
            .select(sched_dsl::queue_name)
            .first::<Option<String>>(&mut default_conn)
            .await
            .optional()
        {
            return q;
        }
    }

    default_workflow_queue()
}

/// The shard-independent default start queue: the process-global registered
/// default (`GLOBAL_DEFAULT_WORKFLOW_QUEUE`), else the literal `"default"`.
///
/// This is the correct answer whenever a schedule-level queue override cannot be
/// read — it depends on no shard's `harvest_schedules` and so can never return a
/// wrong-shard queue.
#[cfg(feature = "db")]
fn default_workflow_queue() -> String {
    GLOBAL_DEFAULT_WORKFLOW_QUEUE
        .read()
        .ok()
        .and_then(|lock| lock.clone())
        .unwrap_or_else(|| "default".to_string())
}

/// Resolve the start queue for a **cross-shard** completion-trigger target.
///
/// Mirrors how `enforce_completion_triggers_outbox` resolves the queue at fire
/// time: on a fresh connection to the target shard's own pool, so the inline
/// admission-gate check (issue #618, F2 re-review) evaluates the exact queue the
/// deferred start will use.
///
/// The source transaction connection is **deliberately not a parameter** (issue
/// #618, F2 re-review round 2): the schedule-level queue override lives only on
/// the target shard, and resolving it on the source connection would query the
/// WRONG shard's `harvest_schedules` — for a target hashing to shard 0,
/// `resolve_target_queue`'s directly-queryable branch would read the source
/// connection and could return a wrong queue, silently mis-evaluating a Queue(q)
/// gate (the exact bug F2 fixed, reintroduced in the connection-failure edge).
/// When the target-shard pool/connection is unavailable we therefore fall back to
/// the shard-INDEPENDENT `default_workflow_queue()`, never to the source
/// connection. Fleet/name/owner/shard gates do not depend on the queue and still
/// evaluate correctly in the fallback. Bounded double-fault edge (documented): a
/// `Queue(schedule-override)` gate can be missed only while the target shard is
/// unreachable — strictly better than returning a wrong-shard queue.
#[cfg(feature = "db")]
pub async fn resolve_cross_shard_target_queue(
    target_workflow_name: &str,
    target_shard: crate::types::ShardId,
) -> String {
    let target_pool = crate::shard::GLOBAL_SHARDED_POOL
        .read()
        .ok()
        .and_then(|p| p.clone())
        .and_then(|sp| sp.exact_pool_for(target_shard).cloned());
    if let Some(tp) = target_pool {
        match tp.get().await {
            Ok(mut target_conn) => {
                return resolve_target_queue(&mut target_conn, target_workflow_name, target_shard)
                    .await;
            }
            Err(e) => tracing::warn!(
                error = %e,
                target_shard = target_shard.as_i32(),
                "could not acquire target-shard connection for cross-shard admission-gate \
                 queue resolution; falling back to the shard-independent default queue"
            ),
        }
    }
    default_workflow_queue()
}

/// Relay-time block: atomically DROP the outbox row and mark the existing fires
/// row `admission_blocked`, on the source-shard connection (issue #618, F-round10).
///
/// Both the outbox row (keyed by `outbox_id`) and the fires row (keyed by
/// `(source_exec_id, trigger_id)`) live on the source shard, so a single
/// source-shard transaction keeps the durable audit record and the row removal
/// consistent — there is no window where the outbox row is dropped but the fires
/// row still reports a successful fire.
///
/// Returns the number of outbox rows deleted (>= 1 iff THIS relay path owned the
/// row). The fires-row UPDATE is applied **only when the delete removed the row**,
/// so a row already claimed by the sibling relay path (immediate xor scanner) is
/// neither re-counted nor has its fires outcome clobbered — the DELETE gates both
/// the count (in the caller) and the fires-row UPDATE, giving exactly-once
/// block accounting and audit consistency across the two relay paths.
#[cfg(feature = "db")]
async fn relay_block_drop_outbox(
    source_conn: &mut diesel_async::AsyncPgConnection,
    outbox_id: Uuid,
    source_exec_id: Uuid,
    trigger_id: Uuid,
) -> Result<usize, diesel::result::Error> {
    use crate::schema::harvest_completion_trigger_fires::dsl as fires_dsl;
    use crate::schema::harvest_completion_trigger_outbox::dsl as outbox_dsl;
    use diesel::prelude::*;
    use diesel_async::{AsyncConnection, RunQueryDsl};

    source_conn
        .transaction(|tx| {
            Box::pin(async move {
                let deleted = diesel::delete(outbox_dsl::harvest_completion_trigger_outbox)
                    .filter(outbox_dsl::id.eq(outbox_id))
                    .execute(tx)
                    .await?;
                // Only mark the fires row blocked when this path actually removed the
                // outbox row; otherwise the sibling relay path already owns it (and may
                // have started the target, leaving the fires row correctly `NULL`).
                if deleted >= 1 {
                    diesel::update(
                        fires_dsl::harvest_completion_trigger_fires
                            .filter(fires_dsl::source_exec_id.eq(source_exec_id))
                            .filter(fires_dsl::trigger_id.eq(trigger_id)),
                    )
                    .set(fires_dsl::outcome.eq(Some("admission_blocked")))
                    .execute(tx)
                    .await?;
                }
                Ok(deleted)
            })
        })
        .await
}

/// Mark a cross-shard completion-trigger outbox row DELIVERED: delete the source
/// outbox row and, only if it was actually removed, count the
/// `completion_trigger_outbox` bypass (issue #618). The fires row is left as a real
/// fire (`NULL`) — a delivered target is a fire that ran, never a block. Shared by
/// the round-15 any-state short-circuit and the round-13 `Started` arm so the two
/// deliveries account identically.
#[cfg(feature = "db")]
async fn relay_mark_delivered(
    source_conn: &mut diesel_async::AsyncPgConnection,
    outbox_id: Uuid,
    metrics: Option<&(dyn crate::telemetry::MetricsRecorder + Send + Sync)>,
) {
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    let deleted = diesel::delete(crate::schema::harvest_completion_trigger_outbox::table)
        .filter(crate::schema::harvest_completion_trigger_outbox::dsl::id.eq(outbox_id))
        .execute(source_conn)
        .await;
    if matches!(deleted, Ok(n) if n >= 1)
        && let Some(m) = metrics
    {
        m.record_admission_bypassed(
            crate::admission_gate::StartProducer::CompletionTriggerOutbox.as_str(),
        );
    }
}

/// Run the cross-shard completion-trigger relay's start/block decision through the
/// existence-aware round-12 [`crate::execution::gate_checked_start_or_load`]
/// primitive (issue #618, F-round13), then apply the source-shard bookkeeping.
///
/// Closes the stale-row existence race: a row whose deterministic target execution
/// ALREADY exists (the immediate relay started the target but then FAILED to delete
/// the source outbox row, and the scanner retries it under a now-active gate) must
/// be treated as DELIVERED — not blocked. `gate_checked_start_or_load` locks the
/// target's existence through the gate/start decision on the target shard:
///
/// - target already exists (`Started { created: false }`) → DELIVERED: delete the
///   source outbox row + count the `completion_trigger_outbox` bypass, leaving the
///   fires row `NULL`/fired. NOT blocked, NOT marked `admission_blocked`.
/// - fresh + matching gate (`Blocked`) → [`relay_block_drop_outbox`] on the source
///   shard (delete outbox row + mark fires `admission_blocked`) + count the block.
/// - fresh + no gate (`Started { created: true }`) → delete outbox + count bypass.
///
/// Two-shard split: `gate_checked_start_or_load` handles the target-shard
/// existence-lock + gate + start atomically; the source outbox delete (+ fires mark
/// on block) is on the SOURCE shard. Exactly-once: the block XOR bypass count is
/// gated on the source-shard delete actually removing the row.
#[cfg(feature = "db")]
#[allow(clippy::too_many_arguments)]
async fn relay_gate_checked_start(
    target_conn: &mut diesel_async::AsyncPgConnection,
    source_conn: &mut diesel_async::AsyncPgConnection,
    params: crate::execution::StartWorkflowParams<'_>,
    gate_workflow_name: String,
    gate_resolved_queue: String,
    gate_owner: Option<String>,
    target_shard: crate::types::ShardId,
    outbox_id: Uuid,
    source_exec_id: Uuid,
    trigger_id: Uuid,
    metrics: Option<&(dyn crate::telemetry::MetricsRecorder + Send + Sync)>,
) -> crate::error::HarvestResult<()> {
    // F-round15: any-state existence = already DELIVERED (one-shot outbox row).
    //
    // A completion-trigger outbox row is a one-shot delivery marker: "trigger T
    // fired; start deterministic target W exactly once, then delete the row." The
    // scanner only re-sees a stale row after the immediate relay STARTED W but then
    // failed to delete the source row — so a stale row implies W was created. If W
    // has since sealed (`TERMINATED`/`CONTINUED_AS_NEW`) or completed
    // (`COMPLETED`/`FAILED`/`CANCELLED`/`TIMED_OUT`), the round-13 path below is
    // WRONG: `gate_checked_start_or_load`'s existence lock is active-only, so it
    // sees no live run → under a matching gate it BLOCKS and marks the fires row
    // `admission_blocked`, and with no gate a *sealed* target lets
    // `start_or_load` create a SECOND W — both re-doing an already-delivered fire.
    //
    // Guard it here, in the completion-trigger relay ONLY (NOT the webhook delegate,
    // whose sealed-prior re-delivery is a legitimate fresh gated start): an
    // any-state existence check on the target shard. Exists in ANY state → the fire
    // was delivered → delete the source outbox row + count the bypass, leaving the
    // fires row a real fire (`NULL`). Stable without a lock because exactly one
    // relay path runs per outbox row (rounds 6/7) — the only creator of THIS
    // deterministic W is THIS relay, so a `false` here cannot race a concurrent
    // create, and `gate_checked_start_or_load` below then owns the fresh
    // create-or-block. Does NOT touch `try_load_active_execution_for_update` /
    // `gate_checked_start_or_load` (correct as-is for the webhook + fresh-create
    // cases).
    if crate::execution::execution_exists_by_key(
        target_conn,
        params.workflow_name,
        params.workflow_id,
    )
    .await?
    {
        tracing::debug!(
            outbox_id = %outbox_id,
            workflow_name = %params.workflow_name,
            workflow_id = %params.workflow_id,
            "[completion_trigger] cross-shard relay: deterministic target already exists (any state); treating the stale outbox row as delivered"
        );
        relay_mark_delivered(source_conn, outbox_id, metrics).await;
        return Ok(());
    }

    // The relay-time gate decision, existence-aware: a LOCKED live target already
    // exists → this is a delivery/idempotent-load, NOT a fresh admission → skip the
    // gate; else re-check the gate on the real resolved queue. Uses `check_cached`
    // (the last-known snapshot, honoured even under a transient fail-closed blip,
    // never the sentinel) — NOT `check()` — because the relay materializes
    // pre-committed in-flight-continuation work, so the same no-permanent-drop
    // rationale as the completion-trigger inline path applies.
    // `will_create` (issue #618, F-round18): apply the gate iff `_collect` will
    // CREATE a new execution; skip it for an idempotent attach/no-op. The relay's
    // own any-state `execution_exists_by_key` check above already short-circuits a
    // stale outbox row whose target exists in ANY state to DELIVERED, so by the time
    // control reaches here NO target exists → `will_create` is trivially `true` and
    // the gate always applies (a fresh cross-shard delivery is a fresh admission).
    let gate = move |will_create: bool| -> Option<(String, &'static str)> {
        if !will_create {
            return None;
        }
        crate::admission_gate::global_admission_gate_cache().and_then(|cache| {
            cache
                .check_cached(
                    &gate_workflow_name,
                    &gate_resolved_queue,
                    target_shard.as_i32(),
                    gate_owner.as_deref(),
                )
                .map(|(_id, reason, scope_kind)| (reason, scope_kind))
        })
    };

    match crate::execution::gate_checked_start_or_load(target_conn, params, metrics, gate).await? {
        crate::execution::GateCheckedStart::Blocked { reason, scope_kind } => {
            tracing::info!(
                scope_kind,
                reason = %reason,
                "[completion_trigger] cross-shard relay blocked by an admission gate on the real target queue; dropping the outbox row"
            );
            match relay_block_drop_outbox(source_conn, outbox_id, source_exec_id, trigger_id).await
            {
                Ok(n) if n >= 1 => {
                    if let Some(m) = metrics {
                        m.record_admission_blocked(scope_kind, &reason);
                    }
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(
                    error = %e,
                    outbox_id = %outbox_id,
                    "[completion_trigger] relay-block drop failed; leaving the outbox row"
                ),
            }
        }
        crate::execution::GateCheckedStart::Started(_started) => {
            // Fresh start OR an idempotent attach to a still-active run: either way the
            // target IS started, so this is a DELIVERY — delete the source outbox row
            // and count the bypass (never a block). Fires stays NULL. (A sealed /
            // completed target was already short-circuited by the any-state check
            // above, so it never reaches this arm.)
            relay_mark_delivered(source_conn, outbox_id, metrics).await;
        }
    }
    Ok(())
}

#[cfg(feature = "db")]
#[derive(Debug, Clone)]
pub struct DeferredTriggerStart {
    pub outbox_id: Uuid,
    /// Source execution id — the fires-row key `(source_exec_id, trigger_id)` so a
    /// relay-time block can mark the existing fires row `admission_blocked`
    /// (issue #618, F-round10).
    pub source_exec_id: Uuid,
    /// Trigger id — the other half of the fires-row key.
    pub trigger_id: Uuid,
    pub source_shard: crate::types::ShardId,
    pub target_shard: crate::types::ShardId,
    pub target_workflow_name: String,
    pub target_workflow_id: String,
    pub target_input: Value,
    pub queue_name: Option<String>,
    pub concurrency_key: Option<String>,
    pub concurrency_limit: Option<u32>,
    pub priority: crate::types::Priority,
    pub max_workflow_input_bytes: u64,
    pub trigger_name: String,
    pub owner: Option<String>,
    pub runbook_url: Option<String>,
    pub severity: Option<String>,
    /// Resolved soft-SLA default (issue #487), converted to a fresh deadline at start.
    pub sla: Option<std::time::Duration>,
    /// Resolved workflow-type retry policy default (issue #523).
    pub retry_policy: Option<crate::policy::RetryPolicy>,
    /// Server-side ceiling on `max_attempts` (issue #523). Clamped at start time.
    pub max_workflow_attempts_ceiling: Option<u32>,
}

#[cfg(feature = "db")]
impl DeferredTriggerStart {
    // Pre-existing, unrelated to issue #605: this function sits just over the
    // 100-line clippy threshold on trunk already; allowed here rather than
    // refactored to keep this change scoped to completion callbacks.
    #[allow(clippy::too_many_lines)]
    pub fn spawn(self) {
        let Some(pool) = crate::shard::GLOBAL_SHARDED_POOL
            .read()
            .ok()
            .and_then(|p| p.clone())
            .and_then(|sp| sp.exact_pool_for(self.target_shard).cloned())
        else {
            tracing::error!(
                "[completion_trigger] GLOBAL_SHARDED_POOL is not initialized during spawn. Cannot start target cross-shard workflow."
            );
            return;
        };
        tokio::spawn(async move {
            let conn_res = pool.get().await;
            let mut target_conn = match conn_res {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!(
                        "[completion_trigger] Failed to get connection for shard {:?}: {:?}",
                        self.target_shard,
                        e
                    );
                    return;
                }
            };
            let queue_name = if let Some(ref q) = self.queue_name {
                q.clone()
            } else {
                resolve_target_queue(
                    &mut target_conn,
                    &self.target_workflow_name,
                    self.target_shard,
                )
                .await
            };

            // Existence-aware relay-time start/block via the round-12
            // `gate_checked_start_or_load` (issue #618, F-round13): the target-shard
            // existence-lock + gate + start are atomic, so a stale outbox row whose
            // target ALREADY exists (this relay started the target but then failed to
            // delete the source outbox row) is treated as DELIVERED on retry rather
            // than wrongly blocked. Uses the process-global recorder (spawn has no
            // explicit metrics param), mirroring the scanner path.
            let Some(source_pool) = crate::shard::GLOBAL_SHARDED_POOL
                .read()
                .ok()
                .and_then(|p| p.clone())
                .and_then(|sp| sp.exact_pool_for(self.source_shard).cloned())
            else {
                tracing::error!(
                    "[completion_trigger] GLOBAL_SHARDED_POOL missing source shard {:?}; leaving the outbox row for the scanner",
                    self.source_shard
                );
                return;
            };
            let mut source_conn = match source_pool.get().await {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!(
                        "[completion_trigger] Failed to get source-shard connection: {:?}; leaving the outbox row for the scanner",
                        e
                    );
                    return;
                }
            };

            let global_metrics = crate::admission_gate::global_admission_metrics();
            let metrics_ref = global_metrics
                .as_ref()
                .map(|m| m.as_ref() as &(dyn crate::telemetry::MetricsRecorder + Send + Sync));

            let gate_workflow_name = self.target_workflow_name.clone();
            let gate_resolved_queue = queue_name.clone();
            let gate_owner = self.owner.clone();
            let params = crate::execution::StartWorkflowParams {
                workflow_name: &self.target_workflow_name,
                workflow_id: &self.target_workflow_id,
                exec_id: crate::types::ExecutionId::new_for_shard(self.target_shard),
                input: self.target_input,
                parent_id: None,
                queue_name: &queue_name,
                execution_timeout: None,
                memo: None,
                search_attrs: None,
                reuse_policy: crate::types::WorkflowIdReusePolicy::AllowDuplicate,
                trace_context: None,
                max_execution_timeout_ceiling: None,
                concurrency_key: self.concurrency_key,
                concurrency_limit: self.concurrency_limit,
                priority: self.priority,
                max_workflow_input_bytes: self.max_workflow_input_bytes,
                start_at: None,
                delay: None,
                max_workflow_start_delay: None,
                owner: self.owner.as_deref(),
                runbook_url: self.runbook_url.as_deref(),
                severity: self.severity.as_deref(),
                context_headers: None,
                sla: self.sla.and_then(|d| chrono::Duration::from_std(d).ok()),
                schedule_id: None,
                scheduled_for: None,
                workflow_attempt: 1,
                workflow_retry_policy: self.retry_policy.clone(),
                retry_of_exec_id: None,
                max_workflow_attempts_ceiling: self.max_workflow_attempts_ceiling,
                origin: None,
                completion_callbacks: None,
            };

            if let Err(e) = relay_gate_checked_start(
                &mut target_conn,
                &mut source_conn,
                params,
                gate_workflow_name,
                gate_resolved_queue,
                gate_owner,
                self.target_shard,
                self.outbox_id,
                self.source_exec_id,
                self.trigger_id,
                metrics_ref,
            )
            .await
            {
                // A start error (e.g. PayloadTooLarge) leaves the outbox row for the
                // scanner to handle (which deletes an oversized-payload row).
                tracing::error!(
                    "[completion_trigger] Failed to start workflow execution cross-shard: {:?}",
                    e
                );
            }
        });
    }
}

#[allow(clippy::too_many_lines)]
#[cfg(feature = "db")]
pub fn evaluate_triggers_for_execution<'a>(
    conn: &'a mut diesel_async::AsyncPgConnection,
    exec_id: crate::types::ExecutionId,
    state: TerminalState,
    metrics: Option<&'a (dyn crate::telemetry::MetricsRecorder + Send + Sync)>,
) -> futures::future::BoxFuture<'a, crate::error::HarvestResult<Vec<DeferredTriggerStart>>> {
    use futures::FutureExt;
    async move {
        use diesel::prelude::*;
        use diesel_async::RunQueryDsl;
        use crate::schema::harvest_completion_triggers::dsl as triggers_dsl;
        use crate::schema::harvest_completion_trigger_fires::dsl as fires_dsl;
        use crate::schema::harvest_workflow_executions::dsl as execs_dsl;
        use crate::models::{CompletionTriggerDb, NewCompletionTriggerFireDb, WorkflowExecution, NewCompletionTriggerOutboxDb};
        use crate::execution::{StartWorkflowParams, start_or_load_workflow_execution};
        use crate::types::WorkflowIdReusePolicy;
        use crate::types::Priority;

        let mut deferred_starts = Vec::new();

        let execution = execs_dsl::harvest_workflow_executions
            .find(exec_id.as_uuid())
            .first::<WorkflowExecution>(conn)
            .await
            .optional()
            .map_err(crate::error::database_error)?;

        let Some(execution) = execution else {
            return Ok(deferred_starts);
        };

        // Durable completion callbacks (issue #605): enqueue any matching
        // delivery rows inside this same terminal transaction. Reuses the
        // execution row already loaded above; a no-op when no completion-
        // callback config has ever been registered
        // (`completion_callback::GLOBAL_CALLBACK_CONFIG` is `None`) or when
        // no target matches this terminal state.
        crate::completion_callback::enqueue_completion_deliveries(conn, &execution, exec_id, state)
            .await?;

        let triggers = triggers_dsl::harvest_completion_triggers
            .filter(triggers_dsl::source_workflow_name.eq(&execution.workflow_name))
            .load::<CompletionTriggerDb>(conn)
            .await
            .map_err(crate::error::database_error)?;

        for trigger_db in triggers {
            let terminal_states: Vec<TerminalState> = serde_json::from_value(trigger_db.terminal_states)
                .unwrap_or_default();

            if !terminal_states.contains(&state) {
                continue;
            }

            let trigger_name = trigger_db.id.to_string();
            let source_output = execution.output.clone().unwrap_or(Value::Null);

            // Output guard (issue #810): evaluated against the RAW source
            // output — deliberately before (and independent of) input
            // mapping, so issue #748 can later change input assembly without
            // touching guard semantics. Pure and deterministic: the same
            // recorded output yields the same fire/skip decision on every
            // redelivery. An UNMET guard is a final decision over recorded
            // output and is recorded as resolved through the same
            // ON CONFLICT DO NOTHING fires-row PK path as a real fire (AC5),
            // so cascade re-entry dedupes identically. An unparseable or
            // over-cap stored condition FAILS CLOSED: skip with the
            // distinct `condition_invalid` reason and NO fires row
            // (mirroring the `validation_failed` precedent below) — a
            // "this binary cannot understand the guard" state must never
            // durably resolve the pair, so a re-entry into evaluation
            // re-evaluates it once the fleet is upgraded or the operator
            // repairs the condition. Honest recovery contract (PR #972
            // review): evaluation runs only inside a source terminal
            // commit and no scanner re-visits unresolved pairs, so such a
            // re-entry is NOT guaranteed — without one the fire is lost
            // for this terminal; the warn + counter below are the
            // operator's detection signal (see ConditionGate::Invalid).
            // Neither case fires or errors the terminal commit.
            match gate_stored_condition(trigger_db.condition.as_ref(), &source_output) {
                ConditionGate::Pass => {}
                ConditionGate::Invalid => {
                    tracing::warn!(
                        trigger_id = %trigger_db.id,
                        source_exec_id = %exec_id,
                        "Completion trigger has an unparseable/invalid stored condition; \
                         failing closed (no fires row — the fire is lost for this terminal \
                         unless evaluation re-enters; re-trigger the target by hand)."
                    );
                    if let Some(m) = metrics {
                        m.record_completion_trigger_skipped(&trigger_name, "condition_invalid");
                    }
                    continue;
                }
                ConditionGate::Unmet => {
                    let inserted = diesel::insert_into(fires_dsl::harvest_completion_trigger_fires)
                        .values(&NewCompletionTriggerFireDb {
                            source_exec_id: exec_id.as_uuid(),
                            trigger_id: trigger_db.id,
                            outcome: Some("condition_unmet".to_string()),
                        })
                        .on_conflict_do_nothing()
                        .execute(conn)
                        .await
                        .map_err(crate::error::database_error)?;
                    if let Some(m) = metrics {
                        if inserted > 0 {
                            m.record_completion_trigger_skipped(&trigger_name, "condition_unmet");
                        } else {
                            // Redelivery of an already-resolved (fired or
                            // skipped) pair — same dedupe signal as today.
                            m.record_completion_trigger_fired(&trigger_name, "deduped");
                        }
                    }
                    continue;
                }
            }

            let input_mapping: InputMapping = serde_json::from_value(trigger_db.input_mapping)
                .unwrap_or(InputMapping::Passthrough);

            let target_input = match input_mapping {
                InputMapping::Passthrough => source_output,
                InputMapping::Static(v) => v,
                InputMapping::Projection(ref path) => project_json_path(&source_output, path),
                // Issue #748: assemble the terminal-outcome envelope from the
                // already-loaded execution row. `output` is threaded only on a
                // COMPLETED source (else NULL); `error` carries the failure /
                // cancel reason. This single `target_input` flows to BOTH the
                // same-shard start and the cross-shard outbox row below, so
                // inline/outbox parity (AC5) holds by construction.
                InputMapping::Outcome { ref projection } => {
                    // Reuse the already-owned `source_output` clone (the guard's
                    // clone from above) on a COMPLETED source instead of
                    // re-borrowing `execution.output` and cloning a second time
                    // inside the builder. `source_output` is unused by this arm
                    // otherwise, so moving it here is free.
                    let output_for_env = if matches!(state, TerminalState::Completed) {
                        Some(source_output)
                    } else {
                        None
                    };
                    let envelope = build_outcome_envelope(
                        exec_id,
                        state,
                        &execution.workflow_id,
                        &execution.workflow_name,
                        output_for_env,
                        execution.error.as_deref(),
                    );
                    match projection {
                        Some(path) => project_json_path(&envelope, path),
                        None => envelope,
                    }
                }
            };

            let target_workflow_id = format!("completion-trigger-{}-{}", trigger_db.id, exec_id);

            // Validate mapped inputs against the target schema
            if let Some(ref target_schema_fn) = {
                let lock = GLOBAL_WORKFLOW_METADATA.read().ok();
                lock.as_ref()
                    .and_then(|guard| guard.as_ref())
                    .and_then(|meta_map| meta_map.get(&trigger_db.target_workflow_name))
                    .and_then(|meta| meta.input_schema)
            } {
                let schema = target_schema_fn();
                if let Err(violations) = crate::info::validate_against_schema(&schema, &target_input) {
                    tracing::warn!(
                        trigger_id = %trigger_db.id,
                        target_workflow_name = %trigger_db.target_workflow_name,
                        violations = ?violations,
                        "Completion trigger input validation failed; skipping trigger execution."
                    );
                    if let Some(m) = metrics {
                        m.record_completion_trigger_fired(&trigger_db.id.to_string(), "validation_failed");
                    }
                    continue;
                }
            }

            // Resolve routing + target metadata BEFORE the fires-row insert so
            // the admission-gate check (issue #618) can evaluate every scope
            // (Fleet/WorkflowName/Queue/ShardId/Owner) and record the correct
            // resolved outcome.
            let router = crate::shard::GLOBAL_SHARD_ROUTER
                .read()
                .ok()
                .and_then(|guard| guard.as_ref().cloned())
                .ok_or_else(|| {
                    tracing::error!("[completion_trigger] GLOBAL_SHARD_ROUTER is not initialized.");
                    crate::error::database_error(diesel::result::Error::RollbackTransaction)
                })?;
            let target_shard = router.pick_for_new_workflow(&trigger_db.target_workflow_name, &target_workflow_id);
            let source_shard = router.shard_for_execution(exec_id);

            // Resolve target metadata (owner, runbook_url, severity, sla, retry_policy)
            let (target_owner, target_runbook_url, target_severity, target_sla, target_retry_policy) = {
                let lock = GLOBAL_WORKFLOW_METADATA.read().ok();
                lock.as_ref()
                    .and_then(|guard| guard.as_ref())
                    .and_then(|meta_map| meta_map.get(&trigger_db.target_workflow_name))
                    .map_or((None, None, None, None, None), |meta| {
                        (
                            meta.owner.clone(),
                            meta.runbook_url.clone(),
                            meta.severity.clone(),
                            meta.sla,
                            meta.retry_policy.clone(),
                        )
                    })
            };

            // issue #618: completion triggers are an in-process start producer
            // and MUST honour an active admission gate. This runs INSIDE the
            // source workflow's terminal-commit transaction, so a block is a
            // CLEAN SKIP that NEVER rolls back the source's terminal commit —
            // mirroring the #810 `condition_unmet` resolved-skip shape. The
            // blocked start is DROPPED, not deferred: identically to a direct
            // API start being refused during an incident. The resolved skip is
            // recorded through the same ON CONFLICT DO NOTHING fires-row PK path
            // a real fire uses (exactly-once, so cascade re-entry dedupes it and
            // this terminal never retries the start), with the distinct
            // `admission_blocked` outcome. When no gate cache is installed (the
            // common path / standalone integrations) the check is skipped and
            // resolution below is byte-identical to pre-#618.
            //
            // Fail-closed handling (issue #618 F3 + Codex P1): consult the
            // LAST-KNOWN cached snapshot via `check_cached`, which ignores the
            // fail-closed/initialized flag and returns only a REAL matching gate
            // (never the nil sentinel). `set_fail_closed()` retains the snapshot,
            // so a gate loaded before a transient gate-DB read error still blocks
            // a matching completion-trigger start (block+drop+count) — otherwise
            // completion triggers would BYPASS an active gate uncounted during
            // exactly the incident the gate exists for, while direct API starts
            // still block. When no cached gate matches (empty snapshot in the boot
            // window, or a transient blip with no gate targeting this start) we
            // PROCEED rather than permanently dropping in-flight continuation (a
            // completion-trigger start cannot retry, unlike an API caller). This
            // is a deliberate, documented divergence from the API path: when
            // initialized-and-healthy both see identical gates; under fail-closed
            // the API blocks everything (caller retries) while triggers honour
            // only known-cached gates. Bounded caveat: a gate ADDED during a
            // gate-DB outage (never loaded into the snapshot) won't block triggers
            // until the next successful refresh.
            // Resolve the target queue at most ONCE per trigger (issue #618, F9):
            // the gate check below and the same-shard start further down both
            // need it. `None` = not yet resolved (no gate cache installed, so the
            // same-shard branch resolves it lazily as before).
            let mut resolved_queue: Option<String> = None;
            if let Some(cache) = crate::admission_gate::global_admission_gate_cache() {
                let gate_queue = if let Some(ref q) = trigger_db.queue_name {
                    q.clone()
                } else if target_shard == source_shard {
                    // Same-shard target: the source transaction connection IS the
                    // target shard's connection, so resolve directly — no extra
                    // DB round-trip (preserves the no-gate/same-shard common-case
                    // property; F9's single-resolution reuse below still applies).
                    resolve_target_queue(conn, &trigger_db.target_workflow_name, target_shard).await
                } else {
                    // Cross-shard target (issue #618, F2 re-review): the outbox
                    // relay resolves the ACTUAL start queue on the TARGET
                    // connection at fire time (see
                    // `enforce_completion_triggers_outbox`). Resolve identically
                    // here — on a fresh target-shard connection — so the inline
                    // gate check evaluates the SAME queue the start will use.
                    // `resolve_cross_shard_target_queue` deliberately takes NO
                    // source connection: for a target hashing to shard 0, querying
                    // `harvest_schedules` on the source `conn` would read the wrong
                    // shard and could miss a Queue(q) gate scoped to the real
                    // target queue. If the target-shard pool is unavailable it
                    // falls back to the shard-independent default queue (never the
                    // source connection); Fleet/name gates still match — only a
                    // schedule-override Queue gate could be missed in that
                    // double-fault edge.
                    resolve_cross_shard_target_queue(&trigger_db.target_workflow_name, target_shard)
                        .await
                };
                resolved_queue = Some(gate_queue.clone());
                if let Some((_gate_id, reason, scope_kind)) = cache.check_cached(
                    &trigger_db.target_workflow_name,
                    &gate_queue,
                    target_shard.as_i32(),
                    target_owner.as_deref(),
                ) {
                    tracing::info!(
                        trigger_id = %trigger_db.id,
                        source_exec_id = %exec_id,
                        target_workflow_name = %trigger_db.target_workflow_name,
                        scope_kind = %scope_kind,
                        reason = %reason,
                        "Completion trigger start blocked by a cached admission gate; \
                         dropping the trigger start (recorded once, not retried)."
                    );
                    let blocked_inserted =
                        diesel::insert_into(fires_dsl::harvest_completion_trigger_fires)
                            .values(&NewCompletionTriggerFireDb {
                                source_exec_id: exec_id.as_uuid(),
                                trigger_id: trigger_db.id,
                                outcome: Some("admission_blocked".to_string()),
                            })
                            .on_conflict_do_nothing()
                            .execute(conn)
                            .await
                            .map_err(crate::error::database_error)?;
                    // Resolve the recorder: the caller's explicit param when
                    // supplied (worker / timeout paths), else the process-global
                    // recorder the plugin publishes at boot (issue #618, F-round5).
                    // The cancel / terminate / parent-close-cascade paths in
                    // `execution.rs` all call this with `metrics: None`, so without
                    // the fallback a completion-trigger start blocked by a gate on
                    // one of those terminal paths would be dropped-and-recorded but
                    // NEVER counted — a hole in the "zero un-counted blocks" bar.
                    let fallback_recorder = if metrics.is_none() {
                        crate::admission_gate::global_admission_metrics()
                    } else {
                        None
                    };
                    // A tuple `match` (rather than `if let ... else`) so the
                    // `&dyn MetricsRecorder` -> annotated `+ Send + Sync` coercion
                    // still applies at the `Some(g.as_ref())` expression site (the
                    // trait has both supertraits), matching how the worker passes
                    // `registry.telemetry().metrics.as_ref()`.
                    let effective_metrics: Option<
                        &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
                    > = match (metrics, fallback_recorder.as_ref()) {
                        (Some(m), _) => Some(m),
                        (None, Some(g)) => Some(g.as_ref()),
                        (None, None) => None,
                    };
                    if let Some(m) = effective_metrics {
                        // Only count the FIRST resolution of this (source, trigger)
                        // pair (issue #618, F4): cascade re-entry / multi-terminal-
                        // path re-eval hits ON CONFLICT DO NOTHING (inserted == 0)
                        // and must not double-count. Mirrors the sibling
                        // `condition_unmet` -> `deduped` shape.
                        if blocked_inserted > 0 {
                            m.record_admission_blocked(scope_kind, &reason);
                        } else {
                            m.record_completion_trigger_fired(&trigger_name, "deduped");
                        }
                    }
                    continue;
                }
            }

            let inserted = diesel::insert_into(fires_dsl::harvest_completion_trigger_fires)
                .values(&NewCompletionTriggerFireDb {
                    source_exec_id: exec_id.as_uuid(),
                    trigger_id: trigger_db.id,
                    // NULL outcome = fired (issue #810 reserves the column
                    // for resolved-skip reasons).
                    outcome: None,
                })
                .on_conflict_do_nothing()
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;

            if inserted == 0 {
                if let Some(m) = metrics {
                    m.record_completion_trigger_fired(&trigger_name, "deduped");
                }
                continue;
            }

            // Resolve target concurrency parameters
            let (concurrency_key, concurrency_limit) = {
                let lock = GLOBAL_WORKFLOW_METADATA.read().ok();
                lock.as_ref()
                    .and_then(|guard| guard.as_ref())
                    .and_then(|meta_map| meta_map.get(&trigger_db.target_workflow_name))
                    .and_then(|meta| meta.concurrency.as_ref())
                    .map_or((None, None), |policy| {
                        let key = crate::concurrency::resolve_concurrency_key(policy.key_expr, &target_input);
                        (key, Some(policy.limit))
                    })
            };

            // Resolve target input caps
            let max_workflow_input_bytes = {
                let global_default = GLOBAL_MAX_WORKFLOW_INPUT_BYTES.read().as_deref().copied().unwrap_or(crate::builder::DEFAULT_MAX_WORKFLOW_INPUT_BYTES);
                let lock = GLOBAL_WORKFLOW_METADATA.read().ok();
                lock.as_ref()
                    .and_then(|guard| guard.as_ref())
                    .and_then(|meta_map| meta_map.get(&trigger_db.target_workflow_name))
                    .and_then(|meta| meta.max_input_bytes)
                    .map_or(global_default, |per_wf| per_wf.max(global_default))
            };

            let max_workflow_attempts_ceiling =
                GLOBAL_MAX_WORKFLOW_ATTEMPTS_CEILING.read().ok().and_then(|g| *g);

            if target_shard == source_shard {
                // Reuse the queue already resolved for the gate check (issue #618,
                // F9); only resolve here when no gate cache was installed.
                let queue_name = if let Some(q) = resolved_queue.take() {
                    q
                } else if let Some(ref q) = trigger_db.queue_name {
                    q.clone()
                } else {
                    resolve_target_queue(conn, &trigger_db.target_workflow_name, target_shard).await
                };

                let target_exec_id = crate::types::ExecutionId::new_for_shard(target_shard);
                let start_res = match start_or_load_workflow_execution(
                    conn,
                    StartWorkflowParams {
                        workflow_name: &trigger_db.target_workflow_name,
                        workflow_id: &target_workflow_id,
                        exec_id: target_exec_id,
                        input: target_input,
                        parent_id: None,
                        queue_name: &queue_name,
                        execution_timeout: None,
                        memo: None,
                        search_attrs: None,
                        reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
                        trace_context: None,
                        max_execution_timeout_ceiling: None,
                        concurrency_key,
                        concurrency_limit,
                        priority: Priority::default(),
                        max_workflow_input_bytes,
                        start_at: None,
                        delay: None,
                        max_workflow_start_delay: None,
                        owner: target_owner.as_deref(),
                        runbook_url: target_runbook_url.as_deref(),
                        severity: target_severity.as_deref(),
                        context_headers: None,
                        sla: target_sla.and_then(|d| chrono::Duration::from_std(d).ok()),
                        schedule_id: None,
                        scheduled_for: None,
                        workflow_attempt: 1,
                        workflow_retry_policy: target_retry_policy.clone(),
                        retry_of_exec_id: None,
                        max_workflow_attempts_ceiling,
                        // Completion-trigger start is not a schedule fire (issue #534).
                        origin: None,
                        completion_callbacks: None,
                    },
                )
                .await
                {
                    Ok(res) => res,
                    Err(crate::error::HarvestError::PayloadTooLarge {
                        kind,
                        observed_bytes,
                        cap_bytes,
                        workflow_type,
                        ..
                    }) => {
                        tracing::warn!(
                            trigger_id = %trigger_db.id,
                            target_workflow_name = %trigger_db.target_workflow_name,
                            kind = %kind,
                            observed_bytes = observed_bytes,
                            cap_bytes = cap_bytes,
                            workflow_type = %workflow_type,
                            "Oversized trigger input payload; skipping trigger execution."
                        );
                        if let Some(m) = metrics {
                            m.record_completion_trigger_fired(&trigger_name, "payload_too_large");
                        }
                        continue;
                    }
                    Err(e) => return Err(e),
                };

                if let Some(m) = metrics {
                    if start_res.created {
                        m.record_completion_trigger_fired(&trigger_name, "started");
                    } else {
                        m.record_completion_trigger_fired(&trigger_name, "skipped");
                    }
                }
            } else {
                // Verify cross-shard database pool is configured before proceeding
                let _pool = {
                    let lock = crate::shard::GLOBAL_SHARDED_POOL.read();
                    lock.ok().and_then(|p| p.clone()).and_then(|sp| sp.exact_pool_for(target_shard).cloned())
                }.ok_or_else(|| {
                    tracing::error!("[completion_trigger] GLOBAL_SHARDED_POOL is not initialized or does not have shard {}.", target_shard);
                    crate::error::database_error(diesel::result::Error::RollbackTransaction)
                })?;

                if let Some(m) = metrics {
                    m.record_completion_trigger_fired(&trigger_name, "started");
                }

                let outbox_row = diesel::insert_into(crate::schema::harvest_completion_trigger_outbox::table)
                    .values(&NewCompletionTriggerOutboxDb {
                        source_exec_id: exec_id.as_uuid(),
                        trigger_id: trigger_db.id,
                        target_shard: target_shard.as_i32(),
                        target_workflow_name: trigger_db.target_workflow_name.clone(),
                        target_workflow_id: target_workflow_id.clone(),
                        target_input: target_input.clone(),
                        queue_name: trigger_db.queue_name.clone(),
                        concurrency_key: concurrency_key.clone(),
                        concurrency_limit: concurrency_limit.map(|l| i32::try_from(l).unwrap_or(i32::MAX)),
                        priority: serde_json::to_value(Priority::default()).unwrap_or(Value::Null),
                        max_workflow_input_bytes: i64::try_from(max_workflow_input_bytes).unwrap_or(i64::MAX),
                    })
                    .get_result::<crate::models::CompletionTriggerOutboxDb>(conn)
                    .await
                    .map_err(crate::error::database_error)?;

                deferred_starts.push(DeferredTriggerStart {
                    outbox_id: outbox_row.id,
                    source_exec_id: exec_id.as_uuid(),
                    trigger_id: trigger_db.id,
                    source_shard,
                    target_shard,
                    target_workflow_name: trigger_db.target_workflow_name.clone(),
                    target_workflow_id,
                    target_input,
                    queue_name: trigger_db.queue_name.clone(),
                    concurrency_key,
                    concurrency_limit,
                    priority: Priority::default(),
                    max_workflow_input_bytes,
                    trigger_name,
                    owner: target_owner,
                    runbook_url: target_runbook_url,
                    severity: target_severity,
                    sla: target_sla,
                    retry_policy: target_retry_policy,
                    max_workflow_attempts_ceiling,
                });
            }
        }

        Ok(deferred_starts)
    }
    .boxed()
}

/// Enforces pending completion triggers outbox tasks.
///
/// # Errors
///
/// Returns an error if any database operations fail.
#[cfg(feature = "db")]
#[allow(clippy::too_many_lines)]
pub async fn enforce_completion_triggers_outbox(
    conn: &mut diesel_async::AsyncPgConnection,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
    sharded_pool: &Option<crate::shard::ShardedDbPool>,
    shard_assignments: &[crate::types::ShardId],
) -> crate::error::HarvestResult<usize> {
    use crate::models::CompletionTriggerOutboxDb;
    use crate::schema::harvest_completion_trigger_outbox::dsl as outbox_dsl;
    use crate::types::Priority;
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    let shards: Vec<i32> = if shard_assignments.is_empty() {
        vec![0]
    } else {
        shard_assignments.iter().map(|s| s.as_i32()).collect()
    };

    // Load up to 50 pending outbox tasks for shards assigned to this worker.
    let pending_tasks = outbox_dsl::harvest_completion_trigger_outbox
        .filter(outbox_dsl::target_shard.eq_any(&shards))
        .limit(50)
        .load::<CompletionTriggerOutboxDb>(conn)
        .await
        .map_err(crate::error::database_error)?;

    if pending_tasks.is_empty() {
        return Ok(0);
    }

    let mut processed_count = 0;
    for task in pending_tasks {
        let target_shard = crate::types::ShardId::new(task.target_shard);
        let Some(target_pool) = sharded_pool
            .as_ref()
            .and_then(|sp| sp.exact_pool_for(target_shard).cloned())
        else {
            continue;
        };

        let mut target_conn = match target_pool.get().await {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(
                    "[completion_trigger outbox] Failed to get connection to target shard {:?}: {:?}",
                    target_shard,
                    e
                );
                continue;
            }
        };

        let queue_name = if let Some(ref q) = task.queue_name {
            q.clone()
        } else {
            resolve_target_queue(&mut target_conn, &task.target_workflow_name, target_shard).await
        };

        let priority: Priority = serde_json::from_value(task.priority).unwrap_or_default();

        let (target_owner, target_runbook_url, target_severity, target_sla, target_retry_policy) = {
            let lock = GLOBAL_WORKFLOW_METADATA.read().ok();
            lock.as_ref()
                .and_then(|guard| guard.as_ref())
                .and_then(|meta_map| meta_map.get(&task.target_workflow_name))
                .map_or((None, None, None, None, None), |meta| {
                    (
                        meta.owner.clone(),
                        meta.runbook_url.clone(),
                        meta.severity.clone(),
                        meta.sla,
                        meta.retry_policy.clone(),
                    )
                })
        };
        let max_workflow_attempts_ceiling = GLOBAL_MAX_WORKFLOW_ATTEMPTS_CEILING
            .read()
            .ok()
            .and_then(|g| *g);

        let gate_workflow_name = task.target_workflow_name.clone();
        let gate_resolved_queue = queue_name.clone();
        let gate_owner = target_owner.clone();
        let params = crate::execution::StartWorkflowParams {
            workflow_name: &task.target_workflow_name,
            workflow_id: &task.target_workflow_id,
            exec_id: crate::types::ExecutionId::new_for_shard(target_shard),
            input: task.target_input,
            parent_id: None,
            queue_name: &queue_name,
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: crate::types::WorkflowIdReusePolicy::AllowDuplicate,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            concurrency_key: task.concurrency_key,
            concurrency_limit: task
                .concurrency_limit
                .map(|l| u32::try_from(l).unwrap_or(0)),
            priority,
            max_workflow_input_bytes: u64::try_from(task.max_workflow_input_bytes).unwrap_or(0),
            start_at: None,
            delay: None,
            max_workflow_start_delay: None,
            owner: target_owner.as_deref(),
            runbook_url: target_runbook_url.as_deref(),
            severity: target_severity.as_deref(),
            context_headers: None,
            sla: target_sla.and_then(|d| chrono::Duration::from_std(d).ok()),
            schedule_id: None,
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: target_retry_policy,
            retry_of_exec_id: None,
            max_workflow_attempts_ceiling,
            // Completion-trigger start is not a schedule fire (issue #534).
            origin: None,
            // Internal start: only builder-wide default callback targets apply
            // (issue #605); a completion-trigger target has no per-execution
            // callback option of its own.
            completion_callbacks: None,
        };

        // Existence-aware relay-time start/block via `gate_checked_start_or_load`
        // (issue #618, F-round13): identical to the immediate `spawn()` relay, and
        // now robust to a STALE outbox row whose target ALREADY exists (the
        // immediate relay started the target but then failed to delete the source
        // row). On this retry the locked-live target is treated as DELIVERED (delete
        // outbox + count bypass), never wrongly blocked/dropped. A fresh target with
        // a matching gate is blocked (drop outbox + mark fires `admission_blocked` +
        // count block); a fresh target with no gate is a counted bypass. All gated on
        // the source-shard delete for exactly-once vs the immediate relay. `conn` is
        // the source shard.
        match relay_gate_checked_start(
            &mut target_conn,
            conn,
            params,
            gate_workflow_name,
            gate_resolved_queue,
            gate_owner,
            target_shard,
            task.id,
            task.source_exec_id,
            task.trigger_id,
            Some(metrics),
        )
        .await
        {
            Ok(()) => processed_count += 1,
            Err(crate::error::HarvestError::PayloadTooLarge {
                kind,
                observed_bytes,
                cap_bytes,
                ..
            }) => {
                // Permanent error: payload will never fit regardless of retries.
                // Delete the outbox row so it does not retry forever.
                tracing::error!(
                    target_workflow = %task.target_workflow_name,
                    kind = %kind,
                    observed_bytes,
                    cap_bytes,
                    "[completion_trigger outbox] permanent error: oversized input payload; deleting outbox row"
                );
                let _ = diesel::delete(outbox_dsl::harvest_completion_trigger_outbox)
                    .filter(outbox_dsl::id.eq(task.id))
                    .execute(conn)
                    .await;
                processed_count += 1;
            }
            Err(e) => {
                tracing::error!(
                    "[completion_trigger outbox] Failed to start workflow execution cross-shard: {:?}",
                    e
                );
            }
        }
    }

    Ok(processed_count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_project_json_path() {
        let val = json!({
            "a": {
                "b": {
                    "c": 42
                }
            },
            "array": [1, 2, 3]
        });

        assert_eq!(project_json_path(&val, "a.b.c"), json!(42));
        assert_eq!(project_json_path(&val, "a.b"), json!({"c": 42}));
        assert_eq!(project_json_path(&val, "a.x"), Value::Null);
        assert_eq!(project_json_path(&val, ""), val);
    }

    #[test]
    fn terminal_state_terminated_round_trips() {
        // issue #504: `Terminated` is a distinct terminal class from `Cancelled`.
        assert_eq!(TerminalState::Terminated.as_str(), "TERMINATED");
        assert_eq!(
            TerminalState::from_str("TERMINATED"),
            Some(TerminalState::Terminated)
        );
        assert_ne!(TerminalState::Terminated, TerminalState::Cancelled);

        // String round-trip for every variant, including the new one.
        for state in [
            TerminalState::Completed,
            TerminalState::Failed,
            TerminalState::Cancelled,
            TerminalState::TimedOut,
            TerminalState::Terminated,
        ] {
            assert_eq!(TerminalState::from_str(state.as_str()), Some(state));
        }

        // Pre-#504 stored trigger rows hold only the original four values; the
        // additive variant must not change their deserialization.
        let legacy: Vec<TerminalState> =
            serde_json::from_value(json!(["Completed", "Failed", "Cancelled", "TimedOut"]))
                .unwrap();
        assert_eq!(
            legacy,
            vec![
                TerminalState::Completed,
                TerminalState::Failed,
                TerminalState::Cancelled,
                TerminalState::TimedOut,
            ]
        );
        // And the new value is accepted by the same serde path the management API uses.
        let new: Vec<TerminalState> = serde_json::from_value(json!(["Terminated"])).unwrap();
        assert_eq!(new, vec![TerminalState::Terminated]);
    }

    // ── issue #748: Outcome input-mapping envelope ─────────────────────────

    #[test]
    fn outcome_envelope_failed_has_error_and_null_output() {
        let exec_id = crate::types::ExecutionId::new();
        let env = build_outcome_envelope(
            exec_id,
            TerminalState::Failed,
            "order-42",
            "fulfill_order",
            None,
            Some("boom"),
        );
        assert_eq!(env["terminal_state"], json!("FAILED"));
        assert_eq!(env["error"], json!("boom"));
        assert_eq!(env["output"], Value::Null);
        assert_eq!(env["source_exec_id"], json!(exec_id.to_string()));
        assert_eq!(env["source_workflow_id"], json!("order-42"));
        assert_eq!(env["source_workflow_name"], json!("fulfill_order"));
    }

    #[test]
    fn outcome_envelope_completed_has_output_and_null_error() {
        let exec_id = crate::types::ExecutionId::new();
        let output = json!({"shipped": true, "carrier": "acme"});
        let env = build_outcome_envelope(
            exec_id,
            TerminalState::Completed,
            "order-7",
            "fulfill_order",
            Some(output.clone()),
            None,
        );
        assert_eq!(env["terminal_state"], json!("COMPLETED"));
        assert_eq!(env["output"], output);
        assert_eq!(env["error"], Value::Null);
        assert_eq!(env["source_workflow_id"], json!("order-7"));
    }

    #[test]
    fn outcome_envelope_timed_out_and_cancelled_carry_reason() {
        let exec_id = crate::types::ExecutionId::new();
        let timed_out = build_outcome_envelope(
            exec_id,
            TerminalState::TimedOut,
            "wid",
            "wf",
            None,
            Some("deadline exceeded"),
        );
        assert_eq!(timed_out["terminal_state"], json!("TIMED_OUT"));
        assert_eq!(timed_out["error"], json!("deadline exceeded"));
        assert_eq!(timed_out["output"], Value::Null);

        let cancelled = build_outcome_envelope(
            exec_id,
            TerminalState::Cancelled,
            "wid",
            "wf",
            None,
            Some("operator cancel: fraud review"),
        );
        assert_eq!(cancelled["terminal_state"], json!("CANCELLED"));
        assert_eq!(cancelled["error"], json!("operator cancel: fraud review"));
        assert_eq!(cancelled["output"], Value::Null);
    }

    #[test]
    fn outcome_mapping_projection_plucks_field() {
        // Drives the same logic the inline `Outcome { projection }` arm uses:
        // build the envelope, then project a dotted path out of it.
        let exec_id = crate::types::ExecutionId::new();
        let env = build_outcome_envelope(
            exec_id,
            TerminalState::Failed,
            "wid",
            "wf",
            None,
            Some("boom"),
        );
        assert_eq!(project_json_path(&env, "error"), json!("boom"));
        assert_eq!(project_json_path(&env, "terminal_state"), json!("FAILED"));
    }

    #[test]
    fn outcome_mapping_none_yields_full_envelope() {
        let exec_id = crate::types::ExecutionId::new();
        let env = build_outcome_envelope(
            exec_id,
            TerminalState::Failed,
            "wid",
            "wf",
            None,
            Some("boom"),
        );
        // `projection = None` means the whole envelope is the target input.
        let obj = env.as_object().expect("envelope is an object");
        assert_eq!(obj.len(), 6);
        for key in [
            "terminal_state",
            "source_exec_id",
            "source_workflow_id",
            "source_workflow_name",
            "output",
            "error",
        ] {
            assert!(obj.contains_key(key), "envelope missing key {key}");
        }
    }

    #[test]
    fn outcome_mapping_projection_missing_path_yields_null() {
        // Graceful degradation for the `Outcome` variant: a projection path
        // that doesn't exist in the envelope resolves to `Null` (same as the
        // inline arm's `project_json_path(&envelope, path)`), never an error.
        let exec_id = crate::types::ExecutionId::new();
        let env = build_outcome_envelope(
            exec_id,
            TerminalState::Failed,
            "wid",
            "wf",
            None,
            Some("boom"),
        );
        assert_eq!(project_json_path(&env, "nonexistent.path"), Value::Null);
    }

    #[test]
    fn outcome_envelope_terminated_state() {
        // TERMINATED is a valid trigger-firing terminal state (issue #504) not
        // named in AC1 but must produce a coherent envelope: the terminate
        // reason lands in `error`, output is null.
        let exec_id = crate::types::ExecutionId::new();
        let env = build_outcome_envelope(
            exec_id,
            TerminalState::Terminated,
            "wid",
            "wf",
            None,
            Some("operator terminate: runaway loop"),
        );
        assert_eq!(env["terminal_state"], json!("TERMINATED"));
        assert_eq!(env["error"], json!("operator terminate: runaway loop"));
        assert_eq!(env["output"], Value::Null);
    }

    #[test]
    fn input_mapping_outcome_serde_round_trip() {
        // Adjacently-tagged encoding for the new variant (both projection shapes).
        // `projection = None` skips the field entirely (FIX 2: `#[serde(default,
        // skip_serializing_if = "Option::is_none")]`), matching how the sibling
        // `condition` field is handled — cleaner than a serialized `null`.
        let none = InputMapping::Outcome { projection: None };
        let none_v = serde_json::to_value(&none).unwrap();
        assert_eq!(none_v, json!({"type": "Outcome", "data": {}}));
        assert_eq!(
            serde_json::from_value::<InputMapping>(none_v).unwrap(),
            none
        );
        // Both the omitted-field form and an explicit `null` deserialize back
        // to `None` (`#[serde(default)]`), so a client may omit `projection`.
        assert_eq!(
            serde_json::from_value::<InputMapping>(json!({"type": "Outcome", "data": {}})).unwrap(),
            none
        );
        assert_eq!(
            serde_json::from_value::<InputMapping>(
                json!({"type": "Outcome", "data": {"projection": null}})
            )
            .unwrap(),
            none
        );

        let some = InputMapping::Outcome {
            projection: Some("error".to_string()),
        };
        let some_v = serde_json::to_value(&some).unwrap();
        assert_eq!(
            some_v,
            json!({"type": "Outcome", "data": {"projection": "error"}})
        );
        assert_eq!(
            serde_json::from_value::<InputMapping>(some_v).unwrap(),
            some
        );

        // AC4: existing variants' encoding is unchanged.
        assert_eq!(
            serde_json::to_value(InputMapping::Passthrough).unwrap(),
            json!({"type": "Passthrough"})
        );
        assert_eq!(
            serde_json::to_value(InputMapping::Static(json!(1))).unwrap(),
            json!({"type": "Static", "data": 1})
        );
        assert_eq!(
            serde_json::to_value(InputMapping::Projection("a.b".to_string())).unwrap(),
            json!({"type": "Projection", "data": "a.b"})
        );
    }

    // ── issue #810: TriggerCondition output guards ─────────────────────────

    fn output() -> Value {
        json!({
            "amount": 1500,
            "region": "EU",
            "ratio": 0.5,
            "flags": {"vip": true, "note": null},
            "tags": ["a", "b"],
        })
    }

    #[test]
    fn condition_eq_hit_miss_missing_and_null() {
        let out = output();
        let hit = TriggerCondition::Eq {
            path: "region".into(),
            value: json!("EU"),
        };
        assert!(hit.evaluate(&out));
        let miss = TriggerCondition::Eq {
            path: "region".into(),
            value: json!("US"),
        };
        assert!(!miss.evaluate(&out));
        // Missing path: comparisons never match.
        let missing = TriggerCondition::Eq {
            path: "nope".into(),
            value: json!("EU"),
        };
        assert!(!missing.evaluate(&out));
        // Present-but-null: comparisons never match either — even Eq(null).
        let null_eq = TriggerCondition::Eq {
            path: "flags.note".into(),
            value: Value::Null,
        };
        assert!(!null_eq.evaluate(&out));
        // Deep path + non-scalar equality both work.
        let deep = TriggerCondition::Eq {
            path: "flags.vip".into(),
            value: json!(true),
        };
        assert!(deep.evaluate(&out));
        let arr = TriggerCondition::Eq {
            path: "tags".into(),
            value: json!(["a", "b"]),
        };
        assert!(arr.evaluate(&out));
    }

    #[test]
    fn condition_noteq_requires_present_non_null() {
        let out = output();
        let ne_hit = TriggerCondition::NotEq {
            path: "region".into(),
            value: json!("US"),
        };
        assert!(ne_hit.evaluate(&out));
        let ne_miss = TriggerCondition::NotEq {
            path: "region".into(),
            value: json!("EU"),
        };
        assert!(!ne_miss.evaluate(&out));
        // Missing and explicit-null are both false (use Exists / IsNull instead).
        let ne_missing = TriggerCondition::NotEq {
            path: "nope".into(),
            value: json!("EU"),
        };
        assert!(!ne_missing.evaluate(&out));
        let ne_null = TriggerCondition::NotEq {
            path: "flags.note".into(),
            value: json!("EU"),
        };
        assert!(!ne_null.evaluate(&out));
    }

    #[test]
    fn condition_ordering_operators_numeric_only() {
        let out = output();
        let gt = |v: Value| TriggerCondition::GreaterThan {
            path: "amount".into(),
            value: v,
        };
        assert!(gt(json!(1000)).evaluate(&out));
        assert!(!gt(json!(1500)).evaluate(&out));
        assert!(!gt(json!(2000)).evaluate(&out));
        assert!(
            TriggerCondition::GreaterThanOrEq {
                path: "amount".into(),
                value: json!(1500),
            }
            .evaluate(&out)
        );
        assert!(
            TriggerCondition::LessThan {
                path: "amount".into(),
                value: json!(1501),
            }
            .evaluate(&out)
        );
        assert!(
            !TriggerCondition::LessThan {
                path: "amount".into(),
                value: json!(1500),
            }
            .evaluate(&out)
        );
        assert!(
            TriggerCondition::LessThanOrEq {
                path: "amount".into(),
                value: json!(1500),
            }
            .evaluate(&out)
        );
        // Non-numeric operand or projected value: ordering is numeric-only → false.
        assert!(
            !TriggerCondition::GreaterThan {
                path: "region".into(),
                value: json!("A"),
            }
            .evaluate(&out)
        );
        assert!(!gt(json!("1000")).evaluate(&out));
        // Missing path → false, never a panic.
        assert!(
            !TriggerCondition::GreaterThan {
                path: "absent".into(),
                value: json!(1),
            }
            .evaluate(&out)
        );
    }

    #[test]
    fn condition_numeric_coercion_int_vs_float() {
        let out = output();
        // 1500 (int) == 1500.0 (float) under numeric coercion.
        assert!(
            TriggerCondition::Eq {
                path: "amount".into(),
                value: json!(1500.0),
            }
            .evaluate(&out)
        );
        assert!(
            TriggerCondition::GreaterThan {
                path: "amount".into(),
                value: json!(1499.5),
            }
            .evaluate(&out)
        );
        assert!(
            TriggerCondition::Eq {
                path: "ratio".into(),
                value: json!(0.5),
            }
            .evaluate(&out)
        );
        // In-list numeric coercion: 1500 matches [1500.0].
        assert!(
            TriggerCondition::In {
                path: "amount".into(),
                values: vec![json!(1500.0), json!("x")],
            }
            .evaluate(&out)
        );
        // Number never coerces to a numeric *string*.
        assert!(
            !TriggerCondition::Eq {
                path: "amount".into(),
                value: json!("1500"),
            }
            .evaluate(&out)
        );
    }

    #[test]
    fn condition_large_integers_compare_exactly() {
        // Two distinct integers above 2^53 must never compare equal: the
        // exact-integer fast path bypasses lossy f64 coercion entirely
        // (2^53 = 9007199254740992; +1 is unrepresentable in f64).
        let out = json!({
            "id": 9_007_199_254_740_993_u64,          // 2^53 + 1
            "big": u64::MAX,                          // above i64::MAX
            "neg": -1_i64,
        });
        assert!(
            !TriggerCondition::Eq {
                path: "id".into(),
                value: json!(9_007_199_254_740_992_u64), // 2^53 — distinct
            }
            .evaluate(&out)
        );
        assert!(
            TriggerCondition::Eq {
                path: "id".into(),
                value: json!(9_007_199_254_740_993_u64),
            }
            .evaluate(&out)
        );
        // u64 above i64::MAX: distinct neighbors stay distinct.
        assert!(
            !TriggerCondition::Eq {
                path: "big".into(),
                value: json!(u64::MAX - 1),
            }
            .evaluate(&out)
        );
        assert!(
            TriggerCondition::Eq {
                path: "big".into(),
                value: json!(u64::MAX),
            }
            .evaluate(&out)
        );
        // In-list membership uses the same exact path.
        assert!(
            !TriggerCondition::In {
                path: "id".into(),
                values: vec![json!(9_007_199_254_740_992_u64)],
            }
            .evaluate(&out)
        );
        assert!(
            TriggerCondition::In {
                path: "id".into(),
                values: vec![json!(9_007_199_254_740_993_u64)],
            }
            .evaluate(&out)
        );
        // Ordering on large integers is exact too (f64 would see these as equal).
        assert!(
            TriggerCondition::GreaterThan {
                path: "id".into(),
                value: json!(9_007_199_254_740_992_u64),
            }
            .evaluate(&out)
        );
        assert!(
            TriggerCondition::LessThan {
                path: "big".into(),
                value: json!(u64::MAX),
            }
            .evaluate(&json!({"big": u64::MAX - 1}))
        );
        // Mixed sign: a negative i64 never equals (and always orders below)
        // a u64 above i64::MAX.
        assert!(
            !TriggerCondition::Eq {
                path: "neg".into(),
                value: json!(u64::MAX),
            }
            .evaluate(&out)
        );
        assert!(
            TriggerCondition::LessThan {
                path: "neg".into(),
                value: json!(u64::MAX),
            }
            .evaluate(&out)
        );
        // Mixed int/float still coerces through f64: `1 == 1.0`.
        let small = json!({"n": 1});
        assert!(
            TriggerCondition::Eq {
                path: "n".into(),
                value: json!(1.0),
            }
            .evaluate(&small)
        );
    }

    #[test]
    fn condition_eq_bool_vs_number_is_false() {
        // A bool is not a number: no coercion, strict Value inequality.
        let out = json!({"flag": true});
        assert!(
            !TriggerCondition::Eq {
                path: "flag".into(),
                value: json!(1),
            }
            .evaluate(&out)
        );
        assert!(
            !TriggerCondition::GreaterThan {
                path: "flag".into(),
                value: json!(0),
            }
            .evaluate(&out)
        );
    }

    #[test]
    fn condition_midpath_scalar_traversal_is_missing() {
        // Walking a member path THROUGH a scalar resolves to missing → false
        // for comparisons, false for Exists.
        let out = json!({"amount": 1500});
        assert!(
            !TriggerCondition::Eq {
                path: "amount.sub".into(),
                value: json!(1500),
            }
            .evaluate(&out)
        );
        assert!(
            !TriggerCondition::Exists {
                path: "amount.sub".into(),
            }
            .evaluate(&out)
        );
    }

    #[test]
    fn condition_in_exists_isnull_semantics() {
        let out = output();
        assert!(
            TriggerCondition::In {
                path: "region".into(),
                values: vec![json!("US"), json!("EU")],
            }
            .evaluate(&out)
        );
        assert!(
            !TriggerCondition::In {
                path: "region".into(),
                values: vec![json!("US")],
            }
            .evaluate(&out)
        );
        // Missing path / empty set → false.
        assert!(
            !TriggerCondition::In {
                path: "absent".into(),
                values: vec![json!("EU")],
            }
            .evaluate(&out)
        );
        assert!(
            !TriggerCondition::In {
                path: "region".into(),
                values: vec![],
            }
            .evaluate(&out)
        );
        // Null projected value never matches In — even In([null]).
        assert!(
            !TriggerCondition::In {
                path: "flags.note".into(),
                values: vec![Value::Null],
            }
            .evaluate(&out)
        );

        // Exists: present (including explicit null) → true; missing → false.
        assert!(
            TriggerCondition::Exists {
                path: "region".into(),
            }
            .evaluate(&out)
        );
        assert!(
            TriggerCondition::Exists {
                path: "flags.note".into(),
            }
            .evaluate(&out)
        );
        assert!(
            !TriggerCondition::Exists {
                path: "absent".into(),
            }
            .evaluate(&out)
        );

        // IsNull: present AND explicitly null → true; missing → false; value → false.
        assert!(
            TriggerCondition::IsNull {
                path: "flags.note".into(),
            }
            .evaluate(&out)
        );
        assert!(
            !TriggerCondition::IsNull {
                path: "absent".into(),
            }
            .evaluate(&out)
        );
        assert!(
            !TriggerCondition::IsNull {
                path: "region".into(),
            }
            .evaluate(&out)
        );
    }

    #[test]
    fn condition_combinators_all_any_not() {
        let out = output();
        let eu = TriggerCondition::Eq {
            path: "region".into(),
            value: json!("EU"),
        };
        let big = TriggerCondition::GreaterThan {
            path: "amount".into(),
            value: json!(1000),
        };
        let small = TriggerCondition::LessThan {
            path: "amount".into(),
            value: json!(10),
        };

        assert!(TriggerCondition::All(vec![eu.clone(), big.clone()]).evaluate(&out));
        assert!(!TriggerCondition::All(vec![eu.clone(), small.clone()]).evaluate(&out));
        assert!(TriggerCondition::Any(vec![small.clone(), big.clone()]).evaluate(&out));
        assert!(!TriggerCondition::Any(vec![small.clone()]).evaluate(&out));
        assert!(TriggerCondition::Not(Box::new(small.clone())).evaluate(&out));
        assert!(!TriggerCondition::Not(Box::new(big.clone())).evaluate(&out));
        // Conventional identities: All([]) = true, Any([]) = false.
        assert!(TriggerCondition::All(vec![]).evaluate(&out));
        assert!(!TriggerCondition::Any(vec![]).evaluate(&out));
        // Nesting: Not(Any([All([eu, big]), small])) — inner All matches → false.
        let nested = TriggerCondition::Not(Box::new(TriggerCondition::Any(vec![
            TriggerCondition::All(vec![eu, big]),
            small,
        ])));
        assert!(!nested.evaluate(&out));
        // Not(Exists(missing)) is the sanctioned "field absent" guard and CAN
        // meaningfully fire on failure sources whose output is NULL.
        assert!(
            TriggerCondition::Not(Box::new(TriggerCondition::Exists {
                path: "absent".into(),
            }))
            .evaluate(&out)
        );
    }

    #[test]
    fn condition_evaluate_against_non_object_outputs() {
        // String output: root-path comparisons work, member paths are missing.
        let s = json!("done");
        assert!(
            TriggerCondition::Eq {
                path: String::new(),
                value: json!("done"),
            }
            .evaluate(&s)
        );
        assert!(
            !TriggerCondition::Eq {
                path: "a".into(),
                value: json!("done"),
            }
            .evaluate(&s)
        );
        // Array output: paths never traverse arrays (segment lookup by key only).
        let arr = json!([1, 2, 3]);
        assert!(
            !TriggerCondition::Eq {
                path: "0".into(),
                value: json!(1),
            }
            .evaluate(&arr)
        );
        assert!(
            TriggerCondition::Eq {
                path: String::new(),
                value: json!([1, 2, 3]),
            }
            .evaluate(&arr)
        );
        // NULL source output (e.g. a Failed source): behaves as literal JSON
        // null at the root — every member path is missing, comparisons false.
        let null_out = Value::Null;
        assert!(
            !TriggerCondition::Eq {
                path: "region".into(),
                value: json!("EU"),
            }
            .evaluate(&null_out)
        );
        assert!(
            !TriggerCondition::Exists {
                path: "region".into(),
            }
            .evaluate(&null_out)
        );
        assert!(
            TriggerCondition::IsNull {
                path: String::new()
            }
            .evaluate(&null_out)
        );
    }

    #[test]
    fn condition_serde_round_trip_uses_adjacent_tagging() {
        let cond = TriggerCondition::All(vec![
            TriggerCondition::Eq {
                path: "region".into(),
                value: json!("EU"),
            },
            TriggerCondition::Not(Box::new(TriggerCondition::In {
                path: "tier".into(),
                values: vec![json!("free")],
            })),
        ]);
        let val = serde_json::to_value(&cond).unwrap();
        // Adjacently tagged, mirroring `InputMapping` exactly.
        assert_eq!(val["type"], json!("All"));
        assert_eq!(val["data"][0]["type"], json!("Eq"));
        assert_eq!(val["data"][0]["data"]["path"], json!("region"));
        assert_eq!(val["data"][0]["data"]["value"], json!("EU"));
        assert_eq!(val["data"][1]["type"], json!("Not"));
        let back: TriggerCondition = serde_json::from_value(val).unwrap();
        assert_eq!(back, cond);
    }

    #[test]
    fn condition_unknown_operator_is_a_deserialize_error() {
        // Unknown operator (e.g. a CEL-style "Regex") must be a serde error →
        // 400 at the HTTP registration boundary, never silently dropped.
        let bad = json!({"type": "Regex", "data": {"path": "a", "value": ".*"}});
        assert!(serde_json::from_value::<TriggerCondition>(bad).is_err());
        // Malformed payload for a known operator is rejected too.
        let missing_value = json!({"type": "Eq", "data": {"path": "a"}});
        assert!(serde_json::from_value::<TriggerCondition>(missing_value).is_err());
    }

    fn nested_all(depth: usize) -> TriggerCondition {
        let mut cond = TriggerCondition::Exists { path: "a".into() };
        for _ in 1..depth {
            cond = TriggerCondition::All(vec![cond]);
        }
        cond
    }

    fn nested_not(depth: usize) -> TriggerCondition {
        let mut cond = TriggerCondition::Exists { path: "a".into() };
        for _ in 1..depth {
            cond = TriggerCondition::Not(Box::new(cond));
        }
        cond
    }

    fn wide_all(leaves: usize) -> TriggerCondition {
        TriggerCondition::All(
            (0..leaves)
                .map(|_| TriggerCondition::Exists { path: "a".into() })
                .collect(),
        )
    }

    #[test]
    fn condition_validate_enforces_bounded_caps() {
        // Depth: exactly the cap passes; one more is rejected.
        assert!(nested_all(MAX_CONDITION_DEPTH).validate().is_ok());
        assert!(nested_all(MAX_CONDITION_DEPTH + 1).validate().is_err());
        // The `Not` arm counts toward the depth cap identically.
        assert!(nested_not(MAX_CONDITION_DEPTH).validate().is_ok());
        assert!(nested_not(MAX_CONDITION_DEPTH + 1).validate().is_err());
        // Node count: a combinator + leaves totalling the cap passes; +1 rejected.
        assert!(wide_all(MAX_CONDITION_NODES - 1).validate().is_ok());
        assert!(wide_all(MAX_CONDITION_NODES).validate().is_err());
        // In-list length cap.
        let in_ok = TriggerCondition::In {
            path: "a".into(),
            values: (0..MAX_CONDITION_IN_VALUES).map(|i| json!(i)).collect(),
        };
        assert!(in_ok.validate().is_ok());
        let in_over = TriggerCondition::In {
            path: "a".into(),
            values: (0..=MAX_CONDITION_IN_VALUES).map(|i| json!(i)).collect(),
        };
        assert!(in_over.validate().is_err());
    }

    #[test]
    fn condition_validate_rejects_malformed_paths() {
        for bad in ["a..b", ".a", "a.", "."] {
            let cond = TriggerCondition::Exists { path: bad.into() };
            assert!(cond.validate().is_err(), "path {bad:?} should be rejected");
        }
        // Empty path = the whole output; explicitly allowed.
        assert!(
            TriggerCondition::Exists {
                path: String::new()
            }
            .validate()
            .is_ok()
        );
        // Malformed paths nested inside combinators are caught too.
        let nested = TriggerCondition::Any(vec![TriggerCondition::Not(Box::new(
            TriggerCondition::Eq {
                path: "a..b".into(),
                value: json!(1),
            },
        ))]);
        assert!(nested.validate().is_err());
    }

    #[test]
    fn condition_gate_stored_condition_semantics() {
        let out = output();
        // NULL stored condition = unconditional = byte-identical legacy pass.
        assert_eq!(gate_stored_condition(None, &out), ConditionGate::Pass);
        // Valid + met → Pass.
        let met = serde_json::to_value(TriggerCondition::GreaterThan {
            path: "amount".into(),
            value: json!(1000),
        })
        .unwrap();
        assert_eq!(gate_stored_condition(Some(&met), &out), ConditionGate::Pass);
        // Valid + unmet → Unmet.
        let unmet = serde_json::to_value(TriggerCondition::GreaterThan {
            path: "amount".into(),
            value: json!(2000),
        })
        .unwrap();
        assert_eq!(
            gate_stored_condition(Some(&unmet), &out),
            ConditionGate::Unmet
        );
        // Unparseable stored JSON → Invalid (FAIL CLOSED — never fire).
        let garbage = json!({"type": "Regex", "data": {}});
        assert_eq!(
            gate_stored_condition(Some(&garbage), &out),
            ConditionGate::Invalid
        );
        // Parseable but over the boundedness caps → Invalid (fail closed).
        let over_cap = serde_json::to_value(nested_all(MAX_CONDITION_DEPTH + 1)).unwrap();
        assert_eq!(
            gate_stored_condition(Some(&over_cap), &out),
            ConditionGate::Invalid
        );
        // A stored JSONB `'null'` (JSON null, not SQL NULL — only writable
        // via direct SQL / external tooling) is Some(&Value::Null): a parse
        // error → Invalid, fail closed, never a legacy-style Pass.
        assert_eq!(
            gate_stored_condition(Some(&Value::Null), &out),
            ConditionGate::Invalid
        );
    }

    #[test]
    fn with_condition_builder_and_legacy_serde_default() {
        let cond = TriggerCondition::Eq {
            path: "region".into(),
            value: json!("EU"),
        };
        let trigger = CompletionTrigger::new("a", "b").with_condition(cond.clone());
        assert_eq!(trigger.condition, Some(cond));
        // Default construction carries no condition.
        assert_eq!(CompletionTrigger::new("a", "b").condition, None);
        // Legacy serialized triggers (no `condition` key) still deserialize.
        let legacy = json!({
            "id": Uuid::new_v4(),
            "source_workflow_name": "a",
            "terminal_states": ["Completed"],
            "target_workflow_name": "b",
            "input_mapping": {"type": "Passthrough"},
            "queue_name": null,
        });
        let parsed: CompletionTrigger = serde_json::from_value(legacy).unwrap();
        assert_eq!(parsed.condition, None);
        // And a condition-less trigger serializes without the key (wire shape
        // unchanged for legacy consumers).
        let val = serde_json::to_value(CompletionTrigger::new("a", "b")).unwrap();
        assert!(val.get("condition").is_none());
    }
}
