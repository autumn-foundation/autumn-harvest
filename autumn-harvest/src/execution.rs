//! Workflow execution persistence helpers.
//!
//! The public start helper in this module gives callers idempotent workflow
//! start semantics scoped to `(workflow_name, workflow_id)`.

use chrono::Utc;
use diesel::ExpressionMethods;
use diesel::OptionalExtension;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use uuid::Uuid;

use crate::build_routing;
use crate::completion_trigger::DeferredTriggerStart;
use crate::error::{HarvestError, HarvestResult, database_error};
use crate::event::WorkflowEvent;
use crate::info::WorkflowInfo;
use crate::models::{NewHarvestSignal, NewWorkflowExecution, WorkflowExecution};
use crate::queue::{self, EnqueueParams, TaskType};
use crate::schema::{harvest_signals, harvest_workflow_executions};
use crate::store;
use crate::telemetry::TraceContextCarrier;
use crate::types::{
    ExecutionId, ParentClosePolicy, Priority, StartSource, WorkflowIdConflictPolicy,
    WorkflowIdReusePolicy,
};

/// Parameters for starting a workflow execution.
///
/// `exec_id` is the workflow's routing key: its UUID carries the target
/// [`crate::types::ShardId`] in its first two bytes (see
/// [`ExecutionId::new_for_shard`]). In multi-shard deployments the caller
/// picks the shard via [`crate::ShardRouter`] and mints the id with
/// [`ExecutionId::new_for_shard`] before calling this helper. Single-shard
/// deployments can pass `ExecutionId::new_for_shard(ShardId::new(0))` or, for
/// tests and non-production code, the sentinel-producing `ExecutionId::new()`.
#[derive(Debug, Clone)]
pub struct StartWorkflowParams<'a> {
    pub workflow_name: &'a str,
    pub workflow_id: &'a str,
    pub exec_id: ExecutionId,
    pub input: serde_json::Value,
    pub parent_id: Option<Uuid>,
    pub queue_name: &'a str,
    pub execution_timeout: Option<chrono::Duration>,
    pub memo: Option<serde_json::Value>,
    pub search_attrs: Option<serde_json::Value>,
    /// How to handle a duplicate `(workflow_name, workflow_id)` collision.
    /// Defaults to [`WorkflowIdReusePolicy::AllowDuplicate`].
    pub reuse_policy: WorkflowIdReusePolicy,
    /// How to handle a collision with a currently-active (RUNNING/PAUSED) prior
    /// (issue #685). Orthogonal to [`Self::reuse_policy`]. Default
    /// [`WorkflowIdConflictPolicy::Unspecified`] preserves the reuse policy's
    /// native active behavior.
    pub conflict_policy: WorkflowIdConflictPolicy,
    /// W3C trace context captured at the call site (e.g., from the HTTP handler's
    /// `harvest.workflow.schedule` span) and stored on the task row so the worker
    /// can stitch the trace across the queue boundary (ADR-0001 §3).
    pub trace_context: Option<TraceContextCarrier>,
    /// Server-side ceiling applied to `execution_timeout` (issue #243).
    ///
    /// When `Some`, the effective timeout is `execution_timeout.min(ceiling)`.
    /// `None` means no ceiling is enforced.  Typically populated from
    /// `BuiltHarvest::max_workflow_execution_timeout` by the plugin layer.
    pub max_execution_timeout_ceiling: Option<chrono::Duration>,
    /// Chain-scoped lifetime cap DURATION for a fresh chain-origin start (issue
    /// #617). Distinct from the per-run [`Self::execution_timeout`]: the chain cap
    /// is anchored at the first run's start and carried verbatim across every
    /// continue-as-new. `None` = the caller specified no chain cap for this start
    /// (a fleet-wide ceiling may still apply — see [`Self::max_workflow_chain_timeout_ceiling`]).
    pub chain_execution_timeout: Option<chrono::Duration>,
    /// Server-side ceiling on the chain cap (issue #617). Unlike
    /// [`Self::max_execution_timeout_ceiling`] (which only caps a *specified*
    /// per-run timeout), this ceiling ALSO acts as a fleet-wide default: a
    /// workflow that specifies no chain cap still inherits the ceiling as its
    /// chain deadline. Typically populated from
    /// `BuiltHarvest::max_workflow_chain_timeout` by the plugin layer.
    pub max_workflow_chain_timeout_ceiling: Option<chrono::Duration>,
    /// Inherited absolute chain deadline for a continuation of the same logical
    /// run (issue #617). When `Some`, it is used VERBATIM as `chain_deadline_at`
    /// (workflow-level retry #523 carries the origin's chain deadline forward,
    /// since a retry is the same logical run continuing). When `None`, the chain
    /// deadline is computed fresh as `target_start_time + effective_chain_timeout`.
    /// Continue-as-new does not use this field: it carries the chain columns
    /// directly on the successor insert in the worker.
    pub inherited_chain_deadline_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Pre-resolved concurrency group key for this workflow run (issue #247).
    ///
    /// Callers resolve the key expression from `WorkflowInfo.concurrency.key_expr`
    /// against the input payload via [`crate::concurrency::resolve_concurrency_key`]
    /// before constructing `StartWorkflowParams`. When `None`, no per-key cap is
    /// applied and only the worker-level semaphore limits concurrency.
    pub concurrency_key: Option<String>,
    /// Maximum number of RUNNING workflow tasks allowed for [`Self::concurrency_key`].
    /// Required whenever `concurrency_key` is `Some`; ignored when it is `None`.
    pub concurrency_limit: Option<u32>,
    /// Within-queue claim priority for this workflow execution (issue #249).
    ///
    /// Stored on the task queue row; does not affect the event history or
    /// replay determinism. Defaults to [`Priority::Normal`] so pre-upgrade
    /// callers that do not set this field are unaffected.
    pub priority: Priority,
    /// Maximum allowed byte size for the workflow input payload (issue #252).
    ///
    /// Enforced only on the fresh-insert path: duplicate collisions resolve
    /// against the existing execution without touching the input. Zero means
    /// uncapped (the default for callers that do not configure a cap).
    pub max_workflow_input_bytes: u64,
    /// Optional timestamp to start the workflow at (issue #322).
    pub start_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Optional duration delay before starting the workflow (issue #322).
    pub delay: Option<chrono::Duration>,
    /// Server-side ceiling on start delay (issue #322).
    pub max_workflow_start_delay: Option<chrono::Duration>,
    pub owner: Option<&'a str>,
    pub runbook_url: Option<&'a str>,
    pub severity: Option<&'a str>,
    /// Ambient string key-value context propagated to all activities and child
    /// workflows without threading through function signatures (issue #481).
    pub context_headers: Option<std::collections::HashMap<String, String>>,
    /// Soft SLA budget for this workflow run (issue #487).
    ///
    /// When set, `sla_deadline_at = started_at + sla` is persisted.  A scanner
    /// detects breach (`now > sla_deadline_at` while RUNNING/SUSPENDED) and
    /// emits `harvest.workflow.sla_breached` exactly once — without altering the
    /// run's lifecycle.
    ///
    /// If `sla > effective_timeout` (the hard deadline), `sla` is clamped down
    /// to `effective_timeout` at start time.  `None` = no SLA enforced.
    pub sla: Option<chrono::Duration>,
    /// The `harvest_schedules.id` that triggered this execution (issue #488).
    /// `None` for manually-started (non-scheduled) workflows. When `Some`, the
    /// start path resolves the prior COMPLETED output and most-recent terminal
    /// error for this schedule and freezes them into the `WorkflowStarted` event.
    pub schedule_id: Option<uuid::Uuid>,
    /// The logical schedule slot this run fires for (issue #488). Carryover selects the
    /// previous fire by this slot (not completion time), so out-of-order completions
    /// (overlap / catch-up / backfill) can't roll an incremental cursor backward.
    /// `None` for manual starts and any non-scheduled call site.
    pub scheduled_for: Option<chrono::DateTime<chrono::Utc>>,
    /// Attempt number for this execution in the retry chain (issue #523). 1 = first attempt.
    /// Callers starting a fresh workflow pass `1`. The retry hook passes `workflow_attempt + 1`.
    pub workflow_attempt: u32,
    /// Effective retry policy frozen at start time (issue #523).
    ///
    /// Precedence: per-start override > schedule-default > workflow-type default, then clamped
    /// by `max_workflow_attempts_ceiling`. `None` = no auto-retry for this execution.
    pub workflow_retry_policy: Option<crate::policy::RetryPolicy>,
    /// ID of the prior failed execution this run retries (issue #523). `None` for first attempt.
    pub retry_of_exec_id: Option<uuid::Uuid>,
    /// Server-side ceiling on `retry_policy.max_attempts` (issue #523).
    ///
    /// When `Some(n)`, the effective max attempts = `min(policy.max_attempts, n)`.
    /// Typically sourced from `BuiltHarvest::max_workflow_attempts`.
    pub max_workflow_attempts_ceiling: Option<u32>,
    /// Dispatch origin of this execution (issue #534).
    ///
    /// One of [`ORIGIN_SCHEDULED`], [`ORIGIN_BACKFILL`], or [`ORIGIN_MANUAL_TRIGGER`]
    /// for schedule-attributed runs (set alongside `schedule_id`); `None` for every
    /// non-scheduled call site. Persisted as metadata only — it never affects replay,
    /// carryover, or shard routing, and is surfaced solely by the per-schedule
    /// run-history endpoint to distinguish normal cadence from backfill/manual fires.
    pub origin: Option<&'a str>,
    /// Per-execution completion-callback targets (issue #605): a JSON array
    /// of `{url, filter}` objects (see
    /// [`crate::completion_callback::CallbackTarget`]). `None` = no
    /// per-execution targets; the effective target set at the terminal
    /// transition is still the union with any builder-wide defaults.
    pub completion_callbacks: Option<serde_json::Value>,
    /// Workflow-start provenance classifier (issue #740): how this execution
    /// was started (API call, schedule tick, child spawn, ...). Persisted as
    /// metadata only — never read on replay. Distinct from `origin` (#534).
    pub start_source: StartSource,
    /// Optional correlation reference for the start source (issue #740), e.g.
    /// the triggering execution id or schedule id. `None` when absent.
    pub start_source_ref: Option<&'a str>,
    /// Optional human/operator attribution for the start (issue #740). `None`
    /// when absent.
    pub started_by: Option<&'a str>,
}

/// Origin marker for a normal scheduler-tick fire (issue #534).
pub const ORIGIN_SCHEDULED: &str = "scheduled";
/// Origin marker for a run created by a schedule backfill (issue #534).
pub const ORIGIN_BACKFILL: &str = "backfill";
/// Origin marker for an ad-hoc operator `trigger-now` fire of a schedule (issue #534).
pub const ORIGIN_MANUAL_TRIGGER: &str = "manual_trigger";

/// Bounded retry cap for the active-conflict seal-race load (issue #685 review,
/// Codex P2). Each retry's `FOR UPDATE` naturally blocks on whichever concurrent
/// contender currently holds the row lock, so this caps the number of distinct
/// serialized winners a single loser will wait behind before falling back to the
/// pre-existing `NotFound`.
///
/// N concurrent replace-type starts (e.g. `terminate_existing`) against one key
/// serialize: each seals the current survivor before inserting its own, so the
/// LAST loser can wait behind up to N-1 winners and needs up to N-1 retries to
/// see the final survivor. Convergence is therefore guaranteed once the cap
/// exceeds the concurrent-burst size (there are only ever N fixed contenders, so
/// after at most N seals the last survivor is never sealed again and stays
/// RUNNING). This cap is set generously above realistic concurrent-delivery
/// bursts; because a retry BLOCKS on a real lock holder (forward progress, not a
/// busy-loop), a high cap costs nothing in the common case — it only bounds the
/// truly-stuck pathological case (a burst larger than the cap, or a prior sealed
/// WITHOUT a replacement, e.g. a concurrent reset), which falls back to the
/// pre-existing terminal `NotFound`.
const SEAL_RACE_MAX_LOAD_RETRIES: u32 = 64;

/// Small fixed backoff (milliseconds) between seal-race load retries. In the
/// contended case the re-issued `FOR UPDATE` already blocks on the current lock
/// holder (so the winner is committed and this sleep is negligible); the backoff
/// only prevents a tight busy-loop in the pathological "sealed without a
/// replacement" case, where the re-issued load returns `None` immediately with no
/// lock holder to block on.
const SEAL_RACE_LOAD_BACKOFF_MS: u64 = 1;

impl StartWorkflowParams<'_> {
    /// Shard derived from the encoded `exec_id`, used to populate the row's
    /// `shard_id` column. Returns `0` when the caller passed an unencoded id
    /// (tests / legacy call sites), matching the pre-sharding default.
    #[must_use]
    pub fn shard_id(&self) -> i32 {
        let shard = self.exec_id.shard();
        if shard.is_unencoded() {
            0
        } else {
            shard.as_i32()
        }
    }
}

/// Result of an idempotent workflow start attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartedWorkflowExecution {
    pub exec_id: ExecutionId,
    pub workflow_name: String,
    pub workflow_id: String,
    pub state: String,
    pub created: bool,
}

impl StartedWorkflowExecution {
    fn from_row(execution: WorkflowExecution, created: bool) -> Self {
        Self {
            exec_id: ExecutionId::from_uuid(execution.id),
            workflow_name: execution.workflow_name,
            workflow_id: execution.workflow_id,
            state: execution.state,
            created,
        }
    }
}

/// Result of a workflow cancellation request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelledWorkflowExecution {
    /// Cancelled workflow execution ID.
    pub exec_id: ExecutionId,
    /// Final execution state.
    pub state: String,
    /// Stored cancellation reason.
    pub reason: String,
    /// `true` when this request performed the terminal transition.
    pub newly_cancelled: bool,
    /// Number of pending/running task rows failed by this request.
    pub failed_task_count: usize,
    /// Workflow type name — used by callers that want to emit per-workflow
    /// metrics without re-querying the execution row.
    pub workflow_name: String,
    /// Task queue the execution was dispatched on.
    pub queue_name: String,
    /// The state the execution was in before this transition.
    pub prior_state: String,
}

impl CancelledWorkflowExecution {
    fn idempotent(exec_id: ExecutionId, execution: WorkflowExecution) -> Self {
        Self {
            exec_id,
            state: execution.state.clone(),
            reason: execution
                .error
                .unwrap_or_else(|| "workflow already cancelled".to_string()),
            newly_cancelled: false,
            failed_task_count: 0,
            workflow_name: execution.workflow_name,
            queue_name: execution.queue_name,
            prior_state: execution.state,
        }
    }

    fn newly_cancelled(
        exec_id: ExecutionId,
        final_state: &str,
        reason: String,
        failed_task_count: usize,
        workflow_name: String,
        queue_name: String,
        prior_state: String,
    ) -> Self {
        Self {
            exec_id,
            state: final_state.to_string(),
            reason,
            newly_cancelled: true,
            failed_task_count,
            workflow_name,
            queue_name,
            prior_state,
        }
    }
}

/// Evaluate the admission gate for a start against the process-global gate cache
/// (issue #618, PR #1014). Returns `Some((gate_id, reason, scope_kind))` on a
/// match, `None` when no gate matches or no cache is installed.
fn evaluate_start_gate(
    mode: crate::admission_gate::GateMode,
    workflow_name: &str,
    queue_name: &str,
    shard_id: i32,
    owner: Option<&str>,
) -> Option<(uuid::Uuid, String, &'static str)> {
    let cache = crate::admission_gate::global_admission_gate_cache()?;
    match mode {
        crate::admission_gate::GateMode::Check => {
            cache.check(workflow_name, queue_name, shard_id, owner)
        }
        crate::admission_gate::GateMode::CheckCached => {
            cache.check_cached(workflow_name, queue_name, shard_id, owner)
        }
    }
}

/// Record a `harvest.admission.blocked` count for a gated start (issue #618, PR
/// #1014). Uses the caller-supplied recorder when present, else the process-global
/// recorder the plugin publishes at boot — so a block on a metrics-less internal
/// start path (cancel / terminate / parent-close cascade) is still counted.
fn record_start_gate_block(
    metrics: Option<&(dyn crate::telemetry::MetricsRecorder + Send + Sync)>,
    scope_kind: &str,
    reason: &str,
) {
    // Truncate to 64 *characters* (not bytes) for bounded metric cardinality;
    // char_indices avoids splitting a multi-byte code point.
    let label = match reason.char_indices().nth(64) {
        Some((i, _)) => &reason[..i],
        None => reason,
    };
    if let Some(m) = metrics {
        m.record_admission_blocked(scope_kind, label);
    } else if let Some(g) = crate::admission_gate::global_admission_metrics() {
        g.record_admission_blocked(scope_kind, label);
    }
}

/// Start a workflow execution or load the existing one, returning both the result
/// and any deferred completion-trigger starts **without spawning them**.
///
/// This is the low-level primitive for callers that run the start inside a
/// larger outer transaction: the `DeferredTriggerStart`s must only be spawned
/// *after* that outer transaction commits, otherwise trigger workflows could
/// start for a start that later rolls back (issue #499 debounce scanner).
///
/// The plain [`start_or_load_workflow_execution`] wrapper spawns them itself for
/// the common standalone case.
///
/// ## Policy behaviour
///
/// | Prior state | `AllowDuplicate` | `RejectDuplicate` | `AllowDuplicateFailedOnly` | `TerminateIfRunning` |
/// |-------------|------------------|-------------------|---------------------------|----------------------|
/// | none | create | create | create | create |
/// | RUNNING | return existing | `Err(AlreadyExists)` | return existing | cancel + start fresh |
/// | COMPLETED | return existing | `Err(AlreadyExists)` | return existing | start fresh |
/// | FAILED | return existing | `Err(AlreadyExists)` | start fresh | start fresh |
/// | CANCELLED | return existing | `Err(AlreadyExists)` | start fresh | start fresh |
///
/// `in_outer_transaction` tells the function whether the caller is running it
/// inside the caller's own transaction. It matters only for the
/// `TerminateIfRunning` pre-check cancellation, which commits (Transaction 1)
/// before the replacement start (Transaction 2): if the replacement start then
/// fails, the prior run's completion-trigger / parent-close follow-up starts
/// must still be spawned for a **top-level** caller (`false`), but must be
/// **suppressed** for a caller inside an outer transaction (`true`) whose
/// rollback reverts that cancellation. On success the follow-ups are always
/// returned for the caller to spawn after its outer commit.
///
/// `reject_fresh_if_debounced`, when `true`, makes the function return
/// [`HarvestError::DebounceFreshStart`] (rolling the transaction back) if the
/// reuse-policy decision under the `FOR UPDATE` lock would create a **fresh**
/// execution (insert, or a `replace_execution` seal+insert). An attach /
/// return-existing decision proceeds normally. This is the atomic gate the
/// debounce-aware HTTP entry points use to allow attach/idempotent calls while
/// routing or rejecting true fresh starts without a TOCTOU (issue #499).
///
/// `gate`, when `Some`, enforces the admission gate AUTHORITATIVELY at the point
/// this function decides — under a `FOR UPDATE` lock on any prior run — that it
/// will CREATE a new execution (issue #618, PR #1014). This closes the entire
/// unlocked-pre-read TOCTOU class in one place: a caller no longer needs an
/// unlocked existence check to decide whether to gate. For a non-`TerminateIfRunning`
/// policy the prior is locked via [`try_load_active_execution_for_update`] at the
/// top of the start transaction, its state fed to [`start_will_create_new_execution`],
/// and the gate applied iff a new execution will be created (an idempotent ATTACH
/// admits nothing and is never gated). For `TerminateIfRunning` — which always
/// creates — the gate is applied ONCE, unlocked (a constant decision), *before*
/// the pre-check cancellation, so a blocked start never cancels a prior. A blocked
/// start returns [`HarvestError::AdmissionBlocked`] (rolling the transaction back —
/// no fresh row, no events) and records `harvest.admission.blocked` exactly once.
/// [`GateMode`](crate::admission_gate::GateMode) selects the cache read (fail-closed
/// `Check` for fresh admissions, snapshot-only `CheckCached` for continuation).
///
/// # Errors
///
/// - [`HarvestError::AlreadyExists`] when `RejectDuplicate` rejects.
/// - [`HarvestError::DebounceFreshStart`] when a fresh start is gated by
///   `reject_fresh_if_debounced`.
/// - [`HarvestError::AdmissionBlocked`] when `gate` matches an active gate on a
///   fresh admission.
/// - [`HarvestError::Database`] for insert/query failures.
/// - Propagates queue/event-store failures from the start transaction.
#[allow(clippy::too_many_lines)]
pub async fn start_or_load_workflow_execution_collect(
    conn: &mut AsyncPgConnection,
    request: StartWorkflowParams<'_>,
    in_outer_transaction: bool,
    reject_fresh_if_debounced: bool,
    metrics: Option<&(dyn crate::telemetry::MetricsRecorder + Send + Sync)>,
    gate: Option<crate::admission_gate::GateMode>,
) -> HarvestResult<(
    StartedWorkflowExecution,
    Vec<DeferredTriggerStart>,
    Vec<(ExecutionId, String)>,
    Vec<(String, String)>,
)> {
    let exec_id = request.exec_id;
    let shard_id_value = request.shard_id();

    // Validate delayed start parameters (issue #322)
    if request.start_at.is_some() && request.delay.is_some() {
        return Err(HarvestError::Config(
            "Cannot specify both start_at and delay".to_string(),
        ));
    }

    let max_delay = request
        .max_workflow_start_delay
        .unwrap_or_else(|| chrono::Duration::days(365));

    if let Some(d) = request.delay {
        if d < chrono::Duration::zero() {
            return Err(HarvestError::Config(
                "Start delay cannot be negative".to_string(),
            ));
        }
        if d > max_delay {
            return Err(HarvestError::Config(format!(
                "Requested delay ({d:?}) exceeds maximum permitted delay ({max_delay:?})",
            )));
        }
    }

    let now = Utc::now();

    if let Some(sa) = request.start_at {
        let max_start_at = now + max_delay;
        if sa > max_start_at {
            return Err(HarvestError::Config(format!(
                "Requested start_at ({sa:?}) exceeds maximum permitted delay ({max_start_at:?})",
            )));
        }
    }

    let target_start_time = if let Some(d) = request.delay {
        now + d
    } else if let Some(sa) = request.start_at {
        sa
    } else {
        now
    };

    // Look up the active build policy for this queue. If a policy exists, new
    // executions are stamped with its build_id so workers can enforce routing.
    // Resolved *before* the TerminateIfRunning pre-check so a build-policy DB
    // error returns without leaving a committed pre-check cancellation whose
    // follow-up starts would be lost.
    let policy = build_routing::get_build_policy(conn, request.queue_name).await?;
    let assigned_build = policy.map(|p| p.resolve_assigned_build(exec_id));

    // For TerminateIfRunning: if there is an existing RUNNING execution, cancel
    // it (Transaction 1) before the start transaction below (Transaction 2). A
    // crash between the two leaves the prior workflow CANCELLED with no new run;
    // retrying with the same policy starts fresh on the next attempt because the
    // CANCELLED row is treated as "start fresh" by TerminateIfRunning.
    // Deferred follow-up starts produced by the TerminateIfRunning pre-check's
    // cancellation (the prior run's completion-trigger / parent-close cascade).
    // Collected here via the no-spawn cancel variant rather than spawned inline:
    // this collect function may itself run inside a caller's transaction (e.g. the
    // debounce scanner's fire transaction), where the spawning wrapper
    // `cancel_workflow_execution` would launch follow-ups that a later rollback of
    // that outer transaction could orphan. They are appended to the returned
    // deferred list so the caller spawns them only after its outer commit.
    let mut pre_check_deferred: Vec<DeferredTriggerStart> = Vec::new();
    let mut deferred_checks: Vec<(ExecutionId, String)> = Vec::new();
    let mut pre_check_cancel_metrics: Vec<(String, String)> = Vec::new();

    // Effective active-prior behavior from the two orthogonal axes (issue #685).
    // `terminate_via_pre_check` is the ONE case whose create-vs-attach decision is
    // state-independent (`reuse == TerminateIfRunning` creates for BOTH active and
    // terminal priors) AND resolves to Terminate — so it can use the unlocked
    // POINT-1 gate + the two-transaction pre-check cancel. For `Unspecified` this is
    // exactly `reuse == TerminateIfRunning`, so all four legacy reuse policies keep
    // their pre-#685 admission/pre-check behavior byte-for-byte. Every other
    // conflict-driven Terminate (e.g. `AllowDuplicate` + `TerminateExisting`) is
    // handled atomically INSIDE the start transaction (the active branch's
    // `inline_cancel` + `replace_execution`), where the create-vs-attach decision is
    // made on the locked prior state — never via a pre-check cancel that would then
    // route the just-cancelled prior through a reuse-policy attach branch.
    let effective_conflict =
        effective_active_conflict_behavior(request.reuse_policy, request.conflict_policy);
    let terminate_via_pre_check = request.reuse_policy == WorkflowIdReusePolicy::TerminateIfRunning
        && effective_conflict == ActiveConflictBehavior::Terminate;

    // POINT 1 (issue #618, PR #1014): `terminate_via_pre_check` ALWAYS creates a
    // replacement (state-independent), so its `will_create` is a constant `true`
    // — the gate decision needs no lock. Apply it BEFORE the pre-check
    // cancellation below so a blocked start never cancels the prior run first
    // (cancel-then-block). A block records the count once and returns without
    // touching the DB.
    if let Some(mode) = gate
        && terminate_via_pre_check
        && !reject_fresh_if_debounced
        && let Some((gate_id, reason, scope_kind)) = evaluate_start_gate(
            mode,
            request.workflow_name,
            request.queue_name,
            shard_id_value,
            request.owner,
        )
    {
        record_start_gate_block(metrics, scope_kind, &reason);
        return Err(HarvestError::AdmissionBlocked { gate_id, reason });
    }

    if terminate_via_pre_check
        // Skip the pre-check cancellation when we're going to reject this fresh
        // start for a debounced workflow — otherwise we'd cancel the prior run
        // (Transaction 1) and then reject, leaving it cancelled with no successor.
        && !reject_fresh_if_debounced
        && let Some(existing) =
            try_load_by_key(conn, request.workflow_name, request.workflow_id).await?
        && matches!(existing.state.as_str(), "RUNNING" | "PAUSED")
    {
        let existing_exec_id = ExecutionId::from_uuid(existing.id);
        // Ignore Config errors: the execution may have transitioned to a terminal
        // state between the pre-check and the cancel lock. In that race the prior
        // run is already done, so we just continue to the start transaction below.
        match cancel_workflow_execution_collect(
            conn,
            existing_exec_id,
            "terminated to start new execution",
        )
        .await
        {
            Ok((_cancelled, mut deferred, mut checks, metrics_opt)) => {
                pre_check_deferred.append(&mut deferred);
                deferred_checks.append(&mut checks);
                if let Some(m) = metrics_opt {
                    pre_check_cancel_metrics.push(m);
                }
            }
            Err(HarvestError::Config(_)) => {}
            Err(e) => return Err(e),
        }
    }

    // Apply the server-side ceiling (if any) before computing the deadline.
    // The effective timeout is the minimum of the per-call value and the
    // operator-configured ceiling; this prevents callers from requesting
    // arbitrarily long SLA windows even when they supply an explicit timeout.
    let effective_timeout = match (
        request.execution_timeout,
        request.max_execution_timeout_ceiling,
    ) {
        (Some(t), Some(ceiling)) => Some(t.min(ceiling)),
        (other, _) => other,
    };

    // Compute deadline_at relative to target_start_time (issue #322).
    let deadline_at = effective_timeout.map(|d| target_start_time + d);

    // Compute effective SLA — clamp down to the hard timeout when sla > deadline
    // (issue #487): the hard timeout fires first so the soft signal can never fire.
    // A non-positive SLA budget (<= 0) is treated as "no SLA": persisting an
    // `sla_deadline_at` at or before `started_at` would flag the run as breached
    // on the very next scan, which is never a meaningful budget.
    let effective_sla = match (request.sla, effective_timeout) {
        (Some(sla), _) if sla <= chrono::Duration::zero() => None,
        (Some(sla), Some(hard)) => Some(sla.min(hard)),
        (Some(sla), None) => Some(sla),
        (None, _) => None,
    };
    let sla_deadline_at = effective_sla.map(|d| target_start_time + d);

    // Chain-scoped lifetime cap (issue #617). The ceiling here acts as BOTH a cap
    // AND a fleet-wide default (AC4) — a workflow that under-specifies still gets
    // capped — which diverges deliberately from #243's per-run ceiling (that only
    // caps a *specified* value). See `effective_chain_timeout`. `chain_deadline_at`
    // is inherited verbatim on a same-logical-run continuation (workflow retry #523)
    // and computed fresh on a chain-origin start.
    let effective_chain_timeout = crate::timeout::effective_chain_timeout(
        request.chain_execution_timeout,
        request.max_workflow_chain_timeout_ceiling,
    );
    // `checked_add_signed` (not `+`) because the chain ceiling doubles as the
    // effective value (AC4): an absurd operator ceiling can reach
    // `chrono::Duration::MAX`, and `DateTime + Duration` PANICS on overflow. On
    // overflow we yield `None` (no chain cap) rather than crash the start path.
    let chain_deadline_at = request
        .inherited_chain_deadline_at
        .or_else(|| effective_chain_timeout.and_then(|d| target_start_time.checked_add_signed(d)));

    let row = NewWorkflowExecution {
        continued_from_exec_id: None,
        first_exec_id: None,
        chain_execution_timeout: effective_chain_timeout,
        chain_deadline_at,
        id: exec_id.as_uuid(),
        workflow_name: request.workflow_name,
        workflow_id: request.workflow_id,
        run_id: Uuid::new_v4(),
        shard_id: shard_id_value,
        input: request.input.clone(),
        parent_id: request.parent_id,
        queue_name: request.queue_name,
        execution_timeout: effective_timeout,
        deadline_at,
        sla: effective_sla,
        sla_deadline_at,
        memo: request.memo.clone(),
        search_attrs: request.search_attrs.clone(),
        assigned_build_id: assigned_build.clone(),
        parent_close_policy: None, // root or awaited child; detached uses worker path
        owner: request.owner,
        runbook_url: request.runbook_url,
        severity: request.severity,
        context_headers: request
            .context_headers
            .as_ref()
            .map(|h| serde_json::to_value(h).unwrap_or(serde_json::Value::Null)),
        schedule_id: request.schedule_id,
        scheduled_for: request.scheduled_for,
        workflow_attempt: request.workflow_attempt.cast_signed(),
        workflow_retry_policy: request.workflow_retry_policy.as_ref().map(|p| {
            // Clamp max_attempts to ceiling before persisting.
            let mut p = p.clone();
            if let Some(ceiling) = request.max_workflow_attempts_ceiling {
                p.max_attempts = p.max_attempts.min(ceiling);
            }
            serde_json::to_value(&p).unwrap_or(serde_json::Value::Null)
        }),
        retry_of_exec_id: request.retry_of_exec_id,
        origin: request.origin,
        completion_callbacks: request.completion_callbacks.clone(),
        start_source: Some(request.start_source.as_str()),
        start_source_ref: request.start_source_ref,
        started_by: request.started_by,
    };
    let mut enqueue = EnqueueParams::new(
        request.queue_name.to_owned(),
        TaskType::Workflow,
        request.input.clone(),
    );
    enqueue.workflow_exec_id = Some(exec_id.as_uuid());
    enqueue.required_build_id = assigned_build.clone();
    // ADR-0001 §3: store the caller's trace context so the worker can restore it.
    enqueue.trace_context.clone_from(&request.trace_context);
    enqueue.concurrency_key.clone_from(&request.concurrency_key);
    enqueue.max_concurrent = request.concurrency_limit;
    enqueue.priority = request.priority.as_i32();
    if request.delay.is_some_and(|d| d > chrono::Duration::zero()) || request.start_at.is_some() {
        enqueue.scheduled_at = target_start_time;
    }

    let main_result = Box::pin(conn.transaction::<(
        StartedWorkflowExecution,
        Vec<DeferredTriggerStart>,
        Vec<(ExecutionId, String)>,
        Vec<(String, String)>,
    ), HarvestError, _>(async |conn| {
        let row = row;
        let enqueue = enqueue.clone();
        let request = request.clone();
        // `gate`, `metrics`, and `shard_id_value` are all `Copy`, so the
        // `async |conn|` closure captures them directly from the enclosing
        // function's environment.
        let mut tx_deferred_checks = Vec::new();

        // Authoritative locked gate (issue #618, PR #1014). For every
        // policy EXCEPT TerminateIfRunning (gated unlocked at POINT 1
        // above), take the `FOR UPDATE` lock on any non-sealed prior
        // FIRST — before the INSERT — so the create-vs-attach decision the
        // gate keys on is made on ONE stable, locked state. This is the
        // move that closes the seal-under-lock TOCTOU: a prior that seals
        // (to CONTINUED_AS_NEW / TERMINATED) between an unlocked pre-read
        // and the start is excluded by the `for_update()` filter, so the
        // fresh replacement it would otherwise leak is caught here. The
        // lock is reused by the INSERT / `..._by_key_for_update` load
        // below. `reject_fresh_if_debounced` starts pass `gate = None`, so
        // this never runs on the debounce path.
        // Recompute the fast-path predicate from the (cloned) request:
        // POINT 1 + the pre-check already applied the unlocked gate for the
        // state-independent `terminate_via_pre_check` case, so skip it here.
        // Every other policy (incl. conflict-driven Terminate with a
        // non-`TerminateIfRunning` reuse, and `TerminateIfRunning` +
        // `UseExisting`/`Fail`) takes the locked read and keys the gate on
        // the create-vs-attach decision for the LOCKED prior state.
        let terminate_via_pre_check = request.reuse_policy
            == WorkflowIdReusePolicy::TerminateIfRunning
            && effective_active_conflict_behavior(request.reuse_policy, request.conflict_policy)
                == ActiveConflictBehavior::Terminate;
        if let Some(mode) = gate
            && !terminate_via_pre_check
            && !reject_fresh_if_debounced
        {
            let prior = try_load_active_execution_for_update(
                conn,
                request.workflow_name,
                request.workflow_id,
            )
            .await?;
            if start_will_create_new_execution(
                prior.as_ref().map(|e| e.state.as_str()),
                request.reuse_policy,
                request.conflict_policy,
            ) && let Some((gate_id, reason, scope_kind)) = evaluate_start_gate(
                mode,
                request.workflow_name,
                request.queue_name,
                shard_id_value,
                request.owner,
            ) {
                record_start_gate_block(metrics, scope_kind, &reason);
                return Err(HarvestError::AdmissionBlocked { gate_id, reason });
            }
        }

        // `on_conflict_do_nothing()` (no explicit target) lets Postgres
        // arbitrate against the partial unique index installed by the
        // continue-as-new migration, which only enforces uniqueness on
        // rows whose state is not sealed (`CONTINUED_AS_NEW` or
        // `TERMINATED`). A previously sealed continue-as-new chain or reset
        // source therefore does not block reusing the same
        // (workflow_name, workflow_id).
        let inserted = diesel::insert_into(harvest_workflow_executions::table)
            .values(&row)
            .on_conflict_do_nothing()
            .returning(WorkflowExecution::as_returning())
            .get_result(conn)
            .await
            .optional()
            .map_err(database_error)?;

        if let Some(execution) = inserted {
            // Atomic debounce gate: this INSERT is a fresh start. For a
            // debounced workflow that must go through debounce admission,
            // roll it back and signal the caller (no TOCTOU — decided here
            // under the inserted row, not via an unlocked pre-scan).
            if reject_fresh_if_debounced {
                return Err(HarvestError::DebounceFreshStart {
                    workflow_name: request.workflow_name.to_string(),
                    workflow_id: request.workflow_id.to_string(),
                });
            }
            if request.start_at.is_some_and(|sa| sa < now) {
                return Err(HarvestError::Config(
                    "Requested start_at is in the past".to_string(),
                ));
            }
            // Enforce the input cap only on the fresh-insert path. Duplicates
            // never reach here so the reuse-policy outcome is unaffected.
            if request.max_workflow_input_bytes > 0 {
                let observed = serde_json::to_string(&request.input).map_or(0, |s| s.len() as u64);
                if observed > request.max_workflow_input_bytes {
                    return Err(crate::error::HarvestError::PayloadTooLarge {
                        kind: crate::error::PayloadKind::WorkflowInput,
                        observed_bytes: observed,
                        cap_bytes: request.max_workflow_input_bytes,
                        workflow_type: request.workflow_name.to_string(),
                        activity_name: None,
                    });
                }
            }
            // Resolve last-completion-result carryover (issue #488).
            // Runs inside the same transaction on the same shard-local
            // connection so the read is consistent with the just-inserted
            // row. Excludes the new row (`id != exec_id`) as a safety
            // guard — the new row has state RUNNING, not COMPLETED, so it
            // can never match, but the explicit exclusion is defensive.
            let (carryover_result, carryover_error) = if let Some(sched_id) = request.schedule_id {
                resolve_carryover(conn, sched_id, exec_id.as_uuid(), request.scheduled_for).await?
            } else {
                (None, None)
            };
            let started_event = WorkflowEvent::WorkflowStarted {
                input: request.input.clone(),
                timestamp: target_start_time,
                last_completion_result: carryover_result,
                last_error: carryover_error,
                scheduled_time: request.scheduled_for,
            };
            store::append_events(conn, exec_id, &[started_event], 0).await?;
            queue::enqueue(conn, &enqueue).await?;
            return Ok((
                StartedWorkflowExecution::from_row(execution, true),
                Vec::new(),
                tx_deferred_checks,
                Vec::new(),
            ));
        }

        // INSERT was a no-op: a NON-SEALED prior existed at INSERT time
        // (the active-uniqueness partial index only covers non-sealed
        // states). Lock that prior row to decide what to do.
        //
        // Seal-race retry (issue #685 review, Codex P2): under concurrent
        // `terminate_existing` (or any replace-type) starts of the same
        // `(workflow_name, workflow_id)`, a LOSER can reach this load AFTER
        // a concurrent winner has sealed the prior it locked
        // (-> CONTINUED_AS_NEW/TERMINATED, excluded by this load's
        // non-sealed filter) and inserted a fresh RUNNING replacement. The
        // loser's first `FOR UPDATE` SELECT took its statement snapshot
        // before the winner committed, blocks on the prior's row lock, and
        // -- on the winner's commit -- sees the prior filtered out via
        // EvalPlanQual while the winner's freshly-inserted row is invisible
        // to that same statement's snapshot, so the load returns `None`.
        //
        // Under READ COMMITTED each SELECT statement gets a FRESH snapshot,
        // so re-issuing the load after the winner has committed picks up the
        // replacement RUNNING row and the start CONVERGES (last-writer-wins)
        // instead of surfacing a transient `NotFound`. The loser holds no
        // conflicting lock after a `None` load: `ON CONFLICT DO NOTHING`
        // never locks the conflicting row, and a `None` `FOR UPDATE` locks
        // nothing -- so the retry is safe. Once a row IS found and
        // `FOR UPDATE`-locked, the subsequent `inline_cancel` +
        // `replace_execution` cannot be sealed out from under us (a rival
        // must first take the same lock, which blocks until we commit). Each
        // retry's `FOR UPDATE` naturally blocks on the current lock holder,
        // so this is not a busy-loop; the small backoff only guards the
        // pathological "sealed without a replacement" case (e.g. a
        // concurrent reset), where the cap falls back to the pre-existing
        // `NotFound`.
        //
        // Non-racing requests never reach a `None` here (the prior is still
        // non-sealed under the lock), so this is transparent to the common
        // path across every reuse/conflict policy.
        let existing = {
            let mut existing: Option<WorkflowExecution> = None;
            for attempt in 0..=SEAL_RACE_MAX_LOAD_RETRIES {
                match load_workflow_execution_by_key_for_update(
                    conn,
                    request.workflow_name,
                    request.workflow_id,
                )
                .await
                {
                    Ok(row) => {
                        existing = Some(row);
                        break;
                    }
                    // Seal race: retry with a fresh statement snapshot until
                    // the winner's replacement row is visible or the cap is
                    // exhausted (then fall through to the terminal NotFound).
                    Err(HarvestError::NotFound(_)) if attempt < SEAL_RACE_MAX_LOAD_RETRIES => {
                        tokio::time::sleep(std::time::Duration::from_millis(
                            SEAL_RACE_LOAD_BACKOFF_MS,
                        ))
                        .await;
                    }
                    Err(e) => return Err(e),
                }
            }
            match existing {
                Some(row) => row,
                None => {
                    return Err(HarvestError::NotFound(format!(
                        "workflow execution {}/{}",
                        request.workflow_name, request.workflow_id
                    )));
                }
            }
        };

        // Branch on active-vs-terminal FIRST (issue #685). An ACTIVE
        // (RUNNING/PAUSED) prior is governed by the orthogonal conflict
        // axis; a terminal non-sealed prior is governed by the reuse axis
        // exactly as before (the conflict axis has no effect there).
        if is_active_conflict_state(&existing.state) {
            match effective_active_conflict_behavior(request.reuse_policy, request.conflict_policy)
            {
                // Return the existing running/paused execution unchanged —
                // no new WorkflowStarted event, no task enqueued, no cancel.
                ActiveConflictBehavior::Attach => Ok((
                    StartedWorkflowExecution::from_row(existing, false),
                    Vec::new(),
                    tx_deferred_checks,
                    Vec::new(),
                )),

                ActiveConflictBehavior::Fail => Err(HarvestError::AlreadyExists {
                    existing_exec_id: ExecutionId::from_uuid(existing.id),
                    existing_state: existing.state,
                }),

                // Cancel the active prior and start fresh (this is exactly
                // the pre-#685 `TerminateIfRunning` active branch). Gate it
                // before cancelling anything so a debounced workflow is
                // routed to admission instead. PAUSED is active and occupies
                // the uniqueness slot, so it must be cancelled before
                // `replace_execution` seals it (issue #383). Reaching this
                // point after the two-transaction pre-check cancel is the
                // extreme-concurrency race where the prior is still active
                // under the lock; we inline the cancel here so the new start
                // is not silently blocked.
                //
                // Concurrency note (issue #685 review, FIX 4 + Codex P2):
                // concurrent `terminate_existing` starts of the SAME
                // `(workflow_name, workflow_id)` against one live prior are
                // last-writer-wins and CONVERGE to a single surviving run.
                // `terminate_existing` has no pre-check, so more contenders
                // reach this inline branch than the pre-#685
                // `TerminateIfRunning` path did; a loser that observed the
                // winner seal the prior row it locked is retried internally
                // by the seal-race loop around the load above (a fresh READ
                // COMMITTED snapshot picks up the winner's replacement RUNNING
                // row), so no transient `NotFound` is surfaced. It does NOT
                // corrupt data, deadlock, or double-run (the seal + insert is
                // transactional; `use_existing` never enters this branch).
                ActiveConflictBehavior::Terminate => {
                    if reject_fresh_if_debounced {
                        return Err(HarvestError::DebounceFreshStart {
                            workflow_name: request.workflow_name.to_string(),
                            workflow_id: request.workflow_id.to_string(),
                        });
                    }
                    let tx_cancel_metrics =
                        vec![(existing.workflow_name.clone(), existing.queue_name.clone())];
                    let mut deferred = inline_cancel(
                        conn,
                        ExecutionId::from_uuid(existing.id),
                        &mut tx_deferred_checks,
                    )
                    .await?;
                    let (started_wf, mut extra_deferred) =
                        replace_execution(conn, existing, &row, &enqueue, exec_id, &request, now)
                            .await?;
                    deferred.append(&mut extra_deferred);
                    Ok((started_wf, deferred, tx_deferred_checks, tx_cancel_metrics))
                }
            }
        } else {
            // Terminal non-sealed prior (COMPLETED/FAILED/CANCELLED/
            // TIMED_OUT/SUSPENDED): reuse axis only, byte-for-byte identical
            // to the pre-#685 behavior.
            match request.reuse_policy {
                WorkflowIdReusePolicy::AllowDuplicate => Ok((
                    StartedWorkflowExecution::from_row(existing, false),
                    Vec::new(),
                    tx_deferred_checks,
                    Vec::new(),
                )),

                WorkflowIdReusePolicy::RejectDuplicate => Err(HarvestError::AlreadyExists {
                    existing_exec_id: ExecutionId::from_uuid(existing.id),
                    existing_state: existing.state,
                }),

                WorkflowIdReusePolicy::AllowDuplicateFailedOnly => {
                    match existing.state.as_str() {
                        "FAILED" | "CANCELLED" => {
                            // Replacing a terminal prior is a fresh start.
                            if reject_fresh_if_debounced {
                                return Err(HarvestError::DebounceFreshStart {
                                    workflow_name: request.workflow_name.to_string(),
                                    workflow_id: request.workflow_id.to_string(),
                                });
                            }
                            // Only these two explicitly abnormal states start fresh.
                            let (started_wf, deferred) = replace_execution(
                                conn, existing, &row, &enqueue, exec_id, &request, now,
                            )
                            .await?;
                            Ok((started_wf, deferred, tx_deferred_checks, Vec::new()))
                        }
                        _ => {
                            // COMPLETED, TIMED_OUT, SUSPENDED, or any other
                            // terminal state: return the existing execution
                            // unchanged.
                            Ok((
                                StartedWorkflowExecution::from_row(existing, false),
                                Vec::new(),
                                tx_deferred_checks,
                                Vec::new(),
                            ))
                        }
                    }
                }

                WorkflowIdReusePolicy::TerminateIfRunning => {
                    // TerminateIfRunning always starts fresh (replace). Gate
                    // it before replacing so a debounced workflow is routed
                    // to admission instead. The prior is terminal here (an
                    // active prior takes the branch above), so no inline
                    // cancel is needed.
                    if reject_fresh_if_debounced {
                        return Err(HarvestError::DebounceFreshStart {
                            workflow_name: request.workflow_name.to_string(),
                            workflow_id: request.workflow_id.to_string(),
                        });
                    }
                    let (started_wf, extra_deferred) =
                        replace_execution(conn, existing, &row, &enqueue, exec_id, &request, now)
                            .await?;
                    Ok((started_wf, extra_deferred, tx_deferred_checks, Vec::new()))
                }
            }
        }
    }))
    .await;

    let mut cancel_metrics = pre_check_cancel_metrics;

    match main_result {
        Ok((
            cancel_result,
            mut deferred_starts,
            mut trans_deferred_checks,
            mut trans_cancel_metrics,
        )) => {
            // Spawn order: the pre-check cancellation's follow-ups first, then the
            // start's own deferred follow-ups — all returned for the caller to spawn
            // after its outer commit.
            pre_check_deferred.append(&mut deferred_starts);
            deferred_checks.append(&mut trans_deferred_checks);
            cancel_metrics.append(&mut trans_cancel_metrics);
            Ok((
                cancel_result,
                pre_check_deferred,
                deferred_checks,
                cancel_metrics,
            ))
        }
        Err(e) => {
            // The replacement start failed. A TerminateIfRunning pre-check cancel may
            // already be durable (Transaction 1 committed): for a top-level caller we
            // spawn its follow-ups now, since the cancellation is permanent even
            // though no new run started. For a caller inside an outer transaction the
            // cancellation will be rolled back with that transaction, so we suppress
            // the spawn and let the caller's rollback revert everything.
            if !in_outer_transaction {
                for start in pre_check_deferred {
                    start.spawn();
                }
                for check in deferred_checks {
                    let _ = check_and_report_unfinished_handlers(conn, check.0, &check.1, metrics)
                        .await;
                }
                if let Some(m) = metrics {
                    for (wf_name, q_name) in cancel_metrics {
                        crate::telemetry::emit_workflow_terminal(
                            m,
                            &wf_name,
                            &q_name,
                            crate::telemetry::WorkflowStatus::Cancelled,
                        );
                    }
                }
            }
            Err(e)
        }
    }
}

/// Start a workflow execution or load the existing one, applying the caller's
/// [`WorkflowIdReusePolicy`] when a duplicate `(workflow_name, workflow_id)`
/// collision occurs.
///
/// Thin wrapper around [`start_or_load_workflow_execution_collect`] that spawns
/// any deferred completion-trigger starts before returning.
///
/// ## Policy behaviour
///
/// | Prior state | `AllowDuplicate` | `RejectDuplicate` | `AllowDuplicateFailedOnly` | `TerminateIfRunning` |
/// |-------------|------------------|-------------------|---------------------------|----------------------|
/// | none | create | create | create | create |
/// | RUNNING | return existing | `Err(AlreadyExists)` | return existing | cancel + start fresh |
/// | COMPLETED | return existing | `Err(AlreadyExists)` | return existing | start fresh |
/// | FAILED | return existing | `Err(AlreadyExists)` | start fresh | start fresh |
/// | CANCELLED | return existing | `Err(AlreadyExists)` | start fresh | start fresh |
///
/// For `TerminateIfRunning` + RUNNING the cancel is performed in a separate
/// transaction (Transaction 1) before the start transaction (Transaction 2). A
/// failure between the two leaves the prior workflow CANCELLED with no new run
/// started; the caller can retry with the same policy to get a fresh run.
///
/// # Errors
///
/// - [`HarvestError::AlreadyExists`] when `RejectDuplicate` rejects.
/// - [`HarvestError::Database`] for insert/query failures.
/// - Propagates queue/event-store failures from the start transaction.
pub async fn start_or_load_workflow_execution(
    conn: &mut AsyncPgConnection,
    request: StartWorkflowParams<'_>,
    gate: Option<crate::admission_gate::GateMode>,
) -> HarvestResult<StartedWorkflowExecution> {
    // Top-level caller (`in_outer_transaction = false`): if a TerminateIfRunning
    // pre-check cancellation commits and the replacement start then fails, the
    // collect fn spawns the cancellation's follow-ups itself before returning Err.
    let (result, deferred_starts, deferred_checks, _cancel_metrics) =
        start_or_load_workflow_execution_collect(conn, request, false, false, None, gate).await?;
    for start in deferred_starts {
        start.spawn();
    }
    for check in deferred_checks {
        let _ = check_and_report_unfinished_handlers(conn, check.0, &check.1, None).await;
    }
    Ok(result)
}

pub async fn start_or_load_workflow_execution_with_metrics(
    conn: &mut AsyncPgConnection,
    request: StartWorkflowParams<'_>,
    metrics: Option<&(dyn crate::telemetry::MetricsRecorder + Send + Sync)>,
    gate: Option<crate::admission_gate::GateMode>,
) -> HarvestResult<StartedWorkflowExecution> {
    let (result, deferred_starts, deferred_checks, cancel_metrics) =
        start_or_load_workflow_execution_collect(conn, request, false, false, metrics, gate)
            .await?;
    for start in deferred_starts {
        start.spawn();
    }
    for check in deferred_checks {
        let _ = check_and_report_unfinished_handlers(conn, check.0, &check.1, metrics).await;
    }
    if let Some(m) = metrics {
        for (wf_name, q_name) in cancel_metrics {
            crate::telemetry::emit_workflow_terminal(
                m,
                &wf_name,
                &q_name,
                crate::telemetry::WorkflowStatus::Cancelled,
            );
        }
    }
    Ok(result)
}

/// Outcome of [`start_or_load_workflow_execution_idempotent`] (issue #808).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdempotentStartOutcome {
    /// This request reserved the `idempotency_key` and performed the start.
    Started(StartedWorkflowExecution),
    /// An earlier same-`idempotency_key` start already created the execution;
    /// this request was a no-op returning that existing run.
    Deduplicated {
        /// The already-created execution's id.
        exec_id: ExecutionId,
        /// The already-created execution's `workflow_id`.
        workflow_id: String,
        /// The already-created execution's current state string.
        state: String,
    },
}

/// Start a workflow with request-scoped idempotency (issue #808).
///
/// Reserves a `(workflow_name, idempotency_key)` claim and performs the start in
/// a **single transaction** so the two are atomic: if the start fails (a
/// reuse-policy conflict, a validation error, ...) the reservation rolls back so
/// a retry can start fresh. When the same key was already claimed by an earlier
/// start whose execution still exists, this is a no-op returning that run
/// ([`IdempotentStartOutcome::Deduplicated`]) — no second `WorkflowStarted`
/// event, no second enqueued task.
///
/// The idempotency dedupe **precedes and short-circuits** the reuse-policy
/// matrix keyed on `workflow_id`: a duplicate key never even reaches
/// `start_or_load_workflow_execution_collect`. When the key is *fresh*, the
/// normal reuse-policy semantics apply unchanged.
///
/// Deferred trigger starts, unfinished-handler checks, and cancellation metrics
/// produced by an admitted start are dispatched after the transaction commits,
/// mirroring [`start_or_load_workflow_execution_with_metrics`].
///
/// `window_secs` is the retention window in seconds (see
/// [`crate::start_idempotency`]).
///
/// # Errors
/// - [`HarvestError::AlreadyExists`] when the underlying reuse policy rejects.
/// - Propagates database / queue / event-store failures (rolling the reservation
///   back).
pub async fn start_or_load_workflow_execution_idempotent(
    conn: &mut AsyncPgConnection,
    request: StartWorkflowParams<'_>,
    idempotency_key: &str,
    window_secs: f64,
    metrics: Option<&(dyn crate::telemetry::MetricsRecorder + Send + Sync)>,
    gate: Option<crate::admission_gate::GateMode>,
) -> HarvestResult<IdempotentStartOutcome> {
    let new_exec_id = request.exec_id;
    let shard_id = request.shard_id();

    let (outcome, deferred_starts, deferred_checks, cancel_metrics) =
        Box::pin(conn.transaction::<(
            IdempotentStartOutcome,
            Vec<DeferredTriggerStart>,
            Vec<(ExecutionId, String)>,
            Vec<(String, String)>,
        ), HarvestError, _>(async |conn| {
            let request = request;
            // `gate` and `metrics` are `Copy`; captured directly by the `async |conn|` closure.
            match crate::start_idempotency::reserve_start_idempotency(
                conn,
                request.workflow_name,
                idempotency_key,
                new_exec_id,
                shard_id,
                window_secs,
            )
            .await?
            {
                crate::start_idempotency::StartIdempotencyReservation::Duplicate {
                    exec_id,
                    workflow_id,
                    state,
                } => Ok((
                    IdempotentStartOutcome::Deduplicated {
                        exec_id,
                        workflow_id,
                        state,
                    },
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                )),
                crate::start_idempotency::StartIdempotencyReservation::Reserved => {
                    let workflow_name = request.workflow_name;
                    let (started, ds, dc, cm) = start_or_load_workflow_execution_collect(
                        conn, request, true, false, metrics, gate,
                    )
                    .await?;
                    // The reserve wrote the claim pointing at `new_exec_id`.
                    // If the reuse policy resolved this fresh-key start to an
                    // *existing* run (e.g. AllowDuplicate attaching to a prior
                    // workflow_id collision), `new_exec_id` was never inserted
                    // — repoint the claim at the real run so a subsequent
                    // same-key request deduplicates cleanly instead of hitting
                    // the defensive reclaim path and re-running the start.
                    if started.exec_id != new_exec_id {
                        crate::start_idempotency::repoint_start_idempotency_claim(
                            conn,
                            workflow_name,
                            idempotency_key,
                            started.exec_id,
                        )
                        .await?;
                    }
                    Ok((IdempotentStartOutcome::Started(started), ds, dc, cm))
                }
            }
        }))
        .await?;

    for start in deferred_starts {
        start.spawn();
    }
    for check in deferred_checks {
        let _ = check_and_report_unfinished_handlers(conn, check.0, &check.1, metrics).await;
    }
    if let Some(m) = metrics {
        for (wf_name, q_name) in cancel_metrics {
            crate::telemetry::emit_workflow_terminal(
                m,
                &wf_name,
                &q_name,
                crate::telemetry::WorkflowStatus::Cancelled,
            );
        }
    }
    Ok(outcome)
}

/// Will `start_or_load_workflow_execution_collect` CREATE a new execution (a new
/// admission), given the locked prior run's state and the reuse policy? (Issue
/// #618, F-round18.)
///
/// Pure mirror of `start_or_load_workflow_execution_collect`'s attach-vs-create
/// decision — used by [`gate_checked_start_or_load`] to apply the admission gate
/// exactly when a NEW execution is created, and skip it only for a genuine
/// idempotent ATTACH to an existing run.
///
/// `prior_state` is the state of the `(workflow_name, workflow_id)` row that
/// occupies the active-uniqueness index — i.e. the value returned by
/// [`try_load_active_execution_for_update`] /
/// [`load_workflow_execution_by_key_for_update`], which BOTH exclude the sealed
/// `CONTINUED_AS_NEW`/`TERMINATED` states. So `None` means "no non-sealed prior"
/// (no prior at all, or only a sealed one) — the `INSERT` succeeds and a fresh
/// execution is created. `Some(state)` means the `INSERT` is a no-op and the
/// reuse policy decides attach-vs-replace.
///
/// The matrix mirrors `_collect` exactly (verified against the code, not the
/// `SignalWithStart` matrix, which escalates terminal-prior attaches to fresh
/// starts and is NOT what `_collect` — the path this primitive calls — does):
///
/// - `None` (no non-sealed prior) → CREATE.
/// - `AllowDuplicate` + any non-sealed prior → ATTACH (`from_row(existing, false)`,
///   unconditional — including a terminal `COMPLETED`/`FAILED`/`CANCELLED`/`TIMED_OUT`
///   prior; no new admission).
/// - `RejectDuplicate` + any non-sealed prior → `Err(AlreadyExists)`: no start at
///   all, so no admission — treated as "no create" (the gate is irrelevant since
///   nothing starts).
/// - `AllowDuplicateFailedOnly` + prior `FAILED`/`CANCELLED` → CREATE
///   (`replace_execution`); any other non-sealed state → ATTACH.
/// - `TerminateIfRunning` + any non-sealed prior → CREATE (`replace_execution`,
///   always; the live-prior pre-check cancels first, then it replaces).
///
/// The `conflict_policy` axis (issue #685) overrides the behavior for an
/// *active* (RUNNING/PAUSED) prior: `Fail`/`UseExisting` never create a fresh
/// run; `TerminateExisting` always does; `Unspecified` defers to the reuse
/// policy's native active behavior (so the matrix above is preserved exactly).
#[must_use]
pub fn start_will_create_new_execution(
    prior_state: Option<&str>,
    reuse_policy: WorkflowIdReusePolicy,
    conflict_policy: WorkflowIdConflictPolicy,
) -> bool {
    // `None` = no non-sealed prior occupies the uniqueness slot → the INSERT
    // succeeds → a fresh execution is created (covers "no prior" and "sealed prior").
    prior_state.is_none_or(|state| {
        if is_active_conflict_state(state) {
            // Active prior: the conflict axis decides. Only a Terminate resolution
            // creates a fresh run (cancel-if-live, then replace).
            matches!(
                effective_active_conflict_behavior(reuse_policy, conflict_policy),
                ActiveConflictBehavior::Terminate
            )
        } else {
            // Terminal non-sealed prior (COMPLETED/FAILED/CANCELLED/TIMED_OUT/
            // SUSPENDED): the reuse axis decides, unchanged from today. The
            // conflict axis has no effect on a terminal prior.
            match reuse_policy {
                // AllowDuplicate ATTACHES; RejectDuplicate errors AlreadyExists —
                // neither creates a new execution.
                WorkflowIdReusePolicy::AllowDuplicate | WorkflowIdReusePolicy::RejectDuplicate => {
                    false
                }
                // Replace only a FAILED/CANCELLED prior; otherwise attach.
                WorkflowIdReusePolicy::AllowDuplicateFailedOnly => {
                    matches!(state, "FAILED" | "CANCELLED")
                }
                // Always replaces (cancel-if-live, then replace) → fresh create.
                WorkflowIdReusePolicy::TerminateIfRunning => true,
            }
        }
    })
}

/// The three possible resolutions of an ACTIVE-prior collision (issue #685).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActiveConflictBehavior {
    /// Return `Err(AlreadyExists)`.
    Fail,
    /// Return the existing running/paused execution's handle (`created == false`).
    Attach,
    /// Cancel the active prior and start a fresh run.
    Terminate,
}

/// True for the states that count as "active" for conflict purposes: RUNNING,
/// PAUSED (issue #685). Matches how `TerminateIfRunning`'s pre-check already
/// treats them. SUSPENDED is not a persisted state.
#[must_use]
pub fn is_active_conflict_state(state: &str) -> bool {
    matches!(state, "RUNNING" | "PAUSED")
}

/// Resolve the effective active-prior behavior from the two orthogonal axes
/// (issue #685).
///
/// With `WorkflowIdConflictPolicy::Unspecified`, each reuse policy maps to its
/// documented native active behavior, so no existing caller changes.
#[must_use]
pub const fn effective_active_conflict_behavior(
    reuse: WorkflowIdReusePolicy,
    conflict: WorkflowIdConflictPolicy,
) -> ActiveConflictBehavior {
    match conflict {
        WorkflowIdConflictPolicy::Fail => ActiveConflictBehavior::Fail,
        WorkflowIdConflictPolicy::UseExisting => ActiveConflictBehavior::Attach,
        WorkflowIdConflictPolicy::TerminateExisting => ActiveConflictBehavior::Terminate,
        WorkflowIdConflictPolicy::Unspecified => match reuse {
            // NATIVE active behaviors of the 4 existing reuse policies. AllowDuplicate
            // and AllowDuplicateFailedOnly both natively attach to an active prior.
            WorkflowIdReusePolicy::AllowDuplicate
            | WorkflowIdReusePolicy::AllowDuplicateFailedOnly => ActiveConflictBehavior::Attach,
            WorkflowIdReusePolicy::RejectDuplicate => ActiveConflictBehavior::Fail,
            WorkflowIdReusePolicy::TerminateIfRunning => ActiveConflictBehavior::Terminate,
        },
    }
}

#[cfg(test)]
mod start_will_create_tests {
    use super::start_will_create_new_execution as will_create_impl;
    use crate::types::WorkflowIdConflictPolicy;
    use crate::types::WorkflowIdConflictPolicy::Unspecified;
    use crate::types::WorkflowIdReusePolicy::{
        AllowDuplicate, AllowDuplicateFailedOnly, RejectDuplicate, TerminateIfRunning,
    };

    // Thin shim preserving the pre-#685 2-arg call shape for the legacy cases
    // (conflict = Unspecified). The new active-conflict axis is exercised by the
    // sibling `active_conflict_tests` module.
    fn will_create(prior: Option<&str>, reuse: crate::types::WorkflowIdReusePolicy) -> bool {
        will_create_impl(prior, reuse, WorkflowIdConflictPolicy::Unspecified)
    }

    // The four non-sealed terminal states + the three non-sealed active states that
    // can occupy the uniqueness index (CONTINUED_AS_NEW/TERMINATED are excluded by
    // the index, so they surface as `None` here).
    const TERMINAL: [&str; 4] = ["COMPLETED", "FAILED", "CANCELLED", "TIMED_OUT"];
    const ACTIVE: [&str; 3] = ["RUNNING", "SUSPENDED", "PAUSED"];

    #[test]
    fn no_prior_always_creates() {
        for policy in [
            AllowDuplicate,
            RejectDuplicate,
            AllowDuplicateFailedOnly,
            TerminateIfRunning,
        ] {
            assert!(
                will_create(None, policy),
                "no non-sealed prior → INSERT succeeds → CREATE ({policy:?})"
            );
        }
    }

    #[test]
    fn allow_duplicate_always_attaches_to_a_non_sealed_prior() {
        // AllowDuplicate returns the existing run unconditionally (created=false) —
        // for BOTH live and terminal priors. It is NOT the SignalWithStart matrix
        // (which escalates a terminal-prior attach to a fresh start); this is the
        // standalone start_or_load path the primitive actually calls.
        for state in ACTIVE.iter().chain(TERMINAL.iter()) {
            assert!(
                !will_create(Some(state), AllowDuplicate),
                "AllowDuplicate attaches (no create) for prior state {state}"
            );
        }
    }

    #[test]
    fn reject_duplicate_never_creates_when_a_prior_exists() {
        // Err(AlreadyExists): nothing starts, so nothing is admitted.
        for state in ACTIVE.iter().chain(TERMINAL.iter()) {
            assert!(
                !will_create(Some(state), RejectDuplicate),
                "RejectDuplicate errors (no create) for prior state {state}"
            );
        }
    }

    #[test]
    fn allow_duplicate_failed_only_replaces_only_failed_or_cancelled() {
        assert!(will_create(Some("FAILED"), AllowDuplicateFailedOnly));
        assert!(will_create(Some("CANCELLED"), AllowDuplicateFailedOnly));
        // Every other non-sealed state attaches (no create).
        for state in ["RUNNING", "SUSPENDED", "PAUSED", "COMPLETED", "TIMED_OUT"] {
            assert!(
                !will_create(Some(state), AllowDuplicateFailedOnly),
                "AllowDuplicateFailedOnly attaches (no create) for prior state {state}"
            );
        }
    }

    #[test]
    fn terminate_if_running_always_replaces_a_non_sealed_prior() {
        for state in ACTIVE.iter().chain(TERMINAL.iter()) {
            assert!(
                will_create(Some(state), TerminateIfRunning),
                "TerminateIfRunning replaces (create) for prior state {state}"
            );
        }
    }

    // ---- issue #685: WorkflowIdConflictPolicy active-prior axis ----

    /// The pre-#685 truth table for `start_will_create_new_execution`, encoded
    /// explicitly. Any (reuse × state) with `conflict = Unspecified` MUST equal
    /// this value — the AC-6 no-regression guarantee.
    fn legacy_will_create(state: &str, reuse: crate::types::WorkflowIdReusePolicy) -> bool {
        match reuse {
            AllowDuplicate | RejectDuplicate => false,
            AllowDuplicateFailedOnly => matches!(state, "FAILED" | "CANCELLED"),
            TerminateIfRunning => true,
        }
    }

    #[test]
    fn unspecified_matches_today_for_every_state_and_reuse() {
        let states = [
            "RUNNING",
            "PAUSED",
            "SUSPENDED",
            "COMPLETED",
            "FAILED",
            "CANCELLED",
            "TIMED_OUT",
        ];
        for reuse in [
            AllowDuplicate,
            RejectDuplicate,
            AllowDuplicateFailedOnly,
            TerminateIfRunning,
        ] {
            for state in states {
                assert_eq!(
                    will_create_impl(Some(state), reuse, Unspecified),
                    legacy_will_create(state, reuse),
                    "Unspecified must equal legacy behavior for ({reuse:?}, {state})"
                );
            }
        }
    }

    #[test]
    fn active_prior_use_existing_never_creates() {
        for reuse in [
            AllowDuplicate,
            RejectDuplicate,
            AllowDuplicateFailedOnly,
            TerminateIfRunning,
        ] {
            for state in ["RUNNING", "PAUSED"] {
                assert!(
                    !will_create_impl(Some(state), reuse, WorkflowIdConflictPolicy::UseExisting),
                    "UseExisting attaches (no create) for active prior ({reuse:?}, {state})"
                );
            }
        }
    }

    #[test]
    fn active_prior_fail_never_creates() {
        for reuse in [
            AllowDuplicate,
            RejectDuplicate,
            AllowDuplicateFailedOnly,
            TerminateIfRunning,
        ] {
            for state in ["RUNNING", "PAUSED"] {
                assert!(
                    !will_create_impl(Some(state), reuse, WorkflowIdConflictPolicy::Fail),
                    "Fail errors (no create) for active prior ({reuse:?}, {state})"
                );
            }
        }
    }

    #[test]
    fn active_prior_terminate_existing_always_creates() {
        for reuse in [
            AllowDuplicate,
            RejectDuplicate,
            AllowDuplicateFailedOnly,
            TerminateIfRunning,
        ] {
            for state in ["RUNNING", "PAUSED"] {
                assert!(
                    will_create_impl(
                        Some(state),
                        reuse,
                        WorkflowIdConflictPolicy::TerminateExisting
                    ),
                    "TerminateExisting cancels + starts fresh for active prior ({reuse:?}, {state})"
                );
            }
        }
    }

    #[test]
    fn conflict_axis_has_no_effect_on_a_terminal_prior() {
        // The conflict axis governs ONLY active priors; a terminal prior is
        // decided entirely by the reuse axis regardless of conflict.
        for conflict in [
            WorkflowIdConflictPolicy::Unspecified,
            WorkflowIdConflictPolicy::Fail,
            WorkflowIdConflictPolicy::UseExisting,
            WorkflowIdConflictPolicy::TerminateExisting,
        ] {
            for reuse in [
                AllowDuplicate,
                RejectDuplicate,
                AllowDuplicateFailedOnly,
                TerminateIfRunning,
            ] {
                for state in ["COMPLETED", "FAILED", "CANCELLED", "TIMED_OUT"] {
                    assert_eq!(
                        will_create_impl(Some(state), reuse, conflict),
                        legacy_will_create(state, reuse),
                        "terminal prior ({state}) must ignore conflict {conflict:?} ({reuse:?})"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod active_conflict_tests {
    use super::{
        ActiveConflictBehavior, effective_active_conflict_behavior, is_active_conflict_state,
    };
    use crate::types::WorkflowIdConflictPolicy as C;
    use crate::types::WorkflowIdReusePolicy as R;

    #[test]
    fn active_states_are_running_and_paused_only() {
        assert!(is_active_conflict_state("RUNNING"));
        assert!(is_active_conflict_state("PAUSED"));
        for other in [
            "SUSPENDED",
            "COMPLETED",
            "FAILED",
            "CANCELLED",
            "TIMED_OUT",
            "CONTINUED_AS_NEW",
            "TERMINATED",
        ] {
            assert!(
                !is_active_conflict_state(other),
                "{other} is not an active-conflict state"
            );
        }
    }

    #[test]
    fn explicit_conflict_overrides_every_reuse_policy() {
        for reuse in [
            R::AllowDuplicate,
            R::RejectDuplicate,
            R::AllowDuplicateFailedOnly,
            R::TerminateIfRunning,
        ] {
            assert_eq!(
                effective_active_conflict_behavior(reuse, C::Fail),
                ActiveConflictBehavior::Fail
            );
            assert_eq!(
                effective_active_conflict_behavior(reuse, C::UseExisting),
                ActiveConflictBehavior::Attach
            );
            assert_eq!(
                effective_active_conflict_behavior(reuse, C::TerminateExisting),
                ActiveConflictBehavior::Terminate
            );
        }
    }

    #[test]
    fn unspecified_maps_to_native_active_behavior() {
        assert_eq!(
            effective_active_conflict_behavior(R::AllowDuplicate, C::Unspecified),
            ActiveConflictBehavior::Attach
        );
        assert_eq!(
            effective_active_conflict_behavior(R::RejectDuplicate, C::Unspecified),
            ActiveConflictBehavior::Fail
        );
        assert_eq!(
            effective_active_conflict_behavior(R::AllowDuplicateFailedOnly, C::Unspecified),
            ActiveConflictBehavior::Attach
        );
        assert_eq!(
            effective_active_conflict_behavior(R::TerminateIfRunning, C::Unspecified),
            ActiveConflictBehavior::Terminate
        );
    }
}

#[cfg(test)]
mod resolve_by_workflow_id_tests {
    use super::{ResolvedRun, select_resolved_run};
    use crate::types::ExecutionId;
    use chrono::{DateTime, TimeZone, Utc};

    fn run(state: &str, minute: u32) -> ResolvedRun {
        ResolvedRun {
            exec_id: ExecutionId::new(),
            state: state.to_string(),
            started_at: at(minute),
        }
    }

    fn at(minute: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 0, minute, 0)
            .single()
            .unwrap()
    }

    #[test]
    fn empty_yields_none() {
        assert_eq!(select_resolved_run(Vec::new()), None);
    }

    #[test]
    fn single_terminal_is_returned() {
        let r = run("COMPLETED", 5);
        assert_eq!(select_resolved_run(vec![r.clone()]), Some(r));
    }

    #[test]
    fn only_active_is_returned() {
        let active = run("RUNNING", 3);
        assert_eq!(select_resolved_run(vec![active.clone()]), Some(active));
    }

    #[test]
    fn active_wins_over_terminals_regardless_of_started_at() {
        // The active run is OLDER than a terminal — it must still win (AC2).
        let active = run("RUNNING", 1);
        let newer_terminal = run("COMPLETED", 9);
        let older_terminal = run("FAILED", 2);
        let picked = select_resolved_run(vec![older_terminal, newer_terminal, active.clone()])
            .expect("some");
        assert_eq!(picked, active, "the non-terminal run wins even when older");
    }

    #[test]
    fn no_active_picks_most_recent_terminal_by_started_at() {
        let oldest = run("FAILED", 1);
        let newest = run("COMPLETED", 8);
        let middle = run("CANCELLED", 4);
        let picked = select_resolved_run(vec![oldest, newest.clone(), middle]).expect("some");
        assert_eq!(picked, newest, "most recent terminal by started_at");
    }

    #[test]
    fn paused_counts_as_active() {
        // PAUSED is a non-terminal active state (issue #383).
        let paused = run("PAUSED", 2);
        let terminal = run("COMPLETED", 7);
        let picked = select_resolved_run(vec![terminal, paused.clone()]).expect("some");
        assert_eq!(picked, paused);
    }

    #[test]
    fn sealed_states_are_terminal() {
        // CONTINUED_AS_NEW and TERMINATED are terminal (is_terminal_state), so a
        // fresh RUNNING successor must win over a sealed predecessor.
        let can = run("CONTINUED_AS_NEW", 1);
        let terminated = run("TERMINATED", 2);
        let successor = run("RUNNING", 3);
        let picked = select_resolved_run(vec![can, terminated, successor.clone()]).expect("some");
        assert_eq!(picked, successor);
    }

    #[test]
    fn two_actives_picks_most_recent_defensively() {
        // At most one active run should exist per (name,id), but if two are
        // observed across shards (writable-subset drift), pick the newest.
        let older = run("RUNNING", 1);
        let newer = run("PAUSED", 5);
        let picked = select_resolved_run(vec![older, newer.clone()]).expect("some");
        assert_eq!(picked, newer);
    }

    #[test]
    fn resolve_terminal_states_matches_is_terminal_state() {
        // The SQL filter list and the pure classification used by
        // select_resolved_run both derive from crate::erase::TERMINAL_STATES, so
        // they are equal *by construction* (RESOLVE_TERMINAL_STATES is an alias
        // for that constant). This test asserts that identity plus exact set
        // equality against is_terminal_state in both directions, so any future
        // refactor that reintroduces a second literal list is caught.
        assert!(
            std::ptr::eq(
                super::RESOLVE_TERMINAL_STATES,
                crate::erase::TERMINAL_STATES
            ),
            "RESOLVE_TERMINAL_STATES must alias crate::erase::TERMINAL_STATES \
             (single source of truth)"
        );
        // Every state in the list is terminal.
        for state in super::RESOLVE_TERMINAL_STATES {
            assert!(
                crate::erase::is_terminal_state(state),
                "{state} in RESOLVE_TERMINAL_STATES must be terminal"
            );
        }
        // Every terminal state (per is_terminal_state) is in the list — the
        // reverse direction, so the two sets are exactly equal.
        for state in [
            "COMPLETED",
            "FAILED",
            "CANCELLED",
            "TIMED_OUT",
            "CONTINUED_AS_NEW",
            "TERMINATED",
        ] {
            assert!(
                crate::erase::is_terminal_state(state),
                "{state} must be terminal per is_terminal_state"
            );
            assert!(
                super::RESOLVE_TERMINAL_STATES.contains(&state),
                "{state} must be in RESOLVE_TERMINAL_STATES"
            );
        }
        for active in ["RUNNING", "PAUSED"] {
            assert!(
                !crate::erase::is_terminal_state(active),
                "{active} must not be terminal"
            );
            assert!(
                !super::RESOLVE_TERMINAL_STATES.contains(&active),
                "{active} must not be in RESOLVE_TERMINAL_STATES"
            );
        }
    }
}

/// Transition `existing` to `CONTINUED_AS_NEW` (releasing the partial unique
/// index slot) then insert `new_row` as a fresh execution with its own
/// `WorkflowStarted` event and task queue entry.
async fn replace_execution(
    conn: &mut AsyncPgConnection,
    existing: WorkflowExecution,
    new_row: &NewWorkflowExecution<'_>,
    enqueue: &EnqueueParams,
    new_exec_id: ExecutionId,
    request: &StartWorkflowParams<'_>,
    now: chrono::DateTime<Utc>,
) -> HarvestResult<(StartedWorkflowExecution, Vec<DeferredTriggerStart>)> {
    if request.start_at.is_some_and(|sa| sa < now) {
        return Err(HarvestError::Config(
            "Requested start_at is in the past".to_string(),
        ));
    }

    // Seal the prior execution row as CONTINUED_AS_NEW. This removes it from
    // the partial unique index scope (WHERE state NOT IN sealed states),
    // allowing the new row to be inserted without violating the constraint.
    diesel::update(harvest_workflow_executions::table.find(existing.id))
        .set((
            harvest_workflow_executions::state.eq("CONTINUED_AS_NEW"),
            harvest_workflow_executions::completed_at.eq(Some(Utc::now())),
        ))
        .execute(conn)
        .await
        .map_err(database_error)?;

    let new_execution = diesel::insert_into(harvest_workflow_executions::table)
        .values(new_row)
        .returning(WorkflowExecution::as_returning())
        .get_result(conn)
        .await
        .map_err(database_error)?;

    if request.max_workflow_input_bytes > 0 {
        let observed = serde_json::to_string(&request.input).map_or(0, |s| s.len() as u64);
        if observed > request.max_workflow_input_bytes {
            return Err(crate::error::HarvestError::PayloadTooLarge {
                kind: crate::error::PayloadKind::WorkflowInput,
                observed_bytes: observed,
                cap_bytes: request.max_workflow_input_bytes,
                workflow_type: request.workflow_name.to_string(),
                activity_name: None,
            });
        }
    }
    let start_timestamp = if request.delay.is_some_and(|d| d > chrono::Duration::zero())
        || request.start_at.is_some()
    {
        enqueue.scheduled_at
    } else {
        Utc::now()
    };
    // Resolve scheduled carryover on the replacement path too (issue #488): a reuse
    // policy that replaces a prior row (e.g. AllowDuplicateFailedOnly retrying a failed
    // scheduled slot, or TerminateIfRunning) still carries schedule_id/scheduled_for, so
    // the rerun (and any continue-as-new fork from it) must see the previous fire's
    // carryover rather than behaving like a first scheduled run.
    let (carryover_result, carryover_error) = if let Some(sched_id) = request.schedule_id {
        resolve_carryover(conn, sched_id, new_exec_id.as_uuid(), request.scheduled_for).await?
    } else {
        (None, None)
    };
    let started_event = WorkflowEvent::WorkflowStarted {
        input: request.input.clone(),
        timestamp: start_timestamp,
        last_completion_result: carryover_result,
        last_error: carryover_error,
        scheduled_time: request.scheduled_for,
    };
    store::append_events(conn, new_exec_id, &[started_event], 0).await?;
    queue::enqueue(conn, enqueue).await?;

    Ok((
        StartedWorkflowExecution::from_row(new_execution, true),
        Vec::new(),
    ))
}

/// Inline cancellation for the `TerminateIfRunning` race condition where a
/// RUNNING row appears inside the start transaction despite the pre-check.
/// Appends a `WorkflowCancelled` event, transitions to CANCELLED, and fails
/// open tasks — all within the caller's transaction.
async fn inline_cancel(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    deferred_checks: &mut Vec<(ExecutionId, String)>,
) -> HarvestResult<Vec<DeferredTriggerStart>> {
    let reason = "terminated to start new execution";
    let history = store::load_history(conn, exec_id).await?;
    store::append_events(
        conn,
        exec_id,
        &[WorkflowEvent::WorkflowCancelled {
            reason: reason.to_string(),
        }],
        history.next_event_id,
    )
    .await?;
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .filter(harvest_workflow_executions::state.eq_any(["RUNNING", "PAUSED"]))
        .set((
            harvest_workflow_executions::state.eq("CANCELLED"),
            harvest_workflow_executions::error.eq(Some(reason)),
            harvest_workflow_executions::completed_at.eq(Some(Utc::now())),
            // Clear active-pause metadata when a paused prior run is sealed (#383).
            harvest_workflow_executions::paused_at.eq(None::<chrono::DateTime<Utc>>),
            harvest_workflow_executions::pause_reason.eq(None::<String>),
            harvest_workflow_executions::pause_actor.eq(None::<String>),
        ))
        .execute(conn)
        .await
        .map_err(database_error)?;
    queue::fail_open_tasks_for_execution(conn, exec_id, &format!("workflow cancelled: {reason}"))
        .await?;
    let (mut deferred, closed_children) =
        Box::pin(apply_parent_close_cascade(conn, exec_id)).await?;
    let triggers = crate::completion_trigger::evaluate_triggers_for_execution(
        conn,
        exec_id,
        crate::completion_trigger::TerminalState::Cancelled,
        None,
    )
    .await?;
    deferred.extend(triggers);

    let workflow_name = if let Ok(exec) = load_execution(conn, exec_id).await {
        exec.workflow_name
    } else {
        String::new()
    };
    if !workflow_name.is_empty() {
        deferred_checks.push((exec_id, workflow_name));
    }
    deferred_checks.extend(closed_children);

    Ok(deferred)
}

/// Cancel a running workflow execution.
///
/// Cancellation is a durable terminal transition: this appends a
/// `WorkflowCancelled` event, marks the execution `CANCELLED`, and fails every
/// pending or running task associated with the execution. Repeating the same
/// operation against an already-cancelled execution is idempotent and does not
/// append another event.
///
/// # Errors
///
/// Returns [`HarvestError::NotFound`] when the execution does not exist,
/// [`HarvestError::Config`] when the execution is already terminal for another
/// reason, and [`HarvestError::Database`] for persistence failures.
/// Notify an **awaited** parent that one of its children reached a terminal
/// state out-of-band (operator cancel/terminate), waking the parked parent.
///
/// When a parent blocks on `spawn_child_workflow().await`, the child's terminal
/// transition is normally propagated to the parent by the worker
/// (`wake_parent_for_child_failure`). Operator cancel/terminate seal the child
/// directly, bypassing the worker, so without this the parent parks forever.
///
/// Only **awaited** children are handled here (`parent_id` set,
/// `parent_close_policy` NULL); **detached** children (policy set) are the
/// `apply_parent_close_cascade` parent→child path's responsibility and are
/// skipped. There is no `ChildWorkflowCancelled` event variant, so a cancel and
/// a terminate both surface to the parent as `ChildWorkflowFailed` (the
/// child-await resolves `Err`) — matching the worker failure path and adding no
/// new event variant. Awaited children are co-located on the parent's shard
/// (the worker append-on-child-conn path relies on the same invariant), so this
/// append on the child's connection targets the correct shard.
async fn notify_awaited_parent_of_child_terminal(
    conn: &mut AsyncPgConnection,
    child_exec_id: ExecutionId,
    execution: &WorkflowExecution,
    error: String,
) -> HarvestResult<()> {
    if let Some(parent_uuid) = execution.parent_id
        && execution.parent_close_policy.is_none()
    {
        let parent_exec_id = ExecutionId::from_uuid(parent_uuid);
        // Lock the parent row and skip the wake if it is already terminal (or
        // gone): an awaited child can outlive its parent (e.g. the parent hit its
        // execution timeout while the child kept running), and appending a
        // command-consumable `ChildWorkflowFailed` after the parent's own terminal
        // event would add replay-visible history past closure. The `FOR UPDATE`
        // lock serializes against a concurrent parent termination — which holds
        // the same row lock — so we either observe the parent terminal here and
        // skip, or append before it seals and the parent consumes the event.
        let parent_state: Option<String> = harvest_workflow_executions::table
            .find(parent_exec_id.as_uuid())
            .select(harvest_workflow_executions::state)
            .for_update()
            .first(conn)
            .await
            .optional()
            .map_err(database_error)?;
        if matches!(parent_state, Some(state) if !crate::erase::is_terminal_state(&state)) {
            // #779 (Codex P2): order any DUE child-timeout deadline BEFORE the
            // child terminal (mirrors worker::wake_parent_for_child_completion/
            // _failure) so an over-deadline child that is operator-CANCELLED or
            // -TERMINATED resolves the parent's `spawn_child_workflow_timeout` to
            // the timeout branch (None), not Err. The parent row is already
            // locked FOR UPDATE above; `materialize_due_child_timeout_deadlines`
            // re-locks it (a same-transaction no-op) *before* it takes the due
            // timers FOR UPDATE — the unified execution-row → timer lock order
            // (see its convention comment, issue #779 Codex round-11) — so this
            // operator path and the worker-wake/child-timeout paths cannot ABBA
            // against each other on the same overdue parent. It then appends
            // `TimerFired` under the same parent-row MAX(event_id) discipline as
            // the child terminal below, so the deadline is ordered first.
            crate::worker::materialize_due_child_timeout_deadlines(conn, parent_exec_id).await?;
            store::append_single_event(
                conn,
                parent_exec_id,
                WorkflowEvent::child_workflow_failed(child_exec_id, error),
            )
            .await?;
            queue::wake_workflow_task(conn, parent_exec_id).await?;
        }
    }
    Ok(())
}

/// Cancel a running workflow execution, returning the deferred completion-trigger
/// starts to the caller **without spawning them** (and without recording the
/// terminal metric).
///
/// This is the building block for callers that run the cancellation inside a
/// larger outer transaction (the external-cancel inline persist and outbox
/// paths): the `DeferredTriggerStart`s must only be spawned *after* that outer
/// transaction commits, otherwise trigger workflows could start for a
/// cancellation that later rolls back (issue #492). The plain
/// [`cancel_workflow_execution`] wrapper spawns them and records the metric
/// itself for the common standalone case.
///
/// # Errors
///
/// Same as [`cancel_workflow_execution`].
#[allow(clippy::too_many_lines)]
pub async fn cancel_workflow_execution_collect(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    reason: &str,
) -> HarvestResult<(
    CancelledWorkflowExecution,
    Vec<DeferredTriggerStart>,
    Vec<(ExecutionId, String)>,
    Option<(String, String)>,
)> {
    let reason = reason.trim();
    let reason = if reason.is_empty() {
        "workflow cancellation requested".to_string()
    } else {
        reason.to_string()
    };

    let (cancel_result, deferred_starts, closed_children) =
        Box::pin(conn.transaction::<_, HarvestError, _>(async |conn| {
            let execution = harvest_workflow_executions::table
                .find(exec_id.as_uuid())
                .select(WorkflowExecution::as_select())
                .for_update()
                .first(conn)
                .await
                .optional()
                .map_err(database_error)?
                .ok_or_else(|| HarvestError::NotFound(format!("workflow execution {exec_id}")))?;

            // Cancellation beats pause (issue #383): a PAUSED execution
            // is cancellable just like a RUNNING one; the transition to
            // CANCELLED clears the pending pause record below.
            let prior_state = execution.state.clone();
            match execution.state.as_str() {
                "RUNNING" | "PAUSED" => {}
                "CANCELLED" => {
                    return Ok((
                        CancelledWorkflowExecution::idempotent(exec_id, execution),
                        Vec::new(),
                        Vec::new(),
                    ));
                }
                state => {
                    return Err(HarvestError::Config(format!(
                        "workflow execution {exec_id} is already terminal ({state})"
                    )));
                }
            }

            let deleted_pending = diesel::delete(
                crate::schema::harvest_task_queue::table
                    .filter(
                        crate::schema::harvest_task_queue::workflow_exec_id
                            .eq(Some(exec_id.as_uuid())),
                    )
                    .filter(crate::schema::harvest_task_queue::task_type.eq("workflow"))
                    .filter(crate::schema::harvest_task_queue::state.eq("PENDING"))
                    .filter(crate::schema::harvest_task_queue::scheduled_at.gt(Utc::now())),
            )
            .execute(conn)
            .await
            .map_err(database_error)?;

            let history = store::load_history(conn, exec_id).await?;
            store::append_events(
                conn,
                exec_id,
                &[WorkflowEvent::WorkflowCancelled {
                    reason: reason.clone(),
                }],
                history.next_event_id,
            )
            .await?;

            let completed_at = Utc::now();
            // Mirror resume_workflow_execution: if this execution was PAUSED,
            // push sla_deadline_at forward by the pause span so the SLA scanner
            // does not record a false breach for time spent paused before cancel
            // (issue #383 × #487). Only extend a deadline that was still ahead
            // when the pause began — a deadline already elapsed while RUNNING
            // stays in the past so its breach is still observed by the scanner.
            let new_sla_deadline_at = if prior_state == "PAUSED" {
                execution
                    .sla_deadline_at
                    .map(|d| match execution.paused_at {
                        Some(p) if d > p => d + (completed_at - p).max(chrono::Duration::zero()),
                        _ => d,
                    })
            } else {
                execution.sla_deadline_at
            };
            let updated =
                diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
                    .filter(harvest_workflow_executions::state.eq_any(["RUNNING", "PAUSED"]))
                    .set((
                        harvest_workflow_executions::state.eq("CANCELLED"),
                        harvest_workflow_executions::output.eq(None::<serde_json::Value>),
                        harvest_workflow_executions::error.eq(Some(reason.clone())),
                        harvest_workflow_executions::completed_at.eq(Some(completed_at)),
                        harvest_workflow_executions::sla_deadline_at.eq(new_sla_deadline_at),
                        // Cancellation wins: clear the pending pause record.
                        harvest_workflow_executions::paused_at.eq(None::<chrono::DateTime<Utc>>),
                        harvest_workflow_executions::pause_reason.eq(None::<String>),
                        harvest_workflow_executions::pause_actor.eq(None::<String>),
                    ))
                    .execute(conn)
                    .await
                    .map_err(database_error)?;

            if updated == 0 {
                return Err(HarvestError::Config(format!(
                    "workflow execution {exec_id} is no longer running"
                )));
            }

            let failed_task_count = queue::fail_open_tasks_for_execution(
                conn,
                exec_id,
                &format!("workflow cancelled: {reason}"),
            )
            .await?;

            let total_failed_or_deleted = deleted_pending + failed_task_count;
            // Wake a parent blocked on this child's await (#787): cancelling
            // an awaited child out-of-band must surface to the parent.
            notify_awaited_parent_of_child_terminal(
                conn,
                exec_id,
                &execution,
                format!("child workflow cancelled: {reason}"),
            )
            .await?;
            let (mut deferred, closed_children) = apply_parent_close_cascade(conn, exec_id).await?;
            let triggers = crate::completion_trigger::evaluate_triggers_for_execution(
                conn,
                exec_id,
                crate::completion_trigger::TerminalState::Cancelled,
                None,
            )
            .await?;
            deferred.extend(triggers);

            Ok((
                CancelledWorkflowExecution::newly_cancelled(
                    exec_id,
                    "CANCELLED",
                    reason,
                    total_failed_or_deleted,
                    execution.workflow_name.clone(),
                    execution.queue_name.clone(),
                    prior_state,
                ),
                deferred,
                closed_children,
            ))
        }))
        .await?;

    let mut deferred_checks = Vec::new();
    let cancel_metrics = if cancel_result.newly_cancelled {
        deferred_checks.push((exec_id, cancel_result.workflow_name.clone()));
        Some((
            cancel_result.workflow_name.clone(),
            cancel_result.queue_name.clone(),
        ))
    } else {
        None
    };
    deferred_checks.extend(closed_children);

    Ok((
        cancel_result,
        deferred_starts,
        deferred_checks,
        cancel_metrics,
    ))
}

/// Cancel a running workflow execution.
///
/// Cancellation is a durable terminal transition: this appends a
/// `WorkflowCancelled` event, marks the execution `CANCELLED`, and fails every
/// pending or running task associated with the execution. Repeating the same
/// operation against an already-cancelled execution is idempotent and does not
/// append another event.
///
/// Completion-trigger / parent-close-cascade follow-up starts are spawned after
/// the cancellation transaction commits, and the terminal metric is recorded.
///
/// # Errors
///
/// Returns [`HarvestError::NotFound`] when the execution does not exist,
/// [`HarvestError::Config`] when the execution is already terminal for another
/// reason, and [`HarvestError::Database`] for persistence failures.
pub async fn cancel_workflow_execution(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    reason: &str,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) -> HarvestResult<CancelledWorkflowExecution> {
    let (cancel_result, deferred_starts, deferred_checks, deferred_terminal) =
        cancel_workflow_execution_collect(conn, exec_id, reason).await?;

    for check in deferred_checks {
        let _ = check_and_report_unfinished_handlers(conn, check.0, &check.1, Some(metrics)).await;
    }
    if let Some((workflow_name, queue_name)) = deferred_terminal {
        crate::telemetry::emit_workflow_terminal(
            metrics,
            &workflow_name,
            &queue_name,
            crate::telemetry::WorkflowStatus::Cancelled,
        );
    }

    for start in deferred_starts {
        start.spawn();
    }

    Ok(cancel_result)
}

/// Maximum length of an operator-supplied pause reason (issue #383).
pub const MAX_PAUSE_REASON_LEN: usize = 500;

/// Result of a workflow pause request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PausedWorkflowExecution {
    /// Paused workflow execution ID.
    pub exec_id: ExecutionId,
    /// Execution state after the request (always `"PAUSED"`).
    pub state: String,
    /// Stored pause reason, if any.
    pub reason: Option<String>,
    /// Actor that requested the pause.
    pub actor: String,
    /// `true` when this request performed the `RUNNING → PAUSED` transition;
    /// `false` when the execution was already paused (idempotent).
    pub newly_paused: bool,
    /// When the pause took effect (issue #609): the timestamp recorded by this
    /// request, or the original pause instant for an idempotent repeat.
    pub paused_at: Option<chrono::DateTime<Utc>>,
    /// Workflow type name (for per-workflow metrics without a re-query).
    pub workflow_name: String,
    /// Task queue the execution was dispatched on.
    pub queue_name: String,
}

/// Result of a workflow resume request.
#[derive(Debug, Clone, PartialEq)]
pub struct ResumedWorkflowExecution {
    /// Resumed workflow execution ID.
    pub exec_id: ExecutionId,
    /// Execution state after the request (`"RUNNING"` after a real resume; the
    /// unchanged current state after a no-op resume of a non-paused run).
    pub state: String,
    /// Actor that requested the resume.
    pub actor: String,
    /// Wall-clock seconds the execution spent paused (`0.0` for a no-op).
    pub pause_duration_secs: f64,
    /// `true` when this request performed the `PAUSED → RUNNING` transition;
    /// `false` when the execution was not paused and nothing was mutated
    /// (idempotent no-op, issue #609 AC7).
    pub newly_resumed: bool,
    /// Workflow type name (for per-workflow metrics without a re-query).
    pub workflow_name: String,
    /// Task queue the execution was dispatched on.
    pub queue_name: String,
}

/// Returns `true` when a pause that started at `paused_at` has exceeded the
/// bounded-pause ceiling `max` as of `now` (issue #383).
///
/// Pure helper used by the auto-resume scanner so the expiry decision can be
/// unit-tested without a database. A non-positive `max` is treated as "expire
/// immediately" so a misconfigured zero ceiling does not strand a paused
/// execution forever.
#[must_use]
pub fn pause_timeout_exceeded(
    paused_at: chrono::DateTime<Utc>,
    now: chrono::DateTime<Utc>,
    max: std::time::Duration,
) -> bool {
    // An overflowing ceiling (absurdly large) effectively never expires → false.
    chrono::Duration::from_std(max).is_ok_and(|max| now - paused_at >= max)
}

/// Pause a running workflow execution (issue #383).
///
/// Pausing is a durable, **non-terminal** transition: it appends a
/// [`WorkflowEvent::WorkflowExecutionPaused`] event, marks the execution
/// `PAUSED`, and records the pause audit metadata. The executor enforces the
/// pause at the claim layer — a workflow task belonging to a `PAUSED` execution
/// is never claimed, so no new commands are dispatched. In-flight activities
/// continue to completion; their results are recorded normally and remain
/// queued behind the pause until [`resume_workflow_execution`].
///
/// Repeating the request against an already-paused execution is idempotent and
/// does not append a second event.
///
/// # Errors
///
/// - [`HarvestError::NotFound`] when the execution does not exist (→ 404).
/// - [`HarvestError::Config`] when the execution is already terminal (→ 409),
///   or the reason exceeds [`MAX_PAUSE_REASON_LEN`] (→ 400).
/// - [`HarvestError::Database`] for persistence failures.
pub async fn pause_workflow_execution(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    reason: Option<&str>,
    actor: &str,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) -> HarvestResult<PausedWorkflowExecution> {
    let actor = if actor.trim().is_empty() {
        "anonymous".to_string()
    } else {
        actor.trim().to_string()
    };
    let reason = reason.map(str::trim).filter(|r| !r.is_empty());
    if let Some(r) = reason
        && r.chars().count() > MAX_PAUSE_REASON_LEN
    {
        return Err(HarvestError::Config(format!(
            "pause reason exceeds {MAX_PAUSE_REASON_LEN} characters"
        )));
    }
    let reason = reason.map(ToOwned::to_owned);

    let paused_at = Utc::now();
    let result = Box::pin(
        conn.transaction::<PausedWorkflowExecution, HarvestError, _>(async |conn| {
            let reason = reason.clone();
            let actor = actor.clone();
            let execution = harvest_workflow_executions::table
                .find(exec_id.as_uuid())
                .select(WorkflowExecution::as_select())
                .for_update()
                .first(conn)
                .await
                .optional()
                .map_err(database_error)?
                .ok_or_else(|| HarvestError::NotFound(format!("workflow execution {exec_id}")))?;

            match execution.state.as_str() {
                "RUNNING" => {}
                "PAUSED" => {
                    return Ok(PausedWorkflowExecution {
                        exec_id,
                        state: "PAUSED".to_string(),
                        reason: execution.pause_reason,
                        actor: execution.pause_actor.unwrap_or(actor),
                        newly_paused: false,
                        paused_at: execution.paused_at,
                        workflow_name: execution.workflow_name,
                        queue_name: execution.queue_name,
                    });
                }
                state => {
                    return Err(HarvestError::Config(format!(
                        "workflow execution {exec_id} is already terminal ({state})"
                    )));
                }
            }

            let history = store::load_history(conn, exec_id).await?;
            store::append_events(
                conn,
                exec_id,
                &[WorkflowEvent::WorkflowExecutionPaused {
                    paused_at,
                    reason: reason.clone(),
                    actor: actor.clone(),
                }],
                history.next_event_id,
            )
            .await?;

            let updated =
                diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
                    .filter(harvest_workflow_executions::state.eq("RUNNING"))
                    .set((
                        harvest_workflow_executions::state.eq("PAUSED"),
                        harvest_workflow_executions::paused_at.eq(Some(paused_at)),
                        harvest_workflow_executions::pause_reason.eq(reason.clone()),
                        harvest_workflow_executions::pause_actor.eq(Some(actor.clone())),
                    ))
                    .execute(conn)
                    .await
                    .map_err(database_error)?;

            if updated == 0 {
                return Err(HarvestError::Config(format!(
                    "workflow execution {exec_id} is no longer running"
                )));
            }

            Ok(PausedWorkflowExecution {
                exec_id,
                state: "PAUSED".to_string(),
                reason,
                actor,
                newly_paused: true,
                paused_at: Some(paused_at),
                workflow_name: execution.workflow_name,
                queue_name: execution.queue_name,
            })
        }),
    )
    .await?;

    if result.newly_paused {
        metrics.record_workflow_paused(&result.workflow_name, &result.queue_name);
    }

    Ok(result)
}

/// SQL to shift still-open task rows' cross-retry wall-clock deadline
/// (`schedule_to_close_at`, issue #378) forward by the pause span on resume
/// (issue #609, AC5).
///
/// Binds: `$1` = pause span in microseconds (`BIGINT`), `$2` = the resumed
/// execution's UUID. Only `PENDING`/`RUNNING` rows that actually carry a
/// deadline are touched, mirroring the scanner's enforcement scope.
///
/// **Frozen rows also get their `scheduled_at` shifted** (issue #609
/// post-review hardening, finding 3, option (b)): a `PENDING` row whose
/// pre-shift `schedule_to_close_at` has already elapsed was *frozen* for the
/// remainder of the pause — unclaimable throughout (the claim query requires
/// `schedule_to_close_at > NOW()`) and spared by both the pause-aware
/// `ScheduleToClose` scanner and the frozen-row-aware `ScheduleToStart`
/// scanner. Shifting its `scheduled_at` forward by the same span is harmless
/// (nothing could have claimed it while frozen — claim ordering by
/// `scheduled_at` is only perturbed for rows that were out of the running
/// anyway) and restores both its `schedule_to_start` budget and its
/// retry-backoff position relative to the resume instant; without it, the
/// next `ScheduleToStart` scan would instantly kill the row post-resume
/// because the pause consumed its entire `scheduled_at + schedule_to_start`
/// window. The shift deliberately does NOT apply to unfrozen `PENDING` rows
/// (they stayed claimable during the pause — activities are not pause-gated
/// — so their queue position and genuine worker-capacity signal must stay
/// untouched) nor to `RUNNING` rows (`scheduled_at` is meaningless
/// in-flight). The `CASE` reads the pre-update `schedule_to_close_at` (SQL
/// `UPDATE` semantics: every `SET` expression sees the old row), so the
/// frozen test is evaluated against the pre-shift deadline.
#[must_use]
pub const fn shift_schedule_to_close_on_resume_query() -> &'static str {
    "UPDATE harvest_task_queue \
     SET schedule_to_close_at = schedule_to_close_at + ($1::bigint * INTERVAL '1 microsecond'), \
         scheduled_at = CASE \
             WHEN state = 'PENDING' AND schedule_to_close_at <= NOW() \
             THEN scheduled_at + ($1::bigint * INTERVAL '1 microsecond') \
             ELSE scheduled_at END \
     WHERE workflow_exec_id = $2 \
     AND state IN ('PENDING', 'RUNNING') \
     AND schedule_to_close_at IS NOT NULL"
}

/// SQL to shift still-open **external** tasks' wall-clock deadline
/// (`harvest_external_tasks.schedule_to_close_at`) forward by the pause span
/// on resume (issue #609 post-review hardening, finding 2).
///
/// Binds: `$1` = pause span in microseconds (`BIGINT`), `$2` = the resumed
/// execution's UUID. Mirrors [`shift_schedule_to_close_on_resume_query`] for
/// the external-task table: only `PENDING` rows are open (every other
/// external state — `COMPLETED`/`FAILED`/`TIMED_OUT`/`CANCELLED` — is
/// terminal), and
/// `schedule_to_close_at` is `NOT NULL` there, so no null guard is needed.
///
/// **Lock ordering (issue #609 post-review hardening, third bot-review
/// round):** the `harvest_external_tasks` convention is *task row →
/// execution row* (set by the completion paths in `external_task.rs` and
/// followed by `timeout::enforce_external_task_timeouts`), but this query
/// runs inside the resume transaction, which already holds the execution row
/// lock — the inverted (execution-first) order. Waiting on a task row from
/// here would therefore be an ABBA deadlock against a concurrent
/// completion/scanner holding that task row and waiting on our execution
/// lock. The `FOR UPDATE SKIP LOCKED` subselect makes this shift *never
/// wait* on a task row, so it cannot participate in a lock cycle. Skipping a
/// locked row is semantically safe in every case: a row locked by
/// `complete_externally`/`fail_externally` becomes terminal once we commit
/// (no shift needed); a row locked by `extend_deadline` gets a fresh
/// `NOW()`-anchored deadline (no paused time charged); and a row locked by
/// the timeout scanner was already expired *before* the pause began (the
/// scan excludes PAUSED executions), so the shift would not have rescued it
/// anyway — a deadline elapsed before the pause is still elapsed after
/// shifting by exactly the pause span.
#[must_use]
pub const fn shift_external_schedule_to_close_on_resume_query() -> &'static str {
    "UPDATE harvest_external_tasks \
     SET schedule_to_close_at = schedule_to_close_at + ($1::bigint * INTERVAL '1 microsecond'), \
         updated_at = NOW() \
     WHERE id IN (\
         SELECT id FROM harvest_external_tasks \
         WHERE workflow_exec_id = $2 \
         AND state = 'PENDING' \
         FOR UPDATE SKIP LOCKED\
     )"
}

/// Resume-time pause-span and deadline arithmetic, extracted from
/// [`resume_workflow_execution`].
struct ResumeDeadlineShifts {
    pause_span: chrono::Duration,
    pause_duration_secs: f64,
    new_deadline_at: Option<chrono::DateTime<Utc>>,
    new_sla_deadline_at: Option<chrono::DateTime<Utc>>,
}

/// Pure resume-time deadline arithmetic (issues #243/#487 × #383).
fn resume_deadline_shifts(
    execution: &WorkflowExecution,
    resumed_at: chrono::DateTime<Utc>,
) -> ResumeDeadlineShifts {
    // Clamp the pause span to a non-negative duration so a clock skew that
    // puts `paused_at` ahead of `resumed_at` neither reports a negative pause
    // nor rewinds the deadline.
    let pause_span = execution
        .paused_at
        .map(|p| resumed_at - p)
        .filter(|span| *span > chrono::Duration::zero())
        .unwrap_or_else(chrono::Duration::zero);
    let pause_duration_secs = pause_span.to_std().map_or(0.0, |d| d.as_secs_f64());

    // Pause suspends the SLA clock (issue #383 × #243): push the absolute
    // execution deadline forward by the time spent paused so paused
    // wall-clock does not count against the workflow's `execution_timeout`.
    // `None` (no deadline) stays `None`.
    let new_deadline_at = execution.deadline_at.map(|d| d + pause_span);
    // Also push the soft SLA deadline forward (issue #487): a workflow paused
    // mid-flight should not breach its SLA while paused — BUT only suspend a
    // deadline that was still ahead when the pause began. A deadline already
    // passed before the pause stays in the past so the breach (which occurred
    // while RUNNING) is still observed by the scanner on the next tick after
    // resume, rather than being silently pushed into the future.
    let new_sla_deadline_at = execution
        .sla_deadline_at
        .map(|d| match execution.paused_at {
            Some(p) if d > p => d + pause_span,
            _ => d,
        });

    ResumeDeadlineShifts {
        pause_span,
        pause_duration_secs,
        new_deadline_at,
        new_sla_deadline_at,
    }
}

/// Upper bound for the resume-time pause-span shift, in microseconds:
/// 100 years (~3.15e15 µs).
///
/// `chrono::Duration::num_microseconds` returns `None` for a span too long to
/// represent in `i64` microseconds (> ~292,471 years); binding an `i64::MAX`
/// fallback into the `... * INTERVAL '1 microsecond'` shift SQL would raise a
/// Postgres "timestamp/interval out of range" error and roll back the whole
/// resume transaction. An unrepresentable (or merely astronomically long)
/// span instead clamps to this finite bound — far below Postgres's
/// interval/timestamp range (timestamps top out at year 294276) — so the
/// shift SQL can never raise "out of range" and the resume always commits.
const MAX_PAUSE_SHIFT_MICROS: i64 = 100 * 365 * 24 * 3600 * 1_000_000;

/// Pure clamp for the pause-span → microseconds conversion used by
/// [`shift_schedule_to_close_for_resume`]: non-positive spans pass through
/// (the caller skips the shift entirely for them); the upper end is bounded
/// by [`MAX_PAUSE_SHIFT_MICROS`] so the bound value is always safe to add to
/// a real timestamp in SQL.
fn clamped_pause_shift_micros(pause_span: chrono::Duration) -> i64 {
    pause_span
        .num_microseconds()
        .unwrap_or(i64::MAX)
        .min(MAX_PAUSE_SHIFT_MICROS)
}

/// AC5 (issue #609) × #378: pause also suspends the per-activity cross-retry
/// wall-clock deadline. Shifts every still-open task row's
/// `schedule_to_close_at` forward by the clamped pause span — and, for
/// pause-frozen `PENDING` rows, `scheduled_at` as well (see
/// [`shift_schedule_to_close_on_resume_query`]) — plus the execution's
/// still-`PENDING` external tasks' `schedule_to_close_at` (see
/// [`shift_external_schedule_to_close_on_resume_query`]; issue #609
/// post-review hardening, finding 2). Runs on the caller's connection so it
/// joins the resume transaction.
///
/// This mirrors `deadline_at`'s unconditional shift rather than
/// `sla_deadline_at`'s elapsed-deadline carve-out: shifting by exactly the
/// pause span never grants extra budget — a deadline already elapsed before
/// the pause began (`deadline < paused_at`) is still in the past after the
/// shift (`deadline + span < paused_at + span = resumed_at`), so the scanner
/// times it out on its next tick after resume; a deadline still ahead retains
/// exactly its remaining budget.
async fn shift_schedule_to_close_for_resume(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    pause_span: chrono::Duration,
) -> HarvestResult<()> {
    let pause_span_micros = clamped_pause_shift_micros(pause_span);
    if pause_span_micros > 0 {
        diesel::sql_query(shift_schedule_to_close_on_resume_query())
            .bind::<diesel::sql_types::BigInt, _>(pause_span_micros)
            .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
            .execute(conn)
            .await
            .map_err(database_error)?;
        diesel::sql_query(shift_external_schedule_to_close_on_resume_query())
            .bind::<diesel::sql_types::BigInt, _>(pause_span_micros)
            .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
            .execute(conn)
            .await
            .map_err(database_error)?;
    }
    Ok(())
}

/// Resume a paused workflow execution (issue #383).
///
/// Appends a [`WorkflowEvent::WorkflowExecutionResumed`] event, transitions the
/// execution back to `RUNNING`, clears the pause audit metadata, and wakes the
/// parked workflow task so the workflow advances on its next decision attempt.
/// Timers whose fire time elapsed while paused fire immediately in their
/// original order on the next decision; signals queued during the pause are
/// delivered in order.
///
/// Resuming an execution that is *not* paused — `RUNNING`, or any terminal
/// state — is a **success no-op** (issue #609, AC7): nothing is mutated, no
/// event is appended, and the result reports `newly_resumed: false` with a
/// zero pause duration. Rationale: idempotent operator retry — a resume
/// retried after the run already resumed (or completed post-resume) must not
/// error. This mirrors the Phase 3.32 terminate `newly_terminated: false`
/// no-op precedent.
///
/// # Errors
///
/// - [`HarvestError::NotFound`] when the execution does not exist (→ 404).
/// - [`HarvestError::Database`] for persistence failures.
#[allow(clippy::too_many_lines)]
pub async fn resume_workflow_execution(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    actor: &str,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) -> HarvestResult<ResumedWorkflowExecution> {
    let actor = if actor.trim().is_empty() {
        "anonymous".to_string()
    } else {
        actor.trim().to_string()
    };

    let resumed_at = Utc::now();
    let result = Box::pin(
        conn.transaction::<ResumedWorkflowExecution, HarvestError, _>(async |conn| {
            let actor = actor.clone();
            let execution = harvest_workflow_executions::table
                .find(exec_id.as_uuid())
                .select(WorkflowExecution::as_select())
                .for_update()
                .first(conn)
                .await
                .optional()
                .map_err(database_error)?
                .ok_or_else(|| HarvestError::NotFound(format!("workflow execution {exec_id}")))?;

            if execution.state != "PAUSED" {
                // Success no-op (issue #609, AC7): idempotent operator
                // retry — see the function doc comment.
                return Ok(ResumedWorkflowExecution {
                    exec_id,
                    state: execution.state,
                    actor,
                    pause_duration_secs: 0.0,
                    newly_resumed: false,
                    workflow_name: execution.workflow_name,
                    queue_name: execution.queue_name,
                });
            }

            let ResumeDeadlineShifts {
                pause_span,
                pause_duration_secs,
                new_deadline_at,
                new_sla_deadline_at,
            } = resume_deadline_shifts(&execution, resumed_at);

            let history = store::load_history(conn, exec_id).await?;
            store::append_events(
                conn,
                exec_id,
                &[WorkflowEvent::WorkflowExecutionResumed {
                    resumed_at,
                    actor: actor.clone(),
                }],
                history.next_event_id,
            )
            .await?;

            let updated =
                diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
                    .filter(harvest_workflow_executions::state.eq("PAUSED"))
                    .set((
                        harvest_workflow_executions::state.eq("RUNNING"),
                        harvest_workflow_executions::paused_at.eq(None::<chrono::DateTime<Utc>>),
                        harvest_workflow_executions::pause_reason.eq(None::<String>),
                        harvest_workflow_executions::pause_actor.eq(None::<String>),
                        harvest_workflow_executions::deadline_at.eq(new_deadline_at),
                        harvest_workflow_executions::sla_deadline_at.eq(new_sla_deadline_at),
                    ))
                    .execute(conn)
                    .await
                    .map_err(database_error)?;

            if updated == 0 {
                return Err(HarvestError::Config(format!(
                    "workflow execution {exec_id} is no longer paused"
                )));
            }

            // Refresh any durable-mutex leases this holder owns (issue
            // #691). A PAUSED holder stops running decision cycles, so it
            // stops renewing its leases; the lease-reclaim scanner skips
            // PAUSED holders while paused, but on resume the lease may be
            // stale, so push it forward now (inside the same transaction
            // that flips PAUSED->RUNNING) to preserve mutual exclusion. A
            // no-op when the mutex tables are absent (guarded).
            // cancel/terminate are already covered by the terminal sweep in
            // `evaluate_triggers_for_execution`. Best-effort — mirroring the
            // per-cycle renewal at the top of `process_workflow_task`, a
            // transient renewal failure is logged and tolerated rather than
            // rolling back the resume (the TTL exceeds several decision
            // cycles and the reclaim path re-checks under the advisory
            // lock, so a healthy resume is never failed by a lease renewal
            // hiccup; the resumed holder renews again on its next cycle).
            if let Err(e) = crate::mutex::renew_leases_for_holder(
                conn,
                exec_id,
                crate::mutex::effective_mutex_lease_ttl(),
            )
            .await
            {
                tracing::warn!(
                    exec_id = %exec_id,
                    error = %e,
                    "failed to renew durable-mutex leases on resume (best-effort)"
                );
            }

            shift_schedule_to_close_for_resume(conn, exec_id, pause_span).await?;

            // Re-arm the executor: wake the parked workflow task so the
            // workflow advances on its next decision attempt. Any timer that
            // fired while paused, or signal queued during the pause, is
            // processed when the woken task is claimed.
            queue::wake_workflow_task(conn, exec_id).await?;

            Ok(ResumedWorkflowExecution {
                exec_id,
                state: "RUNNING".to_string(),
                actor,
                pause_duration_secs,
                newly_resumed: true,
                workflow_name: execution.workflow_name,
                queue_name: execution.queue_name,
            })
        }),
    )
    .await?;

    // A no-op resume never actually resumed anything: skip the duration
    // histogram so zero-length phantom samples don't skew percentiles
    // (mirrors pause gating `record_workflow_paused` on `newly_paused`).
    if result.newly_resumed {
        metrics.record_workflow_pause_duration(
            &result.workflow_name,
            &result.queue_name,
            result.pause_duration_secs,
        );
    }

    Ok(result)
}

// ── Operator-mutable triage tags (issue #759) ─────────────────────────────────

/// A partial, tri-state update to an execution's operator-mutable triage
/// metadata (issue #759): `owner`, `severity`, and a free-text `note`.
///
/// Each field independently distinguishes three states, mirroring the
/// `WorkflowSchedulePatch` (issue #771) PATCH contract: `None` ("absent from
/// the request") means "leave this field unchanged"; `Some(None)` ("explicit
/// JSON `null`") means "clear to NULL"; `Some(Some(v))` means "set to `v`".
#[allow(clippy::option_option)]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TriagePatch {
    pub owner: Option<Option<String>>,
    pub severity: Option<Option<String>>,
    pub note: Option<Option<String>>,
}

/// A single triage-field mutation captured for the audit trail (issue #759, AC5).
///
/// Deliberately **not** part of [`TriageOutcome`]'s public shape — the caller
/// (the management API handler) consumes this to build a compact old->new
/// audit summary before it goes out of scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriageFieldChange {
    pub field: &'static str,
    pub old: Option<String>,
    pub new: Option<String>,
}

/// Result of an [`annotate_workflow_execution`] call: the execution's current
/// triage view (issue #759, AC4/AC7) plus the set of fields this call
/// actually changed.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TriageOutcome {
    pub execution_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub severity: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub triage_note: Option<String>,
    /// Fields this call actually changed (old -> new). Never serialized into
    /// the public HTTP response (issue #759 AC5) -- `Vec<TriageFieldChange>`
    /// implements `Default`, which is all `#[serde(skip)]` requires to still
    /// round-trip `Deserialize`.
    #[serde(skip)]
    pub changed_fields: Vec<TriageFieldChange>,
}

/// Resolve a single tri-state triage field against its current stored value
/// (issue #759). Pure -- no I/O, fully unit-testable without a database.
///
/// Returns the resolved (post-patch) value and, when the field was present in
/// the request (`incoming.is_some()`) *and* its new value differs from the
/// current one, a [`TriageFieldChange`] describing the transition. A field
/// absent from the request (`incoming.is_none()`) always resolves to the
/// unchanged current value with no reported change.
#[allow(clippy::option_option)]
fn resolve_triage_field(
    field: &'static str,
    incoming: Option<Option<String>>,
    current: Option<String>,
) -> (Option<String>, Option<TriageFieldChange>) {
    match incoming {
        None => (current, None),
        Some(new_val) => {
            let change = (new_val != current).then(|| TriageFieldChange {
                field,
                old: current.clone(),
                new: new_val.clone(),
            });
            (new_val, change)
        }
    }
}

type TriageColumns = (Option<String>, Option<String>, Option<String>);

async fn load_triage_for_update(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> HarvestResult<TriageColumns> {
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select((
            harvest_workflow_executions::owner,
            harvest_workflow_executions::severity,
            harvest_workflow_executions::triage_note,
        ))
        .for_update()
        .first::<TriageColumns>(conn)
        .await
        .optional()
        .map_err(database_error)?
        .ok_or_else(|| HarvestError::NotFound(format!("workflow execution {exec_id}")))
}

/// Set, update, or clear an execution's operator-mutable triage tags --
/// `owner`, `severity`, and a free-text `note` -- at any point in its life
/// (issue #759).
///
/// A plain metadata update on `harvest_workflow_executions`: appends **no**
/// [`WorkflowEvent`], is never read by the workflow function, and has zero
/// replay-determinism impact (AC2) -- these columns are operator metadata,
/// not event-sourced workflow state, distinct from author-controlled
/// `search_attrs`. Works on any non-purged execution regardless of lifecycle
/// state -- annotation is orthogonal to state (AC6): a `RUNNING`, `PAUSED`,
/// `FAILED`, or `COMPLETED` execution is all annotated identically.
///
/// Idempotent by construction (AC4): applying the same patch twice yields the
/// same final row, since every field is set unconditionally from the
/// (already-locked) resolved value rather than incrementally modified. An
/// empty patch (every field absent) performs no write and reports no changed
/// fields -- the row is only locked and re-read.
///
/// Shard-local: the caller routes `conn` to the execution's own shard via
/// [`ExecutionId::shard`].
///
/// # Errors
///
/// - [`HarvestError::NotFound`] when the execution does not exist (-> 404).
/// - [`HarvestError::Database`] for persistence failures.
pub async fn annotate_workflow_execution(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    patch: TriagePatch,
) -> HarvestResult<TriageOutcome> {
    // The read + update run in one transaction so the `FOR UPDATE` lock in
    // `load_triage_for_update` actually serializes the read-modify-write
    // against a concurrent annotate call, rather than releasing at statement
    // end in autocommit mode (mirrors `set_legal_hold`, issue #747).
    Box::pin(
        conn.transaction::<TriageOutcome, HarvestError, _>(async |conn| {
            let (cur_owner, cur_severity, cur_note) = load_triage_for_update(conn, exec_id).await?;

            let any_field_present =
                patch.owner.is_some() || patch.severity.is_some() || patch.note.is_some();

            let (new_owner, owner_change) = resolve_triage_field("owner", patch.owner, cur_owner);
            let (new_severity, severity_change) =
                resolve_triage_field("severity", patch.severity, cur_severity);
            let (new_note, note_change) = resolve_triage_field("note", patch.note, cur_note);

            let changed_fields: Vec<TriageFieldChange> =
                [owner_change, severity_change, note_change]
                    .into_iter()
                    .flatten()
                    .collect();

            if any_field_present {
                diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
                    .set((
                        harvest_workflow_executions::owner.eq(new_owner.clone()),
                        harvest_workflow_executions::severity.eq(new_severity.clone()),
                        harvest_workflow_executions::triage_note.eq(new_note.clone()),
                    ))
                    .execute(conn)
                    .await
                    .map_err(database_error)?;
            }

            Ok(TriageOutcome {
                execution_id: exec_id.to_string(),
                owner: new_owner,
                severity: new_severity,
                triage_note: new_note,
                changed_fields,
            })
        }),
    )
    .await
}

/// Reactivate a `FAILED` workflow execution so a redriven dead-letter task can
/// resume from existing history (issue #510).
///
/// This is the load-bearing differentiator of redrive over replay: every DLQ
/// write path seals the owning execution `FAILED` (and appends a terminal
/// `WorkflowFailed` event) at quarantine time, so a redrive must reopen the run
/// before re-enqueuing. It:
///
/// 1. appends a [`WorkflowEvent::WorkflowRedriven`] event **after** the
///    superseded terminal `WorkflowFailed` (append-only — no existing event is
///    rewritten, removed, or reordered), and
/// 2. transitions the execution `FAILED → RUNNING`, clearing the recorded
///    failure (`error`/`output`/`completed_at`).
///
/// The matcher marks the `WorkflowRedriven` event and the `WorkflowFailed` it
/// supersedes as transparent, so the re-enqueued task replays existing history
/// and re-issues the failed step live (see [`crate::replay::HistoryMatcher`]).
///
/// Runs on the caller's connection so it **joins the caller's transaction**;
/// the caller is responsible for re-enqueuing the workflow task. The execution
/// row should already be locked `FOR UPDATE` by the caller and confirmed
/// `FAILED`; the guards here are defensive and roll the transaction back if the
/// state has changed.
///
/// # Errors
///
/// Returns [`HarvestError::NotFound`] if the execution does not exist,
/// [`HarvestError::Config`] if it is not `FAILED`, or [`HarvestError::Database`]
/// on persistence failure.
pub async fn reactivate_failed_execution(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    dead_letter_id: Uuid,
    reason: Option<&str>,
) -> HarvestResult<()> {
    let execution = harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .for_update()
        .first(conn)
        .await
        .optional()
        .map_err(database_error)?
        .ok_or_else(|| HarvestError::NotFound(format!("workflow execution {exec_id}")))?;

    if execution.state != "FAILED" {
        return Err(HarvestError::Config(format!(
            "cannot redrive: workflow execution {exec_id} is {} (only FAILED is reactivatable)",
            execution.state
        )));
    }

    // Re-anchor the hard deadline and soft SLA deadline from now so the timeout
    // and SLA scanners see a fresh window rather than the stale past deadlines
    // that were set when the execution first started. Without this, a FAILED
    // execution with a non-NULL `deadline_at` in the past would be immediately
    // re-killed by `enforce_workflow_execution_timeouts` on the next scan tick.
    let now = Utc::now();
    let new_deadline_at = execution.execution_timeout.map(|d| now + d);
    let new_sla_deadline_at = execution.sla.map(|d| now + d);

    let history = store::load_history(conn, exec_id).await?;
    store::append_events(
        conn,
        exec_id,
        &[WorkflowEvent::WorkflowRedriven {
            redriven_at: now,
            dead_letter_id,
            reason: reason.map(str::to_string),
        }],
        history.next_event_id,
    )
    .await?;

    let updated = diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .filter(harvest_workflow_executions::state.eq("FAILED"))
        .set((
            harvest_workflow_executions::state.eq("RUNNING"),
            harvest_workflow_executions::error.eq(None::<String>),
            harvest_workflow_executions::output.eq(None::<serde_json::Value>),
            harvest_workflow_executions::completed_at.eq(None::<chrono::DateTime<Utc>>),
            harvest_workflow_executions::paused_at.eq(None::<chrono::DateTime<Utc>>),
            harvest_workflow_executions::pause_reason.eq(None::<String>),
            harvest_workflow_executions::pause_actor.eq(None::<String>),
            harvest_workflow_executions::deadline_at.eq(new_deadline_at),
            harvest_workflow_executions::sla_deadline_at.eq(new_sla_deadline_at),
            harvest_workflow_executions::sla_breached.eq(false),
            harvest_workflow_executions::sla_breached_at.eq(None::<chrono::DateTime<Utc>>),
        ))
        .execute(conn)
        .await
        .map_err(database_error)?;

    if updated == 0 {
        // The FOR UPDATE-locked row changed state out from under us — roll the
        // transaction back so the appended WorkflowRedriven event is discarded.
        return Err(HarvestError::Config(format!(
            "workflow execution {exec_id} is no longer FAILED"
        )));
    }

    Ok(())
}

/// Auto-resume executions that have been paused longer than `max_pause_duration`
/// (issue #383, bounded pause).
///
/// Scans `PAUSED` executions whose `paused_at` exceeds the ceiling and resumes
/// each with `actor = "auto-resume(timeout)"`. This prevents orphaned-pause
/// backlogs when an operator pauses during an incident and forgets to resume.
///
/// Returns the number of executions auto-resumed.
///
/// # Errors
///
/// Returns the first database or persistence error encountered. Per-execution
/// races (an execution resumed or cancelled concurrently) are skipped, not
/// treated as fatal.
pub async fn auto_resume_expired_pauses(
    conn: &mut AsyncPgConnection,
    max_pause_duration: std::time::Duration,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) -> HarvestResult<usize> {
    let now = Utc::now();
    // Ceiling too large to represent: nothing can exceed it.
    let Ok(max) = chrono::Duration::from_std(max_pause_duration) else {
        return Ok(0);
    };
    let cutoff = now - max;

    let expired: Vec<ExecutionId> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::state.eq("PAUSED"))
        .filter(harvest_workflow_executions::paused_at.is_not_null())
        .filter(harvest_workflow_executions::paused_at.le(Some(cutoff)))
        .select(harvest_workflow_executions::id)
        .load::<Uuid>(conn)
        .await
        .map_err(database_error)?
        .into_iter()
        .map(ExecutionId::from_uuid)
        .collect();

    let mut resumed = 0;
    for exec_id in expired {
        match resume_workflow_execution(conn, exec_id, "auto-resume(timeout)", metrics).await {
            Ok(r) if r.newly_resumed => {
                resumed += 1;
                tracing::warn!(
                    exec_id = %exec_id,
                    "auto-resumed workflow execution after exceeding max pause duration"
                );
            }
            // The execution was resumed or cancelled between the scan and the
            // claim (surfaced as a `newly_resumed: false` no-op since issue
            // #609); not a fatal condition for the sweep.
            Ok(_) | Err(HarvestError::Config(_) | HarvestError::NotFound(_)) => {}
            Err(e) => return Err(e),
        }
    }

    Ok(resumed)
}

/// Count running detached children that would append a parent cascade event.
pub(crate) async fn parent_close_cascade_event_count(
    conn: &mut AsyncPgConnection,
    parent_exec_id: ExecutionId,
) -> HarvestResult<u64> {
    let policies: Vec<Option<String>> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::parent_id.eq(Some(parent_exec_id.as_uuid())))
        .filter(harvest_workflow_executions::parent_close_policy.is_not_null())
        // Must mirror apply_parent_close_cascade's RUNNING|PAUSED selection so the
        // history-cap preflight count matches the events actually appended (#383).
        .filter(harvest_workflow_executions::state.eq_any(["RUNNING", "PAUSED"]))
        .select(harvest_workflow_executions::parent_close_policy)
        .load::<Option<String>>(conn)
        .await
        .map_err(database_error)?;

    policies.into_iter().try_fold(0_u64, |count, policy_opt| {
        let policy = policy_opt
            .expect("filtered by is_not_null")
            .parse::<ParentClosePolicy>()
            .map_err(HarvestError::Config)?;
        Ok(count + u64::from(policy != ParentClosePolicy::Abandon))
    })
}

/// Apply parent-close cascade to all active detached children of `parent_exec_id`.
///
/// Queries children with `parent_close_policy IS NOT NULL AND state IN
/// ('RUNNING','PAUSED')` — a paused child is still active (issue #383).
/// - Abandon: no-op
/// - `RequestCancel`: appends `WorkflowCancelled`, transitions to CANCELLED, fails tasks
/// - `Terminate`: appends `WorkflowFailed`, transitions to FAILED, fails tasks
///
/// Appends a `ChildWorkflowCascadeApplied` event to the parent history for each
/// non-Abandon action. Idempotent: acts only on RUNNING/PAUSED children.
pub(crate) async fn apply_parent_close_cascade(
    conn: &mut AsyncPgConnection,
    parent_exec_id: ExecutionId,
) -> HarvestResult<(Vec<DeferredTriggerStart>, Vec<(ExecutionId, String)>)> {
    use crate::store;

    // PAUSED is a non-terminal active state (issue #383): a paused child is
    // still an active child, so the parent-close cascade must reach it too —
    // otherwise it could be resumed after the parent closed despite a
    // RequestCancel/Terminate policy.
    let running_children: Vec<(Uuid, String, Option<String>)> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::parent_id.eq(Some(parent_exec_id.as_uuid())))
        .filter(harvest_workflow_executions::parent_close_policy.is_not_null())
        .filter(harvest_workflow_executions::state.eq_any(["RUNNING", "PAUSED"]))
        .select((
            harvest_workflow_executions::id,
            harvest_workflow_executions::workflow_name,
            harvest_workflow_executions::parent_close_policy,
        ))
        .load::<(Uuid, String, Option<String>)>(conn)
        .await
        .map_err(database_error)?;

    let mut deferred = Vec::new();
    let mut closed_children = Vec::new();

    for (child_uuid, child_workflow_name, policy_opt) in running_children {
        let child_exec_id = ExecutionId::from_uuid(child_uuid);
        let policy_str = policy_opt.expect("filtered by is_not_null");
        let policy = policy_str
            .parse::<ParentClosePolicy>()
            .map_err(HarvestError::Config)?;

        let (action, mut child_deferred, mut child_closed) = match policy {
            ParentClosePolicy::Abandon => (None, Vec::new(), Vec::new()),
            ParentClosePolicy::RequestCancel => {
                let (success, d, c) = cascade_cancel_detached_child(
                    conn,
                    child_exec_id,
                    &child_workflow_name,
                    "parent closed",
                )
                .await?;
                (success.then_some("request_cancel"), d, c)
            }
            ParentClosePolicy::Terminate => {
                let (success, d, c) = cascade_terminate_detached_child(
                    conn,
                    child_exec_id,
                    &child_workflow_name,
                    "ParentClosed",
                )
                .await?;
                (success.then_some("terminate"), d, c)
            }
        };

        let Some(action_str) = action else {
            continue;
        };

        deferred.append(&mut child_deferred);
        closed_children.append(&mut child_closed);

        store::append_single_event(
            conn,
            parent_exec_id,
            crate::event::WorkflowEvent::ChildWorkflowCascadeApplied {
                child_id: child_exec_id,
                policy,
                action: action_str.to_string(),
            },
        )
        .await?;
    }

    Ok((deferred, closed_children))
}

async fn cascade_cancel_detached_child(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    workflow_name: &str,
    reason: &str,
) -> HarvestResult<(bool, Vec<DeferredTriggerStart>, Vec<(ExecutionId, String)>)> {
    let mut deferred_starts = Vec::new();
    let mut closed_executions = Vec::new();

    let updated = diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .filter(harvest_workflow_executions::state.eq_any(["RUNNING", "PAUSED"]))
        .set((
            harvest_workflow_executions::state.eq("CANCELLED"),
            harvest_workflow_executions::error.eq(Some(reason.to_string())),
            harvest_workflow_executions::completed_at.eq(Some(Utc::now())),
            // Clear active-pause metadata when a paused child is made terminal so
            // it doesn't appear "terminal and still paused" in APIs/UI (#383).
            harvest_workflow_executions::paused_at.eq(None::<chrono::DateTime<Utc>>),
            harvest_workflow_executions::pause_reason.eq(None::<String>),
            harvest_workflow_executions::pause_actor.eq(None::<String>),
        ))
        .execute(conn)
        .await
        .map_err(database_error)?;
    if updated == 0 {
        return Ok((false, deferred_starts, closed_executions));
    }
    closed_executions.push((exec_id, workflow_name.to_string()));

    store::append_single_event(
        conn,
        exec_id,
        WorkflowEvent::WorkflowCancelled {
            reason: reason.to_string(),
        },
    )
    .await?;
    queue::fail_open_tasks_for_execution(
        conn,
        exec_id,
        &format!("workflow cancelled by parent close: {reason}"),
    )
    .await?;
    let (mut child_deferred, mut child_closed) =
        Box::pin(apply_parent_close_cascade(conn, exec_id)).await?;
    deferred_starts.append(&mut child_deferred);
    closed_executions.append(&mut child_closed);

    let triggers = crate::completion_trigger::evaluate_triggers_for_execution(
        conn,
        exec_id,
        crate::completion_trigger::TerminalState::Cancelled,
        None,
    )
    .await?;
    deferred_starts.extend(triggers);
    Ok((true, deferred_starts, closed_executions))
}

async fn cascade_terminate_detached_child(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    workflow_name: &str,
    reason: &str,
) -> HarvestResult<(bool, Vec<DeferredTriggerStart>, Vec<(ExecutionId, String)>)> {
    let mut deferred_starts = Vec::new();
    let mut closed_executions = Vec::new();

    let updated = diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .filter(harvest_workflow_executions::state.eq_any(["RUNNING", "PAUSED"]))
        .set((
            harvest_workflow_executions::state.eq("FAILED"),
            harvest_workflow_executions::error.eq(Some(reason.to_string())),
            harvest_workflow_executions::completed_at.eq(Some(Utc::now())),
            // Clear active-pause metadata when a paused child is made terminal so
            // it doesn't appear "terminal and still paused" in APIs/UI (#383).
            harvest_workflow_executions::paused_at.eq(None::<chrono::DateTime<Utc>>),
            harvest_workflow_executions::pause_reason.eq(None::<String>),
            harvest_workflow_executions::pause_actor.eq(None::<String>),
        ))
        .execute(conn)
        .await
        .map_err(database_error)?;
    if updated == 0 {
        return Ok((false, deferred_starts, closed_executions));
    }
    closed_executions.push((exec_id, workflow_name.to_string()));

    store::append_single_event(
        conn,
        exec_id,
        WorkflowEvent::workflow_failed(reason.to_string()),
    )
    .await?;
    queue::fail_open_tasks_for_execution(
        conn,
        exec_id,
        &format!("workflow terminated by parent close: {reason}"),
    )
    .await?;
    let (mut child_deferred, mut child_closed) =
        Box::pin(apply_parent_close_cascade(conn, exec_id)).await?;
    deferred_starts.append(&mut child_deferred);
    closed_executions.append(&mut child_closed);

    let triggers = crate::completion_trigger::evaluate_triggers_for_execution(
        conn,
        exec_id,
        crate::completion_trigger::TerminalState::Failed,
        None,
    )
    .await?;
    deferred_starts.extend(triggers);
    Ok((true, deferred_starts, closed_executions))
}

/// Hard-finalize a workflow execution to `TERMINATED` regardless of its
/// current live state.
///
/// `cancel_workflow_execution` is the graceful path: it requires the
/// execution to be `RUNNING`/`PAUSED` and the workflow body must observe
/// the cancellation cooperatively (`is_cancelled`/`check_cancellation`).
/// `terminate_workflow_execution` is the forceful operator escape hatch —
/// it seals a live run (`RUNNING`/`SUSPENDED`/`PAUSED`) in the
/// `TERMINATED` state unilaterally, surfacing to result-awaiting callers as
/// [`HarvestError::Terminated`] (distinct from a cooperative `CANCELLED`
/// and from a `FAILED`). Open task rows are still failed so workers don't
/// keep chewing on a torn-down execution.
///
/// This emits a [`WorkflowEvent::WorkflowCancelled`] (no new event variant —
/// the append-only contract is intact) and records the supplied reason on
/// the row. It is **idempotent against any already-terminal state**
/// (`COMPLETED`/`FAILED`/`CANCELLED`/`TIMED_OUT`/`CONTINUED_AS_NEW`/
/// `TERMINATED`): the call is a non-mutating no-op that appends no second
/// terminal transition.
///
/// # Errors
///
/// Returns [`HarvestError::NotFound`] when the execution does not exist
/// and [`HarvestError::Database`] for persistence failures.
#[allow(clippy::too_many_lines)]
pub async fn terminate_workflow_execution_collect(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    reason: &str,
) -> HarvestResult<(
    CancelledWorkflowExecution,
    Vec<DeferredTriggerStart>,
    Vec<(ExecutionId, String)>,
    Option<(String, String)>,
)> {
    let reason = reason.trim();
    let reason = if reason.is_empty() {
        "workflow termination requested".to_string()
    } else {
        reason.to_string()
    };

    let (cancel_result, deferred_starts, closed_children) =
        Box::pin(conn.transaction::<_, HarvestError, _>(async |conn| {
            let execution = harvest_workflow_executions::table
                .find(exec_id.as_uuid())
                .select(WorkflowExecution::as_select())
                .for_update()
                .first(conn)
                .await
                .optional()
                .map_err(database_error)?
                .ok_or_else(|| HarvestError::NotFound(format!("workflow execution {exec_id}")))?;

            // Idempotent no-op against any already-terminal state
            // (issue #504, AC #7): never append a duplicate terminal
            // transition. `idempotent` returns the existing state with
            // `newly_cancelled = false`.
            if crate::erase::is_terminal_state(&execution.state) {
                return Ok((
                    CancelledWorkflowExecution::idempotent(exec_id, execution),
                    Vec::new(),
                    Vec::new(),
                ));
            }

            let history = store::load_history(conn, exec_id).await?;
            store::append_events(
                conn,
                exec_id,
                &[WorkflowEvent::WorkflowCancelled {
                    reason: reason.clone(),
                }],
                history.next_event_id,
            )
            .await?;

            let completed_at = Utc::now();
            // Mirror cancel/resume: if this execution was PAUSED, push
            // sla_deadline_at forward by the pause span so the SLA scanner
            // does not record a false breach for time spent paused before
            // terminate (issue #383 × #487). The scanner judges terminal rows
            // by `sla_deadline_at < COALESCE(completed_at, NOW())`, so leaving
            // a stale deadline that elapsed during the pause would count a
            // suspended-clock run as breached. Only extend a deadline that was
            // still ahead when the pause began — a deadline already elapsed
            // while RUNNING stays in the past so its breach is still observed.
            let new_sla_deadline_at = if execution.state == "PAUSED" {
                execution
                    .sla_deadline_at
                    .map(|d| match execution.paused_at {
                        Some(p) if d > p => d + (completed_at - p).max(chrono::Duration::zero()),
                        _ => d,
                    })
            } else {
                execution.sla_deadline_at
            };

            // No state-precondition filter: operator override force-writes
            // the live run to the sealed TERMINATED state.
            diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
                .set((
                    harvest_workflow_executions::state.eq("TERMINATED"),
                    harvest_workflow_executions::output.eq(None::<serde_json::Value>),
                    harvest_workflow_executions::error.eq(Some(reason.clone())),
                    harvest_workflow_executions::completed_at.eq(Some(completed_at)),
                    harvest_workflow_executions::sla_deadline_at.eq(new_sla_deadline_at),
                    // Clear active-pause metadata when terminating a paused
                    // run so it doesn't appear terminal-and-paused (#383).
                    harvest_workflow_executions::paused_at.eq(None::<chrono::DateTime<Utc>>),
                    harvest_workflow_executions::pause_reason.eq(None::<String>),
                    harvest_workflow_executions::pause_actor.eq(None::<String>),
                ))
                .execute(conn)
                .await
                .map_err(database_error)?;

            let failed_task_count = queue::fail_open_tasks_for_execution(
                conn,
                exec_id,
                &format!("workflow terminated: {reason}"),
            )
            .await?;
            // Wake a parent blocked on this child's await (#787):
            // force-terminating an awaited child out-of-band must surface
            // to the parent so it does not park forever.
            notify_awaited_parent_of_child_terminal(
                conn,
                exec_id,
                &execution,
                format!("child workflow terminated: {reason}"),
            )
            .await?;
            let (mut deferred, closed_children) = apply_parent_close_cascade(conn, exec_id).await?;
            // Force-terminate fires `Terminated` completion triggers, NOT
            // `Cancelled` — a force-kill is distinct from a cooperative
            // cancellation downstream (issue #504). Operators opt into
            // terminate cascades by registering `terminal_states:
            // ["Terminated"]`.
            let triggers = crate::completion_trigger::evaluate_triggers_for_execution(
                conn,
                exec_id,
                crate::completion_trigger::TerminalState::Terminated,
                None,
            )
            .await?;
            deferred.extend(triggers);

            let prior_state = execution.state.clone();
            Ok((
                CancelledWorkflowExecution::newly_cancelled(
                    exec_id,
                    "TERMINATED",
                    reason,
                    failed_task_count,
                    execution.workflow_name.clone(),
                    execution.queue_name.clone(),
                    prior_state,
                ),
                deferred,
                closed_children,
            ))
        }))
        .await?;

    let mut deferred_checks = Vec::new();
    let mut terminate_metrics = None;
    if cancel_result.newly_cancelled {
        deferred_checks.push((exec_id, cancel_result.workflow_name.clone()));
        if matches!(
            cancel_result.prior_state.as_str(),
            "RUNNING" | "SUSPENDED" | "PAUSED"
        ) {
            terminate_metrics = Some((
                cancel_result.workflow_name.clone(),
                cancel_result.queue_name.clone(),
            ));
        }
    }
    deferred_checks.extend(closed_children);

    Ok((
        cancel_result,
        deferred_starts,
        deferred_checks,
        terminate_metrics,
    ))
}

/// Hard-finalize a workflow execution to `TERMINATED` regardless of its
/// current live state.
///
/// `cancel_workflow_execution` is the graceful path: it requires the
/// execution to be `RUNNING`/`PAUSED` and the workflow body must observe
/// the cancellation cooperatively (`is_cancelled`/`check_cancellation`).
/// `terminate_workflow_execution` is the forceful operator escape hatch —
/// it seals a live run (`RUNNING`/`SUSPENDED`/`PAUSED`) in the
/// `TERMINATED` state unilaterally, surfacing to result-awaiting callers as
/// [`HarvestError::Terminated`] (distinct from a cooperative `CANCELLED`
/// and from a `FAILED`). Open task rows are still failed so workers don't
/// keep chewing on a torn-down execution.
///
/// This emits a [`WorkflowEvent::WorkflowCancelled`] (no new event variant —
/// the append-only contract is intact) and records the supplied reason on
/// the row. It is **idempotent against any already-terminal state**
/// (`COMPLETED`/`FAILED`/`CANCELLED`/`TIMED_OUT`/`CONTINUED_AS_NEW`/
/// `TERMINATED`): the call is a non-mutating no-op that appends no second
/// terminal transition.
///
/// # Errors
///
/// Returns [`HarvestError::NotFound`] when the execution does not exist
/// and [`HarvestError::Database`] for persistence failures.
pub async fn terminate_workflow_execution(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    reason: &str,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) -> HarvestResult<CancelledWorkflowExecution> {
    let (cancel_result, deferred_starts, deferred_checks, deferred_terminal) =
        terminate_workflow_execution_collect(conn, exec_id, reason).await?;

    for check in deferred_checks {
        let _ = check_and_report_unfinished_handlers(conn, check.0, &check.1, Some(metrics)).await;
    }
    if let Some((workflow_name, queue_name)) = deferred_terminal {
        crate::telemetry::emit_workflow_terminal(
            metrics,
            &workflow_name,
            &queue_name,
            crate::telemetry::WorkflowStatus::Terminated,
        );
    }
    for start in deferred_starts {
        start.spawn();
    }

    Ok(cancel_result)
}

/// Non-locking lookup used for the `TerminateIfRunning` pre-check outside any
/// transaction. Returns `None` if no active execution exists.
pub async fn try_load_by_key(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
) -> HarvestResult<Option<WorkflowExecution>> {
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(workflow_name))
        .filter(harvest_workflow_executions::workflow_id.eq(workflow_id))
        .filter(harvest_workflow_executions::state.ne_all(["CONTINUED_AS_NEW", "TERMINATED"]))
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(database_error)
}

/// Any-state existence check keyed on `(workflow_name, workflow_id)`.
///
/// Returns `true` when a row exists in ANY state, including the sealed
/// `CONTINUED_AS_NEW`/`TERMINATED` states that both [`try_load_by_key`] and
/// [`try_load_active_execution_for_update`] deliberately exclude, and the
/// terminal `COMPLETED`/`FAILED`/`CANCELLED`/`TIMED_OUT` states that the
/// active-only lock excludes.
///
/// The completion-trigger cross-shard relay (issue #618, F-round15) uses this to
/// recognise a stale one-shot outbox row whose deterministic target already ran:
/// a target sealed or completed since it was first started must be treated as
/// DELIVERED, never as a fresh (gated) admission. Read-only, unlocked (the
/// relay's exactly-one-path-per-row invariant makes it stable — see
/// `completion_trigger::relay_gate_checked_start`); do NOT use this as a
/// create/attach existence lock — that is [`try_load_active_execution_for_update`]'s
/// job and it must stay active-only for the webhook and fresh-create paths.
pub async fn execution_exists_by_key(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
) -> HarvestResult<bool> {
    diesel::select(diesel::dsl::exists(
        harvest_workflow_executions::table
            .filter(harvest_workflow_executions::workflow_name.eq(workflow_name))
            .filter(harvest_workflow_executions::workflow_id.eq(workflow_id)),
    ))
    .get_result::<bool>(conn)
    .await
    .map_err(database_error)
}

// ─────────────────────────────────────────────────────────────────────────────
// Business-id ("latest run") resolution (issue #805)
// ─────────────────────────────────────────────────────────────────────────────

/// A single candidate run resolved for a `(workflow_name, workflow_id)` pair
/// on one shard (issue #805).
///
/// Deliberately lightweight — only the fields the "latest run" ranking needs —
/// so business-id resolution never loads a full [`WorkflowExecution`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedRun {
    /// The internal execution id of the resolved run.
    pub exec_id: ExecutionId,
    /// The run's execution state (e.g. `RUNNING`, `COMPLETED`).
    pub state: String,
    /// When the run started; used to break ties among terminal runs.
    pub started_at: chrono::DateTime<Utc>,
}

/// The terminal states used by the SQL ranking filter in
/// [`resolve_execution_id_by_workflow_id`].
///
/// This is an alias for [`crate::erase::TERMINAL_STATES`], the single source of
/// truth for terminal-state classification. Because the SQL filter and the pure
/// [`select_resolved_run`] ranking (which delegates to
/// [`crate::erase::is_terminal_state`], itself a `TERMINAL_STATES.contains`)
/// both derive from the same constant, the two can never disagree about which
/// states are terminal — the drift is eliminated by construction rather than
/// merely guarded by a test.
const RESOLVE_TERMINAL_STATES: &[&str] = crate::erase::TERMINAL_STATES;

/// Pick the single best "latest run" from per-shard candidates (issue #805).
///
/// Ranking, matching the business-id resolution rule: if any candidate is
/// **non-terminal** (an active run — at most one should exist per
/// `(workflow_name, workflow_id)`), return the non-terminal one with the
/// greatest `started_at`; otherwise the **most recent terminal** run by
/// `started_at`; otherwise `None`.
///
/// Pure and no-DB: the plugin resolver fans out across shards, collects each
/// shard's best candidate, and calls this to pick the global winner. Terminal
/// classification delegates to [`crate::erase::is_terminal_state`] so it cannot
/// drift from the rest of the engine.
#[must_use]
pub fn select_resolved_run(candidates: Vec<ResolvedRun>) -> Option<ResolvedRun> {
    // Prefer the most-recently-started non-terminal (active) run.
    if let Some(active) = candidates
        .iter()
        .filter(|c| !crate::erase::is_terminal_state(&c.state))
        .max_by_key(|c| c.started_at)
        .cloned()
    {
        return Some(active);
    }
    // No active run: fall back to the most-recently-started terminal run.
    candidates.into_iter().max_by_key(|c| c.started_at)
}

/// Resolve the best "latest run" for `(workflow_name, workflow_id)` on ONE
/// shard (issue #805).
///
/// Returns the shard's single best candidate under the same ranking as
/// [`select_resolved_run`]: a non-terminal (active) run if one exists on this
/// shard, otherwise this shard's most-recent run (which is terminal when no
/// active run exists), otherwise `None`. Read-only.
///
/// The plugin fans this out across every shard and merges the per-shard results
/// with [`select_resolved_run`], so this returning a terminal while another
/// shard holds an active run still resolves correctly. Both queries hit the
/// `UNIQUE (workflow_name, workflow_id)` index.
pub async fn resolve_execution_id_by_workflow_id(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
) -> HarvestResult<Option<ResolvedRun>> {
    // Active-first: a non-terminal run for this key (at most one per shard via
    // the partial unique index). `started_at DESC` is defensive.
    let active = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(workflow_name))
        .filter(harvest_workflow_executions::workflow_id.eq(workflow_id))
        .filter(harvest_workflow_executions::state.ne_all(RESOLVE_TERMINAL_STATES))
        .order(harvest_workflow_executions::started_at.desc())
        .select((
            harvest_workflow_executions::id,
            harvest_workflow_executions::state,
            harvest_workflow_executions::started_at,
        ))
        .first::<(Uuid, String, chrono::DateTime<Utc>)>(conn)
        .await
        .optional()
        .map_err(database_error)?;
    if let Some((id, state, started_at)) = active {
        return Ok(Some(ResolvedRun {
            exec_id: ExecutionId::from_uuid(id),
            state,
            started_at,
        }));
    }

    // No active run on this shard: the most-recently-started row is the
    // most-recent terminal.
    let terminal = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(workflow_name))
        .filter(harvest_workflow_executions::workflow_id.eq(workflow_id))
        .order(harvest_workflow_executions::started_at.desc())
        .select((
            harvest_workflow_executions::id,
            harvest_workflow_executions::state,
            harvest_workflow_executions::started_at,
        ))
        .first::<(Uuid, String, chrono::DateTime<Utc>)>(conn)
        .await
        .optional()
        .map_err(database_error)?;
    Ok(terminal.map(|(id, state, started_at)| ResolvedRun {
        exec_id: ExecutionId::from_uuid(id),
        state,
        started_at,
    }))
}

/// Returns `true` when `err` reports that a target resolved by
/// [`resolve_execution_id_by_workflow_id`] raced to `CONTINUED_AS_NEW`
/// between resolution and a delivery/cancel attempt on it (issue #751).
///
/// A `CONTINUED_AS_NEW` predecessor always commits atomically together with
/// its successor (see [`crate::worker::persist_workflow_continue_as_new`]),
/// so whenever this fires a live successor is guaranteed to already exist
/// under the same `(workflow_name, workflow_id)` key — the caller must
/// re-resolve and retry rather than treating this as either success (cancel)
/// or a definitive failure (signal), unlike every *other* terminal state,
/// which is a conclusive dead end.
///
/// Both [`cancel_workflow_execution_collect`] and
/// [`crate::signal::send_signal_idempotent`] report this as a
/// [`HarvestError::Config`] whose message ends in `"(CONTINUED_AS_NEW)"` (each
/// with an otherwise independently-worded prefix), so matching only the
/// suffix is stable across the two call sites without coupling to either
/// one's exact wording.
pub(crate) fn is_continued_as_new_race(err: &HarvestError) -> bool {
    matches!(err, HarvestError::Config(msg) if msg.ends_with("(CONTINUED_AS_NEW)"))
}

/// Outcome of resolving a `workflow_id`-targeted cancel request to a concrete
/// execution and attempting to cancel it (issue #751).
#[derive(Debug)]
pub enum ByIdCancelOutcome {
    /// The resolved run was cancelled. Carries the same payload
    /// [`cancel_workflow_execution_collect`] returns on success, so callers
    /// can spawn deferred starts / record metrics identically to the
    /// `ExecutionId`-targeted path.
    Cancelled {
        cancelled: Box<CancelledWorkflowExecution>,
        deferred: Vec<DeferredTriggerStart>,
        closed_children: Vec<(ExecutionId, String)>,
        metrics: Option<(String, String)>,
    },
    /// No run has ever existed for this `(workflow_name, workflow_id)`.
    /// Callers apply the same grace-window policy used for an unrecognized
    /// `ExecutionId` before reporting a definitive `target_unknown` failure.
    NoRunFound,
    /// The resolved run — whichever run was "current" at resolution time —
    /// is already terminal. The goal ("nothing is running under this
    /// business key") is already met: a no-op success, never an error.
    AlreadyTerminal,
    /// The resolved run raced to `CONTINUED_AS_NEW` between resolution and
    /// the cancel attempt: a live successor now exists under the same
    /// business key. Not success, not failure — the caller should leave this
    /// attempt unresolved so a later attempt (immediate inline retry or the
    /// next outbox tick) re-resolves and finds the live successor.
    RacedToSuccessor,
}

/// Resolve a `(workflow_name, workflow_id)` target to its current run and
/// cancel it, closing the continue-as-new race by construction (issue #751,
/// AC2/AC3/AC5).
///
/// Because [`resolve_execution_id_by_workflow_id`]'s "active" query excludes
/// every [`crate::erase::TERMINAL_STATES`] state (including
/// `CONTINUED_AS_NEW`) and a `CONTINUED_AS_NEW` predecessor's successor
/// always commits in the very same transaction, a **direct** resolution can
/// never itself return `state == "CONTINUED_AS_NEW"` — the successor would
/// already have won the "active" query, or (if the successor has since also
/// gone terminal) sorts later than the predecessor and wins the "most
/// recent" fallback query instead. Any terminal state observed directly here
/// is therefore genuine, not a masked live run.
///
/// The one race that DOES require handling is the tiny window between this
/// function's own resolve step and its cancel attempt: if the resolved run
/// continues-as-new in that window, [`is_continued_as_new_race`] recognises
/// it and this function reports [`ByIdCancelOutcome::RacedToSuccessor`]
/// rather than misreporting either success or failure.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] for persistence failures. Every other
/// outcome (no run found, already terminal, raced to a successor, or
/// cancelled) is reported as `Ok`.
pub async fn resolve_and_cancel_by_workflow_id(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    reason: &str,
) -> HarvestResult<ByIdCancelOutcome> {
    let Some(run) = resolve_execution_id_by_workflow_id(conn, workflow_name, workflow_id).await?
    else {
        return Ok(ByIdCancelOutcome::NoRunFound);
    };
    if crate::erase::is_terminal_state(&run.state) {
        return Ok(ByIdCancelOutcome::AlreadyTerminal);
    }
    match cancel_workflow_execution_collect(conn, run.exec_id, reason).await {
        Ok((cancelled, deferred, closed_children, metrics)) => Ok(ByIdCancelOutcome::Cancelled {
            cancelled: Box::new(cancelled),
            deferred,
            closed_children,
            metrics,
        }),
        Err(HarvestError::NotFound(_)) => {
            // Vanishingly unlikely (the row existed a moment ago under this
            // same connection) but not impossible under a concurrent retention
            // sweep; treat identically to "never existed".
            Ok(ByIdCancelOutcome::NoRunFound)
        }
        Err(ref e) if is_continued_as_new_race(e) => Ok(ByIdCancelOutcome::RacedToSuccessor),
        Err(HarvestError::Config(_)) => {
            // Any other terminal state discovered via the race window between
            // our resolve and the cancel attempt's own lock — the target
            // completed/failed/etc. on its own; goal still met.
            Ok(ByIdCancelOutcome::AlreadyTerminal)
        }
        Err(e) => Err(e),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SignalWithStart (issue #244)
// ─────────────────────────────────────────────────────────────────────────────

/// Parameters for the atomic `signal_with_start` primitive.
///
/// Combines the inputs of [`StartWorkflowParams`] with the signal name,
/// payload, and optional idempotency key, so a single shard-local transaction
/// can either start a fresh execution and stage the signal for its first
/// dispatch, or attach the signal to an existing live execution.
#[derive(Debug, Clone)]
pub struct SignalWithStartParams<'a> {
    pub workflow_name: &'a str,
    pub workflow_id: &'a str,
    pub exec_id: ExecutionId,
    pub input: serde_json::Value,
    pub parent_id: Option<Uuid>,
    pub queue_name: &'a str,
    pub execution_timeout: Option<chrono::Duration>,
    pub memo: Option<serde_json::Value>,
    pub search_attrs: Option<serde_json::Value>,
    pub reuse_policy: WorkflowIdReusePolicy,
    pub trace_context: Option<TraceContextCarrier>,
    /// Server-side ceiling applied to `execution_timeout`. Forwarded to
    /// [`StartWorkflowParams::max_execution_timeout_ceiling`].
    pub max_execution_timeout_ceiling: Option<chrono::Duration>,
    /// Chain-scoped lifetime cap DURATION for a fresh chain-origin start (issue
    /// #617). Forwarded to [`StartWorkflowParams::chain_execution_timeout`]. A
    /// signal-with-start always begins a fresh chain origin, so there is no
    /// `inherited_chain_deadline_at`. `None` = caller specified no chain cap.
    pub chain_execution_timeout: Option<chrono::Duration>,
    /// Server-side ceiling on the chain cap, doubling as a fleet-wide default
    /// (issue #617). Forwarded to
    /// [`StartWorkflowParams::max_workflow_chain_timeout_ceiling`].
    pub max_workflow_chain_timeout_ceiling: Option<chrono::Duration>,
    /// Pre-resolved concurrency group key. Forwarded to
    /// [`StartWorkflowParams::concurrency_key`].
    pub concurrency_key: Option<String>,
    /// Per-key concurrency cap. Forwarded to
    /// [`StartWorkflowParams::concurrency_limit`].
    pub concurrency_limit: Option<u32>,
    pub signal_name: &'a str,
    pub signal_payload: serde_json::Value,
    /// Optional dedup key. When present, repeated calls with the same
    /// `(workflow_exec_id, idempotency_key)` deliver the signal exactly once.
    /// Backed by a partial unique index on `harvest_signals`; the `NULL` case
    /// preserves the pre-existing `send_signal` behaviour.
    pub idempotency_key: Option<String>,
    /// Payload cap for `start_input` (bytes). Enforced only on the fresh-start
    /// path — attach paths ignore this field. Zero means no cap.
    pub max_workflow_input_bytes: u64,
    /// Payload cap for `signal_payload` (bytes). Zero means no cap.
    pub max_signal_payload_bytes: u64,
    pub owner: Option<&'a str>,
    pub runbook_url: Option<&'a str>,
    pub severity: Option<&'a str>,
    pub context_headers: Option<std::collections::HashMap<String, String>>,
    /// Soft SLA budget forwarded to [`StartWorkflowParams::sla`] (issue #487).
    pub sla: Option<chrono::Duration>,
    /// When `true`, reject (with [`HarvestError::DebounceFreshStart`]) any call
    /// that would create a **fresh** execution rather than attach to a live
    /// (RUNNING/PAUSED) prior. Set by the HTTP handler for a debounced workflow
    /// so an attach/idempotent call is preserved while a fresh start is rejected
    /// — decided atomically under this call's lock (issue #499).
    pub reject_fresh_if_debounced: bool,
    /// Effective workflow-level retry policy (issue #523). Forwarded to
    /// [`StartWorkflowParams::workflow_retry_policy`] on fresh starts.
    pub workflow_retry_policy: Option<serde_json::Value>,
    /// Server-side ceiling on retry attempts. Forwarded to
    /// [`StartWorkflowParams::max_workflow_attempts_ceiling`].
    pub max_workflow_attempts_ceiling: Option<u32>,
    /// The target workflow's registered [`WorkflowInfo`], consulted only to
    /// validate `input` against its published JSON Schema (issue #373) —
    /// and only on a genuine fresh start, never on attach (see
    /// [`HarvestError::InputValidationFailed`]). `None` skips validation
    /// entirely (schema-less workflow, or a caller — e.g. the typed client
    /// stub — that intentionally never validates, matching every other
    /// schema-validation call site being HTTP-JSON-boundary-only).
    pub workflow_info: Option<&'a WorkflowInfo>,
    /// Workflow-start provenance override for a fresh start (issue #740).
    /// `None` records the default [`StartSource::SignalWithStart`]; a webhook
    /// `SignalsWithStart` delegation sets `Some(StartSource::Webhook)` so the
    /// fresh run records `webhook` provenance.
    pub start_source_override: Option<StartSource>,
}

/// Result of a [`signal_with_start_workflow_execution`] call.
///
/// `started_fresh` distinguishes a freshly inserted run from one attached to
/// an existing live execution. `signal_delivered` reports whether the signal
/// row was actually queued: it is `false` when the prior execution is in a
/// terminal state (no signal can land) or when the idempotency key matched a
/// row that was already enqueued.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalWithStartOutcome {
    pub exec_id: ExecutionId,
    pub workflow_name: String,
    pub workflow_id: String,
    pub state: String,
    pub started_fresh: bool,
    pub signal_delivered: bool,
}

/// Atomically start a workflow if no live run for `(workflow_name, workflow_id)`
/// exists (subject to `reuse_policy`), or signal the existing run otherwise.
///
/// ## Outcome matrix (4 reuse policies × prior execution state)
///
/// | Prior state          | `AllowDuplicate`            | `RejectDuplicate`     | `AllowDuplicateFailedOnly`   | `TerminateIfRunning`         |
/// |----------------------|-----------------------------|-----------------------|------------------------------|------------------------------|
/// | none                 | start + signal              | start + signal        | start + signal               | start + signal               |
/// | RUNNING / SUSPENDED  | signal existing             | `Err(AlreadyExists)`  | signal existing              | cancel + start + signal      |
/// | COMPLETED            | start fresh + signal        | `Err(AlreadyExists)`  | start fresh + signal         | start fresh + signal         |
/// | FAILED               | start fresh + signal        | `Err(AlreadyExists)`  | start fresh + signal         | start fresh + signal         |
/// | CANCELLED            | start fresh + signal        | `Err(AlreadyExists)`  | start fresh + signal         | start fresh + signal         |
/// | TERMINATED           | start fresh + signal        | start fresh + signal  | start fresh + signal         | start fresh + signal         |
///
/// "Suspended" workflows are observable to the engine as `RUNNING` — they are
/// running executions whose handler is awaiting external input — so they
/// behave identically to `RUNNING` in this matrix.
///
/// `TERMINATED` is the *sealed* state set by the reset path (`reset.rs`): the
/// row is released from the partial unique index over
/// `(workflow_name, workflow_id) WHERE state NOT IN ('CONTINUED_AS_NEW',
/// 'TERMINATED')`. A `TERMINATED` row is treated as if the `workflow_id` were
/// free, including under `RejectDuplicate`. This matches the broader
/// [`start_or_load_workflow_execution`] semantics; the reset operator
/// explicitly opted the prior row out of the uniqueness scope.
///
/// Note: `AllowDuplicate` and `AllowDuplicateFailedOnly` diverge from the
/// standalone [`start_or_load_workflow_execution`] behaviour for terminal
/// priors: the standalone start returns the existing terminal row, while
/// signal-with-start escalates to a fresh start so the signal can land. This
/// keeps the spec's "no signal silently dropped" invariant intact.
/// `RejectDuplicate` and `TerminateIfRunning` keep their original semantics.
///
/// ## Event ordering
///
/// On a **fresh start**, only `WorkflowStarted` is appended in this call. The
/// signal is staged as a pending `harvest_signals` row (with the supplied
/// idempotency key) and the worker's existing `ingest_pending_signals` path
/// promotes it to a `SignalReceived` event *before* the workflow function is
/// dispatched on its first tick. No new `WorkflowEvent` variant is needed.
///
/// On an **attach**, the signal row is queued in the same transaction and the
/// running workflow's task is woken; the existing signal-delivery path picks
/// it up at the next dispatch boundary.
///
/// On a **cancel + start** (`TerminateIfRunning` + RUNNING prior), the prior
/// execution receives a `WorkflowCancelled` event and is moved to `CANCELLED`,
/// then the fresh start + signal lands — all inside this function's outer
/// transaction. Diesel-async demotes the inner `conn.transaction(..)` blocks
/// in `cancel_workflow_execution` and `start_or_load_workflow_execution` to
/// savepoints under the outer one, so a crash mid-flight rolls back the
/// cancellation as well: the prior workflow stays RUNNING and the caller can
/// retry from a clean state. (This is a strictly safer guarantee than the
/// standalone `start_or_load_workflow_execution` two-transaction shape, which
/// can leave a CANCELLED orphan on a crash; the wrapping transaction here
/// turns that into an all-or-nothing operation.)
///
/// Check a payload value against a byte cap, returning `PayloadTooLarge` when exceeded.
/// Zero cap means uncapped (no check performed).
fn check_sws_payload_cap(
    value: &serde_json::Value,
    kind: crate::error::PayloadKind,
    cap: u64,
    workflow_type: &str,
) -> HarvestResult<()> {
    if cap == 0 {
        return Ok(());
    }
    let observed = serde_json::to_string(value).map_or(0, |s| s.len() as u64);
    if observed > cap {
        return Err(crate::error::HarvestError::PayloadTooLarge {
            kind,
            observed_bytes: observed,
            cap_bytes: cap,
            workflow_type: workflow_type.to_string(),
            activity_name: None,
        });
    }
    Ok(())
}

/// Validate `input` against `workflow_info`'s published JSON Schema
/// (issue #373), when one is registered. Called only on a genuine fresh
/// start (see the two call sites in
/// [`signal_with_start_workflow_execution_with_metrics`]) — an attach never
/// writes `start_input`, so validating it there would reject a call that
/// will never actually use the value it's rejecting (issue #918 review).
fn check_sws_input_schema(
    input: &serde_json::Value,
    workflow_info: Option<&WorkflowInfo>,
) -> HarvestResult<()> {
    let Some(info) = workflow_info else {
        return Ok(());
    };
    info.validate_input(input)
        .map_err(|violations| HarvestError::InputValidationFailed { violations })
}
/// # Errors
///
/// - [`HarvestError::AlreadyExists`] when `RejectDuplicate` rejects.
/// - Propagates queue/event-store failures from the start transaction.
#[allow(clippy::too_many_lines)] // orchestrates idempotency, cap checks, start, TOCTOU retry, and signal atomically
pub async fn signal_with_start_workflow_execution(
    conn: &mut AsyncPgConnection,
    request: SignalWithStartParams<'_>,
) -> HarvestResult<SignalWithStartOutcome> {
    signal_with_start_workflow_execution_with_metrics(conn, request, None, None).await
}

/// # Errors
///
/// - [`HarvestError::AlreadyExists`] when `RejectDuplicate` rejects.
/// - Propagates queue/event-store failures from the start transaction.
#[allow(clippy::too_many_lines)] // orchestrates idempotency, cap checks, start, TOCTOU retry, and signal atomically
pub async fn signal_with_start_workflow_execution_with_metrics(
    conn: &mut AsyncPgConnection,
    request: SignalWithStartParams<'_>,
    metrics: Option<&(dyn crate::telemetry::MetricsRecorder + Send + Sync)>,
    // Admission gate (issue #618, PR #1014). Threaded into the fresh-create start
    // calls below so a signal-with-start that CREATES a new execution is gated
    // AUTHORITATIVELY under the primitive's `FOR UPDATE` lock — closing the
    // request-internal TOCTOU that an unlocked pre-check alone leaves open (the
    // exact seal-under-lock/gate-raised-mid-request window the plain `api` start
    // route was moved off an unlocked pre-read to close). `None` for the
    // continuation/example callers; the HTTP route passes `Some(GateMode::Check)`.
    // The `reject_fresh_if_debounced` branch stays `None` — debounce owns its own
    // admission (bypass-counted scanner relay).
    gate: Option<crate::admission_gate::GateMode>,
) -> HarvestResult<SignalWithStartOutcome> {
    // Single outer transaction: pre-cancel + start (or attach) + signal insert commit
    // atomically. Inner conn.transaction calls become savepoints under this wrapper.
    let (outcome, deferred_starts, deferred_checks, cancel_metrics) =
        Box::pin(conn.transaction::<(
            SignalWithStartOutcome,
            Vec<DeferredTriggerStart>,
            Vec<(ExecutionId, String)>,
            Vec<(String, String)>,
        ), HarvestError, _>(async |conn| {
            let request = request;
            let mut deferred_starts = Vec::new();
            let mut deferred_checks = Vec::new();
            let mut cancel_metrics = Vec::new();

            // Cross-execution dedupe: scope by (workflow_name, workflow_id, key)
            // so escalation/reset paths on a new exec_id don't re-queue the signal.
            if let Some(key) = request.idempotency_key.as_deref()
                && let Some(prior) = lookup_idempotent_signal_dedupe(
                    conn,
                    request.workflow_name,
                    request.workflow_id,
                    key,
                )
                .await?
            {
                return Ok((
                    SignalWithStartOutcome {
                        exec_id: ExecutionId::from_uuid(prior.id),
                        workflow_name: prior.workflow_name,
                        workflow_id: prior.workflow_id,
                        state: prior.state,
                        started_fresh: false,
                        signal_delivered: false,
                    },
                    deferred_starts,
                    deferred_checks,
                    cancel_metrics,
                ));
            }

            // 1. Resolve build routing policy for this queue (pre-lock lookup)
            let policy = build_routing::get_build_policy(conn, request.queue_name).await?;
            let _effective_policy = policy.clone().map(|p| p.build_id);

            // 2. Pre-cancel active execution under TerminateIfRunning before start,
            // matching the pre-check pattern in start_or_load_workflow_execution.
            // Runs in a savepoint: if the signal or start cap checks fail below,
            // the cancellation is rolled back.
            let mut pre_check_deferred = Vec::new();
            if request.reuse_policy == WorkflowIdReusePolicy::TerminateIfRunning
                && !request.reject_fresh_if_debounced
                && let Some(prior) =
                    try_load_by_key(conn, request.workflow_name, request.workflow_id).await?
                && matches!(prior.state.as_str(), "RUNNING" | "PAUSED")
            {
                let prior_exec_id = ExecutionId::from_uuid(prior.id);
                match cancel_workflow_execution_collect(
                    conn,
                    prior_exec_id,
                    "terminated by signal-with-start",
                )
                .await
                {
                    Ok((_cancelled, mut deferred, mut checks, metrics_opt)) => {
                        pre_check_deferred.append(&mut deferred);
                        deferred_checks.append(&mut checks);
                        if let Some(m) = metrics_opt {
                            cancel_metrics.push(m);
                        }
                    }
                    Err(HarvestError::NotFound(_)) => {}
                    Err(e) => return Err(e),
                }
            }
            deferred_starts.append(&mut pre_check_deferred);

            // Upgrade AllowDuplicate / AllowDuplicateFailedOnly to TerminateIfRunning
            // when the prior run is terminal so the signal always lands on a live
            // execution ("no signal silently dropped" invariant from issue #244).
            // For a debounced workflow, skip the upgrade: a terminal prior must not
            // be escalated to a fresh start here — the reject check below routes it
            // to debounce admission instead.
            let effective_policy = if request.reject_fresh_if_debounced {
                request.reuse_policy
            } else {
                resolve_effective_signal_with_start_policy(
                    conn,
                    request.workflow_name,
                    request.workflow_id,
                    request.reuse_policy,
                )
                .await?
            };

            let build_start_request =
                |exec_id: ExecutionId, policy: WorkflowIdReusePolicy| StartWorkflowParams {
                    workflow_name: request.workflow_name,
                    workflow_id: request.workflow_id,
                    exec_id,
                    input: request.input.clone(),
                    parent_id: request.parent_id,
                    queue_name: request.queue_name,
                    execution_timeout: request.execution_timeout,
                    memo: request.memo.clone(),
                    search_attrs: request.search_attrs.clone(),
                    reuse_policy: policy,
                    conflict_policy: crate::types::WorkflowIdConflictPolicy::Unspecified,
                    trace_context: request.trace_context.clone(),
                    max_execution_timeout_ceiling: request.max_execution_timeout_ceiling,
                    // Chain-scoped lifetime cap (issue #617): forward the
                    // request's fresh-origin chain cap + fleet-wide ceiling.
                    // A signal-/update-with-start never inherits a chain
                    // deadline — it always begins a fresh chain origin.
                    chain_execution_timeout: request.chain_execution_timeout,
                    max_workflow_chain_timeout_ceiling: request.max_workflow_chain_timeout_ceiling,
                    inherited_chain_deadline_at: None,
                    concurrency_key: request.concurrency_key.clone(),
                    concurrency_limit: request.concurrency_limit,
                    priority: Priority::default(),
                    max_workflow_input_bytes: 0,
                    start_at: None,
                    delay: None,
                    max_workflow_start_delay: None,
                    owner: request.owner,
                    runbook_url: request.runbook_url,
                    severity: request.severity,
                    context_headers: request.context_headers.clone(),
                    sla: request.sla,
                    schedule_id: None,
                    scheduled_for: None,
                    workflow_attempt: 1,
                    workflow_retry_policy: request
                        .workflow_retry_policy
                        .clone()
                        .and_then(|v| serde_json::from_value(v).ok()),
                    retry_of_exec_id: None,
                    max_workflow_attempts_ceiling: request.max_workflow_attempts_ceiling,
                    origin: None,
                    completion_callbacks: None,
                    start_source: request
                        .start_source_override
                        .unwrap_or(crate::types::StartSource::SignalWithStart),
                    start_source_ref: request
                        .idempotency_key
                        .as_deref()
                        .or(Some(request.workflow_id)),
                    started_by: None,
                };

            // For a debounced workflow, route the start through the no-spawn collect
            // path with reject_fresh: a fresh start (including a TerminateIfRunning
            // cancel+replace) returns DebounceFreshStart and rolls back WITHOUT
            // cancelling a prior or spawning completion-trigger/parent-close
            // follow-ups (issue #499). An attach returns the existing live run;
            // its deferred list is empty and spawned defensively.
            let started = if request.reject_fresh_if_debounced {
                let (s, mut deferred, mut checks, mut metrics_list) =
                    start_or_load_workflow_execution_collect(
                        conn,
                        build_start_request(request.exec_id, effective_policy),
                        true,
                        true,
                        metrics,
                        None,
                    )
                    .await?;
                deferred_starts.append(&mut deferred);
                deferred_checks.append(&mut checks);
                cancel_metrics.append(&mut metrics_list);
                s
            } else {
                let (s, mut deferred, mut checks, mut metrics_list) =
                    start_or_load_workflow_execution_collect(
                        conn,
                        build_start_request(request.exec_id, effective_policy),
                        true,
                        false,
                        metrics,
                        gate,
                    )
                    .await?;
                deferred_starts.append(&mut deferred);
                deferred_checks.append(&mut checks);
                cancel_metrics.append(&mut metrics_list);
                s
            };

            // On fresh start only: enforce workflow input cap and schema (tx
            // rollback on error). An attach never writes start_input, so neither
            // check runs for it (issue #918 review — schema validation used to
            // run unconditionally, pre-lock, in the HTTP handler, rejecting
            // legitimate signal deliveries to an already-running execution
            // whenever the signal payload didn't match the start-input schema).
            if started.created {
                check_sws_payload_cap(
                    &request.input,
                    crate::error::PayloadKind::WorkflowInput,
                    request.max_workflow_input_bytes,
                    request.workflow_name,
                )?;
                check_sws_input_schema(&request.input, request.workflow_info)?;
            }

            // TOCTOU guard: if a concurrent transaction completed the run between
            // the policy resolver's lock and our start, the start helper returns
            // a terminal row. Escalate to TerminateIfRunning so the signal always
            // lands on a live execution rather than being silently dropped.
            // PAUSED is a non-terminal active state (issue #383): treat it like
            // RUNNING here so a signal-with-start attaches to (and buffers the
            // signal for) the paused run instead of cancelling and replacing it.
            let started = if !matches!(started.state.as_str(), "RUNNING" | "PAUSED")
                // For a debounced workflow, never escalate a terminal prior to a
                // fresh start here — that fresh start must go through debounce
                // admission. The reject check below catches the non-live outcome.
                && !request.reject_fresh_if_debounced
                && matches!(
                    request.reuse_policy,
                    WorkflowIdReusePolicy::AllowDuplicate
                        | WorkflowIdReusePolicy::AllowDuplicateFailedOnly
                ) {
                let fresh_exec_id = ExecutionId::new_for_shard(started.exec_id.shard());
                let (fresh, mut deferred, mut checks, mut metrics_list) =
                    start_or_load_workflow_execution_collect(
                        conn,
                        build_start_request(
                            fresh_exec_id,
                            WorkflowIdReusePolicy::TerminateIfRunning,
                        ),
                        true,
                        false,
                        metrics,
                        gate,
                    )
                    .await?;
                deferred_starts.append(&mut deferred);
                deferred_checks.append(&mut checks);
                cancel_metrics.append(&mut metrics_list);
                if fresh.created {
                    check_sws_payload_cap(
                        &request.input,
                        crate::error::PayloadKind::WorkflowInput,
                        request.max_workflow_input_bytes,
                        request.workflow_name,
                    )?;
                    check_sws_input_schema(&request.input, request.workflow_info)?;
                }
                fresh
            } else {
                started
            };

            // Atomic debounce gate (issue #499): under this transaction's lock, a
            // debounced workflow may only *attach* to a live (RUNNING/PAUSED) prior.
            // Any other outcome — a fresh insert (`created`) or a non-live prior the
            // signal can't land on — would be a fresh start, so reject it and let the
            // caller route to debounce admission. Rolls back any fresh insert above.
            if request.reject_fresh_if_debounced
                && (started.created || !matches!(started.state.as_str(), "RUNNING" | "PAUSED"))
            {
                return Err(HarvestError::DebounceFreshStart {
                    workflow_name: request.workflow_name.to_string(),
                    workflow_id: request.workflow_id.to_string(),
                });
            }

            // Check signal payload cap here — after start/attach/AlreadyExists
            // resolution — so RejectDuplicate conflicts surface as 409 AlreadyExists
            // rather than 413 PayloadTooLarge when the payload happens to be oversized.
            // PAUSED counts as live: the signal will be staged and delivered on resume.
            if matches!(started.state.as_str(), "RUNNING" | "PAUSED") {
                check_sws_payload_cap(
                    &request.signal_payload,
                    crate::error::PayloadKind::SignalPayload,
                    request.max_signal_payload_bytes,
                    request.workflow_name,
                )?;
            }

            let signal_delivered = if matches!(started.state.as_str(), "RUNNING" | "PAUSED") {
                stage_signal_with_idempotency(
                    conn,
                    started.exec_id,
                    request.signal_name,
                    request.signal_payload,
                    request.idempotency_key.as_deref(),
                )
                .await?
            } else {
                false
            };

            Ok((
                SignalWithStartOutcome {
                    exec_id: started.exec_id,
                    workflow_name: started.workflow_name,
                    workflow_id: started.workflow_id,
                    state: started.state,
                    started_fresh: started.created,
                    signal_delivered,
                },
                deferred_starts,
                deferred_checks,
                cancel_metrics,
            ))
        }))
        .await?;

    for start in deferred_starts {
        start.spawn();
    }
    for check in deferred_checks {
        let _ = check_and_report_unfinished_handlers(conn, check.0, &check.1, metrics).await;
    }
    if let Some(m) = metrics {
        for (wf_name, q_name) in cancel_metrics {
            crate::telemetry::emit_workflow_terminal(
                m,
                &wf_name,
                &q_name,
                crate::telemetry::WorkflowStatus::Cancelled,
            );
        }
    }

    Ok(outcome)
}

/// Pick the policy `start_or_load_workflow_execution` is invoked with, given
/// the caller's requested policy and the current prior-run state. For
/// signal-with-start, `AllowDuplicate` and `AllowDuplicateFailedOnly` are
/// upgraded to `TerminateIfRunning` whenever the prior run is non-RUNNING so
/// that the spec's "no signal silently dropped" invariant holds on terminal
/// priors. `RejectDuplicate` and `TerminateIfRunning` are returned unchanged.
async fn resolve_effective_signal_with_start_policy(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    requested: WorkflowIdReusePolicy,
) -> HarvestResult<WorkflowIdReusePolicy> {
    if !matches!(
        requested,
        WorkflowIdReusePolicy::AllowDuplicate | WorkflowIdReusePolicy::AllowDuplicateFailedOnly
    ) {
        return Ok(requested);
    }
    // Take the row lock here so the observed state persists through
    // `start_or_load_workflow_execution`'s own `FOR UPDATE` lookup below.
    // Without this, a workflow that transitions RUNNING -> terminal between
    // the resolver's read and the start path's lock could let the
    // spec-prohibited "attach to terminal, drop signal" outcome re-emerge.
    // Both calls share the same connection / outer transaction, so the lock
    // taken here is held through the start path and released only on outer
    // commit or rollback.
    let Some(existing) =
        try_load_active_execution_for_update(conn, workflow_name, workflow_id).await?
    else {
        return Ok(requested);
    };
    if matches!(existing.state.as_str(), "RUNNING" | "PAUSED") {
        // PAUSED is a non-terminal active state (issue #383): keep the requested
        // policy so the start path attaches to the existing run and the signal is
        // queued (buffered for delivery on resume), matching direct send_signal.
        // Only a truly terminal prior is upgraded below.
        Ok(requested)
    } else {
        // Non-RUNNING prior under a non-rejecting policy: upgrade so the
        // start transaction takes the `replace_execution` path (seal prior,
        // insert fresh, append WorkflowStarted) and the signal can land.
        Ok(WorkflowIdReusePolicy::TerminateIfRunning)
    }
}

/// Locking variant of [`try_load_by_key`] used by
/// [`signal_with_start_workflow_execution`]'s resolver. Returns `None` when
/// no active execution exists. Acquires `FOR UPDATE` so the caller's outer
/// transaction holds the row lock until commit, preventing a RUNNING ->
/// terminal race between the resolver decision and the start path's own
/// `FOR UPDATE` lookup.
async fn try_load_active_execution_for_update(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
) -> HarvestResult<Option<WorkflowExecution>> {
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(workflow_name))
        .filter(harvest_workflow_executions::workflow_id.eq(workflow_id))
        .filter(harvest_workflow_executions::state.ne_all(["CONTINUED_AS_NEW", "TERMINATED"]))
        .select(WorkflowExecution::as_select())
        .for_update()
        .first(conn)
        .await
        .optional()
        .map_err(database_error)
}

/// Cross-execution idempotency dedupe for `signal_with_start`.
///
/// Returns the most recent workflow execution of `(workflow_name, workflow_id)`
/// that has a `harvest_signals` row with this `idempotency_key`. The per-shard
/// partial unique index on `(workflow_exec_id, idempotency_key)` only enforces
/// uniqueness within one execution; this query scopes the dedupe to the
/// logical workflow so a webhook retry that arrives after the prior signal
/// drove its execution to a terminal state is recognised as a duplicate and
/// short-circuited before any fresh start / replacement happens.
/// Read-only shared committed-replay lookup for `signal_with_start`
/// idempotency, scoped `(workflow_name, workflow_id, idempotency_key)`, joining
/// `harvest_signals` → `harvest_workflow_executions` (no state filter, newest
/// signal first). Used by BOTH the in-lock authoritative dedup and the
/// read-only handler-edge fast-path probe — single source of truth so the two
/// can never drift.
pub async fn lookup_idempotent_signal_dedupe(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    idempotency_key: &str,
) -> HarvestResult<Option<WorkflowExecution>> {
    use diesel::JoinOnDsl;

    harvest_signals::table
        .inner_join(
            harvest_workflow_executions::table
                .on(harvest_signals::workflow_exec_id.eq(harvest_workflow_executions::id)),
        )
        .filter(harvest_workflow_executions::workflow_name.eq(workflow_name))
        .filter(harvest_workflow_executions::workflow_id.eq(workflow_id))
        .filter(harvest_signals::idempotency_key.eq(idempotency_key))
        .order_by(harvest_signals::received_at.desc())
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(database_error)
}

/// Insert a signal row, returning `false` when the idempotency key collides
/// with an already-staged signal for the same execution.
async fn stage_signal_with_idempotency(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    signal_name: &str,
    payload: serde_json::Value,
    idempotency_key: Option<&str>,
) -> HarvestResult<bool> {
    let row = NewHarvestSignal {
        workflow_exec_id: exec_id.as_uuid(),
        signal_name,
        payload,
        idempotency_key,
    };

    let inserted = diesel::insert_into(harvest_signals::table)
        .values(&row)
        .on_conflict_do_nothing()
        .execute(conn)
        .await
        .map_err(database_error)?;

    if inserted == 0 {
        // Idempotency-key collision — the prior insert already queued an
        // equivalent signal. This is the dedup happy path.
        return Ok(false);
    }

    queue::wake_workflow_task(conn, exec_id).await?;
    Ok(true)
}

/// Locking lookup used inside the start transaction when a policy decision may
/// modify or replace the existing row.
async fn load_workflow_execution_by_key_for_update(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
) -> HarvestResult<WorkflowExecution> {
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(workflow_name))
        .filter(harvest_workflow_executions::workflow_id.eq(workflow_id))
        .filter(harvest_workflow_executions::state.ne_all(["CONTINUED_AS_NEW", "TERMINATED"]))
        .select(WorkflowExecution::as_select())
        .for_update()
        .first(conn)
        .await
        .optional()
        .map_err(database_error)?
        .ok_or_else(|| {
            HarvestError::NotFound(format!("workflow execution {workflow_name}/{workflow_id}"))
        })
}

// ─────────────────────────────────────────────────────────────────────────────
// UpdateWithStart (issue #479)
// ─────────────────────────────────────────────────────────────────────────────

/// Parameters for the atomic `update_with_start` primitive.
///
/// Combines the inputs of [`StartWorkflowParams`] with the update name,
/// arguments, and optional idempotency key so a single shard-local transaction
/// can either start a fresh execution and admit the update for its first
/// dispatch, or attach the update to an existing live execution.
#[derive(Debug, Clone)]
pub struct UpdateWithStartParams<'a> {
    pub workflow_name: &'a str,
    pub workflow_id: &'a str,
    pub exec_id: ExecutionId,
    pub input: serde_json::Value,
    pub parent_id: Option<Uuid>,
    pub queue_name: &'a str,
    pub execution_timeout: Option<chrono::Duration>,
    pub memo: Option<serde_json::Value>,
    pub search_attrs: Option<serde_json::Value>,
    pub reuse_policy: WorkflowIdReusePolicy,
    pub trace_context: Option<TraceContextCarrier>,
    /// Server-side ceiling applied to `execution_timeout`.
    pub max_execution_timeout_ceiling: Option<chrono::Duration>,
    /// Chain-scoped lifetime cap DURATION for a fresh chain-origin start (issue
    /// #617). Forwarded to [`StartWorkflowParams::chain_execution_timeout`]. An
    /// update-with-start always begins a fresh chain origin, so there is no
    /// `inherited_chain_deadline_at`. `None` = caller specified no chain cap.
    pub chain_execution_timeout: Option<chrono::Duration>,
    /// Server-side ceiling on the chain cap, doubling as a fleet-wide default
    /// (issue #617). Forwarded to
    /// [`StartWorkflowParams::max_workflow_chain_timeout_ceiling`].
    pub max_workflow_chain_timeout_ceiling: Option<chrono::Duration>,
    /// Pre-resolved concurrency group key.
    pub concurrency_key: Option<String>,
    /// Per-key concurrency cap.
    pub concurrency_limit: Option<u32>,
    /// Pre-generated update ID. When `idempotency_key` is `Some`, callers
    /// should derive this deterministically (e.g. `UUIDv5`) so the dedup lookup
    /// matches prior admitted updates.
    pub update_id: crate::types::UpdateId,
    /// The name of the update handler to invoke.
    pub update_name: String,
    /// JSON-serialised update arguments.
    pub update_args: serde_json::Value,
    /// Optional dedup key, scoped to `(workflow_name, workflow_id)`. A retry
    /// with the same key returns the previous outcome without re-admitting.
    pub idempotency_key: Option<String>,
    /// Payload cap for `input` (bytes). Enforced only on the fresh-start path.
    pub max_workflow_input_bytes: u64,
    pub owner: Option<&'a str>,
    pub runbook_url: Option<&'a str>,
    pub severity: Option<&'a str>,
    pub context_headers: Option<std::collections::HashMap<String, String>>,
    /// Soft SLA budget forwarded to [`StartWorkflowParams::sla`] (issue #487).
    pub sla: Option<chrono::Duration>,
    /// Effective workflow-level retry policy (issue #523). Forwarded to
    /// [`StartWorkflowParams::workflow_retry_policy`] on fresh starts.
    pub workflow_retry_policy: Option<serde_json::Value>,
    /// Server-side ceiling on retry attempts. Forwarded to
    /// [`StartWorkflowParams::max_workflow_attempts_ceiling`].
    pub max_workflow_attempts_ceiling: Option<u32>,
    /// When `true`, reject (with [`HarvestError::DebounceFreshStart`]) any call
    /// that would create a **fresh** execution rather than attach to a live
    /// (RUNNING/PAUSED) prior. Set by the HTTP handler for a debounced workflow
    /// so an attach/idempotent call is preserved while a fresh start is rejected
    /// — decided atomically under this call's lock (issue #499).
    pub reject_fresh_if_debounced: bool,
}

/// Result of an [`update_with_start_workflow_execution`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateWithStartOutcome {
    pub exec_id: ExecutionId,
    pub workflow_name: String,
    pub workflow_id: String,
    pub state: String,
    pub started_fresh: bool,
    /// The update ID the caller can poll to retrieve the typed result.
    pub update_id: crate::types::UpdateId,
    /// `false` on an idempotency-key cache hit (update was already admitted).
    pub update_admitted: bool,
}

/// Atomically start or attach to a workflow and admit one update.
///
/// Applies the same reuse-policy matrix as `signal_with_start_workflow_execution`
/// but admits exactly one update instead of a signal.
///
/// ## Outcome matrix (mirrors signal-with-start except PAUSED rejects updates)
///
/// | Prior state         | `AllowDuplicate`       | `RejectDuplicate`   | `AllowDupFailedOnly`   | `TerminateIfRunning`      |
/// |---------------------|------------------------|---------------------|------------------------|---------------------------|
/// | none                | start + admit          | start + admit       | start + admit          | start + admit             |
/// | RUNNING             | admit to existing      | `Err(AlreadyExists)`| admit to existing      | cancel + start + admit    |
/// | PAUSED              | `Err(WorkflowPaused)`  | `Err(AlreadyExists)`| `Err(WorkflowPaused)`  | cancel + start + admit    |
/// | COMPLETED/FAILED    | start fresh + admit    | `Err(AlreadyExists)`| start fresh + admit    | start fresh + admit       |
/// | CANCELLED           | start fresh + admit    | `Err(AlreadyExists)`| start fresh + admit    | start fresh + admit       |
/// | TERMINATED          | start fresh + admit    | start fresh + admit | start fresh + admit    | start fresh + admit       |
///
/// ## Event ordering
///
/// On a **fresh start** `WorkflowStarted` is appended and then
/// `UpdateAdmitted` is appended in the same outer transaction. The worker
/// picks up the already-admitted update before first dispatch.
///
/// On an **attach**, `UpdateAdmitted` is appended and the workflow task is
/// woken — both inside the outer transaction.
///
/// ## Idempotency
///
/// When `idempotency_key` is `Some`, the call checks `harvest_events` for an
/// existing `UpdateAdmitted` event with the same `update_id` scoped to
/// `(workflow_name, workflow_id)`. A match returns the prior outcome without
/// re-starting or re-admitting.
///
/// # Errors
///
/// - [`HarvestError::AlreadyExists`] when `RejectDuplicate` rejects.
/// - [`HarvestError::WorkflowPaused`] when attaching to a PAUSED execution.
/// - Propagates queue/event-store failures from the start/admit transactions.
#[allow(clippy::too_many_lines)]
pub async fn update_with_start_workflow_execution(
    conn: &mut AsyncPgConnection,
    request: UpdateWithStartParams<'_>,
) -> HarvestResult<UpdateWithStartOutcome> {
    update_with_start_workflow_execution_with_metrics(conn, request, None, None).await
}

#[allow(clippy::too_many_lines)]
pub async fn update_with_start_workflow_execution_with_metrics(
    conn: &mut AsyncPgConnection,
    request: UpdateWithStartParams<'_>,
    metrics: Option<&(dyn crate::telemetry::MetricsRecorder + Send + Sync)>,
    // Admission gate (issue #618, PR #1014) — see the sibling doc on
    // `signal_with_start_workflow_execution_with_metrics`. Threaded into the
    // fresh-create start calls so an update-with-start that CREATES is gated
    // authoritatively under the primitive's lock; the HTTP route passes
    // `Some(GateMode::Check)`, continuation/example callers pass `None`, and the
    // `reject_fresh_if_debounced` branch stays `None`.
    gate: Option<crate::admission_gate::GateMode>,
) -> HarvestResult<UpdateWithStartOutcome> {
    // Capture the queue for the post-commit update.admitted metric (issue #684)
    // before `request` is moved into the transaction closure. The update name is
    // deliberately NOT a label — the admission site cannot bound an unregistered
    // name (Codex P2; see `admit_update_event`), so the counter is labeled by
    // `workflow` + `queue` only.
    let queue_for_metric = request.queue_name.to_owned();
    let (outcome, deferred_starts, deferred_checks, cancel_metrics) =
        Box::pin(conn.transaction::<(
            UpdateWithStartOutcome,
            Vec<DeferredTriggerStart>,
            Vec<(ExecutionId, String)>,
            Vec<(String, String)>,
        ), HarvestError, _>(async |conn| {
            let request = request;
            let mut deferred_starts = Vec::new();
            let mut deferred_checks = Vec::new();
            let mut cancel_metrics = Vec::new();

            // Cross-execution idempotency dedupe scoped to (workflow_name, workflow_id).
            // When an idempotency key is provided we look up by the supplied update_id
            // (callers should derive it deterministically from the key, e.g. UUIDv5).
            if request.idempotency_key.is_some()
                && let Some(prior) = lookup_idempotent_update_dedupe(
                    conn,
                    request.workflow_name,
                    request.workflow_id,
                    &request.update_id,
                )
                .await?
            {
                return Ok((
                    UpdateWithStartOutcome {
                        exec_id: prior.exec_id,
                        workflow_name: prior.workflow_name,
                        workflow_id: prior.workflow_id,
                        state: prior.state,
                        started_fresh: false,
                        update_id: request.update_id,
                        update_admitted: false,
                    },
                    deferred_starts,
                    deferred_checks,
                    cancel_metrics,
                ));
            }

            // Upgrade AllowDuplicate / AllowDuplicateFailedOnly to TerminateIfRunning
            // when the prior run is terminal so the update always lands on a live
            // execution (mirrors the signal-with-start "no signal dropped" invariant).
            // For a debounced workflow, skip the upgrade: we must not escalate a
            // terminal prior to a fresh start here — the reject check below routes it
            // to debounce admission instead.
            let effective_policy = if request.reject_fresh_if_debounced {
                request.reuse_policy
            } else {
                resolve_effective_signal_with_start_policy(
                    conn,
                    request.workflow_name,
                    request.workflow_id,
                    request.reuse_policy,
                )
                .await?
            };

            let build_start_request =
                |exec_id: ExecutionId, policy: WorkflowIdReusePolicy| StartWorkflowParams {
                    workflow_name: request.workflow_name,
                    workflow_id: request.workflow_id,
                    exec_id,
                    input: request.input.clone(),
                    parent_id: request.parent_id,
                    queue_name: request.queue_name,
                    execution_timeout: request.execution_timeout,
                    memo: request.memo.clone(),
                    search_attrs: request.search_attrs.clone(),
                    reuse_policy: policy,
                    conflict_policy: crate::types::WorkflowIdConflictPolicy::Unspecified,
                    trace_context: request.trace_context.clone(),
                    max_execution_timeout_ceiling: request.max_execution_timeout_ceiling,
                    // Chain-scoped lifetime cap (issue #617): forward the
                    // request's fresh-origin chain cap + fleet-wide ceiling.
                    // A signal-/update-with-start never inherits a chain
                    // deadline — it always begins a fresh chain origin.
                    chain_execution_timeout: request.chain_execution_timeout,
                    max_workflow_chain_timeout_ceiling: request.max_workflow_chain_timeout_ceiling,
                    inherited_chain_deadline_at: None,
                    concurrency_key: request.concurrency_key.clone(),
                    concurrency_limit: request.concurrency_limit,
                    priority: Priority::default(),
                    max_workflow_input_bytes: 0,
                    start_at: None,
                    delay: None,
                    max_workflow_start_delay: None,
                    owner: request.owner,
                    runbook_url: request.runbook_url,
                    severity: request.severity,
                    context_headers: request.context_headers.clone(),
                    sla: request.sla,
                    schedule_id: None,
                    scheduled_for: None,
                    workflow_attempt: 1,
                    workflow_retry_policy: request
                        .workflow_retry_policy
                        .clone()
                        .and_then(|v| serde_json::from_value(v).ok()),
                    retry_of_exec_id: None,
                    max_workflow_attempts_ceiling: request.max_workflow_attempts_ceiling,
                    origin: None,
                    completion_callbacks: None,
                    start_source: crate::types::StartSource::UpdateWithStart,
                    start_source_ref: request
                        .idempotency_key
                        .as_deref()
                        .or(Some(request.workflow_id)),
                    started_by: None,
                };

            // Debounced workflow: route through the no-spawn collect path with
            // reject_fresh so a fresh start (incl. TerminateIfRunning cancel+replace)
            // rolls back via DebounceFreshStart without cancelling/spawning before
            // the rejection (issue #499). Attach returns the existing live run.
            let started = if request.reject_fresh_if_debounced {
                let (s, mut deferred, mut checks, mut metrics_list) =
                    start_or_load_workflow_execution_collect(
                        conn,
                        build_start_request(request.exec_id, effective_policy),
                        true,
                        true,
                        metrics,
                        None,
                    )
                    .await?;
                deferred_starts.append(&mut deferred);
                deferred_checks.append(&mut checks);
                cancel_metrics.append(&mut metrics_list);
                s
            } else {
                let (s, mut deferred, mut checks, mut metrics_list) =
                    start_or_load_workflow_execution_collect(
                        conn,
                        build_start_request(request.exec_id, effective_policy),
                        true,
                        false,
                        metrics,
                        gate,
                    )
                    .await?;
                deferred_starts.append(&mut deferred);
                deferred_checks.append(&mut checks);
                cancel_metrics.append(&mut metrics_list);
                s
            };

            // Enforce workflow input cap on fresh start.
            if started.created {
                check_sws_payload_cap(
                    &request.input,
                    crate::error::PayloadKind::WorkflowInput,
                    request.max_workflow_input_bytes,
                    request.workflow_name,
                )?;
            }

            // TOCTOU guard: if a concurrent transaction completed the run between
            // the policy resolver's lock and our start, escalate so the update lands.
            // SUSPENDED is treated as RUNNING here (not a real DB state today, but
            // defensive). PAUSED is a non-terminal active state; the update will be
            // rejected by admit_update_event below (WorkflowPaused), rolling back.
            let started = if !matches!(started.state.as_str(), "RUNNING" | "SUSPENDED" | "PAUSED")
                // Debounced workflow: never escalate a terminal prior to a fresh
                // start here — route it to debounce admission via the check below.
                && !request.reject_fresh_if_debounced
                && matches!(
                    request.reuse_policy,
                    WorkflowIdReusePolicy::AllowDuplicate
                        | WorkflowIdReusePolicy::AllowDuplicateFailedOnly
                ) {
                let fresh_exec_id = ExecutionId::new_for_shard(started.exec_id.shard());
                let (fresh, mut deferred, mut checks, mut metrics_list) =
                    start_or_load_workflow_execution_collect(
                        conn,
                        build_start_request(
                            fresh_exec_id,
                            WorkflowIdReusePolicy::TerminateIfRunning,
                        ),
                        true,
                        false,
                        metrics,
                        gate,
                    )
                    .await?;
                deferred_starts.append(&mut deferred);
                deferred_checks.append(&mut checks);
                cancel_metrics.append(&mut metrics_list);
                if fresh.created {
                    check_sws_payload_cap(
                        &request.input,
                        crate::error::PayloadKind::WorkflowInput,
                        request.max_workflow_input_bytes,
                        request.workflow_name,
                    )?;
                }
                fresh
            } else {
                started
            };

            // Atomic debounce gate (issue #499): a debounced workflow may only
            // *attach* to a live (RUNNING/SUSPENDED/PAUSED) prior. A fresh insert
            // (`created`) or a non-live prior would be a fresh start — reject and let
            // the caller route to debounce admission. Rolls back any fresh insert.
            if request.reject_fresh_if_debounced
                && (started.created
                    || !matches!(started.state.as_str(), "RUNNING" | "SUSPENDED" | "PAUSED"))
            {
                return Err(HarvestError::DebounceFreshStart {
                    workflow_name: request.workflow_name.to_string(),
                    workflow_id: request.workflow_id.to_string(),
                });
            }

            // Post-lock idempotency re-check: two concurrent calls with the same
            // idempotency_key may both pass the early dedupe query (which runs before
            // the execution row lock is acquired). After the lock is held, any prior
            // admission committed by a racing transaction is now visible — re-check so
            // the loser returns the cached outcome rather than admitting a second time.
            if request.idempotency_key.is_some()
                && let Some(prior) = lookup_idempotent_update_dedupe(
                    conn,
                    request.workflow_name,
                    request.workflow_id,
                    &request.update_id,
                )
                .await?
            {
                return Ok((
                    UpdateWithStartOutcome {
                        exec_id: prior.exec_id,
                        workflow_name: prior.workflow_name,
                        workflow_id: prior.workflow_id,
                        state: prior.state,
                        started_fresh: false,
                        update_id: request.update_id,
                        update_admitted: false,
                    },
                    deferred_starts,
                    deferred_checks,
                    cancel_metrics,
                ));
            }

            // Admit the update against the resolved execution.
            //
            // `admit_update_event` acquires a FOR UPDATE row lock and rejects:
            //   - PAUSED   → HarvestError::WorkflowPaused (rolls back entire tx)
            //   - non-RUNNING → HarvestError::UpdateRejected
            //
            // On fresh start the execution is RUNNING so admission succeeds.
            // The admitted update is part of the same outer transaction as the
            // WorkflowStarted event, so a crash never leaves a half-started
            // execution with no admitted update.
            // Pass `None`: this admission is part of the outer transaction,
            // so update.admitted (issue #684) is emitted post-outer-commit
            // below (gated on `outcome.update_admitted`) rather than at the
            // inner savepoint, so a later outer rollback never over-counts.
            store::admit_update_event(
                conn,
                started.exec_id,
                request.update_id,
                request.update_name.clone(),
                request.update_args.clone(),
                None,
            )
            .await?;

            // Wake the workflow task. For fresh starts, `start_or_load_workflow_execution`
            // already inserted a task queue row; wake_workflow_task is idempotent
            // (it updates the wakeup timestamp) and harmless to call again.
            queue::wake_workflow_task(conn, started.exec_id).await?;

            Ok((
                UpdateWithStartOutcome {
                    exec_id: started.exec_id,
                    workflow_name: started.workflow_name,
                    workflow_id: started.workflow_id,
                    state: started.state,
                    started_fresh: started.created,
                    update_id: request.update_id,
                    update_admitted: true,
                },
                deferred_starts,
                deferred_checks,
                cancel_metrics,
            ))
        }))
        .await?;

    for start in deferred_starts {
        start.spawn();
    }
    for check in deferred_checks {
        let _ = check_and_report_unfinished_handlers(conn, check.0, &check.1, metrics).await;
    }
    if let Some(m) = metrics {
        for (wf_name, q_name) in cancel_metrics {
            crate::telemetry::emit_workflow_terminal(
                m,
                &wf_name,
                &q_name,
                crate::telemetry::WorkflowStatus::Cancelled,
            );
        }
        // Post-outer-commit: emit update.admitted (issue #684) only when an
        // update was actually admitted (an idempotency dedup short-circuit
        // reports update_admitted == false and admits nothing).
        if outcome.update_admitted {
            m.record_update_admitted(&outcome.workflow_name, &queue_for_metric);
        }
    }

    Ok(outcome)
}

/// Minimal row returned by the `update_with_start` idempotency dedupe query.
///
/// Public so the plugin's read-only handler-edge committed-replay probe can
/// build an [`UpdateWithStartOutcome`] from a dedupe hit, sharing the exact
/// lookup ([`lookup_idempotent_update_dedupe`]) the in-lock authoritative path
/// uses — single source of truth so the two can never drift.
#[derive(Debug, Clone)]
pub struct UpdateDedupeRow {
    /// The execution that already admitted the update for this key.
    pub exec_id: ExecutionId,
    /// The owning workflow type.
    pub workflow_name: String,
    /// The owning `workflow_id`.
    pub workflow_id: String,
    /// The execution's current state string.
    pub state: String,
}

/// Cross-execution idempotency dedupe for `update_with_start`.
///
/// Searches `harvest_events` for an `UpdateAdmitted` event with the given
/// `update_id` across all executions of `(workflow_name, workflow_id)`.
/// Returns the owning execution if found, so a retried call can short-circuit
/// without re-starting or re-admitting.
///
/// The lookup uses JSON operators on `event_data` (Postgres JSONB). This is a
/// cold-path read (retries only) so index coverage is not critical.
///
/// Public so the plugin's read-only handler-edge committed-replay probe shares
/// the exact lookup the in-lock authoritative dedup uses — single source of
/// truth so the two can never drift.
pub async fn lookup_idempotent_update_dedupe(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    update_id: &crate::types::UpdateId,
) -> HarvestResult<Option<UpdateDedupeRow>> {
    use diesel::sql_query;
    use diesel::sql_types::Text;

    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: Uuid,
        #[diesel(sql_type = diesel::sql_types::Text)]
        workflow_name: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        workflow_id: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
    }

    let row: Option<Row> = sql_query(
        "SELECT wf.id, wf.workflow_name, wf.workflow_id, wf.state \
         FROM harvest_events e \
         JOIN harvest_workflow_executions wf ON e.workflow_exec_id = wf.id \
         WHERE wf.workflow_name = $1 \
           AND wf.workflow_id = $2 \
           AND e.event_data->>'type' = 'UpdateAdmitted' \
           AND e.event_data->'data'->>'update_id' = $3 \
         ORDER BY e.event_id DESC \
         LIMIT 1",
    )
    .bind::<Text, _>(workflow_name)
    .bind::<Text, _>(workflow_id)
    .bind::<Text, _>(update_id.to_string())
    .get_result(conn)
    .await
    .optional()
    .map_err(database_error)?;

    Ok(row.map(|r| UpdateDedupeRow {
        exec_id: ExecutionId::from_uuid(r.id),
        workflow_name: r.workflow_name,
        workflow_id: r.workflow_id,
        state: r.state,
    }))
}

/// Resolve the last-completion-result and last-error carryover for a schedule (issue #488).
///
/// Queries are shard-local, run on the same `conn` inside the start transaction,
/// and exclude the just-inserted execution by `current_exec_id`.
///
/// Selection is by the **scheduled slot** (`scheduled_for`), not completion time:
/// Selection is by the **scheduled slot** (`scheduled_for`), not completion time:
/// the carryover source is the COMPLETED fire with the **greatest `scheduled_for`
/// strictly before this run's own slot** (`current_scheduled_for`) — i.e. the
/// previous logical fire. Bounding the lookup to earlier slots means a backfill or
/// trigger-now run that starts an *older* logical slot after a newer slot already
/// completed sees the cursor as of its own slot, never a future fire's output; and
/// an older slot finishing late can never roll a newer run's cursor backward (the bug
/// a `completed_at` ordering would have). Rows without a slot (`scheduled_for IS NULL`)
/// are excluded; post-migration every scheduled run carries a slot. When the current
/// run itself has no slot (`current_scheduled_for == None`, defensive — scheduled
/// starts always set it) no carryover is resolved.
///
/// Within a slot, ties are broken by `completed_at DESC, id DESC` so that when the
/// same slot was run more than once (a `TERMINATED` row is released from the active
/// uniqueness index and the slot can be re-run), the **latest** attempt of that slot
/// wins rather than an arbitrary older terminated row.
///
/// Returns `(last_completion_result, last_error)` where:
/// - `last_completion_result` = `output` of the highest earlier-slot COMPLETED fire.
/// - `last_error` = `error` of the highest earlier-slot terminal fire if it was
///   `FAILED`/`TIMED_OUT`; `None` if that fire `COMPLETED`/`CANCELLED`/`TERMINATED`.
async fn resolve_carryover(
    conn: &mut AsyncPgConnection,
    schedule_id: uuid::Uuid,
    current_exec_id: uuid::Uuid,
    current_scheduled_for: Option<chrono::DateTime<chrono::Utc>>,
) -> HarvestResult<(Option<serde_json::Value>, Option<String>)> {
    use crate::schema::harvest_workflow_executions::dsl;
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    // No slot on the current run → can't bound to earlier slots; resolve nothing.
    let Some(current_slot) = current_scheduled_for else {
        return Ok((None, None));
    };

    // Highest earlier-slot COMPLETED fire for this schedule.
    let last_completion_result: Option<serde_json::Value> = harvest_workflow_executions::table
        .filter(dsl::schedule_id.eq(schedule_id))
        .filter(dsl::state.eq("COMPLETED"))
        .filter(dsl::completed_at.is_not_null())
        .filter(dsl::scheduled_for.lt(current_slot))
        .filter(dsl::id.ne(current_exec_id))
        .order((
            dsl::scheduled_for.desc(),
            dsl::completed_at.desc(),
            dsl::id.desc(),
        ))
        .limit(1)
        .select(dsl::output)
        .get_result::<Option<serde_json::Value>>(conn)
        .await
        .optional()
        .map_err(database_error)?
        .flatten();

    // Highest earlier-slot terminal fire for this schedule, across *all* terminal states
    // (COMPLETED, FAILED, TIMED_OUT, CANCELLED, TERMINATED). Surfacing an error only
    // when it is FAILED/TIMED_OUT means a more recent CANCELLED/TERMINATED fire (e.g.
    // via OverlapPolicy::CancelOther / TerminateOther) masks an older failure.
    let last_terminal: Option<(String, Option<String>)> = harvest_workflow_executions::table
        .filter(dsl::schedule_id.eq(schedule_id))
        .filter(dsl::state.eq_any([
            "COMPLETED",
            "FAILED",
            "TIMED_OUT",
            "CANCELLED",
            "TERMINATED",
        ]))
        .filter(dsl::completed_at.is_not_null())
        .filter(dsl::scheduled_for.lt(current_slot))
        .filter(dsl::id.ne(current_exec_id))
        .order((
            dsl::scheduled_for.desc(),
            dsl::completed_at.desc(),
            dsl::id.desc(),
        ))
        .limit(1)
        .select((dsl::state, dsl::error))
        .get_result::<(String, Option<String>)>(conn)
        .await
        .optional()
        .map_err(database_error)?;

    let last_error = last_terminal.and_then(|(state, error)| {
        if state == "FAILED" || state == "TIMED_OUT" {
            error
        } else {
            None
        }
    });

    Ok((last_completion_result, last_error))
}

/// One row of the per-schedule run-history listing (issue #534).
///
/// Each row is a single workflow execution that a schedule launched (attributed
/// via [`StartWorkflowParams::schedule_id`]), with the columns an operator needs
/// to triage a flaky cron: the originating slot, its timing, its terminal `state`,
/// and the dispatch [`origin`](StartWorkflowParams::origin) marker that keeps a
/// backfill storm or manual fire from masquerading as normal cadence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleRunRow {
    /// Execution id of the run.
    pub execution_id: Uuid,
    /// Logical schedule slot this run fired for (`scheduled_for`). `None` for a
    /// `manual_trigger` fire, which is attributed to the schedule but carries no slot.
    pub nominal_fire_time: Option<chrono::DateTime<Utc>>,
    /// When the run started.
    pub started_at: chrono::DateTime<Utc>,
    /// When the run reached a terminal state, or `None` while still active.
    pub completed_at: Option<chrono::DateTime<Utc>>,
    /// Current execution state (`RUNNING`/`COMPLETED`/`FAILED`/`TIMED_OUT`/…).
    pub state: String,
    /// Dispatch origin: `scheduled` / `backfill` / `manual_trigger`. `None` only for
    /// pre-migration rows whose origin could not be reconstructed.
    pub origin: Option<String>,
    /// Terminal failure cause, as stored on the execution row. Carries the raw
    /// `error` column verbatim (the plugin layer gates it to terminally-failed runs
    /// and truncates to the first line for display); `None` when the run has no
    /// recorded error.
    pub error: Option<String>,
}

impl ScheduleRunRow {
    /// The logical-slot sort key: the run's `scheduled_for` slot, falling back to
    /// `started_at` for a slot-less (`manual_trigger`) fire.
    ///
    /// This is the newest-slot-first ordering key (issue #762): runs are ordered by
    /// `COALESCE(scheduled_for, started_at) DESC` so a scheduled run sorts by the slot
    /// it fired for (matching #488's carryover ordering) rather than by completion or
    /// start time, while a slot-less manual fire keeps a deterministic position via
    /// its `started_at`. The plugin cross-shard merge and keyset cursor use the same
    /// key so pagination stays consistent.
    #[must_use]
    pub fn sort_key(&self) -> chrono::DateTime<Utc> {
        self.nominal_fire_time.unwrap_or(self.started_at)
    }
}

/// Filters + keyset cursor for [`list_schedule_runs`] (issue #534).
///
/// Mirrors the #514 execution-list conventions: a `state` filter, optional time
/// bounds on `started_at`, and a `(started_at, id)` keyset cursor for stable
/// newest-first pagination that merges cleanly across shards.
#[derive(Debug, Clone, Default)]
pub struct ScheduleRunQuery {
    /// Restrict to these terminal/active states. Empty = all states.
    pub states: Vec<String>,
    /// Restrict to these origins (`scheduled`/`backfill`/`manual_trigger`). Empty = all.
    pub origins: Vec<String>,
    /// Lower bound (inclusive) on `started_at`.
    pub since: Option<chrono::DateTime<Utc>>,
    /// Upper bound (exclusive) on `started_at`.
    pub until: Option<chrono::DateTime<Utc>>,
    /// Keyset cursor: return only rows strictly before `(sort_key, id)` in the
    /// `COALESCE(scheduled_for, started_at) DESC, id DESC` ordering, where `sort_key`
    /// is the logical-slot key ([`ScheduleRunRow::sort_key`]).
    pub cursor: Option<(chrono::DateTime<Utc>, Uuid)>,
    /// Maximum rows to return. The caller typically passes `limit + 1` to detect
    /// whether a further page exists.
    pub limit: i64,
}

/// SQL for [`list_schedule_runs`] (issue #762).
///
/// Ordered by the logical-slot key `COALESCE(scheduled_for, started_at) DESC, id
/// DESC` (newest-slot-first, matching #488's carryover ordering) so a slot-less
/// `manual_trigger` fire keeps a deterministic position via its `started_at`. The
/// same coalesced key drives the keyset cursor so pagination is stable and the
/// cross-shard merge is well-defined. Params: `$1` = `schedule_id`, `$2` = `shard_id`
/// (prevents double-counting when two logical shards share one physical database),
/// `$3` = state filter array (empty = all), `$4` = origin filter array (empty = all),
/// `$5`/`$6` = optional `since`/`until` bounds on `started_at`, `$7`/`$8` = optional
/// keyset cursor `(sort_key, id)`, `$9` = row limit.
const LIST_SCHEDULE_RUNS_SQL: &str = "
SELECT
    id,
    scheduled_for,
    started_at,
    completed_at,
    state::TEXT AS state,
    origin,
    error
FROM harvest_workflow_executions
WHERE schedule_id = $1::UUID
  AND shard_id    = $2::INT4
  AND (cardinality($3::TEXT[]) = 0 OR state = ANY($3::TEXT[]))
  AND (cardinality($4::TEXT[]) = 0 OR origin = ANY($4::TEXT[]))
  AND ($5::TIMESTAMPTZ IS NULL OR started_at >= $5::TIMESTAMPTZ)
  AND ($6::TIMESTAMPTZ IS NULL OR started_at <  $6::TIMESTAMPTZ)
  AND ($7::TIMESTAMPTZ IS NULL
       OR COALESCE(scheduled_for, started_at) < $7::TIMESTAMPTZ
       OR (COALESCE(scheduled_for, started_at) = $7::TIMESTAMPTZ AND id < $8::UUID))
ORDER BY COALESCE(scheduled_for, started_at) DESC, id DESC
LIMIT $9::BIGINT
";

#[derive(Debug, diesel::QueryableByName)]
struct ScheduleRunSqlRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: Uuid,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
    scheduled_for: Option<chrono::DateTime<Utc>>,
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    started_at: chrono::DateTime<Utc>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
    completed_at: Option<chrono::DateTime<Utc>>,
    #[diesel(sql_type = diesel::sql_types::Text)]
    state: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    origin: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    error: Option<String>,
}

/// List the executions a schedule launched, newest-slot-first (issues #534, #762).
///
/// Shard-local: a schedule's runs may be spread across shards, so the plugin layer
/// fans this out across `iter_shards()` and merges the per-shard results with a
/// keyset merge. Ordered `COALESCE(scheduled_for, started_at) DESC, id DESC` (the
/// logical-slot key) so the cursor is stable, slot-less manual fires keep a
/// deterministic position, and the cross-shard merge is well-defined.
pub async fn list_schedule_runs(
    conn: &mut AsyncPgConnection,
    schedule_id: uuid::Uuid,
    shard_id: i32,
    query: &ScheduleRunQuery,
) -> HarvestResult<Vec<ScheduleRunRow>> {
    use diesel::sql_types::Uuid as SqlUuid;
    use diesel::sql_types::{Array, BigInt, Integer, Nullable, Text, Timestamptz};
    use diesel_async::RunQueryDsl;

    let (cursor_ts, cursor_id) = match query.cursor {
        Some((ts, id)) => (Some(ts), Some(id)),
        None => (None, None),
    };

    let rows = diesel::sql_query(LIST_SCHEDULE_RUNS_SQL)
        .bind::<SqlUuid, _>(schedule_id)
        .bind::<Integer, _>(shard_id)
        .bind::<Array<Text>, _>(query.states.clone())
        .bind::<Array<Text>, _>(query.origins.clone())
        .bind::<Nullable<Timestamptz>, _>(query.since)
        .bind::<Nullable<Timestamptz>, _>(query.until)
        .bind::<Nullable<Timestamptz>, _>(cursor_ts)
        .bind::<Nullable<SqlUuid>, _>(cursor_id)
        .bind::<BigInt, _>(query.limit.max(0).saturating_add(1))
        .load::<ScheduleRunSqlRow>(conn)
        .await
        .map_err(database_error)?;

    Ok(rows
        .into_iter()
        .map(|r| ScheduleRunRow {
            execution_id: r.id,
            nominal_fire_time: r.scheduled_for,
            started_at: r.started_at,
            completed_at: r.completed_at,
            state: r.state,
            origin: r.origin,
            error: r.error,
        })
        .collect())
}

/// A `(state, count)` pair from the per-schedule cadence summary (issue #534).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleRunStateCount {
    /// Execution state.
    pub state: String,
    /// Number of `scheduled`-origin runs in this state within the window.
    pub count: i64,
}

/// SQL for [`schedule_run_state_summary`].
///
/// `GROUP BY state` + `COUNT(*)` in the database so the admin endpoint never
/// transfers millions of rows for a high-frequency schedule. `$1` = `schedule_id`,
/// `$2` = `shard_id` (prevents double-counting when two logical shards share one
/// physical Postgres database), `$3` = optional `since` bound on `started_at`,
/// `$4` = optional `until` bound on `started_at`. Restricted to
/// `origin = 'scheduled'` so backfill/manual fires never inflate cadence counts.
const SCHEDULE_RUN_SUMMARY_SQL: &str = "
SELECT
    state::TEXT AS state,
    COUNT(*)::BIGINT AS count
FROM harvest_workflow_executions
WHERE schedule_id  = $1::UUID
  AND shard_id     = $2::INT4
  AND origin       = 'scheduled'
  AND ($3::TIMESTAMPTZ IS NULL OR started_at >= $3::TIMESTAMPTZ)
  AND ($4::TIMESTAMPTZ IS NULL OR started_at <  $4::TIMESTAMPTZ)
GROUP BY state
ORDER BY state
";

#[derive(Debug, diesel::QueryableByName)]
struct ScheduleRunStateSqlRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    state: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

/// Count a schedule's **scheduled-origin** runs by terminal state (issue #534).
///
/// Restricted to `origin = 'scheduled'` so a backfill storm or ad-hoc manual fire
/// never inflates the failure ratio an operator reads off the cadence summary.
/// Shard-local; the plugin sums the per-shard counts. Uses a SQL `GROUP BY` so
/// a high-frequency schedule never transfers millions of rows to the app layer.
pub async fn schedule_run_state_summary(
    conn: &mut AsyncPgConnection,
    schedule_id: uuid::Uuid,
    shard_id: i32,
    since: Option<chrono::DateTime<Utc>>,
    until: Option<chrono::DateTime<Utc>>,
) -> HarvestResult<Vec<ScheduleRunStateCount>> {
    let rows = diesel::sql_query(SCHEDULE_RUN_SUMMARY_SQL)
        .bind::<diesel::sql_types::Uuid, _>(schedule_id)
        .bind::<diesel::sql_types::Integer, _>(shard_id)
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>, _>(since)
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>, _>(until)
        .load::<ScheduleRunStateSqlRow>(conn)
        .await
        .map_err(database_error)?;

    Ok(rows
        .into_iter()
        .map(|r| ScheduleRunStateCount {
            state: r.state,
            count: r.count,
        })
        .collect())
}

/// One workflow-type row from the per-shard non-terminal execution count
/// (issue #520, workflow-type reachability).
///
/// Each row groups the **non-terminal** executions on a single shard by
/// `workflow_name`. A non-terminal execution is one whose state is not in the
/// terminal set recognised by [`crate::erase::is_terminal_state`]
/// (`COMPLETED`/`FAILED`/`CANCELLED`/`TIMED_OUT`/`CONTINUED_AS_NEW`/`TERMINATED`)
/// — i.e. a `RUNNING`, `SUSPENDED`, or `PAUSED` run whose next replay still
/// requires the `#[workflow]` handler named by `workflow_name`.
/// Maximum number of representative non-terminal execution ids returned per
/// workflow type by [`non_terminal_counts_by_workflow_name`] (issue #700 AC2).
///
/// A bounded sample lets an operator drill straight into stuck runs of an
/// orphaned type without paginating `GET /workflows`. The per-shard SQL caps
/// each shard's contribution with a hardcoded `ARRAY_AGG(...)[1:5]` slice
/// (Diesel `sql_query` cannot interpolate this const — the literal `5` is kept
/// in sync with `REACHABILITY_SAMPLE_CAP` by a guard unit test); the plugin
/// caps the cross-shard union to the same value.
pub const REACHABILITY_SAMPLE_CAP: usize = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowTypeNonTerminalCount {
    /// Workflow type name — the handler its non-terminal executions replay against.
    pub workflow_name: String,
    /// Count of non-terminal executions of this type on the queried shard.
    pub non_terminal_count: i64,
    /// Start time of the oldest non-terminal execution of this type on the shard.
    pub oldest_started_at: chrono::DateTime<Utc>,
    /// A bounded (`REACHABILITY_SAMPLE_CAP`) set of representative non-terminal
    /// execution ids of this type on the shard, ordered oldest-first
    /// (`started_at ASC, id ASC`). Empty is impossible here — a row only exists
    /// when `non_terminal_count >= 1` — but the aggregating plugin returns an
    /// empty vec for `safe_to_remove` types with no rows at all.
    pub sample_execution_ids: Vec<Uuid>,
}

#[derive(Debug, diesel::QueryableByName)]
struct WorkflowTypeNonTerminalSqlRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    workflow_name: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    non_terminal_count: i64,
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    oldest_started_at: chrono::DateTime<Utc>,
    #[diesel(sql_type = diesel::sql_types::Array<diesel::sql_types::Uuid>)]
    sample_execution_ids: Vec<Uuid>,
}

/// SQL for [`non_terminal_counts_by_workflow_name`].
///
/// Read-only `GROUP BY workflow_name` over `harvest_workflow_executions`,
/// filtered to non-terminal states. The state set is the exact complement of
/// [`crate::erase::is_terminal_state`]. `$1` optionally narrows to a single
/// workflow type; `$2` scopes the query to a single logical shard (mirrors the
/// `shard_id` predicate in the version-usage query so that two logical shards
/// sharing the same Postgres database are never double-counted). Both params
/// are nullable — `NULL` means "no filter". Side-effect-free: no claims, no
/// writes, no events appended.
///
/// `sample_execution_ids` (issue #700 AC2) is a bounded, oldest-first sample of
/// the group's non-terminal execution ids. `ARRAY_AGG(id ORDER BY started_at
/// ASC, id ASC)` materialises the full ordered id array for the group before
/// the `[1:5]` (`REACHABILITY_SAMPLE_CAP`) slice keeps only the first few —
/// transient memory proportional to group size, acceptable at the target scale
/// (well under the < 2 s / 100k-execution budget). If a very large group's
/// full-array materialisation ever becomes a concern, the drop-in fallback is a
/// `LATERAL (SELECT ... ORDER BY started_at LIMIT n)` per group.
const NON_TERMINAL_COUNTS_SQL: &str = r"
SELECT
    workflow_name::TEXT AS workflow_name,
    COUNT(*)::BIGINT AS non_terminal_count,
    MIN(started_at) AS oldest_started_at,
    -- [1:5] MUST stay in sync with REACHABILITY_SAMPLE_CAP (guarded by a unit test)
    (ARRAY_AGG(id ORDER BY started_at ASC, id ASC))[1:5] AS sample_execution_ids
FROM harvest_workflow_executions
WHERE state NOT IN (
        'COMPLETED',
        'FAILED',
        'CANCELLED',
        'TIMED_OUT',
        'CONTINUED_AS_NEW',
        'TERMINATED'
      )
  AND ($1::TEXT IS NULL OR workflow_name = $1::TEXT)
  AND ($2::INT4 IS NULL OR shard_id = $2::INT4)
GROUP BY workflow_name
ORDER BY workflow_name
";

/// Count non-terminal workflow executions grouped by `workflow_name` on one shard.
///
/// Powers the workflow-type reachability check (issue #520): a non-terminal
/// execution directly names — via `workflow_name` — the `#[workflow]` handler
/// its next replay requires, so a non-zero count means deleting or renaming
/// that handler would strand in-flight runs in permanent replay failure.
///
/// `shard_id` scopes the query to a single logical shard. Pass `Some(id)` from
/// the per-shard fan-out so that two logical shards sharing the same Postgres
/// database are never double-counted (mirrors the `shard_id` predicate in
/// `load_version_usage`). Pass `None` only in tests or single-shard contexts
/// where the database is exclusively owned by one shard.
///
/// The optional `workflow_type` filter narrows to a single type; the result
/// shape is unchanged (an empty `Vec` when that type has no non-terminal
/// executions on this shard).
///
/// This is a read-only query: it claims nothing, mutates no state, and appends
/// no [`WorkflowEvent`].
///
/// # Errors
///
/// Returns [`HarvestError::Database`] if the query fails.
pub async fn non_terminal_counts_by_workflow_name(
    conn: &mut AsyncPgConnection,
    shard_id: Option<i32>,
    workflow_type: Option<&str>,
) -> HarvestResult<Vec<WorkflowTypeNonTerminalCount>> {
    let rows = diesel::sql_query(NON_TERMINAL_COUNTS_SQL)
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(workflow_type)
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(shard_id)
        .load::<WorkflowTypeNonTerminalSqlRow>(conn)
        .await
        .map_err(database_error)?;

    Ok(rows
        .into_iter()
        .map(|row| WorkflowTypeNonTerminalCount {
            workflow_name: row.workflow_name,
            non_terminal_count: row.non_terminal_count,
            oldest_started_at: row.oldest_started_at,
            sample_execution_ids: row.sample_execution_ids,
        })
        .collect())
}

// ---------------------------------------------------------------------------
// Grouped workflow-count snapshot (issue #544)
// ---------------------------------------------------------------------------
//
// Answers "how many RUNNING/FAILED/... per workflow type, right now, across
// every shard?" in one request instead of paginating `GET /workflows` or
// hand-querying every shard database. The count is computed with a real SQL
// `GROUP BY … COUNT(*)` so the response stays cheap even at millions of live
// executions — never by loading per-execution rows into the app layer.

/// Dimension to group `GET /workflows/count` results by (issue #544).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkflowCountDimension {
    /// Group by `state`.
    State,
    /// Group by `workflow_name`.
    WorkflowName,
}

impl WorkflowCountDimension {
    /// Wire name used in the `group_by` query parameter and response keys.
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::State => "state",
            Self::WorkflowName => "workflow_name",
        }
    }

    /// Parse a dimension from its wire name, or `None` if unknown.
    #[must_use]
    pub fn from_wire(s: &str) -> Option<Self> {
        match s {
            "state" => Some(Self::State),
            "workflow_name" => Some(Self::WorkflowName),
            _ => None,
        }
    }
}

/// Filters + grouping dimensions for the grouped workflow-count snapshot (issue #544).
///
/// Shard-local: the plugin layer fans this out across `iter_shards()` and
/// sums per-group counts across shards.
#[derive(Debug, Clone, Default)]
pub struct WorkflowCountQuery {
    /// Ordered, de-duplicated grouping dimensions. Empty means "no grouping"
    /// (a single total row).
    pub group_by: Vec<WorkflowCountDimension>,
    /// Filter: exact workflow name.
    pub workflow_name: Option<String>,
    /// Filter: restrict to these states (empty = all states).
    pub states: Vec<String>,
    /// Filter: inclusive lower bound on `started_at`.
    pub started_after: Option<chrono::DateTime<Utc>>,
    /// Filter: inclusive upper bound on `started_at` (mirrors the `/workflows`
    /// list endpoint's `started_before`, which is also inclusive).
    pub started_before: Option<chrono::DateTime<Utc>>,
}

/// One grouped count row, either raw from a single shard or already summed across shards (issue #544).
///
/// `state`/`workflow_name` are `None` exactly when that dimension was not
/// part of the query's `group_by`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowCountRow {
    /// Grouped state value, or `None` when not grouping by state.
    pub state: Option<String>,
    /// Grouped workflow name value, or `None` when not grouping by workflow name.
    pub workflow_name: Option<String>,
    /// Number of executions in this group on the queried shard.
    pub count: i64,
}

/// `WHERE` clause shared by all four `COUNT_*_SQL` shapes below (issue #544).
///
/// Factored into one constant so a future filter addition (e.g. `queue_name`)
/// is edited in exactly one place instead of four SQL strings kept in lockstep
/// by hand — a mismatch between them would silently produce a filter that
/// works for some `group_by` combinations but not others, with no compiler
/// check to catch the omission.
// The trailing `starts_with(...)` clause UNCONDITIONALLY excludes built-in
// synthetic liveness-canary runs (issue #796, AC8) so probe executions never
// pollute fleet-count snapshots. The literal mirrors
// `canary::CANARY_WORKFLOW_NAME_PREFIX` (kept in sync by
// `count_where_clause_excludes_the_canary_prefix`). `starts_with` is used, not
// `LIKE '…%'`, because `_` is a single-character wildcard in SQL `LIKE` and the
// prefix is underscore-heavy — a `LIKE` form would over-match. Distinct from
// the #512 replay canary.
const COUNT_WHERE_CLAUSE: &str = r"
WHERE shard_id = $1::INT4
  AND ($2::TEXT IS NULL OR workflow_name = $2::TEXT)
  AND ($3::TEXT[] IS NULL OR state = ANY($3::TEXT[]))
  AND ($4::TIMESTAMPTZ IS NULL OR started_at >= $4::TIMESTAMPTZ)
  AND ($5::TIMESTAMPTZ IS NULL OR started_at <= $5::TIMESTAMPTZ)
  AND NOT starts_with(workflow_name, '__harvest_canary_probe')
";

fn count_total_sql() -> String {
    format!(
        "SELECT COUNT(*)::BIGINT AS count\nFROM harvest_workflow_executions\n{COUNT_WHERE_CLAUSE}"
    )
}

fn count_by_state_sql() -> String {
    format!(
        "SELECT state::TEXT AS state, COUNT(*)::BIGINT AS count\n\
         FROM harvest_workflow_executions\n\
         {COUNT_WHERE_CLAUSE}GROUP BY state"
    )
}

fn count_by_workflow_name_sql() -> String {
    format!(
        "SELECT workflow_name::TEXT AS workflow_name, COUNT(*)::BIGINT AS count\n\
         FROM harvest_workflow_executions\n\
         {COUNT_WHERE_CLAUSE}GROUP BY workflow_name"
    )
}

fn count_by_state_and_workflow_name_sql() -> String {
    format!(
        "SELECT state::TEXT AS state, workflow_name::TEXT AS workflow_name, COUNT(*)::BIGINT AS count\n\
         FROM harvest_workflow_executions\n\
         {COUNT_WHERE_CLAUSE}GROUP BY state, workflow_name"
    )
}

#[derive(Debug, diesel::QueryableByName)]
struct CountTotalSqlRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

#[derive(Debug, diesel::QueryableByName)]
struct CountByStateSqlRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    state: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

#[derive(Debug, diesel::QueryableByName)]
struct CountByWorkflowNameSqlRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    workflow_name: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

#[derive(Debug, diesel::QueryableByName)]
struct CountByStateAndWorkflowNameSqlRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    state: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    workflow_name: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

/// Count workflow executions grouped by the requested dimensions on one shard
/// (issue #544).
///
/// Uses a real SQL `GROUP BY … COUNT(*)` (one of four static queries, selected
/// by which dimensions are requested) so the response stays cheap at any
/// execution volume — this function never loads per-execution rows into the
/// app layer. `shard_id` scopes the query to a single logical shard so two
/// logical shards sharing one physical database are never double-counted
/// (mirrors [`non_terminal_counts_by_workflow_name`] and
/// [`schedule_run_state_summary`]).
///
/// This is a read-only, eventually-consistent point-in-time snapshot: it
/// reflects committed `harvest_workflow_executions.state` at query time and
/// carries no replay or ordering guarantee under concurrent writes.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] if the query fails.
pub async fn count_workflow_executions_grouped(
    conn: &mut AsyncPgConnection,
    shard_id: i32,
    query: &WorkflowCountQuery,
) -> HarvestResult<Vec<WorkflowCountRow>> {
    use diesel::sql_types::{Array, Integer, Nullable, Text, Timestamptz};

    let workflow_name = query.workflow_name.clone();
    let states: Option<Vec<String>> = if query.states.is_empty() {
        None
    } else {
        Some(query.states.clone())
    };
    let started_after = query.started_after;
    let started_before = query.started_before;

    let group_state = query.group_by.contains(&WorkflowCountDimension::State);
    let group_workflow_name = query
        .group_by
        .contains(&WorkflowCountDimension::WorkflowName);

    match (group_state, group_workflow_name) {
        (true, true) => {
            let rows = diesel::sql_query(count_by_state_and_workflow_name_sql())
                .bind::<Integer, _>(shard_id)
                .bind::<Nullable<Text>, _>(workflow_name)
                .bind::<Nullable<Array<Text>>, _>(states)
                .bind::<Nullable<Timestamptz>, _>(started_after)
                .bind::<Nullable<Timestamptz>, _>(started_before)
                .load::<CountByStateAndWorkflowNameSqlRow>(conn)
                .await
                .map_err(database_error)?;
            Ok(rows
                .into_iter()
                .map(|r| WorkflowCountRow {
                    state: Some(r.state),
                    workflow_name: Some(r.workflow_name),
                    count: r.count,
                })
                .collect())
        }
        (true, false) => {
            let rows = diesel::sql_query(count_by_state_sql())
                .bind::<Integer, _>(shard_id)
                .bind::<Nullable<Text>, _>(workflow_name)
                .bind::<Nullable<Array<Text>>, _>(states)
                .bind::<Nullable<Timestamptz>, _>(started_after)
                .bind::<Nullable<Timestamptz>, _>(started_before)
                .load::<CountByStateSqlRow>(conn)
                .await
                .map_err(database_error)?;
            Ok(rows
                .into_iter()
                .map(|r| WorkflowCountRow {
                    state: Some(r.state),
                    workflow_name: None,
                    count: r.count,
                })
                .collect())
        }
        (false, true) => {
            let rows = diesel::sql_query(count_by_workflow_name_sql())
                .bind::<Integer, _>(shard_id)
                .bind::<Nullable<Text>, _>(workflow_name)
                .bind::<Nullable<Array<Text>>, _>(states)
                .bind::<Nullable<Timestamptz>, _>(started_after)
                .bind::<Nullable<Timestamptz>, _>(started_before)
                .load::<CountByWorkflowNameSqlRow>(conn)
                .await
                .map_err(database_error)?;
            Ok(rows
                .into_iter()
                .map(|r| WorkflowCountRow {
                    state: None,
                    workflow_name: Some(r.workflow_name),
                    count: r.count,
                })
                .collect())
        }
        (false, false) => {
            let row = diesel::sql_query(count_total_sql())
                .bind::<Integer, _>(shard_id)
                .bind::<Nullable<Text>, _>(workflow_name)
                .bind::<Nullable<Array<Text>>, _>(states)
                .bind::<Nullable<Timestamptz>, _>(started_after)
                .bind::<Nullable<Timestamptz>, _>(started_before)
                .get_result::<CountTotalSqlRow>(conn)
                .await
                .map_err(database_error)?;
            Ok(vec![WorkflowCountRow {
                state: None,
                workflow_name: None,
                count: row.count,
            }])
        }
    }
}

#[cfg(test)]
mod workflow_count_tests {
    use super::WorkflowCountDimension;

    #[test]
    fn dimension_wire_round_trips() {
        for dim in [
            WorkflowCountDimension::State,
            WorkflowCountDimension::WorkflowName,
        ] {
            assert_eq!(WorkflowCountDimension::from_wire(dim.as_wire()), Some(dim));
        }
    }

    #[test]
    fn dimension_from_wire_rejects_unknown() {
        assert_eq!(WorkflowCountDimension::from_wire("queue_name"), None);
        assert_eq!(WorkflowCountDimension::from_wire(""), None);
    }

    #[test]
    fn dimension_as_wire_matches_query_param_vocabulary() {
        // AC2: group_by accepts 'state', 'workflow_name', or 'state,workflow_name'.
        assert_eq!(WorkflowCountDimension::State.as_wire(), "state");
        assert_eq!(
            WorkflowCountDimension::WorkflowName.as_wire(),
            "workflow_name"
        );
    }

    /// Synthetic liveness-canary runs (issue #796) must be excluded from every
    /// fleet-count snapshot shape (AC8). All four SQL variants share
    /// `COUNT_WHERE_CLAUSE`, so the exclusion appears in each; the literal
    /// prefix must stay in sync with `canary::CANARY_WORKFLOW_NAME_PREFIX`.
    #[test]
    fn count_where_clause_excludes_the_canary_prefix() {
        let prefix = crate::canary::CANARY_WORKFLOW_NAME_PREFIX;
        let clause = format!("NOT starts_with(workflow_name, '{prefix}')");
        for sql in [
            super::count_total_sql(),
            super::count_by_state_sql(),
            super::count_by_workflow_name_sql(),
            super::count_by_state_and_workflow_name_sql(),
        ] {
            assert!(
                sql.contains(&clause),
                "count SQL must exclude the canary prefix `{prefix}` from fleet \
                 counts (AC8); missing `{clause}` in:\n{sql}"
            );
        }
    }
}

/// Load a workflow execution row by execution ID without locking.
pub async fn load_execution(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> HarvestResult<WorkflowExecution> {
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(database_error)?
        .ok_or_else(|| HarvestError::NotFound(format!("workflow execution {exec_id}")))
}

/// The terminal outcome of an awaited target workflow (issue #757), resolved by
/// [`read_external_await_outcome`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExternalAwaitOutcome {
    /// Target reached `COMPLETED` — carries its recorded (inflated) output.
    Completed(serde_json::Value),
    /// Target reached a non-`COMPLETED` terminal state.
    Terminal {
        /// Machine-readable reason code
        /// (`target_failed`/`target_timed_out`/`target_cancelled`/`target_terminated`).
        reason_code: String,
        /// Human-readable message from the target's terminal cause.
        message: Option<String>,
        /// Stable error-type name from a typed target failure.
        error_type: Option<String>,
        /// Structured details from a typed target failure.
        details: Option<serde_json::Value>,
        /// Advisory non-retryable flag from a typed target failure.
        non_retryable: Option<bool>,
    },
}

/// Upper bound on `WorkflowContinuedAsNew` chain hops the await-outcome reader
/// follows before giving up (matches the plugin `/result` chain-walk bound).
const AWAIT_OUTCOME_CHAIN_MAX_HOPS: usize = 128;

/// Three-state result of [`read_external_await_outcome`] (issue #757).
///
/// Distinguishing "still running" from "not found" lets the outbox drive the
/// grace window (`NotFound` + grace-expired → `target_unknown`) and the inline
/// path decide re-park vs append, without a second existence probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExternalAwaitReadResult {
    /// Target reached a terminal state — carries the resolved outcome.
    Terminal(ExternalAwaitOutcome),
    /// Target exists but is still non-terminal (`RUNNING`/`PAUSED`/…), or an
    /// in-flight `CONTINUED_AS_NEW` successor is not yet visible.
    NotYetTerminal,
    /// No execution with this id was found (drives the outbox grace window).
    NotFound,
}

/// Observe-only reader (issue #757): resolve the terminal outcome of `target`
/// for `await_external_workflow`.
///
/// Returns [`ExternalAwaitReadResult::NotYetTerminal`] when the target is still
/// `RUNNING`/`PAUSED`, [`ExternalAwaitReadResult::NotFound`] when no such
/// execution exists (the outbox grace window — not this reader — converts a
/// persistent `NotFound` into `target_unknown`), and
/// [`ExternalAwaitReadResult::Terminal`] otherwise. A `CONTINUED_AS_NEW` target
/// is followed through its successor chain (same-shard) to the true terminal.
///
/// **Never mutates the target or creates any linkage** — a pure read.
pub async fn read_external_await_outcome(
    conn: &mut AsyncPgConnection,
    target: ExecutionId,
) -> HarvestResult<ExternalAwaitReadResult> {
    let mut current = target;
    for hop in 0..AWAIT_OUTCOME_CHAIN_MAX_HOPS {
        let execution = match load_execution(conn, current).await {
            Ok(e) => e,
            // The target itself (hop 0) is absent → drive the grace window.
            // A later hop (a `CONTINUED_AS_NEW` successor not yet visible) is
            // transient: the ORIGINAL target exists, so never report unknown —
            // treat it as still-in-flight and let the outbox retry next tick.
            Err(HarvestError::NotFound(_)) => {
                return Ok(if hop == 0 {
                    ExternalAwaitReadResult::NotFound
                } else {
                    ExternalAwaitReadResult::NotYetTerminal
                });
            }
            Err(e) => return Err(e),
        };

        let outcome = match execution.state.as_str() {
            "COMPLETED" => {
                // The target's `output` row column is read RAW. Core
                // `append_events`/`load_history` use the identity codec (payload
                // codecs are a plugin-layer concern), so on a codec-encrypting
                // deployment this is the ciphertext envelope — the awaiter freezes
                // it inflated into its own history, mirroring the `FAILED`-path
                // `details` caveat below. A large output is copied inline without
                // offloading (a documented future optimization — issue #757).
                ExternalAwaitOutcome::Completed(execution.output.unwrap_or(serde_json::Value::Null))
            }
            "FAILED" => {
                // The typed failure cause (issue #767) lives in the terminal
                // `WorkflowFailed` event's fields — the execution row's `error`
                // column carries only the human message. Read the target's own
                // history (undecoded: `error`/`error_type`/`non_retryable` are in
                // the clear; `details` may be an opaque codec envelope on an
                // encrypted deployment, matching the plugin chain-walk's approach)
                // and extract the LAST `WorkflowFailed`.
                let history = store::load_history_undecoded(conn, current).await?;
                let typed = history.events.into_iter().rev().find_map(|event| {
                    if let WorkflowEvent::WorkflowFailed {
                        error,
                        error_type,
                        details,
                        non_retryable,
                    } = event
                    {
                        Some((error, error_type, details, non_retryable))
                    } else {
                        None
                    }
                });
                match typed {
                    Some((message, error_type, details, non_retryable)) => {
                        ExternalAwaitOutcome::Terminal {
                            reason_code: "target_failed".to_string(),
                            message: Some(message),
                            error_type,
                            details,
                            non_retryable,
                        }
                    }
                    // No WorkflowFailed event (defensive) — fall back to the row's
                    // human message.
                    None => ExternalAwaitOutcome::Terminal {
                        reason_code: "target_failed".to_string(),
                        message: execution.error.clone(),
                        error_type: None,
                        details: None,
                        non_retryable: None,
                    },
                }
            }
            // Non-`COMPLETED` terminals carry `error_type = Some(reason_code)`
            // (issue #757 review) so a coordinator can branch cancelled vs
            // timed-out vs terminated via `err.workflow_error_type()` instead of
            // string-matching the human message.
            "TIMED_OUT" => ExternalAwaitOutcome::Terminal {
                reason_code: "target_timed_out".to_string(),
                message: execution.error.clone(),
                error_type: Some("target_timed_out".to_string()),
                details: None,
                non_retryable: None,
            },
            "CANCELLED" => ExternalAwaitOutcome::Terminal {
                reason_code: "target_cancelled".to_string(),
                message: execution.error.clone(),
                error_type: Some("target_cancelled".to_string()),
                details: None,
                non_retryable: None,
            },
            "TERMINATED" => ExternalAwaitOutcome::Terminal {
                reason_code: "target_terminated".to_string(),
                message: execution.error.clone(),
                error_type: Some("target_terminated".to_string()),
                details: None,
                non_retryable: None,
            },
            "CONTINUED_AS_NEW" => {
                // Follow the successor chain (same shard) to the true terminal.
                let history = store::load_history_undecoded(conn, current).await?;
                let successor = history.events.into_iter().find_map(|event| {
                    if let WorkflowEvent::WorkflowContinuedAsNew { new_exec_id, .. } = event {
                        Some(new_exec_id)
                    } else {
                        None
                    }
                });
                match successor {
                    Some(next) => {
                        current = next;
                        continue;
                    }
                    // No successor recorded yet — treat as still-in-flight.
                    None => return Ok(ExternalAwaitReadResult::NotYetTerminal),
                }
            }
            // RUNNING / PAUSED / SUSPENDED (or any non-terminal): still in flight.
            _ => return Ok(ExternalAwaitReadResult::NotYetTerminal),
        };
        return Ok(ExternalAwaitReadResult::Terminal(outcome));
    }
    // Exceeded chain depth — treat as still in flight rather than fabricating a
    // terminal (the outbox retries next tick).
    Ok(ExternalAwaitReadResult::NotYetTerminal)
}

/// Scans workflow history for unresolved update handlers and records warning logs/metrics if any exist.
pub async fn check_and_report_unfinished_handlers(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    workflow_name: &str,
    metrics: Option<&(dyn crate::telemetry::MetricsRecorder + Send + Sync)>,
) -> HarvestResult<()> {
    let history = store::load_history(conn, exec_id).await?;
    let matcher = crate::replay::HistoryMatcher::new(history.events);
    let count = matcher.unfinished_update_handler_count_at_end();
    if count > 0 {
        tracing::warn!(
            workflow_name = workflow_name,
            execution_id = %exec_id,
            unfinished_update_handler_count = count,
            "Workflow completed with unfinished update handlers"
        );
        if let Some(recorder) = metrics {
            recorder.record_workflow_unfinished_handlers(workflow_name, "update", count as u64);
        }
    }
    Ok(())
}

#[cfg(test)]
mod non_terminal_sql_tests {
    use super::{NON_TERMINAL_COUNTS_SQL, REACHABILITY_SAMPLE_CAP};
    use crate::erase::is_terminal_state;

    /// Diesel `sql_query` cannot interpolate a Rust const into the SQL string,
    /// so the per-shard sample slice is a hardcoded `[1:5]` literal. This guard
    /// binds that literal to `REACHABILITY_SAMPLE_CAP`: if either the SQL slice
    /// or the const changes without the other, this test fails.
    #[test]
    fn sql_sample_slice_matches_reachability_sample_cap() {
        assert!(
            NON_TERMINAL_COUNTS_SQL.contains(&format!("[1:{REACHABILITY_SAMPLE_CAP}]")),
            "SQL sample-slice cap drifted from REACHABILITY_SAMPLE_CAP \
             ({REACHABILITY_SAMPLE_CAP}); the hardcoded [1:N] literal in \
             NON_TERMINAL_COUNTS_SQL must equal it"
        );
    }

    /// The `NOT IN (...)` state list in `NON_TERMINAL_COUNTS_SQL` must be the
    /// exact complement of `erase::is_terminal_state`. If a new terminal state is
    /// added to `is_terminal_state`, this test fails until the SQL is updated,
    /// preventing the reachability query from counting terminal runs as non-terminal
    /// and blocking safe handler removal forever.
    #[test]
    fn non_terminal_sql_excludes_exactly_terminal_states() {
        let terminal_states = [
            "COMPLETED",
            "FAILED",
            "CANCELLED",
            "TIMED_OUT",
            "CONTINUED_AS_NEW",
            "TERMINATED",
        ];
        for state in &terminal_states {
            assert!(
                is_terminal_state(state),
                "State '{state}' is listed in NON_TERMINAL_COUNTS_SQL's NOT IN clause \
                 but is_terminal_state returns false — update one of them to match"
            );
            assert!(
                NON_TERMINAL_COUNTS_SQL.contains(state),
                "is_terminal_state returns true for '{state}' but it is missing from \
                 NON_TERMINAL_COUNTS_SQL's NOT IN clause — add it to keep the lists in sync"
            );
        }
        let candidate_non_terminal = ["RUNNING", "SUSPENDED", "PAUSED"];
        for state in &candidate_non_terminal {
            assert!(
                !is_terminal_state(state),
                "State '{state}' appears in the non-terminal candidate list \
                 but is_terminal_state returned true — remove it from this test"
            );
            assert!(
                !NON_TERMINAL_COUNTS_SQL.contains(&format!("'{state}'")),
                "State '{state}' appears in the NOT IN clause of NON_TERMINAL_COUNTS_SQL \
                 but should be non-terminal — remove it from the exclusion list"
            );
        }
    }
}

#[cfg(test)]
mod pause_helper_tests {
    use super::{
        MAX_PAUSE_SHIFT_MICROS, clamped_pause_shift_micros, pause_timeout_exceeded,
        shift_external_schedule_to_close_on_resume_query, shift_schedule_to_close_on_resume_query,
    };
    use chrono::{Duration as ChronoDuration, Utc};
    use std::time::Duration;

    #[test]
    fn pause_shift_micros_clamps_unrepresentable_span_to_finite_bound() {
        // Binding an i64::MAX fallback into the `* INTERVAL '1 microsecond'`
        // shift SQL would raise a PostgreSQL "timestamp/interval out of
        // range" error and roll back the resume transaction: a span too long
        // for `num_microseconds` must clamp to the finite cap instead.
        assert_eq!(
            clamped_pause_shift_micros(ChronoDuration::MAX),
            MAX_PAUSE_SHIFT_MICROS
        );
        // A representable-but-absurd span beyond the cap clamps too.
        assert_eq!(
            clamped_pause_shift_micros(ChronoDuration::days(200 * 365)),
            MAX_PAUSE_SHIFT_MICROS
        );
    }

    #[test]
    fn pause_shift_micros_passes_normal_spans_through_untouched() {
        assert_eq!(
            clamped_pause_shift_micros(ChronoDuration::minutes(30)),
            30 * 60 * 1_000_000
        );
        assert_eq!(clamped_pause_shift_micros(ChronoDuration::zero()), 0);
        // Non-positive spans pass through; the caller skips the shift for
        // them (`pause_span_micros > 0` guard).
        assert_eq!(
            clamped_pause_shift_micros(ChronoDuration::seconds(-5)),
            -5_000_000
        );
    }

    #[test]
    fn resume_shift_query_targets_open_deadline_bearing_tasks_only() {
        // AC5 (issue #609) × #378: on resume, still-open task rows carrying a
        // cross-retry deadline are shifted forward by the pause span.
        let sql = shift_schedule_to_close_on_resume_query();
        assert!(sql.contains("UPDATE harvest_task_queue"));
        assert!(
            sql.contains("schedule_to_close_at = schedule_to_close_at +"),
            "must shift the existing deadline, not overwrite it"
        );
        assert!(
            sql.contains("'PENDING'") && sql.contains("'RUNNING'"),
            "only open tasks are shifted; terminal rows stay untouched"
        );
        assert!(
            sql.contains("schedule_to_close_at IS NOT NULL"),
            "tasks without a deadline must not be touched"
        );
        assert!(
            sql.contains("workflow_exec_id = $2"),
            "the shift is scoped to the resumed execution"
        );
    }

    #[test]
    fn resume_shift_query_restores_scheduled_at_for_frozen_rows_only() {
        // Finding 3 (issue #609 post-review hardening), option (b): a
        // pause-frozen PENDING row — its pre-shift schedule_to_close_at
        // already elapsed, so it was unclaimable for the pause's remainder —
        // also gets scheduled_at shifted forward, restoring its
        // schedule_to_start budget and retry-backoff position. Unfrozen
        // PENDING rows (still claimable during the pause) and RUNNING rows
        // must keep scheduled_at untouched.
        let sql = shift_schedule_to_close_on_resume_query();
        assert!(
            sql.contains("scheduled_at = CASE"),
            "scheduled_at must be shifted conditionally, never unconditionally"
        );
        assert!(
            sql.contains("WHEN state = 'PENDING' AND schedule_to_close_at <= NOW()"),
            "the frozen predicate is PENDING + pre-shift deadline already elapsed"
        );
        assert!(
            sql.contains("THEN scheduled_at + ($1::bigint * INTERVAL '1 microsecond')"),
            "frozen rows shift scheduled_at by exactly the pause span"
        );
        assert!(
            sql.contains("ELSE scheduled_at END"),
            "non-frozen rows must keep their scheduled_at"
        );
    }

    #[test]
    fn external_resume_shift_query_targets_open_external_tasks_only() {
        // Finding 2 (issue #609 post-review hardening): resume also shifts
        // the execution's still-open external tasks' wall-clock deadline by
        // the pause span, mirroring the task-queue treatment.
        let sql = shift_external_schedule_to_close_on_resume_query();
        assert!(sql.contains("UPDATE harvest_external_tasks"));
        assert!(
            sql.contains("schedule_to_close_at = schedule_to_close_at +"),
            "must shift the existing deadline, not overwrite it"
        );
        assert!(
            sql.contains("state = 'PENDING'"),
            "only PENDING external tasks are open; terminal rows stay untouched"
        );
        assert!(
            sql.contains("workflow_exec_id = $2"),
            "the shift is scoped to the resumed execution"
        );
        assert!(
            sql.contains("updated_at = NOW()"),
            "external-task mutations stamp updated_at"
        );
    }

    #[test]
    fn external_resume_shift_query_never_waits_on_a_locked_task_row() {
        // Third bot-review round (issue #609 post-review hardening): the
        // harvest_external_tasks lock convention is task row → execution row
        // (completion paths, timeout scanner), but this shift runs with the
        // execution row lock already held — the inverted order. It must
        // therefore never *wait* on a task row (SKIP LOCKED), or a concurrent
        // completion/scanner holding the task row and waiting on our
        // execution lock would ABBA-deadlock. Skipped rows are safe by
        // construction — see the query's doc comment.
        let sql = shift_external_schedule_to_close_on_resume_query();
        assert!(
            sql.contains("FOR UPDATE SKIP LOCKED"),
            "the shift must skip, not wait on, concurrently locked task rows"
        );
        assert!(
            sql.contains("WHERE id IN ("),
            "locking happens in the subselect so the outer UPDATE only \
             touches rows this transaction actually holds"
        );
    }

    #[test]
    fn pause_not_expired_within_ceiling() {
        let now = Utc::now();
        let paused_at = now - ChronoDuration::minutes(30);
        assert!(
            !pause_timeout_exceeded(paused_at, now, Duration::from_secs(3600)),
            "a 30-minute pause must not exceed a 1-hour ceiling"
        );
    }

    #[test]
    fn pause_expired_past_ceiling() {
        let now = Utc::now();
        let paused_at = now - ChronoDuration::hours(25);
        assert!(
            pause_timeout_exceeded(paused_at, now, Duration::from_secs(24 * 3600)),
            "a 25-hour pause must exceed the 24-hour ceiling"
        );
    }

    #[test]
    fn pause_expired_exactly_at_ceiling() {
        let now = Utc::now();
        let paused_at = now - ChronoDuration::hours(24);
        assert!(
            pause_timeout_exceeded(paused_at, now, Duration::from_secs(24 * 3600)),
            "a pause exactly at the ceiling is expired (>=)"
        );
    }

    #[test]
    fn zero_ceiling_expires_immediately() {
        let now = Utc::now();
        assert!(
            pause_timeout_exceeded(now, now, Duration::ZERO),
            "a zero ceiling must not strand a paused execution"
        );
    }
}

#[cfg(test)]
mod triage_tests {
    use super::{TriageFieldChange, TriagePatch, resolve_triage_field};

    #[test]
    fn absent_field_leaves_current_unchanged_and_reports_no_change() {
        let (resolved, change) = resolve_triage_field("owner", None, Some("alice".to_string()));
        assert_eq!(resolved, Some("alice".to_string()));
        assert!(
            change.is_none(),
            "an omitted field must not be reported as changed"
        );
    }

    #[test]
    fn setting_a_new_value_resolves_and_reports_the_change() {
        let (resolved, change) = resolve_triage_field(
            "owner",
            Some(Some("bob".to_string())),
            Some("alice".to_string()),
        );
        assert_eq!(resolved, Some("bob".to_string()));
        assert_eq!(
            change,
            Some(TriageFieldChange {
                field: "owner",
                old: Some("alice".to_string()),
                new: Some("bob".to_string()),
            })
        );
    }

    #[test]
    fn explicit_null_clears_and_reports_the_change() {
        let (resolved, change) =
            resolve_triage_field("note", Some(None), Some("investigating".to_string()));
        assert_eq!(resolved, None);
        assert_eq!(
            change,
            Some(TriageFieldChange {
                field: "note",
                old: Some("investigating".to_string()),
                new: None,
            })
        );
    }

    #[test]
    fn setting_the_same_value_is_idempotent_with_no_reported_change() {
        let (resolved, change) = resolve_triage_field(
            "severity",
            Some(Some("P1".to_string())),
            Some("P1".to_string()),
        );
        assert_eq!(resolved, Some("P1".to_string()));
        assert!(
            change.is_none(),
            "re-setting the identical value must not be reported as a change \
             -- proves idempotency at the pure-logic level (issue #759 AC4)"
        );
    }

    #[test]
    fn clearing_an_already_null_field_is_idempotent_with_no_reported_change() {
        let (resolved, change) = resolve_triage_field("severity", Some(None), None);
        assert_eq!(resolved, None);
        assert!(change.is_none());
    }

    #[test]
    fn setting_a_value_when_current_is_none_reports_old_as_none() {
        let (resolved, change) =
            resolve_triage_field("owner", Some(Some("alice".to_string())), None);
        assert_eq!(resolved, Some("alice".to_string()));
        assert_eq!(
            change,
            Some(TriageFieldChange {
                field: "owner",
                old: None,
                new: Some("alice".to_string()),
            })
        );
    }

    #[test]
    fn default_patch_touches_no_fields() {
        let patch = TriagePatch::default();
        assert_eq!(patch.owner, None);
        assert_eq!(patch.severity, None);
        assert_eq!(patch.note, None);
    }
}
