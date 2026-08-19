//! Per-key concurrency limits for tenant fair-share scheduling (issue #247).
//!
//! # Overview
//!
//! When multiple tenants share a worker fleet, a single noisy tenant can
//! saturate the pool and starve everyone else.  `ConcurrencyPolicy` lets an
//! author declare a *key expression* and a *limit*:
//!
//! ```rust
//! use autumn_harvest::concurrency::ConcurrencyPolicy;
//!
//! let policy = ConcurrencyPolicy::new("input.tenant_id", 10);
//! assert_eq!(policy.limit, 10);
//! ```
//!
//! At dispatch time the worker resolves the expression against the workflow's
//! JSON input (via [`resolve_concurrency_key`]) to get the concrete group key
//! (e.g. `"acme"`), then passes `(key, limit)` to [`crate::queue::EnqueueParams`]
//! so the `SKIP LOCKED` claim query enforces it across the whole fleet.
//!
//! # Overflow strategy (issue #811)
//!
//! By default an over-limit start is *deferred*: the task row is enqueued and
//! simply waits for a slot at claim time. [`ConcurrencyOnConflict::CancelRunning`]
//! flips that to *latest-wins* — the newest admitted run supersedes the oldest
//! in-flight run(s) for the same key, using the ordinary cooperative
//! cancellation path (no new event variant, no migration).
//!
//! ```rust
//! use autumn_harvest::concurrency::{ConcurrencyOnConflict, ConcurrencyPolicy};
//!
//! let latest_wins = ConcurrencyPolicy::new("input.doc_id", 1)
//!     .with_on_conflict(ConcurrencyOnConflict::CancelRunning);
//! assert!(latest_wins.on_conflict.is_cancel_running());
//! ```
//!
//! # Sharding note
//!
//! Limits are enforced *within a shard*. Cross-shard global limits are out of
//! scope; embedders wanting a true global cap should route all executions for
//! a given key to a single shard via a custom [`crate::ShardRouter`].
//! See `docs/sharding.md` for details.

/// What to do when admitting a run would exceed the per-key concurrency limit.
///
/// Issue #811. The default ([`Self::Defer`]) is today's behaviour: the task row
/// is enqueued and waits for a free slot at claim time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConcurrencyOnConflict {
    /// Enqueue the new run and let it wait for a slot (today's behaviour).
    #[default]
    Defer,
    /// Latest-wins: admit the new run immediately and cooperatively cancel the
    /// oldest in-flight run(s) for the same key until the limit is respected.
    CancelRunning,
}

impl ConcurrencyOnConflict {
    /// Stable wire/label spelling (`snake_case`), used by the macro attribute,
    /// the HTTP surface, and `GET /admin/concurrency`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Defer => "defer",
            Self::CancelRunning => "cancel_running",
        }
    }

    /// Parse a wire/attribute spelling. Trim- and case-tolerant so an
    /// operator-supplied HTTP body value works; unknown values return `None`
    /// (never a silent fallback to `Defer`).
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "defer" => Some(Self::Defer),
            "cancel_running" => Some(Self::CancelRunning),
            _ => None,
        }
    }

    /// `true` when this strategy supersedes in-flight runs (latest-wins).
    #[must_use]
    pub const fn is_cancel_running(self) -> bool {
        matches!(self, Self::CancelRunning)
    }
}

/// Declarative per-key concurrency constraint attached to a [`crate::info::WorkflowInfo`].
///
/// The macro `#[workflow(concurrency(key = "input.tenant_id", limit = 10))]`
/// populates this struct on the companion `WorkflowInfo`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConcurrencyPolicy {
    /// JSON field path (dot-notation) resolved against the workflow input to
    /// produce the runtime group key.  The `"input."` prefix is stripped if
    /// present so `"input.tenant_id"` and `"tenant_id"` are equivalent.
    ///
    /// Nested paths like `"user.id"` walk into nested objects.
    pub key_expr: &'static str,
    /// Maximum number of RUNNING workflow tasks with the same resolved key,
    /// enforced across the whole worker fleet for this shard.
    pub limit: u32,
    /// What to do when admitting a run would exceed [`Self::limit`] (issue #811).
    pub on_conflict: ConcurrencyOnConflict,
}

impl ConcurrencyPolicy {
    /// Build a policy with the default [`ConcurrencyOnConflict::Defer`] strategy.
    #[must_use]
    pub const fn new(key_expr: &'static str, limit: u32) -> Self {
        Self {
            key_expr,
            limit,
            on_conflict: ConcurrencyOnConflict::Defer,
        }
    }

    /// Set the overflow strategy (issue #811).
    #[must_use]
    pub const fn with_on_conflict(mut self, on_conflict: ConcurrencyOnConflict) -> Self {
        self.on_conflict = on_conflict;
        self
    }
}

/// How many *other* non-terminal runs for a key must be superseded so that the
/// post-admission in-flight count respects `limit`.
///
/// `existing_others` counts the non-terminal runs for the key **excluding** the
/// run being admitted. The admitted run itself always survives (latest-wins), so
/// the shed count is `(existing_others + 1).saturating_sub(max(limit, 1))`.
///
/// A `limit` of `0` is clamped to `1`: a literal zero would demand cancelling
/// every run *including the one we just admitted*, which is never the intent.
#[must_use]
pub const fn supersede_count(existing_others: usize, limit: u32) -> usize {
    let effective = if limit == 0 { 1 } else { limit };
    // `as usize` is lossless on every supported target (>= 32-bit pointers).
    let cap = effective as usize;
    (existing_others + 1).saturating_sub(cap)
}

/// Resolve a dot-notation key expression against a JSON input payload.
///
/// The `"input."` prefix is stripped if present so both `"tenant_id"` and
/// `"input.tenant_id"` work identically.  Nested paths (e.g. `"user.id"`)
/// walk into nested JSON objects.
///
/// Returns `None` when:
/// - The input is not a JSON object.
/// - Any segment along the path is missing.
/// - The resolved value is JSON `null`.
///
/// Non-string values are converted to their JSON string representation
/// (`123` → `"123"`, `true` → `"true"`) so the caller always gets a
/// plain `String` usable as a concurrency group key.
///
/// # Examples
///
/// ```rust
/// use autumn_harvest::concurrency::resolve_concurrency_key;
///
/// let input = serde_json::json!({ "tenant_id": "acme" });
/// assert_eq!(
///     resolve_concurrency_key("input.tenant_id", &input),
///     Some("acme".to_string()),
/// );
///
/// let nested = serde_json::json!({ "user": { "id": 42 } });
/// assert_eq!(
///     resolve_concurrency_key("user.id", &nested),
///     Some("42".to_string()),
/// );
/// ```
#[must_use]
pub fn resolve_concurrency_key(expr: &str, input: &serde_json::Value) -> Option<String> {
    // Strip the "input." prefix so "input.tenant_id" == "tenant_id".
    let path = expr.strip_prefix("input.").unwrap_or(expr);

    let mut current = input;
    for segment in path.split('.') {
        current = current.as_object()?.get(segment)?;
    }

    match current {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

// ── Latest-wins supersede (issue #811) ───────────────────────────────────────

/// Hard cap on how many in-flight runs one admission may supersede.
///
/// Latest-wins is a per-key *fair-share* control, not a bulk-cancel tool: a
/// single start should never open an unbounded transaction cancelling hundreds
/// of executions (each cancel appends events, fails task rows, and runs the
/// parent-close cascade). If a key is over the cap by more than this, the
/// excess is shed by the *next* admission for the same key, so the population
/// still converges without one start paying an unbounded cost.
pub const SUPERSEDE_SCAN_LIMIT: usize = 32;

/// One run that a latest-wins admission superseded.
#[cfg(feature = "db")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupersededRun {
    /// The superseded execution.
    pub exec_id: crate::types::ExecutionId,
    /// Workflow type name — the only label on `harvest.concurrency.superseded`.
    pub workflow_name: String,
    /// Task queue the superseded run was on, for the terminal-outcome metric.
    pub queue_name: String,
}

/// What a supersede pass produced, for the caller to persist/emit.
#[cfg(feature = "db")]
#[derive(Debug, Default)]
pub struct SupersedeOutcome {
    /// Runs actually transitioned to CANCELLED by this admission.
    pub superseded: Vec<SupersededRun>,
    /// Completion-trigger / parent-close follow-up starts produced by those
    /// cancellations. MUST only be spawned after the caller's outer commit.
    pub deferred_starts: Vec<crate::completion_trigger::DeferredTriggerStart>,
    /// Unfinished-handler checks produced by those cancellations.
    pub deferred_checks: Vec<(crate::types::ExecutionId, String)>,
}

/// Serialize every latest-wins admission for a key behind an advisory lock.
///
/// Uses the SAME `hashtext(key)::bigint` namespace the claim-time concurrency
/// gate uses (`queue::claim_task`'s `pg_try_advisory_xact_lock`), so a supersede
/// pass and a claim never interleave for the same key. Deadlock-free by
/// construction: the claim side uses the NON-blocking `pg_try_advisory_xact_lock`
/// and simply skips the row when it cannot take the lock, so it never waits on
/// us while holding row locks we need.
///
/// Taken ONLY on the `CancelRunning` path, so `Defer` starts are byte-for-byte
/// unchanged (zero extra statements).
#[cfg(feature = "db")]
async fn lock_concurrency_key(
    conn: &mut diesel_async::AsyncPgConnection,
    concurrency_key: &str,
) -> crate::error::HarvestResult<()> {
    use diesel_async::RunQueryDsl;
    diesel::sql_query("SELECT pg_advisory_xact_lock(hashtext($1)::bigint)")
        .bind::<diesel::sql_types::Text, _>(concurrency_key)
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(())
}

/// Non-terminal runs sharing `(workflow_name, concurrency_key)`, oldest first.
///
/// Scoped to the workflow TYPE as well as the key so a latest-wins policy can
/// never cancel a *different* workflow type that merely resolved the same key
/// string and did not opt in. That scoping is also what makes this migration-free:
/// the resolved key lives on `harvest_task_queue`, and the join is served by the
/// existing `(workflow_name, workflow_id, shard_id)` and task-queue indexes
/// rather than a new column on `harvest_workflow_executions`.
///
/// `SUSPENDED` is deliberately absent: it is not a persisted state (the state
/// CHECK constraint forbids it), so `RUNNING`/`PAUSED` is the complete active set.
#[cfg(feature = "db")]
async fn active_runs_for_key(
    conn: &mut diesel_async::AsyncPgConnection,
    workflow_name: &str,
    concurrency_key: &str,
    self_exec_id: crate::types::ExecutionId,
) -> crate::error::HarvestResult<Vec<SupersededRun>> {
    use diesel_async::RunQueryDsl;

    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: uuid::Uuid,
        #[diesel(sql_type = diesel::sql_types::Text)]
        workflow_name: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        queue_name: String,
    }

    let rows: Vec<Row> = diesel::sql_query(
        "SELECT e.id, e.workflow_name, e.queue_name \
         FROM harvest_workflow_executions e \
         WHERE e.workflow_name = $1 \
           AND e.state IN ('RUNNING', 'PAUSED') \
           AND e.id <> $2 \
           AND EXISTS ( \
               SELECT 1 FROM harvest_task_queue t \
               WHERE t.workflow_exec_id = e.id \
                 AND t.task_type = 'workflow' \
                 AND t.concurrency_key = $3 \
           ) \
         ORDER BY e.started_at ASC, e.id ASC",
    )
    .bind::<diesel::sql_types::Text, _>(workflow_name)
    .bind::<diesel::sql_types::Uuid, _>(self_exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(concurrency_key)
    .load(conn)
    .await
    .map_err(crate::error::database_error)?;

    Ok(rows
        .into_iter()
        .map(|r| SupersededRun {
            exec_id: crate::types::ExecutionId::from_uuid(r.id),
            workflow_name: r.workflow_name,
            queue_name: r.queue_name,
        })
        .collect())
}

/// Cancellation reason recorded on a superseded run.
pub const SUPERSEDE_CANCEL_REASON: &str = "superseded by a newer run for the same concurrency key";

/// Latest-wins: cancel the OLDEST in-flight runs for `(workflow_name, key)` until
/// the post-admission population respects `limit` (issue #811).
///
/// Must be called from INSIDE the start transaction, AFTER the admitted run's own
/// row is inserted and ONLY when that insert actually created a fresh execution.
/// Both are load-bearing:
///
/// * Superseding *before* the insert would cancel the incumbent and then let the
///   reuse policy attach to the very run it just cancelled.
/// * Superseding on an *attach* (`created == false`) would cancel runs on behalf
///   of a start that admitted nothing.
///
/// The admitted run is excluded by `self_exec_id`, so latest-wins can never
/// cancel itself. Cancellation uses the ordinary cooperative path
/// ([`crate::execution::cancel_workflow_execution_collect`]): the superseded run
/// reaches `CANCELLED`, its `ctx.is_cancelled()` / Saga compensation fire, and
/// its `ParentClosePolicy` cascade runs normally. No new `WorkflowEvent` variant
/// and no migration (AC5).
///
/// # Errors
///
/// Propagates database failures from the advisory lock, the candidate scan, or a
/// cancellation. A candidate that reached a terminal state between the scan and
/// the cancel is skipped, not an error.
#[cfg(feature = "db")]
pub async fn supersede_running_for_key(
    conn: &mut diesel_async::AsyncPgConnection,
    workflow_name: &str,
    concurrency_key: &str,
    limit: u32,
    self_exec_id: crate::types::ExecutionId,
) -> crate::error::HarvestResult<SupersedeOutcome> {
    lock_concurrency_key(conn, concurrency_key).await?;

    let candidates =
        active_runs_for_key(conn, workflow_name, concurrency_key, self_exec_id).await?;
    let shed = supersede_count(candidates.len(), limit).min(SUPERSEDE_SCAN_LIMIT);
    if shed == 0 {
        return Ok(SupersedeOutcome::default());
    }

    let mut outcome = SupersedeOutcome::default();
    for candidate in candidates.into_iter().take(shed) {
        let (cancelled, mut deferred, mut checks, _terminal_metric) =
            match crate::execution::cancel_workflow_execution_collect(
                conn,
                candidate.exec_id,
                SUPERSEDE_CANCEL_REASON,
            )
            .await
            {
                Ok(v) => v,
                // The candidate reached a terminal state between the scan and the
                // cancel (it finished on its own, or an operator cancelled it).
                // The goal -- "not running" -- is already met, so this is a skip,
                // never a failed admission.
                Err(
                    crate::error::HarvestError::NotFound(_) | crate::error::HarvestError::Config(_),
                ) => {
                    continue;
                }
                Err(e) => return Err(e),
            };

        outcome.deferred_starts.append(&mut deferred);
        outcome.deferred_checks.append(&mut checks);
        // Only count a run this admission actually transitioned. An idempotent
        // no-op cancel (already CANCELLED) is not a supersede.
        if cancelled.newly_cancelled {
            outcome.superseded.push(candidate);
        }
    }

    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_top_level_field() {
        let input = serde_json::json!({ "tenant_id": "acme" });
        assert_eq!(
            resolve_concurrency_key("tenant_id", &input),
            Some("acme".to_string())
        );
    }

    #[test]
    fn resolve_input_prefix_stripped() {
        let input = serde_json::json!({ "tenant_id": "acme" });
        assert_eq!(
            resolve_concurrency_key("input.tenant_id", &input),
            Some("acme".to_string())
        );
    }

    #[test]
    fn resolve_nested() {
        let input = serde_json::json!({ "user": { "id": 42 } });
        assert_eq!(
            resolve_concurrency_key("user.id", &input),
            Some("42".to_string())
        );
    }

    #[test]
    fn resolve_missing_returns_none() {
        let input = serde_json::json!({ "other": "val" });
        assert_eq!(resolve_concurrency_key("tenant_id", &input), None);
    }

    #[test]
    fn resolve_null_returns_none() {
        let input = serde_json::json!({ "tenant_id": null });
        assert_eq!(resolve_concurrency_key("tenant_id", &input), None);
    }

    #[test]
    fn resolve_integer_as_string() {
        let input = serde_json::json!({ "tenant_id": 123 });
        assert_eq!(
            resolve_concurrency_key("tenant_id", &input),
            Some("123".to_string())
        );
    }

    #[test]
    fn resolve_non_object_input() {
        let input = serde_json::json!("plain_string");
        assert_eq!(resolve_concurrency_key("tenant_id", &input), None);
    }

    // ── issue #811: latest-wins (CancelRunning) overflow strategy ──────────

    #[test]
    fn on_conflict_defaults_to_defer() {
        assert_eq!(
            ConcurrencyOnConflict::default(),
            ConcurrencyOnConflict::Defer
        );
    }

    #[test]
    fn policy_new_defaults_to_defer() {
        let policy = ConcurrencyPolicy::new("input.tenant_id", 10);
        assert_eq!(policy.key_expr, "input.tenant_id");
        assert_eq!(policy.limit, 10);
        assert_eq!(policy.on_conflict, ConcurrencyOnConflict::Defer);
    }

    #[test]
    fn policy_with_on_conflict_sets_strategy() {
        let policy = ConcurrencyPolicy::new("input.doc_id", 1)
            .with_on_conflict(ConcurrencyOnConflict::CancelRunning);
        assert_eq!(policy.on_conflict, ConcurrencyOnConflict::CancelRunning);
        assert!(policy.on_conflict.is_cancel_running());
    }

    #[test]
    fn on_conflict_as_str_is_snake_case() {
        assert_eq!(ConcurrencyOnConflict::Defer.as_str(), "defer");
        assert_eq!(
            ConcurrencyOnConflict::CancelRunning.as_str(),
            "cancel_running"
        );
    }

    #[test]
    fn on_conflict_parses_from_wire_string() {
        assert_eq!(
            ConcurrencyOnConflict::parse("defer"),
            Some(ConcurrencyOnConflict::Defer)
        );
        assert_eq!(
            ConcurrencyOnConflict::parse("cancel_running"),
            Some(ConcurrencyOnConflict::CancelRunning)
        );
        // Case/whitespace tolerant so an operator-supplied HTTP body value works.
        assert_eq!(
            ConcurrencyOnConflict::parse("  CANCEL_RUNNING "),
            Some(ConcurrencyOnConflict::CancelRunning)
        );
        assert_eq!(ConcurrencyOnConflict::parse("terminate_running"), None);
        assert_eq!(ConcurrencyOnConflict::parse(""), None);
    }

    #[test]
    fn on_conflict_serde_round_trip_is_snake_case() {
        let json = serde_json::to_string(&ConcurrencyOnConflict::CancelRunning).unwrap();
        assert_eq!(json, "\"cancel_running\"");
        let back: ConcurrencyOnConflict = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ConcurrencyOnConflict::CancelRunning);
        assert_eq!(
            serde_json::to_string(&ConcurrencyOnConflict::Defer).unwrap(),
            "\"defer\""
        );
    }

    // `supersede_count(existing_others, limit)` is the whole latest-wins
    // decision: after our own execution is admitted, how many of the OTHER
    // non-terminal runs for this key must be cancelled so that the post-admit
    // in-flight count is <= limit.
    #[test]
    fn supersede_count_limit_one_cancels_the_single_incumbent() {
        assert_eq!(supersede_count(1, 1), 1);
    }

    #[test]
    fn supersede_count_limit_one_with_no_incumbent_cancels_nothing() {
        assert_eq!(supersede_count(0, 1), 0);
    }

    #[test]
    fn supersede_count_limit_n_cancels_down_to_the_cap() {
        // limit = 3, three incumbents + us = 4 -> shed 1 (the oldest).
        assert_eq!(supersede_count(3, 3), 1);
        // limit = 3, two incumbents + us = 3 -> already at the cap, shed none.
        assert_eq!(supersede_count(2, 3), 0);
        // limit = 3, five incumbents + us = 6 -> shed 3.
        assert_eq!(supersede_count(5, 3), 3);
    }

    #[test]
    fn supersede_count_never_underflows_when_under_the_cap() {
        assert_eq!(supersede_count(0, 10), 0);
        assert_eq!(supersede_count(1, 10), 0);
    }

    #[test]
    fn supersede_count_treats_zero_limit_as_one() {
        // A `limit = 0` policy is rejected by the macro and by
        // `HarvestBuilder::try_build`, but a hand-built `StartWorkflowParams`
        // can still carry it. Clamping to 1 keeps the surviving run alive; a
        // literal 0 would demand cancelling everything INCLUDING ourselves.
        assert_eq!(supersede_count(1, 0), 1);
        assert_eq!(supersede_count(0, 0), 0);
    }

    #[test]
    fn supersede_count_saturates_on_absurd_limit() {
        assert_eq!(supersede_count(3, u32::MAX), 0);
    }
}
