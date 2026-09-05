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
//! simply waits for a slot at claim time. [`crate::concurrency::ConcurrencyOnConflict::CancelRunning`]
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
    /// Stable wire spelling (`snake_case`), identical to what the serde
    /// `rename_all` derive emits (pinned by `on_conflict_serde_round_trip_is_snake_case`).
    ///
    /// Paired with [`Self::parse`] as the string round-trip for any surface that
    /// carries the strategy as text. Neither is on a production path today: the
    /// `#[workflow]` macro validates the attribute in the proc-macro crate (which
    /// cannot depend on this type), and `GET /admin/concurrency` serialises via
    /// serde. They exist so a future HTTP/CLI surface has one canonical spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Defer => "defer",
            Self::CancelRunning => "cancel_running",
        }
    }

    /// Parse a wire spelling. Trim- and case-tolerant so an operator-supplied
    /// value works; unknown values return `None` (never a silent fallback to
    /// `Defer`). See [`Self::as_str`] for why this has no production caller yet.
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

/// Shed decision for one latest-wins supersede pass (issue #811, Codex round 2).
///
/// `candidates` are the shed-eligible non-terminal runs on the key; `protected`
/// are in-flight admissions whose start transaction is still open on this task
/// (see `ADMITTING`). Protected runs **count toward the population** — they are
/// real non-terminal runs — but are never selected, so a nested admission can
/// never cancel the outer admission that spawned it.
///
/// Returns both numbers deliberately:
/// * `target` — how many runs the key is over its limit (clamped to
///   `SUPERSEDE_SCAN_LIMIT`).
/// * `shed` — how many we may actually cancel (`target`, capped by the number of
///   shed-eligible candidates).
///
/// `shed < target` means the remaining overflow is entirely protected in-flight
/// admissions. The key stays transiently over its limit and converges on the next
/// ordinary admission, which sees those runs unprotected — the same bounded-shed
/// philosophy as `SUPERSEDE_SCAN_LIMIT`.
///
/// Counting `protected` is what fixes the nested case: excluding it entirely (the
/// first cut) computed `supersede_count(0, 1) == 0` for a nested admission and
/// preserved BOTH runs on a `limit = 1` key.
#[must_use]
pub const fn supersede_plan(candidates: usize, protected: usize, limit: u32) -> SupersedePlan {
    let target = {
        let raw = supersede_count(candidates + protected, limit);
        if raw > SUPERSEDE_SCAN_LIMIT {
            SUPERSEDE_SCAN_LIMIT
        } else {
            raw
        }
    };
    let shed = if target > candidates {
        candidates
    } else {
        target
    };
    SupersedePlan { target, shed }
}

/// Outcome of [`supersede_plan`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SupersedePlan {
    /// How many runs the key is over its declared limit (clamped).
    pub target: usize,
    /// How many of those may actually be cancelled by this pass.
    pub shed: usize,
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
/// pass and a claim never interleave for the same key.
///
/// Taken ONLY on the `CancelRunning` path, so `Defer` starts are byte-for-byte
/// unchanged (zero extra statements).
///
/// # Lock-ordering scope (what is and is NOT guaranteed)
///
/// **Against `queue::claim_task`: deadlock-free by construction.** The claim side
/// uses the NON-blocking `pg_try_advisory_xact_lock` and simply skips the row when
/// it cannot take the lock, so it never waits on us while holding row locks we need.
///
/// **Against the durable-mutex lock (issue #691): a lock-ordering inversion is
/// possible.** Postgres's one-argument advisory locks share a single 64-bit space,
/// and `mutex::lock_mutex_key` takes a *blocking* `pg_advisory_xact_lock(hashtext(..))`
/// in that same space. Two orders exist and they are inverted:
///
/// * *mutex-key then concurrency-key* — a mutex holder reaching a terminal state
///   runs `mutex::sweep_terminal_holder_and_wake` (holding the mutex lock to commit)
///   and its completion trigger then starts a `cancel_running` workflow.
/// * *concurrency-key then mutex-key* — this function, holding the concurrency lock
///   while cancelling an incumbent that itself holds a mutex.
///
/// If both happen concurrently Postgres detects the cycle and aborts one side with
/// SQLSTATE `40P01`, surfacing as [`crate::error::HarvestError::Database`]. The
/// aborted transaction rolls back atomically — no partial supersede, no orphaned
/// cancellation — and the start is safe to retry. It is a liveness hazard, not a
/// correctness one. Reaching it requires a workflow that *both* declares
/// `on_conflict = "cancel_running"` *and* participates in `ctx.mutex` on the same
/// terminal path; see `docs/sharding.md` for the operator note.
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
///
/// # Population note (differs from the claim-time gate)
///
/// This counts non-terminal *execution rows*; the #247 claim gate
/// (`queue::claim_task`) counts *task rows* that are `RUNNING` with a non-null
/// `worker_id`. So a `PAUSED` run — or one whose workflow task is still deferred
/// at the claim gate — occupies no claim-time slot yet still counts here, and can
/// therefore be superseded. That is deliberate: latest-wins enforces "at most N
/// **non-terminal runs** per key, newest wins", which is the operator-visible
/// population, not the momentary dispatch occupancy.
///
/// # Protected in-flight admissions
///
/// Only `self_exec_id` is excluded. An outer admission whose start transaction is
/// still open on this task (see `ADMITTING`) IS returned, so the caller can count
/// it toward the population while filtering it out of the shed set (issue #811,
/// Codex round 2).
///
/// # Bounded fetch
///
/// `fetch_cap` bounds the result set. The caller only needs enough rows to compute
/// `supersede_count(len, limit).min(SUPERSEDE_SCAN_LIMIT)`, which saturates once
/// `len >= limit + SUPERSEDE_SCAN_LIMIT` — so a key with a large backlog never
/// materialises the whole backlog while holding the advisory lock.
#[cfg(feature = "db")]
async fn active_runs_for_key(
    conn: &mut diesel_async::AsyncPgConnection,
    workflow_name: &str,
    concurrency_key: &str,
    self_exec_id: crate::types::ExecutionId,
    fetch_cap: i64,
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

    // Only the admitted run itself is excluded from the scan. A protected
    // in-flight admission (issue #811, Codex round 2) is deliberately RETURNED
    // here so it still counts toward the key's population; the caller filters it
    // out of the shed set. Excluding it from the query too would under-count the
    // group and let a nested admission preserve an over-limit population.
    let excluded: Vec<uuid::Uuid> = vec![self_exec_id.as_uuid()];

    let rows: Vec<Row> = diesel::sql_query(
        "SELECT e.id, e.workflow_name, e.queue_name \
         FROM harvest_workflow_executions e \
         WHERE e.workflow_name = $1 \
           AND e.state IN ('RUNNING', 'PAUSED') \
           AND e.id <> ALL($2) \
           AND EXISTS ( \
               SELECT 1 FROM harvest_task_queue t \
               WHERE t.workflow_exec_id = e.id \
                 AND t.task_type = 'workflow' \
                 AND t.concurrency_key = $3 \
           ) \
         ORDER BY e.started_at ASC, e.id ASC \
         LIMIT $4",
    )
    .bind::<diesel::sql_types::Text, _>(workflow_name)
    .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(excluded)
    .bind::<diesel::sql_types::Text, _>(concurrency_key)
    .bind::<diesel::sql_types::BigInt, _>(fetch_cap)
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

/// `true` when a [`crate::error::HarvestError::Config`] raised by a cancel is the
/// benign "candidate reached a terminal state between the scan and the cancel"
/// race, rather than a genuine fault.
///
/// `Config` is a general-purpose variant, so matching it wholesale would also
/// swallow real faults reachable from inside `cancel_workflow_execution_collect`
/// (a malformed `parent_close_policy` column, a completion-trigger start rejected
/// by an admission gate). Those must stay visible — silently skipping one leaves
/// the key over its declared limit with no diagnostic.
#[must_use]
pub fn is_already_terminal_cancel_race(message: &str) -> bool {
    message.contains("is already terminal (") || message.contains("is no longer running")
}

#[cfg(feature = "db")]
tokio::task_local! {
    /// Execution ids whose start transaction is still open on this task.
    ///
    /// A cancellation performed by [`supersede_running_for_key`] runs the superseded
    /// run's terminal chokepoint, which can start a completion-trigger target
    /// **in the same transaction** — and that nested start may itself supersede.
    /// Because the outer admission's row is already inserted (uncommitted but
    /// visible within the transaction) and carries the same concurrency key, a
    /// nested pass with a different `self_exec_id` would otherwise treat it as a
    /// candidate and cancel the very run the outer start is reporting as created.
    ///
    /// Each pass scopes its own id (plus everything it inherited) for the duration
    /// of its cancellations, and every nested scan excludes the whole set.
    static ADMITTING: Vec<crate::types::ExecutionId>;
}

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
    metrics: Option<&(dyn crate::telemetry::MetricsRecorder + Send + Sync)>,
) -> crate::error::HarvestResult<SupersedeOutcome> {
    let inherited: Vec<crate::types::ExecutionId> =
        ADMITTING.try_with(Clone::clone).unwrap_or_default();
    let mut protected = inherited.clone();
    protected.push(self_exec_id);

    ADMITTING
        .scope(
            protected,
            supersede_inner(
                conn,
                workflow_name,
                concurrency_key,
                limit,
                self_exec_id,
                inherited,
                metrics,
            ),
        )
        .await
}

#[cfg(feature = "db")]
async fn supersede_inner(
    conn: &mut diesel_async::AsyncPgConnection,
    workflow_name: &str,
    concurrency_key: &str,
    limit: u32,
    self_exec_id: crate::types::ExecutionId,
    inherited: Vec<crate::types::ExecutionId>,
    metrics: Option<&(dyn crate::telemetry::MetricsRecorder + Send + Sync)>,
) -> crate::error::HarvestResult<SupersedeOutcome> {
    lock_concurrency_key(conn, concurrency_key).await?;

    // Enough rows to compute the (clamped) shed count exactly -- see
    // `active_runs_for_key`'s "Bounded fetch" note. Saturating so a pathological
    // `limit` can never overflow the bind.
    let fetch_cap =
        i64::from(limit).saturating_add(i64::try_from(SUPERSEDE_SCAN_LIMIT).unwrap_or(i64::MAX));
    let others = active_runs_for_key(
        conn,
        workflow_name,
        concurrency_key,
        self_exec_id,
        fetch_cap,
    )
    .await?;

    // Protected in-flight admissions COUNT toward the population but are never
    // shed (issue #811, Codex round 2).
    //
    // `inherited` holds the outer admission(s) whose start transaction is still
    // open on this task -- see `ADMITTING`. Excluding them from the population
    // entirely (as the first cut did) under-counted the group: a nested
    // admission spawned while cancelling an incumbent would compute
    // `supersede_count(0, 1) == 0` and preserve BOTH the outer run and itself on
    // a `limit = 1` key. Counting them here makes the shed target honest, so
    // every overflow that CAN be shed is shed.
    let (candidates, protected): (Vec<SupersededRun>, Vec<SupersededRun>) = others
        .into_iter()
        .partition(|run| !inherited.contains(&run.exec_id));

    let SupersedePlan {
        target: shed_target,
        shed,
    } = supersede_plan(candidates.len(), protected.len(), limit);
    if shed_target > shed {
        // The only over-limit runs are protected in-flight admissions. The group
        // stays transiently over its declared limit and converges on the next
        // ordinary admission for the key, which sees them unprotected -- the same
        // bounded-shed philosophy as `SUPERSEDE_SCAN_LIMIT`.
        tracing::warn!(
            workflow = %workflow_name,
            concurrency_key = %concurrency_key,
            limit,
            shed_target,
            shed,
            protected = protected.len(),
            "harvest: latest-wins supersede could not shed the full overflow because the \
             remaining runs are protected in-flight admissions; the key stays over its \
             declared limit until the next admission",
        );
        // issue #1197, item 2: promote the warn above to an alertable counter
        // so operators can monitor keys that stay transiently over their
        // declared limit, rather than relying on log scraping.
        //
        // The ONLY reachable path to this branch is a NESTED admission (a
        // non-empty `inherited`, i.e. this call is running inside another
        // admission's `ADMITTING` scope) — see this function's `inherited`
        // parameter doc. Every such nesting is reached through this module's
        // own shed loop below calling `cancel_workflow_execution_collect`,
        // which -- like every other cancel/terminate/parent-close-cascade
        // path in `execution.rs` -- evaluates completion triggers with
        // `metrics: None` (no caller-supplied recorder in scope at that
        // chokepoint). Falling back to the process-global recorder here
        // mirrors the identical, already-established fallback
        // `completion_trigger::evaluate_triggers_for_execution_collecting`'s
        // admission-gate-block branch uses for the same reason: without it,
        // this counter would be wired up but structurally unreachable with a
        // real recorder, silently never firing in production.
        let resolved_metrics = crate::admission_gate::resolve_metrics_with_global_fallback(metrics);
        if let Some(m) = resolved_metrics.as_dyn() {
            let gap = u64::try_from(shed_target - shed).unwrap_or(u64::MAX);
            crate::telemetry::emit_concurrency_residual_over_limit(m, workflow_name, gap);
        }
    }
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
                metrics,
            )
            .await
            {
                Ok(v) => v,
                // The candidate reached a terminal state between the scan and the
                // cancel (it finished on its own, or an operator cancelled it).
                // The goal -- "not running" -- is already met, so this is a skip,
                // never a failed admission.
                Err(crate::error::HarvestError::NotFound(_)) => continue,
                Err(crate::error::HarvestError::Config(msg)) => {
                    if !is_already_terminal_cancel_race(&msg) {
                        // Not the benign race: a genuine fault inside the cancel.
                        // Skipping keeps one corrupt neighbour from wedging every
                        // future admission for this key, but it must never be
                        // silent -- the key stays over its limit until next time.
                        tracing::warn!(
                            candidate = %candidate.exec_id,
                            workflow = %workflow_name,
                            concurrency_key = %concurrency_key,
                            error = %msg,
                            "harvest: latest-wins supersede skipped a candidate on an \
                             unexpected error; the key may remain over its declared limit \
                             until the next admission",
                        );
                    }
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
    // ── issue #811 Codex round 2: protected in-flight admissions count ────

    #[test]
    fn plan_counts_protected_toward_the_population() {
        // The nested case: limit 1, the ONLY other run on the key is the outer
        // admission (protected). The population is 2, so the key is 1 over --
        // even though nothing may be shed.
        let plan = supersede_plan(0, 1, 1);
        assert_eq!(plan.target, 1, "protected run must count toward population");
        assert_eq!(plan.shed, 0, "a protected run must never be shed");
    }

    #[test]
    fn plan_ignoring_protected_would_report_no_overflow() {
        // Falsifies the pre-fix behaviour, which passed only the candidate count:
        // `supersede_count(0, 1) == 0` -> the nested admission preserved BOTH runs.
        assert_eq!(supersede_count(0, 1), 0);
        assert_eq!(supersede_plan(0, 1, 1).target, 1);
    }

    #[test]
    fn plan_sheds_every_candidate_the_honest_count_demands() {
        // limit 2, one protected + two candidates -> population 4, over by 2.
        // Pre-fix this computed `supersede_count(2, 2) == 1` and shed only one.
        let plan = supersede_plan(2, 1, 2);
        assert_eq!(plan.target, 2);
        assert_eq!(plan.shed, 2);
    }

    #[test]
    fn plan_shed_is_capped_by_available_candidates() {
        // Over by 2 but only one shed-eligible run: shed what we can, and report
        // the gap so the caller can warn.
        let plan = supersede_plan(1, 2, 2);
        assert_eq!(plan.target, 2);
        assert_eq!(plan.shed, 1);
    }

    #[test]
    fn plan_with_no_protected_matches_supersede_count() {
        // The common (non-nested) path is byte-for-byte unchanged.
        for candidates in 0_usize..6 {
            for limit in 1_u32..4 {
                let plan = supersede_plan(candidates, 0, limit);
                let expected = supersede_count(candidates, limit).min(SUPERSEDE_SCAN_LIMIT);
                assert_eq!(
                    plan.target, expected,
                    "candidates={candidates} limit={limit}"
                );
                assert_eq!(plan.shed, expected.min(candidates));
            }
        }
    }

    #[test]
    fn plan_clamps_target_to_the_scan_limit() {
        let plan = supersede_plan(SUPERSEDE_SCAN_LIMIT * 4, 0, 1);
        assert_eq!(plan.target, SUPERSEDE_SCAN_LIMIT);
        assert_eq!(plan.shed, SUPERSEDE_SCAN_LIMIT);
    }

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
