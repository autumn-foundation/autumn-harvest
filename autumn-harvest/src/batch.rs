//! Batch operations for fleet-wide workflow management (issue #102).
//!
//! Operators routinely need to act on hundreds or thousands of workflows at
//! once: cancel everything triggered by a bad release, signal every in-flight
//! onboarding to switch to a fallback path, terminate runaway DAG runs from a
//! misconfigured schedule. The single-target `cancel_workflow_execution` and
//! `send_signal` paths are too low-level for that — operators end up writing
//! throwaway scripts that loop over `GET /workflows` and fire one request per
//! row, with no rate limiting and no durability if the script's host dies.
//!
//! This module models a batch as a durable row in `harvest_batch_jobs`. The
//! API layer inserts the row and returns an id immediately; a background
//! executor polls open jobs, walks the filter result set, and applies the
//! per-target action via the existing single-target write paths so the event
//! log stays unchanged. Per-target failures are captured in the `errors`
//! field and counted in `failed`; they do not abort the batch.

use std::str::FromStr;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// What the batch operation does to each matched workflow execution.
///
/// `Cancel` issues a graceful cancel via the existing
/// `cancel_workflow_execution` path. `Terminate` is a hard finalize that does
/// not run cancellation handlers — today it shares the same DB write as
/// `Cancel` (the durable terminal state is `CANCELLED` either way), but the
/// reason string is distinct so operators can tell the two apart in
/// post-mortems and so the Phase 4 cancellation pipeline can branch on it.
/// `Signal` enqueues a signal via the existing `signal::send_signal` path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum BatchAction {
    Cancel,
    Terminate,
    Signal,
}

impl BatchAction {
    /// Stable wire-format string used by both the API and the DB column.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cancel => "Cancel",
            Self::Terminate => "Terminate",
            Self::Signal => "Signal",
        }
    }
}

impl FromStr for BatchAction {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "Cancel" => Ok(Self::Cancel),
            "Terminate" => Ok(Self::Terminate),
            "Signal" => Ok(Self::Signal),
            other => Err(format!(
                "unknown batch action '{other}'; expected one of Cancel, Terminate, Signal"
            )),
        }
    }
}

/// Lifecycle status of a batch job row.
///
/// New rows land in `Pending`, transition to `Running` when the executor
/// claims them, and end in `Completed` (regardless of per-target failures —
/// per-target failures are captured in the `errors` field, not the status).
/// `Failed` is reserved for executor-level failures that prevent the batch
/// from making progress (e.g. a malformed filter that the executor cannot
/// translate into a SQL query).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum BatchJobStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

impl BatchJobStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "Pending",
            Self::Running => "Running",
            Self::Completed => "Completed",
            Self::Failed => "Failed",
        }
    }

    /// Returns `true` if no further executor work is needed for this status.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed)
    }
}

impl FromStr for BatchJobStatus {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "Pending" => Ok(Self::Pending),
            "Running" => Ok(Self::Running),
            "Completed" => Ok(Self::Completed),
            "Failed" => Ok(Self::Failed),
            other => Err(format!("unknown batch job status '{other}'")),
        }
    }
}

/// Filter subset of the `GET /workflows` query contract, captured durably
/// alongside the batch job so the executor can rebuild it on resume.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct BatchFilter {
    /// Workflow execution states to match (e.g. `["RUNNING"]`).
    #[serde(default)]
    pub states: Vec<String>,
    /// Exact match on `workflow_name`.
    #[serde(default)]
    pub workflow_name: Option<String>,
    /// JSONB containment predicates against `search_attrs`. Each entry is a
    /// `{"key": "value"}` object; predicates AND together so repeated keys
    /// narrow rather than widen.
    #[serde(default)]
    pub search_attrs: Vec<Value>,
}

impl BatchFilter {
    /// Returns `true` when the filter has no narrowing criterion. Empty
    /// filters are rejected at the API layer to avoid the "cancel everything"
    /// foot-gun.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.states.is_empty() && self.workflow_name.is_none() && self.search_attrs.is_empty()
    }
}

/// Per-target failure recorded during batch execution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BatchTargetError {
    /// Execution id of the workflow that failed.
    pub execution_id: String,
    /// Human-readable error string.
    pub reason: String,
}

/// Default cap on concurrent per-target operations dispatched by the executor.
pub const DEFAULT_BATCH_CONCURRENCY: u32 = 32;

#[cfg(feature = "db")]
#[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
mod db {
    use super::{
        BatchAction, BatchFilter, BatchJobStatus, BatchTargetError, DEFAULT_BATCH_CONCURRENCY,
    };
    use chrono::{DateTime, Utc};
    use diesel::ExpressionMethods;
    use diesel::OptionalExtension;
    use diesel::QueryDsl;
    use diesel::SelectableHelper;
    use diesel_async::AsyncPgConnection;
    use diesel_async::RunQueryDsl;
    use serde::Serialize;
    use serde_json::Value;
    use uuid::Uuid;

    use crate::error::{HarvestError, HarvestResult, database_error};
    use crate::models::{BatchJob, NewBatchJob};
    use crate::schema::harvest_batch_jobs;

    /// Submission request resolved into the row we want in the database.
    ///
    /// Idempotency: if a row with the same `idempotency_key` already exists we
    /// return that row's id rather than starting a duplicate batch. The key
    /// is supplied by the caller so the same client retry deterministically
    /// resolves to the same job, preventing the "double-cancel" foot-gun.
    #[derive(Debug, Clone)]
    pub struct BatchSubmission {
        pub action: BatchAction,
        pub filter: BatchFilter,
        pub signal_name: Option<String>,
        pub signal_payload: Option<Value>,
        pub idempotency_key: Option<String>,
        pub created_by: Option<String>,
    }

    /// Insert a new batch job row, or return the existing id when an
    /// idempotent retry collides with a prior submission.
    pub async fn submit_batch_job(
        conn: &mut AsyncPgConnection,
        request: BatchSubmission,
    ) -> HarvestResult<Uuid> {
        if request.action == BatchAction::Signal && request.signal_name.is_none() {
            return Err(HarvestError::Config(
                "Signal batch action requires signal_name".to_string(),
            ));
        }
        if request.filter.is_empty() {
            return Err(HarvestError::Config(
                "batch filter must specify at least one criterion: state, workflow_name, or search_attrs".to_string(),
            ));
        }

        let id = Uuid::new_v4();
        let filter_value = serde_json::to_value(&request.filter).map_err(HarvestError::from)?;
        let new_row = NewBatchJob {
            id,
            action: request.action.as_str(),
            filter: filter_value,
            signal_name: request.signal_name.as_deref(),
            signal_payload: request.signal_payload.clone(),
            status: BatchJobStatus::Pending.as_str(),
            idempotency_key: request.idempotency_key.as_deref(),
            created_by: request.created_by.as_deref(),
        };
        match diesel::insert_into(harvest_batch_jobs::table)
            .values(&new_row)
            .returning(harvest_batch_jobs::id)
            .get_result::<Uuid>(conn)
            .await
        {
            Ok(inserted_id) => Ok(inserted_id),
            Err(diesel::result::Error::DatabaseError(
                diesel::result::DatabaseErrorKind::UniqueViolation,
                _,
            )) => {
                // A concurrent submission with the same idempotency_key raced
                // ahead of our insert. Return the id of the row that won.
                harvest_batch_jobs::table
                    .filter(
                        harvest_batch_jobs::idempotency_key
                            .eq(request.idempotency_key.as_deref().unwrap_or("")),
                    )
                    .select(harvest_batch_jobs::id)
                    .first::<Uuid>(conn)
                    .await
                    .map_err(database_error)
            }
            Err(e) => Err(database_error(e)),
        }
    }

    /// Filters accepted by the management list endpoint.
    #[derive(Debug, Default, Clone)]
    pub struct ListFilters {
        pub status: Option<BatchJobStatus>,
        pub action: Option<BatchAction>,
        pub limit: i64,
    }

    pub async fn get_batch_job(
        conn: &mut AsyncPgConnection,
        id: Uuid,
    ) -> HarvestResult<Option<BatchJob>> {
        harvest_batch_jobs::table
            .find(id)
            .select(BatchJob::as_select())
            .first(conn)
            .await
            .optional()
            .map_err(database_error)
    }

    pub async fn list_batch_jobs(
        conn: &mut AsyncPgConnection,
        filters: &ListFilters,
    ) -> HarvestResult<Vec<BatchJob>> {
        let mut query = harvest_batch_jobs::table.into_boxed();
        if let Some(status) = filters.status {
            query = query.filter(harvest_batch_jobs::status.eq(status.as_str()));
        }
        if let Some(action) = filters.action {
            query = query.filter(harvest_batch_jobs::action.eq(action.as_str()));
        }
        let limit = filters.limit.clamp(1, 200);
        query
            .order(harvest_batch_jobs::created_at.desc())
            .limit(limit)
            .select(BatchJob::as_select())
            .load(conn)
            .await
            .map_err(database_error)
    }

    /// Mark a batch job `Running` and record start time.
    pub async fn mark_running(
        conn: &mut AsyncPgConnection,
        id: Uuid,
        total: i64,
    ) -> HarvestResult<()> {
        diesel::update(harvest_batch_jobs::table.find(id))
            .filter(harvest_batch_jobs::status.eq(BatchJobStatus::Pending.as_str()))
            .set((
                harvest_batch_jobs::status.eq(BatchJobStatus::Running.as_str()),
                harvest_batch_jobs::total.eq(total),
                harvest_batch_jobs::started_at.eq(Some(Utc::now())),
                harvest_batch_jobs::updated_at.eq(Utc::now()),
            ))
            .execute(conn)
            .await
            .map_err(database_error)?;
        Ok(())
    }

    /// Lease window after which an in-progress batch is considered abandoned
    /// and another worker may claim it. `record_progress` and `try_claim_job`
    /// both refresh `updated_at`, so a healthy worker keeps the lease alive.
    pub const BATCH_JOB_LEASE_SECS: i64 = 60;

    /// Atomically claim a batch job for exclusive processing by this worker.
    ///
    /// Returns `true` when this caller acquired the claim and may proceed,
    /// `false` when another worker already owns it (skip silently). Uses a
    /// single conditional UPDATE with `PostgreSQL`'s row-level write lock so
    /// two concurrent runtimes cannot both claim the same row: a `Pending`
    /// row matches once and transitions to `Running`; a `Running` row only
    /// matches when its `updated_at` is older than the lease window (the
    /// previous owner is presumed dead). When transitioning from `Pending`
    /// the `total` is persisted atomically with the status change, so the
    /// claim is the single durable write that begins a job.
    pub async fn try_claim_job(
        conn: &mut AsyncPgConnection,
        job_id: Uuid,
        total: i64,
    ) -> HarvestResult<bool> {
        let lease_threshold = Utc::now() - chrono::Duration::seconds(BATCH_JOB_LEASE_SECS);
        let claimed: Option<Uuid> = diesel::sql_query(
            "UPDATE harvest_batch_jobs \
             SET status = 'Running', \
                 total = CASE WHEN status = 'Pending' THEN $3 ELSE total END, \
                 started_at = COALESCE(started_at, now()), \
                 updated_at = now() \
             WHERE id = $1 \
               AND (status = 'Pending' \
                    OR (status = 'Running' AND updated_at < $2)) \
             RETURNING id",
        )
        .bind::<diesel::sql_types::Uuid, _>(job_id)
        .bind::<diesel::sql_types::Timestamptz, _>(lease_threshold)
        .bind::<diesel::sql_types::BigInt, _>(total)
        .get_result::<ClaimedId>(conn)
        .await
        .optional()
        .map_err(database_error)?
        .map(|c| c.id);
        Ok(claimed.is_some())
    }

    #[derive(diesel::QueryableByName)]
    struct ClaimedId {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: Uuid,
    }

    /// Bump completed/failed counters, append per-target errors, and record
    /// the dispatched execution ids atomically.
    ///
    /// The dispatched ids are the durable exclusion set used on resume to
    /// skip already-processed targets regardless of their current workflow
    /// state. All four column updates land in a single SQL UPDATE so a crash
    /// cannot persist counters without the matching `processed_ids` append —
    /// otherwise a resumed tick would re-dispatch already-counted targets.
    pub async fn record_progress(
        conn: &mut AsyncPgConnection,
        id: Uuid,
        delta_completed: i64,
        delta_failed: i64,
        new_errors: &[BatchTargetError],
        dispatched_ids: &[Uuid],
    ) -> HarvestResult<()> {
        use diesel::dsl::sql;
        use diesel::sql_types::Jsonb;

        let errors_value = if new_errors.is_empty() {
            Value::Array(Vec::new())
        } else {
            serde_json::to_value(new_errors).map_err(HarvestError::from)?
        };
        let ids_value = if dispatched_ids.is_empty() {
            Value::Array(Vec::new())
        } else {
            serde_json::to_value(dispatched_ids).map_err(HarvestError::from)?
        };

        // Single UPDATE = single transaction = atomic. Appending an empty
        // JSON array via `||` is a no-op, so it's safe to always include the
        // errors and processed_ids set clauses regardless of input length.
        diesel::update(harvest_batch_jobs::table.find(id))
            .set((
                harvest_batch_jobs::completed.eq(harvest_batch_jobs::completed + delta_completed),
                harvest_batch_jobs::failed.eq(harvest_batch_jobs::failed + delta_failed),
                harvest_batch_jobs::updated_at.eq(Utc::now()),
                harvest_batch_jobs::errors
                    .eq(sql::<Jsonb>("errors || ").bind::<Jsonb, _>(errors_value)),
                harvest_batch_jobs::processed_ids
                    .eq(sql::<Jsonb>("processed_ids || ").bind::<Jsonb, _>(ids_value)),
            ))
            .execute(conn)
            .await
            .map_err(database_error)?;

        Ok(())
    }

    pub async fn mark_completed(conn: &mut AsyncPgConnection, id: Uuid) -> HarvestResult<()> {
        diesel::update(harvest_batch_jobs::table.find(id))
            .set((
                harvest_batch_jobs::status.eq(BatchJobStatus::Completed.as_str()),
                harvest_batch_jobs::completed_at.eq(Some(Utc::now())),
                harvest_batch_jobs::updated_at.eq(Utc::now()),
            ))
            .execute(conn)
            .await
            .map_err(database_error)?;
        Ok(())
    }

    /// Reload pending and running jobs across one shard. Used by the executor
    /// loop on startup and on each tick.
    pub async fn open_jobs(conn: &mut AsyncPgConnection) -> HarvestResult<Vec<BatchJob>> {
        harvest_batch_jobs::table
            .filter(harvest_batch_jobs::status.eq_any(vec![
                BatchJobStatus::Pending.as_str(),
                BatchJobStatus::Running.as_str(),
            ]))
            .order(harvest_batch_jobs::created_at.asc())
            .select(BatchJob::as_select())
            .load(conn)
            .await
            .map_err(database_error)
    }

    /// Convenience: shape returned by the management list/get endpoints.
    /// Mirrors the issue's response contract:
    /// `{ id, action, filter, status, total, completed, failed, started_at, completed_at, errors[] }`.
    #[derive(Debug, Clone, Serialize)]
    pub struct BatchJobView {
        pub id: String,
        pub action: String,
        pub filter: Value,
        pub signal_name: Option<String>,
        pub status: String,
        pub total: i64,
        pub completed: i64,
        pub failed: i64,
        pub started_at: Option<DateTime<Utc>>,
        pub completed_at: Option<DateTime<Utc>>,
        pub errors: Vec<BatchTargetError>,
        pub created_at: DateTime<Utc>,
        pub created_by: Option<String>,
    }

    impl BatchJobView {
        #[must_use]
        pub fn from_row(row: BatchJob) -> Self {
            let errors: Vec<BatchTargetError> =
                serde_json::from_value(row.errors.clone()).unwrap_or_default();
            Self {
                id: row.id.to_string(),
                action: row.action,
                filter: row.filter,
                signal_name: row.signal_name,
                status: row.status,
                total: row.total,
                completed: row.completed,
                failed: row.failed,
                started_at: row.started_at,
                completed_at: row.completed_at,
                errors,
                created_at: row.created_at,
                created_by: row.created_by,
            }
        }
    }

    use crate::execution::{cancel_workflow_execution, terminate_workflow_execution};
    use crate::models::WorkflowExecution;
    use crate::schema::harvest_workflow_executions;
    use crate::shard::ShardedDbPool;
    use crate::signal;
    use crate::telemetry::{MetricsRecorder, NoOpMetrics};
    use crate::types::ExecutionId;
    use crate::worker::DbPool;
    use futures::stream::{FuturesUnordered, StreamExt};
    use std::sync::Arc;

    /// Knobs the executor uses to bound its blast radius on the shared task
    /// queue and event store.
    #[derive(Clone)]
    pub struct BatchExecutorConfig {
        /// Cap on concurrent per-target operations dispatched in a single
        /// executor tick. Keeps a 10k-target batch from monopolising the
        /// connection pool — the issue's success metric calls for no more
        /// than 10% p99 regression on unrelated workflows.
        pub concurrency: u32,
        pub metrics: Arc<dyn MetricsRecorder>,
    }

    impl std::fmt::Debug for BatchExecutorConfig {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("BatchExecutorConfig")
                .field("concurrency", &self.concurrency)
                .finish_non_exhaustive()
        }
    }

    impl Default for BatchExecutorConfig {
        fn default() -> Self {
            Self {
                concurrency: DEFAULT_BATCH_CONCURRENCY,
                metrics: Arc::new(NoOpMetrics),
            }
        }
    }

    /// Resolve a batch filter against `harvest_workflow_executions` on a
    /// single shard. Mirrors `load_workflows` in the plugin's API module but
    /// returns just the IDs the executor needs to act on, and intentionally
    /// drops the per-call limit so a 10k batch can drain over multiple ticks.
    ///
    /// `action` is consulted only to pick the default state filter when the
    /// caller didn't supply one: Cancel/Signal default to the active states
    /// `RUNNING`/`PAUSED` (terminal rows are no-ops, and a paused execution is
    /// still active — cancel accepts it and a signal queues for delivery on
    /// resume); Terminate defaults to "every non-terminal-sealed state"
    /// (excludes `CANCELLED` and `TERMINATED`) because its whole purpose is to
    /// seal stuck live rows as `TERMINATED` (already-terminal rows are
    /// idempotent no-ops).
    async fn resolve_targets_on_shard(
        conn: &mut AsyncPgConnection,
        action: BatchAction,
        filter: &BatchFilter,
    ) -> HarvestResult<Vec<ExecutionId>> {
        use diesel::dsl::sql;
        use diesel::sql_types::{Bool, Jsonb};

        let mut query = harvest_workflow_executions::table.into_boxed();
        if filter.states.is_empty() {
            match action {
                BatchAction::Terminate => {
                    // Terminate seals live runs as TERMINATED; exclude both
                    // already-sealed terminal states so a re-run never
                    // re-selects rows this batch already finalized (#504).
                    query = query.filter(
                        harvest_workflow_executions::state.ne_all(["CANCELLED", "TERMINATED"]),
                    );
                }
                BatchAction::Cancel | BatchAction::Signal => {
                    query = query
                        .filter(harvest_workflow_executions::state.eq_any(["RUNNING", "PAUSED"]));
                }
            }
        } else {
            query = query.filter(harvest_workflow_executions::state.eq_any(&filter.states));
        }
        if let Some(name) = &filter.workflow_name {
            query = query.filter(harvest_workflow_executions::workflow_name.eq(name.clone()));
        }
        for predicate in &filter.search_attrs {
            query =
                query.filter(sql::<Bool>("search_attrs @> ").bind::<Jsonb, _>(predicate.clone()));
        }
        let rows: Vec<WorkflowExecution> = query
            .select(WorkflowExecution::as_select())
            .load(conn)
            .await
            .map_err(database_error)?;
        Ok(rows
            .into_iter()
            .map(|row| ExecutionId::from_uuid(row.id))
            .collect())
    }

    /// Dispatch one per-target action. Returns Ok(()) on success (including
    /// idempotent re-cancel), Err(reason) on per-target failure.
    async fn dispatch_target(
        conn: &mut AsyncPgConnection,
        action: BatchAction,
        signal_name: Option<&str>,
        signal_payload: Option<&Value>,
        target: ExecutionId,
        metrics: &dyn MetricsRecorder,
    ) -> Result<(), String> {
        match action {
            BatchAction::Cancel => {
                cancel_workflow_execution(conn, target, "batch cancel requested", metrics)
                    .await
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            }
            BatchAction::Terminate => {
                // Hard finalize: seals a live run as TERMINATED. Idempotent
                // no-op against any already-terminal state. Cancel would error
                // on those — Terminate is the operator escape hatch.
                terminate_workflow_execution(conn, target, "batch terminate requested", metrics)
                    .await
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            }
            BatchAction::Signal => {
                let name = signal_name.ok_or_else(|| {
                    "Signal action missing signal_name on batch job row".to_string()
                })?;
                let payload = signal_payload.cloned().unwrap_or(Value::Null);
                signal::send_signal(conn, target, name, payload)
                    .await
                    .map_err(|e| e.to_string())
            }
        }
    }

    /// Drive every open batch job to terminal status across all shards.
    ///
    /// One tick is sufficient for a 1k-target batch on a laptop-class
    /// Postgres; the spawned executor task in the plugin re-enters this loop
    /// on each scheduler interval so longer batches drain over multiple
    /// ticks without holding open transactions.
    pub async fn run_executor_once(
        pool: &ShardedDbPool,
        config: &BatchExecutorConfig,
    ) -> HarvestResult<()> {
        // Discover open jobs from every shard. The same job_id may live on
        // every shard's workflow table; the row itself is per-shard so we
        // process each shard's view independently and merge counters via
        // `record_progress` against the *default* shard's row.
        for (_shard, shard_pool) in pool.iter_shards() {
            let mut conn = shard_pool
                .get()
                .await
                .map_err(|e| HarvestError::Database(e.to_string()))?;
            let jobs = open_jobs(&mut conn).await?;
            for job in jobs {
                process_job(pool, shard_pool, job, config).await?;
            }
            // After processing this shard's view we move on; the same job
            // surface on other shards will be picked up in their iteration.
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn process_job(
        pool: &ShardedDbPool,
        owning_shard_pool: &DbPool,
        job: BatchJob,
        config: &BatchExecutorConfig,
    ) -> HarvestResult<()> {
        let action: BatchAction = match job.action.parse() {
            Ok(a) => a,
            Err(reason) => {
                tracing::warn!(job_id = %job.id, reason, "batch job has unknown action; failing");
                let mut conn = owning_shard_pool
                    .get()
                    .await
                    .map_err(|e| HarvestError::Database(e.to_string()))?;
                let _ = mark_failed(&mut conn, job.id, &reason).await;
                return Ok(());
            }
        };
        let filter: BatchFilter = match serde_json::from_value(job.filter.clone()) {
            Ok(f) => f,
            Err(error) => {
                tracing::warn!(job_id = %job.id, %error, "batch job filter is malformed");
                let mut conn = owning_shard_pool
                    .get()
                    .await
                    .map_err(|e| HarvestError::Database(e.to_string()))?;
                let _ = mark_failed(&mut conn, job.id, &error.to_string()).await;
                return Ok(());
            }
        };

        // Walk every shard, collect targets, dispatch with bounded fan-out.
        // Bolt: Pre-calculate total shards to hint the initial capacity of `all_targets`.
        // While we don't know exactly how many executions exist on each shard,
        // pre-allocating an estimated batch size prevents continuous reallocations
        // when concatenating results from 256 default shards.
        let mut all_targets: Vec<ExecutionId> = Vec::with_capacity(pool.iter_shards().count() * 10);
        for (_, shard_pool) in pool.iter_shards() {
            let mut conn = shard_pool
                .get()
                .await
                .map_err(|e| HarvestError::Database(e.to_string()))?;
            let mut targets = resolve_targets_on_shard(&mut conn, action, &filter).await?;
            all_targets.append(&mut targets);
        }

        // Atomic lease claim with total: a Pending row transitions to Running
        // and persists `total` in one write; a Running row with an expired
        // lease is reclaimed without touching `total`. If another worker
        // owns the lease, skip silently. record_progress runs on the owning
        // shard (where the job row lives).
        let mut owning_conn = owning_shard_pool
            .get()
            .await
            .map_err(|e| HarvestError::Database(e.to_string()))?;
        let total = i64::try_from(all_targets.len()).unwrap_or(i64::MAX);
        if !try_claim_job(&mut owning_conn, job.id, total).await? {
            tracing::debug!(
                job_id = %job.id,
                "batch job is owned by another worker; skipping"
            );
            return Ok(());
        }

        // Build the durable exclusion set from prior ticks. Resume-by-identity:
        // a target whose UUID is already in `processed_ids` was dispatched on
        // a previous tick (success or failure was already counted), so skip it
        // regardless of its current workflow state. This is the correct fix
        // for Signal — signaled workflows stay RUNNING and would otherwise be
        // re-dispatched, and an offset-based cursor is unsafe when the RUNNING
        // set drains naturally during a recovery window.
        let processed: std::collections::HashSet<Uuid> =
            serde_json::from_value::<Vec<String>>(job.processed_ids.clone())
                .unwrap_or_default()
                .into_iter()
                .filter_map(|s| Uuid::parse_str(&s).ok())
                .collect();
        let targets_to_dispatch: Vec<_> = all_targets
            .into_iter()
            .filter(|t| !processed.contains(&t.as_uuid()))
            .collect();

        let signal_name = job.signal_name.clone();
        let signal_payload = job.signal_payload.clone();
        let concurrency = usize::try_from(config.concurrency.max(1)).unwrap_or(1);
        let metrics = Arc::clone(&config.metrics);

        // Dispatch in chunks of `concurrency` to bound in-flight ops.
        for chunk in targets_to_dispatch.chunks(concurrency) {
            let mut tasks: FuturesUnordered<_> = FuturesUnordered::new();
            for target in chunk.iter().copied() {
                let pool_for_target = pool.pool_for_execution(target).clone();
                let signal_name = signal_name.clone();
                let signal_payload = signal_payload.clone();
                let metrics = Arc::clone(&metrics);
                tasks.push(async move {
                    let mut conn = match pool_for_target.get().await {
                        Ok(c) => c,
                        Err(e) => return (target, Err(e.to_string())),
                    };
                    let result = dispatch_target(
                        &mut conn,
                        action,
                        signal_name.as_deref(),
                        signal_payload.as_ref(),
                        target,
                        metrics.as_ref(),
                    )
                    .await;
                    (target, result)
                });
            }
            let mut completed_delta = 0i64;
            let mut failed_delta = 0i64;
            let mut new_errors: Vec<BatchTargetError> = Vec::with_capacity(chunk.len());
            let mut dispatched_ids: Vec<Uuid> = Vec::with_capacity(chunk.len());
            while let Some((target, outcome)) = tasks.next().await {
                dispatched_ids.push(target.as_uuid());
                match outcome {
                    Ok(()) => completed_delta += 1,
                    Err(reason) => {
                        failed_delta += 1;
                        new_errors.push(BatchTargetError {
                            execution_id: target.to_string(),
                            reason,
                        });
                    }
                }
            }
            record_progress(
                &mut owning_conn,
                job.id,
                completed_delta,
                failed_delta,
                &new_errors,
                &dispatched_ids,
            )
            .await?;
        }

        mark_completed(&mut owning_conn, job.id).await?;
        Ok(())
    }

    pub async fn mark_failed(
        conn: &mut AsyncPgConnection,
        id: Uuid,
        reason: &str,
    ) -> HarvestResult<()> {
        diesel::update(harvest_batch_jobs::table.find(id))
            .set((
                harvest_batch_jobs::status.eq(BatchJobStatus::Failed.as_str()),
                harvest_batch_jobs::completed_at.eq(Some(Utc::now())),
                harvest_batch_jobs::updated_at.eq(Utc::now()),
                harvest_batch_jobs::errors.eq(serde_json::json!([{
                    "execution_id": "",
                    "reason": reason,
                }])),
            ))
            .execute(conn)
            .await
            .map_err(database_error)?;
        Ok(())
    }
}

#[cfg(feature = "db")]
pub use db::{
    BatchExecutorConfig, BatchJobView, BatchSubmission, ListFilters, get_batch_job,
    list_batch_jobs, mark_completed, mark_failed, mark_running, open_jobs, record_progress,
    run_executor_once, submit_batch_job,
};

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn batch_action_round_trips_through_string() {
        for action in [
            BatchAction::Cancel,
            BatchAction::Terminate,
            BatchAction::Signal,
        ] {
            let s = action.as_str();
            let parsed: BatchAction = s.parse().expect("known action must parse");
            assert_eq!(parsed, action);
        }
    }

    #[test]
    fn batch_action_rejects_unknown_string() {
        let err = BatchAction::from_str("Restart").expect_err("unknown action must fail");
        assert!(err.contains("unknown batch action"));
    }

    #[test]
    fn batch_action_serializes_pascal_case() {
        let value = serde_json::to_value(BatchAction::Cancel).unwrap();
        assert_eq!(value, json!("Cancel"));
        let deserialized: BatchAction = serde_json::from_value(json!("Signal")).unwrap();
        assert_eq!(deserialized, BatchAction::Signal);
    }

    #[test]
    fn batch_job_status_terminal_predicate() {
        assert!(!BatchJobStatus::Pending.is_terminal());
        assert!(!BatchJobStatus::Running.is_terminal());
        assert!(BatchJobStatus::Completed.is_terminal());
        assert!(BatchJobStatus::Failed.is_terminal());
    }

    #[test]
    fn batch_filter_is_empty_when_no_criteria() {
        let empty = BatchFilter::default();
        assert!(empty.is_empty());

        let with_state = BatchFilter {
            states: vec!["RUNNING".to_string()],
            ..BatchFilter::default()
        };
        assert!(!with_state.is_empty());

        let with_name = BatchFilter {
            workflow_name: Some("onboarding".to_string()),
            ..BatchFilter::default()
        };
        assert!(!with_name.is_empty());

        let with_attr = BatchFilter {
            search_attrs: vec![json!({"tenant": "acme"})],
            ..BatchFilter::default()
        };
        assert!(!with_attr.is_empty());
    }

    #[test]
    fn batch_target_error_round_trips_json() {
        let err = BatchTargetError {
            execution_id: "00000000-0000-4000-8000-000000000001".to_string(),
            reason: "workflow execution already terminal (COMPLETED)".to_string(),
        };
        let value = serde_json::to_value(&err).unwrap();
        let back: BatchTargetError = serde_json::from_value(value).unwrap();
        assert_eq!(back, err);
    }
}
