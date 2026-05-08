//! Management API audit trail (issue #158).
//!
//! Records who did what, when, and whether it succeeded for every high-impact
//! management mutation. Does not store raw workflow payloads, activity inputs,
//! or signal bodies — the audit trail is not a second PII store.
//!
//! ## Covered operations
//!
//! workflow.start, workflow.signal, workflow.cancel, workflow.reset,
//! dag.trigger, dag.patch, schedule.create, schedule.pause, schedule.resume,
//! schedule.delete, dlq.replay, dlq.replay.bulk, dlq.discard.bulk,
//! batch.submit, retention.run_now, external_activity.complete,
//! external_activity.fail.
//!
//! ## Explicitly excluded (never produce audit rows)
//!
//! Health checks, all list/get/query routes, worker heartbeats, activity
//! heartbeats, read-only UI page loads, worker fleet reads, and the audit
//! list endpoint itself. See [`crate::audit::EXCLUDED_ROUTES`] for the full list.

use chrono::{DateTime, Utc};
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use uuid::Uuid;

use crate::error::{HarvestResult, database_error};
use crate::models::{AuditRecord, NewAuditRecord};
use crate::schema::harvest_audit_log;

// ── Operation name constants ──────────────────────────────────────────────────

pub const OP_WORKFLOW_START: &str = "workflow.start";
pub const OP_WORKFLOW_SIGNAL: &str = "workflow.signal";
pub const OP_WORKFLOW_CANCEL: &str = "workflow.cancel";
pub const OP_WORKFLOW_RESET: &str = "workflow.reset";
pub const OP_DAG_TRIGGER: &str = "dag.trigger";
pub const OP_DAG_PATCH: &str = "dag.patch";
pub const OP_SCHEDULE_CREATE: &str = "schedule.create";
pub const OP_SCHEDULE_PAUSE: &str = "schedule.pause";
pub const OP_SCHEDULE_RESUME: &str = "schedule.resume";
pub const OP_SCHEDULE_DELETE: &str = "schedule.delete";
pub const OP_DLQ_REPLAY: &str = "dlq.replay";
pub const OP_DLQ_REPLAY_BULK: &str = "dlq.replay.bulk";
pub const OP_DLQ_DISCARD_BULK: &str = "dlq.discard.bulk";
pub const OP_BATCH_SUBMIT: &str = "batch.submit";
pub const OP_RETENTION_RUN_NOW: &str = "retention.run_now";
pub const OP_EXTERNAL_ACTIVITY_COMPLETE: &str = "external_activity.complete";
pub const OP_EXTERNAL_ACTIVITY_FAIL: &str = "external_activity.fail";
pub const OP_WORKER_DRAIN: &str = "worker.drain";

// ── Target type constants ─────────────────────────────────────────────────────

pub const TARGET_WORKFLOW: &str = "workflow";
pub const TARGET_DAG: &str = "dag";
pub const TARGET_SCHEDULE: &str = "schedule";
pub const TARGET_DEAD_LETTER: &str = "dead_letter";
pub const TARGET_BATCH: &str = "batch";
pub const TARGET_RETENTION: &str = "retention";
pub const TARGET_EXTERNAL_ACTIVITY: &str = "external_activity";
pub const TARGET_WORKER: &str = "worker";

// ── Status constants ──────────────────────────────────────────────────────────

pub const STATUS_SUCCEEDED: &str = "succeeded";
pub const STATUS_FAILED: &str = "failed";

// ── Source constants ──────────────────────────────────────────────────────────

pub const SOURCE_API: &str = "api";
pub const SOURCE_CLI: &str = "cli";
pub const SOURCE_UI: &str = "ui";

// ── HTTP header names ─────────────────────────────────────────────────────────

/// Embedder-supplied operator identity header.
///
/// Set this on outgoing requests from the CLI or UI to distinguish call
/// origins. When absent, records default to `"anonymous"` — only acceptable
/// for local/dev deployments.
pub const HEADER_ACTOR: &str = "x-harvest-actor";

/// Correlation header forwarded from the originating request.
pub const HEADER_REQUEST_ID: &str = "x-request-id";

/// Call-origin hint: `"api"` (default), `"cli"`, or `"ui"`.
pub const HEADER_SOURCE: &str = "x-harvest-source";

// ── Retention ─────────────────────────────────────────────────────────────────

/// Default audit record retention in days (90 days ≈ 3 months).
///
/// This is intentionally longer than most incident review windows. Operators
/// can override this via `HarvestApiState::set_audit_retention_days`.
pub const DEFAULT_AUDIT_RETENTION_DAYS: i64 = 90;

// ── Declarative route manifest ────────────────────────────────────────────────

/// Every operation name covered by the audit trail.
///
/// The coverage guard test (`audit_coverage_all_mutation_routes_declared`)
/// verifies that every entry in [`ALL_MUTATION_ROUTES`] that declares an
/// operation references a name in this slice.
pub const AUDITED_OPERATIONS: &[&str] = &[
    OP_WORKFLOW_START,
    OP_WORKFLOW_SIGNAL,
    OP_WORKFLOW_CANCEL,
    OP_WORKFLOW_RESET,
    OP_DAG_TRIGGER,
    OP_DAG_PATCH,
    OP_SCHEDULE_CREATE,
    OP_SCHEDULE_PAUSE,
    OP_SCHEDULE_RESUME,
    OP_SCHEDULE_DELETE,
    OP_DLQ_REPLAY,
    OP_DLQ_REPLAY_BULK,
    OP_DLQ_DISCARD_BULK,
    OP_BATCH_SUBMIT,
    OP_RETENTION_RUN_NOW,
    OP_EXTERNAL_ACTIVITY_COMPLETE,
    OP_EXTERNAL_ACTIVITY_FAIL,
    OP_WORKER_DRAIN,
];

/// Routes explicitly excluded from audit.
///
/// This slice documents the intentional exclusions. It is consumed by the
/// coverage guard test to ensure no route is accidentally omitted from either
/// [`ALL_MUTATION_ROUTES`] or this exclusion list.
pub const EXCLUDED_ROUTES: &[&str] = &[
    "GET /workflows",
    "GET /workflows/{id}",
    "GET /workflows/{id}/children",
    "GET /workflows/{id}/stack",
    "GET /workflows/{id}/query/{query_name}",
    "GET /workflows/{id}/update/{update_id}/result",
    // Updates are synchronous request/response, not tracked as operator
    // audit events in this slice; they appear in the workflow event history.
    "POST /workflows/{id}/update/{update_name}",
    "GET /dags",
    "GET /dags/{dag_name}/runs",
    "GET /dead-letters",
    "GET /health",
    "GET /admin/retention",
    "GET /admin/concurrency",
    "GET /admin/schedules",
    // Heartbeats are high-volume liveness pings, not operator mutations.
    "POST /activities/external/{token}/heartbeat",
    "GET /workers",
    "GET /workers/health",
    "GET /workers/{worker_id}",
    "GET /workers/drain-preview",
    "GET /batch-operations",
    "GET /batch-operations/{id}",
    // The audit list endpoint itself is read-only.
    "GET /admin/audit",
];

/// Declarative manifest of every route in `harvest_api_router`.
///
/// Each entry is `(route_template, Option<operation_name>)`:
/// - `Some(op)` — this route is audited under the named operation.
/// - `None` — this route is explicitly excluded from audit.
///
/// **When you add a new route to `harvest_api_router`, you MUST add an entry
/// here.** The coverage guard test (`audit_coverage_all_mutation_routes_declared`)
/// will fail if any expected mutation route is missing or declared as excluded.
pub const ALL_MUTATION_ROUTES: &[(&str, Option<&str>)] = &[
    // Workflow management
    ("GET /workflows", None),
    ("GET /workflows/{id}", None),
    ("GET /workflows/{id}/children", None),
    ("GET /workflows/{id}/stack", None),
    (
        "POST /workflows/{workflow_name}/start",
        Some(OP_WORKFLOW_START),
    ),
    ("POST /workflows/{id}/cancel", Some(OP_WORKFLOW_CANCEL)),
    ("POST /workflows/{id}/reset", Some(OP_WORKFLOW_RESET)),
    (
        "POST /workflows/{id}/signal/{signal_name}",
        Some(OP_WORKFLOW_SIGNAL),
    ),
    ("GET /workflows/{id}/query/{query_name}", None),
    ("POST /workflows/{id}/update/{update_name}", None),
    ("GET /workflows/{id}/update/{update_id}/result", None),
    // DAG management
    ("GET /dags", None),
    ("GET /dags/{dag_name}/runs", None),
    ("POST /dags/{dag_name}/trigger", Some(OP_DAG_TRIGGER)),
    ("PATCH /dags/{dag_name}", Some(OP_DAG_PATCH)),
    // Dead-letter queue
    ("GET /dead-letters", None),
    ("POST /dead-letters/replay", Some(OP_DLQ_REPLAY_BULK)),
    ("POST /dead-letters/discard", Some(OP_DLQ_DISCARD_BULK)),
    ("POST /dead-letters/{id}/replay", Some(OP_DLQ_REPLAY)),
    // Health / observability (read-only)
    ("GET /health", None),
    ("GET /admin/retention", None),
    ("POST /admin/retention/run-now", Some(OP_RETENTION_RUN_NOW)),
    ("GET /admin/concurrency", None),
    // Schedule management
    ("GET /admin/schedules", None),
    ("POST /admin/schedules/workflow", Some(OP_SCHEDULE_CREATE)),
    ("POST /admin/schedules/{id}/pause", Some(OP_SCHEDULE_PAUSE)),
    (
        "POST /admin/schedules/{id}/resume",
        Some(OP_SCHEDULE_RESUME),
    ),
    ("DELETE /admin/schedules/{id}", Some(OP_SCHEDULE_DELETE)),
    // External activity completion
    (
        "POST /activities/external/{token}/complete",
        Some(OP_EXTERNAL_ACTIVITY_COMPLETE),
    ),
    (
        "POST /activities/external/{token}/fail",
        Some(OP_EXTERNAL_ACTIVITY_FAIL),
    ),
    ("POST /activities/external/{token}/heartbeat", None),
    // Worker fleet
    ("GET /workers/health", None),
    ("GET /workers", None),
    ("GET /workers/{worker_id}", None),
    ("GET /workers/drain-preview", None),
    ("POST /workers/{worker_id}/drain", Some(OP_WORKER_DRAIN)),
    // Batch operations
    ("GET /batch-operations", None),
    ("POST /batch-operations", Some(OP_BATCH_SUBMIT)),
    ("GET /batch-operations/{id}", None),
    // Audit log (read-only)
    ("GET /admin/audit", None),
];

// ── Query filters ─────────────────────────────────────────────────────────────

/// Filters for `list_audit`.
#[derive(Debug, Clone)]
pub struct AuditFilters {
    pub actor: Option<String>,
    pub operation: Option<String>,
    pub target_type: Option<String>,
    pub target_id: Option<String>,
    pub status: Option<String>,
    pub since: Option<DateTime<Utc>>,
    pub before: Option<DateTime<Utc>>,
    /// Maximum number of records to return. Clamped to [1, 500].
    pub limit: i64,
}

impl AuditFilters {
    #[must_use]
    pub const fn default_limit() -> i64 {
        50
    }
}

impl Default for AuditFilters {
    fn default() -> Self {
        Self {
            actor: None,
            operation: None,
            target_type: None,
            target_id: None,
            status: None,
            since: None,
            before: None,
            limit: Self::default_limit(),
        }
    }
}

// ── Database operations ───────────────────────────────────────────────────────

/// Insert a single audit record. Returns the generated audit row id.
///
/// Called after every covered management mutation. For successful mutations,
/// the caller **must** ensure this returns `Ok` before reporting success to
/// the HTTP client — the audit record must be durable before the response is
/// sent.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] if the insert fails.
pub async fn insert_audit(
    conn: &mut AsyncPgConnection,
    record: &NewAuditRecord<'_>,
) -> HarvestResult<Uuid> {
    diesel::insert_into(harvest_audit_log::table)
        .values(record)
        .returning(harvest_audit_log::id)
        .get_result::<Uuid>(conn)
        .await
        .map_err(database_error)
}

/// List audit records matching the given filters, ordered by `occurred_at DESC`.
///
/// The `limit` in `filters` is clamped to [1, 500]. The caller is responsible
/// for merging and re-sorting results when aggregating across multiple shards.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] if the query fails.
pub async fn list_audit(
    conn: &mut AsyncPgConnection,
    filters: &AuditFilters,
) -> HarvestResult<Vec<AuditRecord>> {
    let limit = filters.limit.clamp(1, 500);

    let mut query = harvest_audit_log::table
        .into_boxed()
        .order(harvest_audit_log::occurred_at.desc())
        .limit(limit);

    if let Some(actor) = &filters.actor {
        query = query.filter(harvest_audit_log::actor.eq(actor.clone()));
    }
    if let Some(op) = &filters.operation {
        query = query.filter(harvest_audit_log::operation.eq(op.clone()));
    }
    if let Some(tt) = &filters.target_type {
        query = query.filter(harvest_audit_log::target_type.eq(tt.clone()));
    }
    if let Some(tid) = &filters.target_id {
        query = query.filter(harvest_audit_log::target_id.eq(tid.clone()));
    }
    if let Some(status) = &filters.status {
        query = query.filter(harvest_audit_log::status.eq(status.clone()));
    }
    if let Some(since) = filters.since {
        query = query.filter(harvest_audit_log::occurred_at.ge(since));
    }
    if let Some(before) = filters.before {
        query = query.filter(harvest_audit_log::occurred_at.lt(before));
    }

    query
        .select(AuditRecord::as_select())
        .load(conn)
        .await
        .map_err(database_error)
}

/// Delete audit records older than `retention_days` days.
///
/// Called by the retention subsystem on its configured cadence. Returns the
/// number of rows deleted.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] if the delete fails.
pub async fn purge_old_audit_records(
    conn: &mut AsyncPgConnection,
    retention_days: i64,
) -> HarvestResult<usize> {
    let cutoff = chrono::Utc::now() - chrono::Duration::days(retention_days);
    diesel::delete(harvest_audit_log::table.filter(harvest_audit_log::occurred_at.lt(cutoff)))
        .execute(conn)
        .await
        .map_err(database_error)
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_constants_are_distinct() {
        let mut seen = std::collections::HashSet::new();
        for op in AUDITED_OPERATIONS {
            assert!(seen.insert(*op), "duplicate operation: {op}");
        }
    }

    #[test]
    fn all_routes_in_manifest_reference_known_operations() {
        for (route, declared_op) in ALL_MUTATION_ROUTES {
            if let Some(op) = declared_op {
                assert!(
                    AUDITED_OPERATIONS.contains(op),
                    "route '{route}' declares operation '{op}' which is not in AUDITED_OPERATIONS"
                );
            }
        }
    }

    #[test]
    fn route_manifest_has_no_duplicates() {
        let mut seen = std::collections::HashSet::new();
        for (route, _) in ALL_MUTATION_ROUTES {
            assert!(seen.insert(*route), "duplicate route in manifest: {route}");
        }
    }

    #[test]
    fn audit_filters_default_limit_is_50() {
        assert_eq!(AuditFilters::default_limit(), 50);
    }

    #[test]
    fn audit_filters_default_uses_default_limit_not_zero() {
        // Derived Default would give limit=0, which list_audit clamps to 1.
        // The manual Default implementation must use default_limit() instead.
        assert_eq!(AuditFilters::default().limit, AuditFilters::default_limit());
    }

    #[test]
    fn status_constants_are_correct() {
        assert_eq!(STATUS_SUCCEEDED, "succeeded");
        assert_eq!(STATUS_FAILED, "failed");
    }

    #[test]
    fn source_constants_are_correct() {
        assert_eq!(SOURCE_API, "api");
        assert_eq!(SOURCE_CLI, "cli");
        assert_eq!(SOURCE_UI, "ui");
    }

    #[test]
    fn header_constants_are_lowercase() {
        assert_eq!(HEADER_ACTOR, "x-harvest-actor");
        assert_eq!(HEADER_REQUEST_ID, "x-request-id");
        assert_eq!(HEADER_SOURCE, "x-harvest-source");
    }

    #[test]
    fn default_retention_is_90_days() {
        assert_eq!(DEFAULT_AUDIT_RETENTION_DAYS, 90);
    }
}
