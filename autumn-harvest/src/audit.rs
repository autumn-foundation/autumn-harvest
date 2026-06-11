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

/// Audit operation: Started a new workflow execution.
pub const OP_WORKFLOW_START: &str = "workflow.start";
/// Audit operation: Signaled a running workflow execution.
pub const OP_WORKFLOW_SIGNAL: &str = "workflow.signal";
/// Audit operation: Atomic start-or-attach + signal (issue #244).
pub const OP_WORKFLOW_SIGNAL_WITH_START: &str = "workflow.signal_with_start";
/// Audit operation: Atomic start-or-attach + update admission (issue #479).
pub const OP_WORKFLOW_UPDATE_WITH_START: &str = "workflow.update_with_start";
/// Audit operation: Cancelled a workflow execution.
pub const OP_WORKFLOW_CANCEL: &str = "workflow.cancel";
/// Audit operation: Reset a workflow execution to a previous state.
pub const OP_WORKFLOW_RESET: &str = "workflow.reset";
/// Audit operation: Paused an individual workflow execution (issue #383).
pub const OP_WORKFLOW_PAUSE: &str = "workflow.pause";
/// Audit operation: Resumed a paused workflow execution (issue #383).
pub const OP_WORKFLOW_RESUME: &str = "workflow.resume";
/// Audit operation: Manually triggered a DAG execution.
pub const OP_DAG_TRIGGER: &str = "dag.trigger";
/// Audit operation: Retried a DAG run from a failed node (issue #366).
pub const OP_DAG_RETRY: &str = "dag.retry";
/// Audit operation: Applied a hot-patch to an active DAG.
pub const OP_DAG_PATCH: &str = "dag.patch";
/// Audit operation: Created a new workflow schedule.
pub const OP_SCHEDULE_CREATE: &str = "schedule.create";
/// Audit operation: Paused an active workflow schedule.
pub const OP_SCHEDULE_PAUSE: &str = "schedule.pause";
/// Audit operation: Resumed a paused workflow schedule.
pub const OP_SCHEDULE_RESUME: &str = "schedule.resume";
/// Audit operation: Deleted a workflow schedule.
pub const OP_SCHEDULE_DELETE: &str = "schedule.delete";
/// Audit operation: Triggered a backfill for a workflow schedule.
pub const OP_SCHEDULE_BACKFILL: &str = "schedule.backfill";
/// Audit operation: Triggered an immediate one-off run of a schedule (issue #343).
pub const OP_SCHEDULE_TRIGGER: &str = "schedule.trigger";
/// Audit operation: Replayed a dead-letter queue (DLQ) task.
pub const OP_DLQ_REPLAY: &str = "dlq.replay";
/// Audit operation: Bulk-replayed dead-letter queue (DLQ) tasks.
pub const OP_DLQ_REPLAY_BULK: &str = "dlq.replay.bulk";
/// Audit operation: Bulk-discarded dead-letter queue (DLQ) tasks.
pub const OP_DLQ_DISCARD_BULK: &str = "dlq.discard.bulk";
/// Audit operation: Submitted a batch processing job.
pub const OP_BATCH_SUBMIT: &str = "batch.submit";
/// Audit operation: Atomically started a batch of workflow executions (issue #357).
pub const OP_BATCH_START: &str = "batch.start";
/// Audit operation: Triggered a retention sweep manually.
pub const OP_RETENTION_RUN_NOW: &str = "retention.run_now";
/// Audit operation: Completed an external activity.
pub const OP_EXTERNAL_ACTIVITY_COMPLETE: &str = "external_activity.complete";
/// Audit operation: Failed an external activity.
pub const OP_EXTERNAL_ACTIVITY_FAIL: &str = "external_activity.fail";
/// Audit operation: Initiated draining of a worker fleet.
pub const OP_WORKER_DRAIN: &str = "worker.drain";
/// Audit operation: Overrode rate-limiting parameters.
pub const OP_RATE_LIMIT_OVERRIDE: &str = "rate_limit_override";
/// Audit operation: Opened an SSE execution event stream (issue #324).
pub const OP_EXECUTION_STREAM_OPEN: &str = "execution.stream.open";
/// Audit operation: Closed an SSE execution event stream (issue #324).
pub const OP_EXECUTION_STREAM_CLOSE: &str = "execution.stream.close";
/// Audit operation: Set the active build policy for a queue (issue #362).
pub const OP_BUILD_POLICY_SET: &str = "build_routing.policy.set";
/// Audit operation: Declared a build compatibility entry (issue #362).
pub const OP_BUILD_COMPAT_DECLARE: &str = "build_routing.compat.declare";
/// Audit operation: Revoked a build compatibility entry (issue #362).
pub const OP_BUILD_COMPAT_REVOKE: &str = "build_routing.compat.revoke";
/// Audit operation: Forced an activity circuit breaker open (issue #369).
pub const OP_CIRCUIT_FORCE_OPEN: &str = "circuit.force_open";
/// Audit operation: Forced an activity circuit breaker closed (issue #369).
pub const OP_CIRCUIT_FORCE_CLOSE: &str = "circuit.force_close";
/// Audit operation: Created an admission gate (issue #377).
pub const OP_GATE_CREATE: &str = "gate.create";
/// Audit operation: Lifted (removed) an admission gate (issue #377).
pub const OP_GATE_LIFT: &str = "gate.lift";

// ── Target type constants ─────────────────────────────────────────────────────

pub const TARGET_CIRCUIT: &str = "circuit";
/// Audit target type for admission gate operations (issue #377).
pub const TARGET_GATE: &str = "gate";
pub const TARGET_WORKFLOW: &str = "workflow";
pub const TARGET_DAG: &str = "dag";
pub const TARGET_SCHEDULE: &str = "schedule";
pub const TARGET_DEAD_LETTER: &str = "dead_letter";
pub const TARGET_BATCH: &str = "batch";
pub const TARGET_RETENTION: &str = "retention";
pub const TARGET_EXTERNAL_ACTIVITY: &str = "external_activity";
pub const TARGET_WORKER: &str = "worker";
pub const TARGET_RATE_LIMIT: &str = "rate_limit";
pub const TARGET_BUILD_ROUTING: &str = "build_routing";

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

// ── Security classification ───────────────────────────────────────────────────

/// Three-tier security classification for every Harvest management API route.
///
/// Embedders use this to decide which protection level applies to each route
/// category. The recommended production posture gates all three tiers behind
/// the host application's authentication middleware; the distinction matters
/// for graduated roll-out and for documenting intentional exposure choices.
///
/// See `docs/security-posture.md` for the full mounting recipe, CLI/token
/// semantics, and a production-readiness checklist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteClass {
    /// Always safe to expose without authentication.
    ///
    /// Currently only `GET /health`. Kubernetes liveness/readiness probes and
    /// load-balancer health checks commonly require this endpoint to be
    /// reachable without credentials. Exposing it is an explicit product
    /// decision, not an oversight.
    PublicSafe,

    /// Reads operator state but does not modify workflow execution.
    ///
    /// List, get, query, and export routes. These do not trigger side effects
    /// but may expose sensitive operational data (execution IDs, input/output
    /// payloads, schedule definitions). Protect these in production with the
    /// same middleware layer as mutating routes.
    ReadOnly,

    /// Modifies workflow execution or system configuration.
    ///
    /// Includes workflow start/signal/cancel/reset, DLQ replay/discard,
    /// schedule mutation, batch submission, retention run-now, external
    /// activity completion, and worker drain. Every route in this class that
    /// carries production risk is covered by the audit trail (`harvest_audit_log`).
    /// These routes MUST be protected by authentication middleware in any
    /// non-local deployment.
    Mutating,
}

/// Security classification for every route registered in `harvest_api_router`.
///
/// Each entry is `(route_template, RouteClass)`. The exhaustiveness guard test
/// (`route_classification_covers_all_known_routes`) verifies that this slice
/// and [`ALL_MUTATION_ROUTES`] contain exactly the same route set, so adding a
/// new route to `harvest_api_router` without classifying it here causes a
/// compile-time-visible test failure.
///
/// **When you add a new route to `harvest_api_router`, you MUST add an entry
/// here AND in [`ALL_MUTATION_ROUTES`].**
pub const CLASSIFIED_ROUTES: &[(&str, RouteClass)] = &[
    // ── PublicSafe ── always safe to expose, even without auth ───────────────
    // Kubernetes liveness/readiness probes and load-balancer health checks
    // require /health to be reachable without credentials.
    ("GET /health", RouteClass::PublicSafe),
    // ── ReadOnly ── reads state, does not modify workflow execution ───────────
    ("GET /workflows", RouteClass::ReadOnly),
    ("GET /workflows/{id}", RouteClass::ReadOnly),
    ("GET /workflows/{id}/children", RouteClass::ReadOnly),
    ("GET /workflows/{id}/stack", RouteClass::ReadOnly),
    (
        "GET /workflows/{id}/query/{query_name}",
        RouteClass::ReadOnly,
    ),
    // POST query accepts typed args but never mutates workflow state.
    (
        "POST /workflows/{id}/query/{query_name}",
        RouteClass::ReadOnly,
    ),
    ("GET /workflows/{id}/queries", RouteClass::ReadOnly),
    (
        "GET /workflows/{id}/update/{update_id}/result",
        RouteClass::ReadOnly,
    ),
    ("GET /workflows/{id}/history/export", RouteClass::ReadOnly),
    ("GET /dags", RouteClass::ReadOnly),
    ("GET /dags/{dag_name}/runs", RouteClass::ReadOnly),
    ("GET /dead-letters", RouteClass::ReadOnly),
    ("GET /admin/preflight", RouteClass::ReadOnly),
    ("GET /admin/shards/health", RouteClass::ReadOnly),
    ("GET /admin/version-gates/usage", RouteClass::ReadOnly),
    (
        "GET /admin/version-gates/retirement-check",
        RouteClass::ReadOnly,
    ),
    ("GET /admin/retention", RouteClass::ReadOnly),
    ("GET /admin/concurrency", RouteClass::ReadOnly),
    ("GET /admin/history/exports", RouteClass::ReadOnly),
    ("GET /admin/external-handoffs", RouteClass::ReadOnly),
    ("GET /admin/external-handoffs/{token}", RouteClass::ReadOnly),
    // Admission gates (issue #377)
    ("GET /admin/gates", RouteClass::ReadOnly),
    ("POST /admin/gates", RouteClass::Mutating),
    ("DELETE /admin/gates/{id}", RouteClass::Mutating),
    ("GET /admin/schedules", RouteClass::ReadOnly),
    ("GET /admin/rate-limits", RouteClass::ReadOnly),
    ("GET /admin/audit", RouteClass::ReadOnly),
    ("GET /workers/health", RouteClass::ReadOnly),
    ("GET /workers/drain-preview", RouteClass::ReadOnly),
    ("GET /workers", RouteClass::ReadOnly),
    ("GET /workers/{worker_id}", RouteClass::ReadOnly),
    ("GET /batch-operations", RouteClass::ReadOnly),
    ("GET /batch-operations/{id}", RouteClass::ReadOnly),
    // SSE execution event stream (issue #324): read-only long-poll, never mutates state.
    (
        "GET /executions/{exec_id}/events/stream",
        RouteClass::ReadOnly,
    ),
    // ── Mutating ── modifies workflow execution or system configuration ───────
    // All of these are covered by the audit trail (harvest_audit_log) or are
    // explicitly listed in EXCLUDED_ROUTES with an audit disposition note.
    (
        "POST /workflows/{workflow_name}/start",
        RouteClass::Mutating,
    ),
    (
        "POST /workflows/{workflow_name}/signal-with-start",
        RouteClass::Mutating,
    ),
    ("POST /workflows/{id}/cancel", RouteClass::Mutating),
    ("POST /workflows/{id}/pause", RouteClass::Mutating),
    ("POST /workflows/{id}/resume", RouteClass::Mutating),
    ("POST /workflows/{id}/reset", RouteClass::Mutating),
    (
        "POST /workflows/{id}/signal/{signal_name}",
        RouteClass::Mutating,
    ),
    // Update appends UpdateAdmitted to history and wakes the workflow — mutating.
    // Audit disposition: excluded (synchronous RPC; the event history is the record).
    (
        "POST /workflows/{id}/update/{update_name}",
        RouteClass::Mutating,
    ),
    ("POST /dags/{dag_name}/trigger", RouteClass::Mutating),
    ("PATCH /dags/{dag_name}", RouteClass::Mutating),
    ("POST /dead-letters/replay", RouteClass::Mutating),
    ("POST /dead-letters/discard", RouteClass::Mutating),
    ("POST /dead-letters/{id}/replay", RouteClass::Mutating),
    ("POST /admin/retention/run-now", RouteClass::Mutating),
    ("POST /admin/schedules/workflow", RouteClass::Mutating),
    ("POST /admin/schedules/{id}/pause", RouteClass::Mutating),
    ("POST /admin/schedules/{id}/resume", RouteClass::Mutating),
    ("POST /admin/schedules/{id}/backfill", RouteClass::Mutating),
    ("POST /admin/schedules/{id}/trigger", RouteClass::Mutating),
    ("DELETE /admin/schedules/{id}", RouteClass::Mutating),
    // External activity completion — task-token callbacks from remote workers.
    (
        "POST /activities/external/{token}/complete",
        RouteClass::Mutating,
    ),
    (
        "POST /activities/external/{token}/fail",
        RouteClass::Mutating,
    ),
    // Heartbeat writes liveness state but is intentionally excluded from audit
    // (high-volume, not a control-plane mutation). See EXCLUDED_ROUTES.
    (
        "POST /activities/external/{token}/heartbeat",
        RouteClass::Mutating,
    ),
    ("POST /workers/{worker_id}/drain", RouteClass::Mutating),
    ("POST /admin/rate-limits/{key}", RouteClass::Mutating),
    ("POST /batch-operations", RouteClass::Mutating),
    // Batch workflow start (issue #357)
    ("POST /workflows/batch_start", RouteClass::Mutating),
    // Build routing management (issue #362)
    ("GET /admin/build-routing", RouteClass::ReadOnly),
    ("POST /admin/build-routing/policies", RouteClass::Mutating),
    ("GET /admin/build-routing/compat", RouteClass::ReadOnly),
    ("POST /admin/build-routing/compat", RouteClass::Mutating),
    (
        "DELETE /admin/build-routing/compat/{build_id}/{compat_with}",
        RouteClass::Mutating,
    ),
    // Retire is a read-only reachability check; no DB state is written.
    ("POST /admin/build-routing/retire", RouteClass::ReadOnly),
];

// ── Declarative route manifest ────────────────────────────────────────────────

/// Every operation name covered by the audit trail.
///
/// The coverage guard test (`audit_coverage_all_mutation_routes_declared`)
/// verifies that every entry in [`ALL_MUTATION_ROUTES`] that declares an
/// operation references a name in this slice.
pub const AUDITED_OPERATIONS: &[&str] = &[
    OP_WORKFLOW_START,
    OP_WORKFLOW_SIGNAL,
    OP_WORKFLOW_SIGNAL_WITH_START,
    OP_WORKFLOW_CANCEL,
    OP_WORKFLOW_PAUSE,
    OP_WORKFLOW_RESUME,
    OP_WORKFLOW_RESET,
    OP_DAG_TRIGGER,
    OP_DAG_PATCH,
    OP_SCHEDULE_CREATE,
    OP_SCHEDULE_PAUSE,
    OP_SCHEDULE_RESUME,
    OP_SCHEDULE_DELETE,
    OP_SCHEDULE_BACKFILL,
    OP_SCHEDULE_TRIGGER,
    OP_DLQ_REPLAY,
    OP_DLQ_REPLAY_BULK,
    OP_DLQ_DISCARD_BULK,
    OP_BATCH_SUBMIT,
    OP_BATCH_START,
    OP_RETENTION_RUN_NOW,
    OP_EXTERNAL_ACTIVITY_COMPLETE,
    OP_EXTERNAL_ACTIVITY_FAIL,
    OP_WORKER_DRAIN,
    OP_RATE_LIMIT_OVERRIDE,
    OP_BUILD_POLICY_SET,
    OP_BUILD_COMPAT_DECLARE,
    OP_BUILD_COMPAT_REVOKE,
    // Admission gates (issue #377)
    OP_GATE_CREATE,
    OP_GATE_LIFT,
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
    "POST /workflows/{id}/query/{query_name}",
    "GET /workflows/{id}/queries",
    "GET /workflows/{id}/update/{update_id}/result",
    // Updates are synchronous request/response, not tracked as operator
    // audit events in this slice; they appear in the workflow event history.
    "POST /workflows/{id}/update/{update_name}",
    "GET /workflows/{id}/history/export",
    "GET /dags",
    "GET /dags/{dag_name}/runs",
    "GET /dead-letters",
    "GET /health",
    "GET /admin/preflight",
    "GET /admin/shards/health",
    "GET /admin/version-gates/usage",
    "GET /admin/version-gates/retirement-check",
    "GET /admin/retention",
    "GET /admin/concurrency",
    "GET /admin/history/exports",
    "GET /admin/external-handoffs",
    "GET /admin/external-handoffs/{token}",
    "GET /admin/schedules",
    "GET /admin/rate-limits",
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
    // SSE stream is read-only; stream open/close are audited manually in the handler.
    "GET /executions/{exec_id}/events/stream",
    // Build routing reads and the retire safety check never write audit rows.
    "GET /admin/build-routing",
    "GET /admin/build-routing/compat",
    "POST /admin/build-routing/retire",
    // Admission gate list is read-only.
    "GET /admin/gates",
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
    (
        "POST /workflows/{workflow_name}/signal-with-start",
        Some(OP_WORKFLOW_SIGNAL_WITH_START),
    ),
    ("POST /workflows/{id}/cancel", Some(OP_WORKFLOW_CANCEL)),
    ("POST /workflows/{id}/pause", Some(OP_WORKFLOW_PAUSE)),
    ("POST /workflows/{id}/resume", Some(OP_WORKFLOW_RESUME)),
    ("POST /workflows/{id}/reset", Some(OP_WORKFLOW_RESET)),
    (
        "POST /workflows/{id}/signal/{signal_name}",
        Some(OP_WORKFLOW_SIGNAL),
    ),
    ("GET /workflows/{id}/query/{query_name}", None),
    ("POST /workflows/{id}/query/{query_name}", None),
    ("GET /workflows/{id}/queries", None),
    ("POST /workflows/{id}/update/{update_name}", None),
    ("GET /workflows/{id}/update/{update_id}/result", None),
    ("GET /workflows/{id}/history/export", None),
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
    ("GET /admin/preflight", None),
    ("GET /admin/shards/health", None),
    ("GET /admin/version-gates/usage", None),
    ("GET /admin/version-gates/retirement-check", None),
    ("GET /admin/retention", None),
    ("POST /admin/retention/run-now", Some(OP_RETENTION_RUN_NOW)),
    ("GET /admin/concurrency", None),
    ("GET /admin/history/exports", None),
    ("GET /admin/external-handoffs", None),
    ("GET /admin/external-handoffs/{token}", None),
    // Schedule management
    ("GET /admin/schedules", None),
    ("GET /admin/rate-limits", None),
    ("POST /admin/schedules/workflow", Some(OP_SCHEDULE_CREATE)),
    ("POST /admin/schedules/{id}/pause", Some(OP_SCHEDULE_PAUSE)),
    (
        "POST /admin/schedules/{id}/resume",
        Some(OP_SCHEDULE_RESUME),
    ),
    ("DELETE /admin/schedules/{id}", Some(OP_SCHEDULE_DELETE)),
    (
        "POST /admin/schedules/{id}/backfill",
        Some(OP_SCHEDULE_BACKFILL),
    ),
    (
        "POST /admin/schedules/{id}/trigger",
        Some(OP_SCHEDULE_TRIGGER),
    ),
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
    (
        "POST /admin/rate-limits/{key}",
        Some(OP_RATE_LIMIT_OVERRIDE),
    ),
    // Batch operations
    ("GET /batch-operations", None),
    ("POST /batch-operations", Some(OP_BATCH_SUBMIT)),
    ("GET /batch-operations/{id}", None),
    // Batch workflow start (issue #357)
    ("POST /workflows/batch_start", Some(OP_BATCH_START)),
    // Audit log (read-only)
    ("GET /admin/audit", None),
    // SSE execution event stream (issue #324): read-only; open/close audited in handler.
    ("GET /executions/{exec_id}/events/stream", None),
    // Build routing management (issue #362)
    ("GET /admin/build-routing", None),
    (
        "POST /admin/build-routing/policies",
        Some(OP_BUILD_POLICY_SET),
    ),
    ("GET /admin/build-routing/compat", None),
    (
        "POST /admin/build-routing/compat",
        Some(OP_BUILD_COMPAT_DECLARE),
    ),
    (
        "DELETE /admin/build-routing/compat/{build_id}/{compat_with}",
        Some(OP_BUILD_COMPAT_REVOKE),
    ),
    // retire is a read-only safety check — no state is mutated.
    ("POST /admin/build-routing/retire", None),
    // Admission gates (issue #377)
    ("GET /admin/gates", None),
    ("POST /admin/gates", Some(OP_GATE_CREATE)),
    ("DELETE /admin/gates/{id}", Some(OP_GATE_LIFT)),
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

    // ── Route classification exhaustiveness guards ────────────────────────────
    //
    // These tests enforce that CLASSIFIED_ROUTES and ALL_MUTATION_ROUTES stay
    // in sync. Adding a route to harvest_api_router without classifying it
    // causes one of these tests to fail.

    #[test]
    fn route_classification_covers_all_known_routes() {
        let classified: std::collections::HashSet<&str> =
            CLASSIFIED_ROUTES.iter().map(|(r, _)| *r).collect();

        for (route, _) in ALL_MUTATION_ROUTES {
            assert!(
                classified.contains(route),
                "route '{route}' is in ALL_MUTATION_ROUTES but has no entry in \
                 CLASSIFIED_ROUTES — add it with the correct RouteClass"
            );
        }
    }

    #[test]
    fn all_classified_routes_are_in_route_manifest() {
        let manifest: std::collections::HashSet<&str> =
            ALL_MUTATION_ROUTES.iter().map(|(r, _)| *r).collect();

        for (route, _) in CLASSIFIED_ROUTES {
            assert!(
                manifest.contains(route),
                "route '{route}' is in CLASSIFIED_ROUTES but missing from \
                 ALL_MUTATION_ROUTES — add it to both slices"
            );
        }
    }

    #[test]
    fn classified_routes_has_no_duplicates() {
        let mut seen = std::collections::HashSet::new();
        for (route, _) in CLASSIFIED_ROUTES {
            assert!(
                seen.insert(*route),
                "duplicate route in CLASSIFIED_ROUTES: '{route}'"
            );
        }
    }

    #[test]
    fn classified_routes_count_matches_route_manifest() {
        assert_eq!(
            CLASSIFIED_ROUTES.len(),
            ALL_MUTATION_ROUTES.len(),
            "CLASSIFIED_ROUTES has {} entries but ALL_MUTATION_ROUTES has {} — \
             the two slices must cover exactly the same route set",
            CLASSIFIED_ROUTES.len(),
            ALL_MUTATION_ROUTES.len()
        );
    }

    #[test]
    fn public_safe_routes_are_excluded_from_audit() {
        let excluded: std::collections::HashSet<&str> = EXCLUDED_ROUTES.iter().copied().collect();
        for (route, class) in CLASSIFIED_ROUTES {
            if *class == RouteClass::PublicSafe {
                assert!(
                    excluded.contains(route),
                    "PublicSafe route '{route}' should be in EXCLUDED_ROUTES \
                     since it is never a mutation that needs auditing"
                );
            }
        }
    }

    #[test]
    fn mutating_routes_are_audited_or_explicitly_excluded() {
        let audited: std::collections::HashSet<&str> = ALL_MUTATION_ROUTES
            .iter()
            .filter_map(|(r, op)| op.map(|_| *r))
            .collect();
        let excluded: std::collections::HashSet<&str> = EXCLUDED_ROUTES.iter().copied().collect();

        for (route, class) in CLASSIFIED_ROUTES {
            if *class == RouteClass::Mutating {
                assert!(
                    audited.contains(route) || excluded.contains(route),
                    "Mutating route '{route}' is neither audited (Some(op) in \
                     ALL_MUTATION_ROUTES) nor listed in EXCLUDED_ROUTES — \
                     explicitly declare its audit disposition"
                );
            }
        }
    }
}
