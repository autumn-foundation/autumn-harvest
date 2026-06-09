#![cfg(feature = "db")]

//! Integration tests for the audit trail (issue #158).
//!
//! Spins up a real Postgres container, runs the full migration stack, and
//! exercises `autumn_harvest::audit` insert/list/filter behaviour.

use autumn_harvest::audit::{
    self, AuditFilters, OP_WORKFLOW_CANCEL, OP_WORKFLOW_START, STATUS_FAILED, STATUS_SUCCEEDED,
    TARGET_SCHEDULE, TARGET_WORKFLOW,
};
use autumn_harvest::models::NewAuditRecord;
use autumn_harvest::schema::harvest_audit_log;

use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel_async::AsyncConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

// ── test helpers ─────────────────────────────────────────────────────────────

const INIT_SQL: &str = concat!(
    include_str!("../migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!("../migrations/20260424000001_harvest_trace_context/up.sql"),
    "\n",
    include_str!("../migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../migrations/20260427000000_harvest_continue_as_new/up.sql"),
    "\n",
    include_str!("../migrations/20260429000000_harvest_concurrency_key/up.sql"),
    "\n",
    include_str!("../migrations/20260430000000_harvest_workflow_schedules/up.sql"),
    "\n",
    include_str!("../migrations/20260430000001_harvest_external_tasks/up.sql"),
    "\n",
    include_str!("../migrations/20260508000000_harvest_external_task_updated_at/up.sql"),
    "\n",
    include_str!("../migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../migrations/20260501010000_harvest_batch_jobs/up.sql"),
    "\n",
    include_str!("../migrations/20260501020000_harvest_batch_processed_ids/up.sql"),
    "\n",
    include_str!("../migrations/20260503000000_harvest_workflow_reset/up.sql"),
    "\n",
    include_str!("../migrations/20260504000000_harvest_workflow_parent_children/up.sql"),
    "\n",
    include_str!("../migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!("../migrations/20260513000000_harvest_schedule_pause_metadata/up.sql"),
    "\n",
    include_str!("../migrations/20260518000000_harvest_signal_idempotency/up.sql"),
    "\n",
    include_str!("../migrations/20260518000001_harvest_workflow_execution_timeout/up.sql"),
    "\n",
    include_str!("../migrations/20260519000000_harvest_calendar_awareness/up.sql"),
    "\n",
    include_str!("../migrations/20260522000000_harvest_schedule_decisions/up.sql"),
    "\n",
    include_str!("../migrations/20260522000001_harvest_rate_limiting/up.sql"),
    "\n",
    include_str!("../migrations/20260526000001_harvest_parent_close_policy/up.sql"),
    "\n",
    include_str!("../migrations/20260530000000_harvest_schedule_ha_claim/up.sql"),
    "\n",
    include_str!("../migrations/20260601000000_harvest_schedule_auto_pause/up.sql"),
    "\n",
    include_str!("../migrations/20260601000001_harvest_poison_pill_strikes/up.sql"),
    "\n",
    include_str!("../migrations/20260601000002_harvest_ownership_metadata/up.sql"),
    "\n",
    include_str!("../migrations/20260603000000_harvest_completion_triggers/up.sql"),
    include_str!("../migrations/20260605000000_harvest_admission_gates/up.sql"),
    include_str!("../migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"),
    include_str!("../migrations/20260607000000_harvest_worker_capability_labels/up.sql"),
    include_str!("../migrations/20260607000001_harvest_task_required_capabilities/up.sql"),
    "\n",
    include_str!("../migrations/20260607000002_harvest_workflow_pause/up.sql")
);

async fn make_conn() -> (
    diesel_async::AsyncPgConnection,
    testcontainers::ContainerAsync<Postgres>,
) {
    let container = Postgres::default().start().await.expect("postgres start");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgresql://postgres:postgres@127.0.0.1:{port}/postgres");
    let mut conn = diesel_async::AsyncPgConnection::establish(&url)
        .await
        .expect("connect");
    conn.batch_execute(INIT_SQL).await.expect("migration");
    (conn, container)
}

const fn succeeded_record<'a>(
    actor: &'a str,
    operation: &'a str,
    target_type: &'a str,
    target_id: Option<&'a str>,
) -> NewAuditRecord<'a> {
    NewAuditRecord {
        actor,
        operation,
        target_type,
        target_id,
        route_or_command: "/test/route",
        request_id: None,
        idempotency_key: None,
        status: STATUS_SUCCEEDED,
        error_summary: None,
        shard_id: None,
        source: "api",
    }
}

const fn failed_record<'a>(
    actor: &'a str,
    operation: &'a str,
    target_type: &'a str,
    error: &'a str,
) -> NewAuditRecord<'a> {
    NewAuditRecord {
        actor,
        operation,
        target_type,
        target_id: None,
        route_or_command: "/test/route",
        request_id: None,
        idempotency_key: None,
        status: STATUS_FAILED,
        error_summary: Some(error),
        shard_id: None,
        source: "api",
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn insert_and_retrieve_audit_record() {
    let (mut conn, _c) = make_conn().await;

    let record = succeeded_record(
        "alice",
        OP_WORKFLOW_START,
        TARGET_WORKFLOW,
        Some("exec-123"),
    );
    let id = audit::insert_audit(&mut conn, &record)
        .await
        .expect("insert audit");

    let filters = AuditFilters {
        limit: 10,
        ..Default::default()
    };
    let rows = audit::list_audit(&mut conn, &filters)
        .await
        .expect("list audit");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, id);
    assert_eq!(rows[0].actor, "alice");
    assert_eq!(rows[0].operation, OP_WORKFLOW_START);
    assert_eq!(rows[0].target_type, TARGET_WORKFLOW);
    assert_eq!(rows[0].target_id.as_deref(), Some("exec-123"));
    assert_eq!(rows[0].status, STATUS_SUCCEEDED);
    assert!(rows[0].error_summary.is_none());
}

#[tokio::test]
async fn audit_list_filter_by_actor() {
    let (mut conn, _c) = make_conn().await;

    audit::insert_audit(
        &mut conn,
        &succeeded_record("alice", OP_WORKFLOW_START, TARGET_WORKFLOW, None),
    )
    .await
    .expect("insert alice");
    audit::insert_audit(
        &mut conn,
        &succeeded_record("bob", OP_WORKFLOW_CANCEL, TARGET_WORKFLOW, None),
    )
    .await
    .expect("insert bob");

    let alice_rows = audit::list_audit(
        &mut conn,
        &AuditFilters {
            actor: Some("alice".to_string()),
            limit: 10,
            ..Default::default()
        },
    )
    .await
    .expect("list alice");

    assert_eq!(alice_rows.len(), 1);
    assert_eq!(alice_rows[0].actor, "alice");
}

#[tokio::test]
async fn audit_list_filter_by_operation() {
    let (mut conn, _c) = make_conn().await;

    audit::insert_audit(
        &mut conn,
        &succeeded_record("ops", OP_WORKFLOW_START, TARGET_WORKFLOW, None),
    )
    .await
    .expect("insert start");
    audit::insert_audit(
        &mut conn,
        &succeeded_record("ops", OP_WORKFLOW_CANCEL, TARGET_WORKFLOW, None),
    )
    .await
    .expect("insert cancel");

    let start_rows = audit::list_audit(
        &mut conn,
        &AuditFilters {
            operation: Some(OP_WORKFLOW_START.to_string()),
            limit: 10,
            ..Default::default()
        },
    )
    .await
    .expect("list starts");

    assert_eq!(start_rows.len(), 1);
    assert_eq!(start_rows[0].operation, OP_WORKFLOW_START);
}

#[tokio::test]
async fn audit_list_filter_by_target_type() {
    let (mut conn, _c) = make_conn().await;

    audit::insert_audit(
        &mut conn,
        &succeeded_record("ops", OP_WORKFLOW_START, TARGET_WORKFLOW, None),
    )
    .await
    .expect("insert workflow");
    audit::insert_audit(
        &mut conn,
        &succeeded_record("ops", "schedule.create", TARGET_SCHEDULE, None),
    )
    .await
    .expect("insert schedule");

    let schedule_rows = audit::list_audit(
        &mut conn,
        &AuditFilters {
            target_type: Some(TARGET_SCHEDULE.to_string()),
            limit: 10,
            ..Default::default()
        },
    )
    .await
    .expect("list schedules");

    assert_eq!(schedule_rows.len(), 1);
    assert_eq!(schedule_rows[0].target_type, TARGET_SCHEDULE);
}

#[tokio::test]
async fn audit_list_filter_by_status() {
    let (mut conn, _c) = make_conn().await;

    audit::insert_audit(
        &mut conn,
        &succeeded_record("ops", OP_WORKFLOW_START, TARGET_WORKFLOW, None),
    )
    .await
    .expect("insert success");
    audit::insert_audit(
        &mut conn,
        &failed_record("ops", OP_WORKFLOW_CANCEL, TARGET_WORKFLOW, "not found"),
    )
    .await
    .expect("insert fail");

    let failed_rows = audit::list_audit(
        &mut conn,
        &AuditFilters {
            status: Some(STATUS_FAILED.to_string()),
            limit: 10,
            ..Default::default()
        },
    )
    .await
    .expect("list failed");

    assert_eq!(failed_rows.len(), 1);
    assert_eq!(failed_rows[0].status, STATUS_FAILED);
    assert_eq!(failed_rows[0].error_summary.as_deref(), Some("not found"));
}

#[tokio::test]
async fn audit_list_filter_by_target_id() {
    let (mut conn, _c) = make_conn().await;

    audit::insert_audit(
        &mut conn,
        &succeeded_record("ops", OP_WORKFLOW_START, TARGET_WORKFLOW, Some("exec-aaa")),
    )
    .await
    .expect("insert aaa");
    audit::insert_audit(
        &mut conn,
        &succeeded_record("ops", OP_WORKFLOW_START, TARGET_WORKFLOW, Some("exec-bbb")),
    )
    .await
    .expect("insert bbb");

    let rows = audit::list_audit(
        &mut conn,
        &AuditFilters {
            target_id: Some("exec-aaa".to_string()),
            limit: 10,
            ..Default::default()
        },
    )
    .await
    .expect("list by target_id");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].target_id.as_deref(), Some("exec-aaa"));
}

#[tokio::test]
async fn audit_list_date_range_since() {
    let (mut conn, _c) = make_conn().await;

    let before = chrono::DateTime::parse_from_rfc3339("2026-05-06T12:00:00Z")
        .expect("fixed before timestamp")
        .with_timezone(&chrono::Utc);
    let midpoint = chrono::DateTime::parse_from_rfc3339("2026-05-06T12:30:00Z")
        .expect("fixed midpoint timestamp")
        .with_timezone(&chrono::Utc);
    let after = chrono::DateTime::parse_from_rfc3339("2026-05-06T13:00:00Z")
        .expect("fixed after timestamp")
        .with_timezone(&chrono::Utc);

    let before_id = audit::insert_audit(
        &mut conn,
        &succeeded_record("ops", OP_WORKFLOW_START, TARGET_WORKFLOW, None),
    )
    .await
    .expect("insert before");

    let after_id = audit::insert_audit(
        &mut conn,
        &succeeded_record("ops", OP_WORKFLOW_CANCEL, TARGET_WORKFLOW, None),
    )
    .await
    .expect("insert after");

    diesel::update(harvest_audit_log::table.filter(harvest_audit_log::id.eq(before_id)))
        .set(harvest_audit_log::occurred_at.eq(before))
        .execute(&mut conn)
        .await
        .expect("pin before timestamp");
    diesel::update(harvest_audit_log::table.filter(harvest_audit_log::id.eq(after_id)))
        .set(harvest_audit_log::occurred_at.eq(after))
        .execute(&mut conn)
        .await
        .expect("pin after timestamp");

    let rows = audit::list_audit(
        &mut conn,
        &AuditFilters {
            since: Some(midpoint),
            limit: 10,
            ..Default::default()
        },
    )
    .await
    .expect("list since");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].operation, OP_WORKFLOW_CANCEL);
}

#[tokio::test]
async fn audit_list_ordered_by_occurred_at_desc() {
    let (mut conn, _c) = make_conn().await;

    for i in 0..3u32 {
        let op = match i {
            0 => OP_WORKFLOW_START,
            1 => OP_WORKFLOW_CANCEL,
            _ => "schedule.delete",
        };
        audit::insert_audit(
            &mut conn,
            &succeeded_record("ops", op, TARGET_WORKFLOW, None),
        )
        .await
        .expect("insert");
        // Ensure distinct timestamps.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    let rows = audit::list_audit(
        &mut conn,
        &AuditFilters {
            limit: 10,
            ..Default::default()
        },
    )
    .await
    .expect("list");

    assert_eq!(rows.len(), 3);
    // Verify descending order.
    for pair in rows.windows(2) {
        assert!(pair[0].occurred_at >= pair[1].occurred_at);
    }
    // Newest (schedule.delete) should be first.
    assert_eq!(rows[0].operation, "schedule.delete");
}

#[tokio::test]
async fn audit_list_respects_limit() {
    let (mut conn, _c) = make_conn().await;

    for _ in 0..5 {
        audit::insert_audit(
            &mut conn,
            &succeeded_record("ops", OP_WORKFLOW_START, TARGET_WORKFLOW, None),
        )
        .await
        .expect("insert");
    }

    let rows = audit::list_audit(
        &mut conn,
        &AuditFilters {
            limit: 3,
            ..Default::default()
        },
    )
    .await
    .expect("list limited");

    assert_eq!(rows.len(), 3);
}

#[tokio::test]
async fn failed_audit_record_includes_error_summary() {
    let (mut conn, _c) = make_conn().await;

    let record = failed_record(
        "anonymous",
        OP_WORKFLOW_CANCEL,
        TARGET_WORKFLOW,
        "workflow not found: exec-xyz",
    );
    audit::insert_audit(&mut conn, &record)
        .await
        .expect("insert failed audit");

    let rows = audit::list_audit(
        &mut conn,
        &AuditFilters {
            status: Some(STATUS_FAILED.to_string()),
            limit: 10,
            ..Default::default()
        },
    )
    .await
    .expect("list");

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].error_summary.as_deref(),
        Some("workflow not found: exec-xyz")
    );
    // error_summary should not be the full raw payload.
    let summary = rows[0].error_summary.as_deref().unwrap_or("");
    assert!(!summary.contains("input"));
}

#[tokio::test]
async fn audit_record_with_all_optional_fields() {
    let (mut conn, _c) = make_conn().await;

    let record = NewAuditRecord {
        actor: "ci-bot",
        operation: OP_WORKFLOW_START,
        target_type: TARGET_WORKFLOW,
        target_id: Some("exec-full"),
        route_or_command: "POST /workflows/my_wf/start",
        request_id: Some("req-abc-123"),
        idempotency_key: Some("idem-key-xyz"),
        status: STATUS_SUCCEEDED,
        error_summary: None,
        shard_id: Some(0),
        source: "cli",
    };

    let id = audit::insert_audit(&mut conn, &record)
        .await
        .expect("insert full record");

    let rows = audit::list_audit(
        &mut conn,
        &AuditFilters {
            limit: 1,
            ..Default::default()
        },
    )
    .await
    .expect("list");

    assert_eq!(rows[0].id, id);
    assert_eq!(rows[0].actor, "ci-bot");
    assert_eq!(rows[0].request_id.as_deref(), Some("req-abc-123"));
    assert_eq!(rows[0].idempotency_key.as_deref(), Some("idem-key-xyz"));
    assert_eq!(rows[0].shard_id, Some(0));
    assert_eq!(rows[0].source, "cli");
}

// ── coverage guard: all mutation routes must be declared ─────────────────────

#[test]
fn audit_coverage_all_mutation_routes_declared() {
    // Every route in harvest_api_router must appear in ALL_MUTATION_ROUTES with
    // either Some(operation) (audited) or None (explicitly excluded). This test
    // catches routes added to the API without a matching declaration.
    //
    // The canonical manifest lives in `autumn_harvest::audit::ALL_MUTATION_ROUTES`.
    // When you add a new mutating route to `harvest_api_router`, add an entry to
    // that const — audited routes with `Some(OP_*)`, excluded routes with `None`.

    use autumn_harvest::audit::{ALL_MUTATION_ROUTES, AUDITED_OPERATIONS};

    // Every declared audited operation must exist in AUDITED_OPERATIONS.
    for (route, declared_op) in ALL_MUTATION_ROUTES {
        if let Some(op) = declared_op {
            assert!(
                AUDITED_OPERATIONS.contains(op),
                "Route '{route}' declares operation '{op}' not found in AUDITED_OPERATIONS"
            );
        }
    }

    // No duplicate route entries.
    let mut seen = std::collections::HashSet::new();
    for (route, _) in ALL_MUTATION_ROUTES {
        assert!(
            seen.insert(*route),
            "Duplicate route in ALL_MUTATION_ROUTES: {route}"
        );
    }

    // All known mutation routes (hard-coded expected set) must be present.
    let expected_mutations = [
        "POST /workflows/{workflow_name}/start",
        "POST /workflows/{workflow_name}/signal-with-start",
        "POST /workflows/{id}/cancel",
        "POST /workflows/{id}/reset",
        "POST /workflows/{id}/signal/{signal_name}",
        "POST /dags/{dag_name}/trigger",
        "PATCH /dags/{dag_name}",
        "POST /admin/schedules/workflow",
        "POST /admin/schedules/{id}/pause",
        "POST /admin/schedules/{id}/resume",
        "DELETE /admin/schedules/{id}",
        "POST /dead-letters/{id}/replay",
        "POST /dead-letters/replay",
        "POST /dead-letters/discard",
        "POST /batch-operations",
        "POST /admin/retention/run-now",
        "POST /activities/external/{token}/complete",
        "POST /activities/external/{token}/fail",
    ];

    let declared_routes: std::collections::HashSet<&str> =
        ALL_MUTATION_ROUTES.iter().map(|(r, _)| *r).collect();

    for expected in &expected_mutations {
        assert!(
            declared_routes.contains(*expected),
            "Expected mutation route '{expected}' not found in ALL_MUTATION_ROUTES"
        );
    }

    // Every expected mutation must be audited (not excluded).
    for expected in &expected_mutations {
        let entry = ALL_MUTATION_ROUTES
            .iter()
            .find(|(r, _)| r == expected)
            .unwrap_or_else(|| panic!("missing route {expected}"));
        assert!(
            entry.1.is_some(),
            "Mutation route '{expected}' is declared as excluded (None) but should be audited"
        );
    }
}
