//! HTTP integration tests for operator-mutable triage tags (issue #759).
//!
//! Exercises `PATCH /workflows/{id}/triage`, `GET /workflows/{id}` (describe),
//! and `GET /workflows?owner=&severity=` (list filter) end-to-end.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly (run this file `--test-threads=1`, since the tests scrub
//! shared tables); otherwise a fresh testcontainers Postgres is booted with
//! the full migration set (requires Docker).

#![allow(clippy::similar_names)]
#![allow(clippy::too_many_lines)]

use std::sync::Arc;

use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::schema::harvest_audit_log;
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use diesel::prelude::*;
use diesel::sql_types::{Nullable, Text, Timestamptz};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

type HarvestApiApp = axum::Router;

fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

async fn setup_database() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool should build")
}

fn build_app(pool: &DbPool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("triage-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

/// An app with NO external auth boundary (built-in admin guard active). Used to
/// exercise the admin-guard rejection: without an admin session, admin-only
/// routes return 401 before reaching the handler (no storage needed).
fn build_unauth_app() -> HarvestApiApp {
    harvest_api_router(HarvestApiState::new()).with_state(AppState::for_test())
}

async fn scrub(conn: &mut AsyncPgConnection) {
    for stmt in [
        "DELETE FROM harvest_audit_log",
        "DELETE FROM harvest_completion_deliveries",
        "DELETE FROM harvest_dead_letters",
        "DELETE FROM harvest_events",
        "DELETE FROM harvest_workflow_executions",
    ] {
        // Some tables may not exist in a minimal testcontainers bundle; ignore.
        let _ = diesel::sql_query(stmt).execute(conn).await;
    }
}

async fn patch_json_admin(app: &HarvestApiApp, uri: &str, body: Value) -> (StatusCode, Value) {
    send(app, "PATCH", uri, Some(body), true).await
}

async fn patch_json_noauth(app: &HarvestApiApp, uri: &str, body: Value) -> (StatusCode, Value) {
    send(app, "PATCH", uri, Some(body), false).await
}

async fn get_json(app: &HarvestApiApp, uri: &str) -> (StatusCode, Value) {
    send(app, "GET", uri, None, true).await
}

async fn send(
    app: &HarvestApiApp,
    method: &str,
    uri: &str,
    body: Option<Value>,
    admin: bool,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    if body.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    if admin {
        builder = builder.header("x-harvest-admin", "true");
    }
    let req = builder
        .body(body.map_or_else(Body::empty, |b| Body::from(b.to_string())))
        .unwrap();
    let response = app.clone().oneshot(req).await.expect("request");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).to_string()))
    };
    (status, json)
}

/// Seed an execution (shard 0) in `state` with one `WorkflowStarted` event, plus
/// optional seed owner/severity, so an annotate call has a row to act on.
async fn seed_execution(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    state: &str,
    owner: Option<&str>,
    severity: Option<&str>,
) -> String {
    #[derive(diesel::QueryableByName)]
    struct IdRow {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: uuid::Uuid,
    }
    let now = Utc::now();
    let id: uuid::Uuid = diesel::sql_query(
        "INSERT INTO harvest_workflow_executions
            (workflow_name, workflow_id, shard_id, state, input, started_at, completed_at,
             owner, severity)
         VALUES ($1, $2, 0, $3, '{}'::jsonb, $4, CASE WHEN $3 = 'COMPLETED' THEN $4 ELSE NULL END,
                 $5, $6)
         RETURNING id",
    )
    .bind::<Text, _>(workflow_name)
    .bind::<Text, _>(workflow_id)
    .bind::<Text, _>(state)
    .bind::<Timestamptz, _>(now)
    .bind::<Nullable<Text>, _>(owner)
    .bind::<Nullable<Text>, _>(severity)
    .get_result::<IdRow>(conn)
    .await
    .expect("insert execution")
    .id;

    diesel::sql_query(
        "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data)
         VALUES ($1, 0, 'WorkflowStarted', '{\"type\":\"WorkflowStarted\",\"data\":{\"input\":{},\"timestamp\":\"2026-01-01T00:00:00Z\"}}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .execute(conn)
    .await
    .expect("insert event");

    autumn_harvest::types::ExecutionId::from_uuid(id).to_string()
}

async fn audit_count(conn: &mut AsyncPgConnection, operation: &str, target: &str) -> i64 {
    harvest_audit_log::table
        .filter(harvest_audit_log::operation.eq(operation))
        .filter(harvest_audit_log::target_id.eq(target))
        .count()
        .get_result(conn)
        .await
        .unwrap()
}

/// The `(actor, error_summary)` of the most recently written audit row for
/// `operation`/`target` (issue #759 AC5 -- asserts CONTENT, not just
/// existence/count).
async fn latest_audit_row(
    conn: &mut AsyncPgConnection,
    operation: &str,
    target: &str,
) -> (String, Option<String>) {
    harvest_audit_log::table
        .filter(harvest_audit_log::operation.eq(operation))
        .filter(harvest_audit_log::target_id.eq(target))
        .order(harvest_audit_log::occurred_at.desc())
        .select((harvest_audit_log::actor, harvest_audit_log::error_summary))
        .first::<(String, Option<String>)>(conn)
        .await
        .unwrap()
}

async fn event_count(conn: &mut AsyncPgConnection, exec_id: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct CountRow {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_events WHERE workflow_exec_id = $1")
        .bind::<diesel::sql_types::Uuid, _>(uuid::Uuid::parse_str(exec_id).unwrap())
        .get_result::<CountRow>(conn)
        .await
        .unwrap()
        .n
}

// ── Tests ────────────────────────────────────────────────────────────────────

// PATCH sets owner/severity/note (200), writes exactly one `workflow.annotate`
// audit row, appends zero events (AC2/AC5), and describe reflects the new
// values. A second, identical PATCH is idempotent (200, empty changed_fields —
// AC4).
#[tokio::test]
async fn patch_round_trip_sets_fields_and_reflects_on_describe() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();
    scrub(&mut conn).await;

    let exec_id = seed_execution(&mut conn, "triage_wf", "http-1", "RUNNING", None, None).await;
    let events_before = event_count(&mut conn, &exec_id).await;

    let (status, body) = patch_json_admin(
        &app,
        &format!("/workflows/{exec_id}/triage"),
        json!({ "owner": "alice", "severity": "sev1", "note": "claimed for triage" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["execution_id"], json!(exec_id));
    assert_eq!(body["owner"], json!("alice"));
    assert_eq!(body["severity"], json!("sev1"));
    assert_eq!(body["note"], json!("claimed for triage"));
    let changed: Vec<&str> = body["changed_fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(changed.len(), 3, "all three fields changed: {changed:?}");

    // AC2: annotation appends NO WorkflowEvent.
    assert_eq!(
        event_count(&mut conn, &exec_id).await,
        events_before,
        "annotation must append zero events"
    );

    // AC5: exactly one succeeded audit row.
    assert_eq!(
        audit_count(&mut conn, "workflow.annotate", &exec_id).await,
        1,
        "one workflow.annotate audit row"
    );

    // Describe reflects the new triage view.
    let (status, body) = get_json(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "describe body: {body}");
    assert_eq!(body["execution"]["owner"], json!("alice"));
    assert_eq!(body["execution"]["severity"], json!("sev1"));
    assert_eq!(
        body["execution"]["triage_note"],
        json!("claimed for triage")
    );

    // AC4: an identical repeat is idempotent -- 200, empty changed_fields, no
    // second event, still exactly one audit row this time reporting no diff.
    let (status, body) = patch_json_admin(
        &app,
        &format!("/workflows/{exec_id}/triage"),
        json!({ "owner": "alice", "severity": "sev1", "note": "claimed for triage" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "repeat body: {body}");
    assert_eq!(
        body["changed_fields"].as_array().unwrap().len(),
        0,
        "identical repeat must report zero changed fields"
    );
    assert_eq!(
        audit_count(&mut conn, "workflow.annotate", &exec_id).await,
        2,
        "the repeat still writes its own (no-op) audit row"
    );
}

// AC1: an explicit JSON `null` clears a field to NULL. On the `TriageWorkflowResponse`
// (the PATCH response), every cleared field is OMITTED (`skip_serializing_if`).
// On the describe response, the cleared value is reflected per that struct's
// own established per-field convention: `owner`/`severity` (pre-#759 fields,
// no `skip_serializing_if`) serialize as explicit JSON `null`, while
// `triage_note` (added by #759, `skip_serializing_if` -- matching the recently
// -added `started_by`/`history_bloat_warned_at` sibling fields) is OMITTED.
#[tokio::test]
async fn explicit_null_clears_a_field_and_reflects_on_describe() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();
    scrub(&mut conn).await;

    let exec_id = seed_execution(
        &mut conn,
        "triage_wf",
        "http-2",
        "RUNNING",
        Some("bob"),
        Some("sev2"),
    )
    .await;

    // Seed a note too, via a first PATCH, so we can clear it alongside owner.
    let (status, _body) = patch_json_admin(
        &app,
        &format!("/workflows/{exec_id}/triage"),
        json!({ "note": "will be cleared" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = patch_json_admin(
        &app,
        &format!("/workflows/{exec_id}/triage"),
        json!({ "owner": null, "note": null }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert!(
        body.get("owner").is_none(),
        "cleared owner must be omitted from the PATCH response, got {body}"
    );
    assert!(
        body.get("note").is_none(),
        "cleared note must be omitted from the PATCH response, got {body}"
    );
    assert_eq!(
        body["severity"],
        json!("sev2"),
        "severity untouched by an owner/note-only clear"
    );

    let (_, body) = get_json(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(
        body["execution"]["owner"],
        Value::Null,
        "describe reflects a cleared `owner` as explicit JSON null (pre-#759 field convention)"
    );
    assert!(
        body["execution"].get("triage_note").is_none(),
        "describe OMITS a cleared `triage_note` (skip_serializing_if, matching started_by/history_bloat_warned_at)"
    );
    assert_eq!(body["execution"]["severity"], json!("sev2"));
}

// AC4/AC6: annotating a RUNNING execution works identically to a COMPLETED
// one -- annotation is orthogonal to lifecycle state.
#[tokio::test]
async fn patch_works_on_a_running_execution() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();
    scrub(&mut conn).await;

    let exec_id = seed_execution(&mut conn, "triage_wf", "http-3", "RUNNING", None, None).await;

    let (status, body) = patch_json_admin(
        &app,
        &format!("/workflows/{exec_id}/triage"),
        json!({ "owner": "carol" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["owner"], json!("carol"));
}

// Malformed body (unknown field name) is rejected 400, with a failed audit
// row -- never axum's plain-text 422, and never a silent no-op success.
#[tokio::test]
async fn malformed_body_returns_400_with_audit_row() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();
    scrub(&mut conn).await;

    let exec_id = seed_execution(&mut conn, "triage_wf", "http-4", "RUNNING", None, None).await;

    let (status, body) = patch_json_admin(
        &app,
        &format!("/workflows/{exec_id}/triage"),
        json!({ "onwer": "typo" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "unknown field must be a structured 400, body: {body}"
    );

    // Exactly one failed audit row was written for the rejected attempt.
    let failed: i64 = harvest_audit_log::table
        .filter(harvest_audit_log::operation.eq("workflow.annotate"))
        .filter(harvest_audit_log::target_id.eq(&exec_id))
        .filter(harvest_audit_log::status.eq("failed"))
        .count()
        .get_result(&mut conn)
        .await
        .unwrap();
    assert_eq!(failed, 1, "a malformed body must still be audited");
}

// AC6: a MALFORMED execution id (not a valid UUID at all) is rejected 400 --
// distinct from a well-formed-but-unknown UUID, which is 404 (see
// `unknown_execution_id_returns_404` below). Never a panic, never a 500.
#[tokio::test]
async fn malformed_execution_id_returns_400_not_500() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (status, body) = patch_json_admin(
        &app,
        "/workflows/not-a-valid-uuid/triage",
        json!({ "owner": "x" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a malformed exec id must 400 (via parse_execution_id), never 404 or 500: {body}"
    );
}

// AC6: an unknown execution id returns 404 (never a 500, never a silent
// success).
#[tokio::test]
async fn unknown_execution_id_returns_404() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();
    scrub(&mut conn).await;

    let missing = "00000000-0000-0000-0000-000000000042";
    let (status, body) = patch_json_admin(
        &app,
        &format!("/workflows/{missing}/triage"),
        json!({ "owner": "someone" }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "body: {body}");
}

// AC4: the route is admin-guarded -- with the built-in guard active and no
// admin session, it returns 401 before reaching the handler (no storage
// needed).
#[tokio::test]
async fn patch_requires_admin() {
    let app = build_unauth_app();
    let exec = "00000000-0000-0000-0000-000000000001";

    let (status, _body) = patch_json_noauth(
        &app,
        &format!("/workflows/{exec}/triage"),
        json!({ "owner": "x" }),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "PATCH is admin-guarded");
}

// A set value longer than the operator-reason cap (500 chars, shared with
// pause/legal-hold/queue-pause `reason` fields via `truncate_operator_reason`)
// is truncated at the API boundary rather than persisted unbounded -- these
// fields are now runtime, HTTP-caller-controlled strings (unlike the
// compile-time `&'static str` owner/severity defaults from issue #372), and
// they are echoed into the audit log and the durable row alike.
#[tokio::test]
async fn oversized_field_values_are_truncated_at_the_boundary() {
    const CAP: usize = 500;

    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();
    scrub(&mut conn).await;

    let exec_id = seed_execution(&mut conn, "triage_wf", "http-5", "RUNNING", None, None).await;

    let long_owner = "o".repeat(CAP + 200);
    let long_note = "n".repeat(CAP + 200);

    let (status, body) = patch_json_admin(
        &app,
        &format!("/workflows/{exec_id}/triage"),
        json!({ "owner": long_owner, "note": long_note }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    let got_owner = body["owner"].as_str().expect("owner string");
    let got_note = body["note"].as_str().expect("note string");
    assert_eq!(got_owner.chars().count(), CAP, "owner must be capped");
    assert_eq!(got_note.chars().count(), CAP, "note must be capped");
    assert!(
        "o".repeat(CAP + 200).starts_with(got_owner),
        "the retained prefix must be a genuine truncation, not mangled"
    );

    // The cap holds through to storage, not just the response echo.
    let (_, body) = get_json(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(
        body["execution"]["owner"].as_str().unwrap().chars().count(),
        CAP,
        "describe reflects the capped value, not the original oversized one"
    );
    assert_eq!(
        body["execution"]["triage_note"]
            .as_str()
            .unwrap()
            .chars()
            .count(),
        CAP
    );
}

// AC1/AC4: a truly EMPTY body (`{}`, every field omitted) is a genuine
// HTTP-level no-op -- 200, empty `changed_fields`, no row mutation, and
// (mirroring the pause/resume "no `error_summary` on a no-op" convention
// `triage_diff_summary` documents) the written audit row reports no diff.
// This exercises the full stack the CLI's `--no-flags` mapping relies on
// (the CLI sends exactly `json!({})` when no annotate flags are given).
#[tokio::test]
async fn empty_body_patch_is_a_true_http_level_no_op() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();
    scrub(&mut conn).await;

    let exec_id = seed_execution(
        &mut conn,
        "triage_wf",
        "http-empty",
        "RUNNING",
        Some("frank"),
        Some("sev3"),
    )
    .await;

    let (status, body) =
        patch_json_admin(&app, &format!("/workflows/{exec_id}/triage"), json!({})).await;
    assert_eq!(status, StatusCode::OK, "empty body must 200, body: {body}");
    assert_eq!(
        body["changed_fields"].as_array().unwrap().len(),
        0,
        "an empty body changes nothing"
    );

    // The row itself is untouched.
    let (_, describe) = get_json(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(describe["execution"]["owner"], json!("frank"));
    assert_eq!(describe["execution"]["severity"], json!("sev3"));

    // The call is still audited (existing "no-op still writes its own row"
    // convention, per `patch_round_trip_sets_fields_and_reflects_on_describe`),
    // but its `error_summary` is `None` -- `triage_diff_summary` returns
    // `None` for an empty changed-fields list.
    assert_eq!(
        audit_count(&mut conn, "workflow.annotate", &exec_id).await,
        1
    );
    let (_actor, summary) = latest_audit_row(&mut conn, "workflow.annotate", &exec_id).await;
    assert!(
        summary.is_none(),
        "a true no-op must report no old->new diff, got {summary:?}"
    );
}

// AC5: the audit row for a mutating PATCH captures the ACTOR and the
// old->new VALUES for every changed field -- not merely that a row exists.
// A bug that dropped a field from the diff, swapped old/new, or wrote the
// wrong actor would be invisible to a bare row-count assertion.
#[tokio::test]
async fn audit_row_records_actor_and_old_to_new_values() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();
    scrub(&mut conn).await;

    let exec_id = seed_execution(
        &mut conn,
        "triage_wf",
        "http-audit-content",
        "RUNNING",
        Some("dave"),
        None,
    )
    .await;

    let (status, _body) = patch_json_admin(
        &app,
        &format!("/workflows/{exec_id}/triage"),
        json!({ "owner": "erin", "severity": "sev2" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (actor, summary) = latest_audit_row(&mut conn, "workflow.annotate", &exec_id).await;
    assert_eq!(
        actor, "anonymous",
        "actor is captured on the audit row (no X-Harvest-Actor header sent in this test)"
    );
    let summary = summary.expect("a changed patch must record a diff summary");
    assert!(
        summary.contains("owner: dave -> erin"),
        "diff summary must record the OWNER old->new transition, got {summary:?}"
    );
    assert!(
        summary.contains("severity: (none) -> sev2"),
        "diff summary must record the SEVERITY old(absent)->new transition, got {summary:?}"
    );
}

// AC3: owner/severity set via PATCH are IMMEDIATELY reflected in the existing
// list filters (`GET /workflows?owner=...&severity=...`) with the same
// semantics as the start-time values from #372 -- an execution whose owner
// was reassigned via triage is found under the NEW owner and NOT the old one.
#[tokio::test]
async fn owner_and_severity_set_via_patch_are_reflected_in_list_filter() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();
    scrub(&mut conn).await;

    // Two executions: one starts owned by "team-a", the other unowned.
    let claimed = seed_execution(
        &mut conn,
        "triage_wf",
        "http-filter-1",
        "RUNNING",
        Some("team-a"),
        None,
    )
    .await;
    let other = seed_execution(
        &mut conn,
        "triage_wf",
        "http-filter-2",
        "RUNNING",
        None,
        None,
    )
    .await;

    // Operator claims `claimed` and escalates its severity via triage.
    let (status, _body) = patch_json_admin(
        &app,
        &format!("/workflows/{claimed}/triage"),
        json!({ "owner": "team-b", "severity": "sev1" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Filtering by the NEW owner+severity finds exactly the claimed execution.
    let (status, body) = get_json(&app, "/workflows?owner=team-b&severity=sev1").await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    let ids: Vec<&str> = body
        .as_array()
        .expect("bare array on the happy path")
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert!(
        ids.contains(&claimed.as_str()),
        "claimed execution must be found under its new owner: {ids:?}"
    );
    assert!(
        !ids.contains(&other.as_str()),
        "the unrelated execution must not match: {ids:?}"
    );

    // Filtering by the OLD owner ("team-a") no longer finds it -- the mutation
    // is a full re-route, not an additive tag.
    let (_, body) = get_json(&app, "/workflows?owner=team-a").await;
    let old_owner_ids: Vec<&str> = body
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert!(
        !old_owner_ids.contains(&claimed.as_str()),
        "the old owner must no longer match after re-routing: {old_owner_ids:?}"
    );
}
