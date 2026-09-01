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

use std::collections::BTreeMap;
use std::sync::Arc;

use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::schema::harvest_audit_log;
use autumn_harvest::shard::{ShardRouter, ShardedDbPool};
use autumn_harvest::types::{ExecutionId, ShardId};
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
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

type HarvestApiApp = axum::Router;

fn init_sql() -> Vec<u8> {
    autumn_harvest::test_init_sql().as_bytes().to_vec()
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

/// Build the test app from an arbitrary storage pool + router. Factored out
/// of `build_app` so the exact-shard-resolver tests below can wire a
/// genuinely multi-shard, partially-unavailable topology.
fn build_app_with_router(pool: HarvestDbPool, router: ShardRouter) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(pool);
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("triage-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        router,
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

fn build_app(pool: &DbPool) -> HarvestApiApp {
    build_app_with_router(HarvestDbPool::from(pool.clone()), ShardRouter::default())
}

/// Create a fresh, fully-migrated database and return its URL -- for the
/// exact-shard-resolver tests, which need a genuinely separate database per
/// shard (one database pretending to be two shards would not exercise the
/// "this shard's pool is unconfigured" case). Mirrors
/// `lineage_tree_integration.rs::create_shard_db`.
async fn create_shard_db(admin_url: &str, name: &str) -> String {
    let mut admin = AsyncPgConnection::establish(admin_url)
        .await
        .expect("connect to admin database");
    // Ignore "already exists" -- a re-run against a local server is fine.
    let _ = diesel::sql_query(format!("CREATE DATABASE \"{name}\""))
        .execute(&mut admin)
        .await;

    let url = replace_database(admin_url, name);
    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect to fresh shard database");
    let bundle = String::from_utf8(init_sql()).expect("migration bundle is utf-8");
    conn.batch_execute(&bundle)
        .await
        .expect("apply migration bundle");
    url
}

fn replace_database(url: &str, name: &str) -> String {
    let (prefix, _) = url.rsplit_once('/').expect("url has a database segment");
    format!("{prefix}/{name}")
}

fn unique(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::new_v4().simple())
}

fn router_for(shards: &[i32]) -> ShardRouter {
    let ids: Vec<ShardId> = shards.iter().map(|s| ShardId::new(*s)).collect();
    ShardRouter::new(ids.clone(), ids, ShardId::new(shards[0]))
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
///
/// The id is minted via `ExecutionId::new_for_shard(0)` (matching how a real
/// deployment mints ids -- see `run_chain_integration.rs`'s `seed_run`)
/// rather than left to Postgres's own default UUID generation: the triage
/// handler resolves its connection via the **exact** shard resolver (issue
/// #759 review), which decodes the target shard from the id's own bytes and
/// has no default-shard fallback -- a randomly-generated id's bytes are
/// essentially never shard 0, which would make every seeded row invisible to
/// the handler regardless of the `shard_id` column value set below.
async fn seed_execution(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    state: &str,
    owner: Option<&str>,
    severity: Option<&str>,
) -> String {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let id = exec_id.as_uuid();
    let now = Utc::now();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions
            (id, workflow_name, workflow_id, shard_id, state, input, started_at, completed_at,
             owner, severity)
         VALUES ($1, $2, $3, 0, $4, '{}'::jsonb, $5, CASE WHEN $4 = 'COMPLETED' THEN $5 ELSE NULL END,
                 $6, $7)",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .bind::<Text, _>(workflow_name)
    .bind::<Text, _>(workflow_id)
    .bind::<Text, _>(state)
    .bind::<Timestamptz, _>(now)
    .bind::<Nullable<Text>, _>(owner)
    .bind::<Nullable<Text>, _>(severity)
    .execute(conn)
    .await
    .expect("insert execution");

    diesel::sql_query(
        "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data)
         VALUES ($1, 0, 'WorkflowStarted', '{\"type\":\"WorkflowStarted\",\"data\":{\"input\":{},\"timestamp\":\"2026-01-01T00:00:00Z\"}}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .execute(conn)
    .await
    .expect("insert event");

    exec_id.to_string()
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
    // Values are quoted in the audit summary (issue #759 review) — see
    // `audit_summary_quotes_values_so_lookalike_syntax_cannot_be_misread`
    // below for the escaping contract itself.
    assert!(
        summary.contains(r#"owner: "dave" -> "erin""#),
        "diff summary must record the OWNER old->new transition, got {summary:?}"
    );
    assert!(
        summary.contains(r#"severity: (none) -> "sev2""#),
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

// A whitespace-padded owner/severity value is trimmed before it is persisted
// (issue #759 review): `GET /workflows?owner=...` trims its query value, so
// an untrimmed stored label would be permanently unaddressable through the
// advertised filter. Both describe and the list filter must see the
// TRIMMED value.
#[tokio::test]
async fn whitespace_padded_owner_and_severity_are_trimmed_and_filterable() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();
    scrub(&mut conn).await;

    let exec_id = seed_execution(
        &mut conn,
        "triage_wf",
        "http-whitespace-1",
        "RUNNING",
        None,
        None,
    )
    .await;

    let (status, body) = patch_json_admin(
        &app,
        &format!("/workflows/{exec_id}/triage"),
        json!({ "owner": "  team-a  ", "severity": "\tsev2\n" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(
        body["owner"],
        json!("team-a"),
        "the PATCH response must echo the TRIMMED owner, got {body}"
    );
    assert_eq!(
        body["severity"],
        json!("sev2"),
        "the PATCH response must echo the TRIMMED severity, got {body}"
    );

    let (_, describe) = get_json(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(describe["execution"]["owner"], json!("team-a"));
    assert_eq!(describe["execution"]["severity"], json!("sev2"));

    // Filtering with the CANONICAL (untrimmed-in-the-query, but the filter
    // itself trims) spelling now finds it -- proving the stored value is
    // addressable through the documented filter.
    let (status, body) = get_json(&app, "/workflows?owner=team-a&severity=sev2").await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    let ids: Vec<&str> = body
        .as_array()
        .expect("bare array on the happy path")
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert!(
        ids.contains(&exec_id.as_str()),
        "the trimmed-and-stored owner/severity must be findable via the list filter: {ids:?}"
    );
}

// An all-whitespace owner/severity SET is folded into an explicit clear
// (issue #759 review), matching how `parse_workflow_filters` already treats
// an all-whitespace QUERY value as "no filter" -- a whitespace-only value can
// never be persisted as a phantom, unfilterable label.
#[tokio::test]
async fn all_whitespace_owner_set_is_folded_into_a_clear() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();
    scrub(&mut conn).await;

    let exec_id = seed_execution(
        &mut conn,
        "triage_wf",
        "http-whitespace-2",
        "RUNNING",
        Some("preexisting-owner"),
        Some("sev1"),
    )
    .await;

    let (status, body) = patch_json_admin(
        &app,
        &format!("/workflows/{exec_id}/triage"),
        json!({ "owner": "   ", "severity": "\t\t" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert!(
        body.get("owner").is_none(),
        "an all-whitespace SET must clear owner (skip_serializing_if on None), got {body}"
    );
    assert!(
        body.get("severity").is_none(),
        "an all-whitespace SET must clear severity (skip_serializing_if on None), got {body}"
    );

    let (_, describe) = get_json(&app, &format!("/workflows/{exec_id}")).await;
    assert!(
        describe["execution"]["owner"].is_null(),
        "owner must read back cleared, got {describe}"
    );
    assert!(
        describe["execution"]["severity"].is_null(),
        "severity must read back cleared, got {describe}"
    );
}

// The audit diff summary quotes every old/new value (issue #759 review), so
// a free-text `note` containing summary-lookalike syntax can never be
// mistaken for a second, bogus field-change entry, and a literal value of
// the string "(none)" is visibly distinct (quoted) from the unset sentinel
// (unquoted).
#[tokio::test]
async fn audit_summary_quotes_values_so_lookalike_syntax_cannot_be_misread() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();
    scrub(&mut conn).await;

    let exec_id = seed_execution(
        &mut conn,
        "triage_wf",
        "http-escape-1",
        "RUNNING",
        None,
        None,
    )
    .await;

    // A note whose CONTENT looks like a second, bogus field-change entry.
    let (status, _body) = patch_json_admin(
        &app,
        &format!("/workflows/{exec_id}/triage"),
        json!({ "note": "ready; severity: P3 -> P0" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (_, summary) = latest_audit_row(&mut conn, "workflow.annotate", &exec_id).await;
    let summary = summary.expect("a changed patch must record a diff summary");
    // Only `note` changed, so the WHOLE summary must be exactly this one
    // quoted field-change entry -- not merely `.contains()` it -- proving
    // the note's embedded "; severity: P3 -> P0" text (a deliberate
    // collision with the "; " field-change separator `triage_diff_summary`
    // itself uses to join entries) was never split out into a second,
    // bogus `severity:` entry appended after the note's value.
    assert_eq!(
        summary, r#"note: (none) -> "ready; severity: P3 -> P0""#,
        "the lookalike note content must be wrapped in one quoted value, not read as a \
         separate field-change entry: {summary:?}"
    );

    // A note whose value is LITERALLY the unset-sentinel string "(none)".
    let (status, _body) = patch_json_admin(
        &app,
        &format!("/workflows/{exec_id}/triage"),
        json!({ "note": "(none)" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (_, summary) = latest_audit_row(&mut conn, "workflow.annotate", &exec_id).await;
    let summary = summary.expect("a changed patch must record a diff summary");
    assert!(
        summary.contains(r#"-> "(none)""#),
        "a literal '(none)' VALUE must render quoted, distinct from the unquoted unset \
         sentinel: {summary:?}"
    );
}

// ── Exact shard resolution (issue #759 review) ─────────────────────────────
//
// The PATCH handler resolves its connection via the **exact** shard resolver
// (`db_conn_for_execution_exact`), not the lenient `db_conn_for_execution`
// every other single-execution route on this router still uses. The lenient
// resolver falls back to the **default** shard when the target execution's
// owning shard has no configured pool on this node -- the documented
// mid-shard-add-rollout state. For a WRITE that fallback is a genuine
// correctness hazard, strictly worse than the false-404 a read would
// produce: it can report a false 404 for an execution that exists on the
// unreachable shard, or -- if the default shard happens to hold an unrelated
// row sharing the same UUID -- silently mutate the wrong database. These two
// tests mirror `lineage_tree_integration.rs`'s
// `a_root_on_a_router_known_shard_with_no_pool_is_503_not_a_false_404` /
// `a_root_on_a_shard_this_deployment_does_not_know_is_still_404`.

/// The owning shard is known to the router but has no pool configured on
/// this process -- must fail closed with `503`, never a false `404` or a
/// silent write to the wrong database.
#[tokio::test]
async fn a_target_on_a_router_known_shard_with_no_pool_is_503_not_a_false_404() {
    let (admin_url, _guard) = setup_database().await;
    let shard0 = create_shard_db(&admin_url, &unique("triage_s0")).await;

    // The router knows shards 0 and 1, but this process only has a pool for
    // 0 -- the documented mid-shard-add-rollout state (`readable_shards`
    // widened before the pool is configured).
    let pools = BTreeMap::from([(ShardId::new(0), build_pool(&shard0))]);
    let app = build_app_with_router(
        HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0))),
        router_for(&[0, 1]),
    );

    // An id whose embedded shard is 1 -- the shard this process cannot query.
    let target = ExecutionId::new_for_shard(ShardId::new(1));

    let (status, body) = patch_json_admin(
        &app,
        &format!("/workflows/{target}/triage"),
        json!({ "owner": "someone" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "the owning shard could not be queried, so existence is unknown -- \
         answering 404 (or worse, silently writing elsewhere) would be \
         wrong; body: {body}"
    );
    let message = body.to_string();
    assert!(
        message.contains('1'),
        "the response must name the shard that could not be queried: {message}"
    );
}

/// ...but an id whose embedded shard is not known to this deployment at all
/// (a stale id, or an operator pasting a foreign UUID) must still be a clean
/// `404`, not a confusing `503` -- the exact resolver must not over-reach.
#[tokio::test]
async fn a_target_on_a_shard_this_deployment_does_not_know_is_still_404() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    // Shard 7 is neither a configured pool nor known to the (single-shard,
    // default) router this test app builds.
    let target = ExecutionId::new_for_shard(ShardId::new(7));

    let (status, body) = patch_json_admin(
        &app,
        &format!("/workflows/{target}/triage"),
        json!({ "owner": "someone" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "no shard in this deployment could host that id, so 404 is honest: {body}"
    );
}
