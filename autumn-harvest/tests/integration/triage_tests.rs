#![cfg(feature = "db")]
//! Operator-mutable triage tags behavioral tests — issue #759.
//!
//! Drives the real `annotate_workflow_execution` core function against a
//! Postgres instance and asserts:
//!
//! - A partial patch mutates only the fields present in the request; an
//!   absent field is left unchanged (AC1).
//! - An explicit JSON `null` (`Some(None)`) clears a field to `NULL` (AC1).
//! - An empty patch (every field `None`) is a true no-op: no write occurs and
//!   the row is byte-identical afterward (AC1/AC4).
//! - Applying the same non-empty patch twice is idempotent -- the second
//!   application reports no changed fields and leaves the row unchanged
//!   (AC4).
//! - Annotation appends **zero** `WorkflowEvent`s -- `harvest_events` row
//!   count is unchanged before/after (AC2).
//! - Annotation succeeds identically regardless of execution state --
//!   RUNNING, PAUSED, FAILED, COMPLETED all annotate the same way; only an
//!   unknown execution id is rejected (AC6).
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly (single-threaded; each test scrubs first); otherwise a
//! fresh testcontainers Postgres is booted with the full migration bundle.

use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::DbPool;
use autumn_harvest::{TriagePatch, annotate_workflow_execution};
use diesel::sql_types::{BigInt, Nullable, Text};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

fn init_sql() -> Vec<u8> {
    autumn_harvest::test_init_sql().as_bytes().to_vec()
}

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

#[allow(dead_code)]
fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool build failed")
}

async fn scrub(conn: &mut AsyncPgConnection) {
    for stmt in [
        "DELETE FROM harvest_completion_deliveries",
        "DELETE FROM harvest_dead_letters",
        "DELETE FROM harvest_workflow_executions",
    ] {
        diesel::sql_query(stmt).execute(conn).await.expect(stmt);
    }
}

#[derive(diesel::QueryableByName)]
struct IdRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: uuid::Uuid,
}

#[derive(diesel::QueryableByName)]
struct TriageRow {
    #[diesel(sql_type = Nullable<Text>)]
    owner: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    severity: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    triage_note: Option<String>,
}

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

/// Inserts an execution in the given `state` with the given seed
/// `owner`/`severity`/`triage_note`, plus one trivial `WorkflowStarted` event
/// so history exists (mirrors `legal_hold_tests::insert_completed`).
async fn insert_execution(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    state: &str,
    owner: Option<&str>,
    severity: Option<&str>,
    triage_note: Option<&str>,
) -> uuid::Uuid {
    let id: uuid::Uuid = diesel::sql_query(
        "INSERT INTO harvest_workflow_executions
            (workflow_name, workflow_id, shard_id, state, input, started_at,
             owner, severity, triage_note)
         VALUES ($1, $2, 0, $3, '{}'::jsonb, NOW(), $4, $5, $6)
         RETURNING id",
    )
    .bind::<Text, _>(workflow_name)
    .bind::<Text, _>(workflow_id)
    .bind::<Text, _>(state)
    .bind::<Nullable<Text>, _>(owner)
    .bind::<Nullable<Text>, _>(severity)
    .bind::<Nullable<Text>, _>(triage_note)
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
    id
}

async fn triage_row(conn: &mut AsyncPgConnection, id: uuid::Uuid) -> TriageRow {
    diesel::sql_query(
        "SELECT owner, severity, triage_note FROM harvest_workflow_executions WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .get_result::<TriageRow>(conn)
    .await
    .expect("triage row")
}

async fn event_count(conn: &mut AsyncPgConnection, id: uuid::Uuid) -> i64 {
    diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_events WHERE workflow_exec_id = $1")
        .bind::<diesel::sql_types::Uuid, _>(id)
        .get_result::<CountRow>(conn)
        .await
        .expect("event count")
        .n
}

fn empty_patch() -> TriagePatch {
    TriagePatch::default()
}

// AC1: a partial patch mutates only the fields present in the request; an
// absent field is left unchanged.
#[tokio::test]
async fn partial_update_leaves_other_fields_unchanged() {
    let (url, _container) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let id = insert_execution(
        &mut conn,
        "triage_wf",
        "t1",
        "RUNNING",
        Some("alice"),
        Some("sev2"),
        Some("initial note"),
    )
    .await;
    let exec_id = ExecutionId::from_uuid(id);

    // Only `owner` is present in the patch — severity and note must survive.
    let patch = TriagePatch {
        owner: Some(Some("bob".to_string())),
        severity: None,
        note: None,
    };
    let outcome = annotate_workflow_execution(&mut conn, exec_id, patch)
        .await
        .expect("annotate");

    assert_eq!(outcome.owner.as_deref(), Some("bob"));
    assert_eq!(outcome.severity.as_deref(), Some("sev2"));
    assert_eq!(outcome.triage_note.as_deref(), Some("initial note"));
    assert_eq!(outcome.changed_fields.len(), 1);
    assert_eq!(outcome.changed_fields[0].field, "owner");
    assert_eq!(outcome.changed_fields[0].old.as_deref(), Some("alice"));
    assert_eq!(outcome.changed_fields[0].new.as_deref(), Some("bob"));

    let row = triage_row(&mut conn, id).await;
    assert_eq!(row.owner.as_deref(), Some("bob"));
    assert_eq!(row.severity.as_deref(), Some("sev2"));
    assert_eq!(row.triage_note.as_deref(), Some("initial note"));
}

// AC1: an explicit JSON `null` (Some(None)) clears a field to NULL.
#[tokio::test]
async fn explicit_null_clears_the_field() {
    let (url, _container) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let id = insert_execution(
        &mut conn,
        "triage_wf",
        "t2",
        "RUNNING",
        Some("alice"),
        Some("sev1"),
        Some("some note"),
    )
    .await;
    let exec_id = ExecutionId::from_uuid(id);

    let patch = TriagePatch {
        owner: Some(None),
        severity: None,
        note: Some(None),
    };
    let outcome = annotate_workflow_execution(&mut conn, exec_id, patch)
        .await
        .expect("annotate");

    assert_eq!(outcome.owner, None, "owner cleared to NULL");
    assert_eq!(
        outcome.severity.as_deref(),
        Some("sev1"),
        "severity untouched"
    );
    assert_eq!(outcome.triage_note, None, "note cleared to NULL");

    // Two fields changed: owner and note.
    let changed: Vec<&str> = outcome.changed_fields.iter().map(|c| c.field).collect();
    assert_eq!(changed.len(), 2);
    assert!(changed.contains(&"owner"));
    assert!(changed.contains(&"note"));

    let row = triage_row(&mut conn, id).await;
    assert_eq!(row.owner, None);
    assert_eq!(row.severity.as_deref(), Some("sev1"));
    assert_eq!(row.triage_note, None);
}

// AC1/AC4: an empty patch (every field absent) is a true no-op -- no write
// occurs and no fields are reported changed.
#[tokio::test]
async fn empty_patch_is_a_true_no_op() {
    let (url, _container) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let id = insert_execution(
        &mut conn,
        "triage_wf",
        "t3",
        "RUNNING",
        Some("alice"),
        Some("sev3"),
        Some("note"),
    )
    .await;
    let exec_id = ExecutionId::from_uuid(id);

    let outcome = annotate_workflow_execution(&mut conn, exec_id, empty_patch())
        .await
        .expect("annotate");

    assert_eq!(outcome.owner.as_deref(), Some("alice"));
    assert_eq!(outcome.severity.as_deref(), Some("sev3"));
    assert_eq!(outcome.triage_note.as_deref(), Some("note"));
    assert!(
        outcome.changed_fields.is_empty(),
        "an empty patch must report zero changed fields"
    );

    let row = triage_row(&mut conn, id).await;
    assert_eq!(row.owner.as_deref(), Some("alice"));
    assert_eq!(row.severity.as_deref(), Some("sev3"));
    assert_eq!(row.triage_note.as_deref(), Some("note"));
}

// AC4: applying the same non-empty patch twice is idempotent -- the second
// application reports no changed fields (final value equals current value)
// and leaves the row byte-identical.
#[tokio::test]
async fn identical_patch_applied_twice_is_idempotent() {
    let (url, _container) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let id = insert_execution(&mut conn, "triage_wf", "t4", "RUNNING", None, None, None).await;
    let exec_id = ExecutionId::from_uuid(id);

    let patch = || TriagePatch {
        owner: Some(Some("carol".to_string())),
        severity: Some(Some("sev5".to_string())),
        note: Some(Some("claimed for triage".to_string())),
    };

    let first = annotate_workflow_execution(&mut conn, exec_id, patch())
        .await
        .expect("first annotate");
    assert_eq!(first.changed_fields.len(), 3, "first apply changes all 3");

    let second = annotate_workflow_execution(&mut conn, exec_id, patch())
        .await
        .expect("second annotate");
    assert!(
        second.changed_fields.is_empty(),
        "re-applying an identical patch must report zero changes"
    );
    assert_eq!(second.owner.as_deref(), Some("carol"));
    assert_eq!(second.severity.as_deref(), Some("sev5"));
    assert_eq!(second.triage_note.as_deref(), Some("claimed for triage"));

    let row = triage_row(&mut conn, id).await;
    assert_eq!(row.owner.as_deref(), Some("carol"));
    assert_eq!(row.severity.as_deref(), Some("sev5"));
    assert_eq!(row.triage_note.as_deref(), Some("claimed for triage"));
}

// AC2: annotation appends ZERO WorkflowEvents -- harvest_events row count is
// unchanged before/after, regardless of how many triage fields are touched.
#[tokio::test]
async fn annotation_appends_no_workflow_events() {
    let (url, _container) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let id = insert_execution(&mut conn, "triage_wf", "t5", "RUNNING", None, None, None).await;
    let exec_id = ExecutionId::from_uuid(id);

    let before = event_count(&mut conn, id).await;
    assert_eq!(before, 1, "the seeded WorkflowStarted event");

    let patch = TriagePatch {
        owner: Some(Some("dave".to_string())),
        severity: Some(Some("sev1".to_string())),
        note: Some(Some("escalated".to_string())),
    };
    annotate_workflow_execution(&mut conn, exec_id, patch)
        .await
        .expect("annotate");

    let after = event_count(&mut conn, id).await;
    assert_eq!(
        after, before,
        "annotation must append zero WorkflowEvent rows (AC2)"
    );
}

// AC6: annotation works identically regardless of execution state --
// RUNNING, PAUSED, FAILED, COMPLETED all annotate the same way.
#[tokio::test]
async fn annotation_works_regardless_of_execution_state() {
    let (url, _container) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    for (idx, state) in ["RUNNING", "PAUSED", "FAILED", "COMPLETED", "CANCELLED"]
        .iter()
        .enumerate()
    {
        let id = insert_execution(
            &mut conn,
            "triage_wf",
            &format!("state-{idx}"),
            state,
            None,
            None,
            None,
        )
        .await;
        let exec_id = ExecutionId::from_uuid(id);

        let patch = TriagePatch {
            owner: Some(Some("operator".to_string())),
            severity: Some(Some("sev1".to_string())),
            note: None,
        };
        let outcome = annotate_workflow_execution(&mut conn, exec_id, patch)
            .await
            .unwrap_or_else(|e| panic!("annotate must succeed for state {state}: {e}"));

        assert_eq!(
            outcome.owner.as_deref(),
            Some("operator"),
            "state {state} must be annotatable"
        );
        assert_eq!(outcome.severity.as_deref(), Some("sev1"));
    }
}

// AC6: an unknown execution id is rejected with NotFound (-> 404 at the HTTP
// layer).
#[tokio::test]
async fn unknown_execution_id_returns_not_found() {
    let (url, _container) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;

    let missing = ExecutionId::new();
    let err = annotate_workflow_execution(&mut conn, missing, empty_patch())
        .await
        .expect_err("unknown execution id must error");
    assert!(
        matches!(err, autumn_harvest::HarvestError::NotFound(_)),
        "expected NotFound, got {err:?}"
    );
}
