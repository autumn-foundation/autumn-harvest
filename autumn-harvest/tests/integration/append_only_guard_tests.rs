#![cfg(feature = "db")]
//! The database guard on append-only `harvest_events` (issue #1817).
//!
//! A `BEFORE UPDATE` trigger rejects history rewrites. Only `event_data`
//! can change, and only when a sanctioned writer sets the
//! `harvest.sanctioned_event_rewrite` setting in its transaction.
//! The `cohort` column stays writable. Every other column is immutable.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres.
//! Otherwise each test starts its own container.

use autumn_harvest::WorkflowEvent;
use autumn_harvest::types::ExecutionId;
use diesel::sql_types::{BigInt, Text};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(autumn_harvest::test_init_sql().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url).await.expect("connect")
}

#[derive(diesel::QueryableByName)]
struct IdRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: uuid::Uuid,
}

#[derive(diesel::QueryableByName)]
struct TextRow {
    #[diesel(sql_type = Text)]
    v: String,
}

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

/// Seed one terminal execution with a two-event history.
async fn seed(conn: &mut AsyncPgConnection) -> uuid::Uuid {
    let id = diesel::sql_query(
        "INSERT INTO harvest_workflow_executions
            (workflow_name, workflow_id, shard_id, state, input, created_at, started_at, completed_at)
         VALUES ('append_only_wf', gen_random_uuid()::text, 0, 'COMPLETED', '{}'::jsonb,
                 NOW(), NOW(), NOW())
         RETURNING id",
    )
    .get_result::<IdRow>(conn)
    .await
    .expect("insert execution")
    .id;
    let events = vec![
        WorkflowEvent::MarkerRecorded {
            name: "first".into(),
            details: serde_json::json!({"secret": "pii"}),
        },
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({"status": "ok"}),
        },
    ];
    autumn_harvest::store::append_events(conn, ExecutionId::from_uuid(id), &events, 0)
        .await
        .expect("seed history");
    id
}

/// Run `stmt` against event 0 of `exec` and return the error text, if any.
async fn try_update(conn: &mut AsyncPgConnection, exec: uuid::Uuid, set: &str) -> Option<String> {
    diesel::sql_query(format!(
        "UPDATE harvest_events SET {set} WHERE workflow_exec_id = $1 AND event_id = 0"
    ))
    .bind::<diesel::sql_types::Uuid, _>(exec)
    .execute(conn)
    .await
    .err()
    .map(|e| e.to_string())
}

/// Run `set` inside a transaction that first sets the sanction to `value`.
async fn try_sanctioned_update(
    conn: &mut AsyncPgConnection,
    exec: uuid::Uuid,
    value: &str,
    set: &str,
) -> Option<String> {
    let sql = format!(
        "BEGIN;
         SELECT set_config('harvest.sanctioned_event_rewrite', '{value}', true);
         UPDATE harvest_events SET {set} WHERE workflow_exec_id = '{exec}' AND event_id = 0;
         COMMIT;"
    );
    let out = conn.batch_execute(&sql).await.err().map(|e| e.to_string());
    if out.is_some() {
        conn.batch_execute("ROLLBACK").await.ok();
    }
    out
}

async fn event_data(conn: &mut AsyncPgConnection, exec: uuid::Uuid) -> String {
    diesel::sql_query(
        "SELECT event_data::text AS v FROM harvest_events \
         WHERE workflow_exec_id = $1 AND event_id = 0",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec)
    .get_result::<TextRow>(conn)
    .await
    .expect("read event_data")
    .v
}

const REWRITE: &str = "event_data = jsonb_set(event_data, '{data,details}', '\"rewritten\"')";

fn assert_rejected(err: Option<String>, what: &str) {
    let msg = err.unwrap_or_else(|| panic!("{what}: the guard must reject this UPDATE"));
    assert!(
        msg.contains("append-only"),
        "{what}: the refusal must come from the append-only guard; got {msg}"
    );
}

#[tokio::test]
async fn a_plain_event_data_update_is_rejected() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    let exec = seed(&mut conn).await;
    let before = event_data(&mut conn, exec).await;

    assert_rejected(try_update(&mut conn, exec, REWRITE).await, "plain UPDATE");
    assert_eq!(
        before,
        event_data(&mut conn, exec).await,
        "the row must not change"
    );
}

#[tokio::test]
async fn each_sanctioned_writer_may_rewrite_event_data() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    for value in ["erase", "codec_rotation"] {
        let exec = seed(&mut conn).await;
        let err = try_sanctioned_update(&mut conn, exec, value, REWRITE).await;
        assert!(
            err.is_none(),
            "sanction {value} must allow the rewrite: {err:?}"
        );
        assert!(event_data(&mut conn, exec).await.contains("rewritten"));
    }
}

#[tokio::test]
async fn an_unknown_sanction_is_rejected() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    let exec = seed(&mut conn).await;
    for value in ["", "migration", "ERASE"] {
        assert_rejected(
            try_sanctioned_update(&mut conn, exec, value, REWRITE).await,
            &format!("sanction {value:?}"),
        );
    }
}

#[tokio::test]
async fn a_sanction_set_outside_a_transaction_does_not_reach_the_update() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    let exec = seed(&mut conn).await;
    diesel::sql_query("SELECT set_config('harvest.sanctioned_event_rewrite', 'erase', true)")
        .execute(&mut conn)
        .await
        .expect("set_config");
    assert_rejected(
        try_update(&mut conn, exec, REWRITE).await,
        "a transaction-local sanction from an earlier statement",
    );
}

#[tokio::test]
async fn identity_columns_stay_immutable_under_a_sanction() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    let other = seed(&mut conn).await;
    let exec = seed(&mut conn).await;
    for set in [
        "id = id + 1000000".to_string(),
        "event_id = 99".to_string(),
        "event_type = 'Forged'".to_string(),
        "timestamp = timestamp - INTERVAL '1 day'".to_string(),
        format!("workflow_exec_id = '{other}'"),
        // The event variant inside the payload is identity too.
        "event_data = jsonb_set(event_data, '{type}', '\"WorkflowFailed\"')".to_string(),
    ] {
        assert_rejected(try_update(&mut conn, exec, &set).await, &set);
        assert_rejected(
            try_sanctioned_update(&mut conn, exec, "erase", &set).await,
            &format!("{set} under a sanction"),
        );
    }
}

#[tokio::test]
async fn the_cohort_column_stays_writable() {
    // `disable_partitioning` resets `cohort` on the flat layout. The
    // column is storage placement, not history, so it is sanctioned.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    let exec = seed(&mut conn).await;
    let err = try_update(&mut conn, exec, "cohort = '-infinity'::timestamptz").await;
    assert!(
        err.is_none(),
        "a cohort update must pass the guard: {err:?}"
    );
}

#[tokio::test]
async fn the_guard_trigger_is_installed() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    let n = diesel::sql_query(
        "SELECT count(*) AS n FROM pg_trigger tg JOIN pg_class c ON c.oid = tg.tgrelid \
         WHERE c.relname = 'harvest_events' AND tg.tgname = 'harvest_events_append_only_trg'",
    )
    .get_result::<CountRow>(&mut conn)
    .await
    .expect("count triggers")
    .n;
    assert_eq!(
        n, 1,
        "the migration must install the guard on harvest_events"
    );
}
