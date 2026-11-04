//! The transactional outbox on a SQLite app (issue #2470).
//!
//! App code writes a start in its own business transaction. The relay
//! starts the run. A rolled-back start never runs (AC1). A committed start
//! runs exactly once, across a crash in each window of the relay (AC2).
//!
//! `TestApp` runs no shutdown hook. A test stops the driver with
//! `HarvestSqlite::shutdown`, as a crash would leave it. The next app on the
//! same directory is the restart.

use std::path::Path;
use std::time::{Duration, Instant};

use autumn_harvest::prelude::*;
use autumn_harvest_plugin::sqlite::{
    ExecutionFilter, HarvestSqlite, StartOptions, enqueue_workflow_start_outbox,
};
use autumn_harvest_plugin::{
    HarvestPlugin, WorkflowStartRequest, drain_workflow_start_outbox_once,
    flush_workflow_start_outbox,
};
use autumn_web::AppState;
use autumn_web::db::RuntimeConnection;
use autumn_web::test::{TestApp, TestClient};
use diesel::sql_types::{BigInt, Nullable, Text};
use diesel_async::{AsyncConnection as _, RunQueryDsl as _, SimpleAsyncConnection as _};
use serde_json::json;

#[activity]
async fn pack(_ctx: &ActivityContext, order: String) -> Result<String, String> {
    Ok(format!("packed {order}"))
}

/// Packs one order and completes. It waits for nothing.
#[workflow]
async fn ship(ctx: &WorkflowContext, order: String) -> Result<String, String> {
    ctx.execute_activity(&pack_info(), order)
        .await
        .map_err(|error| error.to_string())
}

/// An app with a pool on its database. `TestApp` builds no pool from the
/// config, and a real boot does.
fn app(dir: &Path) -> TestClient {
    let config = super::config(dir);
    let pool = autumn_web::db::create_pool(&config.database)
        .expect("a pool")
        .expect("a SQLite URL");
    TestApp::new()
        .config(config)
        .with_db(pool)
        .profile("dev")
        .plugin(
            HarvestPlugin::new()
                .workflows(workflows![ship])
                .activities(activities![pack])
                .api("/harvest"),
        )
        .build()
}

fn request(workflow_id: &str) -> WorkflowStartRequest {
    WorkflowStartRequest {
        workflow_name: "ship".to_owned(),
        workflow_id: workflow_id.to_owned(),
        queue_name: "default".to_owned(),
        input: json!(workflow_id),
        memo: Some(json!({ "source": "outbox" })),
        search_attrs: Some(json!({ "channel": "web" })),
    }
}

async fn conn(state: &AppState) -> impl std::ops::DerefMut<Target = RuntimeConnection> {
    state
        .pool()
        .expect("the app has a database")
        .get()
        .await
        .expect("an app connection")
}

/// Write an order and its start in one business transaction. `commit`
/// false rolls both back.
async fn place_order(state: &AppState, order: &str, commit: bool) {
    let mut conn = conn(state).await;
    conn.batch_execute("CREATE TABLE IF NOT EXISTS orders (id TEXT PRIMARY KEY)")
        .await
        .unwrap();
    let conn: &mut RuntimeConnection = &mut conn;
    let outcome = conn
        .transaction::<(), diesel::result::Error, _>(async |conn| {
            diesel::sql_query("INSERT INTO orders (id) VALUES (?)")
                .bind::<Text, _>(order)
                .execute(conn)
                .await?;
            enqueue_workflow_start_outbox(conn, &request(order)).await?;
            if commit {
                Ok(())
            } else {
                Err(diesel::result::Error::RollbackTransaction)
            }
        })
        .await;
    assert_eq!(outcome.is_ok(), commit, "{outcome:?}");
}

#[derive(Debug, diesel::QueryableByName)]
struct OutboxRow {
    #[diesel(sql_type = BigInt)]
    delivery_attempts: i64,
    #[diesel(sql_type = Nullable<Text>)]
    last_error: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    delivered_execution_id: Option<String>,
    #[diesel(sql_type = Nullable<BigInt>)]
    delivered_at: Option<i64>,
    #[diesel(sql_type = BigInt)]
    next_attempt_at: i64,
}

async fn outbox_row(state: &AppState, workflow_id: &str) -> Option<OutboxRow> {
    let mut conn = conn(state).await;
    diesel::sql_query(
        "SELECT delivery_attempts, last_error, delivered_execution_id, delivered_at, \
         next_attempt_at FROM harvest_workflow_outbox WHERE workflow_id = ?",
    )
    .bind::<Text, _>(workflow_id)
    .get_result(&mut *conn)
    .await
    .ok()
}

async fn count(state: &AppState, sql: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = BigInt)]
        n: i64,
    }
    let mut conn = conn(state).await;
    diesel::sql_query(sql)
        .get_result::<Count>(&mut *conn)
        .await
        .map_or(0, |row| row.n)
}

/// Every run of `ship` with the business id `workflow_id`, newest first.
async fn runs(client: &TestClient, workflow_id: &str) -> Vec<String> {
    let handle = client.state().extension::<HarvestSqlite>().unwrap();
    let filter = ExecutionFilter::new()
        .workflow_name("ship")
        .workflow_id(workflow_id.to_owned());
    handle
        .list(filter)
        .await
        .unwrap()
        .executions
        .iter()
        .filter_map(|run| run.execution_id().map(|exec| exec.to_string()))
        .collect()
}

/// Poll until the row is delivered and its run is `COMPLETED`. Return the
/// execution id.
async fn wait_delivered_and_completed(client: &TestClient, workflow_id: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(exec) = outbox_row(client.state(), workflow_id)
            .await
            .and_then(|row| row.delivered_execution_id)
        {
            let body = super::wait_for(client, &format!("/harvest/workflows/{exec}"), |body| {
                body["execution"]["state"] == "COMPLETED"
            })
            .await;
            assert_eq!(
                body["execution"]["output"],
                json!(format!("packed {workflow_id}"))
            );
            return exec;
        }
        assert!(Instant::now() < deadline, "{workflow_id} was not delivered");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Stop the driver, as a crash would. The relay of this app then cannot
/// start a run.
async fn crash(client: &TestClient) {
    let handle = client.state().extension::<HarvestSqlite>().unwrap();
    handle.shutdown().await.unwrap();
}

/// AC1: a start in a rolled-back business transaction never runs. A start
/// in a committed one runs, with its order.
#[tokio::test(flavor = "multi_thread")]
async fn a_start_in_a_rolled_back_transaction_never_runs() {
    let dir = tempfile::tempdir().unwrap();
    let client = app(dir.path());
    let state = client.state();

    place_order(state, "o-rolled-back", false).await;
    assert_eq!(flush_workflow_start_outbox(state).await.unwrap(), 0);
    assert_eq!(count(state, "SELECT COUNT(*) AS n FROM orders").await, 0);
    assert!(outbox_row(state, "o-rolled-back").await.is_none());
    assert_eq!(runs(&client, "o-rolled-back").await, Vec::<String>::new());

    place_order(state, "o-committed", true).await;
    // The top-level drain reaches the SQLite outbox. It races the
    // background relay, so either one can deliver the row. The exact counts
    // are in the unit tests of `sqlite::outbox`, which run no relay.
    drain_workflow_start_outbox_once(state, 10).await.unwrap();
    let exec = wait_delivered_and_completed(&client, "o-committed").await;
    assert_eq!(count(state, "SELECT COUNT(*) AS n FROM orders").await, 1);
    assert_eq!(
        runs(&client, "o-committed").await,
        std::slice::from_ref(&exec)
    );

    // The run keeps the memo and the search attributes of the request.
    let detail: serde_json::Value = client
        .get(&format!("/harvest/workflows/{exec}"))
        .send()
        .await
        .json();
    assert_eq!(detail["execution"]["memo"], json!({ "source": "outbox" }));
    assert_eq!(
        detail["execution"]["search_attrs"],
        json!({ "channel": "web" })
    );

    // A rolled-back start stays invisible after the relay runs too.
    assert_eq!(runs(&client, "o-rolled-back").await, Vec::<String>::new());
}

/// AC2, first window: the process dies after the commit and before the
/// relay starts the run. The next boot runs it once.
#[tokio::test(flavor = "multi_thread")]
async fn a_committed_start_survives_a_crash_before_the_relay() {
    let dir = tempfile::tempdir().unwrap();
    {
        let client = app(dir.path());
        crash(&client).await;
        place_order(client.state(), "o-crash-1", true).await;
        // The relay of the dead process cannot deliver it.
        let row = outbox_row(client.state(), "o-crash-1").await.unwrap();
        assert_eq!(row.delivered_at, None);
    }

    let client = app(dir.path());
    let exec = wait_delivered_and_completed(&client, "o-crash-1").await;
    assert_eq!(runs(&client, "o-crash-1").await, [exec]);
    let row = outbox_row(client.state(), "o-crash-1").await.unwrap();
    assert_eq!(row.delivery_attempts, 1, "{row:?}");
    assert_eq!(row.last_error, None);
}

/// AC2, second window: the process dies after the start commits in the
/// Harvest file and before the relay marks the row. The next boot marks the
/// row with that run, and starts no second run.
#[tokio::test(flavor = "multi_thread")]
async fn a_committed_start_survives_a_crash_between_the_start_and_the_mark() {
    let dir = tempfile::tempdir().unwrap();
    let first = {
        let client = app(dir.path());
        crash(&client).await;
        place_order(client.state(), "o-crash-2", true).await;
        // The start the relay sent before the crash. The driver is gone, so
        // a runtime of its own writes it, as the driver would.
        let mut runtime =
            autumn_harvest_sqlite::SqliteRuntime::open(dir.path().join("app.harvest.db")).unwrap();
        runtime.register_workflow(&ship_info());
        let started = runtime
            .start_workflow_with_options(
                "ship",
                json!("o-crash-2"),
                StartOptions::new().workflow_id("o-crash-2"),
            )
            .unwrap();
        assert!(started.created);
        started.exec_id.to_string()
    };

    let client = app(dir.path());
    let exec = wait_delivered_and_completed(&client, "o-crash-2").await;
    assert_eq!(exec, first, "the relay marks the run that already started");
    assert_eq!(runs(&client, "o-crash-2").await, [first]);
}

/// A redelivery over a run that an operator terminated does not run the
/// start again.
#[tokio::test(flavor = "multi_thread")]
async fn a_redelivery_over_a_terminated_run_does_not_run_it_again() {
    let dir = tempfile::tempdir().unwrap();
    let first = {
        let client = app(dir.path());
        crash(&client).await;
        place_order(client.state(), "o-term", true).await;
        let mut runtime =
            autumn_harvest_sqlite::SqliteRuntime::open(dir.path().join("app.harvest.db")).unwrap();
        runtime.register_workflow(&ship_info());
        let started = runtime
            .start_workflow_with_options(
                "ship",
                json!("o-term"),
                StartOptions::new().workflow_id("o-term"),
            )
            .unwrap();
        runtime.terminate(started.exec_id, "operator").unwrap();
        started.exec_id.to_string()
    };

    let client = app(dir.path());
    let deadline = Instant::now() + Duration::from_secs(15);
    let row = loop {
        let row = outbox_row(client.state(), "o-term").await.unwrap();
        if row.delivered_at.is_some() {
            break row;
        }
        assert!(Instant::now() < deadline, "the row was not delivered");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(row.delivered_execution_id.as_deref(), Some(first.as_str()));
    assert_eq!(runs(&client, "o-term").await, [first]);
}

/// A start that cannot run keeps its error and backs off. A duplicate
/// enqueue of a delivered key is a no-op.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_start_backs_off_and_keeps_its_error() {
    let dir = tempfile::tempdir().unwrap();
    let client = app(dir.path());
    let state = client.state();
    let mut unknown = request("o-unknown");
    unknown.workflow_name = "no_such_workflow".to_owned();
    {
        let mut conn = conn(state).await;
        enqueue_workflow_start_outbox(&mut conn, &unknown)
            .await
            .unwrap();
    }
    let before = chrono::Utc::now().timestamp_millis();
    assert_eq!(flush_workflow_start_outbox(state).await.unwrap(), 0);
    let row = outbox_row(state, "o-unknown").await.unwrap();
    assert!(row.delivery_attempts >= 1, "{row:?}");
    assert!(
        row.last_error
            .as_deref()
            .is_some_and(|error| error.contains("no_such_workflow")),
        "{row:?}"
    );
    assert_eq!(row.delivered_at, None);
    assert!(row.next_attempt_at > before, "{row:?}");

    // A second enqueue of a delivered key starts nothing new.
    place_order(state, "o-once", true).await;
    let exec = wait_delivered_and_completed(&client, "o-once").await;
    {
        let mut conn = conn(state).await;
        enqueue_workflow_start_outbox(&mut conn, &request("o-once"))
            .await
            .unwrap();
    }
    assert_eq!(flush_workflow_start_outbox(state).await.unwrap(), 0);
    assert_eq!(runs(&client, "o-once").await, [exec]);
}

/// An app with no database has no outbox. A flush says what it needs, so
/// the app does not believe that its starts run.
#[tokio::test(flavor = "multi_thread")]
async fn a_flush_without_an_app_database_names_what_it_needs() {
    let dir = tempfile::tempdir().unwrap();
    let client = TestApp::new()
        .config(super::config(dir.path()))
        .profile("dev")
        .plugin(
            HarvestPlugin::new()
                .workflows(workflows![ship])
                .activities(activities![pack])
                .api("/harvest"),
        )
        .build();
    let error = flush_workflow_start_outbox(client.state())
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("embedded"), "{error}");
    assert!(error.contains("#2470"), "{error}");
}
