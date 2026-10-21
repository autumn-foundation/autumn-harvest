//! A durable route publishes the dispatch hint of its start.
//!
//! The dispatch channel is process-global, so this test has its own binary.
//! The app runs no worker, so no reconcile sweep publishes the task row. Only
//! the route's call to `finish()` after the start can publish it.
//!
//! Requires Docker.

// The `#[workflow]` macro reads the unused input parameter.
#![allow(clippy::used_underscore_binding)]

use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::dispatch::{DispatchSettings, MemoryDispatch};
use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestPlugin;
use autumn_harvest_plugin::durable_route::{DurableOutcome, DurableRoute};
use autumn_web::prelude::*;
use autumn_web::test::TestApp;
use diesel::QueryableByName;
use diesel::sql_types::Text;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

/// Waits for a signal that never comes.
#[workflow]
async fn dr_dispatch_parked(ctx: &WorkflowContext, _input: Value) -> Result<Value, String> {
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// Starts `dr_dispatch_parked` and answers at once.
#[post("/durable")]
async fn durable(durable: DurableRoute, Json(body): Json<Value>) -> DurableOutcome {
    durable
        .unscoped()
        .call("dr_dispatch_parked", body)
        .wait(Duration::ZERO)
        .expose_execution_id()
        .send()
        .await
}

#[derive(QueryableByName)]
struct Id {
    #[diesel(sql_type = Text)]
    id: String,
}

// AC-90
#[tokio::test(flavor = "multi_thread")]
async fn the_start_publishes_its_dispatch_hint() {
    let container = Postgres::default()
        .with_init_sql(autumn_harvest::test_init_sql().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    autumn_web::migrate::run_pending(&url, autumn_web::migrate::FRAMEWORK_MIGRATIONS)
        .expect("failed to run framework migrations");
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(&url);
    let pool = Pool::builder(manager)
        .max_size(4)
        .runtime(deadpool::Runtime::Tokio1)
        .build()
        .expect("failed to build pool");
    let config = autumn_web::config::AutumnConfig {
        profile: Some("test".into()),
        security: autumn_web::security::SecurityConfig {
            csrf: autumn_web::security::CsrfConfig {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        },
        database: autumn_web::config::DatabaseConfig {
            url: Some(url.clone()),
            ..Default::default()
        },
        ..Default::default()
    };
    // No worker runs, so nothing else publishes the task row.
    let plugin = HarvestPlugin::new().workflows(vec![__autumn_workflow_info_dr_dispatch_parked()]);
    let client = TestApp::new()
        .config(config)
        .routes(vec![__autumn_route_info_durable()])
        .plugin(plugin)
        .with_db(pool.clone())
        .build();
    // Plugin startup installs its own dispatch topology, so the test channel
    // goes in after it.
    let channel = Arc::new(MemoryDispatch::new());
    autumn_harvest::dispatch::install(channel.clone(), DispatchSettings::default());

    let response = client
        .post("/durable")
        .header("idempotency-key", "key-ac91")
        .json(&json!({"tag": "ac91"}))
        .send()
        .await;
    assert_eq!(response.status.as_u16(), 202, "{}", response.text());
    let exec_id = response
        .header("x-harvest-execution-id")
        .expect("execution id header")
        .to_owned();
    let mut conn = pool.get().await.expect("conn");
    let tasks = diesel::sql_query(
        "SELECT id::text AS id FROM harvest_task_queue WHERE workflow_exec_id = $1::uuid",
    )
    .bind::<Text, _>(&exec_id)
    .load::<Id>(&mut conn)
    .await
    .expect("task rows");
    assert!(!tasks.is_empty(), "the start queued a workflow task");
    let published: Vec<String> = channel
        .published_ids()
        .iter()
        .map(ToString::to_string)
        .collect();
    assert!(
        tasks.iter().any(|task| published.contains(&task.id)),
        "the start published its task: {published:?}"
    );
}
