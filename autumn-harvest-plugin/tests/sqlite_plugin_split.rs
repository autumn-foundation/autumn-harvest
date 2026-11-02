//! A SQLite app with a Postgres Harvest database (issue #2472).
//!
//! The app database is a SQLite file. `harvest.mode` is `split` or
//! `external`, so Harvest runs the Postgres path on `harvest.database.url`.
//! The test checks that a workflow runs there, and that no Harvest file and
//! no Harvest table reach the SQLite side.
//!
//! The suite prefers `HARVEST_TEST_DATABASE_URL` (a server where it may
//! create databases). Otherwise it starts a Postgres container, so it needs
//! Docker. CI runs it in the `sqlite-plugin-split` job:
//!
//! ```bash
//! cargo test -p autumn-harvest-plugin --features sqlite --test sqlite_plugin_split
//! ```

#![cfg(feature = "sqlite")]
// The `#[workflow]` and `#[activity]` macros need an `async fn`.
#![allow(clippy::unused_async)]

use std::path::Path;
use std::time::{Duration, Instant};

use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestPlugin;
use autumn_web::config::AutumnConfig;
use autumn_web::test::{TestApp, TestClient};
use diesel::sql_types::BigInt;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

#[activity]
async fn shout(_ctx: &ActivityContext, name: String) -> Result<String, String> {
    Ok(name.to_uppercase())
}

#[workflow]
async fn greet(ctx: &WorkflowContext, name: String) -> Result<Value, String> {
    let shouted: String = ctx
        .execute_activity(&shout_info(), name)
        .await
        .map_err(|error| error.to_string())?;
    let go = ctx
        .wait_for_signal("go")
        .await
        .map_err(|error| error.to_string())?;
    Ok(json!({ "shouted": shouted, "go": go }))
}

/// One empty Postgres database for one app.
struct HarvestDb {
    url: String,
    _container: Option<ContainerAsync<Postgres>>,
}

async fn harvest_db() -> HarvestDb {
    if let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let name = format!("harvest_split_{}", uuid::Uuid::new_v4().simple());
        let mut admin = AsyncPgConnection::establish(&admin_url)
            .await
            .expect("connect admin");
        diesel::sql_query(format!("CREATE DATABASE \"{name}\""))
            .execute(&mut admin)
            .await
            .expect("create test database");
        let (base, _) = admin_url.rsplit_once('/').expect("a database segment");
        return HarvestDb {
            url: format!("{base}/{name}"),
            _container: None,
        };
    }
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    HarvestDb {
        url: format!("postgres://postgres:postgres@{host}:{port}/postgres"),
        _container: Some(container),
    }
}

/// Set the Harvest environment for one app. The suite has one test, so no
/// other test reads the environment at the same time.
fn set_env(mode: &str, url: &str) {
    // SAFETY: the suite runs one test, and it sets the environment before it
    // builds an app. No other thread reads it then.
    unsafe {
        std::env::set_var("AUTUMN_HARVEST__MODE", mode);
        std::env::set_var("AUTUMN_HARVEST_DATABASE__URL", url);
        // External mode turns the worker off by default.
        std::env::set_var("AUTUMN_HARVEST__WORKER_ENABLED", "true");
    }
}

fn app(dir: &Path) -> TestClient {
    let mut config = AutumnConfig::default();
    config.database.url = Some(format!("sqlite://{}", dir.join("app.db").display()));
    TestApp::new()
        .config(config)
        // `dev` applies the Harvest migrations to the Postgres database.
        .profile("dev")
        .plugin(
            HarvestPlugin::new()
                .workflows(workflows![greet])
                .activities(activities![shout])
                .api("/harvest"),
        )
        .build()
}

/// Poll `path` until `done` accepts the body, or fail after 30 s.
async fn wait_for(client: &TestClient, path: &str, done: impl Fn(u16, &Value) -> bool) -> Value {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let response = client.get(path).send().await;
        let status = response.status.as_u16();
        let body: Value = serde_json::from_str(&response.text()).unwrap_or(Value::Null);
        if done(status, &body) {
            return body;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting on {path}: {status} {body}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Start, signal and finish one run over the Postgres management API.
async fn run_one(client: &TestClient, workflow_id: &str) -> String {
    let health = client.get("/harvest/health").send().await;
    health.assert_ok();
    let health: Value = health.json();
    assert_ne!(health["backend"], "sqlite", "{health}");

    let started = client
        .post("/harvest/workflows/greet/start")
        .json(&json!({ "input": "bee", "workflow_id": workflow_id }))
        .send()
        .await;
    started.assert_status(201);
    let started: Value = started.json();
    let exec = started["execution_id"].as_str().unwrap().to_owned();

    // A signal sent before the run waits for it is buffered.
    let signal = client
        .post(&format!("/harvest/workflows/{exec}/signal/go"))
        .json(&json!({ "ok": true }))
        .send()
        .await;
    assert!(signal.status.is_success(), "signal: {}", signal.status);

    let result = wait_for(
        client,
        &format!("/harvest/workflows/{exec}/result"),
        |status, _| status == 200,
    )
    .await;
    assert_eq!(result["state"], "completed", "{result}");
    let output = &result["output"];
    assert_eq!(output["shouted"], "BEE", "{result}");
    assert_eq!(output["go"], json!({ "ok": true }), "{result}");
    exec
}

async fn count_executions(url: &str, workflow_id: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = BigInt)]
        n: i64,
    }
    let mut conn = AsyncPgConnection::establish(url).await.expect("connect");
    let rows: Vec<Count> = diesel::sql_query(
        "SELECT count(*) AS n FROM harvest_workflow_executions WHERE workflow_id = $1",
    )
    .bind::<diesel::sql_types::Text, _>(workflow_id)
    .load(&mut conn)
    .await
    .expect("count");
    rows[0].n
}

/// Each mode keeps Harvest in Postgres and nothing on the SQLite side.
#[tokio::test(flavor = "multi_thread")]
async fn a_sqlite_app_runs_its_workflows_on_a_postgres_harvest_database() {
    for mode in ["split", "external"] {
        let db = harvest_db().await;
        let dir = tempfile::tempdir().unwrap();
        set_env(mode, &db.url);
        let client = app(dir.path());

        let workflow_id = format!("w-{mode}");
        run_one(&client, &workflow_id).await;

        assert_eq!(count_executions(&db.url, &workflow_id).await, 1, "{mode}");
        assert!(
            !dir.path().join("app.harvest.db").exists(),
            "{mode}: no SQLite Harvest file"
        );
        drop(client);
    }
}
