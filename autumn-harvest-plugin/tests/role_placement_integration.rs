//! Autumn places the Harvest runtimes by role, end to end.
//!
//! Each test boots the real `HarvestPlugin` on a Postgres container with one
//! `AUTUMN_ROLE`, and no Harvest switch. It reads `/actuator/tasks` and runs
//! one workflow. A role that runs the worker completes the run. A role that
//! does not leaves it open.
//!
//! Requires Docker. Each test uses a multi-thread runtime, because
//! `TestApp::plugin` blocks on plugin startup.

#![allow(clippy::unused_async)]

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestPlugin;
use autumn_web::test::{TestApp, TestClient};
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::deadpool::Pool;
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

/// Echoes its input through one activity, so only a worker completes it.
#[workflow]
async fn rp_echo(ctx: &WorkflowContext, input: Value) -> Result<Value, String> {
    ctx.execute_activity_raw("rp_double", input, "default")
        .await
        .map_err(|e| e.to_string())
}

#[activity]
async fn rp_double(_ctx: &ActivityContext, input: Value) -> Result<Value, String> {
    Ok(json!({"twice": [input.clone(), input]}))
}

/// A migrated Postgres 16 container for one test.
struct TestPg {
    _container: ContainerAsync<Postgres>,
    url: String,
    pool: Pool<AsyncPgConnection>,
}

async fn setup_db() -> TestPg {
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
        .max_size(8)
        .runtime(deadpool::Runtime::Tokio1)
        .build()
        .expect("failed to build pool");
    TestPg {
        _container: container,
        url,
        pool,
    }
}

/// The app config of `role`, with the actuator tasks endpoint on.
fn app_config(db: &TestPg, role: &str) -> autumn_web::config::AutumnConfig {
    let mut config: autumn_web::config::AutumnConfig = toml::from_str(&format!(
        "role = \"{role}\"\nprofile = \"test\"\n\
         [security.csrf]\nenabled = false\n\
         [actuator]\nsensitive = true\n\
         [jobs]\nbackend = \"postgres\"\n{PRESETS}"
    ))
    .expect("the config parses");
    config.database.url = Some(db.url.clone());
    config
}

/// Two named roles. `cron` turns the Harvest worker off and keeps Autumn
/// job workers. `flows` keeps the Harvest worker with no Autumn job worker.
const PRESETS: &str = "\
[roles.cron]
base = \"combined\"
capabilities = { \"harvest.worker\" = false, \"harvest.scheduler\" = true }

[roles.flows]
base = \"combined\"
job_workers = false
capabilities = { \"harvest.worker\" = true }
";

fn build(db: &TestPg, role: &str) -> TestClient {
    let plugin = HarvestPlugin::new()
        .workflows(vec![__autumn_workflow_info_rp_echo()])
        .activities(activities![rp_double])
        .worker(WorkerConfig::default())
        .api("/api/harvest")
        .allow_unauthenticated_mutations();
    TestApp::new()
        .config(app_config(db, role))
        .plugin(plugin)
        .with_db(db.pool.clone())
        .build()
}

/// `background_tasks` of `/actuator/tasks`, by task name.
async fn fetch_tasks(client: &TestClient) -> BTreeMap<String, Value> {
    let body: Value = client.get("/actuator/tasks").send().await.json();
    serde_json::from_value(body["background_tasks"].clone()).expect("a task map")
}

/// The Harvest tasks that run on this process.
fn running(tasks: &BTreeMap<String, Value>) -> Vec<String> {
    tasks
        .iter()
        .filter(|(name, task)| name.starts_with("harvest.") && task["status"] == "running")
        .map(|(name, _)| name.clone())
        .collect()
}

/// Wait until the run completes, or fail after 30 s.
async fn wait_for_completion(client: &TestClient, id: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let now = status(client, id).await;
        if now == "completed" {
            return;
        }
        assert!(Instant::now() < deadline, "the run did not complete: {now}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Start one run and return its id.
async fn start(client: &TestClient) -> String {
    let started: Value = client
        .post("/api/harvest/workflows/rp_echo/start")
        .json(&json!({"input": "hi"}))
        .send()
        .await
        .json();
    started["execution_id"]
        .as_str()
        .unwrap_or_else(|| panic!("no execution_id: {started}"))
        .to_owned()
}

/// The status of one run, in lower case.
async fn status(client: &TestClient, id: &str) -> String {
    let run: Value = client
        .get(&format!("/api/harvest/workflows/{id}"))
        .send()
        .await
        .json();
    run["status"]
        .as_str()
        .unwrap_or_else(|| panic!("no status: {run}"))
        .to_ascii_lowercase()
}

/// A `combined` role runs every Harvest task, and its worker completes the
/// run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_combined_role_runs_each_harvest_task_and_completes_a_run() {
    let db = setup_db().await;
    let client = build(&db, "combined");
    let tasks = fetch_tasks(&client).await;
    assert_eq!(
        running(&tasks),
        [
            "harvest.batch",
            "harvest.queues",
            "harvest.retention",
            "harvest.scheduler",
            "harvest.worker"
        ],
        "{tasks:?}"
    );
    let id = start(&client).await;
    wait_for_completion(&client, &id).await;
    // A task with no prepared loop fails after its start. Each task still
    // runs, so the runner gave each one its loop.
    let tasks = fetch_tasks(&client).await;
    assert_eq!(running(&tasks).len(), 5, "{tasks:?}");
}

/// A named role turns the Harvest worker off and keeps the rest. Autumn
/// job workers stay on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_named_role_turns_off_the_harvest_worker() {
    let db = setup_db().await;
    let client = build(&db, "cron");
    let tasks = fetch_tasks(&client).await;
    assert_eq!(
        running(&tasks),
        ["harvest.batch", "harvest.retention", "harvest.scheduler"],
        "{tasks:?}"
    );
    assert_eq!(tasks["harvest.worker"]["status"], "not_placed");
    let id = start(&client).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_ne!(status(&client, &id).await, "completed");
}

/// The reverse: a named role with no Autumn job worker runs the Harvest
/// worker, and completes the run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_named_role_runs_the_harvest_worker_without_job_workers() {
    let db = setup_db().await;
    let client = build(&db, "flows");
    let tasks = fetch_tasks(&client).await;
    assert_eq!(tasks["harvest.worker"]["status"], "running", "{tasks:?}");
    // Autumn places a task with queues only where job workers run.
    assert_eq!(tasks["harvest.queues"]["status"], "not_placed");
    let id = start(&client).await;
    wait_for_completion(&client, &id).await;
}

/// A `web` role runs no Harvest task. It starts a run, but no worker here
/// claims it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_web_role_runs_no_harvest_task_and_leaves_the_run_open() {
    let db = setup_db().await;
    let client = build(&db, "web");
    let tasks = fetch_tasks(&client).await;
    assert_eq!(running(&tasks), Vec::<String>::new(), "{tasks:?}");
    for name in [
        "harvest.worker",
        "harvest.scheduler",
        "harvest.retention",
        "harvest.batch",
        "harvest.queues",
    ] {
        assert_eq!(tasks[name]["status"], "not_placed", "{name}");
        assert_eq!(tasks[name]["runs_here"], false, "{name}");
    }
    let id = start(&client).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_ne!(status(&client, &id).await, "completed");
}
