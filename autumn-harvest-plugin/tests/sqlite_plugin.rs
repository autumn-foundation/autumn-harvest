//! `HarvestPlugin` on the SQLite backend, end to end (issue #2467).
//!
//! Each test boots a real app through `TestApp`, with the app database and
//! the Harvest file in a temp directory. No Postgres and no Docker run.
//!
//! Run it with `cargo test -p autumn-harvest-plugin --features sqlite --test
//! sqlite_plugin`.

#![cfg(feature = "sqlite")]
// The `#[workflow]` and `#[activity]` macros need an `async fn`.
#![allow(clippy::unused_async)]

use std::path::Path;
use std::time::{Duration, Instant};

use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestPlugin;
use autumn_harvest_plugin::sqlite::{HarvestSqlite, StartOptions};
use autumn_web::AppState;
use autumn_web::config::AutumnConfig;
use autumn_web::test::{TestApp, TestClient};
use serde_json::{Value, json};

/// A value given to `HarvestPlugin::state`. The activity reads it back.
#[derive(Debug)]
struct Prefix(&'static str);

#[activity]
async fn shout(ctx: &ActivityContext, name: String) -> Result<String, String> {
    let prefix = ctx.state::<Prefix>().map_or("", |prefix| prefix.0);
    // The plugin adds `AppState`, as on Postgres, so an activity can reach
    // the app's pool or fleet without a global.
    let app = ctx.state::<AppState>().is_some();
    Ok(format!("{prefix}{} app={app}", name.to_uppercase()))
}

#[workflow]
async fn greet(ctx: &WorkflowContext, name: String) -> Result<Value, String> {
    ctx.register_query("stage", || json!("registered"));
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

fn config(dir: &Path) -> AutumnConfig {
    let mut config = AutumnConfig::default();
    config.database.url = Some(format!("sqlite://{}", dir.join("app.db").display()));
    config
}

fn plugin() -> HarvestPlugin {
    HarvestPlugin::new()
        .workflows(workflows![greet])
        .activities(activities![shout])
        .state(Prefix("hi "))
        .api("/harvest")
}

fn app(dir: &Path, profile: &str) -> TestClient {
    TestApp::new()
        .config(config(dir))
        .profile(profile)
        .plugin(plugin())
        .build()
}

/// Poll `path` until `done` accepts the body, or fail after 10 s.
async fn wait_for(client: &TestClient, path: &str, done: impl Fn(&Value) -> bool) -> Value {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let response = client.get(path).send().await;
        if response.status.is_success() {
            let body: Value = response.json();
            if done(&body) {
                return body;
            }
        }
        assert!(Instant::now() < deadline, "timed out waiting on {path}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn start(client: &TestClient, body: &Value) -> String {
    let response = client
        .post("/harvest/workflows/greet/start")
        .json(body)
        .send()
        .await;
    response.assert_status(201);
    let body: Value = response.json();
    body["execution_id"].as_str().unwrap().to_owned()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_workflow_runs_end_to_end_over_http() {
    let dir = tempfile::tempdir().unwrap();
    let client = app(dir.path(), "dev");

    let health = client.get("/harvest/health").send().await;
    health.assert_ok();
    let health: Value = health.json();
    assert_eq!(health["backend"], "sqlite");
    assert!(
        dir.path().join("app.harvest.db").exists(),
        "the Harvest file sits next to the app database"
    );

    let exec = start(&client, &json!({ "input": "bee", "workflow_id": "w-1" })).await;

    // The query answers once the run has started and registered it.
    let stage = wait_for(
        &client,
        &format!("/harvest/workflows/{exec}/query/stage"),
        |_| true,
    )
    .await;
    assert_eq!(stage, json!("registered"));

    let signal = client
        .post(&format!("/harvest/workflows/{exec}/signal/go"))
        .header("Idempotency-Key", "go-1")
        .json(&json!({ "ok": true }))
        .send()
        .await;
    signal.assert_ok();
    let signal: Value = signal.json();
    assert_eq!(signal["delivered"], true);
    // The same key again is not delivered twice.
    let again = client
        .post(&format!("/harvest/workflows/{exec}/signal/go"))
        .header("Idempotency-Key", "go-1")
        .json(&json!({ "ok": true }))
        .send()
        .await;
    let again: Value = again.json();
    assert_eq!(again["delivered"], false);

    let result = wait_for(
        &client,
        &format!("/harvest/workflows/{exec}/result"),
        |body| body["state"] == "COMPLETED",
    )
    .await;
    assert_eq!(result["output"]["shouted"], "hi BEE app=true");
    assert_eq!(result["output"]["go"], json!({ "ok": true }));

    let detail: Value = client
        .get(&format!("/harvest/workflows/{exec}"))
        .send()
        .await
        .json();
    assert_eq!(detail["execution"]["workflow_id"], "w-1");
    assert_eq!(detail["execution"]["input"], "bee");

    let by_id: Value = client
        .get("/harvest/workflows/by-id/greet/w-1")
        .send()
        .await
        .json();
    assert_eq!(by_id["execution"]["id"], exec);

    let list: Value = client
        .get("/harvest/workflows?workflow_name=greet&state=completed")
        .send()
        .await
        .json();
    assert_eq!(list.as_array().map(Vec::len), Some(1));

    let history: Value = client
        .get(&format!("/harvest/workflows/{exec}/history"))
        .send()
        .await
        .json();
    let types: Vec<&str> = history["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|event| event["type"].as_str())
        .collect();
    assert_eq!(types.first(), Some(&"WorkflowStarted"));
    assert_eq!(types.last(), Some(&"WorkflowCompleted"));

    let registered: Value = client
        .get("/harvest/workflows/registered")
        .send()
        .await
        .json();
    assert_eq!(registered, json!([{ "name": "greet" }]));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unported_route_answers_501_and_names_the_epic() {
    let dir = tempfile::tempdir().unwrap();
    let client = app(dir.path(), "dev");
    let response = client.get("/harvest/admin/status").send().await;
    response.assert_status(501);
    response.assert_body_contains("issues/2466");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unsupported_start_option_is_a_400_not_a_silent_drop() {
    let dir = tempfile::tempdir().unwrap();
    let client = app(dir.path(), "dev");
    let response = client
        .post("/harvest/workflows/greet/start")
        .json(&json!({ "input": "bee", "queue": "fast", "start_at": "2030-01-01T00:00:00Z" }))
        .send()
        .await;
    response.assert_status(400);
    response.assert_body_contains("queue");
    response.assert_body_contains("start_at");

    // Postgres dedupes a start on this header. This backend does not, so a
    // retry must not silently start a second run.
    let response = client
        .post("/harvest/workflows/greet/start")
        .header("Idempotency-Key", "k-1")
        .json(&json!({ "input": "bee" }))
        .send()
        .await;
    response.assert_status(400);
    response.assert_body_contains("Idempotency-Key");
    let runs: Value = client.get("/harvest/workflows").send().await.json();
    assert_eq!(runs, json!([]), "a refused start writes nothing");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_anonymous_mutation_outside_dev_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    // `test` is not `dev`, so the #1802 gate is closed. `prod` would also
    // check the `Host` header, which this test does not exercise.
    let client = app(dir.path(), "test");
    let response = client
        .post("/harvest/workflows/greet/start")
        .json(&json!({ "input": "bee" }))
        .send()
        .await;
    response.assert_status(401);
    // A read is not a mutation, so it passes the gate.
    client.get("/harvest/workflows").send().await.assert_ok();
}

#[tokio::test(flavor = "multi_thread")]
async fn app_code_gets_the_handle_from_the_app_state() {
    let dir = tempfile::tempdir().unwrap();
    let client = app(dir.path(), "dev");
    let handle = client
        .state()
        .extension::<HarvestSqlite>()
        .expect("the startup hook installs the handle");
    let started = handle
        .start(
            "greet",
            json!("ant"),
            StartOptions::new().workflow_id("w-app"),
        )
        .await
        .unwrap();
    assert!(started.created);
    handle
        .signal_by_workflow_id("greet", "w-app", "go", json!(1), None)
        .await
        .unwrap();
    let result = wait_for(
        &client,
        &format!("/harvest/workflows/{}/result", started.exec_id),
        |body| body["state"] == "COMPLETED",
    )
    .await;
    assert_eq!(result["output"]["shouted"], "hi ANT app=true");
}

#[test]
fn an_option_with_no_sqlite_port_fails_the_build_and_names_its_issue() {
    let dir = tempfile::tempdir().unwrap();
    let built = std::panic::catch_unwind(|| {
        TestApp::new()
            .config(config(dir.path()))
            .plugin(plugin().enable_api_tokens())
    });
    let payload = built.err().expect("the build refuses the option");
    let message = payload
        .downcast_ref::<String>()
        .cloned()
        .unwrap_or_default();
    assert!(message.contains("enable_api_tokens (#2469)"), "{message}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restart_resumes_the_run_from_the_harvest_file() {
    let dir = tempfile::tempdir().unwrap();
    let exec = {
        let client = app(dir.path(), "dev");
        let exec = start(
            &client,
            &json!({ "input": "wasp", "workflow_id": "w-restart" }),
        )
        .await;
        // Wait until the run is parked on the signal, past its activity.
        wait_for(
            &client,
            &format!("/harvest/workflows/{exec}/history"),
            |body| {
                body["events"].as_array().is_some_and(|events| {
                    events
                        .iter()
                        .any(|event| event["type"] == "ActivityCompleted")
                })
            },
        )
        .await;
        // `TestApp` runs no shutdown hook, so stop the driver as the hook
        // would. It returns once the writer lock is free.
        let handle = client.state().extension::<HarvestSqlite>().unwrap();
        handle.shutdown().await.unwrap();
        assert!(matches!(
            handle
                .start("greet", json!("late"), StartOptions::default())
                .await,
            Err(autumn_harvest_plugin::sqlite::HarvestSqliteError::Stopped)
        ));
        exec
    };

    // The lock is free, so a second runtime can open the file.
    let harvest_file = dir.path().join("app.harvest.db");
    drop(autumn_harvest_sqlite::SqliteRuntime::open(&harvest_file).expect("the lock is free"));

    let client = app(dir.path(), "dev");
    client
        .post(&format!("/harvest/workflows/{exec}/signal/go"))
        .json(&json!("after restart"))
        .send()
        .await
        .assert_ok();
    let result = wait_for(
        &client,
        &format!("/harvest/workflows/{exec}/result"),
        |body| body["state"] == "COMPLETED",
    )
    .await;
    assert_eq!(result["output"]["shouted"], "hi WASP app=true");
    assert_eq!(result["output"]["go"], "after restart");
}

#[tokio::test(flavor = "multi_thread")]
async fn sqlite_path_places_the_file_for_an_in_memory_app_database() {
    let dir = tempfile::tempdir().unwrap();
    let harvest_file = dir.path().join("custom.harvest.db");
    let mut config = AutumnConfig::default();
    config.database.url = Some("sqlite::memory:".to_owned());
    let client = TestApp::new()
        .config(config)
        .profile("dev")
        .plugin(plugin().sqlite_path(&harvest_file))
        .build();
    let health: Value = client.get("/harvest/health").send().await.json();
    assert_eq!(health["path"], harvest_file.display().to_string());
    assert!(harvest_file.exists());
}
