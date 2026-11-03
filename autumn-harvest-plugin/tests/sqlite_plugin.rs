//! `HarvestPlugin` on the SQLite backend, end to end (issue #2467).
//!
//! Each test boots a real app through `TestApp`, with the app database and
//! the Harvest file in a temp directory. No Postgres and no Docker run.
//!
//! Run it with `cargo test -p autumn-harvest-plugin --features sqlite,oidc
//! --test sqlite_plugin`. Without `oidc`, the OIDC checks do not run.

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

#[path = "support/wire_contract.rs"]
mod wire_contract;

#[path = "sqlite_plugin/wire_parity.rs"]
mod wire_parity;

#[path = "sqlite_plugin/read_parity.rs"]
mod read_parity;

#[path = "support/token_contract.rs"]
mod token_contract;

#[path = "support/role_contract.rs"]
mod role_contract;

#[cfg(feature = "oidc")]
#[path = "support/oidc_contract.rs"]
mod oidc_contract;

#[path = "sqlite_plugin/auth_parity.rs"]
mod auth_parity;

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

/// Poll the run until it is `COMPLETED`, and return its `execution`.
async fn wait_completed(client: &TestClient, exec: &str) -> Value {
    let body = wait_for(client, &format!("/harvest/workflows/{exec}"), |body| {
        body["execution"]["state"] == "COMPLETED"
    })
    .await;
    body["execution"].clone()
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
    signal.assert_status(202);
    let signal: Value = signal.json();
    assert_eq!(signal["signal_delivered"], true);
    // The same key again is not delivered twice.
    let again = client
        .post(&format!("/harvest/workflows/{exec}/signal/go"))
        .header("Idempotency-Key", "go-1")
        .json(&json!({ "ok": true }))
        .send()
        .await;
    let again: Value = again.json();
    assert_eq!(again["signal_delivered"], false);

    let result = wait_completed(&client, &exec).await;
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
        .get("/harvest/workflows?workflow_name=greet&state=COMPLETED")
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
    // The Postgres record: the schema fields are null when absent.
    assert_eq!(registered.as_array().map(Vec::len), Some(1));
    assert_eq!(registered[0]["name"], "greet");
    assert_eq!(registered[0]["input_schema"], Value::Null);
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
    let result = wait_completed(&client, &started.exec_id.to_string()).await;
    assert_eq!(result["output"]["shouted"], "hi ANT app=true");
}

#[test]
fn an_option_with_no_sqlite_port_fails_the_build_and_names_its_issue() {
    let dir = tempfile::tempdir().unwrap();
    let built = std::panic::catch_unwind(|| {
        TestApp::new()
            .config(config(dir.path()))
            .plugin(plugin().decode_payloads_on_read())
    });
    let payload = built.err().expect("the build refuses the option");
    let message = payload
        .downcast_ref::<String>()
        .cloned()
        .unwrap_or_default();
    assert!(
        message.contains("decode_payloads_on_read (#2466)"),
        "{message}"
    );
}

/// The SQLite worker reads no cost hold, so the plugin refuses the option
/// rather than run a held type (autumn#1720).
#[test]
fn a_cost_hold_fails_the_build_on_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let built = std::panic::catch_unwind(|| {
        TestApp::new()
            .config(config(dir.path()))
            .plugin(plugin().cost_deferrable_activities(["shout"]))
    });
    let payload = built.err().expect("the build refuses the option");
    let message = payload
        .downcast_ref::<String>()
        .cloned()
        .unwrap_or_default();
    assert!(
        message.contains("HarvestPlugin::cost_deferrable_activities (#2466)"),
        "{message}"
    );
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
        .assert_status(202);
    let result = wait_completed(&client, &exec).await;
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

/// A string schema for `greet`, as `#[workflow(input_schema = ..)]` gives.
fn string_schema() -> Value {
    json!({ "type": "string" })
}

#[tokio::test(flavor = "multi_thread")]
async fn an_input_that_breaks_the_schema_is_refused_before_it_starts() {
    let dir = tempfile::tempdir().unwrap();
    let mut info = greet_info();
    info.input_schema = Some(string_schema);
    let client = TestApp::new()
        .config(config(dir.path()))
        .profile("dev")
        .plugin(
            HarvestPlugin::new()
                .workflows(vec![info])
                .activities(activities![shout])
                .api("/harvest"),
        )
        .build();

    let response = client
        .post("/harvest/workflows/greet/start")
        .json(&json!({ "input": 42 }))
        .send()
        .await;
    response.assert_status(400);
    let body: Value = response.json();
    assert_eq!(body["error"], "input validation failed");
    assert!(body["violations"].as_array().is_some_and(|v| !v.is_empty()));

    let response = client
        .post("/harvest/workflows/greet/signal-with-start")
        .json(&json!({
            "workflow_id": "w-schema",
            "start_input": 42,
            "signal_name": "go",
        }))
        .send()
        .await;
    response.assert_status(400);

    // App code gets the same refusal from the handle.
    let handle = client.state().extension::<HarvestSqlite>().unwrap();
    assert!(matches!(
        handle.start("greet", json!(42), StartOptions::new()).await,
        Err(autumn_harvest_plugin::sqlite::HarvestSqliteError::InvalidInput(_))
    ));

    let runs: Value = client.get("/harvest/workflows").send().await.json();
    assert_eq!(runs, json!([]), "a refused start writes nothing");
    start(&client, &json!({ "input": "bee", "workflow_id": "w-ok" })).await;

    // An attach discards the start input, so it needs none, as on Postgres.
    let attached = client
        .post("/harvest/workflows/greet/signal-with-start")
        .json(&json!({ "workflow_id": "w-ok", "signal_name": "go" }))
        .send()
        .await;
    attached.assert_ok();
    let attached: Value = attached.json();
    assert_eq!(attached["started_fresh"], false);
    assert_eq!(attached["signal_delivered"], true);
}

/// An object schema for the `go` signal of `greet`.
fn object_schema() -> Value {
    json!({ "type": "object" })
}

#[tokio::test(flavor = "multi_thread")]
async fn a_signal_that_breaks_its_schema_is_refused_before_it_is_staged() {
    let dir = tempfile::tempdir().unwrap();
    let go = autumn_harvest::SignalHandlerInfo {
        name: "go",
        workflow: "greet",
        module: module_path!(),
        arg_type_hint: "serde_json::Value",
        description: None,
        arg_schema: Some(object_schema),
    };
    let client = TestApp::new()
        .config(config(dir.path()))
        .profile("dev")
        .plugin(plugin().signals(vec![go]))
        .build();
    let exec = start(&client, &json!({ "input": "bee", "workflow_id": "w-sig" })).await;

    let refused = [
        client
            .post(&format!("/harvest/workflows/{exec}/signal/go"))
            .json(&json!(42))
            .send()
            .await,
        client
            .post("/harvest/workflows/by-id/greet/w-sig/signal/go")
            .json(&json!(42))
            .send()
            .await,
        client
            .post("/harvest/workflows/greet/signal-with-start")
            .json(&json!({
                "workflow_id": "w-sig",
                "start_input": "bee",
                "signal_name": "go",
                "signal_payload": 42,
            }))
            .send()
            .await,
    ];
    for response in refused {
        response.assert_status(400);
        let body: Value = response.json();
        assert_eq!(body["error"], "signal payload validation failed");
    }
    let handle = client.state().extension::<HarvestSqlite>().unwrap();
    assert!(matches!(
        handle
            .signal(exec.parse().unwrap(), "go", json!(42), None)
            .await,
        Err(autumn_harvest_plugin::sqlite::HarvestSqliteError::InvalidSignal(_))
    ));

    // A payload that passes reaches the run, which then completes.
    client
        .post(&format!("/harvest/workflows/{exec}/signal/go"))
        .json(&json!({ "ok": true }))
        .send()
        .await
        .assert_status(202);
    let result = wait_completed(&client, &exec).await;
    assert_eq!(result["output"]["go"], json!({ "ok": true }));
}

/// Tenant routes need `sqlite_per_tenant` (issue #2472). Without it they are
/// a `404` that names the option, never an empty `200`.
#[tokio::test(flavor = "multi_thread")]
async fn a_tenant_route_without_per_tenant_is_a_404() {
    let dir = tempfile::tempdir().unwrap();
    let client = app(dir.path(), "dev");
    let response = client.get("/harvest/tenants/acme/workflows").send().await;
    response.assert_status(404);
    assert!(response.text().contains("sqlite_per_tenant"));
}
