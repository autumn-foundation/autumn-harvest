//! Remote task calls against a real MCP Tasks server (issue #2006).
//!
//! One plugin app serves `report_export` as an MCP task through the #2005
//! route, on a real TCP port. The same app runs `report_caller`, which calls
//! that task through `HttpRemoteTasks`. A `RemoteTaskPoller` settles the
//! token. So each request crosses the real JSON-RPC wire.
//!
//! - The caller suspends, and the poller resumes it with the tool result.
//! - A workflow error on the server is a completed call with `is_error`.
//! - A retried start with the same key gets the same task.
//!
//! Prefers `HARVEST_TEST_DATABASE_URL` (a throwaway database on that
//! server). Otherwise a testcontainer starts. Each test uses a multi-thread
//! runtime, because `TestApp::plugin` blocks on plugin startup.

#![cfg(feature = "mcp")]
#![allow(clippy::unused_async, clippy::used_underscore_binding)]

use std::time::Duration;

use autumn_harvest::prelude::*;
use autumn_harvest::remote_task::{
    self, RemoteProtocol, RemoteTaskCall, RemoteTaskOutcome, RemoteTaskPoller, RemoteTaskRequest,
    RemoteTaskStart, RemoteTaskState, RemoteTaskTransport, RemoteTasks,
};
use autumn_harvest::worker::DbPool;
use autumn_harvest_plugin::HarvestPlugin;
use autumn_harvest_plugin::remote_tasks::{HttpRemoteTasks, RemoteServer};
use autumn_web::test::TestApp;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const TASKS: &str = "/api/harvest/mcp/tasks";
const WAIT: Duration = Duration::from_secs(60);

// ── Fixtures ──────────────────────────────────────────────────────────────────

/// The remote task. It runs for 1 s, then returns a report, or a workflow
/// error for the month `never`.
#[workflow(mcp, description = "Exports one monthly report")]
async fn report_export(ctx: &WorkflowContext, month: String) -> Result<String, String> {
    ctx.timer("render", 1).await.map_err(|e| e.to_string())?;
    if month == "never" {
        return Err("no data for never".to_string());
    }
    Ok(format!("report:{month}"))
}

/// The caller. It calls `report_export` as a remote MCP task.
#[workflow]
async fn report_caller(ctx: &WorkflowContext, month: String) -> Result<RemoteTaskOutcome, String> {
    let call = RemoteTaskCall::mcp(
        "harvest",
        "start_report_export",
        json!({ "body": month }),
        Duration::from_secs(120),
    );
    ctx.call_remote_task(&call).await.map_err(|e| e.to_string())
}

// ── Harness ───────────────────────────────────────────────────────────────────

/// A migrated database for one test.
struct TestDb {
    url: String,
    _container: Option<ContainerAsync<Postgres>>,
}

async fn setup_db() -> TestDb {
    if let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let name = format!("harvest_remote_{}", uuid::Uuid::new_v4().simple());
        let mut admin = AsyncPgConnection::establish(&admin_url)
            .await
            .expect("connect admin");
        diesel::sql_query(format!("CREATE DATABASE \"{name}\""))
            .execute(&mut admin)
            .await
            .expect("create test database");
        let (base, _) = admin_url.rsplit_once('/').expect("a database segment");
        let url = format!("{base}/{name}");
        let mut conn = AsyncPgConnection::establish(&url)
            .await
            .expect("connect test database");
        conn.batch_execute(&autumn_harvest::test_init_sql())
            .await
            .expect("migrate");
        autumn_web::migrate::run_pending(&url, autumn_web::migrate::FRAMEWORK_MIGRATIONS)
            .expect("framework migrations");
        return TestDb {
            url,
            _container: None,
        };
    }
    let container = Postgres::default()
        .with_init_sql(autumn_harvest::test_init_sql().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    autumn_web::migrate::run_pending(&url, autumn_web::migrate::FRAMEWORK_MIGRATIONS)
        .expect("framework migrations");
    TestDb {
        url,
        _container: Some(container),
    }
}

fn pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(5)
        .build()
        .expect("pool")
}

/// A running app on a real port, and the client that calls it.
struct Served {
    base: String,
    remote: RemoteTasks,
    transport: HttpRemoteTasks,
    pool: DbPool,
}

async fn serve_app(db: &TestDb) -> Served {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let base = format!("http://{}", listener.local_addr().expect("addr"));
    let transport =
        HttpRemoteTasks::new().server("harvest", RemoteServer::mcp(&format!("{base}{TASKS}")));
    let remote = RemoteTasks::new(transport.clone());
    let plugin = HarvestPlugin::new()
        .workflows(vec![
            __autumn_workflow_info_report_export(),
            __autumn_workflow_info_report_caller(),
        ])
        .activities(remote_task::activities())
        .state(remote.clone())
        .worker(WorkerConfig::default())
        .api("/api/harvest")
        .mcp_tasks()
        // Issue #1802: set the opt-out. These tests exercise the wire, not auth.
        .allow_unauthenticated_mutations();
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
            url: Some(db.url.clone()),
            ..Default::default()
        },
        ..Default::default()
    };
    let pool = pool(&db.url);
    let router = TestApp::new()
        .config(config)
        .plugin(plugin)
        .with_db(pool.clone())
        .build()
        .into_router();
    tokio::spawn(async move {
        autumn_web::reexports::axum::serve(listener, router)
            .await
            .expect("serve");
    });
    Served {
        base,
        remote,
        transport,
        pool,
    }
}

async fn start_caller(served: &Served, month: &str) -> String {
    let response = reqwest::Client::new()
        .post(format!(
            "{}/api/harvest/workflows/report_caller/start",
            served.base
        ))
        .json(&json!({ "input": month }))
        .send()
        .await
        .expect("start");
    let status = response.status();
    let body: Value = response.json().await.expect("start body");
    assert!(status.is_success(), "start: {status} {body}");
    body["execution_id"]
        .as_str()
        .expect("execution_id")
        .to_string()
}

/// Poll until the caller run ends, and return its execution row.
async fn wait_for_end(served: &Served, poller: &RemoteTaskPoller, exec_id: &str) -> Value {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        poller.poll_once(&served.pool).await.expect("poll");
        let detail: Value = reqwest::Client::new()
            .get(format!("{}/api/harvest/workflows/{exec_id}", served.base))
            .send()
            .await
            .expect("get run")
            .json()
            .await
            .expect("run body");
        let run = detail["execution"].clone();
        let state = run["state"].as_str().unwrap_or_default();
        if !matches!(state, "RUNNING" | "SUSPENDED" | "PAUSED" | "") {
            return run;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the caller did not end: {run}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn output_of(run: &Value) -> RemoteTaskOutcome {
    let output = run.get("output").cloned().expect("output");
    let output = match output {
        Value::String(text) => serde_json::from_str(&text).unwrap_or(Value::String(text)),
        other => other,
    };
    serde_json::from_value(output).expect("a remote task outcome")
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// AC1 over the real wire: the caller suspends on a remote MCP task. The
/// poller settles the token from `tasks/get`, and the caller gets the tool
/// result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_workflow_calls_a_real_mcp_task_and_resumes() {
    let db = setup_db().await;
    let served = serve_app(&db).await;
    let poller = RemoteTaskPoller::new(&served.remote).with_interval(Duration::from_millis(200));

    let exec_id = start_caller(&served, "2026-09").await;
    let run = wait_for_end(&served, &poller, &exec_id).await;
    assert_eq!(run["state"], "COMPLETED", "{run}");
    let outcome = output_of(&run);
    assert!(!outcome.is_error, "{outcome:?}");
    assert_eq!(
        outcome.result["structuredContent"],
        Value::Null,
        "a string output rides in the text content"
    );
    assert!(
        outcome.result.to_string().contains("report:2026-09"),
        "{outcome:?}"
    );
}

/// A workflow error on the server is a completed call with `is_error`. The
/// caller does not retry or fail.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_remote_workflow_error_completes_the_call_with_is_error() {
    let db = setup_db().await;
    let served = serve_app(&db).await;
    let poller = RemoteTaskPoller::new(&served.remote);

    let exec_id = start_caller(&served, "never").await;
    let run = wait_for_end(&served, &poller, &exec_id).await;
    assert_eq!(run["state"], "COMPLETED", "{run}");
    let outcome = output_of(&run);
    assert!(outcome.is_error, "{outcome:?}");
    assert!(
        outcome.result.to_string().contains("no data for never"),
        "{outcome:?}"
    );
}

/// The server honours the start key: a retried start gets the same task.
/// `tasks/get` then reads the same task to its end.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retried_start_gets_the_same_task() {
    let db = setup_db().await;
    let served = serve_app(&db).await;
    let request = RemoteTaskRequest {
        server: "harvest".into(),
        protocol: RemoteProtocol::Mcp,
        tool: "start_report_export".into(),
        arguments: json!({"body": "2026-10"}),
    };

    let first = served
        .transport
        .start(&request, "retry-key-1")
        .await
        .expect("start");
    let second = served
        .transport
        .start(&request, "retry-key-1")
        .await
        .expect("retry");
    let RemoteTaskStart::Task(handle) = first else {
        panic!("expected a task, got {first:?}");
    };
    assert_eq!(second, RemoteTaskStart::Task(handle.clone()));

    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        match served.transport.get(&handle).await.expect("get") {
            RemoteTaskState::Completed(outcome) => {
                assert!(!outcome.is_error, "{outcome:?}");
                break;
            }
            RemoteTaskState::Working => {}
            other => panic!("unexpected state {other:?}"),
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the task never ended"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
