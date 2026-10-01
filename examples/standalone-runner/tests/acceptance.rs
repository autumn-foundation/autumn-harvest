//! Black-box acceptance test for issue #1615.
//!
//! Each test runs the built `standalone-runner` binary against Postgres. It
//! then drives the README steps over real HTTP. The crate names no
//! `autumn-web`, so this is the epic's definition of done, run end to end.
//!
//! Dual-mode database. `HARVEST_TEST_DATABASE_URL` gives each test a fresh
//! database on that server. Otherwise each test starts a Postgres 16
//! container.

#![cfg(unix)]

use std::fs::File;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use hmac::{Hmac, Mac};
use serde_json::{Value, json};
use sha2::Sha256;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const WEBHOOK_SECRET: &str = "acceptance-webhook-secret-0123456789";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);
const RESULT_TIMEOUT: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------
// Database
// ---------------------------------------------------------------------------

/// Keeps a container alive for the test. `None` on the shared-server path.
type DbGuard = Option<ContainerAsync<Postgres>>;

/// A fresh, empty database. No migration is applied.
async fn empty_database() -> (String, DbGuard) {
    if let Ok(base_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let name = format!("standalone_runner_{}", uuid_suffix());
        let mut admin = AsyncPgConnection::establish(&base_url)
            .await
            .expect("connect to the base database");
        diesel::sql_query(format!("CREATE DATABASE \"{name}\""))
            .execute(&mut admin)
            .await
            .expect("create a database");
        let (prefix, _) = base_url.rsplit_once('/').expect("url names a database");
        return (format!("{prefix}/{name}"), None);
    }
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

fn uuid_suffix() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock is after the epoch")
        .as_nanos();
    format!("{}_{nanos}", std::process::id())
}

/// Apply the migrations as `harvest migrate run` does.
async fn migrate(database_url: &str) {
    autumn_harvest::migrate::apply(database_url, &autumn_harvest::migrate::embedded())
        .await
        .expect("migrations should apply");
}

/// Seed a token as `harvest token bootstrap` tells the operator to.
async fn bootstrap_token(database_url: &str) -> String {
    let token = autumn_harvest_cli::build_bootstrap_token("acceptance", "mutate", None, "test")
        .expect("bootstrap token should build");
    let mut conn = AsyncPgConnection::establish(database_url)
        .await
        .expect("connect to seed the token");
    diesel::sql_query(&token.insert_sql)
        .execute(&mut conn)
        .await
        .expect("the bootstrap SQL should run");
    token.secret
}

// ---------------------------------------------------------------------------
// The runner process
// ---------------------------------------------------------------------------

/// The `standalone-runner` binary, running. Drop kills it.
struct Runner {
    child: Child,
    base: String,
    log: PathBuf,
}

impl Runner {
    /// Start the binary and wait until `GET /` answers.
    async fn start(database_url: &str, profile: &str, webhook_secret: Option<&str>) -> Self {
        let port = free_port();
        let log = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("runner-{port}.log"));
        let file = File::create(&log).expect("create the runner log");
        let mut command = Command::new(env!("CARGO_BIN_EXE_standalone-runner"));
        command
            .env("DATABASE_URL", database_url)
            .env("AUTUMN_PROFILE", profile)
            .env("STANDALONE_RUNNER_ADDR", format!("127.0.0.1:{port}"))
            .env_remove("AUTUMN_ENV")
            .env_remove("STANDALONE_RUNNER_WEBHOOK_SECRET")
            .stdin(Stdio::null())
            .stdout(file.try_clone().expect("share the log file"))
            .stderr(file);
        if let Some(secret) = webhook_secret {
            command.env("STANDALONE_RUNNER_WEBHOOK_SECRET", secret);
        }
        let child = command.spawn().expect("the runner binary should spawn");
        let mut runner = Self {
            child,
            base: format!("http://127.0.0.1:{port}"),
            log,
        };
        runner.wait_until_ready().await;
        runner
    }

    async fn wait_until_ready(&mut self) {
        let deadline = Instant::now() + STARTUP_TIMEOUT;
        loop {
            if let Some(status) = self.child.try_wait().expect("poll the child") {
                panic!("the runner exited during startup: {status}\n{}", self.log());
            }
            if let Ok(response) = reqwest::get(&self.base).await
                && response.status().is_success()
            {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "the runner did not start\n{}",
                self.log()
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// What the runner wrote to stdout and stderr so far.
    fn log(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    /// Send SIGINT, as Ctrl-C does, and wait for the exit.
    fn interrupt(mut self) -> ExitStatus {
        let pid = self.child.id().to_string();
        let sent = Command::new("kill")
            .args(["-INT", &pid])
            .status()
            .expect("kill should run");
        assert!(sent.success(), "SIGINT should reach the runner");
        let deadline = Instant::now() + STARTUP_TIMEOUT;
        loop {
            if let Some(status) = self.child.try_wait().expect("poll the child") {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "the runner did not stop\n{}",
                self.log()
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

impl Drop for Runner {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind an ephemeral port")
        .local_addr()
        .expect("read the bound address")
        .port()
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

async fn get(url: &str, bearer: Option<&str>) -> reqwest::Response {
    let mut request = reqwest::Client::new().get(url);
    if let Some(token) = bearer {
        request = request.bearer_auth(token);
    }
    request.send().await.expect("GET should reach the runner")
}

async fn start_order(runner: &Runner, workflow_id: &str) {
    let response = reqwest::Client::new()
        .post(runner.url("/api/harvest/workflows/standalone_order/start"))
        .json(&json!({
            "workflow_id": workflow_id,
            "input": { "order_id": workflow_id, "sku": "sku-book", "quantity": 2 },
        }))
        .send()
        .await
        .expect("start should reach the runner");
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    assert!(status.is_success(), "start failed: {status} {body}");
}

/// Poll the result route until the run has an output.
async fn await_output(runner: &Runner, workflow_id: &str) -> Value {
    let url = runner.url(&format!(
        "/api/harvest/workflows/by-id/standalone_order/{workflow_id}/result"
    ));
    let deadline = Instant::now() + RESULT_TIMEOUT;
    let mut last = String::new();
    while Instant::now() < deadline {
        let response = get(&url, None).await;
        let status = response.status();
        last = response.text().await.unwrap_or_default();
        if status == reqwest::StatusCode::OK {
            let body: Value = serde_json::from_str(&last).expect("result is JSON");
            if let Some(output) = body.get("output").filter(|output| !output.is_null()) {
                return output.clone();
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!(
        "{workflow_id} did not complete in time, last body: {last}\n{}",
        runner.log()
    );
}

fn sign(body: &[u8]) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(WEBHOOK_SECRET.as_bytes()).expect("any key length works");
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

async fn post_webhook(runner: &Runner, body: &[u8], signature: &str) -> reqwest::StatusCode {
    reqwest::Client::new()
        .post(runner.url("/hooks/orders"))
        .header("Content-Type", "application/json")
        .header("X-Webhook-Signature", signature)
        .header("X-Webhook-Delivery", "delivery-1")
        .body(body.to_vec())
        .send()
        .await
        .expect("webhook should reach the runner")
        .status()
}

// ---------------------------------------------------------------------------
// Acceptance
// ---------------------------------------------------------------------------

/// The README `Run` path. The `dev` profile makes the binary apply the
/// migrations itself, on a database that has none. Vantage answers. A
/// started order completes, and `/metrics` reports it. Ctrl-C stops the
/// process cleanly.
#[tokio::test]
async fn dev_profile_migrates_runs_an_order_and_reports_metrics() {
    let (url, _db) = empty_database().await;
    let runner = Runner::start(&url, "dev", None).await;

    let vantage = get(&runner.url("/api/harvest/ui"), None).await;
    assert_eq!(
        vantage.status(),
        reqwest::StatusCode::OK,
        "Vantage is mounted"
    );

    start_order(&runner, "order-1001").await;
    let output = await_output(&runner, "order-1001").await;
    assert_eq!(output["version"], json!(2));
    assert_eq!(output["shipment"]["label_id"], json!("lbl_order-1001"));

    let metrics = get(&runner.url("/metrics"), None).await;
    assert_eq!(metrics.status(), reqwest::StatusCode::OK);
    let text = metrics.text().await.expect("metrics body");
    assert!(
        text.contains("harvest_workflow_started_total{workflow=\"standalone_order\""),
        "missing the started sample:\n{text}"
    );

    let status = runner.interrupt();
    assert!(status.success(), "Ctrl-C should stop cleanly: {status}");
}

/// Outside `dev`, the admin API needs a credential. A token from the
/// `harvest token bootstrap` SQL reaches `preflight`. An anonymous call
/// does not.
#[tokio::test]
async fn prod_profile_admits_a_bootstrap_token_and_nothing_else() {
    let (url, _db) = empty_database().await;
    migrate(&url).await;
    let token = bootstrap_token(&url).await;
    let runner = Runner::start(&url, "prod", None).await;
    let preflight = runner.url("/api/harvest/admin/preflight");

    let anonymous = get(&preflight, None).await;
    assert_eq!(anonymous.status(), reqwest::StatusCode::UNAUTHORIZED);

    let wrong = get(&preflight, Some("hvst_not-a-real-token")).await;
    assert_eq!(wrong.status(), reqwest::StatusCode::UNAUTHORIZED);

    let admitted = get(&preflight, Some(&token)).await;
    let status = admitted.status();
    let body = admitted.text().await.unwrap_or_default();
    assert_eq!(status, reqwest::StatusCode::OK, "{body}");
    let report: Value = serde_json::from_str(&body).expect("preflight is JSON");
    assert!(
        check_named(&report, "admin_auth_boundary").is_some(),
        "{report}"
    );
}

fn check_named<'a>(report: &'a Value, name: &str) -> Option<&'a Value> {
    report["checks"]
        .as_array()?
        .iter()
        .find(|check| check["name"] == json!(name))
}

/// A signed delivery starts the mapped workflow. A bad signature is
/// rejected before any dispatch.
///
/// The profile is `development`, which normalizes to `dev`. The runner must
/// still migrate, so it reads the profile as the embedding does.
#[tokio::test]
async fn signed_webhook_starts_an_order() {
    let (url, _db) = empty_database().await;
    let runner = Runner::start(&url, "development", Some(WEBHOOK_SECRET)).await;
    let body = serde_json::to_vec(&json!({
        "order_id": "hook-7",
        "sku": "sku-pen",
        "quantity": 1,
    }))
    .expect("body serializes");

    let forged = post_webhook(&runner, &body, &format!("sha256={}", "0".repeat(64))).await;
    assert_eq!(forged, reqwest::StatusCode::UNAUTHORIZED);

    let accepted = post_webhook(&runner, &body, &sign(&body)).await;
    assert!(accepted.is_success(), "signed delivery: {accepted}");
    let output = await_output(&runner, "order-hook-7").await;
    assert_eq!(output["order_id"], json!("hook-7"));
}

/// No secret, no receiver. The example never serves an unsigned route.
#[tokio::test]
async fn webhook_route_is_absent_without_a_secret() {
    let (url, _db) = empty_database().await;
    let runner = Runner::start(&url, "dev", None).await;
    let body = b"{}";

    let status = post_webhook(&runner, body, &sign(body)).await;
    assert_eq!(status, reqwest::StatusCode::NOT_FOUND);
}
