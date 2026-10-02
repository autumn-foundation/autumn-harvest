//! Black-box acceptance test for issue #1615.
//!
//! Each test runs the built `standalone-runner` binary against Postgres. It
//! then drives the README steps over real HTTP. The crate names no
//! `autumn-web`, so this is the epic's definition of done, run end to end.
//!
//! Dual-mode database. `HARVEST_TEST_DATABASE_URL` gives each test a fresh
//! database on that server, dropped after the test. Otherwise each test
//! starts a Postgres 16 container. The tests run one at a time.

#![cfg(unix)]

use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::LazyLock;
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

/// One runner and one database at a time.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Every request times out, so a hung runner fails the test.
static HTTP: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("the HTTP client should build")
});

// ---------------------------------------------------------------------------
// Database
// ---------------------------------------------------------------------------

/// Keeps the test database alive. Drop removes it.
enum DbGuard {
    /// A database on the `HARVEST_TEST_DATABASE_URL` server.
    Shared { base_url: String, name: String },
    /// A Postgres container.
    Container(#[allow(dead_code)] Box<ContainerAsync<Postgres>>),
}

impl Drop for DbGuard {
    fn drop(&mut self) {
        let Self::Shared { base_url, name } = self else {
            return;
        };
        let sql = format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)");
        let base_url = base_url.clone();
        // Drop runs inside the test runtime, so a fresh thread owns the work.
        let dropped = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a cleanup runtime should build");
            runtime.block_on(async move {
                let mut admin = AsyncPgConnection::establish(&base_url).await.ok()?;
                diesel::sql_query(sql).execute(&mut admin).await.ok()
            })
        })
        .join();
        if !matches!(dropped, Ok(Some(_))) {
            eprintln!("could not drop the test database {name}");
        }
    }
}

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
        let url = with_database(&base_url, &name);
        return (url, DbGuard::Shared { base_url, name });
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
    (url, DbGuard::Container(Box::new(container)))
}

/// `url` with its database name replaced by `name`. The query is kept.
fn with_database(url: &str, name: &str) -> String {
    let (location, query) = url
        .split_once('?')
        .map_or((url, None), |(l, q)| (l, Some(q)));
    let (authority, _) = location
        .rsplit_once('/')
        .filter(|(authority, _)| authority.contains("//") && !authority.ends_with('/'))
        .unwrap_or((location, ""));
    query.map_or_else(
        || format!("{authority}/{name}"),
        |query| format!("{authority}/{name}?{query}"),
    )
}

#[test]
fn with_database_keeps_the_query_and_the_authority() {
    assert_eq!(
        with_database("postgres://u:p@h:5432/base?sslmode=disable", "x"),
        "postgres://u:p@h:5432/x?sslmode=disable"
    );
    assert_eq!(
        with_database("postgres://u@h/base", "x"),
        "postgres://u@h/x"
    );
    assert_eq!(with_database("postgres://u@h", "x"), "postgres://u@h/x");
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

/// The settings of one runner process.
struct Launch<'a> {
    database_url: &'a str,
    profile: &'a str,
    webhook_secret: Option<&'a str>,
}

impl<'a> Launch<'a> {
    const fn new(database_url: &'a str, profile: &'a str) -> Self {
        Self {
            database_url,
            profile,
            webhook_secret: None,
        }
    }

    const fn webhook_secret(mut self, secret: &'a str) -> Self {
        self.webhook_secret = Some(secret);
        self
    }

    /// Spawn the binary on port 0. Its log names the bound address.
    fn spawn(&self) -> (Child, PathBuf) {
        let log =
            Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("runner-{}.log", uuid_suffix()));
        let file = File::create(&log).expect("create the runner log");
        let mut command = Command::new(env!("CARGO_BIN_EXE_standalone-runner"));
        command
            .env("DATABASE_URL", self.database_url)
            .env("AUTUMN_PROFILE", self.profile)
            .env("STANDALONE_RUNNER_ADDR", "127.0.0.1:0")
            .env_remove("AUTUMN_ENV")
            .env_remove("STANDALONE_RUNNER_WEBHOOK_SECRET")
            .stdin(Stdio::null())
            .stdout(file.try_clone().expect("share the log file"))
            .stderr(file);
        if let Some(secret) = self.webhook_secret {
            command.env("STANDALONE_RUNNER_WEBHOOK_SECRET", secret);
        }
        let child = command.spawn().expect("the runner binary should spawn");
        (child, log)
    }

    /// Start the binary and wait until `GET /` answers.
    async fn start(&self) -> Runner {
        let (child, log) = self.spawn();
        let mut runner = Runner {
            child,
            base: String::new(),
            log,
        };
        runner.wait_until_ready().await;
        runner
    }

    /// Start the binary and expect it to exit before it serves.
    fn refused(&self) -> (ExitStatus, String) {
        let (child, log) = self.spawn();
        let mut runner = Runner {
            child,
            base: String::new(),
            log,
        };
        let status = runner.wait_for_exit();
        (status, runner.log())
    }
}

impl Runner {
    async fn wait_until_ready(&mut self) {
        let deadline = Instant::now() + STARTUP_TIMEOUT;
        loop {
            if let Some(status) = self.child.try_wait().expect("poll the child") {
                panic!("the runner exited during startup: {status}\n{}", self.log());
            }
            if self.base.is_empty()
                && let Some(address) = listening_address(&self.log())
            {
                self.base = format!("http://{address}");
            }
            if !self.base.is_empty()
                && let Ok(response) = HTTP.get(&self.base).send().await
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

    /// Send `signal`, for example `INT` for Ctrl-C, and wait for the exit.
    fn stop_with(mut self, signal: &str) -> ExitStatus {
        let pid = self.child.id().to_string();
        let sent = Command::new("kill")
            .args([&format!("-{signal}"), &pid])
            .status()
            .expect("kill should run");
        assert!(sent.success(), "SIG{signal} should reach the runner");
        self.wait_for_exit()
    }

    fn wait_for_exit(&mut self) -> ExitStatus {
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

/// The address from the runner's `listening` log line.
fn listening_address(log: &str) -> Option<&str> {
    let line = log.lines().find(|line| line.contains("runner listening"))?;
    let address = line.split("address=").nth(1)?;
    address.split_whitespace().next()
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

async fn get(url: &str, bearer: Option<&str>) -> reqwest::Response {
    let mut request = HTTP.get(url);
    if let Some(token) = bearer {
        request = request.bearer_auth(token);
    }
    request.send().await.expect("GET should reach the runner")
}

async fn start_order(runner: &Runner, workflow_id: &str) {
    let response = HTTP
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
    HTTP.post(runner.url("/hooks/orders"))
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
    let _serial = SERIAL.lock().await;
    let (url, _db) = empty_database().await;
    let runner = Launch::new(&url, "dev").start().await;

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

    let status = runner.stop_with("INT");
    assert!(status.success(), "Ctrl-C should stop cleanly: {status}");
}

/// Outside `dev`, the admin API needs a credential. A token from the
/// `harvest token bootstrap` SQL reaches `preflight`. An anonymous call
/// does not.
#[tokio::test]
async fn prod_profile_admits_a_bootstrap_token_and_nothing_else() {
    let _serial = SERIAL.lock().await;
    let (url, _db) = empty_database().await;
    migrate(&url).await;
    let token = bootstrap_token(&url).await;
    let runner = Launch::new(&url, "prod").start().await;
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
    // The README says this check fails outside `dev`: the token gates only
    // the admin routes. A `pass` here would mislead an operator.
    let boundary = check_named(&report, "admin_auth_boundary").expect("the check is present");
    assert_eq!(boundary["status"], json!("fail"), "{report}");
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
    let _serial = SERIAL.lock().await;
    let (url, _db) = empty_database().await;
    let runner = Launch::new(&url, "development")
        .webhook_secret(WEBHOOK_SECRET)
        .start()
        .await;
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
    let _serial = SERIAL.lock().await;
    let (url, _db) = empty_database().await;
    let runner = Launch::new(&url, "dev").start().await;
    let body = b"{}";

    let status = post_webhook(&runner, body, &sign(body)).await;
    assert_eq!(status, reqwest::StatusCode::NOT_FOUND);
}

/// Outside `dev`, a short webhook secret refuses boot. A short HMAC key
/// can be brute-forced from one captured delivery.
#[tokio::test]
async fn prod_profile_refuses_a_weak_webhook_secret() {
    let _serial = SERIAL.lock().await;
    let (url, _db) = empty_database().await;
    migrate(&url).await;

    let (status, log) = Launch::new(&url, "prod").webhook_secret("short").refused();
    assert!(!status.success(), "a weak secret must refuse boot\n{log}");
    assert!(
        log.to_lowercase().contains("secret"),
        "the error names the secret\n{log}"
    );
}

/// An open SSE stream must not hold shutdown forever. Axum's graceful
/// shutdown waits for every response, and a stream never ends on its own.
#[tokio::test]
async fn sigterm_stops_cleanly_with_an_open_stream() {
    let _serial = SERIAL.lock().await;
    let (url, _db) = empty_database().await;
    let runner = Launch::new(&url, "dev").start().await;

    // No worker polls this queue, so the run stays open and so does its stream.
    let started: Value = HTTP
        .post(runner.url("/api/harvest/workflows/standalone_order/start"))
        .json(&json!({
            "workflow_id": "parked-1",
            "queue": "nobody-polls-this",
            "input": { "order_id": "parked-1", "sku": "sku-book", "quantity": 1 },
        }))
        .send()
        .await
        .expect("start should reach the runner")
        .json()
        .await
        .expect("start returns JSON");
    let exec_id = started["execution_id"].as_str().expect("an execution id");
    let stream = reqwest::Client::new()
        .get(runner.url(&format!("/api/harvest/workflows/{exec_id}/stream")))
        .send()
        .await
        .expect("the stream should open");
    assert!(stream.status().is_success(), "stream: {}", stream.status());

    let began = Instant::now();
    let status = runner.stop_with("TERM");
    assert!(status.success(), "SIGTERM should stop cleanly: {status}");
    assert!(
        began.elapsed() < Duration::from_secs(30),
        "shutdown took {:?}",
        began.elapsed()
    );
    drop(stream);
}

/// A managed Postgres such as Fly hands out a URL with no `sslmode` and
/// refuses plaintext. With no `sslmode`, every runner connection must use the
/// TLS the server offers: the pool, the migrations and the LISTEN listeners.
/// A server without TLS gets plaintext instead, and the runner still works.
///
/// The local server offers TLS with a self-signed certificate. The CI
/// container offers none. So the two paths cover both answers.
#[tokio::test]
async fn an_absent_sslmode_uses_tls_when_the_server_offers_it() {
    #[derive(diesel::QueryableByName)]
    struct Transport {
        #[diesel(sql_type = diesel::sql_types::Text)]
        server_ssl: String,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        encrypted: i64,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        total: i64,
    }

    let _serial = SERIAL.lock().await;
    let (url, _db) = empty_database().await;
    let bare = url.split('?').next().expect("a URL").to_owned();
    let runner = Launch::new(&bare, "dev").start().await;
    start_order(&runner, "order-tls").await;
    await_output(&runner, "order-tls").await;
    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect to inspect the runner");
    let seen: Transport = diesel::sql_query(
        "SELECT current_setting('ssl') AS server_ssl, \
                count(*) FILTER (WHERE s.ssl) AS encrypted, \
                count(*) AS total \
         FROM pg_stat_activity a JOIN pg_stat_ssl s USING (pid) \
         WHERE a.datname = current_database() \
           AND a.backend_type = 'client backend' \
           AND a.pid <> pg_backend_pid()",
    )
    .get_result(&mut conn)
    .await
    .expect("read the runner connections");
    assert!(seen.total > 0, "the runner holds connections");
    let expected = if seen.server_ssl == "on" {
        seen.total
    } else {
        0
    };
    assert_eq!(
        seen.encrypted,
        expected,
        "server ssl={}: {} of {} runner connections use TLS\n{}",
        seen.server_ssl,
        seen.encrypted,
        seen.total,
        runner.log()
    );
}

/// SIGTERM, as Docker, systemd and Kubernetes send it, also drains the
/// worker and exits cleanly.
#[tokio::test]
async fn sigterm_stops_cleanly() {
    let _serial = SERIAL.lock().await;
    let (url, _db) = empty_database().await;
    let runner = Launch::new(&url, "dev").start().await;

    let log_before = runner.log();
    let status = runner.stop_with("TERM");
    assert!(
        status.success(),
        "SIGTERM should stop cleanly: {status}\n{log_before}"
    );
}
