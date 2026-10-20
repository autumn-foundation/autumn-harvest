//! Durable streams end to end, with testcontainers.
//!
//! Each test drives one plugin-wired app against a real Postgres. The route
//! `/stream` reads its call settings from test headers. The test client
//! collects the whole body, so each test reads a stream after it ends. A
//! short stream budget ends a stream of a live run.
//!
//! Requires Docker. Each test uses a multi-thread runtime, because
//! `TestApp::plugin` blocks on plugin startup.

#![allow(clippy::unused_async, clippy::used_underscore_binding)]

use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use autumn_harvest::ExecutionId;
use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestPlugin;
use autumn_harvest_plugin::durable_route::{DurableRoute, DurableStream, REPLAYED_HEADER};
use autumn_web::prelude::*;
use autumn_web::reexports::axum;
use autumn_web::test::{TestApp, TestClient, TestResponse};
use axum::http::{HeaderMap, StatusCode};
use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Text};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const EXEC_HEADER: &str = "x-harvest-execution-id";

// ── Fixtures ──────────────────────────────────────────────────────────────────

/// The tags that `ds_record` saw, one entry for each run of the activity.
static SIDE_EFFECTS: Mutex<Vec<String>> = Mutex::new(Vec::new());

fn side_effects(tag: &str) -> usize {
    SIDE_EFFECTS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .filter(|seen| *seen == tag)
        .count()
}

/// Records one side effect for `tag` and returns how many it has seen.
#[activity]
async fn ds_record(_ctx: &ActivityContext, tag: String) -> Result<u64, String> {
    let mut seen = SIDE_EFFECTS.lock().unwrap_or_else(PoisonError::into_inner);
    seen.push(tag.clone());
    Ok(seen.iter().filter(|t| **t == tag).count() as u64)
}

fn tag_of(input: &Value) -> String {
    input["body"]["tag"].as_str().unwrap_or("none").to_owned()
}

/// Runs one side effect, then publishes `body.n` tokens.
#[workflow]
async fn ds_tokens(ctx: &WorkflowContext, input: Value) -> Result<Value, String> {
    let count = ctx
        .execute_activity_raw("ds_record", json!(tag_of(&input)), "default")
        .await
        .map_err(|e| e.to_string())?;
    let n = input["body"]["n"].as_u64().unwrap_or(3);
    for i in 0..n {
        ctx.publish_durable_progress(json!({"token": i}))
            .map_err(|e| e.to_string())?;
    }
    Ok(json!({"user": input["user"], "tokens": n, "count": count}))
}

/// Publishes two tokens, waits for `go`, then publishes two more.
#[workflow]
async fn ds_parked(ctx: &WorkflowContext, _input: Value) -> Result<Value, String> {
    for i in 0..2 {
        ctx.publish_durable_progress(json!({"token": i}))
            .map_err(|e| e.to_string())?;
    }
    let go = ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    for i in 2..4 {
        ctx.publish_durable_progress(json!({"token": i}))
            .map_err(|e| e.to_string())?;
    }
    Ok(json!({"go": go}))
}

/// Publishes one token for its attempt. The first attempt then fails.
#[workflow]
async fn ds_retry(ctx: &WorkflowContext, input: Value) -> Result<Value, String> {
    let count = ctx
        .execute_activity_raw("ds_record", json!(tag_of(&input)), "default")
        .await
        .map_err(|e| e.to_string())?;
    ctx.publish_durable_progress(json!({"attempt_token": count}))
        .map_err(|e| e.to_string())?;
    if count == json!(1) {
        return Err("first attempt fails".to_owned());
    }
    Ok(json!({"count": count}))
}

/// Publishes its hop, then continues as new until `body.hops`.
#[workflow]
async fn ds_chain(ctx: &WorkflowContext, input: Value) -> Result<Value, String> {
    let target = input
        .get("target")
        .and_then(Value::as_u64)
        .or_else(|| input["body"]["hops"].as_u64())
        .unwrap_or(2);
    let hop = input.get("hop").and_then(Value::as_u64).unwrap_or(0);
    ctx.publish_durable_progress(json!({"hop": hop}))
        .map_err(|e| e.to_string())?;
    if hop < target {
        ctx.continue_as_new(json!({"hop": hop + 1, "target": target}))
            .await
            .map_err(|e| e.to_string())?;
        return Ok(Value::Null);
    }
    Ok(json!({"final": hop}))
}

/// Publishes one token, then fails with the typed class `Refused`.
#[workflow]
async fn ds_refused(ctx: &WorkflowContext, _input: Value) -> Result<Value, WorkflowFailure> {
    ctx.publish_durable_progress(json!({"token": 0}))
        .map_err(|e| WorkflowFailure::new("Error", e.to_string()))?;
    Err(WorkflowFailure::new("Refused", "secret workflow message")
        .with_details(json!({"why": "policy"})))
}

// ── Route ─────────────────────────────────────────────────────────────────────

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

/// The streaming test route. The test headers pick the call settings.
#[post("/stream")]
async fn stream(
    durable: DurableRoute,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> DurableStream {
    let user = header(&headers, "x-user").map(str::to_owned);
    let workflow = header(&headers, "x-workflow")
        .unwrap_or("ds_tokens")
        .to_owned();
    let input = json!({"user": user, "body": body});
    let scoped = match &user {
        Some(user) => durable.scope(user),
        None => durable.unscoped(),
    };
    let mut call = scoped.call(&workflow, input);
    if let Some(ms) = header(&headers, "x-stream-ms").and_then(|v| v.parse().ok()) {
        call = call.stream_for(Duration::from_millis(ms));
    }
    if let Some((error_type, status)) = header(&headers, "x-map").and_then(|v| v.split_once('=')) {
        let status = StatusCode::from_u16(status.parse().expect("status")).expect("status");
        call = call.failure_status(error_type, status);
    }
    if headers.contains_key("x-expose") {
        call = call.expose_execution_id();
    }
    call.stream().await
}

// ── Harness ───────────────────────────────────────────────────────────────────

/// A migrated Postgres 16 container for one test.
struct TestPg {
    _container: ContainerAsync<Postgres>,
    url: String,
    pool: Pool<AsyncPgConnection>,
}

async fn setup_db(max_size: usize) -> TestPg {
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
    // A short acquire bound makes a starved pool fail fast.
    let pool = Pool::builder(manager)
        .max_size(max_size)
        .wait_timeout(Some(Duration::from_secs(2)))
        .runtime(deadpool::Runtime::Tokio1)
        .build()
        .expect("failed to build pool");
    TestPg {
        _container: container,
        url,
        pool,
    }
}

fn base_plugin() -> HarvestPlugin {
    HarvestPlugin::new()
        .workflows(vec![
            __autumn_workflow_info_ds_tokens(),
            __autumn_workflow_info_ds_parked(),
            __autumn_workflow_info_ds_retry().with_retry_policy(
                autumn_harvest::policy::RetryPolicy::exponential(2, Duration::from_millis(100)),
            ),
            __autumn_workflow_info_ds_chain(),
            __autumn_workflow_info_ds_refused(),
        ])
        .activities(activities![ds_record])
        .worker(WorkerConfig::default())
        .api("/api/harvest")
        .allow_unauthenticated_mutations()
}

fn app_with(db: &TestPg, plugin: HarvestPlugin) -> TestApp {
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
    TestApp::new()
        .config(config)
        .routes(vec![__autumn_route_info_stream()])
        .plugin(plugin)
        .with_db(db.pool.clone())
}

async fn build_app(db: &TestPg) -> TestClient {
    app_with(db, base_plugin()).build()
}

/// One request to `/stream` with `key` and the test headers in `extra`.
async fn post(
    client: &TestClient,
    key: Option<&str>,
    extra: &[(&str, &str)],
    body: &Value,
) -> TestResponse {
    let mut request = client.post("/stream");
    if let Some(key) = key {
        request = request.header("idempotency-key", key);
    }
    for (name, value) in extra {
        request = request.header(name, value);
    }
    request.json(body).send().await
}

/// One parsed SSE frame. Comments are skipped.
#[derive(Debug, Clone, PartialEq)]
struct Frame {
    event: String,
    id: Option<String>,
    data: Value,
}

fn frames(response: &TestResponse) -> Vec<Frame> {
    assert_eq!(response.status, StatusCode::OK, "{}", response.text());
    assert_eq!(
        response.header("content-type"),
        Some("text/event-stream"),
        "{}",
        response.text()
    );
    response
        .text()
        .split("\n\n")
        .filter_map(|block| {
            let mut event = None;
            let mut id = None;
            let mut data = String::new();
            for line in block.lines() {
                if let Some(v) = line.strip_prefix("event: ") {
                    event = Some(v.to_owned());
                } else if let Some(v) = line.strip_prefix("id: ") {
                    id = Some(v.to_owned());
                } else if let Some(v) = line.strip_prefix("data: ") {
                    data.push_str(v);
                }
            }
            event.map(|event| Frame {
                event,
                id,
                data: serde_json::from_str(&data).expect("frame data is JSON"),
            })
        })
        .collect()
}

fn progress_ids(frames: &[Frame]) -> Vec<String> {
    frames
        .iter()
        .filter(|f| f.event == "progress")
        .filter_map(|f| f.id.clone())
        .collect()
}

const fn last(frames: &[Frame]) -> &Frame {
    frames.last().expect("a final frame")
}

fn exec_id_of(response: &TestResponse) -> ExecutionId {
    response
        .header(EXEC_HEADER)
        .expect("execution id header")
        .parse()
        .expect("execution id")
}

#[derive(QueryableByName)]
struct Count {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

async fn count_runs(db: &TestPg, workflow: &str) -> i64 {
    let mut conn = db.pool.get().await.expect("conn");
    diesel::sql_query(
        "SELECT count(DISTINCT workflow_id)::bigint AS n FROM harvest_workflow_executions \
         WHERE workflow_name = $1",
    )
    .bind::<Text, _>(workflow)
    .get_result::<Count>(&mut conn)
    .await
    .expect("count runs")
    .n
}

async fn signal(client: &TestClient, exec_id: ExecutionId, payload: Value) {
    let pool = client
        .state()
        .extension::<autumn_harvest_plugin::HarvestDbPool>()
        .expect("harvest pool");
    let mut conn = pool.pool_for_execution(exec_id).get().await.expect("conn");
    autumn_harvest::signal::send_signal(&mut conn, exec_id, "go", payload)
        .await
        .expect("signal");
}

fn slug_type(slug: &str) -> String {
    format!("urn:autumn-harvest:durable-route:{slug}")
}

/// Stream until the final frame comes, resuming after each `pending` frame.
async fn until_final(
    client: &TestClient,
    key: &str,
    extra: &[(&str, &str)],
    body: &Value,
) -> Vec<Frame> {
    let mut all: Vec<Frame> = Vec::new();
    for _ in 0..50 {
        let cursor = all.iter().rev().find_map(|f| f.id.clone());
        let mut headers: Vec<(&str, &str)> = extra.to_vec();
        if let Some(cursor) = &cursor {
            headers.push(("last-event-id", cursor.as_str()));
        }
        let response = post(client, Some(key), &headers, body).await;
        if response.status == StatusCode::ACCEPTED {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        let got = frames(&response);
        let done = got.last().is_some_and(|f| f.event != "pending");
        all.extend(got.into_iter().filter(|f| f.event != "pending"));
        if done {
            return all;
        }
    }
    panic!("the stream did not end after 50 tries");
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// SAC-22
#[tokio::test(flavor = "multi_thread")]
async fn stream_sends_every_chunk_then_the_result() {
    let db = setup_db(8).await;
    let client = build_app(&db).await;
    let body = json!({"tag": "sac22", "n": 4});
    let frames = until_final(&client, "key-sac22", &[("x-user", "ada")], &body).await;
    assert_eq!(progress_ids(&frames), ["0.0", "0.1", "0.2", "0.3"]);
    let tokens: Vec<&Value> = frames
        .iter()
        .filter(|f| f.event == "progress")
        .map(|f| &f.data["token"])
        .collect();
    assert_eq!(tokens, [&json!(0), &json!(1), &json!(2), &json!(3)]);
    let end = last(&frames);
    assert_eq!(end.event, "result");
    assert_eq!(end.data["tokens"], 4);
    assert_eq!(end.data["user"], "ada");
    assert_eq!(end.id, None);
    // No SSE id holds an execution id.
    assert!(frames.iter().filter_map(|f| f.id.as_deref()).all(|id| {
        id.split('.')
            .all(|part| part.chars().all(|c| c.is_ascii_digit()))
    }));
}

// SAC-23
#[tokio::test(flavor = "multi_thread")]
async fn resume_with_last_event_id_has_no_gap_and_no_duplicate() {
    let db = setup_db(8).await;
    let client = build_app(&db).await;
    let body = json!({"tag": "sac23"});
    let extra = [
        ("x-user", "ada"),
        ("x-workflow", "ds_parked"),
        ("x-stream-ms", "1500"),
        ("x-expose", "1"),
    ];
    // The run parks after two tokens, so the stream ends at its budget.
    let mut first = post(&client, Some("key-sac23"), &extra, &body).await;
    for _ in 0..20 {
        if progress_ids(&frames(&first)).len() == 2 {
            break;
        }
        first = post(&client, Some("key-sac23"), &extra, &body).await;
    }
    let first_frames = frames(&first);
    assert_eq!(progress_ids(&first_frames), ["0.0", "0.1"]);
    assert_eq!(last(&first_frames).event, "pending");
    signal(&client, exec_id_of(&first), json!("now")).await;
    // Resume after the first token. The stream sends the rest once.
    let mut resumed = Vec::new();
    for _ in 0..50 {
        let with_cursor = [&extra[..], &[("last-event-id", "0.0")]].concat();
        let response = post(&client, Some("key-sac23"), &with_cursor, &body).await;
        assert_eq!(response.header(REPLAYED_HEADER), Some("true"));
        resumed = frames(&response);
        if last(&resumed).event == "result" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(progress_ids(&resumed), ["0.1", "0.2", "0.3"]);
    assert_eq!(last(&resumed).data, json!({"go": "now"}));
    assert_eq!(count_runs(&db, "ds_parked").await, 1);
}

// SAC-24
#[tokio::test(flavor = "multi_thread")]
async fn live_stream_delivers_chunks_published_after_connect() {
    let db = setup_db(8).await;
    let client = build_app(&db).await;
    let body = json!({"tag": "sac24"});
    let extra = [
        ("x-user", "ada"),
        ("x-workflow", "ds_parked"),
        ("x-expose", "1"),
        ("x-stream-ms", "0"),
    ];
    // Wait until the run has parked, with a zero-budget stream.
    let mut parked = post(&client, Some("key-sac24"), &extra, &body).await;
    for _ in 0..50 {
        if progress_ids(&frames(&parked)).len() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        parked = post(&client, Some("key-sac24"), &extra, &body).await;
    }
    let exec_id = exec_id_of(&parked);
    // A live stream is open when the signal lands, 800 ms after it opens.
    let opened = Instant::now();
    let live = async {
        let response = post(
            &client,
            Some("key-sac24"),
            &[
                ("x-user", "ada"),
                ("x-workflow", "ds_parked"),
                ("x-stream-ms", "15000"),
                ("last-event-id", "0.1"),
            ],
            &body,
        )
        .await;
        (response, opened.elapsed())
    };
    let later = async {
        tokio::time::sleep(Duration::from_millis(800)).await;
        signal(&client, exec_id, json!("late")).await;
        opened.elapsed()
    };
    let ((live, ended), signalled) = tokio::join!(live, later);
    let frames = frames(&live);
    assert_eq!(progress_ids(&frames), ["0.2", "0.3"]);
    assert_eq!(last(&frames).event, "result");
    // The stream was open before the signal, and it ended soon after it.
    let after = ended.saturating_sub(signalled);
    assert!(ended > signalled, "the stream ended before the signal");
    assert!(
        after < Duration::from_secs(5),
        "the stream took {after:?} after the signal"
    );
}

// SAC-25
#[tokio::test(flavor = "multi_thread")]
async fn retry_with_the_same_key_starts_no_second_run() {
    let db = setup_db(8).await;
    let client = build_app(&db).await;
    let body = json!({"tag": "sac25", "n": 2});
    let extra = [("x-user", "ada")];
    let first = until_final(&client, "key-sac25", &extra, &body).await;
    let again = post(&client, Some("key-sac25"), &extra, &body).await;
    assert_eq!(again.header(REPLAYED_HEADER), Some("true"));
    let again = frames(&again);
    assert_eq!(progress_ids(&again), progress_ids(&first));
    assert_eq!(last(&again), last(&first));
    assert_eq!(side_effects("sac25"), 1);
    assert_eq!(count_runs(&db, "ds_tokens").await, 1);
}

// SAC-26
#[tokio::test(flavor = "multi_thread")]
async fn changed_body_answers_422_and_streams_nothing() {
    let db = setup_db(8).await;
    let client = build_app(&db).await;
    let extra = [("x-user", "ada")];
    until_final(&client, "key-sac26", &extra, &json!({"tag": "sac26"})).await;
    let changed = post(&client, Some("key-sac26"), &extra, &json!({"tag": "other"})).await;
    assert_eq!(changed.status.as_u16(), 422, "{}", changed.text());
    assert_eq!(
        changed.json::<Value>()["type"],
        slug_type("idempotency-key-reused")
    );
    assert_ne!(changed.header("content-type"), Some("text/event-stream"));
    assert_eq!(count_runs(&db, "ds_tokens").await, 1);
}

// SAC-27
#[tokio::test(flavor = "multi_thread")]
async fn principals_with_the_same_key_get_separate_streams() {
    let db = setup_db(8).await;
    let client = build_app(&db).await;
    let body = json!({"tag": "sac27"});
    let ada = until_final(&client, "key-sac27", &[("x-user", "ada")], &body).await;
    let bob = until_final(&client, "key-sac27", &[("x-user", "bob")], &body).await;
    assert_eq!(last(&ada).data["user"], "ada");
    assert_eq!(last(&bob).data["user"], "bob");
    assert_eq!(count_runs(&db, "ds_tokens").await, 2);
}

// SAC-28
#[tokio::test(flavor = "multi_thread")]
async fn stream_follows_a_retry_attempt() {
    let db = setup_db(8).await;
    let client = build_app(&db).await;
    let extra = [("x-user", "ada"), ("x-workflow", "ds_retry")];
    let frames = until_final(&client, "key-sac28", &extra, &json!({"tag": "sac28"})).await;
    let events: Vec<(&str, Option<&str>)> = frames
        .iter()
        .map(|f| (f.event.as_str(), f.id.as_deref()))
        .collect();
    assert_eq!(
        events,
        [
            ("progress", Some("0.0")),
            ("attempt", Some("1")),
            ("progress", Some("1.0")),
            ("result", None),
        ]
    );
    assert_eq!(frames[1].data, json!({"attempt": 1, "cause": "retry"}));
    assert_eq!(frames[0].data, json!({"attempt_token": 1}));
    assert_eq!(frames[2].data, json!({"attempt_token": 2}));
    // A resume at the attempt frame sends only the second attempt.
    let resumed = post(
        &client,
        Some("key-sac28"),
        &[&extra[..], &[("last-event-id", "1")]].concat(),
        &json!({"tag": "sac28"}),
    )
    .await;
    assert_eq!(progress_ids(&self::frames(&resumed)), ["1.0"]);
}

// SAC-29
#[tokio::test(flavor = "multi_thread")]
async fn stream_follows_continue_as_new() {
    let db = setup_db(8).await;
    let client = build_app(&db).await;
    let extra = [("x-user", "ada"), ("x-workflow", "ds_chain")];
    let frames = until_final(&client, "key-sac29", &extra, &json!({"hops": 2})).await;
    assert_eq!(progress_ids(&frames), ["0.0", "1.0", "2.0"]);
    let hops: Vec<&Value> = frames
        .iter()
        .filter(|f| f.event == "progress")
        .map(|f| &f.data["hop"])
        .collect();
    assert_eq!(hops, [&json!(0), &json!(1), &json!(2)]);
    assert!(
        frames.iter().any(|f| f.event == "attempt"
            && f.data == json!({"attempt": 2, "cause": "continued-as-new"}))
    );
    assert_eq!(last(&frames).data, json!({"final": 2}));
}

// SAC-30
#[tokio::test(flavor = "multi_thread")]
async fn failed_run_ends_with_the_problem_frame() {
    let db = setup_db(8).await;
    let client = build_app(&db).await;
    let extra = [
        ("x-user", "ada"),
        ("x-workflow", "ds_refused"),
        ("x-map", "Refused=422"),
    ];
    let frames = until_final(&client, "key-sac30", &extra, &json!({})).await;
    assert_eq!(progress_ids(&frames), ["0.0"]);
    let end = last(&frames);
    assert_eq!(end.event, "problem");
    assert_eq!(end.data["status"], 422);
    assert_eq!(end.data["type"], slug_type("failed"));
    assert_eq!(end.data["error_type"], "Refused");
    assert_eq!(end.data["details"], json!({"why": "policy"}));
    assert!(!end.data.to_string().contains("secret"));
}

// SAC-31
#[tokio::test(flavor = "multi_thread")]
async fn erased_run_answers_a_plain_410() {
    let db = setup_db(8).await;
    let client = build_app(&db).await;
    let body = json!({"tag": "sac31"});
    let extra = [("x-user", "ada"), ("x-expose", "1")];
    let first = until_final(&client, "key-sac31", &extra, &body).await;
    assert_eq!(last(&first).event, "result");
    let replay = post(&client, Some("key-sac31"), &extra, &body).await;
    let exec_id = exec_id_of(&replay);
    {
        let mut conn = db.pool.get().await.expect("conn");
        autumn_harvest::erase::erase_workflow_payloads(&mut conn, exec_id, "test")
            .await
            .expect("erase");
    }
    // An erased run has no chunk to send, so the answer is not a stream.
    let erased = post(&client, Some("key-sac31"), &extra, &body).await;
    assert_eq!(erased.status.as_u16(), 410, "{}", erased.text());
    assert_ne!(erased.header("content-type"), Some("text/event-stream"));
    assert_eq!(erased.json::<Value>()["type"], slug_type("erased"));
    assert_eq!(erased.header(REPLAYED_HEADER), Some("true"));
}

// SAC-32
#[tokio::test(flavor = "multi_thread")]
async fn stream_under_the_idempotent_layer_answers_500_misconfigured() {
    let db = setup_db(8).await;
    let client = app_with(&db, base_plugin()).idempotent().build();
    let response = post(&client, Some("key-sac32"), &[("x-user", "ada")], &json!({})).await;
    assert_eq!(response.status.as_u16(), 500, "{}", response.text());
    assert_eq!(response.json::<Value>()["type"], slug_type("misconfigured"));
    assert_eq!(count_runs(&db, "ds_tokens").await, 0);
}

// SAC-33
#[tokio::test(flavor = "multi_thread")]
async fn invalid_cursor_answers_400_and_starts_nothing() {
    let db = setup_db(8).await;
    let client = build_app(&db).await;
    for cursor in ["x", "1.-2", "1.2.3"] {
        let extra = [("x-user", "ada"), ("last-event-id", cursor)];
        let response = post(&client, Some("key-sac33"), &extra, &json!({})).await;
        assert_eq!(
            response.status.as_u16(),
            400,
            "{cursor}: {}",
            response.text()
        );
        assert_eq!(
            response.json::<Value>()["type"],
            slug_type("stream-cursor-invalid")
        );
    }
    assert_eq!(count_runs(&db, "ds_tokens").await, 0);
}

// SAC-34
#[tokio::test(flavor = "multi_thread")]
async fn full_stream_cap_answers_202_pending() {
    let db = setup_db(8).await;
    // A cap of 0 answers pending for a live run and for an ended run.
    let none = app_with(&db, base_plugin().durable_route_max_streams(0)).build();
    let parked = [
        ("x-user", "ada"),
        ("x-workflow", "ds_parked"),
        ("x-expose", "1"),
    ];
    let response = post(&none, Some("key-sac34-live"), &parked, &json!({})).await;
    assert_eq!(response.status.as_u16(), 202, "{}", response.text());
    assert_eq!(response.header("retry-after"), Some("1"));
    assert!(response.header(EXEC_HEADER).is_some());
    let done = [("x-user", "ada"), ("x-expose", "1")];
    let body = json!({"tag": "sac34", "n": 1});
    let mut ended = post(&none, Some("key-sac34-done"), &done, &body).await;
    for _ in 0..50 {
        if count_runs(&db, "ds_tokens").await == 1 && side_effects("sac34") == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        ended = post(&none, Some("key-sac34-done"), &done, &body).await;
    }
    assert_eq!(ended.status.as_u16(), 202, "{}", ended.text());
}

// SAC-34
#[tokio::test(flavor = "multi_thread")]
async fn a_live_stream_holds_its_slot_until_it_ends() {
    let db = setup_db(8).await;
    let one = app_with(&db, base_plugin().durable_route_max_streams(1)).build();
    let parked = [
        ("x-user", "ada"),
        ("x-workflow", "ds_parked"),
        ("x-stream-ms", "3000"),
    ];
    let done = [("x-user", "ada"), ("x-expose", "1")];
    let body = json!({"tag": "sac34b"});
    let empty = json!({});
    let holder = post(&one, Some("key-sac34-holder"), &parked, &empty);
    let blocked = async {
        tokio::time::sleep(Duration::from_millis(1500)).await;
        post(&one, Some("key-sac34-blocked"), &done, &body).await
    };
    let (holder, blocked) = tokio::join!(holder, blocked);
    assert_eq!(last(&frames(&holder)).event, "pending");
    assert_eq!(blocked.status.as_u16(), 202, "{}", blocked.text());
    // The holder gave its slot back, so the next request streams.
    let frames = until_final(&one, "key-sac34-blocked", &done, &body).await;
    assert_eq!(last(&frames).event, "result");
}

// SAC-35
#[tokio::test(flavor = "multi_thread")]
async fn live_streams_hold_no_pooled_connection() {
    let db = setup_db(4).await;
    let client = build_app(&db).await;
    // Eight live streams wait 4 s each, longer than the 2 s acquire bound.
    let extra = [
        ("x-user", "ada"),
        ("x-workflow", "ds_parked"),
        ("x-stream-ms", "4000"),
    ];
    let bodies: Vec<Value> = (0..8)
        .map(|i| json!({"tag": format!("sac35-{i}")}))
        .collect();
    let keys: Vec<String> = (0..8).map(|i| format!("key-sac35-{i}")).collect();
    let streams = keys
        .iter()
        .zip(&bodies)
        .map(|(key, body)| post(&client, Some(key.as_str()), &extra, body));
    // A ninth request arrives during the streams and needs the pool.
    let probe = async {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let body = json!({"tag": "sac35-probe"});
        until_final(&client, "key-sac35-probe", &[("x-user", "ada")], &body).await
    };
    let (streamed, probe) = tokio::join!(futures::future::join_all(streams), probe);
    for response in streamed {
        let frames = frames(&response);
        assert_eq!(last(&frames).event, "pending", "{frames:?}");
    }
    assert_eq!(last(&probe).event, "result");
    assert_eq!(count_runs(&db, "ds_parked").await, 8);
}
