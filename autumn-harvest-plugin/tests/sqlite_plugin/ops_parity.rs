//! Schedules and retention on the SQLite plugin (issue #2471).
//!
//! Each route keeps its Postgres path, status codes and field names. A
//! schedule `{id}` is a UUID that the workflow name fixes.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, Instant};

use autumn_harvest::policy::{Schedule, WorkflowSchedule};
use autumn_harvest::prelude::*;
use autumn_harvest::retention::RetentionConfig;
use autumn_harvest_plugin::HarvestPlugin;
use autumn_web::test::{TestApp, TestClient};
use serde_json::{Value, json};

#[workflow]
async fn tick(_ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

fn base() -> HarvestPlugin {
    HarvestPlugin::new()
        .workflows(workflows![tick])
        .api("/harvest")
}

fn boot(dir: &Path, profile: &str, plugin: HarvestPlugin) -> TestClient {
    TestApp::new()
        .config(super::config(dir))
        .profile(profile)
        .plugin(plugin)
        .build()
}

/// Poll `path` until `done` accepts the status and the body, or fail.
async fn eventually(client: &TestClient, path: &str, done: impl Fn(u16, &Value) -> bool) -> Value {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let response = client.get(path).send().await;
        let status = response.status.as_u16();
        let body: Value = serde_json::from_slice(&response.body).unwrap_or(Value::Null);
        if done(status, &body) {
            return body;
        }
        assert!(
            Instant::now() < deadline,
            "timed out on {path}: {status} {body}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn schedules(client: &TestClient) -> Vec<Value> {
    let response = client.get("/harvest/admin/schedules").send().await;
    response.assert_ok();
    let body: Value = response.json();
    body.as_array()
        .cloned()
        .expect("a complete list is an array")
}

async fn start_tick(client: &TestClient) -> String {
    let response = client
        .post("/harvest/workflows/tick/start")
        .json(&json!({ "input": 1 }))
        .send()
        .await;
    response.assert_status(201);
    let body: Value = response.json();
    body["execution_id"].as_str().unwrap().to_owned()
}

/// A code schedule fires on SQLite, and the admin routes list it.
#[tokio::test(flavor = "multi_thread")]
async fn a_code_schedule_fires_and_the_admin_routes_list_it() {
    let dir = tempfile::tempdir().unwrap();
    let schedule = WorkflowSchedule::new("tick", Schedule::Interval(Duration::from_secs(1)))
        .with_input(json!(7));
    let client = boot(dir.path(), "dev", base().workflow_schedule(schedule));

    let runs = eventually(
        &client,
        "/harvest/workflows?workflow_name=tick",
        |status, body| status == 200 && body.as_array().is_some_and(|runs| !runs.is_empty()),
    )
    .await;
    assert!(
        runs[0]["workflow_id"]
            .as_str()
            .is_some_and(|id| id.starts_with("sched:tick:")),
        "{runs}"
    );

    let listed = schedules(&client).await;
    assert_eq!(listed.len(), 1, "{listed:?}");
    let entry = &listed[0];
    assert_eq!(entry["kind"], "workflow");
    assert_eq!(entry["name"], "tick");
    assert_eq!(entry["schedule_expr"], "interval:1");
    assert_eq!(entry["workflow_input"], 7);
    assert_eq!(entry["overlap_policy"], "skip");
    assert_eq!(entry["is_paused"], false);
    let id = entry["id"].as_str().unwrap();
    uuid::Uuid::parse_str(id).expect("the id is a UUID");

    let one: Value = client
        .get(&format!("/harvest/admin/schedules/{id}"))
        .send()
        .await
        .json();
    assert_eq!(one["name"], "tick");
    assert_eq!(one["id"], id);
}

/// An API schedule: create, edit, pause, trigger, resume and delete.
#[tokio::test(flavor = "multi_thread")]
async fn an_api_schedule_round_trips_through_the_admin_routes() {
    let dir = tempfile::tempdir().unwrap();
    let client = boot(dir.path(), "dev", base());

    let created = client
        .post("/harvest/admin/schedules/workflow")
        .json(&json!({ "workflow_name": "tick", "schedule_expr": "interval:3600", "input": 1 }))
        .send()
        .await;
    created.assert_status(201);
    let created: Value = created.json();
    assert_eq!(created["name"], "tick");
    assert_eq!(created["schedule_expr"], "interval:3600");
    let id = created["id"].as_str().unwrap().to_owned();
    let at = |suffix: &str| format!("/harvest/admin/schedules/{id}{suffix}");

    let edited = client
        .patch(&at(""))
        .json(&json!({ "max_active_runs": 2, "input": 3 }))
        .send()
        .await;
    edited.assert_ok();
    let edited: Value = edited.json();
    assert_eq!(edited["max_active_runs"], 2);
    assert_eq!(edited["workflow_input"], 3);

    let paused = client
        .post(&at("/pause"))
        .json(&json!({ "reason": "maintenance" }))
        .send()
        .await;
    paused.assert_ok();
    assert_eq!(paused.json::<Value>(), json!({ "ok": true }));
    let one: Value = client.get(&at("")).send().await.json();
    assert_eq!(one["is_paused"], true);
    assert_eq!(one["pause_reason"], "maintenance", "the reason is stored");

    client.post(&at("/trigger")).send().await.assert_status(409);
    let fired = client.post(&at("/trigger?force=true")).send().await;
    fired.assert_ok();
    let fired: Value = fired.json();
    assert_eq!(fired["outcome"], "fired");
    let exec = fired["execution_id"].as_str().unwrap().to_owned();
    assert!(
        fired["workflow_id"]
            .as_str()
            .is_some_and(|id| id.starts_with("manual-tick-")),
        "{fired}"
    );
    eventually(
        &client,
        &format!("/harvest/workflows/{exec}"),
        |status, body| status == 200 && body["execution"]["output"] == 3,
    )
    .await;

    client.post(&at("/resume")).send().await.assert_ok();
    let one: Value = client.get(&at("")).send().await.json();
    assert_eq!(one["is_paused"], false);
    assert_eq!(
        one["pause_reason"],
        Value::Null,
        "a resume clears the reason"
    );

    // `GET` shows these values, so a read-modify-write client can send them.
    client
        .patch(&at(""))
        .json(&json!({ "catchup": false, "jitter_secs": 0, "queue_name": "default" }))
        .send()
        .await
        .assert_ok();

    client.delete(&at("")).send().await.assert_ok();
    client.get(&at("")).send().await.assert_status(404);
    assert_eq!(schedules(&client).await, Vec::<Value>::new());
}

/// Each refusal is loud: a bad id, an unknown workflow, a field that this
/// backend cannot honour, and a route that is not ported.
#[tokio::test(flavor = "multi_thread")]
async fn the_schedule_routes_refuse_what_sqlite_cannot_honour() {
    let dir = tempfile::tempdir().unwrap();
    let client = boot(dir.path(), "dev", base());

    client
        .get("/harvest/admin/schedules/not-a-uuid")
        .send()
        .await
        .assert_status(400);
    client
        .get(&format!(
            "/harvest/admin/schedules/{}",
            uuid::Uuid::new_v4()
        ))
        .send()
        .await
        .assert_status(404);
    client
        .post("/harvest/admin/schedules/workflow")
        .json(&json!({ "workflow_name": "nope", "schedule_expr": "interval:60" }))
        .send()
        .await
        .assert_status(404);
    let refused = client
        .post("/harvest/admin/schedules/workflow")
        .json(&json!({
            "workflow_name": "tick",
            "schedule_expr": "interval:60",
            "calendar": "holidays",
        }))
        .send()
        .await;
    refused.assert_status(400);
    refused.assert_body_contains("calendar");
    assert_eq!(
        schedules(&client).await,
        Vec::<Value>::new(),
        "a refusal writes nothing"
    );

    let created: Value = client
        .post("/harvest/admin/schedules/workflow")
        .json(&json!({ "workflow_name": "tick", "schedule_expr": "interval:60" }))
        .send()
        .await
        .json();
    let id = created["id"].as_str().unwrap();
    let backfill = client
        .post(&format!("/harvest/admin/schedules/{id}/backfill"))
        .json(&json!({}))
        .send()
        .await;
    backfill.assert_status(501);
    backfill.assert_body_contains("issues/2466");
}

/// A PATCH of a code schedule sets an override. `clear_override` returns
/// it to its code value.
#[tokio::test(flavor = "multi_thread")]
async fn a_patch_of_a_code_schedule_is_an_override() {
    let dir = tempfile::tempdir().unwrap();
    let schedule = WorkflowSchedule::new("tick", Schedule::Interval(Duration::from_secs(3600)));
    let client = boot(dir.path(), "dev", base().workflow_schedule(schedule));

    let id = eventually(&client, "/harvest/admin/schedules", |status, body| {
        status == 200 && body.as_array().is_some_and(|rows| rows.len() == 1)
    })
    .await[0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let path = format!("/harvest/admin/schedules/{id}");

    let patched = client
        .patch(&path)
        .json(&json!({ "schedule_expr": "interval:7200", "reason": "slow down" }))
        .send()
        .await;
    patched.assert_ok();
    assert_eq!(patched.json::<Value>()["schedule_expr"], "interval:7200");

    // An override cannot change the input of a code schedule.
    let refused = client
        .patch(&path)
        .json(&json!({ "input": 9 }))
        .send()
        .await;
    refused.assert_status(400);
    refused.assert_body_contains("input");

    // A clear stands alone, as on Postgres. A refused clear writes nothing.
    let combined = client
        .patch(&path)
        .json(&json!({ "clear_override": true, "max_active_runs": 3 }))
        .send()
        .await;
    combined.assert_status(400);
    combined.assert_body_contains("clear_override");
    let kept: Value = client.get(&path).send().await.json();
    assert_eq!(kept["schedule_expr"], "interval:7200", "the override stays");
    assert_eq!(kept["max_active_runs"], 1);

    let cleared = client
        .patch(&path)
        .json(&json!({ "clear_override": true }))
        .send()
        .await;
    cleared.assert_ok();
    assert_eq!(cleared.json::<Value>()["schedule_expr"], "interval:3600");

    // The next boot would upsert it again, so a delete is refused, as on
    // Postgres.
    let deleted = client.delete(&path).send().await;
    deleted.assert_status(400);
    deleted.assert_body_contains("workflow_schedule");
    client.get(&path).send().await.assert_ok();
}

/// A `PATCH` of the expression alone keeps the stored time zone, as on
/// Postgres. An API schedule has no override to clear or to explain.
#[tokio::test(flavor = "multi_thread")]
async fn a_patch_keeps_the_time_zone_and_refuses_override_fields() {
    let dir = tempfile::tempdir().unwrap();
    let client = boot(dir.path(), "dev", base());
    let created: Value = client
        .post("/harvest/admin/schedules/workflow")
        .json(&json!({
            "workflow_name": "tick",
            "schedule_expr": "cron:0 9 * * *",
            "timezone": "Europe/London",
        }))
        .send()
        .await
        .json();
    assert_eq!(created["timezone"], "Europe/London");
    let path = format!(
        "/harvest/admin/schedules/{}",
        created["id"].as_str().unwrap()
    );

    let patched = client
        .patch(&path)
        .json(&json!({ "schedule_expr": "cron:0 10 * * *" }))
        .send()
        .await;
    patched.assert_ok();
    let patched: Value = patched.json();
    assert_eq!(patched["timezone"], "Europe/London");
    assert_eq!(patched["schedule_expr"], "cron_tz:Europe/London:0 10 * * *");

    for body in [
        json!({ "clear_override": true }),
        json!({ "reason": "why" }),
    ] {
        let refused = client.patch(&path).json(&body).send().await;
        refused.assert_status(400);
        refused.assert_body_contains("not declared in code");
    }
}

/// Outside `dev`, an anonymous schedule write is refused, as on Postgres.
#[tokio::test(flavor = "multi_thread")]
async fn an_anonymous_schedule_write_outside_dev_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let client = boot(dir.path(), "test", base());
    client
        .post("/harvest/admin/schedules/workflow")
        .json(&json!({ "workflow_name": "tick", "schedule_expr": "interval:60" }))
        .send()
        .await
        .assert_status(401);
    client
        .post("/harvest/admin/retention/run-now")
        .send()
        .await
        .assert_status(401);
}

fn retention(max_age_secs: u64, tick_interval_secs: u64) -> RetentionConfig {
    RetentionConfig {
        max_age_secs: Some(max_age_secs),
        tick_interval_secs,
        ..RetentionConfig::default()
    }
}

/// `HarvestPlugin::retention(..)` purges an old terminal run on its tick.
/// `GET /admin/retention` reports the passes.
#[tokio::test(flavor = "multi_thread")]
async fn retention_purges_an_old_run_on_its_tick() {
    let dir = tempfile::tempdir().unwrap();
    let client = boot(dir.path(), "dev", base().retention(retention(1, 1)));
    let exec = start_tick(&client).await;
    eventually(
        &client,
        &format!("/harvest/workflows/{exec}"),
        |status, _| status == 404,
    )
    .await;

    // The status holds the last pass only. A later pass removes nothing, so
    // this checks the shape, not the count. `run_now_queues_a_pass` checks it.
    let status: Value = client.get("/harvest/admin/retention").send().await.json();
    assert_eq!(status["config"]["max_age_secs"], 1);
    assert_eq!(status["per_shard"][0]["shard"], 0);
    assert!(status["per_shard"][0]["ran_at"].is_string(), "{status}");
    assert_eq!(status["per_shard"][0]["last_error"], Value::Null);
}

/// `POST /admin/retention/run-now` queues a pass, as on Postgres. With a
/// one-hour tick, no other pass runs, so the status keeps its counts.
#[tokio::test(flavor = "multi_thread")]
async fn run_now_queues_a_pass() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = retention(1, 3600);
    config.overrides = BTreeMap::from([("tick".to_owned(), 1)]);
    let client = boot(dir.path(), "dev", base().retention(config));
    let exec = start_tick(&client).await;
    eventually(
        &client,
        &format!("/harvest/workflows/{exec}"),
        |status, body| status == 200 && body["execution"]["state"] == "COMPLETED",
    )
    .await;
    // The age counts on the real clock, from the close of the run.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let response = client.post("/harvest/admin/retention/run-now").send().await;
    response.assert_ok();
    assert_eq!(response.json::<Value>(), json!({ "ok": true }));
    eventually(
        &client,
        &format!("/harvest/workflows/{exec}"),
        |status, _| status == 404,
    )
    .await;
    let status = eventually(&client, "/harvest/admin/retention", |status, body| {
        status == 200 && body["per_shard"][0]["deleted_count"] == 1
    })
    .await;
    assert_eq!(status["per_shard"][0]["deleted_by_workflow"]["tick"], 1);
}

/// A retention field with no SQLite store fails the build and names it.
#[test]
fn a_tenant_retention_age_fails_the_build() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = retention(60, 60);
    config.tenant_overrides = BTreeMap::from([("acme".to_owned(), 60)]);
    let built = std::panic::catch_unwind(|| {
        TestApp::new()
            .config(super::config(dir.path()))
            .plugin(base().retention(config))
    });
    let payload = built.err().expect("the build refuses the field");
    let message = payload
        .downcast_ref::<String>()
        .cloned()
        .unwrap_or_default();
    assert!(
        message.contains("RetentionConfig::tenant_overrides"),
        "{message}"
    );
}
