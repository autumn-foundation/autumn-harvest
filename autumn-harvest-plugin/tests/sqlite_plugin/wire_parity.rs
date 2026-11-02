//! The Postgres wire contract on the SQLite routes (issue #2489).
//!
//! Each test runs the shared checks of `support/wire_contract.rs`. The
//! Postgres integration tests run the same checks, so a new difference
//! between the backends fails CI.

// The `#[workflow]` macro reads the unused `_n` input through its expansion.
#![allow(clippy::used_underscore_binding)]

use std::time::{Duration, Instant};

use autumn_harvest::policy::{JitterPolicy, RetryPolicy};
use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestPlugin;
use autumn_web::test::{TestApp, TestClient, TestResponse};
use serde_json::{Value, json};

use super::wire_contract::{self as contract, Reply};

/// Waits for `go`, then completes with the payload.
#[workflow]
async fn parked(ctx: &WorkflowContext, _n: Value) -> Result<Value, String> {
    let go = ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    Ok(json!({ "go": go }))
}

/// Takes `go` signals until one carries `"stop"`. Each signal adds an
/// event, so a test can grow a history past one page.
#[workflow]
async fn collects(ctx: &WorkflowContext, _n: Value) -> Result<Value, String> {
    let mut count = 0_u32;
    loop {
        let go = ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
        if go == json!("stop") {
            return Ok(json!(count));
        }
        count += 1;
    }
}

/// Continues as new once. The successor waits for `go`.
#[workflow]
async fn hop(ctx: &WorkflowContext, n: i64) -> Result<Value, String> {
    if n == 0 {
        ctx.continue_as_new(json!(1))
            .await
            .map_err(|e| e.to_string())?;
    }
    let go = ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    Ok(json!({ "n": n, "go": go }))
}

/// Fails. Its one retry waits an hour before it runs.
#[workflow(retry = RetryPolicy {
    max_attempts: 2,
    initial_interval: Duration::from_secs(3600),
    backoff_coefficient: 1.0,
    max_interval: Duration::from_secs(3600),
    non_retryable_errors: Vec::new(),
    jitter: JitterPolicy::None,
})]
async fn brittle(_ctx: &WorkflowContext, _n: Value) -> Result<Value, String> {
    Err("brittle".to_string())
}

/// Fails with no retry.
#[workflow]
async fn broken(_ctx: &WorkflowContext, _n: Value) -> Result<Value, String> {
    Err("it broke".to_string())
}

/// Registers `set`, then waits for `go`. The validator wants a positive
/// number. The handler fails on 13.
#[workflow]
async fn tunable(ctx: &WorkflowContext, _n: Value) -> Result<Value, String> {
    ctx.register_update_handler(
        "set",
        |input: &Value| match input.as_i64() {
            Some(n) if n > 0 => Ok(()),
            _ => Err("a positive number".to_string()),
        },
        |input: Value| async move {
            if input == json!(13) {
                return Err("13 is unlucky".to_string());
            }
            Ok(json!({ "set": input }))
        },
    );
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

fn app(dir: &std::path::Path) -> TestClient {
    TestApp::new()
        .config(super::config(dir))
        .profile("dev")
        .plugin(
            HarvestPlugin::new()
                .workflows(workflows![parked, collects, hop, brittle, broken, tunable])
                .api("/harvest"),
        )
        .build()
}

fn reply(response: &TestResponse) -> Reply {
    let header = |name: &str| response.header(name).map(str::to_owned);
    Reply {
        status: response.status.as_u16(),
        retry_after: header("retry-after"),
        execution_id: header(contract::EXECUTION_ID_HEADER),
        body: if response.body.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&response.body).unwrap_or(Value::Null)
        },
    }
}

async fn get(client: &TestClient, path: &str) -> Reply {
    reply(&client.get(&format!("/harvest{path}")).send().await)
}

async fn post(client: &TestClient, path: &str, body: &Value) -> Reply {
    reply(
        &client
            .post(&format!("/harvest{path}"))
            .json(body)
            .send()
            .await,
    )
}

async fn post_empty(client: &TestClient, path: &str) -> Reply {
    reply(&client.post(&format!("/harvest{path}")).send().await)
}

/// Start `workflow` with `workflow_id`, and return the execution id.
async fn start(client: &TestClient, workflow: &str, workflow_id: &str, input: Value) -> String {
    let started = post(
        client,
        &format!("/workflows/{workflow}/start"),
        &json!({ "workflow_id": workflow_id, "input": input }),
    )
    .await;
    assert_eq!(started.status, 201, "{started:?}");
    started.body["execution_id"].as_str().unwrap().to_owned()
}

/// Poll `path` until `done` accepts the reply, or fail after 10 s.
async fn wait_for(client: &TestClient, path: &str, done: impl Fn(&Reply) -> bool) -> Reply {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let answer = get(client, path).await;
        if done(&answer) {
            return answer;
        }
        assert!(Instant::now() < deadline, "timed out on {path}: {answer:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_state(client: &TestClient, exec: &str, state: &str) {
    wait_for(client, &format!("/workflows/{exec}"), |answer| {
        answer.body["execution"]["state"] == state
    })
    .await;
}

/// The retry that the failed `exec` started, from its history. A retry has
/// its own `workflow_id`, as on Postgres.
async fn retry_of(client: &TestClient, exec: &str) -> String {
    let path = format!("/workflows/{exec}/history?event_type=WorkflowRetryScheduled");
    let page = wait_for(client, &path, |answer| {
        answer.body["events"]
            .as_array()
            .is_some_and(|e| e.len() == 1)
    })
    .await;
    page.body["events"][0]["data"]["retry_exec_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

// ── GET /workflows/{id}/result ───────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn result_answers_204_while_running_and_the_compact_form_at_the_end() {
    let dir = tempfile::tempdir().unwrap();
    let client = app(dir.path());
    let exec = start(&client, "parked", "r-1", json!("bee")).await;

    contract::result_is_pending(&get(&client, &format!("/workflows/{exec}/result")).await);
    contract::result_is_pending(
        &get(&client, &format!("/workflows/{exec}/result?wait=50ms")).await,
    );
    contract::result_of_unknown_run(
        &get(
            &client,
            &format!("/workflows/{}/result", ExecutionId::new()),
        )
        .await,
        &get(&client, "/workflows/not-an-id/result").await,
    );
    for query in ["bogus=1", "wait=soon"] {
        contract::result_refuses_a_bad_query(
            &get(&client, &format!("/workflows/{exec}/result?{query}")).await,
        );
    }

    let signal = post(&client, &format!("/workflows/{exec}/signal/go"), &json!(7)).await;
    assert_eq!(signal.status, 202, "{signal:?}");
    // A long poll answers once the run ends.
    let done = get(&client, &format!("/workflows/{exec}/result?wait=10s")).await;
    let body = contract::result_is_terminal(&done, "completed");
    assert_eq!(body["output"], json!({ "go": 7 }));

    let by_id = get(&client, "/workflows/by-id/parked/r-1/result").await;
    contract::result_is_terminal(&by_id, "completed");
    assert_eq!(by_id.execution_id.as_deref(), Some(exec.as_str()));

    let failed = start(&client, "broken", "r-2", json!(null)).await;
    let answer = wait_for(&client, &format!("/workflows/{failed}/result"), |a| {
        a.status == 200
    })
    .await;
    let body = contract::result_is_terminal(&answer, "failed");
    assert!(
        body["error"].as_str().unwrap().contains("it broke"),
        "{body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn result_follows_continue_as_new_to_the_final_run() {
    let dir = tempfile::tempdir().unwrap();
    let client = app(dir.path());
    let first = start(&client, "hop", "h-1", json!(0)).await;
    wait_state(&client, &first, "CONTINUED_AS_NEW").await;

    // The successor still waits, so the chain is not done.
    contract::result_is_pending(&get(&client, &format!("/workflows/{first}/result")).await);

    // By id resolves the live successor, as on Postgres.
    let signal = post(&client, "/workflows/by-id/hop/h-1/signal/go", &json!("x")).await;
    assert_eq!(signal.status, 202, "{signal:?}");
    assert_ne!(signal.execution_id.as_deref(), Some(first.as_str()));

    let answer = wait_for(&client, &format!("/workflows/{first}/result"), |a| {
        a.status == 200
    })
    .await;
    let body = contract::result_is_terminal(&answer, "completed");
    assert_eq!(body["output"], json!({ "n": 1, "go": "x" }));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_run_routes_to_its_live_retry() {
    let dir = tempfile::tempdir().unwrap();
    let client = app(dir.path());
    let first = start(&client, "brittle", "b-1", json!(null)).await;
    wait_state(&client, &first, "FAILED").await;
    let retry = retry_of(&client, &first).await;
    assert_ne!(retry, first, "the failure started a retry");

    // The retry waits an hour, so the logical run is not done.
    contract::result_is_pending(&get(&client, &format!("/workflows/{first}/result")).await);

    // A signal to the failed run lands on the retry (issue #843).
    let signal = post(&client, &format!("/workflows/{first}/signal/go"), &json!(1)).await;
    assert_eq!(signal.status, 202, "{signal:?}");
    assert_eq!(signal.body["routed_execution_id"], retry, "{signal:?}");
    assert_eq!(signal.body["signal_delivered"], true);
    // The key still names the failed run. Its retry takes the signal.
    let by_id = post(&client, "/workflows/by-id/brittle/b-1/signal/go", &json!(2)).await;
    assert_eq!(by_id.status, 202, "{by_id:?}");
    assert_eq!(by_id.body["execution_id"], retry, "{by_id:?}");
    assert_eq!(by_id.execution_id.as_deref(), Some(retry.as_str()));

    // A cancel of the failed run cancels the retry.
    let cancel = post_empty(&client, &format!("/workflows/{first}/cancel")).await;
    assert_eq!(cancel.status, 202, "{cancel:?}");
    assert_eq!(cancel.body["execution_id"], retry, "{cancel:?}");
    let answer = wait_for(&client, &format!("/workflows/{first}/result"), |a| {
        a.status == 200
    })
    .await;
    contract::result_is_terminal(&answer, "cancelled");

    // A terminate of a failed run ends its retry.
    let second = start(&client, "brittle", "b-2", json!(null)).await;
    wait_state(&client, &second, "FAILED").await;
    let second_retry = retry_of(&client, &second).await;
    let stop = post_empty(&client, &format!("/workflows/{second}/terminate")).await;
    assert_eq!(stop.status, 202, "{stop:?}");
    assert_eq!(stop.body["execution_id"], second_retry, "{stop:?}");
    assert_eq!(stop.body["newly_terminated"], true);
    assert_eq!(stop.body["state"], "TERMINATED");
}

// ── GET /workflows/{id}/history ──────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn history_pages_with_the_postgres_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let client = app(dir.path());
    let exec = start(&client, "parked", "p-1", json!("bee")).await;
    post(&client, &format!("/workflows/{exec}/signal/go"), &json!(1)).await;
    wait_state(&client, &exec, "COMPLETED").await;

    let full = get(&client, &format!("/workflows/{exec}/history")).await;
    assert_eq!(full.status, 200, "{full:?}");
    contract::history_page_shape(&full.body);
    let total = full.body["total_events"].as_u64().unwrap();
    assert!(total >= 3, "{full:?}");

    let mut pages = Vec::new();
    let mut path = format!("/workflows/{exec}/history?limit=2");
    loop {
        let page = get(&client, &path).await;
        assert_eq!(page.status, 200, "{page:?}");
        let next = page.body["next_cursor"].as_str().map(str::to_owned);
        pages.push(page.body);
        match next {
            Some(cursor) => path = format!("/workflows/{exec}/history?limit=2&after={cursor}"),
            None => break,
        }
    }
    let types = contract::history_pages_cover(&pages, 2, total);
    assert_eq!(types.first().map(String::as_str), Some("WorkflowStarted"));
    assert_eq!(types.last().map(String::as_str), Some("WorkflowCompleted"));

    let filtered = get(
        &client,
        &format!(
            "/workflows/{exec}/history?event_type=SignalReceived&event_type=WorkflowCompleted"
        ),
    )
    .await;
    contract::history_filter_holds_only(
        &filtered.body,
        &["SignalReceived", "WorkflowCompleted"],
        2,
        total,
    );
    let none = get(
        &client,
        &format!("/workflows/{exec}/history?event_type=Nope"),
    )
    .await;
    contract::history_filter_holds_only(&none.body, &[], 0, total);

    // Like Postgres, the route ignores a key that it does not read.
    let ignored = get(&client, &format!("/workflows/{exec}/history?bogus=1")).await;
    assert_eq!(ignored.status, 200, "{ignored:?}");

    let from_zero = get(&client, &format!("/workflows/{exec}/history?after=0")).await;
    assert_eq!(
        from_zero.body["events"].as_array().map(Vec::len),
        usize::try_from(total).ok()
    );

    for query in ["limit=0", "limit=abc", "after=-1", "after=x"] {
        contract::history_refuses(
            &get(&client, &format!("/workflows/{exec}/history?{query}")).await,
            400,
        );
    }
    contract::history_refuses(
        &get(
            &client,
            &format!("/workflows/{}/history", ExecutionId::new()),
        )
        .await,
        404,
    );
}

// ── Describe, by id, list, registered, health ────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn describe_caps_its_history_at_one_page() {
    let dir = tempfile::tempdir().unwrap();
    let client = app(dir.path());
    let exec = start(&client, "collects", "c-1", json!(null)).await;
    for n in 0..105 {
        let signal = post(&client, &format!("/workflows/{exec}/signal/go"), &json!(n)).await;
        assert_eq!(signal.status, 202, "{signal:?}");
    }
    post(
        &client,
        &format!("/workflows/{exec}/signal/go"),
        &json!("stop"),
    )
    .await;
    wait_state(&client, &exec, "COMPLETED").await;

    let describe = get(&client, &format!("/workflows/{exec}")).await;
    contract::describe_shape(&describe.body);
    assert_eq!(describe.body["history"].as_array().map(Vec::len), Some(100));
    assert_eq!(describe.body["history_truncated"], true);
    let full = get(&client, &format!("/workflows/{exec}/history?limit=1000")).await;
    assert!(
        full.body["total_events"].as_u64().unwrap() > 100,
        "{full:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_read_routes_keep_the_postgres_shape() {
    let dir = tempfile::tempdir().unwrap();
    let client = app(dir.path());
    let exec = start(&client, "parked", "d-1", json!("bee")).await;

    let describe = get(&client, &format!("/workflows/{exec}")).await;
    assert_eq!(describe.status, 200, "{describe:?}");
    contract::describe_shape(&describe.body);
    assert_eq!(describe.body["parent_id"], Value::Null);
    assert_eq!(describe.body["legal_hold"], false);

    contract::by_id_names_its_run(&get(&client, "/workflows/by-id/parked/d-1").await, &exec);
    for path in ["/workflows/by-id/parked/", "/workflows/by-id/parked"] {
        let answer = get(&client, path).await;
        assert_eq!(answer.status, 400, "{path}: {answer:?}");
    }

    contract::list_refuses_an_unknown_state(&get(&client, "/workflows?state=BOGUS").await);
    // The state check is case sensitive, as on Postgres.
    contract::list_refuses_an_unknown_state(&get(&client, "/workflows?state=running").await);
    let running = get(&client, "/workflows?state=RUNNING&workflow_name=").await;
    assert_eq!(
        running.body.as_array().map(Vec::len),
        Some(1),
        "{running:?}"
    );
    let page = get(&client, "/workflows?page_size=1").await;
    contract::list_envelope_shape(&page.body);

    let registered = get(&client, "/workflows/registered").await;
    contract::registered_shape(&registered.body);
    assert_eq!(registered.body.as_array().map(Vec::len), Some(6));

    contract::live_shape(&get(&client, "/health/live").await);
    contract::ready_shape(&get(&client, "/health/ready").await);
}

// ── Writes ───────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn the_write_routes_answer_the_postgres_codes() {
    let dir = tempfile::tempdir().unwrap();
    let client = app(dir.path());

    let blank = post(
        &client,
        "/workflows/parked/start",
        &json!({ "workflow_id": "" }),
    )
    .await;
    assert_eq!(blank.status, 400, "{blank:?}");

    let fresh = post(
        &client,
        "/workflows/parked/signal-with-start",
        &json!({ "workflow_id": "w-1", "signal_name": "go", "signal_payload": 1 }),
    )
    .await;
    assert_eq!(fresh.status, 201, "{fresh:?}");
    let exec = fresh.body["execution_id"].as_str().unwrap().to_owned();
    wait_state(&client, &exec, "COMPLETED").await;
    let again = post(
        &client,
        "/workflows/parked/start",
        &json!({ "workflow_id": "w-1", "reuse_policy": "reject_duplicate" }),
    )
    .await;
    assert_eq!(again.status, 409, "{again:?}");
    assert_eq!(again.body["existing_execution_id"], exec);
    assert_eq!(again.body["existing_state"], "COMPLETED");

    let duplicate = post(
        &client,
        "/workflows/parked/signal-with-start",
        &json!({
            "workflow_id": "w-1",
            "signal_name": "go",
            "id_reuse_policy": "reject_duplicate",
        }),
    )
    .await;
    assert_eq!(duplicate.status, 409, "{duplicate:?}");
    assert_eq!(duplicate.body["existing_execution_id"], exec);

    // A signal to a terminal run is a 400, as on Postgres.
    let late = post(&client, &format!("/workflows/{exec}/signal/go"), &json!(2)).await;
    assert_eq!(late.status, 400, "{late:?}");

    // A terminate is 202, and idempotent on a terminal run.
    let ended = post_empty(&client, &format!("/workflows/{exec}/terminate")).await;
    assert_eq!(ended.status, 202, "{ended:?}");
    assert_eq!(ended.body["newly_terminated"], false);
    assert_eq!(ended.body["state"], "COMPLETED");
    assert_eq!(ended.body["reason"], "workflow already completed");

    let run = start(&client, "parked", "w-2", json!(null)).await;
    let signal = post(
        &client,
        &format!("/workflows/{run}/signal/other"),
        &json!(1),
    )
    .await;
    assert_eq!(signal.status, 202, "{signal:?}");
    assert!(
        signal.body.get("routed_execution_id").is_none(),
        "{signal:?}"
    );
    let by_id = post(
        &client,
        "/workflows/by-id/parked/w-2/signal/other",
        &json!(1),
    )
    .await;
    assert_eq!(by_id.status, 202, "{by_id:?}");
    assert_eq!(by_id.execution_id.as_deref(), Some(run.as_str()));

    // A repeat cancel keeps the first reason and is not new.
    let cancel = post_empty(&client, &format!("/workflows/{run}/cancel")).await;
    assert_eq!(cancel.status, 202, "{cancel:?}");
    assert_eq!(cancel.body["newly_cancelled"], true);
    assert_eq!(cancel.body["reason"], "workflow cancellation requested");
    let repeat = post(
        &client,
        &format!("/workflows/{run}/cancel"),
        &json!({ "reason": "second" }),
    )
    .await;
    assert_eq!(repeat.status, 202, "{repeat:?}");
    assert_eq!(repeat.body["newly_cancelled"], false);
    assert_eq!(repeat.body["reason"], "workflow cancellation requested");
    wait_state(&client, &run, "CANCELLED").await;
    let done = post_empty(&client, &format!("/workflows/{run}/cancel")).await;
    assert_eq!(done.status, 202, "{done:?}");
    assert_eq!(done.body["newly_cancelled"], false);
    assert_eq!(done.body["state"], "CANCELLED");

    let live = start(&client, "parked", "w-3", json!(null)).await;
    let stop = post_empty(&client, &format!("/workflows/{live}/terminate")).await;
    assert_eq!(stop.status, 202, "{stop:?}");
    assert_eq!(stop.body["newly_terminated"], true);
    assert_eq!(stop.body["reason"], "workflow termination requested");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_update_answers_the_postgres_body() {
    let dir = tempfile::tempdir().unwrap();
    let client = app(dir.path());
    let exec = start(&client, "tunable", "u-1", json!(null)).await;
    let path = format!("/workflows/{exec}/update/set");

    let ok = post(&client, &path, &json!({ "input": 2 })).await;
    assert_eq!(ok.status, 200, "{ok:?}");
    assert_eq!(ok.body["output"], json!({ "set": 2 }));
    assert!(ok.body["update_id"].is_string(), "{ok:?}");

    let failed = post(&client, &path, &json!({ "input": 13 })).await;
    assert_eq!(failed.status, 409, "{failed:?}");
    assert!(failed.body["update_id"].is_string(), "{failed:?}");
    assert_eq!(failed.body["error"], "13 is unlucky");

    let rejected = post(&client, &path, &json!({ "input": -1 })).await;
    assert_eq!(rejected.status, 422, "{rejected:?}");
    assert_eq!(rejected.body["error"], "update rejected by validator");

    // `wait=completed` is what this backend does, so it passes.
    let waited = post(
        &client,
        &format!("{path}?wait=completed"),
        &json!({ "input": 3 }),
    )
    .await;
    assert_eq!(waited.status, 200, "{waited:?}");
    let admitted = post(
        &client,
        &format!("{path}?wait=admitted"),
        &json!({ "input": 3 }),
    )
    .await;
    assert_eq!(admitted.status, 400, "{admitted:?}");
}
