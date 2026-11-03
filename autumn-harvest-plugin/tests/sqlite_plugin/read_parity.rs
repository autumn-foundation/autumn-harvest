//! The read routes, Vantage and declarative queries on SQLite (issue #2468).
//!
//! Each route test runs a shared check of `support/wire_contract.rs`. The
//! Postgres suite of the same route runs the same check.

// The `#[workflow]` macro reads the unused `_n` input through its expansion.
#![allow(clippy::used_underscore_binding)]
// A `#[query]` handler returns a `Result`.
#![allow(clippy::unnecessary_wraps)]

use std::time::{Duration, Instant};

use autumn_harvest::ParentClosePolicy;
use autumn_harvest::context::WorkflowLogPolicy;
use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestPlugin;
use autumn_web::test::{TestApp, TestClient, TestResponse};
use serde_json::{Value, json};

use super::wire_contract::{self as contract, Reply};

/// Waits for `go`.
#[workflow]
async fn kid(ctx: &WorkflowContext, _n: Value) -> Result<Value, String> {
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// Logs a line, spawns a detached and an awaited `kid`, then awaits the
/// awaited one.
#[workflow]
async fn family(ctx: &WorkflowContext, _n: Value) -> Result<Value, String> {
    ctx.log_info("spawning kids");
    ctx.spawn_child_workflow_detached_raw("kid", json!(2), ParentClosePolicy::Abandon)
        .map_err(|e| e.to_string())?;
    ctx.spawn_child_workflow_raw("kid", json!(1))
        .await
        .map_err(|e| e.to_string())
}

/// Spawns an awaited `family`, so the tree has a grandchild level.
#[workflow]
async fn elder(ctx: &WorkflowContext, _n: Value) -> Result<Value, String> {
    ctx.spawn_child_workflow_raw("family", json!(0))
        .await
        .map_err(|e| e.to_string())
}

/// Continues as new once. The successor waits for `go`.
#[workflow]
async fn relay(ctx: &WorkflowContext, n: i64) -> Result<Value, String> {
    if n == 0 {
        ctx.continue_as_new(json!(1))
            .await
            .map_err(|e| e.to_string())?;
    }
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

#[activity(retry = RetryPolicy::fixed(3, Duration::from_secs(3600)))]
async fn flaky(_ctx: &ActivityContext, _n: i64) -> Result<i64, String> {
    Err("flaky".to_string())
}

/// Arms a timer and runs an activity that backs off for an hour.
#[workflow]
async fn napper(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let info = flaky_info();
    let (nap, out) = futures::join!(
        ctx.timer("nap", 3600),
        ctx.execute_activity::<i64, i64>(&info, n)
    );
    nap.map_err(|e| e.to_string())?;
    out.map_err(|e| e.to_string())
}

/// A declarative query of `family`. It reads the business id.
#[query(workflow = "family")]
fn whoami(ctx: &WorkflowContext) -> Result<String, String> {
    Ok(ctx.workflow_id().to_owned())
}

/// A declarative signal handler of `family`, for the interface.
#[allow(dead_code)]
#[signal(workflow = "family")]
fn wave(_ctx: &WorkflowContext, _hello: String) {}

/// A declarative update handler of `family`, for the interface.
#[update(workflow = "family")]
async fn rename(_ctx: &WorkflowContext, name: String) -> Result<String, String> {
    Ok(name)
}

fn plugin() -> HarvestPlugin {
    HarvestPlugin::new()
        .workflows(workflows![kid, family, elder, relay, napper])
        .activities(activities![flaky])
        .queries(queries![whoami])
        .signals(signals![wave])
        .updates(updates![rename])
        .workflow_log_persistence(WorkflowLogPolicy::new())
        .api("/harvest")
}

fn app(dir: &std::path::Path, profile: &str) -> TestClient {
    TestApp::new()
        .config(super::config(dir))
        .profile(profile)
        .plugin(plugin())
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

/// `GET path` until `done` accepts a `200` body, or fail after 10 s.
async fn get_until(client: &TestClient, path: &str, done: impl Fn(&Value) -> bool) -> Value {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let answer = get(client, path).await;
        if answer.status == 200 && done(&answer.body) {
            return answer.body;
        }
        assert!(Instant::now() < deadline, "timed out on {path}: {answer:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn start(client: &TestClient, workflow: &str, body: &Value) -> String {
    let response = client
        .post(&format!("/harvest/workflows/{workflow}/start"))
        .json(body)
        .send()
        .await;
    response.assert_status(201);
    let body: Value = response.json();
    body["execution_id"].as_str().unwrap().to_owned()
}

/// A `family` run whose awaited child has started.
async fn family_run(client: &TestClient) -> String {
    let exec = start(
        client,
        "family",
        &json!({ "input": 0, "workflow_id": "fam-1" }),
    )
    .await;
    get_until(client, &format!("/workflows/{exec}/children"), |body| {
        body["items"]
            .as_array()
            .is_some_and(|items| items.len() == 2)
    })
    .await;
    exec
}

#[tokio::test(flavor = "multi_thread")]
async fn count_groups_the_runs_as_on_postgres() {
    let dir = tempfile::tempdir().unwrap();
    let client = app(dir.path(), "dev");
    family_run(&client).await;

    let body = get(&client, "/workflows/count").await;
    assert_eq!(body.status, 200, "{body:?}");
    contract::count_shape(&body.body);
    assert_eq!(contract::count_of(&body.body, Some("RUNNING"), None), 3);

    let by_name = get(&client, "/workflows/count?group_by=workflow_name").await;
    contract::count_shape(&by_name.body);
    assert_eq!(contract::count_of(&by_name.body, None, Some("kid")), 2);
    assert_eq!(contract::count_of(&by_name.body, None, Some("family")), 1);

    let filtered = get(
        &client,
        "/workflows/count?workflow_name=kid&state=COMPLETED",
    )
    .await;
    assert_eq!(filtered.body["total"], 0, "{filtered:?}");
    assert_eq!(
        get(&client, "/workflows/count?state=nope").await.status,
        400
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn summaries_answer_an_empty_page_and_check_the_filters() {
    let dir = tempfile::tempdir().unwrap();
    let client = app(dir.path(), "dev");
    let body = get(&client, "/workflows/summaries").await;
    assert_eq!(body.status, 200, "{body:?}");
    contract::summaries_shape(&body.body);
    assert_eq!(body.body["summaries"], json!([]));
    assert_eq!(
        get(&client, "/workflows/summaries?state=nope").await.status,
        400
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_schema_and_the_interface_of_a_registered_workflow() {
    let dir = tempfile::tempdir().unwrap();
    let client = app(dir.path(), "dev");
    let schema = get(&client, "/workflows/registered/family/schema").await;
    assert_eq!(schema.status, 200, "{schema:?}");
    contract::registered_schema_shape(&schema.body, "family");
    assert_eq!(
        get(&client, "/workflows/registered/nope/schema")
            .await
            .status,
        404
    );

    let interface = get(&client, "/workflows/registered/family/interface").await;
    assert_eq!(interface.status, 200, "{interface:?}");
    let [signals, queries, updates] = contract::interface_shape(&interface.body);
    assert_eq!(signals, ["wave"]);
    assert_eq!(queries, ["whoami"]);
    assert_eq!(updates, ["rename"]);
    let empty = get(&client, "/workflows/registered/kid/interface").await;
    let none: [Vec<String>; 3] = Default::default();
    assert_eq!(contract::interface_shape(&empty.body), none);
    assert_eq!(
        get(&client, "/workflows/registered/nope/interface")
            .await
            .status,
        404
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn children_tree_and_stack_list_the_child_runs() {
    let dir = tempfile::tempdir().unwrap();
    let client = app(dir.path(), "dev");
    let exec = family_run(&client).await;

    let children = get(&client, &format!("/workflows/{exec}/children")).await;
    contract::children_shape(&children.body);
    let items = children.body["items"].as_array().unwrap();
    assert!(items.iter().all(|child| child["status"] == "Running"));
    assert!(items.iter().all(|child| child["depth"] == 0));

    let page = get(&client, &format!("/workflows/{exec}/children?limit=1")).await;
    contract::children_shape(&page.body);
    let cursor = page.body["next_cursor"].as_str().expect("a next page");
    let rest = get(
        &client,
        &format!(
            "/workflows/{exec}/children?limit=1&cursor={}",
            urlencode(cursor)
        ),
    )
    .await;
    contract::children_shape(&rest.body);
    assert_eq!(rest.body["items"].as_array().map(Vec::len), Some(1));
    assert_ne!(
        rest.body["items"][0]["exec_id"],
        page.body["items"][0]["exec_id"]
    );

    let by_id = get(&client, "/workflows/by-id/family/fam-1/children").await;
    assert_eq!(by_id.execution_id.as_deref(), Some(exec.as_str()));
    contract::children_shape(&by_id.body);
    let done = get(
        &client,
        &format!("/workflows/{exec}/children?status=completed"),
    )
    .await;
    assert_eq!(done.body["items"], json!([]));
    assert_eq!(
        get(&client, &format!("/workflows/{exec}/children?status=nope"))
            .await
            .status,
        400
    );

    let tree = get(&client, &format!("/workflows/{exec}/tree")).await;
    assert_eq!(tree.status, 200, "{tree:?}");
    contract::tree_shape(&tree.body);
    assert_eq!(tree.body["node_count"], 3);
    let modes: Vec<&str> = tree.body["root"]["children"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|child| child["await_mode"].as_str())
        .collect();
    assert_eq!(modes.len(), 2);
    assert!(
        modes.contains(&"awaited") && modes.contains(&"detached"),
        "{modes:?}"
    );
    let summary = get(&client, &format!("/workflows/{exec}/tree?summary=true")).await;
    assert_eq!(summary.body["total_descendants"], 2, "{summary:?}");
    let shallow = get(&client, &format!("/workflows/{exec}/tree?max_nodes=1")).await;
    assert_eq!(shallow.body["truncated"], true, "{shallow:?}");

    let stack = get(&client, &format!("/workflows/{exec}/stack")).await;
    assert_eq!(stack.status, 200, "{stack:?}");
    contract::stack_shape(&stack.body);
    assert_eq!(
        stack.body["pending_child_workflows"]
            .as_array()
            .map(Vec::len),
        Some(2),
        "{stack:?}"
    );
    let stack_by_id = get(&client, "/workflows/by-id/family/fam-1/stack").await;
    contract::stack_shape(&stack_by_id.body);
    assert_eq!(stack_by_id.execution_id.as_deref(), Some(exec.as_str()));
}

#[tokio::test(flavor = "multi_thread")]
async fn the_stack_and_the_awaitables_name_the_pending_work() {
    let dir = tempfile::tempdir().unwrap();
    let client = app(dir.path(), "dev");
    let exec = start(&client, "napper", &json!({ "input": 3 })).await;
    let stack = get_until(&client, &format!("/workflows/{exec}/stack"), |body| {
        body["pending_activities"][0]["last_failure"].is_object()
    })
    .await;
    contract::stack_shape(&stack);
    let activity = &stack["pending_activities"][0];
    assert_eq!(activity["activity_name"], "flaky");
    assert_eq!(activity["max_attempts"], 3);
    assert_eq!(activity["last_failure"]["error"], "flaky");
    assert!(activity["next_retry_at"].is_string(), "{activity}");
    assert_eq!(stack["pending_timers"][0]["timer_id"], "nap");

    let awaitables = get(&client, &format!("/workflows/{exec}/awaitables")).await;
    assert_eq!(awaitables.status, 200, "{awaitables:?}");
    contract::awaitables_shape(&awaitables.body);
    assert_eq!(awaitables.body["wait_set"], "replayed", "{awaitables:?}");
    let kinds: Vec<&str> = awaitables.body["awaitables"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|awaitable| awaitable["kind"].as_str())
        .collect();
    assert!(
        kinds.contains(&"activity") && kinds.contains(&"timer"),
        "{kinds:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_terminal_run_has_an_empty_stack_and_no_awaitables() {
    let dir = tempfile::tempdir().unwrap();
    let client = app(dir.path(), "dev");
    let exec = start(&client, "kid", &json!({ "input": 0 })).await;
    client
        .post(&format!("/harvest/workflows/{exec}/terminate"))
        .send()
        .await
        .assert_status(202);
    let stack = get(&client, &format!("/workflows/{exec}/stack")).await;
    contract::stack_shape(&stack.body);
    assert_eq!(stack.body["is_terminal"], true);
    let awaitables = get(&client, &format!("/workflows/{exec}/awaitables")).await;
    contract::awaitables_shape(&awaitables.body);
    assert_eq!(awaitables.body["wait_set"], "terminal");
    assert_eq!(awaitables.body["awaitables"], json!([]));
}

#[tokio::test(flavor = "multi_thread")]
async fn the_run_chain_orders_a_continue_as_new_chain() {
    let dir = tempfile::tempdir().unwrap();
    let client = app(dir.path(), "dev");
    let first = start(
        &client,
        "relay",
        &json!({ "input": 0, "workflow_id": "r-1" }),
    )
    .await;
    let chain = get_until(&client, &format!("/workflows/{first}/run-chain"), |body| {
        body["runs"].as_array().is_some_and(|runs| runs.len() == 2)
    })
    .await;
    contract::run_chain_shape(&chain);
    assert_eq!(chain["head_unknown"], false);
    assert_eq!(chain["workflow_id"], "r-1");
    assert_eq!(chain["runs"][0]["exec_id"], first.as_str());
    assert_eq!(chain["runs"][0]["state"], "CONTINUED_AS_NEW");
    // The tail gives the same chain.
    let tail = chain["runs"][1]["exec_id"].as_str().unwrap().to_owned();
    let from_tail = get(&client, &format!("/workflows/{tail}/run-chain")).await;
    contract::run_chain_shape(&from_tail.body);
    assert_eq!(from_tail.body["runs"], chain["runs"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_timeline_logs_and_export_read_one_run() {
    let dir = tempfile::tempdir().unwrap();
    let client = app(dir.path(), "dev");
    let exec = family_run(&client).await;

    let timeline = get(&client, &format!("/workflows/{exec}/timeline")).await;
    assert_eq!(timeline.status, 200, "{timeline:?}");
    contract::timeline_shape(&timeline.body);
    assert_eq!(timeline.body["exec_id"], exec.as_str());
    assert_eq!(timeline.body["workflow_id"], "fam-1");

    let logs = get(&client, &format!("/workflows/{exec}/logs")).await;
    assert_eq!(logs.status, 200, "{logs:?}");
    contract::logs_shape(&logs.body);
    assert_eq!(logs.body["total_lines"], 1);
    assert_eq!(logs.body["lines"][0]["message"], "spawning kids");
    assert_eq!(logs.body["truncated"], false);
    let errors = get(&client, &format!("/workflows/{exec}/logs?level=error")).await;
    assert_eq!(errors.body["lines"], json!([]));
    assert_eq!(
        get(&client, &format!("/workflows/{exec}/logs?level=loud"))
            .await
            .status,
        400
    );

    let history = get(&client, &format!("/workflows/{exec}/history")).await;
    let events = history.body["events"].as_array().unwrap().len();
    let export = get(&client, &format!("/workflows/{exec}/history/export")).await;
    assert_eq!(export.status, 200, "{export:?}");
    contract::history_export_shape(&export.body, events);
    assert_eq!(export.body["workflow_id"], "fam-1");
    assert_eq!(export.body["payload_policy"], "redacted");
    let tiny = get(
        &client,
        &format!("/workflows/{exec}/history/export?max_bytes=1"),
    )
    .await;
    assert_eq!(tiny.status, 413, "{tiny:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn each_ported_route_answers_404_for_an_unknown_run() {
    let dir = tempfile::tempdir().unwrap();
    let client = app(dir.path(), "dev");
    let unknown = ExecutionId::new();
    for route in [
        "children",
        "tree",
        "run-chain",
        "timeline",
        "logs",
        "awaitables",
        "stack",
        "history/export",
    ] {
        let answer = get(&client, &format!("/workflows/{unknown}/{route}")).await;
        assert_eq!(answer.status, 404, "{route}: {answer:?}");
        let bad = get(&client, &format!("/workflows/not-a-uuid/{route}")).await;
        assert_eq!(bad.status, 400, "{route}: {bad:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_admin_reads_refuse_an_anonymous_caller_outside_dev() {
    let dir = tempfile::tempdir().unwrap();
    // `test` is not `dev`, so no admin boundary is open. `prod` would also
    // check the `Host` header.
    let client = app(dir.path(), "test");
    let unknown = ExecutionId::new();
    for path in [
        "/workflows/summaries".to_owned(),
        format!("/workflows/{unknown}/logs"),
        format!("/workflows/{unknown}/awaitables"),
    ] {
        let answer = get(&client, &path).await;
        assert_eq!(answer.status, 401, "{path}: {answer:?}");
    }
    // The other reads are not admin routes on Postgres either.
    assert_eq!(get(&client, "/workflows/count").await.status, 200);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_declarative_query_answers_over_http() {
    let dir = tempfile::tempdir().unwrap();
    let client = app(dir.path(), "dev");
    let exec = family_run(&client).await;
    let answer = get(&client, &format!("/workflows/{exec}/query/whoami")).await;
    assert_eq!(answer.status, 200, "{answer:?}");
    assert_eq!(answer.body, json!("fam-1"));
    let posted = client
        .post(&format!("/harvest/workflows/{exec}/query/whoami"))
        .json(&json!({}))
        .send()
        .await;
    posted.assert_ok();
    assert_eq!(posted.json::<Value>(), json!({ "result": "fam-1" }));
}

#[tokio::test(flavor = "multi_thread")]
async fn vantage_renders_the_run_list_and_one_run_page() {
    let dir = tempfile::tempdir().unwrap();
    let client = app(dir.path(), "dev");
    let exec = start(
        &client,
        "kid",
        &json!({ "input": "<script>alert(1)</script>", "workflow_id": "k-1" }),
    )
    .await;
    get_until(&client, &format!("/workflows/{exec}/history"), |body| {
        body["total_events"].as_u64().is_some_and(|n| n >= 1)
    })
    .await;

    let index = client.get("/harvest/ui").send().await;
    assert!(index.status.is_redirection(), "{:?}", index.status);
    let list = client.get("/harvest/ui/workflows").send().await;
    list.assert_ok();
    assert_eq!(
        list.header("content-type"),
        Some("text/html; charset=utf-8")
    );
    let page = String::from_utf8(list.body.clone()).unwrap();
    assert!(page.contains("Vantage"), "{page}");
    assert!(page.contains(&format!("workflows/{exec}")), "{page}");
    for postgres_only in ["dags", "workers", "schedules", "dead-letters"] {
        assert!(
            !page.contains(&format!("href=\"{postgres_only}")),
            "{postgres_only}"
        );
        assert!(
            !page.contains(&format!("href=\"../{postgres_only}")),
            "{postgres_only}"
        );
    }
    let filtered = client
        .get("/harvest/ui/workflows?state=COMPLETED")
        .send()
        .await;
    let filtered = String::from_utf8(filtered.body.clone()).unwrap();
    assert!(
        !filtered.contains(&format!("workflows/{exec}")),
        "{filtered}"
    );

    let detail = client
        .get(&format!("/harvest/ui/workflows/{exec}"))
        .send()
        .await;
    detail.assert_ok();
    let detail = String::from_utf8(detail.body.clone()).unwrap();
    assert!(detail.contains("k-1"), "{detail}");
    assert!(detail.contains("WorkflowStarted"), "{detail}");
    assert!(
        !detail.contains("<script>alert(1)</script>"),
        "the input is escaped"
    );
    assert!(detail.contains("&lt;script&gt;"), "{detail}");

    let missing = client
        .get(&format!("/harvest/ui/workflows/{}", ExecutionId::new()))
        .send()
        .await;
    missing.assert_status(404);
    client
        .get("/harvest/ui/workflows/nope")
        .send()
        .await
        .assert_status(400);
}

/// Percent-encode a cursor for a query string.
fn urlencode(raw: &str) -> String {
    raw.bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                char::from(byte).to_string()
            }
            other => format!("%{other:02X}"),
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn children_and_tree_walk_the_depth_and_check_their_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let client = app(dir.path(), "dev");
    let exec = start(&client, "elder", &json!({ "input": 0 })).await;
    let deep = get_until(
        &client,
        &format!("/workflows/{exec}/children?depth=1"),
        |body| {
            body["items"]
                .as_array()
                .is_some_and(|items| items.len() == 3)
        },
    )
    .await;
    contract::children_shape(&deep);
    let depths: Vec<u64> = deep["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|child| child["depth"].as_u64())
        .collect();
    assert_eq!(
        depths.iter().filter(|depth| **depth == 1).count(),
        2,
        "{deep}"
    );
    let direct = get(&client, &format!("/workflows/{exec}/children")).await;
    assert_eq!(direct.body["items"].as_array().map(Vec::len), Some(1));
    for bad in ["depth=6", "depth=x", "cursor=garbage", "limit=x"] {
        let answer = get(&client, &format!("/workflows/{exec}/children?{bad}")).await;
        assert_eq!(answer.status, 400, "{bad}: {answer:?}");
    }

    let tree = get(&client, &format!("/workflows/{exec}/tree")).await;
    contract::tree_shape(&tree.body);
    assert_eq!(tree.body["node_count"], 4, "{tree:?}");
    let shallow = get(&client, &format!("/workflows/{exec}/tree?max_depth=1")).await;
    contract::tree_shape(&shallow.body);
    assert_eq!(shallow.body["node_count"], 2, "{shallow:?}");
    assert_eq!(shallow.body["truncated"], true, "{shallow:?}");
    for bad in ["max_nodes=x", "max_depth=x", "summary=maybe"] {
        let answer = get(&client, &format!("/workflows/{exec}/tree?{bad}")).await;
        assert_eq!(answer.status, 400, "{bad}: {answer:?}");
    }

    // A detached child as the root keeps its own mode, as on Postgres.
    let detached = tree.body["root"]["children"][0]["children"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| node["await_mode"] == "detached")
        .expect("a detached grandchild")["execution_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let rooted = get(&client, &format!("/workflows/{detached}/tree")).await;
    assert_eq!(rooted.body["root"]["await_mode"], "detached", "{rooted:?}");
    assert_eq!(
        rooted.body["root"]["parent_close_policy"], "abandon",
        "{rooted:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_route_that_reads_no_query_key_refuses_one() {
    let dir = tempfile::tempdir().unwrap();
    let client = app(dir.path(), "dev");
    let exec = family_run(&client).await;
    for path in [
        format!("/workflows/{exec}/timeline?after=1"),
        format!("/workflows/{exec}/run-chain?x=1"),
        format!("/workflows/{exec}/awaitables?x=1"),
        format!("/workflows/{exec}/stack?x=1"),
        "/workflows/by-id/family/fam-1/stack?x=1".to_owned(),
        "/workflows/registered/family/schema?x=1".to_owned(),
        "/workflows/registered/family/interface?x=1".to_owned(),
    ] {
        assert_eq!(get(&client, &path).await.status, 400, "{path}");
    }
}

/// A run of a workflow that this app no longer registers degrades to the
/// history-only wait-set, as on Postgres.
#[tokio::test(flavor = "multi_thread")]
async fn the_awaitables_of_an_unregistered_workflow_read_history_only() {
    let dir = tempfile::tempdir().unwrap();
    let exec = {
        let client = app(dir.path(), "dev");
        let exec = start(&client, "napper", &json!({ "input": 1 })).await;
        get_until(&client, &format!("/workflows/{exec}/stack"), |body| {
            body["pending_timers"]
                .as_array()
                .is_some_and(|t| !t.is_empty())
        })
        .await;
        let handle = client
            .state()
            .extension::<autumn_harvest_plugin::sqlite::HarvestSqlite>()
            .unwrap();
        handle.shutdown().await.unwrap();
        exec
    };
    let client = TestApp::new()
        .config(super::config(dir.path()))
        .profile("dev")
        .plugin(
            HarvestPlugin::new()
                .workflows(workflows![kid])
                .api("/harvest"),
        )
        .build();
    let awaitables = get(&client, &format!("/workflows/{exec}/awaitables")).await;
    assert_eq!(awaitables.status, 200, "{awaitables:?}");
    contract::awaitables_shape(&awaitables.body);
    assert_eq!(awaitables.body["wait_set"], "history_only");
    assert_eq!(awaitables.body["wait_set_reason"], "handler_not_registered");
    let kinds: Vec<&str> = awaitables.body["awaitables"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|awaitable| awaitable["kind"].as_str())
        .collect();
    assert!(kinds.contains(&"timer"), "{kinds:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn vantage_redirects_keep_the_mount_and_the_list_pages() {
    let dir = tempfile::tempdir().unwrap();
    let client = app(dir.path(), "dev");
    for (path, target) in [
        ("/harvest/ui", "ui/workflows"),
        ("/harvest/ui/", "workflows"),
    ] {
        let index = client.get(path).send().await;
        assert!(index.status.is_redirection(), "{path}: {:?}", index.status);
        assert_eq!(index.header("location"), Some(target), "{path}");
    }
    let mut runs = Vec::new();
    for _ in 0..51 {
        runs.push(start(&client, "kid", &json!({ "input": 0 })).await);
    }
    let first = client.get("/harvest/ui/workflows").send().await;
    first.assert_ok();
    let first = String::from_utf8(first.body.clone()).unwrap();
    let marker = "href=\"workflows?cursor=";
    let at = first.find(marker).expect("a next-page link");
    let rest = &first[at + "href=\"".len()..];
    let next = &rest[..rest.find('"').unwrap()];
    // The oldest run is past the first page of 50.
    assert!(
        !first.contains(&format!("workflows/{}\"", runs[0])),
        "page one"
    );
    let second = client
        .get(&format!("/harvest/ui/{}", next.replace("&amp;", "&")))
        .send()
        .await;
    second.assert_ok();
    let second = String::from_utf8(second.body.clone()).unwrap();
    assert!(
        second.contains(&format!("workflows/{}", runs[0])),
        "{second}"
    );
    let bad = client.get("/harvest/ui/workflows?state=nope").send().await;
    bad.assert_ok();
    let bad = String::from_utf8(bad.body).unwrap();
    assert!(bad.contains("Not applied"), "{bad}");
}
