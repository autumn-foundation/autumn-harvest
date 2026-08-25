//! `GET /admin/history/export-sample` — the stratified in-flight sample export
//! that feeds the replay-drift gate (issue #798).
//!
//! The route answers "give me at most N in-flight histories per registered
//! workflow type, across every shard, plus how many I did *not* take" so a CI
//! job can replay that sample against the candidate build before promotion.
//!
//! AC1 — read-only export, at most N per workflow type, cross-shard, caller-
//!       specified non-terminal states.
//! AC2 — per-type coverage (`sampled` vs `in_flight_total`) so truncation is
//!       never silent.
//! AC3 — honours the existing `payload_policy` redaction/codec path.
//!
//! Dual-mode: uses a running Postgres from `HARVEST_TEST_DATABASE_URL` (as an
//! admin URL onto which fresh shard databases are created) when set — no Docker
//! required — else boots a fresh testcontainers Postgres. Docker is not
//! available in every sandbox, so these tests are compile-checked there and run
//! Docker-backed in CI (matching the #543/#544/#601 precedent).

use std::collections::BTreeMap;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::replay_sample::SampleManifest;
use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::store;
use autumn_harvest::types::{ActivityExecId, ExecutionId, ShardId};
use autumn_harvest::worker::DbPool;
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{HarvestApiState, harvest_api_router};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;
use uuid::Uuid;

type HarvestApiApp = axum::Router;

const SAMPLE_ROUTE: &str = "/admin/history/export-sample";

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// Swap the database-name path segment of a Postgres URL, preserving any query
/// string (e.g. `?sslmode=require`) the admin URL may carry.
fn shard_url(admin_url: &str, database: &str) -> String {
    let (base, query) = admin_url
        .split_once('?')
        .map_or((admin_url, None), |(base, query)| (base, Some(query)));
    let trimmed = base.trim_end_matches('/');
    let prefix = trimmed
        .rfind('/')
        .map_or(trimmed, |index| &trimmed[..=index]);
    query.map_or_else(
        || format!("{prefix}{database}"),
        |query| format!("{prefix}{database}?{query}"),
    )
}

async fn setup_shards(count: usize) -> (Vec<String>, Option<ContainerAsync<Postgres>>) {
    let (admin_url, guard) = if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        (url, None)
    } else {
        let container = Postgres::default()
            .with_tag("16")
            .start()
            .await
            .expect("failed to start Postgres container");
        let host = container.get_host().await.expect("host");
        let port = container.get_host_port_ipv4(5432).await.expect("port");
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
        (url, Some(container))
    };

    let names: Vec<String> = (0..count)
        .map(|_| format!("harvest_sample_{}", Uuid::new_v4().simple()))
        .collect();

    let mut admin = <AsyncPgConnection as AsyncConnection>::establish(&admin_url)
        .await
        .expect("admin connect");
    for db in &names {
        diesel::sql_query(format!("CREATE DATABASE {db}"))
            .execute(&mut admin)
            .await
            .expect("create db");
    }

    let mut urls = Vec::with_capacity(count);
    for db in &names {
        let url = shard_url(&admin_url, db);
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
            .await
            .expect("shard connect");
        conn.batch_execute(autumn_harvest::full_migrations_sql())
            .await
            .expect("migrate shard");
        urls.push(url);
    }
    (urls, guard)
}

fn build_pool(url: &str) -> DbPool {
    let manager =
        diesel_async::pooled_connection::AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            url,
        );
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("pool")
}

/// Build an admin-gated app over `urls` (one shard per URL, shard id = index).
fn build_app(urls: &[String]) -> HarvestApiApp {
    let mut pools = BTreeMap::new();
    for (index, url) in urls.iter().enumerate() {
        let shard = ShardId::new(i32::try_from(index).expect("shard index fits i32"));
        pools.insert(shard, build_pool(url));
    }
    let storage = HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)));

    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(storage);
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

/// Build an app whose **router** knows about more shards than this process has
/// pools for — the mid-rollout topology from the CLAUDE.md "add a shard"
/// procedure, where `readable_shards` is widened before every process has the
/// new shard's pool wired up.
fn build_app_with_router_knowing_extra_shard(
    urls: &[String],
    router_shards: &[i32],
) -> HarvestApiApp {
    use std::sync::Arc;

    use autumn_harvest::policy::WorkflowSchedule;
    use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
    use autumn_harvest::shard::ShardRouter;
    use autumn_harvest::worker::HandlerRegistry;
    use autumn_harvest_plugin::api::{HarvestApiRuntime, HarvestRetentionRuntime};

    let mut pools = BTreeMap::new();
    for (index, url) in urls.iter().enumerate() {
        let shard = ShardId::new(i32::try_from(index).expect("shard index fits i32"));
        pools.insert(shard, build_pool(url));
    }
    let storage = HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)));

    let shards: Vec<ShardId> = router_shards.iter().copied().map(ShardId::new).collect();
    let router = ShardRouter::new(shards.clone(), shards, ShardId::new(0));

    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(storage);
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::<WorkflowSchedule>::new()),
        None,
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        router,
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

async fn get_json(app: &HarvestApiApp, uri: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("request");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let json = serde_json::from_slice(&body).unwrap_or(Value::Null);
    (status, json)
}

/// Seed one execution with a two-event in-flight history.
///
/// `started_at` is explicit so the stratified `ORDER BY started_at` is
/// deterministic rather than dependent on insert timing.
async fn seed_execution(
    url: &str,
    shard: ShardId,
    workflow_name: &str,
    workflow_id: &str,
    state: &str,
    started_at: chrono::DateTime<chrono::Utc>,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(shard);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect");
    let row = NewWorkflowExecution {
        quota_key: None,
        continued_from_exec_id: None,
        first_exec_id: None,
        id: exec_id.as_uuid(),
        workflow_name,
        workflow_id,
        run_id: Uuid::new_v4(),
        shard_id: shard.as_i32(),
        input: json!({ "secret": "plaintext-payload" }),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        deadline_at: None,
        chain_execution_timeout: None,
        chain_deadline_at: None,
        memo: None,
        search_attrs: None,
        assigned_build_id: None,
        parent_close_policy: None,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
        sla: None,
        sla_deadline_at: None,
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        origin: None,
        completion_callbacks: None,
        start_source: None,
        start_source_ref: None,
        started_by: None,
    };
    diesel::insert_into(autumn_harvest::schema::harvest_workflow_executions::table)
        .values(&row)
        .execute(&mut conn)
        .await
        .expect("insert execution");

    let completed_at = if state == "RUNNING" || state == "PAUSED" {
        None
    } else {
        Some(started_at)
    };
    diesel::update(
        autumn_harvest::schema::harvest_workflow_executions::table.find(exec_id.as_uuid()),
    )
    .set((
        autumn_harvest::schema::harvest_workflow_executions::state.eq(state),
        autumn_harvest::schema::harvest_workflow_executions::started_at.eq(started_at),
        autumn_harvest::schema::harvest_workflow_executions::created_at.eq(started_at),
        autumn_harvest::schema::harvest_workflow_executions::completed_at.eq(completed_at),
    ))
    .execute(&mut conn)
    .await
    .expect("set state/time");

    // An in-flight history: started + one scheduled (still running) activity.
    let events = vec![
        WorkflowEvent::workflow_started(json!({ "secret": "plaintext-payload" }), started_at),
        WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "step_one".to_string(),
            input: json!({ "secret": "plaintext-payload" }),
            queue: "default".to_string(),
        },
    ];
    store::append_events(&mut conn, exec_id, &events, 0)
        .await
        .expect("append history");
    exec_id
}

fn manifest_of(body: &Value) -> SampleManifest {
    serde_json::from_value(body["manifest"].clone())
        .expect("response must carry a deserializable SampleManifest")
}

fn coverage_for<'a>(
    manifest: &'a SampleManifest,
    workflow: &str,
) -> &'a autumn_harvest::replay_sample::SampleWorkflowCoverage {
    manifest
        .per_workflow
        .iter()
        .find(|entry| entry.workflow_name == workflow)
        .unwrap_or_else(|| panic!("manifest must cover workflow '{workflow}'"))
}

fn exported_workflow_names(body: &Value) -> Vec<String> {
    body["exports"]
        .as_array()
        .expect("exports must be an array")
        .iter()
        .map(|doc| {
            doc["workflow_name"]
                .as_str()
                .expect("export must carry workflow_name")
                .to_string()
        })
        .collect()
}

fn base_time() -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .expect("parse base time")
        .with_timezone(&chrono::Utc)
}

// ---------------------------------------------------------------------------
// AC1 — stratified per-workflow-type cap
// ---------------------------------------------------------------------------

/// AC1 + AC2, the headline behaviour: an unbalanced fleet (one noisy workflow
/// type, one quiet one) must yield **at most N per type**, not the global
/// top-N — and must report the population it sampled *from*.
#[tokio::test]
async fn sample_is_stratified_per_workflow_type_and_reports_coverage() {
    let (urls, _guard) = setup_shards(1).await;
    let app = build_app(&urls);
    let t0 = base_time();

    // 7 noisy + 2 quiet. A naive `LIMIT 3` ordered by started_at would return
    // three `noisy` rows and zero `quiet` ones — the exact failure this route
    // exists to prevent.
    for index in 0..7 {
        seed_execution(
            &urls[0],
            ShardId::new(0),
            "noisy",
            &format!("noisy-{index}"),
            "RUNNING",
            t0 + chrono::Duration::seconds(index),
        )
        .await;
    }
    for index in 0..2 {
        seed_execution(
            &urls[0],
            ShardId::new(0),
            "quiet",
            &format!("quiet-{index}"),
            "RUNNING",
            t0 + chrono::Duration::seconds(100 + index),
        )
        .await;
    }

    let (status, body) = get_json(&app, &format!("{SAMPLE_ROUTE}?per_workflow=3")).await;
    assert_eq!(status, StatusCode::OK, "sample export must succeed: {body}");

    let names = exported_workflow_names(&body);
    let noisy = names.iter().filter(|name| *name == "noisy").count();
    let quiet = names.iter().filter(|name| *name == "quiet").count();
    assert_eq!(noisy, 3, "noisy must be capped at per_workflow: {body}");
    assert_eq!(
        quiet, 2,
        "quiet must be fully sampled (below the cap), not crowded out: {body}"
    );

    let manifest = manifest_of(&body);
    assert!(manifest.is_complete(), "single reachable shard is complete");
    let noisy_coverage = coverage_for(&manifest, "noisy");
    assert_eq!(noisy_coverage.sampled, 3);
    assert_eq!(
        noisy_coverage.in_flight_total, 7,
        "AC2: the population must be reported, not just the sample"
    );
    assert!(
        noisy_coverage.truncated(),
        "AC2: truncation must be explicit, never silent"
    );
    let quiet_coverage = coverage_for(&manifest, "quiet");
    assert_eq!(quiet_coverage.sampled, 2);
    assert_eq!(quiet_coverage.in_flight_total, 2);
    assert!(!quiet_coverage.truncated());
    assert!(
        manifest.is_truncated(),
        "the bundle as a whole is truncated"
    );
    assert_eq!(manifest.sampled_total, 5);
    assert_eq!(manifest.in_flight_total, 9);
}

/// AC1: terminal executions are never sampled — the gate replays *in-flight*
/// histories, which is what a deploy actually resumes.
#[tokio::test]
async fn terminal_executions_are_never_sampled() {
    let (urls, _guard) = setup_shards(1).await;
    let app = build_app(&urls);
    let t0 = base_time();

    seed_execution(&urls[0], ShardId::new(0), "wf", "live", "RUNNING", t0).await;
    seed_execution(
        &urls[0],
        ShardId::new(0),
        "wf",
        "done",
        "COMPLETED",
        t0 + chrono::Duration::seconds(1),
    )
    .await;
    seed_execution(
        &urls[0],
        ShardId::new(0),
        "wf",
        "dead",
        "FAILED",
        t0 + chrono::Duration::seconds(2),
    )
    .await;

    let (status, body) = get_json(&app, SAMPLE_ROUTE).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        exported_workflow_names(&body).len(),
        1,
        "only the RUNNING execution is in flight: {body}"
    );
    let manifest = manifest_of(&body);
    assert_eq!(coverage_for(&manifest, "wf").in_flight_total, 1);
}

/// AC1: `PAUSED` is in flight (it is a non-terminal active state, issue #383)
/// and is in the default state set.
#[tokio::test]
async fn paused_executions_are_sampled_by_default() {
    let (urls, _guard) = setup_shards(1).await;
    let app = build_app(&urls);
    let t0 = base_time();

    seed_execution(&urls[0], ShardId::new(0), "wf", "running", "RUNNING", t0).await;
    seed_execution(
        &urls[0],
        ShardId::new(0),
        "wf",
        "paused",
        "PAUSED",
        t0 + chrono::Duration::seconds(1),
    )
    .await;

    let (status, body) = get_json(&app, SAMPLE_ROUTE).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(exported_workflow_names(&body).len(), 2, "{body}");
    let manifest = manifest_of(&body);
    assert_eq!(
        manifest.states,
        vec!["PAUSED".to_string(), "RUNNING".to_string()],
        "the default state set must be recorded in the manifest"
    );
}

/// AC1: the caller may narrow the state set. `RUNNING`-only must exclude a
/// paused run.
#[tokio::test]
async fn caller_specified_states_narrow_the_sample() {
    let (urls, _guard) = setup_shards(1).await;
    let app = build_app(&urls);
    let t0 = base_time();

    seed_execution(&urls[0], ShardId::new(0), "wf", "running", "RUNNING", t0).await;
    seed_execution(
        &urls[0],
        ShardId::new(0),
        "wf",
        "paused",
        "PAUSED",
        t0 + chrono::Duration::seconds(1),
    )
    .await;

    let (status, body) = get_json(&app, &format!("{SAMPLE_ROUTE}?states=RUNNING")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(exported_workflow_names(&body).len(), 1, "{body}");
    let manifest = manifest_of(&body);
    assert_eq!(manifest.states, vec!["RUNNING".to_string()]);
}

/// A terminal state in `states` is a caller error — never a silently-empty
/// bundle, and never a gate that replays already-sealed histories.
#[tokio::test]
async fn terminal_state_request_is_rejected_with_400() {
    let (urls, _guard) = setup_shards(1).await;
    let app = build_app(&urls);

    let (status, _) = get_json(&app, &format!("{SAMPLE_ROUTE}?states=COMPLETED")).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a terminal state must be rejected, not silently dropped"
    );

    let (status, _) = get_json(&app, &format!("{SAMPLE_ROUTE}?states=NOT_A_STATE")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

// ---------------------------------------------------------------------------
// AC1 — cross-shard
// ---------------------------------------------------------------------------

/// AC1: the per-type cap is **global**, not per shard. Two shards each holding
/// 3 in-flight runs of one type, sampled at `per_workflow=4`, must yield 4 —
/// not 8 — and must report the merged population of 6.
#[tokio::test]
async fn per_workflow_cap_is_global_across_shards() {
    let (urls, _guard) = setup_shards(2).await;
    let app = build_app(&urls);
    let t0 = base_time();

    for index in 0..3 {
        seed_execution(
            &urls[0],
            ShardId::new(0),
            "wf",
            &format!("s0-{index}"),
            "RUNNING",
            t0 + chrono::Duration::seconds(index),
        )
        .await;
        seed_execution(
            &urls[1],
            ShardId::new(1),
            "wf",
            &format!("s1-{index}"),
            "RUNNING",
            t0 + chrono::Duration::seconds(10 + index),
        )
        .await;
    }

    let (status, body) = get_json(&app, &format!("{SAMPLE_ROUTE}?per_workflow=4")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        exported_workflow_names(&body).len(),
        4,
        "the per-type cap must be enforced globally, not per shard: {body}"
    );

    let manifest = manifest_of(&body);
    let coverage = coverage_for(&manifest, "wf");
    assert_eq!(coverage.sampled, 4);
    assert_eq!(
        coverage.in_flight_total, 6,
        "the population must be summed across shards"
    );
    assert_eq!(manifest.inspected_shards, vec![0, 1]);
}

/// Two **logical** shards backed by one **physical** database must not
/// double-count.
///
/// This topology is supported (and is why `execution::COUNT_WHERE_CLAUSE`,
/// `schedule_run_state_summary`, and `non_terminal_counts_by_workflow_name` all
/// carry `WHERE shard_id = $N`). Without the same predicate on the sample query,
/// the fan-out runs the identical statement twice over the identical rows and
/// corrupts the manifest in both directions at once:
///
/// * `in_flight_total` doubles — the denominator AC2 exists to state honestly
///   becomes a lie, so "50 of 4021" is really "50 of 2010" and an operator
///   under-reads their own coverage by half.
/// * every execution is sampled twice — the bundle carries duplicate fixtures,
///   so real per-type coverage is halved while `sampled` looks unchanged, and
///   the gate spends its budget replaying the same execution twice.
///
/// Both are silent. Nothing errors; the numbers are just wrong.
#[tokio::test]
async fn two_logical_shards_sharing_one_database_do_not_double_count() {
    let (urls, _guard) = setup_shards(1).await;
    // Both logical shards point at the SAME physical database.
    let app = build_app(&[urls[0].clone(), urls[0].clone()]);
    let t0 = base_time();

    for index in 0..3 {
        seed_execution(
            &urls[0],
            ShardId::new(0),
            "wf",
            &format!("shared-{index}"),
            "RUNNING",
            t0 + chrono::Duration::seconds(index),
        )
        .await;
    }

    let (status, body) = get_json(&app, &format!("{SAMPLE_ROUTE}?per_workflow=50")).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let manifest = manifest_of(&body);
    let coverage = coverage_for(&manifest, "wf");
    assert_eq!(
        coverage.in_flight_total, 3,
        "three executions exist; a shard-blind query would report six: {body}"
    );
    assert_eq!(
        exported_workflow_names(&body).len(),
        3,
        "each execution must be sampled once, not once per logical shard: {body}"
    );
    assert_eq!(coverage.sampled, 3);

    // Shard 1 owns none of these rows, so it contributes nothing — and that is
    // exactly what keeps the totals honest.
    assert_eq!(manifest.inspected_shards, vec![0, 1]);

    let ids: std::collections::BTreeSet<&str> = body["exports"]
        .as_array()
        .expect("exports")
        .iter()
        .map(|doc| doc["execution_id"].as_str().expect("execution_id"))
        .collect();
    assert_eq!(ids.len(), 3, "no execution may appear twice: {body}");
}

/// A shard the **router** knows about but this process has no pool for must be
/// reported `unavailable`, never silently omitted (Codex round-19 P1).
///
/// This is the mid-rollout topology from the CLAUDE.md "add a shard" procedure:
/// `readable_shards` is widened *before* every process has the new shard's pool
/// wired up. Enumerating only `pool.iter_shards()` omits that shard from BOTH
/// `inspected_shards` and `unavailable_shards` — and `SampleStatus::from_counts`
/// derives `Complete` from `unavailable == 0`.
///
/// The consequence is the worst one this gate has: the manifest claims complete
/// coverage, so even `require_complete_coverage(true)` certifies the candidate
/// without replaying a single in-flight execution from that shard.
#[tokio::test]
async fn a_router_known_shard_with_no_pool_is_reported_unavailable() {
    let (urls, _guard) = setup_shards(1).await;
    // Pool for shard 0 only; the router additionally advertises shard 1.
    let app = build_app_with_router_knowing_extra_shard(&urls[..1], &[0, 1]);
    let t0 = base_time();

    seed_execution(&urls[0], ShardId::new(0), "wf", "s0-0", "RUNNING", t0).await;

    let (status, body) = get_json(&app, &format!("{SAMPLE_ROUTE}?per_workflow=50")).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let manifest = manifest_of(&body);
    assert_eq!(
        manifest.inspected_shards,
        vec![0],
        "only shard 0 could actually be queried: {body}"
    );
    assert!(
        manifest
            .unavailable_shards
            .iter()
            .any(|entry| entry.contains('1')),
        "the router-known shard with no pool must be named unavailable, not \
         omitted — omitting it lets the status read `complete`: {body}"
    );
    assert!(
        !manifest.is_complete(),
        "a shard that was never queried cannot yield complete coverage: {body}"
    );
}

/// The complement: when the router knows exactly the shards that have pools,
/// coverage is genuinely complete. Pins that the fix does not simply make every
/// export report `partial`.
#[tokio::test]
async fn a_fully_pooled_router_still_reports_complete_coverage() {
    let (urls, _guard) = setup_shards(1).await;
    let app = build_app_with_router_knowing_extra_shard(&urls[..1], &[0]);
    let t0 = base_time();

    seed_execution(&urls[0], ShardId::new(0), "wf", "s0-0", "RUNNING", t0).await;

    let (status, body) = get_json(&app, &format!("{SAMPLE_ROUTE}?per_workflow=50")).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let manifest = manifest_of(&body);
    assert_eq!(manifest.inspected_shards, vec![0]);
    assert!(
        manifest.unavailable_shards.is_empty(),
        "every expected shard was queried: {body}"
    );
    assert!(
        manifest.is_complete(),
        "complete coverage must still be reachable: {body}"
    );
}

/// The engine's built-in liveness canary (issue #796) must never enter a bundle.
///
/// Its probe workflow is registered by the engine, not by the user's gate
/// binary, so a sampled probe lands in `blocked` — and `blocked` dominates the
/// exit-code ladder. A single in-flight probe would therefore pin the gate at
/// exit 2 permanently on any deployment that enables the canary, which is the
/// deployments that care most about deploy safety.
#[tokio::test]
async fn liveness_canary_probes_are_never_sampled() {
    let (urls, _guard) = setup_shards(1).await;
    let app = build_app(&urls);
    let t0 = base_time();

    let canary_name = format!(
        "{}__default",
        autumn_harvest::canary::CANARY_WORKFLOW_NAME_PREFIX
    );
    seed_execution(
        &urls[0],
        ShardId::new(0),
        &canary_name,
        "canary-1",
        "RUNNING",
        t0,
    )
    .await;
    seed_execution(
        &urls[0],
        ShardId::new(0),
        "wf",
        "real-1",
        "RUNNING",
        t0 + chrono::Duration::seconds(1),
    )
    .await;

    let (status, body) = get_json(&app, SAMPLE_ROUTE).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    assert_eq!(
        exported_workflow_names(&body),
        vec!["wf".to_string()],
        "an engine-internal canary probe must not reach a user gate: {body}"
    );

    let manifest = manifest_of(&body);
    assert!(
        manifest
            .per_workflow
            .iter()
            .all(|entry| entry.workflow_name != canary_name),
        "the canary must not appear in the coverage record either — it would \
         inflate the denominator with runs the gate can never verify: {body}"
    );
    assert_eq!(manifest.in_flight_total, 1);
}

/// An unreachable shard degrades the manifest to `partial` and names the shard
/// — the sample is a *lower bound*, never a silently-complete-looking bundle.
#[tokio::test]
async fn unreachable_shard_degrades_the_manifest_to_partial() {
    let (urls, _guard) = setup_shards(2).await;
    let t0 = base_time();
    seed_execution(&urls[0], ShardId::new(0), "wf", "live", "RUNNING", t0).await;

    // Retarget shard 1 at a database that does not exist.
    let down = urls[1].replace("harvest_sample_", "missing_db_");
    let app = build_app(&[urls[0].clone(), down]);

    let (status, body) = get_json(&app, SAMPLE_ROUTE).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a down shard degrades, never 500s: {body}"
    );
    let manifest = manifest_of(&body);
    assert!(
        !manifest.is_complete(),
        "an unreachable shard must not report complete coverage: {body}"
    );
    assert_eq!(
        manifest.status,
        autumn_harvest::replay_sample::SampleStatus::Partial
    );
    assert_eq!(manifest.inspected_shards, vec![0]);
    assert!(
        !manifest.unavailable_shards.is_empty(),
        "the unreachable shard must be named: {body}"
    );
}

/// A shard whose **connection succeeds but whose query fails** must not be
/// counted as inspected.
///
/// This is the distinct sibling of the unreachable-shard case above: there the
/// failure happens at `acquire_conn`, here it happens one step later. Counting
/// the shard the moment its connection is acquired puts it in *both* the
/// inspected and unavailable lists, and `SampleStatus::from_counts` reads
/// `inspected == 0` as its "unavailable" signal — so a single-shard deployment
/// whose query fails would report `partial` while naming one supposedly
/// inspected shard, overstating coverage in the very record the gate uses to
/// decide whether coverage was proven.
///
/// The fault is injected by dropping the table the sample query reads, which
/// leaves the connection perfectly healthy.
#[tokio::test]
async fn a_shard_whose_query_fails_is_not_counted_as_inspected() {
    let (urls, _guard) = setup_shards(1).await;

    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&urls[0])
        .await
        .expect("shard connect");
    conn.batch_execute("DROP TABLE harvest_workflow_executions CASCADE")
        .await
        .expect("drop the table the sample query reads");

    let app = build_app(&urls);
    let (status, body) = get_json(&app, SAMPLE_ROUTE).await;

    assert_eq!(
        status,
        StatusCode::OK,
        "a failing shard query degrades, never 500s: {body}"
    );
    let manifest = manifest_of(&body);
    assert!(
        manifest.inspected_shards.is_empty(),
        "a shard whose query failed was never successfully inspected: {body}"
    );
    assert_eq!(
        manifest.status,
        autumn_harvest::replay_sample::SampleStatus::Unavailable,
        "the only shard failed, so coverage is unavailable — not partial: {body}"
    );
    assert!(
        !manifest.unavailable_shards.is_empty(),
        "the failing shard must be named: {body}"
    );
}

// ---------------------------------------------------------------------------
// AC3 — payload policy
// ---------------------------------------------------------------------------

/// AC3: the sample export routes through the same redaction path as the batch
/// export. The default (`redacted`) must not leak the plaintext payload.
#[tokio::test]
async fn redacted_policy_is_the_default_and_strips_payloads() {
    let (urls, _guard) = setup_shards(1).await;
    let app = build_app(&urls);
    seed_execution(
        &urls[0],
        ShardId::new(0),
        "wf",
        "live",
        "RUNNING",
        base_time(),
    )
    .await;

    let (status, body) = get_json(&app, SAMPLE_ROUTE).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["payload_policy"], json!("redacted"));
    let rendered = serde_json::to_string(&body["exports"]).expect("serialize exports");
    assert!(
        !rendered.contains("plaintext-payload"),
        "the default policy must redact payloads: {rendered}"
    );

    let (status, full) = get_json(&app, &format!("{SAMPLE_ROUTE}?payload_policy=full")).await;
    assert_eq!(status, StatusCode::OK, "{full}");
    assert_eq!(full["payload_policy"], json!("full"));
    let rendered = serde_json::to_string(&full["exports"]).expect("serialize exports");
    assert!(
        rendered.contains("plaintext-payload"),
        "the full policy must carry payloads: {rendered}"
    );
}

// ---------------------------------------------------------------------------
// Bundle round-trip: the whole point of the route
// ---------------------------------------------------------------------------

/// The exported documents must be exactly the shape `WorkflowReplayer` /
/// `replay_bundle` reads — a full round-trip through `HistorySnapshot`.
#[tokio::test]
async fn exported_documents_deserialize_as_replay_snapshots() {
    let (urls, _guard) = setup_shards(1).await;
    let app = build_app(&urls);
    seed_execution(
        &urls[0],
        ShardId::new(0),
        "round_trip_wf",
        "live",
        "RUNNING",
        base_time(),
    )
    .await;

    let (status, body) = get_json(&app, &format!("{SAMPLE_ROUTE}?payload_policy=full")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let exports = body["exports"].as_array().expect("exports array");
    assert_eq!(exports.len(), 1, "{body}");

    let snapshot: autumn_harvest::testing::HistorySnapshot =
        serde_json::from_value(exports[0].clone())
            .expect("an export document must deserialize as a replay HistorySnapshot");
    assert_eq!(snapshot.workflow_name, "round_trip_wf");
    assert_eq!(
        snapshot.events.len(),
        2,
        "the in-flight history must round-trip verbatim"
    );
}

// ---------------------------------------------------------------------------
// Guardrails
// ---------------------------------------------------------------------------

/// The route is admin-gated, like every other `/admin/*` read.
#[tokio::test]
async fn sample_export_requires_admin() {
    let (urls, _guard) = setup_shards(1).await;
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_pool(&urls[0]));
    let storage = HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)));
    let api_state = HarvestApiState::new();
    // No `set_admin_auth_boundary(true)` and no session: an unauthenticated
    // caller must be rejected.
    api_state.install_storage_pool(storage);
    let app = harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"));

    let (status, _) = get_json(&app, SAMPLE_ROUTE).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

/// `per_workflow` is clamped rather than trusted, so a caller cannot ask for an
/// unbounded export.
#[tokio::test]
async fn per_workflow_is_clamped_and_reported() {
    let (urls, _guard) = setup_shards(1).await;
    let app = build_app(&urls);
    seed_execution(
        &urls[0],
        ShardId::new(0),
        "wf",
        "live",
        "RUNNING",
        base_time(),
    )
    .await;

    let (status, body) = get_json(&app, &format!("{SAMPLE_ROUTE}?per_workflow=100000")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["filters"]["per_workflow"],
        json!(autumn_harvest::replay_sample::MAX_PER_WORKFLOW_SAMPLE),
        "an over-large request must be clamped and the effective value echoed: {body}"
    );

    let (status, _) = get_json(&app, &format!("{SAMPLE_ROUTE}?per_workflow=not_a_number")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// An idle fleet yields an empty-but-well-formed bundle: the manifest still
/// says `complete`, so the *harness* (not the exporter) decides whether an
/// empty bundle should block promotion.
#[tokio::test]
async fn idle_fleet_yields_an_empty_but_complete_manifest() {
    let (urls, _guard) = setup_shards(1).await;
    let app = build_app(&urls);

    let (status, body) = get_json(&app, SAMPLE_ROUTE).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["exports"].as_array().expect("array").is_empty());
    let manifest = manifest_of(&body);
    assert!(manifest.is_complete());
    assert!(!manifest.is_truncated());
    assert_eq!(manifest.sampled_total, 0);
    assert_eq!(manifest.in_flight_total, 0);
    assert!(manifest.per_workflow.is_empty());
}

// ---------------------------------------------------------------------------
// End-to-end: export -> bundle -> drift gate
// ---------------------------------------------------------------------------

/// The candidate build's workflow: schedules `step_one` then `step_two`.
fn canonical_workflow<'a>(
    ctx: &'a autumn_harvest::context::WorkflowContext,
    _input: Value,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("step_one", Value::Null, "default")
            .await
            .map_err(|error| error.to_string())?;
        ctx.execute_activity_raw("step_two", Value::Null, "default")
            .await
            .map_err(|error| error.to_string())?;
        Ok(Value::Null)
    })
}

/// A build that swapped the two activities — the classic determinism
/// regression a deploy introduces.
fn reordered_workflow<'a>(
    ctx: &'a autumn_harvest::context::WorkflowContext,
    _input: Value,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("step_two", Value::Null, "default")
            .await
            .map_err(|error| error.to_string())?;
        ctx.execute_activity_raw("step_one", Value::Null, "default")
            .await
            .map_err(|error| error.to_string())?;
        Ok(Value::Null)
    })
}

/// Seed an execution whose recorded history parks mid-flight on `step_one`.
async fn seed_in_flight_two_step(url: &str, shard: ShardId, workflow_id: &str) -> ExecutionId {
    let t0 = base_time();
    let exec_id = seed_execution(url, shard, "gate_wf", workflow_id, "RUNNING", t0).await;
    // `seed_execution` wrote a generic 2-event history; replace it with one that
    // matches `canonical_workflow`'s first command so the bundle is a realistic
    // in-flight fixture rather than a synthetic one.
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect");
    diesel::delete(
        autumn_harvest::schema::harvest_events::table
            .filter(autumn_harvest::schema::harvest_events::workflow_exec_id.eq(exec_id.as_uuid())),
    )
    .execute(&mut conn)
    .await
    .expect("clear seeded history");
    let events = vec![
        WorkflowEvent::workflow_started(Value::Null, t0),
        WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "step_one".to_string(),
            input: Value::Null,
            queue: "default".to_string(),
        },
    ];
    store::append_events(&mut conn, exec_id, &events, 0)
        .await
        .expect("append in-flight history");
    exec_id
}

/// Mirror of the CLI's bundle writer, so this test exercises the same file
/// naming and manifest placement an operator's `--output-dir` produces.
fn write_bundle(dir: &std::path::Path, body: &Value) {
    std::fs::create_dir_all(dir).expect("create bundle dir");
    for export in body["exports"].as_array().expect("exports") {
        let name = autumn_harvest::replay_sample::fixture_file_name(
            export["workflow_name"].as_str().unwrap_or("workflow"),
            export["execution_id"].as_str().unwrap_or("unknown"),
        );
        std::fs::write(
            dir.join(name),
            serde_json::to_string_pretty(export).expect("serialize fixture"),
        )
        .expect("write fixture");
    }
    std::fs::write(
        dir.join(SampleManifest::FILE_NAME),
        serde_json::to_string_pretty(&body["manifest"]).expect("serialize manifest"),
    )
    .expect("write manifest");
}

/// **The money test.** Export a real in-flight sample, write it as a bundle,
/// and run the drift gate over it: clean against the code that produced the
/// history, non-zero against a build that reordered its activities.
///
/// This is the whole loop issue #798 exists to make possible, end to end
/// through the real HTTP route — not a hand-built fixture.
#[tokio::test]
async fn exported_bundle_passes_the_gate_and_catches_a_reordering_regression() {
    let (urls, _guard) = setup_shards(1).await;
    let app = build_app(&urls);
    for index in 0..3 {
        seed_in_flight_two_step(&urls[0], ShardId::new(0), &format!("gate-{index}")).await;
    }

    let (status, body) = get_json(&app, &format!("{SAMPLE_ROUTE}?payload_policy=full")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["exports"].as_array().expect("exports").len(), 3);

    let dir = tempfile::tempdir().expect("tempdir");
    write_bundle(dir.path(), &body);

    // The candidate build is unchanged: the gate must pass, and pass *because*
    // it replayed the fixtures — not because it found none.
    let clean = autumn_harvest::testing::WorkflowReplayer::new()
        .register_fn("gate_wf", canonical_workflow)
        .replay_bundle(dir.path())
        .await;
    assert!(
        clean.is_clean(),
        "an unchanged build must pass the gate over its own in-flight histories: {clean}"
    );
    assert_eq!(clean.exit_code(), 0);
    assert_eq!(
        clean.total, 3,
        "all three fixtures must be replayed: {clean}"
    );
    assert_eq!(clean.succeeded, 3);
    assert!(
        clean.coverage.is_some(),
        "the manifest must be read back as coverage, not replayed as a fixture: {clean}"
    );

    // A build that reordered its activities must be blocked.
    let drifted = autumn_harvest::testing::WorkflowReplayer::new()
        .register_fn("gate_wf", reordered_workflow)
        .replay_bundle(dir.path())
        .await;
    assert!(
        !drifted.is_clean(),
        "a reordering regression must block promotion: {drifted}"
    );
    assert_ne!(drifted.exit_code(), 0);
    assert_eq!(
        drifted.diverged.len(),
        3,
        "every affected in-flight execution must be reported, not just the first: {drifted}"
    );
    for drift in &drifted.diverged {
        assert_eq!(drift.workflow_name, "gate_wf");
        assert!(
            drift.execution_id.is_some(),
            "a drift must name the execution an operator can inspect: {drift:?}"
        );
        assert!(
            drift.first_divergence.contains("step_two"),
            "the divergence must name the mismatch: {drift:?}"
        );
    }
}

/// **Success metric**: export + replay of a 1,000-execution cross-shard sample
/// completes in well under 30 s, and catches a regression with zero false
/// negatives on the sampled set.
///
/// Both halves are asserted against the same bundle, because the metric is only
/// meaningful as a pair — a gate that is fast because it verifies nothing, or
/// thorough but too slow to sit in CI, satisfies neither. The wall clock covers
/// the whole loop an operator actually runs: the cross-shard stratified query,
/// the export pipeline (payload policy, size limits, per-entry failures), the
/// bundle write, and 1,000 replays.
///
/// Ignored by default: seeding 1,000 executions with histories is far heavier
/// than the rest of this suite. Run it deliberately:
///
/// ```bash
/// cargo test -p autumn-harvest-plugin --test replay_sample_export_integration \
///   -- --ignored thousand_execution
/// ```
#[tokio::test]
#[ignore = "perf: seeds 1000 executions; run explicitly"]
async fn thousand_execution_cross_shard_sample_exports_and_replays_within_budget() {
    const TOTAL: usize = 1_000;
    const SHARDS: usize = 2;
    // 20 types x 50 (the documented default per-workflow cap) = 1000, so the
    // sample is genuinely stratified rather than one type's rows.
    const TYPES: usize = 20;
    // The issue's metric describes the operator-facing gate, which ships
    // release-built. Replay is pure local CPU and dominates this test (measured:
    // ~96% of the wall clock, with the cross-shard query + export pipeline under
    // a second), and a debug build pays a large tax on exactly that path — so
    // asserting the literal 30 s against a debug build asserts something far
    // stricter than the metric and would flake on a loaded runner. Release
    // replay throughput is separately pinned by the pre-existing
    // `replay_verifier_bench` (issue #251): 1,000 fixtures averaging 1k events
    // each in under 30 s, versus the ~3 events per fixture here.
    const BUDGET: std::time::Duration = if cfg!(debug_assertions) {
        std::time::Duration::from_secs(120)
    } else {
        std::time::Duration::from_secs(30)
    };

    let (urls, _guard) = setup_shards(SHARDS).await;
    let app = build_app(&urls);

    for index in 0..TOTAL {
        let shard_index = index % SHARDS;
        seed_in_flight_named(
            &urls[shard_index],
            ShardId::new(i32::try_from(shard_index).expect("shard fits")),
            &format!("perf_wf_{:02}", index % TYPES),
            &format!("perf-{index}"),
        )
        .await;
    }

    // The clock starts at the export, not the seeding: seeding is test setup,
    // the export-and-replay loop is what an operator pays for.
    let started = std::time::Instant::now();

    let (status, body) = get_json(
        &app,
        &format!("{SAMPLE_ROUTE}?payload_policy=full&per_workflow=50"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let export_took = started.elapsed();

    let dir = tempfile::tempdir().expect("tempdir");
    write_bundle(dir.path(), &body);
    // Saturating throughout: these are diagnostic phase splits, and a clock
    // anomaly must never panic a passing perf run.
    let write_took = started.elapsed().saturating_sub(export_took);

    let manifest = manifest_of(&body);
    assert_eq!(
        manifest.sampled_total, TOTAL as u64,
        "the whole population must be sampled for this measurement to mean \
         anything: {manifest:?}"
    );
    assert_eq!(manifest.inspected_shards.len(), SHARDS);

    // Clean against the code that produced the histories...
    let mut clean_replayer = autumn_harvest::testing::ReplayVerifier::new();
    for type_index in 0..TYPES {
        clean_replayer =
            clean_replayer.register_fn(format!("perf_wf_{type_index:02}"), canonical_workflow);
    }
    let clean = clean_replayer.replay_bundle(dir.path()).await;
    assert_eq!(clean.total, TOTAL, "{clean}");
    assert!(
        clean.is_clean(),
        "unchanged code must replay clean: {clean}"
    );

    let elapsed = started.elapsed();
    let replay_took = elapsed
        .saturating_sub(export_took)
        .saturating_sub(write_took);
    // Phase breakdown so a budget failure says *which* stage regressed rather
    // than only that the total moved.
    let phases =
        format!("export {export_took:?} + bundle write {write_took:?} + replay {replay_took:?}");
    assert!(
        elapsed < BUDGET,
        "export + replay of {TOTAL} executions took {elapsed:?} ({phases}), \
         over the {BUDGET:?} budget"
    );

    // ...and catches the regression on EVERY affected execution. Zero false
    // negatives on the sampled set is the load-bearing half: a fast gate that
    // misses drift is worse than no gate.
    let mut drift_replayer = autumn_harvest::testing::ReplayVerifier::new();
    for type_index in 0..TYPES {
        drift_replayer =
            drift_replayer.register_fn(format!("perf_wf_{type_index:02}"), reordered_workflow);
    }
    let drifted = drift_replayer.replay_bundle(dir.path()).await;
    assert_eq!(
        drifted.diverged.len(),
        TOTAL,
        "every sampled execution must be reported as drifted — a missed one is \
         a false negative: {drifted}"
    );
    assert_eq!(drifted.exit_code(), 1);

    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    println!(
        "#798 success metric [{profile}]: {TOTAL} executions across {SHARDS} shards \
         exported + replayed in {elapsed:?} (budget {BUDGET:?}) -- {phases}"
    );
}

/// `seed_in_flight_two_step` with a caller-chosen workflow type, so a perf run
/// can spread executions across many registered types.
async fn seed_in_flight_named(
    url: &str,
    shard: ShardId,
    workflow_name: &str,
    workflow_id: &str,
) -> ExecutionId {
    let t0 = base_time();
    let exec_id = seed_execution(url, shard, workflow_name, workflow_id, "RUNNING", t0).await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect");
    diesel::delete(
        autumn_harvest::schema::harvest_events::table
            .filter(autumn_harvest::schema::harvest_events::workflow_exec_id.eq(exec_id.as_uuid())),
    )
    .execute(&mut conn)
    .await
    .expect("clear seeded history");
    let events = vec![
        WorkflowEvent::workflow_started(Value::Null, t0),
        WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "step_one".to_string(),
            input: Value::Null,
            queue: "default".to_string(),
        },
    ];
    store::append_events(&mut conn, exec_id, &events, 0)
        .await
        .expect("append in-flight history");
    exec_id
}

/// A bundle exported under the **default** (`redacted`) payload policy must never
/// be reported as a determinism regression.
///
/// Redaction rewrites payload-bearing fields in place, and canary replay sets
/// `strict_replay = true` — so `match_activity_strict` compares the *redacted
/// stub* against the input the workflow actually computes and diverges on every
/// fixture. Reporting that as "your code drifted" would block a healthy build and
/// send the operator hunting a regression that does not exist.
///
/// The gate must instead refuse the bundle as unreplayable (a harness error,
/// exit 2) and say how to fix it.
#[tokio::test]
async fn a_redacted_bundle_is_refused_not_reported_as_drift() {
    let (urls, _guard) = setup_shards(1).await;
    let app = build_app(&urls);
    seed_in_flight_two_step(&urls[0], ShardId::new(0), "redacted-gate").await;

    // No `payload_policy` override: this is exactly what the documented
    // `harvest history export-sample --output-dir ...` produces.
    let (status, body) = get_json(&app, SAMPLE_ROUTE).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["payload_policy"], json!("redacted"));
    assert_eq!(body["exports"].as_array().expect("exports").len(), 1);

    let dir = tempfile::tempdir().expect("tempdir");
    write_bundle(dir.path(), &body);

    let report = autumn_harvest::testing::WorkflowReplayer::new()
        .register_fn("gate_wf", canonical_workflow)
        .replay_bundle(dir.path())
        .await;

    assert!(
        report.diverged.is_empty(),
        "a redacted bundle must never be reported as code drift — the code did \
         not change, the payloads were stripped: {report}"
    );
    assert_eq!(
        report.blocked.len(),
        1,
        "the redacted fixture must be blocked as unreplayable: {report}"
    );
    assert_eq!(
        report.exit_code(),
        2,
        "a bundle the harness cannot evaluate is a harness error, not drift: {report}"
    );
    let reason = format!("{:?}", report.blocked[0].reason);
    assert!(
        reason.contains("redacted") && reason.contains("--payload-policy full"),
        "the block must tell the operator how to produce a replayable bundle: {reason}"
    );
}

/// The exported fixture must carry the execution's own `context_headers`.
///
/// Headers live in no `WorkflowEvent`, so a fixture that drops them replays the
/// workflow against an *empty* header map. A workflow that branches on
/// `ctx.header(..)` — or embeds one in an activity input — then takes the other
/// branch and the gate reports a divergence the candidate build did not cause:
/// a false red on healthy code, which is how a determinism gate loses its
/// audience. The single-execution export and the DB replay canary both carry
/// them; the sample export must have parity.
#[tokio::test]
async fn exported_fixture_carries_context_headers_for_replay() {
    let (urls, _guard) = setup_shards(1).await;
    let app = build_app(&urls);
    let exec_id = seed_in_flight_two_step(&urls[0], ShardId::new(0), "hdr-1").await;

    // Attach ambient headers to the row, exactly as a live start would.
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&urls[0])
        .await
        .expect("connect");
    diesel::update(
        autumn_harvest::schema::harvest_workflow_executions::table
            .filter(autumn_harvest::schema::harvest_workflow_executions::id.eq(exec_id.as_uuid())),
    )
    .set(
        autumn_harvest::schema::harvest_workflow_executions::context_headers
            .eq(Some(json!({ "tenant-tier": "premium" }))),
    )
    .execute(&mut conn)
    .await
    .expect("attach headers");

    let (status, body) = get_json(&app, &format!("{SAMPLE_ROUTE}?payload_policy=full")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let export = &body["exports"].as_array().expect("exports")[0];

    assert_eq!(
        export["context_headers"]["tenant-tier"],
        json!("premium"),
        "the fixture must carry the ambient headers the live run saw: {export}"
    );

    // And they must survive the round-trip into the shape the gate reads.
    let snapshot: autumn_harvest::testing::HistorySnapshot =
        serde_json::from_value(export.clone()).expect("fixture parses as a replay snapshot");
    assert_eq!(
        snapshot
            .context_headers
            .as_ref()
            .and_then(|headers| headers.get("tenant-tier"))
            .map(String::as_str),
        Some("premium"),
        "a header-branching workflow would replay against an empty map otherwise"
    );
}

/// A `workflow_name` filter narrows the sample to one registered type.
#[tokio::test]
async fn workflow_name_filter_narrows_the_sample() {
    let (urls, _guard) = setup_shards(1).await;
    let app = build_app(&urls);
    let t0 = base_time();
    seed_execution(&urls[0], ShardId::new(0), "alpha", "a", "RUNNING", t0).await;
    seed_execution(
        &urls[0],
        ShardId::new(0),
        "beta",
        "b",
        "RUNNING",
        t0 + chrono::Duration::seconds(1),
    )
    .await;

    let (status, body) = get_json(&app, &format!("{SAMPLE_ROUTE}?workflow_name=alpha")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(exported_workflow_names(&body), vec!["alpha".to_string()]);
    let manifest = manifest_of(&body);
    assert_eq!(manifest.per_workflow.len(), 1);
}

/// Round-23 P2: discovery must not materialize the whole per-type superset, and
/// bounding it must not cost the population census.
///
/// The per-type limit is `per_workflow`, but nothing bounds the number of
/// *types*, so discovery returned `types × per_workflow` rows for the
/// stratifier to throw all but `MAX_SAMPLE_TOTAL` of away. The fix bounds each
/// shard by the effective share the global budget allows — which is only safe
/// if the floor of one row per type holds, because the manifest's
/// `in_flight_total` (and therefore `zero_coverage_types`, the "this type was
/// never replayed" alarm) is read off the first row of each name partition.
///
/// So this seeds many types, each with more executions than the share allows,
/// and asserts the property that a naive `LIMIT` would break: **every type is
/// still present in the manifest with its true population**, even the ones the
/// budget starves down to a single fixture.
#[tokio::test]
async fn many_types_keep_their_population_census_under_the_discovery_bound() {
    // Three executions each, so every type is genuinely truncatable.
    const TYPES: usize = 12;

    let (urls, _guard) = setup_shards(1).await;
    let app = build_app(&urls);
    let t0 = base_time();

    for type_index in 0..TYPES {
        for exec_index in 0..3i64 {
            seed_execution(
                &urls[0],
                ShardId::new(0),
                &format!("wf_{type_index:03}"),
                &format!("wf-{type_index:03}-{exec_index}"),
                "RUNNING",
                t0 + chrono::Duration::seconds(exec_index),
            )
            .await;
        }
    }

    let (status, body) = get_json(&app, &format!("{SAMPLE_ROUTE}?per_workflow=2")).await;
    assert_eq!(status, StatusCode::OK, "sample export must succeed: {body}");

    let manifest = manifest_of(&body);
    assert_eq!(
        manifest.per_workflow.len(),
        TYPES,
        "every in-flight type must appear in the manifest — a type dropped by a \
         discovery bound is a type the gate can say nothing about, with nothing \
         in the report to reveal it: {body}"
    );
    for type_index in 0..TYPES {
        let name = format!("wf_{type_index:03}");
        let coverage = coverage_for(&manifest, &name);
        assert_eq!(
            coverage.in_flight_total, 3,
            "{name}: the true population must survive the bound, or the coverage \
             denominator understates the fleet: {body}"
        );
        assert!(
            coverage.sampled >= 1,
            "{name}: every type must keep at least one fixture: {body}"
        );
    }

    // And the sample itself is unchanged by the bound: `per_workflow = 2` is far
    // below the global share here, so it is the binding constraint exactly as
    // before.
    let names = exported_workflow_names(&body);
    for type_index in 0..TYPES {
        let name = format!("wf_{type_index:03}");
        assert_eq!(
            names.iter().filter(|found| **found == name).count(),
            2,
            "{name}: below the global share, per_workflow still binds: {body}"
        );
    }
    assert_eq!(manifest.sampled_total, (TYPES * 2) as u64);
    assert_eq!(manifest.in_flight_total, (TYPES * 3) as u64);
}

/// The exporter must record *which* executions it wrote, not just how many.
///
/// `sampled` alone accepts any substitution that preserves the count, so the
/// gate cannot tell the bundle it was handed apart from a bundle of the right
/// shape (round-23 P1). The identities are what
/// `ReplayDriftReport::bundle_inventory_mismatches` reconciles against, so an
/// exporter that stopped recording them would silently disarm that check.
#[tokio::test]
async fn the_manifest_records_the_exported_execution_identities() {
    let (urls, _guard) = setup_shards(1).await;
    let app = build_app(&urls);
    let t0 = base_time();

    for index in 0..3 {
        seed_execution(
            &urls[0],
            ShardId::new(0),
            "wf",
            &format!("wf-{index}"),
            "RUNNING",
            t0 + chrono::Duration::seconds(index),
        )
        .await;
    }

    let (status, body) = get_json(&app, &format!("{SAMPLE_ROUTE}?per_workflow=2")).await;
    assert_eq!(status, StatusCode::OK, "sample export must succeed: {body}");

    let manifest = manifest_of(&body);
    let coverage = coverage_for(&manifest, "wf");
    assert_eq!(coverage.sampled, 2);
    assert_eq!(
        coverage.sampled_execution_ids.len(),
        2,
        "the manifest must name every exported execution: {body}"
    );

    // The identities must be exactly the documents the export actually wrote —
    // a list that merely has the right length would satisfy the count check it
    // is meant to strengthen.
    let exported: std::collections::BTreeSet<String> = body["exports"]
        .as_array()
        .expect("exports array")
        .iter()
        .map(|document| document["execution_id"].as_str().unwrap().to_string())
        .collect();
    let declared: std::collections::BTreeSet<String> = coverage
        .sampled_execution_ids
        .iter()
        .map(std::string::ToString::to_string)
        .collect();
    assert_eq!(
        declared, exported,
        "declared identities must be the exported ones: {body}"
    );
}
