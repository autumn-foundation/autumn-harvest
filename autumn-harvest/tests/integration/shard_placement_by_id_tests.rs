#![cfg(feature = "db")]
//! Issue #1146: shard-placement-aware resolution for `workflow_id`-addressed
//! external signal/cancel.
//!
//! Issue #751 resolves a `(workflow_name, workflow_id)` target's owning shard
//! by *re-deriving* `ShardRouter::pick_for_new_workflow` — a prediction of
//! where a **new** workflow would be placed, not an observation of where an
//! **existing** one lives. The two diverge in exactly two ways:
//!
//! 1. **Explicit shard placement (issue #697).** A workflow started under
//!    `ShardPlacement::Shard`/`ResidencyKey` is pinned to a shard the pure
//!    hash may never compute.
//! 2. **Writable-set drift.** `pick_writable` re-hashes over the *current*
//!    `writable_shards` when the readable-set hash lands outside it, so
//!    draining a shard can move where the same business key resolves *after*
//!    a workflow was already placed there.
//!
//! In both cases a `workflow_id`-addressed signal/cancel resolved to the wrong
//! shard, found nothing, and reported `target_unknown` while the target was
//! running. These tests fix the resolution rule: fan out across every expected
//! shard, merge with the canonical `execution::select_resolved_run` ranking,
//! and treat a shard that could not be inspected as *indeterminate* rather
//! than as "not there".
//!
//! Every test in this file uses **two genuinely separate Postgres databases**,
//! one per logical shard — a single physical database mocked as two shards
//! cannot distinguish "found by fanning out" from "found because both shards
//! are the same table".

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::execution::ResolvedRun;
use autumn_harvest::external_target_placement::{
    TargetPlacement, UninspectedShard, fanout_shards, merge_placement,
    resolve_placement_by_workflow_id,
};
use autumn_harvest::models::{NewWorkflowExecution, WorkflowExecution};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::shard::{ShardRouter, ShardedDbPool};
use autumn_harvest::store;
use autumn_harvest::types::{
    ExecutionId, ExternalCancelId, ExternalSignalId, ExternalTarget, ShardId,
};
use autumn_harvest::worker::DbPool;

use diesel::prelude::*;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use std::collections::BTreeMap;
use std::time::Duration;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

/// `GLOBAL_SHARD_ROUTER` / `GLOBAL_SHARDED_POOL` are process-global statics
/// shared by every test file compiled into the one `integration` binary, and
/// every test here installs its own. Serialize the whole file against itself
/// exactly as `workflow_id_targeted_tests.rs` does.
static TEST_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn init_sql() -> Vec<u8> {
    autumn_harvest::test_init_sql().as_bytes().to_vec()
}

fn rewrite_pg_db(base: &str, db: &str) -> String {
    let after_scheme = base.find("://").map_or(0, |i| i + 3);
    let rest = &base[after_scheme..];
    let (authority, tail) = rest
        .find('/')
        .map_or((rest, ""), |i| (&rest[..i], &rest[i + 1..]));
    let query = tail.find('?').map_or("", |i| &tail[i..]);
    format!("{}{}/{}{}", &base[..after_scheme], authority, db, query)
}

/// One physically distinct database, migrated and ready.
async fn setup_one_database() -> (String, Option<ContainerAsync<Postgres>>) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    if let Ok(base_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let db_name = format!("harvest1146_{}", uuid::Uuid::new_v4().simple());
        let mut admin = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&base_url)
            .await
            .expect("failed to connect to HARVEST_TEST_DATABASE_URL base");
        admin
            .batch_execute(&format!("CREATE DATABASE \"{db_name}\""))
            .await
            .expect("failed to create per-test database");
        let new_url = rewrite_pg_db(&base_url, &db_name);
        let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&new_url)
            .await
            .expect("failed to connect to per-test database");
        conn.batch_execute(&autumn_harvest::test_init_sql())
            .await
            .expect("failed to apply migrations to per-test database");
        return (new_url, None);
    }

    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container
        .get_host()
        .await
        .expect("failed to get container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get container port");
    (
        format!("postgres://postgres:postgres@{host}:{port}/postgres"),
        Some(container),
    )
}

fn build_test_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("failed to build test pool")
}

/// Two logical shards backed by two **physically separate** databases.
struct TwoShards {
    urls: BTreeMap<ShardId, String>,
    pools: BTreeMap<ShardId, DbPool>,
    _containers: Vec<ContainerAsync<Postgres>>,
}

impl TwoShards {
    async fn start() -> Self {
        let mut urls = BTreeMap::new();
        let mut pools = BTreeMap::new();
        let mut containers = Vec::new();
        for shard in [ShardId::new(0), ShardId::new(1)] {
            let (url, container) = setup_one_database().await;
            pools.insert(shard, build_test_pool(&url));
            urls.insert(shard, url);
            if let Some(c) = container {
                containers.push(c);
            }
        }
        Self {
            urls,
            pools,
            _containers: containers,
        }
    }

    fn sharded_pool(&self) -> ShardedDbPool {
        ShardedDbPool::from_map(self.pools.clone(), ShardId::new(0))
    }

    /// A sharded pool that is deliberately missing `shard` — the "this process
    /// has no pool for a shard the router knows about" case.
    fn sharded_pool_without(&self, shard: ShardId) -> ShardedDbPool {
        let mut pools = self.pools.clone();
        pools.remove(&shard);
        ShardedDbPool::from_map(pools, ShardId::new(0))
    }

    async fn conn(&self, shard: ShardId) -> AsyncPgConnection {
        <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&self.urls[&shard])
            .await
            .expect("connect to shard database")
    }
}

fn two_shard_router() -> ShardRouter {
    ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    )
}

/// Insert a bare execution row in `state` directly. Mirrors
/// `workflow_id_targeted_tests::insert_running_row`, parameterised on state so
/// a stale terminal run can be planted alongside a live one.
async fn insert_row_in_state(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    exec_id: ExecutionId,
    state: &str,
) {
    let row = NewWorkflowExecution {
        quota_key: None,
        continued_from_exec_id: None,
        first_exec_id: None,
        id: exec_id.as_uuid(),
        workflow_name,
        workflow_id,
        run_id: uuid::Uuid::new_v4(),
        shard_id: exec_id.shard().as_i32(),
        input: serde_json::json!({}),
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
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("insert execution row");
    if state != "RUNNING" {
        diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
            .set(harvest_workflow_executions::state.eq(state))
            .execute(conn)
            .await
            .expect("set execution state");
    }
}

async fn insert_running_row(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    exec_id: ExecutionId,
) {
    insert_row_in_state(conn, workflow_name, workflow_id, exec_id, "RUNNING").await;
}

async fn load_execution(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> WorkflowExecution {
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .expect("load execution")
}

async fn history_event_types(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> Vec<String> {
    store::load_history(conn, exec_id)
        .await
        .expect("load caller history")
        .events
        .iter()
        .map(|e| e.type_name().to_string())
        .collect()
}

/// A business key whose rendezvous hash under `router` lands on `away_from`,
/// so pinning its execution anywhere else is a genuine hash-vs-placement
/// divergence rather than a coincidence.
fn key_hashing_to(router: &ShardRouter, workflow_name: &str, prefix: &str, to: ShardId) -> String {
    (0..10_000)
        .map(|n| format!("{prefix}-{n}"))
        .find(|id| router.pick_for_new_workflow(workflow_name, id) == to)
        .expect("some id in 10k must hash to the requested shard")
}

// ─────────────────────────────────────────────────────────────────────────
// 1. Pure merge / fan-out-set rules (no DB)
// ─────────────────────────────────────────────────────────────────────────

fn run(exec_id: ExecutionId, state: &str, secs: i64) -> ResolvedRun {
    ResolvedRun {
        exec_id,
        state: state.to_string(),
        started_at: chrono::DateTime::from_timestamp(1_800_000_000 + secs, 0)
            .expect("valid timestamp"),
    }
}

fn uninspected(shard: i32) -> UninspectedShard {
    UninspectedShard {
        shard: ShardId::new(shard),
        reason: "no configured storage pool".to_string(),
    }
}

#[test]
fn merge_prefers_a_live_run_over_a_stale_terminal_on_another_shard() {
    let live = run(ExecutionId::new_for_shard(ShardId::new(1)), "RUNNING", 10);
    let stale = run(ExecutionId::new_for_shard(ShardId::new(0)), "COMPLETED", 99);
    let merged = merge_placement(
        vec![(ShardId::new(0), stale), (ShardId::new(1), live.clone())],
        Vec::new(),
    );
    assert_eq!(
        merged,
        TargetPlacement::Found {
            shard: ShardId::new(1),
            run: live
        },
        "an active run must win over a more-recently-started terminal one, \
         whichever shard each is on — this is what a first-hit-wins fan-out \
         would get wrong"
    );
}

#[test]
fn merge_reports_not_found_only_when_every_shard_was_inspected() {
    assert_eq!(
        merge_placement(Vec::new(), Vec::new()),
        TargetPlacement::NotFound
    );
}

#[test]
fn merge_is_indeterminate_when_no_candidate_and_a_shard_was_missed() {
    let merged = merge_placement(Vec::new(), vec![uninspected(1)]);
    assert_eq!(
        merged,
        TargetPlacement::Indeterminate {
            uninspected: vec![uninspected(1)]
        },
        "an unreachable shard must never be reported as `NotFound`: that turns \
         a transient outage into a permanent `target_unknown` in the caller's \
         durable history"
    );
}

#[test]
fn merge_is_indeterminate_when_only_a_terminal_was_found_and_a_shard_was_missed() {
    let terminal = run(ExecutionId::new_for_shard(ShardId::new(0)), "COMPLETED", 5);
    let merged = merge_placement(vec![(ShardId::new(0), terminal)], vec![uninspected(1)]);
    assert_eq!(
        merged,
        TargetPlacement::Indeterminate {
            uninspected: vec![uninspected(1)]
        },
        "a terminal run on an inspected shard does not rule out a LIVE run on \
         the shard we could not inspect — concluding `not_running` there would \
         fail a signal whose target is alive"
    );
}

#[test]
fn merge_accepts_a_live_run_even_when_another_shard_was_missed() {
    let live = run(ExecutionId::new_for_shard(ShardId::new(0)), "RUNNING", 5);
    let merged = merge_placement(vec![(ShardId::new(0), live.clone())], vec![uninspected(1)]);
    assert_eq!(
        merged,
        TargetPlacement::Found {
            shard: ShardId::new(0),
            run: live
        },
        "at most one run per business key is active, so a live run found is \
         authoritative — refusing to deliver to it during an unrelated shard's \
         outage would be strictly worse"
    );
}

#[test]
fn fanout_set_unions_local_pools_with_every_shard_the_router_knows() {
    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)],
        vec![ShardId::new(0)],
        ShardId::new(0),
    );
    let shards = fanout_shards(&[ShardId::new(0), ShardId::new(7)], Some(&router));
    assert_eq!(
        shards,
        vec![
            ShardId::new(0),
            ShardId::new(1),
            ShardId::new(2),
            ShardId::new(7)
        ],
        "a shard the router knows about but this process has no pool for yet \
         (mid a shard-add rollout) must still be named, so it is reported \
         uninspected rather than silently skipped"
    );
}

#[test]
fn fanout_set_is_a_single_shard_for_a_single_shard_deployment() {
    assert_eq!(
        fanout_shards(&[ShardId::new(0)], Some(&ShardRouter::single())),
        vec![ShardId::new(0)],
        "a single-shard deployment must fan out to exactly one query — the \
         one it already made"
    );
}

#[test]
fn fanout_set_falls_back_to_the_default_shard_when_nothing_is_configured() {
    assert_eq!(fanout_shards(&[], None), vec![ShardId::new(0)]);
}

// ─────────────────────────────────────────────────────────────────────────
// 2. Fan-out resolution against two real shard databases
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn fanout_finds_a_target_pinned_off_its_hash_shard() {
    let _guard = TEST_MUTEX.lock().await;
    let shards = TwoShards::start().await;
    let router = two_shard_router();
    autumn_harvest::shard::install_global_router(router.clone());
    let sharded = shards.sharded_pool();

    // A key the hash places on shard 0 …
    let workflow_id = key_hashing_to(&router, "pinned_entity", "pin", ShardId::new(0));
    // … whose workflow was started with an explicit `ShardPlacement::Shard(1)`
    // pin, so it actually lives on shard 1.
    let pinned = ExecutionId::new_for_shard(ShardId::new(1));
    let mut conn1 = shards.conn(ShardId::new(1)).await;
    insert_running_row(&mut conn1, "pinned_entity", &workflow_id, pinned).await;

    // The pre-#1146 hash resolution points at the WRONG shard …
    assert_eq!(
        autumn_harvest::shard::external_target_owning_shard(&ExternalTarget::WorkflowId {
            workflow_name: "pinned_entity".to_string(),
            workflow_id: workflow_id.clone(),
        }),
        Some(ShardId::new(0)),
        "test setup: the hash must predict shard 0 for this key"
    );

    // … and the placement-aware resolution finds where it really is.
    let placement =
        resolve_placement_by_workflow_id(&sharded, Some(&router), "pinned_entity", &workflow_id)
            .await;
    match placement {
        TargetPlacement::Found { shard, run } => {
            assert_eq!(shard, ShardId::new(1));
            assert_eq!(run.exec_id, pinned);
        }
        other => panic!("expected the pinned run on shard 1, got {other:?}"),
    }
}

#[tokio::test]
async fn fanout_finds_a_run_left_behind_by_a_writable_set_change() {
    let _guard = TEST_MUTEX.lock().await;
    let shards = TwoShards::start().await;

    // Placed while both shards were writable.
    let placing_router = two_shard_router();
    let workflow_id = key_hashing_to(&placing_router, "drained_entity", "drain", ShardId::new(1));
    let placed = ExecutionId::new_for_shard(ShardId::new(1));
    let mut conn1 = shards.conn(ShardId::new(1)).await;
    insert_running_row(&mut conn1, "drained_entity", &workflow_id, placed).await;

    // Shard 1 is later drained out of the writable set: still readable, no
    // longer taking new work. `pick_for_new_workflow` now re-hashes over the
    // writable subset and answers shard 0 for the SAME key.
    let drained_router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0)],
        ShardId::new(0),
    );
    autumn_harvest::shard::install_global_router(drained_router.clone());
    assert_eq!(
        drained_router.pick_for_new_workflow("drained_entity", &workflow_id),
        ShardId::new(0),
        "test setup: draining shard 1 must move where this key hashes"
    );

    let placement = resolve_placement_by_workflow_id(
        &shards.sharded_pool(),
        Some(&drained_router),
        "drained_entity",
        &workflow_id,
    )
    .await;
    match placement {
        TargetPlacement::Found { shard, .. } => assert_eq!(
            shard,
            ShardId::new(1),
            "the run is still on the drained shard and must still be found there"
        ),
        other => panic!("expected the run on the drained shard 1, got {other:?}"),
    }
}

#[tokio::test]
async fn fanout_prefers_the_live_run_over_a_stale_terminal_on_the_hash_shard() {
    let _guard = TEST_MUTEX.lock().await;
    let shards = TwoShards::start().await;
    let router = two_shard_router();
    autumn_harvest::shard::install_global_router(router.clone());

    let workflow_id = key_hashing_to(&router, "entity_wf", "stale", ShardId::new(0));
    // A completed run of the same key on the hash shard …
    let stale = ExecutionId::new_for_shard(ShardId::new(0));
    let mut conn0 = shards.conn(ShardId::new(0)).await;
    insert_row_in_state(&mut conn0, "entity_wf", &workflow_id, stale, "COMPLETED").await;
    // … and the live one, pinned elsewhere.
    let live = ExecutionId::new_for_shard(ShardId::new(1));
    let mut conn1 = shards.conn(ShardId::new(1)).await;
    insert_running_row(&mut conn1, "entity_wf", &workflow_id, live).await;

    let placement = resolve_placement_by_workflow_id(
        &shards.sharded_pool(),
        Some(&router),
        "entity_wf",
        &workflow_id,
    )
    .await;
    match placement {
        TargetPlacement::Found { shard, run } => {
            assert_eq!(shard, ShardId::new(1));
            assert_eq!(
                run.exec_id, live,
                "the LIVE run must win, not the stale one"
            );
        }
        other => panic!("expected the live run on shard 1, got {other:?}"),
    }
}

#[tokio::test]
async fn fanout_is_indeterminate_when_an_expected_shard_has_no_pool() {
    let _guard = TEST_MUTEX.lock().await;
    let shards = TwoShards::start().await;
    let router = two_shard_router();
    autumn_harvest::shard::install_global_router(router.clone());

    let placement = resolve_placement_by_workflow_id(
        &shards.sharded_pool_without(ShardId::new(1)),
        Some(&router),
        "absent_wf",
        "nowhere-1",
    )
    .await;
    match placement {
        TargetPlacement::Indeterminate { uninspected } => {
            assert_eq!(uninspected.len(), 1);
            assert_eq!(uninspected[0].shard, ShardId::new(1));
        }
        other => panic!("expected Indeterminate for the un-poolable shard, got {other:?}"),
    }
}

#[tokio::test]
async fn fanout_reports_not_found_when_every_shard_answered_and_none_holds_the_key() {
    let _guard = TEST_MUTEX.lock().await;
    let shards = TwoShards::start().await;
    let router = two_shard_router();
    autumn_harvest::shard::install_global_router(router.clone());

    assert_eq!(
        resolve_placement_by_workflow_id(
            &shards.sharded_pool(),
            Some(&router),
            "absent_wf",
            "nowhere-1"
        )
        .await,
        TargetPlacement::NotFound
    );
}

// ─────────────────────────────────────────────────────────────────────────
// 3. End-to-end through the outbox scanners (the issue's actual symptom)
// ─────────────────────────────────────────────────────────────────────────

/// Seed a caller on shard 0 whose history holds a pending
/// `ExternalSignalRequested` addressed to `(workflow_name, workflow_id)`.
async fn seed_signal_caller(
    conn: &mut AsyncPgConnection,
    caller: ExecutionId,
    caller_id: &str,
    target_name: &str,
    target_id: &str,
) -> ExternalSignalId {
    insert_running_row(conn, "by_id_caller", caller_id, caller).await;
    let signal_id = ExternalSignalId::new();
    store::append_events(
        conn,
        caller,
        &[
            WorkflowEvent::workflow_started(serde_json::json!({}), chrono::Utc::now()),
            WorkflowEvent::ExternalSignalRequested {
                signal_id,
                target: ExternalTarget::WorkflowId {
                    workflow_name: target_name.to_string(),
                    workflow_id: target_id.to_string(),
                },
                signal_name: "ping".to_string(),
                payload: serde_json::json!({"hello": "world"}),
                idempotency_key: None,
            },
        ],
        1,
    )
    .await
    .expect("seed caller history");
    signal_id
}

#[tokio::test]
async fn outbox_signal_by_id_reaches_a_target_pinned_off_its_hash_shard() {
    let _guard = TEST_MUTEX.lock().await;
    let shards = TwoShards::start().await;
    let router = two_shard_router();
    autumn_harvest::shard::install_global_router(router.clone());
    let sharded = shards.sharded_pool();

    // Caller on shard 0. Target key hashes to shard 0 too — but the target is
    // pinned on shard 1, so pre-#1146 the outbox looked only at shard 0, found
    // nothing, and failed the signal `target_unknown` once the grace window
    // expired.
    let workflow_id = key_hashing_to(&router, "pinned_target_wf", "e2e-sig", ShardId::new(0));
    let target = ExecutionId::new_for_shard(ShardId::new(1));
    let mut conn1 = shards.conn(ShardId::new(1)).await;
    insert_running_row(&mut conn1, "pinned_target_wf", &workflow_id, target).await;
    store::append_events(
        &mut conn1,
        target,
        &[WorkflowEvent::workflow_started(
            serde_json::json!({}),
            chrono::Utc::now(),
        )],
        1,
    )
    .await
    .expect("seed target history");

    let caller = ExecutionId::new_for_shard(ShardId::new(0));
    let mut caller_conn = shards.conn(ShardId::new(0)).await;
    let signal_id = seed_signal_caller(
        &mut caller_conn,
        caller,
        "e2e-sig-caller",
        "pinned_target_wf",
        &workflow_id,
    )
    .await;

    let metrics = autumn_harvest::telemetry::NoOpMetrics;
    let processed = autumn_harvest::timeout::enforce_external_signals_outbox(
        &mut caller_conn,
        &metrics,
        // Grace window already expired: pre-#1146 this is precisely when the
        // wrong-shard resolution became a durable `target_unknown` failure.
        Duration::from_millis(0),
        &Some(sharded),
        &[ShardId::new(0)],
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("signal outbox sweep should succeed");
    assert_eq!(processed, 1, "the by-id signal must be resolved this sweep");

    let types = history_event_types(&mut caller_conn, caller).await;
    assert!(
        types.contains(&"ExternalSignalDelivered".to_string()),
        "the pinned target is running and must receive the signal; got {types:?}"
    );

    // And the signal is genuinely queued on the TARGET's database.
    let pending = autumn_harvest::signal::load_pending_signals(&mut conn1, target)
        .await
        .expect("load pending signals on the target shard");
    assert_eq!(
        pending.len(),
        1,
        "signal must be queued against the pinned run"
    );
    let _ = signal_id;
}

#[tokio::test]
async fn outbox_cancel_by_id_reaches_a_target_pinned_off_its_hash_shard() {
    let _guard = TEST_MUTEX.lock().await;
    let shards = TwoShards::start().await;
    let router = two_shard_router();
    autumn_harvest::shard::install_global_router(router.clone());
    let sharded = shards.sharded_pool();

    let workflow_id = key_hashing_to(&router, "pinned_cancel_wf", "e2e-can", ShardId::new(0));
    let target = ExecutionId::new_for_shard(ShardId::new(1));
    let mut conn1 = shards.conn(ShardId::new(1)).await;
    insert_running_row(&mut conn1, "pinned_cancel_wf", &workflow_id, target).await;
    store::append_events(
        &mut conn1,
        target,
        &[WorkflowEvent::workflow_started(
            serde_json::json!({}),
            chrono::Utc::now(),
        )],
        1,
    )
    .await
    .expect("seed target history");

    let caller = ExecutionId::new_for_shard(ShardId::new(0));
    let mut caller_conn = shards.conn(ShardId::new(0)).await;
    insert_running_row(&mut caller_conn, "by_id_caller", "e2e-can-caller", caller).await;
    let cancel_id = ExternalCancelId::new();
    store::append_events(
        &mut caller_conn,
        caller,
        &[
            WorkflowEvent::workflow_started(serde_json::json!({}), chrono::Utc::now()),
            WorkflowEvent::ExternalCancelRequested {
                cancel_id,
                target: ExternalTarget::WorkflowId {
                    workflow_name: "pinned_cancel_wf".to_string(),
                    workflow_id: workflow_id.clone(),
                },
            },
        ],
        1,
    )
    .await
    .expect("seed caller history");

    let metrics = autumn_harvest::telemetry::NoOpMetrics;
    let processed = autumn_harvest::timeout::enforce_external_cancels_outbox(
        &mut caller_conn,
        &metrics,
        Duration::from_millis(0),
        &Some(sharded),
        &[ShardId::new(0)],
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("cancel outbox sweep should succeed");
    assert_eq!(processed, 1, "the by-id cancel must be resolved this sweep");

    let types = history_event_types(&mut caller_conn, caller).await;
    assert!(
        types.contains(&"ExternalCancelDelivered".to_string()),
        "the pinned target must be cancelled, not reported unknown; got {types:?}"
    );
    assert_eq!(
        load_execution(&mut conn1, target).await.state,
        "CANCELLED",
        "the cancel must land on the pinned target's own database"
    );
}

#[tokio::test]
async fn outbox_never_reports_target_unknown_while_a_shard_cannot_be_inspected() {
    let _guard = TEST_MUTEX.lock().await;
    let shards = TwoShards::start().await;
    let router = two_shard_router();
    autumn_harvest::shard::install_global_router(router.clone());
    // The router knows shard 1; this process has no pool for it. The target
    // could be there, so the sweep must leave the request pending.
    let degraded = shards.sharded_pool_without(ShardId::new(1));

    let caller = ExecutionId::new_for_shard(ShardId::new(0));
    let mut caller_conn = shards.conn(ShardId::new(0)).await;
    seed_signal_caller(
        &mut caller_conn,
        caller,
        "degraded-caller",
        "unreachable_target_wf",
        "maybe-over-there-1",
    )
    .await;

    let metrics = autumn_harvest::telemetry::NoOpMetrics;
    autumn_harvest::timeout::enforce_external_signals_outbox(
        &mut caller_conn,
        &metrics,
        // Grace window fully expired — the ONLY thing standing between this
        // request and a permanent `target_unknown` is the fan-out refusing to
        // conclude "not found" from an incomplete inspection.
        Duration::from_millis(0),
        &Some(degraded),
        &[ShardId::new(0)],
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("signal outbox sweep should succeed");

    let types = history_event_types(&mut caller_conn, caller).await;
    assert!(
        !types.contains(&"ExternalSignalFailed".to_string()),
        "an un-inspectable shard must never be turned into a durable \
         `target_unknown` failure; got {types:?}"
    );
    assert!(
        !types.contains(&"ExternalSignalDelivered".to_string()),
        "nothing was delivered either — the request stays pending for a retry"
    );
}

#[tokio::test]
async fn outbox_still_reports_target_unknown_when_every_shard_answered() {
    let _guard = TEST_MUTEX.lock().await;
    let shards = TwoShards::start().await;
    let router = two_shard_router();
    autumn_harvest::shard::install_global_router(router.clone());

    let caller = ExecutionId::new_for_shard(ShardId::new(0));
    let mut caller_conn = shards.conn(ShardId::new(0)).await;
    seed_signal_caller(
        &mut caller_conn,
        caller,
        "unknown-caller",
        "genuinely_absent_wf",
        "no-such-entity-1",
    )
    .await;

    let metrics = autumn_harvest::telemetry::NoOpMetrics;
    autumn_harvest::timeout::enforce_external_signals_outbox(
        &mut caller_conn,
        &metrics,
        Duration::from_millis(0),
        &Some(shards.sharded_pool()),
        &[ShardId::new(0)],
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("signal outbox sweep should succeed");

    let types = history_event_types(&mut caller_conn, caller).await;
    assert!(
        types.contains(&"ExternalSignalFailed".to_string()),
        "a complete fan-out that finds nothing must still fail the request \
         after the grace window — #1146 must not make `target_unknown` \
         unreachable; got {types:?}"
    );
}
