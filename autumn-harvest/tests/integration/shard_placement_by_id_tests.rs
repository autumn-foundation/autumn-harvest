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
//! are the same table". The pure merge/expected-shard rules are unit-tested in
//! `external_target_location`'s own `#[cfg(test)]` module and deliberately not
//! duplicated here.

use autumn_harvest::error::HarvestError;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::external_target_location::{TargetLocation, resolve_location_by_workflow_id};
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::models::{NewWorkflowExecution, WorkflowExecution};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::shard::{ShardRouter, ShardedDbPool};
use autumn_harvest::store;
use autumn_harvest::types::{
    ExecutionId, ExternalCancelId, ExternalSignalId, ExternalTarget, ShardId,
};
use autumn_harvest::worker::{DbPool, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    HarvestBuilder, StartWorkflowParams, WorkerConfig, WorkflowContext,
    start_or_load_workflow_execution,
};

use diesel::prelude::*;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
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
    build_pool_with_max_size(database_url, 8)
}

fn build_pool_with_max_size(database_url: &str, max_size: usize) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(max_size)
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

/// Restores the process-global router/pool to the single-shard default on drop.
///
/// `install_global_router` and `ShardedDbPool::from_map` both write process
/// globals shared by every test file in the one `integration` binary, and this
/// file installs a two-shard topology backed by databases it then drops. A
/// sibling file that assumes the single-shard default — `cross_type_continue_as_new_tests`
/// is one — would otherwise inherit a two-shard router pointing at dead pools.
/// `workflow_id_targeted_tests::build_e2e_worker` defends itself by
/// re-installing on entry; this restores on exit so neither side has to.
struct GlobalTopologyGuard {
    restore_pool: DbPool,
}

impl GlobalTopologyGuard {
    /// `restore_pool` becomes the single shard's pool once the guard drops; it
    /// must outlive the databases this test created, so pass a pool the caller
    /// keeps alive (any live pool will do — nothing reads it after the test).
    const fn new(restore_pool: DbPool) -> Self {
        Self { restore_pool }
    }
}

impl Drop for GlobalTopologyGuard {
    fn drop(&mut self) {
        autumn_harvest::shard::install_global_router(ShardRouter::single());
        let _ = ShardedDbPool::single(self.restore_pool.clone());
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

/// The `reason_code` of the caller's `ExternalSignalFailed`, if it has one.
async fn failed_signal_reason(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> Option<String> {
    store::load_history(conn, exec_id)
        .await
        .expect("load caller history")
        .events
        .into_iter()
        .find_map(|e| match e {
            WorkflowEvent::ExternalSignalFailed { reason_code, .. } => Some(reason_code),
            _ => None,
        })
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
// 2. Fan-out resolution against two real shard databases
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn fanout_finds_a_target_pinned_off_its_hash_shard() {
    let _guard = TEST_MUTEX.lock().await;
    let shards = TwoShards::start().await;
    let router = two_shard_router();
    autumn_harvest::shard::install_global_router(router.clone());
    let _topology = GlobalTopologyGuard::new(shards.pools[&ShardId::new(0)].clone());
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
        resolve_location_by_workflow_id(&sharded, Some(&router), "pinned_entity", &workflow_id)
            .await;
    match placement {
        TargetLocation::Found { shard, run } => {
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
    let _topology = GlobalTopologyGuard::new(shards.pools[&ShardId::new(0)].clone());
    assert_eq!(
        drained_router.pick_for_new_workflow("drained_entity", &workflow_id),
        ShardId::new(0),
        "test setup: draining shard 1 must move where this key hashes"
    );

    let placement = resolve_location_by_workflow_id(
        &shards.sharded_pool(),
        Some(&drained_router),
        "drained_entity",
        &workflow_id,
    )
    .await;
    match placement {
        TargetLocation::Found { shard, .. } => assert_eq!(
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
    let _topology = GlobalTopologyGuard::new(shards.pools[&ShardId::new(0)].clone());

    let workflow_id = key_hashing_to(&router, "entity_wf", "stale", ShardId::new(0));
    // A completed run of the same key on the hash shard …
    let stale = ExecutionId::new_for_shard(ShardId::new(0));
    let mut conn0 = shards.conn(ShardId::new(0)).await;
    insert_row_in_state(&mut conn0, "entity_wf", &workflow_id, stale, "COMPLETED").await;
    // … and the live one, pinned elsewhere.
    let live = ExecutionId::new_for_shard(ShardId::new(1));
    let mut conn1 = shards.conn(ShardId::new(1)).await;
    insert_running_row(&mut conn1, "entity_wf", &workflow_id, live).await;

    let placement = resolve_location_by_workflow_id(
        &shards.sharded_pool(),
        Some(&router),
        "entity_wf",
        &workflow_id,
    )
    .await;
    match placement {
        TargetLocation::Found { shard, run } => {
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
    let _topology = GlobalTopologyGuard::new(shards.pools[&ShardId::new(0)].clone());

    let placement = resolve_location_by_workflow_id(
        &shards.sharded_pool_without(ShardId::new(1)),
        Some(&router),
        "absent_wf",
        "nowhere-1",
    )
    .await;
    match placement {
        TargetLocation::Indeterminate { uninspected } => {
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
    let _topology = GlobalTopologyGuard::new(shards.pools[&ShardId::new(0)].clone());

    assert_eq!(
        resolve_location_by_workflow_id(
            &shards.sharded_pool(),
            Some(&router),
            "absent_wf",
            "nowhere-1"
        )
        .await,
        TargetLocation::NotFound
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
    let _topology = GlobalTopologyGuard::new(shards.pools[&ShardId::new(0)].clone());
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
    let _topology = GlobalTopologyGuard::new(shards.pools[&ShardId::new(0)].clone());
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
    let _topology = GlobalTopologyGuard::new(shards.pools[&ShardId::new(0)].clone());
    // The router knows shard 1; this process has no pool for it. The target
    // could be there, so the sweep must leave the request pending.
    let degraded = shards.sharded_pool_without(ShardId::new(1));

    // Pin the key to the caller's own (poolful) shard. Without this the test
    // could pass against the UNFIXED engine: a key hashing to shard 1 would hit
    // the pre-#1146 "target shard has no pool -> leave pending" branch and
    // record no failure either, for entirely the wrong reason.
    let workflow_id = key_hashing_to(
        &router,
        "unreachable_target_wf",
        "degraded",
        ShardId::new(0),
    );
    let caller = ExecutionId::new_for_shard(ShardId::new(0));
    let mut caller_conn = shards.conn(ShardId::new(0)).await;
    seed_signal_caller(
        &mut caller_conn,
        caller,
        "degraded-caller",
        "unreachable_target_wf",
        &workflow_id,
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
    let _topology = GlobalTopologyGuard::new(shards.pools[&ShardId::new(0)].clone());

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

// ─────────────────────────────────────────────────────────────────────────
// 4. The #751 outcome matrix, reached over the placement-aware route
// ─────────────────────────────────────────────────────────────────────────
//
// Issue #751 gives signal and cancel deliberately OPPOSITE semantics for a
// target whose current run is already terminal: a signal's goal can never be
// met by a dead target (genuine `not_running` failure), while a cancel's goal
// — "nothing running under this key" — already is (no-op success). Both now
// arrive through `DeliveryRoute::CrossShard` and a remote connection, which is
// new code, so both are pinned here rather than assumed from the #751 suite.

/// Seed a terminal-only run of `(name, id)` on shard 1 and a caller on shard 0.
async fn seed_terminal_target_on_shard_one(
    shards: &TwoShards,
    router: &ShardRouter,
    workflow_name: &str,
    prefix: &str,
) -> (String, ExecutionId, ExecutionId) {
    let workflow_id = key_hashing_to(router, workflow_name, prefix, ShardId::new(0));
    let target = ExecutionId::new_for_shard(ShardId::new(1));
    let mut conn1 = shards.conn(ShardId::new(1)).await;
    insert_row_in_state(&mut conn1, workflow_name, &workflow_id, target, "COMPLETED").await;
    (
        workflow_id,
        target,
        ExecutionId::new_for_shard(ShardId::new(0)),
    )
}

#[tokio::test]
async fn outbox_signal_by_id_fails_not_running_against_a_terminal_run_on_another_shard() {
    let _guard = TEST_MUTEX.lock().await;
    let shards = TwoShards::start().await;
    let router = two_shard_router();
    autumn_harvest::shard::install_global_router(router.clone());
    let _topology = GlobalTopologyGuard::new(shards.pools[&ShardId::new(0)].clone());

    let (workflow_id, _target, caller) =
        seed_terminal_target_on_shard_one(&shards, &router, "terminal_sig_wf", "term-sig").await;

    let mut caller_conn = shards.conn(ShardId::new(0)).await;
    seed_signal_caller(
        &mut caller_conn,
        caller,
        "term-sig-caller",
        "terminal_sig_wf",
        &workflow_id,
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

    let reason = failed_signal_reason(&mut caller_conn, caller).await;
    assert_eq!(
        reason.as_deref(),
        Some("not_running"),
        "a terminal current run found on ANOTHER shard is still a genuine \
         `not_running` failure (issue #751 AC4) — never `target_unknown`, which \
         would say we could not find it"
    );
}

#[tokio::test]
async fn outbox_cancel_by_id_is_a_no_op_success_against_a_terminal_run_on_another_shard() {
    let _guard = TEST_MUTEX.lock().await;
    let shards = TwoShards::start().await;
    let router = two_shard_router();
    autumn_harvest::shard::install_global_router(router.clone());
    let _topology = GlobalTopologyGuard::new(shards.pools[&ShardId::new(0)].clone());

    let (workflow_id, target, caller) =
        seed_terminal_target_on_shard_one(&shards, &router, "terminal_can_wf", "term-can").await;

    let mut caller_conn = shards.conn(ShardId::new(0)).await;
    insert_running_row(&mut caller_conn, "by_id_caller", "term-can-caller", caller).await;
    let cancel_id = ExternalCancelId::new();
    store::append_events(
        &mut caller_conn,
        caller,
        &[
            WorkflowEvent::workflow_started(serde_json::json!({}), chrono::Utc::now()),
            WorkflowEvent::ExternalCancelRequested {
                cancel_id,
                target: ExternalTarget::WorkflowId {
                    workflow_name: "terminal_can_wf".to_string(),
                    workflow_id: workflow_id.clone(),
                },
            },
        ],
        1,
    )
    .await
    .expect("seed caller history");

    let metrics = autumn_harvest::telemetry::NoOpMetrics;
    autumn_harvest::timeout::enforce_external_cancels_outbox(
        &mut caller_conn,
        &metrics,
        Duration::from_millis(0),
        &Some(shards.sharded_pool()),
        &[ShardId::new(0)],
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("cancel outbox sweep should succeed");

    let types = history_event_types(&mut caller_conn, caller).await;
    assert!(
        types.contains(&"ExternalCancelDelivered".to_string()),
        "cancel of an already-terminal run is a no-op SUCCESS (issue #751 AC5), \
         the mirror image of the signal case above; got {types:?}"
    );
    let mut conn1 = shards.conn(ShardId::new(1)).await;
    assert_eq!(
        load_execution(&mut conn1, target).await.state,
        "COMPLETED",
        "a no-op success must not mutate the already-terminal target"
    );
}

#[tokio::test]
async fn outbox_cancel_by_id_never_reports_target_unknown_while_a_shard_cannot_be_inspected() {
    let _guard = TEST_MUTEX.lock().await;
    let shards = TwoShards::start().await;
    let router = two_shard_router();
    autumn_harvest::shard::install_global_router(router.clone());
    let _topology = GlobalTopologyGuard::new(shards.pools[&ShardId::new(0)].clone());

    // Exercises the cancel outbox's own `DeliveryRoute::Retry` arm, which
    // returns a six-element step tuple no other test reaches.
    let workflow_id = key_hashing_to(&router, "degraded_cancel_wf", "deg-can", ShardId::new(0));
    let caller = ExecutionId::new_for_shard(ShardId::new(0));
    let mut caller_conn = shards.conn(ShardId::new(0)).await;
    insert_running_row(&mut caller_conn, "by_id_caller", "deg-can-caller", caller).await;
    let cancel_id = ExternalCancelId::new();
    store::append_events(
        &mut caller_conn,
        caller,
        &[
            WorkflowEvent::workflow_started(serde_json::json!({}), chrono::Utc::now()),
            WorkflowEvent::ExternalCancelRequested {
                cancel_id,
                target: ExternalTarget::WorkflowId {
                    workflow_name: "degraded_cancel_wf".to_string(),
                    workflow_id: workflow_id.clone(),
                },
            },
        ],
        1,
    )
    .await
    .expect("seed caller history");

    let metrics = autumn_harvest::telemetry::NoOpMetrics;
    autumn_harvest::timeout::enforce_external_cancels_outbox(
        &mut caller_conn,
        &metrics,
        Duration::from_millis(0),
        &Some(shards.sharded_pool_without(ShardId::new(1))),
        &[ShardId::new(0)],
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("cancel outbox sweep should succeed");

    let types = history_event_types(&mut caller_conn, caller).await;
    assert!(
        !types.contains(&"ExternalCancelFailed".to_string())
            && !types.contains(&"ExternalCancelDelivered".to_string()),
        "an un-inspectable shard leaves the cancel pending — neither a durable \
         `target_unknown` nor a no-op success claimed over a partial view; got {types:?}"
    );
}

// ─────────────────────────────────────────────────────────────────────────
// 5. Liveness, the no-short-circuit rule, and the connection budget
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn outbox_delivers_by_id_on_a_later_sweep_once_the_missing_shard_returns() {
    let _guard = TEST_MUTEX.lock().await;
    let shards = TwoShards::start().await;
    let router = two_shard_router();
    autumn_harvest::shard::install_global_router(router.clone());
    let _topology = GlobalTopologyGuard::new(shards.pools[&ShardId::new(0)].clone());

    let workflow_id = key_hashing_to(&router, "returning_shard_wf", "ret", ShardId::new(0));
    let target = ExecutionId::new_for_shard(ShardId::new(1));
    let mut conn1 = shards.conn(ShardId::new(1)).await;
    insert_running_row(&mut conn1, "returning_shard_wf", &workflow_id, target).await;

    let caller = ExecutionId::new_for_shard(ShardId::new(0));
    let mut caller_conn = shards.conn(ShardId::new(0)).await;
    seed_signal_caller(
        &mut caller_conn,
        caller,
        "ret-caller",
        "returning_shard_wf",
        &workflow_id,
    )
    .await;

    let metrics = autumn_harvest::telemetry::NoOpMetrics;
    let codecs = autumn_harvest::payload_codec::PayloadCodecs::default();

    // Sweep 1: shard 1 has no pool. Nothing is resolved, nothing is recorded.
    let processed = autumn_harvest::timeout::enforce_external_signals_outbox(
        &mut caller_conn,
        &metrics,
        Duration::from_millis(0),
        &Some(shards.sharded_pool_without(ShardId::new(1))),
        &[ShardId::new(0)],
        &codecs,
    )
    .await
    .expect("degraded sweep should succeed");
    assert_eq!(processed, 0, "an indeterminate resolution resolves nothing");
    assert!(
        history_event_types(&mut caller_conn, caller)
            .await
            .iter()
            .all(|t| t != "ExternalSignalFailed" && t != "ExternalSignalDelivered"),
        "the request must still be pending after the degraded sweep"
    );

    // Sweep 2: the shard is back. The SAME pending row is delivered — proving
    // the first sweep left it claimable rather than permanently stuck.
    let processed = autumn_harvest::timeout::enforce_external_signals_outbox(
        &mut caller_conn,
        &metrics,
        Duration::from_millis(0),
        &Some(shards.sharded_pool()),
        &[ShardId::new(0)],
        &codecs,
    )
    .await
    .expect("recovered sweep should succeed");
    assert_eq!(
        processed, 1,
        "the recovered sweep must resolve the pending row"
    );
    assert!(
        history_event_types(&mut caller_conn, caller)
            .await
            .contains(&"ExternalSignalDelivered".to_string()),
        "the by-id signal must be delivered once its shard is inspectable again"
    );
}

#[tokio::test]
async fn outbox_signal_by_id_prefers_the_live_run_over_a_stale_terminal_on_the_callers_shard() {
    let _guard = TEST_MUTEX.lock().await;
    let shards = TwoShards::start().await;
    let router = two_shard_router();
    autumn_harvest::shard::install_global_router(router.clone());
    let _topology = GlobalTopologyGuard::new(shards.pools[&ShardId::new(0)].clone());

    // The end-to-end form of the "no first-hit short circuit" rule: a fan-out
    // that stopped at the caller's own shard would find the dead run and fail
    // the signal `not_running` while the live target waits on shard 1.
    let workflow_id = key_hashing_to(&router, "two_run_wf", "tworun", ShardId::new(0));
    let stale = ExecutionId::new_for_shard(ShardId::new(0));
    let mut conn0 = shards.conn(ShardId::new(0)).await;
    insert_row_in_state(&mut conn0, "two_run_wf", &workflow_id, stale, "COMPLETED").await;
    let live = ExecutionId::new_for_shard(ShardId::new(1));
    let mut conn1 = shards.conn(ShardId::new(1)).await;
    insert_running_row(&mut conn1, "two_run_wf", &workflow_id, live).await;

    let caller = ExecutionId::new_for_shard(ShardId::new(0));
    let mut caller_conn = shards.conn(ShardId::new(0)).await;
    seed_signal_caller(
        &mut caller_conn,
        caller,
        "tworun-caller",
        "two_run_wf",
        &workflow_id,
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
        types.contains(&"ExternalSignalDelivered".to_string()),
        "the live run must win over the stale terminal on the caller's own \
         shard; got {types:?}"
    );
    assert_eq!(
        autumn_harvest::signal::load_pending_signals(&mut conn1, live)
            .await
            .expect("load pending on shard 1")
            .len(),
        1,
        "the signal must be queued against the LIVE run"
    );
    assert!(
        autumn_harvest::signal::load_pending_signals(&mut conn0, stale)
            .await
            .expect("load pending on shard 0")
            .is_empty(),
        "and never against the dead one"
    );
}

#[tokio::test]
async fn outbox_by_id_delivery_completes_when_each_shard_pool_holds_one_connection() {
    let _guard = TEST_MUTEX.lock().await;
    let shards = TwoShards::start().await;
    let router = two_shard_router();
    autumn_harvest::shard::install_global_router(router.clone());
    let _topology = GlobalTopologyGuard::new(shards.pools[&ShardId::new(0)].clone());

    // The production shape the other tests cannot model: the sweep's own
    // connection is checked out of the caller shard's pool, which has exactly
    // one connection. A fan-out that re-acquired from that pool would park
    // forever (`pool.get()` is unbounded — no deadpool `Timeouts` are
    // configured), taking the whole timeout checker with it.
    let one_conn_pools: BTreeMap<ShardId, DbPool> = shards
        .urls
        .iter()
        .map(|(shard, url)| (*shard, build_pool_with_max_size(url, 1)))
        .collect();
    let sharded = ShardedDbPool::from_map(one_conn_pools.clone(), ShardId::new(0));

    let workflow_id = key_hashing_to(&router, "one_conn_wf", "onecon", ShardId::new(0));
    let target = ExecutionId::new_for_shard(ShardId::new(1));
    let mut conn1 = shards.conn(ShardId::new(1)).await;
    insert_running_row(&mut conn1, "one_conn_wf", &workflow_id, target).await;

    let caller = ExecutionId::new_for_shard(ShardId::new(0));
    {
        let mut seed = shards.conn(ShardId::new(0)).await;
        seed_signal_caller(
            &mut seed,
            caller,
            "onecon-caller",
            "one_conn_wf",
            &workflow_id,
        )
        .await;
    }

    // The sweep runs on the pool's ONLY connection, exactly as
    // `spawn_timeout_checker_for_shard` does.
    let mut pooled = one_conn_pools[&ShardId::new(0)]
        .get()
        .await
        .expect("the single pooled connection");

    let metrics = autumn_harvest::telemetry::NoOpMetrics;
    let processed = tokio::time::timeout(
        Duration::from_secs(30),
        autumn_harvest::timeout::enforce_external_signals_outbox(
            &mut pooled,
            &metrics,
            Duration::from_millis(0),
            &Some(sharded),
            &[ShardId::new(0)],
            &autumn_harvest::payload_codec::PayloadCodecs::default(),
        ),
    )
    .await
    .expect("the sweep must not park on its own single-connection pool")
    .expect("signal outbox sweep should succeed");

    assert_eq!(
        processed, 1,
        "the cross-shard by-id signal must be delivered"
    );
    assert_eq!(
        autumn_harvest::signal::load_pending_signals(&mut conn1, target)
            .await
            .expect("load pending signals")
            .len(),
        1
    );
}

// ─────────────────────────────────────────────────────────────────────────
// 6. The inline gate, through a real `Worker`
// ─────────────────────────────────────────────────────────────────────────
//
// Every test above drives the outbox scanners directly. This one drives the
// production path — `worker::persist_external_signal_inline`, gated by
// `external_target_location::inline_delivery_allowed` — because that gate is
// the only part of #1146 that runs inside a workflow's OWN decision
// transaction, where a wrong answer is written to the append-only history
// immediately and irreversibly.

fn e2e_wf_info(
    name: &'static str,
    handler: autumn_harvest::info::WorkflowHandlerFn,
) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "shard_placement_by_id_tests",
        handler,
        execution_timeout: None,
        chain_execution_timeout: None,
        sla: None,
        concurrency: None,
        debounce: None,
        batch: None,
        throttle: None,
        max_input_bytes: None,
        owner: None,
        runbook_url: None,
        severity: None,
        description: None,
        input_schema: None,
        output_schema: None,
        error_schema: None,
        retry_policy: None,
    }
}

fn e2e_start_params(
    exec_id: ExecutionId,
    workflow_name: &'static str,
    workflow_id: &'static str,
    input: serde_json::Value,
) -> StartWorkflowParams<'static> {
    StartWorkflowParams {
        exec_id,
        workflow_name,
        workflow_id,
        input,
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        sla: None,
        memo: None,
        search_attrs: None,
        reuse_policy: autumn_harvest::types::WorkflowIdReusePolicy::default(),
        conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        inherited_chain_deadline_at: None,
        concurrency_key: None,
        concurrency_limit: None,
        concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
        priority: autumn_harvest::types::Priority::default(),
        max_workflow_input_bytes: 0,
        start_at: None,
        delay: None,
        max_workflow_start_delay: None,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        max_workflow_attempts_ceiling: None,
        origin: None,
        completion_callbacks: None,
        start_source: autumn_harvest::StartSource::Api,
        start_source_ref: None,
        started_by: None,
    }
}

/// Workflow type used by the inline-gate end-to-end test below.
const TARGET_WF: &str = "gate_target_wf";

fn gate_target_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let payload: serde_json::Value = ctx
            .receive_signal("ping")
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"status": "signalled", "payload": payload}))
    })
}

fn gate_signaller_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let workflow_name = input["workflow_name"]
            .as_str()
            .ok_or("missing workflow_name")?
            .to_string();
        let workflow_id = input["workflow_id"]
            .as_str()
            .ok_or("missing workflow_id")?
            .to_string();
        match ctx
            .signal_external_workflow_by_id(
                &workflow_name,
                &workflow_id,
                "ping",
                serde_json::json!({"hello": "world"}),
            )
            .await
        {
            Ok(()) => Ok(serde_json::json!({"result": "delivered"})),
            Err(HarvestError::ExternalSignalFailed { reason_code, .. }) => {
                Ok(serde_json::json!({"result": "failed", "reason_code": reason_code}))
            }
            Err(other) => Err(other.to_string()),
        }
    })
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn worker_by_id_signal_is_not_delivered_inline_against_a_stale_terminal_on_the_callers_shard()
{
    let _guard = TEST_MUTEX.lock().await;
    let shards = TwoShards::start().await;
    let router = two_shard_router();
    autumn_harvest::shard::install_global_router(router.clone());
    let _topology = GlobalTopologyGuard::new(shards.pools[&ShardId::new(0)].clone());
    let sharded = shards.sharded_pool();

    // The business key hashes to the CALLER's shard, and a dead run of it sits
    // there — the exact state in which the pre-#1146 gate allowed inline
    // delivery and recorded a permanent `not_running` against a live target.
    // Chosen — not assumed — to hash to the CALLER's shard. If it hashed
    // elsewhere the pre-#1146 gate would have deferred to the outbox for an
    // unrelated reason and this test would pass against the unfixed engine.
    // `StartWorkflowParams` takes `&'static str`, hence the leak; the test
    // process is about to exit.
    let target_id: &'static str = Box::leak(
        key_hashing_to(&router, TARGET_WF, "gate-target", ShardId::new(0)).into_boxed_str(),
    );

    let stale = ExecutionId::new_for_shard(ShardId::new(0));
    let mut conn0 = shards.conn(ShardId::new(0)).await;
    insert_row_in_state(&mut conn0, TARGET_WF, target_id, stale, "COMPLETED").await;

    let live = ExecutionId::new_for_shard(ShardId::new(1));
    let caller = ExecutionId::new_for_shard(ShardId::new(0));

    let built = HarvestBuilder::new()
        .workflows(vec![
            e2e_wf_info(TARGET_WF, gate_target_workflow),
            e2e_wf_info("gate_signaller_wf", gate_signaller_workflow),
        ])
        .worker(WorkerConfig::default())
        .build();
    let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = "worker-1146-gate".to_string();
    runtime_config.poll_interval = Duration::from_millis(50);
    runtime_config.shutdown_timeout = Duration::from_secs(2);
    runtime_config.shard_assignments = vec![ShardId::new(0), ShardId::new(1)];
    runtime_config.sharded_pool = Some(sharded.clone());
    let worker =
        Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker should build"));
    let runner = Arc::clone(&worker);
    let pool_for_run = shards.pools[&ShardId::new(0)].clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    // The LIVE run of the same key, on the other shard.
    let mut conn1 = shards.conn(ShardId::new(1)).await;
    start_or_load_workflow_execution(
        &mut conn1,
        e2e_start_params(live, TARGET_WF, target_id, serde_json::json!({})),
        None,
    )
    .await
    .expect("start the live target on shard 1");
    tokio::time::sleep(Duration::from_millis(300)).await;

    start_or_load_workflow_execution(
        &mut conn0,
        e2e_start_params(
            caller,
            "gate_signaller_wf",
            "gate-caller-1",
            serde_json::json!({"workflow_name": TARGET_WF, "workflow_id": target_id}),
        ),
        None,
    )
    .await
    .expect("start the caller on shard 0");

    let caller_final = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let row = load_execution(&mut conn0, caller).await;
            if row.state != "RUNNING" && row.state != "PENDING" {
                break row;
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
    })
    .await
    .expect("the caller must reach a terminal state");

    assert_eq!(
        caller_final.output,
        Some(serde_json::json!({"result": "delivered"})),
        "the signal must reach the LIVE run on shard 1. A `not_running` here is \
         the inline gate resolving the business key against the caller's own \
         shard and finding only the dead run — the bug #1146 closes"
    );
    assert_eq!(
        autumn_harvest::signal::load_pending_signals(&mut conn1, live)
            .await
            .expect("load pending on the live run")
            .len()
            + usize::from(load_execution(&mut conn1, live).await.state == "COMPLETED"),
        1,
        "the signal was delivered to the live run (still queued, or already \
         consumed and the run completed)"
    );

    worker.shutdown();
    let _ = handle.await;
}
