#![cfg(feature = "db")]
//! A DAG schedule override follows its DAG to a new shard (issue #2389).
//!
//! An override lives on the shard that owns the DAG, next to its schedule
//! row. A routing change moves the schedule row to a new owner. Before #2389
//! the override stayed behind, so the new owner reconciled to the code value.
//!
//! Issue #2405: two shards on one pool share one schedule table. The shard
//! that does not own a DAG must not delete the row of the shard that does.
//!
//! The carry writes under the DR fence of both shards. A retry after a
//! failed source delete keeps an operator clear on the new owner.
//!
//! Execution: honours `HARVEST_TEST_DATABASE_URL` for the admin connection
//! and creates one fresh database per shard off it. Otherwise it boots a
//! testcontainers Postgres 16 and does the same. Separate databases are
//! load-bearing: a shard is the pool that it reads. The colocated tests
//! share one database on purpose.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::policy::{OverlapPolicy, Schedule, WorkflowSchedule};
use autumn_harvest::schedule_override::{
    ScheduleOverridePatch, clear_dag_override, get_dag_override, update_dag_override,
};
use autumn_harvest::scheduler::{DagCatalog, RegisteredDag, SchedulerMonitor, tick_once_sharded};
use autumn_harvest::shard::{ShardRouter, ShardedDbPool};
use autumn_harvest::types::ShardId;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const OLD: ShardId = ShardId::new(0);
const NEW: ShardId = ShardId::new(1);

struct Shards {
    urls: BTreeMap<ShardId, String>,
    _container: Option<ContainerAsync<Postgres>>,
}

impl Shards {
    async fn conn(&self, shard: ShardId) -> AsyncPgConnection {
        AsyncPgConnection::establish(&self.urls[&shard])
            .await
            .expect("connect")
    }

    fn pool(&self) -> ShardedDbPool {
        let pools: BTreeMap<ShardId, DbPool> = self
            .urls
            .iter()
            .map(|(shard, url)| (*shard, build_pool(url)))
            .collect();
        let default = *pools.keys().next().expect("two shards");
        ShardedDbPool::from_map(pools, default)
    }
}

/// Create one fresh, migrated database for each of the two shards.
async fn setup_shards() -> Shards {
    setup_shard_pair(OLD, NEW).await
}

/// Create one fresh, migrated database for each of `old` and `new`.
async fn setup_shard_pair(old: ShardId, new: ShardId) -> Shards {
    let (admin_url, container) = if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        (url, None)
    } else {
        let container = Postgres::default()
            .with_tag("16")
            .start()
            .await
            .expect("postgres start");
        let host = container.get_host().await.expect("host");
        let port = container.get_host_port_ipv4(5432).await.expect("port");
        (
            format!("postgresql://postgres:postgres@{host}:{port}/postgres"),
            Some(container),
        )
    };
    let mut admin = AsyncPgConnection::establish(&admin_url)
        .await
        .expect("admin connect");
    let mut urls = BTreeMap::new();
    for shard in [old, new] {
        let db_name = format!(
            "h2389_s{}_{}",
            shard.as_i32(),
            uuid::Uuid::new_v4().simple()
        );
        diesel::sql_query(format!("CREATE DATABASE {db_name}"))
            .execute(&mut admin)
            .await
            .expect("create shard database");
        let url = replace_database(&admin_url, &db_name);
        let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
        conn.batch_execute(&autumn_harvest::test_init_sql())
            .await
            .expect("migration");
        urls.insert(shard, url);
    }
    Shards {
        urls,
        _container: container,
    }
}

fn replace_database(url: &str, db_name: &str) -> String {
    let (base, query) = url
        .split_once('?')
        .map_or((url, None), |(b, q)| (b, Some(q)));
    let cut = base.rfind('/').expect("postgres URL has a database path");
    let mut out = format!("{}/{db_name}", &base[..cut]);
    if let Some(query) = query {
        out.push('?');
        out.push_str(query);
    }
    out
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("pool")
}

/// Before the change only the old shard takes new work.
fn router_before() -> ShardRouter {
    ShardRouter::new(vec![OLD, NEW], vec![OLD], OLD)
}

/// After the change both shards do.
fn router_after() -> ShardRouter {
    ShardRouter::new(vec![OLD, NEW], vec![OLD, NEW], OLD)
}

/// A DAG name that moves from the old shard to the new one.
fn moving_dag_name() -> String {
    dag_routed_to(&router_after(), NEW, "reshard_dag")
}

/// A DAG name with `prefix` that `router` places on `shard`.
fn dag_routed_to(router: &ShardRouter, shard: ShardId, prefix: &str) -> String {
    (0..1000)
        .map(|i| format!("{prefix}_{i}"))
        .find(|name| router.pick_for_dag(name) == shard)
        .expect("one of 1000 names routes to the shard")
}

/// A DAG name that moves from the new shard back to the old one.
fn returning_dag_name() -> String {
    let after = router_after();
    (0..1000)
        .map(|i| format!("reshard_back_{i}"))
        .find(|name| after.pick_for_dag(name) == OLD)
        .expect("one of 1000 names routes to the old shard")
}

fn code_dag(name: &str) -> RegisteredDag {
    RegisteredDag {
        name: name.to_string(),
        module: "tests".to_string(),
        schedule: Some(Schedule::Interval(Duration::from_secs(3600))),
        catchup: false,
        max_active_runs: 1,
        default_queue: None,
        is_unified: true,
        definition: autumn_harvest::dag::DagBuilder::new()
            .build()
            .expect("an empty DAG is a valid graph"),
        jitter: Duration::ZERO,
        overlap_policy: OverlapPolicy::Skip,
        buffer_all_max: 100,
        owner: None,
        runbook_url: None,
        severity: None,
    }
}

/// The lowered schedule of a unified DAG, as `DagInfo::as_workflow_schedule`
/// builds it.
fn lowered(dag: &RegisteredDag) -> WorkflowSchedule {
    let schedule = dag.schedule.clone().expect("a scheduled DAG");
    let mut ws = WorkflowSchedule::new(dag.name.clone(), schedule);
    ws.dag_name = Some(dag.name.clone());
    ws.catchup = dag.catchup;
    ws.max_active_runs = dag.max_active_runs;
    ws.jitter = dag.jitter;
    ws.overlap_policy = dag.overlap_policy;
    ws.buffer_all_max = dag.buffer_all_max;
    ws
}

async fn tick(pool: &ShardedDbPool, router: ShardRouter, dag: &RegisteredDag) {
    tick_with(pool, router, dag, lowered(dag)).await;
}

/// One tick with the schedule `ws` for `dag`.
async fn tick_with(
    pool: &ShardedDbPool,
    router: ShardRouter,
    dag: &RegisteredDag,
    ws: WorkflowSchedule,
) {
    try_tick(pool, router, dag, ws).await.expect("tick");
}

/// One tick with the schedule `ws` for `dag`, and its result.
async fn try_tick(
    pool: &ShardedDbPool,
    router: ShardRouter,
    dag: &RegisteredDag,
    ws: WorkflowSchedule,
) -> autumn_harvest::error::HarvestResult<()> {
    let mut catalog = DagCatalog::new();
    catalog.insert(dag.name.clone(), dag.clone());
    tick_once_sharded(
        pool.clone(),
        router,
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(catalog),
        Arc::new(vec![ws]),
        SchedulerMonitor::offline(),
    )
    .await
}

async fn schedule_expr(conn: &mut AsyncPgConnection, dag_name: &str) -> Option<String> {
    use autumn_harvest::schema::harvest_schedules::dsl;
    dsl::harvest_schedules
        .filter(dsl::dag_name.eq(dag_name))
        .select(dsl::schedule_expr)
        .first::<Option<String>>(conn)
        .await
        .optional()
        .expect("read schedule")
        .flatten()
}

async fn override_schedule(conn: &mut AsyncPgConnection, dag_name: &str) -> Option<Schedule> {
    get_dag_override(conn, dag_name)
        .await
        .expect("read override")
        .and_then(|value| value.fields.schedule)
}

fn every_minute() -> ScheduleOverridePatch {
    ScheduleOverridePatch {
        schedule: Some(Some(Schedule::Interval(Duration::from_secs(60)))),
        ..ScheduleOverridePatch::default()
    }
}

const MINUTELY: Option<Schedule> = Some(Schedule::Interval(Duration::from_secs(60)));

/// Register the DAG on the old shard, under the old router, with an override.
async fn seed_on_old_shard(shards: &Shards, pool: &ShardedDbPool, dag: &RegisteredDag) {
    let mut old = shards.conn(OLD).await;
    update_dag_override(&mut old, dag, &every_minute(), "ops", Some("incident"))
        .await
        .expect("set override");
    tick(pool, router_before(), dag).await;
    assert_eq!(
        schedule_expr(&mut old, &dag.name).await.as_deref(),
        Some("interval:60"),
        "the old shard runs the override before the move"
    );
}

/// AC1, AC2: after the collection tick the new owner runs the override, and
/// the old shard holds no override row for the DAG.
#[tokio::test]
async fn an_override_follows_its_dag_to_the_new_shard() {
    let shards = setup_shards().await;
    let pool = shards.pool();
    let dag = code_dag(&moving_dag_name());
    seed_on_old_shard(&shards, &pool, &dag).await;

    tick(&pool, router_after(), &dag).await;

    let mut old = shards.conn(OLD).await;
    let mut new = shards.conn(NEW).await;
    assert_eq!(
        schedule_expr(&mut new, &dag.name).await.as_deref(),
        Some("interval:60"),
        "the new owner reconciles to the override, not to code"
    );
    assert_eq!(override_schedule(&mut new, &dag.name).await, MINUTELY);
    assert_eq!(
        override_schedule(&mut old, &dag.name).await,
        None,
        "the old shard keeps no override row"
    );
    assert_eq!(
        schedule_expr(&mut old, &dag.name).await,
        None,
        "the old shard keeps no schedule row"
    );
    let stored = get_dag_override(&mut new, &dag.name)
        .await
        .expect("read")
        .expect("row");
    assert_eq!(stored.reason.as_deref(), Some("incident"));
    assert_eq!(stored.updated_by, "ops");
}

/// A move toward a shard that the tick visits first converges one tick later.
#[tokio::test]
async fn a_move_to_an_earlier_shard_converges_on_the_next_tick() {
    let shards = setup_shards().await;
    let pool = shards.pool();
    let dag = code_dag(&returning_dag_name());
    // Under this router the DAG lives on the new shard, then returns.
    let only_new = ShardRouter::new(vec![OLD, NEW], vec![NEW], OLD);
    let mut new = shards.conn(NEW).await;
    update_dag_override(&mut new, &dag, &every_minute(), "ops", None)
        .await
        .expect("set override");
    tick(&pool, only_new, &dag).await;

    tick(&pool, router_after(), &dag).await;
    tick(&pool, router_after(), &dag).await;

    let mut old = shards.conn(OLD).await;
    assert_eq!(
        schedule_expr(&mut old, &dag.name).await.as_deref(),
        Some("interval:60")
    );
    assert_eq!(override_schedule(&mut new, &dag.name).await, None);
}

/// AC3: a move that fails half-way leaves the override readable, and a retry
/// completes it. Both halves can fail: the copy, and the delete after it.
#[tokio::test]
async fn a_failed_move_keeps_the_override_and_a_retry_completes_it() {
    let shards = setup_shards().await;
    let pool = shards.pool();
    let dag = code_dag(&moving_dag_name());
    seed_on_old_shard(&shards, &pool, &dag).await;
    let mut old = shards.conn(OLD).await;
    let mut new = shards.conn(NEW).await;

    // The copy fails: the new shard refuses the insert.
    refuse(&mut new, "INSERT").await;
    tick(&pool, router_after(), &dag).await;
    assert_eq!(override_schedule(&mut old, &dag.name).await, MINUTELY);
    assert_eq!(override_schedule(&mut new, &dag.name).await, None);
    allow(&mut new).await;

    // The copy lands, and the delete fails: both shards hold the override.
    refuse(&mut old, "DELETE").await;
    tick(&pool, router_after(), &dag).await;
    assert_eq!(override_schedule(&mut old, &dag.name).await, MINUTELY);
    assert_eq!(override_schedule(&mut new, &dag.name).await, MINUTELY);
    allow(&mut old).await;

    tick(&pool, router_after(), &dag).await;
    assert_eq!(override_schedule(&mut old, &dag.name).await, None);
    assert_eq!(override_schedule(&mut new, &dag.name).await, MINUTELY);
    assert_eq!(
        schedule_expr(&mut new, &dag.name).await.as_deref(),
        Some("interval:60")
    );
}

/// AC2: a retry after a failed source delete keeps an operator clear.
///
/// The copy lands, and the source delete fails. The operator then clears the
/// override on the new owner. The next tick must not copy the stale source
/// row back.
#[tokio::test]
async fn an_operator_clear_survives_a_retried_carry() {
    let shards = setup_shards().await;
    let pool = shards.pool();
    let dag = code_dag(&moving_dag_name());
    seed_on_old_shard(&shards, &pool, &dag).await;
    let mut old = shards.conn(OLD).await;
    let mut new = shards.conn(NEW).await;
    refuse(&mut old, "DELETE").await;
    tick(&pool, router_after(), &dag).await;
    assert_eq!(override_schedule(&mut old, &dag.name).await, MINUTELY);
    assert_eq!(override_schedule(&mut new, &dag.name).await, MINUTELY);
    allow(&mut old).await;

    let cleared = clear_dag_override(&mut new, &dag.name)
        .await
        .expect("clear on the new owner");
    assert!(cleared, "the new owner held the copy");
    tick(&pool, router_after(), &dag).await;

    assert_eq!(
        override_schedule(&mut new, &dag.name).await,
        None,
        "the clear on the new owner stays"
    );
    assert_eq!(
        override_schedule(&mut old, &dag.name).await,
        None,
        "the retry deletes the stale source row"
    );
    assert_eq!(
        schedule_expr(&mut new, &dag.name).await.as_deref(),
        Some("interval:3600"),
        "the new owner runs the code value"
    );
}

/// An override that moves out, back and out again arrives each time.
///
/// Each move leaves a record of the copy on the shard that received it. A
/// record that outlives its row must not make a later move skip the copy.
#[tokio::test]
async fn an_override_survives_a_round_trip_reshard() {
    let shards = setup_shards().await;
    let pool = shards.pool();
    let dag = code_dag(&moving_dag_name());
    seed_on_old_shard(&shards, &pool, &dag).await;
    let mut old = shards.conn(OLD).await;
    let mut new = shards.conn(NEW).await;

    tick(&pool, router_after(), &dag).await;
    assert_eq!(override_schedule(&mut new, &dag.name).await, MINUTELY);
    // The old shard is first in shard order, so the move back takes two ticks.
    tick(&pool, router_before(), &dag).await;
    tick(&pool, router_before(), &dag).await;
    assert_eq!(override_schedule(&mut old, &dag.name).await, MINUTELY);
    assert_eq!(override_schedule(&mut new, &dag.name).await, None);
    tick(&pool, router_after(), &dag).await;

    assert_eq!(
        override_schedule(&mut new, &dag.name).await,
        MINUTELY,
        "the third move delivers the override"
    );
    assert_eq!(override_schedule(&mut old, &dag.name).await, None);
    assert_eq!(
        schedule_expr(&mut new, &dag.name).await.as_deref(),
        Some("interval:60")
    );
}

/// An override that the new owner already holds wins over the moved copy.
///
/// An operator who changes the DAG after the move writes to the new owner.
/// The move must not undo that change.
#[tokio::test]
async fn an_override_on_the_new_owner_is_kept() {
    let shards = setup_shards().await;
    let pool = shards.pool();
    let dag = code_dag(&moving_dag_name());
    seed_on_old_shard(&shards, &pool, &dag).await;
    let mut new = shards.conn(NEW).await;
    let newer = ScheduleOverridePatch {
        schedule: Some(Some(Schedule::Interval(Duration::from_secs(120)))),
        ..ScheduleOverridePatch::default()
    };
    update_dag_override(&mut new, &dag, &newer, "oncall", None)
        .await
        .expect("set override on the new owner");

    tick(&pool, router_after(), &dag).await;

    let mut old = shards.conn(OLD).await;
    assert_eq!(override_schedule(&mut old, &dag.name).await, None);
    assert_eq!(
        override_schedule(&mut new, &dag.name).await,
        Some(Schedule::Interval(Duration::from_secs(120)))
    );
}

/// Two shards on one pool share one table, so the move deletes nothing.
///
/// The copy finds the row already in place. A delete after it would remove
/// the only copy.
#[tokio::test]
async fn colocated_shards_keep_the_override() {
    let shards = setup_shards().await;
    let pool = colocated_pool(&shards);
    let dag = code_dag(&moving_dag_name());
    let mut old = shards.conn(OLD).await;
    update_dag_override(&mut old, &dag, &every_minute(), "ops", None)
        .await
        .expect("set override");

    tick(&pool, router_before(), &dag).await;
    tick(&pool, router_after(), &dag).await;

    assert_eq!(override_schedule(&mut old, &dag.name).await, MINUTELY);
}

/// The id and the next fire time of the schedule row of a DAG.
async fn schedule_row(
    conn: &mut AsyncPgConnection,
    dag_name: &str,
) -> Option<(uuid::Uuid, Option<chrono::DateTime<chrono::Utc>>)> {
    use autumn_harvest::schema::harvest_schedules::dsl;
    dsl::harvest_schedules
        .filter(dsl::dag_name.eq(dag_name))
        .select((dsl::id, dsl::next_run_at))
        .first(conn)
        .await
        .optional()
        .expect("read schedule row")
}

/// A pool that two shards share.
fn colocated_pool(shards: &Shards) -> ShardedDbPool {
    let shared = build_pool(&shards.urls[&OLD]);
    ShardedDbPool::from_map(BTreeMap::from([(OLD, shared.clone()), (NEW, shared)]), OLD)
}

/// Two ticks under `router` keep one row with one id and one fire time.
async fn assert_row_is_stable(
    shards: &Shards,
    pool: &ShardedDbPool,
    router: &ShardRouter,
    dag: &RegisteredDag,
) {
    let mut conn = shards.conn(OLD).await;
    tick(pool, router.clone(), dag).await;
    let first = schedule_row(&mut conn, &dag.name).await;
    assert!(first.is_some(), "the DAG has a schedule row after one tick");
    tick(pool, router.clone(), dag).await;
    assert_eq!(
        schedule_row(&mut conn, &dag.name).await,
        first,
        "a second tick does not delete and insert the row again"
    );
}

/// Issue #2405, AC1: the owner is first in shard order.
///
/// Before the fix, the shard after the owner deleted the row each tick.
#[tokio::test]
async fn colocated_shards_keep_the_schedule_row_of_an_earlier_owner() {
    let shards = setup_shards().await;
    let pool = colocated_pool(&shards);
    let dag = code_dag(&moving_dag_name());

    assert_row_is_stable(&shards, &pool, &router_before(), &dag).await;
}

/// Issue #2405, AC1: the owner is last in shard order.
///
/// Before the fix, the shard before the owner deleted the row each tick. The
/// owner then inserted it again with a new fire time.
#[tokio::test]
async fn colocated_shards_keep_the_schedule_row_of_a_later_owner() {
    let shards = setup_shards().await;
    let pool = colocated_pool(&shards);
    let only_new = ShardRouter::new(vec![OLD, NEW], vec![NEW], OLD);
    let dag = code_dag(&moving_dag_name());

    assert_row_is_stable(&shards, &pool, &only_new, &dag).await;
}

/// Issue #2405, AC1: a DAG on every writable shard keeps its row.
///
/// The read-only shard of the pool does not own the DAG. Before the fix, it
/// deleted the row of the writable owner.
#[tokio::test]
async fn colocated_shards_keep_the_row_of_a_dag_on_every_writable_shard() {
    let shards = setup_shards().await;
    let pool = colocated_pool(&shards);
    let dag = code_dag(&moving_dag_name());
    let ws = lowered(&dag).with_all_writable_shards();
    let mut conn = shards.conn(OLD).await;

    tick_with(&pool, router_before(), &dag, ws.clone()).await;
    let first = schedule_row(&mut conn, &dag.name).await;
    assert!(first.is_some(), "the DAG has a schedule row after one tick");
    tick_with(&pool, router_before(), &dag, ws).await;
    assert_eq!(schedule_row(&mut conn, &dag.name).await, first);
}

/// Issue #2405: the owner fires its DAG, not the pool peer before it.
///
/// Both shards read the due row from one table. A fire mints an execution id
/// for the firing shard. The run must carry the shard id of the owner.
#[tokio::test]
async fn the_owner_fires_a_due_dag_on_a_colocated_pool() {
    use autumn_harvest::schema::harvest_workflow_executions::dsl as exec;
    let shards = setup_shards().await;
    let pool = colocated_pool(&shards);
    let only_new = ShardRouter::new(vec![OLD, NEW], vec![NEW], OLD);
    let dag = code_dag(&moving_dag_name());
    let mut conn = shards.conn(OLD).await;
    tick(&pool, only_new.clone(), &dag).await;
    conn.batch_execute("UPDATE harvest_schedules SET next_run_at = now() - interval '1 minute'")
        .await
        .expect("make the row due");

    tick(&pool, only_new, &dag).await;

    let fired: Vec<i32> = exec::harvest_workflow_executions
        .select(exec::shard_id)
        .load(&mut conn)
        .await
        .expect("read executions");
    assert_eq!(fired, vec![NEW.as_i32()], "one run, on the owner shard");
}

/// Issue #2405, AC2: a shard with its own pool still deletes a stale row.
#[tokio::test]
async fn a_shard_with_its_own_pool_collects_a_stale_schedule_row() {
    let shards = setup_shards().await;
    let pool = shards.pool();
    let dag = code_dag(&moving_dag_name());
    tick(&pool, router_before(), &dag).await;
    let mut old = shards.conn(OLD).await;
    assert!(schedule_row(&mut old, &dag.name).await.is_some());

    tick(&pool, router_after(), &dag).await;

    assert_eq!(
        schedule_row(&mut old, &dag.name).await,
        None,
        "the old shard deletes the row that it no longer owns"
    );
    let mut new = shards.conn(NEW).await;
    assert!(schedule_row(&mut new, &dag.name).await.is_some());
}

/// Issue #2405, AC2: two shards of one pool still delete a row when no shard
/// of that pool owns the DAG.
#[tokio::test]
async fn colocated_shards_collect_a_row_that_moved_to_another_pool() {
    const OTHER: ShardId = ShardId::new(2);
    let shards = setup_shards().await;
    let shared = build_pool(&shards.urls[&OLD]);
    let pool = ShardedDbPool::from_map(
        BTreeMap::from([
            (OLD, shared.clone()),
            (NEW, shared),
            (OTHER, build_pool(&shards.urls[&NEW])),
        ]),
        OLD,
    );
    let all = vec![OLD, NEW, OTHER];
    let dag = code_dag(&moving_dag_name());
    tick(&pool, ShardRouter::new(all.clone(), vec![OLD], OLD), &dag).await;
    let mut shared_conn = shards.conn(OLD).await;
    assert!(schedule_row(&mut shared_conn, &dag.name).await.is_some());

    tick(&pool, ShardRouter::new(all, vec![OTHER], OLD), &dag).await;

    assert_eq!(
        schedule_row(&mut shared_conn, &dag.name).await,
        None,
        "the shared pool deletes the row that moved off it"
    );
    let mut other = shards.conn(NEW).await;
    assert!(schedule_row(&mut other, &dag.name).await.is_some());
}

/// Make one statement kind on the override table fail on this shard.
async fn refuse(conn: &mut AsyncPgConnection, statement: &str) {
    conn.batch_execute(&format!(
        "CREATE FUNCTION h2389_refuse() RETURNS trigger LANGUAGE plpgsql AS \
         $$ BEGIN RAISE EXCEPTION 'h2389 refuses'; END $$; \
         CREATE TRIGGER h2389_refuse BEFORE {statement} ON harvest_schedule_overrides \
         FOR EACH ROW EXECUTE FUNCTION h2389_refuse();"
    ))
    .await
    .expect("install refusal");
}

async fn allow(conn: &mut AsyncPgConnection) {
    conn.batch_execute(
        "DROP TRIGGER h2389_refuse ON harvest_schedule_overrides; \
         DROP FUNCTION h2389_refuse();",
    )
    .await
    .expect("drop refusal");
}

// The DR fence of the carry. These tests pin the process-global fence
// registry, so they hold its lock and use shard ids that no other test uses.

const DR_OLD: ShardId = ShardId::new(50);
const DR_NEW: ShardId = ShardId::new(51);

/// The advisory key that holds a copy on the destination.
const HOLD_KEY: i64 = 7_413_001;

/// Make each insert into the override table wait while a test holds
/// [`HOLD_KEY`].
async fn hold_override_inserts(conn: &mut AsyncPgConnection) {
    conn.batch_execute(&format!(
        "CREATE FUNCTION dr_hold_copy() RETURNS trigger LANGUAGE plpgsql AS \
         $$ BEGIN PERFORM pg_advisory_xact_lock_shared({HOLD_KEY}); RETURN NEW; END $$; \
         CREATE TRIGGER dr_hold_copy BEFORE INSERT ON harvest_schedule_overrides \
         FOR EACH ROW EXECUTE FUNCTION dr_hold_copy();"
    ))
    .await
    .expect("install the hold");
}

/// Wait until a session of this database waits for [`HOLD_KEY`].
async fn wait_for_held_copy(conn: &mut AsyncPgConnection) {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let probe = format!(
        "SELECT count(*) AS n FROM pg_locks l JOIN pg_database d ON d.oid = l.database \
         WHERE d.datname = current_database() AND l.locktype = 'advisory' \
         AND NOT l.granted AND l.objid = {HOLD_KEY}"
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let waiting = diesel::sql_query(&probe)
            .get_result::<Count>(conn)
            .await
            .expect("read the lock table")
            .n;
        if waiting > 0 {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the carry never reached the copy"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// End every DR fence guard session on this database. The old region then
/// loses its barrier, as a network partition does.
async fn end_guard_sessions(conn: &mut AsyncPgConnection) {
    conn.batch_execute(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
         WHERE datname = current_database() AND application_name = 'harvest_dr_fence_pass'",
    )
    .await
    .expect("end the guard sessions");
}

/// AC1: a promotion during a carry stops every write of the old process.
///
/// The copy waits on the destination. The new region then promotes the
/// destination, and the old region loses its guard sessions. When the copy
/// can go on, the old process must write nothing: no copy on the new shard
/// and no delete on the old one. A later tick of the old process writes
/// nothing either.
#[tokio::test]
async fn a_promotion_during_a_carry_stops_every_write_of_the_old_process() {
    use autumn_harvest::replication::{FenceRegistry, bump_generation, ensure_generation_row};

    let _fence_serial = crate::cross_region_dr_tests::registry_guard().await;
    let shards = setup_shard_pair(DR_OLD, DR_NEW).await;
    let pool = shards.pool();
    let before = ShardRouter::new(vec![DR_OLD, DR_NEW], vec![DR_OLD], DR_OLD);
    let after = ShardRouter::new(vec![DR_OLD, DR_NEW], vec![DR_OLD, DR_NEW], DR_OLD);
    let dag = code_dag(&dag_routed_to(&after, DR_NEW, "dr_carry"));
    let mut old = shards.conn(DR_OLD).await;
    let mut new = shards.conn(DR_NEW).await;
    let old_generation = ensure_generation_row(&mut old, DR_OLD)
        .await
        .expect("old generation row");
    let new_generation = ensure_generation_row(&mut new, DR_NEW)
        .await
        .expect("new generation row");
    FenceRegistry::clear();
    FenceRegistry::register(DR_OLD, old_generation).expect("pin the old shard");
    FenceRegistry::register(DR_NEW, new_generation).expect("pin the new shard");
    update_dag_override(&mut old, &dag, &every_minute(), "ops", None)
        .await
        .expect("set override");
    tick(&pool, before, &dag).await;
    assert_eq!(
        schedule_expr(&mut old, &dag.name).await.as_deref(),
        Some("interval:60")
    );

    let mut holder = shards.conn(DR_NEW).await;
    holder
        .batch_execute(&format!("SELECT pg_advisory_lock({HOLD_KEY})"))
        .await
        .expect("hold the copy");
    hold_override_inserts(&mut new).await;
    let carry = {
        let (pool, router, dag) = (pool.clone(), after.clone(), dag.clone());
        tokio::spawn(async move { try_tick(&pool, router, &dag, lowered(&dag)).await })
    };
    wait_for_held_copy(&mut new).await;

    let bump = {
        let url = shards.urls[&DR_NEW].clone();
        tokio::spawn(async move {
            let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
            bump_generation(&mut conn, DR_NEW, "failover", "test").await
        })
    };
    end_guard_sessions(&mut new).await;
    bump.await
        .expect("the bump task")
        .expect("the promotion commits");
    holder
        .batch_execute(&format!("SELECT pg_advisory_unlock({HOLD_KEY})"))
        .await
        .expect("free the copy");
    let _ = tokio::time::timeout(Duration::from_secs(60), carry)
        .await
        .expect("the tick ends");

    assert_eq!(
        override_schedule(&mut new, &dag.name).await,
        None,
        "the old process copies nothing after the promotion"
    );
    assert_eq!(
        override_schedule(&mut old, &dag.name).await,
        MINUTELY,
        "the old process deletes nothing after the promotion"
    );

    let _ = try_tick(&pool, after, &dag, lowered(&dag)).await;
    assert_eq!(override_schedule(&mut new, &dag.name).await, None);
    assert_eq!(override_schedule(&mut old, &dag.name).await, MINUTELY);
}

/// A carry on a shard that this process lost writes nothing on either shard.
///
/// The pass guards cover the carry in the scheduler. Each carry transaction
/// also asserts the fence of its own shard. This test calls the carry with no
/// guard, so only those asserts stand between it and a write.
#[tokio::test]
async fn a_fenced_carry_writes_nothing() {
    use autumn_harvest::replication::{FenceRegistry, bump_generation, ensure_generation_row};
    use autumn_harvest::schedule_override::carry_dag_override;

    let _fence_serial = crate::cross_region_dr_tests::registry_guard().await;
    let shards = setup_shard_pair(DR_OLD, DR_NEW).await;
    let dag = code_dag("dr_fenced_carry");
    let mut old = shards.conn(DR_OLD).await;
    let mut new = shards.conn(DR_NEW).await;
    let old_generation = ensure_generation_row(&mut old, DR_OLD)
        .await
        .expect("old generation row");
    let new_generation = ensure_generation_row(&mut new, DR_NEW)
        .await
        .expect("new generation row");
    update_dag_override(&mut old, &dag, &every_minute(), "ops", None)
        .await
        .expect("set override");
    FenceRegistry::clear();
    FenceRegistry::register(DR_OLD, old_generation).expect("pin the old shard");
    FenceRegistry::register(DR_NEW, new_generation).expect("pin the new shard");

    // The destination is promoted: the copy is refused.
    bump_generation(&mut new, DR_NEW, "failover", "test")
        .await
        .expect("promote the new shard");
    let refused = carry_dag_override(&mut old, &mut new, DR_OLD, DR_NEW, &dag.name).await;
    assert!(
        matches!(
            refused,
            Err(autumn_harvest::error::HarvestError::ShardFenced { .. })
        ),
        "{refused:?}"
    );
    assert_eq!(override_schedule(&mut new, &dag.name).await, None);
    assert_eq!(override_schedule(&mut old, &dag.name).await, MINUTELY);

    // The destination is current again, and the source is promoted: the
    // copy lands, and the delete is refused.
    let promoted_new = autumn_harvest::replication::current_generation(&mut new, DR_NEW)
        .await
        .expect("read the new generation")
        .expect("the new shard has a row");
    FenceRegistry::clear();
    FenceRegistry::register(DR_OLD, old_generation).expect("pin the old shard");
    FenceRegistry::register(DR_NEW, promoted_new).expect("pin the new shard");
    bump_generation(&mut old, DR_OLD, "failover", "test")
        .await
        .expect("promote the old shard");
    let refused = carry_dag_override(&mut old, &mut new, DR_OLD, DR_NEW, &dag.name).await;
    assert!(
        matches!(
            refused,
            Err(autumn_harvest::error::HarvestError::ShardFenced { .. })
        ),
        "{refused:?}"
    );
    assert_eq!(override_schedule(&mut new, &dag.name).await, MINUTELY);
    assert_eq!(
        override_schedule(&mut old, &dag.name).await,
        MINUTELY,
        "a fenced source keeps its row"
    );
}
