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
//! Issue #2602: a routing change can land at any time. A clear stays, the
//! newest set wins, and two opposite carries keep the override.
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
use chrono::{DateTime, Utc};
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
    dag_routed_to(&router_after(), OLD, "reshard_back")
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

/// A retry after a failed source delete keeps an operator clear.
///
/// The copy lands, and the source delete fails. The operator then clears the
/// override on the new owner. No later tick may copy the stale source row
/// back: not while the delete still fails, and not when it succeeds.
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

    let cleared = clear_dag_override(&mut new, &dag.name)
        .await
        .expect("clear on the new owner");
    assert!(cleared, "the new owner held the copy");
    tick(&pool, router_after(), &dag).await;
    assert_eq!(
        override_schedule(&mut new, &dag.name).await,
        None,
        "a retry whose delete fails again keeps the clear"
    );
    assert_eq!(override_schedule(&mut old, &dag.name).await, MINUTELY);
    allow(&mut old).await;
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

/// A newer override that the new owner already holds wins over the moved
/// copy.
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

/// The advisory key that holds one carry statement.
const HOLD_KEY: i64 = 7_413_001;

/// Make each `statement` on the override table of this database wait while
/// a test holds [`HOLD_KEY`].
async fn hold_override_statements(conn: &mut AsyncPgConnection, statement: &str) {
    let returned = if statement == "DELETE" { "OLD" } else { "NEW" };
    conn.batch_execute(&format!(
        "CREATE FUNCTION dr_hold_carry() RETURNS trigger LANGUAGE plpgsql AS \
         $$ BEGIN PERFORM pg_advisory_xact_lock_shared({HOLD_KEY}); RETURN {returned}; END $$; \
         CREATE TRIGGER dr_hold_carry BEFORE {statement} ON harvest_schedule_overrides \
         FOR EACH ROW EXECUTE FUNCTION dr_hold_carry();"
    ))
    .await
    .expect("install the hold");
}

/// Wait until a session of this database waits for [`HOLD_KEY`].
async fn wait_for_held_statement(conn: &mut AsyncPgConnection) {
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
            "the carry never reached the held statement"
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

/// How many floor rows this database holds for `dag_name`.
async fn floor_rows(conn: &mut AsyncPgConnection, dag_name: &str) -> i64 {
    use autumn_harvest::schema::harvest_schedule_override_receipts::dsl;
    dsl::harvest_schedule_override_receipts
        .filter(dsl::name.eq(dag_name))
        .count()
        .get_result(conn)
        .await
        .expect("read the floor rows")
}

/// A DAG with an override on [`DR_OLD`], under DR pins of both shards.
struct DrCarry {
    shards: Shards,
    pool: ShardedDbPool,
    after: ShardRouter,
    dag: RegisteredDag,
}

/// Pin both DR shards and register a DAG with an override on [`DR_OLD`].
async fn pinned_carry(prefix: &str) -> DrCarry {
    use autumn_harvest::replication::{FenceRegistry, ensure_generation_row};

    let shards = setup_shard_pair(DR_OLD, DR_NEW).await;
    let pool = shards.pool();
    let before = ShardRouter::new(vec![DR_OLD, DR_NEW], vec![DR_OLD], DR_OLD);
    let after = ShardRouter::new(vec![DR_OLD, DR_NEW], vec![DR_OLD, DR_NEW], DR_OLD);
    let dag = code_dag(&dag_routed_to(&after, DR_NEW, prefix));
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
    DrCarry {
        shards,
        pool,
        after,
        dag,
    }
}

/// Promote `shard` while a carry waits there.
///
/// The test holds `statement` of the carry on `shard` and promotes `shard`.
/// It ends the guard sessions of the old region there. It then frees the
/// statement and lets the tick end.
async fn promote_during_carry(carry: &DrCarry, shard: ShardId, statement: &str) {
    use autumn_harvest::replication::bump_generation;

    let mut held = carry.shards.conn(shard).await;
    let mut holder = carry.shards.conn(shard).await;
    holder
        .batch_execute(&format!("SELECT pg_advisory_lock({HOLD_KEY})"))
        .await
        .expect("hold the statement");
    hold_override_statements(&mut held, statement).await;
    let tick = {
        let (pool, router, dag) = (carry.pool.clone(), carry.after.clone(), carry.dag.clone());
        tokio::spawn(async move { try_tick(&pool, router, &dag, lowered(&dag)).await })
    };
    wait_for_held_statement(&mut held).await;

    let bump = {
        let url = carry.shards.urls[&shard].clone();
        tokio::spawn(async move {
            let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
            bump_generation(&mut conn, shard, "failover", "test").await
        })
    };
    end_guard_sessions(&mut held).await;
    tokio::time::timeout(Duration::from_secs(60), bump)
        .await
        .expect("the bump ends")
        .expect("the bump task joins")
        .expect("the promotion commits");
    holder
        .batch_execute(&format!("SELECT pg_advisory_unlock({HOLD_KEY})"))
        .await
        .expect("free the statement");
    // The stopped pass makes the tick fail or succeed. Only its writes count.
    let _ = tokio::time::timeout(Duration::from_secs(60), tick)
        .await
        .expect("the tick ends")
        .expect("the tick task joins");
}

/// A promotion of the destination during a carry stops every write of the
/// old process.
///
/// The copy waits on the destination. The new region then promotes the
/// destination, and the old region loses its guard sessions. When the copy
/// can go on, the old process must write nothing: no copy on the new shard
/// and no delete on the old one. A later tick of the old process writes
/// nothing either.
#[tokio::test]
async fn a_promotion_during_a_carry_stops_every_write_of_the_old_process() {
    let _fence_serial = crate::cross_region_dr_tests::registry_guard().await;
    let carry = pinned_carry("dr_carry").await;
    let mut old = carry.shards.conn(DR_OLD).await;
    let mut new = carry.shards.conn(DR_NEW).await;

    promote_during_carry(&carry, DR_NEW, "INSERT").await;

    let name = carry.dag.name.as_str();
    assert_eq!(
        override_schedule(&mut new, name).await,
        None,
        "the old process copies nothing after the promotion"
    );
    assert_eq!(floor_rows(&mut new, name).await, 0);
    assert_eq!(
        override_schedule(&mut old, name).await,
        MINUTELY,
        "the old process deletes nothing after the promotion"
    );

    try_tick(
        &carry.pool,
        carry.after.clone(),
        &carry.dag,
        lowered(&carry.dag),
    )
    .await
    .expect("a later tick of the old process runs");
    assert_eq!(override_schedule(&mut new, name).await, None);
    assert_eq!(floor_rows(&mut new, name).await, 0);
    assert_eq!(
        schedule_expr(&mut new, name).await,
        None,
        "the old process writes no schedule row on the promoted shard"
    );
    assert_eq!(override_schedule(&mut old, name).await, MINUTELY);
}

/// A promotion of the source during a carry stops its delete.
///
/// The copy lands before the promotion. The delete then waits on the source,
/// and the new region promotes the source. The source must keep its row, so
/// the old process writes nothing there after the promotion.
#[tokio::test]
async fn a_promotion_of_the_source_during_a_carry_stops_its_delete() {
    let _fence_serial = crate::cross_region_dr_tests::registry_guard().await;
    let carry = pinned_carry("dr_source").await;
    let mut old = carry.shards.conn(DR_OLD).await;
    let mut new = carry.shards.conn(DR_NEW).await;

    promote_during_carry(&carry, DR_OLD, "DELETE").await;

    let name = carry.dag.name.as_str();
    assert_eq!(
        override_schedule(&mut new, name).await,
        MINUTELY,
        "the copy committed before the promotion"
    );
    assert_eq!(
        floor_rows(&mut new, name).await,
        0,
        "a copy into an empty shard raises no floor"
    );
    assert_eq!(
        override_schedule(&mut old, name).await,
        MINUTELY,
        "the old process deletes nothing after the promotion"
    );

    try_tick(
        &carry.pool,
        carry.after.clone(),
        &carry.dag,
        lowered(&carry.dag),
    )
    .await
    .expect("a later tick of the old process runs");
    assert_eq!(override_schedule(&mut old, name).await, MINUTELY);
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

    // A bump promotes the destination, and the fence refuses the copy.
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

    // The pin of the destination is current again, and a bump promotes the
    // source. The carry asserts both fences before it writes, so it copies
    // nothing.
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
    assert_eq!(
        override_schedule(&mut new, &dag.name).await,
        None,
        "a fenced source stops the copy too"
    );
    assert_eq!(
        override_schedule(&mut old, &dag.name).await,
        MINUTELY,
        "a fenced source keeps its row"
    );
}

// Issue #2602: a routing change can land while a carry is not settled.

/// The `updated_at` of the override row of `dag_name`, if any.
async fn version(conn: &mut AsyncPgConnection, dag_name: &str) -> Option<DateTime<Utc>> {
    get_dag_override(conn, dag_name)
        .await
        .expect("read override")
        .map(|value| value.updated_at)
}

/// The floor of `dag_name` on this database, if any.
async fn floor_of(conn: &mut AsyncPgConnection, dag_name: &str) -> Option<DateTime<Utc>> {
    use autumn_harvest::schema::harvest_schedule_override_receipts::dsl;
    dsl::harvest_schedule_override_receipts
        .filter(dsl::kind.eq("dag"))
        .filter(dsl::name.eq(dag_name))
        .select(dsl::row_updated_at)
        .first(conn)
        .await
        .optional()
        .expect("read the floor")
}

/// Set the floor of `dag_name` on this database to `at`.
async fn put_floor(conn: &mut AsyncPgConnection, dag_name: &str, at: DateTime<Utc>) {
    use autumn_harvest::schema::harvest_schedule_override_receipts::dsl;
    diesel::insert_into(dsl::harvest_schedule_override_receipts)
        .values((
            dsl::kind.eq("dag"),
            dsl::name.eq(dag_name),
            dsl::row_updated_at.eq(at),
        ))
        .execute(conn)
        .await
        .expect("write the floor");
}

/// Issue #2602, AC 2: a route back after a failed delete keeps a clear.
///
/// The old shard carries its row to the new owner, and its delete fails. The
/// route then returns to the old shard, and the operator clears the override
/// there. The stale copy on the new shard must not come back.
#[tokio::test]
async fn a_clear_on_the_old_shard_survives_a_route_back() {
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

    let cleared = clear_dag_override(&mut old, &dag.name)
        .await
        .expect("clear on the old shard");
    assert!(cleared, "the old shard held its own row");
    tick(&pool, router_before(), &dag).await;

    assert_eq!(
        override_schedule(&mut old, &dag.name).await,
        None,
        "the clear on the old shard stays"
    );
    assert_eq!(
        override_schedule(&mut new, &dag.name).await,
        None,
        "the route back deletes the stale copy"
    );
    tick(&pool, router_before(), &dag).await;
    assert_eq!(
        schedule_expr(&mut old, &dag.name).await.as_deref(),
        Some("interval:3600"),
        "the old shard runs the code value"
    );
}

/// Wait until a session of this database waits for a lock.
async fn wait_for_lock_wait(conn: &mut AsyncPgConnection) {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let probe = "SELECT count(*) AS n FROM pg_stat_activity \
                 WHERE datname = current_database() AND wait_event_type = 'Lock'";
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let waiting = diesel::sql_query(probe)
            .get_result::<Count>(conn)
            .await
            .expect("read the session table")
            .n;
        if waiting > 0 {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no session waits for a lock"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Issue #2602, AC 3: two carries in opposite directions keep the override.
///
/// One process routes the DAG to the new shard, and another routes it back.
/// The test holds the generation row of the old shard, and only the outward
/// carry pins it. So the outward carry waits at the fence of the old shard.
/// The return carry names the old database by an unpinned shard id, so it
/// runs. Before #2602 the outward copy had committed at that point. The
/// return carry then yielded to the old row and deleted the copy. The
/// outward delete then removed the old row.
#[tokio::test]
async fn opposite_carries_keep_the_override() {
    use autumn_harvest::replication::{FenceRegistry, ensure_generation_row};
    use autumn_harvest::schedule_override::carry_dag_override;
    // The old database under a shard id that no process pins.
    const OLD_ALIAS: ShardId = ShardId::new(52);

    let _fence_serial = crate::cross_region_dr_tests::registry_guard().await;
    let shards = setup_shard_pair(DR_OLD, DR_NEW).await;
    let dag = code_dag("opposite_carries");
    let mut old = shards.conn(DR_OLD).await;
    let mut new = shards.conn(DR_NEW).await;
    update_dag_override(&mut old, &dag, &every_minute(), "ops", None)
        .await
        .expect("set override");
    let generation = ensure_generation_row(&mut old, DR_OLD)
        .await
        .expect("old generation row");
    FenceRegistry::clear();
    FenceRegistry::register(DR_OLD, generation).expect("pin the old shard");
    let mut holder = shards.conn(DR_OLD).await;
    holder
        .batch_execute(&format!(
            "BEGIN; SELECT 1 FROM harvest_shard_generation WHERE shard_id = {} FOR UPDATE",
            DR_OLD.as_i32()
        ))
        .await
        .expect("hold the generation row");

    let outward = {
        let (mut from, mut to) = (shards.conn(DR_OLD).await, shards.conn(DR_NEW).await);
        let name = dag.name.clone();
        tokio::spawn(
            async move { carry_dag_override(&mut from, &mut to, DR_OLD, DR_NEW, &name).await },
        )
    };
    wait_for_lock_wait(&mut old).await;
    let (mut from, mut to) = (shards.conn(DR_NEW).await, shards.conn(DR_OLD).await);
    let back = carry_dag_override(&mut from, &mut to, DR_NEW, OLD_ALIAS, &dag.name).await;
    holder
        .batch_execute("COMMIT")
        .await
        .expect("free the generation row");
    let outward = outward.await.expect("the outward carry joins");
    FenceRegistry::clear();

    back.expect("the return carry runs");
    outward.expect("the outward carry runs");
    let held = [
        override_schedule(&mut old, &dag.name).await,
        override_schedule(&mut new, &dag.name).await,
    ];
    assert_eq!(
        held.iter()
            .filter(|schedule| **schedule == MINUTELY)
            .count(),
        1,
        "one shard keeps the override: {held:?}"
    );
}

/// Issue #2602: a newer source row replaces an older row on the owner.
///
/// The new owner holds an older override. The operator then sets a newer one
/// on the old shard, under a router that still sends the DAG there. Before
/// #2602 the row on the owner always won, so the newer set was lost.
#[tokio::test]
async fn a_newer_source_row_replaces_an_older_row_on_the_owner() {
    let shards = setup_shards().await;
    let pool = shards.pool();
    let dag = code_dag(&moving_dag_name());
    let mut new = shards.conn(NEW).await;
    let older = ScheduleOverridePatch {
        schedule: Some(Some(Schedule::Interval(Duration::from_secs(120)))),
        ..ScheduleOverridePatch::default()
    };
    update_dag_override(&mut new, &dag, &older, "ops", None)
        .await
        .expect("set the older override on the new owner");
    let replaced = version(&mut new, &dag.name).await.expect("a version");
    let mut old = shards.conn(OLD).await;
    update_dag_override(&mut old, &dag, &every_minute(), "ops", None)
        .await
        .expect("set the newer override on the old shard");

    tick(&pool, router_after(), &dag).await;

    assert_eq!(override_schedule(&mut old, &dag.name).await, None);
    assert_eq!(
        override_schedule(&mut new, &dag.name).await,
        MINUTELY,
        "the newer set wins"
    );
    assert_eq!(
        floor_of(&mut new, &dag.name).await,
        Some(replaced),
        "the replaced row raises the floor of the owner"
    );
}

/// Issue #2602: a replace and a clear raise the floor of their shard.
#[tokio::test]
async fn a_replace_and_a_clear_raise_the_floor() {
    let shards = setup_shards().await;
    let dag = code_dag("floor_dag");
    let mut conn = shards.conn(OLD).await;
    update_dag_override(&mut conn, &dag, &every_minute(), "ops", None)
        .await
        .expect("first set");
    let first = version(&mut conn, &dag.name).await.expect("a version");
    assert_eq!(
        floor_of(&mut conn, &dag.name).await,
        None,
        "a first set retires nothing"
    );

    let hourly = ScheduleOverridePatch {
        schedule: Some(Some(Schedule::Interval(Duration::from_secs(7200)))),
        ..ScheduleOverridePatch::default()
    };
    update_dag_override(&mut conn, &dag, &hourly, "ops", None)
        .await
        .expect("replace");
    let second = version(&mut conn, &dag.name).await.expect("a version");
    assert!(second > first, "{second} > {first}");
    assert_eq!(floor_of(&mut conn, &dag.name).await, Some(first));

    assert!(
        clear_dag_override(&mut conn, &dag.name)
            .await
            .expect("clear")
    );
    assert_eq!(floor_of(&mut conn, &dag.name).await, Some(second));
}

/// Issue #2602: a set takes a version above the floor of its shard.
///
/// A skewed clock can read below the floor. A set at or below it would make
/// a later carry of the new row skip, so the override would be lost.
#[tokio::test]
async fn a_set_takes_a_version_above_the_floor() {
    let shards = setup_shards().await;
    let dag = code_dag("future_floor_dag");
    let mut conn = shards.conn(OLD).await;
    let ahead = Utc::now() + chrono::Duration::hours(1);
    put_floor(&mut conn, &dag.name, ahead).await;
    let floor = floor_of(&mut conn, &dag.name).await.expect("a floor");

    update_dag_override(&mut conn, &dag, &every_minute(), "ops", None)
        .await
        .expect("set");

    let set = version(&mut conn, &dag.name).await.expect("a version");
    assert!(set > floor, "{set} > {floor}");
}

/// Issue #2602: the floor migration deletes each #2413 delivery receipt.
///
/// A receipt at or above the live row of its shard is a delivery receipt.
/// Each other receipt is a version that the shard cleared or replaced.
#[tokio::test]
async fn the_floor_migration_keeps_only_retired_versions() {
    let shards = setup_shards().await;
    let mut conn = shards.conn(OLD).await;
    for name in ["live", "yielded", "replaced"] {
        update_dag_override(&mut conn, &code_dag(name), &every_minute(), "ops", None)
            .await
            .expect("set");
    }
    conn.batch_execute("DELETE FROM harvest_schedule_override_receipts")
        .await
        .expect("start with no receipt");
    let at = |name: &str| {
        format!("(SELECT updated_at FROM harvest_schedule_overrides WHERE name = '{name}')")
    };
    conn.batch_execute(&format!(
        "INSERT INTO harvest_schedule_override_receipts (kind, name, row_updated_at) VALUES \
         ('dag', 'live', {live}), \
         ('dag', 'yielded', {yielded} + interval '1 second'), \
         ('dag', 'replaced', {replaced} - interval '1 second'), \
         ('dag', 'cleared', now())",
        live = at("live"),
        yielded = at("yielded"),
        replaced = at("replaced"),
    ))
    .await
    .expect("seed the receipts");
    let up = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/migrations/20261108045837_harvest_schedule_override_floor/up.sql"
    ))
    .expect("read the migration");

    conn.batch_execute(&up).await.expect("run the migration");

    for (name, kept) in [
        ("live", false),
        ("yielded", false),
        ("replaced", true),
        ("cleared", true),
    ] {
        assert_eq!(floor_of(&mut conn, name).await.is_some(), kept, "{name}");
    }
}
