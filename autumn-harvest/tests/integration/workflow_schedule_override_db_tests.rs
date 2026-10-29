#![cfg(feature = "db")]
//! Operator overrides for code-declared workflow schedules (issue #2386).
//!
//! `WorkerBuilder::workflow_schedule` declares a schedule in code. The
//! scheduler converges its row to that code value on every tick. Before
//! #2386 an in-place edit of that row through `PATCH /schedules/{id}` lasted
//! one tick, and the API still reported success. Each test maps to one
//! acceptance criterion of #2386.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! these against a shared local cluster. Otherwise each test starts a fresh
//! testcontainers Postgres 16. On a shared database each test scrubs first.

use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::policy::{Schedule, WorkflowSchedule};
use autumn_harvest::schedule_override::get_workflow_override;
use autumn_harvest::scheduler::{
    DagCatalog, ScheduleUpdateOutcome, SchedulerMonitor, WorkflowOverrideRequest,
    WorkflowOverrideUpdate, WorkflowSchedulePatch, tick_once,
    update_code_declared_workflow_schedule,
};
use autumn_harvest::worker::HandlerRegistry;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

struct TestDb {
    url: String,
    _container: Option<ContainerAsync<Postgres>>,
}

async fn setup_db() -> (AsyncPgConnection, TestDb) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
        scrub(&mut conn).await;
        return (
            conn,
            TestDb {
                url,
                _container: None,
            },
        );
    }
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("migration");
    (
        conn,
        TestDb {
            url,
            _container: Some(container),
        },
    )
}

async fn scrub(conn: &mut AsyncPgConnection) {
    for stmt in [
        "DELETE FROM harvest_schedule_overrides",
        "DELETE FROM harvest_schedule_decisions",
        "DELETE FROM harvest_schedules",
        "DELETE FROM harvest_workflow_executions",
    ] {
        diesel::sql_query(stmt).execute(conn).await.expect(stmt);
    }
}

fn build_pool(url: &str) -> autumn_harvest::worker::DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("pool")
}

const fn hourly() -> Schedule {
    Schedule::Interval(Duration::from_secs(3600))
}

const fn minutely() -> Schedule {
    Schedule::Interval(Duration::from_secs(60))
}

/// The code value of a workflow schedule, as `WorkerBuilder` declares it.
fn code_schedule(name: &str, schedule: Schedule) -> WorkflowSchedule {
    let mut ws = WorkflowSchedule::new(name.to_string(), schedule);
    ws.max_active_runs = 1;
    ws
}

/// One scheduler tick over freshly built code values.
///
/// The schedule list is rebuilt on every call, as a restart does. So a value
/// that survives a call survives a restart.
async fn tick(url: &str, code: &[WorkflowSchedule]) {
    tick_once(
        build_pool(url),
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::new()),
        Arc::new(code.to_vec()),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("tick");
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Row {
    id: uuid::Uuid,
    schedule_expr: Option<String>,
    max_active_runs: i32,
    next_run_at: Option<DateTime<Utc>>,
}

async fn workflow_row(conn: &mut AsyncPgConnection, workflow_name: &str) -> Row {
    use autumn_harvest::schema::harvest_schedules::dsl;
    let (id, schedule_expr, max_active_runs, next_run_at) = dsl::harvest_schedules
        .filter(dsl::workflow_name.eq(workflow_name))
        .select((
            dsl::id,
            dsl::schedule_expr,
            dsl::max_active_runs,
            dsl::next_run_at,
        ))
        .first::<(uuid::Uuid, Option<String>, i32, Option<DateTime<Utc>>)>(conn)
        .await
        .expect("workflow row");
    Row {
        id,
        schedule_expr,
        max_active_runs,
        next_run_at,
    }
}

fn set_cadence(schedule: Schedule) -> WorkflowSchedulePatch {
    WorkflowSchedulePatch {
        schedule: Some(schedule),
        ..WorkflowSchedulePatch::default()
    }
}

/// The operator path of `PATCH /schedules/{id}` for a code-declared schedule.
async fn patch(
    conn: &mut AsyncPgConnection,
    id: uuid::Uuid,
    code: &WorkflowSchedule,
    patch: &WorkflowSchedulePatch,
    clear: bool,
) -> autumn_harvest::HarvestResult<WorkflowOverrideUpdate> {
    let request = WorkflowOverrideRequest {
        schedule_id: id,
        code,
        patch,
        clear,
        actor: "ops",
        reason: Some("incident"),
    };
    update_code_declared_workflow_schedule(conn, &request, &PayloadCodecs::default()).await
}

async fn override_rows(conn: &mut AsyncPgConnection) -> i64 {
    use autumn_harvest::schema::harvest_schedule_overrides::dsl;
    dsl::harvest_schedule_overrides
        .count()
        .get_result(conn)
        .await
        .expect("count overrides")
}

/// AC1: a patched code-declared schedule holds its cadence for three ticks.
///
/// Before #2386 the API edited the row in place, and the next tick
/// converged it back to `interval:3600`.
#[tokio::test]
async fn a_patched_code_declared_schedule_holds_its_cadence() {
    let (mut conn, db) = setup_db().await;
    let code = vec![code_schedule("nightly", hourly())];
    tick(&db.url, &code).await;
    let id = workflow_row(&mut conn, "nightly").await.id;

    let update = patch(&mut conn, id, &code[0], &set_cadence(minutely()), false)
        .await
        .expect("patch");
    let ScheduleUpdateOutcome::Updated(row) = update.outcome else {
        panic!("expected an updated row, got {:?}", update.outcome);
    };
    assert_eq!(row.schedule_expr.as_deref(), Some("interval:60"));
    let stored = update.stored.expect("an override row");
    assert_eq!(stored.fields.schedule, Some(minutely()));
    assert_eq!(stored.reason.as_deref(), Some("incident"));
    let after_patch = workflow_row(&mut conn, "nightly").await;

    for _ in 0..3 {
        tick(&db.url, &code).await;
    }
    assert_eq!(
        workflow_row(&mut conn, "nightly").await,
        after_patch,
        "the tick must leave the patched row as the patch wrote it"
    );
}

/// AC2: the override survives a restart, and a code change still reaches
/// the fields that the override does not set.
#[tokio::test]
async fn the_override_survives_a_restart_with_new_code() {
    let (mut conn, db) = setup_db().await;
    let code = code_schedule("nightly", hourly());
    tick(&db.url, std::slice::from_ref(&code)).await;
    let id = workflow_row(&mut conn, "nightly").await.id;
    patch(&mut conn, id, &code, &set_cadence(minutely()), false)
        .await
        .expect("patch");

    // A restart with new code: a new cadence and a new run cap.
    let mut redeployed = code_schedule("nightly", Schedule::Interval(Duration::from_secs(7200)));
    redeployed.max_active_runs = 3;
    tick(&db.url, std::slice::from_ref(&redeployed)).await;
    let row = workflow_row(&mut conn, "nightly").await;
    assert_eq!(row.schedule_expr.as_deref(), Some("interval:60"));
    assert_eq!(
        row.max_active_runs, 3,
        "a field with no override follows code"
    );
}

/// AC3: clearing the override restores the code value on the next tick.
#[tokio::test]
async fn clearing_the_override_restores_the_code_value() {
    let (mut conn, db) = setup_db().await;
    let code = code_schedule("nightly", hourly());
    tick(&db.url, std::slice::from_ref(&code)).await;
    let id = workflow_row(&mut conn, "nightly").await.id;
    patch(&mut conn, id, &code, &set_cadence(minutely()), false)
        .await
        .expect("patch");

    let cleared = patch(
        &mut conn,
        id,
        &code,
        &WorkflowSchedulePatch::default(),
        true,
    )
    .await
    .expect("clear");
    assert!(cleared.stored.is_none());
    assert_eq!(override_rows(&mut conn).await, 0);
    assert_eq!(
        workflow_row(&mut conn, "nightly")
            .await
            .schedule_expr
            .as_deref(),
        Some("interval:3600"),
        "the clear converges the row at once"
    );
    tick(&db.url, std::slice::from_ref(&code)).await;
    assert_eq!(
        workflow_row(&mut conn, "nightly")
            .await
            .schedule_expr
            .as_deref(),
        Some("interval:3600")
    );
}

/// A second patch merges over the first, field by field.
#[tokio::test]
async fn a_second_patch_merges_over_the_first() {
    let (mut conn, db) = setup_db().await;
    let code = code_schedule("nightly", hourly());
    tick(&db.url, std::slice::from_ref(&code)).await;
    let id = workflow_row(&mut conn, "nightly").await.id;
    patch(&mut conn, id, &code, &set_cadence(minutely()), false)
        .await
        .expect("first patch");
    let second = WorkflowSchedulePatch {
        max_active_runs: Some(4),
        calendar: Some(None),
        ..WorkflowSchedulePatch::default()
    };
    let update = patch(&mut conn, id, &code, &second, false)
        .await
        .expect("second patch");
    let stored = update.stored.expect("an override row");
    assert_eq!(stored.fields.schedule, Some(minutely()));
    assert_eq!(stored.fields.max_active_runs, Some(4));
    assert_eq!(stored.fields.calendar, Some(None), "a JSON null is kept");

    // The stored form reads back the same, through the scheduler's loader.
    let reread = get_workflow_override(&mut conn, "nightly")
        .await
        .expect("read")
        .expect("row");
    assert_eq!(reread.fields.calendar, Some(None));
    tick(&db.url, std::slice::from_ref(&code)).await;
    let row = workflow_row(&mut conn, "nightly").await;
    assert_eq!(row.schedule_expr.as_deref(), Some("interval:60"));
    assert_eq!(row.max_active_runs, 4);
}

/// AC5: a request whose merged value fails validation writes nothing.
#[tokio::test]
async fn an_invalid_override_writes_nothing() {
    let (mut conn, db) = setup_db().await;
    let code = code_schedule("nightly", hourly());
    tick(&db.url, std::slice::from_ref(&code)).await;
    let before = workflow_row(&mut conn, "nightly").await;

    for bad in [
        set_cadence(Schedule::Interval(Duration::ZERO)),
        WorkflowSchedulePatch {
            input: Some(serde_json::json!({"env": "B"})),
            ..WorkflowSchedulePatch::default()
        },
        // A jitter must be shorter than the interval that holds it.
        WorkflowSchedulePatch {
            jitter: Some(Duration::from_secs(7200)),
            ..WorkflowSchedulePatch::default()
        },
    ] {
        let error = patch(&mut conn, before.id, &code, &bad, false)
            .await
            .expect_err("an invalid patch must fail");
        assert!(
            matches!(error, autumn_harvest::HarvestError::Config(_)),
            "{error}"
        );
    }
    assert_eq!(override_rows(&mut conn).await, 0);
    assert_eq!(workflow_row(&mut conn, "nightly").await, before);
}

/// A live fire claim refuses the patch, and the override is not stored.
#[tokio::test]
async fn a_live_fire_claim_writes_nothing() {
    let (mut conn, db) = setup_db().await;
    let code = code_schedule("nightly", hourly());
    tick(&db.url, std::slice::from_ref(&code)).await;
    let before = workflow_row(&mut conn, "nightly").await;
    diesel::sql_query(
        "UPDATE harvest_schedules SET fire_claim_token = gen_random_uuid(), \
         fire_claimed_until = NOW() + INTERVAL '30 seconds'",
    )
    .execute(&mut conn)
    .await
    .expect("claim");

    let update = patch(&mut conn, before.id, &code, &set_cadence(minutely()), false)
        .await
        .expect("patch");
    assert!(matches!(update.outcome, ScheduleUpdateOutcome::ClaimLive));
    assert!(update.stored.is_none());
    assert_eq!(override_rows(&mut conn).await, 0);
    assert_eq!(workflow_row(&mut conn, "nightly").await, before);
}

/// A pass that cannot read a workflow override skips its shard.
#[tokio::test]
async fn an_unreadable_workflow_override_skips_the_pass() {
    let (mut conn, db) = setup_db().await;
    let code = code_schedule("nightly", hourly());
    tick(&db.url, std::slice::from_ref(&code)).await;
    let before = workflow_row(&mut conn, "nightly").await;
    diesel::sql_query(
        "INSERT INTO harvest_schedule_overrides (kind, name, workflow_fields, updated_by) \
         VALUES ('workflow', 'nightly', '{\"queue_name\": 7}', 'test')",
    )
    .execute(&mut conn)
    .await
    .expect("insert corrupt override");

    tick(&db.url, &[code_schedule("nightly", minutely())]).await;
    assert_eq!(
        workflow_row(&mut conn, "nightly").await,
        before,
        "a pass with unreadable overrides must write no schedule row"
    );
}

/// A DAG row is not a workflow override target.
#[tokio::test]
async fn the_database_refuses_workflow_fields_on_a_dag_row() {
    let (mut conn, _db) = setup_db().await;
    let result = diesel::sql_query(
        "INSERT INTO harvest_schedule_overrides (kind, name, workflow_fields, updated_by) \
         VALUES ('dag', 'etl', '{\"queue_name\": \"x\"}', 'test')",
    )
    .execute(&mut conn)
    .await;
    assert!(
        result.is_err(),
        "the check must refuse workflow_fields on a dag row"
    );
}
