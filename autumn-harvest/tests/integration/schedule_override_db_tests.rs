#![cfg(feature = "db")]
//! Operator overrides for code-declared DAG schedules (issue #2385).
//!
//! A DAG schedule is declared in code. Before #2385 the scheduler converged
//! each row to that code value on every tick, so no runtime change lasted.
//! These tests drive the shipped tick and API helpers against a real
//! Postgres. Each test maps to one acceptance criterion of #2385.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! these against a shared local cluster. Otherwise each test starts a fresh
//! testcontainers Postgres 16. On a shared database each test scrubs first.

use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::policy::{OverlapPolicy, Schedule, WorkflowSchedule};
use autumn_harvest::schedule_override::{
    ScheduleOverridePatch, clear_dag_override, get_dag_override, update_dag_override,
};
use autumn_harvest::scheduler::{
    DagCatalog, RegisteredDag, SchedulerMonitor, ensure_dag_schedule, tick_once,
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

/// The code value of a unified DAG, as `compile_dag_catalog` builds it.
fn code_dag(name: &str, schedule: Option<Schedule>) -> RegisteredDag {
    RegisteredDag {
        name: name.to_string(),
        module: "tests".to_string(),
        schedule,
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
fn lowered(dag: &RegisteredDag) -> Option<WorkflowSchedule> {
    let schedule = dag.schedule.clone()?;
    let mut ws = WorkflowSchedule::new(dag.name.clone(), schedule);
    ws.dag_name = Some(dag.name.clone());
    ws.catchup = dag.catchup;
    ws.max_active_runs = dag.max_active_runs;
    ws.jitter = dag.jitter;
    ws.overlap_policy = dag.overlap_policy;
    ws.buffer_all_max = dag.buffer_all_max;
    Some(ws)
}

/// One scheduler tick over freshly built code values.
///
/// The catalog is rebuilt from `dag` on every call, as a restart does. So a
/// value that survives a call survives a restart.
async fn tick(url: &str, dag: &RegisteredDag) {
    let mut catalog = DagCatalog::new();
    catalog.insert(dag.name.clone(), dag.clone());
    let schedules: Vec<WorkflowSchedule> = lowered(dag).into_iter().collect();
    tick_once(
        build_pool(url),
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(catalog),
        Arc::new(schedules),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("tick");
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Row {
    schedule_expr: Option<String>,
    max_active_runs: i32,
    next_run_at: Option<DateTime<Utc>>,
    updated_at: DateTime<Utc>,
    workflow_name: Option<String>,
}

async fn dag_row(conn: &mut AsyncPgConnection, dag_name: &str) -> Row {
    use autumn_harvest::schema::harvest_schedules::dsl;
    let (schedule_expr, max_active_runs, next_run_at, updated_at, workflow_name) =
        dsl::harvest_schedules
            .filter(dsl::dag_name.eq(dag_name))
            .select((
                dsl::schedule_expr,
                dsl::max_active_runs,
                dsl::next_run_at,
                dsl::updated_at,
                dsl::workflow_name,
            ))
            .first::<(
                Option<String>,
                i32,
                Option<DateTime<Utc>>,
                DateTime<Utc>,
                Option<String>,
            )>(conn)
            .await
            .expect("dag row");
    Row {
        schedule_expr,
        max_active_runs,
        next_run_at,
        updated_at,
        workflow_name,
    }
}

fn set_schedule(schedule: Schedule) -> ScheduleOverridePatch {
    ScheduleOverridePatch {
        schedule: Some(Some(schedule)),
        ..ScheduleOverridePatch::default()
    }
}

/// AC1, AC2, AC3: an override wins, the next tick does not rewrite it, and
/// a rebuilt catalog (a restart) keeps it.
#[tokio::test]
async fn an_override_wins_over_code_and_survives_a_restart() {
    let (mut conn, db) = setup_db().await;
    let code = code_dag("etl", Some(hourly()));

    tick(&db.url, &code).await;
    assert_eq!(
        dag_row(&mut conn, "etl").await.schedule_expr.as_deref(),
        Some("interval:3600")
    );

    update_dag_override(
        &mut conn,
        &code,
        &set_schedule(minutely()),
        "ops",
        Some("incident"),
    )
    .await
    .expect("set override");
    let before = Utc::now();
    tick(&db.url, &code).await;
    let row = dag_row(&mut conn, "etl").await;
    assert_eq!(row.schedule_expr.as_deref(), Some("interval:60"));
    let next = row.next_run_at.expect("next_run_at");
    assert!(
        next > before && next <= before + chrono::Duration::seconds(61),
        "next_run_at {next} must be re-anchored inside the new 60 s cadence"
    );

    // AC2: the row is converged to the effective value. Neither registration
    // pass writes it again.
    tick(&db.url, &code).await;
    let again = dag_row(&mut conn, "etl").await;
    assert_eq!(again, row, "a converged override row must not be rewritten");

    // AC3: `tick` rebuilds the catalog from the unchanged code value, as a
    // restart does. The override still holds.
    tick(&db.url, &code_dag("etl", Some(hourly()))).await;
    assert_eq!(
        dag_row(&mut conn, "etl").await.schedule_expr.as_deref(),
        Some("interval:60")
    );
}

/// AC4: clearing the override returns the row to its code value.
#[tokio::test]
async fn clearing_the_override_restores_the_code_value() {
    let (mut conn, db) = setup_db().await;
    let code = code_dag("etl", Some(hourly()));
    update_dag_override(&mut conn, &code, &set_schedule(minutely()), "ops", None)
        .await
        .expect("set override");
    tick(&db.url, &code).await;
    assert_eq!(
        dag_row(&mut conn, "etl").await.schedule_expr.as_deref(),
        Some("interval:60")
    );

    assert!(clear_dag_override(&mut conn, "etl").await.expect("clear"));
    tick(&db.url, &code).await;
    assert_eq!(
        dag_row(&mut conn, "etl").await.schedule_expr.as_deref(),
        Some("interval:3600")
    );
    assert!(
        !clear_dag_override(&mut conn, "etl")
            .await
            .expect("clear twice")
    );
}

/// A field set to null returns to code. A patch that leaves no field
/// deletes the row.
#[tokio::test]
async fn a_patch_merges_field_by_field() {
    let (mut conn, db) = setup_db().await;
    let code = code_dag("etl", Some(hourly()));
    update_dag_override(&mut conn, &code, &set_schedule(minutely()), "ops", None)
        .await
        .expect("set schedule");
    let runs = ScheduleOverridePatch {
        max_active_runs: Some(Some(4)),
        ..ScheduleOverridePatch::default()
    };
    update_dag_override(&mut conn, &code, &runs, "ops", None)
        .await
        .expect("set runs");
    tick(&db.url, &code).await;
    let row = dag_row(&mut conn, "etl").await;
    assert_eq!(row.schedule_expr.as_deref(), Some("interval:60"));
    assert_eq!(row.max_active_runs, 4);

    let revert_schedule = ScheduleOverridePatch {
        schedule: Some(None),
        ..ScheduleOverridePatch::default()
    };
    update_dag_override(&mut conn, &code, &revert_schedule, "ops", None)
        .await
        .expect("revert schedule");
    tick(&db.url, &code).await;
    let row = dag_row(&mut conn, "etl").await;
    assert_eq!(row.schedule_expr.as_deref(), Some("interval:3600"));
    assert_eq!(row.max_active_runs, 4);

    let revert_runs = ScheduleOverridePatch {
        max_active_runs: Some(None),
        ..ScheduleOverridePatch::default()
    };
    let update = update_dag_override(&mut conn, &code, &revert_runs, "ops", None)
        .await
        .expect("revert runs");
    assert!(update.stored.is_none(), "an empty override is not stored");
    assert!(
        get_dag_override(&mut conn, "etl")
            .await
            .expect("get")
            .is_none()
    );
}

/// AC5: an invalid override is rejected and writes nothing.
#[tokio::test]
async fn an_invalid_override_writes_nothing() {
    let (mut conn, _db) = setup_db().await;
    let code = code_dag("etl", Some(hourly()));
    let error = update_dag_override(
        &mut conn,
        &code,
        &set_schedule(Schedule::Interval(Duration::ZERO)),
        "ops",
        None,
    )
    .await
    .expect_err("a zero interval must be rejected");
    assert!(
        matches!(error, autumn_harvest::HarvestError::Config(_)),
        "{error:?}"
    );
    assert!(
        get_dag_override(&mut conn, "etl")
            .await
            .expect("get")
            .is_none()
    );

    // A valid override, then an invalid change on top of it: the stored
    // override is unchanged.
    update_dag_override(&mut conn, &code, &set_schedule(minutely()), "ops", None)
        .await
        .expect("set override");
    let bad_tz = set_schedule(Schedule::CronInTimezone {
        expr: "0 2 * * *".to_string(),
        tz: "Mars/Olympus".to_string(),
    });
    update_dag_override(&mut conn, &code, &bad_tz, "ops", None)
        .await
        .expect_err("an unknown timezone must be rejected");
    let stored = get_dag_override(&mut conn, "etl")
        .await
        .expect("get")
        .expect("stored");
    assert_eq!(stored.fields.schedule, Some(minutely()));
}

/// AC8: an unreadable override table stops the pass from writing any row.
#[tokio::test]
async fn an_unreadable_override_skips_the_pass() {
    let (mut conn, db) = setup_db().await;
    let code = code_dag("etl", Some(hourly()));
    tick(&db.url, &code).await;
    let before = dag_row(&mut conn, "etl").await;

    // A value that no writer stores. The decode fails, so the pass must not
    // fall back to the code value below.
    diesel::sql_query(
        "INSERT INTO harvest_schedule_overrides (kind, name, schedule_expr, updated_by) \
         VALUES ('dag', 'etl', 'bogus', 'test')",
    )
    .execute(&mut conn)
    .await
    .expect("insert corrupt override");

    // A code change that the pass would otherwise write.
    tick(&db.url, &code_dag("etl", Some(minutely()))).await;
    assert_eq!(
        dag_row(&mut conn, "etl").await,
        before,
        "a pass with unreadable overrides must write no schedule row"
    );
}

/// The API path converges to the effective value, not the code value.
#[tokio::test]
async fn ensure_dag_schedule_applies_the_override() {
    let (mut conn, db) = setup_db().await;
    let code = code_dag("etl", Some(hourly()));
    tick(&db.url, &code).await;
    update_dag_override(&mut conn, &code, &set_schedule(minutely()), "ops", None)
        .await
        .expect("set override");

    let row = ensure_dag_schedule(&mut conn, &code).await.expect("ensure");
    assert_eq!(row.schedule_expr.as_deref(), Some("interval:60"));
    let after_ensure = dag_row(&mut conn, "etl").await;

    // The tick agrees with the API path, so it does not rewrite the row.
    tick(&db.url, &code).await;
    assert_eq!(dag_row(&mut conn, "etl").await, after_ensure);
}

/// An override can schedule a unified DAG that declares no schedule.
#[tokio::test]
async fn an_override_can_schedule_an_unscheduled_dag() {
    let (mut conn, db) = setup_db().await;
    let code = code_dag("adhoc", None);
    update_dag_override(&mut conn, &code, &set_schedule(minutely()), "ops", None)
        .await
        .expect("set override");
    tick(&db.url, &code).await;
    let row = dag_row(&mut conn, "adhoc").await;
    assert_eq!(row.schedule_expr.as_deref(), Some("interval:60"));
    assert_eq!(
        row.workflow_name.as_deref(),
        Some("adhoc"),
        "the lowered workflow schedule must claim the row, or it never fires"
    );
    assert!(row.next_run_at.is_some());

    // A second tick finds the row converged.
    tick(&db.url, &code).await;
    assert_eq!(dag_row(&mut conn, "adhoc").await, row);
}
