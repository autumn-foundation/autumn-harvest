#![cfg(feature = "db")]
//! The shared schedule-override cases on Postgres (issue #2390).
//!
//! `autumn-harvest-sqlite` runs the same cases on SQLite. Both suites must
//! reach the same effective value after every step, so a drift in the
//! override rules between the two backends fails one of them.
//!
//! Each `Step::Code` runs one scheduler tick over freshly built code values,
//! as a restart does. A patch and a clear take the operator path of
//! `PATCH /admin/schedules/{id}` for a code-declared schedule.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly. Otherwise a fresh testcontainers Postgres starts. On
//! a shared database each case scrubs first.

use std::sync::Arc;

use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::policy::WorkflowSchedule;
use autumn_harvest::scheduler::{
    DagCatalog, SchedulerMonitor, WorkflowOverrideRequest, WorkflowSchedulePatch, tick_once,
    update_code_declared_workflow_schedule,
};
use autumn_harvest::worker::HandlerRegistry;
use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

#[path = "../../../autumn-harvest-sqlite/tests/integration/schedule_override_fixture.rs"]
mod schedule_override_fixture;

use schedule_override_fixture::{Expect, SCHEDULED_WORKFLOW, Step, cases};

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("migration");
    (url, Some(container))
}

async fn scrub(conn: &mut AsyncPgConnection) {
    for stmt in [
        "DELETE FROM harvest_schedule_overrides",
        "DELETE FROM harvest_schedule_decisions",
        "DELETE FROM harvest_schedules",
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

/// One scheduler tick over `code`, as a restart does.
async fn tick(url: &str, code: &WorkflowSchedule) {
    tick_once(
        build_pool(url),
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::new()),
        Arc::new(vec![code.clone()]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("tick");
}

/// The id and the effective value of the schedule row.
async fn observed(conn: &mut AsyncPgConnection) -> (uuid::Uuid, Expect) {
    use autumn_harvest::schema::harvest_schedules::dsl;
    let (id, expr, max_active_runs) = dsl::harvest_schedules
        .filter(dsl::workflow_name.eq(SCHEDULED_WORKFLOW))
        .select((dsl::id, dsl::schedule_expr, dsl::max_active_runs))
        .first::<(uuid::Uuid, Option<String>, i32)>(conn)
        .await
        .expect("the schedule row");
    let interval_secs = expr
        .as_deref()
        .and_then(|e| e.strip_prefix("interval:"))
        .and_then(|secs| secs.parse().ok())
        .unwrap_or_else(|| panic!("the cases use interval cadences: {expr:?}"));
    let max_active_runs = u32::try_from(max_active_runs).expect("a run limit");
    (
        id,
        Expect::Effective {
            interval_secs,
            max_active_runs,
        },
    )
}

/// The operator path of `PATCH /admin/schedules/{id}`.
async fn patch(
    conn: &mut AsyncPgConnection,
    code: &WorkflowSchedule,
    patch: &WorkflowSchedulePatch,
    clear: bool,
) -> autumn_harvest::HarvestResult<()> {
    let (schedule_id, _) = observed(conn).await;
    let request = WorkflowOverrideRequest {
        schedule_id,
        code,
        patch,
        clear,
        actor: "ops",
        reason: Some("incident"),
    };
    update_code_declared_workflow_schedule(conn, &request, &PayloadCodecs::default())
        .await
        .map(drop)
}

#[tokio::test]
async fn the_shared_override_cases_hold_on_postgres() {
    let (url, _container) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    for case in cases() {
        scrub(&mut conn).await;
        let mut current = None;
        let mut last = None;
        for (index, (step, expect)) in case.steps.into_iter().enumerate() {
            let result = match step {
                Step::Code(code) => {
                    tick(&url, &code).await;
                    current = Some(code);
                    Ok(())
                }
                Step::Patch(fields) => {
                    let code = current.as_ref().expect("a code step comes first");
                    patch(&mut conn, code, &fields, false).await
                }
                Step::Clear => {
                    let code = current.as_ref().expect("a code step comes first");
                    patch(&mut conn, code, &WorkflowSchedulePatch::default(), true).await
                }
            };
            let (_, now) = observed(&mut conn).await;
            match expect {
                Expect::Refused => {
                    assert!(
                        result.is_err(),
                        "{} step {index}: must be refused",
                        case.name
                    );
                    assert_eq!(
                        Some(now),
                        last,
                        "{} step {index}: a refused step writes nothing",
                        case.name
                    );
                }
                Expect::Effective { .. } => {
                    result.unwrap_or_else(|e| panic!("{} step {index}: {e}", case.name));
                    assert_eq!(now, expect, "{} step {index}", case.name);
                    last = Some(expect);
                }
            }
        }
    }
}
