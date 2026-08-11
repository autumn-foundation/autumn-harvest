#![cfg(feature = "db")]
//! Schedule-reconciler registration tests — issue #1157 ("DAG Storm").
//!
//! A `harvest_schedules` row holding a registered DAG's `workflow_name` while
//! carrying a *different* non-NULL `dag_name` was invisible to the reconciler's
//! resolver. The reconciler then tried to move that `workflow_name` onto a
//! different row once per second, forever, failing
//! `harvest_schedules_workflow_name_unique` every time — with no backoff, no
//! memory of the previous failure, and the first failing DAG aborting the rest
//! of that tick's registration.
//!
//! These tests drive the shipped registration entry points against a real
//! Postgres and cover the four defects the report names:
//!
//! * **Defect 1** — the resolver's `dag_name IS NULL` blind spot (UPDATE form).
//! * **Defect 1b** — the same blind spot in INSERT form, no concurrency needed.
//! * **Defect 2** — one unconvergeable schedule starving every schedule after
//!   it in registration order.
//! * **Defect 3** — unconditional re-registration writes on every tick.
//! * **Defect 4** — every process reconciling, with no leader election.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! these against a shared local cluster (Docker-free); otherwise a fresh
//! testcontainers Postgres 16 is started per test. On a shared DB each test
//! `scrub()`s first for isolation.

use std::sync::Arc;

use autumn_harvest::policy::{Schedule, WorkflowSchedule};
use autumn_harvest::scheduler::{
    DagCatalog, SchedulerMonitor, register_workflow_schedules, tick_once,
};
use autumn_harvest::schema::harvest_schedules;
use autumn_harvest::worker::HandlerRegistry;
use chrono::{DateTime, SubsecRound, Utc};
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
use uuid::Uuid;

// ── Harness ─────────────────────────────────────────────────────────────────

/// A migrated database URL plus the container keeping it alive (when Docker
/// was used). Held by the caller so the container outlives the connections.
struct TestDb {
    url: String,
    _container: Option<ContainerAsync<Postgres>>,
}

async fn setup_db() -> (AsyncPgConnection, TestDb) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        // Assumed pre-migrated (see module doc); scrub per test isolates it.
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
    conn.batch_execute(autumn_harvest::full_migrations_sql())
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

/// Clear schedules + executions so a shared migrated DB stays per-test isolated.
async fn scrub(conn: &mut AsyncPgConnection) {
    for stmt in [
        "DELETE FROM harvest_schedule_decisions",
        "DELETE FROM harvest_schedules",
        "DELETE FROM harvest_workflow_executions",
    ] {
        diesel::sql_query(stmt).execute(conn).await.expect(stmt);
    }
}

/// Insert a raw `harvest_schedules` row with an arbitrary
/// (`dag_name`, `workflow_name`) pair — including the internally inconsistent
/// pair the incident report reproduces, which no registration path can produce.
async fn insert_raw_row(
    conn: &mut AsyncPgConnection,
    dag_name: Option<&str>,
    workflow_name: Option<&str>,
    schedule_expr: &str,
) -> Uuid {
    use autumn_harvest::schema::harvest_schedules::dsl;
    let id = Uuid::new_v4();
    diesel::insert_into(harvest_schedules::table)
        .values((
            dsl::id.eq(id),
            dsl::dag_name.eq(dag_name),
            dsl::workflow_name.eq(workflow_name),
            dsl::schedule_expr.eq(schedule_expr),
            dsl::timezone.eq("UTC"),
            dsl::catchup.eq(false),
            dsl::max_active_runs.eq(1),
            dsl::is_paused.eq(false),
            dsl::jitter_secs.eq(0_i64),
            dsl::overlap_policy.eq("skip"),
            dsl::buffered_runs.eq(serde_json::json!([])),
            dsl::buffer_all_max.eq(100),
            dsl::skip_policy.eq("skip"),
        ))
        .execute(conn)
        .await
        .expect("insert raw schedule row");
    id
}

/// The `WorkflowSchedule` a unified DAG derives — `dag_name == workflow_name`,
/// exactly what `DagInfo::as_workflow_schedule` produces.
fn dag_schedule(name: &str) -> WorkflowSchedule {
    let mut ws = WorkflowSchedule::new(name, Schedule::Cron("0 1 * * *".to_string()));
    ws.dag_name = Some(name.to_string());
    ws
}

async fn row_by_id(conn: &mut AsyncPgConnection, id: Uuid) -> (Option<String>, Option<String>) {
    use autumn_harvest::schema::harvest_schedules::dsl;
    dsl::harvest_schedules
        .find(id)
        .select((dsl::dag_name, dsl::workflow_name))
        .first::<(Option<String>, Option<String>)>(conn)
        .await
        .expect("row must still exist")
}

async fn row_for_workflow(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
) -> Option<(Uuid, Option<String>)> {
    use autumn_harvest::schema::harvest_schedules::dsl;
    dsl::harvest_schedules
        .filter(dsl::workflow_name.eq(workflow_name))
        .select((dsl::id, dsl::dag_name))
        .first::<(Uuid, Option<String>)>(conn)
        .await
        .optional()
        .expect("query workflow row")
}

async fn schedule_expr_of(conn: &mut AsyncPgConnection, id: Uuid) -> Option<String> {
    use autumn_harvest::schema::harvest_schedules::dsl;
    dsl::harvest_schedules
        .find(id)
        .select(dsl::schedule_expr)
        .first::<Option<String>>(conn)
        .await
        .expect("schedule_expr")
}

async fn updated_at_of(conn: &mut AsyncPgConnection, id: Uuid) -> DateTime<Utc> {
    use autumn_harvest::schema::harvest_schedules::dsl;
    dsl::harvest_schedules
        .find(id)
        .select(dsl::updated_at)
        .first::<DateTime<Utc>>(conn)
        .await
        .expect("updated_at")
}

async fn schedule_count(conn: &mut AsyncPgConnection) -> i64 {
    use autumn_harvest::schema::harvest_schedules::dsl;
    dsl::harvest_schedules
        .count()
        .get_result(conn)
        .await
        .expect("count")
}

// ── Defect 1 — the resolver's blind spot (UPDATE form) ──────────────────────

#[tokio::test]
async fn a_squatting_row_no_longer_wedges_the_reconciler() {
    let (mut conn, _db) = setup_db().await;

    // The incident's reproduction, verbatim: hand one DAG's workflow_name to a
    // row that is not its DAG row.
    let dag_row = insert_raw_row(&mut conn, Some("my_dag"), None, "cron:0 1 * * *").await;
    let squatter = insert_raw_row(
        &mut conn,
        Some("some_other_dag"),
        Some("my_dag"),
        "cron:0 2 * * *",
    )
    .await;

    // Pre-fix this fails with
    //   duplicate key value violates unique constraint
    //   "harvest_schedules_workflow_name_unique"
    // and keeps failing indefinitely.
    register_workflow_schedules(&mut conn, &[dag_schedule("my_dag")])
        .await
        .expect("registration must converge, not raise a unique violation");

    // The DAG row now owns its name...
    assert_eq!(
        row_by_id(&mut conn, dag_row).await,
        (Some("my_dag".to_string()), Some("my_dag".to_string())),
    );
    // ...and the squatter released the name it never legitimately held, while
    // KEEPING its own identity, so `some_other_dag`'s own registration can
    // re-stamp it. Releasing, not deleting, preserves that row's pause state
    // and counters.
    assert_eq!(
        row_by_id(&mut conn, squatter).await,
        (Some("some_other_dag".to_string()), None),
    );

    // Idempotent: a second pass over the repaired table is a clean no-op.
    register_workflow_schedules(&mut conn, &[dag_schedule("my_dag")])
        .await
        .expect("second pass must also converge");
}

// ── Defect 1b — the same blind spot in INSERT form ──────────────────────────

#[tokio::test]
async fn a_squatted_name_with_no_dag_row_no_longer_fails_the_insert() {
    let (mut conn, _db) = setup_db().await;

    // No `dag_name = 'my_dag'` row at all — the resolver returns (None, None)
    // and falls through to an INSERT carrying BOTH dag_name and workflow_name
    // under `ON CONFLICT (dag_name) DO NOTHING`, whose arbiter offers no
    // protection against the workflow_name unique index. Single session, no
    // race.
    let squatter = insert_raw_row(
        &mut conn,
        Some("some_other_dag"),
        Some("my_dag"),
        "cron:0 2 * * *",
    )
    .await;

    register_workflow_schedules(&mut conn, &[dag_schedule("my_dag")])
        .await
        .expect("insert path must converge, not raise a unique violation");

    let (id, dag) = row_for_workflow(&mut conn, "my_dag")
        .await
        .expect("a row must now own my_dag");
    assert_ne!(id, squatter, "the squatter must not have been hijacked");
    assert_eq!(dag, Some("my_dag".to_string()));
    assert_eq!(
        row_by_id(&mut conn, squatter).await,
        (Some("some_other_dag".to_string()), None),
    );
}

// ── Defect 1 — a genuine collision must NOT become a steal ──────────────────

#[tokio::test]
async fn a_consistent_peer_row_is_a_conflict_not_a_steal() {
    let (mut conn, _db) = setup_db().await;

    // `shared` is a well-formed row for the DAG named `shared`
    // (dag_name == workflow_name — the invariant every registration produces).
    let peer = insert_raw_row(&mut conn, Some("shared"), Some("shared"), "cron:0 3 * * *").await;

    // Now register a DIFFERENT dag that claims the same workflow_name. That is
    // a configuration collision, not corruption. Stealing the name would make
    // the two registrations flap it back and forth at 1 Hz — a new storm.
    let mut colliding = WorkflowSchedule::new("shared", Schedule::Cron("0 4 * * *".to_string()));
    colliding.dag_name = Some("my_dag".to_string());

    let result = register_workflow_schedules(&mut conn, &[colliding]).await;

    // Asserting only `is_err()` would prove nothing: pre-fix this ALSO errored,
    // just opaquely — the resolver missed the peer, fell through to the insert,
    // and the unique index raised `HarvestError::Database("duplicate key ...")`.
    // The distinguishing property is that the refusal is now a *deliberate,
    // typed* decision naming both sides, so an operator can act on it.
    match result {
        Err(autumn_harvest::HarvestError::Config(message)) => {
            assert!(
                message.contains("shared") && message.contains("my_dag"),
                "the refusal must name both schedules so an operator can rename \
                 one; got: {message}"
            );
            assert!(
                !message.contains("duplicate key"),
                "a deliberate refusal, not a leaked constraint violation; got: {message}"
            );
        }
        other => panic!(
            "a genuine name collision must surface as a typed Config refusal, \
             never a silent steal or a raw unique violation; got: {other:?}"
        ),
    }

    // The peer is untouched — no flapping.
    assert_eq!(
        row_by_id(&mut conn, peer).await,
        (Some("shared".to_string()), Some("shared".to_string())),
    );
}

// ── Defect 1 — a decoupled schedule keeps working, and never steals ─────────

/// `WorkflowSchedule` exposes `dag_name`/`workflow_name` as independent public
/// fields and `validate_workflow_schedules` has never required them to agree,
/// so a deployment upgrading into this fix can legitimately hold rows where
/// they differ. Such a row must keep reconciling its own cadence — otherwise
/// the fix strands it, and a row parked by the squatter repair could never
/// re-stamp its own name.
#[tokio::test]
async fn a_decoupled_registration_still_reconciles_its_own_row() {
    let (mut conn, _db) = setup_db().await;

    // A pre-existing decoupled row, exactly as an older version would have
    // persisted it: its dag_name and workflow_name disagree.
    let legacy = insert_raw_row(
        &mut conn,
        Some("owning_dag"),
        Some("wants_this"),
        "cron:0 5 * * *",
    )
    .await;

    // Its own registration must converge it, not refuse it: the cadence change
    // below is exactly what an operator would expect to take effect.
    let mut decoupled =
        WorkflowSchedule::new("wants_this", Schedule::Cron("0 9 * * *".to_string()));
    decoupled.dag_name = Some("owning_dag".to_string());

    register_workflow_schedules(&mut conn, &[decoupled])
        .await
        .expect("a legacy decoupled row must still be reconcilable");

    assert_eq!(
        row_by_id(&mut conn, legacy).await,
        (
            Some("owning_dag".to_string()),
            Some("wants_this".to_string())
        ),
        "reconciling must update the row in place, never strip or duplicate it"
    );
    assert_eq!(
        schedule_expr_of(&mut conn, legacy).await.as_deref(),
        Some("cron:0 9 * * *"),
        "the cadence change must actually reach the row"
    );
    assert_eq!(
        schedule_count(&mut conn).await,
        1,
        "no second row may be inserted alongside the one being reconciled"
    );
}

/// The mirror image: a registrant whose `dag_name` differs from the
/// `workflow_name` it wants has no claim on that name *by right of its own DAG
/// name*, so it must report a foreign holder as a named collision rather than
/// stripping it. Only a registrant that owns the name by right (issue #1157's
/// repro) may release a squatter.
#[tokio::test]
async fn a_decoupled_registration_never_steals_a_foreign_name() {
    let (mut conn, _db) = setup_db().await;

    // Some other row already holds `wants_this`.
    let holder = insert_raw_row(
        &mut conn,
        Some("incumbent_dag"),
        Some("wants_this"),
        "cron:0 2 * * *",
    )
    .await;

    let mut decoupled =
        WorkflowSchedule::new("wants_this", Schedule::Cron("0 5 * * *".to_string()));
    decoupled.dag_name = Some("owning_dag".to_string());

    let result = register_workflow_schedules(&mut conn, &[decoupled]).await;

    match result {
        Err(autumn_harvest::HarvestError::Config(message)) => {
            assert!(
                message.contains("wants_this") && message.contains("owning_dag"),
                "the refusal must name both sides so an operator can rename one; \
                 got: {message}"
            );
        }
        other => panic!(
            "a registrant with no claim by right must report a named collision, \
             never steal the name; got: {other:?}"
        ),
    }

    // The incumbent is untouched — the repair path never manufactures a victim.
    assert_eq!(
        row_by_id(&mut conn, holder).await,
        (
            Some("incumbent_dag".to_string()),
            Some("wants_this".to_string())
        ),
        "a holder we have no claim against must keep its workflow_name"
    );
}

// ── Defect 2 — one bad schedule must not starve the rest ────────────────────

fn empty_registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(vec![], vec![]))
}

fn build_pool(url: &str) -> autumn_harvest::worker::DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("pool")
}

#[tokio::test]
async fn one_unconvergeable_schedule_does_not_starve_the_rest() {
    let (mut conn, db) = setup_db().await;

    // A consistent peer row so `broken_dag`'s claim on `shared` is a genuine,
    // permanent conflict (see the test above).
    insert_raw_row(&mut conn, Some("shared"), Some("shared"), "cron:0 3 * * *").await;

    let mut broken = WorkflowSchedule::new("shared", Schedule::Cron("0 4 * * *".to_string()));
    broken.dag_name = Some("broken_dag".to_string());

    // `broken` is FIRST in registration order. Pre-fix the `?` short-circuit in
    // `register_workflow_schedules_for_shard` aborts the whole pass, so every
    // schedule after it is never registered at all — a tick raises exactly one
    // error however many schedules are broken.
    let schedules = vec![
        broken,
        dag_schedule("healthy_one"),
        dag_schedule("healthy_two"),
    ];

    let pool = build_pool(&db.url);
    tick_once(
        pool,
        empty_registry(),
        Arc::new(DagCatalog::new()),
        Arc::new(schedules),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("a tick must not fail because one schedule is unconvergeable");

    assert!(
        row_for_workflow(&mut conn, "healthy_one").await.is_some(),
        "a healthy schedule after the broken one must still be registered"
    );
    assert!(
        row_for_workflow(&mut conn, "healthy_two").await.is_some(),
        "every healthy schedule after the broken one must still be registered"
    );
}

// ── Defect 3 — no unconditional re-registration writes ──────────────────────

#[tokio::test]
async fn a_converged_registration_pass_performs_no_writes() {
    let (mut conn, _db) = setup_db().await;

    let schedules = vec![dag_schedule("nightly")];
    register_workflow_schedules(&mut conn, &schedules)
        .await
        .expect("first registration writes the row");

    let (id, _) = row_for_workflow(&mut conn, "nightly")
        .await
        .expect("row exists");
    let first = updated_at_of(&mut conn, id).await;

    // On a healthy twelve-DAG deployment the pre-fix reconciler issues ~2
    // million UPDATEs a day against a twelve-row table, none of which change
    // anything — and it is the amplifier that turns any single failure into a
    // sustained log storm.
    for _ in 0..3 {
        register_workflow_schedules(&mut conn, &schedules)
            .await
            .expect("converged registration");
    }

    assert_eq!(
        updated_at_of(&mut conn, id).await,
        first,
        "a converged row must not be rewritten; `updated_at` proves no UPDATE ran"
    );
}

#[tokio::test]
async fn a_drifted_row_is_still_repaired() {
    use autumn_harvest::schema::harvest_schedules::dsl;

    let (mut conn, _db) = setup_db().await;

    let schedules = vec![dag_schedule("nightly")];
    register_workflow_schedules(&mut conn, &schedules)
        .await
        .expect("first registration");
    let (id, _) = row_for_workflow(&mut conn, "nightly")
        .await
        .expect("row exists");

    // Skipping converged writes must NOT cost self-healing: a row mutated out
    // from under the reconciler is still reconciled on the next pass.
    diesel::update(dsl::harvest_schedules.find(id))
        .set(dsl::queue_name.eq(Some("hand_edited")))
        .execute(&mut conn)
        .await
        .expect("drift the row");

    register_workflow_schedules(&mut conn, &schedules)
        .await
        .expect("second registration repairs the drift");

    let queue: Option<String> = dsl::harvest_schedules
        .find(id)
        .select(dsl::queue_name)
        .first(&mut conn)
        .await
        .expect("queue_name");
    assert_eq!(
        queue.as_deref(),
        Some("default"),
        "a drifted column must be repaired, not left alone by the fast path"
    );
}

// ── Defect 4 — one reconciler per fleet, not per process ────────────────────

#[tokio::test]
async fn a_peer_holding_the_registration_lock_makes_the_pass_skip() {
    let (mut conn, db) = setup_db().await;

    // Simulate a peer process mid-registration: hold the registration advisory
    // lock in an open transaction on a second connection.
    let mut holder = AsyncPgConnection::establish(&db.url)
        .await
        .expect("holder connect");
    holder
        .batch_execute("BEGIN")
        .await
        .expect("holder transaction");
    diesel::sql_query(autumn_harvest::scheduler::registration_lock_stmt())
        .bind::<diesel::sql_types::Text, _>(autumn_harvest::scheduler::REGISTRATION_LOCK_KEY)
        .execute(&mut holder)
        .await
        .expect("holder takes the lock");

    let schedules = vec![dag_schedule("nightly")];
    let pool = build_pool(&db.url);
    tick_once(
        pool.clone(),
        empty_registry(),
        Arc::new(DagCatalog::new()),
        Arc::new(schedules.clone()),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("a tick that cannot take the lock must skip, not fail");

    assert_eq!(
        schedule_count(&mut conn).await,
        0,
        "a peer already reconciling must suppress this process's duplicate writes"
    );

    // Release the peer; the very next tick converges.
    holder.batch_execute("ROLLBACK").await.expect("rollback");
    drop(holder);

    tick_once(
        pool,
        empty_registry(),
        Arc::new(DagCatalog::new()),
        Arc::new(schedules),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("tick after the peer released the lock");

    assert!(
        row_for_workflow(&mut conn, "nightly").await.is_some(),
        "registration must resume once the peer releases the lock"
    );
}

// ── Defect 1 — proven through the path production actually runs ─────────────

/// The other squatter tests drive `register_workflow_schedules`, which is the
/// management-API entry point — *not* the 1 Hz reconciler. The tick path
/// differs materially: it runs a convergence probe first, writes under the
/// registration advisory lock, and — critically — **swallows registration
/// errors into the backoff**. So a regression there would leave `tick_once`
/// returning `Ok` with only a WARN log, and a test that checked the return
/// value would be vacuous. This one asserts final row state instead.
#[tokio::test]
async fn the_tick_path_also_repairs_a_squatting_row() {
    let (mut conn, db) = setup_db().await;

    let dag_row = insert_raw_row(&mut conn, Some("my_dag"), None, "cron:0 1 * * *").await;
    let squatter = insert_raw_row(
        &mut conn,
        Some("some_other_dag"),
        Some("my_dag"),
        "cron:0 2 * * *",
    )
    .await;

    tick_once(
        build_pool(&db.url),
        empty_registry(),
        Arc::new(DagCatalog::new()),
        Arc::new(vec![dag_schedule("my_dag")]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("tick");

    // The DAG row now owns its own name...
    assert_eq!(
        row_by_id(&mut conn, dag_row).await,
        (Some("my_dag".to_string()), Some("my_dag".to_string())),
        "the tick path must reconcile the squatter, not just log and back off"
    );
    // ...and the squatter kept its identity, only surrendering the name.
    assert_eq!(
        row_by_id(&mut conn, squatter).await,
        (Some("some_other_dag".to_string()), None),
        "the squatter must be released, never deleted"
    );
}

// ── Defect 1 — releasing a name must preserve everything else ───────────────

/// `release_squatted_workflow_name` nulls one column. That is deliberately
/// non-destructive: the row keeps its identity, operator pause state and
/// counters, so its rightful owner re-stamps it and an operator's pause is not
/// silently undone. Asserting only `(dag_name, workflow_name)` would let a
/// refactor to delete-and-reinsert pass while destroying all of that.
#[tokio::test]
async fn releasing_a_squatted_name_preserves_the_rows_other_state() {
    use autumn_harvest::schema::harvest_schedules::dsl;

    let (mut conn, _db) = setup_db().await;

    insert_raw_row(&mut conn, Some("my_dag"), None, "cron:0 1 * * *").await;
    let squatter = insert_raw_row(
        &mut conn,
        Some("some_other_dag"),
        Some("my_dag"),
        "cron:0 2 * * *",
    )
    .await;

    // Give the squatter operator state and counters worth losing. The
    // timestamp is truncated to microseconds so the round-trip compares
    // exactly -- Postgres stores `timestamptz` at microsecond precision.
    let pinned_next_run = (Utc::now() + chrono::Duration::hours(5)).trunc_subsecs(6);
    diesel::update(dsl::harvest_schedules.find(squatter))
        .set((
            dsl::is_paused.eq(true),
            dsl::paused_by.eq(Some("operator")),
            dsl::runs_started.eq(7),
            dsl::next_run_at.eq(Some(pinned_next_run)),
            dsl::buffered_runs.eq(serde_json::json!(["2026-01-01T00:00:00Z"])),
        ))
        .execute(&mut conn)
        .await
        .expect("seed squatter state");

    register_workflow_schedules(&mut conn, &[dag_schedule("my_dag")])
        .await
        .expect("registration must reconcile the squatter");

    let (is_paused, paused_by, runs_started, next_run_at, buffered) = dsl::harvest_schedules
        .find(squatter)
        .select((
            dsl::is_paused,
            dsl::paused_by,
            dsl::runs_started,
            dsl::next_run_at,
            dsl::buffered_runs,
        ))
        .first::<(
            bool,
            Option<String>,
            i32,
            Option<DateTime<Utc>>,
            serde_json::Value,
        )>(&mut conn)
        .await
        .expect("the released row must still exist");

    assert!(is_paused, "an operator pause must survive the release");
    assert_eq!(paused_by.as_deref(), Some("operator"));
    assert_eq!(runs_started, 7, "bounded-run counters must survive");
    assert_eq!(next_run_at, Some(pinned_next_run));
    assert_eq!(buffered, serde_json::json!(["2026-01-01T00:00:00Z"]));
}

// ── Defect 2 — the backoff must actually be wired into the tick ─────────────

/// The pure tests exercise `ScheduleRegistrationBackoff` in isolation; they
/// prove the data structure, not that the reconciler consults it. Deleting the
/// `should_attempt` guard would leave every one of them passing. This drives
/// two consecutive ticks over one unconvergeable schedule with a caller-owned
/// registry and asserts the second tick was **suppressed** — the failure count
/// stays at 1 rather than escalating to 2.
#[tokio::test]
async fn a_failing_schedule_is_suppressed_on_the_very_next_tick() {
    use autumn_harvest::scheduler::{
        ScheduleRegistrationBackoff, registration_backoff_key, tick_once_sharded_with_backoff,
    };
    use autumn_harvest::shard::{ShardRouter, ShardedDbPool};
    use autumn_harvest::types::ShardId;

    let (mut conn, db) = setup_db().await;

    // A well-formed peer owns `shared`, so registering a different dag for that
    // name is a permanent, unconvergeable configuration collision.
    insert_raw_row(&mut conn, Some("shared"), Some("shared"), "cron:0 3 * * *").await;
    let mut broken = WorkflowSchedule::new("shared", Schedule::Cron("0 4 * * *".to_string()));
    broken.dag_name = Some("my_dag".to_string());

    let backoff = ScheduleRegistrationBackoff::new();
    let key = registration_backoff_key("workflow", "shared", ShardId::new(0));
    let schedules = Arc::new(vec![broken]);

    for _ in 0..2 {
        tick_once_sharded_with_backoff(
            ShardedDbPool::single(build_pool(&db.url)),
            ShardRouter::single(),
            empty_registry(),
            Arc::new(DagCatalog::new()),
            Arc::clone(&schedules),
            SchedulerMonitor::offline(),
            &backoff,
        )
        .await
        .expect("a per-schedule failure must not fail the tick");
    }

    assert_eq!(
        backoff.failure_count(&key),
        1,
        "the second tick must be SUPPRESSED by the backoff, not re-issue the \
         identical failing write -- that re-issue is the storm"
    );
}
