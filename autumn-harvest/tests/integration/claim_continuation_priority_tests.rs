#![cfg(feature = "db")]
#![allow(
    clippy::doc_markdown,
    clippy::items_after_statements,
    clippy::too_many_lines
)]
//! Continuations outrank new starts at claim (issue #1824).
//!
//! A new start is the first workflow task of a freshly admitted run. The
//! start path marks it, and its `attempt` is 0. Every other task is a
//! continuation. Examples are an activity task, a woken workflow task, and the
//! first task of a child, a continue-as-new or a workflow retry.
//!
//! Within one priority level, a new start sorts as if it were due
//! `NEW_START_HANDICAP_SECS` later. A new start that waits longer than that
//! competes FIFO again, so a stream of continuations cannot starve it.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres.
//! Otherwise a fresh testcontainers Postgres boots with the full bundle.

use autumn_harvest::execution::{StartWorkflowParams, start_or_load_workflow_execution};
use autumn_harvest::queue::{
    self, BatchedClaimConfig, EnqueueParams, NEW_START_HANDICAP_SECS, TaskType,
};
use autumn_harvest::types::{ExecutionId, Priority};
use diesel_async::{AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// ── Setup ─────────────────────────────────────────────────────────────────────

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as diesel_async::AsyncConnection>::establish(url)
        .await
        .expect("connect")
}

async fn setup_db() -> (AsyncPgConnection, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (connect(&url).await, None);
    }
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");
    let mut conn = connect(&url).await;
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("migrations");
    (conn, Some(container))
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// How far a fixture row is aged to lead plain FIFO order.
///
/// `enqueue` and `wake_workflow_task` both date a row 5 seconds in the past
/// to absorb clock skew. A lead must clear that allowance by a wide margin.
const FIFO_LEAD_SECS: i32 = 15;

fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", &Uuid::new_v4().simple().to_string()[..8])
}

async fn insert_execution(conn: &mut AsyncPgConnection) -> Uuid {
    let id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, shard_id, input) \
         VALUES ($1, 'continuation-band', $2, 0, '{}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .bind::<diesel::sql_types::Text, _>(id.to_string())
    .execute(conn)
    .await
    .expect("insert execution");
    id
}

/// Admit a new run through the public start path and return its task row.
async fn start_run(
    conn: &mut AsyncPgConnection,
    queue: &str,
    priority: Priority,
    retry_of: Option<Uuid>,
) -> Uuid {
    let exec_id = ExecutionId::new();
    let workflow_id = unique("wf");
    let mut params = StartWorkflowParams::new(
        "continuation-band",
        &workflow_id,
        exec_id,
        serde_json::json!({}),
        queue,
    );
    params.priority = priority;
    params.retry_of_exec_id = retry_of;
    let started = start_or_load_workflow_execution(conn, params, None)
        .await
        .expect("start run");
    assert!(started.created, "the start creates a run");
    task_of(conn, exec_id.as_uuid()).await
}

/// Admit a new run at `priority` and return its first task.
async fn new_start(conn: &mut AsyncPgConnection, queue: &str, priority: i32) -> Uuid {
    let priority = Priority::from_i32(priority).expect("a known priority");
    start_run(conn, queue, priority, None).await
}

/// The one workflow task row of a run.
async fn task_of(conn: &mut AsyncPgConnection, exec_id: Uuid) -> Uuid {
    #[derive(diesel::QueryableByName)]
    struct Id {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: Uuid,
    }
    let row: Id = diesel::sql_query(
        "SELECT id FROM harvest_task_queue WHERE workflow_exec_id = $1 AND task_type = 'workflow'",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id)
    .get_result(conn)
    .await
    .expect("task row");
    row.id
}

/// Enqueue a run's first workflow task outside the start path.
///
/// Child spawns, continue-as-new, reset forks and DLQ redrives write their
/// rows this way. Each extends admitted work, so it is a continuation.
async fn spawned_run(conn: &mut AsyncPgConnection, queue: &str) -> Uuid {
    let exec_id = insert_execution(conn).await;
    let mut params = EnqueueParams::new(queue, TaskType::Workflow, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    queue::enqueue(conn, &params)
        .await
        .expect("enqueue spawned run")
}

/// Enqueue an activity task of an existing run.
async fn activity_continuation(conn: &mut AsyncPgConnection, queue: &str) -> Uuid {
    let exec_id = insert_execution(conn).await;
    let mut params = EnqueueParams::new(queue, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.activity_name = Some("noop".to_string());
    params.activity_id = Some(Uuid::new_v4());
    queue::enqueue(conn, &params)
        .await
        .expect("enqueue activity")
}

/// Start a run, claim and park its workflow task, and return the row id and
/// execution id. A later [`queue::wake_workflow_task`] re-pends the same row
/// as a continuation.
async fn parked_run(conn: &mut AsyncPgConnection, queue: &str, worker: &str) -> (Uuid, Uuid) {
    let task_id = new_start(conn, queue, 0).await;
    let exec_id = exec_of(conn, task_id).await;
    let claimed = claim_one(conn, queue, worker).await;
    assert_eq!(claimed, Some(task_id), "setup claims the run's first task");
    let had_wake = queue::park_workflow_task(conn, task_id, None)
        .await
        .expect("park");
    assert!(!had_wake, "setup has no pending wake");
    (task_id, exec_id)
}

async fn exec_of(conn: &mut AsyncPgConnection, task_id: Uuid) -> Uuid {
    #[derive(diesel::QueryableByName)]
    struct Exec {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        workflow_exec_id: Uuid,
    }
    let row: Exec =
        diesel::sql_query("SELECT workflow_exec_id FROM harvest_task_queue WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(task_id)
            .get_result(conn)
            .await
            .expect("exec id");
    row.workflow_exec_id
}

/// Move a row's due time `secs` seconds further into the past.
///
/// The shift is relative to the row's own due time, so every fixture row
/// stays on the host clock that `enqueue` and `wake_workflow_task` use.
async fn age(conn: &mut AsyncPgConnection, task_id: Uuid, secs: i32) {
    diesel::sql_query(
        "UPDATE harvest_task_queue \
         SET scheduled_at = scheduled_at - make_interval(secs => $2) WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<diesel::sql_types::Integer, _>(secs)
    .execute(conn)
    .await
    .expect("age row");
}

async fn claim_one(conn: &mut AsyncPgConnection, queue: &str, worker: &str) -> Option<Uuid> {
    claim_aged(conn, queue, worker, None).await
}

/// Claim one row with the given priority ageing interval.
async fn claim_aged(
    conn: &mut AsyncPgConnection,
    queue: &str,
    worker: &str,
    aging_secs: Option<u32>,
) -> Option<Uuid> {
    queue::claim_task(conn, &[queue.to_owned()], worker, "", aging_secs, &[], &[])
        .await
        .expect("claim")
        .map(|t| t.id)
}

async fn claim_one_batched(
    conn: &mut AsyncPgConnection,
    queue: &str,
    worker: &str,
) -> Option<Uuid> {
    queue::claim_task_batched(
        conn,
        &[queue.to_owned()],
        worker,
        "",
        None,
        &[],
        &[],
        BatchedClaimConfig {
            batch_size: 2,
            max_batches: 20,
        },
    )
    .await
    .expect("batched claim")
    .map(|t| t.id)
}

/// The single-slot backlog both claim paths share.
///
/// Holds three parked runs, four new starts and two activity continuations.
/// The new starts lead FIFO by [`FIFO_LEAD_SECS`], so plain FIFO would claim
/// them first.
struct Backlog {
    queue: String,
    continuations: Vec<Uuid>,
    starts: Vec<Uuid>,
}

async fn backlog(conn: &mut AsyncPgConnection, worker: &str) -> Backlog {
    let queue = unique("band");
    let mut parked = Vec::new();
    for _ in 0..3 {
        parked.push(parked_run(conn, &queue, worker).await);
    }
    let mut starts = Vec::new();
    for _ in 0..4 {
        let id = new_start(conn, &queue, 0).await;
        age(conn, id, FIFO_LEAD_SECS).await;
        starts.push(id);
    }
    let mut continuations = Vec::new();
    for _ in 0..2 {
        continuations.push(activity_continuation(conn, &queue).await);
    }
    for (task_id, exec_id) in parked {
        queue::wake_workflow_task(conn, ExecutionId::from_uuid(exec_id))
            .await
            .expect("wake");
        continuations.push(task_id);
    }
    pending_check(conn, &continuations).await;
    Backlog {
        queue,
        continuations,
        starts,
    }
}

/// Every continuation is due again, and each woken row was claimed once.
async fn pending_check(conn: &mut AsyncPgConnection, ids: &[Uuid]) {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let due: Count = diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_task_queue \
         WHERE id = ANY($1) AND state = 'PENDING' AND scheduled_at <= NOW()",
    )
    .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(ids)
    .get_result(conn)
    .await
    .expect("count due");
    assert_eq!(
        due.n,
        i64::try_from(ids.len()).expect("small fixture"),
        "every continuation is due"
    );
}

fn assert_continuations_first(order: &[Uuid], backlog: &Backlog) {
    let m = backlog.continuations.len();
    assert_eq!(
        order.len(),
        m + backlog.starts.len(),
        "every row is claimed"
    );
    for id in &order[..m] {
        assert!(
            backlog.continuations.contains(id),
            "the first {m} claims are continuations; got order {order:?}"
        );
    }
    for id in &order[m..] {
        assert!(
            backlog.starts.contains(id),
            "new starts follow the continuations; got order {order:?}"
        );
    }
}

// ── AC1: continuations first, single slot ─────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn single_slot_claims_continuations_before_new_starts() {
    let (mut conn, _container) = setup_db().await;
    let worker = unique("w");
    let backlog = backlog(&mut conn, &worker).await;

    let mut order = Vec::new();
    while let Some(id) = claim_one(&mut conn, &backlog.queue, &worker).await {
        order.push(id);
    }
    assert_continuations_first(&order, &backlog);
}

#[tokio::test(flavor = "multi_thread")]
async fn batched_claim_also_claims_continuations_before_new_starts() {
    let (mut conn, _container) = setup_db().await;
    let worker = unique("w");
    let backlog = backlog(&mut conn, &worker).await;

    let mut order = Vec::new();
    while let Some(id) = claim_one_batched(&mut conn, &backlog.queue, &worker).await {
        order.push(id);
    }
    assert_continuations_first(&order, &backlog);
}

// ── AC1: new starts still progress ────────────────────────────────────────────

/// Each round adds two fresh continuations and claims one, so continuation
/// pressure grows without bound. Every new start older than the handicap is
/// still claimed, one per round, in FIFO order.
#[tokio::test(flavor = "multi_thread")]
async fn new_starts_are_not_starved_by_a_stream_of_continuations() {
    let (mut conn, _container) = setup_db().await;
    let worker = unique("w");
    let queue = unique("starve");
    let aged = i32::try_from(NEW_START_HANDICAP_SECS).expect("small handicap") + FIFO_LEAD_SECS;

    let mut starts = Vec::new();
    for i in 0..3 {
        let id = new_start(&mut conn, &queue, 0).await;
        age(&mut conn, id, aged + 3 - i).await;
        starts.push(id);
    }

    let mut claimed_starts = Vec::new();
    for _ in 0..6 {
        activity_continuation(&mut conn, &queue).await;
        activity_continuation(&mut conn, &queue).await;
        let id = claim_one(&mut conn, &queue, &worker)
            .await
            .expect("a row is due every round");
        if starts.contains(&id) {
            claimed_starts.push(id);
        }
    }
    assert_eq!(
        claimed_starts, starts,
        "aged new starts progress in FIFO order under continuation load"
    );
}

/// The handicap is real but bounded. A young new start yields to a fresh
/// continuation. Once it is older than the handicap, it no longer yields.
#[tokio::test(flavor = "multi_thread")]
async fn the_handicap_expires_after_new_start_handicap_secs() {
    let (mut conn, _container) = setup_db().await;
    let worker = unique("w");
    let handicap = i32::try_from(NEW_START_HANDICAP_SECS).expect("small handicap");

    let queue = unique("young");
    let young = new_start(&mut conn, &queue, 0).await;
    age(&mut conn, young, handicap - FIFO_LEAD_SECS).await;
    let fresh = activity_continuation(&mut conn, &queue).await;
    assert_eq!(claim_one(&mut conn, &queue, &worker).await, Some(fresh));
    assert_eq!(claim_one(&mut conn, &queue, &worker).await, Some(young));

    let queue = unique("old");
    let old = new_start(&mut conn, &queue, 0).await;
    age(&mut conn, old, handicap + FIFO_LEAD_SECS).await;
    let fresh = activity_continuation(&mut conn, &queue).await;
    assert_eq!(claim_one(&mut conn, &queue, &worker).await, Some(old));
    assert_eq!(claim_one(&mut conn, &queue, &worker).await, Some(fresh));
}

// ── Explicit priority still wins ──────────────────────────────────────────────

/// The band is a tie-break inside one priority level. An operator who sets
/// a higher priority on a start keeps that order.
#[tokio::test(flavor = "multi_thread")]
async fn explicit_priority_outranks_the_continuation_band() {
    let (mut conn, _container) = setup_db().await;
    let worker = unique("w");
    let queue = unique("prio");

    let continuation = activity_continuation(&mut conn, &queue).await;
    age(&mut conn, continuation, FIFO_LEAD_SECS).await;
    let urgent_start = new_start(&mut conn, &queue, 1).await;

    assert_eq!(
        claim_one(&mut conn, &queue, &worker).await,
        Some(urgent_start)
    );
    assert_eq!(
        claim_one(&mut conn, &queue, &worker).await,
        Some(continuation)
    );
}

// ── Only a fresh admission yields ─────────────────────────────────────────────

/// A child spawn, continue-as-new or reset fork extends admitted work. Its
/// first task does not yield, even though it was never claimed.
#[tokio::test(flavor = "multi_thread")]
async fn a_spawned_run_is_a_continuation() {
    let (mut conn, _container) = setup_db().await;
    let worker = unique("w");
    let queue = unique("spawn");

    let start = new_start(&mut conn, &queue, 0).await;
    age(&mut conn, start, FIFO_LEAD_SECS).await;
    let spawned = spawned_run(&mut conn, &queue).await;

    assert_eq!(claim_one(&mut conn, &queue, &worker).await, Some(spawned));
    assert_eq!(claim_one(&mut conn, &queue, &worker).await, Some(start));
}

/// A workflow-level retry continues a failed run (issue #523). It does not
/// yield to fresh admissions.
#[tokio::test(flavor = "multi_thread")]
async fn a_workflow_retry_is_a_continuation() {
    let (mut conn, _container) = setup_db().await;
    let worker = unique("w");
    let queue = unique("retry");

    let start = new_start(&mut conn, &queue, 0).await;
    age(&mut conn, start, FIFO_LEAD_SECS).await;
    let failed_run = insert_execution(&mut conn).await;
    let retry = start_run(&mut conn, &queue, Priority::Normal, Some(failed_run)).await;

    assert_eq!(claim_one(&mut conn, &queue, &worker).await, Some(retry));
    assert_eq!(claim_one(&mut conn, &queue, &worker).await, Some(start));
}

/// The start path marks a fresh admission. The marker is what the claim
/// order reads, so pin it on the row.
#[tokio::test(flavor = "multi_thread")]
async fn the_start_path_marks_only_a_fresh_admission() {
    let (mut conn, _container) = setup_db().await;
    let queue = unique("marker");
    let fresh = new_start(&mut conn, &queue, 0).await;
    let spawned = spawned_run(&mut conn, &queue).await;

    #[derive(diesel::QueryableByName)]
    struct Marker {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        new_start: bool,
    }
    for (id, expected) in [(fresh, true), (spawned, false)] {
        let row: Marker =
            diesel::sql_query("SELECT new_start FROM harvest_task_queue WHERE id = $1")
                .bind::<diesel::sql_types::Uuid, _>(id)
                .get_result(&mut conn)
                .await
                .expect("marker");
        assert_eq!(row.new_start, expected);
    }
}

// ── The worker's own claim path ───────────────────────────────────────────────

/// A worker with a full activity pool claims workflow tasks only (issue
/// #1787). Woken runs still go before new starts on that path.
#[tokio::test(flavor = "multi_thread")]
async fn the_kind_filtered_claim_takes_woken_runs_before_new_starts() {
    let (mut conn, _container) = setup_db().await;
    let worker = unique("w");
    let backlog = backlog(&mut conn, &worker).await;

    let mut order = Vec::new();
    while let Some(task) = queue::claim_task_of_kind_on_shard(
        &mut conn,
        std::slice::from_ref(&backlog.queue),
        &worker,
        "",
        None,
        &[],
        &[],
        None,
        Some(TaskType::Workflow),
    )
    .await
    .expect("kind claim")
    {
        order.push(task.id);
    }
    let woken = &backlog.continuations[2..];
    assert_eq!(order.len(), woken.len() + backlog.starts.len());
    for id in &order[..woken.len()] {
        assert!(woken.contains(id), "woken runs go first; got {order:?}");
    }
}

// ── Priority ageing and the band ──────────────────────────────────────────────

/// Priority ageing reads `scheduled_at`. An interval longer than the age of
/// the start leaves the band in force.
#[tokio::test(flavor = "multi_thread")]
async fn slow_priority_ageing_keeps_the_band() {
    let (mut conn, _container) = setup_db().await;
    let worker = unique("w");
    let queue = unique("age-slow");
    let start = new_start(&mut conn, &queue, 0).await;
    age(&mut conn, start, FIFO_LEAD_SECS).await;
    let fresh = activity_continuation(&mut conn, &queue).await;

    let aging = Some(60);
    assert_eq!(
        claim_aged(&mut conn, &queue, &worker, aging).await,
        Some(fresh)
    );
    assert_eq!(
        claim_aged(&mut conn, &queue, &worker, aging).await,
        Some(start)
    );
}

/// An ageing interval shorter than the age of the start lifts it one
/// priority level. That outranks the band, as the docs state.
#[tokio::test(flavor = "multi_thread")]
async fn fast_priority_ageing_lifts_an_aged_start_over_the_band() {
    let (mut conn, _container) = setup_db().await;
    let worker = unique("w");
    let queue = unique("age-fast");
    let start = new_start(&mut conn, &queue, 0).await;
    age(&mut conn, start, FIFO_LEAD_SECS).await;
    let fresh = activity_continuation(&mut conn, &queue).await;

    let aging = Some(10);
    assert_eq!(
        claim_aged(&mut conn, &queue, &worker, aging).await,
        Some(start)
    );
    assert_eq!(
        claim_aged(&mut conn, &queue, &worker, aging).await,
        Some(fresh)
    );
}

// ── Starvation under simulated time ───────────────────────────────────────────

/// Time passes in steps instead of by backdating one row. Each round adds two
/// fresh continuations, claims one, then ages every `PENDING` row of the
/// queue by 10 seconds. The continuation backlog grows without bound. The
/// new start must yield at first and still be claimed in bounded rounds.
#[tokio::test(flavor = "multi_thread")]
async fn a_new_start_progresses_while_time_passes_under_continuation_load() {
    let (mut conn, _container) = setup_db().await;
    let worker = unique("w");
    let queue = unique("sim");
    let start = new_start(&mut conn, &queue, 0).await;

    let mut claimed_in = None;
    for round in 0..16 {
        activity_continuation(&mut conn, &queue).await;
        activity_continuation(&mut conn, &queue).await;
        let id = claim_one(&mut conn, &queue, &worker)
            .await
            .expect("a row is due every round");
        if id == start {
            claimed_in = Some(round);
            break;
        }
        diesel::sql_query(
            "UPDATE harvest_task_queue SET scheduled_at = scheduled_at - INTERVAL '10 seconds' \
             WHERE queue_name = $1 AND state = 'PENDING'",
        )
        .bind::<diesel::sql_types::Text, _>(&queue)
        .execute(&mut conn)
        .await
        .expect("advance time");
    }
    let round = claimed_in.expect("the new start is claimed in bounded rounds");
    assert!(
        round >= 3,
        "the new start yields at first; claimed in round {round}"
    );
}
