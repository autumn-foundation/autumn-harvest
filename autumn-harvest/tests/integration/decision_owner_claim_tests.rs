#![cfg(feature = "db")]
//! Owned workflow-task claims and the owner-mode switch (issue #2252).
//!
//! Owner mode is a per-database switch, so each test here runs on its own
//! database. With `HARVEST_TEST_DATABASE_URL` set, a test creates a fresh
//! database on that server and drops it at the end. Without it, each test
//! starts its own container.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::decision_owner::{
    self, Acquisition, DecisionPartition, EnableOwnerMode, OwnedPartitions, OwnerHolder, OwnerKey,
    OwnerLeaseConfig, OwnerLeaseTtl, OwnerLoop,
};
use autumn_harvest::prelude::*;
use autumn_harvest::queue::{self, EnqueueParams, OwnedClaim, TaskType};
use autumn_harvest::telemetry::{MetricsRecorder, ResidentOutcome, TelemetryConfig};
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, Worker, WorkerRuntimeConfig};
use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Integer, Text};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;

use crate::integration_e2e::{
    build_test_pool, setup_test_database_url, spawn_test_worker,
    wait_for_execution_state_with_timeout,
};

const STEPS: [&str; 3] = ["owner_step_a", "owner_step_b", "owner_step_c"];

// ── Database ────────────────────────────────────────────────────────────────

/// A migrated database that one test owns, and how to drop it.
pub struct OwnDb {
    pub url: String,
    _container: Option<ContainerAsync<Postgres>>,
    name: Option<String>,
}

impl OwnDb {
    pub async fn new() -> Self {
        let Ok(shared) = std::env::var("HARVEST_TEST_DATABASE_URL") else {
            let (url, container) = setup_test_database_url().await;
            return Self {
                url,
                _container: Some(container),
                name: None,
            };
        };
        let name = format!("harvest_owner_{}", uuid::Uuid::new_v4().simple());
        let mut admin = AsyncPgConnection::establish(&shared)
            .await
            .expect("connect");
        diesel::sql_query(format!("CREATE DATABASE {name}"))
            .execute(&mut admin)
            .await
            .expect("create the database");
        let url = with_database(&shared, &name);
        let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
        conn.batch_execute(&autumn_harvest::test_init_sql())
            .await
            .expect("apply migrations");
        Self {
            url,
            _container: None,
            name: Some(name),
        }
    }

    pub async fn conn(&self) -> AsyncPgConnection {
        AsyncPgConnection::establish(&self.url)
            .await
            .expect("connect")
    }

    /// Drop the database. A panicking test leaks it; the prefix names it.
    pub async fn drop_db(self) {
        let (Some(name), Ok(shared)) = (self.name, std::env::var("HARVEST_TEST_DATABASE_URL"))
        else {
            return;
        };
        if let Ok(mut admin) = AsyncPgConnection::establish(&shared).await {
            let _ = diesel::sql_query(format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
                .execute(&mut admin)
                .await;
        }
    }
}

fn with_database(url: &str, database: &str) -> String {
    let (base, query) = url
        .split_once('?')
        .map_or((url, None), |(b, q)| (b, Some(q)));
    let (prefix, _) = base.rsplit_once('/').expect("a database URL has a path");
    query.map_or_else(
        || format!("{prefix}/{database}"),
        |query| format!("{prefix}/{database}?{query}"),
    )
}

fn unique(prefix: &str) -> String {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    format!("{prefix}-{}", &suffix[..12])
}

const fn partition(n: u16) -> DecisionPartition {
    match DecisionPartition::new(n) {
        Some(p) => p,
        None => panic!("partition out of range"),
    }
}

/// Turn owner mode on. A test may have no live owner loop yet, so it forces.
async fn enable(conn: &mut AsyncPgConnection) {
    decision_owner::enable_owner_mode(
        conn,
        &EnableOwnerMode {
            force: true,
            ..EnableOwnerMode::default()
        },
    )
    .await
    .expect("enable owner mode");
}

/// Enqueue a workflow task in `partition`, or with no stamp.
async fn workflow_task(
    conn: &mut AsyncPgConnection,
    queue_name: &str,
    partition: Option<DecisionPartition>,
) -> uuid::Uuid {
    let mut params = EnqueueParams::new(queue_name, TaskType::Workflow, serde_json::json!({}));
    params.decision_partition = partition;
    queue::enqueue(conn, &params).await.expect("enqueue")
}

async fn activity_task(conn: &mut AsyncPgConnection, queue_name: &str) -> uuid::Uuid {
    let mut params = EnqueueParams::new(queue_name, TaskType::Activity, serde_json::json!({}));
    params.activity_name = Some("owner_any".to_string());
    queue::enqueue(conn, &params).await.expect("enqueue")
}

/// The legacy claim of a worker that knows nothing of owner mode.
async fn legacy_claim(
    conn: &mut AsyncPgConnection,
    queue_name: &str,
) -> Option<autumn_harvest::models::TaskQueueItem> {
    queue::claim_task(
        conn,
        &[queue_name.to_string()],
        "legacy-worker",
        "",
        None,
        &[],
        &[],
    )
    .await
    .expect("legacy claim")
}

async fn owned_claim(
    conn: &mut AsyncPgConnection,
    queue_name: &str,
    owned: &OwnedClaim,
) -> Option<autumn_harvest::models::TaskQueueItem> {
    queue::claim_task_with_owner(
        conn,
        &[queue_name.to_string()],
        "owner-worker",
        "",
        None,
        &[],
        &[],
        None,
        None,
        queue::ClaimFairness::Off,
        Some(owned),
    )
    .await
    .expect("owned claim")
}

/// Acquire `p` for a new holder of `worker`.
async fn take(
    conn: &mut AsyncPgConnection,
    worker: &str,
    p: DecisionPartition,
) -> decision_owner::OwnerTenure {
    let holder = OwnerHolder::for_worker(worker);
    match decision_owner::acquire(
        conn,
        ShardId::UNENCODED,
        &holder,
        p,
        &OwnerLeaseConfig::default(),
    )
    .await
    .expect("acquire")
    {
        Acquisition::Acquired(tenure) => tenure,
        other => panic!("acquire {p}: {other:?}"),
    }
}

#[derive(QueryableByName)]
struct StateRow {
    #[diesel(sql_type = Text)]
    state: String,
    #[diesel(sql_type = Integer)]
    attempt: i32,
}

#[derive(QueryableByName)]
struct StampRow {
    #[diesel(sql_type = Text)]
    state: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::SmallInt>)]
    decision_partition: Option<i16>,
}

#[derive(QueryableByName)]
struct StampDueRow {
    #[diesel(sql_type = Text)]
    state: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::SmallInt>)]
    decision_partition: Option<i16>,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    due: bool,
}

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

async fn task_state(conn: &mut AsyncPgConnection, id: uuid::Uuid) -> (String, i32) {
    let row: StateRow =
        diesel::sql_query("SELECT state, attempt FROM harvest_task_queue WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(id)
            .get_result(conn)
            .await
            .expect("read the task");
    (row.state, row.attempt)
}

// ── The claim guard ─────────────────────────────────────────────────────────

/// Done-when item 4: a legacy-mode worker cannot claim a workflow task on
/// an owner-mode shard. The guard is the structural part, so the claim runs
/// the unchanged legacy statement, as an old binary does.
#[tokio::test]
async fn a_legacy_claim_cannot_take_a_workflow_task_on_an_owner_mode_shard() {
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    let q = unique("owner-legacy");
    enable(&mut conn).await;
    let wf = workflow_task(&mut conn, &q, Some(partition(7))).await;
    let unstamped = workflow_task(&mut conn, &q, None).await;

    assert!(
        legacy_claim(&mut conn, &q).await.is_none(),
        "the guard refuses a legacy workflow claim"
    );
    assert_eq!(task_state(&mut conn, wf).await, ("PENDING".to_string(), 0));
    assert_eq!(
        task_state(&mut conn, unstamped).await,
        ("PENDING".to_string(), 0)
    );

    // Activities stay on the shared claim in both modes. A legacy claim that
    // picks a workflow row gets nothing, so an old binary can also miss an
    // activity behind it. That is why the enable step refuses while one is
    // live.
    let act = activity_task(&mut conn, &q).await;
    let claimed = queue::claim_task_of_kind_on_shard(
        &mut conn,
        std::slice::from_ref(&q),
        "legacy-worker",
        "",
        None,
        &[],
        &[],
        None,
        Some(TaskType::Activity),
    )
    .await
    .expect("legacy activity claim");
    assert_eq!(claimed.map(|t| t.id), Some(act));
    assert!(legacy_claim(&mut conn, &q).await.is_none());
    db.drop_db().await;
}

#[tokio::test]
async fn an_owned_claim_takes_only_owned_and_unstamped_workflow_tasks() {
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    let q = unique("owner-owned");
    enable(&mut conn).await;
    let tenure = take(&mut conn, "owner-worker", partition(11)).await;
    let mine = workflow_task(&mut conn, &q, Some(partition(11))).await;
    let other = workflow_task(&mut conn, &q, Some(partition(12))).await;
    let unstamped = workflow_task(&mut conn, &q, None).await;
    let claim = OwnedClaim {
        holder: tenure.holder().as_str().to_string(),
        partitions: vec![partition(11).as_i16()],
    };

    let mut claimed = Vec::new();
    while let Some(task) = owned_claim(&mut conn, &q, &claim).await {
        claimed.push(task.id);
    }
    claimed.sort();
    let mut expected = vec![mine, unstamped];
    expected.sort();
    assert_eq!(
        claimed, expected,
        "the owned partition and the unstamped row"
    );
    assert_eq!(
        task_state(&mut conn, other).await,
        ("PENDING".to_string(), 0)
    );
    db.drop_db().await;
}

/// The guard matches the holder of the owner row, so a stale owner cannot
/// claim after a takeover, even when its local list is wrong.
#[tokio::test]
async fn the_guard_refuses_a_holder_that_does_not_own_the_partition() {
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    let q = unique("owner-stale");
    enable(&mut conn).await;
    let _owner = take(&mut conn, "real-owner", partition(21)).await;
    let wf = workflow_task(&mut conn, &q, Some(partition(21))).await;
    let stale = OwnedClaim {
        holder: OwnerHolder::for_worker("stale-owner").as_str().to_string(),
        partitions: vec![partition(21).as_i16()],
    };
    assert!(owned_claim(&mut conn, &q, &stale).await.is_none());
    assert_eq!(task_state(&mut conn, wf).await, ("PENDING".to_string(), 0));
    db.drop_db().await;
}

/// With the switch off, the owned claim takes no workflow task, so two
/// claim forms never run at once.
#[tokio::test]
async fn an_owned_claim_takes_no_workflow_task_with_the_switch_off() {
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    let q = unique("owner-off");
    let tenure = take(&mut conn, "owner-worker", partition(31)).await;
    let wf = workflow_task(&mut conn, &q, Some(partition(31))).await;
    let claim = OwnedClaim {
        holder: tenure.holder().as_str().to_string(),
        partitions: vec![partition(31).as_i16()],
    };
    assert!(owned_claim(&mut conn, &q, &claim).await.is_none());
    let legacy = legacy_claim(&mut conn, &q).await.expect("the legacy claim");
    assert_eq!(legacy.id, wf);
    db.drop_db().await;
}

#[tokio::test]
async fn the_owned_by_id_claim_obeys_the_same_gate() {
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    let q = unique("owner-by-id");
    enable(&mut conn).await;
    let tenure = take(&mut conn, "owner-worker", partition(41)).await;
    let mine = workflow_task(&mut conn, &q, Some(partition(41))).await;
    let other = workflow_task(&mut conn, &q, Some(partition(42))).await;
    let claim = OwnedClaim {
        holder: tenure.holder().as_str().to_string(),
        partitions: vec![partition(41).as_i16()],
    };
    let by_id = |id| {
        let q = q.clone();
        let claim = claim.clone();
        let url = db.url.clone();
        async move {
            let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
            queue::claim_task_by_id_with_owner(
                &mut conn,
                id,
                &[q],
                "owner-worker",
                "",
                None,
                &[],
                &[],
                None,
                queue::ClaimFairness::Off,
                Some(&claim),
            )
            .await
            .expect("by-id claim")
        }
    };
    assert!(by_id(other).await.is_none(), "not an owned partition");
    assert_eq!(by_id(mine).await.map(|t| t.id), Some(mine));
    // A legacy by-id claim of a stamped row is refused by the guard.
    let late = workflow_task(&mut conn, &q, Some(partition(41))).await;
    let legacy = queue::claim_task_by_id_on_shard(
        &mut conn,
        late,
        std::slice::from_ref(&q),
        "legacy-worker",
        "",
        None,
        &[],
        &[],
        None,
    )
    .await
    .expect("legacy by-id claim");
    assert!(legacy.is_none());
    db.drop_db().await;
}

// ── Enable and disable ──────────────────────────────────────────────────────

async fn register_worker(conn: &mut AsyncPgConnection, worker_id: &str, capable: bool) {
    let labels = if capable {
        decision_owner::advertise_owner_capability(&serde_json::json!({}))
    } else {
        serde_json::json!({})
    };
    diesel::sql_query(
        "INSERT INTO harvest_workers (worker_id, started_at, last_heartbeat_at, queues, \
             shard_assignments, max_concurrency, in_flight_count, host, version, status, labels) \
         VALUES ($1, NOW(), NOW(), '[\"q\"]'::jsonb, '[]'::jsonb, 1, 0, 'h', 'v', 'Active', $2)",
    )
    .bind::<Text, _>(worker_id)
    .bind::<diesel::sql_types::Jsonb, _>(labels)
    .execute(conn)
    .await
    .expect("register a worker");
}

#[tokio::test]
async fn enable_refuses_while_a_live_worker_cannot_run_owner_mode() {
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    register_worker(&mut conn, "old-binary", false).await;
    register_worker(&mut conn, "new-binary", true).await;
    let refused = decision_owner::enable_owner_mode(&mut conn, &EnableOwnerMode::default()).await;
    let message = refused
        .expect_err("a live legacy worker blocks owner mode")
        .to_string();
    assert!(message.contains("old-binary"), "{message}");
    assert!(!message.contains("new-binary"), "{message}");
    let mode = decision_owner::read_owner_mode(&mut conn)
        .await
        .expect("mode");
    assert!(!mode.enabled && !mode.guard_installed, "{mode:?}");

    let forced = decision_owner::enable_owner_mode(
        &mut conn,
        &EnableOwnerMode {
            force: true,
            ..EnableOwnerMode::default()
        },
    )
    .await
    .expect("forced enable");
    assert_eq!(forced.legacy_workers, vec!["old-binary".to_string()]);
    let mode = decision_owner::read_owner_mode(&mut conn)
        .await
        .expect("mode");
    assert!(mode.enabled && mode.guard_installed, "{mode:?}");
    db.drop_db().await;
}

/// The enable step stamps each live workflow task. It re-pends the legacy
/// claim of a worker that is not live. A live worker keeps its claim.
#[tokio::test]
async fn enable_stamps_rows_and_repends_a_legacy_claim_left_running() {
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    let q = unique("owner-enable");
    let exec = uuid::Uuid::new_v4();
    let workflow_id = "owner-enable-wf";
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, shard_id, input) \
         VALUES ($1, 'owner_enable', $2, 0, '{}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec)
    .bind::<Text, _>(workflow_id)
    .execute(&mut conn)
    .await
    .expect("insert an execution");
    let mut params = EnqueueParams::new(&q, TaskType::Workflow, serde_json::json!({}));
    params.workflow_exec_id = Some(exec);
    let task = queue::enqueue(&mut conn, &params).await.expect("enqueue");
    let legacy = legacy_claim(&mut conn, &q).await.expect("a legacy claim");
    assert_eq!(legacy.id, task);
    // A second run, claimed by a live worker.
    register_worker(&mut conn, "live-legacy", true).await;
    let live_exec = uuid::Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, shard_id, input) \
         VALUES ($1, 'owner_enable', 'owner-enable-live', 0, '{}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(live_exec)
    .execute(&mut conn)
    .await
    .expect("insert an execution");
    let mut params = EnqueueParams::new(&q, TaskType::Workflow, serde_json::json!({}));
    params.workflow_exec_id = Some(live_exec);
    let live_task = queue::enqueue(&mut conn, &params).await.expect("enqueue");
    let live = queue::claim_task(
        &mut conn,
        std::slice::from_ref(&q),
        "live-legacy",
        "",
        None,
        &[],
        &[],
    )
    .await
    .expect("claim")
    .expect("the live worker claims");
    assert_eq!(live.id, live_task);

    let report = decision_owner::enable_owner_mode(
        &mut conn,
        &EnableOwnerMode {
            force: true,
            ..EnableOwnerMode::default()
        },
    )
    .await
    .expect("enable");
    assert_eq!(report.stamped, 2);
    assert_eq!(report.repended, 1, "only the claim of the dead worker");
    assert_eq!(task_state(&mut conn, live_task).await.0, "RUNNING");
    let row: StampRow =
        diesel::sql_query("SELECT state, decision_partition FROM harvest_task_queue WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(task)
            .get_result(&mut conn)
            .await
            .expect("read the task");
    assert_eq!(row.state, "PENDING");
    assert_eq!(
        row.decision_partition,
        Some(DecisionPartition::for_workflow_id(workflow_id).as_i16())
    );
    let again = decision_owner::enable_owner_mode(&mut conn, &EnableOwnerMode::default())
        .await
        .expect("enable again");
    assert_eq!(again.repended, 0);
    assert!(again.already_on);
    db.drop_db().await;
}

#[tokio::test]
async fn disable_drops_the_guard_and_the_legacy_claim_works_again() {
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    let q = unique("owner-disable");
    enable(&mut conn).await;
    let wf = workflow_task(&mut conn, &q, Some(partition(51))).await;
    assert!(legacy_claim(&mut conn, &q).await.is_none());
    decision_owner::disable_owner_mode(&mut conn, Duration::from_secs(5))
        .await
        .expect("disable");
    let mode = decision_owner::read_owner_mode(&mut conn)
        .await
        .expect("mode");
    assert!(!mode.enabled && !mode.guard_installed, "{mode:?}");
    assert_eq!(legacy_claim(&mut conn, &q).await.map(|t| t.id), Some(wf));
    db.drop_db().await;
}

#[tokio::test]
async fn enable_refuses_a_fleet_that_polls_different_queues() {
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    register_worker(&mut conn, "queue-a", true).await;
    register_worker(&mut conn, "queue-b", true).await;
    diesel::sql_query(
        "UPDATE harvest_workers SET queues = '[\"other\"]'::jsonb WHERE worker_id = 'queue-b'",
    )
    .execute(&mut conn)
    .await
    .expect("change the queues of one worker");
    let refused = decision_owner::enable_owner_mode(&mut conn, &EnableOwnerMode::default()).await;
    let message = refused
        .expect_err("a mixed fleet blocks owner mode")
        .to_string();
    assert!(message.contains("different queues"), "{message}");
    let mode = decision_owner::read_owner_mode(&mut conn)
        .await
        .expect("mode");
    assert!(!mode.enabled && !mode.guard_installed, "{mode:?}");
    db.drop_db().await;
}

/// Set the handler fingerprint label of one registered worker.
async fn set_handlers_label(conn: &mut AsyncPgConnection, worker_id: &str, value: &str) {
    diesel::sql_query(
        "UPDATE harvest_workers SET labels = labels || jsonb_build_object($2::text, $3::text) \
         WHERE worker_id = $1",
    )
    .bind::<Text, _>(worker_id)
    .bind::<Text, _>(decision_owner::OWNER_HANDLERS_LABEL)
    .bind::<Text, _>(value)
    .execute(conn)
    .await
    .expect("set the handlers label");
}

/// Enable while three workers advertise `a`, `b` and `none`. Return the
/// error text, or `None` when the handler check passes.
async fn enable_with_handlers(a: &str, b: &str) -> Option<String> {
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    for worker in ["owner-a", "owner-b", "participant"] {
        register_worker(&mut conn, worker, true).await;
    }
    set_handlers_label(&mut conn, "owner-a", a).await;
    set_handlers_label(&mut conn, "owner-b", b).await;
    set_handlers_label(&mut conn, "participant", decision_owner::NO_OWNED_HANDLERS).await;
    let message = decision_owner::enable_owner_mode(&mut conn, &EnableOwnerMode::default())
        .await
        .expect_err("no owner loop is live, so enable fails either way")
        .to_string();
    db.drop_db().await;
    (!message.contains("no owner loop is live")).then_some(message)
}

/// An owner claims every workflow task of its partitions, and a decision can
/// run any registered activity inline. Two live owners with different
/// workflow or activity types could each hold a task that only the other
/// can finish. A worker that owns nothing does not count.
#[tokio::test]
async fn enable_refuses_owners_with_different_handlers() {
    let both = decision_owner::handler_set_fingerprint(["checkout"], ["charge", "ship"]);
    let fewer_workflows =
        decision_owner::handler_set_fingerprint(std::iter::empty(), ["charge", "ship"]);
    let fewer_activities = decision_owner::handler_set_fingerprint(["checkout"], ["charge"]);
    for other in [&fewer_workflows, &fewer_activities] {
        let message = enable_with_handlers(&both, other)
            .await
            .expect("different handlers block owner mode");
        assert!(
            message.contains("different workflow or activity types"),
            "{message}"
        );
    }
    assert_eq!(
        enable_with_handlers(&both, &both).await,
        None,
        "one handler set passes the check"
    );
}

/// The fingerprint ignores order and duplicates, and keeps the two kinds
/// apart.
#[test]
fn the_handler_fingerprint_is_a_pair_of_sets() {
    let fp = |workflows: &[&str], activities: &[&str]| {
        decision_owner::handler_set_fingerprint(
            workflows.iter().copied(),
            activities.iter().copied(),
        )
    };
    assert_eq!(fp(&["b", "a", "b"], &["x"]), fp(&["a", "b"], &["x", "x"]));
    assert_ne!(fp(&["a"], &[]), fp(&["a", "b"], &[]));
    assert_ne!(
        fp(&["ab"], &[]),
        fp(&["a", "b"], &[]),
        "names stay separate"
    );
    assert_ne!(
        fp(&["a"], &[]),
        fp(&[], &["a"]),
        "a workflow is not an activity"
    );
}

/// An unowned decision stamps an unstamped row and re-pends it at once, so
/// the owner of its partition takes it next.
#[tokio::test]
async fn an_unowned_claim_stamps_and_repends_its_row() {
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    let q = unique("owner-stamp");
    enable(&mut conn).await;
    let tenure = take(&mut conn, "owner-worker", partition(61)).await;
    let unstamped = workflow_task(&mut conn, &q, None).await;
    let claim = OwnedClaim {
        holder: tenure.holder().as_str().to_string(),
        partitions: vec![partition(61).as_i16()],
    };
    let task = owned_claim(&mut conn, &q, &claim)
        .await
        .expect("the unstamped row");
    assert_eq!(task.id, unstamped);
    let claim_token = queue::TaskClaim::new(task.id, "owner-worker", task.attempt);
    let repended = queue::requeue_unowned_workflow_task(
        &mut conn,
        &claim_token,
        partition(62),
        chrono::Duration::seconds(30),
    )
    .await
    .expect("re-pend");
    assert!(repended);
    let read = async |conn: &mut AsyncPgConnection| -> StampDueRow {
        diesel::sql_query(
            "SELECT state, decision_partition, scheduled_at <= NOW() + interval '1 second' AS due \
             FROM harvest_task_queue WHERE id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(unstamped)
        .get_result(conn)
        .await
        .expect("read the task")
    };
    let row = read(&mut conn).await;
    assert_eq!(row.state, "PENDING");
    assert_eq!(row.decision_partition, Some(partition(62).as_i16()));
    assert!(
        row.due,
        "an unstamped row is due at once, not after the delay"
    );
    // A stamped row keeps its stamp and its delay.
    let stale = OwnedClaim {
        holder: claim.holder.clone(),
        partitions: vec![],
    };
    assert!(
        owned_claim(&mut conn, &q, &stale).await.is_none(),
        "now stamped for p62"
    );
    db.drop_db().await;
}

/// The guard skips a claim while an acquisition holds the owner row. It
/// never waits for it.
#[tokio::test]
async fn the_guard_skips_while_an_acquisition_holds_the_owner_row() {
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    let q = unique("owner-skip");
    enable(&mut conn).await;
    let tenure = take(&mut conn, "owner-worker", partition(71)).await;
    let wf = workflow_task(&mut conn, &q, Some(partition(71))).await;
    let mut holder_conn = db.conn().await;
    holder_conn
        .batch_execute(
            "BEGIN; SELECT 1 FROM harvest_decision_owner WHERE partition = 71 FOR UPDATE",
        )
        .await
        .expect("hold the owner row");
    let claim = OwnedClaim {
        holder: tenure.holder().as_str().to_string(),
        partitions: vec![partition(71).as_i16()],
    };
    let started = std::time::Instant::now();
    assert!(owned_claim(&mut conn, &q, &claim).await.is_none());
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the claim did not wait"
    );
    assert_eq!(task_state(&mut conn, wf).await, ("PENDING".to_string(), 0));
    holder_conn
        .batch_execute("ROLLBACK")
        .await
        .expect("release");
    assert_eq!(
        owned_claim(&mut conn, &q, &claim).await.map(|t| t.id),
        Some(wf)
    );
    db.drop_db().await;
}

// ── The owner loop ──────────────────────────────────────────────────────────

#[derive(QueryableByName)]
struct HeldRow {
    #[diesel(sql_type = Text)]
    holder: String,
    #[diesel(sql_type = BigInt)]
    n: i64,
}

async fn held_by_worker(conn: &mut AsyncPgConnection) -> BTreeMap<String, i64> {
    let rows: Vec<HeldRow> = diesel::sql_query(
        "SELECT holder, count(*)::bigint AS n FROM harvest_decision_owner \
         WHERE holder IS NOT NULL AND lease_until > clock_timestamp() GROUP BY holder",
    )
    .load(conn)
    .await
    .expect("read the holders");
    rows.into_iter()
        .map(|r| {
            let worker = r
                .holder
                .rsplit_once('/')
                .map_or_else(|| r.holder.clone(), |(w, _)| w.to_string());
            (worker, r.n)
        })
        .collect()
}

fn new_loop(worker: &str) -> (OwnerLoop, Arc<OwnedPartitions>) {
    let owned = Arc::new(OwnedPartitions::default());
    let owner_loop = OwnerLoop::new(
        ShardId::UNENCODED,
        worker,
        OwnerLeaseConfig::default(),
        Arc::clone(&owned),
    );
    (owner_loop, owned)
}

async fn tick(owner_loop: &mut OwnerLoop, owned: &OwnedPartitions, conn: &mut AsyncPgConnection) {
    let tick = owner_loop.tick(conn).await.expect("tick");
    for (tenure, deadline) in tick.acquired {
        owned.adopt_until(tenure, deadline);
    }
}

/// Workers rebalance on join and leave (ADR 0009 decision 9).
#[tokio::test]
async fn owner_loops_rebalance_on_join_and_leave() {
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    enable(&mut conn).await;
    let (mut a, owned_a) = new_loop("loop-a");

    tick(&mut a, &owned_a, &mut conn).await;
    assert_eq!(owned_a.len(), 512, "a lone worker owns every partition");
    assert!(owned_a.owner_mode(ShardId::UNENCODED));
    let claim = owned_a
        .owned_claim(ShardId::UNENCODED)
        .expect("owned claims");
    assert_eq!(claim.partitions.len(), 512);

    // Worker B joins. Its first tick makes it a candidate.
    let (mut b, owned_b) = new_loop("loop-b");
    tick(&mut b, &owned_b, &mut conn).await;
    assert!(owned_b.is_empty(), "no partition is free yet");
    tick(&mut a, &owned_a, &mut conn).await;
    assert_eq!(
        owned_a.len(),
        256,
        "A stops claiming the extra partitions at once"
    );
    assert_eq!(
        held_by_worker(&mut conn).await.get("loop-a"),
        Some(&512),
        "and releases them next tick"
    );
    tick(&mut a, &owned_a, &mut conn).await;
    tick(&mut b, &owned_b, &mut conn).await;
    assert_eq!(owned_b.len(), 256, "B acquires the released partitions");
    let held = held_by_worker(&mut conn).await;
    assert_eq!(held.get("loop-a"), Some(&256), "{held:?}");
    assert_eq!(held.get("loop-b"), Some(&256), "{held:?}");

    // Worker B leaves gracefully.
    let released = b.release_everything(&mut conn).await.expect("release");
    assert_eq!(released.len(), 256);
    let after = b.tick(&mut conn).await.expect("a stopped loop");
    assert!(
        after.acquired.is_empty() && !after.owner_mode,
        "a stopped loop does nothing"
    );
    tick(&mut a, &owned_a, &mut conn).await;
    assert_eq!(owned_a.len(), 512, "A takes the partitions back");
    db.drop_db().await;
}

/// A refused loop is a participant. It claims in the owned form, so the
/// guard never blocks it. It is no candidate and owns nothing.
#[tokio::test]
async fn a_refused_loop_claims_in_the_owned_form_and_owns_nothing() {
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    enable(&mut conn).await;
    let (mut a, owned_a) = new_loop("loop-refused");
    a.refuse();
    tick(&mut a, &owned_a, &mut conn).await;
    assert!(owned_a.is_empty());
    let claim = owned_a
        .owned_claim(ShardId::UNENCODED)
        .expect("owned claims");
    assert_eq!(claim.partitions, Vec::<i16>::new());
    let candidates: CountRow =
        diesel::sql_query("SELECT count(*)::bigint AS n FROM harvest_decision_owner_candidates")
            .get_result(&mut conn)
            .await
            .expect("count candidates");
    assert_eq!(candidates.n, 0, "a refused loop is no candidate");
    // Its activity claim passes a workflow row at the queue head.
    let q = unique("owner-refused");
    let _wf = workflow_task(&mut conn, &q, Some(partition(91))).await;
    let act = activity_task(&mut conn, &q).await;
    let got = owned_claim(&mut conn, &q, &claim).await.map(|t| t.id);
    assert_eq!(got, Some(act));
    db.drop_db().await;
}

/// Enable refuses while no owner loop is live, so no workflow task waits for
/// an owner that does not exist.
#[tokio::test]
async fn enable_refuses_while_no_owner_loop_is_live() {
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    let refused = decision_owner::enable_owner_mode(&mut conn, &EnableOwnerMode::default()).await;
    let message = refused.expect_err("no candidate").to_string();
    assert!(message.contains("no owner loop is live"), "{message}");
    let (mut a, owned_a) = new_loop("loop-first");
    tick(&mut a, &owned_a, &mut conn).await;
    assert!(
        owned_a.is_empty(),
        "the switch is off, so the loop only registers"
    );
    decision_owner::enable_owner_mode(&mut conn, &EnableOwnerMode::default())
        .await
        .expect("a live candidate lets enable pass");
    db.drop_db().await;
}

/// A stale local list names a partition that another holder owns. The
/// claim skips that row and takes the next owned row. It does not return
/// empty.
#[tokio::test]
async fn a_stale_partition_list_skips_the_row_and_takes_the_next() {
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    let q = unique("owner-stale-list");
    enable(&mut conn).await;
    let mine = take(&mut conn, "owner-worker", partition(101)).await;
    let _theirs = take(&mut conn, "other-worker", partition(102)).await;
    let head = workflow_task(&mut conn, &q, Some(partition(102))).await;
    let next = workflow_task(&mut conn, &q, Some(partition(101))).await;
    let stale = OwnedClaim {
        holder: mine.holder().as_str().to_string(),
        partitions: vec![partition(101).as_i16(), partition(102).as_i16()],
    };
    assert_eq!(
        owned_claim(&mut conn, &q, &stale).await.map(|t| t.id),
        Some(next)
    );
    assert_eq!(
        task_state(&mut conn, head).await,
        ("PENDING".to_string(), 0)
    );
    db.drop_db().await;
}

/// An acquisition re-pends each workflow task that another worker holds
/// `RUNNING` in the partition (ADR 0009 decision 7).
#[tokio::test]
async fn an_acquisition_repends_a_task_another_worker_left_running() {
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    let q = unique("owner-foreign");
    enable(&mut conn).await;
    let old = take(&mut conn, "old-owner", partition(81)).await;
    let wf = workflow_task(&mut conn, &q, Some(partition(81))).await;
    let claim = OwnedClaim {
        holder: old.holder().as_str().to_string(),
        partitions: vec![partition(81).as_i16()],
    };
    let task = owned_claim(&mut conn, &q, &claim)
        .await
        .expect("the old owner claims");
    assert_eq!(task.id, wf);
    // The old owner's lease lapses with the task still RUNNING.
    diesel::sql_query(
        "UPDATE harvest_decision_owner SET lease_until = clock_timestamp() - interval '1 minute'",
    )
    .execute(&mut conn)
    .await
    .expect("lapse every lease");
    let (mut a, owned_a) = new_loop("new-owner");
    tick(&mut a, &owned_a, &mut conn).await;
    assert_eq!(owned_a.len(), 512);
    let (state, attempt) = task_state(&mut conn, wf).await;
    assert_eq!(
        state, "PENDING",
        "the acquisition re-pends the claim of a dead worker"
    );
    assert_eq!(attempt, 1);
    db.drop_db().await;
}

/// An acquisition leaves the claim of a live worker alone. A live legacy
/// worker finishes its decision and parks the task. A live old owner fails
/// its fence and re-pends its own claim.
#[tokio::test]
async fn an_acquisition_keeps_the_claim_of_a_live_worker() {
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    let q = unique("owner-live");
    let wf = workflow_task(&mut conn, &q, Some(partition(111))).await;
    register_worker(&mut conn, "live-legacy", true).await;
    let task = queue::claim_task(
        &mut conn,
        std::slice::from_ref(&q),
        "live-legacy",
        "",
        None,
        &[],
        &[],
    )
    .await
    .expect("claim")
    .expect("the live legacy worker claims");
    assert_eq!(task.id, wf);
    enable(&mut conn).await;
    let (mut a, owned_a) = new_loop("new-owner");
    tick(&mut a, &owned_a, &mut conn).await;
    assert_eq!(owned_a.len(), 512);
    assert_eq!(task_state(&mut conn, wf).await, ("RUNNING".to_string(), 1));
    db.drop_db().await;
}

/// A tick reports when a foreign partition next becomes free or an orphan.
/// The worker then scans at that time, so a takeover follows a lease end by
/// milliseconds (issue #2254).
#[tokio::test]
async fn a_tick_reports_the_next_foreign_lease_change() {
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    enable(&mut conn).await;
    let _other = take(&mut conn, "other-owner", partition(7)).await;
    let set_lease = async |conn: &mut AsyncPgConnection, offset: &str| {
        diesel::sql_query(format!(
            "UPDATE harvest_decision_owner \
             SET lease_until = clock_timestamp() + interval '{offset}' WHERE partition = 7"
        ))
        .execute(conn)
        .await
        .expect("move the lease end");
    };
    set_lease(&mut conn, "60 seconds").await;
    let (mut a, owned_a) = new_loop("loop-next");
    let report = a.tick(&mut conn).await.expect("tick");
    let next = report.next_change.expect("a foreign lease");
    assert!(
        next > Duration::from_secs(30) && next <= Duration::from_secs(60),
        "the live lease ends next: {next:?}"
    );
    for (tenure, deadline) in report.acquired {
        owned_a.adopt_until(tenure, deadline);
    }
    assert_eq!(owned_a.len(), 511, "its own rows never count");

    // A lapsed lease becomes an orphan one TTL after its end.
    set_lease(&mut conn, "-1 second").await;
    let report = a.tick(&mut conn).await.expect("tick");
    let orphan = OwnerLeaseTtl::DEFAULT.orphan_after();
    let next = report.next_change.expect("a lapsed foreign lease");
    assert!(
        next < orphan && next > orphan / 2,
        "the orphan time is next: {next:?}"
    );
    assert_eq!(report.acquired.len(), 1, "a lone owner takes the free row");
    for (tenure, deadline) in report.acquired {
        owned_a.adopt_until(tenure, deadline);
    }
    let report = a.tick(&mut conn).await.expect("tick");
    assert_eq!(report.next_change, None, "no foreign row is left");
    db.drop_db().await;
}

/// With a chunk, a tick acquires at most that many partitions and asks
/// for another scan at once while free partitions remain (issue #2254).
#[tokio::test]
async fn a_chunked_loop_acquires_in_steps_and_asks_for_more() {
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    enable(&mut conn).await;
    let owned = Arc::new(OwnedPartitions::default());
    let mut chunked = OwnerLoop::new(
        ShardId::UNENCODED,
        "loop-chunked",
        OwnerLeaseConfig::default(),
        Arc::clone(&owned),
    )
    .with_acquire_chunk(200);
    let mut steps = Vec::new();
    loop {
        let report = chunked.tick(&mut conn).await.expect("tick");
        steps.push((report.acquired.len(), report.more));
        for (tenure, deadline) in report.acquired {
            owned.adopt_until(tenure, deadline);
        }
        if !report.more {
            break;
        }
    }
    assert_eq!(steps, vec![(200, true), (200, true), (112, false)]);
    assert_eq!(owned.len(), 512);
    db.drop_db().await;
}

/// The number of transactions that last wrote the owner rows of `ps`.
///
/// Each row version keeps the id of the transaction that wrote it, as
/// `xmin`.
async fn owner_writers(conn: &mut AsyncPgConnection, ps: &[i32]) -> i64 {
    #[derive(QueryableByName)]
    struct Writers {
        #[diesel(sql_type = BigInt)]
        n: i64,
    }
    diesel::sql_query(
        "SELECT count(DISTINCT xmin::text)::bigint AS n \
         FROM harvest_decision_owner WHERE partition = ANY($1)",
    )
    .bind::<diesel::sql_types::Array<Integer>, _>(ps)
    .get_result::<Writers>(conn)
    .await
    .expect("read the row versions")
    .n
}

/// Issue #2296: a joiner takes each chunk of partitions in one
/// transaction, not in one transaction per partition.
#[tokio::test]
async fn a_joiner_takes_each_chunk_in_one_transaction() {
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    enable(&mut conn).await;
    let owned = Arc::new(OwnedPartitions::default());
    let mut joiner = OwnerLoop::new(
        ShardId::UNENCODED,
        "loop-batch",
        OwnerLeaseConfig::default(),
        Arc::clone(&owned),
    )
    .with_acquire_chunk(200);
    let mut more = true;
    while more {
        let report = joiner.tick(&mut conn).await.expect("tick");
        more = report.more;
        let chunk: Vec<i32> = report
            .acquired
            .iter()
            .map(|(t, _)| i32::from(t.partition().get()))
            .collect();
        assert!(!chunk.is_empty(), "each tick takes a chunk");
        assert_eq!(owner_writers(&mut conn, &chunk).await, 1, "one transaction");
        for (tenure, deadline) in report.acquired {
            owned.adopt_until(tenure, deadline);
        }
    }
    assert_eq!(owned.len(), 512);
    db.drop_db().await;
}

/// Issue #2296: a batch that names a missing row fails, and it takes no
/// other row. The batch checks its rows before its one write.
#[tokio::test]
async fn a_batch_acquisition_of_a_missing_row_takes_no_row() {
    #[derive(QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
        holder: Option<String>,
        #[diesel(sql_type = BigInt)]
        epoch: i64,
    }
    async fn row_6(conn: &mut AsyncPgConnection) -> Row {
        diesel::sql_query("SELECT holder, epoch FROM harvest_decision_owner WHERE partition = 6")
            .get_result(conn)
            .await
            .expect("read the row")
    }
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    diesel::sql_query("DELETE FROM harvest_decision_owner WHERE partition = 7")
        .execute(&mut conn)
        .await
        .expect("drop one owner row");
    let before = row_6(&mut conn).await;
    let ps = [
        DecisionPartition::new(6).expect("in range"),
        DecisionPartition::new(7).expect("in range"),
    ];
    let result = decision_owner::acquire_many(
        &mut conn,
        ShardId::UNENCODED,
        &OwnerHolder::for_worker("missing"),
        &ps,
        &OwnerLeaseConfig::default(),
    )
    .await;
    assert!(
        matches!(result, Err(HarvestError::NotFound(_))),
        "{result:?}"
    );
    let after = row_6(&mut conn).await;
    assert_eq!(after.holder, None, "the batch took no row");
    assert_eq!(after.epoch, before.epoch, "the batch kept the epoch");
    db.drop_db().await;
}

/// A chunked loop rescans at once only below its target. Orphans past the
/// target wait a full scan, so the loop does not take them and then release
/// them on the next tick (issue #2254).
#[tokio::test]
async fn a_chunked_loop_stops_asking_for_more_at_its_target() {
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    enable(&mut conn).await;
    let owned = Arc::new(OwnedPartitions::default());
    let mut chunked = OwnerLoop::new(
        ShardId::UNENCODED,
        "loop-target",
        OwnerLeaseConfig::default(),
        Arc::clone(&owned),
    )
    .with_acquire_chunk(64);
    // A live peer that never acquires. Each fresh row is an orphan.
    let mut more = true;
    while more {
        diesel::sql_query(
            "INSERT INTO harvest_decision_owner_candidates (holder, seen_at) \
             VALUES ('idle-peer/0', clock_timestamp()) \
             ON CONFLICT (holder) DO UPDATE SET seen_at = EXCLUDED.seen_at",
        )
        .execute(&mut conn)
        .await
        .expect("keep the peer live");
        let report = chunked.tick(&mut conn).await.expect("tick");
        more = report.more;
        for (tenure, deadline) in report.acquired {
            owned.adopt_until(tenure, deadline);
        }
        assert!(owned.len() <= 256, "a chunk never passes the target");
    }
    assert_eq!(owned.len(), 256, "the loop stops at its target of 256");
    db.drop_db().await;
}

#[tokio::test]
async fn an_owner_loop_releases_everything_when_the_switch_goes_off() {
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    enable(&mut conn).await;
    let (mut a, owned_a) = new_loop("loop-off");
    tick(&mut a, &owned_a, &mut conn).await;
    assert_eq!(owned_a.len(), 512);
    decision_owner::disable_owner_mode(&mut conn, Duration::from_secs(5))
        .await
        .expect("disable");
    let report = a.tick(&mut conn).await.expect("tick");
    assert!(!report.owner_mode);
    assert_eq!(report.dropped.len(), 512);
    assert!(owned_a.is_empty());
    assert!(owned_a.owned_claim(ShardId::UNENCODED).is_none());
    assert!(held_by_worker(&mut conn).await.is_empty());
    let key = OwnerKey {
        shard: ShardId::UNENCODED,
        partition: partition(0),
    };
    assert_eq!(owned_a.pin_for(key), decision_owner::DecisionPin::Legacy);
    db.drop_db().await;
}

// ── Workers ─────────────────────────────────────────────────────────────────

#[activity(start_to_close = "30s")]
async fn owner_step_a(_ctx: &ActivityContext, pause_ms: u64) -> Result<serde_json::Value, String> {
    tokio::time::sleep(Duration::from_millis(pause_ms)).await;
    Ok(serde_json::Value::Null)
}

#[activity(start_to_close = "30s")]
async fn owner_step_b(_ctx: &ActivityContext, pause_ms: u64) -> Result<serde_json::Value, String> {
    tokio::time::sleep(Duration::from_millis(pause_ms)).await;
    Ok(serde_json::Value::Null)
}

#[activity(start_to_close = "30s")]
async fn owner_step_c(_ctx: &ActivityContext, pause_ms: u64) -> Result<serde_json::Value, String> {
    tokio::time::sleep(Duration::from_millis(pause_ms)).await;
    Ok(serde_json::Value::Null)
}

/// Three activities in sequence, as the e2e throughput bench runs. Each run
/// makes four decisions.
#[workflow]
async fn owner_three_steps(ctx: &WorkflowContext, input: (String, u64)) -> Result<String, String> {
    let (queue_name, pause_ms) = input;
    for name in STEPS {
        ctx.execute_activity_raw(name, serde_json::json!(pause_ms), &queue_name)
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok("done".to_string())
}

/// Three one-second timers in sequence. Each run makes four decisions. A
/// timer fires after a short sticky window, and no wake re-arms the pin.
#[workflow]
async fn owner_three_timers(ctx: &WorkflowContext, input: String) -> Result<String, String> {
    let _ = input;
    for name in ["owner_t1", "owner_t2", "owner_t3"] {
        ctx.timer(name, 1).await.map_err(|e| e.to_string())?;
    }
    Ok("done".to_string())
}

/// Counts each resident outcome of one worker.
#[derive(Default, Clone)]
struct Outcomes(Arc<Mutex<BTreeMap<String, u64>>>);

impl Outcomes {
    fn snapshot(&self) -> BTreeMap<String, u64> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl MetricsRecorder for Outcomes {
    fn record_workflow_resident(&self, _workflow: &str, _queue: &str, outcome: ResidentOutcome) {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(outcome.as_str().to_string())
            .or_default() += 1;
    }
}

struct Spec {
    claim_loops: usize,
    sticky: Duration,
}

fn build_worker(queue_name: &str, outcomes: &Outcomes, spec: &Spec) -> Arc<Worker> {
    let built = HarvestBuilder::new()
        .workflows(workflows![owner_three_steps, owner_three_timers])
        .activities(activities![owner_step_a, owner_step_b, owner_step_c])
        .worker(
            WorkerConfig::default()
                .with_queues([queue_name])
                .with_sticky_routing(autumn_harvest::StickyRoutingConfig {
                    lease_ttl: spec.sticky,
                }),
        )
        .telemetry(
            TelemetryConfig::builder()
                .metrics(Arc::new(outcomes.clone()))
                .build(),
        )
        .build();
    let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
    let mut runtime: WorkerRuntimeConfig = worker_config.into();
    runtime.worker_id = unique("owner-claim-worker");
    runtime.poll_interval = Duration::from_millis(25);
    runtime.max_concurrent_workflows = 8;
    runtime.max_concurrent_activities = 16;
    runtime.max_concurrent_claims = spec.claim_loops;
    runtime.worker_heartbeat_interval = Duration::from_secs(1);
    Arc::new(Worker::new(runtime, Arc::new(registry)).expect("worker"))
}

/// Run `runs` workflows with at most `inflight` at once.
async fn run_workflows(
    url: &str,
    pool: &DbPool,
    queue_name: &str,
    runs: usize,
    inflight: usize,
    pause_ms: u64,
) {
    let client = WorkflowHandleClient::single(pool.clone(), url.to_string());
    let mut conn = AsyncPgConnection::establish(url).await.expect("connect");
    let mut started = 0;
    let mut waiting: std::collections::VecDeque<ExecutionId> = std::collections::VecDeque::new();
    while started < runs || !waiting.is_empty() {
        while started < runs && waiting.len() < inflight {
            let opts = TypedStartOptions {
                queue_name: Some(queue_name.to_string()),
                ..Default::default()
            };
            let exec = OwnerThreeStepsStub::start_with_options(
                &mut conn,
                &client,
                &unique("owner-claim-wf"),
                (queue_name.to_string(), pause_ms),
                opts,
            )
            .await
            .expect("start")
            .exec_id();
            started += 1;
            waiting.push_back(exec);
        }
        if let Some(exec) = waiting.pop_front() {
            wait_for_execution_state_with_timeout(url, exec, "COMPLETED", Duration::from_secs(90))
                .await;
        }
    }
}

/// Start `runs` timer workflows at once and wait for each.
async fn run_timer_workflows(url: &str, pool: &DbPool, queue_name: &str, runs: usize) {
    let client = WorkflowHandleClient::single(pool.clone(), url.to_string());
    let mut conn = AsyncPgConnection::establish(url).await.expect("connect");
    let mut started = Vec::with_capacity(runs);
    for _ in 0..runs {
        let opts = TypedStartOptions {
            queue_name: Some(queue_name.to_string()),
            ..Default::default()
        };
        let exec = OwnerThreeTimersStub::start_with_options(
            &mut conn,
            &client,
            &unique("owner-timer-wf"),
            String::new(),
            opts,
        )
        .await
        .expect("start")
        .exec_id();
        started.push(exec);
    }
    for exec in started {
        wait_for_execution_state_with_timeout(url, exec, "COMPLETED", Duration::from_secs(90))
            .await;
    }
}

/// Wait until owners hold every partition, `n` each at least.
async fn wait_for_owners(conn: &mut AsyncPgConnection, workers: usize) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let held = held_by_worker(conn).await;
        let total: i64 = held.values().sum();
        if held.len() == workers && total == 512 {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "owners did not settle: {held:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// One worker with two claim loops runs `runs` workflows. Returns its
/// resident outcomes.
async fn two_loop_outcomes(owner_mode: bool, runs: usize) -> BTreeMap<String, u64> {
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    if owner_mode {
        enable(&mut conn).await;
    }
    let pool: DbPool = build_test_pool(&db.url);
    let q = unique("owner-cold");
    let outcomes = Outcomes::default();
    let spec = Spec {
        claim_loops: 2,
        sticky: autumn_harvest::builder::DEFAULT_STICKY_TIMEOUT,
    };
    let worker = build_worker(&q, &outcomes, &spec);
    let handle = spawn_test_worker(Arc::clone(&worker), pool.clone());
    if owner_mode {
        wait_for_owners(&mut conn, 1).await;
    }
    run_workflows(&db.url, &pool, &q, runs, 32, 0).await;
    worker.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
    db.drop_db().await;
    outcomes.snapshot()
}

/// Done-when item 1: with two claim loops and owner mode on, the worker
/// makes no extra cold decision. Each run makes exactly four decisions, and
/// only the first decision of each run is cold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_claim_loops_in_owner_mode_make_no_extra_cold_decisions() {
    let runs = 200;
    let seen = two_loop_outcomes(true, runs).await;
    let total: u64 = seen.values().sum();
    let runs = u64::try_from(runs).expect("fits");
    assert_eq!(total, 4 * runs, "four decisions per run: {seen:?}");
    assert_eq!(seen.get("cold").copied().unwrap_or(0), runs, "{seen:?}");
    assert_eq!(seen.get("hit").copied().unwrap_or(0), 3 * runs, "{seen:?}");
}

/// The legacy measurement of the test above. The extra decisions depend on
/// timing, so it prints the counts and asserts nothing. The e2e bench
/// records them in a release build.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement, not a gate"]
async fn two_claim_loops_without_owner_mode_measurement() {
    let seen = two_loop_outcomes(false, 200).await;
    eprintln!("legacy mode, two claim loops: {seen:?}");
}

/// Two workers, two claim loops each, and timers longer than a 200 ms
/// sticky window. Returns the resident outcomes of both workers and the
/// number of runs.
async fn sticky_window_outcomes(owner_mode: bool) -> (BTreeMap<String, u64>, Vec<u64>, u64) {
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    if owner_mode {
        enable(&mut conn).await;
    }
    let pool: DbPool = build_test_pool(&db.url);
    let q = unique("owner-window");
    let spec = Spec {
        claim_loops: 2,
        sticky: Duration::from_millis(200),
    };
    let outcomes = [Outcomes::default(), Outcomes::default()];
    let workers: Vec<Arc<Worker>> = outcomes
        .iter()
        .map(|o| build_worker(&q, o, &spec))
        .collect();
    let handles: Vec<_> = workers
        .iter()
        .map(|w| spawn_test_worker(Arc::clone(w), pool.clone()))
        .collect();
    if owner_mode {
        wait_for_owners(&mut conn, 2).await;
    }

    let runs = 24;
    run_timer_workflows(&db.url, &pool, &q, runs).await;
    for worker in &workers {
        worker.shutdown();
    }
    for handle in handles {
        let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
    }
    let mut seen: BTreeMap<String, u64> = BTreeMap::new();
    let mut per_worker = Vec::new();
    for o in &outcomes {
        let snapshot = o.snapshot();
        per_worker.push(snapshot.values().sum());
        for (k, v) in snapshot {
            *seen.entry(k).or_default() += v;
        }
    }
    db.drop_db().await;
    (seen, per_worker, u64::try_from(runs).expect("fits"))
}

/// The hits over the decisions after the first of each run.
fn continuation_hits(seen: &BTreeMap<String, u64>, runs: u64) -> (u64, u64) {
    let total: u64 = seen.values().sum();
    (
        seen.get("hit").copied().unwrap_or(0),
        total.saturating_sub(runs),
    )
}

/// Done-when item 2: resident hits survive the sticky window. Each timer
/// fires after the window. Two workers own half of the partitions
/// each. Every decision after the first of a run resumes on its owner.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resident_hits_survive_the_sticky_window_in_owner_mode() {
    let (seen, per_worker, runs) = sticky_window_outcomes(true).await;
    eprintln!("owner mode, outcomes across the sticky window: {seen:?}");
    let total: u64 = seen.values().sum();
    assert_eq!(total, 4 * runs, "four decisions per run: {seen:?}");
    let (hits, continuations) = continuation_hits(&seen, runs);
    assert!(
        hits * 100 >= continuations * 95,
        "continuation hit rate below 95%: {seen:?}"
    );
    assert!(
        per_worker.iter().all(|n| *n > 0),
        "both owners decide: {per_worker:?}"
    );
}

/// The control for the test above: with sticky routing alone, a decision
/// after the window lands on either worker, so many replay cold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resident_hits_drop_after_the_sticky_window_without_owner_mode() {
    let (seen, _per_worker, runs) = sticky_window_outcomes(false).await;
    eprintln!("legacy mode, outcomes across the sticky window: {seen:?}");
    let (hits, continuations) = continuation_hits(&seen, runs);
    assert!(
        hits * 100 < continuations * 95,
        "the control should miss: {seen:?}"
    );
}
