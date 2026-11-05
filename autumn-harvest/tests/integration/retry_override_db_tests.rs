#![cfg(feature = "db")]
//! The workflow retry policy override (issue #2409).
//!
//! These tests check the store, the table checks and each acceptance
//! criterion against Postgres. A worker drives each failure. The failure path
//! reads the override through `GLOBAL_RUN_DEFAULTS`, and each registry that a
//! test builds replaces that global. So the tests hold the serial lock of
//! `quota_enforcement_tests`, which the other suites that change the global
//! hold too. CI also runs this module with one test thread.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! these against a shared local cluster. Otherwise each test starts a fresh
//! testcontainers Postgres 16. Each test names its workflow types with a
//! fresh suffix, so a shared cluster needs no scrub.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::execution::{RerunRequest, rerun_workflow_execution};
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::policy::{JitterPolicy, RetryPolicy};
use autumn_harvest::run_defaults::{
    MAX_RETRY_ATTEMPTS, MAX_RETRY_INTERVAL, RunDefaultChange, RunDefaultPatch,
    get_run_default_override, load_run_default_overrides, update_run_default_override,
};
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor, tick_once};
use autumn_harvest::schema::{
    harvest_schedules, harvest_schedules_ext, harvest_task_queue, harvest_workflow_executions,
    harvest_workflow_executions_ext,
};
use autumn_harvest::store;
use autumn_harvest::table_split::Ext;
use autumn_harvest::worker::HandlerRegistry;
use autumn_harvest::workflow_retry::{
    RetryLimits, RetryLimitsPatch, RetryPolicySource, SCHEDULE_SOURCE, SOURCE_KEY, encode_stored,
};
use autumn_harvest::{
    ExecutionId, StartWorkflowParams, WorkflowContext, cancel_workflow_execution,
    start_or_load_workflow_execution,
};
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::Value;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use uuid::Uuid;

use crate::integration_e2e::{
    build_runtime_worker, build_test_pool, setup_test_database_url_or_env, spawn_test_worker,
};
use crate::quota_enforcement_tests::{leaked, serial};

/// A retry delay above the cache TTL plus one refresh period.
///
/// A refresh that runs while a test writes an override can bring back the
/// old snapshot. The next refresh corrects it within this delay. So a test
/// that changes the override mid-chain waits out this delay first.
const PAST_REFRESH: Duration = Duration::from_secs(8);

/// The number of calls of [`fail_once_then_succeed`].
static FAIL_ONCE_CALLS: AtomicUsize = AtomicUsize::new(0);

/// [`block_then_complete`] sets this when it starts.
static ENTERED: AtomicBool = AtomicBool::new(false);

/// [`block_then_complete`] waits for this.
static RELEASE: AtomicBool = AtomicBool::new(false);

type HandlerFuture<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>;

fn always_fail(_ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
    Box::pin(async move { Err("transient".to_string()) })
}

fn fail_once_then_succeed(_ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
    Box::pin(async move {
        if FAIL_ONCE_CALLS.fetch_add(1, Ordering::SeqCst) == 0 {
            Err("transient".to_string())
        } else {
            Ok(serde_json::json!("done"))
        }
    })
}

fn block_then_complete(_ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
    Box::pin(async move {
        ENTERED.store(true, Ordering::SeqCst);
        while !AtomicBool::load(&RELEASE, Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok(serde_json::json!("done"))
    })
}

/// A policy with no jitter, so each delay is exact.
const fn policy(max_attempts: u32, interval: Duration) -> RetryPolicy {
    RetryPolicy {
        max_attempts,
        initial_interval: interval,
        backoff_coefficient: 1.0,
        max_interval: interval,
        non_retryable_errors: Vec::new(),
        jitter: JitterPolicy::None,
    }
}

fn wf_info(
    name: &'static str,
    handler: autumn_harvest::info::WorkflowHandlerFn,
    retry_policy: Option<RetryPolicy>,
) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "retry_override_db_tests",
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
        retry_policy,
    }
}

struct Db {
    url: String,
    _container: Option<ContainerAsync<Postgres>>,
}

impl Db {
    async fn new() -> Self {
        let (url, container) = setup_test_database_url_or_env().await;
        Self {
            url,
            _container: container,
        }
    }

    async fn conn(&self) -> AsyncPgConnection {
        AsyncPgConnection::establish(&self.url)
            .await
            .expect("connect")
    }
}

async fn store_change(
    db: &Db,
    workflow_name: &str,
    retry: RetryLimitsPatch,
    clear: bool,
) -> autumn_harvest::HarvestResult<Option<autumn_harvest::run_defaults::RunDefaultOverride>> {
    let mut conn = db.conn().await;
    update_run_default_override(
        &mut conn,
        &RunDefaultChange {
            workflow_name,
            patch: RunDefaultPatch {
                retry,
                ..RunDefaultPatch::default()
            },
            clear,
            actor: "retry-test",
            reason: Some("INC-2409"),
        },
    )
    .await
}

/// Store a change, then write it through to `registry`, as the route does.
async fn change(
    db: &Db,
    registry: Option<&HandlerRegistry>,
    workflow_name: &str,
    retry: RetryLimitsPatch,
    clear: bool,
) {
    let stored = store_change(db, workflow_name, retry, clear)
        .await
        .expect("store the override");
    if let Some(registry) = registry {
        registry.run_defaults().write_through(
            workflow_name,
            stored.map(|value| value.fields).unwrap_or_default(),
        );
    }
}

const fn set_attempts(n: u32) -> RetryLimitsPatch {
    RetryLimitsPatch {
        max_attempts: Some(Some(n)),
        initial_interval: None,
        max_interval: None,
    }
}

/// Start one run of `name` with `policy` frozen on its row.
async fn start(
    conn: &mut AsyncPgConnection,
    name: &'static str,
    policy: Option<RetryPolicy>,
    source: RetryPolicySource,
) -> ExecutionId {
    let workflow_id = format!("{name}-{}", Uuid::new_v4());
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_retry_policy: policy,
            workflow_retry_source: source,
            ..StartWorkflowParams::new(
                name,
                &workflow_id,
                ExecutionId::new(),
                Value::Null,
                "default",
            )
        },
        None,
    )
    .await
    .expect("start")
    .exec_id
}

#[derive(Debug)]
struct Row {
    id: Uuid,
    state: String,
    attempt: i32,
    policy: Option<Value>,
}

impl Row {
    const fn exec_id(&self) -> ExecutionId {
        ExecutionId::from_uuid(self.id)
    }

    fn source(&self) -> Option<&str> {
        self.policy
            .as_ref()
            .and_then(|value| value.get(SOURCE_KEY))
            .and_then(Value::as_str)
    }

    fn attempts(&self) -> u64 {
        self.policy
            .as_ref()
            .and_then(|value| value.get("max_attempts"))
            .and_then(Value::as_u64)
            .expect("a stored policy")
    }

    fn terminal(&self) -> bool {
        matches!(self.state.as_str(), "COMPLETED" | "FAILED" | "CANCELLED")
    }
}

/// Every run of `name`, by attempt.
async fn runs(conn: &mut AsyncPgConnection, name: &str) -> Vec<Row> {
    use harvest_workflow_executions::dsl;
    use harvest_workflow_executions_ext as ext;
    let rows: Vec<(Uuid, String, i32, Option<Value>)> = dsl::harvest_workflow_executions
        .filter(dsl::workflow_name.eq(name))
        .order(ext::workflow_attempt.asc())
        .select((
            dsl::id,
            dsl::state,
            ext::workflow_attempt,
            ext::workflow_retry_policy,
        ))
        .load(conn)
        .await
        .expect("load runs");
    rows.into_iter()
        .map(|(id, state, attempt, policy)| Row {
            id,
            state,
            attempt,
            policy,
        })
        .collect()
}

/// The fire time of the workflow task of one run.
async fn fire_time(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> DateTime<Utc> {
    use harvest_task_queue::dsl;
    dsl::harvest_task_queue
        .filter(dsl::workflow_exec_id.eq(exec_id.as_uuid()))
        .filter(dsl::task_type.eq("workflow"))
        .select(dsl::scheduled_at)
        .first(conn)
        .await
        .expect("the workflow task")
}

/// Wait until `done` holds for the runs of `name`, then return them.
async fn wait_runs(db: &Db, name: &str, what: &str, done: impl Fn(&[Row]) -> bool) -> Vec<Row> {
    let mut conn = db.conn().await;
    for _ in 0..600 {
        let rows = runs(&mut conn, name).await;
        if done(&rows) {
            return rows;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{what}: runs of {name} = {:?}", runs(&mut conn, name).await);
}

/// Give a worker time to start a retry that it should not start.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(1500)).await;
}

/// A worker for a registry, and its task.
struct Running {
    worker: Arc<autumn_harvest::worker::Worker>,
    task: tokio::task::JoinHandle<()>,
}

impl Running {
    fn registry(workflows: Vec<WorkflowInfo>) -> Arc<HandlerRegistry> {
        Arc::new(HandlerRegistry::new(workflows, vec![]))
    }

    fn spawn(db: &Db, registry: &Arc<HandlerRegistry>) -> Self {
        let worker = build_runtime_worker(
            &format!("retry-override-{}", Uuid::new_v4()),
            4,
            4,
            Arc::clone(registry),
        );
        let task = spawn_test_worker(Arc::clone(&worker), build_test_pool(&db.url));
        Self { worker, task }
    }

    async fn stop(self) {
        self.worker.shutdown();
        let _ = tokio::time::timeout(Duration::from_secs(10), self.task).await;
    }
}

// ── The store ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn the_store_round_trips_the_retry_fields() {
    let _serial = serial().await;
    let db = Db::new().await;
    let name = leaked("ro_store_round_trip");
    change(
        &db,
        None,
        name,
        RetryLimitsPatch {
            max_attempts: Some(Some(2)),
            initial_interval: Some(Some(Duration::from_millis(1_500))),
            max_interval: Some(Some(Duration::from_secs(90))),
        },
        false,
    )
    .await;
    let mut conn = db.conn().await;
    let stored = get_run_default_override(&mut conn, name)
        .await
        .expect("read")
        .expect("a row");
    assert_eq!(
        stored.fields.retry,
        RetryLimits {
            max_attempts: Some(2),
            initial_interval: Some(Duration::from_millis(1_500)),
            max_interval: Some(Duration::from_secs(90)),
        }
    );
    assert_eq!(stored.reason.as_deref(), Some("INC-2409"));
    let all = load_run_default_overrides(&mut conn).await.expect("load");
    assert!(all.iter().any(|value| value.workflow_name == name));

    // A null returns one field to the code value and keeps the rest.
    change(
        &db,
        None,
        name,
        RetryLimitsPatch {
            initial_interval: Some(None),
            ..RetryLimitsPatch::default()
        },
        false,
    )
    .await;
    let stored = get_run_default_override(&mut conn, name)
        .await
        .expect("read")
        .expect("a row");
    assert_eq!(stored.fields.retry.max_attempts, Some(2));
    assert_eq!(stored.fields.retry.initial_interval, None);
    assert_eq!(
        stored.fields.retry.max_interval,
        Some(Duration::from_secs(90))
    );

    // A clear deletes the row.
    change(&db, None, name, RetryLimitsPatch::default(), true).await;
    assert!(
        get_run_default_override(&mut conn, name)
            .await
            .expect("read")
            .is_none()
    );
}

#[tokio::test]
async fn the_write_refuses_a_retry_value_that_no_failure_can_use() {
    let _serial = serial().await;
    let db = Db::new().await;
    let name = leaked("ro_store_refusals");
    let too_long = MAX_RETRY_INTERVAL + Duration::from_millis(1);
    let cases = [
        ("zero attempts", set_attempts(0)),
        ("too many attempts", set_attempts(MAX_RETRY_ATTEMPTS + 1)),
        (
            "zero first delay",
            RetryLimitsPatch {
                initial_interval: Some(Some(Duration::ZERO)),
                ..RetryLimitsPatch::default()
            },
        ),
        (
            "sub-millisecond cap",
            RetryLimitsPatch {
                max_interval: Some(Some(Duration::from_micros(10))),
                ..RetryLimitsPatch::default()
            },
        ),
        (
            "a first delay past the start-delay limit",
            RetryLimitsPatch {
                initial_interval: Some(Some(too_long)),
                ..RetryLimitsPatch::default()
            },
        ),
        (
            "a cap past the start-delay limit",
            RetryLimitsPatch {
                max_interval: Some(Some(too_long)),
                ..RetryLimitsPatch::default()
            },
        ),
        (
            "a cap below the first delay",
            RetryLimitsPatch {
                initial_interval: Some(Some(Duration::from_secs(60))),
                max_interval: Some(Some(Duration::from_secs(10))),
                ..RetryLimitsPatch::default()
            },
        ),
    ];
    for (case, retry) in cases {
        let result = store_change(&db, name, retry, false).await;
        assert!(
            matches!(result, Err(autumn_harvest::HarvestError::Config(_))),
            "{case}: {result:?}"
        );
    }
    // The limit itself is accepted.
    change(
        &db,
        None,
        name,
        RetryLimitsPatch {
            max_interval: Some(Some(MAX_RETRY_INTERVAL)),
            ..RetryLimitsPatch::default()
        },
        false,
    )
    .await;

    // A merge can put the cap below the first delay too.
    change(
        &db,
        None,
        name,
        RetryLimitsPatch {
            max_interval: Some(Some(Duration::from_secs(10))),
            ..RetryLimitsPatch::default()
        },
        false,
    )
    .await;
    let merged = store_change(
        &db,
        name,
        RetryLimitsPatch {
            initial_interval: Some(Some(Duration::from_secs(60))),
            ..RetryLimitsPatch::default()
        },
        false,
    )
    .await;
    assert!(
        matches!(merged, Err(autumn_harvest::HarvestError::Config(_))),
        "{merged:?}"
    );
    change(&db, None, name, RetryLimitsPatch::default(), true).await;
}

#[tokio::test]
async fn the_table_refuses_a_retry_row_that_the_engine_cannot_read() {
    let _serial = serial().await;
    let db = Db::new().await;
    let mut conn = db.conn().await;
    let name = leaked("ro_table_check");
    let past = MAX_RETRY_INTERVAL.as_millis() + 1;
    for (case, columns, values) in [
        ("zero attempts", "retry_max_attempts", "0".to_string()),
        (
            "zero first delay",
            "retry_initial_interval_ms",
            "0".to_string(),
        ),
        ("zero cap", "retry_max_interval_ms", "0".to_string()),
        (
            "a first delay past the limit",
            "retry_initial_interval_ms",
            past.to_string(),
        ),
        (
            "a cap past the limit",
            "retry_max_interval_ms",
            past.to_string(),
        ),
        (
            "a cap below the first delay",
            "retry_initial_interval_ms, retry_max_interval_ms",
            "60000, 1000".to_string(),
        ),
    ] {
        let sql = format!(
            "INSERT INTO harvest_run_default_overrides \
             (workflow_name, {columns}, updated_by) VALUES ('{name}', {values}, 't')"
        );
        let result = diesel::sql_query(sql).execute(&mut conn).await;
        assert!(result.is_err(), "{case}: the table must refuse the row");
    }
    // A row that sets only a retry field passes the row check.
    diesel::sql_query(format!(
        "INSERT INTO harvest_run_default_overrides \
         (workflow_name, retry_max_attempts, updated_by) VALUES ('{name}', 1, 't')"
    ))
    .execute(&mut conn)
    .await
    .expect("a retry-only row");
    diesel::sql_query(format!(
        "DELETE FROM harvest_run_default_overrides WHERE workflow_name = '{name}'"
    ))
    .execute(&mut conn)
    .await
    .expect("clean up");
}

// ── AC 1: the next failure of any run, in flight or new ─────────────────

/// The control for the AC tests: with no override, the stored policy
/// retries.
#[tokio::test]
async fn with_no_override_the_stored_policy_retries() {
    let _serial = serial().await;
    let db = Db::new().await;
    let name = leaked("ro_control");
    let code = policy(2, Duration::from_millis(20));
    let registry = Running::registry(vec![wf_info(name, always_fail, Some(code.clone()))]);
    let mut conn = db.conn().await;
    start(&mut conn, name, Some(code), RetryPolicySource::Type).await;
    let running = Running::spawn(&db, &registry);
    let rows = wait_runs(&db, name, "two failed attempts", |rows| {
        rows.len() == 2 && rows.iter().all(Row::terminal)
    })
    .await;
    running.stop().await;
    assert_eq!(rows[1].attempt, 2);
}

#[tokio::test]
async fn ac1_an_override_stops_the_retries_of_a_run_in_flight() {
    let _serial = serial().await;
    let db = Db::new().await;
    let name = leaked("ro_ac1_stop");
    let code = policy(3, Duration::from_millis(20));
    let registry = Running::registry(vec![wf_info(name, always_fail, Some(code.clone()))]);
    let mut conn = db.conn().await;
    // The run starts, and freezes the code policy, before the override.
    let first = start(&mut conn, name, Some(code), RetryPolicySource::Type).await;
    change(&db, Some(&registry), name, set_attempts(1), false).await;

    let running = Running::spawn(&db, &registry);
    wait_runs(&db, name, "the first attempt fails", |rows| {
        rows.first().is_some_and(Row::terminal)
    })
    .await;
    settle().await;
    let rows = runs(&mut conn, name).await;
    running.stop().await;
    assert_eq!(rows.len(), 1, "max_attempts = 1 starts no retry: {rows:?}");
    assert_eq!(rows[0].state, "FAILED");
    let history = store::load_history(&mut conn, first)
        .await
        .expect("history")
        .events;
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowRetryScheduled { .. })),
        "no retry is scheduled"
    );
    assert_eq!(rows[0].attempts(), 3, "the stored code policy is kept");
    change(&db, None, name, RetryLimitsPatch::default(), true).await;
}

#[tokio::test]
async fn ac1_a_change_mid_chain_applies_to_the_next_failure() {
    let _serial = serial().await;
    let db = Db::new().await;
    let name = leaked("ro_ac1_mid_chain");
    let code = policy(3, PAST_REFRESH);
    let registry = Running::registry(vec![wf_info(name, always_fail, Some(code.clone()))]);
    let mut conn = db.conn().await;
    start(&mut conn, name, Some(code), RetryPolicySource::Type).await;
    let running = Running::spawn(&db, &registry);
    wait_runs(&db, name, "the second attempt exists", |rows| {
        rows.len() == 2
    })
    .await;
    // The second attempt waits out its delay. The chain is in flight.
    change(&db, Some(&registry), name, set_attempts(2), false).await;
    wait_runs(&db, name, "the second attempt fails", |rows| {
        rows.get(1).is_some_and(Row::terminal)
    })
    .await;
    settle().await;
    let rows = runs(&mut conn, name).await;
    running.stop().await;
    assert_eq!(
        rows.len(),
        2,
        "the code policy would start a third: {rows:?}"
    );
    change(&db, None, name, RetryLimitsPatch::default(), true).await;
}

#[tokio::test]
async fn ac1_an_override_adds_attempts_to_a_new_run() {
    let _serial = serial().await;
    let db = Db::new().await;
    let name = leaked("ro_ac1_more");
    let code = policy(1, Duration::from_millis(20));
    let registry = Running::registry(vec![wf_info(name, always_fail, Some(code.clone()))]);
    change(&db, Some(&registry), name, set_attempts(3), false).await;
    let running = Running::spawn(&db, &registry);
    let mut conn = db.conn().await;
    start(&mut conn, name, Some(code.clone()), RetryPolicySource::Type).await;
    wait_runs(&db, name, "three failed attempts", |rows| {
        rows.len() == 3 && rows.iter().all(Row::terminal)
    })
    .await;
    settle().await;
    let rows = runs(&mut conn, name).await;
    running.stop().await;
    assert_eq!(rows.len(), 3, "the override caps the chain at 3: {rows:?}");
    for row in &rows {
        assert_eq!(
            row.policy,
            Some(encode_stored(&code, RetryPolicySource::Type)),
            "each attempt stores the code policy, never the override"
        );
    }
    change(&db, None, name, RetryLimitsPatch::default(), true).await;
}

#[tokio::test]
async fn ac1_an_override_slows_the_next_retry() {
    let _serial = serial().await;
    let db = Db::new().await;
    let name = leaked("ro_ac1_slow");
    let code = policy(3, Duration::from_millis(20));
    let registry = Running::registry(vec![wf_info(name, always_fail, Some(code.clone()))]);
    change(
        &db,
        Some(&registry),
        name,
        RetryLimitsPatch {
            initial_interval: Some(Some(Duration::from_secs(3_600))),
            ..RetryLimitsPatch::default()
        },
        false,
    )
    .await;
    let mut conn = db.conn().await;
    let first = start(&mut conn, name, Some(code), RetryPolicySource::Type).await;
    let before = Utc::now();
    let running = Running::spawn(&db, &registry);
    let rows = wait_runs(&db, name, "a delayed retry", |rows| rows.len() == 2).await;
    let after = Utc::now();
    running.stop().await;
    let fire_at = fire_time(&mut conn, rows[1].exec_id()).await;
    let hour = chrono::Duration::hours(1);
    let slack = chrono::Duration::minutes(1);
    assert!(
        fire_at >= before + hour - slack && fire_at <= after + hour + slack,
        "the first delay is the override, 1 hour: {fire_at}"
    );
    let history = store::load_history(&mut conn, first)
        .await
        .expect("history")
        .events;
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowRetryScheduled { .. }))
    );
    // Cancel the delayed retry, so no later worker claims it.
    cancel_workflow_execution(
        &mut conn,
        rows[1].exec_id(),
        "test cleanup",
        &autumn_harvest::telemetry::NoOpMetrics,
    )
    .await
    .expect("cancel");
    change(&db, None, name, RetryLimitsPatch::default(), true).await;
}

#[tokio::test]
async fn ac1_the_fleet_ceiling_clamps_an_override_at_the_failure() {
    let _serial = serial().await;
    let db = Db::new().await;
    let name = leaked("ro_ac1_ceiling");
    let code = policy(1, Duration::from_millis(20));
    let registry = Arc::new(
        HandlerRegistry::new(vec![wf_info(name, always_fail, Some(code.clone()))], vec![])
            .with_max_workflow_attempts_ceiling(Some(2)),
    );
    change(&db, Some(&registry), name, set_attempts(5), false).await;
    let running = Running::spawn(&db, &registry);
    let mut conn = db.conn().await;
    start(&mut conn, name, Some(code), RetryPolicySource::Type).await;
    wait_runs(&db, name, "two failed attempts", |rows| {
        rows.len() >= 2 && rows.iter().all(Row::terminal)
    })
    .await;
    settle().await;
    let rows = runs(&mut conn, name).await;
    running.stop().await;
    assert_eq!(rows.len(), 2, "the ceiling of 2 caps the override of 5");
    change(&db, None, name, RetryLimitsPatch::default(), true).await;
}

// ── AC 2: an attempt that already started is not affected ────────────────

#[tokio::test]
async fn ac2_a_scheduled_attempt_keeps_its_fire_time_and_its_policy() {
    let _serial = serial().await;
    FAIL_ONCE_CALLS.store(0, Ordering::SeqCst);
    let db = Db::new().await;
    let name = leaked("ro_ac2_scheduled");
    let code = policy(2, Duration::from_secs(3));
    let registry = Running::registry(vec![wf_info(
        name,
        fail_once_then_succeed,
        Some(code.clone()),
    )]);
    let mut conn = db.conn().await;
    start(&mut conn, name, Some(code.clone()), RetryPolicySource::Type).await;
    let running = Running::spawn(&db, &registry);
    let rows = wait_runs(&db, name, "the second attempt exists", |rows| {
        rows.len() == 2
    })
    .await;
    let second = rows[1].exec_id();
    let fire_before = fire_time(&mut conn, second).await;
    // The second attempt exists. Stop the retries and slow them now.
    change(
        &db,
        Some(&registry),
        name,
        RetryLimitsPatch {
            max_attempts: Some(Some(1)),
            initial_interval: Some(Some(Duration::from_secs(3_600))),
            max_interval: None,
        },
        false,
    )
    .await;
    assert_eq!(
        fire_time(&mut conn, second).await,
        fire_before,
        "the fire time does not move"
    );
    let rows = wait_runs(&db, name, "the second attempt completes", |rows| {
        rows.get(1).is_some_and(|row| row.state == "COMPLETED")
    })
    .await;
    running.stop().await;
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[1].policy,
        Some(encode_stored(&code, RetryPolicySource::Type)),
        "its stored policy is unchanged"
    );
    change(&db, None, name, RetryLimitsPatch::default(), true).await;
}

#[tokio::test]
async fn ac2_a_running_attempt_runs_to_its_end() {
    let _serial = serial().await;
    ENTERED.store(false, Ordering::SeqCst);
    RELEASE.store(false, Ordering::SeqCst);
    let db = Db::new().await;
    let name = leaked("ro_ac2_running");
    let code = policy(3, Duration::from_millis(20));
    let registry = Running::registry(vec![wf_info(name, block_then_complete, Some(code.clone()))]);
    let mut conn = db.conn().await;
    start(&mut conn, name, Some(code), RetryPolicySource::Type).await;
    let running = Running::spawn(&db, &registry);
    for _ in 0..200 {
        if AtomicBool::load(&ENTERED, Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        AtomicBool::load(&ENTERED, Ordering::SeqCst),
        "the handler starts"
    );
    change(&db, Some(&registry), name, set_attempts(1), false).await;
    RELEASE.store(true, Ordering::SeqCst);
    let rows = wait_runs(&db, name, "the attempt completes", |rows| {
        rows.first().is_some_and(|row| row.state == "COMPLETED")
    })
    .await;
    running.stop().await;
    assert_eq!(rows.len(), 1, "the change cancels nothing: {rows:?}");
    change(&db, None, name, RetryLimitsPatch::default(), true).await;
}

// ── AC 3: a clear restores the code value ─────────────────────────────────

#[tokio::test]
async fn ac3_a_clear_mid_chain_restores_the_code_policy() {
    let _serial = serial().await;
    let db = Db::new().await;
    let name = leaked("ro_ac3_clear");
    let code = policy(2, PAST_REFRESH);
    let registry = Running::registry(vec![wf_info(name, always_fail, Some(code.clone()))]);
    change(&db, Some(&registry), name, set_attempts(4), false).await;
    let mut conn = db.conn().await;
    start(&mut conn, name, Some(code.clone()), RetryPolicySource::Type).await;
    let running = Running::spawn(&db, &registry);
    wait_runs(&db, name, "the second attempt exists", |rows| {
        rows.len() == 2
    })
    .await;
    change(
        &db,
        Some(&registry),
        name,
        RetryLimitsPatch::default(),
        true,
    )
    .await;
    assert!(registry.run_defaults().get(name).is_none());
    assert!(
        get_run_default_override(&mut conn, name)
            .await
            .expect("read")
            .is_none()
    );
    wait_runs(&db, name, "the second attempt fails", |rows| {
        rows.get(1).is_some_and(Row::terminal)
    })
    .await;
    settle().await;
    let rows = runs(&mut conn, name).await;
    running.stop().await;
    assert_eq!(
        rows.len(),
        2,
        "the code policy of 2 attempts applies again: {rows:?}"
    );
    assert_eq!(
        rows[1].policy,
        Some(encode_stored(&code, RetryPolicySource::Type)),
        "the second attempt stored the code policy"
    );
}

// ── AC 4: a schedule's own retry_policy still wins ───────────────────────

#[tokio::test]
async fn ac4_a_schedule_policy_wins_over_the_override() {
    let _serial = serial().await;
    let db = Db::new().await;
    let name = leaked("ro_ac4_own");
    let code = policy(1, Duration::from_millis(20));
    let own = policy(2, Duration::from_millis(20));
    let registry = Running::registry(vec![wf_info(name, always_fail, Some(code))]);
    change(&db, Some(&registry), name, set_attempts(1), false).await;
    let mut conn = db.conn().await;
    start(
        &mut conn,
        name,
        Some(own.clone()),
        RetryPolicySource::Schedule,
    )
    .await;
    let running = Running::spawn(&db, &registry);
    wait_runs(&db, name, "the schedule policy retries once", |rows| {
        rows.len() == 2 && rows.iter().all(Row::terminal)
    })
    .await;
    settle().await;
    let rows = runs(&mut conn, name).await;
    running.stop().await;
    assert_eq!(rows.len(), 2, "{rows:?}");
    for row in &rows {
        assert_eq!(
            row.policy,
            Some(encode_stored(&own, RetryPolicySource::Schedule)),
            "the retry keeps the schedule policy and its source"
        );
    }
    change(&db, None, name, RetryLimitsPatch::default(), true).await;
}

/// Insert a due schedule row of `name` with `retry_policy`.
async fn insert_due_schedule(
    conn: &mut AsyncPgConnection,
    name: &str,
    retry_policy: Option<&RetryPolicy>,
) -> Uuid {
    use harvest_schedules::dsl;
    let id = Uuid::new_v4();
    diesel::insert_into(harvest_schedules::table)
        .values((
            dsl::id.eq(id),
            dsl::workflow_name.eq(name),
            dsl::schedule_expr.eq("interval:3600"),
            dsl::timezone.eq("UTC"),
            dsl::catchup.eq(false),
            dsl::max_active_runs.eq(10),
            dsl::is_paused.eq(false),
            dsl::next_run_at.eq(Utc::now() - chrono::Duration::seconds(5)),
            dsl::jitter_secs.eq(0_i64),
            dsl::overlap_policy.eq("skip"),
            dsl::buffered_runs.eq(serde_json::json!([])),
            dsl::buffer_all_max.eq(0),
            dsl::skip_policy.eq("skip"),
            Ext(harvest_schedules_ext::workflow_input.eq(serde_json::json!({}))),
            Ext(harvest_schedules_ext::retry_policy
                .eq(retry_policy.map(|p| serde_json::to_value(p).expect("json")))),
        ))
        .execute(conn)
        .await
        .expect("insert schedule");
    id
}

#[tokio::test]
async fn ac4_a_scheduler_fire_marks_only_a_schedule_policy() {
    let _serial = serial().await;
    let db = Db::new().await;
    let own = leaked("ro_ac4_tick_own");
    let inherit = leaked("ro_ac4_tick_inherit");
    let code = policy(3, Duration::from_millis(20));
    let own_policy = policy(2, Duration::from_millis(20));
    let registry = Running::registry(vec![
        wf_info(own, always_fail, Some(code.clone())),
        wf_info(inherit, always_fail, Some(code.clone())),
    ]);
    let mut conn = db.conn().await;
    insert_due_schedule(&mut conn, own, Some(&own_policy)).await;
    insert_due_schedule(&mut conn, inherit, None).await;
    tick_once(
        build_test_pool(&db.url),
        Arc::clone(&registry),
        Arc::new(DagCatalog::default()),
        Arc::new(vec![]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("tick");
    let own_runs = runs(&mut conn, own).await;
    let inherit_runs = runs(&mut conn, inherit).await;
    assert_eq!(own_runs.len(), 1, "the tick fires one run");
    assert_eq!(inherit_runs.len(), 1, "the tick fires one run");
    assert_eq!(own_runs[0].source(), Some(SCHEDULE_SOURCE));
    assert_eq!(own_runs[0].attempts(), 2);
    assert_eq!(inherit_runs[0].source(), None);
    assert_eq!(inherit_runs[0].attempts(), 3);

    // The override stops the retries of the run that inherits the code
    // policy. The run of the schedule that declares a policy keeps it.
    change(&db, Some(&registry), own, set_attempts(1), false).await;
    change(&db, Some(&registry), inherit, set_attempts(1), false).await;
    let running = Running::spawn(&db, &registry);
    wait_runs(&db, own, "the schedule policy retries once", |rows| {
        rows.len() == 2 && rows.iter().all(Row::terminal)
    })
    .await;
    wait_runs(&db, inherit, "the inherited run fails", |rows| {
        rows.first().is_some_and(Row::terminal)
    })
    .await;
    settle().await;
    let own_runs = runs(&mut conn, own).await;
    let inherit_runs = runs(&mut conn, inherit).await;
    running.stop().await;
    assert_eq!(own_runs.len(), 2, "{own_runs:?}");
    assert_eq!(inherit_runs.len(), 1, "{inherit_runs:?}");
    diesel::sql_query("DELETE FROM harvest_schedules WHERE workflow_name = ANY($1)")
        .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(vec![
            own.to_string(),
            inherit.to_string(),
        ])
        .execute(&mut conn)
        .await
        .expect("delete the schedules");
    change(&db, None, own, RetryLimitsPatch::default(), true).await;
    change(&db, None, inherit, RetryLimitsPatch::default(), true).await;
}

#[tokio::test]
async fn a_rerun_keeps_the_source_of_the_stored_policy() {
    let _serial = serial().await;
    let db = Db::new().await;
    let mut conn = db.conn().await;
    let own = policy(2, Duration::from_millis(20));
    for (name, source) in [
        (leaked("ro_rerun_schedule"), RetryPolicySource::Schedule),
        (leaked("ro_rerun_type"), RetryPolicySource::Type),
    ] {
        let source_run = start(&mut conn, name, Some(own.clone()), source).await;
        diesel::sql_query(
            "UPDATE harvest_workflow_executions SET state = 'FAILED', completed_at = now() \
             WHERE id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(source_run.as_uuid())
        .execute(&mut conn)
        .await
        .expect("fail the source");
        let outcome = rerun_workflow_execution(
            &mut conn,
            source_run,
            RerunRequest {
                input_override: None,
                workflow_id_override: None,
                started_by: None,
                concurrency_key: None,
                concurrency_limit: None,
                concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
                max_workflow_input_bytes: 0,
                max_execution_timeout_ceiling: None,
                max_workflow_chain_timeout_ceiling: None,
                max_workflow_attempts_ceiling: None,
                trace_context: None,
            },
            None,
        )
        .await
        .expect("rerun");
        let rows = runs(&mut conn, name).await;
        let rerun = rows
            .iter()
            .find(|row| row.id == outcome.exec_id.as_uuid())
            .expect("the rerun row");
        assert_eq!(
            rerun.policy,
            Some(encode_stored(&own, source)),
            "{source:?}: the rerun keeps the policy and its source"
        );
        cancel_workflow_execution(
            &mut conn,
            outcome.exec_id,
            "test cleanup",
            &autumn_harvest::telemetry::NoOpMetrics,
        )
        .await
        .expect("cancel");
    }
}
