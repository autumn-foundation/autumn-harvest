#![cfg(feature = "db")]
//! End-to-end worker proofs for `ctx.timer_business_days` (issue #806).
//!
//! Two complementary proofs, driven through a **real** `Worker` against Postgres:
//!
//! * `worker_arms_business_day_timer_in_one_decision_cycle` — the falsifiable
//!   correctness AC. Sat/Sun plus two holidays (placed on the next two
//!   *weekdays* after the worker's own real anchor, so they genuinely enter the
//!   resolution window on whichever weekday CI runs) must arm a durable timer
//!   whose `harvest_timers.fires_at` lands on the resolved instant computed by
//!   an **independent** reference implementation, skipping the weekend and the
//!   holidays. Also asserts the ONE-DECISION-CYCLE property: exactly one
//!   `TimerStarted` and exactly one `SideEffectRecorded` in history. Note this
//!   is a *necessary*, not sufficient, condition — a split batch would still
//!   record one of each, just across two cycles; the sufficient proof is the
//!   `extract_started_timer_for_suspension_accepts_freeze_plus_timer` unit test
//!   in `worker.rs`, whose allow-list is what makes the single batch legal
//!   (deleting `RecordSideEffect` from it makes both tests here fail).
//!
//! * `business_day_calendar_loaded_from_db_resolves` — the registration path an
//!   embedder actually uses: holidays live in `harvest_calendar_exclusions`, are
//!   loaded with the existing `load_exclusions_for_calendar`, and are handed to
//!   the worker as a `BusinessCalendars` snapshot in shared state.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly; otherwise a fresh testcontainers Postgres is booted with
//! the full migration bundle.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::calendar::BusinessCalendars;
use autumn_harvest::context::{SharedStateMap, WorkflowContext};
use autumn_harvest::event::{SideEffectKind, WorkflowEvent};
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::models::{NewWorkflowExecution, WorkflowExecution};
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::store;
use autumn_harvest::telemetry::{MetricsRecorder, NoOpMetrics, TelemetryConfig};
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};

use chrono::{DateTime, NaiveDate, Utc};
use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// DB setup — env-var-redirectable, otherwise testcontainers.
// ---------------------------------------------------------------------------

fn init_sql() -> Vec<u8> {
    autumn_harvest::test_init_sql().as_bytes().to_vec()
}

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool build failed")
}

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("failed to connect to Postgres")
}

// ---------------------------------------------------------------------------
// Worker construction — the ONLY difference from a stock worker is that the
// BusinessCalendars snapshot rides shared state, exactly as
// `HarvestBuilder::state(..)` delivers it in production.
// ---------------------------------------------------------------------------

fn registry_with_calendars(calendars: BusinessCalendars) -> Arc<HandlerRegistry> {
    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(Arc::new(NoOpMetrics) as Arc<dyn MetricsRecorder>)
            .build(),
    );
    let mut map: SharedStateMap = HashMap::new();
    map.insert(
        std::any::TypeId::of::<BusinessCalendars>(),
        Box::new(calendars),
    );
    Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![sla_flow_info()],
        Vec::<ActivityInfo>::new(),
        Arc::new(map),
        telemetry,
    ))
}

fn sla_flow_info() -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name: "sla_flow",
        module: "business_day_timer_tests",
        handler: sla_flow_handler,
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

fn build_worker(worker_id: &str, registry: Arc<HandlerRegistry>) -> Arc<Worker> {
    Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                codec_rotation_batch_size: 0,
                dr_fencing: false,
                worker_id: worker_id.to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 2,
                max_concurrent_activities: 2,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(1),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 1000,
                priority_aging_secs: None,
                unknown_target_grace_window: Duration::from_secs(5),
                poison_pill_threshold: 3,
                capability_miss_max_redeliveries: 5,
                workflow_task_timeout: Duration::from_secs(10),
                workflow_panic_max_attempts: 3,
                labels: HashMap::new(),
                queue_weights: HashMap::new(),
                max_workflow_pause_duration: Duration::from_secs(24 * 3600),
                max_workflow_history_events: None,
                shard_notification_database_urls: Vec::new(),
                sharded_pool: None,
                slot_tuner: None,
                max_concurrent_sessions: 0,
            },
            registry,
        )
        .expect("worker should build"),
    )
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

type BoxFut<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>>;

/// Arms one business-day timer against the `"biz"` calendar and returns the
/// resolved deadline.
fn sla_flow_handler(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        let n = u32::try_from(
            input
                .get("n")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(2),
        )
        .map_err(|e| e.to_string())?;
        // The calendar name rides the input so each test can register a unique
        // one and stay hermetic against a reused database.
        let calendar = input
            .get("calendar")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("biz")
            .to_string();
        let deadline = ctx
            .timer_business_days("sla", n, &calendar)
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!(deadline.to_rfc3339()))
    })
}

// ---------------------------------------------------------------------------
// Seeding + read helpers
// ---------------------------------------------------------------------------

async fn seed_workflow(conn: &mut AsyncPgConnection, input: serde_json::Value) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let row = NewWorkflowExecution {
        quota_key: None,
        id: exec_id.as_uuid(),
        workflow_name: "sla_flow",
        workflow_id: &format!("wf-{}", exec_id.as_uuid()),
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: input.clone(),
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
        continued_from_exec_id: None,
        first_exec_id: None,
        start_source: None,
        start_source_ref: None,
        started_by: None,
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("insert workflow execution");

    store::append_events(
        conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted");

    let mut params = EnqueueParams::new("default", TaskType::Workflow, input);
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);
    queue::enqueue(conn, &params)
        .await
        .expect("enqueue workflow task");

    exec_id
}

async fn load_history(url: &str, exec_id: ExecutionId) -> Vec<WorkflowEvent> {
    let mut conn = connect(url).await;
    store::load_history(&mut conn, exec_id)
        .await
        .expect("load_history")
        .events
}

async fn load_execution(url: &str, exec_id: ExecutionId) -> WorkflowExecution {
    let mut conn = connect(url).await;
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("reload workflow execution")
}

/// Read the durable timer row's absolute fire instant.
async fn timer_fires_at(url: &str, exec_id: ExecutionId) -> DateTime<Utc> {
    #[derive(QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        fires_at: DateTime<Utc>,
    }
    let mut conn = connect(url).await;
    let rows: Vec<Row> = diesel::sql_query(
        "SELECT fires_at FROM harvest_timers WHERE workflow_exec_id = $1 ORDER BY fires_at",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .load(&mut conn)
    .await
    .expect("load harvest_timers");
    assert_eq!(
        rows.len(),
        1,
        "exactly ONE durable timer row must be armed (a split batch would arm two)"
    );
    rows[0].fires_at
}

/// Wait until the workflow task is parked on its timer (history has `TimerStarted`).
async fn wait_for_timer_started(url: &str, exec_id: ExecutionId, timeout: Duration) {
    tokio::time::timeout(timeout, async {
        loop {
            let h = load_history(url, exec_id).await;
            if h.iter()
                .any(|e| matches!(e, WorkflowEvent::TimerStarted { .. }))
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timer was never armed within {timeout:?}"));
}

const fn day(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).expect("valid date")
}

/// The next `count` **weekdays** strictly after `from`.
///
/// Holidays for these tests must be placed relative to the worker's real anchor
/// (a fixed past date would never enter the resolution window and every
/// holiday-skip assertion would pass vacuously) — but they must also land on
/// days that are business days *without* them, or they contribute nothing. Naive
/// `today + 1 day` / `today + 2 days` fails exactly that on a Friday, where both
/// land on the weekend and the falsifiability assertion (`deadline >
/// weekends_only`) becomes unsatisfiable. Walking forward over weekdays makes
/// the fixture correct on all seven anchors.
///
/// Weekday-only by construction — deliberately independent of
/// `calendar::is_business_day` so the fixture cannot be co-broken with the
/// production predicate it is meant to exercise.
fn next_weekdays_after(from: NaiveDate, count: usize) -> Vec<NaiveDate> {
    let mut out = Vec::with_capacity(count);
    let mut cursor = from;
    while out.len() < count {
        cursor = cursor.succ_opt().expect("date in range");
        if !matches!(
            chrono::Datelike::weekday(&cursor),
            chrono::Weekday::Sat | chrono::Weekday::Sun
        ) {
            out.push(cursor);
        }
    }
    out
}

/// A deliberately **independent** reimplementation of business-day arithmetic,
/// written differently from `calendar::add_business_days` (candidate-date scan
/// rather than a cursor loop) so the integration assertion is not circular.
///
/// Contract mirrored: Sat/Sun are never business days, `holidays` are never
/// business days, `n = 0` means "the anchor if it is a business instant, else
/// the next business instant", and the anchor's time-of-day is preserved.
fn reference_business_deadline(
    anchor: DateTime<Utc>,
    n: u32,
    holidays: &[NaiveDate],
) -> DateTime<Utc> {
    use chrono::{Datelike, Weekday};

    let is_business = |d: NaiveDate| {
        !matches!(d.weekday(), Weekday::Sat | Weekday::Sun) && !holidays.contains(&d)
    };

    let start = anchor.date_naive();
    // Candidate dates, in order, starting at the anchor's own date.
    let mut business_days_seen = 0_u32;
    for offset in 0..3_650_i64 {
        let candidate = start + chrono::Duration::days(offset);
        if !is_business(candidate) {
            continue;
        }
        // The anchor's own date counts as business day 0.
        if business_days_seen == n {
            return candidate
                .and_time(anchor.time())
                .and_local_timezone(Utc)
                .single()
                .expect("UTC has no ambiguous local times");
        }
        business_days_seen += 1;
    }
    panic!("reference scan exhausted for n={n}");
}

// ---------------------------------------------------------------------------
// TEST 1 — falsifiable correctness + one decision cycle
// ---------------------------------------------------------------------------

/// The issue's falsifiable correctness AC.
///
/// The worker anchors on its own real clock, so the fixture cannot use a fixed
/// date (a past anchor never enters the resolution window and every skip
/// assertion passes vacuously). Instead the calendar excludes the **next two
/// weekdays** after the anchor, guaranteeing both holidays bite on whichever
/// weekday CI runs. `timer_business_days(_, 2, cal)` must step over the weekend
/// and both holidays.
///
/// **Falsifiability.** The expected value comes from `reference_business_deadline`
/// — an independent reimplementation (candidate-date scan, not the production
/// cursor loop) — so the assertion is not circular. A separate assertion pins
/// that the holidays actually *moved* the answer (`deadline > weekends_only`),
/// so a silently-broken holiday skip cannot pass. Deleting the holiday check
/// from `calendar::is_business_day` fails this test (mutant-verified).
///
/// The `harvest_timers.fires_at` row is asserted against the frozen deadline
/// with a small tolerance: `fires_at` is anchored to the **Postgres** clock at
/// persist time while the freeze anchors on the worker's clock, so the two are
/// the same instant only up to worker↔DB skew plus the persist latency.
#[tokio::test]
async fn worker_arms_business_day_timer_in_one_decision_cycle() {
    let (url, _container) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;

    // The worker anchors on its own real clock, so the holidays must be placed
    // RELATIVE to now or they never enter the resolution window and the
    // holiday-skip assertion below would pass vacuously. They must also land on
    // WEEKDAYS -- blocking `today+1`/`today+2` verbatim puts both on the weekend
    // when the suite runs on a Friday, where they change nothing and the
    // falsifiability assertion below becomes unsatisfiable.
    let today = Utc::now().date_naive();
    let holidays = next_weekdays_after(today, 2);

    let calendars = BusinessCalendars::new().with_calendar("biz", holidays.clone());
    let exec_id = seed_workflow(&mut conn, serde_json::json!({ "n": 2 })).await;

    let worker = build_worker("bd-worker-1", registry_with_calendars(calendars));
    let handle = {
        let (w, p) = (worker.clone(), pool.clone());
        tokio::spawn(async move { w.run(&p).await })
    };

    wait_for_timer_started(&url, exec_id, Duration::from_secs(30)).await;
    worker.shutdown();
    let _ = handle.await;

    let history = load_history(&url, exec_id).await;

    // --- ONE DECISION CYCLE ---------------------------------------------
    let side_effects: Vec<_> = history
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::SideEffectRecorded {
                kind: SideEffectKind::Custom,
                name: Some(n),
                value,
            } => Some((n.clone(), value.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(
        side_effects.len(),
        1,
        "exactly ONE business-day freeze must be recorded, got {side_effects:?}"
    );
    assert_eq!(side_effects[0].0, "__harvest_business_day:1:sla");

    let timers: Vec<_> = history
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::TimerStarted { .. }))
        .collect();
    assert_eq!(
        timers.len(),
        1,
        "the freeze and the timer must ride ONE suspension batch, got {timers:?}"
    );

    // No `Now` side effect: the anchor is captured inside the freeze closure.
    assert!(
        !history.iter().any(|e| matches!(
            e,
            WorkflowEvent::SideEffectRecorded {
                kind: SideEffectKind::Now,
                ..
            }
        )),
        "the anchor must NOT be recorded as its own Now event: {history:?}"
    );

    // --- FALSIFIABLE CORRECTNESS, TO THE EXACT INSTANT --------------------
    let frozen = &side_effects[0].1;
    assert_eq!(frozen["outcome"], "resolved", "frozen record: {frozen}");
    let anchor: DateTime<Utc> =
        serde_json::from_value(frozen["anchor"].clone()).expect("frozen anchor deserializes");
    let deadline: DateTime<Utc> =
        serde_json::from_value(frozen["deadline"].clone()).expect("frozen deadline deserializes");

    // The anchor is "now" (the worker's real clock), so assert the resolution
    // RELATIVE to it, against the INDEPENDENT reference implementation.
    let expected = reference_business_deadline(anchor, 2, &holidays);
    assert_eq!(
        deadline, expected,
        "the frozen deadline must equal the independent reference resolution \
         (anchor {anchor}, holidays {holidays:?})"
    );

    // Falsifiability: the holidays must actually have BITTEN. Resolving the
    // same anchor with weekends-only must give a strictly earlier deadline --
    // otherwise the assertion above would hold even if holiday-skipping were
    // silently broken.
    let weekends_only = reference_business_deadline(anchor, 2, &[]);
    assert!(
        deadline > weekends_only,
        "the registered holidays must push the deadline later than a \
         weekends-only resolution (got {deadline}, weekends-only {weekends_only}); \
         otherwise this test cannot detect a broken holiday skip"
    );

    // And the resolved date must not itself be a holiday or a weekend.
    let landed = deadline.date_naive();
    assert!(
        !holidays.contains(&landed),
        "resolved deadline landed ON a holiday: {landed}"
    );
    assert!(
        !matches!(
            chrono::Datelike::weekday(&landed),
            chrono::Weekday::Sat | chrono::Weekday::Sun
        ),
        "resolved deadline landed on a weekend: {landed}"
    );

    // The durable timer row's absolute fire instant must match the frozen
    // deadline. `fires_at` is `db_now + duration_secs`, and the two clocks can
    // differ by the sub-second time it took the worker to persist, so allow a
    // small tolerance.
    let fires_at = timer_fires_at(&url, exec_id).await;
    let skew = (fires_at - deadline).num_seconds().abs();
    assert!(
        skew <= 5,
        "harvest_timers.fires_at ({fires_at}) must land on the resolved deadline \
         ({deadline}); skew {skew}s"
    );
}

// ---------------------------------------------------------------------------
// TEST 2 — the DB registration path an embedder actually uses
// ---------------------------------------------------------------------------

/// Holidays live in `harvest_calendar_exclusions`; the embedder loads them with
/// the pre-existing `load_exclusions_for_calendar` and registers the snapshot on
/// the builder. No new table, no new loader.
#[tokio::test]
async fn business_day_calendar_loaded_from_db_resolves() {
    let (url, _container) = setup_db().await;
    let pool = build_pool(&url);
    let mut conn = connect(&url).await;

    // A per-run calendar name keeps the test hermetic when
    // HARVEST_TEST_DATABASE_URL points at a reused database.
    let cal_name = format!("biz-{}", Uuid::new_v4());

    // Holidays are placed relative to now so they genuinely enter the resolution
    // window (a fixed past date would make the assertions vacuous), and on
    // weekdays so they actually bite on every anchor weekday (see
    // `next_weekdays_after`).
    let today = Utc::now().date_naive();
    let holidays = next_weekdays_after(today, 2);

    // Seed a calendar + exclusions through the shipped DB helpers.
    autumn_harvest::calendar::create_calendar(
        &mut conn,
        &cal_name,
        Some("business-day test calendar"),
    )
    .await
    .expect("create calendar");
    autumn_harvest::calendar::replace_calendar_exclusions(&mut conn, &cal_name, &holidays)
        .await
        .expect("seed exclusions");

    let loaded = autumn_harvest::calendar::load_exclusions_for_calendar(&mut conn, &cal_name)
        .await
        .expect("load exclusions");
    assert_eq!(loaded.len(), 2, "both exclusions must load back");
    assert_eq!(
        loaded
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>(),
        holidays
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>(),
        "the loaded exclusions must round-trip the seeded dates"
    );

    let calendars = BusinessCalendars::new().with_calendar(&cal_name, loaded.clone());
    let exec_id = seed_workflow(
        &mut conn,
        serde_json::json!({ "n": 1, "calendar": cal_name }),
    )
    .await;

    let worker = build_worker("bd-worker-2", registry_with_calendars(calendars));
    let handle = {
        let (w, p) = (worker.clone(), pool.clone());
        tokio::spawn(async move { w.run(&p).await })
    };

    wait_for_timer_started(&url, exec_id, Duration::from_secs(30)).await;
    worker.shutdown();
    let _ = handle.await;

    let history = load_history(&url, exec_id).await;
    let frozen = history
        .iter()
        .find_map(|e| match e {
            WorkflowEvent::SideEffectRecorded { value, .. } => Some(value.clone()),
            _ => None,
        })
        .expect("a business-day freeze must be recorded");
    assert_eq!(frozen["outcome"], "resolved", "frozen record: {frozen}");
    assert_eq!(
        frozen["calendar"],
        serde_json::Value::String(cal_name.clone()),
        "the frozen record names the DB-loaded calendar"
    );

    // The DB-sourced holidays must actually have been applied: resolving the
    // same anchor with no holidays must give a strictly earlier deadline.
    let anchor: DateTime<Utc> =
        serde_json::from_value(frozen["anchor"].clone()).expect("frozen anchor deserializes");
    let deadline: DateTime<Utc> =
        serde_json::from_value(frozen["deadline"].clone()).expect("frozen deadline deserializes");
    assert_eq!(
        deadline,
        reference_business_deadline(anchor, 1, &holidays),
        "the DB-loaded calendar must drive the resolution"
    );
    assert!(
        deadline > reference_business_deadline(anchor, 1, &[]),
        "the DB-loaded holidays must push the deadline later than a \
         weekends-only resolution (deadline {deadline}, anchor {anchor})"
    );

    // The execution is parked on its timer, not failed.
    let exec = load_execution(&url, exec_id).await;
    assert_eq!(
        exec.state, "RUNNING",
        "the workflow must be parked on the business-day timer, not failed"
    );
}
