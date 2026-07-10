#![cfg(feature = "db")]
// Test-code style lints (consistent with other integration test files in this crate).
#![allow(
    clippy::doc_markdown,
    clippy::items_after_statements,
    clippy::missing_const_for_fn,
    clippy::type_complexity,
    clippy::significant_drop_tightening
)]
//! Durable completion-callback delivery integration tests — issue #605.
//!
//! Verifies the enqueue-on-terminal-transition + scanner + retry/backoff +
//! dead-letter pipeline against a real Postgres container:
//!
//! - `enqueue_on_completion_creates_one_row_per_matching_target` — a
//!   completed execution with two per-execution targets (`AnyTerminal` and
//!   `CompletedOnly`) enqueues both.
//! - `enqueue_skips_targets_whose_filter_does_not_match` — a `CompletedOnly`
//!   target on a `Failed` execution enqueues nothing.
//! - `enqueue_is_idempotent_on_re_evaluation` — calling
//!   `evaluate_triggers_for_execution` twice for the same execution/state
//!   never double-enqueues (`UNIQUE (workflow_exec_id, callback_index)` +
//!   `ON CONFLICT DO NOTHING`).
//! - `no_targets_configured_enqueues_nothing` — the "identical behavior for
//!   callback-less workflows" guarantee.
//! - `scanner_marks_delivered_on_2xx` — a 200 response transitions
//!   `PENDING` -> `DELIVERED` and the POSTed body carries the fixed
//!   envelope shape, HMAC-signed.
//! - `scanner_reschedules_with_backoff_on_non_2xx` — a 500 response with
//!   `attempt < max_attempts` reschedules with a future `next_attempt_at`.
//! - `scanner_dead_letters_on_retry_exhaustion` — exhausting the retry
//!   budget marks the delivery `FAILED` and writes a
//!   `harvest_dead_letters` row with the typed `CallbackDeliveryExhausted`
//!   reason.
//! - `scanner_ignores_rows_not_yet_due` — a row whose `next_attempt_at` is
//!   in the future is left untouched by the scanner.
//! - `list_deliveries_for_execution_returns_rows_in_callback_index_order`
//!   and the `redrive_delivery_*` tests — the management-surface read and
//!   redrive primitives (issue #605 AC: list + manually redrive).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::completion_callback::{
    CallbackRuntimeConfig, CallbackSecret, CompletionCallbackDeliverer, DeliverFuture,
    DeliveryAttempt, DeliveryRedriveOutcome, GLOBAL_CALLBACK_CONFIG, HostAllowlist, SsrfPolicy,
    fire_due_completion_deliveries, list_deliveries_for_execution, redrive_delivery,
};
use autumn_harvest::completion_trigger::{TerminalState, evaluate_triggers_for_execution};
use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::DbPool;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

/// `GLOBAL_CALLBACK_CONFIG` is a process-wide static (by design — it mirrors
/// `GLOBAL_WORKFLOW_METADATA`'s "set once at startup" contract in
/// production). `#[tokio::test]` functions within one test binary run
/// concurrently by default, so every test in this file must serialize
/// through this lock for the whole time it depends on a stable config,
/// otherwise two tests running at once would clobber each other's
/// deliverer/secret/policy.
static TEST_SERIAL: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

// All migrations through `harvest_completion_deliveries` (issue #605), the
// same base list `tests/nd_block_tests.rs` uses (schema-relevant migrations
// only — see that file's comment for why the full migration count is not
// required: many later migrations don't touch a table this test reads).
const INIT_SQL: &str = concat!(
    include_str!("../../migrations/20260409000000_harvest_initial/up.sql"),
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_set_at TIMESTAMPTZ NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_until TIMESTAMPTZ NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_reason TEXT NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_actor TEXT NULL;\n",
    "\n",
    include_str!("../../migrations/20260619000000_harvest_task_queue_created_at/up.sql"),
    "\n",
    include_str!("../../migrations/20260424000001_harvest_trace_context/up.sql"),
    "\n",
    include_str!("../../migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../../migrations/20260427000000_harvest_continue_as_new/up.sql"),
    "\n",
    include_str!("../../migrations/20260429000000_harvest_concurrency_key/up.sql"),
    "\n",
    include_str!("../../migrations/20260430000000_harvest_workflow_schedules/up.sql"),
    "\n",
    include_str!("../../migrations/20260430000001_harvest_external_tasks/up.sql"),
    "\n",
    include_str!("../../migrations/20260508000000_harvest_external_task_updated_at/up.sql"),
    "\n",
    include_str!("../../migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!("../../migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../../migrations/20260508010000_harvest_workers_drain_deadline/up.sql"),
    "\n",
    include_str!("../../migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!("../../migrations/20260513000000_harvest_schedule_pause_metadata/up.sql"),
    "\n",
    include_str!("../../migrations/20260514020000_harvest_task_activity_id/up.sql"),
    "\n",
    include_str!("../../migrations/20260518000000_harvest_signal_idempotency/up.sql"),
    "\n",
    include_str!("../../migrations/20260517000000_harvest_schedule_jitter/up.sql"),
    "\n",
    include_str!("../../migrations/20260517000001_harvest_schedule_overlap_policy/up.sql"),
    "\n",
    include_str!("../../migrations/20260518000001_harvest_workflow_execution_timeout/up.sql"),
    "\n",
    include_str!("../../migrations/20260613000000_harvest_workflow_sla/up.sql"),
    "\n",
    include_str!("../../migrations/20260519000000_harvest_calendar_awareness/up.sql"),
    "\n",
    include_str!("../../migrations/20260522000000_harvest_schedule_decisions/up.sql"),
    "\n",
    include_str!("../../migrations/20260522000001_harvest_rate_limiting/up.sql"),
    "\n",
    include_str!("../../migrations/20260526000001_harvest_parent_close_policy/up.sql"),
    "\n",
    include_str!("../../migrations/20260530000000_harvest_schedule_ha_claim/up.sql"),
    "\n",
    include_str!("../../migrations/20260601000000_harvest_schedule_auto_pause/up.sql"),
    "\n",
    include_str!("../../migrations/20260601000001_harvest_poison_pill_strikes/up.sql"),
    "\n",
    include_str!("../../migrations/20260601000002_harvest_ownership_metadata/up.sql"),
    "\n",
    include_str!("../../migrations/20260603000000_harvest_completion_triggers/up.sql"),
    include_str!("../../migrations/20260708000001_harvest_completion_trigger_condition/up.sql"),
    include_str!("../../migrations/20260605000000_harvest_admission_gates/up.sql"),
    include_str!("../../migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"),
    include_str!("../../migrations/20260607000000_harvest_worker_capability_labels/up.sql"),
    include_str!("../../migrations/20260607000001_harvest_task_required_capabilities/up.sql"),
    "\n",
    include_str!("../../migrations/20260607000002_harvest_workflow_pause/up.sql"),
    "\n",
    include_str!("../../migrations/20260609000001_harvest_workflow_current_details/up.sql"),
    "\n",
    include_str!("../../migrations/20260610000001_harvest_schedule_bounded_runs/up.sql"),
    "\n",
    include_str!("../../migrations/20260613000001_harvest_schedule_catchup_window/up.sql"),
    "\n",
    include_str!("../../migrations/20260616000001_harvest_workflow_schedule_id/up.sql"),
    "\n",
    include_str!("../../migrations/20260615000001_harvest_context_headers/up.sql"),
    "\n",
    include_str!("../../migrations/20260618000001_harvest_debounce/up.sql"),
    "\n",
    include_str!("../../migrations/20260624000000_harvest_event_batches/up.sql"),
    "\n",
    include_str!("../../migrations/20260626000001_harvest_workflow_retry/up.sql"),
    "\n",
    include_str!("../../migrations/20260628000001_harvest_execution_origin/up.sql"),
    include_str!("../../migrations/20260703000000_harvest_task_queue_wake_requested/up.sql"),
    include_str!("../../migrations/20260704000000_harvest_workflow_nd_block/up.sql"),
    "\n",
    include_str!("../../migrations/20260705000000_harvest_completion_deliveries/up.sql"),
    include_str!("../../migrations/20260706000000_harvest_worker_sessions/up.sql"),
    include_str!("../../migrations/20260710000000_harvest_workflow_continue_chain/up.sql"),
);

async fn setup() -> (String, ContainerAsync<Postgres>) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, container)
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url)
        .await
        .expect("connect failed")
}

/// Build a raw connection pool for a database URL — used to construct a
/// [`ShardedDbPool`] test fixture (mirrors
/// `autumn-harvest-plugin/tests/shard_health_integration.rs`'s identically
/// named helper).
fn build_test_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("failed to build test pool")
}

/// A deliverer that returns a scripted sequence of outcomes (repeating the
/// last one once exhausted) and records every call for assertions.
#[derive(Default)]
struct ScriptedDeliverer {
    responses: Mutex<Vec<DeliveryAttempt>>,
    calls: Mutex<Vec<(String, Vec<u8>, Vec<(&'static str, String)>)>>,
}

impl ScriptedDeliverer {
    fn new(responses: Vec<DeliveryAttempt>) -> Self {
        Self {
            responses: Mutex::new(responses),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn call_count(&self) -> usize {
        self.calls.lock().unwrap().len()
    }
}

impl CompletionCallbackDeliverer for ScriptedDeliverer {
    fn deliver<'a>(
        &'a self,
        target_url: &'a str,
        body: &'a [u8],
        headers: &'a [(&'static str, String)],
    ) -> DeliverFuture<'a> {
        Box::pin(async move {
            self.calls.lock().unwrap().push((
                target_url.to_string(),
                body.to_vec(),
                headers.to_vec(),
            ));
            let mut responses = self.responses.lock().unwrap();
            if responses.len() > 1 {
                responses.remove(0)
            } else {
                responses[0].clone()
            }
        })
    }
}

fn install_config(deliverer: Arc<dyn CompletionCallbackDeliverer>, retry_policy: RetryPolicy) {
    *GLOBAL_CALLBACK_CONFIG.write().unwrap() = Some(Arc::new(CallbackRuntimeConfig {
        deliverer,
        secret: CallbackSecret::new(b"test-secret".to_vec()),
        ssrf_policy: SsrfPolicy::new(HostAllowlist::new().with_pattern("api.example.com")),
        default_targets: Vec::new(),
        retry_policy,
    }));
}

async fn insert_terminal_execution(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    state: &str,
    output: Option<&str>,
    error: Option<&str>,
    completion_callbacks: Option<&str>,
) {
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions
            (id, workflow_name, workflow_id, shard_id, input, state, output, error, completed_at, completion_callbacks)
         VALUES ($1, 'process_refund', 'wf-1', 0, 'null'::jsonb, $2, $3::jsonb, $4, now(), $5::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(state)
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(output)
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(error)
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(completion_callbacks)
    .execute(conn)
    .await
    .expect("insert execution");
}

#[derive(diesel::QueryableByName, Debug)]
struct DeliveryRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    state: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    target_url: String,
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    next_attempt_at: chrono::DateTime<chrono::Utc>,
}

async fn load_deliveries(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> Vec<DeliveryRow> {
    diesel::sql_query(
        "SELECT state, target_url, next_attempt_at FROM harvest_completion_deliveries \
         WHERE workflow_exec_id = $1 ORDER BY callback_index ASC",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .load(conn)
    .await
    .expect("load deliveries")
}

#[tokio::test]
async fn enqueue_on_completion_creates_one_row_per_matching_target() {
    let _guard = TEST_SERIAL.lock().await;
    let (url, _container) = setup().await;
    let mut conn = connect(&url).await;
    install_config(
        Arc::new(ScriptedDeliverer::new(vec![DeliveryAttempt::success(200)])),
        RetryPolicy::exponential(3, Duration::from_secs(1)),
    );

    let exec_id = ExecutionId::new();
    insert_terminal_execution(
        &mut conn,
        exec_id,
        "COMPLETED",
        Some(r#"{"refunded":true}"#),
        None,
        Some(
            r#"[{"url":"https://api.example.com/hook","filter":{"type":"AnyTerminal"}},
                {"url":"https://api.example.com/other","filter":{"type":"CompletedOnly"}}]"#,
        ),
    )
    .await;

    evaluate_triggers_for_execution(&mut conn, exec_id, TerminalState::Completed, None)
        .await
        .expect("evaluate triggers");

    let rows = load_deliveries(&mut conn, exec_id).await;
    assert_eq!(rows.len(), 2, "both targets match TerminalState::Completed");
    assert_eq!(rows[0].state, "PENDING");
    assert_eq!(rows[0].target_url, "https://api.example.com/hook");
    assert_eq!(rows[1].state, "PENDING");
    assert_eq!(rows[1].target_url, "https://api.example.com/other");
}

#[tokio::test]
async fn enqueue_skips_targets_whose_filter_does_not_match() {
    let _guard = TEST_SERIAL.lock().await;
    let (url, _container) = setup().await;
    let mut conn = connect(&url).await;
    install_config(
        Arc::new(ScriptedDeliverer::new(vec![DeliveryAttempt::success(200)])),
        RetryPolicy::exponential(3, Duration::from_secs(1)),
    );

    let exec_id = ExecutionId::new();
    insert_terminal_execution(
        &mut conn,
        exec_id,
        "FAILED",
        None,
        Some("card declined"),
        Some(r#"[{"url":"https://api.example.com/hook","filter":{"type":"CompletedOnly"}}]"#),
    )
    .await;

    evaluate_triggers_for_execution(&mut conn, exec_id, TerminalState::Failed, None)
        .await
        .expect("evaluate triggers");

    let rows = load_deliveries(&mut conn, exec_id).await;
    assert!(rows.is_empty(), "CompletedOnly must not fire on Failed");
}

#[tokio::test]
async fn enqueue_is_idempotent_on_re_evaluation() {
    let _guard = TEST_SERIAL.lock().await;
    let (url, _container) = setup().await;
    let mut conn = connect(&url).await;
    install_config(
        Arc::new(ScriptedDeliverer::new(vec![DeliveryAttempt::success(200)])),
        RetryPolicy::exponential(3, Duration::from_secs(1)),
    );

    let exec_id = ExecutionId::new();
    insert_terminal_execution(
        &mut conn,
        exec_id,
        "COMPLETED",
        Some("{}"),
        None,
        Some(r#"[{"url":"https://api.example.com/hook","filter":{"type":"AnyTerminal"}}]"#),
    )
    .await;

    evaluate_triggers_for_execution(&mut conn, exec_id, TerminalState::Completed, None)
        .await
        .expect("first evaluate");
    evaluate_triggers_for_execution(&mut conn, exec_id, TerminalState::Completed, None)
        .await
        .expect("second evaluate (simulates a parent-close-cascade re-entry)");

    let rows = load_deliveries(&mut conn, exec_id).await;
    assert_eq!(rows.len(), 1, "must not double-enqueue on re-evaluation");
}

#[tokio::test]
async fn no_targets_configured_enqueues_nothing() {
    let _guard = TEST_SERIAL.lock().await;
    let (url, _container) = setup().await;
    let mut conn = connect(&url).await;
    install_config(
        Arc::new(ScriptedDeliverer::new(vec![DeliveryAttempt::success(200)])),
        RetryPolicy::exponential(3, Duration::from_secs(1)),
    );

    let exec_id = ExecutionId::new();
    // No completion_callbacks and no builder defaults configured.
    insert_terminal_execution(&mut conn, exec_id, "COMPLETED", Some("{}"), None, None).await;

    evaluate_triggers_for_execution(&mut conn, exec_id, TerminalState::Completed, None)
        .await
        .expect("evaluate triggers");

    let rows = load_deliveries(&mut conn, exec_id).await;
    assert!(
        rows.is_empty(),
        "a callback-less workflow must behave identically to today: zero rows"
    );
}

/// A deliverer that sleeps a fixed duration before returning, so a test can
/// assert on whether a batch of deliveries ran concurrently or sequentially.
#[derive(Default)]
struct SlowDeliverer {
    delay: Duration,
    call_count: std::sync::atomic::AtomicUsize,
}

impl SlowDeliverer {
    fn new(delay: Duration) -> Self {
        Self {
            delay,
            call_count: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl CompletionCallbackDeliverer for SlowDeliverer {
    fn deliver<'a>(
        &'a self,
        _target_url: &'a str,
        _body: &'a [u8],
        _headers: &'a [(&'static str, String)],
    ) -> DeliverFuture<'a> {
        Box::pin(async move {
            tokio::time::sleep(self.delay).await;
            self.call_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            DeliveryAttempt::success(200)
        })
    }
}

// Regression (issue #605 code review): a batch of due deliveries must be
// dispatched concurrently, not one HTTP POST at a time -- a sequential loop
// would hold the scanner's single pooled connection for the sum of every
// attempt's latency, which for a full batch of slow/unreachable targets
// could stretch a single tick to tens of minutes.
#[tokio::test]
async fn scanner_dispatches_a_batch_of_deliveries_concurrently_not_sequentially() {
    let _guard = TEST_SERIAL.lock().await;
    let (url, _container) = setup().await;
    let mut conn = connect(&url).await;

    const N: usize = 5;
    const DELAY: Duration = Duration::from_millis(300);
    let deliverer = Arc::new(SlowDeliverer::new(DELAY));
    install_config(
        deliverer.clone(),
        RetryPolicy::exponential(3, Duration::from_secs(1)),
    );

    let exec_id = ExecutionId::new();
    let targets: Vec<serde_json::Value> = (0..N)
        .map(|i| {
            serde_json::json!({
                "url": format!("https://api.example.com/hook-{i}"),
                "filter": { "type": "AnyTerminal" }
            })
        })
        .collect();
    let callbacks_json = serde_json::to_string(&targets).unwrap();
    insert_terminal_execution(
        &mut conn,
        exec_id,
        "COMPLETED",
        Some("{}"),
        None,
        Some(&callbacks_json),
    )
    .await;
    evaluate_triggers_for_execution(&mut conn, exec_id, TerminalState::Completed, None)
        .await
        .expect("evaluate triggers");

    let rows = load_deliveries(&mut conn, exec_id).await;
    assert_eq!(rows.len(), N, "one row per target");

    let started = std::time::Instant::now();
    let processed = fire_due_completion_deliveries(&mut conn, &None, &[])
        .await
        .expect("scanner tick");
    let elapsed = started.elapsed();

    assert_eq!(
        processed, N,
        "the whole batch must be processed in one tick"
    );
    let call_count = std::sync::atomic::AtomicUsize::load(
        &deliverer.call_count,
        std::sync::atomic::Ordering::SeqCst,
    );
    assert_eq!(call_count, N);
    // Sequential dispatch would take at least N * DELAY (~1.5s for N=5,
    // 300ms). Concurrent dispatch takes roughly one DELAY plus overhead.
    // Use a generous bound well below the sequential floor to keep this
    // robust under CI scheduling jitter while still failing if dispatch
    // regresses to a sequential loop.
    assert!(
        elapsed < DELAY * 2,
        "a batch of {N} deliveries each taking {DELAY:?} took {elapsed:?} -- \
         this is consistent with sequential dispatch, not concurrent"
    );
}

#[tokio::test]
async fn scanner_marks_delivered_on_2xx() {
    let _guard = TEST_SERIAL.lock().await;
    let (url, _container) = setup().await;
    let mut conn = connect(&url).await;
    let deliverer = Arc::new(ScriptedDeliverer::new(vec![DeliveryAttempt::success(204)]));
    install_config(
        deliverer.clone(),
        RetryPolicy::exponential(3, Duration::from_secs(1)),
    );

    let exec_id = ExecutionId::new();
    insert_terminal_execution(
        &mut conn,
        exec_id,
        "COMPLETED",
        Some(r#"{"refunded":true}"#),
        None,
        Some(r#"[{"url":"https://api.example.com/hook","filter":{"type":"AnyTerminal"}}]"#),
    )
    .await;
    evaluate_triggers_for_execution(&mut conn, exec_id, TerminalState::Completed, None)
        .await
        .expect("evaluate triggers");

    let processed = fire_due_completion_deliveries(&mut conn, &None, &[])
        .await
        .expect("scanner tick");
    assert_eq!(processed, 1);

    let rows = load_deliveries(&mut conn, exec_id).await;
    assert_eq!(rows[0].state, "DELIVERED");
    assert_eq!(deliverer.call_count(), 1);

    let calls = deliverer.calls.lock().unwrap();
    let (posted_url, body, headers) = &calls[0];
    assert_eq!(posted_url, "https://api.example.com/hook");
    let body_str = String::from_utf8(body.clone()).unwrap();
    assert!(body_str.contains("\"delivery_id\""));
    assert!(body_str.contains("\"execution_id\""));
    assert!(body_str.contains("\"state\":\"COMPLETED\""));
    assert!(body_str.contains("\"refunded\":true"));
    assert!(!body_str.contains("\"error\""), "COMPLETED must omit error");
    assert!(
        headers
            .iter()
            .any(|(k, v)| *k == "X-Harvest-Signature" && v.starts_with("sha256=")),
        "must carry the HMAC signature header"
    );
    assert!(
        headers.iter().any(|(k, _)| *k == "X-Harvest-Timestamp"),
        "must carry the timestamp header"
    );
}

#[tokio::test]
async fn scanner_reschedules_with_backoff_on_non_2xx() {
    let _guard = TEST_SERIAL.lock().await;
    let (url, _container) = setup().await;
    let mut conn = connect(&url).await;
    let deliverer = Arc::new(ScriptedDeliverer::new(vec![DeliveryAttempt::success(500)]));
    install_config(
        deliverer.clone(),
        RetryPolicy::exponential(5, Duration::from_secs(60)),
    );

    let exec_id = ExecutionId::new();
    insert_terminal_execution(
        &mut conn,
        exec_id,
        "COMPLETED",
        Some("{}"),
        None,
        Some(r#"[{"url":"https://api.example.com/hook","filter":{"type":"AnyTerminal"}}]"#),
    )
    .await;
    evaluate_triggers_for_execution(&mut conn, exec_id, TerminalState::Completed, None)
        .await
        .expect("evaluate triggers");

    let before = chrono::Utc::now();
    let processed = fire_due_completion_deliveries(&mut conn, &None, &[])
        .await
        .expect("scanner tick");
    assert_eq!(processed, 1);

    let rows = load_deliveries(&mut conn, exec_id).await;
    assert_eq!(
        rows[0].state, "PENDING",
        "not yet exhausted -> reschedule, not FAILED"
    );
    assert!(
        rows[0].next_attempt_at > before,
        "backoff must push next_attempt_at into the future"
    );

    // The scanner must not re-fire it again on the very next tick since
    // it isn't due yet.
    let processed_again = fire_due_completion_deliveries(&mut conn, &None, &[])
        .await
        .expect("second scanner tick");
    assert_eq!(processed_again, 0);
    assert_eq!(deliverer.call_count(), 1);
}

/// A deliverer that sleeps a fixed duration before returning a failing
/// (non-2xx) response, so a test can assert on backoff scheduling relative
/// to when the attempt actually completed rather than when it was claimed.
struct SlowFailingDeliverer {
    delay: Duration,
    status: u16,
}

impl CompletionCallbackDeliverer for SlowFailingDeliverer {
    fn deliver<'a>(
        &'a self,
        _target_url: &'a str,
        _body: &'a [u8],
        _headers: &'a [(&'static str, String)],
    ) -> DeliverFuture<'a> {
        Box::pin(async move {
            tokio::time::sleep(self.delay).await;
            DeliveryAttempt::success(self.status)
        })
    }
}

// Regression (issue #921 review, Codex P2): `next_attempt_at` must be
// scheduled from *after* the attempt actually completed, not from the
// claim-time timestamp captured before dispatch. A deliverer whose attempt
// takes longer than the configured backoff delay would otherwise get a
// `next_attempt_at` that is already in the past by the time the outcome is
// written, so the scanner's very next tick would immediately retry instead
// of honoring the backoff -- hammering an unhealthy receiver.
#[tokio::test]
async fn scanner_schedules_backoff_from_attempt_completion_not_claim_time() {
    let _guard = TEST_SERIAL.lock().await;
    let (url, _container) = setup().await;
    let mut conn = connect(&url).await;

    // The attempt takes 300ms; the configured backoff delay is only 50ms.
    // With the bug, `next_attempt_at` = (pre-dispatch claim time) + 50ms,
    // which is already ~250ms in the past by the time this assertion runs.
    // With the fix, `next_attempt_at` = (post-dispatch completion time) +
    // 50ms, which is still ~50ms in the future.
    let deliverer = Arc::new(SlowFailingDeliverer {
        delay: Duration::from_millis(300),
        status: 500,
    });
    install_config(
        deliverer.clone(),
        RetryPolicy::exponential(5, Duration::from_millis(50)),
    );

    let exec_id = ExecutionId::new();
    insert_terminal_execution(
        &mut conn,
        exec_id,
        "COMPLETED",
        Some("{}"),
        None,
        Some(r#"[{"url":"https://api.example.com/hook","filter":{"type":"AnyTerminal"}}]"#),
    )
    .await;
    evaluate_triggers_for_execution(&mut conn, exec_id, TerminalState::Completed, None)
        .await
        .expect("evaluate triggers");

    let processed = fire_due_completion_deliveries(&mut conn, &None, &[])
        .await
        .expect("scanner tick");
    assert_eq!(processed, 1);

    let after_tick = chrono::Utc::now();
    let rows = load_deliveries(&mut conn, exec_id).await;
    assert_eq!(rows[0].state, "PENDING");
    assert!(
        rows[0].next_attempt_at > after_tick,
        "next_attempt_at ({:?}) must still be in the future relative to when the tick \
         finished ({after_tick:?}) -- backoff must anchor to attempt completion, not claim time",
        rows[0].next_attempt_at
    );
}

// Regression (issue #921 review, Codex P1): a completion delivery that is
// concurrently claimed for its final (exhausting) attempt must not
// re-introduce PII into `harvest_dead_letters.input` if
// `erase_workflow_payloads` tombstones the delivery's payload *after* the
// claim but *before* the outcome is recorded. `apply_outcome`'s DeadLetter
// branch used to build the DLQ entry from the in-memory `row.payload`
// snapshot captured at claim time; if erase committed in the meantime, that
// snapshot is now stale and dead-lettering with it would resurrect the
// erased PII in a brand new DLQ row. Fixed by re-reading the payload from
// the DB (under `FOR UPDATE`, serializing against a concurrent erase) inside
// the same transaction that records the DeadLetter outcome.
#[tokio::test]
async fn erase_racing_a_dead_letter_write_does_not_reintroduce_pii() {
    let _guard = TEST_SERIAL.lock().await;
    let (url, _container) = setup().await;
    let mut conn = connect(&url).await;
    let mut erase_conn = connect(&url).await;

    let pii = "dave@example.com";
    // The attempt takes 300ms; the erase races in after only 50ms, so by the
    // time the deliverer's failure resolves and `apply_outcome` runs, the
    // erase transaction has already committed.
    let deliverer = Arc::new(SlowFailingDeliverer {
        delay: Duration::from_millis(300),
        status: 500,
    });
    // max_attempts = 1 -> the very first (and only) attempt exhausts.
    install_config(
        deliverer.clone(),
        RetryPolicy::exponential(1, Duration::from_millis(1)),
    );

    let exec_id = ExecutionId::new();
    insert_terminal_execution(
        &mut conn,
        exec_id,
        "COMPLETED",
        Some(&format!(r#"{{"email":"{pii}"}}"#)),
        None,
        Some(r#"[{"url":"https://api.example.com/hook","filter":{"type":"AnyTerminal"}}]"#),
    )
    .await;
    evaluate_triggers_for_execution(&mut conn, exec_id, TerminalState::Completed, None)
        .await
        .expect("evaluate triggers");

    let rows = list_deliveries_for_execution(&mut conn, exec_id)
        .await
        .expect("list deliveries");
    let delivery_id = rows[0].id;
    assert!(
        rows[0].payload.to_string().contains(pii),
        "sanity: the frozen payload must carry the PII before erasure"
    );

    let scanner_fut = fire_due_completion_deliveries(&mut conn, &None, &[]);
    let erase_fut = async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        autumn_harvest::erase::erase_workflow_payloads(&mut erase_conn, exec_id, "test race")
            .await
            .expect("erase should succeed")
    };
    let (processed, _erase_outcome) = tokio::join!(scanner_fut, erase_fut);
    assert_eq!(processed.expect("scanner tick"), 1);

    let rows_after = list_deliveries_for_execution(&mut conn, exec_id)
        .await
        .expect("list deliveries after race");
    assert_eq!(rows_after[0].state, "FAILED");
    assert!(
        !rows_after[0].payload.to_string().contains(pii),
        "the delivery's own payload must stay erased"
    );

    #[derive(diesel::QueryableByName)]
    struct DeadLetterInput {
        #[diesel(sql_type = diesel::sql_types::Jsonb)]
        input: serde_json::Value,
    }
    let dlq_rows: Vec<DeadLetterInput> =
        diesel::sql_query("SELECT input FROM harvest_dead_letters WHERE original_task_id = $1")
            .bind::<diesel::sql_types::Uuid, _>(delivery_id)
            .load(&mut conn)
            .await
            .expect("load dead-letter rows");
    assert_eq!(
        dlq_rows.len(),
        1,
        "exactly one dead-letter row must be written"
    );
    assert!(
        !dlq_rows[0].input.to_string().contains(pii),
        "the dead-letter's input must not resurrect the erased PII: {}",
        dlq_rows[0].input
    );
}

#[tokio::test]
async fn scanner_dead_letters_on_retry_exhaustion() {
    let _guard = TEST_SERIAL.lock().await;
    let (url, _container) = setup().await;
    let mut conn = connect(&url).await;
    let deliverer = Arc::new(ScriptedDeliverer::new(vec![DeliveryAttempt::success(503)]));
    // max_attempts = 2, ~instant backoff so the test doesn't need to sleep.
    install_config(
        deliverer.clone(),
        RetryPolicy::exponential(2, Duration::from_millis(1)),
    );

    let exec_id = ExecutionId::new();
    insert_terminal_execution(
        &mut conn,
        exec_id,
        "FAILED",
        None,
        Some("card declined"),
        Some(r#"[{"url":"https://api.example.com/hook","filter":{"type":"AnyTerminal"}}]"#),
    )
    .await;
    evaluate_triggers_for_execution(&mut conn, exec_id, TerminalState::Failed, None)
        .await
        .expect("evaluate triggers");

    for _ in 0..2 {
        // Force due immediately so the second attempt doesn't need to wait
        // out the (tiny, but nonzero) backoff interval.
        diesel::sql_query(
            "UPDATE harvest_completion_deliveries SET next_attempt_at = now() WHERE workflow_exec_id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .execute(&mut conn)
        .await
        .expect("force due");
        fire_due_completion_deliveries(&mut conn, &None, &[])
            .await
            .expect("scanner tick");
    }

    let rows = load_deliveries(&mut conn, exec_id).await;
    assert_eq!(rows[0].state, "FAILED");
    assert_eq!(deliverer.call_count(), 2);

    #[derive(diesel::QueryableByName)]
    struct DlqRow {
        #[diesel(sql_type = diesel::sql_types::Text)]
        task_type: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        error: String,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Uuid>)]
        workflow_exec_id: Option<uuid::Uuid>,
    }
    let dlq: Vec<DlqRow> = diesel::sql_query(
        "SELECT task_type, error, workflow_exec_id FROM harvest_dead_letters WHERE workflow_exec_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .load(&mut conn)
    .await
    .expect("load dlq");

    assert_eq!(dlq.len(), 1, "exactly one DLQ row on exhaustion");
    assert_eq!(
        dlq[0].task_type, "CALLBACK",
        "distinct from workflow/activity task types"
    );
    assert_eq!(dlq[0].workflow_exec_id, Some(exec_id.as_uuid()));
    assert!(dlq[0].error.contains("CallbackDeliveryExhausted"));
    assert!(
        !dlq[0].error.contains("PoisonPill") && !dlq[0].error.contains("WorkflowTaskTimeout"),
        "typed reason must be distinct from clean task-retry exhaustion reasons"
    );
}

// Regression (issue #921 review, Codex P2): `target_url` is only ever
// checked against the SSRF allowlist at enqueue time. A delivery can sit
// PENDING/INFLIGHT/FAILED for a long time -- or be manually redriven long
// after enqueue -- so an operator who tightens the allowlist (removing a
// compromised or decommissioned host) after enqueue must have that change
// actually enforced on the next dispatch attempt, not just for brand-new
// deliveries.
#[tokio::test]
async fn scanner_rejects_dispatch_when_target_is_no_longer_allowlisted() {
    let _guard = TEST_SERIAL.lock().await;
    let (url, _container) = setup().await;
    let mut conn = connect(&url).await;
    let deliverer = Arc::new(ScriptedDeliverer::new(vec![DeliveryAttempt::success(200)]));
    install_config(
        deliverer.clone(),
        RetryPolicy::exponential(3, Duration::from_secs(60)),
    );

    let exec_id = ExecutionId::new();
    insert_terminal_execution(
        &mut conn,
        exec_id,
        "COMPLETED",
        Some(r#"{"refunded":true}"#),
        None,
        Some(r#"[{"url":"https://api.example.com/hook","filter":{"type":"AnyTerminal"}}]"#),
    )
    .await;
    evaluate_triggers_for_execution(&mut conn, exec_id, TerminalState::Completed, None)
        .await
        .expect("evaluate triggers");

    let rows = list_deliveries_for_execution(&mut conn, exec_id)
        .await
        .expect("list deliveries");
    assert_eq!(rows[0].state, "PENDING", "sanity: enqueue succeeded");

    // Simulate an operator tightening the allowlist after this delivery was
    // already enqueued: reinstall the runtime config with an allowlist that
    // no longer covers "api.example.com".
    *GLOBAL_CALLBACK_CONFIG.write().unwrap() = Some(Arc::new(CallbackRuntimeConfig {
        deliverer: deliverer.clone(),
        secret: CallbackSecret::new(b"test-secret".to_vec()),
        ssrf_policy: SsrfPolicy::new(HostAllowlist::new()),
        default_targets: Vec::new(),
        retry_policy: RetryPolicy::exponential(3, Duration::from_secs(60)),
    }));

    let processed = fire_due_completion_deliveries(&mut conn, &None, &[])
        .await
        .expect("scanner tick");
    assert_eq!(processed, 1);

    assert_eq!(
        deliverer.call_count(),
        0,
        "the deliverer must never be invoked for a target the live policy no longer allows"
    );

    let rows_after = list_deliveries_for_execution(&mut conn, exec_id)
        .await
        .expect("list deliveries after tick");
    assert_eq!(
        rows_after[0].state, "FAILED",
        "a rejected target must be dead-lettered, not silently retried forever"
    );
    assert!(
        rows_after[0]
            .last_error
            .as_deref()
            .unwrap_or_default()
            .contains("SSRF policy"),
        "last_error must explain the rejection: {:?}",
        rows_after[0].last_error
    );
}

async fn exhaust_one_callback_delivery(
    conn: &mut AsyncPgConnection,
    workflow_id: &str,
) -> (ExecutionId, uuid::Uuid, uuid::Uuid) {
    let deliverer = Arc::new(ScriptedDeliverer::new(vec![DeliveryAttempt::success(503)]));
    install_config(
        deliverer,
        RetryPolicy::exponential(1, Duration::from_millis(1)),
    );

    let exec_id = ExecutionId::new();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions
            (id, workflow_name, workflow_id, shard_id, input, state, output, error, completed_at, completion_callbacks)
         VALUES ($1, 'process_refund', $2, 0, 'null'::jsonb, 'FAILED', NULL, 'card declined', now(), $3::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(workflow_id)
    .bind::<diesel::sql_types::Text, _>(
        r#"[{"url":"https://api.example.com/hook","filter":{"type":"AnyTerminal"}}]"#,
    )
    .execute(conn)
    .await
    .expect("insert execution");
    evaluate_triggers_for_execution(conn, exec_id, TerminalState::Failed, None)
        .await
        .expect("evaluate triggers");

    fire_due_completion_deliveries(conn, &None, &[])
        .await
        .expect("scanner tick exhausts on first attempt");

    let rows = list_deliveries_for_execution(conn, exec_id)
        .await
        .expect("list deliveries");
    assert_eq!(rows[0].state, "FAILED");
    let delivery_id = rows[0].id;

    #[derive(diesel::QueryableByName)]
    struct DlqId {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: uuid::Uuid,
    }
    let dlq: Vec<DlqId> = diesel::sql_query(
        "SELECT id FROM harvest_dead_letters WHERE original_task_id = $1 AND task_type = 'CALLBACK'",
    )
    .bind::<diesel::sql_types::Uuid, _>(delivery_id)
    .load(conn)
    .await
    .expect("load dlq row");
    assert_eq!(dlq.len(), 1, "exactly one CALLBACK dead-letter row");

    (exec_id, delivery_id, dlq[0].id)
}

// Regression (issue #921 review, Codex P2): the generic DLQ "replay" action
// (API/bulk-replay/UI) used to error with "invalid task_type 'CALLBACK'"
// for a completion-callback dead letter instead of actually redriving the
// delivery. `dlq::replay_dead_letter` now delegates to the completion-
// delivery redrive primitive for a CALLBACK row.
#[tokio::test]
async fn replay_dead_letter_delegates_a_callback_row_to_completion_delivery_redrive() {
    let _guard = TEST_SERIAL.lock().await;
    let (url, _container) = setup().await;
    let mut conn = connect(&url).await;

    let (exec_id, delivery_id, dead_letter_id) =
        exhaust_one_callback_delivery(&mut conn, "replay-cb-wf").await;

    let replayed_id = autumn_harvest::dlq::replay_dead_letter(&mut conn, dead_letter_id, None)
        .await
        .expect("replay_dead_letter must delegate instead of erroring");
    assert_eq!(
        replayed_id, delivery_id,
        "replay_dead_letter returns the delivery's own id for a CALLBACK row"
    );

    let rows = list_deliveries_for_execution(&mut conn, exec_id)
        .await
        .expect("list deliveries after replay");
    assert_eq!(rows[0].state, "PENDING", "delivery must be redriven");

    #[derive(diesel::QueryableByName)]
    struct DlqCount {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        count: i64,
    }
    let after: DlqCount = diesel::sql_query(
        "SELECT count(*) as count FROM harvest_dead_letters WHERE original_task_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(delivery_id)
    .get_result(&mut conn)
    .await
    .expect("count dlq rows");
    assert_eq!(
        after.count, 0,
        "the DLQ row must be cleared by the delegated redrive"
    );
}

// Regression (issue #921 review, Codex P2): same delegation, exercised via
// `redrive_dead_letter` (issue #510's execution-reactivating primitive) --
// the other generic entry point the DLQ UI/API can reach a CALLBACK row
// through.
#[tokio::test]
async fn redrive_dead_letter_delegates_a_callback_row_to_completion_delivery_redrive() {
    let _guard = TEST_SERIAL.lock().await;
    let (url, _container) = setup().await;
    let mut conn = connect(&url).await;

    let (exec_id, delivery_id, dead_letter_id) =
        exhaust_one_callback_delivery(&mut conn, "redrive-cb-wf").await;

    let outcome = autumn_harvest::dlq::redrive_dead_letter(&mut conn, dead_letter_id, None, None)
        .await
        .expect("redrive_dead_letter must delegate instead of erroring");
    assert_eq!(
        outcome,
        autumn_harvest::dlq::RedriveOutcome::Redriven(delivery_id)
    );

    let rows = list_deliveries_for_execution(&mut conn, exec_id)
        .await
        .expect("list deliveries after redrive");
    assert_eq!(rows[0].state, "PENDING", "delivery must be redriven");

    // A second call is idempotent: the DLQ row is already gone, so this
    // mirrors the "already redriven" Skipped semantics for workflow/activity
    // dead letters.
    let outcome_again =
        autumn_harvest::dlq::redrive_dead_letter(&mut conn, dead_letter_id, None, None)
            .await
            .expect("second redrive must not error");
    assert_eq!(outcome_again, autumn_harvest::dlq::RedriveOutcome::Skipped);
}

#[tokio::test]
async fn scanner_ignores_rows_not_yet_due() {
    let _guard = TEST_SERIAL.lock().await;
    let (url, _container) = setup().await;
    let mut conn = connect(&url).await;
    let deliverer = Arc::new(ScriptedDeliverer::new(vec![DeliveryAttempt::success(200)]));
    // A long initial interval means the row is nowhere near due after enqueue.
    install_config(
        deliverer.clone(),
        RetryPolicy::exponential(3, Duration::from_secs(3600)),
    );

    let exec_id = ExecutionId::new();
    insert_terminal_execution(
        &mut conn,
        exec_id,
        "COMPLETED",
        Some("{}"),
        None,
        Some(r#"[{"url":"https://api.example.com/hook","filter":{"type":"AnyTerminal"}}]"#),
    )
    .await;
    evaluate_triggers_for_execution(&mut conn, exec_id, TerminalState::Completed, None)
        .await
        .expect("evaluate triggers");

    // Freshly enqueued rows have next_attempt_at = NOW(), so they *are* due
    // on the very first tick — deliver it once, then push it artificially
    // into the future to prove a not-yet-due row is skipped.
    fire_due_completion_deliveries(&mut conn, &None, &[])
        .await
        .expect("first tick delivers immediately");
    assert_eq!(deliverer.call_count(), 1);

    diesel::sql_query(
        "UPDATE harvest_completion_deliveries SET state = 'PENDING', next_attempt_at = now() + interval '1 hour' WHERE workflow_exec_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut conn)
    .await
    .expect("push into the future");

    let processed = fire_due_completion_deliveries(&mut conn, &None, &[])
        .await
        .expect("second tick");
    assert_eq!(processed, 0, "a not-yet-due row must be left alone");
    assert_eq!(deliverer.call_count(), 1, "no additional delivery attempt");
}

/// Guards against issue #921 review (Codex P2): `claim_due_deliveries`
/// previously claimed *any* due row in the connected database, with no
/// `shard_id` predicate. When two logical shards are backed by the same
/// physical Postgres database (a supported deployment shape — see
/// `docs/sharding.md` and the identical scoping precedent in
/// `execution::non_terminal_counts_by_workflow_name`/
/// `event_batch::fire_due_on_conn`), a shard-0 scanner tick could claim and
/// POST a shard-1 delivery, violating shard assignment and starving the
/// shard-1 scan.
///
/// This test simulates exactly that: two enqueued deliveries, one left on
/// shard 0 and one moved to shard 1, both served by pools that point at the
/// *same* underlying database — then runs the scanner assigned to shard 0
/// only and asserts the shard-1 row is left completely untouched.
#[tokio::test]
async fn scanner_scopes_claims_to_the_assigned_shard_when_shards_share_a_pool() {
    let _guard = TEST_SERIAL.lock().await;
    let (url, _container) = setup().await;
    let mut conn = connect(&url).await;
    let deliverer = Arc::new(ScriptedDeliverer::new(vec![DeliveryAttempt::success(200)]));
    install_config(
        deliverer.clone(),
        RetryPolicy::exponential(3, Duration::from_secs(1)),
    );

    let exec_shard0 = ExecutionId::new();
    insert_terminal_execution(
        &mut conn,
        exec_shard0,
        "COMPLETED",
        Some("{}"),
        None,
        Some(r#"[{"url":"https://api.example.com/shard0","filter":{"type":"AnyTerminal"}}]"#),
    )
    .await;
    evaluate_triggers_for_execution(&mut conn, exec_shard0, TerminalState::Completed, None)
        .await
        .expect("evaluate triggers shard0");

    let exec_shard1 = ExecutionId::new();
    insert_terminal_execution(
        &mut conn,
        exec_shard1,
        "COMPLETED",
        Some("{}"),
        None,
        Some(r#"[{"url":"https://api.example.com/shard1","filter":{"type":"AnyTerminal"}}]"#),
    )
    .await;
    evaluate_triggers_for_execution(&mut conn, exec_shard1, TerminalState::Completed, None)
        .await
        .expect("evaluate triggers shard1");

    // `insert_terminal_execution` always seeds `shard_id = 0`; move the
    // second delivery's row onto logical shard 1 to simulate the
    // shared-physical-database scenario the fix guards against.
    diesel::sql_query(
        "UPDATE harvest_completion_deliveries SET shard_id = 1 WHERE workflow_exec_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_shard1.as_uuid())
    .execute(&mut conn)
    .await
    .expect("move delivery to shard 1");

    // Both ShardId(0) and ShardId(1) point at the SAME physical database --
    // the shared-pool deployment shape this test guards.
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_test_pool(&url));
    pools.insert(ShardId::new(1), build_test_pool(&url));
    let sharded_pool = ShardedDbPool::from_map(pools, ShardId::new(0));

    // Run the scanner assigned to shard 0 only.
    let processed =
        fire_due_completion_deliveries(&mut conn, &Some(sharded_pool), &[ShardId::new(0)])
            .await
            .expect("scanner tick for shard 0");

    assert_eq!(
        processed, 1,
        "only the shard-0 row should be claimed and delivered"
    );
    assert_eq!(deliverer.call_count(), 1);
    assert_eq!(
        deliverer.calls.lock().unwrap()[0].0,
        "https://api.example.com/shard0",
        "the shard-0-only scanner tick must not have delivered the shard-1 row"
    );

    let shard1_rows = load_deliveries(&mut conn, exec_shard1).await;
    assert_eq!(shard1_rows.len(), 1);
    assert_eq!(
        shard1_rows[0].state, "PENDING",
        "the shard-1 row must be left untouched by a shard-0-only scanner tick"
    );
}

#[tokio::test]
async fn list_deliveries_for_execution_returns_rows_in_callback_index_order() {
    let _guard = TEST_SERIAL.lock().await;
    let (url, _container) = setup().await;
    let mut conn = connect(&url).await;
    install_config(
        Arc::new(ScriptedDeliverer::new(vec![DeliveryAttempt::success(200)])),
        RetryPolicy::exponential(3, Duration::from_secs(1)),
    );

    let exec_id = ExecutionId::new();
    insert_terminal_execution(
        &mut conn,
        exec_id,
        "COMPLETED",
        Some("{}"),
        None,
        Some(
            r#"[{"url":"https://api.example.com/hook","filter":{"type":"AnyTerminal"}},
                {"url":"https://api.example.com/other","filter":{"type":"AnyTerminal"}}]"#,
        ),
    )
    .await;
    evaluate_triggers_for_execution(&mut conn, exec_id, TerminalState::Completed, None)
        .await
        .expect("evaluate triggers");

    let rows = list_deliveries_for_execution(&mut conn, exec_id)
        .await
        .expect("list deliveries");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].target_url, "https://api.example.com/hook");
    assert_eq!(rows[1].target_url, "https://api.example.com/other");
    assert_eq!(rows[0].callback_index, 0);
    assert_eq!(rows[1].callback_index, 1);
}

#[tokio::test]
async fn redrive_delivery_resets_a_failed_row_and_clears_its_dlq_entry() {
    let _guard = TEST_SERIAL.lock().await;
    let (url, _container) = setup().await;
    let mut conn = connect(&url).await;
    let deliverer = Arc::new(ScriptedDeliverer::new(vec![DeliveryAttempt::success(503)]));
    install_config(
        deliverer.clone(),
        RetryPolicy::exponential(1, Duration::from_millis(1)),
    );

    let exec_id = ExecutionId::new();
    insert_terminal_execution(
        &mut conn,
        exec_id,
        "FAILED",
        None,
        Some("card declined"),
        Some(r#"[{"url":"https://api.example.com/hook","filter":{"type":"AnyTerminal"}}]"#),
    )
    .await;
    evaluate_triggers_for_execution(&mut conn, exec_id, TerminalState::Failed, None)
        .await
        .expect("evaluate triggers");

    // max_attempts = 1 -> the very first tick exhausts the budget.
    fire_due_completion_deliveries(&mut conn, &None, &[])
        .await
        .expect("scanner tick exhausts on first attempt");
    let rows = list_deliveries_for_execution(&mut conn, exec_id)
        .await
        .expect("list deliveries");
    assert_eq!(rows[0].state, "FAILED");
    assert_eq!(rows[0].attempt, 1);
    assert_eq!(rows[0].max_attempts, 1);
    let delivery_id = rows[0].id;

    #[derive(diesel::QueryableByName)]
    struct DlqCount {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        count: i64,
    }
    let before: Vec<DlqCount> = diesel::sql_query(
        "SELECT count(*) as count FROM harvest_dead_letters WHERE original_task_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(delivery_id)
    .load(&mut conn)
    .await
    .expect("count dlq rows");
    assert_eq!(
        before[0].count, 1,
        "exhaustion must have written exactly one DLQ row"
    );

    let outcome = redrive_delivery(&mut conn, exec_id, delivery_id)
        .await
        .expect("redrive");
    assert_eq!(outcome, DeliveryRedriveOutcome::Redriven);

    let rows_after = list_deliveries_for_execution(&mut conn, exec_id)
        .await
        .expect("list deliveries after redrive");
    assert_eq!(rows_after[0].state, "PENDING");
    // `attempt` is deliberately *not* reset (issue #921 review, Codex P2):
    // reusing attempt numbers across a redrive could let a stale pre-redrive
    // response alias a fresh post-redrive claim's attempt count. Instead the
    // retry budget is refreshed by extending `max_attempts`.
    assert_eq!(
        rows_after[0].attempt, 1,
        "attempt must never be reset — only max_attempts is extended"
    );
    assert_eq!(
        rows_after[0].max_attempts, 2,
        "redrive extends max_attempts by the pre-redrive max_attempts (1 + 1 = 2)"
    );

    let after: Vec<DlqCount> = diesel::sql_query(
        "SELECT count(*) as count FROM harvest_dead_letters WHERE original_task_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(delivery_id)
    .load(&mut conn)
    .await
    .expect("count dlq rows after redrive");
    assert_eq!(
        after[0].count, 0,
        "redrive must clear the DLQ entry it created"
    );

    // The scanner picks the redriven row back up on its next tick.
    let processed = fire_due_completion_deliveries(&mut conn, &None, &[])
        .await
        .expect("post-redrive tick");
    assert_eq!(processed, 1);
    assert_eq!(
        deliverer.call_count(),
        2,
        "one call from exhaustion + one from redrive"
    );
}

#[tokio::test]
async fn redrive_delivery_is_a_no_op_on_an_unknown_id() {
    let _guard = TEST_SERIAL.lock().await;
    let (url, _container) = setup().await;
    let mut conn = connect(&url).await;

    let outcome = redrive_delivery(&mut conn, ExecutionId::new(), uuid::Uuid::new_v4())
        .await
        .expect("redrive unknown id");
    assert_eq!(outcome, DeliveryRedriveOutcome::NotFound);
}

#[tokio::test]
async fn redrive_delivery_is_a_no_op_when_delivery_belongs_to_a_different_execution() {
    let _guard = TEST_SERIAL.lock().await;
    let (url, _container) = setup().await;
    let mut conn = connect(&url).await;
    install_config(
        Arc::new(ScriptedDeliverer::new(vec![DeliveryAttempt::success(500)])),
        RetryPolicy::exponential(1, Duration::from_millis(1)),
    );

    let exec_id = ExecutionId::new();
    insert_terminal_execution(
        &mut conn,
        exec_id,
        "FAILED",
        None,
        Some("boom"),
        Some(r#"[{"url":"https://api.example.com/hook","filter":{"type":"AnyTerminal"}}]"#),
    )
    .await;
    evaluate_triggers_for_execution(&mut conn, exec_id, TerminalState::Failed, None)
        .await
        .expect("evaluate triggers");

    // max_attempts = 1 -> the very first tick exhausts the budget.
    fire_due_completion_deliveries(&mut conn, &None, &[])
        .await
        .expect("scanner tick exhausts on first attempt");
    let rows = list_deliveries_for_execution(&mut conn, exec_id)
        .await
        .expect("list deliveries");
    assert_eq!(rows[0].state, "FAILED");
    let delivery_id = rows[0].id;

    // A delivery_id that is real, but paired with an unrelated execution id,
    // must behave exactly like an unknown delivery_id — never redriven, and
    // never distinguishable from NotFound (no cross-execution existence
    // oracle).
    let unrelated_exec_id = ExecutionId::new();
    let outcome = redrive_delivery(&mut conn, unrelated_exec_id, delivery_id)
        .await
        .expect("redrive with mismatched execution id");
    assert_eq!(outcome, DeliveryRedriveOutcome::NotFound);

    let rows_after = list_deliveries_for_execution(&mut conn, exec_id)
        .await
        .expect("list deliveries after mismatched redrive attempt");
    assert_eq!(
        rows_after[0].state, "FAILED",
        "the delivery must be untouched by a redrive scoped to a different execution"
    );
}

#[tokio::test]
async fn redrive_delivery_rejects_a_non_failed_delivery() {
    let _guard = TEST_SERIAL.lock().await;
    let (url, _container) = setup().await;
    let mut conn = connect(&url).await;
    install_config(
        Arc::new(ScriptedDeliverer::new(vec![DeliveryAttempt::success(200)])),
        RetryPolicy::exponential(3, Duration::from_secs(1)),
    );

    let exec_id = ExecutionId::new();
    insert_terminal_execution(
        &mut conn,
        exec_id,
        "COMPLETED",
        Some("{}"),
        None,
        Some(r#"[{"url":"https://api.example.com/hook","filter":{"type":"AnyTerminal"}}]"#),
    )
    .await;
    evaluate_triggers_for_execution(&mut conn, exec_id, TerminalState::Completed, None)
        .await
        .expect("evaluate triggers");

    let rows = list_deliveries_for_execution(&mut conn, exec_id)
        .await
        .expect("list deliveries");
    let outcome = redrive_delivery(&mut conn, exec_id, rows[0].id)
        .await
        .expect("redrive a still-pending delivery");
    assert_eq!(
        outcome,
        DeliveryRedriveOutcome::NotFailed {
            current_state: "PENDING".to_string()
        }
    );
}
