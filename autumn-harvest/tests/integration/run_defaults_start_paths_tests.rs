//! Run defaults on completion-trigger and typed update-with-start starts
//! (issue #2511).
//!
//! A start takes the execution timeout, the SLA and the chain cap of its type.
//! An operator override wins over the declared value. The fleet ceilings cap
//! the result. These tests drive the three completion-trigger start sites and
//! the typed `update_with_start` stub, then read the stored run.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use autumn_harvest::TypedUpdateWithStartOptions;
use autumn_harvest::completion_trigger::{
    CompletionTrigger, DeferredTriggerStart, GLOBAL_MAX_WORKFLOW_CHAIN_TIMEOUT,
    GLOBAL_MAX_WORKFLOW_EXECUTION_TIMEOUT, GLOBAL_RUN_DEFAULTS, GLOBAL_WORKFLOW_METADATA,
    TerminalState, WorkflowMetadata, enforce_completion_triggers_outbox,
    evaluate_triggers_for_execution, sync_completion_triggers,
};
use autumn_harvest::models::WorkflowExecution;
use autumn_harvest::prelude::*;
use autumn_harvest::run_defaults::{RunDefaultFields, RunDefaultsCache};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::shard::{ShardRouter, ShardedDbPool, install_global_router};
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::types::{ExecutionId, ShardId};
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use uuid::Uuid;

use crate::integration_e2e::{build_test_pool, setup_test_database_url_or_env};

const MINUTE: Duration = Duration::from_mins(1);
const HOUR: Duration = Duration::from_hours(1);

/// The completion-trigger globals are process-wide, so the tests take turns.
static SERIAL: LazyLock<tokio::sync::Mutex<()>> = LazyLock::new(|| tokio::sync::Mutex::new(()));

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect")
}

fn leaked(prefix: &str) -> &'static str {
    Box::leak(format!("{prefix}_{}", Uuid::new_v4().simple()).into_boxed_str())
}

type Metadata = Option<HashMap<String, WorkflowMetadata>>;
type Ceiling = Option<Duration>;

/// Install the globals that the completion-trigger path reads. The drop
/// restores the earlier values, so a test never clears the state of another.
struct Globals {
    previous: (Metadata, Option<Arc<RunDefaultsCache>>, Ceiling, Ceiling),
    _turn: tokio::sync::MutexGuard<'static, ()>,
}

/// Replace the value behind a lock and return the old one. A poisoned lock
/// still works, because the value is plain data.
fn swap<T>(lock: &std::sync::RwLock<T>, value: T) -> T {
    let mut guard = lock
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    std::mem::replace(&mut *guard, value)
}

impl Globals {
    /// `declared` is `(execution_timeout, chain_execution_timeout, sla)`.
    async fn install(
        target: &str,
        declared: (Option<Duration>, Option<Duration>, Option<Duration>),
        overrides: Option<RunDefaultFields>,
        ceilings: (Option<Duration>, Option<Duration>),
    ) -> Self {
        let turn = SERIAL.lock().await;
        let metadata = WorkflowMetadata {
            concurrency: None,
            max_input_bytes: None,
            owner: None,
            runbook_url: None,
            severity: None,
            input_schema: None,
            execution_timeout: declared.0,
            chain_execution_timeout: declared.1,
            sla: declared.2,
            retry_policy: None,
            quota: None,
        };
        let cache = Arc::new(RunDefaultsCache::new());
        if let Some(fields) = overrides {
            cache.write_through(target, fields);
        }
        let previous = (
            swap(
                &GLOBAL_WORKFLOW_METADATA,
                Some(HashMap::from([(target.to_string(), metadata)])),
            ),
            swap(&GLOBAL_RUN_DEFAULTS, Some(cache)),
            swap(&GLOBAL_MAX_WORKFLOW_EXECUTION_TIMEOUT, ceilings.0),
            swap(&GLOBAL_MAX_WORKFLOW_CHAIN_TIMEOUT, ceilings.1),
        );
        Self {
            previous,
            _turn: turn,
        }
    }
}

impl Drop for Globals {
    fn drop(&mut self) {
        let (metadata, cache, execution, chain) = std::mem::take(&mut self.previous);
        swap(&GLOBAL_WORKFLOW_METADATA, metadata);
        swap(&GLOBAL_RUN_DEFAULTS, cache);
        swap(&GLOBAL_MAX_WORKFLOW_EXECUTION_TIMEOUT, execution);
        swap(&GLOBAL_MAX_WORKFLOW_CHAIN_TIMEOUT, chain);
    }
}

fn chrono_of(d: Duration) -> Option<chrono::Duration> {
    chrono::Duration::from_std(d).ok()
}

/// Start a source run that the trigger watches.
async fn start_source(conn: &mut AsyncPgConnection, source: &str) -> ExecutionId {
    let exec_id = ExecutionId::new();
    let workflow_id = format!("src-{}", Uuid::new_v4().simple());
    autumn_harvest::start_or_load_workflow_execution(
        conn,
        autumn_harvest::StartWorkflowParams::new(
            source,
            &workflow_id,
            exec_id,
            serde_json::json!({}),
            "default",
        ),
        None,
    )
    .await
    .expect("start source");
    exec_id
}

async fn target_rows(conn: &mut AsyncPgConnection, target: &str) -> Vec<WorkflowExecution> {
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(target))
        .select(WorkflowExecution::as_select())
        .load(conn)
        .await
        .expect("load target rows")
}

/// Fire the trigger inline, and return the single target run.
async fn inline_target(url: &str, source: &'static str, target: &'static str) -> WorkflowExecution {
    let mut conn = connect(url).await;
    install_global_router(ShardRouter::single());
    let source_exec = start_source(&mut conn, source).await;
    sync_completion_triggers(&mut conn, &[CompletionTrigger::new(source, target)])
        .await
        .expect("register trigger");
    let deferred =
        evaluate_triggers_for_execution(&mut conn, source_exec, TerminalState::Completed, None)
            .await
            .expect("evaluate triggers");
    assert!(deferred.is_empty(), "a same-shard start runs inline");
    let mut rows = target_rows(&mut conn, target).await;
    assert_eq!(rows.len(), 1, "one target run");
    rows.remove(0)
}

/// AC1: the inline start takes the override over the declared value.
#[tokio::test]
async fn an_inline_target_start_takes_the_overrides() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let (source, target) = (leaked("rd_src"), leaked("rd_tgt"));
    let _globals = Globals::install(
        target,
        (Some(HOUR), Some(2 * HOUR), None),
        Some(RunDefaultFields {
            execution_timeout: Some(3 * HOUR),
            sla: Some(30 * MINUTE),
            chain_execution_timeout: Some(4 * HOUR),
            concurrency_limit: None,
            quota: autumn_harvest::quota::QuotaLimits::default(),
            retry: autumn_harvest::workflow_retry::RetryLimits::default(),
        }),
        (None, None),
    )
    .await;
    let row = inline_target(&url, source, target).await;
    assert_eq!(row.execution_timeout, chrono_of(3 * HOUR));
    assert_eq!(row.chain_execution_timeout, chrono_of(4 * HOUR));
    assert!(row.chain_deadline_at.is_some(), "the chain deadline is set");
    assert!(row.sla_deadline_at.is_some(), "the SLA deadline is set");
}

/// AC1: with no override, the inline start takes the declared values.
#[tokio::test]
async fn an_inline_target_start_takes_the_declared_values() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let (source, target) = (leaked("rd_src"), leaked("rd_tgt"));
    let _globals = Globals::install(
        target,
        (Some(HOUR), Some(2 * HOUR), None),
        None,
        (None, None),
    )
    .await;
    let row = inline_target(&url, source, target).await;
    assert_eq!(row.execution_timeout, chrono_of(HOUR));
    assert_eq!(row.chain_execution_timeout, chrono_of(2 * HOUR));
}

/// AC1: the fleet ceilings cap the override.
#[tokio::test]
async fn the_fleet_ceilings_cap_an_inline_target_start() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let (source, target) = (leaked("rd_src"), leaked("rd_tgt"));
    let _globals = Globals::install(
        target,
        (None, None, None),
        Some(RunDefaultFields {
            execution_timeout: Some(10 * HOUR),
            sla: None,
            chain_execution_timeout: Some(20 * HOUR),
            concurrency_limit: None,
            quota: autumn_harvest::quota::QuotaLimits::default(),
            retry: autumn_harvest::workflow_retry::RetryLimits::default(),
        }),
        (Some(HOUR), Some(2 * HOUR)),
    )
    .await;
    let row = inline_target(&url, source, target).await;
    assert_eq!(row.execution_timeout, chrono_of(HOUR));
    assert_eq!(row.chain_execution_timeout, chrono_of(2 * HOUR));
}

/// AC1: with no declared value, the fleet chain ceiling is the default, as on
/// the other start paths (issue #617).
#[tokio::test]
async fn the_fleet_chain_ceiling_is_the_default_of_an_inline_target() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let (source, target) = (leaked("rd_src"), leaked("rd_tgt"));
    let _globals = Globals::install(target, (None, None, None), None, (None, Some(2 * HOUR))).await;
    let row = inline_target(&url, source, target).await;
    assert_eq!(row.chain_execution_timeout, chrono_of(2 * HOUR));
}

/// AC1: the outbox sweep relays a deferred start with the same values.
#[tokio::test]
async fn the_outbox_sweep_start_takes_the_overrides_and_the_ceiling() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let (source, target) = (leaked("rd_src"), leaked("rd_tgt"));
    let _globals = Globals::install(
        target,
        (Some(HOUR), Some(2 * HOUR), None),
        Some(RunDefaultFields {
            execution_timeout: Some(3 * HOUR),
            sla: None,
            chain_execution_timeout: Some(20 * HOUR),
            concurrency_limit: None,
            quota: autumn_harvest::quota::QuotaLimits::default(),
            retry: autumn_harvest::workflow_retry::RetryLimits::default(),
        }),
        (None, Some(5 * HOUR)),
    )
    .await;
    let row = sweep_target(&url, source, target).await;
    assert_eq!(row.execution_timeout, chrono_of(3 * HOUR));
    assert_eq!(
        row.chain_execution_timeout,
        chrono_of(5 * HOUR),
        "the fleet ceiling caps the override"
    );
}

#[derive(diesel::QueryableByName)]
struct TriggerId {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: Uuid,
}

/// One outbox row on shard 0, as a cross-shard evaluation writes it.
struct OutboxRow {
    id: Uuid,
    source_exec: ExecutionId,
    trigger_id: Uuid,
    target_workflow_id: String,
}

/// Register the trigger, start its source, and write one outbox row.
async fn insert_outbox_row(
    conn: &mut AsyncPgConnection,
    source: &'static str,
    target: &'static str,
) -> OutboxRow {
    install_global_router(ShardRouter::single());
    let source_exec = start_source(conn, source).await;
    sync_completion_triggers(conn, &[CompletionTrigger::new(source, target)])
        .await
        .expect("register trigger");
    let trigger_id: Uuid = diesel::sql_query(
        "SELECT id FROM harvest_completion_triggers WHERE target_workflow_name = $1",
    )
    .bind::<diesel::sql_types::Text, _>(target)
    .get_result::<TriggerId>(conn)
    .await
    .expect("trigger id")
    .id;
    let row = OutboxRow {
        id: Uuid::new_v4(),
        source_exec,
        trigger_id,
        target_workflow_id: format!("outbox-{}", Uuid::new_v4().simple()),
    };
    diesel::sql_query(
        "INSERT INTO harvest_completion_trigger_outbox \
         (id, source_exec_id, trigger_id, target_shard, target_workflow_name, \
          target_workflow_id, target_input, queue_name, priority, max_workflow_input_bytes) \
         VALUES ($1, $2, $3, 0, $4, $5, '{}'::jsonb, 'default', '\"normal\"'::jsonb, 0)",
    )
    .bind::<diesel::sql_types::Uuid, _>(row.id)
    .bind::<diesel::sql_types::Uuid, _>(row.source_exec.as_uuid())
    .bind::<diesel::sql_types::Uuid, _>(row.trigger_id)
    .bind::<diesel::sql_types::Text, _>(target)
    .bind::<diesel::sql_types::Text, _>(&row.target_workflow_id)
    .execute(conn)
    .await
    .expect("insert outbox row");
    row
}

/// Relay one outbox row through the scanner, and return the target run.
async fn sweep_target(url: &str, source: &'static str, target: &'static str) -> WorkflowExecution {
    let mut conn = connect(url).await;
    insert_outbox_row(&mut conn, source, target).await;
    let pool = Some(ShardedDbPool::single(build_test_pool(url)));
    enforce_completion_triggers_outbox(&mut conn, &NoOpMetrics, &pool, &[ShardId::new(0)])
        .await
        .expect("sweep");
    let mut rows = target_rows(&mut conn, target).await;
    assert_eq!(rows.len(), 1, "the sweep starts the target");
    rows.remove(0)
}

/// Relay one outbox row through `DeferredTriggerStart::spawn`, the immediate
/// relay of a cross-shard evaluation. `sla` is the value from evaluation
/// time, which the struct carries.
async fn relay_target(
    url: &str,
    source: &'static str,
    target: &'static str,
    sla: Option<Duration>,
) -> WorkflowExecution {
    let mut conn = connect(url).await;
    let row = insert_outbox_row(&mut conn, source, target).await;
    // The constructor installs the global pool that `spawn` reads.
    let _pool = ShardedDbPool::single(build_test_pool(url));
    DeferredTriggerStart {
        outbox_id: row.id,
        source_exec_id: row.source_exec.as_uuid(),
        trigger_id: row.trigger_id,
        source_shard: ShardId::new(0),
        target_shard: ShardId::new(0),
        target_workflow_name: target.to_string(),
        target_workflow_id: row.target_workflow_id,
        target_input: serde_json::json!({}),
        queue_name: Some("default".to_string()),
        concurrency_key: None,
        concurrency_limit: None,
        concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
        priority: autumn_harvest::types::Priority::default(),
        max_workflow_input_bytes: 0,
        trigger_name: "rd_relay".to_string(),
        owner: None,
        runbook_url: None,
        severity: None,
        sla,
        retry_policy: None,
        max_workflow_attempts_ceiling: None,
        codecs: autumn_harvest::payload_codec::PayloadCodecs::default(),
        tenant: None,
    }
    .spawn();
    // The relay runs on a detached task. Wait for its start.
    for _ in 0..200 {
        let mut rows = target_rows(&mut conn, target).await;
        if !rows.is_empty() {
            assert_eq!(rows.len(), 1, "the relay starts one target");
            return rows.remove(0);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the relay did not start the target within 10 seconds");
}

// ── Deadlines of a target run ───────────────────────────────────────────────

/// The three sites that start a completion-trigger target.
#[derive(Debug, Clone, Copy)]
enum Site {
    Inline,
    Relay,
    Sweep,
}

impl Site {
    const ALL: [Self; 3] = [Self::Inline, Self::Relay, Self::Sweep];

    /// Start a target through this site. The relay carries `sla`, as an
    /// evaluation resolves it. The other two sites resolve the SLA again.
    async fn target(
        self,
        url: &str,
        source: &'static str,
        target: &'static str,
        sla: Option<Duration>,
    ) -> WorkflowExecution {
        match self {
            Self::Inline => inline_target(url, source, target).await,
            Self::Relay => relay_target(url, source, target, sla).await,
            Self::Sweep => sweep_target(url, source, target).await,
        }
    }
}

/// The span from `started_at` to `at`, to the nearest second.
///
/// The start reads its clock a few milliseconds before the row default
/// `started_at`, so an exact comparison is off by that gap.
fn whole_secs(started_at: chrono::DateTime<chrono::Utc>, at: chrono::DateTime<chrono::Utc>) -> u64 {
    let millis = (at - started_at).num_milliseconds() + 500;
    u64::try_from(millis.div_euclid(1000)).expect("a deadline after the start")
}

/// The hard and the soft budget of a run, in whole seconds.
fn budgets(row: &WorkflowExecution) -> (Option<u64>, Option<u64>) {
    (
        row.deadline_at.map(|at| whole_secs(row.started_at, at)),
        row.sla_deadline_at.map(|at| whole_secs(row.started_at, at)),
    )
}

fn timeout_override(execution_timeout: Duration) -> RunDefaultFields {
    RunDefaultFields {
        execution_timeout: Some(execution_timeout),
        sla: None,
        chain_execution_timeout: None,
        concurrency_limit: None,
        quota: autumn_harvest::quota::QuotaLimits::default(),
        retry: autumn_harvest::workflow_retry::RetryLimits::default(),
    }
}

/// AC2 and AC3: each site gives `deadline_at = started_at + override`, and
/// clamps the declared SLA to that timeout.
#[tokio::test]
async fn each_target_site_sets_the_override_deadline_and_clamps_the_sla() {
    let (url, _container) = setup_test_database_url_or_env().await;
    for site in Site::ALL {
        let (source, target) = (leaked("rd_src"), leaked("rd_tgt"));
        let _globals = Globals::install(
            target,
            (Some(HOUR), None, Some(3 * HOUR)),
            Some(timeout_override(10 * MINUTE)),
            (None, None),
        )
        .await;
        let row = site.target(&url, source, target, Some(3 * HOUR)).await;
        assert_eq!(
            budgets(&row),
            (Some(600), Some(600)),
            "{site:?}: the 10m override is the deadline, and the 3h SLA clamps to it"
        );
    }
}

/// AC3: each site caps the override at the fleet ceiling, and clamps the
/// SLA to the capped timeout.
#[tokio::test]
async fn each_target_site_caps_the_override_deadline_at_the_fleet_ceiling() {
    let (url, _container) = setup_test_database_url_or_env().await;
    for site in Site::ALL {
        let (source, target) = (leaked("rd_src"), leaked("rd_tgt"));
        let _globals = Globals::install(
            target,
            (None, None, Some(3 * HOUR)),
            Some(timeout_override(10 * HOUR)),
            (Some(HOUR), None),
        )
        .await;
        let row = site.target(&url, source, target, Some(3 * HOUR)).await;
        assert_eq!(
            budgets(&row),
            (Some(3600), Some(3600)),
            "{site:?}: the 1h ceiling caps the 10h override and the 3h SLA"
        );
    }
}

// ── Typed `update_with_start` stub ──────────────────────────────────────────

#[workflow(execution_timeout = "1h", chain_execution_timeout = "2h", sla = "30m")]
#[allow(clippy::unused_async)]
async fn rd_uws_flow(_ctx: &WorkflowContext, input: i64) -> Result<String, String> {
    Ok(format!("done {input}"))
}

#[update(workflow = "rd_uws_flow")]
#[allow(clippy::unused_async)]
async fn rd_uws_poke(_ctx: &WorkflowContext, step: i64) -> Result<String, String> {
    Ok(format!("poked {step}"))
}

fn client_with(
    url: &str,
    cache: Arc<RunDefaultsCache>,
    chain_ceiling: Option<Duration>,
) -> WorkflowHandleClient {
    WorkflowHandleClient::single(build_test_pool(url), url.to_string())
        .with_run_defaults(cache)
        .with_max_workflow_chain_timeout(chain_ceiling)
}

async fn run_uws(url: &str, client: &WorkflowHandleClient) -> WorkflowExecution {
    let mut conn = connect(url).await;
    let outcome = RdUwsFlowStub::update_with_start_rd_uws_poke(
        &mut conn,
        client,
        format!("uws-{}", Uuid::new_v4().simple()),
        serde_json::json!(1),
        1,
        TypedUpdateWithStartOptions::default(),
    )
    .await
    .expect("update with start");
    harvest_workflow_executions::table
        .find(outcome.exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("load run")
}

/// AC2: with no override, the stub takes the declared values.
#[tokio::test]
async fn update_with_start_takes_the_declared_values() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let client = client_with(&url, Arc::new(RunDefaultsCache::new()), None);
    let row = run_uws(&url, &client).await;
    assert_eq!(row.execution_timeout, chrono_of(HOUR));
    assert_eq!(row.chain_execution_timeout, chrono_of(2 * HOUR));
    assert!(row.chain_deadline_at.is_some(), "the chain deadline is set");
}

/// AC2: an override wins over the declared values.
#[tokio::test]
async fn update_with_start_takes_the_overrides() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let cache = Arc::new(RunDefaultsCache::new());
    cache.write_through(
        "rd_uws_flow",
        RunDefaultFields {
            execution_timeout: Some(3 * HOUR),
            sla: Some(10 * MINUTE),
            chain_execution_timeout: Some(4 * HOUR),
            concurrency_limit: None,
            quota: autumn_harvest::quota::QuotaLimits::default(),
            retry: autumn_harvest::workflow_retry::RetryLimits::default(),
        },
    );
    let row = run_uws(&url, &client_with(&url, cache, None)).await;
    assert_eq!(row.execution_timeout, chrono_of(3 * HOUR));
    assert_eq!(row.chain_execution_timeout, chrono_of(4 * HOUR));
    let sla = row.sla_deadline_at.expect("SLA deadline") - row.started_at;
    assert!(
        sla <= chrono::Duration::minutes(10) + chrono::Duration::seconds(5),
        "the SLA override applies: {sla}"
    );
}

/// AC2: the fleet chain ceiling caps the override.
#[tokio::test]
async fn the_fleet_chain_ceiling_caps_an_update_with_start_run() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let cache = Arc::new(RunDefaultsCache::new());
    cache.write_through(
        "rd_uws_flow",
        RunDefaultFields {
            chain_execution_timeout: Some(20 * HOUR),
            ..RunDefaultFields::default()
        },
    );
    let row = run_uws(&url, &client_with(&url, cache, Some(5 * HOUR))).await;
    assert_eq!(row.chain_execution_timeout, chrono_of(5 * HOUR));
}
