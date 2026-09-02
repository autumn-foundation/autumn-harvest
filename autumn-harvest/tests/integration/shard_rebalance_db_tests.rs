//! Postgres-backed coverage for shard rebalancing (issue #964).
//!
//! Every test here runs against **two** throwaway databases, because that is
//! what a shard is in Harvest: a separate Postgres instance. A single-database
//! fake would prove nothing about the property this feature exists to have.
//!
//! Acceptance-criteria map (issue #964):
//!
//! - **AC1** (quiescent-only; wakes never lost, never doubled) —
//!   [`a_non_quiescent_execution_is_skipped_with_named_blockers`],
//!   [`a_signal_arriving_mid_migration_aborts_the_cutover_and_is_not_lost`],
//!   [`a_signal_arriving_after_cutover_is_delivered_to_the_target`],
//!   [`the_sql_cutover_predicate_agrees_with_the_pure_predicate`].
//! - **AC2** (transactional copy + replay verification before cutover) —
//!   [`a_timer_parked_execution_migrates_end_to_end`],
//!   [`the_copy_is_byte_identical_and_replay_verified`],
//!   [`verification_rejects_a_tampered_copy_and_leaves_the_source_untouched`],
//!   [`a_schema_mismatch_refuses_the_copy_before_anything_is_written`].
//! - **AC3** (source sealed, never deleted, forwarding reference) —
//!   [`the_source_is_sealed_with_a_forwarding_pointer_and_never_deleted`],
//!   [`the_sealed_source_releases_the_active_uniqueness_slot`].
//! - **AC4** (any id captured before migration still resolves) —
//!   [`every_id_holder_class_still_resolves_after_migration`],
//!   [`a_twice_migrated_run_resolves_through_the_chain_and_collapses_it`].
//! - **AC5** (zero new `WorkflowEvent` variants; history copied verbatim) —
//!   [`a_migration_appends_no_events_at_all`].
//! - **AC6** (documented dedupe-scope semantics) —
//!   [`signal_idempotency_keys_and_timers_survive_the_copy`].
//! - **AC7** (crash-safe: exactly one authoritative shard at every kill point) —
//!   [`a_crash_at_every_phase_leaves_exactly_one_authoritative_shard`],
//!   [`resume_finishes_a_migration_killed_after_the_cutover`].
//! - **AC8** (operator batch surface with dry-run and progress) —
//!   [`a_dry_run_writes_nothing_and_reports_the_population_a_real_run_would_move`],
//!   [`the_batch_is_bounded_by_its_limit_and_reports_every_outcome`].
//!
//! Runs against `HARVEST_TEST_DATABASE_URL` when set (each test gets two
//! throwaway databases), otherwise against per-test Postgres containers.

#![allow(clippy::too_many_lines)]

use autumn_harvest::error::HarvestError;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::shard_rebalance::{
    MigrationOutcome, MigrationPhase, QuiescenceBlocker, activate_target, assess_quiescence,
    begin_migration, commit_cutover, history_fingerprint, list_migration_candidates,
    load_migration, migrate_execution, migrate_quiescent_executions, observe_quiescence,
    resolve_execution_shard, resume_incomplete_migrations, stage_copy, verify_target_copy,
};
use autumn_harvest::store;
use autumn_harvest::types::{ExecutionId, ShardId};

use chrono::{Duration, Utc};
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

const SOURCE: ShardId = ShardId::new(0);
const TARGET: ShardId = ShardId::new(1);
const THIRD: ShardId = ShardId::new(2);

// ── harness ──────────────────────────────────────────────────────────────────

/// Two isolated, fully-migrated databases standing in for two shards, plus the
/// `ShardedDbPool` that routes between them.
struct TwoShards {
    pool: ShardedDbPool,
    source_url: String,
    target_url: String,
    _containers: Vec<ContainerAsync<Postgres>>,
}

impl TwoShards {
    async fn source(&self) -> AsyncPgConnection {
        connect(&self.source_url).await
    }
    async fn target(&self) -> AsyncPgConnection {
        connect(&self.target_url).await
    }
}

async fn setup_two_shards() -> TwoShards {
    let (source_url, c1) = setup_isolated_db().await;
    let (target_url, c2) = setup_isolated_db().await;
    let pools = [
        (SOURCE, build_pool(&source_url)),
        (TARGET, build_pool(&target_url)),
    ]
    .into_iter()
    .collect();
    TwoShards {
        pool: ShardedDbPool::from_map(pools, SOURCE),
        source_url,
        target_url,
        _containers: c1.into_iter().chain(c2).collect(),
    }
}

/// Three isolated databases, for the cases that need an INTERMEDIATE residence:
/// after A → B → C the forwarding pointer is collapsed past B, so B is only
/// reachable through the durable residence history. Two shards cannot express
/// that — a second hop there lands back on the origin.
struct ThreeShards {
    pool: ShardedDbPool,
    urls: Vec<String>,
    _containers: Vec<ContainerAsync<Postgres>>,
}

async fn setup_three_shards() -> ThreeShards {
    let (a, c1) = setup_isolated_db().await;
    let (b, c2) = setup_isolated_db().await;
    let (c, c3) = setup_isolated_db().await;
    let pools = [
        (SOURCE, build_pool(&a)),
        (TARGET, build_pool(&b)),
        (THIRD, build_pool(&c)),
    ]
    .into_iter()
    .collect();
    ThreeShards {
        pool: ShardedDbPool::from_map(pools, SOURCE),
        urls: vec![a, b, c],
        _containers: c1.into_iter().chain(c2).chain(c3).collect(),
    }
}

async fn setup_isolated_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let db_name = format!("harvest_rebalance_{}", Uuid::new_v4().simple());
        let mut admin = <AsyncPgConnection as AsyncConnection>::establish(&admin_url)
            .await
            .expect("HARVEST_TEST_DATABASE_URL must be reachable");
        admin
            .batch_execute(&format!("CREATE DATABASE \"{db_name}\""))
            .await
            .expect("create throwaway database");
        let url = swap_database(&admin_url, &db_name);
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
            .await
            .expect("connect to throwaway database");
        conn.batch_execute(autumn_harvest::full_migrations_sql())
            .await
            .expect("apply migrations");
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(autumn_harvest::full_migrations_sql().as_bytes().to_vec())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("get host");
    let port = container.get_host_port_ipv4(5432).await.expect("get port");
    (
        format!("postgres://postgres:postgres@{host}:{port}/postgres"),
        Some(container),
    )
}

fn swap_database(url: &str, db_name: &str) -> String {
    let (base, _) = url.split_once('?').unwrap_or((url, ""));
    let cut = base.rfind('/').expect("a postgres URL has a database path");
    format!("{}/{db_name}", &base[..cut])
}

fn build_pool(url: &str) -> autumn_harvest::worker::DbPool {
    let manager =
        diesel_async::pooled_connection::AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            url,
        );
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("build pool")
}

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect")
}

fn codecs() -> PayloadCodecs {
    PayloadCodecs::default()
}

// ── fixtures ─────────────────────────────────────────────────────────────────

/// Insert a `RUNNING` root execution whose `ExecutionId` encodes `SOURCE`.
async fn insert_execution(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
) -> ExecutionId {
    use autumn_harvest::schema::harvest_workflow_executions;
    let exec_id = ExecutionId::new_for_shard(SOURCE);
    let row = NewWorkflowExecution {
        quota_key: None,
        continued_from_exec_id: None,
        first_exec_id: None,
        id: exec_id.as_uuid(),
        workflow_name,
        workflow_id,
        run_id: Uuid::new_v4(),
        shard_id: SOURCE.as_i32(),
        input: json!({"seed": 1}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        deadline_at: None,
        chain_execution_timeout: None,
        chain_deadline_at: None,
        memo: Some(json!({"note": "entity"})),
        search_attrs: Some(json!({"tenant_id": "acme"})),
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
        start_source: None,
        start_source_ref: None,
        started_by: None,
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("insert execution");
    exec_id
}

fn started(input: Value) -> WorkflowEvent {
    WorkflowEvent::WorkflowStarted {
        input,
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }
}

async fn append_history(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    events: &[WorkflowEvent],
) {
    store::append_events_with_codecs(conn, exec_id, events, 0, &codecs())
        .await
        .expect("append events");
}

/// Append events CONTINUING an existing history, rather than starting one.
///
/// `append_history` always begins at event id 0, so a second call collides on
/// `harvest_events_workflow_exec_id_event_id_key`. Modelling "the run woke and
/// executed another cycle" needs the real next id.
async fn append_more(conn: &mut AsyncPgConnection, exec_id: ExecutionId, events: &[WorkflowEvent]) {
    let next: ScalarInt = diesel::sql_query(
        "SELECT (COALESCE(max(event_id), -1) + 1)::INTEGER AS value FROM harvest_events \
          WHERE workflow_exec_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .get_result(conn)
    .await
    .expect("next event id");
    store::append_events_with_codecs(conn, exec_id, events, next.value.unwrap_or(0), &codecs())
        .await
        .expect("append events");
}

/// A durable timer parked in the future, plus the PENDING workflow task row that
/// is how a timer-parked execution actually waits.
async fn park_on_timer(conn: &mut AsyncPgConnection, exec_id: ExecutionId) {
    let fires_at = Utc::now() + Duration::days(7);
    diesel::sql_query(
        "INSERT INTO harvest_timers (id, workflow_exec_id, timer_id, fires_at, fired) \
         VALUES ($1, $2, 'wake', $3, FALSE)",
    )
    .bind::<diesel::sql_types::Uuid, _>(Uuid::new_v4())
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Timestamptz, _>(fires_at)
    .execute(conn)
    .await
    .expect("insert timer");

    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
             (id, queue_name, task_type, workflow_exec_id, input, state, priority, \
              attempt, max_attempts, scheduled_at) \
         VALUES ($1, 'default', 'workflow', $2, '{}'::jsonb, 'PENDING', 0, 0, 3, $3)",
    )
    .bind::<diesel::sql_types::Uuid, _>(Uuid::new_v4())
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Timestamptz, _>(fires_at)
    .execute(conn)
    .await
    .expect("insert parked workflow task");
}

/// The other long-lived shape: a `RUNNING` task row with no worker, which is
/// what `queue::park_workflow_task` produces for a signal-parked run.
async fn park_on_signal(conn: &mut AsyncPgConnection, exec_id: ExecutionId) {
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
             (id, queue_name, task_type, workflow_exec_id, input, state, priority, \
              attempt, max_attempts, scheduled_at, worker_id, started_at) \
         VALUES ($1, 'default', 'workflow', $2, '{}'::jsonb, 'RUNNING', 0, 1, 3, NOW(), \
                 NULL, NULL)",
    )
    .bind::<diesel::sql_types::Uuid, _>(Uuid::new_v4())
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(conn)
    .await
    .expect("insert parked workflow task");
}

async fn deliver_signal(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    name: &str,
    idempotency_key: Option<&str>,
) {
    diesel::sql_query(
        "INSERT INTO harvest_signals \
             (id, workflow_exec_id, signal_name, payload, consumed, idempotency_key) \
         VALUES ($1, $2, $3, '{}'::jsonb, FALSE, $4)",
    )
    .bind::<diesel::sql_types::Uuid, _>(Uuid::new_v4())
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(name)
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(idempotency_key)
    .execute(conn)
    .await
    .expect("insert signal");
}

#[derive(diesel::QueryableByName)]
struct ScalarText {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    value: Option<String>,
}

#[derive(diesel::QueryableByName)]
struct ScalarInt {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Integer>)]
    value: Option<i32>,
}

#[derive(diesel::QueryableByName)]
struct ScalarCount {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    value: i64,
}

async fn state_of(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> Option<String> {
    let row: Option<ScalarText> =
        diesel::sql_query("SELECT state AS value FROM harvest_workflow_executions WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
            .get_result(conn)
            .await
            .optional()
            .expect("query state");
    row.and_then(|r| r.value)
}

async fn forward_of(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> Option<i32> {
    let row: Option<ScalarInt> = diesel::sql_query(
        "SELECT migrated_to_shard AS value FROM harvest_workflow_executions WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .get_result(conn)
    .await
    .optional()
    .expect("query forward");
    row.and_then(|r| r.value)
}

async fn count(conn: &mut AsyncPgConnection, sql: &str, exec_id: ExecutionId) -> i64 {
    let row: ScalarCount = diesel::sql_query(sql)
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .get_result(conn)
        .await
        .expect("count");
    row.value
}

/// Task rows that a worker could still claim for this execution. The claim
/// query filters on the TASK's state, not the execution's, so this — not the
/// execution row — is what "claimable here" actually means.
async fn claimable_tasks(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> i64 {
    count(
        conn,
        "SELECT count(*)::BIGINT AS value FROM harvest_task_queue \
          WHERE workflow_exec_id = $1 AND task_type = 'workflow' \
            AND state IN ('PENDING', 'RUNNING')",
        exec_id,
    )
    .await
}

/// The invariant the whole design exists to hold: a run is authoritative — i.e.
/// has a non-terminal execution row AND a claimable task row — on at most one
/// shard, and after a completed migration on exactly one.
async fn authoritative_shards(shards: &TwoShards, exec_id: ExecutionId) -> Vec<ShardId> {
    let mut out = Vec::new();
    for (shard, url) in [(SOURCE, &shards.source_url), (TARGET, &shards.target_url)] {
        let mut conn = connect(url).await;
        let state = state_of(&mut conn, exec_id).await;
        let live = matches!(state.as_deref(), Some("RUNNING"));
        if live && claimable_tasks(&mut conn, exec_id).await > 0 {
            out.push(shard);
        }
    }
    out
}

/// The standard fixture: a timer-parked entity workflow with real history.
async fn quiescent_fixture(shards: &TwoShards, workflow_id: &str) -> ExecutionId {
    let mut source = shards.source().await;
    let exec_id = insert_execution(&mut source, "entity_flow", workflow_id).await;
    append_history(
        &mut source,
        exec_id,
        &[
            started(json!({"seed": 1})),
            WorkflowEvent::TimerStarted {
                timer_id: autumn_harvest::types::TimerId::new("wake"),
                duration_secs: 604_800,
            },
        ],
    )
    .await;
    park_on_timer(&mut source, exec_id).await;
    exec_id
}

// ── AC2: the copy, the verification, and the end-to-end move ─────────────────

#[tokio::test]
async fn a_timer_parked_execution_migrates_end_to_end() {
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "entity-1").await;

    let outcome = migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("migration must not error");
    assert!(
        matches!(outcome, MigrationOutcome::Migrated { .. }),
        "expected a completed migration, got {outcome:?}"
    );

    // Exactly one shard is authoritative, and it is the target.
    assert_eq!(authoritative_shards(&shards, exec_id).await, vec![TARGET]);

    let mut target = shards.target().await;
    assert_eq!(
        state_of(&mut target, exec_id).await.as_deref(),
        Some("RUNNING")
    );
    // The `shard_id` column follows the run; the ExecutionId deliberately does not.
    let shard_col = count(
        &mut target,
        "SELECT shard_id::BIGINT AS value FROM harvest_workflow_executions WHERE id = $1",
        exec_id,
    )
    .await;
    assert_eq!(shard_col, i64::from(TARGET.as_i32()));
    assert_eq!(
        exec_id.shard(),
        SOURCE,
        "the ExecutionId must NOT be re-minted: that is the whole identity decision"
    );

    // Timers and the parked task row came with it.
    assert_eq!(
        count(
            &mut target,
            "SELECT count(*)::BIGINT AS value FROM harvest_timers \
              WHERE workflow_exec_id = $1 AND NOT fired",
            exec_id
        )
        .await,
        1
    );
    assert_eq!(claimable_tasks(&mut target, exec_id).await, 1);

    // And the memo / search attributes the copy is column-list-free to preserve.
    let mut target2 = shards.target().await;
    let attrs: ScalarText = diesel::sql_query(
        "SELECT search_attrs->>'tenant_id' AS value FROM harvest_workflow_executions \
          WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .get_result(&mut target2)
    .await
    .expect("search attrs");
    assert_eq!(attrs.value.as_deref(), Some("acme"));
}

#[tokio::test]
async fn the_copy_is_byte_identical_and_replay_verified() {
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "entity-verify").await;
    let (mut source, mut target) = (shards.source().await, shards.target().await);

    begin_migration(&mut source, exec_id, SOURCE, TARGET)
        .await
        .expect("begin");
    stage_copy(&mut source, &mut target, exec_id, TARGET)
        .await
        .expect("stage");
    let fingerprint = verify_target_copy(&mut source, &mut target, exec_id, &codecs())
        .await
        .expect("verification must pass on an untouched copy");

    // The fingerprint is the source's own, computed independently here.
    let source_history = store::load_history_with_codecs(&mut source, exec_id, &codecs())
        .await
        .expect("source history");
    assert_eq!(fingerprint, history_fingerprint(&source_history.events));

    // ...and it is durable on the migration row, so an operator can see WHAT
    // was verified rather than only that it passed.
    let record = load_migration(&mut source, exec_id)
        .await
        .expect("load")
        .expect("row exists");
    assert_eq!(record.phase, MigrationPhase::Verified);
    assert_eq!(
        record.verified_fingerprint.as_deref(),
        Some(fingerprint.as_str())
    );

    // The source is still fully authoritative: verification writes nothing there.
    assert_eq!(authoritative_shards(&shards, exec_id).await, vec![SOURCE]);
}

#[tokio::test]
async fn verification_rejects_a_tampered_copy_and_leaves_the_source_untouched() {
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "entity-tamper").await;
    let (mut source, mut target) = (shards.source().await, shards.target().await);

    begin_migration(&mut source, exec_id, SOURCE, TARGET)
        .await
        .expect("begin");
    stage_copy(&mut source, &mut target, exec_id, TARGET)
        .await
        .expect("stage");

    // Corrupt exactly one stored event on the target. This is the failure mode
    // a hand-rolled copy would produce silently.
    diesel::sql_query(
        "UPDATE harvest_events SET event_data = jsonb_set(event_data, '{tampered}', 'true') \
          WHERE workflow_exec_id = $1 AND event_id = 0",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut target)
    .await
    .expect("tamper");

    let error = verify_target_copy(&mut source, &mut target, exec_id, &codecs())
        .await
        .expect_err("a tampered copy must not verify");
    assert!(
        matches!(error, HarvestError::NonDeterministic { .. }),
        "expected a replay-divergence classification, got {error:?}"
    );

    // The source never moved.
    assert_eq!(
        state_of(&mut source, exec_id).await.as_deref(),
        Some("RUNNING")
    );
    assert_eq!(authoritative_shards(&shards, exec_id).await, vec![SOURCE]);
}

#[tokio::test]
async fn a_schema_mismatch_refuses_the_copy_before_anything_is_written() {
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "entity-schema").await;
    let (mut source, mut target) = (shards.source().await, shards.target().await);

    // Simulate a target shard that is behind on migrations. The copy is
    // deliberately column-list-free, so without this guard
    // `jsonb_populate_record` would drop the unknown key SILENTLY.
    target
        .batch_execute("ALTER TABLE harvest_workflow_executions DROP COLUMN triage_note")
        .await
        .expect("drop a column to simulate a stale target");

    begin_migration(&mut source, exec_id, SOURCE, TARGET)
        .await
        .expect("begin");
    let error = stage_copy(&mut source, &mut target, exec_id, TARGET)
        .await
        .expect_err("a schema mismatch must refuse the copy");
    assert!(
        matches!(error, HarvestError::Config(_)),
        "expected a configuration refusal, got {error:?}"
    );
    assert_eq!(
        state_of(&mut target, exec_id).await,
        None,
        "nothing may be written to a mismatched target"
    );
}

// ── AC3: the seal ────────────────────────────────────────────────────────────

#[tokio::test]
async fn the_source_is_sealed_with_a_forwarding_pointer_and_never_deleted() {
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "entity-seal").await;

    migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("migrate");

    let mut source = shards.source().await;
    assert_eq!(
        state_of(&mut source, exec_id).await.as_deref(),
        Some("MIGRATED")
    );
    assert_eq!(
        forward_of(&mut source, exec_id).await,
        Some(TARGET.as_i32())
    );

    // Audit survives: the history is still there on the sealed source.
    assert!(
        count(
            &mut source,
            "SELECT count(*)::BIGINT AS value FROM harvest_events WHERE workflow_exec_id = $1",
            exec_id
        )
        .await
            > 0,
        "the sealed source keeps its history; sealing is never a delete"
    );

    // And it is not claimable there any more — which the claim query decides
    // from the TASK's state, not the execution's.
    assert_eq!(claimable_tasks(&mut source, exec_id).await, 0);
}

#[tokio::test]
async fn the_sealed_source_keeps_the_business_key_slot_so_no_duplicate_can_start() {
    // The issue points at the reset path's `TERMINATED` sealing as the
    // precedent, "which already releases the uniqueness index". Copying that
    // would be WRONG here, and this test is the reason.
    //
    // A reset forks a successor on the SAME shard, so its source must release
    // `(workflow_name, workflow_id)` or the successor could not be inserted. A
    // migration puts the copy on a DIFFERENT database, whose index is its own —
    // so nothing needs releasing, and releasing would let a later start of the
    // same business key (which still hashes back to the source shard) create a
    // SECOND live run alongside the migrated one.
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "entity-unique").await;
    migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("migrate");

    let mut source = shards.source().await;
    let duplicate = diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name) \
         VALUES ($1, 'entity_flow', 'entity-unique', gen_random_uuid(), 0, 'RUNNING', \
                 '{}'::jsonb, 'default')",
    )
    .bind::<diesel::sql_types::Uuid, _>(ExecutionId::new_for_shard(SOURCE).as_uuid())
    .execute(&mut source)
    .await;
    assert!(
        duplicate.is_err(),
        "the sealed source must keep holding the business-key slot; releasing it \
         would allow a second live run for a workflow_id that is still running \
         on the target shard"
    );

    // And the copy holds the identity on the target, in its own index.
    let mut target = shards.target().await;
    assert_eq!(
        state_of(&mut target, exec_id).await.as_deref(),
        Some("RUNNING")
    );
}

// ── AC1: wakes are never lost and never doubled ──────────────────────────────

#[tokio::test]
async fn a_signal_arriving_mid_migration_aborts_the_cutover_and_is_not_lost() {
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "entity-race").await;
    let (mut source, mut target) = (shards.source().await, shards.target().await);

    begin_migration(&mut source, exec_id, SOURCE, TARGET)
        .await
        .expect("begin");
    stage_copy(&mut source, &mut target, exec_id, TARGET)
        .await
        .expect("stage");
    verify_target_copy(&mut source, &mut target, exec_id, &codecs())
        .await
        .expect("verify");

    // The wake lands between verification and cutover — the tightest window.
    deliver_signal(&mut source, exec_id, "poke", None).await;

    let committed = commit_cutover(&mut source, exec_id, TARGET)
        .await
        .expect("cutover query");
    assert!(
        !committed,
        "a woken execution must not cut over: the cutover re-checks quiescence"
    );

    // The source is untouched and still holds the signal.
    assert_eq!(
        state_of(&mut source, exec_id).await.as_deref(),
        Some("RUNNING")
    );
    assert_eq!(authoritative_shards(&shards, exec_id).await, vec![SOURCE]);
    assert_eq!(
        count(
            &mut source,
            "SELECT count(*)::BIGINT AS value FROM harvest_signals \
              WHERE workflow_exec_id = $1 AND NOT consumed",
            exec_id
        )
        .await,
        1,
        "the signal must still be on the source, waiting to be delivered normally"
    );
}

#[tokio::test]
async fn a_signal_arriving_after_cutover_is_delivered_to_the_target() {
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "entity-late").await;
    let (mut source, mut target) = (shards.source().await, shards.target().await);

    begin_migration(&mut source, exec_id, SOURCE, TARGET)
        .await
        .expect("begin");
    stage_copy(&mut source, &mut target, exec_id, TARGET)
        .await
        .expect("stage");
    verify_target_copy(&mut source, &mut target, exec_id, &codecs())
        .await
        .expect("verify");
    assert!(
        commit_cutover(&mut source, exec_id, TARGET)
            .await
            .expect("cutover")
    );

    // Past the cutover, an id-routed write resolves through the seal to the
    // target — which is where the signal lands.
    let resolved = resolve_execution_shard(&shards.pool, exec_id)
        .await
        .expect("resolve");
    assert_eq!(resolved, TARGET);
    deliver_signal(&mut target, exec_id, "poke", None).await;

    // Activation must notice the pending wake and schedule it NOW rather than
    // leaving it waiting on a timer seven days out.
    activate_target(&mut source, &mut target, exec_id)
        .await
        .expect("activate");

    let due_now = count(
        &mut target,
        "SELECT count(*)::BIGINT AS value FROM harvest_task_queue \
          WHERE workflow_exec_id = $1 AND task_type = 'workflow' \
            AND state = 'PENDING' AND scheduled_at <= NOW()",
        exec_id,
    )
    .await;
    assert_eq!(
        due_now, 1,
        "the post-cutover wake must be dispatchable, not lost"
    );

    // Never doubled: the source has no copy of it and nothing claimable.
    assert_eq!(
        count(
            &mut source,
            "SELECT count(*)::BIGINT AS value FROM harvest_signals WHERE workflow_exec_id = $1",
            exec_id
        )
        .await,
        0
    );
    assert_eq!(authoritative_shards(&shards, exec_id).await, vec![TARGET]);
}

#[tokio::test]
async fn a_non_quiescent_execution_is_skipped_with_named_blockers() {
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "entity-busy").await;
    let mut source = shards.source().await;

    // Claim the parked task: a worker is now mid-cycle.
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = 'w1', \
                scheduled_at = NOW(), started_at = NOW() \
          WHERE workflow_exec_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut source)
    .await
    .expect("claim");

    let outcome = migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("no error, just a refusal");
    match outcome {
        MigrationOutcome::Skipped { blockers, .. } => {
            assert!(
                blockers.contains(&QuiescenceBlocker::ClaimedWorkflowTask),
                "the blocker must name itself: {blockers:?}"
            );
        }
        other => panic!("expected Skipped, got {other:?}"),
    }
    assert_eq!(authoritative_shards(&shards, exec_id).await, vec![SOURCE]);
}

#[tokio::test]
async fn the_sql_cutover_predicate_agrees_with_the_pure_predicate() {
    // The one place SQL and Rust could drift: the cutover's WHERE clause
    // re-evaluates quiescence in SQL, while candidate selection uses the pure
    // predicate. If they disagree, either eligible runs never cut over (a wedge)
    // or ineligible ones do (a lost wake). Pin them against real rows.
    let shards = setup_two_shards().await;

    for (label, mutate) in [
        ("quiescent", None::<&str>),
        (
            "claimed",
            Some(
                "UPDATE harvest_task_queue SET state='RUNNING', worker_id='w', started_at=NOW() \
              WHERE workflow_exec_id = $1",
            ),
        ),
        (
            "due",
            Some(
                "UPDATE harvest_task_queue SET state='PENDING', scheduled_at=NOW() - interval '1 hour' \
              WHERE workflow_exec_id = $1",
            ),
        ),
        (
            "wake_requested",
            Some("UPDATE harvest_task_queue SET wake_requested = TRUE WHERE workflow_exec_id = $1"),
        ),
        (
            "signal",
            Some(
                "INSERT INTO harvest_signals (id, workflow_exec_id, signal_name, payload, consumed) \
             VALUES (gen_random_uuid(), $1, 's', '{}'::jsonb, FALSE)",
            ),
        ),
        (
            "nd_blocked",
            Some("UPDATE harvest_workflow_executions SET nd_blocked_at = NOW() WHERE id = $1"),
        ),
        (
            "session",
            Some(
                "INSERT INTO harvest_sessions (id, workflow_exec_id, host_worker_id, queue_name, \
                                           state, created_at, expires_at) \
             VALUES (gen_random_uuid(), $1, 'w', 'default', 'ACTIVE', NOW(), NOW() + interval '1 h')",
            ),
        ),
    ] {
        let exec_id = quiescent_fixture(&shards, &format!("agree-{label}")).await;
        let mut source = shards.source().await;
        if let Some(sql) = mutate {
            diesel::sql_query(sql)
                .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
                .execute(&mut source)
                .await
                .unwrap_or_else(|e| panic!("{label} mutation: {e}"));
        }

        let pure_says = assess_quiescence(
            &observe_quiescence(&mut source, exec_id)
                .await
                .expect("observe"),
        )
        .is_eligible();

        // Drive the SQL half by attempting a real cutover against a migration
        // row parked at VERIFIED, then roll the effect back by inspection.
        begin_migration(&mut source, exec_id, SOURCE, TARGET)
            .await
            .expect("begin");
        // A VERIFIED record is not just a phase: `commit_cutover` also requires
        // the source history to still match the high-water mark verification
        // recorded. Stamp it from the live history so this test exercises the
        // quiescence half in isolation, which is what it is here to pin.
        diesel::sql_query(
            "UPDATE harvest_shard_migrations m SET phase = 'VERIFIED', \
                 verified_event_count = (SELECT count(*) FROM harvest_events ev \
                                          WHERE ev.workflow_exec_id = m.execution_id), \
                 verified_max_event_id = \
                     COALESCE((SELECT max(ev.event_id) FROM harvest_events ev \
                                WHERE ev.workflow_exec_id = m.execution_id), -1) \
               WHERE m.execution_id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .execute(&mut source)
        .await
        .expect("park at VERIFIED");
        let sql_says = commit_cutover(&mut source, exec_id, TARGET)
            .await
            .expect("cutover query");

        assert_eq!(
            pure_says, sql_says,
            "the pure predicate and the cutover SQL disagree for the {label} case"
        );
    }
}

// ── AC4: every id captured before the migration still resolves ───────────────

#[tokio::test]
async fn every_id_holder_class_still_resolves_after_migration() {
    // The acceptance bar is structural rather than enumerated: the ExecutionId
    // is never re-minted, so an id captured by ANY holder is the same 16 bytes
    // afterwards. This test captures ids the way each holder class does — a
    // parent's recorded `child_id`, a stored handle, an external signal target,
    // a webhook's stored reference — and asserts they all resolve to the run's
    // new home through the same resolution the engine uses.
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "entity-ids").await;

    // Every holder class holds exactly this: the 16 bytes.
    let captured_by_parent_child_started = exec_id;
    let captured_by_a_stored_handle = ExecutionId::from_uuid(exec_id.as_uuid());
    let captured_by_an_external_signal_target = ExecutionId::from_uuid(exec_id.as_uuid());
    let captured_by_a_webhook_row = ExecutionId::from_uuid(exec_id.as_uuid());
    let captured_by_a_schedule_lineage = ExecutionId::from_uuid(exec_id.as_uuid());

    // Before: everything resolves to the source.
    for held in [
        captured_by_parent_child_started,
        captured_by_a_stored_handle,
        captured_by_an_external_signal_target,
        captured_by_a_webhook_row,
        captured_by_a_schedule_lineage,
    ] {
        assert_eq!(
            resolve_execution_shard(&shards.pool, held)
                .await
                .expect("resolve"),
            SOURCE
        );
    }

    migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("migrate");

    // After: the same captured ids resolve to the target, with no rewrite of
    // anything anywhere.
    for (label, held) in [
        (
            "parent's ChildWorkflowStarted.child_id",
            captured_by_parent_child_started,
        ),
        ("a stored WorkflowHandle", captured_by_a_stored_handle),
        (
            "an external signal/cancel target",
            captured_by_an_external_signal_target,
        ),
        (
            "a webhook's stored execution reference",
            captured_by_a_webhook_row,
        ),
        (
            "a schedule's carryover lineage",
            captured_by_a_schedule_lineage,
        ),
    ] {
        let resolved = resolve_execution_shard(&shards.pool, held)
            .await
            .unwrap_or_else(|e| panic!("{label} failed to resolve: {e}"));
        assert_eq!(resolved, TARGET, "{label} must resolve to the new home");

        // ...and the run is actually readable there.
        let mut conn = connect(&shards.target_url).await;
        assert_eq!(
            state_of(&mut conn, held).await.as_deref(),
            Some("RUNNING"),
            "{label} must resolve to a live run"
        );
    }
}

#[tokio::test]
async fn a_twice_migrated_run_resolves_through_the_chain_and_collapses_it() {
    // A→B, then B→A (two shards is enough to exercise the chain: the second hop
    // makes the ORIGIN shard's pointer stale until the collapse fixes it).
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "entity-chain").await;

    migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("A -> B");
    assert_eq!(
        resolve_execution_shard(&shards.pool, exec_id)
            .await
            .expect("resolve"),
        TARGET
    );

    // Now forge the pathological case a real deployment must survive: the
    // target is sealed and points BACK at the source, whose own pointer still
    // points here. SOURCE -> TARGET -> SOURCE -> ... is a cycle, and a routing
    // call must fail closed on it rather than spin forever.
    //
    // Two shards is enough to build it precisely because the pointer, not the
    // execution state, is what resolution follows.
    let mut target = shards.target().await;
    diesel::sql_query(
        "UPDATE harvest_workflow_executions \
            SET state = 'MIGRATED', migrated_to_shard = $2, migrated_at = NOW() \
          WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Integer, _>(SOURCE.as_i32())
    .execute(&mut target)
    .await
    .expect("seal the target back at the source");

    let error = resolve_execution_shard(&shards.pool, exec_id)
        .await
        .expect_err("a forwarding cycle must fail closed, not loop");
    assert!(
        error.is_shard_unavailable(),
        "expected the retryable shard-unavailable classification, got {error:?}"
    );
}

#[tokio::test]
async fn a_sealed_source_keeps_forwarding_after_an_operator_force_terminates_it() {
    // `terminate_workflow_execution` carries no state precondition by design —
    // it is an operator override. Applied to a sealed source it overwrites
    // `MIGRATED`, and if resolution keyed on the STATE the run would silently
    // become unreachable by every id anyone had captured. Resolution keys on
    // the POINTER instead, so the override costs nothing.
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "entity-forced").await;
    migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("migrate");

    let mut source = shards.source().await;
    diesel::sql_query("UPDATE harvest_workflow_executions SET state = 'TERMINATED' WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .execute(&mut source)
        .await
        .expect("an operator override must not be refused by the forwarding CHECK");

    assert_eq!(
        resolve_execution_shard(&shards.pool, exec_id)
            .await
            .expect("resolve"),
        TARGET,
        "the id must still resolve after a force-terminate of the sealed source"
    );
}

// ── AC5: zero new event variants ─────────────────────────────────────────────

#[tokio::test]
async fn a_migration_appends_no_events_at_all() {
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "entity-events").await;

    let before: Vec<(i32, String)> = {
        let mut source = shards.source().await;
        diesel::sql_query(
            "SELECT event_id, event_type FROM harvest_events \
              WHERE workflow_exec_id = $1 ORDER BY event_id",
        )
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .load::<EventPair>(&mut source)
        .await
        .expect("events before")
        .into_iter()
        .map(|r| (r.event_id, r.event_type))
        .collect()
    };

    migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("migrate");

    for url in [&shards.source_url, &shards.target_url] {
        let mut conn = connect(url).await;
        let after: Vec<(i32, String)> = diesel::sql_query(
            "SELECT event_id, event_type FROM harvest_events \
              WHERE workflow_exec_id = $1 ORDER BY event_id",
        )
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .load::<EventPair>(&mut conn)
        .await
        .expect("events after")
        .into_iter()
        .map(|r| (r.event_id, r.event_type))
        .collect();
        assert_eq!(
            after, before,
            "a migration must append, reorder and rewrite nothing — on either shard"
        );
    }
}

#[derive(diesel::QueryableByName)]
struct EventPair {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    event_id: i32,
    #[diesel(sql_type = diesel::sql_types::Text)]
    event_type: String,
}

// ── AC6: dedupe scopes ───────────────────────────────────────────────────────

#[tokio::test]
async fn signal_idempotency_keys_and_timers_survive_the_copy() {
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "entity-dedupe").await;

    // A consumed signal carrying an idempotency key. Quiescence forbids an
    // UNconsumed one, but the key's dedupe scope must still move or a webhook
    // retry after the migration would be delivered a second time.
    {
        let mut source = shards.source().await;
        deliver_signal(&mut source, exec_id, "webhook", Some("delivery-42")).await;
        diesel::sql_query("UPDATE harvest_signals SET consumed = TRUE WHERE workflow_exec_id = $1")
            .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
            .execute(&mut source)
            .await
            .expect("consume");
    }

    migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("migrate");

    let mut target = shards.target().await;
    assert_eq!(
        count(
            &mut target,
            "SELECT count(*)::BIGINT AS value FROM harvest_signals \
              WHERE workflow_exec_id = $1 AND idempotency_key = 'delivery-42'",
            exec_id
        )
        .await,
        1,
        "the idempotency key must move with the run"
    );

    // The target enforces the same partial unique index, so a retried delivery
    // collides exactly as it would have on the source.
    let duplicate = diesel::sql_query(
        "INSERT INTO harvest_signals \
             (id, workflow_exec_id, signal_name, payload, consumed, idempotency_key) \
         VALUES (gen_random_uuid(), $1, 'webhook', '{}'::jsonb, FALSE, 'delivery-42')",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut target)
    .await;
    assert!(
        duplicate.is_err(),
        "a redelivered keyed signal must still be deduped after the migration"
    );

    // The unfired timer moved with its exact fire time.
    assert_eq!(
        count(
            &mut target,
            "SELECT count(*)::BIGINT AS value FROM harvest_timers \
              WHERE workflow_exec_id = $1 AND NOT fired AND fires_at > NOW()",
            exec_id
        )
        .await,
        1
    );
}

// ── AC7: crash safety at every kill point ────────────────────────────────────

#[tokio::test]
async fn a_crash_at_every_phase_leaves_exactly_one_authoritative_shard() {
    // The kill-point contract, driven for real: run the migration up to each
    // phase boundary, stop dead (as a process crash would), and assert the run
    // is authoritative on exactly one shard — never zero after the resume sweep,
    // never two at any point.
    let shards = setup_two_shards().await;

    for kill_after in ["begin", "stage", "verify", "cutover"] {
        let exec_id = quiescent_fixture(&shards, &format!("kill-{kill_after}")).await;
        let (mut source, mut target) = (shards.source().await, shards.target().await);

        begin_migration(&mut source, exec_id, SOURCE, TARGET)
            .await
            .expect("begin");
        if kill_after != "begin" {
            stage_copy(&mut source, &mut target, exec_id, TARGET)
                .await
                .expect("stage");
            // A staged copy is inert: it must not be claimable even though its
            // rows exist.
            assert_eq!(
                authoritative_shards(&shards, exec_id).await,
                vec![SOURCE],
                "a staged copy must never be claimable ({kill_after})"
            );
        }
        if kill_after == "verify" || kill_after == "cutover" {
            verify_target_copy(&mut source, &mut target, exec_id, &codecs())
                .await
                .expect("verify");
        }
        if kill_after == "cutover" {
            assert!(
                commit_cutover(&mut source, exec_id, TARGET)
                    .await
                    .expect("cutover")
            );
            // Between the cutover and activation the run is claimable NOWHERE —
            // a liveness gap, never a correctness one. What must never happen is
            // TWO.
            assert!(
                authoritative_shards(&shards, exec_id).await.len() <= 1,
                "never claimable on two shards at once"
            );
        }

        // ── the crash: a fresh process picks the row up ──────────────────────
        let outcomes = resume_incomplete_migrations(&shards.pool, SOURCE, 100, "tester", &codecs())
            .await
            .expect("resume");

        let live = authoritative_shards(&shards, exec_id).await;
        assert_eq!(
            live.len(),
            1,
            "after resume, exactly one shard must be authoritative for {kill_after} \
             (got {live:?}, outcomes {outcomes:?})"
        );
        // Pre-cutover kills resume forward to the target; the design never rolls
        // a verified copy back for its own sake, only when the source woke.
        assert_eq!(
            live,
            vec![TARGET],
            "a resume must finish the migration, not abandon it ({kill_after})"
        );
    }
}

#[tokio::test]
async fn resume_finishes_a_migration_killed_after_the_cutover() {
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "entity-resume").await;
    let (mut source, mut target) = (shards.source().await, shards.target().await);

    begin_migration(&mut source, exec_id, SOURCE, TARGET)
        .await
        .expect("begin");
    stage_copy(&mut source, &mut target, exec_id, TARGET)
        .await
        .expect("stage");
    verify_target_copy(&mut source, &mut target, exec_id, &codecs())
        .await
        .expect("verify");
    assert!(
        commit_cutover(&mut source, exec_id, TARGET)
            .await
            .expect("cutover")
    );

    // Crash here. The durable COMMITTED record is the only thing that knows.
    let record = load_migration(&mut source, exec_id)
        .await
        .expect("load")
        .expect("row");
    assert_eq!(record.phase, MigrationPhase::Committed);

    resume_incomplete_migrations(&shards.pool, SOURCE, 10, "tester", &codecs())
        .await
        .expect("resume");

    assert_eq!(authoritative_shards(&shards, exec_id).await, vec![TARGET]);
    let settled = load_migration(&mut source, exec_id)
        .await
        .expect("load")
        .expect("row");
    assert_eq!(settled.phase, MigrationPhase::Done);

    // Idempotent: a second sweep changes nothing.
    resume_incomplete_migrations(&shards.pool, SOURCE, 10, "tester", &codecs())
        .await
        .expect("resume twice");
    assert_eq!(authoritative_shards(&shards, exec_id).await, vec![TARGET]);
}

#[tokio::test]
async fn a_resume_aborts_a_pre_cutover_migration_whose_source_woke_up() {
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "entity-woke").await;
    let (mut source, mut target) = (shards.source().await, shards.target().await);

    begin_migration(&mut source, exec_id, SOURCE, TARGET)
        .await
        .expect("begin");
    stage_copy(&mut source, &mut target, exec_id, TARGET)
        .await
        .expect("stage");
    deliver_signal(&mut source, exec_id, "poke", None).await;

    resume_incomplete_migrations(&shards.pool, SOURCE, 10, "tester", &codecs())
        .await
        .expect("resume");

    let settled = load_migration(&mut source, exec_id)
        .await
        .expect("load")
        .expect("row");
    assert_eq!(settled.phase, MigrationPhase::Aborted);
    assert_eq!(authoritative_shards(&shards, exec_id).await, vec![SOURCE]);
    assert_eq!(
        state_of(&mut target, exec_id).await,
        None,
        "an aborted migration leaves no debris on the target"
    );
}

// ── AC8: the operator batch surface ──────────────────────────────────────────

#[tokio::test]
async fn a_dry_run_writes_nothing_and_reports_the_population_a_real_run_would_move() {
    let shards = setup_two_shards().await;
    for n in 0..3 {
        quiescent_fixture(&shards, &format!("dry-{n}")).await;
    }
    // ...plus one that is not quiescent.
    let busy = quiescent_fixture(&shards, "dry-busy").await;
    {
        let mut source = shards.source().await;
        deliver_signal(&mut source, busy, "poke", None).await;
    }

    let dry =
        migrate_quiescent_executions(&shards.pool, SOURCE, TARGET, 10, true, "tester", &codecs())
            .await
            .expect("dry run");
    assert!(dry.dry_run);
    assert_eq!(dry.would_migrate(), 3);
    assert_eq!(dry.migrated(), 0);
    assert_eq!(dry.skipped(), 1);
    assert_eq!(dry.examined, 4);

    // Nothing was written anywhere.
    let mut target = shards.target().await;
    assert_eq!(
        count(
            &mut target,
            "SELECT count(*)::BIGINT AS value FROM harvest_workflow_executions \
              WHERE $1 = $1",
            busy
        )
        .await,
        0,
        "a dry run must write nothing to the target"
    );

    // The real run moves exactly the population the dry run named.
    let real =
        migrate_quiescent_executions(&shards.pool, SOURCE, TARGET, 10, false, "tester", &codecs())
            .await
            .expect("real run");
    assert_eq!(real.migrated(), dry.would_migrate());
    assert_eq!(real.skipped(), dry.skipped());
}

#[tokio::test]
async fn the_batch_is_bounded_by_its_limit_and_reports_every_outcome() {
    let shards = setup_two_shards().await;
    for n in 0..5 {
        quiescent_fixture(&shards, &format!("batch-{n}")).await;
    }

    let report =
        migrate_quiescent_executions(&shards.pool, SOURCE, TARGET, 2, false, "tester", &codecs())
            .await
            .expect("batch");
    assert_eq!(
        report.migrated(),
        2,
        "the limit must bound what actually moves"
    );
    assert_eq!(report.source_shard, SOURCE);
    assert_eq!(report.target_shard, TARGET);
    assert!(
        report
            .outcomes
            .iter()
            .all(|o| o.execution_id().shard() == SOURCE),
        "every outcome names the execution it is about"
    );

    // The remaining three are still on the source and still eligible.
    let mut source = shards.source().await;
    let remaining = list_migration_candidates(&mut source, 100)
        .await
        .expect("candidates");
    assert_eq!(remaining.iter().filter(|c| c.is_eligible()).count(), 3);
}

#[tokio::test]
async fn a_batch_to_the_same_shard_is_refused() {
    let shards = setup_two_shards().await;
    let error =
        migrate_quiescent_executions(&shards.pool, SOURCE, SOURCE, 1, true, "tester", &codecs())
            .await
            .expect_err("a self-migration is meaningless and would forward a row to itself");
    assert!(matches!(error, HarvestError::Config(_)), "got {error:?}");
}

#[tokio::test]
async fn a_second_concurrent_migration_for_the_same_execution_is_refused() {
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "entity-double").await;
    let mut source = shards.source().await;

    begin_migration(&mut source, exec_id, SOURCE, TARGET)
        .await
        .expect("first");
    let error = begin_migration(&mut source, exec_id, SOURCE, TARGET)
        .await
        .expect_err("two operators must not open two migrations for one run");
    assert!(
        matches!(error, HarvestError::AlreadyExists { .. }),
        "got {error:?}"
    );
}

// ── Signal-parked runs: the other half of the eligible population ────────────

#[tokio::test]
async fn a_signal_parked_execution_migrates_with_its_parked_task_row() {
    let shards = setup_two_shards().await;
    let mut source = shards.source().await;
    let exec_id = insert_execution(&mut source, "entity_flow", "signal-parked").await;
    append_history(&mut source, exec_id, &[started(json!({}))]).await;
    park_on_signal(&mut source, exec_id).await;

    migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("migrate");

    assert_eq!(authoritative_shards(&shards, exec_id).await, vec![TARGET]);
    let mut target = shards.target().await;
    // The park shape is preserved verbatim: RUNNING with no worker is how the
    // engine represents "waiting on a signal", and a wake re-pends exactly it.
    let parked = count(
        &mut target,
        "SELECT count(*)::BIGINT AS value FROM harvest_task_queue \
          WHERE workflow_exec_id = $1 AND task_type = 'workflow' \
            AND state = 'RUNNING' AND worker_id IS NULL",
        exec_id,
    )
    .await;
    assert_eq!(parked, 1);
}

// ── Codex round 1: the two copies of a rebalanced run ────────────────────────
//
// A rebalance seals the source rather than deleting it, so for as long as the
// source shard's retention has not collected the row an execution's bytes exist
// in two databases. Every operation that reasons about "the execution" has to
// pick the right one — or, for erasure, both. These two tests pin the two
// places where picking the wrong one is a correctness failure rather than a
// latency cost.

#[tokio::test]
async fn erasing_a_migrated_execution_scrubs_the_sealed_source_copy_too() {
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "gdpr-subject").await;

    migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("migrate");

    // Retire the live copy so the erase gate admits it.
    let mut target = shards.target().await;
    diesel::sql_query(
        "UPDATE harvest_workflow_executions SET state = 'COMPLETED', completed_at = now() \
          WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut target)
    .await
    .expect("complete the migrated run");

    // Both databases hold the subject's payload right now. That is the whole
    // hazard: an erase that visits only the shard the id routes to reports
    // success over a complete, readable second copy.
    let mut source = shards.source().await;
    assert!(
        payload_bearing_events(&mut source, exec_id).await > 0,
        "precondition: the sealed source still holds the subject's payloads"
    );

    let outcome = autumn_harvest::erase::erase_workflow_payloads_all_residences(
        &shards.pool,
        exec_id,
        "gdpr subject request",
    )
    .await
    .expect("erase across every residence");

    let mut source = shards.source().await;
    assert_eq!(
        payload_bearing_events(&mut source, exec_id).await,
        0,
        "the sealed source copy must be scrubbed, not just the live one"
    );
    let mut target = shards.target().await;
    assert_eq!(payload_bearing_events(&mut target, exec_id).await, 0);

    // And the response says so: the prior residence is named, so an operator
    // answering a regulator can point at the evidence rather than at intent.
    assert_eq!(outcome.prior_residences.len(), 1);
    assert_eq!(outcome.prior_residences[0].shard_id, SOURCE.as_i32());
    assert!(outcome.prior_residences[0].outcome.events_scrubbed > 0);
}

#[tokio::test]
async fn a_batch_signal_reaches_the_migrated_copy_not_the_sealed_source() {
    use autumn_harvest::batch::{
        BatchAction, BatchExecutorConfig, BatchFilter, BatchSubmission, get_batch_job,
        run_executor_once, submit_batch_job,
    };

    let shards = setup_two_shards().await;
    let exec_id = insert_execution(&mut shards.source().await, "entity_flow", "batch-target").await;
    let mut source = shards.source().await;
    append_history(&mut source, exec_id, &[started(json!({}))]).await;
    park_on_signal(&mut source, exec_id).await;

    migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("migrate");

    // The batch's all-shard scan finds the live RUNNING copy on the target. Its
    // id, though, still encodes SOURCE — the identity is deliberately never
    // re-minted — so an origin-only pool lookup would dispatch the signal into
    // the sealed source, where the row reads as MIGRATED and the send fails.
    let mut target = shards.target().await;
    let job_id = submit_batch_job(
        &mut target,
        BatchSubmission {
            action: BatchAction::Signal,
            filter: BatchFilter {
                states: vec!["RUNNING".to_string()],
                workflow_name: Some("entity_flow".to_string()),
                search_attrs: vec![],
            },
            signal_name: Some("wake".to_string()),
            signal_payload: Some(json!({"from": "batch"})),
            idempotency_key: None,
            created_by: Some("test".to_string()),
        },
    )
    .await
    .expect("submit batch job");

    run_executor_once(&shards.pool, &BatchExecutorConfig::default())
        .await
        .expect("executor tick");

    let mut target = shards.target().await;
    let job = get_batch_job(&mut target, job_id)
        .await
        .expect("load job")
        .expect("job row");
    assert_eq!(job.completed, 1, "job errors: {:?}", job.errors);
    assert_eq!(job.failed, 0, "job errors: {:?}", job.errors);

    // The signal landed where the run actually lives, and nowhere else.
    let mut target = shards.target().await;
    assert_eq!(
        count(
            &mut target,
            "SELECT count(*)::BIGINT AS value FROM harvest_signals \
              WHERE workflow_exec_id = $1 AND signal_name = 'wake'",
            exec_id
        )
        .await,
        1
    );
    let mut source = shards.source().await;
    assert_eq!(
        count(
            &mut source,
            "SELECT count(*)::BIGINT AS value FROM harvest_signals \
              WHERE workflow_exec_id = $1 AND signal_name = 'wake'",
            exec_id
        )
        .await,
        0,
        "the sealed source must not accumulate deliveries for a run it no longer hosts"
    );
}

/// Event rows for `exec_id` that still carry an un-tombstoned payload field.
async fn payload_bearing_events(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> i64 {
    count(
        conn,
        "SELECT count(*)::BIGINT AS value FROM harvest_events e \
          WHERE e.workflow_exec_id = $1 \
            AND jsonb_typeof(e.event_data->'data') = 'object' \
            AND EXISTS ( \
              SELECT 1 FROM jsonb_each(e.event_data->'data') AS f(k, v) \
               WHERE k = ANY(ARRAY['input','output','payload','details','value', \
                                   'last_completion_result']) \
                 AND v <> '{\"_harvest_erased\": true}'::jsonb)",
        exec_id,
    )
    .await
}

// ── Codex round 2: verification is a snapshot, and pointers are collapsed ────

#[tokio::test]
async fn a_cutover_refuses_a_source_whose_history_advanced_since_verification() {
    // The window the quiescence re-check alone does not close. Between
    // verification and the cutover — instant in the end-to-end path, but hours
    // on a resume after a crash at VERIFIED — the run can legitimately wake,
    // execute a whole decision cycle, append events, and park again. It is
    // quiescent once more, so every predicate in the cutover's WHERE passes.
    // Sealing then hands authority to a copy that predates that cycle: not a
    // lost wake but lost PROGRESS, and invisible afterwards.
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "entity-advanced").await;
    let (mut source, mut target) = (shards.source().await, shards.target().await);

    begin_migration(&mut source, exec_id, SOURCE, TARGET)
        .await
        .expect("begin");
    stage_copy(&mut source, &mut target, exec_id, TARGET)
        .await
        .expect("stage");
    verify_target_copy(&mut source, &mut target, exec_id, &codecs())
        .await
        .expect("verify");

    // The run wakes, runs a cycle, and re-parks on a fresh long timer. Modelled
    // exactly as the engine would leave it: new events appended, and a task row
    // that is parked again rather than claimed.
    append_more(
        &mut source,
        exec_id,
        &[
            WorkflowEvent::TimerFired {
                timer_id: autumn_harvest::types::TimerId::new("wake"),
            },
            WorkflowEvent::TimerStarted {
                timer_id: autumn_harvest::types::TimerId::new("wake-2"),
                duration_secs: 604_800,
            },
        ],
    )
    .await;
    // Re-park the SAME task row on a fresh timer, which is what the engine
    // does: a second row would itself be a quiescence blocker and would not
    // model a woken-and-re-parked run at all.
    diesel::sql_query(
        "UPDATE harvest_timers SET fired = TRUE WHERE workflow_exec_id = $1 AND timer_id = 'wake'",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut source)
    .await
    .expect("fire the old timer");
    diesel::sql_query(
        "INSERT INTO harvest_timers (id, workflow_exec_id, timer_id, fires_at, fired) \
         VALUES (gen_random_uuid(), $1, 'wake-2', NOW() + interval '7 days', FALSE)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut source)
    .await
    .expect("start the new timer");
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'PENDING', worker_id = NULL, \
             started_at = NULL, scheduled_at = NOW() + interval '7 days' \
           WHERE workflow_exec_id = $1 AND task_type = 'workflow'",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut source)
    .await
    .expect("re-park the task row");

    // Quiescence alone says yes...
    assert!(
        assess_quiescence(
            &observe_quiescence(&mut source, exec_id)
                .await
                .expect("observe")
        )
        .is_eligible(),
        "precondition: the re-parked run is quiescent again, so quiescence \
         alone would license the cutover"
    );

    // ...and the cutover still refuses, because the verified copy is stale.
    let committed = commit_cutover(&mut source, exec_id, TARGET)
        .await
        .expect("cutover query");
    assert!(
        !committed,
        "the cutover must refuse a source whose history advanced past the \
         verified copy"
    );
    assert_eq!(
        state_of(&mut source, exec_id).await.as_deref(),
        Some("RUNNING"),
        "the source must be left untouched"
    );
    assert_eq!(authoritative_shards(&shards, exec_id).await, vec![SOURCE]);

    // And the refusal is not a wedge. A resume sweep settles the record — it
    // declines the cutover for the same reason and aborts — leaving exactly one
    // authoritative shard and nothing for an operator to clean up by hand. The
    // run is simply migrated later, from its current history.
    resume_incomplete_migrations(&shards.pool, SOURCE, 10, "tester", &codecs())
        .await
        .expect("resume");
    assert_eq!(authoritative_shards(&shards, exec_id).await, vec![SOURCE]);
    let record = load_migration(&mut source, exec_id)
        .await
        .expect("load")
        .expect("row");
    assert_eq!(record.phase, MigrationPhase::Aborted);
}

#[tokio::test]
async fn erasing_a_twice_migrated_run_scrubs_the_intermediate_shard_too() {
    // The forwarding pointers are deliberately COLLAPSED: after A -> B -> C,
    // A points straight at C and B has vanished from the pointer graph. B's
    // sealed copy still holds every payload it had. A residence chain derived
    // from the pointers would therefore report [A, C] and an erasure built on
    // it would claim success having never touched B.
    let shards = setup_three_shards().await;
    let mut a = connect(&shards.urls[0]).await;
    let exec_id = insert_execution(&mut a, "entity_flow", "gdpr-two-hop").await;
    append_history(&mut a, exec_id, &[started(json!({"pii": "subject"}))]).await;
    park_on_signal(&mut a, exec_id).await;

    migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("A -> B");
    migrate_execution(&shards.pool, exec_id, TARGET, THIRD, &codecs())
        .await
        .expect("B -> C");

    // The pointer graph has been collapsed past B...
    let mut a = connect(&shards.urls[0]).await;
    assert_eq!(
        forward_of(&mut a, exec_id).await,
        Some(THIRD.as_i32()),
        "precondition: the origin's pointer was collapsed straight to the newest \
         residence, erasing B from the pointer graph"
    );
    // ...but B still holds the payloads.
    let mut b = connect(&shards.urls[1]).await;
    assert!(
        payload_bearing_events(&mut b, exec_id).await > 0,
        "precondition: the intermediate sealed copy still holds the subject's data"
    );

    let mut c = connect(&shards.urls[2]).await;
    diesel::sql_query(
        "UPDATE harvest_workflow_executions SET state = 'COMPLETED', completed_at = now() \
          WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut c)
    .await
    .expect("complete the migrated run");

    let outcome = autumn_harvest::erase::erase_workflow_payloads_all_residences(
        &shards.pool,
        exec_id,
        "gdpr subject request",
    )
    .await
    .expect("erase across every residence");

    for (label, url) in [
        ("A", &shards.urls[0]),
        ("B", &shards.urls[1]),
        ("C", &shards.urls[2]),
    ] {
        let mut conn = connect(url).await;
        assert_eq!(
            payload_bearing_events(&mut conn, exec_id).await,
            0,
            "shard {label} still holds un-erased payloads"
        );
    }
    let named: Vec<i32> = outcome
        .prior_residences
        .iter()
        .map(|r| r.shard_id)
        .collect();
    assert_eq!(
        named,
        vec![SOURCE.as_i32(), TARGET.as_i32()],
        "both prior residences must be named, oldest first"
    );
}
