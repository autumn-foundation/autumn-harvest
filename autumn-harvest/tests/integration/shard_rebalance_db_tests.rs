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
//!   [`a_signal_arriving_after_cutover_is_retried_then_delivered_to_the_target`],
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
use autumn_harvest::shard::{ShardRouter, ShardedDbPool, install_global_router};
use autumn_harvest::shard_rebalance::{
    MigrationOutcome, MigrationPhase, QuiescenceBlocker, abort_migration, activate_target,
    assess_quiescence, begin_migration, commit_cutover, conn_for_execution_forwarded_with_shard,
    history_fingerprint, list_migration_candidates, load_migration, migrate_execution,
    migrate_quiescent_executions, migrate_quiescent_executions_after, observe_quiescence,
    reconcile_migrated_seal_terminality, reconcile_migrated_seals_after, residence_chain,
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
    insert_execution_with_id(
        conn,
        workflow_name,
        workflow_id,
        ExecutionId::new_for_shard(SOURCE),
        SOURCE,
    )
    .await
}

/// Insert a `RUNNING` root execution under a caller-chosen `ExecutionId`, so a
/// test can mint an id whose shard bits deliberately disagree with the shard
/// the row physically lives on.
async fn insert_execution_with_id(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    exec_id: ExecutionId,
    resident_shard: ShardId,
) -> ExecutionId {
    use autumn_harvest::schema::harvest_workflow_executions;
    let row = NewWorkflowExecution {
        quota_key: None,
        continued_from_exec_id: None,
        first_exec_id: None,
        id: exec_id.as_uuid(),
        workflow_name,
        workflow_id,
        run_id: Uuid::new_v4(),
        shard_id: resident_shard.as_i32(),
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

struct XorCodec(u8);

impl autumn_harvest::payload_codec::PayloadCodec for XorCodec {
    fn codec_id(&self) -> &'static str {
        "xor-test-codec"
    }
    fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, autumn_harvest::payload_codec::CodecError> {
        Ok(raw.iter().map(|b| b ^ self.0).collect())
    }
    fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, autumn_harvest::payload_codec::CodecError> {
        Ok(encoded.iter().map(|b| b ^ self.0).collect())
    }
}

#[tokio::test]
async fn verification_degrades_to_the_raw_copy_check_when_the_codec_is_unregistered() {
    // Issue #1317: `harvest shard rebalance`/`rebalance-resume` pass
    // `PayloadCodecs::default()` (identity-only) into verification. A
    // deployment that encodes its payloads under its own codec -- as any
    // encrypted-at-rest deployment does -- cannot be decoded by that
    // default registry. Before this fix, `verify_target_copy` propagated
    // the resulting `UnknownPayloadCodec`/`UnknownCodecKey` error and
    // refused to migrate at all, for every such deployment.
    let shards = setup_two_shards().await;
    let mut app_codecs = PayloadCodecs::default();
    app_codecs.set_default(std::sync::Arc::new(XorCodec(0x5a)));

    let mut source = shards.source().await;
    let exec_id = insert_execution(&mut source, "entity_flow", "encrypted-payloads").await;
    let start_event = started(json!({"seed": 1}));
    store::append_events_with_codecs(
        &mut source,
        exec_id,
        std::slice::from_ref(&start_event),
        0,
        &app_codecs,
    )
    .await
    .expect("append events under the app's own codec");
    park_on_timer(&mut source, exec_id).await;

    begin_migration(&mut source, exec_id, SOURCE, TARGET)
        .await
        .expect("begin");
    let mut target = shards.target().await;
    stage_copy(&mut source, &mut target, exec_id, TARGET)
        .await
        .expect("stage");

    // The CLI's registry, which knows nothing of the application's codec.
    let cli_codecs = codecs();
    let fingerprint = verify_target_copy(&mut source, &mut target, exec_id, &cli_codecs)
        .await
        .expect(
            "verification must degrade to the raw byte-identity check, not refuse to \
             migrate an encrypted deployment outright",
        );
    assert!(
        fingerprint.starts_with("raw:"),
        "a degraded verification must say so in its fingerprint: {fingerprint}"
    );

    assert_eq!(
        load_migration(&mut source, exec_id)
            .await
            .expect("load migration")
            .expect("record exists")
            .phase,
        MigrationPhase::Verified,
        "the migration must still advance to VERIFIED"
    );

    assert!(
        commit_cutover(&mut source, exec_id, TARGET)
            .await
            .expect("cutover")
    );
    activate_target(&mut source, &mut target, exec_id)
        .await
        .expect("activate");

    // The target's copy, decoded under the APPLICATION's real codec, is
    // exactly what was written -- the raw check verified real bytes, not a
    // vacuous pass.
    let target_history = store::load_history_with_codecs(&mut target, exec_id, &app_codecs)
        .await
        .expect("decode with the real codec");
    assert_eq!(
        serde_json::to_value(&target_history.events).unwrap(),
        serde_json::to_value(vec![start_event]).unwrap()
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
async fn a_signal_arriving_after_cutover_is_retried_then_delivered_to_the_target() {
    // Issue #1317: the prior version of this test seeded the post-cutover
    // signal with `deliver_signal`, a raw insert into `harvest_signals`.
    // That never goes through `signal::send_signal_idempotent`, the
    // engine's real write path.
    //
    // So it proved "a signal row already present at activation is
    // scheduled promptly" (real, and still asserted below). It did not
    // prove "a signal arriving after cutover is delivered". The production
    // path would have refused to create that row at all, in the
    // `"MIGRATED" | "MIGRATING"` branch in `signal.rs`. This test never
    // drove that branch.
    //
    // Rewritten to call the real path. `send_signal_idempotent`, on a
    // connection resolved to the target, still sees `state = 'MIGRATING'`
    // there. Cutover writes only the source; activation is what flips the
    // target to `RUNNING`. The function cannot locally tell "staged, not
    // yet cut over" apart from "cut over, awaiting activation" — see
    // `signal.rs` for why. It refuses, retryably, either way. That is
    // documented, intentional behavior, not a bug this change closes. The
    // retry succeeds once activation runs.
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
    // target — which is where the signal must land, eventually.
    let resolved = resolve_execution_shard(&shards.pool, exec_id)
        .await
        .expect("resolve");
    assert_eq!(resolved, TARGET);

    // The real engine path, on a connection already resolved to the target:
    // refused, retryably, before activation runs.
    let refused = autumn_harvest::signal::send_signal_idempotent(
        &mut target,
        exec_id,
        "poke",
        serde_json::Value::Null,
        None,
    )
    .await;
    assert!(
        matches!(refused, Err(HarvestError::ShardUnavailable { .. })),
        "a signal on the target must be refused, retryably, before activation: {refused:?}"
    );
    assert_eq!(
        count(
            &mut target,
            "SELECT count(*)::BIGINT AS value FROM harvest_signals WHERE workflow_exec_id = $1",
            exec_id
        )
        .await,
        0,
        "a refused send must roll back its own insert, not leave an orphaned row"
    );

    // Activation flips the target to `RUNNING`, closing the refusal window.
    activate_target(&mut source, &mut target, exec_id)
        .await
        .expect("activate");

    // The retry, on the now-`RUNNING` target, succeeds.
    let delivered = autumn_harvest::signal::send_signal_idempotent(
        &mut target,
        exec_id,
        "poke",
        serde_json::Value::Null,
        None,
    )
    .await
    .expect("the retry must succeed once activation has run");
    assert!(delivered, "a fresh, non-keyed send is always delivered");
    assert_eq!(
        count(
            &mut target,
            "SELECT count(*)::BIGINT AS value FROM harvest_signals \
              WHERE workflow_exec_id = $1 AND NOT consumed",
            exec_id
        )
        .await,
        1,
        "the signal must persist on the target once it is live, not be lost"
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
        // `legal_hold_verified` must also be stamped, exactly as
        // `verify_target_copy` would for an execution with no hold. Otherwise
        // the cutover's `LEGAL_HOLD_UNCHANGED_SQL` guard fails closed
        // regardless of quiescence, the half this test is here to pin.
        diesel::sql_query(
            "UPDATE harvest_shard_migrations m SET phase = 'VERIFIED', \
                 verified_event_count = (SELECT count(*) FROM harvest_events ev \
                                          WHERE ev.workflow_exec_id = m.execution_id), \
                 verified_max_event_id = \
                     COALESCE((SELECT max(ev.event_id) FROM harvest_events ev \
                                WHERE ev.workflow_exec_id = m.execution_id), -1), \
                 legal_hold_verified = TRUE, \
                 verified_legal_hold_set_at = \
                     (SELECT legal_hold_set_at FROM harvest_workflow_executions \
                       WHERE id = m.execution_id) \
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
    let remaining = list_migration_candidates(&mut source, 100, None)
        .await
        .expect("candidates");
    assert_eq!(remaining.iter().filter(|c| c.is_eligible()).count(), 3);
}

#[tokio::test]
async fn repeating_the_batch_command_advances_past_a_blocked_prefix() {
    // Issue #1317: before this fix, the scan always restarted at the
    // shard's oldest `RUNNING` row. A busy shard's oldest rows are exactly
    // the population most likely to be permanently blocked (an active
    // session, a parked child). They filled the whole `scan_limit` window
    // on every call. No amount of repeating the documented batch command
    // ever reached an eligible row sitting behind that prefix, because
    // nothing ever moved the window forward.
    let shards = setup_two_shards().await;
    let mut source = shards.source().await;

    // Four OLDER executions, permanently blocked: a worker holds their task
    // claim indefinitely (the same trick
    // `a_non_quiescent_execution_is_skipped_with_named_blockers` uses). Four,
    // not fewer, because `migrate_quiescent_executions` scans `limit * 4`
    // (clamped). With `limit = 1` that is exactly this prefix's size, so
    // the first call's window is entirely consumed by blocked rows.
    for n in 0..4 {
        let exec_id = quiescent_fixture(&shards, &format!("blocked-{n}")).await;
        diesel::sql_query(
            "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = 'stuck-worker', \
                    scheduled_at = NOW(), started_at = NOW() \
              WHERE workflow_exec_id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .execute(&mut source)
        .await
        .expect("claim");
        diesel::sql_query(
            "UPDATE harvest_workflow_executions SET created_at = NOW() - INTERVAL '2 hours' \
              WHERE id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .execute(&mut source)
        .await
        .expect("backdate");
    }

    // One NEWER execution, genuinely eligible.
    let eligible_id = quiescent_fixture(&shards, "eligible-behind-the-blocked-prefix").await;

    // First call: `limit` is small enough that `scan_limit` (4x, per the
    // batch's own headroom policy) exactly covers the blocked prefix. Both
    // examined rows are named `Skipped` with their blocker. The scan is
    // NOT made tighter to exclude them. An operator still sees why each
    // one did not move.
    let first =
        migrate_quiescent_executions(&shards.pool, SOURCE, TARGET, 1, true, "tester", &codecs())
            .await
            .expect("first dry run");
    assert_eq!(first.examined, 4, "the blocked prefix fills this window");
    assert_eq!(first.skipped(), 4);
    assert_eq!(first.would_migrate(), 0);
    let cursor = first
        .next_scan_cursor
        .expect("a full window must offer a cursor to resume past");

    // Repeating the SAME command with the SAME limit and no cursor would
    // find the identical two blocked rows forever -- confirm that directly.
    let repeated_without_cursor =
        migrate_quiescent_executions(&shards.pool, SOURCE, TARGET, 1, true, "tester", &codecs())
            .await
            .expect("repeated dry run");
    assert_eq!(
        repeated_without_cursor.would_migrate(),
        0,
        "without the cursor, the scan restarts at the same blocked prefix"
    );

    // With the cursor, the SAME command finds the eligible execution.
    let resumed = migrate_quiescent_executions_after(
        &shards.pool,
        SOURCE,
        TARGET,
        1,
        true,
        "tester",
        &codecs(),
        Some(cursor),
    )
    .await
    .expect("resumed dry run");
    assert_eq!(resumed.examined, 1);
    assert_eq!(resumed.would_migrate(), 1);
    match &resumed.outcomes[0] {
        MigrationOutcome::WouldMigrate { execution_id } => {
            assert_eq!(*execution_id, eligible_id);
        }
        other => panic!("expected WouldMigrate, got {other:?}"),
    }
}

#[tokio::test]
async fn the_resume_cursor_names_the_last_processed_row_not_the_last_fetched_one() {
    // Issue #1317 review: `moved >= limit` can break the loop before every
    // fetched candidate is examined. A cursor taken from the fetched
    // window's last row (instead of the last row the loop actually
    // reached) then skips every row in between. That gap is never
    // re-examined on the next call.
    let shards = setup_two_shards().await;
    let mut ids = Vec::new();
    for n in 0..5 {
        ids.push(quiescent_fixture(&shards, &format!("cursor-{n}")).await);
        // Force a distinct `created_at` per row so the scan's ASC order is
        // deterministic even at sub-millisecond fixture creation speed.
        let mut source = shards.source().await;
        diesel::sql_query(
            "UPDATE harvest_workflow_executions \
                SET created_at = NOW() - ($2::bigint * INTERVAL '1 second') \
              WHERE id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(ids[n].as_uuid())
        .bind::<diesel::sql_types::BigInt, _>(4 - i64::try_from(n).unwrap())
        .execute(&mut source)
        .await
        .expect("backdate");
    }

    // `limit = 1` and 5 eligible rows means `scan_limit` (4x) fetches the
    // oldest 4. The loop breaks after the very first one. Rows 2-4 of the
    // window are fetched but never examined this call.
    let first =
        migrate_quiescent_executions(&shards.pool, SOURCE, TARGET, 1, true, "tester", &codecs())
            .await
            .expect("first dry run");
    assert_eq!(first.examined, 4);
    assert_eq!(first.would_migrate(), 1);
    match &first.outcomes[0] {
        MigrationOutcome::WouldMigrate { execution_id } => {
            assert_eq!(*execution_id, ids[0], "the oldest row is examined first");
        }
        other => panic!("expected WouldMigrate, got {other:?}"),
    }
    let cursor = first
        .next_scan_cursor
        .expect("a break on `moved >= limit` still leaves more to examine");

    // The resumed call must pick up the SECOND row next, not skip straight
    // past the whole first window to the fifth.
    let resumed = migrate_quiescent_executions_after(
        &shards.pool,
        SOURCE,
        TARGET,
        1,
        true,
        "tester",
        &codecs(),
        Some(cursor),
    )
    .await
    .expect("resumed dry run");
    assert_eq!(resumed.would_migrate(), 1);
    match &resumed.outcomes[0] {
        MigrationOutcome::WouldMigrate { execution_id } => {
            assert_eq!(*execution_id, ids[1], "row 2 must not be skipped over");
        }
        other => panic!("expected WouldMigrate, got {other:?}"),
    }
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

// ── Codex round 3: the residence history outliving the row it lived on ───────

#[tokio::test]
async fn erasure_still_reaches_the_source_after_target_retention_summarised_the_run() {
    // The live shard's retention janitor eventually deletes a terminal run's
    // execution row and keeps only a compact `harvest_execution_summaries` row.
    // The sealed source copies are NOT collected with it — retention
    // deliberately never purges a `MIGRATED` row, because that would destroy
    // the forwarding pointer — so their payloads are still sitting there. A
    // residence lookup that read "never migrated" from the execution row's
    // absence would report a clean erasure over exactly those copies.
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "gdpr-summarised").await;

    migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("migrate");

    // Demote the live copy exactly as retention does: a summary row carrying
    // the residence history, and no execution row.
    let mut target = shards.target().await;
    diesel::sql_query(
        "INSERT INTO harvest_execution_summaries \
             (execution_id, workflow_name, workflow_id, state, started_at, completed_at, \
              duration_ms, shard_id, search_attrs, result, error, parent_id, \
              migrated_from_shards) \
         SELECT e.id, e.workflow_name, e.workflow_id, 'COMPLETED', e.started_at, NOW(), 0, \
                e.shard_id, e.search_attrs, e.input, NULL, e.parent_id, e.migrated_from_shards \
           FROM harvest_workflow_executions e WHERE e.id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut target)
    .await
    .expect("summarise");
    diesel::sql_query("DELETE FROM harvest_workflow_executions WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .execute(&mut target)
        .await
        .expect("retention-delete the execution row");

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
        "the residence history must survive the execution row's collection"
    );
    assert_eq!(outcome.prior_residences.len(), 1);
    assert_eq!(outcome.prior_residences[0].shard_id, SOURCE.as_i32());
}

#[tokio::test]
async fn the_caller_residence_is_read_from_the_held_row_not_decoded_from_its_id() {
    // The outbox sweeps decide whether a delivery target resolves to the same
    // POOL they are already transacting on. `ExecutionId` encodes where a run
    // originated, so a caller that has itself been rebalanced would compare its
    // origin against the target's residence and call two connections to the
    // same database "cross-pool" — taking the branch that checks out a second
    // connection from the pool already driving the transaction, and
    // self-deadlocking a pool of the supported minimum size one.
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "entity-caller").await;

    let mut source = shards.source().await;
    assert_eq!(
        autumn_harvest::shard_rebalance::shard_of_held_row(&mut source, exec_id).await,
        Some(SOURCE)
    );

    migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("migrate");

    // The id still encodes SOURCE — the identity is never re-minted — but the
    // run now lives on TARGET, and the connection that holds it says so.
    assert_eq!(exec_id.shard(), SOURCE);
    let mut target = shards.target().await;
    assert_eq!(
        autumn_harvest::shard_rebalance::shard_of_held_row(&mut target, exec_id).await,
        Some(TARGET),
        "the held row's shard_id follows the run across a migration"
    );

    // A connection that does not hold the row at all answers None, so the
    // caller falls back to the id's encoded shard (the pre-#964 behaviour)
    // rather than to a wrong shard.
    let mut other = shards.source().await;
    assert_eq!(
        autumn_harvest::shard_rebalance::shard_of_held_row(&mut other, ExecutionId::new()).await,
        None
    );
}

#[tokio::test]
async fn conn_for_execution_forwarded_with_shard_names_the_connection_it_returns() {
    // Issue #1317 review: a caller may need to attribute a connection to a
    // shard (an audit log, say). It must read that off THIS call's own
    // return value. A caller that instead re-resolves with a separate
    // `resolve_execution_shard` call while still holding this connection can
    // deadlock a pool-size-one shard against itself.
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "with-shard").await;

    let (mut conn, shard) = conn_for_execution_forwarded_with_shard(&shards.pool, exec_id)
        .await
        .expect("resolve before any migration");
    assert_eq!(
        shard, SOURCE,
        "an un-migrated execution resolves to its origin"
    );
    assert_eq!(
        autumn_harvest::shard_rebalance::shard_of_held_row(&mut conn, exec_id).await,
        Some(SOURCE),
        "the returned connection must actually be checked out from `shard`"
    );
    drop(conn);

    migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("migrate");

    let (mut conn, shard) = conn_for_execution_forwarded_with_shard(&shards.pool, exec_id)
        .await
        .expect("resolve after migration");
    assert_eq!(
        shard, TARGET,
        "the returned shard must follow the run across a migration"
    );
    assert_eq!(
        autumn_harvest::shard_rebalance::shard_of_held_row(&mut conn, exec_id).await,
        Some(TARGET),
        "the returned connection must be checked out from the NEW shard, not the origin"
    );
}

// ── Codex round 4 ───────────────────────────────────────────────────────────

#[tokio::test]
async fn a_terminate_existing_start_refuses_a_rebalanced_prior_instead_of_replacing_it() {
    // `MIGRATED` counts as an active conflict because the run is still live,
    // just elsewhere. The terminate-and-replace branch cannot honour that from
    // here: its `inline_cancel` matches only RUNNING/PAUSED so it no-ops
    // against the seal, and `replace_execution` would then seal the row
    // CONTINUED_AS_NEW — which is excluded from the active-uniqueness index —
    // and insert a fresh run, releasing the business key while the real run
    // keeps executing on its target shard. Two live runs for one key.
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "terminate-me").await;

    migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("migrate");

    let mut source = shards.source().await;
    let err = autumn_harvest::execution::start_or_load_workflow_execution(
        &mut source,
        terminate_existing_start("entity_flow", "terminate-me"),
        None,
    )
    .await
    .expect_err("a start that would replace a rebalanced prior must be refused");
    assert!(
        matches!(err, HarvestError::ShardUnavailable { .. }),
        "expected a retryable ShardUnavailable naming the live residence, got {err:?}"
    );

    // The seal is untouched, so the business key is still held and the live
    // copy on the target is still the only run.
    assert_eq!(
        state_of(&mut source, exec_id).await.as_deref(),
        Some("MIGRATED")
    );
    assert_eq!(authoritative_shards(&shards, exec_id).await, vec![TARGET]);
}

// ── Issue #1317: a completed migrated run must release its business key ─────

#[tokio::test]
async fn a_seal_whose_live_copy_never_finished_is_not_reconciled() {
    // Guards the reconciler itself against the false-positive direction:
    // a still-live target must never be marked observed-terminal.
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "still-running").await;
    migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("migrate");

    let mut source = shards.source().await;
    let reconciled =
        reconcile_migrated_seal_terminality(&mut source, &shards.pool, exec_id, SOURCE)
            .await
            .expect("reconcile must not fail merely because the live copy is still running");
    assert!(
        !reconciled,
        "a live, non-terminal target must not be reconciled"
    );
}

#[tokio::test]
async fn a_hop_that_loops_back_to_the_held_shard_is_refused_not_deadlocked() {
    // Issue #1317 review: the committed window of a reverse migration
    // forwards through the shard reconciliation already holds. A -> B
    // (done) then B -> A (committed, not yet activated) leaves B's seal
    // forwarding to A. A's still-staged copy forwards back to B. On a
    // pool-size-one shard, re-checking out B here would deadlock against
    // the connection this call already holds. This test cannot reproduce
    // the deadlock itself without hanging the suite. It pins the fast,
    // named refusal instead of the slower generic MAX_FORWARD_HOPS cycle
    // message a multi-connection test pool would otherwise reach.
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "there-and-back-cycle").await;
    migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("A -> B");

    let (mut reverse_source, mut reverse_target) = (shards.target().await, shards.source().await);
    begin_migration(&mut reverse_source, exec_id, TARGET, SOURCE)
        .await
        .expect("begin B -> A");
    stage_copy(&mut reverse_source, &mut reverse_target, exec_id, SOURCE)
        .await
        .expect("stage onto A");
    verify_target_copy(&mut reverse_source, &mut reverse_target, exec_id, &codecs())
        .await
        .expect("verify the staged copy on A");
    commit_cutover(&mut reverse_source, exec_id, SOURCE)
        .await
        .expect("commit B -> A")
        .then_some(())
        .expect("the reverse cutover must commit against a quiescent source");

    // Deliberately no `activate_target`: A's staged copy still forwards to
    // B, the committed-window state the finding describes.
    let mut b = shards.target().await;
    let result = reconcile_migrated_seal_terminality(&mut b, &shards.pool, exec_id, TARGET).await;
    let err = result.expect_err("a hop back to the held shard must be refused, not resolved");
    let message = err.to_string();
    assert!(
        message.contains("loops back to shard") && message.contains(&TARGET.to_string()),
        "the refusal must name the held shard, got {message}"
    );
    assert!(
        !message.contains("exceeded"),
        "must fail on the first repeated hop, not after cycling through every hop, got {message}"
    );
}

#[tokio::test]
async fn a_sweep_reports_a_seal_whose_target_is_unreachable_as_a_failure_not_a_silent_skip() {
    // Issue #1317 review: `Err` (a database or unreachable-target problem)
    // used to be treated exactly like `Ok(false)` (not yet terminal) --
    // both a silent no-op. An operator following `next_scan_cursor` alone
    // would then never revisit this seal once the cursor moves past it.
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "unreachable-target").await;
    migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("migrate");

    // A pool that only knows about the source: the seal's forwarding
    // pointer names TARGET, which this pool has no entry for at all.
    let source_only_pool = ShardedDbPool::from_map(
        std::collections::BTreeMap::from([(SOURCE, build_pool(&shards.source_url))]),
        SOURCE,
    );

    let (reconciled, failures, _next_cursor) =
        reconcile_migrated_seals_after(&source_only_pool, SOURCE, 10, None)
            .await
            .expect("the candidate scan itself must still succeed");
    assert_eq!(
        reconciled, 0,
        "the unreachable seal must not count as reconciled"
    );
    assert_eq!(
        failures.len(),
        1,
        "the failure must be reported, not dropped"
    );
    assert_eq!(failures[0].execution_id, exec_id);
}

#[tokio::test]
async fn a_terminate_if_running_start_creates_a_fresh_run_once_the_migrated_prior_finishes() {
    // Before issue #1317's fix, `MIGRATED` was an active conflict FOREVER.
    // Nothing ever noticed the live copy had finished. So this same start
    // request kept hitting the terminate-branch's `ShardUnavailable` refusal
    // for as long as the source shard existed. This is the plainest form of
    // the bug: an ordinary "run it again" the day after it finished.
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "run-me-again").await;
    migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("migrate");

    // The live copy finishes on the target.
    let mut target = shards.target().await;
    diesel::sql_query(
        "UPDATE harvest_workflow_executions SET state = 'COMPLETED', completed_at = now() \
          WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut target)
    .await
    .expect("complete the migrated run");

    // RED: before reconciliation runs, the seal still looks permanently
    // active, so the start is still refused exactly as it was pre-fix.
    let mut source = shards.source().await;
    let still_refused = autumn_harvest::execution::start_or_load_workflow_execution(
        &mut source,
        terminate_if_running_start("entity_flow", "run-me-again"),
        None,
    )
    .await;
    assert!(
        matches!(still_refused, Err(HarvestError::ShardUnavailable { .. })),
        "precondition: an un-reconciled seal must still behave exactly as before, got \
         {still_refused:?}"
    );

    let reconciled =
        reconcile_migrated_seal_terminality(&mut source, &shards.pool, exec_id, SOURCE)
            .await
            .expect("reconcile");
    assert!(reconciled, "the finished target must be observed terminal");

    // GREEN: the same request now creates a fresh run instead of attaching
    // to, or being refused by, the dead seal.
    let started = autumn_harvest::execution::start_or_load_workflow_execution(
        &mut source,
        terminate_if_running_start("entity_flow", "run-me-again"),
        None,
    )
    .await
    .expect("a start over an observed-terminal seal must succeed");
    assert!(
        started.created,
        "must be a fresh run, not an attach to the old seal"
    );
    assert_ne!(
        started.exec_id, exec_id,
        "the fresh run must be a distinct execution from the migrated one"
    );
    assert_eq!(started.state, "RUNNING");

    // The seal itself is untouched: `replace_execution` must never overwrite
    // a MIGRATED row's state, or the forwarding pointer loses retention's
    // and erasure's protection.
    assert_eq!(
        state_of(&mut source, exec_id).await.as_deref(),
        Some("MIGRATED"),
        "the seal's state must stay MIGRATED, never CONTINUED_AS_NEW"
    );
    assert_eq!(
        forward_of(&mut source, exec_id).await,
        Some(TARGET.as_i32())
    );

    // A second reconcile is a no-op, and a second start of the same key
    // reaches the now-active fresh run, not a duplicate-insert error.
    let reconciled_again =
        reconcile_migrated_seal_terminality(&mut source, &shards.pool, exec_id, SOURCE)
            .await
            .expect("reconcile");
    assert!(
        !reconciled_again,
        "reconciling an already-marked seal is a no-op"
    );

    // The released seal and the fresh active run both match this load's
    // `state NOT IN (CONTINUED_AS_NEW, TERMINATED)` filter (issue #1317
    // review). A third start must reach the ACTIVE run and terminate-replace
    // it. It must not mistake the released seal for "no conflict" and
    // attempt a second insert that collides with the real active-row
    // constraint.
    let started_again = autumn_harvest::execution::start_or_load_workflow_execution(
        &mut source,
        terminate_if_running_start("entity_flow", "run-me-again"),
        None,
    )
    .await
    .expect("a third start must reach the active fresh run, not the released seal");
    assert!(
        started_again.created,
        "TerminateIfRunning replaces the active run with a fresh one"
    );
    assert_ne!(
        started_again.exec_id, started.exec_id,
        "the replacement must be a new execution, not the fresh run reused"
    );
    assert_ne!(
        started_again.exec_id, exec_id,
        "the replacement must not be the migrated seal itself"
    );
}

fn terminate_if_running_start<'a>(
    workflow_name: &'a str,
    workflow_id: &'a str,
) -> autumn_harvest::execution::StartWorkflowParams<'a> {
    autumn_harvest::execution::StartWorkflowParams {
        reuse_policy: autumn_harvest::types::WorkflowIdReusePolicy::TerminateIfRunning,
        conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
        ..terminate_existing_start(workflow_name, workflow_id)
    }
}

#[tokio::test]
async fn an_allow_duplicate_start_creates_a_fresh_run_too_once_the_seal_is_reconciled() {
    // `AllowDuplicate` attaches to any VISIBLE non-sealed prior, terminal or
    // not -- it never replaces one. Once a seal is observed-terminal it is
    // released from the active-uniqueness slot the SAME way a `sealed`
    // (`CONTINUED_AS_NEW`/`TERMINATED`) prior already is. Every reuse policy
    // therefore sees "no prior occupies this key" uniformly, the same as
    // they already do for a sealed row. `AllowDuplicate` therefore also
    // gets a fresh run here, not the stale seal's un-refreshed data.
    // Attaching to it would hand back a row whose `output`/`completed_at`
    // were never populated (the real result lives on the target). So a
    // fresh run is the more useful outcome, not merely an accepted side
    // effect.
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "allow-duplicate-me").await;
    migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("migrate");

    let mut target = shards.target().await;
    diesel::sql_query(
        "UPDATE harvest_workflow_executions SET state = 'COMPLETED', completed_at = now() \
          WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut target)
    .await
    .expect("complete the migrated run");

    let mut source = shards.source().await;
    reconcile_migrated_seal_terminality(&mut source, &shards.pool, exec_id, SOURCE)
        .await
        .expect("reconcile")
        .then_some(())
        .expect("the finished target must be observed terminal");

    let started = autumn_harvest::execution::start_or_load_workflow_execution(
        &mut source,
        terminate_existing_start("entity_flow", "allow-duplicate-me"),
        None,
    )
    .await
    .expect("AllowDuplicate over an observed-terminal seal must succeed");
    assert!(started.created);
    assert_ne!(started.exec_id, exec_id);
    assert_eq!(
        state_of(&mut source, exec_id).await.as_deref(),
        Some("MIGRATED"),
        "the seal's state must stay MIGRATED"
    );
}

#[tokio::test]
async fn rolling_back_the_seal_column_refuses_once_a_key_has_both_a_seal_and_a_replacement() {
    // Issue #1317 review: once reconciliation releases a seal and a fresh
    // same-key run is admitted, both rows satisfy the pre-fix migration's
    // narrower `state NOT IN (...)` predicate. Recreating that unique index
    // then fails on the duplicate pair. The down migration must refuse with
    // a clear message instead of surfacing a raw constraint violation.
    let shards = setup_two_shards().await;
    let mut source = shards.source().await;
    let down_sql =
        include_str!("../../migrations/20260915231809_harvest_migrated_seal_terminal_at/down.sql");

    // A reconciled seal and its live replacement, same business key.
    source
        .batch_execute(
            "INSERT INTO harvest_workflow_executions \
               (id, workflow_name, workflow_id, run_id, shard_id, state, input, \
                started_at, created_at, migrated_run_terminal_at, migrated_to_shard, migrated_at) \
             VALUES \
               (gen_random_uuid(), 'wf', 'down-migration-dup', gen_random_uuid(), 0, \
                'MIGRATED', '{}', now(), now(), now(), 1, now()), \
               (gen_random_uuid(), 'wf', 'down-migration-dup', gen_random_uuid(), 0, \
                'RUNNING', '{}', now(), now(), NULL, NULL, NULL)",
        )
        .await
        .expect("seed the duplicate-key scenario");

    let err = Box::pin(
        source.transaction::<(), diesel::result::Error, _>(async |conn| {
            conn.batch_execute(down_sql).await
        }),
    )
    .await
    .expect_err("the guard must refuse before the unique index rebuild can fail raw");
    assert!(
        err.to_string().contains("cannot roll back"),
        "expected the guard's own message, got {err}"
    );

    // The refusal must not leave the column or the widened index touched.
    let column_still_present: ScalarCount = diesel::sql_query(
        "SELECT count(*)::BIGINT AS value FROM information_schema.columns \
          WHERE table_name = 'harvest_workflow_executions' \
            AND column_name = 'migrated_run_terminal_at'",
    )
    .get_result(&mut source)
    .await
    .expect("check column presence");
    assert_eq!(
        column_still_present.value, 1,
        "an aborted rollback must leave the column in place"
    );
}

fn signal_with_start_allow_duplicate<'a>(
    workflow_name: &'a str,
    workflow_id: &'a str,
    signal_name: &'a str,
) -> autumn_harvest::execution::SignalWithStartParams<'a> {
    autumn_harvest::execution::SignalWithStartParams {
        workflow_name,
        workflow_id,
        exec_id: ExecutionId::new_for_shard(SOURCE),
        input: json!({"seed": 2}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: autumn_harvest::types::WorkflowIdReusePolicy::AllowDuplicate,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        concurrency_key: None,
        concurrency_limit: None,
        concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
        signal_name,
        signal_payload: json!({"woke": true}),
        idempotency_key: None,
        max_workflow_input_bytes: 0,
        max_signal_payload_bytes: 0,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
        sla: None,
        workflow_retry_policy: None,
        max_workflow_attempts_ceiling: None,
        reject_fresh_if_debounced: false,
        workflow_info: None,
        start_source_override: None,
        start_source_ref_override: None,
    }
}

#[tokio::test]
async fn a_signal_with_start_attaches_to_the_active_run_not_the_released_seal() {
    // Issue #1317 review: `resolve_effective_signal_with_start_policy`'s own
    // active-key lookup shares the released-seal-vs-live-row ambiguity
    // `load_workflow_execution_by_key_for_update` guards against. If it
    // locks the seal instead of the live replacement, it wrongly reads a
    // non-RUNNING prior. It then upgrades `AllowDuplicate` to
    // `TerminateIfRunning`, and replaces the active run instead of
    // attaching to it.
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "swap-signal").await;
    migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("migrate");

    let mut target = shards.target().await;
    diesel::sql_query(
        "UPDATE harvest_workflow_executions SET state = 'COMPLETED', completed_at = now() \
          WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut target)
    .await
    .expect("complete the migrated run");

    let mut source = shards.source().await;
    reconcile_migrated_seal_terminality(&mut source, &shards.pool, exec_id, SOURCE)
        .await
        .expect("reconcile");

    let first = autumn_harvest::execution::signal_with_start_workflow_execution(
        &mut source,
        signal_with_start_allow_duplicate("entity_flow", "swap-signal", "wake"),
    )
    .await
    .expect("first signal-with-start creates a fresh run over the reconciled seal");
    assert!(first.started_fresh, "no live prior, so this must be fresh");

    let second = autumn_harvest::execution::signal_with_start_workflow_execution(
        &mut source,
        signal_with_start_allow_duplicate("entity_flow", "swap-signal", "wake-again"),
    )
    .await
    .expect("second signal-with-start must attach to the now-active fresh run");
    assert!(
        !second.started_fresh,
        "must attach to the live run, not mistake the released seal for no prior"
    );
    assert_eq!(
        second.exec_id, first.exec_id,
        "must be the SAME execution as the first call created"
    );
}

#[tokio::test]
async fn a_reconciled_seal_alone_does_not_bypass_the_throttle_token() {
    // Issue #1317 review: `resolve_bypass` reads a `Some` return from
    // `try_load_by_key` as "an active execution already satisfies the
    // reuse policy", skipping the throttle token reservation. With only a
    // reconciled seal present, that used to still return `Some`, so a
    // same-key restart bypassed configured throttle pacing entirely.
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "throttle-over-seal").await;
    migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("migrate");

    let mut target = shards.target().await;
    diesel::sql_query(
        "UPDATE harvest_workflow_executions SET state = 'COMPLETED', completed_at = now() \
          WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut target)
    .await
    .expect("complete the migrated run");

    let mut source = shards.source().await;
    reconcile_migrated_seal_terminality(&mut source, &shards.pool, exec_id, SOURCE)
        .await
        .expect("reconcile")
        .then_some(())
        .expect("the finished target must be observed terminal");

    let bypass = autumn_harvest::throttle::resolve_bypass(
        &mut source,
        "entity_flow",
        "throttle-over-seal",
        autumn_harvest::types::WorkflowIdReusePolicy::AllowDuplicate,
    )
    .await
    .expect("resolve_bypass must not fail");
    assert!(
        !bypass,
        "a released seal must not read as a live prior satisfying the reuse policy"
    );
}

fn allow_duplicate_default_conflict_start<'a>(
    workflow_name: &'a str,
    workflow_id: &'a str,
) -> autumn_harvest::execution::StartWorkflowParams<'a> {
    autumn_harvest::execution::StartWorkflowParams {
        reuse_policy: autumn_harvest::types::WorkflowIdReusePolicy::AllowDuplicate,
        conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
        ..terminate_existing_start(workflow_name, workflow_id)
    }
}

// Serialises the tests below that mutate the process-global admission gate
// cache, mirroring the same-purpose lock in `admission_gate_authoritative_tests.rs`.
static TEST_SERIAL: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

fn fleet_gate_cache(
    reason: &str,
) -> std::sync::Arc<autumn_harvest::admission_gate::AdmissionGateCache> {
    let cache = std::sync::Arc::new(autumn_harvest::admission_gate::AdmissionGateCache::new());
    cache.refresh(vec![autumn_harvest::admission_gate::AdmissionGate {
        id: autumn_harvest::admission_gate::AdmissionGateId(Uuid::new_v4()),
        scope: autumn_harvest::admission_gate::GateScope::Fleet,
        reason: reason.to_string(),
        message: None,
        created_by: "test".to_string(),
        created_at: Utc::now(),
        expires_at: None,
    }]);
    cache
}

#[tokio::test]
async fn an_armed_gate_still_applies_over_a_sole_reconciled_seal() {
    // Issue #1317 review: `try_load_active_execution_for_update` fed the
    // admission gate's create-vs-attach mirror
    // (`start_will_create_new_execution`) a `Some(prior)` for a sole
    // observed-terminal seal. `AllowDuplicate`'s native active behavior is
    // Attach, so the mirror reported "no create" and the gate check was
    // skipped. That happened even though the INSERT actually succeeds,
    // since the widened active-uniqueness index already excludes an
    // observed-terminal seal. An armed gate must still see this as a
    // fresh create and block it.
    let _serial = TEST_SERIAL.lock().await;
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "gate-over-reconciled-seal").await;
    migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("migrate");

    let mut target = shards.target().await;
    diesel::sql_query(
        "UPDATE harvest_workflow_executions SET state = 'COMPLETED', completed_at = now() \
          WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut target)
    .await
    .expect("complete the migrated run");

    let mut source = shards.source().await;
    reconcile_migrated_seal_terminality(&mut source, &shards.pool, exec_id, SOURCE)
        .await
        .expect("reconcile")
        .then_some(())
        .expect("the finished target must be observed terminal");

    autumn_harvest::admission_gate::set_global_admission_gate_cache(Some(fleet_gate_cache(
        "gate-over-reconciled-seal-incident",
    )));

    let blocked = autumn_harvest::execution::start_or_load_workflow_execution(
        &mut source,
        allow_duplicate_default_conflict_start("entity_flow", "gate-over-reconciled-seal"),
        Some(autumn_harvest::admission_gate::GateMode::Check),
    )
    .await;

    autumn_harvest::admission_gate::set_global_admission_gate_cache(None);

    assert!(
        matches!(blocked, Err(HarvestError::AdmissionBlocked { .. })),
        "a fresh create over a released seal must still face an armed gate, got {blocked:?}"
    );
}

#[tokio::test]
async fn a_business_key_target_resolves_through_the_seal_to_the_live_copy() {
    // A `WorkflowId` target hashes to a fixed shard, and that shard is exactly
    // where the seal sits — the migration deliberately keeps `MIGRATED` inside
    // the active-uniqueness index, so the business key never moves. Routing by
    // the hash alone therefore delivers to the seal: the cancel outbox reads it
    // as terminal and reports success for a workflow that keeps running.
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "by-key").await;

    migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("migrate");

    let by_key = autumn_harvest::types::ExternalTarget::WorkflowId {
        workflow_name: "entity_flow".to_string(),
        workflow_id: "by-key".to_string(),
    };
    let routed =
        autumn_harvest::shard_rebalance::resolve_target_shard(&shards.pool, &by_key, SOURCE).await;
    assert_eq!(
        routed, TARGET,
        "a business-key target must resolve through the seal to the live residence"
    );

    // And an id target still resolves the same way, so the two agree.
    let by_id = autumn_harvest::types::ExternalTarget::ExecutionId(exec_id);
    assert_eq!(
        autumn_harvest::shard_rebalance::resolve_target_shard(&shards.pool, &by_id, SOURCE).await,
        TARGET
    );
}

#[tokio::test]
async fn a_reverse_migration_keeps_the_live_shard_last_in_the_residence_chain() {
    // A → B → A is supported precisely so a drain can be undone. The stored
    // history is then [A, B] and the run is live on A, so the raw chain is
    // [A, B, A]. A first-occurrence dedup collapses that to [A, B] and leaves
    // the SEALED copy in the final position — which every consumer reads as the
    // live residence, so the erase would gate on the wrong copy.
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "there-and-back").await;

    migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("A -> B");
    migrate_execution(&shards.pool, exec_id, TARGET, SOURCE, &codecs())
        .await
        .expect("B -> A");

    let chain = autumn_harvest::shard_rebalance::residence_chain(&shards.pool, exec_id)
        .await
        .expect("residence chain");
    assert_eq!(
        chain,
        vec![TARGET, SOURCE],
        "the live shard must be last, and each residence must appear once"
    );
    assert_eq!(
        chain.last().copied(),
        Some(SOURCE),
        "the run is live on SOURCE again, so SOURCE must be the final element"
    );
}

/// A `TerminateExisting` start for `(workflow_name, workflow_id)` — the policy
/// that, before the round-4 fix, would seal a rebalanced prior
/// `CONTINUED_AS_NEW` and start a second live run for the same business key.
fn terminate_existing_start<'a>(
    workflow_name: &'a str,
    workflow_id: &'a str,
) -> autumn_harvest::execution::StartWorkflowParams<'a> {
    autumn_harvest::execution::StartWorkflowParams {
        workflow_name,
        workflow_id,
        exec_id: ExecutionId::new_for_shard(SOURCE),
        input: json!({"seed": 2}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: autumn_harvest::types::WorkflowIdReusePolicy::AllowDuplicate,
        conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::TerminateExisting,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        inherited_chain_deadline_at: None,
        concurrency_key: None,
        concurrency_limit: None,
        concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
        priority: autumn_harvest::types::Priority::default(),
        max_workflow_input_bytes: 0,
        start_at: None,
        delay: None,
        max_workflow_start_delay: None,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
        sla: None,
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        max_workflow_attempts_ceiling: None,
        origin: None,
        completion_callbacks: None,
        start_source: autumn_harvest::StartSource::Api,
        start_source_ref: None,
        started_by: None,
    }
}

// ── Codex round 5 ───────────────────────────────────────────────────────────

#[tokio::test]
async fn a_reverse_migration_keeps_the_origin_seal_resolvable_throughout() {
    // A → B → A stages onto a shard this run has lived on before, so the row
    // staging replaces is A's own forwarding seal. Losing it during staging
    // strands every id that routes to A at an inert copy; losing it on abort
    // leaves the execution with no row on its origin shard at all — an id that
    // resolves nowhere, the one outcome this design must never produce.
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "there-and-back-seal").await;

    migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("A -> B");

    // Stage the reverse migration but stop before the cutover.
    let (mut source, mut target) = (shards.target().await, shards.source().await);
    begin_migration(&mut source, exec_id, TARGET, SOURCE)
        .await
        .expect("begin B -> A");
    stage_copy(&mut source, &mut target, exec_id, SOURCE)
        .await
        .expect("stage onto A");

    // Mid-staging, ids routing to A must still reach the live copy on B.
    let mut a = shards.source().await;
    assert_eq!(
        forward_of(&mut a, exec_id).await,
        Some(TARGET.as_i32()),
        "the staged copy must carry A's seal so ids keep resolving during staging"
    );
    assert_eq!(
        resolve_execution_shard(&shards.pool, exec_id)
            .await
            .expect("resolve"),
        TARGET,
        "the run is still live on B until the reverse cutover"
    );
    assert_eq!(authoritative_shards(&shards, exec_id).await, vec![TARGET]);

    // Abort before the cutover: A's seal must come back, not vanish.
    abort_migration(
        &mut source,
        &mut target,
        exec_id,
        "operator changed their mind",
    )
    .await
    .expect("abort");

    let mut a = shards.source().await;
    assert_eq!(
        state_of(&mut a, exec_id).await.as_deref(),
        Some("MIGRATED"),
        "an aborted reverse migration must restore the origin seal, not delete it"
    );
    assert_eq!(forward_of(&mut a, exec_id).await, Some(TARGET.as_i32()));
    assert_eq!(
        resolve_execution_shard(&shards.pool, exec_id)
            .await
            .expect("resolve after abort"),
        TARGET
    );
    assert_eq!(authoritative_shards(&shards, exec_id).await, vec![TARGET]);
}

#[tokio::test]
async fn a_completed_reverse_migration_clears_the_carried_pointer() {
    // The staged copy deliberately carries the target's old seal so ids resolve
    // during staging. Activation must clear it: a live row still pointing at
    // the shard it came from is a forwarding cycle, and `read_forward` matches
    // on the pointer rather than the state, so being RUNNING would not save it.
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "there-and-back-clear").await;

    migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("A -> B");
    migrate_execution(&shards.pool, exec_id, TARGET, SOURCE, &codecs())
        .await
        .expect("B -> A");

    let mut a = shards.source().await;
    assert_eq!(state_of(&mut a, exec_id).await.as_deref(), Some("RUNNING"));
    assert_eq!(
        forward_of(&mut a, exec_id).await,
        None,
        "the live row must not forward to the shard it came from"
    );
    assert_eq!(
        resolve_execution_shard(&shards.pool, exec_id)
            .await
            .expect("resolve"),
        SOURCE
    );
    assert_eq!(authoritative_shards(&shards, exec_id).await, vec![SOURCE]);
}

#[tokio::test]
async fn a_schedule_attributed_execution_is_refused_by_the_sql_predicate_too() {
    // The pure predicate names the blocker; the cutover's SQL must agree, or an
    // execution the candidate scan skips could still be cut over by a resume.
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "scheduled-run").await;

    let mut source = shards.source().await;
    diesel::sql_query(
        "UPDATE harvest_workflow_executions SET schedule_id = gen_random_uuid() WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut source)
    .await
    .expect("attribute the run to a schedule");

    let verdict = assess_quiescence(&observe_quiescence(&mut source, exec_id).await.expect("obs"));
    assert!(!verdict.is_eligible());
    assert!(
        verdict
            .blockers()
            .contains(&QuiescenceBlocker::ScheduleAttributed)
    );

    // And a real migration refuses it rather than moving it away from the
    // schedule whose overlap enforcement is shard-local.
    let outcome = migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("migrate must not error, only decline");
    assert!(
        matches!(outcome, MigrationOutcome::Skipped { .. }),
        "expected a skip with named blockers, got {outcome:?}"
    );
    assert_eq!(authoritative_shards(&shards, exec_id).await, vec![SOURCE]);
}

#[tokio::test]
async fn cancelling_a_sealed_source_is_left_pending_not_reported_delivered() {
    // The cancel outbox maps a terminal-state error to `ExternalCancelDelivered`
    // and never retries. A rebalanced seal is not terminal — the run is alive
    // elsewhere — so answering "already terminal" there would record a delivered
    // cancellation for a workflow that goes on running. It must be retryable.
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "cancel-the-seal").await;

    migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("migrate");

    let mut source = shards.source().await;
    let err = autumn_harvest::execution::cancel_workflow_execution_collect(
        &mut source,
        exec_id,
        "external cancel",
        None,
    )
    .await
    .expect_err("cancelling a forwarding seal must not succeed");
    assert!(
        matches!(err, HarvestError::ShardUnavailable { .. }),
        "expected a retryable ShardUnavailable so the delivery stays pending, got {err:?}"
    );

    // The live copy is untouched and still running.
    let mut target = shards.target().await;
    assert_eq!(
        state_of(&mut target, exec_id).await.as_deref(),
        Some("RUNNING")
    );
}

/// A single-pool deployment must still reach its own executions, whatever the
/// shard bits in their ids say.
///
/// `ShardedDbPool::single` -- the shape of every pre-sharding deployment, and of
/// the plugin's own test harness -- registers exactly one pool, at `ShardId(0)`.
/// An `ExecutionId` minted while a different shard was configured (or by any
/// caller that encodes one) then names a shard for which `exact_pool_for`
/// returns `None`. Resolving the run's own residence through the exact lookup
/// therefore answered `ShardUnavailable` for a row sitting in the only database
/// there is, and the erase path surfaced that as a 503 -- including for an
/// execution under a legal hold, which must answer 409 and never a transport
/// error that invites a retry.
///
/// The live residence resolves through `pool_for`'s default fallback instead,
/// which is how every other read path in the engine reaches a run's database.
/// Safe because the lookup is keyed by execution id: a fallback landing on the
/// wrong database finds no row, never another run's data. Prior residences keep
/// the exact form and are covered by the retired/unreachable tests above.
#[tokio::test]
async fn single_pool_resolves_an_execution_whose_id_encodes_another_shard() {
    let (url, _guard) = setup_isolated_db().await;
    let pool = ShardedDbPool::single(build_pool(&url));

    // The only pool is at ShardId(0); this id says it belongs to shard 7.
    let foreign = ShardId::new(7);
    let exec_id = {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
            .await
            .expect("connect");
        insert_execution_with_id(
            &mut conn,
            "entity",
            "single-pool-foreign-id",
            ExecutionId::new_for_shard(foreign),
            SOURCE,
        )
        .await
    };

    let live = resolve_execution_shard(&pool, exec_id)
        .await
        .expect("a single-pool deployment must resolve its own execution");
    assert_eq!(
        live, foreign,
        "the id still names shard 7; it is the POOL lookup that falls back, \
         so the resolved residence is reported as the id's own shard"
    );

    let chain = residence_chain(&pool, exec_id)
        .await
        .expect("residence chain must not report the only database unavailable");
    assert_eq!(
        chain,
        vec![foreign],
        "a run that never migrated has a one-shard residence chain"
    );
}

#[tokio::test]
async fn conn_for_execution_forwarded_with_shard_reports_the_actual_fallback_shard() {
    // Issue #1317 review: `pool_for` silently substitutes the default
    // shard's pool when the id's encoded origin has no configured pool.
    // That gap can be a mid-rollout config, or -- as here -- a genuinely
    // single-pool deployment. The returned shard must name the pool the
    // connection actually comes from. A caller attributing this
    // connection to a shard (an audit log, say) would otherwise mislabel
    // every row it writes during the fallback.
    let (url, _guard) = setup_isolated_db().await;
    let pool = ShardedDbPool::single(build_pool(&url));

    // The only pool is at ShardId(0); this id says it belongs to shard 7.
    let foreign = ShardId::new(7);
    let exec_id = {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
            .await
            .expect("connect");
        insert_execution_with_id(
            &mut conn,
            "entity",
            "with-shard-foreign-id",
            ExecutionId::new_for_shard(foreign),
            SOURCE,
        )
        .await
    };

    let (mut conn, shard) = conn_for_execution_forwarded_with_shard(&pool, exec_id)
        .await
        .expect("the single configured pool must answer");
    assert_eq!(
        shard,
        pool.default_shard(),
        "the connection actually came from the default shard's pool, not \
         the id's unconfigured origin"
    );
    assert_eq!(
        autumn_harvest::shard_rebalance::shard_of_held_row(&mut conn, exec_id).await,
        Some(pool.default_shard()),
        "the returned connection must be checked out from the shard just reported"
    );
}

/// A declared retired-shard forward names one specific database, so a missing
/// successor pool must fail closed rather than fall back to the default.
///
/// `routed_shard_for_execution` applies an operator-declared `A -> B` forward
/// and returns **B**. The single-pool fallback that lets an ordinary id reach
/// its own row (see `checkout_entry`) must not apply to that B: the operator
/// has asserted A is gone and its ids now live on B, so answering from the
/// default shard reads an unrelated database and returns a confident "not
/// found" for every A-origin execution -- silently, which is the part that
/// makes it worse than an error.
///
/// Pins the distinction the fallback turns on: tolerate a missing pool for an
/// id resolving to its OWN encoded shard, never for one routing has forwarded.
#[tokio::test]
async fn a_declared_retired_forward_requires_its_successor_pool() {
    let (url, _guard) = setup_isolated_db().await;
    // One pool, at ShardId(0). Shard 9 -- the declared successor below -- has
    // no pool on this node.
    let pool = ShardedDbPool::single(build_pool(&url));

    let retired = ShardId::new(5);
    let successor = ShardId::new(9);
    install_global_router(
        ShardRouter::new(
            vec![ShardId::new(0), successor],
            vec![ShardId::new(0)],
            ShardId::new(0),
        )
        .with_shard_forwards([(retired, successor)]),
    );

    let err = resolve_execution_shard(&pool, ExecutionId::new_for_shard(retired))
        .await
        .expect_err("a forwarded id whose successor pool is absent must not resolve");

    // Restore before asserting, so a failure here cannot leak the forward into
    // whatever test runs next in this binary.
    install_global_router(ShardRouter::single());

    match err {
        HarvestError::ShardUnavailable { shard_id, .. } => assert_eq!(
            shard_id,
            successor.as_i32(),
            "the unavailable shard must be reported as the SUCCESSOR the operator declared, \
             not the retired origin -- that is the database an operator has to go install"
        ),
        other => panic!("expected ShardUnavailable for an absent successor pool, got {other:?}"),
    }
}

// ── Issue #1317: seal-predicate and abort-restore hardening ─────────────────
//
// Issue #1317 found that `existing_seal` (read before a reverse-migration
// restage) and the abort-restore fallback both key off `state` rather than
// the forwarding pointer, unlike `read_forward`. Both windows can destroy the
// one seal every A-origin id resolves through.

#[tokio::test]
async fn a_repeated_stage_after_an_interrupted_resume_keeps_the_carried_seal() {
    // A -> B, then B -> A begins and stages successfully. A is now MIGRATING.
    // It carries A's own prior seal, pointing at B, so ids keep resolving
    // during staging. Model a crash between that target commit and the
    // source-side phase advance. Reset the record back to PENDING. A resume
    // sweep observes exactly this state and re-drives it with a second
    // `stage_copy` call.
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "resume-carries-seal").await;

    migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("A -> B");

    let (mut source, mut target) = (shards.target().await, shards.source().await);
    begin_migration(&mut source, exec_id, TARGET, SOURCE)
        .await
        .expect("begin B -> A");
    stage_copy(&mut source, &mut target, exec_id, SOURCE)
        .await
        .expect("first stage onto A");

    diesel::sql_query(
        "UPDATE harvest_shard_migrations SET phase = 'PENDING' WHERE execution_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut source)
    .await
    .expect("simulate a crash before the phase advanced past PENDING");

    stage_copy(&mut source, &mut target, exec_id, SOURCE)
        .await
        .expect("resume re-stages the still-PENDING record");

    let mut a = shards.source().await;
    assert_eq!(
        forward_of(&mut a, exec_id).await,
        Some(TARGET.as_i32()),
        "re-staging a PENDING record must not drop A's own carried seal"
    );
    assert_eq!(
        resolve_execution_shard(&shards.pool, exec_id)
            .await
            .expect("resolve"),
        TARGET,
        "the run is still live on B while the reverse migration is only staged"
    );
}

#[tokio::test]
async fn aborting_before_staging_ever_touched_the_target_leaves_its_seal_untouched() {
    // A -> B seals A. A B -> A reverse migration is opened. `stage_copy` never
    // ran against A, the equivalent of it failing before its target
    // transaction committed. A's row is exactly the untouched original seal.
    // Abort must recognize "nothing to discard" and leave it alone, rather
    // than falling through to a DELETE that matches on `state` alone.
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "abort-never-staged").await;

    migrate_execution(&shards.pool, exec_id, SOURCE, TARGET, &codecs())
        .await
        .expect("A -> B");

    let (mut source, mut target) = (shards.target().await, shards.source().await);
    begin_migration(&mut source, exec_id, TARGET, SOURCE)
        .await
        .expect("begin B -> A");

    abort_migration(&mut source, &mut target, exec_id, "never staged")
        .await
        .expect("abort a PENDING migration whose target was never touched");

    let mut a = shards.source().await;
    assert_eq!(
        state_of(&mut a, exec_id).await.as_deref(),
        Some("MIGRATED"),
        "A's pre-existing seal must survive an abort that never staged over it"
    );
    assert_eq!(
        forward_of(&mut a, exec_id).await,
        Some(TARGET.as_i32()),
        "the untouched seal must keep its pointer, not be deleted"
    );
    assert_eq!(
        resolve_execution_shard(&shards.pool, exec_id)
            .await
            .expect("resolve after abort"),
        TARGET
    );
    assert!(
        count(
            &mut a,
            "SELECT count(*)::BIGINT AS value FROM harvest_events WHERE workflow_exec_id = $1",
            exec_id
        )
        .await
            > 0,
        "A's own pre-migration history must survive an abort that never staged over it"
    );
}

#[tokio::test]
async fn a_repeated_abort_finishes_a_target_cleanup_a_prior_attempt_did_not() {
    // Issue #1317: `abort_migration` claims the abort (phase -> ABORTED, on
    // the source) BEFORE cleaning up the target's staged copy. Those are
    // two separate commits against two separate databases. A target-cleanup
    // failure -- a dropped connection, say -- after the claim already
    // committed used to strand the record. `resume_incomplete_migrations`
    // excludes ABORTED, and a repeated `abort_migration` call refused
    // outright because the claim UPDATE could no longer match a
    // non-ABORTED phase. Neither path could ever finish the cleanup.
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "abort-retry-me").await;

    begin_migration(&mut shards.source().await, exec_id, SOURCE, TARGET)
        .await
        .expect("begin");
    stage_copy(
        &mut shards.source().await,
        &mut shards.target().await,
        exec_id,
        TARGET,
    )
    .await
    .expect("stage");

    // Simulate a claim that committed followed by a cleanup that never ran:
    // run exactly the claim UPDATE `abort_migration` itself runs, without
    // calling `abort_migration` at all.
    let mut source = shards.source().await;
    let claimed = diesel::sql_query(
        "UPDATE harvest_shard_migrations \
            SET phase = 'ABORTED', abort_reason = $2, staged_task = NULL, updated_at = NOW() \
          WHERE execution_id = $1 AND phase IN ('PENDING', 'COPIED', 'VERIFIED')",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>("simulated dropped connection")
    .execute(&mut source)
    .await
    .expect("simulated claim");
    assert_eq!(claimed, 1, "precondition: the claim itself must succeed");

    // Precondition: the target's staged copy is still fully intact -- the
    // cleanup this record's phase claims already happened never actually ran.
    let mut target = shards.target().await;
    assert!(
        count(
            &mut target,
            "SELECT count(*)::BIGINT AS value FROM harvest_events WHERE workflow_exec_id = $1",
            exec_id
        )
        .await
            > 0,
        "precondition: the staged copy must still be sitting on the target"
    );

    // A later call -- an operator re-running the same command, or an
    // automated retry -- must finish the cleanup instead of refusing.
    abort_migration(&mut source, &mut target, exec_id, "operator retry")
        .await
        .expect("a repeated abort must retry the target cleanup, not refuse");

    assert_eq!(
        count(
            &mut target,
            "SELECT count(*)::BIGINT AS value FROM harvest_events WHERE workflow_exec_id = $1",
            exec_id
        )
        .await,
        0,
        "the target's staged copy must be cleaned up by the retry"
    );
    assert_eq!(
        count(
            &mut target,
            "SELECT count(*)::BIGINT AS value FROM harvest_workflow_executions WHERE id = $1",
            exec_id
        )
        .await,
        0,
        "a forward migration's staged row has no seal to restore, so it must be gone"
    );
}

// ── Issue #1317: a hold placed during staging must not be cut over past ─────

#[tokio::test]
async fn a_hold_placed_after_verification_refuses_the_cutover() {
    // Issue #1317: `stage_copy` snapshots the row with no lock, so a hold
    // placed afterwards lands only on the source. The cutover must not seal a
    // source whose hold state has moved since the copy it is about to
    // authorize was verified.
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "hold-during-staging").await;
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

    autumn_harvest::set_legal_hold(
        &mut source,
        exec_id,
        "litigation hold",
        None,
        "compliance-bot",
        Utc::now(),
    )
    .await
    .expect("place a hold after the copy was verified");

    let cut_over = commit_cutover(&mut source, exec_id, TARGET)
        .await
        .expect("cutover call must not error");
    assert!(
        !cut_over,
        "a hold placed after verification must refuse the cutover, not seal past it"
    );
    assert_eq!(
        state_of(&mut source, exec_id).await.as_deref(),
        Some("RUNNING"),
        "a refused cutover must leave the source exactly as it was"
    );

    // The runbook answer is what a declined cutover always requires: abort and
    // restart the migration. A fresh `stage_copy` snapshots the row with the
    // hold already on it, so the second attempt verifies and cuts over clean.
    abort_migration(&mut source, &mut target, exec_id, "hold placed mid-staging")
        .await
        .expect("abort the stale attempt");
    begin_migration(&mut source, exec_id, SOURCE, TARGET)
        .await
        .expect("begin again");
    stage_copy(&mut source, &mut target, exec_id, TARGET)
        .await
        .expect("restage with the hold already in place");
    verify_target_copy(&mut source, &mut target, exec_id, &codecs())
        .await
        .expect("verify");
    assert!(
        commit_cutover(&mut source, exec_id, TARGET)
            .await
            .expect("cutover"),
        "a cutover verified against the current hold state must succeed"
    );
}

#[tokio::test]
async fn a_hold_released_after_verification_also_refuses_the_cutover() {
    // The symmetric direction: a hold active at verification time but
    // released before cutover must equally block sealing on the stale state.
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "hold-released-during-staging").await;
    let (mut source, mut target) = (shards.source().await, shards.target().await);

    autumn_harvest::set_legal_hold(
        &mut source,
        exec_id,
        "under review",
        None,
        "compliance-bot",
        Utc::now(),
    )
    .await
    .expect("place a hold before staging begins");

    begin_migration(&mut source, exec_id, SOURCE, TARGET)
        .await
        .expect("begin");
    stage_copy(&mut source, &mut target, exec_id, TARGET)
        .await
        .expect("stage");
    verify_target_copy(&mut source, &mut target, exec_id, &codecs())
        .await
        .expect("verify");

    autumn_harvest::release_legal_hold(&mut source, exec_id, Utc::now())
        .await
        .expect("release the hold after verification");

    let cut_over = commit_cutover(&mut source, exec_id, TARGET)
        .await
        .expect("cutover call must not error");
    assert!(
        !cut_over,
        "a hold released after verification must also refuse the stale cutover"
    );
}

#[tokio::test]
async fn a_hold_placed_between_staging_and_verification_fails_verification() {
    // Issue #1317: a hold placed after `stage_copy`'s snapshot but BEFORE
    // `verify_target_copy` runs is a narrower window than the two tests
    // above. A stamp-only fix does not close it. Verification would read the
    // NEW hold value and stamp it. The cutover guard would then compare the
    // live value to that same stamp and match. That seals a source whose
    // target copy still holds the pre-hold columns. Verification must
    // compare the source's current value against what was actually staged on
    // the target, not merely record whatever the source shows now.
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "hold-between-stage-and-verify").await;
    let (mut source, mut target) = (shards.source().await, shards.target().await);

    begin_migration(&mut source, exec_id, SOURCE, TARGET)
        .await
        .expect("begin");
    stage_copy(&mut source, &mut target, exec_id, TARGET)
        .await
        .expect("stage before any hold exists");

    autumn_harvest::set_legal_hold(
        &mut source,
        exec_id,
        "hold arrived mid-staging",
        None,
        "compliance-bot",
        Utc::now(),
    )
    .await
    .expect("place a hold after staging but before verification");

    let verify_result = verify_target_copy(&mut source, &mut target, exec_id, &codecs()).await;
    assert!(
        verify_result.is_err(),
        "verification must refuse to authorize a cutover onto a target staged \
         before the hold existed, got {verify_result:?}"
    );

    let record = load_migration(&mut source, exec_id)
        .await
        .expect("load")
        .expect("row");
    assert_eq!(
        record.phase,
        MigrationPhase::Copied,
        "a failed verification must not advance the phase"
    );

    // The recovery path: abort and restage, which snapshots the row WITH the
    // hold this time, so the second attempt verifies and cuts over clean.
    abort_migration(
        &mut source,
        &mut target,
        exec_id,
        "hold arrived mid-staging",
    )
    .await
    .expect("abort");
    begin_migration(&mut source, exec_id, SOURCE, TARGET)
        .await
        .expect("begin again");
    stage_copy(&mut source, &mut target, exec_id, TARGET)
        .await
        .expect("restage with the hold already in place");
    verify_target_copy(&mut source, &mut target, exec_id, &codecs())
        .await
        .expect("verify");
    assert!(
        commit_cutover(&mut source, exec_id, TARGET)
            .await
            .expect("cutover"),
        "a cutover verified against a target staged under the current hold must succeed"
    );
}

#[tokio::test]
async fn a_legacy_verified_record_with_no_hold_snapshot_refuses_the_cutover() {
    // Issue #1317: `verified_legal_hold_set_at IS NOT DISTINCT FROM` alone
    // treats a NULL stamp (never checked) the same as a NULL stamp meaning
    // "checked, no hold". The two are indistinguishable by value once a
    // rolling deploy leaves a record verified by code that predates this
    // column. `legal_hold_verified` must be required too, so such a record
    // fails the cutover guard closed rather than matching by coincidence.
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "legacy-verified-no-hold-snapshot").await;
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

    // Simulate a record verified by code that predates `legal_hold_verified`.
    // The flag reverts to its column default even though the record is
    // otherwise VERIFIED with a matching (NULL) hold stamp.
    diesel::sql_query(
        "UPDATE harvest_shard_migrations SET legal_hold_verified = FALSE WHERE execution_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut source)
    .await
    .expect("simulate a legacy-verified record");

    let cut_over = commit_cutover(&mut source, exec_id, TARGET)
        .await
        .expect("cutover call must not error");
    assert!(
        !cut_over,
        "a record never checked for a hold by this code must not authorize a cutover"
    );
}

// ── Issue #1317: one bad target must not starve the whole resume sweep ──────

#[tokio::test]
async fn a_resume_sweep_finishes_healthy_records_past_one_unreachable_target() {
    // Issue #1317: `resume_incomplete_migrations` checked out its per-record
    // source/target connections with `?`. One record naming an unavailable or
    // unconfigured target shard aborted the whole sweep. That starved a
    // record whose own target is perfectly healthy and sits right behind it.
    let shards = setup_two_shards().await;

    // A record naming a shard this pool has no connection for at all. It is
    // the oldest by `created_at`, so it is the one an unfixed sweep dies on
    // before ever reaching the healthy record below.
    let unreachable_target = ShardId::new(99);
    diesel::sql_query(
        "INSERT INTO harvest_shard_migrations \
             (execution_id, source_shard, target_shard, phase, created_at, updated_at) \
         VALUES ($1, $2, $3, 'PENDING', NOW() - INTERVAL '1 minute', NOW())",
    )
    .bind::<diesel::sql_types::Uuid, _>(Uuid::new_v4())
    .bind::<diesel::sql_types::Integer, _>(SOURCE.as_i32())
    .bind::<diesel::sql_types::Integer, _>(unreachable_target.as_i32())
    .execute(&mut shards.source().await)
    .await
    .expect("seed an unresumable record naming an unreachable target");

    // A real, healthy migration staged and ready to finish.
    let exec_id = quiescent_fixture(&shards, "resume-past-bad-target").await;
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

    let outcomes = resume_incomplete_migrations(&shards.pool, SOURCE, 100, "tester", &codecs())
        .await
        .expect("the sweep must not fail wholesale on the other record's bad target");

    assert!(
        outcomes
            .iter()
            .any(|o| matches!(o, MigrationOutcome::Migrated { execution_id, .. } if *execution_id == exec_id)),
        "the healthy record must still be finished, got {outcomes:?}"
    );
    assert_eq!(authoritative_shards(&shards, exec_id).await, vec![TARGET]);
}

// ── Issue #1317: reopening a migration must not inherit a stale
// hold-verification marker from a prior settled attempt ─────────────────────

#[tokio::test]
async fn reopening_a_settled_migration_clears_the_stale_hold_marker() {
    // A settled (DONE or ABORTED) row can be reused by a later migration.
    // For example, after A -> B -> A, a second A -> B reuses this row.
    // Suppose `begin_migration`'s reset left `legal_hold_verified` at
    // whatever a PRIOR attempt last set it to. An old-code
    // `verify_target_copy` on the NEW attempt predates this column and never
    // touches it. It could leave a stale `TRUE` in place without having
    // checked anything for this attempt. The cutover guard would then trust
    // a check that never happened. `begin_migration` must clear both
    // hold-verification columns whenever it reopens a row, exactly as it
    // already clears `verified_fingerprint`.
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "reopen-clears-hold-marker").await;
    let mut source = shards.source().await;

    // Simulate a settled row left over from a prior attempt that verified a
    // hold, standing in for the general case of a stale prior verification.
    diesel::sql_query(
        "INSERT INTO harvest_shard_migrations \
             (execution_id, source_shard, target_shard, phase, \
              legal_hold_verified, verified_legal_hold_set_at) \
         VALUES ($1, $2, $3, 'DONE', TRUE, NULL)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Integer, _>(SOURCE.as_i32())
    .bind::<diesel::sql_types::Integer, _>(TARGET.as_i32())
    .execute(&mut source)
    .await
    .expect("seed a settled record with a stale hold marker");

    begin_migration(&mut source, exec_id, SOURCE, TARGET)
        .await
        .expect("reopen the settled record");

    let row: HoldMarkerRow = diesel::sql_query(
        "SELECT legal_hold_verified, verified_legal_hold_set_at \
           FROM harvest_shard_migrations WHERE execution_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .get_result(&mut source)
    .await
    .expect("load the reopened record");

    assert!(
        !row.legal_hold_verified,
        "reopening a settled migration must clear the stale verification flag"
    );
    assert_eq!(
        row.verified_legal_hold_set_at, None,
        "reopening a settled migration must clear the stale hold stamp"
    );
}

#[derive(diesel::QueryableByName)]
struct HoldMarkerRow {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    legal_hold_verified: bool,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
    verified_legal_hold_set_at: Option<chrono::DateTime<Utc>>,
}

#[tokio::test]
async fn the_reset_trigger_clears_the_stale_hold_marker_even_when_the_caller_does_not() {
    // Issue #1317: `begin_migration`'s explicit reset only fires when the
    // CODE PERFORMING THE REOPEN knows these columns exist. A parent-version
    // process is schema-compatible and can still reopen a row through its
    // OWN, older `ON CONFLICT` update, one that never mentions
    // `legal_hold_verified`/`verified_legal_hold_set_at` at all. The
    // application-level reset in `begin_migration` cannot protect against a
    // caller that predates it. Only a trigger on the phase transition itself
    // is independent of which binary performed the reopen.
    //
    // Prove that independence directly. Reopen the row with a raw UPDATE
    // shaped exactly like the OLD `begin_migration`: the phase transition
    // alone, with no mention of either hold column. Require the columns to
    // clear anyway.
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "old-code-reopen-clears-hold-marker").await;
    let mut source = shards.source().await;

    diesel::sql_query(
        "INSERT INTO harvest_shard_migrations \
             (execution_id, source_shard, target_shard, phase, \
              legal_hold_verified, verified_legal_hold_set_at) \
         VALUES ($1, $2, $3, 'DONE', TRUE, NULL)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Integer, _>(SOURCE.as_i32())
    .bind::<diesel::sql_types::Integer, _>(TARGET.as_i32())
    .execute(&mut source)
    .await
    .expect("seed a settled record with a stale hold marker");

    // Exactly the pre-#1317 `begin_migration` UPDATE: phase and the
    // pre-existing verification fields, nothing naming either hold column.
    diesel::sql_query(
        "UPDATE harvest_shard_migrations \
            SET phase = 'PENDING', target_shard = $2, source_shard = $3, \
                verified_fingerprint = NULL, abort_reason = NULL, \
                attempts = 0, last_error = NULL, updated_at = NOW() \
          WHERE execution_id = $1 AND phase IN ('DONE', 'ABORTED')",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Integer, _>(SOURCE.as_i32())
    .bind::<diesel::sql_types::Integer, _>(TARGET.as_i32())
    .execute(&mut source)
    .await
    .expect("reopen with an old-shaped UPDATE that never names either hold column");

    let row: HoldMarkerRow = diesel::sql_query(
        "SELECT legal_hold_verified, verified_legal_hold_set_at \
           FROM harvest_shard_migrations WHERE execution_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .get_result(&mut source)
    .await
    .expect("load the reopened record");

    assert!(
        !row.legal_hold_verified,
        "the trigger must clear the stale verification flag even when the \
         reopening UPDATE never names it"
    );
    assert_eq!(
        row.verified_legal_hold_set_at, None,
        "the trigger must clear the stale hold stamp even when the \
         reopening UPDATE never names it"
    );
}

#[tokio::test]
async fn a_declined_cutover_reports_legal_hold_drift_not_a_wake() {
    // Issue #1317: `commit_cutover` returning `false` also covers a hold
    // change since verification. Both drivers reported every decline as
    // "the execution woke up". That conceals the actual compliance-relevant
    // change from the operator and the audit log. A decline caused by hold
    // drift must say so.
    let shards = setup_two_shards().await;
    let exec_id = quiescent_fixture(&shards, "decline-reports-hold-drift").await;
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

    autumn_harvest::set_legal_hold(
        &mut source,
        exec_id,
        "placed after verification",
        None,
        "compliance-bot",
        Utc::now(),
    )
    .await
    .expect("place a hold after the copy was verified");

    let outcomes = resume_incomplete_migrations(&shards.pool, SOURCE, 10, "tester", &codecs())
        .await
        .expect("resume");

    let reason = outcomes
        .iter()
        .find_map(|o| match o {
            MigrationOutcome::Aborted {
                execution_id,
                reason,
            } if *execution_id == exec_id => Some(reason.as_str()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("expected an Aborted outcome for {exec_id}, got {outcomes:?}"));

    assert!(
        reason.contains("legal hold"),
        "a hold-drift decline must name the hold, not a wake; got: {reason}"
    );
    assert!(
        !reason.contains("woke") && !reason.contains("quiescent"),
        "a hold-drift decline must not also claim a wake happened; got: {reason}"
    );
}
