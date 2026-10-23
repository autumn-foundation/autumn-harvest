#![cfg(feature = "db")]
//! The keyed-entity state projection on a real Postgres (issue #2259).
//!
//! - The projection and history agree after a crash at every step of a
//!   history, and through a worker crash in the middle of an op.
//! - A reset of a continued run starts from the starting state of that run.
//! - Erasure tombstones the projection, and codec rotation sweeps it.
//! - The projection trigger survives each partition layout change.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run these tests
//! without Docker.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use autumn_harvest::aead_codec::{AeadCodec, DataKey};
use autumn_harvest::entity::{
    ENTITY_CHECKPOINT_SIDE_EFFECT, ENTITY_DELETED_SIDE_EFFECT, ENTITY_OP_SIGNAL,
    ENTITY_STATE_QUERY, ENTITY_STATE_SIDE_EFFECT, ENTITY_STATS_QUERY, Entity, EntityCheckpoint,
    EntityMessage, EntitySnapshot, EntityStats,
};
use autumn_harvest::entity_state::{self, EntityStateRow, ProjectedSnapshot};
use autumn_harvest::event::{SideEffectKind, WorkflowEvent};
use autumn_harvest::payload_codec::{PayloadCodecs, codec_envelope_key_id};
use autumn_harvest::prelude::*;
use autumn_harvest::reset::{ResetSignalReapplyPolicy, WorkflowResetRequest};
use autumn_harvest::store;
use autumn_harvest::worker::{DbPool, Worker, WorkerRuntimeConfig};
use autumn_harvest::{StickyRoutingConfig, TypedSignalWithStartOptions};
use chrono::Utc;
use diesel::sql_types::{Text, Uuid as SqlUuid};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

use crate::integration_e2e::{build_test_pool, setup_test_database_url_or_env, spawn_test_worker};

// ── fixtures ─────────────────────────────────────────────────────────────────

fn unique(prefix: &str) -> String {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    format!("{prefix}-{}", &suffix[..12])
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url).await.expect("connect")
}

/// Insert a run of `(workflow_name, workflow_id)` in `state`.
async fn insert_run(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    state: &str,
) -> ExecutionId {
    let exec_id = ExecutionId::new();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
         (id, workflow_name, workflow_id, shard_id, input, queue_name, state) \
         VALUES ($1, $2, $3, 0, '{}'::jsonb, 'default', $4)",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .bind::<Text, _>(workflow_name)
    .bind::<Text, _>(workflow_id)
    .bind::<Text, _>(state)
    .execute(conn)
    .await
    .expect("insert run");
    exec_id
}

fn started() -> WorkflowEvent {
    WorkflowEvent::WorkflowStarted {
        input: json!({}),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }
}

fn signal(message: &EntityMessage<i64>) -> WorkflowEvent {
    WorkflowEvent::SignalReceived {
        signal_name: ENTITY_OP_SIGNAL.to_string(),
        payload: json!(message),
    }
}

fn snapshot_value(state: i64, applied: u64) -> Value {
    let mut stats = EntityStats::default();
    stats.applied = applied;
    json!(EntitySnapshot { state, stats })
}

fn snapshot(name: &str, state: i64, applied: u64) -> WorkflowEvent {
    WorkflowEvent::SideEffectRecorded {
        kind: SideEffectKind::EntityState,
        name: Some(name.to_string()),
        value: snapshot_value(state, applied),
    }
}

fn checkpoint_record() -> WorkflowEvent {
    WorkflowEvent::SideEffectRecorded {
        kind: SideEffectKind::Custom,
        name: Some(ENTITY_CHECKPOINT_SIDE_EFFECT.to_string()),
        value: json!(false),
    }
}

/// The history of a counter entity: add 1, add 2, delete.
fn counter_history() -> Vec<WorkflowEvent> {
    vec![
        started(),
        snapshot(ENTITY_STATE_SIDE_EFFECT, 0, 0),
        signal(&EntityMessage::op(1)),
        snapshot(ENTITY_STATE_SIDE_EFFECT, 1, 1),
        checkpoint_record(),
        signal(&EntityMessage::op(2)),
        snapshot(ENTITY_STATE_SIDE_EFFECT, 3, 2),
        checkpoint_record(),
        signal(&EntityMessage::delete()),
        snapshot(ENTITY_DELETED_SIDE_EFFECT, 3, 2),
        WorkflowEvent::WorkflowCompleted { output: json!(3) },
    ]
}

/// The last snapshot in `events`, as `(event index, name, value)`.
fn last_snapshot(events: &[WorkflowEvent]) -> Option<(usize, String, Value)> {
    events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| match event {
            WorkflowEvent::SideEffectRecorded {
                kind: SideEffectKind::EntityState,
                name: Some(name),
                value,
            } => Some((index, name.clone(), value.clone())),
            _ => None,
        })
        .next_back()
}

/// Assert that the projection of the key is the last committed snapshot of
/// the run `exec_id`.
async fn assert_agrees(
    conn: &mut AsyncPgConnection,
    codecs: &PayloadCodecs,
    exec_id: ExecutionId,
    key: (&str, &str),
    step: &str,
) {
    let history = store::load_history_with_codecs(conn, exec_id, codecs)
        .await
        .expect("load history");
    let row = entity_state::read(conn, codecs, key.0, key.1)
        .await
        .expect("read projection");
    match (last_snapshot(&history.events), row) {
        (None, None) => {}
        (Some((index, name, value)), Some(row)) => {
            assert_eq!(row.execution_id, exec_id, "{step}: the run");
            assert_eq!(
                usize::try_from(row.event_id).unwrap(),
                index,
                "{step}: the event"
            );
            assert_eq!(
                row.deleted,
                name == ENTITY_DELETED_SIDE_EFFECT,
                "{step}: the delete"
            );
            let expected = ProjectedSnapshot::Current(serde_json::from_value(value).unwrap());
            assert_eq!(row.snapshot, expected, "{step}: the snapshot");
        }
        (history, row) => panic!("{step}: history {history:?} and projection {row:?} disagree"),
    }
}

/// A migrated Postgres that no other test shares.
///
/// The codec sweep and the partition layout change act on the whole
/// database, so their tests cannot share one.
async fn setup_isolated_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let db_name = format!("harvest_entity_state_{}", uuid::Uuid::new_v4().simple());
        let mut admin = connect(&admin_url).await;
        admin
            .batch_execute(&format!("CREATE DATABASE \"{db_name}\""))
            .await
            .expect("create throwaway database");
        let (base, _) = admin_url.split_once('?').unwrap_or((&admin_url, ""));
        let cut = base.rfind('/').expect("a postgres URL has a database path");
        let url = format!("{}/{db_name}", &base[..cut]);
        let mut conn = connect(&url).await;
        conn.batch_execute(&autumn_harvest::test_init_sql())
            .await
            .expect("apply migrations");
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(autumn_harvest::test_init_sql().into_bytes())
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

// ── crash at every step ──────────────────────────────────────────────────────

/// AC: the projection and history agree after a crash at every step.
///
/// Each event goes in twice. The first insert runs in a transaction whose
/// connection drops before the commit, as a crashed worker does. The second
/// insert commits. After each one, the projection is the last committed
/// snapshot, or no row before the first one.
#[tokio::test]
async fn projection_and_history_agree_after_a_crash_at_every_step() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let codecs = PayloadCodecs::default();
    let name = "entity_state_crash";
    let key = unique("crash");
    let exec_id = insert_run(&mut conn, name, &key, "RUNNING").await;

    for (index, event) in counter_history().iter().enumerate() {
        let event_id = i32::try_from(index).unwrap();
        let mut crashed = connect(&url).await;
        crashed.batch_execute("BEGIN").await.expect("begin");
        store::append_events_with_codecs(
            &mut crashed,
            exec_id,
            std::slice::from_ref(event),
            event_id,
            &codecs,
        )
        .await
        .expect("append before the crash");
        // The projection write is visible inside the transaction.
        if let Some((_, _, value)) = last_snapshot(&counter_history()[..=index]) {
            let inside = entity_state::read(&mut crashed, &codecs, name, &key)
                .await
                .expect("read inside")
                .expect("a row inside the transaction");
            assert_eq!(
                inside.snapshot,
                ProjectedSnapshot::Current(serde_json::from_value(value).unwrap())
            );
        }
        drop(crashed);
        assert_agrees(
            &mut conn,
            &codecs,
            exec_id,
            (name, &key),
            &format!("crash at {index}"),
        )
        .await;

        conn.transaction::<_, autumn_harvest::error::HarvestError, _>(async |tx| {
            store::append_events_with_codecs(
                tx,
                exec_id,
                std::slice::from_ref(event),
                event_id,
                &codecs,
            )
            .await
        })
        .await
        .expect("append and commit");
        assert_agrees(
            &mut conn,
            &codecs,
            exec_id,
            (name, &key),
            &format!("commit at {index}"),
        )
        .await;
    }
    let row = entity_state::read(&mut conn, &codecs, name, &key)
        .await
        .unwrap()
        .expect("the final row");
    assert!(row.deleted, "the delete marks the key");
}

/// The projection orders writes. A run of the key that writes later wins.
/// In one run, an older event never replaces a newer one, so an insert out
/// of order cannot move the row backwards. A stale run cannot write at all:
/// the decision fence and its terminal state stop it before the insert.
#[tokio::test]
async fn a_later_run_wins_and_an_older_event_loses() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let codecs = PayloadCodecs::default();
    let name = "entity_state_order";
    let key = unique("order");
    let first = insert_run(&mut conn, name, &key, "CONTINUED_AS_NEW").await;
    let second = insert_run(&mut conn, name, &key, "RUNNING").await;

    store::append_events(
        &mut conn,
        first,
        &[started(), snapshot(ENTITY_STATE_SIDE_EFFECT, 5, 1)],
        0,
    )
    .await
    .unwrap();
    store::append_events(&mut conn, second, &[started()], 0)
        .await
        .unwrap();
    // The newer event of the second run goes in before the older one.
    store::append_events(
        &mut conn,
        second,
        &[snapshot(ENTITY_STATE_SIDE_EFFECT, 9, 2)],
        2,
    )
    .await
    .unwrap();
    let row = entity_state::read(&mut conn, &codecs, name, &key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.execution_id, second, "the later run wins");
    assert_eq!(row.event_id, 2);
    store::append_events(
        &mut conn,
        second,
        &[snapshot(ENTITY_STATE_SIDE_EFFECT, 7, 1)],
        1,
    )
    .await
    .unwrap();
    let row = entity_state::read(&mut conn, &codecs, name, &key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.event_id, 2, "the older event loses");
    assert_eq!(
        row.snapshot,
        ProjectedSnapshot::Current(serde_json::from_value(snapshot_value(9, 2)).unwrap())
    );
    // Two keys never share a row.
    let other = unique("order");
    let third = insert_run(&mut conn, name, &other, "RUNNING").await;
    store::append_events(
        &mut conn,
        third,
        &[started(), snapshot(ENTITY_STATE_SIDE_EFFECT, 9, 0)],
        0,
    )
    .await
    .unwrap();
    let listed = entity_state::list(&mut conn, &codecs, name, None, 100, false)
        .await
        .unwrap();
    let keys: Vec<&str> = listed.iter().map(|r| r.workflow_id.as_str()).collect();
    assert!(keys.contains(&key.as_str()) && keys.contains(&other.as_str()));
}

/// A shard move that aborts gives the row back. The staged copy of a run
/// moved the row of the key to that run. The abort removes the copy, and
/// the row returns to the latest snapshot of the runs that stay.
#[tokio::test]
async fn an_aborted_copy_gives_the_row_back() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let codecs = PayloadCodecs::default();
    let name = "entity_state_abort";
    let key = unique("abort");
    let kept = insert_run(&mut conn, name, &key, "COMPLETED").await;
    store::append_events(
        &mut conn,
        kept,
        &[started(), snapshot(ENTITY_DELETED_SIDE_EFFECT, 4, 1)],
        0,
    )
    .await
    .unwrap();
    let staged = insert_run(&mut conn, name, &key, "MIGRATING").await;
    store::append_events(
        &mut conn,
        staged,
        &[started(), snapshot(ENTITY_STATE_SIDE_EFFECT, 8, 2)],
        0,
    )
    .await
    .unwrap();
    let row = entity_state::read(&mut conn, &codecs, name, &key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.execution_id, staged, "the copy took the row");

    entity_state::rebuild_without_execution(&mut conn, staged)
        .await
        .unwrap();
    let row = entity_state::read(&mut conn, &codecs, name, &key)
        .await
        .unwrap()
        .expect("the row is back");
    assert_eq!(row.execution_id, kept);
    assert!(row.deleted);
    assert_eq!(
        row.snapshot.query_answer(ENTITY_STATE_QUERY),
        Some(json!(4))
    );

    // With no other run, the key has no row.
    let lone_key = unique("abort");
    let lone = insert_run(&mut conn, name, &lone_key, "MIGRATING").await;
    store::append_events(
        &mut conn,
        lone,
        &[started(), snapshot(ENTITY_STATE_SIDE_EFFECT, 1, 1)],
        0,
    )
    .await
    .unwrap();
    entity_state::rebuild_without_execution(&mut conn, lone)
        .await
        .unwrap();
    assert!(
        entity_state::read(&mut conn, &codecs, name, &lone_key)
            .await
            .unwrap()
            .is_none()
    );
}

// ── erasure and codec rotation ───────────────────────────────────────────────

/// AC: erasure covers the state table. An erase of the run that wrote the row
/// tombstones its snapshot. An erase of an older run leaves the row alone.
#[tokio::test]
async fn erasure_tombstones_the_projection_of_the_erased_run() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let codecs = PayloadCodecs::default();
    let name = "entity_state_erase";

    // An older run of a live key: its erase must not touch the live row.
    let live_key = unique("live");
    let old = insert_run(&mut conn, name, &live_key, "CONTINUED_AS_NEW").await;
    store::append_events(
        &mut conn,
        old,
        &[started(), snapshot(ENTITY_STATE_SIDE_EFFECT, 1, 1)],
        0,
    )
    .await
    .unwrap();
    let live = insert_run(&mut conn, name, &live_key, "RUNNING").await;
    store::append_events(
        &mut conn,
        live,
        &[started(), snapshot(ENTITY_STATE_SIDE_EFFECT, 1, 1)],
        0,
    )
    .await
    .unwrap();
    let outcome = autumn_harvest::erase::erase_workflow_payloads(&mut conn, old, "test")
        .await
        .expect("erase the old run");
    assert_eq!(outcome.entity_states_scrubbed, 0);
    let row = entity_state::read(&mut conn, &codecs, name, &live_key)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(row.snapshot, ProjectedSnapshot::Current(_)));

    // A deleted entity: its last run wrote the row.
    let key = unique("erase");
    let exec_id = insert_run(&mut conn, name, &key, "COMPLETED").await;
    store::append_events(&mut conn, exec_id, &counter_history(), 0)
        .await
        .unwrap();
    let outcome = autumn_harvest::erase::erase_workflow_payloads(&mut conn, exec_id, "test")
        .await
        .expect("erase");
    assert_eq!(outcome.entity_states_scrubbed, 1);
    let row = entity_state::read(&mut conn, &codecs, name, &key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.snapshot, ProjectedSnapshot::Erased);
    assert_eq!(row.snapshot.query_answer(ENTITY_STATE_QUERY), None);
    let raw: Vec<SnapshotCell> =
        diesel::sql_query("SELECT snapshot FROM harvest_entity_state WHERE workflow_id = $1")
            .bind::<Text, _>(&key)
            .load(&mut conn)
            .await
            .unwrap();
    assert_eq!(raw[0].snapshot, autumn_harvest::erase::erasure_tombstone());

    let again = autumn_harvest::erase::erase_workflow_payloads(&mut conn, exec_id, "test")
        .await
        .expect("erase again");
    assert_eq!(again.entity_states_scrubbed, 0, "erasure is idempotent");
}

#[derive(diesel::QueryableByName)]
struct SnapshotCell {
    #[diesel(sql_type = diesel::sql_types::Jsonb)]
    snapshot: Value,
}

/// The stored `snapshot` cell of the key `workflow_id`.
async fn stored_snapshot(conn: &mut AsyncPgConnection, workflow_id: &str) -> Value {
    let rows: Vec<SnapshotCell> =
        diesel::sql_query("SELECT snapshot FROM harvest_entity_state WHERE workflow_id = $1")
            .bind::<Text, _>(workflow_id)
            .load(conn)
            .await
            .unwrap();
    rows.into_iter().next().expect("a row").snapshot
}

/// A registry with two AES-256-GCM keys, active on `k1`.
fn two_key_registry() -> PayloadCodecs {
    let codecs = PayloadCodecs::default();
    for (key_id, byte) in [("k1", 0x11), ("k2", 0x22)] {
        let key = DataKey::from_bytes(&[byte; 32]).expect("data key");
        AeadCodec::new(key_id, &key)
            .expect("aead codec")
            .register_with(&codecs)
            .expect("register");
    }
    codecs.set_active_key("k1").expect("activate k1");
    codecs
}

/// AC: codec rotation covers the state table. The projection holds the
/// ciphertext of the event. The sweep moves it to the active key, and a read
/// still decodes the same state.
#[tokio::test]
async fn codec_rotation_sweeps_the_projection() {
    let (url, _container) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let name = "entity_state_codec";
    let key = unique("codec");
    let exec_id = insert_run(&mut conn, name, &key, "RUNNING").await;
    store::append_events_with_codecs(
        &mut conn,
        exec_id,
        &[started(), snapshot(ENTITY_STATE_SIDE_EFFECT, 42, 1)],
        0,
        &codecs,
    )
    .await
    .unwrap();
    let before = stored_snapshot(&mut conn, &key).await;
    assert_eq!(codec_envelope_key_id(&before), Some("k1"), "{before}");
    assert!(
        before.get("state").is_none() && !before.to_string().contains("\"state\""),
        "the projection holds ciphertext"
    );

    codecs.set_active_key("k2").expect("activate k2");
    let mut rounds = 0;
    while autumn_harvest::codec_rotation::sweep_codec_reencryption_once(
        &mut conn,
        0,
        &codecs,
        100,
        &autumn_harvest::telemetry::NoOpMetrics,
    )
    .await
    .expect("sweep")
        > 0
    {
        rounds += 1;
        assert!(rounds < 20, "the sweep converges");
    }
    let after = stored_snapshot(&mut conn, &key).await;
    assert_eq!(codec_envelope_key_id(&after), Some("k2"), "{after}");
    let row = entity_state::read(&mut conn, &codecs, name, &key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        row.snapshot.query_answer(ENTITY_STATE_QUERY),
        Some(json!(42)),
        "the plaintext is unchanged"
    );
    let census =
        autumn_harvest::codec_rotation::load_shard_rotation_progress(&mut conn, 0, &codecs)
            .await
            .expect("census");
    assert_eq!(
        census.rows_by_key_id.get("k1").copied(),
        None,
        "no cell is left on the retired key"
    );
}

// ── partition layouts ────────────────────────────────────────────────────────

/// Each partition layout change reinstalls the projection trigger. A
/// snapshot written after each change still reaches the projection.
#[tokio::test]
async fn the_projection_trigger_survives_each_layout_change() {
    let (url, _container) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = PayloadCodecs::default();
    let name = "entity_state_layout";
    let key = unique("layout");
    let exec_id = insert_run(&mut conn, name, &key, "RUNNING").await;
    store::append_events(&mut conn, exec_id, &[started()], 0)
        .await
        .unwrap();

    let mut next = 1;
    let mut write = async |conn: &mut AsyncPgConnection, state: i64| {
        store::append_events(
            conn,
            exec_id,
            &[snapshot(ENTITY_STATE_SIDE_EFFECT, state, 0)],
            next,
        )
        .await
        .unwrap();
        next += 1;
        let row = entity_state::read(conn, &codecs, name, &key)
            .await
            .unwrap()
            .expect("a row");
        assert_eq!(
            row.snapshot.query_answer(ENTITY_STATE_QUERY),
            Some(json!(state))
        );
    };
    write(&mut conn, 1).await;
    let partitioned = autumn_harvest::partition::detect_layout(&mut conn)
        .await
        .expect("layout")
        .is_partitioned();
    if partitioned {
        autumn_harvest::partition::disable_partitioning(&mut conn)
            .await
            .expect("revert");
        write(&mut conn, 2).await;
    }
    autumn_harvest::partition::enable_partitioning(
        &mut conn,
        &autumn_harvest::partition::EnableOptions::default(),
    )
    .await
    .expect("convert");
    write(&mut conn, 3).await;
    autumn_harvest::partition::disable_partitioning(&mut conn)
        .await
        .expect("revert");
    write(&mut conn, 4).await;
    assert!(
        autumn_harvest::partition::operator_triggers(&mut conn)
            .await
            .unwrap()
            .is_empty(),
        "the projection trigger is not an operator trigger"
    );
}

// ── real workers ─────────────────────────────────────────────────────────────

/// When set, the `hang` op hangs in its activity, as on a dead worker.
static HANG: AtomicBool = AtomicBool::new(false);

#[activity(
    start_to_close = "2s",
    retry = RetryPolicy::fixed(5, Duration::from_millis(100))
)]
async fn entity_state_step(_ctx: &ActivityContext, op: String) -> Result<String, String> {
    if op == "hang" && AtomicBool::load(&HANG, Ordering::SeqCst) {
        std::future::pending::<()>().await;
    }
    Ok(op)
}

/// An entity whose state is the list of applied ops. Each op runs one
/// activity on the run's own queue.
#[workflow]
async fn entity_state_log(ctx: &WorkflowContext, input: Value) -> Result<Vec<String>, String> {
    let queue = ctx.queue_name().to_string();
    Entity::<Vec<String>>::from_input(ctx, input)
        .run(|mut log: Vec<String>, op: String| {
            let queue = queue.clone();
            async move {
                let done: String = ctx
                    .execute_activity_with_opts(
                        &entity_state_step_info(),
                        op,
                        Some(&queue),
                        None,
                        None,
                    )
                    .await
                    .map_err(|e| e.to_string())?;
                log.push(done);
                Ok(log)
            }
        })
        .await
        .map_err(|e| e.to_string())
}

/// An entity that takes a checkpoint after two ops.
#[workflow]
async fn entity_state_pairs(ctx: &WorkflowContext, input: Value) -> Result<Vec<String>, String> {
    Entity::<Vec<String>>::from_input(ctx, input)
        .max_ops_per_run(2)
        .run(|mut log: Vec<String>, op: String| async move {
            log.push(op);
            Ok(log)
        })
        .await
        .map_err(|e| e.to_string())
}

fn build_worker(queue: &str) -> Arc<Worker> {
    let config = WorkerConfig::default().with_sticky_routing(StickyRoutingConfig {
        lease_ttl: Duration::ZERO,
    });
    let built = HarvestBuilder::new()
        .workflows(workflows![entity_state_log, entity_state_pairs])
        .activities(activities![entity_state_step])
        .worker(config.with_queues([queue]))
        .build();
    let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = unique("entity-state-worker");
    runtime_config.poll_interval = Duration::from_millis(50);
    Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker should build"))
}

fn spawn(worker: &Arc<Worker>, pool: &DbPool) -> tokio::task::JoinHandle<()> {
    spawn_test_worker(Arc::clone(worker), pool.clone())
}

async fn send(
    conn: &mut AsyncPgConnection,
    client: &WorkflowHandleClient,
    queue: &str,
    key: &str,
    pairs: bool,
    message: EntityMessage<String>,
) -> ExecutionId {
    let opts = TypedSignalWithStartOptions {
        queue_name: Some(queue.to_string()),
        idempotency_key: Some(uuid::Uuid::new_v4().to_string()),
        ..Default::default()
    };
    let handle = if pairs {
        EntityStatePairsStub::signal_with_start(
            conn,
            client,
            key,
            json!({}),
            ENTITY_OP_SIGNAL,
            message,
            opts,
        )
        .await
    } else {
        EntityStateLogStub::signal_with_start(
            conn,
            client,
            key,
            json!({}),
            ENTITY_OP_SIGNAL,
            message,
            opts,
        )
        .await
    };
    handle.expect("signal with start").exec_id()
}

fn op(name: &str) -> EntityMessage<String> {
    EntityMessage::op(name.to_string())
}

/// Wait until the projection row of the key passes `done`.
async fn wait_for_row(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    key: &str,
    what: &str,
    done: impl Fn(&EntityStateRow) -> bool,
) -> EntityStateRow {
    let codecs = PayloadCodecs::default();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(row) = entity_state::read(conn, &codecs, workflow_name, key)
            .await
            .expect("read projection")
            && done(&row)
        {
            return row;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn state_of(row: &EntityStateRow) -> Option<Vec<String>> {
    row.snapshot
        .query_answer(ENTITY_STATE_QUERY)
        .and_then(|v| serde_json::from_value(v).ok())
}

/// Compare the projection with the history of the run that wrote it, in one
/// snapshot of the database. Returns `false` when the key has no row yet.
async fn sample_agrees(conn: &mut AsyncPgConnection, workflow_name: &str, key: &str) -> bool {
    let codecs = PayloadCodecs::default();
    conn.batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("begin");
    let row = entity_state::read(conn, &codecs, workflow_name, key)
        .await
        .expect("read projection");
    let found = if let Some(row) = row {
        let history = store::load_history_with_codecs(conn, row.execution_id, &codecs)
            .await
            .expect("load history");
        let (index, name, value) =
            last_snapshot(&history.events).expect("the run that wrote the row has a snapshot");
        assert_eq!(usize::try_from(row.event_id).unwrap(), index);
        assert_eq!(row.deleted, name == ENTITY_DELETED_SIDE_EFFECT);
        assert_eq!(
            row.snapshot,
            ProjectedSnapshot::Current(serde_json::from_value(value).unwrap())
        );
        true
    } else {
        false
    };
    conn.batch_execute("COMMIT").await.expect("commit");
    found
}

/// AC: the projection and history agree after a crash at every step, on a
/// real worker. A sampler compares them in one database snapshot every few
/// milliseconds. Worker A hangs in an op and dies. Worker B retries the op.
#[tokio::test]
async fn projection_tracks_history_through_a_worker_crash() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let pool_a = build_test_pool(&url);
    let mut conn = connect(&url).await;
    let client = WorkflowHandleClient::single(pool.clone(), url.clone());
    let queue = unique("entity-state-q");
    let key = unique("tracked");
    let name = "entity_state_log";

    let stop = Arc::new(AtomicBool::new(false));
    let sampler = {
        let stop = Arc::clone(&stop);
        let url = url.clone();
        let key = key.clone();
        tokio::spawn(async move {
            let mut conn = connect(&url).await;
            let mut samples = 0_u32;
            while !AtomicBool::load(&stop, Ordering::SeqCst) {
                if sample_agrees(&mut conn, name, &key).await {
                    samples += 1;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            samples
        })
    };

    let worker_a = build_worker(&queue);
    let handle_a = spawn(&worker_a, &pool_a);
    send(&mut conn, &client, &queue, &key, false, op("a")).await;
    wait_for_row(&mut conn, name, &key, "op a", |row| {
        state_of(row) == Some(vec!["a".to_string()])
    })
    .await;

    HANG.store(true, Ordering::SeqCst);
    send(&mut conn, &client, &queue, &key, false, op("hang")).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    handle_a.abort();
    let _ = handle_a.await;
    worker_a.shutdown();
    pool_a.close();
    HANG.store(false, Ordering::SeqCst);

    let worker_b = build_worker(&queue);
    let handle_b = spawn(&worker_b, &pool);
    send(&mut conn, &client, &queue, &key, false, op("c")).await;
    let row = wait_for_row(&mut conn, name, &key, "op c", |row| {
        state_of(row).is_some_and(|log| log.len() == 3)
    })
    .await;
    assert_eq!(
        state_of(&row),
        Some(vec!["a".to_string(), "hang".to_string(), "c".to_string()])
    );
    send(
        &mut conn,
        &client,
        &queue,
        &key,
        false,
        EntityMessage::delete(),
    )
    .await;
    let row = wait_for_row(&mut conn, name, &key, "the delete", |row| row.deleted).await;
    assert_eq!(state_of(&row).map(|log| log.len()), Some(3));

    stop.store(true, Ordering::SeqCst);
    let samples = sampler.await.expect("sampler");
    assert!(samples > 0, "the sampler saw the projection");
    assert!(
        sample_agrees(&mut conn, name, &key).await,
        "the final row agrees"
    );
    worker_b.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle_b).await;
}

/// The running run of the key.
async fn running_run(conn: &mut AsyncPgConnection, workflow_name: &str, key: &str) -> ExecutionId {
    #[derive(diesel::QueryableByName)]
    struct Id {
        #[diesel(sql_type = SqlUuid)]
        id: uuid::Uuid,
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let rows: Vec<Id> = diesel::sql_query(
            "SELECT id FROM harvest_workflow_executions \
             WHERE workflow_name = $1 AND workflow_id = $2 AND state = 'RUNNING'",
        )
        .bind::<Text, _>(workflow_name)
        .bind::<Text, _>(key)
        .load(conn)
        .await
        .expect("load runs");
        if let Some(row) = rows.into_iter().next() {
            return ExecutionId::from_uuid(row.id);
        }
        assert!(Instant::now() < deadline, "no running run");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// The first snapshot in the history of `exec_id`.
async fn first_snapshot(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> Option<Value> {
    let history = store::load_history(conn, exec_id).await.expect("history");
    history.events.into_iter().find_map(|event| match event {
        WorkflowEvent::SideEffectRecorded {
            kind: SideEffectKind::EntityState,
            value,
            ..
        } => Some(value),
        _ => None,
    })
}

/// AC: replay and reset of a continued run rebuild the starting state.
///
/// Two ops end the first run with continue-as-new. The continued run starts
/// from `[a, b]`. A reset of the continued run to its start starts again
/// from `[a, b]`, and the projection moves to the reset run. A later op then
/// applies on top of it. A query reads the projection.
#[tokio::test]
async fn a_reset_of_a_continued_run_rebuilds_its_starting_state() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let mut conn = connect(&url).await;
    let client = WorkflowHandleClient::single(pool.clone(), url.clone());
    let queue = unique("entity-state-reset-q");
    let key = unique("reset");
    let name = "entity_state_pairs";
    let ab = vec!["a".to_string(), "b".to_string()];

    let worker = build_worker(&queue);
    let handle = spawn(&worker, &pool);
    send(&mut conn, &client, &queue, &key, true, op("a")).await;
    send(&mut conn, &client, &queue, &key, true, op("b")).await;

    // The continued run records its starting state first.
    let row = wait_for_row(&mut conn, name, &key, "the continued run", |row| {
        state_of(row) == Some(ab.clone()) && row.event_id <= 2
    })
    .await;
    let continued = running_run(&mut conn, name, &key).await;
    assert_eq!(
        row.execution_id, continued,
        "the projection is on the new run"
    );
    assert_eq!(
        first_snapshot(&mut conn, continued).await,
        Some(
            json!({"state": ab, "stats": {"applied": 2, "failed": 0, "checkpoints": 1, "last_error": null}})
        ),
    );

    // Reset the continued run to its start.
    let reset = autumn_harvest::reset::reset_workflow_execution(
        &mut conn,
        continued,
        WorkflowResetRequest {
            reset_to_event_id: Some(0),
            reset_point: None,
            reason: "issue 2259".to_string(),
            operator_id: "test".to_string(),
            signal_reapply: ResetSignalReapplyPolicy::Drop,
            allow_terminal_source: false,
        },
        None,
    )
    .await
    .expect("reset");
    let row = wait_for_row(&mut conn, name, &key, "the reset run", |row| {
        row.execution_id == reset.new_exec_id
    })
    .await;
    assert_eq!(state_of(&row), Some(ab.clone()), "the starting state");
    assert_eq!(
        first_snapshot(&mut conn, reset.new_exec_id).await,
        first_snapshot(&mut conn, continued).await,
        "the reset run starts where the continued run started"
    );

    send(&mut conn, &client, &queue, &key, true, op("c")).await;
    let row = wait_for_row(&mut conn, name, &key, "op c", |row| {
        state_of(row).is_some_and(|log| log.len() == 3)
    })
    .await;
    assert_eq!(
        state_of(&row),
        Some(vec!["a".to_string(), "b".to_string(), "c".to_string()])
    );
    let codecs = PayloadCodecs::default();
    let answer =
        entity_state::answer_query(&mut conn, &codecs, row.execution_id, ENTITY_STATS_QUERY)
            .await
            .expect("answer")
            .expect("the projection answers");
    assert_eq!(answer["applied"], json!(3));

    worker.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
}

/// The checkpoint input of a continued run carries the state itself. Its
/// first snapshot equals the decoded input.
#[tokio::test]
async fn the_checkpoint_input_carries_the_starting_state() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let mut conn = connect(&url).await;
    let client = WorkflowHandleClient::single(pool.clone(), url.clone());
    let queue = unique("entity-state-input-q");
    let key = unique("input");
    let name = "entity_state_pairs";

    let worker = build_worker(&queue);
    let handle = spawn(&worker, &pool);
    send(&mut conn, &client, &queue, &key, true, op("x")).await;
    send(&mut conn, &client, &queue, &key, true, op("y")).await;
    wait_for_row(&mut conn, name, &key, "the continued run", |row| {
        state_of(row).is_some_and(|log| log.len() == 2) && row.event_id <= 2
    })
    .await;
    let continued = running_run(&mut conn, name, &key).await;
    #[derive(diesel::QueryableByName)]
    struct Input {
        #[diesel(sql_type = diesel::sql_types::Jsonb)]
        input: Value,
    }
    let rows: Vec<Input> =
        diesel::sql_query("SELECT input FROM harvest_workflow_executions WHERE id = $1")
            .bind::<SqlUuid, _>(continued.as_uuid())
            .load(&mut conn)
            .await
            .unwrap();
    let input: EntityCheckpoint<Vec<String>> =
        serde_json::from_value(rows[0].input.clone()).unwrap();
    let first: EntitySnapshot<Vec<String>> =
        serde_json::from_value(first_snapshot(&mut conn, continued).await.unwrap()).unwrap();
    assert_eq!(first.state, input.state);
    assert_eq!(first.stats, input.stats);

    worker.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
}
