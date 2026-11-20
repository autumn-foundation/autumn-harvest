#![cfg(feature = "db")]
//! The pending table of the external outbox, on a real Postgres.
//!
//! A trigger on `harvest_events` keeps `harvest_external_outbox_pending`.
//! A request adds a row. A resolution with the same key removes it. The
//! three outbox claims read the table, so an idle claim does not walk the
//! resolved history.
//!
//! - The table follows requests and resolutions, in any insert order.
//! - An idle claim reads a fixed number of buffers, whatever the history.
//! - The migration backfills each unresolved request, and only those.
//! - Each partition layout change keeps the trigger.
//! - A drain of the `DEFAULT` partition does not bring back a resolved
//!   request.
//! - A property test compares the table with the legacy unresolved set.

use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Text, Timestamptz};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use jiff::Timestamp;
use proptest::prelude::*;
use proptest::test_runner::{TestCaseError, TestRunner};

use autumn_harvest::timeout::{
    external_await_outbox_claim_query, external_cancel_outbox_claim_query,
    external_signal_outbox_claim_query,
};

use crate::claim_bench_support::db as claim_bench_db;
use crate::integration_e2e::setup_test_database_url_or_env;

/// The table under test.
const PENDING_TABLE: &str = "harvest_external_outbox_pending";

/// The migration that adds the table.
const MIGRATION_DIR: &str = "20261119232545_harvest_external_outbox_pending";

/// The most buffers that an idle claim may read.
///
/// The legacy claim read about 4.7 buffers for each resolved request. The
/// gates below seed thousands, so a claim that walks them fails by far.
const IDLE_CLAIM_BUDGET: u64 = 16;

/// The most buffers that a claim of one pending request may read.
const ONE_ROW_CLAIM_BUDGET: u64 = 48;

/// One outbox family: the request type, the two resolution types and the
/// correlation key.
struct Family {
    requested: &'static str,
    resolved: [&'static str; 2],
    key: &'static str,
    query: fn() -> &'static str,
}

const FAMILIES: [Family; 3] = [
    Family {
        requested: "ExternalSignalRequested",
        resolved: ["ExternalSignalDelivered", "ExternalSignalFailed"],
        key: "signal_id",
        query: external_signal_outbox_claim_query,
    },
    Family {
        requested: "ExternalCancelRequested",
        resolved: ["ExternalCancelDelivered", "ExternalCancelFailed"],
        key: "cancel_id",
        query: external_cancel_outbox_claim_query,
    },
    Family {
        requested: "ExternalAwaitRequested",
        resolved: ["ExternalAwaitResolved", "ExternalAwaitFailed"],
        key: "await_id",
        query: external_await_outbox_claim_query,
    },
];

#[derive(QueryableByName, Debug)]
struct IdRow {
    #[diesel(sql_type = BigInt)]
    id: i64,
}

#[derive(QueryableByName, Debug, PartialEq, Eq)]
struct PendingRow {
    #[diesel(sql_type = BigInt)]
    event_row_id: i64,
    #[diesel(sql_type = Text)]
    request_type: String,
    #[diesel(sql_type = Text)]
    correlation_id: String,
}

#[derive(QueryableByName, Debug)]
struct InstantRow {
    #[diesel(sql_type = Timestamptz)]
    #[diesel(deserialize_as = jiff_diesel::Timestamp)]
    requested_at: Timestamp,
}

#[derive(QueryableByName, Debug)]
struct PlanLine {
    #[diesel(sql_type = Text, column_name = "QUERY PLAN")]
    line: String,
}

// ── fixtures ─────────────────────────────────────────────────────────────────

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url).await.expect("connect")
}

fn unique(prefix: &str) -> String {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    format!("{prefix}-{}", &suffix[..12])
}

/// Insert one run in `state` and return its id.
async fn insert_run(conn: &mut AsyncPgConnection, workflow_name: &str, state: &str) -> uuid::Uuid {
    let id = uuid::Uuid::new_v4();
    conn.batch_execute(&format!(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name, \
              started_at, created_at) \
         VALUES ('{id}', '{workflow_name}', '{workflow_name}-{id}', gen_random_uuid(), 0, \
                 '{state}', '{{}}'::jsonb, 'default', NOW(), NOW())"
    ))
    .await
    .expect("insert run");
    id
}

/// The SQL `VALUES` tuple of one event. `data` is the inner `data` object.
fn event_values(exec: uuid::Uuid, event_id: i32, event_type: &str, data: &str) -> String {
    format!(
        "('{exec}', {event_id}, '{event_type}', \
          jsonb_build_object('type', '{event_type}', 'data', '{data}'::jsonb), NOW())"
    )
}

/// Insert events in one statement, in the given order. Returns their row ids.
async fn insert_events(conn: &mut AsyncPgConnection, values: &[String]) -> Vec<i64> {
    let rows: Vec<IdRow> = diesel::sql_query(format!(
        "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp) \
         VALUES {} RETURNING id",
        values.join(", ")
    ))
    .load(conn)
    .await
    .expect("insert events");
    rows.into_iter().map(|r| r.id).collect()
}

/// Insert one event that carries `key = value`, and return its row id.
async fn keyed_event(
    conn: &mut AsyncPgConnection,
    exec: uuid::Uuid,
    event_id: i32,
    event_type: &str,
    key: &str,
    value: &str,
) -> i64 {
    insert_events(
        conn,
        &[event_values(
            exec,
            event_id,
            event_type,
            &format!("{{\"{key}\": \"{value}\"}}"),
        )],
    )
    .await[0]
}

/// The pending rows of the runs in `execs`, in row id order.
async fn pending_rows(conn: &mut AsyncPgConnection, execs: &[uuid::Uuid]) -> Vec<PendingRow> {
    let list = uuid_list(execs);
    diesel::sql_query(format!(
        "SELECT event_row_id, request_type, correlation_id FROM {PENDING_TABLE} \
         WHERE workflow_exec_id IN ({list}) ORDER BY event_row_id"
    ))
    .load(conn)
    .await
    .expect("read the pending table")
}

/// The pending row ids of the runs in `execs`.
async fn pending_ids(conn: &mut AsyncPgConnection, execs: &[uuid::Uuid]) -> Vec<i64> {
    pending_rows(conn, execs)
        .await
        .into_iter()
        .map(|r| r.event_row_id)
        .collect()
}

fn uuid_list(execs: &[uuid::Uuid]) -> String {
    execs
        .iter()
        .map(|e| format!("'{e}'::uuid"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The unresolved requests of the runs in `execs`, as the legacy claim
/// predicate selects them, in row id order. It ignores run state.
async fn legacy_unresolved(conn: &mut AsyncPgConnection, execs: &[uuid::Uuid]) -> Vec<i64> {
    let list = uuid_list(execs);
    let arms = FAMILIES
        .iter()
        .map(|f| {
            format!(
                "SELECT e.id FROM harvest_events e \
                 WHERE e.workflow_exec_id IN ({list}) \
                   AND e.event_type = '{requested}' \
                   AND (e.event_data->'data'->>'{key}') IS NOT NULL \
                   AND NOT EXISTS ( \
                       SELECT 1 FROM harvest_events res \
                       WHERE res.workflow_exec_id = e.workflow_exec_id \
                         AND res.event_type IN ('{first}', '{second}') \
                         AND res.event_data->'data'->>'{key}' = e.event_data->'data'->>'{key}')",
                requested = f.requested,
                key = f.key,
                first = f.resolved[0],
                second = f.resolved[1],
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let rows: Vec<IdRow> = diesel::sql_query(format!("SELECT id FROM ({arms}) u ORDER BY id"))
        .load(conn)
        .await
        .expect("legacy unresolved set");
    rows.into_iter().map(|r| r.id).collect()
}

/// Shared buffers that one generic-plan claim reads, as `EXPLAIN` reports
/// them on its top node.
///
/// The scanner keeps each claim prepared, so the generic plan is the plan
/// that runs.
async fn claim_buffers(conn: &mut AsyncPgConnection, sql: &str) -> (u64, String) {
    conn.batch_execute(&format!(
        "SET plan_cache_mode = force_generic_plan; \
         PREPARE outbox_claim(int4[], int8[]) AS {sql}"
    ))
    .await
    .expect("prepare the claim");
    let lines: Vec<PlanLine> = diesel::sql_query(
        "EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF) EXECUTE outbox_claim('{0}', '{}')",
    )
    .load(conn)
    .await
    .expect("explain the claim");
    conn.batch_execute("DEALLOCATE outbox_claim; RESET plan_cache_mode")
        .await
        .expect("deallocate the claim");
    let plan = lines
        .into_iter()
        .map(|l| l.line)
        .collect::<Vec<_>>()
        .join("\n");
    (top_node_buffers(&plan), plan)
}

/// Sum `hit` and `read` on the first `Buffers:` line. `EXPLAIN` omits the
/// line for a node that reads no buffer.
fn top_node_buffers(plan: &str) -> u64 {
    let Some(line) = plan
        .lines()
        .take_while(|l| !l.trim_start().starts_with("Planning"))
        .find(|l| l.trim_start().starts_with("Buffers:"))
    else {
        return 0;
    };
    ["hit=", "read="]
        .iter()
        .filter_map(|tag| {
            let start = line.find(tag)? + tag.len();
            line[start..]
                .split(|c: char| !c.is_ascii_digit())
                .next()?
                .parse::<u64>()
                .ok()
        })
        .sum()
}

/// Seed `count` resolved requests of `family` over `runs` runs of the
/// workflow `workflow_name`. Event ids start at `band`.
async fn seed_resolved_history(
    conn: &mut AsyncPgConnection,
    family: &Family,
    workflow_name: &str,
    band: i32,
    count: i32,
) {
    conn.batch_execute(&format!(
        "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp) \
         SELECT x.id, {band} + g, '{requested}', \
                jsonb_build_object('type', '{requested}', \
                    'data', jsonb_build_object('{key}', 'h-' || g)), \
                NOW() - interval '1 day' + (g || ' seconds')::interval \
         FROM generate_series(1, {count}) g \
         JOIN LATERAL ( \
             SELECT id FROM harvest_workflow_executions \
             WHERE workflow_name = '{workflow_name}' \
             ORDER BY id OFFSET (g % 40) LIMIT 1) x ON TRUE; \
         INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp) \
         SELECT r.workflow_exec_id, r.event_id + 100000, '{resolved}', \
                jsonb_build_object('type', '{resolved}', \
                    'data', jsonb_build_object('{key}', r.event_data->'data'->>'{key}')), \
                r.timestamp + interval '1 second' \
         FROM harvest_events r \
         JOIN harvest_workflow_executions x ON x.id = r.workflow_exec_id \
         WHERE x.workflow_name = '{workflow_name}' AND r.event_type = '{requested}';",
        requested = family.requested,
        resolved = family.resolved[0],
        key = family.key,
    ))
    .await
    .expect("seed resolved history");
}

/// A migrated database that no other test shares.
async fn isolated_db() -> claim_bench_db::BenchDb {
    claim_bench_db::setup_bench_db().await.unwrap_or_else(|r| {
        panic!(
            "these gates need an isolated database, from HARVEST_TEST_DATABASE_URL \
             (an admin connection string) or a reachable Docker daemon: {}",
            r.0
        )
    })
}

fn migration_sql(file: &str) -> String {
    let path = format!(
        "{}/migrations/{MIGRATION_DIR}/{file}",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"))
}

// ── the projection ───────────────────────────────────────────────────────────

/// A request adds one row. Each of the two resolution types removes it.
#[tokio::test]
async fn a_request_adds_a_row_and_a_resolution_removes_it() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    for family in &FAMILIES {
        let exec = insert_run(&mut conn, &unique("outbox_pending_basic"), "RUNNING").await;
        let first = keyed_event(&mut conn, exec, 1, family.requested, family.key, "k1").await;
        let second = keyed_event(&mut conn, exec, 2, family.requested, family.key, "k2").await;
        assert_eq!(
            pending_rows(&mut conn, &[exec]).await,
            vec![
                PendingRow {
                    event_row_id: first,
                    request_type: family.requested.to_string(),
                    correlation_id: "k1".to_string(),
                },
                PendingRow {
                    event_row_id: second,
                    request_type: family.requested.to_string(),
                    correlation_id: "k2".to_string(),
                },
            ],
            "{}: each request adds a row",
            family.requested
        );
        keyed_event(&mut conn, exec, 3, family.resolved[0], family.key, "k1").await;
        assert_eq!(pending_ids(&mut conn, &[exec]).await, vec![second]);
        keyed_event(&mut conn, exec, 4, family.resolved[1], family.key, "k2").await;
        assert!(
            pending_ids(&mut conn, &[exec]).await.is_empty(),
            "{}: each resolution type removes its row",
            family.requested
        );
    }
}

/// The check is per run. A resolution in another run leaves the row. A
/// resolution of another family leaves it too.
#[tokio::test]
async fn a_resolution_of_another_run_or_family_leaves_the_row() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    for (slot, family) in FAMILIES.iter().enumerate() {
        let exec = insert_run(&mut conn, &unique("outbox_pending_scope"), "RUNNING").await;
        let other = insert_run(&mut conn, &unique("outbox_pending_scope"), "RUNNING").await;
        let request = keyed_event(&mut conn, exec, 1, family.requested, family.key, "k").await;
        keyed_event(&mut conn, other, 1, family.resolved[0], family.key, "k").await;
        let foreign = &FAMILIES[(slot + 1) % FAMILIES.len()];
        keyed_event(&mut conn, exec, 2, foreign.resolved[0], family.key, "k").await;
        assert_eq!(pending_ids(&mut conn, &[exec]).await, vec![request]);
    }
}

/// A request with no key, or a JSON null key, is never claimed. It adds no
/// row.
#[tokio::test]
async fn a_request_without_a_key_adds_no_row() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    for family in &FAMILIES {
        let exec = insert_run(&mut conn, &unique("outbox_pending_nokey"), "RUNNING").await;
        insert_events(
            &mut conn,
            &[
                event_values(exec, 1, family.requested, "{}"),
                event_values(
                    exec,
                    2,
                    family.requested,
                    &format!("{{\"{}\": null}}", family.key),
                ),
            ],
        )
        .await;
        assert_eq!(pending_ids(&mut conn, &[exec]).await, Vec::<i64>::new());
    }
}

/// One resolution resolves each request with its key, as the legacy
/// `NOT EXISTS` check does.
#[tokio::test]
async fn one_resolution_removes_each_request_with_its_key() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    for family in &FAMILIES {
        let exec = insert_run(&mut conn, &unique("outbox_pending_dup"), "RUNNING").await;
        keyed_event(&mut conn, exec, 1, family.requested, family.key, "same").await;
        keyed_event(&mut conn, exec, 2, family.requested, family.key, "same").await;
        assert_eq!(pending_ids(&mut conn, &[exec]).await.len(), 2);
        keyed_event(&mut conn, exec, 3, family.resolved[1], family.key, "same").await;
        assert_eq!(pending_ids(&mut conn, &[exec]).await, Vec::<i64>::new());
    }
}

/// A resolution that is already stored wins over a later request. This
/// holds in one statement, in either row order, and across statements.
#[tokio::test]
async fn a_stored_resolution_wins_in_any_insert_order() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    for family in &FAMILIES {
        let key = format!("{{\"{}\": \"k\"}}", family.key);
        let one_statement = insert_run(&mut conn, &unique("outbox_pending_order"), "RUNNING").await;
        insert_events(
            &mut conn,
            &[
                event_values(one_statement, 2, family.resolved[0], &key),
                event_values(one_statement, 1, family.requested, &key),
            ],
        )
        .await;
        let two_statements =
            insert_run(&mut conn, &unique("outbox_pending_order"), "RUNNING").await;
        keyed_event(
            &mut conn,
            two_statements,
            2,
            family.resolved[1],
            family.key,
            "k",
        )
        .await;
        keyed_event(
            &mut conn,
            two_statements,
            1,
            family.requested,
            family.key,
            "k",
        )
        .await;
        assert!(
            pending_ids(&mut conn, &[one_statement, two_statements])
                .await
                .is_empty(),
            "{}: a resolved request has no row",
            family.requested
        );
    }
}

/// A row goes with its run, as retention removes the run.
#[tokio::test]
async fn a_row_goes_with_its_run() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let exec = insert_run(&mut conn, &unique("outbox_pending_cascade"), "COMPLETED").await;
    keyed_event(
        &mut conn,
        exec,
        1,
        FAMILIES[0].requested,
        FAMILIES[0].key,
        "k",
    )
    .await;
    assert_eq!(pending_ids(&mut conn, &[exec]).await.len(), 1);
    conn.batch_execute(&format!(
        "DELETE FROM harvest_events WHERE workflow_exec_id = '{exec}'; \
         DELETE FROM harvest_workflow_executions WHERE id = '{exec}';"
    ))
    .await
    .expect("remove the run");
    assert_eq!(pending_ids(&mut conn, &[exec]).await, Vec::<i64>::new());
}

/// The row keeps the request instant, which orders the drain. A caller can
/// set that instant, so it can be older than the append.
#[tokio::test]
async fn a_row_keeps_the_request_instant() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let exec = insert_run(&mut conn, &unique("outbox_pending_instant"), "RUNNING").await;
    let rows: Vec<IdRow> = diesel::sql_query(format!(
        "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp) \
         VALUES ('{exec}', 1, 'ExternalSignalRequested', \
                 '{{\"type\": \"ExternalSignalRequested\", \"data\": {{\"signal_id\": \"k\"}}}}'::jsonb, \
                 '2020-01-02T03:04:05Z') RETURNING id"
    ))
    .load(&mut conn)
    .await
    .expect("insert a back-dated request");
    let instant: Vec<InstantRow> = diesel::sql_query(format!(
        "SELECT requested_at FROM {PENDING_TABLE} WHERE event_row_id = {}",
        rows[0].id
    ))
    .load(&mut conn)
    .await
    .expect("read the row");
    let expected: Timestamp = "2020-01-02T03:04:05Z".parse().unwrap();
    assert_eq!(instant[0].requested_at, expected);
}

// ── the claim ────────────────────────────────────────────────────────────────

/// An idle claim reads a fixed number of buffers, whatever the resolved
/// history holds. A claim of one pending request stays small too.
#[tokio::test]
async fn an_idle_claim_does_not_read_the_resolved_history() {
    let bench = isolated_db().await;
    let mut conn = claim_bench_db::connect(&bench.url).await;
    for _ in 0..40 {
        insert_run(&mut conn, "outbox_idle_running", "RUNNING").await;
        insert_run(&mut conn, "outbox_idle_done", "COMPLETED").await;
    }
    for (slot, family) in FAMILIES.iter().enumerate() {
        let band = 1_000 + i32::try_from(slot).unwrap() * 10_000;
        seed_resolved_history(&mut conn, family, "outbox_idle_running", band, 1_500).await;
        seed_resolved_history(&mut conn, family, "outbox_idle_done", band, 1_500).await;
    }
    // Each resolution deleted its pending row. Autovacuum reclaims the dead
    // entries of a small table soon. Run it now, so the gate reads the steady
    // state and not the churn since the last pass.
    conn.batch_execute(&format!("VACUUM {PENDING_TABLE}"))
        .await
        .expect("vacuum the pending table");
    conn.batch_execute("ANALYZE harvest_events; ANALYZE harvest_workflow_executions;")
        .await
        .expect("analyze");

    for family in &FAMILIES {
        let (buffers, plan) = claim_buffers(&mut conn, (family.query)()).await;
        assert!(
            buffers <= IDLE_CLAIM_BUDGET,
            "{}: an idle claim read {buffers} buffers over 3,000 resolved requests\n{plan}",
            family.requested
        );
    }

    let exec = insert_run(&mut conn, "outbox_idle_pending", "RUNNING").await;
    for (slot, family) in FAMILIES.iter().enumerate() {
        let event_id = i32::try_from(slot).unwrap() + 1;
        keyed_event(&mut conn, exec, event_id, family.requested, family.key, "p").await;
        let (buffers, plan) = claim_buffers(&mut conn, (family.query)()).await;
        assert!(
            plan.contains("rows=1 "),
            "{}: the claim returns the pending request\n{plan}",
            family.requested
        );
        assert!(
            buffers <= ONE_ROW_CLAIM_BUDGET,
            "{}: a claim of one request read {buffers} buffers\n{plan}",
            family.requested
        );
    }
}

// ── the migration ────────────────────────────────────────────────────────────

/// The migration backfills each unresolved request, on any run state, and
/// no resolved one. The trigger then keeps the table.
#[tokio::test]
async fn the_migration_backfills_each_unresolved_request() {
    let bench = isolated_db().await;
    let mut conn = claim_bench_db::connect(&bench.url).await;
    conn.batch_execute(&format!("BEGIN; {} COMMIT;", migration_sql("down.sql")))
        .await
        .expect("revert the migration");

    let mut execs = Vec::new();
    for (slot, family) in FAMILIES.iter().enumerate() {
        let running = insert_run(&mut conn, "outbox_backfill", "RUNNING").await;
        let done = insert_run(&mut conn, "outbox_backfill", "COMPLETED").await;
        execs.extend([running, done]);
        let base = i32::try_from(slot).unwrap() * 10;
        keyed_event(
            &mut conn,
            running,
            base + 1,
            family.requested,
            family.key,
            "open",
        )
        .await;
        keyed_event(
            &mut conn,
            running,
            base + 2,
            family.requested,
            family.key,
            "shut",
        )
        .await;
        keyed_event(
            &mut conn,
            running,
            base + 3,
            family.resolved[1],
            family.key,
            "shut",
        )
        .await;
        keyed_event(
            &mut conn,
            done,
            base + 1,
            family.requested,
            family.key,
            "left",
        )
        .await;
        insert_events(
            &mut conn,
            &[event_values(done, base + 2, family.requested, "{}")],
        )
        .await;
    }
    let expected = legacy_unresolved(&mut conn, &execs).await;
    assert_eq!(expected.len(), 6, "two unresolved requests for each family");

    conn.batch_execute(&format!("BEGIN; {} COMMIT;", migration_sql("up.sql")))
        .await
        .expect("apply the migration");
    assert_eq!(pending_ids(&mut conn, &execs).await, expected);

    let later = insert_run(&mut conn, "outbox_backfill", "RUNNING").await;
    keyed_event(
        &mut conn,
        later,
        1,
        FAMILIES[0].requested,
        FAMILIES[0].key,
        "new",
    )
    .await;
    assert_eq!(pending_ids(&mut conn, &[later]).await.len(), 1);
}

// ── partition layouts ────────────────────────────────────────────────────────

/// Each layout change keeps the trigger, and the trigger is not an operator
/// trigger.
#[tokio::test]
async fn the_trigger_survives_each_layout_change() {
    let bench = isolated_db().await;
    let mut conn = claim_bench_db::connect(&bench.url).await;
    let exec = insert_run(&mut conn, "outbox_layout", "RUNNING").await;
    let mut next = 1;
    let mut request = async |conn: &mut AsyncPgConnection| {
        let id = keyed_event(
            conn,
            exec,
            next,
            "ExternalSignalRequested",
            "signal_id",
            &format!("k{next}"),
        )
        .await;
        next += 1;
        assert!(
            pending_ids(conn, &[exec]).await.contains(&id),
            "the trigger adds a row after the layout change"
        );
    };
    request(&mut conn).await;
    if autumn_harvest::partition::detect_layout(&mut conn)
        .await
        .expect("layout")
        .is_partitioned()
    {
        autumn_harvest::partition::disable_partitioning(&mut conn)
            .await
            .expect("revert");
        request(&mut conn).await;
    }
    autumn_harvest::partition::enable_partitioning(
        &mut conn,
        &autumn_harvest::partition::EnableOptions::default(),
    )
    .await
    .expect("convert");
    request(&mut conn).await;
    autumn_harvest::partition::disable_partitioning(&mut conn)
        .await
        .expect("revert");
    request(&mut conn).await;
    assert!(
        autumn_harvest::partition::operator_triggers(&mut conn)
            .await
            .unwrap()
            .is_empty(),
        "the outbox trigger is not an operator trigger"
    );
    let requests = usize::try_from(next - 1).unwrap();
    assert_eq!(
        pending_ids(&mut conn, &[exec]).await.len(),
        requests,
        "each request survives each layout change"
    );
}

/// A drain of the `DEFAULT` partition inserts each row again. A resolved
/// request must not come back, and a pending one must stay once.
#[tokio::test]
async fn a_default_partition_drain_keeps_the_table_exact() {
    let bench = isolated_db().await;
    let mut conn = claim_bench_db::connect(&bench.url).await;
    if !autumn_harvest::partition::detect_layout(&mut conn)
        .await
        .expect("layout")
        .is_partitioned()
    {
        autumn_harvest::partition::enable_partitioning(
            &mut conn,
            &autumn_harvest::partition::EnableOptions::default(),
        )
        .await
        .expect("convert");
    }
    let today = autumn_harvest::partition::partition_name(autumn_harvest::partition::cohort_start(
        Timestamp::now(),
        autumn_harvest::partition::DEFAULT_COHORT_WIDTH_SECS,
    ));
    conn.batch_execute(&format!("DROP TABLE {today}"))
        .await
        .expect("park new rows in the DEFAULT partition");

    let exec = insert_run(&mut conn, "outbox_drain", "RUNNING").await;
    let family = &FAMILIES[0];
    let open = keyed_event(&mut conn, exec, 1, family.requested, family.key, "open").await;
    keyed_event(&mut conn, exec, 2, family.requested, family.key, "shut").await;
    keyed_event(&mut conn, exec, 3, family.resolved[0], family.key, "shut").await;
    assert_eq!(pending_ids(&mut conn, &[exec]).await, vec![open]);

    let moved = autumn_harvest::partition::drain_default(&mut conn)
        .await
        .expect("drain");
    assert!(moved >= 3, "the drain moves the parked rows");
    assert_eq!(pending_ids(&mut conn, &[exec]).await, vec![open]);
}

// ── property ─────────────────────────────────────────────────────────────────

/// One generated insert: family, run, key, kind and a statement break.
///
/// Kind 0 is a request, 1 and 2 are the two resolution types, and 3 is a
/// request without a key.
type Op = (usize, usize, u8, u8, bool);

fn ops() -> impl Strategy<Value = Vec<Op>> {
    proptest::collection::vec((0usize..3, 0usize..2, 0u8..3, 0u8..4, any::<bool>()), 1..24)
}

/// Apply `ops` to two new runs. Then compare the table with the legacy
/// unresolved set.
async fn check_against_the_legacy_set(url: &str, ops: Vec<Op>) -> Result<(), TestCaseError> {
    let mut conn = connect(url).await;
    let execs = [
        insert_run(&mut conn, "outbox_pending_prop", "RUNNING").await,
        insert_run(&mut conn, "outbox_pending_prop", "RUNNING").await,
    ];
    let mut next_event = [1i32, 1i32];
    let mut statement: Vec<String> = Vec::new();
    for (family, run, key, kind, new_statement) in ops {
        if new_statement && !statement.is_empty() {
            insert_events(&mut conn, &statement).await;
            statement.clear();
        }
        let family = &FAMILIES[family];
        let (event_type, data) = match kind {
            0 => (
                family.requested,
                format!("{{\"{}\": \"k{key}\"}}", family.key),
            ),
            1 | 2 => (
                family.resolved[usize::from(kind - 1)],
                format!("{{\"{}\": \"k{key}\"}}", family.key),
            ),
            _ => (family.requested, "{}".to_string()),
        };
        statement.push(event_values(execs[run], next_event[run], event_type, &data));
        next_event[run] += 1;
    }
    if !statement.is_empty() {
        insert_events(&mut conn, &statement).await;
    }
    let table = pending_ids(&mut conn, &execs).await;
    let legacy = legacy_unresolved(&mut conn, &execs).await;
    prop_assert_eq!(table, legacy);
    Ok(())
}

/// For any insert sequence, the table holds exactly the requests that the
/// legacy predicate calls unresolved.
#[test]
fn the_table_matches_the_legacy_unresolved_set() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let (url, _container) = runtime.block_on(setup_test_database_url_or_env());
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(48);
    let mut runner = TestRunner::new(proptest::test_runner::Config {
        cases,
        failure_persistence: None,
        ..proptest::test_runner::Config::default()
    });
    runner
        .run(&ops(), |ops| {
            runtime.block_on(check_against_the_legacy_set(&url, ops))
        })
        .unwrap();
}
