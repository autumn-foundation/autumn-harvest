#![cfg(feature = "db")]
//! The by-id task row statements read the table through its primary key.
//!
//! Each statement in `row_guard::guarded_statements` finds one row by `id`.
//! Its guard on `state` and `worker_id` must not let the planner choose a
//! partial index. Two fixtures make the planner try:
//!
//! - Grouped: 100,000 rows, and 5 to 200 `RUNNING` rows for each of 100
//!   workers. At 5 to 50 rows per worker, `running_worker_idx` can cost less
//!   than the primary key.
//! - Idle: the statistics come from a table with no live row. Each live
//!   partial index then looks empty.
//!
//! Other tests prove that the `CASE` form keeps the same rows. Each test owns
//! a fresh, migrated database and drops it at the end.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a Postgres server, or let
//! each test start a container.

use autumn_harvest::row_guard::{GuardedStatement, guarded_statements};
use diesel::QueryableByName;
use diesel::sql_types::Text;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const ROWS: i64 = 100_000;
const WORKERS: i64 = 100;

type DbGuard = Option<ContainerAsync<Postgres>>;

async fn setup_server() -> (String, DbGuard) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

/// `url` with its database set to `database`. A query string stays.
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

/// A fresh, migrated database that one test owns.
struct Db {
    conn: AsyncPgConnection,
    url: String,
    name: String,
    server: String,
}

impl Db {
    /// Create and migrate a database on `server`. Autovacuum is off on the
    /// task queue, so no background analyze changes the statistics.
    async fn create(server: &str) -> Self {
        let name = format!("row_guard_{}", uuid::Uuid::new_v4().simple());
        AsyncPgConnection::establish(server)
            .await
            .expect("connect to the server")
            .batch_execute(&format!("CREATE DATABASE {name}"))
            .await
            .expect("create database");
        let url = with_database(server, &name);
        let mut conn = AsyncPgConnection::establish(&url)
            .await
            .expect("connect to the fresh database");
        conn.batch_execute(&autumn_harvest::test_init_sql())
            .await
            .expect("apply migration bundle");
        conn.batch_execute("ALTER TABLE harvest_task_queue SET (autovacuum_enabled = false)")
            .await
            .expect("turn off autovacuum");
        Self {
            conn,
            url,
            name,
            server: server.to_owned(),
        }
    }

    /// Drop the database, best effort.
    async fn drop(self) {
        let Self {
            conn, name, server, ..
        } = self;
        drop(conn);
        if let Ok(mut admin) = AsyncPgConnection::establish(&server).await {
            let _ = admin
                .batch_execute(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
                .await;
        }
    }
}

/// Insert `rows` rows. The first `running` are `RUNNING`, spread over the
/// workers. Of the rest, 2% are `PENDING`, and the others are terminal.
async fn seed(conn: &mut AsyncPgConnection, rows: i64, running: i64) {
    conn.batch_execute(&format!(
        "INSERT INTO harvest_task_queue \
             (queue_name, task_type, input, state, worker_id, started_at, attempt, activity_name) \
         SELECT 'default', \
                CASE WHEN g % 3 = 0 THEN 'workflow' ELSE 'activity' END, \
                '{{}}'::jsonb, \
                CASE WHEN g <= {running} THEN 'RUNNING' \
                     WHEN g % 50 = 0 THEN 'PENDING' \
                     WHEN g % 50 = 1 THEN 'FAILED' \
                     ELSE 'COMPLETED' END, \
                CASE WHEN g <= {running} THEN 'worker-' || (g % {WORKERS}) END, \
                CASE WHEN g <= {running} THEN now() - g * INTERVAL '1 ms' END, \
                1, \
                CASE WHEN g % 3 = 0 THEN NULL ELSE 'activity' END \
         FROM generate_series(1, {rows}) g"
    ))
    .await
    .expect("seed rows");
}

async fn analyze(conn: &mut AsyncPgConnection) {
    conn.batch_execute("VACUUM ANALYZE harvest_task_queue")
        .await
        .expect("vacuum analyze");
}

/// A literal for a parameter of `ty`. A custom plan reads it. A generic plan
/// does not.
fn argument(ty: &str) -> &'static str {
    match ty {
        "uuid" => "'00000000-0000-0000-0000-000000000000'",
        "uuid[]" => "'{00000000-0000-0000-0000-000000000000}'",
        "text" => "'worker-7'",
        "integer" | "bigint" | "double precision" => "1",
        "timestamptz" => "now()",
        "interval" => "'1 second'",
        "text[]" => "'{PENDING,RUNNING}'",
        "boolean" => "true",
        "integer[]" | "bigint[]" => "'{}'",
        "bytea" => "decode(repeat('00', 64), 'hex')",
        other => panic!("no argument for parameter type {other}"),
    }
}

/// The settings of the claim transaction, and the planner defaults.
const SETTINGS: [(&str, &str); 2] = [
    (
        "claim",
        "SET LOCAL jit = off; SET LOCAL enable_bitmapscan = off; SET LOCAL enable_sort = off;",
    ),
    ("default", ""),
];

#[derive(QueryableByName)]
struct Plan {
    #[diesel(sql_type = Text, column_name = "QUERY PLAN")]
    plan: String,
}

/// The generic or custom plan of `statement` as JSON text.
async fn plan_of(
    conn: &mut AsyncPgConnection,
    statement: &GuardedStatement,
    settings: &str,
    cache_mode: &str,
) -> serde_json::Value {
    let types = statement.param_types.join(", ");
    let args: Vec<&str> = statement
        .param_types
        .iter()
        .map(|ty| argument(ty))
        .collect();
    conn.batch_execute(&format!(
        "BEGIN; {settings} SET LOCAL plan_cache_mode = {cache_mode}; \
         PREPARE guarded ({types}) AS {}",
        statement.sql
    ))
    .await
    .unwrap_or_else(|e| panic!("{}: prepare failed: {e}\n{}", statement.name, statement.sql));
    let plan: Plan = diesel::sql_query(format!(
        "EXPLAIN (FORMAT JSON) EXECUTE guarded ({})",
        args.join(", ")
    ))
    .get_result(conn)
    .await
    .unwrap_or_else(|e| panic!("{}: explain failed: {e}", statement.name));
    conn.batch_execute("DEALLOCATE guarded; ROLLBACK")
        .await
        .expect("end plan transaction");
    serde_json::from_str(&plan.plan).expect("plan is JSON")
}

/// Each scan of `harvest_task_queue` in `node`: `(alias, node type, index)`.
fn task_queue_scans(node: &serde_json::Value, out: &mut Vec<(String, String, String)>) {
    if node.get("Relation Name").and_then(|v| v.as_str()) == Some("harvest_task_queue")
        && node.get("Node Type").and_then(|v| v.as_str()) != Some("ModifyTable")
    {
        let text = |key: &str| {
            node.get(key)
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_owned()
        };
        let index = if text("Node Type") == "Bitmap Heap Scan" {
            bitmap_indexes(node).join(",")
        } else {
            text("Index Name")
        };
        out.push((text("Alias"), text("Node Type"), index));
    }
    if let Some(children) = node.get("Plans").and_then(|v| v.as_array()) {
        for child in children {
            task_queue_scans(child, out);
        }
    }
}

/// The indexes that the bitmap index scans under `node` read.
fn bitmap_indexes(node: &serde_json::Value) -> Vec<String> {
    let mut out = Vec::new();
    for child in node
        .get("Plans")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
    {
        if let Some(index) = child.get("Index Name").and_then(|v| v.as_str()) {
            out.push(index.to_owned());
        }
        out.extend(bitmap_indexes(child));
    }
    out
}

/// True for a one-row claim: a by-id claim or an owner take.
fn is_claim(statement: &str) -> bool {
    statement.starts_with("claim_by_id") || statement.starts_with("take_by_execution")
}

/// A scan that may use another index, by design.
///
/// The concurrency cap recheck counts the running rows of a key through the
/// `recheck` alias. A one-row claim also counts them through the `t` alias.
fn exempt(statement: &str, alias: &str) -> bool {
    alias == "recheck" || (is_claim(statement) && alias == "t")
}

/// Each statement on `conn` that leaves its full index, as a message.
///
/// Each checked scan reads the full index of the statement or the primary
/// key. A claim reads its candidate through its full index, then updates
/// the row through the primary key.
async fn primary_key_failures(conn: &mut AsyncPgConnection, fixture: &str) -> Vec<String> {
    let mut failures = Vec::new();
    for statement in guarded_statements() {
        for (settings_name, settings) in SETTINGS {
            for cache_mode in ["force_generic_plan", "force_custom_plan"] {
                let plan = plan_of(conn, &statement, settings, cache_mode).await;
                let mut scans = Vec::new();
                task_queue_scans(&plan[0]["Plan"], &mut scans);
                let checked: Vec<_> = scans
                    .iter()
                    .filter(|(a, _, _)| !exempt(statement.name, a))
                    .collect();
                let on_pkey = !checked.is_empty()
                    && checked.iter().all(|(_, node, index)| {
                        (node.starts_with("Index") || node == "Bitmap Heap Scan")
                            && (index == "harvest_task_queue_pkey" || index == statement.index)
                    });
                if !on_pkey {
                    failures.push(format!(
                        "{fixture}/{settings_name}/{cache_mode}: {} scans {checked:?}",
                        statement.name
                    ));
                }
            }
        }
    }
    failures
}

#[tokio::test]
async fn by_id_guards_use_the_primary_key_for_any_slot_count() {
    let (server, _guard) = setup_server().await;
    let mut db = Db::create(&server).await;
    let mut failures = Vec::new();
    for per_worker in [5, 20, 50, 200] {
        db.conn
            .batch_execute("TRUNCATE harvest_task_queue CASCADE")
            .await
            .expect("empty the queue");
        seed(&mut db.conn, ROWS, per_worker * WORKERS).await;
        analyze(&mut db.conn).await;
        let fixture = format!("{per_worker} per worker");
        failures.extend(primary_key_failures(&mut db.conn, &fixture).await);
    }
    db.drop().await;
    assert!(
        failures.is_empty(),
        "these by-id statements leave the primary key:\n{}",
        failures.join("\n")
    );
}

/// The statistics come from a table with no live row. The live rows arrive
/// after the analyze, as a load arrives after an idle period.
#[tokio::test]
async fn by_id_guards_use_the_primary_key_after_an_idle_analyze() {
    let (server, _guard) = setup_server().await;
    let mut db = Db::create(&server).await;
    db.conn
        .batch_execute(&format!(
            "INSERT INTO harvest_task_queue (queue_name, task_type, input, state, attempt) \
             SELECT 'default', 'workflow', '{{}}'::jsonb, 'COMPLETED', 1 \
             FROM generate_series(1, {ROWS})"
        ))
        .await
        .expect("seed terminal rows");
    analyze(&mut db.conn).await;
    seed(&mut db.conn, 2_000, 2_000).await;
    let failures = primary_key_failures(&mut db.conn, "idle statistics").await;
    db.drop().await;
    assert!(
        failures.is_empty(),
        "these by-id statements leave the primary key:\n{}",
        failures.join("\n")
    );
}

// ── Equivalence: the CASE form keeps the same rows ─────────────────────────

/// `sql` with each `(CASE WHEN g THEN true END)` written back as `(g)`.
///
/// A guard holds no ` THEN `. So a `CASE` whose first ` THEN ` is not
/// ` THEN true END)` is another expression, such as a sort key, and stays.
fn plain_form(sql: &str) -> String {
    const OPEN: &str = "(CASE WHEN ";
    const THEN: &str = " THEN ";
    const CLOSE: &str = " THEN true END)";
    let mut out = String::with_capacity(sql.len());
    let mut rest = sql;
    while let Some(at) = rest.find(OPEN) {
        let then = rest[at..].find(THEN).expect("a CASE has a THEN") + at;
        if !rest[then..].starts_with(CLOSE) {
            out.push_str(&rest[..at + OPEN.len()]);
            rest = &rest[at + OPEN.len()..];
            continue;
        }
        out.push_str(&rest[..at]);
        out.push('(');
        out.push_str(&rest[at + OPEN.len()..then]);
        out.push(')');
        rest = &rest[then + CLOSE.len()..];
    }
    out.push_str(rest);
    out
}

#[derive(QueryableByName)]
struct Verdict {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    same: bool,
    #[diesel(sql_type = Text)]
    case: String,
}

/// `WHERE a AND CASE WHEN g THEN true END` keeps a row exactly when
/// `WHERE a AND g` keeps it, for each SQL truth value of `a` and `g`.
async fn assert_truth_table(conn: &mut AsyncPgConnection) {
    let verdicts: Vec<Verdict> = diesel::sql_query(
        "SELECT ((a AND (CASE WHEN g THEN true END)) IS TRUE) \
                  = ((a AND g) IS TRUE) AS same, \
                format('a=%s g=%s', a, g) AS case \
         FROM (VALUES (true), (false), (NULL::boolean)) AS x(a), \
              (VALUES (true), (false), (NULL::boolean)) AS y(g)",
    )
    .load(conn)
    .await
    .expect("truth table");
    assert_eq!(verdicts.len(), 9, "every pair of truth values");
    for verdict in verdicts {
        assert!(verdict.same, "the forms differ for {}", verdict.case);
    }
}

/// Seed rows for the differential test: every mix of state, worker, kind,
/// queue, activity, cap and strike count that a guard reads.
///
/// Each row has its own execution, so an owner take names one row. The
/// execution of a row with the held activity is paused.
async fn seed_differential(conn: &mut AsyncPgConnection) {
    conn.batch_execute(
        "INSERT INTO harvest_queue_pauses (queue_name, reason) VALUES ('held', 'test'); \
         INSERT INTO harvest_activity_pauses (activity_name, reason) VALUES ('held', 'test'); \
         WITH seeded AS ( \
             SELECT gen_random_uuid() AS exec, s, w, k, q, a, c, cap \
             FROM unnest(ARRAY['RUNNING', 'PENDING', 'FAILED']) s, \
                  unnest(ARRAY['worker-7', 'worker-8', NULL]) w, \
                  unnest(ARRAY['workflow', 'activity']) k, \
                  unnest(ARRAY['held', 'open']) q, \
                  unnest(ARRAY['held', 'open']) a, \
                  unnest(ARRAY[0, 1]) c, \
                  unnest(ARRAY[true, false]) cap \
         ), runs AS ( \
             INSERT INTO harvest_workflow_executions \
                 (id, workflow_name, workflow_id, shard_id, input, state) \
             SELECT exec, 'w', exec::text, 0, '{}', \
                    CASE WHEN a = 'held' THEN 'PAUSED' ELSE 'RUNNING' END \
             FROM seeded \
         ) \
         INSERT INTO harvest_task_queue \
             (queue_name, task_type, input, state, worker_id, started_at, attempt, \
              crash_strikes, activity_name, concurrency_key, concurrency_cap, \
              workflow_exec_id) \
         SELECT q, k, '{}'::jsonb, s, w, now(), 1, c, \
                CASE WHEN k = 'activity' THEN a END, \
                CASE WHEN cap THEN 'key' END, CASE WHEN cap THEN 0 END, exec \
         FROM seeded",
    )
    .await
    .expect("seed differential rows");
}

/// One seeded row: its id and the id of its execution.
#[derive(QueryableByName)]
struct Row {
    #[diesel(sql_type = Text)]
    id: String,
    #[diesel(sql_type = Text)]
    exec: String,
}

#[derive(QueryableByName)]
struct Digest {
    #[diesel(sql_type = Text)]
    digest: String,
}

/// The arguments of `statement` for one seeded row.
///
/// A claim polls the two seeded queues as `$2`. Its `$5` and `$6` list no
/// activity, so no row skips its rate limit or its capability test. An
/// owner take names the execution of the row, not the row.
fn arguments(statement: &GuardedStatement, row: &Row) -> Vec<String> {
    let id = if statement.name.starts_with("take_by_execution") {
        &row.exec
    } else {
        &row.id
    };
    let claim = is_claim(statement.name);
    statement
        .param_types
        .iter()
        .enumerate()
        .map(|(at, ty)| match *ty {
            "uuid" => format!("'{id}'"),
            "uuid[]" => format!("ARRAY['{id}']::uuid[]"),
            "text[]" if claim && at == 1 => "ARRAY['held', 'open']".to_owned(),
            "text[]" if claim => "'{}'".to_owned(),
            other => argument(other).to_owned(),
        })
        .collect()
}

/// The rows that `sql` returns or changes for `row`. The transaction then
/// rolls back. An owner take runs with owner mode on.
///
/// A changed row shows its columns that a guard or a release sets. A time
/// column shows only whether it is NULL, because each run reads a new clock.
async fn effect_of(
    conn: &mut AsyncPgConnection,
    statement: &GuardedStatement,
    sql: &str,
    row: &Row,
) -> String {
    let types = statement.param_types.join(", ");
    let args = arguments(statement, row);
    let owner = if statement.name.starts_with("take_by_execution") {
        "UPDATE harvest_decision_owner_mode SET enabled = true;"
    } else {
        ""
    };
    let run = if sql.starts_with("SELECT") {
        "CREATE TEMP TABLE differential_out AS EXECUTE differential"
    } else {
        "EXECUTE differential"
    };
    conn.batch_execute(&format!(
        "BEGIN; {owner} PREPARE differential ({types}) AS {sql}; {run} ({});",
        args.join(", ")
    ))
    .await
    .unwrap_or_else(|e| panic!("{}: execute failed: {e}", statement.name));
    let query = if sql.starts_with("SELECT") {
        "SELECT coalesce(string_agg(r::text, ',' ORDER BY r::text), '') AS digest \
         FROM differential_out r"
    } else {
        "SELECT coalesce(string_agg(concat_ws('|', id, state, worker_id, attempt, \
             crash_strikes, capability_misses, capability_miss_workers, \
             capability_miss_handler, sticky_worker_id, wake_requested, error, \
             activity_name, started_at IS NULL, scheduled_at IS NULL, \
             last_heartbeat_at IS NULL, sticky_until IS NULL, timer_fires_at IS NULL), \
             ',' ORDER BY id), '') AS digest \
         FROM harvest_task_queue WHERE xmin = pg_current_xact_id()::xid"
    };
    let digest: Digest = diesel::sql_query(query)
        .get_result(conn)
        .await
        .expect("digest");
    conn.batch_execute("DEALLOCATE differential; ROLLBACK")
        .await
        .expect("roll back");
    digest.digest
}

/// The `CASE` form keeps a row exactly when the plain guard keeps it. Each
/// raw statement then returns or changes the same rows in both forms, for
/// every seeded row.
#[tokio::test]
async fn each_raw_statement_changes_the_same_rows_in_both_forms() {
    let (server, _guard) = setup_server().await;
    let mut db = Db::create(&server).await;
    assert_truth_table(&mut db.conn).await;
    seed_differential(&mut db.conn).await;
    let rows: Vec<Row> = diesel::sql_query(
        "SELECT id::text AS id, workflow_exec_id::text AS exec FROM harvest_task_queue",
    )
    .load(&mut db.conn)
    .await
    .expect("rows");
    for statement in guarded_statements().into_iter().filter(|s| s.raw) {
        let plain = plain_form(&statement.sql);
        assert_ne!(plain, statement.sql, "{} has a CASE guard", statement.name);
        assert!(!plain.contains(" THEN true END)"), "{plain}");
        let mut hits = 0;
        for row in &rows {
            let guarded = effect_of(&mut db.conn, &statement, &statement.sql, row).await;
            let unguarded = effect_of(&mut db.conn, &statement, &plain, row).await;
            assert_eq!(
                guarded, unguarded,
                "{} differs from its plain form for row {}",
                statement.name, row.id
            );
            if !guarded.is_empty() {
                hits += 1;
            }
        }
        assert!(hits > 0, "{}: the fixture matches no row", statement.name);
        assert!(
            hits < rows.len(),
            "{}: the fixture matches every row",
            statement.name
        );
    }
    db.drop().await;
}

/// The number of sessions of this database that wait on another session.
async fn blocked_sessions(conn: &mut AsyncPgConnection) -> i64 {
    #[derive(QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let count: Count = diesel::sql_query(
        "SELECT count(*) AS n FROM pg_stat_activity \
         WHERE datname = current_database() \
           AND cardinality(pg_blocking_pids(pid)) > 0",
    )
    .get_result(conn)
    .await
    .expect("blocked sessions");
    count.n
}

/// When a concurrent writer commits, against the release.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Order {
    /// The writer commits before the release starts.
    CommitsFirst,
    /// The writer holds the row lock, so the release waits for it.
    HoldsTheRow,
    /// The writer commits after the release ends.
    CommitsLast,
}

/// One concurrent writer beside one release.
struct Case {
    name: &'static str,
    release: &'static str,
    writer: &'static str,
    order: Order,
    want: usize,
}

/// Run `sql` as the release of `case` on a new claimed row. Return the
/// number of rows that the release changed.
async fn run_beside_writer(
    setup: &mut AsyncPgConnection,
    db_url: &str,
    case: &Case,
    sql: String,
) -> usize {
    let id = uuid::Uuid::new_v4();
    let queue = format!("q-{id}");
    setup
        .batch_execute(&format!(
            "INSERT INTO harvest_task_queue \
                 (id, queue_name, task_type, input, state, worker_id, started_at, attempt) \
             VALUES ('{id}', '{queue}', 'activity', '{{}}'::jsonb, 'RUNNING', \
                     'worker-7', now(), 1)"
        ))
        .await
        .expect("insert claimed row");
    let writer = case
        .writer
        .replace("$ID", &format!("'{id}'"))
        .replace("$QUEUE", &format!("'{queue}'"));

    let mut writer_conn = AsyncPgConnection::establish(db_url).await.expect("writer");
    writer_conn
        .batch_execute(&format!("BEGIN; {writer}"))
        .await
        .expect("writer runs");
    if case.order == Order::CommitsFirst {
        writer_conn.batch_execute("COMMIT").await.expect("commit");
    }

    let mut release_conn = AsyncPgConnection::establish(db_url).await.expect("release");
    let release = tokio::spawn(async move {
        diesel::sql_query(sql)
            .bind::<diesel::sql_types::Uuid, _>(id)
            .bind::<Text, _>("worker-7")
            .execute(&mut release_conn)
            .await
            .expect("release")
    });
    if case.order == Order::HoldsTheRow {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while blocked_sessions(setup).await == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "{}: the release never waited on the row",
                case.name
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        writer_conn.batch_execute("COMMIT").await.expect("commit");
    }
    let changed = tokio::time::timeout(std::time::Duration::from_secs(30), release)
        .await
        .unwrap_or_else(|_| panic!("{}: the release hangs", case.name))
        .expect("release task");
    if case.order == Order::CommitsLast {
        writer_conn.batch_execute("COMMIT").await.expect("commit");
    }
    changed
}

/// A release that runs beside a concurrent writer gives the same result in
/// both forms.
///
/// Postgres checks every condition of an `UPDATE` again on the new row
/// version, the CASE included. A completion or a reclaim that commits first
/// makes the release change nothing. A heartbeat keeps the claim, so the
/// release still applies. A pause is seen only when it commits before the
/// release starts. In production, `try_lock_queue_for_claim` orders a later
/// pause after the claim.
#[tokio::test]
async fn a_release_beside_a_completion_reclaim_or_pause_matches_the_plain_form() {
    use autumn_harvest::queue_pause::{release_claim_if_queue_paused_query, release_claim_query};

    let cases = [
        Case {
            name: "completion",
            release: release_claim_query(),
            writer: "UPDATE harvest_task_queue SET state = 'COMPLETED' WHERE id = $ID",
            order: Order::HoldsTheRow,
            want: 0,
        },
        Case {
            name: "reclaim",
            release: release_claim_query(),
            writer: "UPDATE harvest_task_queue SET worker_id = 'worker-8' WHERE id = $ID",
            order: Order::HoldsTheRow,
            want: 0,
        },
        Case {
            name: "heartbeat",
            release: release_claim_query(),
            writer: "UPDATE harvest_task_queue SET last_heartbeat_at = now() WHERE id = $ID",
            order: Order::HoldsTheRow,
            want: 1,
        },
        Case {
            name: "pause in flight during the release",
            release: release_claim_if_queue_paused_query(),
            writer: "INSERT INTO harvest_queue_pauses (queue_name, reason) VALUES ($QUEUE, 'test')",
            order: Order::CommitsLast,
            want: 0,
        },
        Case {
            name: "pause before the release starts",
            release: release_claim_if_queue_paused_query(),
            writer: "INSERT INTO harvest_queue_pauses (queue_name, reason) VALUES ($QUEUE, 'test')",
            order: Order::CommitsFirst,
            want: 1,
        },
    ];

    let (server, _guard) = setup_server().await;
    let db = Db::create(&server).await;
    let mut setup = AsyncPgConnection::establish(&db.url).await.expect("setup");
    for case in &cases {
        for sql in [case.release.to_owned(), plain_form(case.release)] {
            let changed = run_beside_writer(&mut setup, &db.url, case, sql).await;
            assert_eq!(
                changed, case.want,
                "{}: the release changed {changed} rows",
                case.name
            );
        }
    }
    drop(setup);
    db.drop().await;
}
