#![cfg(feature = "db")]
//! The guarded task row statements read the table through their key index.
//!
//! Each statement in `row_guard::guarded_statements` finds task rows by `id`
//! or by `workflow_exec_id`. Its guard on `state` and `worker_id` must not
//! let the planner choose a partial index. Two fixtures make the planner try:
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

use autumn_harvest::row_guard::{GuardedStatement, Key, guarded_statements};
use diesel::QueryableByName;
use diesel::sql_types::Text;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const ROWS: i64 = 100_000;
const WORKERS: i64 = 100;
/// The task rows of each execution in the plan fixtures.
const ROWS_PER_EXECUTION: i64 = 4;

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

/// The execution of task row `g`, as SQL.
fn execution_of_row() -> String {
    format!("md5('execution-' || (g / {ROWS_PER_EXECUTION}))::uuid")
}

/// Insert one execution for each `ROWS_PER_EXECUTION` task rows of `rows`.
async fn seed_executions(conn: &mut AsyncPgConnection, rows: i64) {
    conn.batch_execute(&format!(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, shard_id, input, state) \
         SELECT md5('execution-' || e)::uuid, 'w', 'wf-' || e, 0, '{{}}', 'RUNNING' \
         FROM generate_series(0, {rows} / {ROWS_PER_EXECUTION}) e \
         ON CONFLICT DO NOTHING"
    ))
    .await
    .expect("seed executions");
}

/// Insert `rows` rows. The first `running` are `RUNNING`, spread over the
/// workers. Of the rest, 2% are `PENDING`, and the others are terminal.
async fn seed(conn: &mut AsyncPgConnection, rows: i64, running: i64) {
    seed_executions(conn, rows).await;
    let execution = execution_of_row();
    conn.batch_execute(&format!(
        "INSERT INTO harvest_task_queue \
             (queue_name, task_type, input, state, worker_id, started_at, attempt, activity_name, \
              workflow_exec_id) \
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
                CASE WHEN g % 3 = 0 THEN NULL ELSE 'activity' END, \
                {execution} \
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

/// One scan of `harvest_task_queue` in a plan.
#[derive(Debug)]
struct Scan {
    alias: String,
    node: String,
    index: String,
    /// True when the scan tests an index condition. An index scan with no
    /// condition reads the whole index.
    keyed: bool,
}

/// Each scan of `harvest_task_queue` in `node`.
fn task_queue_scans(node: &serde_json::Value, out: &mut Vec<Scan>) {
    if node.get("Relation Name").and_then(|v| v.as_str()) == Some("harvest_task_queue")
        && node.get("Node Type").and_then(|v| v.as_str()) != Some("ModifyTable")
    {
        let text = |key: &str| {
            node.get(key)
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_owned()
        };
        let bitmap = text("Node Type") == "Bitmap Heap Scan";
        let index = if bitmap {
            bitmap_indexes(node).join(",")
        } else {
            text("Index Name")
        };
        out.push(Scan {
            alias: text("Alias"),
            node: text("Node Type"),
            index,
            keyed: bitmap || node.get("Index Cond").is_some(),
        });
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

/// A scan that may use another index. The concurrency cap recheck counts the
/// running rows of a key through the `recheck` alias, by design.
fn exempt(alias: &str) -> bool {
    alias == "recheck"
}

/// True when each checked scan in `plan` reads `index` with a condition.
fn reads_key_index(plan: &serde_json::Value, index: &str) -> (bool, Vec<Scan>) {
    let mut scans = Vec::new();
    task_queue_scans(&plan[0]["Plan"], &mut scans);
    scans.retain(|scan| !exempt(&scan.alias));
    let on_key = !scans.is_empty()
        && scans.iter().all(|scan| {
            (scan.node.starts_with("Index") || scan.node == "Bitmap Heap Scan")
                && scan.index == index
                && scan.keyed
        });
    (on_key, scans)
}

/// What the plan check found on one fixture.
#[derive(Default)]
struct KeyIndexReport {
    /// Each guarded statement that leaves its key index, as a message.
    failures: Vec<String>,
    /// The number of plain forms that leave their key index. A fixture that
    /// provokes no plain form tests nothing.
    plain_leaves: usize,
}

/// Each statement on `conn` that leaves its key index. Each raw statement
/// is also planned in its plain form, as a negative control.
async fn key_index_report(conn: &mut AsyncPgConnection, fixture: &str) -> KeyIndexReport {
    let mut report = KeyIndexReport::default();
    for statement in guarded_statements() {
        let index = statement.key.index();
        for (settings_name, settings) in SETTINGS {
            for cache_mode in ["force_generic_plan", "force_custom_plan"] {
                let plan = plan_of(conn, &statement, settings, cache_mode).await;
                let (on_key, scans) = reads_key_index(&plan, index);
                if !on_key {
                    report.failures.push(format!(
                        "{fixture}/{settings_name}/{cache_mode}: {} scans {scans:?}",
                        statement.name
                    ));
                }
                let unguarded = plain_form(&statement.sql);
                if statement.raw && unguarded != statement.sql {
                    let control = GuardedStatement {
                        sql: unguarded,
                        ..statement.clone()
                    };
                    let control_plan = plan_of(conn, &control, settings, cache_mode).await;
                    if !reads_key_index(&control_plan, index).0 {
                        report.plain_leaves += 1;
                    }
                }
            }
        }
    }
    report
}

#[tokio::test]
async fn guarded_statements_use_their_key_index_for_any_slot_count() {
    let (server, _guard) = setup_server().await;
    let mut db = Db::create(&server).await;
    let mut failures = Vec::new();
    let mut plain_leaves = 0;
    for per_worker in [5, 20, 50, 200] {
        db.conn
            .batch_execute("TRUNCATE harvest_task_queue CASCADE")
            .await
            .expect("empty the queue");
        seed(&mut db.conn, ROWS, per_worker * WORKERS).await;
        analyze(&mut db.conn).await;
        let fixture = format!("{per_worker} per worker");
        let report = key_index_report(&mut db.conn, &fixture).await;
        failures.extend(report.failures);
        plain_leaves += report.plain_leaves;
    }
    db.drop().await;
    assert!(plain_leaves > 0, "no plain form leaves its key index");
    assert!(
        failures.is_empty(),
        "these guarded statements leave their key index:\n{}",
        failures.join("\n")
    );
}

/// The statistics come from a table with no live row. The live rows arrive
/// after the analyze, as a load arrives after an idle period.
#[tokio::test]
async fn guarded_statements_use_their_key_index_after_an_idle_analyze() {
    let (server, _guard) = setup_server().await;
    let mut db = Db::create(&server).await;
    seed_executions(&mut db.conn, ROWS).await;
    let execution = execution_of_row();
    db.conn
        .batch_execute(&format!(
            "INSERT INTO harvest_task_queue \
                 (queue_name, task_type, input, state, attempt, workflow_exec_id) \
             SELECT 'default', 'workflow', '{{}}'::jsonb, 'COMPLETED', 1, {execution} \
             FROM generate_series(1, {ROWS}) g"
        ))
        .await
        .expect("seed terminal rows");
    analyze(&mut db.conn).await;
    seed(&mut db.conn, 2_000, 2_000).await;
    let KeyIndexReport {
        failures,
        plain_leaves,
    } = key_index_report(&mut db.conn, "idle statistics").await;
    db.drop().await;
    assert!(plain_leaves > 0, "no plain form leaves its key index");
    assert!(
        failures.is_empty(),
        "these guarded statements leave their key index:\n{}",
        failures.join("\n")
    );
}

// ── Equivalence: the CASE form keeps the same rows ─────────────────────────

/// `sql` with each `(CASE WHEN g THEN true END)` written back as `(g)`.
fn plain_form(sql: &str) -> String {
    const OPEN: &str = "(CASE WHEN ";
    const CLOSE: &str = " THEN true END)";
    let mut out = String::with_capacity(sql.len());
    let mut rest = sql;
    while let Some(at) = rest.find(OPEN) {
        let end = rest[at..].find(CLOSE).expect("a CASE guard closes") + at;
        out.push_str(&rest[..at]);
        out.push('(');
        out.push_str(&rest[at + OPEN.len()..end]);
        out.push(')');
        rest = &rest[end + CLOSE.len()..];
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
/// Each row has its own execution, so a by-execution statement reads one
/// row. A row with no worker is parked. A workflow row with one strike is a
/// mixed-signal suspension that is due later. A row with a cap has a
/// deadline, and a row with no strike has an unconsumed signal.
async fn seed_differential(conn: &mut AsyncPgConnection) {
    conn.batch_execute(
        "INSERT INTO harvest_queue_pauses (queue_name, reason) VALUES ('held', 'test'); \
         INSERT INTO harvest_activity_pauses (activity_name, reason) VALUES ('held', 'test'); \
         CREATE TEMP TABLE differential_rows AS \
         SELECT gen_random_uuid() AS e, s, w, k, q, a, c, cap \
         FROM unnest(ARRAY['RUNNING', 'PENDING', 'FAILED']) s, \
              unnest(ARRAY['worker-7', 'worker-8', NULL]) w, \
              unnest(ARRAY['workflow', 'activity']) k, \
              unnest(ARRAY['held', 'open']) q, \
              unnest(ARRAY['held', 'open']) a, \
              unnest(ARRAY[0, 1]) c, \
              unnest(ARRAY[true, false]) cap; \
         INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, shard_id, input, state) \
         SELECT e, 'w', e::text, 0, '{}', \
                CASE WHEN a = 'held' THEN 'PAUSED' ELSE 'RUNNING' END \
         FROM differential_rows; \
         INSERT INTO harvest_task_queue \
             (queue_name, task_type, input, state, worker_id, started_at, attempt, \
              crash_strikes, activity_name, concurrency_key, concurrency_cap, \
              workflow_exec_id, scheduled_at, schedule_to_close_at) \
         SELECT q, k, '{}'::jsonb, s, w, CASE WHEN w IS NOT NULL THEN now() END, 1, c, \
                CASE WHEN k = 'activity' THEN a \
                     WHEN c = 1 THEN 'mixed_signal_suspension' END, \
                CASE WHEN cap THEN 'key' END, CASE WHEN cap THEN 0 END, \
                e, \
                CASE WHEN c = 1 THEN now() + INTERVAL '1 hour' ELSE now() END, \
                CASE WHEN cap THEN now() + INTERVAL '1 hour' END \
         FROM differential_rows; \
         INSERT INTO harvest_signals (workflow_exec_id, signal_name, payload) \
         SELECT e, 'nudge', '{}'::jsonb FROM differential_rows WHERE c = 0",
    )
    .await
    .expect("seed differential rows");
}

#[derive(QueryableByName)]
struct Digest {
    #[diesel(sql_type = Text)]
    digest: String,
}

/// The rows that `sql` returns or changes for task `id`. The transaction then
/// rolls back.
///
/// A changed row shows its columns that a guard or a release sets. A time
/// column shows only whether it is NULL, because each run reads a new clock.
async fn effect_of(
    conn: &mut AsyncPgConnection,
    statement: &GuardedStatement,
    sql: &str,
    id: &str,
) -> String {
    let types = statement.param_types.join(", ");
    let args: Vec<String> = statement
        .param_types
        .iter()
        .map(|ty| match *ty {
            "uuid" => format!("'{id}'"),
            "uuid[]" => format!("ARRAY['{id}']::uuid[]"),
            other => argument(other).to_owned(),
        })
        .collect();
    let run = if sql.starts_with("SELECT") {
        "CREATE TEMP TABLE differential_out AS EXECUTE differential"
    } else {
        "EXECUTE differential"
    };
    conn.batch_execute(&format!(
        "BEGIN; PREPARE differential ({types}) AS {sql}; {run} ({});",
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
    #[derive(QueryableByName)]
    struct Id {
        #[diesel(sql_type = Text)]
        id: String,
    }
    let (server, _guard) = setup_server().await;
    let mut db = Db::create(&server).await;
    assert_truth_table(&mut db.conn).await;
    seed_differential(&mut db.conn).await;
    let mut keys = Vec::new();
    for column in ["id", "workflow_exec_id"] {
        let ids: Vec<Id> = diesel::sql_query(format!(
            "SELECT {column}::text AS id FROM harvest_task_queue"
        ))
        .load(&mut db.conn)
        .await
        .expect("ids");
        keys.push(ids);
    }
    for statement in guarded_statements().into_iter().filter(|s| s.raw) {
        let ids = match statement.key {
            Key::Id => &keys[0],
            Key::Execution | Key::Workflow | Key::Activity => &keys[1],
        };
        let plain = plain_form(&statement.sql);
        assert_ne!(plain, statement.sql, "{} has a CASE guard", statement.name);
        assert!(!plain.contains(" THEN true END)"), "{plain}");
        let mut hits = 0;
        for Id { id } in ids {
            let guarded = effect_of(&mut db.conn, &statement, &statement.sql, id).await;
            let unguarded = effect_of(&mut db.conn, &statement, &plain, id).await;
            assert_eq!(
                guarded, unguarded,
                "{} differs from its plain form for row {id}",
                statement.name
            );
            if !guarded.is_empty() {
                hits += 1;
            }
        }
        assert!(hits > 0, "{}: the fixture matches no row", statement.name);
        assert!(
            hits < ids.len(),
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
    release: String,
    writer: &'static str,
    order: Order,
    want: usize,
}

/// `sql` with each `$n` replaced by `value`. A `$n` followed by a digit is a
/// different parameter, so it stays.
fn with_parameter(sql: &str, n: usize, value: &str) -> String {
    let marker = format!("${n}");
    let mut out = String::with_capacity(sql.len());
    let mut rest = sql;
    while let Some(at) = rest.find(&marker) {
        let after = &rest[at + marker.len()..];
        out.push_str(&rest[..at]);
        if after.starts_with(|c: char| c.is_ascii_digit()) {
            out.push_str(&marker);
        } else {
            out.push_str(value);
        }
        rest = after;
    }
    out.push_str(rest);
    out
}

/// The raw guarded statement named `name`.
fn guarded(name: &str) -> GuardedStatement {
    guarded_statements()
        .into_iter()
        .find(|s| s.name == name && s.raw)
        .unwrap_or_else(|| panic!("no raw guarded statement {name}"))
}

/// Run `statement` with `sql` as the release of `case` on a new claimed
/// workflow task row. Return the number of rows that the release changed.
///
/// A by-id release binds the row id and `worker-7`. A by-execution release
/// puts the execution id and `now()` into the SQL text for `$1` and `$2`.
async fn run_beside_writer(
    setup: &mut AsyncPgConnection,
    db_url: &str,
    case: &Case,
    statement: &GuardedStatement,
    sql: String,
) -> usize {
    let id = uuid::Uuid::new_v4();
    let execution = uuid::Uuid::new_v4();
    let queue = format!("q-{id}");
    setup
        .batch_execute(&format!(
            "INSERT INTO harvest_workflow_executions \
                 (id, workflow_name, workflow_id, shard_id, input, state) \
             VALUES ('{execution}', 'w', '{execution}', 0, '{{}}', 'RUNNING'); \
             INSERT INTO harvest_task_queue \
                 (id, queue_name, task_type, input, state, worker_id, started_at, attempt, \
                  workflow_exec_id) \
             VALUES ('{id}', '{queue}', 'workflow', '{{}}'::jsonb, 'RUNNING', \
                     'worker-7', now(), 1, '{execution}')"
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
    let key = statement.key;
    let release = tokio::spawn(async move {
        match key {
            Key::Id => diesel::sql_query(sql)
                .bind::<diesel::sql_types::Uuid, _>(id)
                .bind::<Text, _>("worker-7")
                .execute(&mut release_conn)
                .await
                .expect("release"),
            Key::Execution | Key::Workflow | Key::Activity => diesel::sql_query(with_parameter(
                &with_parameter(&sql, 2, "now()"),
                1,
                &format!("'{execution}'::uuid"),
            ))
            .execute(&mut release_conn)
            .await
            .expect("release"),
        }
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
///
/// The wake cases run by execution. A wake fallback that waits on a park
/// sees the parked row, which has no worker, so it changes nothing. The
/// wake then re-pends the parked row.
#[tokio::test]
async fn a_release_beside_a_completion_reclaim_or_pause_matches_the_plain_form() {
    use autumn_harvest::queue_pause::{release_claim_if_queue_paused_query, release_claim_query};

    const PARK: &str = "UPDATE harvest_task_queue SET worker_id = NULL, started_at = NULL \
                        WHERE id = $ID";
    let cases = [
        Case {
            name: "completion",
            release: release_claim_query().to_owned(),
            writer: "UPDATE harvest_task_queue SET state = 'COMPLETED' WHERE id = $ID",
            order: Order::HoldsTheRow,
            want: 0,
        },
        Case {
            name: "reclaim",
            release: release_claim_query().to_owned(),
            writer: "UPDATE harvest_task_queue SET worker_id = 'worker-8' WHERE id = $ID",
            order: Order::HoldsTheRow,
            want: 0,
        },
        Case {
            name: "heartbeat",
            release: release_claim_query().to_owned(),
            writer: "UPDATE harvest_task_queue SET last_heartbeat_at = now() WHERE id = $ID",
            order: Order::HoldsTheRow,
            want: 1,
        },
        Case {
            name: "pause in flight during the release",
            release: release_claim_if_queue_paused_query().to_owned(),
            writer: "INSERT INTO harvest_queue_pauses (queue_name, reason) VALUES ($QUEUE, 'test')",
            order: Order::CommitsLast,
            want: 0,
        },
        Case {
            name: "pause before the release starts",
            release: release_claim_if_queue_paused_query().to_owned(),
            writer: "INSERT INTO harvest_queue_pauses (queue_name, reason) VALUES ($QUEUE, 'test')",
            order: Order::CommitsFirst,
            want: 1,
        },
        Case {
            name: "park while a wake fallback waits",
            release: guarded("wake_fallback").sql,
            writer: PARK,
            order: Order::HoldsTheRow,
            want: 0,
        },
        Case {
            name: "heartbeat while a wake fallback waits",
            release: guarded("wake_fallback").sql,
            writer: "UPDATE harvest_task_queue SET last_heartbeat_at = now() WHERE id = $ID",
            order: Order::HoldsTheRow,
            want: 1,
        },
        Case {
            name: "park before a wake re-pend",
            release: guarded("wake_repend").sql,
            writer: PARK,
            order: Order::CommitsFirst,
            want: 1,
        },
        Case {
            name: "park after a wake re-pend",
            release: guarded("wake_repend").sql,
            writer: PARK,
            order: Order::CommitsLast,
            want: 0,
        },
    ];

    let (server, _guard) = setup_server().await;
    let db = Db::create(&server).await;
    let mut setup = AsyncPgConnection::establish(&db.url).await.expect("setup");
    for case in &cases {
        let statement = guarded_statements()
            .into_iter()
            .find(|s| s.raw && s.sql == case.release)
            .unwrap_or_else(|| panic!("{}: no raw guarded statement has this SQL", case.name));
        for sql in [case.release.clone(), plain_form(&case.release)] {
            let changed = run_beside_writer(&mut setup, &db.url, case, &statement, sql).await;
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
