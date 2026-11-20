#![cfg(feature = "db")]
//! The by-id task row statements read the table through its primary key.
//!
//! Each statement in `row_guard::guarded_statements` finds one row by `id`.
//! Its guard on `state` and `worker_id` must not let the planner choose a
//! partial index. Two fixtures make the planner try:
//!
//! - Grouped: 100,000 rows, and 5 to 200 `RUNNING` rows for each of 100
//!   workers. `running_worker_idx` then costs less than the primary key.
//! - Idle: the statistics come from a table with no live row. Each live
//!   partial index then looks empty.
//!
//! Each fixture gets its own fresh, migrated database.

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

async fn create_fresh_db(admin_url: &str) -> AsyncPgConnection {
    let name = format!("row_guard_{}", uuid::Uuid::new_v4().simple());
    let mut admin = AsyncPgConnection::establish(admin_url)
        .await
        .expect("connect to admin database");
    admin
        .batch_execute(&format!("CREATE DATABASE \"{name}\""))
        .await
        .expect("create database");
    let (prefix, _) = admin_url.rsplit_once('/').expect("url has a db segment");
    let mut conn = AsyncPgConnection::establish(&format!("{prefix}/{name}"))
        .await
        .expect("connect to fresh database");
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("apply migration bundle");
    conn.batch_execute("ALTER TABLE harvest_task_queue SET (autovacuum_enabled = false)")
        .await
        .expect("turn off autovacuum");
    conn
}

/// Insert `rows` rows. The first `running` are `RUNNING`, spread over the
/// workers. The rest are terminal or `PENDING`.
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

/// A fixture with `per_worker` `RUNNING` rows for each worker.
async fn grouped_fixture(admin_url: &str, per_worker: i64) -> AsyncPgConnection {
    let mut conn = create_fresh_db(admin_url).await;
    seed(&mut conn, ROWS, per_worker * WORKERS).await;
    analyze(&mut conn).await;
    conn
}

/// A fixture whose statistics come from a table with no live row. The live
/// rows arrive after the analyze, as a load arrives after an idle period.
async fn idle_fixture(admin_url: &str) -> AsyncPgConnection {
    let mut conn = create_fresh_db(admin_url).await;
    conn.batch_execute(&format!(
        "INSERT INTO harvest_task_queue (queue_name, task_type, input, state, attempt) \
         SELECT 'default', 'workflow', '{{}}'::jsonb, 'COMPLETED', 1 \
         FROM generate_series(1, {ROWS})"
    ))
    .await
    .expect("seed terminal rows");
    analyze(&mut conn).await;
    seed(&mut conn, 2_000, 2_000).await;
    conn
}

/// A literal for a parameter of `ty`. A generic plan does not read it.
fn argument(ty: &str) -> &'static str {
    match ty {
        "uuid" => "'00000000-0000-0000-0000-000000000000'",
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
        out.push((text("Alias"), text("Node Type"), text("Index Name")));
    }
    if let Some(children) = node.get("Plans").and_then(|v| v.as_array()) {
        for child in children {
            task_queue_scans(child, out);
        }
    }
}

/// A scan that may use another index. The concurrency cap recheck counts the
/// running rows of a key through the `recheck` alias, by design.
fn exempt(alias: &str) -> bool {
    alias == "recheck"
}

/// Every statement on `conn` reads its target row through the primary key.
async fn assert_primary_key_plans(conn: &mut AsyncPgConnection, fixture: &str) {
    let mut failures = Vec::new();
    for statement in guarded_statements() {
        for (settings_name, settings) in SETTINGS {
            for cache_mode in ["force_generic_plan", "force_custom_plan"] {
                let plan = plan_of(conn, &statement, settings, cache_mode).await;
                let mut scans = Vec::new();
                task_queue_scans(&plan[0]["Plan"], &mut scans);
                let checked: Vec<_> = scans.iter().filter(|(a, _, _)| !exempt(a)).collect();
                let on_pkey = !checked.is_empty()
                    && checked.iter().all(|(_, node, index)| {
                        node.starts_with("Index") && index == "harvest_task_queue_pkey"
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
    assert!(
        failures.is_empty(),
        "these by-id statements leave the primary key:\n{}",
        failures.join("\n")
    );
}

#[tokio::test]
async fn by_id_guards_use_the_primary_key_for_any_slot_count() {
    let (url, _guard) = setup_server().await;
    for per_worker in [5, 20, 50, 200] {
        let mut conn = grouped_fixture(&url, per_worker).await;
        assert_primary_key_plans(&mut conn, &format!("{per_worker} per worker")).await;
    }
}

#[tokio::test]
async fn by_id_guards_use_the_primary_key_after_an_idle_analyze() {
    let (url, _guard) = setup_server().await;
    let mut conn = idle_fixture(&url).await;
    assert_primary_key_plans(&mut conn, "idle statistics").await;
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
#[tokio::test]
async fn the_case_form_keeps_a_row_exactly_when_the_guard_is_true() {
    let (url, _guard) = setup_server().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let verdicts: Vec<Verdict> = diesel::sql_query(
        "SELECT ((a AND (CASE WHEN g THEN true END)) IS TRUE) \
                  = ((a AND g) IS TRUE) AS same, \
                format('a=%s g=%s', a, g) AS case \
         FROM (VALUES (true), (false), (NULL::boolean)) AS x(a), \
              (VALUES (true), (false), (NULL::boolean)) AS y(g)",
    )
    .load(&mut conn)
    .await
    .expect("truth table");
    assert_eq!(verdicts.len(), 9, "every pair of truth values");
    for verdict in verdicts {
        assert!(verdict.same, "the forms differ for {}", verdict.case);
    }
}

/// Seed rows for the differential test: every mix of state, worker, kind,
/// queue, activity, cap and strike count that a guard reads.
async fn seed_differential(conn: &mut AsyncPgConnection) {
    conn.batch_execute(
        "INSERT INTO harvest_queue_pauses (queue_name, reason) VALUES ('held', 'test'); \
         INSERT INTO harvest_activity_pauses (activity_name, reason) VALUES ('held', 'test'); \
         INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, shard_id, input, state) \
         VALUES ('00000000-0000-0000-0000-0000000000a1', 'w', 'held', 0, '{}', 'PAUSED'), \
                ('00000000-0000-0000-0000-0000000000a2', 'w', 'open', 0, '{}', 'RUNNING'); \
         INSERT INTO harvest_task_queue \
             (queue_name, task_type, input, state, worker_id, started_at, attempt, \
              crash_strikes, activity_name, concurrency_key, concurrency_cap, \
              workflow_exec_id) \
         SELECT q, k, '{}'::jsonb, s, w, now(), 1, c, \
                CASE WHEN k = 'activity' THEN a END, \
                CASE WHEN cap THEN 'key' END, CASE WHEN cap THEN 0 END, \
                CASE WHEN a = 'held' THEN '00000000-0000-0000-0000-0000000000a1'::uuid \
                     ELSE '00000000-0000-0000-0000-0000000000a2'::uuid END \
         FROM unnest(ARRAY['RUNNING', 'PENDING', 'FAILED']) s, \
              unnest(ARRAY['worker-7', 'worker-8', NULL]) w, \
              unnest(ARRAY['workflow', 'activity']) k, \
              unnest(ARRAY['held', 'open']) q, \
              unnest(ARRAY['held', 'open']) a, \
              unnest(ARRAY[0, 1]) c, \
              unnest(ARRAY[true, false]) cap",
    )
    .await
    .expect("seed differential rows");
}

#[derive(QueryableByName)]
struct Digest {
    #[diesel(sql_type = Text)]
    digest: String,
}

/// The rows that `sql` returns or changes for task `id`, rolled back after.
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

/// Each raw statement returns or changes the same rows in the CASE form and
/// in the plain form, for every seeded row.
#[tokio::test]
async fn each_raw_statement_changes_the_same_rows_in_both_forms() {
    #[derive(QueryableByName)]
    struct Id {
        #[diesel(sql_type = Text)]
        id: String,
    }
    let (url, _guard) = setup_server().await;
    let mut conn = create_fresh_db(&url).await;
    seed_differential(&mut conn).await;
    let ids: Vec<Id> = diesel::sql_query("SELECT id::text AS id FROM harvest_task_queue")
        .load(&mut conn)
        .await
        .expect("ids");
    let mut matched = 0;
    for statement in guarded_statements().into_iter().filter(|s| s.raw) {
        let plain = plain_form(&statement.sql);
        assert_ne!(plain, statement.sql, "{} has a CASE guard", statement.name);
        assert!(!plain.contains("CASE WHEN state"), "{plain}");
        let mut hits = 0;
        for Id { id } in &ids {
            let guarded = effect_of(&mut conn, &statement, &statement.sql, id).await;
            let unguarded = effect_of(&mut conn, &statement, &plain, id).await;
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
        matched += hits;
    }
    assert!(matched > 0);
}

/// The number of sessions that wait on a lock.
async fn lock_waiters(conn: &mut AsyncPgConnection) -> i64 {
    #[derive(QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let count: Count = diesel::sql_query(
        "SELECT count(*) AS n FROM pg_stat_activity \
         WHERE wait_event_type = 'Lock' AND datname = current_database()",
    )
    .get_result(conn)
    .await
    .expect("lock waiters");
    count.n
}

/// A release that runs beside a concurrent writer gives the same result in
/// both forms.
///
/// Postgres checks every condition of an `UPDATE` again on the new row
/// version, the CASE included. A completion or a reclaim that commits first
/// makes the release change nothing. A heartbeat keeps the claim, so the
/// release still applies. A pause is seen only when it commits before the
/// release starts. The queue lock covers a pause that commits later.
#[tokio::test]
async fn a_release_beside_a_completion_reclaim_or_pause_matches_the_plain_form() {
    use autumn_harvest::queue_pause::{release_claim_if_queue_paused_query, release_claim_query};

    /// When the writer commits, against the release.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Order {
        /// The writer commits before the release starts.
        CommitsFirst,
        /// The writer holds the row lock, so the release waits for it.
        HoldsTheRow,
        /// The writer commits after the release ends.
        CommitsLast,
    }
    struct Case {
        name: &'static str,
        release: &'static str,
        writer: &'static str,
        order: Order,
        want: usize,
    }
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
            name: "pause after the release starts",
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

    let (url, _guard) = setup_server().await;
    let mut setup = create_fresh_db(&url).await;
    let db_url = {
        #[derive(QueryableByName)]
        struct Name {
            #[diesel(sql_type = Text)]
            name: String,
        }
        let name: Name = diesel::sql_query("SELECT current_database() AS name")
            .get_result(&mut setup)
            .await
            .expect("database name");
        let (prefix, _) = url.rsplit_once('/').expect("url has a db segment");
        format!("{prefix}/{}", name.name)
    };

    for case in cases {
        for sql in [case.release.to_owned(), plain_form(case.release)] {
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

            let mut holder = AsyncPgConnection::establish(&db_url).await.expect("holder");
            holder
                .batch_execute(&format!("BEGIN; {writer}"))
                .await
                .expect("writer runs");
            if case.order == Order::CommitsFirst {
                holder
                    .batch_execute("COMMIT")
                    .await
                    .expect("writer commits");
            }

            let mut releaser = AsyncPgConnection::establish(&db_url)
                .await
                .expect("releaser");
            let waiting = tokio::spawn(async move {
                diesel::sql_query(sql)
                    .bind::<diesel::sql_types::Uuid, _>(id)
                    .bind::<Text, _>("worker-7")
                    .execute(&mut releaser)
                    .await
                    .expect("release")
            });
            if case.order == Order::HoldsTheRow {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
                while lock_waiters(&mut setup).await == 0 {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "{}: the release never waited on the row",
                        case.name
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                holder
                    .batch_execute("COMMIT")
                    .await
                    .expect("writer commits");
            }
            let released = waiting.await.expect("release task");
            if case.order == Order::CommitsLast {
                holder
                    .batch_execute("COMMIT")
                    .await
                    .expect("writer commits");
            }
            assert_eq!(
                released, case.want,
                "{}: the release changed {released} rows",
                case.name
            );
        }
    }
}
