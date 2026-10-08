//! Seeded deep-backlog fixture generator (issue #1956).
//!
//! The fixture is a production-shaped `harvest_task_queue`:
//!
//! * At least 1M live task rows at Ledger scale ([`LEDGER_LIVE_ROWS`]).
//! * Skewed queues and concurrency keys. A power transform of a uniform draw,
//!   `idx = floor(n * u^k)`, gives rank 0 a share of `(1/n)^(1/k)`.
//! * A dead-tuple ratio from real `UPDATE` and `DELETE` churn.
//!
//! Every value comes from `md5` of the seed, a tag and a row ordinal. The SQL
//! calls no volatile function, so one seed gives one fixture. Rows go in
//! hash order, so related rows spread across heap pages as in production.
//!
//! Autovacuum is off on the fixture tables. Otherwise it can remove the dead
//! tuples while a measurement runs. The fixture is a snapshot of a table
//! that autovacuum has not reached yet.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Text};
use diesel_async::{AsyncConnection, AsyncPgConnection, SimpleAsyncConnection};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

use autumn_harvest::queue::{self, EnqueueParams, TaskType};

use super::pg_stats_snapshot::{self as stats, Statements, StatsSnapshot};
use super::throwaway_db::ThrowawayDb;

/// Live task rows at Ledger scale. Issue #1956 asks for at least 1M.
pub const LEDGER_LIVE_ROWS: u64 = 1_000_000;

/// Live task rows of the shallow control run. The issue #1956 e2e profile
/// drained queues of about this depth.
pub const SHALLOW_LIVE_ROWS: u64 = 4_000;

/// The largest fixture [`FixtureSpec::validate`] accepts. It keeps every
/// ordinal well inside `bigint` and every count inside `u64` arithmetic.
pub const MAX_LIVE_ROWS: u64 = 1_000_000_000;

/// Prefix of every name the fixture writes.
pub const PREFIX: &str = "deep-backlog";

/// Prefix of every database the fixture creates.
const DB_PREFIX: &str = "harvest_deep_backlog";

/// The fixed origin of every seeded timestamp. A fixed origin keeps the rows
/// deterministic. It is in the past, so a `PENDING` row is due.
const EPOCH: &str = "TIMESTAMPTZ '2026-01-01 00:00:00+00'";

/// The `scheduled_at` of a row that waits for a retry backoff or a timer.
const FAR_FUTURE: &str = "TIMESTAMPTZ '2100-01-01 00:00:00+00'";

/// Distinct activity names, picked with a mild skew.
const ACTIVITY_NAMES: u64 = 8;

/// How long [`FixtureServer::start`] may take to start a container.
const CONTAINER_START_BOUND: Duration = Duration::from_secs(240);

/// The shape of one fixture. [`FixtureSpec::ledger`] gives the defaults.
#[derive(Debug, Clone, PartialEq)]
pub struct FixtureSpec {
    /// Every value in the fixture derives from this seed.
    pub seed: u64,
    /// Task rows left after the churn.
    pub live_rows: u64,
    /// Task rows per workflow execution. One of them is the workflow task.
    pub tasks_per_execution: u64,
    /// Distinct queues. An execution and its tasks share one queue.
    pub queues: u64,
    /// Power-skew exponent of the queue pick. `1.0` is uniform.
    pub queue_skew: f64,
    /// Distinct concurrency keys, as in a multi-tenant deployment.
    pub keys: u64,
    /// Power-skew exponent of the key pick. A large value makes a hot tenant.
    pub key_skew: f64,
    /// Share of executions whose tasks carry a concurrency key.
    pub keyed_share: f64,
    /// `concurrency_cap` on every keyed row.
    pub key_cap: i32,
    /// Target `n_dead_tup / (n_live_tup + n_dead_tup)` on the task queue.
    pub dead_ratio: f64,
    /// Share of live rows that are `RUNNING`.
    pub running_share: f64,
    /// Share of live rows that are terminal and wait for the hygiene sweep.
    pub terminal_share: f64,
    /// Share of live rows that are `PENDING` but due in the future.
    pub future_share: f64,
    /// Rows in `harvest_workers`. The claim reads them on every call.
    pub workers: u64,
}

/// The churn that makes the dead tuples.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Churn {
    /// Live `PENDING` rows that a worker claims and releases:
    /// `PENDING -> RUNNING -> PENDING`. Each leaves two dead versions.
    pub released: u64,
    /// Extra rows that run, complete and go to the hygiene sweep:
    /// `PENDING -> RUNNING -> COMPLETED -> deleted`. Each leaves three.
    pub reclaimed: u64,
}

impl Churn {
    /// Dead tuples the churn leaves.
    #[must_use]
    pub const fn dead_tuples(&self) -> u64 {
        2 * self.released + 3 * self.reclaimed
    }
}

/// Task slots per seeded worker, as `harvest_workers.max_concurrency`.
pub const WORKER_SLOTS: i32 = 16;

impl FixtureSpec {
    /// The Ledger defaults at [`LEDGER_LIVE_ROWS`].
    ///
    /// The dead ratio is 10%. The hygiene migration sets the autovacuum scale
    /// factor of the task queue to 2%. So a table that autovacuum keeps up
    /// with stays near 2%. A table at 10% models autovacuum lag, for example
    /// behind a long transaction that holds the xmin horizon.
    #[must_use]
    pub const fn ledger(seed: u64) -> Self {
        Self {
            seed,
            live_rows: LEDGER_LIVE_ROWS,
            tasks_per_execution: 4,
            queues: 64,
            queue_skew: 3.0,
            keys: 4096,
            key_skew: 4.0,
            keyed_share: 0.5,
            key_cap: 32,
            dead_ratio: 0.10,
            running_share: 0.01,
            terminal_share: 0.04,
            future_share: 0.05,
            workers: 64,
        }
    }

    /// The same shape with `live_rows` live rows.
    #[must_use]
    pub const fn at_scale(mut self, live_rows: u64) -> Self {
        self.live_rows = live_rows;
        self
    }

    /// Check that the spec can be seeded.
    ///
    /// # Errors
    /// Returns the first field that is out of range.
    pub fn validate(&self) -> Result<(), String> {
        let unit = |name: &str, v: f64| {
            if (0.0..=1.0).contains(&v) {
                Ok(())
            } else {
                Err(format!("{name} = {v} is not in [0, 1]"))
            }
        };
        for (name, v) in [
            ("live_rows", self.live_rows),
            ("tasks_per_execution", self.tasks_per_execution),
            ("queues", self.queues),
            ("keys", self.keys),
            ("workers", self.workers),
        ] {
            if v == 0 {
                return Err(format!("{name} must be at least 1"));
            }
        }
        if self.live_rows > MAX_LIVE_ROWS {
            return Err(format!(
                "live_rows = {} is above the {MAX_LIVE_ROWS} cap",
                self.live_rows
            ));
        }
        for (name, v) in [("queue_skew", self.queue_skew), ("key_skew", self.key_skew)] {
            if !v.is_finite() || v < 1.0 {
                return Err(format!("{name} = {v} must be a finite value of at least 1"));
            }
        }
        if !(0.0..1.0).contains(&self.dead_ratio) {
            return Err(format!("dead_ratio = {} is not in [0, 1)", self.dead_ratio));
        }
        unit("keyed_share", self.keyed_share)?;
        unit("running_share", self.running_share)?;
        unit("terminal_share", self.terminal_share)?;
        unit("future_share", self.future_share)?;
        let states = self.running_share + self.terminal_share + self.future_share;
        if states > 1.0 {
            return Err(format!("the state shares sum to {states}, above 1"));
        }
        if self.key_cap < 1 {
            return Err(format!("key_cap = {} must be at least 1", self.key_cap));
        }
        Ok(())
    }

    /// Workflow executions that own the live rows.
    #[must_use]
    pub const fn executions(&self) -> u64 {
        self.live_rows.div_ceil(self.tasks_per_execution)
    }

    /// The churn that gives [`Self::dead_ratio`].
    ///
    /// The churn follows the task lifecycle, so every update changes `state`.
    /// About half the dead tuples come from released claims, and the rest
    /// from reclaimed tasks.
    #[must_use]
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    pub fn churn(&self) -> Churn {
        let dead = (self.live_rows as f64 * self.dead_ratio / (1.0 - self.dead_ratio)).round();
        let dead = dead as u64;
        let released = (dead / 4).min(self.live_rows);
        let reclaimed = ((dead - 2 * released) as f64 / 3.0).round() as u64;
        Churn {
            released,
            reclaimed,
        }
    }

    /// Task slots of the seeded fleet.
    #[must_use]
    #[allow(clippy::cast_sign_loss)]
    pub const fn fleet_slots(&self) -> u64 {
        self.workers * WORKER_SLOTS as u64
    }

    /// Every queue name, head queue first.
    #[must_use]
    pub fn queue_names(&self) -> Vec<String> {
        (0..self.queues)
            .map(|i| format!("{PREFIX}-q-{i}"))
            .collect()
    }

    /// A uniform draw in `[0, 1)` from the seed, a tag and an ordinal.
    fn uniform(&self, tag: &str, ordinal: &str) -> String {
        format!(
            "(('x' || substr(md5('{}:{tag}:' || ({ordinal})::text), 1, 8))::bit(32)::bigint::float8 \
             / 4294967296.0)",
            self.seed
        )
    }

    /// A rank in `[0, n)` with power skew `k`. Rank 0 is the hottest.
    fn skewed(&self, tag: &str, ordinal: &str, n: u64, k: f64) -> String {
        format!(
            "LEAST(floor({n} * power({u}, {k:?}))::bigint, {last})",
            u = self.uniform(tag, ordinal),
            last = n - 1
        )
    }

    /// A deterministic UUID from the seed, a tag and an ordinal.
    fn uuid(&self, tag: &str, ordinal: &str) -> String {
        format!("md5('{}:{tag}:' || ({ordinal})::text)::uuid", self.seed)
    }

    /// The seed script in three phases. A pure function of the spec.
    ///
    /// The churn runs in one transaction, so no statement in it prunes the
    /// dead tuples of another.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn seed_script(&self) -> SeedScript {
        let live = self.live_rows;
        let churn = self.churn();
        let released = churn.released;
        let execs = self.executions();
        let tpe = self.tasks_per_execution;
        let total = live + churn.reclaimed;
        // The execution of task ordinal `i`. A reclaimed row belongs to the
        // execution of a live row, as an earlier task of the same run.
        let exec_of = format!(
            "(CASE WHEN i < {live} THEN i / {tpe} ELSE ((i - {live}) / {tpe}) % {execs} END)"
        );
        let queue = self.skewed("queue", "e", self.queues, self.queue_skew);
        let key = self.skewed("key", "e", self.keys, self.key_skew);
        let keyed = format!("{} < {:?}", self.uniform("keyed", "e"), self.keyed_share);
        let activity = self.skewed("activity", "i", ACTIVITY_NAMES, 2.0);
        let state_u = self.uniform("state", "i");
        let future_u = self.uniform("future", "i");
        let prio_u = self.uniform("priority", "i");
        let running = self.running_share;
        let terminal = running + self.terminal_share;
        // Among PENDING rows, the share that is due in the future.
        let pending_share = 1.0 - terminal;
        let future = if pending_share > 0.0 {
            self.future_share / pending_share
        } else {
            0.0
        };
        let exec_id = self.uuid("exec", "e");
        let task_id = self.uuid("task", "i");
        let activity_id = self.uuid("activity-id", "i");
        let workers = self.workers;
        let fleet = self.fleet_slots();
        let cap = self.key_cap;
        let seed = self.seed;
        let at_i = format!("{EPOCH} + i * INTERVAL '1 millisecond'");
        let seq = "(input->>'seq')::bigint";
        let load = vec![
            "TRUNCATE harvest_task_queue, harvest_workflow_executions, harvest_workers CASCADE"
                .to_string(),
            "ALTER TABLE harvest_task_queue SET (autovacuum_enabled = false)".to_string(),
            "ALTER TABLE harvest_workflow_executions SET (autovacuum_enabled = false)".to_string(),
            format!(
                "INSERT INTO harvest_workers \
                   (worker_id, max_concurrency, host, build_id, queues, labels, started_at, \
                    last_heartbeat_at) \
                 SELECT '{PREFIX}-worker-' || i, {WORKER_SLOTS}, '{PREFIX}-host', '', '[]'::jsonb, \
                        '{{}}'::jsonb, {EPOCH}, {EPOCH} \
                 FROM generate_series(0, {last}) AS s(i)",
                last = workers - 1
            ),
            format!(
                "INSERT INTO harvest_workflow_executions \
                   (id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name, \
                    started_at, created_at) \
                 SELECT {exec_id}, '{PREFIX}-wf', '{PREFIX}-' || e, {run_id}, 0, 'RUNNING', \
                        '{{}}'::jsonb, '{PREFIX}-q-' || {queue}, \
                        {EPOCH} + e * INTERVAL '1 millisecond', \
                        {EPOCH} + e * INTERVAL '1 millisecond' \
                 FROM generate_series(0, {last}) AS s(e) \
                 ORDER BY md5('{seed}:exec-order:' || e::text)",
                run_id = self.uuid("run", "e"),
                last = execs - 1,
            ),
            // Layer `b` draws each row. Layer `k` ranks the RUNNING draws of
            // one key and task type, and `f` keeps at most `cap` of them. Layer
            // `r` ranks the rest, and `t` keeps at most one fleet of them. A
            // draw past a cap stays PENDING, as a claim the gate refuses.
            format!(
                "INSERT INTO harvest_task_queue \
                   (id, queue_name, task_type, workflow_exec_id, activity_name, activity_id, input, \
                    state, priority, worker_id, attempt, max_attempts, scheduled_at, started_at, \
                    completed_at, last_heartbeat_at, concurrency_key, concurrency_cap, created_at) \
                 SELECT {task_id}, '{PREFIX}-q-' || {queue}, ttype, {exec_id}, \
                        CASE WHEN ttype = 'activity' THEN '{PREFIX}-activity-' || {activity} END, \
                        CASE WHEN ttype = 'activity' THEN {activity_id} END, \
                        jsonb_build_object('seq', i, 'pad', repeat('x', 96)), \
                        st, \
                        CASE WHEN {prio_u} < 0.9 THEN 0 ELSE 1 + (i % 9)::int END, \
                        CASE WHEN st = 'RUNNING' \
                             THEN '{PREFIX}-worker-' || ((run_rank - 1) % {workers}) END, \
                        CASE WHEN st = 'PENDING' THEN 0 ELSE 1 END, 3, \
                        CASE WHEN st = 'PENDING' AND i < {live} AND i >= {released} \
                                  AND {future_u} < {future:?} THEN {FAR_FUTURE} \
                             ELSE {at_i} END, \
                        CASE WHEN st <> 'PENDING' THEN {at_i} END, \
                        CASE WHEN st IN ('COMPLETED', 'FAILED') THEN {at_i} END, \
                        CASE WHEN st = 'RUNNING' THEN {at_i} END, \
                        ckey, \
                        CASE WHEN ckey IS NOT NULL THEN {cap} END, \
                        {at_i} \
                 FROM ( \
                   SELECT r.*, \
                          CASE WHEN may_run AND run_rank <= {fleet} THEN 'RUNNING' \
                               WHEN base = 'RUNNING' THEN 'PENDING' \
                               ELSE base END AS st \
                   FROM ( \
                     SELECT f.*, row_number() OVER (PARTITION BY may_run ORDER BY i) AS run_rank \
                     FROM ( \
                       SELECT k.*, (base = 'RUNNING' AND (ckey IS NULL OR key_rank <= {cap})) AS may_run \
                       FROM ( \
                         SELECT b.*, \
                                row_number() OVER (PARTITION BY ckey, ttype, base ORDER BY i) AS key_rank \
                         FROM ( \
                           SELECT i, e, \
                                  CASE WHEN i < {live} AND i % {tpe} = 0 \
                                       THEN 'workflow' ELSE 'activity' END AS ttype, \
                                  CASE WHEN {keyed} THEN '{PREFIX}-k-' || {key} END AS ckey, \
                                  CASE WHEN i >= {live} OR i < {released} THEN 'PENDING' \
                                       WHEN {state_u} < {running:?} THEN 'RUNNING' \
                                       WHEN {state_u} < {terminal:?} \
                                         THEN CASE WHEN i % 5 = 0 THEN 'FAILED' ELSE 'COMPLETED' END \
                                       ELSE 'PENDING' END AS base \
                           FROM generate_series(0, {last}) AS s(i), \
                                LATERAL (SELECT {exec_of} AS e) AS x \
                         ) AS b \
                       ) AS k \
                     ) AS f \
                   ) AS r \
                 ) AS t \
                 ORDER BY md5('{seed}:task-order:' || i::text)",
                last = total - 1,
            ),
        ];
        let churn = vec![format!(
            "BEGIN; \
             UPDATE harvest_task_queue \
                SET state = 'RUNNING', worker_id = '{PREFIX}-worker-0', started_at = {EPOCH}, \
                    attempt = attempt + 1 \
              WHERE {seq} < {released}; \
             UPDATE harvest_task_queue \
                SET state = 'PENDING', worker_id = NULL, started_at = NULL, \
                    error = '{PREFIX}: released by a worker' \
              WHERE {seq} < {released}; \
             UPDATE harvest_task_queue \
                SET state = 'RUNNING', worker_id = '{PREFIX}-worker-0', started_at = {EPOCH}, \
                    attempt = 1 \
              WHERE {seq} >= {live}; \
             UPDATE harvest_task_queue SET state = 'COMPLETED', completed_at = {EPOCH} \
              WHERE {seq} >= {live}; \
             DELETE FROM harvest_task_queue WHERE {seq} >= {live}; \
             COMMIT"
        )];
        let analyze = vec![
            "ANALYZE harvest_task_queue".to_string(),
            "ANALYZE harvest_workflow_executions".to_string(),
            "ANALYZE harvest_workers".to_string(),
        ];
        SeedScript {
            load,
            churn,
            analyze,
        }
    }
}

/// The seed script of [`FixtureSpec::seed_script`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeedScript {
    /// Create the rows. Every row is live after this phase.
    pub load: Vec<String>,
    /// Make the dead tuples, in one transaction.
    pub churn: Vec<String>,
    /// Refresh the planner statistics.
    pub analyze: Vec<String>,
}

impl SeedScript {
    /// Every statement, in the order [`seed`] runs them.
    pub fn statements(&self) -> impl Iterator<Item = &String> {
        self.load.iter().chain(&self.churn).chain(&self.analyze)
    }
}

/// The share of rank 0 under power skew `k` over `n` ranks: `(1/n)^(1/k)`.
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn head_share(n: u64, k: f64) -> f64 {
    (1.0 / n as f64).powf(1.0 / k)
}

/// The share of `rank` under power skew `k` over `n` ranks.
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn rank_share(rank: u64, n: u64, k: f64) -> f64 {
    let cdf = |r: u64| (r as f64 / n as f64).powf(1.0 / k);
    cdf(rank + 1) - cdf(rank)
}

/// What [`seed`] wrote.
#[derive(Debug, Clone)]
pub struct SeedReport {
    pub executions: u64,
    pub churn: Churn,
    pub elapsed: Duration,
    /// The shape right after the seed, before any reader prunes a page.
    pub shape: FixtureShape,
    /// `pg_stat_user_tables` right after the seed.
    pub tables: Vec<stats::TableStats>,
}

/// The measured shape of a seeded fixture.
#[derive(Debug, Clone)]
pub struct FixtureShape {
    pub live_rows: u64,
    /// Rows per queue, largest first.
    pub queue_counts: Vec<(String, i64)>,
    /// The share of keyed rows that carry the hottest key.
    pub top_key_share: f64,
    pub keyed_rows: u64,
    /// Rows per state, largest first.
    pub states: Vec<(String, i64)>,
    /// `pg_stat_user_tables.n_live_tup` of the task queue.
    pub n_live_tup: i64,
    /// `pg_stat_user_tables.n_dead_tup` of the task queue.
    pub n_dead_tup: i64,
    pub heap_bytes: i64,
    /// The most `RUNNING` rows of one concurrency key and task type.
    pub max_running_per_key: i64,
    /// The most `RUNNING` rows of one worker.
    pub max_running_per_worker: i64,
}

impl FixtureShape {
    /// The share of live rows in the queue at `rank`, largest first.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn queue_share(&self, rank: usize) -> f64 {
        self.queue_counts
            .get(rank)
            .map_or(0.0, |(_, n)| *n as f64 / self.live_rows.max(1) as f64)
    }

    /// `n_dead_tup / (n_live_tup + n_dead_tup)`, as autovacuum sees it.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn dead_ratio(&self) -> f64 {
        let all = self.n_live_tup + self.n_dead_tup;
        if all == 0 {
            0.0
        } else {
            self.n_dead_tup as f64 / all as f64
        }
    }

    /// Live rows in `state`.
    #[must_use]
    pub fn state_count(&self, state: &str) -> i64 {
        self.states
            .iter()
            .find(|(s, _)| s == state)
            .map_or(0, |(_, n)| *n)
    }
}

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

#[derive(QueryableByName)]
struct KeyCount {
    #[diesel(sql_type = Text)]
    k: String,
    #[diesel(sql_type = BigInt)]
    n: i64,
}

#[derive(QueryableByName)]
struct TextRow {
    #[diesel(sql_type = Text)]
    t: String,
}

/// Connect to `url`.
///
/// # Panics
/// Panics when the server is unreachable.
pub async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url)
        .await
        .unwrap_or_else(|e| panic!("connect to the fixture database: {e}"))
}

/// Seed `spec` into the database of `conn`. The database must be migrated.
///
/// The census runs between the load and the churn. A read after the churn
/// prunes pages, and pruning lowers `n_dead_tup`. So `seed` reads the table
/// stats once, right after `ANALYZE`, before any scan of the heap.
///
/// # Panics
/// Panics when the spec is invalid or a statement fails.
pub async fn seed(conn: &mut AsyncPgConnection, spec: &FixtureSpec) -> SeedReport {
    spec.validate()
        .unwrap_or_else(|e| panic!("invalid fixture spec: {e}"));
    let started = Instant::now();
    let script = spec.seed_script();
    run_all(conn, &script.load).await;
    let census = census(conn, spec.live_rows).await;
    run_all(conn, &script.churn).await;
    // `ANALYZE` must see the churn counters, or it counts them twice.
    stats::flush_counters(conn).await;
    run_all(conn, &script.analyze).await;
    let tables = stats::read_table_stats(conn)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let tq = tables
        .iter()
        .find(|t| t.relname == "harvest_task_queue")
        .expect("pg_stat_user_tables lists harvest_task_queue")
        .clone();
    let heap_bytes = count(conn, "SELECT pg_relation_size('harvest_task_queue') AS n").await;
    SeedReport {
        executions: spec.executions(),
        churn: spec.churn(),
        elapsed: started.elapsed(),
        shape: FixtureShape {
            n_live_tup: tq.n_live_tup,
            n_dead_tup: tq.n_dead_tup,
            heap_bytes,
            ..census
        },
        tables,
    }
}

async fn run_all(conn: &mut AsyncPgConnection, statements: &[String]) {
    for sql in statements {
        conn.batch_execute(sql)
            .await
            .unwrap_or_else(|e| panic!("fixture SQL failed: {e}\n--- sql ---\n{sql}"));
    }
}

async fn count(conn: &mut AsyncPgConnection, sql: &str) -> i64 {
    use diesel_async::RunQueryDsl;
    diesel::sql_query(sql)
        .get_result::<CountRow>(conn)
        .await
        .unwrap_or_else(|e| panic!("fixture count failed: {e}\n--- sql ---\n{sql}"))
        .n
}

async fn grouped(conn: &mut AsyncPgConnection, sql: &str) -> Vec<(String, i64)> {
    use diesel_async::RunQueryDsl;
    diesel::sql_query(sql)
        .load::<KeyCount>(conn)
        .await
        .unwrap_or_else(|e| panic!("fixture group failed: {e}\n--- sql ---\n{sql}"))
        .into_iter()
        .map(|row| (row.k, row.n))
        .collect()
}

/// Count the rows that stay live after the churn. The table stats stay zero
/// here, because [`seed`] reads them later.
#[allow(clippy::cast_sign_loss, clippy::cast_precision_loss)]
async fn census(conn: &mut AsyncPgConnection, live: u64) -> FixtureShape {
    let live_filter = format!("(input->>'seq')::bigint < {live}");
    let live_rows = count(
        conn,
        &format!("SELECT COUNT(*) AS n FROM harvest_task_queue WHERE {live_filter}"),
    )
    .await as u64;
    let queue_counts = grouped(
        conn,
        &format!(
            "SELECT queue_name AS k, COUNT(*) AS n FROM harvest_task_queue WHERE {live_filter} \
             GROUP BY queue_name ORDER BY n DESC, k"
        ),
    )
    .await;
    let keys = grouped(
        conn,
        &format!(
            "SELECT concurrency_key AS k, COUNT(*) AS n FROM harvest_task_queue \
             WHERE {live_filter} AND concurrency_key IS NOT NULL \
             GROUP BY concurrency_key ORDER BY n DESC, k LIMIT 1"
        ),
    )
    .await;
    let keyed_rows = count(
        conn,
        &format!(
            "SELECT COUNT(*) AS n FROM harvest_task_queue \
             WHERE {live_filter} AND concurrency_key IS NOT NULL"
        ),
    )
    .await as u64;
    let top_key_share =
        <[_]>::first(&keys).map_or(0.0, |(_, n)| *n as f64 / keyed_rows.max(1) as f64);
    let states = grouped(
        conn,
        &format!(
            "SELECT state AS k, COUNT(*) AS n FROM harvest_task_queue WHERE {live_filter} \
             GROUP BY state ORDER BY n DESC, k"
        ),
    )
    .await;
    let max_running_per_key = count(
        conn,
        &format!(
            "SELECT COALESCE(MAX(n), 0) AS n FROM ( \
               SELECT COUNT(*) AS n FROM harvest_task_queue \
               WHERE {live_filter} AND state = 'RUNNING' AND concurrency_key IS NOT NULL \
               GROUP BY concurrency_key, task_type) AS k"
        ),
    )
    .await;
    let max_running_per_worker = count(
        conn,
        &format!(
            "SELECT COALESCE(MAX(n), 0) AS n FROM ( \
               SELECT COUNT(*) AS n FROM harvest_task_queue \
               WHERE {live_filter} AND state = 'RUNNING' GROUP BY worker_id) AS w"
        ),
    )
    .await;
    FixtureShape {
        live_rows,
        queue_counts,
        top_key_share,
        keyed_rows,
        states,
        n_live_tup: 0,
        n_dead_tup: 0,
        heap_bytes: 0,
        max_running_per_key,
        max_running_per_worker,
    }
}

/// Rows in `harvest_task_queue` now.
///
/// # Panics
/// Panics when the query fails.
#[allow(clippy::cast_sign_loss)]
pub async fn live_task_rows(conn: &mut AsyncPgConnection) -> u64 {
    count(conn, "SELECT COUNT(*) AS n FROM harvest_task_queue").await as u64
}

/// An `md5` over the seeded content and the physical row order.
///
/// The hash leaves out columns with a server default, because the seed does
/// not write them.
///
/// # Panics
/// Panics when the query fails.
pub async fn fingerprint(conn: &mut AsyncPgConnection) -> String {
    use diesel_async::RunQueryDsl;
    diesel::sql_query(
        "SELECT md5(
           (SELECT string_agg(
              (id, queue_name, task_type, workflow_exec_id, activity_name, activity_id, input,
               state, priority, worker_id, attempt, max_attempts, scheduled_at, started_at,
               completed_at, last_heartbeat_at, concurrency_key, concurrency_cap, error,
               created_at)::text, E'\\n' ORDER BY ctid)
            FROM harvest_task_queue)
           || (SELECT string_agg(
                 (id, workflow_name, workflow_id, run_id, state, queue_name, started_at)::text,
                 E'\\n' ORDER BY ctid)
               FROM harvest_workflow_executions)
         ) AS t",
    )
    .get_result::<TextRow>(conn)
    .await
    .unwrap_or_else(|e| panic!("fixture fingerprint failed: {e}"))
    .t
}

/// A Postgres server for fixture databases.
///
/// With `HARVEST_TEST_DATABASE_URL` set, that URL is an admin URL. Otherwise
/// the server is a `postgres:16` container with `pg_stat_statements`
/// preloaded.
pub struct FixtureServer {
    admin_url: String,
    _container: Option<ContainerAsync<Postgres>>,
}

impl FixtureServer {
    /// Use the env server, or start a container.
    ///
    /// # Errors
    /// Returns why no server is available. A caller skips on an error.
    pub async fn start() -> Result<Self, String> {
        if let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
            return Ok(Self {
                admin_url,
                _container: None,
            });
        }
        let start = Postgres::default()
            .with_tag("16")
            // `with_cmd` replaces the image command, so `fsync=off` is repeated.
            .with_cmd([
                "-c",
                "shared_preload_libraries=pg_stat_statements",
                "-c",
                "fsync=off",
            ])
            .start();
        let container = tokio::time::timeout(CONTAINER_START_BOUND, start)
            .await
            .map_err(|_| "the postgres:16 container did not start in time".to_string())?
            .map_err(|e| format!("no Docker daemon and HARVEST_TEST_DATABASE_URL unset ({e})"))?;
        let host = container
            .get_host()
            .await
            .map_err(|e| format!("container host: {e}"))?;
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .map_err(|e| format!("container port: {e}"))?;
        Ok(Self {
            admin_url: format!("postgres://postgres:postgres@{host}:{port}/postgres"),
            _container: Some(container),
        })
    }

    /// Create and migrate a fresh database.
    pub async fn create_database(&self) -> FixtureDb {
        FixtureDb {
            db: ThrowawayDb::create_on(&self.admin_url, DB_PREFIX).await,
        }
    }

    /// Whether a database named `name` exists on this server.
    ///
    /// # Panics
    /// Panics when the server is unreachable.
    pub async fn database_exists(&self, name: &str) -> bool {
        use diesel_async::RunQueryDsl;
        let mut admin = connect(&self.admin_url).await;
        diesel::sql_query("SELECT COUNT(*) AS n FROM pg_database WHERE datname = $1")
            .bind::<Text, _>(name)
            .get_result::<CountRow>(&mut admin)
            .await
            .unwrap_or_else(|e| panic!("look up database {name}: {e}"))
            .n
            > 0
    }
}

/// A fixture database. [`Self::snapshot_and_drop`] reads the stats views and
/// then drops it. On an unwind, the inner guard drops it with no snapshot.
pub struct FixtureDb {
    db: ThrowawayDb,
}

impl FixtureDb {
    #[must_use]
    pub fn url(&self) -> String {
        self.db.url()
    }

    #[must_use]
    pub fn name(&self) -> &str {
        self.db.name()
    }

    /// Capture both stats views, then drop the database.
    ///
    /// The method takes `self`, so no caller can drop first and read later.
    /// Close every pool on the database before the call.
    pub async fn snapshot_and_drop(self) -> StatsSnapshot {
        let mut conn = connect(&self.db.url()).await;
        let snapshot = stats::capture(&mut conn).await;
        drop(conn);
        drop(self.db);
        snapshot
    }
}

/// The claim workload a Ledger run drives.
#[derive(Debug, Clone)]
pub struct WorkloadConfig {
    /// Concurrent claimers, each on its own connection.
    pub claimers: usize,
    /// Stop after this many claims.
    pub max_claims: u64,
    /// Start no claim after this wall-clock bound, even below `max_claims`.
    /// A claim in flight at the bound runs to its end.
    pub budget: Duration,
}

impl WorkloadConfig {
    /// The Ledger workload for `spec`. `HARVEST_DEEP_BACKLOG_SECS` sets the
    /// budget.
    ///
    /// The claim cap scales with the table: one claim per 200 live rows, and
    /// at least 20. Each cycle leaves about three dead versions, and
    /// autovacuum is off. So a run adds at most 1.5 dead versions per 100
    /// live rows, and a shallow and a deep run keep a similar dead ratio.
    #[must_use]
    pub fn ledger(spec: &FixtureSpec) -> Self {
        Self {
            claimers: 4,
            max_claims: (spec.live_rows / 200).max(20),
            budget: Duration::from_secs(env_u64("HARVEST_DEEP_BACKLOG_SECS", 600)),
        }
    }
}

/// What [`drive_claims`] did.
#[derive(Debug, Clone, Default)]
pub struct WorkloadReport {
    pub claims: u64,
    pub completions: u64,
    /// Completed tasks deleted, as the hygiene sweep would.
    pub reclaims: u64,
    pub enqueues: u64,
    pub empty_polls: u64,
    pub errors: u64,
    pub first_error: Option<String>,
    pub elapsed: Duration,
}

#[derive(Default)]
struct Tally {
    /// Claim slots taken: claims made plus claims in flight.
    slots: AtomicU64,
    claims: AtomicU64,
    completions: AtomicU64,
    reclaims: AtomicU64,
    enqueues: AtomicU64,
    empty_polls: AtomicU64,
    errors: AtomicU64,
    first_error: std::sync::Mutex<Option<String>>,
}

impl Tally {
    fn error(&self, what: &str, e: &dyn std::fmt::Display) {
        self.errors.fetch_add(1, Ordering::Relaxed);
        let mut first = self.first_error.lock().expect("poisoned");
        if first.is_none() {
            *first = Some(format!("{what}: {e}"));
        }
    }
}

/// Failed calls in a row after which a claimer stops. Any call that succeeds
/// resets the count. A broken connection fails every call, so more retries
/// only use up the budget.
const MAX_CONSECUTIVE_ERRORS: u32 = 20;

/// The pause after a failed claim, so a failing claimer does not spin.
const ERROR_BACKOFF: Duration = Duration::from_millis(100);

/// Reserve one of `max` claim slots. Returns `false` when all are taken.
#[must_use]
pub fn try_reserve(slots: &AtomicU64, max: u64) -> bool {
    let mut taken = slots.load(Ordering::Acquire);
    while taken < max {
        match slots.compare_exchange_weak(taken, taken + 1, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return true,
            Err(now) => taken = now,
        }
    }
    false
}

/// Give back a slot that [`try_reserve`] took, after a claim found nothing.
pub fn release(slots: &AtomicU64) {
    slots.fetch_sub(1, Ordering::AcqRel);
}

/// The bound on one claim. A claim on a 1M-row backlog takes about 17 s.
///
/// The budget stops new claims only. A claim in flight at the budget runs to
/// its end. Dropping it would not stop the server, which keeps the session
/// open with unflushed counters and makes the snapshot partial.
const CLAIM_BOUND: Duration = Duration::from_secs(300);

/// The bound on each step after a claim: complete, delete and enqueue. Each
/// step touches one row, so the bound is far above its normal cost.
const CYCLE_STEP_BOUND: Duration = Duration::from_secs(60);

/// Run `fut` for at most `bound`. Returns `None` at the bound.
async fn bounded<T>(bound: Duration, fut: impl std::future::Future<Output = T>) -> Option<T> {
    tokio::time::timeout(bound, fut).await.ok()
}

/// Delete the completed task `id`, as the hygiene sweep does later.
///
/// With the replacement, this keeps the table at its seeded depth. Without
/// it, each claim leaves a terminal row, and a shallow table grows by half
/// over its run. The statement shows in `pg_stat_statements` as the only one
/// that the engine does not issue.
async fn reclaim(conn: &mut AsyncPgConnection, id: uuid::Uuid) -> Result<(), String> {
    use diesel_async::RunQueryDsl;
    diesel::sql_query("DELETE FROM harvest_task_queue WHERE id = $1 AND state = 'COMPLETED'")
        .bind::<diesel::sql_types::Uuid, _>(id)
        .execute(conn)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// A replacement for a completed task, so the backlog keeps its depth.
///
/// The replacement is always an activity task of the same execution, queue,
/// key and priority. In the engine, a workflow task that completes schedules
/// activities. It does not enqueue a second workflow task.
fn replacement(task: &autumn_harvest::models::TaskQueueItem) -> EnqueueParams {
    let mut params = EnqueueParams::new(
        task.queue_name.clone(),
        TaskType::Activity,
        serde_json::json!({ "replaces": task.id }),
    );
    params.workflow_exec_id = task.workflow_exec_id;
    params.activity_name = Some(
        task.activity_name
            .clone()
            .unwrap_or_else(|| format!("{PREFIX}-activity-0")),
    );
    params.priority = task.priority;
    params.concurrency_key.clone_from(&task.concurrency_key);
    params.max_concurrent = task.concurrency_cap.and_then(|cap| u32::try_from(cap).ok());
    params
}

/// One claimer of [`drive_claims`]: claim, complete, delete and replace, until
/// the deadline or `max` claims in all.
async fn run_claimer(
    conn: &mut AsyncPgConnection,
    queues: &[String],
    worker: &str,
    deadline: Instant,
    max: u64,
    tally: &Tally,
) {
    let mut consecutive_errors = 0;
    // A slot is reserved before the claim starts, so concurrent claimers
    // never pass `max` in all. A claim that takes no task gives its slot back.
    while Instant::now() < deadline && try_reserve(&tally.slots, max) {
        let claim = queue::claim_task(conn, queues, worker, "", None, &[], &[]);
        let Some(claimed) = bounded(CLAIM_BOUND, claim).await else {
            release(&tally.slots);
            tally.error("claim", &"no result within the claim bound");
            break;
        };
        let task = match claimed {
            Ok(Some(task)) => {
                consecutive_errors = 0;
                task
            }
            Ok(None) => {
                release(&tally.slots);
                tally.empty_polls.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
            Err(e) => {
                release(&tally.slots);
                tally.error("claim", &e);
                consecutive_errors += 1;
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                    break;
                }
                tokio::time::sleep(ERROR_BACKOFF).await;
                continue;
            }
        };
        tally.claims.fetch_add(1, Ordering::Relaxed);
        // A claimed task finishes its cycle even past the deadline. A cycle
        // cut halfway would change the table depth that the run measures.
        let complete = queue::complete_task(conn, task.id, serde_json::json!({}));
        match bounded(CYCLE_STEP_BOUND, complete).await {
            Some(Ok(())) => {
                tally.completions.fetch_add(1, Ordering::Relaxed);
            }
            Some(Err(e)) => {
                tally.error("complete", &e);
                consecutive_errors += 1;
            }
            None => tally.error("complete", &"no result within the step bound"),
        }
        match bounded(CYCLE_STEP_BOUND, reclaim(conn, task.id)).await {
            Some(Ok(())) => {
                tally.reclaims.fetch_add(1, Ordering::Relaxed);
            }
            Some(Err(e)) => {
                tally.error("reclaim", &e);
                consecutive_errors += 1;
            }
            None => tally.error("reclaim", &"no result within the step bound"),
        }
        let params = replacement(&task);
        match bounded(CYCLE_STEP_BOUND, queue::enqueue(conn, &params)).await {
            Some(Ok(_)) => {
                consecutive_errors = 0;
                tally.enqueues.fetch_add(1, Ordering::Relaxed);
            }
            Some(Err(e)) => {
                tally.error("enqueue", &e);
                consecutive_errors += 1;
            }
            None => tally.error("enqueue", &"no result within the step bound"),
        }
        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
            break;
        }
    }
}

/// Drive the engine claim path against the fixture at `url`.
///
/// Each claimer claims with [`queue::claim_task`] over every queue, completes
/// the task, deletes it, and enqueues a replacement. Every measured statement
/// but the delete comes from the engine, and the table keeps its depth.
///
/// # Panics
/// Panics when a claimer cannot connect.
pub async fn drive_claims(
    url: &str,
    spec: &FixtureSpec,
    config: &WorkloadConfig,
) -> WorkloadReport {
    let tally = Arc::new(Tally::default());
    let queues = Arc::new(spec.queue_names());
    let started = Instant::now();
    let deadline = started
        .checked_add(config.budget)
        .expect("the workload budget fits in an Instant");
    let mut handles = Vec::new();
    for n in 0..config.claimers {
        let tally = Arc::clone(&tally);
        let queues = Arc::clone(&queues);
        let url = url.to_string();
        let max = config.max_claims;
        handles.push(tokio::spawn(async move {
            let mut conn = connect(&url).await;
            let worker = format!("{PREFIX}-worker-{n}");
            run_claimer(&mut conn, &queues, &worker, deadline, max, &tally).await;
        }));
    }
    for handle in handles {
        handle.await.expect("a claimer task panicked");
    }
    let first_error = tally.first_error.lock().expect("poisoned").clone();
    WorkloadReport {
        claims: tally.claims.load(Ordering::Relaxed),
        completions: tally.completions.load(Ordering::Relaxed),
        reclaims: tally.reclaims.load(Ordering::Relaxed),
        enqueues: tally.enqueues.load(Ordering::Relaxed),
        empty_polls: tally.empty_polls.load(Ordering::Relaxed),
        errors: tally.errors.load(Ordering::Relaxed),
        first_error,
        elapsed: started.elapsed(),
    }
}

/// A staging directory for one Ledger capture.
///
/// The capture writes its artifacts here. [`Self::publish`] moves them into
/// the target only after the run passes every check. A stage that drops
/// without a publish removes itself, so a failed run never replaces part of
/// the committed evidence.
#[derive(Debug)]
pub struct StagedArtifacts {
    staging: PathBuf,
    target: PathBuf,
}

impl StagedArtifacts {
    /// Make a new staging directory inside `target`.
    ///
    /// # Errors
    /// Returns the I/O error of the directory create.
    pub fn new(target: &Path) -> std::io::Result<Self> {
        // Inside the target, so each move is a rename on one file system.
        let staging = target.join(format!(".staging-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&staging)?;
        Ok(Self {
            staging,
            target: target.to_path_buf(),
        })
    }

    /// The directory that the capture writes to.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.staging
    }

    /// Move every staged file over its target, then remove the stage.
    ///
    /// The set moves whole or not at all. Each target must be a file or
    /// absent, so a bad target fails before any move. Each old file goes
    /// aside into the stage before its new file moves in. When a move fails,
    /// the new files go back and the old files return.
    ///
    /// # Errors
    /// Returns the error of the check or of the first move that fails.
    pub fn publish(self) -> std::io::Result<Vec<PathBuf>> {
        let mut names: Vec<std::ffi::OsString> = std::fs::read_dir(&self.staging)?
            .map(|entry| entry.map(|e| e.file_name()))
            .collect::<std::io::Result<_>>()?;
        names.sort();
        for name in &names {
            let to = self.target.join(name);
            if to.symlink_metadata().is_ok_and(|m| !m.is_file()) {
                return Err(std::io::Error::other(format!(
                    "{} is not a file, so the capture cannot replace it",
                    to.display()
                )));
            }
        }
        let previous = self.staging.join(".previous");
        std::fs::create_dir(&previous)?;
        let mut set_aside = Vec::new();
        let mut moved = Vec::new();
        let mut result = Ok(());
        for name in &names {
            let to = self.target.join(name);
            if to.symlink_metadata().is_ok() {
                result = std::fs::rename(&to, previous.join(name));
                if result.is_err() {
                    break;
                }
                set_aside.push(name);
            }
            result = std::fs::rename(self.staging.join(name), &to);
            if result.is_err() {
                break;
            }
            moved.push(name);
        }
        if let Err(e) = result {
            for name in moved.iter().rev() {
                let _ = std::fs::rename(self.target.join(name), self.staging.join(name));
            }
            for name in set_aside.iter().rev() {
                let _ = std::fs::rename(previous.join(name), self.target.join(name));
            }
            return Err(e);
        }
        Ok(names.iter().map(|name| self.target.join(name)).collect())
    }
}

impl Drop for StagedArtifacts {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.staging);
    }
}

/// Where the Ledger capture writes. `HARVEST_DEEP_BACKLOG_OUT` overrides the
/// default, `docs/perf-artifacts/deep-backlog`.
#[must_use]
pub fn artifact_dir() -> PathBuf {
    std::env::var_os("HARVEST_DEEP_BACKLOG_OUT")
        .filter(|v| !v.is_empty())
        .map_or_else(
            || {
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("..")
                    .join("docs/perf-artifacts/deep-backlog")
            },
            PathBuf::from,
        )
}

/// The `u64` in env var `name`, or `default` when it is unset or blank.
///
/// # Panics
/// Panics when the value is not a `u64`. A typo in a knob must not silently
/// run a different experiment.
#[must_use]
pub fn env_u64(name: &str, default: u64) -> u64 {
    match std::env::var(name) {
        Ok(v) if !v.trim().is_empty() => v
            .trim()
            .parse()
            .unwrap_or_else(|e| panic!("{name}={v:?} is not a whole number: {e}")),
        _ => default,
    }
}

#[derive(QueryableByName)]
struct ExplainRow {
    #[diesel(sql_type = Text, column_name = "QUERY PLAN")]
    query_plan: String,
}

/// `EXPLAIN (ANALYZE, BUFFERS, SETTINGS)` of one engine claim over every queue.
///
/// The SQL is [`queue::claim_task_query`], the statement [`queue::claim_task`]
/// runs, with its six binds as literals. `EXPLAIN ANALYZE` runs the claim,
/// so it runs in a transaction that rolls back. The plan names the node that
/// costs most, which `pg_stat_statements` cannot do.
///
/// # Panics
/// Panics when the claim query grows a seventh bind, so the literals no
/// longer match it.
pub async fn explain_claim(conn: &mut AsyncPgConnection, spec: &FixtureSpec) -> String {
    use diesel_async::RunQueryDsl;
    let queue_list = spec
        .queue_names()
        .iter()
        .map(|q| format!("'{q}'"))
        .collect::<Vec<_>>()
        .join(",");
    let literals = [
        format!("'{PREFIX}-worker-0'"),
        format!("ARRAY[{queue_list}]::text[]"),
        "''".to_string(),
        "NULL".to_string(),
        "ARRAY[]::text[]".to_string(),
        "ARRAY[]::text[]".to_string(),
    ];
    let raw = queue::claim_task_query();
    assert!(
        !raw.contains(&format!("${}", literals.len() + 1)),
        "claim_task_query() has more than {} binds; extend the literals",
        literals.len()
    );
    // Replace `$6` before `$1`, so `$1` cannot match the front of `$10`.
    let mut sql = raw.to_string();
    for (i, literal) in literals.iter().enumerate().rev() {
        sql = sql.replace(&format!("${}", i + 1), literal);
    }
    let _ = conn.batch_execute("BEGIN").await;
    let plan = diesel::sql_query(format!("EXPLAIN (ANALYZE, BUFFERS, SETTINGS) {sql}"))
        .load::<ExplainRow>(conn)
        .await;
    let _ = conn.batch_execute("ROLLBACK").await;
    plan.map_or_else(
        |e| format!("EXPLAIN failed: {e}"),
        |rows| {
            rows.into_iter()
                .map(|r| r.query_plan)
                .collect::<Vec<_>>()
                .join("\n")
        },
    )
}

/// The server version and the settings that move claim cost.
///
/// JIT matters most. The claim plan costs more than `jit_above_cost` on a deep
/// backlog, and the planner plans each claim again.
async fn server_settings(conn: &mut AsyncPgConnection) -> String {
    use diesel_async::RunQueryDsl;
    diesel::sql_query(
        "SELECT format('%s; jit=%s, shared_buffers=%s, work_mem=%s, max_parallel_workers_per_gather=%s', \
                version(), current_setting('jit'), current_setting('shared_buffers'), \
                current_setting('work_mem'), current_setting('max_parallel_workers_per_gather')) AS t",
    )
    .get_result::<TextRow>(conn)
    .await
    .map_or_else(|e| format!("unknown ({e})"), |row| row.t)
}

/// Seed `spec`, drive `workload`, snapshot, drop, and write the artifacts.
///
/// One function, so a reader sees the whole run in order. It is long because
/// the summary has many lines.
///
/// Returns the summary text for `fixture-summary.txt` and the workload
/// report, so the caller can reject a run that measured nothing.
///
/// # Panics
/// Panics when seeding fails or an artifact cannot be written.
#[allow(clippy::cast_precision_loss)]
#[allow(clippy::too_many_lines)]
pub async fn capture_run(
    server: &FixtureServer,
    label: &str,
    spec: &FixtureSpec,
    workload: &WorkloadConfig,
    out_dir: &Path,
) -> (String, WorkloadReport) {
    eprintln!("== {label}: seeding {} live rows ==", spec.live_rows);
    let db = server.create_database().await;
    let name = db.name().to_string();
    let mut conn = connect(&db.url()).await;
    let seeded = seed(&mut conn, spec).await;
    let settings = server_settings(&mut conn).await;
    // Before the reset, so this claim stays out of the workload statements.
    // `EXPLAIN ANALYZE` scans tables, and a rollback keeps those counters. So
    // the workload baseline is read after it.
    std::fs::write(
        out_dir.join(format!("{label}-claim.explain.txt")),
        format!(
            "-- EXPLAIN (ANALYZE, BUFFERS, SETTINGS) of one claim over every queue, \
             after the seed, rolled back --\n{}\n",
            explain_claim(&mut conn, spec).await
        ),
    )
    .expect("write the claim plan");
    let baseline = stats::read_table_stats(&mut conn)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let shape = &seeded.shape;
    std::fs::write(
        out_dir.join(format!("{label}-post-seed-pg_stat_user_tables.txt")),
        format!(
            "-- pg_stat_user_tables after seeding, before the workload --\n{}",
            stats::render_tables(&seeded.tables)
        ),
    )
    .expect("write the post-seed tables");
    // Without a reset, the statements view holds the seed and the plan too.
    // Evidence from such a run is not workload-only, so the capture stops.
    stats::reset_statements(&mut conn)
        .await
        .unwrap_or_else(|e| panic!("the Ledger capture needs pg_stat_statements: {e}"));
    drop(conn);

    eprintln!("== {label}: driving claims ==");
    let report = drive_claims(&db.url(), spec, workload).await;
    let snapshot = db.snapshot_and_drop().await;
    // A partial snapshot misses counters, so its evidence does not add up.
    assert!(
        snapshot.lingering.is_empty(),
        "{label}: the snapshot is partial: {:?}",
        snapshot.lingering
    );
    let dropped = !server.database_exists(&name).await;
    let deltas = stats::table_deltas(&baseline, &snapshot.tables);
    std::fs::write(
        out_dir.join(format!("{label}-workload-pg_stat_user_tables.txt")),
        format!(
            "-- pg_stat_user_tables, workload deltas (n_live_tup and n_dead_tup are final) --\n{}{}",
            stats::partial_banner(&snapshot),
            stats::render_tables(&deltas)
        ),
    )
    .expect("write the workload tables");
    std::fs::write(
        out_dir.join(format!("{label}-pg_stat_statements.txt")),
        format!(
            "-- pg_stat_statements over the workload, this database only, top {} by buffers --\n{}{}",
            stats::TOP_STATEMENTS,
            stats::partial_banner(&snapshot),
            stats::render_statements(&snapshot.statements, stats::TOP_STATEMENTS)
        ),
    )
    .expect("write the statements");

    let mut s = String::new();
    let _ = writeln!(s, "## {label}\n");
    let _ = writeln!(s, "spec: {spec:?}");
    let _ = writeln!(
        s,
        "seeded: {} live rows, {} executions, churn {:?}, in {:.1}s",
        shape.live_rows,
        seeded.executions,
        seeded.churn,
        seeded.elapsed.as_secs_f64()
    );
    let _ = writeln!(
        s,
        "task queue: {} live rows, heap {:.1} MiB, n_live_tup {}, n_dead_tup {}, dead ratio {:.3} (target {:.3})",
        shape.live_rows,
        shape.heap_bytes as f64 / 1_048_576.0,
        shape.n_live_tup,
        shape.n_dead_tup,
        shape.dead_ratio(),
        spec.dead_ratio
    );
    let _ = writeln!(
        s,
        "queues: head share {:.3} (expected {:.3}), {} queues hold rows",
        shape.queue_share(0),
        head_share(spec.queues, spec.queue_skew),
        shape.queue_counts.len()
    );
    for (q, n) in shape.queue_counts.iter().take(5) {
        let _ = writeln!(s, "  {q}: {n}");
    }
    let _ = writeln!(
        s,
        "keys: {} keyed rows, hot key share {:.3} (expected {:.3})",
        shape.keyed_rows,
        shape.top_key_share,
        head_share(spec.keys, spec.key_skew)
    );
    let _ = writeln!(s, "states: {:?}", shape.states);
    let _ = writeln!(
        s,
        "running: {} rows, at most {} per key and task type (cap {}), at most {} per worker ({} slots)",
        shape.state_count("RUNNING"),
        shape.max_running_per_key,
        spec.key_cap,
        shape.max_running_per_worker,
        WORKER_SLOTS
    );
    let _ = writeln!(s, "server: {settings}");
    let _ = writeln!(s, "pg_stat_statements reset: ok");
    let _ = writeln!(
        s,
        "workload: {} claimers, {} claims, {} completions, {} reclaims, {} enqueues, {} empty polls, {} errors, in {:.1}s ({:.1} claims/s)",
        workload.claimers,
        report.claims,
        report.completions,
        report.reclaims,
        report.enqueues,
        report.empty_polls,
        report.errors,
        report.elapsed.as_secs_f64(),
        report.claims as f64 / report.elapsed.as_secs_f64().max(1e-9)
    );
    if let Some(e) = &report.first_error {
        let _ = writeln!(s, "first error: {e}");
    }
    if let Statements::Captured(rows) = &snapshot.statements {
        let total: i64 = rows.iter().map(stats::StatementStats::total_buffers).sum();
        let _ = writeln!(s, "statements: {} rows, {total} shared buffers", rows.len());
    }
    let _ = writeln!(s, "database dropped after the snapshot: {dropped}\n");
    (s, report)
}
