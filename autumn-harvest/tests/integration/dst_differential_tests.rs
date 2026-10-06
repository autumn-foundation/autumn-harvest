//! Differential test: the simulator's oracle against Postgres (issue #1830).
//!
//! Each test runs seeded simulations on the in-memory oracle. It then
//! replays each run's operation log on a real database through the
//! production statements. Every step must give the same outcome and the
//! same rows. A green run shows that the oracle models the SQL for the
//! compared columns, so an oracle sweep result also holds for the SQL.
//!
//! The `state-only` writes are not production statements. They copy the
//! pre-#1789 guard of `formal/tla/ActivityClaim.tla`.
//!
//! `HARVEST_DST_SEEDS`, `HARVEST_DST_SEED_BASE` and `HARVEST_DST_SEED` pick
//! the seeds, as for the `dst` target. `docs/testing/simulation.md`
//! describes the harness.

use std::collections::BTreeMap;

use autumn_harvest::dst::{
    self, Claim, Fencing, Invariant, Op, Orphan, Outcome, Row, SeedPlan, SimConfig, SimReport,
    TaskState, WriteOutcome,
};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::models::{NewWorkflowExecution, TaskQueueItem};
use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::poison_pill::{orphaned_running_tasks_query, requeue_orphan_stmt};
use autumn_harvest::queue::{self, ClaimWrite, EnqueueParams, TaskClaim, TaskType};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::store;
use autumn_harvest::types::{ActivityExecId, ExecutionId};
use autumn_harvest::worker::{append_activity_started_for_test, finalize_activity_completion};
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel::sql_types::{Array, BigInt, Integer, Jsonb, Nullable, Text, Timestamptz};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

const ACTIVITY: &str = "dst_activity";

/// The seeds of a normal test run. The nightly job sets more. Fewer seeds
/// miss the kept requeue and the stale write after a finish.
const DEFAULT_SEEDS: u64 = 32;

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let mut conn = connect(&url).await;
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("migration");
    (url, Some(container))
}

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect")
}

/// One activity task: its own execution and its own queue.
struct PgTask {
    queue: String,
    exec_id: ExecutionId,
    activity_id: ActivityExecId,
    task_id: Uuid,
}

/// The production statements behind the simulator's [`Op`] set.
struct PgStore<'c> {
    conn: &'c mut AsyncPgConnection,
    fencing: Fencing,
    stale_after_secs: i64,
    /// Virtual time 0. Liveness stamps and the reclaimer's clock are
    /// `epoch + now_ms`, so the reclaimer sees the simulated clock.
    epoch: DateTime<Utc>,
    /// The prefix that makes worker ids unique to this run.
    prefix: String,
    tasks: Vec<PgTask>,
    /// The claimed snapshot of each claim, for the start and finish seams.
    claims: BTreeMap<Claim, TaskQueueItem>,
    codecs: PayloadCodecs,
}

#[derive(QueryableByName)]
struct OrphanRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: Uuid,
    #[diesel(sql_type = Text)]
    worker_id: String,
    #[diesel(sql_type = Integer)]
    crash_strikes: i32,
}

#[derive(QueryableByName)]
struct TaskRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: Uuid,
    #[diesel(sql_type = Text)]
    state: String,
    #[diesel(sql_type = Nullable<Text>)]
    worker_id: Option<String>,
    #[diesel(sql_type = Integer)]
    attempt: i32,
    #[diesel(sql_type = Integer)]
    crash_strikes: i32,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    started: bool,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    heartbeat_stamped: bool,
    #[diesel(sql_type = Nullable<Jsonb>)]
    heartbeat_details: Option<serde_json::Value>,
    #[diesel(sql_type = Nullable<Jsonb>)]
    output: Option<serde_json::Value>,
}

fn payload(tag: u64) -> serde_json::Value {
    serde_json::json!({ "tag": tag })
}

fn tag_of(value: &serde_json::Value) -> u64 {
    value
        .get("tag")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_else(|| panic!("payload without a tag: {value}"))
}

impl<'c> PgStore<'c> {
    async fn seed(conn: &'c mut AsyncPgConnection, config: &SimConfig) -> Self {
        let run = Uuid::new_v4();
        let prefix = format!("dst-{run}-");
        let mut tasks = Vec::with_capacity(config.tasks);
        for index in 0..config.tasks {
            tasks.push(seed_task(conn, &format!("dst-{run}-t{index}")).await);
        }
        let stale_after_secs = i64::try_from(config.stale_after_ms / 1_000).expect("stale bound");
        assert_eq!(
            config.stale_after_ms % 1_000,
            0,
            "the reclaimer binds the stale bound in whole seconds"
        );
        Self {
            conn,
            fencing: config.fencing,
            stale_after_secs,
            epoch: Utc::now(),
            prefix,
            tasks,
            claims: BTreeMap::new(),
            codecs: PayloadCodecs::default(),
        }
    }

    fn at(&self, ms: u64) -> DateTime<Utc> {
        self.epoch + chrono::Duration::milliseconds(i64::try_from(ms).expect("time"))
    }

    fn worker(&self, sim_id: &str) -> String {
        format!("{}{sim_id}", self.prefix)
    }

    fn sim_worker(&self, pg_id: &str) -> String {
        pg_id
            .strip_prefix(&self.prefix)
            .unwrap_or_else(|| panic!("foreign worker id {pg_id}"))
            .to_string()
    }

    fn task_index(&self, id: Uuid) -> usize {
        self.tasks
            .iter()
            .position(|task| task.task_id == id)
            .expect("a task of this run")
    }

    fn snapshot(&self, claim: &Claim) -> &TaskQueueItem {
        self.claims.get(claim).expect("a claim of this run")
    }

    fn task_claim(&self, claim: &Claim) -> TaskClaim {
        TaskClaim::new(
            self.tasks[claim.task].task_id,
            self.worker(&claim.worker),
            claim.attempt,
        )
    }

    async fn apply(&mut self, op: &Op) -> Outcome {
        match op {
            Op::Beat { worker, at_ms } => self.beat(worker, *at_ms).await,
            Op::Claim { worker, task } => self.claim(worker, *task).await,
            Op::Start { claim } => self.start(claim).await,
            Op::Heartbeat { claim, tag } => self.heartbeat(claim, *tag).await,
            Op::Release { claim } => self.release(claim).await,
            Op::Complete { claim, tag } => self.complete(claim, *tag).await,
            Op::Scan { now_ms } => self.scan(*now_ms).await,
            Op::Requeue { orphan, now_ms } => self.requeue(orphan, *now_ms).await,
        }
    }

    async fn beat(&mut self, worker: &str, at_ms: u64) -> Outcome {
        diesel::sql_query(
            "INSERT INTO harvest_workers (worker_id, last_heartbeat_at, max_concurrency, host) \
             VALUES ($1, $2, 10, 'dst') \
             ON CONFLICT (worker_id) DO UPDATE SET last_heartbeat_at = EXCLUDED.last_heartbeat_at",
        )
        .bind::<Text, _>(self.worker(worker))
        .bind::<Timestamptz, _>(self.at(at_ms))
        .execute(self.conn)
        .await
        .expect("beat");
        Outcome::Beat
    }

    async fn claim(&mut self, worker: &str, task: usize) -> Outcome {
        let claimed = queue::claim_task(
            self.conn,
            std::slice::from_ref(&self.tasks[task].queue),
            &self.worker(worker),
            "",
            None,
            &[],
            &[],
        )
        .await
        .expect("claim");
        let Some(item) = claimed else {
            return Outcome::Claimed(None);
        };
        let claim = Claim {
            task,
            worker: worker.to_string(),
            attempt: item.attempt,
        };
        self.claims.insert(claim.clone(), item);
        Outcome::Claimed(Some(claim))
    }

    async fn start(&mut self, claim: &Claim) -> Outcome {
        let applied = match self.fencing {
            Fencing::ClaimEpoch => {
                let item = self.snapshot(claim).clone();
                let worker = self.worker(&claim.worker);
                let exec_id = self.tasks[claim.task].exec_id;
                append_activity_started_for_test(
                    self.conn,
                    &item,
                    exec_id,
                    ACTIVITY,
                    &worker,
                    &self.codecs,
                )
                .await
                .expect("start")
                .is_some()
            }
            // The pre-fix start appends an event under `state = 'RUNNING'`.
            // The event is not compared, so the row read is enough.
            Fencing::StateOnly => self.row(claim.task).await.state == TaskState::Running,
        };
        write_outcome(applied)
    }

    async fn release(&mut self, claim: &Claim) -> Outcome {
        let applied = match self.fencing {
            Fencing::ClaimEpoch => {
                let fenced = self.task_claim(claim);
                queue::release_unstarted_claim(self.conn, &fenced)
                    .await
                    .expect("release")
                    == ClaimWrite::Applied
            }
            // The release with the pre-fix guard: no claim predicate.
            Fencing::StateOnly => {
                diesel::sql_query(
                    "UPDATE harvest_task_queue \
                     SET state = 'PENDING', worker_id = NULL, started_at = NULL, \
                         last_heartbeat_at = NULL, attempt = GREATEST(attempt - 1, 0) \
                     WHERE id = $1 AND state = 'RUNNING'",
                )
                .bind::<diesel::sql_types::Uuid, _>(self.tasks[claim.task].task_id)
                .execute(self.conn)
                .await
                .expect("pre-fix release")
                    > 0
            }
        };
        write_outcome(applied)
    }

    async fn heartbeat(&mut self, claim: &Claim, tag: u64) -> Outcome {
        let applied = match self.fencing {
            Fencing::ClaimEpoch => {
                let fenced = self.task_claim(claim);
                queue::record_heartbeat(self.conn, &fenced, payload(tag))
                    .await
                    .expect("heartbeat")
                    == ClaimWrite::Applied
            }
            // The heartbeat write before issue #1789: no claim predicate.
            Fencing::StateOnly => {
                diesel::sql_query(
                    "UPDATE harvest_task_queue \
                     SET last_heartbeat_at = clock_timestamp(), heartbeat_details = $2 \
                     WHERE id = $1 AND state = 'RUNNING'",
                )
                .bind::<diesel::sql_types::Uuid, _>(self.tasks[claim.task].task_id)
                .bind::<Jsonb, _>(payload(tag))
                .execute(self.conn)
                .await
                .expect("pre-fix heartbeat")
                    > 0
            }
        };
        write_outcome(applied)
    }

    async fn complete(&mut self, claim: &Claim, tag: u64) -> Outcome {
        let task_id = self.tasks[claim.task].task_id;
        let applied = match self.fencing {
            Fencing::ClaimEpoch => {
                let item = self.snapshot(claim).clone();
                let PgTask {
                    exec_id,
                    activity_id,
                    ..
                } = self.tasks[claim.task];
                let before = self.completed_events(exec_id).await;
                finalize_activity_completion(
                    self.conn,
                    &item,
                    exec_id,
                    activity_id,
                    payload(tag),
                    None,
                    &self.codecs,
                )
                .await
                .expect("finalize");
                self.completed_events(exec_id).await > before
            }
            // `complete_task` is the unfenced write. Before issue #1789 the
            // owner used it too. It appends no event.
            Fencing::StateOnly => queue::complete_task(self.conn, task_id, payload(tag))
                .await
                .is_ok(),
        };
        write_outcome(applied)
    }

    /// Check `TerminalStateHasOneEvent` on the real history: a completed
    /// row has exactly one `ActivityCompleted` event, and any other row has
    /// none.
    async fn check_terminal_events(&mut self) -> Result<(), String> {
        let rows = self.rows().await;
        for (task, row) in rows.iter().enumerate() {
            let events = self.completed_events(self.tasks[task].exec_id).await;
            let expected = usize::from(row.state == TaskState::Completed);
            if events != expected {
                return Err(format!(
                    "t{task} is {:?} with {events} ActivityCompleted events",
                    row.state
                ));
            }
        }
        Ok(())
    }

    async fn completed_events(&mut self, exec_id: ExecutionId) -> usize {
        store::load_history(self.conn, exec_id)
            .await
            .expect("history")
            .events
            .iter()
            .filter(|event| matches!(event, WorkflowEvent::ActivityCompleted { .. }))
            .count()
    }

    /// The production scan, with `NOW()` bound to the simulated clock.
    async fn scan(&mut self, now_ms: u64) -> Outcome {
        let production = orphaned_running_tasks_query();
        assert_eq!(
            production.matches("NOW()").count(),
            1,
            "the scan reads the clock once: {production}"
        );
        let scan = production.replace("NOW()", "$2");
        let sql =
            format!("SELECT id, worker_id, crash_strikes FROM ({scan}) orphan WHERE id = ANY($3)");
        let ids: Vec<Uuid> = self.tasks.iter().map(|task| task.task_id).collect();
        let rows: Vec<OrphanRow> = diesel::sql_query(sql)
            .bind::<BigInt, _>(self.stale_after_secs)
            .bind::<Timestamptz, _>(self.at(now_ms))
            .bind::<Array<diesel::sql_types::Uuid>, _>(ids)
            .load(self.conn)
            .await
            .expect("scan");
        let mut orphans: Vec<Orphan> = rows
            .into_iter()
            .map(|row| Orphan {
                task: self.task_index(row.id),
                worker: self.sim_worker(&row.worker_id),
                crash_strikes: row.crash_strikes,
            })
            .collect();
        orphans.sort();
        Outcome::Orphans(orphans)
    }

    /// The reclaimer's lock, then its requeue statement, as `requeue_orphan`.
    async fn requeue(&mut self, orphan: &Orphan, now_ms: u64) -> Outcome {
        let task_id = self.tasks[orphan.task].task_id;
        let worker = self.worker(&orphan.worker);
        let strikes = orphan.crash_strikes;
        let stale = self.stale_after_secs;
        let now = self.at(now_ms);
        let moved = self
            .conn
            .transaction::<usize, diesel::result::Error, _>(async |conn| {
                diesel::sql_query("SELECT id FROM harvest_task_queue WHERE id = $1 FOR UPDATE")
                    .bind::<diesel::sql_types::Uuid, _>(task_id)
                    .execute(conn)
                    .await?;
                diesel::sql_query(requeue_orphan_stmt())
                    .bind::<diesel::sql_types::Uuid, _>(task_id)
                    .bind::<Text, _>(&worker)
                    .bind::<Integer, _>(strikes)
                    .bind::<Integer, _>(strikes + 1)
                    .bind::<BigInt, _>(stale)
                    .bind::<Timestamptz, _>(now)
                    .execute(conn)
                    .await
            })
            .await
            .expect("requeue");
        Outcome::Requeued(moved > 0)
    }

    async fn rows(&mut self) -> Vec<Row> {
        let ids: Vec<Uuid> = self.tasks.iter().map(|task| task.task_id).collect();
        let loaded: Vec<TaskRow> = diesel::sql_query(
            "SELECT id, state, worker_id, attempt, crash_strikes, \
                 started_at IS NOT NULL AS started, \
                 last_heartbeat_at IS NOT NULL AS heartbeat_stamped, \
                 heartbeat_details, output \
             FROM harvest_task_queue WHERE id = ANY($1)",
        )
        .bind::<Array<diesel::sql_types::Uuid>, _>(ids)
        .load(self.conn)
        .await
        .expect("rows");
        let mut rows = vec![None; self.tasks.len()];
        for row in loaded {
            let state = match row.state.as_str() {
                "PENDING" => TaskState::Pending,
                "RUNNING" => TaskState::Running,
                "COMPLETED" => TaskState::Completed,
                other => panic!("unexpected state {other}"),
            };
            rows[self.task_index(row.id)] = Some(Row {
                state,
                worker: row.worker_id.as_deref().map(|id| self.sim_worker(id)),
                attempt: row.attempt,
                crash_strikes: row.crash_strikes,
                started: row.started,
                heartbeat_stamped: row.heartbeat_stamped,
                heartbeat: row.heartbeat_details.as_ref().map(tag_of),
                output: row.output.as_ref().map(tag_of),
            });
        }
        rows.into_iter()
            .map(|row| row.expect("every task row exists"))
            .collect()
    }

    async fn row(&mut self, task: usize) -> Row {
        self.rows().await.swap_remove(task)
    }
}

const fn write_outcome(applied: bool) -> Outcome {
    Outcome::Write(if applied {
        WriteOutcome::Applied
    } else {
        WriteOutcome::LeaseLost
    })
}

async fn seed_task(conn: &mut AsyncPgConnection, queue: &str) -> PgTask {
    let exec_id = insert_execution(conn, queue).await;
    let activity_id = ActivityExecId::new();
    store::append_events(
        conn,
        exec_id,
        &[
            WorkflowEvent::WorkflowStarted {
                input: serde_json::json!({}),
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: ACTIVITY.to_string(),
                input: serde_json::json!({}),
                queue: queue.to_string(),
            },
        ],
        0,
    )
    .await
    .expect("seed history");
    let mut params = EnqueueParams::new(queue, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.activity_name = Some(ACTIVITY.to_string());
    params.activity_id = Some(activity_id.as_uuid());
    params.max_attempts = 1_000;
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(1);
    let task_id = queue::enqueue(conn, &params).await.expect("enqueue");
    PgTask {
        queue: queue.to_string(),
        exec_id,
        activity_id,
        task_id,
    }
}

async fn insert_execution(conn: &mut AsyncPgConnection, queue: &str) -> ExecutionId {
    let exec_id = ExecutionId::new();
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&NewWorkflowExecution {
            quota_key: None,
            continued_from_exec_id: None,
            first_exec_id: None,
            id: exec_id.as_uuid(),
            workflow_name: "dst_wf",
            workflow_id: &format!("wf-{queue}"),
            run_id: Uuid::new_v4(),
            shard_id: 0,
            input: serde_json::json!({}).into(),
            memo: None,
            search_attrs: None,
            queue_name: queue,
            parent_id: None,
            parent_close_policy: None,
            assigned_build_id: None,
            execution_timeout: None,
            deadline_at: None,
            chain_execution_timeout: None,
            chain_deadline_at: None,
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
        })
        .execute(conn)
        .await
        .expect("insert execution");
    exec_id
}

/// The command that replays one seed of this test on Postgres.
fn postgres_repro_command(seed: u64) -> String {
    format!(
        "{}={seed} cargo test -p autumn-harvest --test integration \
         dst_differential_tests::postgres_matches_the_oracle_for_every_seed \
         -- --nocapture --test-threads=1",
        dst::SEED_VAR
    )
}

/// Replay the operation log of `report` on Postgres and compare each step.
async fn replay_on_postgres(conn: &mut AsyncPgConnection, report: &SimReport) {
    let seed = report.config.seed;
    let mut pg = PgStore::seed(conn, &report.config).await;
    for record in &report.steps {
        let outcome = pg.apply(&record.op).await;
        let context = || {
            format!(
                "seed {seed}, step {}: {:?}\nreproduce on Postgres: {}\n\
                 print the oracle trace: {}\nlast steps:\n{}",
                record.step,
                record.op,
                postgres_repro_command(seed),
                dst::repro_command(&report.config),
                report.trace_tail(dst::TAIL_LINES)
            )
        };
        assert_eq!(outcome, record.outcome, "outcome differs at {}", context());
        let rows = pg.rows().await;
        assert_eq!(rows, record.rows, "rows differ at {}", context());
        // The pre-fix terminal write appends no event, so only the fenced
        // replay checks the history.
        if report.config.fencing == Fencing::ClaimEpoch
            && let Err(error) = pg.check_terminal_events().await
        {
            panic!("{error} at {}", context());
        }
    }
}

#[tokio::test]
async fn postgres_matches_the_oracle_for_every_seed() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let plan = SeedPlan::from_env(DEFAULT_SEEDS).unwrap_or_else(|error| panic!("{error}"));
    let mut covered = dst::SimStats::default();
    for seed in plan.seeds() {
        let report = dst::run_twice(&SimConfig::new(seed)).unwrap_or_else(|e| panic!("{e}"));
        if let Some(violation) = &report.violation {
            panic!(
                "seed {seed}: {violation}\nreproduce: {}\nlast steps:\n{}",
                dst::repro_command(&report.config),
                report.trace_tail(dst::TAIL_LINES)
            );
        }
        replay_on_postgres(&mut conn, &report).await;
        covered.merge(&report.stats);
    }
    // A sweep of one seed cannot reach every branch, so only a full run
    // checks coverage.
    if plan.count >= DEFAULT_SEEDS {
        assert!(
            covered.reclaims > 0
                && covered.requeues_kept > 0
                && covered.releases > 0
                && covered.stale_completes_rejected > 0
                && covered.stale_after_finish > 0,
            "the replayed runs reach every race: {covered:?}"
        );
    }
}

#[tokio::test]
async fn postgres_reproduces_the_pre_fix_bug_on_the_failing_seed() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let report = (0..64)
        .map(|seed| {
            dst::run(
                &SimConfig::new(seed)
                    .with_fencing(Fencing::StateOnly)
                    .checking(&[Invariant::TerminalByCurrentClaim]),
            )
        })
        .find(|report| report.violation.is_some())
        .expect("the pre-fix guard fails within 64 seeds");
    // Every step matches, the stale terminal write included.
    replay_on_postgres(&mut conn, &report).await;
    let last = report.steps.last().expect("steps");
    assert!(matches!(last.op, Op::Complete { .. }), "{:?}", last.op);
    assert_eq!(last.outcome, Outcome::Write(WriteOutcome::Applied));
}
