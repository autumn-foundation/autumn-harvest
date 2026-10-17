//! The Postgres driver: a testcontainers Postgres 16 and one real `Worker`.
//!
//! Each run starts its own container, so runs in parallel share nothing. Set
//! `HARVEST_TEST_DATABASE_URL` to use a migrated Postgres instead. The runs
//! then share its `default` queue, so run them with `--test-threads=1`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::execution::{self, StartWorkflowParams};
use autumn_harvest::models::WorkflowExecution;
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::types::{ExecutionId, ShardId, UpdateId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{queue, signal, store};
use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::Value;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

use crate::compare::{Execution, Outcome, Run, children};
use crate::scenario::{Scenario, Step};

/// The longest a script step or the final wait may take.
const WAIT: Duration = Duration::from_secs(30);

/// How often the driver reads the database while it waits.
const POLL: Duration = Duration::from_millis(25);

/// A database URL, and the container that serves it, if the driver started one.
struct Database {
    url: String,
    _container: Option<ContainerAsync<Postgres>>,
}

async fn database() -> Database {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return Database {
            url,
            _container: None,
        };
    }
    let container = Postgres::default()
        .with_init_sql(autumn_harvest::test_init_sql().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("start the Postgres 16 container");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container port");
    Database {
        url: format!("postgres://postgres:postgres@{host}:{port}/postgres"),
        _container: Some(container),
    }
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url)
        .await
        .expect("connect to Postgres")
}

fn pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("build the pool")
}

fn worker_config(queue: &str) -> WorkerRuntimeConfig {
    WorkerRuntimeConfig {
        codec_rotation_batch_size: 0,
        scanner: autumn_harvest::scanner_lease::ScannerConfig::default(),
        dr: autumn_harvest::replication::DrConfig::default(),
        worker_id: "conformance".to_owned(),
        queues: vec![queue.to_owned()],
        notification_database_url: None,
        max_concurrent_workflows: 4,
        max_concurrent_activities: 4,
        max_concurrent_claims: autumn_harvest::worker::DEFAULT_MAX_CONCURRENT_CLAIMS,
        poll_interval: POLL,
        shutdown_timeout: Duration::from_secs(1),
        cancellation_grace_period: Duration::from_secs(1),
        sticky_timeout: Duration::from_secs(5),
        max_local_activity_start_to_close: Duration::from_secs(60),
        shard_assignments: vec![ShardId::new(0)],
        worker_heartbeat_interval: Duration::from_secs(5),
        build_id: String::new(),
        deployment_name: None,
        workflow_cache_size: 1000,
        resident_workflows: true,
        priority_aging_secs: None,
        fairness_keys: false,
        unknown_target_grace_window: Duration::from_secs(5),
        poison_pill_threshold: 3,
        capability_miss_max_redeliveries: 5,
        workflow_task_timeout: Duration::from_secs(30),
        workflow_panic_max_attempts: 3,
        labels: HashMap::new(),
        queue_weights: HashMap::new(),
        max_workflow_pause_duration: Duration::from_secs(24 * 3600),
        max_workflow_history_events: None,
        shard_notification_database_urls: Vec::new(),
        sharded_pool: None,
        slot_tuner: None,
        max_concurrent_sessions: 0,
    }
}

async fn execution_row(conn: &mut AsyncPgConnection, exec: ExecutionId) -> WorkflowExecution {
    harvest_workflow_executions::table
        .find(exec.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .expect("read the execution row")
}

async fn history(conn: &mut AsyncPgConnection, exec: ExecutionId) -> Vec<WorkflowEvent> {
    store::load_history(conn, exec)
        .await
        .expect("read the history")
        .events
}

fn is_terminal(state: &str) -> bool {
    autumn_harvest::erase::is_terminal_state(state)
}

/// A wait that panics with `what` after [`WAIT`].
struct Deadline {
    what: String,
    end: tokio::time::Instant,
}

impl Deadline {
    fn new(what: impl Into<String>) -> Self {
        Self {
            what: what.into(),
            end: tokio::time::Instant::now() + WAIT,
        }
    }

    fn passed(&self) -> bool {
        tokio::time::Instant::now() >= self.end
    }

    /// Sleep one poll interval. Panic when the deadline has passed.
    async fn tick(&self) {
        assert!(
            tokio::time::Instant::now() < self.end,
            "postgres: timed out waiting for {}",
            self.what
        );
        tokio::time::sleep(POLL).await;
    }
}

/// Send each signal of a batch in its own transaction, in order.
///
/// One transaction would give every row the same `received_at`. Postgres
/// then orders the rows by a random id, so the history order is random.
async fn send_batch(conn: &mut AsyncPgConnection, exec: ExecutionId, batch: &[(&str, Value)]) {
    for (name, payload) in batch {
        signal::send_signal(conn, exec, name, payload.clone())
            .await
            .expect("send a signal");
    }
}

/// Admit an update, wake the run, and wait for its result event.
async fn update(conn: &mut AsyncPgConnection, exec: ExecutionId, name: &str, input: Value) -> Value {
    let update_id = UpdateId::new();
    store::admit_update_event(conn, exec, update_id, name.to_owned(), input, None)
        .await
        .expect("admit the update");
    queue::wake_workflow_task(conn, exec)
        .await
        .expect("wake the run");
    let deadline = Deadline::new(format!("the result of update {name}"));
    loop {
        let found = history(conn, exec).await.into_iter().find_map(|e| match e {
            WorkflowEvent::UpdateCompleted { update_id: id, output } if id == update_id => {
                Some(output)
            }
            WorkflowEvent::UpdateFailed { update_id: id, error, .. } if id == update_id => {
                Some(serde_json::json!({ "update_failed": error }))
            }
            _ => None,
        });
        if let Some(result) = found {
            return result;
        }
        deadline.tick().await;
    }
}

/// Run `scenario` on a fresh Postgres and return what the backend did.
pub async fn run(scenario: &Scenario) -> Run {
    let db = database().await;
    let mut conn = connect(&db.url).await;
    let queue = "default";
    let root = ExecutionId::new_for_shard(ShardId::new(0));
    let workflow_id = format!("{}-{}", scenario.name, root);

    execution::start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams::new(
            scenario.workflow,
            &workflow_id,
            root,
            scenario.input.clone(),
            queue,
        ),
        None,
    )
    .await
    .expect("start the root run");
    if !scenario.initial_signals.is_empty() {
        send_batch(&mut conn, root, &scenario.initial_signals).await;
    }

    let registry = Arc::new(
        HandlerRegistry::new((scenario.workflows)(), (scenario.activities)()).with_handler_infos(
            Vec::new(),
            (scenario.updates)(),
            Vec::new(),
        ),
    );
    let worker = Arc::new(Worker::new(worker_config(queue), registry).expect("build the worker"));
    let pool = pool(&db.url);
    let handle = tokio::spawn({
        let worker = Arc::clone(&worker);
        async move { worker.run(&pool).await }
    });

    let mut update_results = Vec::new();
    for step in &scenario.steps {
        match step {
            Step::Signals(batch) => send_batch(&mut conn, root, batch).await,
            Step::AwaitEvent { event, count } => {
                let deadline = Deadline::new(format!("{count} {event} events"));
                loop {
                    let events = history(&mut conn, root).await;
                    let seen = events
                        .iter()
                        .filter(|e| crate::compare::event_type(e) == *event)
                        .count();
                    if seen >= *count {
                        break;
                    }
                    if deadline.passed() {
                        let row = execution_row(&mut conn, root).await;
                        panic!(
                            "postgres: timed out waiting for {count} {event} events. \
                             State {}, error {:?}, history {:?}",
                            row.state,
                            row.error,
                            events.iter().map(crate::compare::event_type).collect::<Vec<_>>()
                        );
                    }
                    deadline.tick().await;
                }
            }
            Step::Update { name, input } => {
                update_results.push(update(&mut conn, root, name, input.clone()).await);
            }
            Step::Cancel(reason) => {
                execution::cancel_workflow_execution(&mut conn, root, reason, &NoOpMetrics)
                    .await
                    .expect("cancel the run");
            }
            Step::Terminate(reason) => {
                execution::terminate_workflow_execution(&mut conn, root, reason, &NoOpMetrics)
                    .await
                    .expect("terminate the run");
            }
        }
    }

    let chain = follow_chain(&mut conn, root).await;
    let mut executions = Vec::new();
    for (i, id) in chain.iter().enumerate() {
        executions.push(Execution {
            role: if i == 0 {
                "root".to_owned()
            } else {
                format!("successor {i}")
            },
            workflow: scenario.workflow.to_owned(),
            events: history(&mut conn, *id).await,
        });
    }
    // Each child must end too. Read them in order of first mention.
    let mut next = 0;
    while next < executions.len() {
        for (child, workflow) in children(&executions[next].events) {
            wait_for_end(&mut conn, child).await;
            let n = executions.iter().filter(|e| e.role.starts_with("child")).count() + 1;
            executions.push(Execution {
                role: format!("child {n}"),
                workflow,
                events: history(&mut conn, child).await,
            });
        }
        next += 1;
    }
    worker.shutdown();
    let _ = handle.await;

    let last = chain[chain.len() - 1];
    let row = execution_row(&mut conn, last).await;
    let outcome = match row.state.as_str() {
        "COMPLETED" => Outcome::Completed(row.output.map(Into::into).unwrap_or(Value::Null)),
        "FAILED" => Outcome::Failed(row.error.unwrap_or_default()),
        other => Outcome::Ended(other.to_owned()),
    };
    Run {
        outcome,
        executions,
        update_results,
    }
}

/// Wait until `exec` is terminal. Return its state.
async fn wait_for_end(conn: &mut AsyncPgConnection, exec: ExecutionId) -> String {
    let deadline = Deadline::new(format!("execution {exec} to end"));
    loop {
        let state = execution_row(conn, exec).await.state;
        if is_terminal(&state) {
            return state;
        }
        deadline.tick().await;
    }
}

/// Wait until each run of the continue-as-new chain is terminal.
async fn follow_chain(conn: &mut AsyncPgConnection, root: ExecutionId) -> Vec<ExecutionId> {
    let mut chain = vec![root];
    loop {
        let current = chain[chain.len() - 1];
        if wait_for_end(conn, current).await != "CONTINUED_AS_NEW" {
            return chain;
        }
        let next = history(conn, current).await.into_iter().find_map(|e| match e {
            WorkflowEvent::WorkflowContinuedAsNew { new_exec_id, .. } => Some(new_exec_id),
            _ => None,
        });
        match next {
            Some(id) => chain.push(id),
            None => return chain,
        }
    }
}
