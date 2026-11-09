//! The durable agent stores on Postgres.
//!
//! - The store suite passes, through a new pool for each call.
//! - An episode append whose acknowledgement goes missing after the commit
//!   retries, and the episode lands once. A worker in the test process runs
//!   the op, so the retry is the real activity retry.
//! - A memory reset removes the rows of the agent from each store, and the
//!   final state counts them. `harvest agents erase` reads those counts.
//!
//! The tests use `HARVEST_TEST_DATABASE_URL` when it is set. Point it at a
//! dedicated database. Without it, each test starts a Postgres container.
//! CI runs this target from a `linux` row of
//! `.github/ci/integration-suites.txt`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod agent_stores_suite;
mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use agent_stores_suite::{Stores, unique};
use autumn_harvest::worker::{DbPool, Worker, WorkerRuntimeConfig};
use autumn_harvest::{HarvestBuilder, ShardId, WorkerConfig, migrate};
use autumn_harvest_agent::episode::{
    Appended, Cleared, Episode, EpisodeLog, EpisodeSettings, EpisodeStore, PassMark,
};
use autumn_harvest_agent::postgres::{self, PostgresAgents};
use autumn_harvest_agent::stores::{
    PostgresEpisodeStore, PostgresMemoryStore, PostgresTranscriptStore,
};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentId, AgentMemory, AgentModel, AgentOp, AgentProfile, AgentState,
    BoxFuture, ChatRequest, ChatRole, ContentPart, Control, EditKey, ErrorKind, MemoryScope,
};
use common::{Reply, TryFnModel, answer};
use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Binary, Text};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

/// How long a test waits for a worker.
const LIMIT: Duration = Duration::from_secs(60);

/// A migrated database, and the container that serves it, if any.
async fn database() -> (String, Option<ContainerAsync<Postgres>>) {
    let (url, container) = if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        (url, None)
    } else {
        let container = Postgres::default()
            .with_tag("16")
            .start()
            .await
            .expect("postgres container should start");
        let host = container.get_host().await.expect("container host");
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("container port");
        (
            format!("postgres://postgres:postgres@{host}:{port}/postgres"),
            Some(container),
        )
    };
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    migrate::apply_to_connection(&mut conn, &migrate::embedded())
        .await
        .expect("migrate");
    (url, container)
}

fn pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("pool")
}

fn stores_on(url: &str) -> Stores {
    let pool = pool(url);
    Stores {
        episodes: Arc::new(PostgresEpisodeStore::new(pool.clone())),
        memory: Arc::new(PostgresMemoryStore::new(pool.clone())),
        transcripts: Arc::new(PostgresTranscriptStore::new(pool)),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_postgres_stores_pass_the_suite() {
    let (url, _container) = database().await;
    agent_stores_suite::run_all(&|| stores_on(&url)).await;
}

// ---------------------------------------------------------------------------
// The agent on a worker.
// ---------------------------------------------------------------------------

/// A running worker. Dropping it stops the worker.
struct Running {
    worker: Arc<Worker>,
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.worker.shutdown();
        self.handle.abort();
    }
}

/// Start a worker of the agent workflows on `queue` and `default`.
fn run_worker(url: &str, queue: &str, harness: AgentHarness) -> Running {
    let mut config = WorkerConfig::default()
        .with_queues([queue, "default"])
        .with_shard_assignments([ShardId::new(0)]);
    config.max_concurrent_workflows = 8;
    config.max_concurrent_activities = 8;
    let (registry, _dags, _schedules, config) = postgres::register(HarvestBuilder::new(), harness)
        .worker(config)
        .build()
        .into_worker_parts();
    let mut runtime = WorkerRuntimeConfig::from(config);
    runtime.poll_interval = Duration::from_millis(25);
    let worker = Arc::new(Worker::new(runtime, Arc::new(registry)).expect("worker"));
    let handle = tokio::spawn({
        let worker = Arc::clone(&worker);
        let pool = pool(url);
        async move { worker.run(&pool).await }
    });
    Running { worker, handle }
}

/// The text of every user message of a request.
fn user_text(request: &ChatRequest) -> String {
    request
        .messages
        .iter()
        .filter(|message| message.role == ChatRole::User)
        .flat_map(|message| message.content.iter())
        .filter_map(|part| match part {
            ContentPart::Text(text) => Some(text.clone()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// A model that answers each op, and gives one fact per episode to a pass.
fn model() -> Arc<TryFnModel> {
    TryFnModel::new(|request: &ChatRequest| -> Reply {
        let ids: Vec<String> = user_text(request)
            .split("<episode id=\"")
            .skip(1)
            .filter_map(|rest| rest.split('"').next().map(str::to_owned))
            .collect();
        if ids.is_empty() {
            return answer("noted", 1);
        }
        let facts: Vec<serde_json::Value> = ids
            .iter()
            .enumerate()
            .map(|(n, id)| {
                serde_json::json!({
                    "subject": "ada",
                    "relation": "said",
                    "object": format!("thing {n}"),
                    "episodes": [id],
                })
            })
            .collect();
        answer(&serde_json::json!({ "facts": facts }).to_string(), 1)
    })
}

async fn settled(
    pg: &PostgresAgents,
    id: &AgentId,
    until: impl Fn(&AgentState) -> bool,
) -> AgentState {
    postgres::wait_for(pg, id, LIMIT, until)
        .await
        .expect("read the state")
        .unwrap_or_else(|| panic!("the agent {id} did not settle"))
}

#[derive(QueryableByName)]
struct Count {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

#[derive(QueryableByName)]
struct Body {
    #[diesel(sql_type = Binary)]
    body: Vec<u8>,
}

/// The rows of `table` whose column `filter` is `value`.
async fn count(url: &str, table: &str, filter: &str, value: &str) -> i64 {
    use diesel_async::RunQueryDsl as _;
    let mut conn = AsyncPgConnection::establish(url).await.unwrap();
    let row: Count = diesel::sql_query(format!(
        "SELECT COUNT(*) AS n FROM {table} WHERE {filter} = $1"
    ))
    .bind::<Text, _>(value)
    .get_result(&mut conn)
    .await
    .unwrap();
    row.n
}

/// An episode store whose first append commits and then reports a lost
/// connection, as a crash between the commit and the acknowledgement
/// does.
#[derive(Debug)]
struct LoseFirstAck {
    inner: PostgresEpisodeStore,
    appends: AtomicU32,
}

impl EpisodeStore for LoseFirstAck {
    fn load<'a>(&'a self, scope: &'a MemoryScope) -> BoxFuture<'a, Result<EpisodeLog, AgentError>> {
        self.inner.load(scope)
    }

    fn append<'a>(
        &'a self,
        scope: &'a MemoryScope,
        key: &'a EditKey,
        episode: Episode,
        max_episodes: u32,
    ) -> BoxFuture<'a, Result<Appended, AgentError>> {
        Box::pin(async move {
            let appended = self.inner.append(scope, key, episode, max_episodes).await?;
            if self.appends.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(AgentError::new(
                    ErrorKind::Unavailable,
                    "the connection closed before the acknowledgement",
                ));
            }
            Ok(appended)
        })
    }

    fn mark<'a>(
        &'a self,
        scope: &'a MemoryScope,
        mark: PassMark,
    ) -> BoxFuture<'a, Result<bool, AgentError>> {
        self.inner.mark(scope, mark)
    }

    fn clear<'a>(
        &'a self,
        scope: &'a MemoryScope,
        key: &'a EditKey,
    ) -> BoxFuture<'a, Result<Cleared, AgentError>> {
        self.inner.clear(scope, key)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retried_postgres_append_after_a_lost_ack_lands_once() {
    let (url, _container) = database().await;
    let queue = unique("pg-stores-ack");
    let kind = unique("desk");
    let episodes = Arc::new(LoseFirstAck {
        inner: PostgresEpisodeStore::new(pool(&url)),
        appends: AtomicU32::new(0),
    });
    let harness = AgentHarness::new(model())
        .memory(Arc::new(PostgresMemoryStore::new(pool(&url))))
        .episodes(episodes.clone())
        .agent(
            AgentProfile::new(&kind)
                .memory(AgentMemory::Store)
                .episodes(EpisodeSettings::default()),
        );
    let pg = PostgresAgents::single(pool(&url)).with_queue(&queue);
    let _worker = run_worker(&url, &queue, harness);
    let id = AgentId::new(&kind, "d1").unwrap();

    postgres::send(&pg, &id, &AgentOp::message("hello"), None)
        .await
        .unwrap();
    let state = settled(&pg, &id, |state| state.segments >= 1).await;
    assert_eq!(state.failures, 0, "{:?}", state.last_error);
    assert_eq!(
        episodes.appends.load(Ordering::SeqCst),
        2,
        "the activity retried once"
    );
    let scope = MemoryScope::new(format!("agent:{id}:episodes"));
    let log = PostgresEpisodeStore::new(pool(&url))
        .load(&scope)
        .await
        .unwrap();
    assert_eq!(log.len(), 1, "one op, one episode");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_erase_removes_the_rows_of_the_agent_and_counts_them() {
    let (url, _container) = database().await;
    let queue = unique("pg-stores-erase");
    let kind = unique("desk");
    let stores = stores_on(&url);
    let harness = AgentHarness::new(model() as Arc<dyn AgentModel>)
        .memory(stores.memory.clone())
        .episodes(stores.episodes.clone())
        .transcript_store(stores.transcripts.clone())
        .agent(
            AgentProfile::new(&kind)
                .memory(AgentMemory::Store)
                .episodes(EpisodeSettings::default().every(3)),
        );
    let pg = PostgresAgents::single(pool(&url)).with_queue(&queue);
    let _worker = run_worker(&url, &queue, harness);
    let id = AgentId::new(&kind, "d1").unwrap();
    let episodes = MemoryScope::new(format!("agent:{id}:episodes"));
    let memory = MemoryScope::new(format!("agent:{id}"));

    for (n, text) in ["one", "two", "three"].into_iter().enumerate() {
        let message = format!("m-{n}");
        postgres::send(
            &pg,
            &id,
            &AgentOp::message_with_id(text, &message),
            Some(&message),
        )
        .await
        .unwrap();
        settled(&pg, &id, |state| state.segments > n as u64).await;
    }
    let facts: usize = stores
        .memory
        .load(&memory)
        .await
        .unwrap()
        .iter()
        .map(|block| block.entries.len())
        .sum();
    assert_eq!(facts, 3, "one fact per episode");
    // A plain note, so the memory count differs from the fact count.
    let note = autumn_harvest_agent::memory::MemoryOp::Add {
        block: "user".into(),
        text: "prefers mornings".into(),
    };
    stores
        .memory
        .apply(&memory, note, &EditKey::new("note"))
        .await
        .unwrap();
    let windows = count(&url, "harvest_agent_transcripts", "session", id.as_str()).await;
    assert!(windows > 0, "each op kept a window under its session");

    postgres::send(&pg, &id, &AgentOp::Control(Control::ResetMemory), None)
        .await
        .unwrap();
    let state = settled(&pg, &id, |state| state.transcripts_erased.is_some()).await;
    assert_eq!(state.failures, 0, "{:?}", state.last_error);
    let reset = state.episode_reset.expect("the episode count");
    assert_eq!(reset.episodes, 3);
    assert_eq!(reset.facts, 3);
    assert_eq!(state.memory_entries_erased, Some(4), "3 facts and 1 note");
    assert_eq!(
        state.transcripts_erased,
        Some(u64::try_from(windows).unwrap())
    );

    assert_eq!(
        count(&url, "harvest_agent_transcripts", "session", id.as_str()).await,
        0,
        "no window of the agent"
    );
    // A clear keeps each document as a tombstone, so its version grows.
    assert_eq!(
        tombstone(&url, memory.as_str()).await,
        b"{}",
        "the memory tombstone is empty"
    );
    let body = String::from_utf8(tombstone(&url, episodes.as_str()).await).unwrap();
    for text in ["noted", "thing"] {
        assert!(
            !body.contains(text),
            "the tombstone holds no episode or fact text: {body}"
        );
    }
    let log = stores.episodes.load(&episodes).await.unwrap();
    assert!(log.is_empty());
    assert_eq!(log.generation(), 1, "the tombstone keeps the generation");
}

/// The body of the episode document of `scope`.
async fn tombstone(url: &str, scope: &str) -> Vec<u8> {
    use diesel_async::RunQueryDsl as _;
    let mut conn = AsyncPgConnection::establish(url).await.unwrap();
    let row: Body = diesel::sql_query("SELECT body FROM harvest_agent_store_docs WHERE scope = $1")
        .bind::<Text, _>(scope)
        .get_result(&mut conn)
        .await
        .unwrap();
    row.body
}
