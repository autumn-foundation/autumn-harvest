//! The durable agent stores on `SQLite`.
//!
//! - The store suite passes on a file, through new handles for each call.
//! - An episode append that a crash interrupts lands once. The crash test
//!   runs this binary again as a child process. The child aborts after the
//!   append commits, and the parent replays the op on a new process.
//! - A memory reset removes the rows of the agent from each store, and the
//!   final state counts them. `harvest agents erase` reads those counts.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod agent_stores_suite;
mod common;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use agent_stores_suite::Stores;
use autumn_harvest_agent::episode::{
    Appended, Cleared, Episode, EpisodeLog, EpisodeSettings, EpisodeStore, PassMark,
};
use autumn_harvest_agent::stores::{SqliteEpisodeStore, SqliteMemoryStore, SqliteTranscriptStore};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentId, AgentMemory, AgentModel, AgentOp, AgentProfile, AgentState,
    BoxFuture, ChatRequest, ChatRole, ContentPart, Control, EditKey, MemoryScope, sqlite,
};
use autumn_harvest_sqlite::SqliteRuntime;
use common::{Reply, TryFnModel, answer, runtime};

fn fresh_db() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

fn stores_on(path: &Path) -> Stores {
    Stores {
        episodes: Arc::new(SqliteEpisodeStore::open(path).unwrap()),
        memory: Arc::new(SqliteMemoryStore::open(path).unwrap()),
        transcripts: Arc::new(SqliteTranscriptStore::open(path).unwrap()),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_sqlite_stores_pass_the_suite() {
    let (_dir, path) = fresh_db();
    agent_stores_suite::run_all(&|| stores_on(&path)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_in_memory_sqlite_stores_open() {
    let stores = Stores {
        episodes: Arc::new(SqliteEpisodeStore::open_in_memory().unwrap()),
        memory: Arc::new(SqliteMemoryStore::open_in_memory().unwrap()),
        transcripts: Arc::new(SqliteTranscriptStore::open_in_memory().unwrap()),
    };
    agent_stores_suite::a_window_round_trips(&|| Stores {
        episodes: Arc::clone(&stores.episodes),
        memory: Arc::clone(&stores.memory),
        transcripts: Arc::clone(&stores.transcripts),
    })
    .await;
}

#[test]
fn debug_output_shows_no_row() {
    let (_dir, path) = fresh_db();
    let stores = stores_on(&path);
    for debug in [
        format!("{:?}", stores.episodes),
        format!("{:?}", stores.memory),
        format!("{:?}", stores.transcripts),
    ] {
        assert!(!debug.contains("agent.db"), "{debug}");
    }
}

// ---------------------------------------------------------------------------
// The agent on the stores.
// ---------------------------------------------------------------------------

fn agent() -> AgentId {
    AgentId::new("desk", "d1").unwrap()
}

fn profile() -> AgentProfile {
    AgentProfile::new("desk")
        .memory(AgentMemory::Store)
        .episodes(EpisodeSettings::default().every(3))
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
        let text = user_text(request);
        let ids: Vec<String> = text
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

fn harness_on(path: &Path, model: Arc<dyn AgentModel>) -> AgentHarness {
    let stores = stores_on(path);
    AgentHarness::new(model)
        .memory(stores.memory)
        .episodes(stores.episodes)
        .transcript_store(stores.transcripts)
        .agent(profile())
}

async fn say(rt: &mut SqliteRuntime, text: &str) {
    sqlite::send(rt, &agent(), &AgentOp::message(text), None).unwrap();
    sqlite::drive(rt, &agent()).await.unwrap();
}

fn state_of(rt: &SqliteRuntime, id: &AgentId) -> impl Future<Output = AgentState> + Send + 'static {
    let query = Box::pin(sqlite::state(rt, id));
    async move { query.await.unwrap().unwrap() }
}

/// The rows of a table that match `filter`, read with a plain connection.
fn count(path: &Path, table: &str, filter: &str, value: &str) -> i64 {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.query_row(
        &format!("SELECT COUNT(*) FROM {table} WHERE {filter} = ?1"),
        [value],
        |row| row.get(0),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn an_erase_removes_the_rows_of_the_agent_and_counts_them() {
    let (_dir, path) = fresh_db();
    let mut rt = runtime(&path, harness_on(&path, model()));
    for text in ["one", "two", "three"] {
        say(&mut rt, text).await;
    }
    let held = stores_on(&path);
    let episodes = MemoryScope::new("agent:desk/d1:episodes");
    let memory = MemoryScope::new("agent:desk/d1");
    assert_eq!(held.episodes.load(&episodes).await.unwrap().len(), 3);
    let facts: usize = held
        .memory
        .load(&memory)
        .await
        .unwrap()
        .iter()
        .map(|block| block.entries.len())
        .sum();
    assert_eq!(facts, 3, "one fact per episode");
    let windows = count(&path, "harvest_agent_transcripts", "session", "desk/d1");
    assert!(windows > 0, "each op kept a window under its session");

    sqlite::send(
        &mut rt,
        &agent(),
        &AgentOp::Control(Control::ResetMemory),
        None,
    )
    .unwrap();
    sqlite::drive(&mut rt, &agent()).await.unwrap();

    let state = state_of(&rt, &agent()).await;
    assert_eq!(state.failures, 0, "{:?}", state.last_error);
    let reset = state.episode_reset.expect("the episode count");
    assert_eq!(reset.episodes, 3);
    assert_eq!(reset.facts, 3);
    assert_eq!(state.memory_entries_erased, Some(3));
    assert_eq!(
        state.transcripts_erased,
        Some(u64::try_from(windows).unwrap())
    );

    assert_eq!(
        count(&path, "harvest_agent_transcripts", "session", "desk/d1"),
        0,
        "no window of the agent"
    );
    assert_eq!(
        count(&path, "harvest_agent_store_docs", "scope", "agent:desk/d1"),
        0,
        "no memory row of the agent"
    );
    let conn = rusqlite::Connection::open(&path).unwrap();
    let body: Vec<u8> = conn
        .query_row(
            "SELECT body FROM harvest_agent_store_docs WHERE scope = ?1",
            ["agent:desk/d1:episodes"],
            |row| row.get(0),
        )
        .unwrap();
    let body = String::from_utf8(body).unwrap();
    assert!(
        !body.contains("noted"),
        "the tombstone holds no episode text: {body}"
    );
    let log = held.episodes.load(&episodes).await.unwrap();
    assert!(log.is_empty());
    assert_eq!(log.generation(), 1, "the tombstone keeps the generation");
}

// ---------------------------------------------------------------------------
// A crash after the append commits.
// ---------------------------------------------------------------------------

const DB_ENV: &str = "AGENT_STORES_CRASH_DB";

/// A `SQLite` episode store that aborts the process after its first append
/// commits, before the activity can report.
#[derive(Debug)]
struct AbortAfterAppend(SqliteEpisodeStore);

impl EpisodeStore for AbortAfterAppend {
    fn load<'a>(&'a self, scope: &'a MemoryScope) -> BoxFuture<'a, Result<EpisodeLog, AgentError>> {
        self.0.load(scope)
    }

    fn append<'a>(
        &'a self,
        scope: &'a MemoryScope,
        key: &'a EditKey,
        episode: Episode,
        max_episodes: u32,
    ) -> BoxFuture<'a, Result<Appended, AgentError>> {
        Box::pin(async move {
            let appended = self.0.append(scope, key, episode, max_episodes).await;
            assert!(appended.is_ok(), "{appended:?}");
            std::process::abort();
        })
    }

    fn mark<'a>(
        &'a self,
        scope: &'a MemoryScope,
        mark: PassMark,
    ) -> BoxFuture<'a, Result<bool, AgentError>> {
        self.0.mark(scope, mark)
    }

    fn clear<'a>(
        &'a self,
        scope: &'a MemoryScope,
        key: &'a EditKey,
    ) -> BoxFuture<'a, Result<Cleared, AgentError>> {
        self.0.clear(scope, key)
    }
}

/// The child half of the crash test. It aborts inside the append.
#[tokio::test(flavor = "multi_thread")]
async fn child_aborts_after_the_append_commits() {
    let Some(db) = std::env::var_os(DB_ENV) else {
        return;
    };
    let path = PathBuf::from(db);
    let episodes = AbortAfterAppend(SqliteEpisodeStore::open(&path).unwrap());
    let harness = AgentHarness::new(model())
        .memory(Arc::new(SqliteMemoryStore::open(&path).unwrap()))
        .episodes(Arc::new(episodes))
        .agent(
            AgentProfile::new("desk")
                .memory(AgentMemory::Store)
                .episodes(EpisodeSettings::default()),
        );
    let mut rt = runtime(&path, harness);
    sqlite::send(&mut rt, &agent(), &AgentOp::message("hello"), None).unwrap();
    let _ = sqlite::drive(&mut rt, &agent()).await;
    panic!("the child must abort inside the append");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_crash_after_the_sqlite_append_does_not_duplicate_it() {
    let (_dir, path) = fresh_db();
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "child_aborts_after_the_append_commits",
            "--nocapture",
        ])
        .env(DB_ENV, &path)
        .status()
        .unwrap();
    assert!(!status.success(), "the child must crash, not finish");
    let scope = MemoryScope::new("agent:desk/d1:episodes");
    let committed = SqliteEpisodeStore::open(&path).unwrap();
    assert_eq!(
        committed.load(&scope).await.unwrap().len(),
        1,
        "the child committed the append before the crash"
    );

    // A new process replays the op. The append activity runs again with
    // the same key.
    let model = model();
    let harness = AgentHarness::new(model.clone())
        .memory(Arc::new(SqliteMemoryStore::open(&path).unwrap()))
        .episodes(Arc::new(SqliteEpisodeStore::open(&path).unwrap()))
        .agent(
            AgentProfile::new("desk")
                .memory(AgentMemory::Store)
                .episodes(EpisodeSettings::default()),
        );
    let mut rt = runtime(&path, harness);
    sqlite::drive(&mut rt, &agent()).await.unwrap();

    let state = state_of(&rt, &agent()).await;
    assert_eq!(state.failures, 0, "{:?}", state.last_error);
    assert_eq!(state.last_answer.as_deref(), Some("noted"));
    assert_eq!(
        model.requests().len(),
        0,
        "the recorded turn is not sent again"
    );
    let log = SqliteEpisodeStore::open(&path)
        .unwrap()
        .load(&scope)
        .await
        .unwrap();
    assert_eq!(log.len(), 1, "one op, one episode");
}
