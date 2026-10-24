//! Persistent workspaces of durable agents (issue #2274).
//!
//! The SQLite tests keep the blobs next to the database, so a fresh runtime
//! sees what an earlier one saved. A drive at a later clock lets a retry
//! backoff end.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use autumn_harvest::aead_codec::{AeadCodec, DataKey};
use autumn_harvest::chrono::{DateTime, Utc};
use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest_agent::flow::labels;
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentHeartbeat, AgentId, AgentOp, AgentProfile, AgentState,
    BoxFuture, ChatRole, ContentPart, Control, DirWorkspaceBlobs, ErrorKind, FlowRules, FnTool,
    InMemoryWorkspaceBlobs, Rule, Tool, ToolEffect, WorkspaceBlobs, WorkspaceMount, WorkspaceStore,
    sqlite,
};
use autumn_harvest_sqlite::{RunState, SqliteRuntime};
use common::{ScriptedModel, answer, calls, runtime};
use serde_json::json;

fn fresh_db() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

fn state(rt: &SqliteRuntime, agent: &AgentId) -> impl Future<Output = AgentState> + Send + 'static {
    let query = sqlite::state(rt, agent);
    async move { query.await.unwrap().expect("the agent runs") }
}

/// Drive the agent at the drive clock `at`, through each checkpoint.
async fn drive_at(rt: &mut SqliteRuntime, agent: &AgentId, at: DateTime<Utc>) -> RunState {
    let mut exec = sqlite::current_run(rt, agent).unwrap().unwrap();
    loop {
        match rt.run_until_blocked_as_of(exec, at).await.unwrap() {
            RunState::ContinuedAsNew(next) => exec = next,
            other => return other,
        }
    }
}

/// Send `text` and drive the agent until it is idle. A later clock lets
/// each retry backoff end.
async fn say(rt: &mut SqliteRuntime, agent: &AgentId, text: &str) -> AgentState {
    sqlite::send(rt, agent, &AgentOp::message(text), None).unwrap();
    let mut at = Utc::now();
    for _ in 0..5 {
        drive_at(rt, agent, at).await;
        at += chrono::Duration::minutes(5);
    }
    state(rt, agent).await
}

/// `write_file`: write `text` at `path`. The text may hold `<n>`, which
/// becomes the number of this execution of the tool.
fn write_tool(runs: Arc<AtomicUsize>) -> Arc<dyn Tool> {
    FnTool::with_context(
        "write_file",
        "Write a file in the workspace.",
        json!({"type": "object"}),
        move |input, ctx| {
            let n = runs.fetch_add(1, Ordering::SeqCst) + 1;
            async move {
                let path = input["path"].as_str().unwrap_or_default().to_owned();
                let text = input["text"]
                    .as_str()
                    .unwrap_or_default()
                    .replace("<n>", &n.to_string());
                let workspace = ctx.workspace.expect("the tool uses the workspace");
                workspace.with(|files| files.write(&path, text))?;
                Ok(json!({"written": path}))
            }
        },
    )
    .effect(ToolEffect::Internal)
    .uses_workspace()
    .shared()
}

/// `read_file`: the text at `path`, or `null`.
fn read_tool() -> Arc<dyn Tool> {
    FnTool::with_context(
        "read_file",
        "Read a file in the workspace.",
        json!({"type": "object"}),
        |input, ctx| async move {
            let path = input["path"].as_str().unwrap_or_default().to_owned();
            let workspace = ctx.workspace.expect("the tool uses the workspace");
            Ok(json!(
                workspace.with(|files| files.read_text(&path).map(str::to_owned))
            ))
        },
    )
    .effect(ToolEffect::ReadOnly)
    .uses_workspace()
    .shared()
}

fn harness(
    model: Arc<ScriptedModel>,
    store: WorkspaceStore,
    runs: &Arc<AtomicUsize>,
) -> AgentHarness {
    AgentHarness::new(model)
        .tool(write_tool(runs.clone()))
        .tool(read_tool())
        .workspace(store)
        .agent(AgentProfile::new("coder").workspace())
}

fn write(id: &str, path: &str, text: &str) -> common::Reply {
    calls(
        &[(id, "write_file", json!({"path": path, "text": text}))],
        1,
    )
}

fn read(id: &str, path: &str) -> common::Reply {
    calls(&[(id, "read_file", json!({"path": path}))], 1)
}

/// The text of every tool result in the transcript, in order.
fn tool_results(state: &AgentState) -> Vec<String> {
    state
        .transcript
        .iter()
        .filter(|m| m.role == ChatRole::Tool)
        .flat_map(|m| &m.content)
        .filter_map(|part| match part {
            ContentPart::ToolResult { content, .. } => Some(content.clone()),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_file_from_turn_one_is_visible_in_turn_five_after_a_restart() {
    let (dir, db) = fresh_db();
    let blobs = dir.path().join("workspaces");
    let store = || {
        WorkspaceStore::new(
            Arc::new(DirWorkspaceBlobs::new(&blobs)),
            PayloadCodecs::default(),
        )
    };
    let agent = AgentId::new("coder", "ada").unwrap();
    let runs = Arc::new(AtomicUsize::new(0));
    {
        let model = ScriptedModel::new(vec![
            write("w1", "notes.txt", "remember the flag"),
            answer("saved", 1),
        ]);
        let mut rt = runtime(&db, harness(model, store(), &runs));
        let first = say(&mut rt, &agent, "turn 1").await;
        assert!(first.workspace.is_some(), "turn 1 saved a snapshot");
    }

    // A fresh runtime, as after a worker restart.
    let model = ScriptedModel::new(vec![
        answer("ok 2", 1),
        answer("ok 3", 1),
        answer("ok 4", 1),
        read("r5", "notes.txt"),
        answer("found it", 1),
    ]);
    let mut rt = runtime(&db, harness(model, store(), &runs));
    for turn in 2..=4 {
        say(&mut rt, &agent, &format!("turn {turn}")).await;
    }
    let fifth = say(&mut rt, &agent, "turn 5").await;
    let results = tool_results(&fifth);
    assert!(
        results
            .last()
            .is_some_and(|r| r.contains("remember the flag")),
        "{results:?}"
    );
    assert_eq!(fifth.last_answer.as_deref(), Some("found it"));
}

/// Blobs whose first put lands but reports a failure, as a lost ack.
#[derive(Debug, Default)]
struct LostAck {
    inner: InMemoryWorkspaceBlobs,
    failed: AtomicBool,
}

impl WorkspaceBlobs for LostAck {
    fn put<'a>(
        &'a self,
        scope: &'a str,
        digest: &'a str,
        bytes: Vec<u8>,
    ) -> BoxFuture<'a, Result<(), AgentError>> {
        Box::pin(async move {
            self.inner.put(scope, digest, bytes).await?;
            if self.failed.swap(true, Ordering::SeqCst) {
                Ok(())
            } else {
                Err(AgentError::new(ErrorKind::Unavailable, "the ack was lost"))
            }
        })
    }

    fn get<'a>(
        &'a self,
        scope: &'a str,
        digest: &'a str,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, AgentError>> {
        self.inner.get(scope, digest)
    }

    fn list<'a>(&'a self, scope: &'a str) -> BoxFuture<'a, Result<Vec<String>, AgentError>> {
        self.inner.list(scope)
    }

    fn delete<'a>(
        &'a self,
        scope: &'a str,
        digest: &'a str,
    ) -> BoxFuture<'a, Result<bool, AgentError>> {
        self.inner.delete(scope, digest)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stale_attempt_never_becomes_the_head() {
    let (_dir, db) = fresh_db();
    let blobs = Arc::new(LostAck::default());
    let store = WorkspaceStore::new(blobs.clone(), PayloadCodecs::default());
    let agent = AgentId::new("coder", "bo").unwrap();
    let runs = Arc::new(AtomicUsize::new(0));
    let model = ScriptedModel::new(vec![
        write("w1", "out.txt", "attempt <n>"),
        answer("written", 1),
        read("r2", "out.txt"),
        answer("read", 1),
    ]);
    let mut rt = runtime(&db, harness(model, store, &runs));
    let first = say(&mut rt, &agent, "write it").await;
    assert_eq!(
        runs.load(Ordering::SeqCst),
        2,
        "the lost ack retried the call"
    );
    let head = first.workspace.expect("a head");
    let scope = WorkspaceMount::agent_scope(agent.as_str());
    assert_eq!(
        blobs.list(&scope).await.unwrap(),
        [head.key()],
        "the op end deleted the snapshot of the stale attempt"
    );

    let second = say(&mut rt, &agent, "read it").await;
    let results = tool_results(&second);
    assert!(
        results.last().is_some_and(|r| r.contains("attempt 2")),
        "the head is the recorded attempt: {results:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reset_removes_every_snapshot_and_the_store_holds_only_ciphertext() {
    let (_dir, db) = fresh_db();
    let codecs = PayloadCodecs::default();
    AeadCodec::new("k1", &DataKey::generate())
        .unwrap()
        .register_with(&codecs)
        .unwrap();
    codecs.set_active_key("k1").unwrap();
    let blobs = Arc::new(InMemoryWorkspaceBlobs::default());
    let store = WorkspaceStore::new(blobs.clone(), codecs);
    let agent = AgentId::new("coder", "cy").unwrap();
    let runs = Arc::new(AtomicUsize::new(0));
    let model = ScriptedModel::new(vec![
        write("w1", "a.txt", "secret-alpha"),
        answer("one", 1),
        write("w2", "b.txt", "secret-beta"),
        answer("two", 1),
    ]);
    let mut rt = runtime(&db, harness(model, store, &runs));
    say(&mut rt, &agent, "one").await;
    let two = say(&mut rt, &agent, "two").await;
    assert_eq!(two.workspace.as_ref().map(|head| head.files), Some(2));
    for raw in blobs.raw() {
        let text = String::from_utf8_lossy(&raw);
        assert!(!text.contains("secret"), "a snapshot is in clear: {text}");
    }
    // A snapshot that no recorded result names, as from a lost attempt.
    let scope = WorkspaceMount::agent_scope(agent.as_str());
    blobs
        .put(&scope, &"f".repeat(64), b"orphan".to_vec())
        .await
        .unwrap();

    sqlite::send(
        &mut rt,
        &agent,
        &AgentOp::Control(Control::ResetWorkspace),
        None,
    )
    .unwrap();
    sqlite::drive(&mut rt, &agent).await.unwrap();
    let reset = state(&rt, &agent).await;
    assert_eq!(reset.workspace, None);
    assert_eq!(held_keys(&blobs, &scope).await, Vec::<String>::new());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reset_on_the_dir_store_leaves_no_bytes_of_a_dropped_save() {
    let (dir, db) = fresh_db();
    let root = dir.path().join("workspaces");
    let store = WorkspaceStore::new(
        Arc::new(DirWorkspaceBlobs::new(&root)),
        PayloadCodecs::default(),
    );
    let agent = AgentId::new("coder", "drop").unwrap();
    let runs = Arc::new(AtomicUsize::new(0));
    let model = ScriptedModel::new(vec![write("w1", "a.txt", "secret-alpha"), answer("one", 1)]);
    let mut rt = runtime(&db, harness(model, store, &runs));
    let head = say(&mut rt, &agent, "one").await.workspace.expect("a head");
    // A save that timed out leaves the scratch name of its key.
    let folder = std::fs::read_dir(&root)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let name = format!("{}.tmp-{}", head.key(), "a".repeat(32));
    std::fs::write(folder.join(name), b"secret-orphan").unwrap();

    sqlite::send(
        &mut rt,
        &agent,
        &AgentOp::Control(Control::ResetWorkspace),
        None,
    )
    .unwrap();
    sqlite::drive(&mut rt, &agent).await.unwrap();
    assert_eq!(held_files(&root), Vec::<PathBuf>::new());
}

/// Every file under `root` that holds bytes. A reset leaves only an empty
/// marker.
fn held_files(root: &std::path::Path) -> Vec<PathBuf> {
    let mut held = Vec::new();
    for folder in std::fs::read_dir(root).unwrap() {
        for file in std::fs::read_dir(folder.unwrap().path()).unwrap() {
            let path = file.unwrap().path();
            if std::fs::metadata(&path).unwrap().len() > 0 {
                held.push(path);
            }
        }
    }
    held
}

/// The keys of `scope` that hold bytes. A reset leaves only an empty
/// marker, which keeps its bound.
async fn held_keys(blobs: &InMemoryWorkspaceBlobs, scope: &str) -> Vec<String> {
    let mut held = Vec::new();
    for key in blobs.list(scope).await.unwrap() {
        let bytes = blobs.get(scope, &key).await.unwrap().unwrap_or_default();
        if !bytes.is_empty() {
            held.push(key);
        }
    }
    held
}

/// A store whose blobs live in memory, with no codec.
fn memory_store() -> WorkspaceStore {
    WorkspaceStore::new(
        Arc::new(InMemoryWorkspaceBlobs::default()),
        PayloadCodecs::default(),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_only_tick_runs_a_workspace_tool_but_keeps_the_head() {
    let (_dir, db) = fresh_db();
    let agent = AgentId::new("coder", "di").unwrap();
    let runs = Arc::new(AtomicUsize::new(0));
    let model = ScriptedModel::new(vec![
        write("w1", "notes.txt", "the first draft"),
        answer("saved", 1),
        // A tick is read-only. Its write runs, but the head stays.
        write("w2", "notes.txt", "a draft from a tick"),
        answer("HEARTBEAT_OK", 1),
        read("r3", "notes.txt"),
        answer("read", 1),
    ]);
    let profile = AgentProfile::new("coder")
        .workspace()
        .heartbeat(AgentHeartbeat::every(Duration::from_secs(60)));
    let harness = AgentHarness::new(model)
        .tool(write_tool(runs.clone()))
        .tool(read_tool())
        .workspace(memory_store())
        .agent(profile);
    let mut rt = runtime(&db, harness);
    let start = Utc::now();
    sqlite::send(&mut rt, &agent, &AgentOp::message("write it"), None).unwrap();
    drive_at(&mut rt, &agent, start).await;
    let head = state(&rt, &agent)
        .await
        .workspace
        .expect("the op saved a snapshot");

    drive_at(&mut rt, &agent, start + chrono::Duration::seconds(90)).await;
    let ticked = state(&rt, &agent).await;
    assert_eq!(ticked.ticks, 1);
    assert_eq!(runs.load(Ordering::SeqCst), 2, "the tick ran the tool");
    assert_eq!(ticked.workspace, Some(head), "the tick keeps the head");

    sqlite::send(&mut rt, &agent, &AgentOp::message("read it"), None).unwrap();
    drive_at(&mut rt, &agent, start + chrono::Duration::seconds(100)).await;
    let results = tool_results(&state(&rt, &agent).await);
    assert!(
        results
            .last()
            .is_some_and(|r| r.contains("the first draft")),
        "{results:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn clear_transcript_keeps_the_labels_of_the_workspace_files() {
    let (_dir, db) = fresh_db();
    let agent = AgentId::new("coder", "ed").unwrap();
    let runs = Arc::new(AtomicUsize::new(0));
    let inbox = FnTool::new(
        "inbox",
        "Read mail.",
        json!({"type": "object"}),
        |_| async { Ok(json!("mail from a stranger")) },
    )
    .effect(ToolEffect::ReadOnly)
    .label(labels::UNTRUSTED_INPUT)
    .shared();
    let model = ScriptedModel::new(vec![
        calls(&[("i1", "inbox", json!({}))], 1),
        write("w1", "mail.txt", "a copy of the mail"),
        answer("filed", 1),
    ]);
    // A flow rule turns the labels on. It never fires here.
    let rules = FlowRules::new().forbid(
        [labels::UNTRUSTED_INPUT, "effect:external"],
        Rule::Deny("no send after untrusted input".into()),
    );
    let harness = AgentHarness::new(model)
        .tool(inbox)
        .tool(write_tool(runs))
        .workspace(memory_store())
        .flow_rules(rules)
        .agent(AgentProfile::new("coder").workspace());
    let mut rt = runtime(&db, harness);
    let filed = say(&mut rt, &agent, "file the mail").await;
    assert!(
        filed.workspace_labels.contains(labels::UNTRUSTED_INPUT),
        "{:?}",
        filed.workspace_labels
    );

    let clear = AgentOp::Control(Control::ClearTranscript);
    sqlite::send(&mut rt, &agent, &clear, None).unwrap();
    sqlite::drive(&mut rt, &agent).await.unwrap();
    let cleared = state(&rt, &agent).await;
    assert_eq!(
        cleared.transcript,
        Vec::<autumn_harvest_agent::ChatMessage>::new()
    );
    assert!(
        cleared.flow_labels.contains(labels::UNTRUSTED_INPUT),
        "the files keep the taint of the run that wrote them: {:?}",
        cleared.flow_labels
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_late_retention_of_an_old_op_keeps_the_newest_head() {
    let (_dir, db) = fresh_db();
    let blobs = Arc::new(InMemoryWorkspaceBlobs::default());
    let store = WorkspaceStore::new(blobs.clone(), PayloadCodecs::default());
    let agent = AgentId::new("coder", "late").unwrap();
    let runs = Arc::new(AtomicUsize::new(0));
    let model = ScriptedModel::new(vec![
        write("w1", "a.txt", "one"),
        answer("written", 1),
        write("w2", "a.txt", "two"),
        answer("written again", 1),
    ]);
    let mut rt = runtime(&db, harness(model, store.clone(), &runs));
    let first = say(&mut rt, &agent, "write one").await;
    let first = first.workspace.expect("a head");
    let second = say(&mut rt, &agent, "write two").await;
    let second = second.workspace.expect("a head");
    assert!(second.generation > first.generation);

    // An attempt of the retention of the first op runs after the second op.
    let scope = WorkspaceMount::agent_scope(agent.as_str());
    store.retain(&scope, &first).await.unwrap();
    let mount = WorkspaceMount {
        scope: scope.clone(),
        head: Some(second.clone()),
        generation: 0,
    };
    let loaded = store.load(&mount).await.unwrap();
    assert_eq!(loaded.read_text("a.txt"), Some("two"));
    assert_eq!(blobs.list(&scope).await.unwrap(), [second.key()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reset_in_a_new_life_deletes_the_keys_of_an_old_life_on_a_fast_clock() {
    let (_dir, db) = fresh_db();
    let blobs = Arc::new(InMemoryWorkspaceBlobs::default());
    let store = WorkspaceStore::new(blobs.clone(), PayloadCodecs::default());
    let agent = AgentId::new("coder", "skew").unwrap();
    let runs = Arc::new(AtomicUsize::new(0));
    let model = ScriptedModel::new(vec![write("w1", "a.txt", "old"), answer("written", 1)]);
    let mut rt = runtime(&db, harness(model, store, &runs));
    let now = Utc::now();
    // The first life runs on a clock that is an hour fast.
    let fast = now + chrono::Duration::hours(1);
    sqlite::send(&mut rt, &agent, &AgentOp::message("write it"), None).unwrap();
    drive_at(&mut rt, &agent, fast).await;
    let scope = WorkspaceMount::agent_scope(agent.as_str());
    assert_eq!(
        blobs.list(&scope).await.unwrap().len(),
        1,
        "the first life saved"
    );
    sqlite::delete(&mut rt, &agent, None).unwrap();
    drive_at(&mut rt, &agent, fast).await;

    // A second life starts on a correct clock and resets, as an erase does.
    sqlite::send(
        &mut rt,
        &agent,
        &AgentOp::Control(Control::ResetWorkspace),
        None,
    )
    .unwrap();
    drive_at(&mut rt, &agent, now).await;
    assert_eq!(
        held_keys(&blobs, &scope).await,
        Vec::<String>::new(),
        "the reset deleted the key of the first life"
    );
}
