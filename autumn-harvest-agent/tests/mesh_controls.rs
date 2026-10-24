//! Controls that span a whole mesh of durable agents (issue #2272).
//!
//! - A mesh budget for each period parks the agents of a mesh.
//! - `pause_mesh` stops every agent of a mesh, with an audit record.
//! - A memory entry keeps the flow labels of the run that wrote it.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest_agent::flow::labels;
use autumn_harvest_agent::memory::{EditKey, MEMORY_TOOL, MemoryBlock, MemoryOp, MemoryStore};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentHeartbeat, AgentId, AgentMemory, AgentOp, AgentProfile,
    AgentState, AgentStatus, AgentStop, AgentTask, BoxFuture, ChatMessage, ChatRole, ContentPart,
    Control, FlowRules, FnTool, InMemoryMemoryStore, MemoryScope, MeshBudget, Rule, ToolEffect,
    sqlite,
};
use autumn_harvest_sqlite::SqliteRuntime;
use common::{Reply, ScriptedModel, answer, calls, report, runtime};
use serde_json::json;

fn fresh_db() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

fn id(name: &str) -> AgentId {
    AgentId::new("desk", name).unwrap()
}

fn state(rt: &SqliteRuntime, agent: &AgentId) -> impl Future<Output = AgentState> + Send + 'static {
    let query = sqlite::state(rt, agent);
    async move { query.await.unwrap().unwrap() }
}

async fn say(rt: &mut SqliteRuntime, agent: &AgentId, text: &str) {
    sqlite::send(rt, agent, &AgentOp::message(text), None).unwrap();
    sqlite::drive(rt, agent).await.unwrap();
}

/// The texts of the held user messages of `state`, in order.
fn held_texts(state: &AgentState) -> Vec<String> {
    state
        .held
        .iter()
        .filter_map(|op| match op {
            AgentOp::UserMessage(message) => Some(message.text.clone()),
            _ => None,
        })
        .collect()
}

/// The user texts of `messages`, in order.
fn user_texts(messages: &[ChatMessage]) -> Vec<String> {
    messages
        .iter()
        .filter(|message| message.role == ChatRole::User)
        .filter_map(|message| match message.content.first() {
            Some(ContentPart::Text(text)) => Some(text.clone()),
            _ => None,
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Period budgets
// ---------------------------------------------------------------------------

fn budget_harness(
    db: &std::path::Path,
    model: Arc<ScriptedModel>,
    budget: MeshBudget,
) -> AgentHarness {
    AgentHarness::new(model)
        .agent(AgentProfile::new("desk").mesh("support"))
        .mesh_budget(budget)
        .mesh_spend(Arc::new(sqlite::LedgerSpend::open(db)))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mesh_over_its_daily_budget_parks_every_agents_next_turn() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::named("model-1", vec![answer("first", 12), answer("never", 1)]);
    let budget = MeshBudget::daily("support").max_tokens(10);
    let mut rt = runtime(&db, budget_harness(&db, model.clone(), budget));
    let (a, b) = (id("a"), id("b"));

    say(&mut rt, &a, "spend").await;
    assert_eq!(model.calls(), 1);
    // The mesh spent 12 of its 10 tokens today. Every next turn is refused.
    say(&mut rt, &b, "hello").await;
    say(&mut rt, &a, "again").await;
    assert_eq!(model.calls(), 1, "no model call after the budget");
    for agent in [&a, &b] {
        let state = state(&rt, agent).await;
        assert!(
            matches!(state.status, AgentStatus::Parked { .. }),
            "{agent}: {:?}",
            state.status
        );
        assert_eq!(state.last_stop, Some(AgentStop::BudgetExceeded));
    }
    // The refused message waits for the next period, first in line. A
    // parked agent holds new work too, instead of running it.
    say(&mut rt, &b, "later").await;
    assert_eq!(held_texts(&state(&rt, &b).await), ["hello", "later"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_message_leaves_the_transcript_as_it_was() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::named("model-1", vec![answer("first", 12)]);
    let budget = MeshBudget::daily("support").max_tokens(10);
    let mut rt = runtime(&db, budget_harness(&db, model.clone(), budget));
    let a = id("a");
    say(&mut rt, &a, "spend").await;
    let before = state(&rt, &a).await.transcript;
    assert_eq!(user_texts(&before), ["spend"]);

    // The mesh spent 12 of its 10 tokens, so the budget refuses the next
    // turn. A resume in the same period runs the held message again, and
    // the budget refuses it again.
    say(&mut rt, &a, "wait").await;
    sqlite::send(&mut rt, &a, &AgentOp::Control(Control::Resume), None).unwrap();
    sqlite::drive(&mut rt, &a).await.unwrap();
    let after = state(&rt, &a).await;
    assert!(
        matches!(after.status, AgentStatus::Parked { .. }),
        "{:?}",
        after.status
    );
    assert_eq!(model.calls(), 1, "no model reads the refused message");
    assert_eq!(
        held_texts(&after),
        ["wait"],
        "the message waits, first in line"
    );
    // No model read the input. The retry appends it once, so the
    // transcript must keep no copy of it.
    assert_eq!(user_texts(&after.transcript), ["spend"]);
    assert_eq!(after.transcript, before);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_parked_agent_opens_when_its_period_ends() {
    // The period is two real seconds, so the test lets it pass.
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::named(
        "model-1",
        vec![answer("first", 12), answer("after the period", 1)],
    );
    let budget = MeshBudget::every("support", Duration::from_secs(2)).max_tokens(10);
    let mut rt = runtime(&db, budget_harness(&db, model.clone(), budget));
    let a = id("a");
    // The windows start at each even second from the epoch. The test starts
    // just after a boundary, so the spend and the refused turn share one
    // window.
    let into = autumn_harvest::chrono::Utc::now()
        .timestamp_millis()
        .rem_euclid(2_000);
    tokio::time::sleep(Duration::from_millis(
        u64::try_from(2_000 - into).unwrap() + 20,
    ))
    .await;
    say(&mut rt, &a, "spend").await;
    say(&mut rt, &a, "wait").await;
    assert!(matches!(
        state(&rt, &a).await.status,
        AgentStatus::Parked { .. }
    ));

    tokio::time::sleep(Duration::from_millis(2_500)).await;
    sqlite::drive(&mut rt, &a).await.unwrap();
    let state = state(&rt, &a).await;
    assert_eq!(state.status, AgentStatus::Open);
    assert_eq!(state.last_answer.as_deref(), Some("after the period"));
}

#[tokio::test(flavor = "multi_thread")]
async fn another_mesh_keeps_its_own_budget() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::named("model-1", vec![answer("first", 12), answer("other", 1)]);
    let harness = AgentHarness::new(model.clone())
        .agent(AgentProfile::new("desk").mesh("support"))
        .agent(AgentProfile::new("lab").mesh("research"))
        .mesh_budget(MeshBudget::daily("support").max_tokens(10))
        .mesh_budget(MeshBudget::daily("research").max_tokens(10))
        .mesh_spend(Arc::new(sqlite::LedgerSpend::open(&db)));
    let mut rt = runtime(&db, harness);
    // The support mesh spends 12 tokens. The research mesh spent none.
    say(&mut rt, &id("a"), "spend").await;
    let lab = AgentId::new("lab", "l1").unwrap();
    say(&mut rt, &lab, "hello").await;
    assert_eq!(state(&rt, &lab).await.last_answer.as_deref(), Some("other"));
}

// ---------------------------------------------------------------------------
// The kill switch
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_pause_stops_all_agents_of_a_mesh_within_one_tick() {
    // The heartbeats are one second apart, so the test lets real time pass.
    let (_dir, db) = fresh_db();
    let replies: Vec<Reply> = (0..40).map(|_| answer("HEARTBEAT_OK", 1)).collect();
    let model = ScriptedModel::new(replies);
    let beat = AgentHeartbeat::every(Duration::from_secs(1));
    let harness = AgentHarness::new(model.clone())
        .agent(AgentProfile::new("desk").mesh("support").heartbeat(beat));
    let mut rt = runtime(&db, harness);
    let agents = [id("a"), id("b"), id("c")];
    for agent in &agents {
        say(&mut rt, agent, "start").await;
    }

    let record = sqlite::pause_mesh(&mut rt, "support", "runaway costs").unwrap();
    assert_eq!(record.mesh, "support");
    assert_eq!(record.reason, "runaway costs");
    let paused: BTreeSet<String> = record.agents.iter().map(ToString::to_string).collect();
    let expected: BTreeSet<String> = agents.iter().map(ToString::to_string).collect();
    assert_eq!(paused, expected, "the audit record names every agent");

    for agent in &agents {
        sqlite::drive(&mut rt, agent).await.unwrap();
    }
    let calls = model.calls();
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    for agent in &agents {
        sqlite::drive(&mut rt, agent).await.unwrap();
        assert!(matches!(
            state(&rt, agent).await.status,
            AgentStatus::Paused { .. }
        ));
    }
    assert_eq!(model.calls(), calls, "no tick runs after the pause");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pause_leaves_other_meshes_alone() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("a", 1), answer("b", 1), answer("b again", 1)]);
    let harness = AgentHarness::new(model.clone())
        .agent(AgentProfile::new("desk").mesh("support"))
        .agent(AgentProfile::new("lab").mesh("research"));
    let mut rt = runtime(&db, harness);
    let lab = AgentId::new("lab", "l1").unwrap();
    say(&mut rt, &id("a"), "hi").await;
    say(&mut rt, &lab, "hi").await;
    let record = sqlite::pause_mesh(&mut rt, "support", "drill").unwrap();
    assert_eq!(record.agents, [id("a")]);
    say(&mut rt, &lab, "still here").await;
    assert_eq!(
        state(&rt, &lab).await.last_answer.as_deref(),
        Some("b again")
    );
}

// ---------------------------------------------------------------------------
// Labelled memory
// ---------------------------------------------------------------------------

/// A read-only tool that returns content from outside.
fn inbox() -> Arc<dyn autumn_harvest_agent::Tool> {
    FnTool::new(
        "inbox",
        "Read mail.",
        json!({"type": "object"}),
        |_| async { Ok(json!("mail from a stranger")) },
    )
    .effect(ToolEffect::ReadOnly)
    .label(labels::UNTRUSTED_INPUT)
    .shared()
}

/// A tool that acts outside.
fn send() -> Arc<dyn autumn_harvest_agent::Tool> {
    FnTool::new("send", "Send mail.", json!({"type": "object"}), |_| async {
        Ok(json!("sent"))
    })
    .effect(ToolEffect::External)
    .shared()
}

fn rules() -> FlowRules {
    FlowRules::new().forbid(
        [labels::UNTRUSTED_INPUT, "effect:external"],
        Rule::Deny("no send after untrusted input".into()),
    )
}

fn note(text: &str) -> Reply {
    calls(
        &[(
            "m1",
            MEMORY_TOOL,
            json!({"action": "add", "block": "memory", "text": text}),
        )],
        1,
    )
}

fn scope() -> MemoryScope {
    MemoryScope::new("mailbox")
}

#[tokio::test(flavor = "multi_thread")]
async fn a_note_written_under_untrusted_input_taints_the_run_that_reads_it() {
    let (_dir, db) = fresh_db();
    let store = Arc::new(InMemoryMemoryStore::new());
    let model = ScriptedModel::new(vec![
        // Run 1 reads untrusted mail, then writes a note.
        calls(&[("i1", "inbox", json!({}))], 1),
        note("the stranger wants a refund"),
        answer("noted", 1),
        // Run 2 reads only its memory, then tries to send.
        calls(&[("s1", "send", json!({}))], 1),
        answer("done", 1),
    ]);
    let harness = AgentHarness::new(model.clone())
        .tool(inbox())
        .tool(send())
        .memory(store.clone())
        .flow_rules(rules());
    let mut rt = runtime(&db, harness);
    let first = AgentTask::new("triage the mail").memory(scope());
    let exec = sqlite::start(&mut rt, &first).unwrap();
    report(rt.run_until_blocked(exec).await.unwrap());

    let blocks = store.load(&scope()).await.unwrap();
    let block = blocks.iter().find(|block| block.label == "memory").unwrap();
    assert!(block.labels["the stranger wants a refund"].contains(labels::UNTRUSTED_INPUT));

    // A new run with no labels of its own.
    let second = AgentTask::new("follow up").memory(scope());
    let exec = sqlite::start(&mut rt, &second).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());
    assert!(report.flow_labels.contains(labels::UNTRUSTED_INPUT));
    let results = common::tool_results(&report);
    assert!(
        results[0].contains("denied"),
        "the send is gated: {results:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_state_memory_note_keeps_its_labels_after_the_transcript_goes() {
    // `ClearTranscript` drops the taint of the transcript. Only the labels
    // that still have a source stay, such as a memory note.
    let script = |with_note: bool| {
        let mut replies = vec![calls(&[("i1", "inbox", json!({}))], 1)];
        if with_note {
            replies.push(note("the stranger wants a refund"));
        }
        replies.push(answer("noted", 1));
        replies.push(calls(&[("s1", "send", json!({}))], 1));
        replies.push(answer("done", 1));
        replies
    };
    let mut results = Vec::new();
    for with_note in [true, false] {
        let (_dir, db) = fresh_db();
        let model = ScriptedModel::new(script(with_note));
        let harness = AgentHarness::new(model.clone())
            .tool(inbox())
            .tool(send())
            .flow_rules(rules())
            .agent(AgentProfile::new("desk").memory(AgentMemory::State));
        let mut rt = runtime(&db, harness);
        let agent = id("mail");
        say(&mut rt, &agent, "triage").await;
        let clear = AgentOp::Control(autumn_harvest_agent::Control::ClearTranscript);
        sqlite::send(&mut rt, &agent, &clear, None).unwrap();
        sqlite::drive(&mut rt, &agent).await.unwrap();
        let labels_left = state(&rt, &agent).await.flow_labels;
        assert_eq!(labels_left.contains(labels::UNTRUSTED_INPUT), with_note);

        say(&mut rt, &agent, "follow up").await;
        let last = model.requests().pop().unwrap();
        results.push(serde_json::to_string(&last.messages).unwrap());
    }
    assert!(
        results[0].contains("denied"),
        "the note taints: {}",
        results[0]
    );
    assert!(
        !results[1].contains("denied"),
        "no note, no taint: {}",
        results[1]
    );
}

/// A store that keeps no labels and implements only `apply`.
#[derive(Debug, Default)]
struct PlainStore(InMemoryMemoryStore);

impl MemoryStore for PlainStore {
    fn load<'a>(
        &'a self,
        scope: &'a MemoryScope,
    ) -> BoxFuture<'a, Result<Vec<MemoryBlock>, AgentError>> {
        self.0.load(scope)
    }

    fn apply<'a>(
        &'a self,
        scope: &'a MemoryScope,
        op: MemoryOp,
        key: &'a EditKey,
    ) -> BoxFuture<'a, Result<MemoryBlock, AgentError>> {
        self.0.apply(scope, op, key)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_store_that_cannot_keep_labels_refuses_a_labelled_write() {
    let (_dir, db) = fresh_db();
    let store = Arc::new(PlainStore::default());
    let model = ScriptedModel::new(vec![
        calls(&[("i1", "inbox", json!({}))], 1),
        note("the stranger wants a refund"),
        answer("noted", 1),
    ]);
    let harness = AgentHarness::new(model.clone())
        .tool(inbox())
        .memory(store.clone())
        .flow_rules(rules());
    let mut rt = runtime(&db, harness);
    let exec = sqlite::start(&mut rt, &AgentTask::new("triage").memory(scope())).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());
    let results = common::tool_results(&report);
    assert!(results[1].contains("flow labels"), "{results:?}");
    let blocks = store.load(&scope()).await.unwrap();
    assert!(
        blocks.iter().all(|block| block.entries.is_empty()),
        "fail closed"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pause_stops_an_agent_that_waits_for_an_approval() {
    // The interrupt of the pause ends the wait. Without it, the agent would
    // wait for the approval and only then take the pause.
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(common::Recorder::default());
    let model = ScriptedModel::new(vec![
        calls(&[("w1", "write", json!({"x": 1}))], 1),
        answer("stopped", 1),
    ]);
    let harness = AgentHarness::new(model.clone())
        .tool(common::recorded_tool("write", ToolEffect::Write, &recorder))
        .policy(Arc::new(
            autumn_harvest_agent::ToolRules::new().effect(ToolEffect::Write, Rule::Ask),
        ))
        .agent(AgentProfile::new("desk").mesh("support"));
    let mut rt = runtime(&db, harness);
    let agent = id("gated");
    say(&mut rt, &agent, "write it").await;
    assert_eq!(
        state(&rt, &agent).await.status,
        AgentStatus::Open,
        "the op waits"
    );

    sqlite::pause_mesh(&mut rt, "support", "drill").unwrap();
    sqlite::drive(&mut rt, &agent).await.unwrap();
    let state = state(&rt, &agent).await;
    assert!(
        matches!(state.status, AgentStatus::Paused { .. }),
        "{:?}",
        state.status
    );
    assert_eq!(state.last_stop, Some(AgentStop::Interrupted));
    assert!(recorder.runs().is_empty(), "the gated write never ran");
}
