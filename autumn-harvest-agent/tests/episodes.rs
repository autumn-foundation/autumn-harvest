//! The episode log and the consolidation pass (issue #2454).
//!
//! With `AgentProfile::episodes`, each op appends one episode to the scope
//! `agent:<id>:episodes`. The workflow `agent_consolidate` turns the
//! unconsolidated episodes into facts in the memory of the agent.
//!
//! The crash tests run this binary again as a child process. The child
//! aborts inside a store call. The stores keep their state in a file, so
//! the parent reads what the child wrote.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::failure::IntoActivityErrorString as _;
use autumn_harvest::llm_budget::LlmBudgetExceeded;
use autumn_harvest::quota::QuotaResource;
use autumn_harvest::testing::WorkflowTestEnv;
use autumn_harvest_agent::consolidate::{
    CONSOLIDATE_WORKFLOW_NAME, ConsolidateRead, ConsolidationReport, ConsolidationStop,
    agent_consolidate_info,
};
use autumn_harvest_agent::episode::{
    Appended, Cleared, Episode, EpisodeLog, EpisodeSettings, EpisodeStore, InMemoryEpisodeStore,
    PassMark,
};
use autumn_harvest_agent::memory::{MemoryOp, apply_to};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentId, AgentMemory, AgentModel, AgentOp, AgentProfile, AgentState,
    AgentStop, BoxFuture, ChatRequest, ChatResponse, ChatRole, ContentPart, Control, EditKey,
    FlowRules, FnTool, InMemoryMemoryStore, MemoryBlock, MemoryScope, MemoryStore, Rule,
    StopReason, TokenUsage, ToolEffect, sqlite,
};
use autumn_harvest_sqlite::{ExecutionFilter, ExecutionId, SqliteRuntime};
use common::{Reply, TryFnModel, runtime};
use serde::{Deserialize, Serialize};
use serde_json::json;

const LABEL: &str = "untrusted_input";

fn fresh_db() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

fn agent() -> AgentId {
    AgentId::new("desk", "d1").unwrap()
}

fn memory_scope() -> MemoryScope {
    MemoryScope::new("agent:desk/d1")
}

fn episode_scope() -> MemoryScope {
    MemoryScope::new("agent:desk/d1:episodes")
}

fn profile(settings: Option<EpisodeSettings>) -> AgentProfile {
    let profile = AgentProfile::new("desk").memory(AgentMemory::Store);
    match settings {
        Some(settings) => profile.episodes(settings),
        None => profile,
    }
}

/// The text of the first user message of a request.
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

/// The episode ids that a consolidation request lists, in order.
fn episode_ids(request: &ChatRequest) -> Vec<String> {
    user_text(request)
        .split("<episode id=\"")
        .skip(1)
        .filter_map(|rest| rest.split('"').next().map(str::to_owned))
        .collect()
}

fn is_pass(request: &ChatRequest) -> bool {
    user_text(request).contains("<episode id=\"")
}

fn text_reply(text: &str, input: u32, output: u32) -> ChatResponse {
    ChatResponse {
        content: vec![ContentPart::Text(text.to_owned())],
        stop_reason: StopReason::EndTurn,
        usage: TokenUsage::new(input, output),
    }
}

/// The default fact reply: one fact from the first two episodes, one from
/// the last one.
fn two_facts(ids: &[String]) -> String {
    let first: Vec<&String> = ids.iter().take(2).collect();
    let last: Vec<&String> = ids.iter().rev().take(1).collect();
    json!({"facts": [
        {"subject": "ada", "relation": "likes", "object": "green tea", "episodes": first},
        {"subject": "ada", "relation": "works_at", "object": "acme", "episodes": last},
    ]})
    .to_string()
}

/// A model that answers each op with `answer <n>`, and each pass with
/// `facts` of the listed episode ids.
fn model(facts: fn(&[String]) -> String) -> Arc<TryFnModel> {
    let ops = Arc::new(AtomicU32::new(0));
    TryFnModel::new(move |request| -> Reply {
        if is_pass(request) {
            return Ok(text_reply(&facts(&episode_ids(request)), 40, 9));
        }
        let n = ops.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(text_reply(&format!("answer {n}"), 3, 1))
    })
}

fn pass_count(model: &TryFnModel) -> usize {
    model
        .requests()
        .iter()
        .filter(|request| is_pass(request))
        .count()
}

async fn say(rt: &mut SqliteRuntime, text: &str) {
    sqlite::send(rt, &agent(), &AgentOp::message(text), None).unwrap();
    sqlite::drive(rt, &agent()).await.unwrap();
}

fn state_of(rt: &SqliteRuntime, id: &AgentId) -> impl Future<Output = AgentState> + Send + 'static {
    let query = Box::pin(sqlite::state(rt, id));
    async move { query.await.unwrap().unwrap() }
}

/// The entries of the `memory` block of the agent.
async fn facts(store: &dyn MemoryStore) -> MemoryBlock {
    store
        .load(&memory_scope())
        .await
        .unwrap()
        .into_iter()
        .find(|block| block.label == "memory")
        .unwrap()
}

/// The reports of every pass, in start order.
fn pass_reports(rt: &SqliteRuntime) -> Vec<ConsolidationReport> {
    let filter = ExecutionFilter::new().workflow_name(CONSOLIDATE_WORKFLOW_NAME);
    let mut runs: Vec<ExecutionId> = rt
        .list_executions(&filter)
        .unwrap()
        .executions
        .iter()
        .filter_map(autumn_harvest_sqlite::ExecutionSummary::execution_id)
        .collect();
    runs.sort_by_key(ToString::to_string);
    runs.into_iter()
        .filter_map(|exec| report_of(rt, exec))
        .collect()
}

fn report_of(rt: &SqliteRuntime, exec: ExecutionId) -> Option<ConsolidationReport> {
    rt.load_history(exec)
        .unwrap()
        .into_iter()
        .find_map(|event| match event {
            WorkflowEvent::WorkflowCompleted { output } => {
                Some(serde_json::from_value(output).unwrap())
            }
            _ => None,
        })
}

fn episode(id: &str, text: &str, labels: &[&str]) -> Episode {
    Episode {
        id: id.to_owned(),
        text: text.to_owned(),
        stop: AgentStop::Completed,
        at_ms: 1_700_000_000_000,
        labels: labels.iter().map(|label| (*label).to_owned()).collect(),
    }
}

async fn seed(store: &dyn EpisodeStore, episodes: &[Episode]) {
    for item in episodes {
        let key = EditKey::new(format!("episode:{}", item.id));
        store
            .append(&episode_scope(), &key, item.clone(), 200)
            .await
            .unwrap();
    }
}

// ---------------------------------------------------------------------------
// The episode log.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn with_the_setting_off_an_op_writes_no_episode() {
    let (_dir, db) = fresh_db();
    let episodes = Arc::new(InMemoryEpisodeStore::default());
    let harness = AgentHarness::new(model(two_facts))
        .memory(Arc::new(InMemoryMemoryStore::new()))
        .episodes(episodes.clone())
        .agent(profile(None));
    let mut rt = runtime(&db, harness);
    say(&mut rt, "hello").await;

    assert_eq!(
        state_of(&rt, &agent()).await.last_answer.as_deref(),
        Some("answer 1")
    );
    assert_eq!(episodes.load(&episode_scope()).await.unwrap().len(), 0);
    let exec = sqlite::current_run(&rt, &agent()).unwrap().unwrap();
    let appends = rt
        .load_history(exec)
        .unwrap()
        .iter()
        .filter(|event| {
            matches!(event, WorkflowEvent::ActivityScheduled { name, .. }
                if name == "agent_episode_append")
        })
        .count();
    assert_eq!(appends, 0, "no new activity, so old runs replay");
}

#[tokio::test(flavor = "multi_thread")]
async fn with_the_setting_on_each_op_appends_one_episode() {
    let (_dir, db) = fresh_db();
    let episodes = Arc::new(InMemoryEpisodeStore::default());
    let harness = AgentHarness::new(model(two_facts))
        .memory(Arc::new(InMemoryMemoryStore::new()))
        .episodes(episodes.clone())
        .agent(profile(Some(EpisodeSettings::default())));
    let mut rt = runtime(&db, harness);
    say(&mut rt, "one").await;
    say(&mut rt, "two").await;

    let log = episodes.load(&episode_scope()).await.unwrap();
    let kept: Vec<&Episode> = log.episodes().collect();
    assert_eq!(kept.len(), 2);
    assert_eq!(kept[0].text, "answer 1");
    assert_eq!(kept[1].text, "answer 2");
    assert_ne!(kept[0].id, kept[1].id);
    assert!(kept[0].id.ends_with(":s0"), "{}", kept[0].id);
    assert_eq!(kept[0].stop, AgentStop::Completed);
    assert!(kept[0].at_ms > 0);
    assert_eq!(log.pending(10).len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_episode_keeps_the_flow_labels_of_its_op() {
    let (_dir, db) = fresh_db();
    let episodes = Arc::new(InMemoryEpisodeStore::default());
    let inbox = FnTool::new(
        "inbox",
        "Read mail.",
        json!({"type": "object"}),
        |_| async { Ok(json!("mail from a stranger")) },
    )
    .effect(ToolEffect::ReadOnly)
    .label(LABEL)
    .shared();
    let model = common::ScriptedModel::new(vec![
        common::calls(&[("i1", "inbox", json!({}))], 1),
        common::answer("read it", 1),
    ]);
    // The harness tracks labels only under flow rules.
    let rules = FlowRules::new().forbid(
        [LABEL, "effect:external"],
        Rule::Deny("no send after untrusted input".into()),
    );
    let harness = AgentHarness::new(model)
        .tool(inbox)
        .flow_rules(rules)
        .memory(Arc::new(InMemoryMemoryStore::new()))
        .episodes(episodes.clone())
        .agent(profile(Some(EpisodeSettings::default())));
    let mut rt = runtime(&db, harness);
    say(&mut rt, "read my mail").await;

    let log = episodes.load(&episode_scope()).await.unwrap();
    let kept: Vec<&Episode> = log.episodes().collect();
    assert_eq!(kept.len(), 1);
    assert!(kept[0].labels.contains(LABEL), "{:?}", kept[0].labels);
}

#[tokio::test(flavor = "multi_thread")]
async fn episodes_without_a_memory_store_fail_the_op_with_a_named_error() {
    let (_dir, db) = fresh_db();
    let harness = AgentHarness::new(model(two_facts))
        .episodes(Arc::new(InMemoryEpisodeStore::default()))
        .agent(
            AgentProfile::new("desk")
                .memory(AgentMemory::State)
                .episodes(EpisodeSettings::default()),
        );
    let mut rt = runtime(&db, harness);
    say(&mut rt, "hello").await;
    let state = state_of(&rt, &agent()).await;
    assert_eq!(state.failures, 1);
    assert!(
        state
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("AgentMemory::Store")),
        "{:?}",
        state.last_error
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_third_episode_evicts_the_oldest_and_the_next_pass_counts_it() {
    let (_dir, db) = fresh_db();
    let episodes = Arc::new(InMemoryEpisodeStore::default());
    let memory = Arc::new(InMemoryMemoryStore::new());
    let model = model(two_facts);
    let harness = AgentHarness::new(model.clone())
        .memory(memory.clone())
        .episodes(episodes.clone())
        .agent(profile(Some(EpisodeSettings::default().max_episodes(2))));
    let mut rt = runtime(&db, harness);
    for text in ["one", "two", "three"] {
        say(&mut rt, text).await;
    }
    let log = episodes.load(&episode_scope()).await.unwrap();
    let texts: Vec<&str> = log.episodes().map(|item| item.text.as_str()).collect();
    assert_eq!(texts, ["answer 2", "answer 3"], "the oldest goes first");
    assert_eq!(log.dropped, 1);

    let exec = sqlite::consolidate(&mut rt, &agent()).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    let report = report_of(&rt, exec).unwrap();
    assert_eq!(report.stop, ConsolidationStop::Completed);
    assert_eq!(report.episodes, 2);
    assert_eq!(report.dropped, 1, "the next pass counts the eviction");
    assert_eq!(episodes.load(&episode_scope()).await.unwrap().dropped, 0);
    assert_eq!(pass_count(&model), 1);
}

// ---------------------------------------------------------------------------
// The consolidation pass.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_pass_over_three_episodes_writes_facts_with_their_sources() {
    let (_dir, db) = fresh_db();
    let episodes = Arc::new(InMemoryEpisodeStore::default());
    let memory = Arc::new(InMemoryMemoryStore::new());
    let model = model(two_facts);
    let harness = AgentHarness::new(model.clone())
        .memory(memory.clone())
        .episodes(episodes.clone())
        .agent(profile(Some(EpisodeSettings::default().every(3))));
    let mut rt = runtime(&db, harness);
    say(&mut rt, "one").await;
    say(&mut rt, "two").await;
    assert_eq!(pass_count(&model), 0, "two episodes do not trigger");
    say(&mut rt, "three").await;
    assert_eq!(pass_count(&model), 1, "the third episode triggers a pass");

    let ids: Vec<String> = episodes
        .load(&episode_scope())
        .await
        .unwrap()
        .episodes()
        .map(|item| item.id.clone())
        .collect();
    let block = facts(memory.as_ref()).await;
    assert_eq!(
        block.entries,
        [
            format!("ada likes green tea [episodes: {}, {}]", ids[0], ids[1]),
            format!("ada works_at acme [episodes: {}]", ids[2]),
        ]
    );
    let log = episodes.load(&episode_scope()).await.unwrap();
    assert_eq!(log.pending(10).len(), 0, "every episode is consolidated");

    let reports = pass_reports(&rt);
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].stop, ConsolidationStop::Completed);
    assert_eq!(reports[0].episodes, 3);
    assert_eq!(reports[0].facts_written, 2);
    assert_eq!(state_of(&rt, &agent()).await.failures, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_fact_with_no_known_source_is_rejected() {
    let (_dir, db) = fresh_db();
    let episodes = Arc::new(InMemoryEpisodeStore::default());
    let memory = Arc::new(InMemoryMemoryStore::new());
    seed(episodes.as_ref(), &[episode("e1", "ada likes tea", &[])]).await;
    let model = model(|ids| {
        json!({"facts": [
            {"subject": "ada", "relation": "likes", "object": "tea", "episodes": ids},
            {"subject": "bob", "relation": "likes", "object": "milk", "episodes": ["nope"]},
        ]})
        .to_string()
    });
    let harness = AgentHarness::new(model)
        .memory(memory.clone())
        .episodes(episodes)
        .agent(profile(Some(EpisodeSettings::default())));
    let mut rt = runtime(&db, harness);
    let exec = sqlite::consolidate(&mut rt, &agent()).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    let report = report_of(&rt, exec).unwrap();
    assert_eq!(report.facts_written, 1);
    assert_eq!(report.facts_rejected, 1);
    assert_eq!(
        facts(memory.as_ref()).await.entries,
        ["ada likes tea [episodes: e1]"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reply_that_does_not_parse_marks_no_episode() {
    let (_dir, db) = fresh_db();
    let episodes = Arc::new(InMemoryEpisodeStore::default());
    seed(episodes.as_ref(), &[episode("e1", "ada likes tea", &[])]).await;
    let harness = AgentHarness::new(model(|_| "no facts here".to_owned()))
        .memory(Arc::new(InMemoryMemoryStore::new()))
        .episodes(episodes.clone())
        .agent(profile(Some(EpisodeSettings::default())));
    let mut rt = runtime(&db, harness);
    let exec = sqlite::consolidate(&mut rt, &agent()).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    let report = report_of(&rt, exec).unwrap();
    assert_eq!(report.stop, ConsolidationStop::InvalidReply);
    let log = episodes.load(&episode_scope()).await.unwrap();
    assert_eq!(log.pending(10).len(), 1, "the next pass reads it again");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pass_with_no_episodes_calls_no_model() {
    let (_dir, db) = fresh_db();
    let model = model(two_facts);
    let harness = AgentHarness::new(model.clone())
        .memory(Arc::new(InMemoryMemoryStore::new()))
        .episodes(Arc::new(InMemoryEpisodeStore::default()))
        .agent(profile(Some(EpisodeSettings::default())));
    let mut rt = runtime(&db, harness);
    let exec = sqlite::consolidate(&mut rt, &agent()).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    let report = report_of(&rt, exec).unwrap();
    assert_eq!(report.stop, ConsolidationStop::NothingToDo);
    assert_eq!(model.requests().len(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pass_of_a_kind_with_no_setting_is_disabled() {
    let (_dir, db) = fresh_db();
    let model = model(two_facts);
    let harness = AgentHarness::new(model.clone())
        .memory(Arc::new(InMemoryMemoryStore::new()))
        .episodes(Arc::new(InMemoryEpisodeStore::default()))
        .agent(profile(None));
    let mut rt = runtime(&db, harness);
    let exec = sqlite::consolidate(&mut rt, &agent()).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    assert_eq!(
        report_of(&rt, exec).unwrap().stop,
        ConsolidationStop::Disabled
    );
    assert_eq!(model.requests().len(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_fact_from_a_labelled_episode_carries_its_labels() {
    let (_dir, db) = fresh_db();
    let episodes = Arc::new(InMemoryEpisodeStore::default());
    let memory = Arc::new(InMemoryMemoryStore::new());
    seed(
        episodes.as_ref(),
        &[
            episode("e1", "a stranger asked for a refund", &[LABEL]),
            episode("e2", "ada likes tea", &["pii"]),
        ],
    )
    .await;
    // The model names only e2. The fact still read e1, so it takes both.
    let model = model(|_| {
        json!({"facts": [
            {"subject": "ada", "relation": "likes", "object": "tea", "episodes": ["e2"]},
        ]})
        .to_string()
    });
    let harness = AgentHarness::new(model)
        .memory(memory.clone())
        .episodes(episodes)
        .agent(profile(Some(EpisodeSettings::default())));
    let mut rt = runtime(&db, harness);
    let exec = sqlite::consolidate(&mut rt, &agent()).unwrap();
    rt.run_until_blocked(exec).await.unwrap();

    let block = facts(memory.as_ref()).await;
    let entry = "ada likes tea [episodes: e2]";
    assert_eq!(block.entries, [entry]);
    let want: BTreeSet<String> = [LABEL, "pii"].iter().map(|l| (*l).to_owned()).collect();
    assert_eq!(block.labels.get(entry), Some(&want));
}

/// A memory store that keeps no labels and implements only `apply`.
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
async fn a_store_without_labels_refuses_a_labelled_fact() {
    let (_dir, db) = fresh_db();
    let episodes = Arc::new(InMemoryEpisodeStore::default());
    let memory = Arc::new(PlainStore::default());
    seed(
        episodes.as_ref(),
        &[episode("e1", "a stranger asked for a refund", &[LABEL])],
    )
    .await;
    let harness = AgentHarness::new(model(two_facts))
        .memory(memory.clone())
        .episodes(episodes)
        .agent(profile(Some(EpisodeSettings::default())));
    let mut rt = runtime(&db, harness);
    let exec = sqlite::consolidate(&mut rt, &agent()).unwrap();
    rt.run_until_blocked(exec).await.unwrap();

    let report = report_of(&rt, exec).unwrap();
    assert_eq!(report.facts_written, 0);
    assert_eq!(report.facts_refused, 2);
    assert!(
        report
            .refusals
            .iter()
            .all(|reason| reason.contains("flow labels")),
        "{:?}",
        report.refusals
    );
    assert_eq!(facts(memory.as_ref()).await.entries.len(), 0, "fail closed");
}

// ---------------------------------------------------------------------------
// The budget and the cost ledger.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_refused_turn_ends_the_pass_as_budget_exceeded() {
    let refusal = LlmBudgetExceeded {
        resource: QuotaResource::RunLlmTokens,
        limit: 0,
        current: 0,
    }
    .into_failure()
    .into_error_payload();
    let read = ConsolidateRead::new(
        EpisodeSettings::default(),
        memory_scope(),
        episode_scope(),
        vec![episode("e1", "ada likes tea", &[])],
        0,
    );
    let outcome = WorkflowTestEnv::new()
        .mock_activity("agent_consolidate_read", move |_| {
            Ok(serde_json::to_value(&read).unwrap())
        })
        .mock_activity_retries("agent_consolidate_turn", vec![Err(refusal)])
        .run(
            agent_consolidate_info().handler,
            json!({"agent": "desk/d1"}),
        )
        .await;
    let report: ConsolidationReport =
        serde_json::from_value(outcome.result.clone().unwrap()).unwrap();
    assert_eq!(report.stop, ConsolidationStop::BudgetExceeded);
    assert_eq!(report.facts_written, 0);
    let writes = outcome
        .events()
        .iter()
        .filter(|event| {
            matches!(event, WorkflowEvent::ActivityScheduled { name, .. }
                if name == "agent_consolidate_write")
        })
        .count();
    assert_eq!(writes, 0, "a refused pass writes no fact");
}

/// A model that names itself and prices each call.
#[derive(Debug)]
struct Priced(Arc<TryFnModel>);

impl AgentModel for Priced {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        self.0.chat(request)
    }

    fn model_id(&self) -> Option<&str> {
        Some("priced-model-1")
    }

    fn cost_usd_micros(&self, usage: &TokenUsage) -> Option<u64> {
        Some(u64::from(usage.input_tokens) * 3 + u64::from(usage.output_tokens) * 15)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_pass_tokens_are_in_the_ledger_under_its_run() {
    let (_dir, db) = fresh_db();
    let episodes = Arc::new(InMemoryEpisodeStore::default());
    seed(episodes.as_ref(), &[episode("e1", "ada likes tea", &[])]).await;
    let harness = AgentHarness::new(Arc::new(Priced(model(two_facts))))
        .memory(Arc::new(InMemoryMemoryStore::new()))
        .episodes(episodes)
        .agent(profile(Some(EpisodeSettings::default())));
    let mut rt = runtime(&db, harness);
    let exec = sqlite::consolidate(&mut rt, &agent()).unwrap();
    rt.run_until_blocked(exec).await.unwrap();

    let rows = rt.llm_ledger(exec).unwrap();
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0].model, "priced-model-1");
    assert_eq!(rows[0].input_tokens, 40);
    assert_eq!(rows[0].output_tokens, 9);
    assert_eq!(rows[0].cost_usd_micros, Some(40 * 3 + 9 * 15));
    let report = report_of(&rt, exec).unwrap();
    assert_eq!(report.usage, TokenUsage::new(40, 9));
}

// ---------------------------------------------------------------------------
// Erase.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_memory_reset_removes_episodes_and_facts_and_counts_them() {
    let (_dir, db) = fresh_db();
    let episodes = Arc::new(InMemoryEpisodeStore::default());
    let memory = Arc::new(InMemoryMemoryStore::new());
    let harness = AgentHarness::new(model(two_facts))
        .memory(memory.clone())
        .episodes(episodes.clone())
        .agent(profile(Some(EpisodeSettings::default().every(3))));
    let mut rt = runtime(&db, harness);
    for text in ["one", "two", "three"] {
        say(&mut rt, text).await;
    }
    assert_eq!(facts(memory.as_ref()).await.entries.len(), 2);

    sqlite::send(
        &mut rt,
        &agent(),
        &AgentOp::Control(Control::ResetMemory),
        None,
    )
    .unwrap();
    sqlite::drive(&mut rt, &agent()).await.unwrap();

    let state = state_of(&rt, &agent()).await;
    let reset = state.episode_reset.expect("the reset reports its counts");
    assert_eq!(reset.episodes, 3);
    assert_eq!(reset.facts, 2);
    assert_eq!(state.failures, 0, "{:?}", state.last_error);
    assert_eq!(episodes.load(&episode_scope()).await.unwrap().len(), 0);
    assert_eq!(facts(memory.as_ref()).await.entries.len(), 0);
}

// ---------------------------------------------------------------------------
// Crashes. The stores keep their state in a file.
// ---------------------------------------------------------------------------

/// What [`FileStores`] keeps on disk.
#[derive(Debug, Default, Serialize, Deserialize)]
struct Disk {
    episodes: BTreeMap<String, EpisodeLog>,
    memory: BTreeMap<String, Vec<MemoryBlock>>,
    applied: BTreeSet<String>,
}

/// An episode store and a memory store in one JSON file. With `abort` set,
/// the process aborts after that store call saves.
#[derive(Debug)]
struct FileStores {
    path: PathBuf,
    abort: Option<Abort>,
    writes: Mutex<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Abort {
    /// After the first episode append.
    Append,
    /// After the first fact write.
    Fact,
}

impl FileStores {
    fn new(path: &Path, abort: Option<Abort>) -> Arc<Self> {
        Arc::new(Self {
            path: path.to_owned(),
            abort,
            writes: Mutex::new(0),
        })
    }

    fn read(&self) -> Disk {
        std::fs::read(&self.path)
            .ok()
            .map(|bytes| serde_json::from_slice(&bytes).unwrap())
            .unwrap_or_default()
    }

    fn save(&self, disk: &Disk, after: Abort) {
        std::fs::write(&self.path, serde_json::to_vec(disk).unwrap()).unwrap();
        if self.abort == Some(after) {
            std::process::abort();
        }
    }
}

impl EpisodeStore for FileStores {
    fn load<'a>(&'a self, scope: &'a MemoryScope) -> BoxFuture<'a, Result<EpisodeLog, AgentError>> {
        let log = self
            .read()
            .episodes
            .get(scope.as_str())
            .cloned()
            .unwrap_or_default();
        Box::pin(std::future::ready(Ok(log)))
    }

    fn append<'a>(
        &'a self,
        scope: &'a MemoryScope,
        key: &'a EditKey,
        episode: Episode,
        max_episodes: u32,
    ) -> BoxFuture<'a, Result<Appended, AgentError>> {
        let mut disk = self.read();
        let log = disk.episodes.entry(scope.as_str().to_owned()).or_default();
        let appended = log.append(key, episode, max_episodes);
        self.save(&disk, Abort::Append);
        Box::pin(std::future::ready(Ok(appended)))
    }

    fn mark<'a>(
        &'a self,
        scope: &'a MemoryScope,
        mark: PassMark,
    ) -> BoxFuture<'a, Result<(), AgentError>> {
        let mut disk = self.read();
        disk.episodes
            .entry(scope.as_str().to_owned())
            .or_default()
            .mark(&mark);
        self.save(&disk, Abort::Append);
        Box::pin(std::future::ready(Ok(())))
    }

    fn clear<'a>(
        &'a self,
        scope: &'a MemoryScope,
        key: &'a EditKey,
    ) -> BoxFuture<'a, Result<Cleared, AgentError>> {
        let mut disk = self.read();
        let log = disk.episodes.remove(scope.as_str()).unwrap_or_default();
        let _ = key;
        let cleared = log.into_cleared();
        self.save(&disk, Abort::Append);
        Box::pin(std::future::ready(Ok(cleared)))
    }
}

impl MemoryStore for FileStores {
    fn load<'a>(
        &'a self,
        scope: &'a MemoryScope,
    ) -> BoxFuture<'a, Result<Vec<MemoryBlock>, AgentError>> {
        let blocks = self
            .read()
            .memory
            .get(scope.as_str())
            .cloned()
            .unwrap_or_else(MemoryBlock::defaults);
        Box::pin(std::future::ready(Ok(blocks)))
    }

    fn apply<'a>(
        &'a self,
        scope: &'a MemoryScope,
        op: MemoryOp,
        key: &'a EditKey,
    ) -> BoxFuture<'a, Result<MemoryBlock, AgentError>> {
        self.apply_labelled_now(scope, &op, key, &BTreeSet::new())
    }

    fn apply_labelled<'a>(
        &'a self,
        scope: &'a MemoryScope,
        op: MemoryOp,
        key: &'a EditKey,
        labels: &'a BTreeSet<String>,
    ) -> BoxFuture<'a, Result<MemoryBlock, AgentError>> {
        self.apply_labelled_now(scope, &op, key, labels)
    }

    fn clear<'a>(&'a self, scope: &'a MemoryScope) -> BoxFuture<'a, Result<(), AgentError>> {
        let mut disk = self.read();
        disk.memory.remove(scope.as_str());
        self.save(&disk, Abort::Append);
        Box::pin(std::future::ready(Ok(())))
    }
}

impl FileStores {
    fn apply_labelled_now<'a>(
        &'a self,
        scope: &'a MemoryScope,
        op: &MemoryOp,
        key: &'a EditKey,
        labels: &BTreeSet<String>,
    ) -> BoxFuture<'a, Result<MemoryBlock, AgentError>> {
        let mut disk = self.read();
        let done = format!("{}|{}", scope.as_str(), key.as_str());
        let blocks = disk
            .memory
            .entry(scope.as_str().to_owned())
            .or_insert_with(MemoryBlock::defaults);
        if disk.applied.contains(&done) {
            let block = blocks
                .iter()
                .find(|block| block.label == op.block())
                .cloned()
                .unwrap();
            return Box::pin(std::future::ready(Ok(block)));
        }
        let result = apply_to(blocks, op, labels);
        disk.applied.insert(done);
        let first = {
            let mut writes = self.writes.lock().unwrap();
            *writes += 1;
            *writes == 1
        };
        if first {
            self.save(&disk, Abort::Fact);
        } else {
            self.save(&disk, Abort::Append);
        }
        Box::pin(std::future::ready(result))
    }
}

const DB_ENV: &str = "AGENT_EPISODE_CRASH_DB";
const STORE_ENV: &str = "AGENT_EPISODE_CRASH_STORE";
const ABORT_ENV: &str = "AGENT_EPISODE_CRASH_ABORT";

fn file_harness(stores: &Arc<FileStores>, model: Arc<dyn AgentModel>) -> AgentHarness {
    AgentHarness::new(model)
        .memory(stores.clone())
        .episodes(stores.clone())
        .agent(profile(Some(EpisodeSettings::default())))
}

/// The child half of the crash tests. It aborts inside a store call.
#[tokio::test(flavor = "multi_thread")]
async fn child_runs_until_it_crashes() {
    let (Some(db), Some(store), Some(abort)) = (
        std::env::var_os(DB_ENV),
        std::env::var_os(STORE_ENV),
        std::env::var_os(ABORT_ENV),
    ) else {
        return;
    };
    let abort = if abort == "append" {
        Abort::Append
    } else {
        Abort::Fact
    };
    let stores = FileStores::new(Path::new(&store), Some(abort));
    let mut rt = runtime(Path::new(&db), file_harness(&stores, model(two_facts)));
    match abort {
        Abort::Append => {
            sqlite::send(&mut rt, &agent(), &AgentOp::message("hello"), None).unwrap();
            let _ = sqlite::drive(&mut rt, &agent()).await;
        }
        Abort::Fact => {
            let exec = sqlite::consolidate(&mut rt, &agent()).unwrap();
            let _ = rt.run_until_blocked(exec).await;
        }
    }
    panic!("the child must abort inside the store call");
}

fn crash_child(db: &Path, store: &Path, abort: &str) {
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_runs_until_it_crashes", "--nocapture"])
        .env(DB_ENV, db)
        .env(STORE_ENV, store)
        .env(ABORT_ENV, abort)
        .status()
        .unwrap();
    assert!(!status.success(), "the child must crash, not finish");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_crash_after_the_append_does_not_duplicate_it() {
    let (dir, db) = fresh_db();
    let store = dir.path().join("stores.json");
    crash_child(&db, &store, "append");

    // A new process replays the op. The append runs again with its key.
    let stores = FileStores::new(&store, None);
    let model = model(two_facts);
    let mut rt = runtime(&db, file_harness(&stores, model.clone()));
    sqlite::drive(&mut rt, &agent()).await.unwrap();

    let state = state_of(&rt, &agent()).await;
    assert_eq!(state.last_answer.as_deref(), Some("answer 1"));
    assert_eq!(
        model.requests().len(),
        0,
        "the recorded turn is not sent again"
    );
    let log = EpisodeStore::load(stores.as_ref(), &episode_scope())
        .await
        .unwrap();
    assert_eq!(log.len(), 1, "one op, one episode");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_crash_mid_pass_and_a_rerun_write_no_fact_twice() {
    let (dir, db) = fresh_db();
    let store = dir.path().join("stores.json");
    let seeded = FileStores::new(&store, None);
    seed(
        seeded.as_ref(),
        &[
            episode("e1", "ada likes green tea", &[]),
            episode("e2", "ada drinks green tea", &[]),
            episode("e3", "ada works at acme", &[]),
        ],
    )
    .await;
    crash_child(&db, &store, "fact");

    let stores = FileStores::new(&store, None);
    let model = model(two_facts);
    let mut rt = runtime(&db, file_harness(&stores, model.clone()));
    let filter = ExecutionFilter::new().workflow_name(CONSOLIDATE_WORKFLOW_NAME);
    let exec = rt
        .list_executions(&filter)
        .unwrap()
        .executions
        .iter()
        .find_map(autumn_harvest_sqlite::ExecutionSummary::execution_id)
        .unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    assert_eq!(
        model.requests().len(),
        0,
        "the recorded reply is not sent again"
    );
    let report = report_of(&rt, exec).unwrap();
    assert_eq!(report.stop, ConsolidationStop::Completed);

    // A rerun finds no unconsolidated episode, so it writes nothing.
    let again = sqlite::consolidate(&mut rt, &agent()).unwrap();
    rt.run_until_blocked(again).await.unwrap();
    assert_eq!(
        report_of(&rt, again).unwrap().stop,
        ConsolidationStop::NothingToDo
    );

    let block = facts(stores.as_ref()).await;
    assert_eq!(
        block.entries,
        [
            "ada likes green tea [episodes: e1, e2]",
            "ada works_at acme [episodes: e3]",
        ],
        "each fact once"
    );
}
