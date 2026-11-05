//! One consolidation schedule for many agents (issue #2540).
//!
//! The workflow `agent_consolidate_all` lists the running agents of each
//! kind with the trigger `Schedule`. It starts one pass for each agent with
//! pending episodes, up to `max_passes`. The tests drive it on SQLite, with
//! `sqlite::Directory` as the agent directory.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use autumn_harvest::policy::Schedule;
use autumn_harvest_agent::consolidate::{
    self, CONSOLIDATE_WORKFLOW_NAME, CONSOLIDATION_ATTR, ConsolidationStop,
};
use autumn_harvest_agent::consolidate_all::{
    CONSOLIDATE_ALL_WORKFLOW_NAME, ConsolidateAll, DispatchReport,
};
use autumn_harvest_agent::episode::{EpisodeSettings, EpisodeStore, InMemoryEpisodeStore};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentId, AgentMemory, AgentOp, AgentProfile, ChatRequest,
    ChatResponse, ChatRole, ContentPart, ErrorKind, InMemoryMemoryStore, Mandate, MemoryScope,
    MemoryStore, StopReason, TokenUsage, sqlite,
};
use autumn_harvest_sqlite::{ExecutionFilter, RunState, ScheduleTrigger, SqliteRuntime};
use common::{Reply, TryFnModel, runtime};
use serde_json::json;

fn fresh_db() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

fn desk(name: &str) -> AgentId {
    AgentId::new("desk", name).unwrap()
}

fn scheduled(kind: &str) -> AgentProfile {
    AgentProfile::new(kind)
        .memory(AgentMemory::Store)
        .episodes(EpisodeSettings::default())
}

/// The text of the user messages of a request.
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

fn is_pass(request: &ChatRequest) -> bool {
    user_text(request).contains("<episode id=\"")
}

/// The episode ids that a pass request lists.
fn episode_ids(request: &ChatRequest) -> Vec<String> {
    user_text(request)
        .split("<episode id=\"")
        .skip(1)
        .filter_map(|rest| rest.split('"').next().map(str::to_owned))
        .collect()
}

fn text_reply(text: &str) -> ChatResponse {
    ChatResponse {
        content: vec![ContentPart::Text(text.to_owned())],
        stop_reason: StopReason::EndTurn,
        usage: TokenUsage::new(4, 2),
    }
}

/// A model that echoes each op, and answers each pass with one fact.
fn echo_model() -> Arc<TryFnModel> {
    TryFnModel::new(|request| -> Reply { Ok(answer(request)) })
}

fn answer(request: &ChatRequest) -> ChatResponse {
    if is_pass(request) {
        let facts = json!({"facts": [
            {"subject": "ada", "relation": "likes", "object": "tea", "episodes": episode_ids(request)},
        ]});
        return text_reply(&facts.to_string());
    }
    text_reply(&format!("re: {}", user_text(request)))
}

fn pass_requests(model: &TryFnModel) -> usize {
    model
        .requests()
        .iter()
        .filter(|request| is_pass(request))
        .count()
}

/// A harness with the stores, one scheduled kind `desk`, and the SQLite
/// directory of `db`.
fn harness(
    db: &Path,
    model: Arc<TryFnModel>,
    episodes: &Arc<InMemoryEpisodeStore>,
    memory: &Arc<InMemoryMemoryStore>,
) -> AgentHarness {
    AgentHarness::new(model)
        .memory(memory.clone())
        .episodes(episodes.clone())
        .directory(Arc::new(sqlite::Directory::open(db)))
        .agent(scheduled("desk"))
}

/// Send one message to `id` and run its op. Wait a little, so the next
/// episode has a later time.
async fn say(rt: &mut SqliteRuntime, id: &AgentId, text: &str) {
    sqlite::send(rt, id, &AgentOp::message(text), None).unwrap();
    sqlite::drive(rt, id).await.unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
}

async fn fire(rt: &mut SqliteRuntime, task: &ConsolidateAll) -> DispatchReport {
    let exec = sqlite::consolidate_all(rt, task).unwrap();
    match rt.run_until_blocked(exec).await.unwrap() {
        RunState::Completed(value) => serde_json::from_value(value).unwrap(),
        other => panic!("the dispatcher did not complete: {other:?}"),
    }
}

async fn pending(store: &InMemoryEpisodeStore, id: &AgentId) -> usize {
    store
        .load(&consolidate::episode_scope(id))
        .await
        .unwrap()
        .pending(100)
        .len()
}

async fn facts(store: &InMemoryMemoryStore, id: &AgentId) -> Vec<String> {
    store
        .load(&MemoryScope::new(format!("agent:{id}")))
        .await
        .unwrap()
        .into_iter()
        .find(|block| block.label == "memory")
        .map(|block| block.entries)
        .unwrap_or_default()
}

/// The passes of `id` that publish `agent_consolidation_of`.
fn passes_of(rt: &SqliteRuntime, id: &AgentId) -> usize {
    let filter = ExecutionFilter::new()
        .workflow_name(CONSOLIDATE_WORKFLOW_NAME)
        .search_attribute(CONSOLIDATION_ATTR, json!(id.as_str()));
    rt.list_executions(&filter).unwrap().executions.len()
}

fn started(report: &DispatchReport) -> Vec<String> {
    report
        .passes
        .iter()
        .map(|pass| pass.agent.as_str().to_owned())
        .collect()
}

// ---------------------------------------------------------------------------
// Acceptance criteria.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn two_scheduled_agents_both_get_a_pass_on_one_firing() {
    let (_dir, db) = fresh_db();
    let episodes = Arc::new(InMemoryEpisodeStore::default());
    let memory = Arc::new(InMemoryMemoryStore::new());
    let model = echo_model();
    let mut rt = runtime(&db, harness(&db, model.clone(), &episodes, &memory));
    let (d1, d2) = (desk("d1"), desk("d2"));
    for round in 0..2 {
        say(&mut rt, &d1, &format!("one {round}")).await;
        say(&mut rt, &d2, &format!("two {round}")).await;
        assert_eq!(pending(&episodes, &d1).await, 1);
        assert_eq!(pending(&episodes, &d2).await, 1);

        let report = fire(&mut rt, &ConsolidateAll::default()).await;
        assert_eq!(report.listed, 2, "{report:?}");
        assert_eq!(report.started, 2, "{report:?}");
        assert_eq!(report.waiting, 0);
        assert_eq!(started(&report), ["desk/d1", "desk/d2"]);
        for pass in &report.passes {
            assert_eq!(pass.stop, Some(ConsolidationStop::Completed), "{pass:?}");
        }
        assert_eq!(pending(&episodes, &d1).await, 0, "round {round}");
        assert_eq!(pending(&episodes, &d2).await, 0, "round {round}");
    }
    assert_eq!(facts(&memory, &d1).await.len(), 2);
    assert_eq!(facts(&memory, &d2).await.len(), 2);
    assert_eq!(pass_requests(&model), 4, "one pass per agent per firing");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_schedule_fires_one_dispatcher_for_every_agent() {
    let (_dir, db) = fresh_db();
    let episodes = Arc::new(InMemoryEpisodeStore::default());
    let memory = Arc::new(InMemoryMemoryStore::new());
    let mut rt = runtime(&db, harness(&db, echo_model(), &episodes, &memory));
    let (d1, d2) = (desk("d1"), desk("d2"));
    say(&mut rt, &d1, "one").await;
    say(&mut rt, &d2, "two").await;

    let hourly = Schedule::Interval(Duration::from_secs(3_600));
    let schedule = consolidate::schedule(hourly, &ConsolidateAll::new().max_passes(8)).unwrap();
    assert_eq!(schedule.workflow_name, CONSOLIDATE_ALL_WORKFLOW_NAME);
    rt.upsert_schedule(&schedule).unwrap();
    assert_eq!(rt.list_schedules().unwrap().len(), 1, "one schedule");

    let fired = rt
        .trigger_schedule(CONSOLIDATE_ALL_WORKFLOW_NAME, false, None)
        .unwrap();
    let ScheduleTrigger::Fired { exec_id, .. } = fired else {
        panic!("the schedule did not fire: {fired:?}");
    };
    let RunState::Completed(value) = rt.run_until_blocked(exec_id).await.unwrap() else {
        panic!("the dispatcher did not complete");
    };
    let report: DispatchReport = serde_json::from_value(value).unwrap();
    assert_eq!(report.started, 2, "{report:?}");
    assert_eq!(facts(&memory, &d1).await.len(), 1);
    assert_eq!(facts(&memory, &d2).await.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_idle_agent_costs_no_model_call_and_no_pass() {
    let (_dir, db) = fresh_db();
    let episodes = Arc::new(InMemoryEpisodeStore::default());
    let memory = Arc::new(InMemoryMemoryStore::new());
    let model = echo_model();
    let mut rt = runtime(&db, harness(&db, model.clone(), &episodes, &memory));
    let (d1, d2) = (desk("d1"), desk("d2"));
    say(&mut rt, &d1, "one").await;
    say(&mut rt, &d2, "two").await;
    fire(&mut rt, &ConsolidateAll::default()).await;
    assert_eq!(pass_requests(&model), 2);

    // Only d1 has a new episode. d2 is idle.
    say(&mut rt, &d1, "three").await;
    let calls = model.requests().len();
    let report = fire(&mut rt, &ConsolidateAll::default()).await;
    assert_eq!(report.listed, 2, "{report:?}");
    assert_eq!(report.idle, 1, "{report:?}");
    assert_eq!(started(&report), ["desk/d1"]);
    assert_eq!(model.requests().len(), calls + 1, "one pass call only");
    assert_eq!(passes_of(&rt, &d1), 2);
    assert_eq!(passes_of(&rt, &d2), 1, "the idle agent gets no pass run");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_agent_whose_pass_still_runs_gets_no_second_pass() {
    let (_dir, db) = fresh_db();
    let episodes = Arc::new(InMemoryEpisodeStore::default());
    let memory = Arc::new(InMemoryMemoryStore::new());
    // The first pass call fails with a retryable error, so that pass waits
    // for its retry and still runs.
    let failed = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&failed);
    let model = TryFnModel::new(move |request| -> Reply {
        if is_pass(request) && !flag.swap(true, Ordering::SeqCst) {
            return Err(AgentError::new(ErrorKind::Unavailable, "busy"));
        }
        Ok(answer(request))
    });
    let mut rt = runtime(&db, harness(&db, model.clone(), &episodes, &memory));
    let (d1, d2) = (desk("d1"), desk("d2"));
    say(&mut rt, &d1, "one").await;
    say(&mut rt, &d2, "two").await;

    let exec = sqlite::consolidate(&mut rt, &d1).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingTimer | RunState::InProgress),
        "the pass of d1 still runs: {state:?}"
    );
    assert!(failed.load(Ordering::SeqCst));
    assert_eq!(passes_of(&rt, &d1), 1, "the pass publishes its agent");

    let report = fire(&mut rt, &ConsolidateAll::default()).await;
    assert_eq!(report.running, 1, "{report:?}");
    assert_eq!(started(&report), ["desk/d2"]);
    assert_eq!(passes_of(&rt, &d1), 1, "d1 gets no second pass");
    assert_eq!(pending(&episodes, &d1).await, 1, "the first pass owns it");
}

#[tokio::test(flavor = "multi_thread")]
async fn one_firing_starts_at_most_max_passes_and_the_rest_wait() {
    let (_dir, db) = fresh_db();
    let episodes = Arc::new(InMemoryEpisodeStore::default());
    let memory = Arc::new(InMemoryMemoryStore::new());
    let model = echo_model();
    let mut rt = runtime(&db, harness(&db, model.clone(), &episodes, &memory));
    // d3 waits longest, then d1, then d2.
    for name in ["d3", "d1", "d2"] {
        say(&mut rt, &desk(name), name).await;
    }
    let mut times = Vec::new();
    for name in ["d3", "d1", "d2"] {
        let log = episodes
            .load(&consolidate::episode_scope(&desk(name)))
            .await
            .unwrap();
        times.push(log.pending(1)[0].at_ms);
    }
    assert!(times[0] < times[1] && times[1] < times[2], "{times:?}");
    let two = ConsolidateAll::new().max_passes(2);

    let first = fire(&mut rt, &two).await;
    assert_eq!(first.due, 3, "{first:?}");
    assert_eq!(first.started, 2);
    assert_eq!(first.waiting, 1);
    assert_eq!(started(&first), ["desk/d3", "desk/d1"], "oldest first");
    assert_eq!(pass_requests(&model), 2);
    assert_eq!(pending(&episodes, &desk("d2")).await, 1, "d2 waits");

    let second = fire(&mut rt, &two).await;
    assert_eq!(started(&second), ["desk/d2"], "{second:?}");
    assert_eq!(second.waiting, 0);
    assert_eq!(second.idle, 2);
    assert_eq!(pending(&episodes, &desk("d2")).await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stuck_agent_does_not_starve_the_others() {
    let (_dir, db) = fresh_db();
    let episodes = Arc::new(InMemoryEpisodeStore::default());
    let memory = Arc::new(InMemoryMemoryStore::new());
    // A pass over the episode `stuck` never parses, so it marks nothing and
    // its episode stays the oldest.
    let model = TryFnModel::new(|request| -> Reply {
        if is_pass(request) && user_text(request).contains("stuck") {
            return Ok(text_reply("no facts here"));
        }
        Ok(answer(request))
    });
    let mut rt = runtime(&db, harness(&db, model, &episodes, &memory));
    say(&mut rt, &desk("d1"), "stuck").await;
    say(&mut rt, &desk("d2"), "fine").await;
    let one = ConsolidateAll::new().max_passes(1);

    let first = fire(&mut rt, &one).await;
    assert_eq!(
        started(&first),
        ["desk/d1"],
        "the oldest episode goes first"
    );
    assert_eq!(first.passes[0].stop, Some(ConsolidationStop::InvalidReply));
    let second = fire(&mut rt, &one).await;
    assert_eq!(
        started(&second),
        ["desk/d2"],
        "the stuck agent waits its turn"
    );
    let third = fire(&mut rt, &one).await;
    assert_eq!(started(&third), ["desk/d1"], "{third:?}");
    assert_eq!(pending(&episodes, &desk("d2")).await, 0);
}

// ---------------------------------------------------------------------------
// Edges.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_kind_with_an_every_trigger_gets_no_dispatched_pass() {
    let (_dir, db) = fresh_db();
    let episodes = Arc::new(InMemoryEpisodeStore::default());
    let memory = Arc::new(InMemoryMemoryStore::new());
    let model = echo_model();
    let every = AgentProfile::new("tick")
        .memory(AgentMemory::Store)
        .episodes(EpisodeSettings::default().every(100));
    let harness = harness(&db, model.clone(), &episodes, &memory).agent(every);
    let mut rt = runtime(&db, harness);
    let tick = AgentId::new("tick", "t1").unwrap();
    say(&mut rt, &tick, "one").await;
    say(&mut rt, &desk("d1"), "two").await;

    let report = fire(&mut rt, &ConsolidateAll::default()).await;
    assert_eq!(report.listed, 1, "{report:?}");
    assert_eq!(started(&report), ["desk/d1"]);
    assert_eq!(
        pending(&episodes, &tick).await,
        1,
        "its own trigger owns it"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_pass_does_not_stop_the_others() {
    let (_dir, db) = fresh_db();
    let episodes = Arc::new(InMemoryEpisodeStore::default());
    let memory = Arc::new(InMemoryMemoryStore::new());
    let model = TryFnModel::new(|request| -> Reply {
        if is_pass(request) && user_text(request).contains("boom") {
            return Err(AgentError::new(ErrorKind::Provider, "refused"));
        }
        Ok(answer(request))
    });
    let mut rt = runtime(&db, harness(&db, model, &episodes, &memory));
    say(&mut rt, &desk("d1"), "boom").await;
    say(&mut rt, &desk("d2"), "fine").await;

    let report = fire(&mut rt, &ConsolidateAll::default()).await;
    assert_eq!(report.started, 2, "{report:?}");
    let d1 = &report.passes[0];
    assert_eq!(d1.stop, None);
    assert!(
        d1.error.as_deref().unwrap_or("").contains("refused"),
        "{d1:?}"
    );
    assert_eq!(report.passes[1].stop, Some(ConsolidationStop::Completed));
    assert_eq!(report.failed, 1);
    assert_eq!(pending(&episodes, &desk("d2")).await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dispatcher_with_no_directory_fails_with_a_named_error() {
    let (_dir, db) = fresh_db();
    let harness = AgentHarness::new(echo_model())
        .memory(Arc::new(InMemoryMemoryStore::new()))
        .episodes(Arc::new(InMemoryEpisodeStore::default()))
        .agent(scheduled("desk"));
    let mut rt = runtime(&db, harness);
    let exec = sqlite::consolidate_all(&mut rt, &ConsolidateAll::default()).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    let RunState::Failed(error) = state else {
        panic!("want a failed run, got {state:?}");
    };
    // SQLite keeps the message of the named error. It names the fix.
    assert!(error.contains("call AgentHarness::directory"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_kind_whose_mandate_forbids_the_pass_model_is_not_served() {
    let (_dir, db) = fresh_db();
    let episodes = Arc::new(InMemoryEpisodeStore::default());
    let memory = Arc::new(InMemoryMemoryStore::new());
    let model = echo_model();
    let forbidden = AgentProfile::new("locked")
        .memory(AgentMemory::Store)
        .episodes(EpisodeSettings::default().model("cheap"))
        .mandate(Mandate::new("locked").models(["main"]));
    let harness = harness(&db, model.clone(), &episodes, &memory)
        .model_named("cheap", model.clone())
        .model_named("main", model.clone())
        .agent(forbidden);
    let mut rt = runtime(&db, harness);
    let locked = AgentId::new("locked", "l1").unwrap();
    say(&mut rt, &locked, "one").await;
    say(&mut rt, &desk("d1"), "two").await;

    let report = fire(&mut rt, &ConsolidateAll::default()).await;
    assert_eq!(report.listed, 1, "{report:?}");
    assert_eq!(started(&report), ["desk/d1"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_harness_with_no_scheduled_kind_starts_nothing() {
    let (_dir, db) = fresh_db();
    let model = echo_model();
    let harness = AgentHarness::new(model.clone()).agent(AgentProfile::new("plain"));
    let mut rt = runtime(&db, harness);
    let report = fire(&mut rt, &ConsolidateAll::default()).await;
    assert_eq!(report, DispatchReport::default());
    assert_eq!(model.requests().len(), 0);
}

#[test]
fn the_task_decodes_from_an_empty_object() {
    let task: ConsolidateAll = serde_json::from_str("{}").unwrap();
    assert_eq!(task, ConsolidateAll::default());
    assert_eq!(task.max_passes, 16);
}
