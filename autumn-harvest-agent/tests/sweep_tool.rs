//! The `sweep` workflow tool on SQLite (issue #2716).
//!
//! A parent run calls `sweep` once. The workflow runs one `agent_sweep`
//! child over the items. `docs/design/DESIGN-2716.md` maps each test to its
//! rule.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::num::NonZeroU32;
use std::sync::Arc;

use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest_agent::sampling::{SweepItemStatus, SweepStop};
use autumn_harvest_agent::workflow::agent_loop_info;
use autumn_harvest_agent::{
    AgentHarness, AgentProfile, AgentReport, AgentStop, AgentTask, Answered, ChatRequest,
    ContentPart, LoopGuard, SWEEP_TOOL, SweepTool, SweepToolResult, sqlite,
};
use common::{TryFnModel, answer, calls, report, runtime, tool_results};
use serde_json::{Value, json};

/// The kind of each item run.
const KIND: &str = "research";

/// The tokens of each model call of an item.
const ITEM_TOKENS: u32 = 7;

/// The label that each sweep result adds to the run.
const OUTSIDE: &str = "outside_text";

fn fresh_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

/// The item text of `request`: the first text part that names a source.
fn item_text(request: &ChatRequest) -> String {
    request
        .messages
        .iter()
        .flat_map(|message| message.content.iter())
        .find_map(|part| match part {
            ContentPart::Text(text) if text.contains("Source: ") => Some(text.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

fn offers_sweep(request: &ChatRequest) -> bool {
    request.tools.iter().any(|tool| tool.name == SWEEP_TOOL)
}

fn has_tool_result(request: &ChatRequest) -> bool {
    request
        .messages
        .iter()
        .flat_map(|message| message.content.iter())
        .any(|part| matches!(part, ContentPart::ToolResult { .. }))
}

/// A model for both sides. The parent calls `sweep` with `arguments` once,
/// then answers `done`. An item answers with the text after `Source: `.
fn model(arguments: Value) -> Arc<TryFnModel> {
    TryFnModel::new(move |request: &ChatRequest| {
        if offers_sweep(request) {
            if has_tool_result(request) {
                return answer("done", 3);
            }
            return calls(&[("c1", SWEEP_TOOL, arguments.clone())], 3);
        }
        let text = item_text(request);
        let source = text
            .rsplit("Source: ")
            .next()
            .unwrap_or_default()
            .to_owned();
        answer(&format!("read {source}"), ITEM_TOKENS)
    })
}

fn three_items() -> Value {
    json!({
        "instruction": "Summarize the source in one line.",
        "items": [
            {"label": "alpha", "source": "https://a.example"},
            {"label": "beta", "source": "https://b.example"},
            {"label": "gamma", "source": "https://c.example"}
        ]
    })
}

fn setting(ceiling: u32) -> SweepTool {
    let scorer = autumn_harvest_agent::sampling::ScorerRef::deterministic(Answered::ID).unwrap();
    SweepTool::new(KIND, NonZeroU32::new(ceiling).unwrap(), scorer)
        .max_items(3)
        .flow_label(OUTSIDE)
}

fn harness(model: Arc<TryFnModel>) -> AgentHarness {
    AgentHarness::new(model)
        .agent(
            AgentProfile::new(KIND)
                .system("You read one source.")
                .max_steps(4),
        )
        .scorer(Arc::new(Answered::new()))
}

fn task(sweep: Option<SweepTool>) -> AgentTask {
    let task = AgentTask::new("research three sources")
        .system("You plan.")
        .max_steps(5)
        .loop_guard(LoopGuard::disabled());
    match sweep {
        Some(sweep) => task.sweep(sweep),
        None => task,
    }
}

async fn run(model: &Arc<TryFnModel>, task: &AgentTask) -> (AgentReport, Vec<Value>) {
    let (_dir, db) = fresh_db();
    let mut rt = runtime(&db, harness(Arc::clone(model)));
    let exec = sqlite::start(&mut rt, task).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());
    let results = tool_results(&report)
        .iter()
        .map(|text| serde_json::from_str(text).unwrap())
        .collect();
    (report, results)
}

fn item_requests(model: &TryFnModel) -> usize {
    model
        .requests()
        .iter()
        .filter(|request| !offers_sweep(request))
        .count()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sweep_call_returns_one_row_per_label_and_its_spend() {
    let model = model(three_items());
    let (report, results) = run(&model, &task(Some(setting(1_000)))).await;

    assert_eq!(report.stop, AgentStop::Completed);
    assert_eq!(results.len(), 1, "one sweep result");
    let result: SweepToolResult = serde_json::from_value(results[0].clone()).unwrap();
    let labels: Vec<&str> = result.items.iter().map(|row| row.label.as_str()).collect();
    assert_eq!(
        labels,
        ["alpha", "beta", "gamma"],
        "the rows keep the order"
    );
    for row in &result.items {
        assert!(row.ok, "{row:?}");
        assert_eq!(row.status, SweepItemStatus::Completed);
        assert_eq!(row.tokens, ITEM_TOKENS);
    }
    assert_eq!(
        result.text,
        "alpha: read https://a.example\n\nbeta: read https://b.example\n\ngamma: read https://c.example"
    );
    assert_eq!(result.stopped, SweepStop::Completed);
    assert_eq!(result.spent.ceiling, 1_000, "the result names the ceiling");
    assert_eq!(result.spent.total_tokens, 3 * ITEM_TOKENS);
    assert_eq!(item_requests(&model), 3, "one model call per item");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_parent_counts_the_spend_of_the_sweep_tree() {
    let model = model(three_items());
    let (report, _) = run(&model, &task(Some(setting(1_000)))).await;
    // Two parent turns of 3 tokens each, and three items.
    assert_eq!(report.usage.total(), 2 * 3 + 3 * ITEM_TOKENS);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sweep_result_marks_the_run_with_the_labels_of_the_setting() {
    let model = model(three_items());
    let (report, _) = run(&model, &task(Some(setting(1_000)))).await;
    assert!(
        report.flow_labels.contains(OUTSIDE),
        "{:?}",
        report.flow_labels
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_ceiling_stops_the_items_that_it_does_not_reach() {
    // One item at a time. The first item gets the whole ceiling and spends
    // 7. The second gets the 3 left and passes them. No token is left for
    // the third.
    let model = model(three_items());
    let (_, results) = run(&model, &task(Some(setting(10).concurrency(1)))).await;
    let result: SweepToolResult = serde_json::from_value(results[0].clone()).unwrap();
    let statuses: Vec<SweepItemStatus> = result.items.iter().map(|row| row.status).collect();
    assert_eq!(statuses[2], SweepItemStatus::NotRun, "{statuses:?}");
    assert!(!result.items[2].ok);
    assert_eq!(result.stopped, SweepStop::BudgetExceeded);
    assert!(item_requests(&model) < 3, "the third item makes no call");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_call_over_the_item_bound_is_refused_before_any_model_call() {
    let four = json!({
        "instruction": "Summarize.",
        "items": [
            {"label": "a", "source": "1"},
            {"label": "b", "source": "2"},
            {"label": "c", "source": "3"},
            {"label": "d", "source": "4"}
        ]
    });
    let model = model(four);
    let (report, results) = run(&model, &task(Some(setting(1_000)))).await;
    assert_eq!(report.stop, AgentStop::Completed);
    let error = results[0]["error"].as_str().unwrap();
    assert!(error.contains("at most 3 items"), "{error}");
    assert_eq!(item_requests(&model), 0);
    assert!(!report.flow_labels.contains(OUTSIDE), "no child ran");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_call_cannot_widen_the_ceiling() {
    let mut widened = three_items();
    widened["max_total_tokens"] = json!(9_999_999);
    let model = model(widened);
    let (_, results) = run(&model, &task(Some(setting(1_000)))).await;
    assert!(results[0]["error"].is_string(), "{}", results[0]);
    assert_eq!(item_requests(&model), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_run_without_the_setting_does_not_offer_the_tool() {
    let model = TryFnModel::new(|_request: &ChatRequest| answer("plain", 1));
    let (report, _) = run(&model, &task(None)).await;
    assert_eq!(report.text, "plain");
    assert!(
        model
            .requests()
            .iter()
            .all(|request| !offers_sweep(request))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_item_run_never_offers_the_tool() {
    let model = model(three_items());
    let profile = AgentProfile::new(KIND)
        .system("You read one source.")
        .sweep(setting(1_000));
    let harness = AgentHarness::new(Arc::clone(&model) as _)
        .agent(profile)
        .scorer(Arc::new(Answered::new()));
    let (_dir, db) = fresh_db();
    let mut rt = runtime(&db, harness);
    let exec = sqlite::start(&mut rt, &task(Some(setting(1_000)))).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());
    assert_eq!(report.stop, AgentStop::Completed);
    // Only the two parent turns offer the tool. A draw drops the setting.
    let offered = model.requests().iter().filter(|r| offers_sweep(r)).count();
    assert_eq!(offered, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_policy_refusal_gives_an_error_result_and_the_run_goes_on() {
    // The default policy admits each scorer that calls no model. A judge
    // scorer is not admitted, so the sweep refuses before any model call.
    let judge = autumn_harvest_agent::sampling::ScorerRef::judge("judge", "m").unwrap();
    let refused = SweepTool::new(KIND, NonZeroU32::new(1_000).unwrap(), judge);
    let model = model(three_items());
    let (report, results) = run(&model, &task(Some(refused))).await;
    assert_eq!(report.stop, AgentStop::Completed);
    let error = results[0]["error"].as_str().unwrap();
    assert!(error.starts_with("the sweep failed"), "{error}");
    assert_eq!(item_requests(&model), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_parent_history_replays_clean() {
    let model = model(three_items());
    let (_dir, db) = fresh_db();
    let mut rt = runtime(&db, harness(Arc::clone(&model)));
    let exec = sqlite::start(&mut rt, &task(Some(setting(1_000)))).unwrap();
    let ended = report(rt.run_until_blocked(exec).await.unwrap());
    assert_eq!(ended.stop, AgentStop::Completed);
    let history = rt.load_history(exec).unwrap();
    let replay = WorkflowReplayer::new()
        .register(vec![agent_loop_info()])
        .with_execution_id(exec)
        .replay_from_events(history)
        .await;
    assert!(
        matches!(replay.status, ReplayStatus::ReplaySucceeded),
        "{replay}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sweep_takes_at_most_what_the_parent_has_left() {
    // The parent cap is 13. Its first turn spends 3, so the sweep gets 10
    // and not the ceiling of 1000.
    let model = model(three_items());
    let parent = task(Some(setting(1_000).concurrency(1))).max_total_tokens(13);
    let (_, results) = run(&model, &parent).await;
    let result: SweepToolResult = serde_json::from_value(results[0].clone()).unwrap();
    assert_eq!(
        result.spent.ceiling, 10,
        "the ceiling is what the parent has left"
    );
    assert_eq!(result.items[2].status, SweepItemStatus::NotRun);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_parent_with_no_token_left_starts_no_sweep() {
    // The first turn spends the whole parent cap of 3.
    let model = model(three_items());
    let parent = task(Some(setting(1_000))).max_total_tokens(3);
    let (_dir, db) = fresh_db();
    let mut rt = runtime(&db, harness(Arc::clone(&model)));
    let exec = sqlite::start(&mut rt, &parent).unwrap();
    let _ = rt.run_until_blocked(exec).await.unwrap();
    assert_eq!(item_requests(&model), 0, "no item ran");
}
