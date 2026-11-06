//! The `agent_sweep` workflow on SQLite.
//!
//! Each item is a child `agent_sample` run. The sweep keeps one row per
//! item and the totals. The crash test is in `sweep_crash.rs`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unnecessary_literal_bound
)]

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use autumn_harvest::builder::DEFAULT_MAX_ACTIVITY_INPUT_BYTES;
use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::payload_store::PayloadStore;
use autumn_harvest_agent::bounds::{exceeds_activity_input, json_len};
use autumn_harvest_agent::model::BoxFuture;
use autumn_harvest_agent::sampling::{
    InMemorySamplingPolicyStore, MAX_SWEEP_ITEM_BYTES, MAX_SWEEP_LIST_BYTES, PolicyRead,
    SampleStrategy, SamplingPolicy, SamplingPolicyStore, ScorerRef, Strategy, SweepInput,
    SweepItem, SweepItemStatus, SweepItemStore, SweepItems, SweepReport, SweepStop,
};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentModel, AgentProfile, ChatRequest, ChatResponse, ContentPart,
    ExactMatch, FiniteF64, MeshBudget, MeshSpend, Spend, TokenUsage, sqlite,
};
use autumn_harvest_sqlite::{RunState, SqliteRuntime};
use common::{MemStore, TryFnModel, answer, runtime};
use futures::FutureExt as _;
use serde_json::json;

const KIND: &str = "researcher";

fn fresh_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

/// The text of the first user message of `request`: the item input.
fn item_of(request: &ChatRequest) -> String {
    request
        .messages
        .iter()
        .flat_map(|message| message.content.iter())
        .find_map(|part| match part {
            ContentPart::Text(text) => Some(text.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

/// The five questions of the fixed set, with their expected answers.
const QUESTIONS: [(&str, &str); 5] = [
    ("1 + 1", "2"),
    ("2 + 2", "4"),
    ("3 + 3", "6"),
    ("4 + 4", "8"),
    ("5 + 5", "10"),
];

fn five_items() -> Vec<SweepItem> {
    QUESTIONS
        .iter()
        .map(|(input, expected)| SweepItem::new(*input).expected(*expected))
        .collect()
}

/// A model that answers 1, 2 and 4 right, and 3 and 5 wrong, with 5 tokens.
fn three_of_five() -> Arc<TryFnModel> {
    TryFnModel::new(|request: &ChatRequest| {
        let text = match item_of(request).as_str() {
            "1 + 1" => "2",
            "2 + 2" => "4",
            "3 + 3" => "7",
            "4 + 4" => "8",
            _ => "11",
        };
        answer(text, 5)
    })
}

fn exact() -> ScorerRef {
    ScorerRef::deterministic(ExactMatch::ID).unwrap()
}

fn harness(model: Arc<dyn AgentModel>) -> AgentHarness {
    AgentHarness::new(model)
        .agent(AgentProfile::new(KIND))
        .scorer(Arc::new(ExactMatch::per_item()))
}

async fn drive(rt: &mut SqliteRuntime, input: &SweepInput) -> RunState {
    let exec = sqlite::start_sweep(rt, input).expect("the sweep starts");
    rt.run_until_blocked(exec).await.expect("the sweep drives")
}

fn sweep_report(state: RunState) -> SweepReport {
    match state {
        RunState::Completed(value) => serde_json::from_value(value).expect("the report decodes"),
        other => panic!("expected a completed sweep, got {other:?}"),
    }
}

fn ledger_rows(rt: &SqliteRuntime) -> Vec<autumn_harvest_sqlite::LlmLedgerRow> {
    use autumn_harvest::chrono::{DateTime, Utc};
    rt.llm_ledger_between(DateTime::<Utc>::MIN_UTC, DateTime::<Utc>::MAX_UTC)
        .unwrap()
}

fn store_with(policy: &SamplingPolicy) -> Arc<InMemorySamplingPolicyStore> {
    let store = Arc::new(InMemorySamplingPolicyStore::new());
    store
        .set(KIND, policy, "ops@example.com")
        .now_or_never()
        .expect("the in-memory store is ready at once")
        .expect("the policy is valid");
    store
}

// ── the table ───────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn a_five_item_sweep_with_exact_match_gives_five_rows() {
    let (_dir, db) = fresh_db();
    let model = three_of_five();
    let mut rt = runtime(&db, harness(model.clone()));
    let input = SweepInput::new(
        KIND,
        SweepItems::inline(five_items()),
        SampleStrategy::single(),
        exact(),
    );
    let report = sweep_report(drive(&mut rt, &input).await);

    assert_eq!(report.rows.len(), 5, "one row per item");
    let accepted: Vec<bool> = report.rows.iter().map(|row| row.accepted).collect();
    assert_eq!(accepted, [true, true, false, true, false]);
    for (n, row) in report.rows.iter().enumerate() {
        assert_eq!(row.item as usize, n, "the rows keep the item order");
        assert_eq!(row.status, SweepItemStatus::Completed);
        assert_eq!(row.draws, 1, "a single draw");
        let verdict = row.verdict.as_ref().expect("each row has a verdict");
        if row.accepted {
            assert_eq!(verdict.as_score().map(FiniteF64::get), Some(1.0), "row {n}");
        } else {
            assert!(verdict.is_reject(), "row {n}: {verdict:?}");
        }
        assert_eq!(row.usage.total(), 5, "row {n} keeps the tokens of its draw");
        assert_eq!(row.revision, Some(0), "row {n} reads the default policy");
    }
    let answers: Vec<Option<&str>> = report
        .rows
        .iter()
        .map(|row| row.answer.as_deref())
        .collect();
    assert_eq!(answers, [Some("2"), Some("4"), None, Some("8"), None]);
    let totals = &report.totals;
    assert_eq!(totals.items, 5);
    assert_eq!(totals.run, 5);
    assert_eq!(totals.accepted, 3);
    assert_eq!(totals.accept_rate.map(FiniteF64::get), Some(0.6));
    assert_eq!(totals.mean_score.map(FiniteF64::get), Some(0.6));
    assert_eq!(totals.not_run, 0);
    assert_eq!(report.stopped, SweepStop::Completed);
    assert_eq!(model.requests().len(), 5, "one model call per item");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_single_draw_sweep_needs_no_strategy_in_the_policy() {
    let (_dir, db) = fresh_db();
    // The default policy lists no strategy, so each run is a single draw.
    let harness =
        harness(three_of_five()).sampling_policies(store_with(&SamplingPolicy::default()));
    let mut rt = runtime(&db, harness);
    let input = SweepInput::new(
        KIND,
        SweepItems::inline(five_items()),
        SampleStrategy::single(),
        exact(),
    );
    let report = sweep_report(drive(&mut rt, &input).await);
    assert_eq!(report.totals.accepted, 3);
    assert_eq!(report.concurrency, 1, "the default cap is 1");
    assert_eq!(report.draw_concurrency, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn best_of_n_items_share_the_policy_cap_with_their_draws() {
    let (_dir, db) = fresh_db();
    let policy = SamplingPolicy {
        strategies: BTreeSet::from([Strategy::BestOfN]),
        max_draws: 4,
        max_concurrent_draws: 4,
        ..SamplingPolicy::default()
    };
    let model = three_of_five();
    let harness = harness(model.clone()).sampling_policies(store_with(&policy));
    let mut rt = runtime(&db, harness);
    let input = SweepInput::new(
        KIND,
        SweepItems::inline(five_items()),
        SampleStrategy::best_of_n(3),
        exact(),
    )
    .concurrency(2);
    let report = sweep_report(drive(&mut rt, &input).await);
    assert_eq!(report.concurrency, 2);
    assert_eq!(
        report.draw_concurrency, 2,
        "2 items x 2 draws is the cap of 4"
    );
    assert!(report.rows.iter().all(|row| row.draws == 3));
    assert_eq!(model.requests().len(), 15, "3 draws for each of 5 items");
    assert_eq!(report.totals.accepted, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_concurrency_over_the_policy_cap_is_refused_before_any_model_call() {
    let (_dir, db) = fresh_db();
    let policy = SamplingPolicy {
        max_concurrent_draws: 2,
        ..SamplingPolicy::default()
    };
    let model = three_of_five();
    let mut rt = runtime(
        &db,
        harness(model.clone()).sampling_policies(store_with(&policy)),
    );
    let input = SweepInput::new(
        KIND,
        SweepItems::inline(five_items()),
        SampleStrategy::single(),
        exact(),
    )
    .concurrency(3);
    match drive(&mut rt, &input).await {
        RunState::Failed(error) => {
            assert!(error.contains("too_many_concurrent_draws"), "{error}");
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
    assert_eq!(model.requests().len(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_empty_item_list_is_refused() {
    let (_dir, db) = fresh_db();
    let model = three_of_five();
    let mut rt = runtime(&db, harness(model.clone()));
    let input = SweepInput::new(
        KIND,
        SweepItems::inline(Vec::new()),
        SampleStrategy::single(),
        exact(),
    );
    match drive(&mut rt, &input).await {
        RunState::Failed(error) => assert!(error.contains("no_items"), "{error}"),
        other => panic!("expected a refusal, got {other:?}"),
    }
    assert_eq!(model.requests().len(), 0);
}

// ── the budget ──────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn a_budget_for_two_items_marks_the_rest_not_run() {
    let (_dir, db) = fresh_db();
    let model = three_of_five();
    let mut rt = runtime(&db, harness(model.clone()));
    // Each item spends 5 tokens. The sweep has 10.
    let input = SweepInput::new(
        KIND,
        SweepItems::inline(five_items()),
        SampleStrategy::single(),
        exact(),
    )
    .concurrency(1)
    .max_total_tokens(10);
    let report = sweep_report(drive(&mut rt, &input).await);

    assert_eq!(report.stopped, SweepStop::BudgetExceeded);
    let statuses: Vec<SweepItemStatus> = report.rows.iter().map(|row| row.status).collect();
    assert_eq!(
        statuses,
        [
            SweepItemStatus::Completed,
            SweepItemStatus::Completed,
            SweepItemStatus::NotRun,
            SweepItemStatus::NotRun,
            SweepItemStatus::NotRun,
        ]
    );
    for row in &report.rows[2..] {
        assert_eq!(row.draws, 0);
        assert_eq!(row.usage.total(), 0);
        assert!(row.verdict.is_none());
    }
    assert_eq!(report.totals.not_run, 3);
    assert_eq!(report.totals.run, 2);
    assert_eq!(model.requests().len(), 2, "no model call for items 3 to 5");
    let model_calls = ledger_rows(&rt)
        .iter()
        .filter(|row| row.activity_name == "agent_model_turn")
        .count();
    assert_eq!(model_calls, 2, "the ledger shows no third model call");
}

/// A mesh spend source that reads the tokens that the model spent.
#[derive(Debug)]
struct Counted(Arc<AtomicU64>);

impl MeshSpend for Counted {
    fn spent<'a>(
        &'a self,
        _mesh: &'a str,
        _since: autumn_harvest::chrono::DateTime<autumn_harvest::chrono::Utc>,
    ) -> BoxFuture<'a, Result<Spend, AgentError>> {
        let tokens = self.0.load(Ordering::SeqCst);
        Box::pin(std::future::ready(Ok(Spend {
            tokens,
            cost_usd_micros: 0,
        })))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_llm_budget_refusal_stops_the_sweep() {
    let (_dir, db) = fresh_db();
    let spent = Arc::new(AtomicU64::new(0));
    let counter = Arc::clone(&spent);
    let model = TryFnModel::new(move |request: &ChatRequest| {
        counter.fetch_add(5, Ordering::SeqCst);
        let text = if item_of(request) == "1 + 1" {
            "2"
        } else {
            "4"
        };
        answer(text, 5)
    });
    // The mesh may spend 10 tokens, so the budget admits 2 items.
    let harness = AgentHarness::new(model.clone())
        .agent(AgentProfile::new(KIND).mesh("eval"))
        .scorer(Arc::new(ExactMatch::per_item()))
        .mesh_budget(MeshBudget::daily("eval").max_tokens(10))
        .mesh_spend(Arc::new(Counted(spent)));
    let mut rt = runtime(&db, harness);
    let input = SweepInput::new(
        KIND,
        SweepItems::inline(five_items()),
        SampleStrategy::single(),
        exact(),
    )
    .concurrency(1);
    let report = sweep_report(drive(&mut rt, &input).await);

    assert_eq!(report.stopped, SweepStop::BudgetExceeded);
    let statuses: Vec<SweepItemStatus> = report.rows.iter().map(|row| row.status).collect();
    assert_eq!(
        statuses[..2],
        [SweepItemStatus::Completed, SweepItemStatus::Completed]
    );
    assert!(
        statuses[2..]
            .iter()
            .all(|status| *status == SweepItemStatus::NotRun),
        "{statuses:?}"
    );
    assert_eq!(model.requests().len(), 2, "no model call for items 3 to 5");
    assert_eq!(report.totals.accepted, 2);
}

// ── usage and cost ──────────────────────────────────────────────────────────

/// A model with an id and a price of 3 micro-dollars per token.
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
        Some("priced")
    }

    fn cost_usd_micros(&self, usage: &TokenUsage) -> Option<u64> {
        Some(u64::from(usage.total()) * 3)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_total_cost_equals_the_ledger_of_the_sweep_tree() {
    let (_dir, db) = fresh_db();
    // Draw n of each item spends 4 + n tokens, so each item spends 9.
    let inner = TryFnModel::new(|request: &ChatRequest| {
        let n = request.draw.map_or(0, autumn_harvest_agent::DrawId::get);
        let text = if n == 1 { "4" } else { "2" };
        answer(text, 4 + n)
    });
    let policy = SamplingPolicy {
        strategies: BTreeSet::from([Strategy::BestOfN]),
        max_draws: 2,
        max_concurrent_draws: 2,
        ..SamplingPolicy::default()
    };
    let harness = harness(Arc::new(Priced(inner))).sampling_policies(store_with(&policy));
    let mut rt = runtime(&db, harness);
    let input = SweepInput::new(
        KIND,
        SweepItems::inline(five_items()),
        SampleStrategy::best_of_n(2),
        exact(),
    );
    let report = sweep_report(drive(&mut rt, &input).await);

    // The fresh database holds only this sweep tree.
    let rows = ledger_rows(&rt);
    assert_eq!(rows.len(), 10, "2 model turns for each of 5 items");
    let ledger_tokens: u64 = rows
        .iter()
        .map(|row| row.input_tokens + row.output_tokens)
        .sum();
    let ledger_cost: u64 = rows.iter().filter_map(|row| row.cost_usd_micros).sum();
    assert_eq!(ledger_cost, 9 * 3 * 5);
    assert_eq!(report.totals.cost_usd_micros, ledger_cost);
    assert_eq!(u64::from(report.totals.usage.total()), ledger_tokens);
    let row_cost: u64 = report.rows.iter().map(|row| row.cost_usd_micros).sum();
    assert_eq!(row_cost, ledger_cost);
    // Items 1 and 2 accept: draw 0 says "2", draw 1 says "4".
    assert_eq!(report.totals.accepted, 2);
    assert_eq!(
        report.totals.cost_per_accepted_usd_micros,
        Some(ledger_cost / 2)
    );
}

// ── items by reference ──────────────────────────────────────────────────────

/// A model that answers each item with the number before its first space.
fn echo_number() -> Arc<TryFnModel> {
    TryFnModel::new(|request: &ChatRequest| {
        let input = item_of(request);
        let number = input.split(' ').next().unwrap_or_default().to_owned();
        answer(&number, 1)
    })
}

fn large_items(count: usize) -> Vec<SweepItem> {
    let pad = "x".repeat(60 * 1024);
    (0..count)
        .map(|n| SweepItem::new(format!("{n} {pad}")).expected(n.to_string()))
        .collect()
}

fn item_store(store: &Arc<MemStore>) -> SweepItemStore {
    SweepItemStore::new(
        Arc::clone(store) as Arc<dyn PayloadStore>,
        PayloadCodecs::default(),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn a_list_over_the_activity_cap_goes_by_reference() {
    let (_dir, db) = fresh_db();
    let items = large_items(36);
    let inline = SweepItems::inline(items.clone());
    assert!(
        exceeds_activity_input(&inline),
        "the list is over the cap of {DEFAULT_MAX_ACTIVITY_INPUT_BYTES} bytes"
    );
    let blobs = Arc::new(MemStore::default());
    let store = item_store(&blobs);
    let stored = store.put(&items).await.expect("the list is stored");
    let model = echo_number();
    let harness = AgentHarness::new(model.clone())
        .agent(AgentProfile::new(KIND))
        .scorer(Arc::new(ExactMatch::per_item()))
        .sweep_items(store);
    let mut rt = runtime(&db, harness);
    let input = SweepInput::new(KIND, stored, SampleStrategy::single(), exact());
    assert!(json_len(&input) < 1024, "the input holds a reference only");
    let report = sweep_report(drive(&mut rt, &input).await);

    assert_eq!(report.rows.len(), 36);
    assert_eq!(report.totals.accepted, 36, "the sweep read each item");
    assert_eq!(model.requests().len(), 36);
    assert_eq!(
        blobs.gets(),
        3,
        "a page holds at most 1 MiB of items, and each page is read once"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stored_list_with_another_digest_fails_before_any_model_call() {
    let (_dir, db) = fresh_db();
    let blobs = Arc::new(MemStore::default());
    let store = item_store(&blobs);
    let stored = store.put(&five_items()).await.unwrap();
    let mut value = serde_json::to_value(&stored).unwrap();
    value["stored"]["digest"] = json!("0".repeat(64));
    let forged: SweepItems = serde_json::from_value(value).unwrap();
    let model = three_of_five();
    let harness = harness(model.clone()).sweep_items(store);
    let mut rt = runtime(&db, harness);
    let input = SweepInput::new(KIND, forged, SampleStrategy::single(), exact());
    match drive(&mut rt, &input).await {
        RunState::Failed(error) => assert!(error.contains("digest"), "{error}"),
        other => panic!("expected a failure, got {other:?}"),
    }
    assert_eq!(model.requests().len(), 0);
}

#[tokio::test]
async fn the_store_refuses_an_item_over_the_item_cap() {
    let blobs = Arc::new(MemStore::default());
    let huge = SweepItem::new("x".repeat(MAX_SWEEP_ITEM_BYTES + 1));
    let err = item_store(&blobs).put(&[huge]).await.unwrap_err();
    assert!(err.to_string().contains("item 0"), "{err}");
    assert_eq!(blobs.len(), 0, "the store wrote nothing");
}

// ── the input and the report ────────────────────────────────────────────────

#[test]
fn a_sweep_input_with_a_policy_field_fails_to_decode() {
    let input = SweepInput::new(
        KIND,
        SweepItems::inline(five_items()),
        SampleStrategy::single(),
        exact(),
    );
    let mut value = serde_json::to_value(&input).unwrap();
    assert_eq!(value["strategy"], json!("single"));
    assert_eq!(
        value["items"]["inline"][0],
        json!({"input": "1 + 1", "expected": "2"})
    );
    value["max_concurrent_draws"] = json!(16);
    let err = serde_json::from_value::<SweepInput>(value).unwrap_err();
    assert!(err.to_string().contains("max_concurrent_draws"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_sweep_history_replays_clean() {
    use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
    use autumn_harvest_agent::sampling::agent_sweep_info;

    let (_dir, db) = fresh_db();
    let mut rt = runtime(&db, harness(three_of_five()));
    let input = SweepInput::new(
        KIND,
        SweepItems::inline(five_items()),
        SampleStrategy::single(),
        exact(),
    );
    let exec = sqlite::start_sweep(&mut rt, &input).unwrap();
    let report = sweep_report(rt.run_until_blocked(exec).await.unwrap());
    assert_eq!(report.totals.accepted, 3);
    let history = rt.load_history(exec).unwrap();
    let replay = WorkflowReplayer::new()
        .register(vec![agent_sweep_info()])
        .with_execution_id(exec)
        .replay_from_events(history)
        .await;
    assert!(
        matches!(replay.status, ReplayStatus::ReplaySucceeded),
        "{replay}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stored_list_with_no_item_store_fails_before_any_model_call() {
    let (_dir, db) = fresh_db();
    let blobs = Arc::new(MemStore::default());
    let stored = item_store(&blobs).put(&five_items()).await.unwrap();
    let model = three_of_five();
    let mut rt = runtime(&db, harness(model.clone()));
    let input = SweepInput::new(KIND, stored, SampleStrategy::single(), exact());
    match drive(&mut rt, &input).await {
        RunState::Failed(error) => assert!(error.contains("sweep item store"), "{error}"),
        other => panic!("expected a failure, got {other:?}"),
    }
    assert_eq!(model.requests().len(), 0);
}

#[tokio::test]
async fn the_store_refuses_a_list_over_the_list_cap() {
    let blobs = Arc::new(MemStore::default());
    let item = SweepItem::new("x".repeat(MAX_SWEEP_ITEM_BYTES - 64));
    let count = usize::try_from(MAX_SWEEP_LIST_BYTES).unwrap() / (MAX_SWEEP_ITEM_BYTES - 64) + 1;
    let err = item_store(&blobs)
        .put(&vec![item; count])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("larger than"), "{err}");
    assert_eq!(blobs.len(), 0, "the store wrote nothing");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_spent_profile_cap_stops_its_item_and_not_the_sweep() {
    let (_dir, db) = fresh_db();
    let policy = SamplingPolicy {
        strategies: BTreeSet::from([Strategy::BestOfN]),
        max_draws: 2,
        ..SamplingPolicy::default()
    };
    // The profile pays for one draw of 5 tokens per item.
    let mut profile = AgentProfile::new(KIND);
    profile.max_total_tokens = Some(5);
    let model = three_of_five();
    let harness = AgentHarness::new(model.clone())
        .agent(profile)
        .scorer(Arc::new(ExactMatch::per_item()))
        .sampling_policies(store_with(&policy));
    let mut rt = runtime(&db, harness);
    let input = SweepInput::new(
        KIND,
        SweepItems::inline(five_items()),
        SampleStrategy::best_of_n(2),
        exact(),
    );
    let report = sweep_report(drive(&mut rt, &input).await);
    assert_eq!(report.stopped, SweepStop::Completed, "each item runs");
    assert!(
        report
            .rows
            .iter()
            .all(|row| row.status == SweepItemStatus::BudgetExceeded && row.draws == 1)
    );
    assert_eq!(model.requests().len(), 5, "one draw for each item");
    assert_eq!(report.totals.accepted, 3);
}

/// A policy store that answers read `n` with `read(n)`.
struct Scripted {
    reads: std::sync::atomic::AtomicUsize,
    read: Box<
        dyn Fn(usize) -> Result<PolicyRead, autumn_harvest_agent::sampling::PolicyStoreError>
            + Send
            + Sync,
    >,
}

impl std::fmt::Debug for Scripted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Scripted")
    }
}

impl Scripted {
    fn new(
        read: impl Fn(usize) -> Result<PolicyRead, autumn_harvest_agent::sampling::PolicyStoreError>
        + Send
        + Sync
        + 'static,
    ) -> Arc<Self> {
        Arc::new(Self {
            reads: std::sync::atomic::AtomicUsize::new(0),
            read: Box::new(read),
        })
    }
}

impl SamplingPolicyStore for Scripted {
    fn read<'a>(
        &'a self,
        _kind: &'a str,
    ) -> BoxFuture<'a, Result<PolicyRead, autumn_harvest_agent::sampling::PolicyStoreError>> {
        let n = self.reads.fetch_add(1, Ordering::SeqCst);
        Box::pin(std::future::ready((self.read)(n)))
    }

    fn set<'a>(
        &'a self,
        _kind: &'a str,
        _policy: &'a SamplingPolicy,
        _actor: &'a str,
    ) -> BoxFuture<
        'a,
        Result<
            autumn_harvest_agent::sampling::PolicyChange,
            autumn_harvest_agent::sampling::PolicyStoreError,
        >,
    > {
        unreachable!("the sweep never sets a policy")
    }

    fn reset<'a>(
        &'a self,
        _kind: &'a str,
        _actor: &'a str,
    ) -> BoxFuture<
        'a,
        Result<
            autumn_harvest_agent::sampling::PolicyChange,
            autumn_harvest_agent::sampling::PolicyStoreError,
        >,
    > {
        unreachable!("the sweep never resets a policy")
    }

    fn audit<'a>(
        &'a self,
        _kind: &'a str,
        _limit: u32,
    ) -> BoxFuture<
        'a,
        Result<
            Vec<autumn_harvest_agent::sampling::PolicyAudit>,
            autumn_harvest_agent::sampling::PolicyStoreError,
        >,
    > {
        unreachable!("the sweep never reads the audit")
    }
}

fn revision(revision: u64) -> PolicyRead {
    PolicyRead {
        policy: SamplingPolicy::default(),
        revision,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_policy_change_during_the_sweep_stops_it() {
    let (_dir, db) = fresh_db();
    // The sweep and items 1 and 2 read revision 3. Item 3 reads revision 4.
    let store = Scripted::new(|n| Ok(revision(if n < 3 { 3 } else { 4 })));
    let model = three_of_five();
    let mut rt = runtime(&db, harness(model.clone()).sampling_policies(store));
    let input = SweepInput::new(
        KIND,
        SweepItems::inline(five_items()),
        SampleStrategy::single(),
        exact(),
    );
    let report = sweep_report(drive(&mut rt, &input).await);
    assert_eq!(report.stopped, SweepStop::ConfigChanged);
    assert_eq!(report.revision, 3);
    let revisions: Vec<Option<u64>> = report.rows.iter().map(|row| row.revision).collect();
    assert_eq!(revisions, [Some(3), Some(3), Some(4), None, None]);
    assert_eq!(report.rows[3].status, SweepItemStatus::NotRun);
    assert_eq!(model.requests().len(), 3, "no item starts after the change");
}

fn corrupt() -> autumn_harvest_agent::sampling::PolicyStoreError {
    autumn_harvest_agent::sampling::PolicyStoreError::Corrupt {
        reason: "test".to_owned(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_child_gives_a_failed_row_and_the_sweep_goes_on() {
    let (_dir, db) = fresh_db();
    // Read 1 is the sweep. Read 2 is item 1, which fails.
    let store = Scripted::new(|n| {
        if n == 1 {
            Err(corrupt())
        } else {
            Ok(revision(0))
        }
    });
    let model = three_of_five();
    let mut rt = runtime(&db, harness(model.clone()).sampling_policies(store));
    let input = SweepInput::new(
        KIND,
        SweepItems::inline(five_items()),
        SampleStrategy::single(),
        exact(),
    );
    let report = sweep_report(drive(&mut rt, &input).await);
    assert_eq!(report.rows[0].status, SweepItemStatus::Failed);
    assert!(report.rows[0].error.as_deref().unwrap().contains("corrupt"));
    assert_eq!(report.totals.failed, 1);
    assert!(
        report.rows[1..]
            .iter()
            .all(|row| row.status == SweepItemStatus::Completed)
    );
    assert_eq!(report.stopped, SweepStop::Completed);
    assert_eq!(model.requests().len(), 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_child_spends_its_whole_share_of_the_budget() {
    let (_dir, db) = fresh_db();
    let store = Scripted::new(|n| {
        if n == 1 {
            Err(corrupt())
        } else {
            Ok(revision(0))
        }
    });
    let model = three_of_five();
    let mut rt = runtime(&db, harness(model.clone()).sampling_policies(store));
    // Item 1 gets the whole budget as its share, and its spend is unknown.
    let input = SweepInput::new(
        KIND,
        SweepItems::inline(five_items()),
        SampleStrategy::single(),
        exact(),
    )
    .concurrency(1)
    .max_total_tokens(10);
    let report = sweep_report(drive(&mut rt, &input).await);
    assert_eq!(report.rows[0].status, SweepItemStatus::Failed);
    assert_eq!(report.stopped, SweepStop::BudgetExceeded);
    assert_eq!(report.totals.not_run, 4);
    assert_eq!(
        model.requests().len(),
        0,
        "no item runs past the charged share"
    );
}
