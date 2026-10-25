//! The agent cost ledger on the SQLite backend (issue #2275).
//!
//! The SQLite agent wrapper records one ledger row per model call, as the
//! Postgres `agent_model_turn` activity does. The runtime writes the row in
//! the transaction that appends the completion event of the turn. A crash
//! before that commit writes no row, so a re-run counts each turn once.
//!
//! The crash tests run this binary again as a child process. The child
//! drives one run and calls `abort` inside a model call or a tool call. The
//! parent then opens the same database and finishes the run.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::any::TypeId;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use autumn_harvest::context::{ActivityContext, SharedStateMap};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest_agent::workflow::agent_model_turn_info;
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentModel, AgentStop, AgentTask, ChatRequest, ChatResponse,
    ModelTurn, TokenUsage, ToolEffect, sqlite,
};
use autumn_harvest_sqlite::{ExecutionId, LlmLedgerRow, RunState, SqliteReader, SqliteRuntime};
use common::{
    MemStore, Recorder, Reply, ScriptedModel, answer, calls, recorded_tool, report, response_cache,
    runtime,
};
use serde_json::{Value, json};

/// The model id that [`PricedModel`] reports.
const MODEL_ID: &str = "priced-model-1";

/// A scripted model that names itself and prices each call. With
/// `abort_on` set, it aborts the process inside that call.
#[derive(Debug)]
struct PricedModel {
    inner: Arc<ScriptedModel>,
    abort_on: Option<usize>,
}

impl PricedModel {
    fn new(replies: Vec<Reply>) -> Arc<Self> {
        Self::aborting(replies, None)
    }

    fn aborting(replies: Vec<Reply>, abort_on: Option<usize>) -> Arc<Self> {
        Arc::new(Self {
            inner: ScriptedModel::named(MODEL_ID, replies),
            abort_on,
        })
    }
}

impl AgentModel for PricedModel {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> autumn_harvest_agent::model::BoxFuture<'a, Result<ChatResponse, AgentError>> {
        if self.abort_on == Some(self.inner.calls() + 1) {
            std::process::abort();
        }
        self.inner.chat(request)
    }

    fn model_id(&self) -> Option<&str> {
        Some(MODEL_ID)
    }

    fn cost_usd_micros(&self, usage: &TokenUsage) -> Option<u64> {
        Some(u64::from(usage.input_tokens) * 3 + u64::from(usage.output_tokens) * 15)
    }
}

/// `reply` with `input` prompt tokens and `output` answer tokens.
fn priced(reply: Reply, input: u32, output: u32) -> Reply {
    reply.map(|response| ChatResponse {
        usage: TokenUsage::new(input, output),
        ..response
    })
}

/// Three model turns: two tool rounds, then an answer. The second tool call
/// asks the child of a crash test to abort.
fn script() -> Vec<Reply> {
    vec![
        priced(calls(&[("c1", "lookup", json!({"n": 1}))], 0), 110, 7),
        priced(
            calls(&[("c2", "lookup", json!({"n": 2, "crash": true}))], 0),
            220,
            9,
        ),
        priced(answer("done", 0), 330, 11),
    ]
}

/// The tokens of each turn of [`script`].
const TURN_TOKENS: [(u64, u64); 3] = [(110, 7), (220, 9), (330, 11)];

/// A `lookup` tool. With `crash` set, it aborts on a call that asks for it.
fn lookup(crash: bool) -> Arc<dyn autumn_harvest_agent::Tool> {
    autumn_harvest_agent::FnTool::new(
        "lookup",
        "Look a value up.",
        json!({"type": "object"}),
        move |input: Value| async move {
            if crash && input["crash"] == json!(true) {
                std::process::abort();
            }
            Ok(json!({"found": input["n"]}))
        },
    )
    .effect(ToolEffect::ReadOnly)
    .shared()
}

fn harness(model: Arc<PricedModel>, crash: bool) -> AgentHarness {
    AgentHarness::new(model).tool(lookup(crash))
}

fn fresh_db() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

/// The rows of `exec` must be one row per turn of [`script`], in order.
fn assert_one_row_per_turn(rows: &[LlmLedgerRow]) {
    let tokens: Vec<_> = rows
        .iter()
        .map(|row| (row.input_tokens, row.output_tokens))
        .collect();
    assert_eq!(tokens, TURN_TOKENS, "one row per completed model turn");
    for row in rows {
        assert_eq!(row.activity_name, "agent_model_turn");
        assert_eq!(row.model, MODEL_ID);
        assert_eq!(row.call_index, 0, "a turn makes one model call");
        assert_eq!(
            row.cost_usd_micros,
            Some(row.input_tokens * 3 + row.output_tokens * 15)
        );
    }
    let mut events: Vec<_> = rows.iter().map(|row| row.event_seq).collect();
    events.dedup();
    assert_eq!(events.len(), rows.len(), "each turn has its own completion");
}

// A SQLite run records one row per model call. Each row names the
// completion event of its turn, and that event holds the same usage.
#[tokio::test(flavor = "multi_thread")]
async fn a_sqlite_run_records_one_row_per_model_call() {
    let (_dir, db) = fresh_db();
    let mut rt = runtime(&db, harness(PricedModel::new(script()), false));

    let exec = sqlite::start(&mut rt, &AgentTask::new("look twice")).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());
    assert_eq!(report.stop, AgentStop::Completed);

    let rows = rt.llm_ledger(exec).unwrap();
    assert_one_row_per_turn(&rows);
    let history = rt.load_history(exec).unwrap();
    for row in &rows {
        let WorkflowEvent::ActivityCompleted { output, .. } =
            &history[usize::try_from(row.event_seq).unwrap()]
        else {
            panic!("{row:?} must name an ActivityCompleted event");
        };
        let turn: ModelTurn = serde_json::from_value(output.clone()).unwrap();
        assert_eq!(
            (
                u64::from(turn.usage.input_tokens),
                u64::from(turn.usage.output_tokens)
            ),
            (row.input_tokens, row.output_tokens)
        );
    }
    assert_eq!(
        u64::from(report.usage.input_tokens),
        rows.iter().map(|row| row.input_tokens).sum::<u64>(),
        "the ledger sums to the usage of the report"
    );
}

/// The model id, the input and output tokens and the cost of one call.
type Figures = (String, u64, u64, Option<u64>);

/// The `LlmCall`s that the Postgres `agent_model_turn` activity records for
/// each of `requests`, with the model that `script` drives.
///
/// The activity records through `ActivityContext::record_llm_call`. The
/// Postgres worker writes each call as one row, with its position as the
/// call index. The Postgres DB suite (`llm_ledger_tests`) pins that write.
async fn postgres_calls(requests: &[Value], script: Vec<Reply>) -> Vec<Vec<Figures>> {
    let mut state: SharedStateMap = HashMap::new();
    let harness = AgentHarness::new(PricedModel::new(script)).tool(lookup(false));
    state.insert(TypeId::of::<AgentHarness>(), Box::new(harness));
    let state = Arc::new(state);
    let info = agent_model_turn_info();
    let mut attempts = Vec::new();
    for request in requests {
        // One context per attempt, as the worker builds one per attempt.
        let ctx = ActivityContext::new_test_with_state(Arc::clone(&state));
        (info.handler)(&ctx, request.clone()).await.unwrap();
        let calls = ctx
            .llm_calls()
            .iter()
            .map(|call| {
                (
                    call.model().to_owned(),
                    call.input_tokens(),
                    call.output_tokens(),
                    call.cost_usd_micros(),
                )
            })
            .collect();
        attempts.push(calls);
    }
    attempts
}

// The same script gives the same rows on both backends: the activity name,
// the call index, the model, the tokens and the cost. The SQLite side reads
// its rows. The Postgres side runs the Postgres activity on the turn inputs
// that the SQLite run recorded.
#[tokio::test(flavor = "multi_thread")]
async fn the_rows_match_the_postgres_activity_for_the_same_script() {
    let (_dir, db) = fresh_db();
    let mut rt = runtime(&db, harness(PricedModel::new(script()), false));
    let exec = sqlite::start(&mut rt, &AgentTask::new("look twice")).unwrap();
    report(rt.run_until_blocked(exec).await.unwrap());

    let info = agent_model_turn_info();
    let requests: Vec<Value> = rt
        .load_history(exec)
        .unwrap()
        .into_iter()
        .filter_map(|event| match event {
            WorkflowEvent::ActivityScheduled { name, input, .. } if name == info.name => {
                Some(input)
            }
            _ => None,
        })
        .collect();
    assert_eq!(requests.len(), 3);
    let postgres = postgres_calls(&requests, script()).await;

    let rows = rt.llm_ledger(exec).unwrap();
    let mut sqlite: Vec<Vec<Figures>> = vec![Vec::new(); requests.len()];
    let mut turn = 0;
    for (index, row) in rows.iter().enumerate() {
        if index > 0 && row.event_seq != rows[index - 1].event_seq {
            turn += 1;
        }
        assert_eq!(row.activity_name, info.name);
        assert_eq!(
            usize::try_from(row.call_index).unwrap(),
            sqlite[turn].len(),
            "the call index is the position of the call in its attempt"
        );
        sqlite[turn].push((
            row.model.clone(),
            row.input_tokens,
            row.output_tokens,
            row.cost_usd_micros,
        ));
    }
    assert_eq!(sqlite, postgres);
    assert_eq!(sqlite.iter().map(Vec::len).collect::<Vec<_>>(), [1, 1, 1]);
}

// A response cache hit calls no model and spends no tokens, so it records
// no row. The miss before it records one.
#[tokio::test(flavor = "multi_thread")]
async fn a_cache_hit_records_no_row() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::named("m-1", vec![priced(answer("hello", 0), 40, 2)]);
    let store = Arc::new(MemStore::default());
    let harness = AgentHarness::new(model.clone()).response_cache(response_cache(&store));
    let mut rt = runtime(&db, harness);
    let task = AgentTask::new("hi").tenant("acme");

    let first = sqlite::start(&mut rt, &task).unwrap();
    report(rt.run_until_blocked(first).await.unwrap());
    let second = sqlite::start(&mut rt, &task).unwrap();
    report(rt.run_until_blocked(second).await.unwrap());

    assert_eq!(model.calls(), 1, "the second run is a cache hit");
    let miss = rt.llm_ledger(first).unwrap();
    assert_eq!(miss.len(), 1, "{miss:?}");
    assert_eq!(
        (
            miss[0].model.as_str(),
            miss[0].input_tokens,
            miss[0].cost_usd_micros
        ),
        ("m-1", 40, None),
        "a model with no price records an unpriced call"
    );
    assert_eq!(rt.llm_ledger(second).unwrap(), Vec::new());
}

// A tool call records no row. Only a model turn does.
#[tokio::test(flavor = "multi_thread")]
async fn a_tool_call_records_no_row() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = PricedModel::new(vec![
        priced(calls(&[("c1", "note", json!({}))], 0), 5, 1),
        priced(answer("ok", 0), 6, 1),
    ]);
    let harness =
        AgentHarness::new(model).tool(recorded_tool("note", ToolEffect::ReadOnly, &recorder));
    let mut rt = runtime(&db, harness);

    let exec = sqlite::start(&mut rt, &AgentTask::new("note it")).unwrap();
    report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(recorder.runs().len(), 1);
    let rows = rt.llm_ledger(exec).unwrap();
    let names: Vec<_> = rows.iter().map(|row| row.activity_name.as_str()).collect();
    assert_eq!(names, ["agent_model_turn", "agent_model_turn"]);
}

const DB_ENV: &str = "AGENT_LEDGER_CRASH_DB";
const AT_ENV: &str = "AGENT_LEDGER_CRASH_AT";

/// The child half of a crash test. It does nothing unless the parent set the
/// environment. It aborts inside the second model call, or inside the second
/// tool call.
#[tokio::test(flavor = "multi_thread")]
async fn child_runs_until_it_crashes() {
    let (Some(db), Some(at)) = (std::env::var_os(DB_ENV), std::env::var_os(AT_ENV)) else {
        return;
    };
    let in_model = at == "model";
    let model = PricedModel::aborting(script(), in_model.then_some(2));
    let mut rt = runtime(Path::new(&db), harness(model, !in_model));
    let exec = sqlite::start(&mut rt, &AgentTask::new("look twice")).unwrap();
    std::fs::write(exec_file(Path::new(&db)), exec.to_string()).unwrap();
    let _ = rt.run_until_blocked(exec).await;
    panic!("the child must abort before the run ends");
}

/// Run the child until it aborts at `at`. Returns the directory, the
/// database and the run.
fn crash_child(at: &str) -> (tempfile::TempDir, PathBuf, ExecutionId) {
    let (dir, db) = fresh_db();
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_runs_until_it_crashes", "--nocapture"])
        .env(DB_ENV, &db)
        .env(AT_ENV, at)
        .status()
        .unwrap();
    assert!(!status.success(), "the child must crash, not finish");
    let exec = std::fs::read_to_string(exec_file(&db))
        .unwrap()
        .parse()
        .unwrap();
    (dir, db, exec)
}

/// Finish the crashed run in this process. `rest` is the script from the
/// turn that the child did not complete.
async fn resume(db: &Path, exec: ExecutionId, rest: Vec<Reply>) -> SqliteRuntime {
    let mut rt = runtime(db, harness(PricedModel::new(rest), false));
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "{state:?}");
    rt
}

/// The tokens of each row that the crashed child committed.
fn committed_tokens(db: &Path, exec: ExecutionId) -> Vec<u64> {
    SqliteReader::open(db)
        .unwrap()
        .llm_ledger(exec)
        .unwrap()
        .iter()
        .map(|row| row.input_tokens)
        .collect()
}

// The child dies inside the second model call. Its first turn committed one
// row. The second attempt wrote none, so the re-sent call counts once.
#[tokio::test(flavor = "multi_thread")]
async fn a_crash_inside_a_model_call_counts_each_turn_once() {
    let (_dir, db, exec) = crash_child("model");
    assert_eq!(committed_tokens(&db, exec), [110]);

    let mut rest = script();
    rest.drain(..1);
    let rt = resume(&db, exec, rest).await;

    assert_one_row_per_turn(&rt.llm_ledger(exec).unwrap());
}

// The child dies inside the second tool call, after two turns committed. The
// resumed run does not repeat a completed turn, so it adds only the third.
#[tokio::test(flavor = "multi_thread")]
async fn a_crash_inside_a_tool_call_counts_each_turn_once() {
    let (_dir, db, exec) = crash_child("tool");
    assert_eq!(committed_tokens(&db, exec), [110, 220]);

    let mut rest = script();
    rest.drain(..2);
    let rt = resume(&db, exec, rest).await;

    assert_one_row_per_turn(&rt.llm_ledger(exec).unwrap());
}

/// The file where the child records its execution id.
fn exec_file(db: &Path) -> PathBuf {
    db.with_extension("exec")
}
