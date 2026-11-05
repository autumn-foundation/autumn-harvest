//! The `agent_refine` workflow on SQLite (issue #2380).
//!
//! A scripted proposer, composer and model drive each run. The source runs
//! are `agent_loop` runs on the same database. Each test checks one
//! criterion of the issue.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
// The `#[workflow]` macro reads its input through a binding.
#![allow(clippy::used_underscore_binding)]

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use autumn_harvest::failure::parse_typed_payload;
use autumn_harvest::prelude::*;
use autumn_harvest_agent::model::BoxFuture;
use autumn_harvest_agent::refine::{
    Charge, ComposeContext, LiveReprobe, OverlayComposer, RefineInput, RefineMode, RefineReport,
    StageRole,
};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentModel, AgentProfile, AgentTask, ChatRequest, ChatResponse,
    ChatRole, ContentPart, Dimension, FnTool, InMemoryOverlayStore, MeshBudget, OverlayProposer,
    ProfileOverlay, ProposeContext, ProposerReply, RefineStatus, StageOutcome, StopReason,
    TokenUsage, ToolEffect, sqlite,
};
use autumn_harvest_sqlite::{ExecutionId, RunState, SqliteRuntime};
use serde_json::json;

const KIND: &str = "support";
const BASE: &str = "Help the user.";
const DB_ENV: &str = "REFINE_RUN_CRASH_DB";
const LOG_ENV: &str = "REFINE_RUN_CRASH_LOG";

/// Append one line to the shared call log.
fn log(path: Option<&Path>, line: &str) {
    let Some(path) = path else {
        return;
    };
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    writeln!(file, "{line}").unwrap();
    file.sync_all().unwrap();
}

/// The system text of a model request.
fn system_of(request: &ChatRequest) -> String {
    request
        .messages
        .iter()
        .find(|message| message.role == ChatRole::System)
        .map(|message| {
            message
                .content
                .iter()
                .filter_map(|part| match part {
                    ContentPart::Text(text) => Some(text.as_str()),
                    _ => None,
                })
                .collect::<String>()
        })
        .unwrap_or_default()
}

/// A model that answers each run. With `tools`, the first turn of a run
/// calls `lookup` with its system text, so a probe makes a new call.
#[derive(Debug, Default)]
struct Model {
    tools: bool,
    /// The calls of the probes: each request with another system text.
    probe_calls: AtomicUsize,
    /// Abort the process at the start of this probe call.
    abort_at: Option<usize>,
}

impl AgentModel for Model {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        let system = system_of(request);
        // A probe runs an overlay, and each overlay names its edit.
        if system.contains(" edit ") || system.contains("composed") {
            let n = self.probe_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if self.abort_at == Some(n) {
                std::process::abort();
            }
        }
        let answered = request
            .messages
            .iter()
            .flat_map(|message| message.content.iter())
            .any(|part| matches!(part, ContentPart::ToolResult { .. }));
        let content = if self.tools && !answered {
            vec![ContentPart::ToolCall {
                id: "call-1".to_owned(),
                name: "lookup".to_owned(),
                arguments: json!({ "q": system }),
            }]
        } else {
            vec![ContentPart::Text("done".to_owned())]
        };
        let stop = if self.tools && !answered {
            StopReason::ToolUse
        } else {
            StopReason::EndTurn
        };
        Box::pin(async move {
            Ok(ChatResponse {
                content,
                stop_reason: stop,
                usage: TokenUsage::new(10, 5),
            })
        })
    }
}

/// A proposer that writes a new system text from its context. It logs
/// each call. With `keep`, that stage gives the current overlay back.
#[derive(Debug, Default)]
struct Proposer {
    calls: AtomicUsize,
    keep: Option<(Dimension, u32)>,
    log: Option<PathBuf>,
    /// Add `read_only: false` to each proposal.
    clear_read_only: bool,
}

impl OverlayProposer for Proposer {
    fn propose<'a>(
        &'a self,
        context: &'a ProposeContext<'a>,
    ) -> BoxFuture<'a, Result<ProposerReply, AgentError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        log(
            self.log.as_deref(),
            &format!("propose {} {}", context.dimension, context.sample),
        );
        context.charge(Charge::new(Some("scripted"), TokenUsage::new(100, 20)).cost(7));
        let reply = if self.keep == Some((context.dimension, context.sample)) {
            context.current.clone()
        } else {
            let from = context.current.system.as_deref().unwrap_or(BASE);
            ProfileOverlay {
                system: Some(format!(
                    "{from}\n{} edit {}",
                    context.dimension, context.sample
                )),
                read_only: self.clear_read_only.then_some(false),
                ..ProfileOverlay::default()
            }
        };
        Box::pin(async move { Ok(ProposerReply::Overlay(reply)) })
    }
}

/// A composer that names its sample and the size of its pool.
#[derive(Debug, Default)]
struct Composer {
    calls: AtomicUsize,
    log: Option<PathBuf>,
    /// Add `read_only: false` to each composition.
    clear_read_only: bool,
}

impl OverlayComposer for Composer {
    fn compose<'a>(
        &'a self,
        context: &'a ComposeContext<'a>,
    ) -> BoxFuture<'a, Result<ProposerReply, AgentError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        log(self.log.as_deref(), &format!("compose {}", context.sample));
        context.charge(Charge::new(Some("scripted"), TokenUsage::new(100, 20)));
        let from = context.current.system.as_deref().unwrap_or(BASE);
        let reply = ProfileOverlay {
            system: Some(format!(
                "{from}\ncomposed {} of {}",
                context.sample,
                context.candidates.len()
            )),
            read_only: self.clear_read_only.then_some(false),
            ..ProfileOverlay::default()
        };
        Box::pin(async move { Ok(ProposerReply::Overlay(reply)) })
    }
}

struct Fixture {
    rt: SqliteRuntime,
    proposer: Arc<Proposer>,
    composer: Arc<Composer>,
    model: Arc<Model>,
}

fn fixture(db: &Path, proposer: Proposer, composer: Composer, model: Model) -> Fixture {
    let proposer = Arc::new(proposer);
    let composer = Arc::new(composer);
    let model = Arc::new(model);
    let lookup = FnTool::new(
        "lookup",
        "Look up a record.",
        json!({"type": "object"}),
        |input: serde_json::Value| async move { Ok(json!({ "found": input })) },
    )
    .effect(ToolEffect::ReadOnly)
    .shared();
    let base = AgentHarness::new(Arc::clone(&model) as Arc<dyn AgentModel>)
        .agent(AgentProfile::new(KIND).system(BASE).max_steps(20))
        .tool(lookup);
    let store = InMemoryOverlayStore::new(Arc::new(base.clone()));
    let harness = base
        .overlay_store(Arc::new(store))
        .proposer(Arc::clone(&proposer) as Arc<dyn OverlayProposer>)
        .composer(Arc::clone(&composer) as Arc<dyn OverlayComposer>)
        .refine_sources(Arc::new(sqlite::Sources::open(db)));
    let mut rt = SqliteRuntime::open(db).unwrap();
    rt.register_workflow(&other_info());
    sqlite::register(&mut rt, Arc::new(harness));
    Fixture {
        rt,
        proposer,
        composer,
        model,
    }
}

fn task(kind: &str) -> AgentTask {
    let mut task = AgentTask::new("Check the refund.").kind(kind);
    task.system = Some(BASE.to_owned());
    task
}

/// Start and finish one `agent_loop` source run of `kind`.
async fn source(rt: &mut SqliteRuntime, kind: &str) -> ExecutionId {
    let exec = sqlite::start(rt, &task(kind)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "{state:?}");
    exec
}

async fn refine(rt: &mut SqliteRuntime, input: &RefineInput) -> RunState {
    let exec = sqlite::start_refine(rt, input).unwrap();
    rt.run_until_blocked(exec).await.unwrap()
}

fn report_of(state: RunState) -> RefineReport {
    match state {
        RunState::Completed(value) => serde_json::from_value(value).unwrap(),
        other => panic!("expected a completed refine run, got {other:?}"),
    }
}

fn error_type(state: &RunState) -> String {
    let RunState::Failed(payload) = state else {
        panic!("expected a failed refine run, got {state:?}");
    };
    parse_typed_payload(payload)
        .unwrap_or_else(|| panic!("a typed failure, got {payload}"))
        .error_type
}

/// Any other workflow, for a source of the wrong type.
#[allow(clippy::unused_async)]
#[workflow]
async fn other(_ctx: &WorkflowContext, n: u32) -> Result<u32, String> {
    Ok(n)
}

#[tokio::test(flavor = "multi_thread")]
async fn parallel_with_k_two_records_six_probes_and_six_sessions() {
    let dir = tempfile::tempdir().unwrap();
    let mut f = fixture(
        &dir.path().join("a.db"),
        Proposer::default(),
        Composer::default(),
        Model::default(),
    );
    let run = source(&mut f.rt, KIND).await;
    let report = report_of(
        refine(
            &mut f.rt,
            &RefineInput::new(KIND, vec![run], 2, RefineMode::Parallel, 7),
        )
        .await,
    );
    assert_eq!(report.status, RefineStatus::Chosen, "{report:?}");
    assert_eq!(report.ledger.sessions, 6);
    assert_eq!(report.ledger.probes, 6);
    assert_eq!(report.ledger.selections, 1);
    assert_eq!(f.proposer.calls.load(Ordering::SeqCst), 4);
    assert_eq!(f.composer.calls.load(Ordering::SeqCst), 2);
    let roles: Vec<StageRole> = report.stages.iter().map(|stage| stage.role).collect();
    assert_eq!(
        roles
            .iter()
            .filter(|role| **role == StageRole::Compose)
            .count(),
        2
    );
    let chosen = report.chosen.expect("a chosen overlay");
    assert!(
        chosen.system.as_deref().unwrap().contains("composed"),
        "{chosen:?}"
    );
    // Each probe charged its model turn, and each session its call.
    assert_eq!(report.ledger.tokens, 6 * 120 + 6 * 15);
}

#[tokio::test(flavor = "multi_thread")]
async fn sequential_with_k_two_records_the_same() {
    let dir = tempfile::tempdir().unwrap();
    let mut f = fixture(
        &dir.path().join("a.db"),
        Proposer::default(),
        Composer::default(),
        Model::default(),
    );
    let run = source(&mut f.rt, KIND).await;
    let report = report_of(
        refine(
            &mut f.rt,
            &RefineInput::new(KIND, vec![run], 2, RefineMode::Sequential, 7),
        )
        .await,
    );
    assert_eq!(report.status, RefineStatus::Chosen, "{report:?}");
    assert_eq!(report.ledger.sessions, 6);
    assert_eq!(report.ledger.probes, 6);
    assert_eq!(report.ledger.selections, 0);
    // The second step edits the composition of the first step.
    let chosen = report.chosen.unwrap().system.unwrap();
    assert_eq!(chosen.matches("composed").count(), 2, "{chosen}");
}

#[tokio::test(flavor = "multi_thread")]
async fn two_sources_double_the_identity() {
    let dir = tempfile::tempdir().unwrap();
    let mut f = fixture(
        &dir.path().join("a.db"),
        Proposer::default(),
        Composer::default(),
        Model::default(),
    );
    let one = source(&mut f.rt, KIND).await;
    let two = source(&mut f.rt, KIND).await;
    for mode in [RefineMode::Parallel, RefineMode::Sequential] {
        let input = RefineInput::new(KIND, vec![one, two], 1, mode, 3);
        let report = report_of(refine(&mut f.rt, &input).await);
        assert_eq!(report.ledger.sessions, 6, "{mode:?}");
        assert_eq!(report.ledger.probes, 6, "{mode:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn one_kept_base_stage_gives_six_sessions_and_five_probes() {
    for mode in [RefineMode::Parallel, RefineMode::Sequential] {
        let dir = tempfile::tempdir().unwrap();
        let proposer = Proposer {
            keep: Some((Dimension::Inv, 1)),
            ..Proposer::default()
        };
        let mut f = fixture(
            &dir.path().join("a.db"),
            proposer,
            Composer::default(),
            Model::default(),
        );
        let run = source(&mut f.rt, KIND).await;
        let report =
            report_of(refine(&mut f.rt, &RefineInput::new(KIND, vec![run], 2, mode, 7)).await);
        assert_eq!(report.status, RefineStatus::Chosen, "{mode:?}: {report:?}");
        assert_eq!(report.ledger.sessions, 6, "{mode:?}");
        assert_eq!(report.ledger.probes, 5, "{mode:?}");
        assert_eq!(report.ledger.kept_base, 1, "{mode:?}");
        let kept: Vec<_> = report
            .stages
            .iter()
            .filter(|stage| stage.outcome == StageOutcome::KeptBase)
            .collect();
        assert_eq!(kept.len(), 1);
        assert!(kept[0].probe.is_none(), "a kept stage adds no probe");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_budget_below_the_spend_ends_the_run_with_no_choice() {
    let dir = tempfile::tempdir().unwrap();
    let mut f = fixture(
        &dir.path().join("a.db"),
        Proposer::default(),
        Composer::default(),
        Model::default(),
    );
    let run = source(&mut f.rt, KIND).await;
    let input = RefineInput::new(KIND, vec![run], 2, RefineMode::Sequential, 7)
        .budget(MeshBudget::daily("refine").max_tokens(150));
    let report = report_of(refine(&mut f.rt, &input).await);
    assert_eq!(report.status, RefineStatus::BudgetExhausted);
    assert!(report.chosen.is_none());
    assert!(report.ledger.tokens >= 150);
    // INV spends 120 tokens and DOM 120 more. The gate before the first
    // probe then stops the run.
    assert_eq!(report.ledger.sessions, 2, "{:?}", report.ledger);
    assert_eq!(report.ledger.probes, 0, "{:?}", report.ledger);
    assert_eq!(report.ledger.tokens, 240);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_zero_budget_spends_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let mut f = fixture(
        &dir.path().join("a.db"),
        Proposer::default(),
        Composer::default(),
        Model::default(),
    );
    let run = source(&mut f.rt, KIND).await;
    let input = RefineInput::new(KIND, vec![run], 2, RefineMode::Parallel, 7)
        .budget(MeshBudget::daily("refine").max_cost_usd_micros(0));
    let report = report_of(refine(&mut f.rt, &input).await);
    assert_eq!(report.status, RefineStatus::BudgetExhausted);
    assert_eq!(report.ledger.sessions, 0);
    assert_eq!(f.proposer.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn one_seed_and_one_script_give_byte_identical_reports() {
    let dir = tempfile::tempdir().unwrap();
    let mut f = fixture(
        &dir.path().join("a.db"),
        Proposer::default(),
        Composer::default(),
        Model::default(),
    );
    let run = source(&mut f.rt, KIND).await;
    let input = RefineInput::new(KIND, vec![run], 2, RefineMode::Parallel, 11);
    let RunState::Completed(first) = refine(&mut f.rt, &input).await else {
        panic!("the first run ends");
    };
    let RunState::Completed(second) = refine(&mut f.rt, &input).await else {
        panic!("the second run ends");
    };
    assert_eq!(
        serde_json::to_vec(&first).unwrap(),
        serde_json::to_vec(&second).unwrap()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn each_bad_source_fails_at_start_with_a_named_error() {
    let dir = tempfile::tempdir().unwrap();
    let mut f = fixture(
        &dir.path().join("a.db"),
        Proposer::default(),
        Composer::default(),
        Model::default(),
    );
    let good = source(&mut f.rt, KIND).await;
    let running = sqlite::start(&mut f.rt, &task(KIND)).unwrap();
    let other_kind = source(&mut f.rt, "billing").await;
    let other = f.rt.start_workflow("other", json!(1)).unwrap();
    assert!(matches!(
        f.rt.run_until_blocked(other).await.unwrap(),
        RunState::Completed(_)
    ));
    let missing = ExecutionId::new();
    let model_calls = f.model.probe_calls.load(Ordering::SeqCst);
    let cases = [
        (running, "RefineSourceInFlight"),
        (other_kind, "RefineSourceOtherKind"),
        (other, "RefineSourceNotAgentRun"),
        (missing, "RefineSourceNotFound"),
    ];
    for (bad, name) in cases {
        let input = RefineInput::new(KIND, vec![good, bad], 1, RefineMode::Parallel, 1);
        let state = refine(&mut f.rt, &input).await;
        assert_eq!(error_type(&state), name, "{state:?}");
    }
    let zero_k = RefineInput::new(KIND, vec![good], 0, RefineMode::Parallel, 1);
    assert_eq!(
        error_type(&refine(&mut f.rt, &zero_k).await),
        "RefineInvalidInput"
    );
    let unknown = RefineInput::new("nobody", vec![good], 1, RefineMode::Parallel, 1);
    assert_eq!(
        error_type(&refine(&mut f.rt, &unknown).await),
        "RefineSourceOtherKind"
    );
    assert_eq!(f.proposer.calls.load(Ordering::SeqCst), 0);
    assert_eq!(f.composer.calls.load(Ordering::SeqCst), 0);
    assert_eq!(f.model.probe_calls.load(Ordering::SeqCst), model_calls);
}

#[tokio::test(flavor = "multi_thread")]
async fn sqlite_skips_the_live_re_probe_and_the_notes_say_so() {
    let dir = tempfile::tempdir().unwrap();
    let model = Model {
        tools: true,
        ..Model::default()
    };
    let mut f = fixture(
        &dir.path().join("a.db"),
        Proposer::default(),
        Composer::default(),
        model,
    );
    let run = source(&mut f.rt, KIND).await;
    let report = report_of(
        refine(
            &mut f.rt,
            &RefineInput::new(KIND, vec![run], 1, RefineMode::Parallel, 7),
        )
        .await,
    );
    assert_eq!(report.ledger.probes, 3);
    assert_eq!(report.ledger.live_reprobes, 0);
    for stage in &report.stages {
        let probe = stage.probe.as_ref().expect("each candidate has a probe");
        assert!(probe.stubbed_tool_calls > 0, "{probe:?}");
        let LiveReprobe::Skipped { reason } = &probe.live else {
            panic!("{probe:?}");
        };
        assert!(reason.contains("SQLite"), "{reason}");
        assert!(
            stage
                .notes
                .iter()
                .any(|note| note.contains("live re-probe skipped")),
            "{:?}",
            stage.notes
        );
    }
}

/// The child half of the crash test. It does nothing unless the parent
/// set the environment.
#[tokio::test(flavor = "multi_thread")]
async fn child_refines_until_it_crashes() {
    let (Some(db), Some(log_path)) = (std::env::var_os(DB_ENV), std::env::var_os(LOG_ENV)) else {
        return;
    };
    let db = PathBuf::from(db);
    let log_path = PathBuf::from(log_path);
    let mut f = fixture(
        &db,
        Proposer {
            log: Some(log_path.clone()),
            ..Proposer::default()
        },
        Composer {
            log: Some(log_path),
            ..Composer::default()
        },
        Model {
            abort_at: Some(4),
            ..Model::default()
        },
    );
    let run: ExecutionId = std::fs::read_to_string(db.with_extension("source"))
        .unwrap()
        .parse()
        .unwrap();
    let exec = sqlite::start_refine(
        &mut f.rt,
        &RefineInput::new(KIND, vec![run], 2, RefineMode::Parallel, 5),
    )
    .unwrap();
    std::fs::write(db.with_extension("exec"), exec.to_string()).unwrap();
    let _ = f.rt.run_until_blocked(exec).await;
    panic!("the child must abort before the run ends");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_crash_after_the_third_probe_resumes_to_the_same_report() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("crash.db");
    let log_path = dir.path().join("calls.log");
    // The source run.
    {
        let mut f = fixture(
            &db,
            Proposer::default(),
            Composer::default(),
            Model::default(),
        );
        let run = source(&mut f.rt, KIND).await;
        std::fs::write(db.with_extension("source"), run.to_string()).unwrap();
    }
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_refines_until_it_crashes", "--nocapture"])
        .env(DB_ENV, &db)
        .env(LOG_ENV, &log_path)
        .status()
        .unwrap();
    assert!(!status.success(), "the child must crash, not finish");
    let child_log = std::fs::read_to_string(&log_path).unwrap();
    let child_calls = child_log.lines().count();
    // The child ran the four proposals and three probes, then died at the
    // start of the fourth probe.
    assert_eq!(
        child_log.lines().collect::<Vec<_>>(),
        [
            "propose INV 1",
            "propose INV 2",
            "propose DOM 1",
            "propose DOM 2"
        ]
    );

    // The parent resumes the crashed run.
    let mut f = fixture(
        &db,
        Proposer::default(),
        Composer::default(),
        Model::default(),
    );
    let exec: ExecutionId = std::fs::read_to_string(db.with_extension("exec"))
        .unwrap()
        .parse()
        .unwrap();
    let resumed = report_of(f.rt.run_until_blocked(exec).await.unwrap());
    let resumed_calls =
        f.proposer.calls.load(Ordering::SeqCst) + f.composer.calls.load(Ordering::SeqCst);
    drop(f);

    // An uninterrupted run with the same seed.
    let mut clean = fixture(
        &db,
        Proposer::default(),
        Composer::default(),
        Model::default(),
    );
    let run: ExecutionId = std::fs::read_to_string(db.with_extension("source"))
        .unwrap()
        .parse()
        .unwrap();
    let input = RefineInput::new(KIND, vec![run], 2, RefineMode::Parallel, 5);
    let whole = report_of(refine(&mut clean.rt, &input).await);
    let whole_calls =
        clean.proposer.calls.load(Ordering::SeqCst) + clean.composer.calls.load(Ordering::SeqCst);

    assert_eq!(resumed, whole);
    assert_eq!(whole.ledger.probes, 6);
    let total = child_calls + resumed_calls;
    assert!(
        total == whole_calls || total == whole_calls + 1,
        "child {child_calls} + parent {resumed_calls} against {whole_calls}"
    );
    // The child completed three probes. The parent asked no proposer for the
    // stages that the child completed. It asked only the two composers.
    assert_eq!(resumed_calls, 2, "{resumed_calls} of {whole_calls}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_spend_is_charged_to_the_refine_run_in_the_cost_ledger() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("a.db");
    let mut f = fixture(
        &db,
        Proposer::default(),
        Composer::default(),
        Model::default(),
    );
    let run = source(&mut f.rt, KIND).await;
    let input = RefineInput::new(KIND, vec![run], 1, RefineMode::Parallel, 7)
        .budget(MeshBudget::daily("refine").max_tokens(1_000_000));
    let exec = sqlite::start_refine(&mut f.rt, &input).unwrap();
    let report = report_of(f.rt.run_until_blocked(exec).await.unwrap());
    let rows = f.rt.llm_ledger(exec).unwrap();
    let tokens: u64 = rows
        .iter()
        .map(|row| row.input_tokens + row.output_tokens)
        .sum();
    assert_eq!(tokens, report.ledger.tokens);
    let cost: u64 = rows.iter().filter_map(|row| row.cost_usd_micros).sum();
    assert_eq!(cost, report.ledger.cost_usd_micros);
    assert_eq!(cost, 2 * 7, "each proposer call costs 7");
    // The mesh spend of the budget holds the rows of the run.
    let spend = autumn_harvest_agent::mesh_control::MeshSpend::spent(
        &sqlite::LedgerSpend::open(&db),
        "refine",
        autumn_harvest::chrono::DateTime::<autumn_harvest::chrono::Utc>::MIN_UTC,
    )
    .await
    .unwrap();
    assert_eq!(spend.tokens, tokens);
}

/// What a test selector answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pick {
    /// The last id of the pool.
    Last,
    /// An id that is not in the pool.
    Outside,
    /// An error that a retry cannot fix.
    Fail,
}

/// A selector that answers with its `Pick`.
#[derive(Debug)]
struct Selector {
    pick: Pick,
}

impl autumn_harvest_agent::refine::OverlaySelector for Selector {
    fn select<'a>(
        &'a self,
        context: &'a autumn_harvest_agent::refine::SelectContext<'a>,
    ) -> BoxFuture<'a, Result<autumn_harvest_agent::eval::anon::AnonId, AgentError>> {
        assert!(!context.text.contains("sha256:"), "{}", context.text);
        let answer = match self.pick {
            Pick::Last => Ok(context.candidates.last().unwrap().id.clone()),
            Pick::Outside => Ok(serde_json::from_value(json!("c-000000000000")).unwrap()),
            Pick::Fail => Err(AgentError::new(
                autumn_harvest_agent::ErrorKind::Decode,
                "none",
            )),
        };
        Box::pin(async move { answer })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_selector_picks_by_opaque_id_and_the_rule_covers_a_bad_answer() {
    use autumn_harvest_agent::refine::SelectedBy;
    for pick in [Pick::Last, Pick::Outside, Pick::Fail] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("a.db");
        let model = Arc::new(Model::default());
        let base = AgentHarness::new(Arc::clone(&model) as Arc<dyn AgentModel>)
            .agent(AgentProfile::new(KIND).system(BASE).max_steps(20));
        let store = InMemoryOverlayStore::new(Arc::new(base.clone()));
        let harness = base
            .overlay_store(Arc::new(store))
            .proposer(Arc::new(Proposer::default()))
            .composer(Arc::new(Composer::default()))
            .selector(Arc::new(Selector { pick }))
            .refine_sources(Arc::new(sqlite::Sources::open(&db)));
        let mut rt = SqliteRuntime::open(&db).unwrap();
        sqlite::register(&mut rt, Arc::new(harness));
        let run = source(&mut rt, KIND).await;
        let input = RefineInput::new(KIND, vec![run], 2, RefineMode::Parallel, 7);
        let report = report_of(refine(&mut rt, &input).await);
        assert_eq!(report.selections.len(), 1, "{pick:?}");
        let selection = &report.selections[0];
        let chosen = &report.stages[selection.stage.unwrap()];
        assert_eq!(chosen.role, StageRole::Compose);
        let StageOutcome::Proposed(overlay) = &chosen.outcome else {
            panic!("{chosen:?}");
        };
        assert_eq!(report.chosen.as_ref(), Some(overlay));
        let note = selection.note.as_deref().unwrap_or_default();
        match pick {
            Pick::Last => {
                assert_eq!(selection.by, SelectedBy::Selector);
                assert_eq!(note, "");
            }
            Pick::Outside => {
                assert_eq!(selection.by, SelectedBy::Rule);
                assert!(note.contains("not in the pool"), "{note}");
            }
            Pick::Fail => {
                assert_eq!(selection.by, SelectedBy::Rule);
                assert!(note.contains("selector failed"), "{note}");
            }
        }
    }
}

/// A composer that checks what it reads.
#[derive(Debug, Default)]
struct Peek {
    seen: std::sync::Mutex<Vec<(usize, String)>>,
}

impl OverlayComposer for Peek {
    fn compose<'a>(
        &'a self,
        context: &'a ComposeContext<'a>,
    ) -> BoxFuture<'a, Result<ProposerReply, AgentError>> {
        self.seen
            .lock()
            .unwrap()
            .push((context.candidates.len(), context.text.to_owned()));
        let ids: Vec<String> = context
            .candidates
            .iter()
            .map(|c| c.id.to_string())
            .collect();
        let pool: Vec<String> = context
            .report
            .candidates
            .iter()
            .map(|entry| entry.id.to_string())
            .collect();
        assert_eq!(ids, pool, "the overlays and the pool share ids and order");
        Box::pin(async { Ok(ProposerReply::Overlay(ProfileOverlay::default())) })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_composer_reads_opaque_ids_and_no_digest() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("a.db");
    let peek = Arc::new(Peek::default());
    let model = Arc::new(Model::default());
    let base = AgentHarness::new(Arc::clone(&model) as Arc<dyn AgentModel>)
        .agent(AgentProfile::new(KIND).system(BASE).max_steps(20));
    let store = InMemoryOverlayStore::new(Arc::new(base.clone()));
    let harness = base
        .overlay_store(Arc::new(store))
        .proposer(Arc::new(Proposer::default()))
        .composer(Arc::clone(&peek) as Arc<dyn OverlayComposer>)
        .refine_sources(Arc::new(sqlite::Sources::open(&db)));
    let mut rt = SqliteRuntime::open(&db).unwrap();
    sqlite::register(&mut rt, Arc::new(harness));
    let run = source(&mut rt, KIND).await;
    let report = report_of(
        refine(
            &mut rt,
            &RefineInput::new(KIND, vec![run], 2, RefineMode::Parallel, 7),
        )
        .await,
    );
    let seen = peek.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2);
    for (count, text) in &seen {
        assert_eq!(*count, 4, "each composer reads the 2k probes");
        assert!(!text.contains("sha256:"), "{text}");
        for stage in &report.stages {
            if let Some(probe) = &stage.probe {
                assert!(!text.contains(&probe.overlay_digest));
            }
        }
    }
    // Each composition keeps the base, so the run changes nothing.
    assert_eq!(report.status, RefineStatus::Unchanged);
    assert!(report.chosen.is_none());
    assert_eq!(report.ledger.sessions, 6);
    assert_eq!(report.ledger.probes, 4);
    assert_eq!(report.ledger.kept_base, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_durable_agent_source_of_the_kind_is_refused_at_start() {
    let dir = tempfile::tempdir().unwrap();
    let mut f = fixture(
        &dir.path().join("a.db"),
        Proposer::default(),
        Composer::default(),
        Model::default(),
    );
    let agent = autumn_harvest_agent::AgentId::new(KIND, "acct-1").unwrap();
    sqlite::send(
        &mut f.rt,
        &agent,
        &autumn_harvest_agent::AgentOp::message("hi"),
        None,
    )
    .unwrap();
    sqlite::drive(&mut f.rt, &agent).await.unwrap();
    sqlite::delete(&mut f.rt, &agent, None).unwrap();
    sqlite::drive(&mut f.rt, &agent).await.unwrap();
    let filter = autumn_harvest_sqlite::ExecutionFilter::new().workflow_name("agent");
    let ended =
        f.rt.list_executions(&filter)
            .unwrap()
            .executions
            .iter()
            .find_map(autumn_harvest_sqlite::ExecutionSummary::execution_id)
            .expect("an agent run");
    let input = RefineInput::new(KIND, vec![ended], 1, RefineMode::Parallel, 1);
    assert_eq!(
        error_type(&refine(&mut f.rt, &input).await),
        "RefineSourceNotReplayable"
    );
    assert_eq!(f.proposer.calls.load(Ordering::SeqCst), 0);
}

/// A proposer that fails with a retryable kind on every call.
#[derive(Debug, Default)]
struct Flaky;

impl OverlayProposer for Flaky {
    fn propose<'a>(
        &'a self,
        _context: &'a ProposeContext<'a>,
    ) -> BoxFuture<'a, Result<ProposerReply, AgentError>> {
        Box::pin(async {
            Err(AgentError::new(
                autumn_harvest_agent::ErrorKind::Unavailable,
                "down",
            ))
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stage_that_fails_every_retry_falls_back_and_keeps_the_identity() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("a.db");
    let model = Arc::new(Model::default());
    let base = AgentHarness::new(Arc::clone(&model) as Arc<dyn AgentModel>)
        .agent(AgentProfile::new(KIND).system(BASE).max_steps(20));
    let store = InMemoryOverlayStore::new(Arc::new(base.clone()));
    let harness = base
        .overlay_store(Arc::new(store))
        .proposer(Arc::new(Flaky))
        .composer(Arc::new(Composer::default()))
        .refine_sources(Arc::new(sqlite::Sources::open(&db)));
    let mut rt = SqliteRuntime::open(&db).unwrap();
    sqlite::register(&mut rt, Arc::new(harness));
    let run = source(&mut rt, KIND).await;
    let exec = sqlite::start_refine(
        &mut rt,
        &RefineInput::new(KIND, vec![run], 1, RefineMode::Sequential, 7),
    )
    .unwrap();
    // A retryable failure backs off on durable timers.
    let mut state = rt.run_until_blocked(exec).await.unwrap();
    for hour in 1..=20 {
        if !matches!(state, RunState::WaitingTimer) {
            break;
        }
        let at = autumn_harvest::chrono::Utc::now() + autumn_harvest::chrono::Duration::hours(hour);
        state = rt.run_until_blocked_as_of(exec, at).await.unwrap();
    }
    let report = report_of(state);
    assert_eq!(report.ledger.sessions, 3);
    assert_eq!(report.ledger.fell_back, 2);
    assert_eq!(report.ledger.probes, 1);
    assert_eq!(report.status, RefineStatus::Chosen);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_composer_fails_the_run_with_a_named_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("a.db");
    let model = Arc::new(Model::default());
    let base = AgentHarness::new(Arc::clone(&model) as Arc<dyn AgentModel>)
        .agent(AgentProfile::new(KIND).system(BASE).max_steps(20));
    let store = InMemoryOverlayStore::new(Arc::new(base.clone()));
    let harness = base
        .overlay_store(Arc::new(store))
        .proposer(Arc::new(Proposer::default()))
        .refine_sources(Arc::new(sqlite::Sources::open(&db)));
    let mut rt = SqliteRuntime::open(&db).unwrap();
    sqlite::register(&mut rt, Arc::new(harness));
    let run = source(&mut rt, KIND).await;
    let state = refine(
        &mut rt,
        &RefineInput::new(KIND, vec![run], 1, RefineMode::Parallel, 7),
    )
    .await;
    assert_eq!(error_type(&state), "RefineStageFailed");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_budget_equal_to_the_spend_passes_and_one_less_does_not() {
    let dir = tempfile::tempdir().unwrap();
    let mut f = fixture(
        &dir.path().join("a.db"),
        Proposer::default(),
        Composer::default(),
        Model::default(),
    );
    let run = source(&mut f.rt, KIND).await;
    let input = RefineInput::new(KIND, vec![run], 1, RefineMode::Parallel, 7);
    let whole = report_of(refine(&mut f.rt, &input).await).ledger.tokens;
    let exact = input
        .clone()
        .budget(MeshBudget::daily("refine").max_tokens(whole));
    let report = report_of(refine(&mut f.rt, &exact).await);
    assert_eq!(report.status, RefineStatus::Chosen);
    assert_eq!(report.ledger.tokens, whole);
    let short = input.budget(MeshBudget::daily("refine").max_tokens(whole - 1));
    let report = report_of(refine(&mut f.rt, &short).await);
    assert_eq!(report.status, RefineStatus::BudgetExhausted);
    assert!(report.chosen.is_none());
}

/// A proposer that charges each call, then fails with a retryable kind.
#[derive(Debug, Default)]
struct Billed;

impl OverlayProposer for Billed {
    fn propose<'a>(
        &'a self,
        context: &'a ProposeContext<'a>,
    ) -> BoxFuture<'a, Result<ProposerReply, AgentError>> {
        context.charge(Charge::new(Some("scripted"), TokenUsage::new(50, 0)).cost(3));
        Box::pin(async {
            Err(AgentError::new(
                autumn_harvest_agent::ErrorKind::Unavailable,
                "down",
            ))
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_stage_keeps_the_spend_of_its_last_attempt() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("a.db");
    let model = Arc::new(Model::default());
    let base = AgentHarness::new(Arc::clone(&model) as Arc<dyn AgentModel>)
        .agent(AgentProfile::new(KIND).system(BASE).max_steps(20));
    let store = InMemoryOverlayStore::new(Arc::new(base.clone()));
    let harness = base
        .overlay_store(Arc::new(store))
        .proposer(Arc::new(Billed))
        .composer(Arc::new(Composer::default()))
        .refine_sources(Arc::new(sqlite::Sources::open(&db)));
    let mut rt = SqliteRuntime::open(&db).unwrap();
    sqlite::register(&mut rt, Arc::new(harness));
    let run = source(&mut rt, KIND).await;
    let exec = sqlite::start_refine(
        &mut rt,
        &RefineInput::new(KIND, vec![run], 1, RefineMode::Sequential, 7),
    )
    .unwrap();
    let mut state = rt.run_until_blocked(exec).await.unwrap();
    for hour in 1..=20 {
        if !matches!(state, RunState::WaitingTimer) {
            break;
        }
        let at = autumn_harvest::chrono::Utc::now() + autumn_harvest::chrono::Duration::hours(hour);
        state = rt.run_until_blocked_as_of(exec, at).await.unwrap();
    }
    let report = report_of(state);
    let fell: Vec<_> = report
        .stages
        .iter()
        .filter(|stage| matches!(stage.outcome, StageOutcome::FellBack { .. }))
        .collect();
    assert_eq!(fell.len(), 2);
    for stage in fell {
        assert_eq!(stage.usage, TokenUsage::new(50, 0), "{stage:?}");
        assert_eq!(stage.cost_usd_micros, 3);
        let StageOutcome::FellBack { error } = &stage.outcome else {
            unreachable!();
        };
        assert!(!error.contains("attempt"), "replay-stable text: {error}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_source_past_the_policy_ceilings_is_refused_before_any_call() {
    let dir = tempfile::tempdir().unwrap();
    let mut f = fixture(
        &dir.path().join("a.db"),
        Proposer::default(),
        Composer::default(),
        Model::default(),
    );
    // The source ran with more steps than the kind profile allows now. Each
    // probe would apply an overlay to that profile and fail.
    let mut wide = task(KIND);
    wide.max_steps = 30;
    let exec = sqlite::start(&mut f.rt, &wide).unwrap();
    assert!(matches!(
        f.rt.run_until_blocked(exec).await.unwrap(),
        RunState::Completed(_)
    ));
    let input = RefineInput::new(KIND, vec![exec], 1, RefineMode::Parallel, 1);
    let state = refine(&mut f.rt, &input).await;
    assert_eq!(error_type(&state), "RefineSourceOverCeilings");
    assert_eq!(f.proposer.calls.load(Ordering::SeqCst), 0);
    assert_eq!(f.model.probe_calls.load(Ordering::SeqCst), 0);

    // A source within the ceilings refines, and each stage checks its replies
    // against the profile of that source.
    let mut narrow = task(KIND);
    narrow.max_steps = 12;
    let exec = sqlite::start(&mut f.rt, &narrow).unwrap();
    assert!(matches!(
        f.rt.run_until_blocked(exec).await.unwrap(),
        RunState::Completed(_)
    ));
    let input = RefineInput::new(KIND, vec![exec], 1, RefineMode::Parallel, 1);
    let report = report_of(refine(&mut f.rt, &input).await);
    assert_eq!(report.ledger.sessions, 3);
    assert_eq!(report.ledger.probes, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_carried_overlay_fits_each_later_source() {
    let dir = tempfile::tempdir().unwrap();
    let mut f = fixture(
        &dir.path().join("a.db"),
        Proposer {
            clear_read_only: true,
            ..Proposer::default()
        },
        Composer {
            clear_read_only: true,
            ..Composer::default()
        },
        Model::default(),
    );
    // The first source can write. The second one ran read-only, and the
    // default ceilings never clear `read_only`.
    let writable = source(&mut f.rt, KIND).await;
    let mut locked = task(KIND);
    locked.read_only = true;
    let exec = sqlite::start(&mut f.rt, &locked).unwrap();
    assert!(matches!(
        f.rt.run_until_blocked(exec).await.unwrap(),
        RunState::Completed(_)
    ));
    for mode in [RefineMode::Parallel, RefineMode::Sequential] {
        let input = RefineInput::new(KIND, vec![writable, exec], 1, mode, 3);
        let report = report_of(refine(&mut f.rt, &input).await);
        // No reply that clears `read_only` is a candidate, so nothing that
        // the second source refuses is carried to it.
        assert_eq!(report.ledger.sessions, 6, "{mode:?}");
        assert_eq!(report.ledger.fell_back, 6, "{mode:?}");
        assert_eq!(report.ledger.probes, 0, "{mode:?}");
        assert_eq!(report.status, RefineStatus::Unchanged, "{mode:?}");
        for stage in &report.stages {
            let StageOutcome::FellBack { error } = &stage.outcome else {
                panic!("{stage:?}");
            };
            assert!(error.contains("clears read_only"), "{error}");
            // A stage of the first source refuses the reply for the second.
            if stage.source == writable {
                assert!(error.contains("another source"), "{error}");
            }
        }
    }
}
