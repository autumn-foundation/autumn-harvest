//! A live re-probe counts in a refine run, on SQLite.
//!
//! SQLite has no fork. A scripted `RefineSources` reads the SQLite source
//! and answers each live re-probe. It fails the live run of each overlay
//! whose system text holds a scripted mark.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use autumn_harvest_agent::eval::LiveCheck;
use autumn_harvest_agent::eval::anon::AnonId;
use autumn_harvest_agent::model::BoxFuture;
use autumn_harvest_agent::refine::{
    ComposeContext, LiveEnd, LiveProbeError, LiveReprobe, LiveRequest, OverlayComposer,
    OverlaySelector, RefineInput, RefineMode, RefineReport, RefineSources, SelectContext,
    SelectedBy, SourceRecord, StageRole,
};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentModel, AgentProfile, AgentTask, ChatRequest, ChatResponse,
    ChatRole, ContentPart, FnTool, InMemoryOverlayStore, OverlayProposer, ProfileOverlay,
    ProposeContext, ProposerReply, RefineStatus, StageOutcome, StopReason, TokenUsage, ToolEffect,
    sqlite,
};
use autumn_harvest_sqlite::{ExecutionId, RunState, SqliteRuntime};
use serde_json::json;

const KIND: &str = "support";
const BASE: &str = "Help the user.";
/// The model id of the harness. A live error that names it is redacted.
const MODEL_ID: &str = "model-live-id-3f9e";

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

/// A model whose first turn calls `lookup` with its system text. So each
/// probe of a new overlay stubs one tool call and asks for a live re-probe.
/// An overlay that ends with `composed 2` calls it twice, so the rule ranks
/// that composition below `composed 1`.
#[derive(Debug)]
struct Model;

impl AgentModel for Model {
    fn model_id(&self) -> Option<&str> {
        Some(MODEL_ID)
    }

    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        let answered = request
            .messages
            .iter()
            .flat_map(|message| message.content.iter())
            .any(|part| matches!(part, ContentPart::ToolResult { .. }));
        let system = system_of(request);
        let calls = if system.ends_with("composed 2") { 2 } else { 1 };
        let (content, stop) = if answered {
            (
                vec![ContentPart::Text("done".to_owned())],
                StopReason::EndTurn,
            )
        } else {
            let content = (1..=calls)
                .map(|n| ContentPart::ToolCall {
                    id: format!("call-{n}"),
                    name: "lookup".to_owned(),
                    arguments: json!({ "q": system, "n": n }),
                })
                .collect();
            (content, StopReason::ToolUse)
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

/// A proposer that adds one line to the current system text.
#[derive(Debug)]
struct Proposer;

impl OverlayProposer for Proposer {
    fn propose<'a>(
        &'a self,
        context: &'a ProposeContext<'a>,
    ) -> BoxFuture<'a, Result<ProposerReply, AgentError>> {
        let from = context.current.system.as_deref().unwrap_or(BASE);
        let reply = ProfileOverlay {
            system: Some(format!(
                "{from}\n{} edit {}",
                context.dimension, context.sample
            )),
            ..ProfileOverlay::default()
        };
        Box::pin(async move { Ok(ProposerReply::Overlay(reply)) })
    }
}

/// What a composer read for one composition.
#[derive(Debug, Clone)]
struct Read {
    /// The live end of each candidate, by overlay system text.
    live: Vec<(String, Option<LiveCheck>)>,
    /// The pool text.
    text: String,
}

/// A composer that names its sample, and keeps what it read.
#[derive(Debug, Default)]
struct Composer {
    seen: Mutex<Vec<Read>>,
}

impl OverlayComposer for Composer {
    fn compose<'a>(
        &'a self,
        context: &'a ComposeContext<'a>,
    ) -> BoxFuture<'a, Result<ProposerReply, AgentError>> {
        let live = context
            .candidates
            .iter()
            .zip(&context.report.candidates)
            .map(|(candidate, entry)| {
                assert_eq!(candidate.id, entry.id);
                (
                    candidate.overlay.system.clone().unwrap_or_default(),
                    entry.live.clone(),
                )
            })
            .collect();
        self.seen.lock().unwrap().push(Read {
            live,
            text: context.text.to_owned(),
        });
        let from = context.current.system.as_deref().unwrap_or(BASE);
        let reply = ProfileOverlay {
            system: Some(format!("{from}\ncomposed {}", context.sample)),
            ..ProfileOverlay::default()
        };
        Box::pin(async move { Ok(ProposerReply::Overlay(reply)) })
    }
}

/// Sources that read SQLite and script each live re-probe.
#[derive(Debug)]
struct Scripted {
    inner: sqlite::Sources,
    /// The live run of an overlay whose system text holds one of these
    /// marks does not complete.
    fail: Vec<String>,
    calls: AtomicUsize,
}

impl RefineSources for Scripted {
    fn source(&self, run: ExecutionId) -> BoxFuture<'_, Result<Option<SourceRecord>, AgentError>> {
        self.inner.source(run)
    }

    fn live_probe<'a>(
        &'a self,
        request: LiveRequest<'a>,
    ) -> BoxFuture<'a, Result<LiveEnd, LiveProbeError>> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        let system = request.overlay.system.clone().unwrap_or_default();
        let failed = self.fail.iter().any(|mark| system.contains(mark.as_str()));
        let end: LiveEnd = serde_json::from_value(json!({
            "exec_id": format!("fork-{n}"),
            "completed": !failed,
            "error": failed.then(|| format!("{MODEL_ID} refused the tool call")),
            "usage": {"input_tokens": 1, "output_tokens": 1},
            "cost_usd_micros": 1,
        }))
        .unwrap();
        Box::pin(async move { Ok(end) })
    }
}

/// A selector that picks the candidate whose system text ends with
/// `want`, and counts its calls.
#[derive(Debug)]
struct Selector {
    want: &'static str,
    calls: AtomicUsize,
    /// The live end of the pick, as the pool shows it.
    seen: Mutex<Option<LiveCheck>>,
}

impl OverlaySelector for Selector {
    fn select<'a>(
        &'a self,
        context: &'a SelectContext<'a>,
    ) -> BoxFuture<'a, Result<AnonId, AgentError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let pick = context
            .candidates
            .iter()
            .find(|c| {
                c.overlay
                    .system
                    .as_deref()
                    .is_some_and(|s| s.ends_with(self.want))
            })
            .unwrap();
        let entry = context
            .report
            .candidates
            .iter()
            .find(|entry| entry.id == pick.id)
            .unwrap();
        *self.seen.lock().unwrap() = entry.live.clone();
        let id = pick.id.clone();
        Box::pin(async move { Ok(id) })
    }
}

/// The runtime of one test, and the parts that it reads after the run.
struct Fixture {
    rt: SqliteRuntime,
    composer: Arc<Composer>,
    sources: Arc<Scripted>,
}

fn fixture(db: &std::path::Path, fail: &[&str], selector: Option<Arc<Selector>>) -> Fixture {
    let lookup = FnTool::new(
        "lookup",
        "Look up a record.",
        json!({"type": "object"}),
        |input: serde_json::Value| async move { Ok(json!({ "found": input })) },
    )
    .effect(ToolEffect::ReadOnly)
    .shared();
    let base = AgentHarness::new(Arc::new(Model) as Arc<dyn AgentModel>)
        .agent(AgentProfile::new(KIND).system(BASE).max_steps(20))
        .tool(lookup);
    let composer = Arc::new(Composer::default());
    let sources = Arc::new(Scripted {
        inner: sqlite::Sources::open(db),
        fail: fail.iter().map(|mark| (*mark).to_owned()).collect(),
        calls: AtomicUsize::new(0),
    });
    let store = InMemoryOverlayStore::new(Arc::new(base.clone()));
    let mut harness = base
        .overlay_store(Arc::new(store))
        .proposer(Arc::new(Proposer))
        .composer(Arc::clone(&composer) as Arc<dyn OverlayComposer>)
        .refine_sources(Arc::clone(&sources) as Arc<dyn RefineSources>);
    if let Some(selector) = selector {
        harness = harness.selector(selector as Arc<dyn OverlaySelector>);
    }
    let mut rt = SqliteRuntime::open(db).unwrap();
    sqlite::register(&mut rt, Arc::new(harness));
    Fixture {
        rt,
        composer,
        sources,
    }
}

/// Run one source and one dry refine run over it.
async fn run(f: &mut Fixture, k: u32, mode: RefineMode) -> RefineReport {
    let mut task = AgentTask::new("Check the refund.").kind(KIND);
    task.system = Some(BASE.to_owned());
    let exec = sqlite::start(&mut f.rt, &task).unwrap();
    let state = f.rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "{state:?}");
    let input = RefineInput::new(KIND, vec![exec], k, mode, 7).dry_run();
    let refine = sqlite::start_refine(&mut f.rt, &input).unwrap();
    match f.rt.run_until_blocked(refine).await.unwrap() {
        RunState::Completed(value) => serde_json::from_value(value).unwrap(),
        other => panic!("expected a completed refine run, got {other:?}"),
    }
}

/// The system text of the chosen overlay.
fn chosen_system(report: &RefineReport) -> String {
    report
        .chosen
        .as_ref()
        .and_then(|overlay| overlay.system.clone())
        .unwrap_or_default()
}

/// The last line of the system text of `stage`.
fn last_line(report: &RefineReport, stage: usize) -> String {
    let StageOutcome::Proposed(overlay) = &report.stages[stage].outcome else {
        panic!("{:?}", report.stages[stage]);
    };
    let system = overlay.system.as_deref().unwrap_or_default();
    system.lines().last().unwrap_or_default().to_owned()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_live_re_probe_changes_the_rule_pick() {
    // With no live failure, the rule picks composition 1. It stubs fewer
    // tool calls.
    let line = "composed 1";
    let dir = tempfile::tempdir().unwrap();
    let mut f = fixture(&dir.path().join("a.db"), &[], None);
    let first = run(&mut f, 2, RefineMode::Parallel).await;
    assert_eq!(first.selections.len(), 1);
    let picked = first.selections[0].stage.unwrap();
    assert_eq!(first.stages[picked].role, StageRole::Compose);
    assert_eq!(last_line(&first, picked), line, "{first:#?}");

    // The same run, with a failed live re-probe of composition 1.
    let dir = tempfile::tempdir().unwrap();
    let mut f = fixture(&dir.path().join("a.db"), &[line], None);
    let second = run(&mut f, 2, RefineMode::Parallel).await;
    let selection = &second.selections[0];
    assert_eq!(selection.by, SelectedBy::Rule);
    let other = selection.stage.unwrap();
    assert_eq!(last_line(&second, other), "composed 2", "{second:#?}");
    assert!(chosen_system(&second).ends_with("composed 2"));
    // The report keeps the live end of the refused composition.
    let refused = second
        .stages
        .iter()
        .find(|stage| {
            matches!(&stage.outcome, StageOutcome::Proposed(o)
                if o.system.as_deref().is_some_and(|s| s.ends_with(line)))
        })
        .unwrap();
    let probe = refused.probe.as_ref().unwrap();
    assert!(
        matches!(&probe.live, LiveReprobe::Ran(end) if !end.completed),
        "{probe:?}"
    );
    // The live re-probe of each probe ran, so the identity still holds.
    assert_eq!(second.ledger.probes, 6);
    assert_eq!(second.ledger.live_reprobes, 6);
    assert_eq!(f.sources.calls.load(Ordering::SeqCst), 6);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_selector_pick_of_a_live_failure_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let selector = Arc::new(Selector {
        want: "composed 1",
        calls: AtomicUsize::new(0),
        seen: Mutex::new(None),
    });
    let mut f = fixture(
        &dir.path().join("a.db"),
        &["composed 1"],
        Some(Arc::clone(&selector)),
    );
    let report = run(&mut f, 2, RefineMode::Parallel).await;
    assert_eq!(selector.calls.load(Ordering::SeqCst), 1);
    let seen = selector.seen.lock().unwrap().clone();
    assert!(
        matches!(&seen, Some(LiveCheck::Failed { .. })),
        "the selector reads the live end: {seen:?}"
    );
    let selection = &report.selections[0];
    assert_eq!(selection.by, SelectedBy::Rule);
    let note = selection.note.as_deref().unwrap_or_default();
    assert!(note.contains("live re-probe"), "{note}");
    assert!(
        chosen_system(&report).ends_with("composed 2"),
        "{report:#?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pool_of_live_failures_picks_none_and_asks_no_selector() {
    let dir = tempfile::tempdir().unwrap();
    let selector = Arc::new(Selector {
        want: "composed 1",
        calls: AtomicUsize::new(0),
        seen: Mutex::new(None),
    });
    let mut f = fixture(
        &dir.path().join("a.db"),
        &["composed"],
        Some(Arc::clone(&selector)),
    );
    let report = run(&mut f, 2, RefineMode::Parallel).await;
    assert_eq!(selector.calls.load(Ordering::SeqCst), 0);
    let selection = &report.selections[0];
    assert_eq!(selection.by, SelectedBy::Rule);
    assert_eq!(selection.stage, None);
    assert_eq!(selection.id, None);
    let note = selection.note.as_deref().unwrap_or_default();
    assert!(note.contains("live re-probe"), "{note}");
    assert_eq!(report.status, RefineStatus::Unchanged);
    assert!(report.chosen.is_none());
    assert_eq!(report.ledger.selections, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn sequential_mode_keeps_the_overlay_before_a_live_failure() {
    // With no live failure, step 2 builds on composition 1.
    let dir = tempfile::tempdir().unwrap();
    let mut f = fixture(&dir.path().join("a.db"), &[], None);
    let report = run(&mut f, 2, RefineMode::Sequential).await;
    assert!(chosen_system(&report).contains("composed 1"), "{report:#?}");

    // Composition 1 fails live, so step 2 starts from the base.
    let dir = tempfile::tempdir().unwrap();
    let mut f = fixture(&dir.path().join("a.db"), &["composed 1"], None);
    let report = run(&mut f, 2, RefineMode::Sequential).await;
    let chosen = chosen_system(&report);
    assert!(!chosen.contains("composed 1"), "{chosen}");
    assert!(chosen.ends_with("composed 2"), "{chosen}");
    let first = report
        .stages
        .iter()
        .find(|stage| stage.role == StageRole::Compose)
        .unwrap();
    assert!(
        first
            .notes
            .iter()
            .any(|note| note.contains("live re-probe")),
        "{:?}",
        first.notes
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_composer_reads_the_live_end_of_each_candidate() {
    let dir = tempfile::tempdir().unwrap();
    let mut f = fixture(&dir.path().join("a.db"), &["INV edit 1"], None);
    run(&mut f, 1, RefineMode::Parallel).await;
    let seen = f.composer.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 1);
    let read = &seen[0];
    assert_eq!(read.live.len(), 2);
    for (system, live) in &read.live {
        if system.ends_with("INV edit 1") {
            let Some(LiveCheck::Failed { error }) = live else {
                panic!("{live:?}");
            };
            let error = error.as_deref().unwrap_or_default();
            assert!(error.contains("[redacted] refused"), "{error}");
        } else {
            assert_eq!(live, &Some(LiveCheck::Completed), "{system}");
        }
    }
    assert!(read.text.contains("Live re-probe: failed"), "{}", read.text);
    assert!(
        read.text.contains("Live re-probe: completed"),
        "{}",
        read.text
    );
    assert!(!read.text.contains(MODEL_ID), "{}", read.text);
    assert!(!read.text.contains("fork-"), "{}", read.text);
}
