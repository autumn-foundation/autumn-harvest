//! The lesson library of a kind on SQLite.
//!
//! A scripted proposer, composer, selector and model drive each refine run.
//! Each test checks one criterion of the issue. The design is in
//! `docs/design/DESIGN-2382.md`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest_agent::memory::{MemoryOp, MemorySnapshot};
use autumn_harvest_agent::model::BoxFuture;
use autumn_harvest_agent::refine::{
    AutoAdopt, Charge, Choice, ComposeContext, DailyCap, LessonEntries, LessonOutcome, Lessons,
    OverlayComposer, OverlaySelector, RefineDecision, RefineInput, RefineMode, RefinePolicy,
    RefineReport, SelectContext, SelectedBy, agent_refine_info, lesson_scope, lessons_block,
    read_lessons,
};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentId, AgentModel, AgentOp, AgentProfile, AgentTask, ChatRequest,
    ChatResponse, ContentPart, Control, EditKey, InMemoryMemoryStore, InMemoryOverlayStore,
    MemoryBlock, MemoryScope, MemoryStore, OverlayPart, OverlayProposer, ProfileOverlay,
    ProfileOverlayStore, ProposeContext, ProposerReply, Provenance, RefineStatus, StopReason,
    TokenUsage, sqlite,
};
use autumn_harvest_sqlite::{ExecutionId, RunState, SqliteRuntime};

const KIND: &str = "support";
const BASE: &str = "Help the user.";
const HOUR_MS: i64 = 3_600_000;

/// A model that answers each run.
#[derive(Debug, Default)]
struct Model;

impl AgentModel for Model {
    fn chat<'a>(
        &'a self,
        _request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        Box::pin(async move {
            Ok(ChatResponse {
                content: vec![ContentPart::Text("done".to_owned())],
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::new(10, 5),
            })
        })
    }
}

/// A proposer that adds one line to the current system text. It keeps the
/// lesson snapshot of each call.
#[derive(Debug, Default)]
struct Proposer {
    seen: Mutex<Vec<Option<MemorySnapshot>>>,
}

impl OverlayProposer for Proposer {
    fn propose<'a>(
        &'a self,
        context: &'a ProposeContext<'a>,
    ) -> BoxFuture<'a, Result<ProposerReply, AgentError>> {
        context.charge(Charge::new(Some("scripted"), TokenUsage::new(1, 1)));
        self.seen.lock().unwrap().push(context.lessons.cloned());
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

/// A composer that names its sample.
#[derive(Debug, Default)]
struct Composer;

impl OverlayComposer for Composer {
    fn compose<'a>(
        &'a self,
        context: &'a ComposeContext<'a>,
    ) -> BoxFuture<'a, Result<ProposerReply, AgentError>> {
        context.charge(Charge::new(Some("scripted"), TokenUsage::new(1, 1)));
        let from = context.current.system.as_deref().unwrap_or(BASE);
        let reply = ProfileOverlay {
            system: Some(format!("{from}\ncomposed {}", context.sample)),
            ..ProfileOverlay::default()
        };
        Box::pin(async move { Ok(ProposerReply::Overlay(reply)) })
    }
}

/// A selector that picks the first candidate and says why.
#[derive(Debug, Default)]
struct Selector;

impl OverlaySelector for Selector {
    fn select<'a>(
        &'a self,
        context: &'a SelectContext<'a>,
    ) -> BoxFuture<'a, Result<autumn_harvest_agent::eval::anon::AnonId, AgentError>> {
        let id = context.candidates[0].id.clone();
        Box::pin(async move { Ok(id) })
    }

    fn choose<'a>(
        &'a self,
        context: &'a SelectContext<'a>,
    ) -> BoxFuture<'a, Result<Choice, AgentError>> {
        let id = context.candidates[0].id.clone();
        Box::pin(async move {
            Ok(Choice::new(id)
                .why("it checks the refund first")
                .lesson("check the refund before the reply"))
        })
    }
}

/// A selector that names an id outside the pool, with text. The rule then
/// picks, and the text goes.
#[derive(Debug, Default)]
struct Outside;

impl OverlaySelector for Outside {
    fn select<'a>(
        &'a self,
        _context: &'a SelectContext<'a>,
    ) -> BoxFuture<'a, Result<autumn_harvest_agent::eval::anon::AnonId, AgentError>> {
        Box::pin(
            async move { Ok(serde_json::from_value(serde_json::json!("c-000000000000")).unwrap()) },
        )
    }

    fn choose<'a>(
        &'a self,
        context: &'a SelectContext<'a>,
    ) -> BoxFuture<'a, Result<Choice, AgentError>> {
        Box::pin(async move {
            let id = self.select(context).await?;
            Ok(Choice::new(id).why("a reason for no candidate"))
        })
    }
}

/// A selector that implements `select` only.
#[derive(Debug, Default)]
struct SelectOnly;

impl OverlaySelector for SelectOnly {
    fn select<'a>(
        &'a self,
        context: &'a SelectContext<'a>,
    ) -> BoxFuture<'a, Result<autumn_harvest_agent::eval::anon::AnonId, AgentError>> {
        let id = context.candidates[0].id.clone();
        Box::pin(async move { Ok(id) })
    }
}

/// A memory store with the default `ensure_block`.
#[derive(Debug, Default)]
struct NoBlockStore(InMemoryMemoryStore);

impl MemoryStore for NoBlockStore {
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

/// A memory store that cannot keep labels. It adds a block, and it applies
/// an edit with no labels.
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

    fn ensure_block<'a>(
        &'a self,
        scope: &'a MemoryScope,
        block: MemoryBlock,
    ) -> BoxFuture<'a, Result<(), AgentError>> {
        self.0.ensure_block(scope, block)
    }
}

/// A runtime and its stores.
struct Fixture {
    _dir: tempfile::TempDir,
    harness: AgentHarness,
    rt: SqliteRuntime,
    store: Arc<InMemoryOverlayStore>,
    memory: Option<Arc<dyn MemoryStore>>,
    proposer: Arc<Proposer>,
    clock: Arc<std::sync::atomic::AtomicI64>,
}

fn fixture() -> Fixture {
    fixture_with(Arc::new(InMemoryMemoryStore::new()))
}

fn fixture_with(memory: Arc<dyn MemoryStore>) -> Fixture {
    fixture_of(Some(memory), Arc::new(Selector))
}

fn fixture_of(memory: Option<Arc<dyn MemoryStore>>, selector: Arc<dyn OverlaySelector>) -> Fixture {
    use std::sync::atomic::{AtomicI64, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("lessons.db");
    let proposer = Arc::new(Proposer::default());
    let base = AgentHarness::new(Arc::new(Model) as Arc<dyn AgentModel>)
        .agent(AgentProfile::new(KIND).system(BASE).max_steps(20));
    let store = Arc::new(InMemoryOverlayStore::new(Arc::new(base.clone())));
    let mut harness = base
        .overlay_store(Arc::clone(&store) as Arc<dyn ProfileOverlayStore>)
        .proposer(Arc::clone(&proposer) as Arc<dyn OverlayProposer>)
        .composer(Arc::new(Composer) as Arc<dyn OverlayComposer>)
        .selector(selector)
        .refine_sources(Arc::new(sqlite::Sources::open(&db)));
    if let Some(memory) = &memory {
        harness = harness.memory(Arc::clone(memory));
    }
    let clock = Arc::new(AtomicI64::new(1_767_614_400_000));
    let mut rt = SqliteRuntime::open(&db).unwrap();
    let read = Arc::clone(&clock);
    rt.set_clock(move || {
        chrono::DateTime::from_timestamp_millis(read.load(Ordering::SeqCst)).unwrap()
    });
    sqlite::register(&mut rt, Arc::new(harness.clone()));
    Fixture {
        _dir: dir,
        harness,
        rt,
        store,
        memory,
        proposer,
        clock,
    }
}

impl Fixture {
    /// Finish one `agent_loop` source run with `labels`, under the workflow
    /// id `id`, or a new id.
    async fn source_as(&mut self, id: Option<&str>, labels: &[&str]) -> ExecutionId {
        self.source_with(id, labels, |_| {}).await
    }

    /// As [`Self::source_as`], with `shape` applied to the task.
    async fn source_with(
        &mut self,
        id: Option<&str>,
        labels: &[&str],
        shape: impl FnOnce(&mut AgentTask),
    ) -> ExecutionId {
        let mut task = AgentTask::new("Check the refund.").kind(KIND);
        task.system = Some(BASE.to_owned());
        task.flow_labels = labels.iter().map(|label| (*label).to_owned()).collect();
        shape(&mut task);
        let input = serde_json::to_value(&task).unwrap();
        let exec = match id {
            Some(id) => self
                .rt
                .start_workflow_with_id(autumn_harvest_agent::WORKFLOW_NAME, id, input)
                .unwrap(),
            None => sqlite::start(&mut self.rt, &task).unwrap(),
        };
        let state = self.rt.run_until_blocked(exec).await.unwrap();
        assert!(matches!(state, RunState::Completed(_)), "{state:?}");
        exec
    }

    async fn source(&mut self, labels: &[&str]) -> ExecutionId {
        self.source_as(None, labels).await
    }

    /// Start a refine run on `run` and drive it until it blocks.
    async fn refine(&mut self, run: ExecutionId) -> (ExecutionId, RunState) {
        let input = RefineInput::new(KIND, vec![run], 1, RefineMode::Parallel, 7);
        let exec = sqlite::start_refine(&mut self.rt, &input).unwrap();
        let state = self.rt.run_until_blocked(exec).await.unwrap();
        (exec, state)
    }

    /// Refine `run`, approve the candidate, and return the report. Replay
    /// the history of the run strictly.
    async fn adopt(&mut self, run: ExecutionId) -> RefineReport {
        let (exec, state) = self.refine(run).await;
        self.finish(exec, state, &RefineDecision::Approve).await
    }

    /// Send `decision` to a waiting run, drive it to its end, and replay
    /// its history strictly.
    async fn finish(
        &mut self,
        exec: ExecutionId,
        mut state: RunState,
        decision: &RefineDecision,
    ) -> RefineReport {
        if !matches!(state, RunState::Completed(_)) {
            sqlite::decide_refine(&mut self.rt, exec, decision).unwrap();
            state = self.rt.run_until_blocked(exec).await.unwrap();
        }
        self.replay(exec).await;
        report_of(state)
    }

    /// Replay the history of `exec` strictly.
    fn replay(&self, exec: ExecutionId) -> impl Future<Output = ()> + Send + 'static {
        let events = self.rt.load_history(exec).unwrap();
        let harness = self.harness.clone();
        async move {
            let report = WorkflowReplayer::new()
                .register(vec![agent_refine_info()])
                .with_state(harness)
                .with_execution_id(exec)
                .replay_from_events(events)
                .await;
            assert!(
                matches!(report.status, ReplayStatus::ReplaySucceeded),
                "{report:?}"
            );
        }
    }

    /// Every block of the lesson scope.
    fn blocks(&self) -> impl Future<Output = Vec<MemoryBlock>> + Send + 'static {
        let memory = self.memory.clone().expect("a memory store");
        async move { memory.load(&lesson_scope(KIND)).await.unwrap() }
    }

    /// The lesson block of the kind.
    fn block(&self) -> impl Future<Output = MemoryBlock> + Send + 'static {
        let memory = self.memory.clone().expect("a memory store");
        async move { lesson_block(memory.as_ref()).await }
    }

    /// Set the refine policy of the kind.
    fn set_policy(&self, policy: &RefinePolicy) -> impl Future<Output = ()> + Send + 'static {
        let (store, policy) = (Arc::clone(&self.store), policy.clone());
        async move {
            let current = store.policy(KIND).await.unwrap();
            store
                .set_policy(KIND, &policy, current.revision, Provenance::operator("ops"))
                .await
                .unwrap();
        }
    }
}

/// The lesson block of the kind in `memory`.
async fn lesson_block(memory: &dyn MemoryStore) -> MemoryBlock {
    memory
        .load(&lesson_scope(KIND))
        .await
        .unwrap()
        .into_iter()
        .find(|block| block.label == "lessons")
        .unwrap_or_else(|| MemoryBlock::new("lessons", "", 0))
}

fn report_of(state: RunState) -> RefineReport {
    match state {
        RunState::Completed(value) => serde_json::from_value(value).unwrap(),
        other => panic!("expected a completed refine run, got {other:?}"),
    }
}

/// A policy that adopts a system edit with no wait, with `max` lessons.
fn auto_policy(max: u32) -> RefinePolicy {
    RefinePolicy {
        auto_adopt: AutoAdopt {
            parts: std::iter::once(OverlayPart::System).collect(),
            max_per_day: DailyCap::limited(100).unwrap(),
        },
        lessons: Lessons {
            max_entries: LessonEntries::new(max).unwrap(),
        },
        ..RefinePolicy::default()
    }
}

fn labels(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|item| (*item).to_owned()).collect()
}

/// The versions of the lessons, oldest first.
fn versions(block: &MemoryBlock) -> Vec<autumn_harvest_agent::OverlayVersion> {
    read_lessons(block)
        .iter()
        .map(|lesson| lesson.version)
        .collect()
}

/// A manual policy with `max` lessons: each candidate waits for a person.
fn manual_policy(max: u32) -> RefinePolicy {
    RefinePolicy {
        lessons: Lessons {
            max_entries: LessonEntries::new(max).unwrap(),
        },
        ..RefinePolicy::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_adopted_refine_writes_one_lesson_that_names_the_version() {
    let mut f = fixture();
    let run = f.source(&[]).await;
    let report = f.adopt(run).await;
    assert_eq!(report.status, RefineStatus::Adopted, "{report:?}");
    let adopted = report.gate.as_ref().unwrap().adopted.unwrap();
    assert!(
        matches!(report.lesson, Some(LessonOutcome::Written { version, evicted: 0 }) if version == adopted),
        "{report:?}"
    );
    let blocks = f.blocks().await;
    let labels_of: Vec<&str> = blocks.iter().map(|block| block.label.as_str()).collect();
    assert_eq!(labels_of, ["lessons"], "the scope holds one block");
    let lessons = read_lessons(&blocks[0]);
    assert_eq!(lessons.len(), 1, "{lessons:?}");
    assert_eq!(lessons[0].version, adopted);
    assert_eq!(lessons[0].sources, vec![run]);
    assert_eq!(lessons[0].why, "it checks the refund first");
    assert_eq!(lessons[0].lesson, "check the refund before the reply");
    assert_eq!(
        report.flow_labels,
        BTreeSet::new(),
        "a clean run has no label"
    );
    assert_eq!(blocks[0].labels.get(&blocks[0].entries[0]), None);
    // A second adoption adds a second lesson to the same block.
    let run = f.source(&[]).await;
    f.adopt(run).await;
    let blocks = f.blocks().await;
    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0].entries.len(), 2);
}

/// The lesson snapshot of each proposer request in the history of `exec`.
fn proposer_snapshots(f: &Fixture, exec: ExecutionId) -> Vec<MemorySnapshot> {
    f.rt.load_history(exec)
        .unwrap()
        .into_iter()
        .filter_map(|event| match event {
            WorkflowEvent::ActivityScheduled { name, input, .. }
                if name == "agent_refine_propose" =>
            {
                Some(serde_json::from_value(input["lessons"].clone()).unwrap())
            }
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_labelled_source_taints_the_lesson_and_the_next_proposer() {
    let mut f = fixture();
    let tainted = f.source(&["untrusted"]).await;
    let first = f.adopt(tainted).await;
    assert_eq!(first.status, RefineStatus::Adopted, "{first:?}");
    assert_eq!(first.flow_labels, labels(&["untrusted"]));
    let block = f.block().await;
    assert_eq!(block.entries.len(), 1);
    assert_eq!(
        block.labels.get(&block.entries[0]),
        Some(&labels(&["untrusted"]))
    );

    // A clean source. The proposer reads the tainted lesson.
    f.proposer.seen.lock().unwrap().clear();
    let clean = f.source(&[]).await;
    let (exec, state) = f.refine(clean).await;
    let second = f.finish(exec, state, &RefineDecision::Approve).await;
    assert_eq!(second.status, RefineStatus::Adopted, "{second:?}");
    let seen = f.proposer.seen.lock().unwrap().clone();
    assert_ne!(seen.len(), 0);
    for snapshot in &seen {
        let snapshot = snapshot.as_ref().expect("each stage reads the lessons");
        assert_eq!(snapshot.labels(), labels(&["untrusted"]));
        assert!(
            snapshot.text().contains("check the refund"),
            "{}",
            snapshot.text()
        );
    }
    // History records the labelled snapshot in each proposer request.
    let recorded = proposer_snapshots(&f, exec);
    assert_eq!(recorded.len(), seen.len());
    assert!(
        recorded
            .iter()
            .all(|snap| snap.labels() == labels(&["untrusted"]))
    );
    assert_eq!(
        second.flow_labels,
        labels(&["untrusted"]),
        "the run takes the labels"
    );
    let block = f.block().await;
    assert_eq!(block.entries.len(), 2);
    assert_eq!(
        block.labels.get(&block.entries[1]),
        Some(&labels(&["untrusted"])),
        "a lesson that read a tainted lesson stays tainted"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_store_without_labels_fails_the_write_and_keeps_the_adoption() {
    let mut f = fixture_with(Arc::new(PlainStore::default()));
    let run = f.source(&["untrusted"]).await;
    let report = f.adopt(run).await;
    assert_eq!(report.status, RefineStatus::Adopted, "{report:?}");
    let adopted = report.gate.as_ref().unwrap().adopted;
    let active = f.store.active(KIND).await.unwrap().map(|s| s.version());
    assert_eq!(active, adopted, "the adoption stands");
    match &report.lesson {
        Some(LessonOutcome::Failed { error }) => {
            assert!(error.contains("cannot keep flow labels"), "{error}");
        }
        other => panic!("expected a failed write, got {other:?}"),
    }
    assert_eq!(
        f.block().await.entries,
        Vec::<String>::new(),
        "no lesson lost its taint"
    );
    // A clean refine on the same store writes its lesson.
    let run = f.source(&[]).await;
    let report = f.adopt(run).await;
    assert!(
        matches!(report.lesson, Some(LessonOutcome::Written { .. })),
        "{report:?}"
    );
    assert_eq!(f.block().await.entries.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_store_with_no_block_or_no_store_fails_the_write_and_keeps_the_adoption() {
    let mut f = fixture_with(Arc::new(NoBlockStore::default()));
    let run = f.source(&[]).await;
    let report = f.adopt(run).await;
    assert_eq!(report.status, RefineStatus::Adopted, "{report:?}");
    assert!(
        matches!(&report.lesson, Some(LessonOutcome::Failed { error }) if error.contains("ensure_block")),
        "{report:?}"
    );
    let mut f = fixture_of(None, Arc::new(Selector));
    let run = f.source(&[]).await;
    let report = f.adopt(run).await;
    assert_eq!(report.status, RefineStatus::Adopted, "{report:?}");
    assert!(
        matches!(&report.lesson, Some(LessonOutcome::Failed { error }) if error.contains("no memory store")),
        "{report:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_bound_evicts_the_oldest_and_a_raise_applies_at_once() {
    let mut f = fixture();
    f.set_policy(&auto_policy(2)).await;
    let mut adopted = Vec::new();
    for n in 0..3 {
        let run = f.source(&[]).await;
        let report = f.adopt(run).await;
        assert_eq!(report.status, RefineStatus::AutoAdopted, "{report:?}");
        let evicted = u32::from(n == 2);
        assert!(
            matches!(report.lesson, Some(LessonOutcome::Written { evicted: e, .. }) if e == evicted),
            "{report:?}"
        );
        adopted.push(report.gate.unwrap().adopted.unwrap());
    }
    assert_eq!(
        versions(&f.block().await),
        adopted[1..],
        "the third lesson evicts the first"
    );

    f.set_policy(&auto_policy(3)).await;
    let run = f.source(&[]).await;
    let report = f.adopt(run).await;
    let fourth = report.gate.as_ref().unwrap().adopted.unwrap();
    assert!(
        matches!(
            report.lesson,
            Some(LessonOutcome::Written { evicted: 0, .. })
        ),
        "{report:?}"
    );
    assert_eq!(
        versions(&f.block().await),
        vec![adopted[1], adopted[2], fourth]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bound_change_during_the_wait_applies_to_the_write() {
    let mut f = fixture();
    f.set_policy(&manual_policy(2)).await;
    let mut adopted = Vec::new();
    for _ in 0..2 {
        let run = f.source(&[]).await;
        adopted.push(f.adopt(run).await.gate.unwrap().adopted.unwrap());
    }
    // The run records a bound of 2 at its start. The operator raises it
    // while the run waits for a person.
    let run = f.source(&[]).await;
    let (exec, state) = f.refine(run).await;
    assert!(!matches!(state, RunState::Completed(_)), "{state:?}");
    f.set_policy(&manual_policy(3)).await;
    let report = f.finish(exec, state, &RefineDecision::Approve).await;
    assert!(
        matches!(
            report.lesson,
            Some(LessonOutcome::Written { evicted: 0, .. })
        ),
        "{report:?}"
    );
    adopted.push(report.gate.unwrap().adopted.unwrap());
    assert_eq!(versions(&f.block().await), adopted);
    // A lower bound during the wait applies as well.
    let run = f.source(&[]).await;
    let (exec, state) = f.refine(run).await;
    f.set_policy(&manual_policy(1)).await;
    let report = f.finish(exec, state, &RefineDecision::Approve).await;
    assert!(
        matches!(
            report.lesson,
            Some(LessonOutcome::Written { evicted: 3, .. })
        ),
        "{report:?}"
    );
    let newest = report.gate.unwrap().adopted.unwrap();
    assert_eq!(versions(&f.block().await), vec![newest], "the newest stays");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rejected_or_timed_out_refine_writes_no_lesson() {
    use std::sync::atomic::Ordering;
    let mut f = fixture();
    let run = f.source(&[]).await;
    let (exec, state) = f.refine(run).await;
    assert!(!matches!(state, RunState::Completed(_)), "{state:?}");
    let reject = RefineDecision::Reject {
        reason: "no".to_owned(),
    };
    let report = f.finish(exec, state, &reject).await;
    assert_eq!(report.status, RefineStatus::Rejected);
    assert_eq!(report.lesson, None);

    let run = f.source(&[]).await;
    let (exec, _) = f.refine(run).await;
    f.clock.fetch_add(8 * 24 * HOUR_MS, Ordering::SeqCst);
    let report = report_of(f.rt.run_until_blocked(exec).await.unwrap());
    assert_eq!(report.status, RefineStatus::TimedOut);
    assert_eq!(report.lesson, None);
    assert_eq!(f.block().await.entries, Vec::<String>::new());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_superseded_or_dry_run_writes_no_lesson() {
    let mut f = fixture();
    let one = f.source(&[]).await;
    let two = f.source(&[]).await;
    let (first, first_state) = f.refine(one).await;
    let (second, second_state) = f.refine(two).await;
    let report = f.finish(first, first_state, &RefineDecision::Approve).await;
    assert_eq!(report.status, RefineStatus::Adopted);
    let report = f
        .finish(second, second_state, &RefineDecision::Approve)
        .await;
    assert_eq!(report.status, RefineStatus::Superseded, "{report:?}");
    assert_eq!(report.lesson, None);
    assert_eq!(f.block().await.entries.len(), 1);

    let run = f.source(&[]).await;
    let input = RefineInput::new(KIND, vec![run], 1, RefineMode::Parallel, 7).dry_run();
    let exec = sqlite::start_refine(&mut f.rt, &input).unwrap();
    let report = report_of(f.rt.run_until_blocked(exec).await.unwrap());
    assert_eq!(report.status, RefineStatus::Chosen);
    assert_eq!(report.lesson, None);
    assert_eq!(f.block().await.entries.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_edit_writes_a_lesson_that_names_the_edited_version() {
    let mut f = fixture();
    let run = f.source(&[]).await;
    let (exec, state) = f.refine(run).await;
    let overlay = ProfileOverlay {
        system: Some("An edited prompt.".to_owned()),
        ..ProfileOverlay::default()
    };
    let report = f
        .finish(exec, state, &RefineDecision::Edit { overlay })
        .await;
    assert_eq!(report.status, RefineStatus::Adopted, "{report:?}");
    let gate = report.gate.unwrap();
    let adopted = gate.adopted.unwrap();
    assert_ne!(adopted, gate.version, "the edit is a new version");
    assert_eq!(versions(&f.block().await), vec![adopted]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rule_pick_or_a_plain_selector_gives_no_text() {
    for (selector, expected) in [
        (
            Arc::new(Outside) as Arc<dyn OverlaySelector>,
            SelectedBy::Rule,
        ),
        (
            Arc::new(SelectOnly) as Arc<dyn OverlaySelector>,
            SelectedBy::Selector,
        ),
    ] {
        let mut f = fixture_of(Some(Arc::new(InMemoryMemoryStore::new())), selector);
        let run = f.source(&[]).await;
        let report = f.adopt(run).await;
        assert_eq!(report.status, RefineStatus::Adopted, "{report:?}");
        let lessons = read_lessons(&f.block().await);
        assert_eq!(lessons.len(), 1);
        assert_eq!(lessons[0].why, "", "{report:?}");
        assert_eq!(lessons[0].lesson, "");
        let entry = &f.block().await.entries[0];
        assert!(!entry.contains("\"why\""), "{entry}");
        let by: Vec<SelectedBy> = report.selections.iter().map(|pick| pick.by).collect();
        assert_eq!(by, [expected]);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lesson_names_the_agent_of_a_memory_scope_and_of_each_read_lesson() {
    let mut f = fixture();
    let scoped = f
        .source_with(None, &[], |task| {
            task.memory_scope = Some(MemoryScope::new("agent:support/acct-42"));
        })
        .await;
    let report = f.adopt(scoped).await;
    assert!(matches!(report.lesson, Some(LessonOutcome::Written { .. })));
    let lessons = read_lessons(&f.block().await);
    assert_eq!(
        lessons[0].agents,
        BTreeSet::from(["support/acct-42".to_owned()])
    );
    // A run that read that lesson names its agent too.
    let plain = f.source_as(Some("support/acct-7"), &[]).await;
    f.adopt(plain).await;
    let lessons = read_lessons(&f.block().await);
    assert_eq!(
        lessons[1].agents,
        BTreeSet::from(["support/acct-42".to_owned(), "support/acct-7".to_owned()])
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_erase_of_an_agent_removes_exactly_its_lessons() {
    let mut f = fixture();
    let ada = AgentId::parse("support/acct-42").unwrap();
    // The lessons of other agents come first, so none of them read a
    // lesson of `ada`.
    for id in ["support/acct-7", "support/acct-4"] {
        let run = f.source_as(Some(id), &[]).await;
        let report = f.adopt(run).await;
        assert!(
            matches!(report.lesson, Some(LessonOutcome::Written { .. })),
            "{report:?}"
        );
    }
    let scoped = f
        .source_with(None, &[], |task| {
            task.memory_scope = Some(MemoryScope::new("agent:support/acct-42"));
        })
        .await;
    f.adopt(scoped).await;
    let named = f.source_as(Some("support/acct-42"), &[]).await;
    f.adopt(named).await;
    // A plain note in the block is no lesson, so it stays.
    let memory = f.memory.clone().unwrap();
    let scope = lesson_scope(KIND);
    memory.ensure_block(&scope, lessons_block()).await.unwrap();
    let note = MemoryOp::Add {
        block: "lessons".to_owned(),
        text: "a plain note about support/acct-42".to_owned(),
    };
    memory
        .apply(&scope, note, &EditKey::new("note"))
        .await
        .unwrap();
    let before = read_lessons(&f.block().await);
    assert_eq!(before.len(), 4);
    let kept: Vec<_> = before
        .iter()
        .filter(|lesson| !lesson.agents.contains("support/acct-42"))
        .cloned()
        .collect();
    assert_eq!(kept.len(), 2, "{before:?}");

    sqlite::send(
        &mut f.rt,
        &ada,
        &AgentOp::Control(Control::ResetMemory),
        None,
    )
    .unwrap();
    sqlite::drive(&mut f.rt, &ada).await.unwrap();
    let state = sqlite::state(&f.rt, &ada).await.unwrap().unwrap();
    assert_eq!(state.failures, 0, "{:?}", state.last_error);
    assert_eq!(state.lessons_removed, Some(2));
    // `harvest agents erase` reads this key from the final state.
    assert_eq!(serde_json::to_value(&state).unwrap()["lessons_removed"], 2);

    let block = f.block().await;
    assert_eq!(
        read_lessons(&block),
        kept,
        "the lessons of other agents stay"
    );
    assert!(
        block
            .entries
            .iter()
            .any(|entry| entry.starts_with("a plain note"))
    );
}
