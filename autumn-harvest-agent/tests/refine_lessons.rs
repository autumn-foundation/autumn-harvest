//! The lesson library of a kind on SQLite.
//!
//! A scripted proposer, composer, selector and model drive each refine run.
//! Each test checks one criterion of the issue. The design is in
//! `docs/design/DESIGN-2382.md`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest_agent::memory::{MemoryOp, MemorySnapshot};
use autumn_harvest_agent::model::BoxFuture;
use autumn_harvest_agent::refine::{
    AutoAdopt, Charge, Choice, ComposeContext, DailyCap, LessonEntries, LessonOutcome, Lessons,
    OverlayComposer, OverlaySelector, RefineDecision, RefineInput, RefineMode, RefinePolicy,
    RefineReport, SelectContext, agent_refine_info, lesson_scope, read_lessons,
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
    memory: Arc<dyn MemoryStore>,
    proposer: Arc<Proposer>,
    clock: Arc<std::sync::atomic::AtomicI64>,
}

fn fixture() -> Fixture {
    fixture_with(Arc::new(InMemoryMemoryStore::new()))
}

fn fixture_with(memory: Arc<dyn MemoryStore>) -> Fixture {
    use std::sync::atomic::{AtomicI64, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("lessons.db");
    let proposer = Arc::new(Proposer::default());
    let base = AgentHarness::new(Arc::new(Model) as Arc<dyn AgentModel>)
        .agent(AgentProfile::new(KIND).system(BASE).max_steps(20));
    let store = Arc::new(InMemoryOverlayStore::new(Arc::new(base.clone())));
    let harness = base
        .overlay_store(Arc::clone(&store) as Arc<dyn ProfileOverlayStore>)
        .proposer(Arc::clone(&proposer) as Arc<dyn OverlayProposer>)
        .composer(Arc::new(Composer) as Arc<dyn OverlayComposer>)
        .selector(Arc::new(Selector) as Arc<dyn OverlaySelector>)
        .memory(Arc::clone(&memory))
        .refine_sources(Arc::new(sqlite::Sources::open(&db)));
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
        let mut task = AgentTask::new("Check the refund.").kind(KIND);
        task.system = Some(BASE.to_owned());
        task.flow_labels = labels.iter().map(|label| (*label).to_owned()).collect();
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
        let (exec, mut state) = self.refine(run).await;
        if !matches!(state, RunState::Completed(_)) {
            sqlite::decide_refine(&mut self.rt, exec, &RefineDecision::Approve).unwrap();
            state = self.rt.run_until_blocked(exec).await.unwrap();
        }
        let events = self.rt.load_history(exec).unwrap();
        let report = WorkflowReplayer::new()
            .register(vec![agent_refine_info()])
            .with_state(self.harness.clone())
            .with_execution_id(exec)
            .replay_from_events(events)
            .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "{report:?}"
        );
        report_of(state)
    }

    /// The lesson block of the kind.
    fn block(&self) -> impl Future<Output = MemoryBlock> + Send + 'static {
        let memory = Arc::clone(&self.memory);
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
    let lessons = read_lessons(&f.block().await);
    assert_eq!(lessons.len(), 1, "{lessons:?}");
    assert_eq!(lessons[0].version, adopted);
    assert_eq!(lessons[0].sources, vec![run]);
    assert_eq!(lessons[0].why, "it checks the refund first");
    assert_eq!(lessons[0].lesson, "check the refund before the reply");
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
    let second = f.adopt(clean).await;
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
            assert!(error.contains("flow labels"), "{error}");
        }
        other => panic!("expected a failed write, got {other:?}"),
    }
    assert_eq!(
        f.block().await.entries,
        Vec::<String>::new(),
        "no lesson lost its taint"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_bound_evicts_the_oldest_and_a_raise_applies_at_once() {
    let mut f = fixture();
    f.set_policy(&auto_policy(2)).await;
    let mut versions = Vec::new();
    for _ in 0..3 {
        let run = f.source(&[]).await;
        let report = f.adopt(run).await;
        assert_eq!(report.status, RefineStatus::AutoAdopted, "{report:?}");
        versions.push(report.gate.unwrap().adopted.unwrap());
    }
    let kept: Vec<_> = read_lessons(&f.block().await)
        .iter()
        .map(|lesson| lesson.version)
        .collect();
    assert_eq!(kept, versions[1..], "the third lesson evicts the first");

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
    let kept: Vec<_> = read_lessons(&f.block().await)
        .iter()
        .map(|lesson| lesson.version)
        .collect();
    assert_eq!(kept, vec![versions[1], versions[2], fourth]);
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
    sqlite::decide_refine(&mut f.rt, exec, &reject).unwrap();
    let report = report_of(f.rt.run_until_blocked(exec).await.unwrap());
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
async fn an_erase_of_an_agent_removes_exactly_its_lessons() {
    let mut f = fixture();
    let ada = AgentId::parse("support/acct-42").unwrap();
    for id in ["support/acct-42", "support/acct-7", "support/acct-42"] {
        let run = f.source_as(Some(id), &[]).await;
        let report = f.adopt(run).await;
        assert!(
            matches!(report.lesson, Some(LessonOutcome::Written { .. })),
            "{report:?}"
        );
    }
    let before = read_lessons(&f.block().await);
    assert_eq!(before.len(), 3);
    assert!(before[0].agents.contains("support/acct-42"), "{before:?}");

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

    let after = read_lessons(&f.block().await);
    assert_eq!(after.len(), 1, "{after:?}");
    assert_eq!(after[0], before[1], "the lesson of another agent stays");
}
