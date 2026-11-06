//! The adoption gate of `agent_refine` on SQLite.
//!
//! A scripted proposer, composer and model drive each run. A test clock
//! moves the deadline of the gate. Each test checks one criterion of the
//! issue. The design is in `docs/design/DESIGN-2381.md`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest_agent::model::BoxFuture;
use autumn_harvest_agent::refine::{
    ApprovalTimeout, Charge, ComposeContext, OverlayComposer, PolicyCeilings, REFINE_KIND_ATTR,
    REFINE_STATUS_ATTR, RefineDecision, RefineInput, RefineMode, RefinePolicy, RefineReport,
    agent_refine_info, refine_approval_signal,
};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentId, AgentModel, AgentOp, AgentProfile, AgentTask, AutoAdopt,
    ChatRequest, ChatResponse, ChatRole, ContentPart, InMemoryOverlayStore, OverlayPart,
    OverlayProposer, OverlayVersion, ProfileOverlay, ProfileOverlayStore, ProposeContext,
    ProposerReply, Provenance, RefineStatus, StopReason, TokenUsage, sqlite,
};
use autumn_harvest_sqlite::{
    ExecutionFilter, ExecutionId, ExecutionSummary, RunState, SqliteRuntime,
};
use chrono::{DateTime, TimeZone, Utc};
use serde_json::json;

const KIND: &str = "support";
const BASE: &str = "Help the user.";
const HOUR_MS: i64 = 3_600_000;

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

/// A model that answers each run and records each system text.
#[derive(Debug, Default)]
struct Model {
    prompts: Mutex<Vec<String>>,
}

impl AgentModel for Model {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        self.prompts.lock().unwrap().push(system_of(request));
        Box::pin(async move {
            Ok(ChatResponse {
                content: vec![ContentPart::Text("done".to_owned())],
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::new(10, 5),
            })
        })
    }
}

/// A proposer that adds one line to the current system text.
#[derive(Debug, Default)]
struct Proposer {
    /// With a store, the next call puts and activates an operator version,
    /// as an operator does while the loop runs.
    meddle: Mutex<Option<Arc<InMemoryOverlayStore>>>,
}

/// The overlay that an operator activates during the loop.
fn operator_overlay() -> ProfileOverlay {
    ProfileOverlay {
        system: Some("Operator prompt.".to_owned()),
        ..ProfileOverlay::default()
    }
}

impl OverlayProposer for Proposer {
    fn propose<'a>(
        &'a self,
        context: &'a ProposeContext<'a>,
    ) -> BoxFuture<'a, Result<ProposerReply, AgentError>> {
        context.charge(Charge::new(Some("scripted"), TokenUsage::new(1, 1)));
        let meddle = self.meddle.lock().unwrap().take();
        let from = context.current.system.as_deref().unwrap_or(BASE);
        let reply = ProfileOverlay {
            system: Some(format!(
                "{from}\n{} edit {}",
                context.dimension, context.sample
            )),
            ..ProfileOverlay::default()
        };
        Box::pin(async move {
            if let Some(store) = meddle {
                let ops = || Provenance::operator("ops");
                let own = store.put(KIND, &operator_overlay(), ops()).await.unwrap();
                let active = store.active(KIND).await.unwrap().map(|s| s.version());
                store.activate(KIND, own, active, ops()).await.unwrap();
            }
            Ok(ProposerReply::Overlay(reply))
        })
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

/// A runtime, its store and a test clock.
struct Fixture {
    _dir: tempfile::TempDir,
    db: std::path::PathBuf,
    harness: AgentHarness,
    rt: SqliteRuntime,
    store: Arc<InMemoryOverlayStore>,
    model: Arc<Model>,
    proposer: Arc<Proposer>,
    clock: Arc<AtomicI64>,
}

/// The start of the test clock.
fn epoch() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 1, 5, 12, 0, 0).unwrap()
}

fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("gate.db");
    let model = Arc::new(Model::default());
    let proposer = Arc::new(Proposer::default());
    let base = AgentHarness::new(Arc::clone(&model) as Arc<dyn AgentModel>)
        .agent(AgentProfile::new(KIND).system(BASE).max_steps(20));
    let store = Arc::new(InMemoryOverlayStore::new(Arc::new(base.clone())));
    let harness = base
        .overlay_store(Arc::clone(&store) as Arc<dyn ProfileOverlayStore>)
        .proposer(Arc::clone(&proposer) as Arc<dyn OverlayProposer>)
        .composer(Arc::new(Composer) as Arc<dyn OverlayComposer>)
        .refine_sources(Arc::new(sqlite::Sources::open(&db)));
    let clock = Arc::new(AtomicI64::new(epoch().timestamp_millis()));
    let rt = open(&db, &harness, &clock);
    Fixture {
        _dir: dir,
        db,
        harness,
        rt,
        store,
        model,
        proposer,
        clock,
    }
}

/// A runtime on `db` with the test clock.
fn open(db: &std::path::Path, harness: &AgentHarness, clock: &Arc<AtomicI64>) -> SqliteRuntime {
    let mut rt = SqliteRuntime::open(db).unwrap();
    let read = Arc::clone(clock);
    rt.set_clock(move || DateTime::from_timestamp_millis(read.load(Ordering::SeqCst)).unwrap());
    sqlite::register(&mut rt, Arc::new(harness.clone()));
    rt
}

impl Fixture {
    /// Drop the runtime and open a new one on the same file, as a restart
    /// of the worker does.
    fn restart(&mut self) {
        let old = std::mem::replace(&mut self.rt, SqliteRuntime::open_in_memory().unwrap());
        drop(old);
        self.rt = open(&self.db, &self.harness, &self.clock);
    }

    /// Move the test clock forward by `ms`.
    fn advance(&self, ms: i64) {
        self.clock.fetch_add(ms, Ordering::SeqCst);
    }

    /// Finish one `agent_loop` source run.
    async fn source(&mut self) -> ExecutionId {
        let mut task = AgentTask::new("Check the refund.").kind(KIND);
        task.system = Some(BASE.to_owned());
        let exec = sqlite::start(&mut self.rt, &task).unwrap();
        let state = self.rt.run_until_blocked(exec).await.unwrap();
        assert!(matches!(state, RunState::Completed(_)), "{state:?}");
        exec
    }

    /// Start a refine run on a new source, and drive it to the gate.
    async fn waiting(&mut self) -> ExecutionId {
        let run = self.source().await;
        let input = RefineInput::new(KIND, vec![run], 1, RefineMode::Sequential, 7);
        let exec = sqlite::start_refine(&mut self.rt, &input).unwrap();
        let state = self.rt.run_until_blocked(exec).await.unwrap();
        assert!(blocked(&state), "the run waits at the gate: {state:?}");
        exec
    }

    async fn drive(&mut self, exec: ExecutionId) -> RunState {
        self.rt.run_until_blocked(exec).await.unwrap()
    }

    fn decide(&mut self, exec: ExecutionId, decision: &RefineDecision) {
        sqlite::decide_refine(&mut self.rt, exec, decision).unwrap();
    }

    /// The search attribute `key` of `exec`.
    fn attr(&self, exec: ExecutionId, key: &str) -> Option<serde_json::Value> {
        let description = self.rt.describe(exec).unwrap();
        description
            .summary
            .search_attributes
            .value()
            .and_then(|attrs| attrs.get(key).cloned())
    }
}

/// Replay the recorded history of `exec` strictly.
async fn replay(harness: AgentHarness, events: Vec<WorkflowEvent>, exec: ExecutionId) {
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

/// The active version of the kind.
async fn active(store: &InMemoryOverlayStore) -> Option<OverlayVersion> {
    store
        .active(KIND)
        .await
        .unwrap()
        .map(|stored| stored.version())
}

/// Every version of the kind with its provenance.
async fn stored_versions(
    store: &InMemoryOverlayStore,
) -> Vec<(OverlayVersion, Provenance, ProfileOverlay)> {
    store
        .list(KIND)
        .await
        .unwrap()
        .iter()
        .map(|stored| {
            (
                stored.version(),
                stored.provenance().clone(),
                stored.overlay().clone(),
            )
        })
        .collect()
}

/// Set the approval timeout of the kind.
async fn set_timeout(store: &InMemoryOverlayStore, timeout: Duration) {
    let policy = RefinePolicy {
        approval_timeout: ApprovalTimeout::new(timeout).unwrap(),
        ..RefinePolicy::default()
    };
    store
        .set_policy(KIND, &policy, None, Provenance::operator("ops"))
        .await
        .unwrap();
}

/// Whether the run waits on a signal or a timer.
const fn blocked(state: &RunState) -> bool {
    matches!(state, RunState::WaitingSignal(_) | RunState::WaitingTimer)
}

fn report_of(state: RunState) -> RefineReport {
    match state {
        RunState::Completed(value) => serde_json::from_value(value).unwrap(),
        other => panic!("expected a completed refine run, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_timeout_leaves_the_pointer_and_lists_the_version_inactive() {
    let mut f = fixture();
    let exec = f.waiting().await;
    let versions = stored_versions(&f.store).await;
    assert_eq!(versions.len(), 1, "{versions:?}");
    assert_eq!(versions[0].1, Provenance::refine(exec));
    assert_eq!(active(&f.store).await, None);
    f.advance(7 * 24 * HOUR_MS + 1_000);
    let report = report_of(f.drive(exec).await);
    assert_eq!(report.status, RefineStatus::TimedOut, "{report:?}");
    assert_eq!(active(&f.store).await, None, "a timeout activates nothing");
    assert_eq!(
        stored_versions(&f.store).await.len(),
        1,
        "the version stays stored"
    );
    assert_eq!(report.gate.as_ref().expect("a gate").version, versions[0].0);
    assert_eq!(f.attr(exec, REFINE_STATUS_ATTR), Some(json!("timed_out")));
}

#[tokio::test(flavor = "multi_thread")]
async fn reject_leaves_the_pointer_and_sets_rejected() {
    let mut f = fixture();
    let exec = f.waiting().await;
    f.decide(
        exec,
        &RefineDecision::Reject {
            reason: "too long".to_owned(),
        },
    );
    let report = report_of(f.drive(exec).await);
    assert_eq!(report.status, RefineStatus::Rejected, "{report:?}");
    assert_eq!(active(&f.store).await, None);
    let versions = stored_versions(&f.store).await;
    assert_eq!(versions.len(), 1, "the version stays stored");
    assert_eq!(f.attr(exec, REFINE_STATUS_ATTR), Some(json!("rejected")));
    assert_eq!(f.attr(exec, REFINE_KIND_ATTR), Some(json!(KIND)));
}

#[tokio::test(flavor = "multi_thread")]
async fn approve_activates_the_version_and_the_next_op_sees_its_prompt() {
    let mut f = fixture();
    let exec = f.waiting().await;
    let (chosen, _, overlay) = stored_versions(&f.store).await.remove(0);
    f.decide(exec, &RefineDecision::Approve);
    let report = report_of(f.drive(exec).await);
    assert_eq!(report.status, RefineStatus::Adopted, "{report:?}");
    assert_eq!(active(&f.store).await, Some(chosen));
    let gate = report.gate.expect("a gate record");
    assert_eq!(gate.adopted, Some(chosen));
    assert_eq!(f.attr(exec, REFINE_STATUS_ATTR), Some(json!("adopted")));
    let last = f.store.last_activation(KIND).await.unwrap().unwrap();
    assert_eq!(last.actor(), &Provenance::refine(exec));
    // The next op of the kind runs on the adopted prompt.
    let id = AgentId::new(KIND, "next").unwrap();
    f.model.prompts.lock().unwrap().clear();
    sqlite::send(&mut f.rt, &id, &AgentOp::message("hello"), None).unwrap();
    sqlite::drive(&mut f.rt, &id).await.unwrap();
    let prompts = f.model.prompts.lock().unwrap().clone();
    let want = overlay.system.unwrap();
    assert!(
        prompts.iter().any(|prompt| prompt.contains(&want)),
        "the op sees the adopted prompt: {prompts:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_edit_past_the_ceilings_is_refused_and_the_run_keeps_waiting() {
    let mut f = fixture();
    let exec = f.waiting().await;
    // The edit comes late, so a reset of the deadline would show.
    f.advance(6 * 24 * HOUR_MS);
    let wide = ProfileOverlay {
        max_steps: Some(1_000),
        ..ProfileOverlay::default()
    };
    f.decide(exec, &RefineDecision::Edit { overlay: wide });
    let state = f.drive(exec).await;
    assert!(blocked(&state), "a refused edit keeps the wait: {state:?}");
    assert_eq!(
        active(&f.store).await,
        None,
        "a refused edit activates nothing"
    );
    assert_eq!(
        stored_versions(&f.store).await.len(),
        1,
        "a refused edit puts nothing"
    );
    assert_eq!(
        f.attr(exec, REFINE_STATUS_ATTR),
        Some(json!("waiting_approval"))
    );
    // The deadline stays at seven days from the open.
    f.advance(24 * HOUR_MS + 1_000);
    let report = report_of(f.drive(exec).await);
    assert_eq!(report.status, RefineStatus::TimedOut, "{report:?}");
    assert_eq!(active(&f.store).await, None);
    let gate = report.gate.expect("a gate record");
    assert!(
        gate.notes.iter().any(|note| note.contains("refused")),
        "{gate:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_edit_past_the_run_ceilings_is_refused_after_a_wider_policy() {
    let mut f = fixture();
    let exec = f.waiting().await;
    // The store now allows 1000 steps. The run read 20 at its start.
    let wide = RefinePolicy {
        ceilings: PolicyCeilings {
            max_steps: std::num::NonZeroU32::new(1_000),
            ..PolicyCeilings::default()
        },
        ..RefinePolicy::default()
    };
    f.store
        .set_policy(KIND, &wide, None, Provenance::operator("ops"))
        .await
        .unwrap();
    let edit = ProfileOverlay {
        max_steps: Some(500),
        ..ProfileOverlay::default()
    };
    f.decide(exec, &RefineDecision::Edit { overlay: edit });
    let state = f.drive(exec).await;
    assert!(blocked(&state), "{state:?}");
    assert_eq!(active(&f.store).await, None);
    assert_eq!(
        stored_versions(&f.store).await.len(),
        1,
        "the store got no put"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_edit_in_the_ceilings_puts_and_activates_a_further_version() {
    let mut f = fixture();
    let exec = f.waiting().await;
    let edited = ProfileOverlay {
        system: Some("Help the user. Be brief.".to_owned()),
        ..ProfileOverlay::default()
    };
    f.decide(
        exec,
        &RefineDecision::Edit {
            overlay: edited.clone(),
        },
    );
    let report = report_of(f.drive(exec).await);
    assert_eq!(report.status, RefineStatus::Adopted, "{report:?}");
    let versions = stored_versions(&f.store).await;
    assert_eq!(versions.len(), 2, "{versions:?}");
    assert_eq!(versions[1].1, Provenance::refine(exec));
    assert_eq!(versions[1].2, edited);
    assert_eq!(active(&f.store).await, Some(versions[1].0));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_approval_sent_before_the_wait_opened_is_dropped_and_the_run_times_out() {
    let mut f = fixture();
    let run = f.source().await;
    let input = RefineInput::new(KIND, vec![run], 1, RefineMode::Sequential, 7);
    let exec = sqlite::start_refine(&mut f.rt, &input).unwrap();
    // The run does not wait yet.
    f.decide(exec, &RefineDecision::Approve);
    let state = f.drive(exec).await;
    assert!(blocked(&state), "the early approval is dropped: {state:?}");
    assert_eq!(active(&f.store).await, None);
    f.advance(7 * 24 * HOUR_MS + 1_000);
    let report = report_of(f.drive(exec).await);
    assert_eq!(report.status, RefineStatus::TimedOut, "{report:?}");
    assert_eq!(active(&f.store).await, None);
    assert_eq!(report.gate.expect("a gate record").dropped, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_operator_activation_during_the_wait_makes_approve_superseded() {
    let mut f = fixture();
    let exec = f.waiting().await;
    let manual = ProfileOverlay {
        system: Some("Operator prompt.".to_owned()),
        ..ProfileOverlay::default()
    };
    let ops = || Provenance::operator("ops");
    let own = f.store.put(KIND, &manual, ops()).await.unwrap();
    f.store.activate(KIND, own, None, ops()).await.unwrap();
    f.decide(exec, &RefineDecision::Approve);
    let report = report_of(f.drive(exec).await);
    assert_eq!(report.status, RefineStatus::Superseded, "{report:?}");
    assert_eq!(
        active(&f.store).await,
        Some(own),
        "the operator version stays"
    );
    assert_eq!(f.attr(exec, REFINE_STATUS_ATTR), Some(json!("superseded")));
    assert_eq!(report.gate.expect("a gate").superseded_by, Some(own));
}

#[tokio::test(flavor = "multi_thread")]
async fn the_status_filter_lists_a_waiting_run_and_not_a_finished_one() {
    let mut f = fixture();
    let done = f.waiting().await;
    f.decide(
        done,
        &RefineDecision::Reject {
            reason: String::new(),
        },
    );
    report_of(f.drive(done).await);
    let waiting = f.waiting().await;
    let filter = ExecutionFilter::new()
        .workflow_name("agent_refine")
        .search_attribute(REFINE_STATUS_ATTR, json!("waiting_approval"))
        .search_attribute(REFINE_KIND_ATTR, json!(KIND));
    let listed: Vec<ExecutionId> =
        f.rt.list_executions(&filter)
            .unwrap()
            .executions
            .iter()
            .filter_map(ExecutionSummary::execution_id)
            .collect();
    assert_eq!(listed, vec![waiting]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_one_hour_timeout_ends_the_wait_after_one_hour_of_test_clock() {
    let mut f = fixture();
    set_timeout(&f.store, Duration::from_secs(3_600)).await;
    let exec = f.waiting().await;
    f.advance(HOUR_MS - 60_000);
    let state = f.drive(exec).await;
    assert!(blocked(&state), "the gate waits under one hour: {state:?}");
    f.advance(61_000);
    let report = report_of(f.drive(exec).await);
    assert_eq!(report.status, RefineStatus::TimedOut, "{report:?}");
    assert_eq!(f.attr(exec, REFINE_STATUS_ATTR), Some(json!("timed_out")));
    assert_eq!(active(&f.store).await, None);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unreadable_payload_keeps_the_wait() {
    let mut f = fixture();
    let exec = f.waiting().await;
    let signal = refine_approval_signal(exec);
    f.rt.send_signal(exec, &signal, json!({"verdict": "maybe"}))
        .unwrap();
    let state = f.drive(exec).await;
    assert!(blocked(&state), "{state:?}");
    f.decide(exec, &RefineDecision::Approve);
    let report = report_of(f.drive(exec).await);
    assert_eq!(report.status, RefineStatus::Adopted, "{report:?}");
    let gate = report.gate.expect("a gate record");
    assert_eq!(gate.decisions, 2);
    assert!(
        gate.notes.iter().any(|note| note.contains("unreadable")),
        "{gate:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dry_run_stores_nothing_and_opens_no_gate() {
    let mut f = fixture();
    let run = f.source().await;
    let input = RefineInput::new(KIND, vec![run], 1, RefineMode::Sequential, 7).dry_run();
    let exec = sqlite::start_refine(&mut f.rt, &input).unwrap();
    let report = report_of(f.drive(exec).await);
    assert_eq!(report.status, RefineStatus::Chosen, "{report:?}");
    assert!(report.gate.is_none());
    assert_eq!(
        stored_versions(&f.store).await.len(),
        0,
        "a dry run stores nothing"
    );
    assert_eq!(f.attr(exec, REFINE_STATUS_ATTR), Some(json!("chosen")));
}

#[test]
fn an_approval_payload_decodes_as_a_refine_decision() {
    let approve: RefineDecision = serde_json::from_value(json!({"verdict": "approve"})).unwrap();
    assert_eq!(approve, RefineDecision::Approve);
    let edit: RefineDecision =
        serde_json::from_value(json!({"verdict": "edit", "arguments": {"system": "x"}})).unwrap();
    assert!(matches!(edit, RefineDecision::Edit { .. }));
    let reject: RefineDecision = serde_json::from_value(json!({"verdict": "reject"})).unwrap();
    assert!(matches!(reject, RefineDecision::Reject { .. }));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_allowed_part_goes_live_with_no_wait() {
    let mut f = fixture();
    let policy = RefinePolicy {
        auto_adopt: AutoAdopt {
            parts: std::iter::once(OverlayPart::System).collect(),
            ..AutoAdopt::default()
        },
        ..RefinePolicy::default()
    };
    f.store
        .set_policy(KIND, &policy, None, Provenance::operator("ops"))
        .await
        .unwrap();
    let run = f.source().await;
    let input = RefineInput::new(KIND, vec![run], 1, RefineMode::Sequential, 7);
    let exec = sqlite::start_refine(&mut f.rt, &input).unwrap();
    let report = report_of(f.drive(exec).await);
    assert_eq!(report.status, RefineStatus::AutoAdopted, "{report:?}");
    let versions = stored_versions(&f.store).await;
    assert_eq!(versions[0].1, Provenance::auto(exec));
    assert_eq!(active(&f.store).await, Some(versions[0].0));
    assert_eq!(
        f.attr(exec, REFINE_STATUS_ATTR),
        Some(json!("auto_adopted"))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restart_keeps_the_wait_and_its_deadline() {
    let mut f = fixture();
    let exec = f.waiting().await;
    let wide = ProfileOverlay {
        max_steps: Some(1_000),
        ..ProfileOverlay::default()
    };
    f.decide(exec, &RefineDecision::Edit { overlay: wide });
    assert!(blocked(&f.drive(exec).await));
    f.restart();
    f.advance(HOUR_MS);
    let state = f.drive(exec).await;
    assert!(blocked(&state), "the wait survives a restart: {state:?}");
    f.decide(exec, &RefineDecision::Approve);
    let report = report_of(f.drive(exec).await);
    assert_eq!(report.status, RefineStatus::Adopted, "{report:?}");
    let gate = report.gate.expect("a gate record");
    assert_eq!(gate.decisions, 2);
    let events = f.rt.load_history(exec).unwrap();
    replay(f.harness.clone(), events, exec).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn approve_expects_the_version_active_at_refine_start() {
    let mut f = fixture();
    let base = ProfileOverlay {
        system: Some("Operator base.".to_owned()),
        ..ProfileOverlay::default()
    };
    let ops = || Provenance::operator("ops");
    let first = f.store.put(KIND, &base, ops()).await.unwrap();
    f.store.activate(KIND, first, None, ops()).await.unwrap();
    let exec = f.waiting().await;
    let chosen = stored_versions(&f.store).await[1].0;
    assert_eq!(active(&f.store).await, Some(first));
    f.decide(exec, &RefineDecision::Approve);
    let report = report_of(f.drive(exec).await);
    assert_eq!(report.status, RefineStatus::Adopted, "{report:?}");
    let gate = report.gate.expect("a gate record");
    assert_eq!(gate.expected, Some(first));
    assert_eq!(gate.adopted, Some(chosen));
    assert_eq!(active(&f.store).await, Some(chosen));
    let last = f.store.last_activation(KIND).await.unwrap().unwrap();
    assert_eq!(last.from(), Some(first));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restart_keeps_the_deadline() {
    let mut f = fixture();
    let exec = f.waiting().await;
    f.advance(6 * 24 * HOUR_MS);
    f.restart();
    assert!(blocked(&f.drive(exec).await));
    f.advance(24 * HOUR_MS + 1_000);
    let report = report_of(f.drive(exec).await);
    assert_eq!(report.status, RefineStatus::TimedOut, "{report:?}");
    let events = f.rt.load_history(exec).unwrap();
    replay(f.harness.clone(), events, exec).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pointer_move_during_the_loop_makes_approve_superseded() {
    let mut f = fixture();
    *f.proposer.meddle.lock().unwrap() = Some(Arc::clone(&f.store));
    let exec = f.waiting().await;
    let versions = stored_versions(&f.store).await;
    let operator = versions
        .iter()
        .find(|(_, provenance, _)| provenance == &Provenance::operator("ops"))
        .map(|(version, _, _)| *version);
    assert!(operator.is_some(), "{versions:?}");
    f.decide(exec, &RefineDecision::Approve);
    let report = report_of(f.drive(exec).await);
    assert_eq!(report.status, RefineStatus::Superseded, "{report:?}");
    let gate = report.gate.expect("a gate record");
    assert_eq!(gate.expected, None, "nothing was active at refine start");
    assert_eq!(gate.superseded_by, operator);
    assert_eq!(active(&f.store).await, operator);
}
