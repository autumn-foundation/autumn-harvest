//! A live probe on SQLite and on the tool-call activity (issue #2378).
//!
//! SQLite has no fork, so `eval::live_probe` refuses it. The refusal comes
//! before any write, so the source history stays the same.
//!
//! The other tests start a run with a probe mark. They check the gate of
//! the tool-call activity, the delivery, the approval notice and the
//! durable peer refusal.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use autumn_harvest_agent::eval::{EvalError, live_probe};
use autumn_harvest_agent::mesh::DELEGATE_TOOL;
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentId, AgentTask, BoxFuture, ChatRequest, Delivery, Mandate,
    PROBE_EFFECT_REFUSED, Peer, Probe, ProfileOverlay, Report, ToolCall, ToolCallRequest,
    ToolEffect, sqlite,
};
use common::{Recorder, ScriptedModel, TryFnModel, answer, calls, recorded_tool, report, runtime};
use serde_json::{Value, json};

#[tokio::test(flavor = "multi_thread")]
async fn a_live_probe_on_sqlite_is_unsupported() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let model = ScriptedModel::new(vec![answer("done", 2)]);
    let harness = AgentHarness::new(model.clone());
    let mut rt = runtime(&dir.path().join("agent.db"), harness.clone());
    let exec = sqlite::start(&mut rt, &AgentTask::new("Check the account.")).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    let before = serde_json::to_value(rt.load_history(exec).unwrap()).unwrap();

    let refused = live_probe(
        &rt,
        &harness,
        exec,
        &ProfileOverlay::default(),
        &[ToolEffect::ReadOnly],
    )
    .await;

    assert!(
        matches!(refused, Err(EvalError::Unsupported(_))),
        "got {refused:?}"
    );
    assert_eq!(
        serde_json::to_value(rt.load_history(exec).unwrap()).unwrap(),
        before
    );
    assert_eq!(model.calls(), 1, "the probe asked no model");
}

fn request(name: &str, read_only: bool, probe: Option<&[ToolEffect]>) -> ToolCallRequest {
    ToolCallRequest {
        run_id: "probe-1".to_owned(),
        session_id: None,
        step: 0,
        call: ToolCall {
            id: "c1".to_owned(),
            name: name.to_owned(),
            arguments: json!({}),
        },
        memory_scope: None,
        read_only,
        allowed_tools: None,
        labels: None,
        flow_labels: BTreeSet::new(),
        workspace: None,
        skill: None,
        allowed_effects: None,
        probe_effects: probe.map(|effects| effects.iter().copied().collect()),
    }
}

fn error_code(content: &str) -> Value {
    serde_json::from_str::<Value>(content).unwrap()["error"].clone()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_tool_call_activity_runs_only_the_probe_effects() {
    let writes = Arc::new(Recorder::default());
    let reads = Arc::new(Recorder::default());
    let harness = AgentHarness::new(ScriptedModel::new(Vec::new()))
        .tool(recorded_tool("write", ToolEffect::Write, &writes))
        .tool(recorded_tool("read", ToolEffect::ReadOnly, &reads));
    let read_only = [ToolEffect::ReadOnly];

    let refused = harness
        .tool_call(request("write", false, Some(&read_only)))
        .await
        .unwrap();
    assert!(refused.is_error);
    assert_eq!(error_code(&refused.content), PROBE_EFFECT_REFUSED);
    assert_eq!(writes.runs().len(), 0, "execute never ran");

    // The probe gate goes before the read-only rule, so the code is stable.
    let both = harness
        .tool_call(request("write", true, Some(&read_only)))
        .await
        .unwrap();
    assert_eq!(error_code(&both.content), PROBE_EFFECT_REFUSED);

    let read = harness
        .tool_call(request("read", false, Some(&read_only)))
        .await
        .unwrap();
    assert!(!read.is_error, "{read:?}");
    assert_eq!(reads.runs().len(), 1);

    // An unknown tool keeps its own refusal.
    let unknown = harness
        .tool_call(request("missing", false, Some(&read_only)))
        .await
        .unwrap();
    assert!(unknown.content.contains("unknown tool"), "{unknown:?}");

    // An empty set refuses each tool.
    let none = harness
        .tool_call(request("read", false, Some(&[])))
        .await
        .unwrap();
    assert_eq!(error_code(&none.content), PROBE_EFFECT_REFUSED);
    assert_eq!(reads.runs().len(), 1);

    // A run with no mark runs the tool.
    let plain = harness
        .tool_call(request("write", false, None))
        .await
        .unwrap();
    assert!(!plain.is_error, "{plain:?}");
    assert_eq!(writes.runs().len(), 1);
}

/// A delivery that counts its reports.
#[derive(Debug, Default)]
struct CountingDelivery(AtomicUsize);

impl Delivery for CountingDelivery {
    fn deliver<'a>(&'a self, _report: &'a Report) -> BoxFuture<'a, Result<(), AgentError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

fn mark() -> Probe {
    Probe::new([ToolEffect::ReadOnly])
}

#[tokio::test(flavor = "multi_thread")]
async fn a_probe_run_sends_no_report() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let delivery = Arc::new(CountingDelivery::default());
    let harness = AgentHarness::new(ScriptedModel::new(vec![
        answer("first", 2),
        answer("second", 2),
    ]))
    .delivery(Arc::clone(&delivery) as Arc<dyn Delivery>);
    let mut rt = runtime(&dir.path().join("agent.db"), harness);
    let mut task = AgentTask::new("Check the account.");
    task.deliver = true;

    let plain = sqlite::start(&mut rt, &task).unwrap();
    report(rt.run_until_blocked(plain).await.unwrap());
    assert_eq!(delivery.0.load(Ordering::SeqCst), 1, "a plain run delivers");

    task.probe = Some(mark());
    let probe = sqlite::start(&mut rt, &task).unwrap();
    let probed = report(rt.run_until_blocked(probe).await.unwrap());
    assert_eq!(probed.text, "second");
    assert_eq!(
        delivery.0.load(Ordering::SeqCst),
        1,
        "a probe delivers nothing"
    );
}

/// A delivery that counts approval notices.
#[derive(Debug, Default)]
struct CountingNotices(AtomicUsize);

impl Delivery for CountingNotices {
    fn deliver<'a>(&'a self, _report: &'a Report) -> BoxFuture<'a, Result<(), AgentError>> {
        Box::pin(async { Ok(()) })
    }

    fn notify_approval<'a>(
        &'a self,
        _notice: &'a autumn_harvest_agent::ApprovalNotice,
    ) -> BoxFuture<'a, Result<(), AgentError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_probe_run_sends_no_approval_notice() {
    use autumn_harvest_agent::{Rule, ToolRules};
    use autumn_harvest_sqlite::RunState;

    let dir = tempfile::tempdir().expect("a temporary directory");
    let notices = Arc::new(CountingNotices::default());
    let recorder = Arc::new(Recorder::default());
    let lookup = || calls(&[("c1", "lookup", json!({}))], 2);
    let harness = AgentHarness::new(ScriptedModel::new(vec![lookup(), lookup()]))
        .tool(recorded_tool("lookup", ToolEffect::ReadOnly, &recorder))
        .policy(Arc::new(
            ToolRules::new().effect(ToolEffect::ReadOnly, Rule::Ask),
        ))
        .delivery(Arc::clone(&notices) as Arc<dyn Delivery>);
    let mut rt = runtime(&dir.path().join("agent.db"), harness);
    let mut task = AgentTask::new("Check the account.");

    let plain = sqlite::start(&mut rt, &task).unwrap();
    let parked = rt.run_until_blocked(plain).await.unwrap();
    assert!(matches!(parked, RunState::WaitingSignal(_)), "{parked:?}");
    assert_eq!(notices.0.load(Ordering::SeqCst), 1, "a plain run notifies");

    task.probe = Some(mark());
    let probe = sqlite::start(&mut rt, &task).unwrap();
    let parked = rt.run_until_blocked(probe).await.unwrap();
    assert!(matches!(parked, RunState::WaitingSignal(_)), "{parked:?}");
    assert_eq!(
        notices.0.load(Ordering::SeqCst),
        1,
        "a probe sends no notice"
    );
    assert!(recorder.runs().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_probe_never_delegates_to_a_durable_peer() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let model = TryFnModel::new(|request: &ChatRequest| {
        let answered = request
            .messages
            .iter()
            .flat_map(|message| message.content.iter())
            .any(|part| matches!(part, autumn_harvest_agent::ContentPart::ToolResult { .. }));
        if answered {
            answer("done", 5)
        } else {
            calls(
                &[(
                    "d1",
                    DELEGATE_TOOL,
                    json!({"agent": "desk", "task": "Ask the desk."}),
                )],
                5,
            )
        }
    });
    let desk = AgentId::new("desk", "one").unwrap();
    let harness = AgentHarness::new(model).peer(Peer::agent("desk", "The support desk.", &desk));
    let mut rt = runtime(&dir.path().join("agent.db"), harness);
    let mut task = AgentTask::new("Ask the desk.").mandate(Mandate::new("planner").peers(["desk"]));
    task.probe = Some(mark());

    let exec = sqlite::start(&mut rt, &task).unwrap();
    let probed = report(rt.run_until_blocked(exec).await.unwrap());

    let results = common::tool_results(&probed);
    assert_eq!(results.len(), 1, "{results:?}");
    assert!(
        results[0].contains("may not delegate"),
        "the delegation is refused: {results:?}"
    );
    assert!(
        sqlite::current_run(&rt, &desk).unwrap().is_none(),
        "the desk never woke"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_probe_never_writes_the_memory_of_the_source() {
    let store = Arc::new(autumn_harvest_agent::InMemoryMemoryStore::default());
    let harness = AgentHarness::new(ScriptedModel::new(Vec::new()))
        .memory(Arc::clone(&store) as Arc<dyn autumn_harvest_agent::MemoryStore>);
    let scope = autumn_harvest_agent::MemoryScope::new("user-1");
    let mut call = request(
        "memory",
        false,
        Some(&[ToolEffect::ReadOnly, ToolEffect::Internal]),
    );
    call.memory_scope = Some(scope.clone());
    call.call.arguments = json!({"command": "append", "block": "notes", "text": "probe note"});

    let refused = harness.tool_call(call.clone()).await.unwrap();

    assert_eq!(error_code(&refused.content), PROBE_EFFECT_REFUSED);
    let snapshot = harness.memory_snapshot(scope).await.unwrap();
    assert!(
        !format!("{snapshot:?}").contains("probe note"),
        "the store holds no probe note: {snapshot:?}"
    );
    // The same call with no mark reaches the memory tool.
    call.probe_effects = None;
    let plain = harness.tool_call(call).await.unwrap();
    assert_ne!(
        error_code(&plain.content),
        PROBE_EFFECT_REFUSED,
        "{plain:?}"
    );
}
