//! The desk end to end on SQLite, with the offline model.
//!
//! A test drops the runtime between steps, as a process restart does. The
//! next runtime opens the same file and goes on.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::{Arc, Mutex};

use agent_mesh::desk::Desk;
use agent_mesh::mesh::{self, DeskDelivery, MeshConfig};
use agent_mesh::scripted::{ScriptedDesk, TOKENS_PER_CALL};
use autumn_harvest::chrono::{DateTime, Duration, Utc};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest_agent::{AgentStatus, Approval, LiveEvent, LiveSink, PeerEnvelope, sqlite};
use autumn_harvest_sqlite::SqliteRuntime;

/// Keeps every live event.
#[derive(Debug, Default)]
struct Recorder(Mutex<Vec<LiveEvent>>);

impl LiveSink for Recorder {
    fn send(&self, event: &LiveEvent) {
        self.0.lock().unwrap().push(event.clone());
    }
}

impl Recorder {
    fn text(&self) -> String {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter_map(|event| match event {
                LiveEvent::Delta { text, .. } => Some(text.as_str()),
                LiveEvent::Start { .. } => None,
            })
            .collect()
    }
}

fn runtime(desk: &Desk, config: &MeshConfig, live: &Arc<Recorder>) -> SqliteRuntime {
    let harness = mesh::harness(
        desk,
        Arc::new(ScriptedDesk::default()),
        config,
        Some(live.clone()),
        Arc::new(DeskDelivery::new(desk.clone(), false)),
    );
    mesh::open(desk, harness).unwrap()
}

fn fresh() -> (tempfile::TempDir, Desk) {
    let dir = tempfile::tempdir().unwrap();
    let desk = Desk::new(dir.path().join("desk"));
    desk.init().unwrap();
    (dir, desk)
}

/// The peer messages that `agent` received.
fn peer_messages(rt: &SqliteRuntime, agent: &autumn_harvest_agent::AgentId) -> Vec<PeerEnvelope> {
    let filter = autumn_harvest_sqlite::ExecutionFilter::new()
        .workflow_name(autumn_harvest_agent::AGENT_WORKFLOW_NAME)
        .workflow_id(agent.as_str());
    let mut out = Vec::new();
    for summary in rt.list_executions(&filter).unwrap().executions {
        let Some(exec) = summary.execution_id() else {
            continue;
        };
        for event in rt.load_history(exec).unwrap() {
            if let WorkflowEvent::SignalReceived { payload, .. } = event
                && payload["op"]["type"] == "peer_message"
            {
                out.push(serde_json::from_value(payload["op"].clone()).unwrap());
            }
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn a_session_checks_the_inbox_answers_a_ticket_and_waits_for_approval() {
    let (_dir, desk) = fresh();
    let config = MeshConfig::default();
    let live = Arc::new(Recorder::default());
    let t0: DateTime<Utc> = Utc::now();

    // Start the mesh, then let the heartbeat of the triage agent fire.
    let mut rt = runtime(&desk, &config, &live);
    mesh::start(&mut rt, t0).await.unwrap();
    let t1 = t0 + Duration::minutes(16);
    mesh::settle(&mut rt, t1).await.unwrap();
    let reports = desk.reports();
    assert!(
        reports.iter().any(|line| line.contains("2 open tickets")),
        "the check reported the inbox: {reports:?}"
    );
    assert!(desk.filed_log().is_empty(), "a check files nothing");
    drop(rt);

    // A person asks for an answer. The process stops at the approval.
    let mut rt = runtime(&desk, &config, &live);
    let ticket = desk.ticket("T-1").unwrap().unwrap();
    mesh::ask_answer(&mut rt, &ticket, "answer:T-1").unwrap();
    let settled = mesh::settle(&mut rt, t1).await.unwrap();
    let (exec, signal) = settled.approval.expect("the reply waits for approval");
    assert!(
        desk.reply("T-1").is_none(),
        "nothing is filed before approval"
    );
    drop(rt);

    // A new process approves. The reply is filed once.
    let mut rt = runtime(&desk, &config, &live);
    mesh::decide(&mut rt, exec, &signal, &Approval::Approve).unwrap();
    let settled = mesh::settle(&mut rt, t1).await.unwrap();
    assert_eq!(settled.approval, None);
    assert_eq!(desk.filed_log(), ["T-1"], "one run of file_reply");
    let reply = desk.reply("T-1").unwrap();
    assert!(reply.contains("export.max_rows"), "{reply}");

    let triage = sqlite::state(&rt, &mesh::triage()).await.unwrap().unwrap();
    assert!(
        triage
            .last_answer
            .as_deref()
            .unwrap_or_default()
            .contains("I filed the reply to T-1"),
        "{:?}",
        triage.last_answer
    );
    // The answer streamed as live text.
    assert!(
        live.text().contains("I filed the reply to T-1"),
        "{}",
        live.text()
    );

    // Each peer got its message once, under a narrower mandate.
    for peer in [mesh::researcher(), mesh::critic()] {
        let received = peer_messages(&rt, &peer);
        assert_eq!(received.len(), 1, "{peer}: {received:?}");
        let mandate = &received[0].mandate;
        assert!(!mandate.allows_tool("file_reply"), "{peer}: {mandate:?}");
    }

    // The ledger holds the spend of the whole mesh.
    let spent = mesh::spent_today(&desk, t1).await.unwrap();
    assert!(spent.tokens >= u64::from(TOKENS_PER_CALL) * 8, "{spent:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rejected_reply_is_never_filed() {
    let (_dir, desk) = fresh();
    let config = MeshConfig::default();
    let live = Arc::new(Recorder::default());
    let t0 = Utc::now();
    let mut rt = runtime(&desk, &config, &live);
    mesh::start(&mut rt, t0).await.unwrap();
    let ticket = desk.ticket("T-2").unwrap().unwrap();
    mesh::ask_answer(&mut rt, &ticket, "answer:T-2").unwrap();
    let (exec, signal) = mesh::settle(&mut rt, t0).await.unwrap().approval.unwrap();
    let reject = Approval::Reject {
        reason: "Wait for the security team.".into(),
    };
    mesh::decide(&mut rt, exec, &signal, &reject).unwrap();
    mesh::settle(&mut rt, t0).await.unwrap();
    assert_eq!(
        desk.filed_log(),
        Vec::<String>::new(),
        "no run of file_reply"
    );
    assert!(desk.reply("T-2").is_none());
    let triage = sqlite::state(&rt, &mesh::triage()).await.unwrap().unwrap();
    assert!(
        triage
            .last_answer
            .unwrap_or_default()
            .contains("was not filed"),
        "the agent read the rejection"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_spent_budget_parks_the_mesh_until_the_next_day() {
    let (_dir, desk) = fresh();
    // A check takes two model turns. An answer takes six turns up to its
    // approval, so a cap of six turns holds one of them in a day. Three
    // checks spend the first day.
    let config = MeshConfig {
        daily_tokens: Some(u64::from(TOKENS_PER_CALL) * 6),
        ..MeshConfig::default()
    };
    let live = Arc::new(Recorder::default());
    let t0: DateTime<Utc> = "2026-10-23T08:00:00Z".parse().unwrap();
    let mut rt = runtime(&desk, &config, &live);
    mesh::start(&mut rt, t0).await.unwrap();
    let mut t1 = t0;
    for _ in 0..3 {
        t1 += Duration::minutes(16);
        mesh::settle(&mut rt, t1).await.unwrap();
    }
    assert_eq!(desk.reports().len(), 3, "{:?}", desk.reports());
    let ticket = desk.ticket("T-1").unwrap().unwrap();
    mesh::ask_answer(&mut rt, &ticket, "answer:T-1").unwrap();
    mesh::settle(&mut rt, t1).await.unwrap();
    let triage = sqlite::state(&rt, &mesh::triage()).await.unwrap().unwrap();
    assert!(
        matches!(triage.status, AgentStatus::Parked { .. }),
        "{:?}",
        triage.status
    );
    let spent = mesh::spent_today(&desk, t1).await.unwrap();
    // The check runs before each turn, so the mesh passes the cap by one
    // turn at most.
    assert!(spent.tokens <= u64::from(TOKENS_PER_CALL) * 7, "{spent:?}");

    // The next day opens the budget. The held message runs then.
    let t2: DateTime<Utc> = "2026-10-24T00:01:00Z".parse().unwrap();
    let settled = mesh::settle(&mut rt, t2).await.unwrap();
    assert!(
        settled.approval.is_some(),
        "the answer runs on the next day"
    );
}
