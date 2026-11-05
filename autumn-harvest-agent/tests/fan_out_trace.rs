//! The `debug` event of a delegation that ran alone (issue #2483).
//!
//! The event needs a global subscriber, because the SQLite runtime runs on
//! worker threads. A global subscriber also sees the events of each other
//! test in the binary, so this test has a binary of its own.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::sync::{Arc, Mutex};

use autumn_harvest_agent::mesh::DELEGATE_TOOL;
use autumn_harvest_agent::{
    AgentHarness, AgentTask, Approval, Mandate, Peer, Rule, ToolRules, sqlite,
};
use autumn_harvest_sqlite::RunState;
use common::{ScriptedModel, answer, calls, report, runtime};
use serde_json::json;

/// A writer that keeps each line of the subscriber.
#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Logs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Logs {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_reason_logs_once_across_replays() {
    let logs = Logs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    // Three gated delegations in one turn. Each approval resumes the run,
    // and each resume replays the calls before it.
    let delegate =
        |id: &'static str, agent: &str| (id, DELEGATE_TOOL, json!({"agent": agent, "task": "go"}));
    let model = ScriptedModel::new(vec![
        calls(
            &[
                delegate("d1", "flights"),
                delegate("d2", "hotels"),
                delegate("d3", "visas"),
            ],
            1,
        ),
        answer("flights found", 1),
        answer("hotels found", 1),
        answer("visas checked", 1),
        answer("trip planned", 1),
    ]);
    let harness = AgentHarness::new(model)
        .peer(Peer::new("flights", "Finds flights."))
        .peer(Peer::new("hotels", "Finds hotels."))
        .peer(Peer::new("visas", "Checks visas."))
        .policy(Arc::new(ToolRules::new().tool(DELEGATE_TOOL, Rule::Ask)));
    let dir = tempfile::tempdir().unwrap();
    let mut rt = runtime(&dir.path().join("agent.db"), harness);
    let task = AgentTask::new("plan the trip")
        .mandate(Mandate::new("planner").peers(["flights", "hotels", "visas"]))
        .parallel_children(3);
    let exec = sqlite::start(&mut rt, &task).unwrap();
    let mut state = rt.run_until_blocked(exec).await.unwrap();
    let mut resumes = 0;
    while let RunState::WaitingSignal(signal) = state {
        sqlite::decide(&mut rt, exec, &signal, &Approval::Approve).unwrap();
        state = rt.run_until_blocked(exec).await.unwrap();
        resumes += 1;
    }
    let report = report(state);
    assert_eq!(report.text, "trip planned");
    assert_eq!(resumes, 3, "each call waited for a person");

    let text = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    let fired: Vec<&str> = text
        .lines()
        .filter(|line| line.contains("a delegation ran alone"))
        .collect();
    assert_eq!(fired.len(), 3, "{text}");
    for id in ["d1", "d2", "d3"] {
        let once = fired
            .iter()
            .filter(|line| {
                line.contains(&format!("call_id={id}")) && line.contains("reason=\"gated\"")
            })
            .count();
        assert_eq!(once, 1, "{id}: {text}");
    }
}
