//! An agent session as a durable [`Agent`] (issues #1975 and #2263).
//!
//! Each session is one agent: the agent id is `session/<name>`. A user
//! message is one op. The op runs one segment of the agent loop with the whole
//! harness: tools, policy, approvals, budgets, memory, the loop guard and
//! delivery. See `DESIGN-2262.md`.
//!
//! ## Why an agent
//!
//! - Two messages to one session never run at the same time. The second
//!   message waits until the first reply is in the transcript.
//! - A worker crash loses no turn. Replay rebuilds the transcript, and a
//!   recorded model reply is not requested again.
//! - The transcript has a bound: [`AgentProfile::window`]. The agent drops the
//!   oldest turns first, and it cuts only at a user message.
//! - The history of a long session has a bound. The agent takes a checkpoint
//!   and carries its state forward.
//!
//! ## Run it
//!
//! ```sh
//! cargo run -p autumn-harvest-agent --features sqlite --example agent_session_entity
//! ```
//!
//! ## Send a message on Postgres
//!
//! Register `workflows()`, `activities()` and the harness, as in
//! `docs/agent-adapter.md`. Then send a signal-with-start:
//!
//! ```console
//! $ curl -X POST $HARVEST/workflows/agent/signal-with-start \
//!     -H 'content-type: application/json' \
//!     -d '{"workflow_id":"session/42","start_input":{},
//!          "signal_name":"harvest.entity.op",
//!          "signal_payload":{"kind":"op","op":{"type":"user_message","text":"hi"}},
//!          "idempotency_key":"msg-1"}'
//! $ curl $HARVEST/workflows/by-id/agent/session%2F42/query/harvest.entity.state
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use autumn_harvest_agent::{
    Agent, AgentError, AgentHarness, AgentId, AgentModel, AgentOp, AgentProfile, BoxFuture,
    ChatRequest, ChatResponse, ChatRole, ContentPart, StopReason, TokenUsage, sqlite,
};
use autumn_harvest_sqlite::SqliteRuntime;

/// The bound on the transcript of a session, in messages.
pub const WINDOW: u32 = 20;

/// A stand-in model. It answers from the request alone, so the example needs
/// no key and no network.
#[derive(Debug, Default)]
pub struct EchoModel {
    calls: AtomicUsize,
}

impl EchoModel {
    /// The model calls so far.
    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl AgentModel for EchoModel {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let last = request
            .messages
            .iter()
            .rev()
            .find(|message| message.role == ChatRole::User)
            .and_then(|message| message.content.first())
            .and_then(|part| match part {
                ContentPart::Text(text) => Some(text.clone()),
                _ => None,
            })
            .unwrap_or_default();
        let reply = format!("turn {}: you said '{last}'", request.messages.len());
        Box::pin(async move {
            Ok(ChatResponse {
                content: vec![ContentPart::Text(reply)],
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::new(10, 5),
            })
        })
    }
}

/// The harness: one model and the `session` profile.
pub fn harness(model: Arc<EchoModel>) -> AgentHarness {
    AgentHarness::new(model).agent(
        AgentProfile::new("session")
            .system("You are a helpful assistant.")
            .window(WINDOW),
    )
}

/// Open the database and register the agent workflows against `model`.
///
/// # Panics
///
/// Panics when the database does not open.
pub fn open(db: &std::path::Path, model: Arc<EchoModel>) -> SqliteRuntime {
    let mut rt = SqliteRuntime::open(db).expect("the database opens");
    sqlite::register(&mut rt, Arc::new(harness(model)));
    rt
}

/// Send one message to the session and drive it until it waits again.
///
/// # Panics
///
/// Panics when the send or the drive fails.
pub async fn say(rt: &mut SqliteRuntime, session: &AgentId, text: &str, message_id: &str) {
    sqlite::send(
        rt,
        session,
        &AgentOp::message_with_id(text, message_id),
        Some(message_id),
    )
    .expect("the message is staged");
    sqlite::drive(rt, session).await.expect("the agent runs");
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let db = dir.path().join("sessions.db");
    let session = AgentId::new("session", "42").expect("a valid id");

    let model = Arc::new(EchoModel::default());
    let mut rt = open(&db, Arc::clone(&model));
    say(&mut rt, &session, "hello", "m1").await;
    // The process stops here, as a crash would.
    drop(rt);

    let restarted = Arc::new(EchoModel::default());
    let mut rt = open(&db, Arc::clone(&restarted));
    say(&mut rt, &session, "and again", "m2").await;
    let state = sqlite::state(&rt, &session)
        .await
        .expect("the query runs")
        .expect("the session runs");

    println!("agent_session_entity: one durable agent for each session (issue #2263)");
    println!("  agent:   {}", Agent::new(session).id());
    println!("  answer:  {}", state.last_answer.unwrap_or_default());
    println!("  turns:   {}", state.transcript.len());
    println!(
        "  model calls: {} before the restart, {} after it",
        model.calls(),
        restarted.calls()
    );
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    fn session() -> AgentId {
        AgentId::new("session", "t").unwrap()
    }

    /// Each reply lands before the next message starts.
    #[tokio::test(flavor = "multi_thread")]
    async fn messages_run_one_at_a_time_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(EchoModel::default());
        let mut rt = open(&dir.path().join("a.db"), Arc::clone(&model));
        let id = session();
        sqlite::send(&mut rt, &id, &AgentOp::message("one"), None).unwrap();
        sqlite::send(&mut rt, &id, &AgentOp::message("two"), None).unwrap();
        sqlite::drive(&mut rt, &id).await.unwrap();
        let state = sqlite::state(&rt, &id).await.unwrap().unwrap();
        let texts: Vec<String> = state
            .transcript
            .iter()
            .filter_map(|message| match message.content.first() {
                Some(ContentPart::Text(text)) => Some(text.clone()),
                _ => None,
            })
            .collect();
        // The system prompt is message 1. The second turn sees three more.
        assert_eq!(
            texts,
            [
                "one",
                "turn 2: you said 'one'",
                "two",
                "turn 4: you said 'two'"
            ]
        );
    }

    /// A restart replays the recorded turns. It asks the model only for the
    /// new message.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_restart_asks_no_recorded_turn_again() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("b.db");
        let id = session();
        let first = Arc::new(EchoModel::default());
        let mut rt = open(&db, Arc::clone(&first));
        say(&mut rt, &id, "one", "m1").await;
        drop(rt);
        let second = Arc::new(EchoModel::default());
        let mut rt = open(&db, Arc::clone(&second));
        say(&mut rt, &id, "two", "m2").await;
        // A client retry of the same message id delivers nothing new.
        say(&mut rt, &id, "two", "m2").await;
        assert_eq!((first.calls(), second.calls()), (1, 1));
        let state = sqlite::state(&rt, &id).await.unwrap().unwrap();
        assert_eq!(state.transcript.len(), 4);
    }

    /// The transcript keeps at most `WINDOW` messages.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_transcript_has_a_bound() {
        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(EchoModel::default());
        let mut rt = open(&dir.path().join("c.db"), model);
        let id = session();
        for n in 0..15 {
            sqlite::send(&mut rt, &id, &AgentOp::message(format!("m{n}")), None).unwrap();
        }
        sqlite::drive(&mut rt, &id).await.unwrap();
        let state = sqlite::state(&rt, &id).await.unwrap().unwrap();
        assert_eq!(state.transcript.len(), usize::try_from(WINDOW).unwrap());
        assert_eq!(state.transcript[0].role, ChatRole::User);
    }
}
