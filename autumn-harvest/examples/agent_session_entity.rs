#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! An agent session as a keyed entity (issue #1975).
//!
//! Each session id is one entity. A user message is one operation. The
//! handler calls the model as an activity and appends both turns to the
//! transcript. See `docs/adr/0006-keyed-entity.md`.
//!
//! ## Why an entity
//!
//! - Two messages to one session never run at the same time. The second
//!   message waits until the first reply is in the transcript.
//! - A worker crash loses no turn. Replay rebuilds the transcript, and a
//!   recorded model reply is not bought again.
//! - A long session does not grow its history without a bound. The loop
//!   takes a checkpoint and carries the transcript forward.
//!
//! ## Send a message
//!
//! ```rust,ignore
//! AgentSessionStub::signal_with_start(
//!     conn, &client, "session-42", EntityCheckpoint::<Session>::default(),
//!     ENTITY_OP_SIGNAL,
//!     EntityMessage::op(SessionOp::UserMessage { text: "hi".into() }),
//!     TypedSignalWithStartOptions {
//!         idempotency_key: Some(message_id),
//!         ..Default::default()
//!     },
//! ).await?;
//! ```
//!
//! Over HTTP, use the signal-with-start route and the state query:
//!
//! ```console
//! $ curl -X POST $HARVEST/workflows/agent_session/signal-with-start \
//!     -d '{"workflow_id":"session-42","start_input":{},
//!          "signal_name":"harvest.entity.op",
//!          "signal_payload":{"kind":"op","op":{"user_message":{"text":"hi"}}},
//!          "idempotency_key":"msg-1"}'
//! $ curl $HARVEST/workflows/by-id/agent_session/session-42/query/harvest.entity.state
//! ```
//!
//! Run the embedded tests with:
//!
//! ```bash
//! cargo test -p autumn-harvest --features testing --example agent_session_entity
//! ```

use autumn_harvest::entity::{Entity, EntityCheckpoint};
use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

/// One turn of the transcript.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Turn {
    pub role: String,
    pub text: String,
}

/// The durable state of one session.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Session {
    pub turns: Vec<Turn>,
}

/// An operation on a session.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionOp {
    /// The user sends a message. The model answers it.
    UserMessage { text: String },
    /// Forget the transcript and keep the session.
    Clear,
}

/// One model call. A real build calls the Messages API here.
///
/// The stub answers from the transcript only, so the example needs no key.
#[activity(start_to_close = "60s")]
async fn agent_reply(_ctx: &ActivityContext, transcript: Vec<Turn>) -> Result<String, String> {
    let last = transcript.last().map(|t| t.text.as_str()).unwrap_or("");
    Ok(format!("turn {}: you said '{last}'", transcript.len()))
}

/// The session entity. The `workflow_id` is the session id.
#[workflow]
async fn agent_session(
    ctx: &WorkflowContext,
    input: EntityCheckpoint<Session>,
) -> Result<Session, String> {
    Entity::new(ctx, input)
        .run(|mut session: Session, op: SessionOp| async move {
            match op {
                SessionOp::UserMessage { text } => {
                    session.turns.push(Turn {
                        role: "user".into(),
                        text,
                    });
                    let reply: String = ctx
                        .execute_activity(&agent_reply_info(), session.turns.clone())
                        .await
                        .map_err(|e| e.to_string())?;
                    session.turns.push(Turn {
                        role: "assistant".into(),
                        text: reply,
                    });
                }
                SessionOp::Clear => session.turns.clear(),
            }
            Ok(session)
        })
        .await
        .map_err(|e| e.to_string())
}

fn main() {
    let _registration = HarvestBuilder::new()
        .workflows(workflows![agent_session])
        .activities(activities![agent_reply])
        .try_build()
        .expect("example registration should be valid");
    println!("agent_session_entity: one entity for each agent session (issue #1975)");
    println!("  - messages to one session run one at a time");
    println!("  - replay rebuilds the transcript after a crash");
    println!("  - a checkpoint carries the transcript across continue-as-new");
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;
    use autumn_harvest::entity::{ENTITY_OP_SIGNAL, EntityMessage};
    use autumn_harvest::event::WorkflowEvent;
    use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer, WorkflowTestEnv};
    use serde_json::{Value, json};

    fn say(text: &str) -> Value {
        json!(EntityMessage::op(SessionOp::UserMessage {
            text: text.into()
        }))
    }

    fn end() -> Value {
        json!(EntityMessage::<SessionOp>::delete())
    }

    fn env(messages: &[Value]) -> WorkflowTestEnv {
        messages
            .iter()
            .fold(WorkflowTestEnv::new(), |env, m| {
                env.queue_signal(ENTITY_OP_SIGNAL, m.clone())
            })
            .mock_activity("agent_reply", |input| {
                let turns: Vec<Turn> = serde_json::from_value(input).map_err(|e| e.to_string())?;
                Ok(json!(format!("reply to {}", turns.last().unwrap().text)))
            })
    }

    fn turns(session: &Value) -> Vec<(String, String)> {
        let session: Session = serde_json::from_value(session.clone()).unwrap();
        session
            .turns
            .into_iter()
            .map(|t| (t.role, t.text))
            .collect()
    }

    /// Each reply lands before the next message starts.
    #[tokio::test]
    async fn messages_run_one_at_a_time_in_order() {
        let outcome = env(&[say("one"), say("two"), end()])
            .run(agent_session_info().handler, json!({}))
            .await;
        let session = outcome.result.clone().expect("session ends at the delete");
        assert_eq!(
            turns(&session),
            vec![
                ("user".into(), "one".into()),
                ("assistant".into(), "reply to one".into()),
                ("user".into(), "two".into()),
                ("assistant".into(), "reply to two".into()),
            ]
        );
        let inputs: Vec<usize> = outcome
            .events()
            .iter()
            .filter_map(|e| match e {
                WorkflowEvent::ActivityScheduled { input, .. } => input.as_array().map(Vec::len),
                _ => None,
            })
            .collect();
        assert_eq!(
            inputs,
            vec![1, 3],
            "the second call sees the first reply in its transcript"
        );
    }

    /// A crash is a replay of the recorded history. The replay buys no new
    /// model call and rebuilds the same transcript.
    #[tokio::test]
    async fn a_replay_after_a_crash_rebuilds_the_session() {
        let outcome = env(&[say("one"), say("two"), end()])
            .run(agent_session_info().handler, json!({}))
            .await;
        let report = WorkflowReplayer::new()
            .register(vec![agent_session_info()])
            .replay_from_events(outcome.events().to_vec())
            .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "{report}"
        );
    }
}
