//! A durable agent loop for autumn-harvest.
//!
//! This crate adapts [`autumn_plugin_agent`] to the engine. The agent loop is a
//! workflow. Each model call and each tool call is an activity. A tool call
//! that the policy gates waits on a durable signal with a deadline.
//!
//! A crash costs at most the one step that was in flight. Replay reads every
//! completed model call, policy decision and tool call from history.
//!
//! The crate depends on the core engine with no default features and on
//! plugin-agent with no default features. It has no `autumn-web` dependency.
//!
//! # Use it on SQLite
//!
//! ```no_run
//! # async fn demo(client: std::sync::Arc<dyn autumn_plugin_agent::LlmClient>)
//! # -> Result<(), autumn_harvest_sqlite::SqliteError> {
//! use std::sync::Arc;
//! use autumn_harvest_agent::{AgentHarness, AgentTask, sqlite};
//! use autumn_harvest_sqlite::{RunState, SqliteRuntime};
//!
//! let mut rt = SqliteRuntime::open("agent.db")?;
//! sqlite::register(&mut rt, Arc::new(AgentHarness::new(client)));
//! let exec = sqlite::start(&mut rt, &AgentTask::new("Summarise the README"))?;
//! if let RunState::Completed(report) = rt.run_until_blocked(exec).await? {
//!     println!("{report}");
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Use it on Postgres
//!
//! Register [`agent_loop`](workflow::agent_loop) and the two activities, and
//! install the harness as worker state:
//!
//! ```ignore
//! HarvestBuilder::new()
//!     .workflows(workflows![agent_loop])
//!     .activities(activities![agent_model_turn, agent_tool_call])
//!     .state(AgentHarness::new(client).tool(tool))
//! ```
//!
//! See `docs/agent-adapter.md` for the whole pattern.

pub mod approval;
pub mod bounds;
pub mod harness;
#[cfg(feature = "sqlite")]
pub mod sqlite;
pub mod types;
pub mod workflow;

pub use harness::AgentHarness;
pub use types::{
    AgentReport, AgentStop, AgentTask, GatedCall, ModelTurn, ModelTurnRequest, ToolCallRequest,
    ToolOutcome, TurnStop,
};
pub use workflow::{WORKFLOW_NAME, agent_loop, agent_model_turn, agent_tool_call};
