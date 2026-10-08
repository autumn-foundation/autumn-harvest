//! A durable agent loop for autumn-harvest.
//!
//! The agent loop is a workflow. Each model call and each tool call is an
//! activity. A tool call that the policy gates waits on a durable signal with
//! a deadline. A crash costs at most the one step that was in flight: replay
//! reads every completed model call, policy decision and tool call from
//! history.
//!
//! The crate owns its agent primitives: [`AgentModel`], [`Tool`],
//! [`ToolPolicy`], [`Approval`] and the message types. It also has the
//! always-on primitives: [`heartbeat`], [`followup`], [`delivery`],
//! [`memory`] and [`loop_guard`]. It depends on the core
//! engine only, with no default features. It has no Autumn plugin dependency.
//! An app implements [`AgentModel`] for its provider, or bridges a framework
//! it already uses.
//!
//! # Use it on SQLite
//!
//! ```no_run
//! # #[cfg(feature = "sqlite")]
//! # async fn demo(model: std::sync::Arc<dyn autumn_harvest_agent::AgentModel>)
//! # -> Result<(), autumn_harvest_sqlite::SqliteError> {
//! use std::sync::Arc;
//! use autumn_harvest_agent::{AgentHarness, AgentTask, sqlite};
//! use autumn_harvest_sqlite::{RunState, SqliteRuntime};
//!
//! let mut rt = SqliteRuntime::open("agent.db")?;
//! sqlite::register(&mut rt, Arc::new(AgentHarness::new(model)));
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
//! Register the workflows and the activities, and install the harness as
//! worker state:
//!
//! ```
//! # fn demo(model: std::sync::Arc<dyn autumn_harvest_agent::AgentModel>) {
//! use autumn_harvest::builder::HarvestBuilder;
//! use autumn_harvest_agent::{AgentHarness, activities, workflows};
//!
//! let built = HarvestBuilder::new()
//!     .workflows(workflows())
//!     .activities(activities())
//!     .state(AgentHarness::new(model))
//!     .build();
//! assert!(built.state::<AgentHarness>().is_some());
//! # }
//! ```
//!
//! See `docs/agent-adapter.md` for the whole pattern.

pub mod approval;
pub mod bounds;
pub mod delivery;
pub mod error;
pub mod followup;
pub mod harness;
pub mod heartbeat;
pub mod loop_guard;
pub mod memory;
pub mod message;
pub mod model;
pub mod policy;
#[cfg(feature = "sqlite")]
pub mod sqlite;
pub mod tool;
pub mod types;
pub mod workflow;

pub use approval::Approval;
pub use error::{AgentError, ErrorKind};
pub use harness::AgentHarness;
pub use message::{
    ChatMessage, ChatRole, ContentPart, RunId, SessionId, StopReason, TokenUsage, ToolCall,
    ToolDefinition,
};
pub use model::{AgentModel, BoxFuture, ChatRequest, ChatResponse};
pub use policy::{AllowAll, Rule, RunInfo, Strictest, ToolDecision, ToolPolicy, ToolRules};
pub use tool::{FnTool, Tool, ToolContext, ToolEffect};
pub use types::{
    AgentReport, AgentStop, AgentTask, ModelTurn, ModelTurnRequest, ToolCallRequest, ToolOutcome,
};
pub use workflow::{
    WORKFLOW_NAME, activities, agent_deliver, agent_deliver_info, agent_loop, agent_loop_info,
    agent_memory_snapshot, agent_memory_snapshot_info, agent_model_turn, agent_model_turn_info,
    agent_precheck, agent_precheck_info, agent_tool_call, agent_tool_call_info, workflows,
};
