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
//! [`memory`] and [`loop_guard`]. [`context_reset`] lets a long run save
//! notes and restart from them. A [`Scorer`] checks a run and returns a
//! recorded [`Verdict`], and [`select`] picks the best draw (issue #2441).
//! [`ResponseCache`] serves an identical model call across runs. The
//! [`draw`] module gives each sample of a run its own identity, so the cache
//! keeps the samples apart. The [`episode`] log records each op, and
//! [`consolidate`] turns the episodes into facts (issue #2454).
//! [`consolidate_all`] starts the passes of many agents from one schedule
//! (issue #2540). The [`recall`] module measures what an agent forgets across ops (issue
//! #2455). The `eval` feature adds the `eval` module. That module re-drives
//! a recorded run with a candidate model or prompt. The
//! `sandbox` feature adds the `sandbox` module. Its `SandboxTool` runs agent
//! code in a sandbox.
//!
//! A durable [`Agent`] (issue #2263) is a named, long-lived actor. It is the
//! agent loop inside a keyed entity: the workflow `agent`, with its
//! [`AgentId`] `<kind>/<name>` as the workflow id. It handles one op at a
//! time from a durable inbox, and it survives a crash. A message can queue,
//! steer the running op, or interrupt it (issue #2265). See section 15 of
//! `docs/agent-adapter.md`.
//!
//! The [`sampling`] module holds the operator sampling policy of each kind
//! (issue #2439). [`sampling::agent_draws`] reads it once, then runs the
//! draws of a strategy. [`sampling::agent_sample`] also scores the draws
//! and selects one (issue #2443).
//!
//! The [`mesh`] module lets a run delegate a task to a [`Peer`]. A
//! [`Mandate`] bounds the delegation tree. The [`flow`] module gates a
//! dangerous sequence of tool calls with [`FlowRules`].
//!
//! The crate depends on the core engine only, with no default features. It
//! has no Autumn plugin dependency.
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
//! The `postgres` feature adds the `postgres` module. It has the calls of
//! the `sqlite` module, for an app whose workers run on Postgres.
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

pub mod agent;
pub mod answer_check;
pub mod approval;
pub mod bounds;
pub mod consolidate;
pub mod consolidate_all;
pub mod context_reset;
pub mod delivery;
pub mod draw;
pub mod episode;
pub mod error;
#[cfg(feature = "eval")]
pub mod eval;
mod fan_out_plan;
pub mod flow;
pub mod followup;
pub mod harness;
pub mod heartbeat;
pub mod live;
pub mod loop_guard;
pub mod memory;
pub mod mesh;
pub mod mesh_control;
pub mod message;
pub mod model;
pub mod policy;
#[cfg(feature = "postgres")]
pub mod postgres;
pub mod recall;
pub mod refine;
pub mod response_cache;
pub mod sampling;
#[cfg(feature = "sandbox")]
pub mod sandbox;
pub mod score;
pub mod skill;
#[cfg(feature = "sqlite")]
pub mod sqlite;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub mod stores;
pub mod tool;
pub mod transcript;
pub mod types;
pub mod workflow;
pub mod workspace;

pub use agent::overlay::{
    Ceilings, GUARDRAILS_HEADING, ModelRegistry, OverlayError, ProfileOverlay, ToolRegistry,
};
pub use agent::overlay_diff::{OverlayChange, diff as overlay_diff};
#[cfg(feature = "postgres")]
pub use agent::overlay_store::PostgresOverlayStore;
#[cfg(feature = "sqlite")]
pub use agent::overlay_store::SqliteOverlayStore;
pub use agent::overlay_store::{
    Activation, AutoAdoptOutcome, InMemoryOverlayStore, MAX_AUTHOR_LEN, OverlayStoreError,
    OverlayValidator, OverlayVersion, PolicyLogEntry, PolicyState, ProfileOverlayStore, Provenance,
    Rollback, StoreClock, StoredOverlay,
};
pub use agent::profile_read::{OverlayRead, ProfileRead};
pub use agent::{
    AGENT_WORKFLOW_NAME, Agent, AgentHeartbeat, AgentId, AgentIdError, AgentMemory, AgentOp,
    AgentProfile, AgentState, AgentStatus, Control, HeartbeatSetting, INTERRUPT_SIGNAL, Interrupt,
    PROFILE_VERSION_ATTR, PeerEnvelope, PeerReply, STEER_SIGNAL, Steer, Urgency, UserMessage,
    agent, agent_info, agent_memory_erase, agent_memory_erase_info, agent_memory_reset,
    agent_memory_reset_info, agent_profile, agent_profile_info, agent_profile_overlay,
    agent_profile_overlay_info, agent_transcript_erase, agent_transcript_erase_info,
    agent_workspace_floor, agent_workspace_floor_info, agent_workspace_reset,
    agent_workspace_reset_info, agent_workspace_retain, agent_workspace_retain_info,
};
pub use answer_check::{
    AnswerCheck, CheckAdmission, CheckRequest, agent_answer_check, agent_answer_check_info,
};
pub use approval::Approval;
pub use consolidate::{
    CONSOLIDATE_WORKFLOW_NAME, ConsolidateTask, ConsolidationReport, ConsolidationStop,
    agent_consolidate, agent_consolidate_info,
};
pub use consolidate_all::{
    AgentDirectory, CONSOLIDATE_ALL_WORKFLOW_NAME, ConsolidateAll, DispatchReport, LastPass,
    agent_consolidate_all, agent_consolidate_all_info,
};
pub use context_reset::{ContextReset, ProgressNotes};
pub use delivery::{ApprovalNotice, Delivery, LogDelivery, ParkedCall, Report, ReportSource};
pub use draw::{Draw, DrawId, run_draws};
pub use episode::{
    Appended, Cleared, ConsolidationTrigger, Episode, EpisodeLog, EpisodeReset, EpisodeSettings,
    EpisodeStore, InMemoryEpisodeStore, PassMark,
};
pub use error::{AgentError, ErrorKind};
pub use flow::{FlowRule, FlowRules, Recheck, Released};
pub use followup::Followups;
pub use harness::{AgentHarness, DEFAULT_MODEL, DEFAULT_SCORE_TIMEOUT};
pub use heartbeat::{
    HEARTBEAT_WORKFLOW_NAME, HeartbeatReport, HeartbeatTask, Precheck, agent_heartbeat,
    agent_heartbeat_info,
};
pub use live::{LiveEvent, LiveSink, TurnText};
pub use loop_guard::LoopGuard;
pub use memory::{EditKey, InMemoryMemoryStore, MemoryBlock, MemoryScope, MemoryStore};
pub use mesh::{DelegationRecord, MANDATE_META_KEY, Mandate, Peer, Refusal, SerialReason};
pub use mesh_control::{BudgetPeriod, MeshBudget, MeshPause, MeshSpend, Spend};
pub use message::{
    ChatMessage, ChatRole, ContentPart, RunId, SessionId, StopReason, TokenUsage, ToolCall,
    ToolDefinition,
};
pub use model::{AgentModel, BoxFuture, ChatRequest, ChatResponse};
pub use policy::{AllowAll, Rule, RunInfo, Strictest, ToolDecision, ToolPolicy, ToolRules};
pub use refine::{
    AdoptDecision, Adoption, ApprovalTimeout, AutoAdopt, DailyCap, GateState, OverlayPart,
    PolicyCeilings, RefinePolicy, RefineStatus, WaitReason, adopt, agent_refine_policy,
    agent_refine_policy_info, read_refine_policy,
};
pub use refine::{
    BundleFile, BundleManifest, DEFAULT_PROPOSAL_ATTEMPTS, DEFAULT_PROPOSER_OUTPUT_TOKENS,
    DEFAULT_PROPOSER_TIMEOUT, Dimension, Evidence, KEEP_BASE, MAX_PROPOSAL_ATTEMPTS, ModelProposer,
    OUTPUT_CONTRACT, OverlayProposer, ProposalAttempts, ProposalAttemptsError, ProposeContext,
    ProposeRequest, ProposerReply, Stage, StageOutcome, agent_refine_propose,
    agent_refine_propose_info,
};
pub use response_cache::{
    CacheIndex, CacheKey, CacheScope, InMemoryCacheIndex, KeyTenant, ResponseCache,
};
pub use score::{
    ExactMatch, FiniteF64, JsonSchema, MAX_REJECT_REASON_BYTES, NonFinite, SampleScore,
    ScoreContext, ScoreInput, ScoreRequest, Scorer, ToolRecord, Verdict, agent_sample_score,
    agent_sample_score_info, agent_score, agent_score_info, select,
};
pub use skill::{LOAD_SKILL_TOOL, SkillText};
#[cfg(feature = "postgres")]
pub use stores::{PostgresEpisodeStore, PostgresMemoryStore, PostgresTranscriptStore};
#[cfg(feature = "sqlite")]
pub use stores::{SqliteEpisodeStore, SqliteMemoryStore, SqliteTranscriptStore};
pub use tool::{FnTool, Tool, ToolContext, ToolEffect};
pub use transcript::{InMemoryTranscriptStore, PrefixRef, TranscriptStore, WindowOwner};
pub use types::{
    AgentReport, AgentStop, AgentTask, MAX_PARALLEL_CHILDREN, ModelTurn, ModelTurnRequest,
    PROBE_EFFECT_REFUSED, Probe, RunIdSource, ToolCallRequest, ToolOutcome,
};
pub use workflow::{
    WORKFLOW_NAME, activities, agent_approval_notice, agent_approval_notice_info, agent_deliver,
    agent_deliver_info, agent_discover_peers, agent_discover_peers_info, agent_loop,
    agent_loop_info, agent_memory_snapshot, agent_memory_snapshot_info, agent_model_turn,
    agent_model_turn_info, agent_precheck, agent_precheck_info, agent_tool_call,
    agent_tool_call_info, workflows,
};
pub use workspace::{
    DirWorkspaceBlobs, InMemoryWorkspaceBlobs, Workspace, WorkspaceBlobs, WorkspaceFile,
    WorkspaceHandle, WorkspaceMount, WorkspaceRef, WorkspaceStore,
};

/// Compiles each Rust fence in the two agent guides as a doctest (issue #2482).
///
/// A fence that drifts from the public API fails `cargo test --doc`. Each
/// fence must compile, so a reader can copy it as written. The guard suite
/// `tests/guide_snippets.rs` refuses a fence that rustdoc skips. A failure
/// names the guide and the line of the fence. The `doc-snippets` job in CI
/// runs these doctests, also on a docs-only change.
#[cfg(all(
    doctest,
    feature = "sqlite",
    feature = "postgres",
    feature = "eval",
    feature = "sandbox"
))]
mod guide_snippets {
    #[doc = include_str!("../../docs/agent-adapter.md")]
    struct AgentAdapterGuide;

    #[doc = include_str!("../../docs/agent-mesh.md")]
    struct AgentMeshGuide;
}
