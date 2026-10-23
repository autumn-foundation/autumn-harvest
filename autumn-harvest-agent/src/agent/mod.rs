//! A durable agent: the agent loop as a keyed entity (issue #2263).
//!
//! An [`Agent`] is a named, long-lived actor. It owns its state, handles one
//! op at a time, and survives a crash. The design is in `DESIGN-2262.md`.
//!
//! | Agent concept | Harvest part |
//! |---|---|
//! | Address | An [`AgentId`], `<kind>/<name>`. It is the workflow id. |
//! | Inbox | Signals named `harvest.entity.op` that hold an [`AgentOp`] |
//! | Config | An [`AgentProfile`] for each kind, on the harness |
//! | One op | One segment of the agent loop, with the whole harness |
//! | State | [`AgentState`]: the transcript, the flow labels and the counters |
//! | Checkpoint | Continue-as-new. The state rides in the input. |
//!
//! # Guarantees
//!
//! - **One op in flight.** The entity loop awaits each op to its end.
//! - **An acknowledged op is never lost.** The engine stores each signal
//!   before it acknowledges it. A paused agent holds work in its state and
//!   runs it, in order, when it resumes.
//! - **Replay rebuilds the transcript.** Each model turn and each tool call is
//!   a recorded activity. Each op ends with a recorded state snapshot.
//! - **A failed op keeps the transcript as it was.** It keeps the flow labels
//!   and the tokens that its tools and turns used.
//! - **Names never repeat.** Each segment gets the run id
//!   `<incarnation>:s<n>`. The state carries the tool-round counter, so
//!   approval names stay unique for the life of the agent.
//!
//! # Send an op
//!
//! On SQLite, call [`crate::sqlite::send`]. On Postgres, send a
//! signal-with-start to the workflow `agent`. Use the agent id as the
//! workflow id, `{}` as the input, and [`Agent::payload`] as the payload.

pub mod inbox;

use std::collections::{BTreeSet, VecDeque};
use std::time::Duration;

use autumn_harvest::entity::{ENTITY_OP_SIGNAL, Entity, EntityMessage};
use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

use crate::delivery::{Report, ReportSource, report_text};
use crate::flow::Released;
use crate::loop_guard::LoopGuard;
use crate::memory::MemoryScope;
use crate::mesh::Mandate;
use crate::message::{ChatMessage, ChatRole, RunId, SessionId};
use crate::types::{AgentStop, AgentTask, DEFAULT_APPROVAL_TIMEOUT_SECS, DEFAULT_MAX_STEPS};
use crate::workflow::{
    Member, Progress, Run, SteerBook, Tree, deliver, discover, harness, run_segment,
};
use inbox::{Admit, InboxStatus, OpClass, admit};

/// The registered workflow name of a durable agent.
pub const AGENT_WORKFLOW_NAME: &str = "agent";

/// The most bytes of [`AgentState::last_error`].
pub const MAX_ERROR_BYTES: usize = 1_024;

/// The most work ops that a paused agent holds. A later op fails, and
/// [`AgentState::last_error`] names it.
pub const MAX_HELD_OPS: usize = 256;

/// The default bound on the transcript, in messages.
pub const DEFAULT_WINDOW: u32 = 200;

/// The longest agent name, in bytes.
pub const MAX_AGENT_NAME_BYTES: usize = 200;

/// The signal that interrupts the op that an agent runs (issue #2265).
///
/// Its payload is an [`Interrupt`]. An interrupt that arrives while no op
/// runs is dropped when the next op starts.
pub const INTERRUPT_SIGNAL: &str = "agent.interrupt";

/// The signal that adds a message to the op that an agent runs (issue
/// #2265). Its payload is a [`Steer`].
pub const STEER_SIGNAL: &str = "agent.steer";

/// Why a text is not a valid [`AgentId`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum AgentIdError {
    /// The id has no `/` between the kind and the name.
    #[error("an agent id is <kind>/<name>, got {0:?}")]
    Shape(String),
    /// The kind is not a valid agent name.
    #[error("the agent kind {0:?} is not valid: use 1 to 64 of a-z, 0-9, _ and -")]
    Kind(String),
    /// The name is empty, too long, or holds a space or a control character.
    #[error("the agent name {0:?} is not valid: use 1 to 200 printable ASCII bytes, no space")]
    Name(String),
}

/// The address of a durable agent: `<kind>/<name>`.
///
/// The kind selects the [`AgentProfile`]. The name tells agents of one kind
/// apart, for example `support/acct-42`. The id is the workflow id.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct AgentId(String);

impl AgentId {
    /// The id of the agent `name` of the kind `kind`.
    ///
    /// # Errors
    ///
    /// Returns an error when `kind` or `name` is not valid.
    pub fn new(kind: &str, name: &str) -> Result<Self, AgentIdError> {
        Self::parse(&format!("{kind}/{name}"))
    }

    /// Read an id from its text form.
    ///
    /// # Errors
    ///
    /// Returns an error when the text is not `<kind>/<name>`, or a part is not
    /// valid.
    pub fn parse(text: &str) -> Result<Self, AgentIdError> {
        let (kind, name) = text
            .split_once('/')
            .ok_or_else(|| AgentIdError::Shape(text.to_owned()))?;
        if !crate::mesh::valid_name(kind) {
            return Err(AgentIdError::Kind(kind.to_owned()));
        }
        let printable = name.bytes().all(|byte| byte.is_ascii_graphic());
        if name.is_empty() || name.len() > MAX_AGENT_NAME_BYTES || !printable {
            return Err(AgentIdError::Name(name.to_owned()));
        }
        Ok(Self(text.to_owned()))
    }

    /// The kind: the part before the first `/`.
    #[must_use]
    pub fn kind(&self) -> &str {
        self.0.split_once('/').map_or("", |(kind, _)| kind)
    }

    /// The name: the part after the first `/`.
    #[must_use]
    pub fn name(&self) -> &str {
        self.0.split_once('/').map_or("", |(_, name)| name)
    }

    /// The id as text. It is the workflow id.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for AgentId {
    type Error = AgentIdError;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        Self::parse(&text)
    }
}

impl From<AgentId> for String {
    fn from(id: AgentId) -> Self {
        id.0
    }
}

impl std::fmt::Display for AgentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// How a message treats the op that runs when it arrives (issue #2265).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Urgency {
    /// Run after the current op.
    #[default]
    Queue,
    /// Join the current op before its next model turn. With no op running,
    /// run as a queued message. It needs a message id.
    Steer,
    /// Stop the current op, then run.
    Interrupt,
}

impl Urgency {
    /// `true` for [`Urgency::Queue`], the default.
    #[must_use]
    pub const fn is_queue(&self) -> bool {
        matches!(self, Self::Queue)
    }
}

/// A message from a person.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserMessage {
    /// The text for the agent.
    pub text: String,
    /// An id that the client chose. Use it as the idempotency key of the send.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    /// How the message treats the op that runs.
    #[serde(default, skip_serializing_if = "Urgency::is_queue")]
    pub urgency: Urgency,
}

/// The payload of [`STEER_SIGNAL`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Steer {
    /// The text for the agent.
    pub text: String,
    /// The id of the message. The op with this id is then a no-op.
    pub message_id: String,
}

/// The payload of [`INTERRUPT_SIGNAL`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Interrupt {
    /// The id of the message that comes with the interrupt, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
}

/// A control op. It applies at once, also while the agent is paused.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
#[non_exhaustive]
pub enum Control {
    /// Hold every later work op until a resume.
    Pause {
        /// Why, for the audit trail.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    /// Run the held ops in order, then take new ones.
    Resume,
    /// Clear the memory of the agent.
    ResetMemory,
    /// Clear the transcript. The flow labels stay.
    ClearTranscript,
}

/// One op in the inbox of an agent.
///
/// The wire form is internally tagged by `type`. A tick op does not decode
/// from a signal: only the agent itself makes one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum AgentOp {
    /// A message from a person.
    UserMessage(UserMessage),
    /// A control op.
    Control(Control),
}

impl AgentOp {
    /// A message from a person.
    #[must_use]
    pub fn message(text: impl Into<String>) -> Self {
        Self::UserMessage(UserMessage {
            text: text.into(),
            message_id: None,
            urgency: Urgency::Queue,
        })
    }

    /// A message from a person, with the id that the client chose.
    #[must_use]
    pub fn message_with_id(text: impl Into<String>, message_id: impl Into<String>) -> Self {
        Self::UserMessage(UserMessage {
            text: text.into(),
            message_id: Some(message_id.into()),
            urgency: Urgency::Queue,
        })
    }

    /// A message with an id and an urgency. See [`Urgency`].
    #[must_use]
    pub fn urgent(
        text: impl Into<String>,
        message_id: impl Into<String>,
        urgency: Urgency,
    ) -> Self {
        Self::UserMessage(UserMessage {
            text: text.into(),
            message_id: Some(message_id.into()),
            urgency,
        })
    }

    /// The class of the op for the inbox kernel.
    #[must_use]
    pub const fn class(&self) -> OpClass {
        match self {
            Self::UserMessage(_) => OpClass::Work,
            Self::Control(_) => OpClass::Control,
        }
    }
}

/// Where the memory of an agent lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AgentMemory {
    /// No memory.
    #[default]
    None,
    /// The `MemoryStore` of the harness, in the scope `agent:<id>`.
    Store,
}

/// The config of one kind of agent.
///
/// The harness holds one profile for each kind. The agent reads its profile
/// once per run, through a recorded activity, so a deploy changes the next
/// run only.
// The flags are independent switches on a recorded payload.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentProfile {
    /// The kind that this profile configures.
    pub kind: String,
    /// The system prompt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system: Option<String>,
    /// The bound on tool rounds in one op.
    #[serde(default = "default_max_steps")]
    pub max_steps: u32,
    /// The bound on provider-reported tokens in one op.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_total_tokens: Option<u32>,
    /// The output cap of one model call.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
    /// How long a gated call waits for a decision.
    #[serde(default = "default_approval_timeout")]
    pub approval_timeout_secs: u64,
    /// The activity-input cap of the workers. See
    /// [`AgentTask::max_request_bytes`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_request_bytes: Option<u64>,
    /// Refuse every tool call that writes or acts outside the app.
    #[serde(default)]
    pub read_only: bool,
    /// Send each answer to the harness delivery.
    #[serde(default)]
    pub deliver: bool,
    /// Where the memory of the agent lives.
    #[serde(default)]
    pub memory: AgentMemory,
    /// The authority of the agent, and the peers it may ask.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mandate: Option<Mandate>,
    /// The tenant that scopes the response cache.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    /// The loop guard of each op.
    #[serde(default)]
    pub loop_guard: LoopGuard,
    /// The bound on the transcript, in messages. The agent drops the oldest
    /// turns first. Zero keeps no transcript.
    #[serde(default = "default_window")]
    pub window: u32,
    /// Take a checkpoint after this many ops in one run. `None` leaves the
    /// decision to the history size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_ops_per_run: Option<u64>,
}

const fn default_max_steps() -> u32 {
    DEFAULT_MAX_STEPS
}

const fn default_approval_timeout() -> u64 {
    DEFAULT_APPROVAL_TIMEOUT_SECS
}

const fn default_window() -> u32 {
    DEFAULT_WINDOW
}

impl AgentProfile {
    /// A profile for `kind` with the default bounds.
    #[must_use]
    pub fn new(kind: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            system: None,
            max_steps: DEFAULT_MAX_STEPS,
            max_total_tokens: None,
            max_output_tokens: None,
            approval_timeout_secs: DEFAULT_APPROVAL_TIMEOUT_SECS,
            max_request_bytes: None,
            read_only: false,
            deliver: false,
            memory: AgentMemory::None,
            mandate: None,
            tenant: None,
            loop_guard: LoopGuard::default(),
            window: DEFAULT_WINDOW,
            max_ops_per_run: None,
        }
    }

    /// Set the system prompt.
    #[must_use]
    pub fn system(mut self, system: impl Into<String>) -> Self {
        self.system = Some(system.into());
        self
    }

    /// Set the bound on tool rounds in one op.
    #[must_use]
    pub const fn max_steps(mut self, max_steps: u32) -> Self {
        self.max_steps = max_steps;
        self
    }

    /// Set the bound on provider-reported tokens in one op.
    #[must_use]
    pub const fn max_total_tokens(mut self, max: u32) -> Self {
        self.max_total_tokens = Some(max);
        self
    }

    /// Set how long a gated call waits for a decision.
    #[must_use]
    pub const fn approval_timeout(mut self, timeout: Duration) -> Self {
        self.approval_timeout_secs = AgentTask::new_timeout_secs(timeout);
        self
    }

    /// Refuse every tool call that writes or acts outside the app.
    #[must_use]
    pub const fn read_only(mut self) -> Self {
        self.read_only = true;
        self
    }

    /// Send each answer to the harness delivery.
    #[must_use]
    pub const fn deliver(mut self) -> Self {
        self.deliver = true;
        self
    }

    /// Keep memory in the `MemoryStore` of the harness.
    #[must_use]
    pub const fn memory(mut self, memory: AgentMemory) -> Self {
        self.memory = memory;
        self
    }

    /// Set the mandate of the agent.
    #[must_use]
    pub fn mandate(mut self, mandate: Mandate) -> Self {
        self.mandate = Some(mandate);
        self
    }

    /// Set the tenant that scopes the response cache.
    #[must_use]
    pub fn tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant = Some(tenant.into());
        self
    }

    /// Set the bound on the transcript, in messages.
    #[must_use]
    pub const fn window(mut self, messages: u32) -> Self {
        self.window = messages;
        self
    }

    /// Take a checkpoint after `n` ops in one run.
    #[must_use]
    pub const fn max_ops_per_run(mut self, n: u64) -> Self {
        self.max_ops_per_run = Some(n);
        self
    }

    /// The memory scope of the agent `id`, when the profile keeps memory in a
    /// store.
    #[must_use]
    pub fn memory_scope(&self, id: &AgentId) -> Option<MemoryScope> {
        match self.memory {
            AgentMemory::None => None,
            AgentMemory::Store => Some(MemoryScope::new(format!("agent:{id}"))),
        }
    }

    /// The task of one segment of the agent `id`.
    fn segment_task(&self, id: &AgentId, input: &str, unattended: bool) -> AgentTask {
        let mandate_read_only = self
            .mandate
            .as_ref()
            .is_some_and(|mandate| mandate.read_only);
        let mut task = AgentTask::new(input).max_steps(self.max_steps);
        task.system.clone_from(&self.system);
        task.session_id = Some(SessionId::new(id.as_str()));
        task.max_total_tokens = self.max_total_tokens;
        task.max_output_tokens = self.max_output_tokens;
        task.approval_timeout_secs = self.approval_timeout_secs;
        task.max_request_bytes = self.max_request_bytes;
        task.deliver = self.deliver;
        task.memory_scope = self.memory_scope(id);
        task.loop_guard = self.loop_guard;
        task.read_only = self.read_only || mandate_read_only || unattended;
        task.unattended = unattended;
        task.tenant.clone_from(&self.tenant);
        task.mandate.clone_from(&self.mandate);
        task
    }
}

/// The status of an agent.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
#[non_exhaustive]
pub enum AgentStatus {
    /// The agent takes work.
    #[default]
    Open,
    /// A person paused the agent. Work waits.
    Paused {
        /// Why, from the pause op.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
}

impl AgentStatus {
    /// The status of the inbox kernel.
    #[must_use]
    pub const fn inbox(&self) -> InboxStatus {
        match self {
            Self::Open => InboxStatus::Open,
            Self::Paused { .. } => InboxStatus::Paused,
        }
    }

    /// The status name for listing: `open` or `paused`.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Paused { .. } => "paused",
        }
    }
}

/// The durable state of one agent.
///
/// Each op ends with a snapshot of it in history. A checkpoint carries it to
/// the next run. Add a field only with a default, so an older snapshot still
/// decodes.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct AgentState {
    /// A recorded id that names this life of the agent. A delete ends the
    /// life. Run ids start with it, so a new life never reuses a key.
    pub incarnation: String,
    /// Segments that ran. The next run id is `<incarnation>:s<segments>`.
    pub segments: u64,
    /// The conversation, oldest first, without the system prompt.
    pub transcript: Vec<ChatMessage>,
    /// The flow labels that the agent has exercised.
    pub flow_labels: BTreeSet<String>,
    /// Provider-reported tokens over the life of the agent.
    pub tokens_used: u64,
    /// Tool rounds over the life of the agent. Approval names hold it.
    pub rounds: u32,
    /// Whether the agent takes work.
    pub status: AgentStatus,
    /// Work that arrived while the agent was paused, oldest first.
    pub held: VecDeque<AgentOp>,
    /// Ops that failed.
    pub failures: u64,
    /// The error of the last failed op, cut to [`MAX_ERROR_BYTES`].
    pub last_error: Option<String>,
    /// How the last segment ended.
    pub last_stop: Option<AgentStop>,
    /// The answer of the last segment.
    pub last_answer: Option<String>,
    /// The steer messages that wait for their other half (issue #2265).
    pub steer: SteerBook,
}

/// A durable agent: its address and the ops it takes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Agent {
    id: AgentId,
}

impl Agent {
    /// The agent with the address `id`.
    #[must_use]
    pub const fn new(id: AgentId) -> Self {
        Self { id }
    }

    /// The address of the agent. It is the workflow id.
    #[must_use]
    pub const fn id(&self) -> &AgentId {
        &self.id
    }

    /// The signal payload that sends `op`.
    #[must_use]
    pub fn payload(op: &AgentOp) -> serde_json::Value {
        serde_json::json!(EntityMessage::op(op))
    }

    /// The signal payload that deletes the agent.
    #[must_use]
    pub fn delete_payload() -> serde_json::Value {
        serde_json::json!(EntityMessage::<AgentOp>::delete())
    }

    /// The extra signal that goes before `op`, if its urgency needs one.
    ///
    /// Send the signal first, then the op. A [`Urgency::Steer`] message with
    /// no id needs no signal: it runs as a queued message.
    #[must_use]
    pub fn urgency_signal(op: &AgentOp) -> Option<(&'static str, serde_json::Value)> {
        let AgentOp::UserMessage(message) = op else {
            return None;
        };
        match (message.urgency, &message.message_id) {
            (Urgency::Steer, Some(id)) => Some((
                STEER_SIGNAL,
                serde_json::json!(Steer {
                    text: message.text.clone(),
                    message_id: id.clone(),
                }),
            )),
            (Urgency::Interrupt, id) => Some((
                INTERRUPT_SIGNAL,
                serde_json::json!(Interrupt {
                    message_id: id.clone(),
                }),
            )),
            _ => None,
        }
    }
}

/// The parts of one run that every op reads.
struct AgentRun<'a> {
    ctx: &'a WorkflowContext,
    /// The agent id, or why the workflow id is not one.
    id: Result<AgentId, String>,
    /// The profile of the kind, or why there is none.
    profile: Result<AgentProfile, String>,
    /// The mandate and the peers of the agent, when the profile has a mandate.
    member: Option<Member>,
}

/// The durable agent workflow. The workflow id is the [`AgentId`].
///
/// Each op runs one segment of the agent loop. See the [module
/// docs](self).
///
/// # Errors
///
/// Returns an engine error of the entity loop, for example a cancel.
#[workflow]
pub async fn agent(ctx: &WorkflowContext, input: serde_json::Value) -> Result<AgentState, String> {
    let id = AgentId::parse(ctx.workflow_id()).map_err(|err| err.to_string());
    let profile = match &id {
        Ok(id) => lookup_profile(ctx, id).await?,
        Err(err) => Err(err.clone()),
    };
    let member = match &profile {
        Ok(AgentProfile {
            mandate: Some(mandate),
            ..
        }) if mandate.validate().is_ok() => Some(discover(ctx, mandate).await?),
        _ => None,
    };
    let run = AgentRun {
        ctx,
        id,
        profile,
        member,
    };
    let max_ops = run
        .profile
        .as_ref()
        .ok()
        .and_then(|profile| profile.max_ops_per_run);
    let entity = Entity::<AgentState>::from_input(ctx, input);
    let entity = match max_ops {
        Some(n) => entity.max_ops_per_run(n),
        None => entity,
    };
    entity
        .run(|state, op: AgentOp| run.handle(state, op))
        .await
        .map_err(|err| err.to_string())
}

/// Read the profile of the kind of `id` through the recorded activity.
async fn lookup_profile(
    ctx: &WorkflowContext,
    id: &AgentId,
) -> Result<Result<AgentProfile, String>, String> {
    let found: Option<AgentProfile> = ctx
        .execute_activity(&agent_profile_info(), id.kind().to_owned())
        .await
        .map_err(|err| err.to_string())?;
    Ok(found.ok_or_else(|| format!("no agent profile for the kind {:?}", id.kind())))
}

impl AgentRun<'_> {
    /// Apply one op to `state`.
    ///
    /// The handler never returns `Err` for a failed segment. A failed op is a
    /// count and an error text in the state, so its labels and its tokens
    /// stay.
    async fn handle(&self, mut state: AgentState, op: AgentOp) -> Result<AgentState, String> {
        if state.incarnation.is_empty() {
            state.incarnation = self.ctx.new_uuid().to_string();
        }
        match admit(state.status.inbox(), op.class()) {
            Admit::Apply => {
                if let AgentOp::Control(control) = op {
                    state = self.control(state, control).await;
                }
            }
            Admit::Hold => hold(&mut state, op),
            Admit::Run => state = self.work(state, op).await,
            Admit::Skip => {}
        }
        Ok(state)
    }

    /// Apply one control op.
    async fn control(&self, mut state: AgentState, control: Control) -> AgentState {
        match control {
            Control::Pause { reason } => state.status = AgentStatus::Paused { reason },
            Control::Resume => {
                state.status = AgentStatus::Open;
                while let Some(op) = state.held.pop_front() {
                    state = self.work(state, op).await;
                }
            }
            Control::ResetMemory => {
                if let Err(err) = self.reset_memory().await {
                    fail(&mut state, &err);
                }
            }
            Control::ClearTranscript => state.transcript.clear(),
        }
        state
    }

    /// Clear the memory scope of the agent, when it has one.
    async fn reset_memory(&self) -> Result<(), String> {
        let (Ok(id), Ok(profile)) = (&self.id, &self.profile) else {
            return Ok(());
        };
        let Some(scope) = profile.memory_scope(id) else {
            return Ok(());
        };
        self.ctx
            .execute_activity(&agent_memory_reset_info(), scope)
            .await
            .map_err(|err| err.to_string())
    }

    /// Run one work op as a segment.
    async fn work(&self, mut state: AgentState, op: AgentOp) -> AgentState {
        match op {
            AgentOp::UserMessage(message) => {
                if let (Urgency::Steer, Some(id)) = (message.urgency, message.message_id) {
                    // A running segment took the steer signal, so the op is done.
                    if SteerBook::take(&mut state.steer.steered, &id) {
                        return state;
                    }
                    // The op runs first. Its steer signal is then a no-op.
                    SteerBook::note(&mut state.steer.ran, id);
                }
                self.segment(state, &message.text, false).await
            }
            AgentOp::Control(_) => state,
        }
    }

    /// Run one segment with `input` and fold it into `state`.
    async fn segment(&self, mut state: AgentState, input: &str, unattended: bool) -> AgentState {
        let (id, profile) = match (&self.id, &self.profile) {
            (Ok(id), Ok(profile)) => (id, profile),
            (Err(err), _) | (_, Err(err)) => {
                fail(&mut state, err);
                return state;
            }
        };
        if let Some(Err(err)) = profile.mandate.as_ref().map(Mandate::validate) {
            fail(
                &mut state,
                &format!("the profile mandate is not valid: {err}"),
            );
            return state;
        }
        // An interrupt that arrived while no op ran is stale.
        if let Err(err) = self.ctx.drain_signals_raw(INTERRUPT_SIGNAL) {
            fail(&mut state, &err.to_string());
            return state;
        }
        let task = profile.segment_task(id, input, unattended);
        let run_id = format!("{}:s{}", state.incarnation, state.segments);
        state.segments = state.segments.saturating_add(1);
        let mut progress = Progress::new(run_id.clone(), state.transcript.clone());
        progress.final_turn_rules = true;
        progress.steer = Some(std::mem::take(&mut state.steer));
        let mut tree = Tree {
            root: run_id.clone(),
            rounds: state.rounds,
            delegations: 0,
            exercised: state
                .flow_labels
                .iter()
                .chain(profile.mandate.iter().flat_map(|m| &m.flow_labels))
                .cloned()
                .collect(),
            released: Released::new(),
            records: Vec::new(),
        };
        let run = Run {
            task: &task,
            agent: self.member.as_ref(),
            live: true,
        };
        let result = run_segment(self.ctx, run, &mut progress, &mut tree, input).await;
        state.steer = progress.steer.take().unwrap_or_default();
        // A failed op keeps what its tools and turns did to the labels and
        // the tokens. Only the transcript stays as it was.
        state.rounds = tree.rounds;
        state.flow_labels = tree.exercised;
        state.tokens_used = state
            .tokens_used
            .saturating_add(u64::from(progress.usage.total()));
        match result {
            Ok(stop) => {
                let messages = crate::types::without_system(&progress.messages);
                state.transcript = trim_window(messages, profile.window);
                state.last_stop = Some(stop);
                let answer = progress.delivered_text(&progress.last_text).to_owned();
                state.last_answer = Some(answer.clone());
                if task.deliver {
                    self.deliver(&mut state, &run_id, &task, stop, &answer)
                        .await;
                }
            }
            Err(err) => fail(&mut state, &err),
        }
        state
    }

    /// Send the answer of one segment to the delivery.
    async fn deliver(
        &self,
        state: &mut AgentState,
        run_id: &str,
        task: &AgentTask,
        stop: AgentStop,
        answer: &str,
    ) {
        let Some(text) = report_text(ReportSource::Run, stop, answer) else {
            return;
        };
        let report = Report {
            source: ReportSource::Run,
            run_id: RunId::new(run_id),
            segment: 0,
            session_id: task.session_id.clone(),
            text,
            stop,
        };
        if let Err(err) = deliver(self.ctx, report).await {
            fail(state, &err);
        }
    }
}

/// Put a work op at the end of the held queue.
///
/// The queue has a bound. An op past it fails, and the error names it, so
/// no op leaves without a trace.
fn hold(state: &mut AgentState, op: AgentOp) {
    if state.held.len() < MAX_HELD_OPS {
        state.held.push_back(op);
        return;
    }
    let id = match &op {
        AgentOp::UserMessage(message) => message.message_id.clone(),
        AgentOp::Control(_) => None,
    };
    fail(
        state,
        &format!(
            "the held queue is full ({MAX_HELD_OPS} ops); the op {} was refused",
            id.as_deref().unwrap_or("without an id")
        ),
    );
}

/// Count a failed op and keep its error.
fn fail(state: &mut AgentState, error: &str) {
    state.failures = state.failures.saturating_add(1);
    state.last_error = Some(cut(error, MAX_ERROR_BYTES));
}

/// Cut `text` to at most `max` bytes, on a `char` boundary.
fn cut(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_owned();
    }
    let mut end = max;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_owned()
}

/// Keep at most `window` messages of `messages`.
///
/// The cut lands on a user message, so a tool call never loses its result.
/// When the last user message starts more than `window` messages from the
/// end, the transcript keeps the whole last exchange.
fn trim_window(messages: Vec<ChatMessage>, window: u32) -> Vec<ChatMessage> {
    let window = usize::try_from(window).unwrap_or(usize::MAX);
    if messages.len() <= window {
        return messages;
    }
    if window == 0 {
        return Vec::new();
    }
    let earliest = messages.len() - window;
    let starts = |message: &ChatMessage| message.role == ChatRole::User;
    let cut = messages
        .iter()
        .enumerate()
        .skip(earliest)
        .find(|(_, message)| starts(message))
        .or_else(|| messages.iter().enumerate().rev().find(|(_, m)| starts(m)))
        .map_or(0, |(at, _)| at);
    messages.into_iter().skip(cut).collect()
}

/// The profile of one kind, as an activity.
///
/// # Errors
///
/// Returns an error only when no harness is installed.
#[activity(
    start_to_close = "1m",
    retry = RetryPolicy::exponential(3, Duration::from_secs(1))
)]
pub async fn agent_profile(
    ctx: &ActivityContext,
    kind: String,
) -> Result<Option<AgentProfile>, String> {
    Ok(harness(ctx)?.agent_profile(&kind))
}

/// Clear one memory scope, as an activity.
///
/// # Errors
///
/// Returns an error when no store is installed or the store cannot clear.
#[activity(
    start_to_close = "1m",
    retry = RetryPolicy::exponential(3, Duration::from_secs(1))
)]
pub async fn agent_memory_reset(ctx: &ActivityContext, scope: MemoryScope) -> Result<(), String> {
    harness(ctx)?.memory_reset(scope).await
}

/// The signal name of an agent op.
#[must_use]
pub const fn op_signal() -> &'static str {
    ENTITY_OP_SIGNAL
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::message::ContentPart;

    #[test]
    fn an_id_is_a_kind_and_a_name() {
        let id = AgentId::new("support", "acct-42").unwrap();
        assert_eq!(id.as_str(), "support/acct-42");
        assert_eq!(id.kind(), "support");
        assert_eq!(id.name(), "acct-42");
        // The name may hold a slash.
        let nested = AgentId::parse("support/eu/acct-42").unwrap();
        assert_eq!(nested.kind(), "support");
        assert_eq!(nested.name(), "eu/acct-42");
    }

    #[test]
    fn a_bad_id_is_refused() {
        assert!(matches!(
            AgentId::parse("support"),
            Err(AgentIdError::Shape(_))
        ));
        assert!(matches!(
            AgentId::parse("Support/a"),
            Err(AgentIdError::Kind(_))
        ));
        assert!(matches!(AgentId::parse("/a"), Err(AgentIdError::Kind(_))));
        assert!(matches!(AgentId::parse("s/"), Err(AgentIdError::Name(_))));
        assert!(matches!(
            AgentId::parse("s/a b"),
            Err(AgentIdError::Name(_))
        ));
        let long = format!("s/{}", "a".repeat(MAX_AGENT_NAME_BYTES + 1));
        assert!(matches!(AgentId::parse(&long), Err(AgentIdError::Name(_))));
    }

    #[test]
    fn an_id_decodes_only_when_valid() {
        let id: AgentId = serde_json::from_value(serde_json::json!("a/b")).unwrap();
        assert_eq!(id.as_str(), "a/b");
        assert!(serde_json::from_value::<AgentId>(serde_json::json!("nope")).is_err());
    }

    #[test]
    fn the_op_wire_form_is_tagged() {
        let op = AgentOp::message_with_id("hi", "m-1");
        assert_eq!(
            serde_json::to_value(&op).unwrap(),
            serde_json::json!({"type": "user_message", "text": "hi", "message_id": "m-1"})
        );
        let pause = AgentOp::Control(Control::Pause { reason: None });
        assert_eq!(
            serde_json::to_value(&pause).unwrap(),
            serde_json::json!({"type": "control", "action": "pause"})
        );
        assert_eq!(
            Agent::payload(&pause),
            serde_json::json!({"kind": "op", "op": {"type": "control", "action": "pause"}})
        );
    }

    #[test]
    fn an_old_state_decodes_with_defaults() {
        let state: AgentState = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(state, AgentState::default());
        assert_eq!(state.status, AgentStatus::Open);
    }

    #[test]
    fn a_profile_decodes_from_its_kind_alone() {
        let profile: AgentProfile =
            serde_json::from_value(serde_json::json!({"kind": "support"})).unwrap();
        assert_eq!(profile, AgentProfile::new("support"));
    }

    fn text(role: ChatRole, text: &str) -> ChatMessage {
        ChatMessage::text(role, text)
    }

    fn tool_result() -> ChatMessage {
        ChatMessage {
            role: ChatRole::Tool,
            content: vec![ContentPart::ToolResult {
                tool_call_id: "c".into(),
                content: "ok".into(),
            }],
        }
    }

    #[test]
    fn the_window_cuts_on_a_user_message() {
        let messages = vec![
            text(ChatRole::User, "u1"),
            text(ChatRole::Assistant, "a1"),
            text(ChatRole::User, "u2"),
            text(ChatRole::Assistant, "call"),
            tool_result(),
            text(ChatRole::Assistant, "a2"),
        ];
        // Five messages would start on the reply a1. The cut moves on to u2.
        let kept = trim_window(messages.clone(), 5);
        assert_eq!(kept, messages[2..].to_vec());
        assert_eq!(trim_window(messages.clone(), 6), messages);
        assert_eq!(trim_window(messages, 0), Vec::new());
    }

    #[test]
    fn the_window_keeps_the_last_exchange_whole() {
        let messages = vec![
            text(ChatRole::User, "u1"),
            text(ChatRole::Assistant, "call"),
            tool_result(),
            text(ChatRole::Assistant, "call"),
            tool_result(),
        ];
        // No user message starts in the last two. The whole exchange stays.
        assert_eq!(trim_window(messages.clone(), 2), messages);
    }

    #[test]
    fn a_long_error_is_cut_on_a_char_boundary() {
        let error = "é".repeat(MAX_ERROR_BYTES);
        let kept = cut(&error, MAX_ERROR_BYTES);
        assert!(kept.len() <= MAX_ERROR_BYTES);
        assert!(error.starts_with(&kept));
    }

    #[test]
    fn a_full_held_queue_refuses_and_names_the_op() {
        let mut state = AgentState::default();
        for n in 0..MAX_HELD_OPS {
            hold(&mut state, AgentOp::message(format!("m{n}")));
        }
        hold(&mut state, AgentOp::message_with_id("late", "m-late"));
        assert_eq!(state.held.len(), MAX_HELD_OPS);
        assert_eq!(state.failures, 1);
        assert!(state.last_error.unwrap().contains("m-late"));
    }

    #[test]
    fn the_segment_task_follows_the_profile() {
        let id = AgentId::new("support", "a").unwrap();
        let profile = AgentProfile::new("support")
            .system("Be brief.")
            .memory(AgentMemory::Store)
            .deliver();
        let task = profile.segment_task(&id, "hi", false);
        assert_eq!(task.input, "hi");
        assert_eq!(task.system.as_deref(), Some("Be brief."));
        assert_eq!(task.session_id, Some(SessionId::new("support/a")));
        assert_eq!(task.memory_scope, Some(MemoryScope::new("agent:support/a")));
        assert!(task.deliver);
        assert!(!task.read_only);
        let unattended = profile.segment_task(&id, "tick", true);
        assert!(unattended.read_only && unattended.unattended);
    }
}
