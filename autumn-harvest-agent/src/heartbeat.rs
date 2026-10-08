//! Heartbeats: an agent that wakes on a schedule and speaks only when
//! something needs attention.
//!
//! One tick is one short workflow, [`agent_heartbeat`]:
//!
//! 1. The [`Precheck`] activity runs. A `false` answer ends the tick with no
//!    model call.
//! 2. The agent loop runs the heartbeat prompt. It is read-only unless the
//!    task allows actions.
//! 3. A report that is not a silent acknowledgement goes to the delivery.
//!
//! On Postgres, an engine schedule starts one tick per firing. See
//! [`schedule`]. On SQLite, the app starts each tick. A tick is a whole
//! workflow, so a crash mid-tick resumes like any other run.
//!
//! A tick never books a follow-up. A memory scope gives it a read-only
//! snapshot. It can write memory only with
//! [`HeartbeatTask::allow_memory_writes`].

use std::fmt::Debug;
use std::time::Duration;

use autumn_harvest::policy::{Schedule, WorkflowSchedule};
use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

use crate::delivery::ReportSource;
use crate::memory::MemoryScope;
use crate::message::{ChatMessage, SessionId};
use crate::model::BoxFuture;
use crate::types::{AgentReport, AgentTask, DEFAULT_MAX_STEPS};
use crate::workflow::{agent_precheck_info, drive};

/// The registered heartbeat workflow name.
pub const HEARTBEAT_WORKFLOW_NAME: &str = "agent_heartbeat";

/// The prompt of a heartbeat task that sets none.
pub const DEFAULT_HEARTBEAT_PROMPT: &str = "This is a scheduled check. \
Review what you can see and report only what needs attention. \
If nothing needs attention, reply with exactly HEARTBEAT_OK.";

/// The default time a heartbeat tick waits for an approval: 15 minutes.
///
/// No person watches a tick. A long wait would also hold the schedule, which
/// starts no new tick while one runs.
pub const DEFAULT_HEARTBEAT_APPROVAL_TIMEOUT_SECS: u64 = 15 * 60;

/// A cheap check that can skip a heartbeat tick before any model call.
///
/// Use it to look for new work, for example an unread inbox. Keep it fast
/// and free of side effects. It runs once per tick and is not retried.
///
/// The task it gets has no `history`, so a long history cannot make the
/// check too large to record. A check over the harness hook budget skips the
/// tick.
pub trait Precheck: Send + Sync + Debug {
    /// Return `true` to run the tick.
    fn should_run<'a>(&'a self, task: &'a HeartbeatTask) -> BoxFuture<'a, bool>;
}

/// The input of one heartbeat tick.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HeartbeatTask {
    /// The user message of the tick.
    #[serde(default = "default_prompt")]
    pub prompt: String,
    /// The system prompt.
    #[serde(default)]
    pub system: Option<String>,
    /// Earlier messages to continue.
    #[serde(default)]
    pub history: Vec<ChatMessage>,
    /// The session the tick belongs to.
    #[serde(default)]
    pub session_id: Option<SessionId>,
    /// The memory the tick reads and updates.
    #[serde(default)]
    pub memory_scope: Option<MemoryScope>,
    /// Let the tick run tools that write or act outside. Off by default.
    #[serde(default)]
    pub allow_actions: bool,
    /// The most tool rounds in one tick.
    #[serde(default = "default_max_steps")]
    pub max_steps: u32,
    /// The bound on provider-reported tokens for one tick.
    #[serde(default)]
    pub max_total_tokens: Option<u32>,
    /// The output cap of one model call.
    #[serde(default)]
    pub max_output_tokens: Option<u32>,
    /// How long a gated call waits for a decision before it is denied.
    #[serde(default = "default_approval_timeout")]
    pub approval_timeout_secs: u64,
    /// Let the tick write its memory. Off by default: a tick often reads
    /// untrusted data, and a note shows in every later run of the scope.
    #[serde(default)]
    pub allow_memory_writes: bool,
}

fn default_prompt() -> String {
    DEFAULT_HEARTBEAT_PROMPT.to_owned()
}

const fn default_max_steps() -> u32 {
    DEFAULT_MAX_STEPS
}

const fn default_approval_timeout() -> u64 {
    DEFAULT_HEARTBEAT_APPROVAL_TIMEOUT_SECS
}

impl Default for HeartbeatTask {
    fn default() -> Self {
        Self::new()
    }
}

impl HeartbeatTask {
    /// A read-only tick with the default prompt.
    #[must_use]
    pub fn new() -> Self {
        Self {
            prompt: default_prompt(),
            system: None,
            history: Vec::new(),
            session_id: None,
            memory_scope: None,
            allow_actions: false,
            max_steps: DEFAULT_MAX_STEPS,
            max_total_tokens: None,
            max_output_tokens: None,
            approval_timeout_secs: DEFAULT_HEARTBEAT_APPROVAL_TIMEOUT_SECS,
            allow_memory_writes: false,
        }
    }

    /// Set the prompt. Tell the model to reply with exactly
    /// [`HEARTBEAT_OK`](crate::delivery::HEARTBEAT_OK) when nothing needs
    /// attention.
    #[must_use]
    pub fn prompt(mut self, prompt: impl Into<String>) -> Self {
        self.prompt = prompt.into();
        self
    }

    /// Set the system prompt.
    #[must_use]
    pub fn system(mut self, system: impl Into<String>) -> Self {
        self.system = Some(system.into());
        self
    }

    /// Continue earlier messages.
    #[must_use]
    pub fn history(mut self, history: Vec<ChatMessage>) -> Self {
        self.history = history;
        self
    }

    /// Tag the tick with a session.
    #[must_use]
    pub fn session(mut self, session_id: SessionId) -> Self {
        self.session_id = Some(session_id);
        self
    }

    /// Read and update the memory of `scope`.
    #[must_use]
    pub fn memory(mut self, scope: MemoryScope) -> Self {
        self.memory_scope = Some(scope);
        self
    }

    /// Let the tick run tools that write or act outside.
    #[must_use]
    pub const fn allow_actions(mut self) -> Self {
        self.allow_actions = true;
        self
    }

    /// Set the most tool rounds in one tick.
    #[must_use]
    pub const fn max_steps(mut self, max_steps: u32) -> Self {
        self.max_steps = max_steps;
        self
    }

    /// Set the bound on provider-reported tokens for one tick.
    #[must_use]
    pub const fn max_total_tokens(mut self, max: u32) -> Self {
        self.max_total_tokens = Some(max);
        self
    }

    /// Set the output cap of one model call.
    #[must_use]
    pub const fn max_output_tokens(mut self, max: u32) -> Self {
        self.max_output_tokens = Some(max);
        self
    }

    /// Set how long a gated call waits for a decision. A part second rounds
    /// up to a whole second.
    #[must_use]
    pub const fn approval_timeout(mut self, timeout: Duration) -> Self {
        self.approval_timeout_secs = AgentTask::new_timeout_secs(timeout);
        self
    }

    /// Let the tick write its memory.
    #[must_use]
    pub const fn allow_memory_writes(mut self) -> Self {
        self.allow_memory_writes = true;
        self
    }

    /// The agent task of one tick.
    fn agent_task(&self) -> AgentTask {
        let mut task = AgentTask::new(self.prompt.clone())
            .history(self.history.clone())
            .max_steps(self.max_steps)
            .deliver();
        task.system.clone_from(&self.system);
        task.session_id.clone_from(&self.session_id);
        task.memory_scope.clone_from(&self.memory_scope);
        task.max_total_tokens = self.max_total_tokens;
        task.max_output_tokens = self.max_output_tokens;
        task.approval_timeout_secs = self.approval_timeout_secs;
        task.read_only = !self.allow_actions;
        task.unattended = true;
        task.unattended_memory_writes = self.allow_memory_writes;
        task
    }
}

/// The output of one heartbeat tick.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HeartbeatReport {
    /// The precheck skipped the tick. No model call ran.
    pub skipped: bool,
    /// The delivery took a report.
    pub delivered: bool,
    /// The agent run, when the tick ran.
    pub report: Option<AgentReport>,
}

/// One heartbeat tick.
///
/// # Errors
///
/// Returns an error when an activity of the agent loop fails for good.
#[workflow]
pub async fn agent_heartbeat(
    ctx: &WorkflowContext,
    task: HeartbeatTask,
) -> Result<HeartbeatReport, String> {
    let probe = HeartbeatTask {
        history: Vec::new(),
        ..task.clone()
    };
    let run: bool = ctx
        .execute_activity(&agent_precheck_info(), probe)
        .await
        .map_err(|e| e.to_string())?;
    if !run {
        return Ok(HeartbeatReport {
            skipped: true,
            delivered: false,
            report: None,
        });
    }
    let (report, delivered) = drive(ctx, &task.agent_task(), ReportSource::Heartbeat).await?;
    Ok(HeartbeatReport {
        skipped: false,
        delivered: delivered > 0,
        report: Some(report),
    })
}

/// A Postgres engine schedule that starts one heartbeat tick per firing.
///
/// Register it with `HarvestBuilder::workflow_schedule`, for example with
/// `Schedule::Interval(Duration::from_secs(1_800))` for a tick every 30
/// minutes.
///
/// The engine keeps one schedule per workflow name, so a deployment has one
/// heartbeat schedule. For one heartbeat per user, start each tick from the
/// app, or let one scheduled workflow start the ticks.
///
/// # Errors
///
/// Returns an error when `task` cannot encode as JSON.
pub fn schedule(
    when: Schedule,
    task: &HeartbeatTask,
) -> Result<WorkflowSchedule, serde_json::Error> {
    Ok(
        WorkflowSchedule::new(HEARTBEAT_WORKFLOW_NAME, when)
            .with_input(serde_json::to_value(task)?),
    )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::delivery::HEARTBEAT_OK;

    #[test]
    fn the_default_prompt_names_the_silent_answer() {
        assert!(DEFAULT_HEARTBEAT_PROMPT.contains(HEARTBEAT_OK));
    }

    #[test]
    fn a_tick_is_read_only_and_delivered_by_default() {
        let task = HeartbeatTask::new().agent_task();
        assert!(task.read_only);
        assert!(task.deliver);
        assert!(task.followups.is_none(), "a tick never books a follow-up");
    }

    #[test]
    fn allow_actions_lifts_read_only_but_not_the_memory_rule() {
        let task = HeartbeatTask::new()
            .memory(MemoryScope::new("u"))
            .allow_actions()
            .agent_task();
        assert!(!task.read_only);
        assert!(task.unattended);
        assert_eq!(task.memory_tool_scope(), None);
    }

    #[test]
    fn an_empty_task_decodes_to_the_defaults() {
        let task: HeartbeatTask = serde_json::from_str("{}").unwrap();
        assert_eq!(task, HeartbeatTask::new());
    }

    #[test]
    fn the_schedule_names_the_heartbeat_workflow() {
        let task = HeartbeatTask::new().prompt("check the queue");
        let schedule = schedule(Schedule::Manual, &task).unwrap();
        assert_eq!(schedule.workflow_name, HEARTBEAT_WORKFLOW_NAME);
        assert_eq!(schedule.input, serde_json::to_value(&task).unwrap());
    }
}
