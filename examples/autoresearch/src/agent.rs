//! The agent harness: tools, policy, flow rules, the reviewer peer and the
//! stores.
//!
//! An agent run names its lab in `AgentTask::session`. Each tool resolves the
//! lab from `ToolContext::session_id` and serves only a lab that this process
//! registered. So one process can serve several labs, and a recorded run can
//! never reach a lab that it did not name.
//!
//! | Tool | Effect | Gate |
//! |---|---|---|
//! | `read_file` | read-only | none |
//! | `show_diff` | read-only | none |
//! | `read_paper` | read-only, `untrusted_input` | none |
//! | `edit_train` | write | approval after `read_paper` (flow rule) |
//! | `rewind` | write | always approval |
//! | anything else | | denied |

use std::path::Path;
use std::sync::Arc;

use autumn_harvest_agent::{
    AgentHarness, AgentModel, AgentTask, FlowRules, HeartbeatTask, Peer, Tool, ToolRules,
};

use crate::lab::Lab;

/// The researcher reads and edits. The marker tells a scripted model its role.
pub const RESEARCHER_ROLE: &str = "ROLE: researcher";
/// The reviewer peer judges a diff.
pub const REVIEWER_ROLE: &str = "ROLE: reviewer";
/// The heartbeat agent watches the results.
pub const WATCHER_ROLE: &str = "ROLE: watcher";
/// The report agent writes the morning report.
pub const REPORTER_ROLE: &str = "ROLE: reporter";
/// The name of the reviewer peer.
pub const REVIEWER: &str = "reviewer";
/// The tool names.
pub const READ_FILE: &str = "read_file";
/// See [`READ_FILE`].
pub const READ_PAPER: &str = "read_paper";
/// See [`READ_FILE`].
pub const SHOW_DIFF: &str = "show_diff";
/// See [`READ_FILE`].
pub const EDIT_TRAIN: &str = "edit_train";
/// See [`READ_FILE`].
pub const REWIND: &str = "rewind";

/// The parts that a harness needs beside the model.
#[derive(Debug, Clone)]
pub struct HarnessParts {
    /// The labs that this process serves.
    pub labs: Vec<Lab>,
    /// Where the memory, the reports and the response cache live.
    pub state_dir: std::path::PathBuf,
}

/// The five tools, bound to `labs`.
#[must_use]
pub fn tools(labs: &[Lab]) -> Vec<Arc<dyn Tool>> {
    let _ = labs;
    unimplemented!("green phase")
}

/// The per-tool policy: `rewind` asks a person, an unknown tool is denied.
#[must_use]
pub fn policy() -> ToolRules {
    unimplemented!("green phase")
}

/// The flow rule: a write after untrusted input asks a person.
#[must_use]
pub fn flow_rules() -> FlowRules {
    unimplemented!("green phase")
}

/// Build the harness for `labs`, with every store under `state_dir`.
///
/// # Errors
///
/// Returns an error when a store directory cannot be created.
pub fn harness(model: Arc<dyn AgentModel>, parts: &HarnessParts) -> Result<AgentHarness, String> {
    let _ = (model, parts);
    unimplemented!("green phase")
}

/// The `reviewer` peer: read-only, two tools, a small step budget.
#[must_use]
pub fn reviewer() -> Peer {
    unimplemented!("green phase")
}

/// The heartbeat tick that watches the results of `lab`.
#[must_use]
pub fn heartbeat_task(lab: &Path) -> HeartbeatTask {
    let _ = lab;
    unimplemented!("green phase")
}

/// The detached agent that writes the morning report of a session.
#[must_use]
pub fn report_task(lab: &str, tag: &str) -> AgentTask {
    let _ = (lab, tag);
    unimplemented!("green phase")
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use autumn_harvest_agent::{ToolCall, ToolContext, ToolDecision, ToolEffect};
    use serde_json::json;

    use super::*;

    fn parts() -> (tempfile::TempDir, HarnessParts) {
        let dir = tempfile::tempdir().unwrap();
        let lab = Lab::init(dir.path().join("lab")).unwrap();
        let state_dir = dir.path().join("state");
        (
            dir,
            HarnessParts {
                labs: vec![lab],
                state_dir,
            },
        )
    }

    fn context(lab: &Lab) -> ToolContext {
        ToolContext {
            run_id: autumn_harvest_agent::RunId::new("run-1"),
            call_id: "call-1".into(),
            session_id: Some(autumn_harvest_agent::SessionId::new(
                lab.root().display().to_string(),
            )),
            step: 0,
        }
    }

    fn tool(tools: &[Arc<dyn Tool>], name: &str) -> Arc<dyn Tool> {
        tools.iter().find(|t| t.name() == name).cloned().unwrap()
    }

    #[test]
    fn there_are_five_tools_with_their_effects() {
        let (_dir, parts) = parts();
        let tools = tools(&parts.labs);
        assert_eq!(tools.len(), 5);
        assert_eq!(tool(&tools, READ_FILE).effect(), ToolEffect::ReadOnly);
        assert_eq!(tool(&tools, SHOW_DIFF).effect(), ToolEffect::ReadOnly);
        assert_eq!(tool(&tools, READ_PAPER).effect(), ToolEffect::ReadOnly);
        assert!(tool(&tools, READ_PAPER)
            .labels()
            .contains(&autumn_harvest_agent::flow::labels::UNTRUSTED_INPUT.to_string()));
        assert_eq!(tool(&tools, EDIT_TRAIN).effect(), ToolEffect::Write);
        assert_eq!(tool(&tools, REWIND).effect(), ToolEffect::Write);
    }

    #[test]
    fn the_harness_builds_with_its_stores() {
        let (_dir, parts) = parts();
        let model = Arc::new(crate::scripted::ScriptedResearcher::default());
        harness(model, &parts).unwrap();
        assert!(parts.state_dir.join("memory").is_dir());
    }

    #[tokio::test]
    async fn a_tool_serves_only_a_registered_lab() {
        let (_dir, parts) = parts();
        let tools = tools(&parts.labs);
        let read = tool(&tools, READ_FILE);
        let ok = read
            .execute(json!({"path": "train.py"}), &context(&parts.labs[0]))
            .await
            .unwrap();
        assert!(ok["content"].as_str().unwrap().contains("MATRIX_LR"));

        let mut stranger = context(&parts.labs[0]);
        stranger.session_id = Some(autumn_harvest_agent::SessionId::new("/somewhere/else"));
        assert!(read.execute(json!({"path": "train.py"}), &stranger).await.is_err());
        let mut none = context(&parts.labs[0]);
        none.session_id = None;
        assert!(read.execute(json!({"path": "train.py"}), &none).await.is_err());
    }

    #[tokio::test]
    async fn edit_train_writes_train_py_only() {
        let (_dir, parts) = parts();
        let tools = tools(&parts.labs);
        let edit = tool(&tools, EDIT_TRAIN);
        let ctx = context(&parts.labs[0]);
        edit.execute(json!({"old": "DEPTH = 8", "new": "DEPTH = 10"}), &ctx)
            .await
            .unwrap();
        assert!(parts.labs[0].read("train.py").unwrap().contains("DEPTH = 10"));
        // The tool has no path argument, so a path in the input does nothing.
        let prepare = parts.labs[0].read("prepare.py").unwrap();
        let _ = edit
            .execute(
                json!({"path": "prepare.py", "old": "TIME_BUDGET = 300", "new": "TIME_BUDGET = 9"}),
                &ctx,
            )
            .await;
        assert_eq!(parts.labs[0].read("prepare.py").unwrap(), prepare);
    }

    #[test]
    fn the_policy_gates_rewind_and_denies_unknown_tools() {
        let (_dir, parts) = parts();
        let tools = tools(&parts.labs);
        let policy = policy();
        let call = |name: &str| ToolCall {
            id: "c1".into(),
            name: name.into(),
            arguments: json!({}),
        };
        let decide = |name: &str| {
            let known = tools.iter().find(|t| t.name() == name);
            policy.decide_now(&call(name), known.map(AsRef::as_ref))
        };
        assert!(matches!(decide(REWIND), ToolDecision::RequireApproval { .. }));
        assert_eq!(decide(EDIT_TRAIN), ToolDecision::Allow);
        assert_eq!(decide(READ_PAPER), ToolDecision::Allow);
        assert!(matches!(decide("run_shell"), ToolDecision::Deny { .. }));
    }

    #[test]
    fn the_flow_rule_gates_a_write_after_untrusted_input() {
        let rules = flow_rules();
        assert_eq!(rules.rules().len(), 1);
        let labels = rules.rules()[0].labels();
        assert!(labels.contains(autumn_harvest_agent::flow::labels::UNTRUSTED_INPUT));
        assert!(labels.contains("effect:write"));
    }

    #[test]
    fn the_reviewer_is_read_only_with_two_tools() {
        let peer = reviewer();
        assert_eq!(peer.name, REVIEWER);
        assert!(peer.read_only);
        let tools = peer.tools.unwrap();
        assert!(tools.contains(SHOW_DIFF) && tools.contains(READ_FILE));
        assert_eq!(tools.len(), 2);
        assert!(peer.system.unwrap().contains(REVIEWER_ROLE));
    }

    #[test]
    fn the_report_and_heartbeat_tasks_are_read_only_and_name_their_lab() {
        let task = report_task("/labs/a", "oct17");
        assert!(task.read_only && task.deliver);
        assert_eq!(task.session_id.unwrap().as_str(), "/labs/a");
        let tick = heartbeat_task(Path::new("/labs/a"));
        assert_eq!(tick.session_id.unwrap().as_str(), "/labs/a");
        assert!(tick.system.unwrap().contains(WATCHER_ROLE));
    }
}
