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
//!
//! A tool call in flight at a crash runs again. `edit_train` keys each edit
//! on the run, the step and the call id, so the re-run changes nothing.
//! `rewind` resets to a fixed commit, so a re-run is harmless.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest_agent::flow::{effect_label, labels};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentModel, AgentTask, ErrorKind, FlowRules, FnTool, HeartbeatTask,
    InMemoryCacheIndex, Peer, ResponseCache, Rule, SessionId, Tool, ToolContext, ToolEffect,
    ToolRules,
};
use serde_json::{Value, json};

use crate::lab::Lab;
use crate::stores::{DirPayloadStore, FileDelivery, FileMemoryStore, ResultsPrecheck};

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

/// The system prompt of the reviewer peer.
const REVIEWER_SYSTEM: &str = "ROLE: reviewer\n\
You review one diff of train.py against the simplicity rule. A small gain \
that adds many lines is not worth it. Read the diff with show_diff. Answer \
`APPROVE: <reason>` or `REJECT: <reason>` on one line.";

/// The system prompt of the heartbeat watcher.
const WATCHER_SYSTEM: &str = "ROLE: watcher\n\
You watch a research lab while the human sleeps. Read results.tsv. Reply \
HEARTBEAT_OK when nothing needs a person. Reply `ALERT: <reason>` when \
three runs in a row crashed.";

/// The system prompt of the morning report writer.
const REPORTER_SYSTEM: &str = "ROLE: reporter\n\
You write the morning report of one night of research. Read results.tsv. \
Name the number of experiments, the kept ones and the best result.";

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
    let labs: Arc<Vec<Lab>> = Arc::new(labs.to_vec());
    let schema = |properties: Value, required: &[&str]| json!({"type": "object", "properties": properties, "required": required});
    vec![
        lab_tool(
            &labs,
            READ_FILE,
            "Read one lab file: train.py, prepare.py, program.md, results.tsv, README.md or run.log.",
            schema(json!({"path": {"type": "string"}}), &["path"]),
            |lab, input, _| {
                let path = text(&input, "path")?;
                Ok(json!({"path": path, "content": lab.read(&path)?}))
            },
        )
        .effect(ToolEffect::ReadOnly)
        .shared(),
        lab_tool(
            &labs,
            READ_PAPER,
            "Read one note from papers/, by name without .md. Its text comes from outside the lab.",
            schema(json!({"name": {"type": "string"}}), &["name"]),
            |lab, input, _| {
                let name = text(&input, "name")?;
                Ok(json!({"name": name, "content": lab.read_paper(&name)?}))
            },
        )
        .effect(ToolEffect::ReadOnly)
        .label(labels::UNTRUSTED_INPUT)
        .shared(),
        lab_tool(
            &labs,
            SHOW_DIFF,
            "Show the diff of train.py against the last commit.",
            schema(json!({}), &[]),
            |lab, _, _| Ok(json!({"diff": lab.diff_train()?})),
        )
        .effect(ToolEffect::ReadOnly)
        .shared(),
        lab_tool(
            &labs,
            EDIT_TRAIN,
            "Replace the one occurrence of `old` in train.py with `new`. This is the only file you can change.",
            schema(
                json!({"old": {"type": "string"}, "new": {"type": "string"}}),
                &["old", "new"],
            ),
            |lab, input, ctx| {
                let call = format!("{}/{}/{}", ctx.run_id.as_str(), ctx.step, ctx.call_id);
                lab.edit_train_once(&text(&input, "old")?, &text(&input, "new")?, &call)?;
                Ok(json!({"ok": true, "diff": lab.diff_train()?}))
            },
        )
        .effect(ToolEffect::Write)
        .shared(),
        lab_tool(
            &labs,
            REWIND,
            "Reset the research branch to an older kept commit. Use it very rarely. A person must approve it.",
            schema(
                json!({"commit": {"type": "string"}, "reason": {"type": "string"}}),
                &["commit"],
            ),
            |lab, input, _| {
                let commit = text(&input, "commit")?;
                let kept = lab
                    .kept(&commit)?
                    .ok_or_else(|| format!("{commit} is not a kept commit in results.tsv"))?;
                // A kept commit of another session in the lab is on another
                // branch. Only an earlier commit of this branch is a rewind.
                lab.git(&["merge-base", "--is-ancestor", &commit, "HEAD"])
                    .map_err(|_| format!("{commit} is not an earlier commit of this branch"))?;
                lab.reset_to(&commit)?;
                Ok(json!({"ok": true, "rewound_to": commit, "val_bpb": kept.val_bpb}))
            },
        )
        .effect(ToolEffect::Write)
        .shared(),
    ]
}

/// A tool whose body runs on the lab that the call's session names.
fn lab_tool(
    labs: &Arc<Vec<Lab>>,
    name: &str,
    description: &str,
    schema: Value,
    body: fn(&Lab, Value, &ToolContext) -> Result<Value, String>,
) -> FnTool {
    let labs = Arc::clone(labs);
    FnTool::with_context(name, description, schema, move |input, ctx: ToolContext| {
        let result = lab_for(&labs, &ctx).and_then(|lab| {
            body(lab, input, &ctx).map_err(|e| AgentError::new(ErrorKind::Tool, e))
        });
        std::future::ready(result)
    })
}

/// The lab that `ctx` names. A call with no session, or a session that this
/// process does not serve, is refused.
fn lab_for<'a>(labs: &'a [Lab], ctx: &ToolContext) -> Result<&'a Lab, AgentError> {
    let session = ctx.session_id.as_ref().map(SessionId::as_str);
    Lab::serving(labs, session).ok_or_else(|| {
        AgentError::new(
            ErrorKind::Config,
            format!("the run names lab {session:?}, which this process does not serve"),
        )
    })
}

/// The string argument `key` of a tool call.
fn text(input: &Value, key: &str) -> Result<String, String> {
    input
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("the argument {key:?} must be a string"))
}

/// The per-tool policy: `rewind` asks a person, an unknown tool is denied.
#[must_use]
pub fn policy() -> ToolRules {
    ToolRules::new()
        .tool(REWIND, Rule::Ask)
        .unknown(Rule::Deny("this lab has no such tool".into()))
}

/// The flow rule: a write after untrusted input asks a person.
#[must_use]
pub fn flow_rules() -> FlowRules {
    FlowRules::new().forbid(
        [labels::UNTRUSTED_INPUT, effect_label(ToolEffect::Write)],
        Rule::Ask,
    )
}

/// Build the harness for `labs`, with every store under `state_dir`.
///
/// # Errors
///
/// Returns an error when a store directory cannot be created.
pub fn harness(model: Arc<dyn AgentModel>, parts: &HarnessParts) -> Result<AgentHarness, String> {
    let memory = FileMemoryStore::new(parts.state_dir.join("memory"))?;
    let blobs = DirPayloadStore::new(parts.state_dir.join("cache"))?;
    // An identical request within a day gets the stored answer. The index
    // lives in memory, so a restart starts it empty.
    let cache = ResponseCache::new(
        Arc::new(blobs),
        Arc::new(InMemoryCacheIndex::new(1_024)),
        PayloadCodecs::default(),
    )
    .max_age(Duration::from_secs(24 * 3_600));
    Ok(AgentHarness::new(model)
        .tools(tools(&parts.labs))
        .policy(Arc::new(policy()))
        .flow_rules(flow_rules())
        .peer(reviewer())
        .memory(Arc::new(memory))
        .delivery(Arc::new(FileDelivery::new(parts.labs.clone())))
        .precheck(Arc::new(ResultsPrecheck::new(parts.labs.clone())))
        .response_cache(cache))
}

/// The `reviewer` peer: read-only, two tools, a small step budget.
#[must_use]
pub fn reviewer() -> Peer {
    Peer::new(
        REVIEWER,
        "Judges a diff of train.py against the simplicity rule.",
    )
    .system(REVIEWER_SYSTEM)
    .tools([SHOW_DIFF, READ_FILE])
    .max_steps(2)
    .read_only()
}

/// The heartbeat tick that watches the results of `lab`.
#[must_use]
pub fn heartbeat_task(lab: &Path) -> HeartbeatTask {
    HeartbeatTask::new()
        .system(WATCHER_SYSTEM)
        .prompt("Check results.tsv now.")
        .session(SessionId::new(lab.display().to_string()))
}

/// The detached agent that writes the morning report of a session.
#[must_use]
pub fn report_task(lab: &str, tag: &str) -> AgentTask {
    AgentTask::new(format!(
        "The session {}{tag} has ended. Write the morning report.",
        crate::lab::BRANCH_PREFIX
    ))
    .system(REPORTER_SYSTEM)
    .session(SessionId::new(lab))
    .max_steps(2)
    .read_only()
    .deliver()
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
        assert!(
            tool(&tools, READ_PAPER)
                .labels()
                .contains(&autumn_harvest_agent::flow::labels::UNTRUSTED_INPUT.to_string())
        );
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
        assert!(
            read.execute(json!({"path": "train.py"}), &stranger)
                .await
                .is_err()
        );
        let mut none = context(&parts.labs[0]);
        none.session_id = None;
        assert!(
            read.execute(json!({"path": "train.py"}), &none)
                .await
                .is_err()
        );
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
        assert!(
            parts.labs[0]
                .read("train.py")
                .unwrap()
                .contains("DEPTH = 10")
        );
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

    #[tokio::test]
    async fn an_edit_call_that_runs_again_changes_nothing() {
        let (_dir, parts) = parts();
        let tools = tools(&parts.labs);
        let edit = tool(&tools, EDIT_TRAIN);
        let ctx = context(&parts.labs[0]);
        let insert = json!({"old": "DEPTH = 8", "new": "DEPTH = 8\nEXTRA_KNOB = 1"});
        edit.execute(insert.clone(), &ctx).await.unwrap();
        // The same call after a crash.
        edit.execute(insert.clone(), &ctx).await.unwrap();
        let train = parts.labs[0].read("train.py").unwrap();
        assert_eq!(train.matches("EXTRA_KNOB").count(), 1);
        // The same call id in a later step is a new call.
        let mut later = ctx.clone();
        later.step = 1;
        edit.execute(insert, &later).await.unwrap();
        let train = parts.labs[0].read("train.py").unwrap();
        assert_eq!(train.matches("EXTRA_KNOB").count(), 2);
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
        assert!(matches!(
            decide(REWIND),
            ToolDecision::RequireApproval { .. }
        ));
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
