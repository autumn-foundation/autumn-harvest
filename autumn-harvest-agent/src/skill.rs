//! Skills: domain know-how that a run loads on demand (issue #2373).
//!
//! A skill has a name, a one-line summary and a body. The system prompt
//! lists each name and summary under [`SKILLS_HEADING`]. It never holds a
//! body. A run with skills gets the [`LOAD_SKILL_TOOL`] tool. A call returns
//! the body as an ordinary recorded `agent_tool_call`, so replay reads it
//! from history. The design is in `docs/design/DESIGN-2373.md`.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use crate::error::{AgentError, ErrorKind};
use crate::harness::{cut_bytes, max_tool_result_bytes};
use crate::mesh::valid_name;
use crate::message::ToolDefinition;
use crate::model::BoxFuture;
use crate::tool::{Tool, ToolContext, ToolEffect};
use crate::types::SkillLookup;

/// The name of the tool that loads a skill. The harness never shows an app
/// tool with this name.
pub const LOAD_SKILL_TOOL: &str = "load_skill";

/// The heading of the skill list in the system prompt.
pub const SKILLS_HEADING: &str = "## Skills";

/// The most characters in the summary of one skill.
pub const MAX_SKILL_SUMMARY_CHARS: usize = 200;

/// The text of one skill.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillText {
    /// One line that tells the model when to load the skill. The system
    /// prompt holds it.
    pub summary: String,
    /// The know-how. Only a `load_skill` call returns it.
    pub body: String,
}

impl SkillText {
    /// A skill with `summary` and `body`.
    #[must_use]
    pub fn new(summary: impl Into<String>, body: impl Into<String>) -> Self {
        Self {
            summary: summary.into(),
            body: body.into(),
        }
    }
}

/// Why the skill `name` with `text` is not valid, or `Ok`.
///
/// A name follows [`valid_name`]. It joins the system prompt, so it carries
/// no other text. A summary is one line of 1 to
/// [`MAX_SKILL_SUMMARY_CHARS`] characters. So the listing stays small, and
/// a body cannot hide in it.
///
/// # Errors
///
/// Returns the reason as a static text.
pub fn check(name: &str, text: &SkillText) -> Result<(), &'static str> {
    if !valid_name(name) {
        Err("the name must hold 1 to 64 of a-z, 0-9, _ and -")
    } else if text.summary.trim().is_empty() {
        Err("the summary is empty")
    } else if text.summary.contains(['\n', '\r']) {
        Err("the summary has a line break")
    } else if text.summary.chars().count() > MAX_SKILL_SUMMARY_CHARS {
        Err("the summary is too long")
    } else {
        Ok(())
    }
}

/// The first skill of `skills` that is not valid, with the reason.
pub(crate) fn first_invalid(
    skills: &BTreeMap<String, SkillText>,
) -> Option<(&String, &'static str)> {
    skills
        .iter()
        .find_map(|(name, text)| check(name, text).err().map(|reason| (name, reason)))
}

/// The skill list of the system prompt, or `None` with no skills. It holds
/// each name and summary, in name order. It holds no body.
pub(crate) fn listing(skills: &BTreeMap<String, SkillText>) -> Option<String> {
    if skills.is_empty() {
        return None;
    }
    let mut text =
        format!("{SKILLS_HEADING}\n\nCall `{LOAD_SKILL_TOOL}` with a name to read a skill.\n");
    for (name, skill) in skills {
        // A write to a `String` cannot fail.
        let _ = write!(text, "\n- `{name}`: {}", skill.summary);
    }
    Some(text)
}

/// The skill name that a `load_skill` call asks for, if any.
pub(crate) fn requested(arguments: &serde_json::Value) -> Option<&str> {
    arguments.get("name").and_then(serde_json::Value::as_str)
}

/// What `skills` hold for a `load_skill` call with `arguments`.
///
/// The body is cut to fit half of the request cap `cap`. The other half
/// holds the rest of the tool-call input. A cap of zero is no cap.
pub(crate) fn lookup(
    skills: &BTreeMap<String, SkillText>,
    arguments: &serde_json::Value,
    cap: u64,
) -> SkillLookup {
    requested(arguments)
        .and_then(|name| skills.get(name))
        .map_or(SkillLookup::Missing, |skill| {
            let max = max_tool_result_bytes(cap / 2);
            SkillLookup::Found(cut_bytes(skill.body.clone(), max))
        })
}

/// The description of the `load_skill` tool.
const SKILL_TOOL_DESCRIPTION: &str =
    "Load the text of one skill. The system prompt lists the skills.";

/// The definition of the `load_skill` tool.
pub(crate) fn definition() -> ToolDefinition {
    ToolDefinition {
        name: LOAD_SKILL_TOOL.to_owned(),
        description: SKILL_TOOL_DESCRIPTION.to_owned(),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "The name of the skill."}
            },
            "required": ["name"],
            "additionalProperties": false
        }),
    }
}

/// The built-in `load_skill` tool, for the decision of a turn.
///
/// Its effect is [`ToolEffect::ReadOnly`]. The harness answers a call from
/// [`ToolCallRequest::skill`](crate::ToolCallRequest), so
/// [`execute`](Tool::execute) never runs.
#[derive(Debug)]
pub(crate) struct SkillTool;

impl Tool for SkillTool {
    fn name(&self) -> &str {
        LOAD_SKILL_TOOL
    }

    fn description(&self) -> &'static str {
        SKILL_TOOL_DESCRIPTION
    }

    fn input_schema(&self) -> serde_json::Value {
        definition().input_schema
    }

    fn effect(&self) -> ToolEffect {
        ToolEffect::ReadOnly
    }

    fn execute<'a>(
        &'a self,
        _input: serde_json::Value,
        _ctx: &'a ToolContext,
    ) -> BoxFuture<'a, Result<serde_json::Value, AgentError>> {
        let err = AgentError::new(
            ErrorKind::Tool,
            "load_skill reads the tool-call input, not this method",
        );
        Box::pin(std::future::ready(Err(err)))
    }

    fn definition(&self) -> ToolDefinition {
        definition()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn skills() -> BTreeMap<String, SkillText> {
        BTreeMap::from([
            ("triage".to_owned(), SkillText::new("Sort.", "Step one.")),
            (
                "refund".to_owned(),
                SkillText::new("Pay.", "x".repeat(10_000)),
            ),
        ])
    }

    #[test]
    fn the_listing_holds_names_and_summaries_only() {
        let text = listing(&skills()).expect("two skills");
        assert_eq!(
            text,
            "## Skills\n\nCall `load_skill` with a name to read a skill.\n\n- `refund`: Pay.\n- `triage`: Sort."
        );
        assert_eq!(listing(&BTreeMap::new()), None);
    }

    #[test]
    fn a_lookup_finds_a_body_or_reports_it_missing() {
        let skills = skills();
        assert_eq!(
            lookup(&skills, &json!({"name": "triage"}), 0),
            SkillLookup::Found("Step one.".to_owned())
        );
        assert_eq!(
            lookup(&skills, &json!({"name": "nope"}), 0),
            SkillLookup::Missing
        );
        assert_eq!(
            lookup(&skills, &json!({"skill": "triage"}), 0),
            SkillLookup::Missing
        );
        assert_eq!(lookup(&skills, &json!("triage"), 0), SkillLookup::Missing);
    }

    #[test]
    fn a_lookup_cuts_the_body_to_half_the_cap() {
        let SkillLookup::Found(body) = lookup(&skills(), &json!({"name": "refund"}), 4_096) else {
            panic!("refund is found");
        };
        assert!(body.len() <= max_tool_result_bytes(2_048), "{}", body.len());
        assert!(body.ends_with("…[truncated]"));
    }

    #[test]
    fn the_tool_only_reads_and_never_runs() {
        assert_eq!(SkillTool.effect(), ToolEffect::ReadOnly);
        assert_eq!(SkillTool.definition(), definition());
        assert!(!SkillTool.interruptible() && !SkillTool.uses_workspace());
        let ctx = ToolContext {
            run_id: crate::message::RunId::new("r"),
            call_id: "c".into(),
            session_id: None,
            step: 0,
            workspace: None,
        };
        let ran = futures::executor::block_on(SkillTool.execute(json!({}), &ctx));
        assert!(ran.is_err());
    }
}
