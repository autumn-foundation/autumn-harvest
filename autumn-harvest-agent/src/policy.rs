//! Tool policies: decide, per call, whether the agent may act.
//!
//! The model-turn activity asks the [`ToolPolicy`] about every call and
//! records the answer with the reply. Replay reads the record, so the policy
//! is never asked twice about one call.

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::message::{RunId, SessionId, TokenUsage, ToolCall};
use crate::model::BoxFuture;
use crate::tool::{Tool, ToolEffect};

/// Facts about the run a policy decides in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunInfo {
    /// The run id.
    pub run_id: RunId,
    /// The session of the run, if any.
    pub session_id: Option<SessionId>,
    /// Tool rounds used before this turn.
    pub steps_used: u32,
    /// The bound on tool rounds.
    pub max_steps: u32,
    /// Tokens spent so far, this turn included.
    pub usage: TokenUsage,
}

/// A policy's answer for one call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum ToolDecision {
    /// Run the call.
    Allow,
    /// Wait for a person to decide.
    RequireApproval {
        /// Why, for the reviewer.
        reason: String,
    },
    /// Do not run the call. The model reads the reason.
    Deny {
        /// Why, for the model.
        reason: String,
    },
}

/// Decides whether the agent may run a tool call.
pub trait ToolPolicy: Send + Sync + std::fmt::Debug {
    /// Decide on one call. `tool` is `None` when the model named a tool that
    /// does not exist.
    fn decide<'a>(
        &'a self,
        call: &'a ToolCall,
        tool: Option<&'a dyn Tool>,
        info: &'a RunInfo,
    ) -> BoxFuture<'a, ToolDecision>;
}

/// Allows every call. The default policy.
#[derive(Debug, Clone, Copy, Default)]
pub struct AllowAll;

impl ToolPolicy for AllowAll {
    fn decide<'a>(
        &'a self,
        _call: &'a ToolCall,
        _tool: Option<&'a dyn Tool>,
        _info: &'a RunInfo,
    ) -> BoxFuture<'a, ToolDecision> {
        Box::pin(std::future::ready(ToolDecision::Allow))
    }
}

/// How [`ToolRules`] treats a tool or an effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rule {
    /// Run without asking.
    Allow,
    /// Wait for a person.
    Ask,
    /// Refuse, with this reason.
    Deny(String),
}

/// Rules per tool name, then per [`ToolEffect`], then [`Rule::Allow`].
#[derive(Debug, Clone, Default)]
pub struct ToolRules {
    by_name: HashMap<String, Rule>,
    by_effect: HashMap<ToolEffect, Rule>,
}

impl ToolRules {
    /// No rules: every call is allowed.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Rules for an unattended run: read-only and internal tools run, and
    /// every other tool is refused.
    #[must_use]
    pub fn read_only() -> Self {
        let deny = Rule::Deny("this run may only read data and update its own notes".to_owned());
        Self::new()
            .effect(ToolEffect::Write, deny.clone())
            .effect(ToolEffect::External, deny)
    }

    /// Ask a person before any write or external call.
    #[must_use]
    pub fn ask_before_acting() -> Self {
        Self::new()
            .effect(ToolEffect::Write, Rule::Ask)
            .effect(ToolEffect::External, Rule::Ask)
    }

    /// Set the rule for one tool name. It wins over effect rules.
    #[must_use]
    pub fn tool(mut self, name: impl Into<String>, rule: Rule) -> Self {
        self.by_name.insert(name.into(), rule);
        self
    }

    /// Set the rule for every tool with this effect.
    #[must_use]
    pub fn effect(mut self, effect: ToolEffect, rule: Rule) -> Self {
        self.by_effect.insert(effect, rule);
        self
    }

    /// The decision for one call.
    #[must_use]
    pub fn decide_now(&self, call: &ToolCall, tool: Option<&dyn Tool>) -> ToolDecision {
        let rule = self.by_name.get(&call.name).or_else(|| {
            tool.map(Tool::effect)
                .and_then(|effect| self.by_effect.get(&effect))
        });
        match rule {
            None | Some(Rule::Allow) => ToolDecision::Allow,
            Some(Rule::Ask) => ToolDecision::RequireApproval {
                reason: format!("{} needs approval before it runs", call.name),
            },
            Some(Rule::Deny(reason)) => ToolDecision::Deny {
                reason: reason.clone(),
            },
        }
    }
}

impl ToolPolicy for ToolRules {
    fn decide<'a>(
        &'a self,
        call: &'a ToolCall,
        tool: Option<&'a dyn Tool>,
        _info: &'a RunInfo,
    ) -> BoxFuture<'a, ToolDecision> {
        Box::pin(std::future::ready(self.decide_now(call, tool)))
    }
}

/// Combines policies and keeps the strictest answer. Any
/// [`ToolDecision::Deny`] wins, then any [`ToolDecision::RequireApproval`],
/// else [`ToolDecision::Allow`].
///
/// A read-only run uses it to put [`ToolRules::read_only`] on top of the app
/// policy.
#[derive(Debug, Clone)]
pub struct Strictest(Vec<Arc<dyn ToolPolicy>>);

impl Strictest {
    /// Combine these policies.
    #[must_use]
    pub const fn new(policies: Vec<Arc<dyn ToolPolicy>>) -> Self {
        Self(policies)
    }
}

impl ToolPolicy for Strictest {
    fn decide<'a>(
        &'a self,
        call: &'a ToolCall,
        tool: Option<&'a dyn Tool>,
        info: &'a RunInfo,
    ) -> BoxFuture<'a, ToolDecision> {
        Box::pin(async move {
            let mut verdict = ToolDecision::Allow;
            for policy in &self.0 {
                match policy.decide(call, tool, info).await {
                    deny @ ToolDecision::Deny { .. } => return deny,
                    ask @ ToolDecision::RequireApproval { .. } => {
                        if verdict == ToolDecision::Allow {
                            verdict = ask;
                        }
                    }
                    ToolDecision::Allow => {}
                }
            }
            verdict
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tool::FnTool;
    use serde_json::json;

    fn call(name: &str) -> ToolCall {
        ToolCall {
            id: "c".into(),
            name: name.into(),
            arguments: json!({}),
        }
    }

    fn tool(name: &str, effect: ToolEffect) -> FnTool {
        FnTool::new(name, "t", json!({}), |_| async { Ok(json!(null)) }).effect(effect)
    }

    #[test]
    fn a_name_rule_wins_over_an_effect_rule() {
        let rules = ToolRules::ask_before_acting().tool("send", Rule::Allow);
        let send = tool("send", ToolEffect::External);
        assert_eq!(
            rules.decide_now(&call("send"), Some(&send)),
            ToolDecision::Allow
        );
        let write = tool("write", ToolEffect::Write);
        assert!(matches!(
            rules.decide_now(&call("write"), Some(&write)),
            ToolDecision::RequireApproval { .. }
        ));
    }

    #[test]
    fn read_only_rules_refuse_writes_and_allow_reads() {
        let rules = ToolRules::read_only();
        let read = tool("read", ToolEffect::ReadOnly);
        let write = tool("write", ToolEffect::Write);
        assert_eq!(
            rules.decide_now(&call("read"), Some(&read)),
            ToolDecision::Allow
        );
        assert!(matches!(
            rules.decide_now(&call("write"), Some(&write)),
            ToolDecision::Deny { .. }
        ));
        assert_eq!(rules.decide_now(&call("ghost"), None), ToolDecision::Allow);
    }

    #[tokio::test]
    async fn the_strictest_answer_wins() {
        let info = RunInfo {
            run_id: RunId::new("r"),
            session_id: None,
            steps_used: 0,
            max_steps: 1,
            usage: TokenUsage::default(),
        };
        let write = tool("write", ToolEffect::Write);
        let ask: Arc<dyn ToolPolicy> = Arc::new(ToolRules::ask_before_acting());
        let deny: Arc<dyn ToolPolicy> = Arc::new(ToolRules::read_only());
        let both = Strictest::new(vec![ask.clone(), deny]);
        assert!(matches!(
            both.decide(&call("write"), Some(&write), &info).await,
            ToolDecision::Deny { .. }
        ));
        let only_ask = Strictest::new(vec![Arc::new(AllowAll), ask]);
        assert!(matches!(
            only_ask.decide(&call("write"), Some(&write), &info).await,
            ToolDecision::RequireApproval { .. }
        ));
        let none = Strictest::new(Vec::new());
        assert_eq!(
            none.decide(&call("x"), None, &info).await,
            ToolDecision::Allow
        );
    }

    #[test]
    fn the_decision_shape_is_stable_in_history() {
        assert_eq!(
            json!(ToolDecision::Deny { reason: "r".into() }),
            json!({"decision": "deny", "reason": "r"})
        );
    }
}
