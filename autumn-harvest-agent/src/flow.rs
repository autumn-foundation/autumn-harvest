//! Flow rules: refuse a sequence of tool calls that is safe call by call but
//! dangerous as a whole.
//!
//! Each tool carries labels, such as [`labels::UNTRUSTED_INPUT`]. Each call
//! also carries the label of its [`ToolEffect`]. A run tree records the labels
//! of every call that ran. A [`FlowRule`] names a label set that one run tree
//! must not complete without a person.
//!
//! The workflow holds the exercised labels, and the model-turn activity
//! records each decision. Model output cannot remove a label. Replay reads the
//! recorded decision and does not check the rules again.

use std::collections::BTreeSet;

use crate::policy::{Rule, ToolDecision};
use crate::tool::{Tool, ToolEffect};

/// Well-known tool labels.
pub mod labels {
    /// The tool returns content that someone outside the app controls, such
    /// as a web page, an email or a file upload.
    pub const UNTRUSTED_INPUT: &str = "untrusted_input";
    /// The tool returns private or sensitive data.
    pub const PRIVATE_DATA: &str = "private_data";
}

/// The label that every call to a tool with `effect` carries.
#[must_use]
pub const fn effect_label(effect: ToolEffect) -> &'static str {
    match effect {
        ToolEffect::ReadOnly => "effect:read_only",
        ToolEffect::Internal => "effect:internal",
        ToolEffect::Write => "effect:write",
        ToolEffect::External => "effect:external",
    }
}

/// The labels of one call: the tool labels and the effect label. A call to
/// an unknown tool has no labels.
#[must_use]
pub fn call_labels(tool: Option<&dyn Tool>) -> BTreeSet<String> {
    tool.map(|tool| {
        tool.labels()
            .iter()
            .cloned()
            .chain(std::iter::once(effect_label(tool.effect()).to_owned()))
            .collect()
    })
    .unwrap_or_default()
}

/// One label set that a run tree must not complete without a person.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowRule {
    labels: BTreeSet<String>,
    rule: Rule,
}

impl FlowRule {
    /// The labels of the rule.
    #[must_use]
    pub const fn labels(&self) -> &BTreeSet<String> {
        &self.labels
    }

    /// What happens to the call that completes the set.
    #[must_use]
    pub const fn rule(&self) -> &Rule {
        &self.rule
    }
}

/// The flow rules of a harness.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FlowRules {
    rules: Vec<FlowRule>,
}

impl FlowRules {
    /// No rules.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a rule. A call that completes `labels` in one run tree gets
    /// `rule`. [`Rule::Allow`] and an empty set add nothing.
    #[must_use]
    pub fn forbid<I, S>(self, labels: I, rule: Rule) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let labels: BTreeSet<String> = labels.into_iter().map(Into::into).collect();
        if labels.is_empty() || rule == Rule::Allow {
            return self;
        }
        let mut rules = self.rules;
        rules.push(FlowRule { labels, rule });
        Self { rules }
    }

    /// The Agents Rule of Two: a run tree holds at most two of untrusted
    /// input, private data, and a write or external effect. A call that
    /// completes all three waits for a person.
    #[must_use]
    pub fn rule_of_two() -> Self {
        let read = [labels::UNTRUSTED_INPUT, labels::PRIVATE_DATA];
        Self::new()
            .forbid(
                read.into_iter().chain([effect_label(ToolEffect::Write)]),
                Rule::Ask,
            )
            .forbid(
                read.into_iter().chain([effect_label(ToolEffect::External)]),
                Rule::Ask,
            )
    }

    /// Whether the harness has no rules.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// The rules.
    #[must_use]
    pub fn rules(&self) -> &[FlowRule] {
        &self.rules
    }

    /// The decision for a call with `call` labels, after the run tree has
    /// exercised `exercised`.
    ///
    /// A rule trips when its set is inside `exercised` plus `call`, and the
    /// call adds at least one label of the set. The strictest tripped rule
    /// wins. With no tripped rule, the answer is [`ToolDecision::Allow`].
    #[must_use]
    pub fn check(&self, exercised: &BTreeSet<String>, call: &BTreeSet<String>) -> ToolDecision {
        let mut verdict = ToolDecision::Allow;
        for rule in &self.rules {
            let adds = rule.labels.iter().any(|label| call.contains(label));
            let completes = rule
                .labels
                .iter()
                .all(|label| exercised.contains(label) || call.contains(label));
            if !(adds && completes) {
                continue;
            }
            let tripped = match &rule.rule {
                Rule::Deny(reason) => {
                    return ToolDecision::Deny {
                        reason: reason.clone(),
                    };
                }
                Rule::Ask => ToolDecision::RequireApproval {
                    reason: format!(
                        "this call completes the flow labels {}",
                        rule.labels.iter().cloned().collect::<Vec<_>>().join(", ")
                    ),
                },
                Rule::Allow => continue,
            };
            if verdict == ToolDecision::Allow {
                verdict = tripped;
            }
        }
        verdict
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tool::FnTool;
    use serde_json::json;

    fn set(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    }

    #[test]
    fn every_effect_has_a_distinct_label() {
        let all = [
            ToolEffect::ReadOnly,
            ToolEffect::Internal,
            ToolEffect::Write,
            ToolEffect::External,
        ];
        let names: BTreeSet<&str> = all.iter().map(|effect| effect_label(*effect)).collect();
        assert_eq!(names.len(), all.len());
        assert_eq!(effect_label(ToolEffect::External), "effect:external");
        assert_eq!(effect_label(ToolEffect::Write), "effect:write");
    }

    #[test]
    fn a_call_carries_its_tool_labels_and_its_effect() {
        let tool = FnTool::new("inbox", "Read mail.", json!({}), |_| async {
            Ok(json!(null))
        })
        .effect(ToolEffect::ReadOnly)
        .label(labels::UNTRUSTED_INPUT)
        .label(labels::PRIVATE_DATA);
        assert_eq!(
            call_labels(Some(&tool)),
            set(&["effect:read_only", "private_data", "untrusted_input"])
        );
        assert_eq!(call_labels(None), BTreeSet::new());
    }

    #[test]
    fn a_rule_trips_only_on_the_call_that_completes_it() {
        let rules = FlowRules::new().forbid(["a", "b"], Rule::Deny("no a and b".into()));
        assert_eq!(rules.check(&set(&[]), &set(&["a"])), ToolDecision::Allow);
        assert_eq!(
            rules.check(&set(&["a"]), &set(&["b"])),
            ToolDecision::Deny {
                reason: "no a and b".into()
            }
        );
        assert_eq!(
            rules.check(&set(&[]), &set(&["a", "b"])),
            ToolDecision::Deny {
                reason: "no a and b".into()
            }
        );
        // A call that adds no label of the set passes, even after the set is
        // complete.
        assert_eq!(
            rules.check(&set(&["a", "b"]), &set(&["c"])),
            ToolDecision::Allow
        );
    }

    #[test]
    fn the_strictest_tripped_rule_wins() {
        let rules = FlowRules::new()
            .forbid(["a", "b"], Rule::Ask)
            .forbid(["b", "c"], Rule::Deny("never".into()));
        assert!(matches!(
            rules.check(&set(&["a"]), &set(&["b"])),
            ToolDecision::RequireApproval { .. }
        ));
        assert!(matches!(
            rules.check(&set(&["a", "c"]), &set(&["b"])),
            ToolDecision::Deny { .. }
        ));
    }

    #[test]
    fn allow_rules_and_empty_sets_add_nothing() {
        let rules = FlowRules::new()
            .forbid(["a"], Rule::Allow)
            .forbid(Vec::<String>::new(), Rule::Ask);
        assert!(rules.is_empty());
    }

    #[test]
    fn the_rule_of_two_gates_the_third_property() {
        let rules = FlowRules::rule_of_two();
        assert_eq!(rules.rules().len(), 2);
        let read = set(&[labels::UNTRUSTED_INPUT, labels::PRIVATE_DATA]);
        assert!(matches!(
            rules.check(&read, &set(&["effect:external"])),
            ToolDecision::RequireApproval { .. }
        ));
        assert!(matches!(
            rules.check(&read, &set(&["effect:write"])),
            ToolDecision::RequireApproval { .. }
        ));
        // Two of the three pass.
        assert_eq!(
            rules.check(&set(&[labels::PRIVATE_DATA]), &set(&["effect:external"])),
            ToolDecision::Allow
        );
        assert_eq!(
            rules.check(&read, &set(&["effect:read_only"])),
            ToolDecision::Allow
        );
    }
}
