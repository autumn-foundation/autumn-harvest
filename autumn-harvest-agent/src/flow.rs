//! Flow rules: refuse a sequence of tool calls that is safe call by call but
//! dangerous as a whole.
//!
//! Each tool carries labels, such as [`labels::UNTRUSTED_INPUT`]. Each call
//! also carries the label of its [`ToolEffect`]. A run tree records the labels
//! of every call that the decision lets run. A [`FlowRule`] names a label set
//! and what happens to the call that completes it: a person decides, or the
//! call is refused. After that, each call with a label of the set trips the
//! rule again. Under [`Recheck::EachCall`], the default, one approval releases
//! one call. Under [`Recheck::UntilApproved`], one approval releases the rule
//! for the rest of the run tree.
//!
//! The workflow holds the exercised labels and the released rules. The
//! model-turn activity records each decision. Model output cannot remove a
//! label or release a rule. Replay reads the recorded decision and does not
//! check the rules again.
//!
//! A run that continues another run must also continue its labels. Pass
//! [`AgentReport::flow_labels`](crate::AgentReport::flow_labels) to
//! [`AgentTask::flow_labels`](crate::AgentTask::flow_labels), as you pass the
//! transcript to the history.

use std::collections::BTreeSet;

use crate::policy::{Rule, ToolDecision};

/// The rules that an approval releases, each named by its label set.
pub type Released = BTreeSet<BTreeSet<String>>;
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
pub(crate) fn call_labels(tool: Option<&dyn Tool>) -> BTreeSet<String> {
    tool.map(|tool| {
        tool.labels()
            .iter()
            .cloned()
            .chain(std::iter::once(effect_label(tool.effect()).to_owned()))
            .collect()
    })
    .unwrap_or_default()
}

/// Which calls a rule gates after its label set is complete in a run tree.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Recheck {
    /// Each later call with a label of the set trips the rule again. One
    /// approval releases one call. This is the default.
    #[default]
    EachCall,
    /// The rule trips as under [`Recheck::EachCall`] until a person approves
    /// a call that trips it. The approval releases the rule for the rest of
    /// the run tree. A later send can then carry data from a later prompt
    /// injection.
    ///
    /// Only an approval releases the rule. A rejection, a timeout, a call
    /// that waits in the same turn, or labels from another run release
    /// nothing. A [`Rule::Deny`] rule never asks, so it is never released.
    UntilApproved,
}

/// One label set, and what happens to the call that completes it in one run
/// tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowRule {
    labels: BTreeSet<String>,
    rule: Rule,
    recheck: Recheck,
}

impl FlowRule {
    /// The labels of the rule.
    #[must_use]
    pub const fn labels(&self) -> &BTreeSet<String> {
        &self.labels
    }

    /// What happens to a call that trips the rule.
    #[must_use]
    pub const fn rule(&self) -> &Rule {
        &self.rule
    }

    /// Which calls the rule gates after its set is complete.
    #[must_use]
    pub const fn recheck(&self) -> Recheck {
        self.recheck
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

    /// Add a rule with [`Recheck::EachCall`]. A call that completes `labels`
    /// in one run tree gets `rule`. Each later call with one of `labels` gets
    /// `rule` too. [`Rule::Allow`] and an empty set add nothing.
    #[must_use]
    pub fn forbid<I, S>(self, labels: I, rule: Rule) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.forbid_with(labels, rule, Recheck::EachCall)
    }

    /// Add a rule with the given `recheck`. [`Rule::Allow`] and an empty set
    /// add nothing.
    ///
    /// Two rules with the same label set share one release.
    #[must_use]
    pub fn forbid_with<I, S>(self, labels: I, rule: Rule, recheck: Recheck) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let labels: BTreeSet<String> = labels.into_iter().map(Into::into).collect();
        if labels.is_empty() || rule == Rule::Allow {
            return self;
        }
        let mut rules = self.rules;
        rules.push(FlowRule {
            labels,
            rule,
            recheck,
        });
        Self { rules }
    }

    /// Set `recheck` on every rule that the harness has now.
    #[must_use]
    pub fn with_recheck(self, recheck: Recheck) -> Self {
        let rules = self
            .rules
            .into_iter()
            .map(|rule| FlowRule { recheck, ..rule })
            .collect();
        Self { rules }
    }

    /// The Agents Rule of Two. A run tree needs a person before it holds all
    /// three of untrusted input, private data, and a write or external
    /// effect. The call that completes the three waits for a person. Each
    /// later call with one of the labels waits too, so each send needs its own
    /// approval. [`FlowRules::with_recheck`] with [`Recheck::UntilApproved`]
    /// asks once per run tree instead.
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

    /// Whether an approval can release a rule of the harness.
    #[must_use]
    pub fn can_release(&self) -> bool {
        self.rules
            .iter()
            .any(|rule| rule.recheck == Recheck::UntilApproved && rule.rule == Rule::Ask)
    }

    /// The decision for a call with `call` labels, after the run tree has
    /// exercised `exercised` and a person has released `released`.
    ///
    /// A rule trips when its set is inside `exercised` plus `call`, and the
    /// call carries at least one label of the set. The label can already be
    /// in `exercised`. A complete set thus gates each later call that touches
    /// it. A [`Recheck::UntilApproved`] rule in `released` does not trip. The
    /// strictest tripped rule wins. With no tripped rule, the answer is
    /// [`ToolDecision::Allow`].
    #[must_use]
    pub fn check(
        &self,
        exercised: &BTreeSet<String>,
        released: &Released,
        call: &BTreeSet<String>,
    ) -> ToolDecision {
        self.decide(exercised, released, call).0
    }

    /// [`check`](Self::check), and the rules that an approval of the call
    /// releases. The set is empty unless the decision asks a person.
    pub(crate) fn decide(
        &self,
        exercised: &BTreeSet<String>,
        released: &Released,
        call: &BTreeSet<String>,
    ) -> (ToolDecision, Released) {
        let mut verdict = ToolDecision::Allow;
        let mut freed = Released::new();
        for rule in &self.rules {
            let until_approved = rule.recheck == Recheck::UntilApproved;
            if until_approved && released.contains(&rule.labels) {
                continue;
            }
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
                    return (
                        ToolDecision::Deny {
                            reason: reason.clone(),
                        },
                        Released::new(),
                    );
                }
                Rule::Ask => ToolDecision::RequireApproval {
                    reason: format!(
                        "this call completes the flow labels {}",
                        rule.labels.iter().cloned().collect::<Vec<_>>().join(", ")
                    ),
                },
                Rule::Allow => continue,
            };
            if until_approved {
                freed.insert(rule.labels.clone());
            }
            if verdict == ToolDecision::Allow {
                verdict = tripped;
            }
        }
        (verdict, freed)
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

    fn none() -> Released {
        Released::new()
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
    fn a_rule_trips_on_the_call_that_completes_it() {
        let rules = FlowRules::new().forbid(["a", "b"], Rule::Deny("no a and b".into()));
        assert_eq!(
            rules.check(&set(&[]), &none(), &set(&["a"])),
            ToolDecision::Allow
        );
        assert_eq!(
            rules.check(&set(&["a"]), &none(), &set(&["b"])),
            ToolDecision::Deny {
                reason: "no a and b".into()
            }
        );
        assert_eq!(
            rules.check(&set(&[]), &none(), &set(&["a", "b"])),
            ToolDecision::Deny {
                reason: "no a and b".into()
            }
        );
        // A call that adds no label of the set passes, even after the set is
        // complete.
        assert_eq!(
            rules.check(&set(&["a", "b"]), &none(), &set(&["c"])),
            ToolDecision::Allow
        );
    }

    #[test]
    fn a_complete_set_gates_each_later_call_that_carries_one_of_its_labels() {
        let rules = FlowRules::new().forbid(["a", "b"], Rule::Ask);
        let ask = ToolDecision::RequireApproval {
            reason: "this call completes the flow labels a, b".into(),
        };
        // One approval releases one call. A later call with a label of the
        // set asks again, even when the tree already holds that label.
        assert_eq!(rules.check(&set(&["a", "b"]), &none(), &set(&["a"])), ask);
        assert_eq!(
            rules.check(&set(&["a", "b"]), &none(), &set(&["b", "c"])),
            ask
        );
        assert_eq!(
            rules.check(&set(&["a", "b"]), &none(), &set(&["c"])),
            ToolDecision::Allow
        );
    }

    #[test]
    fn until_approved_gates_each_call_until_a_release() {
        let rules = FlowRules::new().forbid_with(["a", "b"], Rule::Ask, Recheck::UntilApproved);
        let ask = ToolDecision::RequireApproval {
            reason: "this call completes the flow labels a, b".into(),
        };
        assert_eq!(
            rules.check(&set(&[]), &none(), &set(&["a"])),
            ToolDecision::Allow
        );
        // With no release, a complete set gates each call that touches it.
        assert_eq!(rules.check(&set(&["a"]), &none(), &set(&["b"])), ask);
        assert_eq!(rules.check(&set(&["a", "b"]), &none(), &set(&["a"])), ask);
        // An approval of a tripped call releases the rule.
        let (decision, releases) = rules.decide(&set(&["a"]), &none(), &set(&["b"]));
        assert_eq!(decision, ask);
        assert_eq!(releases, Released::from([set(&["a", "b"])]));
        assert_eq!(
            rules.check(&set(&["a", "b"]), &releases, &set(&["a"])),
            ToolDecision::Allow
        );
        assert_eq!(
            rules.check(&set(&["a", "b"]), &releases, &set(&["b", "c"])),
            ToolDecision::Allow
        );
    }

    #[test]
    fn only_an_until_approved_ask_rule_can_be_released() {
        let each = FlowRules::new().forbid(["a", "b"], Rule::Ask);
        let (decision, releases) = each.decide(&set(&["a"]), &none(), &set(&["b"]));
        assert!(matches!(decision, ToolDecision::RequireApproval { .. }));
        assert_eq!(releases, none());
        assert!(!each.can_release());
        // An each-call rule ignores a release of the same set.
        let prior = Released::from([set(&["a", "b"])]);
        assert!(matches!(
            each.check(&set(&["a", "b"]), &prior, &set(&["a"])),
            ToolDecision::RequireApproval { .. }
        ));
        let deny = FlowRules::new().forbid_with(
            ["a", "b"],
            Rule::Deny("never".into()),
            Recheck::UntilApproved,
        );
        assert!(!deny.can_release());
        assert_eq!(
            deny.decide(&set(&["a"]), &none(), &set(&["b"])),
            (
                ToolDecision::Deny {
                    reason: "never".into()
                },
                none()
            )
        );
    }

    #[test]
    fn each_call_is_the_default_and_with_recheck_sets_every_rule() {
        let default = FlowRules::rule_of_two();
        assert!(
            default
                .rules()
                .iter()
                .all(|rule| rule.recheck() == Recheck::EachCall)
        );
        let relaxed = FlowRules::rule_of_two().with_recheck(Recheck::UntilApproved);
        assert_eq!(relaxed.rules().len(), default.rules().len());
        assert!(
            relaxed
                .rules()
                .iter()
                .all(|rule| rule.recheck() == Recheck::UntilApproved)
        );
        assert!(relaxed.can_release());
    }

    #[test]
    fn the_strictest_tripped_rule_wins() {
        let rules = FlowRules::new()
            .forbid(["a", "b"], Rule::Ask)
            .forbid(["b", "c"], Rule::Deny("never".into()));
        assert!(matches!(
            rules.check(&set(&["a"]), &none(), &set(&["b"])),
            ToolDecision::RequireApproval { .. }
        ));
        assert!(matches!(
            rules.check(&set(&["a", "c"]), &none(), &set(&["b"])),
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
            rules.check(&read, &none(), &set(&["effect:external"])),
            ToolDecision::RequireApproval { .. }
        ));
        assert!(matches!(
            rules.check(&read, &none(), &set(&["effect:write"])),
            ToolDecision::RequireApproval { .. }
        ));
        // Two of the three pass.
        assert_eq!(
            rules.check(
                &set(&[labels::PRIVATE_DATA]),
                &none(),
                &set(&["effect:external"])
            ),
            ToolDecision::Allow
        );
        assert_eq!(
            rules.check(&read, &none(), &set(&["effect:read_only"])),
            ToolDecision::Allow
        );
    }
}
