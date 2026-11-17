//! The `sweep` workflow tool (issue #2716).
//!
//! A run with [`SweepTool`] set offers the model the `sweep` tool. One call
//! takes an instruction and a list of labelled sources. The workflow runs
//! one [`agent_sweep`](crate::sampling::agent_sweep) child over them, with a
//! single draw for each item. No activity runs the call, so the engine tool
//! timeout does not apply. A crash resumes the child where it stopped.
//!
//! Each item is a full agent run of the sweep kind, with its tools. Its
//! transcript stays out of the parent, because the sweep runs each item
//! with `SampleInput::brief`. The parent reads one short row for each item
//! and the joined answers. So its context grows by the rows, not by the
//! sources.
//!
//! The setting holds a token ceiling, and the field is not optional.
//! `docs/design/DESIGN-2448.md` says that the token cap is the only bound on
//! the spend of a sweep, so every call sets it. The result names the spend.
//! The parent counts the usage and the cost of the sweep tree as its own.
//!
//! An item can read outside text. So the result adds the labels of the
//! setting to the run, and a later rule sees that taint.
//!
//! `docs/design/DESIGN-2716.md` records the design.

use std::collections::BTreeSet;
use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

use crate::message::ToolDefinition;
use crate::sampling::{
    MAX_SWEEP_ITEM_BYTES, MAX_SWEEP_ITEMS, SampleStrategy, ScorerRef, SweepInput, SweepItem,
    SweepItemStatus, SweepItems, SweepReport,
};

/// The name of the sweep tool.
pub const SWEEP_TOOL: &str = "sweep";

const fn default_max_items() -> u32 {
    MAX_SWEEP_ITEMS
}

/// The sweep tool setting of a run. It is off unless a task or a profile
/// sets it.
///
/// Only an operator sets it. The model chooses the instruction and the
/// sources, never the kind, the ceiling or the scorer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SweepTool {
    /// The agent kind of each item. Its profile builds each item run, and
    /// its sampling policy applies.
    pub kind: String,
    /// The token ceiling of one call: the budget of the whole sweep tree.
    pub max_total_tokens: NonZeroU32,
    /// The most items of one call. It is at most [`MAX_SWEEP_ITEMS`].
    #[serde(default = "default_max_items")]
    pub max_items: u32,
    /// The scorer of each item. The policy of the kind must allow it.
    pub scorer: ScorerRef,
    /// The most items at the same time. `None` takes the cap of the
    /// policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency: Option<NonZeroU32>,
    /// The flow labels that each result adds to the run.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub flow_labels: BTreeSet<String>,
}

impl SweepTool {
    /// A sweep over the kind `kind`, with the ceiling `max_total_tokens` on
    /// each call and the scorer `scorer`.
    #[must_use]
    pub fn new(kind: impl Into<String>, max_total_tokens: NonZeroU32, scorer: ScorerRef) -> Self {
        Self {
            kind: kind.into(),
            max_total_tokens,
            max_items: MAX_SWEEP_ITEMS,
            scorer,
            concurrency: None,
            flow_labels: BTreeSet::new(),
        }
    }

    /// Refuse a call with more than `max` items. A value over
    /// [`MAX_SWEEP_ITEMS`] counts as that cap.
    #[must_use]
    pub const fn max_items(mut self, max: u32) -> Self {
        self.max_items = max;
        self
    }

    /// Run at most `n` items at the same time. A value of 0 takes the cap
    /// of the policy.
    #[must_use]
    pub const fn concurrency(mut self, n: u32) -> Self {
        self.concurrency = NonZeroU32::new(n);
        self
    }

    /// Add `label` to the run after each result.
    #[must_use]
    pub fn flow_label(mut self, label: impl Into<String>) -> Self {
        self.flow_labels.insert(label.into());
        self
    }

    /// The most items at the same time that the setting asks for. `None`
    /// takes the cap of the policy, and the policy admits the ask.
    #[must_use]
    pub const fn item_concurrency(&self) -> Option<NonZeroU32> {
        self.concurrency
    }

    /// The item bound of one call.
    #[must_use]
    pub const fn item_bound(&self) -> u32 {
        if self.max_items < MAX_SWEEP_ITEMS {
            self.max_items
        } else {
            MAX_SWEEP_ITEMS
        }
    }
}

/// The definition the model sees.
#[must_use]
pub fn definition(settings: &SweepTool) -> ToolDefinition {
    ToolDefinition {
        name: SWEEP_TOOL.to_owned(),
        description: format!(
            "Research many sources in one call. A separate read-only agent reads each \
             source with its own tools and follows the instruction. Its transcript stays out \
             of your context. You get one short answer for each label, and the tokens that \
             the call spent. Give at most {} items. The call spends at most {} tokens. An \
             item that the budget does not reach comes back with status not_run.",
            settings.item_bound(),
            settings.max_total_tokens,
        ),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "instruction": {
                    "type": "string",
                    "description": "What to do with each source. Ask for a short answer."
                },
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "label": {"type": "string", "description": "A name for the item."},
                            "source": {"type": "string", "description": "A URL, a path or a query."}
                        },
                        "required": ["label", "source"]
                    }
                }
            },
            "required": ["instruction", "items"]
        }),
    }
}

/// One item of a call.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct CallItem {
    label: String,
    source: String,
}

/// The arguments of one call.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct CallArgs {
    instruction: String,
    items: Vec<CallItem>,
}

/// One checked call: the labels in item order, and the sweep input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Planned {
    /// The label of each item, in item order.
    pub(crate) labels: Vec<String>,
    /// The input of the child sweep.
    pub(crate) input: SweepInput,
}

/// The text of the item `label` over `source`.
fn item_text(instruction: &str, label: &str, source: &str) -> String {
    format!("{instruction}\n\nItem: {label}\nSource: {source}")
}

/// Check one `sweep` call against `settings`, and plan the child sweep.
///
/// A refusal is the text that the model reads. No child starts.
pub(crate) fn plan(arguments: &serde_json::Value, settings: &SweepTool) -> Result<Planned, String> {
    // serde also decodes a struct from a JSON array, so the arguments must
    // be an object to have the named fields.
    let args = arguments
        .is_object()
        .then(|| serde_json::from_value::<CallArgs>(arguments.clone()).ok())
        .flatten()
        .ok_or_else(|| {
            "sweep needs an object with `instruction` and `items`, a list of {label, source}"
                .to_owned()
        })?;
    let instruction = args.instruction.trim();
    if instruction.is_empty() {
        return Err("sweep needs a non-empty instruction".to_owned());
    }
    if args.items.is_empty() {
        return Err("sweep needs at least one item".to_owned());
    }
    let bound = settings.item_bound();
    let count = u32::try_from(args.items.len()).unwrap_or(u32::MAX);
    if count > bound {
        return Err(format!(
            "sweep takes at most {bound} items, and the call gives {count}; split the list"
        ));
    }
    let mut labels = Vec::with_capacity(args.items.len());
    let mut items = Vec::with_capacity(args.items.len());
    for item in &args.items {
        if item.label.trim().is_empty() || item.source.trim().is_empty() {
            return Err("each sweep item needs a non-empty label and source".to_owned());
        }
        let text = item_text(instruction, &item.label, &item.source);
        if text.len() > MAX_SWEEP_ITEM_BYTES {
            return Err(format!(
                "the sweep item {:?} is over {MAX_SWEEP_ITEM_BYTES} bytes",
                item.label
            ));
        }
        labels.push(item.label.clone());
        items.push(SweepItem::new(text));
    }
    let mut input = SweepInput::new(
        settings.kind.clone(),
        SweepItems::inline(items),
        SampleStrategy::single(),
        settings.scorer.clone(),
    )
    .max_total_tokens(settings.max_total_tokens.get());
    input.concurrency = settings.item_concurrency();
    Ok(Planned { labels, input })
}

/// The token cap of one call: the ceiling of `settings`, or the tokens
/// that the parent has left under `parent_cap` after `spent`, whichever is
/// lower.
#[must_use]
pub const fn ceiling(settings: &SweepTool, parent_cap: Option<u32>, spent: u32) -> u32 {
    let own = settings.max_total_tokens.get();
    match parent_cap {
        Some(cap) => {
            let left = cap.saturating_sub(spent);
            if left < own { left } else { own }
        }
        None => own,
    }
}

/// The row of one item that the model reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SweepToolRow {
    /// The label of the item.
    pub label: String,
    /// Whether the item gave an answer that its scorer accepted.
    pub ok: bool,
    /// What happened to the item.
    pub status: SweepItemStatus,
    /// The tokens of the item tree.
    pub tokens: u32,
    /// The error of a failed item.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// The spend of one call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SweepSpend {
    /// The token ceiling of the call.
    pub ceiling: u32,
    /// The input tokens of the sweep tree.
    pub input_tokens: u32,
    /// The output tokens of the sweep tree.
    pub output_tokens: u32,
    /// The sum of both.
    pub total_tokens: u32,
    /// The cost of the sweep tree, in millionths of a US dollar.
    pub cost_usd_micros: u64,
}

/// The result of one call, as the model reads it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SweepToolResult {
    /// The answers, one paragraph for each item with an answer, in item
    /// order. Each paragraph starts with the label.
    pub text: String,
    /// One row for each item, in item order.
    pub items: Vec<SweepToolRow>,
    /// Why the sweep started no more items.
    pub stopped: crate::sampling::SweepStop,
    /// What the call spent.
    pub spent: SweepSpend,
}

/// The result of `report` for the items `labels`.
pub(crate) fn result_of(labels: &[String], report: &SweepReport, ceiling: u32) -> SweepToolResult {
    let mut paragraphs = Vec::new();
    let mut items = Vec::with_capacity(report.rows.len());
    for row in &report.rows {
        let label = usize::try_from(row.item)
            .ok()
            .and_then(|index| labels.get(index))
            .cloned()
            .unwrap_or_else(|| format!("item {}", row.item));
        if let Some(answer) = row.answer.as_deref().filter(|text| !text.trim().is_empty()) {
            paragraphs.push(format!("{label}: {}", answer.trim()));
        }
        items.push(SweepToolRow {
            label,
            ok: row.status == SweepItemStatus::Completed && row.accepted,
            status: row.status,
            tokens: row.usage.total(),
            error: row.error.clone(),
        });
    }
    let usage = report.totals.usage;
    SweepToolResult {
        text: paragraphs.join("\n\n"),
        items,
        stopped: report.stopped,
        spent: SweepSpend {
            ceiling,
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
            total_tokens: usage.total(),
            cost_usd_micros: report.totals.cost_usd_micros,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ExactMatch;
    use serde_json::json;

    fn settings() -> SweepTool {
        let ceiling = NonZeroU32::new(1_000).unwrap_or(NonZeroU32::MIN);
        let scorer = ScorerRef::deterministic(ExactMatch::ID).unwrap_or_else(|| unreachable!());
        SweepTool::new("research", ceiling, scorer).max_items(3)
    }

    fn call(items: &serde_json::Value) -> serde_json::Value {
        json!({"instruction": "Summarize.", "items": items})
    }

    #[test]
    fn a_call_sets_the_ceiling_and_a_single_draw() {
        let planned = plan(
            &call(&json!([{"label": "a", "source": "https://a"}])),
            &settings(),
        );
        let planned = planned.unwrap_or_else(|err| panic!("{err}"));
        assert_eq!(planned.labels, ["a"]);
        assert_eq!(planned.input.max_total_tokens, Some(1_000));
        assert_eq!(planned.input.strategy, SampleStrategy::single());
        assert_eq!(planned.input.kind, "research");
    }

    #[test]
    fn a_call_over_the_item_bound_is_refused() {
        let items = json!([
            {"label": "a", "source": "1"},
            {"label": "b", "source": "2"},
            {"label": "c", "source": "3"},
            {"label": "d", "source": "4"}
        ]);
        let refused = plan(&call(&items), &settings());
        assert!(refused.is_err_and(|err| err.contains("at most 3 items")));
    }

    #[test]
    fn an_empty_call_is_refused() {
        assert!(plan(&call(&json!([])), &settings()).is_err());
        let blank = json!({"instruction": " ", "items": [{"label": "a", "source": "1"}]});
        assert!(plan(&blank, &settings()).is_err());
        assert!(plan(&json!(["x"]), &settings()).is_err());
    }

    #[test]
    fn a_call_cannot_carry_a_setting() {
        let widened = json!({
            "instruction": "x",
            "items": [{"label": "a", "source": "1"}],
            "max_total_tokens": 9_999_999
        });
        assert!(plan(&widened, &settings()).is_err());
    }

    #[test]
    fn the_ceiling_never_passes_what_the_parent_has_left() {
        let settings = settings();
        assert_eq!(ceiling(&settings, None, 500), 1_000);
        assert_eq!(ceiling(&settings, Some(5_000), 500), 1_000);
        assert_eq!(ceiling(&settings, Some(800), 500), 300);
        assert_eq!(ceiling(&settings, Some(400), 500), 0);
    }

    #[test]
    fn the_item_bound_never_passes_the_sweep_cap() {
        let wide = settings().max_items(u32::MAX);
        assert_eq!(wide.item_bound(), MAX_SWEEP_ITEMS);
    }
}
