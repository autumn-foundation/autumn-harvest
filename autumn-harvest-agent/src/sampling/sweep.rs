//! The `agent_sweep` workflow: one strategy over a fixed set of inputs.
//!
//! A sweep runs each item as a child `agent_sample` run, and keeps one row
//! for each item. History records the end of each child, so a crash runs
//! only the items in flight again. A completed item never runs again.
//!
//! - The sweep reads the profile and the policy, and admits the strategy
//!   and the scorer before the first model call. Each child reads them
//!   again. A child that reads another configuration stops the sweep.
//! - [`sweep_plan`](super::sweep_plan) splits the policy cap between the
//!   items and their draws.
//! - The items share one token budget. Each wave splits the rest with
//!   `sibling_cap`, as in `agent_sample`.
//! - An LLM budget refusal in a child stops the sweep. Each item that did
//!   not start is `not_run`.
//! - A list over the activity cap goes in a [`SweepItemStore`]. The input
//!   then holds a [`SweepItemsRef`], and the sweep reads the items in
//!   recorded pages.
//!
//! The design is in `docs/design/DESIGN-2448.md`.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::payload_store::PayloadStore;
use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::admit::{Refusal, SampleStep, SweepPlan, sample_step, sweep_plan};
use super::draws::{read_policy, read_profile_versioned};
use super::policy::{SamplingPolicy, ScorerRef};
use super::sample::{
    OpSample, SAMPLE_WORKFLOW_NAME, SampleInput, SampleOutcome, SampleReport, SampleStop,
    SampleStrategy, admit_sample, check_judge,
};
use crate::agent::overlay_store::OverlayVersion;
use crate::bounds::{json_len, sibling_cap};
use crate::error::{AgentError, ErrorKind};
use crate::harness::cut_bytes;
use crate::mesh::valid_name;
use crate::message::TokenUsage;
use crate::response_cache::hex;
use crate::score::{FiniteF64, Verdict, select};
use crate::types::{AgentStop, MAX_PARALLEL_CHILDREN};
use crate::workflow::harness;

/// The workflow name of [`agent_sweep`].
pub const SWEEP_WORKFLOW_NAME: &str = "agent_sweep";

/// The most items of one sweep.
pub const MAX_SWEEP_ITEMS: u32 = 1_000;

/// The most bytes of one item, as JSON.
///
/// A page holds at least one item, so a page read always fits the result
/// cap of an activity.
pub const MAX_SWEEP_ITEM_BYTES: usize = 64 * 1024;

/// The most bytes of one stored list, as JSON.
///
/// Each page read decodes the whole list, so the cap bounds the work of a
/// sweep.
pub const MAX_SWEEP_LIST_BYTES: u64 = 16 * 1024 * 1024;

/// The most bytes of the items of one page read, as JSON.
pub const MAX_SWEEP_PAGE_BYTES: usize = 1024 * 1024;

/// The most bytes of the answer that a row keeps.
pub const MAX_SWEEP_ANSWER_BYTES: usize = 512;

/// The most bytes of a reject reason or an error that a row keeps.
pub const MAX_SWEEP_REASON_BYTES: usize = 256;

/// One input of a sweep.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SweepItem {
    /// The user message of each draw of the item.
    pub input: String,
    /// The expected answer. The score input of each draw carries it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected: Option<String>,
}

impl SweepItem {
    /// An item with no expected answer.
    #[must_use]
    pub fn new(input: impl Into<String>) -> Self {
        Self {
            input: input.into(),
            expected: None,
        }
    }

    /// Set the expected answer.
    #[must_use]
    pub fn expected(mut self, expected: impl Into<String>) -> Self {
        self.expected = Some(expected.into());
        self
    }
}

/// The reference of a list in a [`SweepItemStore`].
///
/// Each read checks the item count and the digest. So the list cannot
/// change while a sweep reads it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SweepItemsRef {
    /// The key of the blob in the payload store.
    pub key: String,
    /// The SHA-256 digest of the plain JSON list with sorted keys, as
    /// lowercase hex.
    pub digest: String,
    /// The number of items.
    pub items: u32,
    /// The plain size of the list, in bytes.
    pub bytes: u64,
}

/// The items of a sweep: in the input, or in a [`SweepItemStore`].
///
/// It encodes as `{"inline": [...]}` or `{"stored": {...}}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SweepItems {
    /// The items, in order.
    Inline(Vec<SweepItem>),
    /// The reference of a stored list.
    Stored(SweepItemsRef),
}

impl SweepItems {
    /// The items `items`, in the input.
    #[must_use]
    pub const fn inline(items: Vec<SweepItem>) -> Self {
        Self::Inline(items)
    }

    /// The number of items. A count over `u32::MAX` reads as `u32::MAX`.
    #[must_use]
    pub fn len(&self) -> u32 {
        match self {
            Self::Inline(items) => u32::try_from(items.len()).unwrap_or(u32::MAX),
            Self::Stored(stored) => stored.items,
        }
    }

    /// Whether the list has no item.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The input of one `agent_sweep` run.
///
/// It denies unknown keys, so it never carries a policy field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SweepInput {
    /// The agent kind. Its profile builds each draw, and its policy applies.
    pub kind: String,
    /// The items.
    pub items: SweepItems,
    /// The strategy of each item.
    pub strategy: SampleStrategy,
    /// The scorer of each item. The policy must allow it.
    pub scorer: ScorerRef,
    /// The most items at the same time. `None` takes the cap of the
    /// policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency: Option<NonZeroU32>,
    /// The token budget of the whole sweep tree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_total_tokens: Option<u32>,
}

impl SweepInput {
    /// A sweep of `strategy` over `items` for the kind `kind`.
    #[must_use]
    pub fn new(
        kind: impl Into<String>,
        items: SweepItems,
        strategy: SampleStrategy,
        scorer: ScorerRef,
    ) -> Self {
        Self {
            kind: kind.into(),
            items,
            strategy,
            scorer,
            concurrency: None,
            max_total_tokens: None,
        }
    }

    /// Run at most `n` items at the same time. A value of 0 takes the cap
    /// of the policy.
    #[must_use]
    pub const fn concurrency(mut self, n: u32) -> Self {
        self.concurrency = NonZeroU32::new(n);
        self
    }

    /// Cap the tokens of the whole sweep tree. The cap of the profile still
    /// applies to each item.
    #[must_use]
    pub const fn max_total_tokens(mut self, max: u32) -> Self {
        self.max_total_tokens = Some(max);
        self
    }
}

/// What happened to one item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SweepItemStatus {
    /// The item runs to its end.
    Completed,
    /// A budget stops the item after it spends.
    BudgetExceeded,
    /// The child run fails in the engine. The row holds the error.
    Failed,
    /// The item makes no model call: the sweep stops first, or an LLM
    /// budget refuses each first call.
    NotRun,
}

/// The row of one item.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SweepRow {
    /// The index of the item, from 0.
    pub item: u32,
    /// What happened to the item.
    pub status: SweepItemStatus,
    /// The verdict of the selected draw. With no selection, it is the best
    /// score, else the first reject. `None` when no draw runs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verdict: Option<Verdict>,
    /// Whether the item selected a draw that passed. A cascade that
    /// escalates keeps an answer, but it is not accepted.
    pub accepted: bool,
    /// The answer of the selected draw, cut to [`MAX_SWEEP_ANSWER_BYTES`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub answer: Option<String>,
    /// The number of draws that the item runs.
    pub draws: u32,
    /// The tokens of the item tree, judge calls included.
    pub usage: TokenUsage,
    /// The cost of the item tree, in millionths of a US dollar.
    #[serde(default, skip_serializing_if = "crate::agent::is_zero")]
    pub cost_usd_micros: u64,
    /// The error of a failed item, cut to [`MAX_SWEEP_REASON_BYTES`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// The policy revision that the child reads. `None` when no child
    /// report comes back.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<u64>,
    /// The overlay version of the profile that builds the draws of the
    /// child. `None` is the bare profile, or no child report.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overlay_version: Option<OverlayVersion>,
}

impl SweepRow {
    /// The row of an item that makes no model call.
    const fn not_run(item: u32) -> Self {
        Self {
            item,
            status: SweepItemStatus::NotRun,
            verdict: None,
            accepted: false,
            answer: None,
            draws: 0,
            usage: TokenUsage::new(0, 0),
            cost_usd_micros: 0,
            error: None,
            revision: None,
            overlay_version: None,
        }
    }
}

/// The totals of a sweep.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SweepTotals {
    /// The items of the sweep.
    pub items: u32,
    /// The items that are not `not_run`.
    pub run: u32,
    /// The items that selected a draw.
    pub accepted: u32,
    /// The items that made no model call.
    pub not_run: u32,
    /// The items whose child failed in the engine.
    pub failed: u32,
    /// The mean score of the rows with a verdict. A reject counts as 0.
    /// `None` when no row has a verdict.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mean_score: Option<FiniteF64>,
    /// `accepted / run`. `None` when no item ran.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accept_rate: Option<FiniteF64>,
    /// The tokens of the sweep tree.
    pub usage: TokenUsage,
    /// The cost of the sweep tree, in millionths of a US dollar.
    #[serde(default, skip_serializing_if = "crate::agent::is_zero")]
    pub cost_usd_micros: u64,
    /// `cost_usd_micros / accepted`, rounded down. `None` when no item
    /// selected a draw.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_per_accepted_usd_micros: Option<u64>,
}

/// Why the sweep starts no more items.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SweepStop {
    /// Each item starts.
    Completed,
    /// An LLM budget refuses a call of an item, or no token is left.
    BudgetExceeded,
    /// A child reads another policy revision or another overlay version
    /// than the sweep. The rows of that wave keep what each child read.
    ConfigChanged,
}

/// The result of one `agent_sweep` run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SweepReport {
    /// The agent kind.
    pub kind: String,
    /// The strategy of each item.
    pub strategy: SampleStrategy,
    /// The scorer of each item.
    pub scorer: ScorerRef,
    /// The revision of the policy that the sweep reads. 0 for every default.
    pub revision: u64,
    /// The overlay version of the profile that the sweep reads. `None` is
    /// the bare profile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overlay_version: Option<OverlayVersion>,
    /// The most items at the same time.
    pub concurrency: u32,
    /// The most draws of one item at the same time.
    pub draw_concurrency: u32,
    /// Why the sweep stops.
    pub stopped: SweepStop,
    /// One row for each item, in item order.
    pub rows: Vec<SweepRow>,
    /// The totals.
    pub totals: SweepTotals,
}

/// Run one sweep.
///
/// # Errors
///
/// Returns an error that starts with
/// [`REFUSED_PREFIX`](super::REFUSED_PREFIX) when the item list or the
/// policy refuses the sweep. No model call runs before it. Returns an error
/// when a read fails for good, for example a stored list with another
/// digest. A child that fails does not fail the sweep: its row is
/// `failed`.
#[workflow]
pub async fn agent_sweep(ctx: &WorkflowContext, input: SweepInput) -> Result<SweepReport, String> {
    if !valid_name(&input.kind) {
        return Err(Refusal::InvalidKind.error_text());
    }
    let total = check_items(&input.items).map_err(|refusal| refusal.error_text())?;
    let Some((_, overlay_version)) = read_profile_versioned(ctx, &input.kind).await? else {
        return Err(Refusal::UnknownKind.error_text());
    };
    let read = read_policy(ctx, &input.kind).await?;
    let plan = plan(&read.policy, &input, total).map_err(|refusal| refusal.error_text())?;
    let mut source = ItemSource::new(&input.items);
    let mut rows = Vec::with_capacity(usize::try_from(total).unwrap_or(0));
    let admitted = Admitted {
        revision: read.revision,
        overlay_version,
    };
    let stopped = run_items(ctx, &input, plan, admitted, total, &mut source, &mut rows).await?;
    let ran = u32::try_from(rows.len()).unwrap_or(u32::MAX);
    rows.extend((ran..total).map(SweepRow::not_run));
    let totals = totals(total, &rows);
    Ok(SweepReport {
        kind: input.kind,
        strategy: input.strategy,
        scorer: input.scorer,
        revision: read.revision,
        overlay_version,
        concurrency: plan.items,
        draw_concurrency: plan.draws,
        stopped,
        rows,
        totals,
    })
}

/// The policy revision and the overlay version that the sweep admits
/// under.
#[derive(Debug, Clone, Copy)]
struct Admitted {
    revision: u64,
    overlay_version: Option<OverlayVersion>,
}

impl Admitted {
    /// Whether `row` comes from a child that read another configuration.
    fn differs(self, row: &SweepRow) -> bool {
        row.revision.is_some_and(|revision| {
            revision != self.revision || row.overlay_version != self.overlay_version
        })
    }
}

/// The item concurrency and the draw concurrency of a sweep.
#[derive(Debug, Clone, Copy)]
struct Plan {
    items: u32,
    draws: u32,
}

/// Refuse an empty list, a list over [`MAX_SWEEP_ITEMS`], and an inline
/// item over [`MAX_SWEEP_ITEM_BYTES`]. Returns the item count.
fn check_items(items: &SweepItems) -> Result<u32, Refusal> {
    let total = items.len();
    if total == 0 {
        return Err(Refusal::NoItems);
    }
    if total > MAX_SWEEP_ITEMS {
        return Err(Refusal::TooManyItems {
            asked: total,
            max: MAX_SWEEP_ITEMS,
        });
    }
    if let SweepItems::Inline(list) = items {
        check_item_sizes(list)?;
    }
    Ok(total)
}

/// Refuse the first item over [`MAX_SWEEP_ITEM_BYTES`].
fn check_item_sizes(items: &[SweepItem]) -> Result<(), Refusal> {
    let max = MAX_SWEEP_ITEM_BYTES as u64;
    match (0_u32..).zip(items).find(|(_, item)| json_len(*item) > max) {
        Some((item, _)) => Err(Refusal::ItemTooLarge { item, max }),
        None => Ok(()),
    }
}

/// Admit the sweep under `policy`.
///
/// [`sweep_plan`] splits the cap between the items and their draws. The
/// sample admission then checks the strategy, the draws and the scorer of
/// one item, with the draw concurrency of the plan.
fn plan(policy: &SamplingPolicy, input: &SweepInput, total: u32) -> Result<Plan, Refusal> {
    // With no ask, the sweep takes the cap, at most one slot per item. The
    // draws of each item then share the rest of the cap.
    let cap = policy.max_concurrent_draws.clamp(1, MAX_PARALLEL_CHILDREN);
    let asked = input
        .concurrency
        .map_or_else(|| cap.min(total), NonZeroU32::get);
    // The most draws that one item can run at the same time. A beam
    // round runs `width * expansion` steps.
    let wide = match &input.strategy {
        SampleStrategy::BestOfN { n } => *n,
        SampleStrategy::Beam {
            width, expansion, ..
        } => width.saturating_mul(*expansion),
        _ => 1,
    };
    let SweepPlan::Run { items, draws } = sweep_plan(policy.max_concurrent_draws, asked, wide)
    else {
        return Err(Refusal::TooManyConcurrentDraws {
            asked,
            max: policy.max_concurrent_draws.min(MAX_PARALLEL_CHILDREN),
        });
    };
    let probe = OpSample {
        concurrency: NonZeroU32::new(draws),
        ..OpSample::new(input.strategy.clone(), input.scorer.clone())
    };
    admit_sample(policy, &probe)?;
    // A vote takes no scorer, and a sweep row needs one. So a sweep refuses
    // a vote here, before its first item.
    check_judge(&probe)?;
    Ok(Plan { items, draws })
}

/// The `agent_sample` input of `item`.
fn item_input(input: &SweepInput, item: &SweepItem, draws: u32) -> SampleInput {
    let mut sample = SampleInput::new(
        input.kind.clone(),
        item.input.clone(),
        input.strategy.clone(),
        input.scorer.clone(),
    )
    .concurrency(draws);
    sample.expected.clone_from(&item.expected);
    sample.brief = true;
    sample
}

/// Run the item waves, and push one row for each item that started.
///
/// [`sample_step`] gives the width of each wave over the items. Item `i`
/// of a wave gets the token cap `sibling_cap(left, width, i)`. After the
/// wave, the spend of each item counts against the budget.
///
/// Each child reads the profile and the policy again. A child that reads
/// another configuration stops the sweep after its wave, so a change by
/// the operator applies at once. A configuration change wins over a
/// budget stop.
async fn run_items(
    ctx: &WorkflowContext,
    input: &SweepInput,
    plan: Plan,
    admitted: Admitted,
    total: u32,
    source: &mut ItemSource<'_>,
    rows: &mut Vec<SweepRow>,
) -> Result<SweepStop, String> {
    let budget = input.max_total_tokens;
    let mut spent: u32 = 0;
    loop {
        let started = u32::try_from(rows.len()).unwrap_or(u32::MAX);
        let left = budget.map_or(0, |max| max.saturating_sub(spent));
        let width = match sample_step(total, started, plan.items, budget.is_some(), left) {
            SampleStep::Done => return Ok(SweepStop::Completed),
            SampleStep::BudgetExceeded => return Ok(SweepStop::BudgetExceeded),
            SampleStep::Wave { width } => width,
        };
        let wave = source.take(ctx, started, width).await?;
        let mut children = Vec::with_capacity(wave.len());
        for (index, item) in (0..width).zip(&wave) {
            let mut sample = item_input(input, item, plan.draws);
            if budget.is_some() {
                sample.max_total_tokens = Some(sibling_cap(left, width, index));
            }
            let value = serde_json::to_value(&sample).map_err(|err| err.to_string())?;
            children.push((SAMPLE_WORKFLOW_NAME.to_owned(), value));
        }
        let ended = ctx
            .spawn_child_workflow_fan_out_collect_raw(children)
            .await
            .map_err(|err| err.to_string())?;
        let mut halt = None;
        for ((index, item), result) in (0..width).zip(started..).zip(ended) {
            let (row, refused) = row_of(item, result);
            if admitted.differs(&row) {
                halt = Some(SweepStop::ConfigChanged);
            } else if refused {
                halt = halt.or(Some(SweepStop::BudgetExceeded));
            }
            // A failed child returns no spend, so its whole share counts.
            let cost = if row.status == SweepItemStatus::Failed && budget.is_some() {
                sibling_cap(left, width, index)
            } else {
                row.usage.total()
            };
            spent = spent.saturating_add(cost);
            rows.push(row);
        }
        if let Some(stop) = halt {
            return Ok(stop);
        }
    }
}

/// The items of a sweep, in order.
///
/// A stored list is read in recorded pages. Replay reads each page from
/// history.
struct ItemSource<'a> {
    items: &'a SweepItems,
    read: Vec<SweepItem>,
}

impl<'a> ItemSource<'a> {
    const fn new(items: &'a SweepItems) -> Self {
        Self {
            items,
            read: Vec::new(),
        }
    }

    /// The items `start..start + width`.
    ///
    /// The sweep asks for each item once, in order. So a stored page is
    /// dropped once the sweep took its items.
    async fn take(
        &mut self,
        ctx: &WorkflowContext,
        start: u32,
        width: u32,
    ) -> Result<Vec<SweepItem>, String> {
        let wanted = usize::try_from(width).unwrap_or(usize::MAX);
        let stored = match self.items {
            SweepItems::Inline(list) => {
                let from = usize::try_from(start).unwrap_or(usize::MAX);
                return Ok(list.iter().skip(from).take(wanted).cloned().collect());
            }
            SweepItems::Stored(stored) => stored,
        };
        while self.read.len() < wanted {
            let next = start.saturating_add(u32::try_from(self.read.len()).unwrap_or(u32::MAX));
            let page = SweepPage {
                items: stored.clone(),
                start: next,
            };
            let mut items: Vec<SweepItem> = ctx
                .execute_activity(&agent_sweep_items_info(), page)
                .await
                .map_err(|err| err.to_string())?;
            if items.is_empty() {
                return Err(format!("the stored list has no item {next}"));
            }
            self.read.append(&mut items);
        }
        Ok(self.read.drain(..wanted).collect())
    }
}

/// The row of item `item`, and whether an LLM budget refused a call of it.
///
/// A child that stops `budget_exceeded` gives a `budget_exceeded` row. A
/// child whose every draw an LLM budget refused before any spend is
/// `not_run`. A child that fails gives a `failed` row.
fn row_of(item: u32, result: Result<Value, String>) -> (SweepRow, bool) {
    let report = result.and_then(|value| {
        serde_json::from_value::<SampleReport>(value).map_err(|err| err.to_string())
    });
    let report = match report {
        Ok(report) => report,
        Err(error) => {
            let row = SweepRow {
                status: SweepItemStatus::Failed,
                error: Some(cut_bytes(error, MAX_SWEEP_REASON_BYTES)),
                ..SweepRow::not_run(item)
            };
            return (row, false);
        }
    };
    let budget = report.stopped == SampleStop::BudgetExceeded;
    let never_ran = report
        .draws
        .iter()
        .all(|draw| draw.stop == AgentStop::BudgetExceeded && draw.usage.total() == 0);
    if report.refused && never_ran {
        let row = SweepRow {
            revision: Some(report.revision),
            overlay_version: report.overlay_version,
            ..SweepRow::not_run(item)
        };
        return (row, true);
    }
    let row = SweepRow {
        item,
        status: if budget {
            SweepItemStatus::BudgetExceeded
        } else {
            SweepItemStatus::Completed
        },
        verdict: row_verdict(&report),
        accepted: matches!(report.outcome, SampleOutcome::Selected { .. }),
        answer: report
            .selected
            .as_ref()
            .map(|selected| cut_bytes(selected.text.clone(), MAX_SWEEP_ANSWER_BYTES)),
        draws: report.attempts,
        usage: report.usage,
        cost_usd_micros: report.cost_usd_micros,
        error: None,
        revision: Some(report.revision),
        overlay_version: report.overlay_version,
    };
    (row, report.refused)
}

/// The verdict of a row: the selected draw, else the best score, else the
/// first reject. A reject reason is cut to [`MAX_SWEEP_REASON_BYTES`].
fn row_verdict(report: &SampleReport) -> Option<Verdict> {
    let all: Vec<_> = report
        .draws
        .iter()
        .map(|draw| (draw.draw, draw.verdict.clone()))
        .collect();
    let chosen = match report.outcome {
        SampleOutcome::Selected { draw } | SampleOutcome::Escalated { draw } => Some(draw),
        SampleOutcome::NoAcceptedDraw => select(&all),
    };
    let verdict = chosen
        .and_then(|id| all.iter().find(|(draw, _)| *draw == id))
        .or_else(|| all.first())
        .map(|(_, verdict)| verdict.clone())?;
    Some(match verdict {
        Verdict::Reject(reason) => Verdict::reject(cut_bytes(reason, MAX_SWEEP_REASON_BYTES)),
        scored @ Verdict::Score(_) => scored,
    })
}

fn totals(items: u32, rows: &[SweepRow]) -> SweepTotals {
    let count = |status: SweepItemStatus| {
        let n = rows.iter().filter(|row| row.status == status).count();
        u32::try_from(n).unwrap_or(u32::MAX)
    };
    let not_run = count(SweepItemStatus::NotRun);
    let failed = count(SweepItemStatus::Failed);
    let run = u32::try_from(rows.len())
        .unwrap_or(u32::MAX)
        .saturating_sub(not_run);
    let accepted =
        u32::try_from(rows.iter().filter(|row| row.accepted).count()).unwrap_or(u32::MAX);
    let usage = rows.iter().fold(TokenUsage::default(), |usage, row| {
        usage.saturating_add(row.usage)
    });
    let cost_usd_micros = rows
        .iter()
        .fold(0_u64, |cost, row| cost.saturating_add(row.cost_usd_micros));
    SweepTotals {
        items,
        run,
        accepted,
        not_run,
        failed,
        mean_score: mean_score(rows),
        accept_rate: ratio(accepted, run),
        usage,
        cost_usd_micros,
        cost_per_accepted_usd_micros: (accepted > 0).then(|| cost_usd_micros / u64::from(accepted)),
    }
}

/// The mean score of the rows with a verdict. A reject counts as 0.
///
/// A sum that is not finite divides each score first, so the mean stays
/// finite.
fn mean_score(rows: &[SweepRow]) -> Option<FiniteF64> {
    let scores: Vec<f64> = rows
        .iter()
        .filter_map(|row| row.verdict.as_ref())
        .map(|verdict| verdict.as_score().map_or(0.0, FiniteF64::get))
        .collect();
    let n = f64::from(u32::try_from(scores.len()).ok().filter(|n| *n > 0)?);
    let sum: f64 = scores.iter().sum();
    let mean = if sum.is_finite() {
        sum / n
    } else {
        scores.iter().map(|score| score / n).sum()
    };
    FiniteF64::new(mean).ok()
}

/// `part / whole`, or `None` when `whole` is 0.
fn ratio(part: u32, whole: u32) -> Option<FiniteF64> {
    (whole > 0)
        .then(|| FiniteF64::new(f64::from(part) / f64::from(whole)).ok())
        .flatten()
}

/// The input of the `agent_sweep_items` activity: one page of a stored
/// list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SweepPage {
    /// The stored list.
    pub items: SweepItemsRef,
    /// The index of the first item of the page.
    pub start: u32,
}

/// One page of a stored list, as an activity.
///
/// A page holds the items from `start`, at most [`MAX_SWEEP_PAGE_BYTES`] of
/// them, and at least one. It is empty only when `start` is past the end.
///
/// # Errors
///
/// Returns a non-retryable error when the harness has no item store, or
/// the list has another digest or another count. Returns a retryable error
/// when the store fails.
#[activity(
    start_to_close = "1m",
    retry = RetryPolicy::exponential(8, Duration::from_secs(2))
)]
pub async fn agent_sweep_items(
    ctx: &ActivityContext,
    page: SweepPage,
) -> Result<Vec<SweepItem>, String> {
    harness(ctx)?.sweep_page(&page).await
}

/// A store of sweep item lists over a payload store.
///
/// The codec registry encodes each list, so an encrypting codec encrypts
/// it at rest. Clones share the store and the registry.
///
/// Give it its own payload store. The orphan sweep of an engine store
/// deletes a blob that no run references.
#[derive(Clone)]
pub struct SweepItemStore {
    store: Arc<dyn PayloadStore>,
    codecs: PayloadCodecs,
}

impl std::fmt::Debug for SweepItemStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SweepItemStore")
            .field("store", &self.store.store_id())
            .finish_non_exhaustive()
    }
}

impl SweepItemStore {
    /// A store over `store`, encoded by `codecs`.
    ///
    /// Pass a clone of the codec registry that the engine uses.
    #[must_use]
    pub fn new(store: Arc<dyn PayloadStore>, codecs: PayloadCodecs) -> Self {
        Self { store, codecs }
    }

    /// Store `items`, and return their reference for a [`SweepInput`].
    ///
    /// # Errors
    ///
    /// Returns a `Config` error for an empty list, a list over
    /// [`MAX_SWEEP_ITEMS`] or [`MAX_SWEEP_LIST_BYTES`], an item over
    /// [`MAX_SWEEP_ITEM_BYTES`], or a list that does not encode. The store
    /// then writes nothing. Returns an `Unavailable` error when the
    /// store fails.
    pub async fn put(&self, items: &[SweepItem]) -> Result<SweepItems, AgentError> {
        let count = u32::try_from(items.len()).unwrap_or(u32::MAX);
        let shape = SweepItems::Stored(SweepItemsRef {
            key: String::new(),
            digest: String::new(),
            items: count,
            bytes: 0,
        });
        check_items(&shape)
            .and_then(|_| check_item_sizes(items))
            .map_err(|refusal| AgentError::new(ErrorKind::Config, refusal.to_string()))?;
        // The digest reads the canonical form: a JSON value sorts its keys,
        // so a read after the codec gives the same bytes.
        let value = serde_json::to_value(items)
            .map_err(|err| AgentError::new(ErrorKind::Config, err.to_string()))?;
        let plain = serde_json::to_vec(&value)
            .map_err(|err| AgentError::new(ErrorKind::Config, err.to_string()))?;
        if plain.len() as u64 > MAX_SWEEP_LIST_BYTES {
            return Err(AgentError::new(
                ErrorKind::Config,
                format!("the list is larger than {MAX_SWEEP_LIST_BYTES} bytes"),
            ));
        }
        let digest = hex(&Sha256::digest(&plain));
        let encoded = self.codecs.encode_payload(&value).map_err(|_| {
            AgentError::new(
                ErrorKind::Config,
                "the payload codec could not encode a sweep item list",
            )
        })?;
        let bytes = serde_json::to_vec(&encoded)
            .map_err(|err| AgentError::new(ErrorKind::Config, err.to_string()))?;
        let key = self
            .store
            .put(&bytes)
            .await
            .map_err(|err| AgentError::new(ErrorKind::Unavailable, err.to_string()))?;
        Ok(SweepItems::Stored(SweepItemsRef {
            key,
            digest,
            items: count,
            bytes: plain.len() as u64,
        }))
    }

    /// The page of `items` from `start`. See [`agent_sweep_items`].
    ///
    /// # Errors
    ///
    /// Returns a `Decode` error when the list has another digest, size or
    /// count, or an item over [`MAX_SWEEP_ITEM_BYTES`]. Returns an
    /// `Unavailable` error when the store fails.
    pub async fn page(
        &self,
        items: &SweepItemsRef,
        start: u32,
    ) -> Result<Vec<SweepItem>, AgentError> {
        if items.bytes > MAX_SWEEP_LIST_BYTES {
            return Err(AgentError::new(
                ErrorKind::Decode,
                format!("a stored sweep list is larger than {MAX_SWEEP_LIST_BYTES} bytes"),
            ));
        }
        let bytes = self
            .store
            .get(&items.key)
            .await
            .map_err(|err| AgentError::new(ErrorKind::Unavailable, err.to_string()))?;
        let list = self.decode(&bytes, items)?;
        let from = usize::try_from(start).unwrap_or(usize::MAX);
        let max = MAX_SWEEP_PAGE_BYTES as u64;
        let mut size = 0_u64;
        let mut page = Vec::new();
        for item in list.into_iter().skip(from) {
            size = size.saturating_add(json_len(&item));
            if !page.is_empty() && size > max {
                break;
            }
            page.push(item);
        }
        Ok(page)
    }

    /// The list inside the codec envelope `bytes`, checked against `items`.
    ///
    /// The errors use fixed text, because a serde error can quote an item.
    fn decode(&self, bytes: &[u8], items: &SweepItemsRef) -> Result<Vec<SweepItem>, AgentError> {
        let shape = || AgentError::new(ErrorKind::Decode, "a stored sweep list does not decode");
        // The digest of the reference binds the plain list. So a plain blob
        // from before a codec rollout still reads.
        let value: Value = serde_json::from_slice(bytes).map_err(|_| shape())?;
        let decoded = self.codecs.decode_payload(&value).map_err(|_| shape())?;
        let plain = serde_json::to_vec(&decoded).map_err(|_| shape())?;
        if plain.len() as u64 != items.bytes || hex(&Sha256::digest(&plain)) != items.digest {
            return Err(AgentError::new(
                ErrorKind::Decode,
                "the stored sweep list has another digest than its reference",
            ));
        }
        let list: Vec<SweepItem> = serde_json::from_slice(&plain).map_err(|_| shape())?;
        if u32::try_from(list.len()).ok() != Some(items.items) {
            return Err(AgentError::new(
                ErrorKind::Decode,
                "the stored sweep list has another item count than its reference",
            ));
        }
        check_item_sizes(&list)
            .map_err(|refusal| AgentError::new(ErrorKind::Decode, refusal.to_string()))?;
        Ok(list)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(value: serde_json::Value) -> SampleReport {
        serde_json::from_value(value).unwrap()
    }

    fn sample(stopped: &str, outcome: &serde_json::Value, draws: &serde_json::Value) -> Value {
        serde_json::json!({
            "kind": "k", "strategy": "single", "revision": 0, "concurrency": 1,
            "outcome": outcome, "stopped": stopped, "attempts": 1, "draws": draws,
            "usage": {"input_tokens": 3, "output_tokens": 0}, "cost_usd_micros": 9,
        })
    }

    fn draw(n: u32, verdict: &serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "draw": n, "stop": "completed", "verdict": verdict,
            "usage": {"input_tokens": 3, "output_tokens": 0},
        })
    }

    #[test]
    fn a_row_without_a_selection_keeps_the_best_score() {
        let value = sample(
            "completed",
            &serde_json::json!("no_accepted_draw"),
            &serde_json::json!([
                draw(0, &serde_json::json!({"reject": "no"})),
                draw(1, &serde_json::json!({"score": 0.3})),
            ]),
        );
        let (row, halted) = row_of(4, Ok(value.clone()));
        assert!(!halted);
        assert_eq!(row.item, 4);
        assert_eq!(row.status, SweepItemStatus::Completed);
        assert!(!row.accepted);
        assert_eq!(
            row.verdict.and_then(|verdict| verdict.as_score()),
            FiniteF64::new(0.3).ok()
        );
        assert_eq!(
            row_verdict(&report(value)).map(|v| v.is_reject()),
            Some(false)
        );
    }

    #[test]
    fn an_escalated_cascade_keeps_its_answer_but_is_not_accepted() {
        let mut value = sample(
            "completed",
            &serde_json::json!({"escalated": {"draw": 1}}),
            &serde_json::json!([
                draw(0, &serde_json::json!({"score": 0.2})),
                draw(1, &serde_json::json!({"score": 0.4})),
            ]),
        );
        value["selected"] = serde_json::json!({
            "stop": "completed", "text": "maybe", "steps_used": 1, "tool_calls": 0,
            "usage": {"input_tokens": 1, "output_tokens": 0}, "messages": [],
        });
        let (row, halted) = row_of(0, Ok(value));
        assert!(!halted);
        assert!(!row.accepted, "no tier passed its threshold");
        assert_eq!(row.answer.as_deref(), Some("maybe"));
        assert_eq!(
            row.verdict.and_then(|verdict| verdict.as_score()),
            FiniteF64::new(0.4).ok()
        );
    }

    #[test]
    fn a_row_cuts_a_long_reject_reason() {
        let long = "r".repeat(MAX_SWEEP_REASON_BYTES * 4);
        let value = sample(
            "completed",
            &serde_json::json!("no_accepted_draw"),
            &serde_json::json!([draw(0, &serde_json::json!({ "reject": long }))]),
        );
        let (row, _) = row_of(0, Ok(value));
        let Some(Verdict::Reject(reason)) = row.verdict else {
            panic!("expected a reject");
        };
        assert!(reason.len() <= MAX_SWEEP_REASON_BYTES);
    }

    #[test]
    fn a_budget_stop_halts_only_on_an_llm_refusal() {
        let mut value = sample(
            "budget_exceeded",
            &serde_json::json!({"selected": {"draw": 0}}),
            &serde_json::json!([draw(0, &serde_json::json!({"score": 1.0}))]),
        );
        let (row, halted) = row_of(0, Ok(value.clone()));
        assert!(!halted, "a spent token cap stops the item only");
        assert_eq!(row.status, SweepItemStatus::BudgetExceeded);
        assert!(row.accepted);
        assert_eq!(row.cost_usd_micros, 9);
        value["refused"] = serde_json::json!(true);
        let (row, halted) = row_of(0, Ok(value));
        assert!(halted, "an LLM refusal stops the sweep");
        assert_eq!(
            row.status,
            SweepItemStatus::BudgetExceeded,
            "the item spent"
        );
    }

    #[test]
    fn an_item_refused_before_any_spend_is_not_run() {
        let refused_draw = serde_json::json!({
            "draw": 0, "stop": "budget_exceeded", "verdict": {"reject": "budget"},
            "usage": {"input_tokens": 0, "output_tokens": 0},
        });
        let mut value = sample(
            "budget_exceeded",
            &serde_json::json!("no_accepted_draw"),
            &serde_json::json!([refused_draw]),
        );
        value["refused"] = serde_json::json!(true);
        let (row, halted) = row_of(3, Ok(value.clone()));
        assert!(halted);
        assert_eq!(row.status, SweepItemStatus::NotRun);
        assert_eq!(row.revision, Some(0));
        // A draw that completes with no spend, such as a cache hit, ran.
        value["draws"] = serde_json::json!([draw(0, &serde_json::json!({"score": 1.0}))]);
        value["outcome"] = serde_json::json!({"selected": {"draw": 0}});
        let (row, _) = row_of(3, Ok(value));
        assert_eq!(row.status, SweepItemStatus::BudgetExceeded);
        assert!(row.accepted);
    }

    #[test]
    fn a_failed_child_gives_a_failed_row_that_does_not_halt() {
        let (row, halted) = row_of(2, Err("e".repeat(MAX_SWEEP_REASON_BYTES * 2)));
        assert!(!halted);
        assert_eq!(row.status, SweepItemStatus::Failed);
        assert!(row.error.unwrap().len() <= MAX_SWEEP_REASON_BYTES);
        assert_eq!(row.draws, 0);
    }

    #[test]
    fn the_totals_count_each_status() {
        let mut accepted = SweepRow::not_run(0);
        accepted.status = SweepItemStatus::Completed;
        accepted.accepted = true;
        accepted.verdict = Some(Verdict::score(1.0).unwrap());
        accepted.cost_usd_micros = 10;
        let mut rejected = SweepRow::not_run(1);
        rejected.status = SweepItemStatus::Completed;
        rejected.verdict = Some(Verdict::reject("no"));
        rejected.cost_usd_micros = 5;
        let mut failed = SweepRow::not_run(2);
        failed.status = SweepItemStatus::Failed;
        let rows = [accepted, rejected, failed, SweepRow::not_run(3)];
        let totals = totals(4, &rows);
        assert_eq!(
            (totals.run, totals.accepted, totals.not_run, totals.failed),
            (3, 1, 1, 1)
        );
        assert_eq!(totals.mean_score.map(FiniteF64::get), Some(0.5));
        assert_eq!(totals.accept_rate.map(FiniteF64::get), Some(1.0 / 3.0));
        assert_eq!(totals.cost_usd_micros, 15);
        assert_eq!(totals.cost_per_accepted_usd_micros, Some(15));
        let none = super::totals(1, &[SweepRow::not_run(0)]);
        assert_eq!(none.mean_score, None);
        assert_eq!(none.accept_rate, None);
        assert_eq!(none.cost_per_accepted_usd_micros, None);
    }

    #[test]
    fn a_huge_mean_stays_finite() {
        let mut rows = Vec::new();
        for item in 0..2 {
            let mut row = SweepRow::not_run(item);
            row.status = SweepItemStatus::Completed;
            row.verdict = Some(Verdict::score(f64::MAX).unwrap());
            rows.push(row);
        }
        assert_eq!(mean_score(&rows).map(FiniteF64::get), Some(f64::MAX));
    }

    #[test]
    fn the_item_checks_refuse_each_bound() {
        assert_eq!(
            check_items(&SweepItems::inline(Vec::new())),
            Err(Refusal::NoItems)
        );
        let huge = SweepItem::new("x".repeat(MAX_SWEEP_ITEM_BYTES));
        assert_eq!(
            check_items(&SweepItems::inline(vec![SweepItem::new("a"), huge])),
            Err(Refusal::ItemTooLarge {
                item: 1,
                max: MAX_SWEEP_ITEM_BYTES as u64
            })
        );
        let many = SweepItems::Stored(SweepItemsRef {
            key: "k".to_owned(),
            digest: String::new(),
            items: MAX_SWEEP_ITEMS + 1,
            bytes: 0,
        });
        assert_eq!(check_items(&many).unwrap_err().code(), "too_many_items");
    }

    #[test]
    fn a_beam_item_counts_each_step_of_its_round() {
        let policy = SamplingPolicy {
            strategies: std::collections::BTreeSet::from([super::super::Strategy::Beam]),
            max_draws: 64,
            max_concurrent_draws: 8,
            ..SamplingPolicy::default()
        };
        let input = SweepInput::new(
            "k",
            SweepItems::inline(vec![SweepItem::new("a"), SweepItem::new("b")]),
            SampleStrategy::beam(2, 2, 3),
            ScorerRef::deterministic("exact_match").unwrap(),
        );
        let plan = plan(&policy, &input, 2).unwrap();
        assert_eq!(
            (plan.items, plan.draws),
            (2, 4),
            "2 items x 4 steps is the cap"
        );
        let wide = SweepInput {
            strategy: SampleStrategy::beam(4, 4, 1),
            ..input
        };
        let plan = super::plan(&policy, &wide, 2).unwrap();
        assert_eq!(
            (plan.items, plan.draws),
            (2, 4),
            "the cap bounds a wider round"
        );
    }

    #[test]
    fn a_sweep_input_refuses_an_unknown_key() {
        let value = serde_json::json!({
            "kind": "k", "items": {"inline": [{"input": "x"}]},
            "strategy": "single", "scorer": {"id": "exact_match"}, "strategies": [],
        });
        assert!(serde_json::from_value::<SweepInput>(value).is_err());
        let item = serde_json::json!({"input": "x", "draw_effects": []});
        assert!(serde_json::from_value::<SweepItem>(item).is_err());
    }
}
