//! Recursive cross-shard execution lineage tree (issue #621).
//!
//! When a saga or fan-out workflow spawns children that spawn grandchildren, a
//! DLQ/stuck alert lands with a single execution id and no map of the family it
//! belongs to. The only relationship primitive before this module was
//! [`store::load_workflow_children`](autumn_harvest::store::load_workflow_children),
//! which returns **direct** children from a **single shard** — doubly
//! insufficient for triage, because a spawned child gets a fresh
//! rendezvous-hashed [`ExecutionId`] and can land on a *different* shard than
//! its parent.
//!
//! This module owns the **pure** half of the answer: a bounded, cycle-safe
//! frontier walk ([`LineageWalk`]) plus the nesting/summarisation of its result.
//! The async cross-shard fan-out that feeds it lives in
//! [`crate::api::build_lineage_report`], which drives the walk level by level
//! over [`iter_shards`](crate::state::HarvestDbPool::iter_shards).
//!
//! ## Why the walk is a state machine rather than a recursive `async fn`
//!
//! Splitting "decide what to admit" from "go fetch it" keeps every bound —
//! node budget, depth cap, cycle rejection, deterministic truncation order —
//! unit-testable without a database, and keeps the DB layer to two batched
//! queries per level (`parent_id = ANY($1)` plus one existence probe) instead
//! of one query per parent per shard.
//!
//! ## Bounds
//!
//! - `max_depth` (default [`DEFAULT_LINEAGE_MAX_DEPTH`]) caps recursion depth.
//! - `max_nodes` (default [`DEFAULT_LINEAGE_MAX_NODES`]) caps total nodes,
//!   **including the root**.
//!
//! Hitting either sets `truncated: true` with a machine-readable
//! [`TruncationReason`] — never a silent cap — and names the parents whose
//! subtrees were dropped so an operator can re-root the call there and
//! continue.
//!
//! ## Consistency
//!
//! Shards are read independently, so a tree may be marginally time-skewed
//! across shards (a child on shard B may be observed a few milliseconds after
//! its parent on shard A). That is acceptable for incident triage and is
//! documented on the endpoint; there is no cross-shard transaction.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use autumn_harvest::models::WorkflowExecution;
use autumn_harvest::store::{AwaitMode, LineageChildRow};
use autumn_harvest::types::ExecutionId;
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::api::KNOWN_WORKFLOW_STATES;
use crate::shard_fanout::{FanoutStatus, UnavailableShard};

/// Default recursion depth cap (issue #621 AC).
pub const DEFAULT_LINEAGE_MAX_DEPTH: u8 = 20;
/// Default total-node cap, including the root (issue #621 AC).
pub const DEFAULT_LINEAGE_MAX_NODES: usize = 1000;
/// Hard ceiling on the caller-supplied `max_depth`.
///
/// A bound is what stops one triage call from turning into an unbounded scan,
/// so the *caller-supplied* bound is itself bounded.
pub const LINEAGE_MAX_DEPTH_CEILING: u8 = 50;
/// Hard ceiling on the caller-supplied `max_nodes`.
pub const LINEAGE_MAX_NODES_CEILING: usize = 10_000;
/// Cap on how many dropped-subtree parent ids are echoed in a truncated response.
///
/// A pathologically wide truncation must not itself produce an unbounded
/// response body. Overflow is reported by `truncated_parents_capped`.
pub const TRUNCATED_PARENTS_CAP: usize = 100;

/// Why a lineage walk stopped early.
///
/// Additive-only: new variants may be appended, but these two must never be
/// removed or renamed — a CI/alerting consumer keys on the wire string.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TruncationReason {
    /// The walk reached `max_depth` and at least one leaf provably has
    /// children that were not fetched.
    MaxDepth,
    /// The walk exhausted the `max_nodes` budget.
    MaxNodes,
}

/// Caller-supplied (or defaulted) bounds for one lineage walk.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
pub struct LineageLimits {
    /// Maximum recursion depth below the requested root.
    pub max_depth: u8,
    /// Maximum total nodes in the returned tree, **including** the root.
    pub max_nodes: usize,
}

impl Default for LineageLimits {
    fn default() -> Self {
        Self {
            max_depth: DEFAULT_LINEAGE_MAX_DEPTH,
            max_nodes: DEFAULT_LINEAGE_MAX_NODES,
        }
    }
}

impl LineageLimits {
    /// Validate caller-supplied bounds, falling back to the documented
    /// defaults when absent.
    ///
    /// Out-of-range values are **rejected**, not clamped: the response echoes
    /// the `limits` it applied, and silently clamping would make that echo lie
    /// about what the operator asked for.
    ///
    /// # Errors
    ///
    /// Returns a human-readable message naming the offending bound, which the
    /// HTTP layer surfaces as `400 Bad Request`.
    pub fn parse(max_depth: Option<u32>, max_nodes: Option<u32>) -> Result<Self, String> {
        let max_depth = match max_depth {
            None => DEFAULT_LINEAGE_MAX_DEPTH,
            Some(d) => {
                let ceiling = u32::from(LINEAGE_MAX_DEPTH_CEILING);
                if d == 0 || d > ceiling {
                    return Err(format!("max_depth must be between 1 and {ceiling}"));
                }
                u8::try_from(d).unwrap_or(LINEAGE_MAX_DEPTH_CEILING)
            }
        };
        let max_nodes = match max_nodes {
            None => DEFAULT_LINEAGE_MAX_NODES,
            Some(n) => {
                let ceiling = u32::try_from(LINEAGE_MAX_NODES_CEILING).unwrap_or(u32::MAX);
                if n == 0 || n > ceiling {
                    return Err(format!("max_nodes must be between 1 and {ceiling}"));
                }
                usize::try_from(n).unwrap_or(LINEAGE_MAX_NODES_CEILING)
            }
        };
        Ok(Self {
            max_depth,
            max_nodes,
        })
    }
}

/// One execution in the returned lineage tree.
#[derive(Debug, Clone, Serialize)]
pub struct LineageNode {
    /// This execution's id — the handle every other management route takes.
    pub execution_id: String,
    /// Registered workflow type name.
    pub workflow_name: String,
    /// Caller-supplied business workflow id.
    pub workflow_id: String,
    /// Raw persisted state (`RUNNING`, `FAILED`, …), matching the `state=`
    /// filter vocabulary of `GET /workflows`.
    pub state: String,
    /// When this execution started.
    pub started_at: DateTime<Utc>,
    /// When this execution reached a terminal state; `null` while it is live.
    pub closed_at: Option<DateTime<Utc>>,
    /// The parent that spawned this execution. `null` for a top-level run; a
    /// non-null value on the tree's own root means the operator re-rooted at a
    /// mid-tree node.
    pub parent_id: Option<String>,
    /// Distance below the tree's root. The root is always `0`, even when it is
    /// itself somebody's child.
    pub depth: u8,
    /// Shard this row was read from.
    pub shard_id: i32,
    /// Spawn relationship: `awaited` (the parent suspended on this child) or
    /// `detached` (the parent did not suspend).
    pub await_mode: AwaitMode,
    /// For a detached child, the policy applied when the parent closes.
    /// `null` for awaited children.
    pub parent_close_policy: Option<String>,
    /// Direct children, oldest-first. Empty for a leaf — never `null`.
    pub children: Vec<Self>,
}

/// Compact root identity echoed by the `?summary=true` response, so a
/// summary caller still knows *whose* roll-up this is (and the root's own
/// state, which the descendant counts deliberately exclude).
#[derive(Debug, Clone, Serialize)]
pub struct LineageRootRef {
    /// The root execution's id.
    pub execution_id: String,
    /// The root's registered workflow type name.
    pub workflow_name: String,
    /// The root's business workflow id.
    pub workflow_id: String,
    /// The root's own state — **not** included in `counts`.
    pub state: String,
}

/// Full nested lineage tree (the default response body).
#[derive(Debug, Clone, Serialize)]
pub struct LineageTreeReport {
    /// The requested execution, with its descendants nested beneath it.
    pub root: LineageNode,
    /// Total nodes in the tree, including the root.
    pub node_count: usize,
    /// Deepest level actually returned (`0` for a leaf root).
    pub max_depth_reached: u8,
    /// `true` when a bound stopped the walk before the tree was complete.
    pub truncated: bool,
    /// Which bound stopped the walk; `null` when `truncated` is false.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncation_reason: Option<TruncationReason>,
    /// Execution ids of nodes in the returned tree whose child subtrees were
    /// dropped. Re-root the call at one of these to continue the walk.
    ///
    /// Every id listed **provably** has children (either observed and dropped
    /// for budget, or confirmed by the existence probe) — a childless leaf is
    /// never named, so an operator is never sent chasing a subtree that does
    /// not exist. On a `max_nodes` truncation the list may be *incomplete*: a
    /// parent whose children all fell beyond the per-shard fetch window is not
    /// observed. `truncated` is the authoritative "this tree is incomplete"
    /// signal; this list is the actionable, bounded subset.
    pub truncated_parent_ids: Vec<String>,
    /// `true` when more than [`TRUNCATED_PARENTS_CAP`] parents were dropped and
    /// the list above was capped.
    pub truncated_parents_capped: bool,
    /// The bounds actually applied to this walk.
    pub limits: LineageLimits,
    /// Cross-shard completeness of the read (issue #756). A `partial` tree is
    /// missing whatever lives on the unavailable shards.
    pub status: FanoutStatus,
    /// Shards that could not be read, named with a reason. Empty when
    /// `status == complete`.
    pub unavailable_shards: Vec<UnavailableShard>,
}

/// Per-state descendant roll-up (the `?summary=true` response body).
#[derive(Debug, Clone, Serialize)]
pub struct LineageSummaryReport {
    /// Identity (and own state) of the subtree's root.
    pub root: LineageRootRef,
    /// Per-state **descendant** counts, keyed by the lowercased state name.
    /// Every state in the management API's known-state vocabulary is always
    /// present (at `0` when absent), so a client can index without a presence
    /// check. The root itself is not counted.
    pub counts: BTreeMap<String, i64>,
    /// Total descendants (== the sum of `counts`), excluding the root.
    pub total_descendants: usize,
    /// Deepest level reached, as in the tree response.
    pub max_depth_reached: u8,
    /// `true` when a bound stopped the walk before the subtree was complete —
    /// so the counts below are a lower bound.
    pub truncated: bool,
    /// Which bound stopped the walk; `null` when `truncated` is false.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncation_reason: Option<TruncationReason>,
    /// Dropped-subtree parents, as in the tree response.
    pub truncated_parent_ids: Vec<String>,
    /// `true` when the list above was capped.
    pub truncated_parents_capped: bool,
    /// The bounds actually applied.
    pub limits: LineageLimits,
    /// Cross-shard completeness of the read (issue #756).
    pub status: FanoutStatus,
    /// Shards that could not be read.
    pub unavailable_shards: Vec<UnavailableShard>,
}

/// Project a lineage row into the tree's **root** node: depth `0`, no children
/// yet, and its own `parent_id` preserved so a mid-tree re-root is visible as
/// such.
#[must_use]
pub fn lineage_root_node(row: &LineageChildRow) -> LineageNode {
    LineageNode {
        execution_id: row.exec_id.to_string(),
        workflow_name: row.workflow_name.clone(),
        workflow_id: row.workflow_id.clone(),
        state: row.state.clone(),
        started_at: row.started_at,
        closed_at: row.completed_at,
        parent_id: row.parent_id.map(|p| p.to_string()),
        depth: 0,
        shard_id: row.shard_id,
        await_mode: row.await_mode,
        parent_close_policy: row.parent_close_policy.map(|p| p.as_str().to_string()),
        children: Vec::new(),
    }
}

/// Project the requested root's full execution row into the lineage row shape,
/// so the root and its descendants are built by exactly one code path.
#[must_use]
pub fn root_row_from_execution(execution: &WorkflowExecution) -> LineageChildRow {
    let parent_close_policy = execution
        .parent_close_policy
        .as_deref()
        .and_then(|s| s.parse().ok());
    LineageChildRow {
        exec_id: ExecutionId::from_uuid(execution.id),
        parent_id: execution.parent_id.map(ExecutionId::from_uuid),
        workflow_name: execution.workflow_name.clone(),
        workflow_id: execution.workflow_id.clone(),
        state: execution.state.clone(),
        started_at: execution.started_at,
        completed_at: execution.completed_at,
        shard_id: execution.shard_id,
        // Same derivation as the batch loader: a detached child is exactly one
        // carrying a parent-close policy.
        await_mode: if parent_close_policy.is_some() {
            AwaitMode::Detached
        } else {
            AwaitMode::Awaited
        },
        parent_close_policy,
    }
}

/// Bounded, cycle-safe frontier walk over `parent_id` edges.
///
/// The caller drives it level by level: fetch the current frontier's children
/// (across shards), hand them to [`admit_level`](Self::admit_level), and repeat
/// with the returned next frontier until it is empty, the depth cap is reached,
/// or [`is_exhausted`](Self::is_exhausted) reports the node budget is spent.
/// Then probe that final, unexpanded frontier for surviving children, feed the
/// answer to [`record_probe_result`](Self::record_probe_result), and
/// [`finish`](Self::finish).
#[derive(Debug)]
pub struct LineageWalk {
    limits: LineageLimits,
    /// Every id ever admitted (root included). This is the cycle/duplicate
    /// guard: an id is admitted at most once, so a malformed `parent_id` loop
    /// terminates instead of recursing forever.
    visited: HashSet<uuid::Uuid>,
    /// Admitted descendant rows, in admission order.
    nodes: Vec<LineageChildRow>,
    /// Ids admitted at the most recent level — the leaves the walk never
    /// expanded, and therefore the existence probe's input.
    /// Parents whose children were observed and dropped for budget, plus any
    /// the probe confirmed. Sorted/deduped by the `BTreeSet`.
    dropped_parents: BTreeSet<uuid::Uuid>,
    reason: Option<TruncationReason>,
    max_depth_reached: u8,
}

impl LineageWalk {
    /// Start a walk rooted at `root_id`.
    #[must_use]
    pub fn new(root_id: ExecutionId, limits: LineageLimits) -> Self {
        let mut visited = HashSet::new();
        visited.insert(root_id.as_uuid());
        Self {
            limits,
            visited,
            nodes: Vec::new(),
            dropped_parents: BTreeSet::new(),
            reason: None,
            max_depth_reached: 0,
        }
    }

    /// Total nodes admitted so far, including the root.
    #[must_use]
    pub const fn node_count(&self) -> usize {
        self.nodes.len() + 1
    }

    /// How many more nodes may be admitted before the budget is spent.
    #[must_use]
    pub const fn remaining_budget(&self) -> usize {
        self.limits.max_nodes.saturating_sub(self.node_count())
    }

    /// `true` when no further node may be admitted.
    #[must_use]
    pub const fn is_exhausted(&self) -> bool {
        self.remaining_budget() == 0
    }

    /// `true` when a bound has stopped the walk short of a complete tree.
    #[must_use]
    pub const fn truncated(&self) -> bool {
        self.reason.is_some()
    }

    /// Which bound stopped the walk, if any.
    #[must_use]
    pub const fn truncation_reason(&self) -> Option<TruncationReason> {
        self.reason
    }

    /// Parents whose subtrees were dropped, as raw uuids.
    #[must_use]
    pub fn dropped_parent_ids(&self) -> Vec<uuid::Uuid> {
        self.dropped_parents.iter().copied().collect()
    }

    /// Admit one level's discovered rows and return the next frontier.
    ///
    /// Rows arrive per-shard and are therefore re-sorted `(started_at,
    /// exec_id)` before any budget cut: which rows survive a truncation must
    /// not depend on shard iteration order, or the tree an operator sees would
    /// vary between identical calls.
    pub fn admit_level(&mut self, depth: u8, mut rows: Vec<LineageChildRow>) -> Vec<uuid::Uuid> {
        rows.sort_by(|a, b| {
            a.started_at
                .cmp(&b.started_at)
                .then_with(|| a.exec_id.as_uuid().cmp(&b.exec_id.as_uuid()))
        });

        let mut next = Vec::new();
        for row in rows {
            let uuid = row.exec_id.as_uuid();
            // Cycle/duplicate guard. Also rejects a self-parent row (the root
            // is pre-seeded) and the same row surfacing from two shards.
            if !self.visited.insert(uuid) {
                continue;
            }
            if self.is_exhausted() {
                // Over budget: keep scanning (rather than breaking) so every
                // observed dropped parent is named, not just the first.
                //
                // The id stays in `visited`. Exhaustion is monotone — once
                // `is_exhausted()` is true it never becomes false again — so
                // no admission can follow a rejection, and leaving the id
                // marked keeps a duplicate of a rejected row from being
                // admitted if that invariant is ever relaxed.
                self.reason.get_or_insert(TruncationReason::MaxNodes);
                if let Some(parent) = row.parent_id {
                    self.dropped_parents.insert(parent.as_uuid());
                }
                continue;
            }
            next.push(uuid);
            self.nodes.push(row);
        }

        if !next.is_empty() {
            self.max_depth_reached = self.max_depth_reached.max(depth);
        }
        next
    }

    /// Record which unexpanded leaves provably have children.
    ///
    /// Only ids returned by the probe are named as dropped subtrees, so a
    /// childless leaf is never reported as truncated. An empty result is the
    /// signal that the walk genuinely finished — it must not truncate.
    ///
    /// Which bound to blame is decided by the live budget rather than assumed
    /// to be the depth cap: a level can spend the budget *exactly* (no row is
    /// ever rejected, so `admit_level` never sets a reason) and still be what
    /// stopped the walk. Blaming `max_depth` there would send an operator to
    /// raise a bound that was nowhere near its limit.
    pub fn record_probe_result(&mut self, parents_with_children: &[uuid::Uuid]) {
        if parents_with_children.is_empty() {
            return;
        }
        let bound = if self.is_exhausted() {
            TruncationReason::MaxNodes
        } else {
            TruncationReason::MaxDepth
        };
        // `get_or_insert` preserves a verdict already set by a budget-rejected
        // row: that bound terminated the walk first, so it stays reported.
        self.reason.get_or_insert(bound);
        self.dropped_parents
            .extend(parents_with_children.iter().copied());
    }

    /// Note that a shard's fetch window came back full.
    ///
    /// `fetch_limit` is `remaining_budget + 1` precisely so an overflow row is
    /// *observed* and its parent named. That sentinel is defeated when a row
    /// in the window is dropped by the cycle/duplicate guard, which consumes a
    /// slot without consuming budget — the SQL `LIMIT` may then have cut real
    /// rows that were never seen. A saturated window with no budget rejection
    /// is therefore reported as a `MaxNodes` truncation: over-reporting sends
    /// an operator to re-root needlessly, while under-reporting is the silent
    /// cap the bounds exist to prevent.
    pub fn note_saturated_fetch_window(&mut self) {
        self.reason.get_or_insert(TruncationReason::MaxNodes);
    }

    /// Nest the admitted rows beneath `root` and produce the report.
    ///
    /// `status`/`unavailable_shards` default to a complete read; the async
    /// driver overwrites them via [`LineageTreeReport`]'s public fields.
    #[must_use]
    pub fn finish(self, root: LineageNode) -> LineageTreeReport {
        let node_count = self.node_count();
        let max_depth_reached = self.max_depth_reached;
        let truncated = self.truncated();
        let truncation_reason = self.reason;

        let all_dropped: Vec<uuid::Uuid> = self.dropped_parents.iter().copied().collect();
        let truncated_parents_capped = all_dropped.len() > TRUNCATED_PARENTS_CAP;
        let truncated_parent_ids: Vec<String> = all_dropped
            .into_iter()
            .take(TRUNCATED_PARENTS_CAP)
            .map(|u| ExecutionId::from_uuid(u).to_string())
            .collect();

        // Group by parent, then walk down from the root. Recursion depth is
        // bounded by `max_depth` (≤ LINEAGE_MAX_DEPTH_CEILING), so this cannot
        // overflow the stack.
        let mut by_parent: HashMap<uuid::Uuid, Vec<LineageChildRow>> = HashMap::new();
        for row in self.nodes {
            if let Some(parent) = row.parent_id {
                by_parent.entry(parent.as_uuid()).or_default().push(row);
            }
        }
        for children in by_parent.values_mut() {
            children.sort_by(|a, b| {
                a.started_at
                    .cmp(&b.started_at)
                    .then_with(|| a.exec_id.as_uuid().cmp(&b.exec_id.as_uuid()))
            });
        }

        let mut root = root;
        attach_children(&mut root, &mut by_parent, 1);

        LineageTreeReport {
            root,
            node_count,
            max_depth_reached,
            truncated,
            truncation_reason,
            truncated_parent_ids,
            truncated_parents_capped,
            limits: self.limits,
            status: FanoutStatus::Complete,
            unavailable_shards: Vec::new(),
        }
    }
}

/// Recursively attach `by_parent` rows beneath `node`, stamping each level's
/// depth. Rows are *removed* from the map as they are attached, so a malformed
/// duplicate edge cannot cause the same subtree to be materialised twice.
fn attach_children(
    node: &mut LineageNode,
    by_parent: &mut HashMap<uuid::Uuid, Vec<LineageChildRow>>,
    depth: u8,
) {
    let Ok(parent_uuid) = node.execution_id.parse::<uuid::Uuid>() else {
        return;
    };
    let Some(rows) = by_parent.remove(&parent_uuid) else {
        return;
    };
    for row in rows {
        let mut child = LineageNode {
            execution_id: row.exec_id.to_string(),
            workflow_name: row.workflow_name,
            workflow_id: row.workflow_id,
            state: row.state,
            started_at: row.started_at,
            closed_at: row.completed_at,
            parent_id: row.parent_id.map(|p| p.to_string()),
            depth,
            shard_id: row.shard_id,
            await_mode: row.await_mode,
            parent_close_policy: row.parent_close_policy.map(|p| p.as_str().to_string()),
            children: Vec::new(),
        };
        attach_children(&mut child, by_parent, depth.saturating_add(1));
        node.children.push(child);
    }
}

impl LineageTreeReport {
    /// Collapse the tree into a per-state descendant roll-up.
    ///
    /// The root is deliberately excluded from `counts` (they are *descendant*
    /// counts) and echoed separately on `root`, so "does anything under here
    /// look bad?" is answerable without reasoning about whether the node you
    /// already know about is included.
    #[must_use]
    pub fn into_summary(self) -> LineageSummaryReport {
        let mut counts: BTreeMap<String, i64> = KNOWN_WORKFLOW_STATES
            .iter()
            .map(|s| (s.to_ascii_lowercase(), 0))
            .collect();
        let mut total_descendants = 0usize;
        collect_states(&self.root, &mut counts, &mut total_descendants, true);

        LineageSummaryReport {
            root: LineageRootRef {
                execution_id: self.root.execution_id,
                workflow_name: self.root.workflow_name,
                workflow_id: self.root.workflow_id,
                state: self.root.state,
            },
            counts,
            total_descendants,
            max_depth_reached: self.max_depth_reached,
            truncated: self.truncated,
            truncation_reason: self.truncation_reason,
            truncated_parent_ids: self.truncated_parent_ids,
            truncated_parents_capped: self.truncated_parents_capped,
            limits: self.limits,
            status: self.status,
            unavailable_shards: self.unavailable_shards,
        }
    }
}

/// Depth-first state tally. `is_root` skips the root's own state so `counts`
/// stays a pure descendant roll-up. An unrecognised state is still counted
/// under its own key rather than silently dropped.
fn collect_states(
    node: &LineageNode,
    counts: &mut BTreeMap<String, i64>,
    total: &mut usize,
    is_root: bool,
) {
    if !is_root {
        *counts.entry(node.state.to_ascii_lowercase()).or_insert(0) += 1;
        *total += 1;
    }
    for child in &node.children {
        collect_states(child, counts, total, false);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use autumn_harvest::types::{ParentClosePolicy, ShardId};
    use chrono::{Duration, TimeZone};

    fn t(offset: i64) -> chrono::DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000, 0).unwrap() + Duration::seconds(offset)
    }

    fn id(n: u128) -> ExecutionId {
        ExecutionId::from_uuid(uuid::Uuid::from_u128(n))
    }

    fn row(exec: ExecutionId, parent: ExecutionId, state: &str, offset: i64) -> LineageChildRow {
        LineageChildRow {
            exec_id: exec,
            parent_id: Some(parent),
            workflow_name: "child_wf".to_string(),
            workflow_id: format!("wf-{}", exec.as_uuid().as_u128()),
            state: state.to_string(),
            started_at: t(offset),
            completed_at: None,
            shard_id: 0,
            await_mode: AwaitMode::Awaited,
            parent_close_policy: None,
        }
    }

    fn root_node(exec: ExecutionId) -> LineageNode {
        LineageNode {
            execution_id: exec.to_string(),
            workflow_name: "root_wf".to_string(),
            workflow_id: "wf-root".to_string(),
            state: "RUNNING".to_string(),
            started_at: t(0),
            closed_at: None,
            parent_id: None,
            depth: 0,
            shard_id: 0,
            await_mode: AwaitMode::Awaited,
            parent_close_policy: None,
            children: Vec::new(),
        }
    }

    fn generous() -> LineageLimits {
        LineageLimits {
            max_depth: DEFAULT_LINEAGE_MAX_DEPTH,
            max_nodes: DEFAULT_LINEAGE_MAX_NODES,
        }
    }

    // ── Limits parsing / validation ──────────────────────────────────────

    /// The node ceiling as the `u32` the HTTP query layer hands us.
    fn nodes_ceiling_u32() -> u32 {
        u32::try_from(LINEAGE_MAX_NODES_CEILING).expect("node ceiling fits in u32")
    }

    #[test]
    fn limits_default_to_the_documented_values() {
        let limits = LineageLimits::parse(None, None).expect("defaults are valid");
        assert_eq!(limits.max_depth, 20, "AC: max_depth defaults to 20");
        assert_eq!(limits.max_nodes, 1000, "AC: max_nodes defaults to 1000");
    }

    #[test]
    fn limits_reject_out_of_range_values_rather_than_silently_clamping() {
        // A silently-clamped bound would make the response's `limits` echo lie
        // about what was actually applied.
        assert!(LineageLimits::parse(Some(0), None).is_err(), "depth 0");
        assert!(LineageLimits::parse(None, Some(0)).is_err(), "nodes 0");
        assert!(
            LineageLimits::parse(Some(u32::from(LINEAGE_MAX_DEPTH_CEILING) + 1), None).is_err(),
            "depth above ceiling"
        );
        assert!(
            LineageLimits::parse(None, Some(nodes_ceiling_u32() + 1)).is_err(),
            "nodes above ceiling"
        );
    }

    #[test]
    fn limits_accept_values_at_the_ceiling() {
        let limits = LineageLimits::parse(
            Some(u32::from(LINEAGE_MAX_DEPTH_CEILING)),
            Some(nodes_ceiling_u32()),
        )
        .expect("ceiling is inclusive");
        assert_eq!(limits.max_depth, LINEAGE_MAX_DEPTH_CEILING);
        assert_eq!(limits.max_nodes, LINEAGE_MAX_NODES_CEILING);
    }

    // ── Frontier admission ───────────────────────────────────────────────

    #[test]
    fn admit_level_returns_the_next_frontier() {
        let root = id(1);
        let mut walk = LineageWalk::new(root, generous());
        let next = walk.admit_level(
            1,
            vec![
                row(id(2), root, "RUNNING", 1),
                row(id(3), root, "RUNNING", 2),
            ],
        );
        assert_eq!(next.len(), 2, "both children become the next frontier");
        assert!(next.contains(&id(2).as_uuid()));
        assert!(next.contains(&id(3).as_uuid()));
    }

    #[test]
    fn a_saturated_fetch_window_is_reported_as_a_budget_truncation() {
        // The `remaining + 1` fetch sentinel assumes the overflow row reaches
        // the budget check. A row dropped by the cycle/duplicate guard
        // consumes a window slot WITHOUT consuming budget, so the SQL LIMIT
        // may have cut real rows that were never observed. Over-reporting is
        // the safe direction; a silent cap is the one the bounds forbid.
        let root = id(1);
        let mut walk = LineageWalk::new(
            root,
            LineageLimits {
                max_depth: 20,
                max_nodes: 3,
            },
        );
        // Window is [duplicate-of-root, c1]: the first is deduped (no budget
        // spent), the second fits — so nothing is ever budget-rejected.
        walk.admit_level(
            1,
            vec![
                row(root, root, "RUNNING", 0),
                row(id(2), root, "RUNNING", 1),
            ],
        );
        assert!(
            !walk.truncated(),
            "nothing has signalled a cut yet at this point"
        );

        walk.note_saturated_fetch_window();
        assert!(
            walk.truncated(),
            "a full window means rows may have been cut"
        );
        assert_eq!(walk.truncation_reason(), Some(TruncationReason::MaxNodes));
    }

    #[test]
    fn a_saturated_window_does_not_override_an_existing_verdict() {
        let root = id(1);
        let mut walk = LineageWalk::new(
            root,
            LineageLimits {
                max_depth: 20,
                max_nodes: 2,
            },
        );
        walk.admit_level(
            1,
            vec![
                row(id(2), root, "RUNNING", 1),
                row(id(3), root, "RUNNING", 2),
            ],
        );
        assert_eq!(walk.truncation_reason(), Some(TruncationReason::MaxNodes));
        walk.note_saturated_fetch_window();
        assert_eq!(
            walk.truncation_reason(),
            Some(TruncationReason::MaxNodes),
            "the reason already set by a rejected row stands"
        );
    }

    #[test]
    fn admit_level_sorts_by_started_at_not_by_execution_id() {
        // Ids are DECORRELATED from timestamps here on purpose: real
        // execution ids are random, so a fixture where uuid order happens to
        // match `started_at` order cannot tell a correct sort from one keyed
        // on the id. `started_at` must win.
        let root = id(1);
        let mut walk = LineageWalk::new(root, generous());
        let next = walk.admit_level(
            1,
            vec![
                row(id(100), root, "RUNNING", 30), // lowest id, NEWEST
                row(id(300), root, "RUNNING", 10), // highest id, OLDEST
                row(id(200), root, "RUNNING", 20),
            ],
        );
        assert_eq!(
            next,
            vec![id(300).as_uuid(), id(200).as_uuid(), id(100).as_uuid()],
            "oldest-first by started_at, which here is the REVERSE of id order"
        );
    }

    #[test]
    fn admit_level_breaks_started_at_ties_by_execution_id() {
        // Without a total order, two rows sharing a timestamp could survive a
        // truncation in either order depending on shard iteration.
        let root = id(1);
        let mut walk = LineageWalk::new(root, generous());
        let next = walk.admit_level(
            1,
            vec![
                row(id(900), root, "RUNNING", 5),
                row(id(400), root, "RUNNING", 5),
                row(id(700), root, "RUNNING", 5),
            ],
        );
        assert_eq!(
            next,
            vec![id(400).as_uuid(), id(700).as_uuid(), id(900).as_uuid()],
            "identical started_at falls back to exec_id, matching the SQL ORDER BY"
        );
    }

    #[test]
    fn admit_level_sorts_cross_shard_rows_deterministically() {
        // Rows arrive per-shard, so the merged level must be re-sorted before
        // any node-budget cut — otherwise which rows survive truncation would
        // depend on shard iteration order.
        let root = id(1);
        let mut walk = LineageWalk::new(root, generous());
        let next = walk.admit_level(
            1,
            vec![
                row(id(30), root, "RUNNING", 30),
                row(id(10), root, "RUNNING", 10),
                row(id(20), root, "RUNNING", 20),
            ],
        );
        assert_eq!(
            next,
            vec![id(10).as_uuid(), id(20).as_uuid(), id(30).as_uuid()],
            "oldest-first regardless of arrival order"
        );
    }

    // ── Cycle / duplicate safety (AC) ────────────────────────────────────

    #[test]
    fn a_self_parent_row_is_rejected_by_the_visited_set() {
        // Malformed data: A claims itself as its own parent.
        let root = id(1);
        let mut walk = LineageWalk::new(root, generous());
        let next = walk.admit_level(1, vec![row(root, root, "RUNNING", 1)]);
        assert!(
            next.is_empty(),
            "the root must never be re-admitted as its own child"
        );
        assert_eq!(walk.node_count(), 1, "only the root counts");
    }

    #[test]
    fn a_two_node_cycle_terminates_instead_of_recursing_forever() {
        // A -> B, then B -> A (malformed back-edge).
        let a = id(1);
        let b = id(2);
        let mut walk = LineageWalk::new(a, generous());
        let level1 = walk.admit_level(1, vec![row(b, a, "RUNNING", 1)]);
        assert_eq!(level1, vec![b.as_uuid()]);
        let level2 = walk.admit_level(2, vec![row(a, b, "RUNNING", 2)]);
        assert!(level2.is_empty(), "the cycle back-edge is dropped");
    }

    #[test]
    fn a_duplicate_row_from_two_shards_appears_once() {
        let root = id(1);
        let dup = id(2);
        let mut walk = LineageWalk::new(root, generous());
        let next = walk.admit_level(
            1,
            vec![row(dup, root, "RUNNING", 1), row(dup, root, "RUNNING", 1)],
        );
        assert_eq!(next.len(), 1, "AC: each execution_id appears exactly once");
        assert_eq!(walk.node_count(), 2, "root + one child");
    }

    // ── max_nodes truncation (AC: no silent cap) ─────────────────────────

    #[test]
    fn max_nodes_truncation_sets_the_reason_and_names_the_dropped_parent() {
        let root = id(1);
        let mut walk = LineageWalk::new(
            root,
            LineageLimits {
                max_depth: 20,
                max_nodes: 2, // root + 1 child
            },
        );
        let next = walk.admit_level(
            1,
            vec![
                row(id(10), root, "RUNNING", 10),
                row(id(20), root, "RUNNING", 20),
            ],
        );
        assert_eq!(next.len(), 1, "only the budgeted child is admitted");
        assert_eq!(next[0], id(10).as_uuid(), "oldest survives (deterministic)");
        assert!(walk.truncated(), "AC: truncation is never silent");
        assert_eq!(walk.truncation_reason(), Some(TruncationReason::MaxNodes));
        assert!(
            walk.dropped_parent_ids().contains(&root.as_uuid()),
            "AC: the dropped subtree is named by its parent execution_id"
        );
    }

    #[test]
    fn remaining_budget_shrinks_as_nodes_are_admitted_and_never_underflows() {
        let root = id(1);
        let mut walk = LineageWalk::new(
            root,
            LineageLimits {
                max_depth: 20,
                max_nodes: 3,
            },
        );
        assert_eq!(walk.remaining_budget(), 2, "root already consumes one slot");
        walk.admit_level(1, vec![row(id(2), root, "RUNNING", 1)]);
        assert_eq!(walk.remaining_budget(), 1);
        walk.admit_level(2, vec![row(id(3), id(2), "RUNNING", 2)]);
        assert_eq!(walk.remaining_budget(), 0);
        assert!(walk.is_exhausted());
    }

    #[test]
    fn recording_a_probe_result_marks_max_depth_truncation() {
        let root = id(1);
        let mut walk = LineageWalk::new(
            root,
            LineageLimits {
                max_depth: 1,
                max_nodes: 1000,
            },
        );
        walk.admit_level(1, vec![row(id(2), root, "RUNNING", 1)]);
        assert!(
            !walk.truncated(),
            "not truncated until the probe proves children exist"
        );
        walk.record_probe_result(&[id(2).as_uuid()]);
        assert!(walk.truncated());
        assert_eq!(walk.truncation_reason(), Some(TruncationReason::MaxDepth));
        assert!(walk.dropped_parent_ids().contains(&id(2).as_uuid()));
    }

    #[test]
    fn a_probe_that_finds_nothing_leaves_the_tree_untruncated() {
        // The honest answer for a tree that happens to END exactly at the depth
        // cap: nothing was dropped, so `truncated` must stay false.
        let root = id(1);
        let mut walk = LineageWalk::new(
            root,
            LineageLimits {
                max_depth: 1,
                max_nodes: 1000,
            },
        );
        walk.admit_level(1, vec![row(id(2), root, "RUNNING", 1)]);
        walk.record_probe_result(&[]);
        assert!(!walk.truncated(), "no false-positive truncation");
        assert_eq!(walk.truncation_reason(), None);
        assert!(walk.dropped_parent_ids().is_empty());
    }

    #[test]
    fn max_nodes_wins_the_reason_when_both_bounds_are_hit() {
        // max_nodes terminated the walk, so it is the reported reason.
        let root = id(1);
        let mut walk = LineageWalk::new(
            root,
            LineageLimits {
                max_depth: 1,
                max_nodes: 1,
            },
        );
        walk.admit_level(1, vec![row(id(2), root, "RUNNING", 1)]);
        assert_eq!(walk.truncation_reason(), Some(TruncationReason::MaxNodes));
        walk.record_probe_result(&[root.as_uuid()]);
        assert_eq!(
            walk.truncation_reason(),
            Some(TruncationReason::MaxNodes),
            "the first bound that terminated the walk is reported"
        );
    }

    // ── Nesting ──────────────────────────────────────────────────────────

    #[test]
    fn finish_nests_a_three_level_tree_under_the_root() {
        let root = id(1);
        let mut walk = LineageWalk::new(root, generous());
        walk.admit_level(1, vec![row(id(2), root, "RUNNING", 1)]);
        walk.admit_level(2, vec![row(id(3), id(2), "FAILED", 2)]);
        let report = walk.finish(root_node(root));

        assert_eq!(report.node_count, 3);
        assert_eq!(report.max_depth_reached, 2);
        assert_eq!(report.root.children.len(), 1);
        let child = &report.root.children[0];
        assert_eq!(child.execution_id, id(2).to_string());
        assert_eq!(child.depth, 1);
        assert_eq!(child.parent_id.as_deref(), Some(root.to_string().as_str()));
        assert_eq!(child.children.len(), 1);
        let grandchild = &child.children[0];
        assert_eq!(grandchild.execution_id, id(3).to_string());
        assert_eq!(grandchild.depth, 2);
        assert_eq!(grandchild.state, "FAILED");
    }

    #[test]
    fn a_leaf_root_returns_an_empty_children_array_not_an_error() {
        let root = id(1);
        let walk = LineageWalk::new(root, generous());
        let report = walk.finish(root_node(root));
        assert!(
            report.root.children.is_empty(),
            "AC: empty children, not an error"
        );
        assert_eq!(report.node_count, 1);
        assert_eq!(report.max_depth_reached, 0);
        assert!(!report.truncated);
    }

    #[test]
    fn siblings_are_nested_oldest_first_for_a_stable_rendering() {
        let root = id(1);
        let mut walk = LineageWalk::new(root, generous());
        walk.admit_level(
            1,
            vec![
                row(id(30), root, "RUNNING", 30),
                row(id(10), root, "RUNNING", 10),
                row(id(20), root, "RUNNING", 20),
            ],
        );
        let report = walk.finish(root_node(root));
        let ids: Vec<String> = report
            .root
            .children
            .iter()
            .map(|c| c.execution_id.clone())
            .collect();
        assert_eq!(
            ids,
            vec![id(10).to_string(), id(20).to_string(), id(30).to_string()]
        );
    }

    #[test]
    fn the_spawn_relationship_is_carried_on_every_node() {
        let root = id(1);
        let mut walk = LineageWalk::new(root, generous());
        let mut detached = row(id(2), root, "RUNNING", 1);
        detached.await_mode = AwaitMode::Detached;
        detached.parent_close_policy = Some(ParentClosePolicy::RequestCancel);
        walk.admit_level(1, vec![detached]);
        let report = walk.finish(root_node(root));
        let child = &report.root.children[0];
        assert_eq!(child.await_mode, AwaitMode::Detached);
        assert_eq!(
            child.parent_close_policy.as_deref(),
            Some("request_cancel"),
            "AC: detached children carry their parent_close_policy"
        );
    }

    #[test]
    fn dropped_parent_ids_are_capped_and_flagged() {
        let root = id(1);
        let mut walk = LineageWalk::new(root, generous());
        let many: Vec<uuid::Uuid> = (0..(TRUNCATED_PARENTS_CAP as u128 + 5))
            .map(|n| id(1000 + n).as_uuid())
            .collect();
        walk.record_probe_result(&many);
        let report = walk.finish(root_node(root));
        assert_eq!(report.truncated_parent_ids.len(), TRUNCATED_PARENTS_CAP);
        assert!(report.truncated_parents_capped, "overflow must be flagged");
    }

    // ── Summary mode ─────────────────────────────────────────────────────

    #[test]
    fn summary_counts_descendants_by_state_and_excludes_the_root() {
        let root = id(1);
        let mut walk = LineageWalk::new(root, generous());
        walk.admit_level(
            1,
            vec![
                row(id(2), root, "RUNNING", 1),
                row(id(3), root, "RUNNING", 2),
                row(id(4), root, "FAILED", 3),
            ],
        );
        walk.admit_level(2, vec![row(id(5), id(2), "COMPLETED", 4)]);
        let report = walk.finish(root_node(root));
        let summary = report.into_summary();

        assert_eq!(summary.total_descendants, 4);
        assert_eq!(summary.counts.get("running"), Some(&2));
        assert_eq!(summary.counts.get("failed"), Some(&1));
        assert_eq!(summary.counts.get("completed"), Some(&1));
        // The root is RUNNING but must NOT be counted among its own descendants.
        assert_eq!(
            summary.counts.values().sum::<i64>(),
            4,
            "AC: per-state DESCENDANT counts"
        );
        assert_eq!(
            summary.root.state, "RUNNING",
            "root state is echoed separately"
        );
    }

    #[test]
    fn summary_always_reports_every_known_state_key_even_at_zero() {
        // A client checking `counts["failed"] > 0` must not have to handle a
        // missing key.
        let root = id(1);
        let walk = LineageWalk::new(root, generous());
        let summary = walk.finish(root_node(root)).into_summary();
        for state in [
            "running",
            "failed",
            "completed",
            "cancelled",
            "timed_out",
            "terminated",
        ] {
            assert_eq!(
                summary.counts.get(state),
                Some(&0),
                "state key `{state}` must always be present"
            );
        }
        assert_eq!(summary.total_descendants, 0);
    }

    #[test]
    fn summary_preserves_the_truncation_verdict() {
        let root = id(1);
        let mut walk = LineageWalk::new(
            root,
            LineageLimits {
                max_depth: 20,
                max_nodes: 1,
            },
        );
        walk.admit_level(1, vec![row(id(2), root, "RUNNING", 1)]);
        let summary = walk.finish(root_node(root)).into_summary();
        assert!(summary.truncated);
        assert_eq!(summary.truncation_reason, Some(TruncationReason::MaxNodes));
        assert!(!summary.truncated_parent_ids.is_empty());
    }

    // ── Root node projection ─────────────────────────────────────────────

    #[test]
    fn a_mid_tree_root_reports_its_own_parent_and_depth_zero() {
        // AC: "Rooting at a mid-tree node returns just the subtree below it."
        // The chosen root is always depth 0 even when it has a parent.
        let parent = id(99);
        let me = id(1);
        let node = lineage_root_node(&LineageChildRow {
            exec_id: me,
            parent_id: Some(parent),
            workflow_name: "mid_wf".to_string(),
            workflow_id: "wf-mid".to_string(),
            state: "RUNNING".to_string(),
            started_at: t(0),
            completed_at: None,
            shard_id: 3,
            await_mode: AwaitMode::Detached,
            parent_close_policy: Some(ParentClosePolicy::Terminate),
        });
        assert_eq!(node.depth, 0);
        assert_eq!(node.parent_id.as_deref(), Some(parent.to_string().as_str()));
        assert_eq!(node.shard_id, 3);
        assert_eq!(node.parent_close_policy.as_deref(), Some("terminate"));
        assert!(node.children.is_empty());
        let _ = ShardId::new(0);
    }
}
