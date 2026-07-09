//! Continue-as-new run-chain assembly (issue #701).
//!
//! A workflow that calls `continue_as_new` forks forward into a fresh execution
//! that shares its `workflow_id` and shard but has a brand-new `exec_id`. Over a
//! long-lived, self-continuing workflow this produces a *chain* of executions.
//! This module turns a flat set of chain-member rows into an ordered, operator-
//! facing view (`RunChainResponse`) so a single logical run's whole history is
//! discoverable from any one of its members.
//!
//! This module is **pure** — it takes already-loaded rows and produces the wire
//! response. The DB query that loads the rows and the HTTP route that serves the
//! response live in the plugin crate.
//!
//! ## Chain back-links (issue #701)
//!
//! Two nullable columns on `harvest_workflow_executions` index the chain:
//! - `continued_from_exec_id` — the immediate predecessor.
//! - `first_exec_id` — the chain **origin**, denormalized onto every successor so
//!   any member resolves the head in one indexed lookup.
//!
//! Both are `None` for the origin run and for every non-continued run. Legacy
//! rows written before the feature also carry `None`; [`assemble_run_chain`]
//! still orders them via a `started_at` fallback and flags the head as unknown.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// One execution in a continue-as-new chain, as served to operators.
///
/// Field names serialize as `snake_case` (the wire contract).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunChainRecord {
    /// The execution id (stringified UUID).
    pub exec_id: String,
    /// The internal run id (stringified UUID). Fresh per execution in the chain.
    pub run_id: String,
    /// Zero-based ordinal of this run within the ordered chain.
    pub sequence: u32,
    /// Raw DB state string (e.g. `"COMPLETED"`, `"CONTINUED_AS_NEW"`).
    pub state: String,
    /// When this run started.
    pub started_at: DateTime<Utc>,
    /// When this run reached a terminal state, if it has.
    pub completed_at: Option<DateTime<Utc>>,
    /// Collapsed terminal outcome (see [`outcome_for_state`]).
    pub outcome: String,
    /// Forward link to the next run in the chain (the successor's `exec_id`).
    /// `None` for the tail (the live or final run).
    pub continued_to_exec_id: Option<String>,
}

/// The assembled run-chain view for one logical (continue-as-new) run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunChainResponse {
    /// `true` when the chain participates in continue-as-new but the true origin
    /// cannot be proven from the available rows (legacy rows lacking back-links).
    /// The first entry in `runs` is then a best-effort head, not a confirmed one.
    pub head_unknown: bool,
    /// The shared `workflow_id` of the chain (all members share it). `None` when
    /// there are no rows.
    pub workflow_id: Option<String>,
    /// The chain's executions in continue-as-new order (oldest first).
    pub runs: Vec<RunChainRecord>,
}

/// Minimal per-row input [`assemble_run_chain`] needs.
///
/// Constructed by the plugin's DB layer from `harvest_workflow_executions` rows
/// and by unit tests directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunChainRow {
    /// The execution id (primary key).
    pub exec_id: Uuid,
    /// The internal run id.
    pub run_id: Uuid,
    /// The shared logical workflow id.
    pub workflow_id: String,
    /// Raw DB state string.
    pub state: String,
    /// When this run started.
    pub started_at: DateTime<Utc>,
    /// Terminal timestamp, if reached.
    pub completed_at: Option<DateTime<Utc>>,
    /// Immediate predecessor in the chain (issue #701). `None` for the origin
    /// and for non-continued / legacy rows.
    pub continued_from_exec_id: Option<Uuid>,
    /// Chain origin (issue #701), denormalized onto every successor. `None` for
    /// the origin and for non-continued / legacy rows.
    pub first_exec_id: Option<Uuid>,
}

/// Collapse a raw DB execution state string to a stable terminal-outcome label.
///
/// Non-terminal / active states (`RUNNING`, `PAUSED`, `SUSPENDED`) and any
/// unknown value collapse to `"running"` — a safe default that never claims a
/// run finished when it did not.
///
/// ## Examples
///
/// ```
/// use autumn_harvest::run_chain::outcome_for_state;
///
/// assert_eq!(outcome_for_state("COMPLETED"), "completed");
/// assert_eq!(outcome_for_state("CONTINUED_AS_NEW"), "continued_as_new");
/// assert_eq!(outcome_for_state("RUNNING"), "running");
/// assert_eq!(outcome_for_state("something-else"), "running");
/// ```
#[must_use]
pub fn outcome_for_state(state: &str) -> &'static str {
    match state {
        "COMPLETED" => "completed",
        "FAILED" => "failed",
        "CANCELLED" => "cancelled",
        "TIMED_OUT" => "timed_out",
        "TERMINATED" => "terminated",
        "CONTINUED_AS_NEW" => "continued_as_new",
        // RUNNING / PAUSED / SUSPENDED and any unknown value.
        _ => "running",
    }
}

/// Assemble an ordered [`RunChainResponse`] from a flat set of chain-member rows.
///
/// `head_exec_id` is the resolved chain head — the plugin computes it from the
/// queried row as `queried_row.first_exec_id.unwrap_or(queried_row.id)`.
///
/// ## Ordering
///
/// 1. Start at the row whose `exec_id == head_exec_id` (or the earliest-started
///    row if none matches).
/// 2. Hop forward via `continued_from_exec_id`: append the row that points back
///    at the current one. When no such row exists but the current run is
///    `CONTINUED_AS_NEW` and unvisited rows remain (a legacy chain lacking the
///    back-link), append the remaining rows ordered by `(started_at, exec_id)`.
/// 3. Any still-unvisited rows are appended in `(started_at, exec_id)` order.
///
/// The loop is bounded by `rows.len() + 1` iterations, so a corrupt cycle in the
/// back-links can never hang.
///
/// `continued_to_exec_id` on each record is the `exec_id` of the next ordered
/// run (`None` for the tail) — derived purely from ordering so it stays correct
/// for legacy chains that never stored a forward index.
///
/// See the module docs and `head_unknown` for the head-confidence rule.
// The rows are taken by value: this is the owned DB->wire boundary the plugin
// hands over after loading the chain, and the function is the sole consumer.
#[must_use]
#[allow(clippy::needless_pass_by_value)]
pub fn assemble_run_chain(rows: Vec<RunChainRow>, head_exec_id: Uuid) -> RunChainResponse {
    if rows.is_empty() {
        return RunChainResponse {
            head_unknown: false,
            workflow_id: None,
            runs: Vec::new(),
        };
    }

    let n = rows.len();
    let mut visited = vec![false; n];

    // Stable `(started_at, exec_id)` ordering used for fallbacks.
    let mut by_time: Vec<usize> = (0..n).collect();
    by_time.sort_by(|&a, &b| {
        rows[a]
            .started_at
            .cmp(&rows[b].started_at)
            .then_with(|| rows[a].exec_id.cmp(&rows[b].exec_id))
    });

    // 1. Pick the starting index: the row matching head_exec_id, else earliest.
    let start = rows
        .iter()
        .position(|r| r.exec_id == head_exec_id)
        .unwrap_or_else(|| by_time[0]);

    let mut ordered: Vec<usize> = Vec::with_capacity(n);
    ordered.push(start);
    visited[start] = true;

    // 2. Walk forward, bounded by n + 1 to defend against cycles.
    let mut current = start;
    for _ in 0..=n {
        // Prefer the explicit forward link: an unvisited row whose predecessor
        // is `current`.
        let current_exec_id = rows[current].exec_id;
        let next = rows
            .iter()
            .position(|r| r.continued_from_exec_id == Some(current_exec_id))
            .filter(|&i| !visited[i]);

        if let Some(next_idx) = next {
            ordered.push(next_idx);
            visited[next_idx] = true;
            current = next_idx;
            continue;
        }

        // Legacy fallback: current is a CONTINUED_AS_NEW run with no stored
        // forward link but unvisited rows remain — append them in time order.
        if rows[current].state == "CONTINUED_AS_NEW" {
            let mut last_appended = None;
            for &idx in &by_time {
                if !visited[idx] {
                    ordered.push(idx);
                    visited[idx] = true;
                    last_appended = Some(idx);
                }
            }
            if let Some(last) = last_appended {
                // Continue walking from the new tail in case further explicit
                // links resume (mixed chains).
                current = last;
                continue;
            }
        }
        break;
    }

    // 3. Defensive: append any still-unvisited rows in time order.
    for &idx in &by_time {
        if !visited[idx] {
            ordered.push(idx);
            visited[idx] = true;
        }
    }

    // Build records with derived forward links and sequence numbers.
    let mut runs: Vec<RunChainRecord> = Vec::with_capacity(ordered.len());
    for (seq, &row_idx) in ordered.iter().enumerate() {
        let row = &rows[row_idx];
        let continued_to = ordered
            .get(seq + 1)
            .map(|&next_idx| rows[next_idx].exec_id.to_string());
        runs.push(RunChainRecord {
            exec_id: row.exec_id.to_string(),
            run_id: row.run_id.to_string(),
            sequence: u32::try_from(seq).unwrap_or(u32::MAX),
            state: row.state.clone(),
            started_at: row.started_at,
            completed_at: row.completed_at,
            outcome: outcome_for_state(&row.state).to_string(),
            continued_to_exec_id: continued_to,
        });
    }

    // head_unknown rule (see module docs / issue #701).
    let head_row = &rows[ordered[0]];
    let confirmed_origin = rows
        .iter()
        .any(|r| r.first_exec_id == Some(head_row.exec_id));
    let head_participates_in_can = rows.len() > 1 || head_row.state == "CONTINUED_AS_NEW";
    let head_lacks_backlink =
        head_row.continued_from_exec_id.is_none() && head_row.first_exec_id.is_none();
    let head_unknown = head_lacks_backlink && head_participates_in_can && !confirmed_origin;

    let workflow_id = Some(head_row.workflow_id.clone());

    RunChainResponse {
        head_unknown,
        workflow_id,
        runs,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn ts(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000 + secs, 0).unwrap()
    }

    /// Builder for a chain row keeping the tests terse.
    fn row(
        exec_id: Uuid,
        state: &str,
        started_secs: i64,
        continued_from: Option<Uuid>,
        first: Option<Uuid>,
    ) -> RunChainRow {
        RunChainRow {
            exec_id,
            run_id: Uuid::new_v4(),
            workflow_id: "wf-1".to_string(),
            state: state.to_string(),
            started_at: ts(started_secs),
            completed_at: if state == "RUNNING" {
                None
            } else {
                Some(ts(started_secs + 1))
            },
            continued_from_exec_id: continued_from,
            first_exec_id: first,
        }
    }

    // ── outcome_for_state ────────────────────────────────────────────────────

    #[test]
    fn outcome_for_state_maps_every_known_state() {
        assert_eq!(outcome_for_state("RUNNING"), "running");
        assert_eq!(outcome_for_state("PAUSED"), "running");
        assert_eq!(outcome_for_state("SUSPENDED"), "running");
        assert_eq!(outcome_for_state("COMPLETED"), "completed");
        assert_eq!(outcome_for_state("FAILED"), "failed");
        assert_eq!(outcome_for_state("CANCELLED"), "cancelled");
        assert_eq!(outcome_for_state("TIMED_OUT"), "timed_out");
        assert_eq!(outcome_for_state("TERMINATED"), "terminated");
        assert_eq!(outcome_for_state("CONTINUED_AS_NEW"), "continued_as_new");
    }

    #[test]
    fn outcome_for_state_unknown_defaults_to_running() {
        assert_eq!(outcome_for_state(""), "running");
        assert_eq!(outcome_for_state("WAT"), "running");
        assert_eq!(outcome_for_state("completed"), "running"); // case-sensitive
    }

    // ── empty / single ───────────────────────────────────────────────────────

    #[test]
    fn empty_input_yields_empty_response() {
        let resp = assemble_run_chain(Vec::new(), Uuid::nil());
        assert!(!resp.head_unknown);
        assert_eq!(resp.workflow_id, None);
        assert!(resp.runs.is_empty());
    }

    #[test]
    fn single_standalone_run_is_not_a_chain() {
        // Canonical case 3: standalone never-CAN run.
        let a = Uuid::new_v4();
        let resp = assemble_run_chain(vec![row(a, "COMPLETED", 0, None, None)], a);
        assert_eq!(resp.runs.len(), 1);
        assert!(!resp.head_unknown);
        assert_eq!(resp.runs[0].sequence, 0);
        assert_eq!(resp.runs[0].outcome, "completed");
        assert_eq!(resp.runs[0].continued_to_exec_id, None);
        assert_eq!(resp.workflow_id.as_deref(), Some("wf-1"));
    }

    // ── canonical case 1: fully post-feature chain ───────────────────────────

    fn post_feature_chain() -> (Uuid, Uuid, Uuid, Vec<RunChainRow>) {
        // origin -> mid -> tail. Successors carry continued_from + first_exec_id.
        let origin = Uuid::new_v4();
        let mid = Uuid::new_v4();
        let tail = Uuid::new_v4();
        let rows = vec![
            row(origin, "CONTINUED_AS_NEW", 0, None, None),
            row(mid, "CONTINUED_AS_NEW", 10, Some(origin), Some(origin)),
            row(tail, "RUNNING", 20, Some(mid), Some(origin)),
        ];
        (origin, mid, tail, rows)
    }

    #[test]
    fn post_feature_chain_orders_and_links() {
        let (origin, mid, tail, rows) = post_feature_chain();
        let resp = assemble_run_chain(rows, origin);

        assert!(!resp.head_unknown, "confirmed origin => head known");
        assert_eq!(resp.runs.len(), 3);
        assert_eq!(resp.runs[0].exec_id, origin.to_string());
        assert_eq!(resp.runs[1].exec_id, mid.to_string());
        assert_eq!(resp.runs[2].exec_id, tail.to_string());
        // sequence numbering
        assert_eq!(resp.runs[0].sequence, 0);
        assert_eq!(resp.runs[1].sequence, 1);
        assert_eq!(resp.runs[2].sequence, 2);
        // forward links
        assert_eq!(
            resp.runs[0].continued_to_exec_id.as_deref(),
            Some(mid.to_string().as_str())
        );
        assert_eq!(
            resp.runs[1].continued_to_exec_id.as_deref(),
            Some(tail.to_string().as_str())
        );
        assert_eq!(
            resp.runs[2].continued_to_exec_id, None,
            "tail has no forward link"
        );
        // outcomes
        assert_eq!(resp.runs[2].outcome, "running");
    }

    #[test]
    fn post_feature_chain_resolves_identically_from_middle_and_tail() {
        let (origin, mid, tail, rows) = post_feature_chain();
        // Resolving from a successor: the plugin passes head = row.first_exec_id
        // = origin. Same ordered chain regardless of which member was queried.
        let from_mid = assemble_run_chain(rows.clone(), origin);
        let from_tail = assemble_run_chain(rows, origin);

        let ids: Vec<String> = from_mid.runs.iter().map(|r| r.exec_id.clone()).collect();
        assert_eq!(
            ids,
            vec![origin.to_string(), mid.to_string(), tail.to_string()]
        );
        assert_eq!(from_mid, from_tail);
        assert!(!from_mid.head_unknown);
    }

    #[test]
    fn ordering_is_independent_of_input_row_order() {
        // Same chain, rows shuffled: ordering must follow continued_from links.
        let (origin, mid, tail, mut rows) = post_feature_chain();
        rows.reverse(); // tail, mid, origin
        let resp = assemble_run_chain(rows, origin);
        let ids: Vec<String> = resp.runs.iter().map(|r| r.exec_id.clone()).collect();
        assert_eq!(
            ids,
            vec![origin.to_string(), mid.to_string(), tail.to_string()]
        );
    }

    // ── canonical case 2: pure legacy chain ──────────────────────────────────

    #[test]
    fn pure_legacy_chain_orders_by_time_and_flags_head_unknown() {
        // All back-links NULL; >1 run; head state CONTINUED_AS_NEW.
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let c = Uuid::new_v4();
        let rows = vec![
            row(a, "CONTINUED_AS_NEW", 0, None, None),
            row(b, "CONTINUED_AS_NEW", 10, None, None),
            row(c, "RUNNING", 20, None, None),
        ];
        // Head resolves to `a` (queried_row.first_exec_id.unwrap_or(id) = a).
        let resp = assemble_run_chain(rows, a);
        assert!(resp.head_unknown, "legacy chain cannot confirm origin");
        let ids: Vec<String> = resp.runs.iter().map(|r| r.exec_id.clone()).collect();
        assert_eq!(ids, vec![a.to_string(), b.to_string(), c.to_string()]);
        // forward links still derived from ordering
        assert_eq!(
            resp.runs[0].continued_to_exec_id.as_deref(),
            Some(b.to_string().as_str())
        );
        assert_eq!(resp.runs[2].continued_to_exec_id, None);
    }

    // ── canonical case 4: mixed chain (documented graceful degradation) ───────

    #[test]
    fn mixed_chain_boundary_run_reported_as_head_without_unknown() {
        // Legacy prefix (leg0 -> boundary) then post-feature suffix.
        // The post-feature successor `succ` sets continued_from = boundary and
        // first_exec_id = boundary (because boundary.first_exec_id is None, so
        // the successor stamps boundary.id). Thus SOME row's first_exec_id points
        // at boundary => confirmed_origin = true for boundary => head_unknown =
        // false, with boundary reported as head. The truly-first legacy runs
        // (leg0) are unreachable — documented, intentional degradation.
        let boundary = Uuid::new_v4();
        let succ = Uuid::new_v4();
        // Plugin resolving from `succ` computes head = succ.first_exec_id = boundary.
        let rows = vec![
            row(boundary, "CONTINUED_AS_NEW", 10, None, None),
            row(succ, "RUNNING", 20, Some(boundary), Some(boundary)),
        ];
        let resp = assemble_run_chain(rows, boundary);
        assert!(
            !resp.head_unknown,
            "boundary is a first_exec_id target => reported as a confirmed head"
        );
        let ids: Vec<String> = resp.runs.iter().map(|r| r.exec_id.clone()).collect();
        assert_eq!(ids, vec![boundary.to_string(), succ.to_string()]);
        assert_eq!(resp.runs[0].sequence, 0);
        assert_eq!(
            resp.runs[0].continued_to_exec_id.as_deref(),
            Some(succ.to_string().as_str())
        );
    }

    // ── head_unknown edge: two-run post-feature, head confirmed ──────────────

    #[test]
    fn two_run_post_feature_head_is_known() {
        let origin = Uuid::new_v4();
        let tail = Uuid::new_v4();
        let rows = vec![
            row(origin, "CONTINUED_AS_NEW", 0, None, None),
            row(tail, "COMPLETED", 10, Some(origin), Some(origin)),
        ];
        let resp = assemble_run_chain(rows, origin);
        assert!(!resp.head_unknown);
        assert_eq!(resp.runs[1].outcome, "completed");
    }

    #[test]
    fn head_not_found_falls_back_to_earliest_started() {
        // head_exec_id matches no row: start at earliest by started_at.
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let rows = vec![
            row(b, "CONTINUED_AS_NEW", 30, None, None),
            row(a, "CONTINUED_AS_NEW", 0, None, None),
        ];
        let resp = assemble_run_chain(rows, Uuid::new_v4());
        assert_eq!(
            resp.runs[0].exec_id,
            a.to_string(),
            "earliest starts the chain"
        );
    }

    // ── serde: snake_case wire contract ──────────────────────────────────────

    #[test]
    fn response_serializes_snake_case() {
        let (origin, _mid, _tail, rows) = post_feature_chain();
        let resp = assemble_run_chain(rows, origin);
        let v = serde_json::to_value(&resp).unwrap();
        assert!(v.get("head_unknown").is_some());
        assert!(v.get("workflow_id").is_some());
        let runs = v.get("runs").unwrap().as_array().unwrap();
        let first = &runs[0];
        for key in [
            "exec_id",
            "run_id",
            "sequence",
            "state",
            "started_at",
            "completed_at",
            "outcome",
            "continued_to_exec_id",
        ] {
            assert!(first.get(key).is_some(), "missing snake_case field: {key}");
        }
        // no camelCase leakage
        assert!(first.get("execId").is_none());
        assert!(first.get("continuedToExecId").is_none());
    }

    #[test]
    fn cycle_in_backlinks_does_not_hang() {
        // Corrupt data: a and b point at each other. Bounded loop must terminate.
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let rows = vec![
            row(a, "CONTINUED_AS_NEW", 0, Some(b), Some(a)),
            row(b, "CONTINUED_AS_NEW", 10, Some(a), Some(a)),
        ];
        let resp = assemble_run_chain(rows, a);
        // Both rows appear exactly once.
        assert_eq!(resp.runs.len(), 2);
        let mut ids: Vec<String> = resp.runs.iter().map(|r| r.exec_id.clone()).collect();
        ids.sort();
        let mut expected = vec![a.to_string(), b.to_string()];
        expected.sort();
        assert_eq!(ids, expected);
    }
}
