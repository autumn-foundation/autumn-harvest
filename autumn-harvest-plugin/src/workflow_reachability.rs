//! Management read model for workflow-type handler reachability (issue #520).
//!
//! Answers a single safe-deploy question for deployments **without** build-id
//! routing (#171): *"is it safe to delete or rename this `#[workflow]` handler,
//! or would doing so strand in-flight runs in permanent replay failure?"*
//!
//! A non-terminal execution's `workflow_name` directly names the handler its
//! next replay requires. So a read-only `GROUP BY workflow_name` over
//! non-terminal `harvest_workflow_executions`, fanned out across
//! [`iter_shards`](crate::state::HarvestDbPool::iter_shards) and joined against
//! the in-memory handler registry, is an exact, side-effect-free answer.
//!
//! ## Verdicts
//!
//! - `safe_to_remove` — zero non-terminal executions for this type; the handler
//!   can be deleted.
//! - `in_use` — ≥1 non-terminal execution **and** the handler is still
//!   registered in this deployment.
//! - `orphaned` — ≥1 non-terminal execution **and** the handler is **not**
//!   registered (already-wedged runs, surfaced *before* they manifest as
//!   DLQ/timeout).
//!
//! ## Partial answers
//!
//! When a shard is unreachable it is reported in `shards` (never silently
//! dropped) and the report `status` is `partial` (some shards inspected) or
//! `unavailable` (none inspected). A `safe_to_remove` verdict is authoritative
//! **only** when `status == complete`: an unreachable shard could host
//! non-terminal executions, so a partial answer must never be mistaken for a
//! safe one. The CLI gate fails closed on `partial`/`unavailable` for exactly
//! this reason.
//!
//! This is the **type-level** reachability question. It is orthogonal to
//! **build-id** `build_reachability` (#171, "can I retire this worker build")
//! and to the **`ctx.version()`** gate-retirement check ("can I remove this
//! version branch inside a handler"). See the "Safe handler removal" runbook.

use std::collections::{BTreeMap, BTreeSet};

use autumn_harvest::WorkflowTypeNonTerminalCount;
use autumn_harvest::execution::non_terminal_counts_by_workflow_name;
use autumn_harvest::worker::DbPool;
use autumn_web::error::AutumnError;
use chrono::{DateTime, Utc};
use futures::future::join_all;
use serde::{Deserialize, Serialize};

use crate::api::{HarvestApiState, map_error};
use crate::shard_fanout::{self, ShardObservation};

/// Query string accepted by `GET /admin/workflow-types/reachability`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct WorkflowReachabilityQuery {
    /// Narrow the report to a single workflow type. When set, the response still
    /// returns the full object for that type (`non_terminal_count = 0` +
    /// `safe_to_remove` when no non-terminal executions exist).
    pub workflow_type: Option<String>,
}

/// Per-type safe-removal verdict.
///
/// Additive-only public API: new variants may be appended, but the three below
/// must never be removed or renamed.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReachabilityVerdict {
    /// Zero non-terminal executions — the handler can be deleted.
    SafeToRemove,
    /// ≥1 non-terminal execution and the handler is still registered.
    InUse,
    /// ≥1 non-terminal execution and the handler is not registered (wedged).
    Orphaned,
}

/// Overall cross-shard completeness of the report.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReachabilityReportStatus {
    /// Every expected shard was inspected — verdicts are authoritative.
    Complete,
    /// At least one shard was inspected and at least one was unavailable —
    /// `safe_to_remove` verdicts are provisional.
    Partial,
    /// No shard could be inspected — no verdict is authoritative.
    Unavailable,
}

/// Per-shard inspection outcome.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ReachabilityShardStatus {
    /// The shard was queried successfully.
    Inspected,
    /// The shard could not be queried; its counts are not in this report.
    Unavailable,
}

/// Read-only workflow-type reachability report.
#[derive(Debug, Clone, Serialize)]
pub struct WorkflowReachabilityReport {
    /// Cross-shard completeness. `safe_to_remove` is authoritative only when
    /// this is `complete`.
    pub status: ReachabilityReportStatus,
    /// When the report was assembled.
    pub observed_at: DateTime<Utc>,
    /// Echo of the `workflow_type` filter, if any.
    pub filter: Option<String>,
    /// One entry per workflow type that is either registered or has ≥1
    /// non-terminal execution on any inspected shard. Sorted by `workflow_type`.
    pub items: Vec<WorkflowTypeReachability>,
    /// Per-shard inspection outcomes, including any unavailable shard.
    pub shards: Vec<ReachabilityShardInspection>,
}

/// One workflow type's aggregated reachability across all inspected shards.
#[derive(Debug, Clone, Serialize)]
pub struct WorkflowTypeReachability {
    /// Workflow type name (== the handler name its replay requires).
    pub workflow_type: String,
    /// Whether a handler for this type is registered in the running deployment.
    pub registered: bool,
    /// Total non-terminal executions across inspected shards.
    pub non_terminal_count: i64,
    /// Age of the oldest non-terminal execution, in seconds. `null` when there
    /// are none.
    pub oldest_non_terminal_age_secs: Option<i64>,
    /// Safe-removal verdict for this type.
    pub verdict: ReachabilityVerdict,
    /// Per-shard counts (only shards with ≥1 non-terminal execution of this
    /// type appear). Sorted by `shard_id`.
    pub shard_breakdown: Vec<ReachabilityShardCount>,
}

/// One shard's contribution to a type's non-terminal count.
#[derive(Debug, Clone, Serialize)]
pub struct ReachabilityShardCount {
    /// Shard identifier.
    pub shard_id: i32,
    /// Non-terminal executions of this type on this shard.
    pub non_terminal_count: i64,
    /// Age of the oldest non-terminal execution of this type on this shard.
    pub oldest_non_terminal_age_secs: Option<i64>,
}

/// Per-shard inspection record surfaced in the report.
#[derive(Debug, Clone, Serialize)]
pub struct ReachabilityShardInspection {
    /// Shard identifier.
    pub shard_id: i32,
    /// Whether the shard was inspected or was unavailable.
    pub status: ReachabilityShardStatus,
    /// Error detail when `status == unavailable`.
    pub error: Option<String>,
}

/// Per-workflow-type accumulator: aggregates counts across shards.
///
/// Only `per_shard` is stored; the total count and oldest start time are
/// derived on demand so there is no redundant state to drift.
#[derive(Debug)]
struct ReachabilityAccumulator {
    per_shard: BTreeMap<i32, (i64, DateTime<Utc>)>,
}

impl ReachabilityAccumulator {
    const fn empty() -> Self {
        Self {
            per_shard: BTreeMap::new(),
        }
    }

    fn add(&mut self, shard_id: i32, row: &WorkflowTypeNonTerminalCount) {
        self.per_shard
            .entry(shard_id)
            .and_modify(|(count, oldest)| {
                *count += row.non_terminal_count;
                *oldest = (*oldest).min(row.oldest_started_at);
            })
            .or_insert((row.non_terminal_count, row.oldest_started_at));
    }

    fn non_terminal_count(&self) -> i64 {
        self.per_shard.values().map(|(count, _)| count).sum()
    }

    fn oldest_started_at(&self) -> Option<DateTime<Utc>> {
        self.per_shard
            .values()
            .map(|(_, oldest)| *oldest)
            .reduce(DateTime::min)
    }
}

/// Build the workflow-type reachability report without mutating any state.
///
/// # Errors
///
/// Returns an [`AutumnError`] when the Harvest runtime (and therefore the
/// handler registry) is not started — the `registered` flag cannot be resolved
/// without it.
pub async fn build_workflow_reachability_report(
    api_state: &HarvestApiState,
    query: WorkflowReachabilityQuery,
) -> Result<WorkflowReachabilityReport, AutumnError> {
    let observed_at = Utc::now();

    // The handler registry is the authoritative source for `registered`.
    let runtime = api_state.runtime().map_err(map_error)?;
    let registered: BTreeSet<String> = runtime
        .registry()
        .workflows
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();

    // Only include shards that have a configured pool. A shard the router knows
    // about but has no pool for (e.g. during a shard-add rollout before the
    // pool is wired up) must not appear as "unavailable" and make the report
    // partial — it simply hasn't been added yet.
    let pools = shard_fanout::pools_by_shard(api_state);
    let expected_shards: BTreeSet<i32> = if pools.is_empty() {
        // No pool configured: expose a synthetic shard-0 entry so the report
        // is well-formed ("unavailable") rather than having an empty shards list.
        BTreeSet::from([0])
    } else {
        pools.keys().copied().collect()
    };

    let filter = query.workflow_type.clone();

    let observations = expected_shards
        .iter()
        .map(|shard_id| {
            let pool = pools.get(shard_id).cloned();
            observe_shard(*shard_id, pool, filter.clone())
        })
        .collect::<Vec<_>>();
    let observations = join_all(observations).await;

    Ok(build_report_from_observations(
        observed_at,
        query.workflow_type,
        &registered,
        observations,
    ))
}

async fn observe_shard(
    shard_id: i32,
    pool: Option<DbPool>,
    filter: Option<String>,
) -> ShardObservation<WorkflowTypeNonTerminalCount> {
    let Some(pool) = pool else {
        return ShardObservation {
            shard_id,
            rows: Vec::new(),
            error: Some(format!("shard {shard_id} has no configured storage pool")),
        };
    };
    let Ok(mut conn) = pool.get().await else {
        return ShardObservation {
            shard_id,
            rows: Vec::new(),
            error: Some(format!(
                "database connection for shard {shard_id} could not be acquired"
            )),
        };
    };
    match non_terminal_counts_by_workflow_name(&mut conn, filter.as_deref()).await {
        Ok(rows) => ShardObservation {
            shard_id,
            rows,
            error: None,
        },
        Err(error) => ShardObservation {
            shard_id,
            rows: Vec::new(),
            error: Some(error.to_string()),
        },
    }
}

fn build_report_from_observations(
    observed_at: DateTime<Utc>,
    filter: Option<String>,
    registered: &BTreeSet<String>,
    observations: Vec<ShardObservation<WorkflowTypeNonTerminalCount>>,
) -> WorkflowReachabilityReport {
    let inspected_shards = observations
        .iter()
        .filter(|observation| observation.error.is_none())
        .count();
    let unavailable_shards = observations
        .iter()
        .filter(|observation| observation.error.is_some())
        .count();

    // Aggregate observed non-terminal counts per workflow type.
    let mut accumulators = BTreeMap::<String, ReachabilityAccumulator>::new();
    for observation in &observations {
        for row in &observation.rows {
            accumulators
                .entry(row.workflow_name.clone())
                .or_insert_with(ReachabilityAccumulator::empty)
                .add(observation.shard_id, row);
        }
    }

    // The universe of types is: every registered type plus every type with an
    // observed non-terminal execution. A `?workflow_type=` filter narrows the
    // universe to exactly that single type.
    let universe: BTreeSet<String> = filter.as_ref().map_or_else(
        || {
            registered
                .iter()
                .cloned()
                .chain(accumulators.keys().cloned())
                .collect()
        },
        |workflow_type| BTreeSet::from([workflow_type.clone()]),
    );

    let items = universe
        .into_iter()
        .map(|workflow_type| {
            let acc = accumulators.get(&workflow_type);
            let is_registered = registered.contains(&workflow_type);
            let non_terminal_count = acc.map_or(0, ReachabilityAccumulator::non_terminal_count);
            let oldest_non_terminal_age_secs = acc
                .and_then(ReachabilityAccumulator::oldest_started_at)
                .map(|started_at| shard_fanout::age_secs(observed_at, started_at));
            let shard_breakdown = acc.map_or_else(Vec::new, |a| {
                a.per_shard
                    .iter()
                    .map(|(shard_id, (count, oldest))| ReachabilityShardCount {
                        shard_id: *shard_id,
                        non_terminal_count: *count,
                        oldest_non_terminal_age_secs: Some(shard_fanout::age_secs(
                            observed_at,
                            *oldest,
                        )),
                    })
                    .collect()
            });
            WorkflowTypeReachability {
                workflow_type,
                registered: is_registered,
                non_terminal_count,
                oldest_non_terminal_age_secs,
                verdict: compute_verdict(non_terminal_count, is_registered),
                shard_breakdown,
            }
        })
        .collect::<Vec<_>>();

    let mut shards = observations
        .into_iter()
        .map(|observation| ReachabilityShardInspection {
            shard_id: observation.shard_id,
            status: if observation.error.is_some() {
                ReachabilityShardStatus::Unavailable
            } else {
                ReachabilityShardStatus::Inspected
            },
            error: observation.error,
        })
        .collect::<Vec<_>>();
    shards.sort_by_key(|shard| shard.shard_id);

    WorkflowReachabilityReport {
        status: report_status(inspected_shards == 0, unavailable_shards > 0),
        observed_at,
        filter,
        items,
        shards,
    }
}

const fn compute_verdict(non_terminal_count: i64, registered: bool) -> ReachabilityVerdict {
    if non_terminal_count == 0 {
        ReachabilityVerdict::SafeToRemove
    } else if registered {
        ReachabilityVerdict::InUse
    } else {
        ReachabilityVerdict::Orphaned
    }
}

const fn report_status(no_inspected: bool, has_unavailable: bool) -> ReachabilityReportStatus {
    if no_inspected {
        ReachabilityReportStatus::Unavailable
    } else if has_unavailable {
        ReachabilityReportStatus::Partial
    } else {
        ReachabilityReportStatus::Complete
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone as _;

    fn at(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0).unwrap()
    }

    fn row(workflow_name: &str, count: i64, oldest: i64) -> WorkflowTypeNonTerminalCount {
        WorkflowTypeNonTerminalCount {
            workflow_name: workflow_name.to_string(),
            non_terminal_count: count,
            oldest_started_at: at(oldest),
        }
    }

    fn registered(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|name| (*name).to_string()).collect()
    }

    fn item<'a>(
        report: &'a WorkflowReachabilityReport,
        workflow_type: &str,
    ) -> &'a WorkflowTypeReachability {
        report
            .items
            .iter()
            .find(|item| item.workflow_type == workflow_type)
            .unwrap_or_else(|| panic!("expected item for {workflow_type}"))
    }

    fn obs(
        shard_id: i32,
        rows: Vec<WorkflowTypeNonTerminalCount>,
        error: Option<&str>,
    ) -> ShardObservation<WorkflowTypeNonTerminalCount> {
        ShardObservation {
            shard_id,
            rows,
            error: error.map(ToOwned::to_owned),
        }
    }

    #[test]
    fn registered_with_no_executions_is_safe_to_remove() {
        let report = build_report_from_observations(
            at(1000),
            None,
            &registered(&["onboarding"]),
            vec![obs(0, Vec::new(), None)],
        );

        assert_eq!(report.status, ReachabilityReportStatus::Complete);
        let onboarding = item(&report, "onboarding");
        assert!(onboarding.registered);
        assert_eq!(onboarding.non_terminal_count, 0);
        assert_eq!(onboarding.oldest_non_terminal_age_secs, None);
        assert_eq!(onboarding.verdict, ReachabilityVerdict::SafeToRemove);
        assert!(onboarding.shard_breakdown.is_empty());
    }

    #[test]
    fn registered_with_non_terminal_executions_is_in_use() {
        let report = build_report_from_observations(
            at(1000),
            None,
            &registered(&["onboarding"]),
            vec![obs(0, vec![row("onboarding", 3, 400)], None)],
        );

        let onboarding = item(&report, "onboarding");
        assert!(onboarding.registered);
        assert_eq!(onboarding.non_terminal_count, 3);
        assert_eq!(onboarding.oldest_non_terminal_age_secs, Some(600));
        assert_eq!(onboarding.verdict, ReachabilityVerdict::InUse);
    }

    #[test]
    fn unregistered_with_non_terminal_executions_is_orphaned() {
        // The handler was removed but a run of it is still live: this is the
        // already-wedged case surfaced before DLQ/timeout.
        let report = build_report_from_observations(
            at(1000),
            None,
            &registered(&[]),
            vec![obs(0, vec![row("legacy_flow", 1, 0)], None)],
        );

        let legacy = item(&report, "legacy_flow");
        assert!(!legacy.registered);
        assert_eq!(legacy.non_terminal_count, 1);
        assert_eq!(legacy.verdict, ReachabilityVerdict::Orphaned);
    }

    #[test]
    fn filter_for_absent_type_returns_safe_to_remove_zero_object() {
        let report = build_report_from_observations(
            at(1000),
            Some("never_started".to_string()),
            &registered(&["onboarding"]),
            vec![obs(0, Vec::new(), None)],
        );

        assert_eq!(report.items.len(), 1);
        let only = &report.items[0];
        assert_eq!(only.workflow_type, "never_started");
        assert!(!only.registered);
        assert_eq!(only.non_terminal_count, 0);
        assert_eq!(only.verdict, ReachabilityVerdict::SafeToRemove);
    }

    #[test]
    fn counts_aggregate_across_shards_with_breakdown() {
        let report = build_report_from_observations(
            at(1000),
            None,
            &registered(&["onboarding"]),
            vec![
                obs(0, vec![row("onboarding", 2, 400)], None),
                obs(1, vec![row("onboarding", 5, 100)], None),
            ],
        );

        let onboarding = item(&report, "onboarding");
        assert_eq!(onboarding.non_terminal_count, 7);
        // Oldest across shards is the shard-1 start (100 -> age 900).
        assert_eq!(onboarding.oldest_non_terminal_age_secs, Some(900));
        assert_eq!(onboarding.shard_breakdown.len(), 2);
        assert_eq!(onboarding.shard_breakdown[0].shard_id, 0);
        assert_eq!(onboarding.shard_breakdown[0].non_terminal_count, 2);
        assert_eq!(onboarding.shard_breakdown[1].shard_id, 1);
        assert_eq!(onboarding.shard_breakdown[1].non_terminal_count, 5);
    }

    #[test]
    fn unavailable_shard_makes_report_partial_and_is_named() {
        let report = build_report_from_observations(
            at(1000),
            None,
            &registered(&["onboarding"]),
            vec![
                obs(0, vec![row("onboarding", 1, 500)], None),
                obs(1, Vec::new(), Some("connection refused")),
            ],
        );

        // A partial answer must never read as a clean "safe to remove".
        assert_eq!(report.status, ReachabilityReportStatus::Partial);
        let shard1 = report
            .shards
            .iter()
            .find(|shard| shard.shard_id == 1)
            .expect("shard 1 must be reported");
        assert_eq!(shard1.status, ReachabilityShardStatus::Unavailable);
        assert_eq!(shard1.error.as_deref(), Some("connection refused"));
    }

    #[test]
    fn all_shards_unavailable_makes_report_unavailable() {
        let report = build_report_from_observations(
            at(1000),
            None,
            &registered(&["onboarding"]),
            vec![obs(0, Vec::new(), Some("pool missing"))],
        );

        assert_eq!(report.status, ReachabilityReportStatus::Unavailable);
    }

    #[test]
    fn verdicts_serialize_as_snake_case() {
        assert_eq!(
            serde_json::to_value(ReachabilityVerdict::SafeToRemove).unwrap(),
            serde_json::json!("safe_to_remove")
        );
        assert_eq!(
            serde_json::to_value(ReachabilityVerdict::InUse).unwrap(),
            serde_json::json!("in_use")
        );
        assert_eq!(
            serde_json::to_value(ReachabilityVerdict::Orphaned).unwrap(),
            serde_json::json!("orphaned")
        );
    }

    #[test]
    fn registered_and_observed_types_both_appear() {
        // `legacy_flow` is unregistered-but-live (orphaned); `onboarding` is
        // registered-but-idle (safe). Both must be enumerated.
        let report = build_report_from_observations(
            at(1000),
            None,
            &registered(&["onboarding"]),
            vec![obs(0, vec![row("legacy_flow", 2, 0)], None)],
        );

        let types: Vec<&str> = report
            .items
            .iter()
            .map(|item| item.workflow_type.as_str())
            .collect();
        assert_eq!(types, vec!["legacy_flow", "onboarding"]);
        assert_eq!(
            item(&report, "legacy_flow").verdict,
            ReachabilityVerdict::Orphaned
        );
        assert_eq!(
            item(&report, "onboarding").verdict,
            ReachabilityVerdict::SafeToRemove
        );
    }
}
