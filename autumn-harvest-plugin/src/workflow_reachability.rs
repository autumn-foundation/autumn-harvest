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
use crate::config::OrphanStartupAction;
use crate::shard_fanout::{self, FanoutStatus, ShardObservation};

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
///
/// A type alias onto the shared [`FanoutStatus`] (rather than its own enum)
/// so the complete/partial/unavailable derivation rule lives in exactly one
/// place, alongside `schedule_runs`' and `workflow_count`'s reports.
/// `safe_to_remove` verdicts are authoritative only when this is `complete`.
pub type ReachabilityReportStatus = FanoutStatus;

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
    /// Top-level summary (issue #700 AC3): `true` when any type has an
    /// `orphaned` verdict, so a CI/CD gate asserts "zero orphans" against a
    /// single boolean instead of walking `items`. Note this reflects **only**
    /// the inspected shards; when `status` is `partial`/`unavailable` an
    /// unreachable shard could still host an orphaned type.
    pub orphaned: bool,
    /// Total count of stranded **executions** (not types) across every
    /// `orphaned`-verdict type — the single number a "zero orphans" gate
    /// compares against.
    pub total_orphaned_executions: i64,
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
    /// A bounded (`REACHABILITY_SAMPLE_CAP`) set of representative non-terminal
    /// execution ids so an operator can drill into stuck runs (issue #700 AC2).
    /// Empty for `safe_to_remove` types. These are **representative and
    /// bounded** — NOT a guaranteed global-oldest-across-shards set: each shard
    /// pre-orders its own contribution oldest-first, and the cross-shard union
    /// is capped in shard-iteration (ascending `shard_id`) order.
    pub sample_execution_ids: Vec<uuid::Uuid>,
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
/// `per_shard` drives the total count and oldest start time (derived on demand
/// so there is no redundant state to drift). `samples` is a flat, shard-ordered
/// concatenation of each shard's already-bounded, oldest-first sample ids —
/// capped to the plugin cap on read (see [`Self::samples`]).
#[derive(Debug)]
struct ReachabilityAccumulator {
    per_shard: BTreeMap<i32, (i64, DateTime<Utc>)>,
    /// Sample execution ids, appended per contributing shard in ascending
    /// `shard_id` order (observations are iterated in sorted-shard order), so a
    /// truncation favours lower shard ids — representative, not global-oldest.
    samples: Vec<uuid::Uuid>,
}

impl ReachabilityAccumulator {
    const fn empty() -> Self {
        Self {
            per_shard: BTreeMap::new(),
            samples: Vec::new(),
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
        self.samples
            .extend(row.sample_execution_ids.iter().copied());
    }

    fn non_terminal_count(&self) -> i64 {
        self.per_shard.values().map(|(count, _)| count).sum()
    }

    fn oldest_started_at(&self) -> Option<DateTime<Utc>> {
        self.per_shard
            .values()
            .map(|(_, oldest)| *oldest)
            .reduce(std::cmp::min)
    }

    /// The cross-shard sample union, truncated to `cap`. A single shard produces
    /// at most one row per type, so `samples` is already deduped across shards
    /// by construction (distinct execution ids per shard).
    fn samples(&self, cap: usize) -> Vec<uuid::Uuid> {
        self.samples.iter().take(cap).copied().collect()
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
    // Union plain workflow handlers with unified-DAG names. In unified-DAG
    // deployments, live runs are stored in harvest_workflow_executions with
    // workflow_name = dag name, but those names live in registered_dag_names
    // rather than registry().workflows. Without the union a registered DAG with
    // active runs would falsely appear as `orphaned`, causing the CLI monitor
    // to exit 2 on healthy deployments.
    let registered: BTreeSet<String> = runtime
        .registry()
        .workflows
        .keys()
        .cloned()
        .chain(runtime.registered_dag_names().iter().cloned())
        .collect();

    // The expected shard set is both the storage pool AND the router's
    // readable_shards/default_shard: a shard the router knows about but for
    // which this process has no pool (e.g. during a mixed-rollout or
    // shard-add before the pool is wired up) must appear as "unavailable" in
    // the report rather than being silently dropped — the report status then
    // becomes "partial" and the CLI fails closed (exit 2), preventing a false
    // "safe_to_remove" verdict from a shard that was never inspected. See
    // [`shard_fanout::expected_shards`] for the shared derivation.
    let pools = shard_fanout::pools_by_shard(api_state);
    let expected_shards = shard_fanout::expected_shards(api_state, &pools);

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
    match non_terminal_counts_by_workflow_name(&mut conn, Some(shard_id), filter.as_deref()).await {
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
            let sample_execution_ids = acc.map_or_else(Vec::new, |a| {
                a.samples(autumn_harvest::execution::REACHABILITY_SAMPLE_CAP)
            });
            WorkflowTypeReachability {
                workflow_type,
                registered: is_registered,
                non_terminal_count,
                oldest_non_terminal_age_secs,
                verdict: compute_verdict(non_terminal_count, is_registered),
                sample_execution_ids,
                shard_breakdown,
            }
        })
        .collect::<Vec<_>>();

    // AC3 top-level summary: `orphaned` gates a "zero orphans" CI check, and
    // `total_orphaned_executions` sums the stranded *executions* (not types)
    // across every orphaned-verdict type.
    let orphaned = items
        .iter()
        .any(|item| item.verdict == ReachabilityVerdict::Orphaned);
    let total_orphaned_executions = items
        .iter()
        .filter(|item| item.verdict == ReachabilityVerdict::Orphaned)
        .map(|item| item.non_terminal_count)
        .sum();

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
        status: FanoutStatus::from_counts(inspected_shards, unavailable_shards),
        observed_at,
        filter,
        orphaned,
        total_orphaned_executions,
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

/// The action to take at boot after evaluating workflow-type reachability
/// (issue #700 AC4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupOrphanDecision {
    /// Boot normally — no orphaned types, or the check is disabled.
    Continue,
    /// Log a warning naming the orphaned types, then boot.
    Warn,
    /// Refuse to start (the caller returns an error before committing startup).
    Abort,
}

/// Pure, DB-free decision for the boot-time orphaned-workflow gate.
///
/// Rules:
/// - `Off` → always `Continue` (the check is disabled).
/// - No orphans (`orphaned == false`) → `Continue` for every action; a clean
///   boot never warns.
/// - `Warn` + orphaned → `Warn`.
/// - `Fail` + orphaned + `status == Complete` → `Abort` (the one refuse case).
/// - `Fail` + orphaned + `Partial`/`Unavailable` → `Warn`. **Crash-loop
///   safety:** a boot loop has no human in the loop, so a transient unreachable
///   shard must never hard-fail startup (asymmetric with the CLI gate, which
///   fails *closed* on a partial report because a human reads its exit code).
#[must_use]
pub fn startup_orphan_decision(
    action: OrphanStartupAction,
    orphaned: bool,
    status: ReachabilityReportStatus,
) -> StartupOrphanDecision {
    if action == OrphanStartupAction::Off || !orphaned {
        return StartupOrphanDecision::Continue;
    }
    match action {
        OrphanStartupAction::Fail if status == FanoutStatus::Complete => {
            StartupOrphanDecision::Abort
        }
        // Warn, or Fail on an incomplete report (crash-loop safety).
        OrphanStartupAction::Warn | OrphanStartupAction::Fail => StartupOrphanDecision::Warn,
        // Unreachable: Off + orphaned handled by the early return above.
        OrphanStartupAction::Off => StartupOrphanDecision::Continue,
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
            sample_execution_ids: Vec::new(),
        }
    }

    fn row_with_samples(
        workflow_name: &str,
        count: i64,
        oldest: i64,
        samples: &[uuid::Uuid],
    ) -> WorkflowTypeNonTerminalCount {
        WorkflowTypeNonTerminalCount {
            workflow_name: workflow_name.to_string(),
            non_terminal_count: count,
            oldest_started_at: at(oldest),
            sample_execution_ids: samples.to_vec(),
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

    // ── AC2: bounded execution-id samples per group ─────────────────────────

    #[test]
    fn samples_are_capped_and_bounded_per_type() {
        // Two shards each contribute the per-shard SQL cap (5) of samples; the
        // cross-shard union (10) must be capped back down to
        // REACHABILITY_SAMPLE_CAP (5), and every returned id must be drawn from
        // the seeded set.
        let shard0: Vec<uuid::Uuid> = (0..5).map(|_| uuid::Uuid::new_v4()).collect();
        let shard1: Vec<uuid::Uuid> = (0..5).map(|_| uuid::Uuid::new_v4()).collect();
        let all: BTreeSet<uuid::Uuid> = shard0.iter().chain(shard1.iter()).copied().collect();

        let report = build_report_from_observations(
            at(1000),
            None,
            &registered(&["batch"]),
            vec![
                obs(0, vec![row_with_samples("batch", 40, 100, &shard0)], None),
                obs(1, vec![row_with_samples("batch", 30, 200, &shard1)], None),
            ],
        );

        let batch = item(&report, "batch");
        assert_eq!(batch.non_terminal_count, 70);
        assert_eq!(
            batch.sample_execution_ids.len(),
            autumn_harvest::execution::REACHABILITY_SAMPLE_CAP,
            "the cross-shard sample union must be capped at REACHABILITY_SAMPLE_CAP"
        );
        for id in &batch.sample_execution_ids {
            assert!(
                all.contains(id),
                "every sample must be a seeded execution id"
            );
        }
    }

    #[test]
    fn safe_to_remove_type_has_empty_samples() {
        let report = build_report_from_observations(
            at(1000),
            None,
            &registered(&["onboarding"]),
            vec![obs(0, Vec::new(), None)],
        );
        let onboarding = item(&report, "onboarding");
        assert_eq!(onboarding.verdict, ReachabilityVerdict::SafeToRemove);
        assert!(
            onboarding.sample_execution_ids.is_empty(),
            "a type with no non-terminal executions has no samples"
        );
    }

    #[test]
    fn samples_merge_across_shards() {
        // When the union fits under the cap, every shard's samples appear.
        let a: Vec<uuid::Uuid> = (0..2).map(|_| uuid::Uuid::new_v4()).collect();
        let b: Vec<uuid::Uuid> = (0..2).map(|_| uuid::Uuid::new_v4()).collect();
        let report = build_report_from_observations(
            at(1000),
            None,
            &registered(&["flow"]),
            vec![
                obs(0, vec![row_with_samples("flow", 2, 100, &a)], None),
                obs(1, vec![row_with_samples("flow", 2, 200, &b)], None),
            ],
        );
        let flow = item(&report, "flow");
        let got: BTreeSet<uuid::Uuid> = flow.sample_execution_ids.iter().copied().collect();
        for id in a.iter().chain(b.iter()) {
            assert!(got.contains(id), "merged samples must include both shards");
        }
        assert_eq!(flow.sample_execution_ids.len(), 4);
    }

    // ── AC3: top-level orphaned summary for a single-field CI gate ──────────

    #[test]
    fn report_reports_orphaned_true_and_total_when_orphan_present() {
        let report = build_report_from_observations(
            at(1000),
            None,
            &registered(&[]),
            vec![obs(0, vec![row("legacy_flow", 3, 0)], None)],
        );
        assert!(report.orphaned);
        assert_eq!(report.total_orphaned_executions, 3);
    }

    #[test]
    fn report_orphaned_false_and_zero_total_when_no_orphans() {
        // subscription is in_use, onboarding is safe_to_remove — neither orphaned.
        let report = build_report_from_observations(
            at(1000),
            None,
            &registered(&["onboarding", "subscription"]),
            vec![obs(0, vec![row("subscription", 4, 100)], None)],
        );
        assert!(!report.orphaned);
        assert_eq!(report.total_orphaned_executions, 0);
    }

    #[test]
    fn total_orphaned_sums_across_multiple_orphaned_types_and_shards() {
        // legacy_a (2 + 5) and legacy_b (3) are both unregistered -> orphaned.
        // subscription is registered -> in_use and must NOT be counted.
        let report = build_report_from_observations(
            at(1000),
            None,
            &registered(&["subscription"]),
            vec![
                obs(
                    0,
                    vec![row("legacy_a", 2, 0), row("subscription", 9, 0)],
                    None,
                ),
                obs(1, vec![row("legacy_a", 5, 0), row("legacy_b", 3, 0)], None),
            ],
        );
        assert!(report.orphaned);
        // 2 + 5 (legacy_a) + 3 (legacy_b) = 10; subscription (9) excluded.
        assert_eq!(report.total_orphaned_executions, 10);
    }

    // ── AC4: pure boot-time fail-fast decision ──────────────────────────────

    #[test]
    fn fail_aborts_only_on_orphaned_and_complete() {
        use crate::config::OrphanStartupAction;
        // The one and only Abort case.
        assert_eq!(
            startup_orphan_decision(
                OrphanStartupAction::Fail,
                true,
                ReachabilityReportStatus::Complete
            ),
            StartupOrphanDecision::Abort
        );
        // No orphans -> Continue even under Fail (a clean boot must not warn).
        assert_eq!(
            startup_orphan_decision(
                OrphanStartupAction::Fail,
                false,
                ReachabilityReportStatus::Complete
            ),
            StartupOrphanDecision::Continue
        );
    }

    #[test]
    fn fail_warns_not_aborts_on_partial_status() {
        use crate::config::OrphanStartupAction;
        // Crash-loop safety: a transient partial/unavailable shard must never
        // hard-fail boot, even when orphans are observed on the reachable shards.
        for status in [
            ReachabilityReportStatus::Partial,
            ReachabilityReportStatus::Unavailable,
        ] {
            assert_eq!(
                startup_orphan_decision(OrphanStartupAction::Fail, true, status),
                StartupOrphanDecision::Warn,
                "Fail must degrade to Warn on an incomplete report"
            );
        }
    }

    #[test]
    fn warn_never_aborts() {
        use crate::config::OrphanStartupAction;
        assert_eq!(
            startup_orphan_decision(
                OrphanStartupAction::Warn,
                true,
                ReachabilityReportStatus::Complete
            ),
            StartupOrphanDecision::Warn
        );
        assert_eq!(
            startup_orphan_decision(
                OrphanStartupAction::Warn,
                false,
                ReachabilityReportStatus::Complete
            ),
            StartupOrphanDecision::Continue
        );
        assert_eq!(
            startup_orphan_decision(
                OrphanStartupAction::Warn,
                true,
                ReachabilityReportStatus::Partial
            ),
            StartupOrphanDecision::Warn
        );
    }

    #[test]
    fn off_skips_entirely() {
        use crate::config::OrphanStartupAction;
        for orphaned in [true, false] {
            for status in [
                ReachabilityReportStatus::Complete,
                ReachabilityReportStatus::Partial,
                ReachabilityReportStatus::Unavailable,
            ] {
                assert_eq!(
                    startup_orphan_decision(OrphanStartupAction::Off, orphaned, status),
                    StartupOrphanDecision::Continue,
                    "Off must always Continue"
                );
            }
        }
    }
}
