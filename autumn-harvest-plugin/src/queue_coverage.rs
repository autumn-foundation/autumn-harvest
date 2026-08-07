//! Fleet-wide task-queue coverage read model (issue #774).
//!
//! Answers a single safe-deploy question: *"which task queues have pending
//! work but zero live workers polling them?"* A workflow that schedules an
//! activity onto a typo'd or undeployed queue produces a `harvest_task_queue`
//! row no live worker will ever claim; without this surface the operator's
//! only signal is a `schedule_to_start` timeout (if configured at all) or
//! silence forever.
//!
//! ## "Coverage" vs. "capacity"
//!
//! Coverage means *a poller exists*, not *capacity is available*. A queue
//! whose assigned workers are all saturated at `max_concurrency` is still
//! covered — that's a capacity/saturation concern (issues #531/#742),
//! deliberately out of scope here. Only **zero** live workers polling a
//! queue makes it "uncovered".
//!
//! A worker is "live" for coverage purposes when its heartbeat is fresh
//! (`WorkerHealth::Healthy`) and its lifecycle status is `Active` **or**
//! `Draining` — a draining worker is still finishing in-flight work and
//! still counts as covering the queue it was assigned. Only `Stopped`
//! workers (and stale/absent heartbeats) fail to cover.
//!
//! ## Distinct from build-id reachability (#171) and shard health (#522)
//!
//! This surface answers *"does any live worker poll this queue at all"* —
//! not *"do the workers that poll it run a build compatible with this
//! task's `required_build_id`"* (that is #171's `build_reachability`) and
//! not the writable/candidate-shard-scoped promotion-safety check
//! [`crate::shard_health`] already performs for rollout gating. Both of
//! those existing surfaces stay unchanged; this module is a dedicated,
//! all-shards, all-queues read purpose-built for the deploy/migration
//! smoke-check use case issue #774 asks for.
//!
//! ## Partial availability
//!
//! An unreachable shard is named in `shards` (never silently dropped) and
//! the report `status` degrades to `partial`/`unavailable`, mirroring the
//! DLQ-aggregate / shard-health / workflow-reachability fan-out contract.
//!
//! ## Excluded: paused queues
//!
//! A queue currently paused via [`autumn_harvest::queue_pause`] is excluded
//! from the uncovered list — its lack of live-worker action is deliberate
//! operator intent, already surfaced by the separate
//! `harvest_queue_paused_too_long` alert. Reporting it here too would
//! duplicate that signal under a different name.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use autumn_harvest::queue::{self, PendingQueueDemand};
use autumn_harvest::queue_pause;
use autumn_harvest::worker::DbPool;
use autumn_harvest::workers::{WorkerFilters, WorkerHealth, WorkerRow, WorkerStatus, list_workers};
use chrono::{DateTime, Utc};
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::api::HarvestApiState;
use crate::shard_fanout::{self, FanoutStatus, ShardObservation};

/// Query string accepted by `GET /admin/queue-coverage`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct QueueCoverageQuery {
    /// Narrow the report to a single queue name. Any string is a valid
    /// value — an unknown/never-scheduled queue simply yields
    /// `uncovered: false` (there is nothing pending on it), never an error.
    pub queue_name: Option<String>,
}

/// Overall cross-shard completeness of the report.
///
/// A type alias onto the shared [`FanoutStatus`] so the
/// complete/partial/unavailable derivation rule lives in exactly one place,
/// alongside `workflow_reachability`'s and `workflow_count`'s reports.
pub type QueueCoverageReportStatus = FanoutStatus;

/// Per-shard inspection outcome.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum QueueCoverageShardStatus {
    /// The shard was queried successfully.
    Inspected,
    /// The shard could not be queried; its queues are not in this report.
    Unavailable,
}

/// Read-only fleet-wide queue-coverage report.
#[derive(Debug, Clone, Serialize)]
pub struct QueueCoverageReport {
    /// Cross-shard completeness. An `uncovered: false` verdict is
    /// authoritative only when this is `complete` — an unreachable shard
    /// could host an uncovered queue this report never saw.
    pub status: QueueCoverageReportStatus,
    /// When the report was assembled.
    pub observed_at: DateTime<Utc>,
    /// Echo of the `queue_name` filter, if any.
    pub filter: Option<String>,
    /// Top-level summary (AC4): `true` when at least one queue has pending
    /// work and zero live pollers, so a CI/CD gate asserts "fully covered"
    /// with a single-field check instead of walking `items`. Reflects only
    /// the inspected shards.
    pub uncovered: bool,
    /// Count of distinct uncovered queue names — the single number a
    /// "fully covered" gate compares against.
    pub total_uncovered_queues: usize,
    /// One entry per uncovered queue, sorted by `queue_name`.
    pub items: Vec<QueueCoverageEntry>,
    /// Per-shard inspection outcomes, including any unavailable shard.
    pub shards: Vec<QueueCoverageShardInspection>,
}

/// One queue with pending work and zero live pollers, aggregated across
/// inspected shards.
#[derive(Debug, Clone, Serialize)]
pub struct QueueCoverageEntry {
    /// The uncovered queue's name.
    pub queue_name: String,
    /// Total `PENDING` tasks on this queue across inspected shards.
    pub pending_count: i64,
    /// A bounded (`QUEUE_COVERAGE_SAMPLE_CAP`) set of representative task
    /// ids (AC3), for chaining into the per-task eligibility explainer.
    pub sample_task_ids: Vec<Uuid>,
    /// A bounded set of representative execution ids (AC3), for chaining
    /// directly into `GET /workflows/{id}`.
    pub sample_execution_ids: Vec<Uuid>,
    /// Per-shard pending counts (only shards where this queue is
    /// uncovered appear). Sorted by `shard_id`.
    pub shard_breakdown: Vec<QueueCoverageShardCount>,
}

/// One shard's contribution to an uncovered queue's pending count.
#[derive(Debug, Clone, Serialize)]
pub struct QueueCoverageShardCount {
    pub shard_id: i32,
    pub pending_count: i64,
}

/// Per-shard inspection record surfaced in the report.
#[derive(Debug, Clone, Serialize)]
pub struct QueueCoverageShardInspection {
    pub shard_id: i32,
    pub status: QueueCoverageShardStatus,
    pub error: Option<String>,
}

/// One shard's raw observation: a queue with pending work that has zero
/// live pollers *on this shard*, before cross-shard aggregation.
#[derive(Debug, Clone)]
struct UncoveredQueueDemand {
    queue_name: String,
    pending_count: i64,
    sample_task_ids: Vec<Uuid>,
    sample_execution_ids: Vec<Uuid>,
}

/// Per-queue accumulator: aggregates uncovered demand across shards.
///
/// Mirrors `workflow_reachability::ReachabilityAccumulator`'s shape:
/// `per_shard` drives the total and breakdown; `samples_task`/`samples_exec`
/// are flat, shard-ordered concatenations of each shard's already-bounded
/// sample ids, capped again on read.
#[derive(Debug)]
struct QueueCoverageAccumulator {
    per_shard: BTreeMap<i32, i64>,
    samples_task: Vec<Uuid>,
    samples_exec: Vec<Uuid>,
}

impl QueueCoverageAccumulator {
    const fn empty() -> Self {
        Self {
            per_shard: BTreeMap::new(),
            samples_task: Vec::new(),
            samples_exec: Vec::new(),
        }
    }

    fn add(&mut self, shard_id: i32, row: &UncoveredQueueDemand) {
        self.per_shard
            .entry(shard_id)
            .and_modify(|count| *count += row.pending_count)
            .or_insert(row.pending_count);
        self.samples_task
            .extend(row.sample_task_ids.iter().copied());
        self.samples_exec
            .extend(row.sample_execution_ids.iter().copied());
    }

    fn pending_count(&self) -> i64 {
        self.per_shard.values().sum()
    }

    /// The cross-shard task-id sample union, truncated to `cap`.
    fn samples_task(&self, cap: usize) -> Vec<Uuid> {
        self.samples_task.iter().take(cap).copied().collect()
    }

    /// The cross-shard execution-id sample union, truncated to `cap`.
    fn samples_exec(&self, cap: usize) -> Vec<Uuid> {
        self.samples_exec.iter().take(cap).copied().collect()
    }
}

/// Build the queue-coverage report without mutating any state.
///
/// Infallible by construction (mirrors `shard_health`'s report handler): a
/// per-shard DB or query failure is folded into that shard's
/// [`ShardObservation`] error rather than an `Err`, so a partial fleet
/// outage degrades the report's `status` instead of failing the whole
/// request.
pub async fn build_queue_coverage_report(
    api_state: &HarvestApiState,
    query: QueueCoverageQuery,
) -> QueueCoverageReport {
    let observed_at = Utc::now();
    let worker_stale_threshold = api_state.worker_stale_threshold();

    let pools = shard_fanout::pools_by_shard(api_state);
    let expected_shards = shard_fanout::expected_shards(api_state, &pools);

    let filter = query.queue_name.clone();

    let observations = expected_shards
        .iter()
        .map(|shard_id| {
            let pool = pools.get(shard_id).cloned();
            observe_shard(*shard_id, pool, filter.clone(), worker_stale_threshold)
        })
        .collect::<Vec<_>>();
    let observations = join_all(observations).await;

    build_report_from_observations(observed_at, filter, observations)
}

/// Whether `worker` is a live poller of `queue_name` on `shard_id` for
/// coverage purposes: assigned to the shard, has a fresh heartbeat, is
/// `Active` or `Draining` (not `Stopped`), and lists the queue.
fn worker_covers_queue(worker: &WorkerRow, queue_name: &str, shard_id: i32) -> bool {
    let is_live = worker.health == WorkerHealth::Healthy
        && (worker.worker.status == WorkerStatus::Active.as_str()
            || worker.worker.status == WorkerStatus::Draining.as_str());
    let assigned_to_shard = worker
        .worker
        .shard_assignments
        .as_array()
        .is_some_and(|shards| {
            shards
                .iter()
                .any(|value| value.as_i64() == Some(i64::from(shard_id)))
        });
    let has_queue = worker.worker.queues.as_array().is_some_and(|queues| {
        queues
            .iter()
            .any(|value| value.as_str() == Some(queue_name))
    });
    is_live && assigned_to_shard && has_queue
}

async fn observe_shard(
    shard_id: i32,
    pool: Option<DbPool>,
    filter: Option<String>,
    worker_stale_threshold: Duration,
) -> ShardObservation<UncoveredQueueDemand> {
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

    let pending: Vec<PendingQueueDemand> =
        match queue::pending_queue_demand_by_queue_name(&mut conn, filter.as_deref()).await {
            Ok(rows) => rows,
            Err(error) => {
                return ShardObservation {
                    shard_id,
                    rows: Vec::new(),
                    error: Some(error.to_string()),
                };
            }
        };
    // Nothing pending on this shard (optionally narrowed by `filter`) —
    // skip the worker/pause reads entirely; there is nothing to report.
    if pending.is_empty() {
        return ShardObservation {
            shard_id,
            rows: Vec::new(),
            error: None,
        };
    }

    let worker_filters = WorkerFilters {
        limit: i64::MAX,
        ..WorkerFilters::new()
    };
    let workers = match list_workers(&mut conn, &worker_filters, worker_stale_threshold).await {
        Ok(workers) => workers,
        Err(error) => {
            return ShardObservation {
                shard_id,
                rows: Vec::new(),
                error: Some(error.to_string()),
            };
        }
    };

    let paused: BTreeSet<String> = match queue_pause::paused_queue_names(&mut conn).await {
        Ok(names) => names.into_iter().collect(),
        Err(error) => {
            return ShardObservation {
                shard_id,
                rows: Vec::new(),
                error: Some(error.to_string()),
            };
        }
    };

    let rows = pending
        .into_iter()
        .filter(|demand| !paused.contains(&demand.queue_name))
        .filter(|demand| {
            !workers
                .iter()
                .any(|worker| worker_covers_queue(worker, &demand.queue_name, shard_id))
        })
        .map(|demand| UncoveredQueueDemand {
            queue_name: demand.queue_name,
            pending_count: demand.pending_count,
            sample_task_ids: demand.sample_task_ids,
            sample_execution_ids: demand.sample_execution_ids,
        })
        .collect();

    ShardObservation {
        shard_id,
        rows,
        error: None,
    }
}

fn build_report_from_observations(
    observed_at: DateTime<Utc>,
    filter: Option<String>,
    observations: Vec<ShardObservation<UncoveredQueueDemand>>,
) -> QueueCoverageReport {
    let inspected_shards = observations
        .iter()
        .filter(|observation| observation.error.is_none())
        .count();
    let unavailable_shards = observations
        .iter()
        .filter(|observation| observation.error.is_some())
        .count();

    let mut accumulators = BTreeMap::<String, QueueCoverageAccumulator>::new();
    for observation in &observations {
        for row in &observation.rows {
            accumulators
                .entry(row.queue_name.clone())
                .or_insert_with(QueueCoverageAccumulator::empty)
                .add(observation.shard_id, row);
        }
    }

    let mut items = accumulators
        .into_iter()
        .map(|(queue_name, acc)| {
            let shard_breakdown = acc
                .per_shard
                .iter()
                .map(|(shard_id, count)| QueueCoverageShardCount {
                    shard_id: *shard_id,
                    pending_count: *count,
                })
                .collect();
            QueueCoverageEntry {
                pending_count: acc.pending_count(),
                sample_task_ids: acc.samples_task(queue::QUEUE_COVERAGE_SAMPLE_CAP),
                sample_execution_ids: acc.samples_exec(queue::QUEUE_COVERAGE_SAMPLE_CAP),
                shard_breakdown,
                queue_name,
            }
        })
        .collect::<Vec<_>>();
    items.sort_by(|a, b| a.queue_name.cmp(&b.queue_name));

    let uncovered = !items.is_empty();
    let total_uncovered_queues = items.len();

    let mut shards = observations
        .into_iter()
        .map(|observation| QueueCoverageShardInspection {
            shard_id: observation.shard_id,
            status: if observation.error.is_some() {
                QueueCoverageShardStatus::Unavailable
            } else {
                QueueCoverageShardStatus::Inspected
            },
            error: observation.error,
        })
        .collect::<Vec<_>>();
    shards.sort_by_key(|shard| shard.shard_id);

    QueueCoverageReport {
        status: FanoutStatus::from_counts(inspected_shards, unavailable_shards),
        observed_at,
        filter,
        uncovered,
        total_uncovered_queues,
        items,
        shards,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone as _;

    fn at(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0).unwrap()
    }

    fn demand(queue_name: &str, pending_count: i64) -> UncoveredQueueDemand {
        UncoveredQueueDemand {
            queue_name: queue_name.to_string(),
            pending_count,
            sample_task_ids: Vec::new(),
            sample_execution_ids: Vec::new(),
        }
    }

    fn demand_with_samples(
        queue_name: &str,
        pending_count: i64,
        task_samples: &[Uuid],
        exec_samples: &[Uuid],
    ) -> UncoveredQueueDemand {
        UncoveredQueueDemand {
            queue_name: queue_name.to_string(),
            pending_count,
            sample_task_ids: task_samples.to_vec(),
            sample_execution_ids: exec_samples.to_vec(),
        }
    }

    fn obs(
        shard_id: i32,
        rows: Vec<UncoveredQueueDemand>,
        error: Option<&str>,
    ) -> ShardObservation<UncoveredQueueDemand> {
        ShardObservation {
            shard_id,
            rows,
            error: error.map(ToOwned::to_owned),
        }
    }

    fn item<'a>(report: &'a QueueCoverageReport, queue_name: &str) -> &'a QueueCoverageEntry {
        report
            .items
            .iter()
            .find(|item| item.queue_name == queue_name)
            .unwrap_or_else(|| panic!("expected item for {queue_name}"))
    }

    #[test]
    fn no_uncovered_queues_is_fully_covered() {
        let report = build_report_from_observations(at(1000), None, vec![obs(0, Vec::new(), None)]);

        assert_eq!(report.status, QueueCoverageReportStatus::Complete);
        assert!(!report.uncovered);
        assert_eq!(report.total_uncovered_queues, 0);
        assert!(report.items.is_empty());
    }

    #[test]
    fn single_uncovered_queue_is_reported() {
        let report = build_report_from_observations(
            at(1000),
            None,
            vec![obs(0, vec![demand("typo_queue", 42)], None)],
        );

        assert!(report.uncovered);
        assert_eq!(report.total_uncovered_queues, 1);
        let entry = item(&report, "typo_queue");
        assert_eq!(entry.pending_count, 42);
        assert_eq!(entry.shard_breakdown.len(), 1);
        assert_eq!(entry.shard_breakdown[0].shard_id, 0);
        assert_eq!(entry.shard_breakdown[0].pending_count, 42);
    }

    #[test]
    fn pending_counts_aggregate_across_shards_with_breakdown() {
        let report = build_report_from_observations(
            at(1000),
            None,
            vec![
                obs(0, vec![demand("orphan_queue", 10)], None),
                obs(1, vec![demand("orphan_queue", 5)], None),
            ],
        );

        let entry = item(&report, "orphan_queue");
        assert_eq!(entry.pending_count, 15);
        assert_eq!(entry.shard_breakdown.len(), 2);
        assert_eq!(entry.shard_breakdown[0].shard_id, 0);
        assert_eq!(entry.shard_breakdown[0].pending_count, 10);
        assert_eq!(entry.shard_breakdown[1].shard_id, 1);
        assert_eq!(entry.shard_breakdown[1].pending_count, 5);
    }

    #[test]
    fn unavailable_shard_makes_report_partial_and_is_named() {
        let report = build_report_from_observations(
            at(1000),
            None,
            vec![
                obs(0, vec![demand("orphan_queue", 1)], None),
                obs(1, Vec::new(), Some("connection refused")),
            ],
        );

        assert_eq!(report.status, QueueCoverageReportStatus::Partial);
        let shard1 = report
            .shards
            .iter()
            .find(|shard| shard.shard_id == 1)
            .expect("shard 1 must be reported");
        assert_eq!(shard1.status, QueueCoverageShardStatus::Unavailable);
        assert_eq!(shard1.error.as_deref(), Some("connection refused"));
    }

    #[test]
    fn all_shards_unavailable_makes_report_unavailable() {
        let report = build_report_from_observations(
            at(1000),
            None,
            vec![obs(0, Vec::new(), Some("pool missing"))],
        );

        assert_eq!(report.status, QueueCoverageReportStatus::Unavailable);
        // An unavailable report must never assert clean coverage.
        assert!(!report.uncovered);
        assert_ne!(report.status, QueueCoverageReportStatus::Complete);
    }

    #[test]
    fn shard_status_serializes_lowercase() {
        assert_eq!(
            serde_json::to_value(QueueCoverageShardStatus::Inspected).unwrap(),
            serde_json::json!("inspected")
        );
        assert_eq!(
            serde_json::to_value(QueueCoverageShardStatus::Unavailable).unwrap(),
            serde_json::json!("unavailable")
        );
    }

    #[test]
    fn items_are_sorted_by_queue_name() {
        let report = build_report_from_observations(
            at(1000),
            None,
            vec![obs(
                0,
                vec![demand("zeta_queue", 1), demand("alpha_queue", 1)],
                None,
            )],
        );

        let names: Vec<&str> = report.items.iter().map(|i| i.queue_name.as_str()).collect();
        assert_eq!(names, vec!["alpha_queue", "zeta_queue"]);
    }

    // ── AC3: bounded representative sample ids ───────────────────────────

    #[test]
    fn samples_are_capped_and_bounded_per_queue() {
        let shard0_task: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();
        let shard1_task: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();
        let shard0_exec: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();
        let shard1_exec: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();
        let all_task: BTreeSet<Uuid> = shard0_task
            .iter()
            .chain(shard1_task.iter())
            .copied()
            .collect();
        let all_exec: BTreeSet<Uuid> = shard0_exec
            .iter()
            .chain(shard1_exec.iter())
            .copied()
            .collect();

        let report = build_report_from_observations(
            at(1000),
            None,
            vec![
                obs(
                    0,
                    vec![demand_with_samples(
                        "wide_queue",
                        40,
                        &shard0_task,
                        &shard0_exec,
                    )],
                    None,
                ),
                obs(
                    1,
                    vec![demand_with_samples(
                        "wide_queue",
                        30,
                        &shard1_task,
                        &shard1_exec,
                    )],
                    None,
                ),
            ],
        );

        let entry = item(&report, "wide_queue");
        assert_eq!(entry.pending_count, 70);
        assert_eq!(
            entry.sample_task_ids.len(),
            queue::QUEUE_COVERAGE_SAMPLE_CAP,
            "the cross-shard task-id sample union must be capped"
        );
        assert_eq!(
            entry.sample_execution_ids.len(),
            queue::QUEUE_COVERAGE_SAMPLE_CAP,
            "the cross-shard execution-id sample union must be capped"
        );
        for id in &entry.sample_task_ids {
            assert!(all_task.contains(id), "every task sample must be seeded");
        }
        for id in &entry.sample_execution_ids {
            assert!(
                all_exec.contains(id),
                "every execution sample must be seeded"
            );
        }
    }

    #[test]
    fn samples_merge_across_shards_when_under_cap() {
        let a: Vec<Uuid> = (0..2).map(|_| Uuid::new_v4()).collect();
        let b: Vec<Uuid> = (0..2).map(|_| Uuid::new_v4()).collect();
        let report = build_report_from_observations(
            at(1000),
            None,
            vec![
                obs(0, vec![demand_with_samples("q", 2, &a, &[])], None),
                obs(1, vec![demand_with_samples("q", 2, &b, &[])], None),
            ],
        );
        let entry = item(&report, "q");
        let got: BTreeSet<Uuid> = entry.sample_task_ids.iter().copied().collect();
        for id in a.iter().chain(b.iter()) {
            assert!(got.contains(id), "merged samples must include both shards");
        }
        assert_eq!(entry.sample_task_ids.len(), 4);
    }

    // ── worker_covers_queue coverage matrix (AC1/AC2) ────────────────────

    fn worker_row(
        status: WorkerStatus,
        health: WorkerHealth,
        shard_assignments: &[i64],
        queues: &[&str],
    ) -> WorkerRow {
        use autumn_harvest::models::HarvestWorker;

        WorkerRow {
            worker: HarvestWorker {
                worker_id: "worker-1".to_string(),
                started_at: at(0),
                last_heartbeat_at: at(0),
                queues: serde_json::json!(queues),
                shard_assignments: serde_json::json!(shard_assignments),
                max_concurrency: 10,
                in_flight_count: 0,
                host: "host".to_string(),
                version: None,
                status: status.as_str().to_string(),
                drain_deadline_at: None,
                build_id: String::new(),
                deployment_name: None,
                labels: serde_json::json!({}),
                max_concurrent_sessions: 0,
                in_use_sessions: 0,
            },
            health,
            active_task_ids: Vec::new(),
        }
    }

    #[test]
    fn active_worker_with_matching_queue_and_shard_covers() {
        let worker = worker_row(
            WorkerStatus::Active,
            WorkerHealth::Healthy,
            &[0],
            &["orphan_queue"],
        );
        assert!(worker_covers_queue(&worker, "orphan_queue", 0));
    }

    #[test]
    fn draining_worker_still_covers_ac1() {
        // AC1: Draining counts as covering, only Stopped fails to cover.
        let worker = worker_row(
            WorkerStatus::Draining,
            WorkerHealth::Healthy,
            &[0],
            &["orphan_queue"],
        );
        assert!(worker_covers_queue(&worker, "orphan_queue", 0));
    }

    #[test]
    fn stopped_worker_does_not_cover() {
        let worker = worker_row(
            WorkerStatus::Stopped,
            WorkerHealth::Healthy,
            &[0],
            &["orphan_queue"],
        );
        assert!(!worker_covers_queue(&worker, "orphan_queue", 0));
    }

    #[test]
    fn stale_worker_does_not_cover() {
        let worker = worker_row(
            WorkerStatus::Active,
            WorkerHealth::Stale,
            &[0],
            &["orphan_queue"],
        );
        assert!(!worker_covers_queue(&worker, "orphan_queue", 0));
    }

    #[test]
    fn worker_on_different_shard_does_not_cover() {
        let worker = worker_row(
            WorkerStatus::Active,
            WorkerHealth::Healthy,
            &[1],
            &["orphan_queue"],
        );
        assert!(!worker_covers_queue(&worker, "orphan_queue", 0));
    }

    #[test]
    fn worker_without_the_queue_does_not_cover() {
        let worker = worker_row(
            WorkerStatus::Active,
            WorkerHealth::Healthy,
            &[0],
            &["other_queue"],
        );
        assert!(!worker_covers_queue(&worker, "orphan_queue", 0));
    }
}
