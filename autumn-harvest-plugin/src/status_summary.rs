//! Rolled-up management health summary for `GET /api/harvest/admin/status`
//! (issue #679).
//!
//! Composes five existing read models into one JSON document with a single
//! worst-of verdict so an operator (or a dashboard/CI gate) can answer *"is
//! Harvest healthy right now?"* in one request instead of polling
//! `/workers/health`, `/admin/shards/health`, `/dead-letters/aggregate`, and
//! `/workflows?no_progress_minutes=N` separately and reconciling them by hand.
//!
//! This module never duplicates or changes any per-domain check's semantics:
//! the `shards` subsystem is [`crate::shard_health::build_shard_health_report`]
//! verbatim; the worker/DLQ/queue/stalled numbers come from the same core
//! helpers those endpoints use, fanned out across shards with the shared
//! [`crate::shard_fanout`] scaffold. All the classification logic (thresholds →
//! `healthy`/`degraded`/`critical`) is in pure, unit-testable functions.
//!
//! Thresholds are **starter defaults, not universal SLOs** — see
//! [`StatusThresholds`].

use std::collections::BTreeMap;

use autumn_harvest::worker::DbPool;
use autumn_harvest::workers::{WorkerFilters, WorkerHealth, WorkerRow, WorkerStatus, list_workers};
use chrono::{DateTime, Utc};
use diesel::sql_types::{BigInt, Nullable};
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use futures::future::join_all;
use serde::Serialize;

use crate::api::{HarvestApiState, dedup_workers_by_freshest};
use crate::shard_fanout::{self, ShardObservation, UnavailableShard};
use crate::shard_health::{
    REASON_SHARD_UNREACHABLE, ShardHealthReport, ShardReadiness, build_shard_health_report,
};

// ── Reason codes ────────────────────────────────────────────────────────────
// Bounded, low-cardinality machine-readable codes surfaced per subsystem.

const REASON_WORKER_NO_ACTIVE: &str = "worker_no_active";
const REASON_WORKER_UNHEALTHY_FRACTION: &str = "worker_unhealthy_fraction";
const REASON_DLQ_BACKLOG: &str = "dlq_backlog";
const REASON_DLQ_RECENT_ENTRY: &str = "dlq_recent_entry";
const REASON_QUEUE_BACKLOG: &str = "queue_backlog";
const REASON_STALLED_WORKFLOWS: &str = "stalled_workflows";

// ── Drill-down paths ────────────────────────────────────────────────────────

const DRILL_DOWN_WORKERS: &str = "/workers/health";
const DRILL_DOWN_SHARDS: &str = "/admin/shards/health";
const DRILL_DOWN_DEAD_LETTERS: &str = "/dead-letters/aggregate";
const DRILL_DOWN_QUEUES: &str = "/admin/shards/health";

/// Verdict for one subsystem (and the overall rollup).
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SubsystemStatus {
    /// No signal breaches a threshold.
    Healthy,
    /// At least one signal breaches a degraded threshold; needs attention but
    /// is not an active outage.
    Degraded,
    /// At least one signal breaches a critical threshold; an active problem.
    Critical,
}

impl SubsystemStatus {
    const fn rank(self) -> u8 {
        match self {
            Self::Healthy => 0,
            Self::Degraded => 1,
            Self::Critical => 2,
        }
    }
}

/// Fold an iterator of subsystem verdicts into the single worst one.
///
/// `Critical` dominates `Degraded` dominates `Healthy`; an empty iterator is
/// `Healthy`.
pub fn worst_status(statuses: impl IntoIterator<Item = SubsystemStatus>) -> SubsystemStatus {
    statuses
        .into_iter()
        .fold(SubsystemStatus::Healthy, |acc, s| {
            if s.rank() > acc.rank() { s } else { acc }
        })
}

/// Tunable thresholds for the rolled-up status verdict.
///
/// **These are starter defaults, not universal SLOs.** Every deployment has a
/// different tolerance for DLQ depth, queue backlog, and stalled runs; override
/// them per environment with [`crate::plugin::HarvestPlugin::with_status_thresholds`].
#[derive(Debug, Clone)]
pub struct StatusThresholds {
    /// Any dead letter at or above this total marks dead-letters `degraded`
    /// (default `1` — any dead letter is worth a look).
    pub dlq_degraded_total: i64,
    /// Dead-letter total at or above this marks dead-letters `critical`
    /// (default `100`).
    pub dlq_critical_total: i64,
    /// If the newest dead letter is younger than this many seconds, treat it as
    /// an active failure and mark dead-letters `critical` (default `300` — a
    /// dead-letter within 5 minutes).
    pub dlq_critical_recent_secs: i64,
    /// Busiest-queue claimable backlog at or above this marks queues `degraded`
    /// (default `1000`).
    pub queue_degraded_backlog: i64,
    /// Busiest-queue claimable backlog at or above this marks queues `critical`
    /// (default `10000`).
    pub queue_critical_backlog: i64,
    /// No-progress window (minutes) used to count stalled executions and to
    /// build the stalled drill-down link (default `60`).
    pub stalled_no_progress_minutes: i64,
    /// Stalled-execution count at or above this marks stalled `degraded`
    /// (default `1`).
    pub stalled_degraded_count: i64,
    /// Stalled-execution count at or above this marks stalled `critical`
    /// (default `50`).
    pub stalled_critical_count: i64,
    /// Fraction of unhealthy (stale) workers at or above which workers is
    /// `degraded` (default `0.25`).
    pub worker_degraded_unhealthy_fraction: f64,
    /// Fraction of unhealthy (stale) workers at or above which workers is
    /// `critical` (default `0.5`). Zero active workers while any are registered
    /// is always `critical`, independent of this fraction.
    pub worker_critical_unhealthy_fraction: f64,
}

impl Default for StatusThresholds {
    fn default() -> Self {
        Self {
            dlq_degraded_total: 1,
            dlq_critical_total: 100,
            dlq_critical_recent_secs: 300,
            queue_degraded_backlog: 1_000,
            queue_critical_backlog: 10_000,
            stalled_no_progress_minutes: 60,
            stalled_degraded_count: 1,
            stalled_critical_count: 50,
            worker_degraded_unhealthy_fraction: 0.25,
            worker_critical_unhealthy_fraction: 0.5,
        }
    }
}

/// One subsystem block in the rolled-up response.
///
/// Common fields plus a flattened object of that subsystem's headline numbers
/// (`active`/`total`, `max_backlog`, `count`, …). `drill_down` is `Some`
/// (a management-API path to investigate) only when the subsystem is not
/// `healthy`; `null` otherwise.
#[derive(Debug, Clone, Serialize)]
pub struct SubsystemHealth {
    /// Stable subsystem identifier: `workers`, `shards`, `dead_letters`,
    /// `queues`, or `stalled_workflows`.
    pub name: &'static str,
    /// This subsystem's verdict.
    pub status: SubsystemStatus,
    /// Bounded, deduped, sorted machine-readable reason codes.
    pub reason_codes: Vec<String>,
    /// Management-API path to drill into this subsystem — `Some` only when the
    /// subsystem is not `healthy`, `null` otherwise.
    pub drill_down: Option<String>,
    /// This subsystem's headline numbers, flattened into the block.
    #[serde(flatten)]
    pub metrics: serde_json::Value,
}

/// The full rolled-up management health summary (issue #679).
#[derive(Debug, Clone, Serialize)]
pub struct HealthSummaryReport {
    /// Worst-of the five subsystem verdicts.
    pub status: SubsystemStatus,
    /// When this snapshot was assembled.
    pub as_of: DateTime<Utc>,
    /// The five subsystem blocks, in a stable order.
    pub subsystems: Vec<SubsystemHealth>,
    /// Shards that could not be inspected (from either the shard-health pass or
    /// the bundle fan-out), deduped by `shard_id` and sorted.
    pub unavailable_shards: Vec<UnavailableShard>,
}

// ── Pure classifiers ────────────────────────────────────────────────────────

/// Classify the worker fleet from its deduped counts.
///
/// Two independent sub-verdicts are combined with [`worst_status`], drawing a
/// deliberate line between the workers that can *claim* new work and the
/// workers that are merely *supposed to be running*:
///
/// - `active` = workers that can CLAIM new work = **Healthy AND status
///   `Active`**. Draining workers refuse new tasks, and Stale or Stopped
///   workers are not running, so none of them count as claimable capacity.
/// - `supposed_running` = workers whose status is `Active` **or** `Draining`
///   (regardless of health; `Stopped` excluded). A crashed-but-un-drained
///   worker (status still `Active`, health `Stale`) is counted here, so a
///   silently dead fleet is still visible to the fraction check.
///
/// 1. **Capacity check.** `active == 0 && queue_backlog > 0` ⇒ `critical`
///    (`worker_no_active`): work is queued with nothing able to run it. This
///    fires for an empty, an all-`Stopped`, an all-`Draining`, *and* an
///    all-`Stale`-`Active` fleet whenever a backlog exists — because in every
///    one of those cases no worker can claim the pending work (issue #679
///    review P2: the earlier `supposed_running == 0` short-circuit let a
///    drain window with a backlog read `healthy`).
/// 2. **Health-fraction check.** Runs whenever `supposed_running > 0`,
///    independent of the backlog, so a stale/crashed fleet is flagged even
///    with nothing queued. `unhealthy / supposed_running` is compared against
///    the degraded / critical thresholds; `unhealthy` counts only stale
///    `Active`/`Draining` workers, so `Stopped` rows never inflate it. Skipped
///    when `supposed_running == 0` (no division by zero) — a clean
///    scale-to-zero or fresh install with no backlog therefore stays
///    `healthy`.
///
/// `total` (all deduped workers, `Stopped` included) drives only the response's
/// headline `total` metric — never the verdict. A healthy, saturated fleet
/// with a large backlog stays `healthy` on the workers axis: backlog is the
/// queues subsystem's concern, not the workers'.
#[must_use]
pub fn classify_workers(
    active: usize,
    supposed_running: usize,
    unhealthy: usize,
    total: usize,
    queue_backlog: i64,
    thresholds: &StatusThresholds,
) -> (SubsystemStatus, Vec<String>) {
    // Claimable workers are a subset of the supposed-running (Active+Draining)
    // set; `total` (incl. Stopped) is the headline metric only, not the verdict.
    debug_assert!(
        active <= supposed_running,
        "active ({active}) must not exceed supposed_running ({supposed_running})"
    );
    debug_assert!(
        supposed_running <= total,
        "supposed_running ({supposed_running}) must not exceed total ({total})"
    );
    let mut status = SubsystemStatus::Healthy;
    let mut codes = Vec::new();

    // 1. Capacity: pending work with no worker able to claim it. Keyed on the
    //    claimable count (`active`), so an all-Draining or all-Stale-Active
    //    fleet with a backlog criticals — none of those workers can claim.
    if active == 0 && queue_backlog > 0 {
        status = worst_status([status, SubsystemStatus::Critical]);
        codes.push(REASON_WORKER_NO_ACTIVE.to_string());
    }

    // 2. Health fraction over supposed-to-be-running workers (Stopped excluded).
    //    Independent of the backlog, so a stale fleet is flagged even when idle.
    if supposed_running > 0 {
        #[allow(clippy::cast_precision_loss)]
        let fraction = unhealthy as f64 / supposed_running as f64;
        if fraction >= thresholds.worker_critical_unhealthy_fraction {
            status = worst_status([status, SubsystemStatus::Critical]);
            codes.push(REASON_WORKER_UNHEALTHY_FRACTION.to_string());
        } else if fraction >= thresholds.worker_degraded_unhealthy_fraction {
            status = worst_status([status, SubsystemStatus::Degraded]);
            codes.push(REASON_WORKER_UNHEALTHY_FRACTION.to_string());
        }
    }

    finalize_codes(&mut codes);
    (status, codes)
}

/// Classify the dead-letter queue from its total and newest-entry age.
///
/// A large backlog degrades/criticals by count; a *recent* dead letter (within
/// `dlq_critical_recent_secs`) is treated as an active failure and forces
/// `critical` regardless of the total.
#[must_use]
pub fn classify_dead_letters(
    total: i64,
    newest_entry_age_secs: Option<i64>,
    thresholds: &StatusThresholds,
) -> (SubsystemStatus, Vec<String>) {
    let mut status = SubsystemStatus::Healthy;
    let mut codes = Vec::new();
    if total >= thresholds.dlq_critical_total {
        status = SubsystemStatus::Critical;
        codes.push(REASON_DLQ_BACKLOG.to_string());
    } else if total >= thresholds.dlq_degraded_total {
        status = SubsystemStatus::Degraded;
        codes.push(REASON_DLQ_BACKLOG.to_string());
    }
    if total > 0
        && newest_entry_age_secs.is_some_and(|age| age <= thresholds.dlq_critical_recent_secs)
    {
        status = worst_status([status, SubsystemStatus::Critical]);
        codes.push(REASON_DLQ_RECENT_ENTRY.to_string());
    }
    finalize_codes(&mut codes);
    (status, codes)
}

/// Classify the busiest queue's claimable backlog.
#[must_use]
pub fn classify_queues(
    max_backlog: i64,
    thresholds: &StatusThresholds,
) -> (SubsystemStatus, Vec<String>) {
    let mut codes = Vec::new();
    let status = if max_backlog >= thresholds.queue_critical_backlog {
        codes.push(REASON_QUEUE_BACKLOG.to_string());
        SubsystemStatus::Critical
    } else if max_backlog >= thresholds.queue_degraded_backlog {
        codes.push(REASON_QUEUE_BACKLOG.to_string());
        SubsystemStatus::Degraded
    } else {
        SubsystemStatus::Healthy
    };
    (status, codes)
}

/// Classify the stalled-execution count.
#[must_use]
pub fn classify_stalled(
    count: i64,
    thresholds: &StatusThresholds,
) -> (SubsystemStatus, Vec<String>) {
    let mut codes = Vec::new();
    let status = if count >= thresholds.stalled_critical_count {
        codes.push(REASON_STALLED_WORKFLOWS.to_string());
        SubsystemStatus::Critical
    } else if count >= thresholds.stalled_degraded_count {
        codes.push(REASON_STALLED_WORKFLOWS.to_string());
        SubsystemStatus::Degraded
    } else {
        SubsystemStatus::Healthy
    };
    (status, codes)
}

/// Classify the shards subsystem from the shard-health report.
///
/// Maps `overall_readiness` (`ready`/`degraded`/`unavailable`) to
/// `healthy`/`degraded`/`critical`, counts rows by readiness, and returns the
/// deduped, sorted union of every row's reason codes.
#[must_use]
pub fn classify_shards(
    report: &ShardHealthReport,
) -> (SubsystemStatus, Vec<String>, usize, usize, usize) {
    let status = match report.overall_readiness {
        ShardReadiness::Ready => SubsystemStatus::Healthy,
        ShardReadiness::Degraded => SubsystemStatus::Degraded,
        ShardReadiness::Unavailable => SubsystemStatus::Critical,
    };
    let mut ready = 0;
    let mut degraded = 0;
    let mut unavailable = 0;
    let mut codes = Vec::new();
    for row in &report.shards {
        match row.readiness {
            ShardReadiness::Ready => ready += 1,
            ShardReadiness::Degraded => degraded += 1,
            ShardReadiness::Unavailable => unavailable += 1,
        }
        codes.extend(row.reason_codes.iter().cloned());
    }
    finalize_codes(&mut codes);
    (status, codes, ready, degraded, unavailable)
}

fn finalize_codes(codes: &mut Vec<String>) {
    codes.sort();
    codes.dedup();
}

// ── Bundle merge ────────────────────────────────────────────────────────────

/// The already-merged, cross-shard numbers the four non-shard subsystems are
/// classified from (pure input to [`build_status_response`]).
#[derive(Debug, Clone)]
pub struct StatusBundleMerge {
    /// Deduped workers that are healthy AND active.
    pub workers_active: usize,
    /// Deduped workers whose status is draining.
    pub workers_draining: usize,
    /// Deduped workers that are supposed to be running (status `Active` or
    /// `Draining`, `Stopped` excluded) — the unhealthy-fraction denominator.
    pub workers_supposed_running: usize,
    /// Deduped workers whose health is stale AND that are supposed to be
    /// running (status `Active` or `Draining`) — the unhealthy-fraction
    /// numerator. Intentionally `Stopped` stale workers are excluded so a
    /// scale-down cannot inflate the fraction into a false `critical`.
    pub workers_unhealthy: usize,
    /// Total distinct (deduped) workers across shards (`Stopped` included).
    pub workers_total: usize,
    /// Dead-letter total summed across shards.
    pub dlq_total: i64,
    /// Age (seconds) of the most recent dead letter across shards, if any.
    pub dlq_newest_age_secs: Option<i64>,
    /// Highest per-queue claimable backlog across shards.
    pub queue_max_backlog: i64,
    /// Name of the busiest queue, if any backlog exists.
    pub queue_max_backlog_queue: Option<String>,
    /// Stalled-execution count summed across shards (bounded per shard).
    pub stalled_count: i64,
    /// `true` when at least one expected shard could not be inspected in the
    /// bundle pass — the non-shard subsystems cannot assert `healthy` on
    /// incomplete data.
    pub incomplete: bool,
    /// Shards that could not be inspected in the bundle pass.
    pub unavailable_shards: Vec<UnavailableShard>,
}

/// Per-shard bundle observation (one shard's contribution to the merge).
#[derive(Debug, Clone)]
struct ShardBundle {
    workers: Vec<WorkerRow>,
    dlq_total: i64,
    dlq_newest_age_secs: Option<i64>,
    by_queue: BTreeMap<String, i64>,
    stalled_count: i64,
}

/// Merge per-shard bundle observations into the cross-shard numbers (pure).
///
/// Takes ownership so the merged collections can move each shard's owned
/// `WorkerRow`s and queue-name `String`s in, rather than cloning them.
#[must_use]
fn merge_bundle(observations: Vec<ShardObservation<ShardBundle>>) -> StatusBundleMerge {
    // Summarize errors first (borrows), before the consuming loop moves the rows.
    let (_inspected, unavailable_shards) = shard_fanout::summarize_shard_errors(&observations);
    let incomplete = !unavailable_shards.is_empty();

    let mut all_workers: Vec<WorkerRow> = Vec::new();
    let mut dlq_total = 0i64;
    let mut dlq_newest: Option<i64> = None;
    let mut by_queue: BTreeMap<String, i64> = BTreeMap::new();
    let mut stalled_count = 0i64;

    for observation in observations {
        for bundle in observation.rows {
            all_workers.extend(bundle.workers);
            dlq_total += bundle.dlq_total;
            if let Some(age) = bundle.dlq_newest_age_secs {
                dlq_newest = Some(dlq_newest.map_or(age, |cur| cur.min(age)));
            }
            for (queue, count) in bundle.by_queue {
                *by_queue.entry(queue).or_default() += count;
            }
            stalled_count += bundle.stalled_count;
        }
    }

    // Dedup across shards: a multi-shard worker registers a row in each shard
    // DB, so counting the raw rows would double-count it (mirrors
    // `workers_health`).
    let deduped = dedup_workers_by_freshest(all_workers);
    let workers_total = deduped.len();
    let workers_active = deduped
        .iter()
        .filter(|w| {
            w.health == WorkerHealth::Healthy && w.worker.status == WorkerStatus::Active.as_str()
        })
        .count();
    let workers_draining = deduped
        .iter()
        .filter(|w| w.worker.status == WorkerStatus::Draining.as_str())
        .count();
    // "Supposed to be running" = Active or Draining (Stopped excluded). A
    // Stopped worker is intentionally gone, not unhealthy, so it is left out of
    // the fraction's numerator and denominator entirely (issue #679 review).
    let is_supposed_running = |w: &&WorkerRow| {
        w.worker.status == WorkerStatus::Active.as_str()
            || w.worker.status == WorkerStatus::Draining.as_str()
    };
    let workers_supposed_running = deduped.iter().filter(is_supposed_running).count();
    let workers_unhealthy = deduped
        .iter()
        .filter(|w| w.health == WorkerHealth::Stale && is_supposed_running(w))
        .count();

    // Busiest queue; first-wins on ties (BTreeMap iterates sorted by key, so
    // the alphabetically-first queue wins a tie deterministically).
    let mut queue_max_backlog = 0i64;
    let mut queue_max_backlog_queue: Option<String> = None;
    for (queue, count) in by_queue {
        if count > queue_max_backlog {
            queue_max_backlog = count;
            queue_max_backlog_queue = Some(queue);
        }
    }

    StatusBundleMerge {
        workers_active,
        workers_draining,
        workers_supposed_running,
        workers_unhealthy,
        workers_total,
        dlq_total,
        dlq_newest_age_secs: dlq_newest,
        queue_max_backlog,
        queue_max_backlog_queue,
        stalled_count,
        incomplete,
        unavailable_shards,
    }
}

// ── Response assembly (pure) ─────────────────────────────────────────────────

fn make_subsystem(
    name: &'static str,
    status: SubsystemStatus,
    mut codes: Vec<String>,
    incomplete: bool,
    drill_down_path: String,
    metrics: serde_json::Value,
) -> SubsystemHealth {
    let status = if incomplete {
        codes.push(REASON_SHARD_UNREACHABLE.to_string());
        worst_status([status, SubsystemStatus::Degraded])
    } else {
        status
    };
    finalize_codes(&mut codes);
    let drill_down = (status != SubsystemStatus::Healthy).then_some(drill_down_path);
    SubsystemHealth {
        name,
        status,
        reason_codes: codes,
        drill_down,
        metrics,
    }
}

fn collect_unavailable_shards(
    shard_report: &ShardHealthReport,
    bundle: &StatusBundleMerge,
) -> Vec<UnavailableShard> {
    let mut merged: BTreeMap<i32, UnavailableShard> = BTreeMap::new();
    for row in &shard_report.shards {
        if !row.reachable {
            let reason = row
                .error_summary
                .clone()
                // `.as_slice()` forces the slice inherent `first`; a bare
                // `row.reason_codes.first()` resolves to the in-scope
                // `diesel::RunQueryDsl::first` (a query method) instead.
                .or_else(|| row.reason_codes.as_slice().first().cloned())
                .unwrap_or_else(|| "shard unreachable".to_string());
            merged.entry(row.shard_id).or_insert(UnavailableShard {
                shard_id: row.shard_id,
                reason,
            });
        }
    }
    for shard in &bundle.unavailable_shards {
        merged
            .entry(shard.shard_id)
            .or_insert_with(|| shard.clone());
    }
    merged.into_values().collect()
}

/// Assemble the rolled-up response from the shard-health report and the merged
/// bundle numbers (pure, no DB).
///
/// When the bundle pass was `incomplete` (a shard was unreachable in it), each
/// of the four non-shard subsystems degrades to at least `degraded` and carries
/// the `shard_unreachable` reason code — they cannot assert `healthy` on
/// incomplete data. The `shards` subsystem reflects an unreachable shard via
/// its own readiness instead. The two unavailable-shard lists are merged and
/// deduped by `shard_id`. Top-level `status` is the worst of all five.
#[must_use]
pub fn build_status_response(
    as_of: DateTime<Utc>,
    shard_report: &ShardHealthReport,
    bundle: &StatusBundleMerge,
    thresholds: &StatusThresholds,
) -> HealthSummaryReport {
    // workers
    let (workers_status, workers_codes) = classify_workers(
        bundle.workers_active,
        bundle.workers_supposed_running,
        bundle.workers_unhealthy,
        bundle.workers_total,
        bundle.queue_max_backlog,
        thresholds,
    );
    let workers = make_subsystem(
        "workers",
        workers_status,
        workers_codes,
        bundle.incomplete,
        DRILL_DOWN_WORKERS.to_string(),
        serde_json::json!({
            "active": bundle.workers_active,
            "draining": bundle.workers_draining,
            "unhealthy": bundle.workers_unhealthy,
            "total": bundle.workers_total,
        }),
    );

    // shards (never gets the bundle-incomplete escalation; reflects its own
    // readiness)
    let (shards_status, shards_codes, ready, degraded, unavailable) = classify_shards(shard_report);
    let shards = make_subsystem(
        "shards",
        shards_status,
        shards_codes,
        false,
        DRILL_DOWN_SHARDS.to_string(),
        serde_json::json!({
            "ready": ready,
            "degraded": degraded,
            "unavailable": unavailable,
        }),
    );

    // dead_letters
    let (dl_status, dl_codes) =
        classify_dead_letters(bundle.dlq_total, bundle.dlq_newest_age_secs, thresholds);
    let dead_letters = make_subsystem(
        "dead_letters",
        dl_status,
        dl_codes,
        bundle.incomplete,
        DRILL_DOWN_DEAD_LETTERS.to_string(),
        serde_json::json!({
            "total": bundle.dlq_total,
            "newest_entry_age_secs": bundle.dlq_newest_age_secs,
        }),
    );

    // queues
    let (queues_status, queues_codes) = classify_queues(bundle.queue_max_backlog, thresholds);
    let queues = make_subsystem(
        "queues",
        queues_status,
        queues_codes,
        bundle.incomplete,
        DRILL_DOWN_QUEUES.to_string(),
        serde_json::json!({
            "max_backlog": bundle.queue_max_backlog,
            "max_backlog_queue": bundle.queue_max_backlog_queue,
        }),
    );

    // stalled_workflows
    let (stalled_status, stalled_codes) = classify_stalled(bundle.stalled_count, thresholds);
    let stalled = make_subsystem(
        "stalled_workflows",
        stalled_status,
        stalled_codes,
        bundle.incomplete,
        format!(
            "/workflows?no_progress_minutes={}",
            thresholds.stalled_no_progress_minutes
        ),
        serde_json::json!({ "count": bundle.stalled_count }),
    );

    let subsystems = vec![workers, shards, dead_letters, queues, stalled];
    let status = worst_status(subsystems.iter().map(|s| s.status));

    HealthSummaryReport {
        status,
        as_of,
        subsystems,
        unavailable_shards: collect_unavailable_shards(shard_report, bundle),
    }
}

// ── Async orchestration (DB) ─────────────────────────────────────────────────

/// Build the rolled-up management health summary for `GET /admin/status`.
///
/// Runs the shard-health pass and the per-shard bundle fan-out concurrently,
/// then merges them with the pure [`build_status_response`]. Never fails
/// wholesale: an unreachable shard degrades the affected subsystems and is
/// named in `unavailable_shards` rather than returning an error.
pub async fn build_status_report(
    api_state: &HarvestApiState,
    thresholds: &StatusThresholds,
) -> HealthSummaryReport {
    let as_of = Utc::now();
    // Activity names with a circuit-breaker policy skip the rate-limit gate at
    // claim, so the per-shard claimable-backlog scan must exempt them from its
    // rate-limit filter to match `claim_task` (and the shard-health coverage
    // gate, which resolves the same set — see `shard_health.rs`). Resolved once
    // here: the tracked-names set is per-runtime (in-process/global), not
    // per-shard. Empty when no runtime is installed (early boot) or no breakers
    // are configured — the safe under-count-free fallback.
    let circuit_breaker_activities: Vec<String> = api_state
        .runtime()
        .ok()
        .map(|r| {
            r.registry()
                .circuit_breakers()
                .tracked_activity_names()
                .to_vec()
        })
        .unwrap_or_default();
    let (shard_report, bundle) = tokio::join!(
        build_shard_health_report(api_state, None),
        fan_out_bundle(api_state, thresholds, &circuit_breaker_activities),
    );
    build_status_response(as_of, &shard_report, &bundle, thresholds)
}

async fn fan_out_bundle(
    api_state: &HarvestApiState,
    thresholds: &StatusThresholds,
    circuit_breaker_activities: &[String],
) -> StatusBundleMerge {
    let pools = shard_fanout::pools_by_shard(api_state);
    let expected = shard_fanout::expected_shards(api_state, &pools);
    let stale_threshold = api_state.worker_stale_threshold();

    let observations = expected
        .iter()
        .map(|shard_id| {
            let pool = pools.get(shard_id).cloned();
            observe_bundle_shard(
                *shard_id,
                pool,
                stale_threshold,
                thresholds,
                circuit_breaker_activities,
            )
        })
        .collect::<Vec<_>>();
    let observations = join_all(observations).await;

    merge_bundle(observations)
}

async fn observe_bundle_shard(
    shard_id: i32,
    pool: Option<DbPool>,
    stale_threshold: std::time::Duration,
    thresholds: &StatusThresholds,
    circuit_breaker_activities: &[String],
) -> ShardObservation<ShardBundle> {
    let mut conn = match shard_fanout::acquire_shard_conn(shard_id, pool).await {
        Ok(conn) => conn,
        Err(observation) => return observation,
    };
    match gather_bundle(
        &mut conn,
        stale_threshold,
        thresholds,
        circuit_breaker_activities,
    )
    .await
    {
        Ok(bundle) => ShardObservation {
            shard_id,
            rows: vec![bundle],
            error: None,
        },
        Err(error) => ShardObservation {
            shard_id,
            rows: Vec::new(),
            error: Some(error),
        },
    }
}

async fn gather_bundle(
    conn: &mut AsyncPgConnection,
    stale_threshold: std::time::Duration,
    thresholds: &StatusThresholds,
    circuit_breaker_activities: &[String],
) -> Result<ShardBundle, String> {
    let worker_filters = WorkerFilters {
        limit: i64::MAX,
        ..WorkerFilters::new()
    };
    let workers = list_workers(conn, &worker_filters, stale_threshold)
        .await
        .map_err(|e| e.to_string())?;

    let dlq_total = autumn_harvest::dlq::dead_letter_count(conn)
        .await
        .map_err(|e| e.to_string())?;
    let dlq_newest_age_secs = dlq_newest_age_secs(conn).await?;

    // Reuse the same claimable-demand core helper the shard-health coverage
    // gate uses. Pass the circuit-breaker-tracked activity names so their rows
    // are counted as claimable even when the rate-limit bucket is empty (they
    // skip the rate-limit gate at claim), matching `claim_task` and the
    // shard-health scan — otherwise this under-counts real claimable backlog.
    let demands =
        autumn_harvest::queue::claimable_pending_demand_by_queue(conn, circuit_breaker_activities)
            .await
            .map_err(|e| e.to_string())?;
    let mut by_queue: BTreeMap<String, i64> = BTreeMap::new();
    for demand in &demands {
        *by_queue.entry(demand.queue_name.clone()).or_default() += demand.count;
    }

    // Bounded count: cap at critical+1 so a huge stall backlog stays cheap and
    // the response never loads unbounded rows.
    let cap = thresholds.stalled_critical_count.saturating_add(1);
    let stalled_count =
        count_stalled_candidates(conn, thresholds.stalled_no_progress_minutes, cap).await?;

    Ok(ShardBundle {
        workers,
        dlq_total,
        dlq_newest_age_secs,
        by_queue,
        stalled_count,
    })
}

#[derive(diesel::QueryableByName)]
struct AgeRow {
    #[diesel(sql_type = Nullable<BigInt>)]
    age: Option<i64>,
}

/// Seconds since the most recent dead-letter row on this shard (`NULL` when the
/// DLQ is empty).
async fn dlq_newest_age_secs(conn: &mut AsyncPgConnection) -> Result<Option<i64>, String> {
    let row: AgeRow = diesel::sql_query(
        "SELECT CEIL(EXTRACT(EPOCH FROM (NOW() - MAX(failed_at))))::BIGINT AS age \
         FROM harvest_dead_letters",
    )
    .get_result(conn)
    .await
    .map_err(|e| e.to_string())?;
    Ok(row.age.map(|age| age.max(0)))
}

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    cnt: i64,
}

/// Bounded count of stalled *candidate* executions on this shard.
///
/// Mirrors the candidate predicate of [`crate::api::load_stalled_workflows`]
/// (its default `include_sleeping = false` shape): an active-state execution
/// (`RUNNING`/`SUSPENDED`) with no `harvest_events` row newer than `minutes`,
/// **and** which is not "correctly sleeping" — i.e. it either has other pending
/// work (a claimable task-queue row, a non-terminal child, or an unconsumed
/// signal) or has no sole future-dated durable timer to be legitimately parked
/// on / has an already-overdue timer. The future-timer exclusion predicate
/// below is copied verbatim from `load_stalled_workflows` so the two agree: a
/// workflow idling on a long `ctx.timer`/`receive_signal_timeout` is **not**
/// counted as stalled. Counting stops at `cap` so the status endpoint never
/// scans an unbounded backlog; the drill-down link remains authoritative.
///
/// Cost note: this is a `NOT EXISTS` anti-join per `RUNNING`/`SUSPENDED` row
/// (backed by the `idx_harvest_events_exec_last` covering index), so its cost
/// scales with the active-execution count. `GET /admin/status` is an on-demand
/// triage surface, not a per-second dashboard poll.
async fn count_stalled_candidates(
    conn: &mut AsyncPgConnection,
    minutes: i64,
    cap: i64,
) -> Result<i64, String> {
    // The `AND (... future-timer exclusion ...)` block mirrors the
    // `!include_sleeping` filter in `crate::api::load_stalled_workflows` exactly
    // (only the correlation alias differs: `e.id` here vs. the qualified
    // `harvest_workflow_executions.id` there). Keep the two in sync.
    let row: CountRow = diesel::sql_query(
        "SELECT COUNT(*)::BIGINT AS cnt FROM ( \
             SELECT 1 FROM harvest_workflow_executions e \
             WHERE e.state IN ('RUNNING', 'SUSPENDED') \
             AND NOT EXISTS ( \
                 SELECT 1 FROM harvest_events ev \
                 WHERE ev.workflow_exec_id = e.id \
                 AND ev.timestamp >= NOW() - ($1 * INTERVAL '1 minute') \
             ) \
             AND ( \
                 EXISTS ( \
                     SELECT 1 FROM harvest_task_queue \
                     WHERE workflow_exec_id = e.id \
                     AND state IN ('PENDING','CLAIMED','RUNNING','BACKOFF') \
                 ) \
              OR EXISTS ( \
                     SELECT 1 FROM harvest_workflow_executions c \
                     WHERE c.parent_id = e.id \
                     AND c.state NOT IN ( \
                         'COMPLETED','FAILED','CANCELLED', \
                         'TIMED_OUT','CONTINUED_AS_NEW','TERMINATED' \
                     ) \
                 ) \
              OR EXISTS ( \
                     SELECT 1 FROM harvest_signals \
                     WHERE workflow_exec_id = e.id AND consumed = false \
                 ) \
              OR NOT EXISTS ( \
                     SELECT 1 FROM harvest_timers \
                     WHERE workflow_exec_id = e.id \
                     AND fired = false AND fires_at > NOW() \
                 ) \
              OR EXISTS ( \
                     SELECT 1 FROM harvest_timers \
                     WHERE workflow_exec_id = e.id \
                     AND fired = false AND fires_at <= NOW() \
                 ) \
             ) \
             LIMIT $2 \
         ) t",
    )
    .bind::<BigInt, _>(minutes)
    .bind::<BigInt, _>(cap)
    .get_result(conn)
    .await
    .map_err(|e| e.to_string())?;
    Ok(row.cnt)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shard_fanout::UnavailableShard;
    use crate::shard_health::{
        DlqSummary, QueueDepthSummary, ShardHealthReport, ShardHealthRow, ShardReadiness,
        ShardSchedulerCoverage, ShardSchemaHealth,
    };
    use chrono::{TimeZone as _, Utc};
    use std::collections::BTreeMap;

    fn thresholds() -> StatusThresholds {
        StatusThresholds::default()
    }

    fn shard_row(
        shard_id: i32,
        readiness: ShardReadiness,
        reachable: bool,
        reason_codes: &[&str],
    ) -> ShardHealthRow {
        ShardHealthRow {
            shard_id,
            roles: Vec::new(),
            candidate: false,
            reachable,
            active_worker_count: 0,
            stale_worker_count: 0,
            schema: ShardSchemaHealth {
                ready: true,
                applied_count: Some(1),
                missing_migrations: Vec::new(),
                error: None,
            },
            worker_coverage: Vec::new(),
            scheduler: ShardSchedulerCoverage {
                enabled: false,
                ready: true,
                running: true,
                last_tick_at: None,
                tick_interval_ms: 0,
                freshness_window_secs: 0,
                schedule_count: 0,
                error: None,
            },
            queue_depth: QueueDepthSummary {
                total_pending: 0,
                by_queue: BTreeMap::new(),
                constraint_demands: Vec::new(),
                error: None,
            },
            dlq: DlqSummary {
                count: Some(0),
                error: None,
            },
            last_health_sample_time: None,
            readiness,
            reason_codes: reason_codes.iter().map(|s| (*s).to_string()).collect(),
            blocking_reasons: Vec::new(),
            error_summary: None,
        }
    }

    fn shard_report(overall: ShardReadiness, rows: Vec<ShardHealthRow>) -> ShardHealthReport {
        ShardHealthReport {
            overall_readiness: overall,
            observed_at: Utc.timestamp_opt(1000, 0).unwrap(),
            freshness_window_secs: 10,
            candidate_shard: None,
            shards: rows,
        }
    }

    fn healthy_bundle() -> StatusBundleMerge {
        StatusBundleMerge {
            workers_active: 3,
            workers_draining: 0,
            workers_supposed_running: 3,
            workers_unhealthy: 0,
            workers_total: 3,
            dlq_total: 0,
            dlq_newest_age_secs: None,
            queue_max_backlog: 0,
            queue_max_backlog_queue: None,
            stalled_count: 0,
            incomplete: false,
            unavailable_shards: Vec::new(),
        }
    }

    // ── worst_status ─────────────────────────────────────────────────────────

    #[test]
    fn worst_status_of_empty_is_healthy() {
        assert_eq!(worst_status(std::iter::empty()), SubsystemStatus::Healthy);
    }

    #[test]
    fn worst_status_all_healthy_is_healthy() {
        assert_eq!(
            worst_status([SubsystemStatus::Healthy, SubsystemStatus::Healthy]),
            SubsystemStatus::Healthy
        );
    }

    #[test]
    fn worst_status_one_critical_dominates() {
        assert_eq!(
            worst_status([
                SubsystemStatus::Healthy,
                SubsystemStatus::Degraded,
                SubsystemStatus::Critical,
                SubsystemStatus::Healthy,
            ]),
            SubsystemStatus::Critical
        );
    }

    #[test]
    fn worst_status_degraded_when_no_critical() {
        assert_eq!(
            worst_status([SubsystemStatus::Healthy, SubsystemStatus::Degraded]),
            SubsystemStatus::Degraded
        );
    }

    // ── StatusThresholds::default ──────────────────────────────────────────────

    #[test]
    fn thresholds_default_sanity() {
        let t = StatusThresholds::default();
        assert!(t.dlq_degraded_total >= 1);
        assert!(t.dlq_critical_total > t.dlq_degraded_total);
        assert!(t.dlq_critical_recent_secs > 0);
        assert!(t.queue_critical_backlog > t.queue_degraded_backlog);
        assert!(t.stalled_critical_count > t.stalled_degraded_count);
        assert!(t.stalled_no_progress_minutes > 0);
        assert!(t.worker_critical_unhealthy_fraction > t.worker_degraded_unhealthy_fraction);
    }

    // ── classify_workers ───────────────────────────────────────────────────────

    #[test]
    fn classify_workers_empty_fleet_no_backlog_is_healthy() {
        // No workers, no queued work ⇒ genuinely idle fresh install ⇒ healthy.
        let (status, codes) = classify_workers(0, 0, 0, 0, 0, &thresholds());
        assert_eq!(status, SubsystemStatus::Healthy);
        assert!(codes.is_empty());
    }

    #[test]
    fn classify_workers_empty_fleet_with_backlog_is_critical() {
        // FIX 2 (issue #679 review): no workers but work is queued ⇒ nothing to
        // run it ⇒ critical.
        let (status, codes) = classify_workers(0, 0, 0, 0, 42, &thresholds());
        assert_eq!(status, SubsystemStatus::Critical);
        assert!(codes.contains(&"worker_no_active".to_string()));
    }

    #[test]
    fn classify_workers_zero_active_with_workers_is_critical() {
        // No active workers, but the sole supposed-running (Active/Draining)
        // worker is stale ⇒ fraction 1/1 = 1.0 ≥ critical ⇒ critical. With no
        // backlog the capacity check is skipped, so this surfaces as
        // worker_unhealthy_fraction rather than worker_no_active (which now
        // requires active == 0 AND a pending backlog).
        let (status, codes) = classify_workers(0, 1, 1, 3, 0, &thresholds());
        assert_eq!(status, SubsystemStatus::Critical);
        assert!(codes.contains(&"worker_unhealthy_fraction".to_string()));
        assert!(!codes.contains(&"worker_no_active".to_string()));
    }

    #[test]
    fn classify_workers_stopped_only_no_backlog_is_healthy() {
        // Codex #679 review P2: after a clean scale-to-zero the fleet holds
        // only Stopped rows (supposed_running == 0, total > 0) and no work is
        // queued. This must read healthy, not a false worker_no_active critical.
        let (status, codes) = classify_workers(0, 0, 0, 7, 0, &thresholds());
        assert_eq!(
            status,
            SubsystemStatus::Healthy,
            "an all-Stopped fleet with no backlog must not read critical"
        );
        assert!(!codes.contains(&"worker_no_active".to_string()));
        assert!(codes.is_empty());
    }

    #[test]
    fn classify_workers_stopped_only_with_backlog_is_critical() {
        // Same all-Stopped fleet, but work is queued with nothing live to run
        // it ⇒ critical via supposed_running==0 && backlog>0.
        let (status, codes) = classify_workers(0, 0, 0, 7, 42, &thresholds());
        assert_eq!(status, SubsystemStatus::Critical);
        assert!(codes.contains(&"worker_no_active".to_string()));
    }

    #[test]
    fn classify_workers_degraded_unhealthy_fraction() {
        // 1 of 3 supposed-running unhealthy = 0.33 ≥ degraded 0.25, < critical 0.5.
        let (status, codes) = classify_workers(2, 3, 1, 3, 0, &thresholds());
        assert_eq!(status, SubsystemStatus::Degraded);
        assert!(codes.contains(&"worker_unhealthy_fraction".to_string()));
    }

    #[test]
    fn classify_workers_critical_unhealthy_fraction() {
        // 2 of 3 supposed-running unhealthy = 0.66 ≥ critical 0.5.
        let (status, codes) = classify_workers(1, 3, 2, 3, 0, &thresholds());
        assert_eq!(status, SubsystemStatus::Critical);
        assert!(codes.contains(&"worker_unhealthy_fraction".to_string()));
    }

    #[test]
    fn classify_workers_all_healthy() {
        let (status, codes) = classify_workers(5, 5, 0, 5, 0, &thresholds());
        assert_eq!(status, SubsystemStatus::Healthy);
        assert!(codes.is_empty());
    }

    #[test]
    fn classify_workers_stopped_stale_workers_excluded_from_fraction() {
        // FIX 3 (issue #679 review): 3 Active-healthy + 7 Stopped-stale.
        // The 7 Stopped rows are intentionally gone, not unhealthy, so they are
        // excluded from the fraction: supposed_running = 3, unhealthy = 0.
        // total = 10 (all deduped, for the headline metric only).
        let (status, codes) = classify_workers(3, 3, 0, 10, 0, &thresholds());
        assert_eq!(
            status,
            SubsystemStatus::Healthy,
            "a scaled-down fleet of stale Stopped workers must not read critical"
        );
        assert!(codes.is_empty());
    }

    #[test]
    fn classify_workers_all_draining_with_backlog_is_critical() {
        // THE #679 review P2 case: a deploy/drain window where every non-Stopped
        // worker is Draining (0 claimable) and healthy, with pending work queued.
        // Draining workers cannot claim new tasks, so nothing can run the
        // backlog ⇒ critical worker_no_active — not a false healthy from the old
        // `supposed_running > 0` short-circuit.
        let (status, codes) = classify_workers(0, 3, 0, 3, 42, &thresholds());
        assert_eq!(
            status,
            SubsystemStatus::Critical,
            "an all-Draining fleet with a backlog cannot claim the work"
        );
        assert!(codes.contains(&"worker_no_active".to_string()));
        assert!(!codes.contains(&"worker_unhealthy_fraction".to_string()));
    }

    #[test]
    fn classify_workers_all_draining_no_backlog_is_healthy() {
        // Same all-Draining fleet, but nothing is queued: an idle drain window is
        // healthy — no work is waiting for a claimant.
        let (status, codes) = classify_workers(0, 3, 0, 3, 0, &thresholds());
        assert_eq!(status, SubsystemStatus::Healthy);
        assert!(codes.is_empty());
    }

    #[test]
    fn classify_workers_all_stale_active_no_backlog_is_critical_fraction() {
        // A crashed, un-drained fleet: every worker's status is still Active but
        // all are Stale, with no backlog. The capacity check is skipped (no work
        // queued), but the health fraction (3/3 = 1.0 ≥ critical) still flags the
        // dead fleet ⇒ critical worker_unhealthy_fraction. This is why the
        // fraction check runs independently of the backlog.
        let (status, codes) = classify_workers(0, 3, 3, 3, 0, &thresholds());
        assert_eq!(status, SubsystemStatus::Critical);
        assert!(codes.contains(&"worker_unhealthy_fraction".to_string()));
        assert!(!codes.contains(&"worker_no_active".to_string()));
    }

    #[test]
    fn classify_workers_healthy_saturated_fleet_with_huge_backlog_stays_healthy() {
        // A fully healthy, saturated fleet (active > 0) with a large backlog is
        // healthy on the WORKERS axis — a deep queue is the queues subsystem's
        // concern, not a worker-fleet fault.
        let (status, codes) = classify_workers(3, 6, 0, 6, 1_000_000, &thresholds());
        assert_eq!(status, SubsystemStatus::Healthy);
        assert!(codes.is_empty());
    }

    #[test]
    fn classify_workers_degraded_fraction_with_active_claimant_present() {
        // 1 claimable worker among 4 supposed-running, 1 stale ⇒ fraction
        // 1/4 = 0.25 ≥ degraded 0.25. active > 0 so the capacity check is skipped;
        // the run is degraded on the health fraction alone.
        let (status, codes) = classify_workers(1, 4, 1, 4, 0, &thresholds());
        assert_eq!(status, SubsystemStatus::Degraded);
        assert!(codes.contains(&"worker_unhealthy_fraction".to_string()));
        assert!(!codes.contains(&"worker_no_active".to_string()));
    }

    // ── classify_dead_letters ──────────────────────────────────────────────────

    #[test]
    fn classify_dead_letters_none_is_healthy() {
        let (status, codes) = classify_dead_letters(0, None, &thresholds());
        assert_eq!(status, SubsystemStatus::Healthy);
        assert!(codes.is_empty());
    }

    #[test]
    fn classify_dead_letters_below_critical_is_degraded() {
        let (status, codes) = classify_dead_letters(5, Some(100_000), &thresholds());
        assert_eq!(status, SubsystemStatus::Degraded);
        assert!(codes.contains(&"dlq_backlog".to_string()));
        // Old entry (100_000s > 300s) does NOT escalate to critical.
        assert!(!codes.contains(&"dlq_recent_entry".to_string()));
    }

    #[test]
    fn classify_dead_letters_recent_entry_is_critical() {
        // A fresh dead-letter (age within window) means an active failure ⇒ critical.
        let (status, codes) = classify_dead_letters(5, Some(30), &thresholds());
        assert_eq!(status, SubsystemStatus::Critical);
        assert!(codes.contains(&"dlq_recent_entry".to_string()));
    }

    #[test]
    fn classify_dead_letters_high_total_is_critical() {
        let (status, codes) = classify_dead_letters(500, Some(100_000), &thresholds());
        assert_eq!(status, SubsystemStatus::Critical);
        assert!(codes.contains(&"dlq_backlog".to_string()));
    }

    // ── classify_queues ────────────────────────────────────────────────────────

    #[test]
    fn classify_queues_below_thresholds_is_healthy() {
        let (status, codes) = classify_queues(10, &thresholds());
        assert_eq!(status, SubsystemStatus::Healthy);
        assert!(codes.is_empty());
    }

    #[test]
    fn classify_queues_degraded_backlog() {
        let (status, codes) = classify_queues(2000, &thresholds());
        assert_eq!(status, SubsystemStatus::Degraded);
        assert!(codes.contains(&"queue_backlog".to_string()));
    }

    #[test]
    fn classify_queues_critical_backlog() {
        let (status, codes) = classify_queues(50_000, &thresholds());
        assert_eq!(status, SubsystemStatus::Critical);
        assert!(codes.contains(&"queue_backlog".to_string()));
    }

    // ── classify_stalled ───────────────────────────────────────────────────────

    #[test]
    fn classify_stalled_zero_is_healthy() {
        let (status, codes) = classify_stalled(0, &thresholds());
        assert_eq!(status, SubsystemStatus::Healthy);
        assert!(codes.is_empty());
    }

    #[test]
    fn classify_stalled_degraded() {
        let (status, codes) = classify_stalled(5, &thresholds());
        assert_eq!(status, SubsystemStatus::Degraded);
        assert!(codes.contains(&"stalled_workflows".to_string()));
    }

    #[test]
    fn classify_stalled_critical() {
        let (status, codes) = classify_stalled(100, &thresholds());
        assert_eq!(status, SubsystemStatus::Critical);
        assert!(codes.contains(&"stalled_workflows".to_string()));
    }

    // ── classify_shards ────────────────────────────────────────────────────────

    #[test]
    fn classify_shards_ready_maps_to_healthy() {
        let report = shard_report(
            ShardReadiness::Ready,
            vec![shard_row(0, ShardReadiness::Ready, true, &[])],
        );
        let (status, codes, ready, degraded, unavailable) = classify_shards(&report);
        assert_eq!(status, SubsystemStatus::Healthy);
        assert_eq!((ready, degraded, unavailable), (1, 0, 0));
        assert!(codes.is_empty());
    }

    #[test]
    fn classify_shards_degraded_maps_and_unions_reason_codes() {
        let report = shard_report(
            ShardReadiness::Degraded,
            vec![
                shard_row(0, ShardReadiness::Ready, true, &[]),
                shard_row(
                    1,
                    ShardReadiness::Degraded,
                    true,
                    &["scheduler_stale", "no_live_worker"],
                ),
            ],
        );
        let (status, codes, ready, degraded, unavailable) = classify_shards(&report);
        assert_eq!(status, SubsystemStatus::Degraded);
        assert_eq!((ready, degraded, unavailable), (1, 1, 0));
        // Deduped + sorted union.
        assert_eq!(
            codes,
            vec!["no_live_worker".to_string(), "scheduler_stale".to_string()]
        );
    }

    #[test]
    fn classify_shards_unavailable_maps_to_critical() {
        let report = shard_report(
            ShardReadiness::Unavailable,
            vec![shard_row(
                0,
                ShardReadiness::Unavailable,
                false,
                &["shard_unreachable"],
            )],
        );
        let (status, _codes, ready, degraded, unavailable) = classify_shards(&report);
        assert_eq!(status, SubsystemStatus::Critical);
        assert_eq!((ready, degraded, unavailable), (0, 0, 1));
    }

    // ── build_status_response ──────────────────────────────────────────────────

    #[test]
    fn build_status_response_all_healthy() {
        let report = shard_report(
            ShardReadiness::Ready,
            vec![shard_row(0, ShardReadiness::Ready, true, &[])],
        );
        let resp = build_status_response(
            Utc.timestamp_opt(2000, 0).unwrap(),
            &report,
            &healthy_bundle(),
            &thresholds(),
        );
        assert_eq!(resp.status, SubsystemStatus::Healthy);
        assert_eq!(resp.subsystems.len(), 5);
        // drill_down is null (None) for every healthy subsystem.
        assert!(resp.subsystems.iter().all(|s| s.drill_down.is_none()));
        assert!(resp.unavailable_shards.is_empty());
    }

    #[test]
    fn build_status_response_drill_down_present_only_when_non_healthy() {
        let report = shard_report(
            ShardReadiness::Ready,
            vec![shard_row(0, ShardReadiness::Ready, true, &[])],
        );
        let mut bundle = healthy_bundle();
        bundle.dlq_total = 5; // degraded dead_letters
        let resp = build_status_response(
            Utc.timestamp_opt(2000, 0).unwrap(),
            &report,
            &bundle,
            &thresholds(),
        );
        assert_eq!(resp.status, SubsystemStatus::Degraded);
        let dl = resp
            .subsystems
            .iter()
            .find(|s| s.name == "dead_letters")
            .expect("dead_letters subsystem present");
        assert_eq!(dl.status, SubsystemStatus::Degraded);
        assert_eq!(dl.drill_down.as_deref(), Some("/dead-letters/aggregate"));
        // Other still-healthy subsystems keep drill_down null.
        let workers = resp
            .subsystems
            .iter()
            .find(|s| s.name == "workers")
            .expect("workers subsystem present");
        assert!(workers.drill_down.is_none());
    }

    #[test]
    fn build_status_response_partial_degrades_non_shard_subsystems() {
        let report = shard_report(
            ShardReadiness::Ready,
            vec![shard_row(0, ShardReadiness::Ready, true, &[])],
        );
        let mut bundle = healthy_bundle();
        bundle.incomplete = true;
        bundle.unavailable_shards = vec![UnavailableShard {
            shard_id: 1,
            reason: "connection refused".to_string(),
        }];
        let resp = build_status_response(
            Utc.timestamp_opt(2000, 0).unwrap(),
            &report,
            &bundle,
            &thresholds(),
        );
        // Every non-shard subsystem is at least degraded and names shard_unreachable.
        for name in ["workers", "dead_letters", "queues", "stalled_workflows"] {
            let s = resp
                .subsystems
                .iter()
                .find(|s| s.name == name)
                .unwrap_or_else(|| panic!("{name} subsystem present"));
            assert_ne!(
                s.status,
                SubsystemStatus::Healthy,
                "{name} must degrade on partial"
            );
            assert!(
                s.reason_codes.contains(&"shard_unreachable".to_string()),
                "{name} must carry shard_unreachable on partial"
            );
        }
        // Merged unavailable shard is surfaced.
        assert!(resp.unavailable_shards.iter().any(|u| u.shard_id == 1));
    }

    #[test]
    fn build_status_response_merges_and_dedups_unavailable_shards() {
        // The shard-health report names shard 2 unreachable; the bundle names
        // shards 1 and 2. The merged list dedups by shard_id.
        let report = shard_report(
            ShardReadiness::Unavailable,
            vec![shard_row(
                2,
                ShardReadiness::Unavailable,
                false,
                &["shard_unreachable"],
            )],
        );
        let mut bundle = healthy_bundle();
        bundle.incomplete = true;
        bundle.unavailable_shards = vec![
            UnavailableShard {
                shard_id: 1,
                reason: "a".to_string(),
            },
            UnavailableShard {
                shard_id: 2,
                reason: "b".to_string(),
            },
        ];
        let resp = build_status_response(
            Utc.timestamp_opt(2000, 0).unwrap(),
            &report,
            &bundle,
            &thresholds(),
        );
        let mut ids: Vec<i32> = resp.unavailable_shards.iter().map(|u| u.shard_id).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 2]);
        // Shards subsystem is critical ⇒ overall critical.
        assert_eq!(resp.status, SubsystemStatus::Critical);
    }

    #[test]
    fn subsystem_status_serializes_lowercase() {
        assert_eq!(
            serde_json::to_value(SubsystemStatus::Healthy).unwrap(),
            serde_json::json!("healthy")
        );
        assert_eq!(
            serde_json::to_value(SubsystemStatus::Degraded).unwrap(),
            serde_json::json!("degraded")
        );
        assert_eq!(
            serde_json::to_value(SubsystemStatus::Critical).unwrap(),
            serde_json::json!("critical")
        );
    }

    #[test]
    fn stalled_drill_down_uses_configured_no_progress_minutes() {
        let report = shard_report(
            ShardReadiness::Ready,
            vec![shard_row(0, ShardReadiness::Ready, true, &[])],
        );
        let mut bundle = healthy_bundle();
        bundle.stalled_count = 5; // degraded
        let mut t = thresholds();
        t.stalled_no_progress_minutes = 42;
        let resp = build_status_response(Utc.timestamp_opt(2000, 0).unwrap(), &report, &bundle, &t);
        let stalled = resp
            .subsystems
            .iter()
            .find(|s| s.name == "stalled_workflows")
            .expect("stalled subsystem present");
        assert_eq!(
            stalled.drill_down.as_deref(),
            Some("/workflows?no_progress_minutes=42")
        );
    }
}
