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
//! workers (and stale/absent heartbeats) fail to cover. A worker registered
//! with an **empty** `shard_assignments` array (the `Worker::run` legacy
//! single-shard path, which polls the default pool with no explicit
//! assignment) is treated as covering *whichever shard is being inspected* —
//! matching what it actually polls, not what its registration literally
//! lists — since this module only ever asks that question against a worker
//! row already known to be registered on that exact shard's own database.
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
//!
//! That alert fires on pause *duration* alone, independent of whether the
//! queue actually has stranded backlog, so a queue that is simultaneously
//! paused *and* has zero live pollers *and* has real pending work can go
//! unreported by that alert for its full grace window. To close that
//! visibility gap without duplicating the alert's own signal, the exclusion
//! itself is surfaced in [`QueueCoverageReport::excluded_paused_queues`] —
//! the queue names that would be reported uncovered the moment they were
//! unpaused without gaining a live poller. A paused queue that already has
//! live pollers is not included there (nothing interesting to report — it
//! would not have been uncovered anyway).

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use autumn_harvest::error::HarvestResult;
use autumn_harvest::queue::{self, PendingQueueDemand};
use autumn_harvest::queue_pause;
use autumn_harvest::worker::DbPool;
use autumn_harvest::workers::{WorkerHealth, WorkerRow, WorkerStatus};
use chrono::{DateTime, Utc};
use diesel_async::AsyncPgConnection;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::api::HarvestApiState;
use crate::shard_fanout::{self, FanoutStatus, ShardObservation};

/// A query string component's percent-decoded bytes are not valid UTF-8.
///
/// Returned by [`parse_raw_query_pairs_strict`]; the caller (`GET
/// /admin/queue-coverage`) maps this to a `400` JSON response rather than
/// silently substituting `U+FFFD`, the fallback axum's built-in
/// `Query<T>` extractor performs via `serde_urlencoded`/`form_urlencoded`
/// (issue #774 review).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidQueryEncoding;

impl std::fmt::Display for InvalidQueryEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "query string contains an invalid percent-encoded UTF-8 byte sequence"
        )
    }
}

impl std::error::Error for InvalidQueryEncoding {}

/// Strictly percent-decodes a raw query string into `(key, value)` pairs.
///
/// A malformed value like `?queue_name=%FF` would otherwise silently decode
/// to `queue_name=<U+FFFD>` via axum's lossy `Query<T>` extractor — a
/// legitimate-looking but wrong filter that lets a scoped deploy gate
/// (`GET /admin/queue-coverage`) pass instead of rejecting the request with
/// the documented `400` (issue #774 review).
///
/// Mirrors `form_urlencoded::parse`'s grammar exactly: split on `&`, split
/// each segment on the first `=` (a segment with no `=` is a key with an
/// empty value), `+` decodes to a literal space *before* percent-decoding.
/// An empty segment (from a leading/trailing/doubled `&`, or an entirely
/// empty query string) is skipped.
///
/// # Errors
///
/// Returns [`InvalidQueryEncoding`] on the first key or value that is
/// malformed in either of two ways: a syntactically invalid `%` escape (not
/// followed by exactly two hex digits, e.g. `%`, `%2`, `%GG`), or a
/// syntactically valid escape whose decoded bytes are not valid UTF-8 (e.g.
/// `%FF`). This is the one place in the `queue-coverage` request path that
/// *can* reject an input outright — [`QueueCoverageQuery::from_query_pairs`]
/// below, which consumes this function's output, stays infallible by
/// construction.
pub fn parse_raw_query_pairs_strict(
    raw_query: &str,
) -> Result<Vec<(String, String)>, InvalidQueryEncoding> {
    let mut pairs = Vec::new();
    for segment in raw_query.split('&') {
        if segment.is_empty() {
            continue;
        }
        let (raw_key, raw_value) = segment.split_once('=').unwrap_or((segment, ""));
        pairs.push((
            decode_form_component_strict(raw_key)?,
            decode_form_component_strict(raw_value)?,
        ));
    }
    Ok(pairs)
}

/// Strictly percent-decodes one `application/x-www-form-urlencoded`
/// key/value component: `+` -> space first, then `%XX` percent-decoding
/// with strict (non-lossy) UTF-8 validation of the resulting bytes.
fn decode_form_component_strict(raw: &str) -> Result<String, InvalidQueryEncoding> {
    if !has_only_well_formed_percent_escapes(raw) {
        return Err(InvalidQueryEncoding);
    }
    let space_decoded = raw.replace('+', " ");
    percent_encoding::percent_decode_str(&space_decoded)
        .decode_utf8()
        .map(std::borrow::Cow::into_owned)
        .map_err(|_| InvalidQueryEncoding)
}

/// Whether every `%` in `s` is immediately followed by exactly two ASCII
/// hex digits.
///
/// `percent_encoding::percent_decode_str` does **not** reject a malformed
/// escape on its own: an incomplete (`%`, `%2`) or non-hex (`%GG`) sequence
/// is left as a **literal, undecoded** run of bytes rather than an error --
/// and since those bytes (`%`, `G`, digits, ...) are themselves valid ASCII,
/// `decode_utf8()` still succeeds trivially. Without this pre-check,
/// `?queue_name=orders%GG` would silently decode to the literal string
/// `"orders%GG"` and query that (almost certainly nonexistent) queue name
/// instead of being rejected -- a second, distinct malformed-encoding
/// shape from the already-handled `%FF`-decodes-to-invalid-UTF-8 case
/// (issue #774 review).
fn has_only_well_formed_percent_escapes(s: &str) -> bool {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let well_formed = bytes.get(i + 1).is_some_and(u8::is_ascii_hexdigit)
                && bytes.get(i + 2).is_some_and(u8::is_ascii_hexdigit);
            if !well_formed {
                return false;
            }
            i += 3;
        } else {
            i += 1;
        }
    }
    true
}

/// Query string accepted by `GET /admin/queue-coverage`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct QueueCoverageQuery {
    /// Narrow the report to a single queue name. Any *decoded* string is a
    /// valid value — an unknown/never-scheduled queue simply yields
    /// `uncovered: false` (there is nothing pending on it), never an error.
    /// A malformed *encoding* (invalid percent-decoded UTF-8) is rejected
    /// earlier, by [`parse_raw_query_pairs_strict`], before it ever reaches
    /// this type — see that function's doc comment.
    pub queue_name: Option<String>,
}

impl QueueCoverageQuery {
    /// Parse raw `(key, value)` query-string pairs into a
    /// [`QueueCoverageQuery`].
    ///
    /// Infallible by construction (issue #774 AC8, matching the
    /// `dlq::DlqAggregateParams`/`workflow_count::WorkflowCountParams`/
    /// `usage::UsageParams` convention) **once the pairs are already valid
    /// decoded strings** — see [`parse_raw_query_pairs_strict`] for the
    /// upstream check that actually can reject a request. `queue_name` is a
    /// free-text filter with no invalid *decoded value* — any string is a
    /// legitimate (if possibly never-scheduled) queue name — so unlike a
    /// `group_by`/`state`/date param, there is no value to reject here. A
    /// repeated `queue_name` key resolves last-value-wins (never an error);
    /// an unrecognized key is ignored for forward-compatibility.
    ///
    /// The value is preserved **verbatim** (never trimmed) — only the
    /// literal empty string `""` normalizes to "no filter". This
    /// deliberately diverges from `WorkerFilters::queue`'s
    /// trim-and-treat-blank-as-absent convention: `WorkerConfig::with_queues`
    /// rejects only an *exactly-empty* queue name (`assert!(!q.is_empty())`)
    /// — it does **not** reject leading/trailing whitespace or an
    /// all-whitespace name, so a queue registered as e.g. `" priority "` or
    /// `" "` is unusual but valid. Trimming the filter here would silently
    /// retarget such a query onto a *different* (trimmed) queue name than
    /// the one actually registered — this endpoint's whole job is confirming
    /// coverage for one exact, operator-supplied name — and an
    /// all-whitespace value would collapse to "unfiltered" (matching
    /// *every* queue) rather than "match this one whitespace-named queue",
    /// the opposite of what a caller who typed that value intended. An
    /// exact match that legitimately finds nothing is safer than either.
    /// This intentionally sidesteps a real pre-#774-hardening bug where the
    /// derive-based `Query<QueueCoverageQuery>` extractor's built-in
    /// duplicate-key rejection surfaced as a `400` with a `text/plain` body
    /// rather than the JSON `400` AC8 requires — using this hand-parsed,
    /// always-succeeding path removes that failure mode entirely instead of
    /// just recoloring its response body.
    #[must_use]
    pub fn from_query_pairs(pairs: &[(String, String)]) -> Self {
        let mut params = Self::default();
        for (key, value) in pairs {
            if key == "queue_name" {
                params.queue_name = (!value.is_empty()).then(|| value.clone());
            }
            // Unknown keys are ignored for forward-compatibility.
        }
        params
    }
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
    /// Queue names with pending work and zero live pollers that were
    /// EXCLUDED from `items`/`uncovered` solely because the queue is
    /// currently paused (see the module-level "Excluded: paused queues"
    /// doc). Sorted, deduped across shards. A queue in this list would be
    /// reported uncovered the moment it is unpaused without gaining a live
    /// poller. Empty when nothing pending is both paused and pollerless.
    pub excluded_paused_queues: Vec<String>,
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

    let futures = expected_shards
        .iter()
        .map(|shard_id| {
            let pool = pools.get(shard_id).cloned();
            observe_shard(*shard_id, pool, filter.clone(), worker_stale_threshold)
        })
        .collect::<Vec<_>>();
    let results = join_all(futures).await;
    let (observations, paused_uncovered_sets): (Vec<_>, Vec<_>) = results.into_iter().unzip();

    let mut report = build_report_from_observations(observed_at, filter, observations);
    report.excluded_paused_queues = merge_excluded_paused_queues(paused_uncovered_sets);
    report
}

/// Merge per-shard "paused and would-be-uncovered" queue-name sets into one
/// sorted, deduped list for [`QueueCoverageReport::excluded_paused_queues`].
fn merge_excluded_paused_queues(sets: Vec<BTreeSet<String>>) -> Vec<String> {
    sets.into_iter()
        .flatten()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// Whether `worker` is a live poller of `queue_name` on `shard_id` for
/// coverage purposes: assigned to the shard, has a fresh heartbeat, is
/// `Active` or `Draining` (not `Stopped`), and lists the queue.
///
/// Shard-membership (including the empty-`shard_assignments` legacy-worker
/// special case) is delegated to the shared
/// [`shard_fanout::worker_covers_shard`] predicate -- see its doc comment
/// for the full rationale. That helper also backs
/// `shard_health.rs::worker_assigned_to_shard`/`worker_can_cover`, which had
/// the identical gap (issue #1150, discovered as a duplicate of this
/// function's own bug while implementing #774) -- factoring the predicate
/// into one shared function means the two consumers cannot drift apart
/// again.
pub(crate) fn worker_covers_queue(worker: &WorkerRow, queue_name: &str, shard_id: i32) -> bool {
    let is_live = worker.health == WorkerHealth::Healthy
        && (worker.worker.status == WorkerStatus::Active.as_str()
            || worker.worker.status == WorkerStatus::Draining.as_str());
    let has_queue = worker.worker.queues.as_array().is_some_and(|queues| {
        queues
            .iter()
            .any(|value| value.as_str() == Some(queue_name))
    });
    is_live && shard_fanout::worker_covers_shard(worker, shard_id) && has_queue
}

/// Load only the workers that can possibly count as coverage on this shard's
/// own connection: `status IN (Active, Draining)` **and** a fresh heartbeat,
/// both applied server-side, in a single query.
///
/// `worker_covers_queue` only ever treats an `Active`/`Draining` worker with
/// a fresh heartbeat as live -- a `Stopped` row, or a crashed-but-still-
/// `Active` row (nothing ever calls `transition_status` on a dead process;
/// only its heartbeat goes stale) can never contribute coverage. A prior
/// revision reused the shared, general-purpose `list_workers`/`WorkerFilters`
/// surface, which applies only `status` in SQL and filters heartbeat
/// freshness (`health`) client-side, *after* loading every status-matching
/// row (`autumn_harvest::workers::apply_worker_filters`) -- so once the
/// artificial `WorkerFilters::MAX_LIMIT` cap that bug caused was removed
/// (see the git history for that round), this call had **no** bound at all
/// on how many crashed-but-`Active`/`Draining` rows it deserialized on every
/// coverage check that finds even one pending task, on a long-lived
/// deployment that never cleanly shuts down every worker (issue #774
/// review, fourth round).
///
/// This bypasses `list_workers` entirely and issues a purpose-built
/// existence query instead: the heartbeat cutoff
/// (`Utc::now() - worker_stale_threshold`) is computed once, in Rust, then
/// bound as a plain timestamp comparison alongside the `status IN (...)`
/// filter -- so Postgres itself excludes every crashed/stale row before a
/// single byte crosses the wire, and there is no need for (and no risk of
/// re-truncating past) any row-count limit at all. `.ge(cutoff)` matches
/// `WorkerHealth::classify`'s own boundary exactly (`elapsed <=
/// stale_threshold` is Healthy, i.e. `last_heartbeat_at >= now -
/// stale_threshold`), so a row that passes this SQL filter is *guaranteed*
/// to classify as `Healthy` a moment later -- the trailing `WorkerHealth::classify`
/// call below is therefore a cheap, defense-in-depth confirmation of an
/// already-SQL-filtered set, not a second unbounded pass over stale rows.
pub(crate) async fn fetch_potentially_live_workers(
    conn: &mut AsyncPgConnection,
    worker_stale_threshold: Duration,
) -> HarvestResult<Vec<WorkerRow>> {
    use autumn_harvest::models::HarvestWorker;
    use autumn_harvest::schema::harvest_workers;
    use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
    use diesel_async::RunQueryDsl;

    let stale_secs = i64::try_from(worker_stale_threshold.as_secs()).unwrap_or(i64::MAX);
    let cutoff = Utc::now() - chrono::Duration::seconds(stale_secs);

    let rows: Vec<HarvestWorker> = harvest_workers::table
        .select(HarvestWorker::as_select())
        .filter(harvest_workers::status.eq_any([
            WorkerStatus::Active.as_str(),
            WorkerStatus::Draining.as_str(),
        ]))
        .filter(harvest_workers::last_heartbeat_at.ge(cutoff))
        .load(conn)
        .await
        .map_err(autumn_harvest::error::database_error)?;

    Ok(rows
        .into_iter()
        .map(|w| {
            let health = WorkerHealth::classify(w.last_heartbeat_at, worker_stale_threshold);
            WorkerRow {
                worker: w,
                health,
                active_task_ids: Vec::new(),
            }
        })
        .filter(|w| w.health == WorkerHealth::Healthy)
        .collect())
}

/// One shard's fully-inspected observation: the uncovered-demand rows plus
/// the queue names that were excluded solely because they are paused (see
/// [`merge_excluded_paused_queues`]). On any error path the second element
/// is empty, exactly like `rows` — coverage cannot be determined without a
/// successful read, so nothing is asserted either way.
async fn observe_shard(
    shard_id: i32,
    pool: Option<DbPool>,
    filter: Option<String>,
    worker_stale_threshold: Duration,
) -> (ShardObservation<UncoveredQueueDemand>, BTreeSet<String>) {
    let Some(pool) = pool else {
        return (
            ShardObservation {
                shard_id,
                rows: Vec::new(),
                error: Some(format!("shard {shard_id} has no configured storage pool")),
            },
            BTreeSet::new(),
        );
    };
    let Ok(mut conn) = pool.get().await else {
        return (
            ShardObservation {
                shard_id,
                rows: Vec::new(),
                error: Some(format!(
                    "database connection for shard {shard_id} could not be acquired"
                )),
            },
            BTreeSet::new(),
        );
    };

    let pending: Vec<PendingQueueDemand> =
        match queue::pending_queue_demand_by_queue_name(&mut conn, filter.as_deref()).await {
            Ok(rows) => rows,
            Err(error) => {
                return (
                    ShardObservation {
                        shard_id,
                        rows: Vec::new(),
                        error: Some(error.to_string()),
                    },
                    BTreeSet::new(),
                );
            }
        };
    // Nothing pending on this shard (optionally narrowed by `filter`) —
    // skip the worker/pause reads entirely; there is nothing to report.
    if pending.is_empty() {
        return (
            ShardObservation {
                shard_id,
                rows: Vec::new(),
                error: None,
            },
            BTreeSet::new(),
        );
    }

    let workers = match fetch_potentially_live_workers(&mut conn, worker_stale_threshold).await {
        Ok(workers) => workers,
        Err(error) => {
            return (
                ShardObservation {
                    shard_id,
                    rows: Vec::new(),
                    error: Some(error.to_string()),
                },
                BTreeSet::new(),
            );
        }
    };

    let paused: BTreeSet<String> = match queue_pause::paused_queue_names(&mut conn).await {
        Ok(names) => names.into_iter().collect(),
        Err(error) => {
            return (
                ShardObservation {
                    shard_id,
                    rows: Vec::new(),
                    error: Some(error.to_string()),
                },
                BTreeSet::new(),
            );
        }
    };

    let (rows, paused_uncovered) =
        partition_uncovered_and_paused(pending, &workers, &paused, shard_id);

    (
        ShardObservation {
            shard_id,
            rows,
            error: None,
        },
        paused_uncovered,
    )
}

/// Partitions this shard's `pending` demand into the genuinely-uncovered
/// rows and the set of queue names that were excluded solely because they
/// are currently paused (see [`merge_excluded_paused_queues`]).
fn partition_uncovered_and_paused(
    pending: Vec<PendingQueueDemand>,
    workers: &[WorkerRow],
    paused: &BTreeSet<String>,
    shard_id: i32,
) -> (Vec<UncoveredQueueDemand>, BTreeSet<String>) {
    let mut paused_uncovered = BTreeSet::new();
    let rows = pending
        .into_iter()
        .filter_map(|demand| {
            let has_coverage = workers
                .iter()
                .any(|worker| worker_covers_queue(worker, &demand.queue_name, shard_id));
            if paused.contains(&demand.queue_name) {
                if !has_coverage {
                    paused_uncovered.insert(demand.queue_name);
                }
                return None;
            }
            if has_coverage {
                return None;
            }
            Some(UncoveredQueueDemand {
                queue_name: demand.queue_name,
                pending_count: demand.pending_count,
                sample_task_ids: demand.sample_task_ids,
                sample_execution_ids: demand.sample_execution_ids,
            })
        })
        .collect();
    (rows, paused_uncovered)
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
        excluded_paused_queues: Vec::new(),
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
    fn legacy_worker_with_empty_shard_assignments_covers_shard_zero() {
        // A `Worker::run` legacy single-shard worker (no explicit
        // `shard_assignments` configured) registers with a literal `[]`, but
        // its poll loop falls back to the default pool and claims work
        // against whatever database that pool points at -- this coverage
        // check must treat that the same way, or every queue a perfectly
        // healthy legacy worker actually polls is falsely reported
        // uncovered.
        let worker = worker_row(
            WorkerStatus::Active,
            WorkerHealth::Healthy,
            &[],
            &["orphan_queue"],
        );
        assert!(worker_covers_queue(&worker, "orphan_queue", 0));
    }

    #[test]
    fn legacy_worker_with_empty_shard_assignments_covers_a_nonzero_shard_too() {
        // The empty-array normalization must NOT be hardcoded to shard 0:
        // `worker_covers_queue` is only ever called by `observe_shard` with a
        // worker/shard_id pair already scoped to that one shard's own
        // connection, so an empty-array worker found there covers whatever
        // `shard_id` is being evaluated -- including a deployment whose
        // single/default shard is numbered something other than 0 (issue
        // #774 review). A worker row is identical regardless of which shard
        // it was fetched from; only the `shard_id` argument -- standing in
        // for which shard's connection produced this row -- changes here.
        let worker = worker_row(
            WorkerStatus::Active,
            WorkerHealth::Healthy,
            &[],
            &["orphan_queue"],
        );
        assert!(worker_covers_queue(&worker, "orphan_queue", 1));
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

    // ── AC2: coverage means a poller exists, not that capacity is free ──

    fn worker_row_saturated(
        status: WorkerStatus,
        health: WorkerHealth,
        shard_assignments: &[i64],
        queues: &[&str],
    ) -> WorkerRow {
        let mut worker = worker_row(status, health, shard_assignments, queues);
        worker.worker.max_concurrency = 3;
        worker.worker.in_flight_count = 3;
        worker
    }

    #[test]
    fn saturated_worker_still_covers_ac2() {
        // AC2: "coverage means a poller exists, not capacity is available".
        // A worker fully saturated at max_concurrency must still count as
        // a live poller — utilization is #531/#742's job, not this check's.
        let worker = worker_row_saturated(
            WorkerStatus::Active,
            WorkerHealth::Healthy,
            &[0],
            &["orphan_queue"],
        );
        assert_eq!(worker.worker.in_flight_count, worker.worker.max_concurrency);
        assert!(worker_covers_queue(&worker, "orphan_queue", 0));
    }

    // ── AC5: coverage is orthogonal to build-id compatibility (#171) ────

    fn worker_row_with_build(
        status: WorkerStatus,
        health: WorkerHealth,
        shard_assignments: &[i64],
        queues: &[&str],
        build_id: &str,
    ) -> WorkerRow {
        let mut worker = worker_row(status, health, shard_assignments, queues);
        worker.worker.build_id = build_id.to_string();
        worker
    }

    #[test]
    fn build_incompatible_worker_still_covers_ac5() {
        // AC5: this check answers "does any live worker poll this queue at
        // all", never "...with a build compatible with the pending task's
        // required_build_id" — that distinction belongs to #171's
        // `build_reachability`. `worker_covers_queue` must never consult
        // `build_id`, so an arbitrary/incompatible build still covers.
        let worker = worker_row_with_build(
            WorkerStatus::Active,
            WorkerHealth::Healthy,
            &[0],
            &["orphan_queue"],
            "some-incompatible-build-sha",
        );
        assert!(worker_covers_queue(&worker, "orphan_queue", 0));
    }

    // ── excluded_paused_queues: partition_uncovered_and_paused + merge ──

    fn pending_demand(queue_name: &str, pending_count: i64) -> PendingQueueDemand {
        PendingQueueDemand {
            queue_name: queue_name.to_string(),
            pending_count,
            sample_task_ids: Vec::new(),
            sample_execution_ids: Vec::new(),
        }
    }

    #[test]
    fn partition_excludes_paused_queues_from_uncovered_rows() {
        let pending = vec![
            pending_demand("paused_q", 5),
            pending_demand("uncovered_q", 3),
        ];
        let (rows, paused_uncovered) =
            partition_uncovered_and_paused(pending, &[], &["paused_q".to_string()].into(), 0);

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].queue_name, "uncovered_q");
        assert_eq!(
            paused_uncovered,
            BTreeSet::from(["paused_q".to_string()]),
            "a paused, pollerless queue must be recorded in the side-channel"
        );
    }

    #[test]
    fn partition_paused_and_covered_queue_is_reported_nowhere() {
        // A paused queue that already has a live poller has nothing
        // interesting to report — it would not have been uncovered even if
        // it were unpaused right now.
        let pending = vec![pending_demand("paused_covered_q", 5)];
        let worker = worker_row(
            WorkerStatus::Active,
            WorkerHealth::Healthy,
            &[0],
            &["paused_covered_q"],
        );
        let (rows, paused_uncovered) = partition_uncovered_and_paused(
            pending,
            &[worker],
            &["paused_covered_q".to_string()].into(),
            0,
        );

        assert!(rows.is_empty());
        assert!(
            paused_uncovered.is_empty(),
            "a paused-but-covered queue must not appear in excluded_paused_queues"
        );
    }

    #[test]
    fn partition_unpaused_and_covered_queue_is_reported_nowhere() {
        let pending = vec![pending_demand("covered_q", 5)];
        let worker = worker_row(
            WorkerStatus::Active,
            WorkerHealth::Healthy,
            &[0],
            &["covered_q"],
        );
        let (rows, paused_uncovered) =
            partition_uncovered_and_paused(pending, &[worker], &BTreeSet::new(), 0);

        assert!(rows.is_empty());
        assert!(paused_uncovered.is_empty());
    }

    #[test]
    fn merge_excluded_paused_queues_dedups_and_sorts_across_shards() {
        let shard0: BTreeSet<String> = ["zeta_paused", "alpha_paused"]
            .into_iter()
            .map(String::from)
            .collect();
        let shard1: BTreeSet<String> = ["alpha_paused", "beta_paused"]
            .into_iter()
            .map(String::from)
            .collect();

        let merged = merge_excluded_paused_queues(vec![shard0, shard1]);

        assert_eq!(merged, vec!["alpha_paused", "beta_paused", "zeta_paused"]);
    }

    #[test]
    fn merge_excluded_paused_queues_empty_input_is_empty() {
        assert!(merge_excluded_paused_queues(Vec::new()).is_empty());
        assert!(merge_excluded_paused_queues(vec![BTreeSet::new()]).is_empty());
    }

    // ── QueueCoverageQuery::from_query_pairs (AC8) ──────────────────────

    #[test]
    fn from_query_pairs_empty_input_is_default() {
        let query = QueueCoverageQuery::from_query_pairs(&[]);
        assert_eq!(query.queue_name, None);
    }

    #[test]
    fn from_query_pairs_ignores_unknown_keys() {
        // Unknown keys are ignored rather than rejected, matching every
        // other `from_query_pairs` implementation in this codebase — never
        // the derive-extractor's blanket "unknown field" 400.
        let pairs = vec![("bogus_param".to_string(), "value".to_string())];
        let query = QueueCoverageQuery::from_query_pairs(&pairs);
        assert_eq!(query.queue_name, None);
    }

    #[test]
    fn from_query_pairs_duplicate_key_last_value_wins() {
        // A repeated `queue_name` resolves last-value-wins, never a 400 —
        // this is the direct fix for the pre-hardening AC8 regression
        // where the derive-based extractor rejected a duplicate key with a
        // `text/plain` body instead of the required JSON error shape.
        let pairs = vec![
            ("queue_name".to_string(), "first".to_string()),
            ("queue_name".to_string(), "second".to_string()),
        ];
        let query = QueueCoverageQuery::from_query_pairs(&pairs);
        assert_eq!(query.queue_name.as_deref(), Some("second"));
    }

    #[test]
    fn from_query_pairs_empty_string_value_normalizes_to_no_filter() {
        // Only the literal empty string means "no filter" -- see the
        // exact-match rationale on `from_query_pairs` itself.
        let pairs = vec![("queue_name".to_string(), String::new())];
        let query = QueueCoverageQuery::from_query_pairs(&pairs);
        assert_eq!(query.queue_name, None);
    }

    #[test]
    fn from_query_pairs_whitespace_only_value_is_preserved_as_exact_filter() {
        // A whitespace-only value is NOT normalized away -- `with_queues`
        // permits a queue name that is entirely whitespace (it only rejects
        // the exactly-empty string), so trimming/dropping this filter would
        // make it impossible to query coverage for such a queue, and would
        // silently widen the filter to "match every queue" instead.
        let pairs = vec![("queue_name".to_string(), "   ".to_string())];
        let query = QueueCoverageQuery::from_query_pairs(&pairs);
        assert_eq!(query.queue_name.as_deref(), Some("   "));
    }

    #[test]
    fn from_query_pairs_preserves_surrounding_whitespace() {
        // Surrounding whitespace is preserved verbatim, not trimmed -- a
        // queue registered as e.g. " email-workers " (unusual but valid
        // under `with_queues`) must be filterable by its exact name.
        let pairs = vec![("queue_name".to_string(), "  email-workers  ".to_string())];
        let query = QueueCoverageQuery::from_query_pairs(&pairs);
        assert_eq!(query.queue_name.as_deref(), Some("  email-workers  "));
    }

    // ── parse_raw_query_pairs_strict (issue #774 review) ────────────────

    #[test]
    fn parse_raw_query_pairs_strict_empty_input_is_empty_pairs() {
        assert_eq!(parse_raw_query_pairs_strict(""), Ok(Vec::new()));
    }

    #[test]
    fn parse_raw_query_pairs_strict_decodes_normal_pairs() {
        assert_eq!(
            parse_raw_query_pairs_strict("queue_name=email"),
            Ok(vec![("queue_name".to_string(), "email".to_string())])
        );
    }

    #[test]
    fn parse_raw_query_pairs_strict_decodes_percent_encoded_values() {
        // %20 must decode to a literal space, matching form_urlencoded.
        assert_eq!(
            parse_raw_query_pairs_strict("queue_name=hello%20world"),
            Ok(vec![("queue_name".to_string(), "hello world".to_string())])
        );
    }

    #[test]
    fn parse_raw_query_pairs_strict_decodes_plus_as_space() {
        // application/x-www-form-urlencoded: unescaped `+` means space.
        assert_eq!(
            parse_raw_query_pairs_strict("queue_name=a+b"),
            Ok(vec![("queue_name".to_string(), "a b".to_string())])
        );
    }

    #[test]
    fn parse_raw_query_pairs_strict_a_literal_plus_is_encoded_as_percent_2b() {
        // %2B is a percent-encoded literal '+', distinct from bare '+'
        // (which means space) -- the two must decode differently.
        assert_eq!(
            parse_raw_query_pairs_strict("queue_name=a%2Bb"),
            Ok(vec![("queue_name".to_string(), "a+b".to_string())])
        );
    }

    #[test]
    fn parse_raw_query_pairs_strict_rejects_invalid_utf8_in_value() {
        // 0xFF is never a valid standalone UTF-8 byte -- this is the exact
        // review-flagged repro (`?queue_name=%FF`).
        assert_eq!(
            parse_raw_query_pairs_strict("queue_name=%FF"),
            Err(InvalidQueryEncoding)
        );
    }

    #[test]
    fn parse_raw_query_pairs_strict_rejects_invalid_utf8_in_key() {
        assert_eq!(
            parse_raw_query_pairs_strict("%FF=value"),
            Err(InvalidQueryEncoding)
        );
    }

    #[test]
    fn parse_raw_query_pairs_strict_rejects_a_lone_trailing_percent() {
        // `%` with nothing after it -- percent_decode_str leaves it as a
        // literal `%` (still valid UTF-8 on its own), so only an explicit
        // hex-escape well-formedness check catches this.
        assert_eq!(
            parse_raw_query_pairs_strict("queue_name=orders%"),
            Err(InvalidQueryEncoding)
        );
    }

    #[test]
    fn parse_raw_query_pairs_strict_rejects_a_percent_with_one_hex_digit() {
        assert_eq!(
            parse_raw_query_pairs_strict("queue_name=orders%2"),
            Err(InvalidQueryEncoding)
        );
    }

    #[test]
    fn parse_raw_query_pairs_strict_rejects_non_hex_percent_escape() {
        // `%GG` -- the exact review-flagged repro. `G` is not a hex digit,
        // so percent_decode_str leaves `%GG` undecoded rather than erroring;
        // the caller must not silently query the literal "orders%GG".
        assert_eq!(
            parse_raw_query_pairs_strict("queue_name=orders%GG"),
            Err(InvalidQueryEncoding)
        );
    }

    #[test]
    fn parse_raw_query_pairs_strict_rejects_non_hex_percent_escape_in_key() {
        assert_eq!(
            parse_raw_query_pairs_strict("queue%ZZname=value"),
            Err(InvalidQueryEncoding)
        );
    }

    #[test]
    fn parse_raw_query_pairs_strict_accepts_well_formed_lowercase_hex_escape() {
        // Lowercase hex digits are just as well-formed as uppercase.
        assert_eq!(
            parse_raw_query_pairs_strict("queue_name=hello%2fworld"),
            Ok(vec![("queue_name".to_string(), "hello/world".to_string())])
        );
    }

    #[test]
    fn parse_raw_query_pairs_strict_skips_empty_segments() {
        // A leading/trailing/doubled `&` must not produce a spurious
        // empty-key pair.
        assert_eq!(
            parse_raw_query_pairs_strict("&a=1&&b=2&"),
            Ok(vec![
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "2".to_string()),
            ])
        );
    }

    #[test]
    fn parse_raw_query_pairs_strict_key_without_equals_has_empty_value() {
        assert_eq!(
            parse_raw_query_pairs_strict("queue_name"),
            Ok(vec![("queue_name".to_string(), String::new())])
        );
    }

    #[test]
    fn parse_raw_query_pairs_strict_preserves_percent_encoded_whitespace_padding() {
        // Regression tie-in to the whitespace-preservation fix above: a
        // caller who genuinely percent-encodes surrounding whitespace must
        // still get it back verbatim through the strict decoder.
        assert_eq!(
            parse_raw_query_pairs_strict("queue_name=%20email%20"),
            Ok(vec![("queue_name".to_string(), " email ".to_string())])
        );
    }

    #[test]
    fn parse_raw_query_pairs_strict_duplicate_and_unknown_keys_survive_to_from_query_pairs() {
        // The strict decoder itself must not reject a duplicate/unknown
        // key -- that's `QueueCoverageQuery::from_query_pairs`'s job (last-
        // value-wins / ignore), matching `duplicate_and_unknown_query_params_do_not_400`.
        let pairs = parse_raw_query_pairs_strict(
            "queue_name=typo_queue_a&queue_name=typo_queue_b&unknown_param=1",
        )
        .expect("well-formed UTF-8 pairs must parse");
        let query = QueueCoverageQuery::from_query_pairs(&pairs);
        assert_eq!(query.queue_name.as_deref(), Some("typo_queue_b"));
    }
}
