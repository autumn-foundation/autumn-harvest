//! Built-in Prometheus scrape endpoint for `HarvestPlugin` (issue #355).
//!
//! [`HarvestMetricsRecorder`] is an in-process aggregator that implements
//! both `autumn_harvest::telemetry::MetricsRecorder` (so the core engine can
//! record samples into it) and `autumn_web::actuator::MetricsSource` (so
//! autumn-web's already-shared `/actuator/prometheus` endpoint can render
//! those samples alongside the app's own `autumn_http_*` families and any
//! other plugin's metrics).
//!
//! This deliberately does **not** touch the global `metrics`-crate registry
//! (unlike the `metrics-rs` adapter/escape hatch documented in
//! `docs/telemetry.md`) — there is nothing to double-register, and no new
//! dependency is pulled in. `HarvestPlugin::with_metrics_scrape()` wires one
//! shared instance into both the core `HarvestBuilder::telemetry(..)` slot
//! and `AppBuilder::metrics_source("harvest", ..)`.
//!
//! This aggregates the nine ADR-0001 §7 catalogue metrics named in issue
//! #355, plus `harvest.queue.oldest_pending_age`, `harvest.worker.slots_*`,
//! `harvest.shard.stranded_pending`, and `harvest.schedule.overdue` (issue
//! #696) — the engine's own background samplers already compute these under the
//! same `MetricsRecorder::is_enabled()`
//! gate (see `HarvestMetricsRecorder::is_enabled`, which reports `true`
//! unconditionally so the nine required metrics are actually sampled), so
//! leaving them unimplemented would mean the sampler's DB queries still ran
//! on every tick with their results silently discarded. It also aggregates the
//! four broker-connector families (issue #944), which are not sampler-adjacent
//! but do back shipped dashboard panels — leaving those to the no-op default
//! would make a dropped metric indistinguishable from an idle consumer. Every
//! other `MetricsRecorder` method keeps the trait's no-op default — an embedder
//! who needs the full metric surface (e.g. `harvest.workflow.terminal`,
//! `harvest.activity.attempts`/`.retries`, `harvest.schedule.fire_attempts`,
//! and the rest of the starter alert pack in `docs/alerts/`) or OTLP export
//! still reaches for the `metrics-rs` adapter escape hatch — this endpoint
//! does **not** back the full starter alert pack.
//!
//! A `MetricFamily` is only emitted once at least one sample has been
//! recorded for it — there is no way to synthesize a meaningful zero-value
//! default for a labeled series before its label values are known.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use autumn_harvest::telemetry::{
    ActivityStatus, ConnectorOutcome, METRIC_LABEL_ACTIVITY, METRIC_LABEL_KIND, METRIC_LABEL_NAME,
    METRIC_LABEL_OUTCOME, METRIC_LABEL_QUEUE, METRIC_LABEL_REASON, METRIC_LABEL_SHARD,
    METRIC_LABEL_SLOT_TYPE, METRIC_LABEL_SOURCE, METRIC_LABEL_STATUS, METRIC_LABEL_WORKFLOW,
    MetricsRecorder, PoisonReason, SlotType, WorkflowStatus,
};
use autumn_web::actuator::{MetricFamily, MetricKind, MetricSample, MetricsSource};

/// Label values keyed to a stable position; label *names* are supplied by
/// the caller at render time (see [`push_counter`]/[`push_gauge`]/[`push_histogram`]),
/// since every call site always records labels in the same declared order.
type LabelValues = Vec<String>;

#[derive(Default)]
struct Counter(RwLock<HashMap<LabelValues, u64>>);

impl Counter {
    fn incr(&self, labels: LabelValues, delta: u64) {
        let mut map = self
            .0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *map.entry(labels).or_insert(0) += delta;
    }

    fn snapshot(&self) -> Vec<(LabelValues, u64)> {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }
}

#[derive(Default)]
struct Gauge(RwLock<HashMap<LabelValues, f64>>);

impl Gauge {
    fn set(&self, labels: LabelValues, value: f64) {
        let mut map = self
            .0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        map.insert(labels, value);
    }

    fn snapshot(&self) -> Vec<(LabelValues, f64)> {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }
}

/// `(count, sum)` per label combination — the Prometheus histogram/summary
/// decomposition without bucket boundaries. `autumn_web::actuator::MetricKind`
/// only supports `Counter`/`Gauge` (no `Histogram` variant), so a duration
/// metric is rendered as a `_count` and a `_sum` counter pair; a caller can
/// still chart an average via `rate(x_sum[5m]) / rate(x_count[5m])`. Full
/// bucketed histograms remain available via the `metrics-rs` adapter escape
/// hatch documented in `docs/telemetry.md`.
#[derive(Default)]
struct Histogram(RwLock<HashMap<LabelValues, (u64, f64)>>);

impl Histogram {
    // The write guard is held only for the two-statement bump below (bounded,
    // uncontended in-process work); the lint's general "hold locks briefly"
    // advice doesn't apply cleanly to `HashMap::entry`'s borrow shape here.
    #[allow(clippy::significant_drop_tightening)]
    fn observe(&self, labels: LabelValues, value: f64) {
        let mut map = self
            .0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = map.entry(labels).or_insert((0, 0.0));
        entry.0 += 1;
        entry.1 += value;
    }

    fn snapshot(&self) -> Vec<(LabelValues, (u64, f64))> {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }
}

#[derive(Default)]
struct Inner {
    workflow_started: Counter,
    workflow_duration: Histogram,
    activity_duration: Histogram,
    timer_started: Counter,
    queue_depth: Gauge,
    dlq_entries: Gauge,
    schedule_runs: Counter,
    schedule_skipped: Counter,
    retention_deleted: Counter,
    // These four back-fill metrics that the engine's own is_enabled()-gated
    // background samplers already compute (queue-depth sampler's oldest-age
    // query, the stranded-work scanner, the worker-slot sampler): once
    // `.with_metrics_scrape()` flips `is_enabled()` to true, those samplers
    // run regardless of whether this recorder was going to use the result.
    // Implementing them turns already-paid-for sampler work into real
    // series instead of a discarded read.
    queue_oldest_pending_age: Gauge,
    worker_slots_in_use: Gauge,
    worker_slots_available: Gauge,
    worker_slot_target: Gauge,
    shard_stranded_pending: Gauge,
    // The #696 overdue-schedule sampler runs under the same is_enabled() gate;
    // render its primary gauge here so the recommended built-in scrape path (not
    // just the metrics-rs adapter) exposes `harvest_schedule_overdue`.
    schedule_overdue: Gauge,
    // Broker connectors (issue #944). Not sampler-adjacent -- these are emitted
    // straight from the connector receive loop -- but this recorder is
    // per-metric hand-maintained, so leaving them to the trait no-op would make
    // `.with_metrics_scrape()` silently discard every connector sample while
    // the shipped dashboard panels stayed flat. An idle consumer and a dropped
    // metric would then look identical.
    connector_received: Counter,
    connector_dispatched: Counter,
    connector_poisoned: Counter,
    connector_lag: Gauge,
}

/// In-process aggregator for the built-in Prometheus scrape endpoint
/// (issue #355) — see the module docs above for exactly which metrics are
/// covered.
///
/// Implements both `MetricsRecorder` (recording side, installed via
/// `HarvestBuilder::telemetry`) and `MetricsSource` (rendering side,
/// registered via `AppBuilder::metrics_source`) over one shared
/// `Arc<Inner>`, so `.clone()` is cheap and every clone observes the same
/// underlying counters/gauges/histograms.
#[derive(Clone, Default)]
pub struct HarvestMetricsRecorder(Arc<Inner>);

impl HarvestMetricsRecorder {
    /// Create a fresh, empty aggregator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl MetricsRecorder for HarvestMetricsRecorder {
    // Must be `true` for the engine's own is_enabled()-gated background
    // samplers (queue depth, DLQ depth, worker slots, stranded-work) to run
    // at all -- the required nine catalogue metrics live behind those same
    // samplers. Every metric those samplers compute is implemented below so
    // none of that gated work goes to waste.
    fn is_enabled(&self) -> bool {
        true
    }

    fn record_workflow_started(&self, workflow_name: &str, queue: &str) {
        self.0
            .workflow_started
            .incr(vec![workflow_name.to_owned(), queue.to_owned()], 1);
    }

    fn record_workflow_completed(
        &self,
        workflow_name: &str,
        queue: &str,
        duration_secs: f64,
        status: WorkflowStatus,
    ) {
        self.0.workflow_duration.observe(
            vec![
                workflow_name.to_owned(),
                queue.to_owned(),
                status.as_str().to_owned(),
            ],
            duration_secs,
        );
    }

    fn record_activity_completed(
        &self,
        activity_name: &str,
        queue: &str,
        duration_secs: f64,
        status: ActivityStatus,
    ) {
        self.0.activity_duration.observe(
            vec![
                activity_name.to_owned(),
                queue.to_owned(),
                status.as_str().to_owned(),
            ],
            duration_secs,
        );
    }

    fn record_timer_started(&self, _duration_secs: f64) {
        self.0.timer_started.incr(Vec::new(), 1);
    }

    #[allow(clippy::cast_precision_loss)]
    fn record_queue_depth(&self, queue_name: &str, depth: u64) {
        self.0
            .queue_depth
            .set(vec![queue_name.to_owned()], depth as f64);
    }

    #[allow(clippy::cast_precision_loss)]
    fn record_dlq_entries(&self, shard: u16, depth: u64) {
        self.0
            .dlq_entries
            .set(vec![shard.to_string()], depth as f64);
    }

    fn record_schedule_run(&self, kind: &str, name: &str) {
        self.0
            .schedule_runs
            .incr(vec![kind.to_owned(), name.to_owned()], 1);
    }

    fn record_schedule_skipped(&self, kind: &str, name: &str, reason: &str) {
        self.0
            .schedule_skipped
            .incr(vec![kind.to_owned(), name.to_owned(), reason.to_owned()], 1);
    }

    fn record_schedule_skipped_n(&self, kind: &str, name: &str, reason: &str, count: u64) {
        self.0.schedule_skipped.incr(
            vec![kind.to_owned(), name.to_owned(), reason.to_owned()],
            count,
        );
    }

    fn record_retention_tick(
        &self,
        shard: u16,
        _candidate_count: u64,
        _deleted_count: u64,
        _duration_secs: f64,
    ) {
        // The `harvest.retention.deleted` counter is now owned by
        // `record_retention_deleted`, which carries a per-workflow-type label
        // (issue #737). Emitting it here too would double-count. Candidate
        // count and duration have no scrape metric today; the per-shard tick
        // observability point is kept as a no-op.
        let _ = shard;
    }

    fn record_retention_deleted(&self, workflow: &str, count: u64) {
        self.0
            .retention_deleted
            .incr(vec![workflow.to_owned()], count);
    }

    fn record_queue_oldest_pending_age(&self, queue_name: &str, age_secs: f64) {
        self.0
            .queue_oldest_pending_age
            .set(vec![queue_name.to_owned()], age_secs);
    }

    #[allow(clippy::cast_precision_loss)]
    fn record_worker_slots(&self, slot_type: SlotType, in_use: u64, available: u64) {
        self.0
            .worker_slots_in_use
            .set(vec![slot_type.as_str().to_owned()], in_use as f64);
        self.0
            .worker_slots_available
            .set(vec![slot_type.as_str().to_owned()], available as f64);
    }

    #[allow(clippy::cast_precision_loss)]
    fn record_worker_slot_target(&self, slot_type: SlotType, target: u64) {
        self.0
            .worker_slot_target
            .set(vec![slot_type.as_str().to_owned()], target as f64);
    }

    #[allow(clippy::cast_precision_loss)]
    fn record_shard_stranded_pending(&self, shard: u16, count: u64) {
        self.0
            .shard_stranded_pending
            .set(vec![shard.to_string()], count as f64);
    }

    fn record_schedule_overdue(&self, kind: &str, name: &str, overdue: bool) {
        // Last-write-wins gauge per (kind, name): 1.0 when the schedule is overdue
        // relative to its own cadence, 0.0 when healthy (issue #696). The #696
        // sampler already ran under the is_enabled() gate to compute this; without
        // this override the value would fall through to the trait no-op and the
        // primary overdue gauge would be absent from the built-in scrape endpoint.
        self.0.schedule_overdue.set(
            vec![kind.to_owned(), name.to_owned()],
            f64::from(u8::from(overdue)),
        );
    }

    fn record_connector_received(&self, source: &str) {
        self.0.connector_received.incr(vec![source.to_owned()], 1);
    }

    fn record_connector_dispatched(&self, source: &str, outcome: ConnectorOutcome) {
        self.0
            .connector_dispatched
            .incr(vec![source.to_owned(), outcome.as_str().to_owned()], 1);
    }

    fn record_connector_poisoned(&self, source: &str, reason: PoisonReason) {
        self.0
            .connector_poisoned
            .incr(vec![source.to_owned(), reason.as_str().to_owned()], 1);
    }

    #[allow(clippy::cast_precision_loss)]
    fn record_connector_lag(&self, source: &str, lag: i64) {
        // A level, not an accumulation: last-write-wins per source, so a
        // partition draining to zero reads zero instead of the sum of every
        // sample the poll loop ever took.
        self.0
            .connector_lag
            .set(vec![source.to_owned()], lag as f64);
    }
}

fn zip_labels(label_names: &[&str], values: LabelValues) -> Vec<(String, String)> {
    label_names
        .iter()
        .map(|s| (*s).to_string())
        .zip(values)
        .collect()
}

fn push_counter(
    out: &mut Vec<MetricFamily>,
    name: &str,
    help: &str,
    label_names: &[&str],
    data: Vec<(LabelValues, u64)>,
) {
    if data.is_empty() {
        return;
    }
    #[allow(clippy::cast_precision_loss)]
    let samples = data
        .into_iter()
        .map(|(vals, v)| MetricSample {
            labels: zip_labels(label_names, vals),
            value: v as f64,
        })
        .collect();
    out.push(MetricFamily {
        name: name.to_string(),
        help: help.to_string(),
        kind: MetricKind::Counter,
        samples,
    });
}

fn push_gauge(
    out: &mut Vec<MetricFamily>,
    name: &str,
    help: &str,
    label_names: &[&str],
    data: Vec<(LabelValues, f64)>,
) {
    if data.is_empty() {
        return;
    }
    let samples = data
        .into_iter()
        .map(|(vals, value)| MetricSample {
            labels: zip_labels(label_names, vals),
            value,
        })
        .collect();
    out.push(MetricFamily {
        name: name.to_string(),
        help: help.to_string(),
        kind: MetricKind::Gauge,
        samples,
    });
}

#[allow(clippy::cast_precision_loss)]
fn push_histogram(
    out: &mut Vec<MetricFamily>,
    name: &str,
    help: &str,
    label_names: &[&str],
    data: Vec<(LabelValues, (u64, f64))>,
) {
    if data.is_empty() {
        return;
    }
    let mut count_samples = Vec::with_capacity(data.len());
    let mut sum_samples = Vec::with_capacity(data.len());
    for (vals, (count, sum)) in data {
        let labels = zip_labels(label_names, vals);
        count_samples.push(MetricSample {
            labels: labels.clone(),
            value: count as f64,
        });
        sum_samples.push(MetricSample { labels, value: sum });
    }
    out.push(MetricFamily {
        name: format!("{name}_count"),
        help: format!("{help} (sample count)"),
        kind: MetricKind::Counter,
        samples: count_samples,
    });
    out.push(MetricFamily {
        name: format!("{name}_sum"),
        help: format!("{help} (sum)"),
        kind: MetricKind::Counter,
        samples: sum_samples,
    });
}

/// The nine ADR-0001 §7 catalogue metrics named in issue #355.
fn push_catalogue_metrics(families: &mut Vec<MetricFamily>, inner: &Inner) {
    push_counter(
        families,
        "harvest_workflow_started_total",
        "Total number of workflow executions started",
        &[METRIC_LABEL_WORKFLOW, METRIC_LABEL_QUEUE],
        inner.workflow_started.snapshot(),
    );
    push_histogram(
        families,
        "harvest_workflow_duration",
        "Workflow execution duration in seconds",
        &[
            METRIC_LABEL_WORKFLOW,
            METRIC_LABEL_QUEUE,
            METRIC_LABEL_STATUS,
        ],
        inner.workflow_duration.snapshot(),
    );
    push_histogram(
        families,
        "harvest_activity_duration",
        "Activity execution duration in seconds",
        &[
            METRIC_LABEL_ACTIVITY,
            METRIC_LABEL_QUEUE,
            METRIC_LABEL_STATUS,
        ],
        inner.activity_duration.snapshot(),
    );
    push_counter(
        families,
        "harvest_timer_started_total",
        "Total number of durable timers started",
        &[],
        inner.timer_started.snapshot(),
    );
    push_gauge(
        families,
        "harvest_queue_depth",
        "Current pending task count per queue",
        &[METRIC_LABEL_QUEUE],
        inner.queue_depth.snapshot(),
    );
    push_gauge(
        families,
        "harvest_dlq_entries",
        "Current dead-letter queue entry count per shard",
        &[METRIC_LABEL_SHARD],
        inner.dlq_entries.snapshot(),
    );
    push_counter(
        families,
        "harvest_schedule_runs_total",
        "Total number of schedule firings",
        &[METRIC_LABEL_KIND, METRIC_LABEL_NAME],
        inner.schedule_runs.snapshot(),
    );
    push_counter(
        families,
        "harvest_schedule_skipped_total",
        "Total number of skipped schedule firings",
        &[METRIC_LABEL_KIND, METRIC_LABEL_NAME, METRIC_LABEL_REASON],
        inner.schedule_skipped.snapshot(),
    );
    push_counter(
        families,
        "harvest_retention_deleted_total",
        "Total number of records deleted by the retention sweep, per workflow type",
        &[METRIC_LABEL_WORKFLOW],
        inner.retention_deleted.snapshot(),
    );
}

/// Metrics the engine's own `is_enabled()`-gated background samplers already
/// compute alongside the nine catalogue metrics above -- implemented so that
/// sampler work isn't silently discarded (see the module docs).
fn push_sampler_adjacent_metrics(families: &mut Vec<MetricFamily>, inner: &Inner) {
    push_gauge(
        families,
        "harvest_queue_oldest_pending_age",
        "Age in seconds of the oldest claimable pending task per queue",
        &[METRIC_LABEL_QUEUE],
        inner.queue_oldest_pending_age.snapshot(),
    );
    push_gauge(
        families,
        "harvest_worker_slots_in_use",
        "Currently occupied dispatch slots per slot type",
        &[METRIC_LABEL_SLOT_TYPE],
        inner.worker_slots_in_use.snapshot(),
    );
    push_gauge(
        families,
        "harvest_worker_slots_available",
        "Currently free dispatch slots per slot type",
        &[METRIC_LABEL_SLOT_TYPE],
        inner.worker_slots_available.snapshot(),
    );
    push_gauge(
        families,
        "harvest_worker_slot_target",
        "Adaptive slot tuner's current resize target per slot type",
        &[METRIC_LABEL_SLOT_TYPE],
        inner.worker_slot_target.snapshot(),
    );
    push_gauge(
        families,
        "harvest_shard_stranded_pending",
        "Claimable pending task demand per shard with no compatible worker",
        &[METRIC_LABEL_SHARD],
        inner.shard_stranded_pending.snapshot(),
    );
    push_gauge(
        families,
        "harvest_schedule_overdue",
        "Whether a schedule is overdue to fire relative to its own cadence (1 = overdue, 0 = healthy)",
        &[METRIC_LABEL_KIND, METRIC_LABEL_NAME],
        inner.schedule_overdue.snapshot(),
    );
}

/// Broker-connector families (issue #944).
///
/// Rendered here so the recommended `.with_metrics_scrape()` path exposes the
/// same four families the `metrics-rs` adapter does — the shipped dashboard
/// panels read these, and a missing family is indistinguishable from an idle
/// consumer.
fn push_connector_metrics(families: &mut Vec<MetricFamily>, inner: &Inner) {
    push_counter(
        families,
        "harvest_connector_received_total",
        "Total number of broker messages received by a connector, per binding",
        &[METRIC_LABEL_SOURCE],
        inner.connector_received.snapshot(),
    );
    push_counter(
        families,
        "harvest_connector_dispatched_total",
        "Total number of broker messages that reached a terminal disposition, per binding and outcome",
        &[METRIC_LABEL_SOURCE, METRIC_LABEL_OUTCOME],
        inner.connector_dispatched.snapshot(),
    );
    push_counter(
        families,
        "harvest_connector_poisoned_total",
        "Total number of broker messages dead-lettered, per binding and reason",
        &[METRIC_LABEL_SOURCE, METRIC_LABEL_REASON],
        inner.connector_poisoned.snapshot(),
    );
    push_gauge(
        families,
        "harvest_connector_lag",
        "Broker-reported work this connector still owes, per binding",
        &[METRIC_LABEL_SOURCE],
        inner.connector_lag.snapshot(),
    );
}

impl MetricsSource for HarvestMetricsRecorder {
    fn collect(&self) -> Vec<MetricFamily> {
        let mut families = Vec::new();
        push_catalogue_metrics(&mut families, &self.0);
        push_sampler_adjacent_metrics(&mut families, &self.0);
        push_connector_metrics(&mut families, &self.0);
        families
    }
}

#[cfg(test)]
// Every assertion compares an exactly-representable whole-number count/sum
// (small integers, safe well below f64's 2^53 exact-integer bound) against a
// literal -- intentional exact equality, not a precision-sensitive float
// comparison.
#[allow(clippy::float_cmp)]
mod tests {
    use super::*;

    fn family<'a>(families: &'a [MetricFamily], name: &str) -> &'a MetricFamily {
        families
            .iter()
            .find(|f| f.name == name)
            .unwrap_or_else(|| panic!("missing family {name} in {families:?}"))
    }

    fn sample_value(family: &MetricFamily, labels: &[(&str, &str)]) -> f64 {
        family
            .samples
            .iter()
            .find(|s| {
                s.labels.len() == labels.len()
                    && labels
                        .iter()
                        .all(|(k, v)| s.labels.iter().any(|(sk, sv)| sk == k && sv == v))
            })
            .unwrap_or_else(|| panic!("missing sample {labels:?} in family {family:?}"))
            .value
    }

    #[test]
    fn no_families_emitted_before_any_recording() {
        let recorder = HarvestMetricsRecorder::new();
        assert!(recorder.collect().is_empty());
    }

    #[test]
    fn workflow_started_is_a_counter_with_workflow_and_queue_labels() {
        let recorder = HarvestMetricsRecorder::new();
        recorder.record_workflow_started("onboarding", "default");
        recorder.record_workflow_started("onboarding", "default");
        recorder.record_workflow_started("billing", "priority");

        let families = recorder.collect();
        let f = family(&families, "harvest_workflow_started_total");
        assert_eq!(f.kind, MetricKind::Counter);
        assert_eq!(
            sample_value(f, &[("workflow", "onboarding"), ("queue", "default")]),
            2.0
        );
        assert_eq!(
            sample_value(f, &[("workflow", "billing"), ("queue", "priority")]),
            1.0
        );
    }

    #[test]
    fn workflow_duration_decomposes_into_count_and_sum_counters() {
        let recorder = HarvestMetricsRecorder::new();
        recorder.record_workflow_completed("onboarding", "default", 1.5, WorkflowStatus::Completed);
        recorder.record_workflow_completed("onboarding", "default", 2.5, WorkflowStatus::Completed);

        let families = recorder.collect();
        let count_f = family(&families, "harvest_workflow_duration_count");
        let sum_f = family(&families, "harvest_workflow_duration_sum");
        assert_eq!(count_f.kind, MetricKind::Counter);
        assert_eq!(sum_f.kind, MetricKind::Counter);
        let labels = [
            ("workflow", "onboarding"),
            ("queue", "default"),
            ("status", "completed"),
        ];
        assert_eq!(sample_value(count_f, &labels), 2.0);
        assert_eq!(sample_value(sum_f, &labels), 4.0);
    }

    #[test]
    fn activity_duration_decomposes_into_count_and_sum_counters() {
        let recorder = HarvestMetricsRecorder::new();
        recorder.record_activity_completed(
            "send_email",
            "email-workers",
            0.25,
            ActivityStatus::Completed,
        );

        let families = recorder.collect();
        let count_f = family(&families, "harvest_activity_duration_count");
        let sum_f = family(&families, "harvest_activity_duration_sum");
        let labels = [
            ("activity", "send_email"),
            ("queue", "email-workers"),
            ("status", "completed"),
        ];
        assert_eq!(sample_value(count_f, &labels), 1.0);
        assert_eq!(sample_value(sum_f, &labels), 0.25);
    }

    #[test]
    fn timer_started_is_an_unlabeled_counter() {
        let recorder = HarvestMetricsRecorder::new();
        recorder.record_timer_started(30.0);
        recorder.record_timer_started(60.0);

        let families = recorder.collect();
        let f = family(&families, "harvest_timer_started_total");
        assert_eq!(f.kind, MetricKind::Counter);
        assert_eq!(f.samples.len(), 1);
        assert_eq!(f.samples[0].labels.len(), 0);
        assert_eq!(f.samples[0].value, 2.0);
    }

    #[test]
    fn queue_depth_is_a_last_write_wins_gauge() {
        let recorder = HarvestMetricsRecorder::new();
        recorder.record_queue_depth("default", 5);
        recorder.record_queue_depth("default", 3);

        let families = recorder.collect();
        let f = family(&families, "harvest_queue_depth");
        assert_eq!(f.kind, MetricKind::Gauge);
        assert_eq!(sample_value(f, &[("queue", "default")]), 3.0);
    }

    #[test]
    fn dlq_entries_is_a_gauge_labeled_by_shard() {
        let recorder = HarvestMetricsRecorder::new();
        recorder.record_dlq_entries(0, 7);

        let families = recorder.collect();
        let f = family(&families, "harvest_dlq_entries");
        assert_eq!(f.kind, MetricKind::Gauge);
        assert_eq!(sample_value(f, &[("shard", "0")]), 7.0);
    }

    #[test]
    fn schedule_runs_is_a_counter_labeled_by_kind_and_name() {
        let recorder = HarvestMetricsRecorder::new();
        recorder.record_schedule_run("cron", "nightly_report");

        let families = recorder.collect();
        let f = family(&families, "harvest_schedule_runs_total");
        assert_eq!(
            sample_value(f, &[("kind", "cron"), ("name", "nightly_report")]),
            1.0
        );
    }

    #[test]
    fn schedule_skipped_batches_via_record_schedule_skipped_n() {
        let recorder = HarvestMetricsRecorder::new();
        recorder.record_schedule_skipped("cron", "nightly_report", "overlap");
        recorder.record_schedule_skipped_n("cron", "nightly_report", "overlap", 4);

        let families = recorder.collect();
        let f = family(&families, "harvest_schedule_skipped_total");
        assert_eq!(
            sample_value(
                f,
                &[
                    ("kind", "cron"),
                    ("name", "nightly_report"),
                    ("reason", "overlap")
                ]
            ),
            5.0
        );
    }

    #[test]
    fn retention_deleted_is_a_counter_labeled_by_workflow() {
        // Issue #737: the `harvest.retention.deleted` counter is now owned by
        // `record_retention_deleted` and carries a per-workflow-type label; the
        // per-shard `record_retention_tick` no longer emits it (avoids
        // double-count). See metrics_rs_adapter.rs for the mirror.
        let recorder = HarvestMetricsRecorder::new();
        // A tick alone emits nothing for this counter now.
        recorder.record_retention_tick(0, 100, 42, 0.5);
        recorder.record_retention_deleted("onboarding", 42);

        let families = recorder.collect();
        let f = family(&families, "harvest_retention_deleted_total");
        assert_eq!(sample_value(f, &[("workflow", "onboarding")]), 42.0);
    }

    #[test]
    fn queue_oldest_pending_age_is_a_gauge_labeled_by_queue() {
        let recorder = HarvestMetricsRecorder::new();
        recorder.record_queue_oldest_pending_age("default", 12.5);

        let families = recorder.collect();
        let f = family(&families, "harvest_queue_oldest_pending_age");
        assert_eq!(f.kind, MetricKind::Gauge);
        assert_eq!(sample_value(f, &[("queue", "default")]), 12.5);
    }

    #[test]
    fn worker_slots_are_gauges_labeled_by_slot_type() {
        let recorder = HarvestMetricsRecorder::new();
        recorder.record_worker_slots(SlotType::Workflow, 3, 7);

        let families = recorder.collect();
        let in_use = family(&families, "harvest_worker_slots_in_use");
        let available = family(&families, "harvest_worker_slots_available");
        assert_eq!(in_use.kind, MetricKind::Gauge);
        assert_eq!(available.kind, MetricKind::Gauge);
        assert_eq!(sample_value(in_use, &[("slot_type", "workflow")]), 3.0);
        assert_eq!(sample_value(available, &[("slot_type", "workflow")]), 7.0);
    }

    #[test]
    fn worker_slot_target_is_a_gauge_labeled_by_slot_type() {
        let recorder = HarvestMetricsRecorder::new();
        recorder.record_worker_slot_target(SlotType::Activity, 10);

        let families = recorder.collect();
        let f = family(&families, "harvest_worker_slot_target");
        assert_eq!(sample_value(f, &[("slot_type", "activity")]), 10.0);
    }

    #[test]
    fn shard_stranded_pending_is_a_gauge_labeled_by_shard() {
        let recorder = HarvestMetricsRecorder::new();
        recorder.record_shard_stranded_pending(2, 9);

        let families = recorder.collect();
        let f = family(&families, "harvest_shard_stranded_pending");
        assert_eq!(sample_value(f, &[("shard", "2")]), 9.0);
    }

    #[test]
    fn schedule_overdue_is_a_gauge_labeled_by_kind_and_name() {
        // Issue #696 (Codex round 3): the built-in scrape recorder must render
        // `harvest_schedule_overdue` (1 = overdue, 0 = healthy) so the recommended
        // `with_metrics_scrape()` path exposes the primary overdue gauge the alert
        // consumes — not only the metrics-rs adapter.
        let recorder = HarvestMetricsRecorder::new();
        recorder.record_schedule_overdue("workflow", "foo", true);
        recorder.record_schedule_overdue("dag", "bar", false);

        let families = recorder.collect();
        let f = family(&families, "harvest_schedule_overdue");
        assert_eq!(f.kind, MetricKind::Gauge);
        assert_eq!(
            sample_value(f, &[("kind", "workflow"), ("name", "foo")]),
            1.0
        );
        assert_eq!(sample_value(f, &[("kind", "dag"), ("name", "bar")]), 0.0);
    }

    #[test]
    fn schedule_overdue_gauge_is_last_write_wins() {
        // A healthy → overdue → recovered transition must reflect the latest value.
        let recorder = HarvestMetricsRecorder::new();
        recorder.record_schedule_overdue("workflow", "foo", true);
        recorder.record_schedule_overdue("workflow", "foo", false);

        let families = recorder.collect();
        let f = family(&families, "harvest_schedule_overdue");
        assert_eq!(
            sample_value(f, &[("kind", "workflow"), ("name", "foo")]),
            0.0
        );
    }

    #[test]
    fn connector_metrics_reach_the_built_in_scrape_endpoint() {
        // Issue #944 (Codex round E): this recorder is per-metric
        // hand-maintained, so a new family that is not overridden here falls
        // through to the trait no-op and `.with_metrics_scrape()` DISCARDS every
        // sample. The connector work shipped dashboard panels, which makes that
        // silence indistinguishable from a healthy, idle consumer.
        use autumn_harvest::telemetry::{ConnectorOutcome, PoisonReason};

        let recorder = HarvestMetricsRecorder::new();
        recorder.record_connector_received("orders");
        recorder.record_connector_received("orders");
        recorder.record_connector_dispatched("orders", ConnectorOutcome::Dispatched);
        recorder.record_connector_dispatched("orders", ConnectorOutcome::IdempotentReplay);
        recorder.record_connector_poisoned("orders", PoisonReason::MappingRejected);
        recorder.record_connector_lag("orders", 42);

        let families = recorder.collect();

        let received = family(&families, "harvest_connector_received_total");
        assert_eq!(received.kind, MetricKind::Counter);
        assert_eq!(sample_value(received, &[("source", "orders")]), 2.0);

        let dispatched = family(&families, "harvest_connector_dispatched_total");
        assert_eq!(dispatched.kind, MetricKind::Counter);
        assert_eq!(
            sample_value(
                dispatched,
                &[("source", "orders"), ("outcome", "dispatched")]
            ),
            1.0
        );
        assert_eq!(
            sample_value(
                dispatched,
                &[("source", "orders"), ("outcome", "idempotent_replay")]
            ),
            1.0
        );

        let poisoned = family(&families, "harvest_connector_poisoned_total");
        assert_eq!(poisoned.kind, MetricKind::Counter);
        assert_eq!(
            sample_value(
                poisoned,
                &[("source", "orders"), ("reason", "mapping_rejected")]
            ),
            1.0
        );

        let lag = family(&families, "harvest_connector_lag");
        assert_eq!(lag.kind, MetricKind::Gauge);
        assert_eq!(sample_value(lag, &[("source", "orders")]), 42.0);
    }

    #[test]
    fn connector_lag_is_a_last_write_wins_gauge() {
        // Lag is a level, not an accumulation: a partition draining from 500 to
        // 0 must read 0, and a wedged one must keep reporting its backlog
        // rather than summing every sample the poll loop ever took.
        use autumn_harvest::telemetry::ConnectorOutcome;

        let recorder = HarvestMetricsRecorder::new();
        recorder.record_connector_lag("orders", 500);
        recorder.record_connector_lag("orders", 0);
        // A second source keeps its own level.
        recorder.record_connector_lag("audit", 7);
        // Unrelated families must not be perturbed.
        recorder.record_connector_dispatched("audit", ConnectorOutcome::Dispatched);

        let families = recorder.collect();
        let lag = family(&families, "harvest_connector_lag");
        assert_eq!(sample_value(lag, &[("source", "orders")]), 0.0);
        assert_eq!(sample_value(lag, &[("source", "audit")]), 7.0);
    }

    #[test]
    fn clones_share_the_same_underlying_state() {
        let recorder = HarvestMetricsRecorder::new();
        let clone = recorder.clone();
        clone.record_workflow_started("onboarding", "default");

        let families = recorder.collect();
        let f = family(&families, "harvest_workflow_started_total");
        assert_eq!(
            sample_value(f, &[("workflow", "onboarding"), ("queue", "default")]),
            1.0
        );
    }
}
