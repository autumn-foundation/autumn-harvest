//! Metrics catalogue coverage tests.
//!
//! Verifies that every `METRIC_*` constant in the ADR-0001 §7 catalogue is
//! reachable via the `MetricsRecorder` API, that label cardinality rules are
//! enforced by construction (no `execution.id` on any metric), and that
//! `record_dlq_entries` is included in the trait surface.
//!
//! These tests run without the `db` feature so they work on every platform
//! and CI environment.

use std::sync::{Arc, Mutex};

use autumn_harvest::telemetry::{
    ActivityStatus, METRIC_ACTIVITY_DURATION, METRIC_DLQ_ENTRIES, METRIC_QUEUE_DEPTH,
    METRIC_QUEUE_DISPATCHED, METRIC_RETENTION_DELETED, METRIC_SAGA_COMPENSATED,
    METRIC_SAGA_COMPENSATION_FAILED, METRIC_SCHEDULE_DECISION_WRITE_FAILED, METRIC_SCHEDULE_RUNS,
    METRIC_SCHEDULE_SKIPPED, METRIC_SIGNAL_RECEIVED, METRIC_SIGNAL_UNHANDLED, METRIC_TIMER_STARTED,
    METRIC_UPDATE_ADMITTED, METRIC_UPDATE_COMPLETED, METRIC_UPDATE_FAILED, METRIC_UPDATE_REJECTED,
    METRIC_WORKFLOW_CONTINUE_AS_NEW, METRIC_WORKFLOW_DURATION, METRIC_WORKFLOW_HISTORY_SIZE,
    METRIC_WORKFLOW_STARTED, METRIC_WORKFLOW_TASK_TIMEOUT, MetricsRecorder, NoOpMetrics,
    WorkflowStatus,
};

// ---------------------------------------------------------------------------
// Recording test implementation
// ---------------------------------------------------------------------------

/// Captured record of a single metric sample, carrying the name (one of the
/// `METRIC_*` constants) and the labels as key/value string pairs.
#[derive(Debug, Clone)]
pub struct MetricSample {
    pub name: &'static str,
    pub labels: Vec<(&'static str, String)>,
}

/// `MetricsRecorder` implementation that buffers every sample so tests can
/// assert exact call patterns.
#[derive(Debug, Default)]
pub struct RecordingMetrics {
    pub samples: Mutex<Vec<MetricSample>>,
}

impl RecordingMetrics {
    /// # Panics
    /// Panics if the internal mutex is poisoned.
    pub fn drain(&self) -> Vec<MetricSample> {
        self.samples.lock().unwrap().drain(..).collect()
    }
}

impl MetricsRecorder for RecordingMetrics {
    fn record_workflow_started(&self, workflow_name: &str, queue: &str) {
        self.samples.lock().unwrap().push(MetricSample {
            name: METRIC_WORKFLOW_STARTED,
            labels: vec![
                ("workflow", workflow_name.to_owned()),
                ("queue", queue.to_owned()),
            ],
        });
    }

    fn record_workflow_completed(
        &self,
        workflow_name: &str,
        queue: &str,
        _duration_secs: f64,
        status: WorkflowStatus,
    ) {
        self.samples.lock().unwrap().push(MetricSample {
            name: METRIC_WORKFLOW_DURATION,
            labels: vec![
                ("workflow", workflow_name.to_owned()),
                ("queue", queue.to_owned()),
                ("status", status.as_str().to_owned()),
            ],
        });
    }

    fn record_workflow_history_size(&self, workflow_name: &str, event_count: u64) {
        self.samples.lock().unwrap().push(MetricSample {
            name: METRIC_WORKFLOW_HISTORY_SIZE,
            labels: vec![
                ("workflow.type", workflow_name.to_owned()),
                ("count", event_count.to_string()),
            ],
        });
    }

    fn record_workflow_continue_as_new(&self, workflow_name: &str) {
        self.samples.lock().unwrap().push(MetricSample {
            name: METRIC_WORKFLOW_CONTINUE_AS_NEW,
            labels: vec![("workflow.type", workflow_name.to_owned())],
        });
    }

    fn record_activity_completed(
        &self,
        activity_name: &str,
        queue: &str,
        _duration_secs: f64,
        status: ActivityStatus,
    ) {
        self.samples.lock().unwrap().push(MetricSample {
            name: METRIC_ACTIVITY_DURATION,
            labels: vec![
                ("activity", activity_name.to_owned()),
                ("queue", queue.to_owned()),
                ("status", status.as_str().to_owned()),
            ],
        });
    }

    fn record_timer_started(&self, _duration_secs: f64) {
        self.samples.lock().unwrap().push(MetricSample {
            name: METRIC_TIMER_STARTED,
            labels: vec![],
        });
    }

    fn record_queue_depth(&self, queue_name: &str, depth: u64) {
        self.samples.lock().unwrap().push(MetricSample {
            name: METRIC_QUEUE_DEPTH,
            labels: vec![
                ("queue", queue_name.to_owned()),
                ("depth", depth.to_string()),
            ],
        });
    }

    fn record_dlq_entries(&self, shard: u16, depth: u64) {
        self.samples.lock().unwrap().push(MetricSample {
            name: METRIC_DLQ_ENTRIES,
            labels: vec![("shard", shard.to_string()), ("depth", depth.to_string())],
        });
    }

    fn record_schedule_run(&self, kind: &str, name: &str) {
        self.samples.lock().unwrap().push(MetricSample {
            name: METRIC_SCHEDULE_RUNS,
            labels: vec![("kind", kind.to_owned()), ("name", name.to_owned())],
        });
    }

    fn record_schedule_skipped(&self, kind: &str, name: &str, reason: &str) {
        self.samples.lock().unwrap().push(MetricSample {
            name: METRIC_SCHEDULE_SKIPPED,
            labels: vec![
                ("kind", kind.to_owned()),
                ("name", name.to_owned()),
                ("reason", reason.to_owned()),
            ],
        });
    }

    fn record_schedule_decision_write_failed(&self) {
        self.samples.lock().unwrap().push(MetricSample {
            name: METRIC_SCHEDULE_DECISION_WRITE_FAILED,
            labels: vec![],
        });
    }

    fn record_retention_tick(
        &self,
        _shard: u16,
        _candidate_count: u64,
        _deleted_count: u64,
        _duration_secs: f64,
    ) {
        // Issue #737: the retention deletion counter is now owned by
        // `record_retention_deleted` (labeled by workflow type). The tick
        // observability point emits nothing for the catalogue counter, so the
        // coverage guard exercises the real emission path below, not this one.
    }

    fn record_retention_deleted(&self, workflow: &str, count: u64) {
        self.samples.lock().unwrap().push(MetricSample {
            name: METRIC_RETENTION_DELETED,
            labels: vec![
                ("workflow", workflow.to_string()),
                ("count", count.to_string()),
            ],
        });
    }

    fn record_workflow_task_timeout(&self, workflow_name: &str, queue: &str) {
        self.samples.lock().unwrap().push(MetricSample {
            name: METRIC_WORKFLOW_TASK_TIMEOUT,
            labels: vec![
                ("workflow", workflow_name.to_owned()),
                ("queue", queue.to_owned()),
            ],
        });
    }

    fn record_task_dispatched(&self, queue_name: &str) {
        self.samples.lock().unwrap().push(MetricSample {
            name: METRIC_QUEUE_DISPATCHED,
            labels: vec![("queue", queue_name.to_owned())],
        });
    }

    fn record_saga_compensated(&self, workflow_name: &str, queue: &str) {
        self.samples.lock().unwrap().push(MetricSample {
            name: METRIC_SAGA_COMPENSATED,
            labels: vec![
                ("workflow", workflow_name.to_owned()),
                ("queue", queue.to_owned()),
            ],
        });
    }

    fn record_saga_compensation_failed(&self, workflow_name: &str, queue: &str) {
        self.samples.lock().unwrap().push(MetricSample {
            name: METRIC_SAGA_COMPENSATION_FAILED,
            labels: vec![
                ("workflow", workflow_name.to_owned()),
                ("queue", queue.to_owned()),
            ],
        });
    }

    // ── Signal & update lifecycle counters (issue #684) ───────────────────

    fn record_signal_received(&self, workflow_name: &str, queue: &str) {
        // Issue #684 (Codex P2): no `name` label — free-form send route.
        self.samples.lock().unwrap().push(MetricSample {
            name: METRIC_SIGNAL_RECEIVED,
            labels: vec![
                ("workflow", workflow_name.to_owned()),
                ("queue", queue.to_owned()),
            ],
        });
    }

    fn record_signal_unhandled(&self, workflow_name: &str, queue: &str) {
        // Issue #684 (Codex P2): no `name` label — free-form send route.
        self.samples.lock().unwrap().push(MetricSample {
            name: METRIC_SIGNAL_UNHANDLED,
            labels: vec![
                ("workflow", workflow_name.to_owned()),
                ("queue", queue.to_owned()),
            ],
        });
    }

    fn record_update_admitted(&self, workflow_name: &str, queue: &str) {
        self.samples.lock().unwrap().push(MetricSample {
            name: METRIC_UPDATE_ADMITTED,
            labels: vec![
                ("workflow", workflow_name.to_owned()),
                ("queue", queue.to_owned()),
            ],
        });
    }

    fn record_update_rejected(&self, workflow_name: &str, update_name: &str) {
        self.samples.lock().unwrap().push(MetricSample {
            name: METRIC_UPDATE_REJECTED,
            labels: vec![
                ("workflow", workflow_name.to_owned()),
                ("name", update_name.to_owned()),
            ],
        });
    }

    fn record_update_completed(&self, workflow_name: &str, update_name: &str, queue: &str) {
        self.samples.lock().unwrap().push(MetricSample {
            name: METRIC_UPDATE_COMPLETED,
            labels: vec![
                ("workflow", workflow_name.to_owned()),
                ("name", update_name.to_owned()),
                ("queue", queue.to_owned()),
            ],
        });
    }

    fn record_update_failed(&self, workflow_name: &str, update_name: &str, queue: &str) {
        self.samples.lock().unwrap().push(MetricSample {
            name: METRIC_UPDATE_FAILED,
            labels: vec![
                ("workflow", workflow_name.to_owned()),
                ("name", update_name.to_owned()),
                ("queue", queue.to_owned()),
            ],
        });
    }
}

// ---------------------------------------------------------------------------
// Catalogue coverage tests
// ---------------------------------------------------------------------------

#[test]
#[allow(clippy::too_many_lines)] // one call + assertion per catalogue metric
fn all_catalogue_metrics_are_reachable_via_trait() {
    // Wires every record_* call through RecordingMetrics and verifies the
    // expected metric name appears in the sample list — one assertion per
    // ADR-0001 §7 catalogue entry.
    let rec = RecordingMetrics::default();

    rec.record_workflow_started("my_workflow", "default");
    rec.record_workflow_completed("my_workflow", "default", 1.0, WorkflowStatus::Completed);
    rec.record_workflow_history_size("my_workflow", 42);
    rec.record_workflow_continue_as_new("my_workflow");
    rec.record_activity_completed("my_activity", "default", 0.5, ActivityStatus::Completed);
    rec.record_timer_started(30.0);
    rec.record_queue_depth("default", 5);
    rec.record_dlq_entries(0, 2);
    rec.record_schedule_run("workflow", "nightly");
    rec.record_schedule_skipped("workflow", "nightly", "paused");
    rec.record_schedule_decision_write_failed();
    rec.record_retention_tick(0, 100, 50, 0.01);
    rec.record_retention_deleted("my_workflow", 50);
    rec.record_workflow_task_timeout("my_workflow", "default");
    rec.record_task_dispatched("default");
    rec.record_signal_received("my_workflow", "default");
    rec.record_signal_unhandled("my_workflow", "default");
    rec.record_update_admitted("my_workflow", "default");
    rec.record_update_rejected("my_workflow", "set_priority");
    rec.record_update_completed("my_workflow", "set_priority", "default");
    rec.record_update_failed("my_workflow", "set_priority", "default");

    let samples = rec.drain();
    let names: Vec<&str> = samples.iter().map(|s| s.name).collect();

    assert!(
        names.contains(&METRIC_SCHEDULE_DECISION_WRITE_FAILED),
        "harvest.schedule.decision_write_failed not sampled"
    );
    assert!(
        names.contains(&METRIC_WORKFLOW_STARTED),
        "harvest.workflow.started not sampled"
    );
    assert!(
        names.contains(&METRIC_WORKFLOW_DURATION),
        "harvest.workflow.duration not sampled"
    );
    assert!(
        names.contains(&METRIC_WORKFLOW_HISTORY_SIZE),
        "harvest.workflow.history_size not sampled"
    );
    assert!(
        names.contains(&METRIC_WORKFLOW_CONTINUE_AS_NEW),
        "harvest.workflow.continue_as_new not sampled"
    );
    for sample in samples.iter().filter(|sample| {
        matches!(
            sample.name,
            METRIC_WORKFLOW_HISTORY_SIZE | METRIC_WORKFLOW_CONTINUE_AS_NEW
        )
    }) {
        assert!(
            sample
                .labels
                .iter()
                .any(|(key, value)| *key == "workflow.type" && value == "my_workflow"),
            "new workflow-history metrics must use workflow.type only; got {sample:?}"
        );
    }
    assert!(
        names.contains(&METRIC_ACTIVITY_DURATION),
        "harvest.activity.duration not sampled"
    );
    assert!(
        names.contains(&METRIC_TIMER_STARTED),
        "harvest.timer.started not sampled"
    );
    assert!(
        names.contains(&METRIC_QUEUE_DEPTH),
        "harvest.queue.depth not sampled"
    );
    assert!(
        names.contains(&METRIC_DLQ_ENTRIES),
        "harvest.dlq.entries not sampled"
    );
    assert!(
        names.contains(&METRIC_SCHEDULE_RUNS),
        "harvest.schedule.runs not sampled"
    );
    assert!(
        names.contains(&METRIC_SCHEDULE_SKIPPED),
        "harvest.schedule.skipped not sampled"
    );
    assert!(
        names.contains(&METRIC_RETENTION_DELETED),
        "harvest.retention.deleted not sampled"
    );
    assert!(
        names.contains(&METRIC_WORKFLOW_TASK_TIMEOUT),
        "harvest.workflow.task_timeout not sampled"
    );
    assert!(
        names.contains(&METRIC_QUEUE_DISPATCHED),
        "harvest.queue.dispatched not sampled"
    );
    for (metric, label) in [
        (METRIC_SIGNAL_RECEIVED, "harvest.signal.received"),
        (METRIC_SIGNAL_UNHANDLED, "harvest.signal.unhandled"),
        (METRIC_UPDATE_ADMITTED, "harvest.update.admitted"),
        (METRIC_UPDATE_REJECTED, "harvest.update.rejected"),
        (METRIC_UPDATE_COMPLETED, "harvest.update.completed"),
        (METRIC_UPDATE_FAILED, "harvest.update.failed"),
    ] {
        assert!(names.contains(&metric), "{label} not sampled");
    }
}

#[test]
fn cardinality_no_execution_id_label_on_any_metric() {
    // ADR-0001 §7: execution.id is span-only and FORBIDDEN as a metric label.
    // RecordingMetrics captures all labels; none must be "execution_id" or
    // "execution.id".
    let rec = RecordingMetrics::default();

    rec.record_workflow_started("wf", "default");
    rec.record_workflow_completed("wf", "default", 1.0, WorkflowStatus::Failed);
    rec.record_workflow_history_size("wf", 12);
    rec.record_workflow_continue_as_new("wf");
    rec.record_activity_completed("act", "default", 0.5, ActivityStatus::Failed);
    rec.record_timer_started(10.0);
    rec.record_queue_depth("default", 0);
    rec.record_dlq_entries(0, 0);
    rec.record_schedule_run("dag", "my_dag");
    rec.record_schedule_skipped("dag", "my_dag", "max_active_runs_reached");
    rec.record_schedule_decision_write_failed();
    rec.record_retention_tick(0, 0, 0, 0.0);
    rec.record_workflow_task_timeout("wf", "default");
    rec.record_signal_received("wf", "default");
    rec.record_signal_unhandled("wf", "default");
    rec.record_update_admitted("wf", "default");
    rec.record_update_rejected("wf", "set_priority");
    rec.record_update_completed("wf", "set_priority", "default");
    rec.record_update_failed("wf", "set_priority", "default");

    for sample in rec.drain() {
        for (key, _val) in &sample.labels {
            assert!(
                !key.contains("execution_id") && !key.contains("execution.id"),
                "forbidden label '{key}' found on metric '{}' — execution.id must never be a metric label",
                sample.name,
            );
        }
    }
}

#[test]
fn signal_update_lifecycle_counters_reachable_with_bounded_labels() {
    // Issue #684: the six signal/update lifecycle counters must be reachable
    // via the trait with exactly the bounded label sets documented in
    // telemetry.rs — execution.id is forbidden by construction (ADR-0001 §7).
    // Scope note (mirrors saga_counters_...): the label keys asserted below
    // belong to the RecordingMetrics test double, so this pins the trait's
    // reachable surface; production label content is pinned by the
    // metrics_rs_adapter bridge test and the context-level tests.
    let rec = RecordingMetrics::default();
    rec.record_signal_received("wf", "default");
    rec.record_signal_unhandled("wf", "default");
    rec.record_update_admitted("wf", "default");
    rec.record_update_rejected("wf", "set_priority");
    rec.record_update_completed("wf", "set_priority", "default");
    rec.record_update_failed("wf", "set_priority", "default");

    let samples = rec.drain();
    assert_eq!(samples.len(), 6);

    // Issue #684 (Codex P2): the signal counters AND update.admitted carry NO
    // free-form `name` label — signal names come from the free-form send route,
    // and admission happens at the free-form update route boundary before the
    // name is resolved against a handler (declarative OR imperative), so neither
    // can be bounded by construction. update.rejected/completed/failed keep the
    // `name` label because it resolves against the registered/declared handler
    // set (failed buckets an unregistered name to the __unregistered__ sentinel).
    let expected: &[(&str, &[&str])] = &[
        (METRIC_SIGNAL_RECEIVED, &["workflow", "queue"]),
        (METRIC_SIGNAL_UNHANDLED, &["workflow", "queue"]),
        (METRIC_UPDATE_ADMITTED, &["workflow", "queue"]),
        (METRIC_UPDATE_REJECTED, &["workflow", "name"]),
        (METRIC_UPDATE_COMPLETED, &["workflow", "name", "queue"]),
        (METRIC_UPDATE_FAILED, &["workflow", "name", "queue"]),
    ];

    for (metric, want_keys) in expected {
        let sample = samples
            .iter()
            .find(|s| s.name == *metric)
            .unwrap_or_else(|| panic!("{metric} not sampled"));
        let keys: Vec<&str> = sample.labels.iter().map(|(k, _)| *k).collect();
        assert_eq!(
            &keys, want_keys,
            "{metric} must carry exactly {want_keys:?}; got {sample:?}"
        );
        for (key, _) in &sample.labels {
            assert!(
                !key.contains("execution_id") && !key.contains("execution.id"),
                "execution.id must never be a metric label (metric '{metric}')"
            );
        }
    }
}

#[test]
fn dlq_entries_shard_label_is_present() {
    // Verifies the shard label is populated correctly for single-shard
    // (shard 0) and multi-shard (shard 3) deployments.
    let rec = RecordingMetrics::default();
    rec.record_dlq_entries(0, 7);
    rec.record_dlq_entries(3, 12);

    let samples = rec.drain();
    assert_eq!(samples.len(), 2);

    let shard_labels: Vec<&str> = samples
        .iter()
        .flat_map(|s| s.labels.iter())
        .filter(|(k, _)| *k == "shard")
        .map(|(_, v)| v.as_str())
        .collect();
    assert!(shard_labels.contains(&"0"), "shard=0 must be reported");
    assert!(shard_labels.contains(&"3"), "shard=3 must be reported");
}

#[test]
fn workflow_status_label_values_match_adr() {
    // ADR-0001 §7: workflow.duration status label must be one of
    // completed | failed | suspended | continued_as_new.
    let rec = RecordingMetrics::default();
    rec.record_workflow_completed("wf", "q", 1.0, WorkflowStatus::Completed);
    rec.record_workflow_completed("wf", "q", 1.0, WorkflowStatus::Failed);
    rec.record_workflow_completed("wf", "q", 1.0, WorkflowStatus::Suspended);
    rec.record_workflow_completed("wf", "q", 1.0, WorkflowStatus::ContinuedAsNew);

    let status_values: Vec<String> = rec
        .drain()
        .into_iter()
        .flat_map(|s| s.labels)
        .filter(|(k, _)| *k == "status")
        .map(|(_, v)| v)
        .collect();

    assert!(status_values.contains(&"completed".to_owned()));
    assert!(status_values.contains(&"failed".to_owned()));
    assert!(status_values.contains(&"suspended".to_owned()));
    assert!(status_values.contains(&"continued_as_new".to_owned()));
}

#[test]
fn schedule_skipped_reason_values_match_adr() {
    // ADR-0001 §7: schedule.skipped reason must be one of
    // paused | max_active_runs_reached | catchup_disabled.
    let rec = RecordingMetrics::default();
    rec.record_schedule_skipped("workflow", "wf", "paused");
    rec.record_schedule_skipped("workflow", "wf", "max_active_runs_reached");
    rec.record_schedule_skipped("workflow", "wf", "catchup_disabled");

    let reasons: Vec<String> = rec
        .drain()
        .into_iter()
        .flat_map(|s| s.labels)
        .filter(|(k, _)| *k == "reason")
        .map(|(_, v)| v)
        .collect();

    assert!(reasons.contains(&"paused".to_owned()));
    assert!(reasons.contains(&"max_active_runs_reached".to_owned()));
    assert!(reasons.contains(&"catchup_disabled".to_owned()));
}

#[test]
fn saga_counters_reachable_and_never_labeled_by_execution_id() {
    // Issue #801: both saga counters must be reachable via the trait with
    // exactly the workflow + queue labels — execution.id is forbidden by
    // construction (ADR-0001 §7). Scope note (post-review): the label keys
    // asserted below belong to the RecordingMetrics test double, so this
    // test pins the trait's reachable surface, not production label content
    // (which is pinned by the context-level label tests and the
    // metrics_rs_adapter bridge test).
    let rec = RecordingMetrics::default();
    rec.record_saga_compensated("book_trip", "payments");
    rec.record_saga_compensation_failed("book_trip", "payments");

    let samples = rec.drain();
    assert_eq!(samples.len(), 2);

    let compensated = samples
        .iter()
        .find(|s| s.name == METRIC_SAGA_COMPENSATED)
        .expect("harvest.saga.compensated not sampled");
    let failed = samples
        .iter()
        .find(|s| s.name == METRIC_SAGA_COMPENSATION_FAILED)
        .expect("harvest.saga.compensation_failed not sampled");

    for sample in [compensated, failed] {
        let keys: Vec<&str> = sample.labels.iter().map(|(k, _)| *k).collect();
        assert_eq!(
            keys,
            vec!["workflow", "queue"],
            "saga counters must carry exactly workflow + queue labels; got {sample:?}"
        );
        for (key, _) in &sample.labels {
            assert!(
                !key.contains("execution_id") && !key.contains("execution.id"),
                "execution.id must never be a metric label (metric '{}')",
                sample.name
            );
        }
    }
}

#[test]
fn noop_metrics_implements_all_catalogue_methods() {
    // Smoke test: NoOpMetrics must implement every record_* method in the
    // catalogue without panicking.
    let rec: Arc<dyn MetricsRecorder> = Arc::new(NoOpMetrics);
    rec.record_workflow_started("wf", "q");
    rec.record_workflow_completed("wf", "q", 1.0, WorkflowStatus::Completed);
    rec.record_workflow_history_size("wf", 2);
    rec.record_workflow_continue_as_new("wf");
    rec.record_activity_completed("act", "q", 0.5, ActivityStatus::Completed);
    rec.record_timer_started(5.0);
    rec.record_queue_depth("q", 0);
    rec.record_dlq_entries(0, 0);
    rec.record_schedule_run("workflow", "wf");
    rec.record_schedule_skipped("workflow", "wf", "paused");
    rec.record_schedule_decision_write_failed();
    rec.record_retention_tick(0, 0, 0, 0.0);
    rec.record_concurrency_key_in_flight("key", 0);
    rec.record_concurrency_key_deferred("key", 0);
}
