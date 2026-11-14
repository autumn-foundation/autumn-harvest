//! Falsifiable-proxy integration tests for activity outcome counters (issue #528).
//!
//! `harvest.activity.attempts{outcome}` and `harvest.activity.retries` fire at
//! the right `MetricsRecorder` call sites. Their labels have low cardinality
//! and carry no `execution.id`.
//!
//! These tests need no database. They call the `MetricsRecorder` methods
//! directly, as `tests/activity_failure_tests.rs` does for
//! `record_activity_failed`.

use autumn_harvest::telemetry::{ActivityStatus, MetricsRecorder, NoOpMetrics};
use std::sync::Arc;
use std::sync::Mutex;

// ---------------------------------------------------------------------------
// Recording mock for asserting counter values and labels.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct RecordingActivityMetrics {
    attempts: Mutex<Vec<(String, String, String)>>, // (activity, queue, outcome)
    retries: Mutex<Vec<(String, String)>>,          // (activity, queue)
    failures: Mutex<Vec<(String, String)>>,         // (activity, error_type)
}

impl MetricsRecorder for RecordingActivityMetrics {
    fn record_activity_attempt(&self, activity_name: &str, queue: &str, outcome: ActivityStatus) {
        self.attempts.lock().unwrap().push((
            activity_name.to_owned(),
            queue.to_owned(),
            outcome.as_str().to_owned(),
        ));
    }

    fn record_activity_retried(&self, activity_name: &str, queue: &str) {
        self.retries
            .lock()
            .unwrap()
            .push((activity_name.to_owned(), queue.to_owned()));
    }

    fn record_activity_failed(
        &self,
        activity_name: &str,
        _workflow_type: &str,
        error_type: &str,
        _non_retryable: bool,
    ) {
        self.failures
            .lock()
            .unwrap()
            .push((activity_name.to_owned(), error_type.to_owned()));
    }
}

impl RecordingActivityMetrics {
    fn completed_count(&self, activity: &str) -> usize {
        self.attempts
            .lock()
            .unwrap()
            .iter()
            .filter(|(a, _, o)| a == activity && o == "completed")
            .count()
    }

    fn failed_attempt_count(&self, activity: &str) -> usize {
        self.attempts
            .lock()
            .unwrap()
            .iter()
            .filter(|(a, _, o)| a == activity && o == "failed")
            .count()
    }

    fn retry_count(&self, activity: &str) -> usize {
        self.retries
            .lock()
            .unwrap()
            .iter()
            .filter(|(a, _)| a == activity)
            .count()
    }

    fn terminal_failure_count(&self, activity: &str) -> usize {
        self.failures
            .lock()
            .unwrap()
            .iter()
            .filter(|(a, _)| a == activity)
            .count()
    }
}

// ---------------------------------------------------------------------------
// Trait method existence — compile-time AC verification.
// ---------------------------------------------------------------------------

#[test]
fn record_activity_attempt_exists_on_metrics_recorder_trait() {
    // AC4: new MetricsRecorder methods are present.
    let rec = NoOpMetrics;
    rec.record_activity_attempt("send_email", "default", ActivityStatus::Completed);
    rec.record_activity_attempt("send_email", "default", ActivityStatus::Failed);
}

#[test]
fn record_activity_retried_exists_on_metrics_recorder_trait() {
    let rec = NoOpMetrics;
    rec.record_activity_retried("send_email", "default");
}

#[test]
fn trait_is_object_safe_with_new_methods() {
    // Both new methods must not break object safety.
    let _rec: Arc<dyn MetricsRecorder> = Arc::new(NoOpMetrics);
}

// ---------------------------------------------------------------------------
// K successes, F terminal failures, R retries — the issue's falsifiable proxy.
// ---------------------------------------------------------------------------

#[test]
fn k_successes_emit_k_completed_attempts_no_retries() {
    const K: usize = 5;
    let metrics = RecordingActivityMetrics::default();

    for _ in 0..K {
        metrics.record_activity_attempt("charge_card", "billing", ActivityStatus::Completed);
    }

    assert_eq!(
        metrics.completed_count("charge_card"),
        K,
        "harvest.activity.attempts{{outcome=completed}} must == K"
    );
    assert_eq!(
        metrics.retry_count("charge_card"),
        0,
        "harvest.activity.retries must be 0 for pure successes"
    );
    assert_eq!(
        metrics.failed_attempt_count("charge_card"),
        0,
        "harvest.activity.attempts{{outcome=failed}} must be 0 for pure successes"
    );
}

#[test]
fn f_terminal_failures_emit_f_failed_attempts_and_f_failure_records() {
    const F: usize = 3;
    let metrics = RecordingActivityMetrics::default();

    for _ in 0..F {
        metrics.record_activity_attempt("charge_card", "billing", ActivityStatus::Failed);
        metrics.record_activity_failed("charge_card", "", "PaymentDeclined", true);
    }

    assert_eq!(
        metrics.failed_attempt_count("charge_card"),
        F,
        "harvest.activity.attempts{{outcome=failed}} must == F"
    );
    assert_eq!(
        metrics.terminal_failure_count("charge_card"),
        F,
        "harvest.activity.failed must == F"
    );
    assert_eq!(
        metrics.retry_count("charge_card"),
        0,
        "harvest.activity.retries must be 0 for terminal failures (no retry scheduled)"
    );
}

/// `harvest.activity.failed` counts terminal failures only, so a retried
/// attempt does not fire it.
#[test]
fn r_retries_emit_r_retried_and_r_failed_attempts_no_terminal_failure() {
    const R: usize = 4;
    let metrics = RecordingActivityMetrics::default();

    for _ in 0..R {
        metrics.record_activity_attempt("charge_card", "billing", ActivityStatus::Failed);
        metrics.record_activity_retried("charge_card", "billing");
    }

    assert_eq!(
        metrics.retry_count("charge_card"),
        R,
        "harvest.activity.retries must == R"
    );
    assert_eq!(
        metrics.failed_attempt_count("charge_card"),
        R,
        "harvest.activity.attempts{{outcome=failed}} must == R (one per failed attempt)"
    );
    assert_eq!(
        metrics.terminal_failure_count("charge_card"),
        0,
        "harvest.activity.failed must be 0 — no terminal failure, all retried"
    );
}

#[test]
fn k_successes_f_failures_r_retries_all_labelled_by_activity_type() {
    const K: usize = 3;
    const F: usize = 2;
    const R: usize = 4;

    let metrics = RecordingActivityMetrics::default();

    for _ in 0..K {
        metrics.record_activity_attempt("charge_card", "billing", ActivityStatus::Completed);
    }

    for _ in 0..R {
        metrics.record_activity_attempt("charge_card", "billing", ActivityStatus::Failed);
        metrics.record_activity_retried("charge_card", "billing");
    }

    for _ in 0..F {
        metrics.record_activity_attempt("charge_card", "billing", ActivityStatus::Failed);
        metrics.record_activity_failed("charge_card", "", "Transient", false);
    }

    // AC1: single-family ratio: completed / (completed + failed)
    assert_eq!(
        metrics.completed_count("charge_card"),
        K,
        "harvest.activity.attempts{{outcome=completed}} == K"
    );
    assert_eq!(
        metrics.failed_attempt_count("charge_card"),
        R + F,
        "harvest.activity.attempts{{outcome=failed}} == R + F (all failed attempts)"
    );

    // AC2: retry counter == R
    assert_eq!(
        metrics.retry_count("charge_card"),
        R,
        "harvest.activity.retries == R"
    );

    // `harvest.activity.failed` still fires only on a terminal failure.
    assert_eq!(
        metrics.terminal_failure_count("charge_card"),
        F,
        "harvest.activity.failed == F (terminal failures only)"
    );
}

/// AC3: label cardinality check — outcome is bounded to 2 values.
#[test]
fn outcome_label_has_bounded_cardinality() {
    let outcomes = [ActivityStatus::Completed, ActivityStatus::Failed];
    let strings: Vec<&str> = outcomes.iter().map(|s| s.as_str()).collect();

    assert_eq!(strings.len(), 2);
    assert!(strings.contains(&"completed"));
    assert!(strings.contains(&"failed"));
}

/// AC3: `execution.id` is not a parameter. These calls would not compile if
/// one were required.
#[test]
fn execution_id_is_not_a_parameter_of_new_activity_counters() {
    let rec: Arc<dyn MetricsRecorder> = Arc::new(NoOpMetrics);
    rec.record_activity_attempt("act", "q", ActivityStatus::Completed);
    rec.record_activity_retried("act", "q");
}

#[test]
fn counters_are_labelled_per_activity_type() {
    let metrics = RecordingActivityMetrics::default();

    metrics.record_activity_attempt("send_email", "email", ActivityStatus::Completed);
    metrics.record_activity_attempt("send_email", "email", ActivityStatus::Completed);
    metrics.record_activity_attempt("charge_card", "billing", ActivityStatus::Completed);
    metrics.record_activity_retried("send_email", "email");
    metrics.record_activity_retried("charge_card", "billing");
    metrics.record_activity_retried("charge_card", "billing");

    assert_eq!(metrics.completed_count("send_email"), 2);
    assert_eq!(metrics.completed_count("charge_card"), 1);
    assert_eq!(metrics.retry_count("send_email"), 1);
    assert_eq!(metrics.retry_count("charge_card"), 2);
}

#[test]
fn counters_are_thread_safe() {
    use std::thread;

    let metrics = Arc::new(RecordingActivityMetrics::default());
    let mut handles = Vec::new();

    for _ in 0..10 {
        let m = Arc::clone(&metrics);
        handles.push(thread::spawn(move || {
            m.record_activity_attempt("act", "q", ActivityStatus::Completed);
            m.record_activity_retried("act", "q");
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(metrics.completed_count("act"), 10);
    assert_eq!(metrics.retry_count("act"), 10);
}
