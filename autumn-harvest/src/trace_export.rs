//! Chrome Trace Event Format exporter for DAG profiles.
//!
//! Converts a `DagProfile` into a JSON structure compatible with trace viewers
//! like `chrome://tracing` or `ui.perfetto.dev`.

use crate::dag_profiler::{DagProfile, ProfilerEventKind};
use serde_json::{Value, json};
use std::collections::HashMap;

use crate::timeline::{StepOutcome, Timeline};

/// Exports a `DagProfile` to Chrome Trace Event Format.
///
/// Converts the simulated DAG timeline into a series of "Complete" (`"ph": "X"`)
/// trace events. Each task is represented as a span with a start time (`ts`)
/// and duration (`dur`) in microseconds.
///
/// # Returns
/// A JSON `Value` representing the trace events array.
#[must_use]
pub fn export_chrome_trace(profile: &DagProfile) -> Value {
    let mut events = Vec::new();
    let mut start_times = HashMap::new();

    for event in &profile.timeline {
        match &event.kind {
            ProfilerEventKind::TaskStarted(idx, _) => {
                start_times.insert(*idx, event.time);
            }
            ProfilerEventKind::TaskCompleted(idx, name) => {
                if let Some(start_time) = start_times.remove(idx) {
                    let dur = event.time.saturating_sub(start_time);
                    events.push(json!({
                        "name": name,
                        "cat": "dag_task",
                        "ph": "X",
                        "ts": u64::try_from(start_time.as_micros()).unwrap_or(u64::MAX),
                        "dur": u64::try_from(dur.as_micros()).unwrap_or(u64::MAX),
                        "pid": 1,
                        "tid": 1,
                        "args": {
                            "task_index": idx
                        }
                    }));
                }
            }
        }
    }

    json!(events)
}

/// Exports a workflow `Timeline` to Chrome Trace Event Format.
///
/// Converts the reconstructed timeline of a workflow execution into a series of
/// "Complete" (`"ph": "X"`) trace events.
///
/// # Returns
/// A JSON `Value` representing the trace events array.
#[must_use]
pub fn export_timeline_chrome_trace(timeline: &Timeline) -> Value {
    let mut events = Vec::new();

    for (idx, step) in timeline.steps.iter().enumerate() {
        let name = step
            .name
            .clone()
            .unwrap_or_else(|| step.step_kind.as_str().to_string());
        let cat = step.step_kind.as_str();
        let ts = step.scheduled_at.timestamp_micros();
        let dur = step.total_ms * 1000;

        let mut args = serde_json::Map::new();
        args.insert("step_index".to_string(), json!(idx));
        if let Some(wait) = step.wait_ms {
            args.insert("wait_ms".to_string(), json!(wait));
        }
        if let Some(exec) = step.exec_ms {
            args.insert("exec_ms".to_string(), json!(exec));
        }
        if let Some(attempt) = step.attempt {
            args.insert("attempt".to_string(), json!(attempt));
        }
        let outcome_str = match step.outcome {
            StepOutcome::Completed => "Completed",
            StepOutcome::Failed => "Failed",
            StepOutcome::TimedOut => "TimedOut",
            StepOutcome::Cancelled => "Cancelled",
            StepOutcome::Fired => "Fired",
            StepOutcome::Pending => "Pending",
        };
        args.insert("outcome".to_string(), json!(outcome_str));

        events.push(json!({
            "name": name,
            "cat": cat,
            "ph": "X",
            "ts": ts,
            "dur": dur,
            "pid": 1,
            "tid": 1,
            "args": args
        }));
    }

    json!(events)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag_profiler::{ProfilerEvent, ProfilerEventKind};
    use std::time::Duration;

    #[test]
    fn test_export_timeline_chrome_trace() {
        use crate::timeline::{StepKind, StepOutcome, Timeline, TimelineRollup, TimelineStep};
        use chrono::TimeZone;

        let step1 = TimelineStep {
            step_kind: StepKind::Activity,
            name: Some("test_act".to_string()),
            scheduled_at: chrono::Utc.timestamp_opt(1000, 0).unwrap(),
            ended_at: Some(chrono::Utc.timestamp_opt(1001, 0).unwrap()),
            total_ms: 1000,
            wait_ms: Some(200),
            exec_ms: Some(800),
            attempt: Some(1),
            outcome: StepOutcome::Completed,
        };
        let tl = Timeline {
            exec_id: "exec-1".into(),
            workflow_id: "wf-1".into(),
            workflow_name: "my_wf".into(),
            state: "COMPLETED".into(),
            steps: vec![step1],
            rollup: TimelineRollup {
                total_wall_clock_ms: 1000,
                busy_ms: 800,
                wait_ms: 200,
                step_count_by_kind: std::collections::BTreeMap::new(),
                slowest_step: None,
            },
        };

        let trace = export_timeline_chrome_trace(&tl);
        let arr = trace.as_array().expect("Expected JSON array");
        assert_eq!(arr.len(), 1);

        let event = &arr[0];
        assert_eq!(event["name"], "test_act");
        assert_eq!(event["cat"], "activity");
        assert_eq!(event["ph"], "X");
        assert_eq!(event["ts"], 1_000_000_000);
        assert_eq!(event["dur"], 1_000_000);
        assert_eq!(event["args"]["wait_ms"], 200);
        assert_eq!(event["args"]["outcome"], "Completed");
    }

    #[test]
    fn test_export_chrome_trace() {
        let profile = DagProfile {
            total_duration: Duration::from_secs(5),
            peak_concurrency: 1,
            timeline: vec![
                ProfilerEvent {
                    time: Duration::from_secs(1),
                    kind: ProfilerEventKind::TaskStarted(0, "activity_a".to_string()),
                },
                ProfilerEvent {
                    time: Duration::from_secs(3),
                    kind: ProfilerEventKind::TaskCompleted(0, "activity_a".to_string()),
                },
            ],
        };

        let trace = export_chrome_trace(&profile);
        let arr = trace.as_array().expect("Expected JSON array");
        assert_eq!(arr.len(), 1);

        let event = &arr[0];
        assert_eq!(event["name"], "activity_a");
        assert_eq!(event["ph"], "X");
        assert_eq!(event["ts"], 1_000_000);
        assert_eq!(event["dur"], 2_000_000);
    }
}
