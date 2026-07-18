//! Chrome Trace Event Format exporter for DAG profiles.
//!
//! Converts a `DagProfile` into a JSON structure compatible with trace viewers
//! like `chrome://tracing` or `ui.perfetto.dev`.

#[cfg(any(test, feature = "testing"))]
use crate::dag_profiler::{DagProfile, ProfilerEventKind};
use crate::timeline::Timeline;
use serde_json::{Value, json};
#[cfg(any(test, feature = "testing"))]
use std::collections::HashMap;

/// Exports a `DagProfile` to Chrome Trace Event Format.
///
/// Converts the simulated DAG timeline into a series of "Complete" (`"ph": "X"`)
/// trace events. Each task is represented as a span with a start time (`ts`)
/// and duration (`dur`) in microseconds.
///
/// # Returns
/// A JSON `Value` representing the trace events array.
#[cfg(any(test, feature = "testing"))]
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

#[must_use]
pub fn export_timeline_chrome_trace(timeline: &Timeline) -> Value {
    let mut events = Vec::new();

    for (idx, step) in timeline.steps.iter().enumerate() {
        let name = step
            .name
            .clone()
            .unwrap_or_else(|| step.step_kind.as_str().to_string());
        let ts_micros = step.scheduled_at.timestamp_micros();
        // total_ms could be negative if clocks are weird, but clamped >= 0 in timeline
        let dur_micros = step.total_ms.max(0) * 1000;

        let mut args = serde_json::Map::new();
        args.insert("step_index".to_string(), json!(idx));
        args.insert("outcome".to_string(), json!(step.outcome));
        if let Some(attempt) = step.attempt {
            args.insert("attempt".to_string(), json!(attempt));
        }
        if let Some(wait_ms) = step.wait_ms {
            args.insert("wait_ms".to_string(), json!(wait_ms));
        }
        if let Some(exec_ms) = step.exec_ms {
            args.insert("exec_ms".to_string(), json!(exec_ms));
        }

        events.push(json!({
            "name": name,
            "cat": step.step_kind.as_str(),
            "ph": "X",
            "ts": ts_micros,
            "dur": dur_micros,
            "pid": timeline.exec_id,
            "tid": step.step_kind.as_str(),
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

    use crate::timeline::{StepKind, StepOutcome, Timeline, TimelineRollup, TimelineStep};
    use chrono::Utc;

    #[test]
    fn test_export_timeline_chrome_trace() {
        let now = Utc::now();
        let step = TimelineStep {
            step_kind: StepKind::Activity,
            name: Some("test_activity".to_string()),
            scheduled_at: now,
            ended_at: Some(now + std::time::Duration::from_millis(150)),
            total_ms: 150,
            wait_ms: Some(50),
            exec_ms: Some(100),
            outcome: StepOutcome::Completed,
            attempt: Some(1),
        };

        let timeline = Timeline {
            exec_id: "exec-1".to_string(),
            workflow_id: "wf-1".to_string(),
            workflow_name: "TestWorkflow".to_string(),
            state: "COMPLETED".to_string(),
            steps: vec![step],
            rollup: TimelineRollup {
                total_wall_clock_ms: 150,
                busy_ms: 100,
                wait_ms: 50,
                slowest_step: None,
                step_count_by_kind: std::collections::BTreeMap::default(),
            },
        };

        let trace = export_timeline_chrome_trace(&timeline);
        let arr = trace.as_array().expect("Expected JSON array");
        assert_eq!(arr.len(), 1);

        let event = &arr[0];
        assert_eq!(event["name"], "test_activity");
        assert_eq!(event["cat"], "activity");
        assert_eq!(event["ph"], "X");
        assert_eq!(event["ts"], now.timestamp_micros());
        assert_eq!(event["dur"], 150_000);
        assert_eq!(event["pid"], "exec-1");
        assert_eq!(event["tid"], "activity");
        assert_eq!(event["args"]["step_index"], 0);
        assert_eq!(event["args"]["outcome"], "completed");
        assert_eq!(event["args"]["attempt"], 1);
        assert_eq!(event["args"]["wait_ms"], 50);
        assert_eq!(event["args"]["exec_ms"], 100);
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
