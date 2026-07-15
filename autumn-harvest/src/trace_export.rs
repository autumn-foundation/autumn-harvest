//! Chrome Trace Event Format exporter for DAG profiles and Timelines.
//!
//! Converts a `DagProfile` or a `Timeline` into a JSON structure compatible with trace viewers
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

/// Exports a historical `Timeline` to Chrome Trace Event Format.
///
/// Converts a real workflow execution timeline into an array of trace events.
/// Each timeline step is represented as a span (`"ph": "X"`), with activities
/// split into 'Queue' and 'Execution' segments if queue-wait splits are available.
///
/// # Returns
/// A JSON `Value` representing the trace events array.
#[must_use]
pub fn export_timeline_trace(timeline: &Timeline) -> Value {
    let mut events = Vec::new();

    for step in &timeline.steps {
        let start_ts = step.scheduled_at.timestamp_micros();
        let name = step
            .name
            .clone()
            .unwrap_or_else(|| step.step_kind.as_str().to_string());
        let cat = step.step_kind.as_str();

        let outcome_val = serde_json::to_value(step.outcome).unwrap_or_else(|_| json!("unknown"));

        if let (Some(wait_ms), Some(exec_ms)) = (step.wait_ms, step.exec_ms) {
            events.push(json!({
                "name": format!("Wait: {name}"),
                "cat": cat,
                "ph": "X",
                "ts": start_ts,
                "dur": wait_ms * 1000,
                "pid": timeline.workflow_id,
                "tid": "Queue",
                "args": { "outcome": outcome_val.clone() }
            }));

            events.push(json!({
                "name": name,
                "cat": cat,
                "ph": "X",
                "ts": start_ts + (wait_ms * 1000),
                "dur": exec_ms * 1000,
                "pid": timeline.workflow_id,
                "tid": cat,
                "args": {
                    "outcome": outcome_val,
                    "attempt": step.attempt
                }
            }));
        } else {
            events.push(json!({
                "name": name,
                "cat": cat,
                "ph": "X",
                "ts": start_ts,
                "dur": step.total_ms * 1000,
                "pid": timeline.workflow_id,
                "tid": cat,
                "args": {
                    "outcome": outcome_val
                }
            }));
        }
    }

    json!(events)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag_profiler::{DagProfile, ProfilerEvent, ProfilerEventKind};
    use crate::timeline::{StepKind, StepOutcome, Timeline, TimelineRollup, TimelineStep};
    use chrono::TimeZone;
    use std::collections::BTreeMap;
    use std::time::Duration;

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

    #[test]
    fn test_export_timeline_trace() {
        // A fixed time so timestamp_micros is deterministic.
        let now = chrono::Utc.timestamp_opt(1_600_000_000, 0).unwrap();

        let timeline = Timeline {
            exec_id: "exec-1".to_string(),
            workflow_id: "wf-1".to_string(),
            workflow_name: "test_wf".to_string(),
            state: "COMPLETED".to_string(),
            steps: vec![TimelineStep {
                step_kind: StepKind::Activity,
                name: Some("activity_a".to_string()),
                scheduled_at: now,
                ended_at: Some(now + chrono::Duration::milliseconds(150)),
                total_ms: 150,
                wait_ms: Some(50),
                exec_ms: Some(100),
                outcome: StepOutcome::Completed,
                attempt: Some(1),
            }],
            rollup: TimelineRollup {
                total_wall_clock_ms: 150,
                busy_ms: 100,
                wait_ms: 50,
                slowest_step: None,
                step_count_by_kind: BTreeMap::new(),
            },
        };

        let trace = export_timeline_trace(&timeline);
        let arr = trace.as_array().expect("Expected JSON array");
        assert_eq!(arr.len(), 2);

        let wait_event = &arr[0];
        assert_eq!(wait_event["name"], "Wait: activity_a");
        assert_eq!(wait_event["dur"], 50_000);
        assert_eq!(wait_event["tid"], "Queue");
        assert_eq!(wait_event["pid"], "wf-1");

        let exec_event = &arr[1];
        assert_eq!(exec_event["name"], "activity_a");
        assert_eq!(exec_event["dur"], 100_000);
        assert_eq!(exec_event["tid"], "activity");
    }
}
