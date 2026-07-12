//! Chrome Trace Event Format exporter for DAG profiles.
//!
//! Converts a `DagProfile` into a JSON structure compatible with trace viewers
//! like `chrome://tracing` or `ui.perfetto.dev`.

use crate::dag_profiler::{DagProfile, ProfilerEventKind};
use crate::timeline::Timeline;
use serde_json::{Value, json};
use std::collections::HashMap;

/// Exports a `Timeline` to Chrome Trace Event Format.
///
/// Converts a real workflow execution timeline into a series of "Complete" (`"ph": "X"`)
/// trace events. Each step is represented as a span with a start time (`ts`)
/// and duration (`dur`) in microseconds, relative to the first recorded step's scheduling time.
///
/// # Returns
/// A JSON `Value` representing the trace events array.
#[must_use]
pub fn export_timeline_chrome_trace(timeline: &Timeline) -> Value {
    let mut events = Vec::new();

    // Find the earliest timestamp to use as relative zero for tracing.
    let base_ts = timeline.steps.iter().map(|s| s.scheduled_at).min();

    if let Some(base_ts) = base_ts {
        for (idx, step) in timeline.steps.iter().enumerate() {
            let relative_ts = step.scheduled_at.signed_duration_since(base_ts);
            // Ignore events scheduled before the base (shouldn't happen with min).
            if relative_ts.num_microseconds().unwrap_or(-1) >= 0 {
                let name = step.name.as_ref().map_or_else(|| format!("{:?}", step.step_kind), Clone::clone);

                events.push(json!({
                    "name": name,
                    "cat": format!("{:?}", step.step_kind),
                    "ph": "X",
                    "ts": relative_ts.num_microseconds().unwrap_or(0),
                    "dur": step.total_ms * 1000,
                    "pid": timeline.exec_id,
                    "tid": timeline.workflow_name,
                    "args": {
                        "step_index": idx,
                        "outcome": format!("{:?}", step.outcome),
                    }
                }));
            }
        }
    }

    json!(events)
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag_profiler::{ProfilerEvent, ProfilerEventKind};
    use crate::timeline::{StepKind, StepOutcome, TimelineRollup, TimelineStep};
    use chrono::{TimeZone, Utc};
    use std::collections::BTreeMap;
    use std::time::Duration;

    #[test]
    fn test_export_timeline_chrome_trace() {
        let ts_base = Utc.timestamp_opt(1_600_000_000, 0).unwrap();
        let ts_offset1 = Utc.timestamp_opt(1_600_000_001, 500_000_000).unwrap();

        let timeline = Timeline {
            exec_id: "exec-1".to_string(),
            workflow_id: "wf-1".to_string(),
            workflow_name: "MyWorkflow".to_string(),
            state: "COMPLETED".to_string(),
            steps: vec![
                TimelineStep {
                    step_kind: StepKind::Activity,
                    name: Some("activity_1".to_string()),
                    scheduled_at: ts_base,
                    ended_at: None,
                    total_ms: 1000,
                    wait_ms: None,
                    exec_ms: None,
                    outcome: StepOutcome::Completed,
                    attempt: None,
                },
                TimelineStep {
                    step_kind: StepKind::Timer,
                    name: Some("timer_1".to_string()),
                    scheduled_at: ts_offset1,
                    ended_at: None,
                    total_ms: 500,
                    wait_ms: None,
                    exec_ms: None,
                    outcome: StepOutcome::Completed,
                    attempt: None,
                },
            ],
            rollup: TimelineRollup {
                total_wall_clock_ms: 1500,
                busy_ms: 1000,
                wait_ms: 500,
                slowest_step: None,
                step_count_by_kind: BTreeMap::new(),
            },
        };

        let trace = export_timeline_chrome_trace(&timeline);
        let arr = trace.as_array().expect("Expected JSON array");
        assert_eq!(arr.len(), 2);

        let event1 = &arr[0];
        assert_eq!(event1["name"], "activity_1");
        assert_eq!(event1["cat"], "Activity");
        assert_eq!(event1["ph"], "X");
        assert_eq!(event1["ts"], 0);
        assert_eq!(event1["dur"], 1_000_000);
        assert_eq!(event1["pid"], "exec-1");
        assert_eq!(event1["tid"], "MyWorkflow");
        assert_eq!(event1["args"]["step_index"], 0);

        let event2 = &arr[1];
        assert_eq!(event2["name"], "timer_1");
        assert_eq!(event2["cat"], "Timer");
        assert_eq!(event2["ph"], "X");
        assert_eq!(event2["ts"], 1_500_000); // 1.5 seconds later
        assert_eq!(event2["dur"], 500_000);
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
