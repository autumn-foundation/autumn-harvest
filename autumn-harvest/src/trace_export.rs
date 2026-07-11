//! Chrome Trace Event Format exporter for DAG profiles.
//!
//! Converts a `DagProfile` into a JSON structure compatible with trace viewers
//! like `chrome://tracing` or `ui.perfetto.dev`.

use crate::dag_profiler::{DagProfile, ProfilerEventKind};
use serde_json::{Value, json};
use std::collections::HashMap;

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
    let mut events = Vec::with_capacity(profile.timeline.len());
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
}
