//! Chrome Trace Event exporter for DAG execution profiles.
//!
//! Converts a `DagProfile` timeline into the Chrome Trace Event JSON format.
//! The resulting JSON can be loaded into `chrome://tracing` or `https://ui.perfetto.dev/`
//! to visually inspect task concurrency, duration, and bottlenecks.

use crate::dag_profiler::{DagProfile, ProfilerEventKind};
use serde::Serialize;

#[derive(Serialize)]
struct TraceEvent {
    name: String,
    cat: &'static str,
    ph: &'static str,
    pid: u32,
    tid: usize,
    ts: u128,
}

#[derive(Serialize)]
struct TraceFormat {
    #[serde(rename = "traceEvents")]
    trace_events: Vec<TraceEvent>,
    #[serde(rename = "displayTimeUnit")]
    display_time_unit: &'static str,
}

/// Exports a `DagProfile` to a Chrome Trace Event JSON string.
///
/// The exported JSON represents the timeline of the simulated DAG execution.
/// You can save this string to a file (e.g., `trace.json`) and open it in
/// `chrome://tracing` or `https://ui.perfetto.dev/`.
///
/// # Examples
///
/// ```rust
/// use autumn_harvest::dag::DagBuilder;
/// use autumn_harvest::dag_profiler::DagProfiler;
/// use autumn_harvest::dag_trace_export::export_chrome_trace;
/// use std::time::Duration;
///
/// fn my_activity() {}
///
/// let mut builder = DagBuilder::new();
/// let _a = builder.activity(my_activity);
/// let dag = builder.build().unwrap();
///
/// let profiler = DagProfiler::new(dag)
///     .mock_duration("my_activity", Duration::from_millis(500));
/// let profile = profiler.profile();
///
/// let trace_json = export_chrome_trace(&profile).unwrap();
/// assert!(trace_json.contains("my_activity"));
/// assert!(trace_json.contains("\"ph\":\"B\""));
/// ```
///
/// # Errors
/// Returns `serde_json::Error` if serialization fails.
pub fn export_chrome_trace(profile: &DagProfile) -> Result<String, serde_json::Error> {
    let mut events = Vec::new();

    for event in &profile.timeline {
        let (ph, task_id, name) = match &event.kind {
            ProfilerEventKind::TaskStarted(id, name) => ("B", *id, name.clone()),
            ProfilerEventKind::TaskCompleted(id, name) => ("E", *id, name.clone()),
        };

        events.push(TraceEvent {
            name,
            cat: "dag_task",
            ph,
            pid: 1,
            // Assigning thread ID equal to task ID so they don't incorrectly stack
            // and fail to render if they overlap in weird ways, though if we want
            // to show them densely, we might map them to available worker threads.
            // Using task_id ensures each task gets its own row in the basic view.
            tid: task_id,
            ts: event.time.as_micros(),
        });
    }

    let trace = TraceFormat {
        trace_events: events,
        display_time_unit: "ms",
    };

    serde_json::to_string(&trace)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::DagBuilder;
    use crate::dag_profiler::DagProfiler;
    use std::time::Duration;

    fn activity_a() {}
    fn activity_b() {}

    #[test]
    fn test_export_chrome_trace() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(activity_a);
        let _b = builder.activity(activity_b).upstream(&a);
        let dag = builder.build().unwrap();

        let profiler = DagProfiler::new(dag)
            .mock_duration("activity_a", Duration::from_millis(1))
            .mock_duration("activity_b", Duration::from_millis(2));
        let profile = profiler.profile();

        let trace = export_chrome_trace(&profile).unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&trace).unwrap();
        let trace_events = parsed["traceEvents"].as_array().unwrap();

        assert_eq!(trace_events.len(), 4); // A start, A end, B start, B end

        // Check first event
        let event_a_start = &trace_events[0];
        assert_eq!(event_a_start["name"], "activity_a");
        assert_eq!(event_a_start["ph"], "B");
        assert_eq!(event_a_start["ts"], 0);

        // Check last event
        let event_b_end = &trace_events[3];
        assert_eq!(event_b_end["name"], "activity_b");
        assert_eq!(event_b_end["ph"], "E");
        assert_eq!(event_b_end["ts"], 3000);
    }
}
