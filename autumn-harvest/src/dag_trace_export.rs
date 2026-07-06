//! Exporter for DAG profiles to Chrome Trace Event format.
//!
//! Provides utilities to export a [`DagProfile`] into JSON that can be loaded
//! into Chrome Tracing (`chrome://tracing`) or Perfetto for advanced performance
//! visualization and debugging.

use crate::dag_profiler::{DagProfile, ProfilerEventKind};
use serde_json::{Value, json};

/// Exports a `DagProfile` to a Chrome Trace Event format JSON string.
///
/// This allows you to visualize the simulated execution timeline, peak concurrency,
/// and critical path using standard performance analysis tools like Perfetto.
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
/// let a = builder.activity(my_activity);
/// let dag = builder.build().unwrap();
///
/// let profiler = DagProfiler::new(dag).mock_duration("my_activity", Duration::from_millis(10));
/// let profile = profiler.profile();
///
/// let trace_json = export_chrome_trace(&profile).unwrap();
/// assert!(trace_json.contains("my_activity"));
/// ```
///
///
/// # Panics
///
/// Panics if the event time exceeds the bounds of `u64` microseconds.
/// # Errors
/// Returns `serde_json::Error` if serialization fails.
pub fn export_chrome_trace(profile: &DagProfile) -> Result<String, serde_json::Error> {
    let mut events: Vec<Value> = Vec::new();

    for event in &profile.timeline {
        let ts_micros = u64::try_from(event.time.as_micros()).unwrap();

        match &event.kind {
            ProfilerEventKind::TaskStarted(idx, name) => {
                events.push(json!({
                    "name": name,
                    "cat": "activity",
                    "ph": "B",
                    "pid": 1,
                    "tid": *idx,
                    "ts": ts_micros,
                }));
            }
            ProfilerEventKind::TaskCompleted(idx, name) => {
                events.push(json!({
                    "name": name,
                    "cat": "activity",
                    "ph": "E",
                    "pid": 1,
                    "tid": *idx,
                    "ts": ts_micros,
                }));
            }
        }
    }

    serde_json::to_string_pretty(&events)
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
            .mock_duration("activity_a", Duration::from_millis(50))
            .mock_duration("activity_b", Duration::from_millis(100));

        let profile = profiler.profile();
        let json_str = export_chrome_trace(&profile).unwrap();

        let parsed: Vec<Value> = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed.len(), 4); // 2 tasks * 2 events (Begin/End)

        assert_eq!(parsed[0]["ph"], "B");
        assert_eq!(parsed[0]["name"], "activity_a");
        assert_eq!(parsed[0]["ts"], 0);

        assert_eq!(parsed[1]["ph"], "E");
        assert_eq!(parsed[1]["name"], "activity_a");
        assert_eq!(parsed[1]["ts"], 50000); // 50ms = 50,000us
    }
}
