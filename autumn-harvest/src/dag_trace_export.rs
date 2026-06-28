use crate::dag_profiler::{DagProfile, ProfilerEventKind};

/// Export a DAG execution profile to the Chrome Trace Event format (JSON).
///
/// This generates a JSON array of events that can be loaded into
/// <chrome://tracing> or <https://ui.perfetto.dev> for timeline visualization.
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
/// let profile = DagProfiler::new(dag)
///     .mock_duration("my_activity", Duration::from_secs(1))
///     .profile();
///
/// let trace = export_chrome_trace(&profile);
/// assert!(trace.contains("my_activity"));
/// ```
#[must_use]
pub fn export_chrome_trace(profile: &DagProfile) -> String {
    use std::collections::HashMap;
    let mut events = Vec::new();
    let mut starts = HashMap::new();

    for event in &profile.timeline {
        match &event.kind {
            ProfilerEventKind::TaskStarted(id, name) => {
                starts.insert(*id, (name.clone(), event.time.as_micros()));
            }
            ProfilerEventKind::TaskCompleted(id, _name) => {
                if let Some((name, ts)) = starts.remove(id) {
                    let dur = event.time.as_micros().saturating_sub(ts);
                    events.push(serde_json::json!({
                        "name": name,
                        "ph": "X",
                        "ts": ts,
                        "dur": dur,
                        "pid": 1,
                        "tid": 1
                    }));
                }
            }
        }
    }

    serde_json::to_string(&events).unwrap_or_else(|_| "[]".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::DagBuilder;
    use crate::dag_profiler::DagProfiler;
    use serde_json::Value;
    use std::time::Duration;

    fn test_activity_1() {}
    fn test_activity_2() {}

    #[test]
    fn test_export_chrome_trace() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(test_activity_1);
        let _b = builder.activity(test_activity_2).upstream(&a);
        let dag = builder.build().unwrap();

        let profile = DagProfiler::new(dag)
            .mock_duration("test_activity_1", Duration::from_micros(100))
            .mock_duration("test_activity_2", Duration::from_micros(200))
            .profile();

        let trace_json = export_chrome_trace(&profile);
        let trace: Vec<Value> =
            serde_json::from_str(&trace_json).expect("should produce valid JSON");

        assert!(!trace.is_empty(), "trace should not be empty");

        let first_event = &trace[0];
        assert_eq!(first_event["name"], "test_activity_1");
        assert_eq!(first_event["ph"], "X"); // Complete event
        assert_eq!(first_event["ts"], 0);
        assert_eq!(first_event["dur"], 100);
    }
}
