//! Chrome Trace Event exporter for DAG profiles.
//!
//! Provides utilities to export a [`DagProfile`] into the Chrome Trace Event format (JSON),
//! which can be visualized in tools like `chrome://tracing` or Perfetto.

use crate::dag_profiler::{DagProfile, ProfilerEventKind};
use serde_json::{Value, json};
use std::collections::HashMap;

/// Exports a [`DagProfile`] into a Chrome Trace Event Format JSON string.
///
/// Converts the start and end events of a DAG execution profile into a series
/// of Complete Events (`"ph": "X"`), packing non-overlapping tasks into shared
/// "lanes" (represented as thread IDs, `tid`) for a compact visualization.
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
/// use autumn_harvest::dag::DagBuilder;
/// use autumn_harvest::dag_profiler::DagProfiler;
/// use autumn_harvest::dag_trace::export_chrome_trace;
///
/// fn my_activity() {}
///
/// let mut builder = DagBuilder::new();
/// let _a = builder.activity(my_activity);
/// let dag = builder.build().unwrap();
///
/// let profile = DagProfiler::new(dag)
///     .mock_duration("my_activity", Duration::from_secs(2))
///     .profile();
///
/// let json_trace = export_chrome_trace(&profile).unwrap();
/// assert!(json_trace.contains("\"ph\":\"X\""));
/// ```
///
/// # Errors
/// Returns `serde_json::Error` if serialization fails.
pub fn export_chrome_trace(profile: &DagProfile) -> Result<String, serde_json::Error> {
    let mut trace_events: Vec<Value> = Vec::new();

    // Track in-flight tasks: task_index -> (start_time_micros, lane_id)
    let mut in_flight: HashMap<usize, (u128, usize)> = HashMap::new();
    // Track lane availability: lane_id -> busy
    let mut lanes: Vec<bool> = Vec::new();

    for event in &profile.timeline {
        let time_micros = event.time.as_micros();

        match &event.kind {
            ProfilerEventKind::TaskStarted(task_index, _) => {
                // Find first available lane
                let mut assigned_lane = None;
                for (lane_id, is_busy) in lanes.iter_mut().enumerate() {
                    if !*is_busy {
                        *is_busy = true;
                        assigned_lane = Some(lane_id);
                        break;
                    }
                }

                // Or create a new lane
                let lane_id = assigned_lane.unwrap_or_else(|| {
                    let new_id = lanes.len();
                    lanes.push(true);
                    new_id
                });

                in_flight.insert(*task_index, (time_micros, lane_id));
            }
            ProfilerEventKind::TaskCompleted(task_index, task_name) => {
                if let Some((start_micros, lane_id)) = in_flight.remove(task_index) {
                    // Free the lane
                    lanes[lane_id] = false;

                    let duration_micros = time_micros.saturating_sub(start_micros);

                    // Add Complete event (ph: "X")
                    trace_events.push(json!({
                        "name": task_name,
                        "cat": "activity",
                        "ph": "X",
                        "ts": start_micros,
                        "dur": duration_micros,
                        "pid": 1,
                        "tid": lane_id,
                        "args": {
                            "task_index": task_index
                        }
                    }));
                }
            }
        }
    }

    let trace_document = json!({
        "traceEvents": trace_events,
        "displayTimeUnit": "ms"
    });

    serde_json::to_string(&trace_document)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::DagBuilder;
    use crate::dag_profiler::DagProfiler;
    use serde_json::Value;
    use std::time::Duration;

    fn act_a() {}
    fn act_b() {}
    fn act_c() {}

    #[test]
    fn test_export_linear_trace() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(act_a);
        let _b = builder.activity(act_b).upstream(&a);
        let dag = builder.build().unwrap();

        let profile = DagProfiler::new(dag)
            .mock_duration("act_a", Duration::from_millis(100))
            .mock_duration("act_b", Duration::from_millis(50))
            .profile();

        let json_str = export_chrome_trace(&profile).unwrap();
        let parsed: Value = serde_json::from_str(&json_str).unwrap();

        let events = parsed["traceEvents"].as_array().unwrap();
        assert_eq!(events.len(), 2);

        // Linear execution means they should both use tid (lane) 0 since they don't overlap
        let e1 = &events[0];
        assert_eq!(e1["name"], "act_a");
        assert_eq!(e1["tid"], 0);
        assert_eq!(e1["ts"], 0);
        assert_eq!(e1["dur"], 100_000);

        let e2 = &events[1];
        assert_eq!(e2["name"], "act_b");
        assert_eq!(e2["tid"], 0);
        assert_eq!(e2["ts"], 100_000);
        assert_eq!(e2["dur"], 50_000);
    }

    #[test]
    fn test_export_parallel_trace() {
        let mut builder = DagBuilder::new();
        let start = builder.activity(act_a);
        let _b = builder.activity(act_b).upstream(&start);
        let _c = builder.activity(act_c).upstream(&start);
        let dag = builder.build().unwrap();

        let profile = DagProfiler::new(dag)
            .mock_duration("act_a", Duration::from_millis(10))
            .mock_duration("act_b", Duration::from_millis(50))
            .mock_duration("act_c", Duration::from_millis(60))
            .profile();

        let json_str = export_chrome_trace(&profile).unwrap();
        let parsed: Value = serde_json::from_str(&json_str).unwrap();
        let events = parsed["traceEvents"].as_array().unwrap();

        assert_eq!(events.len(), 3);

        let mut tids: Vec<u64> = events.iter().map(|e| e["tid"].as_u64().unwrap()).collect();
        tids.sort_unstable();
        tids.dedup();
        // Since `act_b` and `act_c` run in parallel, we need 2 lanes.
        assert_eq!(tids, vec![0, 1]);
    }
}
