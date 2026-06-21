//! Chrome Trace Event Exporter for DAG profiles.
//!
//! Visualizes DAG execution timelines by exporting a `DagProfile` to the
//! Chrome Trace Event format (viewable in Perfetto or `<chrome://tracing>`).
//! Critical path tasks are colored differently to highlight bottlenecks.

use crate::critical_path::CriticalPathResult;
use crate::dag_profiler::{DagProfile, ProfilerEventKind};
use serde_json::Value;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};

/// Export a DAG profile and critical path analysis to a Chrome Trace Event JSON array.
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn export_chrome_trace(profile: &DagProfile, critical_path: &CriticalPathResult) -> Value {
    let mut events = Vec::new();

    // Map each task index to a thread ID (tid) so concurrent tasks appear on different rows.
    let mut available_tids = BinaryHeap::new();
    // Default to a small pool of available TIDs
    for i in 0..profile.peak_concurrency {
        available_tids.push(Reverse(i as u32));
    }

    // We might need more if peak_concurrency calculation was conservative or zero.
    let mut next_tid = profile.peak_concurrency as u32;

    let mut active_tids = HashMap::new();

    for event in &profile.timeline {
        let ts_micros = event.time.as_micros() as u64;

        match &event.kind {
            ProfilerEventKind::TaskStarted(task_index, task_name) => {
                let tid = if let Some(Reverse(id)) = available_tids.pop() {
                    id
                } else {
                    let id = next_tid;
                    next_tid += 1;
                    id
                };

                active_tids.insert(*task_index, tid);

                let is_critical = critical_path.path_indices.contains(task_index);
                let cname = if is_critical { "bad" } else { "good" };

                events.push(serde_json::json!({
                    "name": task_name,
                    "cat": "dag_task",
                    "ph": "B",
                    "ts": ts_micros,
                    "pid": 1,
                    "tid": tid,
                    "cname": cname,
                    "args": {
                        "critical_path": is_critical
                    }
                }));
            }
            ProfilerEventKind::TaskCompleted(task_index, task_name) => {
                let tid = active_tids.remove(task_index).unwrap_or(0);
                available_tids.push(Reverse(tid));

                events.push(serde_json::json!({
                    "name": task_name,
                    "cat": "dag_task",
                    "ph": "E",
                    "ts": ts_micros,
                    "pid": 1,
                    "tid": tid,
                }));
            }
        }
    }

    serde_json::json!(events)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::critical_path::CriticalPathAnalyzer;
    use crate::dag::DagBuilder;
    use crate::dag_profiler::DagProfiler;
    use std::time::Duration;

    fn activity_a() {}
    fn activity_b() {}
    fn activity_c() {}
    fn activity_d() {}

    #[test]
    fn test_export_chrome_trace_coloring() {
        let mut builder = DagBuilder::new();
        let start = builder.activity(activity_a);
        let branch1 = builder.activity(activity_b).upstream(&start);
        let branch2 = builder.activity(activity_c).upstream(&start);
        let _end = builder
            .activity(activity_d)
            .upstream(&branch1)
            .upstream(&branch2);

        let dag = builder.build().unwrap();

        // branch2 (C) is longer, so A -> C -> D is the critical path.
        // branch1 (B) is not on the critical path.
        let profiler = DagProfiler::new(dag.clone())
            .mock_duration("activity_a", Duration::from_secs(1))
            .mock_duration("activity_b", Duration::from_secs(2))
            .mock_duration("activity_c", Duration::from_secs(5))
            .mock_duration("activity_d", Duration::from_secs(1));
        let profile = profiler.profile();

        let analyzer = CriticalPathAnalyzer::new(dag)
            .mock_duration("activity_a", Duration::from_secs(1))
            .mock_duration("activity_b", Duration::from_secs(2))
            .mock_duration("activity_c", Duration::from_secs(5))
            .mock_duration("activity_d", Duration::from_secs(1));
        let critical_path = analyzer.analyze();

        let trace = export_chrome_trace(&profile, &critical_path);
        let arr = trace.as_array().expect("Trace must be an array");
        assert!(!arr.is_empty(), "Trace array must not be empty");

        let mut a_found = false;
        let mut b_found = false;
        let mut c_found = false;

        for event in arr {
            if event["ph"] == "B" {
                let name = event["name"].as_str().unwrap();
                let cname = event["cname"].as_str().unwrap();

                if name == "activity_a" {
                    assert_eq!(cname, "bad", "activity_a is critical");
                    a_found = true;
                } else if name == "activity_b" {
                    assert_eq!(cname, "good", "activity_b is not critical");
                    b_found = true;
                } else if name == "activity_c" {
                    assert_eq!(cname, "bad", "activity_c is critical");
                    c_found = true;
                }
            }
        }

        assert!(a_found, "activity_a B event not found");
        assert!(b_found, "activity_b B event not found");
        assert!(c_found, "activity_c B event not found");
    }
}
