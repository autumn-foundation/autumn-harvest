#![cfg(feature = "testing")]

use autumn_harvest::dag::DagBuilder;
use autumn_harvest::dag_profiler::DagProfiler;
use autumn_harvest::dag_trace_export::export_chrome_trace;
use std::time::Duration;

const fn activity_a() {}
const fn activity_b() {}

#[test]
fn test_export_chrome_trace_integration() {
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

    assert_eq!(trace_events.len(), 4);
}
