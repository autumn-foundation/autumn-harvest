//! Visualization exporters for DAG definitions.
//!
//! Provides utilities to export workflow DAGs (Directed Acyclic Graphs) into human-readable
//! and tool-compatible diagram formats such as Mermaid.js and Graphviz DOT.
//! This is useful for debugging, documentation, and visualizing dependencies.
use crate::dag::DagDefinition;
use std::fmt::Write;

/// Exports the DAG definition to a Mermaid.js flowchart.
///
/// # Examples
///
/// ```rust
/// use autumn_harvest::dag::DagBuilder;
/// use autumn_harvest::dag_export::export_mermaid;
///
/// fn my_activity() {}
/// fn my_other_activity() {}
///
/// let mut builder = DagBuilder::new();
/// let a = builder.activity(my_activity);
/// let b = builder.activity(my_other_activity).upstream(&a);
/// let dag = builder.build().unwrap();
///
/// let mermaid = export_mermaid(&dag).unwrap();
/// assert!(mermaid.contains("graph TD"));
/// assert!(mermaid.contains("-->"));
/// ```
///
/// # Errors
/// Returns `std::fmt::Error` if string formatting fails.
pub fn export_mermaid(dag: &DagDefinition) -> Result<String, std::fmt::Error> {
    let mut out = String::new();
    writeln!(out, "graph TD")?;

    let tasks = dag.tasks();

    for (i, task) in tasks.iter().enumerate() {
        writeln!(out, "    t{i}[\"{}\"]", task.activity_name)?;
    }

    for (i, task) in tasks.iter().enumerate() {
        for &upstream in &task.upstreams {
            writeln!(out, "    t{upstream} --> t{i}")?;
        }
    }

    Ok(out)
}

/// Exports the DAG definition to Graphviz DOT format.
///
/// # Examples
///
/// ```rust
/// use autumn_harvest::dag::DagBuilder;
/// use autumn_harvest::dag_export::export_dot;
///
/// fn my_activity() {}
/// fn my_other_activity() {}
///
/// let mut builder = DagBuilder::new();
/// let a = builder.activity(my_activity);
/// let b = builder.activity(my_other_activity).upstream(&a);
/// let dag = builder.build().unwrap();
///
/// let dot = export_dot(&dag).unwrap();
/// assert!(dot.contains("digraph DAG {"));
/// assert!(dot.contains("->"));
/// ```
///
/// # Errors
/// Returns `std::fmt::Error` if string formatting fails.
pub fn export_dot(dag: &DagDefinition) -> Result<String, std::fmt::Error> {
    let mut out = String::new();
    writeln!(out, "digraph DAG {{")?;

    let tasks = dag.tasks();

    for (i, task) in tasks.iter().enumerate() {
        writeln!(out, "    t{i} [label=\"{}\"];", task.activity_name)?;
    }

    for (i, task) in tasks.iter().enumerate() {
        for &upstream in &task.upstreams {
            writeln!(out, "    t{upstream} -> t{i};")?;
        }
    }

    writeln!(out, "}}")?;
    Ok(out)
}

/// Exports a `DagProfile` to the Chrome Trace Event Format.
///
/// This JSON output can be loaded into `chrome://tracing` or Perfetto UI
/// to visualize the timeline and concurrency of the DAG execution.
///
/// # Errors
/// Returns `serde_json::Error` if serialization fails.
#[cfg(feature = "testing")]
pub fn export_chrome_trace(
    profile: &crate::dag_profiler::DagProfile,
) -> Result<String, serde_json::Error> {
    use crate::dag_profiler::ProfilerEventKind;
    use serde_json::json;

    let mut events = Vec::new();

    for event in &profile.timeline {
        let (ph, tid, name) = match &event.kind {
            ProfilerEventKind::TaskStarted(idx, name) => ("B", *idx, name),
            ProfilerEventKind::TaskCompleted(idx, name) => ("E", *idx, name),
        };

        #[allow(clippy::cast_possible_truncation)]
        let ts_micros = event.time.as_micros() as u64;

        events.push(json!({
            "name": name,
            "cat": "task",
            "ph": ph,
            "ts": ts_micros,
            "pid": 1,
            "tid": tid,
            "args": {}
        }));
    }

    serde_json::to_string(&events)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::DagBuilder;

    #[cfg(feature = "testing")]
    use crate::dag_profiler::DagProfiler;
    #[cfg(feature = "testing")]
    use std::time::Duration;

    fn dummy_activity() {}
    fn dummy_activity2() {}
    fn dummy_activity3() {}

    #[test]
    fn test_export_empty_dag() {
        let builder = DagBuilder::new();
        let dag = builder.build().unwrap();

        let mermaid = export_mermaid(&dag).unwrap();
        assert_eq!(mermaid, "graph TD\n");

        let dot = export_dot(&dag).unwrap();
        assert_eq!(dot, "digraph DAG {\n}\n");
    }

    #[test]
    fn test_export_simple_dag() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity);
        let b1 = builder.activity(dummy_activity2).upstream(&a);
        let b2 = builder.activity(dummy_activity2).upstream(&a);
        let _c = builder
            .activity(dummy_activity3)
            .upstream(&b1)
            .upstream(&b2);

        let dag = builder.build().unwrap();

        let mermaid = export_mermaid(&dag).unwrap();
        let expected_mermaid = "\
graph TD
    t0[\"dummy_activity\"]
    t1[\"dummy_activity2\"]
    t2[\"dummy_activity2\"]
    t3[\"dummy_activity3\"]
    t0 --> t1
    t0 --> t2
    t1 --> t3
    t2 --> t3
";
        assert_eq!(mermaid, expected_mermaid);

        let dot = export_dot(&dag).unwrap();
        let expected_dot = "\
digraph DAG {
    t0 [label=\"dummy_activity\"];
    t1 [label=\"dummy_activity2\"];
    t2 [label=\"dummy_activity2\"];
    t3 [label=\"dummy_activity3\"];
    t0 -> t1;
    t0 -> t2;
    t1 -> t3;
    t2 -> t3;
}
";
        assert_eq!(dot, expected_dot);
    }

    #[cfg(feature = "testing")]
    #[test]
    fn test_export_chrome_trace() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity);
        let _b = builder.activity(dummy_activity2).upstream(&a);
        let dag = builder.build().unwrap();

        let profiler = DagProfiler::new(dag)
            .mock_duration("dummy_activity", Duration::from_secs(2))
            .mock_duration("dummy_activity2", Duration::from_secs(3));

        let profile = profiler.profile();

        let trace_json = export_chrome_trace(&profile).unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&trace_json).unwrap();
        let events = parsed.as_array().expect("Expected JSON array");
        assert_eq!(events.len(), 4);

        let first_event = &events[0];
        assert_eq!(first_event["name"], "dummy_activity");
        assert_eq!(first_event["ph"], "B");
        assert_eq!(first_event["ts"], 0);

        let last_event = &events[3];
        assert_eq!(last_event["name"], "dummy_activity2");
        assert_eq!(last_event["ph"], "E");
        assert_eq!(last_event["ts"], 5_000_000);
    }
}
