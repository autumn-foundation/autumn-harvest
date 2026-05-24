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

/// Exports a DAG execution profile to Google Trace Event Format (Chrome Tracing).
///
/// This format can be loaded in `chrome://tracing` or `https://ui.perfetto.dev/`
/// to visualize the execution timeline of the DAG in a Gantt chart.
///
/// # Errors
/// Returns `std::fmt::Error` if string formatting fails.
#[cfg(feature = "testing")]
pub fn export_chrome_trace(
    profile: &crate::dag_profiler::DagProfile,
) -> Result<String, std::fmt::Error> {
    use crate::dag_profiler::ProfilerEventKind;

    let mut events = Vec::new();

    for event in &profile.timeline {
        let ts_micros = event.time.as_micros();

        let (task_idx, name, ph) = match &event.kind {
            ProfilerEventKind::TaskStarted(idx, name) => (*idx, name, "B"),
            ProfilerEventKind::TaskCompleted(idx, name) => (*idx, name, "E"),
        };

        // We assign each task a unique thread ID (`tid`) matching its index.
        // This ensures tasks render on separate lanes in the trace viewer.
        let json_event = format!(
            r#"{{"name":"{name}","cat":"task","ph":"{ph}","pid":1,"tid":{task_idx},"ts":{ts_micros}}}"#
        );
        events.push(json_event);
    }

    let mut out = String::new();
    writeln!(out, "[")?;
    if !events.is_empty() {
        writeln!(out, "  {}", events.join(",\n  "))?;
    }
    writeln!(out, "]")?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::DagBuilder;

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

    #[test]
    #[cfg(feature = "testing")]
    fn test_export_chrome_trace() {
        use crate::dag_profiler::{DagProfile, ProfilerEvent, ProfilerEventKind};
        use std::time::Duration;

        let profile = DagProfile {
            total_duration: Duration::from_secs(1),
            peak_concurrency: 1,
            timeline: vec![
                ProfilerEvent {
                    time: Duration::from_secs(0),
                    kind: ProfilerEventKind::TaskStarted(0, "task_a".to_string()),
                },
                ProfilerEvent {
                    time: Duration::from_secs(1),
                    kind: ProfilerEventKind::TaskCompleted(0, "task_a".to_string()),
                },
            ],
        };

        let trace = export_chrome_trace(&profile).unwrap();
        assert!(trace.starts_with('['));
        assert!(trace.contains(r#""name":"task_a""#));
        assert!(trace.contains(r#""ph":"B""#));
        assert!(trace.contains(r#""ph":"E""#));
        assert!(trace.contains(r#""tid":0"#));
        assert!(trace.contains(r#""ts":0"#));
        assert!(trace.contains(r#""ts":1000000"#));
        assert!(trace.ends_with("]\n"));
    }
}
