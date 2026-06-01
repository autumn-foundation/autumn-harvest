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

/// Exports the DAG execution profile to Chrome Trace Event format.
///
/// # Errors
/// Returns `std::fmt::Error` if string formatting fails.
#[cfg(feature = "testing")]
pub fn export_chrome_trace(
    profile: &crate::dag_profiler::DagProfile,
) -> Result<String, std::fmt::Error> {
    let mut events = Vec::new();
    let mut starts: std::collections::HashMap<usize, (std::time::Duration, String)> =
        std::collections::HashMap::new();

    for event in &profile.timeline {
        match &event.kind {
            crate::dag_profiler::ProfilerEventKind::TaskStarted(id, name) => {
                starts.insert(*id, (event.time, name.clone()));
            }
            crate::dag_profiler::ProfilerEventKind::TaskCompleted(id, _) => {
                if let Some((start_time, name)) = starts.remove(id) {
                    let ts = start_time.as_micros();
                    let dur = event.time.saturating_sub(start_time).as_micros();
                    events.push((name, ts, dur, *id));
                }
            }
        }
    }

    events.sort_by_key(|e| e.1); // Sort by ts

    let mut out = String::from("[\n");
    for (i, e) in events.iter().enumerate() {
        write!(
            out,
            "  {{\"name\": \"{}\", \"cat\": \"task\", \"ph\": \"X\", \"ts\": {}, \"dur\": {}, \"pid\": 1, \"tid\": {}}}",
            e.0, e.1, e.2, e.3
        )?;
        if i < events.len() - 1 {
            out.push_str(",\n");
        } else {
            out.push('\n');
        }
    }
    out.push(']');
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::DagBuilder;
    #[cfg(feature = "testing")]
    use crate::dag_profiler::{DagProfile, ProfilerEvent, ProfilerEventKind};

    fn dummy_activity() {}
    fn dummy_activity2() {}
    fn dummy_activity3() {}

    #[cfg(feature = "testing")]
    #[test]
    fn test_export_chrome_trace() {
        let profile = DagProfile {
            total_duration: std::time::Duration::from_secs(5),
            peak_concurrency: 1,
            timeline: vec![
                ProfilerEvent {
                    time: std::time::Duration::from_secs(0),
                    kind: ProfilerEventKind::TaskStarted(0, "activity_a".to_string()),
                },
                ProfilerEvent {
                    time: std::time::Duration::from_secs(2),
                    kind: ProfilerEventKind::TaskCompleted(0, "activity_a".to_string()),
                },
                ProfilerEvent {
                    time: std::time::Duration::from_secs(2),
                    kind: ProfilerEventKind::TaskStarted(1, "activity_b".to_string()),
                },
                ProfilerEvent {
                    time: std::time::Duration::from_secs(5),
                    kind: ProfilerEventKind::TaskCompleted(1, "activity_b".to_string()),
                },
            ],
        };

        let trace = export_chrome_trace(&profile).unwrap();
        assert!(trace.contains("\"name\": \"activity_a\""));
        assert!(trace.contains("\"ts\": 0"));
        assert!(trace.contains("\"dur\": 2000000"));
        assert!(trace.contains("\"name\": \"activity_b\""));
        assert!(trace.contains("\"ts\": 2000000"));
        assert!(trace.contains("\"dur\": 3000000"));
    }

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
}
