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

#[cfg(feature = "testing")]
/// Exports a `DagProfile` to a Mermaid.js Gantt chart.
///
/// # Examples
///
/// ```rust
/// # #[cfg(feature = "testing")]
/// # {
/// use autumn_harvest::dag_profiler::{DagProfile, ProfilerEvent, ProfilerEventKind};
/// use autumn_harvest::dag_export::export_mermaid_gantt;
/// use std::time::Duration;
///
/// let profile = DagProfile {
///     total_duration: Duration::from_secs(5),
///     peak_concurrency: 1,
///     timeline: vec![
///         ProfilerEvent { time: Duration::from_secs(0), kind: ProfilerEventKind::TaskStarted(0, "task_a".to_string()) },
///         ProfilerEvent { time: Duration::from_secs(2), kind: ProfilerEventKind::TaskCompleted(0, "task_a".to_string()) },
///     ]
/// };
///
/// let gantt = export_mermaid_gantt(&profile).unwrap();
/// assert!(gantt.contains("gantt"));
/// assert!(gantt.contains("task_a :t0, 0, 2000"));
/// # }
/// ```
///
/// # Errors
/// Returns `std::fmt::Error` if string formatting fails.
pub fn export_mermaid_gantt(
    profile: &crate::dag_profiler::DagProfile,
) -> Result<String, std::fmt::Error> {
    use crate::dag_profiler::ProfilerEventKind;
    use std::collections::HashMap;

    let mut out = String::new();
    writeln!(out, "gantt")?;
    writeln!(out, "    title DAG Execution Profile")?;
    writeln!(out, "    dateFormat  x")?;
    writeln!(out, "    axisFormat  %M:%S")?;
    writeln!(out, "    section Tasks")?;

    let mut start_times = HashMap::new();
    let mut tasks = Vec::new();

    for event in &profile.timeline {
        match &event.kind {
            ProfilerEventKind::TaskStarted(idx, name) => {
                start_times.insert(*idx, (name.clone(), event.time));
            }
            ProfilerEventKind::TaskCompleted(idx, _name) => {
                if let Some((name, start_time)) = start_times.remove(idx) {
                    tasks.push((*idx, name, start_time, event.time));
                }
            }
        }
    }

    // Sort tasks deterministically by start time, then index
    tasks.sort_by(|a, b| a.2.cmp(&b.2).then_with(|| a.0.cmp(&b.0)));

    for (idx, name, start, end) in tasks {
        let start_ms = start.as_millis();
        let end_ms = end.as_millis();
        writeln!(out, "    {name} :t{idx}, {start_ms}, {end_ms}")?;
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
    fn test_export_mermaid_gantt() {
        use crate::dag_profiler::{DagProfile, ProfilerEvent, ProfilerEventKind};
        use std::time::Duration;

        let profile = DagProfile {
            total_duration: Duration::from_secs(5),
            peak_concurrency: 1,
            timeline: vec![
                ProfilerEvent {
                    time: Duration::from_secs(0),
                    kind: ProfilerEventKind::TaskStarted(0, "task_a".to_string()),
                },
                ProfilerEvent {
                    time: Duration::from_secs(2),
                    kind: ProfilerEventKind::TaskCompleted(0, "task_a".to_string()),
                },
                ProfilerEvent {
                    time: Duration::from_secs(2),
                    kind: ProfilerEventKind::TaskStarted(1, "task_b".to_string()),
                },
                ProfilerEvent {
                    time: Duration::from_secs(5),
                    kind: ProfilerEventKind::TaskCompleted(1, "task_b".to_string()),
                },
            ],
        };

        let gantt = export_mermaid_gantt(&profile).unwrap();
        let expected_gantt = "\
gantt
    title DAG Execution Profile
    dateFormat  x
    axisFormat  %M:%S
    section Tasks
    task_a :t0, 0, 2000
    task_b :t1, 2000, 5000
";
        assert_eq!(gantt, expected_gantt);
    }
}
