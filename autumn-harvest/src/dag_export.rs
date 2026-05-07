//! Visualization exporters for DAG definitions.
//!
//! Provides utilities to export workflow DAGs (Directed Acyclic Graphs) into human-readable
//! and tool-compatible diagram formats such as Mermaid.js and Graphviz DOT.
//! This is useful for debugging, documentation, and visualizing dependencies.
use crate::dag::DagDefinition;
#[cfg(feature = "testing")]
use crate::dag_profiler::{DagProfile, ProfilerEventKind};
#[cfg(feature = "testing")]
use std::collections::HashMap;

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
}

/// Exports a DAG execution profile to a Mermaid.js Gantt chart.
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
/// use autumn_harvest::dag_profiler::{DagProfile, ProfilerEvent, ProfilerEventKind};
/// use autumn_harvest::dag_export::export_mermaid_gantt;
///
/// let profile = DagProfile {
///     total_duration: Duration::from_secs(5),
///     peak_concurrency: 1,
///     timeline: vec![
///         ProfilerEvent { time: Duration::from_secs(0), kind: ProfilerEventKind::TaskStarted(0, "task_a".to_string()) },
///         ProfilerEvent { time: Duration::from_secs(5), kind: ProfilerEventKind::TaskCompleted(0, "task_a".to_string()) },
///     ]
/// };
///
/// let gantt = export_mermaid_gantt(&profile).unwrap();
/// assert!(gantt.contains("gantt"));
/// assert!(gantt.contains("task_a"));
/// ```
///
/// # Errors
/// Returns `std::fmt::Error` if string formatting fails.
#[cfg(feature = "testing")]
pub fn export_mermaid_gantt(profile: &DagProfile) -> Result<String, std::fmt::Error> {
    let mut out = String::new();
    writeln!(out, "gantt")?;
    writeln!(
        out,
        "    title DAG Execution Profile (Peak Concurrency: {})",
        profile.peak_concurrency
    )?;
    writeln!(out, "    dateFormat X")?;
    writeln!(out, "    axisFormat %M:%S")?;
    writeln!(out, "    section Tasks")?;

    let mut start_times = HashMap::new();

    for event in &profile.timeline {
        match &event.kind {
            ProfilerEventKind::TaskStarted(id, _name) => {
                start_times.insert(*id, event.time.as_secs());
            }
            ProfilerEventKind::TaskCompleted(id, name) => {
                if let Some(start) = start_times.remove(id) {
                    let end = event.time.as_secs();
                    writeln!(out, "    {name} : {start}, {end}")?;
                }
            }
        }
    }

    Ok(out)
}

#[cfg(test)]
#[cfg(feature = "testing")]
mod testing_tests {
    use super::*;
    use crate::dag_profiler::{DagProfile, ProfilerEvent, ProfilerEventKind};
    use std::time::Duration;

    #[test]
    fn test_export_mermaid_gantt() {
        let profile = DagProfile {
            total_duration: Duration::from_secs(5),
            peak_concurrency: 2,
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
        let expected = "gantt\n    title DAG Execution Profile (Peak Concurrency: 2)\n    dateFormat X\n    axisFormat %M:%S\n    section Tasks\n    task_a : 0, 2\n    task_b : 2, 5\n";
        assert_eq!(gantt, expected);
    }
}
