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

#[cfg(feature = "testing")]
use crate::critical_path::CriticalPathResult;
#[cfg(feature = "testing")]
use crate::dag_profiler::{DagProfile, ProfilerEventKind};
#[cfg(feature = "testing")]
use std::collections::HashMap;

/// Exports the DAG execution profile to a Mermaid.js Gantt chart.
///
/// # Errors
/// Returns `std::fmt::Error` if string formatting fails.
#[cfg(feature = "testing")]
pub fn export_mermaid_gantt(
    profile: &DagProfile,
    critical_path: Option<&CriticalPathResult>,
) -> Result<String, std::fmt::Error> {
    let mut out = String::new();
    writeln!(out, "gantt")?;
    writeln!(out, "    title DAG Execution Profile")?;
    writeln!(out, "    dateFormat x")?;
    writeln!(out, "    axisFormat %H:%M:%S")?;

    let mut task_starts = HashMap::new();
    let mut task_ends = HashMap::new();

    for event in &profile.timeline {
        match &event.kind {
            ProfilerEventKind::TaskStarted(idx, name) => {
                task_starts.insert(*idx, (name.clone(), event.time.as_millis()));
            }
            ProfilerEventKind::TaskCompleted(idx, _) => {
                task_ends.insert(*idx, event.time.as_millis());
            }
        }
    }

    let mut cp_set = std::collections::HashSet::new();
    if let Some(cp) = critical_path {
        for idx in &cp.path_indices {
            cp_set.insert(*idx);
        }
    }

    let mut indices: Vec<_> = task_starts.keys().copied().collect();
    indices.sort_unstable();

    for idx in indices {
        if let (Some((name, start)), Some(end)) = (task_starts.get(&idx), task_ends.get(&idx)) {
            let duration = end - start;
            let crit_mod = if cp_set.contains(&idx) { "crit, " } else { "" };
            writeln!(
                out,
                "    {name} :{crit_mod}task_{idx}, {start}, {duration}ms"
            )?;
        }
    }

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

    #[cfg(feature = "testing")]
    #[test]
    fn test_export_mermaid_gantt() {
        use crate::critical_path::CriticalPathResult;
        use crate::dag_profiler::{DagProfile, ProfilerEvent, ProfilerEventKind};
        use std::time::Duration;

        let profile = DagProfile {
            total_duration: Duration::from_secs(10),
            peak_concurrency: 2,
            timeline: vec![
                ProfilerEvent {
                    time: Duration::from_secs(0),
                    kind: ProfilerEventKind::TaskStarted(0, "task_a".to_string()),
                },
                ProfilerEvent {
                    time: Duration::from_secs(5),
                    kind: ProfilerEventKind::TaskCompleted(0, "task_a".to_string()),
                },
                ProfilerEvent {
                    time: Duration::from_secs(5),
                    kind: ProfilerEventKind::TaskStarted(1, "task_b".to_string()),
                },
                ProfilerEvent {
                    time: Duration::from_secs(10),
                    kind: ProfilerEventKind::TaskCompleted(1, "task_b".to_string()),
                },
            ],
        };

        let cp = CriticalPathResult {
            total_duration: Duration::from_secs(10),
            path_indices: vec![0, 1],
            path_names: vec!["task_a".to_string(), "task_b".to_string()],
        };

        let gantt = export_mermaid_gantt(&profile, Some(&cp)).unwrap();
        assert!(gantt.contains("gantt"));
        assert!(gantt.contains("dateFormat x"));
        assert!(gantt.contains("axisFormat %H:%M:%S"));
        assert!(gantt.contains("crit, "));
        assert!(gantt.contains("task_a :crit, task_0, 0, 5000ms"));
        assert!(gantt.contains("task_b :crit, task_1, 5000, 5000ms"));
    }
}
