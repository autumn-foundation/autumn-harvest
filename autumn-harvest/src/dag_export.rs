//! Visualization exporters for DAG definitions.
//!
//! Provides utilities to export workflow DAGs (Directed Acyclic Graphs) into human-readable
//! and tool-compatible diagram formats such as Mermaid.js and Graphviz DOT.
//! This is useful for debugging, documentation, and visualizing dependencies.
use crate::dag::DagDefinition;
use std::collections::HashMap;
use std::fmt::Write;
use std::time::Duration;

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

/// Exports the DAG definition to a Mermaid.js Gantt chart, simulating execution timelines.
///
/// This exporter uses mocked or explicit activity durations to calculate parallel
/// execution paths and plots them on a timeline.
///
/// # Examples
///
/// ```rust
/// use autumn_harvest::dag::DagBuilder;
/// use autumn_harvest::dag_export::export_mermaid_gantt;
/// use std::collections::HashMap;
/// use std::time::Duration;
///
/// fn a() {}
/// fn b() {}
///
/// let mut builder = DagBuilder::new();
/// let t1 = builder.activity(a);
/// let t2 = builder.activity(b).upstream(&t1);
/// let dag = builder.build().unwrap();
///
/// let gantt = export_mermaid_gantt(&dag, Duration::from_secs(10), &HashMap::new()).unwrap();
/// assert!(gantt.contains("gantt"));
/// assert!(gantt.contains("after t0"));
/// ```
///
/// # Errors
/// Returns `std::fmt::Error` if string formatting fails.
pub fn export_mermaid_gantt<S: std::hash::BuildHasher>(
    dag: &DagDefinition,
    default_duration: Duration,
    activity_durations: &HashMap<String, Duration, S>,
) -> Result<String, std::fmt::Error> {
    let mut out = String::new();
    writeln!(out, "gantt")?;
    writeln!(out, "    title DAG Execution Timeline")?;
    writeln!(out, "    dateFormat X")?;
    writeln!(out, "    axisFormat %s")?;
    writeln!(out)?;
    writeln!(out, "    section Tasks")?;

    let tasks = dag.tasks();

    // We must track the calculated duration for each task.
    // If a task has no upstreams, we can define it starting at 0.
    // If it has upstreams, we use Mermaid's "after tX tY" syntax.

    for (i, task) in tasks.iter().enumerate() {
        let duration = task.start_to_close.unwrap_or_else(|| {
            activity_durations
                .get(&task.activity_name)
                .copied()
                .unwrap_or(default_duration)
        });

        let duration_secs = duration.as_secs().max(1); // Min 1s to be visible

        if task.upstreams.is_empty() {
            writeln!(
                out,
                "    {} : t{}, 0, {}s",
                task.activity_name, i, duration_secs
            )?;
        } else {
            let upstream_refs = task
                .upstreams
                .iter()
                .map(|u| format!("t{u}"))
                .collect::<Vec<_>>()
                .join(", ");

            writeln!(
                out,
                "    {} : t{}, after {}, {}s",
                task.activity_name, i, upstream_refs, duration_secs
            )?;
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::DagBuilder;
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

        let gantt = export_mermaid_gantt(&dag, Duration::from_secs(10), &HashMap::new()).unwrap();
        assert!(gantt.starts_with("gantt\n"));
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

        let gantt = export_mermaid_gantt(&dag, Duration::from_secs(5), &HashMap::new()).unwrap();
        let expected_gantt = "\
gantt
    title DAG Execution Timeline
    dateFormat X
    axisFormat %s

    section Tasks
    dummy_activity : t0, 0, 5s
    dummy_activity2 : t1, after t0, 5s
    dummy_activity2 : t2, after t0, 5s
    dummy_activity3 : t3, after t1, t2, 5s
";
        assert_eq!(gantt, expected_gantt);
    }

    #[test]
    fn test_export_gantt_with_mocked_durations() {
        let mut builder = DagBuilder::new();
        let start = builder.activity(dummy_activity);
        let _branch1 = builder.activity(dummy_activity2).upstream(&start);
        let _branch2 = builder.activity(dummy_activity3).upstream(&start);

        let dag = builder.build().unwrap();

        let mut mocks = HashMap::new();
        mocks.insert("dummy_activity2".to_string(), Duration::from_secs(20));

        let gantt = export_mermaid_gantt(&dag, Duration::from_secs(5), &mocks).unwrap();
        assert!(gantt.contains("dummy_activity : t0, 0, 5s"));
        assert!(gantt.contains("dummy_activity2 : t1, after t0, 20s"));
        assert!(gantt.contains("dummy_activity3 : t2, after t0, 5s"));
    }
}
