//! Visualization exporters for DAG definitions.
//!
//! Provides utilities to export workflow DAGs (Directed Acyclic Graphs) into human-readable
//! and tool-compatible diagram formats such as Mermaid.js and Graphviz DOT.
//! This is useful for debugging, documentation, and visualizing dependencies.
use crate::dag::DagDefinition;
#[cfg(feature = "testing")]
use crate::dag_profiler::{DagProfile, ProfilerEventKind};
use std::fmt::Write;

/// Exports the DAG execution profile to a Mermaid.js Gantt chart.
///
/// # Examples
///
/// ```rust
/// use autumn_harvest::dag::DagBuilder;
/// use autumn_harvest::dag_profiler::DagProfiler;
/// use autumn_harvest::dag_export::export_mermaid_gantt;
/// use std::time::Duration;
///
/// fn my_activity() {}
/// fn my_other_activity() {}
///
/// let mut builder = DagBuilder::new();
/// let a = builder.activity(my_activity);
/// let b = builder.activity(my_other_activity).upstream(&a);
/// let dag = builder.build().unwrap();
///
/// let profiler = DagProfiler::new(dag.clone())
///     .mock_duration("my_activity", Duration::from_secs(2))
///     .mock_duration("my_other_activity", Duration::from_secs(3));
/// let profile = profiler.profile();
///
/// let gantt = export_mermaid_gantt(&dag, &profile).unwrap();
/// assert!(gantt.contains("gantt"));
/// assert!(gantt.contains("my_activity"));
/// assert!(gantt.contains("my_other_activity"));
/// ```
///
/// # Errors
/// Returns `std::fmt::Error` if string formatting fails.
#[cfg(feature = "testing")]
pub fn export_mermaid_gantt(
    dag: &DagDefinition,
    profile: &DagProfile,
) -> Result<String, std::fmt::Error> {
    use std::time::Duration;

    let mut out = String::new();
    writeln!(out, "gantt")?;
    writeln!(out, "    title DAG Execution Profile")?;
    writeln!(out, "    dateFormat x")?; // x represents Unix epoch time (or milliseconds from start)
    writeln!(out, "    axisFormat %S.%L")?; // seconds.milliseconds

    writeln!(out, "    section Tasks")?;

    let tasks = dag.tasks();
    let mut task_starts: Vec<Option<Duration>> = vec![None; tasks.len()];
    let mut task_ends: Vec<Option<Duration>> = vec![None; tasks.len()];

    for event in &profile.timeline {
        match &event.kind {
            ProfilerEventKind::TaskStarted(idx, _) => task_starts[*idx] = Some(event.time),
            ProfilerEventKind::TaskCompleted(idx, _) => task_ends[*idx] = Some(event.time),
        }
    }

    for (i, task) in tasks.iter().enumerate() {
        if let (Some(start), Some(end)) = (task_starts[i], task_ends[i]) {
            let start_ms = start.as_millis();
            let end_ms = end.as_millis();
            // Mermaid requires duration or end time. For 0-duration tasks, we'll give them 1ms to render something.
            let render_end_ms = if start_ms == end_ms {
                start_ms + 1
            } else {
                end_ms
            };
            writeln!(
                out,
                "    {} : t{}, {}, {}",
                task.activity_name, i, start_ms, render_end_ms
            )?;
        }
    }

    Ok(out)
}

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

    #[cfg(feature = "testing")]
    use crate::dag_profiler::DagProfiler;
    #[cfg(feature = "testing")]
    use std::time::Duration;

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
    #[cfg(feature = "testing")]
    fn test_export_gantt() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity);
        let b = builder.activity(dummy_activity2).upstream(&a);
        let _c = builder.activity(dummy_activity3).upstream(&b);
        let dag = builder.build().unwrap();

        let profiler = DagProfiler::new(dag.clone())
            .mock_duration("dummy_activity", Duration::from_millis(100))
            .mock_duration("dummy_activity2", Duration::from_millis(150))
            .mock_duration("dummy_activity3", Duration::from_millis(50));

        let profile = profiler.profile();
        let gantt = export_mermaid_gantt(&dag, &profile).unwrap();

        let expected_gantt = "\
gantt
    title DAG Execution Profile
    dateFormat x
    axisFormat %S.%L
    section Tasks
    dummy_activity : t0, 0, 100
    dummy_activity2 : t1, 100, 250
    dummy_activity3 : t2, 250, 300
";
        assert_eq!(gantt, expected_gantt);
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
