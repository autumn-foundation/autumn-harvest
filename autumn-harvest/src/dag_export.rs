//! Visualization exporters for DAG definitions.
//!
//! Provides utilities to export workflow DAGs (Directed Acyclic Graphs) into human-readable
//! and tool-compatible diagram formats such as Mermaid.js and Graphviz DOT.
//! This is useful for debugging, documentation, and visualizing dependencies.
use crate::dag::DagDefinition;
use std::fmt::Write;

#[cfg(feature = "testing")]
use crate::dag_profiler::DagProfile;

/// Exports a DAG execution profile to a Mermaid.js Gantt chart.
///
/// This provides a visual timeline of task executions, showing their start times,
/// durations, and concurrency based on simulated profiling data.
///
/// # Errors
/// Returns `std::fmt::Error` if string formatting fails.
#[cfg(feature = "testing")]
pub fn export_mermaid_gantt(profile: &DagProfile) -> Result<String, std::fmt::Error> {
    use crate::dag_profiler::ProfilerEventKind;

    let mut out = String::new();
    writeln!(out, "```mermaid")?;
    writeln!(out, "gantt")?;
    writeln!(out, "    dateFormat X")?;
    writeln!(out, "    axisFormat %S")?;
    writeln!(out, "    title DAG Execution Profile")?;
    writeln!(out, "    section Tasks")?;

    let mut start_times = std::collections::HashMap::new();

    for event in &profile.timeline {
        match &event.kind {
            ProfilerEventKind::TaskStarted(idx, name) => {
                start_times.insert(*idx, (name.clone(), event.time));
            }
            ProfilerEventKind::TaskCompleted(idx, _) => {
                if let Some((name, start_time)) = start_times.remove(idx) {
                    let duration = event.time.saturating_sub(start_time);
                    writeln!(
                        out,
                        "    {} : {}, {}s",
                        name,
                        start_time.as_secs(),
                        duration.as_secs()
                    )?;
                }
            }
        }
    }

    writeln!(out, "```")?;
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
    fn test_export_mermaid_gantt_simple() {
        use crate::dag_profiler::DagProfiler;
        use std::time::Duration;

        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity);
        let b1 = builder.activity(dummy_activity2).upstream(&a);
        let b2 = builder.activity(dummy_activity2).upstream(&a);
        let _c = builder
            .activity(dummy_activity3)
            .upstream(&b1)
            .upstream(&b2);

        let dag = builder.build().unwrap();
        let profiler = DagProfiler::new(dag)
            .mock_duration("dummy_activity", Duration::from_secs(10))
            .mock_duration("dummy_activity2", Duration::from_secs(5))
            .mock_duration("dummy_activity3", Duration::from_secs(2));

        let profile = profiler.profile();
        let gantt = export_mermaid_gantt(&profile).unwrap();

        let expected_gantt = "\
```mermaid
gantt
    dateFormat X
    axisFormat %S
    title DAG Execution Profile
    section Tasks
    dummy_activity : 0, 10s
    dummy_activity2 : 10, 5s
    dummy_activity2 : 10, 5s
    dummy_activity3 : 15, 2s
```
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
