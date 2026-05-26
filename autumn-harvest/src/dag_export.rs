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
use crate::dag_profiler::{DagProfile, ProfilerEventKind};

/// Exports a DAG execution profile to a Mermaid.js Gantt chart.
///
/// This provides a visual timeline of task executions, showing concurrency and critical paths.
/// Since Mermaid Gantt charts require absolute dates, this generator mocks an execution
/// starting from `2000-01-01T00:00:00`.
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
/// let profiler = DagProfiler::new(dag).mock_duration("my_activity", Duration::from_secs(5));
/// let profile = profiler.profile();
///
/// let gantt = export_mermaid_gantt(&profile).unwrap();
/// assert!(gantt.contains("gantt"));
/// assert!(gantt.contains("my_activity"));
/// ```
///
/// # Errors
/// Returns `std::fmt::Error` if string formatting fails.
#[cfg(feature = "testing")]
pub fn export_mermaid_gantt(profile: &DagProfile) -> Result<String, std::fmt::Error> {
    use std::collections::HashMap;

    let mut out = String::new();
    writeln!(out, "gantt")?;
    writeln!(out, "    title DAG Execution Profile")?;
    writeln!(out, "    dateFormat YYYY-MM-DDTHH:mm:ss")?;
    writeln!(out, "    axisFormat %H:%M:%S")?;
    writeln!(out)?;
    writeln!(out, "    section Tasks")?;

    // We need to pair TaskStarted and TaskCompleted to get durations.
    let mut starts = HashMap::new();
    for event in &profile.timeline {
        match &event.kind {
            ProfilerEventKind::TaskStarted(id, name) => {
                starts.insert(*id, (name.clone(), event.time));
            }
            ProfilerEventKind::TaskCompleted(id, _name) => {
                if let Some((name, start_time)) = starts.remove(id) {
                    let end_time = event.time;
                    let start_ts = format_duration_as_timestamp(start_time);
                    let end_ts = format_duration_as_timestamp(end_time);

                    writeln!(out, "    {name} : t{id}, {start_ts}, {end_ts}")?;
                }
            }
        }
    }

    Ok(out)
}

#[cfg(feature = "testing")]
fn format_duration_as_timestamp(duration: std::time::Duration) -> String {
    // Mermaid requires valid datetimes. We mock execution starting at 2000-01-01 00:00:00.
    let total_secs = duration.as_secs();
    let days = total_secs / 86400;
    let rem_secs = total_secs % 86400;
    let hours = rem_secs / 3600;
    let mins = (rem_secs % 3600) / 60;
    let secs = rem_secs % 60;

    // Day is 1-indexed, starting from Jan 1
    format!(
        "2000-01-{:02}T{:02}:{:02}:{:02}",
        days + 1,
        hours,
        mins,
        secs
    )
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
    fn test_export_gantt_chart() {
        use crate::dag_profiler::DagProfiler;
        use std::time::Duration;

        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity);
        let _b1 = builder.activity(dummy_activity2).upstream(&a);
        let dag = builder.build().unwrap();

        let profiler = DagProfiler::new(dag)
            .mock_duration("dummy_activity", Duration::from_secs(5))
            .mock_duration("dummy_activity2", Duration::from_secs(10));
        let profile = profiler.profile();

        let gantt = export_mermaid_gantt(&profile).unwrap();

        assert!(gantt.contains("gantt"));
        assert!(gantt.contains("title DAG Execution Profile"));
        assert!(gantt.contains("dummy_activity : t0, 2000-01-01T00:00:00, 2000-01-01T00:00:05"));
        assert!(gantt.contains("dummy_activity2 : t1, 2000-01-01T00:00:05, 2000-01-01T00:00:15"));
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
