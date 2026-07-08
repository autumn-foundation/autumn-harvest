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

/// Exports the Critical Path to a Mermaid.js Gantt chart.
///
/// Visualizes the longest execution path through the DAG, showing the sequence of bottlenecks.
/// Since `CriticalPathResult` currently doesn't store individual task durations, this
/// basic visualizer evenly distributes the time or shows the sequence.
/// For a more precise visualization, it can be extended when `CriticalPathResult`
/// includes individual durations.
///
/// # Examples
///
/// ```rust
/// use autumn_harvest::dag::DagBuilder;
/// use autumn_harvest::critical_path::CriticalPathAnalyzer;
/// use autumn_harvest::dag_export::export_mermaid_gantt;
/// use std::time::Duration;
///
/// fn a() {}
/// fn b() {}
///
/// let mut builder = DagBuilder::new();
/// let n1 = builder.activity(a);
/// let n2 = builder.activity(b).upstream(&n1);
/// let dag = builder.build().unwrap();
///
/// let analyzer = CriticalPathAnalyzer::new(dag)
///     .mock_duration("a", Duration::from_secs(10))
///     .mock_duration("b", Duration::from_secs(5));
/// let result = analyzer.analyze();
///
/// let gantt = export_mermaid_gantt(&result).unwrap();
/// assert!(gantt.contains("gantt"));
/// assert!(gantt.contains("a :"));
/// assert!(gantt.contains("b :"));
/// ```
///
/// # Errors
/// Returns `std::fmt::Error` if string formatting fails.
#[cfg(feature = "dag-export-gantt")]
pub fn export_mermaid_gantt(
    critical_path: &crate::critical_path::CriticalPathResult,
) -> Result<String, std::fmt::Error> {
    let mut out = String::new();
    writeln!(out, "gantt")?;
    writeln!(out, "    title Critical Path")?;
    writeln!(out, "    dateFormat  X")?; // X is unix timestamp format, we use simple numbers
    writeln!(out, "    axisFormat %s")?; // seconds format

    writeln!(out, "    section Activities")?;

    let mut start_time = 0;

    // We don't have individual durations in the current CriticalPathResult,
    // so we will just create a sequential visualization using 1s for each step
    // just to show the dependency chain in a Gantt format, until we augment CriticalPathResult.
    // To make it look like a Gantt, each task starts after the previous one.

    for (i, name) in critical_path.path_names.iter().enumerate() {
        let task_id = format!("t{i}");
        let duration = 1; // Default to 1 for visual sequence
        if i == 0 {
            writeln!(out, "    {name} : {task_id}, {start_time}, {duration}s")?;
        } else {
            let prev_id = format!("t{}", i - 1);
            writeln!(out, "    {name} : {task_id}, after {prev_id}, {duration}s")?;
        }
        start_time += duration;
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

    #[test]
    #[cfg(feature = "dag-export-gantt")]
    fn test_export_gantt() {
        use crate::critical_path::CriticalPathAnalyzer;
        use std::time::Duration;

        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity);
        let _b = builder.activity(dummy_activity2).upstream(&a);
        let dag = builder.build().unwrap();

        let analyzer = CriticalPathAnalyzer::new(dag)
            .mock_duration("dummy_activity", Duration::from_secs(10))
            .mock_duration("dummy_activity2", Duration::from_secs(5));

        let result = analyzer.analyze();
        let gantt = export_mermaid_gantt(&result).unwrap();

        assert!(gantt.contains("gantt"));
        assert!(gantt.contains("dummy_activity : t0"));
        assert!(gantt.contains("dummy_activity2 : t1, after t0"));
    }
}
