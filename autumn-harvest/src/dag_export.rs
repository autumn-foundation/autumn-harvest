//! Visualization exporters for DAG definitions.
//!
//! Provides utilities to export workflow DAGs (Directed Acyclic Graphs) into human-readable
//! and tool-compatible diagram formats such as Mermaid.js and Graphviz DOT.
//! This is useful for debugging, documentation, and visualizing dependencies.
use crate::critical_path::CriticalPathResult;
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

/// Exports the DAG definition to a Mermaid.js flowchart, highlighting the critical path.
///
/// # Examples
///
/// ```rust
/// use autumn_harvest::dag::DagBuilder;
/// use autumn_harvest::critical_path::CriticalPathAnalyzer;
/// use autumn_harvest::dag_export::export_mermaid_with_critical_path;
/// use std::time::Duration;
///
/// fn setup() {}
/// fn heavy_processing() {}
/// fn fast_processing() {}
/// fn finish() {}
///
/// let mut builder = DagBuilder::new();
/// let a = builder.activity(setup);
/// let b = builder.activity(heavy_processing).upstream(&a);
/// let c = builder.activity(fast_processing).upstream(&a);
/// let d = builder.activity(finish).upstream(&b).upstream(&c);
/// let dag = builder.build().unwrap();
///
/// let analyzer = CriticalPathAnalyzer::new(&dag)
///     .with_activity_duration("heavy_processing", Duration::from_secs(60))
///     .with_activity_duration("fast_processing", Duration::from_secs(5));
/// let result = analyzer.analyze();
///
/// let mermaid = export_mermaid_with_critical_path(&dag, &result).unwrap();
/// assert!(mermaid.contains("style t1 stroke:#ff0000")); // heavy_processing
/// assert!(mermaid.contains("==>")); // Critical edges
/// ```
///
/// # Errors
/// Returns `std::fmt::Error` if string formatting fails.
pub fn export_mermaid_with_critical_path(
    dag: &DagDefinition,
    critical_path: &CriticalPathResult,
) -> Result<String, std::fmt::Error> {
    let mut out = String::new();
    writeln!(out, "graph TD")?;

    let tasks = dag.tasks();

    for (i, task) in tasks.iter().enumerate() {
        writeln!(out, "    t{i}[\"{}\"]", task.activity_name)?;
    }

    for (i, task) in tasks.iter().enumerate() {
        for &upstream in &task.upstreams {
            // Check if this edge is on the critical path
            let is_critical_edge = critical_path.path_indices.windows(2).any(|window| {
                if let [u, v] = window {
                    *u == upstream && *v == i
                } else {
                    false
                }
            });

            if is_critical_edge {
                writeln!(out, "    t{upstream} ==>|critical| t{i}")?;
            } else {
                writeln!(out, "    t{upstream} --> t{i}")?;
            }
        }
    }

    // Apply styles to critical path nodes
    if !critical_path.path_indices.is_empty() {
        writeln!(out)?;
        writeln!(out, "    %% Critical Path Highlighting")?;
        for &node_idx in &critical_path.path_indices {
            writeln!(out, "    style t{node_idx} stroke:#ff0000,stroke-width:4px")?;
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

    #[test]
    fn test_export_mermaid_with_critical_path() {
        use crate::critical_path::CriticalPathAnalyzer;
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

        let analyzer = CriticalPathAnalyzer::new(dag.clone())
            .mock_duration("dummy_activity", Duration::from_secs(1))
            .mock_duration("dummy_activity2", Duration::from_secs(10)) // Make b1/b2 critical
            .mock_duration("dummy_activity3", Duration::from_secs(1));

        let result = analyzer.analyze();

        let mermaid = export_mermaid_with_critical_path(&dag, &result).unwrap();

        // Critical path nodes and edges should be highlighted
        assert!(mermaid.contains("==>|critical|"));
        assert!(mermaid.contains("stroke:#ff0000"));
    }
}
