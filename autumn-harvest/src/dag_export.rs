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
///
/// fn my_activity() {}
/// fn my_other_activity() {}
///
/// let mut builder = DagBuilder::new();
/// let a = builder.activity(my_activity);
/// let b = builder.activity(my_other_activity).upstream(&a);
/// let dag = builder.build().unwrap();
/// let cp = CriticalPathAnalyzer::new(dag.clone()).analyze();
///
/// let mermaid = export_mermaid_with_critical_path(&dag, &cp).unwrap();
/// assert!(mermaid.contains("style t1 fill:#f9f,stroke:#333,stroke-width:4px"));
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
    let cp_indices: std::collections::HashSet<usize> =
        critical_path.path_indices.iter().copied().collect();

    for (i, task) in tasks.iter().enumerate() {
        writeln!(out, "    t{i}[\"{}\"]", task.activity_name)?;
    }

    let mut link_idx = 0;
    for (i, task) in tasks.iter().enumerate() {
        for &upstream in &task.upstreams {
            writeln!(out, "    t{upstream} --> t{i}")?;

            // Check if this edge is part of the critical path
            let is_cp_edge = if cp_indices.contains(&upstream) && cp_indices.contains(&i) {
                match (
                    critical_path.path_indices.iter().position(|&x| x == upstream),
                    critical_path.path_indices.iter().position(|&x| x == i)
                ) {
                    (Some(pos_u), Some(pos_i)) => pos_i == pos_u + 1,
                    _ => false,
                }
            } else {
                false
            };
            if is_cp_edge {
                writeln!(
                    out,
                    "    linkStyle {link_idx} stroke:#f00,stroke-width:4px"
                )?;
            }
            link_idx += 1;
        }
    }

    for i in &critical_path.path_indices {
        writeln!(out, "    style t{i} fill:#f9f,stroke:#333,stroke-width:4px")?;
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

/// Exports the DAG definition to Graphviz DOT format, highlighting the critical path.
///
/// # Examples
///
/// ```rust
/// use autumn_harvest::dag::DagBuilder;
/// use autumn_harvest::critical_path::CriticalPathAnalyzer;
/// use autumn_harvest::dag_export::export_dot_with_critical_path;
///
/// fn my_activity() {}
/// fn my_other_activity() {}
///
/// let mut builder = DagBuilder::new();
/// let a = builder.activity(my_activity);
/// let b = builder.activity(my_other_activity).upstream(&a);
/// let dag = builder.build().unwrap();
/// let cp = CriticalPathAnalyzer::new(dag.clone()).analyze();
///
/// let dot = export_dot_with_critical_path(&dag, &cp).unwrap();
/// assert!(dot.contains("color=red"));
/// ```
///
/// # Errors
/// Returns `std::fmt::Error` if string formatting fails.
pub fn export_dot_with_critical_path(
    dag: &DagDefinition,
    critical_path: &CriticalPathResult,
) -> Result<String, std::fmt::Error> {
    let mut out = String::new();
    writeln!(out, "digraph DAG {{")?;

    let tasks = dag.tasks();
    let cp_indices: std::collections::HashSet<usize> =
        critical_path.path_indices.iter().copied().collect();

    for (i, task) in tasks.iter().enumerate() {
        if cp_indices.contains(&i) {
            writeln!(
                out,
                "    t{i} [label=\"{}\", style=filled, fillcolor=pink, color=red, penwidth=2.0];",
                task.activity_name
            )?;
        } else {
            writeln!(out, "    t{i} [label=\"{}\"];", task.activity_name)?;
        }
    }

    for (i, task) in tasks.iter().enumerate() {
        for &upstream in &task.upstreams {
            let is_cp_edge = if cp_indices.contains(&upstream) && cp_indices.contains(&i) {
                match (
                    critical_path.path_indices.iter().position(|&x| x == upstream),
                    critical_path.path_indices.iter().position(|&x| x == i)
                ) {
                    (Some(pos_u), Some(pos_i)) => pos_i == pos_u + 1,
                    _ => false,
                }
            } else {
                false
            };

            if is_cp_edge {
                writeln!(out, "    t{upstream} -> t{i} [color=red, penwidth=2.0];")?;
            } else {
                writeln!(out, "    t{upstream} -> t{i};")?;
            }
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

    use crate::critical_path::CriticalPathAnalyzer;
    use std::time::Duration;

    #[test]
    fn test_export_with_critical_path() {
        let mut builder = DagBuilder::new();
        let start = builder.activity(dummy_activity); // 0
        let branch1 = builder.activity(dummy_activity2).upstream(&start); // 1
        let branch2 = builder.activity(dummy_activity3).upstream(&start); // 2
        let _end = builder
            .activity(dummy_activity)
            .upstream(&branch1)
            .upstream(&branch2); // 3

        let dag = builder.build().unwrap();

        let analyzer = CriticalPathAnalyzer::new(dag.clone())
            .mock_duration("dummy_activity", Duration::from_secs(1))
            .mock_duration("dummy_activity2", Duration::from_secs(2))
            .mock_duration("dummy_activity3", Duration::from_secs(10));
        let critical_path = analyzer.analyze();

        let mermaid = export_mermaid_with_critical_path(&dag, &critical_path).unwrap();
        // Check if critical path styling is applied to node 2 and 3
        assert!(mermaid.contains("style t2 fill:#f9f,stroke:#333,stroke-width:4px"));
        assert!(mermaid.contains("linkStyle 1 stroke:#f00,stroke-width:4px")); // t0 -> t2
        assert!(mermaid.contains("linkStyle 3 stroke:#f00,stroke-width:4px")); // t2 -> t3

        let dot = export_dot_with_critical_path(&dag, &critical_path).unwrap();
        assert!(dot.contains(
            "t2 [label=\"dummy_activity3\", style=filled, fillcolor=pink, color=red, penwidth=2.0];"
        ));
        assert!(dot.contains("t0 -> t2 [color=red, penwidth=2.0];"));
        assert!(dot.contains("t2 -> t3 [color=red, penwidth=2.0];"));
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
