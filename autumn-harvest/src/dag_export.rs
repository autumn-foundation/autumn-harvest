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

/// Exports the DAG definition to a Mermaid.js flowchart, highlighting the critical path.
///
/// Nodes on the critical path will be highlighted, and edges between them will be bolded and colored red.
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

    let mut link_index = 0;
    let mut critical_links = Vec::new();

    for (i, task) in tasks.iter().enumerate() {
        for &upstream in &task.upstreams {
            writeln!(out, "    t{upstream} --> t{i}")?;

            // Check if this edge is part of the critical path
            let is_critical_edge = critical_path
                .path_indices
                .windows(2)
                .any(|w| w[0] == upstream && w[1] == i);
            if is_critical_edge {
                critical_links.push(link_index);
            }
            link_index += 1;
        }
    }

    if !critical_path.path_indices.is_empty() {
        writeln!(
            out,
            "    classDef critical fill:#ffcccc,stroke:#ff0000,stroke-width:2px;"
        )?;

        let critical_nodes: Vec<String> = critical_path
            .path_indices
            .iter()
            .map(|&i| format!("t{i}"))
            .collect();
        writeln!(out, "    class {} critical;", critical_nodes.join(","))?;

        for link_idx in critical_links {
            writeln!(
                out,
                "    linkStyle {link_idx} stroke:#ff0000,stroke-width:2px;"
            )?;
        }
    }

    Ok(out)
}

/// Exports the DAG definition to Graphviz DOT format, highlighting the critical path.
///
/// Nodes and edges on the critical path will be colored red.
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

    for (i, task) in tasks.iter().enumerate() {
        if critical_path.path_indices.contains(&i) {
            writeln!(
                out,
                "    t{i} [label=\"{}\", color=\"red\", style=\"filled\", fillcolor=\"#ffcccc\"];",
                task.activity_name
            )?;
        } else {
            writeln!(out, "    t{i} [label=\"{}\"];", task.activity_name)?;
        }
    }

    for (i, task) in tasks.iter().enumerate() {
        for &upstream in &task.upstreams {
            let is_critical_edge = critical_path
                .path_indices
                .windows(2)
                .any(|w| w[0] == upstream && w[1] == i);
            if is_critical_edge {
                writeln!(
                    out,
                    "    t{upstream} -> t{i} [color=\"red\", penwidth=2.0];"
                )?;
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
    fn test_export_critical_path() {
        use crate::critical_path::CriticalPathAnalyzer;
        use std::time::Duration;

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
            .mock_duration("dummy_activity2", Duration::from_secs(5)) // Critical path
            .mock_duration("dummy_activity3", Duration::from_secs(2));

        let cp_result = analyzer.analyze();

        let mermaid = export_mermaid_with_critical_path(&dag, &cp_result).unwrap();
        assert!(
            mermaid.contains("classDef critical fill:#ffcccc,stroke:#ff0000,stroke-width:2px;")
        );
        assert!(mermaid.contains("linkStyle 0 stroke:#ff0000,stroke-width:2px;")); // t0 -> t1
        assert!(mermaid.contains("linkStyle 2 stroke:#ff0000,stroke-width:2px;")); // t1 -> t3
        assert!(mermaid.contains("class t0,t1,t3 critical;"));

        let dot = export_dot_with_critical_path(&dag, &cp_result).unwrap();
        assert!(dot.contains(
            "t0 [label=\"dummy_activity\", color=\"red\", style=\"filled\", fillcolor=\"#ffcccc\"];"
        ));
        assert!(dot.contains("t0 -> t1 [color=\"red\", penwidth=2.0];"));
        assert!(dot.contains("t1 -> t3 [color=\"red\", penwidth=2.0];"));
    }
}
