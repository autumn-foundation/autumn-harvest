//! Critical Path Exporter for DAGs.
//!
//! Provides utilities to export workflow DAGs (Directed Acyclic Graphs) into
//! Mermaid.js diagrams with the critical path highlighted.
use crate::critical_path::CriticalPathResult;
use crate::dag::DagDefinition;
use std::fmt::Write;

/// Exports the DAG definition to a Mermaid.js flowchart, highlighting the critical path.
///
/// Nodes on the critical path are styled with a red border, and edges on the critical path
/// are drawn thicker and colored red to stand out.
///
/// # Examples
///
/// ```rust
/// use autumn_harvest::dag::DagBuilder;
/// use autumn_harvest::critical_path::CriticalPathAnalyzer;
/// use autumn_harvest::dag_critical_path_export::export_critical_path_mermaid;
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
/// let analyzer = CriticalPathAnalyzer::new(dag.clone())
///     .mock_duration("my_activity", Duration::from_secs(10))
///     .mock_duration("my_other_activity", Duration::from_secs(5));
/// let cp_result = analyzer.analyze();
///
/// let mermaid = export_critical_path_mermaid(&dag, &cp_result).unwrap();
/// assert!(mermaid.contains("graph TD"));
/// assert!(mermaid.contains("linkStyle"));
/// ```
///
/// # Errors
/// Returns `std::fmt::Error` if string formatting fails.
pub fn export_critical_path_mermaid(
    dag: &DagDefinition,
    cp: &CriticalPathResult,
) -> Result<String, std::fmt::Error> {
    let mut out = String::new();
    writeln!(out, "graph TD")?;

    let tasks = dag.tasks();

    for (i, task) in tasks.iter().enumerate() {
        writeln!(out, "    t{i}[\"{}\"]", task.activity_name)?;
    }

    let mut link_idx = 0;
    let mut cp_links = Vec::new();

    for (i, task) in tasks.iter().enumerate() {
        for &upstream in &task.upstreams {
            // Check if both nodes are sequentially in the critical path
            let is_cp_edge = cp.path_indices.iter().position(|&x| x == i).is_some_and(|pos_i| {
                pos_i > 0 && cp.path_indices[pos_i - 1] == upstream
            });

            if is_cp_edge {
                cp_links.push(link_idx);
            }

            writeln!(out, "    t{upstream} --> t{i}")?;
            link_idx += 1;
        }
    }

    // Apply styles to critical path nodes
    for &node_idx in &cp.path_indices {
        writeln!(out, "    style t{node_idx} stroke:#f00,stroke-width:2px")?;
    }

    // Apply styles to critical path edges
    for &link in &cp_links {
        writeln!(out, "    linkStyle {link} stroke:#f00,stroke-width:2px")?;
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::critical_path::CriticalPathAnalyzer;
    use crate::dag::DagBuilder;
    use std::time::Duration;

    fn dummy_activity() {}
    fn dummy_activity2() {}
    fn dummy_activity3() {}

    #[test]
    fn test_export_critical_path_mermaid() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity); // 0
        let b1 = builder.activity(dummy_activity2).upstream(&a); // 1
        let b2 = builder.activity(dummy_activity2).upstream(&a); // 2
        let _c = builder
            .activity(dummy_activity3)
            .upstream(&b1) // 3
            .upstream(&b2);

        let _dag = builder.build().unwrap();

        // Let's make the path 0 -> 2 -> 3 the critical path
        // Because both b1 and b2 have the same name, we can't mock them differently via name
        // but we can set start_to_close on the builder.
        // Let's build it again with start_to_close to differentiate branches
        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity); // 0
        let branch_short = builder
            .activity(dummy_activity2)
            .upstream(&a)
            .start_to_close(Duration::from_secs(1)); // 1
        let branch_long = builder
            .activity(dummy_activity2)
            .upstream(&a)
            .start_to_close(Duration::from_secs(10)); // 2
        let _c = builder
            .activity(dummy_activity3)
            .upstream(&branch_short)
            .upstream(&branch_long); // 3

        let dag = builder.build().unwrap();
        let analyzer = CriticalPathAnalyzer::new(dag.clone());
        let cp = analyzer.analyze();

        assert_eq!(cp.path_indices, vec![0, 2, 3]);

        let mermaid = export_critical_path_mermaid(&dag, &cp).unwrap();

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
    style t0 stroke:#f00,stroke-width:2px
    style t2 stroke:#f00,stroke-width:2px
    style t3 stroke:#f00,stroke-width:2px
    linkStyle 1 stroke:#f00,stroke-width:2px
    linkStyle 3 stroke:#f00,stroke-width:2px
";
        assert_eq!(mermaid, expected_mermaid);
    }
}
