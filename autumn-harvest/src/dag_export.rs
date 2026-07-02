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

/// Exports a simulated DAG definition to a Mermaid.js flowchart with nodes styled
/// by their execution result.
///
/// # Examples
///
/// ```rust
/// use autumn_harvest::dag::DagBuilder;
/// use autumn_harvest::dag_simulator::DagSimulator;
/// use autumn_harvest::dag_export::export_mermaid_with_simulation;
///
/// fn my_activity() {}
/// fn my_other_activity() {}
///
/// let mut builder = DagBuilder::new();
/// let a = builder.activity(my_activity);
/// let b = builder.activity(my_other_activity).upstream(&a);
/// let dag = builder.build().unwrap();
/// let sim = DagSimulator::new(dag.clone()).run();
///
/// let mermaid = export_mermaid_with_simulation(&dag, &sim).unwrap();
/// assert!(mermaid.contains("classDef succeeded"));
/// assert!(mermaid.contains("class t0 succeeded;"));
/// ```
///
/// # Errors
/// Returns `std::fmt::Error` if string formatting fails.
#[cfg(any(test, feature = "testing"))]
pub fn export_mermaid_with_simulation(
    dag: &DagDefinition,
    sim: &crate::dag_simulator::DagSimulatorResult,
) -> Result<String, std::fmt::Error> {
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

    writeln!(
        out,
        "    classDef succeeded fill:#4CAF50,color:white,stroke-width:2px,stroke:#388E3C;"
    )?;
    writeln!(
        out,
        "    classDef failed fill:#F44336,color:white,stroke-width:2px,stroke:#D32F2F;"
    )?;
    writeln!(
        out,
        "    classDef skipped fill:#E0E0E0,color:#9E9E9E,stroke-width:2px,stroke:#BDBDBD,stroke-dasharray: 5 5;"
    )?;

    for (i, _) in tasks.iter().enumerate() {
        if let Some(status) = sim.get_status(i) {
            let class_name = match status {
                crate::policy::TaskStatus::Succeeded => "succeeded",
                crate::policy::TaskStatus::Failed => "failed",
                crate::policy::TaskStatus::Skipped => "skipped",
            };
            writeln!(out, "    class t{i} {class_name};")?;
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

    #[test]
    fn test_export_mermaid_with_simulation() {
        use crate::dag_simulator::DagSimulator;

        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity);
        let _b = builder.activity(dummy_activity2).upstream(&a);
        let dag = builder.build().unwrap();

        // Simulate where A fails, causing B to be skipped (default behavior for upstream failures).
        let sim = DagSimulator::new(dag.clone())
            .mock_activity("dummy_activity", || Err("Fail".into()))
            .run();

        let mermaid = export_mermaid_with_simulation(&dag, &sim).unwrap();

        // Assert base structures
        assert!(mermaid.contains("graph TD"));
        assert!(mermaid.contains("t0[\"dummy_activity\"]"));
        assert!(mermaid.contains("t1[\"dummy_activity2\"]"));
        assert!(mermaid.contains("t0 --> t1"));

        // Assert class definitions
        assert!(mermaid.contains("classDef succeeded fill:#4CAF50"));
        assert!(mermaid.contains("classDef failed fill:#F44336"));
        assert!(mermaid.contains("classDef skipped fill:#E0E0E0"));

        // Assert assigned classes based on simulation result
        // t0 (dummy_activity) should be failed
        assert!(mermaid.contains("class t0 failed;"));
        // t1 (dummy_activity2) should be skipped
        assert!(mermaid.contains("class t1 skipped;"));
    }
}
