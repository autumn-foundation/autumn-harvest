//! Visualization exporters for DAG definitions.
//!
//! Provides utilities to export workflow DAGs (Directed Acyclic Graphs) into human-readable
//! and tool-compatible diagram formats such as Mermaid.js and Graphviz DOT.
//! This is useful for debugging, documentation, and visualizing dependencies.
use crate::dag::DagDefinition;
use std::collections::HashMap;
use std::fmt::Write;
use std::time::Duration;

/// Exports the DAG definition to a Mermaid.js Gantt chart.
///
/// # Examples
///
/// ```rust
/// use autumn_harvest::dag::DagBuilder;
/// use autumn_harvest::dag_export::export_mermaid_gantt;
/// use std::collections::HashMap;
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
/// let durations = HashMap::new();
/// let gantt = export_mermaid_gantt(&dag, &durations, Duration::from_secs(5), &[]).unwrap();
/// assert!(gantt.contains("gantt"));
/// assert!(gantt.contains("after"));
/// ```
///
/// # Errors
/// Returns `std::fmt::Error` if string formatting fails.
pub fn export_mermaid_gantt<S: std::hash::BuildHasher>(
    dag: &DagDefinition,
    durations: &HashMap<String, Duration, S>,
    default_duration: Duration,
    critical_path_indices: &[usize],
) -> Result<String, std::fmt::Error> {
    let mut out = String::new();
    writeln!(out, "gantt")?;
    writeln!(out, "    title DAG Execution Schedule")?;
    writeln!(out, "    dateFormat  YYYY-MM-DD")?;
    writeln!(out, "    axisFormat  %H:%M:%S")?;

    let tasks = dag.tasks();

    for (i, task) in tasks.iter().enumerate() {
        let duration = durations
            .get(&task.activity_name)
            .unwrap_or(&default_duration);
        let duration_secs = duration.as_secs().max(1);

        let is_crit = critical_path_indices.contains(&i);
        let crit_mod = if is_crit { "crit, " } else { "" };

        let upstreams = &task.upstreams;
        if upstreams.is_empty() {
            writeln!(
                out,
                "    {} :{}t{}, 2024-01-01, {}s",
                task.activity_name, crit_mod, i, duration_secs
            )?;
        } else {
            let mut deps_str = String::new();
            for (idx, &u) in upstreams.iter().enumerate() {
                if idx > 0 {
                    deps_str.push(' ');
                }
                let _ = write!(deps_str, "t{u}");
            }
            writeln!(
                out,
                "    {} :{}t{}, after {}, {}s",
                task.activity_name, crit_mod, i, deps_str, duration_secs
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
    fn test_export_gantt_chart() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity);
        let b1 = builder.activity(dummy_activity2).upstream(&a);
        let b2 = builder.activity(dummy_activity2).upstream(&a);
        let _c = builder
            .activity(dummy_activity3)
            .upstream(&b1)
            .upstream(&b2);

        let dag = builder.build().unwrap();

        let mut durations = HashMap::new();
        durations.insert("dummy_activity".to_string(), Duration::from_secs(10));
        durations.insert("dummy_activity3".to_string(), Duration::from_secs(15));

        let gantt =
            export_mermaid_gantt(&dag, &durations, Duration::from_secs(5), &[0, 3]).unwrap();

        let expected_gantt = "\
gantt
    title DAG Execution Schedule
    dateFormat  YYYY-MM-DD
    axisFormat  %H:%M:%S
    dummy_activity :crit, t0, 2024-01-01, 10s
    dummy_activity2 :t1, after t0, 5s
    dummy_activity2 :t2, after t0, 5s
    dummy_activity3 :crit, t3, after t1 t2, 15s
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
