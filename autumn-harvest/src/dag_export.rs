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

use crate::critical_path::CriticalPathResult;

/// Exports the DAG definition to a Mermaid.js flowchart, highlighting the critical path.
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
    let cp_nodes: std::collections::HashSet<usize> =
        critical_path.path_indices.iter().copied().collect();
    // Only edges that are *consecutive* steps along the critical path are
    // critical transitions. Membership in `cp_nodes` alone is not enough: a
    // DAG can have a non-critical "shortcut" edge between two nodes that both
    // happen to lie on the critical path (e.g. `A->B->C` plus a direct `A->C`).
    let cp_edges: std::collections::HashSet<(usize, usize)> = critical_path
        .path_indices
        .windows(2)
        .map(|w| (w[0], w[1]))
        .collect();

    writeln!(
        out,
        "    classDef critical fill:#ffcccc,stroke:#ff0000,stroke-width:2px;"
    )?;

    for (i, task) in tasks.iter().enumerate() {
        if cp_nodes.contains(&i) {
            writeln!(out, "    t{i}[\"{}\"]:::critical", task.activity_name)?;
        } else {
            writeln!(out, "    t{i}[\"{}\"]", task.activity_name)?;
        }
    }

    let mut link_index = 0;
    for (i, task) in tasks.iter().enumerate() {
        for &upstream in &task.upstreams {
            writeln!(out, "    t{upstream} --> t{i}")?;

            if cp_edges.contains(&(upstream, i)) {
                writeln!(
                    out,
                    "    linkStyle {link_index} stroke:#ff0000,stroke-width:2px;"
                )?;
            }
            link_index += 1;
        }
    }

    Ok(out)
}

#[cfg(feature = "testing")]
use crate::dag_profiler::{DagProfile, ProfilerEventKind};

/// Exports a simulated DAG profile to a Mermaid.js Gantt chart.
///
/// # Errors
/// Returns `std::fmt::Error` if string formatting fails.
#[cfg(feature = "testing")]
pub fn export_profile_mermaid_gantt(profile: &DagProfile) -> Result<String, std::fmt::Error> {
    let mut out = String::new();
    writeln!(out, "gantt")?;
    writeln!(out, "    title DAG Execution Profile")?;
    writeln!(out, "    dateFormat  X")?;
    writeln!(out, "    axisFormat  %S")?;

    let mut starts = std::collections::HashMap::new();
    let mut output_lines = Vec::new();

    for event in &profile.timeline {
        match &event.kind {
            ProfilerEventKind::TaskStarted(idx, name) => {
                starts.insert(*idx, (name.clone(), event.time));
            }
            ProfilerEventKind::TaskCompleted(idx, _) => {
                if let Some((name, start_time)) = starts.remove(idx) {
                    // Use fractional seconds so sub-second tasks are not
                    // truncated to a zero-length bar (Mermaid renders decimal
                    // seconds correctly under `dateFormat X`).
                    let start_sec = start_time.as_secs_f64();
                    let duration_sec = event.time.saturating_sub(start_time).as_secs_f64();
                    output_lines.push(format!("    {name} :t{idx}, {start_sec}, {duration_sec}s"));
                }
            }
        }
    }

    for line in output_lines {
        writeln!(out, "{line}")?;
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

    const fn dummy_activity4() {}
    const fn dummy_activity5() {}

    #[test]
    fn test_export_mermaid_with_critical_path() {
        use crate::critical_path::CriticalPathAnalyzer;
        use crate::dag::DagBuilder;
        use std::time::Duration;

        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity);
        let _b = builder.activity(dummy_activity4).upstream(&a);
        let _c = builder.activity(dummy_activity5).upstream(&a);
        let dag = builder.build().unwrap();

        let analyzer = CriticalPathAnalyzer::new(dag.clone())
            .mock_duration("dummy_activity4", Duration::from_secs(10))
            .mock_duration("dummy_activity5", Duration::from_secs(2));
        let cp_result = analyzer.analyze();

        let mermaid = export_mermaid_with_critical_path(&dag, &cp_result).unwrap();
        assert!(mermaid.contains("classDef critical"));
        assert!(mermaid.contains("stroke:#ff0000"));
        assert!(mermaid.contains("t0[\"dummy_activity\"]:::critical"));
        assert!(mermaid.contains("t1[\"dummy_activity4\"]:::critical"));
        assert!(!mermaid.contains("t2[\"dummy_activity5\"]:::critical"));
    }

    #[test]
    #[cfg(feature = "testing")]
    fn test_export_profile_mermaid_gantt() {
        use crate::dag::DagBuilder;
        use crate::dag_profiler::DagProfiler;
        use std::time::Duration;

        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity);
        let _b = builder.activity(dummy_activity4).upstream(&a);
        let dag = builder.build().unwrap();

        let profiler =
            DagProfiler::new(dag).mock_duration("dummy_activity", Duration::from_secs(2));
        let profile = profiler.profile();

        let gantt = export_profile_mermaid_gantt(&profile).unwrap();
        assert!(gantt.contains("gantt"));
        assert!(gantt.contains("dateFormat  X"));
        assert!(gantt.contains("axisFormat  %S"));
        assert!(gantt.contains("dummy_activity"));
    }
}
