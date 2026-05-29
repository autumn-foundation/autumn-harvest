//! Visualization exporters for DAG definitions.
//!
//! Provides utilities to export workflow DAGs (Directed Acyclic Graphs) into human-readable
//! and tool-compatible diagram formats such as Mermaid.js and Graphviz DOT.
//! This is useful for debugging, documentation, and visualizing dependencies.
use crate::dag::DagDefinition;
use std::fmt::Write;

#[cfg(feature = "testing")]
use crate::critical_path::CriticalPathResult;
#[cfg(feature = "testing")]
use crate::dag_profiler::{DagProfile, ProfilerEventKind};

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

/// Exports a DAG profile to a Mermaid.js Gantt chart.
///
/// # Errors
/// Returns `std::fmt::Error` if string formatting fails.
#[cfg(feature = "testing")]
pub fn export_mermaid_gantt(profile: &DagProfile) -> Result<String, std::fmt::Error> {
    let mut out = String::new();
    writeln!(out, "gantt")?;
    writeln!(out, "    title DAG Execution Profile")?;
    writeln!(out, "    dateFormat  s")?;
    writeln!(out, "    axisFormat  %M:%S")?;

    // Sort tasks by index to keep them organized
    let mut task_events: std::collections::HashMap<usize, (String, u64, u64)> =
        std::collections::HashMap::new();

    for event in &profile.timeline {
        match &event.kind {
            ProfilerEventKind::TaskStarted(idx, name) => {
                let entry = task_events
                    .entry(*idx)
                    .or_insert_with(|| (name.clone(), 0, 0));
                entry.1 = event.time.as_secs();
            }
            ProfilerEventKind::TaskCompleted(idx, _) => {
                if let Some(entry) = task_events.get_mut(idx) {
                    entry.2 = event.time.as_secs();
                }
            }
        }
    }

    let mut sorted_tasks: Vec<_> = task_events.into_iter().collect();
    sorted_tasks.sort_by_key(|&(idx, _)| idx);

    for (idx, (name, start, end)) in sorted_tasks {
        writeln!(out, "    section Task {idx}")?;
        writeln!(out, "    {name} :a{idx}, {start}s, {}s", end.saturating_sub(start))?;
    }

    Ok(out)
}

/// Combines DAG visualization, timeline profile, and critical path analysis into an HTML report.
///
/// # Errors
/// Returns `std::fmt::Error` if string formatting fails.
#[cfg(feature = "testing")]
pub fn export_html_report(
    dag: &DagDefinition,
    profile: &DagProfile,
    critical_path: &CriticalPathResult,
) -> Result<String, std::fmt::Error> {
    let mut out = String::new();
    writeln!(out, "<!DOCTYPE html>")?;
    writeln!(out, "<html>")?;
    writeln!(out, "<head>")?;
    writeln!(out, "    <title>DAG Execution Report</title>")?;
    writeln!(out, "    <script type=\"module\">")?;
    writeln!(
        out,
        "        import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';"
    )?;
    writeln!(out, "        mermaid.initialize({{ startOnLoad: true }});")?;
    writeln!(out, "    </script>")?;
    writeln!(out, "    <style>")?;
    writeln!(
        out,
        "        body {{ font-family: system-ui, sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px; }}"
    )?;
    writeln!(
        out,
        "        .section {{ margin-bottom: 40px; padding: 20px; border: 1px solid #ddd; border-radius: 8px; }}"
    )?;
    writeln!(
        out,
        "        .metric {{ font-size: 1.2em; font-weight: bold; color: #2c3e50; }}"
    )?;
    writeln!(out, "        .critical-path {{ color: #e74c3c; }}")?;
    writeln!(out, "    </style>")?;
    writeln!(out, "</head>")?;
    writeln!(out, "<body>")?;
    writeln!(out, "    <h1>DAG Execution Report</h1>")?;

    writeln!(out, "    <div class=\"section\">")?;
    writeln!(out, "        <h2>Summary Metrics</h2>")?;
    writeln!(
        out,
        "        <p>Total Estimated Duration: <span class=\"metric\">{}s</span></p>",
        profile.total_duration.as_secs()
    )?;
    writeln!(
        out,
        "        <p>Peak Concurrency: <span class=\"metric\">{}</span></p>",
        profile.peak_concurrency
    )?;
    writeln!(
        out,
        "        <p>Critical Path Duration: <span class=\"metric critical-path\">{}s</span></p>",
        critical_path.total_duration.as_secs()
    )?;
    writeln!(
        out,
        "        <p>Critical Path: <span class=\"critical-path\">{}</span></p>",
        critical_path.path_names.join(" -> ")
    )?;
    writeln!(out, "    </div>")?;

    writeln!(out, "    <div class=\"section\">")?;
    writeln!(out, "        <h2>Execution Timeline (Gantt)</h2>")?;
    writeln!(out, "        <div class=\"mermaid\">")?;
    writeln!(out, "{}", export_mermaid_gantt(profile)?)?;
    writeln!(out, "        </div>")?;
    writeln!(out, "    </div>")?;

    writeln!(out, "    <div class=\"section\">")?;
    writeln!(out, "        <h2>Dependency Graph (Flowchart)</h2>")?;
    writeln!(out, "        <div class=\"mermaid\">")?;
    writeln!(out, "{}", export_mermaid(dag)?)?;
    writeln!(out, "        </div>")?;
    writeln!(out, "    </div>")?;

    writeln!(out, "</body>")?;
    writeln!(out, "</html>")?;

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

    #[cfg(feature = "testing")]
    #[test]
    fn test_export_mermaid_gantt() {
        use crate::dag_profiler::DagProfiler;
        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity);
        let _b = builder.activity(dummy_activity2).upstream(&a);
        let dag = builder.build().unwrap();

        let profiler = DagProfiler::new(dag)
            .mock_duration("dummy_activity", std::time::Duration::from_secs(2))
            .mock_duration("dummy_activity2", std::time::Duration::from_secs(3));
        let profile = profiler.profile();

        let gantt = export_mermaid_gantt(&profile).unwrap();
        assert!(gantt.contains("gantt"));
        assert!(gantt.contains("dummy_activity :a0, 0s, 2s"));
        assert!(gantt.contains("dummy_activity2 :a1, 2s, 3s"));
    }

    #[cfg(feature = "testing")]
    #[test]
    fn test_export_html_report() {
        use crate::critical_path::CriticalPathAnalyzer;
        use crate::dag_profiler::DagProfiler;

        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy_activity);
        let _b = builder.activity(dummy_activity2).upstream(&a);
        let dag = builder.build().unwrap();

        let profiler = DagProfiler::new(dag.clone())
            .mock_duration("dummy_activity", std::time::Duration::from_secs(2))
            .mock_duration("dummy_activity2", std::time::Duration::from_secs(3));
        let profile = profiler.profile();

        let analyzer = CriticalPathAnalyzer::new(dag.clone())
            .mock_duration("dummy_activity", std::time::Duration::from_secs(2))
            .mock_duration("dummy_activity2", std::time::Duration::from_secs(3));
        let cp = analyzer.analyze();

        let html = export_html_report(&dag, &profile, &cp).unwrap();
        assert!(html.contains("<!DOCTYPE html>"));
        assert!(html.contains("Total Estimated Duration: <span class=\"metric\">5s</span>"));
        assert!(html.contains("Critical Path: <span class=\"critical-path\">dummy_activity -> dummy_activity2</span>"));
    }
}
