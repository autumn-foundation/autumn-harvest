//! Mermaid.js Gantt chart exporter for DAG profiles.
//!
//! Visualizes a `DagProfile` (and optionally a `CriticalPathResult`) as a Gantt chart,
//! highlighting the critical path to easily identify bottlenecks.

use crate::critical_path::CriticalPathResult;
use crate::dag_profiler::{DagProfile, ProfilerEventKind};
use std::fmt::Write;

/// Exports a DAG execution profile to a Mermaid.js Gantt chart.
///
/// If a `CriticalPathResult` is provided, tasks on the critical path will be highlighted.
///
/// # Errors
/// Returns `std::fmt::Error` if string formatting fails.
///
/// # Panics
/// Panics if a task ID present in the timeline is missing from the task starts map.
pub fn export_mermaid_gantt(
    profile: &DagProfile,
    critical_path: Option<&CriticalPathResult>,
) -> Result<String, std::fmt::Error> {
    let mut out = String::new();
    writeln!(out, "gantt")?;
    writeln!(out, "    title DAG Execution Profile")?;
    writeln!(out, "    dateFormat x")?;
    writeln!(out, "    axisFormat %M:%S")?;

    if profile.timeline.is_empty() {
        return Ok(out);
    }

    writeln!(out, "    section Tasks")?;

    let mut starts = std::collections::HashMap::new();
    let mut ends = std::collections::HashMap::new();

    for event in &profile.timeline {
        match &event.kind {
            ProfilerEventKind::TaskStarted(idx, name) => {
                starts.insert(*idx, (event.time, name.clone()));
            }
            ProfilerEventKind::TaskCompleted(idx, _) => {
                ends.insert(*idx, event.time);
            }
        }
    }

    let mut task_indices: Vec<_> = starts.keys().copied().collect();
    // Sort tasks by start time, then by index
    task_indices.sort_by(|a, b| {
        let time_a = starts.get(a).unwrap().0;
        let time_b = starts.get(b).unwrap().0;
        time_a.cmp(&time_b).then_with(|| a.cmp(b))
    });

    for idx in task_indices {
        if let (Some((start_time, name)), Some(end_time)) = (starts.get(&idx), ends.get(&idx)) {
            let start_ms = start_time.as_millis();
            let duration_ms = end_time.saturating_sub(*start_time).as_millis();

            let is_crit = critical_path.is_some_and(|cp| cp.path_indices.contains(&idx));
            let tags = if is_crit { "crit, " } else { "" };

            writeln!(out, "    {name} :{tags}t{idx}, {start_ms}, {duration_ms}ms")?;
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::critical_path::CriticalPathAnalyzer;
    use crate::dag::DagBuilder;
    use crate::dag_profiler::DagProfiler;
    use std::time::Duration;

    fn activity_a() {}
    fn activity_b() {}
    fn activity_c() {}
    fn activity_d() {}

    #[test]
    fn test_export_gantt_empty() {
        let builder = DagBuilder::new();
        let dag = builder.build().unwrap();

        let profile = DagProfiler::new(dag.clone()).profile();
        let cp_result = CriticalPathAnalyzer::new(dag).analyze();

        let mermaid = export_mermaid_gantt(&profile, Some(&cp_result)).unwrap();
        assert!(mermaid.contains("gantt"));
        assert!(!mermaid.contains("section Tasks"));
    }

    #[test]
    fn test_export_gantt_branching() {
        let mut builder = DagBuilder::new();
        let start = builder.activity(activity_a);

        let branch1 = builder.activity(activity_b).upstream(&start);
        let branch2 = builder.activity(activity_c).upstream(&start);

        let _end = builder
            .activity(activity_d)
            .upstream(&branch1)
            .upstream(&branch2);

        let dag = builder.build().unwrap();

        let profiler = DagProfiler::new(dag.clone())
            .mock_duration("activity_a", Duration::from_secs(1))
            .mock_duration("activity_b", Duration::from_secs(4))
            .mock_duration("activity_c", Duration::from_secs(10))
            .mock_duration("activity_d", Duration::from_secs(1));
        let profile = profiler.profile();

        let analyzer = CriticalPathAnalyzer::new(dag)
            .mock_duration("activity_a", Duration::from_secs(1))
            .mock_duration("activity_b", Duration::from_secs(4))
            .mock_duration("activity_c", Duration::from_secs(10))
            .mock_duration("activity_d", Duration::from_secs(1));
        let cp_result = analyzer.analyze();

        let mermaid = export_mermaid_gantt(&profile, Some(&cp_result)).unwrap();

        assert!(mermaid.contains("gantt"));
        assert!(mermaid.contains("section Tasks"));
        // A, C, D should be on the critical path, but not B.
        // B (activity_b) is task 1.
        // C (activity_c) is task 2.
        assert!(mermaid.contains("activity_a :crit, t0"));
        assert!(mermaid.contains("activity_b :t1")); // no crit
        assert!(mermaid.contains("activity_c :crit, t2"));
        assert!(mermaid.contains("activity_d :crit, t3"));
    }
}
