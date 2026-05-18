//! Gantt chart visualization for DAG executions.
//!
//! Provides utilities to export DAG execution profiles into Mermaid.js Gantt charts.

use crate::dag_profiler::{DagProfile, ProfilerEventKind};
use std::collections::HashMap;
use std::fmt::Write;

/// Exports a DAG execution profile to a Mermaid.js Gantt chart.
///
/// Converts the timeline of simulated task events into a visual schedule.
///
/// # Examples
///
/// ```rust
/// use autumn_harvest::dag::DagBuilder;
/// use autumn_harvest::dag_profiler::DagProfiler;
/// use autumn_harvest::dag_gantt::export_mermaid_gantt;
/// use std::time::Duration;
///
/// const fn my_activity() {}
/// const fn my_other_activity() {}
///
/// let mut builder = DagBuilder::new();
/// let a = builder.activity(my_activity);
/// let b = builder.activity(my_other_activity).upstream(&a);
/// let dag = builder.build().unwrap();
///
/// let profile = DagProfiler::new(dag)
///     .mock_duration("my_activity", Duration::from_secs(2))
///     .mock_duration("my_other_activity", Duration::from_secs(3))
///     .profile();
///
/// let mermaid = export_mermaid_gantt(&profile).unwrap();
/// assert!(mermaid.contains("gantt"));
/// assert!(mermaid.contains("dateFormat"));
/// ```
///
/// # Errors
/// Returns `std::fmt::Error` if string formatting fails.
pub fn export_mermaid_gantt(profile: &DagProfile) -> Result<String, std::fmt::Error> {
    let mut out = String::new();
    writeln!(out, "gantt")?;
    writeln!(out, "    title DAG Execution Profile")?;
    writeln!(out, "    dateFormat  s")?; // Using seconds as the format
    writeln!(out, "    axisFormat  %S")?; // Use simpler axis format

    let mut start_times = HashMap::new();
    let mut end_times = HashMap::new();
    let mut task_names = HashMap::new();

    for event in &profile.timeline {
        match &event.kind {
            ProfilerEventKind::TaskStarted(id, name) => {
                start_times.insert(*id, event.time);
                task_names.insert(*id, name.clone());
            }
            ProfilerEventKind::TaskCompleted(id, _name) => {
                end_times.insert(*id, event.time);
            }
        }
    }

    // Iterate sorted by ID to maintain deterministic output
    let mut ids: Vec<usize> = start_times.keys().copied().collect();
    ids.sort_unstable();

    for id in ids {
        if let (Some(start), Some(end), Some(name)) = (
            start_times.get(&id),
            end_times.get(&id),
            task_names.get(&id),
        ) {
            let start_sec = start.as_secs();
            let end_sec = end.as_secs();
            writeln!(
                out,
                "    {} :t{}, {}, {}s",
                name,
                id,
                start_sec,
                end_sec - start_sec
            )?;
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::DagBuilder;
    use crate::dag_profiler::DagProfiler;
    use std::time::Duration;

    const fn activity_a() {}
    const fn activity_b() {}
    const fn activity_c() {}

    #[test]
    fn test_export_gantt_linear() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(activity_a);
        let _b = builder.activity(activity_b).upstream(&a);
        let dag = builder.build().unwrap();

        let profile = DagProfiler::new(dag)
            .mock_duration("activity_a", Duration::from_secs(2))
            .mock_duration("activity_b", Duration::from_secs(3))
            .profile();

        let gantt = export_mermaid_gantt(&profile).unwrap();

        let expected = "gantt\n    title DAG Execution Profile\n    dateFormat  s\n    axisFormat  %S\n    activity_a :t0, 0, 2s\n    activity_b :t1, 2, 3s\n";
        assert_eq!(gantt, expected);
    }

    #[test]
    fn test_export_gantt_parallel() {
        let mut builder = DagBuilder::new();
        let start = builder.activity(activity_a);
        let _branch1 = builder.activity(activity_b).upstream(&start);
        let _branch2 = builder.activity(activity_c).upstream(&start);
        let dag = builder.build().unwrap();

        let profile = DagProfiler::new(dag)
            .mock_duration("activity_a", Duration::from_secs(1))
            .mock_duration("activity_b", Duration::from_secs(4))
            .mock_duration("activity_c", Duration::from_secs(2))
            .profile();

        let gantt = export_mermaid_gantt(&profile).unwrap();

        let expected = "gantt\n    title DAG Execution Profile\n    dateFormat  s\n    axisFormat  %S\n    activity_a :t0, 0, 1s\n    activity_b :t1, 1, 4s\n    activity_c :t2, 1, 2s\n";
        assert_eq!(gantt, expected);
    }
}
