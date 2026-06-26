//! Gantt chart visualization for DAG execution profiles.
//!
//! Provides utilities to export a `DagProfile` into a Mermaid.js Gantt chart.
//! This allows visualizing the bottleneck tasks, concurrency, and timeline
//! of a DAG execution.

use crate::dag_profiler::{DagProfile, ProfilerEventKind};
use std::collections::HashMap;
use std::fmt::Write;
use std::time::Duration;

/// Exports a DAG execution profile to a Mermaid.js Gantt chart.
///
/// # Examples
///
/// ```rust
/// use autumn_harvest::dag::DagBuilder;
/// use autumn_harvest::dag_profiler::DagProfiler;
/// use autumn_harvest::dag_gantt::export_mermaid_gantt;
/// use std::time::Duration;
///
/// fn my_activity() {}
///
/// let mut builder = DagBuilder::new();
/// let a = builder.activity(my_activity);
/// let dag = builder.build().unwrap();
///
/// let profile = DagProfiler::new(dag)
///     .mock_duration("my_activity", Duration::from_secs(5))
///     .profile();
///
/// let gantt = export_mermaid_gantt(&profile).unwrap();
/// assert!(gantt.contains("gantt"));
/// assert!(gantt.contains("my_activity"));
/// ```
///
/// # Errors
/// Returns `std::fmt::Error` if string formatting fails.
pub fn export_mermaid_gantt(profile: &DagProfile) -> Result<String, std::fmt::Error> {
    let mut out = String::new();
    writeln!(out, "gantt")?;
    writeln!(out, "    title DAG Execution Profile")?;
    writeln!(out, "    dateFormat x")?;
    writeln!(out, "    axisFormat %H:%M:%S")?;
    writeln!(out, "    section Tasks")?;

    // Extract start and end times for each task
    let mut starts: HashMap<usize, (String, Duration)> = HashMap::new();
    let mut ends: HashMap<usize, Duration> = HashMap::new();

    for event in &profile.timeline {
        match &event.kind {
            ProfilerEventKind::TaskStarted(idx, name) => {
                starts.insert(*idx, (name.clone(), event.time));
            }
            ProfilerEventKind::TaskCompleted(idx, _name) => {
                ends.insert(*idx, event.time);
            }
        }
    }

    // Sort by start time to make the chart logical
    let mut tasks_ordered: Vec<_> = starts.into_iter().collect();
    tasks_ordered.sort_by_key(|(_, (_, start_time))| *start_time);

    for (idx, (name, start_time)) in tasks_ordered {
        let end_time = ends.get(&idx).copied().unwrap_or(start_time);

        writeln!(
            out,
            "    {} : t{}, {}, {}",
            name,
            idx,
            start_time.as_millis(),
            end_time.as_millis()
        )?;
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::DagBuilder;
    use crate::dag_profiler::DagProfiler;

    fn activity_a() {}
    fn activity_b() {}
    fn activity_c() {}

    #[test]
    fn test_export_gantt() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(activity_a);
        let _b = builder.activity(activity_b).upstream(&a);
        let _c = builder.activity(activity_c).upstream(&a);
        let dag = builder.build().unwrap();

        let profile = DagProfiler::new(dag)
            .mock_duration("activity_a", Duration::from_secs(2))
            .mock_duration("activity_b", Duration::from_secs(3))
            .mock_duration("activity_c", Duration::from_secs(1))
            .profile();

        let gantt = export_mermaid_gantt(&profile).unwrap();
        assert!(gantt.contains("gantt"));
        assert!(gantt.contains("activity_a : t0, 0, 2000"));
        assert!(gantt.contains("activity_b : t1, 2000, 5000"));
        assert!(gantt.contains("activity_c : t2, 2000, 3000"));
    }
}
