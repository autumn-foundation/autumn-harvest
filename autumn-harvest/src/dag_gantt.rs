//! Gantt chart visualization for DAG profiles.
//!
//! Provides utilities to export the timeline of a DAG execution profile into a Mermaid.js
//! Gantt chart, enabling visual bottleneck analysis and concurrency insights.

use crate::dag_profiler::{DagProfile, ProfilerEventKind};
use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use std::collections::HashMap;
use std::fmt::Write;

/// Exports a DAG execution profile to a Mermaid.js Gantt chart.
///
/// This visualizes the concurrency and timeline of tasks in the simulated DAG execution.
///
/// # Examples
///
/// ```rust
/// use autumn_harvest::dag::DagBuilder;
/// use autumn_harvest::dag_profiler::DagProfiler;
/// use autumn_harvest::dag_gantt::export_mermaid_gantt;
/// use std::time::Duration;
///
/// fn a() {}
/// fn b() {}
///
/// let mut builder = DagBuilder::new();
/// let node_a = builder.activity(a);
/// let _node_b = builder.activity(b).upstream(&node_a);
/// let dag = builder.build().unwrap();
///
/// let profile = DagProfiler::new(dag)
///     .mock_duration("a", Duration::from_secs(2))
///     .mock_duration("b", Duration::from_secs(3))
///     .profile();
///
/// let gantt = export_mermaid_gantt(&profile).unwrap();
/// assert!(gantt.contains("gantt"));
/// assert!(gantt.contains("a :"));
/// assert!(gantt.contains("b :"));
/// ```
///
/// # Errors
///
/// Returns `std::fmt::Error` if string formatting fails.
pub fn export_mermaid_gantt(profile: &DagProfile) -> Result<String, std::fmt::Error> {
    let mut out = String::new();
    writeln!(out, "gantt")?;
    writeln!(out, "    title DAG Execution Profile")?;
    writeln!(out, "    dateFormat  YYYY-MM-DDTHH:mm:ss.SSS")?;
    writeln!(out, "    axisFormat  %H:%M:%S")?;
    writeln!(out, "    section Timeline")?;

    let base_time = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
    let mut start_times: HashMap<usize, ChronoDuration> = HashMap::new();

    for event in &profile.timeline {
        match &event.kind {
            ProfilerEventKind::TaskStarted(idx, _) => {
                let ms = i64::try_from(event.time.as_millis()).unwrap_or(i64::MAX);
                start_times.insert(*idx, ChronoDuration::milliseconds(ms));
            }
            ProfilerEventKind::TaskCompleted(idx, name) => {
                if let Some(start_offset) = start_times.remove(idx) {
                    let end_ms = i64::try_from(event.time.as_millis()).unwrap_or(i64::MAX);
                    let end_offset = ChronoDuration::milliseconds(end_ms);

                    let duration_ms = (end_offset - start_offset).num_milliseconds();
                    let start_dt = base_time + start_offset;

                    writeln!(
                        out,
                        "    {} : {}, {}ms",
                        name,
                        start_dt.format("%Y-%m-%dT%H:%M:%S.%3f"),
                        duration_ms
                    )?;
                }
            }
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

    fn activity_a() {}
    fn activity_b() {}
    fn activity_c() {}

    #[test]
    fn test_export_mermaid_gantt_linear() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(activity_a);
        let _b = builder.activity(activity_b).upstream(&a);
        let dag = builder.build().unwrap();

        let profile = DagProfiler::new(dag)
            .mock_duration("activity_a", Duration::from_secs(2))
            .mock_duration("activity_b", Duration::from_secs(3))
            .profile();

        let gantt = export_mermaid_gantt(&profile).unwrap();

        assert!(gantt.contains("gantt"));
        assert!(gantt.contains("title DAG Execution Profile"));
        assert!(gantt.contains("activity_a : 2024-01-01T00:00:00.000, 2000ms"));
        assert!(gantt.contains("activity_b : 2024-01-01T00:00:02.000, 3000ms"));
    }

    #[test]
    fn test_export_mermaid_gantt_parallel() {
        let mut builder = DagBuilder::new();
        let start = builder.activity(activity_a);
        let _branch1 = builder.activity(activity_b).upstream(&start);
        let _branch2 = builder.activity(activity_c).upstream(&start);
        let dag = builder.build().unwrap();

        let profile = DagProfiler::new(dag)
            .mock_duration("activity_a", Duration::from_secs(1))
            .mock_duration("activity_b", Duration::from_millis(500))
            .mock_duration("activity_c", Duration::from_millis(1500))
            .profile();

        let gantt = export_mermaid_gantt(&profile).unwrap();

        assert!(gantt.contains("activity_a : 2024-01-01T00:00:00.000, 1000ms"));
        assert!(gantt.contains("activity_b : 2024-01-01T00:00:01.000, 500ms"));
        assert!(gantt.contains("activity_c : 2024-01-01T00:00:01.000, 1500ms"));
    }

    #[test]
    fn test_export_mermaid_gantt_empty() {
        let builder = DagBuilder::new();
        let dag = builder.build().unwrap();
        let profile = DagProfiler::new(dag).profile();
        let gantt = export_mermaid_gantt(&profile).unwrap();

        assert!(gantt.contains("gantt"));
        assert!(!gantt.contains("ms"));
    }
}
