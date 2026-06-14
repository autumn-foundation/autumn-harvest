//! Visualization exporters for DAG execution profiles.
//!
//! Provides utilities to export a `DagProfile` into a Mermaid.js Gantt chart
//! to visualize task execution timelines and concurrency.

use crate::dag_profiler::{DagProfile, ProfilerEventKind};
use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use std::collections::HashMap;
use std::fmt::Write;

/// Exports a `DagProfile` to a Mermaid.js Gantt chart.
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
/// use autumn_harvest::dag::DagBuilder;
/// use autumn_harvest::dag_profiler::DagProfiler;
/// use autumn_harvest::profile_export::export_profile_gantt;
///
/// fn my_activity() {}
/// fn my_other_activity() {}
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
/// let gantt = export_profile_gantt(&profile).unwrap();
/// assert!(gantt.contains("gantt"));
/// assert!(gantt.contains("my_activity"));
/// ```
///
/// # Errors
/// Returns `std::fmt::Error` if string formatting fails.
pub fn export_profile_gantt(profile: &DagProfile) -> Result<String, std::fmt::Error> {
    let mut out = String::new();
    writeln!(out, "gantt")?;
    writeln!(out, "    title DAG Execution Profile")?;
    writeln!(out, "    dateFormat YYYY-MM-DDTHH:mm:ssZ")?;
    writeln!(out, "    axisFormat %H:%M:%S")?;
    writeln!(out, "    section Tasks")?;

    let base_time = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap();

    let mut start_times = HashMap::new();
    let mut tasks = Vec::new();

    for event in &profile.timeline {
        match &event.kind {
            ProfilerEventKind::TaskStarted(id, name) => {
                start_times.insert(*id, (name.clone(), event.time.as_millis()));
            }
            ProfilerEventKind::TaskCompleted(id, _name) => {
                if let Some((name, start_ms)) = start_times.remove(id) {
                    tasks.push((name, start_ms, event.time.as_millis()));
                }
            }
        }
    }

    for (name, start_ms, end_ms) in tasks {
        let start_ms_i64: i64 = start_ms.try_into().unwrap_or(i64::MAX);
        let end_ms_i64: i64 = end_ms.try_into().unwrap_or(i64::MAX);
        let start = base_time + ChronoDuration::milliseconds(start_ms_i64);
        let end = base_time + ChronoDuration::milliseconds(end_ms_i64);
        writeln!(
            out,
            "    {} : {}, {}",
            name,
            start.format("%Y-%m-%dT%H:%M:%SZ"),
            end.format("%Y-%m-%dT%H:%M:%SZ")
        )?;
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::DagBuilder;
    use crate::dag_profiler::DagProfiler;
    use std::time::Duration;

    fn test_activity_a() {}
    fn test_activity_b() {}

    #[test]
    fn test_export_profile_gantt() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(test_activity_a);
        let _b = builder.activity(test_activity_b).upstream(&a);
        let dag = builder.build().unwrap();

        let profile = DagProfiler::new(dag)
            .mock_duration("test_activity_a", Duration::from_secs(2))
            .mock_duration("test_activity_b", Duration::from_secs(3))
            .profile();

        let gantt = export_profile_gantt(&profile).unwrap();

        assert!(gantt.contains("gantt"));
        assert!(gantt.contains("title DAG Execution Profile"));
        assert!(gantt.contains("test_activity_a : 2000-01-01T00:00:00Z, 2000-01-01T00:00:02Z"));
        assert!(gantt.contains("test_activity_b : 2000-01-01T00:00:02Z, 2000-01-01T00:00:05Z"));
    }
}
