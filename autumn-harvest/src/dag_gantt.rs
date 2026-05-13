//! Gantt chart visualization exporter for DAG execution profiles.
//!
//! Provides utilities to export a simulated DAG execution profile into a
//! Mermaid.js Gantt chart. This enables developers to visually inspect
//! concurrency, task durations, and the overall execution timeline.

use crate::dag_profiler::{DagProfile, ProfilerEventKind};
use std::collections::HashMap;
use std::fmt::Write;

/// Exports a `DagProfile` to a Mermaid.js Gantt chart.
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
/// let _a = builder.activity(my_activity);
/// let dag = builder.build().unwrap();
/// let profile = DagProfiler::new(dag).mock_duration("my_activity", Duration::from_secs(5)).profile();
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
    writeln!(out, "    axisFormat %s s")?;

    // We need to match starts with ends
    let mut starts = HashMap::new();

    for event in &profile.timeline {
        match &event.kind {
            ProfilerEventKind::TaskStarted(idx, _name) => {
                starts.insert(*idx, event.time);
            }
            ProfilerEventKind::TaskCompleted(idx, name) => {
                if let Some(start_time) = starts.get(idx) {
                    let start_ms = start_time.as_millis();
                    let end_ms = event.time.as_millis();
                    writeln!(out, "    {name} :t{idx}, {start_ms}, {end_ms}")?;
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

    #[test]
    fn test_export_mermaid_gantt_basic() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(activity_a);
        let _b = builder.activity(activity_b).upstream(&a);
        let dag = builder.build().unwrap();

        let profiler = DagProfiler::new(dag)
            .mock_duration("activity_a", Duration::from_secs(2))
            .mock_duration("activity_b", Duration::from_secs(3));

        let profile = profiler.profile();
        let gantt = export_mermaid_gantt(&profile).unwrap();

        assert!(gantt.contains("gantt"));
        assert!(gantt.contains("title DAG Execution Profile"));
        assert!(gantt.contains("dateFormat x"));
        assert!(gantt.contains("activity_a :t0, 0, 2000"));
        assert!(gantt.contains("activity_b :t1, 2000, 5000"));
    }
}
