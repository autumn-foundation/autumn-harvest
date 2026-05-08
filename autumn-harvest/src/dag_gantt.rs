#[cfg(feature = "testing")]
use crate::dag_profiler::{DagProfile, ProfilerEventKind};
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
/// let mermaid = export_mermaid_gantt(&profile).unwrap();
/// assert!(mermaid.contains("gantt"));
/// assert!(mermaid.contains("my_activity"));
/// ```
///
/// # Errors
/// Returns `std::fmt::Error` if string formatting fails.
#[cfg(feature = "testing")]
pub fn export_mermaid_gantt(profile: &DagProfile) -> Result<String, std::fmt::Error> {
    let mut out = String::new();
    writeln!(out, "gantt")?;
    writeln!(out, "    title DAG Execution Profile")?;
    writeln!(out, "    dateFormat  s")?;
    writeln!(out, "    axisFormat  %S")?;
    writeln!(out)?;
    writeln!(out, "    section Tasks")?;

    // We need to match starts and ends.
    // The timeline is chronologically sorted.
    let mut starts = std::collections::HashMap::new();

    for event in &profile.timeline {
        match &event.kind {
            ProfilerEventKind::TaskStarted(idx, name) => {
                starts.insert(*idx, (name.clone(), event.time.as_secs()));
            }
            ProfilerEventKind::TaskCompleted(idx, _name) => {
                if let Some((name, start_secs)) = starts.remove(idx) {
                    let end_secs = event.time.as_secs();
                    let duration = end_secs.saturating_sub(start_secs);
                    // format: Task Name : t1, 0s, 5s
                    writeln!(out, "    {name} : t{idx}, {start_secs}s, {duration}s")?;
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

    fn act_a() {}
    fn act_b() {}

    #[test]
    #[cfg(feature = "testing")]
    fn test_export_mermaid_gantt() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(act_a);
        let _b = builder.activity(act_b).upstream(&a);
        let dag = builder.build().unwrap();

        let profile = DagProfiler::new(dag)
            .mock_duration("act_a", Duration::from_secs(2))
            .mock_duration("act_b", Duration::from_secs(3))
            .profile();

        let gantt = export_mermaid_gantt(&profile).unwrap();

        let expected = "\
gantt
    title DAG Execution Profile
    dateFormat  s
    axisFormat  %S

    section Tasks
    act_a : t0, 0s, 2s
    act_b : t1, 2s, 3s
";
        assert_eq!(gantt, expected);
    }
}
