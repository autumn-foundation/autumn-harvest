//! Visualization exporters for DAG profiles using Gantt charts.
//!
//! Provides utilities to export workflow DAG execution profiles into Mermaid.js
//! Gantt charts.

use crate::critical_path::CriticalPathResult;
use crate::dag_profiler::{DagProfile, ProfilerEventKind};
use std::collections::HashMap;
use std::fmt::Write;
use std::time::Duration;

/// Exports the profiled execution of a DAG into a Mermaid.js Gantt chart.
///
/// If a [`CriticalPathResult`] is provided, the tasks that form the critical
/// path are highlighted in the generated chart.
///
/// # Examples
///
/// ```rust
/// use autumn_harvest::dag_gantt::export_gantt;
/// use autumn_harvest::dag_profiler::{DagProfile, ProfilerEvent, ProfilerEventKind};
/// use std::time::Duration;
///
/// let profile = DagProfile {
///     total_duration: Duration::from_secs(10),
///     peak_concurrency: 1,
///     timeline: vec![
///         ProfilerEvent {
///             time: Duration::from_secs(0),
///             kind: ProfilerEventKind::TaskStarted(0, "A".to_string()),
///         },
///         ProfilerEvent {
///             time: Duration::from_secs(10),
///             kind: ProfilerEventKind::TaskCompleted(0, "A".to_string()),
///         },
///     ],
/// };
///
/// let gantt = export_gantt(&profile, None).unwrap();
/// assert!(gantt.contains("gantt"));
/// assert!(gantt.contains("A : t0, 0, 10"));
/// ```
///
struct TaskSpan {
    name: String,
    start_time: Duration,
    end_time: Option<Duration>,
}

/// Exports the profiled execution of a DAG into a Mermaid.js Gantt chart.
///
/// If a [`CriticalPathResult`] is provided, the tasks that form the critical
/// path are highlighted in the generated chart.
///
/// # Examples
///
/// ```rust
/// use autumn_harvest::dag_gantt::export_gantt;
/// use autumn_harvest::dag_profiler::{DagProfile, ProfilerEvent, ProfilerEventKind};
/// use std::time::Duration;
///
/// let profile = DagProfile {
///     total_duration: Duration::from_secs(10),
///     peak_concurrency: 1,
///     timeline: vec![
///         ProfilerEvent {
///             time: Duration::from_secs(0),
///             kind: ProfilerEventKind::TaskStarted(0, "A".to_string()),
///         },
///         ProfilerEvent {
///             time: Duration::from_secs(10),
///             kind: ProfilerEventKind::TaskCompleted(0, "A".to_string()),
///         },
///     ],
/// };
///
/// let gantt = export_gantt(&profile, None).unwrap();
/// assert!(gantt.contains("gantt"));
/// assert!(gantt.contains("A : t0, 0, 10"));
/// ```
///
/// # Errors
/// Returns `std::fmt::Error` if string formatting fails.
pub fn export_gantt(
    profile: &DagProfile,
    critical_path: Option<&CriticalPathResult>,
) -> Result<String, std::fmt::Error> {
    let mut out = String::new();
    writeln!(out, "gantt")?;
    writeln!(out, "    title DAG Execution Profile")?;
    writeln!(out, "    dateFormat X")?;
    writeln!(out, "    axisFormat %s")?;
    writeln!(out)?;
    writeln!(out, "    section Tasks")?;

    let mut tasks: HashMap<usize, TaskSpan> = HashMap::new();

    for event in &profile.timeline {
        match &event.kind {
            ProfilerEventKind::TaskStarted(idx, name) => {
                tasks.insert(
                    *idx,
                    TaskSpan {
                        name: name.clone(),
                        start_time: event.time,
                        end_time: None,
                    },
                );
            }
            ProfilerEventKind::TaskCompleted(idx, _name) => {
                if let Some(span) = tasks.get_mut(idx) {
                    span.end_time = Some(event.time);
                }
            }
        }
    }

    let mut sorted_tasks: Vec<(usize, TaskSpan)> = tasks.into_iter().collect();
    sorted_tasks.sort_by(|(idx_a, a), (idx_b, b)| {
        a.start_time
            .cmp(&b.start_time)
            .then_with(|| idx_a.cmp(idx_b))
    });

    for (idx, span) in sorted_tasks {
        let is_critical = critical_path.is_some_and(|cp| cp.path_indices.contains(&idx));
        let modifier = if is_critical { "crit, " } else { "" };
        let end_time = span.end_time.unwrap_or(profile.total_duration);

        writeln!(
            out,
            "    {} : {}t{}, {}, {}",
            span.name,
            modifier,
            idx,
            span.start_time.as_secs(),
            end_time.as_secs()
        )?;
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::critical_path::CriticalPathResult;
    use crate::dag_profiler::{DagProfile, ProfilerEvent, ProfilerEventKind};
    use std::time::Duration;

    #[test]
    fn test_export_gantt() {
        let profile = DagProfile {
            total_duration: Duration::from_secs(10),
            peak_concurrency: 2,
            timeline: vec![
                ProfilerEvent {
                    time: Duration::from_secs(0),
                    kind: ProfilerEventKind::TaskStarted(0, "A".to_string()),
                },
                ProfilerEvent {
                    time: Duration::from_secs(5),
                    kind: ProfilerEventKind::TaskCompleted(0, "A".to_string()),
                },
            ],
        };

        let gantt = export_gantt(&profile, None).unwrap();
        assert!(gantt.contains("gantt"));
        assert!(gantt.contains("A : t0, 0, 5"));
    }

    #[test]
    fn test_export_gantt_with_critical_path() {
        let profile = DagProfile {
            total_duration: Duration::from_secs(10),
            peak_concurrency: 1,
            timeline: vec![
                ProfilerEvent {
                    time: Duration::from_secs(0),
                    kind: ProfilerEventKind::TaskStarted(0, "A".to_string()),
                },
                ProfilerEvent {
                    time: Duration::from_secs(5),
                    kind: ProfilerEventKind::TaskCompleted(0, "A".to_string()),
                },
            ],
        };

        let cp = CriticalPathResult {
            total_duration: Duration::from_secs(5),
            path_indices: vec![0],
            path_names: vec!["A".to_string()],
        };

        let gantt = export_gantt(&profile, Some(&cp)).unwrap();
        assert!(gantt.contains("A : crit, t0, 0, 5"));
    }
}
