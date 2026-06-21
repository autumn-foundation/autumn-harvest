//! DAG execution profiler.
//!
//! Simulates a DAG execution based on expected task durations to calculate
//! exact timeline metrics like peak concurrency and estimated completion time.

use crate::dag::DagDefinition;
use std::collections::HashMap;
use std::time::Duration;

/// An event in the profiler timeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProfilerEventKind {
    TaskStarted(usize, String),
    TaskCompleted(usize, String),
}

/// A recorded event at a specific duration from the start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfilerEvent {
    pub time: Duration,
    pub kind: ProfilerEventKind,
}

/// The result of a profile run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DagProfile {
    /// Total duration of the DAG execution.
    pub total_duration: Duration,
    /// Maximum number of tasks running concurrently.
    pub peak_concurrency: usize,
    /// The timeline of events.
    pub timeline: Vec<ProfilerEvent>,
}

/// A discrete-event simulator to profile DAG execution.
pub struct DagProfiler {
    dag: DagDefinition,
    activity_durations: HashMap<String, Duration>,
    default_duration: Duration,
}

impl DagProfiler {
    /// Create a new profiler.
    #[must_use]
    pub fn new(dag: DagDefinition) -> Self {
        Self {
            dag,
            activity_durations: HashMap::new(),
            default_duration: Duration::from_secs(1),
        }
    }

    /// Set a default duration for unmocked activities.
    #[must_use]
    pub const fn with_default_duration(mut self, duration: Duration) -> Self {
        self.default_duration = duration;
        self
    }

    /// Mock the duration of a specific activity by name.
    #[must_use]
    pub fn mock_duration(mut self, name: &str, duration: Duration) -> Self {
        self.activity_durations.insert(name.to_string(), duration);
        self
    }

    /// Run the simulation.
    #[must_use]
    pub fn profile(&self) -> DagProfile {
        let tasks = self.dag.tasks();
        let levels = self.dag.execution_levels();

        if tasks.is_empty() {
            return DagProfile {
                total_duration: Duration::ZERO,
                peak_concurrency: 0,
                timeline: Vec::new(),
            };
        }

        let mut timeline = Vec::new();
        let mut peak_concurrency = 0;
        let mut task_start_times = vec![None; tasks.len()];
        let mut task_end_times = vec![None; tasks.len()];

        // Priority queue-like behavior using a sorted vector of (time, kind)
        let mut event_queue: Vec<(Duration, usize, bool)> = Vec::new(); // bool: true = start, false = end

        for level in levels {
            for &task_index in level {
                let task = &tasks[task_index];
                let duration = self
                    .activity_durations
                    .get(&task.activity_name)
                    .copied()
                    .or(task.start_to_close)
                    .unwrap_or(self.default_duration);

                // Find when this task can start
                let mut start_time = Duration::ZERO;
                for &up_idx in &task.upstreams {
                    if let Some(up_end) = task_end_times[up_idx] {
                        start_time = start_time.max(up_end);
                    }
                }

                task_start_times[task_index] = Some(start_time);
                let end_time = start_time + duration;
                task_end_times[task_index] = Some(end_time);

                event_queue.push((start_time, task_index, true));
                event_queue.push((end_time, task_index, false));
            }
        }

        // Sort events chronologically. If times are equal, ends come before starts.
        event_queue.sort_by(|a, b| {
            a.0.cmp(&b.0).then_with(|| {
                // If the time is the same, and the task is the same, start before end (handles 0 duration tasks).
                if a.1 == b.1 && a.2 != b.2 {
                    return if a.2 {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Greater
                    };
                }

                // Otherwise, end events (false) go before start events (true) to prevent artificial concurrency peaks
                match (a.2, b.2) {
                    (false, true) => std::cmp::Ordering::Less,
                    (true, false) => std::cmp::Ordering::Greater,
                    _ => std::cmp::Ordering::Equal,
                }
            })
        });

        let mut current_concurrency = 0;
        let mut total_duration = Duration::ZERO;

        for (time, task_index, is_start) in event_queue {
            let task_name = tasks[task_index].activity_name.clone();

            if is_start {
                timeline.push(ProfilerEvent {
                    time,
                    kind: ProfilerEventKind::TaskStarted(task_index, task_name),
                });
                current_concurrency += 1;
                if current_concurrency > peak_concurrency {
                    peak_concurrency = current_concurrency;
                }
            } else {
                timeline.push(ProfilerEvent {
                    time,
                    kind: ProfilerEventKind::TaskCompleted(task_index, task_name),
                });
                current_concurrency -= 1;
                total_duration = time; // Time goes forward, so last event sets total duration
            }
        }

        DagProfile {
            total_duration,
            peak_concurrency,
            timeline,
        }
    }
}

/// Exports the given DAG profile as an ASCII Gantt chart.
///
/// Returns an empty string if the timeline is empty.
/// The `max_width` parameter controls the number of characters used for the timeline bar.
///
/// # Errors
/// Returns `std::fmt::Error` if string formatting fails.
pub fn export_profile_ascii_gantt(
    profile: &DagProfile,
    max_width: usize,
) -> Result<String, std::fmt::Error> {
    use std::collections::HashMap;
    use std::fmt::Write;

    let mut out = String::new();
    if profile.timeline.is_empty() {
        return Ok(out);
    }

    let mut starts = HashMap::new();
    let mut spans = Vec::new();

    for event in &profile.timeline {
        match &event.kind {
            ProfilerEventKind::TaskStarted(idx, _name) => {
                starts.insert(*idx, event.time);
            }
            ProfilerEventKind::TaskCompleted(idx, name) => {
                if let Some(start_time) = starts.remove(idx) {
                    spans.push((name.clone(), start_time, event.time, *idx));
                }
            }
        }
    }

    spans.sort_by_key(|s| (s.1, s.3));

    let total_secs = profile.total_duration.as_secs_f64();
    let max_name_len = spans.iter().map(|s| s.0.len()).max().unwrap_or(0);

    for (name, start, end, _) in spans {
        let start_secs = start.as_secs_f64();
        let end_secs = end.as_secs_f64();

        let start_col = if total_secs > 0.0 {
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss, clippy::cast_precision_loss)]
            {
                ((start_secs / total_secs) * max_width as f64).round() as usize
            }
        } else {
            0
        };
        let mut end_col = if total_secs > 0.0 {
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss, clippy::cast_precision_loss)]
            {
                ((end_secs / total_secs) * max_width as f64).round() as usize
            }
        } else {
            1
        };

        if end_col == start_col {
            end_col += 1;
        }

        let pre_spaces = " ".repeat(start_col);
        let bar_len = end_col.saturating_sub(start_col);
        let bar = "█".repeat(bar_len);
        let post_spaces = " ".repeat(max_width.saturating_sub(end_col));

        writeln!(
            &mut out,
            "{name:>max_name_len$} |{pre_spaces}{bar}{post_spaces}| {start_secs:.2}s - {end_secs:.2}s"
        )?;
    }

    Ok(out)
}

/// Exports the given DAG profile as a Mermaid.js Gantt chart.
///
/// Returns an empty string containing only headers if the timeline is empty.
///
/// # Errors
/// Returns `std::fmt::Error` if string formatting fails.
pub fn export_profile_mermaid_gantt(profile: &DagProfile) -> Result<String, std::fmt::Error> {
    use std::collections::HashMap;
    use std::fmt::Write;

    let mut out = String::new();
    writeln!(out, "gantt")?;
    writeln!(out, "    title DAG Execution Profile")?;
    writeln!(out, "    dateFormat  s")?;
    writeln!(out, "    axisFormat  %S")?;

    if profile.timeline.is_empty() {
        return Ok(out);
    }

    let mut starts = HashMap::new();
    let mut spans = Vec::new();

    for event in &profile.timeline {
        match &event.kind {
            ProfilerEventKind::TaskStarted(idx, _) => {
                starts.insert(*idx, event.time);
            }
            ProfilerEventKind::TaskCompleted(idx, name) => {
                if let Some(start_time) = starts.remove(idx) {
                    spans.push((name.clone(), start_time, event.time, *idx));
                }
            }
        }
    }

    spans.sort_by_key(|s| (s.1, s.3));

    for (name, start, end, _) in spans {
        let start_secs = start.as_secs_f64();
        let dur_secs = end.as_secs_f64() - start_secs;
        writeln!(out, "    {name} : {start_secs:.2}, {dur_secs:.2}s")?;
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::DagBuilder;

    fn activity_a() {}
    fn activity_b() {}
    fn activity_c() {}
    fn activity_d() {}

    #[test]
    fn test_linear_profile() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(activity_a);
        let _b = builder.activity(activity_b).upstream(&a);
        let dag = builder.build().unwrap();

        let profiler = DagProfiler::new(dag)
            .mock_duration("activity_a", Duration::from_secs(2))
            .mock_duration("activity_b", Duration::from_secs(3));

        let profile = profiler.profile();

        assert_eq!(profile.total_duration, Duration::from_secs(5));
        assert_eq!(profile.peak_concurrency, 1);
        assert_eq!(profile.timeline.len(), 4);
    }

    #[test]
    fn test_zero_duration_profile() {
        let mut builder = DagBuilder::new();
        let _a = builder.activity(activity_a);
        let dag = builder.build().unwrap();

        let profiler = DagProfiler::new(dag).mock_duration("activity_a", Duration::ZERO);

        let profile = profiler.profile();
        assert_eq!(profile.total_duration, Duration::ZERO);
        assert_eq!(profile.peak_concurrency, 1);
        assert_eq!(profile.timeline.len(), 2);
    }

    #[test]
    fn test_parallel_profile() {
        let mut builder = DagBuilder::new();
        let start = builder.activity(activity_a);

        let branch1 = builder.activity(activity_b).upstream(&start);
        let branch2 = builder.activity(activity_c).upstream(&start);

        let _end = builder
            .activity(activity_d)
            .upstream(&branch1)
            .upstream(&branch2);

        let dag = builder.build().unwrap();

        let profiler = DagProfiler::new(dag)
            .mock_duration("activity_a", Duration::from_secs(1))
            .mock_duration("activity_b", Duration::from_secs(4))
            .mock_duration("activity_c", Duration::from_secs(2))
            .mock_duration("activity_d", Duration::from_secs(1));

        let profile = profiler.profile();

        assert_eq!(profile.total_duration, Duration::from_secs(6));
        // max concurrency is 2 because B and C run at the same time.
        assert_eq!(profile.peak_concurrency, 2);
    }

    #[test]
    fn test_export_ascii_gantt() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(activity_a);
        let _b = builder.activity(activity_b).upstream(&a);
        let dag = builder.build().unwrap();

        let profiler = DagProfiler::new(dag)
            .mock_duration("activity_a", Duration::from_secs(2))
            .mock_duration("activity_b", Duration::from_secs(3));

        let profile = profiler.profile();

        let ascii = export_profile_ascii_gantt(&profile, 40).unwrap();
        assert!(ascii.contains("activity_a"));
        assert!(ascii.contains("activity_b"));
        assert!(ascii.contains("0.00s - 2.00s"));
        assert!(ascii.contains("2.00s - 5.00s"));
        assert!(ascii.contains('█'));
    }

    #[test]
    fn test_export_mermaid_gantt() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(activity_a);
        let _b = builder.activity(activity_b).upstream(&a);
        let dag = builder.build().unwrap();

        let profiler = DagProfiler::new(dag)
            .mock_duration("activity_a", Duration::from_secs(2))
            .mock_duration("activity_b", Duration::from_secs(3));

        let profile = profiler.profile();

        let mermaid = export_profile_mermaid_gantt(&profile).unwrap();
        assert!(mermaid.starts_with("gantt"));
        assert!(mermaid.contains("title DAG Execution Profile"));
        assert!(mermaid.contains("activity_a : 0.00, 2.00s"));
        assert!(mermaid.contains("activity_b : 2.00, 3.00s"));
    }

    #[test]
    fn test_export_empty_profile() {
        let profile = DagProfile {
            total_duration: Duration::ZERO,
            peak_concurrency: 0,
            timeline: Vec::new(),
        };

        let ascii = export_profile_ascii_gantt(&profile, 40).unwrap();
        assert_eq!(ascii, "");

        let mermaid = export_profile_mermaid_gantt(&profile).unwrap();
        assert!(mermaid.starts_with("gantt"));
        assert!(!mermaid.contains("activity_"));
    }
}
