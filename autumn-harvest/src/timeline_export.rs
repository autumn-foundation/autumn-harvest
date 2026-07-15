//! Visualization exporters for execution timelines.
//!
//! Provides utilities to export a [`Timeline`] into tool-compatible diagram
//! formats such as Mermaid.js Gantt charts and Chrome Trace Event Format.

use crate::timeline::{StepKind, Timeline};
use serde_json::{Value, json};
use std::fmt::Write;

/// Exports a [`Timeline`] to a Mermaid.js Gantt chart.
///
/// # Errors
/// Returns `std::fmt::Error` if string formatting fails.
pub fn export_timeline_mermaid_gantt(timeline: &Timeline) -> Result<String, std::fmt::Error> {
    let mut out = String::new();
    writeln!(out, "gantt")?;
    writeln!(
        out,
        "    title Execution Timeline: {}",
        timeline.workflow_name
    )?;
    writeln!(out, "    dateFormat  X")?;
    writeln!(out, "    axisFormat  %S")?;

    for (i, step) in timeline.steps.iter().enumerate() {
        let name = step.name.as_deref().unwrap_or("unnamed");
        #[allow(clippy::cast_precision_loss)]
        let start_sec = step.scheduled_at.timestamp_millis() as f64 / 1000.0;
        #[allow(clippy::cast_precision_loss)]
        let duration_sec = step.total_ms as f64 / 1000.0;

        let kind_str = match step.step_kind {
            StepKind::Activity => "Activity",
            StepKind::LocalActivity => "LocalActivity",
            StepKind::Timer => "Timer",
            StepKind::ChildWorkflow => "ChildWorkflow",
            StepKind::SignalWait => "SignalWait",
            StepKind::SideEffect => "SideEffect",
        };

        writeln!(
            out,
            "    [{kind_str}] {name} :t{i}, {start_sec}, {duration_sec}s"
        )?;
    }

    Ok(out)
}

/// Exports a [`Timeline`] to Chrome Trace Event Format.
///
/// Converts the execution timeline into a series of "Complete" (`"ph": "X"`)
/// trace events compatible with trace viewers like `chrome://tracing` or
/// `ui.perfetto.dev`. Each step in the timeline is represented as a span
/// with a start time (`ts`) and duration (`dur`) in microseconds.
///
/// Steps are grouped into different "threads" (`tid`) based on their kind
/// (e.g., Activity, Timer) to make visualization cleaner.
///
/// # Returns
/// A JSON `Value` representing the trace events array.
#[must_use]
pub fn export_timeline_chrome_trace(timeline: &Timeline) -> Value {
    let mut events = Vec::new();

    // Metadata events for cleaner thread names in Perfetto
    let thread_names = [
        (1, "Activity"),
        (2, "LocalActivity"),
        (3, "Timer"),
        (4, "ChildWorkflow"),
        (5, "SignalWait"),
        (6, "SideEffect"),
    ];

    for (tid, name) in thread_names {
        events.push(json!({
            "name": "thread_name",
            "ph": "M",
            "pid": 1,
            "tid": tid,
            "args": {
                "name": name
            }
        }));
    }

    for (i, step) in timeline.steps.iter().enumerate() {
        let name = step.name.as_deref().unwrap_or("unnamed");

        let tid = match step.step_kind {
            StepKind::Activity => 1,
            StepKind::LocalActivity => 2,
            StepKind::Timer => 3,
            StepKind::ChildWorkflow => 4,
            StepKind::SignalWait => 5,
            StepKind::SideEffect => 6,
        };

        let ts_micros = step.scheduled_at.timestamp_micros();
        let dur_micros = step.total_ms.saturating_mul(1000);

        let cat = serde_json::to_string(&step.step_kind)
            .unwrap_or_else(|_| "\"unknown\"".to_string())
            .replace('"', "");

        events.push(json!({
            "name": name,
            "cat": cat,
            "ph": "X",
            "ts": ts_micros,
            "dur": dur_micros,
            "pid": 1,
            "tid": tid,
            "args": {
                "step_index": i,
                "status": if step.ended_at.is_some() { "completed" } else { "open" }
            }
        }));
    }

    json!(events)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timeline::{StepKind, TimelineRollup, TimelineStep};
    use chrono::{TimeZone, Utc};
    use std::collections::BTreeMap;

    fn test_timeline() -> Timeline {
        let dt1 = Utc.with_ymd_and_hms(2023, 1, 1, 12, 0, 0).unwrap();
        let dt2 = Utc.with_ymd_and_hms(2023, 1, 1, 12, 0, 5).unwrap();

        Timeline {
            exec_id: "exec-1".to_string(),
            workflow_id: "wf-1".to_string(),
            workflow_name: "test_flow".to_string(),
            state: "COMPLETED".to_string(),
            steps: vec![
                TimelineStep {
                    step_kind: StepKind::Activity,
                    name: Some("fetch_data".to_string()),
                    scheduled_at: dt1,
                    ended_at: Some(dt2),
                    total_ms: 5000,
                    wait_ms: None,
                    exec_ms: None,
                    attempt: None,
                    outcome: crate::timeline::StepOutcome::Completed,
                },
                TimelineStep {
                    step_kind: StepKind::Timer,
                    name: Some("timer-1".to_string()),
                    scheduled_at: dt2,
                    ended_at: None, // Open step
                    total_ms: 1000,
                    wait_ms: None,
                    exec_ms: None,
                    attempt: None,
                    outcome: crate::timeline::StepOutcome::Pending,
                },
            ],
            rollup: TimelineRollup {
                total_wall_clock_ms: 6000,
                busy_ms: 5000,
                wait_ms: 1000,
                slowest_step: None,
                step_count_by_kind: BTreeMap::new(),
            },
        }
    }

    #[test]
    fn test_export_timeline_mermaid_gantt() {
        let timeline = test_timeline();
        let gantt = export_timeline_mermaid_gantt(&timeline).unwrap();

        assert!(gantt.contains("gantt"));
        assert!(gantt.contains("title Execution Timeline: test_flow"));
        assert!(gantt.contains("dateFormat  X"));
        assert!(gantt.contains("axisFormat  %S"));

        // Check activity step formatting
        assert!(gantt.contains("[Activity] fetch_data :t0"));
        assert!(gantt.contains("5s"));

        // Check timer step formatting
        assert!(gantt.contains("[Timer] timer-1 :t1"));
        assert!(gantt.contains("1s"));
    }

    #[test]
    fn test_export_timeline_chrome_trace() {
        let timeline = test_timeline();
        let trace = export_timeline_chrome_trace(&timeline);

        let arr = trace.as_array().expect("Expected JSON array");

        // 6 metadata events + 2 step events
        assert_eq!(arr.len(), 8);

        // Find activity event
        let step_event1 = arr.iter().find(|e| e["name"] == "fetch_data").unwrap();
        assert_eq!(step_event1["cat"], "activity");
        assert_eq!(step_event1["tid"], 1);
        assert_eq!(step_event1["dur"], 5_000_000);
        assert_eq!(step_event1["args"]["status"], "completed");
        assert_eq!(step_event1["args"]["step_index"], 0);

        // Find timer event
        let step_event2 = arr.iter().find(|e| e["name"] == "timer-1").unwrap();
        assert_eq!(step_event2["cat"], "timer");
        assert_eq!(step_event2["tid"], 3);
        assert_eq!(step_event2["dur"], 1_000_000);
        assert_eq!(step_event2["args"]["status"], "open");
        assert_eq!(step_event2["args"]["step_index"], 1);
    }
}
