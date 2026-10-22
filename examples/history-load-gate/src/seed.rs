//! A synthetic history for the load measurements.

use autumn_harvest::WorkflowEvent;
use autumn_harvest::types::ActivityExecId;
use chrono::{TimeZone, Utc};
use serde_json::{Value, json};
use uuid::Uuid;

/// A history of `events` events.
///
/// Event 0 is `WorkflowStarted`. The rest alternate `ActivityScheduled`
/// and `ActivityCompleted`. Each payload holds `payload_bytes` of text.
#[must_use]
pub fn history(events: usize, payload_bytes: usize) -> Vec<WorkflowEvent> {
    let pad = "x".repeat(payload_bytes);
    let mut out = Vec::with_capacity(events.max(1));
    out.push(WorkflowEvent::WorkflowStarted {
        input: payload(&pad, 0),
        timestamp: Utc
            .timestamp_opt(1_790_000_000, 0)
            .single()
            .unwrap_or_default(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    });
    for index in 1..events {
        // Events 1 and 2 share activity 1, events 3 and 4 activity 2.
        let step = index.div_ceil(2);
        let activity_id = ActivityExecId::from_uuid(Uuid::from_u128(step as u128));
        out.push(if index % 2 == 1 {
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "step".to_owned(),
                input: payload(&pad, step),
                queue: "default".to_owned(),
            }
        } else {
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: payload(&pad, step),
            }
        });
    }
    out
}

/// One payload: the step number and the padding text.
fn payload(pad: &str, step: usize) -> Value {
    json!({ "step": step, "pad": pad })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_history_has_the_requested_length_and_starts_the_run() {
        for events in [1, 2, 3, 10_000] {
            let history = history(events, 64);
            assert_eq!(history.len(), events);
            assert!(matches!(history[0], WorkflowEvent::WorkflowStarted { .. }));
        }
    }

    #[test]
    fn each_completion_follows_its_schedule() {
        let history = history(5, 8);
        let ids: Vec<_> = history
            .iter()
            .filter_map(|event| match event {
                WorkflowEvent::ActivityScheduled { activity_id, .. }
                | WorkflowEvent::ActivityCompleted { activity_id, .. } => Some(*activity_id),
                _ => None,
            })
            .collect();
        assert_eq!(ids.len(), 4);
        assert_eq!(ids[0], ids[1]);
        assert_eq!(ids[2], ids[3]);
        assert_ne!(ids[0], ids[2]);
        assert!(matches!(
            history[2],
            WorkflowEvent::ActivityCompleted { .. }
        ));
    }

    #[test]
    fn a_history_round_trips_through_its_stored_form() {
        let history = history(101, 32);
        let text = serde_json::to_string(&history).expect("encode");
        let back: Vec<WorkflowEvent> = serde_json::from_str(&text).expect("decode");
        assert_eq!(
            serde_json::to_value(&back).expect("value"),
            serde_json::to_value(&history).expect("value")
        );
    }

    #[test]
    fn the_payload_size_sets_the_stored_size() {
        let small = serde_json::to_string(&history(11, 16)).expect("encode");
        let large = serde_json::to_string(&history(11, 1_024)).expect("encode");
        assert!(large.len() > small.len() + 10 * 1_000);
    }
}
