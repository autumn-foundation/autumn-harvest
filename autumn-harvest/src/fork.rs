//! Non-destructive fork of a workflow run (issue #2000).
//!
//! A fork copies a history prefix of a source run to a new execution with a
//! new workflow id. It never writes a source row. See `DESIGN-2000.md`.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use diesel_async::AsyncPgConnection;

pub use crate::event::ForkEffects;
use crate::context::WorkflowCommand;
use crate::error::HarvestError;
use crate::event::WorkflowEvent;
use crate::reset::{ResetInvalidPoint, ResetPoint};
use crate::types::{ActivityExecId, ExecutionId};
use crate::worker::HandlerRegistry;

/// Error type of an activity that a recorded fork cannot serve.
pub const ERROR_TYPE_FORK_EFFECT_UNAVAILABLE: &str = "ForkEffectUnavailable";

/// Request body of `POST /workflows/{id}/fork`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WorkflowForkRequest {
    /// Where the fork starts. `None` means event `0` (`WorkflowStarted`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fork_point: Option<ResetPoint>,
    /// Operator-supplied reason.
    #[serde(default)]
    pub reason: String,
    /// Operator identity for audit.
    #[serde(default)]
    pub operator_id: String,
    /// How the fork resolves an effect. The default is `recorded`.
    #[serde(default)]
    pub effects: ForkEffects,
    /// Workflow id of the fork. `None` derives a new id from the source id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,
    /// Input of the fork. Needs fork point `0`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<Value>,
    /// Results that replace the results of activities after the fork point.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub activity_overrides: Vec<ForkActivityOverride>,
}

/// One activity result that the caller sets for a fork.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForkActivityOverride {
    /// Name of the activity.
    pub activity_name: String,
    /// 1-based count of `ActivityScheduled` events with this name.
    pub occurrence: u32,
    /// The result that the activity returns in the fork.
    pub output: Value,
}

/// Result of a committed fork.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForkResult {
    /// The new execution.
    pub new_exec_id: ExecutionId,
    /// Workflow id of the new execution.
    pub workflow_id: String,
    /// The source execution. The fork did not change it.
    pub forked_from_exec_id: ExecutionId,
    /// Last source event copied into the fork.
    pub fork_event_id: i64,
    /// Number of source events copied into the fork.
    pub events_carried_over: usize,
    /// How the fork resolves an effect.
    pub effects: ForkEffects,
}

/// Errors of a fork.
#[derive(Debug, thiserror::Error)]
pub enum WorkflowForkError {
    /// The fork point is not a valid decision boundary.
    #[error(transparent)]
    InvalidPoint(#[from] ResetInvalidPoint),
    /// The source history holds a continue-as-new.
    #[error("continue-as-new histories cannot be forked")]
    ContinueAsNew,
    /// The source payloads were erased (issue #495).
    #[error("workflow execution {exec_id} had its payloads erased, so it cannot be forked")]
    ErasedSource {
        /// The source execution.
        exec_id: ExecutionId,
    },
    /// Recorded mode cannot serve an effect after the fork point.
    #[error(
        "recorded mode cannot serve {kind} at event {event_id}; fork with effects = live to run it"
    )]
    UnservableEffect {
        /// The event type of the effect.
        kind: String,
        /// The source event id of the effect.
        event_id: i64,
    },
    /// The carried history holds a mutex grant.
    #[error(
        "event {event_id} grants durable mutex '{key}'; a fork cannot inherit a mutex, so fork \
         before the grant"
    )]
    CarriedMutex {
        /// The mutex key.
        key: String,
        /// The source event id of the grant.
        event_id: i64,
    },
    /// An input or an activity override is not valid.
    #[error("invalid fork override: {message}")]
    InvalidOverride {
        /// What is wrong.
        message: String,
    },
    /// Another live run holds the workflow id.
    #[error("workflow id '{workflow_id}' is in use by another run")]
    WorkflowIdInUse {
        /// The workflow id.
        workflow_id: String,
    },
    /// A storage or database error.
    #[error(transparent)]
    Harvest(#[from] HarvestError),
}

impl From<diesel::result::Error> for WorkflowForkError {
    fn from(error: diesel::result::Error) -> Self {
        Self::Harvest(crate::error::database_error(error))
    }
}

/// Fork `source_id` to a new execution. The source is never changed.
///
/// # Errors
///
/// Returns [`WorkflowForkError`] when the source or the request is not valid,
/// or when a write fails.
pub async fn fork_workflow_execution(
    conn: &mut AsyncPgConnection,
    source_id: ExecutionId,
    request: WorkflowForkRequest,
    registry: Option<&HandlerRegistry>,
) -> Result<ForkResult, WorkflowForkError> {
    let _ = (conn, source_id, request, registry);
    Err(WorkflowForkError::Harvest(HarvestError::Config(
        "fork is not implemented".to_string(),
    )))
}

/// The first command in `commands` that recorded mode cannot serve.
///
/// Returns the command name. A recorded fork fails before such a command
/// runs.
#[must_use]
pub fn live_effect_refusal(commands: &[WorkflowCommand]) -> Option<&'static str> {
    let _ = commands;
    todo!("issue #2000")
}

/// The terminal event that the fork appends for its activity `activity_id`.
///
/// The order is: a caller override, then the source record with the same
/// input, then a non-retryable `ForkEffectUnavailable` failure.
#[must_use]
pub fn served_outcome(
    fork_events: &[WorkflowEvent],
    source_events: &[WorkflowEvent],
    activity_id: ActivityExecId,
) -> Option<WorkflowEvent> {
    let _ = (fork_events, source_events, activity_id);
    todo!("issue #2000")
}

/// Check a fork point and the request against the source history.
///
/// # Errors
///
/// Returns [`WorkflowForkError`] when the fork cannot start at `fork_event_id`.
pub fn validate_fork(
    events: &[WorkflowEvent],
    fork_event_id: i64,
    request: &WorkflowForkRequest,
) -> Result<crate::reset::ResetPlan, WorkflowForkError> {
    let _ = (events, fork_event_id, request);
    todo!("issue #2000")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::types::{ExternalSignalId, ExternalTarget};

    fn started(input: Value) -> WorkflowEvent {
        WorkflowEvent::WorkflowStarted {
            input,
            timestamp: chrono::Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }
    }

    fn scheduled(id: ActivityExecId, name: &str, input: Value) -> WorkflowEvent {
        WorkflowEvent::ActivityScheduled {
            activity_id: id,
            name: name.to_string(),
            input,
            queue: "default".to_string(),
        }
    }

    fn completed(id: ActivityExecId, output: Value) -> WorkflowEvent {
        WorkflowEvent::ActivityCompleted {
            activity_id: id,
            output,
        }
    }

    fn marker(effects: ForkEffects) -> WorkflowEvent {
        WorkflowEvent::WorkflowForked {
            forked_from_exec_id: ExecutionId::new(),
            fork_event_id: 0,
            effects,
            reason: "test".to_string(),
            operator_id: "test".to_string(),
        }
    }

    /// A source that charged twice and completed.
    fn source() -> (Vec<WorkflowEvent>, ActivityExecId, ActivityExecId) {
        let first = ActivityExecId::new();
        let second = ActivityExecId::new();
        let events = vec![
            started(json!({})),
            scheduled(first, "charge", json!({ "n": 1 })),
            completed(first, json!("ch-1")),
            scheduled(second, "charge", json!({ "n": 2 })),
            completed(second, json!("ch-2")),
            WorkflowEvent::WorkflowCompleted {
                output: json!("done"),
            },
        ];
        (events, first, second)
    }

    fn output_of(event: Option<WorkflowEvent>) -> Value {
        match event {
            Some(WorkflowEvent::ActivityCompleted { output, .. }) => output,
            other => panic!("expected ActivityCompleted, got {other:?}"),
        }
    }

    #[test]
    fn effects_default_to_recorded() {
        assert_eq!(ForkEffects::default(), ForkEffects::Recorded);
        let request: WorkflowForkRequest = serde_json::from_value(json!({})).unwrap();
        assert_eq!(request.effects, ForkEffects::Recorded);
        let live: WorkflowForkRequest =
            serde_json::from_value(json!({ "effects": "live" })).unwrap();
        assert_eq!(live.effects, ForkEffects::Live);
        assert!(serde_json::from_value::<WorkflowForkRequest>(json!({ "effects": "LIVE" })).is_err());
    }

    #[test]
    fn a_fork_takes_the_recorded_result_by_occurrence() {
        let (source, _, _) = source();
        let fork_first = ActivityExecId::new();
        let fork_second = ActivityExecId::new();
        let fork = vec![
            started(json!({})),
            marker(ForkEffects::Recorded),
            scheduled(fork_first, "charge", json!({ "n": 1 })),
            completed(fork_first, json!("ch-1")),
            scheduled(fork_second, "charge", json!({ "n": 2 })),
        ];
        let served = served_outcome(&fork, &source, fork_second);
        assert_eq!(output_of(served.clone()), json!("ch-2"));
        match served {
            Some(WorkflowEvent::ActivityCompleted { activity_id, .. }) => {
                assert_eq!(activity_id, fork_second, "the fork id, not the source id");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn a_different_input_has_no_record() {
        let (source, _, _) = source();
        let id = ActivityExecId::new();
        let fork = vec![
            started(json!({})),
            marker(ForkEffects::Recorded),
            scheduled(id, "charge", json!({ "n": 99 })),
        ];
        match served_outcome(&fork, &source, id) {
            Some(WorkflowEvent::ActivityFailed {
                error_type,
                non_retryable,
                ..
            }) => {
                assert_eq!(error_type, ERROR_TYPE_FORK_EFFECT_UNAVAILABLE);
                assert!(non_retryable);
            }
            other => panic!("expected a ForkEffectUnavailable failure, got {other:?}"),
        }
    }

    #[test]
    fn an_erased_record_is_not_served() {
        let (mut source, first, _) = source();
        source[2] = completed(first, crate::erase::erasure_tombstone());
        let id = ActivityExecId::new();
        let fork = vec![
            started(json!({})),
            marker(ForkEffects::Recorded),
            scheduled(id, "charge", json!({ "n": 1 })),
        ];
        assert!(matches!(
            served_outcome(&fork, &source, id),
            Some(WorkflowEvent::ActivityFailed { .. })
        ));
    }

    #[test]
    fn an_override_wins_over_the_record() {
        let (source, _, _) = source();
        let id = ActivityExecId::new();
        let fork = vec![
            started(json!({})),
            marker(ForkEffects::Recorded),
            WorkflowEvent::ForkActivityResultOverridden {
                activity_name: "charge".to_string(),
                occurrence: 1,
                output: json!("stub"),
            },
            scheduled(id, "charge", json!({ "n": 1 })),
        ];
        assert_eq!(output_of(served_outcome(&fork, &source, id)), json!("stub"));
    }

    #[test]
    fn a_recorded_failure_is_served_as_a_failure() {
        let first = ActivityExecId::new();
        let source = vec![
            started(json!({})),
            scheduled(first, "charge", json!({})),
            WorkflowEvent::ActivityFailed {
                activity_id: first,
                error: "declined".to_string(),
                attempt: 3,
                error_type: "CardDeclined".to_string(),
                non_retryable: true,
                details: None,
            },
        ];
        let id = ActivityExecId::new();
        let fork = vec![
            started(json!({})),
            marker(ForkEffects::Recorded),
            scheduled(id, "charge", json!({})),
        ];
        match served_outcome(&fork, &source, id) {
            Some(WorkflowEvent::ActivityFailed {
                activity_id,
                error_type,
                ..
            }) => {
                assert_eq!(activity_id, id);
                assert_eq!(error_type, "CardDeclined");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn a_live_fork_serves_nothing() {
        let (source, _, _) = source();
        let id = ActivityExecId::new();
        let fork = vec![
            started(json!({})),
            marker(ForkEffects::Live),
            scheduled(id, "charge", json!({ "n": 1 })),
        ];
        assert!(served_outcome(&fork, &source, id).is_none());
    }

    #[test]
    fn an_activity_with_an_outcome_is_not_served_again() {
        let (source, _, _) = source();
        let id = ActivityExecId::new();
        let fork = vec![
            started(json!({})),
            marker(ForkEffects::Recorded),
            scheduled(id, "charge", json!({ "n": 1 })),
            completed(id, json!("race")),
        ];
        assert!(served_outcome(&fork, &source, id).is_none());
    }

    #[test]
    fn a_fork_point_after_the_terminal_event_is_refused() {
        let (source, _, _) = source();
        let error = validate_fork(&source, 5, &WorkflowForkRequest::default()).unwrap_err();
        assert!(matches!(error, WorkflowForkError::InvalidPoint(_)), "{error}");
        assert!(validate_fork(&source, 4, &WorkflowForkRequest::default()).is_ok());
        assert!(validate_fork(&source, 0, &WorkflowForkRequest::default()).is_ok());
    }

    #[test]
    fn a_carried_mutex_grant_is_refused() {
        let events = vec![
            started(json!({})),
            WorkflowEvent::MutexGranted {
                key: "acct-1".to_string(),
                lock_seq: 1,
                acquired_at: chrono::Utc::now(),
            },
        ];
        let error = validate_fork(&events, 1, &WorkflowForkRequest::default()).unwrap_err();
        assert!(
            matches!(error, WorkflowForkError::CarriedMutex { ref key, event_id: 1 } if key == "acct-1"),
            "{error}"
        );
        assert!(validate_fork(&events, 0, &WorkflowForkRequest::default()).is_ok());
    }

    #[test]
    fn recorded_mode_refuses_an_effect_it_cannot_serve() {
        let id = ActivityExecId::new();
        let events = vec![
            started(json!({})),
            WorkflowEvent::LocalActivityScheduled {
                activity_id: id,
                name: "lookup".to_string(),
                input: json!({}),
                resolved: true,
                retry_policy: None,
                start_to_close_nanos: None,
            },
            WorkflowEvent::LocalActivityCompleted {
                activity_id: id,
                output: json!({}),
            },
        ];
        let error = validate_fork(&events, 0, &WorkflowForkRequest::default()).unwrap_err();
        assert!(
            matches!(error, WorkflowForkError::UnservableEffect { ref kind, event_id: 1 } if kind == "LocalActivityScheduled"),
            "{error}"
        );
        let live = WorkflowForkRequest {
            effects: ForkEffects::Live,
            ..WorkflowForkRequest::default()
        };
        assert!(validate_fork(&events, 0, &live).is_ok());
        assert!(validate_fork(&events, 2, &WorkflowForkRequest::default()).is_ok());
    }

    #[test]
    fn an_input_override_needs_fork_point_zero() {
        let (source, _, _) = source();
        let request = WorkflowForkRequest {
            input: Some(json!({ "n": 5 })),
            ..WorkflowForkRequest::default()
        };
        assert!(validate_fork(&source, 0, &request).is_ok());
        let error = validate_fork(&source, 2, &request).unwrap_err();
        assert!(matches!(error, WorkflowForkError::InvalidOverride { .. }), "{error}");
    }

    #[test]
    fn an_override_must_target_an_activity_after_the_fork_point() {
        let (source, _, _) = source();
        let at = |occurrence| WorkflowForkRequest {
            activity_overrides: vec![ForkActivityOverride {
                activity_name: "charge".to_string(),
                occurrence,
                output: json!("stub"),
            }],
            ..WorkflowForkRequest::default()
        };
        assert!(validate_fork(&source, 2, &at(2)).is_ok());
        for bad in [0, 1] {
            let error = validate_fork(&source, 2, &at(bad)).unwrap_err();
            assert!(matches!(error, WorkflowForkError::InvalidOverride { .. }), "{error}");
        }
        let twice = WorkflowForkRequest {
            activity_overrides: [at(2).activity_overrides, at(2).activity_overrides].concat(),
            ..WorkflowForkRequest::default()
        };
        assert!(matches!(
            validate_fork(&source, 2, &twice),
            Err(WorkflowForkError::InvalidOverride { .. })
        ));
    }

    #[test]
    fn a_recorded_fork_refuses_a_live_effect_command() {
        let (tx, _rx) = tokio::sync::oneshot::channel();
        let signal = WorkflowCommand::SignalExternalWorkflow {
            signal_id: ExternalSignalId::new(),
            target: ExternalTarget::ExecutionId(ExecutionId::new()),
            signal_name: "go".to_string(),
            payload: json!({}),
            result_tx: tx,
            already_requested: false,
            idempotency_key: None,
        };
        let marker = WorkflowCommand::RecordMarker {
            name: "m".to_string(),
            details: json!({}),
        };
        assert_eq!(live_effect_refusal(&[marker]), None);
        let marker = WorkflowCommand::RecordMarker {
            name: "m".to_string(),
            details: json!({}),
        };
        assert_eq!(
            live_effect_refusal(&[marker, signal]),
            Some("SignalExternalWorkflow")
        );
    }
}
