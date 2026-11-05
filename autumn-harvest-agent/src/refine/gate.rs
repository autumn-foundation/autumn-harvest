//! The adoption gate of a refine run.
//!
//! When `agent_refine` chooses an overlay, [`run_gate`] decides whether it
//! goes live. The design is in `docs/design/DESIGN-2381.md`.
//!
//! 1. [`agent_refine_adopt`] calls [`adopt`]. A candidate that the policy
//!    allows goes live as `auto:<run>`. Any other candidate is put as an
//!    inactive version, with the provenance `refine:<run>`.
//! 2. The run drops each signal [`refine_approval_signal`] that arrived
//!    before this point, as eval does. A decision needs a reviewer who saw
//!    the open wait.
//! 3. The run waits on that signal until the approval timeout of its
//!    recorded policy passes. The deadline uses the durable clock, so a
//!    refused decision never moves it.
//! 4. [`agent_refine_decide`] applies an `approve` or an `edit` with a
//!    compare-and-swap on the version that was active at refine start. A
//!    pointer that moved ends the run as [`RefineStatus::Superseded`].
//!
//! The run sets the search attributes [`REFINE_KIND_ATTR`] and
//! [`REFINE_STATUS_ATTR`], so an operator can list the waiting runs.

use std::time::Duration;

use autumn_harvest::failure::{ActivityFailure, IntoActivityErrorString};
use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::prelude::*;
use autumn_harvest::types::ExecutionId;
use serde::{Deserialize, Serialize};

use super::adopt::{Adoption, RefineStatus, WaitReason, adopt};
#[cfg(feature = "eval")]
use super::policy::GateState;
use super::policy::RefinePolicy;
use crate::agent::overlay::{Ceilings, ProfileOverlay};
use crate::agent::overlay_store::{OverlayStoreError, OverlayVersion, Provenance};
#[cfg(feature = "eval")]
use crate::approval::{Decision, await_decision};
use crate::harness::{AgentHarness, truncate};

/// The prefix of the approval signal of a refine run.
pub const SIGNAL_REFINE_APPROVAL: &str = "refine_approval";

/// The search attribute that holds the kind of a refine run.
pub const REFINE_KIND_ATTR: &str = "refine_kind";

/// The search attribute that holds the status of a refine run.
pub const REFINE_STATUS_ATTR: &str = "refine_status";

/// The most characters of one note of the gate.
#[cfg(feature = "eval")]
const MAX_NOTE_CHARS: usize = 500;

/// The most notes of the gate. A later note replaces the last one, so a
/// flood of bad decisions cannot grow the report without a bound.
#[cfg(feature = "eval")]
const MAX_NOTES: usize = 16;

/// The error type of a store failure that no retry can fix.
const GATE_STORE_FAILED: &str = "RefineGateStoreFailed";

/// The approval signal of the refine run `run`.
#[must_use]
pub fn refine_approval_signal(run: ExecutionId) -> String {
    format!("{SIGNAL_REFINE_APPROVAL}:{run}")
}

/// A decision of a person for one refine run. It is the signal payload.
///
/// The `verdict` tag is the tag of [`Approval`](crate::approval::Approval).
/// An `edit` also reads the key `arguments` of `Approval::Edit`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "verdict", rename_all = "snake_case")]
pub enum RefineDecision {
    /// Activate the stored version.
    Approve,
    /// Put this overlay as a further version, then activate it. The run
    /// checks it against the ceilings again.
    Edit {
        /// The overlay to put.
        #[serde(alias = "arguments")]
        overlay: ProfileOverlay,
    },
    /// Activate nothing. The version stays stored and inactive.
    Reject {
        /// Why, for the report.
        #[serde(default)]
        reason: String,
    },
}

/// One event of the gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateEvent {
    /// The deadline passed with no decision.
    Deadline,
    /// A payload did not decode.
    Unreadable,
    /// A person rejected the version.
    Reject,
    /// The store activated the approved or edited version.
    Activated,
    /// The store or the ceilings refused the decision.
    Refused,
    /// The active version moved after refine start.
    Conflict,
}

/// What the gate does after one event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateStep {
    /// Wait for the next decision until the same deadline.
    Wait,
    /// End: a version went live.
    Adopted,
    /// End: a person rejected the version.
    Rejected,
    /// End: no decision came before the deadline.
    TimedOut,
    /// End: an operator moved the pointer first.
    Superseded,
}

impl GateStep {
    /// The run status of an end, or `None` for [`GateStep::Wait`].
    #[must_use]
    pub const fn status(self) -> Option<RefineStatus> {
        match self {
            Self::Wait => None,
            Self::Adopted => Some(RefineStatus::Adopted),
            Self::Rejected => Some(RefineStatus::Rejected),
            Self::TimedOut => Some(RefineStatus::TimedOut),
            Self::Superseded => Some(RefineStatus::Superseded),
        }
    }
}

/// The step of the gate after `event`.
///
/// Only [`GateEvent::Activated`] gives [`GateStep::Adopted`]. A refused or
/// unreadable decision keeps the wait. `formal/verus/src/refine_gate.rs`
/// proves it.
#[must_use]
#[allow(
    clippy::match_same_arms,
    reason = "one arm per event, as the Verus copy has"
)]
pub const fn gate_step(event: GateEvent) -> GateStep {
    match event {
        GateEvent::Deadline => GateStep::TimedOut,
        GateEvent::Unreadable => GateStep::Wait,
        GateEvent::Reject => GateStep::Rejected,
        GateEvent::Activated => GateStep::Adopted,
        GateEvent::Refused => GateStep::Wait,
        GateEvent::Conflict => GateStep::Superseded,
    }
}

/// The gate in the report of a refine run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GateRecord {
    /// The stored version of the chosen overlay.
    pub version: OverlayVersion,
    /// The version that was active at refine start. An approval expects it.
    pub expected: Option<OverlayVersion>,
    /// Why the version waited for a person. An auto adoption has none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait: Option<WaitReason>,
    /// The version that went live.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub adopted: Option<OverlayVersion>,
    /// The active version that won over the decision of a person.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded_by: Option<OverlayVersion>,
    /// The signals that arrived before the wait opened. The run dropped
    /// them.
    #[serde(default)]
    pub dropped: u32,
    /// Each decision that the run read.
    #[serde(default)]
    pub decisions: u32,
    /// Each refused or unreadable decision, and the reason of a reject.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
}

#[cfg(feature = "eval")]
impl GateRecord {
    const fn new(version: OverlayVersion, expected: Option<OverlayVersion>) -> Self {
        Self {
            version,
            expected,
            wait: None,
            adopted: None,
            superseded_by: None,
            dropped: 0,
            decisions: 0,
            notes: Vec::new(),
        }
    }

    /// Add one note, cut to [`MAX_NOTE_CHARS`]. Keep at most [`MAX_NOTES`].
    fn note(&mut self, text: &str) {
        let text = truncate(text, MAX_NOTE_CHARS);
        if self.notes.len() < MAX_NOTES {
            self.notes.push(text);
        } else if let Some(last) = self.notes.last_mut() {
            *last = text;
        }
    }
}

/// The input of [`agent_refine_adopt`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdoptRequest {
    /// The refined kind.
    pub kind: String,
    /// The chosen overlay.
    pub candidate: ProfileOverlay,
    /// The refine run.
    pub run: ExecutionId,
    /// The policy that the run read.
    pub policy: RefinePolicy,
}

/// What [`agent_refine_decide`] does.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum GateAction {
    /// Activate the stored version.
    Approve,
    /// Check, put and activate this overlay.
    Edit {
        /// The edited overlay.
        overlay: ProfileOverlay,
    },
}

/// The input of [`agent_refine_decide`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecideRequest {
    /// The refined kind.
    pub kind: String,
    /// The refine run.
    pub run: ExecutionId,
    /// The stored version of the chosen overlay.
    pub version: OverlayVersion,
    /// The version that was active at refine start.
    pub expected: Option<OverlayVersion>,
    /// What to do.
    pub action: GateAction,
    /// The resolved ceilings of the run.
    pub ceilings: Ceilings,
}

/// The output of [`agent_refine_decide`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum DecideOutcome {
    /// The version is active now.
    Activated {
        /// The active version.
        version: OverlayVersion,
    },
    /// The ceilings or the store refused the decision. Nothing changed.
    Refused {
        /// Why.
        reason: String,
    },
    /// The active version is not the expected one. Nothing was activated.
    Conflict {
        /// The active version.
        active: Option<OverlayVersion>,
    },
}

/// The error payload of a store error. Only a backend error is retryable.
fn store_failure(err: &OverlayStoreError) -> String {
    match err {
        OverlayStoreError::Backend { .. } => err.to_string(),
        other => ActivityFailure::non_retryable(GATE_STORE_FAILED, other.to_string())
            .into_error_payload(),
    }
}

/// The error payload of a worker with no overlay store.
fn no_store() -> String {
    ActivityFailure::non_retryable(
        "OverlayStoreMissing",
        "no overlay store is installed: pass one to AgentHarness::overlay_store",
    )
    .into_error_payload()
}

/// Run [`adopt`] for the chosen overlay of a refine run.
///
/// # Errors
///
/// Returns a retryable error for a backend failure or a moved pointer, and
/// a non-retryable payload for any other store error.
pub(crate) async fn run_adopt(
    harness: &AgentHarness,
    request: AdoptRequest,
) -> Result<Adoption, String> {
    let store = harness.overlays().ok_or_else(no_store)?;
    adopt(
        store.as_ref(),
        &request.kind,
        &request.candidate,
        request.run,
        &request.policy,
    )
    .await
    .map_err(|err| match err {
        OverlayStoreError::Conflict { .. } => err.to_string(),
        other => store_failure(&other),
    })
}

/// The adoption step of a refine run, as an activity. See [`run_adopt`].
///
/// # Errors
///
/// The errors of [`run_adopt`].
#[activity(
    start_to_close = "1m",
    retry = RetryPolicy::exponential(5, Duration::from_secs(1))
)]
pub async fn agent_refine_adopt(
    ctx: &ActivityContext,
    request: AdoptRequest,
) -> Result<Adoption, String> {
    run_adopt(crate::workflow::harness(ctx)?, request).await
}

/// Apply one decision of a person to the store.
///
/// An `edit` must apply to the profile of the kind under the run ceilings.
/// The store then checks it under its current policy. A retry finds the
/// version that an earlier attempt put, and a move that an earlier attempt
/// made.
///
/// # Errors
///
/// Returns a retryable error for a backend failure, and a non-retryable
/// payload for any other store error.
pub(crate) async fn run_decide(
    harness: &AgentHarness,
    request: DecideRequest,
) -> Result<DecideOutcome, String> {
    let DecideRequest {
        kind,
        run,
        version,
        expected,
        action,
        ceilings,
    } = request;
    let store = harness.overlays().ok_or_else(no_store)?;
    let own = Provenance::refine(run);
    let target = match action {
        GateAction::Approve => version,
        GateAction::Edit { overlay } => {
            let profile = harness.agent_profile(&kind).ok_or_else(|| {
                ActivityFailure::non_retryable(
                    GATE_STORE_FAILED,
                    format!(
                        "the harness has no profile of the kind {:?}",
                        truncate(&kind, crate::mesh::MAX_NAME_LEN)
                    ),
                )
                .into_error_payload()
            })?;
            if let Err(err) = crate::agent::overlay::apply(&profile, &overlay, &ceilings, harness) {
                return Ok(DecideOutcome::Refused {
                    reason: err.to_string(),
                });
            }
            let versions = store.list(&kind).await.map_err(|e| store_failure(&e))?;
            let earlier = versions
                .iter()
                .rev()
                .find(|stored| stored.provenance() == &own && stored.overlay() == &overlay);
            match earlier {
                Some(stored) => stored.version(),
                None => match store.put(&kind, &overlay, own.clone()).await {
                    Ok(put) => put,
                    Err(
                        err @ (OverlayStoreError::Overlay(_)
                        | OverlayStoreError::TooLarge { .. }
                        | OverlayStoreError::UnknownKind { .. }),
                    ) => {
                        return Ok(DecideOutcome::Refused {
                            reason: err.to_string(),
                        });
                    }
                    Err(err) => return Err(store_failure(&err)),
                },
            }
        }
    };
    match store.activate(&kind, target, expected, own.clone()).await {
        Ok(()) => Ok(DecideOutcome::Activated { version: target }),
        Err(OverlayStoreError::Conflict { actual }) => {
            // A retry after a lost reply finds its own move.
            let last = store
                .last_activation(&kind)
                .await
                .map_err(|e| store_failure(&e))?;
            let own_move = actual == Some(target)
                && last.is_some_and(|last| last.to() == target && last.actor() == &own);
            if own_move {
                Ok(DecideOutcome::Activated { version: target })
            } else {
                Ok(DecideOutcome::Conflict { active: actual })
            }
        }
        Err(err @ (OverlayStoreError::Overlay(_) | OverlayStoreError::UnknownKind { .. })) => {
            Ok(DecideOutcome::Refused {
                reason: err.to_string(),
            })
        }
        Err(err) => Err(store_failure(&err)),
    }
}

/// One decision of a refine run, as an activity. See [`run_decide`].
///
/// # Errors
///
/// The errors of [`run_decide`].
#[activity(
    start_to_close = "1m",
    retry = RetryPolicy::exponential(5, Duration::from_secs(1))
)]
pub async fn agent_refine_decide(
    ctx: &ActivityContext,
    request: DecideRequest,
) -> Result<DecideOutcome, String> {
    run_decide(crate::workflow::harness(ctx)?, request).await
}

#[cfg(feature = "eval")]
/// Set the search attributes of a refine run of `kind` with `status`.
///
/// The write is metadata only. A failure writes a warning and never fails
/// the run.
pub(crate) fn set_status(ctx: &WorkflowContext, kind: &str, status: RefineStatus) {
    let patch = [
        (
            REFINE_KIND_ATTR.to_owned(),
            Some(serde_json::Value::from(kind)),
        ),
        (
            REFINE_STATUS_ATTR.to_owned(),
            Some(serde_json::Value::from(status.as_str())),
        ),
    ];
    if let Err(err) = ctx.upsert_search_attrs(patch) {
        tracing::warn!(error = %err, "the refine status attributes were not set");
    }
}

#[cfg(feature = "eval")]
/// What the gate of one refine run reads.
pub(crate) struct Gate<'a> {
    /// The workflow context of the run.
    pub ctx: &'a WorkflowContext,
    /// The refined kind.
    pub kind: &'a str,
    /// The chosen overlay.
    pub chosen: &'a ProfileOverlay,
    /// The version that was active at refine start.
    pub expected: Option<OverlayVersion>,
    /// The policy that the run read.
    pub policy: &'a RefinePolicy,
    /// The resolved ceilings of the run.
    pub ceilings: &'a Ceilings,
}

#[cfg(feature = "eval")]
/// The failure payload of a gate step that failed with `err`.
fn gate_failure(what: &str, err: &HarvestError) -> String {
    ActivityFailure::non_retryable(
        "RefineGateFailed",
        truncate(&format!("{what}: {err}"), MAX_NOTE_CHARS),
    )
    .into_error_payload()
}

#[cfg(feature = "eval")]
impl Gate<'_> {
    /// Run the gate. Returns the status of the run and the gate record.
    /// The record is `None` when the store found no change.
    ///
    /// # Errors
    ///
    /// Returns a failure payload when an activity fails for good, or when
    /// the wait fails.
    pub(crate) async fn run(self) -> Result<(RefineStatus, Option<GateRecord>), String> {
        let run = self.ctx.execution_id();
        let request = AdoptRequest {
            kind: self.kind.to_owned(),
            candidate: self.chosen.clone(),
            run,
            policy: self.policy.clone(),
        };
        let adoption: Adoption = self
            .ctx
            .execute_activity(&agent_refine_adopt_info(), request)
            .await
            .map_err(|err| gate_failure("the adoption step failed", &err))?;
        let Some(version) = adoption.version else {
            return Ok((RefineStatus::Unchanged, None));
        };
        let mut record = GateRecord::new(version, self.expected);
        if adoption.refine_status == RefineStatus::AutoAdopted {
            record.adopted = Some(version);
            return Ok((RefineStatus::AutoAdopted, Some(record)));
        }
        record.wait = adoption.wait;
        let name = refine_approval_signal(run);
        let early = self
            .ctx
            .drain_signals_raw(&name)
            .map_err(|err| gate_failure("the early signals were not read", &err))?;
        record.dropped = u32::try_from(early.len()).unwrap_or(u32::MAX);
        let opened = self.ctx.system_now();
        set_status(self.ctx, self.kind, RefineStatus::AwaitingApproval);
        loop {
            let event = self.next(&name, opened, &mut record).await?;
            if let Some(status) = gate_step(event).status() {
                return Ok((status, Some(record)));
            }
        }
    }

    /// Wait for the next decision, and apply it. Returns its event.
    async fn next(
        &self,
        name: &str,
        opened: autumn_harvest::chrono::DateTime<autumn_harvest::chrono::Utc>,
        record: &mut GateRecord,
    ) -> Result<GateEvent, String> {
        let now = self.ctx.system_now();
        let GateState::Waiting { until } = self.policy.gate_state(opened, now) else {
            return Ok(GateEvent::Deadline);
        };
        let left = (until - now).to_std().unwrap_or(Duration::ZERO);
        let decision = await_decision::<RefineDecision>(self.ctx, name, left)
            .await
            .map_err(|err| {
                ActivityFailure::non_retryable("RefineGateFailed", truncate(&err, MAX_NOTE_CHARS))
                    .into_error_payload()
            })?;
        let action = match decision {
            Decision::TimedOut => return Ok(GateEvent::Deadline),
            Decision::Unreadable(why) => {
                record.decisions = record.decisions.saturating_add(1);
                record.note(&format!("an unreadable decision was ignored: {why}"));
                return Ok(GateEvent::Unreadable);
            }
            Decision::Decided(RefineDecision::Reject { reason }) => {
                record.decisions = record.decisions.saturating_add(1);
                if !reason.is_empty() {
                    record.note(&format!("rejected: {reason}"));
                }
                return Ok(GateEvent::Reject);
            }
            Decision::Decided(RefineDecision::Approve) => GateAction::Approve,
            Decision::Decided(RefineDecision::Edit { overlay }) => GateAction::Edit { overlay },
        };
        record.decisions = record.decisions.saturating_add(1);
        let request = DecideRequest {
            kind: self.kind.to_owned(),
            run: self.ctx.execution_id(),
            version: record.version,
            expected: self.expected,
            action,
            ceilings: self.ceilings.clone(),
        };
        let outcome: DecideOutcome = self
            .ctx
            .execute_activity(&agent_refine_decide_info(), request)
            .await
            .map_err(|err| gate_failure("the decision step failed", &err))?;
        Ok(match outcome {
            DecideOutcome::Activated { version } => {
                record.adopted = Some(version);
                GateEvent::Activated
            }
            DecideOutcome::Refused { reason } => {
                record.note(&format!("a decision was refused: {reason}"));
                GateEvent::Refused
            }
            DecideOutcome::Conflict { active } => {
                record.superseded_by = active;
                GateEvent::Conflict
            }
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use proptest::prelude::*;
    use serde_json::json;

    const EVENTS: [GateEvent; 6] = [
        GateEvent::Deadline,
        GateEvent::Unreadable,
        GateEvent::Reject,
        GateEvent::Activated,
        GateEvent::Refused,
        GateEvent::Conflict,
    ];

    /// The end of a gate that reads `events` in order, or `None` when it
    /// still waits.
    fn fold(events: &[GateEvent]) -> Option<GateStep> {
        events
            .iter()
            .map(|event| gate_step(*event))
            .find(|step| *step != GateStep::Wait)
    }

    #[test]
    fn each_event_has_its_step() {
        assert_eq!(gate_step(GateEvent::Deadline), GateStep::TimedOut);
        assert_eq!(gate_step(GateEvent::Unreadable), GateStep::Wait);
        assert_eq!(gate_step(GateEvent::Reject), GateStep::Rejected);
        assert_eq!(gate_step(GateEvent::Activated), GateStep::Adopted);
        assert_eq!(gate_step(GateEvent::Refused), GateStep::Wait);
        assert_eq!(gate_step(GateEvent::Conflict), GateStep::Superseded);
    }

    #[test]
    fn each_end_maps_to_its_status_name() {
        let names: Vec<_> = [
            GateStep::Adopted,
            GateStep::Rejected,
            GateStep::TimedOut,
            GateStep::Superseded,
        ]
        .iter()
        .map(|step| step.status().unwrap().as_str())
        .collect();
        assert_eq!(names, ["adopted", "rejected", "timed_out", "superseded"]);
        assert_eq!(GateStep::Wait.status(), None);
        assert_eq!(RefineStatus::AwaitingApproval.as_str(), "waiting_approval");
    }

    #[test]
    fn the_signal_names_its_run() {
        let run = ExecutionId::new();
        assert_eq!(
            refine_approval_signal(run),
            format!("refine_approval:{run}")
        );
    }

    #[test]
    fn a_decision_reads_the_approval_tag() {
        let approve: RefineDecision =
            serde_json::from_value(json!({"verdict": "approve"})).unwrap();
        assert_eq!(approve, RefineDecision::Approve);
        let edit: RefineDecision =
            serde_json::from_value(json!({"verdict": "edit", "overlay": {"system": "x"}})).unwrap();
        assert!(matches!(edit, RefineDecision::Edit { .. }));
        let reject: RefineDecision =
            serde_json::from_value(json!({"verdict": "reject", "reason": "no"})).unwrap();
        assert_eq!(
            reject,
            RefineDecision::Reject {
                reason: "no".to_owned()
            }
        );
    }

    #[test]
    fn an_edit_with_a_policy_key_does_not_decode() {
        let bad = json!({"verdict": "edit", "overlay": {"auto_adopt": {"parts": ["system"]}}});
        assert!(serde_json::from_value::<RefineDecision>(bad).is_err());
    }

    #[cfg(feature = "eval")]
    #[test]
    fn the_notes_stay_bounded() {
        let mut record = GateRecord::new(OverlayVersion::new(1).unwrap(), None);
        for n in 0..100 {
            record.note(&format!("note {n} {}", "x".repeat(1_000)));
        }
        assert_eq!(record.notes.len(), MAX_NOTES);
        assert!(record.notes.last().unwrap().starts_with("note 99"));
        assert!(record.notes.iter().all(|note| note.chars().count()
            <= MAX_NOTE_CHARS + crate::harness::TRUNCATED.chars().count()));
    }

    proptest! {
        #[test]
        fn only_an_activation_adopts(picks in proptest::collection::vec(0..EVENTS.len(), 0..12)) {
            let events: Vec<GateEvent> = picks.iter().map(|&at| EVENTS[at]).collect();
            let end = fold(&events);
            if end == Some(GateStep::Adopted) {
                let first = events
                    .iter()
                    .position(|event| gate_step(*event) != GateStep::Wait)
                    .unwrap();
                prop_assert_eq!(events[first], GateEvent::Activated);
            }
            if !events.contains(&GateEvent::Activated) {
                prop_assert_ne!(end, Some(GateStep::Adopted));
            }
        }

        #[test]
        fn an_end_is_final(
            picks in proptest::collection::vec(0..EVENTS.len(), 1..12),
            more in proptest::collection::vec(0..EVENTS.len(), 0..12),
        ) {
            let events: Vec<GateEvent> = picks.iter().map(|&at| EVENTS[at]).collect();
            let end = fold(&events);
            prop_assume!(end.is_some());
            let mut longer = events;
            longer.extend(more.iter().map(|&at| EVENTS[at]));
            prop_assert_eq!(fold(&longer), end);
        }
    }
}
