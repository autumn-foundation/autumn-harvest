//! Whether a refined overlay goes live without a person (issue #2394).
//!
//! [`adopt`] hands a candidate to
//! [`ProfileOverlayStore::auto_adopt`]. The store runs the policy kernel
//! [`decide_adoption`](super::policy::decide_adoption) against the active
//! version. It puts and activates the version in one transaction, under
//! the daily cap. Any other candidate is put as `refine:<run>`. It waits
//! for the adoption gate of [`gate`](super::gate).

use std::collections::BTreeSet;

use autumn_harvest::ExecutionId;
use serde::{Deserialize, Serialize};

use super::policy::{OverlayPart, RefinePolicy};
use crate::agent::overlay::ProfileOverlay;
use crate::agent::overlay_store::{
    AutoAdoptOutcome, OverlayStoreError, OverlayVersion, ProfileOverlayStore, Provenance,
};
use crate::harness::truncate;
use crate::mesh::MAX_NAME_LEN;

/// The status of a refine run.
///
/// The loop of `agent_refine` gives `Chosen`, `Unchanged` or
/// `BudgetExhausted`. The adoption step and the gate give the other values.
/// [`RefineStatus::as_str`] is the value of the search attribute
/// `refine_status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RefineStatus {
    /// The candidate went live without a person.
    AutoAdopted,
    /// The candidate waits for a person. Its name is `waiting_approval`.
    /// A value written as `awaiting_approval` still decodes.
    #[serde(rename = "waiting_approval", alias = "awaiting_approval")]
    AwaitingApproval,
    /// The candidate changes no part. Nothing was stored.
    Unchanged,
    /// The adoption gate waited for its whole timeout. See
    /// [`GateState::TimedOut`](super::policy::GateState::TimedOut).
    TimedOut,
    /// The refine run chose an overlay that differs from the active one
    /// (issue #2380). The adoption step comes next.
    Chosen,
    /// The refine run spent its budget before its plan ended (issue
    /// #2380). It chose no overlay.
    BudgetExhausted,
    /// A person approved the candidate, or an edit of it, and it went live.
    Adopted,
    /// A person rejected the candidate. It stays stored and inactive.
    Rejected,
    /// The active version moved after refine start, so the approval
    /// activated nothing.
    Superseded,
}

impl RefineStatus {
    /// The name of the status: the value of the search attribute
    /// `refine_status`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AutoAdopted => "auto_adopted",
            Self::AwaitingApproval => "waiting_approval",
            Self::Unchanged => "unchanged",
            Self::TimedOut => "timed_out",
            Self::Chosen => "chosen",
            Self::BudgetExhausted => "budget_exhausted",
            Self::Adopted => "adopted",
            Self::Rejected => "rejected",
            Self::Superseded => "superseded",
        }
    }
}

/// Why a candidate waits for a person.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum WaitReason {
    /// The candidate changes parts that the policy does not allow.
    PartsNeedApproval {
        /// The changed parts that the policy does not allow.
        parts: BTreeSet<OverlayPart>,
    },
    /// The auto adoptions of the UTC day reach the daily cap.
    DailyCap {
        /// The daily cap.
        cap: u32,
    },
}

impl std::fmt::Display for WaitReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PartsNeedApproval { parts } => {
                let names: Vec<_> = parts.iter().map(|part| part.name()).collect();
                write!(
                    f,
                    "the policy does not auto-adopt the parts {}",
                    names.join(", ")
                )
            }
            Self::DailyCap { cap } => write!(
                f,
                "the daily cap of {cap} auto adoption(s) is reached (auto_adopt.max_per_day)"
            ),
        }
    }
}

/// The result of [`adopt`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Adoption {
    /// The status of the refine run.
    pub refine_status: RefineStatus,
    /// The stored version, or `None` when nothing changed.
    pub version: Option<OverlayVersion>,
    /// Why the candidate waits, if it does.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait: Option<WaitReason>,
    /// The active version when it was not the expected one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded_by: Option<OverlayVersion>,
}

/// Adopt `candidate` for `kind` over the active version, or store it to
/// wait for a person. See [`adopt_over`].
///
/// # Errors
///
/// The errors of the store. [`OverlayStoreError::Conflict`] means that the
/// pointer moved after the read. A retry reads it again.
pub async fn adopt(
    store: &dyn ProfileOverlayStore,
    kind: &str,
    candidate: &ProfileOverlay,
    run: ExecutionId,
    policy: &RefinePolicy,
) -> Result<Adoption, OverlayStoreError> {
    let expected = store.active(kind).await?.map(|stored| stored.version());
    let adoption = adopt_over(store, kind, candidate, run, expected, policy).await?;
    if adoption.refine_status == RefineStatus::Superseded {
        return Err(OverlayStoreError::Conflict {
            actual: adoption.superseded_by,
        });
    }
    Ok(adoption)
}

/// Adopt `candidate` for `kind` over the version `expected`, or store it
/// to wait for a person.
///
/// `run` is the refine run. `policy` is the policy that the run read once.
/// The store checks it with its current policy, and the stricter of the
/// two decides. The cap counts in the UTC day of the store clock.
///
/// - A candidate equal to the active overlay stores nothing:
///   [`RefineStatus::Unchanged`].
/// - A candidate whose changed parts both policies allow goes live as
///   `auto:<run>`, when the daily cap allows it:
///   [`RefineStatus::AutoAdopted`].
/// - Any other candidate is put as `refine:<run>`:
///   [`RefineStatus::AwaitingApproval`]. A wait on the cap writes a
///   `harvest.audit` tracing event that names the cap.
///
/// A retry after a lost reply finds its own `auto:<run>` version, and
/// reports it as adopted with no move. The version need not be active, so
/// a retry never undoes a rollback or a later adoption. A retry also finds
/// its own `refine:<run>` version of the same overlay first. It then waits
/// with no reason and puts no second version. So a new day or a wider
/// policy never changes the outcome of a retry.
///
/// # Errors
///
/// The errors of the store. A pointer that is not `expected` gives
/// [`RefineStatus::Superseded`], not an error, so a candidate built on an
/// older version never goes live.
pub async fn adopt_over(
    store: &dyn ProfileOverlayStore,
    kind: &str,
    candidate: &ProfileOverlay,
    run: ExecutionId,
    expected: Option<OverlayVersion>,
    policy: &RefinePolicy,
) -> Result<Adoption, OverlayStoreError> {
    let own = Provenance::auto(run);
    let versions = store.list(kind).await?;
    if let Some(done) = versions.iter().find(|stored| stored.provenance() == &own) {
        return Ok(Adoption {
            refine_status: RefineStatus::AutoAdopted,
            version: Some(done.version()),
            wait: None,
            superseded_by: None,
        });
    }
    let waits = Provenance::refine(run);
    if let Some(earlier) = versions
        .iter()
        .find(|stored| stored.provenance() == &waits && stored.overlay() == candidate)
    {
        // A retry keeps the outcome of its first attempt.
        return Ok(Adoption {
            refine_status: RefineStatus::AwaitingApproval,
            version: Some(earlier.version()),
            wait: None,
            superseded_by: None,
        });
    }
    let outcome = match store
        .auto_adopt(kind, candidate, run, expected, policy)
        .await
    {
        Ok(outcome) => outcome,
        Err(OverlayStoreError::Conflict { actual }) => {
            return Ok(Adoption {
                refine_status: RefineStatus::Superseded,
                version: None,
                wait: None,
                superseded_by: actual,
            });
        }
        Err(err) => return Err(err),
    };
    let wait = match outcome {
        AutoAdoptOutcome::Adopted(version) => {
            return Ok(Adoption {
                refine_status: RefineStatus::AutoAdopted,
                version: Some(version),
                wait: None,
                superseded_by: None,
            });
        }
        AutoAdoptOutcome::Unchanged => {
            return Ok(Adoption {
                refine_status: RefineStatus::Unchanged,
                version: None,
                wait: None,
                superseded_by: None,
            });
        }
        AutoAdoptOutcome::NeedsApproval { parts } => WaitReason::PartsNeedApproval { parts },
        AutoAdoptOutcome::CapReached { cap, .. } => WaitReason::DailyCap { cap },
    };
    if let WaitReason::DailyCap { cap } = &wait {
        tracing::info!(
            target: "harvest.audit",
            kind = %truncate(kind, MAX_NAME_LEN),
            run = %run,
            cap = *cap,
            reason = %wait,
            "a refined overlay waits: the auto adoption cap is reached"
        );
    }
    let version = store.put(kind, candidate, waits).await?;
    Ok(Adoption {
        refine_status: RefineStatus::AwaitingApproval,
        version: Some(version),
        wait: Some(wait),
        superseded_by: None,
    })
}
