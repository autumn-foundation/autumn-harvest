//! Whether a refined overlay goes live without a person (issue #2394).
//!
//! [`adopt`] hands a candidate to
//! [`ProfileOverlayStore::auto_adopt`]. The store runs the policy kernel
//! [`decide_adoption`](super::policy::decide_adoption) against the active
//! version. It puts and activates the version in one transaction, under
//! the daily cap. Any other candidate is put as `refine:<run>`. It waits
//! for the adoption gate of issue #2381.

use std::collections::BTreeSet;

use autumn_harvest::ExecutionId;
use serde::{Deserialize, Serialize};

use super::policy::{OverlayPart, RefinePolicy};
use crate::agent::overlay::ProfileOverlay;
use crate::agent::overlay_store::{
    AutoAdoptOutcome, OverlayStoreError, OverlayVersion, ProfileOverlayStore, Provenance,
    StoredOverlay,
};
use crate::harness::truncate;
use crate::mesh::MAX_NAME_LEN;

/// The status of a refine run after its adoption step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RefineStatus {
    /// The candidate went live without a person.
    AutoAdopted,
    /// The candidate waits for a person.
    AwaitingApproval,
    /// The candidate changes no part. Nothing was stored.
    Unchanged,
    /// The adoption gate waited for its whole timeout. See
    /// [`GateState::TimedOut`](super::policy::GateState::TimedOut).
    TimedOut,
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
}

/// Adopt `candidate` for `kind`, or store it to wait for a person.
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
/// A retry after a lost reply finds its own `auto:<run>` version active,
/// and reports it as adopted.
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
    let active = store.active(kind).await?;
    if let Some(stored) = &active
        && stored.provenance() == &Provenance::auto(run)
        && stored.overlay() == candidate
    {
        return Ok(Adoption {
            refine_status: RefineStatus::AutoAdopted,
            version: Some(stored.version()),
            wait: None,
        });
    }
    let expected = active.as_ref().map(StoredOverlay::version);
    let wait = match store
        .auto_adopt(kind, candidate, run, expected, policy)
        .await?
    {
        AutoAdoptOutcome::Adopted(version) => {
            return Ok(Adoption {
                refine_status: RefineStatus::AutoAdopted,
                version: Some(version),
                wait: None,
            });
        }
        AutoAdoptOutcome::Unchanged => {
            return Ok(Adoption {
                refine_status: RefineStatus::Unchanged,
                version: None,
                wait: None,
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
    let version = store.put(kind, candidate, Provenance::refine(run)).await?;
    Ok(Adoption {
        refine_status: RefineStatus::AwaitingApproval,
        version: Some(version),
        wait: Some(wait),
    })
}
