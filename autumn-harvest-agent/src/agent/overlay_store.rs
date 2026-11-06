//! The versioned store of profile overlays (issue #2371).
//!
//! Each agent kind has a log of immutable overlay versions and one active
//! pointer. [`ProfileOverlayStore::put`] appends a version. A version never
//! changes after the put. [`ProfileOverlayStore::activate`] moves the
//! pointer with a compare-and-swap. A rollback activates an earlier version.
//!
//! Each move of the pointer logs an [`Activation`] with its actor (issue
//! #2374). [`ProfileOverlayStore::rollback`] reverses the last move.
//!
//! Each store validates an overlay with its [`OverlayValidator`] before it
//! stores it, and again before it activates it. A deploy can narrow a
//! profile after an activation. So a reader of [`ProfileOverlayStore::active`]
//! applies the overlay with [`overlay::apply`], which checks it again.
//!
//! A store does its work when the future is polled. The `SQLite` store runs
//! its statements on the calling thread, as `MemoryStore` stores do.
//!
//! Each store also holds the refine policy log of each kind (issue #2394).
//! [`ProfileOverlayStore::policy`] reads the current policy. Only an
//! operator writes it. The validator applies each overlay under the
//! ceilings of that policy. [`ProfileOverlayStore::auto_adopt`] puts and
//! activates a version in one transaction, under the daily cap.
//!
//! The design is in `docs/design/DESIGN-2371.md`,
//! `docs/design/DESIGN-2374.md` and `docs/design/DESIGN-2394.md`.
//!
//! [`overlay::apply`]: super::overlay::apply

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, PoisonError};

use autumn_harvest::ExecutionId;
use autumn_harvest::chrono::{DateTime, Utc};
use autumn_harvest::overlay_log::{
    AUTHOR_AUTO, AUTHOR_OPERATOR, AUTHOR_REFINE, ActivateDecision, ActivateOutcome, AutoAdoptRaw,
    AutoMoveDecision, MoveRequest, OverlayRow, PolicyExpect, decide_auto_move, logs_activation,
    next_overlay_version, policy_write_passes, rollback_target, utc_day,
};
#[cfg(any(feature = "sqlite", feature = "postgres"))]
use autumn_harvest::overlay_log::{AutoAdoptRequest, PolicyAppend, PolicyRow, PolicyWrite};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::overlay::{OverlayError, ProfileOverlay, apply};
use crate::harness::{AgentHarness, truncate};
use crate::mesh::MAX_NAME_LEN;
use crate::model::BoxFuture;
use crate::refine::policy::{
    AdoptDecision, DailyCap, OverlayPart, RefinePolicy, changed_parts, decide_adoption, mask,
};

/// The most characters in an operator id.
pub const MAX_AUTHOR_LEN: usize = 256;

/// The most bytes in the JSON of one stored overlay.
pub const MAX_OVERLAY_BYTES: usize = 64 * 1024;

/// The clock of a store (issue #2394). An auto adoption counts its daily
/// cap in the UTC day of this clock, and logs its move at this time.
///
/// The embedder sets it, never a caller of a store method. So a refine run
/// cannot pick the day of its cap. A test sets a fixed clock.
pub type StoreClock = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;

/// The system clock.
fn system_clock() -> StoreClock {
    Arc::new(Utc::now)
}

/// The number of one overlay version of a kind. The first version is 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OverlayVersion(u64);

impl OverlayVersion {
    /// The version `number`, or `None` for 0.
    #[must_use]
    pub const fn new(number: u64) -> Option<Self> {
        if number == 0 {
            None
        } else {
            Some(Self(number))
        }
    }

    /// The version as a number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for OverlayVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "v{}", self.0)
    }
}

/// Who or what wrote an overlay version.
///
/// The caller states it. A store checks its form, not its truth. A later
/// issue can add a kind of author, so match it with a wildcard arm.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "author", content = "id", rename_all = "snake_case")]
#[non_exhaustive]
pub enum Provenance {
    /// An operator, by operator id.
    Operator(String),
    /// A refine run, by its execution id.
    Refine(ExecutionId),
    /// A refine run that adopted a version with no person, by its execution
    /// id (issue #2394).
    Auto(ExecutionId),
}

/// `operator:<id>`, `refine:<run>` or `auto:<run>`.
impl std::fmt::Display for Provenance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.author_kind(), self.author())
    }
}

impl Provenance {
    /// An operator with the id `id`.
    #[must_use]
    pub fn operator(id: impl Into<String>) -> Self {
        Self::Operator(id.into())
    }

    /// The refine run `execution`.
    #[must_use]
    pub const fn refine(execution: ExecutionId) -> Self {
        Self::Refine(execution)
    }

    /// The refine run `execution`, adopting with no person.
    #[must_use]
    pub const fn auto(execution: ExecutionId) -> Self {
        Self::Auto(execution)
    }

    /// The stored author kind: `operator`, `refine` or `auto`.
    #[must_use]
    pub const fn author_kind(&self) -> &'static str {
        match self {
            Self::Operator(_) => AUTHOR_OPERATOR,
            Self::Refine(_) => AUTHOR_REFINE,
            Self::Auto(_) => AUTHOR_AUTO,
        }
    }

    /// The stored author: the operator id, or the execution id.
    #[must_use]
    pub fn author(&self) -> String {
        match self {
            Self::Operator(id) => id.clone(),
            Self::Refine(execution) | Self::Auto(execution) => execution.to_string(),
        }
    }

    /// Refuse an `auto` actor, then check the form. Only
    /// [`ProfileOverlayStore::auto_adopt`] records an `auto` version or
    /// move, so each one passes the daily cap.
    fn check_manual(&self) -> Result<(), OverlayStoreError> {
        match self {
            Self::Auto(_) => Err(OverlayStoreError::InvalidProvenance {
                reason: "only an auto adoption records an auto actor",
            }),
            _ => self.check(),
        }
    }

    /// Refuse an actor that is not a valid operator. Only an operator
    /// writes the refine policy, so the loop cannot widen its own gate.
    fn check_operator(&self) -> Result<(), OverlayStoreError> {
        match self {
            Self::Operator(_) => self.check(),
            _ => Err(OverlayStoreError::InvalidProvenance {
                reason: "only an operator sets the refine policy",
            }),
        }
    }

    /// Refuse an operator id that is empty or long, that starts or ends
    /// with white space, or that holds a control or invisible character. So
    /// two ids that look the same in an audit are the same id.
    fn check(&self) -> Result<(), OverlayStoreError> {
        let Self::Operator(id) = self else {
            return Ok(());
        };
        check_operator_id(id).map_err(|reason| OverlayStoreError::InvalidProvenance { reason })
    }

    /// The provenance of a stored row.
    fn decode(author_kind: &str, author: &str) -> Result<Self, OverlayStoreError> {
        match author_kind {
            AUTHOR_OPERATOR => Ok(Self::Operator(author.to_owned())),
            AUTHOR_REFINE => author
                .parse()
                .map(Self::Refine)
                .map_err(|_| corrupt("a refine author is not an execution id")),
            AUTHOR_AUTO => author
                .parse()
                .map(Self::Auto)
                .map_err(|_| corrupt("an auto author is not an execution id")),
            _ => Err(corrupt("the author kind is unknown")),
        }
    }
}

/// One stored overlay version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredOverlay {
    kind: String,
    version: OverlayVersion,
    bytes: Vec<u8>,
    overlay: ProfileOverlay,
    provenance: Provenance,
    created_at: DateTime<Utc>,
}

impl StoredOverlay {
    /// The agent kind.
    #[must_use]
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// The version.
    #[must_use]
    pub const fn version(&self) -> OverlayVersion {
        self.version
    }

    /// The bytes that the put stored: the JSON of the overlay.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The overlay, decoded from [`bytes`](Self::bytes).
    #[must_use]
    pub const fn overlay(&self) -> &ProfileOverlay {
        &self.overlay
    }

    /// Who or what wrote the version.
    #[must_use]
    pub const fn provenance(&self) -> &Provenance {
        &self.provenance
    }

    /// The time of the put.
    #[must_use]
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Decode a stored row of `kind`. An error is
    /// [`OverlayStoreError::Corrupt`]: a version 0, a body that does not
    /// decode, or an unknown author.
    pub(crate) fn from_row(kind: &str, row: OverlayRow) -> Result<Self, OverlayStoreError> {
        let version =
            OverlayVersion::new(row.version).ok_or_else(|| corrupt("a stored version is 0"))?;
        let overlay = serde_json::from_slice(&row.body)
            .map_err(|_| corrupt("a stored overlay does not decode"))?;
        Ok(Self {
            kind: kind.to_owned(),
            version,
            bytes: row.body,
            overlay,
            provenance: Provenance::decode(&row.author_kind, &row.author)?,
            created_at: row.created_at,
        })
    }
}

/// One move of the active pointer of a kind (issue #2374).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Activation {
    kind: String,
    seq: u64,
    from: Option<OverlayVersion>,
    to: OverlayVersion,
    actor: Provenance,
    activated_at: DateTime<Utc>,
}

impl Activation {
    /// The agent kind.
    #[must_use]
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// The number of the move in its kind, from 1.
    #[must_use]
    pub const fn seq(&self) -> u64 {
        self.seq
    }

    /// The active version before the move, or `None` for an unset pointer.
    #[must_use]
    pub const fn from(&self) -> Option<OverlayVersion> {
        self.from
    }

    /// The active version after the move.
    #[must_use]
    pub const fn to(&self) -> OverlayVersion {
        self.to
    }

    /// Who or what moved the pointer.
    #[must_use]
    pub const fn actor(&self) -> &Provenance {
        &self.actor
    }

    /// The time of the move.
    #[must_use]
    pub const fn activated_at(&self) -> DateTime<Utc> {
        self.activated_at
    }

    /// Decode a stored move of `kind`. An error is
    /// [`OverlayStoreError::Corrupt`].
    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    fn from_row(
        kind: &str,
        row: &autumn_harvest::overlay_log::ActivationRow,
    ) -> Result<Self, OverlayStoreError> {
        let version = |n| OverlayVersion::new(n).ok_or_else(|| corrupt("a logged version is 0"));
        Ok(Self {
            kind: kind.to_owned(),
            seq: row.seq,
            from: row.from.map(version).transpose()?,
            to: version(row.to)?,
            actor: Provenance::decode(&row.actor_kind, &row.actor)?,
            activated_at: row.activated_at,
        })
    }
}

/// The refine policy of a kind, as a store reads it (issue #2394).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyState {
    /// The policy. With no stored policy, every default.
    pub policy: RefinePolicy,
    /// The revision of the last change, or `None` when the kind has none.
    /// A write sends it back as its expected revision.
    pub revision: Option<u64>,
    /// Whether the last change stored a policy. A reset stores none.
    pub custom: bool,
}

/// One change of the refine policy of a kind: its old and new value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyLogEntry {
    kind: String,
    revision: u64,
    old: Option<RefinePolicy>,
    new: Option<RefinePolicy>,
    actor: Provenance,
    changed_at: DateTime<Utc>,
}

impl PolicyLogEntry {
    /// The agent kind.
    #[must_use]
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// The number of the change in its kind, from 1.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// The policy before the change. `None` means every default.
    #[must_use]
    pub const fn before(&self) -> Option<&RefinePolicy> {
        self.old.as_ref()
    }

    /// The policy after the change. `None` means every default: a reset.
    #[must_use]
    pub const fn after(&self) -> Option<&RefinePolicy> {
        self.new.as_ref()
    }

    /// The operator who made the change.
    #[must_use]
    pub const fn actor(&self) -> &Provenance {
        &self.actor
    }

    /// The time of the change.
    #[must_use]
    pub const fn changed_at(&self) -> DateTime<Utc> {
        self.changed_at
    }

    /// Decode a stored row of `kind`. An error is
    /// [`OverlayStoreError::Corrupt`].
    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    fn from_row(kind: &str, row: PolicyRow) -> Result<Self, OverlayStoreError> {
        let body = |bytes: Option<Vec<u8>>| {
            bytes
                .map(|bytes| serde_json::from_slice(&bytes))
                .transpose()
                .map_err(|_| corrupt("a stored refine policy does not decode"))
        };
        Ok(Self {
            kind: kind.to_owned(),
            revision: row.seq,
            old: body(row.old_body)?,
            new: body(row.new_body)?,
            actor: Provenance::decode(&row.actor_kind, &row.actor)?,
            changed_at: row.changed_at,
        })
    }

    /// The state of the policy after this change.
    fn state(&self) -> PolicyState {
        PolicyState {
            policy: self.new.clone().unwrap_or_default(),
            revision: Some(self.revision),
            custom: self.new.is_some(),
        }
    }
}

/// The state of a kind whose last change is `last`.
fn policy_state(last: Option<&PolicyLogEntry>) -> PolicyState {
    last.map_or_else(
        || PolicyState {
            policy: RefinePolicy::default(),
            revision: None,
            custom: false,
        },
        PolicyLogEntry::state,
    )
}

/// The outcome of [`ProfileOverlayStore::auto_adopt`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AutoAdoptOutcome {
    /// The new version is active.
    Adopted(OverlayVersion),
    /// The overlay changes no part of the expected version. Nothing
    /// changed.
    Unchanged,
    /// The overlay changes parts that the policy does not allow. Nothing
    /// changed.
    NeedsApproval {
        /// The changed parts that the policy does not allow.
        parts: BTreeSet<OverlayPart>,
    },
    /// The daily cap is full. Nothing changed.
    CapReached {
        /// The daily cap.
        cap: u32,
        /// The auto adoptions of the UTC day.
        used: u32,
    },
}

/// The result of a rollback: the active version before and after it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Rollback {
    /// The version that was active.
    pub from: OverlayVersion,
    /// The version that is active now.
    pub to: OverlayVersion,
}

/// Why a store refuses a call.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum OverlayStoreError {
    /// The kind has no such version. The pointer did not move.
    #[error("the kind has no overlay version {version}")]
    UnknownVersion {
        /// The version that the call named.
        version: OverlayVersion,
    },
    /// The active pointer is not the expected one. It did not move.
    #[error("the active overlay is {}, not the expected version", show_active(*.actual))]
    Conflict {
        /// The active version at the time of the refusal.
        actual: Option<OverlayVersion>,
    },
    /// [`overlay::apply`](super::overlay::apply) refuses the overlay.
    #[error(transparent)]
    Overlay(#[from] OverlayError),
    /// The validator has no profile of the kind.
    #[error("no agent profile has the kind {kind:?}")]
    UnknownKind {
        /// The kind, cut to [`MAX_NAME_LEN`] characters.
        kind: String,
    },
    /// The provenance is not valid.
    #[error("the provenance is not valid: {reason}")]
    InvalidProvenance {
        /// Why.
        reason: &'static str,
    },
    /// The last revision of the refine policy is not the expected one
    /// (issue #2394). Nothing changed.
    #[error("the refine policy is at revision {}, not the expected one", show_revision(*.actual))]
    PolicyConflict {
        /// The last revision at the time of the refusal.
        actual: Option<u64>,
    },
    /// The JSON of the overlay is above [`MAX_OVERLAY_BYTES`].
    #[error("the overlay has {bytes} bytes, above the limit of {limit}")]
    TooLarge {
        /// The size of the JSON.
        bytes: usize,
        /// The limit.
        limit: usize,
    },
    /// The version counter of the kind is full.
    #[error("the overlay version counter is full")]
    Full,
    /// A rollback found no version that was active before the current
    /// one. Nothing changed.
    #[error("the kind has no earlier activation to roll back to")]
    NoEarlierActivation,
    /// A stored row does not decode.
    #[error("a stored overlay row is corrupt: {reason}")]
    Corrupt {
        /// Why.
        reason: &'static str,
    },
    /// The backend failed.
    #[error("the overlay store failed: {message}")]
    Backend {
        /// The backend error.
        message: String,
    },
}

/// A policy revision for an error message: `3`, or `none`.
fn show_revision(revision: Option<u64>) -> String {
    revision.map_or_else(|| "none".to_owned(), |revision| revision.to_string())
}

/// A pointer for an error message: `v3`, or `unset`.
fn show_active(active: Option<OverlayVersion>) -> String {
    active.map_or_else(|| "unset".to_owned(), |version| version.to_string())
}

/// Refuse an operator id that is empty or long, that starts or ends with
/// white space, or that holds a control or invisible character. So two ids
/// that look the same in an audit are the same id.
pub(crate) fn check_operator_id(id: &str) -> Result<(), &'static str> {
    if id.trim().is_empty() {
        Err("the operator id is empty")
    } else if id.chars().count() > MAX_AUTHOR_LEN {
        Err("the operator id is too long")
    } else if id.trim() != id {
        Err("the operator id starts or ends with white space")
    } else if id.chars().any(|c| c.is_control() || is_invisible(c)) {
        Err("the operator id holds a control or invisible character")
    } else {
        Ok(())
    }
}

/// Whether `c` is a format character that shows no glyph. These are the
/// zero-width spaces and joiners, the bidirectional controls, the soft
/// hyphen and the byte order mark.
const fn is_invisible(c: char) -> bool {
    matches!(
        c,
        '\u{00AD}'
            | '\u{061C}'
            | '\u{180E}'
            | '\u{200B}'..='\u{200F}'
            | '\u{202A}'..='\u{202E}'
            | '\u{2060}'..='\u{2064}'
            | '\u{2066}'..='\u{206F}'
            | '\u{FEFF}'
    )
}

const fn corrupt(reason: &'static str) -> OverlayStoreError {
    OverlayStoreError::Corrupt { reason }
}

fn backend(error: impl std::fmt::Display) -> OverlayStoreError {
    OverlayStoreError::Backend {
        message: error.to_string(),
    }
}

/// Tells whether an overlay of a kind is valid.
///
/// A store calls it before each put and each activation, with the refine
/// policy of the kind (issue #2394).
pub trait OverlayValidator: Send + Sync {
    /// Accept or refuse `overlay` for `kind` under `policy`.
    ///
    /// # Errors
    ///
    /// Returns [`OverlayStoreError::UnknownKind`] for a kind with no
    /// profile, or [`OverlayStoreError::Overlay`] for an overlay that
    /// [`overlay::apply`](super::overlay::apply) refuses.
    fn validate(
        &self,
        kind: &str,
        overlay: &ProfileOverlay,
        policy: &RefinePolicy,
    ) -> Result<(), OverlayStoreError>;
}

/// The profile of the kind, under the policy ceilings of
/// [`PolicyCeilings::resolve`](crate::refine::policy::PolicyCeilings::resolve),
/// with the models of the harness. Under the default policy, an overlay
/// can only narrow its profile.
impl OverlayValidator for AgentHarness {
    fn validate(
        &self,
        kind: &str,
        overlay: &ProfileOverlay,
        policy: &RefinePolicy,
    ) -> Result<(), OverlayStoreError> {
        let profile = self
            .agent_profile(kind)
            .ok_or_else(|| OverlayStoreError::UnknownKind {
                kind: truncate(kind, MAX_NAME_LEN),
            })?;
        apply(&profile, overlay, &policy.ceilings.resolve(&profile), self)?;
        Ok(())
    }
}

/// Storage for versioned profile overlays.
///
/// An implementation validates each overlay with an [`OverlayValidator`]
/// before a put and before an activation. It never changes a stored
/// version, and it never gives a version number twice.
pub trait ProfileOverlayStore: Send + Sync + std::fmt::Debug {
    /// Validate `overlay` and append it as the next version of `kind`.
    ///
    /// # Errors
    ///
    /// The errors of [`OverlayValidator::validate`],
    /// [`OverlayStoreError::InvalidProvenance`],
    /// [`OverlayStoreError::TooLarge`], [`OverlayStoreError::Full`] and
    /// [`OverlayStoreError::Backend`].
    fn put<'a>(
        &'a self,
        kind: &'a str,
        overlay: &'a ProfileOverlay,
        provenance: Provenance,
    ) -> BoxFuture<'a, Result<OverlayVersion, OverlayStoreError>>;

    /// Make `version` the active version of `kind` when the active version
    /// is `expected_active`. `actor` names who moves the pointer.
    ///
    /// A move logs an [`Activation`] in the same transaction. An activation
    /// of the active version succeeds and logs nothing.
    ///
    /// # Errors
    ///
    /// [`OverlayStoreError::InvalidProvenance`],
    /// [`OverlayStoreError::UnknownVersion`] for a version that does not
    /// exist, the errors of [`OverlayValidator::validate`],
    /// [`OverlayStoreError::Conflict`] for another active version, and
    /// [`OverlayStoreError::Backend`]. The pointer moves only on success.
    fn activate<'a>(
        &'a self,
        kind: &'a str,
        version: OverlayVersion,
        expected_active: Option<OverlayVersion>,
        actor: Provenance,
    ) -> BoxFuture<'a, Result<(), OverlayStoreError>>;

    /// As [`activate`](Self::activate), when the last move of the pointer
    /// of `kind` is also the one that the caller read. `head` is its `seq`,
    /// or `None` for no move. A move after the read gives
    /// [`OverlayStoreError::Conflict`], also when the pointer came back.
    ///
    /// Each store of this crate checks `head` in the swap transaction. The
    /// default checks it before the swap, so a move in between can pass.
    ///
    /// # Errors
    ///
    /// The errors of [`activate`](Self::activate).
    fn activate_guarded<'a>(
        &'a self,
        kind: &'a str,
        version: OverlayVersion,
        expected_active: Option<OverlayVersion>,
        head: Option<u64>,
        actor: Provenance,
    ) -> BoxFuture<'a, Result<(), OverlayStoreError>> {
        Box::pin(async move {
            let last = self.last_activation(kind).await?;
            if last.as_ref().map(Activation::seq) != head {
                let actual = self.active(kind).await?.map(|stored| stored.version());
                return Err(OverlayStoreError::Conflict { actual });
            }
            self.activate(kind, version, expected_active, actor).await
        })
    }

    /// The last move of the pointer of `kind`, or `None`.
    ///
    /// # Errors
    ///
    /// [`OverlayStoreError::Corrupt`] and [`OverlayStoreError::Backend`].
    fn last_activation<'a>(
        &'a self,
        kind: &'a str,
    ) -> BoxFuture<'a, Result<Option<Activation>, OverlayStoreError>>;

    /// Activate the version that was active before the current one.
    ///
    /// It reads the last move `from -> to`, validates `from`, then activates
    /// `from` over the expected pointer `to`. The swap also needs that move
    /// to be the last one, so a pointer that moved after the read fails,
    /// also when it moved back. A second rollback undoes the first one.
    ///
    /// # Errors
    ///
    /// [`OverlayStoreError::NoEarlierActivation`] when no move exists, or
    /// the last move started from an unset pointer. Else the errors of
    /// [`activate`](Self::activate). A move after the read gives
    /// [`OverlayStoreError::Conflict`]. Nothing changes on an error.
    fn rollback<'a>(
        &'a self,
        kind: &'a str,
        actor: Provenance,
    ) -> BoxFuture<'a, Result<Rollback, OverlayStoreError>>;

    /// The active version of `kind`, or `None`.
    ///
    /// It does not validate the version again. Apply it with
    /// [`overlay::apply`](super::overlay::apply) before use.
    ///
    /// # Errors
    ///
    /// [`OverlayStoreError::Corrupt`] and [`OverlayStoreError::Backend`].
    fn active<'a>(
        &'a self,
        kind: &'a str,
    ) -> BoxFuture<'a, Result<Option<StoredOverlay>, OverlayStoreError>>;

    /// The version `version` of `kind`, or `None`.
    ///
    /// # Errors
    ///
    /// [`OverlayStoreError::Corrupt`] and [`OverlayStoreError::Backend`].
    fn get<'a>(
        &'a self,
        kind: &'a str,
        version: OverlayVersion,
    ) -> BoxFuture<'a, Result<Option<StoredOverlay>, OverlayStoreError>>;

    /// Every version of `kind`, oldest first.
    ///
    /// # Errors
    ///
    /// [`OverlayStoreError::Corrupt`] and [`OverlayStoreError::Backend`].
    fn list<'a>(
        &'a self,
        kind: &'a str,
    ) -> BoxFuture<'a, Result<Vec<StoredOverlay>, OverlayStoreError>>;

    /// The refine policy of `kind` (issue #2394). With no stored policy,
    /// every default.
    ///
    /// # Errors
    ///
    /// [`OverlayStoreError::Corrupt`] and [`OverlayStoreError::Backend`].
    fn policy<'a>(&'a self, kind: &'a str)
    -> BoxFuture<'a, Result<PolicyState, OverlayStoreError>>;

    /// Store `policy` for `kind` when the last revision is
    /// `expected_revision`. Only an operator may call it. The log keeps the
    /// old and the new policy.
    ///
    /// # Errors
    ///
    /// [`OverlayStoreError::InvalidProvenance`] for an actor that is not a
    /// valid operator, [`OverlayStoreError::PolicyConflict`] for another
    /// revision, and [`OverlayStoreError::Backend`]. Nothing changes on an
    /// error.
    fn set_policy<'a>(
        &'a self,
        kind: &'a str,
        policy: &'a RefinePolicy,
        expected_revision: Option<u64>,
        actor: Provenance,
    ) -> BoxFuture<'a, Result<PolicyLogEntry, OverlayStoreError>>;

    /// Restore every default for `kind`. Only an operator may call it. The
    /// log keeps the old policy. A reset of the defaults also logs a row.
    ///
    /// # Errors
    ///
    /// [`OverlayStoreError::InvalidProvenance`] and
    /// [`OverlayStoreError::Backend`].
    fn reset_policy<'a>(
        &'a self,
        kind: &'a str,
        actor: Provenance,
    ) -> BoxFuture<'a, Result<PolicyLogEntry, OverlayStoreError>>;

    /// Every change of the refine policy of `kind`, oldest first.
    ///
    /// # Errors
    ///
    /// [`OverlayStoreError::Corrupt`] and [`OverlayStoreError::Backend`].
    fn policy_log<'a>(
        &'a self,
        kind: &'a str,
    ) -> BoxFuture<'a, Result<Vec<PolicyLogEntry>, OverlayStoreError>>;

    /// Put `overlay` as `auto:<run>` and make it active over
    /// `expected_active`, in one transaction, when the policy allows it
    /// (issue #2394).
    ///
    /// The store enforces the gate itself. `run_policy` is the policy that
    /// the run read once. The store also reads its current policy. A part
    /// goes live only when both policies allow it, and the stricter daily
    /// cap counts. So a change during the run never widens the run, and a
    /// narrowing reaches it at once. The store compares the overlay with
    /// the expected version, and the compare-and-swap keeps that version
    /// active to the end.
    ///
    /// Under the lock of the head row, the store counts the auto moves of
    /// the UTC day of its [`StoreClock`]. A full cap returns
    /// [`AutoAdoptOutcome::CapReached`]. So two runs at once never pass the
    /// cap. An unchanged or a refused overlay changes nothing.
    ///
    /// # Errors
    ///
    /// The errors of [`put`](Self::put),
    /// [`OverlayStoreError::UnknownVersion`] for an expected version that
    /// does not exist, and [`OverlayStoreError::Conflict`] for another
    /// active version. Nothing changes on an error.
    fn auto_adopt<'a>(
        &'a self,
        kind: &'a str,
        overlay: &'a ProfileOverlay,
        run: ExecutionId,
        expected_active: Option<OverlayVersion>,
        run_policy: &'a RefinePolicy,
    ) -> BoxFuture<'a, Result<AutoAdoptOutcome, OverlayStoreError>>;
}

/// The raw policy body of `policy`. `None` is a reset.
#[cfg(any(feature = "sqlite", feature = "postgres"))]
fn policy_body(policy: Option<&RefinePolicy>) -> Result<Option<Vec<u8>>, OverlayStoreError> {
    policy
        .map(|policy| serde_json::to_vec(policy).map_err(backend))
        .transpose()
}

/// The result of a raw policy write of `kind`.
#[cfg(any(feature = "sqlite", feature = "postgres"))]
fn policy_appended(kind: &str, outcome: PolicyAppend) -> Result<PolicyLogEntry, OverlayStoreError> {
    match outcome {
        PolicyAppend::Appended(row) => PolicyLogEntry::from_row(kind, row),
        PolicyAppend::Conflict { actual } => Err(OverlayStoreError::PolicyConflict { actual }),
    }
}

/// The result of a raw auto adoption under `cap`.
fn auto_adopted(cap: DailyCap, raw: AutoAdoptRaw) -> Result<AutoAdoptOutcome, OverlayStoreError> {
    match raw {
        AutoAdoptRaw::Adopted(row) => OverlayVersion::new(row.version)
            .map(AutoAdoptOutcome::Adopted)
            .ok_or_else(|| corrupt("a stored version is 0")),
        AutoAdoptRaw::CapReached { used } => Ok(AutoAdoptOutcome::CapReached {
            cap: cap.limit().unwrap_or(u32::MAX),
            used: u32::try_from(used).unwrap_or(u32::MAX),
        }),
        AutoAdoptRaw::Conflict { actual } => Err(OverlayStoreError::Conflict {
            actual: actual
                .map(|n| OverlayVersion::new(n).ok_or_else(|| corrupt("a stored pointer is 0")))
                .transpose()?,
        }),
        AutoAdoptRaw::Full => Err(OverlayStoreError::Full),
        AutoAdoptRaw::PolicyMoved { actual } => Err(OverlayStoreError::PolicyConflict { actual }),
    }
}

/// Check the provenance and the overlay under `policy`, then encode the
/// overlay. Every store calls it first in `put`. An `auto` provenance is
/// refused, because only `auto_adopt` writes one.
fn encode_checked(
    validator: &dyn OverlayValidator,
    kind: &str,
    overlay: &ProfileOverlay,
    provenance: &Provenance,
    policy: &RefinePolicy,
) -> Result<Vec<u8>, OverlayStoreError> {
    provenance.check_manual()?;
    encode_valid(validator, kind, overlay, policy)
}

/// What [`auto_gate`] decides before the transaction of an auto adoption.
enum AutoGate {
    /// No move. The outcome is final.
    Done(AutoAdoptOutcome),
    /// Move under `cap`, with the encoded overlay.
    Go {
        /// The JSON of the overlay.
        bytes: Vec<u8>,
        /// The stricter daily cap of the two policies.
        cap: DailyCap,
        /// The policy revision that the gate read. The transaction checks
        /// it.
        revision: Option<u64>,
    },
}

/// The most times an auto adoption runs its gate. Each new run means that
/// an operator changed the policy during the last one.
const AUTO_GATE_TRIES: usize = 4;

/// Run the gate of an auto adoption, then `commit` its move.
///
/// `commit` gets the encoded overlay, the cap and the policy revision that
/// the gate read. A policy change after the gate refuses the move with
/// [`AutoAdoptRaw::PolicyMoved`]. The gate then runs again on the new
/// policy. So a policy change that commits first always binds the move.
async fn auto_adopt_gated<S, F, Fut>(
    store: &S,
    validator: &dyn OverlayValidator,
    kind: &str,
    overlay: &ProfileOverlay,
    expected: Option<OverlayVersion>,
    run_policy: &RefinePolicy,
    mut commit: F,
) -> Result<AutoAdoptOutcome, OverlayStoreError>
where
    S: ProfileOverlayStore + ?Sized,
    F: FnMut(Vec<u8>, DailyCap, Option<u64>) -> Fut,
    Fut: Future<Output = Result<AutoAdoptRaw, OverlayStoreError>>,
{
    let mut actual = None;
    for _ in 0..AUTO_GATE_TRIES {
        let (bytes, cap, revision) =
            match auto_gate(store, validator, kind, overlay, expected, run_policy).await? {
                AutoGate::Done(outcome) => return Ok(outcome),
                AutoGate::Go {
                    bytes,
                    cap,
                    revision,
                } => (bytes, cap, revision),
            };
        match commit(bytes, cap, revision).await? {
            AutoAdoptRaw::PolicyMoved { actual: moved } => actual = moved,
            raw => return auto_adopted(cap, raw),
        }
    }
    Err(OverlayStoreError::PolicyConflict { actual })
}

/// The gate of an auto adoption through `store`.
///
/// It reads the current policy of the kind. A part is allowed only when
/// `run_policy` and the current policy both allow it. It compares
/// `overlay` with the expected version. The transaction then checks that
/// this version is still active. An adoption validates under the current
/// policy.
async fn auto_gate<S: ProfileOverlayStore + ?Sized>(
    store: &S,
    validator: &dyn OverlayValidator,
    kind: &str,
    overlay: &ProfileOverlay,
    expected: Option<OverlayVersion>,
    run_policy: &RefinePolicy,
) -> Result<AutoGate, OverlayStoreError> {
    let state = store.policy(kind).await?;
    let (current, revision) = (state.policy, state.revision);
    let base = match expected {
        Some(version) => store
            .get(kind, version)
            .await?
            .ok_or(OverlayStoreError::UnknownVersion { version })?
            .overlay()
            .clone(),
        None => ProfileOverlay::default(),
    };
    let allowed: BTreeSet<OverlayPart> = run_policy
        .auto_adopt
        .parts
        .intersection(&current.auto_adopt.parts)
        .copied()
        .collect();
    let changed = changed_parts(&base, overlay);
    Ok(
        match decide_adoption(mask(&allowed), mask(&changed), 0, None) {
            AdoptDecision::Unchanged => AutoGate::Done(AutoAdoptOutcome::Unchanged),
            AdoptDecision::NeedsApproval => AutoGate::Done(AutoAdoptOutcome::NeedsApproval {
                parts: changed.difference(&allowed).copied().collect(),
            }),
            AdoptDecision::Adopt | AdoptDecision::CapReached => AutoGate::Go {
                bytes: encode_valid(validator, kind, overlay, &current)?,
                cap: run_policy
                    .auto_adopt
                    .max_per_day
                    .stricter(current.auto_adopt.max_per_day),
                revision,
            },
        },
    )
}

/// Validate `overlay` under `policy`, then encode it.
fn encode_valid(
    validator: &dyn OverlayValidator,
    kind: &str,
    overlay: &ProfileOverlay,
    policy: &RefinePolicy,
) -> Result<Vec<u8>, OverlayStoreError> {
    validator.validate(kind, overlay, policy)?;
    let bytes = serde_json::to_vec(overlay).map_err(backend)?;
    if bytes.len() > MAX_OVERLAY_BYTES {
        return Err(OverlayStoreError::TooLarge {
            bytes: bytes.len(),
            limit: MAX_OVERLAY_BYTES,
        });
    }
    Ok(bytes)
}

/// The version of a stored row, or [`OverlayStoreError::Full`] when the
/// counter of the kind is full.
#[cfg(any(feature = "sqlite", feature = "postgres"))]
fn appended(row: Option<OverlayRow>) -> Result<OverlayVersion, OverlayStoreError> {
    let row = row.ok_or(OverlayStoreError::Full)?;
    OverlayVersion::new(row.version).ok_or_else(|| corrupt("a stored version is 0"))
}

/// The result of an activation outcome of `version`.
fn activated(version: OverlayVersion, outcome: ActivateOutcome) -> Result<(), OverlayStoreError> {
    match outcome {
        ActivateOutcome::Activated => Ok(()),
        ActivateOutcome::UnknownVersion => Err(OverlayStoreError::UnknownVersion { version }),
        ActivateOutcome::Conflict { actual: None } => {
            Err(OverlayStoreError::Conflict { actual: None })
        }
        ActivateOutcome::Conflict {
            actual: Some(actual),
        } => Err(OverlayStoreError::Conflict {
            actual: Some(
                OverlayVersion::new(actual).ok_or_else(|| corrupt("a stored pointer is 0"))?,
            ),
        }),
    }
}

/// Activate through `store`: check the actor, read the version, validate
/// it, then call `swap`. A version row never changes, so the check holds at
/// the swap.
async fn activate_checked<S, F, Fut>(
    store: &S,
    validator: &dyn OverlayValidator,
    kind: &str,
    version: OverlayVersion,
    actor: &Provenance,
    swap: F,
) -> Result<(), OverlayStoreError>
where
    S: ProfileOverlayStore + ?Sized,
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<ActivateOutcome, OverlayStoreError>>,
{
    actor.check_manual()?;
    let stored = store
        .get(kind, version)
        .await?
        .ok_or(OverlayStoreError::UnknownVersion { version })?;
    let policy = store.policy(kind).await?.policy;
    validator.validate(kind, stored.overlay(), &policy)?;
    activated(version, swap().await?)
}

/// The raw move request of `version` over `expected` by `author`.
const fn move_request<'b>(
    version: OverlayVersion,
    expected: Option<OverlayVersion>,
    actor: &Provenance,
    author: &'b str,
    after_seq: Option<u64>,
    head: Option<Option<u64>>,
) -> MoveRequest<'b> {
    MoveRequest {
        version: version.get(),
        expected: match expected {
            Some(expected) => Some(expected.get()),
            None => None,
        },
        actor_kind: actor.author_kind(),
        actor: author,
        after_seq,
        head,
    }
}

/// Roll back through `store`: check the actor, read the last move, then
/// validate its start. `swap` then gets the target, the expected pointer
/// and the `seq` of the move. The swap needs that move to be the last one.
async fn rollback_checked<S, F, Fut>(
    store: &S,
    validator: &dyn OverlayValidator,
    kind: &str,
    actor: &Provenance,
    swap: F,
) -> Result<Rollback, OverlayStoreError>
where
    S: ProfileOverlayStore + ?Sized,
    F: FnOnce(OverlayVersion, OverlayVersion, u64) -> Fut,
    Fut: Future<Output = Result<ActivateOutcome, OverlayStoreError>>,
{
    actor.check_manual()?;
    let last = store
        .last_activation(kind)
        .await?
        .ok_or(OverlayStoreError::NoEarlierActivation)?;
    let plan = rollback_target(last.from.map(OverlayVersion::get), last.to.get())
        .ok_or(OverlayStoreError::NoEarlierActivation)?;
    let to = OverlayVersion::new(plan.target).ok_or_else(|| corrupt("a logged version is 0"))?;
    let stored = store
        .get(kind, to)
        .await?
        .ok_or(OverlayStoreError::UnknownVersion { version: to })?;
    let policy = store.policy(kind).await?.policy;
    validator.validate(kind, stored.overlay(), &policy)?;
    activated(to, swap(to, last.to, last.seq).await?)?;
    Ok(Rollback { from: last.to, to })
}

/// A [`ProfileOverlayStore`] in process memory. Its contents go when the
/// process stops, so use it for tests and demos only.
pub struct InMemoryOverlayStore {
    validator: Arc<dyn OverlayValidator>,
    clock: StoreClock,
    kinds: Mutex<HashMap<String, KindLog>>,
}

/// The log of one kind in memory.
#[derive(Default)]
struct KindLog {
    versions: Vec<StoredOverlay>,
    active: Option<OverlayVersion>,
    moves: Vec<Activation>,
    policies: Vec<PolicyLogEntry>,
}

impl std::fmt::Debug for InMemoryOverlayStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kinds = self.kinds.lock().map_or(0, |kinds| kinds.len());
        f.debug_struct("InMemoryOverlayStore")
            .field("kinds", &kinds)
            .finish_non_exhaustive()
    }
}

impl InMemoryOverlayStore {
    /// An empty store that validates with `validator`.
    #[must_use]
    pub fn new(validator: Arc<dyn OverlayValidator>) -> Self {
        Self {
            validator,
            kinds: Mutex::new(HashMap::new()),
            clock: system_clock(),
        }
    }

    /// The store with `clock` in place of the system clock.
    #[must_use]
    pub fn with_clock(mut self, clock: StoreClock) -> Self {
        self.clock = clock;
        self
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, KindLog>> {
        self.kinds.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl ProfileOverlayStore for InMemoryOverlayStore {
    fn put<'a>(
        &'a self,
        kind: &'a str,
        overlay: &'a ProfileOverlay,
        provenance: Provenance,
    ) -> BoxFuture<'a, Result<OverlayVersion, OverlayStoreError>> {
        Box::pin(async move {
            let policy = self.policy(kind).await?.policy;
            self.put_now(kind, overlay, &provenance, &policy)
        })
    }

    fn activate<'a>(
        &'a self,
        kind: &'a str,
        version: OverlayVersion,
        expected_active: Option<OverlayVersion>,
        actor: Provenance,
    ) -> BoxFuture<'a, Result<(), OverlayStoreError>> {
        Box::pin(async move {
            activate_checked(self, &*self.validator, kind, version, &actor, || async {
                let author = actor.author();
                let request = move_request(version, expected_active, &actor, &author, None, None);
                Ok(self.swap_now(kind, &request, &actor))
            })
            .await
        })
    }

    fn activate_guarded<'a>(
        &'a self,
        kind: &'a str,
        version: OverlayVersion,
        expected_active: Option<OverlayVersion>,
        head: Option<u64>,
        actor: Provenance,
    ) -> BoxFuture<'a, Result<(), OverlayStoreError>> {
        Box::pin(async move {
            activate_checked(self, &*self.validator, kind, version, &actor, || async {
                let author = actor.author();
                let request =
                    move_request(version, expected_active, &actor, &author, None, Some(head));
                Ok(self.swap_now(kind, &request, &actor))
            })
            .await
        })
    }

    fn rollback<'a>(
        &'a self,
        kind: &'a str,
        actor: Provenance,
    ) -> BoxFuture<'a, Result<Rollback, OverlayStoreError>> {
        Box::pin(async move {
            let actor = &actor;
            rollback_checked(
                self,
                &*self.validator,
                kind,
                actor,
                |to, from, seq| async move {
                    let author = actor.author();
                    let request = move_request(to, Some(from), actor, &author, Some(seq), None);
                    Ok(self.swap_now(kind, &request, actor))
                },
            )
            .await
        })
    }

    fn last_activation<'a>(
        &'a self,
        kind: &'a str,
    ) -> BoxFuture<'a, Result<Option<Activation>, OverlayStoreError>> {
        Box::pin(async move {
            let kinds = self.lock();
            let last = kinds.get(kind).and_then(|log| log.moves.last().cloned());
            drop(kinds);
            Ok(last)
        })
    }

    fn active<'a>(
        &'a self,
        kind: &'a str,
    ) -> BoxFuture<'a, Result<Option<StoredOverlay>, OverlayStoreError>> {
        Box::pin(async move {
            let kinds = self.lock();
            let active = kinds.get(kind).and_then(|log| {
                let version = log.active?;
                log.versions
                    .iter()
                    .find(|stored| stored.version == version)
                    .cloned()
            });
            drop(kinds);
            Ok(active)
        })
    }

    fn get<'a>(
        &'a self,
        kind: &'a str,
        version: OverlayVersion,
    ) -> BoxFuture<'a, Result<Option<StoredOverlay>, OverlayStoreError>> {
        Box::pin(async move {
            let kinds = self.lock();
            let stored = kinds.get(kind).and_then(|log| {
                log.versions
                    .iter()
                    .find(|stored| stored.version == version)
                    .cloned()
            });
            drop(kinds);
            Ok(stored)
        })
    }

    fn list<'a>(
        &'a self,
        kind: &'a str,
    ) -> BoxFuture<'a, Result<Vec<StoredOverlay>, OverlayStoreError>> {
        Box::pin(async move {
            let kinds = self.lock();
            let versions = kinds
                .get(kind)
                .map(|log| log.versions.clone())
                .unwrap_or_default();
            drop(kinds);
            Ok(versions)
        })
    }

    fn policy<'a>(
        &'a self,
        kind: &'a str,
    ) -> BoxFuture<'a, Result<PolicyState, OverlayStoreError>> {
        Box::pin(async move {
            let kinds = self.lock();
            let state = policy_state(kinds.get(kind).and_then(|log| log.policies.last()));
            drop(kinds);
            Ok(state)
        })
    }

    fn set_policy<'a>(
        &'a self,
        kind: &'a str,
        policy: &'a RefinePolicy,
        expected_revision: Option<u64>,
        actor: Provenance,
    ) -> BoxFuture<'a, Result<PolicyLogEntry, OverlayStoreError>> {
        Box::pin(async move {
            self.write_policy(
                kind,
                Some(policy),
                PolicyExpect::Revision(expected_revision),
                actor,
            )
        })
    }

    fn reset_policy<'a>(
        &'a self,
        kind: &'a str,
        actor: Provenance,
    ) -> BoxFuture<'a, Result<PolicyLogEntry, OverlayStoreError>> {
        Box::pin(async move { self.write_policy(kind, None, PolicyExpect::Any, actor) })
    }

    fn policy_log<'a>(
        &'a self,
        kind: &'a str,
    ) -> BoxFuture<'a, Result<Vec<PolicyLogEntry>, OverlayStoreError>> {
        Box::pin(async move {
            let kinds = self.lock();
            let log = kinds
                .get(kind)
                .map(|log| log.policies.clone())
                .unwrap_or_default();
            drop(kinds);
            Ok(log)
        })
    }

    fn auto_adopt<'a>(
        &'a self,
        kind: &'a str,
        overlay: &'a ProfileOverlay,
        run: ExecutionId,
        expected_active: Option<OverlayVersion>,
        run_policy: &'a RefinePolicy,
    ) -> BoxFuture<'a, Result<AutoAdoptOutcome, OverlayStoreError>> {
        Box::pin(async move {
            let provenance = Provenance::auto(run);
            auto_adopt_gated(
                self,
                &*self.validator,
                kind,
                overlay,
                expected_active,
                run_policy,
                |bytes, cap, revision| {
                    let raw = self.auto_adopt_now(
                        kind,
                        bytes,
                        &provenance,
                        expected_active,
                        cap,
                        revision,
                    );
                    std::future::ready(raw)
                },
            )
            .await
        })
    }
}

impl InMemoryOverlayStore {
    /// Validate and append one version under the lock.
    ///
    /// It decodes the stored bytes, as the database stores do on a read.
    fn put_now(
        &self,
        kind: &str,
        overlay: &ProfileOverlay,
        provenance: &Provenance,
        policy: &RefinePolicy,
    ) -> Result<OverlayVersion, OverlayStoreError> {
        let bytes = encode_checked(&*self.validator, kind, overlay, provenance, policy)?;
        let mut kinds = self.lock();
        let log = kinds.entry(kind.to_owned()).or_default();
        let latest = log.versions.last().map(|stored| stored.version.get());
        let version = next_overlay_version(latest).ok_or(OverlayStoreError::Full)?;
        let row = OverlayRow {
            version,
            body: bytes,
            author_kind: provenance.author_kind().to_owned(),
            author: provenance.author(),
            created_at: Utc::now(),
        };
        let stored = StoredOverlay::from_row(kind, row)?;
        let version = stored.version;
        log.versions.push(stored);
        drop(kinds);
        Ok(version)
    }

    /// Append one policy change under the lock. `policy` is `None` for a
    /// reset.
    fn write_policy(
        &self,
        kind: &str,
        policy: Option<&RefinePolicy>,
        expected: PolicyExpect,
        actor: Provenance,
    ) -> Result<PolicyLogEntry, OverlayStoreError> {
        actor.check_operator()?;
        let mut kinds = self.lock();
        let log = kinds.entry(kind.to_owned()).or_default();
        let last = log.policies.last();
        let actual = last.map(PolicyLogEntry::revision);
        if !policy_write_passes(expected, actual) {
            return Err(OverlayStoreError::PolicyConflict { actual });
        }
        let entry = PolicyLogEntry {
            kind: kind.to_owned(),
            revision: actual.map_or(1, |revision| revision + 1),
            old: last.and_then(|last| last.new.clone()),
            new: policy.cloned(),
            actor,
            changed_at: Utc::now(),
        };
        log.policies.push(entry.clone());
        drop(kinds);
        Ok(entry)
    }

    /// Put and activate one version under the lock, as the database stores
    /// do in one transaction.
    fn auto_adopt_now(
        &self,
        kind: &str,
        bytes: Vec<u8>,
        provenance: &Provenance,
        expected: Option<OverlayVersion>,
        cap: DailyCap,
        policy_revision: Option<u64>,
    ) -> Result<AutoAdoptRaw, OverlayStoreError> {
        let at = (self.clock)();
        let mut kinds = self.lock();
        let log = kinds.entry(kind.to_owned()).or_default();
        let revision = log.policies.last().map(PolicyLogEntry::revision);
        if revision != policy_revision {
            return Ok(AutoAdoptRaw::PolicyMoved { actual: revision });
        }
        let (start, end) = utc_day(at);
        let used = log
            .moves
            .iter()
            .filter(|done| {
                matches!(done.actor, Provenance::Auto(_))
                    && (start..end).contains(&done.activated_at)
            })
            .count();
        let used = u64::try_from(used).unwrap_or(u64::MAX);
        let actual = log.active.map(OverlayVersion::get);
        match decide_auto_move(expected.map(OverlayVersion::get), actual, used, cap.limit()) {
            AutoMoveDecision::Conflict => return Ok(AutoAdoptRaw::Conflict { actual }),
            AutoMoveDecision::CapReached => return Ok(AutoAdoptRaw::CapReached { used }),
            AutoMoveDecision::Move => {}
        }
        let latest = log.versions.last().map(|stored| stored.version.get());
        let Some(version) = next_overlay_version(latest) else {
            return Ok(AutoAdoptRaw::Full);
        };
        let row = OverlayRow {
            version,
            body: bytes,
            author_kind: provenance.author_kind().to_owned(),
            author: provenance.author(),
            created_at: Utc::now(),
        };
        let stored = StoredOverlay::from_row(kind, row.clone())?;
        let seq = log.moves.last().map_or(1, |last| last.seq + 1);
        log.moves.push(Activation {
            kind: kind.to_owned(),
            seq,
            from: log.active,
            to: stored.version,
            actor: provenance.clone(),
            activated_at: at,
        });
        log.active = Some(stored.version);
        log.versions.push(stored);
        drop(kinds);
        Ok(AutoAdoptRaw::Adopted(row))
    }

    /// Swap the pointer and log the move under the lock.
    fn swap_now(
        &self,
        kind: &str,
        request: &MoveRequest<'_>,
        actor: &Provenance,
    ) -> ActivateOutcome {
        let Some(version) = OverlayVersion::new(request.version) else {
            return ActivateOutcome::UnknownVersion;
        };
        let mut kinds = self.lock();
        let outcome = kinds
            .get_mut(kind)
            .map_or(ActivateOutcome::UnknownVersion, |log| {
                let known = log.versions.iter().any(|stored| stored.version == version);
                let actual = log.active.map(OverlayVersion::get);
                let last_seq = log.moves.last().map(|last| last.seq);
                match request.decide(known, actual, last_seq) {
                    ActivateDecision::UnknownVersion => ActivateOutcome::UnknownVersion,
                    ActivateDecision::Conflict => ActivateOutcome::Conflict { actual },
                    ActivateDecision::Activate => {
                        if logs_activation(actual, version.get()) {
                            let seq = log.moves.last().map_or(1, |last| last.seq + 1);
                            log.moves.push(Activation {
                                kind: kind.to_owned(),
                                seq,
                                from: log.active,
                                to: version,
                                actor: actor.clone(),
                                activated_at: Utc::now(),
                            });
                            log.active = Some(version);
                        }
                        ActivateOutcome::Activated
                    }
                }
            });
        drop(kinds);
        outcome
    }
}

/// A [`ProfileOverlayStore`] in a `SQLite` file.
///
/// It opens its own connection. It can share the file of a
/// `SqliteRuntime`. A call runs its statements on the polling thread, and a
/// busy file can hold that thread for up to 5 seconds.
#[cfg(feature = "sqlite")]
pub struct SqliteOverlayStore {
    log: autumn_harvest_sqlite::SqliteOverlayLog,
    validator: Arc<dyn OverlayValidator>,
    clock: StoreClock,
}

#[cfg(feature = "sqlite")]
impl std::fmt::Debug for SqliteOverlayStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteOverlayStore").finish_non_exhaustive()
    }
}

#[cfg(feature = "sqlite")]
impl SqliteOverlayStore {
    /// Open the store in the database at `path`.
    ///
    /// # Errors
    ///
    /// Returns [`OverlayStoreError::Backend`] when the file does not open.
    pub fn open(
        path: impl AsRef<std::path::Path>,
        validator: Arc<dyn OverlayValidator>,
    ) -> Result<Self, OverlayStoreError> {
        let log = autumn_harvest_sqlite::SqliteOverlayLog::open(path).map_err(backend)?;
        Ok(Self {
            log,
            validator,
            clock: system_clock(),
        })
    }

    /// Open a store in a new, private in-memory database.
    ///
    /// # Errors
    ///
    /// Returns [`OverlayStoreError::Backend`] when the database does not
    /// open.
    pub fn open_in_memory(validator: Arc<dyn OverlayValidator>) -> Result<Self, OverlayStoreError> {
        let log = autumn_harvest_sqlite::SqliteOverlayLog::open_in_memory().map_err(backend)?;
        Ok(Self {
            log,
            validator,
            clock: system_clock(),
        })
    }

    /// The store with `clock` in place of the system clock.
    #[must_use]
    pub fn with_clock(mut self, clock: StoreClock) -> Self {
        self.clock = clock;
        self
    }
}

#[cfg(feature = "sqlite")]
impl ProfileOverlayStore for SqliteOverlayStore {
    fn put<'a>(
        &'a self,
        kind: &'a str,
        overlay: &'a ProfileOverlay,
        provenance: Provenance,
    ) -> BoxFuture<'a, Result<OverlayVersion, OverlayStoreError>> {
        Box::pin(async move {
            let policy = self.policy(kind).await?.policy;
            let bytes = encode_checked(&*self.validator, kind, overlay, &provenance, &policy)?;
            let row = self
                .log
                .append(kind, &bytes, provenance.author_kind(), &provenance.author())
                .map_err(backend)?;
            appended(row)
        })
    }

    fn activate<'a>(
        &'a self,
        kind: &'a str,
        version: OverlayVersion,
        expected_active: Option<OverlayVersion>,
        actor: Provenance,
    ) -> BoxFuture<'a, Result<(), OverlayStoreError>> {
        Box::pin(async move {
            activate_checked(self, &*self.validator, kind, version, &actor, || async {
                let author = actor.author();
                let request = move_request(version, expected_active, &actor, &author, None, None);
                self.log.activate(kind, &request).map_err(backend)
            })
            .await
        })
    }

    fn activate_guarded<'a>(
        &'a self,
        kind: &'a str,
        version: OverlayVersion,
        expected_active: Option<OverlayVersion>,
        head: Option<u64>,
        actor: Provenance,
    ) -> BoxFuture<'a, Result<(), OverlayStoreError>> {
        Box::pin(async move {
            activate_checked(self, &*self.validator, kind, version, &actor, || async {
                let author = actor.author();
                let request =
                    move_request(version, expected_active, &actor, &author, None, Some(head));
                self.log.activate(kind, &request).map_err(backend)
            })
            .await
        })
    }

    fn rollback<'a>(
        &'a self,
        kind: &'a str,
        actor: Provenance,
    ) -> BoxFuture<'a, Result<Rollback, OverlayStoreError>> {
        Box::pin(async move {
            let actor = &actor;
            rollback_checked(
                self,
                &*self.validator,
                kind,
                actor,
                |to, from, seq| async move {
                    let author = actor.author();
                    let request = move_request(to, Some(from), actor, &author, Some(seq), None);
                    self.log.activate(kind, &request).map_err(backend)
                },
            )
            .await
        })
    }

    fn last_activation<'a>(
        &'a self,
        kind: &'a str,
    ) -> BoxFuture<'a, Result<Option<Activation>, OverlayStoreError>> {
        Box::pin(async move {
            self.log
                .last_activation(kind)
                .map_err(backend)?
                .map(|row| Activation::from_row(kind, &row))
                .transpose()
        })
    }

    fn active<'a>(
        &'a self,
        kind: &'a str,
    ) -> BoxFuture<'a, Result<Option<StoredOverlay>, OverlayStoreError>> {
        Box::pin(async move { decode_one(kind, self.log.active(kind).map_err(backend)?) })
    }

    fn get<'a>(
        &'a self,
        kind: &'a str,
        version: OverlayVersion,
    ) -> BoxFuture<'a, Result<Option<StoredOverlay>, OverlayStoreError>> {
        Box::pin(
            async move { decode_one(kind, self.log.get(kind, version.get()).map_err(backend)?) },
        )
    }

    fn list<'a>(
        &'a self,
        kind: &'a str,
    ) -> BoxFuture<'a, Result<Vec<StoredOverlay>, OverlayStoreError>> {
        Box::pin(async move { decode_all(kind, self.log.list(kind).map_err(backend)?) })
    }

    fn policy<'a>(
        &'a self,
        kind: &'a str,
    ) -> BoxFuture<'a, Result<PolicyState, OverlayStoreError>> {
        Box::pin(async move {
            let last = self
                .log
                .last_policy(kind)
                .map_err(backend)?
                .map(|row| PolicyLogEntry::from_row(kind, row))
                .transpose()?;
            Ok(policy_state(last.as_ref()))
        })
    }

    fn set_policy<'a>(
        &'a self,
        kind: &'a str,
        policy: &'a RefinePolicy,
        expected_revision: Option<u64>,
        actor: Provenance,
    ) -> BoxFuture<'a, Result<PolicyLogEntry, OverlayStoreError>> {
        Box::pin(async move {
            self.write_policy(
                kind,
                Some(policy),
                PolicyExpect::Revision(expected_revision),
                &actor,
            )
        })
    }

    fn reset_policy<'a>(
        &'a self,
        kind: &'a str,
        actor: Provenance,
    ) -> BoxFuture<'a, Result<PolicyLogEntry, OverlayStoreError>> {
        Box::pin(async move { self.write_policy(kind, None, PolicyExpect::Any, &actor) })
    }

    fn policy_log<'a>(
        &'a self,
        kind: &'a str,
    ) -> BoxFuture<'a, Result<Vec<PolicyLogEntry>, OverlayStoreError>> {
        Box::pin(async move {
            self.log
                .policy_log(kind)
                .map_err(backend)?
                .into_iter()
                .map(|row| PolicyLogEntry::from_row(kind, row))
                .collect()
        })
    }

    fn auto_adopt<'a>(
        &'a self,
        kind: &'a str,
        overlay: &'a ProfileOverlay,
        run: ExecutionId,
        expected_active: Option<OverlayVersion>,
        run_policy: &'a RefinePolicy,
    ) -> BoxFuture<'a, Result<AutoAdoptOutcome, OverlayStoreError>> {
        Box::pin(async move {
            let actor = Provenance::auto(run).author();
            auto_adopt_gated(
                self,
                &*self.validator,
                kind,
                overlay,
                expected_active,
                run_policy,
                |bytes, cap, revision| {
                    let request = AutoAdoptRequest {
                        body: &bytes,
                        expected: expected_active.map(OverlayVersion::get),
                        actor: &actor,
                        cap: cap.limit(),
                        at: (self.clock)(),
                        policy_revision: revision,
                    };
                    std::future::ready(self.log.auto_adopt(kind, &request).map_err(backend))
                },
            )
            .await
        })
    }
}

#[cfg(feature = "sqlite")]
impl SqliteOverlayStore {
    /// One raw policy write of `kind` by `actor`.
    fn write_policy(
        &self,
        kind: &str,
        policy: Option<&RefinePolicy>,
        expected: PolicyExpect,
        actor: &Provenance,
    ) -> Result<PolicyLogEntry, OverlayStoreError> {
        actor.check_operator()?;
        let body = policy_body(policy)?;
        let author = actor.author();
        let write = PolicyWrite {
            body: body.as_deref(),
            expected,
            actor_kind: actor.author_kind(),
            actor: &author,
        };
        policy_appended(kind, self.log.append_policy(kind, &write).map_err(backend)?)
    }
}

/// A [`ProfileOverlayStore`] on Postgres.
///
/// The queries are in `autumn_harvest::overlay_log`. In a sharded
/// deployment, give the store the pool of shard 0.
#[cfg(feature = "postgres")]
pub struct PostgresOverlayStore {
    pool: autumn_harvest::worker::DbPool,
    validator: Arc<dyn OverlayValidator>,
    clock: StoreClock,
}

#[cfg(feature = "postgres")]
impl std::fmt::Debug for PostgresOverlayStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresOverlayStore")
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "postgres")]
impl PostgresOverlayStore {
    /// A store on `pool` that validates with `validator`.
    #[must_use]
    pub fn new(pool: autumn_harvest::worker::DbPool, validator: Arc<dyn OverlayValidator>) -> Self {
        Self {
            pool,
            validator,
            clock: system_clock(),
        }
    }

    /// The store with `clock` in place of the system clock.
    #[must_use]
    pub fn with_clock(mut self, clock: StoreClock) -> Self {
        self.clock = clock;
        self
    }

    /// Run one raw move of `kind` on a pool connection.
    async fn swap(
        &self,
        kind: &str,
        request: &MoveRequest<'_>,
    ) -> Result<ActivateOutcome, OverlayStoreError> {
        let mut conn = self.conn().await?;
        autumn_harvest::overlay_log::activate_overlay(&mut conn, kind, request)
            .await
            .map_err(backend)
    }

    async fn conn(
        &self,
    ) -> Result<
        impl std::ops::DerefMut<Target = autumn_harvest::diesel_async::AsyncPgConnection>,
        OverlayStoreError,
    > {
        self.pool.get().await.map_err(backend)
    }
}

#[cfg(feature = "postgres")]
impl ProfileOverlayStore for PostgresOverlayStore {
    fn put<'a>(
        &'a self,
        kind: &'a str,
        overlay: &'a ProfileOverlay,
        provenance: Provenance,
    ) -> BoxFuture<'a, Result<OverlayVersion, OverlayStoreError>> {
        Box::pin(async move {
            let policy = self.policy(kind).await?.policy;
            let bytes = encode_checked(&*self.validator, kind, overlay, &provenance, &policy)?;
            let mut conn = self.conn().await?;
            let row = autumn_harvest::overlay_log::append_overlay(
                &mut conn,
                kind,
                &bytes,
                provenance.author_kind(),
                &provenance.author(),
            )
            .await
            .map_err(backend)?;
            appended(row)
        })
    }

    fn activate<'a>(
        &'a self,
        kind: &'a str,
        version: OverlayVersion,
        expected_active: Option<OverlayVersion>,
        actor: Provenance,
    ) -> BoxFuture<'a, Result<(), OverlayStoreError>> {
        Box::pin(async move {
            activate_checked(self, &*self.validator, kind, version, &actor, || async {
                let author = actor.author();
                let request = move_request(version, expected_active, &actor, &author, None, None);
                self.swap(kind, &request).await
            })
            .await
        })
    }

    fn activate_guarded<'a>(
        &'a self,
        kind: &'a str,
        version: OverlayVersion,
        expected_active: Option<OverlayVersion>,
        head: Option<u64>,
        actor: Provenance,
    ) -> BoxFuture<'a, Result<(), OverlayStoreError>> {
        Box::pin(async move {
            activate_checked(self, &*self.validator, kind, version, &actor, || async {
                let author = actor.author();
                let request =
                    move_request(version, expected_active, &actor, &author, None, Some(head));
                self.swap(kind, &request).await
            })
            .await
        })
    }

    fn rollback<'a>(
        &'a self,
        kind: &'a str,
        actor: Provenance,
    ) -> BoxFuture<'a, Result<Rollback, OverlayStoreError>> {
        Box::pin(async move {
            let actor = &actor;
            rollback_checked(
                self,
                &*self.validator,
                kind,
                actor,
                |to, from, seq| async move {
                    let author = actor.author();
                    let request = move_request(to, Some(from), actor, &author, Some(seq), None);
                    self.swap(kind, &request).await
                },
            )
            .await
        })
    }

    fn last_activation<'a>(
        &'a self,
        kind: &'a str,
    ) -> BoxFuture<'a, Result<Option<Activation>, OverlayStoreError>> {
        Box::pin(async move {
            let mut conn = self.conn().await?;
            autumn_harvest::overlay_log::last_activation(&mut conn, kind)
                .await
                .map_err(backend)?
                .map(|row| Activation::from_row(kind, &row))
                .transpose()
        })
    }

    fn active<'a>(
        &'a self,
        kind: &'a str,
    ) -> BoxFuture<'a, Result<Option<StoredOverlay>, OverlayStoreError>> {
        Box::pin(async move {
            let mut conn = self.conn().await?;
            let row = autumn_harvest::overlay_log::active_overlay(&mut conn, kind)
                .await
                .map_err(backend)?;
            decode_one(kind, row)
        })
    }

    fn get<'a>(
        &'a self,
        kind: &'a str,
        version: OverlayVersion,
    ) -> BoxFuture<'a, Result<Option<StoredOverlay>, OverlayStoreError>> {
        Box::pin(async move {
            let mut conn = self.conn().await?;
            let row = autumn_harvest::overlay_log::get_overlay(&mut conn, kind, version.get())
                .await
                .map_err(backend)?;
            decode_one(kind, row)
        })
    }

    fn list<'a>(
        &'a self,
        kind: &'a str,
    ) -> BoxFuture<'a, Result<Vec<StoredOverlay>, OverlayStoreError>> {
        Box::pin(async move {
            let mut conn = self.conn().await?;
            let rows = autumn_harvest::overlay_log::list_overlays(&mut conn, kind)
                .await
                .map_err(backend)?;
            decode_all(kind, rows)
        })
    }

    fn policy<'a>(
        &'a self,
        kind: &'a str,
    ) -> BoxFuture<'a, Result<PolicyState, OverlayStoreError>> {
        Box::pin(async move {
            let mut conn = self.conn().await?;
            let last = autumn_harvest::overlay_log::last_policy(&mut conn, kind)
                .await
                .map_err(backend)?
                .map(|row| PolicyLogEntry::from_row(kind, row))
                .transpose()?;
            Ok(policy_state(last.as_ref()))
        })
    }

    fn set_policy<'a>(
        &'a self,
        kind: &'a str,
        policy: &'a RefinePolicy,
        expected_revision: Option<u64>,
        actor: Provenance,
    ) -> BoxFuture<'a, Result<PolicyLogEntry, OverlayStoreError>> {
        Box::pin(async move {
            self.write_policy(
                kind,
                Some(policy),
                PolicyExpect::Revision(expected_revision),
                &actor,
            )
            .await
        })
    }

    fn reset_policy<'a>(
        &'a self,
        kind: &'a str,
        actor: Provenance,
    ) -> BoxFuture<'a, Result<PolicyLogEntry, OverlayStoreError>> {
        Box::pin(async move {
            self.write_policy(kind, None, PolicyExpect::Any, &actor)
                .await
        })
    }

    fn policy_log<'a>(
        &'a self,
        kind: &'a str,
    ) -> BoxFuture<'a, Result<Vec<PolicyLogEntry>, OverlayStoreError>> {
        Box::pin(async move {
            let mut conn = self.conn().await?;
            autumn_harvest::overlay_log::policy_log(&mut conn, kind)
                .await
                .map_err(backend)?
                .into_iter()
                .map(|row| PolicyLogEntry::from_row(kind, row))
                .collect()
        })
    }

    fn auto_adopt<'a>(
        &'a self,
        kind: &'a str,
        overlay: &'a ProfileOverlay,
        run: ExecutionId,
        expected_active: Option<OverlayVersion>,
        run_policy: &'a RefinePolicy,
    ) -> BoxFuture<'a, Result<AutoAdoptOutcome, OverlayStoreError>> {
        Box::pin(async move {
            let actor = Provenance::auto(run).author();
            let actor = actor.as_str();
            auto_adopt_gated(
                self,
                &*self.validator,
                kind,
                overlay,
                expected_active,
                run_policy,
                |bytes, cap, revision| async move {
                    let request = AutoAdoptRequest {
                        body: &bytes,
                        expected: expected_active.map(OverlayVersion::get),
                        actor,
                        cap: cap.limit(),
                        at: (self.clock)(),
                        policy_revision: revision,
                    };
                    let mut conn = self.conn().await?;
                    autumn_harvest::overlay_log::auto_adopt_overlay(&mut conn, kind, &request)
                        .await
                        .map_err(backend)
                },
            )
            .await
        })
    }
}

#[cfg(feature = "postgres")]
impl PostgresOverlayStore {
    /// One raw policy write of `kind` by `actor`.
    async fn write_policy(
        &self,
        kind: &str,
        policy: Option<&RefinePolicy>,
        expected: PolicyExpect,
        actor: &Provenance,
    ) -> Result<PolicyLogEntry, OverlayStoreError> {
        actor.check_operator()?;
        let body = policy_body(policy)?;
        let author = actor.author();
        let write = PolicyWrite {
            body: body.as_deref(),
            expected,
            actor_kind: actor.author_kind(),
            actor: &author,
        };
        let mut conn = self.conn().await?;
        let outcome = autumn_harvest::overlay_log::append_policy(&mut conn, kind, &write)
            .await
            .map_err(backend)?;
        policy_appended(kind, outcome)
    }
}

/// Decode one stored row of `kind`.
#[cfg(any(feature = "sqlite", feature = "postgres"))]
fn decode_one(
    kind: &str,
    row: Option<OverlayRow>,
) -> Result<Option<StoredOverlay>, OverlayStoreError> {
    row.map(|row| StoredOverlay::from_row(kind, row))
        .transpose()
}

/// Decode the stored rows of `kind`.
#[cfg(any(feature = "sqlite", feature = "postgres"))]
fn decode_all(kind: &str, rows: Vec<OverlayRow>) -> Result<Vec<StoredOverlay>, OverlayStoreError> {
    rows.into_iter()
        .map(|row| StoredOverlay::from_row(kind, row))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::AgentProfile;
    use crate::error::{AgentError, ErrorKind};
    use crate::model::{AgentModel, ChatRequest, ChatResponse};

    /// A model that no test calls.
    #[derive(Debug)]
    struct NoModel;

    impl AgentModel for NoModel {
        fn chat<'a>(
            &'a self,
            _request: &'a ChatRequest,
        ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
            Box::pin(async { Err(AgentError::new(ErrorKind::Provider, "unused")) })
        }
    }

    fn harness() -> AgentHarness {
        AgentHarness::new(Arc::new(NoModel))
            .model_named("m1", Arc::new(NoModel))
            .agent(AgentProfile::new("support").max_steps(10).model("m1"))
    }

    #[test]
    fn the_harness_validates_under_the_ceilings_of_the_profile() {
        let harness = harness();
        let narrow = ProfileOverlay {
            max_steps: Some(4),
            ..ProfileOverlay::default()
        };
        assert_eq!(
            harness.validate("support", &narrow, &RefinePolicy::default()),
            Ok(())
        );
        let wide = ProfileOverlay {
            max_steps: Some(11),
            ..ProfileOverlay::default()
        };
        assert_eq!(
            harness.validate("support", &wide, &RefinePolicy::default()),
            Err(OverlayStoreError::Overlay(OverlayError::AboveCeiling {
                field: "max_steps"
            }))
        );
    }

    #[test]
    fn the_harness_refuses_a_kind_with_no_profile() {
        let long = "k".repeat(500);
        let err = harness().validate(&long, &ProfileOverlay::default(), &RefinePolicy::default());
        let Err(OverlayStoreError::UnknownKind { kind }) = err else {
            panic!("want UnknownKind, got {err:?}");
        };
        assert!(kind.starts_with(&"k".repeat(MAX_NAME_LEN)), "{kind}");
        assert!(kind.len() < 2 * MAX_NAME_LEN, "{kind}");
    }

    #[tokio::test]
    async fn a_store_on_the_harness_refuses_a_widening_put() {
        let store = InMemoryOverlayStore::new(Arc::new(harness()));
        let wide = ProfileOverlay {
            model: Some(crate::harness::DEFAULT_MODEL.to_owned()),
            ..ProfileOverlay::default()
        };
        let err = store
            .put("support", &wide, Provenance::operator("ops"))
            .await;
        assert!(
            matches!(
                err,
                Err(OverlayStoreError::Overlay(
                    OverlayError::ModelNotAllowed { .. }
                ))
            ),
            "{err:?}"
        );
        assert_eq!(store.list("support").await.expect("list"), []);
    }

    #[test]
    fn version_zero_does_not_exist() {
        assert_eq!(OverlayVersion::new(0), None);
        let v3 = OverlayVersion::new(3).expect("v3");
        assert_eq!(v3.get(), 3);
        assert_eq!(v3.to_string(), "v3");
    }

    #[test]
    fn an_operator_id_is_bounded() {
        assert_eq!(Provenance::operator("ops").check(), Ok(()));
        assert_eq!(
            Provenance::operator("o".repeat(MAX_AUTHOR_LEN)).check(),
            Ok(())
        );
        assert!(
            Provenance::operator("o".repeat(MAX_AUTHOR_LEN + 1))
                .check()
                .is_err()
        );
        assert_eq!(Provenance::refine(ExecutionId::new()).check(), Ok(()));
    }

    #[test]
    fn a_stored_row_that_does_not_decode_is_corrupt() {
        let row = |version, body: &[u8], author_kind: &str, author: &str| OverlayRow {
            version,
            body: body.to_vec(),
            author_kind: author_kind.to_owned(),
            author: author.to_owned(),
            created_at: Utc::now(),
        };
        for bad in [
            row(0, b"{}", AUTHOR_OPERATOR, "ops"),
            row(1, b"{\"tools\":[]}", AUTHOR_OPERATOR, "ops"),
            row(1, b"{}", "robot", "ops"),
            row(1, b"{}", AUTHOR_REFINE, "not-an-id"),
        ] {
            assert!(
                matches!(
                    StoredOverlay::from_row("support", bad),
                    Err(OverlayStoreError::Corrupt { .. })
                ),
                "a bad row decodes"
            );
        }
        let run = ExecutionId::new();
        let good =
            StoredOverlay::from_row("support", row(2, b"{}", AUTHOR_REFINE, &run.to_string()))
                .expect("decode");
        assert_eq!(good.provenance(), &Provenance::refine(run));
        assert_eq!(good.version().get(), 2);
    }

    #[test]
    fn an_operator_id_has_no_hidden_or_padded_text() {
        for bad in [
            " ops",
            "ops ",
            "o\u{200B}ps",
            "ops\u{202E}",
            "\u{FEFF}ops",
            "o\tps",
        ] {
            assert!(
                Provenance::operator(bad).check().is_err(),
                "{bad:?} must be refused"
            );
        }
        assert_eq!(Provenance::operator("ops team").check(), Ok(()));
        assert_eq!(Provenance::operator("ops@exämple").check(), Ok(()));
    }

    #[tokio::test]
    async fn a_large_overlay_is_refused() {
        let store = InMemoryOverlayStore::new(Arc::new(harness()));
        let big = ProfileOverlay {
            system: Some("x".repeat(MAX_OVERLAY_BYTES)),
            ..ProfileOverlay::default()
        };
        let err = store
            .put("support", &big, Provenance::operator("ops"))
            .await;
        assert!(
            matches!(
                err,
                Err(OverlayStoreError::TooLarge {
                    limit: MAX_OVERLAY_BYTES,
                    ..
                })
            ),
            "{err:?}"
        );
        assert_eq!(store.list("support").await.expect("list"), []);
    }

    #[test]
    fn a_conflict_names_the_active_version() {
        let three = OverlayStoreError::Conflict {
            actual: OverlayVersion::new(3),
        };
        assert_eq!(
            three.to_string(),
            "the active overlay is v3, not the expected version"
        );
        let unset = OverlayStoreError::Conflict { actual: None };
        assert_eq!(
            unset.to_string(),
            "the active overlay is unset, not the expected version"
        );
    }

    #[test]
    fn a_stored_pointer_of_zero_is_corrupt() {
        let version = OverlayVersion::new(1).expect("v1");
        assert!(matches!(
            activated(version, ActivateOutcome::Conflict { actual: Some(0) }),
            Err(OverlayStoreError::Corrupt { .. })
        ));
    }

    #[test]
    fn provenance_has_a_stable_json_form() {
        let json = serde_json::to_value(Provenance::operator("ops")).expect("encode");
        assert_eq!(json, serde_json::json!({"author": "operator", "id": "ops"}));
    }
}
