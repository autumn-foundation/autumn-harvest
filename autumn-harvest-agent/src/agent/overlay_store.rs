//! The versioned store of profile overlays (issue #2371).
//!
//! Each agent kind has a log of immutable overlay versions and one active
//! pointer. [`ProfileOverlayStore::put`] appends a version. A version never
//! changes after the put. [`ProfileOverlayStore::activate`] moves the
//! pointer with a compare-and-swap. A rollback activates an earlier version.
//!
//! Each store validates an overlay with [`overlay::apply`] before it stores
//! it, and again before it activates it. So no stored version widens its
//! profile. The design is in `docs/design/DESIGN-2371.md`.
//!
//! [`overlay::apply`]: super::overlay::apply

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use autumn_harvest::ExecutionId;
use autumn_harvest::chrono::{DateTime, Utc};
use autumn_harvest::overlay_log::{
    AUTHOR_OPERATOR, AUTHOR_REFINE, ActivateDecision, ActivateOutcome, OverlayRow, decide_activate,
    next_overlay_version,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::overlay::{Ceilings, OverlayError, ProfileOverlay, apply};
use crate::harness::{AgentHarness, truncate};
use crate::mesh::MAX_NAME_LEN;
use crate::model::BoxFuture;

/// The most characters in an operator id.
pub const MAX_AUTHOR_LEN: usize = 256;

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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "author", content = "id", rename_all = "snake_case")]
pub enum Provenance {
    /// An operator, by operator id.
    Operator(String),
    /// A refine run, by its execution id.
    Refine(ExecutionId),
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

    /// The stored author kind: `operator` or `refine`.
    #[must_use]
    pub const fn author_kind(&self) -> &'static str {
        match self {
            Self::Operator(_) => AUTHOR_OPERATOR,
            Self::Refine(_) => AUTHOR_REFINE,
        }
    }

    /// The stored author: the operator id, or the execution id.
    #[must_use]
    pub fn author(&self) -> String {
        match self {
            Self::Operator(id) => id.clone(),
            Self::Refine(execution) => execution.to_string(),
        }
    }

    /// Refuse an empty, long or multi-line operator id.
    fn check(&self) -> Result<(), OverlayStoreError> {
        let Self::Operator(id) = self else {
            return Ok(());
        };
        let reason = if id.trim().is_empty() {
            "the operator id is empty"
        } else if id.chars().count() > MAX_AUTHOR_LEN {
            "the operator id is too long"
        } else if id.chars().any(char::is_control) {
            "the operator id holds a control character"
        } else {
            return Ok(());
        };
        Err(OverlayStoreError::InvalidProvenance { reason })
    }

    /// The provenance of a stored row.
    fn decode(author_kind: &str, author: &str) -> Result<Self, OverlayStoreError> {
        match author_kind {
            AUTHOR_OPERATOR => Ok(Self::Operator(author.to_owned())),
            AUTHOR_REFINE => author
                .parse()
                .map(Self::Refine)
                .map_err(|_| corrupt("a refine author is not an execution id")),
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

    /// Decode a stored row of `kind`. A store on another backend can call
    /// it.
    ///
    /// # Errors
    ///
    /// Returns [`OverlayStoreError::Corrupt`] for a version 0, a body that
    /// does not decode, or an unknown author.
    pub fn from_row(kind: &str, row: OverlayRow) -> Result<Self, OverlayStoreError> {
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
    #[error("the active overlay is {actual:?}, not the expected version")]
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
    /// The version counter of the kind is full.
    #[error("the overlay version counter is full")]
    Full,
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
/// A store calls it before each put and each activation.
pub trait OverlayValidator: Send + Sync {
    /// Accept or refuse `overlay` for `kind`.
    ///
    /// # Errors
    ///
    /// Returns [`OverlayStoreError::UnknownKind`] for a kind with no
    /// profile, or [`OverlayStoreError::Overlay`] for an overlay that
    /// [`overlay::apply`](super::overlay::apply) refuses.
    fn validate(&self, kind: &str, overlay: &ProfileOverlay) -> Result<(), OverlayStoreError>;
}

/// The profile of the kind, under [`Ceilings::from_profile`], with the
/// models of the harness. So an overlay can only narrow its profile.
impl OverlayValidator for AgentHarness {
    fn validate(&self, kind: &str, overlay: &ProfileOverlay) -> Result<(), OverlayStoreError> {
        let profile = self
            .agent_profile(kind)
            .ok_or_else(|| OverlayStoreError::UnknownKind {
                kind: truncate(kind, MAX_NAME_LEN),
            })?;
        apply(&profile, overlay, &Ceilings::from_profile(&profile), self)?;
        Ok(())
    }
}

/// Storage for versioned profile overlays.
pub trait ProfileOverlayStore: Send + Sync + std::fmt::Debug {
    /// Validate `overlay` and append it as the next version of `kind`.
    ///
    /// # Errors
    ///
    /// The errors of [`OverlayValidator::validate`],
    /// [`OverlayStoreError::InvalidProvenance`], [`OverlayStoreError::Full`]
    /// and [`OverlayStoreError::Backend`].
    fn put<'a>(
        &'a self,
        kind: &'a str,
        overlay: &'a ProfileOverlay,
        provenance: Provenance,
    ) -> BoxFuture<'a, Result<OverlayVersion, OverlayStoreError>>;

    /// Make `version` the active version of `kind` when the active version
    /// is `expected_active`.
    ///
    /// # Errors
    ///
    /// [`OverlayStoreError::UnknownVersion`] for a version that does not
    /// exist, the errors of [`OverlayValidator::validate`],
    /// [`OverlayStoreError::Conflict`] for another active version, and
    /// [`OverlayStoreError::Backend`]. The pointer moves only on success.
    fn activate<'a>(
        &'a self,
        kind: &'a str,
        version: OverlayVersion,
        expected_active: Option<OverlayVersion>,
    ) -> BoxFuture<'a, Result<(), OverlayStoreError>>;

    /// The active version of `kind`, or `None`.
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
}

/// Check the provenance and the overlay, then encode the overlay. Every
/// store calls it first in `put`.
fn encode_checked(
    validator: &dyn OverlayValidator,
    kind: &str,
    overlay: &ProfileOverlay,
    provenance: &Provenance,
) -> Result<Vec<u8>, OverlayStoreError> {
    provenance.check()?;
    validator.validate(kind, overlay)?;
    serde_json::to_vec(overlay).map_err(backend)
}

/// The result of an activation outcome of `version`.
fn activated(version: OverlayVersion, outcome: ActivateOutcome) -> Result<(), OverlayStoreError> {
    match outcome {
        ActivateOutcome::Activated => Ok(()),
        ActivateOutcome::UnknownVersion => Err(OverlayStoreError::UnknownVersion { version }),
        ActivateOutcome::Conflict { actual } => Err(OverlayStoreError::Conflict {
            actual: actual.and_then(OverlayVersion::new),
        }),
    }
}

/// Activate through `store`: read the version, validate it, then call
/// `swap`. A version row never changes, so the check holds at the swap.
async fn activate_checked<S, F, Fut>(
    store: &S,
    validator: &dyn OverlayValidator,
    kind: &str,
    version: OverlayVersion,
    swap: F,
) -> Result<(), OverlayStoreError>
where
    S: ProfileOverlayStore + ?Sized,
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<ActivateOutcome, OverlayStoreError>>,
{
    let stored = store
        .get(kind, version)
        .await?
        .ok_or(OverlayStoreError::UnknownVersion { version })?;
    validator.validate(kind, stored.overlay())?;
    activated(version, swap().await?)
}

/// A [`ProfileOverlayStore`] in process memory. Its contents go when the
/// process stops, so use it for tests and demos only.
pub struct InMemoryOverlayStore {
    validator: Arc<dyn OverlayValidator>,
    kinds: Mutex<HashMap<String, KindLog>>,
}

/// The log of one kind in memory.
#[derive(Default)]
struct KindLog {
    versions: Vec<StoredOverlay>,
    active: Option<OverlayVersion>,
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
        }
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
        Box::pin(std::future::ready(self.put_now(kind, overlay, provenance)))
    }

    fn activate<'a>(
        &'a self,
        kind: &'a str,
        version: OverlayVersion,
        expected_active: Option<OverlayVersion>,
    ) -> BoxFuture<'a, Result<(), OverlayStoreError>> {
        Box::pin(activate_checked(
            self,
            &*self.validator,
            kind,
            version,
            move || std::future::ready(Ok(self.swap_now(kind, version, expected_active))),
        ))
    }

    fn active<'a>(
        &'a self,
        kind: &'a str,
    ) -> BoxFuture<'a, Result<Option<StoredOverlay>, OverlayStoreError>> {
        let kinds = self.lock();
        let active = kinds.get(kind).and_then(|log| {
            let version = log.active?;
            log.versions
                .iter()
                .find(|stored| stored.version == version)
                .cloned()
        });
        drop(kinds);
        Box::pin(std::future::ready(Ok(active)))
    }

    fn get<'a>(
        &'a self,
        kind: &'a str,
        version: OverlayVersion,
    ) -> BoxFuture<'a, Result<Option<StoredOverlay>, OverlayStoreError>> {
        let kinds = self.lock();
        let stored = kinds.get(kind).and_then(|log| {
            log.versions
                .iter()
                .find(|stored| stored.version == version)
                .cloned()
        });
        drop(kinds);
        Box::pin(std::future::ready(Ok(stored)))
    }

    fn list<'a>(
        &'a self,
        kind: &'a str,
    ) -> BoxFuture<'a, Result<Vec<StoredOverlay>, OverlayStoreError>> {
        let kinds = self.lock();
        let versions = kinds
            .get(kind)
            .map(|log| log.versions.clone())
            .unwrap_or_default();
        drop(kinds);
        Box::pin(std::future::ready(Ok(versions)))
    }
}

impl InMemoryOverlayStore {
    /// Validate and append one version under the lock.
    fn put_now(
        &self,
        kind: &str,
        overlay: &ProfileOverlay,
        provenance: Provenance,
    ) -> Result<OverlayVersion, OverlayStoreError> {
        let bytes = encode_checked(&*self.validator, kind, overlay, &provenance)?;
        let mut kinds = self.lock();
        let log = kinds.entry(kind.to_owned()).or_default();
        let latest = log.versions.last().map(|stored| stored.version.get());
        let version = next_overlay_version(latest)
            .and_then(OverlayVersion::new)
            .ok_or(OverlayStoreError::Full)?;
        log.versions.push(StoredOverlay {
            kind: kind.to_owned(),
            version,
            bytes,
            overlay: overlay.clone(),
            provenance,
            created_at: Utc::now(),
        });
        drop(kinds);
        Ok(version)
    }

    /// Swap the pointer under the lock.
    fn swap_now(
        &self,
        kind: &str,
        version: OverlayVersion,
        expected: Option<OverlayVersion>,
    ) -> ActivateOutcome {
        let mut kinds = self.lock();
        let outcome = kinds
            .get_mut(kind)
            .map_or(ActivateOutcome::UnknownVersion, |log| {
                let known = log.versions.iter().any(|stored| stored.version == version);
                let actual = log.active.map(OverlayVersion::get);
                match decide_activate(known, expected.map(OverlayVersion::get), actual) {
                    ActivateDecision::UnknownVersion => ActivateOutcome::UnknownVersion,
                    ActivateDecision::Conflict => ActivateOutcome::Conflict { actual },
                    ActivateDecision::Activate => {
                        log.active = Some(version);
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
/// `SqliteRuntime`.
#[cfg(feature = "sqlite")]
pub struct SqliteOverlayStore {
    log: autumn_harvest_sqlite::SqliteOverlayLog,
    validator: Arc<dyn OverlayValidator>,
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
        Ok(Self { log, validator })
    }

    /// Open a store in a new, private in-memory database.
    ///
    /// # Errors
    ///
    /// Returns [`OverlayStoreError::Backend`] when the database does not
    /// open.
    pub fn open_in_memory(validator: Arc<dyn OverlayValidator>) -> Result<Self, OverlayStoreError> {
        let log = autumn_harvest_sqlite::SqliteOverlayLog::open_in_memory().map_err(backend)?;
        Ok(Self { log, validator })
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
        let put = || {
            let bytes = encode_checked(&*self.validator, kind, overlay, &provenance)?;
            let row = self
                .log
                .append(kind, &bytes, provenance.author_kind(), &provenance.author())
                .map_err(backend)?;
            OverlayVersion::new(row.version).ok_or_else(|| corrupt("a stored version is 0"))
        };
        Box::pin(std::future::ready(put()))
    }

    fn activate<'a>(
        &'a self,
        kind: &'a str,
        version: OverlayVersion,
        expected_active: Option<OverlayVersion>,
    ) -> BoxFuture<'a, Result<(), OverlayStoreError>> {
        Box::pin(activate_checked(
            self,
            &*self.validator,
            kind,
            version,
            move || {
                std::future::ready(
                    self.log
                        .activate(
                            kind,
                            version.get(),
                            expected_active.map(OverlayVersion::get),
                        )
                        .map_err(backend),
                )
            },
        ))
    }

    fn active<'a>(
        &'a self,
        kind: &'a str,
    ) -> BoxFuture<'a, Result<Option<StoredOverlay>, OverlayStoreError>> {
        let row = self.log.active(kind).map_err(backend);
        Box::pin(std::future::ready(decode_one(kind, row)))
    }

    fn get<'a>(
        &'a self,
        kind: &'a str,
        version: OverlayVersion,
    ) -> BoxFuture<'a, Result<Option<StoredOverlay>, OverlayStoreError>> {
        let row = self.log.get(kind, version.get()).map_err(backend);
        Box::pin(std::future::ready(decode_one(kind, row)))
    }

    fn list<'a>(
        &'a self,
        kind: &'a str,
    ) -> BoxFuture<'a, Result<Vec<StoredOverlay>, OverlayStoreError>> {
        let rows = self.log.list(kind).map_err(backend);
        Box::pin(std::future::ready(decode_all(kind, rows)))
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
        Self { pool, validator }
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
            let bytes = encode_checked(&*self.validator, kind, overlay, &provenance)?;
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
            OverlayVersion::new(row.version).ok_or_else(|| corrupt("a stored version is 0"))
        })
    }

    fn activate<'a>(
        &'a self,
        kind: &'a str,
        version: OverlayVersion,
        expected_active: Option<OverlayVersion>,
    ) -> BoxFuture<'a, Result<(), OverlayStoreError>> {
        Box::pin(activate_checked(
            self,
            &*self.validator,
            kind,
            version,
            move || async move {
                let mut conn = self.conn().await?;
                autumn_harvest::overlay_log::activate_overlay(
                    &mut conn,
                    kind,
                    version.get(),
                    expected_active.map(OverlayVersion::get),
                )
                .await
                .map_err(backend)
            },
        ))
    }

    fn active<'a>(
        &'a self,
        kind: &'a str,
    ) -> BoxFuture<'a, Result<Option<StoredOverlay>, OverlayStoreError>> {
        Box::pin(async move {
            let mut conn = self.conn().await?;
            let row = autumn_harvest::overlay_log::active_overlay(&mut conn, kind)
                .await
                .map_err(backend);
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
                .map_err(backend);
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
                .map_err(backend);
            decode_all(kind, rows)
        })
    }
}

/// Decode one stored row of `kind`.
#[cfg(any(feature = "sqlite", feature = "postgres"))]
fn decode_one(
    kind: &str,
    row: Result<Option<OverlayRow>, OverlayStoreError>,
) -> Result<Option<StoredOverlay>, OverlayStoreError> {
    row?.map(|row| StoredOverlay::from_row(kind, row))
        .transpose()
}

/// Decode the stored rows of `kind`.
#[cfg(any(feature = "sqlite", feature = "postgres"))]
fn decode_all(
    kind: &str,
    rows: Result<Vec<OverlayRow>, OverlayStoreError>,
) -> Result<Vec<StoredOverlay>, OverlayStoreError> {
    rows?
        .into_iter()
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
        assert_eq!(harness.validate("support", &narrow), Ok(()));
        let wide = ProfileOverlay {
            max_steps: Some(11),
            ..ProfileOverlay::default()
        };
        assert_eq!(
            harness.validate("support", &wide),
            Err(OverlayStoreError::Overlay(OverlayError::AboveCeiling {
                field: "max_steps"
            }))
        );
    }

    #[test]
    fn the_harness_refuses_a_kind_with_no_profile() {
        let long = "k".repeat(500);
        let err = harness().validate(&long, &ProfileOverlay::default());
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
    fn provenance_has_a_stable_json_form() {
        let json = serde_json::to_value(Provenance::operator("ops")).expect("encode");
        assert_eq!(json, serde_json::json!({"author": "operator", "id": "ops"}));
    }
}
