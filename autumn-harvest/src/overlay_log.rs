//! The log of agent profile overlays (issue #2371).
//!
//! Each agent kind has a log of immutable overlay versions and one active
//! pointer. The typed store is `ProfileOverlayStore` in
//! `autumn-harvest-agent`. This module holds the pure kernel and the
//! Postgres queries. `autumn-harvest-sqlite` holds the `SQLite` queries.
//!
//! The rules:
//!
//! - The first version of a kind is 1. Each later version is the last one
//!   plus 1. See [`next_overlay_version`]. The head row keeps the last
//!   version, and a trigger refuses its delete, so no version number comes
//!   back.
//! - A version row never changes. The trigger
//!   `harvest_agent_overlays_immutable_trg` refuses each `UPDATE`.
//! - An activation is a compare-and-swap on the pointer. See
//!   [`decide_activate`].
//!
//! The design is in `docs/design/DESIGN-2371.md`.

use chrono::{DateTime, Utc};

/// The `author_kind` of a version that an operator wrote.
pub const AUTHOR_OPERATOR: &str = "operator";

/// The `author_kind` of a version that a refine run wrote.
pub const AUTHOR_REFINE: &str = "refine";

/// What an activation does, from the facts that it reads under the lock of
/// the head row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivateDecision {
    /// Move the pointer to the version.
    Activate,
    /// Refuse. The kind has no such version.
    UnknownVersion,
    /// Refuse. The pointer is not the expected one.
    Conflict,
}

/// The outcome of an activation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivateOutcome {
    /// The pointer names the version now.
    Activated,
    /// The kind has no such version. The pointer did not move.
    UnknownVersion,
    /// The pointer is `actual`, not the expected one. It did not move.
    Conflict {
        /// The pointer at the time of the refusal.
        actual: Option<u64>,
    },
}

/// The version after `latest`, the last version of a kind. `None` means
/// that the counter is full.
///
/// `formal/verus/src/overlay_log.rs` proves it.
#[must_use]
pub const fn next_overlay_version(latest: Option<u64>) -> Option<u64> {
    match latest {
        None => Some(1),
        Some(last) => {
            if last < u64::MAX {
                Some(last + 1)
            } else {
                None
            }
        }
    }
}

/// What an activation does. `known` tells whether the version exists.
/// `expected` is the pointer that the caller read. `actual` is the pointer
/// under the lock.
///
/// An unknown version comes first, because a retry cannot fix it.
///
/// `formal/verus/src/overlay_log.rs` proves it.
#[must_use]
pub const fn decide_activate(
    known: bool,
    expected: Option<u64>,
    actual: Option<u64>,
) -> ActivateDecision {
    if !known {
        return ActivateDecision::UnknownVersion;
    }
    let same = match (expected, actual) {
        (None, None) => true,
        (Some(want), Some(have)) => want == have,
        _ => false,
    };
    if same {
        ActivateDecision::Activate
    } else {
        ActivateDecision::Conflict
    }
}

/// One stored overlay version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OverlayRow {
    /// The version, from 1.
    pub version: u64,
    /// The bytes that the writer stored.
    pub body: Vec<u8>,
    /// [`AUTHOR_OPERATOR`] or [`AUTHOR_REFINE`].
    pub author_kind: String,
    /// The operator id, or the execution id of the refine run.
    pub author: String,
    /// The time of the write.
    pub created_at: DateTime<Utc>,
}

#[cfg(feature = "db")]
#[doc(hidden)]
pub use pg::{activate_overlay, active_overlay, append_overlay, get_overlay, list_overlays};

/// The Postgres queries. They do not validate an overlay. Call them through
/// `ProfileOverlayStore`, which does.
#[cfg(feature = "db")]
mod pg {
    use chrono::{DateTime, Utc};
    use diesel::QueryableByName;
    use diesel::sql_types::{BigInt, Binary, Bool, Nullable, Text, Timestamptz};
    use diesel_async::{AsyncPgConnection, RunQueryDsl};

    use super::{ActivateDecision, ActivateOutcome, OverlayRow, decide_activate};
    use crate::error::{HarvestError, HarvestResult, database_error};

    /// The columns of one version row.
    const ROW_COLUMNS: &str = "version, body, author_kind, author, created_at";

    #[derive(QueryableByName)]
    struct Row {
        #[diesel(sql_type = BigInt)]
        version: i64,
        #[diesel(sql_type = Binary)]
        body: Vec<u8>,
        #[diesel(sql_type = Text)]
        author_kind: String,
        #[diesel(sql_type = Text)]
        author: String,
        #[diesel(sql_type = Timestamptz)]
        created_at: DateTime<Utc>,
    }

    impl Row {
        fn decode(self) -> HarvestResult<OverlayRow> {
            Ok(OverlayRow {
                version: to_version(self.version)?,
                body: self.body,
                author_kind: self.author_kind,
                author: self.author,
                created_at: self.created_at,
            })
        }
    }

    #[derive(QueryableByName)]
    struct Pointer {
        #[diesel(sql_type = Nullable<BigInt>)]
        active: Option<i64>,
    }

    #[derive(QueryableByName)]
    struct Exists {
        #[diesel(sql_type = Bool)]
        known: bool,
    }

    /// A stored version as `u64`. The table check keeps it at 1 or more.
    fn to_version(stored: i64) -> HarvestResult<u64> {
        u64::try_from(stored)
            .map_err(|_| HarvestError::Database(format!("overlay version {stored} is negative")))
    }

    /// The statement of [`append_overlay`].
    ///
    /// The upsert takes the lock of the head row, so two appends of one kind
    /// get two versions. A full counter matches no row, so the statement
    /// inserts nothing.
    fn append_overlay_sql() -> String {
        format!(
            "WITH head AS (
                 INSERT INTO harvest_agent_overlay_heads (kind, latest)
                 VALUES ($1, 1)
                 ON CONFLICT (kind) DO UPDATE
                     SET latest = harvest_agent_overlay_heads.latest + 1,
                         updated_at = NOW()
                     WHERE harvest_agent_overlay_heads.latest < {max}
                 RETURNING latest
             )
             INSERT INTO harvest_agent_overlays (kind, version, body, author_kind, author)
             SELECT $1, head.latest, $2, $3, $4 FROM head
             RETURNING {ROW_COLUMNS}",
            max = i64::MAX,
        )
    }

    /// Append `body` as the next version of `kind`. Returns the stored row,
    /// or `None` when the counter of the kind is full.
    ///
    /// The database sets the version and the time. The caller validates
    /// `body` first.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when the statement fails.
    pub async fn append_overlay(
        conn: &mut AsyncPgConnection,
        kind: &str,
        body: &[u8],
        author_kind: &str,
        author: &str,
    ) -> HarvestResult<Option<OverlayRow>> {
        let rows: Vec<Row> = diesel::sql_query(append_overlay_sql())
            .bind::<Text, _>(kind)
            .bind::<Binary, _>(body)
            .bind::<Text, _>(author_kind)
            .bind::<Text, _>(author)
            .load(conn)
            .await
            .map_err(database_error)?;
        rows.into_iter().next().map(Row::decode).transpose()
    }

    /// Move the pointer of `kind` to `version` when it is `expected`.
    ///
    /// One transaction locks the head row, then checks the version. The
    /// check is a new statement, so its snapshot sees each put that
    /// committed while the lock waited. [`decide_activate`] then decides. So
    /// of two activations with the same `expected`, only one moves the
    /// pointer. A version or an `expected` above `i64::MAX` never matches.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when a statement fails.
    pub async fn activate_overlay(
        conn: &mut AsyncPgConnection,
        kind: &str,
        version: u64,
        expected: Option<u64>,
    ) -> HarvestResult<ActivateOutcome> {
        use diesel_async::AsyncConnection;

        let Ok(stored) = i64::try_from(version) else {
            return Ok(ActivateOutcome::UnknownVersion);
        };
        Box::pin(conn.transaction(async |conn| {
            let Some(head) = lock_overlay_head(conn, kind).await? else {
                return Ok(ActivateOutcome::UnknownVersion);
            };
            let known = overlay_exists(conn, kind, stored).await?;
            let actual = head.active.map(to_version).transpose()?;
            match decide_activate(known, expected, actual) {
                ActivateDecision::UnknownVersion => Ok(ActivateOutcome::UnknownVersion),
                ActivateDecision::Conflict => Ok(ActivateOutcome::Conflict { actual }),
                ActivateDecision::Activate => {
                    set_overlay_pointer(conn, kind, stored, head.active).await?;
                    Ok(ActivateOutcome::Activated)
                }
            }
        }))
        .await
    }

    /// Lock the head row of `kind`, and read its pointer. `None` means that
    /// the kind has no version.
    async fn lock_overlay_head(
        conn: &mut AsyncPgConnection,
        kind: &str,
    ) -> HarvestResult<Option<Pointer>> {
        let rows: Vec<Pointer> = diesel::sql_query(
            "SELECT active FROM harvest_agent_overlay_heads WHERE kind = $1 FOR UPDATE",
        )
        .bind::<Text, _>(kind)
        .load(conn)
        .await
        .map_err(database_error)?;
        Ok(rows.into_iter().next())
    }

    /// Whether `kind` holds `version`.
    async fn overlay_exists(
        conn: &mut AsyncPgConnection,
        kind: &str,
        version: i64,
    ) -> HarvestResult<bool> {
        diesel::sql_query(
            "SELECT EXISTS (SELECT 1 FROM harvest_agent_overlays
                            WHERE kind = $1 AND version = $2) AS known",
        )
        .bind::<Text, _>(kind)
        .bind::<BigInt, _>(version)
        .get_result::<Exists>(conn)
        .await
        .map(|row| row.known)
        .map_err(database_error)
    }

    /// Set the pointer of `kind` under the lock of [`lock_overlay_head`].
    ///
    /// The compare repeats the decision in SQL. A row that it does not match
    /// fails the call, so a bug in the caller cannot move the pointer.
    async fn set_overlay_pointer(
        conn: &mut AsyncPgConnection,
        kind: &str,
        version: i64,
        expected: Option<i64>,
    ) -> HarvestResult<()> {
        let updated = diesel::sql_query(
            "UPDATE harvest_agent_overlay_heads
             SET active = $2, updated_at = NOW()
             WHERE kind = $1 AND active IS NOT DISTINCT FROM $3",
        )
        .bind::<Text, _>(kind)
        .bind::<BigInt, _>(version)
        .bind::<Nullable<BigInt>, _>(expected)
        .execute(conn)
        .await
        .map_err(database_error)?;
        if updated == 1 {
            Ok(())
        } else {
            Err(HarvestError::Database(format!(
                "the overlay pointer of {kind:?} moved under its lock"
            )))
        }
    }

    /// The active version of `kind`, or `None`.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when the query fails.
    pub async fn active_overlay(
        conn: &mut AsyncPgConnection,
        kind: &str,
    ) -> HarvestResult<Option<OverlayRow>> {
        let rows: Vec<Row> = diesel::sql_query(
            "SELECT o.version, o.body, o.author_kind, o.author, o.created_at
             FROM harvest_agent_overlay_heads h
             JOIN harvest_agent_overlays o ON o.kind = h.kind AND o.version = h.active
             WHERE h.kind = $1",
        )
        .bind::<Text, _>(kind)
        .load(conn)
        .await
        .map_err(database_error)?;
        rows.into_iter().next().map(Row::decode).transpose()
    }

    /// The version `version` of `kind`, or `None`.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when the query fails.
    pub async fn get_overlay(
        conn: &mut AsyncPgConnection,
        kind: &str,
        version: u64,
    ) -> HarvestResult<Option<OverlayRow>> {
        let Ok(stored) = i64::try_from(version) else {
            return Ok(None);
        };
        let rows: Vec<Row> = diesel::sql_query(format!(
            "SELECT {ROW_COLUMNS} FROM harvest_agent_overlays
             WHERE kind = $1 AND version = $2"
        ))
        .bind::<Text, _>(kind)
        .bind::<BigInt, _>(stored)
        .load(conn)
        .await
        .map_err(database_error)?;
        rows.into_iter().next().map(Row::decode).transpose()
    }

    /// Every version of `kind`, oldest first.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when the query fails.
    pub async fn list_overlays(
        conn: &mut AsyncPgConnection,
        kind: &str,
    ) -> HarvestResult<Vec<OverlayRow>> {
        let rows: Vec<Row> = diesel::sql_query(format!(
            "SELECT {ROW_COLUMNS} FROM harvest_agent_overlays
             WHERE kind = $1 ORDER BY version"
        ))
        .bind::<Text, _>(kind)
        .load(conn)
        .await
        .map_err(database_error)?;
        rows.into_iter().map(Row::decode).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_version_is_one_and_each_next_is_one_more() {
        assert_eq!(next_overlay_version(None), Some(1));
        assert_eq!(next_overlay_version(Some(1)), Some(2));
        assert_eq!(next_overlay_version(Some(41)), Some(42));
    }

    #[test]
    fn a_full_counter_has_no_next_version() {
        assert_eq!(next_overlay_version(Some(u64::MAX)), None);
        assert_eq!(next_overlay_version(Some(u64::MAX - 1)), Some(u64::MAX));
    }

    #[test]
    fn an_unknown_version_comes_before_a_conflict() {
        assert_eq!(
            decide_activate(false, Some(1), Some(2)),
            ActivateDecision::UnknownVersion
        );
        assert_eq!(
            decide_activate(false, None, None),
            ActivateDecision::UnknownVersion
        );
    }

    #[test]
    fn a_known_version_activates_only_over_the_expected_pointer() {
        use ActivateDecision::{Activate, Conflict};
        for (expected, actual, want) in [
            (None, None, Activate),
            (Some(3), Some(3), Activate),
            (None, Some(3), Conflict),
            (Some(3), None, Conflict),
            (Some(3), Some(4), Conflict),
        ] {
            assert_eq!(decide_activate(true, expected, actual), want);
        }
    }
}
