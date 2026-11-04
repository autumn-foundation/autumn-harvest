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
//! - Each move of the pointer adds one row to the activation log, in the
//!   same transaction (issue #2374). See [`logs_activation`]. A rollback
//!   reverses the last move. See [`rollback_target`].
//!
//! - Each kind has an append-only refine policy log (issue #2394). Its last
//!   row is the current policy. See [`policy_write_passes`].
//! - An auto adoption puts a version and moves the pointer in one
//!   transaction. It counts the auto moves of one UTC day under the lock of
//!   the head row. See [`decide_auto_move`].
//!
//! The design is in `docs/design/DESIGN-2371.md`,
//! `docs/design/DESIGN-2374.md` and `docs/design/DESIGN-2394.md`.
//! `formal/verus/src/overlay_log.rs` proves the kernel.

use chrono::{DateTime, TimeDelta, Utc};

/// The `author_kind` of a version that an operator wrote.
pub const AUTHOR_OPERATOR: &str = "operator";

/// The `author_kind` of a version that a refine run wrote.
pub const AUTHOR_REFINE: &str = "refine";

/// The `author_kind` of a version that a refine run adopted with no person
/// (issue #2394).
pub const AUTHOR_AUTO: &str = "auto";

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

/// Whether an activation of `version` logs a move. `actual` is the pointer
/// under the lock. An activation of the active version moves nothing, so it
/// logs nothing.
///
/// `formal/verus/src/overlay_log.rs` proves it.
#[must_use]
pub const fn logs_activation(actual: Option<u64>, version: u64) -> bool {
    match actual {
        Some(have) => have != version,
        None => true,
    }
}

/// The plan of a rollback: activate `target` over the pointer `expected`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RollbackPlan {
    /// The version to activate: the start of the last move.
    pub target: u64,
    /// The expected pointer: the end of the last move.
    pub expected: u64,
}

/// The plan that reverses the last move `from -> to`. `None` means that no
/// version was active before the move, so no rollback exists.
///
/// `formal/verus/src/overlay_log.rs` proves it.
#[must_use]
pub const fn rollback_target(from: Option<u64>, to: u64) -> Option<RollbackPlan> {
    match from {
        Some(target) => Some(RollbackPlan {
            target,
            expected: to,
        }),
        None => None,
    }
}

/// What a rollback swap does, under the lock of the head row. `read_seq` is
/// the `seq` of the last move that the rollback read. `last_seq` is the
/// `seq` of the last move under the lock.
///
/// A move after the read changes `last_seq`, so the swap is a conflict. A
/// pointer that moved away and back therefore never passes.
///
/// `formal/verus/src/overlay_log.rs` proves it.
#[must_use]
pub const fn decide_rollback(
    known: bool,
    expected: u64,
    actual: Option<u64>,
    read_seq: u64,
    last_seq: Option<u64>,
) -> ActivateDecision {
    match decide_activate(known, Some(expected), actual) {
        ActivateDecision::Activate => match last_seq {
            Some(seq) if seq == read_seq => ActivateDecision::Activate,
            _ => ActivateDecision::Conflict,
        },
        other => other,
    }
}

/// One move request for a raw log. The caller validates the version first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MoveRequest<'a> {
    /// The version to activate.
    pub version: u64,
    /// The pointer that the caller read.
    pub expected: Option<u64>,
    /// [`AUTHOR_OPERATOR`] or [`AUTHOR_REFINE`].
    pub actor_kind: &'a str,
    /// The operator id, or the execution id of the refine run.
    pub actor: &'a str,
    /// For a rollback, the `seq` of the last move that it read. The swap
    /// then also needs that move to be the last one. See
    /// [`decide_rollback`].
    pub after_seq: Option<u64>,
}

impl MoveRequest<'_> {
    /// The decision under the lock. `last_seq` is the `seq` of the last
    /// move, and it counts only for a rollback.
    #[must_use]
    pub const fn decide(
        &self,
        known: bool,
        actual: Option<u64>,
        last_seq: Option<u64>,
    ) -> ActivateDecision {
        match (self.after_seq, self.expected) {
            (Some(read_seq), Some(expected)) => {
                decide_rollback(known, expected, actual, read_seq, last_seq)
            }
            // A rollback always expects a pointer. A guard with none never
            // passes, so no caller can drop the guard by mistake.
            (Some(_), None) => match decide_activate(known, None, actual) {
                ActivateDecision::UnknownVersion => ActivateDecision::UnknownVersion,
                _ => ActivateDecision::Conflict,
            },
            (None, _) => decide_activate(known, self.expected, actual),
        }
    }
}

/// One row of the activation log: a move of the pointer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivationRow {
    /// The number of the move in its kind, from 1.
    pub seq: u64,
    /// The pointer before the move.
    pub from: Option<u64>,
    /// The pointer after the move.
    pub to: u64,
    /// [`AUTHOR_OPERATOR`] or [`AUTHOR_REFINE`].
    pub actor_kind: String,
    /// The operator id, or the execution id of the refine run.
    pub actor: String,
    /// The time of the move.
    pub activated_at: DateTime<Utc>,
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

/// The start and the end of the UTC day of `at`.
#[must_use]
pub fn utc_day(at: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>) {
    let start = at
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .map_or(at, |midnight| midnight.and_utc());
    let end = start
        .checked_add_signed(TimeDelta::days(1))
        .unwrap_or(DateTime::<Utc>::MAX_UTC);
    (start, end)
}

/// The revision that a policy write expects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyExpect {
    /// No compare. A reset writes over any revision.
    Any,
    /// The revision that the caller read, or `None` for a kind with no
    /// change.
    Revision(Option<u64>),
}

/// Whether a policy write that expects `expected` passes. `last` is the
/// revision under the write.
#[must_use]
pub const fn policy_write_passes(expected: PolicyExpect, last: Option<u64>) -> bool {
    match expected {
        PolicyExpect::Any => true,
        PolicyExpect::Revision(None) => last.is_none(),
        PolicyExpect::Revision(Some(want)) => match last {
            Some(have) => want == have,
            None => false,
        },
    }
}

/// What an auto adoption does under the lock of the head row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoMoveDecision {
    /// Move the pointer to the new version.
    Move,
    /// Refuse. The pointer is not the expected one.
    Conflict,
    /// Refuse. The auto moves of the day reach the cap.
    CapReached,
}

/// What an auto adoption does. `expected` is the pointer that the caller
/// read, and `actual` is the pointer under the lock. `used` counts the auto
/// moves of the day. `cap` is the daily cap, or `None` for no cap.
///
/// A stale pointer comes first, because the caller must read again.
///
/// `formal/verus/src/overlay_log.rs` proves it.
#[must_use]
pub const fn decide_auto_move(
    expected: Option<u64>,
    actual: Option<u64>,
    used: u64,
    cap: Option<u32>,
) -> AutoMoveDecision {
    let same = match (expected, actual) {
        (None, None) => true,
        (Some(want), Some(have)) => want == have,
        _ => false,
    };
    if !same {
        return AutoMoveDecision::Conflict;
    }
    match cap {
        Some(limit) if used >= limit as u64 => AutoMoveDecision::CapReached,
        _ => AutoMoveDecision::Move,
    }
}

/// One row of the refine policy log of a kind (issue #2394).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyRow {
    /// The number of the change in its kind, from 1.
    pub seq: u64,
    /// The body before the change. `None` means the defaults.
    pub old_body: Option<Vec<u8>>,
    /// The body after the change. `None` means the defaults.
    pub new_body: Option<Vec<u8>>,
    /// [`AUTHOR_OPERATOR`]. Only an operator writes a policy.
    pub actor_kind: String,
    /// The operator id.
    pub actor: String,
    /// The time of the change.
    pub changed_at: DateTime<Utc>,
}

/// One write of the refine policy log. The caller checks the actor and the
/// body first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PolicyWrite<'a> {
    /// The new body, or `None` for the defaults.
    pub body: Option<&'a [u8]>,
    /// The revision that the caller expects.
    pub expected: PolicyExpect,
    /// [`AUTHOR_OPERATOR`].
    pub actor_kind: &'a str,
    /// The operator id.
    pub actor: &'a str,
}

/// The outcome of a policy write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyAppend {
    /// The log holds the new row.
    Appended(PolicyRow),
    /// The last revision is `actual`, not the expected one.
    Conflict {
        /// The last revision at the time of the refusal.
        actual: Option<u64>,
    },
}

/// One auto adoption request. The caller validates the body first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutoAdoptRequest<'a> {
    /// The overlay body.
    pub body: &'a [u8],
    /// The pointer that the caller read.
    pub expected: Option<u64>,
    /// The execution id of the refine run.
    pub actor: &'a str,
    /// The daily cap, or `None` for no cap.
    pub cap: Option<u32>,
    /// The time of the move. Its UTC day is the day of the cap.
    pub at: DateTime<Utc>,
    /// The policy revision that the gate read. The transaction compares
    /// it, so a policy change after the gate refuses the move.
    pub policy_revision: Option<u64>,
}

/// The outcome of an auto adoption. Only `Adopted` changes the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AutoAdoptRaw {
    /// The new version is active.
    Adopted(OverlayRow),
    /// The auto moves of the day reach the cap. `used` counts them.
    CapReached {
        /// The auto moves of the day.
        used: u64,
    },
    /// The pointer is `actual`, not the expected one.
    Conflict {
        /// The pointer at the time of the refusal.
        actual: Option<u64>,
    },
    /// The version counter of the kind is full.
    Full,
    /// The policy changed after the gate read it. The caller runs the gate
    /// again.
    PolicyMoved {
        /// The policy revision at the time of the refusal.
        actual: Option<u64>,
    },
}

#[cfg(feature = "db")]
#[doc(hidden)]
pub use pg::{
    activate_overlay, active_overlay, append_overlay, append_policy, auto_adopt_overlay,
    get_overlay, last_activation, last_policy, list_overlays, policy_log,
};

/// The Postgres queries. They do not validate an overlay. Call them through
/// `ProfileOverlayStore`, which does.
#[cfg(feature = "db")]
mod pg {
    use chrono::{DateTime, Utc};
    use diesel::QueryableByName;
    use diesel::sql_types::{BigInt, Binary, Bool, Nullable, Text, Timestamptz};
    use diesel_async::{AsyncPgConnection, RunQueryDsl};

    use super::{
        AUTHOR_AUTO, ActivateDecision, ActivateOutcome, ActivationRow, AutoAdoptRaw,
        AutoAdoptRequest, AutoMoveDecision, MoveRequest, OverlayRow, PolicyAppend, PolicyExpect,
        PolicyRow, PolicyWrite, decide_auto_move, logs_activation, utc_day,
    };
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

    /// Move the pointer of `kind` as `request` asks, and log the move with
    /// its actor.
    ///
    /// One transaction locks the head row, then checks the version. The
    /// check is a new statement, so its snapshot sees each put that
    /// committed while the lock waited. [`MoveRequest::decide`] then
    /// decides. So of two activations with the same `expected`, only one
    /// moves the pointer. A version or an `expected` above `i64::MAX` never
    /// matches. The log row commits with the move. An activation of the
    /// active version logs nothing.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when a statement fails.
    pub async fn activate_overlay(
        conn: &mut AsyncPgConnection,
        kind: &str,
        request: &MoveRequest<'_>,
    ) -> HarvestResult<ActivateOutcome> {
        use diesel_async::AsyncConnection;

        let Ok(stored) = i64::try_from(request.version) else {
            return Ok(ActivateOutcome::UnknownVersion);
        };
        Box::pin(conn.transaction(async |conn| {
            let Some(head) = lock_overlay_head(conn, kind).await? else {
                return Ok(ActivateOutcome::UnknownVersion);
            };
            let known = overlay_exists(conn, kind, stored).await?;
            let actual = head.active.map(to_version).transpose()?;
            let last_seq = match request.after_seq {
                Some(_) => last_activation(conn, kind).await?.map(|row| row.seq),
                None => None,
            };
            match request.decide(known, actual, last_seq) {
                ActivateDecision::UnknownVersion => Ok(ActivateOutcome::UnknownVersion),
                ActivateDecision::Conflict => Ok(ActivateOutcome::Conflict { actual }),
                ActivateDecision::Activate => {
                    if logs_activation(actual, request.version) {
                        set_overlay_pointer(conn, kind, stored, head.active).await?;
                        log_activation(conn, kind, head.active, stored, request).await?;
                    }
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

    /// Add one move to the activation log of `kind`, under the lock of
    /// [`lock_overlay_head`]. The lock makes the next `seq` safe.
    async fn log_activation(
        conn: &mut AsyncPgConnection,
        kind: &str,
        from: Option<i64>,
        to: i64,
        request: &MoveRequest<'_>,
    ) -> HarvestResult<()> {
        diesel::sql_query(
            "INSERT INTO harvest_agent_overlay_activations
                 (kind, seq, from_version, to_version, actor_kind, actor)
             SELECT $1, COALESCE(MAX(seq), 0) + 1, $2, $3, $4, $5
             FROM harvest_agent_overlay_activations WHERE kind = $1",
        )
        .bind::<Text, _>(kind)
        .bind::<Nullable<BigInt>, _>(from)
        .bind::<BigInt, _>(to)
        .bind::<Text, _>(request.actor_kind)
        .bind::<Text, _>(request.actor)
        .execute(conn)
        .await
        .map_err(database_error)?;
        Ok(())
    }

    #[derive(QueryableByName)]
    struct Move {
        #[diesel(sql_type = BigInt)]
        seq: i64,
        #[diesel(sql_type = Nullable<BigInt>)]
        from_version: Option<i64>,
        #[diesel(sql_type = BigInt)]
        to_version: i64,
        #[diesel(sql_type = Text)]
        actor_kind: String,
        #[diesel(sql_type = Text)]
        actor: String,
        #[diesel(sql_type = Timestamptz)]
        activated_at: DateTime<Utc>,
    }

    /// The last move of the pointer of `kind`, or `None`.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when the query fails.
    pub async fn last_activation(
        conn: &mut AsyncPgConnection,
        kind: &str,
    ) -> HarvestResult<Option<ActivationRow>> {
        let rows: Vec<Move> = diesel::sql_query(
            "SELECT seq, from_version, to_version, actor_kind, actor, activated_at
             FROM harvest_agent_overlay_activations
             WHERE kind = $1 ORDER BY seq DESC LIMIT 1",
        )
        .bind::<Text, _>(kind)
        .load(conn)
        .await
        .map_err(database_error)?;
        rows.into_iter()
            .next()
            .map(|row| {
                Ok(ActivationRow {
                    seq: to_version(row.seq)?,
                    from: row.from_version.map(to_version).transpose()?,
                    to: to_version(row.to_version)?,
                    actor_kind: row.actor_kind,
                    actor: row.actor,
                    activated_at: row.activated_at,
                })
            })
            .transpose()
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

    /// The columns of one policy row.
    const POLICY_COLUMNS: &str = "seq, old_body, new_body, actor_kind, actor, changed_at";

    #[derive(QueryableByName)]
    struct Policy {
        #[diesel(sql_type = BigInt)]
        seq: i64,
        #[diesel(sql_type = Nullable<Binary>)]
        old_body: Option<Vec<u8>>,
        #[diesel(sql_type = Nullable<Binary>)]
        new_body: Option<Vec<u8>>,
        #[diesel(sql_type = Text)]
        actor_kind: String,
        #[diesel(sql_type = Text)]
        actor: String,
        #[diesel(sql_type = Timestamptz)]
        changed_at: DateTime<Utc>,
    }

    impl Policy {
        fn decode(self) -> HarvestResult<PolicyRow> {
            Ok(PolicyRow {
                seq: to_version(self.seq)?,
                old_body: self.old_body,
                new_body: self.new_body,
                actor_kind: self.actor_kind,
                actor: self.actor,
                changed_at: self.changed_at,
            })
        }
    }

    /// The last row of the refine policy log of `kind`, or `None`.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when the query fails.
    pub async fn last_policy(
        conn: &mut AsyncPgConnection,
        kind: &str,
    ) -> HarvestResult<Option<PolicyRow>> {
        let rows: Vec<Policy> = diesel::sql_query(format!(
            "SELECT {POLICY_COLUMNS} FROM harvest_agent_refine_policies
             WHERE kind = $1 ORDER BY seq DESC LIMIT 1"
        ))
        .bind::<Text, _>(kind)
        .load(conn)
        .await
        .map_err(database_error)?;
        rows.into_iter().next().map(Policy::decode).transpose()
    }

    /// Every row of the refine policy log of `kind`, oldest first.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when the query fails.
    pub async fn policy_log(
        conn: &mut AsyncPgConnection,
        kind: &str,
    ) -> HarvestResult<Vec<PolicyRow>> {
        let rows: Vec<Policy> = diesel::sql_query(format!(
            "SELECT {POLICY_COLUMNS} FROM harvest_agent_refine_policies
             WHERE kind = $1 ORDER BY seq"
        ))
        .bind::<Text, _>(kind)
        .load(conn)
        .await
        .map_err(database_error)?;
        rows.into_iter().map(Policy::decode).collect()
    }

    /// The statement of [`append_policy`].
    ///
    /// It reads the last row and inserts the next one in one statement. The
    /// compare runs in the `WHERE`. A writer that read the same last row
    /// takes the same `seq`, so the primary key refuses it.
    const APPEND_POLICY_SQL: &str = "WITH last AS (
             SELECT seq, new_body FROM harvest_agent_refine_policies
             WHERE kind = $1 ORDER BY seq DESC LIMIT 1
         )
         INSERT INTO harvest_agent_refine_policies
             (kind, seq, old_body, new_body, actor_kind, actor)
         SELECT $1, COALESCE((SELECT seq FROM last), 0) + 1,
                (SELECT new_body FROM last), $2, $3, $4
         WHERE NOT $5 OR (SELECT seq FROM last) IS NOT DISTINCT FROM $6
         RETURNING seq, old_body, new_body, actor_kind, actor, changed_at";

    /// The most tries of a write with no compare. Each lost try means that
    /// another writer committed a row.
    const POLICY_TRIES: usize = 8;

    /// The transaction lock of the refine policy of a kind. Its key space
    /// is a 64-bit hash with a prefix of its own, as the fairness weights
    /// lock does.
    const POLICY_LOCK_SQL: &str =
        "SELECT pg_advisory_xact_lock(hashtextextended('harvest_agent_refine_policy:' || $1, 0))";

    /// Take the transaction lock of the refine policy of `kind`.
    ///
    /// A policy write and an auto adoption of one kind take it first. So a
    /// policy change never commits between the gate check of an adoption
    /// and its move.
    async fn lock_policy(conn: &mut AsyncPgConnection, kind: &str) -> HarvestResult<()> {
        diesel::sql_query(POLICY_LOCK_SQL)
            .bind::<Text, _>(kind)
            .execute(conn)
            .await
            .map_err(database_error)?;
        Ok(())
    }

    /// Append one row to the refine policy log of `kind`.
    ///
    /// A write with a compare that does not match, or that loses a race,
    /// returns [`PolicyAppend::Conflict`]. A write with no compare tries
    /// again after a lost race.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when a statement fails.
    pub async fn append_policy(
        conn: &mut AsyncPgConnection,
        kind: &str,
        write: &PolicyWrite<'_>,
    ) -> HarvestResult<PolicyAppend> {
        let (compare, expected) = match write.expected {
            PolicyExpect::Any => (false, None),
            PolicyExpect::Revision(None) => (true, None),
            PolicyExpect::Revision(Some(seq)) => {
                // A revision above `i64::MAX` was never stored.
                let Ok(seq) = i64::try_from(seq) else {
                    let actual = last_policy(conn, kind).await?.map(|row| row.seq);
                    return Ok(PolicyAppend::Conflict { actual });
                };
                (true, Some(seq))
            }
        };
        for _ in 0..POLICY_TRIES {
            let result: Result<Vec<Policy>, diesel::result::Error> = Box::pin(
                diesel_async::AsyncConnection::transaction(conn, async |conn| {
                    diesel::sql_query(POLICY_LOCK_SQL)
                        .bind::<Text, _>(kind)
                        .execute(conn)
                        .await?;
                    diesel::sql_query(APPEND_POLICY_SQL)
                        .bind::<Text, _>(kind)
                        .bind::<Nullable<Binary>, _>(write.body)
                        .bind::<Text, _>(write.actor_kind)
                        .bind::<Text, _>(write.actor)
                        .bind::<Bool, _>(compare)
                        .bind::<Nullable<BigInt>, _>(expected)
                        .load(conn)
                        .await
                }),
            )
            .await;
            match result {
                Ok(rows) => {
                    if let Some(row) = rows.into_iter().next() {
                        return Ok(PolicyAppend::Appended(row.decode()?));
                    }
                    let actual = last_policy(conn, kind).await?.map(|row| row.seq);
                    return Ok(PolicyAppend::Conflict { actual });
                }
                Err(diesel::result::Error::DatabaseError(
                    diesel::result::DatabaseErrorKind::UniqueViolation,
                    _,
                )) if !compare => {}
                Err(diesel::result::Error::DatabaseError(
                    diesel::result::DatabaseErrorKind::UniqueViolation,
                    _,
                )) => {
                    let actual = last_policy(conn, kind).await?.map(|row| row.seq);
                    return Ok(PolicyAppend::Conflict { actual });
                }
                Err(error) => return Err(database_error(error)),
            }
        }
        Err(HarvestError::Database(format!(
            "the refine policy of {kind:?} lost {POLICY_TRIES} races in a row"
        )))
    }

    /// Why an auto adoption transaction stops.
    enum Stop {
        /// A statement failed.
        Failed(HarvestError),
        /// The adoption refuses. The transaction rolls back.
        Refused(AutoAdoptRaw),
    }

    impl From<diesel::result::Error> for Stop {
        fn from(error: diesel::result::Error) -> Self {
            Self::Failed(database_error(error))
        }
    }

    impl From<HarvestError> for Stop {
        fn from(error: HarvestError) -> Self {
            Self::Failed(error)
        }
    }

    #[derive(QueryableByName)]
    struct Count {
        #[diesel(sql_type = BigInt)]
        used: i64,
    }

    /// Put `request.body` as the next version of `kind`, and make it active,
    /// in one transaction (issue #2394).
    ///
    /// The transaction first takes the policy lock of the kind, and checks
    /// that the policy revision is the one that the gate read. A changed
    /// policy returns [`AutoAdoptRaw::PolicyMoved`]. Then the put takes the
    /// lock of the head row. Under it, the transaction
    /// checks the pointer and counts the auto moves of the UTC day of
    /// `request.at`. [`decide_auto_move`] decides. A refusal rolls the put
    /// back, so nothing changes. The move logs `request.at` as its time, so
    /// the count and the log use one clock.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when a statement fails.
    pub async fn auto_adopt_overlay(
        conn: &mut AsyncPgConnection,
        kind: &str,
        request: &AutoAdoptRequest<'_>,
    ) -> HarvestResult<AutoAdoptRaw> {
        use diesel_async::AsyncConnection;

        let Ok(expected) = request.expected.map(i64::try_from).transpose() else {
            return Ok(AutoAdoptRaw::Conflict { actual: None });
        };
        let outcome = Box::pin(conn.transaction(async |conn| {
            lock_policy(conn, kind).await?;
            let revision = last_policy(conn, kind).await?.map(|row| row.seq);
            if revision != request.policy_revision {
                return Err(Stop::Refused(AutoAdoptRaw::PolicyMoved {
                    actual: revision,
                }));
            }
            let rows: Vec<Row> = diesel::sql_query(append_overlay_sql())
                .bind::<Text, _>(kind)
                .bind::<Binary, _>(request.body)
                .bind::<Text, _>(AUTHOR_AUTO)
                .bind::<Text, _>(request.actor)
                .load(conn)
                .await?;
            let Some(row) = rows.into_iter().next() else {
                return Err(Stop::Refused(AutoAdoptRaw::Full));
            };
            let row = row.decode()?;
            let version = i64::try_from(row.version)
                .map_err(|_| HarvestError::Database("an overlay version is too large".into()))?;
            let head = lock_overlay_head(conn, kind)
                .await?
                .ok_or_else(|| HarvestError::Database(format!("no head row for {kind:?}")))?;
            let actual = head.active.map(to_version).transpose()?;
            let used = match request.cap {
                Some(_) => count_auto_moves(conn, kind, request).await?,
                None => 0,
            };
            match decide_auto_move(request.expected, actual, used, request.cap) {
                AutoMoveDecision::Conflict => {
                    return Err(Stop::Refused(AutoAdoptRaw::Conflict { actual }));
                }
                AutoMoveDecision::CapReached => {
                    return Err(Stop::Refused(AutoAdoptRaw::CapReached { used }));
                }
                AutoMoveDecision::Move => {}
            }
            set_overlay_pointer(conn, kind, version, expected).await?;
            diesel::sql_query(
                "INSERT INTO harvest_agent_overlay_activations
                     (kind, seq, from_version, to_version, actor_kind, actor, activated_at)
                 SELECT $1, COALESCE(MAX(seq), 0) + 1, $2, $3, $4, $5, $6
                 FROM harvest_agent_overlay_activations WHERE kind = $1",
            )
            .bind::<Text, _>(kind)
            .bind::<Nullable<BigInt>, _>(head.active)
            .bind::<BigInt, _>(version)
            .bind::<Text, _>(AUTHOR_AUTO)
            .bind::<Text, _>(request.actor)
            .bind::<Timestamptz, _>(request.at)
            .execute(conn)
            .await?;
            Ok(AutoAdoptRaw::Adopted(row))
        }))
        .await;
        match outcome {
            Ok(adopted) => Ok(adopted),
            Err(Stop::Refused(refused)) => Ok(refused),
            Err(Stop::Failed(error)) => Err(error),
        }
    }

    /// The auto moves of `kind` in the UTC day of `request.at`.
    async fn count_auto_moves(
        conn: &mut AsyncPgConnection,
        kind: &str,
        request: &AutoAdoptRequest<'_>,
    ) -> HarvestResult<u64> {
        let (start, end) = utc_day(request.at);
        let count: Count = diesel::sql_query(
            "SELECT COUNT(*) AS used FROM harvest_agent_overlay_activations
             WHERE kind = $1 AND actor_kind = $2
               AND activated_at >= $3 AND activated_at < $4",
        )
        .bind::<Text, _>(kind)
        .bind::<Text, _>(AUTHOR_AUTO)
        .bind::<Timestamptz, _>(start)
        .bind::<Timestamptz, _>(end)
        .get_result(conn)
        .await
        .map_err(database_error)?;
        Ok(u64::try_from(count.used).unwrap_or(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_policy_write_compares_the_revision() {
        use PolicyExpect::{Any, Revision};
        assert!(policy_write_passes(Any, None));
        assert!(policy_write_passes(Any, Some(4)));
        assert!(policy_write_passes(Revision(None), None));
        assert!(!policy_write_passes(Revision(None), Some(1)));
        assert!(policy_write_passes(Revision(Some(4)), Some(4)));
        assert!(!policy_write_passes(Revision(Some(3)), Some(4)));
        assert!(!policy_write_passes(Revision(Some(1)), None));
    }

    #[test]
    fn an_auto_move_checks_the_pointer_then_the_cap() {
        use AutoMoveDecision::{CapReached, Conflict, Move};
        assert_eq!(decide_auto_move(None, None, 0, Some(1)), Move);
        assert_eq!(decide_auto_move(Some(1), Some(1), 1, Some(1)), CapReached);
        assert_eq!(decide_auto_move(Some(1), Some(2), 0, Some(1)), Conflict);
        assert_eq!(decide_auto_move(None, Some(2), 9, Some(1)), Conflict);
        assert_eq!(decide_auto_move(Some(1), Some(1), u64::MAX, None), Move);
        assert_eq!(decide_auto_move(Some(1), Some(1), 0, Some(0)), CapReached);
    }

    #[test]
    fn a_utc_day_runs_from_midnight_to_midnight() {
        let at = DateTime::parse_from_rfc3339("2026-11-03T23:59:59Z")
            .expect("time")
            .with_timezone(&Utc);
        let (start, end) = utc_day(at);
        assert_eq!(start.to_rfc3339(), "2026-11-03T00:00:00+00:00");
        assert_eq!(end.to_rfc3339(), "2026-11-04T00:00:00+00:00");
        assert_eq!(utc_day(start), (start, end));
        assert_eq!(utc_day(end).0, end);
    }

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
    fn only_a_move_logs_an_activation() {
        assert!(logs_activation(None, 1));
        assert!(logs_activation(Some(1), 2));
        assert!(!logs_activation(Some(2), 2));
    }

    #[test]
    fn a_rollback_swap_needs_the_move_that_it_read() {
        use ActivateDecision::{Activate, Conflict, UnknownVersion};
        assert_eq!(decide_rollback(true, 2, Some(2), 5, Some(5)), Activate);
        assert_eq!(decide_rollback(true, 2, Some(2), 5, Some(7)), Conflict);
        assert_eq!(decide_rollback(true, 2, Some(2), 5, None), Conflict);
        assert_eq!(decide_rollback(true, 2, Some(3), 5, Some(5)), Conflict);
        assert_eq!(
            decide_rollback(false, 2, Some(2), 5, Some(5)),
            UnknownVersion
        );
        let rollback = MoveRequest {
            version: 1,
            expected: Some(2),
            actor_kind: AUTHOR_OPERATOR,
            actor: "ops",
            after_seq: Some(5),
        };
        assert_eq!(rollback.decide(true, Some(2), Some(6)), Conflict);
        let plain = MoveRequest {
            after_seq: None,
            ..rollback
        };
        assert_eq!(plain.decide(true, Some(2), Some(6)), Activate);
        let unguarded = MoveRequest {
            expected: None,
            ..rollback
        };
        assert_eq!(unguarded.decide(true, None, Some(5)), Conflict);
        assert_eq!(unguarded.decide(false, None, Some(5)), UnknownVersion);
    }

    #[test]
    fn a_rollback_reverses_the_last_move() {
        assert_eq!(
            rollback_target(Some(1), 2),
            Some(RollbackPlan {
                target: 1,
                expected: 2
            })
        );
        assert_eq!(rollback_target(None, 1), None);
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
