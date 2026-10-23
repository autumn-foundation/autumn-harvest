//! The keyed-entity state projection (issue #2259).
//!
//! The entity loop records each state change as a history event. The event is
//! `SideEffectRecorded` with kind [`SideEffectKind::EntityState`]. The trigger
//! [`PROJECTION_TRIGGER`] copies its value into [`PROJECTION_TABLE`] in the
//! transaction that inserts the event. So the projection and history commit
//! together, on every insert path: a decision, a reset, a fork and a shard
//! move. See `docs/adr/0006-keyed-entity.md`.
//!
//! The table holds one row for each `(workflow_name, workflow_id)`. A row of
//! the same run moves forward by event id. A row of another run moves when
//! the new `harvest_events.id` is larger.
//!
//! The table is not `harvest_events`, so it is not an append-only exception.
//! Codec rotation sweeps its `snapshot` column, and erasure tombstones it.
//!
//! [`SideEffectKind::EntityState`]: crate::event::SideEffectKind::EntityState

#[cfg(feature = "db")]
use chrono::{DateTime, Utc};
#[cfg(feature = "db")]
use diesel::sql_types::{Bool, Integer, Jsonb, Text, Timestamptz, Uuid as SqlUuid};
#[cfg(feature = "db")]
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde_json::Value;

#[cfg(feature = "db")]
use crate::entity::ENTITY_SNAPSHOT_OMITTED_KEY;
use crate::entity::{ENTITY_STATE_QUERY, ENTITY_STATS_QUERY, EntitySnapshot};
#[cfg(feature = "db")]
use crate::error::{HarvestResult, database_error};
#[cfg(feature = "db")]
use crate::payload_codec::PayloadCodecs;
#[cfg(feature = "db")]
use crate::types::ExecutionId;

/// The projection table.
pub const PROJECTION_TABLE: &str = "harvest_entity_state";

/// The trigger on `harvest_events` that writes [`PROJECTION_TABLE`].
pub const PROJECTION_TRIGGER: &str = "harvest_entity_state_projection_trg";

/// The function of [`PROJECTION_TRIGGER`].
pub const PROJECTION_FUNCTION: &str = "harvest_entity_state_project";

/// [`PROJECTION_TRIGGER`]'s `pg_trigger.tgtype` bitmask: `TRIGGER_TYPE_ROW` (1)
/// | `TRIGGER_TYPE_INSERT` (4). An `AFTER` trigger sets no timing bit.
pub(crate) const PROJECTION_TRIGGER_TGTYPE: i16 = 5;

/// The `CREATE TRIGGER` statement that installs [`PROJECTION_TRIGGER`].
///
/// The partition layout changes build new relations with `LIKE`, which copies
/// no triggers. Each change runs this statement next to the append-only
/// guard. The `WHEN` clause has quoted literals, so a PL/pgSQL block embeds it
/// in a dollar-quoted string.
#[must_use]
pub(crate) fn create_projection_trigger_sql() -> String {
    format!(
        "CREATE TRIGGER {PROJECTION_TRIGGER} AFTER INSERT ON harvest_events \
         FOR EACH ROW \
         WHEN (NEW.event_type = 'SideEffectRecorded' \
         AND (NEW.event_data -> 'data' ->> 'kind') = 'EntityState') \
         EXECUTE FUNCTION {PROJECTION_FUNCTION}()"
    )
}

/// What a projection row holds.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProjectedSnapshot {
    /// The decoded snapshot.
    Current(EntitySnapshot<Value>),
    /// An erasure replaced the snapshot with a tombstone.
    Erased,
    /// The payload store holds the snapshot. A read must replay the run.
    Offloaded,
    /// The loop did not store the state, because it was past a payload cap.
    /// A read must replay the run.
    Omitted,
}

impl ProjectedSnapshot {
    /// The answer to the entity query `query_name`.
    ///
    /// Returns `None` for any other query, and for a snapshot that is not
    /// [`Current`](Self::Current).
    #[must_use]
    pub fn query_answer(&self, query_name: &str) -> Option<Value> {
        let Self::Current(snapshot) = self else {
            return None;
        };
        match query_name {
            ENTITY_STATE_QUERY => Some(snapshot.state.clone()),
            ENTITY_STATS_QUERY => serde_json::to_value(&snapshot.stats).ok(),
            _ => None,
        }
    }
}

/// Whether `query_name` is an entity query that the projection can answer.
#[must_use]
pub fn is_projected_query(query_name: &str) -> bool {
    matches!(query_name, ENTITY_STATE_QUERY | ENTITY_STATS_QUERY)
}

/// Decode one stored `snapshot` cell.
///
/// # Errors
///
/// A codec error, or a snapshot that is not `{"state", "stats"}`.
#[cfg(feature = "db")]
fn decode_snapshot(codecs: &PayloadCodecs, stored: &Value) -> HarvestResult<ProjectedSnapshot> {
    if crate::erase::is_erasure_tombstone(stored) {
        return Ok(ProjectedSnapshot::Erased);
    }
    if crate::payload_store::is_offload_envelope(stored) {
        return Ok(ProjectedSnapshot::Offloaded);
    }
    let decoded = codecs.decode_column(stored)?;
    if decoded.get(ENTITY_SNAPSHOT_OMITTED_KEY).is_some() {
        return Ok(ProjectedSnapshot::Omitted);
    }
    Ok(ProjectedSnapshot::Current(serde_json::from_value(decoded)?))
}

/// One row of [`PROJECTION_TABLE`].
#[cfg(feature = "db")]
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct EntityStateRow {
    /// The entity type.
    pub workflow_name: String,
    /// The entity key.
    pub workflow_id: String,
    /// The run that wrote the row.
    pub execution_id: ExecutionId,
    /// The history position of the snapshot in that run.
    pub event_id: i32,
    /// The snapshot ended the entity.
    pub deleted: bool,
    /// The snapshot.
    pub snapshot: ProjectedSnapshot,
    /// The time of the write.
    pub updated_at: DateTime<Utc>,
}

#[cfg(feature = "db")]
#[derive(diesel::QueryableByName)]
struct RawRow {
    #[diesel(sql_type = Text)]
    workflow_name: String,
    #[diesel(sql_type = Text)]
    workflow_id: String,
    #[diesel(sql_type = SqlUuid)]
    execution_id: uuid::Uuid,
    #[diesel(sql_type = Integer)]
    event_id: i32,
    #[diesel(sql_type = Bool)]
    deleted: bool,
    #[diesel(sql_type = Jsonb)]
    snapshot: Value,
    #[diesel(sql_type = Timestamptz)]
    updated_at: DateTime<Utc>,
}

#[cfg(feature = "db")]
impl RawRow {
    fn decode(self, codecs: &PayloadCodecs) -> HarvestResult<EntityStateRow> {
        Ok(EntityStateRow {
            snapshot: decode_snapshot(codecs, &self.snapshot)?,
            workflow_name: self.workflow_name,
            workflow_id: self.workflow_id,
            execution_id: ExecutionId::from_uuid(self.execution_id),
            event_id: self.event_id,
            deleted: self.deleted,
            updated_at: self.updated_at,
        })
    }
}

#[cfg(feature = "db")]
const SELECT_ROW: &str = "SELECT workflow_name, workflow_id, execution_id, event_id, deleted, \
                          snapshot, updated_at FROM harvest_entity_state";

/// Read the projection row of the entity `(workflow_name, workflow_id)`.
///
/// Reads one row by its unique key. Replays nothing.
///
/// # Errors
///
/// [`crate::error::HarvestError::Database`] on a query failure, or a codec
/// error on a snapshot that does not decode.
#[cfg(feature = "db")]
pub async fn read(
    conn: &mut AsyncPgConnection,
    codecs: &PayloadCodecs,
    workflow_name: &str,
    workflow_id: &str,
) -> HarvestResult<Option<EntityStateRow>> {
    let rows: Vec<RawRow> = diesel::sql_query(format!(
        "{SELECT_ROW} WHERE workflow_name = $1 AND workflow_id = $2"
    ))
    .bind::<Text, _>(workflow_name)
    .bind::<Text, _>(workflow_id)
    .load(conn)
    .await
    .map_err(database_error)?;
    rows.into_iter()
        .next()
        .map(|row| row.decode(codecs))
        .transpose()
}

/// Read the projection row that the run `exec_id` wrote last.
///
/// Returns `None` when another run of the key wrote the row since, or when
/// the run wrote no snapshot.
///
/// # Errors
///
/// As [`read`].
#[cfg(feature = "db")]
pub async fn read_for_execution(
    conn: &mut AsyncPgConnection,
    codecs: &PayloadCodecs,
    exec_id: ExecutionId,
) -> HarvestResult<Option<EntityStateRow>> {
    let rows: Vec<RawRow> = diesel::sql_query(format!("{SELECT_ROW} WHERE execution_id = $1"))
        .bind::<SqlUuid, _>(exec_id.as_uuid())
        .load(conn)
        .await
        .map_err(database_error)?;
    rows.into_iter()
        .next()
        .map(|row| row.decode(codecs))
        .transpose()
}

/// The largest page that [`list`] returns.
pub const MAX_LIST_LIMIT: u32 = 1000;

/// List the entities of type `workflow_name`, in key order.
///
/// `after` is the last key of the previous page, or `None` for the first
/// page. A page holds at most `limit` rows, capped at [`MAX_LIST_LIMIT`]. A
/// deleted entity is left out unless `include_deleted` is set. The unique key
/// index serves the scan.
///
/// # Errors
///
/// As [`read`].
#[cfg(feature = "db")]
pub async fn list(
    conn: &mut AsyncPgConnection,
    codecs: &PayloadCodecs,
    workflow_name: &str,
    after: Option<&str>,
    limit: u32,
    include_deleted: bool,
) -> HarvestResult<Vec<EntityStateRow>> {
    let rows: Vec<RawRow> = diesel::sql_query(format!(
        "{SELECT_ROW} WHERE workflow_name = $1 AND ($2::text IS NULL OR workflow_id > $2) \
         AND ($3 OR NOT deleted) ORDER BY workflow_id LIMIT $4"
    ))
    .bind::<Text, _>(workflow_name)
    .bind::<diesel::sql_types::Nullable<Text>, _>(after)
    .bind::<Bool, _>(include_deleted)
    .bind::<diesel::sql_types::BigInt, _>(i64::from(limit.min(MAX_LIST_LIMIT)))
    .load(conn)
    .await
    .map_err(database_error)?;
    rows.into_iter().map(|row| row.decode(codecs)).collect()
}

/// Answer the entity query `query_name` on the run `exec_id` from the
/// projection.
///
/// Returns `None` when the query is not an entity query, when the run has no
/// row, or when the row holds no readable snapshot. The caller then replays
/// the run.
///
/// # Errors
///
/// As [`read`].
#[cfg(feature = "db")]
pub async fn answer_query(
    conn: &mut AsyncPgConnection,
    codecs: &PayloadCodecs,
    exec_id: ExecutionId,
    query_name: &str,
) -> HarvestResult<Option<Value>> {
    if !is_projected_query(query_name) {
        return Ok(None);
    }
    Ok(read_for_execution(conn, codecs, exec_id)
        .await?
        .and_then(|row| row.snapshot.query_answer(query_name)))
}

/// Tombstone the snapshot of each row that the run `exec_id` wrote.
///
/// Erasure calls it in its own transaction. Returns the count of rows that
/// changed, so a second call returns 0.
///
/// # Errors
///
/// [`crate::error::HarvestError::Database`] on a query failure.
#[cfg(feature = "db")]
pub(crate) async fn erase_for_execution(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> HarvestResult<usize> {
    diesel::sql_query(
        "UPDATE harvest_entity_state SET snapshot = $2 \
         WHERE execution_id = $1 AND snapshot IS DISTINCT FROM $2",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .bind::<Jsonb, _>(crate::erase::erasure_tombstone())
    .execute(conn)
    .await
    .map_err(database_error)
}

/// Give the row of the key that `exec_id` holds back to the other runs of
/// the key (issue #2259).
///
/// A shard move that aborts deletes the staged events of `exec_id`. Those
/// events moved the row of the key to `exec_id`. This call deletes that row
/// and rebuilds it from the latest snapshot event of the other runs of the
/// key on this database. With no such event, the key has no row. Call it
/// before the staged events go, in the same transaction.
///
/// # Errors
///
/// [`crate::error::HarvestError::Database`] on a query failure.
#[cfg(feature = "db")]
pub async fn rebuild_without_execution(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> HarvestResult<()> {
    #[derive(diesel::QueryableByName)]
    struct Key {
        #[diesel(sql_type = Text)]
        workflow_name: String,
        #[diesel(sql_type = Text)]
        workflow_id: String,
    }
    let keys: Vec<Key> = diesel::sql_query(
        "DELETE FROM harvest_entity_state WHERE execution_id = $1 \
         RETURNING workflow_name, workflow_id",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .load(conn)
    .await
    .map_err(database_error)?;
    for key in keys {
        diesel::sql_query(
            "INSERT INTO harvest_entity_state \
                 (workflow_name, workflow_id, execution_id, event_id, event_row_id, \
                  deleted, snapshot) \
             SELECT x.workflow_name, x.workflow_id, e.workflow_exec_id, e.event_id, e.id, \
                    coalesce((e.event_data -> 'data' ->> 'name') = $4, false), \
                    coalesce(e.event_data -> 'data' -> 'value', 'null'::jsonb) \
               FROM harvest_workflow_executions x \
               JOIN harvest_events e ON e.workflow_exec_id = x.id \
              WHERE x.workflow_name = $1 AND x.workflow_id = $2 AND x.id <> $3 \
                AND e.event_type = 'SideEffectRecorded' \
                AND (e.event_data -> 'data' ->> 'kind') = 'EntityState' \
              ORDER BY e.id DESC LIMIT 1",
        )
        .bind::<Text, _>(&key.workflow_name)
        .bind::<Text, _>(&key.workflow_id)
        .bind::<SqlUuid, _>(exec_id.as_uuid())
        .bind::<Text, _>(crate::entity::ENTITY_DELETED_SIDE_EFFECT)
        .execute(conn)
        .await
        .map_err(database_error)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::entity::EntityStats;

    /// The migration and the partition paths must install the same trigger.
    #[test]
    fn the_migration_installs_the_projection_trigger() {
        let up = include_str!("../migrations/20261023015028_harvest_entity_state/up.sql");
        let squashed = up.split_whitespace().collect::<Vec<_>>().join(" ");
        let trigger = create_projection_trigger_sql()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        assert!(squashed.contains(&trigger), "{trigger}");
        for name in [PROJECTION_TABLE, PROJECTION_FUNCTION] {
            assert!(up.contains(name), "the migration must use {name}");
        }
        assert!(
            up.contains(&format!("'{}'", crate::entity::ENTITY_DELETED_SIDE_EFFECT)),
            "the trigger reads the deleted name"
        );
        let kind = serde_json::to_value(crate::event::SideEffectKind::EntityState).unwrap();
        assert!(
            up.contains(&format!("'{}'", kind.as_str().unwrap())),
            "the trigger selects the serialized kind"
        );
    }

    #[test]
    fn the_projection_trigger_has_no_dollar_quote() {
        assert!(!create_projection_trigger_sql().contains('$'));
    }

    #[test]
    fn a_current_snapshot_answers_both_entity_queries() {
        let stats = EntityStats {
            applied: 2,
            ..EntityStats::default()
        };
        let snapshot = ProjectedSnapshot::Current(EntitySnapshot {
            state: json!({"n": 3}),
            stats: stats.clone(),
        });
        assert_eq!(
            snapshot.query_answer(ENTITY_STATE_QUERY),
            Some(json!({"n": 3}))
        );
        assert_eq!(
            snapshot.query_answer(ENTITY_STATS_QUERY),
            Some(serde_json::to_value(stats).unwrap())
        );
        assert_eq!(snapshot.query_answer("other"), None);
        assert_eq!(
            ProjectedSnapshot::Erased.query_answer(ENTITY_STATE_QUERY),
            None
        );
        assert_eq!(
            ProjectedSnapshot::Offloaded.query_answer(ENTITY_STATE_QUERY),
            None
        );
        assert_eq!(
            ProjectedSnapshot::Omitted.query_answer(ENTITY_STATE_QUERY),
            None
        );
        assert!(is_projected_query(ENTITY_STATE_QUERY));
        assert!(!is_projected_query("progress"));
    }

    /// Each stored cell shape decodes to its variant. Only a full snapshot
    /// answers a query.
    #[cfg(feature = "db")]
    #[test]
    fn each_stored_cell_decodes_to_its_variant() {
        let codecs = PayloadCodecs::default();
        let decode = |cell: Value| decode_snapshot(&codecs, &cell).unwrap();
        assert_eq!(
            decode(crate::erase::erasure_tombstone()),
            ProjectedSnapshot::Erased
        );
        assert_eq!(
            decode(json!({ crate::payload_store::OFFLOAD_ENVELOPE_KEY: 1, "ref": "x" })),
            ProjectedSnapshot::Offloaded
        );
        assert_eq!(
            decode(json!({ ENTITY_SNAPSHOT_OMITTED_KEY: "too_large", "stats": {} })),
            ProjectedSnapshot::Omitted
        );
        let current = decode(json!({"state": [1], "stats": {"applied": 1}}));
        assert_eq!(current.query_answer(ENTITY_STATE_QUERY), Some(json!([1])));
        assert!(decode_snapshot(&codecs, &json!({"stats": {}})).is_err());
    }
}
