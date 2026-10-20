//! Runtime weight overrides and state upkeep for fairness keys (issue #1976).
//!
//! A worker with fairness keys on rotates its claims across the keys of a
//! queue in weighted round robin. See [`crate::queue_fairness::FairClock`] for
//! the rules and [`crate::queue::splice_fairness`] for the SQL.
//!
//! A key with no override has weight
//! [`crate::queue_fairness::DEFAULT_FAIRNESS_WEIGHT`]. An override applies at
//! the next fair claim of its key. No worker restart is needed.
//!
//! A fairness key bounds load, not access. Any caller that may start a
//! workflow may set any key. The authorizer hook does not see the key, so set
//! or strip it in your own service before a start reaches Harvest.
//!
//! Each shard keeps its own weights. These functions write the shard of the
//! connection that you pass. The admin HTTP routes write every shard.

use chrono::{DateTime, Utc};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

use crate::error::{HarvestError, HarvestResult};
use crate::queue_fairness::{
    MAX_FAIRNESS_OVERRIDES_PER_QUEUE, validate_fairness_key, validate_fairness_weight,
};

/// One weight override.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FairnessWeight {
    /// The queue of the key.
    pub queue_name: String,
    /// The fairness key.
    pub fairness_key: String,
    /// The weight. A key with weight `w` gets `w` claims for each claim of a
    /// key with weight `1`, while both have work.
    pub weight: f64,
    /// Who set the override.
    pub updated_by: String,
    /// When the override was set.
    pub updated_at: DateTime<Utc>,
}

/// The claim state of one key, for operators.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FairnessKeyState {
    /// The queue of the key.
    pub queue_name: String,
    /// The fairness key. The empty key holds rows with no key.
    pub fairness_key: String,
    /// The virtual time at which the key may start next.
    pub pass: f64,
    /// The start tag of the last claim of the key.
    pub last_start: f64,
    /// `max(pass, V) - V`. The claim serves keys with a smaller lag first.
    pub lag: f64,
    /// When a claim or prune last wrote the row.
    pub updated_at: DateTime<Utc>,
}

#[derive(diesel::QueryableByName)]
struct WeightRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    queue_name: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    fairness_key: String,
    #[diesel(sql_type = diesel::sql_types::Double)]
    weight: f64,
    #[diesel(sql_type = diesel::sql_types::Text)]
    updated_by: String,
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    updated_at: DateTime<Utc>,
}

impl From<WeightRow> for FairnessWeight {
    fn from(r: WeightRow) -> Self {
        Self {
            queue_name: r.queue_name,
            fairness_key: r.fairness_key,
            weight: r.weight,
            updated_by: r.updated_by,
            updated_at: r.updated_at,
        }
    }
}

/// Set the weight of `fairness_key` in `queue_name`.
///
/// A new override fails when the queue already holds
/// [`MAX_FAIRNESS_OVERRIDES_PER_QUEUE`] overrides. A change to an existing
/// override always succeeds. A queue-scoped advisory lock makes the count and
/// the insert one step, so two concurrent inserts cannot pass the cap.
///
/// # Errors
///
/// Returns [`HarvestError::Config`] for an invalid queue name, key or weight,
/// or when the queue is at the cap. Returns a database error on query failure.
pub async fn set_fairness_weight(
    conn: &mut AsyncPgConnection,
    queue_name: &str,
    fairness_key: &str,
    weight: f64,
    actor: &str,
) -> HarvestResult<FairnessWeight> {
    crate::queue_pause::validate_queue_name(queue_name)?;
    validate_fairness_key(fairness_key)?;
    let weight = validate_fairness_weight(weight)?;
    let queue = queue_name.to_owned();
    let key = fairness_key.to_owned();
    let actor = actor.to_owned();

    conn.transaction::<FairnessWeight, HarvestError, _>(async move |conn| {
        diesel::sql_query(
            "SELECT pg_advisory_xact_lock(hashtextextended('harvest_fairness_weights:' || $1, 0))",
        )
        .bind::<diesel::sql_types::Text, _>(&queue)
        .execute(conn)
        .await?;

        let row: WeightRow = diesel::sql_query(SET_WEIGHT_SQL)
            .bind::<diesel::sql_types::Text, _>(&queue)
            .bind::<diesel::sql_types::Text, _>(&key)
            .bind::<diesel::sql_types::Double, _>(weight)
            .bind::<diesel::sql_types::Text, _>(&actor)
            .bind::<diesel::sql_types::BigInt, _>(
                i64::try_from(MAX_FAIRNESS_OVERRIDES_PER_QUEUE).unwrap_or(i64::MAX),
            )
            .get_result(conn)
            .await
            .map_err(|e| match e {
                diesel::result::Error::NotFound => HarvestError::Config(format!(
                    "queue {queue:?} already holds {MAX_FAIRNESS_OVERRIDES_PER_QUEUE} \
                     fairness weight overrides; clear one first"
                )),
                other => HarvestError::from(other),
            })?;
        Ok(row.into())
    })
    .await
}

/// Upsert one override unless it is new and the queue is at the cap.
///
/// Binds: `$1` queue, `$2` key, `$3` weight, `$4` actor, `$5` cap. Returns no
/// row when the cap blocks a new key.
const SET_WEIGHT_SQL: &str = "\
    INSERT INTO harvest_fairness_weights \
        (queue_name, fairness_key, weight, updated_by, updated_at) \
    SELECT $1, $2, $3, $4, NOW() \
    WHERE EXISTS ( \
        SELECT 1 FROM harvest_fairness_weights \
        WHERE queue_name = $1 AND fairness_key = $2 \
    ) OR ( \
        SELECT COUNT(*) FROM harvest_fairness_weights WHERE queue_name = $1 \
    ) < $5 \
    ON CONFLICT (queue_name, fairness_key) DO UPDATE \
    SET weight = EXCLUDED.weight, updated_by = EXCLUDED.updated_by, \
        updated_at = EXCLUDED.updated_at \
    RETURNING queue_name, fairness_key, weight, updated_by, updated_at";

/// Remove the override of `fairness_key` in `queue_name`.
///
/// The key goes back to the default weight at its next claim. Returns `true`
/// when an override was removed.
///
/// # Errors
///
/// Returns a database error on query failure.
pub async fn clear_fairness_weight(
    conn: &mut AsyncPgConnection,
    queue_name: &str,
    fairness_key: &str,
) -> HarvestResult<bool> {
    let n = diesel::sql_query(
        "DELETE FROM harvest_fairness_weights WHERE queue_name = $1 AND fairness_key = $2",
    )
    .bind::<diesel::sql_types::Text, _>(queue_name)
    .bind::<diesel::sql_types::Text, _>(fairness_key)
    .execute(conn)
    .await?;
    Ok(n > 0)
}

/// Every override of `queue_name`, by key.
///
/// # Errors
///
/// Returns a database error on query failure.
pub async fn list_fairness_weights(
    conn: &mut AsyncPgConnection,
    queue_name: &str,
) -> HarvestResult<Vec<FairnessWeight>> {
    let rows: Vec<WeightRow> = diesel::sql_query(
        "SELECT queue_name, fairness_key, weight, updated_by, updated_at \
         FROM harvest_fairness_weights WHERE queue_name = $1 ORDER BY fairness_key",
    )
    .bind::<diesel::sql_types::Text, _>(queue_name)
    .load(conn)
    .await?;
    Ok(rows.into_iter().map(Into::into).collect())
}

/// The claim state of every key of `queue_name`, smallest lag first.
///
/// # Errors
///
/// Returns a database error on query failure.
pub async fn list_fairness_state(
    conn: &mut AsyncPgConnection,
    queue_name: &str,
) -> HarvestResult<Vec<FairnessKeyState>> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        queue_name: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        fairness_key: String,
        #[diesel(sql_type = diesel::sql_types::Double)]
        pass: f64,
        #[diesel(sql_type = diesel::sql_types::Double)]
        last_start: f64,
        #[diesel(sql_type = diesel::sql_types::Double)]
        lag: f64,
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        updated_at: DateTime<Utc>,
    }
    let rows: Vec<Row> = diesel::sql_query(
        "SELECT s.queue_name, s.fairness_key, s.pass, s.last_start, \
                GREATEST(s.pass - MAX(s.last_start) OVER (), 0) AS lag, s.updated_at \
         FROM harvest_fairness_state s WHERE s.queue_name = $1 \
         ORDER BY lag, s.fairness_key",
    )
    .bind::<diesel::sql_types::Text, _>(queue_name)
    .load(conn)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| FairnessKeyState {
            queue_name: r.queue_name,
            fairness_key: r.fairness_key,
            pass: r.pass,
            last_start: r.last_start,
            lag: r.lag,
            updated_at: r.updated_at,
        })
        .collect())
}

/// The most prune batches one call runs.
///
/// Key churn above one batch per tick still drains, and one call stays
/// bounded.
pub const MAX_PRUNE_BATCHES: usize = 10;

/// Delete fairness state that no claim needs.
///
/// A row goes when no claim wrote it since `cutoff` and one of two cases
/// holds:
///
/// - **An idle key.** No `PENDING` task of its queue has its key, and the row
///   is not the clock row. The clock row has the greatest
///   `(last_start, fairness_key)` of its queue, and it defines `V`. Prune
///   keeps it, so `V` never falls. The key restarts at `V` and loses its debt,
///   if any. That loss is at most `1 / weight`, one claim, at the weight of
///   its last claim. The property test
///   `prune_forgives_at_most_one_claim_of_debt` proves that on the model.
/// - **An idle queue.** The queue has no `PENDING` task, and no claim charged
///   any of its keys since `cutoff`. Its last row, the clock row, goes. This
///   is the idle rule of start-time fair queuing: with no backlog, every debt
///   is forgiven. See [`crate::queue_fairness::FairClock::reset_if_idle`].
///
/// Rows are locked `SKIP LOCKED`, so prune never waits for a claim.
///
/// Prune deletes in batches of `batch_size`, up to [`MAX_PRUNE_BATCHES`]
/// batches, and stops at the first short batch. With `preview`, nothing is
/// deleted, and the count is what one call would delete. A preview takes no
/// lock on the real rows. With `queue_name`, only that queue is pruned.
/// Returns the number of rows.
///
/// # Errors
///
/// Returns a database error on query failure.
pub async fn prune_fairness_state(
    conn: &mut AsyncPgConnection,
    queue_name: Option<&str>,
    cutoff: DateTime<Utc>,
    batch_size: usize,
    preview: bool,
) -> HarvestResult<u64> {
    let batch = i64::try_from(batch_size).unwrap_or(i64::MAX).max(1);
    let queue = queue_name.map(str::to_owned);
    if !preview {
        return Ok(delete_prune_batches(
            conn,
            STATE_TABLE,
            ACTIVE_LIVE,
            queue.as_deref(),
            cutoff,
            batch,
        )
        .await?);
    }
    // A preview runs the live batches on a temp copy of the state rows, then
    // rolls back. A one-pass count would miss rows that qualify only in a
    // later batch, such as the last row of an idle queue. The copy keeps the
    // real rows unlocked, so a claim never waits for a preview.
    let ended = conn
        .transaction::<(), PreviewEnd, _>(async move |conn| {
            diesel::sql_query(format!(
                "CREATE TEMP TABLE {PREVIEW_TABLE} \
                 (LIKE {STATE_TABLE} INCLUDING INDEXES) ON COMMIT DROP"
            ))
            .execute(conn)
            .await?;
            diesel::sql_query(format!(
                "CREATE TEMP TABLE {PREVIEW_ACTIVE} \
                 (queue_name TEXT NOT NULL, fairness_key TEXT NOT NULL) ON COMMIT DROP"
            ))
            .execute(conn)
            .await?;
            // One statement copies both inputs, so they share one snapshot at
            // any isolation level and nesting depth. The count then equals a
            // live prune at one instant.
            diesel::sql_query(format!(
                "WITH cp AS ( \
                     INSERT INTO {PREVIEW_TABLE} SELECT * FROM {STATE_TABLE} \
                     WHERE $1::TEXT IS NULL OR queue_name = $1 \
                 ) \
                 INSERT INTO {PREVIEW_ACTIVE} {ACTIVE_LIVE_SQL}"
            ))
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(queue.as_deref())
            .execute(conn)
            .await?;
            diesel::sql_query(format!("ANALYZE {PREVIEW_TABLE}"))
                .execute(conn)
                .await?;
            let n = delete_prune_batches(
                conn,
                PREVIEW_TABLE,
                PREVIEW_ACTIVE,
                queue.as_deref(),
                cutoff,
                batch,
            )
            .await?;
            Err(PreviewEnd::Count(n))
        })
        .await;
    match ended {
        Err(PreviewEnd::Count(n)) => Ok(n),
        Err(PreviewEnd::Failed(e)) => Err(e.into()),
        Ok(()) => Ok(0),
    }
}

/// The state table.
const STATE_TABLE: &str = "harvest_fairness_state";

/// The pending keys that the live prune reads. A subquery over the task
/// table, limited to the queues in `clock`.
const ACTIVE_LIVE: &str = "( \
    SELECT DISTINCT t.queue_name, COALESCE(t.fairness_key, '') AS fairness_key \
    FROM harvest_task_queue t \
    WHERE t.state = 'PENDING' \
      AND t.queue_name IN (SELECT queue_name FROM clock) \
)";

/// The pending keys of a preview: a one-statement copy of the task table.
/// `$1` is the queue, or `NULL` for every queue.
const ACTIVE_LIVE_SQL: &str = "\
    SELECT DISTINCT t.queue_name, COALESCE(t.fairness_key, '') \
    FROM harvest_task_queue t \
    WHERE t.state = 'PENDING' AND ($1::TEXT IS NULL OR t.queue_name = $1)";

/// The temp copy of the pending keys that a preview reads.
const PREVIEW_ACTIVE: &str = "harvest_fairness_preview_active";

/// The temp copy that a preview prunes.
const PREVIEW_TABLE: &str = "harvest_fairness_preview";

/// How a preview transaction ends. It always rolls back, so the count
/// leaves the transaction as its error.
enum PreviewEnd {
    Count(u64),
    Failed(diesel::result::Error),
}

impl From<diesel::result::Error> for PreviewEnd {
    fn from(e: diesel::result::Error) -> Self {
        Self::Failed(e)
    }
}

/// Delete prune victims from `state` in batches of `batch` rows, up to
/// [`MAX_PRUNE_BATCHES`] batches. Stops at the first short batch. Returns
/// the number of rows deleted.
async fn delete_prune_batches(
    conn: &mut AsyncPgConnection,
    state: &str,
    active: &str,
    queue_name: Option<&str>,
    cutoff: DateTime<Utc>,
    batch: i64,
) -> Result<u64, diesel::result::Error> {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let victims = PRUNE_VICTIMS_SQL
        .replace("@state@", state)
        .replace("@active@", active);
    let sql = format!(
        "WITH {victims}, gone AS ( \
             DELETE FROM {state} d USING victims v \
             WHERE d.queue_name = v.queue_name AND d.fairness_key = v.fairness_key \
             RETURNING 1 \
         ) SELECT COUNT(*) AS n FROM gone"
    );
    let mut total = 0u64;
    for _ in 0..MAX_PRUNE_BATCHES {
        let count: Count = diesel::sql_query(&sql)
            .bind::<diesel::sql_types::Timestamptz, _>(cutoff)
            .bind::<diesel::sql_types::BigInt, _>(batch)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(queue_name)
            .get_result(conn)
            .await?;
        total += u64::try_from(count.n).unwrap_or(0);
        if count.n < batch {
            break;
        }
    }
    Ok(total)
}

/// The rows that [`prune_fairness_state`] may delete. Binds `$1` cutoff,
/// `$2` row limit and `$3` queue (`NULL` for every queue). `@state@` stands
/// for the state table and `@active@` for the pending keys.
///
/// A row goes in one of two cases:
///
/// - Its key has no pending task, and it is not the clock row. The clock row
///   has the greatest `(last_start, fairness_key)` of its queue. Concurrent
///   pruners thus keep the same row, and `V` never falls. The key loses its
///   debt, at most `1 / weight`.
/// - It is the last row of an idle queue: no pending task and no charge since
///   the cutoff. With no backlog, no key is behind another, so every debt is
///   forgiven.
///
/// A row that another pruner holds still counts as a row of the queue. A
/// partial or rolled-back reset thus never lowers `V`.
///
/// `active` reads the pending keys of the pruned queues once. The planner
/// can then hash it for the anti-join. A probe per state row would scan the
/// pending backlog once per row.
const PRUNE_VICTIMS_SQL: &str = "\
    clock AS ( \
        SELECT g.queue_name, g.last_charge, k.fairness_key AS clock_key \
        FROM ( \
            SELECT queue_name, MAX(updated_at) AS last_charge \
            FROM @state@ \
            WHERE $3::TEXT IS NULL OR queue_name = $3 \
            GROUP BY queue_name \
        ) g \
        CROSS JOIN LATERAL ( \
            SELECT c.fairness_key FROM @state@ c \
            WHERE c.queue_name = g.queue_name \
            ORDER BY c.last_start DESC, c.fairness_key DESC \
            LIMIT 1 \
        ) k \
    ), \
    active AS MATERIALIZED ( \
        SELECT x.queue_name, x.fairness_key FROM @active@ x \
    ), \
    victims AS ( \
        SELECT s.queue_name, s.fairness_key \
        FROM @state@ s \
        JOIN clock c ON c.queue_name = s.queue_name \
        WHERE s.updated_at < $1 \
          AND ( \
              ( \
                  s.fairness_key <> c.clock_key \
                  AND NOT EXISTS ( \
                      SELECT 1 FROM active a \
                      WHERE a.queue_name = s.queue_name \
                        AND a.fairness_key = s.fairness_key \
                  ) \
              ) \
              OR ( \
                  c.last_charge < $1 \
                  AND NOT EXISTS ( \
                      SELECT 1 FROM active a WHERE a.queue_name = s.queue_name \
                  ) \
                  AND NOT EXISTS ( \
                      SELECT 1 FROM @state@ o \
                      WHERE o.queue_name = s.queue_name \
                        AND o.fairness_key <> s.fairness_key \
                  ) \
              ) \
          ) \
        ORDER BY s.last_start, s.updated_at \
        LIMIT $2 \
        FOR UPDATE OF s SKIP LOCKED \
    )";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_weight_sql_caps_only_new_keys() {
        assert!(SET_WEIGHT_SQL.contains("WHERE EXISTS"));
        assert!(SET_WEIGHT_SQL.contains("< $5"));
        assert!(SET_WEIGHT_SQL.contains("ON CONFLICT (queue_name, fairness_key) DO UPDATE"));
    }

    #[test]
    fn prune_keeps_the_clock_row_and_active_keys() {
        assert!(PRUNE_VICTIMS_SQL.contains("s.fairness_key <> c.clock_key"));
        assert!(PRUNE_VICTIMS_SQL.contains("ORDER BY c.last_start DESC, c.fairness_key DESC"));
        assert!(PRUNE_VICTIMS_SQL.contains("SKIP LOCKED"));
        assert!(ACTIVE_LIVE.contains("t.state = 'PENDING'"));
        assert!(ACTIVE_LIVE_SQL.contains("t.state = 'PENDING'"));
        assert!(PRUNE_VICTIMS_SQL.contains("active AS MATERIALIZED"));
    }

    #[test]
    fn prune_resets_an_idle_queue_at_its_last_row() {
        assert!(PRUNE_VICTIMS_SQL.contains("c.last_charge < $1"));
        assert!(
            PRUNE_VICTIMS_SQL.contains("SELECT 1 FROM active a WHERE a.queue_name = s.queue_name")
        );
        // A row that another pruner holds still counts.
        assert!(PRUNE_VICTIMS_SQL.contains("o.fairness_key <> s.fairness_key"));
    }

    #[test]
    fn prune_reads_the_state_table_only_through_the_placeholder() {
        assert!(!PRUNE_VICTIMS_SQL.contains("harvest_fairness_state"));
        assert!(!PRUNE_VICTIMS_SQL.contains("harvest_task_queue"));
    }
}
