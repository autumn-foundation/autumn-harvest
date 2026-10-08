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
//! workflow may set any key. Confine callers with the authorizer hook.

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

/// Delete the state of idle keys that the claim cannot tell from no state.
///
/// A row goes when all of these hold:
///
/// - No `PENDING` task of its queue has its key.
/// - Its `pass` is at most the queue clock `V`, so it holds no debt.
/// - Its `last_start` is below `V`, so it does not set `V`.
/// - No claim wrote it since `cutoff`.
///
/// Such a key starts at `V` with or without its row, so prune changes no
/// claim. The property test `prune_is_invisible_to_the_claim` proves that on
/// the model. Rows are locked `SKIP LOCKED`, so prune never waits for a claim.
/// With `preview`, nothing is deleted and the count is what a real pass would
/// delete. Returns the number of rows, at most `batch_size`. With
/// `queue_name`, only that queue is pruned.
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
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let batch = i64::try_from(batch_size).unwrap_or(i64::MAX).max(1);
    let sql = if preview {
        format!("WITH {PRUNE_VICTIMS_SQL} SELECT COUNT(*) AS n FROM victims")
    } else {
        format!(
            "WITH {PRUNE_VICTIMS_SQL}, gone AS ( \
                 DELETE FROM harvest_fairness_state d USING victims v \
                 WHERE d.queue_name = v.queue_name AND d.fairness_key = v.fairness_key \
                 RETURNING 1 \
             ) SELECT COUNT(*) AS n FROM gone"
        )
    };
    let count: Count = diesel::sql_query(sql)
        .bind::<diesel::sql_types::Timestamptz, _>(cutoff)
        .bind::<diesel::sql_types::BigInt, _>(batch)
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(queue_name)
        .get_result(conn)
        .await?;
    Ok(u64::try_from(count.n).unwrap_or(0))
}

/// The rows that [`prune_fairness_state`] may delete. Binds `$1` cutoff,
/// `$2` batch size and `$3` queue (`NULL` for every queue).
const PRUNE_VICTIMS_SQL: &str = "\
    clock AS ( \
        SELECT queue_name, MAX(last_start) AS v \
        FROM harvest_fairness_state \
        WHERE $3::TEXT IS NULL OR queue_name = $3 \
        GROUP BY queue_name \
    ), \
    victims AS ( \
        SELECT s.queue_name, s.fairness_key \
        FROM harvest_fairness_state s \
        JOIN clock c ON c.queue_name = s.queue_name \
        WHERE s.pass <= c.v \
          AND s.last_start < c.v \
          AND s.updated_at < $1 \
          AND NOT EXISTS ( \
              SELECT 1 FROM harvest_task_queue t \
              WHERE t.queue_name = s.queue_name \
                AND t.state = 'PENDING' \
                AND COALESCE(t.fairness_key, '') = s.fairness_key \
          ) \
        ORDER BY s.updated_at \
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
    fn prune_keeps_debt_and_the_clock_row() {
        assert!(PRUNE_VICTIMS_SQL.contains("s.pass <= c.v"));
        assert!(PRUNE_VICTIMS_SQL.contains("s.last_start < c.v"));
        assert!(PRUNE_VICTIMS_SQL.contains("SKIP LOCKED"));
        assert!(PRUNE_VICTIMS_SQL.contains("t.state = 'PENDING'"));
    }
}
