//! The agent cost ledger on `SQLite` (issue #2275).
//!
//! The Postgres engine writes one `harvest_llm_ledger` row per LLM call of a
//! completed activity attempt (issue #1996). This module is the same ledger
//! for the embedded backend. The rows hold the model id, the tokens, the cost
//! and the latency of each call.
//!
//! An activity gives its calls to the runtime in one of two ways:
//! - an `#[activity]` body calls `ActivityContext::record_llm_call`;
//! - a closure that
//!   [`register_activity_with_ledger`](crate::SqliteRuntime::register_activity_with_ledger)
//!   registers returns its calls with its output.
//!
//! The runtime writes the rows in the transaction that appends the completion
//! event, `ActivityCompleted` or `LocalActivityCompleted`. The key is
//! `(exec_id, event_seq, call_index)`. `event_seq` is the `seq` of the
//! completion event. So the rows commit with the event or not at all. A
//! crash before the commit writes no row, and the re-run writes the rows of
//! its own attempt once. A failed or timed-out attempt writes no row, as on
//! Postgres.
//!
//! The table is outside `harvest_events`, so history and replay do not
//! change.

use std::time::Duration;

use autumn_harvest::ExecutionId;
use autumn_harvest::llm_ledger::{LlmCall, MAX_LATENCY_MS, MAX_LLM_CALLS_PER_ATTEMPT};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension as _, Row, params};

use crate::error::{SqliteError, SqliteResult};

/// One stored ledger row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LlmLedgerRow {
    /// The run that made the call.
    pub exec_id: ExecutionId,
    /// The `seq` of the completion event of the attempt in `harvest_events`.
    pub event_seq: i64,
    /// The position of the call in its attempt, from 0.
    pub call_index: u32,
    /// The registered name of the activity.
    pub activity_name: String,
    /// The model id.
    pub model: String,
    /// The input tokens.
    pub input_tokens: u64,
    /// The output tokens.
    pub output_tokens: u64,
    /// The cost in millionths of a US dollar. `None` means unpriced.
    pub cost_usd_micros: Option<u64>,
    /// The latency of the call in whole milliseconds.
    pub latency_ms: u64,
    /// The time of the completion, from the drive clock.
    pub recorded_at: DateTime<Utc>,
}

/// Fill each unset latency as `ActivityContext::record_llm_call` would.
///
/// A closure returns its calls when its body returns. So the first call gets
/// the run time of the attempt, if it has no latency, and each later call
/// gets zero. The context sets each latency at record time, so this changes
/// nothing for the calls of an `#[activity]` body.
pub fn fill_unset_latency(calls: Vec<LlmCall>, elapsed: Duration) -> Vec<LlmCall> {
    calls
        .into_iter()
        .enumerate()
        .map(|(index, call)| match call.latency() {
            Some(_) => call,
            None if index == 0 => call.with_latency(elapsed),
            None => call.with_latency(Duration::ZERO),
        })
        .collect()
}

/// Write one ledger row per call, keyed by the completion event `event_seq`.
///
/// Call it in the transaction that appends the completion event, after the
/// append. An empty slice writes nothing.
///
/// A call that fails its check is skipped with a warning, as on Postgres.
/// So is each call past [`MAX_LLM_CALLS_PER_ATTEMPT`]. The completion still
/// commits, because the model call is already paid for.
///
/// # Errors
///
/// Returns [`SqliteError::Sqlite`] if an insert fails.
pub fn insert_rows(
    conn: &Connection,
    exec_id: ExecutionId,
    event_seq: i64,
    activity_name: &str,
    calls: &[LlmCall],
    recorded_at_ms: i64,
) -> SqliteResult<()> {
    if calls.is_empty() {
        return Ok(());
    }
    if calls.len() > MAX_LLM_CALLS_PER_ATTEMPT {
        tracing::warn!(
            activity = activity_name,
            calls = calls.len(),
            max = MAX_LLM_CALLS_PER_ATTEMPT,
            "the LLM ledger keeps the first calls of an attempt only"
        );
    }
    let as_integer = |value: u64| i64::try_from(value).unwrap_or(i64::MAX);
    let mut insert = conn.prepare_cached(
        "INSERT INTO harvest_llm_ledger \
         (exec_id, event_seq, call_index, activity_name, model, input_tokens, \
          output_tokens, cost_usd_micros, latency_ms, recorded_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
    )?;
    let exec = exec_id.to_string();
    for (index, call) in calls.iter().enumerate().take(MAX_LLM_CALLS_PER_ATTEMPT) {
        if let Err(err) = call.validate() {
            tracing::warn!(error = %err, activity = activity_name, "skipping an invalid LLM ledger call");
            continue;
        }
        insert.execute(params![
            exec,
            event_seq,
            i64::try_from(index).unwrap_or(i64::MAX),
            activity_name,
            call.model(),
            as_integer(call.input_tokens()),
            as_integer(call.output_tokens()),
            call.cost_usd_micros().map(as_integer),
            call.latency().map_or(0, latency_ms),
            recorded_at_ms,
        ])?;
    }
    Ok(())
}

/// The latency in whole milliseconds, saturated to [`MAX_LATENCY_MS`].
fn latency_ms(latency: Duration) -> i64 {
    i64::try_from(latency.as_millis()).map_or(MAX_LATENCY_MS, |ms| ms.min(MAX_LATENCY_MS))
}

/// The columns of [`LlmLedgerRow`], in field order.
const COLUMNS: &str = "exec_id, event_seq, call_index, activity_name, model, input_tokens, \
                       output_tokens, cost_usd_micros, latency_ms, recorded_at";

/// The rows of `exec_id`, in completion order, then call order.
///
/// A file without the table has no rows. A read-only reader cannot add the
/// table to an older file.
///
/// # Errors
///
/// Returns [`SqliteError::Sqlite`] if the query fails. Returns
/// [`SqliteError::Corrupt`] for a row that does not parse.
pub fn rows_of(conn: &Connection, exec_id: ExecutionId) -> SqliteResult<Vec<LlmLedgerRow>> {
    if !table_exists(conn)? {
        return Ok(Vec::new());
    }
    let mut stmt = conn.prepare_cached(&format!(
        "SELECT {COLUMNS} FROM harvest_llm_ledger WHERE exec_id = ?1 \
         ORDER BY event_seq, call_index"
    ))?;
    let rows = stmt.query_map([exec_id.to_string()], raw_row)?;
    rows.map(|raw| parse(raw?)).collect()
}

/// The rows recorded at or after `from` and before `until`, oldest first.
///
/// # Errors
///
/// The errors of [`rows_of`].
pub fn rows_between(
    conn: &Connection,
    from: DateTime<Utc>,
    until: DateTime<Utc>,
) -> SqliteResult<Vec<LlmLedgerRow>> {
    if !table_exists(conn)? {
        return Ok(Vec::new());
    }
    let mut stmt = conn.prepare_cached(&format!(
        "SELECT {COLUMNS} FROM harvest_llm_ledger \
         WHERE recorded_at >= ?1 AND recorded_at < ?2 \
         ORDER BY recorded_at, exec_id, event_seq, call_index"
    ))?;
    let rows = stmt.query_map(
        params![from.timestamp_millis(), until.timestamp_millis()],
        raw_row,
    )?;
    rows.map(|raw| parse(raw?)).collect()
}

/// True if the file has the ledger table.
fn table_exists(conn: &Connection) -> SqliteResult<bool> {
    let found: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'harvest_llm_ledger'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    Ok(found.is_some())
}

/// One row as `SQLite` stores it.
struct RawRow {
    exec_id: String,
    event_seq: i64,
    call_index: i64,
    activity_name: String,
    model: String,
    input_tokens: i64,
    output_tokens: i64,
    cost_usd_micros: Option<i64>,
    latency_ms: i64,
    recorded_at: i64,
}

fn raw_row(row: &Row<'_>) -> rusqlite::Result<RawRow> {
    Ok(RawRow {
        exec_id: row.get(0)?,
        event_seq: row.get(1)?,
        call_index: row.get(2)?,
        activity_name: row.get(3)?,
        model: row.get(4)?,
        input_tokens: row.get(5)?,
        output_tokens: row.get(6)?,
        cost_usd_micros: row.get(7)?,
        latency_ms: row.get(8)?,
        recorded_at: row.get(9)?,
    })
}

fn parse(raw: RawRow) -> SqliteResult<LlmLedgerRow> {
    let corrupt = |field: &str| {
        SqliteError::Corrupt(format!(
            "harvest_llm_ledger.{field} of execution {} is out of range",
            raw.exec_id
        ))
    };
    let count = |value: i64, field: &str| u64::try_from(value).map_err(|_| corrupt(field));
    Ok(LlmLedgerRow {
        exec_id: raw.exec_id.parse().map_err(|_| corrupt("exec_id"))?,
        event_seq: raw.event_seq,
        call_index: u32::try_from(raw.call_index).map_err(|_| corrupt("call_index"))?,
        input_tokens: count(raw.input_tokens, "input_tokens")?,
        output_tokens: count(raw.output_tokens, "output_tokens")?,
        cost_usd_micros: raw
            .cost_usd_micros
            .map(|cost| count(cost, "cost_usd_micros"))
            .transpose()?,
        latency_ms: count(raw.latency_ms, "latency_ms")?,
        recorded_at: DateTime::from_timestamp_millis(raw.recorded_at)
            .ok_or_else(|| corrupt("recorded_at"))?,
        activity_name: raw.activity_name,
        model: raw.model,
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use autumn_harvest::ExecutionId;
    use autumn_harvest::llm_ledger::{
        LlmCall, MAX_COST_USD_MICROS, MAX_LATENCY_MS, MAX_LLM_CALLS_PER_ATTEMPT,
        MAX_MODEL_ID_BYTES, MAX_TOKENS_PER_CALL, MODEL_ID_PUNCTUATION,
    };
    use rusqlite::Connection;

    use super::{fill_unset_latency, insert_rows, rows_of};
    use crate::schema::{SCHEMA, migrate};

    fn open() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        migrate(&conn).unwrap();
        conn
    }

    #[test]
    fn the_rows_hold_each_call_in_order() {
        let conn = open();
        let exec = ExecutionId::new();
        let calls = [
            LlmCall::new("model-a", 10, 2)
                .with_cost_usd_micros(7)
                .with_latency(Duration::from_millis(1_500)),
            LlmCall::new("model-b", 20, 4).with_latency(Duration::from_micros(900)),
        ];
        insert_rows(&conn, exec, 5, "think", &calls, 1_700_000_000_123).unwrap();

        let rows = rows_of(&conn, exec).unwrap();
        assert_eq!(rows.len(), 2, "{rows:?}");
        assert_eq!(rows[0].exec_id, exec);
        assert_eq!(rows[0].event_seq, 5);
        assert_eq!(rows[0].call_index, 0);
        assert_eq!(rows[0].activity_name, "think");
        assert_eq!(rows[0].model, "model-a");
        assert_eq!((rows[0].input_tokens, rows[0].output_tokens), (10, 2));
        assert_eq!(rows[0].cost_usd_micros, Some(7));
        assert_eq!(rows[0].latency_ms, 1_500);
        assert_eq!(rows[0].recorded_at.timestamp_millis(), 1_700_000_000_123);
        assert_eq!(rows[1].call_index, 1);
        assert_eq!(rows[1].cost_usd_micros, None, "an unpriced call");
        assert_eq!(rows[1].latency_ms, 0, "a latency rounds down to whole ms");
    }

    #[test]
    fn an_invalid_call_is_skipped_and_keeps_its_index_free() {
        let conn = open();
        let exec = ExecutionId::new();
        let calls = [
            LlmCall::new("a b", 1, 1),
            LlmCall::new("good", 1, 1),
            LlmCall::new("m", MAX_TOKENS_PER_CALL + 1, 0),
        ];
        insert_rows(&conn, exec, 1, "think", &calls, 0).unwrap();

        let rows = rows_of(&conn, exec).unwrap();
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!((rows[0].model.as_str(), rows[0].call_index), ("good", 1));
    }

    #[test]
    fn the_rows_stop_at_the_cap_of_one_attempt() {
        let conn = open();
        let exec = ExecutionId::new();
        let calls = vec![LlmCall::new("m", 1, 1); MAX_LLM_CALLS_PER_ATTEMPT + 3];
        insert_rows(&conn, exec, 1, "think", &calls, 0).unwrap();
        assert_eq!(
            rows_of(&conn, exec).unwrap().len(),
            MAX_LLM_CALLS_PER_ATTEMPT
        );
    }

    #[test]
    fn a_huge_latency_saturates_to_the_ledger_limit() {
        let conn = open();
        let exec = ExecutionId::new();
        let calls = [LlmCall::new("m", 1, 1).with_latency(Duration::MAX)];
        insert_rows(&conn, exec, 1, "think", &calls, 0).unwrap();
        let rows = rows_of(&conn, exec).unwrap();
        assert_eq!(rows[0].latency_ms, MAX_LATENCY_MS.unsigned_abs());
    }

    #[test]
    fn the_first_unset_latency_takes_the_run_time_of_the_attempt() {
        let elapsed = Duration::from_millis(40);
        let set = Duration::from_millis(7);
        let calls = fill_unset_latency(
            vec![
                LlmCall::new("a", 1, 1),
                LlmCall::new("b", 1, 1).with_latency(set),
                LlmCall::new("c", 1, 1),
            ],
            elapsed,
        );
        let latencies: Vec<_> = calls.iter().map(LlmCall::latency).collect();
        assert_eq!(latencies, [Some(elapsed), Some(set), Some(Duration::ZERO)]);
    }

    // The table checks hold the same bounds as the core. A row that passes
    // `LlmCall::validate` therefore never fails its insert.
    #[test]
    fn the_table_checks_match_the_core_limits() {
        for check in [
            format!("length(CAST(model AS BLOB)) BETWEEN 1 AND {MAX_MODEL_ID_BYTES}"),
            format!("input_tokens BETWEEN 0 AND {MAX_TOKENS_PER_CALL}"),
            format!("output_tokens BETWEEN 0 AND {MAX_TOKENS_PER_CALL}"),
            format!("cost_usd_micros BETWEEN 0 AND {MAX_COST_USD_MICROS}"),
            format!("latency_ms BETWEEN 0 AND {MAX_LATENCY_MS}"),
            format!("call_index BETWEEN 0 AND {}", MAX_LLM_CALLS_PER_ATTEMPT - 1),
            format!("'*[^A-Za-z0-9{MODEL_ID_PUNCTUATION}]*'"),
        ] {
            assert!(SCHEMA.contains(&check), "SCHEMA must hold `{check}`");
        }
    }

    #[test]
    fn the_table_refuses_a_model_id_outside_the_token_charset() {
        let conn = open();
        let seq = std::cell::Cell::new(0_i64);
        let insert = |model: &str| {
            seq.set(seq.get() + 1);
            conn.execute(
                "INSERT INTO harvest_llm_ledger (exec_id, event_seq, call_index, activity_name, \
                 model, input_tokens, output_tokens, latency_ms, recorded_at) \
                 VALUES ('e', ?2, 0, 'a', ?1, 0, 0, 0, 0)",
                rusqlite::params![model, seq.get()],
            )
        };
        for model in ["a b", "modèle", "m\nx", "", "[x]", "*"] {
            assert!(insert(model).is_err(), "{model:?}");
        }
        for model in [
            "claude-sonnet-5-5",
            "models/gemini-2.0:latest",
            "org/llama+lora@v3",
        ] {
            insert(model).unwrap_or_else(|err| panic!("{model}: {err}"));
        }
    }
}
