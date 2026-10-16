//! Cron and interval schedules (issue #2031).
//!
//! A schedule is one row of `harvest_schedules`, keyed by its workflow name,
//! as on Postgres. Each `poll_once` pass fires the rows whose slot is due.
//! [`SqliteRuntime`](crate::SqliteRuntime) starts the run. This module owns
//! the rows and the slot rules.
//!
//! The slot arithmetic is the core [`schedule_plan`], which the Postgres
//! scheduler also calls. So both backends compute the same slots.
//!
//! Catch-up is off: a due row fires one run for its oldest slot. The next
//! slot then counts from that slot, or from now after a long gap.
//!
//! The backend accepts the core [`WorkflowSchedule`]. It rejects, by name,
//! each field it cannot honor. It ignores `jitter`: jitter spreads the fires
//! of a fleet, and one writer has no fleet. Retry jitter has the same rule.

use autumn_harvest::policy::{CatchupPolicy, OverlapPolicy, Schedule, WorkflowSchedule};
use autumn_harvest::schedule_plan;
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;

use crate::error::{SqliteError, SqliteResult};

/// One stored schedule, as [`SqliteRuntime::list_schedules`](crate::SqliteRuntime::list_schedules)
/// returns it.
///
/// It is `non_exhaustive`, so a later field is not a breaking change. It has
/// no `PartialEq`, because [`Schedule`] has none.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ScheduleSummary {
    /// The workflow that each fire starts. It is also the schedule key.
    pub workflow_name: String,
    /// The cron or interval spec.
    pub schedule: Schedule,
    /// `true` while the schedule is paused.
    pub paused: bool,
    /// The next slot. `None` never fires.
    pub next_run_at: Option<DateTime<Utc>>,
    /// The slot of the last started run.
    pub last_run_at: Option<DateTime<Utc>>,
}

/// A due schedule, read inside its fire transaction.
pub struct Due {
    /// The schedule key and the workflow to start.
    pub workflow_name: String,
    /// The input of the run.
    pub input: Value,
    /// `true` for `allow_all`. Then the run limit does not apply.
    pub allow_all: bool,
    /// The run limit of `skip`.
    pub max_active_runs: i64,
    /// The due slot in epoch milliseconds.
    pub slot: i64,
    schedule: Schedule,
}

/// Insert or update `schedule` at `now`, in epoch milliseconds.
///
/// A new row takes its first slot after `now`, and its `paused` flag from
/// `schedule`. An update keeps the stored pause, so an app can upsert on
/// every open. An update with the same spec also keeps the next slot, so a
/// reopen never moves a slot. A changed spec counts its next slot from `now`.
pub fn upsert(conn: &Connection, schedule: &WorkflowSchedule, now: i64) -> SqliteResult<()> {
    // After this check the overlap policy is `skip` or `allow_all`.
    check_supported(schedule)?;
    autumn_harvest::policy::validate_schedule(&schedule.schedule)
        .map_err(SqliteError::InvalidSchedule)?;
    let spec = serde_json::to_string(&schedule.schedule)?;
    let tx = conn.unchecked_transaction()?;
    let stored: Option<(String, Option<i64>)> = tx
        .query_row(
            "SELECT schedule_json, next_run_at FROM harvest_schedules WHERE workflow_name = ?1",
            [&schedule.workflow_name],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let next_run_at = match stored {
        Some((old_spec, next)) if old_spec == spec => next,
        _ => first_slot(&schedule.schedule, now),
    };
    tx.execute(
        "INSERT INTO harvest_schedules (workflow_name, schedule_json, input_json, \
           overlap_policy, max_active_runs, paused, next_run_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
         ON CONFLICT (workflow_name) DO UPDATE SET \
           schedule_json = excluded.schedule_json, input_json = excluded.input_json, \
           overlap_policy = excluded.overlap_policy, \
           max_active_runs = excluded.max_active_runs, next_run_at = excluded.next_run_at",
        params![
            schedule.workflow_name,
            spec,
            serde_json::to_string(&schedule.input)?,
            schedule.overlap_policy.as_str(),
            i64::from(schedule.max_active_runs),
            schedule.paused,
            next_run_at,
        ],
    )?;
    tx.commit()?;
    Ok(())
}

/// Remove the schedule of `workflow_name`. Its runs keep running.
pub fn delete(conn: &Connection, workflow_name: &str) -> SqliteResult<()> {
    let removed = conn.execute(
        "DELETE FROM harvest_schedules WHERE workflow_name = ?1",
        [workflow_name],
    )?;
    if removed == 0 {
        return Err(SqliteError::ScheduleNotFound(workflow_name.to_string()));
    }
    Ok(())
}

/// Pause or resume the schedule of `workflow_name`.
///
/// A resume counts the next slot from `now`, so the slots missed while
/// paused do not fire. A resume of a running schedule changes nothing.
pub fn set_paused(
    conn: &Connection,
    workflow_name: &str,
    paused: bool,
    now: i64,
) -> SqliteResult<()> {
    let tx = conn.unchecked_transaction()?;
    let Some((spec, was_paused)) = tx
        .query_row(
            "SELECT schedule_json, paused FROM harvest_schedules WHERE workflow_name = ?1",
            [workflow_name],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?)),
        )
        .optional()?
    else {
        return Err(SqliteError::ScheduleNotFound(workflow_name.to_string()));
    };
    if paused {
        tx.execute(
            "UPDATE harvest_schedules SET paused = 1 WHERE workflow_name = ?1",
            [workflow_name],
        )?;
    } else if was_paused {
        let next = first_slot(&parse_spec(&spec)?, now);
        tx.execute(
            "UPDATE harvest_schedules SET paused = 0, next_run_at = ?2 WHERE workflow_name = ?1",
            params![workflow_name, next],
        )?;
    }
    tx.commit()?;
    Ok(())
}

/// Every stored schedule, by workflow name.
pub fn list(conn: &Connection) -> SqliteResult<Vec<ScheduleSummary>> {
    let mut stmt = conn.prepare(
        "SELECT workflow_name, schedule_json, paused, next_run_at, last_run_at \
         FROM harvest_schedules ORDER BY workflow_name",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, bool>(2)?,
            row.get::<_, Option<i64>>(3)?,
            row.get::<_, Option<i64>>(4)?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (workflow_name, spec, paused, next, last) = row?;
        out.push(ScheduleSummary {
            workflow_name,
            schedule: parse_spec(&spec)?,
            paused,
            next_run_at: next.and_then(DateTime::from_timestamp_millis),
            last_run_at: last.and_then(DateTime::from_timestamp_millis),
        });
    }
    Ok(out)
}

/// The names of the schedules due at `now`, oldest slot first.
pub fn due_names(conn: &Connection, now: i64) -> SqliteResult<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT workflow_name FROM harvest_schedules \
         WHERE paused = 0 AND next_run_at <= ?1 ORDER BY next_run_at, workflow_name",
    )?;
    let names = stmt
        .query_map([now], |row| row.get(0))?
        .collect::<Result<_, _>>()?;
    Ok(names)
}

/// The schedule `workflow_name`, if it is still due at `now`. The caller
/// reads it inside the fire transaction, so the slot cannot fire twice.
pub fn load_due(conn: &Connection, workflow_name: &str, now: i64) -> SqliteResult<Option<Due>> {
    let row = conn
        .query_row(
            "SELECT schedule_json, input_json, overlap_policy, max_active_runs, next_run_at \
             FROM harvest_schedules \
             WHERE workflow_name = ?1 AND paused = 0 AND next_run_at <= ?2",
            params![workflow_name, now],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            },
        )
        .optional()?;
    let Some((spec, input, overlap, max_active_runs, slot)) = row else {
        return Ok(None);
    };
    Ok(Some(Due {
        workflow_name: workflow_name.to_string(),
        input: serde_json::from_str(&input)?,
        allow_all: OverlapPolicy::from_db(&overlap) == OverlapPolicy::AllowAll,
        max_active_runs,
        slot,
        schedule: parse_spec(&spec)?,
    }))
}

/// The active runs of the schedule `workflow_name` at `now`, successors
/// included.
///
/// A run past its deadline or with a pending cancel does not count. Its next
/// drive ends it, and Postgres has already ended it.
pub fn active_runs(conn: &Connection, workflow_name: &str, now: i64) -> SqliteResult<i64> {
    Ok(conn.query_row(
        "SELECT COUNT(*) FROM harvest_executions \
         WHERE schedule_name = ?1 AND state = 'RUNNING' AND cancel_reason IS NULL \
           AND (deadline_at IS NULL OR deadline_at > ?2)",
        params![workflow_name, now],
        |row| row.get(0),
    )?)
}

/// Move `due` to its next slot after a fire at `now`. `dispatched` is `true`
/// when the slot has its run, so the slot becomes `last_run_at`.
pub fn advance(conn: &Connection, due: &Due, now: i64, dispatched: bool) -> SqliteResult<()> {
    let slot = to_time(due.slot);
    let next = schedule_plan::next_slot_after_fire(&due.schedule, slot, to_time(now))
        .map(|at| at.timestamp_millis());
    conn.execute(
        "UPDATE harvest_schedules SET next_run_at = ?2, \
           last_run_at = CASE WHEN ?3 THEN ?4 ELSE last_run_at END \
         WHERE workflow_name = ?1",
        params![due.workflow_name, next, dispatched, due.slot],
    )?;
    Ok(())
}

/// The `workflow_id` of the run of one slot. It mirrors the Postgres form
/// `sched:<schedule id>:<workflow>:<seconds>`. The workflow name is the key
/// here, so it stands for the schedule id.
pub fn slot_workflow_id(workflow_name: &str, slot: i64) -> String {
    let secs = slot.div_euclid(1000);
    match slot.rem_euclid(1000) {
        0 => format!("sched:{workflow_name}:{secs}"),
        millis => format!("sched:{workflow_name}:{secs}.{:06}", millis * 1000),
    }
}

/// Refuse each field this backend cannot honor, by name. The message starts
/// with the field token, as other unsupported features do.
fn check_supported(schedule: &WorkflowSchedule) -> SqliteResult<()> {
    let catchup = schedule.catchup_policy.unwrap_or(if schedule.catchup {
        CatchupPolicy::Unbounded
    } else {
        CatchupPolicy::SkipAll
    });
    let refused = [
        ("dag_name", schedule.dag_name.is_some()),
        (
            "catchup_policy",
            schedule.catchup_policy.is_some() && catchup != CatchupPolicy::SkipAll,
        ),
        ("catchup", catchup != CatchupPolicy::SkipAll),
        (
            "overlap_policy",
            !matches!(
                schedule.overlap_policy,
                OverlapPolicy::Skip | OverlapPolicy::AllowAll
            ),
        ),
        ("calendar", schedule.calendar.is_some()),
        ("end_at", schedule.end_at.is_some()),
        ("max_runs", schedule.max_runs.is_some()),
        (
            "consecutive_failure_limit",
            schedule.consecutive_failure_limit.is_some(),
        ),
        ("retry_policy", schedule.retry_policy.is_some()),
        ("execution_timeout", schedule.execution_timeout.is_some()),
        (
            "chain_execution_timeout",
            schedule.chain_execution_timeout.is_some(),
        ),
        ("all_writable_shards", schedule.all_writable_shards),
        ("queue_name", schedule.queue_name != "default"),
    ];
    match refused.iter().find(|(_, refused)| *refused) {
        Some((field, _)) => Err(SqliteError::Unsupported(format!(
            "WorkflowSchedule.{field} — the sqlite backend does not support it (issue #2031)"
        ))),
        None => Ok(()),
    }
}

/// The first slot of `schedule` after `now`, in epoch milliseconds.
fn first_slot(schedule: &Schedule, now: i64) -> Option<i64> {
    schedule_plan::next_fire_after(schedule, to_time(now)).map(|at| at.timestamp_millis())
}

fn parse_spec(spec: &str) -> SqliteResult<Schedule> {
    serde_json::from_str(spec).map_err(|_| SqliteError::corrupt("schedule_json"))
}

/// An epoch-millisecond time. A value out of range reads as the epoch.
fn to_time(millis: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(millis).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::slot_workflow_id;

    #[test]
    fn a_slot_id_matches_the_postgres_form() {
        assert_eq!(slot_workflow_id("tick", 60_000), "sched:tick:60");
        assert_eq!(slot_workflow_id("tick", 60_250), "sched:tick:60.250000");
    }
}
