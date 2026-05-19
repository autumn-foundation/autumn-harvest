//! Calendar-aware schedule filtering for business-day and holiday-skip jobs.
//!
//! A [`Calendar`] is a named, Postgres-backed set of excluded dates. Schedules
//! can reference a calendar by name and declare a [`SkipPolicy`] that controls
//! what happens when a scheduled fire date falls on an excluded day.
//!
//! ## Pure helpers (always available)
//! - [`is_excluded_date`] — O(n) membership check
//! - [`apply_skip_policy`] — adjusts or suppresses a date
//!
//! ## DB-dependent helpers (require the `db` feature)
//! - [`preview_schedule_firings`] — generates a preview of upcoming fire times
//! - [`plan_backfill_with_calendar`] — like `plan_backfill_timestamps` but calendar-aware

use chrono::NaiveDate;
#[cfg(feature = "db")]
use chrono::TimeZone;
use serde::{Deserialize, Serialize};

use crate::policy::SkipPolicy;

/// An in-memory representation of a named calendar.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Calendar {
    /// Stable string identifier (e.g. `"us-federal-holidays"`, `"nyse"`).
    pub name: String,
    /// Human-readable description.
    pub description: Option<String>,
    /// `true` for built-in calendars shipped by Harvest; operators cannot delete them.
    pub built_in: bool,
}

/// Returns `true` when `date` appears in `excluded_dates`.
///
/// The check is O(n) linear scan. For performance-sensitive paths sort
/// `excluded_dates` and use binary search; the function handles both sorted
/// and unsorted slices correctly.
#[must_use]
pub fn is_excluded_date(date: NaiveDate, excluded_dates: &[NaiveDate]) -> bool {
    excluded_dates.contains(&date)
}

/// Adjust a scheduled fire date according to a calendar and skip policy.
///
/// - When `date` is not excluded, returns `Some(date)` unchanged.
/// - `SkipPolicy::Skip` → `None` (firing is suppressed).
/// - `SkipPolicy::RunNextBusinessDay` → first subsequent non-excluded date.
/// - `SkipPolicy::RunPrevBusinessDay` → most recent preceding non-excluded date.
///
/// Scans up to 365 days in either direction; returns `None` if no non-excluded
/// day can be found within that window (degenerate calendar with 365 consecutive
/// exclusions).
#[must_use]
pub fn apply_skip_policy(
    date: NaiveDate,
    skip_policy: SkipPolicy,
    excluded_dates: &[NaiveDate],
) -> Option<NaiveDate> {
    if !is_excluded_date(date, excluded_dates) {
        return Some(date);
    }
    match skip_policy {
        SkipPolicy::Skip => None,
        SkipPolicy::RunNextBusinessDay => {
            let mut candidate = date + chrono::Duration::days(1);
            for _ in 0..365 {
                if !is_excluded_date(candidate, excluded_dates) {
                    return Some(candidate);
                }
                candidate += chrono::Duration::days(1);
            }
            None
        }
        SkipPolicy::RunPrevBusinessDay => {
            let mut candidate = date - chrono::Duration::days(1);
            for _ in 0..365 {
                if !is_excluded_date(candidate, excluded_dates) {
                    return Some(candidate);
                }
                candidate -= chrono::Duration::days(1);
            }
            None
        }
    }
}

/// Rebase a `DateTime<Utc>` to a different date, preserving the time-of-day.
///
/// Returns the original datetime if the date cannot be combined with the time.
#[cfg(feature = "db")]
#[must_use]
fn rebase_to_date(
    ts: chrono::DateTime<chrono::Utc>,
    date: NaiveDate,
) -> chrono::DateTime<chrono::Utc> {
    let naive = date.and_time(ts.time());
    chrono::Utc.from_utc_datetime(&naive)
}

/// A single entry in a schedule fire preview.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScheduleFirePreview {
    /// The original (unadjusted) scheduled fire timestamp.
    pub scheduled_at: chrono::DateTime<chrono::Utc>,
    /// The effective fire timestamp after calendar + skip policy.
    ///
    /// `None` means the firing is suppressed (`SkipPolicy::Skip` on an excluded date).
    pub effective_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Human-readable reason.
    ///
    /// - `"Fired"` — fires as scheduled.
    /// - `"SkippedByCalendar:<calendar_name>"` — suppressed by calendar.
    /// - `"DeferredFrom:<YYYY-MM-DD>"` — moved to a different day.
    pub reason: String,
}

/// Generate the next `count` preview entries for a schedule.
///
/// Each entry explains whether the firing is suppressed, deferred, or fired as-is.
/// Pass an empty `excluded_dates` slice (or `calendar_name = None`) to disable
/// calendar filtering.
///
/// The preview does not query the database; supply `excluded_dates` from the DB
/// calendar helpers.
#[cfg(feature = "db")]
#[must_use]
pub fn preview_schedule_firings(
    schedule: &crate::policy::Schedule,
    from: chrono::DateTime<chrono::Utc>,
    count: usize,
    calendar_name: Option<&str>,
    excluded_dates: &[NaiveDate],
    skip_policy: SkipPolicy,
) -> Vec<ScheduleFirePreview> {
    use crate::scheduler::next_run_after_pub;

    let mut entries = Vec::with_capacity(count);
    let mut cursor = from;

    while entries.len() < count {
        let Some(fire_time) = next_run_after_pub(Some(schedule), cursor) else {
            break;
        };
        cursor = fire_time;
        let fire_date = fire_time.date_naive();

        let (effective_at, reason) = calendar_name.map_or_else(
            || (Some(fire_time), "Fired".to_string()),
            |cal_name| match apply_skip_policy(fire_date, skip_policy, excluded_dates) {
                None => (None, format!("SkippedByCalendar:{cal_name}")),
                Some(adjusted) if adjusted == fire_date => (Some(fire_time), "Fired".to_string()),
                Some(adjusted) => (
                    Some(rebase_to_date(fire_time, adjusted)),
                    format!("DeferredFrom:{}", fire_date.format("%Y-%m-%d")),
                ),
            },
        );

        entries.push(ScheduleFirePreview {
            scheduled_at: fire_time,
            effective_at,
            reason,
        });
    }

    entries
}

/// Like [`crate::scheduler::plan_backfill_timestamps`] but respects a calendar.
///
/// Excluded dates are filtered out or adjusted per the skip policy:
/// - `Skip` → the timestamp is dropped from the result.
/// - `RunNextBusinessDay` / `RunPrevBusinessDay` → the timestamp is adjusted to
///   the nearest non-excluded day while preserving the original time-of-day.
///
/// # Errors
///
/// Returns [`crate::scheduler::BackfillPlanError::LimitExceeded`] when the raw
/// timestamp window would exceed `max_count` before calendar filtering.
#[cfg(feature = "db")]
pub fn plan_backfill_with_calendar(
    schedule: Option<&crate::policy::Schedule>,
    from: chrono::DateTime<chrono::Utc>,
    to: chrono::DateTime<chrono::Utc>,
    max_count: usize,
    excluded_dates: &[NaiveDate],
    skip_policy: SkipPolicy,
) -> Result<Vec<chrono::DateTime<chrono::Utc>>, crate::scheduler::BackfillPlanError> {
    let raw = crate::scheduler::plan_backfill_timestamps(schedule, from, to, max_count)?;
    let adjusted = raw
        .into_iter()
        .filter_map(|ts| {
            let date = ts.date_naive();
            apply_skip_policy(date, skip_policy, excluded_dates).map(|adj_date| {
                if adj_date == date {
                    ts
                } else {
                    rebase_to_date(ts, adj_date)
                }
            })
        })
        .collect();
    Ok(adjusted)
}

// ── DB helpers (require `db` feature) ─────────────────────────────────────────

/// Load all exclusion dates for a named calendar from the database.
///
/// Returns an empty vec when the calendar name is not found or has no
/// exclusions. This is intentionally lenient: a misconfigured `calendar_name`
/// silently disables filtering rather than halting the scheduler.
///
/// # Errors
///
/// Returns `HarvestError::Database` on connection or query failure.
#[cfg(feature = "db")]
pub async fn load_exclusions_for_calendar(
    conn: &mut diesel_async::AsyncPgConnection,
    calendar_name: &str,
) -> crate::error::HarvestResult<Vec<NaiveDate>> {
    use crate::schema::harvest_calendar_exclusions;
    use diesel::{ExpressionMethods, QueryDsl};
    use diesel_async::RunQueryDsl;

    let rows: Vec<chrono::NaiveDate> = harvest_calendar_exclusions::table
        .filter(harvest_calendar_exclusions::calendar_name.eq(calendar_name))
        .select(harvest_calendar_exclusions::excluded_date)
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    Ok(rows)
}

/// Load all calendars from the database.
///
/// # Errors
///
/// Returns `HarvestError::Database` on connection or query failure.
#[cfg(feature = "db")]
pub async fn list_calendars(
    conn: &mut diesel_async::AsyncPgConnection,
) -> crate::error::HarvestResult<Vec<crate::models::HarvestCalendar>> {
    use crate::schema::harvest_calendars;
    use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
    use diesel_async::RunQueryDsl;

    harvest_calendars::table
        .select(crate::models::HarvestCalendar::as_select())
        .order(harvest_calendars::name.asc())
        .load(conn)
        .await
        .map_err(crate::error::database_error)
}

/// Get a single calendar by name.
///
/// # Errors
///
/// Returns `HarvestError::Database` on query failure, or
/// `HarvestError::NotFound` when no calendar has that name.
#[cfg(feature = "db")]
pub async fn get_calendar(
    conn: &mut diesel_async::AsyncPgConnection,
    name: &str,
) -> crate::error::HarvestResult<crate::models::HarvestCalendar> {
    use crate::schema::harvest_calendars;
    use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper};
    use diesel_async::RunQueryDsl;

    harvest_calendars::table
        .filter(harvest_calendars::name.eq(name))
        .select(crate::models::HarvestCalendar::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?
        .ok_or_else(|| crate::error::HarvestError::NotFound(format!("calendar '{name}'")))
}

/// Create a new custom calendar.
///
/// Fails with `HarvestError::Config` if a calendar with the same name already exists.
///
/// # Errors
///
/// Returns `HarvestError::Database` on query failure, or
/// `HarvestError::Config` when the name conflicts with an existing calendar.
#[cfg(feature = "db")]
pub async fn create_calendar(
    conn: &mut diesel_async::AsyncPgConnection,
    name: &str,
    description: Option<&str>,
) -> crate::error::HarvestResult<crate::models::HarvestCalendar> {
    use crate::models::NewHarvestCalendar;
    use crate::schema::harvest_calendars;
    use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
    use diesel_async::RunQueryDsl;

    let new = NewHarvestCalendar {
        id: uuid::Uuid::new_v4(),
        name,
        description,
        built_in: false,
    };
    diesel::insert_into(harvest_calendars::table)
        .values(&new)
        .execute(conn)
        .await
        .map_err(|e| {
            if e.to_string().contains("unique") || e.to_string().contains("duplicate") {
                crate::error::HarvestError::Config(format!("calendar '{name}' already exists"))
            } else {
                crate::error::database_error(e)
            }
        })?;

    harvest_calendars::table
        .filter(harvest_calendars::name.eq(name))
        .select(crate::models::HarvestCalendar::as_select())
        .first(conn)
        .await
        .map_err(crate::error::database_error)
}

/// Replace the exclusion set for a calendar (PUT semantics).
///
/// Deletes all existing exclusions for `calendar_name`, then inserts `dates`.
/// Built-in calendars may be updated (operators can extend them).
///
/// # Errors
///
/// Returns `HarvestError::Database` on query failure, or
/// `HarvestError::NotFound` when no calendar has that name.
#[cfg(feature = "db")]
pub async fn replace_calendar_exclusions(
    conn: &mut diesel_async::AsyncPgConnection,
    calendar_name: &str,
    dates: &[NaiveDate],
) -> crate::error::HarvestResult<()> {
    use crate::models::NewHarvestCalendarExclusion;
    use crate::schema::harvest_calendar_exclusions;
    use diesel::{ExpressionMethods, QueryDsl};
    use diesel_async::RunQueryDsl;

    // Validate the calendar exists.
    get_calendar(conn, calendar_name).await?;

    diesel::delete(
        harvest_calendar_exclusions::table
            .filter(harvest_calendar_exclusions::calendar_name.eq(calendar_name)),
    )
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;

    let rows: Vec<NewHarvestCalendarExclusion<'_>> = dates
        .iter()
        .map(|&excluded_date| NewHarvestCalendarExclusion {
            calendar_name,
            excluded_date,
        })
        .collect();

    if !rows.is_empty() {
        diesel::insert_into(harvest_calendar_exclusions::table)
            .values(&rows)
            .on_conflict((
                harvest_calendar_exclusions::calendar_name,
                harvest_calendar_exclusions::excluded_date,
            ))
            .do_nothing()
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;
    }

    Ok(())
}

/// Delete a custom calendar (built-in calendars cannot be deleted).
///
/// Exclusions are cascade-deleted by the FK constraint.
///
/// # Errors
///
/// Returns `HarvestError::NotFound` when the calendar does not exist.
/// Returns `HarvestError::Config` when attempting to delete a built-in calendar.
#[cfg(feature = "db")]
pub async fn delete_calendar(
    conn: &mut diesel_async::AsyncPgConnection,
    name: &str,
) -> crate::error::HarvestResult<()> {
    use crate::schema::harvest_calendars;
    use diesel::{ExpressionMethods, QueryDsl};
    use diesel_async::RunQueryDsl;

    let cal = get_calendar(conn, name).await?;
    if cal.built_in {
        return Err(crate::error::HarvestError::Config(format!(
            "built-in calendar '{name}' cannot be deleted; create a custom calendar with the same name to shadow it"
        )));
    }

    diesel::delete(harvest_calendars::table.filter(harvest_calendars::name.eq(name)))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(())
}

// ── Built-in calendar data ────────────────────────────────────────────────────

/// US federal public holidays for 2025 and 2026 (observed dates).
///
/// Used to seed the `"us-federal-holidays"` built-in calendar in the migration.
/// Operators can shadow this calendar per-deployment by creating a custom
/// calendar with the same name.
pub const US_FEDERAL_HOLIDAYS_2025_2026: &[&str] = &[
    // 2025
    "2025-01-01", // New Year's Day
    "2025-01-20", // MLK Day (observed)
    "2025-02-17", // Presidents' Day (observed)
    "2025-05-26", // Memorial Day
    "2025-06-19", // Juneteenth
    "2025-07-04", // Independence Day
    "2025-09-01", // Labor Day
    "2025-10-13", // Columbus Day
    "2025-11-11", // Veterans Day
    "2025-11-27", // Thanksgiving
    "2025-12-25", // Christmas
    // 2026
    "2026-01-01", // New Year's Day
    "2026-01-19", // MLK Day (observed)
    "2026-02-16", // Presidents' Day (observed)
    "2026-05-25", // Memorial Day
    "2026-06-19", // Juneteenth
    "2026-07-03", // Independence Day (observed, 4th falls on Saturday)
    "2026-09-07", // Labor Day
    "2026-10-12", // Columbus Day
    "2026-11-11", // Veterans Day
    "2026-11-26", // Thanksgiving
    "2026-12-25", // Christmas
];

/// NYSE market holidays for 2025 and 2026.
pub const NYSE_HOLIDAYS_2025_2026: &[&str] = &[
    // 2025
    "2025-01-01", // New Year's Day
    "2025-01-20", // MLK Day
    "2025-02-17", // Presidents' Day
    "2025-04-18", // Good Friday
    "2025-05-26", // Memorial Day
    "2025-06-19", // Juneteenth
    "2025-07-04", // Independence Day
    "2025-09-01", // Labor Day
    "2025-11-27", // Thanksgiving
    "2025-12-25", // Christmas
    // 2026
    "2026-01-01", // New Year's Day
    "2026-01-19", // MLK Day
    "2026-02-16", // Presidents' Day
    "2026-04-03", // Good Friday
    "2026-05-25", // Memorial Day
    "2026-06-19", // Juneteenth
    "2026-07-03", // Independence Day (observed)
    "2026-09-07", // Labor Day
    "2026-11-26", // Thanksgiving
    "2026-12-25", // Christmas
];

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn date(s: &str) -> NaiveDate {
        NaiveDate::parse_from_str(s, "%Y-%m-%d").expect("valid date")
    }

    fn excluded(dates: &[&str]) -> Vec<NaiveDate> {
        dates.iter().map(|s| date(s)).collect()
    }

    // ── is_excluded_date ──────────────────────────────────────────────────────

    #[test]
    fn is_excluded_date_true_for_excluded() {
        let exc = excluded(&["2026-07-04", "2026-12-25"]);
        assert!(is_excluded_date(date("2026-07-04"), &exc));
        assert!(is_excluded_date(date("2026-12-25"), &exc));
    }

    #[test]
    fn is_excluded_date_false_for_non_excluded() {
        let exc = excluded(&["2026-07-04"]);
        assert!(!is_excluded_date(date("2026-07-05"), &exc));
        assert!(!is_excluded_date(date("2026-01-01"), &exc));
    }

    #[test]
    fn is_excluded_date_empty_exclusions_always_false() {
        assert!(!is_excluded_date(date("2026-07-04"), &[]));
    }

    // ── apply_skip_policy ─────────────────────────────────────────────────────

    #[test]
    fn apply_skip_policy_non_excluded_returns_same() {
        let exc = excluded(&["2026-07-04"]);
        let d = date("2026-07-05");
        assert_eq!(apply_skip_policy(d, SkipPolicy::Skip, &exc), Some(d));
        assert_eq!(
            apply_skip_policy(d, SkipPolicy::RunNextBusinessDay, &exc),
            Some(d)
        );
    }

    #[test]
    fn apply_skip_policy_skip_suppresses_excluded() {
        let exc = excluded(&["2026-07-04"]);
        assert_eq!(
            apply_skip_policy(date("2026-07-04"), SkipPolicy::Skip, &exc),
            None
        );
    }

    #[test]
    fn apply_skip_policy_next_business_day_finds_next() {
        let exc = excluded(&["2026-07-04"]);
        assert_eq!(
            apply_skip_policy(date("2026-07-04"), SkipPolicy::RunNextBusinessDay, &exc),
            Some(date("2026-07-05"))
        );
    }

    #[test]
    fn apply_skip_policy_prev_business_day_finds_prev() {
        let exc = excluded(&["2026-07-04"]);
        assert_eq!(
            apply_skip_policy(date("2026-07-04"), SkipPolicy::RunPrevBusinessDay, &exc),
            Some(date("2026-07-03"))
        );
    }

    #[test]
    fn apply_skip_policy_chains_through_multiple_consecutive_exclusions() {
        let exc = excluded(&["2026-08-06", "2026-08-07", "2026-08-08"]);
        assert_eq!(
            apply_skip_policy(date("2026-08-06"), SkipPolicy::RunNextBusinessDay, &exc),
            Some(date("2026-08-09"))
        );
    }

    #[test]
    fn apply_skip_policy_prev_chains_through_multiple_consecutive_exclusions() {
        let exc = excluded(&["2026-08-06", "2026-08-07", "2026-08-08"]);
        assert_eq!(
            apply_skip_policy(date("2026-08-08"), SkipPolicy::RunPrevBusinessDay, &exc),
            Some(date("2026-08-05"))
        );
    }

    // ── preview_schedule_firings ──────────────────────────────────────────────

    #[cfg(feature = "db")]
    mod db_tests {
        use super::*;
        use crate::policy::Schedule;
        use chrono::Utc;

        #[test]
        fn preview_firings_no_calendar_all_fired() {
            let from = "2026-07-01T00:00:00Z"
                .parse::<chrono::DateTime<Utc>>()
                .unwrap();
            let schedule = Schedule::Cron("0 9 * * *".to_string());
            let previews =
                preview_schedule_firings(&schedule, from, 3, None, &[], SkipPolicy::Skip);
            assert_eq!(previews.len(), 3);
            for p in &previews {
                assert_eq!(p.reason, "Fired");
                assert!(p.effective_at.is_some());
            }
        }

        #[test]
        fn preview_firings_skip_excluded() {
            let from = "2026-07-03T10:00:00Z"
                .parse::<chrono::DateTime<Utc>>()
                .unwrap();
            let schedule = Schedule::Cron("0 9 * * *".to_string());
            let exc = excluded(&["2026-07-04"]);
            let previews = preview_schedule_firings(
                &schedule,
                from,
                3,
                Some("us-federal-holidays"),
                &exc,
                SkipPolicy::Skip,
            );
            let skipped = previews
                .iter()
                .find(|p| p.scheduled_at.date_naive() == date("2026-07-04"));
            assert!(skipped.is_some(), "Jul 4 should appear in preview");
            let skipped = skipped.unwrap();
            assert_eq!(skipped.effective_at, None);
            assert!(
                skipped.reason.starts_with("SkippedByCalendar:"),
                "reason should start with SkippedByCalendar: got '{}'",
                skipped.reason
            );
        }

        #[test]
        fn preview_firings_deferred_next() {
            let from = "2026-07-03T10:00:00Z"
                .parse::<chrono::DateTime<Utc>>()
                .unwrap();
            let schedule = Schedule::Cron("0 9 * * *".to_string());
            let exc = excluded(&["2026-07-04"]);
            let previews = preview_schedule_firings(
                &schedule,
                from,
                3,
                Some("us-federal-holidays"),
                &exc,
                SkipPolicy::RunNextBusinessDay,
            );
            let deferred = previews
                .iter()
                .find(|p| p.scheduled_at.date_naive() == date("2026-07-04"));
            assert!(deferred.is_some());
            let deferred = deferred.unwrap();
            assert!(deferred.effective_at.is_some());
            assert_eq!(
                deferred.effective_at.unwrap().date_naive(),
                date("2026-07-05")
            );
            assert!(
                deferred.reason.starts_with("DeferredFrom:"),
                "reason should start with DeferredFrom: got '{}'",
                deferred.reason
            );
        }

        // ── plan_backfill_with_calendar ───────────────────────────────────────

        #[test]
        fn backfill_skip_drops_excluded_dates() {
            let from = "2026-07-01T09:00:00Z"
                .parse::<chrono::DateTime<Utc>>()
                .unwrap();
            let to = "2026-07-05T09:00:00Z"
                .parse::<chrono::DateTime<Utc>>()
                .unwrap();
            let schedule = Schedule::Cron("0 9 * * *".to_string());
            let exc = excluded(&["2026-07-04"]);
            let result =
                plan_backfill_with_calendar(Some(&schedule), from, to, 100, &exc, SkipPolicy::Skip)
                    .unwrap();
            let dates: Vec<_> = result.iter().map(chrono::DateTime::date_naive).collect();
            assert!(
                !dates.contains(&date("2026-07-04")),
                "Jul 4 should be excluded"
            );
            assert!(
                dates.contains(&date("2026-07-05")),
                "Jul 5 should be present"
            );
        }

        #[test]
        fn backfill_next_business_day_adjusts_excluded_date() {
            let from = "2026-07-01T09:00:00Z"
                .parse::<chrono::DateTime<Utc>>()
                .unwrap();
            let to = "2026-07-05T09:00:00Z"
                .parse::<chrono::DateTime<Utc>>()
                .unwrap();
            let schedule = Schedule::Cron("0 9 * * *".to_string());
            let exc = excluded(&["2026-07-04"]);
            let result = plan_backfill_with_calendar(
                Some(&schedule),
                from,
                to,
                100,
                &exc,
                SkipPolicy::RunNextBusinessDay,
            )
            .unwrap();
            let dates: Vec<_> = result.iter().map(chrono::DateTime::date_naive).collect();
            assert!(
                dates.iter().filter(|&&d| d == date("2026-07-05")).count() >= 1,
                "Jul 5 should appear (as adjusted Jul 4 and/or natural Jul 5)"
            );
            assert!(
                !dates.contains(&date("2026-07-04")),
                "raw Jul 4 should not appear"
            );
        }

        #[test]
        fn backfill_no_exclusions_passes_through() {
            let from = "2026-07-01T09:00:00Z"
                .parse::<chrono::DateTime<Utc>>()
                .unwrap();
            let to = "2026-07-03T09:00:00Z"
                .parse::<chrono::DateTime<Utc>>()
                .unwrap();
            let schedule = Schedule::Cron("0 9 * * *".to_string());
            let plain =
                crate::scheduler::plan_backfill_timestamps(Some(&schedule), from, to, 100).unwrap();
            let with_cal =
                plan_backfill_with_calendar(Some(&schedule), from, to, 100, &[], SkipPolicy::Skip)
                    .unwrap();
            assert_eq!(
                plain, with_cal,
                "empty exclusion list must not change results"
            );
        }
    }
}
