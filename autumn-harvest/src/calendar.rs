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

#[cfg(feature = "db")]
use chrono::TimeZone;
use chrono::{Datelike, NaiveDate};
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

/// Internal helper: returns `true` if `date` should be treated as excluded,
/// checking both the explicit exclusion list and the weekend flag.
fn is_excluded_impl(date: NaiveDate, excluded_dates: &[NaiveDate], exclude_weekends: bool) -> bool {
    (exclude_weekends && matches!(date.weekday(), chrono::Weekday::Sat | chrono::Weekday::Sun))
        || excluded_dates.contains(&date)
}

/// Adjust a scheduled fire date according to a calendar and skip policy.
///
/// - When `date` is not excluded, returns `Some(date)` unchanged.
/// - `SkipPolicy::Skip` → `None` (firing is suppressed).
/// - `SkipPolicy::RunNextBusinessDay` → first subsequent non-excluded weekday.
/// - `SkipPolicy::RunPrevBusinessDay` → most recent preceding non-excluded weekday.
///
/// Set `exclude_weekends = true` when the attached calendar is `"weekends-off"`
/// (or any calendar that implies Saturday/Sunday are always excluded). When
/// `true`, Saturday and Sunday are treated as excluded regardless of whether they
/// appear in `excluded_dates`.
///
/// Scans up to 365 days in either direction; returns `None` if no non-excluded
/// day can be found within that window (degenerate calendar with 365 consecutive
/// exclusions).
#[must_use]
pub fn apply_skip_policy(
    date: NaiveDate,
    skip_policy: SkipPolicy,
    excluded_dates: &[NaiveDate],
    exclude_weekends: bool,
) -> Option<NaiveDate> {
    if !is_excluded_impl(date, excluded_dates, exclude_weekends) {
        return Some(date);
    }
    match skip_policy {
        SkipPolicy::Skip => None,
        SkipPolicy::RunNextBusinessDay => {
            let mut candidate = date + chrono::Duration::days(1);
            for _ in 0..365 {
                if !is_excluded_impl(candidate, excluded_dates, exclude_weekends) {
                    return Some(candidate);
                }
                candidate += chrono::Duration::days(1);
            }
            None
        }
        SkipPolicy::RunPrevBusinessDay => {
            let mut candidate = date - chrono::Duration::days(1);
            for _ in 0..365 {
                if !is_excluded_impl(candidate, excluded_dates, exclude_weekends) {
                    return Some(candidate);
                }
                candidate -= chrono::Duration::days(1);
            }
            None
        }
    }
}

/// Returns `true` when the calendar name implies Saturday/Sunday are always excluded.
///
/// Used to derive the `exclude_weekends` flag without a separate DB lookup.
///
/// This is a **scheduler-only** convention. The business-day timer primitive
/// ([`add_business_days`], issue #806) does *not* consult it — there, Saturday
/// and Sunday are always non-business days by definition, whatever the calendar
/// is named. See [`is_business_day`].
#[must_use]
pub fn calendar_excludes_weekends(calendar_name: &str) -> bool {
    calendar_name == "weekends-off"
}

// ── Business-day arithmetic (issue #806) ──────────────────────────────────────

/// Maximum `n` accepted by the business-day helpers (~14 calendar years).
///
/// Bounds the derived scan so a typo (`n = u32::MAX`) is a loud rejection rather
/// than a multi-million-iteration stall inside a workflow decision cycle.
pub const MAX_BUSINESS_DAYS: u32 = 3650;

/// Returns `true` when `date` is a business day for the business-day timer API.
///
/// A date is a business day when it is **not** a Saturday or Sunday **and** does
/// not appear in `holidays`.
///
/// Weekends are always non-business here, independent of the calendar's name —
/// deliberately *not* the [`calendar_excludes_weekends`] `"weekends-off"`
/// convention the scheduler uses, so a calendar that only enumerates public
/// holidays (`us-federal-holidays`, `nyse`) is immediately correct for
/// [`add_business_days`] with no migration. Regions whose weekend is not
/// Sat/Sun enumerate their weekend dates as holidays; a configurable weekend
/// day-set is a named follow-up.
///
/// `holidays` is a [`BTreeSet`](std::collections::BTreeSet) so lookups are
/// O(log n) inside the scan loop rather than the O(n) slice scan
/// [`is_excluded_date`] performs.
#[must_use]
pub fn is_business_day(date: NaiveDate, holidays: &std::collections::BTreeSet<NaiveDate>) -> bool {
    !matches!(date.weekday(), chrono::Weekday::Sat | chrono::Weekday::Sun)
        && !holidays.contains(&date)
}

/// A resolved business-day deadline plus the non-business dates stepped over.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BusinessDayResolution {
    /// The resolved fire instant. Preserves the anchor's UTC time-of-day.
    pub deadline: chrono::DateTime<chrono::Utc>,
    /// Non-business dates the scan stepped over, in scan order.
    ///
    /// Recorded for the audit trail so a workflow's history answers
    /// "why did this land on Tuesday?" without re-reading the calendar.
    pub skipped: Vec<NaiveDate>,
}

/// Why a business-day resolution could not be computed.
///
/// Every variant is a **deterministic function of the anchor, `n`, and the
/// calendar snapshot** — never of wall-clock time — so a rejection frozen into
/// history replays to the identical rejection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum BusinessDayRejection {
    /// The scan window elapsed without finding enough business days.
    ///
    /// Only reachable with a degenerate calendar that excludes every day in the
    /// derived window (`n * 7 + 14` calendar days).
    ScanExhausted {
        /// The derived scan bound, in calendar days.
        scanned_days: u32,
    },
    /// The scan needed a date past the last date the calendar knows about.
    ///
    /// Answering anyway would silently degrade to a weekends-only guess that
    /// ignores every holiday beyond the horizon — the wrong answer delivered
    /// confidently. Extend the calendar instead.
    CoverageExhausted {
        /// The last date the calendar has an exclusion for.
        coverage_end: NaiveDate,
    },
    /// Advancing the cursor would overflow [`NaiveDate`]'s representable range.
    DateOverflow,
    /// `n` exceeds [`MAX_BUSINESS_DAYS`].
    ///
    /// Enforced by the helpers themselves, not just by their callers: both are
    /// `pub` and re-exported, and an unbounded `n` drives a multi-million
    /// iteration scan that accumulates a `skipped` vector proportional to it.
    TooManyBusinessDays {
        /// The `n` that was passed.
        requested: u32,
        /// [`MAX_BUSINESS_DAYS`].
        max: u32,
    },
}

impl std::fmt::Display for BusinessDayRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ScanExhausted { scanned_days } => write!(
                f,
                "no business day found within {scanned_days} calendar days; the calendar excludes every day in that window"
            ),
            Self::TooManyBusinessDays { requested, max } => write!(
                f,
                "n = {requested} exceeds the maximum of {max} business days"
            ),
            Self::CoverageExhausted { coverage_end } => write!(
                f,
                "resolution requires a date after {coverage_end}, the last date this calendar covers; extend the calendar's exclusions"
            ),
            Self::DateOverflow => write!(f, "date arithmetic overflowed the representable range"),
        }
    }
}

/// An in-memory, immutable snapshot of the named calendars a worker can resolve.
///
/// Register one on the builder with `HarvestBuilder::state(BusinessCalendars::…)`
/// to make [`WorkflowContext::timer_business_days`](crate::WorkflowContext::timer_business_days)
/// usable. Built-in calendars come from the shipped
/// [`US_FEDERAL_HOLIDAYS_2025_2026`] / [`NYSE_HOLIDAYS_2025_2026`] arrays;
/// DB-backed calendars are loaded by the embedder with
/// [`load_exclusions_for_calendar`] and registered the same way.
///
/// **A registered-but-empty calendar is not the same as an unknown name.**
/// [`get`](Self::get) keys on registry presence, so a typo'd calendar name
/// resolves to `None` (a loud error) rather than an empty holiday set (a silent
/// weekends-only answer).
///
/// This type is not `db`-gated — it is a plain in-memory map.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BusinessCalendars {
    calendars: std::collections::BTreeMap<String, RegisteredCalendar>,
}

/// One registered calendar: its holiday set plus an optional declared horizon.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct RegisteredCalendar {
    holidays: std::collections::BTreeSet<NaiveDate>,
    coverage_end: Option<NaiveDate>,
}

impl BusinessCalendars {
    /// The last date the shipped built-in calendars have holiday data for.
    ///
    /// The `2025`/`2026` arrays are complete through the end of 2026, so a
    /// resolution needing a **2027** date is rejected with
    /// [`BusinessDayRejection::CoverageExhausted`] rather than silently answered
    /// weekends-only. Bump this (and the arrays) when adding a year.
    pub const BUILTIN_COVERAGE_END: NaiveDate = match NaiveDate::from_ymd_opt(2026, 12, 31) {
        Some(d) => d,
        None => panic!("2026-12-31 is a valid date"),
    };

    /// An empty registry — every lookup resolves to `None`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The calendars Harvest ships: `"us-federal-holidays"` and `"nyse"`.
    ///
    /// Both declare a coverage horizon of [`BUILTIN_COVERAGE_END`](Self::BUILTIN_COVERAGE_END).
    ///
    /// # Intended for demos, examples, and tests — not long-lived production
    ///
    /// The shipped arrays are a **dated, finite** convenience. Because they
    /// declare a horizon, a resolution needing a date past
    /// [`BUILTIN_COVERAGE_END`](Self::BUILTIN_COVERAGE_END) is *rejected*
    /// (loudly, never silently answered weekends-only) — and a coverage
    /// rejection is **frozen** into the workflow's history, so extending the
    /// arrays in a later release does **not** recover executions that already
    /// froze one; those need a workflow reset.
    ///
    /// The practical consequence: once wall-clock time approaches the horizon,
    /// every `builtin()`-backed resolution starts freezing rejections
    /// fleet-wide. For production, register a calendar loaded from
    /// `harvest_calendar_exclusions`
    /// ([`load_exclusions_for_calendar`](crate::calendar::load_exclusions_for_calendar))
    /// so the holiday set — and its horizon — are operator-owned data rather
    /// than a compiled-in constant with an expiry date.
    #[must_use]
    pub fn builtin() -> Self {
        Self::new()
            .with_calendar_covering(
                "us-federal-holidays",
                parse_builtin_dates(US_FEDERAL_HOLIDAYS_2025_2026),
                Self::BUILTIN_COVERAGE_END,
            )
            .with_calendar_covering(
                "nyse",
                parse_builtin_dates(NYSE_HOLIDAYS_2025_2026),
                Self::BUILTIN_COVERAGE_END,
            )
    }

    /// Register (or replace) one calendar's holiday set, with **no** declared
    /// coverage horizon. Last write wins.
    ///
    /// Without a horizon the calendar is treated as authoritative forever — a
    /// resolution far past its last holiday is answered weekends-only rather
    /// than rejected. That is correct for a genuinely weekends-only calendar and
    /// convenient for tests, but for a holiday calendar loaded from a finite
    /// data source prefer [`with_calendar_covering`](Self::with_calendar_covering)
    /// so a resolution past the data's end fails loudly instead of quietly
    /// ignoring every holiday beyond it.
    #[must_use]
    pub fn with_calendar(
        mut self,
        name: impl Into<String>,
        holidays: impl IntoIterator<Item = NaiveDate>,
    ) -> Self {
        self.calendars.insert(
            name.into(),
            RegisteredCalendar {
                holidays: holidays.into_iter().collect(),
                coverage_end: None,
            },
        );
        self
    }

    /// Register (or replace) one calendar, **declaring** how far its data goes.
    ///
    /// `coverage_end` is the last date the calendar has holiday data for — not
    /// the last date it happens to list a holiday on. A resolution that needs a
    /// later date is rejected with [`BusinessDayRejection::CoverageExhausted`],
    /// turning "silently ignored every holiday after the data ran out" into a
    /// loud, deterministic error.
    #[must_use]
    pub fn with_calendar_covering(
        mut self,
        name: impl Into<String>,
        holidays: impl IntoIterator<Item = NaiveDate>,
        coverage_end: NaiveDate,
    ) -> Self {
        self.calendars.insert(
            name.into(),
            RegisteredCalendar {
                holidays: holidays.into_iter().collect(),
                coverage_end: Some(coverage_end),
            },
        );
        self
    }

    /// The holiday set for `name`, or `None` when the name is not registered.
    ///
    /// A registered-but-empty calendar returns `Some(empty_set)` — genuinely
    /// distinct from an unknown name, so a typo is a loud error rather than a
    /// silent weekends-only answer.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&std::collections::BTreeSet<NaiveDate>> {
        self.calendars.get(name).map(|c| &c.holidays)
    }

    /// The **declared** coverage horizon for `name`, if one was declared.
    ///
    /// `None` for an unknown calendar and for one registered via
    /// [`with_calendar`](Self::with_calendar) (which declares no horizon). It is
    /// deliberately *not* inferred from the maximum exclusion date: a sparse
    /// calendar's last holiday says nothing about how far its data extends, and
    /// inferring one there would reject every resolution landing after it.
    #[must_use]
    pub fn coverage_end(&self, name: &str) -> Option<NaiveDate> {
        self.calendars.get(name)?.coverage_end
    }

    /// The registered calendar names, sorted.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.calendars.keys().map(String::as_str)
    }
}

impl FromIterator<(String, Vec<NaiveDate>)> for BusinessCalendars {
    fn from_iter<T: IntoIterator<Item = (String, Vec<NaiveDate>)>>(iter: T) -> Self {
        Self {
            calendars: iter
                .into_iter()
                .map(|(name, dates)| {
                    (
                        name,
                        RegisteredCalendar {
                            holidays: dates.into_iter().collect(),
                            coverage_end: None,
                        },
                    )
                })
                .collect(),
        }
    }
}

/// Parse a shipped `YYYY-MM-DD` constant array. Malformed entries are dropped.
fn parse_builtin_dates(raw: &[&str]) -> Vec<NaiveDate> {
    let out: Vec<NaiveDate> = raw
        .iter()
        .filter_map(|s| {
            let parsed = NaiveDate::parse_from_str(s, "%Y-%m-%d");
            debug_assert!(parsed.is_ok(), "built-in calendar date '{s}' must parse");
            parsed.ok()
        })
        .collect();
    // A `debug_assert!` alone would let a typo'd literal silently vanish from a
    // RELEASE build's built-in calendar — a shipped holiday missing from a
    // deadline calculation, with no error anywhere. The length check is cheap
    // and runs in every profile.
    assert_eq!(
        out.len(),
        raw.len(),
        "a built-in calendar date literal failed to parse; the built-ins must \
         be complete in every build profile"
    );
    out
}

/// Slack added to the derived scan bound, in calendar days.
///
/// Sized to absorb a real corporate shutdown (a fortnight of consecutive
/// closures plus the weekends bracketing it) rather than a fortnight alone.
/// It is a **floor, independent of `n`**: `n = 0` is the roll-forward case, so
/// it must traverse the LONGEST unbroken non-business run before finding its
/// first business day — a bound that shrinks with `n` scales backwards from the
/// requirement and would reject `n = 0` on a calendar where `n = 1` succeeds.
const SCAN_BOUND_SLACK_DAYS: u32 = 30;

/// Derived scan bound in calendar days: `n * 7 + 30`.
///
/// Provably sufficient — every 7-day window contains at least one weekday, and
/// the fixed [`SCAN_BOUND_SLACK_DAYS`] floor absorbs a month of consecutive
/// closures at either end regardless of `n`. Scales with `n` rather than
/// silently capping at a fixed horizon.
const fn derived_scan_bound(n: u32) -> u32 {
    n.saturating_mul(7).saturating_add(SCAN_BOUND_SLACK_DAYS)
}

/// Resolve the instant `n` business days after `anchor`.
///
/// Business days are computed on **UTC dates**; the resolved deadline preserves
/// the anchor's UTC time-of-day. This matches the `DATE`-typed exclusion rows
/// and [`rebase_to_date`]'s convention, and is DST-free by construction.
///
/// `n = 0` **rolls forward**: the deadline is the anchor itself when its UTC
/// date is a business day, otherwise the next business date at the same
/// time-of-day. (See [`next_business_day_at_or_after`] for the intent-revealing
/// spelling.)
///
/// Weekends are always skipped — see [`is_business_day`].
///
/// # Errors
///
/// - [`BusinessDayRejection::ScanExhausted`] when the calendar excludes every
///   day in the derived `n * 7 + 14` calendar-day window.
/// - [`BusinessDayRejection::DateOverflow`] when advancing the cursor would
///   leave [`NaiveDate`]'s representable range.
///
/// Both are pure functions of the inputs, so a caller may freeze the rejection
/// into workflow history and replay it verbatim.
///
/// This form imposes **no coverage horizon** — see
/// [`add_business_days_bounded`], which rejects a resolution that would need a
/// date the calendar does not cover.
pub fn add_business_days(
    anchor: chrono::DateTime<chrono::Utc>,
    n: u32,
    holidays: &std::collections::BTreeSet<NaiveDate>,
) -> Result<BusinessDayResolution, BusinessDayRejection> {
    add_business_days_bounded(anchor, n, holidays, None)
}

/// [`add_business_days`], additionally rejecting past a calendar's coverage horizon.
///
/// `coverage_end` is the last date the calendar knows about (see
/// [`BusinessCalendars::coverage_end`]). When the scan needs a **later** date
/// the result would silently degrade to a weekends-only guess that ignores
/// every holiday beyond the horizon, so it is rejected instead. `None` disables
/// the check (a calendar with no exclusions is correct forever).
///
/// The horizon applies to **every** examined date including the anchor, so a
/// zero-advance resolution past the horizon is rejected too. A resolution that
/// lands exactly on `coverage_end` is accepted.
///
/// # Errors
///
/// As [`add_business_days`], plus [`BusinessDayRejection::CoverageExhausted`].
pub fn add_business_days_bounded(
    anchor: chrono::DateTime<chrono::Utc>,
    n: u32,
    holidays: &std::collections::BTreeSet<NaiveDate>,
    coverage_end: Option<NaiveDate>,
) -> Result<BusinessDayResolution, BusinessDayRejection> {
    // Enforced HERE, not only in the callers: both entry points are `pub` and
    // re-exported, and an unbounded `n` otherwise drives a multi-million
    // iteration scan whose `skipped` vector grows with it.
    if n > MAX_BUSINESS_DAYS {
        return Err(BusinessDayRejection::TooManyBusinessDays {
            requested: n,
            max: MAX_BUSINESS_DAYS,
        });
    }

    let bound = derived_scan_bound(n);
    let time_of_day = anchor.time();
    let mut cursor = anchor.date_naive();
    let mut skipped = Vec::new();
    let mut remaining = n;
    let mut advanced = 0_u32;

    loop {
        // Checked at the TOP of the body so it also covers the ZERO-ADVANCE
        // resolution (`n = 0` on a business-day anchor), which returns before
        // the cursor ever moves. Checking only after advancing let a weekday
        // anchor past the horizon resolve silently while a weekend anchor
        // rejected — the verdict must not depend on the anchor's weekday.
        // `cursor > end` (not `>=`) keeps exactly-on-horizon accepted.
        if let Some(end) = coverage_end
            && cursor > end
        {
            return Err(BusinessDayRejection::CoverageExhausted { coverage_end: end });
        }

        if is_business_day(cursor, holidays) {
            if remaining == 0 {
                let deadline = cursor.and_time(time_of_day).and_utc();
                return Ok(BusinessDayResolution { deadline, skipped });
            }
            remaining -= 1;
        } else {
            skipped.push(cursor);
        }

        if advanced == bound {
            return Err(BusinessDayRejection::ScanExhausted {
                scanned_days: bound,
            });
        }
        cursor = cursor
            .checked_add_days(chrono::Days::new(1))
            .ok_or(BusinessDayRejection::DateOverflow)?;
        advanced += 1;
    }
}

/// Resolve the first business instant at or after `anchor`.
///
/// The intent-revealing spelling of `add_business_days(anchor, 0, holidays)`:
/// returns `anchor` unchanged when its UTC date is a business day, otherwise
/// the next business date at the same UTC time-of-day.
///
/// # Errors
///
/// Same as [`add_business_days`].
pub fn next_business_day_at_or_after(
    anchor: chrono::DateTime<chrono::Utc>,
    holidays: &std::collections::BTreeSet<NaiveDate>,
) -> Result<BusinessDayResolution, BusinessDayRejection> {
    add_business_days(anchor, 0, holidays)
}

/// Rebase a `DateTime<Utc>` to a different date, preserving the time-of-day.
///
/// For `CronInTimezone` schedules the wall-clock time in the schedule's timezone
/// is preserved across DST boundaries instead of the raw UTC clock time. Falls
/// back to naive UTC rebasing for plain `Cron`/`Interval`/`None` schedules.
///
/// Returns the original datetime if the date cannot be combined with the time.
#[cfg(feature = "db")]
#[must_use]
fn rebase_to_date(
    ts: chrono::DateTime<chrono::Utc>,
    date: NaiveDate,
    schedule: Option<&crate::policy::Schedule>,
) -> chrono::DateTime<chrono::Utc> {
    if let Some(crate::policy::Schedule::CronInTimezone { tz, .. }) = schedule
        && let Ok(tz) = tz.parse::<chrono_tz::Tz>()
    {
        let local_ts = ts.with_timezone(&tz);
        if let Some(local_dt) = date
            .and_time(local_ts.time())
            .and_local_timezone(tz)
            .earliest()
        {
            return local_dt.with_timezone(&chrono::Utc);
        }
    }
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

        let exclude_weekends = calendar_name.is_some_and(calendar_excludes_weekends);
        let (effective_at, reason) = calendar_name.map_or_else(
            || (Some(fire_time), "Fired".to_string()),
            |cal_name| match apply_skip_policy(
                fire_date,
                skip_policy,
                excluded_dates,
                exclude_weekends,
            ) {
                None => (None, format!("SkippedByCalendar:{cal_name}")),
                Some(adjusted) if adjusted == fire_date => (Some(fire_time), "Fired".to_string()),
                Some(adjusted) => (
                    Some(rebase_to_date(fire_time, adjusted, Some(schedule))),
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

/// Backfill slot returned by [`plan_backfill_with_calendar`]: `(original_slot, fire_time)`.
///
/// `original_slot` is the raw cron-generated timestamp used for deterministic
/// workflow-ID generation. `fire_time` is the calendar-adjusted dispatch time
/// (may equal `original_slot` when the slot is not excluded).
pub type BackfillSlot = (chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>);

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
/// Returns `(original_slot, fire_time)` pairs.
///
/// `original_slot` is the raw cron-generated timestamp used for deterministic
/// workflow-ID generation. `fire_time` is the calendar-adjusted dispatch time
/// (may equal `original_slot` for unaffected slots). Keeping both prevents
/// two logical slots that rebase onto the same calendar day from colliding on
/// the derived workflow ID.
pub fn plan_backfill_with_calendar(
    schedule: Option<&crate::policy::Schedule>,
    from: chrono::DateTime<chrono::Utc>,
    to: chrono::DateTime<chrono::Utc>,
    max_count: usize,
    excluded_dates: &[NaiveDate],
    skip_policy: SkipPolicy,
    exclude_weekends: bool,
) -> Result<Vec<BackfillSlot>, crate::scheduler::BackfillPlanError> {
    let raw = crate::scheduler::plan_backfill_timestamps(schedule, from, to, max_count)?;
    let pairs = raw
        .into_iter()
        .filter_map(|ts| {
            let date = ts.date_naive();
            apply_skip_policy(date, skip_policy, excluded_dates, exclude_weekends).map(|adj_date| {
                if adj_date == date {
                    (ts, ts)
                } else {
                    (ts, rebase_to_date(ts, adj_date, schedule))
                }
            })
        })
        .collect();
    Ok(pairs)
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

/// Batched form of [`load_exclusions_for_calendar`] for many calendar names at once.
///
/// One query covering every name in `calendar_names` instead of one query
/// per name (Ledger perf pass on `GET /admin/schedules`, which otherwise
/// calls `load_exclusions_for_calendar` once per schedule row via
/// `scheduler::resolve_effective_fire_at`, re-querying the same calendar's
/// exclusions once per schedule that references it).
///
/// A name with no exclusion rows (misconfigured or exclusion-free) is simply
/// absent from the returned map, matching what `load_exclusions_for_calendar`
/// returns for it (an empty `Vec`) -- callers should treat a missing key the
/// same as an empty list.
///
/// # Errors
///
/// Returns `HarvestError::Database` on connection or query failure.
#[cfg(feature = "db")]
pub async fn load_exclusions_for_calendars(
    conn: &mut diesel_async::AsyncPgConnection,
    calendar_names: &[&str],
) -> crate::error::HarvestResult<std::collections::HashMap<String, Vec<NaiveDate>>> {
    use crate::schema::harvest_calendar_exclusions;
    use diesel::{ExpressionMethods, QueryDsl};
    use diesel_async::RunQueryDsl;

    if calendar_names.is_empty() {
        return Ok(std::collections::HashMap::new());
    }

    let rows: Vec<(String, chrono::NaiveDate)> = harvest_calendar_exclusions::table
        .filter(harvest_calendar_exclusions::calendar_name.eq_any(calendar_names))
        .select((
            harvest_calendar_exclusions::calendar_name,
            harvest_calendar_exclusions::excluded_date,
        ))
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    let mut map: std::collections::HashMap<String, Vec<NaiveDate>> =
        std::collections::HashMap::new();
    for (name, date) in rows {
        map.entry(name).or_default().push(date);
    }
    Ok(map)
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
    use diesel_async::{AsyncConnection, RunQueryDsl};

    // Validate the calendar exists before opening a transaction.
    get_calendar(conn, calendar_name).await?;

    let rows: Vec<NewHarvestCalendarExclusion<'_>> = dates
        .iter()
        .map(|&excluded_date| NewHarvestCalendarExclusion {
            calendar_name,
            excluded_date,
        })
        .collect();

    Box::pin(conn.transaction(async |tx| {
        diesel::delete(
            harvest_calendar_exclusions::table
                .filter(harvest_calendar_exclusions::calendar_name.eq(calendar_name)),
        )
        .execute(tx)
        .await
        .map_err(crate::error::database_error)?;

        if !rows.is_empty() {
            diesel::insert_into(harvest_calendar_exclusions::table)
                .values(&rows)
                .on_conflict((
                    harvest_calendar_exclusions::calendar_name,
                    harvest_calendar_exclusions::excluded_date,
                ))
                .do_nothing()
                .execute(tx)
                .await
                .map_err(crate::error::database_error)?;
        }

        Ok(())
    }))
    .await
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

    // ── business-day arithmetic (issue #806) ──────────────────────────────────

    fn holidays(dates: &[&str]) -> std::collections::BTreeSet<NaiveDate> {
        dates.iter().map(|s| date(s)).collect()
    }

    fn utc(s: &str) -> chrono::DateTime<chrono::Utc> {
        s.parse::<chrono::DateTime<chrono::Utc>>()
            .expect("valid RFC 3339 instant")
    }

    #[test]
    fn is_business_day_excludes_weekends_and_holidays() {
        let h = holidays(&["2026-07-06"]);
        assert!(is_business_day(date("2026-07-02"), &h), "Thursday");
        assert!(is_business_day(date("2026-07-03"), &h), "Friday");
        assert!(!is_business_day(date("2026-07-04"), &h), "Saturday");
        assert!(!is_business_day(date("2026-07-05"), &h), "Sunday");
        assert!(!is_business_day(date("2026-07-06"), &h), "holiday");
        assert!(is_business_day(date("2026-07-07"), &h), "Tuesday");
    }

    #[test]
    fn is_business_day_agrees_with_is_excluded_impl_weekend_semantics() {
        // Pins that the business-day rule and the scheduler's own weekend rule
        // cannot drift apart. `is_business_day` uses a BTreeSet for O(log n)
        // lookups (issue #806 R10) rather than `is_excluded_impl`'s O(n) slice
        // scan, so the agreement must be asserted explicitly.
        let list = excluded(&["2026-07-06"]);
        let set = holidays(&["2026-07-06"]);
        let mut d = date("2026-06-29");
        for _ in 0..30 {
            assert_eq!(
                is_business_day(d, &set),
                !is_excluded_impl(d, &list, true),
                "weekend/holiday semantics disagree on {d}"
            );
            d = d.succ_opt().expect("in range");
        }
    }

    #[test]
    fn add_business_days_thursday_plus_two_is_monday() {
        // 2026-07-02 is a Thursday; +2 business days steps over Sat 07-04 / Sun 07-05.
        let res = add_business_days(utc("2026-07-02T09:00:00Z"), 2, &holidays(&[]))
            .expect("resolves within the scan bound");
        assert_eq!(res.deadline.date_naive(), date("2026-07-06"), "Monday");
    }

    #[test]
    fn add_business_days_skips_holiday_to_tuesday() {
        // Same anchor, but Monday 2026-07-06 is a holiday -> lands Tuesday.
        let res = add_business_days(utc("2026-07-02T09:00:00Z"), 2, &holidays(&["2026-07-06"]))
            .expect("resolves within the scan bound");
        assert_eq!(res.deadline.date_naive(), date("2026-07-07"), "Tuesday");
    }

    #[test]
    fn add_business_days_n_zero_rolls_forward_off_weekend() {
        // Saturday anchor with n = 0 rolls forward to the next business instant.
        let res = add_business_days(utc("2026-07-04T09:00:00Z"), 0, &holidays(&[]))
            .expect("resolves within the scan bound");
        assert_eq!(res.deadline, utc("2026-07-06T09:00:00Z"));
    }

    #[test]
    fn add_business_days_n_zero_on_business_day_is_identity() {
        let anchor = utc("2026-07-02T09:00:00Z");
        let res = add_business_days(anchor, 0, &holidays(&[])).expect("resolves");
        assert_eq!(
            res.deadline, anchor,
            "n = 0 on a business instant fires at the anchor itself"
        );
        assert!(res.skipped.is_empty(), "nothing was stepped over");
    }

    #[test]
    fn add_business_days_preserves_time_of_day() {
        let res = add_business_days(utc("2026-07-02T14:37:11Z"), 2, &holidays(&[]))
            .expect("resolves within the scan bound");
        assert_eq!(res.deadline, utc("2026-07-06T14:37:11Z"));
    }

    #[test]
    fn add_business_days_records_skipped_dates() {
        let res = add_business_days(utc("2026-07-02T09:00:00Z"), 2, &holidays(&["2026-07-06"]))
            .expect("resolves within the scan bound");
        assert_eq!(
            res.skipped,
            vec![date("2026-07-04"), date("2026-07-05"), date("2026-07-06")],
            "weekend days and the holiday are recorded in scan order"
        );
    }

    #[test]
    fn n_zero_tolerates_a_real_corporate_shutdown() {
        // REVIEW P2 (arithmetic): the derived scan bound was `n * 7 + 14`, so
        // n = 0 examined only 15 days -- yet n = 0 is the ROLL-FORWARD case,
        // which must traverse the LONGEST unbroken non-business run before
        // finding its first business day. The bound scaled backwards from the
        // requirement, so an ordinary corporate winter shutdown (10 weekdays,
        // weekends automatic) spuriously rejected at n = 0 while n = 1 on the
        // SAME anchor and calendar succeeded.
        //
        // This matters because context.rs freezes the rejection into history:
        // recovery would need a workflow reset.
        let shutdown: std::collections::BTreeSet<NaiveDate> = [
            "2025-12-22",
            "2025-12-23",
            "2025-12-24",
            "2025-12-25",
            "2025-12-26",
            "2025-12-29",
            "2025-12-30",
            "2025-12-31",
            "2026-01-01",
            "2026-01-02",
        ]
        .iter()
        .map(|d| date(d))
        .collect();

        // Saturday 2025-12-20: the first business day is Mon 2026-01-05, at
        // calendar-day offset 16 -- just past the old 15-day window.
        let anchor = utc("2025-12-20T09:00:00Z");
        let got = add_business_days(anchor, 0, &shutdown).expect("n = 0 must resolve");
        assert_eq!(
            got.deadline,
            utc("2026-01-05T09:00:00Z"),
            "n = 0 must roll forward across a 10-weekday shutdown"
        );

        // The falsifiability half: n = 1 and n = 2 already worked, so a bound
        // that rejects n = 0 while accepting n = 1 is self-evidently wrong.
        assert!(add_business_days(anchor, 1, &shutdown).is_ok());
        assert!(add_business_days(anchor, 2, &shutdown).is_ok());
    }

    #[test]
    fn coverage_horizon_rejects_a_zero_advance_past_the_horizon() {
        // REVIEW P2 (arithmetic): the horizon check ran only AFTER the cursor
        // advanced, so a resolution that returns without advancing (n = 0 on a
        // business-day anchor) bypassed it entirely -- returning Ok for a date
        // the calendar knows nothing about. That is exactly the "weekends-only
        // guess" the horizon exists to prevent, and it made the outcome depend
        // on the anchor's weekday: a Monday past the horizon silently resolved
        // while a Saturday past the horizon rejected.
        let holidays: std::collections::BTreeSet<NaiveDate> =
            std::iter::once(date("2026-07-03")).collect();
        let horizon = date("2026-12-31");

        // Monday 2027-07-05 -- a real US federal holiday the calendar cannot
        // see. Must REJECT rather than answer "it's a weekday, so business".
        assert_eq!(
            add_business_days_bounded(utc("2027-07-05T09:00:00Z"), 0, &holidays, Some(horizon)),
            Err(BusinessDayRejection::CoverageExhausted {
                coverage_end: horizon
            }),
            "a zero-advance resolution past the horizon must be rejected"
        );

        // The weekday must not change the verdict.
        assert_eq!(
            add_business_days_bounded(utc("2027-07-10T09:00:00Z"), 0, &holidays, Some(horizon)),
            Err(BusinessDayRejection::CoverageExhausted {
                coverage_end: horizon
            }),
        );

        // Regression guard: landing exactly ON the horizon is still ACCEPTED,
        // and an in-coverage anchor is unaffected.
        let on_horizon =
            add_business_days_bounded(utc("2026-12-31T09:00:00Z"), 0, &holidays, Some(horizon))
                .expect("exactly-on-horizon must be accepted");
        assert_eq!(on_horizon.deadline, utc("2026-12-31T09:00:00Z"));
    }

    #[test]
    fn helpers_enforce_max_business_days() {
        // REVIEW P3 (arithmetic): MAX_BUSINESS_DAYS was documented as the bound
        // the business-day HELPERS accept, but only the WorkflowContext caller
        // enforced it. Both helpers are `pub` and re-exported, so an embedder
        // passing u32::MAX drove ~95M iterations accumulating ~108 MB of
        // skipped dates before overflowing. Enforce it where it is documented.
        let empty = std::collections::BTreeSet::new();
        let anchor = utc("2026-07-02T09:00:00Z");

        assert_eq!(
            add_business_days(anchor, MAX_BUSINESS_DAYS + 1, &empty),
            Err(BusinessDayRejection::TooManyBusinessDays {
                requested: MAX_BUSINESS_DAYS + 1,
                max: MAX_BUSINESS_DAYS,
            }),
        );
        assert_eq!(
            add_business_days_bounded(anchor, u32::MAX, &empty, None),
            Err(BusinessDayRejection::TooManyBusinessDays {
                requested: u32::MAX,
                max: MAX_BUSINESS_DAYS,
            }),
        );
        // Exactly at the bound is still accepted (it is a maximum, not a limit
        // one below).
        assert!(add_business_days(anchor, MAX_BUSINESS_DAYS, &empty).is_ok());
    }

    #[test]
    fn add_business_days_scan_exhaustion_rejects() {
        // n = 1 derives a scan bound of 1 * 7 + 30 = 37 calendar days.
        // 60 consecutive exclusions starve the scan before it finds a business day.
        let mut all = std::collections::BTreeSet::new();
        let mut d = date("2026-07-02");
        for _ in 0..60 {
            all.insert(d);
            d = d.succ_opt().expect("in range");
        }
        assert_eq!(
            add_business_days(utc("2026-07-02T09:00:00Z"), 1, &all),
            Err(BusinessDayRejection::ScanExhausted { scanned_days: 37 }),
        );
    }

    #[test]
    fn add_business_days_near_naivedate_max_rejects_without_panic() {
        // `NaiveDate + Duration` panics on overflow; `checked_add_days` must be used.
        assert_eq!(
            add_business_days(chrono::DateTime::<chrono::Utc>::MAX_UTC, 1, &holidays(&[])),
            Err(BusinessDayRejection::DateOverflow),
        );
    }

    #[test]
    fn next_business_day_at_or_after_rolls_weekend_forward() {
        let res = next_business_day_at_or_after(utc("2026-07-04T09:00:00Z"), &holidays(&[]))
            .expect("resolves");
        assert_eq!(res.deadline, utc("2026-07-06T09:00:00Z"));
    }

    #[test]
    fn next_business_day_at_or_after_on_business_day_is_identity() {
        let anchor = utc("2026-07-02T09:00:00Z");
        let res = next_business_day_at_or_after(anchor, &holidays(&[])).expect("resolves");
        assert_eq!(res.deadline, anchor);
    }

    #[test]
    fn max_business_days_is_bounded() {
        assert_eq!(MAX_BUSINESS_DAYS, 3650, "~14 calendar years");
    }

    // ── BusinessCalendars snapshot (issue #806) ───────────────────────────────

    #[test]
    fn business_calendars_distinguishes_unknown_from_empty() {
        // A registered-but-empty calendar is NOT the same as an unknown name:
        // the former means "no holidays, weekends only", the latter is a typo.
        let cals = BusinessCalendars::new().with_calendar("empty-cal", []);
        assert!(
            cals.get("empty-cal").is_some(),
            "registered calendar resolves even with zero exclusions"
        );
        assert!(
            cals.get("empty-cal").expect("registered").is_empty(),
            "and it is genuinely empty"
        );
        assert!(
            cals.get("typo-cal").is_none(),
            "an unregistered name must be None, never an empty set"
        );
    }

    #[test]
    fn business_calendars_builtin_contains_us_federal_and_nyse() {
        let cals = BusinessCalendars::builtin();
        let fed = cals
            .get("us-federal-holidays")
            .expect("built-in us-federal-holidays");
        assert!(fed.contains(&date("2026-12-25")), "Christmas 2026");
        assert!(fed.contains(&date("2025-07-04")), "Independence Day 2025");
        let nyse = cals.get("nyse").expect("built-in nyse");
        assert!(nyse.contains(&date("2026-04-03")), "Good Friday 2026");
        assert!(
            !fed.contains(&date("2026-04-03")),
            "Good Friday is NYSE-only, not a US federal holiday"
        );
    }

    #[test]
    fn business_calendars_coverage_end_is_declared_not_inferred() {
        // A sparse calendar's LAST HOLIDAY says nothing about how far its data
        // extends. Inferring a horizon from it would reject every resolution
        // landing after that holiday -- a false positive on the common case.
        let inferred = BusinessCalendars::new().with_calendar(
            "sparse",
            [date("2026-01-01"), date("2027-06-15"), date("2026-12-25")],
        );
        assert_eq!(
            inferred.coverage_end("sparse"),
            None,
            "with_calendar declares no horizon, even with exclusions present"
        );

        let declared = BusinessCalendars::new().with_calendar_covering(
            "sparse",
            [date("2026-01-01")],
            date("2026-12-31"),
        );
        assert_eq!(declared.coverage_end("sparse"), Some(date("2026-12-31")));

        assert_eq!(
            declared.coverage_end("unknown"),
            None,
            "unknown calendar has no coverage horizon"
        );
    }

    #[test]
    fn builtin_calendar_coverage_ends_2026_12_31() {
        // Documents the live data cliff: the shipped built-in arrays carry
        // 2025 + 2026 holidays, so their data ends 2026-12-31. A resolution
        // needing a 2027 date must be REJECTED, not silently answered
        // weekends-only. See `add_business_days_past_coverage_horizon_rejects`.
        let cals = BusinessCalendars::builtin();
        assert_eq!(
            cals.coverage_end("us-federal-holidays"),
            Some(date("2026-12-31")),
        );
        assert_eq!(cals.coverage_end("nyse"), Some(date("2026-12-31")));
        assert_eq!(
            BusinessCalendars::BUILTIN_COVERAGE_END,
            date("2026-12-31"),
            "bump alongside the holiday arrays when adding a year"
        );
    }

    #[test]
    fn builtin_resolution_into_2027_is_rejected_not_silently_weekends_only() {
        // The headline data-cliff guard, end to end through the built-ins.
        let cals = BusinessCalendars::builtin();
        let res = add_business_days_bounded(
            utc("2026-12-30T09:00:00Z"),
            5,
            cals.get("us-federal-holidays").expect("built-in"),
            cals.coverage_end("us-federal-holidays"),
        );
        assert_eq!(
            res,
            Err(BusinessDayRejection::CoverageExhausted {
                coverage_end: date("2026-12-31")
            }),
        );
    }

    #[test]
    fn business_calendars_from_iter_round_trips() {
        let cals = BusinessCalendars::from_iter([
            ("a".to_string(), vec![date("2026-07-04")]),
            ("b".to_string(), vec![date("2026-12-25")]),
        ]);
        assert!(cals.get("a").expect("a").contains(&date("2026-07-04")));
        assert!(cals.get("b").expect("b").contains(&date("2026-12-25")));
        assert!(cals.get("c").is_none());
    }

    #[test]
    fn business_calendars_with_calendar_last_write_wins() {
        let cals = BusinessCalendars::new()
            .with_calendar("cal", [date("2026-07-04")])
            .with_calendar("cal", [date("2026-12-25")]);
        let set = cals.get("cal").expect("cal");
        assert!(!set.contains(&date("2026-07-04")), "replaced, not merged");
        assert!(set.contains(&date("2026-12-25")));
    }

    // ── coverage-horizon rejection (issue #806) ───────────────────────────────

    #[test]
    fn add_business_days_past_coverage_horizon_rejects() {
        // Anchor sits inside coverage but n = 5 walks the cursor past the last
        // date the calendar has data for -> the answer would be weekends-only
        // guesswork, so it must be a loud, deterministic rejection.
        let coverage_end = date("2026-12-31");
        let res = add_business_days_bounded(
            utc("2026-12-28T09:00:00Z"),
            5,
            &holidays(&["2026-12-25"]),
            Some(coverage_end),
        );
        assert_eq!(
            res,
            Err(BusinessDayRejection::CoverageExhausted { coverage_end }),
        );
    }

    #[test]
    fn add_business_days_within_coverage_horizon_resolves() {
        // 2026-12-28 is a Monday; +2 business days is Wed 12-30, inside the
        // 12-31 horizon, so the same calendar that rejects n = 5 resolves n = 2.
        let res = add_business_days_bounded(
            utc("2026-12-28T09:00:00Z"),
            2,
            &holidays(&["2026-12-25"]),
            Some(date("2026-12-31")),
        )
        .expect("Mon -> Wed is inside coverage");
        assert_eq!(res.deadline.date_naive(), date("2026-12-30"), "Wednesday");
    }

    #[test]
    fn add_business_days_landing_exactly_on_the_horizon_resolves() {
        // The horizon is inclusive: a deadline landing exactly on coverage_end
        // is fully covered and must NOT be rejected.
        let res = add_business_days_bounded(
            utc("2026-12-30T09:00:00Z"),
            1,
            &holidays(&[]),
            Some(date("2026-12-31")),
        )
        .expect("Wed -> Thu lands exactly on the horizon");
        assert_eq!(res.deadline.date_naive(), date("2026-12-31"));
    }

    #[test]
    fn add_business_days_with_no_coverage_horizon_is_unbounded() {
        // A calendar with no exclusions at all imposes no horizon.
        let res = add_business_days_bounded(utc("2030-03-04T09:00:00Z"), 2, &holidays(&[]), None)
            .expect("no horizon -> weekends-only resolution is intended");
        assert_eq!(res.deadline.date_naive(), date("2030-03-06"));
    }

    #[test]
    fn add_business_days_delegates_to_bounded_without_a_horizon() {
        let h = holidays(&["2026-07-06"]);
        assert_eq!(
            add_business_days(utc("2026-07-02T09:00:00Z"), 2, &h),
            add_business_days_bounded(utc("2026-07-02T09:00:00Z"), 2, &h, None),
        );
    }

    // ── apply_skip_policy ─────────────────────────────────────────────────────

    #[test]
    fn apply_skip_policy_non_excluded_returns_same() {
        let exc = excluded(&["2026-07-04"]);
        let d = date("2026-07-05");
        assert_eq!(apply_skip_policy(d, SkipPolicy::Skip, &exc, false), Some(d));
        assert_eq!(
            apply_skip_policy(d, SkipPolicy::RunNextBusinessDay, &exc, false),
            Some(d)
        );
    }

    #[test]
    fn apply_skip_policy_skip_suppresses_excluded() {
        let exc = excluded(&["2026-07-04"]);
        assert_eq!(
            apply_skip_policy(date("2026-07-04"), SkipPolicy::Skip, &exc, false),
            None
        );
    }

    #[test]
    fn apply_skip_policy_next_business_day_finds_next() {
        let exc = excluded(&["2026-07-04"]);
        assert_eq!(
            apply_skip_policy(
                date("2026-07-04"),
                SkipPolicy::RunNextBusinessDay,
                &exc,
                false
            ),
            Some(date("2026-07-05"))
        );
    }

    #[test]
    fn apply_skip_policy_prev_business_day_finds_prev() {
        let exc = excluded(&["2026-07-04"]);
        assert_eq!(
            apply_skip_policy(
                date("2026-07-04"),
                SkipPolicy::RunPrevBusinessDay,
                &exc,
                false
            ),
            Some(date("2026-07-03"))
        );
    }

    #[test]
    fn apply_skip_policy_chains_through_multiple_consecutive_exclusions() {
        let exc = excluded(&["2026-08-06", "2026-08-07", "2026-08-08"]);
        assert_eq!(
            apply_skip_policy(
                date("2026-08-06"),
                SkipPolicy::RunNextBusinessDay,
                &exc,
                false
            ),
            Some(date("2026-08-09"))
        );
    }

    #[test]
    fn apply_skip_policy_prev_chains_through_multiple_consecutive_exclusions() {
        let exc = excluded(&["2026-08-06", "2026-08-07", "2026-08-08"]);
        assert_eq!(
            apply_skip_policy(
                date("2026-08-08"),
                SkipPolicy::RunPrevBusinessDay,
                &exc,
                false
            ),
            Some(date("2026-08-05"))
        );
    }

    #[test]
    fn apply_skip_policy_next_exceeds_365_days_returns_none() {
        let start_date = date("2026-01-01");
        let mut exc = Vec::new();
        // Exclude start_date and the next 365 days
        for i in 0..=365 {
            exc.push(start_date + chrono::Duration::days(i));
        }

        assert_eq!(
            apply_skip_policy(start_date, SkipPolicy::RunNextBusinessDay, &exc, false),
            None
        );
    }

    #[test]
    fn apply_skip_policy_prev_exceeds_365_days_returns_none() {
        let start_date = date("2026-12-31");
        let mut exc = Vec::new();
        // Exclude start_date and the previous 365 days
        for i in 0..=365 {
            exc.push(start_date - chrono::Duration::days(i));
        }

        assert_eq!(
            apply_skip_policy(start_date, SkipPolicy::RunPrevBusinessDay, &exc, false),
            None
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
            let result = plan_backfill_with_calendar(
                Some(&schedule),
                from,
                to,
                100,
                &exc,
                SkipPolicy::Skip,
                false,
            )
            .unwrap();
            let dates: Vec<_> = result.iter().map(|(_, ft)| ft.date_naive()).collect();
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
                false,
            )
            .unwrap();
            let dates: Vec<_> = result.iter().map(|(_, ft)| ft.date_naive()).collect();
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
            let with_cal = plan_backfill_with_calendar(
                Some(&schedule),
                from,
                to,
                100,
                &[],
                SkipPolicy::Skip,
                false,
            )
            .unwrap();
            let with_cal_times: Vec<_> = with_cal.into_iter().map(|(_, ft)| ft).collect();
            assert_eq!(
                plain, with_cal_times,
                "empty exclusion list must not change results"
            );
        }
    }
}
