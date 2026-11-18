//! Date and time helpers over [`jiff`] (ADR 0018).
//!
//! Harvest stores an instant as a [`jiff::Timestamp`] and an elapsed time as
//! a [`jiff::SignedDuration`]. This module holds the few helpers that jiff
//! does not give directly.
//!
//! - [`format`] prints a timestamp byte-for-byte as `chrono` printed it.
//!   Hashed audit records, cursors and API fields depend on those bytes.
//! - The duration helpers keep the `i64` results and truncation rules of
//!   the `chrono` accessors that they replace.
//! - [`cron`] lets `croner` walk a jiff zoned date and time.
//! - `sql` maps a duration to a Postgres `interval` as Diesel mapped one.
//!
//! A timestamp in JSON uses the jiff `serde` format. It is RFC 3339 with a
//! `Z` suffix and the fewest fraction digits that keep the value exact.
//! `chrono` wrote 3, 6 or 9 digits. Each library reads the output of the
//! other, so a stored value stays readable.

pub mod cron;
pub mod format;
#[cfg(feature = "db")]
pub mod sql;

pub use format::{SecondsFormat, rfc3339, rfc3339_opts, utc_display};
pub use jiff::tz::TimeZone;
pub use jiff::{SignedDuration, Timestamp, civil};

/// Returns the IANA time zone `name`, as `chrono-tz` resolved it.
///
/// jiff reads the host database first. A Debian or Ubuntu host without the
/// `tzdata-legacy` package lacks the old link names, such as `US/Eastern`.
/// `chrono-tz` always knew them. So a name that the host lacks falls back
/// to the database that jiff bundles. A stored schedule in such a zone then
/// still fires.
///
/// `chrono-tz` matched a name with its exact case. jiff ignores case. This
/// function refuses a name that differs in case from the zone that it finds,
/// so it accepts the names that `chrono-tz` accepted.
#[must_use]
pub fn zone(name: &str) -> Option<TimeZone> {
    let found = TimeZone::get(name)
        .or_else(|_| jiff::tz::TimeZoneDatabase::bundled().get(name))
        .ok()?;
    (found.iana_name() == Some(name)).then_some(found)
}

/// Seconds in one day of elapsed time.
const SECONDS_PER_DAY: i64 = 86_400;

/// Returns `days` days of elapsed time, as `chrono::Duration::days` did.
///
/// A day here is 24 hours of elapsed time, not a calendar day. A value past
/// the range of [`SignedDuration`] saturates.
#[must_use]
pub const fn days(days: i64) -> SignedDuration {
    SignedDuration::from_secs(days.saturating_mul(SECONDS_PER_DAY))
}

/// Returns the whole days in `duration`, truncated toward zero.
///
/// This matches `chrono::Duration::num_days`.
#[must_use]
pub const fn num_days(duration: SignedDuration) -> i64 {
    duration.as_secs() / SECONDS_PER_DAY
}

/// Returns the whole milliseconds in `duration`, truncated toward zero.
///
/// This matches `chrono::Duration::num_milliseconds`. A `chrono` duration
/// always fit in `i64` milliseconds. A jiff duration can be longer, so a
/// value past that range saturates.
#[must_use]
pub fn num_milliseconds(duration: SignedDuration) -> i64 {
    saturate_i128(duration.as_millis())
}

/// Returns the whole microseconds in `duration`, or `None` past `i64`.
///
/// This matches `chrono::Duration::num_microseconds`.
#[must_use]
pub fn num_microseconds(duration: SignedDuration) -> Option<i64> {
    i64::try_from(duration.as_micros()).ok()
}

/// Returns the nanoseconds in `duration`, or `None` past `i64`.
///
/// This matches `chrono::Duration::num_nanoseconds`.
#[must_use]
pub fn num_nanoseconds(duration: SignedDuration) -> Option<i64> {
    i64::try_from(duration.as_nanos()).ok()
}

/// Clamps an `i128` count to the `i64` range.
fn saturate_i128(value: i128) -> i64 {
    i64::try_from(value).unwrap_or(if value < 0 { i64::MIN } else { i64::MAX })
}

/// Returns the UTC instant of a calendar date and wall clock time.
///
/// This replaces `Utc.with_ymd_and_hms(..).single()`. It returns `None` for
/// a field out of range, or for a year outside the range of jiff.
#[must_use]
pub fn utc_ymd_hms(
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
) -> Option<Timestamp> {
    let date = date_ymd(year, month, day)?;
    let time = civil::Time::new(
        i8::try_from(hour).ok()?,
        i8::try_from(minute).ok()?,
        i8::try_from(second).ok()?,
        0,
    )
    .ok()?;
    date.to_datetime(time)
        .to_zoned_datetime(TimeZone::UTC)
        .ok()
        .map(|zoned| zoned.timestamp())
}

/// Returns a calendar date, or `None` for a field out of range.
///
/// This replaces `chrono::NaiveDate::from_ymd_opt`.
#[must_use]
pub fn date_ymd(year: i32, month: u32, day: u32) -> Option<civil::Date> {
    civil::Date::new(
        i16::try_from(year).ok()?,
        i8::try_from(month).ok()?,
        i8::try_from(day).ok()?,
    )
    .ok()
}

/// Returns the UTC calendar date of `at`.
///
/// This replaces `DateTime::<Utc>::date_naive`.
#[must_use]
pub fn utc_date(at: Timestamp) -> civil::Date {
    TimeZone::UTC.to_datetime(at).date()
}

/// Returns the UTC wall clock date and time of `at`.
///
/// This replaces `DateTime::<Utc>::naive_utc`.
#[must_use]
pub fn utc_datetime(at: Timestamp) -> civil::DateTime {
    TimeZone::UTC.to_datetime(at)
}

/// Returns the instant of a wall clock date and time read as UTC.
///
/// This replaces `NaiveDateTime::and_utc`. It returns `None` only at the
/// edge of the jiff range.
#[must_use]
pub fn from_utc_datetime(datetime: civil::DateTime) -> Option<Timestamp> {
    TimeZone::UTC.to_timestamp(datetime).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zone_resolves_legacy_links_and_keeps_case() {
        assert!(zone("Europe/Paris").is_some());
        assert!(zone("US/Eastern").is_some());
        assert!(zone("UTC").is_some());
        assert!(zone("europe/paris").is_none());
        assert!(zone("utc").is_none());
        assert!(zone("Nope/Zone").is_none());
        assert!(zone("").is_none());
    }

    #[test]
    fn days_is_whole_days_of_elapsed_time() {
        assert_eq!(days(2), SignedDuration::from_hours(48));
        assert_eq!(days(-1), SignedDuration::from_hours(-24));
        assert_eq!(days(i64::MAX), SignedDuration::from_secs(i64::MAX));
    }

    #[test]
    fn num_days_truncates_toward_zero() {
        assert_eq!(num_days(SignedDuration::from_hours(47)), 1);
        assert_eq!(num_days(SignedDuration::from_hours(-47)), -1);
        assert_eq!(num_days(SignedDuration::ZERO), 0);
    }

    #[test]
    fn num_milliseconds_truncates_and_saturates() {
        assert_eq!(num_milliseconds(SignedDuration::from_micros(1_999)), 1);
        assert_eq!(num_milliseconds(SignedDuration::from_micros(-1_999)), -1);
        assert_eq!(num_milliseconds(SignedDuration::MAX), i64::MAX);
        assert_eq!(num_milliseconds(SignedDuration::MIN), i64::MIN);
    }

    #[test]
    fn num_micros_and_nanos_overflow_to_none() {
        assert_eq!(num_microseconds(SignedDuration::from_millis(3)), Some(3_000));
        assert_eq!(num_microseconds(SignedDuration::MAX), None);
        assert_eq!(num_nanoseconds(SignedDuration::from_secs(1)), Some(1_000_000_000));
        assert_eq!(num_nanoseconds(SignedDuration::from_secs(10_000_000_000)), None);
    }

    #[test]
    fn utc_ymd_hms_builds_a_utc_instant() {
        let at = utc_ymd_hms(2023, 11, 14, 22, 13, 20);
        assert_eq!(at, Some(Timestamp::constant(1_700_000_000, 0)));
    }

    #[test]
    fn utc_ymd_hms_rejects_out_of_range_fields() {
        assert_eq!(utc_ymd_hms(2023, 2, 30, 0, 0, 0), None);
        assert_eq!(utc_ymd_hms(2023, 13, 1, 0, 0, 0), None);
        assert_eq!(utc_ymd_hms(2023, 1, 1, 24, 0, 0), None);
        assert_eq!(utc_ymd_hms(20_000, 1, 1, 0, 0, 0), None);
    }

    #[test]
    fn date_helpers_round_trip_through_utc() {
        let at = Timestamp::constant(1_700_000_000, 5);
        assert_eq!(utc_date(at), civil::date(2023, 11, 14));
        assert_eq!(from_utc_datetime(utc_datetime(at)), Some(at));
        assert_eq!(date_ymd(2024, 2, 29), Some(civil::date(2024, 2, 29)));
        assert_eq!(date_ymd(2023, 2, 29), None);
    }
}
