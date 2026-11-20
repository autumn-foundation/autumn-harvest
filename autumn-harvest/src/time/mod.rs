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
//! - [`parse`] reads input text that `chrono` read, such as a request field.
//! - `sql` maps a duration to a Postgres `interval` as Diesel mapped one.
//!
//! A timestamp in JSON uses the jiff `serde` format. It is RFC 3339 with a
//! `Z` suffix and the fewest fraction digits that keep the value exact.
//! `chrono` wrote 3, 6 or 9 digits. Each library reads the output of the
//! other, so a stored value stays readable. [`serde_clamped`] also reads a
//! stored year past the jiff range.

pub mod cron;
pub mod format;
pub mod parse;
pub mod serde_clamped;
pub mod serde_secs_nanos;
// The Postgres wrappers live in a flat file. The SQLite feasibility
// inventory scans `src` one level deep, so it records their coupling.
#[cfg(feature = "db")]
#[path = "../time_sql.rs"]
pub mod sql;

pub use format::{SecondsFormat, rfc3339, rfc3339_opts, utc_display};
pub use jiff::tz::TimeZone;
pub use jiff::{SignedDuration, Timestamp, civil};

/// Returns the IANA time zone `name`, as `chrono-tz` resolved it.
///
/// A name is valid only if the database that jiff bundles holds it with the
/// exact case. `chrono-tz` matched a name with its exact case too, and jiff
/// ignores case. The bundled names are the 597 names of `chrono-tz` 0.10,
/// plus `Factory`, which `chrono-tz` left out, so this function refuses it.
/// A host database can also hold `localtime` and `posixrules`. They follow
/// the zone of each host, so two hosts could disagree on one schedule.
///
/// The host database supplies the rules of a valid name when it holds the
/// name. A Debian or Ubuntu host without the `tzdata-legacy` package lacks
/// the old link names, such as `US/Eastern`. The bundled copy then supplies
/// the rules, so a stored schedule in such a zone still fires.
#[must_use]
pub fn zone(name: &str) -> Option<TimeZone> {
    if name == NOT_IN_CHRONO_TZ {
        return None;
    }
    let bundled = jiff::tz::TimeZoneDatabase::bundled()
        .get(name)
        .ok()
        .filter(|found| found.iana_name() == Some(name))?;
    match TimeZone::get(name) {
        Ok(host) if host.iana_name() == Some(name) => Some(host),
        _ => Some(bundled),
    }
}

/// The one bundled zone name that `chrono-tz` did not accept.
const NOT_IN_CHRONO_TZ: &str = "Factory";

/// The longest duration that `chrono::Duration::from_std` accepted.
///
/// That bound is `i64::MAX` milliseconds, about 292 million years.
pub const CHRONO_MAX_DURATION: SignedDuration = SignedDuration::from_millis(i64::MAX);

/// Converts a standard duration as `chrono::Duration::from_std(d).ok()` did.
///
/// `SignedDuration::try_from` accepts up to `i64::MAX` seconds. `chrono`
/// refused a value past `i64::MAX` milliseconds. Code that dropped or
/// replaced such a value keeps that result through this function.
#[must_use]
pub fn from_std_bounded(duration: std::time::Duration) -> Option<SignedDuration> {
    SignedDuration::try_from(duration)
        .ok()
        .filter(|converted| *converted <= CHRONO_MAX_DURATION)
}

/// The first second that `chrono` held, `DateTime::<Utc>::MIN_UTC`.
const CHRONO_MIN_UTC_SECOND: i64 = -8_334_601_228_800;

/// The last second that `chrono` held, `DateTime::<Utc>::MAX_UTC`.
const CHRONO_MAX_UTC_SECOND: i64 = 8_210_266_876_799;

/// Adds `duration` to `at`, as `checked_add_signed` did in `chrono`.
///
/// A sum inside the jiff range gives that sum. A sum past the jiff range,
/// but inside the `chrono` range, saturates to [`Timestamp::MAX`] or
/// [`Timestamp::MIN`]. `chrono` held that sum, so a far deadline stays far.
/// A sum past the `chrono` range gives `None`, as `checked_add_signed` did.
#[must_use]
pub fn checked_add_chrono_range(at: Timestamp, duration: SignedDuration) -> Option<Timestamp> {
    // The sum is exact in nanoseconds. An `i128` holds it with no overflow.
    let sum = at.as_nanosecond() + duration.as_nanos();
    let first = i128::from(CHRONO_MIN_UTC_SECOND) * NANOS_PER_SECOND;
    let last = i128::from(CHRONO_MAX_UTC_SECOND) * NANOS_PER_SECOND + (NANOS_PER_SECOND - 1);
    match at.checked_add(duration) {
        Ok(sum) => Some(sum),
        Err(_) if duration.is_positive() && sum <= last => Some(Timestamp::MAX),
        Err(_) if duration.is_negative() && sum >= first => Some(Timestamp::MIN),
        Err(_) => None,
    }
}

/// Splits `at` into whole seconds and nanoseconds, as `chrono` did.
///
/// `chrono` floored the seconds, so the fraction is never negative. jiff
/// truncates both parts toward zero, so before 1970 its fraction is
/// negative. An id that `chrono` built keeps its bytes through this split.
#[must_use]
pub fn chrono_split(at: Timestamp) -> (i64, u32) {
    let nanos = at.subsec_nanosecond();
    if nanos < 0 {
        (at.as_second() - 1, (nanos + 1_000_000_000).unsigned_abs())
    } else {
        (at.as_second(), nanos.unsigned_abs())
    }
}

/// Nanoseconds in one second.
const NANOS_PER_SECOND: i128 = 1_000_000_000;

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

/// Returns the instant `millis` milliseconds after the Unix epoch, clamped
/// into the jiff range.
///
/// Use it to read a stored value. `chrono` could store an instant up to the
/// year 262142, for example its `MAX_UTC` sentinel. jiff ends in the year
/// 9999. A stored value past either end reads as [`Timestamp::MIN`] or
/// [`Timestamp::MAX`], so an old row still loads.
#[must_use]
pub fn from_millisecond_clamped(millis: i64) -> Timestamp {
    Timestamp::from_millisecond(millis).unwrap_or(if millis < 0 {
        Timestamp::MIN
    } else {
        Timestamp::MAX
    })
}

/// Returns the instant `micros` microseconds after the Unix epoch, clamped
/// into the jiff range. See [`from_millisecond_clamped`].
#[must_use]
pub fn from_microsecond_clamped(micros: i64) -> Timestamp {
    Timestamp::from_microsecond(micros).unwrap_or(if micros < 0 {
        Timestamp::MIN
    } else {
        Timestamp::MAX
    })
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
    fn zone_refuses_names_that_chrono_tz_refused() {
        assert!(zone("localtime").is_none());
        assert!(zone("posixrules").is_none());
        assert!(zone("Factory").is_none());
        assert!(zone("America/New_York").is_some());
    }

    #[test]
    fn clamped_reads_keep_old_chrono_sentinels_loadable() {
        // chrono's `DateTime::<Utc>::MAX_UTC.timestamp_millis()`.
        assert_eq!(
            from_millisecond_clamped(8_210_266_876_799_999),
            Timestamp::MAX
        );
        assert_eq!(from_millisecond_clamped(i64::MIN), Timestamp::MIN);
        assert_eq!(from_microsecond_clamped(i64::MAX), Timestamp::MAX);
        assert_eq!(
            from_millisecond_clamped(1_700_000_000_123),
            Timestamp::constant(1_700_000_000, 123_000_000)
        );
        assert_eq!(
            from_microsecond_clamped(-1),
            Timestamp::constant(-1, 999_999_000)
        );
    }

    #[test]
    fn from_std_bounded_keeps_the_chrono_range() {
        let max_ms = std::time::Duration::from_millis(u64::try_from(i64::MAX).unwrap());
        assert_eq!(from_std_bounded(max_ms), Some(CHRONO_MAX_DURATION));
        assert_eq!(
            from_std_bounded(max_ms + std::time::Duration::from_millis(1)),
            None
        );
        assert_eq!(
            from_std_bounded(std::time::Duration::from_secs(i64::MAX as u64)),
            None
        );
        assert_eq!(
            from_std_bounded(std::time::Duration::from_secs(5)),
            Some(SignedDuration::from_secs(5))
        );
    }

    #[test]
    fn checked_add_chrono_range_saturates_inside_the_chrono_range() {
        let now = Timestamp::constant(1_700_000_000, 0);
        let hour = SignedDuration::from_hours(1);
        assert_eq!(checked_add_chrono_range(now, hour), Some(now + hour));
        let ten_thousand_years = SignedDuration::from_secs(315_576_000_000);
        assert_eq!(
            checked_add_chrono_range(now, ten_thousand_years),
            Some(Timestamp::MAX)
        );
        let past_chrono = SignedDuration::from_secs(9_000_000_000_000);
        assert_eq!(checked_add_chrono_range(now, past_chrono), None);
        let back_twenty_thousand_years = SignedDuration::from_secs(-631_152_000_000);
        assert_eq!(
            checked_add_chrono_range(now, back_twenty_thousand_years),
            Some(Timestamp::MIN)
        );
        assert_eq!(checked_add_chrono_range(now, -past_chrono), None);
        assert_eq!(checked_add_chrono_range(now, SignedDuration::MAX), None);
        assert_eq!(checked_add_chrono_range(now, SignedDuration::MIN), None);
    }

    #[test]
    fn checked_add_chrono_range_keeps_the_nanoseconds_at_the_chrono_ends() {
        let past_min = SignedDuration::new(-8_334_601_228_800, -1);
        assert_eq!(
            checked_add_chrono_range(Timestamp::UNIX_EPOCH, past_min),
            None
        );
        let at_min = SignedDuration::from_secs(-8_334_601_228_800);
        assert_eq!(
            checked_add_chrono_range(Timestamp::UNIX_EPOCH, at_min),
            Some(Timestamp::MIN)
        );
        let half = Timestamp::new(0, 500_000_000).expect("in range");
        let past_max = SignedDuration::new(8_210_266_876_799, 600_000_000);
        assert_eq!(checked_add_chrono_range(half, past_max), None);
        let before_epoch = Timestamp::new(-1, -900_000_000).expect("in range");
        let inside = SignedDuration::from_secs(8_210_266_876_801);
        assert_eq!(
            checked_add_chrono_range(before_epoch, inside),
            Some(Timestamp::MAX)
        );
        let late = Timestamp::new(1_700_000_000, 600_000_000).expect("in range");
        let over = SignedDuration::new(8_208_566_876_799, 700_000_000);
        assert_eq!(checked_add_chrono_range(late, over), None);
    }

    #[test]
    fn chrono_split_floors_the_seconds_before_the_epoch() {
        let half_before = Timestamp::new(0, -500_000_000).expect("in range");
        assert_eq!(chrono_split(half_before), (-1, 500_000_000));
        let earlier = Timestamp::new(-1, -750_000_000).expect("in range");
        assert_eq!(chrono_split(earlier), (-2, 250_000_000));
        assert_eq!(
            chrono_split(Timestamp::new(-3, 0).expect("in range")),
            (-3, 0)
        );
        let after = Timestamp::new(1_700_000_000, 5).expect("in range");
        assert_eq!(chrono_split(after), (1_700_000_000, 5));
        assert_eq!(chrono_split(Timestamp::MIN).1, 0);
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
        assert_eq!(
            num_microseconds(SignedDuration::from_millis(3)),
            Some(3_000)
        );
        assert_eq!(num_microseconds(SignedDuration::MAX), None);
        assert_eq!(
            num_nanoseconds(SignedDuration::from_secs(1)),
            Some(1_000_000_000)
        );
        assert_eq!(
            num_nanoseconds(SignedDuration::from_secs(10_000_000_000)),
            None
        );
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
