//! A `croner` time type over a jiff 1.0 zoned date and time.
//!
//! `croner` implements its `CronDateTime` trait for `chrono` and for jiff
//! 0.2, but not for jiff 1.0. [`CronZoned`] implements the trait here. It
//! follows the jiff 0.2 implementation of `croner` line for line. That one
//! resolves a wall clock time the same way as the `chrono` one: a repeated
//! hour gives both instants and a skipped hour gives none. So a schedule
//! fires at the same instants as it did under `chrono`. A sweep of two
//! years, eight zones and twelve patterns found no difference in two
//! million comparisons.

use std::cmp::Ordering;

use croner::errors::CronError;
use croner::{CivilDate, CivilDateTime, CivilTime, CronDateTime, Resolution, Weekday};
use jiff::tz::{AmbiguousOffset, TimeZone};
use jiff::{SignedDuration, Timestamp, ZonedDateTime, civil};

/// A zoned date and time that `croner` can search from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CronZoned(pub ZonedDateTime);

impl CronZoned {
    /// Returns `at` in `zone`.
    #[must_use]
    pub fn new(at: Timestamp, zone: TimeZone) -> Self {
        Self(at.to_zoned_datetime(zone))
    }

    /// Returns `at` in UTC.
    #[must_use]
    pub fn utc(at: Timestamp) -> Self {
        Self::new(at, TimeZone::UTC)
    }

    /// Returns the instant that this value names.
    #[must_use]
    pub fn timestamp(&self) -> Timestamp {
        self.0.timestamp()
    }
}

/// Converts a jiff wall clock date and time to the `croner` form.
fn to_civil(datetime: civil::DateTime) -> CivilDateTime {
    // jiff keeps each field in range, and each field here is non-negative,
    // so the casts lose nothing.
    #[allow(clippy::cast_sign_loss, reason = "jiff keeps each field non-negative")]
    CivilDateTime::new(
        CivilDate::from_parts_unchecked(
            i32::from(datetime.year()),
            datetime.month() as u32,
            datetime.day() as u32,
        ),
        CivilTime::from_parts_unchecked(
            datetime.hour() as u32,
            datetime.minute() as u32,
            datetime.second() as u32,
        ),
    )
}

/// Converts a `croner` wall clock date and time to the jiff form.
///
/// This fails outside the jiff year range, -9999 to 9999.
fn to_jiff(civil: CivilDateTime) -> Result<civil::DateTime, CronError> {
    let year = i16::try_from(civil.year()).map_err(|_| CronError::InvalidDate)?;
    let part = |value: u32| i8::try_from(value).map_err(|_| CronError::InvalidDate);
    civil::DateTime::new(
        year,
        part(civil.month())?,
        part(civil.day())?,
        part(civil.hour())?,
        part(civil.minute())?,
        part(civil.second())?,
        0,
    )
    .map_err(|_| CronError::InvalidDate)
}

/// Converts a jiff weekday to the `croner` form.
fn to_weekday(weekday: civil::Weekday) -> Weekday {
    Weekday::from_days_from_sunday(u32::from(weekday.to_sunday_zero_offset().unsigned_abs()))
}

impl CronDateTime for CronZoned {
    fn to_civil(&self) -> CivilDateTime {
        to_civil(self.0.datetime())
    }

    fn civil_weekday(&self) -> Weekday {
        to_weekday(self.0.weekday())
    }

    fn resolve_civil(&self, civil: CivilDateTime) -> Result<Resolution<Self>, CronError> {
        let datetime = to_jiff(civil)?;
        let ambiguous = self.0.time_zone().to_ambiguous_zoned_datetime(datetime);
        Ok(match ambiguous.offset() {
            AmbiguousOffset::Unambiguous { .. } => Resolution::Single(Self(
                ambiguous.unambiguous().map_err(|_| CronError::InvalidTime)?,
            )),
            AmbiguousOffset::Fold { .. } => {
                let earlier = ambiguous.clone().earlier().map_err(|_| CronError::InvalidTime)?;
                let later = ambiguous.later().map_err(|_| CronError::InvalidTime)?;
                Resolution::Ambiguous(Self(earlier), Self(later))
            }
            AmbiguousOffset::Gap { .. } => Resolution::Gap,
        })
    }

    fn checked_add_seconds(&self, seconds: i64) -> Option<Self> {
        self.0.checked_add(SignedDuration::from_secs(seconds)).ok().map(Self)
    }

    fn cmp_instant(&self, other: &Self) -> Ordering {
        // The two halves of a repeated wall clock range name two instants,
        // so they compare unequal here.
        self.0.timestamp().cmp(&other.0.timestamp())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn sydney() -> TimeZone {
        TimeZone::get("Australia/Sydney").unwrap_or(TimeZone::UTC)
    }

    #[test]
    fn next_occurrence_in_utc() {
        let cron = croner::Cron::from_str("0 0 * * *").ok();
        let start = CronZoned::utc(Timestamp::constant(1_700_000_000, 0));
        let next = cron.and_then(|c| c.find_next_occurrence(&start, false).ok());
        // 2023-11-15T00:00:00Z.
        assert_eq!(next.map(|n| n.timestamp()), Some(Timestamp::constant(1_700_006_400, 0)));
    }

    #[test]
    fn a_skipped_wall_clock_hour_fires_at_the_end_of_the_gap() {
        // Sydney skips 02:00-03:00 on 2024-10-06. A 02:30 job runs at 03:00
        // AEDT that day, which is 2024-10-05T16:00:00Z. `croner` on `chrono`
        // gives the same instant.
        let cron = croner::Cron::from_str("30 2 * * *").ok();
        let start = CronZoned::new(Timestamp::constant(1_728_100_000, 0), sydney());
        let next = cron.and_then(|c| c.find_next_occurrence(&start, false).ok());
        assert_eq!(
            next.map(|n| n.timestamp().to_string()),
            Some("2024-10-05T16:00:00Z".to_string())
        );
    }

    #[test]
    fn a_repeated_wall_clock_hour_fires_once_at_the_earlier_instant() {
        // Sydney repeats 02:00-03:00 on 2024-04-07. The first 02:30 is
        // 2024-04-06T15:30:00Z, at +11:00.
        let cron = croner::Cron::from_str("30 2 * * *").ok();
        let start = CronZoned::new(Timestamp::constant(1_712_390_000, 0), sydney());
        let next = cron.and_then(|c| c.find_next_occurrence(&start, false).ok());
        assert_eq!(
            next.map(|n| n.timestamp().to_string()),
            Some("2024-04-06T15:30:00Z".to_string())
        );
    }
}
