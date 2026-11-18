//! Diesel wrappers that map a jiff duration to a Postgres `interval`.
//!
//! `jiff-diesel` maps an `interval` to a calendar `jiff::Span` only. Harvest
//! stores elapsed time, so [`PgDuration`] maps it to a
//! [`SignedDuration`]. The rules are the ones that the Diesel `chrono`
//! feature used, so a stored interval reads back to the same value:
//!
//! - A write puts whole days in `days`, the rest in `microseconds`, and
//!   zero in `months`.
//! - A read counts a month as 30 days and a day as 24 hours, as the
//!   Postgres `justify_interval` function does.
//!
//! A timestamp in a query uses `jiff_diesel::Timestamp` through
//! `jiff_diesel::ToDiesel`. A model field of type `Option<Timestamp>` uses
//! [`NullableTimestamp`] here. One derive can then both read and write it.

use diesel::deserialize::{self, FromSql, FromSqlRow};
use diesel::expression::AsExpression;
use diesel::pg::data_types::PgInterval;
use diesel::pg::{Pg, PgValue};
use diesel::serialize::{self, Output, ToSql};
use diesel::sql_types::{Interval, Nullable, Timestamptz};
use jiff::{SignedDuration, Timestamp};

/// Microseconds in one day.
const MICROS_PER_DAY: i64 = 86_400_000_000;
/// Days in one month, as Postgres counts them for `justify_interval`.
const DAYS_PER_MONTH: i64 = 30;

/// A [`SignedDuration`] that Diesel reads from and writes to an `interval`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, AsExpression, FromSqlRow)]
#[diesel(sql_type = Interval)]
pub struct PgDuration(pub SignedDuration);

impl From<SignedDuration> for PgDuration {
    fn from(duration: SignedDuration) -> Self {
        Self(duration)
    }
}

impl From<PgDuration> for SignedDuration {
    fn from(duration: PgDuration) -> Self {
        duration.0
    }
}

/// Converts a duration to the `interval` parts that Diesel `chrono` wrote.
fn to_interval(duration: SignedDuration) -> Result<PgInterval, &'static str> {
    let micros = super::num_microseconds(duration)
        .ok_or("Failed to create microseconds by overflow")?;
    let days = i32::try_from(super::num_days(duration))
        .map_err(|_| "Failed to get i32 days from i64")?;
    Ok(PgInterval {
        microseconds: micros % MICROS_PER_DAY,
        days,
        months: 0,
    })
}

/// Converts `interval` parts to a duration as Diesel `chrono` read them.
fn from_interval(interval: PgInterval) -> SignedDuration {
    let days = i64::from(interval.months) * DAYS_PER_MONTH + i64::from(interval.days);
    super::days(days).saturating_add(SignedDuration::from_micros(interval.microseconds))
}

impl ToSql<Interval, Pg> for PgDuration {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        let interval = to_interval(self.0)?;
        <PgInterval as ToSql<Interval, Pg>>::to_sql(&interval, &mut out.reborrow())
    }
}

impl FromSql<Interval, Pg> for PgDuration {
    fn from_sql(bytes: PgValue<'_>) -> deserialize::Result<Self> {
        let interval: PgInterval = FromSql::<Interval, Pg>::from_sql(bytes)?;
        Ok(Self(from_interval(interval)))
    }
}

/// An optional [`SignedDuration`] for a nullable `interval` column.
///
/// Diesel cannot convert `Option<PgDuration>` to `Option<SignedDuration>`
/// in a `deserialize_as`, so a nullable column takes this wrapper.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, AsExpression, FromSqlRow)]
#[diesel(sql_type = Nullable<Interval>)]
pub struct NullablePgDuration(pub Option<SignedDuration>);

impl From<Option<SignedDuration>> for NullablePgDuration {
    fn from(duration: Option<SignedDuration>) -> Self {
        Self(duration)
    }
}

impl From<SignedDuration> for NullablePgDuration {
    fn from(duration: SignedDuration) -> Self {
        Self(Some(duration))
    }
}

impl From<NullablePgDuration> for Option<SignedDuration> {
    fn from(duration: NullablePgDuration) -> Self {
        duration.0
    }
}

impl ToSql<Nullable<Interval>, Pg> for NullablePgDuration {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        match self.0 {
            Some(duration) => {
                let interval = to_interval(duration)?;
                <PgInterval as ToSql<Interval, Pg>>::to_sql(&interval, &mut out.reborrow())
            }
            None => Ok(serialize::IsNull::Yes),
        }
    }
}

impl FromSql<Nullable<Interval>, Pg> for NullablePgDuration {
    fn from_sql(bytes: PgValue<'_>) -> deserialize::Result<Self> {
        let interval: PgInterval = FromSql::<Interval, Pg>::from_sql(bytes)?;
        Ok(Self(Some(from_interval(interval))))
    }

    fn from_nullable_sql(bytes: Option<PgValue<'_>>) -> deserialize::Result<Self> {
        bytes.map_or(Ok(Self(None)), Self::from_sql)
    }
}

/// Converts a duration to the wrapper that Diesel binds as an `interval`.
///
/// `jiff_diesel::ToDiesel` does the same for a timestamp. That trait is
/// foreign, so it cannot cover a jiff duration here.
pub trait ToPgInterval {
    /// The wrapper type.
    type Target;
    /// Returns the wrapper.
    fn to_pg_interval(self) -> Self::Target;
}

impl ToPgInterval for SignedDuration {
    type Target = PgDuration;
    fn to_pg_interval(self) -> PgDuration {
        PgDuration(self)
    }
}

impl ToPgInterval for Option<SignedDuration> {
    type Target = NullablePgDuration;
    fn to_pg_interval(self) -> NullablePgDuration {
        NullablePgDuration(self)
    }
}

/// An optional [`Timestamp`] for a nullable `timestamptz` model field.
///
/// `jiff_diesel::NullableTimestamp` converts from `Option<Timestamp>` only.
/// An `AsChangeset` derive converts the value inside a `Some`, so it needs a
/// conversion from `Timestamp` as well. This wrapper has both, so one
/// `serialize_as` serves `Insertable` and `AsChangeset` together.
///
/// An `Insertable` field of this type writes `NULL` for `None`, where an
/// unwrapped `Option` writes the column default. Use it only on a column
/// whose default is `NULL`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, AsExpression, FromSqlRow)]
#[diesel(sql_type = Nullable<Timestamptz>)]
pub struct NullableTimestamp(pub Option<Timestamp>);

impl From<Option<Timestamp>> for NullableTimestamp {
    fn from(at: Option<Timestamp>) -> Self {
        Self(at)
    }
}

impl From<Timestamp> for NullableTimestamp {
    fn from(at: Timestamp) -> Self {
        Self(Some(at))
    }
}

impl From<NullableTimestamp> for Option<Timestamp> {
    fn from(at: NullableTimestamp) -> Self {
        at.0
    }
}

impl ToSql<Nullable<Timestamptz>, Pg> for NullableTimestamp {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        match self.0 {
            Some(at) => {
                let at = jiff_diesel::Timestamp::from(at);
                <jiff_diesel::Timestamp as ToSql<Timestamptz, Pg>>::to_sql(&at, &mut out.reborrow())
            }
            None => Ok(serialize::IsNull::Yes),
        }
    }
}

impl FromSql<Nullable<Timestamptz>, Pg> for NullableTimestamp {
    fn from_sql(bytes: PgValue<'_>) -> deserialize::Result<Self> {
        let at: jiff_diesel::Timestamp = FromSql::<Timestamptz, Pg>::from_sql(bytes)?;
        Ok(Self(Some(at.to_jiff())))
    }

    fn from_nullable_sql(bytes: Option<PgValue<'_>>) -> deserialize::Result<Self> {
        bytes.map_or(Ok(Self(None)), Self::from_sql)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_write_splits_days_from_microseconds() {
        let duration = SignedDuration::from_secs(2 * 86_400 + 90) + SignedDuration::from_micros(7);
        let interval = to_interval(duration).ok();
        assert_eq!(
            interval.map(|i| (i.months, i.days, i.microseconds)),
            Some((0, 2, 90_000_007))
        );
    }

    #[test]
    fn a_negative_duration_splits_toward_zero() {
        let interval = to_interval(SignedDuration::from_hours(-25)).ok();
        assert_eq!(
            interval.map(|i| (i.months, i.days, i.microseconds)),
            Some((0, -1, -3_600_000_000))
        );
    }

    #[test]
    fn a_read_counts_a_month_as_thirty_days() {
        let interval = PgInterval { microseconds: 5, days: 1, months: 2 };
        assert_eq!(
            from_interval(interval),
            SignedDuration::from_hours(61 * 24) + SignedDuration::from_micros(5)
        );
    }

    #[test]
    fn a_write_then_a_read_round_trips() {
        for micros in [0, 1, -1, 86_400_000_000, 123_456_789_012, -987_654_321_098] {
            let duration = SignedDuration::from_micros(micros);
            let back = to_interval(duration).map(from_interval).ok();
            assert_eq!(back, Some(duration), "{micros}");
        }
    }

    #[test]
    fn a_duration_past_i64_microseconds_does_not_write() {
        assert!(to_interval(SignedDuration::MAX).is_err());
    }
}
