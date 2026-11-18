//! A `serde` format for a duration that keeps the `chrono` wire form.
//!
//! `chrono` wrote a `TimeDelta` as the pair `[seconds, nanoseconds]`. The
//! seconds round toward negative infinity, and the nanoseconds are in
//! `0..1_000_000_000`. jiff writes a `SignedDuration` as an ISO 8601 string,
//! such as `PT1H`. The two forms cannot read each other. A model that the
//! API returns, or that an older process reads, keeps the pair through this
//! module.
//!
//! Use it with `#[serde(with = "...")]`, or with the [`option`] module for
//! an `Option<SignedDuration>`.

use jiff::SignedDuration;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::CHRONO_MAX_DURATION;

/// Nanoseconds in one second.
const NANOS_PER_SECOND: i32 = 1_000_000_000;

/// Splits `duration` into the floored seconds and the nanoseconds of `chrono`.
fn parts(duration: SignedDuration) -> (i64, i32) {
    let (seconds, nanos) = (duration.as_secs(), duration.subsec_nanos());
    if nanos < 0 {
        (seconds - 1, nanos + NANOS_PER_SECOND)
    } else {
        (seconds, nanos)
    }
}

/// Builds a duration from a pair, with the bounds of `TimeDelta::new`.
fn from_parts<E: serde::de::Error>((seconds, nanos): (i64, i32)) -> Result<SignedDuration, E> {
    if !(0..NANOS_PER_SECOND).contains(&nanos) {
        return Err(E::custom("TimeDelta out of bounds"));
    }
    let duration = SignedDuration::new(seconds, nanos);
    if duration > CHRONO_MAX_DURATION || duration < -CHRONO_MAX_DURATION {
        return Err(E::custom("TimeDelta out of bounds"));
    }
    Ok(duration)
}

/// Writes `duration` as `[seconds, nanoseconds]`.
///
/// # Errors
///
/// Returns the error of the serializer.
pub fn serialize<S: Serializer>(duration: &SignedDuration, serializer: S) -> Result<S::Ok, S::Error> {
    parts(*duration).serialize(serializer)
}

/// Reads `[seconds, nanoseconds]`, as `chrono` read a `TimeDelta`.
///
/// # Errors
///
/// Returns the error of the deserializer, or an error for a pair outside
/// the `chrono` range.
pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<SignedDuration, D::Error> {
    from_parts(<(i64, i32)>::deserialize(deserializer)?)
}

/// The same format for an `Option<SignedDuration>`. `None` is `null`.
pub mod option {
    use super::{Deserialize, Deserializer, Serialize, Serializer, SignedDuration, from_parts, parts};

    /// Writes `Some(duration)` as `[seconds, nanoseconds]`, and `None` as `null`.
    ///
    /// # Errors
    ///
    /// Returns the error of the serializer.
    #[allow(clippy::ref_option)]
    pub fn serialize<S: Serializer>(
        duration: &Option<SignedDuration>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        duration.map(parts).serialize(serializer)
    }

    /// Reads `null` as `None`, and a pair as [`super::deserialize`] does.
    ///
    /// # Errors
    ///
    /// Returns the error of the deserializer, or an error for a pair outside
    /// the `chrono` range.
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<SignedDuration>, D::Error> {
        Option::<(i64, i32)>::deserialize(deserializer)?
            .map(from_parts)
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Row {
        #[serde(with = "super")]
        bare: SignedDuration,
        #[serde(default, with = "option")]
        maybe: Option<SignedDuration>,
    }

    #[test]
    fn writes_the_chrono_pair() {
        let row = Row {
            bare: SignedDuration::from_hours(1),
            maybe: Some(SignedDuration::from_nanos(-1)),
        };
        let json = serde_json::to_string(&row).unwrap();
        assert_eq!(json, r#"{"bare":[3600,0],"maybe":[-1,999999999]}"#);
        assert_eq!(serde_json::from_str::<Row>(&json).unwrap(), row);
        let none = Row { bare: SignedDuration::ZERO, maybe: None };
        assert_eq!(serde_json::to_string(&none).unwrap(), r#"{"bare":[0,0],"maybe":null}"#);
    }

    #[test]
    fn reads_a_missing_option_as_none() {
        let row: Row = serde_json::from_str(r#"{"bare":[1,500000000]}"#).unwrap();
        assert_eq!(row.bare, SignedDuration::from_millis(1_500));
        assert_eq!(row.maybe, None);
    }

    #[test]
    fn refuses_a_pair_that_chrono_refused() {
        assert!(serde_json::from_str::<Row>(r#"{"bare":[0,-1]}"#).is_err());
        assert!(serde_json::from_str::<Row>(r#"{"bare":[0,1000000000]}"#).is_err());
        assert!(serde_json::from_str::<Row>(r#"{"bare":[9223372036854775807,0]}"#).is_err());
        assert!(serde_json::from_str::<Row>(r#"{"bare":"PT1H"}"#).is_err());
        let max = r#"{"bare":[9223372036854775,807000000]}"#;
        assert_eq!(serde_json::from_str::<Row>(max).unwrap().bare, CHRONO_MAX_DURATION);
    }
}
