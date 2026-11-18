//! A `serde` reader that clamps a stored timestamp into the jiff range.
//!
//! `chrono` wrote a year outside 0 to 9999 with a sign and four or more
//! digits, such as `+12026-01-01T00:00:00Z` or `-0001-01-01T00:00:00Z`.
//! jiff reads a signed year only with six digits, in the range -9999 to
//! 9999. This reader rewrites such a year to the jiff form. A year past the
//! jiff range reads as [`Timestamp::MIN`] or [`Timestamp::MAX`], as the
//! Diesel readers in `time::sql` do. A stored event then still loads.
//!
//! Use it with `#[serde(deserialize_with = "...")]`. The writer stays the
//! jiff default.

use jiff::Timestamp;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer};

/// Reads a timestamp and clamps a year past the jiff range.
///
/// # Errors
///
/// Returns the error of the deserializer, or an error for text that is not
/// a timestamp in the jiff or the `chrono` form.
pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Timestamp, D::Error> {
    let text = String::deserialize(deserializer)?;
    parse(&text).map_err(D::Error::custom)
}

/// Reads an optional timestamp and clamps a year past the jiff range.
pub mod option {
    use super::{Deserialize, Deserializer, Timestamp, parse};
    use serde::de::Error as _;

    /// Reads `null` as `None`, and a timestamp as [`super::deserialize`] does.
    ///
    /// # Errors
    ///
    /// Returns the error of the deserializer, or an error for text that is
    /// not a timestamp in the jiff or the `chrono` form.
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Timestamp>, D::Error> {
        Option::<String>::deserialize(deserializer)?
            .map(|text| parse(&text).map_err(D::Error::custom))
            .transpose()
    }
}

/// Parses `text` in the jiff form, or in the signed-year form of `chrono`.
///
/// # Errors
///
/// Returns the jiff parse error when `text` is in neither form.
pub fn parse(text: &str) -> Result<Timestamp, jiff::Error> {
    let error = match text.parse::<Timestamp>() {
        Ok(at) => return Ok(at),
        Err(error) => error,
    };
    let Some(sign) = text.chars().next().filter(|c| matches!(c, '+' | '-')) else {
        return Err(error);
    };
    let rest = &text[1..];
    let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
    if digits < 4 || rest.as_bytes().get(digits) != Some(&b'-') {
        return Err(error);
    }
    let tail = &rest[digits..];
    let year = rest[..digits].parse::<u64>().unwrap_or(u64::MAX);
    if year <= 9999 {
        return format!("{sign}{year:06}{tail}").parse::<Timestamp>();
    }
    // Year 2000 is a leap year, so a valid 29 February still checks here.
    format!("2000{tail}").parse::<Timestamp>()?;
    Ok(if sign == '-' { Timestamp::MIN } else { Timestamp::MAX })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Deserialize)]
    struct Row {
        #[serde(deserialize_with = "deserialize")]
        at: Timestamp,
        #[serde(default, deserialize_with = "option::deserialize")]
        maybe: Option<Timestamp>,
    }

    fn row(json: &str) -> Row {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn reads_the_jiff_and_the_chrono_forms() {
        let at = Timestamp::constant(1_700_000_000, 123_000_000);
        assert_eq!(row(r#"{"at":"2023-11-14T22:13:20.123Z"}"#).at, at);
        assert_eq!(row(r#"{"at":"2023-11-14T22:13:20.123000Z"}"#).at, at);
        assert_eq!(row(r#"{"at":"2023-11-14T22:13:20.123Z"}"#).maybe, None);
        assert_eq!(row(r#"{"at":"2023-11-14T22:13:20.123Z","maybe":null}"#).maybe, None);
    }

    #[test]
    fn clamps_a_chrono_year_past_the_jiff_range() {
        let far = row(r#"{"at":"+12026-01-01T00:00:00Z","maybe":"+262142-12-31T23:59:59.999999999Z"}"#);
        assert_eq!(far.at, Timestamp::MAX);
        assert_eq!(far.maybe, Some(Timestamp::MAX));
        assert_eq!(row(r#"{"at":"-12026-01-01T00:00:00Z"}"#).at, Timestamp::MIN);
    }

    #[test]
    fn rewrites_a_chrono_negative_year_inside_the_jiff_range() {
        let expected: Timestamp = "-000001-01-01T00:00:00Z".parse().unwrap();
        assert_eq!(parse("-0001-01-01T00:00:00Z").unwrap(), expected);
    }

    #[test]
    fn refuses_text_that_is_not_a_timestamp() {
        assert!(parse("+12026-13-01T00:00:00Z").is_err());
        assert!(parse("+12026").is_err());
        assert!(parse("tomorrow").is_err());
        assert!(serde_json::from_str::<Row>(r#"{"at":"+12026-01-01"}"#).is_err());
    }
}
