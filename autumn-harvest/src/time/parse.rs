//! Parses text into jiff values with the grammar that `chrono` 0.4 read.
//!
//! jiff and `chrono` accept different sets of strings. jiff refuses some
//! inputs that `chrono` read. Examples are `9999-12-31T23:59:59Z`, a
//! fraction of more than 9 digits and the `chrono` display form
//! `2026-12-01 09:00:00 UTC`. A client that sent such a value before the
//! upgrade must still get the same result.
//!
//! [`rfc3339`] and [`relaxed`] try jiff first. When jiff refuses the text,
//! they try the `chrono` grammar. So an instant that either library read
//! still parses. An instant past the jiff range, but inside the range of
//! `chrono`, clamps to [`Timestamp::MAX`] or [`Timestamp::MIN`].
//!
//! [`date`] keeps the `chrono` grammar for a calendar date only. jiff also
//! reads a full date and time as a date, and drops its time and offset.
//! `chrono` refused that text, so [`date`] refuses it too.

use jiff::tz::Offset;
use jiff::{Timestamp, civil};

/// The first year that `chrono` 0.4 held.
const CHRONO_MIN_YEAR: i64 = -262_143;

/// The last year that `chrono` 0.4 held.
const CHRONO_MAX_YEAR: i64 = 262_142;

/// Seconds in one day. `chrono` refused an offset of one day or more.
const SECONDS_PER_DAY: i64 = 86_400;

/// Parses an RFC 3339 instant, as `DateTime::parse_from_rfc3339` did.
///
/// Text that jiff reads gives the jiff result. Other text goes through the
/// strict grammar of `chrono`. An instant past the jiff range clamps to the
/// nearest end of that range.
///
/// # Errors
///
/// Returns the jiff error when neither grammar reads the text.
pub fn rfc3339(text: &str) -> Result<Timestamp, jiff::Error> {
    parse_with(text, strict_fields)
}

/// Parses an instant, as `str::parse::<DateTime<Utc>>` and its `serde`
/// read did.
///
/// That `chrono` grammar is looser than RFC 3339. It reads one-digit fields,
/// spaces between fields, an offset without a colon, and a `UTC` suffix in
/// any case. So it reads the `chrono` display form. Text that jiff reads
/// gives the jiff result. An instant past the jiff range clamps to the
/// nearest end of that range.
///
/// # Errors
///
/// Returns the jiff error when neither grammar reads the text.
pub fn relaxed(text: &str) -> Result<Timestamp, jiff::Error> {
    parse_with(text, relaxed_fields)
}

/// Parses a calendar date, as `str::parse::<chrono::NaiveDate>` did.
///
/// The grammar is `Y-M-D`. The month and the day take one or two digits.
/// A year of more than 4 digits needs a sign. Spaces can come around each
/// field. The function refuses any other text, which includes a date with a
/// time.
///
/// It also refuses a year outside the jiff range, -9999 to 9999. `chrono`
/// read such a year, but a `civil::Date` cannot hold it. Such a calendar
/// exclusion date can match no fire date. The upgrade guide records this.
#[must_use]
pub fn date(text: &str) -> Option<civil::Date> {
    let (rest, year, month, day) = date_fields(text)?;
    if !rest.trim_start().is_empty() {
        return None;
    }
    civil::Date::new(
        i16::try_from(year).ok()?,
        i8::try_from(month).ok()?,
        i8::try_from(day).ok()?,
    )
    .ok()
}

/// `serde` adapters for request fields that `chrono` deserialized.
///
/// Use them with `#[serde(default, deserialize_with = "...")]`. Each reads
/// a JSON string as the `chrono` type of the field read it.
pub mod de {
    use jiff::{Timestamp, civil};
    use serde::{Deserialize, Deserializer, de::Error};

    /// Reads a required instant with [`super::relaxed`].
    ///
    /// # Errors
    ///
    /// Returns a `serde` error when the value is not a string, or when the
    /// string is not an instant.
    pub fn required<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Timestamp, D::Error> {
        let text = String::deserialize(deserializer)?;
        super::relaxed(&text).map_err(D::Error::custom)
    }

    /// Reads an optional instant with [`super::relaxed`]. JSON `null` reads
    /// as `None`.
    ///
    /// # Errors
    ///
    /// Returns a `serde` error when the value is not a string or `null`, or
    /// when the string is not an instant.
    pub fn optional<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Timestamp>, D::Error> {
        Option::<String>::deserialize(deserializer)?
            .map(|text| super::relaxed(&text).map_err(D::Error::custom))
            .transpose()
    }

    /// Reads a field that tells "absent" from "null", with
    /// [`super::relaxed`].
    ///
    /// The caller marks the field `#[serde(default)]`. An absent field is
    /// then `None`. JSON `null` is `Some(None)`.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`optional`].
    pub fn tristate<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Option<Timestamp>>, D::Error> {
        optional(deserializer).map(Some)
    }

    /// Reads a list of calendar dates with [`super::date`].
    ///
    /// # Errors
    ///
    /// Returns a `serde` error when an element is not a date string.
    pub fn dates<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<civil::Date>, D::Error> {
        Vec::<String>::deserialize(deserializer)?
            .iter()
            .map(|text| {
                super::date(text).ok_or_else(|| {
                    D::Error::custom(format!("invalid date '{text}'; expected YYYY-MM-DD"))
                })
            })
            .collect()
    }
}

/// The fields of an instant that a `chrono` grammar read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Fields {
    year: i64,
    month: i64,
    day: i64,
    hour: i64,
    minute: i64,
    second: i64,
    nanosecond: i64,
    offset: i64,
}

impl Fields {
    /// Returns the instant, with the range checks of `chrono`.
    ///
    /// A value that `chrono` refused gives `None`. A value that `chrono`
    /// held, but that is past the jiff range, clamps to the nearest end.
    fn to_timestamp(self) -> Option<Timestamp> {
        let valid = (CHRONO_MIN_YEAR..=CHRONO_MAX_YEAR).contains(&self.year)
            && (1..=12).contains(&self.month)
            && (1..=days_in_month(self.year, self.month)).contains(&self.day)
            && (0..=23).contains(&self.hour)
            && (0..=59).contains(&self.minute)
            && (0..=60).contains(&self.second)
            && self.offset.abs() < SECONDS_PER_DAY;
        if !valid {
            return None;
        }
        // `chrono` also refused a value that the offset moved past its range.
        let utc_secs = self.hour * 3_600 + self.minute * 60 + self.second.min(59) - self.offset;
        let first_day = self.year == CHRONO_MIN_YEAR && self.month == 1 && self.day == 1;
        let last_day = self.year == CHRONO_MAX_YEAR && self.month == 12 && self.day == 31;
        if (first_day && utc_secs < 0) || (last_day && utc_secs >= SECONDS_PER_DAY) {
            return None;
        }
        let past_end = if self.year > 0 {
            Timestamp::MAX
        } else {
            Timestamp::MIN
        };
        let Ok(year) = i16::try_from(self.year) else {
            return Some(past_end);
        };
        // A leap second reads as second 59, as the jiff parser reads it.
        let civil = civil::DateTime::new(
            year,
            i8::try_from(self.month).ok()?,
            i8::try_from(self.day).ok()?,
            i8::try_from(self.hour).ok()?,
            i8::try_from(self.minute).ok()?,
            i8::try_from(self.second.min(59)).ok()?,
            i32::try_from(self.nanosecond).ok()?,
        );
        let Ok(civil) = civil else {
            return Some(past_end);
        };
        let offset = Offset::from_seconds(i32::try_from(self.offset).ok()?).ok()?;
        Some(offset.to_timestamp(civil).unwrap_or(past_end))
    }
}

/// Tries jiff, then `fallback`. Returns the jiff error when both fail.
fn parse_with(text: &str, fallback: fn(&str) -> Option<Fields>) -> Result<Timestamp, jiff::Error> {
    let error = match text.parse::<Timestamp>() {
        Ok(at) => return Ok(at),
        Err(error) => error,
    };
    fallback(text).and_then(Fields::to_timestamp).ok_or(error)
}

/// Returns the days in `month` of the proleptic Gregorian `year`.
const fn days_in_month(year: i64, month: i64) -> i64 {
    let leap = year.rem_euclid(4) == 0 && (year.rem_euclid(100) != 0 || year.rem_euclid(400) == 0);
    match month {
        2 if leap => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Reads `min..=max` ASCII digits, as `chrono` `scan::number` did.
fn number(text: &str, min: usize, max: usize) -> Option<(&str, i64)> {
    let count = text
        .bytes()
        .take(max)
        .take_while(u8::is_ascii_digit)
        .count();
    if count < min {
        return None;
    }
    let value = text.as_bytes()[..count]
        .iter()
        .try_fold(0_i64, |total, digit| {
            total.checked_mul(10)?.checked_add(i64::from(digit - b'0'))
        })?;
    Some((&text[count..], value))
}

/// Reads a fraction after its dot, as `chrono` `scan::nanosecond` did.
///
/// The first 9 digits count. Further digits are read and ignored.
fn fraction(text: &str) -> Option<(&str, i64)> {
    let (rest, value) = number(text, 1, 9)?;
    let count = u32::try_from(text.len() - rest.len()).ok()?;
    let nanosecond = value * 10_i64.pow(9 - count);
    Some((
        rest.trim_start_matches(|c: char| c.is_ascii_digit()),
        nanosecond,
    ))
}

/// Reads two ASCII digits.
fn two_digits(text: &str) -> Option<(&str, i64)> {
    let bytes = text.as_bytes();
    let (tens, ones) = (*bytes.first()?, *bytes.get(1)?);
    (tens.is_ascii_digit() && ones.is_ascii_digit()).then(|| {
        (
            &text[2..],
            i64::from(tens - b'0') * 10 + i64::from(ones - b'0'),
        )
    })
}

/// Reads an offset in seconds, as `chrono` `scan::timezone_offset` did.
///
/// `Z` in either case is UTC. A sign is `+`, `-` or U+2212. The hour and the
/// minute take two digits each. The strict grammar needs a colon between
/// them. The relaxed grammar skips any colons and spaces there.
fn offset(text: &str, relaxed: bool) -> Option<(&str, i64)> {
    if let Some(rest) = text.strip_prefix(['Z', 'z']) {
        return Some((rest, 0));
    }
    let (negative, rest) = if let Some(rest) = text.strip_prefix('+') {
        (false, rest)
    } else {
        (true, text.strip_prefix(['-', '\u{2212}'])?)
    };
    let (rest, hours) = two_digits(rest)?;
    let rest = if relaxed {
        rest.trim_start_matches(|c: char| c == ':' || c.is_whitespace())
    } else {
        rest.strip_prefix(':')?
    };
    let (rest, minutes) = two_digits(rest)?;
    if minutes > 59 {
        return None;
    }
    let seconds = hours * 3_600 + minutes * 60;
    Some((rest, if negative { -seconds } else { seconds }))
}

/// Reads the strict RFC 3339 grammar of `chrono` `parse_rfc3339`.
fn strict_fields(text: &str) -> Option<Fields> {
    let bytes = text.as_bytes();
    if bytes.len() < 19 {
        return None;
    }
    let separators = [(4, "-"), (7, "-"), (10, "Tt "), (13, ":"), (16, ":")];
    if !separators
        .iter()
        .all(|&(at, set)| set.as_bytes().contains(&bytes[at]))
    {
        return None;
    }
    let digits = |from: usize, to: usize| -> Option<i64> {
        bytes[from..to].iter().try_fold(0_i64, |total, &digit| {
            digit
                .is_ascii_digit()
                .then(|| total * 10 + i64::from(digit - b'0'))
        })
    };
    let (year, month, day) = (digits(0, 4)?, digits(5, 7)?, digits(8, 10)?);
    let (hour, minute, second) = (digits(11, 13)?, digits(14, 16)?, digits(17, 19)?);
    let (rest, nanosecond) = if bytes[19..].first() == Some(&b'.') {
        fraction(&text[20..])?
    } else {
        (&text[19..], 0)
    };
    let (rest, offset) = offset(rest, false)?;
    rest.is_empty().then_some(Fields {
        year,
        month,
        day,
        hour,
        minute,
        second,
        nanosecond,
        offset,
    })
}

/// Reads `Y-M-D` as the date items of `chrono` read it.
///
/// Returns the text after the day, then the year, month and day.
fn date_fields(text: &str) -> Option<(&str, i64, i64, i64)> {
    let text = text.trim_start();
    let (rest, year) = if let Some(rest) = text.strip_prefix('-') {
        let (rest, year) = number(rest, 1, usize::MAX)?;
        (rest, -year)
    } else if let Some(rest) = text.strip_prefix('+') {
        number(rest, 1, usize::MAX)?
    } else {
        number(text, 1, 4)?
    };
    let rest = rest.trim_start().strip_prefix('-')?;
    let (rest, month) = number(rest.trim_start(), 1, 2)?;
    let rest = rest.trim_start().strip_prefix('-')?;
    let (rest, day) = number(rest.trim_start(), 1, 2)?;
    Some((rest, year, month, day))
}

/// Reads the relaxed grammar of `chrono` `DateTime::<FixedOffset>::from_str`.
fn relaxed_fields(text: &str) -> Option<Fields> {
    let (rest, year, month, day) = date_fields(text)?;
    let rest = rest.strip_prefix(['T', 't', ' '])?;
    let (rest, hour) = number(rest.trim_start(), 1, 2)?;
    let rest = rest.trim_start().strip_prefix(':')?;
    let (rest, minute) = number(rest.trim_start(), 1, 2)?;
    let rest = rest.trim_start().strip_prefix(':')?;
    let (rest, second) = number(rest.trim_start(), 1, 2)?;
    let (rest, nanosecond) = match rest.strip_prefix('.') {
        Some(after_dot) => fraction(after_dot)?,
        None => (rest, 0),
    };
    let rest = rest.trim_start();
    let (rest, offset) = match rest.get(..3) {
        Some(name) if name.eq_ignore_ascii_case("UTC") => (&rest[3..], 0),
        _ => offset(rest, true)?,
    };
    rest.trim_start().is_empty().then_some(Fields {
        year,
        month,
        day,
        hour,
        minute,
        second,
        nanosecond,
        offset,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(text: &str) -> Timestamp {
        text.parse().expect("a jiff instant")
    }

    #[test]
    fn rfc3339_clamps_a_sentinel_that_chrono_held() {
        assert!("9999-12-31T23:59:59Z".parse::<Timestamp>().is_err());
        assert_eq!(rfc3339("9999-12-31T23:59:59Z").ok(), Some(Timestamp::MAX));
        assert_eq!(
            rfc3339("9999-12-30T22:00:01+00:00").ok(),
            Some(Timestamp::MAX)
        );
        assert_eq!(rfc3339("9999-12-31t00:00:00.5z").ok(), Some(Timestamp::MAX));
        assert_eq!(relaxed("+10000-01-01T00:00:00Z").ok(), Some(Timestamp::MAX));
        assert_eq!(
            relaxed("-9999-01-01T00:00:00+01:00").ok(),
            Some(Timestamp::MIN)
        );
        assert_eq!(relaxed("-10000-01-01T00:00:00Z").ok(), Some(Timestamp::MIN));
    }

    #[test]
    fn rfc3339_keeps_the_strict_chrono_grammar() {
        assert_eq!(
            rfc3339("2026-01-01T00:00:00.1234567891Z").ok(),
            Some(at("2026-01-01T00:00:00.123456789Z"))
        );
        assert_eq!(
            rfc3339("2026-01-01T00:00:00\u{2212}05:00").ok(),
            Some(at("2026-01-01T05:00:00Z"))
        );
        assert_eq!(
            rfc3339("2026-01-01T00:00:00Z").ok(),
            Some(at("2026-01-01T00:00:00Z"))
        );
        assert!(rfc3339("2026-12-01 09:00:00 UTC").is_err());
        assert!(rfc3339("2026-12-1T09:00:00Z").is_err());
        assert!(rfc3339("9999-12-31T23:59:59").is_err());
        assert!(rfc3339("9999-13-01T00:00:00Z").is_err());
        assert!(rfc3339("9999-12-31T23:59:59.Z").is_err());
        assert!(rfc3339("9999-12-31T23:59:59+0100").is_err());
        assert!(rfc3339("9999-12-31T23:59:59Z ").is_err());
        assert!(rfc3339("").is_err());
    }

    #[test]
    fn relaxed_reads_the_chrono_from_str_grammar() {
        let nine = at("2026-12-01T09:00:00Z");
        assert_eq!(relaxed("2026-12-01 09:00:00 UTC").ok(), Some(nine));
        assert_eq!(relaxed("2026-12-01 09:00:00 utc").ok(), Some(nine));
        assert_eq!(relaxed("2026-12-1T9:00:0Z").ok(), Some(nine));
        assert_eq!(relaxed(" 2026-12-01T09:00:00 +00:00 ").ok(), Some(nine));
        assert_eq!(relaxed("2026 - 12 - 01T09 : 00 : 00Z").ok(), Some(nine));
        assert_eq!(relaxed("2026-12-01T10:30:00+0130").ok(), Some(nine));
        assert_eq!(relaxed("2026-12-01T04:00:00\u{2212}05:00").ok(), Some(nine));
        assert_eq!(
            relaxed("2026-12-01T09:00:00.0000000019Z").ok(),
            Some(at("2026-12-01T09:00:00.000000001Z"))
        );
        assert!(relaxed("2026-12-01T09:00:00").is_err());
        assert!(relaxed("2026-02-30T09:00:00Z").is_err());
        assert!(relaxed("2026-12-01T09:00:00Z trailing").is_err());
        assert!(relaxed("+262143-01-01T00:00:00Z").is_err());
        assert!(relaxed("+262142-12-31T23:59:59-00:01").is_err());
        assert!(relaxed("-262143-01-01T00:00:00+00:01").is_err());
        assert_eq!(
            relaxed("+262142-12-31T23:59:59+00:01").ok(),
            Some(Timestamp::MAX)
        );
        assert_eq!(
            relaxed("-262143-01-01T00:00:00-00:01").ok(),
            Some(Timestamp::MIN)
        );
        assert!(relaxed("20260-12-01T09:00:00Z").is_err());
    }

    #[test]
    fn date_keeps_the_chrono_naive_date_grammar() {
        assert_eq!(date("2026-07-04"), Some(civil::date(2026, 7, 4)));
        assert_eq!(date("2026-7-4"), Some(civil::date(2026, 7, 4)));
        assert_eq!(date(" 2026 - 07 - 04 "), Some(civil::date(2026, 7, 4)));
        assert_eq!(date("26-07-04"), Some(civil::date(26, 7, 4)));
        assert_eq!(date("-0001-01-01"), Some(civil::date(-1, 1, 1)));
        assert_eq!(date("2026-07-04T23:30:00-05:00"), None);
        assert_eq!(date("2026-07-04 23:30"), None);
        assert_eq!(date("20260704"), None);
        assert_eq!(date("12026-07-04"), None);
        assert_eq!(date("+12026-07-04"), None);
        assert_eq!(date("2026-02-29"), None);
        assert_eq!(date("2024-02-29"), Some(civil::date(2024, 2, 29)));
    }

    #[derive(Debug, serde::Deserialize)]
    struct Body {
        #[serde(default, deserialize_with = "de::optional")]
        start_at: Option<Timestamp>,
        // The outer `Option` is absent; the inner one is JSON `null`.
        #[allow(clippy::option_option)]
        #[serde(default, deserialize_with = "de::tristate")]
        end_at: Option<Option<Timestamp>>,
        #[serde(default, deserialize_with = "de::dates")]
        dates: Vec<civil::Date>,
    }

    #[test]
    fn serde_adapters_read_what_chrono_read() {
        let body: Body = serde_json::from_str(
            r#"{"start_at":"9999-12-31T23:59:59Z","end_at":null,"dates":["2026-7-4"]}"#,
        )
        .expect("a valid body");
        assert_eq!(body.start_at, Some(Timestamp::MAX));
        assert_eq!(body.end_at, Some(None));
        assert_eq!(body.dates, vec![civil::date(2026, 7, 4)]);

        let empty: Body = serde_json::from_str("{}").expect("an empty body");
        assert_eq!((empty.start_at, empty.end_at), (None, None));
        assert_eq!(empty.dates, [] as [jiff::civil::Date; 0]);

        let display: Body = serde_json::from_str(r#"{"end_at":"2026-12-01 09:00:00 UTC"}"#)
            .expect("the chrono display form");
        assert_eq!(display.end_at, Some(Some(at("2026-12-01T09:00:00Z"))));

        assert!(
            serde_json::from_str::<Body>(r#"{"dates":["2026-07-04T23:30:00-05:00"]}"#).is_err()
        );
        assert!(serde_json::from_str::<Body>(r#"{"start_at":"soon"}"#).is_err());
    }
}
