//! Timestamp text in the exact formats that `chrono` wrote.
//!
//! Some stored or hashed text holds a timestamp. The audit chain hashes one,
//! a cursor encodes one, and API fields return one. A change of format there
//! breaks a stored hash or a client parser. So each function here writes the
//! bytes that the `chrono` call it replaces wrote, for every value in the
//! jiff range. The tests pin those bytes.
//!
//! Prefer the jiff `Display` and `serde` formats for new text. Use this
//! module only where the old bytes are a contract.

use std::fmt::Write as _;

use jiff::Timestamp;
use jiff::tz::TimeZone;

/// The fraction digits to write, as `chrono::SecondsFormat` chose them.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum SecondsFormat {
    /// No fraction.
    Secs,
    /// Three fraction digits.
    Millis,
    /// Six fraction digits.
    Micros,
    /// Nine fraction digits.
    Nanos,
    /// No fraction for a whole second, else 3, 6 or 9 digits. Each choice
    /// is the shortest one that keeps every nonzero digit.
    AutoSi,
}

/// Writes `at` as `chrono::DateTime::<Utc>::to_rfc3339` did.
///
/// The offset is `+00:00` and the fraction follows [`SecondsFormat::AutoSi`].
#[must_use]
pub fn rfc3339(at: Timestamp) -> String {
    rfc3339_opts(at, SecondsFormat::AutoSi, false)
}

/// Writes `at` as `chrono::DateTime::<Utc>::to_rfc3339_opts` did.
///
/// With `use_z`, the offset is `Z`. Without it, the offset is `+00:00`.
/// Extra fraction digits are truncated, not rounded.
#[must_use]
pub fn rfc3339_opts(at: Timestamp, format: SecondsFormat, use_z: bool) -> String {
    let mut out = String::with_capacity(32);
    write_datetime(&mut out, at, 'T', format);
    out.push_str(if use_z { "Z" } else { "+00:00" });
    out
}

/// Writes `at` as the `Display` impl of `chrono::DateTime<Utc>` did.
///
/// The form is `2023-11-14 22:13:20.5 UTC`, with the fraction digits of
/// [`SecondsFormat::AutoSi`]. Use it only where those bytes are a contract.
#[must_use]
pub fn utc_display(at: Timestamp) -> String {
    let mut out = String::with_capacity(32);
    write_datetime(&mut out, at, ' ', SecondsFormat::AutoSi);
    out.push_str(" UTC");
    out
}

/// Writes the UTC date, `separator`, the time and the fraction of `at`.
fn write_datetime(out: &mut String, at: Timestamp, separator: char, format: SecondsFormat) {
    let dt = TimeZone::UTC.to_datetime(at);
    let year = i32::from(dt.year());
    // `chrono` writes four digits in 0..=9999 and a sign with four or more
    // digits outside that range. Writing to a `String` cannot fail.
    if (0..=9999).contains(&year) {
        let _ = write!(out, "{year:04}");
    } else {
        let _ = write!(out, "{year:+05}");
    }
    let _ = write!(
        out,
        "-{:02}-{:02}{separator}{:02}:{:02}:{:02}",
        dt.month(),
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second(),
    );
    let nanos = dt.subsec_nanosecond();
    let digits = match format {
        SecondsFormat::Secs => 0,
        SecondsFormat::AutoSi if nanos == 0 => 0,
        SecondsFormat::Millis => 3,
        SecondsFormat::AutoSi if nanos % 1_000_000 == 0 => 3,
        SecondsFormat::Micros => 6,
        SecondsFormat::AutoSi if nanos % 1_000 == 0 => 6,
        SecondsFormat::Nanos | SecondsFormat::AutoSi => 9,
    };
    match digits {
        3 => {
            let _ = write!(out, ".{:03}", nanos / 1_000_000);
        }
        6 => {
            let _ = write!(out, ".{:06}", nanos / 1_000);
        }
        9 => {
            let _ = write!(out, ".{nanos:09}");
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2023-11-14T22:13:20Z plus `nanos`.
    fn at(nanos: i32) -> Timestamp {
        Timestamp::constant(1_700_000_000, nanos)
    }

    // Every expected string below is the output of chrono 0.4.45 for the
    // same instant and the same call.

    #[test]
    fn rfc3339_matches_chrono_to_rfc3339() {
        assert_eq!(rfc3339(at(0)), "2023-11-14T22:13:20+00:00");
        assert_eq!(rfc3339(at(100_000_000)), "2023-11-14T22:13:20.100+00:00");
        assert_eq!(rfc3339(at(120_000)), "2023-11-14T22:13:20.000120+00:00");
        assert_eq!(
            rfc3339(at(123_456_789)),
            "2023-11-14T22:13:20.123456789+00:00"
        );
        assert_eq!(rfc3339(at(1)), "2023-11-14T22:13:20.000000001+00:00");
    }

    #[test]
    fn rfc3339_opts_matches_each_chrono_seconds_format() {
        let t = at(123_456_789);
        assert_eq!(
            rfc3339_opts(t, SecondsFormat::Secs, true),
            "2023-11-14T22:13:20Z"
        );
        assert_eq!(
            rfc3339_opts(t, SecondsFormat::Millis, true),
            "2023-11-14T22:13:20.123Z"
        );
        assert_eq!(
            rfc3339_opts(t, SecondsFormat::Micros, true),
            "2023-11-14T22:13:20.123456Z"
        );
        assert_eq!(
            rfc3339_opts(t, SecondsFormat::Nanos, true),
            "2023-11-14T22:13:20.123456789Z"
        );
        assert_eq!(
            rfc3339_opts(at(5_000), SecondsFormat::Micros, true),
            "2023-11-14T22:13:20.000005Z"
        );
        assert_eq!(
            rfc3339_opts(at(0), SecondsFormat::Micros, false),
            "2023-11-14T22:13:20.000000+00:00"
        );
        assert_eq!(
            rfc3339_opts(at(0), SecondsFormat::AutoSi, true),
            "2023-11-14T22:13:20Z"
        );
    }

    #[test]
    fn utc_display_matches_chrono_display() {
        assert_eq!(utc_display(at(0)), "2023-11-14 22:13:20 UTC");
        assert_eq!(utc_display(at(100_000_000)), "2023-11-14 22:13:20.100 UTC");
        assert_eq!(
            utc_display(at(123_456_000)),
            "2023-11-14 22:13:20.123456 UTC"
        );
    }

    #[test]
    fn jiff_parses_every_chrono_form_back_to_the_same_instant() {
        for nanos in [0, 1, 120_000, 100_000_000, 123_456_789] {
            let t = at(nanos);
            for text in [
                rfc3339(t),
                rfc3339_opts(t, SecondsFormat::Nanos, true),
                rfc3339_opts(t, SecondsFormat::AutoSi, true),
            ] {
                assert_eq!(text.parse::<Timestamp>().ok(), Some(t), "{text}");
            }
        }
    }

    #[test]
    fn years_outside_four_digits_carry_a_sign() {
        let early = Timestamp::constant(-62_198_755_200, 0);
        assert_eq!(
            rfc3339_opts(early, SecondsFormat::Secs, true),
            "-0001-01-01T00:00:00Z"
        );
        assert_eq!(
            rfc3339_opts(Timestamp::UNIX_EPOCH, SecondsFormat::Secs, true),
            "1970-01-01T00:00:00Z"
        );
    }
}
