//! The `chrono` values at the `autumn-web` boundary.
//!
//! The plugin keeps time as `jiff` values (ADR 0018). `autumn-web` 0.10
//! still takes `chrono` values in its webhook delivery log and in its
//! tenant restore. This module converts a `jiff` value for those calls
//! only.

/// Returns `at` as a `chrono` UTC instant, with every nanosecond.
///
/// The `chrono` range holds the whole `jiff` range, so the result is `Some`
/// for every input.
pub(crate) fn to_chrono(at: jiff::Timestamp) -> Option<chrono::DateTime<chrono::Utc>> {
    // jiff truncates the seconds toward zero. chrono takes floored seconds
    // and a fraction that is not negative.
    let (mut secs, mut nanos) = (at.as_second(), at.subsec_nanosecond());
    if nanos < 0 {
        secs -= 1;
        nanos += 1_000_000_000;
    }
    chrono::DateTime::from_timestamp(secs, nanos.unsigned_abs())
}

#[cfg(test)]
mod tests {
    use super::to_chrono;

    #[test]
    fn every_instant_keeps_its_nanoseconds() {
        for (secs, nanos) in [(1_700_000_000, 123_456_789), (0, 0), (-1, -1), (-5, -500_000_000)] {
            let at = jiff::Timestamp::new(secs, nanos).unwrap();
            let converted = to_chrono(at).unwrap();
            assert_eq!(
                converted.timestamp_nanos_opt().map(i128::from),
                Some(at.as_nanosecond())
            );
        }
    }

    #[test]
    fn the_jiff_range_converts() {
        assert!(to_chrono(jiff::Timestamp::MIN).is_some());
        assert!(to_chrono(jiff::Timestamp::MAX).is_some());
    }
}
