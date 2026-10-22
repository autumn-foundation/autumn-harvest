//! Percentiles of timed samples.

use std::time::Duration;

/// The percentiles of one set of samples.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Summary {
    /// The number of samples.
    pub samples: usize,
    /// The median.
    pub p50: Duration,
    /// The 99th percentile.
    pub p99: Duration,
    /// The largest sample.
    pub max: Duration,
}

/// The nearest-rank percentile `q` of `sorted`. `q` is in `(0, 1]`.
///
/// Returns `None` for no samples.
#[must_use]
pub fn percentile(sorted: &[Duration], q: f64) -> Option<Duration> {
    if sorted.is_empty() {
        return None;
    }
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "sample counts are far below 2^53, and the rank is clamped"
    )]
    let rank = (q * sorted.len() as f64).ceil() as usize;
    sorted.get(rank.clamp(1, sorted.len()) - 1).copied()
}

/// The summary of `samples`, in any order. Returns `None` for no samples.
#[must_use]
pub fn summarize(samples: &[Duration]) -> Option<Summary> {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    Some(Summary {
        samples: sorted.len(),
        p50: percentile(&sorted, 0.50)?,
        p99: percentile(&sorted, 0.99)?,
        max: *sorted.last()?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn the_nearest_rank_percentile_of_one_to_a_hundred() {
        let sorted: Vec<Duration> = (1..=100).map(ms).collect();
        assert_eq!(percentile(&sorted, 0.50), Some(ms(50)));
        assert_eq!(percentile(&sorted, 0.99), Some(ms(99)));
        assert_eq!(percentile(&sorted, 1.0), Some(ms(100)));
    }

    #[test]
    fn a_small_sample_takes_its_top_rank_for_p99() {
        let sorted: Vec<Duration> = (1..=10).map(ms).collect();
        assert_eq!(percentile(&sorted, 0.99), Some(ms(10)));
        assert_eq!(percentile(&[ms(7)], 0.50), Some(ms(7)));
    }

    #[test]
    fn no_samples_give_no_percentile() {
        assert_eq!(percentile(&[], 0.5), None);
        assert_eq!(summarize(&[]), None);
    }

    #[test]
    fn a_summary_sorts_its_samples() {
        let summary = summarize(&[ms(3), ms(1), ms(2)]).expect("summary");
        assert_eq!(
            summary,
            Summary {
                samples: 3,
                p50: ms(2),
                p99: ms(3),
                max: ms(3),
            }
        );
    }
}
