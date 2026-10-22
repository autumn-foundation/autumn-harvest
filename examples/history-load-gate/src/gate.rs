//! The stage 2 thresholds of `docs/rnd/typed-state-snapshots.md`.
//!
//! Either threshold trips the gate. Each rule is a strict `>`, as the spike
//! wrote it. So one trip decides, even when the other value is missing.
//! Only complete data with no trip gives a no-go.

use std::time::Duration;

/// The gate thresholds.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Thresholds {
    /// The largest share of `cold` decisions that does not trip the gate.
    pub cold_share: f64,
    /// The largest cold p99 that does not trip the gate.
    pub cold_p99: Duration,
}

impl Default for Thresholds {
    /// 20% of decisions, and 50 ms.
    fn default() -> Self {
        Self {
            cold_share: 0.20,
            cold_p99: Duration::from_millis(50),
        }
    }
}

/// What a fleet week observed. `None` means not measured.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Observed {
    /// The share of `cold` decisions over the week, from 0 to 1.
    pub cold_share: Option<f64>,
    /// The p99 time of a cold decision over the week.
    pub cold_p99: Option<Duration>,
}

/// One threshold that an observation passed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Trip {
    /// The cold share passed its threshold.
    ColdShare(f64),
    /// The cold p99 passed its threshold.
    ColdP99(Duration),
}

/// The gate result.
#[derive(Debug, Clone, PartialEq)]
pub enum Verdict {
    /// At least one threshold tripped. Stage 2 goes ahead.
    Go(Vec<Trip>),
    /// Both values are known, and neither tripped.
    NoGo,
    /// No value tripped, and a value is missing.
    Undecided,
}

/// Applies `thresholds` to `observed`.
#[must_use]
pub fn verdict(thresholds: &Thresholds, observed: &Observed) -> Verdict {
    let mut trips = Vec::new();
    if let Some(share) = observed.cold_share
        && share > thresholds.cold_share
    {
        trips.push(Trip::ColdShare(share));
    }
    if let Some(p99) = observed.cold_p99
        && p99 > thresholds.cold_p99
    {
        trips.push(Trip::ColdP99(p99));
    }
    if !trips.is_empty() {
        Verdict::Go(trips)
    } else if observed.cold_share.is_some() && observed.cold_p99.is_some() {
        Verdict::NoGo
    } else {
        Verdict::Undecided
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn full(share: f64, p99_us: u64) -> Observed {
        Observed {
            cold_share: Some(share),
            cold_p99: Some(Duration::from_micros(p99_us)),
        }
    }

    #[test]
    fn the_defaults_are_the_spike_thresholds() {
        let thresholds = Thresholds::default();
        assert!((thresholds.cold_share - 0.20).abs() < f64::EPSILON);
        assert_eq!(thresholds.cold_p99, Duration::from_millis(50));
    }

    #[test]
    fn values_at_the_thresholds_do_not_trip() {
        let result = verdict(&Thresholds::default(), &full(0.20, 50_000));
        assert_eq!(result, Verdict::NoGo);
    }

    #[test]
    fn a_cold_share_above_twenty_percent_trips() {
        let result = verdict(&Thresholds::default(), &full(0.2001, 1_000));
        assert_eq!(result, Verdict::Go(vec![Trip::ColdShare(0.2001)]));
    }

    #[test]
    fn a_cold_p99_above_fifty_ms_trips() {
        let result = verdict(&Thresholds::default(), &full(0.05, 50_001));
        assert_eq!(
            result,
            Verdict::Go(vec![Trip::ColdP99(Duration::from_micros(50_001))])
        );
    }

    #[test]
    fn both_thresholds_can_trip_together() {
        let Verdict::Go(trips) = verdict(&Thresholds::default(), &full(0.5, 90_000)) else {
            panic!("both values trip");
        };
        assert_eq!(trips.len(), 2);
    }

    #[test]
    fn one_trip_decides_without_the_other_value() {
        // Either threshold alone justifies stage 2.
        let share_only = Observed {
            cold_share: Some(0.4),
            cold_p99: None,
        };
        assert_eq!(
            verdict(&Thresholds::default(), &share_only),
            Verdict::Go(vec![Trip::ColdShare(0.4)])
        );
    }

    #[test]
    fn a_missing_value_with_no_trip_is_undecided() {
        let p99_only = Observed {
            cold_share: None,
            cold_p99: Some(Duration::from_millis(3)),
        };
        assert_eq!(
            verdict(&Thresholds::default(), &p99_only),
            Verdict::Undecided
        );
        assert_eq!(
            verdict(&Thresholds::default(), &Observed::default()),
            Verdict::Undecided
        );
    }
}
