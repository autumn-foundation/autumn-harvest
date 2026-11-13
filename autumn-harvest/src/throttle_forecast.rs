//! Throttle forecast: when does each throttled start run?
//!
//! A [`ThrottlePolicy`] paces workflow starts with a token bucket. A burst of
//! starts above the bucket waits in `harvest_start_throttle`. Many schedules
//! of one workflow type often share one cron expression, such as
//! `0 * * * *` for each tenant. Every hour they then reach one bucket at the
//! same instant. Nothing tells the author how late the last start runs, or
//! whether `schedule_to_start` drops part of the burst.
//!
//! This module answers that question before deploy. [`forecast`] replays a
//! list of arrival bursts through the bucket. [`forecast_schedules`] walks
//! real schedules with [`SlotWalker`] and forecasts their slots.
//! [`steady_state`] tells whether a periodic burst drains before the next one.
//!
//! ## Model
//!
//! The bucket holds `burst` tokens at offset zero. It refills at
//! `refill_per_sec` tokens each second, up to `burst`. A start needs one whole
//! token. Waiting starts are admitted in arrival order, and each one is
//! admitted as soon as a token is free. A start that would wait longer than
//! `schedule_to_start` is dropped and takes no token.
//!
//! The forecast is a lower bound on each wait. The real scanner fires on a
//! tick, and it fires at most
//! [`THROTTLE_FIRE_PER_KEY_CAP`](crate::throttle::THROTTLE_FIRE_PER_KEY_CAP)
//! rows of one key per tick. Both can only add delay.
//!
//! The module is pure. It sends no SQL and reads no clock.

use std::collections::BTreeMap;
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::policy::Schedule;
use crate::schedule_plan::SlotWalker;
use crate::throttle::ThrottlePolicy;

/// The largest number of schedule slots that one forecast reads.
///
/// A one-second interval over a long horizon has millions of slots. This cap
/// keeps the forecast finite.
pub const MAX_FORECAST_SLOTS: usize = 100_000;

/// A burst of starts that reach the bucket at one instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Arrival {
    /// The offset of the burst from the start of the forecast.
    pub at: Duration,
    /// The number of starts in the burst.
    pub count: u64,
}

/// The forecast of one arrival burst.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchForecast {
    /// The offset of the burst from the start of the forecast.
    pub at: Duration,
    /// The number of starts in the burst.
    pub count: u64,
    /// The starts that the bucket admits.
    pub admitted: u64,
    /// The starts that `schedule_to_start` drops.
    pub dropped: u64,
    /// The admission offset of the first admitted start.
    pub first_admit: Option<Duration>,
    /// The admission offset of the last admitted start.
    pub last_admit: Option<Duration>,
    /// The longest wait of an admitted start in this burst.
    pub max_wait: Duration,
}

/// The result of a throttle forecast.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThrottleForecast {
    /// One entry for each burst, in arrival order.
    pub batches: Vec<BatchForecast>,
    /// `true` when the slot walk stops at [`MAX_FORECAST_SLOTS`].
    pub truncated: bool,
}

impl ThrottleForecast {
    /// The number of starts that the bucket admits.
    #[must_use]
    pub fn admitted(&self) -> u64 {
        self.batches.iter().map(|b| b.admitted).sum()
    }

    /// The number of starts that `schedule_to_start` drops.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.batches.iter().map(|b| b.dropped).sum()
    }

    /// The longest wait of an admitted start.
    #[must_use]
    pub fn max_wait(&self) -> Duration {
        self.batches
            .iter()
            .map(|b| b.max_wait)
            .max()
            .unwrap_or_default()
    }

    /// The admission offset of the last admitted start.
    #[must_use]
    pub fn drained_at(&self) -> Option<Duration> {
        self.batches.iter().filter_map(|b| b.last_admit).max()
    }
}

/// The long-run behaviour of one burst size that repeats each period.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SteadyState {
    /// The bucket refills before the next burst. Every period looks the same.
    Keeps {
        /// The wait of the last start of each burst.
        worst_wait: Duration,
    },
    /// The refill cannot keep up. The backlog grows without a bound.
    FallsBehind {
        /// The growth of the wait of the last start in each period.
        lag_per_period: Duration,
    },
}

/// A policy that the forecast cannot model.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
pub enum ForecastError {
    /// The refill rate is not a finite number above zero.
    #[error("throttle refill rate must be a finite number > 0, got {0}")]
    InvalidRate(f64),
    /// The burst is not a finite number of 1 or more.
    #[error("throttle burst must be a finite number >= 1, got {0}")]
    InvalidBurst(f64),
}

/// The bucket of one policy, in GCRA form.
///
/// `tat` is the theoretical arrival time of the next token, in seconds. A
/// start at `a` is admitted at `max(a, tat - tolerance)`.
struct Bucket {
    interval: f64,
    tolerance: f64,
    schedule_to_start: Option<f64>,
    tat: f64,
}

impl Bucket {
    fn new(policy: &ThrottlePolicy) -> Result<Self, ForecastError> {
        let rate = policy.refill_per_sec;
        if !rate.is_finite() || rate <= 0.0 {
            return Err(ForecastError::InvalidRate(rate));
        }
        if !policy.burst.is_finite() || policy.burst < 1.0 {
            return Err(ForecastError::InvalidBurst(policy.burst));
        }
        Ok(Self {
            interval: rate.recip(),
            tolerance: (policy.burst - 1.0) / rate,
            schedule_to_start: policy.schedule_to_start.map(|d| d.as_secs_f64()),
            tat: 0.0,
        })
    }

    /// Admit one burst. Start `j` of the burst is admitted at
    /// `max(a, tat + j * interval - tolerance)`. Waits grow with `j`, so the
    /// deadline drops a suffix of the burst.
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "counts and offsets stay far inside the exact range of f64"
    )]
    fn admit(&mut self, arrival: Arrival) -> BatchForecast {
        let a = arrival.at.as_secs_f64();
        let tat = self.tat.max(a);
        let admitted = match self.schedule_to_start {
            None => arrival.count,
            Some(deadline) => {
                let slack = a + deadline + self.tolerance - tat;
                if slack < 0.0 {
                    0
                } else {
                    let fits = (slack / self.interval).floor() + 1.0;
                    if fits >= arrival.count as f64 {
                        arrival.count
                    } else {
                        fits as u64
                    }
                }
            }
        };
        let start_of = |j: u64| a.max((j as f64).mul_add(self.interval, tat) - self.tolerance);
        let (first_admit, last_admit, max_wait) = if admitted == 0 {
            (None, None, Duration::ZERO)
        } else {
            let last = start_of(admitted - 1);
            (
                Some(seconds(start_of(0))),
                Some(seconds(last)),
                seconds(last - a),
            )
        };
        self.tat = (admitted as f64).mul_add(self.interval, tat);
        BatchForecast {
            at: arrival.at,
            count: arrival.count,
            admitted,
            dropped: arrival.count - admitted,
            first_admit,
            last_admit,
            max_wait,
        }
    }
}

/// Convert seconds to a duration, rounded to the nanosecond.
///
/// The rounding absorbs the float error of an inexact token interval.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "the value is clamped at zero, and the cast saturates"
)]
fn seconds(secs: f64) -> Duration {
    Duration::from_nanos((secs.max(0.0) * 1e9).round() as u64)
}

/// Forecast the admission of each burst in `arrivals`.
///
/// The bursts may come in any order. The result lists them by offset.
///
/// # Errors
/// Returns [`ForecastError`] when the policy has no valid rate or burst.
pub fn forecast(
    policy: &ThrottlePolicy,
    arrivals: &[Arrival],
) -> Result<ThrottleForecast, ForecastError> {
    let mut bucket = Bucket::new(policy)?;
    let mut sorted = arrivals.to_vec();
    sorted.sort_by_key(|arrival| arrival.at);
    Ok(ThrottleForecast {
        batches: sorted.into_iter().map(|a| bucket.admit(a)).collect(),
        truncated: false,
    })
}

/// Forecast the slots of `schedules` after `from`, up to and including `until`.
///
/// All schedules share one bucket, as the schedules of one unkeyed workflow
/// type do. Slots at one instant form one burst. The bucket is full at `from`.
///
/// # Errors
/// Returns [`ForecastError`] when the policy has no valid rate or burst.
pub fn forecast_schedules(
    policy: &ThrottlePolicy,
    schedules: &[Schedule],
    from: DateTime<Utc>,
    until: DateTime<Utc>,
) -> Result<ThrottleForecast, ForecastError> {
    // Validate before the walk, so a bad policy fails fast.
    Bucket::new(policy)?;
    let mut slots: BTreeMap<DateTime<Utc>, u64> = BTreeMap::new();
    let mut read = 0usize;
    let mut truncated = false;
    'walk: for schedule in schedules {
        let walker = SlotWalker::new(schedule);
        let mut next = walker.next_after(from);
        while let Some(slot) = next.filter(|slot| *slot <= until) {
            if read == MAX_FORECAST_SLOTS {
                truncated = true;
                break 'walk;
            }
            *slots.entry(slot).or_default() += 1;
            read += 1;
            next = walker.next_after(slot);
        }
    }
    let arrivals: Vec<Arrival> = slots
        .into_iter()
        .map(|(slot, count)| Arrival {
            at: (slot - from).to_std().unwrap_or_default(),
            count,
        })
        .collect();
    let mut result = forecast(policy, &arrivals)?;
    result.truncated = truncated;
    Ok(result)
}

/// Classify a burst of `fan_out` starts that repeats each `period`.
///
/// The bucket is full before the first burst. When the refill of one period
/// covers the burst, the bucket is full again before each later burst, so
/// every period has the same waits.
///
/// # Errors
/// Returns [`ForecastError`] when the policy has no valid rate or burst.
#[allow(
    clippy::cast_precision_loss,
    reason = "a fan-out stays far inside the exact range of f64"
)]
pub fn steady_state(
    policy: &ThrottlePolicy,
    fan_out: u64,
    period: Duration,
) -> Result<SteadyState, ForecastError> {
    Bucket::new(policy)?;
    let drain = fan_out as f64 / policy.refill_per_sec;
    let period = period.as_secs_f64();
    if drain <= period {
        let excess = fan_out as f64 - policy.burst;
        Ok(SteadyState::Keeps {
            worst_wait: seconds(excess / policy.refill_per_sec),
        })
    } else {
        Ok(SteadyState::FallsBehind {
            lag_per_period: seconds(drain - period),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn policy(rate: &str, burst: Option<f64>, sts: Option<Duration>) -> ThrottlePolicy {
        ThrottlePolicy::from_rate_str(rate, burst, None, sts).unwrap()
    }

    const fn secs(s: u64) -> Duration {
        Duration::from_secs(s)
    }

    fn at(s: u64, count: u64) -> Arrival {
        Arrival { at: secs(s), count }
    }

    #[test]
    fn a_burst_within_the_bucket_runs_at_once() {
        let f = forecast(&policy("10/s", None, None), &[at(0, 10)]).unwrap();
        assert_eq!(f.admitted(), 10);
        assert_eq!(f.max_wait(), Duration::ZERO);
        assert_eq!(f.drained_at(), Some(Duration::ZERO));
    }

    #[test]
    fn the_excess_waits_one_token_interval_each() {
        // 1/s with a burst of 5: five run at 0, then one each second.
        let f = forecast(&policy("1/s", Some(5.0), None), &[at(0, 8)]).unwrap();
        let b = f.batches[0];
        assert_eq!(b.admitted, 8);
        assert_eq!(b.first_admit, Some(Duration::ZERO));
        assert_eq!(b.last_admit, Some(secs(3)));
        assert_eq!(b.max_wait, secs(3));
    }

    #[test]
    fn a_fractional_burst_waits_for_the_missing_part() {
        // A burst of 2.5 admits two at 0. The third needs half a token.
        let f = forecast(&policy("2/s", Some(2.5), None), &[at(0, 3)]).unwrap();
        assert_eq!(f.drained_at(), Some(Duration::from_millis(250)));
    }

    #[test]
    fn schedule_to_start_drops_the_tail_of_a_burst() {
        // Waits are 0,0,1,2,3,4. A 2.5 s deadline keeps the first four.
        let p = policy("1/s", Some(2.0), Some(Duration::from_millis(2500)));
        let f = forecast(&p, &[at(0, 6)]).unwrap();
        assert_eq!(f.admitted(), 4);
        assert_eq!(f.dropped(), 2);
        assert_eq!(f.max_wait(), secs(2));
    }

    #[test]
    fn a_dropped_start_takes_no_token_from_a_later_burst() {
        let p = policy("1/s", Some(1.0), Some(Duration::from_millis(500)));
        // At 0: one runs, two drop. At 1: the token is back, so one runs at once.
        let f = forecast(&p, &[at(0, 3), at(1, 1)]).unwrap();
        assert_eq!(f.batches[0].dropped, 2);
        assert_eq!(f.batches[1].first_admit, Some(secs(1)));
        assert_eq!(f.batches[1].max_wait, Duration::ZERO);
    }

    #[test]
    fn a_backlog_carries_into_the_next_burst() {
        // At 0: 4 starts, burst 2, rate 1/s. They run at 0,0,1,2.
        // At 1: the next token is free at 3, so the new start waits 2 s.
        let f = forecast(&policy("1/s", Some(2.0), None), &[at(0, 4), at(1, 1)]).unwrap();
        assert_eq!(f.batches[1].first_admit, Some(secs(3)));
        assert_eq!(f.batches[1].max_wait, secs(2));
    }

    #[test]
    fn a_quiet_gap_refills_the_bucket_to_the_burst_only() {
        let f = forecast(&policy("1/s", Some(2.0), None), &[at(0, 2), at(100, 3)]).unwrap();
        // The bucket caps at 2, so the third start of the second burst waits.
        assert_eq!(f.batches[1].max_wait, secs(1));
    }

    #[test]
    fn arrivals_out_of_order_are_sorted() {
        let p = policy("1/s", Some(1.0), None);
        let sorted = forecast(&p, &[at(0, 2), at(5, 2)]).unwrap();
        let shuffled = forecast(&p, &[at(5, 2), at(0, 2)]).unwrap();
        assert_eq!(sorted, shuffled);
    }

    #[test]
    fn an_invalid_policy_is_refused() {
        let mut p = policy("1/s", None, None);
        p.refill_per_sec = 0.0;
        assert_eq!(forecast(&p, &[]), Err(ForecastError::InvalidRate(0.0)));
        let mut p = policy("1/s", None, None);
        p.burst = 0.5;
        assert_eq!(
            steady_state(&p, 1, secs(1)),
            Err(ForecastError::InvalidBurst(0.5))
        );
    }

    #[test]
    fn aligned_hourly_schedules_share_one_bucket() {
        // 300 tenants on one hourly cron, a 100/m throttle with a burst of 100.
        let schedules = vec![Schedule::Cron("0 0 * * * *".into()); 300];
        let from = Utc.with_ymd_and_hms(2026, 1, 1, 0, 30, 0).unwrap();
        let until = Utc.with_ymd_and_hms(2026, 1, 1, 2, 30, 0).unwrap();
        let f = forecast_schedules(&policy("100/m", None, None), &schedules, from, until).unwrap();
        assert_eq!(f.batches.len(), 2);
        assert_eq!(f.batches[0].at, secs(30 * 60));
        assert_eq!(f.batches[0].count, 300);
        // 200 starts over the burst at 0.6 s each: the last waits 120 s.
        assert_eq!(f.batches[0].max_wait, secs(120));
        assert_eq!(f.batches[1].max_wait, secs(120));
        assert!(!f.truncated);
    }

    #[test]
    fn staggered_schedules_wait_only_when_the_rate_is_short() {
        let schedules = vec![
            Schedule::Cron("0 0 * * * *".into()),
            Schedule::Cron("0 30 * * * *".into()),
        ];
        let from = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let until = Utc.with_ymd_and_hms(2026, 1, 1, 3, 0, 0).unwrap();
        let f = forecast_schedules(&policy("1/h", None, None), &schedules, from, until).unwrap();
        // Slots at 0:30, 1:00, 1:30, ... are 30 minutes apart; 1/h cannot keep up.
        assert!(f.max_wait() > Duration::ZERO);
        let f = forecast_schedules(&policy("2/h", None, None), &schedules, from, until).unwrap();
        assert_eq!(f.max_wait(), Duration::ZERO);
    }

    #[test]
    fn the_slot_walk_is_capped() {
        let schedules = [Schedule::Interval(secs(1))];
        let from = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let until = Utc.with_ymd_and_hms(2026, 2, 1, 0, 0, 0).unwrap();
        let f = forecast_schedules(&policy("10/s", None, None), &schedules, from, until).unwrap();
        assert!(f.truncated);
        assert_eq!(f.batches.len(), MAX_FORECAST_SLOTS);
    }

    #[test]
    fn manual_schedules_have_no_slots() {
        let from = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let until = Utc.with_ymd_and_hms(2026, 1, 2, 0, 0, 0).unwrap();
        let f = forecast_schedules(&policy("1/s", None, None), &[Schedule::Manual], from, until)
            .unwrap();
        assert_eq!(f.batches, Vec::new());
        assert_eq!(f.drained_at(), None);
    }

    #[test]
    fn steady_state_keeps_up_when_the_refill_covers_the_burst() {
        // 300 per hour against 100/m: the excess 200 drains in 120 s.
        let s = steady_state(&policy("100/m", None, None), 300, secs(3600)).unwrap();
        assert_eq!(
            s,
            SteadyState::Keeps {
                worst_wait: secs(120)
            }
        );
        let s = steady_state(&policy("100/m", None, None), 50, secs(3600)).unwrap();
        assert_eq!(
            s,
            SteadyState::Keeps {
                worst_wait: Duration::ZERO
            }
        );
    }

    #[test]
    fn steady_state_falls_behind_when_the_refill_is_short() {
        // 10 each second against 5/s: each period adds 1 s of lag.
        let s = steady_state(&policy("5/s", None, None), 10, secs(1)).unwrap();
        assert_eq!(
            s,
            SteadyState::FallsBehind {
                lag_per_period: secs(1)
            }
        );
    }

    /// A direct token-bucket model, one start at a time, with no GCRA algebra.
    fn simulate(p: &ThrottlePolicy, arrivals: &[Arrival]) -> (u64, Duration) {
        let (mut tokens, mut clock, mut admitted, mut worst) = (p.burst, 0.0_f64, 0, 0.0_f64);
        for arrival in arrivals {
            let a = arrival.at.as_secs_f64();
            for _ in 0..arrival.count {
                let mut t = a.max(clock);
                tokens = (t - clock).mul_add(p.refill_per_sec, tokens).min(p.burst);
                if tokens < 1.0 {
                    let wait = (1.0 - tokens) / p.refill_per_sec;
                    if p.schedule_to_start
                        .is_some_and(|d| t + wait - a > d.as_secs_f64())
                    {
                        continue;
                    }
                    t += wait;
                    tokens = 1.0;
                }
                tokens -= 1.0;
                clock = t;
                admitted += 1;
                worst = worst.max(t - a);
            }
        }
        (admitted, seconds(worst))
    }

    #[test]
    fn the_forecast_matches_a_direct_bucket_model() {
        let deadline = Some(Duration::from_millis(3700));
        for p in [
            policy("1/s", Some(3.0), None),
            policy("7/s", Some(2.5), None),
            policy("1/s", Some(3.0), deadline),
            policy("3/s", Some(1.0), deadline),
        ] {
            let arrivals: Vec<_> = (0..30).map(|i| at(i * 2 + i % 3, (i * 7) % 11)).collect();
            let f = forecast(&p, &arrivals).unwrap();
            let (admitted, worst) = simulate(&p, &arrivals);
            assert_eq!(f.admitted(), admitted, "{p:?}");
            assert_eq!(f.dropped() > 0, p.schedule_to_start.is_some(), "{p:?}");
            let gap = f.max_wait().abs_diff(worst);
            assert!(gap < Duration::from_micros(1), "{p:?}: {gap:?}");
        }
    }

    #[test]
    fn steady_state_agrees_with_the_forecast() {
        for (rate, burst, n, period) in [
            ("1/s", 3.0, 7, 10),
            ("1/s", 3.0, 12, 10),
            ("100/m", 100.0, 300, 3600),
            ("5/s", 5.0, 10, 1),
        ] {
            let p = policy(rate, Some(burst), None);
            let arrivals: Vec<_> = (0..20).map(|i| at(i * period, n)).collect();
            let f = forecast(&p, &arrivals).unwrap();
            let first = f.batches[0].max_wait;
            let last = f.batches[19].max_wait;
            match steady_state(&p, n, secs(period)).unwrap() {
                SteadyState::Keeps { worst_wait } => {
                    assert_eq!(first, worst_wait, "{rate} {n}");
                    assert_eq!(last, worst_wait, "{rate} {n}");
                }
                SteadyState::FallsBehind { lag_per_period } => {
                    assert_eq!(
                        last.checked_sub(first),
                        Some(lag_per_period * 19),
                        "{rate} {n}"
                    );
                }
            }
        }
    }
}
