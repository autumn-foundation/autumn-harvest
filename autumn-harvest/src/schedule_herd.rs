//! Schedule herd forecast: which schedules fire at the same moment.
//!
//! Each schedule is checked alone, so nothing warns when twenty of them all
//! say `0 * * * *`. At the top of each hour the scheduler then starts twenty
//! runs at once, and their activities hit the workers together. This module
//! walks every schedule over a window with [`SlotWalker`]. It groups the
//! fires into buckets of fixed width and reports each bucket that holds many
//! schedules.
//!
//! Only cron schedules have a fixed phase. An interval schedule counts from
//! the end of its previous run, so its fire times depend on history. A
//! manual schedule never fires. The forecast lists both kinds as unanchored
//! and leaves them out of every bucket.
//!
//! The module is pure. It sends no SQL and reads no clock.

use std::collections::BTreeMap;
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::policy::Schedule;
use crate::schedule_plan::SlotWalker;

/// The largest number of fires that the forecast reads from one schedule.
///
/// A `* * * * * *` cron fires each second. This cap keeps a long window finite.
pub const MAX_FIRES_PER_SCHEDULE: usize = 100_000;

/// One bucket in which several schedules fire together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HerdSpot {
    /// The start of the bucket.
    pub at: DateTime<Utc>,
    /// The names of the schedules that fire in the bucket, sorted and unique.
    pub schedules: Vec<String>,
}

impl HerdSpot {
    /// The number of distinct schedules in the bucket.
    #[must_use]
    pub const fn size(&self) -> usize {
        self.schedules.len()
    }
}

/// The result of a herd forecast.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HerdForecast {
    /// The total number of fires that the forecast read.
    pub fires: usize,
    /// The buckets with at least `min_herd` schedules.
    ///
    /// The largest bucket comes first. Buckets of equal size are in time order.
    pub spots: Vec<HerdSpot>,
    /// The schedules with no fixed phase: intervals and manual schedules.
    pub unanchored: Vec<String>,
    /// The schedules that reached [`MAX_FIRES_PER_SCHEDULE`] in the window.
    ///
    /// The forecast omits their later fires.
    pub truncated: Vec<String>,
}

impl HerdForecast {
    /// The largest bucket, if any bucket reaches `min_herd`.
    #[must_use]
    pub fn peak(&self) -> Option<&HerdSpot> {
        self.spots.first()
    }

    /// For each schedule, the number of spots that include it.
    ///
    /// A schedule with a high count is the best one to move to another minute.
    /// The highest count comes first. Equal counts are in name order.
    #[must_use]
    pub fn offenders(&self) -> Vec<(String, usize)> {
        let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
        for spot in &self.spots {
            for name in &spot.schedules {
                *counts.entry(name).or_default() += 1;
            }
        }
        let mut ranked: Vec<(String, usize)> = counts
            .into_iter()
            .map(|(name, count)| (name.to_string(), count))
            .collect();
        ranked.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        ranked
    }
}

/// Forecast the herds of `schedules` in the window `[from, until)`.
///
/// Each fire goes into the bucket that starts at a whole multiple of `bucket`
/// since the Unix epoch. A `bucket` under one second counts as one second.
/// A bucket becomes a spot when it holds at least `min_herd` distinct
/// schedules. A `min_herd` of 0 counts as 2, because one schedule alone is no
/// herd.
#[must_use]
pub fn forecast(
    schedules: &[(&str, &Schedule)],
    from: DateTime<Utc>,
    until: DateTime<Utc>,
    bucket: Duration,
    min_herd: usize,
) -> HerdForecast {
    forecast_shifted(schedules, from, until, bucket, min_herd, |_, fire| fire)
}

/// [`forecast`] with a shift applied to each fire before it goes into a bucket.
///
/// `shift` gets the index of the schedule in `schedules` and the slot time.
/// It returns the time at which the run really starts. The scheduler adds a
/// jitter offset to each slot, so pass that offset here. The window check uses
/// the slot time, as the scheduler does.
#[must_use]
pub fn forecast_shifted(
    schedules: &[(&str, &Schedule)],
    from: DateTime<Utc>,
    until: DateTime<Utc>,
    bucket: Duration,
    min_herd: usize,
    shift: impl Fn(usize, DateTime<Utc>) -> DateTime<Utc>,
) -> HerdForecast {
    // The width counts in milliseconds, so a fractional bucket keeps its size.
    let width = i64::try_from(bucket.as_millis())
        .unwrap_or(i64::MAX)
        .max(1_000);
    let min_herd = if min_herd == 0 { 2 } else { min_herd };
    let mut buckets: BTreeMap<i64, Vec<String>> = BTreeMap::new();
    let mut fires = 0;
    let mut unanchored = Vec::new();
    let mut truncated = Vec::new();

    for (index, &(name, schedule)) in schedules.iter().enumerate() {
        if !matches!(
            schedule,
            Schedule::Cron(_) | Schedule::CronInTimezone { .. }
        ) {
            unanchored.push(name.to_string());
            continue;
        }
        let walker = SlotWalker::new(schedule);
        // The walk looks for fires strictly after its reference. So it starts
        // one second before `from` to include a fire at `from` itself.
        let mut cursor = from - chrono::Duration::seconds(1);
        let mut read = 0;
        while let Some(next) = walker.next_after(cursor).filter(|&t| t < until) {
            cursor = next;
            if next < from {
                continue;
            }
            if read == MAX_FIRES_PER_SCHEDULE {
                truncated.push(name.to_string());
                break;
            }
            read += 1;
            let key = shift(index, next).timestamp_millis().div_euclid(width);
            buckets.entry(key).or_default().push(name.to_string());
        }
        fires += read;
    }

    let mut spots: Vec<HerdSpot> = buckets
        .into_iter()
        .filter_map(|(key, mut members)| {
            // A caller can pass one name twice, so dedup before the threshold.
            members.sort();
            members.dedup();
            let at = DateTime::from_timestamp_millis(key.saturating_mul(width))?;
            (members.len() >= min_herd).then_some(HerdSpot {
                at,
                schedules: members,
            })
        })
        .collect();
    spots.sort_by(|a, b| b.size().cmp(&a.size()).then_with(|| a.at.cmp(&b.at)));

    HerdForecast {
        fires,
        spots,
        unanchored,
        truncated,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::{DateTime, Utc};

    use super::{MAX_FIRES_PER_SCHEDULE, forecast, forecast_shifted};
    use crate::policy::Schedule;

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).unwrap()
    }

    fn cron(expr: &str) -> Schedule {
        Schedule::Cron(expr.to_string())
    }

    const HOUR: i64 = 3_600;
    const MINUTE: Duration = Duration::from_secs(60);

    #[test]
    fn hourly_schedules_herd_at_the_top_of_each_hour() {
        let a = cron("0 * * * *");
        let b = cron("0 * * * *");
        let c = cron("17 * * * *");
        let report = forecast(
            &[("a", &a), ("b", &b), ("c", &c)],
            at(0),
            at(3 * HOUR),
            MINUTE,
            2,
        );
        assert_eq!(report.fires, 9);
        let times: Vec<_> = report.spots.iter().map(|s| s.at).collect();
        assert_eq!(times, vec![at(0), at(HOUR), at(2 * HOUR)]);
        assert!(report.spots.iter().all(|s| s.schedules == ["a", "b"]));
        assert_eq!(report.peak().map(super::HerdSpot::size), Some(2));
    }

    #[test]
    fn a_wider_bucket_joins_nearby_fires() {
        let a = cron("0 * * * *");
        let c = cron("4 * * * *");
        let narrow = forecast(&[("a", &a), ("c", &c)], at(0), at(HOUR), MINUTE, 2);
        assert_eq!(narrow.spots, []);
        let wide = forecast(
            &[("a", &a), ("c", &c)],
            at(0),
            at(HOUR),
            Duration::from_secs(300),
            2,
        );
        assert_eq!(wide.spots.len(), 1);
        assert_eq!(wide.spots[0].at, at(0));
    }

    #[test]
    fn the_largest_spot_comes_first() {
        let a = cron("0 * * * *");
        let b = cron("0 * * * *");
        let d = cron("0 */2 * * *");
        let report = forecast(
            &[("a", &a), ("b", &b), ("d", &d)],
            at(0),
            at(4 * HOUR),
            MINUTE,
            2,
        );
        let sizes: Vec<_> = report.spots.iter().map(super::HerdSpot::size).collect();
        assert_eq!(sizes, vec![3, 3, 2, 2]);
        assert_eq!(report.spots[0].at, at(0));
        assert_eq!(report.spots[1].at, at(2 * HOUR));
        assert_eq!(
            report.offenders(),
            vec![
                ("a".to_string(), 4),
                ("b".to_string(), 4),
                ("d".to_string(), 2)
            ]
        );
    }

    #[test]
    fn one_schedule_alone_is_never_a_herd() {
        let every_second = cron("* * * * * *");
        let report = forecast(&[("s", &every_second)], at(0), at(120), MINUTE, 0);
        assert_eq!(report.fires, 120);
        assert_eq!(report.spots, []);
    }

    #[test]
    fn min_herd_filters_small_spots() {
        let a = cron("0 * * * *");
        let b = cron("0 * * * *");
        let report = forecast(&[("a", &a), ("b", &b)], at(0), at(HOUR), MINUTE, 3);
        assert_eq!(report.spots, []);
        assert!(report.peak().is_none());
    }

    #[test]
    fn the_window_includes_from_and_excludes_until() {
        let a = cron("0 * * * *");
        let b = cron("0 * * * *");
        let report = forecast(&[("a", &a), ("b", &b)], at(HOUR), at(2 * HOUR), MINUTE, 2);
        assert_eq!(report.fires, 2);
        assert_eq!(report.spots.len(), 1);
        assert_eq!(report.spots[0].at, at(HOUR));
        let late = DateTime::from_timestamp(HOUR, 500_000_000).unwrap();
        let none = forecast(&[("a", &a), ("b", &b)], late, at(2 * HOUR), MINUTE, 2);
        assert_eq!(none.fires, 0);
    }

    #[test]
    fn interval_and_manual_schedules_are_unanchored() {
        let a = cron("0 * * * *");
        let every = Schedule::Interval(Duration::from_secs(3_600));
        let report = forecast(
            &[("a", &a), ("every", &every), ("manual", &Schedule::Manual)],
            at(0),
            at(HOUR),
            MINUTE,
            2,
        );
        assert_eq!(report.unanchored, ["every", "manual"]);
        assert_eq!(report.fires, 1);
        assert_eq!(report.spots, []);
    }

    #[test]
    fn zones_shift_the_phase_of_a_cron() {
        let utc = cron("0 9 * * *");
        let tokyo = Schedule::CronInTimezone {
            expr: "0 18 * * *".to_string(),
            tz: "Asia/Tokyo".to_string(),
        };
        let report = forecast(
            &[("utc", &utc), ("tokyo", &tokyo)],
            at(0),
            at(24 * HOUR),
            MINUTE,
            2,
        );
        assert_eq!(report.spots.len(), 1);
        assert_eq!(report.spots[0].at, at(9 * HOUR));
        assert_eq!(report.spots[0].schedules, ["tokyo", "utc"]);
    }

    #[test]
    fn a_dense_schedule_is_truncated() {
        let every_second = cron("* * * * * *");
        let limit = i64::try_from(MAX_FIRES_PER_SCHEDULE).unwrap();
        let report = forecast(&[("s", &every_second)], at(0), at(limit + 10), MINUTE, 2);
        assert_eq!(report.fires, MAX_FIRES_PER_SCHEDULE);
        assert_eq!(report.truncated, ["s"]);
    }

    #[test]
    fn a_fractional_bucket_keeps_its_width() {
        let two = cron("2 * * * * *");
        let three = cron("3 * * * * *");
        let width = Duration::from_millis(2_500);
        let apart = forecast(&[("two", &two), ("three", &three)], at(0), at(4), width, 2);
        assert_eq!(apart.spots, []);
        let five = cron("5 * * * * *");
        let six = cron("6 * * * * *");
        let join = forecast(&[("five", &five), ("six", &six)], at(0), at(10), width, 2);
        let start = DateTime::from_timestamp_millis(5_000).unwrap();
        assert_eq!(join.spots[0].at, start);
    }

    #[test]
    fn a_shift_moves_a_fire_out_of_its_herd() {
        let a = cron("0 * * * *");
        let b = cron("0 * * * *");
        let schedules = [("a", &a), ("b", &b)];
        let jitter = |index: usize, t: DateTime<Utc>| {
            if index == 1 {
                t + chrono::Duration::seconds(90)
            } else {
                t
            }
        };
        let apart = forecast_shifted(&schedules, at(0), at(HOUR), MINUTE, 2, jitter);
        assert_eq!(apart.fires, 2);
        assert_eq!(apart.spots, []);
        let still = forecast_shifted(&schedules, at(0), at(HOUR), MINUTE, 2, |_, t| t);
        assert_eq!(still, forecast(&schedules, at(0), at(HOUR), MINUTE, 2));
    }

    #[test]
    fn a_repeated_name_counts_once() {
        let a = cron("0 * * * *");
        let b = cron("0 * * * *");
        let report = forecast(
            &[("a", &a), ("b", &b), ("a", &a)],
            at(0),
            at(HOUR),
            MINUTE,
            3,
        );
        assert_eq!(report.spots, []);
        let pair = forecast(
            &[("a", &a), ("b", &b), ("a", &a)],
            at(0),
            at(HOUR),
            MINUTE,
            2,
        );
        assert_eq!(pair.spots[0].schedules, ["a", "b"]);
    }

    #[test]
    fn a_bad_cron_fires_nothing() {
        let bad = cron("not a cron");
        let a = cron("0 * * * *");
        let report = forecast(&[("bad", &bad), ("a", &a)], at(0), at(HOUR), MINUTE, 2);
        assert_eq!(report.fires, 1);
        assert_eq!(report.spots, []);
        assert_eq!(report.unanchored, Vec::<String>::new());
    }
}
