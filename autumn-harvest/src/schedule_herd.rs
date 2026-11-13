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

/// What [`forecast_shifted`] does with one slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fire {
    /// The run of the slot starts at this time.
    Start(DateTime<Utc>),
    /// The slot never runs. The walk goes on to the next slot.
    Skip,
    /// The schedule runs no more. The walk of this schedule ends.
    Stop,
}

/// The result of a herd forecast.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HerdForecast {
    /// The number of fires that start inside the window.
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
    forecast_shifted(
        schedules,
        from,
        until,
        bucket,
        min_herd,
        Duration::ZERO,
        |_, fire| Fire::Start(fire),
    )
}

/// [`forecast`] with a shift applied to each fire before it goes into a bucket.
///
/// `shift` gets the index of the schedule in `schedules` and the slot time.
/// It returns the time at which the run really starts. The scheduler adds a
/// jitter offset to each slot, so pass that offset here.
///
/// The window bounds the start, not the slot. A slot before `from` can start
/// inside the window, so the walk begins `lookback` before `from`. Pass the
/// largest shift here. A start outside `[from, until)` is not counted, but
/// `shift` still sees its slot.
///
/// [`Fire::Skip`] passes over a slot that the scheduler never dispatches, such
/// as one before its next pending slot. [`Fire::Stop`] ends the walk of that
/// schedule. A run budget or an end time stops a schedule in this way. The
/// calls for one schedule come in slot order.
#[must_use]
pub fn forecast_shifted(
    schedules: &[(&str, &Schedule)],
    from: DateTime<Utc>,
    until: DateTime<Utc>,
    bucket: Duration,
    min_herd: usize,
    lookback: Duration,
    mut shift: impl FnMut(usize, DateTime<Utc>) -> Fire,
) -> HerdForecast {
    // The width counts in milliseconds, so a fractional bucket keeps its size.
    let width = i64::try_from(bucket.as_millis())
        .unwrap_or(i64::MAX)
        .max(1_000);
    let min_herd = if min_herd == 0 { 2 } else { min_herd };
    // Each bucket holds schedule indices, not names. A schedule that fires
    // again in the same bucket adds no entry, so the memory follows the
    // distinct members and not the fires.
    let mut buckets: BTreeMap<i64, Vec<usize>> = BTreeMap::new();
    let mut fires = 0;
    let walk_from = chrono::Duration::from_std(lookback)
        .ok()
        .and_then(|back| from.checked_sub_signed(back))
        .unwrap_or(from);
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
        // one second before `walk_from` to include a fire at `walk_from` itself.
        let mut cursor = walk_from - chrono::Duration::seconds(1);
        let mut read = 0;
        while let Some(next) = walker.next_after(cursor).filter(|&t| t < until) {
            cursor = next;
            if next < walk_from {
                continue;
            }
            if read == MAX_FIRES_PER_SCHEDULE {
                truncated.push(name.to_string());
                break;
            }
            let start = match shift(index, next) {
                Fire::Start(start) => start,
                Fire::Skip => continue,
                Fire::Stop => break,
            };
            read += 1;
            if start < from || start >= until {
                continue;
            }
            fires += 1;
            let key = start.timestamp_millis().div_euclid(width);
            let members = buckets.entry(key).or_default();
            if members.last() != Some(&index) {
                members.push(index);
            }
        }
    }

    let mut spots: Vec<HerdSpot> = buckets
        .into_iter()
        .filter_map(|(key, mut members)| {
            // A jittered schedule can come back to a bucket after another one.
            members.sort_unstable();
            members.dedup();
            if members.len() < min_herd {
                return None;
            }
            // A caller can pass one name twice, so dedup names before the
            // threshold too.
            let mut names: Vec<String> = members
                .into_iter()
                .map(|index| schedules[index].0.to_string())
                .collect();
            names.sort();
            names.dedup();
            let at = DateTime::from_timestamp_millis(key.saturating_mul(width))?;
            (names.len() >= min_herd).then_some(HerdSpot {
                at,
                schedules: names,
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

    use super::{Fire, MAX_FIRES_PER_SCHEDULE, forecast, forecast_shifted};
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
            Fire::Start(if index == 1 {
                t + chrono::Duration::seconds(90)
            } else {
                t
            })
        };
        let apart = forecast_shifted(
            &schedules,
            at(0),
            at(HOUR),
            MINUTE,
            2,
            Duration::ZERO,
            jitter,
        );
        assert_eq!(apart.fires, 2);
        assert_eq!(apart.spots, []);
        let still = forecast_shifted(
            &schedules,
            at(0),
            at(HOUR),
            MINUTE,
            2,
            Duration::ZERO,
            |_, t| Fire::Start(t),
        );
        assert_eq!(still, forecast(&schedules, at(0), at(HOUR), MINUTE, 2));
    }

    #[test]
    fn a_stop_ends_the_walk_of_its_schedule() {
        let a = cron("0 * * * *");
        let b = cron("0 * * * *");
        let schedules = [("a", &a), ("b", &b)];
        let cap = |index: usize, t: DateTime<Utc>| {
            if index == 0 || t < at(HOUR) {
                Fire::Start(t)
            } else {
                Fire::Stop
            }
        };
        let report = forecast_shifted(
            &schedules,
            at(0),
            at(3 * HOUR),
            MINUTE,
            2,
            Duration::ZERO,
            cap,
        );
        assert_eq!(report.fires, 4, "a fires 3 times, b only once");
        assert_eq!(report.spots.len(), 1);
        assert_eq!(report.spots[0].at, at(0));
    }

    #[test]
    fn many_dense_schedules_make_one_herd_per_bucket() {
        let every_second = cron("* * * * * *");
        let names: Vec<String> = (0..50).map(|n| format!("s{n}")).collect();
        let schedules: Vec<(&str, &Schedule)> =
            names.iter().map(|n| (n.as_str(), &every_second)).collect();
        let report = forecast(&schedules, at(0), at(HOUR), MINUTE, 2);
        assert_eq!(report.fires, 50 * 3_600);
        assert_eq!(report.spots.len(), 60);
        assert!(report.spots.iter().all(|s| s.size() == 50));
    }

    #[test]
    fn the_window_bounds_the_shifted_start_not_the_slot() {
        let a = cron("0 * * * *");
        let b = cron("0 * * * *");
        let schedules = [("a", &a), ("b", &b)];
        let late = |_: usize, t: DateTime<Utc>| Fire::Start(t + chrono::Duration::minutes(5));
        let back = Duration::from_secs(600);
        // The 00:00 slots are before the window but start at 00:05, inside it.
        let early = forecast_shifted(&schedules, at(60), at(HOUR), MINUTE, 2, back, late);
        assert_eq!(early.fires, 2);
        assert_eq!(early.spots.len(), 1);
        assert_eq!(early.spots[0].at, at(300));
        // With no lookback the walk never sees those slots.
        let blind = forecast_shifted(
            &schedules,
            at(60),
            at(HOUR),
            MINUTE,
            2,
            Duration::ZERO,
            late,
        );
        assert_eq!(blind.fires, 0);
        // The 00:00 slots are inside a window that ends at 00:03, but start
        // after it.
        let cut = forecast_shifted(&schedules, at(0), at(180), MINUTE, 2, back, late);
        assert_eq!(cut.fires, 0);
        assert_eq!(cut.spots, []);
    }

    #[test]
    fn a_skip_passes_over_one_slot() {
        let a = cron("0 * * * *");
        let b = cron("0 * * * *");
        let schedules = [("a", &a), ("b", &b)];
        let skip_first = |index: usize, t: DateTime<Utc>| {
            if index == 1 && t == at(0) {
                Fire::Skip
            } else {
                Fire::Start(t)
            }
        };
        let report = forecast_shifted(
            &schedules,
            at(0),
            at(3 * HOUR),
            MINUTE,
            2,
            Duration::ZERO,
            skip_first,
        );
        assert_eq!(
            report.fires, 5,
            "b skips 00:00 and fires at 01:00 and 02:00"
        );
        let times: Vec<_> = report.spots.iter().map(|s| s.at).collect();
        assert_eq!(times, vec![at(HOUR), at(2 * HOUR)]);
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
