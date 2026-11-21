//! Schedule stagger planner: move herded schedules apart.
//!
//! [`schedule_herd`](crate::schedule_herd) finds the buckets in which many
//! cron schedules fire together. It names the offenders, but it does not say
//! where to move them. This module answers that question. It gives each cron
//! schedule a fixed delay, so that the largest herd becomes as small as it can.
//!
//! The planner is greedy. It visits the schedules in offender order, the most
//! herded one first. For each one, it tries every delay from zero to `max` in
//! steps of `step`. The other schedules keep their current delays. It keeps the
//! delay with the smallest peak. A tie goes to the smaller crowding total, and
//! then to the smaller delay. So a schedule that is not in a herd never moves.
//!
//! A delay of zero is always a candidate. So the plan is never worse than the
//! start, and the peak after the plan is at most the peak before it.
//!
//! A delay maps onto a cron edit. A delay of 7 minutes on `0 * * * *` gives
//! `7 * * * *`. The planner does not write the expression, because a delay can
//! cross an hour or a day boundary of a cron field.
//!
//! The module is pure. It sends no SQL and reads no clock. Each candidate costs
//! one herd forecast, so keep the window short. One cycle of the schedules is
//! enough.

use std::time::Duration;

use jiff::{SignedDuration, Timestamp};

use crate::policy::Schedule;
use crate::schedule_herd::{Fire, HerdForecast, forecast, forecast_shifted};

/// The largest number of delays that the planner tries for one schedule.
///
/// A `step` that is small against `max` gives too many candidates. The planner
/// then widens the step so that the count stays at this bound.
pub const MAX_CANDIDATE_DELAYS: u32 = 120;

/// The delay that the plan gives to one schedule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaggerMove {
    /// The name of the schedule.
    pub schedule: String,
    /// The fixed delay to add to each fire of the schedule.
    pub delay: Duration,
}

/// The result of a stagger plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaggerPlan {
    /// The herd forecast with no delays.
    pub before: HerdForecast,
    /// The herd forecast with the delays of [`moves`](Self::moves).
    pub after: HerdForecast,
    /// The schedules that move, in the order that the planner chose them.
    ///
    /// A schedule with a zero delay is not in the list.
    pub moves: Vec<StaggerMove>,
}

impl StaggerPlan {
    /// The size of the largest herd before the plan, or 0 with no herd.
    #[must_use]
    pub fn peak_before(&self) -> usize {
        self.before.peak().map_or(0, |spot| spot.size())
    }

    /// The size of the largest herd after the plan, or 0 with no herd.
    #[must_use]
    pub fn peak_after(&self) -> usize {
        self.after.peak().map_or(0, |spot| spot.size())
    }
}

/// The settings of a stagger plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StaggerWindow {
    /// The start of the forecast window, included.
    pub from: Timestamp,
    /// The end of the forecast window, excluded.
    pub until: Timestamp,
    /// The width of one herd bucket, as in [`forecast`].
    pub bucket: Duration,
    /// The distance between two candidate delays.
    ///
    /// A step under one second counts as one second.
    pub step: Duration,
    /// The largest delay that the plan can give, included.
    pub max: Duration,
}

/// Plan fixed delays that make the largest herd of `schedules` small.
///
/// Interval and manual schedules have no fixed phase, so they never move.
/// They stay in the `unanchored` list of each forecast.
#[must_use]
pub fn plan(schedules: &[(&str, &Schedule)], window: StaggerWindow) -> StaggerPlan {
    let before = forecast(schedules, window.from, window.until, window.bucket, 2);
    let candidates = candidate_delays(window.step, window.max);
    let mut delays = vec![Duration::ZERO; schedules.len()];
    let mut moves = Vec::new();

    for index in visit_order(schedules, &before) {
        let mut best: Option<((usize, usize), Duration)> = None;
        for &delay in &candidates {
            delays[index] = delay;
            let score = score(&shifted(schedules, window, &delays));
            if best.is_none_or(|(top, _)| score < top) {
                best = Some((score, delay));
            }
        }
        let chosen = best.map_or(Duration::ZERO, |(_, delay)| delay);
        delays[index] = chosen;
        if !chosen.is_zero() {
            moves.push(StaggerMove {
                schedule: schedules[index].0.to_string(),
                delay: chosen,
            });
        }
    }

    let after = shifted(schedules, window, &delays);
    StaggerPlan {
        before,
        after,
        moves,
    }
}

/// The delays to try, from zero to `max` in steps of `step`.
fn candidate_delays(step: Duration, max: Duration) -> Vec<Duration> {
    let step = step.max(Duration::from_secs(1));
    let wanted = max.as_secs_f64() / step.as_secs_f64();
    let step = if wanted > f64::from(MAX_CANDIDATE_DELAYS) {
        max / MAX_CANDIDATE_DELAYS
    } else {
        step
    };
    let mut delays = vec![Duration::ZERO];
    let mut next = step;
    while next <= max && !step.is_zero() {
        delays.push(next);
        next += step;
    }
    delays
}

/// The anchored schedules, the most herded first.
///
/// A schedule in no herd comes last, in input order. The planner still visits
/// it, because a move of another schedule can bring a herd onto it.
fn visit_order(schedules: &[(&str, &Schedule)], before: &HerdForecast) -> Vec<usize> {
    let ranks = before.offenders();
    let rank_of = |name: &str| {
        ranks
            .iter()
            .find(|(offender, _)| offender == name)
            .map_or(0, |(_, count)| *count)
    };
    let mut order: Vec<usize> = (0..schedules.len())
        .filter(|&index| {
            matches!(
                schedules[index].1,
                Schedule::Cron(_) | Schedule::CronInTimezone { .. }
            )
        })
        .collect();
    order.sort_by_key(|&index| std::cmp::Reverse(rank_of(schedules[index].0)));
    order
}

/// The herd forecast with a fixed delay on each schedule.
fn shifted(
    schedules: &[(&str, &Schedule)],
    window: StaggerWindow,
    delays: &[Duration],
) -> HerdForecast {
    forecast_shifted(
        schedules,
        window.from,
        window.until,
        window.bucket,
        2,
        window.max,
        |index, slot| {
            SignedDuration::try_from(delays[index])
                .ok()
                .and_then(|delay| slot.checked_add(delay).ok())
                .map_or(Fire::Skip, Fire::Start)
        },
    )
}

/// The peak size, then the crowding total over all spots.
///
/// The crowding total counts each schedule once in each spot. A lower total
/// means fewer schedules share a bucket.
fn score(report: &HerdForecast) -> (usize, usize) {
    let peak = report.peak().map_or(0, |spot| spot.size());
    let crowding = report.spots.iter().map(|spot| spot.size()).sum();
    (peak, crowding)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use jiff::Timestamp;

    use super::{MAX_CANDIDATE_DELAYS, StaggerWindow, candidate_delays, plan};
    use crate::policy::Schedule;

    const HOUR: i64 = 3_600;
    const MINUTE: Duration = Duration::from_secs(60);

    fn at(secs: i64) -> Timestamp {
        Timestamp::from_second(secs).unwrap()
    }

    fn cron(expr: &str) -> Schedule {
        Schedule::Cron(expr.to_string())
    }

    fn hour_window(max_minutes: u64) -> StaggerWindow {
        StaggerWindow {
            from: at(0),
            until: at(2 * HOUR),
            bucket: MINUTE,
            step: MINUTE,
            max: MINUTE * u32::try_from(max_minutes).unwrap(),
        }
    }

    #[test]
    fn a_herd_of_four_hourly_schedules_spreads_to_no_herd() {
        let crons: Vec<Schedule> = (0..4).map(|_| cron("0 * * * *")).collect();
        let names = ["a", "b", "c", "d"];
        let schedules: Vec<(&str, &Schedule)> = names.iter().copied().zip(crons.iter()).collect();
        let result = plan(&schedules, hour_window(10));
        assert_eq!(result.peak_before(), 4);
        assert_eq!(result.peak_after(), 0);
        assert_eq!(result.after.spots, []);
        assert_eq!(result.moves.len(), 3);
        assert_eq!(result.after.fires, result.before.fires);
    }

    #[test]
    fn the_plan_takes_the_smallest_delay_that_breaks_a_herd() {
        let a = cron("0 * * * *");
        let b = cron("0 * * * *");
        let result = plan(&[("a", &a), ("b", &b)], hour_window(30));
        assert_eq!(result.moves.len(), 1);
        assert_eq!(result.moves[0].delay, MINUTE);
        assert_eq!(result.peak_after(), 0);
    }

    #[test]
    fn a_lone_schedule_never_moves() {
        let a = cron("0 * * * *");
        let b = cron("0 * * * *");
        let c = cron("30 * * * *");
        let result = plan(&[("a", &a), ("b", &b), ("c", &c)], hour_window(10));
        assert!(result.moves.iter().all(|m| m.schedule != "c"));
        assert_eq!(result.peak_after(), 0);
    }

    #[test]
    fn a_move_never_lands_on_another_schedule() {
        let a = cron("0 * * * *");
        let b = cron("0 * * * *");
        let c = cron("1 * * * *");
        let result = plan(&[("a", &a), ("b", &b), ("c", &c)], hour_window(2));
        assert_eq!(result.peak_after(), 0);
        assert_eq!(result.moves.len(), 1);
        assert_eq!(result.moves[0].delay, MINUTE * 2);
    }

    #[test]
    fn a_zero_max_keeps_the_herd_and_moves_nothing() {
        let a = cron("0 * * * *");
        let b = cron("0 * * * *");
        let result = plan(&[("a", &a), ("b", &b)], hour_window(0));
        assert_eq!(result.moves, []);
        assert_eq!(result.peak_after(), 2);
        assert_eq!(result.after, result.before);
    }

    #[test]
    fn a_short_max_reduces_the_peak_as_far_as_it_can() {
        let crons: Vec<Schedule> = (0..6).map(|_| cron("0 * * * *")).collect();
        let names = ["a", "b", "c", "d", "e", "f"];
        let schedules: Vec<(&str, &Schedule)> = names.iter().copied().zip(crons.iter()).collect();
        let result = plan(&schedules, hour_window(2));
        assert_eq!(result.peak_before(), 6);
        assert_eq!(result.peak_after(), 2);
    }

    #[test]
    fn unanchored_schedules_never_move() {
        let a = cron("0 * * * *");
        let b = cron("0 * * * *");
        let every = Schedule::Interval(Duration::from_secs(3_600));
        let result = plan(
            &[
                ("a", &a),
                ("b", &b),
                ("every", &every),
                ("manual", &Schedule::Manual),
            ],
            hour_window(5),
        );
        assert!(
            result
                .moves
                .iter()
                .all(|m| m.schedule == "a" || m.schedule == "b")
        );
        assert_eq!(result.after.unanchored, ["every", "manual"]);
    }

    #[test]
    fn the_peak_after_is_never_above_the_peak_before() {
        let crons = [
            cron("0 * * * *"),
            cron("*/15 * * * *"),
            cron("0,30 * * * *"),
            cron("0 */2 * * *"),
            cron("45 * * * *"),
        ];
        let names = ["a", "b", "c", "d", "e"];
        let schedules: Vec<(&str, &Schedule)> = names.iter().copied().zip(crons.iter()).collect();
        let result = plan(&schedules, hour_window(7));
        assert!(result.peak_after() <= result.peak_before());
        assert!(result.peak_after() < result.peak_before());
    }

    #[test]
    fn a_fine_step_is_widened_to_the_candidate_bound() {
        let delays = candidate_delays(Duration::from_secs(1), Duration::from_secs(86_400));
        let bound = usize::try_from(MAX_CANDIDATE_DELAYS).unwrap();
        assert_eq!(delays.len(), bound + 1);
        assert_eq!(delays.first(), Some(&Duration::ZERO));
        assert_eq!(delays.last(), Some(&Duration::from_secs(86_400)));
    }

    #[test]
    fn a_step_under_one_second_counts_as_one_second() {
        let delays = candidate_delays(Duration::ZERO, Duration::from_secs(3));
        assert_eq!(delays.len(), 4);
    }

    #[test]
    fn an_empty_input_gives_an_empty_plan() {
        let result = plan(&[], hour_window(5));
        assert_eq!(result.moves, []);
        assert_eq!(result.peak_before(), 0);
        assert_eq!(result.peak_after(), 0);
    }
}
