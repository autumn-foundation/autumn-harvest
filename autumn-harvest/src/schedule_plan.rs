//! Backend-neutral slot arithmetic for cron and interval schedules (issue
//! #2031).
//!
//! The Postgres scheduler and the `SQLite` backend both call these
//! functions, so both compute the same slots. Nothing here needs a database.

use chrono::{DateTime, Utc};
use croner::Cron;

use crate::policy::Schedule;

/// The first slot of `schedule` strictly after `reference`.
///
/// Returns `None` for [`Schedule::Manual`] and for a spec that does not
/// parse. Also returns `None` for an interval that does not advance, or that
/// passes the last date that `chrono` holds.
#[must_use]
pub fn next_fire_after(schedule: &Schedule, reference: DateTime<Utc>) -> Option<DateTime<Utc>> {
    match schedule {
        Schedule::Cron(expr) => Cron::new(expr)
            .with_seconds_optional()
            .parse()
            .ok()
            .and_then(|cron| cron.find_next_occurrence(&reference, false).ok()),
        Schedule::CronInTimezone { expr, tz } => {
            // An unknown zone never fires. Validation rejects it earlier.
            let tz: chrono_tz::Tz = tz.parse().ok()?;
            let cron = Cron::new(expr).with_seconds_optional().parse().ok()?;
            let local_ref = reference.with_timezone(&tz);
            let candidate = cron.find_next_occurrence(&local_ref, false).ok()?;
            // Spring forward: a match in the skipped hour, such as 02:30,
            // resolves to the first valid second after the gap. That instant
            // does not match the pattern. A second search that includes the
            // candidate moves to the real next match. On a normal match it
            // returns the same instant.
            //
            // Fall back: the strict first search already moves past the first
            // occurrence of the repeated hour, so the hour fires once.
            let local_next = cron.find_next_occurrence(&candidate, true).ok()?;
            Some(local_next.with_timezone(&Utc))
        }
        Schedule::Interval(interval) => chrono::Duration::from_std(*interval)
            .ok()
            // A zero period returns `reference` again. A catch-up walk then
            // loops forever (issue #484). So a period that does not advance
            // has no next slot. `validate_schedule` also rejects it.
            .filter(|duration| *duration > chrono::Duration::zero())
            // A sum past the last date that chrono holds also means no next
            // run (issue #2048).
            .and_then(|duration| reference.checked_add_signed(duration)),
        Schedule::Manual => None,
    }
}

/// The slot after a fire of `fired`, with catch-up off.
///
/// The next slot counts from `fired`, so a late fire does not drift the
/// cadence. When that slot is not after `now`, the next slot counts from
/// `now` instead. So a long gap fires once and then realigns.
#[must_use]
pub fn next_slot_after_fire(
    schedule: &Schedule,
    fired: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    next_fire_after(schedule, fired)
        .filter(|&next| next > now)
        .or_else(|| next_fire_after(schedule, now))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::{DateTime, Utc};

    use super::{next_fire_after, next_slot_after_fire};
    use crate::policy::Schedule;

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).unwrap()
    }

    #[test]
    fn an_interval_adds_its_period() {
        let every = Schedule::Interval(Duration::from_secs(60));
        assert_eq!(next_fire_after(&every, at(1_000)), Some(at(1_060)));
        let never = Schedule::Interval(Duration::ZERO);
        assert_eq!(next_fire_after(&never, at(1_000)), None);
    }

    #[test]
    fn a_cron_finds_its_next_match() {
        let five = Schedule::Cron("*/5 * * * *".to_string());
        assert_eq!(next_fire_after(&five, at(1)), Some(at(300)));
        assert_eq!(next_fire_after(&five, at(300)), Some(at(600)), "strict");
        let bad = Schedule::Cron("nope".to_string());
        assert_eq!(next_fire_after(&bad, at(1)), None);
    }

    #[test]
    fn manual_never_fires() {
        assert_eq!(next_fire_after(&Schedule::Manual, at(1)), None);
    }

    #[test]
    fn a_late_fire_keeps_the_cadence_and_a_gap_realigns() {
        let every = Schedule::Interval(Duration::from_secs(60));
        assert_eq!(
            next_slot_after_fire(&every, at(60), at(70)),
            Some(at(120)),
            "a late fire keeps the grid"
        );
        assert_eq!(
            next_slot_after_fire(&every, at(60), at(630)),
            Some(at(690)),
            "a long gap counts from now"
        );
    }
}
