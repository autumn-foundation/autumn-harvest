//! Backend-neutral slot arithmetic for cron and interval schedules (issue
//! #2031).
//!
//! The Postgres scheduler and the `SQLite` backend both call these
//! functions, so both compute the same slots. Nothing here needs a database.

use chrono::{DateTime, Utc};

use crate::policy::{Schedule, parse_cron};

/// A schedule prepared for repeated slot lookups.
///
/// A catch-up walk asks for many slots of one schedule. Building this value
/// once parses the cron expression and the zone once, not once per slot.
pub struct SlotWalker {
    kind: WalkerKind,
}

enum WalkerKind {
    Cron(croner::Cron),
    CronInTimezone(croner::Cron, chrono_tz::Tz),
    Interval(chrono::Duration),
    Never,
}

impl SlotWalker {
    /// Prepare `schedule`. A spec that does not parse never fires.
    #[must_use]
    pub fn new(schedule: &Schedule) -> Self {
        let kind = match schedule {
            Schedule::Cron(expr) => parse_cron(expr).ok().map(WalkerKind::Cron),
            Schedule::CronInTimezone { expr, tz } => {
                // An unknown zone never fires. Validation rejects it earlier.
                tz.parse::<chrono_tz::Tz>().ok().and_then(|tz| {
                    parse_cron(expr)
                        .ok()
                        .map(|cron| WalkerKind::CronInTimezone(cron, tz))
                })
            }
            Schedule::Interval(interval) => chrono::Duration::from_std(*interval)
                .ok()
                // A zero period returns `reference` again. A catch-up walk then
                // loops forever (issue #484). So a period that does not advance
                // has no next slot. `validate_schedule` also rejects it.
                .filter(|duration| *duration > chrono::Duration::zero())
                .map(WalkerKind::Interval),
            Schedule::Manual => None,
        };
        Self {
            kind: kind.unwrap_or(WalkerKind::Never),
        }
    }

    /// The first slot strictly after `reference`.
    #[must_use]
    pub fn next_after(&self, reference: DateTime<Utc>) -> Option<DateTime<Utc>> {
        match &self.kind {
            WalkerKind::Cron(cron) => cron.find_next_occurrence(&reference, false).ok(),
            WalkerKind::CronInTimezone(cron, tz) => {
                let local_ref = reference.with_timezone(tz);
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
            // A sum past the last date that chrono holds also means no next
            // run (issue #2048).
            WalkerKind::Interval(duration) => reference.checked_add_signed(*duration),
            WalkerKind::Never => None,
        }
    }
}

/// The first slot of `schedule` strictly after `reference`.
///
/// Returns `None` for [`Schedule::Manual`] and for a spec that does not
/// parse. Also returns `None` for an interval that does not advance, or that
/// passes the last date that `chrono` holds.
#[must_use]
pub fn next_fire_after(schedule: &Schedule, reference: DateTime<Utc>) -> Option<DateTime<Utc>> {
    SlotWalker::new(schedule).next_after(reference)
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

    /// Croner 4 rejects these forms by default. A stored schedule can hold one,
    /// and a parse failure here stops that schedule with no error.
    #[test]
    fn a_stored_step_without_a_range_still_fires() {
        let reference: DateTime<Utc> = "2026-03-01T00:01:00Z".parse().expect("timestamp");
        for (expr, expected) in [
            ("0/10 * * * *", "2026-03-01T00:10:00Z"),
            ("5/5 * * * *", "2026-03-01T00:05:00Z"),
            ("0 0/6 * * *", "2026-03-01T06:00:00Z"),
        ] {
            let schedule = Schedule::Cron(expr.to_string());
            assert_eq!(
                next_fire_after(&schedule, reference),
                Some(expected.parse().expect("timestamp")),
                "{expr}",
            );
            assert!(
                crate::policy::validate_schedule(&schedule).is_ok(),
                "{expr}"
            );
        }
    }

    /// In the repeated hour, a daily job does not fire again and an hourly job
    /// does. The next slot is never before the reference.
    #[test]
    fn the_repeated_hour_moves_forward() {
        let in_zone = |expr: &str| Schedule::CronInTimezone {
            expr: expr.to_string(),
            tz: "America/New_York".to_string(),
        };
        let at = |text: &str| -> DateTime<Utc> { text.parse().expect("timestamp") };

        // 01:00:01 EST is in the second pass of the repeated hour.
        let second_pass = at("2026-11-01T06:00:01Z");
        assert_eq!(
            next_fire_after(&in_zone("30 1 * * *"), second_pass),
            Some(at("2026-11-02T06:30:00Z")),
        );
        assert_eq!(
            next_fire_after(&in_zone("*/15 * * * *"), second_pass),
            Some(at("2026-11-01T06:15:00Z")),
        );

        // 01:00:02 EDT is in the first pass. The hourly job fires at 01:00 EST.
        assert_eq!(
            next_fire_after(&in_zone("0 * * * *"), at("2026-11-01T05:00:02Z")),
            Some(at("2026-11-01T06:00:00Z")),
        );
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
