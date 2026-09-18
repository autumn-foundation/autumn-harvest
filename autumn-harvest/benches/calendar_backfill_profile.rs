//! Deterministic (non-criterion) instruction/allocation-count profiling
//! harness for `autumn_harvest::calendar::plan_backfill_with_calendar` — the
//! calendar-aware backfill planner behind
//! `POST /admin/schedules/{id}/backfill` (`autumn-harvest-plugin/src/api.rs`,
//! `schedule_backfill` -> `plan_backfill_with_calendar`). Wall-clock timing is
//! not admissible evidence on this (shared-vCPU) machine — every number this
//! harness produces evidence for is a deterministic instruction count
//! (`valgrind --tool=callgrind`) or allocation count/bytes
//! (`valgrind --tool=dhat`), both reproducible bit-for-bit on any machine.
//!
//! # Workload
//!
//! An hourly `Schedule::Interval` backfilled across `SLOTS` raw timestamps
//! (the `max_count` the admin handler passes through from the request body,
//! `DEFAULT_BACKFILL_MAX_COUNT` = 1,000 by default and **not hard-capped** by
//! the handler — an operator can request more) against an `EXCLUSIONS`-entry
//! calendar exclusion list, exactly as `load_exclusions_for_calendar` would
//! return it: every exclusion row ever inserted for the named calendar, with
//! no date-range bound (see that function's doc comment). A calendar in
//! continuous use only grows this list; there is no retention or archival
//! path for it. The exclusion dates are all drawn from the two years
//! *before* `from`, so none of them land inside the backfill window itself —
//! every one of the `SLOTS` calendar checks this workload performs is a
//! miss, which is realistic (a backfill request commonly targets a recent
//! window while the calendar's exclusion history spans years) and is also
//! `Vec::contains`'s worst case: a linear scan cannot short-circuit on an
//! absent element, so every check pays the full `EXCLUSIONS`-length scan.
//! The exclusion dates are inserted in a deterministic non-sorted order (a
//! fixed-stride permutation, not chronological) since nothing in the public
//! contract (`is_excluded_date`'s doc comment) promises callers hand in a
//! sorted slice.
//!
//! `CALENDAR_PROFILE_SLOTS` (default `2000`) sets the backfill slot count
//! (`max_count`, and the exact number of raw timestamps the interval
//! schedule produces between `from` and `to`). `CALENDAR_PROFILE_EXCLUSIONS`
//! (default `3000`) sets the exclusion-list length. `CALENDAR_PROFILE_REPS`
//! (default `3`) repeats the whole `plan_backfill_with_calendar` call against
//! the same fixed schedule and exclusion list.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest --bench calendar_backfill_profile \
//!   --no-run --message-format=json 2>/dev/null \
//!   | jq -r 'select(.reason=="compiler-artifact" and .target.name=="calendar_backfill_profile") | .executable')
//! valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! callgrind_annotate --threshold=98 cg.out
//! valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
//! ```

use std::time::Duration as StdDuration;

use autumn_harvest::calendar::plan_backfill_with_calendar;
use autumn_harvest::policy::{Schedule, SkipPolicy};
use chrono::{NaiveDate, TimeZone, Utc};

/// Reads `key` as a `usize`, using `default` only when the variable is
/// genuinely *absent*. A *present but malformed* value is a configuration
/// error, not silently substituted for the default.
fn env_usize(key: &str, default: usize) -> usize {
    match std::env::var(key) {
        Ok(raw) => raw
            .parse()
            .unwrap_or_else(|e| panic!("{key}={raw:?} is not a valid usize: {e}")),
        Err(std::env::VarError::NotPresent) => default,
        Err(std::env::VarError::NotUnicode(raw)) => {
            panic!("{key}={} is not valid Unicode", raw.to_string_lossy())
        }
    }
}

/// Builds `count` exclusion dates spanning the two years before `before`, in
/// a fixed-stride (non-chronological) order — a long-lived calendar's rows
/// come back from `SELECT ... WHERE calendar_name = $1` with no `ORDER BY`,
/// so insertion order (not date order) is what a real caller gets.
fn build_exclusions(before: NaiveDate, count: usize) -> Vec<NaiveDate> {
    let window_days: i64 = 730;
    // A stride coprime with `window_days` (730 = 2 * 5 * 73) visits every
    // day in the window exactly once before repeating, for any `count` up to
    // `window_days`.
    let stride: i64 = 317;
    let mut dates = Vec::with_capacity(count);
    for i in 0..count {
        #[allow(clippy::cast_possible_wrap)]
        let offset = ((i as i64) * stride).rem_euclid(window_days) + 1;
        dates.push(before - chrono::Duration::days(offset));
    }
    dates
}

fn main() {
    let slots = env_usize("CALENDAR_PROFILE_SLOTS", 2000);
    let exclusions = env_usize("CALENDAR_PROFILE_EXCLUSIONS", 3000);
    let reps = env_usize("CALENDAR_PROFILE_REPS", 3);

    assert!(slots > 0, "CALENDAR_PROFILE_SLOTS must be at least 1, got 0");
    assert!(
        exclusions > 0,
        "CALENDAR_PROFILE_EXCLUSIONS must be at least 1, got 0"
    );
    // reps=0 would exit having measured nothing but setup, and could be
    // mistaken for a valid (implausibly fast) measurement.
    assert!(reps > 0, "CALENDAR_PROFILE_REPS must be at least 1, got 0");

    let from = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    let to = from + chrono::Duration::hours(i64::try_from(slots - 1).expect("slots fits i64"));
    let schedule = Schedule::Interval(StdDuration::from_secs(3600));
    let excluded_dates = build_exclusions(from.date_naive(), exclusions);

    let mut total_slots = 0usize;
    for _ in 0..reps {
        let result = plan_backfill_with_calendar(
            Some(&schedule),
            from,
            to,
            slots,
            &excluded_dates,
            SkipPolicy::RunNextBusinessDay,
            false,
        )
        .expect("slots was sized to exactly fill max_count, never exceed it");
        // None of the exclusion dates fall inside [from, to], so every raw
        // slot survives unadjusted -- a change that silently started
        // dropping or rebasing slots (not just moving their cost) would fail
        // this harness rather than produce a quietly-wrong "faster" number.
        assert_eq!(
            result.len(),
            slots,
            "no exclusion date falls inside the backfill window, so all {slots} slots should \
             survive unadjusted, got {}",
            result.len()
        );
        total_slots += result.len();
        std::hint::black_box(&result);
    }

    println!(
        "calendar_backfill_profile: slots={slots} exclusions={exclusions} reps={reps} \
         total_slots={total_slots}"
    );
}
