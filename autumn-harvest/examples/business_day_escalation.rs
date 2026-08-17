#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Business-day durable waits via `ctx.timer_business_days` — issue #806.
//!
//! ## The problem
//!
//! Support SLAs, contractual response windows, and settlement deadlines are
//! written in *business days*, not calendar days: "escalate if unacknowledged
//! after 2 business days". `ctx.timer(id, 2 * 86_400)` is wrong the moment the
//! window straddles a weekend or a public holiday, and hand-rolling it means
//! reimplementing weekend/holiday arithmetic in every workflow:
//!
//! ```rust,ignore
//! // Foot-guns: reaching for Utc::now() breaks replay (HVG001); re-reading the
//! // calendar on replay lets an operator's holiday edit silently move an
//! // already-armed deadline.
//! let mut d = chrono::Utc::now();
//! let mut left = 2;
//! while left > 0 { d = d + chrono::Duration::days(1); if is_business(d) { left -= 1; } }
//! ctx.timer("escalate", (d - chrono::Utc::now()).num_seconds() as u64).await?;
//! ```
//!
//! ## The solution: `ctx.timer_business_days(id, n, calendar)`
//!
//! One call. The anchor is captured **once** on first live execution and the
//! resolved deadline is frozen into history as a `SideEffectRecorded` event —
//! an *existing* variant, so **no new `WorkflowEvent`, no migration**. On every
//! replay the frozen deadline is used verbatim and the calendar is **not**
//! re-read, so an operator editing the calendar after the timer is armed can
//! never move its deadline.
//!
//! ```rust,ignore
//! use autumn_harvest::prelude::*;
//!
//! #[workflow]
//! async fn ticket_sla(ctx: &WorkflowContext, ticket: Ticket) -> Result<String, String> {
//!     // "Escalate after 2 business days" — one ctx call.
//!     ctx.timer_business_days("escalate", 2, "us-support").await?;
//!     // ... escalate ...
//!     Ok("escalated".into())
//! }
//! ```
//!
//! ## Registering the calendar
//!
//! The primitive reads a [`BusinessCalendars`] snapshot from shared state, so
//! the holiday set is resolved **on the worker**, not per call:
//!
//! ```rust,ignore
//! let calendars = BusinessCalendars::new()
//!     .with_calendar("us-support", vec![/* holidays, e.g. loaded from
//!                                          harvest_calendar_exclusions */]);
//! HarvestBuilder::new().state(calendars) /* ... */;
//! ```
//!
//! Weekends (Sat/Sun) are **always** non-business days for this primitive; the
//! named calendar contributes the holiday set. Business days are computed on
//! **UTC** dates and the deadline preserves the anchor's UTC time-of-day.
//!
//! Run the embedded tests with:
//! ```bash
//! cargo test -p autumn-harvest --no-default-features --features testing \
//!     --example business_day_escalation
//! ```

use autumn_harvest::calendar::BusinessCalendars;
use autumn_harvest::prelude::*;
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ticket {
    pub id: String,
    /// Business days the customer contract allows before escalation.
    pub sla_business_days: u32,
}

/// The success-metric shape: an "escalate after N business days" SLA expressed
/// in **one** `ctx` call.
#[workflow]
async fn ticket_sla(ctx: &WorkflowContext, ticket: Ticket) -> Result<String, String> {
    let deadline = ctx
        .timer_business_days("escalate", ticket.sla_business_days, "us-support")
        .await
        .map_err(|e| e.to_string())?;

    Ok(format!(
        "ticket {} escalated at {}",
        ticket.id,
        deadline.to_rfc3339()
    ))
}

/// Sequential composition: two business-day waits in one workflow. Each needs
/// its own timer id — the second wait re-anchors on the instant the first one
/// resolved, so the windows chain rather than overlap.
#[workflow]
async fn two_stage_escalation(ctx: &WorkflowContext, ticket: Ticket) -> Result<String, String> {
    ctx.timer_business_days("first-nudge", 1, "us-support")
        .await
        .map_err(|e| e.to_string())?;
    let final_deadline = ctx
        .timer_business_days("escalate", 2, "us-support")
        .await
        .map_err(|e| e.to_string())?;

    Ok(format!(
        "ticket {} escalated to management at {}",
        ticket.id,
        final_deadline.to_rfc3339()
    ))
}

fn holiday(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).expect("valid date")
}

/// The calendar an embedder would register on `HarvestBuilder::state(..)`.
/// In production the holiday set typically comes from
/// `calendar::load_exclusions_for_calendar` so operators can edit it in the DB.
fn support_calendar() -> BusinessCalendars {
    BusinessCalendars::new().with_calendar(
        "us-support",
        vec![
            holiday(2026, 7, 3), // observed Independence Day
            holiday(2026, 9, 7), // Labor Day
        ],
    )
}

fn main() {
    let _wfs = workflows![ticket_sla, two_stage_escalation];
    let cal = support_calendar();
    println!("business_day_escalation example compiled successfully");
    println!();
    println!(
        "registered calendars: {:?}",
        cal.names().collect::<Vec<_>>()
    );
    println!();
    println!("ctx.timer_business_days(id, n, calendar) — business-day durable wait:");
    println!("  - anchor captured ONCE on first live execution");
    println!("  - resolved deadline frozen into an existing SideEffectRecorded event");
    println!("  - replay uses the frozen deadline verbatim; the calendar is NOT re-read");
    println!("  - Sat/Sun always non-business; the calendar contributes holidays");
    println!("  - no new WorkflowEvent variant, no migration");
}

// Gated on the `testing` feature as well as `test`: the example must keep
// building under `--no-default-features`, while `autumn_harvest::testing` only
// exists for external consumers when the `testing` feature is enabled.
#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;
    use autumn_harvest::event::WorkflowEvent;
    use autumn_harvest::testing::WorkflowTestEnv;
    use chrono::{DateTime, Datelike, Duration, TimeZone, Utc, Weekday};
    use serde_json::json;

    fn ticket(n: u32) -> Ticket {
        Ticket {
            id: "TKT-1".to_string(),
            sla_business_days: n,
        }
    }

    /// Anchor on a Thursday so a 2-business-day SLA must cross the weekend.
    fn thursday() -> DateTime<Utc> {
        let anchor = Utc.with_ymd_and_hms(2026, 7, 9, 9, 0, 0).single().unwrap();
        assert_eq!(anchor.weekday(), Weekday::Thu, "fixture must be a Thursday");
        anchor
    }

    /// The headline AC: 2 business days anchored Thursday lands on the
    /// following **Monday**, skipping Sat + Sun — and the run completes with no
    /// real sleeping.
    #[tokio::test]
    async fn two_business_days_from_thursday_skips_the_weekend() {
        let outcome = WorkflowTestEnv::new()
            .with_business_calendars(support_calendar())
            .with_frozen_anchor(thursday())
            .run(ticket_sla_info().handler, json!(ticket(2)))
            .await;
        assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);

        // Thu 2026-07-09 + 2 business days = Mon 2026-07-13.
        let expected = Utc.with_ymd_and_hms(2026, 7, 13, 9, 0, 0).single().unwrap();
        assert_eq!(expected.weekday(), Weekday::Mon);

        let out = outcome.result.as_ref().unwrap().to_string();
        assert!(
            out.contains(&expected.to_rfc3339()),
            "expected the Monday deadline {expected} in {out}"
        );

        // A naive calendar-day wait would have landed on Saturday.
        let naive = thursday() + Duration::days(2);
        assert_eq!(naive.weekday(), Weekday::Sat);
        assert!(
            !out.contains(&naive.to_rfc3339()),
            "must NOT resolve to the naive calendar-day answer {naive}"
        );
    }

    /// Anchoring the Thursday *before* the 2026-07-03 holiday makes the holiday
    /// bite as well as the weekend: Thu 07-02 + 2 business days skips Fri 07-03
    /// (holiday) and Sat/Sun, landing Tue 07-07.
    #[tokio::test]
    async fn holidays_are_skipped_as_well_as_weekends() {
        let anchor = Utc.with_ymd_and_hms(2026, 7, 2, 9, 0, 0).single().unwrap();
        assert_eq!(anchor.weekday(), Weekday::Thu);

        let outcome = WorkflowTestEnv::new()
            .with_business_calendars(support_calendar())
            .with_frozen_anchor(anchor)
            .run(ticket_sla_info().handler, json!(ticket(2)))
            .await;
        assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);

        // Fri 07-03 is a holiday, Sat/Sun are weekends → Mon 07-06 is the 1st
        // business day, Tue 07-07 the 2nd.
        let expected = Utc.with_ymd_and_hms(2026, 7, 7, 9, 0, 0).single().unwrap();
        let out = outcome.result.as_ref().unwrap().to_string();
        assert!(
            out.contains(&expected.to_rfc3339()),
            "expected the post-holiday deadline {expected} in {out}"
        );
    }

    /// Sequential composition genuinely **chains**: the second window anchors
    /// where the first resolved, not back at the original anchor.
    ///
    /// Falsifiable by construction. From Thu 2026-07-09: the first wait (1
    /// business day) lands Fri 07-10; the second (2 business days) then steps
    /// over the weekend to **Tue 07-14**. Had the second wait re-anchored at the
    /// original Thursday it would land Mon 07-13 — a different date — so this
    /// assertion fails if chaining regresses.
    #[tokio::test]
    async fn sequential_business_day_waits_chain() {
        let outcome = WorkflowTestEnv::new()
            .with_business_calendars(support_calendar())
            .with_frozen_anchor(thursday())
            .run(two_stage_escalation_info().handler, json!(ticket(0)))
            .await;
        assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);

        let timers: Vec<_> = outcome
            .events()
            .iter()
            .filter_map(|e| match e {
                WorkflowEvent::TimerStarted { timer_id, .. } => Some(timer_id.to_string()),
                _ => None,
            })
            .collect();
        assert_eq!(
            timers,
            vec!["first-nudge".to_string(), "escalate".to_string()],
            "both business-day waits must arm their own durable timer"
        );

        // The chaining proof: the two frozen anchors differ, the second anchor
        // equals the first deadline, and the final deadline is the chained one.
        let frozen: Vec<serde_json::Value> = outcome
            .events()
            .iter()
            .filter_map(|e| match e {
                WorkflowEvent::SideEffectRecorded { value, .. } => Some(value.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(frozen.len(), 2, "one freeze per business-day wait");
        let a0: DateTime<Utc> = serde_json::from_value(frozen[0]["anchor"].clone()).unwrap();
        let d0: DateTime<Utc> = serde_json::from_value(frozen[0]["deadline"].clone()).unwrap();
        let a1: DateTime<Utc> = serde_json::from_value(frozen[1]["anchor"].clone()).unwrap();
        let d1: DateTime<Utc> = serde_json::from_value(frozen[1]["deadline"].clone()).unwrap();
        assert_eq!(a0, thursday(), "first wait anchors at the frozen start");
        assert_eq!(
            d0.to_rfc3339(),
            "2026-07-10T09:00:00+00:00",
            "1 business day from Thursday is Friday"
        );
        assert_eq!(a1, d0, "the second window anchors where the first resolved");
        assert_eq!(
            d1.to_rfc3339(),
            "2026-07-14T09:00:00+00:00",
            "2 business days from Friday steps over the weekend to Tuesday — \
             re-anchoring at Thursday instead would give Mon 07-13"
        );
        assert!(
            outcome
                .result
                .as_ref()
                .unwrap()
                .to_string()
                .contains("2026-07-14T09:00:00+00:00"),
            "the workflow must return the chained deadline"
        );
    }

    /// The determinism AC, end to end: the same recorded history replays to the
    /// identical fire instant — the resolution is never re-run.
    #[tokio::test]
    async fn business_day_history_replays_deterministically() {
        let outcome = WorkflowTestEnv::new()
            .with_business_calendars(support_calendar())
            .with_frozen_anchor(thursday())
            .run(ticket_sla_info().handler, json!(ticket(2)))
            .await;
        assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);

        // (a) Round-trip: the harness forwards the same snapshot a real worker
        //     holds, so this asserts the history replays cleanly.
        let report = outcome.replay_check(ticket_sla_info().handler).await;
        assert!(
            matches!(
                report.status,
                autumn_harvest::testing::ReplayStatus::ReplaySucceeded
            ),
            "replay regression:\n{report}"
        );

        // (b) The freeze proof: replay under a calendar that now excludes the
        //     frozen deadline's own date. A recompute would move the deadline
        //     and diverge from the recorded `TimerStarted`; the frozen record
        //     must win instead.
        let frozen_deadline: DateTime<Utc> = outcome
            .events()
            .iter()
            .find_map(|e| match e {
                WorkflowEvent::SideEffectRecorded { value, .. } => {
                    serde_json::from_value(value["deadline"].clone()).ok()
                }
                _ => None,
            })
            .expect("a business-day freeze must be recorded");
        let hostile = BusinessCalendars::new()
            .with_calendar("us-support", vec![frozen_deadline.date_naive()]);
        let hostile_report = autumn_harvest::testing::WorkflowReplayer::new()
            .with_state(hostile)
            .register_fn("ticket_sla", ticket_sla_info().handler)
            .replay_from_events(outcome.events().to_vec())
            .await;
        assert!(
            matches!(
                hostile_report.status,
                autumn_harvest::testing::ReplayStatus::ReplaySucceeded
            ),
            "a calendar edit after arming must not move the frozen deadline:\n{hostile_report}"
        );
    }
}
