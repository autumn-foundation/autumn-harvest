//! Timezone-aware cron schedule example (issue #245).
//!
//! Demonstrates registering a `Schedule::CronInTimezone` so that a "9am Pacific
//! weekdays" job fires at the correct local time year-round without any manual
//! DST adjustments.
//!
//! ## DST guarantee
//!
//! When America/Los_Angeles transitions between PST (UTC-8) and PDT (UTC-7),
//! the scheduler automatically evaluates the cron expression in the declared
//! timezone:
//! - Spring-forward: the non-existent 02:00-03:00 window is skipped; the next
//!   firing is on the following day.
//! - Fall-back: the repeated 01:00-02:00 window fires exactly once (on the
//!   first occurrence, PDT); the second occurrence of that hour is not fired.
//!
//! ## Usage
//!
//! ```rust
//! use std::time::Duration;
//! use autumn_harvest::policy::{Schedule, WorkflowSchedule};
//!
//! // Fire every weekday at 9:00 AM Pacific time.
//! // The tz field accepts any IANA timezone name.
//! let schedule = Schedule::CronInTimezone {
//!     expr: "0 9 * * 1-5".into(),
//!     tz: "America/Los_Angeles".into(),
//! };
//!
//! let workflow_schedule = WorkflowSchedule::new("daily_finance_report", schedule);
//! ```
//!
//! Compare to the UTC-only variant, which drifts by 1 hour twice a year:
//!
//! ```rust
//! use autumn_harvest::policy::Schedule;
//!
//! // Fires at 09:00 UTC year-round — that's 01:00 or 02:00 Pacific depending on DST.
//! let drifting = Schedule::Cron("0 9 * * 1-5".into());
//!
//! // Fires at 09:00 Pacific year-round — correct regardless of DST.
//! let stable = Schedule::CronInTimezone {
//!     expr: "0 9 * * 1-5".into(),
//!     tz: "America/Los_Angeles".into(),
//! };
//! ```
//!
//! ## Validation
//!
//! Unknown timezone names are rejected at builder time, not at first tick:
//!
//! ```rust
//! use autumn_harvest::{
//!     builder::{HarvestBuilder, HarvestBuilderError},
//!     policy::{Schedule, WorkflowSchedule},
//! };
//!
//! // This workflow info would normally come from `#[workflow]` + `workflows![]`.
//! // Abbreviated here for illustration.
//! //
//! // let result = HarvestBuilder::new()
//! //     .workflows(workflows![my_workflow])
//! //     .workflow_schedule(WorkflowSchedule::new(
//! //         "my_workflow",
//! //         Schedule::CronInTimezone {
//! //             expr: "0 9 * * *".into(),
//! //             tz: "Not/ATimezone".into(),   // ← rejected here
//! //         },
//! //     ))
//! //     .try_build();
//! //
//! // assert!(matches!(
//! //     result.unwrap_err(),
//! //     HarvestBuilderError::UnknownTimezone { ref name } if name == "Not/ATimezone"
//! // ));
//! ```

fn main() {
    use autumn_harvest::policy::{Schedule, WorkflowSchedule};

    let schedule = Schedule::CronInTimezone {
        expr: "0 9 * * 1-5".into(),
        tz: "America/Los_Angeles".into(),
    };
    println!("Schedule: {schedule:?}");
    println!("Timezone: {}", schedule.timezone_str());

    let _ws = WorkflowSchedule::new("daily_finance_report", schedule);
    println!("WorkflowSchedule built successfully.");
    println!(
        "This schedule fires at 9:00 AM Pacific weekdays, year-round, \
         regardless of DST transitions."
    );
}
