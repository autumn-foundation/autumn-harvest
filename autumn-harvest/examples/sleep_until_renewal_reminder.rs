#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Absolute-deadline durable waits via `ctx.sleep_until` — issue #749.
//!
//! ## The problem
//!
//! A workflow often needs to wait until an *absolute instant* — "24h before
//! this subscription expires", "at midnight UTC", "until this coupon lapses" —
//! not for a *relative* number of seconds. Today authors hand-roll it:
//!
//! ```rust,ignore
//! // Foot-guns: reaching for Utc::now() breaks replay (HVG001); a deadline
//! // already in the past underflows `as u64` into a multi-year sleep.
//! let secs = (deadline - chrono::Utc::now()).num_seconds() as u64;
//! ctx.timer("wake", secs).await?;
//! ```
//!
//! ## The solution: `ctx.sleep_until(id, deadline)`
//!
//! One call. The wall clock is captured once via the deterministic
//! [`WorkflowContext::system_now`] (frozen into history as a
//! `SideEffectRecorded` event — an existing variant, no new event, no
//! migration), so every replay recomputes the identical remaining duration and
//! resolves at the same instant on every worker. A past deadline clamps to zero
//! and fires on the next poll; a sub-second remainder rounds up so the wait
//! never resolves early.
//!
//! ```rust,ignore
//! use autumn_harvest::prelude::*;
//!
//! #[workflow]
//! async fn renewal_reminder(ctx: &WorkflowContext, sub: Subscription) -> Result<(), String> {
//!     let remind_at = /* absolute expiry */ - chrono::Duration::hours(24);
//!     ctx.sleep_until("remind", remind_at).await?;   // durable — survives worker restart
//!     // ... send the reminder ...
//!     Ok(())
//! }
//! ```
//!
//! Run the embedded tests with:
//! ```bash
//! cargo test -p autumn-harvest --no-default-features --features testing \
//!     --example sleep_until_renewal_reminder
//! ```

use autumn_harvest::prelude::*;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    /// Absolute expiry instant, epoch-millis.
    pub expires_at_millis: i64,
}

/// Waits until 24h before an absolute expiry instant, then "sends" a reminder.
#[workflow]
async fn renewal_reminder(ctx: &WorkflowContext, sub: Subscription) -> Result<String, String> {
    let expiry = DateTime::<Utc>::from_timestamp_millis(sub.expires_at_millis)
        .ok_or_else(|| "invalid expiry".to_string())?;
    let remind_at = expiry - Duration::hours(24);

    // Durable absolute-deadline wait — replay-safe, survives worker restart.
    ctx.sleep_until("renewal-reminder", remind_at)
        .await
        .map_err(|e| e.to_string())?;

    Ok(format!("reminder sent for expiry {}", expiry.to_rfc3339()))
}

fn main() {
    let _wfs = workflows![renewal_reminder];
    println!("sleep_until_renewal_reminder example compiled successfully");
    println!();
    println!("ctx.sleep_until(id, deadline) — absolute-deadline durable wait:");
    println!("  - wall clock captured once via ctx.system_now() (replay-safe)");
    println!("  - past deadline → fires next poll; sub-second remainder rounds up");
    println!("  - lowers onto existing TimerStarted/TimerFired — no new event, no migration");
}

// Gated on the `testing` feature as well as `test`: the example must keep
// building under `--no-default-features`, while `autumn_harvest::testing` only
// exists for external consumers when the `testing` feature is enabled.
#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;
    use autumn_harvest::event::WorkflowEvent;
    use autumn_harvest::testing::WorkflowTestEnv;
    use serde_json::json;

    fn expiry_30_days_out() -> i64 {
        (Utc::now() + Duration::days(30)).timestamp_millis()
    }

    #[tokio::test]
    async fn reminder_waits_until_the_absolute_deadline() {
        let sub = Subscription {
            expires_at_millis: expiry_30_days_out(),
        };
        let outcome = WorkflowTestEnv::new()
            .run(renewal_reminder_info().handler, json!(sub))
            .await;
        assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);

        // The durable timer fired without any real sleeping; its recorded
        // duration is ~29 days (30d expiry minus the 24h lead time).
        let timer_secs = outcome
            .events()
            .iter()
            .find_map(|e| match e {
                WorkflowEvent::TimerStarted { duration_secs, .. } => Some(*duration_secs),
                _ => None,
            })
            .expect("a durable timer must be recorded");
        let twenty_nine_days = 29_u64 * 24 * 3600;
        // Allow a little slack for the wall-clock delta between test setup and
        // the internal system_now() capture.
        assert!(
            (twenty_nine_days - 60..=twenty_nine_days + 60).contains(&timer_secs),
            "expected ~29 days of wait, got {timer_secs}s"
        );
    }

    #[tokio::test]
    async fn reminder_with_a_past_expiry_fires_immediately() {
        // Expiry already in the past → the 24h-before deadline is also past →
        // zero-duration timer, no real sleep, run completes.
        let sub = Subscription {
            expires_at_millis: (Utc::now() - Duration::days(365)).timestamp_millis(),
        };
        let outcome = WorkflowTestEnv::new()
            .run(renewal_reminder_info().handler, json!(sub))
            .await;
        assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);

        let timer_secs = outcome
            .events()
            .iter()
            .find_map(|e| match e {
                WorkflowEvent::TimerStarted { duration_secs, .. } => Some(*duration_secs),
                _ => None,
            })
            .expect("a durable timer must be recorded even for a past deadline");
        assert_eq!(timer_secs, 0, "a past deadline must fire immediately");
    }
}
