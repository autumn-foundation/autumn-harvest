#![allow(clippy::unused_async)]

use autumn_harvest::prelude::*;
use std::time::Duration;

#[activity]
async fn simple_activity(_ctx: &ActivityContext, name: String) -> Result<String, String> {
    Ok(format!("hello {name}"))
}

#[activity(
    retry = RetryPolicy::fixed(3, Duration::from_secs(1)),
    start_to_close = "30s",
    queue = "email-workers"
)]
async fn configured_activity(_ctx: &ActivityContext, input: String) -> Result<String, String> {
    Ok(input)
}

// Local activity — no queue, no heartbeat, no schedule_to_start
#[activity(local = true)]
async fn pure_local(_ctx: &ActivityContext, x: i64) -> Result<i64, String> {
    Ok(x * 2)
}

// Local activity with start_to_close (the only timeout local activities support)
#[activity(local = true, start_to_close = "10s")]
async fn timed_local(_ctx: &ActivityContext, val: String) -> Result<String, String> {
    Ok(val.to_uppercase())
}

// Local activity with a retry policy
#[activity(local = true, retry = RetryPolicy::fixed(3, Duration::from_secs(1)))]
async fn retryable_local(_ctx: &ActivityContext) -> Result<(), String> {
    Ok(())
}

#[test]
fn activity_companion_returns_name() {
    let info = __autumn_activity_info_simple_activity();
    assert_eq!(info.name, "simple_activity");
    assert!(info.default_retry_policy.is_none());
    assert_eq!(info.default_queue, None);
    assert!(!info.is_local);
}

#[test]
fn configured_activity_companion_has_policy() {
    let info = __autumn_activity_info_configured_activity();
    assert_eq!(info.name, "configured_activity");
    assert!(info.default_retry_policy.is_some());
    assert_eq!(info.default_queue, Some("email-workers"));
    assert_eq!(info.default_start_to_close, Some(Duration::from_secs(30)));
    assert!(!info.is_local);
}

#[test]
fn local_activity_companion_has_is_local_true() {
    let info = __autumn_activity_info_pure_local();
    assert_eq!(info.name, "pure_local");
    assert!(info.is_local);
    assert!(info.default_heartbeat_timeout.is_none());
    assert!(info.default_schedule_to_start.is_none());
    assert!(info.default_queue.is_none());
}

#[test]
fn local_activity_with_start_to_close_is_local() {
    let info = __autumn_activity_info_timed_local();
    assert!(info.is_local);
    assert_eq!(info.default_start_to_close, Some(Duration::from_secs(10)));
}

#[test]
fn local_activity_with_retry_policy_is_local() {
    let info = __autumn_activity_info_retryable_local();
    assert!(info.is_local);
    assert!(info.default_retry_policy.is_some());
}

#[test]
fn regular_activity_is_not_local() {
    let simple = __autumn_activity_info_simple_activity();
    assert!(!simple.is_local);
    let configured = __autumn_activity_info_configured_activity();
    assert!(!configured.is_local);
}
