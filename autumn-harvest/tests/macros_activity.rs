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

#[test]
fn activity_companion_returns_name() {
    let info = __autumn_activity_info_simple_activity();
    assert_eq!(info.name, "simple_activity");
    assert!(info.default_retry_policy.is_none());
    assert_eq!(info.default_queue, None);
}

#[test]
fn configured_activity_companion_has_policy() {
    let info = __autumn_activity_info_configured_activity();
    assert_eq!(info.name, "configured_activity");
    assert!(info.default_retry_policy.is_some());
    assert_eq!(info.default_queue, Some("email-workers"));
    assert_eq!(info.default_start_to_close, Some(Duration::from_secs(30)));
}
