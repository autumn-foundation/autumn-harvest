//! No-DB wiring tests for the activity circuit breaker (issue #369).
//!
//! Verifies that a [`CircuitBreakerPolicy`] declared on an `ActivityInfo`
//! (as the `#[activity(circuit_breaker = ...)]` macro emits) is picked up by the
//! [`HandlerRegistry`] and exposed through the registry the worker dispatch
//! path and the management API both consult. The breaker state-machine logic
//! itself is unit-tested in `src/circuit_breaker.rs`; the full dispatch +
//! replay-safety path is covered by the testcontainers e2e suite.

// The worker `HandlerRegistry` is gated behind the `db` feature; constructing
// it needs no live database, only the type.
#![cfg(feature = "db")]
#![allow(clippy::unused_async)]

use std::time::{Duration, Instant};

use autumn_harvest::circuit_breaker::AttemptOutcome;
use autumn_harvest::prelude::*;
use autumn_harvest::worker::HandlerRegistry;

#[activity(
    start_to_close = "30s",
    circuit_breaker = CircuitBreakerPolicy::new(10, Duration::from_secs(30), Duration::from_secs(60))
)]
async fn flaky_downstream(_ctx: &ActivityContext, addr: String) -> Result<(), String> {
    let _ = addr;
    Ok(())
}

#[activity(start_to_close = "30s")]
async fn no_breaker(_ctx: &ActivityContext, addr: String) -> Result<(), String> {
    let _ = addr;
    Ok(())
}

fn registry() -> HandlerRegistry {
    HandlerRegistry::new(vec![], activities![flaky_downstream, no_breaker])
}

#[test]
fn registry_builds_breakers_from_declared_policies() {
    let reg = registry();
    let breakers = reg.circuit_breakers();
    assert!(
        breakers.has_policy("flaky_downstream"),
        "activity with a declared policy should be tracked"
    );
    assert!(
        !breakers.has_policy("no_breaker"),
        "activity without a policy retains today's behaviour (no breaker)"
    );
}

#[test]
fn declared_breaker_trips_and_short_circuits() {
    let reg = registry();
    let breakers = reg.circuit_breakers();
    let now = Instant::now();

    // 10 consecutive retryable failures within the window trip the breaker.
    // Model the real flow: dispatch (to get a current-generation token) then
    // report the failure.
    for _ in 0..9 {
        let token = match breakers.on_dispatch("flaky_downstream", now) {
            DispatchDecision::Allow { token } => token,
            DispatchDecision::ShortCircuit { .. } => panic!("expected Allow"),
        };
        assert_eq!(
            breakers.on_result(
                "flaky_downstream",
                AttemptOutcome::RetryableFailure,
                token,
                now
            ),
            None
        );
    }
    let token = match breakers.on_dispatch("flaky_downstream", now) {
        DispatchDecision::Allow { token } => token,
        DispatchDecision::ShortCircuit { .. } => panic!("expected Allow"),
    };
    assert_eq!(
        breakers.on_result(
            "flaky_downstream",
            AttemptOutcome::RetryableFailure,
            token,
            now
        ),
        Some(CircuitTransition::Tripped)
    );

    // Now dispatch short-circuits, and the snapshot reports the open state.
    assert!(matches!(
        breakers.on_dispatch("flaky_downstream", now),
        DispatchDecision::ShortCircuit { .. }
    ));
    let snap = breakers.snapshot("flaky_downstream", now).unwrap();
    assert_eq!(snap.state, "open");
    assert_eq!(snap.failure_threshold, 10);
}

#[test]
fn untracked_activity_never_short_circuits() {
    let reg = registry();
    let breakers = reg.circuit_breakers();
    let now = Instant::now();
    // Even after many failures, an activity without a policy always dispatches.
    for _ in 0..100 {
        let token = match breakers.on_dispatch("no_breaker", now) {
            DispatchDecision::Allow { token } => token,
            DispatchDecision::ShortCircuit { .. } => panic!("expected Allow"),
        };
        breakers.on_result("no_breaker", AttemptOutcome::RetryableFailure, token, now);
    }
    assert!(matches!(
        breakers.on_dispatch("no_breaker", now),
        DispatchDecision::Allow { .. }
    ));
    assert!(breakers.snapshot("no_breaker", now).is_none());
}

#[test]
fn force_open_and_close_through_shared_registry() {
    let reg = registry();
    let breakers = reg.circuit_breakers();
    let now = Instant::now();

    breakers.force_open("flaky_downstream", now);
    assert!(
        breakers
            .snapshot("flaky_downstream", now)
            .unwrap()
            .forced_open
    );
    assert!(matches!(
        breakers.on_dispatch("flaky_downstream", now),
        DispatchDecision::ShortCircuit { .. }
    ));

    breakers.force_close("flaky_downstream");
    let snap = breakers.snapshot("flaky_downstream", now).unwrap();
    assert_eq!(snap.state, "closed");
    assert!(!snap.forced_open);
    assert!(matches!(
        breakers.on_dispatch("flaky_downstream", now),
        DispatchDecision::Allow { .. }
    ));
}

/// Local activities bypass the dispatch path the breaker is enforced on, so a
/// breaker declared on one must not be tracked (it would appear configured in
/// the admin API yet never trip). The `#[activity]` macro rejects this at
/// compile time; the registry filter is the defensive guard for hand-built
/// `ActivityInfo`s, which this test exercises directly.
#[test]
fn local_activity_circuit_breaker_is_not_tracked() {
    let local_with_breaker = ActivityInfo {
        name: "inline_work",
        module: "test",
        default_retry_policy: None,
        default_start_to_close: Some(Duration::from_secs(5)),
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: None,
        max_concurrent: None,
        concurrency_key: None,
        is_local: true,
        max_input_bytes: None,
        max_result_bytes: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        circuit_breaker: Some(CircuitBreakerPolicy::new(
            1,
            Duration::from_secs(30),
            Duration::from_secs(60),
        )),
        handler: |_ctx, input| Box::pin(async move { Ok(input) }),
    };
    let reg = HandlerRegistry::new(vec![], vec![local_with_breaker]);
    let breakers = reg.circuit_breakers();
    assert!(
        !breakers.has_policy("inline_work"),
        "a local activity's circuit breaker must not be registered"
    );
    assert!(breakers.is_empty());
}
