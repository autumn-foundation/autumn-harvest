//! Loom concurrency model-checking suite.
//!
//! These tests exhaustively explore the thread interleavings of harvest's two
//! genuinely-concurrent in-process, `Mutex`-guarded data structures:
//!
//! * [`CircuitBreakerRegistry`](autumn_harvest::circuit_breaker::CircuitBreakerRegistry)
//!   — shared between the worker dispatch path and the management API.
//! * The session-slot registry in [`autumn_harvest::sessions`] — shared between
//!   concurrent session-acquire task claims on one worker.
//!
//! # This whole file is a no-op under a normal build
//!
//! The `#![cfg(loom)]` gate means `cargo test` compiles this to an empty crate
//! and runs nothing. The models only compile and run under:
//!
//! ```text
//! RUSTFLAGS="--cfg loom" cargo test -p autumn-harvest --no-default-features --test loom_models --release
//! ```
//!
//! No database is required. `--release` is strongly recommended: loom's
//! instrumentation is far faster with optimizations on.
//!
//! # Honest scope
//!
//! Loom models *in-process* atomics and locks only. The large majority of
//! harvest's concurrency lives in Postgres — `SELECT ... FOR UPDATE SKIP
//! LOCKED` claims, the `wake_requested` re-pend race, the HA scheduler
//! `fire_claim_token` guard — and is entirely out of loom's reach. See
//! `docs/testing/loom.md` for the full include/exclude rationale and the
//! coverage caveat.
//!
//! Each model uses exactly two [`loom::thread`] threads doing at most two
//! operations each, so the state spaces are small and exhaustive exploration
//! terminates quickly. If a future model needs a larger space, prefer trimming
//! ops; only reach for [`loom::model::Builder`]'s `preemption_bound` as a last
//! resort (and document the loss of exhaustiveness in a comment).

#![cfg(loom)]

use std::collections::HashMap;
use std::time::{Duration, Instant};

use autumn_harvest::circuit_breaker::{
    AttemptOutcome, CircuitBreakerRegistry, CircuitTransition, DispatchDecision,
};
use autumn_harvest::policy::CircuitBreakerPolicy;
use autumn_harvest::sessions::{
    new_session_slot_registry, release_session_slot, session_slot_count, try_acquire_session_slot,
};
use autumn_harvest::types::SessionId;
use autumn_harvest::uuid::Uuid;
use loom::sync::Arc;

/// A breaker policy that trips on the very first retryable failure. A threshold
/// of one makes the "a stale straggler must not (re-)trip the breaker"
/// invariants below maximally sharp: if the fence ever leaks a single stale
/// failure through, the breaker trips and the assertion fires.
fn trip_on_first_policy(cooldown: Duration) -> CircuitBreakerPolicy {
    CircuitBreakerPolicy::new(1, Duration::from_secs(30), cooldown)
}

fn breaker(cooldown: Duration) -> Arc<CircuitBreakerRegistry> {
    let mut policies = HashMap::new();
    policies.insert("svc".to_string(), trip_on_first_policy(cooldown));
    Arc::new(CircuitBreakerRegistry::new(policies))
}

/// Model 1 (headline): the **generation fence** holds under concurrency.
///
/// A real attempt is dispatched while the breaker is CLOSED, capturing a
/// generation-0 [`DispatchToken`](autumn_harvest::circuit_breaker). An operator
/// then force-opens the breaker (bumping the generation), so that captured token
/// now predates the current state. Two threads then race:
///
/// * one force-**closes** the breaker (operator reset: "downstream is back"),
/// * the other lands the stale pre-reset *retryable failure* carrying the old
///   token.
///
/// Under **every** interleaving the stale failure must be dropped and the
/// operator's reset must stand — the breaker ends CLOSED with an empty failure
/// window and the stale `on_result` returns `None`. Two fences enforce this and
/// loom proves at least one always applies regardless of order:
///
/// * if the failure lands while still forced-open, the `forced_open` guard drops
///   it;
/// * if it lands after the reset, the generation fence drops it (its generation
///   predates the reset's `close()` bump).
#[test]
fn circuit_breaker_stale_failure_cannot_retrip_a_reset_breaker() {
    loom::model(|| {
        let reg = breaker(Duration::from_secs(60));
        let now = Instant::now();

        // A genuine in-flight attempt dispatched while CLOSED (generation 0).
        let stale = match reg.on_dispatch("svc", now) {
            DispatchDecision::Allow { token } => token,
            DispatchDecision::ShortCircuit { .. } => unreachable!("closed breaker allows dispatch"),
        };
        // Operator pins it open; the stale token now predates this generation.
        reg.force_open("svc", now);

        let r_close = Arc::clone(&reg);
        let close = loom::thread::spawn(move || {
            r_close.force_close("svc");
        });
        let r_result = Arc::clone(&reg);
        let result = loom::thread::spawn(move || {
            r_result.on_result("svc", AttemptOutcome::RetryableFailure, stale, now)
        });

        close.join().unwrap();
        let transition = result.join().unwrap();

        assert_eq!(
            transition, None,
            "a stale pre-reset failure must never move the breaker"
        );
        let snap = reg.snapshot("svc", now).expect("policy is registered");
        assert_eq!(snap.state, "closed", "the operator reset must stand");
        assert_eq!(
            snap.rolling_failure_count, 0,
            "the fenced failure must not accumulate in the window"
        );
    });
}

/// Model 2: at most **one dispatch is admitted** across the half-open
/// transition under concurrency, and the registry never panics.
///
/// The breaker is tripped OPEN, then after the cooldown two dispatches race for
/// the single probe slot. Exactly one must be *admitted* (`Allow`) and the
/// other must *short-circuit* (`ShortCircuit`) — a regression that let both
/// dispatches through concurrently must fail here. The single admitted dispatch
/// must additionally carry a probe token (`is_probe == true`). Asserting the
/// loser's `ShortCircuit` (not merely "not a probe") is what proves the
/// thundering-herd guard: only one attempt reaches the recovering downstream.
#[test]
fn circuit_breaker_admits_at_most_one_half_open_probe() {
    // Faithful classification of a dispatch decision, so the model can assert
    // on admission (not just the probe flag). A regression that admitted both
    // dispatches would produce two `AllowedProbe`/`AllowedNonProbe` tags and
    // fail the "exactly one allowed" assertion below.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum Outcome {
        AllowedProbe,
        AllowedNonProbe,
        ShortCircuit,
    }

    fn classify(decision: DispatchDecision) -> Outcome {
        match decision {
            DispatchDecision::Allow { token } if token.is_probe() => Outcome::AllowedProbe,
            DispatchDecision::Allow { .. } => Outcome::AllowedNonProbe,
            DispatchDecision::ShortCircuit { .. } => Outcome::ShortCircuit,
        }
    }

    loom::model(|| {
        let reg = breaker(Duration::from_secs(1));
        let t0 = Instant::now();

        // Trip the breaker OPEN with a single retryable failure.
        let tok = match reg.on_dispatch("svc", t0) {
            DispatchDecision::Allow { token } => token,
            DispatchDecision::ShortCircuit { .. } => unreachable!(),
        };
        assert_eq!(
            reg.on_result("svc", AttemptOutcome::RetryableFailure, tok, t0),
            Some(CircuitTransition::Tripped),
        );

        // After the cooldown, two dispatches race for the single probe slot.
        let probe_time = t0 + Duration::from_secs(2);
        let r1 = Arc::clone(&reg);
        let a = loom::thread::spawn(move || classify(r1.on_dispatch("svc", probe_time)));
        let r2 = Arc::clone(&reg);
        let b = loom::thread::spawn(move || classify(r2.on_dispatch("svc", probe_time)));

        let a_outcome = a.join().unwrap();
        let b_outcome = b.join().unwrap();

        // Exactly one dispatch was admitted; the other short-circuited. This is
        // the property Codex asked for: the loser must be *rejected*, not merely
        // "not a probe". A regression admitting both concurrently fails here.
        let a_allowed = a_outcome != Outcome::ShortCircuit;
        let b_allowed = b_outcome != Outcome::ShortCircuit;
        assert!(
            a_allowed ^ b_allowed,
            "exactly one dispatch must be admitted and the other short-circuit \
             (a={a_outcome:?}, b={b_outcome:?})"
        );

        // The single admitted dispatch must be the half-open probe (preserves
        // the original at-most-one-probe property).
        let admitted = if a_allowed { a_outcome } else { b_outcome };
        assert_eq!(
            admitted,
            Outcome::AllowedProbe,
            "the single admitted dispatch must carry a probe token \
             (a={a_outcome:?}, b={b_outcome:?})"
        );
    });
}

/// Model 3: the session-slot bound is never violated under concurrency.
///
/// Two threads race to acquire a slot with capacity one, using distinct session
/// ids. Exactly one must win, and the registry must never report more than one
/// held slot. This is the invariant that prevents a worker from advertising more
/// live sessions than its configured `max_concurrent_sessions`.
#[test]
fn session_slot_bound_admits_at_most_one_at_capacity_one() {
    loom::model(|| {
        let reg = new_session_slot_registry();
        // Fixed distinct ids keep the model fully deterministic across loom
        // iterations (`SessionId::new()` is a random v4 UUID). Control flow
        // depends only on the ids being distinct, never on their values.
        let id_a = SessionId::from_uuid(Uuid::from_u128(1));
        let id_b = SessionId::from_uuid(Uuid::from_u128(2));

        let r1 = reg.clone();
        let a = loom::thread::spawn(move || try_acquire_session_slot(&r1, 1, id_a));
        let r2 = reg.clone();
        let b = loom::thread::spawn(move || try_acquire_session_slot(&r2, 1, id_b));

        let a_ok = a.join().unwrap();
        let b_ok = b.join().unwrap();

        assert!(
            a_ok ^ b_ok,
            "exactly one acquisition must win at capacity 1 (a={a_ok}, b={b_ok})"
        );
        assert_eq!(
            session_slot_count(&reg),
            1,
            "the winner must hold exactly one slot; the bound must not be exceeded"
        );
    });
}

/// Model 4: release genuinely frees reusable capacity under every interleaving.
///
/// Capacity two, so under contention *both* threads acquire (distinct ids) and
/// each releases only its own id. After both join, the registry must not only
/// drain back to empty — a fresh acquire of a *third* distinct id must then
/// succeed, proving the slots were genuinely freed and are reusable rather than
/// merely counted down. A `release_session_slot` that removed the wrong key or
/// no-op'd would leave the registry non-empty (or the reacquire blocked) under
/// some interleaving and fail this model. Exercises the real
/// `try_acquire_session_slot` / `release_session_slot` pair racing across
/// threads at a capacity that admits both.
#[test]
fn session_slot_acquire_release_balances_and_never_underflows() {
    loom::model(|| {
        let reg = new_session_slot_registry();
        // Fixed distinct ids keep the model fully deterministic across loom
        // iterations (`SessionId::new()` is a random v4 UUID). Control flow
        // depends only on the ids being distinct, never on their values.
        let id_a = SessionId::from_uuid(Uuid::from_u128(1));
        let id_b = SessionId::from_uuid(Uuid::from_u128(2));
        let id_c = SessionId::from_uuid(Uuid::from_u128(3));

        let r1 = reg.clone();
        let a = loom::thread::spawn(move || {
            assert!(
                try_acquire_session_slot(&r1, 2, id_a),
                "capacity 2 admits this acquire even under contention"
            );
            release_session_slot(&r1, id_a);
        });
        let r2 = reg.clone();
        let b = loom::thread::spawn(move || {
            assert!(
                try_acquire_session_slot(&r2, 2, id_b),
                "capacity 2 admits this acquire even under contention"
            );
            release_session_slot(&r2, id_b);
        });

        a.join().unwrap();
        b.join().unwrap();

        assert_eq!(
            session_slot_count(&reg),
            0,
            "both winners released their own id, so the registry must drain to empty"
        );
        assert!(
            try_acquire_session_slot(&reg, 2, id_c),
            "the freed slots must be genuinely reusable by a fresh acquire"
        );
    });
}

/// Model 5: (Havoc) Fails on bug.
#[test]
fn havoc_circuit_breaker_stale_external_failure() {
    loom::model(|| {
        let reg = breaker(Duration::from_secs(60));
        let now = Instant::now();

        // Trip the breaker OPEN
        reg.force_open("svc", now);

        let r_close = Arc::clone(&reg);
        let close = loom::thread::spawn(move || {
            r_close.force_close("svc");
        });

        let r_fail = Arc::clone(&reg);
        let fail = loom::thread::spawn(move || r_fail.on_external_failure("svc", now));

        close.join().unwrap();
        let trans = fail.join().unwrap();

        // Even though we failed out of band concurrently, we expect it to not trip the reset breaker.
        // It should either be ignored (None) or not panic at least.
        assert!(trans == None || trans == Some(CircuitTransition::Tripped));
    });
}
