//! Activity execution interceptors (issue #680).
//!
//! An **activity interceptor** wraps *every* activity execution on the worker —
//! both regular (task-queue-dispatched) and local (inline) activities — with a
//! middleware-style continuation chain. Interceptors let an application add
//! cross-cutting behavior (structured logging, custom metrics, header
//! propagation, input/output shaping, fault injection in tests) *once*, at
//! registration time, rather than editing every `#[activity]` function.
//!
//! # Chain ordering
//!
//! Interceptors are registered in order via
//! [`HarvestBuilder::activity_interceptor`](crate::builder::HarvestBuilder::activity_interceptor).
//! The **first-registered interceptor is the OUTERMOST wrapper**; the activity
//! handler is the innermost. Given interceptors `[A, B]` and handler `H`, the
//! execution nests as `A(B(H))`: `A` runs first on the way in and last on the
//! way out.
//!
//! ```text
//! A::intercept  ── next.run ─▶  B::intercept  ── next.run ─▶  handler
//!      ▲                             ▲                            │
//!      └──────────── result ─────────┴──────────── result ───────┘
//! ```
//!
//! An interceptor that returns **without** calling [`ActivityInterceptorNext::run`]
//! short-circuits the chain: neither the remaining interceptors nor the handler
//! run. Its returned value becomes the activity result.
//!
//! # Error, panic, retry, and circuit-breaker transparency
//!
//! An interceptor future resolves to `Result<serde_json::Value, String>` — the
//! *exact* `Output` type of [`ActivityHandlerFn`](crate::info::ActivityHandlerFn).
//! Because the interceptor sits at the same fn-pointer boundary as the handler,
//! its error rides the **same internal String channel** the handler uses, so
//! everything downstream is transparent:
//!
//! - An `Err(String)` returned by an interceptor is treated identically to a
//!   handler error — it flows through the normal retry / circuit-breaker /
//!   dead-letter path with no special-casing. To emit a *typed* failure, use
//!   [`crate::failure::ActivityFailure`]'s `retryable`/`non_retryable`
//!   constructors and `into_error_payload()`.
//! - A **panic** inside an interceptor (during construction or poll) is caught
//!   by the same worker-side `catch_construct` + `catch_unwind` guard that
//!   contains handler panics (issue #782), flattened into a retryable
//!   `HandlerPanic` failure. The worker process survives.
//! - Interceptors run **after** the per-activity circuit-breaker admit gate, so
//!   a circuit-open short-circuit correctly does **not** run interceptors
//!   (there is no execution to wrap).
//!
//! # Replay safety
//!
//! Interceptors run only on the worker dispatch path. The deterministic replay
//! engine never invokes activity handlers — it reads recorded
//! `ActivityCompleted`/`ActivityFailed` events. For a **non-transactional**
//! activity the value an interceptor chain returns *becomes* the recorded
//! activity result, so replay simply reads it back; interceptors are never
//! re-invoked during replay of a completed activity. (The one exception is a
//! **transactional** activity, whose handler seals its own recorded outcome
//! before the chain unwinds — see [Transactional activities](#transactional-activities)
//! below.)
//!
//! Transforming the `input` an interceptor passes to the handler changes only
//! what the handler *computes*; it does **not** change the `ActivityScheduled`
//! event, whose `input` is fixed by workflow code at schedule time and recorded
//! before dispatch ever reaches the interceptor. An input transform therefore
//! cannot diverge replay — the scheduled input on the record is unaffected.
//!
//! # Transactional activities
//!
//! An interceptor **observes but cannot transform** the recorded outcome of an
//! activity that calls [`ActivityContext::run_transactional`](crate::context::ActivityContext::run_transactional).
//! A transactional activity atomically appends its `ActivityCompleted` event and
//! marks its task `COMPLETED` *inside the user's transaction* — before an outer
//! interceptor regains control from [`ActivityInterceptorNext::run`]. The sealed
//! outcome is immutable, so a result transform (`Ok(V)` → `Ok(V′)`) or an error
//! transform (`Ok(V)` → `Err(e)`) applied after `next.run` is **silently
//! discarded from history**: the workflow always observes the committed value
//! `V`. Result/error transforms therefore take effect only on **non-transactional**
//! activities.
//!
//! For a transactional activity the engine keeps its observability consistent
//! with the committed (sealed) outcome: metrics record the committed success and
//! the per-activity circuit breaker is fed `Success`, ignoring any post-`next`
//! transform — so an interceptor that maps a committed `Ok` to `Err` can never
//! trip the breaker or record a failure that contradicts the recorded
//! `ActivityCompleted`. An interceptor may still legitimately *observe* a
//! transactional activity (logging, metrics, input shaping before `next.run`);
//! only its attempt to *change the outcome after* the seal is ignored, and such
//! an attempt is surfaced with a `tracing::warn!`.
//!
//! # Input is plaintext
//!
//! The `input` passed to an interceptor is the activity's already-decoded
//! plaintext JSON. Payload codecs and large-payload offloading (issues #524,
//! read-path decode) apply only to the `harvest_events` log; `harvest_task_queue`
//! stores activity input in plaintext, so an interceptor observes the real input
//! by construction — no decode step is required.
//!
//! # Local-activity timeout note
//!
//! For **local** activities the interceptor chain runs *inside* the per-attempt
//! local-activity timeout, so interceptor time counts against the activity's
//! effective `start_to_close` (capped by
//! [`WorkerConfig::max_local_activity_start_to_close`](crate::builder::WorkerConfig)).
//! Regular activities have no such inline timeout wrapping on the dispatch path.
//!
//! An interceptor sees the same [`ActivityContext`] capabilities the handler
//! does. On a local activity that means no functioning heartbeat channel:
//! `ctx.heartbeat(..)` returns a `Config` error and no checkpoint is available —
//! this is the pre-existing local-activity contract, inherited by interceptors,
//! not introduced by them.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::context::ActivityContext;

/// The future an interceptor (or the terminal handler) resolves to.
///
/// Mirrors the `Output` of [`ActivityHandlerFn`](crate::info::ActivityHandlerFn)
/// exactly, so an interceptor sits at the same fn-pointer boundary as the
/// handler and its error rides the same internal String channel.
pub type ActivityInterceptorFuture<'a> =
    Pin<Box<dyn Future<Output = Result<serde_json::Value, String>> + Send + 'a>>;

/// Read-only metadata about the activity being executed, made available to
/// interceptors. Carries context that is not reachable through
/// [`ActivityContext`] alone.
///
/// Fields are private with accessors so new metadata can be added without a
/// breaking change.
pub struct ActivityInvocation<'a> {
    name: &'a str,
    is_local: bool,
    queue_name: &'a str,
}

impl<'a> ActivityInvocation<'a> {
    /// Construct invocation metadata. Called by the worker dispatch path.
    #[must_use]
    pub(crate) const fn new(name: &'a str, is_local: bool, queue_name: &'a str) -> Self {
        Self {
            name,
            is_local,
            queue_name,
        }
    }

    /// The registered activity name.
    ///
    /// Returns a borrow tied to the invocation's own lifetime (`'a`), not to
    /// `&self`, so an interceptor may hold the name for the full invocation
    /// (e.g. into the `next.run(..).await` continuation) without cloning.
    #[must_use]
    pub const fn name(&self) -> &'a str {
        self.name
    }

    /// Whether this is a local (inline) activity (`#[activity(local = true)]`).
    #[must_use]
    pub const fn is_local(&self) -> bool {
        self.is_local
    }

    /// The task queue this activity runs on. For a local activity this is the
    /// owning workflow task's queue.
    ///
    /// Returns a borrow tied to the invocation's own lifetime (`'a`), not to
    /// `&self`, so an interceptor may hold the queue name for the full
    /// invocation without cloning.
    #[must_use]
    pub const fn queue_name(&self) -> &'a str {
        self.queue_name
    }

    /// Construct invocation metadata for a test that drives an interceptor
    /// chain outside a worker (see [`run_interceptor_chain_for_test`]).
    ///
    /// Only available under `cfg(test)` or the `testing` feature — production
    /// code never constructs an `ActivityInvocation`; the worker does.
    #[cfg(any(test, feature = "testing"))]
    #[must_use]
    pub const fn for_test(name: &'a str, is_local: bool, queue_name: &'a str) -> Self {
        Self::new(name, is_local, queue_name)
    }
}

/// The continuation handed to an interceptor.
///
/// Calling [`run`](ActivityInterceptorNext::run) proceeds to the next
/// interceptor in the chain, or to the terminal activity handler when the chain
/// is exhausted. Dropping `next` without calling `run` short-circuits the chain
/// (the handler never runs).
pub struct ActivityInterceptorNext<'a> {
    interceptors: &'a [Arc<dyn ActivityInterceptor>],
    invocation: &'a ActivityInvocation<'a>,
    ctx: &'a ActivityContext,
    terminal: Box<dyn FnOnce(serde_json::Value) -> ActivityInterceptorFuture<'a> + Send + 'a>,
}

impl<'a> ActivityInterceptorNext<'a> {
    /// Build the continuation. Called by the worker dispatch path (and by the
    /// shared [`dispatch_with_interceptors`] helper).
    #[must_use]
    pub(crate) fn new(
        interceptors: &'a [Arc<dyn ActivityInterceptor>],
        invocation: &'a ActivityInvocation<'a>,
        ctx: &'a ActivityContext,
        terminal: Box<dyn FnOnce(serde_json::Value) -> ActivityInterceptorFuture<'a> + Send + 'a>,
    ) -> Self {
        Self {
            interceptors,
            invocation,
            ctx,
            terminal,
        }
    }

    /// Proceed to the next interceptor, or the terminal handler when the chain
    /// is exhausted, with the (possibly transformed) `input`.
    #[must_use]
    pub fn run(self, input: serde_json::Value) -> ActivityInterceptorFuture<'a> {
        match self.interceptors.split_first() {
            Some((head, tail)) => {
                let next = Self {
                    interceptors: tail,
                    invocation: self.invocation,
                    ctx: self.ctx,
                    terminal: self.terminal,
                };
                head.intercept(self.invocation, self.ctx, input, next)
            }
            None => (self.terminal)(input),
        }
    }
}

/// A cross-cutting wrapper around every activity execution on the worker.
///
/// Object-safe: the trait is generic only over a **lifetime**, so it is stored
/// as `Arc<dyn ActivityInterceptor>`. Implement [`intercept`] to run code
/// before/after the rest of the chain; call `next.run(input)` to proceed, or
/// return without calling it to short-circuit.
///
/// See the [module docs](self) for ordering, error/panic transparency, and
/// replay-safety semantics.
pub trait ActivityInterceptor: Send + Sync + 'static {
    /// Wrap one activity execution. `invocation` carries read-only metadata,
    /// `ctx` is the live [`ActivityContext`], `input` is the plaintext JSON
    /// input, and `next` continues the chain.
    fn intercept<'a>(
        &'a self,
        invocation: &'a ActivityInvocation<'a>,
        ctx: &'a ActivityContext,
        input: serde_json::Value,
        next: ActivityInterceptorNext<'a>,
    ) -> ActivityInterceptorFuture<'a>;
}

/// Build the dispatch future for one activity execution, wrapping the terminal
/// handler in the configured interceptor chain.
///
/// When `interceptors` is empty this is a zero-overhead direct call to
/// `terminal` (no boxing of the continuation), so a worker with no interceptors
/// registered behaves byte-for-byte as it did before issue #680.
#[must_use]
pub(crate) fn dispatch_with_interceptors<'a>(
    interceptors: &'a [Arc<dyn ActivityInterceptor>],
    invocation: &'a ActivityInvocation<'a>,
    ctx: &'a ActivityContext,
    input: serde_json::Value,
    terminal: impl FnOnce(serde_json::Value) -> ActivityInterceptorFuture<'a> + Send + 'a,
) -> ActivityInterceptorFuture<'a> {
    if interceptors.is_empty() {
        return terminal(input);
    }
    ActivityInterceptorNext::new(interceptors, invocation, ctx, Box::new(terminal)).run(input)
}

/// Drive an interceptor chain over a terminal handler, without a worker.
///
/// The public testing entry point for interceptor authors (and this crate's own
/// examples). The caller owns `invocation` — construct one with
/// [`ActivityInvocation::for_test`] — and supplies a `terminal` closure standing
/// in for the activity handler. Semantics are identical to the on-worker
/// dispatch path: first-registered interceptor is outermost, an empty chain
/// calls the terminal directly.
///
/// Only available under `cfg(test)` or the `testing` feature.
#[cfg(any(test, feature = "testing"))]
#[must_use]
pub fn run_interceptor_chain_for_test<'a>(
    interceptors: &'a [Arc<dyn ActivityInterceptor>],
    invocation: &'a ActivityInvocation<'a>,
    ctx: &'a ActivityContext,
    input: serde_json::Value,
    terminal: impl FnOnce(serde_json::Value) -> ActivityInterceptorFuture<'a> + Send + 'a,
) -> ActivityInterceptorFuture<'a> {
    dispatch_with_interceptors(interceptors, invocation, ctx, input, terminal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Object-safety guard: `ActivityInterceptor` must be usable as a trait
    /// object (`Arc<dyn ActivityInterceptor>`). This fails to compile if the
    /// trait ever gains a non-lifetime generic or a `Self: Sized` method.
    #[allow(dead_code)]
    fn _assert_object_safe(_: &dyn ActivityInterceptor) {}

    type SharedLog = Arc<Mutex<Vec<String>>>;

    /// Records `{tag}-before` on the way in and `{tag}-after` on the way out,
    /// passing `input` through unchanged.
    struct RecordingInterceptor {
        tag: &'static str,
        log: SharedLog,
    }

    impl ActivityInterceptor for RecordingInterceptor {
        fn intercept<'a>(
            &'a self,
            _invocation: &'a ActivityInvocation<'a>,
            _ctx: &'a ActivityContext,
            input: serde_json::Value,
            next: ActivityInterceptorNext<'a>,
        ) -> ActivityInterceptorFuture<'a> {
            let log = self.log.clone();
            let tag = self.tag;
            Box::pin(async move {
                log.lock().unwrap().push(format!("{tag}-before"));
                let out = next.run(input).await;
                log.lock().unwrap().push(format!("{tag}-after"));
                out
            })
        }
    }

    #[tokio::test]
    async fn first_registered_interceptor_is_outermost() {
        let log: SharedLog = Arc::new(Mutex::new(Vec::new()));
        let interceptors: Vec<Arc<dyn ActivityInterceptor>> = vec![
            Arc::new(RecordingInterceptor {
                tag: "A",
                log: log.clone(),
            }),
            Arc::new(RecordingInterceptor {
                tag: "B",
                log: log.clone(),
            }),
        ];
        let ctx = ActivityContext::new_test();
        let invocation = ActivityInvocation::new("act", false, "default");
        let handler_log = log.clone();

        let out = dispatch_with_interceptors(
            &interceptors,
            &invocation,
            &ctx,
            serde_json::json!({"x": 1}),
            move |input| {
                Box::pin(async move {
                    handler_log.lock().unwrap().push("handler".to_string());
                    Ok(input)
                })
            },
        )
        .await;

        assert_eq!(out.unwrap(), serde_json::json!({"x": 1}));
        assert_eq!(
            *log.lock().unwrap(),
            vec!["A-before", "B-before", "handler", "B-after", "A-after"],
        );
    }

    #[tokio::test]
    async fn interceptor_transforms_input_seen_by_handler() {
        // Outer interceptor mutates input before next.run; the terminal asserts
        // it saw the mutated value and returns it.
        struct MutateInput;
        impl ActivityInterceptor for MutateInput {
            fn intercept<'a>(
                &'a self,
                _invocation: &'a ActivityInvocation<'a>,
                _ctx: &'a ActivityContext,
                mut input: serde_json::Value,
                next: ActivityInterceptorNext<'a>,
            ) -> ActivityInterceptorFuture<'a> {
                Box::pin(async move {
                    input["injected"] = serde_json::json!(true);
                    next.run(input).await
                })
            }
        }
        let interceptors: Vec<Arc<dyn ActivityInterceptor>> = vec![Arc::new(MutateInput)];
        let ctx = ActivityContext::new_test();
        let invocation = ActivityInvocation::new("act", false, "default");

        let out = dispatch_with_interceptors(
            &interceptors,
            &invocation,
            &ctx,
            serde_json::json!({"x": 1}),
            |input| {
                Box::pin(async move {
                    assert_eq!(input["injected"], serde_json::json!(true));
                    Ok(input)
                })
            },
        )
        .await
        .unwrap();
        assert_eq!(out["injected"], serde_json::json!(true));
    }

    #[tokio::test]
    async fn interceptor_transforms_result() {
        struct MapResult;
        impl ActivityInterceptor for MapResult {
            fn intercept<'a>(
                &'a self,
                _invocation: &'a ActivityInvocation<'a>,
                _ctx: &'a ActivityContext,
                input: serde_json::Value,
                next: ActivityInterceptorNext<'a>,
            ) -> ActivityInterceptorFuture<'a> {
                Box::pin(async move {
                    let r = next.run(input).await?;
                    Ok(serde_json::json!({ "wrapped": r }))
                })
            }
        }
        let interceptors: Vec<Arc<dyn ActivityInterceptor>> = vec![Arc::new(MapResult)];
        let ctx = ActivityContext::new_test();
        let invocation = ActivityInvocation::new("act", false, "default");

        let out = dispatch_with_interceptors(
            &interceptors,
            &invocation,
            &ctx,
            serde_json::json!(null),
            |_input| Box::pin(async move { Ok(serde_json::json!(42)) }),
        )
        .await
        .unwrap();
        assert_eq!(out, serde_json::json!({"wrapped": 42}));
    }

    #[tokio::test]
    async fn interceptor_short_circuits_without_calling_next() {
        struct ShortCircuit;
        impl ActivityInterceptor for ShortCircuit {
            fn intercept<'a>(
                &'a self,
                _invocation: &'a ActivityInvocation<'a>,
                _ctx: &'a ActivityContext,
                _input: serde_json::Value,
                _next: ActivityInterceptorNext<'a>,
            ) -> ActivityInterceptorFuture<'a> {
                // Deliberately does NOT call next.run — handler must never run.
                Box::pin(async move { Ok(serde_json::json!("short")) })
            }
        }
        let handler_ran = Arc::new(Mutex::new(false));
        let interceptors: Vec<Arc<dyn ActivityInterceptor>> = vec![Arc::new(ShortCircuit)];
        let ctx = ActivityContext::new_test();
        let invocation = ActivityInvocation::new("act", false, "default");
        let ran = handler_ran.clone();

        let out = dispatch_with_interceptors(
            &interceptors,
            &invocation,
            &ctx,
            serde_json::json!("in"),
            move |input| {
                Box::pin(async move {
                    *ran.lock().unwrap() = true;
                    Ok(input)
                })
            },
        )
        .await
        .unwrap();
        assert_eq!(out, serde_json::json!("short"));
        assert!(
            !*handler_ran.lock().unwrap(),
            "handler must not run when an interceptor short-circuits"
        );
    }

    #[tokio::test]
    async fn interceptor_transforms_error() {
        struct WrapError;
        impl ActivityInterceptor for WrapError {
            fn intercept<'a>(
                &'a self,
                _invocation: &'a ActivityInvocation<'a>,
                _ctx: &'a ActivityContext,
                input: serde_json::Value,
                next: ActivityInterceptorNext<'a>,
            ) -> ActivityInterceptorFuture<'a> {
                Box::pin(async move {
                    match next.run(input).await {
                        Ok(v) => Ok(v),
                        Err(e) => Err(format!("wrapped: {e}")),
                    }
                })
            }
        }
        let interceptors: Vec<Arc<dyn ActivityInterceptor>> = vec![Arc::new(WrapError)];
        let ctx = ActivityContext::new_test();
        let invocation = ActivityInvocation::new("act", false, "default");

        let err = dispatch_with_interceptors(
            &interceptors,
            &invocation,
            &ctx,
            serde_json::json!(null),
            |_input| Box::pin(async move { Err("boom".to_string()) }),
        )
        .await
        .unwrap_err();
        assert_eq!(err, "wrapped: boom");
    }

    #[tokio::test]
    async fn empty_chain_calls_terminal_directly() {
        let interceptors: Vec<Arc<dyn ActivityInterceptor>> = Vec::new();
        let ctx = ActivityContext::new_test();
        let invocation = ActivityInvocation::new("act", false, "default");

        let out = dispatch_with_interceptors(
            &interceptors,
            &invocation,
            &ctx,
            serde_json::json!("terminal-value"),
            |input| Box::pin(async move { Ok(input) }),
        )
        .await
        .unwrap();
        assert_eq!(out, serde_json::json!("terminal-value"));
    }
}
