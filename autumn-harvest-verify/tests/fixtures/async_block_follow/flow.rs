//! Fixture: the `async` block a closure returns (issue #2010).
//!
//! The analysis follows that block only when a bodyless callee polls it. It
//! must not turn a former `unknown` into a false `proven-deterministic`.
//! `../RUSTC_VERSION.txt` gives the build commands.
#![allow(dead_code, unused)]

use std::future::Future;

use autumn_harvest::{Saga, WorkflowContext};

type Out = Result<u64, String>;

fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

pub fn __autumn_workflow_info_wf_saga_clock() -> u8 {
    0
}

/// A clock read inside the `async` block of a saga step.
pub async fn wf_saga_clock(ctx: &WorkflowContext) -> Out {
    let mut saga = Saga::new(ctx);
    saga.step(
        || async { ctx.execute_activity_raw("r", now_nanos()).await },
        |_| async { Ok(()) },
    )
    .await
}

/// A first-party `async fn` that reads the clock.
async fn reserve(ctx: &WorkflowContext) -> Out {
    ctx.execute_activity_raw("reserve", now_nanos()).await
}

pub fn __autumn_workflow_info_wf_saga_async_fn() -> u8 {
    0
}

/// The saga step closure returns the future of a first-party `async fn`.
pub async fn wf_saga_async_fn(ctx: &WorkflowContext) -> Out {
    let mut saga = Saga::new(ctx);
    saga.step(|| reserve(ctx), |_| async { Ok(()) }).await
}

pub fn __autumn_workflow_info_wf_external() -> u8 {
    0
}

/// An untrusted callee with an `async` block in its turbofish.
pub async fn wf_external(ctx: &WorkflowContext) -> Out {
    other_crate::wrap(async { ctx.execute_activity_raw("a", 1).await }).await
}

macro_rules! blk {
    ($ctx:expr, $v:expr) => {
        async move { $ctx.execute_activity_raw("m", $v).await }
    };
}

fn helper(ctx: &WorkflowContext) -> impl Future<Output = Out> + '_ {
    blk!(ctx, now_nanos())
}

pub fn __autumn_workflow_info_wf_shared_span() -> u8 {
    0
}

/// Two `async` blocks share one macro span. One of them reads the clock.
pub async fn wf_shared_span(ctx: &WorkflowContext) -> Out {
    let _ = blk!(ctx, 1).await?;
    helper(ctx).await
}

pub fn __autumn_workflow_info_wf_block_write() -> u8 {
    0
}

/// An awaited `async` block writes the clock into a captured local.
pub async fn wf_block_write(ctx: &WorkflowContext) -> Out {
    let mut seen = 0_u64;
    async {
        seen = now_nanos();
    }
    .await;
    ctx.execute_activity_raw("b", seen).await
}

pub fn __autumn_workflow_info_wf_step_write() -> u8 {
    0
}

/// A saga step block writes the clock through a captured reference.
pub async fn wf_step_write(ctx: &WorkflowContext) -> Out {
    let mut seen = 0_u64;
    {
        let mut saga = Saga::new(ctx);
        saga.step(
            || {
                let s = &mut seen;
                async move {
                    *s = now_nanos();
                    ctx.execute_activity_raw("a", 1).await
                }
            },
            |_| async { Ok(()) },
        )
        .await?;
    }
    ctx.execute_activity_raw("b", seen).await
}

pub fn __autumn_workflow_info_wf_foreign_block() -> u8 {
    0
}

/// The step closure returns a future built in another crate.
pub async fn wf_foreign_block(ctx: &WorkflowContext) -> Out {
    let mut saga = Saga::new(ctx);
    saga.step(|| ctx.rpit_block(1), |_| async { Ok(()) }).await
}

pub fn __autumn_workflow_info_wf_step_write_tuple() -> u8 {
    0
}

/// The same write, through a `&mut` packed in a tuple.
pub async fn wf_step_write_tuple(ctx: &WorkflowContext) -> Out {
    let mut seen = 0_u64;
    {
        let mut saga = Saga::new(ctx);
        saga.step(
            || {
                let refs = (&mut seen,);
                async move {
                    *refs.0 = now_nanos();
                    ctx.execute_activity_raw("a", 1).await
                }
            },
            |_| async { Ok(()) },
        )
        .await?;
    }
    ctx.execute_activity_raw("b", seen).await
}

/// A named type that hides a `&mut`.
pub struct Refs<'a> {
    seen: &'a mut u64,
}

fn record(refs: Refs<'_>) {
    *refs.seen = now_nanos();
}

pub fn __autumn_workflow_info_wf_step_write_struct() -> u8 {
    0
}

/// The same write, through a `&mut` inside a named struct.
pub async fn wf_step_write_struct(ctx: &WorkflowContext) -> Out {
    let mut seen = 0_u64;
    {
        let mut saga = Saga::new(ctx);
        saga.step(
            || {
                let refs = Refs { seen: &mut seen };
                async move {
                    record(refs);
                    ctx.execute_activity_raw("a", 1).await
                }
            },
            |_| async { Ok(()) },
        )
        .await?;
    }
    ctx.execute_activity_raw("b", seen).await
}

pub fn __autumn_workflow_info_wf_step_write_cell() -> u8 {
    0
}

/// The same write, through a shared `&Cell` capture.
pub async fn wf_step_write_cell(ctx: &WorkflowContext) -> Out {
    let seen = std::cell::Cell::new(0_u64);
    {
        let mut saga = Saga::new(ctx);
        saga.step(
            || {
                let cell = &seen;
                async move {
                    cell.set(now_nanos());
                    ctx.execute_activity_raw("a", 1).await
                }
            },
            |_| async { Ok(()) },
        )
        .await?;
    }
    ctx.execute_activity_raw("b", seen.get()).await
}

pub fn __autumn_workflow_info_wf_step_write_arc() -> u8 {
    0
}

/// The same write, through an owned `Arc<Mutex<_>>` clone.
pub async fn wf_step_write_arc(ctx: &WorkflowContext) -> Out {
    let seen = std::sync::Arc::new(std::sync::Mutex::new(0_u64));
    {
        let mut saga = Saga::new(ctx);
        saga.step(
            || {
                let shared = std::sync::Arc::clone(&seen);
                async move {
                    if let Ok(mut slot) = shared.lock() {
                        *slot = now_nanos();
                    }
                    ctx.execute_activity_raw("a", 1).await
                }
            },
            |_| async { Ok(()) },
        )
        .await?;
    }
    let value = seen.lock().map_or(0, |slot| *slot);
    ctx.execute_activity_raw("b", value).await
}

/// A first-party future whose `poll` reads the clock.
pub struct ClockFuture;

impl Future for ClockFuture {
    type Output = Out;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Out> {
        std::task::Poll::Ready(Ok(now_nanos()))
    }
}

pub fn __autumn_workflow_info_wf_step_named_future() -> u8 {
    0
}

/// The step closure returns a named future, and its result feeds a command.
pub async fn wf_step_named_future(ctx: &WorkflowContext) -> Out {
    let mut saga = Saga::new(ctx);
    let a = saga.step(|| ClockFuture, |_| async { Ok(()) }).await?;
    ctx.execute_activity_raw("b", a).await
}

/// A first-party future that holds caller state and writes it in `poll`.
pub struct WriteFuture<'a> {
    seen: &'a mut u64,
}

impl Future for WriteFuture<'_> {
    type Output = Out;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Out> {
        *self.seen = now_nanos();
        std::task::Poll::Ready(Ok(1))
    }
}

pub fn __autumn_workflow_info_wf_step_named_write_future() -> u8 {
    0
}

/// The named future writes the clock into caller state.
pub async fn wf_step_named_write_future(ctx: &WorkflowContext) -> Out {
    let mut seen = 0_u64;
    {
        let mut saga = Saga::new(ctx);
        saga.step(|| WriteFuture { seen: &mut seen }, |_| async { Ok(()) })
            .await?;
    }
    ctx.execute_activity_raw("b", seen).await
}

pub fn __autumn_workflow_info_wf_step_mut_ref_future() -> u8 {
    0
}

/// The step closure returns `&mut F` for a first-party future `F`.
pub async fn wf_step_mut_ref_future(ctx: &WorkflowContext) -> Out {
    let mut fut = ClockFuture;
    let mut saga = Saga::new(ctx);
    let a = saga.step(|| &mut fut, |_| async { Ok(()) }).await?;
    ctx.execute_activity_raw("b", a).await
}

pub fn __autumn_workflow_info_wf_step_move_mut() -> u8 {
    0
}

/// A `move` closure hands its captured `&mut` to the block.
pub async fn wf_step_move_mut(ctx: &WorkflowContext) -> Out {
    let mut seen = 0_u64;
    {
        let s = &mut seen;
        let mut saga = Saga::new(ctx);
        saga.step(
            move || async move {
                *s = now_nanos();
                ctx.execute_activity_raw("a", 1).await
            },
            |_| async { Ok(()) },
        )
        .await?;
    }
    ctx.execute_activity_raw("b", seen).await
}

type BoxedOut<'a> = std::pin::Pin<Box<dyn Future<Output = Out> + Send + 'a>>;

pub fn __autumn_workflow_info_wf_step_boxed_dyn() -> u8 {
    0
}

/// The step closure erases its block behind `Pin<Box<dyn Future>>`.
pub async fn wf_step_boxed_dyn(ctx: &WorkflowContext) -> Out {
    let mut saga = Saga::new(ctx);
    let a = saga
        .step(
            || -> BoxedOut<'_> { Box::pin(async { Ok(now_nanos()) }) },
            |_| async { Ok(()) },
        )
        .await?;
    ctx.execute_activity_raw("b", a).await
}

/// A first-party wrapper whose `poll` ignores the inner future.
pub struct Wrap<F>(F);

impl<F: Future<Output = Out>> Future for Wrap<F> {
    type Output = Out;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Out> {
        std::task::Poll::Ready(Ok(now_nanos()))
    }
}

pub fn __autumn_workflow_info_wf_step_wrapped_block() -> u8 {
    0
}

/// The step closure returns `Wrap<{async block}>`.
pub async fn wf_step_wrapped_block(ctx: &WorkflowContext) -> Out {
    let mut saga = Saga::new(ctx);
    let a = saga
        .step(|| Wrap(async { Ok(1) }), |_| async { Ok(()) })
        .await?;
    ctx.execute_activity_raw("b", a).await
}

pub fn __autumn_workflow_info_wf_step_external_future() -> u8 {
    0
}

/// The step closure builds an untrusted crate's future with no call.
pub async fn wf_step_external_future(ctx: &WorkflowContext) -> Out {
    let mut saga = Saga::new(ctx);
    let a = saga
        .step(|| other_crate::ClockFuture, |_| async { Ok(()) })
        .await?;
    ctx.execute_activity_raw("b", a).await
}

pub fn __autumn_workflow_info_wf_step_boxed_external() -> u8 {
    0
}

/// The step closure erases an untrusted unit future behind `dyn Future`.
pub async fn wf_step_boxed_external(ctx: &WorkflowContext) -> Out {
    let mut saga = Saga::new(ctx);
    let a = saga
        .step(
            || -> BoxedOut<'_> { Box::pin(other_crate::ClockFuture) },
            |_| async { Ok(()) },
        )
        .await?;
    ctx.execute_activity_raw("b", a).await
}

pub fn __autumn_workflow_info_wf_step_async_closure() -> u8 {
    0
}

/// The forward step is an `async` closure that reads the clock.
pub async fn wf_step_async_closure(ctx: &WorkflowContext) -> Out {
    let mut saga = Saga::new(ctx);
    let a = saga.step(async || Ok(now_nanos()), |_| async { Ok(()) }).await?;
    ctx.execute_activity_raw("b", a).await
}

/// Returns its argument. A first-party helper with a body.
pub fn identity<T>(value: T) -> T {
    value
}

pub fn __autumn_workflow_info_wf_step_identity_write() -> u8 {
    0
}

/// The step block writes the clock through a captured reference. The
/// closure builds it in a temporary and returns it through `identity`.
pub async fn wf_step_identity_write(ctx: &WorkflowContext) -> Out {
    let mut seen = 0_u64;
    {
        let mut saga = Saga::new(ctx);
        saga.step(
            || {
                let s = &mut seen;
                let f = async move {
                    *s = now_nanos();
                    ctx.execute_activity_raw("a", 1).await
                };
                identity(f)
            },
            |_| async { Ok(()) },
        )
        .await?;
    }
    ctx.execute_activity_raw("b", seen).await
}

pub fn __autumn_workflow_info_wf_step_moved_write() -> u8 {
    0
}

/// The step block writes the clock through a captured reference. The
/// closure moves it through a tuple before it returns it.
pub async fn wf_step_moved_write(ctx: &WorkflowContext) -> Out {
    let mut seen = 0_u64;
    {
        let mut saga = Saga::new(ctx);
        saga.step(
            || {
                let s = &mut seen;
                let f = async move {
                    *s = now_nanos();
                    ctx.execute_activity_raw("a", 1).await
                };
                let pair = (f, 1_u8);
                pair.0
            },
            |_| async { Ok(()) },
        )
        .await?;
    }
    ctx.execute_activity_raw("b", seen).await
}

/// A first-party future that holds shared state and writes it in `poll`.
pub struct SharedFuture {
    shared: std::sync::Arc<std::sync::Mutex<u64>>,
}

impl Future for SharedFuture {
    type Output = Out;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Out> {
        if let Ok(mut seen) = self.shared.lock() {
            *seen = now_nanos();
        }
        std::task::Poll::Ready(Ok(1))
    }
}

pub fn __autumn_workflow_info_wf_step_named_shared_future() -> u8 {
    0
}

/// The named future writes the clock into shared state the caller reads.
pub async fn wf_step_named_shared_future(ctx: &WorkflowContext) -> Out {
    let shared = std::sync::Arc::new(std::sync::Mutex::new(0_u64));
    {
        let mut saga = Saga::new(ctx);
        let handle = shared.clone();
        saga.step(
            move || SharedFuture { shared: handle },
            |_| async { Ok(()) },
        )
        .await?;
    }
    let seen = shared.lock().map_or(0, |v| *v);
    ctx.execute_activity_raw("b", seen).await
}

impl SharedFuture {
    /// Builds the future around the shared state.
    pub fn new(shared: std::sync::Arc<std::sync::Mutex<u64>>) -> Self {
        Self { shared }
    }
}

pub fn __autumn_workflow_info_wf_step_named_shared_ctor() -> u8 {
    0
}

/// As `wf_step_named_shared_future`, through a first-party constructor.
pub async fn wf_step_named_shared_ctor(ctx: &WorkflowContext) -> Out {
    let shared = std::sync::Arc::new(std::sync::Mutex::new(0_u64));
    {
        let mut saga = Saga::new(ctx);
        let handle = shared.clone();
        saga.step(move || SharedFuture::new(handle), |_| async { Ok(()) })
            .await?;
    }
    let seen = shared.lock().map_or(0, |v| *v);
    ctx.execute_activity_raw("b", seen).await
}

/// A lifetime-free newtype that hides its interior mutability.
#[derive(Clone)]
pub struct Shared(std::sync::Arc<std::sync::Mutex<u64>>);

impl Shared {
    /// Writes `value` into the shared state.
    pub fn set(&self, value: u64) {
        if let Ok(mut v) = self.0.lock() {
            *v = value;
        }
    }
}

pub fn __autumn_workflow_info_wf_step_write_newtype() -> u8 {
    0
}

/// A saga step block writes the clock through a captured newtype clone.
pub async fn wf_step_write_newtype(ctx: &WorkflowContext) -> Out {
    let seen = Shared(std::sync::Arc::new(std::sync::Mutex::new(0_u64)));
    {
        let mut saga = Saga::new(ctx);
        let handle = seen.clone();
        saga.step(
            move || async move {
                handle.set(now_nanos());
                ctx.execute_activity_raw("a", 1).await
            },
            |_| async { Ok(()) },
        )
        .await?;
    }
    let read = seen.0.lock().map_or(0, |v| *v);
    ctx.execute_activity_raw("b", read).await
}

#[path = "noext"]
mod noext;

pub fn __autumn_workflow_info_wf_step_raw_ptr_write() -> u8 {
    0
}

/// A saga step block writes the clock through a captured raw pointer.
pub async fn wf_step_raw_ptr_write(ctx: &WorkflowContext) -> Out {
    let mut seen = 0_u64;
    let p: *mut u64 = &raw mut seen;
    {
        let mut saga = Saga::new(ctx);
        saga.step(
            || async move {
                // SAFETY: `seen` outlives the step, and nothing else holds it.
                unsafe { *p = now_nanos() };
                ctx.execute_activity_raw("a", 1).await
            },
            |_| async { Ok(()) },
        )
        .await?;
    }
    ctx.execute_activity_raw("b", seen).await
}

pub fn __autumn_workflow_info_wf_step_closure_capture() -> u8 {
    0
}

/// A saga step block calls a captured closure that writes the clock into
/// shared state. A clone of that state feeds a later command.
pub async fn wf_step_closure_capture(ctx: &WorkflowContext) -> Out {
    let shared = std::sync::Arc::new(std::sync::Mutex::new(0_u64));
    let kept = std::sync::Arc::clone(&shared);
    {
        let mut saga = Saga::new(ctx);
        saga.step(
            move || {
                let write = move || *shared.lock().unwrap() = now_nanos();
                async move {
                    write();
                    ctx.execute_activity_raw("a", 1).await
                }
            },
            |_| async { Ok(()) },
        )
        .await?;
    }
    let seen = *kept.lock().unwrap();
    ctx.execute_activity_raw("b", seen).await
}

/// A future that only a shared reference implements. Its `poll` reads the
/// clock.
pub struct RefClock;

impl Future for &RefClock {
    type Output = Out;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::task::Poll::Ready(Ok(now_nanos()))
    }
}

static REF_CLOCK: RefClock = RefClock;

pub fn __autumn_workflow_info_wf_step_ref_future() -> u8 {
    0
}

/// The forward step returns `&RefClock`, a future by its `&` impl.
pub async fn wf_step_ref_future(ctx: &WorkflowContext) -> Out {
    let mut saga = Saga::new(ctx);
    let a = saga.step(|| &REF_CLOCK, |_| async { Ok(()) }).await?;
    ctx.execute_activity_raw("b", a).await
}
