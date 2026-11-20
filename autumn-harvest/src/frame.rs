//! Keep a large child future off the stack of a debug build (issue #2752).
//!
//! A debug build makes a child future in the frame of its caller. A caller
//! of that caller then holds a copy too. `Box::pin(child())` does not help,
//! because the caller still makes the future before the box takes it.
//!
//! [`boxed`] makes the future in its own frame, and that frame ends before
//! the poll. So the caller holds a pointer only. An optimized build keeps
//! small frames without it, and pays one allocation per call.
//!
//! ```
//! use autumn_harvest::frame::boxed;
//!
//! async fn child(input: u32) -> u32 {
//!     input + 1
//! }
//!
//! async fn parent() -> u32 {
//!     boxed(|| child(1)).await
//! }
//! # assert_eq!(futures::executor::block_on(parent()), 2);
//! ```

use std::future::Future;
use std::pin::Pin;

/// Make the future of `make` on the heap. See the [module docs](self).
#[inline(never)]
pub fn boxed<F, Fut>(make: F) -> Pin<Box<Fut>>
where
    F: FnOnce() -> Fut,
    Fut: Future,
{
    Box::pin(make())
}
