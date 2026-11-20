//! Keep a large child future off the stack of a debug build (issue #2752).
//!
//! A debug build makes a child future in the frame of its caller. A caller
//! of that caller then holds a copy too. `Box::pin(child())` keeps the child
//! out of the parent future. But the caller frame still holds a copy, because
//! the caller makes the future before the box takes it.
//!
//! [`boxed`] makes the future in its own frame, and that frame ends before
//! the poll. So the caller holds a pointer only. An optimized build does not
//! need the box. It still pays one allocation for each call.
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
//!
//! # In a workflow that harvest-verify checks
//!
//! harvest-verify reports a boundary for a closure future that names a
//! lifetime or captures `&mut`. The workflow then stays `unknown`. Use a
//! named function with the same shape there:
//!
//! ```
//! use std::future::Future;
//! use std::pin::Pin;
//!
//! async fn step(items: &mut Vec<u32>) -> usize {
//!     items.push(1);
//!     items.len()
//! }
//!
//! #[inline(never)]
//! fn boxed_step(items: &mut Vec<u32>) -> Pin<Box<impl Future<Output = usize> + '_>> {
//!     Box::pin(step(items))
//! }
//! # let mut items = Vec::new();
//! # assert_eq!(futures::executor::block_on(boxed_step(&mut items)), 1);
//! ```

use std::future::Future;
use std::pin::Pin;

/// Make the future of `make` on the heap. See the [module docs](self).
///
/// `#[inline(never)]` keeps the frame of `boxed` apart from the frame of its
/// caller.
#[inline(never)]
pub fn boxed<F, Fut>(make: F) -> Pin<Box<Fut>>
where
    F: FnOnce() -> Fut,
    Fut: Future,
{
    Box::pin(make())
}
