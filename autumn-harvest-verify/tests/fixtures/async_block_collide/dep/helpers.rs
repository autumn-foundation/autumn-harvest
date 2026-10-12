//! An untrusted dependency whose crate name is also a local module name
//! (issue #2010).
use std::future::Future;

/// A call-free future whose `poll` reads the clock.
pub struct ClockFuture;

impl Future for ClockFuture {
    type Output = Result<u64, String>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let now = std::time::SystemTime::now();
        std::task::Poll::Ready(Ok(now.elapsed().map_or(0, |d| d.as_secs())))
    }
}
