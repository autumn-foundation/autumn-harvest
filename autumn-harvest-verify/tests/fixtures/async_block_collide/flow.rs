//! Fixture: a dependency crate and a local module share the name `helpers`
//! (issue #2010).
//!
//! MIR prints both paths as `helpers::..`. The dependency future must stay a
//! boundary. `../RUSTC_VERSION.txt` gives the build commands.
#![allow(dead_code)]

use autumn_harvest::{Saga, WorkflowContext};

type Out = Result<u64, String>;

/// A local module named like the dependency. Its body path keeps the module.
pub mod helpers {
    pub fn local() -> u8 {
        1
    }
}

/// A root function of the same name, so MIR cannot trim `helpers::local`.
pub fn local() -> u8 {
    2
}

/// An unrelated local type with the dependency type's name. Its `poll`
/// reads no clock, so following it would hide the dependency `poll`.
pub struct ClockFuture {
    n: u64,
}

impl std::future::Future for ClockFuture {
    type Output = Out;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Out> {
        std::task::Poll::Ready(Ok(self.n))
    }
}

pub fn __autumn_workflow_info_wf_colliding_crate() -> u8 {
    0
}

/// The step closure builds the dependency future with no call.
pub async fn wf_colliding_crate(ctx: &WorkflowContext) -> Out {
    let mut saga = Saga::new(ctx);
    let a = saga
        .step(|| ::helpers::ClockFuture, |_| async { Ok(()) })
        .await?;
    ctx.execute_activity_raw("b", a + u64::from(helpers::local())).await
}
