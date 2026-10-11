//! Fixture: the engine reached through a renamed crate and a facade (issue
//! #2010).
//!
//! MIR prints the defining crate, `autumn_harvest::Saga`, for both. The check
//! must still see each saga. `../RUSTC_VERSION.txt` gives the build commands.
#![allow(dead_code)]

type Out = Result<u64, String>;

pub fn __autumn_workflow_info_wf_renamed_crate() -> u8 {
    0
}

/// The engine crate is renamed to `ah`. The late error is a gap.
pub async fn wf_renamed_crate(ctx: &ah::WorkflowContext) -> Out {
    let mut saga = ah::Saga::new(ctx);
    let a = saga
        .step(
            || async { ctx.execute_activity_raw("reserve", 1).await },
            |a| async move { ctx.execute_activity_raw("release", a).await.map(|_| ()) },
        )
        .await?;
    Err(format!("late {a}"))
}

pub fn __autumn_workflow_info_wf_facade_crate() -> u8 {
    0
}

/// The engine `Saga` comes through a facade crate. The late error is a gap.
pub async fn wf_facade_crate(ctx: &facade::WorkflowContext) -> Out {
    let mut saga = facade::Saga::new(ctx);
    let a = saga
        .step(
            || async { ctx.execute_activity_raw("reserve", 1).await },
            |a| async move { ctx.execute_activity_raw("release", a).await.map(|_| ()) },
        )
        .await?;
    Err(format!("late {a}"))
}
