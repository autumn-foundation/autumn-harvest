//! Fixture: a workflow crate's own type named `Saga` (issue #2010).
//!
//! The check must not read it as the engine's `Saga`.
//! `../RUSTC_VERSION.txt` gives the build commands.
#![allow(dead_code)]

use autumn_harvest::WorkflowContext;

type Out = Result<u64, String>;

/// A local type that happens to be named `Saga`.
mod own {
    pub struct Saga {
        steps: u64,
    }

    impl Saga {
        pub const fn new() -> Self {
            Self { steps: 0 }
        }

        pub async fn step(&mut self, value: u64) -> Result<u64, String> {
            Ok(value)
        }

        pub async fn compensate_all(&mut self) -> Result<(), String> {
            Ok(())
        }

        /// Takes the engine's saga away, out of the check's sight.
        pub fn consume(saga: autumn_harvest::Saga<'_>) {
            drop(saga);
        }
    }
}

pub fn __autumn_workflow_info_wf_own_saga_type() -> u8 {
    0
}

/// Uses `own::Saga`, not the engine's. It registers no compensation.
pub async fn wf_own_saga_type(ctx: &WorkflowContext) -> Out {
    let mut saga = own::Saga::new();
    let a = saga.step(1).await?;
    saga.compensate_all().await?;
    ctx.execute_activity_raw("ship", a).await
}

pub fn __autumn_workflow_info_wf_own_type_takes_engine_saga() -> u8 {
    0
}

/// An engine saga passed to a method of the local `own::Saga`.
pub async fn wf_own_type_takes_engine_saga(ctx: &WorkflowContext) -> Out {
    let mut saga = autumn_harvest::Saga::new(ctx);
    let a = saga
        .step(
            || async { ctx.execute_activity_raw("reserve", 1).await },
            |a| async move { ctx.execute_activity_raw("release", a).await.map(|_| ()) },
        )
        .await?;
    own::Saga::consume(saga);
    ctx.execute_activity_raw("ship", a).await
}

/// A second local `Saga` with a method named like an engine method.
mod other {
    pub struct Saga {
        n: u64,
    }

    impl Saga {
        /// Takes the engine saga and runs no compensation.
        pub async fn compensate_all(saga: &mut autumn_harvest::Saga<'_>) -> Result<(), String> {
            let _ = saga;
            Ok(())
        }
    }
}

/// A local `Saga` at the crate root. MIR prints its path with no module.
pub struct Saga {
    n: u64,
}

impl Saga {
    /// Takes the engine saga and runs no compensation.
    pub async fn compensate_all(saga: &mut autumn_harvest::Saga<'_>) -> Result<(), String> {
        let _ = saga;
        Ok(())
    }
}

pub fn __autumn_workflow_info_wf_module_lookalike_compensate() -> u8 {
    0
}

/// `other::Saga::compensate_all` is not the engine unwind.
pub async fn wf_module_lookalike_compensate(ctx: &WorkflowContext) -> Out {
    let mut saga = autumn_harvest::Saga::new(ctx);
    let a = saga
        .step(
            || async { ctx.execute_activity_raw("reserve", 1).await },
            |a| async move { ctx.execute_activity_raw("release", a).await.map(|_| ()) },
        )
        .await?;
    other::Saga::compensate_all(&mut saga).await?;
    Err(format!("late {a}"))
}

pub fn __autumn_workflow_info_wf_root_lookalike_compensate() -> u8 {
    0
}

/// The crate-root `Saga::compensate_all` is not the engine unwind.
pub async fn wf_root_lookalike_compensate(ctx: &WorkflowContext) -> Out {
    let mut saga = autumn_harvest::Saga::new(ctx);
    let a = saga
        .step(
            || async { ctx.execute_activity_raw("reserve", 1).await },
            |a| async move { ctx.execute_activity_raw("release", a).await.map(|_| ()) },
        )
        .await?;
    Saga::compensate_all(&mut saga).await?;
    Err(format!("late {a}"))
}
