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
