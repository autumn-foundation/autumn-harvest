//! Fixture: a wrapper type declared in a nested module file (issue #2010).
//!
//! MIR names no span in `state/shared.rs`. The source index must still read
//! its fields. `../RUSTC_VERSION.txt` gives the build commands.
#![allow(dead_code)]

mod state;

use autumn_harvest::{Saga, WorkflowContext};
use state::shared::Shared;

type Out = Result<u64, String>;

fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Writes `value` through the wrapper.
fn set(shared: &Shared, value: u64) {
    if let Ok(mut v) = shared.0.lock() {
        *v = value;
    }
}

pub fn __autumn_workflow_info_wf_step_write_modfile() -> u8 {
    0
}

/// A step block writes the clock through a captured `Shared`.
pub async fn wf_step_write_modfile(ctx: &WorkflowContext) -> Out {
    let seen = std::sync::Arc::new(std::sync::Mutex::new(0_u64));
    {
        let mut saga = Saga::new(ctx);
        let handle = Shared(std::sync::Arc::clone(&seen));
        saga.step(
            move || async move {
                set(&handle, now_nanos());
                ctx.execute_activity_raw("a", 1).await
            },
            |_| async { Ok(()) },
        )
        .await?;
    }
    let read = seen.lock().map_or(0, |v| *v);
    ctx.execute_activity_raw("b", read).await
}
