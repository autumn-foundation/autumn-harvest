//! **S31** — a clock read inside a `Saga` compensation closure.
//!
//! **Mechanism.** `Value` inside a closure that the engine runs later.
//! `Saga::step` stores the compensation and runs it on an unwind. Its
//! closure reads the clock through `fine_stamp` and passes the value to an
//! activity. The engine's future has no body here and is trusted. So the
//! finding rests on analyzing the closure where it is passed to `step`.
//!
//! **Launder.** `fine_stamp` is in another crate, where det_check does not
//! follow, and HVG never leaves the annotated body.
//!
//! **Expected trace hops.** the compensation closure ⇒ `fine_stamp` ⇒
//! `SystemTime::now` ⇒ `WorkflowContext::execute_activity_raw`.

use autumn_harvest::Saga;
use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers_deep as deep;

/// Reserves stock, and stamps the release with the wall clock.
#[workflow]
pub async fn wf_saga_compensation_clock(ctx: &WorkflowContext, sku: String) -> Result<u64, String> {
    let mut saga = Saga::new(ctx);
    let reserved = saga
        .step::<u64, serde_json::Value, _, _, _, _>(
            || async {
                ctx.execute_activity_raw("reserve", serde_json::json!({ "sku": sku }), "corpus")
                    .await
                    .map(|_| 1)
            },
            |count| async move {
                let at = deep::fine_stamp();
                ctx.execute_activity_raw(
                    "release",
                    serde_json::json!({ "count": count, "at": at }),
                    "corpus",
                )
                .await
            },
        )
        .await
        .map_err(|e| e.to_string())?;
    Ok(reserved)
}
