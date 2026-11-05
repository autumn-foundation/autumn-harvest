//! **C20** — a test-only `proptest!` block that names `String` must not
//! change the verdict of a workflow (issue #2545).
//!
//! The workflow captures a `String` in a `Saga` step. The analyzer proves it.
//! The `proptest!` block below names `String`. It declares tests, not
//! types, so `String` stays a plain type. Verdict: `proven-deterministic`.

use autumn_harvest::Saga;
use autumn_harvest::prelude::*;

/// Reserves stock in a `Saga` step that captures a `String`.
#[workflow]
pub async fn wf_test_macro_names_type(ctx: &WorkflowContext, sku: String) -> Result<u64, String> {
    let mut saga = Saga::new(ctx);
    saga.step::<u64, serde_json::Value, _, _, _, _>(
        || async {
            ctx.execute_activity_raw("reserve", serde_json::json!({ "sku": sku }), "corpus")
                .await
                .map(|_| 1)
        },
        |count| async move {
            ctx.execute_activity_raw("release", serde_json::json!({ "count": count }), "corpus")
                .await
        },
    )
    .await
    .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn any_label_is_kept(label in any::<String>()) {
            prop_assert_eq!(label.clone(), label);
        }
    }
}
