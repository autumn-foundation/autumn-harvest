//! **S23** — `Uuid::new_v4()` in a helper: the rule exists, the rule does not
//! protect you.
//!
//! **Mechanism.** `Value`. `helpers::idem_key()` is `uuid::Uuid::new_v4()`. The
//! UUID becomes the activity's idempotency key, so every replay proposes a
//! different key and event correlation breaks.
//!
//! **Launder.** This is the cleanest demonstration in the corpus that *rule
//! coverage is not protection*. HVG002 lists `Uuid::new_v4` explicitly and
//! DET003 lists `Uuid::new_v4(` and `uuid::Uuid::new_v4` explicitly — both
//! know this exact call. HVG only visits the annotated body, which has no
//! `Uuid` token; det_check resolves one hop to a **same-file, same-module**
//! helper, and `idem_key` is in another crate. One crate boundary turns a
//! fully-covered rule into a false negative.
//!
//! **Expected trace hops.** `wf_uuid_in_helper` ⇒
//! `harvest_verify_corpus_helpers::idem_key` ⇒ `uuid::Uuid::new_v4` (the
//! source) ⇒ `Uuid::to_string` ⇒ `WorkflowContext::execute_activity_raw`.

use autumn_harvest::prelude::*;
use harvest_verify_corpus_helpers as helpers;

/// Submits a payment with a freshly minted idempotency key.
#[workflow]
pub async fn wf_uuid_in_helper(ctx: &WorkflowContext, amount: u32) -> Result<String, String> {
    let key = helpers::idem_key().to_string();
    ctx.execute_activity_raw(
        "pay",
        serde_json::json!({ "amount": amount, "key": key }),
        "corpus",
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(key)
}
