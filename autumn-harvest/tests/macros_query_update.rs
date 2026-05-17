//! Integration tests for the `#[query]` and `#[update]` declarative macros (issue #346).
//!
//! Run with:
//!   cargo test -p autumn-harvest --test macros_query_update --features testing
#![allow(clippy::unused_async, clippy::used_underscore_binding)]

use autumn_harvest::prelude::*;

// ── Helper types ──────────────────────────────────────────────────────────────

#[derive(serde::Deserialize, serde::Serialize, Clone)]
struct StatusRequest {
    verbose: bool,
}

#[derive(serde::Deserialize, serde::Serialize, Clone, PartialEq, Debug)]
struct StatusResponse {
    status: String,
}

#[derive(serde::Deserialize, serde::Serialize, Clone)]
struct ApproveRequest {
    approved: bool,
}

// ── Query handlers ────────────────────────────────────────────────────────────

// Single-param query
#[query(workflow = "my_workflow")]
fn get_status(req: StatusRequest) -> Result<StatusResponse, String> {
    Ok(StatusResponse {
        status: if req.verbose {
            "RUNNING (verbose)".to_string()
        } else {
            "RUNNING".to_string()
        },
    })
}

// Zero-param query
#[query(workflow = "my_workflow")]
fn get_count() -> Result<u64, String> {
    Ok(42)
}

// Multi-param query (two params)
#[query(workflow = "my_workflow")]
fn get_item(index: u32, prefix: String) -> Result<String, String> {
    Ok(format!("{prefix}:{index}"))
}

// ── Update handlers ───────────────────────────────────────────────────────────

// Update without validator
#[update(workflow = "my_workflow")]
async fn approve(req: ApproveRequest) -> Result<bool, String> {
    Ok(req.approved)
}

// Validator function referenced by the update macro
fn validate_approve(req: &serde_json::Value) -> Result<(), String> {
    if let Some(approved) = req.get("approved").and_then(|v| v.as_bool()) {
        if approved {
            Ok(())
        } else {
            Err("approval rejected".to_string())
        }
    } else {
        Err("missing approved field".to_string())
    }
}

// Update with validator
#[update(workflow = "my_workflow", validator = validate_approve)]
async fn approve_with_validator(req: ApproveRequest) -> Result<bool, String> {
    Ok(req.approved)
}

// ── Tests: companion info fields ──────────────────────────────────────────────

#[test]
fn query_companion_returns_correct_name() {
    let info = __autumn_query_handler_info_get_status();
    assert_eq!(info.name, "get_status");
}

#[test]
fn query_companion_returns_correct_workflow() {
    let info = __autumn_query_handler_info_get_status();
    assert_eq!(info.workflow, "my_workflow");
}

#[test]
fn query_companion_returns_type_hints() {
    let info = __autumn_query_handler_info_get_status();
    assert!(
        info.input_type_hint.contains("StatusRequest"),
        "input_type_hint was {:?}",
        info.input_type_hint
    );
    assert!(
        info.output_type_hint.contains("StatusResponse"),
        "output_type_hint was {:?}",
        info.output_type_hint
    );
}

#[test]
fn query_companion_zero_param_type_hint() {
    let info = __autumn_query_handler_info_get_count();
    assert_eq!(
        info.input_type_hint, "()",
        "zero-arg query should have input_type_hint '()'"
    );
    assert!(
        info.output_type_hint.contains("u64"),
        "output_type_hint was {:?}",
        info.output_type_hint
    );
}

#[test]
fn query_companion_multi_param_type_hint() {
    let info = __autumn_query_handler_info_get_item();
    assert!(
        info.input_type_hint.contains("u32") && info.input_type_hint.contains("String"),
        "input_type_hint for multi-param should include both types, got {:?}",
        info.input_type_hint
    );
}

#[test]
fn update_companion_returns_correct_name() {
    let info = __autumn_update_handler_info_approve();
    assert_eq!(info.name, "approve");
}

#[test]
fn update_companion_returns_correct_workflow() {
    let info = __autumn_update_handler_info_approve();
    assert_eq!(info.workflow, "my_workflow");
}

#[test]
fn update_companion_without_validator_flags() {
    let info = __autumn_update_handler_info_approve();
    assert!(!info.has_validator, "approve has no validator");
    assert!(info.validator.is_none(), "validator field must be None");
}

#[test]
fn update_companion_with_validator_flags() {
    let info = __autumn_update_handler_info_approve_with_validator();
    assert!(info.has_validator, "approve_with_validator has a validator");
    assert!(info.validator.is_some(), "validator field must be Some");
}

#[test]
fn update_companion_type_hints() {
    let info = __autumn_update_handler_info_approve();
    assert!(
        info.input_type_hint.contains("ApproveRequest"),
        "input_type_hint was {:?}",
        info.input_type_hint
    );
    assert!(
        info.output_type_hint.contains("bool"),
        "output_type_hint was {:?}",
        info.output_type_hint
    );
}

// ── Tests: collect macros ─────────────────────────────────────────────────────

#[test]
fn queries_macro_collects_correct_count_and_names() {
    let qs: Vec<QueryHandlerInfo> = queries![get_status, get_count, get_item];
    assert_eq!(qs.len(), 3);
    assert_eq!(qs[0].name, "get_status");
    assert_eq!(qs[1].name, "get_count");
    assert_eq!(qs[2].name, "get_item");
}

#[test]
fn updates_macro_collects_correct_count_and_names() {
    let us: Vec<UpdateHandlerInfo> = updates![approve, approve_with_validator];
    assert_eq!(us.len(), 2);
    assert_eq!(us[0].name, "approve");
    assert_eq!(us[1].name, "approve_with_validator");
}

// ── Tests: dispatch ───────────────────────────────────────────────────────────

#[test]
fn query_handler_dispatches_single_param() {
    let info = __autumn_query_handler_info_get_status();
    let result = (info.handler)(serde_json::json!({"verbose": false})).unwrap();
    assert_eq!(result, serde_json::json!({"status": "RUNNING"}));
}

#[test]
fn query_handler_dispatches_verbose_param() {
    let info = __autumn_query_handler_info_get_status();
    let result = (info.handler)(serde_json::json!({"verbose": true})).unwrap();
    assert_eq!(result, serde_json::json!({"status": "RUNNING (verbose)"}));
}

#[test]
fn query_handler_dispatches_zero_params() {
    let info = __autumn_query_handler_info_get_count();
    let result = (info.handler)(serde_json::Value::Null).unwrap();
    assert_eq!(result, serde_json::json!(42));
}

#[test]
fn query_handler_dispatches_multi_param() {
    let info = __autumn_query_handler_info_get_item();
    let result = (info.handler)(serde_json::json!([5, "item"])).unwrap();
    assert_eq!(result, serde_json::json!("item:5"));
}

#[tokio::test]
async fn update_handler_dispatches_without_validator() {
    let info = __autumn_update_handler_info_approve();
    let result = (info.handler)(serde_json::json!({"approved": true}))
        .await
        .unwrap();
    assert_eq!(result, serde_json::json!(true));
}

#[tokio::test]
async fn update_handler_with_validator_accept() {
    let info = __autumn_update_handler_info_approve_with_validator();

    let validator = info.validator.unwrap();
    assert!(
        validator(&serde_json::json!({"approved": true})).is_ok(),
        "valid input should pass"
    );

    let result = (info.handler)(serde_json::json!({"approved": true}))
        .await
        .unwrap();
    assert_eq!(result, serde_json::json!(true));
}

#[test]
fn update_handler_with_validator_reject() {
    let info = __autumn_update_handler_info_approve_with_validator();
    let validator = info.validator.unwrap();
    let err = validator(&serde_json::json!({"approved": false})).unwrap_err();
    assert_eq!(err, "approval rejected");
}

#[test]
fn update_handler_with_validator_missing_field_reject() {
    let info = __autumn_update_handler_info_approve_with_validator();
    let validator = info.validator.unwrap();
    let err = validator(&serde_json::json!({})).unwrap_err();
    assert_eq!(err, "missing approved field");
}

// ── Tests: auto-registration on WorkflowContext ───────────────────────────────

#[test]
fn query_handler_can_be_registered_on_context() {
    let info = __autumn_query_handler_info_get_status();
    let ctx = WorkflowContext::new_test();

    // Register the declarative handler using the convenience method
    ctx.register_declarative_query_handler(&info);

    // Verify it's accessible through the normal query dispatch path
    let result = ctx
        .execute_query_with_args("get_status", serde_json::json!({"verbose": false}))
        .unwrap();
    assert_eq!(result, serde_json::json!({"status": "RUNNING"}));
}

#[tokio::test]
async fn update_handler_can_be_registered_on_context() {
    let info = __autumn_update_handler_info_approve();
    let ctx = WorkflowContext::new_test();

    ctx.register_declarative_update_handler(&info);

    // Verify it's accessible through the normal update dispatch path
    let future = ctx
        .invoke_update("approve", serde_json::json!({"approved": true}))
        .unwrap();
    let result = future.await.unwrap();
    assert_eq!(result, serde_json::json!(true));
}

// ── Tests: backward compatibility ────────────────────────────────────────────

#[test]
fn bare_query_macro_still_passes_through() {
    // Bare #[query] (no workflow attribute) should still compile and work
    // as a documentation marker (no companion fn generated).

    #[query]
    fn my_old_style_query(req: &StatusRequest) -> Result<StatusResponse, String> {
        Ok(StatusResponse {
            status: if req.verbose { "verbose" } else { "normal" }.to_string(),
        })
    }

    // The function should compile and be callable normally
    let resp = my_old_style_query(&StatusRequest { verbose: false }).unwrap();
    assert_eq!(resp.status, "normal");
}
