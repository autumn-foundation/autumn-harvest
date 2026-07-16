//! Integration tests for the `#[query]` and `#[update]` declarative macros (issue #346).
//!
//! Run with:
//!   `cargo test -p autumn-harvest --test macros_query_handlers --features testing`
// Test handler functions are constrained by the macro interface (must return
// Result, take args by value, etc.) so several pedantic lints don't apply here.
#![allow(
    clippy::doc_markdown,
    clippy::missing_const_for_fn,
    clippy::needless_pass_by_value,
    clippy::option_if_let_else,
    clippy::redundant_closure_for_method_calls,
    clippy::unnecessary_wraps,
    clippy::unused_async,
    clippy::used_underscore_binding
)]

use autumn_harvest::context::{WorkflowContext, empty_shared_state};
use autumn_harvest::prelude::*;
use autumn_harvest::types::ExecutionId;

#[workflow]
async fn my_workflow(_ctx: &WorkflowContext) -> Result<(), String> {
    Ok(())
}

#[workflow]
async fn state_wf(_ctx: &WorkflowContext) -> Result<(), String> {
    Ok(())
}

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
fn get_status(_ctx: &WorkflowContext, req: StatusRequest) -> Result<StatusResponse, String> {
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
fn get_count(_ctx: &WorkflowContext) -> Result<u64, String> {
    Ok(42)
}

// Multi-param query (two params)
#[query(workflow = "my_workflow")]
fn get_item(_ctx: &WorkflowContext, index: u32, prefix: String) -> Result<String, String> {
    Ok(format!("{prefix}:{index}"))
}

// ── Update handlers ───────────────────────────────────────────────────────────

// Update without validator
#[update(workflow = "my_workflow")]
async fn approve(_ctx: &WorkflowContext, req: ApproveRequest) -> Result<bool, String> {
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
async fn approve_with_validator(
    _ctx: &WorkflowContext,
    req: ApproveRequest,
) -> Result<bool, String> {
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
    let ctx = WorkflowContext::new_test();
    let result = (info.handler)(&ctx, serde_json::json!({"verbose": false})).unwrap();
    assert_eq!(result, serde_json::json!({"status": "RUNNING"}));
}

#[test]
fn query_handler_dispatches_verbose_param() {
    let info = __autumn_query_handler_info_get_status();
    let ctx = WorkflowContext::new_test();
    let result = (info.handler)(&ctx, serde_json::json!({"verbose": true})).unwrap();
    assert_eq!(result, serde_json::json!({"status": "RUNNING (verbose)"}));
}

#[test]
fn query_handler_dispatches_zero_params() {
    let info = __autumn_query_handler_info_get_count();
    let ctx = WorkflowContext::new_test();
    let result = (info.handler)(&ctx, serde_json::Value::Null).unwrap();
    assert_eq!(result, serde_json::json!(42));
}

#[test]
fn query_handler_dispatches_multi_param() {
    let info = __autumn_query_handler_info_get_item();
    let ctx = WorkflowContext::new_test();
    let result = (info.handler)(&ctx, serde_json::json!([5, "item"])).unwrap();
    assert_eq!(result, serde_json::json!("item:5"));
}

#[tokio::test]
async fn update_handler_dispatches_without_validator() {
    let info = __autumn_update_handler_info_approve();
    let ctx = WorkflowContext::new_for_handler(
        ExecutionId::new(),
        chrono::Utc::now(),
        None,
        empty_shared_state(),
    );
    let result = (info.handler)(ctx, serde_json::json!({"approved": true}))
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

    let ctx = WorkflowContext::new_for_handler(
        ExecutionId::new(),
        chrono::Utc::now(),
        None,
        empty_shared_state(),
    );
    let result = (info.handler)(ctx, serde_json::json!({"approved": true}))
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
    ctx.register_declarative_query_handler(&info);

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

    let future = ctx
        .invoke_update("approve", serde_json::json!({"approved": true}))
        .unwrap();
    let result = future.await.unwrap();
    assert_eq!(result, serde_json::json!(true));
}

// ── Tests: idempotent re-registration (replay safety) ────────────────────────

#[tokio::test]
async fn declarative_handlers_register_idempotently_on_multiple_replays() {
    let query_info = __autumn_query_handler_info_get_count();
    let update_info = __autumn_update_handler_info_approve();
    let ctx = WorkflowContext::new_test();

    // Simulate the executor calling registration on each replay cycle.
    for _ in 0..3 {
        ctx.register_declarative_query_handler(&query_info);
        ctx.register_declarative_update_handler(&update_info);
    }

    let q = ctx
        .execute_query_with_args("get_count", serde_json::Value::Null)
        .unwrap();
    assert_eq!(q, serde_json::json!(42));

    let u = ctx
        .invoke_update("approve", serde_json::json!({"approved": true}))
        .unwrap()
        .await
        .unwrap();
    assert_eq!(u, serde_json::json!(true));
}

// ── Tests: ctx state access ───────────────────────────────────────────────────

#[derive(Clone)]
struct MyCounter {
    count: u64,
}

#[query(workflow = "state_wf")]
fn read_counter(ctx: &WorkflowContext) -> Result<u64, String> {
    ctx.state::<MyCounter>()
        .map(|s| s.count)
        .ok_or_else(|| "no state".to_string())
}

#[test]
fn query_ctx_can_access_shared_state() {
    use autumn_harvest::context::SharedStateMap;
    use std::any::TypeId;

    let mut map = SharedStateMap::new();
    map.insert(TypeId::of::<MyCounter>(), Box::new(MyCounter { count: 77 }));
    let state = std::sync::Arc::new(map);

    let info = __autumn_query_handler_info_read_counter();
    let ctx = WorkflowContext::new_for_handler(ExecutionId::new(), chrono::Utc::now(), None, state);
    let result = (info.handler)(ctx.as_ref(), serde_json::Value::Null).unwrap();
    assert_eq!(result, serde_json::json!(77));
}

// ── Tests: HarvestBuilder integration ────────────────────────────────────────

#[test]
fn builder_queries_and_updates_methods_store_handlers() {
    use autumn_harvest::builder::HarvestBuilder;

    let built = HarvestBuilder::new()
        .queries(queries![get_status, get_count])
        .updates(updates![approve])
        .build();

    assert_eq!(built.query_handlers().len(), 2);
    assert_eq!(built.update_handlers().len(), 1);
    assert_eq!(built.query_handlers_for("my_workflow").len(), 2);
    assert_eq!(built.update_handlers_for("my_workflow").len(), 1);
    assert_eq!(built.query_handlers_for("other_workflow").len(), 0);
}

// ── Tests: backward compatibility ────────────────────────────────────────────

#[test]
fn bare_query_macro_still_passes_through() {
    #[query]
    fn my_old_style_query(req: &StatusRequest) -> Result<StatusResponse, String> {
        Ok(StatusResponse {
            status: if req.verbose { "verbose" } else { "normal" }.to_string(),
        })
    }

    let resp = my_old_style_query(&StatusRequest { verbose: false }).unwrap();
    assert_eq!(resp.status, "normal");
}

// ── Tests: Qualified absolute paths ────────────────────────────────────────────

pub mod flows {
    pub struct MyAbsoluteWfStub;
    impl MyAbsoluteWfStub {
        #[must_use]
        pub fn info() -> ::autumn_harvest::WorkflowInfo {
            ::autumn_harvest::WorkflowInfo {
                mcp: false,
                name: "my_absolute_wf",
                module: "flows",
                handler: |_, _| Box::pin(async { Ok(::autumn_harvest::serde_json::Value::Null) }),
                execution_timeout: None,
                sla: None,
                concurrency: None,

                debounce: None,
                batch: None,
                throttle: None,
                max_input_bytes: None,
                owner: None,
                runbook_url: None,
                severity: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
                retry_policy: None,
            }
        }
    }
}

#[query(workflow = "crate::macros_query_handlers::flows::my_absolute_wf")]
fn query_absolute(_ctx: &WorkflowContext) -> Result<bool, String> {
    Ok(true)
}

#[update(workflow = "crate::macros_query_handlers::flows::my_absolute_wf")]
async fn update_absolute(_ctx: &WorkflowContext) -> Result<bool, String> {
    Ok(true)
}

#[test]
fn absolute_workflow_paths_compile_successfully() {
    let q_info = __autumn_query_handler_info_query_absolute();
    assert_eq!(q_info.name, "query_absolute");
    assert_eq!(q_info.workflow, "my_absolute_wf");

    let u_info = __autumn_update_handler_info_update_absolute();
    assert_eq!(u_info.name, "update_absolute");
    assert_eq!(u_info.workflow, "my_absolute_wf");
}

// ── Tests: Relative paths (self::, super::, and plain relative) ────────────────

pub mod relative_test_self {
    use super::*;

    pub mod flows {
        pub struct MyRelativeWfStub;
        impl MyRelativeWfStub {
            #[must_use]
            pub fn info() -> ::autumn_harvest::WorkflowInfo {
                ::autumn_harvest::WorkflowInfo {
                    mcp: false,
                    name: "my_relative_wf",
                    module: "relative_test_self::flows",
                    handler: |_, _| {
                        Box::pin(async { Ok(::autumn_harvest::serde_json::Value::Null) })
                    },
                    execution_timeout: None,
                    sla: None,
                    concurrency: None,

                    debounce: None,
                    batch: None,
                    throttle: None,
                    max_input_bytes: None,
                    owner: None,
                    runbook_url: None,
                    severity: None,
                    description: None,
                    input_schema: None,
                    output_schema: None,
                    error_schema: None,
                    retry_policy: None,
                }
            }
        }
    }

    #[query(workflow = "self::flows::my_relative_wf")]
    fn query_relative_self(_ctx: &WorkflowContext) -> Result<bool, String> {
        Ok(true)
    }

    #[update(workflow = "self::flows::my_relative_wf")]
    async fn update_relative_self(_ctx: &WorkflowContext) -> Result<bool, String> {
        Ok(true)
    }
}

pub mod relative_test_super {
    use super::*;

    pub mod flows {
        pub struct MyRelativeWfStub;
        impl MyRelativeWfStub {
            #[must_use]
            pub fn info() -> ::autumn_harvest::WorkflowInfo {
                ::autumn_harvest::WorkflowInfo {
                    mcp: false,
                    name: "my_relative_wf",
                    module: "relative_test_super::flows",
                    handler: |_, _| {
                        Box::pin(async { Ok(::autumn_harvest::serde_json::Value::Null) })
                    },
                    execution_timeout: None,
                    sla: None,
                    concurrency: None,

                    debounce: None,
                    batch: None,
                    throttle: None,
                    max_input_bytes: None,
                    owner: None,
                    runbook_url: None,
                    severity: None,
                    description: None,
                    input_schema: None,
                    output_schema: None,
                    error_schema: None,
                    retry_policy: None,
                }
            }
        }
    }

    pub mod nested_child {
        use super::*;

        #[query(workflow = "super::flows::my_relative_wf")]
        fn query_relative_super(_ctx: &WorkflowContext) -> Result<bool, String> {
            Ok(true)
        }

        #[update(workflow = "super::flows::my_relative_wf")]
        async fn update_relative_super(_ctx: &WorkflowContext) -> Result<bool, String> {
            Ok(true)
        }
    }
}

pub mod relative_test_plain {
    use super::*;

    pub mod flows {
        pub struct MyRelativeWfStub;
        impl MyRelativeWfStub {
            #[must_use]
            pub fn info() -> ::autumn_harvest::WorkflowInfo {
                ::autumn_harvest::WorkflowInfo {
                    mcp: false,
                    name: "my_relative_wf",
                    module: "relative_test_plain::flows",
                    handler: |_, _| {
                        Box::pin(async { Ok(::autumn_harvest::serde_json::Value::Null) })
                    },
                    execution_timeout: None,
                    sla: None,
                    concurrency: None,

                    debounce: None,
                    batch: None,
                    throttle: None,
                    max_input_bytes: None,
                    owner: None,
                    runbook_url: None,
                    severity: None,
                    description: None,
                    input_schema: None,
                    output_schema: None,
                    error_schema: None,
                    retry_policy: None,
                }
            }
        }
    }

    #[query(workflow = "flows::my_relative_wf")]
    fn query_relative_plain(_ctx: &WorkflowContext) -> Result<bool, String> {
        Ok(true)
    }

    #[update(workflow = "flows::my_relative_wf")]
    async fn update_relative_plain(_ctx: &WorkflowContext) -> Result<bool, String> {
        Ok(true)
    }
}

#[test]
fn relative_workflow_paths_compile_and_resolve_successfully() {
    let q_self = relative_test_self::__autumn_query_handler_info_query_relative_self();
    assert_eq!(q_self.name, "query_relative_self");
    assert_eq!(q_self.workflow, "my_relative_wf");

    let u_self = relative_test_self::__autumn_update_handler_info_update_relative_self();
    assert_eq!(u_self.name, "update_relative_self");
    assert_eq!(u_self.workflow, "my_relative_wf");

    let q_super =
        relative_test_super::nested_child::__autumn_query_handler_info_query_relative_super();
    assert_eq!(q_super.name, "query_relative_super");
    assert_eq!(q_super.workflow, "my_relative_wf");

    let u_super =
        relative_test_super::nested_child::__autumn_update_handler_info_update_relative_super();
    assert_eq!(u_super.name, "update_relative_super");
    assert_eq!(u_super.workflow, "my_relative_wf");

    let q_plain = relative_test_plain::__autumn_query_handler_info_query_relative_plain();
    assert_eq!(q_plain.name, "query_relative_plain");
    assert_eq!(q_plain.workflow, "my_relative_wf");

    let u_plain = relative_test_plain::__autumn_update_handler_info_update_relative_plain();
    assert_eq!(u_plain.name, "update_relative_plain");
    assert_eq!(u_plain.workflow, "my_relative_wf");
}

// ── MCP tool exposure opt-in for updates (issue #597) ─────────────────────────

#[update(workflow = "my_workflow", mcp)]
async fn mcp_bare_update(_ctx: &WorkflowContext, req: ApproveRequest) -> Result<bool, String> {
    Ok(req.approved)
}

#[update(workflow = "my_workflow", mcp = true)]
async fn mcp_eq_true_update(_ctx: &WorkflowContext, req: ApproveRequest) -> Result<bool, String> {
    Ok(req.approved)
}

#[update(workflow = "my_workflow", mcp = false)]
async fn mcp_eq_false_update(_ctx: &WorkflowContext, req: ApproveRequest) -> Result<bool, String> {
    Ok(req.approved)
}

#[test]
fn update_mcp_bare_flag_sets_true() {
    let info = __autumn_update_handler_info_mcp_bare_update();
    assert!(
        info.mcp,
        "#[update(..., mcp)] must set UpdateHandlerInfo.mcp = true"
    );
}

#[test]
fn update_mcp_eq_true_sets_true() {
    let info = __autumn_update_handler_info_mcp_eq_true_update();
    assert!(info.mcp);
}

#[test]
fn update_mcp_eq_false_sets_false() {
    let info = __autumn_update_handler_info_mcp_eq_false_update();
    assert!(!info.mcp);
}

#[test]
fn update_mcp_defaults_to_false() {
    let info = __autumn_update_handler_info_approve();
    assert!(
        !info.mcp,
        "updates without the mcp attribute must default to mcp = false"
    );
}

#[test]
fn update_mcp_composes_with_validator() {
    let info = __autumn_update_handler_info_approve_with_validator();
    assert!(!info.mcp);
    assert!(info.has_validator);
}

// ── issue #610: interface schema — description attr, signal companion, aliases ─

#[query(workflow = "my_workflow", description = "read the current status")]
fn described_query(_ctx: &WorkflowContext) -> Result<u64, String> {
    Ok(1)
}

#[update(workflow = "my_workflow", description = "approve the order")]
async fn described_update(_ctx: &WorkflowContext, req: ApproveRequest) -> Result<bool, String> {
    Ok(req.approved)
}

#[signal(workflow = "my_workflow", description = "cancel the order")]
async fn cancel_order(_ctx: &WorkflowContext, _req: ApproveRequest) -> Result<(), String> {
    Ok(())
}

#[signal(workflow = "my_workflow")]
async fn pause_order(_ctx: &WorkflowContext, _amount: u32) -> Result<(), String> {
    Ok(())
}

#[test]
fn query_description_attr_threads_through() {
    let info = __autumn_query_handler_info_described_query();
    assert_eq!(info.description, Some("read the current status"));
    assert!(info.arg_schema.is_none());
    assert!(info.response_schema.is_none());
}

#[test]
fn update_description_attr_threads_through() {
    let info = __autumn_update_handler_info_described_update();
    assert_eq!(info.description, Some("approve the order"));
    assert!(info.arg_schema.is_none());
}

#[test]
fn query_public_info_alias_exists() {
    // Chaining a schema builder at registration must compile & work.
    fn arg() -> serde_json::Value {
        serde_json::json!({"type": "null"})
    }
    let info = described_query_info();
    assert_eq!(info.name, "described_query");
    let info = described_query_info().with_arg_schema_fn(arg);
    assert!(info.arg_schema.is_some());
}

#[test]
fn update_public_info_alias_exists() {
    let info = described_update_info();
    assert_eq!(info.name, "described_update");
}

#[test]
fn signal_companion_returns_signal_handler_info() {
    let info = __autumn_signal_handler_info_cancel_order();
    assert_eq!(info.name, "cancel_order");
    assert_eq!(info.workflow, "my_workflow");
    assert_eq!(info.description, Some("cancel the order"));
    assert!(
        info.arg_type_hint.contains("ApproveRequest"),
        "arg_type_hint was {:?}",
        info.arg_type_hint
    );
    assert!(info.arg_schema.is_none());
}

#[test]
fn signal_public_info_alias_exists() {
    let info: SignalHandlerInfo = cancel_order_info();
    assert_eq!(info.name, "cancel_order");
}

#[test]
fn signal_without_description_defaults_none() {
    let info = pause_order_info();
    assert_eq!(info.name, "pause_order");
    assert!(info.description.is_none());
    assert!(
        info.arg_type_hint.contains("u32"),
        "arg_type_hint was {:?}",
        info.arg_type_hint
    );
}

#[test]
fn signals_macro_collects_correct_count_and_names() {
    let ss: Vec<SignalHandlerInfo> = signals![cancel_order, pause_order];
    assert_eq!(ss.len(), 2);
    assert_eq!(ss[0].name, "cancel_order");
    assert_eq!(ss[1].name, "pause_order");
}

#[test]
fn builder_signals_method_stores_handlers() {
    use autumn_harvest::builder::HarvestBuilder;

    let built = HarvestBuilder::new()
        .signals(signals![cancel_order, pause_order])
        .build();
    assert_eq!(built.signal_handlers().len(), 2);
}
