#![allow(clippy::unused_async, clippy::used_underscore_binding)]

use autumn_harvest::prelude::*;

#[workflow]
async fn test_workflow(_ctx: &WorkflowContext, _input: String) -> Result<String, String> {
    Ok("done".into())
}

#[workflow(execution_timeout = "24h")]
async fn billing_reconciliation(_ctx: &WorkflowContext, _run_date: String) -> Result<(), String> {
    Ok(())
}

#[workflow(execution_timeout = "30m")]
async fn quick_workflow(_ctx: &WorkflowContext) -> Result<(), String> {
    Ok(())
}

#[workflow(concurrency(key = "input.doc_id", limit = 1, on_conflict = "cancel_running"))]
async fn latest_wins_workflow(_ctx: &WorkflowContext, _input: String) -> Result<String, String> {
    Ok(String::new())
}

#[workflow(concurrency(key = "input.doc_id", limit = 3, on_conflict = "defer"))]
async fn explicit_defer_workflow(_ctx: &WorkflowContext, _input: String) -> Result<String, String> {
    Ok(String::new())
}

#[workflow(concurrency(key = "input.tenant_id", limit = 10))]
async fn concurrency_workflow(_ctx: &WorkflowContext, _input: String) -> Result<String, String> {
    Ok("done".into())
}

// Issue #617: chain-scoped lifetime cap attribute + per-run execution_timeout.
#[workflow(execution_timeout = "30m", chain_execution_timeout = "7d")]
async fn long_poller(_ctx: &WorkflowContext) -> Result<(), String> {
    Ok(())
}

#[test]
fn workflow_chain_execution_timeout_attribute_parses(/* issue #617 */) {
    let info = __autumn_workflow_info_long_poller();
    assert_eq!(info.name, "long_poller");
    // The per-run and chain caps are independent.
    assert_eq!(
        info.execution_timeout,
        Some(std::time::Duration::from_secs(1_800)),
        "execution_timeout = '30m'"
    );
    let chain = info
        .chain_execution_timeout
        .expect("chain_execution_timeout = '7d' must produce Some(...)");
    assert_eq!(
        chain,
        std::time::Duration::from_secs(7 * 86_400),
        "7d = 604800 seconds"
    );
}

#[test]
fn workflow_without_chain_attribute_has_none() {
    let info = __autumn_workflow_info_test_workflow();
    assert!(
        info.chain_execution_timeout.is_none(),
        "no chain_execution_timeout attribute → None (issue #617)"
    );
}

#[test]
fn workflow_info_with_chain_execution_timeout_builder_sets_field() {
    // Issue #617: fluent builder parity with `with_execution_timeout`.
    let info = __autumn_workflow_info_test_workflow()
        .with_chain_execution_timeout(std::time::Duration::from_secs(3_600));
    assert_eq!(
        info.chain_execution_timeout,
        Some(std::time::Duration::from_secs(3_600))
    );
}

#[test]
fn workflow_companion_exists_and_returns_info() {
    let info = __autumn_workflow_info_test_workflow();
    assert_eq!(info.name, "test_workflow");
    assert!(
        info.execution_timeout.is_none(),
        "no execution_timeout attribute → None"
    );
    assert!(
        info.concurrency.is_none(),
        "plain workflow should have no concurrency policy"
    );
}

#[test]
fn workflow_execution_timeout_attribute_24h() {
    let info = __autumn_workflow_info_billing_reconciliation();
    assert_eq!(info.name, "billing_reconciliation");
    let timeout = info
        .execution_timeout
        .expect("execution_timeout = '24h' must produce Some(...)");
    assert_eq!(
        timeout,
        std::time::Duration::from_secs(86_400),
        "24h = 86400 seconds"
    );
}

#[test]
fn workflow_execution_timeout_attribute_30m() {
    let info = __autumn_workflow_info_quick_workflow();
    assert_eq!(info.name, "quick_workflow");
    let timeout = info
        .execution_timeout
        .expect("execution_timeout = '30m' must produce Some(...)");
    assert_eq!(
        timeout,
        std::time::Duration::from_secs(1_800),
        "30m = 1800 seconds"
    );
}

#[test]
fn workflow_concurrency_macro_sets_policy() {
    let info = __autumn_workflow_info_concurrency_workflow();
    assert_eq!(info.name, "concurrency_workflow");
    let policy = info
        .concurrency
        .expect("concurrency workflow must have a policy");
    assert_eq!(policy.key_expr, "input.tenant_id");
    assert_eq!(policy.limit, 10);
    // Issue #811 AC1: omitting `on_conflict` keeps today's defer behaviour.
    assert_eq!(
        policy.on_conflict,
        autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
        "a policy that omits on_conflict must default to Defer, so every \
         pre-#811 workflow is byte-for-byte unchanged"
    );
}

#[test]
fn workflow_concurrency_macro_parses_cancel_running() {
    // Issue #811 AC1: the attribute parses and attaches the latest-wins
    // strategy to the registered `WorkflowInfo`.
    let info = __autumn_workflow_info_latest_wins_workflow();
    assert_eq!(info.name, "latest_wins_workflow");
    let policy = info
        .concurrency
        .expect("latest-wins workflow must have a policy");
    assert_eq!(policy.key_expr, "input.doc_id");
    assert_eq!(policy.limit, 1);
    assert_eq!(
        policy.on_conflict,
        autumn_harvest::concurrency::ConcurrencyOnConflict::CancelRunning
    );
    assert!(policy.on_conflict.is_cancel_running());
}

#[test]
fn workflow_concurrency_macro_parses_explicit_defer() {
    // Spelling `on_conflict = "defer"` explicitly must resolve to exactly the
    // same policy as omitting it — the attribute is additive, never a mode
    // switch that changes the other fields' meaning.
    let info = __autumn_workflow_info_explicit_defer_workflow();
    let policy = info
        .concurrency
        .expect("explicit-defer workflow must have a policy");
    assert_eq!(policy.key_expr, "input.doc_id");
    assert_eq!(policy.limit, 3);
    assert_eq!(
        policy.on_conflict,
        autumn_harvest::concurrency::ConcurrencyOnConflict::Defer
    );
    assert!(!policy.on_conflict.is_cancel_running());
}

#[workflow(
    owner = "billing",
    runbook = "https://wiki.acme.com/runbook",
    severity = "sev2"
)]
async fn metadata_workflow(_ctx: &WorkflowContext) -> Result<(), String> {
    Ok(())
}

#[test]
fn workflow_metadata_attributes() {
    let info = __autumn_workflow_info_metadata_workflow();
    assert_eq!(info.owner, Some("billing"));
    assert_eq!(info.runbook_url, Some("https://wiki.acme.com/runbook"));
    assert_eq!(info.severity, Some("sev2"));
}

// ── MCP tool exposure opt-in (issue #597) ─────────────────────────────────────

#[workflow(mcp)]
async fn mcp_bare_workflow(_ctx: &WorkflowContext, _input: String) -> Result<String, String> {
    Ok("done".into())
}

#[workflow(mcp = true)]
async fn mcp_eq_true_workflow(_ctx: &WorkflowContext) -> Result<(), String> {
    Ok(())
}

#[workflow(mcp = false)]
async fn mcp_eq_false_workflow(_ctx: &WorkflowContext) -> Result<(), String> {
    Ok(())
}

#[workflow(mcp, description = "an MCP-exposed workflow")]
async fn mcp_with_description_workflow(_ctx: &WorkflowContext) -> Result<(), String> {
    Ok(())
}

#[test]
fn workflow_mcp_bare_flag_sets_true() {
    let info = __autumn_workflow_info_mcp_bare_workflow();
    assert!(
        info.mcp,
        "#[workflow(mcp)] must set WorkflowInfo.mcp = true"
    );
}

#[test]
fn workflow_mcp_eq_true_sets_true() {
    let info = __autumn_workflow_info_mcp_eq_true_workflow();
    assert!(info.mcp, "#[workflow(mcp = true)] must set mcp = true");
}

#[test]
fn workflow_mcp_eq_false_sets_false() {
    let info = __autumn_workflow_info_mcp_eq_false_workflow();
    assert!(!info.mcp, "#[workflow(mcp = false)] must set mcp = false");
}

#[test]
fn workflow_mcp_defaults_to_false() {
    let info = __autumn_workflow_info_test_workflow();
    assert!(
        !info.mcp,
        "workflows without the mcp attribute must default to mcp = false"
    );
}

#[test]
fn workflow_mcp_composes_with_other_attributes() {
    let info = __autumn_workflow_info_mcp_with_description_workflow();
    assert!(info.mcp);
    assert_eq!(info.description, Some("an MCP-exposed workflow"));
}

// ── typed workflow failures (issue #767) ──────────────────────────────────────

#[cfg(feature = "testing")]
#[workflow]
async fn wf_typed_fail(_ctx: &WorkflowContext, _in: ()) -> Result<(), WorkflowFailure> {
    Err(WorkflowFailure::new("ValidationRejected", "bad").non_retryable())
}

#[cfg(feature = "testing")]
#[workflow]
async fn wf_string_fail(_ctx: &WorkflowContext, _in: ()) -> Result<(), String> {
    Err("boom".into())
}

/// A custom error enum (AC1): a workflow returning `Result<_, MyErr>` must NOT
/// be false-detected as a `WorkflowFailure` — it routes through `.to_string()`
/// and stays untyped.
#[cfg(feature = "testing")]
#[derive(Debug)]
enum MyErr {
    Boom,
}

#[cfg(feature = "testing")]
impl std::fmt::Display for MyErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Boom => write!(f, "boom-enum"),
        }
    }
}

#[cfg(feature = "testing")]
#[workflow]
async fn wf_enum_fail(_ctx: &WorkflowContext, _in: ()) -> Result<(), MyErr> {
    Err(MyErr::Boom)
}

#[cfg(feature = "testing")]
#[test]
fn workflow_typed_failure_encodes_wire_envelope() {
    let info = __autumn_workflow_info_wf_typed_fail();
    let ctx = WorkflowContext::new_test();
    let result = futures::executor::block_on((info.handler)(
        &ctx,
        autumn_harvest::serde_json::Value::Null,
    ));
    let payload = result.expect_err("workflow returned Err");
    let decoded = autumn_harvest::failure::decode_workflow_failure(&payload);
    assert_eq!(decoded.error_type, Some("ValidationRejected".to_string()));
    assert_eq!(decoded.non_retryable, Some(true));
    assert_eq!(decoded.message, "bad");
}

#[cfg(feature = "testing")]
#[test]
fn workflow_string_failure_stays_untyped() {
    let info = __autumn_workflow_info_wf_string_fail();
    let ctx = WorkflowContext::new_test();
    let result = futures::executor::block_on((info.handler)(
        &ctx,
        autumn_harvest::serde_json::Value::Null,
    ));
    let payload = result.expect_err("workflow returned Err");
    assert_eq!(payload, "boom");
    let decoded = autumn_harvest::failure::decode_workflow_failure(&payload);
    assert!(decoded.error_type.is_none());
    assert_eq!(decoded.message, "boom");
}

#[cfg(feature = "testing")]
#[test]
fn workflow_custom_enum_failure_stays_untyped() {
    // AC1: an `Err(MyEnum)` must route through `.to_string()` and stay untyped —
    // the macro's `WorkflowFailure` detector must not false-positive on a custom
    // enum type.
    let info = __autumn_workflow_info_wf_enum_fail();
    let ctx = WorkflowContext::new_test();
    let result = futures::executor::block_on((info.handler)(
        &ctx,
        autumn_harvest::serde_json::Value::Null,
    ));
    let payload = result.expect_err("workflow returned Err");
    assert_eq!(payload, MyErr::Boom.to_string());
    assert!(
        autumn_harvest::failure::decode_workflow_failure(&payload)
            .error_type
            .is_none()
    );
}

// ── Issue #802 — opt-in declared activity/child dependencies ─────────────────
//
// The declaration is recorded verbatim as registration metadata; it is the
// *preflight* check (`check_catalog_consistency`) that resolves each name
// against the registry. The macro deliberately does NOT resolve the listed
// identifiers to their companion `_info()` symbols: a bare ident in an
// attribute is never name-resolved by the compiler, so a typo stays a
// preflight failure (the exact failure mode AC2 specifies) rather than
// becoming a compile error, and the author needs no extra `use` in scope.

#[activity]
#[allow(clippy::unused_async)]
async fn dep_send_email(_ctx: &ActivityContext, addr: String) -> Result<String, String> {
    Ok(addr)
}

#[workflow]
#[allow(clippy::unused_async)]
async fn dep_generate_report(_ctx: &WorkflowContext, _input: ()) -> Result<(), String> {
    Ok(())
}

#[workflow(
    activities = [dep_send_email, charge_card],
    children = [dep_generate_report]
)]
#[allow(clippy::unused_async)]
async fn onboarding_declared(_ctx: &WorkflowContext, _input: String) -> Result<(), String> {
    Ok(())
}

// Every path qualifier form must parse. `crate` / `self` / `super` are
// KEYWORDS, not `Ident`s, so a peek-based guard would reject them before
// `syn::Path` ever saw the token — each is covered here deliberately.
#[workflow(activities = [
    crate::billing::charge_card,
    self::sibling_activity,
    super::parent_activity,
    ::root_crate::rooted_activity,
    bare_ident_activity,
    "raw_dispatched_name",
])]
#[allow(clippy::unused_async)]
async fn path_and_literal_declared(_ctx: &WorkflowContext) -> Result<(), String> {
    Ok(())
}

#[workflow(activities = [], children = [])]
#[allow(clippy::unused_async)]
async fn explicitly_declares_nothing(_ctx: &WorkflowContext) -> Result<(), String> {
    Ok(())
}

#[test]
fn workflow_declared_dependencies_attribute_parses(/* issue #802 AC1 */) {
    let info = __autumn_workflow_info_onboarding_declared();
    assert_eq!(info.name, "onboarding_declared");
    assert_eq!(
        info.declared_activities,
        Some(["dep_send_email", "charge_card"].as_slice()),
        "activities = [..] must be recorded in declaration order"
    );
    assert_eq!(
        info.declared_children,
        Some(["dep_generate_report"].as_slice()),
        "children = [..] must be recorded in declaration order"
    );
}

#[test]
fn workflow_declared_dependencies_accept_paths_and_string_literals() {
    // A path records its LAST segment (the registered name is always the fn
    // ident — neither macro has a `name = "..."` rename attribute), and a
    // string literal is accepted verbatim so a raw-dispatched name
    // (`ctx.execute_activity_raw("raw_dispatched_name", ..)`) can be declared
    // even though it is not an identifier in scope.
    let info = __autumn_workflow_info_path_and_literal_declared();
    assert_eq!(
        info.declared_activities,
        Some(
            [
                "charge_card",         // crate::billing::charge_card
                "sibling_activity",    // self::sibling_activity
                "parent_activity",     // super::parent_activity
                "rooted_activity",     // ::root_crate::rooted_activity
                "bare_ident_activity", // bare ident
                "raw_dispatched_name", // string literal
            ]
            .as_slice()
        ),
        "every path qualifier form must reduce to its last segment, and a \
         string literal must pass through verbatim"
    );
    assert!(info.declared_children.is_none());
}

#[test]
fn workflow_without_dependency_attributes_has_none(/* issue #802 AC4 */) {
    // The zero-false-positive guarantee is structural: a workflow that never
    // opts in carries `None`, which preflight skips entirely.
    let info = __autumn_workflow_info_test_workflow();
    assert!(info.declared_activities.is_none());
    assert!(info.declared_children.is_none());
}

#[test]
fn workflow_empty_declaration_is_opt_in_not_absent() {
    // `Some(&[])` ("I depend on nothing") is a deliberately different state
    // from `None` ("I did not opt in"), so an author can assert emptiness.
    let info = __autumn_workflow_info_explicitly_declares_nothing();
    assert_eq!(info.declared_activities, Some([].as_slice()));
    assert_eq!(info.declared_children, Some([].as_slice()));
}

#[test]
fn workflow_info_declared_dependency_builders_set_fields() {
    // Fluent parity for programmatic (non-macro) registration.
    let info = __autumn_workflow_info_test_workflow()
        .with_declared_activities(&["send_email"])
        .with_declared_children(&["generate_report"]);
    assert_eq!(info.declared_activities, Some(["send_email"].as_slice()));
    assert_eq!(info.declared_children, Some(["generate_report"].as_slice()));
}
