//! Small parsing helpers shared across the `#[workflow(...)]`, `#[update(...)]`,
//! and sibling attribute-macro argument parsers.

/// Parse a bare-flag-or-explicit-bool attribute value: `name` (bare, implies
/// `true`) or `name = true`/`name = false`.
///
/// Used by every `#[workflow(...)]`/`#[update(...)]` boolean attribute
/// (`mcp`, `allow_nondeterministic_apis`, ...) so the bare-vs-explicit
/// parsing rule lives in exactly one place.
pub fn parse_bool_flag(meta: &syn::meta::ParseNestedMeta<'_>) -> syn::Result<bool> {
    if meta.input.peek(syn::Token![=]) {
        let value: syn::LitBool = meta.value()?.parse()?;
        Ok(value.value)
    } else {
        Ok(true)
    }
}

/// Returns `true` when the first parameter is a `&Expected` reference, for
/// whatever context-type name `expected_ident` names (e.g. `"WorkflowContext"`,
/// `"WebhookCtx"`).
///
/// `query.rs`/`update.rs`/`signal.rs` each hard-code their own copy of this
/// check against a fixed `"WorkflowContext"` ident; this generalized version
/// exists so newer macros (`webhook.rs`) don't add yet another near-identical
/// copy. The three pre-existing hard-coded copies are left as-is to keep this
/// change scoped.
pub fn first_param_is_ctx_type(
    inputs: &syn::punctuated::Punctuated<syn::FnArg, syn::token::Comma>,
    expected_ident: &str,
) -> bool {
    let Some(first) = inputs.first() else {
        return false;
    };
    let syn::FnArg::Typed(pt) = first else {
        return false;
    };
    let syn::Type::Reference(r) = &*pt.ty else {
        return false;
    };
    let syn::Type::Path(tp) = &*r.elem else {
        return false;
    };
    tp.path
        .segments
        .last()
        .is_some_and(|s| s.ident == expected_ident)
}

/// Returns `true` when the return type's last path segment is `Result`.
pub fn returns_result(output: &syn::ReturnType) -> bool {
    let syn::ReturnType::Type(_, ty) = output else {
        return false;
    };
    let syn::Type::Path(type_path) = &**ty else {
        return false;
    };
    type_path
        .path
        .segments
        .last()
        .is_some_and(|s| s.ident == "Result")
}
