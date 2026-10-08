//! Small parsing helpers shared across the `#[workflow(...)]`, `#[update(...)]`,
//! and sibling attribute-macro argument parsers.

/// Compile-time validator for the runtime `task_duration()` string format
/// (`"30s"`, `"5m"`, `"1h"`, `"1h30m"`, ...): digits followed by one of
/// `s`/`m`/`h`/`d`, optionally space-separated, with no overflow and no
/// trailing garbage. Mirrors (but does not call) the runtime parser in
/// `autumn-harvest/src/lib.rs::task_duration` so an invalid duration string
/// is rejected at compile time rather than silently accepted by the macro
/// and failing only when the workflow/DAG actually starts.
///
/// Shared by every attribute-macro argument parser that accepts a duration
/// string (`#[workflow(...)]`'s `start_to_close`/`heartbeat_timeout`/
/// `schedule_to_start`/`execution_timeout`, `#[dag(...)]`'s
/// `execution_timeout`/`sla`, ...) so the validation rule lives in exactly
/// one place.
pub fn is_valid_task_duration(s: &str) -> bool {
    let mut total_secs = 0u64;
    let mut current_num = String::new();
    for ch in s.chars() {
        if ch.is_ascii_digit() {
            if current_num == "0" {
                current_num.clear();
            }
            if current_num.len() > 20 {
                return false;
            }
            current_num.push(ch);
        } else if ch.is_ascii_alphabetic() {
            let Ok(num) = current_num.parse::<u64>() else {
                return false;
            };
            current_num.clear();
            let mult = match ch {
                's' => 1,
                'm' => 60,
                'h' => 3600,
                'd' => 86400,
                _ => return false,
            };
            match num
                .checked_mul(mult)
                .and_then(|v| total_secs.checked_add(v))
            {
                Some(v) => total_secs = v,
                None => return false,
            }
        } else if ch != ' ' {
            return false;
        }
    }
    current_num.is_empty() && total_secs != 0
}

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
