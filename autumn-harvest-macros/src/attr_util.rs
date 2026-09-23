//! Small parsing helpers shared across the `#[workflow(...)]`, `#[update(...)]`,
//! and sibling attribute-macro argument parsers.

use proc_macro2::TokenStream;
use quote::quote;

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

/// Best-effort Rust type name for a handler's non-context parameters.
///
/// Returns a bare type name for one parameter, or a parenthesized,
/// comma-joined tuple for several parameters.
///
/// `query.rs` and `update.rs` each hard-coded this exact derivation as
/// `build_input_type_hint`. `signal.rs` hard-coded the same body as
/// `build_arg_type_hint`. All three attribute macros publish the result on
/// their handler-info struct. Each field is documented with the same phrase:
/// "Best-effort Rust type name for the input" or "for the payload". Commit
/// `dfee5fce` (issue #346) added the first two copies together. Commit
/// `12b3ab24` (issue #610) copied the same body into `signal.rs` under a new
/// name when signal handlers gained the same discovery field.
pub fn arg_type_hint(params: &[&syn::FnArg]) -> String {
    if params.is_empty() {
        return "()".to_string();
    }
    if params.len() == 1
        && let syn::FnArg::Typed(pt) = params[0]
    {
        return crate::type_name_hint(&pt.ty);
    }
    let parts: Vec<_> = params
        .iter()
        .filter_map(|arg| {
            if let syn::FnArg::Typed(pt) = arg {
                Some(crate::type_name_hint(&pt.ty))
            } else {
                None
            }
        })
        .collect();
    format!("({})", parts.join(", "))
}

/// Decode a handler's non-`ctx` parameters by arity (0/1/N), invoke the
/// handler, and encode the `Ok` value to JSON, mapping the `Err` value
/// through `encode_err`.
///
/// `query.rs`'s `build_query_dispatch`, `update.rs`'s `build_update_dispatch`,
/// and the inline `dispatch` in `workflow.rs`/`activity.rs` each hand-mirrored
/// this exact three-arm body (issue #1632). `workflow.rs` and `activity.rs`
/// were byte-identical already; `query.rs`/`update.rs` differed from them and
/// from each other only in how the handler is invoked and how its error is
/// encoded, both of which vary by what the caller's own signature looks like,
/// not by which macro is calling.
///
/// - `args_ident`: the companion fn's JSON-value parameter (`args` for
///   query/update, `input` for workflow/activity).
/// - `multi_args_binding`: the local name the N-arity branch rebinds
///   `args_ident` to before indexing it. Kept separate from `args_ident`
///   because query/update already bind a parameter named `args`; rebinding
///   to `args` again would shadow it, so those two sites use `__args`, while
///   workflow/activity's parameter is named `input` and rebind to `args`.
/// - `ctx_expr`: how the caller passes its context (`ctx`, or update's
///   `ctx.as_ref()`).
/// - `await_tokens`: empty for query's sync handlers, `.await` elsewhere.
/// - `encode_err`: the handler-error encoder already computed by the caller
///   (`|e| e.to_string()` for query/update; a typed-failure encoder or the
///   same fallback for workflow/activity).
#[allow(clippy::too_many_arguments)]
pub fn build_handler_dispatch(
    fn_name: &syn::Ident,
    param_names: &[&syn::Ident],
    args_ident: &syn::Ident,
    multi_args_binding: &syn::Ident,
    ctx_expr: &TokenStream,
    await_tokens: &TokenStream,
    encode_err: &TokenStream,
) -> TokenStream {
    if param_names.is_empty() {
        quote! {
            let result = #fn_name(#ctx_expr) #await_tokens;
            result.map_err(#encode_err)
                .and_then(|v| {
                    ::autumn_harvest::serde_json::to_value(v).map_err(|e| e.to_string())
                })
        }
    } else if param_names.len() == 1 {
        let name = &param_names[0];
        quote! {
            let #name = ::autumn_harvest::serde_json::from_value(#args_ident)
                .map_err(|e| e.to_string())?;
            let result = #fn_name(#ctx_expr, #name) #await_tokens;
            result.map_err(#encode_err)
                .and_then(|v| {
                    ::autumn_harvest::serde_json::to_value(v).map_err(|e| e.to_string())
                })
        }
    } else {
        let indices = (0..param_names.len()).map(syn::Index::from);
        let names = param_names.to_owned();
        quote! {
            let #multi_args_binding: ::autumn_harvest::serde_json::Value = #args_ident;
            #(
                let #names = ::autumn_harvest::serde_json::from_value(#multi_args_binding[#indices].clone())
                    .map_err(|e| e.to_string())?;
            )*
            let result = #fn_name(#ctx_expr, #(#names),*) #await_tokens;
            result.map_err(#encode_err)
                .and_then(|v| {
                    ::autumn_harvest::serde_json::to_value(v).map_err(|e| e.to_string())
                })
        }
    }
}

#[cfg(test)]
mod arg_type_hint_tests {
    use super::arg_type_hint;

    /// Parses a bare parameter list into owned `syn::FnArg` values, mirroring
    /// how each macro slices `func.sig.inputs` after skipping `ctx`.
    fn params_from(sig: &str) -> Vec<syn::FnArg> {
        let f: syn::ItemFn = syn::parse_str(&format!("fn f({sig}) {{}}")).unwrap();
        f.sig.inputs.into_iter().collect()
    }

    #[test]
    fn no_params_hints_unit() {
        let owned = params_from("");
        let refs: Vec<_> = owned.iter().collect();
        assert_eq!(arg_type_hint(&refs), "()");
    }

    #[test]
    fn one_param_hints_the_bare_type_name() {
        let owned = params_from("x: String");
        let refs: Vec<_> = owned.iter().collect();
        assert_eq!(arg_type_hint(&refs), "String");
    }

    #[test]
    fn one_generic_param_hints_the_inner_type_too() {
        let owned = params_from("x: Option<String>");
        let refs: Vec<_> = owned.iter().collect();
        assert_eq!(arg_type_hint(&refs), "Option<String>");
    }

    #[test]
    fn multiple_params_hint_as_a_tuple() {
        let owned = params_from("a: u32, b: bool");
        let refs: Vec<_> = owned.iter().collect();
        assert_eq!(arg_type_hint(&refs), "(u32, bool)");
    }
}
