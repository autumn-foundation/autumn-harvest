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

/// Identifiers of every parameter, dropping the leading `ctx` and any
/// parameter whose pattern is not a bare identifier.
///
/// `activity.rs`, `query.rs`, `signal.rs`, `update.rs`, and `workflow.rs`
/// each hard-coded this exact filter to build the args their generated
/// dispatch code deserializes into. All five already call
/// [`arg_type_hint`] on the same `params` slice this takes, so the two
/// helpers share one calling convention.
pub fn param_idents<'a>(params: &'a [&syn::FnArg]) -> Vec<&'a syn::Ident> {
    params
        .iter()
        .filter_map(|arg| {
            if let syn::FnArg::Typed(pt) = arg
                && let syn::Pat::Ident(ident) = &*pt.pat
            {
                return Some(&ident.ident);
            }
            None
        })
        .collect()
}

#[cfg(test)]
mod param_idents_tests {
    use super::param_idents;

    fn params_from(sig: &str) -> Vec<syn::FnArg> {
        let f: syn::ItemFn = syn::parse_str(&format!("fn f({sig}) {{}}")).unwrap();
        f.sig.inputs.into_iter().collect()
    }

    #[test]
    fn no_params_yields_no_idents() {
        let owned = params_from("");
        let refs: Vec<_> = owned.iter().collect();
        assert_eq!(param_idents(&refs), Vec::<&syn::Ident>::new());
    }

    #[test]
    fn typed_ident_params_are_collected_in_order() {
        let owned = params_from("a: u32, b: bool");
        let refs: Vec<_> = owned.iter().collect();
        let names: Vec<String> = param_idents(&refs).iter().map(ToString::to_string).collect();
        assert_eq!(names, vec!["a", "b"]);
    }

    /// A non-ident pattern (destructuring, `_`) is silently dropped, not an
    /// error. This matches every pre-extraction copy's behavior exactly --
    /// none of the five rejected such a parameter at macro-expansion time.
    #[test]
    fn non_ident_pattern_is_silently_dropped() {
        let owned = params_from("(a, b): (u32, u32), c: bool");
        let refs: Vec<_> = owned.iter().collect();
        let names: Vec<String> = param_idents(&refs).iter().map(ToString::to_string).collect();
        assert_eq!(names, vec!["c"]);
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
