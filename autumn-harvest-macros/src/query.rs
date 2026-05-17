//! `#[query]` attribute macro for declarative read-only workflow query handlers.
//!
//! **No `workflow` attribute** (bare `#[query]`): pass-through marker, identical
//! to the pre-issue-#346 behaviour. Register the handler imperatively via
//! `WorkflowContext::register_query_handler` inside the workflow body.
//!
//! **With `workflow = "name"`**: generates a companion function
//! `__autumn_query_handler_info_{fn_name}() -> QueryHandlerInfo`. The function
//! must be synchronous, return `Result<T, E>`, and take `ctx: &WorkflowContext`
//! as its first argument.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{ItemFn, LitStr, parse::Parser as _};

struct QueryAttrs {
    workflow: Option<String>,
}

fn parse_attrs(attr: TokenStream) -> syn::Result<QueryAttrs> {
    let mut result = QueryAttrs { workflow: None };

    if attr.is_empty() {
        return Ok(result);
    }

    syn::meta::parser(|meta| {
        if meta.path.is_ident("workflow") {
            let value: LitStr = meta.value()?.parse()?;
            result.workflow = Some(value.value());
            Ok(())
        } else {
            Err(meta.error("unsupported attribute: expected `workflow = \"workflow_name\"`"))
        }
    })
    .parse2(attr)?;

    Ok(result)
}

pub fn query_macro(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attrs = match parse_attrs(attr) {
        Ok(a) => a,
        Err(e) => return e.to_compile_error(),
    };

    let func: ItemFn = match syn::parse2(item) {
        Ok(f) => f,
        Err(e) => return e.to_compile_error(),
    };

    // Without `workflow = "..."`: pass-through marker (backward compat).
    let Some(workflow_name) = attrs.workflow else {
        return quote! { #func };
    };

    // ── Validation ────────────────────────────────────────────────────────────

    if func.sig.asyncness.is_some() {
        return syn::Error::new_spanned(
            func.sig.fn_token,
            "#[query] handlers must be synchronous (async fn is not supported for queries)",
        )
        .to_compile_error();
    }

    // First parameter must be ctx: &WorkflowContext.
    if !first_param_is_ctx(&func.sig.inputs) {
        return syn::Error::new_spanned(
            &func.sig,
            "#[query] handlers must take `ctx: &WorkflowContext` as the first argument",
        )
        .to_compile_error();
    }

    // Return type must be Result<T, E>.
    if !returns_result(&func.sig.output) {
        return syn::Error::new_spanned(
            &func.sig.output,
            "#[query] return type must be `Result<T, E>`",
        )
        .to_compile_error();
    }

    // ── Companion generation ──────────────────────────────────────────────────

    let fn_name = &func.sig.ident;
    let fn_name_str = fn_name.to_string();
    let companion_name = format_ident!("__autumn_query_handler_info_{fn_name}");

    // Skip the leading ctx param when building type hints and dispatch args.
    let params: Vec<_> = func.sig.inputs.iter().skip(1).collect();
    let param_names: Vec<_> = params
        .iter()
        .filter_map(|arg| {
            if let syn::FnArg::Typed(pt) = arg
                && let syn::Pat::Ident(ident) = &*pt.pat
            {
                return Some(&ident.ident);
            }
            None
        })
        .collect();

    let input_type_hint = build_input_type_hint(&params);
    let output_type_hint = extract_ok_type_hint(&func.sig.output);

    let dispatch = build_query_dispatch(fn_name, &param_names);

    quote! {
        #func

        #[doc(hidden)]
        pub fn #companion_name() -> ::autumn_harvest::QueryHandlerInfo {
            fn __dispatch(
                ctx: &::autumn_harvest::WorkflowContext,
                args: ::autumn_harvest::serde_json::Value,
            ) -> Result<::autumn_harvest::serde_json::Value, String> {
                #dispatch
            }

            ::autumn_harvest::QueryHandlerInfo {
                name: #fn_name_str,
                workflow: #workflow_name,
                module: module_path!(),
                input_type_hint: #input_type_hint,
                output_type_hint: #output_type_hint,
                handler: __dispatch,
            }
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Returns `true` when the first parameter in the list matches `ctx: &WorkflowContext`.
fn first_param_is_ctx(inputs: &syn::punctuated::Punctuated<syn::FnArg, syn::token::Comma>) -> bool {
    let Some(first) = inputs.first() else {
        return false;
    };
    let syn::FnArg::Typed(pt) = first else {
        return false;
    };
    // Accept any `&Xxx` reference type whose last path segment is `WorkflowContext`.
    let syn::Type::Reference(r) = &*pt.ty else {
        return false;
    };
    let syn::Type::Path(tp) = &*r.elem else {
        return false;
    };
    tp.path
        .segments
        .last()
        .is_some_and(|s| s.ident == "WorkflowContext")
}

fn returns_result(output: &syn::ReturnType) -> bool {
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

fn build_query_dispatch(fn_name: &syn::Ident, param_names: &[&syn::Ident]) -> TokenStream {
    if param_names.is_empty() {
        quote! {
            let result = #fn_name(ctx);
            result.map_err(|e| e.to_string())
                .and_then(|v| {
                    ::autumn_harvest::serde_json::to_value(v).map_err(|e| e.to_string())
                })
        }
    } else if param_names.len() == 1 {
        let name = &param_names[0];
        quote! {
            let #name = ::autumn_harvest::serde_json::from_value(args)
                .map_err(|e| e.to_string())?;
            let result = #fn_name(ctx, #name);
            result.map_err(|e| e.to_string())
                .and_then(|v| {
                    ::autumn_harvest::serde_json::to_value(v).map_err(|e| e.to_string())
                })
        }
    } else {
        let indices = (0..param_names.len()).map(syn::Index::from);
        let names = param_names.to_owned();
        quote! {
            let __args: ::autumn_harvest::serde_json::Value = args;
            #(
                let #names = ::autumn_harvest::serde_json::from_value(__args[#indices].clone())
                    .map_err(|e| e.to_string())?;
            )*
            let result = #fn_name(ctx, #(#names),*);
            result.map_err(|e| e.to_string())
                .and_then(|v| {
                    ::autumn_harvest::serde_json::to_value(v).map_err(|e| e.to_string())
                })
        }
    }
}

/// Returns a `String` describing the input parameters for `input_type_hint`.
fn build_input_type_hint(params: &[&syn::FnArg]) -> String {
    if params.is_empty() {
        return "()".to_string();
    }
    if params.len() == 1
        && let syn::FnArg::Typed(pt) = params[0]
    {
        return type_name_hint(&pt.ty);
    }
    // Multiple params: show as tuple
    let parts: Vec<_> = params
        .iter()
        .filter_map(|arg| {
            if let syn::FnArg::Typed(pt) = arg {
                Some(type_name_hint(&pt.ty))
            } else {
                None
            }
        })
        .collect();
    format!("({})", parts.join(", "))
}

/// Extracts the `T` from `Result<T, E>` for use as `output_type_hint`.
fn extract_ok_type_hint(output: &syn::ReturnType) -> String {
    let syn::ReturnType::Type(_, ty) = output else {
        return "()".to_string();
    };
    let syn::Type::Path(type_path) = &**ty else {
        return type_name_hint(ty);
    };
    let Some(last) = type_path.path.segments.last() else {
        return "()".to_string();
    };
    if last.ident != "Result" {
        return last.ident.to_string();
    }
    if let syn::PathArguments::AngleBracketed(ref args) = last.arguments
        && let Some(syn::GenericArgument::Type(ok_ty)) = args.args.first()
    {
        return type_name_hint(ok_ty);
    }
    "()".to_string()
}

/// Returns the human-readable name of a type suitable for type hints.
fn type_name_hint(ty: &syn::Type) -> String {
    match ty {
        syn::Type::Path(tp) => tp
            .path
            .segments
            .last()
            .map_or_else(
                || "?".to_string(),
                |s| {
                    let ident = s.ident.to_string();
                    if let syn::PathArguments::AngleBracketed(ref args) = s.arguments {
                        let inner: Vec<_> = args
                            .args
                            .iter()
                            .filter_map(|a| {
                                if let syn::GenericArgument::Type(t) = a {
                                    Some(type_name_hint(t))
                                } else {
                                    None
                                }
                            })
                            .collect();
                        if inner.is_empty() {
                            ident
                        } else {
                            format!("{ident}<{}>", inner.join(", "))
                        }
                    } else {
                        ident
                    }
                },
            ),
        syn::Type::Reference(r) => type_name_hint(&r.elem),
        syn::Type::Tuple(t) if t.elems.is_empty() => "()".to_string(),
        syn::Type::Tuple(t) => {
            let parts: Vec<_> = t.elems.iter().map(type_name_hint).collect();
            format!("({})", parts.join(", "))
        }
        _ => "?".to_string(),
    }
}
