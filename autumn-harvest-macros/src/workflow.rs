//! `#[workflow]` attribute macro implementation.
//!
//! Emits the original function unchanged plus a companion:
//!   `pub fn __autumn_workflow_info_{name}() -> ::autumn_harvest::WorkflowInfo`

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{ItemFn, LitStr, parse::Parser as _};

struct WorkflowAttrs {
    execution_timeout: Option<String>,
}

fn parse_attrs(attr: TokenStream) -> syn::Result<WorkflowAttrs> {
    let mut result = WorkflowAttrs {
        execution_timeout: None,
    };

    syn::meta::parser(|meta| {
        if meta.path.is_ident("execution_timeout") {
            let value: LitStr = meta.value()?.parse()?;
            result.execution_timeout = Some(value.value());
            Ok(())
        } else {
            Err(meta.error("unsupported attribute: expected execution_timeout"))
        }
    })
    .parse2(attr)?;

    Ok(result)
}

pub fn workflow_macro(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attrs = match parse_attrs(attr) {
        Ok(a) => a,
        Err(e) => return e.to_compile_error(),
    };

    let input_fn: ItemFn = match syn::parse2(item) {
        Ok(f) => f,
        Err(e) => return e.to_compile_error(),
    };

    if input_fn.sig.asyncness.is_none() {
        return syn::Error::new_spanned(
            input_fn.sig.fn_token,
            "#[workflow] functions must be async",
        )
        .to_compile_error();
    }

    let fn_name = &input_fn.sig.ident;
    let fn_name_str = fn_name.to_string();
    let companion_name = format_ident!("__autumn_workflow_info_{fn_name}");

    // Collect parameter names after the first (ctx is first, rest are inputs).
    // For Phase 1 we pass all non-ctx args as a single JSON value.
    let params: Vec<_> = input_fn.sig.inputs.iter().skip(1).collect();
    let param_names: Vec<_> = params
        .iter()
        .filter_map(|arg| {
            if let syn::FnArg::Typed(pat) = arg
                && let syn::Pat::Ident(ident) = pat.pat.as_ref()
            {
                return Some(&ident.ident);
            }
            None
        })
        .collect();

    let dispatch = if param_names.is_empty() {
        quote! {
            let result = #fn_name(ctx).await;
            result.map_err(|e| e.to_string())
                .and_then(|v| {
                    ::autumn_harvest::serde_json::to_value(v)
                        .map_err(|e| e.to_string())
                })
        }
    } else if param_names.len() == 1 {
        let name = &param_names[0];
        quote! {
            let #name = ::autumn_harvest::serde_json::from_value(input)
                .map_err(|e| e.to_string())?;
            let result = #fn_name(ctx, #name).await;
            result.map_err(|e| e.to_string())
                .and_then(|v| {
                    ::autumn_harvest::serde_json::to_value(v)
                        .map_err(|e| e.to_string())
                })
        }
    } else {
        // Multiple params: expect input to be a JSON array [arg1, arg2, ...]
        let indices = (0..param_names.len()).map(syn::Index::from);
        let names = param_names.clone();
        quote! {
            let args: ::autumn_harvest::serde_json::Value = input;
            #(
                let #names = ::autumn_harvest::serde_json::from_value(args[#indices].clone())
                    .map_err(|e| e.to_string())?;
            )*
            let result = #fn_name(ctx, #(#names),*).await;
            result.map_err(|e| e.to_string())
                .and_then(|v| {
                    ::autumn_harvest::serde_json::to_value(v)
                        .map_err(|e| e.to_string())
                })
        }
    };

    // Emit execution_timeout as Option<Duration> using the task_duration helper.
    let execution_timeout_expr = match &attrs.execution_timeout {
        Some(s) => quote! {
            ::autumn_harvest::task_duration(#s)
        },
        None => quote! { None },
    };

    quote! {
        #input_fn

        #[doc(hidden)]
        pub fn #companion_name() -> ::autumn_harvest::WorkflowInfo {
            ::autumn_harvest::WorkflowInfo {
                name: #fn_name_str,
                module: module_path!(),
                handler: |ctx, input| {
                    ::std::boxed::Box::pin(async move {
                        #dispatch
                    })
                },
                execution_timeout: #execution_timeout_expr,
            }
        }
    }
}
