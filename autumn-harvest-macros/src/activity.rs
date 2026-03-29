//! `#[activity]` attribute macro implementation.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Expr, ItemFn, LitStr, parse::Parser as _};

struct ActivityAttrs {
    retry: Option<Expr>,
    start_to_close: Option<String>,
    heartbeat_timeout: Option<String>,
    schedule_to_start: Option<String>,
    queue: Option<String>,
}

fn parse_attrs(attr: TokenStream) -> syn::Result<ActivityAttrs> {
    let mut result = ActivityAttrs {
        retry: None,
        start_to_close: None,
        heartbeat_timeout: None,
        schedule_to_start: None,
        queue: None,
    };

    syn::meta::parser(|meta| {
        if meta.path.is_ident("retry") {
            // Parse as Expr so nested function calls with commas work correctly,
            // e.g. `retry = RetryPolicy::fixed(3, Duration::from_secs(1))`.
            let value: Expr = meta.value()?.parse()?;
            result.retry = Some(value);
            Ok(())
        } else if meta.path.is_ident("start_to_close") {
            let value: LitStr = meta.value()?.parse()?;
            result.start_to_close = Some(value.value());
            Ok(())
        } else if meta.path.is_ident("heartbeat_timeout") {
            let value: LitStr = meta.value()?.parse()?;
            result.heartbeat_timeout = Some(value.value());
            Ok(())
        } else if meta.path.is_ident("schedule_to_start") {
            let value: LitStr = meta.value()?.parse()?;
            result.schedule_to_start = Some(value.value());
            Ok(())
        } else if meta.path.is_ident("queue") {
            let value: LitStr = meta.value()?.parse()?;
            result.queue = Some(value.value());
            Ok(())
        } else {
            Err(meta.error("unsupported attribute: expected retry, start_to_close, heartbeat_timeout, schedule_to_start, or queue"))
        }
    })
    .parse2(attr)?;

    Ok(result)
}

fn duration_expr(s: &str) -> TokenStream {
    quote! {
        ::autumn_harvest::task_duration(#s)
            .expect(concat!("invalid duration string: ", #s))
    }
}

// The function necessarily handles multiple code-gen paths (0/1/N params,
// 5 optional attribute fields, quote! blocks) — splitting it further would
// hurt readability more than the length lint helps.
#[allow(clippy::too_many_lines)]
pub fn activity_macro(attr: TokenStream, item: TokenStream) -> TokenStream {
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
            "#[activity] functions must be async",
        )
        .to_compile_error();
    }

    let fn_name = &input_fn.sig.ident;
    let fn_name_str = fn_name.to_string();
    let companion_name = format_ident!("__autumn_activity_info_{fn_name}");

    let retry_expr = attrs
        .retry
        .as_ref()
        .map_or_else(|| quote! { None }, |policy| quote! { Some(#policy) });

    let start_to_close_expr = attrs.start_to_close.as_deref().map_or_else(
        || quote! { None },
        |s| {
            let d = duration_expr(s);
            quote! { Some(#d) }
        },
    );

    let heartbeat_timeout_expr = attrs.heartbeat_timeout.as_deref().map_or_else(
        || quote! { None },
        |s| {
            let d = duration_expr(s);
            quote! { Some(#d) }
        },
    );

    let schedule_to_start_expr = attrs.schedule_to_start.as_deref().map_or_else(
        || quote! { None },
        |s| {
            let d = duration_expr(s);
            quote! { Some(#d) }
        },
    );

    let queue_expr = attrs
        .queue
        .as_deref()
        .map_or_else(|| quote! { None }, |q| quote! { Some(#q) });

    let params: Vec<_> = input_fn.sig.inputs.iter().skip(1).collect();
    let param_names: Vec<_> = params
        .iter()
        .filter_map(|arg| {
            if let syn::FnArg::Typed(pat) = arg {
                if let syn::Pat::Ident(ident) = pat.pat.as_ref() {
                    return Some(&ident.ident);
                }
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

    quote! {
        #input_fn

        #[doc(hidden)]
        pub fn #companion_name() -> ::autumn_harvest::ActivityInfo {
            ::autumn_harvest::ActivityInfo {
                name: #fn_name_str,
                module: module_path!(),
                default_retry_policy: #retry_expr,
                default_start_to_close: #start_to_close_expr,
                default_heartbeat_timeout: #heartbeat_timeout_expr,
                default_schedule_to_start: #schedule_to_start_expr,
                default_queue: #queue_expr,
                handler: |ctx, input| {
                    ::std::boxed::Box::pin(async move {
                        #dispatch
                    })
                },
            }
        }
    }
}
