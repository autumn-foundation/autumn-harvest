//! `#[activity]` attribute macro implementation.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Expr, ItemFn, LitInt, LitStr, parse::Parser as _};

use crate::parse_byte_size_macro;

struct ActivityAttrs {
    retry: Option<Expr>,
    start_to_close: Option<String>,
    heartbeat_timeout: Option<String>,
    schedule_to_start: Option<String>,
    queue: Option<String>,
    max_concurrent: Option<u32>,
    concurrency_key: Option<String>,
    local: bool,
    /// Per-activity input size cap override in bytes (issue #252).
    max_input_bytes: Option<u64>,
    /// Per-activity result size cap override in bytes (issue #252).
    max_result_bytes: Option<u64>,
    rate_limit_rps: Option<f64>,
    rate_limit_burst: Option<f64>,
    rate_limit_key: Option<String>,
    /// Circuit-breaker policy expression (issue #369), e.g.
    /// `CircuitBreakerPolicy::new(10, Duration::from_secs(30), Duration::from_secs(60))`.
    circuit_breaker: Option<Expr>,
}

#[allow(clippy::too_many_lines)]
fn parse_attrs(attr: TokenStream) -> syn::Result<ActivityAttrs> {
    let mut result = ActivityAttrs {
        retry: None,
        start_to_close: None,
        heartbeat_timeout: None,
        schedule_to_start: None,
        queue: None,
        max_concurrent: None,
        concurrency_key: None,
        local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        circuit_breaker: None,
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
        } else if meta.path.is_ident("max_concurrent") {
            let value: LitInt = meta.value()?.parse()?;
            let n: u32 = value.base10_parse()?;
            if n == 0 {
                return Err(meta.error("max_concurrent must be greater than zero"));
            }
            result.max_concurrent = Some(n);
            Ok(())
        } else if meta.path.is_ident("concurrency_key") {
            let value: LitStr = meta.value()?.parse()?;
            result.concurrency_key = Some(value.value());
            Ok(())
        } else if meta.path.is_ident("local") {
            let value: syn::LitBool = meta.value()?.parse()?;
            result.local = value.value();
            Ok(())
        } else if meta.path.is_ident("max_input_bytes") {
            let value: LitStr = meta.value()?.parse()?;
            let s = value.value();
            let bytes = parse_byte_size_macro(&s).ok_or_else(|| {
                meta.error(format!(
                    "invalid byte size '{s}'; expected format like \"2MiB\", \"512KiB\", \"4MB\""
                ))
            })?;
            result.max_input_bytes = Some(bytes);
            Ok(())
        } else if meta.path.is_ident("max_result_bytes") {
            let value: LitStr = meta.value()?.parse()?;
            let s = value.value();
            let bytes = parse_byte_size_macro(&s).ok_or_else(|| {
                meta.error(format!(
                    "invalid byte size '{s}'; expected format like \"2MiB\", \"512KiB\", \"4MB\""
                ))
            })?;
            result.max_result_bytes = Some(bytes);
            Ok(())
        } else if meta.path.is_ident("rate_limit_rps") {
            let value: syn::Lit = meta.value()?.parse()?;
            let n = match value {
                syn::Lit::Float(f) => f.base10_parse::<f64>()?,
                syn::Lit::Int(i) => i.base10_parse::<f64>()?,
                _ => return Err(meta.error("rate_limit_rps must be an integer or float")),
            };
            if n <= 0.0 {
                return Err(meta.error("rate_limit_rps must be greater than zero"));
            }
            result.rate_limit_rps = Some(n);
            Ok(())
        } else if meta.path.is_ident("rate_limit_burst") {
            let value: syn::Lit = meta.value()?.parse()?;
            let n = match value {
                syn::Lit::Float(f) => f.base10_parse::<f64>()?,
                syn::Lit::Int(i) => i.base10_parse::<f64>()?,
                _ => return Err(meta.error("rate_limit_burst must be an integer or float")),
            };
            if n <= 0.0 {
                return Err(meta.error("rate_limit_burst must be greater than zero"));
            }
            result.rate_limit_burst = Some(n);
            Ok(())
        } else if meta.path.is_ident("rate_limit_key") {
            let value: LitStr = meta.value()?.parse()?;
            result.rate_limit_key = Some(value.value());
            Ok(())
        } else if meta.path.is_ident("circuit_breaker") {
            // Parse as Expr so nested constructor calls with commas work, e.g.
            // `circuit_breaker = CircuitBreakerPolicy::new(10, w, c)`.
            let value: Expr = meta.value()?.parse()?;
            result.circuit_breaker = Some(value);
            Ok(())
        } else {
            Err(meta.error(
                "unsupported attribute: expected retry, start_to_close, heartbeat_timeout, \
                 schedule_to_start, queue, max_concurrent, concurrency_key, local, \
                 max_input_bytes, max_result_bytes, rate_limit_rps, rate_limit_burst, \
                 rate_limit_key, or circuit_breaker",
            ))
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

/// Returns `true` when the function's return type is `Result<_, ActivityFailure>`
/// (with or without a path prefix like `failure::` or `autumn_harvest::failure::`).
///
/// Used to opt into the typed-failure encoding without breaking activities
/// that return `Result<T, String>`, `HarvestResult<T>`, or a custom error
/// enum.  Users who alias `ActivityFailure` to another name fall back to the
/// `.to_string()` path — the typed JSON encoding can still be obtained by
/// calling `.into_error_payload()` manually.
fn activity_returns_activity_failure(output: &syn::ReturnType) -> bool {
    let syn::ReturnType::Type(_, ty) = output else {
        return false;
    };
    let syn::Type::Path(type_path) = &**ty else {
        return false;
    };
    let Some(last) = type_path.path.segments.last() else {
        return false;
    };
    if last.ident != "Result" {
        return false;
    }
    let syn::PathArguments::AngleBracketed(args) = &last.arguments else {
        return false;
    };
    // Second generic argument is the error type.
    let Some(syn::GenericArgument::Type(syn::Type::Path(err_path))) = args.args.iter().nth(1)
    else {
        return false;
    };
    err_path
        .path
        .segments
        .last()
        .is_some_and(|seg| seg.ident == "ActivityFailure")
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

    // Local activities may only use start_to_close. Emit a clear compile error
    // for any unsupported option to prevent silent misconfiguration.
    if attrs.local {
        if attrs.heartbeat_timeout.is_some() {
            return syn::Error::new_spanned(
                input_fn.sig.fn_token,
                "local activities do not support heartbeat_timeout; \
                 use a regular activity if you need heartbeating",
            )
            .to_compile_error();
        }
        if attrs.schedule_to_start.is_some() {
            return syn::Error::new_spanned(
                input_fn.sig.fn_token,
                "local activities do not support schedule_to_start; \
                 local activities always run inline on the workflow worker",
            )
            .to_compile_error();
        }
        if attrs.queue.is_some() {
            return syn::Error::new_spanned(
                input_fn.sig.fn_token,
                "local activities do not support queue; \
                 local activities always run on the worker executing the workflow",
            )
            .to_compile_error();
        }
        if attrs.circuit_breaker.is_some() {
            return syn::Error::new_spanned(
                input_fn.sig.fn_token,
                "local activities do not support circuit_breaker; \
                 the circuit breaker is enforced on the task-dispatch path, which \
                 local activities bypass by running inline on the workflow worker",
            )
            .to_compile_error();
        }
    }

    let fn_name = &input_fn.sig.ident;
    let fn_name_str = fn_name.to_string();
    let companion_name = format_ident!("__autumn_activity_info_{fn_name}");
    let public_info_name = format_ident!("{fn_name}_info");

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

    let max_concurrent_expr = attrs
        .max_concurrent
        .map_or_else(|| quote! { None }, |n| quote! { Some(#n) });

    // Default concurrency_key to the activity's own name when max_concurrent is
    // set but no explicit key was provided. This ensures each activity caps
    // itself independently unless the operator deliberately groups activities.
    let concurrency_key_expr = match attrs.concurrency_key.as_deref() {
        Some(key) => quote! { Some(#key) },
        None => {
            if attrs.max_concurrent.is_some() {
                quote! { Some(#fn_name_str) }
            } else {
                quote! { None }
            }
        }
    };

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

    // Encode the activity's error.
    //
    // Backward-compat: by default we use `.to_string()` (the original
    // `ToString`-bounded path) so every activity that returns
    // `Result<T, String>`, `Result<T, HarvestError>`, or a custom
    // `thiserror` enum continues to compile and behave identically.
    //
    // Issue #227: when the user opts into the typed surface by returning
    // `Result<T, ActivityFailure>` (recognized syntactically below), we
    // route through `ActivityFailure`'s `IntoActivityErrorString` impl so
    // the JSON payload carries `error_type` and `non_retryable`.
    let returns_activity_failure = activity_returns_activity_failure(&input_fn.sig.output);
    let encode_err = if returns_activity_failure {
        quote! {
            |e| ::autumn_harvest::failure::IntoActivityErrorString::into_error_payload(e)
        }
    } else {
        quote! {
            |e| e.to_string()
        }
    };
    let dispatch = if param_names.is_empty() {
        quote! {
            let result = #fn_name(ctx).await;
            result.map_err(#encode_err)
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
            result.map_err(#encode_err)
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
            result.map_err(#encode_err)
                .and_then(|v| {
                    ::autumn_harvest::serde_json::to_value(v)
                        .map_err(|e| e.to_string())
                })
        }
    };

    let is_local = attrs.local;

    let max_input_bytes_expr = attrs
        .max_input_bytes
        .map_or_else(|| quote! { None }, |b| quote! { Some(#b) });
    let max_result_bytes_expr = attrs
        .max_result_bytes
        .map_or_else(|| quote! { None }, |b| quote! { Some(#b) });

    let rate_limit_rps_expr = attrs
        .rate_limit_rps
        .map_or_else(|| quote! { None }, |n| quote! { Some(#n) });
    let rate_limit_burst_expr = attrs
        .rate_limit_burst
        .map_or_else(|| quote! { None }, |n| quote! { Some(#n) });
    let rate_limit_key_expr = match attrs.rate_limit_key.as_deref() {
        Some(key) => quote! { Some(#key) },
        None => {
            if attrs.rate_limit_rps.is_some() || attrs.rate_limit_burst.is_some() {
                quote! { Some(#fn_name_str) }
            } else {
                quote! { None }
            }
        }
    };

    let circuit_breaker_expr = attrs
        .circuit_breaker
        .as_ref()
        .map_or_else(|| quote! { None }, |expr| quote! { Some(#expr) });

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
                max_concurrent: #max_concurrent_expr,
                concurrency_key: #concurrency_key_expr,
                is_local: #is_local,
                max_input_bytes: #max_input_bytes_expr,
                max_result_bytes: #max_result_bytes_expr,
                rate_limit_rps: #rate_limit_rps_expr,
                rate_limit_burst: #rate_limit_burst_expr,
                rate_limit_key: #rate_limit_key_expr,
                circuit_breaker: #circuit_breaker_expr,
                handler: |ctx, input| {
                    ::std::boxed::Box::pin(async move {
                        #dispatch
                    })
                },
            }
        }

        /// Returns the [`::autumn_harvest::ActivityInfo`] for this activity.
        ///
        /// Pass to typed dispatch helpers on [`::autumn_harvest::WorkflowContext`]:
        ///
        /// ```rust,ignore
        /// ctx.execute_activity(&#public_info_name(), input).await?;
        /// ```
        pub fn #public_info_name() -> ::autumn_harvest::ActivityInfo {
            #companion_name()
        }
    }
}
