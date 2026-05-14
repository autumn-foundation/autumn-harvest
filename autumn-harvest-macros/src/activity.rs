//! The `#[activity]` attribute macro implementation.
//!
//! This module contains the parsing and expansion logic that transforms
//! standard async Rust functions into distributed tasks capable of routing
//! inputs and handling resilient retry policies.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Expr, ItemFn, LitInt, LitStr, parse::Parser as _};

/// The parsed configuration payload extracted from the `#[activity(...)]` attribute.
///
/// When a tired developer annotates an async function with `#[activity]`, this struct
/// holds all the knobs and dials they've configured for how that activity should
/// behave in the distributed system. It bridges the gap between the macro's token stream
/// and the strongly-typed [`autumn_harvest::ActivityInfo`] struct used by the engine.
struct ActivityAttrs {
    /// The retry policy expression. If a network call fails, this dictates how
    /// aggressively the engine should try again.
    retry: Option<Expr>,
    /// The maximum allowed duration for the activity to execute.
    /// Prevents zombie workers from holding resources indefinitely.
    start_to_close: Option<String>,
    /// The heartbeat timeout. If the activity doesn't report progress within
    /// this window, the engine assumes the worker died and reschedules it.
    heartbeat_timeout: Option<String>,
    /// The maximum time the activity is allowed to sit in the queue waiting
    /// for a worker to pick it up.
    schedule_to_start: Option<String>,
    /// The specific task queue this activity must be routed to. Useful for
    /// pinning heavy jobs to specialized worker nodes.
    queue: Option<String>,
    /// A concurrency limit to protect downstream services (like a fragile legacy API)
    /// from being overwhelmed by too many simultaneous executions.
    max_concurrent: Option<u32>,
    /// An optional string key used to group concurrency limits across different activities.
    concurrency_key: Option<String>,
    /// A flag indicating that this activity is incredibly fast and should be
    /// executed inline by the workflow worker itself, skipping the task queue entirely.
    local: bool,
}

/// Parses the raw token stream from the `#[activity(...)]` macro into a structured `ActivityAttrs`.
///
/// This function is the gatekeeper. It reads the developer's raw configuration,
/// validating names and parsing values, ensuring we don't accidentally accept
/// an unsupported attribute that would silently fail in production.
///
/// # Panics
///
/// This function returns a `syn::Result` rather than panicking, surfacing graceful
/// compiler errors directly to the user's IDE if they misspell an attribute.
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
        } else {
            Err(meta.error("unsupported attribute: expected retry, start_to_close, heartbeat_timeout, schedule_to_start, queue, max_concurrent, concurrency_key, or local"))
        }
    })
    .parse2(attr)?;

    Ok(result)
}

/// Generates a token stream that parses a human-readable duration string into a [`std::time::Duration`].
///
/// We want users to write `start_to_close = "5m"` instead of forcing them to instantiate
/// `Duration::from_secs(300)` directly in the macro. This helper writes the runtime
/// parsing code that executes when the companion info struct is initialized.
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
/// The core engine of the `#[activity]` macro.
///
/// This macro transforms a standard async function into a distributed task. It preserves
/// the original function entirely, but weaves a hidden "companion function" into the module.
/// This companion function (e.g., `__autumn_activity_info_my_task`) is what the worker
/// actually calls to discover the task's name, configuration, and execution wrapper.
///
/// By generating this companion, we keep the user's original function pure and testable,
/// while still providing the engine with the strongly-typed metadata it needs to route
/// inputs and outputs across the network.
///
/// # Examples
///
/// ```ignore
/// #[activity(start_to_close = "5m", retry = RetryPolicy::default())]
/// async fn process_payment(ctx: ActivityContext, amount: u64) -> Result<(), ActivityFailure> {
///     // Business logic
///     Ok(())
/// }
/// ```
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
                handler: |ctx, input| {
                    ::std::boxed::Box::pin(async move {
                        #dispatch
                    })
                },
            }
        }
    }
}
