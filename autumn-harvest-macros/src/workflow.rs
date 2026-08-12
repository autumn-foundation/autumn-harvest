//! `#[workflow]` attribute macro implementation.
//!
//! Emits the original function unchanged plus a companion:
//!   `pub fn __autumn_workflow_info_{name}() -> ::autumn_harvest::WorkflowInfo`

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{ItemFn, LitStr, parse::Parser as _};

use crate::parse_byte_size_macro;

/// Compile-time check that a duration literal is parseable by the runtime
/// `::autumn_harvest::task_duration` helper (`<digits><s|m|h|d>` runs, non-zero).
///
/// Mirrors `autumn_harvest::task_duration`'s accepted format so an invalid
/// `#[workflow(debounce(window = ..., max_wait = ...))]` literal is rejected as a
/// build error rather than panicking at registration via the emitted `.expect`.
/// Kept intentionally permissive/identical to the runtime parser; the runtime
/// `.expect` remains as a defensive backstop.
use crate::attr_util::is_valid_task_duration;

// ---------------------------------------------------------------------------
// Attribute parsing
// ---------------------------------------------------------------------------

struct ConcurrencyArgs {
    key_expr: String,
    limit: u32,
}

struct DebounceArgs {
    key_expr: String,
    window: String,
    max_wait: Option<String>,
}

struct BatchArgs {
    key_expr: String,
    max_size: u32,
    max_wait: String,
}

struct ThrottleArgs {
    rate: String,
    burst: Option<f64>,
    key: Option<String>,
    schedule_to_start: Option<String>,
}

/// Validate a rate string like `"100/m"` at macro-expansion time so a typo is a
/// build error, not a registration-time panic from the emitted `.expect(...)`.
/// Mirrors the runtime `autumn_harvest::throttle::parse_rate` grammar.
fn is_valid_rate(s: &str) -> bool {
    let Some((count, unit)) = s.split_once('/') else {
        return false;
    };
    let Ok(n) = count.trim().parse::<f64>() else {
        return false;
    };
    n.is_finite() && n > 0.0 && matches!(unit.trim(), "s" | "m" | "h")
}

/// The per-period count of an already-validated rate string (e.g. `"100/m"`
/// -> `100.0`, `"0.5/s"` -> `0.5`). Only call after [`is_valid_rate`] has
/// confirmed the string is well-formed -- mirrors
/// `autumn_harvest::throttle::RateSpec::per_period_count`'s runtime parsing
/// so the macro's compile-time check agrees with `ThrottlePolicy::from_rate_str`'s
/// defaulted-burst rejection (issue #607 code review).
fn rate_per_period_count(s: &str) -> f64 {
    s.split_once('/')
        .and_then(|(count, _unit)| count.trim().parse::<f64>().ok())
        .unwrap_or(0.0)
}

struct WorkflowAttrs {
    execution_timeout: Option<String>,
    /// Chain-scoped lifetime cap (issue #617). Parsed from
    /// `#[workflow(chain_execution_timeout = "7d")]`. Stored as
    /// `WorkflowInfo::chain_execution_timeout: Option<Duration>`. Distinct from
    /// `execution_timeout`: anchored at the first run's start and carried verbatim
    /// across every continue-as-new.
    chain_execution_timeout: Option<String>,
    /// Soft SLA budget (issue #487). Parsed from `#[workflow(sla = "2h")]`.
    /// Stored as `WorkflowInfo::sla: Option<Duration>`.
    sla: Option<String>,
    concurrency: Option<ConcurrencyArgs>,
    /// Trailing-edge debounce policy (issue #499). Parsed from
    /// `#[workflow(debounce(key = "input.tenant_id", window = "30s", max_wait = "5m"))]`.
    debounce: Option<DebounceArgs>,
    /// Event batching policy (issue #518). Parsed from
    /// `#[workflow(batch(key = "input.tenant_id", max_size = 10, max_wait = "30s"))]`.
    batch: Option<BatchArgs>,
    /// Start-throttle policy (issue #607). Parsed from
    /// `#[workflow(throttle(rate = "100/m", burst = 20, key = "input.tenant_id", schedule_to_start = "5m"))]`.
    throttle: Option<ThrottleArgs>,
    /// Per-workflow-type cap override in bytes (issue #252). Parsed from
    /// `#[workflow(max_input_bytes = "8MiB")]` at compile time.
    max_input_bytes: Option<u64>,
    owner: Option<String>,
    runbook: Option<String>,
    severity: Option<String>,
    /// Human-readable description for operator/UI discovery (issue #373).
    /// Parsed from `#[workflow(description = "...")]`.
    description: Option<String>,
    allow_nondeterministic_apis: bool,
    /// Workflow-level retry policy (issue #523).
    /// Parsed from `#[workflow(retry = RetryPolicy::exponential(3, Duration::from_secs(1)))]`.
    retry: Option<syn::Expr>,
    /// Opt-in MCP tool exposure (issue #597). Parsed from `#[workflow(mcp)]`
    /// or `#[workflow(mcp = true)]`. Only sets `WorkflowInfo::mcp` — the
    /// macro never emits `::autumn_web::` paths.
    mcp: bool,
}

#[allow(clippy::too_many_lines)]
fn parse_attrs(attr: TokenStream) -> syn::Result<WorkflowAttrs> {
    let mut result = WorkflowAttrs {
        execution_timeout: None,
        chain_execution_timeout: None,
        sla: None,
        concurrency: None,
        debounce: None,
        batch: None,
        throttle: None,
        max_input_bytes: None,
        owner: None,
        runbook: None,
        severity: None,
        description: None,
        allow_nondeterministic_apis: false,
        retry: None,
        mcp: false,
    };

    syn::meta::parser(|meta| {
        if meta.path.is_ident("execution_timeout") {
            let value: LitStr = meta.value()?.parse()?;
            result.execution_timeout = Some(value.value());
            Ok(())
        } else if meta.path.is_ident("chain_execution_timeout") {
            let value: LitStr = meta.value()?.parse()?;
            result.chain_execution_timeout = Some(value.value());
            Ok(())
        } else if meta.path.is_ident("sla") {
            let value: LitStr = meta.value()?.parse()?;
            result.sla = Some(value.value());
            Ok(())
        } else if meta.path.is_ident("concurrency") {
            let mut key_expr: Option<String> = None;
            let mut limit: Option<u32> = None;
            meta.parse_nested_meta(|inner| {
                if inner.path.is_ident("key") {
                    let value: LitStr = inner.value()?.parse()?;
                    key_expr = Some(value.value());
                    Ok(())
                } else if inner.path.is_ident("limit") {
                    let value: syn::LitInt = inner.value()?.parse()?;
                    let n: u32 = value.base10_parse()?;
                    if n == 0 {
                        return Err(inner.error("concurrency limit must be greater than zero"));
                    }
                    limit = Some(n);
                    Ok(())
                } else {
                    Err(inner.error("expected `key` or `limit`"))
                }
            })?;
            let key_expr = key_expr.ok_or_else(|| {
                syn::Error::new(
                    proc_macro2::Span::call_site(),
                    "concurrency requires `key = \"...\"`",
                )
            })?;
            let limit = limit.ok_or_else(|| {
                syn::Error::new(
                    proc_macro2::Span::call_site(),
                    "concurrency requires `limit = N`",
                )
            })?;
            result.concurrency = Some(ConcurrencyArgs { key_expr, limit });
            Ok(())
        } else if meta.path.is_ident("debounce") {
            let mut key_expr: Option<String> = None;
            let mut window: Option<String> = None;
            let mut max_wait: Option<String> = None;
            meta.parse_nested_meta(|inner| {
                if inner.path.is_ident("key") {
                    let value: LitStr = inner.value()?.parse()?;
                    key_expr = Some(value.value());
                    Ok(())
                } else if inner.path.is_ident("window") {
                    let value: LitStr = inner.value()?.parse()?;
                    // Validate at compile time so a typo is a build error, not a
                    // registration-time panic from the emitted `.expect(...)`.
                    if !is_valid_task_duration(&value.value()) {
                        return Err(syn::Error::new_spanned(
                            &value,
                            "invalid debounce `window` duration; expected e.g. \"30s\", \"5m\", \"1h\", \"2d\"",
                        ));
                    }
                    window = Some(value.value());
                    Ok(())
                } else if inner.path.is_ident("max_wait") {
                    let value: LitStr = inner.value()?.parse()?;
                    if !is_valid_task_duration(&value.value()) {
                        return Err(syn::Error::new_spanned(
                            &value,
                            "invalid debounce `max_wait` duration; expected e.g. \"30s\", \"5m\", \"1h\", \"2d\"",
                        ));
                    }
                    max_wait = Some(value.value());
                    Ok(())
                } else {
                    Err(inner.error("expected `key`, `window`, or `max_wait`"))
                }
            })?;
            let key_expr = key_expr.ok_or_else(|| {
                syn::Error::new(
                    proc_macro2::Span::call_site(),
                    "debounce requires `key = \"...\"`",
                )
            })?;
            let window = window.ok_or_else(|| {
                syn::Error::new(
                    proc_macro2::Span::call_site(),
                    "debounce requires `window = \"...\"` (e.g. `window = \"30s\"`)",
                )
            })?;
            result.debounce = Some(DebounceArgs { key_expr, window, max_wait });
            Ok(())
        } else if meta.path.is_ident("batch") {
            let mut key_expr: Option<String> = None;
            let mut max_size: Option<u32> = None;
            let mut max_wait: Option<String> = None;
            meta.parse_nested_meta(|inner| {
                if inner.path.is_ident("key") {
                    let value: LitStr = inner.value()?.parse()?;
                    key_expr = Some(value.value());
                    Ok(())
                } else if inner.path.is_ident("max_size") {
                    let value: syn::LitInt = inner.value()?.parse()?;
                    let n: u32 = value.base10_parse()?;
                    if n == 0 {
                        return Err(inner.error("batch max_size must be greater than zero"));
                    }
                    max_size = Some(n);
                    Ok(())
                } else if inner.path.is_ident("max_wait") {
                    let value: LitStr = inner.value()?.parse()?;
                    if !is_valid_task_duration(&value.value()) {
                        return Err(syn::Error::new_spanned(
                            &value,
                            "invalid batch `max_wait` duration; expected e.g. \"30s\", \"5m\", \"1h\", \"2d\"",
                        ));
                    }
                    max_wait = Some(value.value());
                    Ok(())
                } else {
                    Err(inner.error("expected `key`, `max_size`, or `max_wait`"))
                }
            })?;
            let key_expr = key_expr.ok_or_else(|| {
                syn::Error::new(
                    proc_macro2::Span::call_site(),
                    "batch requires `key = \"...\"`",
                )
            })?;
            let max_size = max_size.ok_or_else(|| {
                syn::Error::new(
                    proc_macro2::Span::call_site(),
                    "batch requires `max_size = N`",
                )
            })?;
            let max_wait = max_wait.ok_or_else(|| {
                syn::Error::new(
                    proc_macro2::Span::call_site(),
                    "batch requires `max_wait = \"...\"` (e.g. `max_wait = \"10s\"`)",
                )
            })?;
            result.batch = Some(BatchArgs { key_expr, max_size, max_wait });
            Ok(())
        } else if meta.path.is_ident("throttle") {
            let mut rate: Option<String> = None;
            let mut burst: Option<f64> = None;
            let mut key: Option<String> = None;
            let mut schedule_to_start: Option<String> = None;
            meta.parse_nested_meta(|inner| {
                if inner.path.is_ident("rate") {
                    let value: LitStr = inner.value()?.parse()?;
                    if !is_valid_rate(&value.value()) {
                        return Err(syn::Error::new_spanned(
                            &value,
                            "invalid throttle `rate`; expected \"<count>/<unit>\" with unit s, m, or h (e.g. \"100/m\")",
                        ));
                    }
                    rate = Some(value.value());
                    Ok(())
                } else if inner.path.is_ident("burst") {
                    // Accept an integer or float literal.
                    let value: syn::Lit = inner.value()?.parse()?;
                    let n = match value {
                        syn::Lit::Int(i) => i.base10_parse::<f64>()?,
                        syn::Lit::Float(f) => f.base10_parse::<f64>()?,
                        other => {
                            return Err(syn::Error::new_spanned(
                                other,
                                "throttle `burst` must be an integer or float",
                            ))
                        }
                    };
                    if !n.is_finite() {
                        return Err(inner.error("throttle `burst` must be a finite number"));
                    }
                    if n < 1.0 {
                        return Err(inner.error(
                            "throttle `burst` must be >= 1.0 (a bucket capacity below \
                             one token can never successfully debit)",
                        ));
                    }
                    burst = Some(n);
                    Ok(())
                } else if inner.path.is_ident("key") {
                    let value: LitStr = inner.value()?.parse()?;
                    key = Some(value.value());
                    Ok(())
                } else if inner.path.is_ident("schedule_to_start") {
                    let value: LitStr = inner.value()?.parse()?;
                    if !is_valid_task_duration(&value.value()) {
                        return Err(syn::Error::new_spanned(
                            &value,
                            "invalid throttle `schedule_to_start` duration; expected e.g. \"30s\", \"5m\", \"1h\"",
                        ));
                    }
                    schedule_to_start = Some(value.value());
                    Ok(())
                } else {
                    Err(inner.error("expected `rate`, `burst`, `key`, or `schedule_to_start`"))
                }
            })?;
            let rate = rate.ok_or_else(|| {
                syn::Error::new(
                    proc_macro2::Span::call_site(),
                    "throttle requires `rate = \"...\"` (e.g. `rate = \"100/m\"`)",
                )
            })?;
            // A sub-unit rate (e.g. "0.5/s") with no explicit `burst` would
            // default the bucket capacity to that same sub-one value, which
            // can never successfully debit a token -- `ThrottlePolicy::
            // from_rate_str` rejects this at runtime, but by then the
            // generated companion function's `.expect(...)` panics at
            // workflow-registration time instead of failing to compile.
            // Reject it here instead (issue #607 code review).
            if burst.is_none() && rate_per_period_count(&rate) < 1.0 {
                return Err(syn::Error::new(
                    proc_macro2::Span::call_site(),
                    format!(
                        "throttle rate '{rate}' has a per-period count below 1.0; \
                         pass an explicit burst >= 1.0 to use a sub-unit rate"
                    ),
                ));
            }
            result.throttle = Some(ThrottleArgs {
                rate,
                burst,
                key,
                schedule_to_start,
            });
            Ok(())
        } else if meta.path.is_ident("allow_nondeterministic_apis") {
            result.allow_nondeterministic_apis = crate::attr_util::parse_bool_flag(&meta)?;
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
        } else if meta.path.is_ident("owner") {
            let value: LitStr = meta.value()?.parse()?;
            result.owner = Some(value.value());
            Ok(())
        } else if meta.path.is_ident("runbook") {
            let value: LitStr = meta.value()?.parse()?;
            result.runbook = Some(value.value());
            Ok(())
        } else if meta.path.is_ident("severity") {
            let value: LitStr = meta.value()?.parse()?;
            let s = value.value();
            if s != "sev1" && s != "sev2" && s != "sev3" && s != "sev4" {
                return Err(meta.error(format!(
                    "invalid severity level: '{s}'; expected 'sev1', 'sev2', 'sev3', or 'sev4'"
                )));
            }
            result.severity = Some(s);
            Ok(())
        } else if meta.path.is_ident("description") {
            let value: LitStr = meta.value()?.parse()?;
            result.description = Some(value.value());
            Ok(())
        } else if meta.path.is_ident("retry") {
            let value: syn::Expr = meta.value()?.parse()?;
            result.retry = Some(value);
            Ok(())
        } else if meta.path.is_ident("mcp") {
            result.mcp = crate::attr_util::parse_bool_flag(&meta)?;
            Ok(())
        } else {
            Err(meta.error(
                "unsupported attribute: expected `execution_timeout`, `chain_execution_timeout`, `sla`, `concurrency`, `debounce`, `batch`, `throttle`, `max_input_bytes`, `owner`, `runbook`, `severity`, `description`, `retry`, `mcp`, or `allow_nondeterministic_apis`",
            ))
        }
    })
    .parse2(attr)?;

    Ok(result)
}

/// Whether the workflow's `Result<_, E>` error type `E` names `WorkflowFailure`
/// (issue #767). Mirrors `activity.rs::activity_returns_activity_failure`.
///
/// Detection is by the error type's **last path segment ident** (`== "WorkflowFailure"`,
/// with or without a `failure::` / `autumn_harvest::failure::` prefix), mirroring
/// `activity_returns_activity_failure`. A `use` alias or a rename of the type is
/// therefore not detected and falls back to the `.to_string()` path — a documented
/// limitation, consistent with the activity precedent.
fn workflow_returns_workflow_failure(output: &syn::ReturnType) -> bool {
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
        .is_some_and(|seg| seg.ident == "WorkflowFailure")
}

// ---------------------------------------------------------------------------
// Main macro
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_lines, clippy::cognitive_complexity)]
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

    let mut warnings_tokens = quote! {};

    let ctx_param_name = if let Some(syn::FnArg::Typed(pat_type)) = input_fn.sig.inputs.first() {
        if let syn::Pat::Ident(pat_ident) = &*pat_type.pat {
            Some(pat_ident.ident.to_string())
        } else {
            None
        }
    } else {
        None
    };

    if !attrs.allow_nondeterministic_apis {
        use syn::visit::Visit as _;
        let catalog = crate::determinism_lint::load_catalog_metadata();
        let mut visitor = crate::determinism_lint::DeterminismVisitor::new(catalog);
        visitor.context_param_name = ctx_param_name;
        visitor.visit_item_fn(&input_fn);

        let mut errors = Vec::new();
        for finding in visitor.findings {
            if finding.severity == "HardBlocker" {
                let compile_msg = format!(
                    "[{}] Workflow determinism violation: {}\nAlternative: {}",
                    finding.rule_id, finding.message, finding.alternative
                );
                errors.push(syn::Error::new(finding.span, compile_msg));
            } else if finding.severity == "Warning" {
                let warn_msg = format!(
                    "[{}] Workflow determinism warning: {}\nAlternative: {}",
                    finding.rule_id, finding.message, finding.alternative
                );
                let span = finding.span;
                let warn_tokens = quote::quote_spanned! { span =>
                    const _: () = {
                        #[deprecated(since = "0.3.0", note = #warn_msg)]
                        fn determinism_warning() {}
                        fn trigger() {
                            determinism_warning();
                        }
                    };
                };
                warnings_tokens.extend(warn_tokens);
            }
        }

        if !errors.is_empty() {
            let mut compile_errors = quote! {};
            for err in errors {
                compile_errors.extend(err.to_compile_error());
            }
            return quote! {
                #warnings_tokens
                #input_fn
                #compile_errors
            };
        }
    }

    let fn_name = &input_fn.sig.ident;
    let fn_name_str = fn_name.to_string();
    let companion_name = format_ident!("__autumn_workflow_info_{fn_name}");
    let public_info_name = format_ident!("{fn_name}_info");

    // Collect parameter names after the first (ctx is first, rest are inputs).
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

    // If the workflow returns `Result<_, WorkflowFailure>`, route the error
    // through `WorkflowFailure`'s `IntoWorkflowErrorString` impl so the engine
    // can recover the typed `error_type` / `details` / `non_retryable` from the
    // wire envelope. Otherwise the legacy `.to_string()` path is used, keeping
    // every `Result<_, String>` (or other `ToString` error) workflow unchanged.
    let returns_workflow_failure = workflow_returns_workflow_failure(&input_fn.sig.output);
    let encode_err = if returns_workflow_failure {
        quote! {
            |e| ::autumn_harvest::failure::IntoWorkflowErrorString::into_workflow_error_payload(e)
        }
    } else {
        quote! { |e| e.to_string() }
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
            result.map_err(#encode_err)
                .and_then(|v| {
                    ::autumn_harvest::serde_json::to_value(v)
                        .map_err(|e| e.to_string())
                })
        }
    };

    // Emit execution_timeout as Option<Duration> using the task_duration helper.
    let execution_timeout_expr = attrs.execution_timeout.as_deref().map_or_else(
        || quote! { None },
        |s| quote! { ::autumn_harvest::task_duration(#s) },
    );

    // Emit chain_execution_timeout as Option<Duration> (issue #617).
    let chain_execution_timeout_expr = attrs.chain_execution_timeout.as_deref().map_or_else(
        || quote! { None },
        |s| quote! { ::autumn_harvest::task_duration(#s) },
    );

    // Emit sla as Option<Duration> (issue #487).
    let sla_expr = attrs.sla.as_deref().map_or_else(
        || quote! { None },
        |s| quote! { ::autumn_harvest::task_duration(#s) },
    );

    // Emit concurrency as Option<ConcurrencyPolicy>.
    let concurrency_expr = match attrs.concurrency {
        None => quote! { ::std::option::Option::None },
        Some(ConcurrencyArgs { key_expr, limit }) => {
            quote! {
                ::std::option::Option::Some(
                    ::autumn_harvest::concurrency::ConcurrencyPolicy {
                        key_expr: #key_expr,
                        limit: #limit,
                    }
                )
            }
        }
    };

    // Emit debounce as Option<DebouncePolicy> (issue #499).
    let debounce_expr = match attrs.debounce {
        None => quote! { ::std::option::Option::None },
        Some(DebounceArgs {
            key_expr,
            window,
            max_wait,
        }) => {
            let max_wait_expr = max_wait.map_or_else(
                || quote! { ::std::option::Option::None },
                |s| {
                    quote! {
                        ::std::option::Option::Some(
                            ::autumn_harvest::task_duration(#s)
                                .expect("debounce max_wait must be a valid duration string")
                        )
                    }
                },
            );
            quote! {
                ::std::option::Option::Some(
                    ::autumn_harvest::debounce::DebouncePolicy {
                        key_expr: #key_expr,
                        window: ::autumn_harvest::task_duration(#window)
                            .expect("debounce window must be a valid duration string"),
                        max_wait: #max_wait_expr,
                    }
                )
            }
        }
    };

    // Emit batch as Option<BatchPolicy> (issue #518).
    let batch_expr = match attrs.batch {
        None => quote! { ::std::option::Option::None },
        Some(BatchArgs {
            key_expr,
            max_size,
            max_wait,
        }) => {
            quote! {
                ::std::option::Option::Some(
                    ::autumn_harvest::event_batch::BatchPolicy {
                        key_expr: #key_expr.to_string(),
                        max_size: #max_size as usize,
                        max_wait: ::autumn_harvest::task_duration(#max_wait)
                            .expect("batch max_wait must be a valid duration string"),
                    }
                )
            }
        }
    };

    // Emit throttle as Option<ThrottlePolicy> (issue #607). All parsing is
    // delegated to the core `from_rate_str` (single source of truth); the rate
    // and durations are already compile-time validated above.
    let throttle_expr = match attrs.throttle {
        None => quote! { ::std::option::Option::None },
        Some(ThrottleArgs {
            rate,
            burst,
            key,
            schedule_to_start,
        }) => {
            let burst_expr = burst.map_or_else(
                || quote! { ::std::option::Option::None },
                |b| quote! { ::std::option::Option::Some(#b) },
            );
            let key_expr = key.as_deref().map_or_else(
                || quote! { ::std::option::Option::None },
                |k| quote! { ::std::option::Option::Some(#k) },
            );
            let sts_expr = schedule_to_start.as_deref().map_or_else(
                || quote! { ::std::option::Option::None },
                |s| {
                    quote! {
                        ::std::option::Option::Some(
                            ::autumn_harvest::task_duration(#s)
                                .expect("throttle schedule_to_start must be a valid duration string")
                        )
                    }
                },
            );
            quote! {
                ::std::option::Option::Some(
                    ::autumn_harvest::throttle::ThrottlePolicy::from_rate_str(
                        #rate, #burst_expr, #key_expr, #sts_expr,
                    )
                    .expect("throttle rate must be a valid rate string")
                )
            }
        }
    };

    let max_input_bytes_expr = attrs
        .max_input_bytes
        .map_or_else(|| quote! { None }, |b| quote! { Some(#b) });

    let description_expr = attrs.description.as_deref().map_or_else(
        || quote! { ::std::option::Option::None },
        |s| quote! { ::std::option::Option::Some(#s) },
    );

    // Emit retry_policy as Option<RetryPolicy> (issue #523).
    let retry_policy_expr = attrs.retry.as_ref().map_or_else(
        || quote! { ::std::option::Option::None },
        |expr| quote! { ::std::option::Option::Some(#expr) },
    );

    let camel_name = to_pascal_case(&fn_name_str);
    let stub_name = format_ident!("{}Stub", camel_name);
    let ok_type = extract_ok_type(&input_fn.sig.output);

    let serialize_args = if param_names.is_empty() {
        quote! { ::autumn_harvest::serde_json::Value::Null }
    } else if param_names.len() == 1 {
        let name = &param_names[0];
        quote! { ::autumn_harvest::serde_json::to_value(&#name).map_err(::autumn_harvest::error::HarvestError::Serialization)? }
    } else {
        quote! { ::autumn_harvest::serde_json::to_value((#(&#param_names),*)).map_err(::autumn_harvest::error::HarvestError::Serialization)? }
    };

    let owner_expr = attrs
        .owner
        .as_deref()
        .map_or_else(|| quote! { None }, |s| quote! { Some(#s) });
    let runbook_url_expr = attrs
        .runbook
        .as_deref()
        .map_or_else(|| quote! { None }, |s| quote! { Some(#s) });
    let severity_expr = attrs
        .severity
        .as_deref()
        .map_or_else(|| quote! { None }, |s| quote! { Some(#s) });

    let mcp = attrs.mcp;
    let mcp_expr = quote! { #mcp };

    quote! {
        #warnings_tokens
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
                chain_execution_timeout: #chain_execution_timeout_expr,
                sla: #sla_expr,
                concurrency: #concurrency_expr,
                debounce: #debounce_expr,
                batch: #batch_expr,
                throttle: #throttle_expr,
                max_input_bytes: #max_input_bytes_expr,
                owner: #owner_expr,
                runbook_url: #runbook_url_expr,
                severity: #severity_expr,
                description: #description_expr,
                input_schema: ::std::option::Option::None,
                output_schema: ::std::option::Option::None,
                error_schema: ::std::option::Option::None,
                retry_policy: #retry_policy_expr,
                mcp: #mcp_expr,
            }
        }

        /// Returns the [`::autumn_harvest::WorkflowInfo`] for this workflow.
        ///
        /// Pass to typed dispatch helpers on [`::autumn_harvest::WorkflowContext`]:
        ///
        /// ```rust,ignore
        /// let result = ctx.spawn_child_workflow(&#public_info_name(), input).await?;
        /// ```
        pub fn #public_info_name() -> ::autumn_harvest::WorkflowInfo {
            #companion_name()
        }

        ::autumn_harvest::cfg_db! {
            /// Zero-cost typed client stub for the workflow.
            #[derive(Debug, Clone, Copy)]
            pub struct #stub_name;

            impl #stub_name {
                /// Get the WorkflowInfo for this workflow.
                pub fn info() -> ::autumn_harvest::WorkflowInfo {
                    #companion_name()
                }

                /// Start this workflow using default options and return an awaitable typed handle.
                pub async fn start(
                    conn: &mut ::autumn_harvest::diesel_async::AsyncPgConnection,
                    client: &::autumn_harvest::WorkflowHandleClient,
                    workflow_id: impl Into<::std::string::String>,
                    #(#params,)*
                ) -> ::autumn_harvest::HarvestResult<::autumn_harvest::TypedWorkflowHandle<#ok_type>> {
                    Self::start_with_options(
                        conn,
                        client,
                        workflow_id,
                        #(#param_names,)*
                        ::std::default::Default::default()
                    ).await
                }

                /// Start this workflow with custom options and return an awaitable typed handle.
                pub async fn start_with_options(
                    conn: &mut ::autumn_harvest::diesel_async::AsyncPgConnection,
                    client: &::autumn_harvest::WorkflowHandleClient,
                    workflow_id: impl Into<::std::string::String>,
                    #(#params,)*
                    opts: ::autumn_harvest::TypedStartOptions,
                ) -> ::autumn_harvest::HarvestResult<::autumn_harvest::TypedWorkflowHandle<#ok_type>> {
                    let workflow_id = workflow_id.into();
                    let input = #serialize_args;
                    let info = #public_info_name();
                    // Issue #499: debounce admission is owned exclusively by the
                    // HTTP start route (`POST /workflows/{name}/start`). That route
                    // has the registry + shard-router context the gate requires:
                    // it routes the pending record onto the *debounce-key's* shard,
                    // resolves the effective start options (SLA/timeout defaults +
                    // server ceilings, operator metadata), enforces the input cap,
                    // rejects `start_at`/`delay`, and preserves idempotent retries.
                    //
                    // The typed stub cannot reproduce that correctly: it only
                    // receives the workflow_id-derived `conn` (not the debounce-key
                    // shard's connection) and has no registry handle, and a deferred
                    // debounced start has no exec_id-keyed handle to return anyway.
                    // So rather than silently bypassing the policy or admitting onto
                    // the wrong shard, reject early with a clear pointer to the HTTP
                    // route. (Compile-time `#[workflow(debounce(...))]` is visible
                    // here via `info.debounce`; a fluent `.with_debounce(...)` policy
                    // is registry-only and is enforced by the HTTP route instead.)
                    if let ::std::option::Option::Some(debounce_policy) = info.debounce {
                        if ::autumn_harvest::debounce::resolve_debounce_key(
                            debounce_policy.key_expr,
                            &input,
                        )
                        .is_some()
                        {
                            return ::std::result::Result::Err(
                                ::autumn_harvest::error::HarvestError::Config(::std::format!(
                                    "workflow '{0}' has a debounce policy; debounced starts \
                                     must use the HTTP start route POST /workflows/{0}/start \
                                     (the typed client cannot express a deferred debounced start)",
                                    info.name,
                                )),
                            );
                        }
                    }
                    // Same rationale as debounce: a throttle defers excess starts,
                    // which the typed client cannot express (no exec_id-keyed handle
                    // exists for a deferred start). A keyed throttle applies only when
                    // its key resolves; an unkeyed (global) throttle always applies.
                    if let ::std::option::Option::Some(throttle_policy) = info.throttle {
                        let throttle_applies = match throttle_policy.key_expr {
                            ::std::option::Option::Some(k) => {
                                ::autumn_harvest::throttle::resolve_throttle_key(k, &input).is_some()
                            }
                            ::std::option::Option::None => true,
                        };
                        if throttle_applies {
                            return ::std::result::Result::Err(
                                ::autumn_harvest::error::HarvestError::Config(::std::format!(
                                    "workflow '{0}' has a start-throttle policy; throttled starts \
                                     must use the HTTP start route POST /workflows/{0}/start \
                                     (the typed client cannot express a deferred throttled start)",
                                    info.name,
                                )),
                            );
                        }
                    }
                    if let ::std::option::Option::Some(batch_policy) = info.batch.as_ref() {
                        if ::autumn_harvest::concurrency::resolve_concurrency_key(
                            &batch_policy.key_expr,
                            &input,
                        )
                        .is_some()
                        {
                            return ::std::result::Result::Err(
                                ::autumn_harvest::error::HarvestError::Config(::std::format!(
                                    "workflow '{0}' has an event batching policy; batched starts \
                                     must use the HTTP start route POST /workflows/{0}/start \
                                     (the typed client cannot express a deferred batched start)",
                                    info.name,
                                )),
                            );
                        }
                    }
                    if opts.batch.is_some() {
                        return ::std::result::Result::Err(
                            ::autumn_harvest::error::HarvestError::Config(::std::format!(
                                "workflow '{0}' start request specified a batch policy; batched starts \
                                 must use the HTTP start route POST /workflows/{0}/start \
                                 (the typed client cannot express a deferred batched start)",
                                info.name,
                            )),
                        );
                    }
                    let exec_id = opts.exec_id.unwrap_or_else(|| {
                        let shard = client.pick_shard_for_new_workflow(info.name, &workflow_id);
                        ::autumn_harvest::types::ExecutionId::new_for_shard(shard)
                    });

                    let (concurrency_key, concurrency_limit) = if let Some(ref policy) = info.concurrency {
                        let key = ::autumn_harvest::concurrency::resolve_concurrency_key(&policy.key_expr, &input);
                        (key, Some(policy.limit))
                    } else {
                        (None, None)
                    };

                    let execution_timeout = match opts.execution_timeout.or(info.execution_timeout) {
                        ::std::option::Option::Some(d) => ::std::option::Option::Some(
                            ::autumn_harvest::chrono::Duration::from_std(d)
                                .map_err(|_| ::autumn_harvest::error::HarvestError::Config(
                                    "execution_timeout exceeds chrono duration range".to_string()
                                ))?
                        ),
                        ::std::option::Option::None => ::std::option::Option::None,
                    };

                    let max_execution_timeout_ceiling = match client.max_workflow_execution_timeout() {
                        ::std::option::Option::Some(d) => ::std::option::Option::Some(
                            ::autumn_harvest::chrono::Duration::from_std(d)
                                .map_err(|_| ::autumn_harvest::error::HarvestError::Config(
                                    "max_execution_timeout_ceiling exceeds chrono duration range".to_string()
                                ))?
                        ),
                        ::std::option::Option::None => ::std::option::Option::None,
                    };

                    // Chain-scoped lifetime cap (issue #617). Resolved from the
                    // workflow-type default and the fleet-wide ceiling, at parity
                    // with the per-run `execution_timeout` above.
                    let chain_execution_timeout = match info.chain_execution_timeout {
                        ::std::option::Option::Some(d) => ::std::option::Option::Some(
                            ::autumn_harvest::chrono::Duration::from_std(d)
                                .map_err(|_| ::autumn_harvest::error::HarvestError::Config(
                                    "chain_execution_timeout exceeds chrono duration range".to_string()
                                ))?
                        ),
                        ::std::option::Option::None => ::std::option::Option::None,
                    };
                    let max_workflow_chain_timeout_ceiling = match client.max_workflow_chain_timeout() {
                        ::std::option::Option::Some(d) => ::std::option::Option::Some(
                            ::autumn_harvest::chrono::Duration::from_std(d)
                                .map_err(|_| ::autumn_harvest::error::HarvestError::Config(
                                    "max_workflow_chain_timeout_ceiling exceeds chrono duration range".to_string()
                                ))?
                        ),
                        ::std::option::Option::None => ::std::option::Option::None,
                    };

                    let max_workflow_start_delay = {
                        let ceiling_chrono = ::autumn_harvest::chrono::Duration::from_std(client.max_workflow_start_delay())
                            .map_err(|_| ::autumn_harvest::error::HarvestError::Config(
                                "max_workflow_start_delay ceiling exceeds chrono duration range".to_string()
                            ))?;
                        let requested_chrono = opts.max_workflow_start_delay.unwrap_or(ceiling_chrono);
                        requested_chrono.min(ceiling_chrono)
                    };

                    let params = ::autumn_harvest::execution::StartWorkflowParams {
                        workflow_name: info.name,
                        workflow_id: &workflow_id,
                        exec_id,
                        input,
                        parent_id: opts.parent_id,
                        queue_name: opts.queue_name.as_deref().unwrap_or("default"),
                        execution_timeout,
                        memo: opts.memo,
                        search_attrs: opts.search_attrs,
                        reuse_policy: opts.reuse_policy.unwrap_or(::autumn_harvest::types::WorkflowIdReusePolicy::AllowDuplicate),
                        conflict_policy: ::autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
                        trace_context: opts.trace_context,
                        max_execution_timeout_ceiling,
                        chain_execution_timeout,
                        max_workflow_chain_timeout_ceiling,
                        inherited_chain_deadline_at: ::std::option::Option::None,
                        concurrency_key,
                        concurrency_limit,
                        priority: opts.priority.unwrap_or_default(),
                        max_workflow_input_bytes: client.max_workflow_input_bytes(info.max_input_bytes),
                        start_at: opts.start_at,
                        delay: opts.delay,
                        max_workflow_start_delay: ::std::option::Option::Some(max_workflow_start_delay),
                        owner: info.owner,
                        runbook_url: info.runbook_url,
                        severity: info.severity,
                        context_headers: opts.context_headers,
                        sla: opts.sla.or(info.sla).and_then(|d|
                            ::autumn_harvest::chrono::Duration::from_std(d).ok()
                        ),
                        schedule_id: ::std::option::Option::None,
                        scheduled_for: ::std::option::Option::None,
                        workflow_attempt: 1,
                        workflow_retry_policy: info.retry_policy.clone(),
                        retry_of_exec_id: ::std::option::Option::None,
                        max_workflow_attempts_ceiling: client.max_workflow_attempts(),
                        origin: None,
                        completion_callbacks: ::std::option::Option::None,
                        start_source: ::autumn_harvest::types::StartSource::Api,
                        start_source_ref: ::std::option::Option::None,
                        started_by: ::std::option::Option::None,
                    };

                    let started = client.start_or_load(conn, params).await?;
                    Ok(::autumn_harvest::TypedWorkflowHandle::new(started.handle))
                }

                /// Atomically start a workflow and queue a signal, or attach the signal to a running execution.
                pub async fn signal_with_start<S>(
                    conn: &mut ::autumn_harvest::diesel_async::AsyncPgConnection,
                    client: &::autumn_harvest::WorkflowHandleClient,
                    workflow_id: impl Into<::std::string::String>,
                    #(#params,)*
                    signal_name: impl Into<::std::string::String>,
                    signal_payload: S,
                    opts: ::autumn_harvest::TypedSignalWithStartOptions,
                ) -> ::autumn_harvest::HarvestResult<::autumn_harvest::TypedWorkflowHandle<#ok_type>>
                where
                    S: ::autumn_harvest::serde::Serialize,
                {
                    let workflow_id = workflow_id.into();
                    let input = #serialize_args;
                    let info = #public_info_name();
                    // Issue #499: debounce admission is owned exclusively by the HTTP
                    // start route (see `start_with_options` for the rationale). A
                    // debounced workflow must not be started — or signal-with-started —
                    // through the typed client, which cannot route to the debounce-key
                    // shard or admit through the gate. Reject with a pointer to HTTP.
                    if let ::std::option::Option::Some(debounce_policy) = info.debounce {
                        if ::autumn_harvest::debounce::resolve_debounce_key(
                            debounce_policy.key_expr,
                            &input,
                        )
                        .is_some()
                        {
                            return ::std::result::Result::Err(
                                ::autumn_harvest::error::HarvestError::Config(::std::format!(
                                    "workflow '{0}' has a debounce policy; debounced starts \
                                     must use the HTTP start route POST /workflows/{0}/start \
                                     (the typed client cannot express a deferred debounced start)",
                                    info.name,
                                )),
                            );
                        }
                    }
                    // Same rationale as debounce: a throttle defers excess starts,
                    // which the typed client cannot express (no exec_id-keyed handle
                    // exists for a deferred start). A keyed throttle applies only when
                    // its key resolves; an unkeyed (global) throttle always applies.
                    if let ::std::option::Option::Some(throttle_policy) = info.throttle {
                        let throttle_applies = match throttle_policy.key_expr {
                            ::std::option::Option::Some(k) => {
                                ::autumn_harvest::throttle::resolve_throttle_key(k, &input).is_some()
                            }
                            ::std::option::Option::None => true,
                        };
                        if throttle_applies {
                            return ::std::result::Result::Err(
                                ::autumn_harvest::error::HarvestError::Config(::std::format!(
                                    "workflow '{0}' has a start-throttle policy; throttled starts \
                                     must use the HTTP start route POST /workflows/{0}/start \
                                     (the typed client cannot express a deferred throttled start)",
                                    info.name,
                                )),
                            );
                        }
                    }
                    if let ::std::option::Option::Some(batch_policy) = info.batch.as_ref() {
                        if ::autumn_harvest::concurrency::resolve_concurrency_key(
                            &batch_policy.key_expr,
                            &input,
                        )
                        .is_some()
                        {
                            return ::std::result::Result::Err(
                                ::autumn_harvest::error::HarvestError::Config(::std::format!(
                                    "workflow '{0}' has an event batching policy; batched starts \
                                     must use the HTTP start route POST /workflows/{0}/start \
                                     (the typed client cannot express a deferred batched start)",
                                    info.name,
                                )),
                            );
                        }
                    }
                    let exec_id = opts.exec_id.unwrap_or_else(|| {
                        let shard = client.pick_shard_for_new_workflow(info.name, &workflow_id);
                        ::autumn_harvest::types::ExecutionId::new_for_shard(shard)
                    });

                    let (concurrency_key, concurrency_limit) = if let Some(ref policy) = info.concurrency {
                        let key = ::autumn_harvest::concurrency::resolve_concurrency_key(&policy.key_expr, &input);
                        (key, Some(policy.limit))
                    } else {
                        (None, None)
                    };

                    let execution_timeout = match opts.execution_timeout.or(info.execution_timeout) {
                        ::std::option::Option::Some(d) => ::std::option::Option::Some(
                            ::autumn_harvest::chrono::Duration::from_std(d)
                                .map_err(|_| ::autumn_harvest::error::HarvestError::Config(
                                    "execution_timeout exceeds chrono duration range".to_string()
                                ))?
                        ),
                        ::std::option::Option::None => ::std::option::Option::None,
                    };

                    let max_execution_timeout_ceiling = match client.max_workflow_execution_timeout() {
                        ::std::option::Option::Some(d) => ::std::option::Option::Some(
                            ::autumn_harvest::chrono::Duration::from_std(d)
                                .map_err(|_| ::autumn_harvest::error::HarvestError::Config(
                                    "max_execution_timeout_ceiling exceeds chrono duration range".to_string()
                                ))?
                        ),
                        ::std::option::Option::None => ::std::option::Option::None,
                    };

                    // Note (issue #617): this typed-stub signal-with-start passes
                    // `None` for the two chain-cap fields below, so it does NOT
                    // apply the chain-scoped lifetime cap. The chain cap (both the
                    // workflow-type default and the fleet-wide ceiling-as-default)
                    // IS resolved on the HTTP signal-with-start route and on this
                    // stub's own `start`/`start_with_options` path (via
                    // `StartWorkflowParams`), plus the scheduler/backfill start
                    // paths — never here.

                    let payload = ::autumn_harvest::serde_json::to_value(&signal_payload)
                        .map_err(::autumn_harvest::error::HarvestError::Serialization)?;

                    let params = ::autumn_harvest::execution::SignalWithStartParams {
                        workflow_name: info.name,
                        workflow_id: &workflow_id,
                        exec_id,
                        input,
                        parent_id: opts.parent_id,
                        queue_name: opts.queue_name.as_deref().unwrap_or("default"),
                        execution_timeout,
                        memo: opts.memo,
                        search_attrs: opts.search_attrs,
                        reuse_policy: opts.reuse_policy.unwrap_or(::autumn_harvest::types::WorkflowIdReusePolicy::AllowDuplicate),
                        trace_context: opts.trace_context,
                        max_execution_timeout_ceiling,
                        // Chain-scoped lifetime cap (issue #617): the typed-stub
                        // signal-with-start does NOT thread the chain cap. It is
                        // resolved on the HTTP signal-with-start route and on the
                        // typed stub's own `start`/`start_with_options` path.
                        chain_execution_timeout: ::std::option::Option::None,
                        max_workflow_chain_timeout_ceiling: ::std::option::Option::None,
                        concurrency_key,
                        concurrency_limit,
                        signal_name: &signal_name.into(),
                        signal_payload: payload,
                        idempotency_key: opts.idempotency_key,
                        max_workflow_input_bytes: client.max_workflow_input_bytes(info.max_input_bytes),
                        max_signal_payload_bytes: opts.max_signal_payload_bytes.unwrap_or_else(|| client.max_signal_payload_bytes()),
                        owner: info.owner,
                        runbook_url: info.runbook_url,
                        severity: info.severity,
                        context_headers: opts.context_headers,
                        sla: opts.sla.or(info.sla).and_then(|d|
                            ::autumn_harvest::chrono::Duration::from_std(d).ok()
                        ),
                        // Typed stubs already reject debounced workflows up front.
                        reject_fresh_if_debounced: false,
                        workflow_retry_policy: info.retry_policy
                            .and_then(|p| ::autumn_harvest::serde_json::to_value(&p).ok()),
                        max_workflow_attempts_ceiling: client.max_workflow_attempts(),
                        // Schema validation (issue #373) is an HTTP-JSON-boundary
                        // concern only: a typed stub caller's `input: I` is already
                        // checked by Rust's type system, so it never needs runtime
                        // schema validation -- consistent with every other
                        // `validate_input` call site being in the HTTP handlers.
                        workflow_info: None,
                        start_source_override: None,
                        start_source_ref_override: None,
                    };

                    let outcome = ::autumn_harvest::execution::signal_with_start_workflow_execution(conn, params).await?;
                    Ok(::autumn_harvest::TypedWorkflowHandle::new(client.handle(outcome.exec_id)))
                }
            }
        }
    }
}

fn to_pascal_case(s: &str) -> String {
    crate::to_pascal_case(s)
}

fn extract_ok_type(output: &syn::ReturnType) -> syn::Type {
    crate::extract_ok_type(output)
}

#[cfg(test)]
mod duration_validation_tests {
    use super::is_valid_task_duration;

    #[test]
    fn accepts_valid_durations() {
        for s in ["30s", "5m", "1h", "2d", "1h30m", "90s", "0s5s"] {
            assert!(is_valid_task_duration(s), "should accept '{s}'");
        }
    }

    #[test]
    fn rejects_invalid_durations() {
        // empty, zero, unknown unit, missing unit, trailing digits, garbage.
        for s in [
            "", "0s", "0", "5minutes", "5", "30", "abc", "5x", "-5s", "5s ",
        ] {
            // note: "5s " has a trailing space which task_duration tolerates, so
            // exclude it from the reject set below by testing it separately.
            if s == "5s " {
                assert!(is_valid_task_duration(s), "trailing space is tolerated");
                continue;
            }
            assert!(!is_valid_task_duration(s), "should reject '{s}'");
        }
    }
}

#[cfg(test)]
mod rate_validation_tests {
    use super::is_valid_rate;

    #[test]
    fn accepts_valid_rates() {
        for s in ["100/m", "1/s", "3600/h", "0.5/s"] {
            assert!(is_valid_rate(s), "should accept '{s}'");
        }
    }

    #[test]
    fn rejects_invalid_rates() {
        for s in [
            "100",
            "abc/m",
            "100/x",
            "0/m",
            "-5/m",
            "inf/m",
            "infinity/s",
            "-inf/m",
            "nan/m",
        ] {
            assert!(!is_valid_rate(s), "should reject '{s}'");
        }
    }
}
