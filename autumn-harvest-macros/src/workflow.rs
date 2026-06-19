//! `#[workflow]` attribute macro implementation.
//!
//! Emits the original function unchanged plus a companion:
//!   `pub fn __autumn_workflow_info_{name}() -> ::autumn_harvest::WorkflowInfo`

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{ItemFn, LitStr, parse::Parser as _};

use crate::parse_byte_size_macro;

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

struct WorkflowAttrs {
    execution_timeout: Option<String>,
    /// Soft SLA budget (issue #487). Parsed from `#[workflow(sla = "2h")]`.
    /// Stored as `WorkflowInfo::sla: Option<Duration>`.
    sla: Option<String>,
    concurrency: Option<ConcurrencyArgs>,
    /// Trailing-edge debounce policy (issue #499). Parsed from
    /// `#[workflow(debounce(key = "input.tenant_id", window = "30s", max_wait = "5m"))]`.
    debounce: Option<DebounceArgs>,
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
}

#[allow(clippy::too_many_lines)]
fn parse_attrs(attr: TokenStream) -> syn::Result<WorkflowAttrs> {
    let mut result = WorkflowAttrs {
        execution_timeout: None,
        sla: None,
        concurrency: None,
        debounce: None,
        max_input_bytes: None,
        owner: None,
        runbook: None,
        severity: None,
        description: None,
        allow_nondeterministic_apis: false,
    };

    syn::meta::parser(|meta| {
        if meta.path.is_ident("execution_timeout") {
            let value: LitStr = meta.value()?.parse()?;
            result.execution_timeout = Some(value.value());
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
                    window = Some(value.value());
                    Ok(())
                } else if inner.path.is_ident("max_wait") {
                    let value: LitStr = inner.value()?.parse()?;
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
        } else if meta.path.is_ident("allow_nondeterministic_apis") {
            if meta.input.peek(syn::Token![=]) {
                let value: syn::LitBool = meta.value()?.parse()?;
                result.allow_nondeterministic_apis = value.value;
            } else {
                result.allow_nondeterministic_apis = true;
            }
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
        } else {
            Err(meta.error(
                "unsupported attribute: expected `execution_timeout`, `sla`, `concurrency`, `debounce`, `max_input_bytes`, `owner`, `runbook`, `severity`, `description`, or `allow_nondeterministic_apis`",
            ))
        }
    })
    .parse2(attr)?;

    Ok(result)
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
    let execution_timeout_expr = attrs.execution_timeout.as_deref().map_or_else(
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

    let max_input_bytes_expr = attrs
        .max_input_bytes
        .map_or_else(|| quote! { None }, |b| quote! { Some(#b) });

    let description_expr = attrs.description.as_deref().map_or_else(
        || quote! { ::std::option::Option::None },
        |s| quote! { ::std::option::Option::Some(#s) },
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
                sla: #sla_expr,
                concurrency: #concurrency_expr,
                debounce: #debounce_expr,
                max_input_bytes: #max_input_bytes_expr,
                owner: #owner_expr,
                runbook_url: #runbook_url_expr,
                severity: #severity_expr,
                description: #description_expr,
                input_schema: ::std::option::Option::None,
                output_schema: ::std::option::Option::None,
                error_schema: ::std::option::Option::None,
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
                        trace_context: opts.trace_context,
                        max_execution_timeout_ceiling,
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
