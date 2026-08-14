//! `#[dag]` attribute macro implementation.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{ItemFn, LitBool, LitInt, LitStr, parse::Parser as _};

#[derive(Debug, Default)]
struct DagAttrs {
    schedule: Option<String>,
    catchup: bool,
    max_active_runs: u32,
    default_queue: Option<String>,
    jitter: Option<String>,
    owner: Option<String>,
    runbook: Option<String>,
    severity: Option<String>,
    mcp: bool,
    /// `#[dag(execution_timeout = "4h")]` (issue #743): a hard wall-clock
    /// deadline for the whole scheduled DAG run, propagated to the shadow
    /// `WorkflowInfo::execution_timeout` and enforced by the EXISTING #243
    /// `enforce_workflow_execution_timeouts` scanner — no new scanner.
    execution_timeout: Option<String>,
    /// `#[dag(sla = "3h")]` (issue #743): a soft SLA observed by the EXISTING
    /// #487 `enforce_workflow_sla_breaches` scanner without altering the run's
    /// lifecycle. Clamped down to `execution_timeout` at start when declared
    /// larger than it (AC5, via the shared `clamp_info_default_sla` logic).
    sla: Option<String>,
}

fn parse_attrs(attr: TokenStream) -> syn::Result<DagAttrs> {
    let mut result = DagAttrs {
        max_active_runs: 1,
        ..DagAttrs::default()
    };

    syn::meta::parser(|meta| {
        if meta.path.is_ident("schedule") {
            let value: LitStr = meta.value()?.parse()?;
            result.schedule = Some(value.value());
            Ok(())
        } else if meta.path.is_ident("catchup") {
            let value: LitBool = meta.value()?.parse()?;
            result.catchup = value.value;
            Ok(())
        } else if meta.path.is_ident("max_active_runs") {
            let value: LitInt = meta.value()?.parse()?;
            result.max_active_runs = value.base10_parse()?;
            Ok(())
        } else if meta.path.is_ident("default_queue") {
            let value: LitStr = meta.value()?.parse()?;
            result.default_queue = Some(value.value());
            Ok(())
        } else if meta.path.is_ident("jitter") {
            let value: LitStr = meta.value()?.parse()?;
            result.jitter = Some(value.value());
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
        } else if meta.path.is_ident("mcp") {
            result.mcp = crate::attr_util::parse_bool_flag(&meta)?;
            Ok(())
        } else if meta.path.is_ident("execution_timeout") {
            let value: LitStr = meta.value()?.parse()?;
            // Validate at compile time (issue #743 AC10) so a typo is a build
            // error, not a registration-time panic from the emitted duration
            // parse.
            if !crate::attr_util::is_valid_task_duration(&value.value()) {
                return Err(syn::Error::new_spanned(
                    &value,
                    "invalid execution_timeout duration; expected e.g. \"30s\", \"5m\", \"4h\", \"2d\"",
                ));
            }
            result.execution_timeout = Some(value.value());
            Ok(())
        } else if meta.path.is_ident("sla") {
            let value: LitStr = meta.value()?.parse()?;
            if !crate::attr_util::is_valid_task_duration(&value.value()) {
                return Err(syn::Error::new_spanned(
                    &value,
                    "invalid sla duration; expected e.g. \"30s\", \"5m\", \"3h\", \"2d\"",
                ));
            }
            result.sla = Some(value.value());
            Ok(())
        } else {
            Err(meta.error(
                "unsupported attribute: expected schedule, catchup, max_active_runs, default_queue, jitter, owner, runbook, severity, mcp, execution_timeout, or sla",
            ))
        }
    })
    .parse2(attr)?;

    Ok(result)
}

#[allow(clippy::too_many_lines)]
pub fn dag_macro(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attrs = match parse_attrs(attr) {
        Ok(attrs) => attrs,
        Err(error) => return error.to_compile_error(),
    };

    let input_fn: ItemFn = match syn::parse2(item) {
        Ok(function) => function,
        Err(error) => return error.to_compile_error(),
    };

    if input_fn.sig.asyncness.is_some() {
        return syn::Error::new_spanned(
            input_fn.sig.fn_token,
            "#[dag] functions must not be async",
        )
        .to_compile_error();
    }

    let fn_name = &input_fn.sig.ident;
    let fn_name_str = fn_name.to_string();
    let dag_companion_name = format_ident!("__autumn_dag_info_{fn_name}");

    let schedule_expr = attrs.schedule.as_deref().map_or_else(
        || quote! { None },
        |expr| quote! { Some(::autumn_harvest::Schedule::Cron(#expr.to_string())) },
    );
    let catchup = attrs.catchup;
    let max_active_runs = attrs.max_active_runs;
    let default_queue = attrs
        .default_queue
        .as_deref()
        .map_or_else(|| quote! { None }, |queue| quote! { Some(#queue) });

    let jitter_expr = attrs.jitter.as_deref().map_or_else(
        || quote! { ::std::time::Duration::ZERO },
        |s| {
            quote! {
                ::autumn_harvest::task_duration(#s)
                    .expect(concat!("invalid jitter duration string: ", #s))
            }
        },
    );

    // Emit execution_timeout/sla as Option<Duration> (issue #743). Already
    // validated for parseability in `parse_attrs`, so `task_duration` here
    // always succeeds -- mirrors `#[workflow(execution_timeout = ...)]`'s
    // emission in `workflow.rs`.
    let execution_timeout_expr = attrs.execution_timeout.as_deref().map_or_else(
        || quote! { None },
        |s| quote! { ::autumn_harvest::task_duration(#s) },
    );
    let sla_expr = attrs.sla.as_deref().map_or_else(
        || quote! { None },
        |s| quote! { ::autumn_harvest::task_duration(#s) },
    );

    // Emit the shadow WorkflowInfo companion when the unified-dag-execution
    // feature is enabled on the proc-macro crate (transitively enabled by
    // `autumn-harvest/unified-dag-execution`).
    #[cfg(feature = "unified-dag-execution")]
    let workflow_companion = emit_workflow_companion(fn_name, &fn_name_str, &attrs);

    #[cfg(not(feature = "unified-dag-execution"))]
    let workflow_companion = quote! {};

    // Inline the workflow handler into `DagInfo::workflow_handler` so the
    // runtime can route new runs through the unified path without a separate
    // companion function lookup.
    //
    // The level walker itself lives in exactly one place —
    // `::autumn_harvest::dag::run_unified_dag` — so this inlined handler and the
    // shadow `WorkflowInfo::handler` (see `emit_workflow_companion`) can never
    // drift. The `DagBuilder` (which holds `Rc<RefCell<..>>` and is not `Send`)
    // is built and dropped in a scoped block before the walk's first `.await`.
    #[cfg(feature = "unified-dag-execution")]
    let workflow_handler_field = {
        let builder_init_for_field = attrs.default_queue.as_deref().map_or_else(
            || quote! { ::autumn_harvest::DagBuilder::new() },
            |q| quote! { ::autumn_harvest::DagBuilder::with_default_queue(#q) },
        );
        quote! {
            workflow_handler: Some(|ctx, _input| {
                ::std::boxed::Box::pin(async move {
                    let (__levels, __tasks): (
                        ::std::vec::Vec<::std::vec::Vec<usize>>,
                        ::std::vec::Vec<::autumn_harvest::DagTask>,
                    ) = {
                        let mut __dag_builder = #builder_init_for_field;
                        #fn_name(&mut __dag_builder);
                        let __definition = __dag_builder
                            .build()
                            .map_err(|e| e.to_string())?;
                        (
                            __definition.execution_levels().to_vec(),
                            __definition.tasks().to_vec(),
                        )
                    };
                    ::autumn_harvest::dag::run_unified_dag(ctx, _input, __levels, __tasks).await
                })
            }),
        }
    };

    #[cfg(not(feature = "unified-dag-execution"))]
    let workflow_handler_field = quote! {
        workflow_handler: None,
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

    quote! {
        #input_fn

        #[doc(hidden)]
        pub fn #dag_companion_name() -> ::autumn_harvest::DagInfo {
            ::autumn_harvest::DagInfo {
                name: #fn_name_str,
                module: module_path!(),
                schedule: #schedule_expr,
                catchup: #catchup,
                max_active_runs: #max_active_runs,
                default_queue: #default_queue,
                builder: |dag| {
                    #fn_name(dag);
                },
                #workflow_handler_field
                jitter: #jitter_expr,
                overlap_policy: ::autumn_harvest::OverlapPolicy::Skip,
                buffer_all_max: 100u32,
                owner: #owner_expr,
                runbook_url: #runbook_url_expr,
                severity: #severity_expr,
                mcp: #mcp,
                execution_timeout: #execution_timeout_expr,
                sla: #sla_expr,
            }
        }

        #workflow_companion
    }
}

/// Build the token stream for the shadow `__autumn_workflow_info_{name}()`
/// companion function (only compiled when `unified-dag-execution` is on).
#[cfg(feature = "unified-dag-execution")]
fn emit_workflow_companion(
    fn_name: &syn::Ident,
    fn_name_str: &str,
    attrs: &DagAttrs,
) -> TokenStream {
    let companion_name = format_ident!("__autumn_workflow_info_{fn_name}");

    let builder_init = attrs.default_queue.as_deref().map_or_else(
        || quote! { ::autumn_harvest::DagBuilder::new() },
        |q| quote! { ::autumn_harvest::DagBuilder::with_default_queue(#q) },
    );

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
    // issue #743: the shadow WorkflowInfo must carry the SAME
    // execution_timeout/sla the DagInfo companion carries, so the unified
    // start path (`start_or_load_workflow_execution`) resolves an identical
    // deadline/sla regardless of whether the run was dispatched via the DAG
    // trigger route or a plain workflow start.
    let execution_timeout_expr = attrs.execution_timeout.as_deref().map_or_else(
        || quote! { None },
        |s| quote! { ::autumn_harvest::task_duration(#s) },
    );
    let sla_expr = attrs.sla.as_deref().map_or_else(
        || quote! { ::std::option::Option::None },
        |s| quote! { ::autumn_harvest::task_duration(#s) },
    );

    quote! {
        /// Shadow `WorkflowInfo` for this DAG, emitted when the
        /// `unified-dag-execution` feature is enabled (issue #256 Step 1).
        ///
        /// The handler builds the compiled [`DagDefinition`] in a scoped block
        /// (so the non-`Send` `DagBuilder` is dropped before any `.await`), then
        /// hands the `(levels, tasks)` to the single shared level walker
        /// `::autumn_harvest::dag::run_unified_dag`. That walker dispatches each
        /// activity through `ctx.execute_activity_raw_with_opts`, evaluates
        /// trigger rules / conditions from the accumulated task statuses, and
        /// awaits any signal/timer gate node — deterministic and replay-safe.
        #[doc(hidden)]
        pub fn #companion_name() -> ::autumn_harvest::WorkflowInfo {
            ::autumn_harvest::WorkflowInfo {
                name: #fn_name_str,
                module: module_path!(),
                handler: |ctx, _input| {
                    ::std::boxed::Box::pin(async move {
                        // Build the DagDefinition inside a scoped block so that
                        // `DagBuilder` (which holds `Rc<RefCell<...>>` and is
                        // not `Send`) is dropped before any `.await` point.
                        let (__levels, __tasks): (
                            ::std::vec::Vec<::std::vec::Vec<usize>>,
                            ::std::vec::Vec<::autumn_harvest::DagTask>,
                        ) = {
                            let mut __dag_builder = #builder_init;
                            #fn_name(&mut __dag_builder);
                            let __definition = __dag_builder
                                .build()
                                .map_err(|e| e.to_string())?;
                            (
                                __definition.execution_levels().to_vec(),
                                __definition.tasks().to_vec(),
                            )
                            // __dag_builder and __definition are dropped here.
                        };

                        ::autumn_harvest::dag::run_unified_dag(ctx, _input, __levels, __tasks).await
                    })
                },
                execution_timeout: #execution_timeout_expr,
                // DAGs carry no chain-scoped lifetime cap (issue #617).
                chain_execution_timeout: None,
                sla: #sla_expr,
                concurrency: ::std::option::Option::None,
                debounce: ::std::option::Option::None,
                batch: ::std::option::Option::None,
                throttle: ::std::option::Option::None,
                max_input_bytes: ::std::option::Option::None,
                owner: #owner_expr,
                runbook_url: #runbook_url_expr,
                severity: #severity_expr,
                description: ::std::option::Option::None,
                input_schema: ::std::option::Option::None,
                output_schema: ::std::option::Option::None,
                error_schema: ::std::option::Option::None,
                retry_policy: ::std::option::Option::None,
                // `#[dag(mcp)]` opt-in (issue #601 follow-up): the DAG's
                // `start`/`status`/`watch` MCP tools are generated from this
                // exact `WorkflowInfo.mcp` flag by
                // `mcp_tools::collect_descriptors`, mirroring `#[workflow(mcp)]`.
                // The DAG-specific trigger contract (admission gates,
                // `max_active_runs`) is preserved separately: the MCP
                // generator detects this is a DAG via `DagInfo` and routes
                // its `start` tool through `trigger_dag_run` rather than the
                // generic `start_workflow` path.
                mcp: #mcp,
                // Issue #802 targets the IMPERATIVE path: a `#[dag]` already
                // declares its activity references structurally, and preflight
                // validates those through `dag_unregistered_activity_failures`.
                // Opting the shadow `WorkflowInfo` in as well would make the
                // workflow pass re-report the same misses, doubling every DAG
                // failure in `details.failures`. Mirrors
                // `DagInfo::as_workflow_info`.
                declared_activities: ::std::option::Option::None,
                declared_children: ::std::option::Option::None,
            }
        }
    }
}
