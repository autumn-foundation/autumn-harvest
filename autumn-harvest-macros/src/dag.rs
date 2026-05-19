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
        } else {
            Err(meta.error(
                "unsupported attribute: expected schedule, catchup, max_active_runs, default_queue, or jitter",
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

    // Emit the shadow WorkflowInfo companion when the unified-dag-execution
    // feature is enabled on the proc-macro crate (transitively enabled by
    // `autumn-harvest/unified-dag-execution`).
    #[cfg(feature = "unified-dag-execution")]
    let workflow_companion =
        emit_workflow_companion(fn_name, &fn_name_str, attrs.default_queue.as_ref());

    #[cfg(not(feature = "unified-dag-execution"))]
    let workflow_companion = quote! {};

    // Inline the workflow handler into `DagInfo::workflow_handler` so the
    // runtime can route new runs through the unified path without a separate
    // companion function lookup.
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

                    let __n = __tasks.len();
                    let mut __statuses: ::std::vec::Vec<
                        ::std::option::Option<::autumn_harvest::policy::TaskStatus>,
                    > = vec![None; __n];

                    for __level in &__levels {
                        let mut __activity_futs = ::std::vec::Vec::new();

                        for &__task_idx in __level {
                            let __activity_name: ::std::string::String =
                                __tasks[__task_idx].activity_name.clone();
                            let __queue_str: ::std::string::String = __tasks[__task_idx]
                                .queue
                                .clone()
                                .unwrap_or_default();
                            let __upstreams: ::std::vec::Vec<usize> =
                                __tasks[__task_idx].upstreams.clone();
                            let __trigger_rule = __tasks[__task_idx].trigger_rule.clone();
                            let __retry_override = __tasks[__task_idx].retry_policy.clone();
                            let __stc_override = __tasks[__task_idx].start_to_close;

                            let __ups: ::std::vec::Vec<
                                ::autumn_harvest::policy::TaskStatus,
                            > = __upstreams
                                .iter()
                                .map(|&__i| {
                                    __statuses[__i].unwrap_or(
                                        ::autumn_harvest::policy::TaskStatus::Skipped,
                                    )
                                })
                                .collect();
                            let __should_run: bool = __trigger_rule.should_run(&__ups);

                            if !__should_run {
                                __statuses[__task_idx] = Some(
                                    ::autumn_harvest::policy::TaskStatus::Skipped,
                                );
                                continue;
                            }

                            let __activity_input = match _input.clone() {
                                ::autumn_harvest::serde_json::Value::Object(mut __object) => {
                                    __object.insert(
                                        "dag_task".to_owned(),
                                        ::autumn_harvest::serde_json::Value::String(
                                            __activity_name.clone(),
                                        ),
                                    );
                                    ::autumn_harvest::serde_json::Value::Object(__object)
                                }
                                __conf => {
                                    let mut __object =
                                        ::autumn_harvest::serde_json::Map::new();
                                    __object.insert("conf".to_owned(), __conf);
                                    __object.insert(
                                        "dag_task".to_owned(),
                                        ::autumn_harvest::serde_json::Value::String(
                                            __activity_name.clone(),
                                        ),
                                    );
                                    ::autumn_harvest::serde_json::Value::Object(__object)
                                }
                            };

                            __activity_futs.push(async move {
                                let __status = match ctx
                                    .execute_activity_raw_with_opts(
                                        &__activity_name,
                                        __activity_input,
                                        &__queue_str,
                                        __retry_override,
                                        __stc_override,
                                    )
                                    .await
                                {
                                    Ok(_) => ::autumn_harvest::policy::TaskStatus::Succeeded,
                                    Err(
                                        ::autumn_harvest::HarvestError::ActivityFailed { .. }
                                        | ::autumn_harvest::HarvestError::Timeout { .. },
                                    ) => {
                                        ::autumn_harvest::policy::TaskStatus::Failed
                                    }
                                    Err(__error) => return Err(__error.to_string()),
                                };
                                Ok::<_, ::std::string::String>((__task_idx, __status))
                            });
                        }

                        for __activity_result in
                            ::autumn_harvest::futures::future::join_all(__activity_futs).await
                        {
                            let (__task_idx, __status) = __activity_result?;
                            __statuses[__task_idx] = Some(__status);
                        }
                    }

                    let __any_failed = __statuses.iter().any(|s| {
                        matches!(s, Some(::autumn_harvest::policy::TaskStatus::Failed))
                    });
                    if __any_failed {
                        return Err("one or more DAG tasks failed".to_owned());
                    }

                    Ok(::autumn_harvest::serde_json::Value::Null)
                })
            }),
        }
    };

    #[cfg(not(feature = "unified-dag-execution"))]
    let workflow_handler_field = quote! {
        workflow_handler: None,
    };

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
            }
        }

        #workflow_companion
    }
}

/// Build the token stream for the shadow `__autumn_workflow_info_{name}()`
/// companion function (only compiled when `unified-dag-execution` is on).
#[cfg(feature = "unified-dag-execution")]
#[allow(clippy::too_many_lines)]
fn emit_workflow_companion(
    fn_name: &syn::Ident,
    fn_name_str: &str,
    default_queue: Option<&String>,
) -> TokenStream {
    let companion_name = format_ident!("__autumn_workflow_info_{fn_name}");

    let builder_init = default_queue.map(String::as_str).map_or_else(
        || quote! { ::autumn_harvest::DagBuilder::new() },
        |q| quote! { ::autumn_harvest::DagBuilder::with_default_queue(#q) },
    );

    quote! {
        /// Shadow `WorkflowInfo` for this DAG, emitted when the
        /// `unified-dag-execution` feature is enabled (issue #256 Step 1).
        ///
        /// The handler walks the compiled [`DagDefinition`] level by level,
        /// dispatches each activity through `ctx.execute_activity_raw`, and
        /// evaluates trigger rules from the accumulated task statuses so the
        /// execution is deterministic and replay-safe.
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

                        // Per-task status accumulator indexed by task index.
                        let __n = __tasks.len();
                        let mut __statuses: ::std::vec::Vec<
                            ::std::option::Option<::autumn_harvest::policy::TaskStatus>,
                        > = vec![None; __n];

                        // Walk levels in order; tasks within each level are
                        // dispatched before awaiting completions so independent
                        // same-level tasks preserve classic DAG parallelism.
                        for __level in &__levels {
                            let mut __activity_futs = ::std::vec::Vec::new();

                            for &__task_idx in __level {
                                // Eagerly copy task metadata into owned values
                                // so no borrow of `__tasks` crosses `.await`.
                                let __activity_name: ::std::string::String =
                                    __tasks[__task_idx].activity_name.clone();
                                let __queue_str: ::std::string::String = __tasks[__task_idx]
                                    .queue
                                    .clone()
                                    .unwrap_or_default();
                                let __upstreams: ::std::vec::Vec<usize> =
                                    __tasks[__task_idx].upstreams.clone();
                                let __trigger_rule =
                                    __tasks[__task_idx].trigger_rule.clone();
                                let __retry_override =
                                    __tasks[__task_idx].retry_policy.clone();
                                let __stc_override =
                                    __tasks[__task_idx].start_to_close;

                                let __ups: ::std::vec::Vec<
                                    ::autumn_harvest::policy::TaskStatus,
                                > = __upstreams
                                    .iter()
                                    .map(|&__i| {
                                        __statuses[__i].unwrap_or(
                                            ::autumn_harvest::policy::TaskStatus::Skipped,
                                        )
                                    })
                                    .collect();
                                let __should_run: bool = __trigger_rule.should_run(&__ups);

                                if !__should_run {
                                    __statuses[__task_idx] = Some(
                                        ::autumn_harvest::policy::TaskStatus::Skipped,
                                    );
                                    continue; // skip to next task in this level
                                }

                                let __activity_input = match _input.clone() {
                                    ::autumn_harvest::serde_json::Value::Object(mut __object) => {
                                        __object.insert(
                                            "dag_task".to_owned(),
                                            ::autumn_harvest::serde_json::Value::String(
                                                __activity_name.clone(),
                                            ),
                                        );
                                        ::autumn_harvest::serde_json::Value::Object(__object)
                                    }
                                    __conf => {
                                        let mut __object =
                                            ::autumn_harvest::serde_json::Map::new();
                                        __object.insert("conf".to_owned(), __conf);
                                        __object.insert(
                                            "dag_task".to_owned(),
                                            ::autumn_harvest::serde_json::Value::String(
                                                __activity_name.clone(),
                                            ),
                                        );
                                        ::autumn_harvest::serde_json::Value::Object(__object)
                                    }
                                };

                                __activity_futs.push(async move {
                                    let __status = match ctx
                                        .execute_activity_raw_with_opts(
                                            &__activity_name,
                                            __activity_input,
                                            &__queue_str,
                                            __retry_override,
                                            __stc_override,
                                        )
                                        .await
                                    {
                                        Ok(_) => ::autumn_harvest::policy::TaskStatus::Succeeded,
                                        Err(
                                            ::autumn_harvest::HarvestError::ActivityFailed { .. }
                                            | ::autumn_harvest::HarvestError::Timeout { .. },
                                        ) => {
                                            ::autumn_harvest::policy::TaskStatus::Failed
                                        }
                                        Err(__error) => return Err(__error.to_string()),
                                    };
                                    Ok::<_, ::std::string::String>((__task_idx, __status))
                                });
                            }

                            for __activity_result in
                                ::autumn_harvest::futures::future::join_all(__activity_futs).await
                            {
                                let (__task_idx, __status) = __activity_result?;
                                __statuses[__task_idx] = Some(__status);
                            }
                        }

                        let __any_failed = __statuses.iter().any(|s| {
                            matches!(s, Some(::autumn_harvest::policy::TaskStatus::Failed))
                        });
                        if __any_failed {
                            return Err("one or more DAG tasks failed".to_owned());
                        }

                        Ok(::autumn_harvest::serde_json::Value::Null)
                    })
                },
                execution_timeout: None,
                concurrency: ::std::option::Option::None,
                max_input_bytes: ::std::option::Option::None,
            }
        }
    }
}
