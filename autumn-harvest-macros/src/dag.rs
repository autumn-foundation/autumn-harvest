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
        } else {
            Err(meta.error(
                "unsupported attribute: expected schedule, catchup, max_active_runs, default_queue, jitter, owner, runbook, or severity",
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
    let workflow_companion = emit_workflow_companion(
        fn_name,
        &fn_name_str,
        attrs.default_queue.as_ref(),
        attrs.owner.as_deref(),
        attrs.runbook.as_deref(),
        attrs.severity.as_deref(),
    );

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
                        ::autumn_harvest::policy::TaskStatus,
                    > = vec![::autumn_harvest::policy::TaskStatus::Skipped; __n];
                    let mut __outputs: ::std::vec::Vec<
                        ::autumn_harvest::serde_json::Value,
                    > = vec![::autumn_harvest::serde_json::Value::Null; __n];

                    for __level in &__levels {
                        let mut __activity_futs: ::std::vec::Vec<
                            ::std::pin::Pin<
                                ::std::boxed::Box<
                                    dyn ::std::future::Future<
                                        Output = ::std::result::Result<
                                            (usize, ::autumn_harvest::policy::TaskStatus, ::autumn_harvest::serde_json::Value),
                                            ::std::string::String,
                                        >,
                                    > + Send,
                                >,
                            >,
                        > = ::std::vec::Vec::new();

                        for &__task_idx in __level {
                            let __activity_name: ::std::string::String =
                                __tasks[__task_idx].activity_name.clone();
                            let __queue_str: ::std::string::String = __tasks[__task_idx]
                                .queue
                                .clone()
                                .unwrap_or_default();
                            let __upstreams: ::std::vec::Vec<usize> =
                                __tasks[__task_idx].upstreams.clone();
                            let __retry_override = __tasks[__task_idx].retry_policy.clone();
                            let __stc_override = __tasks[__task_idx].start_to_close;

                            match __tasks[__task_idx].dispatch_decision(&__statuses, &__outputs) {
                                ::autumn_harvest::DagDispatchDecision::SkipByTriggerRule => {
                                    __statuses[__task_idx] = ::autumn_harvest::policy::TaskStatus::Skipped;
                                    continue;
                                }
                                ::autumn_harvest::DagDispatchDecision::SkipByCondition => {
                                    ctx.dag_skip_marker(__task_idx, &__activity_name, &__upstreams)
                                        .map_err(|e| e.to_string())?;
                                    __statuses[__task_idx] = ::autumn_harvest::policy::TaskStatus::Skipped;
                                    continue;
                                }
                                ::autumn_harvest::DagDispatchDecision::Run => {}
                            }

                            let __map_upstream_opt = __tasks[__task_idx].map_upstream;
                            if let Some(__upstream_idx) = __map_upstream_opt {
                                let __upstream_val = __outputs[__upstream_idx].clone();
                                let __policy = __tasks[__task_idx].map_failure_policy;
                                let __activity_name_clone = __activity_name.clone();
                                let __queue_str_clone = __queue_str.clone();
                                let __retry_override_clone = __retry_override.clone();
                                let __stc_override_clone = __stc_override;
                                __activity_futs.push(::std::boxed::Box::pin(async move {
                                    let __array = match &__upstream_val {
                                        ::autumn_harvest::serde_json::Value::Array(__arr) => __arr,
                                        _ => return Err("mapped upstream output is not a JSON array".to_owned()),
                                    };
                                    let __n_instances = __array.len();
                                    if __n_instances == 0 {
                                        return Ok((__task_idx, ::autumn_harvest::policy::TaskStatus::Succeeded, ::autumn_harvest::serde_json::Value::Array(Vec::new())));
                                    }

                                    let mut __instance_futs = ::std::vec::Vec::new();
                                    for (__i, __item) in __array.iter().enumerate() {
                                        let __item_input = __item.clone();
                                        let __act_name = __activity_name_clone.clone();
                                        let __q_str = __queue_str_clone.clone();
                                        let __ret_override = __retry_override_clone.clone();
                                        let __stc_over = __stc_override_clone;
                                        __instance_futs.push(async move {
                                            let __res = ctx
                                                .execute_activity_raw_with_opts(
                                                    &__act_name,
                                                    __item_input,
                                                    &__q_str,
                                                    __ret_override,
                                                    __stc_over,
                                                )
                                                .await;
                                            (__i, __res)
                                        });
                                    }

                                    let mut __results = vec![::autumn_harvest::serde_json::Value::Null; __n_instances];
                                    let mut __status = ::autumn_harvest::policy::TaskStatus::Succeeded;
                                    let mut __final_val = ::autumn_harvest::serde_json::Value::Null;

                                    if __policy == ::autumn_harvest::policy::MapFailurePolicy::FailFast {
                                        use ::autumn_harvest::futures::StreamExt as _;
                                        let mut __stream = __instance_futs.into_iter().collect::<::autumn_harvest::futures::stream::FuturesUnordered<_>>();
                                        while let Some((__i, __res)) = __stream.next().await {
                                            match __res {
                                                Ok(__val) => {
                                                    __results[__i] = __val;
                                                }
                                                Err(__err) => {
                                                    match &__err {
                                                        ::autumn_harvest::HarvestError::ActivityFailed { .. }
                                                        | ::autumn_harvest::HarvestError::Timeout { .. } => {
                                                            __status = ::autumn_harvest::policy::TaskStatus::Failed;
                                                            drop(__stream);
                                                            break;
                                                        }
                                                        _ => return Err(__err.to_string()),
                                                    }
                                                }
                                            }
                                        }
                                        if __status == ::autumn_harvest::policy::TaskStatus::Succeeded {
                                            __final_val = ::autumn_harvest::serde_json::Value::Array(__results);
                                        }
                                    } else {
                                        let mut __collect_results = vec![::autumn_harvest::serde_json::Value::Null; __n_instances];
                                        for __res_item in ::autumn_harvest::futures::future::join_all(__instance_futs).await {
                                            let (__i, __res) = __res_item;
                                            let __obj = match __res {
                                                Ok(__val) => ::autumn_harvest::serde_json::json!({
                                                    "status": "succeeded",
                                                    "value": __val
                                                }),
                                                Err(__err) => {
                                                    let __err_str = match &__err {
                                                        ::autumn_harvest::HarvestError::ActivityFailed { source, .. } => source.to_string(),
                                                        _ => __err.to_string(),
                                                    };
                                                    ::autumn_harvest::serde_json::json!({
                                                        "status": "failed",
                                                        "error": __err_str
                                                    })
                                                }
                                            };
                                            __collect_results[__i] = __obj;
                                        }
                                        __status = ::autumn_harvest::policy::TaskStatus::Succeeded;
                                        __final_val = ::autumn_harvest::serde_json::Value::Array(__collect_results);
                                    }

                                    Ok::<_, ::std::string::String>((__task_idx, __status, __final_val))
                                }));
                            } else {
                                let __has_mapped_upstream = __upstreams.iter().any(|&__i| __tasks[__i].map_upstream.is_some());
                                let __activity_input = if __has_mapped_upstream {
                                    let __mapped_up_idx = *__upstreams.iter().find(|&&__i| __tasks[__i].map_upstream.is_some()).unwrap();
                                    __outputs[__mapped_up_idx].clone()
                                } else {
                                    match _input.clone() {
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
                                    }
                                };

                                __activity_futs.push(::std::boxed::Box::pin(async move {
                                    let (__status, __val) = match ctx
                                        .execute_activity_raw_with_opts(
                                            &__activity_name,
                                            __activity_input,
                                            &__queue_str,
                                            __retry_override,
                                            __stc_override,
                                        )
                                        .await
                                    {
                                        Ok(__val) => (::autumn_harvest::policy::TaskStatus::Succeeded, __val),
                                        Err(
                                            ::autumn_harvest::HarvestError::ActivityFailed { .. }
                                            | ::autumn_harvest::HarvestError::Timeout { .. },
                                        ) => {
                                            (::autumn_harvest::policy::TaskStatus::Failed, ::autumn_harvest::serde_json::Value::Null)
                                        }
                                        Err(__error) => return Err(__error.to_string()),
                                    };
                                    Ok::<_, ::std::string::String>((__task_idx, __status, __val))
                                }));
                            }
                        }

                        for __activity_result in
                            ::autumn_harvest::futures::future::join_all(__activity_futs).await
                        {
                            let (__task_idx, __status, __val) = __activity_result?;
                            __statuses[__task_idx] = __status;
                            __outputs[__task_idx] = __val;
                        }
                    }

                    let __any_failed = __statuses.iter().any(|s| {
                        matches!(s, ::autumn_harvest::policy::TaskStatus::Failed)
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
    owner: Option<&str>,
    runbook: Option<&str>,
    severity: Option<&str>,
) -> TokenStream {
    let companion_name = format_ident!("__autumn_workflow_info_{fn_name}");

    let builder_init = default_queue.map(String::as_str).map_or_else(
        || quote! { ::autumn_harvest::DagBuilder::new() },
        |q| quote! { ::autumn_harvest::DagBuilder::with_default_queue(#q) },
    );

    let owner_expr = owner.map_or_else(|| quote! { None }, |s| quote! { Some(#s) });
    let runbook_url_expr = runbook.map_or_else(|| quote! { None }, |s| quote! { Some(#s) });
    let severity_expr = severity.map_or_else(|| quote! { None }, |s| quote! { Some(#s) });

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
                            ::autumn_harvest::policy::TaskStatus,
                        > = vec![::autumn_harvest::policy::TaskStatus::Skipped; __n];
                        let mut __outputs: ::std::vec::Vec<
                            ::autumn_harvest::serde_json::Value,
                        > = vec![::autumn_harvest::serde_json::Value::Null; __n];

                        // Walk levels in order; tasks within each level are
                        // dispatched before awaiting completions so independent
                        // same-level tasks preserve classic DAG parallelism.
                        for __level in &__levels {
                            let mut __activity_futs: ::std::vec::Vec<
                                ::std::pin::Pin<
                                    ::std::boxed::Box<
                                        dyn ::std::future::Future<
                                            Output = ::std::result::Result<
                                                (usize, ::autumn_harvest::policy::TaskStatus, ::autumn_harvest::serde_json::Value),
                                                ::std::string::String,
                                            >,
                                        > + Send,
                                    >,
                                >,
                            > = ::std::vec::Vec::new();

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
                                let __retry_override =
                                    __tasks[__task_idx].retry_policy.clone();
                                let __stc_override =
                                    __tasks[__task_idx].start_to_close;

                                match __tasks[__task_idx].dispatch_decision(&__statuses, &__outputs) {
                                    ::autumn_harvest::DagDispatchDecision::SkipByTriggerRule => {
                                        __statuses[__task_idx] = ::autumn_harvest::policy::TaskStatus::Skipped;
                                        continue;
                                    }
                                    ::autumn_harvest::DagDispatchDecision::SkipByCondition => {
                                        ctx.dag_skip_marker(__task_idx, &__activity_name, &__upstreams)
                                            .map_err(|e| e.to_string())?;
                                        __statuses[__task_idx] = ::autumn_harvest::policy::TaskStatus::Skipped;
                                        continue;
                                    }
                                    ::autumn_harvest::DagDispatchDecision::Run => {}
                                }

                                let __map_upstream_opt = __tasks[__task_idx].map_upstream;
                                if let Some(__upstream_idx) = __map_upstream_opt {
                                    let __upstream_val = __outputs[__upstream_idx].clone();
                                    let __policy = __tasks[__task_idx].map_failure_policy;
                                    let __activity_name_clone = __activity_name.clone();
                                    let __queue_str_clone = __queue_str.clone();
                                    let __retry_override_clone = __retry_override.clone();
                                    let __stc_override_clone = __stc_override;
                                    __activity_futs.push(::std::boxed::Box::pin(async move {
                                     let __array = match &__upstream_val {
                                         ::autumn_harvest::serde_json::Value::Array(__arr) => __arr,
                                         _ => return Err("mapped upstream output is not a JSON array".to_owned()),
                                     };
                                     let __n_instances = __array.len();
                                     if __n_instances == 0 {
                                         return Ok((__task_idx, ::autumn_harvest::policy::TaskStatus::Succeeded, ::autumn_harvest::serde_json::Value::Array(Vec::new())));
                                     }

                                     let mut __instance_futs = ::std::vec::Vec::new();
                                     for (__i, __item) in __array.iter().enumerate() {
                                         let __item_input = __item.clone();
                                         let __act_name = __activity_name_clone.clone();
                                         let __q_str = __queue_str_clone.clone();
                                         let __ret_override = __retry_override_clone.clone();
                                         let __stc_over = __stc_override_clone;
                                         __instance_futs.push(async move {
                                             let __res = ctx
                                                 .execute_activity_raw_with_opts(
                                                     &__act_name,
                                                     __item_input,
                                                     &__q_str,
                                                     __ret_override,
                                                     __stc_over,
                                                 )
                                                 .await;
                                             (__i, __res)
                                         });
                                     }

                                     let mut __results = vec![::autumn_harvest::serde_json::Value::Null; __n_instances];
                                     let mut __status = ::autumn_harvest::policy::TaskStatus::Succeeded;
                                     let mut __final_val = ::autumn_harvest::serde_json::Value::Null;

                                     if __policy == ::autumn_harvest::policy::MapFailurePolicy::FailFast {
                                         use ::autumn_harvest::futures::StreamExt as _;
                                         let mut __stream = __instance_futs.into_iter().collect::<::autumn_harvest::futures::stream::FuturesUnordered<_>>();
                                         while let Some((__i, __res)) = __stream.next().await {
                                             match __res {
                                                 Ok(__val) => {
                                                     __results[__i] = __val;
                                                 }
                                                 Err(__err) => {
                                                     match &__err {
                                                         ::autumn_harvest::HarvestError::ActivityFailed { .. }
                                                         | ::autumn_harvest::HarvestError::Timeout { .. } => {
                                                             __status = ::autumn_harvest::policy::TaskStatus::Failed;
                                                             drop(__stream);
                                                             break;
                                                         }
                                                         _ => return Err(__err.to_string()),
                                                     }
                                                 }
                                             }
                                         }
                                         if __status == ::autumn_harvest::policy::TaskStatus::Succeeded {
                                             __final_val = ::autumn_harvest::serde_json::Value::Array(__results);
                                         }
                                     } else {
                                         let mut __collect_results = vec![::autumn_harvest::serde_json::Value::Null; __n_instances];
                                         for __res_item in ::autumn_harvest::futures::future::join_all(__instance_futs).await {
                                             let (__i, __res) = __res_item;
                                             let __obj = match __res {
                                                 Ok(__val) => ::autumn_harvest::serde_json::json!({
                                                     "status": "succeeded",
                                                     "value": __val
                                                 }),
                                                 Err(__err) => {
                                                     match &__err {
                                                         ::autumn_harvest::HarvestError::ActivityFailed { .. }
                                                         | ::autumn_harvest::HarvestError::Timeout { .. } => {
                                                             let __err_str = match &__err {
                                                                 ::autumn_harvest::HarvestError::ActivityFailed { source, .. } => source.to_string(),
                                                                 _ => __err.to_string(),
                                                             };
                                                             ::autumn_harvest::serde_json::json!({
                                                                 "status": "failed",
                                                                 "error": __err_str
                                                             })
                                                         }
                                                         _ => return Err(__err.to_string()),
                                                     }
                                                 }
                                             };
                                             __collect_results[__i] = __obj;
                                         }
                                         __status = ::autumn_harvest::policy::TaskStatus::Succeeded;
                                         __final_val = ::autumn_harvest::serde_json::Value::Array(__collect_results);
                                     }

                                     Ok::<_, ::std::string::String>((__task_idx, __status, __final_val))
                                 }));
                                } else {
                                    let __has_mapped_upstream = __upstreams.iter().any(|&__i| __tasks[__i].map_upstream.is_some());
                                    let __activity_input = if __has_mapped_upstream {
                                        let __mapped_up_idx = *__upstreams.iter().find(|&&__i| __tasks[__i].map_upstream.is_some()).unwrap();
                                        __outputs[__mapped_up_idx].clone()
                                    } else {
                                        match _input.clone() {
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
                                        }
                                    };

                                    __activity_futs.push(::std::boxed::Box::pin(async move {
                                        let (__status, __val) = match ctx
                                            .execute_activity_raw_with_opts(
                                                &__activity_name,
                                                __activity_input,
                                                &__queue_str,
                                                __retry_override,
                                                __stc_override,
                                            )
                                            .await
                                        {
                                            Ok(__val) => (::autumn_harvest::policy::TaskStatus::Succeeded, __val),
                                            Err(
                                                ::autumn_harvest::HarvestError::ActivityFailed { .. }
                                                | ::autumn_harvest::HarvestError::Timeout { .. },
                                            ) => {
                                                (::autumn_harvest::policy::TaskStatus::Failed, ::autumn_harvest::serde_json::Value::Null)
                                            }
                                            Err(__error) => return Err(__error.to_string()),
                                        };
                                        Ok::<_, ::std::string::String>((__task_idx, __status, __val))
                                    }));
                                }
                            }

                            for __activity_result in
                                ::autumn_harvest::futures::future::join_all(__activity_futs).await
                            {
                                let (__task_idx, __status, __val) = __activity_result?;
                                __statuses[__task_idx] = __status;
                                __outputs[__task_idx] = __val;
                            }
                        }

                        let __any_failed = __statuses.iter().any(|s| {
                            matches!(s, ::autumn_harvest::policy::TaskStatus::Failed)
                        });
                        if __any_failed {
                            return Err("one or more DAG tasks failed".to_owned());
                        }

                        Ok(::autumn_harvest::serde_json::Value::Null)
                    })
                },
                execution_timeout: None,
                sla: ::std::option::Option::None,
                concurrency: ::std::option::Option::None,
                debounce: ::std::option::Option::None,
                batch: ::std::option::Option::None,
                max_input_bytes: ::std::option::Option::None,
                owner: #owner_expr,
                runbook_url: #runbook_url_expr,
                severity: #severity_expr,
                description: ::std::option::Option::None,
                input_schema: ::std::option::Option::None,
                output_schema: ::std::option::Option::None,
                error_schema: ::std::option::Option::None,
                retry_policy: ::std::option::Option::None,
                // Unified DAGs are never MCP-exposed (issue #597): #[dag] has
                // no `mcp` attribute; the DAG trigger surface is its own
                // management-API family.
                mcp: false,
            }
        }
    }
}
