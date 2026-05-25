//! `#[update]` attribute macro for declarative workflow update handlers (issue #346).
//!
//! Generates a companion function
//! `__autumn_update_handler_info_{fn_name}() -> UpdateHandlerInfo`
//! alongside the user's async function.
//!
//! Attributes:
//! - `workflow = "name"` (required) — the workflow this handler belongs to.
//! - `validator = path::to::fn` (optional) — synchronous validator called before
//!   the update is admitted to history.
//!
//! The annotated function must:
//! - Be `async`
//! - Take `ctx: &WorkflowContext` as its first argument
//! - Return `Result<T, E>`

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Expr, ItemFn, LitStr, parse::Parser as _};

struct UpdateAttrs {
    workflow: Option<String>,
    validator: Option<Expr>,
}

fn parse_attrs(attr: TokenStream) -> syn::Result<UpdateAttrs> {
    let mut result = UpdateAttrs {
        workflow: None,
        validator: None,
    };

    syn::meta::parser(|meta| {
        if meta.path.is_ident("workflow") {
            let value: LitStr = meta.value()?.parse()?;
            result.workflow = Some(value.value());
            Ok(())
        } else if meta.path.is_ident("validator") {
            let value: Expr = meta.value()?.parse()?;
            result.validator = Some(value);
            Ok(())
        } else {
            Err(meta.error(
                "unsupported attribute: expected `workflow = \"name\"` or `validator = path::to::fn`",
            ))
        }
    })
    .parse2(attr)?;

    Ok(result)
}

#[allow(clippy::too_many_lines)]
pub fn update_macro(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attrs = match parse_attrs(attr) {
        Ok(a) => a,
        Err(e) => return e.to_compile_error(),
    };

    let func: ItemFn = match syn::parse2(item) {
        Ok(f) => f,
        Err(e) => return e.to_compile_error(),
    };

    let Some(workflow_name) = attrs.workflow else {
        return syn::Error::new_spanned(
            func.sig.fn_token,
            "#[update] requires `workflow = \"workflow_name\"`",
        )
        .to_compile_error();
    };

    // ── Validation ────────────────────────────────────────────────────────────

    if func.sig.asyncness.is_none() {
        return syn::Error::new_spanned(func.sig.fn_token, "#[update] handlers must be async")
            .to_compile_error();
    }

    // First parameter must be ctx: &WorkflowContext.
    if !first_param_is_ctx(&func.sig.inputs) {
        return syn::Error::new_spanned(
            &func.sig,
            "#[update] handlers must take `ctx: &WorkflowContext` as the first argument",
        )
        .to_compile_error();
    }

    // Return type must be Result<T, E>.
    if !returns_result(&func.sig.output) {
        return syn::Error::new_spanned(
            &func.sig.output,
            "#[update] return type must be `Result<T, E>`",
        )
        .to_compile_error();
    }

    // ── Companion generation ──────────────────────────────────────────────────

    let fn_name = &func.sig.ident;
    let fn_name_str = fn_name.to_string();
    let companion_name = format_ident!("__autumn_update_handler_info_{fn_name}");

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

    let dispatch = build_update_dispatch(fn_name, &param_names);

    let (has_validator, validator_expr) = attrs.validator.as_ref().map_or_else(
        || (quote! { false }, quote! { None }),
        |validator_path| {
            (
                quote! { true },
                quote! { Some(#validator_path as ::autumn_harvest::UpdateValidatorFn) },
            )
        },
    );

    let parsed_path = match crate::parse_and_validate_workflow_path(
        &workflow_name,
        proc_macro2::Span::call_site(),
    ) {
        Ok(p) => p,
        Err(e) => return e.to_compile_error(),
    };
    let workflow_simple_name = parsed_path.workflow_simple_name;
    let camel_wf = to_pascal_case(&workflow_simple_name);
    let stub_ident = format_ident!("{camel_wf}Stub");
    let method_name = format_ident!("update_{fn_name}");
    let ok_type = extract_ok_type(&func.sig.output);

    let serialize_payload = if param_names.is_empty() {
        quote! { ::autumn_harvest::serde_json::Value::Null }
    } else if param_names.len() == 1 {
        let name = &param_names[0];
        quote! { ::autumn_harvest::serde_json::to_value(&#name).map_err(::autumn_harvest::error::HarvestError::Serialization)? }
    } else {
        quote! { ::autumn_harvest::serde_json::to_value((#(&#param_names),*)).map_err(::autumn_harvest::error::HarvestError::Serialization)? }
    };

    let method_name_with_timeout = format_ident!("{}_with_timeout", method_name);

    let mod_name = format_ident!("__autumn_update_impl_{fn_name}");
    let path_tokens = parsed_path.path_tokens;
    let is_absolute = parsed_path.is_absolute;
    let leading_colon = if is_absolute {
        quote! { :: }
    } else {
        quote! {}
    };
    let nested_path_tokens = if is_absolute
        || parsed_path
            .original_module_parts
            .first()
            .is_some_and(|s| s == "crate")
    {
        path_tokens.clone()
    } else if parsed_path.original_module_parts.is_empty() {
        Vec::new()
    } else {
        let mut tokens = Vec::new();
        tokens.push(quote! { super });
        let first = parsed_path.original_module_parts.first().unwrap();
        if first == "self" {
            for p in parsed_path.original_module_parts.iter().skip(1) {
                let id = format_ident!("{}", p);
                tokens.push(quote! { #id });
            }
        } else {
            for p in &parsed_path.original_module_parts {
                let id = format_ident!("{}", p);
                tokens.push(quote! { #id });
            }
        }
        tokens
    };
    let impl_block = if path_tokens.is_empty() {
        quote! {
            ::autumn_harvest::cfg_db! {
                impl #stub_ident {
                    /// Execute this typed update handler in-process with a default 30-second timeout.
                    pub async fn #method_name(
                        conn: &mut ::autumn_harvest::diesel_async::AsyncPgConnection,
                        handle: &::autumn_harvest::WorkflowHandle,
                        #(#params),*
                    ) -> ::autumn_harvest::HarvestResult<#ok_type> {
                        Self::#method_name_with_timeout(
                            conn,
                            handle,
                            #(#param_names,)*
                            ::std::time::Duration::from_secs(30)
                        ).await
                    }

                    /// Execute this typed update handler in-process with a custom timeout.
                    pub async fn #method_name_with_timeout(
                        conn: &mut ::autumn_harvest::diesel_async::AsyncPgConnection,
                        handle: &::autumn_harvest::WorkflowHandle,
                        #(#params,)*
                        timeout: ::std::time::Duration,
                    ) -> ::autumn_harvest::HarvestResult<#ok_type> {
                        let args = #serialize_payload;
                        let raw = handle.execute_update_in_process(
                            conn,
                            #fn_name_str,
                            args,
                            timeout
                        ).await?;
                        ::autumn_harvest::serde_json::from_value(raw)
                            .map_err(::autumn_harvest::error::HarvestError::Serialization)
                    }
                }
            }
        }
    } else {
        quote! {
            ::autumn_harvest::cfg_db! {
                mod #mod_name {
                    use super::*;
                    use #leading_colon #(#nested_path_tokens::)*#stub_ident;
                    impl #stub_ident {
                        /// Execute this typed update handler in-process with a default 30-second timeout.
                        pub async fn #method_name(
                            conn: &mut ::autumn_harvest::diesel_async::AsyncPgConnection,
                            handle: &::autumn_harvest::WorkflowHandle,
                            #(#params),*
                        ) -> ::autumn_harvest::HarvestResult<#ok_type> {
                            Self::#method_name_with_timeout(
                                conn,
                                handle,
                                #(#param_names,)*
                                ::std::time::Duration::from_secs(30)
                            ).await
                        }

                        /// Execute this typed update handler in-process with a custom timeout.
                        pub async fn #method_name_with_timeout(
                            conn: &mut ::autumn_harvest::diesel_async::AsyncPgConnection,
                            handle: &::autumn_harvest::WorkflowHandle,
                            #(#params,)*
                            timeout: ::std::time::Duration,
                        ) -> ::autumn_harvest::HarvestResult<#ok_type> {
                            let args = #serialize_payload;
                            let raw = handle.execute_update_in_process(
                                conn,
                                #fn_name_str,
                                args,
                                timeout
                            ).await?;
                            ::autumn_harvest::serde_json::from_value(raw)
                                .map_err(::autumn_harvest::error::HarvestError::Serialization)
                        }
                    }
                }
            }
        }
    };

    quote! {
        #func

        #[doc(hidden)]
        pub fn #companion_name() -> ::autumn_harvest::UpdateHandlerInfo {
            fn __dispatch(
                ctx: ::std::sync::Arc<::autumn_harvest::WorkflowContext>,
                args: ::autumn_harvest::serde_json::Value,
            ) -> ::std::pin::Pin<::std::boxed::Box<
                dyn ::std::future::Future<
                    Output = Result<::autumn_harvest::serde_json::Value, String>,
                > + Send,
            >> {
                ::std::boxed::Box::pin(async move {
                    #dispatch
                })
            }

            ::autumn_harvest::UpdateHandlerInfo {
                name: #fn_name_str,
                workflow: #workflow_simple_name,
                module: module_path!(),
                input_type_hint: #input_type_hint,
                output_type_hint: #output_type_hint,
                has_validator: #has_validator,
                handler: __dispatch,
                validator: #validator_expr,
            }
        }

        #impl_block
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Returns `true` when the first parameter matches `ctx: &WorkflowContext`.
fn first_param_is_ctx(inputs: &syn::punctuated::Punctuated<syn::FnArg, syn::token::Comma>) -> bool {
    let Some(first) = inputs.first() else {
        return false;
    };
    let syn::FnArg::Typed(pt) = first else {
        return false;
    };
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

fn build_update_dispatch(fn_name: &syn::Ident, param_names: &[&syn::Ident]) -> TokenStream {
    if param_names.is_empty() {
        quote! {
            let result = #fn_name(ctx.as_ref()).await;
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
            let result = #fn_name(ctx.as_ref(), #name).await;
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
            let result = #fn_name(ctx.as_ref(), #(#names),*).await;
            result.map_err(|e| e.to_string())
                .and_then(|v| {
                    ::autumn_harvest::serde_json::to_value(v).map_err(|e| e.to_string())
                })
        }
    }
}

fn build_input_type_hint(params: &[&syn::FnArg]) -> String {
    if params.is_empty() {
        return "()".to_string();
    }
    if params.len() == 1
        && let syn::FnArg::Typed(pt) = params[0]
    {
        return type_name_hint(&pt.ty);
    }
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

fn extract_ok_type_hint(output: &syn::ReturnType) -> String {
    crate::extract_ok_type_hint(output)
}

fn type_name_hint(ty: &syn::Type) -> String {
    crate::type_name_hint(ty)
}

fn to_pascal_case(s: &str) -> String {
    crate::to_pascal_case(s)
}

fn extract_ok_type(output: &syn::ReturnType) -> syn::Type {
    crate::extract_ok_type(output)
}
