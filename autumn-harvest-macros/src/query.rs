//! `#[query]` attribute macro for marking read-only workflow query handlers.
//!
//! The `#[query]` attribute is a **documentation and validation marker**. It
//! preserves the annotated function unchanged and signals to readers that the
//! function is intended to be registered as a read-only query handler via
//! `WorkflowContext::register_query_handler`.
//!
//! Unlike `#[workflow]` and `#[activity]`, `#[query]` does not generate a
//! companion info function — query handlers are closures that capture workflow
//! state and are registered at runtime, not at builder time.
//!
//! # Example
//!
//! ```ignore
//! use autumn_harvest::prelude::*;
//! use std::sync::{Arc, Mutex};
//!
//! #[derive(serde::Deserialize)]
//! struct ProgressQuery { include_details: bool }
//!
//! #[derive(serde::Serialize)]
//! struct ProgressResponse { processed: u32, details: Option<String> }
//!
//! // Mark the function as a query handler for documentation and tooling.
//! #[query]
//! fn progress_query(req: &ProgressQuery, processed: u32) -> Result<ProgressResponse, String> {
//!     Ok(ProgressResponse {
//!         processed,
//!         details: if req.include_details { Some("step 3".to_owned()) } else { None },
//!     })
//! }
//!
//! #[workflow]
//! async fn my_workflow(ctx: &WorkflowContext, _input: ()) -> Result<(), String> {
//!     let processed = Arc::new(Mutex::new(0u32));
//!     let state = processed.clone();
//!
//!     // Register using register_query_handler; the #[query] attribute is
//!     // purely a human/tooling signal — it does not auto-register.
//!     ctx.register_query_handler("progress", move |req: &ProgressQuery| {
//!         progress_query(req, *state.lock().unwrap())
//!     });
//!
//!     // ... workflow body ...
//!     Ok(())
//! }
//! ```

use proc_macro2::TokenStream;
use quote::quote;
use syn::{ItemFn, parse2};

/// Parse and emit the annotated function unchanged.
///
/// Validates that the attributed item is a function (not a struct, enum, …).
/// Future enhancements may enforce the `fn(&Req) -> Result<Resp, String>` shape
/// or auto-generate registration glue.
pub fn query_macro(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let func: ItemFn = match parse2(item) {
        Ok(f) => f,
        Err(e) => return e.to_compile_error(),
    };

    // Pass the function through unchanged.
    quote! { #func }
}
