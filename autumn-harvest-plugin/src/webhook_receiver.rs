//! Inbound HTTP webhook receiver route generation and dispatch (issue #344).
//!
//! Mirrors the `mcp_tools.rs` route-generation pattern (issue #597): app-level
//! [`autumn_web::Route`] values are generated at [`crate::plugin::HarvestPlugin::build`]
//! time from the descriptors [`HarvestPlugin::webhooks(...)`] registered, one
//! per `#[webhook]` binding.
//!
//! Two entry points build from the same trigger descriptors.
//! [`build_webhook_routes`] returns `Vec<autumn_web::Route>` for
//! `HarvestPlugin`/`autumn_web::AppBuilder`. [`build_webhook_router`] returns
//! a bare `axum::Router<()>` for an embedder mounting `harvest_api_router`
//! standalone (issue #1612). Both dispatch through the same handler, built
//! once in `webhook_method_router`.
//!
//! # Who verifies what
//!
//! Each generated handler takes
//! `Result<autumn_web::webhook::SignedWebhook, autumn_web::AutumnError>`.
//! Signature, timestamp and optional replay checks **already run** in axum's
//! extractor phase, before the handler code. This module never parses a raw,
//! unverified request body. See `docs/getting-started/12-webhooks.md` for the
//! `[security.webhooks]` configuration.
//!
//! # Dispatch
//!
//! Dispatch delegates to the management API handlers
//! [`crate::api::start_workflow`] and [`crate::api::signal_with_start_workflow`].
//! So schema validation, the admission gate, debounce, batch, SLA and
//! execution timeout apply identically to a webhook start.
//! [`reshape_dispatch_response`] reshapes the delegate response into the
//! issue's `{"status", "workflow_exec_id", "workflow_id"}` envelope. It also
//! classifies the outcome for `harvest.webhook.received`/`.rejected`. It
//! writes a `webhook.trigger` audit row in addition to the delegate's own
//! row. That row lets an operator find the webhook path that triggered a run.
//!
//! The delegation avoids a second copy of the admission, debounce, batch and
//! schema logic. But it couples [`reshape_dispatch_response`] to the HTTP
//! response *shape* (status code and JSON body), not to typed return values.
//! Several past classification bugs came from that coupling. A `202` debounce
//! admission or a delegate `4xx` was misread as an internal error. So the
//! classification checks `is_client_error()` before it treats a status as
//! internal. A change to the delegate response shape needs a manual re-check
//! here.

use autumn_harvest::audit::{
    self, HEADER_IDEMPOTENCY_KEY, OP_WEBHOOK_TRIGGER, STATUS_FAILED, STATUS_SUCCEEDED,
    TARGET_WORKFLOW,
};
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::models::NewAuditRecord;
use autumn_harvest::telemetry::{MetricsRecorder, WebhookOutcome};
use autumn_harvest::types::WorkflowId;
use autumn_harvest::webhook_trigger::{
    WebhookCtx, WebhookHandlerError, WebhookTarget, WebhookTriggerInfo, validate_webhook_triggers,
};
use autumn_web::reexports::axum;
use autumn_web::webhook::{SignedWebhook, WebhookRegistry};
use autumn_web::{AppState, Route};
use axum::response::IntoResponse as _;
use axum::{Extension, Json};

use crate::api::{HarvestApiState, SignalWithStartRequest, StartWorkflowRequest};

/// The configuration types that [`build_webhook_router`] takes.
///
/// The re-export lets an embedder configure a receiver with no `autumn-web`
/// entry in its own manifest (issue #1615).
pub use autumn_web::webhook::{WebhookConfig, WebhookConfigError, WebhookEndpointConfig};

/// Build the app-level routes for a set of registered webhook triggers.
///
/// The scan of `registered_dags` (e.g. from `HarvestBuilder::dag_infos()`)
/// rejects a trigger that targets a unified DAG with a specific message. A unified
/// DAG has an auto-registered shadow `WorkflowInfo`. Without this scan it
/// passes the registered-workflow check and fails only on the first live
/// request. Only unified DAGs (`workflow_handler.is_some()`) are excluded
/// here. They are exactly the DAG names that `HarvestApiState::is_registered_dag`
/// rejects at request time. A classic DAG has no shadow `WorkflowInfo`, so
/// the registered-workflow check already rejects it.
///
/// For an embedder mounting webhooks without `HarvestPlugin`, see
/// [`build_webhook_router`] -- this function's `Vec<autumn_web::Route>`
/// return type only composes with an `autumn_web::AppBuilder`.
///
/// # Panics
///
/// Panics when [`validate_webhook_triggers`] rejects the set (duplicate or
/// malformed binding paths). Also panics when a trigger targets a registered
/// DAG (use `POST /dags/{name}/trigger`) or an unregistered workflow. A mount
/// conflict or dangling target must fail at `HarvestPlugin::build` time, not
/// on the first live request.
#[must_use]
pub fn build_webhook_routes(
    triggers: &[WebhookTriggerInfo],
    registered_workflows: &[WorkflowInfo],
    registered_dags: &[autumn_harvest::DagInfo],
    api_state: &HarvestApiState,
) -> Vec<Route> {
    validate_triggers_or_panic(triggers, registered_workflows, registered_dags);
    triggers
        .iter()
        .map(|trigger| build_webhook_route(trigger, api_state.clone()))
        .collect()
}

/// Build a bare, state-erased `axum::Router<()>` mounting the given webhook triggers.
///
/// The standalone counterpart to [`build_webhook_routes`], for an embedder
/// that mounts `harvest_api_router` on a plain Axum server instead of going
/// through `HarvestPlugin` and `autumn_web::app()` (issue #1612).
///
/// `autumn_web::Route::handler` is `MethodRouter<autumn_web::AppState>`, not
/// generic over the router's state type. The `SignedWebhook` extractor it
/// wraps only implements `FromRequest<AppState>`, reading the installed
/// [`WebhookRegistry`] out of that state's typed-extension store. So a bare
/// `axum::Router` cannot use `SignedWebhook` directly; what it needs is an
/// `AppState`, just not one built by the full `autumn_web::app()` pipeline.
/// `AppState::detached()` is exactly that: a minimal `AppState` with no HTTP
/// server or database pool attached. It is documented for "background
/// runtimes or helper processes that still need framework-managed resources
/// such as typed extensions".
///
/// This function builds one, installs a [`WebhookRegistry`] resolved from
/// `webhook_config` into it, mounts every trigger, and erases the state with
/// `Router::with_state`. The registry is the same one `HarvestPlugin`
/// installs from `autumn.toml`'s `[security.webhooks]`. It is supplied
/// directly here because a standalone embedder has no `autumn.toml` to load
/// it from. The returned `Router<()>` carries the `AppState` inside its
/// closures, not in its type. So it merges onto an embedder's own bare
/// router like any other handler.
///
/// # Replay protection is not available standalone
///
/// Autumn's boot-window/failure replay-key cleanup is `WebhookReplayCleanupLayer`
/// -- it releases a reserved replay key on a `5xx` instead of leaving it
/// stuck forever. It is installed only by `autumn_web::app()`'s own
/// router-building code, with no public constructor an embedder can install
/// by hand. An endpoint with `replay_protection` enabled would reserve a key
/// on every signed request and never release it on failure. So this function
/// rejects such a config outright, rather than mounting a route with that
/// latent correctness bug.
///
/// This is a narrow gap in practice. `docs/getting-started/12-webhooks.md`
/// already recommends `replay_protection = false` for every Harvest-bound
/// endpoint. Harvest's own dedup -- the mapping function's deterministic
/// `WorkflowId`, or the `SignalsWithStart` idempotency key -- is durable, and
/// autumn-web's in-memory replay store is not.
///
/// # Errors
///
/// Returns [`WebhookConfigError::InvalidEndpoint`] when any endpoint in
/// `webhook_config` has `replay_protection` enabled, or whatever
/// [`WebhookRegistry::from_config`] itself rejects (missing/weak secrets,
/// duplicate paths, an unsupported replay backend).
///
/// # Panics
///
/// Same as [`build_webhook_routes`]: a malformed/duplicate trigger set, a
/// trigger targeting a registered DAG, or a trigger targeting an
/// unregistered workflow. Also panics when a trigger's path has no matching
/// entry in `webhook_config.endpoints`. `SignedWebhook` installs a verifier
/// per configured endpoint, not per trigger, so an uncovered path would
/// otherwise 500 on every request. Failing here at build time instead is the
/// point (PR #1720).
pub fn build_webhook_router(
    triggers: &[WebhookTriggerInfo],
    registered_workflows: &[WorkflowInfo],
    registered_dags: &[autumn_harvest::DagInfo],
    api_state: &HarvestApiState,
    webhook_config: &WebhookConfig,
) -> Result<axum::Router<()>, WebhookConfigError> {
    validate_triggers_or_panic(triggers, registered_workflows, registered_dags);
    for trigger in triggers {
        assert!(
            webhook_config
                .endpoints
                .iter()
                .any(|endpoint| endpoint.path == trigger.path),
            "webhook trigger '{}' (path '{}') has no matching entry in webhook_config.endpoints \
             -- SignedWebhook installs a verifier per configured endpoint, not per trigger, so \
             every delivery to this path would fail at request time (a 500) instead of failing \
             here at build time. Add a WebhookEndpointConfig for '{}' to the WebhookConfig passed \
             to build_webhook_router.",
            trigger.name,
            trigger.path,
            trigger.path
        );
    }
    if let Some(endpoint) = webhook_config
        .endpoints
        .iter()
        .find(|endpoint| endpoint.replay_protection)
    {
        return Err(WebhookConfigError::InvalidEndpoint {
            name: endpoint.name.clone(),
            message: "standalone webhook mounting (build_webhook_router) requires \
                replay_protection = false: the boot-window/5xx replay-key cleanup layer is \
                installed only by autumn_web's own AppBuilder, so a reserved replay key would \
                never be released on failure here. Call \
                WebhookEndpointConfig::without_replay_protection() -- this is already the \
                recommended setting for Harvest-bound endpoints (docs/getting-started/\
                12-webhooks.md), since Harvest's own dedup is durable and autumn-web's \
                in-memory replay store is not."
                .to_string(),
        });
    }
    let registry = WebhookRegistry::from_config(webhook_config)?;
    let detached_state = AppState::detached();
    detached_state.insert_extension(registry);
    let mut router = axum::Router::<AppState>::new();
    for trigger in triggers {
        router = router.route(
            trigger.path,
            webhook_method_router(trigger, api_state.clone()),
        );
    }
    Ok(router.with_state(detached_state))
}

/// Shared fail-fast validation for [`build_webhook_routes`] and
/// [`build_webhook_router`] -- see [`build_webhook_routes`]'s own doc comment
/// for what each check catches and why.
fn validate_triggers_or_panic(
    triggers: &[WebhookTriggerInfo],
    registered_workflows: &[WorkflowInfo],
    registered_dags: &[autumn_harvest::DagInfo],
) {
    if let Err(e) = validate_webhook_triggers(triggers) {
        panic!("HarvestPlugin::webhooks(...) failed validation: {e}");
    }
    let dag_names: std::collections::HashSet<&str> = registered_dags
        .iter()
        .filter(|d| d.workflow_handler.is_some())
        .map(|d| d.name)
        .collect();
    for trigger in triggers {
        let workflow = trigger.target.workflow();
        assert!(
            !dag_names.contains(workflow),
            "webhook trigger '{}' (path '{}') targets '{workflow}', which is a registered DAG \
             -- webhooks can only target plain #[workflow] handlers; DAGs are triggered via \
             POST /dags/{workflow}/trigger, not a #[webhook] binding",
            trigger.name,
            trigger.path
        );
        assert!(
            registered_workflows.iter().any(|w| w.name == workflow),
            "webhook trigger '{}' (path '{}') targets workflow '{workflow}', which is not \
             registered on HarvestPlugin -- register it via .workflows(workflows![...]) before \
             .webhooks(webhooks![...])",
            trigger.name,
            trigger.path
        );
    }
}

/// The axum handler shared by both [`build_webhook_routes`] (wrapped into an
/// `autumn_web::Route` with its OpenAPI/idempotency/timeout/SEO metadata) and
/// [`build_webhook_router`] (mounted directly). One definition so the two
/// entry points can never dispatch differently for the same trigger.
fn webhook_method_router(
    trigger: &WebhookTriggerInfo,
    api_state: HarvestApiState,
) -> axum::routing::MethodRouter<AppState> {
    let path = trigger.path;
    let target = trigger.target;
    let handler_fn = trigger.handler;
    let queue = trigger.queue;

    axum::routing::post(
        move |headers: axum::http::HeaderMap,
              hook: Result<SignedWebhook, autumn_web::AutumnError>| {
            let api_state = api_state.clone();
            // Boxed: the delegated start/signal-with-start handler's future is
            // large (clippy::large_futures) and this is a cold edge path.
            async move {
                Box::pin(handle_webhook(
                    api_state, path, target, handler_fn, queue, headers, hook,
                ))
                .await
            }
        },
    )
}

fn build_webhook_route(trigger: &WebhookTriggerInfo, api_state: HarvestApiState) -> Route {
    let path = trigger.path;
    let name = trigger.name;
    let handler = webhook_method_router(trigger, api_state);

    Route {
        method: axum::http::Method::POST,
        path,
        handler,
        name,
        api_doc: autumn_web::openapi::ApiDoc {
            method: "POST",
            path,
            operation_id: name,
            description: Some(
                "Inbound webhook receiver (issue #344). Signature/timestamp/replay \
                 verification is configured under [security.webhooks], not here.",
            ),
            success_status: 202,
            // Never an MCP tool: an inbound webhook is reached by an external
            // sender's HMAC signature, not an authenticated MCP caller.
            mcp_tool: false,
            ..Default::default()
        },
        api_version: None,
        sunset_opt_out: false,
        repository: None,
        idempotency: autumn_web::RouteIdempotency::Direct,
        // Inherit the global request-timeout deadline (autumn-web 0.6).
        timeout: autumn_web::RouteTimeout::Inherit,
        // The class that a route macro sets when it names none (since autumn-web 0.9).
        criticality: autumn_web::Criticality::Default,
        // An inbound webhook endpoint is machine-to-machine and is never
        // rendered or indexed, so it carries no SEO meta (since autumn-web 0.7).
        seo: autumn_web::SeoRouteDefaults::EMPTY,
        // The group of the plugin that registers the route (since autumn-web 0.10).
        group: None,
    }
}

/// Record `harvest.webhook.received` unconditionally and
/// `harvest.webhook.rejected` for every outcome except [`WebhookOutcome::Accepted`]
/// and [`WebhookOutcome::IdempotentReplay`] (both are successful dispatch
/// outcomes, not rejections).
fn record_outcome(metrics: &dyn MetricsRecorder, path: &str, outcome: WebhookOutcome) {
    metrics.record_webhook_received(path, outcome);
    if !matches!(
        outcome,
        WebhookOutcome::Accepted | WebhookOutcome::IdempotentReplay
    ) {
        metrics.record_webhook_rejected(path, outcome);
    }
}

fn error_response(
    status: axum::http::StatusCode,
    error_code: &str,
    message: &str,
) -> axum::response::Response {
    (
        status,
        Json(serde_json::json!({ "error_code": error_code, "error": message })),
    )
        .into_response()
}

/// Classify a `SignedWebhook` extraction failure's HTTP status into a
/// [`WebhookOutcome`]. A `409 Conflict` is the autumn-web replay layer
/// rejecting a duplicate delivery ID. It is a benign redelivery, not a
/// verification failure. Every other status is a verification failure.
fn outcome_for_verify_status(status: axum::http::StatusCode) -> WebhookOutcome {
    if status == axum::http::StatusCode::CONFLICT {
        WebhookOutcome::IdempotentReplay
    } else {
        WebhookOutcome::VerifyFailed
    }
}

/// Normalizes a resolved webhook delivery id: blank/whitespace-only is
/// treated as "no delivery id resolved", not a genuine (if empty) id.
///
/// The autumn-web JSON-body `"id"` fallback (`resolve_delivery_id` /
/// `json_string_field`) has no non-empty guard, unlike its header lookup.
/// So a `{"id": ""}` payload resolves `Some("")`, not `None`. Without this
/// step, all such deliveries share one idempotency key
/// (`{path}:{signal_name}:`). A `SignalsWithStart` target then silently
/// drops all but the first unrelated event (PR #918). A non-blank id is
/// returned untouched, not trimmed.
fn normalize_delivery_id(raw: Option<&str>) -> Option<String> {
    raw.map(str::to_string).filter(|d| !d.trim().is_empty())
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn handle_webhook(
    api_state: HarvestApiState,
    path: &'static str,
    target: WebhookTarget,
    handler: autumn_harvest::webhook_trigger::WebhookHandlerFn,
    queue: Option<&'static str>,
    headers: axum::http::HeaderMap,
    hook: Result<SignedWebhook, autumn_web::AutumnError>,
) -> axum::response::Response {
    // Check the verification result *before* the runtime-installed check. A
    // bad signature is a sender problem, not a server problem. It must be
    // reported as such (401/400), even during the boot window before
    // `on_startup` installs the runtime. A generic "runtime not started"
    // would misdirect a legitimate integration that debugs a real
    // signature/secret mismatch. Metrics are best-effort here. If the
    // runtime is not up yet, there is no `MetricsRecorder` to record against.
    // That is a documented boot-window limitation, as on every other
    // management route.
    let hook = match hook {
        Ok(hook) => hook,
        Err(err) => {
            if let Ok(runtime) = api_state.runtime() {
                let metrics = runtime.registry().telemetry().metrics.clone();
                record_outcome(
                    metrics.as_ref(),
                    path,
                    outcome_for_verify_status(err.status()),
                );
            }
            return err.into_response();
        }
    };

    // Deliberately NOT `map_error(e).into_response()`, which gives a `400`.
    // With `[security.webhooks].replay_protection` on, the extractor already
    // reserved this delivery's replay key. The autumn-web
    // `webhook_replay_cleanup_middleware` releases a reserved key only on a
    // `5xx`. A runtime that has not started is a transient boot-window
    // condition, not a bad request. A `400` would consume the delivery id
    // for good. The provider retry after boot would then get a false
    // duplicate `409` forever and never dispatch (PR #918).
    let metrics = match api_state.trigger_metrics() {
        Ok(metrics) => metrics,
        Err(e) => {
            return error_response(
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "runtime_not_started",
                &e,
            );
        }
    };

    // These routes skip `admit_mutation`, so they run the DR fence check
    // here (issue #1823). The refusal is a `503`. A `5xx` releases the replay
    // key, so the provider retries later against a node with authority.
    // The guards are held until this handler returns.
    let fence = match crate::api::enforce_dr_fence(&api_state).await {
        Ok(guards) => guards,
        Err(refusal) => return refusal.into_response(),
    };

    let payload: serde_json::Value = match serde_json::from_slice(hook.raw_body()) {
        Ok(v) => v,
        Err(e) => {
            record_outcome(metrics.as_ref(), path, WebhookOutcome::ParseFailed);
            return error_response(
                axum::http::StatusCode::BAD_REQUEST,
                "parse_failed",
                &format!("webhook body is not valid JSON: {e}"),
            );
        }
    };

    let delivery_id = normalize_delivery_id(hook.delivery_id());

    let ctx = WebhookCtx::new(
        path,
        hook.endpoint().to_string(),
        hook.provider().to_string(),
        delivery_id,
        hook.event_type().map(str::to_string),
        hook.raw_body().to_vec(),
    );

    if matches!(target, WebhookTarget::SignalsWithStart { .. }) && ctx.delivery_id.is_none() {
        record_outcome(metrics.as_ref(), path, WebhookOutcome::MissingIdempotency);
        return error_response(
            axum::http::StatusCode::BAD_REQUEST,
            "missing_idempotency",
            &format!(
                "endpoint '{}' resolves no delivery id -- a SignalsWithStart webhook target \
                 requires one (configure delivery_id_header under [security.webhooks] or \
                 include a top-level \"id\" field in the payload)",
                ctx.endpoint
            ),
        );
    }

    let workflow_id = match handler(&ctx, &payload) {
        Ok(id) => id,
        Err(WebhookHandlerError::Deserialize(msg)) => {
            record_outcome(metrics.as_ref(), path, WebhookOutcome::ParseFailed);
            return error_response(axum::http::StatusCode::BAD_REQUEST, "parse_failed", &msg);
        }
        Err(WebhookHandlerError::Rejected(msg)) => {
            record_outcome(metrics.as_ref(), path, WebhookOutcome::ParseFailed);
            return error_response(
                axum::http::StatusCode::BAD_REQUEST,
                "mapping_rejected",
                &msg,
            );
        }
    };

    // A SQLite app starts and signals through its handle (issue #2471).
    let sqlite = sqlite_dispatch(
        &api_state,
        target,
        path,
        &workflow_id,
        ctx.delivery_id.as_deref(),
        &payload,
    )
    .await;
    let dispatch_response = if let Some(response) = sqlite {
        response
    } else {
        match target {
            WebhookTarget::Starts { workflow } => {
                // Strip any upstream `Idempotency-Key` header before delegating.
                // Issue #808 made `start_workflow` read it as a start dedup key.
                // Many providers (Stripe et al.) send their OWN key, which is not
                // a Harvest start key. In #808's logic it would (a) merge two
                // DIFFERENT bindings or events that reuse a provider key into one
                // execution. It would also (b) trip the #808 `400` for a key with
                // throttle, debounce or batch, which breaks those targets.
                // Webhook starts already dedupe on the mapper's `workflow_id`
                // (reuse policy on `(workflow_name, workflow_id)`). `from_webhook`
                // sets the body `idempotency_key` to `None`, so the header is the
                // last source. So its removal fully disables #808 dedup for webhook
                // starts. The handler reads the same header-name constant, so the
                // two cannot drift.
                let mut start_headers = headers.clone();
                start_headers.remove(HEADER_IDEMPOTENCY_KEY);
                // A lost fence session stops the dispatch with a `503`.
                crate::api::run_dr_fenced(
                    &fence,
                    Box::pin(crate::api::start_workflow(
                        Extension(api_state.clone()),
                        axum::extract::Path(workflow.to_string()),
                        None,
                        None,
                        start_headers,
                        Ok(Json(StartWorkflowRequest::from_webhook(
                            workflow_id.as_str().to_string(),
                            payload,
                            queue.map(str::to_string),
                        ))),
                    )),
                )
                .await
            }
            WebhookTarget::SignalsWithStart {
                workflow,
                signal_name,
            } => {
                // Namespace the idempotency key by this binding (path +
                // signal_name), not the raw provider delivery id. The
                // `signal_with_start` dedupe key is only
                // `(workflow_name, workflow_id, idempotency_key)`. Two bindings
                // can target one (workflow_name, workflow_id) pair, for example
                // two providers that signal one entity workflow. With raw ids,
                // they collide whenever their delivery ids match. Small
                // sequential ids make that a real risk, and so does one event
                // redelivered to two configured paths. The second delivery then
                // reads as a replay and its signal is silently dropped. `path`
                // alone disambiguates each binding, because
                // `validate_webhook_triggers` forbids duplicate paths.
                // `signal_name` adds defense in depth. A same-binding redelivery
                // still maps to the same key, so dedup is unchanged.
                //
                // The upstream `Idempotency-Key` header cannot leak into #808
                // dedup here. `signal_with_start_workflow` reads its key only
                // from the request BODY (`request.idempotency_key`), never the
                // header. So this path needs no header stripping.
                let namespaced_idempotency_key = ctx
                    .delivery_id
                    .as_deref()
                    .map(|delivery_id| namespaced_key(path, signal_name, delivery_id));
                // A lost fence session stops the dispatch with a `503`.
                crate::api::run_dr_fenced(
                    &fence,
                    Box::pin(crate::api::signal_with_start_workflow(
                        Extension(api_state.clone()),
                        axum::extract::Path(workflow.to_string()),
                        None,
                        headers.clone(),
                        Json(SignalWithStartRequest::from_webhook(
                            workflow_id.as_str().to_string(),
                            payload,
                            signal_name.to_string(),
                            namespaced_idempotency_key,
                            queue.map(str::to_string),
                        )),
                    )),
                )
                .await
            }
        }
    };

    let route = format!("POST {path}");
    let audit_ctx = DispatchAuditCtx {
        api_state: &api_state,
        headers: &headers,
        route: &route,
        workflow_id: workflow_id.as_str(),
        delivery_id: ctx.delivery_id.as_deref(),
    };
    // The reshape writes an audit row, so it runs under the same guards. A
    // lost fence session skips it and answers `503`.
    crate::api::run_dr_fenced(
        &fence,
        reshape_dispatch_response(metrics.as_ref(), path, dispatch_response, audit_ctx),
    )
    .await
}

/// The signal-with-start key of one delivery: the binding's path, its
/// signal, then the provider delivery id. See the `SignalsWithStart` arm.
fn namespaced_key(path: &str, signal_name: &str, delivery_id: &str) -> String {
    format!("{path}:{signal_name}:{delivery_id}")
}

/// Dispatch one delivery through the SQLite handle (issue #2471).
///
/// It returns `None` on Postgres. On SQLite it answers the Postgres start or
/// signal-with-start response, so [`reshape_dispatch_response`] reads it
/// unchanged. Before startup it answers `503`, which releases the replay
/// key, so the provider retries.
#[cfg(feature = "sqlite")]
async fn sqlite_dispatch(
    api_state: &HarvestApiState,
    target: WebhookTarget,
    path: &str,
    workflow_id: &WorkflowId,
    delivery_id: Option<&str>,
    payload: &serde_json::Value,
) -> Option<axum::response::Response> {
    let handle = match api_state.sqlite_handle()? {
        Ok(handle) => handle,
        Err(error) => {
            return Some(error_response(
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "runtime_not_started",
                &error.to_string(),
            ));
        }
    };
    let workflow_id = workflow_id.as_str().to_string();
    Some(match target {
        WebhookTarget::Starts { workflow } => {
            crate::sqlite::triggers::start(&handle, workflow, Some(workflow_id), payload.clone())
                .await
        }
        WebhookTarget::SignalsWithStart {
            workflow,
            signal_name,
        } => {
            crate::sqlite::triggers::signal_with_start(
                &handle,
                workflow,
                workflow_id,
                signal_name.to_string(),
                payload.clone(),
                delivery_id.map(|delivery_id| namespaced_key(path, signal_name, delivery_id)),
            )
            .await
        }
    })
}

/// The Postgres build has no SQLite handle.
#[cfg(not(feature = "sqlite"))]
#[allow(clippy::unused_async)]
async fn sqlite_dispatch(
    _api_state: &HarvestApiState,
    _target: WebhookTarget,
    _path: &str,
    _workflow_id: &WorkflowId,
    _delivery_id: Option<&str>,
    _payload: &serde_json::Value,
) -> Option<axum::response::Response> {
    None
}

/// Context needed to write the `webhook.trigger` audit row for one dispatch
/// attempt, threaded through from the verified request.
struct DispatchAuditCtx<'a> {
    api_state: &'a HarvestApiState,
    headers: &'a axum::http::HeaderMap,
    route: &'a str,
    /// The mapping function's deterministic `WorkflowId`. It is the
    /// `target_id` fallback when the delegate produced no execution, as after
    /// a debounce or batch admission or a delegate rejection.
    workflow_id: &'a str,
    delivery_id: Option<&'a str>,
}

/// A minimal shape shared by [`crate::api`]'s `StartWorkflowResponse` and
/// `SignalWithStartResponse` -- both carry `execution_id`/`workflow_id`,
/// which is all this module needs to build its own envelope. Reading only
/// these two fields (rather than the private response types) avoids
/// widening the visibility of either type beyond what is needed.
/// `signal_delivered` is present only on `SignalWithStartResponse`. A
/// `SignalsWithStart` target reports `200 OK` for both "attached to an
/// existing execution and delivered a genuinely new signal" and "deduped
/// redelivery". The boolean is the only way this module can tell them apart.
#[derive(serde::Deserialize)]
struct DispatchIds {
    execution_id: String,
    workflow_id: String,
    #[serde(default)]
    signal_delivered: Option<bool>,
}

/// Rewrite a successful delegate response into the issue's
/// `{"status", "workflow_exec_id", "workflow_id"}` envelope.
/// A `202 Accepted` admission and a delegate error pass through unchanged.
/// Both already carry a structured JSON body. Each call writes the
/// `webhook.trigger` audit row, whether the delegate accepted or rejected
/// the request.
async fn reshape_dispatch_response(
    metrics: &dyn MetricsRecorder,
    path: &str,
    response: axum::response::Response,
    audit_ctx: DispatchAuditCtx<'_>,
) -> axum::response::Response {
    let status = response.status();

    // A debounce or batch gate deferred the start. There is no execution
    // yet, but this is a successful admission, not an internal error.
    if status == axum::http::StatusCode::ACCEPTED {
        record_outcome(metrics, path, WebhookOutcome::Accepted);
        write_dispatch_audit(&audit_ctx, STATUS_SUCCEEDED, None, None).await;
        return response;
    }

    // The delegate itself rejected the request (e.g. issue #373 schema
    // validation, a debounce/start_at conflict, a batching-max-size limit).
    // That is a caller/config problem, not an internal failure of this handler.
    // Pass the structured error body of the delegate through unchanged rather
    // than manufacturing a new one, and do not count it as an internal error.
    if status.is_client_error() {
        record_outcome(metrics, path, WebhookOutcome::ParseFailed);
        write_dispatch_audit(
            &audit_ctx,
            STATUS_FAILED,
            None,
            Some(&format!("dispatch rejected by delegate: {status}")),
        )
        .await;
        return response;
    }

    if status != axum::http::StatusCode::CREATED && status != axum::http::StatusCode::OK {
        record_outcome(metrics, path, WebhookOutcome::InternalError);
        write_dispatch_audit(
            &audit_ctx,
            STATUS_FAILED,
            None,
            Some(&format!("dispatch failed: {status}")),
        )
        .await;
        return response;
    }

    let bytes = match axum::body::to_bytes(response.into_body(), 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            record_outcome(metrics, path, WebhookOutcome::InternalError);
            write_dispatch_audit(
                &audit_ctx,
                STATUS_FAILED,
                None,
                Some(&format!("failed to read dispatch response: {e}")),
            )
            .await;
            return error_response(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                &format!("failed to read dispatch response: {e}"),
            );
        }
    };
    let parsed: DispatchIds = match serde_json::from_slice(&bytes) {
        Ok(p) => p,
        Err(e) => {
            record_outcome(metrics, path, WebhookOutcome::InternalError);
            write_dispatch_audit(
                &audit_ctx,
                STATUS_FAILED,
                None,
                Some(&format!("failed to parse dispatch response: {e}")),
            )
            .await;
            return error_response(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                &format!("failed to parse dispatch response: {e}"),
            );
        }
    };

    // `201 Created` is unambiguous (`Accepted`). A `200 OK` is ambiguous.
    // `signal_delivered == Some(true)` means a live execution got a new
    // signal, which is a first-time `Accepted` for the sender.
    // `Some(false)` or `None` means a deduped redelivery. A `Starts` target
    // returns `200 OK` only for an already-started `WorkflowId`.
    let outcome =
        if status == axum::http::StatusCode::CREATED || parsed.signal_delivered == Some(true) {
            WebhookOutcome::Accepted
        } else {
            WebhookOutcome::IdempotentReplay
        };
    record_outcome(metrics, path, outcome);
    write_dispatch_audit(
        &audit_ctx,
        STATUS_SUCCEEDED,
        Some(parsed.execution_id.as_str()),
        None,
    )
    .await;
    let http_status = if outcome == WebhookOutcome::Accepted {
        axum::http::StatusCode::ACCEPTED
    } else {
        axum::http::StatusCode::OK
    };
    (
        http_status,
        Json(serde_json::json!({
            "status": outcome.as_str(),
            "workflow_exec_id": parsed.execution_id,
            "workflow_id": parsed.workflow_id,
        })),
    )
        .into_response()
}

/// Write the `webhook.trigger` audit row for one dispatch attempt (issue
/// #344's audit AC). This is best-effort. A connection or insert failure is
/// logged and swallowed, not turned into a second error response. The
/// sender already has its real response, and a broken audit sink must not
/// un-deliver it.
///
/// With a known `exec_id`, the row goes to that execution's shard via
/// [`crate::api::db_conn_for_execution`], like every execution-scoped audit
/// write. With no execution yet (an admission or an early delegate
/// rejection), the row goes to the default shard. This matches the
/// debounce and batch admission audit writes in `api.rs`.
async fn write_dispatch_audit(
    audit_ctx: &DispatchAuditCtx<'_>,
    status: &'static str,
    exec_id: Option<&str>,
    error_summary: Option<&str>,
) {
    let (actor, source, request_id) =
        crate::api::audit_context(audit_ctx.headers, audit_ctx.api_state);

    // A SQLite app keeps its audit rows in the Harvest file (issue #2471).
    #[cfg(feature = "sqlite")]
    if audit_ctx.api_state.sqlite_handle().is_some() {
        let ar = NewAuditRecord {
            actor: &actor,
            operation: OP_WEBHOOK_TRIGGER,
            target_type: TARGET_WORKFLOW,
            target_id: exec_id.or(Some(audit_ctx.workflow_id)),
            route_or_command: audit_ctx.route,
            request_id: request_id.as_deref(),
            idempotency_key: audit_ctx.delivery_id,
            status,
            error_summary,
            shard_id: None,
            source: &source,
        };
        let written = match audit_ctx.api_state.auth_store() {
            Some(store) => match store.session().await {
                Ok(mut session) => session.insert_audit(&ar).await.map_err(|e| e.to_string()),
                Err(e) => Err(e.to_string()),
            },
            None => Err("the audit store is not open".to_owned()),
        };
        if let Err(e) = written {
            tracing::error!(error = %e, "audit insert failed for webhook.trigger");
        }
        return;
    }

    let acquired = if let Some(raw_exec_id) = exec_id {
        match crate::api::parse_execution_id(raw_exec_id) {
            Ok(id) => match crate::api::db_conn_for_execution(audit_ctx.api_state, id).await {
                Ok(conn) => Some((conn, Some(id.shard().as_i32()))),
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        "audit conn acquisition failed for webhook.trigger"
                    );
                    None
                }
            },
            Err(e) => {
                tracing::error!(error = %e, "invalid execution id for webhook.trigger audit");
                None
            }
        }
    } else {
        match audit_ctx.api_state.storage_pool() {
            Ok(pool) => match crate::api::acquire_conn(pool.default_pool()).await {
                Ok(conn) => Some((conn, None)),
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        "audit conn acquisition failed for webhook.trigger"
                    );
                    None
                }
            },
            Err(e) => {
                tracing::error!(error = %e, "storage pool unavailable for webhook.trigger audit");
                None
            }
        }
    };

    let Some((mut conn, shard_id)) = acquired else {
        return;
    };

    let ar = NewAuditRecord {
        actor: &actor,
        operation: OP_WEBHOOK_TRIGGER,
        target_type: TARGET_WORKFLOW,
        target_id: exec_id.or(Some(audit_ctx.workflow_id)),
        route_or_command: audit_ctx.route,
        request_id: request_id.as_deref(),
        idempotency_key: audit_ctx.delivery_id,
        status,
        error_summary,
        shard_id,
        source: &source,
    };
    if let Err(e) = audit::insert_audit(&mut conn, &ar).await {
        tracing::error!(error = %e, "audit insert failed for webhook.trigger");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Before startup a SQLite app answers `503`, which releases the replay
    /// key. A delivery never reaches the Postgres start path (issue #2471).
    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn a_sqlite_delivery_before_startup_is_a_503() {
        let api_state = HarvestApiState::new();
        api_state.set_sqlite_backend();
        let response = sqlite_dispatch(
            &api_state,
            WebhookTarget::Starts { workflow: "orders" },
            "/hooks/orders",
            &WorkflowId::new("o-1"),
            Some("dlv-1"),
            &serde_json::json!({}),
        )
        .await
        .expect("a SQLite app answers on its own path");
        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert!(
            api_state.trigger_metrics().is_err(),
            "no metrics before startup"
        );
        // A Postgres app takes its own path.
        let postgres = HarvestApiState::new();
        assert!(
            sqlite_dispatch(
                &postgres,
                WebhookTarget::Starts { workflow: "orders" },
                "/hooks/orders",
                &WorkflowId::new("o-1"),
                None,
                &serde_json::json!({}),
            )
            .await
            .is_none()
        );
    }

    #[derive(Default)]
    struct RecordingMetrics {
        received: std::sync::Mutex<Vec<(String, WebhookOutcome)>>,
        rejected: std::sync::Mutex<Vec<(String, WebhookOutcome)>>,
    }

    impl MetricsRecorder for RecordingMetrics {
        fn record_webhook_received(&self, path: &str, outcome: WebhookOutcome) {
            self.received
                .lock()
                .unwrap()
                .push((path.to_string(), outcome));
        }
        fn record_webhook_rejected(&self, path: &str, outcome: WebhookOutcome) {
            self.rejected
                .lock()
                .unwrap()
                .push((path.to_string(), outcome));
        }
    }

    #[test]
    fn normalize_delivery_id_treats_none_as_none() {
        assert_eq!(normalize_delivery_id(None), None);
    }

    #[test]
    fn normalize_delivery_id_treats_blank_and_whitespace_as_none() {
        assert_eq!(normalize_delivery_id(Some("")), None);
        assert_eq!(normalize_delivery_id(Some("   ")), None);
        assert_eq!(normalize_delivery_id(Some("\t\n")), None);
    }

    #[test]
    fn normalize_delivery_id_preserves_a_genuine_id_untrimmed() {
        assert_eq!(
            normalize_delivery_id(Some("evt_123")),
            Some("evt_123".to_string())
        );
        // A real (non-blank) id with incidental surrounding whitespace is
        // passed through as-is -- only the all-whitespace case is missing.
        assert_eq!(
            normalize_delivery_id(Some(" evt_123 ")),
            Some(" evt_123 ".to_string())
        );
    }

    #[test]
    fn outcome_for_verify_status_maps_conflict_to_idempotent_replay() {
        assert_eq!(
            outcome_for_verify_status(axum::http::StatusCode::CONFLICT),
            WebhookOutcome::IdempotentReplay
        );
    }

    #[test]
    fn outcome_for_verify_status_maps_everything_else_to_verify_failed() {
        for status in [
            axum::http::StatusCode::BAD_REQUEST,
            axum::http::StatusCode::UNAUTHORIZED,
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        ] {
            assert_eq!(
                outcome_for_verify_status(status),
                WebhookOutcome::VerifyFailed
            );
        }
    }

    #[test]
    fn record_outcome_always_records_received() {
        let m = RecordingMetrics::default();
        for outcome in [
            WebhookOutcome::Accepted,
            WebhookOutcome::IdempotentReplay,
            WebhookOutcome::VerifyFailed,
            WebhookOutcome::ParseFailed,
            WebhookOutcome::MissingIdempotency,
            WebhookOutcome::InternalError,
        ] {
            record_outcome(&m, "/hooks/x", outcome);
        }
        assert_eq!(m.received.lock().unwrap().len(), 6);
    }

    #[test]
    fn record_outcome_only_rejects_failure_outcomes() {
        let m = RecordingMetrics::default();
        record_outcome(&m, "/hooks/x", WebhookOutcome::Accepted);
        record_outcome(&m, "/hooks/x", WebhookOutcome::IdempotentReplay);
        assert!(m.rejected.lock().unwrap().is_empty());

        record_outcome(&m, "/hooks/x", WebhookOutcome::VerifyFailed);
        record_outcome(&m, "/hooks/x", WebhookOutcome::ParseFailed);
        record_outcome(&m, "/hooks/x", WebhookOutcome::MissingIdempotency);
        record_outcome(&m, "/hooks/x", WebhookOutcome::InternalError);
        assert_eq!(m.rejected.lock().unwrap().len(), 4);
    }

    fn no_db_audit_ctx<'a>(
        api_state: &'a HarvestApiState,
        headers: &'a axum::http::HeaderMap,
    ) -> DispatchAuditCtx<'a> {
        DispatchAuditCtx {
            api_state,
            headers,
            route: "POST /hooks/test",
            workflow_id: "wf-1",
            delivery_id: None,
        }
    }

    /// Every case below runs `write_dispatch_audit` transitively. A fresh
    /// no-DB [`HarvestApiState`] has no storage pool, so the audit write is
    /// a logged no-op, not a panic. So the outcome logic in
    /// [`reshape_dispatch_response`] runs here without Docker.
    /// `webhook_receiver_integration.rs` covers the audit *row* content.
    #[tokio::test]
    async fn reshape_passes_a_debounced_202_through_unchanged_as_accepted() {
        let m = RecordingMetrics::default();
        let api_state = HarvestApiState::new();
        let headers = axum::http::HeaderMap::new();
        let debounce_body = serde_json::json!({
            "debounced": true,
            "workflow_name": "wf",
            "workflow_id": "wf-1",
            "debounce_key": "k",
            "fire_at": "2026-01-01T00:00:00Z",
            "pending_count": 3,
        });
        let response = (
            axum::http::StatusCode::ACCEPTED,
            Json(debounce_body.clone()),
        )
            .into_response();

        let out = reshape_dispatch_response(
            &m,
            "/hooks/x",
            response,
            no_db_audit_ctx(&api_state, &headers),
        )
        .await;

        assert_eq!(out.status(), axum::http::StatusCode::ACCEPTED);
        assert_eq!(
            m.received.lock().unwrap().as_slice(),
            [("/hooks/x".to_string(), WebhookOutcome::Accepted)]
        );
        assert!(m.rejected.lock().unwrap().is_empty());
        let bytes = axum::body::to_bytes(out.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            parsed, debounce_body,
            "debounce body must pass through byte-for-byte"
        );
    }

    #[tokio::test]
    async fn reshape_classifies_delegate_client_error_as_parse_failed_not_internal() {
        let m = RecordingMetrics::default();
        let api_state = HarvestApiState::new();
        let headers = axum::http::HeaderMap::new();
        let response = (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "input validation failed"})),
        )
            .into_response();

        let out = reshape_dispatch_response(
            &m,
            "/hooks/x",
            response,
            no_db_audit_ctx(&api_state, &headers),
        )
        .await;

        assert_eq!(out.status(), axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(
            m.received.lock().unwrap().as_slice(),
            [("/hooks/x".to_string(), WebhookOutcome::ParseFailed)]
        );
        assert_eq!(
            m.rejected.lock().unwrap().as_slice(),
            [("/hooks/x".to_string(), WebhookOutcome::ParseFailed)],
            "an ordinary delegate-side 4xx must not be counted as internal_error"
        );
    }

    #[tokio::test]
    async fn reshape_classifies_ok_with_signal_delivered_true_as_accepted() {
        let m = RecordingMetrics::default();
        let api_state = HarvestApiState::new();
        let headers = axum::http::HeaderMap::new();
        let response = (
            axum::http::StatusCode::OK,
            Json(serde_json::json!({
                "execution_id": "e1",
                "workflow_name": "wf",
                "workflow_id": "wf-1",
                "state": "RUNNING",
                "started_fresh": false,
                "signal_delivered": true,
            })),
        )
            .into_response();

        let out = reshape_dispatch_response(
            &m,
            "/hooks/x",
            response,
            no_db_audit_ctx(&api_state, &headers),
        )
        .await;

        assert_eq!(
            out.status(),
            axum::http::StatusCode::ACCEPTED,
            "a genuinely new signal delivered to an attached execution is `accepted`, not a replay"
        );
        assert_eq!(
            m.received.lock().unwrap().as_slice(),
            [("/hooks/x".to_string(), WebhookOutcome::Accepted)]
        );
        assert!(m.rejected.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn reshape_classifies_ok_with_signal_delivered_false_as_idempotent_replay() {
        let m = RecordingMetrics::default();
        let api_state = HarvestApiState::new();
        let headers = axum::http::HeaderMap::new();
        let response = (
            axum::http::StatusCode::OK,
            Json(serde_json::json!({
                "execution_id": "e1",
                "workflow_name": "wf",
                "workflow_id": "wf-1",
                "state": "RUNNING",
                "started_fresh": false,
                "signal_delivered": false,
            })),
        )
            .into_response();

        let out = reshape_dispatch_response(
            &m,
            "/hooks/x",
            response,
            no_db_audit_ctx(&api_state, &headers),
        )
        .await;

        assert_eq!(out.status(), axum::http::StatusCode::OK);
        assert_eq!(
            m.received.lock().unwrap().as_slice(),
            [("/hooks/x".to_string(), WebhookOutcome::IdempotentReplay)]
        );
        assert!(m.rejected.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn reshape_classifies_ok_with_no_signal_delivered_field_as_idempotent_replay() {
        // The `Starts` target's response shape has no `signal_delivered`
        // field at all -- its `200 OK` is always a redelivery.
        let m = RecordingMetrics::default();
        let api_state = HarvestApiState::new();
        let headers = axum::http::HeaderMap::new();
        let response = (
            axum::http::StatusCode::OK,
            Json(serde_json::json!({
                "execution_id": "e1",
                "workflow_name": "wf",
                "workflow_id": "wf-1",
                "state": "RUNNING",
            })),
        )
            .into_response();

        let out = reshape_dispatch_response(
            &m,
            "/hooks/x",
            response,
            no_db_audit_ctx(&api_state, &headers),
        )
        .await;

        assert_eq!(out.status(), axum::http::StatusCode::OK);
        assert_eq!(
            m.received.lock().unwrap().as_slice(),
            [("/hooks/x".to_string(), WebhookOutcome::IdempotentReplay)]
        );
    }

    #[tokio::test]
    async fn reshape_classifies_created_as_accepted() {
        let m = RecordingMetrics::default();
        let api_state = HarvestApiState::new();
        let headers = axum::http::HeaderMap::new();
        let response = (
            axum::http::StatusCode::CREATED,
            Json(serde_json::json!({
                "execution_id": "e1",
                "workflow_name": "wf",
                "workflow_id": "wf-1",
                "state": "RUNNING",
            })),
        )
            .into_response();

        let out = reshape_dispatch_response(
            &m,
            "/hooks/x",
            response,
            no_db_audit_ctx(&api_state, &headers),
        )
        .await;

        assert_eq!(out.status(), axum::http::StatusCode::ACCEPTED);
        assert_eq!(
            m.received.lock().unwrap().as_slice(),
            [("/hooks/x".to_string(), WebhookOutcome::Accepted)]
        );
    }

    #[tokio::test]
    async fn reshape_classifies_a_genuine_5xx_as_internal_error() {
        let m = RecordingMetrics::default();
        let api_state = HarvestApiState::new();
        let headers = axum::http::HeaderMap::new();
        let response = (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "storage pool exhausted"})),
        )
            .into_response();

        let out = reshape_dispatch_response(
            &m,
            "/hooks/x",
            response,
            no_db_audit_ctx(&api_state, &headers),
        )
        .await;

        assert_eq!(out.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            m.received.lock().unwrap().as_slice(),
            [("/hooks/x".to_string(), WebhookOutcome::InternalError)]
        );
        assert_eq!(
            m.rejected.lock().unwrap().as_slice(),
            [("/hooks/x".to_string(), WebhookOutcome::InternalError)]
        );
    }
}
