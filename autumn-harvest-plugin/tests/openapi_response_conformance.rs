#![allow(clippy::doc_markdown)]
#![allow(clippy::too_many_lines)]
//! Live proof of the typed core-route responses (issue #1616).
//!
//! The published TypeScript client trusts the types on
//! `CORE_CLIENT_ROUTES`. This suite drives each of those routes against
//! Postgres and checks every body against the published schema. The check
//! is strict:
//!
//! * Each property in a body must be declared. An undeclared key fails, so a
//!   contract that omits a field fails.
//! * Each declared property must carry a type, or `x-harvest-any`.
//! * Each value must match its type. `null` must be declared.
//! * Each `required` property must be present.
//!
//! The suite uses `HARVEST_TEST_DATABASE_URL` when it is set. Otherwise it
//! starts a testcontainers Postgres.

use std::pin::Pin;
use std::sync::Arc;

use autumn_harvest::debounce::DebouncePolicy;
use autumn_harvest::event_batch::BatchPolicy;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::throttle::ThrottlePolicy;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::{WorkflowInfo, context::WorkflowContext};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_harvest_plugin::openapi::{CORE_CLIENT_ROUTES, openapi_document};
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(autumn_harvest::test_init_sql().as_bytes().to_vec())
        .with_tag("16")
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool should build")
}

fn noop_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(json!({ "status": "ok" })) })
}

fn info(name: &'static str) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "tests",
        handler: noop_workflow,
        execution_timeout: None,
        chain_execution_timeout: None,
        sla: None,
        concurrency: None,
        debounce: None,
        batch: None,
        throttle: None,
        max_input_bytes: None,
        owner: None,
        runbook_url: None,
        severity: None,
        description: None,
        input_schema: None,
        output_schema: None,
        error_schema: None,
        retry_policy: None,
    }
}

const PLAIN: &str = "conformance_plain";
const DEBOUNCED: &str = "conformance_debounced";
const BATCHED: &str = "conformance_batched";
const FLUSHED: &str = "conformance_flushed";
const THROTTLED: &str = "conformance_throttled";

fn registry() -> HandlerRegistry {
    let mut debounced = info(DEBOUNCED);
    debounced.debounce = Some(DebouncePolicy {
        key_expr: "input.tenant_id",
        window: std::time::Duration::from_secs(30),
        max_wait: None,
    });
    let mut batched = info(BATCHED);
    batched.batch = Some(BatchPolicy {
        key_expr: "input.tenant_id".to_string(),
        max_size: 10,
        max_wait: std::time::Duration::from_secs(10),
    });
    // A batch of one flushes at once, so the start returns 201.
    let mut flushed = info(FLUSHED);
    flushed.batch = Some(BatchPolicy {
        key_expr: "input.tenant_id".to_string(),
        max_size: 1,
        max_wait: std::time::Duration::from_secs(10),
    });
    // A burst of one defers the second start in the same minute.
    let mut throttled = info(THROTTLED);
    throttled.throttle = Some(
        ThrottlePolicy::from_rate_str("1/m", Some(1.0), Some("input.tenant_id"), None)
            .expect("valid rate"),
    );
    HandlerRegistry::new(
        vec![info(PLAIN), debounced, batched, flushed, throttled],
        vec![],
    )
}

fn build_app(pool: &DbPool) -> axum::Router {
    let api_state = HarvestApiState::new();
    api_state.set_allow_unauthenticated_mutations(true);
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::new(registry()),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("conformance-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state)
}

/// One request and its parsed JSON body (`Null` when empty).
async fn call(
    app: &axum::Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
    headers: &[(&str, &str)],
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    let request = match body {
        Some(body) => builder
            .header("content-type", "application/json")
            .body(Body::from(body.to_string())),
        None => builder.body(Body::empty()),
    }
    .unwrap();
    let response = app.clone().oneshot(request).await.expect("request");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| panic!("{method} {uri}: body is not JSON: {bytes:?}"))
    };
    (status, json)
}

/// True when `value` has the JSON Schema type `name`.
fn has_type(value: &Value, name: &str) -> bool {
    match name {
        "string" => value.is_string(),
        "integer" => value.is_i64() || value.is_u64(),
        "number" => value.is_number(),
        "boolean" => value.is_boolean(),
        "object" => value.is_object(),
        "array" => value.is_array(),
        "null" => value.is_null(),
        other => panic!("unknown schema type {other}"),
    }
}

/// Check `value` against `schema` and collect each mismatch in `errors`.
fn check(value: &Value, schema: &Value, at: &str, errors: &mut Vec<String>) {
    if schema["x-harvest-any"] == true {
        return;
    }
    let types: Vec<&str> = match &schema["type"] {
        Value::String(one) => vec![one.as_str()],
        Value::Array(many) => many.iter().filter_map(Value::as_str).collect(),
        _ => {
            errors.push(format!("{at}: the schema declares no type"));
            return;
        }
    };
    if !types.iter().any(|name| has_type(value, name)) {
        errors.push(format!("{at}: {value} is not {types:?}"));
        return;
    }
    if let Value::Object(map) = value {
        let properties = schema["properties"].as_object();
        for required in schema["required"].as_array().into_iter().flatten() {
            let name = required.as_str().unwrap_or_default();
            if !map.contains_key(name) {
                errors.push(format!("{at}.{name}: required but absent"));
            }
        }
        let open = schema["additionalProperties"] == true;
        for (name, field) in map {
            match properties.and_then(|declared| declared.get(name)) {
                Some(declared) => check(field, declared, &format!("{at}.{name}"), errors),
                None if open => {}
                None => errors.push(format!("{at}.{name}: not declared")),
            }
        }
    }
    if let (Value::Array(items), Some(item_schema)) = (value, schema.get("items")) {
        for (index, item) in items.iter().enumerate() {
            check(item, item_schema, &format!("{at}[{index}]"), errors);
        }
    }
}

/// Records which core routes the suite reached, and every mismatch.
#[derive(Default)]
struct Conformance {
    reached: Vec<(String, String)>,
    errors: Vec<String>,
}

impl Conformance {
    /// Check one response against the published document.
    fn record(&mut self, method: &str, route: &str, status: StatusCode, body: &Value) {
        assert!(
            status.is_success(),
            "{method} {route}: expected a 2xx, got {status}: {body}"
        );
        let at = format!("{method} {route} {}", status.as_u16());
        let response = &openapi_document()["paths"][route][method.to_lowercase()]["responses"]
            [status.as_str()];
        assert!(response.is_object(), "{at}: the status is not documented");
        self.reached.push((method.to_owned(), route.to_owned()));
        if body.is_null() {
            assert!(
                response.get("content").is_none(),
                "{at}: the document declares a body, the handler sent none"
            );
            return;
        }
        let schema = &response["content"]["application/json"]["schema"];
        check(body, schema, &at, &mut self.errors);
    }
}

async fn start(app: &axum::Router, workflow: &str, body: Value) -> (StatusCode, Value) {
    call(
        app,
        "POST",
        &format!("/workflows/{workflow}/start"),
        Some(body),
        &[],
    )
    .await
}

#[tokio::test]
async fn core_routes_match_their_published_schema() {
    let (url, _container) = setup_db().await;
    let app = build_app(&build_pool(&url));
    let run = uuid::Uuid::new_v4();
    let mut seen = Conformance::default();
    let start_route = "/workflows/{workflow_name}/start";

    let (status, body) = call(&app, "GET", "/health", None, &[]).await;
    seen.record("GET", "/health", status, &body);

    // A fresh start: 201.
    let (status, started) = start(&app, PLAIN, json!({ "workflow_id": format!("a-{run}") })).await;
    seen.record("POST", start_route, status, &started);
    let exec_id = started["execution_id"]
        .as_str()
        .expect("execution_id")
        .to_owned();

    // An idempotent replay: 200 with `deduplicated`.
    let key = format!("key-{run}");
    for _ in 0..2 {
        let (status, body) = call(
            &app,
            "POST",
            &format!("/workflows/{PLAIN}/start"),
            Some(json!({ "workflow_id": format!("b-{run}") })),
            &[("idempotency-key", key.as_str())],
        )
        .await;
        seen.record("POST", start_route, status, &body);
    }

    // A pinned start echoes `shard_id`.
    let (status, body) = start(
        &app,
        PLAIN,
        json!({ "workflow_id": format!("c-{run}"), "shard_id": 0 }),
    )
    .await;
    seen.record("POST", start_route, status, &body);

    // The deferred and batched shapes.
    let tenant = json!({ "tenant_id": format!("t-{run}") });
    for workflow in [DEBOUNCED, BATCHED, FLUSHED, THROTTLED, THROTTLED] {
        let body = json!({ "workflow_id": uuid::Uuid::new_v4().to_string(), "input": tenant });
        let (status, body) = start(&app, workflow, body).await;
        seen.record("POST", start_route, status, &body);
    }

    let (status, body) = call(&app, "GET", &format!("/workflows/{exec_id}"), None, &[]).await;
    seen.record("GET", "/workflows/{id}", status, &body);

    // A running execution has no result yet: 204.
    let (status, body) = call(
        &app,
        "GET",
        &format!("/workflows/{exec_id}/result"),
        None,
        &[],
    )
    .await;
    seen.record("GET", "/workflows/{id}/result", status, &body);

    let (status, body) = call(
        &app,
        "POST",
        &format!("/workflows/{exec_id}/signal/go"),
        Some(json!({ "payload": { "n": 1 } })),
        &[],
    )
    .await;
    seen.record(
        "POST",
        "/workflows/{id}/signal/{signal_name}",
        status,
        &body,
    );

    let (status, body) = call(
        &app,
        "POST",
        &format!("/workflows/{exec_id}/cancel"),
        Some(json!({ "reason": "conformance" })),
        &[],
    )
    .await;
    seen.record("POST", "/workflows/{id}/cancel", status, &body);

    // A cancelled execution has a result: 200.
    let (status, body) = call(
        &app,
        "GET",
        &format!("/workflows/{exec_id}/result"),
        None,
        &[],
    )
    .await;
    seen.record("GET", "/workflows/{id}/result", status, &body);

    // Status after cancel carries `completed_at` and history.
    let (status, body) = call(&app, "GET", &format!("/workflows/{exec_id}"), None, &[]).await;
    seen.record("GET", "/workflows/{id}", status, &body);

    let (_, other) = start(&app, PLAIN, json!({ "workflow_id": format!("d-{run}") })).await;
    let other_id = other["execution_id"].as_str().expect("execution_id");
    let (status, body) = call(
        &app,
        "POST",
        &format!("/workflows/{other_id}/terminate"),
        Some(json!({ "reason": "conformance" })),
        &[],
    )
    .await;
    seen.record("POST", "/workflows/{id}/terminate", status, &body);

    for (method, route) in CORE_CLIENT_ROUTES {
        assert!(
            seen.reached.iter().any(|(m, r)| m == method && r == route),
            "the suite never reached {method} {route}"
        );
    }
    assert!(
        seen.errors.is_empty(),
        "responses do not match the published schema:\n{:#?}",
        seen.errors
    );
}
