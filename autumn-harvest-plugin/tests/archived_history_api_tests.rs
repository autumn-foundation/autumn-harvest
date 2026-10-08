//! No-database tests for the archived-history read path (issue #1983).
//!
//! `GET /workflows/{id}/archived-history` and its Vantage page read only from
//! the history archiver on the installed runtime. These tests drive the real
//! routers with a fake archiver.

use std::collections::HashMap;
use std::sync::Arc;

use autumn_harvest::WorkflowEvent;
use autumn_harvest::aead_codec::{AeadCodec, DataKey};
use autumn_harvest::history_export::{
    HistoryExportDocument, HistoryExportRequest, HistoryPayloadPolicy, export_history,
};
use autumn_harvest::payload_codec::{PayloadCodecs, is_codec_envelope};
use autumn_harvest::retention::{
    ArchiveFetchError, ArchiveFetchFuture, ArchiverFuture, HistoryArchiver, RetentionConfig,
};
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::HandlerRegistry;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_harvest_plugin::ui::harvest_ui_router;
use autumn_web::reexports::axum::Router;
use autumn_web::reexports::axum::body::{Body, to_bytes};
use autumn_web::reexports::http::{Method, Request, StatusCode};
use autumn_web::session::Session;
use tower::ServiceExt;

const MARKER: &str = "PLAINTEXT-MARKER-1983";

/// How the fake archiver answers a fetch.
#[derive(Clone)]
enum Mode {
    /// Serve the stored documents.
    Serve(HashMap<ExecutionId, HistoryExportDocument>),
    /// Use the trait default, which cannot read back.
    WriteOnly,
    /// Fail every fetch.
    Fail,
}

struct FakeArchiver(Mode);

impl HistoryArchiver for FakeArchiver {
    fn archive(&self, _doc: &HistoryExportDocument) -> ArchiverFuture<'_> {
        Box::pin(async { Ok(()) })
    }

    fn fetch(&self, execution_id: &ExecutionId) -> ArchiveFetchFuture<'_> {
        let result = match &self.0 {
            Mode::Serve(docs) => Ok(docs.get(execution_id).cloned()),
            Mode::WriteOnly => Err(ArchiveFetchError::Unsupported),
            Mode::Fail => Err(ArchiveFetchError::Backend("bucket unreachable".into())),
        };
        Box::pin(async move { result })
    }
}

fn doc_with_events(
    execution_id: ExecutionId,
    events: Vec<serde_json::Value>,
) -> HistoryExportDocument {
    let mut doc = export_history(HistoryExportRequest {
        workflow_name: "archived_wf".to_string(),
        workflow_id: Some("order-42".to_string()),
        queue_name: Some("default".to_string()),
        execution_id,
        shard_id: 0,
        state: "COMPLETED".to_string(),
        events: Vec::new(),
        exported_at: chrono::Utc::now(),
        payload_policy: HistoryPayloadPolicy::Full,
        max_bytes: Some(usize::MAX),
        context_headers: None,
        execution_timeout: None,
        deadline_at: None,
        parent_execution_id: None,
    })
    .unwrap();
    doc.event_count = events.len();
    doc.events = events;
    doc
}

fn plain_event() -> serde_json::Value {
    serde_json::to_value(WorkflowEvent::WorkflowCompleted {
        output: serde_json::json!({ "secret": MARKER }),
    })
    .unwrap()
}

fn runtime(archiver: Option<Arc<dyn HistoryArchiver>>) -> HarvestApiRuntime {
    let runtime = HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        None,
        Vec::new(),
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(RetentionConfig::default()),
        ShardRouter::single(),
    );
    match archiver {
        Some(archiver) => runtime.with_history_archiver(archiver),
        None => runtime,
    }
}

fn state_with(archiver: Option<Arc<dyn HistoryArchiver>>) -> HarvestApiState {
    let state = HarvestApiState::new();
    state.install(runtime(archiver));
    state
}

fn serving(doc: HistoryExportDocument) -> Arc<dyn HistoryArchiver> {
    let mut docs = HashMap::new();
    docs.insert(doc.execution_id, doc);
    Arc::new(FakeArchiver(Mode::Serve(docs)))
}

fn state_serving(doc: HistoryExportDocument) -> HarvestApiState {
    state_with(Some(serving(doc)))
}

fn app(state: HarvestApiState) -> Router {
    harvest_api_router(state.clone()).nest("/ui", harvest_ui_router(state))
}

fn get(uri: &str) -> Request<Body> {
    Request::builder()
        .method(Method::GET)
        .uri(uri)
        .body(Body::empty())
        .unwrap()
}

fn get_as_admin(uri: &str) -> Request<Body> {
    let mut request = get(uri);
    let mut data = HashMap::new();
    data.insert("user_id".to_string(), "operator-1".to_string());
    data.insert("role".to_string(), "admin".to_string());
    request.extensions_mut().insert(Session::new_for_test(
        "harvest-test-session".to_string(),
        data,
    ));
    request
}

async fn send(state: HarvestApiState, request: Request<Body>) -> (StatusCode, String) {
    let response = app(state).oneshot(request).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, String::from_utf8(body.to_vec()).unwrap())
}

fn api_path(id: &ExecutionId) -> String {
    format!("/workflows/{id}/archived-history")
}

fn ui_path(id: &ExecutionId) -> String {
    format!("/ui/workflows/{id}/archived-history")
}

#[tokio::test]
async fn unauthenticated_request_is_blocked() {
    let id = ExecutionId::new();
    let state = state_serving(doc_with_events(id, vec![plain_event()]));
    let (status, _) = send(state.clone(), get(&api_path(&id))).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = send(state, get(&ui_path(&id))).await;
    assert_ne!(status, StatusCode::OK, "the page needs admin too");
}

#[tokio::test]
async fn archived_history_is_served() {
    let id = ExecutionId::new();
    let state = state_serving(doc_with_events(id, vec![plain_event()]));
    let (status, body) = send(state, get_as_admin(&api_path(&id))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["execution_id"], id.to_string());
    assert_eq!(json["workflow_name"], "archived_wf");
    assert_eq!(json["schema"], "autumn-harvest.history-export");
    assert_eq!(json["events"][0]["type"], "WorkflowCompleted");
    assert_eq!(json["events"][0]["data"]["output"]["secret"], MARKER);
}

#[tokio::test]
async fn a_run_missing_from_the_archive_is_not_found() {
    let state = state_serving(doc_with_events(ExecutionId::new(), vec![]));
    let (status, body) = send(state, get_as_admin(&api_path(&ExecutionId::new()))).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
}

#[tokio::test]
async fn no_archiver_is_service_unavailable() {
    let state = state_with(None);
    let (status, body) = send(state, get_as_admin(&api_path(&ExecutionId::new()))).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert!(body.contains("archiver"), "{body}");
}

#[tokio::test]
async fn a_write_only_archiver_is_service_unavailable() {
    let state = state_with(Some(Arc::new(FakeArchiver(Mode::WriteOnly))));
    let (status, body) = send(state, get_as_admin(&api_path(&ExecutionId::new()))).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
}

#[tokio::test]
async fn a_store_failure_is_service_unavailable() {
    let state = state_with(Some(Arc::new(FakeArchiver(Mode::Fail))));
    let (status, body) = send(state, get_as_admin(&api_path(&ExecutionId::new()))).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert!(body.contains("bucket unreachable"), "{body}");
}

#[tokio::test]
async fn an_invalid_id_is_a_bad_request() {
    let state = state_with(None);
    let (status, _) = send(
        state,
        get_as_admin("/workflows/not-a-uuid/archived-history"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

fn codecs() -> PayloadCodecs {
    let codecs = PayloadCodecs::default();
    let codec = AeadCodec::new("k-ui", &DataKey::generate()).unwrap();
    codecs.register_key("k-ui", Arc::new(codec)).unwrap();
    codecs.set_active_key("k-ui").unwrap();
    codecs
}

fn encoded_doc(codecs: &PayloadCodecs, id: ExecutionId) -> HistoryExportDocument {
    let event = codecs
        .encode_event(&WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({ "secret": MARKER }),
        })
        .unwrap();
    doc_with_events(id, vec![event])
}

#[tokio::test]
async fn payloads_decode_under_the_read_path_gate() {
    let codecs = codecs();
    let id = ExecutionId::new();
    let state = state_serving(encoded_doc(&codecs, id));
    state.set_payload_codecs(codecs);
    state.set_decode_payloads_on_read(true);
    let (status, body) = send(state, get_as_admin(&api_path(&id))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["events"][0]["data"]["output"]["secret"], MARKER);
}

#[tokio::test]
async fn payloads_stay_encoded_when_decode_on_read_is_off() {
    let codecs = codecs();
    let id = ExecutionId::new();
    let state = state_serving(encoded_doc(&codecs, id));
    state.set_payload_codecs(codecs);
    let (status, body) = send(state, get_as_admin(&api_path(&id))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(!body.contains(MARKER), "no plaintext without the gate");
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(is_codec_envelope(&json["events"][0]["data"]["output"]));
}

#[tokio::test]
async fn vantage_shows_the_archived_history() {
    let id = ExecutionId::new();
    let state = state_serving(doc_with_events(id, vec![plain_event()]));
    let (status, html) = send(state, get_as_admin(&ui_path(&id))).await;
    assert_eq!(status, StatusCode::OK, "{html}");
    assert!(html.contains("Archived history"), "page title");
    assert!(html.contains("archived_wf"), "workflow name");
    assert!(html.contains("order-42"), "workflow id");
    assert!(html.contains("WorkflowCompleted"), "event type");
    assert!(html.contains(MARKER), "event data");
    assert!(!html.contains("<script"), "Vantage pages carry no script");
}

#[tokio::test]
async fn vantage_reports_a_missing_archive() {
    let state = state_serving(doc_with_events(ExecutionId::new(), vec![]));
    let (status, html) = send(state, get_as_admin(&ui_path(&ExecutionId::new()))).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{html}");
}

#[tokio::test]
async fn vantage_reports_no_archiver() {
    let state = state_with(None);
    let (status, html) = send(state, get_as_admin(&ui_path(&ExecutionId::new()))).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{html}");
}

#[tokio::test]
async fn vantage_escapes_event_data() {
    let id = ExecutionId::new();
    let event = serde_json::to_value(WorkflowEvent::WorkflowCompleted {
        output: serde_json::json!({ "html": "<img src=x onerror=alert(1)>" }),
    })
    .unwrap();
    let state = state_serving(doc_with_events(id, vec![event]));
    let (status, html) = send(state, get_as_admin(&ui_path(&id))).await;
    assert_eq!(status, StatusCode::OK);
    assert!(!html.contains("<img src=x"), "event data is escaped");
}

#[tokio::test]
async fn vantage_caps_a_long_history() {
    let id = ExecutionId::new();
    let events = (0..1001).map(|_| plain_event()).collect();
    let state = state_serving(doc_with_events(id, events));
    let (status, html) = send(state, get_as_admin(&ui_path(&id))).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("first 1000 events"),
        "the page says it is capped"
    );
    assert_eq!(
        html.matches("view payload").count(),
        1000,
        "1000 rows render"
    );
}
