//! End-to-end check shared by the S3 and GCS emulator suites (issue #1983).
//!
//! One retention pass archives a codec-encoded run to the object store. The
//! test then reads the run back through the management API and Vantage.

use std::sync::Arc;

use autumn_harvest::aead_codec::{AeadCodec, DataKey};
use autumn_harvest::payload_codec::is_codec_envelope;
use autumn_harvest::types::ExecutionId;
use autumn_harvest::{RetentionConfig, WorkflowEvent};
use autumn_harvest_plugin::api::{HarvestApiState, harvest_api_router};
use autumn_harvest_plugin::object_store::{ObjectBackend, ObjectHistoryArchiver};
use autumn_harvest_plugin::ui::harvest_ui_router;
use autumn_harvest_plugin::{
    HarvestMode, HarvestRunner, HarvestRunnerResources, HarvestRuntimeConfig,
};
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

/// A string that must never appear in an object when the codec is on.
pub const MARKER: &str = "PLAINTEXT-MARKER-1983";

#[derive(diesel::QueryableByName)]
struct Count {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

async fn execution_rows(conn: &mut AsyncPgConnection, id: uuid::Uuid) -> i64 {
    diesel::sql_query("SELECT COUNT(*) AS count FROM harvest_workflow_executions WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(id)
        .get_result::<Count>(conn)
        .await
        .expect("count executions")
        .count
}

async fn send(app: &axum::Router, method: &str, uri: &str) -> (StatusCode, String) {
    let body = if method == "POST" { "{}" } else { "" };
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .expect("request failed");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    (
        status,
        String::from_utf8(bytes.to_vec()).expect("utf-8 body"),
    )
}

/// Archive one run through retention, then read it back three ways.
///
/// `backend` must point at an empty bucket on a live emulator.
#[allow(clippy::too_many_lines)]
pub async fn retention_archive_reads_back_through_api_and_vantage<B: ObjectBackend>(
    backend: Arc<B>,
) {
    let pg = Postgres::default()
        .with_init_sql(autumn_harvest::test_init_sql().as_bytes().to_vec())
        .with_tag("16")
        .start()
        .await
        .expect("start postgres");
    let host = pg.get_host().await.expect("pg host");
    let port = pg.get_host_port_ipv4(5432).await.expect("pg port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    let codec = AeadCodec::new("k-e2e", &DataKey::generate()).expect("codec");
    let builder = autumn_harvest::HarvestBuilder::new()
        .aead_payload_codec_key(codec)
        .active_payload_codec_key("k-e2e");
    let codecs = builder.payload_codecs().clone();
    let archiver = ObjectHistoryArchiver::new(Arc::clone(&backend))
        .with_prefix("history/")
        .with_codecs(codecs.clone());
    let builder = builder
        .retention(RetentionConfig {
            max_age_secs: Some(7 * 24 * 60 * 60),
            tick_interval_secs: 60 * 60,
            archival_timeout_secs: 30,
            ..Default::default()
        })
        .history_archiver(archiver);

    let manager =
        diesel_async::pooled_connection::AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            &url,
        );
    let pool = deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("pool");
    let runner = HarvestRunner::start(
        builder.build(),
        &HarvestRuntimeConfig {
            mode: HarvestMode::External,
            worker_enabled: false,
            scheduler_enabled: false,
            database: autumn_harvest_plugin::HarvestDatabaseConfig {
                url: Some(url.clone()),
            },
            outbox: autumn_harvest_plugin::HarvestOutboxConfig::default(),
            batch: autumn_harvest_plugin::HarvestBatchConfig::default(),
            readiness: autumn_harvest_plugin::HarvestReadinessConfig::default(),
            startup: autumn_harvest_plugin::HarvestStartupConfig::default(),
            redis: autumn_harvest_plugin::HarvestRedisConfig::default(),
        },
        HarvestRunnerResources::new(pool),
    )
    .await
    .expect("runner starts");

    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let id = uuid::Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions
            (id, workflow_name, workflow_id, shard_id, state, input, queue_name,
             started_at, completed_at, created_at)
         VALUES ($1, 'archived_wf', 'order-42', 0, 'COMPLETED', '{}'::jsonb, 'default',
                 NOW() - INTERVAL '11 days', NOW() - INTERVAL '10 days', NOW() - INTERVAL '11 days')",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .execute(&mut conn)
    .await
    .expect("insert execution");
    let exec_id = ExecutionId::from_uuid(id);
    let events = vec![WorkflowEvent::WorkflowCompleted {
        output: serde_json::json!({ "secret": MARKER }),
    }];
    autumn_harvest::store::append_events_with_codecs(&mut conn, exec_id, &events, 0, &codecs)
        .await
        .expect("append encoded history");

    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(runner.storage_pool());
    api_state.install(runner.api_runtime());
    api_state.set_admin_auth_boundary(true);
    api_state.set_payload_codecs(codecs);
    api_state.set_decode_payloads_on_read(true);
    let app = harvest_api_router(api_state.clone()).nest("/ui", harvest_ui_router(api_state));

    let (status, body) = send(&app, "POST", "/admin/retention/run-now").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut deleted = false;
    for _ in 0..100 {
        if execution_rows(&mut conn, id).await == 0 {
            deleted = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(deleted, "retention archives the run and deletes it");

    let (status, html) = send(&app, "GET", &format!("/ui/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{html}");
    assert!(
        html.contains(&format!("{exec_id}/archived-history")),
        "the 404 page links to the archive: {html}"
    );

    let raw = backend
        .get(&format!("history/{exec_id}.json"))
        .await
        .expect("read object")
        .expect("the archive object exists");
    let text = String::from_utf8(raw.clone()).expect("utf-8 object");
    assert!(!text.contains(MARKER), "the archive object is ciphertext");
    let envelope: serde_json::Value = serde_json::from_slice(&raw).expect("JSON object");
    assert!(
        is_codec_envelope(&envelope),
        "the object is a codec envelope"
    );

    let (status, body) = send(
        &app,
        "GET",
        &format!("/workflows/{exec_id}/archived-history"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let doc: serde_json::Value = serde_json::from_str(&body).expect("JSON body");
    assert_eq!(doc["execution_id"], exec_id.to_string());
    assert_eq!(doc["workflow_name"], "archived_wf");
    assert_eq!(doc["events"][0]["data"]["output"]["secret"], MARKER);

    let (status, html) = send(
        &app,
        "GET",
        &format!("/ui/workflows/{exec_id}/archived-history"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{html}");
    assert!(html.contains("Archived history"), "page title");
    assert!(html.contains(MARKER), "decoded event data");

    // Each decoded read is audited, with the source of the caller.
    // These are audit route templates, not format strings.
    #[allow(clippy::literal_string_with_formatting_args)]
    let audited_routes = [
        ("GET /workflows/{id}/archived-history", "api"),
        ("GET /ui/workflows/{id}/archived-history", "ui"),
    ];
    for (route, source) in audited_routes {
        let audited = diesel::sql_query(
            "SELECT COUNT(*) AS count FROM harvest_audit_log
             WHERE route_or_command = $1 AND source = $2 AND target_id = $3",
        )
        .bind::<diesel::sql_types::Text, _>(route)
        .bind::<diesel::sql_types::Text, _>(source)
        .bind::<diesel::sql_types::Text, _>(exec_id.to_string())
        .get_result::<Count>(&mut conn)
        .await
        .expect("count audit rows")
        .count;
        assert_eq!(audited, 1, "{route} audits one decoded read as {source}");
    }

    runner.stop().await;
}
