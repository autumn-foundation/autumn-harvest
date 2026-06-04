use autumn_harvest_plugin::api::HarvestApiState;
use axum::{body::{Body, Bytes}, http::StatusCode, response::IntoResponse, extract::Extension};

async fn my_handler(
    body: axum::body::Body,
) -> Result<Bytes, StatusCode> {
    const DEFAULT_PAYLOAD_LIMIT: usize = 2 * 1024 * 1024; // 2MB fallback
    let bytes = axum::body::to_bytes(body, DEFAULT_PAYLOAD_LIMIT).await
        .map_err(|_| StatusCode::PAYLOAD_TOO_LARGE)?;
    Ok(bytes)
}

fn main() {}
