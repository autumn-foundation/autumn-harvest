use axum::{body::{Body, Bytes}, http::StatusCode, response::IntoResponse, extract::Extension};
use std::convert::Infallible;

async fn my_handler(
    body: axum::body::Body,
) -> Result<Bytes, StatusCode> {
    // we want to use axum::body::to_bytes
    match axum::body::to_bytes(body, 1024 * 1024).await {
        Ok(bytes) => Ok(bytes),
        Err(_) => Err(StatusCode::PAYLOAD_TOO_LARGE),
    }
}

fn main() {}
