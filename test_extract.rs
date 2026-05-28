use axum::body::Body;

#[tokio::main]
async fn main() {
    let body = Body::from("test");
    let bytes = axum::body::to_bytes(body, 1024).await.unwrap();
    println!("{:?}", bytes);
}
