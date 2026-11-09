//! A local HTTP server for the client tests. It needs no key and no network.

#![allow(clippy::unwrap_used)]

use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// The requests that a test server got, each as lowercase text.
pub type Seen = Arc<Mutex<Vec<String>>>;

/// A server that answers each request with the next `(head, body)`. It reads
/// the whole request first, so no unread byte resets the connection.
pub async fn serve_each(answers: Vec<(String, Vec<u8>)>) -> (String, Seen) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/v1/endpoint", listener.local_addr().unwrap());
    let seen: Seen = Arc::default();
    let kept = seen.clone();
    tokio::spawn(async move {
        for (head, body) in answers {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = vec![0_u8; 16 * 1024];
            loop {
                let read = socket.read(&mut buffer).await.unwrap();
                request.extend_from_slice(&buffer[..read]);
                let text = String::from_utf8_lossy(&request).to_lowercase();
                let whole = text.find("\r\n\r\n").is_some_and(|end| {
                    let length = text
                        .lines()
                        .find_map(|l| l.strip_prefix("content-length: "))
                        .and_then(|n| n.trim().parse::<usize>().ok())
                        .unwrap_or(0);
                    request.len() >= end + 4 + length
                });
                if whole || read == 0 {
                    kept.lock().unwrap().push(text);
                    break;
                }
            }
            socket.write_all(head.as_bytes()).await.unwrap();
            socket.write_all(&body).await.unwrap();
        }
    });
    (url, seen)
}

/// The head of a response with `status` and a body of `length` bytes.
pub fn head(status: u16, length: usize) -> String {
    format!("HTTP/1.1 {status} X\r\ncontent-length: {length}\r\nconnection: close\r\n\r\n")
}

/// A one-shot server that answers with `head` and `body`. The request is
/// kept before the answer goes out, so a test can read it at once.
pub async fn serve_raw(head: String, body: Vec<u8>) -> (String, Seen) {
    serve_each(vec![(head, body)]).await
}

/// A one-shot server that answers with `status` and `body`.
pub async fn serve(status: u16, body: &[u8]) -> (String, Seen) {
    serve_raw(head(status, body.len()), body.to_vec()).await
}

/// The text of the first request that `seen` holds, or `""`.
pub fn first(seen: &Seen) -> String {
    seen.lock().unwrap().first().cloned().unwrap_or_default()
}
