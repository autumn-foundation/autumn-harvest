1. **Define `MAX_API_PAYLOAD_BYTES`**
   - Add `const MAX_API_PAYLOAD_BYTES: usize = 2 * 1024 * 1024; // 2MB` to `autumn-harvest-plugin/src/api.rs`.

2. **Refactor `query_workflow_post`**
   - Change `body: Bytes` to `body: axum::body::Body`
   - Use `let body_bytes = axum::body::to_bytes(body, MAX_API_PAYLOAD_BYTES).await.map_err(|e| AutumnError::bad_request_msg(format!("Payload too large: {e}")))?;`
   - Replace uses of `body` with `body_bytes`

3. **Refactor `bulk_replay_dead_letters_handler`**
   - Change `body: axum::body::Bytes` to `body: axum::body::Body`
   - Use `let body_bytes = match axum::body::to_bytes(body, MAX_API_PAYLOAD_BYTES).await { Ok(b) => b, Err(e) => return AutumnError::bad_request_msg(format!("Payload too large: {e}")).into_response() };`
   - Pass `&body_bytes` instead of `&body` to `parse_bulk_dlq_request`

4. **Refactor `bulk_discard_dead_letters_handler`**
   - Same as `bulk_replay_dead_letters_handler`

5. **Test and Verify**
   - Run `cargo clippy -p autumn-harvest-plugin`
   - Run `cargo test -p autumn-harvest-plugin`

6. **Pre-commit and Submit**
   - Complete pre commit steps to ensure proper testing, verification, review, and reflection are done.
   - Run `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test`, and `cargo fmt --all`
   - Submit PR with 🔒 Warden specific PR format.
