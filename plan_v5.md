1. Fix the Axum path parameter syntax in the test `warden_dos_payload_exploit_test` in `autumn-harvest-plugin/src/api.rs`. It should use `:id` and `:name` instead of `{id}` and `{name}`.
2. In `query_workflow_post`, move `let bytes = axum::body::to_bytes(body, MAX_API_PAYLOAD_BYTES).await.map_err(|e| AutumnError::bad_request_msg(format!("failed to read body: {e}")))?;` to before `hydrate_ctx_for_query` to prevent tying up backend resources on invalid payloads. Wait, does `body` need to be consumed before `hydrate_ctx_for_query`? Yes, we can just extract it first.
3. Run `cargo test -p autumn-harvest-plugin --lib` again.
4. Run `cargo clippy -p autumn-harvest-plugin -- -D warnings`.
5. Submit PR.
