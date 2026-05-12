1. **Threat**: The `worker.rs` and other files currently use `saturating_add` or standard operators (`+=`) to calculate new event IDs or lengths. For example, `next_event_id += i32::try_from(prefix_events.len())`, or `rows.last().map_or(0, |row| row.event_id.saturating_add(1))`. If there's integer overflow/collison on database IDs or event counters, using `saturating_add` can silently hide the integer overflow, leading to corrupted logic or data collisions when the max integer value is saturated instead of appropriately failing out with a database error.
2. **Defense**: Replace these calculations with `checked_add` and return an appropriate database error `HarvestError::Database("Event ID overflow".to_string())` when integer overflow occurs. This avoids the silent overflow and enforces data integrity correctly.
3. **Execution Plan**:
   - Refactor `autumn-harvest/src/worker.rs` to use `checked_add` instead of `+=`.
   - Refactor `autumn-harvest/src/store.rs` to use `checked_add` instead of `saturating_add`.
   - Refactor `autumn-harvest/src/reset.rs` to use `checked_add` instead of `saturating_add`.
4. **Verification**: Run `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test`, and `cargo fmt --all` to make sure changes build and pass.
