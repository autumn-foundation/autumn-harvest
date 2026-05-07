1. **Target**: Improve test coverage for `autumn-harvest/src/error.rs` as it lacks unit test coverage for many error formatting methods.
2. **Action**: Implement unit tests for `HarvestError` enum variants to check `Display` traits in `autumn-harvest/src/error.rs` file.
   - `HarvestError::Serialization`
   - `HarvestError::UnknownPayloadCodec`
   - `HarvestError::QueueFull`
   - `HarvestError::NotFound`
   - `HarvestError::Config`
   - `HarvestError::AlreadyExists`
   - `HarvestError::UpdateRejected`
   - `HarvestError::UpdateHandlerNotFound`
   - `HarvestError::InvalidSearchAttribute`
   - `HarvestError::ActivityFailed`
   - `HarvestError::WorkflowFailed`
   - `HarvestError::Cancelled`
3. **Verification**: Run `cargo llvm-cov -p autumn-harvest --no-report --lib` to see if coverage in `error.rs` improves.
