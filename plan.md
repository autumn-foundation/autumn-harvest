1. **Fix `GateCreateError::TooManyGates` in `autumn-harvest/src/admission_gate.rs`:**
   - Use `replace_with_git_merge_diff` to update `[`TooManyGates`](GateCreateError::TooManyGates)` to `[`TooManyGates`](crate::admission_gate::GateCreateError::TooManyGates)`.
   - Use a separate step to update `[`GateCreateError::TooManyGates`]` to `[`GateCreateError::TooManyGates`](crate::admission_gate::GateCreateError::TooManyGates)`.

2. **Fix `HarvestError::QueryTimedOut` in `autumn-harvest/src/builder.rs`:**
   - Use `replace_with_git_merge_diff` to update `[`HarvestError::QueryTimedOut`]` to `[`crate::error::HarvestError::QueryTimedOut`]`.

3. **Fix `resolve_concurrency_key` in `autumn-harvest/src/concurrency.rs`:**
   - Use `replace_with_git_merge_diff` to update `[`resolve_concurrency_key`]` to `[`crate::concurrency::resolve_concurrency_key`]`.

4. **Fix `ActivityInfo` in `autumn-harvest/src/context.rs`:**
   - Use `replace_with_git_merge_diff` to update `[`ActivityInfo`]` to `[`crate::info::ActivityInfo`]`.

5. **Fix `WorkflowInfo` in `autumn-harvest/src/context.rs`:**
   - Use `replace_with_git_merge_diff` to update `[`WorkflowInfo`]` to `[`crate::info::WorkflowInfo`]`.

6. **Fix `QueryHandlerInfo` and `UpdateHandlerInfo` in `autumn-harvest/src/context.rs`:**
   - Use `replace_with_git_merge_diff` to update `[`QueryHandlerInfo`]` to `[`crate::info::QueryHandlerInfo`]`.
   - Use `replace_with_git_merge_diff` to update `[`UpdateHandlerInfo`]` to `[`crate::info::UpdateHandlerInfo`]`.

7. **Fix `Ok(())` in `autumn-harvest/src/context.rs`:**
   - Use `replace_with_git_merge_diff` to update `[`Ok(())`]` to `` `Ok(())` ``.

8. **Fix `WorkflowLogger` in `autumn-harvest/src/executor.rs`:**
   - Use `replace_with_git_merge_diff` to update `[`WorkflowLogger`]` to `[`crate::context::WorkflowLogger`]`.

9. **Fix `OverlapPolicy::Skip` and `OverlapPolicy::BufferAll` in `autumn-harvest/src/info.rs`:**
   - Use `replace_with_git_merge_diff` to update `[`OverlapPolicy::Skip`]` to `[`crate::policy::OverlapPolicy::Skip`]`.
   - Use `replace_with_git_merge_diff` to update `[`OverlapPolicy::BufferAll`]` to `[`crate::policy::OverlapPolicy::BufferAll`]`.

10. **Fix `with_schemas` in `autumn-harvest/src/info.rs`:**
    - Use `replace_with_git_merge_diff` to update `[`with_schemas`](Self::with_schemas)` to `` `with_schemas` `` since `with_schemas` isn't a method on `WorkflowInfo`.

11. **Fix `HarvestError::ActivityFailed` in `autumn-harvest/src/replay.rs`:**
    - Use `replace_with_git_merge_diff` to update `[`HarvestError::ActivityFailed`]` to `[`crate::error::HarvestError::ActivityFailed`]`.

12. **Fix `WorkflowContext::spawn_child_workflow_detached_raw` in `autumn-harvest/src/types.rs`:**
    - Use `replace_with_git_merge_diff` to update `[`WorkflowContext::spawn_child_workflow_detached_raw`]` to `[`crate::context::WorkflowContext::spawn_child_workflow_detached_raw`]`.

13. **Fix `database_error` in `autumn-harvest/src/schedule_decision.rs`:**
    - Use `replace_with_git_merge_diff` to update `[`database_error`]` to `[`crate::error::database_error`]`.

14. **Format and Verify:**
    - Use `run_in_bash_session` to run `cargo doc -p autumn-harvest --all-features --no-deps` to ensure all links resolve.
    - Use `run_in_bash_session` to run `cargo fmt --all`.
    - Use `run_in_bash_session` to run `cargo clippy --all-targets --all-features -- -D warnings`.
    - Use `run_in_bash_session` to run `cargo test`.

15. **Pre-commit:**
    - Complete pre-commit steps to ensure proper testing, verification, review, and reflection are done.

16. **Submit PR:**
    - Use `run_in_bash_session` to embed the exact PR title and description directly into the git commit message (e.g., `git commit -am "<title>\n\n<description_sections>"`).
    - PR Title: `🎻 Bard: [documentation update]`
    - Description:
        - 📖 Chapter: Core modules documentation in `autumn-harvest`.
        - 🔦 Insight: Fixed unresolved intra-doc links by providing explicit, fully qualified paths to resolve `rustdoc::broken_intra_doc_links` warnings.
        - 🧪 Example: `[`HarvestError::ActivityFailed`]` was changed to `[`crate::error::HarvestError::ActivityFailed`]`.
        - 🖼️ Preview: Documentation builds smoothly without warnings.
    - Submit using `submit` tool with same details.
