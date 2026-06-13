1. **Refactor `evaluate_eligibility_for_shard` in `autumn-harvest-plugin/src/api.rs`**:
    - `evaluate_eligibility_for_shard` is a God Function (~490 lines).
    - We will extract several helper functions out of `evaluate_eligibility_for_shard` to improve readability and reduce nesting/complexity.
    - Specifically, extract:
        - `fetch_eligibility_tasks`: to fetch `TaskQueueItem` either by ID or query the top 1000 tasks.
        - `compute_pending_metrics`: to compute `pending_count` and `oldest_pending_age_secs`.
        - `check_worker_eligibility`: The giant loop checking each worker against all tasks and their requirements.
    - These changes are purely structural to flatten the code and will not alter any business logic or output, ensuring exact behavior parity.
    - Will apply `#[allow(clippy::too_many_lines)]` if the resulting chunks are still lengthy but vastly simpler.
2. **Review diff and run tests**:
    - `cargo fmt --all`
    - `cargo clippy --all-targets --all-features -- -D warnings`
    - `cargo test -p autumn-harvest-plugin --lib`
3. **Complete pre-commit steps to ensure proper testing, verification, review, and reflection are done.**
4. **Submit PR**:
    - Create PR with title: "⚒️ Forge: Refactor evaluate_eligibility_for_shard"
    - Description sections: "🚮 Smell", "✨ Solution", "🧼 Benefit", "🛡️ Verification".
