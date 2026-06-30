1. **Add `SequentialActivitiesRule` to `autumn-harvest/src/analyzer.rs`**:
   - This rule tracks how many activities run strictly one after the other without overlapping. If the number of sequential activities reaches a threshold (e.g. 3), it warns the user that they might benefit from running activities concurrently (using `futures::join!` or DAGs).
2. **Add `harvest-analyze` CLI binary**:
   - Create `autumn-harvest-cli/src/bin/harvest_analyze.rs`.
   - It will take a JSON file (HistorySnapshot or HistoryExportDocument), parse the `events` array into `Vec<WorkflowEvent>`, run the `HistoryAnalyzer` over it with default rules (including `SequentialActivitiesRule`), and print the warnings nicely using some colors.
3. **Register `harvest-analyze` in `autumn-harvest-cli/Cargo.toml`**:
   - `[[bin]] name = "harvest-analyze" path = "src/bin/harvest_analyze.rs"`
4. **Testing & Pre-commit**:
   - Add unit tests for `SequentialActivitiesRule` in `autumn-harvest/src/analyzer.rs`.
   - Run `cargo fmt --all`, `cargo clippy`, and `cargo test`.
   - Complete pre-commit steps to ensure proper testing, verification, review, and reflection are done.
5. **Submit**: Create the PR with the title "🌟 Nova: [harvest-analyze] Workflow History Analyzer CLI & Sequential Task Detection" describing the feature, the spark, the potential, and the risks.
