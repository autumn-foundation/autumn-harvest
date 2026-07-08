1. **Add new feature `Export PlantUML Sequence Diagram` to `history_export.rs`**
   - Use `run_in_bash_session` to execute a Python script that adds `export_plantuml_sequence` to `autumn-harvest/src/history_export.rs`.
   - The function should take `&[WorkflowEvent]` and return `Result<String, std::fmt::Error>` containing a PlantUML sequence diagram, which works similarly to `export_mermaid_sequence` but outputs PlantUML (`@startuml` / `@enduml`).
2. **Add unit tests for `export_plantuml_sequence`**
   - Use `run_in_bash_session` with `sed` or `cat` to append tests checking the `export_plantuml_sequence` output to `autumn-harvest/src/history_export.rs`.
3. **Expose `export_plantuml_sequence` in `lib.rs`**
   - Use `run_in_bash_session` to use `sed` to update `autumn-harvest/src/lib.rs` by adding `export_plantuml_sequence` to the public exports.
4. **Update `autumn-harvest-cli` to use the new feature**
   - Use `run_in_bash_session` to run a Python script to update `autumn-harvest-cli/src/lib.rs` to support `--format plantuml-sequence` for the `history export` command. We can modify `HistoryCommand::Export` to accept a new output format or print it if a specific flag is passed (since `OutputFormat` only handles JSON, we'll probably add a new CLI command `harvest history plantuml-sequence <execution_id>` to match `HistoryCommand::Export`).
5. **Verify changes to `history_export.rs`, `lib.rs`, and `autumn-harvest-cli/src/lib.rs`**
   - Use `run_in_bash_session` to run `git diff` and verify all code modifications.
6. **Ensure no regressions**
   - Use `run_in_bash_session` to run `cargo fmt --all`, `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo test`.
7. **Complete pre-commit steps to ensure proper testing, verification, review, and reflection are done.**
8. **Submit PR with the "🌟 Nova: [Feature Name]" title**
