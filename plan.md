1. **Refactor `process_workflow_task` in `autumn-harvest/src/worker.rs` to fix readability smells.**
   - The `process_workflow_task` function is extremely long (over 600 lines) and deeply nested, specifically in handling `WorkflowOutcome::Suspended`.
   - I will extract the logic for checking if all signals are resolved and reconstructing unresolved commands into a named helper function, e.g., `reconstruct_unresolved_signals`.
   - This extraction will flatten the nesting and reduce the length of the god function, improving readability according to Forge's principles, without changing the behavior.
2. **Complete pre-commit steps.**
   - Complete pre-commit steps to ensure proper testing, verification, review, and reflection are done.
3. **Submit the change.**
   - Create a PR with title "⚒️ Forge: Extract unresolved signal reconstruction in worker" and the required description format.
