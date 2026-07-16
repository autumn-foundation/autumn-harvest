## Trunk heal — add `StartSource` fields to `ui_integration` test literals (semantic merge conflict #1095 × #1085)

**Test-code-only — no production code changed.** Trunk-dev went RED on the Lint
leg (`cargo clippy -p autumn-harvest-plugin --all-targets -- -D warnings`), which
blocks every branch off trunk repo-wide, with a hard compile error:

```
error[E0063]: missing fields `start_source`, `start_source_ref` and `started_by`
              in initializer of `StartWorkflowParams<'_>`
  --> autumn-harvest-plugin/tests/ui_integration.rs:3670:9
```

Root cause: a **semantic merge conflict** between two independently-green PRs.
PR #1095 (issue #740, StartSource provenance, merged as `3fa812d2`) added three
**required** fields to `StartWorkflowParams` in `autumn-harvest/src/execution.rs`
— `start_source: StartSource`, `start_source_ref: Option<&'a str>`,
`started_by: Option<&'a str>` — and updated every construction site that existed
at the time (its CI was green). PR #1085 (Vantage DAG run-graph + timeline pages,
merged *after* #1095 became ready) added a **new** `StartWorkflowParams { .. }`
literal in the `dag957_seed_run` helper of `ui_integration.rs` that predates the
field additions. Git merged the two cleanly (no textual conflict), but the
combination does not compile: the new literal is missing the three now-required
fields.

Fix: add the three fields to the one un-updated literal, mirroring #1095's own
neutral test convention verbatim (`StartSource::Api` / `None` / `None`, inserted
right after `completion_callbacks: None,` — identical to the four sibling literals
#1095 updated in this same file at the `insert_workflow_on_url` /
`insert_child_workflow_on_url` / `workflow_detail_ui_*` helpers). A brace-tracking
scan of every `StartWorkflowParams { .. }` literal across the whole workspace
confirmed this was the **only** straggler — all core-crate and other plugin
literals were already updated by #1095.

Because `start_source` is metadata only (never read during replay), this is not a
behavioral change: **no new `WorkflowEvent` variant, no migration, no route/contract
change, no replay/determinism impact.**

Validation (Docker-backed Postgres 16): the 8 DAG run-graph/retry `ui_integration`
tests that seed via the fixed `dag957_seed_run` helper (`ui_dag_run_graph_*`,
`ui_dag_retry_*`) all pass end-to-end — exercised, not merely compile-checked
(10/10 in the `ui_dag_` filter). Gates green: `cargo clippy -p autumn-harvest-plugin
--all-targets -- -D warnings` at both stable and 1.97.0 (plus the
`mcp,unified-dag-execution` / `webhooks` / `metrics` feature combos),
`cargo fmt --all -- --check`, and MSRV `cargo +1.88.0 check --workspace`.
