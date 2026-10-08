## Refactor — `StartWorkflowParams::new` replaces 40-field literals (issue #1448)

`StartWorkflowParams` has 40 fields. Each production start site wrote all of
them, so every new field touched up to 12 files.

`StartWorkflowParams::new` now takes the five required fields: name, id,
execution id, input and queue. All other fields get a neutral default.
`workflow_attempt` is `1`, because zero grants one extra retry. Call sites
override only what they vary, with `..StartWorkflowParams::new(..)`.

The production sites in `autumn-harvest` and `autumn-harvest-plugin` use the
constructor. So do the `#[workflow]` macro expansion and the quickstart example. A site that accepts a new field's default needs no edit.

Trade-off: `Default` is not implemented. Five fields have no sound default.
Tests and assays keep their literals. They can move later.

No behavior change. No migration. No `WorkflowEvent` change.
`harvest_events` is not touched.

Tests: `start_params_new_tests` in `execution.rs`. One test destructures every
field without `..`, so a new field fails the build until its default is pinned.
