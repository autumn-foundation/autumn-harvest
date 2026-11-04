## Fix — completion-trigger and typed update-with-start starts take the run defaults (issue #2511)

Two start paths ignored the chain cap and the run-default overrides.

- **Completion-trigger target starts.** The in-process start, the immediate
  outbox relay and the outbox sweep set no `execution_timeout` and no chain
  cap. They now take the declared value of the target type, the operator
  override (#2387, #2406) and the fleet ceilings. A deferred start resolves
  them when it fires, so the outbox row needs no new column. The one exception
  is the SLA of the immediate relay. It keeps the value of evaluation time.
- **Typed `update_with_start` stub.** It passed `None` for both chain fields.
  It now resolves the execution timeout, the SLA and the chain cap like the
  typed `start` stub, and it passes the client chain ceiling.

**Behavior change.** A target type that declares an `execution_timeout` now
gets that timeout on a completion-trigger start. Before, such a run had none.

**API.** `HandlerRegistry::with_max_workflow_chain_timeout` and
`with_max_workflow_execution_timeout` are no longer `const fn`. They write the
ceilings that the completion-trigger path reads. `WorkflowMetadata` has two
new public fields, `execution_timeout` and `chain_execution_timeout`.

No migration is needed. No `WorkflowEvent` changes. The design is
`docs/design/DESIGN-2511.md`.

**Tests.** Unit tests cover the resolver. DB tests cover the inline start,
the outbox sweep and the typed stub, each with an override and a ceiling. The
guard `run_defaults_chain_guard` now finds a read through `Self::info()`. It
allows one line, the metadata copy in `worker.rs`. A source test checks that
each of the three start sites resolves the run defaults.
