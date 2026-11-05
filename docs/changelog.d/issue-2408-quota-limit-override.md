## Feature — override a workflow type's quota limits at runtime (issue #2408)

`WorkflowInfo::quota` declares three per-tenant admission caps. They were
fixed at startup. `PATCH /admin/workflow-types/{workflow_name}/run-defaults`
now takes `quota_max_active_executions`, `quota_max_history_bytes` and
`quota_max_dead_letters`, with the tri-state, the audit row and the cache of
the other run-default fields. `GET` shows them in `code`, `override` and
`effective`. The key and the LLM caps stay deploy-only.

**Semantics.** No row stores a quota cap, and each admission reads the caps
again. So a lower cap refuses the next admission of a key over it, with the
`429` of issue #946. A run in flight continues: admission is the only place
where a cap applies. A higher cap admits the next admission. Clearing the
override restores the code caps. An override can set a cap that code leaves
unset, on a type with at least one admission cap. The route refuses each
field for a type with no admission cap, and a cap of zero or past its
column.

**One resolver.** `enforce_quota_admission` applies the override through
`run_defaults::current_quota_policy`. Each start, child spawn, fork, DLQ
redrive and cross-shard child calls it. The fork applies it to its history
check too. The guard `quota_override_guard` fails on a `check_quota` call
outside `enforce_quota_admission`, and on a read of a code cap.

**`GET /admin/quotas`.** `limits` now holds the effective caps. Each entry
adds `code_limits` and `override`, which names the stored caps with
`reason`, `updated_by` and `updated_at`. The new `workflow_types` list holds
the same fields for each type with an admission cap.

**Migration.** `20261105015009_harvest_run_default_quota_limits` adds three
nullable `BIGINT` columns to `harvest_run_default_overrides`, and widens the
row check to seven columns. No `WorkflowEvent` variant and no replay impact.
Design: `docs/design/DESIGN-2408.md`.

**Upgrade guide (for the next release).** Run the migration. During a roll,
an older process refuses the quota fields with a 400 and admits under the
code caps. So set a cap only after every process runs the new release. See
the migration row in `docs/upgrading/0.5.0.md`.

**Tests.** Unit tests on `QuotaLimits`, the fields, the merge and the cache.
DB tests on the store, the table check and the admission. HTTP tests for
each acceptance criterion and each refusal. A source guard over the cap
reads.
