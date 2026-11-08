## Override the input of a code-declared workflow schedule (issue #2412)

`PATCH /admin/schedules/{id}` on a code-declared workflow schedule now takes
`input`. Before, it refused `input` with 400, so an operator needed a deploy
to change the arguments of a scheduled run. The design is in
`docs/design/DESIGN-2412.md`.

- The override stores the input in the new column
  `harvest_schedule_overrides.workflow_input`. It goes through
  `PayloadCodecs::encode_column`, as `harvest_schedules.workflow_input` does.
  With column encoding on, the cell is ciphertext.
- The scheduler decodes the cell when it loads the overrides of a shard. The
  effective schedule then holds the override input, and the reconciler
  converges the row. The patch converges the row in its own transaction, so
  the next fire takes the new input.
- `CODEC_COLUMNS` lists the new column, keyed by the new `id` column. The
  rotation sweep re-encodes it, and the census and the retirement gate count
  it.
- A cell that does not decode is an unreadable override. The pass of its
  shard skips, as for any unreadable override. A clear removes it with no
  decode.
- A clear restores the code input. The set and the clear each write one
  `schedule.update` audit row. The audit row and the override listing hold no
  plaintext.
- `WorkflowSchedulePatch::input` now merges and applies like any other
  override field. The SQLite backend still refuses it, because it has no
  payload codecs.

**Migration.** `20261108003205_harvest_schedule_override_input` adds
`id UUID NOT NULL DEFAULT gen_random_uuid()` with a `UNIQUE` constraint, and
`workflow_input JSONB` with a check that keeps it NULL on a DAG row. The
volatile default rewrites `harvest_schedule_overrides`. The table holds one
row per overridden schedule, and `lock_timeout = '5s'` bounds the lock. No
hot table, no `WorkflowEvent` variant, no replay impact.

**Breaking.** `schedule_override::load_overrides` and
`schedule_override::get_workflow_override` take a `&PayloadCodecs`.

**Tests.** Unit tests cover the merge, the effective schedule, the
validation and the tick apply. `workflow_schedule_override_db_tests` covers
the next fire, the clear, a redeploy and the DAG row check.
`codec_column_coverage_tests` covers the ciphertext at rest, the listing, the
fire on plaintext and a cell that does not decode. `codec_rotation_db_tests`
covers the sweep, the census and a fire after a rotation. The plugin test
`code_declared_schedule_takes_an_input_override` covers the route and the
audit rows.

### Upgrade notes for the next release guide

- **Migration.** As above. No new grant: the sweep needs `UPDATE` on
  `harvest_schedule_overrides`, which the preflight already checks.
- **Mixed versions.** A process from before this release ignores the new
  column. Its tick converges the row back to the code input. Set an input
  override only after every process runs this release.
