# DESIGN-2412: Override the input of a code-declared workflow schedule

Issue #2412. Parent epic #2384. Follow-up to #2386. Found by the parameter
inventory in `docs/design/DESIGN-2391.md`.

## Problem

`PATCH /admin/schedules/{id}` on a code-declared workflow schedule writes an
override (#2386). It refuses `input` with 400. The input is a payload under
the payload codecs, and the override table stores plain JSON. An operator
cannot change the arguments of a scheduled run without a deploy.

## Acceptance criteria

1. The next fire after the change takes the overridden input.
2. The stored override is ciphertext when a codec is active, and a key
   rotation re-encodes it.
3. Clearing the override restores the code input. Every change is audited.

## Facts

- `harvest_schedules.workflow_input` holds the input of each run. The
  registration and the schedule API write it through
  `PayloadCodecs::encode_column`. The tick decodes it before the start.
- The reconciler compares the decoded input, not the stored bytes. Each
  encode makes a new nonce, so a byte compare would read as drift.
- `harvest_schedule_overrides` has the primary key `(kind, name)`. It has no
  `UUID` column.
- The rotation sweep reads `CODEC_COLUMNS`. Each entry needs a `UUID` key
  column. The sweep pages by that key, and its compare-and-swap matches it.
- `docs/security-posture.md` marks each `JSONB` column Covered or Clear. A
  unit test keeps that table equal to `CODEC_COLUMNS`.
- `update_code_declared_workflow_schedule` converges the row in the same
  transaction as the override write. So the next fire already reads the new
  row.
- A scheduler pass that cannot decode an override row skips its shard. A
  clear locks the row without a decode, so it removes a bad row.
- `PATCH /admin/schedules/{id}` writes a `schedule.update` audit row on
  success and on each refusal. The row holds no payload.
- The SQLite backend has no payload codecs. It refuses `input` by name.

## Brainstorm

1. A new column `harvest_schedule_overrides.workflow_input`, encoded with
   `encode_column`, as the schedule row stores its input.
2. The encoded input as a key of `workflow_fields`, swept as
   `CellShape::Field("input")`.
3. A new table `harvest_schedule_override_inputs`, keyed by a `UUID`.
4. Write the input into `harvest_schedules.workflow_input` only, and mark
   the row so that the reconciler skips the input.
5. Store the plaintext and leave it out of the sweep.
6. Teach the sweep a composite key, so the table needs no `UUID`.

For the sweep key:

- A. Add `id UUID NOT NULL DEFAULT gen_random_uuid()` with a `UNIQUE`
  constraint.
- B. Make `(kind, name)` a sweep key type.

Decision: 1 with A.

- Option 1 matches the schedule row. It keeps the input out of the clear
  `workflow_fields` object, so that column stays Clear in the posture
  table. A `CHECK` keeps the input off a DAG row.
- Option 2 mixes a Covered cell into a Clear column. The posture table then
  needs a split row, and a writer can drop the envelope by mistake.
- Option 3 needs a second write and a join on every load.
- Option 4 makes the reconciler know about overrides. The #2386 design keeps
  it blind to them.
- Option 5 breaks AC 2.
- Option 6 and B change every swept column for one table.
- A costs one table rewrite of a small table. The `UNIQUE` index makes the
  sweep's key match exact.

## Reverse brainstorm: how could this break?

| Way to break it | Guard |
|---|---|
| A writer stores the input in clear while a codec is active. | `store_workflow_override` takes the codecs and calls `encode_column`. A DB test reads the raw cell. |
| The census does not count the new cell. A key retires while an override still needs it. | The column is in `CODEC_COLUMNS`, so the census and the retirement gate count it. The seed test fails without a cell. |
| The sweep rewrites the plaintext, or the scheduler reads the old key after a rotation. | The sweep changes ciphertext only. A DB test decodes before and after. A tick after the sweep keeps the run input. |
| The sweep loses a race with a patch, and writes the old input back. | The compare-and-swap matches the stored bytes. A patch writes new bytes, so the swap loses. |
| A pass applies a snapshot that a patch replaced. | Each write changes `xmin`, so the fingerprint fence skips the pass. |
| A decode error prints the input. | The error names the column only. |
| The listing or the audit row shows the plaintext. | The listing shows the stored cell. The audit row holds no input. |
| A clear leaves the override input on the row. | A clear converges the row to the code value in the same transaction. |
| A reshard carry copies a DAG row with an input. | A DAG row has no input, by `CHECK`. |
| An older process converges the row back to the code input. | The upgrade note says to set an input override only after every process runs this release. |
| A lost codec key makes the row unreadable, and the shard pass stops. | An input that does not decode holds back its own schedule only. The pass leaves that schedule out of registration, so its row keeps its last value. A new input or a clear replaces the cell with no decode. |
| A held-back schedule fires on a stale value. | A PATCH converges the row in the same transaction as the override write. So the row holds the latest override, and a fire of it runs what the operator set. A fire of a row input that does not decode starts nothing, as before. |
| An input stored while column encoding was off stays in clear. | The sweep converts ciphertext only, as for every codec column. The runbook says to send the input again after encoding is turned on. |

## Six thinking hats

- **White (facts).** The schedule row is already Covered. The sweep needs a
  `UUID`. The PATCH path already converges the row and audits each change.
- **Red (feel).** One column next to the other override columns feels
  plain. A second table feels heavy for one cell.
- **Black (risk).** A volatile default rewrites the table. The table holds
  one row per overridden schedule, and the migration bounds its lock wait.
  A mixed-version fleet can flip the input each tick.
- **Yellow (value).** An operator changes run arguments with no deploy, under
  the same encryption and rotation as every other payload.
- **Green (ideas).** The listing shows the stored cell, so an operator sees
  that an input override exists without a decrypt.
- **Blue (process).** Red tests first, one per criterion. Then the
  migration, the store, the scheduler, the API and the sweep entry. Then the
  posture table, the ledger profiles and the gates.

## Design

### Schema

Migration `20261108003205_harvest_schedule_override_input`:

- `id UUID NOT NULL DEFAULT gen_random_uuid()`, `UNIQUE`. The sweep key.
- `workflow_input JSONB`. NULL inherits the code input.
- `CHECK (kind = 'workflow' OR workflow_input IS NULL)`.

### Store

- `WorkflowSchedulePatch::input` now merges like any other field.
  `effective_workflow_schedule` applies it.
  `validate_effective_workflow` no longer refuses it.
- `store_workflow_override` encodes the input with `encode_column`.
- The loaders decode it with the codecs that the caller passes. In the
  scheduler pass, a cell that does not decode marks its workflow name. The
  pass leaves that schedule out and runs the others. A PATCH that sets a new
  input does not decode the old cell.
- `list_overrides` shows the stored cell under `input`.

### Scheduler

- `load_shard_overrides` passes the registry codecs. The effective schedule
  then holds the override input, and the reconciler converges the row.
- `update_code_declared_workflow_schedule` accepts an input. It writes the
  row and the override in one transaction.

### API

`check_override_fields` no longer refuses `input`. The `schedule.update`
audit rows stay as they are.

### Rotation

`CODEC_COLUMNS` gets `harvest_schedule_overrides.workflow_input`, keyed by
`id`. The posture table marks it Covered.

## Out of scope

- The SQLite backend keeps its refusal of `input`. It has no codecs.
- A DAG override input. A DAG run takes `run_conf`, not a schedule input.
- A size check of the input at write time. The fire path enforces the
  workflow input cap, as for a row edit.

## Test plan

| AC | Test |
|---|---|
| 1 | `workflow_schedule_override_db_tests::the_next_fire_takes_the_overridden_input` |
| 1 | `workflow_schedule_override_db_tests::an_input_override_wins_over_a_redeploy` |
| 1 | `schedule_update_integration::code_declared_schedule_takes_an_input_override` |
| 2 | `codec_column_coverage_tests::a_schedule_input_override_is_ciphertext_and_the_fire_reads_plaintext` |
| 2 | `codec_rotation_db_tests::a_rotated_schedule_override_input_keeps_its_plaintext` |
| 2 | `codec_rotation_db_tests::the_sweep_re_encrypts_every_codec_column_without_changing_plaintext` |
| 2 | `codec_column_coverage_tests::an_override_input_that_cannot_decode_holds_back_its_own_schedule_only` |
| 3 | `workflow_schedule_override_db_tests::clearing_an_input_override_restores_the_code_input` |
| 3 | `schedule_update_integration::code_declared_schedule_takes_an_input_override` |
| - | `workflow_schedule_override_db_tests::the_database_refuses_an_input_on_a_dag_row` |
