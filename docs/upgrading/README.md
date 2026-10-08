# Upgrading Harvest

This page states the rolling-deploy contract (issue #1828). It tells you
which versions can run side by side, and in which order to upgrade.

Each release also has its own guide:

- [0.7.0 to 0.8.0](0.8.0.md) (in progress; it grows as changes merge)
- [0.6.0 to 0.7.0](0.7.0.md)
- [0.5.0 to 0.6.0](0.6.0.md)
- [0.4.0 to 0.5.0](0.5.0.md)

Read [Lock-safe online migrations](online-migrations.md) before you write a
migration.

In this page, **N** is the release that you deploy. **N-1** is the minor
release before it, for example `0.6` for `0.7`. Patch releases of one minor
version follow the same rules.

---

## Supported version skew

During a roll, N-1 and N run together on one database. They share the
queues and they finish each other's runs. Harvest supports no other pair. Do
not run N-2 with N.

| Process | Schema of N-1 | Schema of N |
|---|---|---|
| Worker, starter or API server of N-1 | Supported | Supported |
| Worker, starter or API server of N | Not supported | Supported |

A starter is any process that starts, signals or cancels a run. It writes
events too, so it follows the same rules as a worker.

The engine does not refuse the schema of N-1. Outside `dev`, a process with
pending migrations logs a warning and starts. So gate the deploy on
`harvest migrate status --check`. See [Rolling deploy order](#rolling-deploy-order).

## Schema changes: expand, then contract

A migration either expands or contracts the schema.

- **Expand.** The change adds something. Old code does not see it. Examples:
  a new table, a nullable column, a column with a default, a new index, a
  wider check constraint.
- **Contract.** The change removes or narrows something. Old code can fail.
  Examples: drop or rename a column or table, narrow a type, add `NOT NULL`
  without a default, narrow a check constraint.

The rules:

1. Release N contains expand migrations only.
2. A contract migration ships at least one minor release after the last
   release that reads or writes the object. If N stops using a column, N+1
   can drop it. N cannot.
3. A new trigger or constraint must accept every write that N-1 makes. If
   it cannot, this page names the N-1 operation that fails, and the
   operator must not run that operation during the roll.
4. Each migration bounds its locks. See
   [online-migrations.md](online-migrations.md).

Do not run `down.sql` to roll back a deploy. Roll back the binaries only.
N-1 runs on the schema of N, so the schema can stay.

## Event and codec formats

Workflow history is the long-lived data. Replay reads it back in order. So
each version must read the history that the other version writes.

- **N reads N-1.** N decodes each `WorkflowEvent` variant and field that N-1
  writes. Never remove a variant or a field that N-1 writes.
- **N-1 reads N.** N must not write a new event variant, a new required
  field or a new payload envelope by default. Make the new form readable in
  one release. Write it by default in a later release, or behind an opt-in
  that the operator turns on after the roll.
- **Fleet gates.** `activate_codec_key` refuses a new codec key until each
  live worker advertises that it can read the keyed envelope (issue #1244).
  Use a gate like this for a new format when you can.

### Known limits for 0.6 and 0.7

These 0.6 and 0.7 behaviors break the contract. Avoid each one during the
roll.

- **Payload codecs.** A 0.6 worker does not apply the codec that you set
  with `HarvestBuilder::payload_codec`. It writes plain payloads, and it
  fails a run when it reads an envelope. A 0.7 worker writes envelopes. So
  with a codec, do not run 0.6 and 0.7 workers together. Stop each 0.6
  worker, then start the 0.7 workers. A 0.7 worker reads the plain history
  that 0.6 wrote. You cannot roll back to 0.6 after a 0.7 worker writes.
- **Escaped envelopes.** With no codec, 0.7 writes a nested envelope
  (issue #1253) when a payload already has the shape of an envelope. A 0.6
  worker does not detect it. It gives the wrapped object to the workflow as
  data, so the run gets wrong output with no error. No fleet gate covers
  this case.
- **PII erasure.** 0.7 adds the append-only guard on `harvest_events`
  (issue #1817). The erasure of 0.6 does not set the sanction, so the
  trigger refuses it with a database error. The transaction rolls back, so
  no data changes. During the roll, run erasure from 0.7 only.
- **Rate-limit bucket GC.** The 0.7 retention sweep deletes idle rate-limit
  buckets. A 0.6 enqueue does not lock the bucket, so the sweep can delete
  it under a 0.6 task. That task then waits until another enqueue for the
  same key. During the roll, call
  `RetentionConfig::without_rate_limit_bucket_gc()` on 0.7.
- **Audit retention.** 0.6 retention deletes audit rows older than the
  cutoff, also rows that 0.7 has not exported yet. Configure an audit export
  sink after the roll, or set the 0.6 `audit_retention_days` to `0`.
- **API token scopes.** A 0.6 API server reads an `admin` token as `read`.
  It also lets a `mutate` token reach admin routes (issue #1803 is 0.7
  only). Mint `admin` tokens after the roll.
- **Non-cancellable blocks (issue #1984).** A cancel that finds an open
  `ctx.non_cancellable` block writes the new `WorkflowCancelRequested`
  event. A worker of an earlier version cannot read it. Only a run whose
  code calls `ctx.non_cancellable` gets the event, and that code needs the
  new version. Deploy such code after the roll.
- **Codec key rotation, shard rebalancing, DR fencing and
  `harvest partition enable`.** 0.6 does not know these features. Start
  them after the roll. The codec fleet gate above enforces this for key
  rotation.

## Rolling deploy order

1. Read the guide for N. Make the changes that it asks for.
2. Apply the migrations of N while N-1 runs. Run `autumn migrate`. For a
   split or external Harvest database, also run `harvest migrate run` on
   each shard database, with `--include-dir` for the plugin migrations. See
   the [0.6.0 guide](0.6.0.md#split--external-mode-still-applies-its-own-harvest-migrations).
3. Gate the deploy on `harvest migrate status --check`, with the same
   `--include-dir`.
4. Replace the N-1 processes with N processes, in any order and at any
   speed. Runs in flight continue on N. A known limit above can require a
   different order.
5. Do not use a feature that is new in N until no N-1 process runs.
6. Finish the roll before you apply the migrations of N+1.

To roll back, replace the N processes with N-1 processes. Keep the schema.

## How CI proves the contract

The `mixed-version-smoke` job in `.github/workflows/ci.yml` runs
`scripts/run-mixed-version-smoke.sh`. The script does these steps:

1. It finds N-1: the highest `vX.Y.Z` tag of an earlier minor version.
2. It builds the smoke worker in `scripts/mixed-version-smoke` against this
   tree and against N-1. Each build uses the `Cargo.lock` of its tree.
3. It applies the migrations of N.
4. **Roll forward.** N-1 starts the runs and runs their first step. N-1
   stops while the runs wait on a durable timer. N finishes the runs.
5. **Roll back.** The same steps in the other direction: N starts the
   runs, and N-1 finishes them.
6. **Mixed fleet.** N-1 and N poll the same queue at the same time.
7. **Codec.** Steps 4 to 6 again, with the test payload codec installed in
   each worker. For 0.6, only the roll forward runs. See the known limits.

Each run executes an activity, waits on a timer and executes a second
activity. Each activity returns the label of the binary that ran it.

- In a roll, the check asserts which version ran each step.
- In the mixed fleet, the check asserts that each version ran at least one
  step.
- With the codec, the check asserts that the `WorkflowCompleted` event
  holds an envelope that decodes to the run output.

Run it on your machine:

```sh
git fetch --tags
DATABASE_URL=postgres://postgres:postgres@localhost:5432/mvsmoke \
  ./scripts/run-mixed-version-smoke.sh
```

The script drops and recreates the `public` schema of that database. Set
`MV_SMOKE_PREVIOUS` to test against another tag.

When the API of N-1 differs from this tree, a cargo feature in
`scripts/mixed-version-smoke/src/compat.rs` selects the old form. Delete
the feature when no supported N-1 needs it.

The smoke workload covers starts, activities, durable timers, replay and
the codec envelope. It does not cover signals, child workflows, schedules,
erasure, retention or key rotation. A change to one of those needs its own
mixed-version reasoning in review.

`docs/audits/mixed-version-contract.py` runs in the `lint` job. It fails
when this page loses a section, or when the job or the script goes missing.
