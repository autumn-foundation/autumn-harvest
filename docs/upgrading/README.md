# Upgrading Harvest

This page states the rolling-deploy contract (issue #1828). It tells you
which versions can run side by side, and in which order to upgrade.

Each release also has its own guide:

- [0.6.0 to 0.7.0](0.7.0.md)
- [0.5.0 to 0.6.0](0.6.0.md)
- [0.4.0 to 0.5.0](0.5.0.md)

Read [Lock-safe online migrations](online-migrations.md) before you write a
migration.

In this page, **N** is the release that you deploy. **N-1** is the release
before it. A release is a minor version, such as `0.7`. Patch releases of
one minor version follow the same rules.

---

## Supported version skew

During a roll, N-1 and N run together on one database. They share the
queues and they finish each other's runs. No other pair is supported. Do not
run N-2 with N.

| Process | Schema of N-1 | Schema of N |
|---|---|---|
| Worker or starter of N-1 | Supported | Supported |
| Worker or starter of N | Not supported | Supported |

A starter is any process that starts, signals or cancels a run. It writes
events too, so it follows the same rules as a worker.

Version N must not run on the schema of N-1. Apply the migrations of N
first. See [Rolling deploy order](#rolling-deploy-order).

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
   it cannot, the guide for N names the N-1 operation that fails, and the
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
- **Fleet gates.** Where the engine can check readiness, it does. A codec key
  rotation (issue #948) writes the keyed envelope. `activate_codec_key`
  refuses until each live worker advertises that it reads it (issue #1244).
- **Codec envelopes.** An un-rotated codec writes the version 1 envelope.
  The worker of each release from 0.7 reads it.

### Known limits for 0.6 and 0.7

- **Payload codecs.** A 0.6 worker does not apply the codec that you set
  with `HarvestBuilder::payload_codec`. It writes plain payloads, and it
  fails a run when it reads an envelope. A 0.7 worker writes envelopes. So
  with a codec, do not run 0.6 and 0.7 workers together. Stop each 0.6
  worker, then start the 0.7 workers. A 0.7 worker reads the plain history
  that 0.6 wrote. You cannot roll back to 0.6 after a 0.7 worker writes.
  Without a codec, the full contract applies.
- **PII erasure.** 0.7 adds the append-only guard on `harvest_events`
  (issue #1817). The erasure of 0.6 does not set the sanction, so the
  trigger refuses it. No data changes. During the roll, run erasure from
  0.7 only.
- **Codec key rotation.** Start a rotation after the roll. The fleet gate
  above enforces this.
- **Escaped envelopes.** With no codec, 0.7 writes a nested envelope
  (issue #1253) when a payload already has the shape of an envelope. A 0.6
  worker cannot read it. No fleet gate covers this case.

## Rolling deploy order

1. Read the guide for N. Make the changes that it asks for.
2. Apply the migrations of N while N-1 runs. Use `autumn migrate`, or
   `harvest migrate run` for a split or external database. Gate the deploy
   on `harvest migrate status --check`.
3. Replace the N-1 processes with N processes. Do this in any order and at
   any speed. Runs in flight continue on N.
4. Do not use a feature that is new in N until no N-1 process runs.
5. Finish the roll before you apply the migrations of N+1.

To roll back, replace the N processes with N-1 processes. Keep the schema.

## How CI proves the contract

The `mixed-version-smoke` job in `.github/workflows/ci.yml` runs
`scripts/run-mixed-version-smoke.sh`. The script does these steps:

1. It finds N-1: the highest `vX.Y.Z` tag below the workspace version.
2. It builds the smoke worker in `scripts/mixed-version-smoke` against this
   tree and against N-1. Each build uses the `Cargo.lock` of its tree.
3. It applies the migrations of N.
4. **Roll forward.** N-1 starts the runs and runs their first step. N-1
   stops while the runs wait on a durable timer. N finishes the runs.
5. **Roll back.** The same steps in the other direction: N starts the
   runs, and N-1 finishes them.
6. **Mixed fleet.** N-1 and N poll the same queue at the same time.
7. **Codec.** Steps 4 to 6 again, with one test payload codec on both
   sides. For 0.6, only the roll forward runs. See the known limits.

Each run executes an activity, waits on a timer and executes a second
activity. Each activity returns the label of the binary that ran it. The
check asserts which version ran each step.

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
erasure or key rotation. A change to one of those needs its own
mixed-version reasoning in review.

`docs/audits/mixed-version-contract.py` runs in the `lint` job. It fails
when this page loses a section, or when the job or the script goes missing.
