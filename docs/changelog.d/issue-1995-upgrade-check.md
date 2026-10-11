## Feature — pre-deploy upgrade verdict for each in-flight run (issue #1995)

The new upgrade check gives each in-flight run one verdict against a
candidate build: `migrate`, `review` or `pin`. It runs inside the candidate
build, because only that build holds its workflow types and codec keys.
`autumn_harvest::upgrade_check::run_command` is the command. The
`upgrade_check` example is a complete binary. Guide: `docs/upgrade-check.md`.
Design: `DESIGN-1995.md`.

One check per failure mode:

- **Determinism.** A canary replay of the recorded history under the
  candidate code. A divergence pins the run.
- **Rehydration.** The replay decodes each recorded payload into the
  candidate types. Candidate schemas also check the workflow input, signal
  and update payloads, and each signal that waits in `harvest_signals`. A
  failure pins the run. A signal, recorded or pending, or an open update
  with no candidate schema needs review.
- **Structural drift.** `cargo harvest-verify --emit-structure FILE` writes the
  resolved call graph of each workflow: each body with a span-free digest of
  its raw MIR text, its call sites and the commands it emits. The check
  diffs the manifests of the two builds. A changed body that the run can
  still execute needs review.

Design decisions:

- A change to the workflow body itself always needs review. A change to a
  helper gives `migrate` only when the history proves the run finished it.
  The helper must start at most once. It must emit only commands with known
  names that no other body emits. Each of them must be complete and none
  open, and a decision must run after the last result.
- Any `unknown` boundary in a workflow graph gives review. A `const` read
  from a crate outside the analysis adds an `external-const` boundary,
  because its value is not in the reader's MIR.
- The replay runs with the candidate worker's setup: its query handlers
  (`queries`), its payload caps (`with_payload_caps`), its offload
  threshold (`with_offloader`), its history policy (`with_history_policy`)
  and its build id (`with_build_id`). A run whose next payload is over a
  candidate cap gets `pin`.
- A run verdict holds ids, names, finding kinds and event indexes. It holds
  no payload and no error text, because a serde error quotes the value it
  rejects. The `incomplete` list holds shard database errors only.
- The database path reads inside read-only transactions and writes nothing.
  It decodes with the candidate codecs, and inflates with the candidate
  offloader, in memory.
- `--database-url-env NAME` keeps a database URL out of the process list.
- The `[[sink]]` rows of the `harvest-verify` model gain a `step` field. It
  names the history record each sink writes. The `await_condition` row gets
  `step = "other"`, because it can park with no command. The model version
  is now `2026.10.0`.

No migration. No new `WorkflowEvent` variant. No new route. No engine
change.

Tests:

- `autumn-harvest/tests/integration/upgrade_check_tests.rs`: one red test
  per failure mode, the migrate cases, and codec-encrypted histories that
  the check decodes in memory.
- `autumn-harvest/tests/integration/upgrade_check_db_tests.rs`: verdicts read
  from Postgres, with no plaintext in the report and no row written.
- `autumn-harvest-verify/tests/structure.rs`: manifests of two builds of one
  fixture. An activity-only change and a line shift leave the graph the
  same. A changed match value or `const` item changes it. An end-to-end
  test feeds both manifests to the core check.
