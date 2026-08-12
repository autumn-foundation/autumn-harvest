## Phase 3.x — Gate payload-schema changes in CI to protect in-flight replay (issue #794)

**Implemented.** A contributor renames `OnboardInput.email` → `email_address`.
`cargo build` is green, tests are green, the PR merges — and on deploy **every
in-flight `onboarding` execution** wedges, because its recorded
`WorkflowStarted` event carries `{"email": …}` and no longer deserializes.
Nothing in the toolchain caught it: the payload types compile fine, the
incompatibility is with **JSON already written to `harvest_events`**. Issue
[#373](https://github.com/autumn-foundation/autumn-harvest/issues/373) already
publishes each workflow's `input_schema` / `output_schema` / `error_schema`;
what was missing was a **pre-merge check** that diffs them across revisions.

```console
$ harvest schema check --current /tmp/current.json
onboarding.input: /email — breaking — property `email` removed; the value recorded for it is
  silently dropped on replay, so the workflow no longer observes it. If this is a rename, add
  `#[serde(alias = "email")]` to the new field.
onboarding.input: /email_address — breaking — required property `email_address` added; JSON
  recorded before this change has no such key and fails to deserialize (serde: `missing field
  \`email_address\``). Make it `Option<T>` or add `#[serde(default)]` …
schema check: 2 breaking change(s) of 2 delta(s)
$ echo $?
1
```

**No new `WorkflowEvent` variant, no event-schema change, no migration, no
change to the `::autumn_harvest::` macro path contract.** The check is
read-only: pure `serde_json` analysis of two files, no database, no network.

### The artifact

`docs/workflow-schema-contract.json` — a checked-in, versioned, machine-readable
baseline mirroring the `docs/api-contract.json` precedent (`version`,
`contract_version`, `description`, `compatibility`, `coverage`,
`acknowledged_breaking_changes`, `workflows`), keyed by workflow name + schema
role and regenerable with `harvest schema update`.

Stored schemas are **canonicalised**: annotations (`title`, `description`,
`examples`, `default`, `$schema`, non-numeric `format`) are stripped, `enum`
arrays are sorted, and the two `schemars` unit-enum forms are normalised to one.
Both sides are canonicalised again at diff time, so editing a doc comment — which
`schemars` copies into `description` — yields **zero deltas and a byte-identical
artifact**, and the baseline never rots into noise everyone learns to ignore.

### The front door

`harvest schema check --baseline <path> --current <path> [--format text|json]`
and `harvest schema update --acknowledge "<why>" [--recorded-in <ref>]`, both
pure-local subcommands handled before the API dispatch path (mirroring
`harvest det-check`, issue #778).

`--current` is what the app publishes *right now*. Harvest is a **library** — the
registry lives in the embedder's process — so it comes from a three-line binary
in your own crate calling `WorkflowSchemaContract::from_infos(version, &infos)`
against the same `Vec<WorkflowInfo>` you register. `autumn-harvest/examples/schema_workflow.rs`
gained `--emit-contract` as the working reference. A raw
`GET /workflows/registered` response body is also accepted verbatim, so
`curl … > current.json` works with no post-processing.

### The ruleset (replay-read perspective)

Every verdict answers one question: *does JSON already in `harvest_events` still
deserialize into the new type, and still mean the same thing?* Deliberately not
general JSON Schema subtyping — the schema is an asymmetric **read** contract
for historical data.

**Breaking:** adding a required property · removing a property · renaming (both
halves reported) · optional → required · narrowing a type or a numeric `format`
(`int64` → `int32`, `i64` → `u64`) · removing an `enum` value or `oneOf`/`anyOf`
variant · restricting `additionalProperties` · introducing or tightening a bound
· changing tuple arity · **withdrawing** a published schema · any change to a
constraint keyword outside the analysed set.

**Compatible:** adding an optional property · required → optional · widening a
type or numeric format · adding an `enum` value or a *disjoint* variant ·
relaxing `additionalProperties` or a bound · adding a workflow type or
publishing a schema for the first time · editing any annotation.

Three ruleset decisions are load-bearing and were driven by empirical probes of
`schemars` 0.8.22 output rather than assumption:

- **Numeric `format` is analysed, not an annotation.** `i64 → i32` changes *only*
  `format` (`int64` → `int32`); `type` stays `"integer"`. Parking `format` with
  the annotations would make a textbook break invisible. A width lattice over
  `i128` ranges (float ±2²⁴, double ±2⁵³) means `int32 → double` is a genuine
  widening while `int64 → double` is correctly breaking.
- **`oneOf`/`anyOf` branches are matched by identity, not position.** `schemars`
  collapses all unit variants into one branch emitted *first*, so positional
  diffing produces a false-positive storm. Branches are keyed by tag / unit
  value / single-required-property, **after `$ref` resolution** — without that,
  `T → Option<T>` in the non-primitive form
  (`{"anyOf":[{"$ref":…},{"type":"null"}]}`) reads as "the object variant was
  removed", i.e. the single most common widening reported as breaking.
- **Unanalysed keywords fail closed in *both* directions.** "Removing a
  constraint always loosens" is false: dropping a `patternProperties` entry
  while `additionalProperties: false` remains *tightens* the schema. The differ
  does not model keyword interactions, so it refuses to guess and lets the
  author acknowledge.

`$ref` is resolved on **both** sides (cycle-guarded, so recursive types
terminate): a change inside `definitions.Inner` leaves `properties.inner`
byte-identical, so without resolution the most common *nested* break is
invisible.

**Removing a workflow type is `compatible`** — deliberately. That hazard is real
but this gate cannot see how many non-terminal executions exist;
`harvest workflow-types reachability` ([#520](https://github.com/autumn-foundation/autumn-harvest/issues/520))
answers it exactly by counting live runs. Double-gating would mean every
legitimate handler removal needs a schema acknowledgement it can learn nothing
from — pure acknowledgement fatigue — so the delta's reason routes there instead.

### Exit-code contract and output

`0` when every delta is compatible; `1` when any is breaking. The diff is always
printed to stdout **before** the non-zero exit. Default text output is
`workflow.role: <field_path> — <verdict> — <reason>`; `--format json` emits a
per-type, per-field machine-readable diff (`workflow`, `role`, `field_path`,
`change`, `verdict`, `reason`), with `field_path` an RFC 6901 JSON Pointer
matching the violation format `POST /workflows/{name}/start` already returns.

### The escape hatch — auditable, never silent

`harvest schema update` **refuses** a breaking delta unless `--acknowledge`
records why it is safe, and names the flag in the error. The justification (plus
an optional `--recorded-in` pointer to a changelog fragment / PR / runbook) is
written into the artifact as an **append-only** `acknowledged_breaking_changes`
record, so the schema change and its justification land in the same reviewable
git diff. A blank or whitespace-only reason is refused — a rubber stamp is not
an acknowledgement — and on refusal the baseline is left untouched.

### Docs and CI

New guide `docs/workflow-schema-contract-guide.md` (the perspective, the full
ruleset with per-rule *why*, generating `--current`, the exit-code contract, the
escape hatch, a GitHub Actions recipe and a pre-commit hook, and the explicit
out-of-scope list). Cross-linked from `docs/workflow-determinism-guide.md`,
which gains a "Gating payload-schema changes in CI" section and folds the gate
into the release playbook as layer 2 of six.

CI runs the gate **end to end** on every OS: generate the contract from the
example registry, then `harvest schema check` it against the checked-in
baseline. That is simultaneously the drift guard for the artifact and proof the
documented recipe works. (`examples/schema_workflow.rs` was previously not built
by CI at all.)

### Scope

**In scope:** the three published workflow payload schemas (#373).
**Out of scope, deliberately:** signal / query / update payload schemas
([#610](https://github.com/autumn-foundation/autumn-harvest/issues/610)),
activity schemas, runtime enforcement / per-build pinning
([#171](https://github.com/autumn-foundation/autumn-harvest/issues/171)),
auto-migration or upcasting of recorded payloads, and non-deterministic *code*
([#386](https://github.com/autumn-foundation/autumn-harvest/issues/386) /
[#778](https://github.com/autumn-foundation/autumn-harvest/issues/778)).
A workflow publishing no schema is not gated at all — opt-in, exactly like #373
— but *withdrawing* an already-published schema is breaking, so coverage cannot
silently regress.

### Tests

New core module `autumn-harvest/src/schema_contract.rs` (unconditional — no `db`
or `schema` gate, so the CLI can link it with `default-features = false`).

101 no-DB integration tests in
`autumn-harvest/tests/integration/workflow_schema_contract_tests.rs`. Every rule
that is a *validation* narrowing carries an independent **oracle** assertion
against the engine's own `validate_against_schema` — the same code that gates
`POST /workflows/{name}/start` — proving the recorded instance is valid under
the baseline and invalid under the current schema, so the verdict is not merely
"what the differ says". Also covered: the numeric-format width lattice, both
unit-enum forms, both `Option<T>` representations, the `additionalProperties`
3-point lattice, map/tuple/array recursion, `$ref` resolution and recursive-cycle
termination, the depth cap reporting itself rather than silently truncating,
annotation churn producing zero deltas, artifact round-trip and byte stability,
the escape hatch in all three states, and the seeded breaking/compatible
success-metric fixtures.

27 CLI integration tests in
`autumn-harvest-cli/tests/integration/schema_check_cli.rs` (exit codes, missing
and malformed baselines, both output shapes, the escape hatch, clap wiring, and
the raw `GET /workflows/registered` body as `--current`) — including three that
spawn the **real `harvest` binary** via `env!("CARGO_BIN_EXE_harvest")` and
assert the process exit status and stdout, so the exit-code contract is proven
at the process boundary rather than only at the library one. Plus 7 inline unit
tests for the branch-key and canonicalisation primitives.

**Success metric — met.** A seeded breaking-change fixture trips the gate (exit
`1`) and a seeded compatible-change fixture passes it (exit `0`), asserted both
as library tests and end to end through the real binary.

### Post-review hardening

A multi-angle review (replay-semantics, differ-soundness, CLI/UX, test-quality)
produced findings that were fixed and each pinned by a test verified to fail
when the fix is reverted:

- **`allOf` was unanalysed but not fail-closed** — the one place the gate was
  actively *wrong*. `{"type":"integer"}` →
  `{"allOf":[{"type":"integer"},{"minimum":10}]}` rejects a recorded `1` while
  every flat-compared keyword is unchanged. `allOf` is now diffed positionally
  (added *or* removed conjunct is breaking, the latter because draft-07
  `additionalProperties` does not see into subschemas, so dropping a member can
  *tighten*). Paired with canonicalisation that lifts the lone
  `{"allOf":[{"$ref":…}]}` wrapper `schemars`' `RemoveRefSiblings` emits the
  moment a struct-typed field gains a doc comment — otherwise a `///` edit would
  read as a breaking change.
- **An unresolvable `$ref` change was invisible.** `$ref` sits in the analysed
  set, which excludes it from the fail-closed sweep, so swapping one external or
  dangling target for another reported *no delta at all*. Now reported breaking.
- **A malformed bound read as "removed" → compatible.** `"minimum": 10` →
  `"minimum": "10"` collapsed to `None` through `as_f64`, indistinguishable from
  absent. Present-but-unreadable bounds now fail closed.
- **Indistinguishable `oneOf` branches collapsed silently.** Branches are matched
  across revisions by key in a map, so two same-keyed branches dropped one —
  hiding a variant *removal* behind the survivor. Ambiguous keying now fails
  closed. Relatedly, the externally-tagged heuristic now requires a single
  *declared property* as well as a single required one, so an ordinary struct
  with one mandatory field is no longer mistaken for a disjoint variant.
- **`contract_version` was never validated.** A future v2 artifact would be
  diffed silently under v1 rules — confident answers from the wrong ruleset, the
  one failure mode a compatibility gate must not have. Now refused.
- **A truncated report no longer certifies anything.** The delta cap bounds
  *storage*; the tallies keep counting, and `check` now fails closed on
  truncation rather than reporting on a partial listing.
- **An empty `--current` is diagnosed, not diffed.** A producer that registered
  no workflows is technically "every workflow removed", but that buries the real
  cause under N breaking deltas and invites an `--acknowledge` that would
  overwrite the baseline with nothing and disarm the gate permanently.
- **The baseline is written atomically** (sibling temp file + `rename`), so an
  interrupted `update` cannot leave a half-written artifact that the next
  `check` would diff against.

A second round (automated review on the PR) found four more of the same class:

- **Bounds were compared through `f64` with an absolute `f64::EPSILON`
  tolerance.** `f64::EPSILON` (~2.2e-16) is machine epsilon *at 1.0*, so it
  swallowed any smaller change — `minimum: 1e-20` → `2e-20` is a real narrowing
  reported as no delta — and `f64`'s 53-bit mantissa collapsed distinct integers
  above 2^53, so a bound on an `i64` field could move unnoticed. Integers now
  compare exactly through `i128`; only a genuine float falls back to `f64`,
  where `f64` *is* the value's own representation.
- **The branch container keyword was not compared.** Only the *current* keyword
  was kept, so `anyOf` → `oneOf` compared equal branch-for-branch and emitted
  nothing — while narrowing: `anyOf` accepts a value matching two or more
  branches, `oneOf` requires exactly one, so a recorded integer matching both an
  `integer` and a `number` branch is now rejected. `oneOf` → `anyOf` is the
  compatible direction. A node carrying *both* keywords now fails closed, since
  only one is analysed as the container.
- **`anyOf` branch order was erased by the match map.** `anyOf` is what
  `schemars` emits for `#[serde(untagged)]`, and serde binds the **first**
  matching variant in declaration order — so reordering `Int(i64)` and
  `Float(f64)` silently rebinds a recorded integer to the float variant. It
  still deserializes; it no longer means the same thing, which is exactly what
  this gate exists to catch. Reported without attempting to prove the branches
  overlap (disjointness is not something the differ models); disjoint variants
  can be acknowledged. `oneOf` order is not checked — exactly-one matching means
  order cannot affect binding.
- **A truncated diff could be acknowledged.** Audit records were generated from
  the capped delta listing while the rebase took the whole contract, so breaking
  changes past the cap were absorbed with nothing naming them — contradicting
  the artifact's promise of a record per absorbed delta. Now refused.
