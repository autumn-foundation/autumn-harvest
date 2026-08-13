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

150 no-DB integration tests in
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

35 CLI integration tests in
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

**Third review round** — two false verdicts and one skipped gate:

- **An unchanged ambiguous branch set was reported breaking.** Two multi-field
  object variants of a `#[serde(untagged)]` enum both key as `type:object`, so
  the fail-closed collision guard fired — on *both* sides, for an identical
  schema. Because the ambiguity is a property of the shape rather than of the
  edit, that workflow's own baseline could never pass the gate again: a
  false-BREAKING that permanently blocks every unrelated change to the
  repository, which for a CI gate is the worst outcome there is. Indistinguishable
  branches are now compared **pairwise by position** when both sides have the
  same count — unchanged yields nothing, and a nested change (including a `$ref`
  target change one level down) is still caught by the recursion. A change in
  the branch **count** is genuinely unclassifiable (the differ cannot say which
  branch went) and still fails closed. The accepted cost is that *reordering*
  ambiguous branches produces positional deltas; they are acknowledgeable, and
  for `anyOf` a reorder is a real rebind risk anyway.
- **An `anyOf` variant inserted ahead of an existing branch was compatible.**
  The second-round order check compares only keys **common to both** revisions,
  so a newly-added key is invisible to it — and the add loop marked every
  `anyOf` addition compatible. Prepending `Float(f64)` before `Int(i64)`
  therefore passed, even though serde's untagged binding rule captures every
  recorded integer into the new variant: a false-COMPATIBLE on exactly the
  silent-rebind class the order check exists to catch. Additions are now
  position-aware — inserted ahead of any pre-existing branch is breaking,
  appended after all of them stays compatible (the `T` → `Option<T>` shape,
  which appends a `null` branch). `oneOf` is unaffected: exactly-one matching
  means order cannot rebind anything.
- **The gate did not run for baseline-only changes.** The artifact lives under
  `docs/`, and CI treats a docs-only PR as needing no code jobs — so the gate's
  own trusted *input* was the one file that could change without being compared
  against the generated contract. A hand-edited, stale, or malformed baseline
  merged unchecked, which is precisely the drift the gate exists to prevent. The
  docs-only filter now carves out `docs/workflow-schema-contract.json` by exact
  path, pinned by a test that reads `ci.yml`. The guide beside it stays
  docs-only. This supersedes the earlier note that documented the hole as a
  known limitation.

**Fourth review round** — one false-COMPATIBLE and one bypassable gate (a third
finding, that `fs::rename` does not replace an existing file on Windows, was
refuted: `std` documents "replacing the original file if `to` already exists"
and the Windows implementation passes `MOVEFILE_REPLACE_EXISTING`):

- **A tag on the ADDED `oneOf` branch was taken as proof of disjointness.** The
  flag describes the new branch alone — that it is narrow, not that the branches
  it now sits beside are. Adding the singleton `{"enum":["x"]}` next to a broad
  `{"type":"string"}` makes the recorded value `"x"` match two branches, and
  `oneOf` requires exactly one, so the recorded value is rejected — reported
  compatible. Disjointness is now established only when **every** branch is
  tagged, which is serde's externally-tagged shape: the serializer emits exactly
  one variant key, so a recorded payload matches exactly one branch however many
  variants are added. The oracle (`validate_against_schema`, the same code that
  gates `POST /workflows/{name}/start`) confirms the fixture is a genuine break.
- **The escape hatch could be bypassed by hand-editing the artifact.**
  `schema update` refuses to absorb a breaking change without a justification,
  but nothing stopped a contributor overwriting the file directly. That leaves
  the artifact and the freshly generated contract in agreement, so the gate's
  diff is empty and a replay-breaking change merges with no record — defeating
  the issue's own success metric ("100% caught at PR/CI time") on exactly the
  path the audit log exists for. New `unacknowledged_breaking()` compares the
  artifact's change since its PREVIOUS revision and requires an acknowledgement
  covering every breaking delta; matching is a multiset difference over
  `(workflow, role, field_path, change)` restricted to records **new** in this
  revision, so a record carried over from the base cannot be reused to let the
  same field break twice. Exposed as `schema check --acknowledged-in <artifact>`
  and wired as a second CI step against `git show <base>:<artifact>`. The base
  object being unreachable (force-push, transient fetch failure) warns rather
  than blocking every PR on infrastructure; the artifact's first introduction
  has no previous revision and is skipped.

**Fifth review round** — two more Codex findings, both false-COMPATIBLE:

- **Canonicalisation deduped a genuine `oneOf` duplicate away.** Unit-only enum
  branches (`oneOf: [{"enum":["A"]}, {"enum":["B"]}]`, the shape `schemars` emits
  for a fieldless Rust enum) collapse into one flat `enum` array so a purely
  representational change produces no delta — and `sorted_enum` dedupes the
  merged values. Duplicating a branch (`[{"enum":["x"]}, {"enum":["x"]}]`) makes
  the recorded value `"x"` match twice, which `oneOf` rejects, but the collapse
  merged the two branches and the dedupe erased the difference: both revisions
  canonicalised identically and the break was invisible. The collapse is now
  skipped when it would dedupe a duplicate away, and only for `oneOf` — `anyOf`
  is "at least one", where duplicate branches are genuinely harmless, so it still
  collapses (it must, or every unit-only enum would churn). Distinct singleton
  branches still collapse in both containers, pinned by a test that would fail if
  the normalisation regressed.
- **The append-only acknowledgement log could be rewritten rather than
  appended to.** `unacknowledged_breaking()` subtracts the base revision's
  records so a stale one cannot cover a fresh break — but a contributor who
  *retargets* an existing record (editing the workflow/role/path/change it names,
  keeping a plausible reason) defeats that subtraction: the identity it looked
  for is no longer in the base multiset, so nothing is subtracted and the
  modified record reads as fresh coverage. The rewrite is invisible to the
  ordinary diff too, since the artifact and the generated contract still agree.
  New `dropped_acknowledgements()` reports records the base revision carried that
  are gone now — the rewrite's one observable signature — surfaced as a distinct
  `SchemaContractAuditLogRewritten` error because the remedy differs (restore the
  record and append, rather than record this break). Matching uses the same
  four-field identity, so editing only a `reason` or `recorded_in` is deliberately
  not reported: neither can make a record cover a different break, and flagging a
  typo fix would block legitimate work for no safety gain.

Both fixes were mutation-verified — the guard removed, the specific test
confirmed failing, then restored. A third mutant (moving the audit-log check
after the coverage check) **survived**, correctly: the two checks are
independent and both run before the branch returns, so ordering only decides
which failure is reported first. The code comment claiming otherwise was
corrected rather than left as an unfalsifiable rationale.

**Sixth review round** — two Codex findings, plus a third bug their tests
uncovered:

- **An OPTIONAL tag was taken as proof of disjointness.** A property pinned to
  a single `enum`/`const` value marked its branch discriminated whether or not
  the property was `required`. With an optional `tag` pinned to `"A"` the empty
  object already validates, so adding a branch whose optional `tag` is pinned to
  `"B"` makes `{}` match twice — rejected by `oneOf`'s exactly-one rule — yet
  the addition was reported compatible. A tag now counts only when it is in the
  branch's `required` set. Probed against `schemars` 0.8.22 before changing
  anything: a real `#[serde(tag = "…")]` enum emits `"required": ["type", …]`,
  so the narrowing costs no legitimate shape.
- **Changing a non-final `anyOf` branch could silently rebind recorded data.**
  `anyOf` is `#[serde(untagged)]`, where serde binds the FIRST matching variant,
  but the per-branch recursion judged each branch in isolation. Making branch
  0's `a` optional, with branch 1 requiring `b`, lets a recorded `{"b": …}` that
  bound to variant B match branch 0 and bind to variant A — it still
  deserializes, it just means something else — and only a *compatible*
  `property_became_optional` delta was emitted. Such a change is now breaking
  unless the branch is provably disjoint from every branch after it.
  Disjointness is proven two ways, both conservative: disjoint `type` sets
  (`integer` widened to cover `number` first, since a recorded integer satisfies
  both) or two differently-tagged discriminated branches. That escape is
  load-bearing rather than decorative — `Option<T>` is
  `anyOf: [{$ref: T}, {"type":"null"}]`, so without it *every* widening behind
  an `Option` would demand an acknowledgement; `object` and `null` share no
  instance, so nothing can rebind. Adding an *optional* property is also exempt:
  it does not change which instances the branch matches. `oneOf` is untouched —
  its exactly-one rule makes declaration order irrelevant.
- **Found while writing the tests above, not reported: the branch identity key
  was unstable under a compatible edit.** `branch_key` folded the *declared*
  property count into the key, so a struct with one required field behind
  `Option<T>` that gained an optional field flipped from `variant:a` to
  `type:object` — the branch then read as removed-and-re-added, two breaking
  deltas for adding an optional field. This is a false-BREAKING on one of the
  most common widenings there is, and it had been live since the branch-keying
  work in round 1. The key is now the required property's name alone; the
  property count still governs the *discriminated* flag, which is the only thing
  it was ever load-bearing for.

Five separate mutants were run for this round — the tag-required check, the key
stability fix, the rebind guard, the disjointness escape, and the
optional-property exemption — each reverted individually, the specific test
confirmed failing, then restored. The escape and the exemption were given their
own tests precisely because a guard's *carve-outs* are where a false-BREAKING
would hide, and neither would have been falsifiable through the guard's own
positive test.

**Seventh review round** — three Codex findings: two false-COMPATIBLE paths and
one unreachable arm.

- **A MIXED integer/float bound comparison still collapsed through `f64`.** The
  round-2 precision fix took its exact `i128` path only when *both* operands
  were JSON integers. A bound rewritten `9007199254740992.0` → `9007199254740993`
  is one integer and one float, so it fell back to `f64` — where the integer
  rounds *down* to 2^53 and compares equal to the baseline. That is a genuine
  tightening (the recorded value 2^53 satisfied the old minimum and is rejected
  by the new one) reported as no change at all: the round-2 hole, reached
  through the one pairing that fix did not cover. New `cmp_i128_f64` keeps the
  integer exact and compares it against the float's `floor`, with any fractional
  remainder breaking the tie; only a float/float pair still uses `f64`, where
  `f64` *is* both values' own representation. A bound written `10` in one
  revision and `10.0` in the other still compares equal, so the fix cannot make
  an unchanged bound look changed.
- **The rebind guard's optional-property exemption ignored `additionalProperties`.**
  Round 6 exempted `OptionalPropertyAdded` from altering a branch's match set,
  on the reasoning that a schema tolerating unknown keys already accepted that
  payload. That reasoning does not survive `additionalProperties: false`
  (serde's `deny_unknown_fields`): such a branch *rejected* `{a,b}` before and
  accepts it now. With branch 0 accepting only `{a}` and branch 1 accepting
  `{a,b}`, adding optional `b` to branch 0 silently rebinds every recorded
  `{a,b}` from variant B to variant A under serde's first-match rule — the exact
  hazard round 6 exists to catch, walking straight through its own exemption.
  The exemption is now withdrawn when the branch denies unknown properties. The
  guard's own doc comment had stated this precondition ("a schema *without*
  `additionalProperties: false`") without the code ever checking it.
- **A malformed `enum` becoming a well-formed one emitted nothing.** `diff_enum`
  handled "appeared" and "disappeared" but not "present in both, unreadable in
  one": with both objects carrying the key, neither presence arm fired and the
  function returned silently. A validator ignores a non-array `enum` and
  enforces an array one, so `"enum": "x"` → `"enum": ["x"]` newly rejects every
  recorded value outside the set while every flat-compared keyword is unchanged
  — the round-1 malformed-bound finding, in the one analysed keyword that never
  reaches the fail-closed sweep. Both directions now fail closed; two identical
  malformed values are not an edit and still emit nothing.

Three mutants were run for this round, one per fix, each reverted individually
with the specific test confirmed failing before restoring. Four
over-firing guards were written alongside them and were **already green before
the fixes** — an open branch gaining an optional property, a closed *final*
branch (nothing after it to steal from), an unchanged bound written `10` vs
`10.0`, and an unchanged malformed enum — so each fix is pinned on both sides:
it fires on the hazard and stays silent on the shapes it must not block.

### Windows CLI stack overflow (a pre-existing defect this PR surfaced)

The three `the_real_binary_*` tests added here are the **first** tests in the
repository to spawn the `harvest` binary as a process, and they immediately
failed on Windows CI with exit `-1073741571` — `STATUS_STACK_OVERFLOW`
(`0xC00000FD`). This is **not** a schema-gate regression: `harvest --help`
overflows identically, before any subcommand is dispatched. Windows gives a
process's main thread 1 MiB, and a debug build of a CLI with this many
subcommands exceeds that inside clap's own argument-tree construction; Linux's
8 MiB default hid it completely, so every `harvest` invocation on Windows has
been aborting for as long as the CLI has had this many subcommands.

`autumn-harvest-cli`'s `main` now does its work on a thread it spawns with a
16 MiB stack (a thread's stack is sized at spawn and is not subject to the main
thread's limit) — the same approach rustc itself takes, for the same reason.
Exit-code semantics are unchanged; `std::process::exit` does not unwind, so
stdout is flushed explicitly rather than relying on it to drain a block-buffered
handle. Skipping the tests on Windows was rejected: it would have deleted the
evidence and shipped a CLI that cannot run there.

Guarded by `the_real_binary_survives_a_one_mebibyte_main_thread_stack`, which
reproduces the Windows condition on Unix with `ulimit -s 1024` (that bounds the
main thread only, so it passes exactly when `main` sizes its own). Confirmed
red before the fix with `thread 'main' has overflowed its stack / fatal runtime
error: stack overflow, aborting`, and green after.

### Eighth and ninth review rounds

Five findings, all valid, all narrowing the gate (each removes a
false-COMPATIBLE — a change that could break replay and merged clean). Two of
them were only provable because `schemars` 0.8.22 was probed directly rather
than assumed, and that probe is what kept the fixes from costing any derived
schema.

- **A `oneOf` branch tagged at a DIFFERENT key was treated as disjoint.**
  Disjointness was decided by comparing branch *identity strings* and accepting
  any difference, so an internal tag on `tag` beside one on `kind` read as
  provably disjoint. They are not: `{"tag":"A","kind":"B"}` satisfies both, and
  `oneOf` requires exactly one match, so that recorded payload — valid against
  an open one-branch baseline — is newly rejected. A discriminator is now held
  structurally (`Discriminator::{Tag, Unit, Variant, None}`) and a tag proves
  disjointness only when both branches pin the **same** key to different
  values. Probed: a real `#[serde(tag = "…")]` enum emits the same tag name on
  every branch, so this costs no derived schema.
- **An OPEN single-required-field object was treated as an externally-tagged
  variant.** Two of those are not disjoint either — `{"A":…,"B":…}` satisfies
  both, each treating the other's key as an undeclared extra. The proof now
  requires `additionalProperties: false`, which the probe confirms `schemars`
  emits for externally-tagged enums. This replaces an earlier argument from
  serde's *serializer* emitting one key: true of data serde wrote, but the gate
  also accepts hand-written schemas and JSON arriving over
  `POST /workflows/{name}/start`. Three existing tests were correcting-not-
  weakening casualties: their fixtures modelled the shape *without* the closure
  and so asserted a verdict that was wrong; they now use a shared `variant()`
  helper that emits what `schemars` actually does.
- **Disjointness is a property of every PAIR, not of each branch.** The
  all-branches-carry-a-tag boolean was replaced by a pairwise check, which is
  what the first finding above actually requires.
- **Widening an EXISTING `oneOf` branch skipped the overlap guard entirely.**
  The guard returned early for anything that was not `anyOf`. Baseline branches
  `{"maximum":0}` and `{"minimum":1}` accept a recorded `1` exactly once;
  relaxing the first to `{"maximum":2}` reads as a compatible bound relaxation
  in isolation while making `1` match both branches and be rejected. `oneOf`
  now checks a widening against **every** sibling (order is irrelevant there,
  unlike `anyOf`'s first-match rule, which still checks only later branches).
  Narrowing is deliberately exempt: it cannot create an overlap, and the
  payloads it drops are already reported by the narrowing itself.
- **`default` was stripped as an annotation.** It is one to a *validator* — no
  value of it changes whether a payload validates — but not to *serde*.
  `#[serde(default = "retries")]` leaves the field optional and records the
  computed fallback there (probed: `"default": 3`). Change the function to
  return `5` and every recorded payload that omitted the key deserializes to a
  different value: same JSON, different meaning, and invisible because both
  revisions had the key stripped before comparison. `default` is now compared
  where serde would actually substitute it — beside a key optional on **both**
  sides. One beside a required key, or on a root or array-item schema, is never
  substituted and stays silent, so documentation edits still produce no deltas.
- **A malformed `additionalProperties` ranked as a real schema.** A value that
  is neither a boolean nor an object is ignored by a validator, so it
  constrains nothing and belongs with `true` on the lattice, not with a schema.
  Ranking it as a schema meant correcting `"additionalProperties": "oops"` to
  `{"type":"string"}` left both revisions at the same rank and emitted nothing,
  while every recorded non-string extra became invalid. Same fail-closed family
  as the round-7 malformed-`enum` fix.
- **Declaring a key recorded payloads carried as an extra.** Serde *ignores* an
  undeclared key but *parses* a declared one, so adding `b: Option<String>`
  breaks any recorded payload whose `b` was not a string. This is reported only
  where it is **provable** — when the baseline declared what an extra had to be
  (`"additionalProperties": {"type":"integer"}`) and the new property's type
  shares nothing with it. On a fully open object nothing is provable in either
  direction, and reporting it would make *every* optional-field addition
  breaking — the single most common compatible change, and the opposite of what
  the gate is for — so it stays compatible. The residual and its
  `#[serde(deny_unknown_fields)]` mitigation (which turns the unprovable case
  into a provably safe one) are documented under "Limits the gate does not
  cover".

Nine mutants were run across the two rounds, one per fix, each reverted
individually with the specific test confirmed failing before restoring. Every
fix ships with its over-firing guard, because a guard's *carve-outs* are where
a false-BREAKING would hide: the same-tag-key shape stays compatible, the
closed-variant shape stays compatible, narrowing a `oneOf` branch reports only
the narrowing, a single-branch `oneOf` has nothing to overlap, a `default`
beside a required key or on a root schema emits nothing, two malformed values
emit nothing, and adding an optional property to a fully open object is still
compatible.

The checked-in baseline was regenerated: it embeds the ruleset, and moving
`default` from the ignored-annotation list to the analysed one changes it. The
gate re-run reports zero deltas and the artifact is byte-current.

### Tenth review round

Three more false-COMPATIBLEs — each one a shape where the gate would have let a
real replay break merge — plus one documentation-drift gap found while fixing
them.

- **Widening the *last* `oneOf` branch.** Ambiguous branches (two that key the
  same, e.g. two multi-field object variants) are compared by position, and the
  positional path handed each branch only the branches declared *after* it —
  for `anyOf` and `oneOf` alike. That is right for `anyOf`, which binds the
  first match. `oneOf` requires **exactly** one match and is order-independent,
  so the last branch got no rivals at all and its widening was scored on its own
  compatible merits. Relaxing the last branch's `required` until it matches
  anything now correctly reports breaking. Round 9 fixed this for *keyed*
  branches; this is the same rule for the positional path.
- **A semantic `format` is not an annotation.** Canonicalization dropped every
  non-numeric `format`, justified by "the engine's validator ignores `format`,
  and a string is a string". That answers the wrong question: this gate asks
  whether recorded JSON still **deserializes**, and `format: "uuid"` is
  `schemars`' record that the field is a `Uuid`, whose `Deserialize` rejects a
  string that is not one. So `String -> Uuid` — a textbook break — canonicalized
  identically on both sides and vanished. Introducing or changing a semantic
  format is now breaking; removing one is compatible. The accepted cost is that
  a purely descriptive `#[schemars(format = "email")]` on a plain `String` is
  flagged too: the differ sees the schema, not the Rust type, and a curated
  allowlist of "really assertive" formats would be silently wrong for any custom
  `JsonSchema` impl. That is a one-time acknowledgeable flag; the mirror mistake
  is a silent break.
- **Cardinality bounds are unsigned integers.** `minLength`/`maxLength`/
  `minItems`/`maxItems` count things, and the engine's own `validate_node` reads
  them with `as_u64`, **ignoring** anything else — while the differ read them
  with `as_f64`. So `"minLength": 1.5` (which the validator ignores) compared
  against `1` (which it enforces) scored as a *relaxation*, when the effective
  constraint actually went from none to "rejects the empty string". Readability
  now matches the validator: `as_u64` for cardinalities, `as_f64` for
  `minimum`/`maximum`.
- **Ruleset drift guard.** The artifact embeds the published ruleset, but
  nothing compared it to the code — `schema check` diffs *schemas*, and a
  ruleset edit produces no delta. A new rule could therefore ship while the
  checked-in artifact kept telling operators the old one, which is precisely
  what happened while writing this round.
  `the_checked_in_baseline_documents_the_current_ruleset` closes it.

Seven mutants, one per fix, each reverted individually with the specific test
confirmed failing before restoring. Over-firing guards again ship with each
fix: widening the last **`anyOf`** branch stays compatible (nothing binds after
it), a tagged `oneOf` whose branches are provably disjoint stays compatible
under the same widening, narrowing reports only the narrowing, an unchanged
`format` produces no delta at all, and a fractional `minimum` — genuinely
readable — still relaxes compatibly.

The baseline was regenerated for the ruleset change; the gate re-run reports
zero deltas, which also confirms the semantic-format rule costs the shipped
`examples/**` schemas nothing.

### Eleventh review round

One finding, in the escape hatch rather than the differ: a **blank
acknowledgement could still buy coverage**.

A rubber stamp is refused at every authoring path — `acknowledged_update`
returns `BlankAcknowledgement`, and `diff_schema_contracts` reports
`AcknowledgementMissingReason` for a hand-edited one. Neither reaches
`unacknowledged_breaking`'s `head`: the escape-hatch mode loads that artifact
*separately* from the two contracts it diffs, and coverage matched on
`(workflow, role, field_path, change)` alone. Run `schema check
--acknowledged-in` on its own — without the ordinary check, whose baseline
happens to *be* the head artifact — and a hand-written record with an empty
`reason` absorbed a real break.

Fixed in two places, for two different reasons:

- **The core** (`unacknowledged_breaking`) now skips blank-reason records when
  building the coverage multiset. This is the fail-closed guarantee, and it
  holds for every caller regardless of invocation order.
- **The CLI** reports the blank record explicitly. Without it the operator sees
  "nothing acknowledges this" beside a record that plainly exists; the core fix
  alone gets the verdict right and the diagnostic wrong.

Two mutants, one per half, each reverted individually with the specific test
confirmed failing before restoring. Reverting only the CLI half is instructive:
the check still exits 1 (the core filter is what makes it fail closed) and only
the message assertion fails, which is exactly the split the two fixes encode.
The over-firing guard — a record with a real reason still covering its delta —
was green *before* the fix, pinning it on both sides.

### Twelfth review round

Two findings, and neither is in the branch-matching machinery the previous six
rounds circled — one is in canonicalization's most basic assumption, the other
is in what CI asks the gate for.

- **A payload field is not an annotation, however it is spelled.** Annotation
  stripping keys on the KEY NAME, and the recursion treated every JSON object as
  a schema — including the `properties` map, whose keys are arbitrary field names
  chosen by the payload author. A field literally called `description` (or
  `title`, `examples`, `deprecated`, `readOnly`) was therefore **deleted from
  both revisions**, and a field that does not exist cannot be seen to change
  type: `String -> integer` on it canonicalized identically and passed. The same
  applied to `$defs`/`definitions` entries (a deleted definition leaves its
  `$ref` dangling, so the whole subtree stops being compared) and to an
  object-valued `default`, which is instance data the round-9 fix compares —
  rewriting it silently changes what is compared. Canonicalization is now
  context-aware: `SCHEMA_MAP_KEYWORDS` (`properties`, `patternProperties`,
  `$defs`, `definitions`, `dependentSchemas`, `dependencies`) preserve every key
  and canonicalize each value as a schema; `INSTANCE_VALUE_KEYWORDS` (`default`,
  `const`) are preserved byte-for-byte; everything else recurses as before. A
  genuine annotation in annotation position is still stripped, so no doc-comment
  edit becomes a delta.
- **CI asked "is anything breaking?" when the artifact's promise is "this is
  what was deployed".** `schema check` exits 0 on a compatible delta, so nobody
  regenerates the baseline, so it records what was deployed *some time ago* while
  the gate reads it as what was deployed *last*. That gap round-trips: add an
  enum variant (compatible, nothing absorbed), deploy and record payloads
  carrying it, then remove it again — the generated contract now equals the
  **stale** baseline, both checks see zero deltas, and replay of anything written
  by the intermediate release fails with no step ever reporting a delta. New
  `schema check --require-current` refuses any unabsorbed delta, compatible ones
  included, and CI passes it. It is `conflicts_with` the escape-hatch mode, where
  deltas against the base revision are the entire point.

Four mutants across the two fixes, each reverted individually with the specific
test confirmed failing before restoring — and one of them **survived on the first
attempt**, which was the useful result: the CI-wiring guard asserted
`CI_YAML.contains("--require-current")`, and the step carries a comment
explaining the flag, so removing it from the *command* left the guard green. It
now scans the invocation lines specifically and fails on that mutation. The
over-firing guards ship alongside: a real annotation is still stripped, an
already-current baseline still passes `--require-current`, and an artifact
carrying acknowledgement records is current once its schemas match.

### Thirteenth review round

Two P2 findings, both about the gate quietly disagreeing with itself.

- **A `$ref` inside an unanalysed constraint laundered a breaking change.** The
  fail-closed sweep compares an untracked keyword's value literally, and
  `definitions`/`$defs` are filtered out of it as containers "reached through
  `$ref`". But only the *analysed* traversal follows refs — so when a definition
  is referenced solely from an unanalysed keyword, nothing walks there.
  `"not": {"$ref": "#/$defs/T"}` stays byte-identical while `T` is rewritten
  from `{"type":"string"}` to `{"type":"integer"}`: a recorded `1` goes from
  accepted to rejected and the gate reports a clean pass. Writing that identical
  change inline was already breaking, so one level of indirection was enough to
  bypass the rule. Every local reference reachable from an unchanged unanalysed
  constraint is now resolved on both sides — transitively, cycle-safe via the
  pointer set that doubles as the visited guard, and depth-capped — with any
  differing target reported breaking, one delta per keyword. A target that
  resolves on only one side counts as differing, which is the fail-closed
  outcome. Because both roots are canonicalized before the diff, annotation
  churn inside such a definition still produces nothing.
- **Every generated artifact told operators that `default` and non-numeric
  `format` are stripped.** Rounds 9 and 10 promoted both to analysed keywords
  whose change is reported breaking, and the hand-written description never
  followed. The round-10 drift guard structurally cannot catch this: it compares
  the artifact to the code constant, so a stale sentence is stale on both sides
  and compares equal. The description is now *generated* from
  `ANNOTATION_KEYWORDS`, the same constant the canonicalizer uses, so it cannot
  contradict the ruleset it describes; `unanalysed_keyword_policy` gained the
  ref-following clause above. Two new guards pin it against the ruleset rather
  than against a copy of it: no analysed keyword may appear in the stripped list,
  and the list must equal `ignored_annotations` exactly — the second one blocking
  the trivial "delete the sentence" fix to the first.

Four mutants, each reverted individually with the specific test confirmed
failing: flag every constraint containing a `$ref` (all three over-firing guards
fail), follow only one hop (the transitive test fails), and restore the stale
hardcoded annotation list (both description guards fail). The over-firing guards
ship alongside — an unchanged definition, annotation churn inside one, and a
self-referential definition each stay silent.

### Fourteenth review round

One P1, verified against both the differ and the engine before acting.

- **A `$ref` cycle silently swallowed the landing node's own constraints.**
  `diff_node` memoizes each resolved `$ref` pair before recursing, which is what
  makes the diff linear rather than exponential over cross-referencing
  definitions. But a chain that terminates on a *cycle* lands on a node that
  still carries `$ref` — `resolve_ref` breaks out rather than looping — so the
  recursive call re-resolved to that same pair, hit the memo, and returned
  without comparing anything. `{"$ref":"#","type":"string"}` →
  `{"$ref":"#","type":"integer"}` produced zero deltas, while the engine's
  `validate_node` breaks the cycle the same way and then *enforces* the sibling
  `type`, so a recorded string went from accepted to rejected on a clean gate
  pass. The memo is kept — dropping it does not terminate — and the recursion is
  now gated on the resolved node being ref-free; the cycle-terminated case falls
  through to the keyword comparison that was already there, with `b`/`c` bound to
  the landing nodes. Termination still holds because the memo was already
  inserted, so anything under the landing node that walks back around the cycle
  resolves to the same pair and returns.

  Only the cycle-terminated case changed. When resolution reaches a ref-free
  target, discarding the referring node's siblings stays correct: that is
  draft-07 `$ref` semantics, and it is what the engine's own `schema = resolved`
  reassignment does.

Two mutants, each reverted individually with the specific outcome confirmed:
restoring the unconditional recursion fails exactly the two detection tests, and
disabling the memo does not terminate at all. Three over-firing guards ship
alongside — an unchanged cyclic schema, annotation churn inside one, and a
recursive definition reached through `properties` from its own cycle landing
node each stay silent.

### Fifteenth review round

One P2, and the interesting part is where the fix does *not* go.

- **An unrecognised `type` name bought a disjointness proof it cannot support.**
  The engine's `type_matches` falls through to `_ => true` for any name it does
  not enforce, so `{"type":"bogus"}` accepts *every* value — it is the universal
  set. The differ compared declared names as strings, so `bogus` and `string`
  read as disjoint, and adding such a branch beside an existing string branch was
  reported compatible; a recorded `"x"` then matches both branches and `oneOf`
  rejects it.

  The restriction is applied to `branches_provably_disjoint` only, **not** inside
  `type_sets_disjoint`, because the two callers use the same predicate in
  opposite directions. Proving disjointness to justify a *compatible* verdict
  must refuse an unreadable name. Proving it to justify a *breaking* one — the
  `additionalProperties` caller, asking "can no recorded extra satisfy the newly
  declared property?" — must keep it: an unreadable extras type means the
  baseline accepted extras of every type, so a recorded one really can fail the
  new declaration, and refusing the proof there would lose a true break. The
  unifying rule is that an unreadable type name never buys a COMPATIBLE verdict.

Two mutants. Bypassing the readability guard fails the three detection tests.
Moving it into `type_sets_disjoint` — the plausible "simpler" placement —
initially survived, which showed the site choice was reasoned but unpinned;
`declaring_a_property_over_unreadable_baseline_extras_is_still_breaking` now
pins it, and the mutant fails against it. Three over-firing guards ship
alongside, including one asserting every recognised name still proves
disjointness against every other, so the change reads as a whitelist rather than
a blanket refusal. The whitelist itself is pinned against the ENGINE rather than
against a copy of itself: each name must actually reject a wrong-typed value,
and `bogus` must accept every value — the premise the fix rests on.

### Sixteenth review round

Three findings: one genuine P1, one genuine P2, and one refuted with a test that
now pins the refutation.

- **P1 — declaring an optional property over typed extras needed containment,
  not overlap.** Serde ignores an undeclared key but *parses* a declared one, so
  every recorded value that satisfied the baseline's `additionalProperties` is
  newly type-checked against the added property. The differ asked whether the two
  shared *no* type, which is the wrong question: extras typed
  `["string","integer"]` and a new `b: Option<String>` overlap on `string`, so the
  addition read as compatible, yet a recorded `{"b": 1}` had passed the extras
  schema and is now rejected. The predicate is now containment (`extras ⊆
  declared`) via the same `type_set_admits` helper `diff_types` uses, so an
  unrecognised name on the declared side fails closed identically. Disjointness
  remains a strict subset of the new answer, so every verdict this site produced
  before is preserved.

  Consequence: `type_sets_disjoint` no longer has a breaking-direction caller, so
  the round-15 comment describing two callers with opposite needs is rewritten to
  state the rule rather than an obsolete inventory.

- **P2 — a `$ref` under the ignored branch-container sibling was invisible.** A
  node carrying both `oneOf` and `anyOf` is guarded by comparing the two
  *literal* arrays, so an unchanged `anyOf` whose `$ref` target was rewritten
  slipped past. Nothing else would see it: `branch_set` traverses `oneOf` only,
  `anyOf` is an ANALYSED keyword so the fail-closed sweep in `diff_unanalysed`
  skips it, and `definitions`/`$defs` are excluded there as containers "reached
  through `$ref`". The engine enforces the two keywords as independent blocks, so
  narrowing the referenced target really does reject a value the analysed branch
  set still accepts. The existing `diff_refs_under_unanalysed` sweep is now run
  over the ignored sibling, with the ignored keyword derived from what
  `branch_set` actually selected so the two cannot drift.

- **Refuted — a malformed `properties` is not a distinct case.** The report asked
  for a present-but-unreadable `properties` to fail closed the way a malformed
  `enum` or `additionalProperties` does. The engine reads the keyword as
  `.get("properties").and_then(as_object)` in *both* places it consults it — the
  recursion into declared fields, and the `known_keys` set that
  `additionalProperties` subtracts — so `"oops"` declares exactly nothing,
  byte-identically to omitting the key. The transition the report describes is
  therefore the open-object case already deferred for adjudication, reached
  through a different spelling. Failing closed on only the malformed spelling
  would make the verdict depend on a difference the runtime cannot observe:
  deleting the corrupt line would flip BREAKING to COMPATIBLE with no change in
  behaviour. Two tests ship instead of a fix — one pinning the equivalence
  against the real validator (so a future divergence forces the rule), one
  showing the `required` walk already catches the non-deferred half.

Seven mutants across the two fixes, each killed by the test that pins that
branch: reverting containment to overlap, and each of the containment predicate's
three arms flipped, fail exactly their own detection or over-firing guard;
removing the sibling sweep, and pointing it at the *selected* keyword instead of
the ignored one, both fail the rewritten-target test. Two over-firing guards ship
with the sweep — an unchanged target and annotation churn inside one — and three
with the containment predicate, covering the wider-extras, unconstrained-property
and already-compatible directions.

### Seventeenth review round

One P1, fixed.

- **P1 — containing the extras' `type` is not containing the extras.** The
  sixteenth round replaced overlap with containment at the
  `additionalProperties` site, but the containment it computed was over `type`
  alone. Every *other* constraint keyword narrows further, so a declared
  property could contain the extras' type and still reject values the baseline
  accepted: extras `{"type": "string"}` with a new property
  `{"type": "string", "enum": ["x"]}` contains `string`, yet a recorded
  `{"b": "y"}` deserialized before and does not now. The same holds for a bound
  (`minLength`), a `pattern`, or any keyword the extras schema did not already
  demand. Proving the general case is full schema containment, which this differ
  does not implement, so the predicate now fails closed on anything the extras
  schema does not already demand *identically*. Two keywords are exempt by
  construction: `type`, which has its own containment rule, and `default`, which
  supplies a value for an *absent* key — a recorded extra is by definition
  present, so a default cannot reject one.

  The accepted cost is one direction of over-firing: a declared constraint
  *looser* than the baseline's own extra constraint (`minLength` 5 → 3) reads as
  a difference and fails closed, even though every recorded extra still
  validates. That is the safe direction — it asks for an acknowledgement rather
  than silently passing a break — and ships pinned by a test that documents it
  as a decision rather than an accident.

Four mutants. Dropping the constraint guard entirely, and weakening it to
presence-only rather than value-identity, each fail their own detection test —
the second only after a *tightened*-constraint test was added, since a guard that
merely checks presence still catches the *added*-keyword direction. Inverting the
two `type`-containment arms fails the wider-extras and unconstrained-property
guards. A fifth mutant survives and is documented rather than tested: the
`ANNOTATION_KEYWORDS` filter inside the guard is unreachable, because
canonicalisation strips annotations from both roots before the diff ever runs. It
stays as defensive code that would matter if that ordering ever changed.

### Eighteenth review round

Two findings, both fixed — one in the differ, one in the CI gate itself.

- **P1 — `const` is not a proof of disjointness.** A `oneOf` branch tagged with
  `const` was accepted as a discriminator, so adding a branch beside it read as
  COMPATIBLE on the grounds that the two are provably disjoint. The engine's
  `validate_node` has no `const` arm at all: an unenforced keyword accepts every
  value, making a `const`-tagged branch the universal set — exactly the
  unrecognised-`type`-name case the fifteenth round closed. Only a singleton
  `enum` proves anything, because `enum` *is* enforced.

  The fix separates the two jobs the tag was doing. A branch **key** is identity
  — it matches a baseline branch to its current counterpart — and never claims
  disjointness, so it still keys on `const`; dropping it there would make two
  differently-tagged branches key alike and mask a variant removal. A
  **`Discriminator`** is proof, and is now minted only from a singleton `enum`.
  This is the same directional rule the differ already applies elsewhere: an
  unreadable or unenforced construct never buys a COMPATIBLE verdict.

- **P2 — `--require-current` never compared the artifact's regenerated
  metadata.** The flag refused a stale schema *body*, but the differ only ever
  reads `workflows`, so a hand-edited `compatibility` block — the
  machine-readable claim about which rules this gate implements — sailed
  through. A reviewer trusting `analysed_keywords` could therefore read a
  keyword list this build does not implement. `--require-current` now also
  compares `description` and `compatibility`, naming which drifted and pointing
  at `harvest schema update`.

  Two siblings are deliberately excluded. `version` records which *build*
  produced the artifact rather than what the gate checks, so comparing it would
  force a regeneration on every crate version bump for no change in meaning.
  `contract_version` needs no comparison at all — `parse` already hard-refuses a
  value this build does not implement, and rebuilds `workflows` and `coverage`
  from the entries.

Five mutants. Dropping either metadata comparison fails its own detection test;
adding a `version` comparison fails the over-firing guard that pins the exclusion.
On the differ side, restoring `const` to the discriminator fails the
added-branch test, and dropping `const` from the branch *key* fails the
variant-removal test — the pin that makes the identity-versus-proof split sharp,
since an unchanged schema compares identically even with degraded keys and so
cannot distinguish the two.
