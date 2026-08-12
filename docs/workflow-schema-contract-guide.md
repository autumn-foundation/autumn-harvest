# Workflow Schema Contract — Replay-Compatibility Gate

This guide is for anyone who changes a `#[workflow]` function's **input, output,
or error type**. It explains what `docs/workflow-schema-contract.json` is, how
`harvest schema check` classifies a change, and how to acknowledge a deliberate
migration.

Tracked by [issue #794](https://github.com/autumn-foundation/autumn-harvest/issues/794);
builds on the published-schema surface from
[issue #373](https://github.com/autumn-foundation/autumn-harvest/issues/373).

---

## The problem it solves

A contributor renames a field:

```diff
 #[derive(Serialize, Deserialize, JsonSchema)]
 pub struct OnboardInput {
     pub user_id: i64,
-    pub email: String,
+    pub email_address: String,
 }
```

`cargo build` is green. Tests are green. The PR merges. On deploy, **every
in-flight `onboarding` execution** fails: its recorded `WorkflowStarted` event
carries `{"email": …}`, which no longer deserializes into `OnboardInput`, so the
run wedges and eventually DLQs.

Nothing in the toolchain catches this, because the payload types compile fine —
the incompatibility is with **JSON already written to `harvest_events`**, not
with any other code.

`harvest schema check` is that missing gate. It diffs the schemas your app
publishes *right now* against a checked-in baseline and fails the build on any
change that would break replay.

---

## The perspective: replay-read

Every verdict answers exactly one question:

> **Does JSON already recorded in `harvest_events` still deserialize into the
> new type, and still mean the same thing?**

This is deliberately *not* general JSON Schema subtyping. It is
asymmetric — the schema is a **read** contract for historical data — which is
why, for example, adding an enum variant is safe (nothing recorded matches it)
while removing one is not (recorded data does).

---

## Quick start

```console
# 1. Generate what your app publishes right now (see "Generating --current").
$ cargo run --bin dump-schema-contract > /tmp/current.json

# 2. Gate it.
$ harvest schema check --current /tmp/current.json
schema check: no breaking changes (0 compatible delta(s))
```

A breaking change:

```console
$ harvest schema check --current /tmp/current.json
onboarding.input: /email — breaking — property `email` removed; the value recorded for it is
  silently dropped on replay, so the workflow no longer observes it. If this is a rename, add
  `#[serde(alias = "email")]` to the new field.
onboarding.input: /email_address — breaking — required property `email_address` added; JSON
  recorded before this change has no such key and fails to deserialize (serde: `missing field
  \`email_address\``). Make it `Option<T>` or add `#[serde(default)]` to keep in-flight
  executions replayable
schema check: 2 breaking change(s) of 2 delta(s)
$ echo $?
1
```

---

## Generating `--current`

Harvest is a **library**: the workflow registry lives in *your* process, so only
your crate can enumerate it. Add a three-line binary:

```rust
// src/bin/dump-schema-contract.rs
fn main() {
    let contract = autumn_harvest::WorkflowSchemaContract::from_infos(
        env!("CARGO_PKG_VERSION"),
        &registered_workflows(), // the same Vec<WorkflowInfo> you pass to .workflows(...)
    );
    print!("{}", contract.to_json_pretty().unwrap());
}
```

Point it at the **same** `Vec<WorkflowInfo>` your `HarvestPlugin`/`HarvestBuilder`
registration uses. The gate can only ever be as accurate as the registry it is
handed, so sharing one function is the difference between a real gate and a
decorative one.

This runs offline: no database, no server, no `db` feature.

`autumn-harvest/examples/schema_workflow.rs` is a working reference — run it
with `--emit-contract` to see the generator in action.

### Alternative: a running server

`--current` also accepts a raw `GET /workflows/registered` response body, so a
staging deployment works with no post-processing:

```console
$ curl -s "$HARVEST_URL/workflows/registered" > /tmp/current.json
$ harvest schema check --current /tmp/current.json
```

(`HARVEST_URL` already includes the mount prefix — it defaults to
`http://localhost:3000/api/harvest` — so the path above is *not* prefixed again.)

The generator is preferred for CI: it needs no live service, and it reflects the
code in the PR rather than whatever is currently deployed.

---

## The ruleset

Verdicts are computed from the **replay-read** perspective above. The full
ruleset is also embedded in the artifact itself (`.compatibility`), so it
travels with the file.

### Breaking

| Change | Why it breaks replay |
|---|---|
| Adding a **required** property | Recorded JSON has no such key → serde `missing field` |
| Removing a property | The recorded value is silently dropped; a hard failure under `#[serde(deny_unknown_fields)]` |
| **Renaming** a property | Surfaces as *removed* + *added-required* — both halves reported |
| Making an optional property required | Recorded JSON that omitted it now fails |
| Narrowing a type (`string` → `integer`; dropping `null`) | Recorded values of the dropped type fail |
| Narrowing a numeric **format** (`int64` → `int32`, `i64` → `u64`) | Recorded values outside the new range fail |
| Removing an `enum` value or a `oneOf`/`anyOf` variant | Recorded data matching it fails |
| Restricting `additionalProperties` (absent → schema → `false`) | Recorded extra keys are now rejected |
| Introducing or tightening a bound (`minimum`, `maxLength`, …) | Recorded values outside the new bound fail |
| Changing tuple arity | Recorded arrays of the old length fail |
| **Withdrawing** a published schema | Coverage regression: the type is no longer gated at all |
| Adding a `oneOf`/`anyOf` variant that is **not disjoint** from an existing one | Recorded data that matched exactly one branch may now match two — `oneOf` requires exactly one |
| Adding **or removing** an `allOf` conjunct | `allOf` is an AND: adding narrows. Removing is *also* breaking — draft-07 `additionalProperties` does not see into subschemas, so dropping a member that declared `properties` turns those keys into "additional" and **tightens** the schema |
| Two `oneOf`/`anyOf` branches the differ cannot tell apart | Branches are matched across revisions by identity; ambiguous keying means a removal could pass unnoticed, so the comparison fails closed |
| Changing an **unresolvable** `$ref` (external, dangling, cyclic) | The target cannot be read, so the change cannot be classified |
| A bound that is present but **not a JSON number** | It cannot be placed on the tighten/relax lattice; treating it as absent would report a malformed value as "removed" → compatible |
| A checked-in acknowledgement whose reason is **blank** | A rubber stamp is not an acknowledgement; the artifact is rejected |
| Any change to a constraint keyword **outside the analysed set** | Fail-closed — see below |

### Compatible

| Change | Why it is safe |
|---|---|
| Adding an **optional** property (`Option<T>` or `#[serde(default)]`) | Recorded JSON that omits it still deserializes |
| Removing a required marker | Strictly fewer keys are demanded |
| Widening a type (`integer` → `number`, `T` → `Option<T>`) | Every recorded value is still accepted |
| Widening a numeric format (`int32` → `int64`) | Every recorded value is still in range |
| Adding an `enum` value, or a **disjoint** `oneOf`/`anyOf` variant | Nothing recorded matches it |
| Relaxing `additionalProperties` or a bound | Strictly more is accepted |
| Adding a workflow type, or publishing a schema for the first time | Nothing was recorded under a contract that did not exist |
| **Removing** a workflow type | Gated more accurately elsewhere — see below |
| Editing any annotation (`title`, `description`, `examples`, …) | Not a constraint; produces **zero** deltas |

### Why removing a workflow type is *compatible* here

Deleting a `#[workflow]` handler while executions are still in flight is a real
hazard — but this gate is the wrong instrument for it. It cannot see how many
non-terminal executions of that type exist.
[`harvest workflow-types reachability`](runbooks/safe-handler-removal.md)
([issue #520](https://github.com/autumn-foundation/autumn-harvest/issues/520))
answers that question **exactly**, by counting live runs. Double-gating would
mean every legitimate handler removal needs a schema acknowledgement it can
learn nothing from — pure acknowledgement fatigue. So this gate defers, and says
so in the delta's reason.

### Fail-closed on unanalysed keywords

The differ reasons about a fixed keyword vocabulary:

```
type  required  properties  enum  items  additionalProperties
allOf  anyOf  oneOf  minLength  maxLength  minimum  maximum
minItems  maxItems  format  $ref
```

That is a deliberate **superset** of what the engine's own start-time validator
(`autumn_harvest::info::validate_against_schema`) enforces — it covers
`minItems`/`maxItems` and numeric `format`, which the validator ignores. Erring
wide is the safe direction for a gate: at worst it asks for an acknowledgement
on a change that would not actually have broken anything.

Any **other** constraint keyword (`pattern`, `patternProperties`, `if`/`then`,
`not`, `const`, `dependencies`, …) is reported **breaking on any change —
added, modified, *or removed***.

Removing a constraint is not reliably a loosening. Dropping a
`patternProperties` entry while `additionalProperties: false` remains *tightens*
the schema: keys the pattern used to admit become "additional" and are now
rejected. The differ does not model keyword interactions, so it refuses to
guess and lets you acknowledge.

### Limits the gate does not cover

Two are worth knowing before you rely on it:

- **It cannot see behind an unresolvable `$ref`.** A *change* to such a
  reference is reported breaking, but if both revisions point at the same
  external document and the *document* changes, that is outside this gate's
  reach. Keep published schemas self-contained — `schemars` output already is.
- **`oneOf` disjointness rests on serde's own serializer.** A variant branch is
  treated as disjoint when it carries a tag, a unit `enum`, or the
  single-property shape serde emits for an externally-tagged enum. That is
  sound because a payload already in `harvest_events` was written by serde's
  `Serialize`, which emits exactly one variant key. A hand-written schema that
  invents its own `oneOf` shape may not satisfy that assumption — but it will
  fall into the "cannot tell apart" row above and fail closed rather than pass.

### Annotations never dirty the file

`schemars` copies your doc comments into `description`. Storing them would mean
every doc edit produced a spurious diff, and the baseline would rot until
everyone stopped reading it.

Stored schemas are therefore **canonicalised**: annotations (`title`,
`description`, `examples`, `default`, `$schema`, non-numeric `format`, …) are
stripped, `enum` arrays are sorted, and the two `schemars` unit-enum forms are
normalised to one. Both sides are canonicalised again at diff time, so a
doc-comment edit yields both zero deltas *and* a byte-identical artifact.

---

## Exit-code contract

| Code | Meaning | stdout |
|---|---|---|
| `0` | Every delta is compatible (including "no deltas at all") | the diff |
| `1` | At least one delta is breaking | the diff, **then** the error on stderr |
| `1` | The check could not run: a baseline/current file is missing, unreadable, or malformed | *(nothing)* — error on stderr |
| `1` | The diff was **truncated** at the delta cap, so it cannot certify anything | the partial diff, then the error |

`1` is deliberately overloaded: from CI's point of view "breaking change" and
"the gate could not run" are the same answer — *do not merge*. Distinguish them
by stderr, or by `--format json` (which prints a parseable diff only in the
first two rows).

The diff is printed to stdout **before** the non-zero exit whenever one exists,
so CI logs are self-explanatory. A read/parse failure has no diff to print.

### Why truncation fails

The differ caps how many deltas it will enumerate. Past that cap the report is
incomplete, so exiting `0` would assert something the tool cannot know. It fails
closed instead: exit `0` always means *nothing breaking*, never *nothing
breaking that fit*.

### Output formats

`--format text` (default) prints one line per delta:

```
<workflow>.<role>: <field_path> — <verdict> — <reason>
```

followed by a summary line. `--format json` emits the machine-readable diff:

```json
{
  "deltas": [
    {
      "workflow": "onboarding",
      "role": "input",
      "field_path": "/email_address",
      "change": "required_property_added",
      "verdict": "breaking",
      "reason": "required property `email_address` added; …"
    }
  ],
  "breaking_count": 1,
  "compatible_count": 0
}
```

`truncated: true` is added only when the delta cap was hit.

`field_path` is an RFC 6901 JSON Pointer **for ordinary object properties**,
matching the violation format `POST /workflows/{name}/start` already returns —
so `/email_address` and `/address/city` can be fed straight to a pointer
library.

It is a *locator*, not a guaranteed pointer, in four cases where the difference
is not at a concrete instance path. These use deliberately pointer-invalid
syntax so they can never be mistaken for a real key:

| Form | Means |
|---|---|
| `""` (empty) | The schema as a whole — e.g. it was published or withdrawn |
| `/[]` | The array's `items` schema (every element) |
| `/[0]`, `/[1]`, … | A specific tuple position |
| `/{*}` | The map's `additionalProperties` value schema |
| `/<variant:Paid>` | An externally tagged `oneOf`/`anyOf` branch |
| `/<tag:kind="paid">` | An internally tagged branch |
| `/<unit:"Cancelled">` | A unit (fieldless) enum branch |

Ordinary property names *are* escaped per RFC 6901 (`/` → `~1`, `~` → `~0`), so
a field literally named `a/b` renders as `/a~1b`.

---

## Acknowledging a deliberate breaking change

Sometimes a breaking change is correct: you have drained the in-flight
executions, pinned old runs to an old build with
[build-id routing](runbooks/safe-deploy.md)
([issue #171](https://github.com/autumn-foundation/autumn-harvest/issues/171)),
or reset them ([issue #148](https://github.com/autumn-foundation/autumn-harvest/issues/148)).

The gate must not block you — but the acknowledgement must never be silent.

```console
$ harvest schema update \
    --current /tmp/current.json \
    --acknowledge "GDPR rename; in-flight runs drained via reset (#148)" \
    --recorded-in docs/changelog.d/pr-794-schema-contract-gate.md
schema baseline updated: 2 delta(s) recorded, 2 acknowledged as breaking
```

This rewrites the baseline **and** appends an audit record per absorbed delta:

```json
"acknowledged_breaking_changes": [
  {
    "workflow": "onboarding",
    "role": "input",
    "field_path": "/email",
    "change": "property_removed",
    "reason": "GDPR rename; in-flight runs drained via reset (#148)",
    "recorded_in": "docs/changelog.d/pr-794-schema-contract-gate.md"
  }
]
```

Both the schema change and the justification land in the same reviewable git
diff. A reviewer sees *what* changed, *why* it was safe, and *where* the
migration is written up.

Guardrails:

- `update` **refuses** a breaking delta without `--acknowledge`, and names the
  flag in the error.
- A blank or whitespace-only `--acknowledge` is refused: a rubber stamp is not
  an acknowledgement.
- On refusal the baseline is left **untouched**.
- A purely compatible change needs no acknowledgement — just re-run `update`.
- The audit log is **append-only**: prior acknowledgements are carried forward,
  so the record of past migrations is never quietly erased.

---

## Recipes

### GitHub Actions

```yaml
  schema-gate:
    name: Workflow schema contract
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - name: Generate the currently published schemas
        run: cargo run --quiet --bin dump-schema-contract > /tmp/current.json
      - name: Gate backward-incompatible payload changes
        run: |
          cargo run --quiet -p autumn-harvest-cli --bin harvest -- \
            schema check --current /tmp/current.json
```

`schema check` needs no database and no services, so it belongs in the same
cheap lint job as `det-check`.

### Pre-commit hook

```sh
#!/bin/sh
# Block commits that break replay of in-flight executions.
cargo run -q --bin dump-schema-contract > /tmp/harvest-schema-current.json || exit 1
if ! cargo run -q -p autumn-harvest-cli --bin harvest -- \
        schema check --current /tmp/harvest-schema-current.json; then
    echo "A workflow payload schema changed incompatibly." >&2
    echo "Fix it, or acknowledge a deliberate migration with:" >&2
    echo "  harvest schema update --current … --acknowledge \"<why this is safe>\"" >&2
    exit 1
fi
```

---

## Regression tests

Three guards keep this artifact honest, all in
`autumn-harvest/tests/integration/workflow_schema_contract_tests.rs`, which
pulls the file in with `include_str!` so a hand-edit cannot slip past:

| Test | Guards |
|---|---|
| `the_checked_in_baseline_parses_and_is_self_consistent` | It parses, and `contract_version` matches `SCHEMA_CONTRACT_VERSION` |
| `the_checked_in_baseline_workflows_are_sorted_and_unique` | Stable ordering, no duplicate entries |
| `the_checked_in_baseline_schemas_are_canonicalised` | No annotations were committed, so doc edits stay no-ops |

Plus the CI step **`Gate workflow payload-schema changes (issue #794)`**, which
regenerates the contract from the example registry and diffs it against the
checked-in file — proving both that the artifact is current and that the
documented recipe runs.

### When you change a workflow payload type

1. Change the type.
2. Regenerate `--current` (see above).
3. Run `harvest schema check`. If it passes, run `harvest schema update` and
   commit the artifact with your change.
4. If it fails and the change is deliberate, re-run `update` with
   `--acknowledge "<why it is safe>"` and `--recorded-in <ref>`.
5. Commit the artifact **in the same PR** — the schema change and its
   justification belong in one reviewable diff.

> **Note.** `docs/workflow-schema-contract.json` lives under `docs/`, and CI
> treats a docs-only PR as needing no code jobs. A PR that touches *only* this
> artifact therefore skips the gate and these tests. That matches how
> `docs/api-contract.json` already behaves, but this file is the gate's trusted
> *input*, so review artifact-only edits on their merits rather than assuming
> CI vetted them.

---

## Scope

**In scope:** the `input_schema`, `output_schema`, and `error_schema` published
by registered `#[workflow]` types (issue #373).

**Out of scope**, deliberately:

- **Signal / query / update payload schemas.** These *are* published — issue
  [#610](https://github.com/autumn-foundation/autumn-harvest/issues/610) shipped
  them on `GET /workflows/registered/{name}/interface` — but this gate does not
  diff them yet. Their payloads are recorded too (`SignalReceived`,
  `UpdateAdmitted`), so the same hazard applies; extending the artifact with
  those roles is the obvious follow-up.
- **Activity schemas** — an activity's recorded input/output has the same
  hazard, but no published-schema surface exists for it yet.
- **Runtime enforcement / per-build pinning** — [issue #171](https://github.com/autumn-foundation/autumn-harvest/issues/171).
- **Auto-migration or upcasting** of recorded payloads.
- **Non-deterministic *code*** — [`harvest det-check`](workflow-determinism-guide.md#running-the-check-in-ci)
  ([#386](https://github.com/autumn-foundation/autumn-harvest/issues/386),
  [#778](https://github.com/autumn-foundation/autumn-harvest/issues/778)).

A workflow that publishes **no** schema is not gated at all — the check is
opt-in, exactly like #373 itself. `coverage` in the artifact makes that visible
at a glance, and *withdrawing* an already-published schema is breaking, so
coverage cannot silently regress.

---

## Where this sits in the release playbook

The schema gate is one layer of replay defence-in-depth, not the whole of it:

1. **`harvest det-check`** — non-deterministic *code* reachable from workflow bodies.
2. **`harvest schema check`** *(this gate)* — incompatible *payload* changes.
3. **`WorkflowReplayer` / `harvest-replay`** — replay real exported histories against the new code.
4. **`ctx.patched()` / `ctx.version()`** — fence a deliberate logic change across deploys.
5. **Build-id routing (#171)** — pin in-flight executions to the old build during a breaking migration.
6. **`harvest workflow-types reachability` (#520)** — confirm a handler is safe to delete.

Layers 1 and 2 are static and cheap enough for every PR. Layer 3 is the
authoritative check, but it needs a recorded history to run against — which is
exactly why the static gates exist.
