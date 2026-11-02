# Schema contract diff: skip the copy of an already canonical schema

Issue: none. Found by profiling the CI schema gate.

## Workload

`diff_schema_contracts` runs in `harvest schema check` and the CI schema gate.
The harness `autumn-harvest/benches/schema_contract_diff_profile.rs` builds two
contracts of 200 workflows and 400 activities. The schemas have the stored
(canonical) form of a real contract: `definitions`, `$ref`, `oneOf`, enums.
One entry in five is edited.

## Profile (baseline, callgrind, `diff_schema_contracts` only)

| Cost | Share of diff |
| --- | --- |
| `diff_node` walk (1,400 calls) | 68.7% |
| `diff_role` → `canonicalize_node` (2,800 calls) | 20.3% |
| freeing the canonicalised copies | 10.7% |

`diff_role` canonicalised both sides of every pair. Stored contracts are
already canonical, so each call made a deep copy that equalled its input.

## Change

`canonicalize_node` returns `Option<Value>`. `None` means the canonical form
equals the input. `diff_role` borrows the input in that case. A node copies
only when an annotation, an unsorted `enum`, or a `oneOf`/`anyOf`/`allOf`
rewrite changes it. `canonicalize_schema` keeps its signature and result.

## Measurement

Tool: `valgrind --tool=callgrind --toggle-collect='*diff_schema_contracts*'`
and `valgrind --tool=dhat`. Same machine, same session.

| Counter | Before | After | Delta |
| --- | --- | --- | --- |
| Instructions (200 wf, 400 act) | 610,425,043 | 528,244,470 | -13.5% |
| Instructions (100 wf, 200 act) | 306,390,199 | 257,593,284 | -15.9% |
| Instructions (50 wf, 100 act) | 154,451,223 | 130,327,677 | -15.6% |
| Heap bytes allocated | 127,661,631 | 111,239,031 | -12.9% |
| Heap blocks allocated | 1,080,118 | 1,044,438 | -3.3% |

Heap figures are the dhat total minus a `SCHEMA_DIFF_BUILD_ONLY=1` run.

Correctness: 200,000 random schemas, plus `docs/workflow-schema-contract.json`,
gave identical output from the old and new `canonicalize_schema`. The 281
schema-contract integration tests pass unchanged.

## Reproduce

```sh
CARGO_PROFILE_RELEASE_DEBUG=1 cargo build --release -p autumn-harvest \
  --bench schema_contract_diff_profile
valgrind --tool=callgrind --toggle-collect='*diff_schema_contracts*' \
  target/release/deps/schema_contract_diff_profile-<hash>
```
