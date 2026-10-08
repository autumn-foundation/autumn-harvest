# Schema validation: kind-gated keyword lookups

`validate_node` looked up `required`, `properties`, `items` and
`additionalProperties` in the schema map before it checked the value's kind.
Leaf values are the common case, so most of those lookups failed their kind
test and were thrown away. Each lookup is a `BTreeMap` key comparison, which
shows up as `memcmp`. Wall-clock time is not admissible on this machine; all
numbers are `valgrind --tool=callgrind` instruction counts.

## Workload

`benches/schema_validate_profile.rs`, unchanged. It validates a valid
order-checkout payload against its schema 5,000 times.

## Profile (before)

- `validate_node` (self, recursive): 54.71%
- `__memcmp_avx2_movbe` (map key compares): 34.70%
- `validate_node` (outer): 3.83%
- `Index::index_into`: 3.62%

## Change

Check `value.as_object()` or `value.as_array()` first. Look up the keyword
only when the value has the matching kind. Behavior is unchanged.

## Measurement

| Counter | Before | After | Delta |
| --- | --- | --- | --- |
| Ir (callgrind) | 312,534,544 | 253,774,544 | -18.80% |
| `__memcmp_avx2_movbe` Ir | 108,462,315 | 83,822,315 | -22.7% |

Raw output: `docs/perf-artifacts/schema-validate-kind-gated-lookups/`.

## Reproduce

```bash
cargo build --profile bench -p autumn-harvest --bench schema_validate_profile
valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
  --callgrind-out-file=cg.out target/release/deps/schema_validate_profile-*
```
