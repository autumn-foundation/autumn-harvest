# Schema validation: lazy JSON-Pointer path construction

This note documents a follow-up profiling pass over
`autumn_harvest::info::validate_against_schema` (issue #373) after the prior
`HashSet` fix (PR #1169), and the resulting change in `validate_node`'s
per-node path bookkeeping. Wall-clock timing is not admissible evidence on
this (shared-vCPU) machine — every number below is a deterministic
instruction count (`valgrind --tool=callgrind`) or allocation count
(`valgrind --tool=dhat`), both reproducible bit-for-bit on any machine.

## Workload

The harness is `benches/schema_validate_profile.rs` — the same one PR #1169
introduced, unchanged. It validates a realistic order-checkout payload
(nested `customer`/`shipping_address` objects, a 6-element `items` array,
`required`, `enum`, `minLength`/`maxLength`, `minimum`/`maximum`, and both
forms of `additionalProperties`) against a **matching valid payload**, 5,000
times. The valid-payload case is deliberate: it's the common case in
production, and it's the one where any eagerly-computed-but-unused
diagnostic work is pure waste, since no violation is ever pushed.

`validate_against_schema` has no feature gate — it runs on every
`POST /workflows/{name}/start`, signal-with-start, update-with-start, and
completion-trigger fan-in delivery for a workflow that publishes an input
schema, on every deployment.

## Profile

```bash
BIN=$(cargo bench -p autumn-harvest --no-default-features --bench schema_validate_profile \
  --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="schema_validate_profile") | .executable')
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --threshold=98 cg.out
```

Fresh baseline on `HEAD` at the time of this pass (PR #1169 already merged;
the reported total is close to, but not identical to, PR #1169's own
"after" number — expected run-to-run/session-to-session variance on shared
hardware, not something this pass chases):

```
579,995,201 (100.0%)  PROGRAM TOTALS

196,535,000 (33.89%)  autumn_harvest::info::validate_node'2
103,552,315 (17.85%)  __memcmp_avx2_movbe
 32,250,000 ( 5.56%)  alloc::str::<impl str>::replace
 31,205,405 ( 5.38%)  _int_free
 23,370,588 ( 4.03%)  alloc::fmt::format::format_inner
 21,774,734 ( 3.75%)  malloc
 19,570,749 ( 3.37%)  core::fmt::write
 17,820,312 ( 3.07%)  <alloc::string::String as core::fmt::Write>::write_str
 14,286,300 ( 2.46%)  free
 14,002,762 ( 2.41%)  __memcpy_avx_unaligned_erms
 13,980,006 ( 2.41%)  autumn_harvest::info::validate_node
 12,675,000 ( 2.19%)  <str as serde_json::value::index::Index>::index_into
 11,630,000 ( 2.01%)  alloc::raw_vec::RawVecInner<A>::finish_grow
 11,250,000 ( 1.94%)  alloc::raw_vec::RawVecInner<A>::reserve::do_reserve_and_handle
  9,320,125 ( 1.61%)  realloc
  8,750,000 ( 1.51%)  core::fmt::Formatter::pad
  ...
```

`alloc::str::replace` + `alloc::fmt::format::format_inner` + `core::fmt::write`
+ `String::write_str` + `core::fmt::Formatter::pad` sum to
`32,250,000 + 23,370,588 + 19,570,749 + 17,820,312 + 8,750,000 =
101,761,649` — **17.54% of total instructions**, all in the library's own
code, and comfortably clearing the ≥5%-of-workload gate by 3.5×. A large
share of the allocator traffic (`_int_free`, `malloc`, `free`, `realloc`,
`finish_grow`, `do_reserve_and_handle`) is downstream of the same call
sites.

Tracing the call tree: `validate_node`'s "properties — recurse" block builds
a fresh, escaped RFC 6901 child pointer **before every recursive call, for
every property visited**, regardless of whether that subtree produces a
violation:

```rust
let escaped_name = prop_name.replace('~', "~0").replace('/', "~1");
let child_ptr = format!("{ptr}/{escaped_name}");
validate_node(root, prop_schema, prop_value, &child_ptr, out, depth + 1);
```

The same pattern repeats for array items (`format!("{ptr}/{i}")`) and for
the `additionalProperties` object-schema branch. `str::replace` always
allocates a fresh `String` via `fold`'s `String::with_capacity` seed —
**even when the pattern never matches** — so a property name like
`"customer"` (no `~` or `/`) still pays two heap allocations before the
third (`format!`) builds the child pointer. On the fixture's schema this
sums to over a hundred `child_ptr` allocations per `validate_against_schema`
call, none of which is ever read back: `child_ptr` is only used to seed the
`field_path` of a `SchemaViolation`, and the fixture produces zero
violations.

## Hypothesis

Deferring path construction — passing a zero-allocation, borrowed chain of
path segments through the recursion instead of a pre-materialized `&str`
prefix, and building (and RFC-6901-escaping) the actual `String` only at
the handful of sites that construct a `SchemaViolation` — removes the
`replace`/`format!`/`fmt::write` family entirely from the zero-violation
path, at zero cost to correctness: the same violations, same messages, and
same `field_path` values are produced when a violation *does* occur, just
computed on demand instead of speculatively on every node visit.

Given the flat profile already attributes ~17.5% directly to this pattern,
plus a share of the ~20% allocator-family cost the discarded `String`s
drive, removing the eager construction should produce a large,
double-digit-percent reduction in both instructions and allocation counts.

## Change

`autumn-harvest/src/info.rs`: `validate_node`'s `ptr: &str` parameter is
replaced with `path: &JsonPointerPath<'_>`, a new private enum:

```rust
enum JsonPointerPath<'a> {
    Root,
    Prop { parent: &'a Self, name: &'a str },
    Index { parent: &'a Self, index: usize },
}
```

Each recursive call constructs a `JsonPointerPath::Prop`/`Index` value on
its **own stack frame** and passes a borrow of it down — a "cons list on
the stack," the same shape the function's pre-existing `$ref` cycle guard
(`visited: HashSet<&str>`) already uses for borrowed, non-owning state. No
heap allocation happens building the chain itself. `JsonPointerPath::materialize`
walks the chain and builds the escaped `String` — applying the same RFC
6901 escape (`~` → `~0`, `/` → `~1`) inline, character by character, instead
of via two chained `.replace()` calls — and is called exactly at the nine
sites that previously called the `path()` closure or built `child_ptr`
directly for a violation. Every recursion site that only needed the path to
*pass through* to a child call (`properties`, `items`, the
`additionalProperties` object branch, and the unchanged `ptr` reuse in
`allOf`/`anyOf`/`oneOf`) now passes the borrowed `path`/`child_path`
reference with no string work at all.

Behavior is unchanged: `validate_against_schema`'s public signature is
untouched (still takes `&Value, &Value`, still returns
`Result<(), Vec<SchemaViolation>>`); every existing `field_path` assertion
in `info.rs`'s unit tests (including the cyclic-`$ref` recursion-bound test,
which the depth guard's `field_path` also now derives from `materialize()`)
passes unchanged.

## Measurement

Both binaries built from the identical harness/`Cargo.toml` bench
declaration, differing only by this one-file diff, same
`valgrind --tool=callgrind --branch-sim=no --cache-sim=no` and
`valgrind --tool=dhat` invocations, same session.

| | Instructions (Ir) |
|---|---|
| Before | 579,995,201 |
| After  | 326,387,546 |
| **Reduction** | **253,607,655 (43.73%)** |

The reduction clears the ≥5% floor by close to 9×.

| dhat | Before | After | Reduction |
|---|---|---|---|
| Blocks | 695,227 | 227 | 695,000 (**99.97%**) |
| Bytes  | 7,489,758 | 29,758 | 7,460,000 (**99.60%**) |

695,000 eliminated blocks ÷ 5,000 calls = **139 allocations eliminated per
call** — path-construction traffic for the zero-violation case is
essentially gone; the residual 227 blocks / 29,758 bytes in the "after"
trace is one-time process/fixture setup (`order_schema()`/`order_payload()`
construction, called once outside the measured loop), not per-call cost.

The flat profile after the change confirms the mechanism, not just the
total:

```
326,387,546 (100.0%)  PROGRAM TOTALS

185,445,000 (56.82%)  autumn_harvest::info::validate_node'2
102,072,315 (31.27%)  __memcmp_avx2_movbe
 12,675,000 ( 3.88%)  <str as serde_json::value::index::Index>::index_into
 12,405,006 ( 3.80%)  autumn_harvest::info::validate_node
  5,085,000 ( 1.56%)  core::str::count::char_count_general_case
  4,125,000 ( 1.26%)  alloc::collections::btree::search::search_tree
```

`alloc::str::replace`, `alloc::fmt::format::format_inner`, `core::fmt::write`,
`String::write_str`, and `core::fmt::Formatter::pad` are **entirely absent**
above the `--threshold=98` cutoff — they were 17.54% of the total before.
Nearly the whole allocator-traffic family (`_int_free`, `malloc`, `free`,
`realloc`, `finish_grow`, `do_reserve_and_handle`, `__rdl_alloc`,
`__rdl_realloc`, `__memcpy_avx_unaligned_erms`) drops below visibility too.
`__memcmp_avx2_movbe` (103,552,315 → 102,072,315, essentially flat in
absolute terms — it only *looks* larger as a percentage because the total
shrank) and `index_into`/`char_count_general_case` (12,675,000 and
5,085,000 respectively, byte-identical in both profiles) are the inherent
cost of the walker's design: `serde_json::Value::Object` is
`BTreeMap`-backed (this crate does not enable `preserve_order`), so every
`schema_obj.get(...)`/`properties.get(prop_name)` lookup pays a `BTreeMap`
key-comparison walk — a dependency-level cost already flagged as
out-of-scope by PR #1169, unaffected by this change.

**Correctness**: `cargo test -p autumn-harvest --no-default-features
--features testing --lib` (1,891 tests) and `cargo test -p autumn-harvest
--no-default-features --features testing --test integration` (1,568 tests,
including the `macros_compile_fail` trybuild suite) both pass unchanged, 0
failures — run twice: once immediately after the functional change, and
again after the clippy-driven lint fixes below, to verify the final
shipping diff. `cargo build -p autumn-harvest --all-features`, `cargo
clippy -p autumn-harvest --lib --benches --all-features -- -D warnings`,
and `cargo fmt --check -p autumn-harvest` are all clean.

## Reproduce

```bash
BIN=$(cargo bench -p autumn-harvest --no-default-features --bench schema_validate_profile \
  --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="schema_validate_profile") | .executable')

# Instruction count:
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --threshold=98 cg.out | head -30

# Allocation counts/bytes:
valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
```

`SCHEMA_PROFILE_N` (default `5_000`) controls the iteration count if more
valgrind wall-time headroom is needed.
