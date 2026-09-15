# `shard_rebalance::history_fingerprint` — a per-event canonicalization buffer

Wall-clock timing is not admissible evidence on this (shared-vCPU) machine —
every number below is a deterministic instruction count
(`valgrind --tool=callgrind`) or allocation count/bytes
(`valgrind --tool=dhat`).

## 🎯 Workload

`history_fingerprint` is the replay-determinism check
`shard_rebalance::db::verify_target_copy` runs on both the source and the
target side of a shard migration (`docs/sharding.md`): it hashes a decoded
event history down to one string an operator can compare, so a migration can
say not only *that* the copy verified but *what* it agreed on.

The harness is `autumn-harvest/benches/history_fingerprint_profile.rs`,
newly added by this change. It reuses `replay_profile_support.rs`'s
`build_history` — the exact issue #135 history shape (`ActivityScheduled`/
`ActivityCompleted` pairs, each carrying a ~230-byte realistic JSON payload)
`replay_profile.rs` already profiles `WorkflowReplayer` against — and calls
`history_fingerprint` on the resulting 10,001-event history
(`HFP_PROFILE_N=5000`, matching `replay_profile.rs`'s default). A migrated
execution's real history can be this large or larger: nothing in
`verify_target_copy`'s call path caps it, and a long-lived continue-as-new
chain (see `run_chain_profile.rs`'s doc comment) or a wide fan-out workflow
routinely carries thousands of events.

```bash
BIN=$(cargo bench -p autumn-harvest --no-default-features --features testing \
  --bench history_fingerprint_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.executable != null) | .executable')
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --threshold=98 cg.out
valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
```

## 📈 Profile

Flat profile, pre-fix (`docs/perf-artifacts/history-fingerprint/before-callgrind-flat.txt`):

```
461,455,374 (100.0%)  PROGRAM TOTALS

175,397,730 (38.01%)  sha2::sha256::compress256
 61,305,728 (13.29%)  _int_malloc
 32,952,131 ( 7.14%)  _int_free
 28,994,362 ( 6.28%)  serde_json::ser::format_escaped_str_contents
 20,205,581 ( 4.38%)  malloc
 13,680,052 ( 2.96%)  alloc::collections::btree::map::IntoIter<K,V,A>::dying_next
 13,020,380 ( 2.82%)  free
 11,037,767 ( 2.39%)  __memcpy_avx_unaligned_erms
  8,745,000 ( 1.90%)  <alloc::string::String as core::clone::Clone>::clone
  ...
  2,815,082 ( 0.61%)  history_fingerprint_profile::support::build_history  <- one-time history build
```

`build_history` — the harness's one-time input construction, not the
target — is 0.61% of the profile; `history_fingerprint` itself, and
everything it calls (the SHA-256 hashing, the JSON canonicalization, and
`HistoryMatcher::new`'s bookkeeping), accounts for the other 99.39%. That
clears the ≥5%-of-workload gate by nearly 20×, whichever way the split is
drawn.

Two things stand out in the flat profile:

* **`sha2::sha256::compress256` (38.01%) is inherent.** Hashing the
  canonicalized history is the function's job; nothing here reduces how
  many bytes get hashed.
* **`_int_malloc`/`_int_free`/`malloc`/`free` together are ~27.6%** — more
  instructions than the JSON-escaping work that produces the bytes those
  allocations hold (`format_escaped_str_contents`, 6.28%). That is the
  signal: step 1 of `history_fingerprint` calls `serde_json::to_string`
  once per event, allocating and freeing one fresh `String` per iteration,
  purely to copy its bytes into the hasher and then discard it:

  ```rust
  for event in events {
      let canonical = serde_json::to_string(event)
          .unwrap_or_else(|e| format!("<unserializable event: {e}>"));
      hasher.update(canonical.as_bytes());
      hasher.update([0u8]);
  }
  ```

  10,001 events means 10,001 allocate-serialize-copy-free cycles for a
  buffer whose contents are never read after the `hasher.update` call two
  lines later.

## 💡 Hypothesis

The per-event `String` buffer can be hoisted out of the loop and reused:
`serde_json::to_writer` emits the exact same bytes `serde_json::to_string`
does (same `Serializer`, same `Formatter`, just a different `io::Write`
sink), so writing into a `Vec<u8>` that gets `clear()`-ed and reused every
iteration produces an identical byte stream to hash, at the cost of a
handful of capacity-growing reallocations total instead of one alloc (and
one matching free) per event.

## 🔧 Change

`autumn-harvest/src/shard_rebalance.rs`, `history_fingerprint`, step 1 only:

* One `Vec<u8>` (`canonical`) is allocated before the loop.
* Each iteration `clear()`s it, `serde_json::to_writer`s the event into it,
  and hashes `&canonical` instead of a freshly allocated `String`'s bytes.
* The same fallback text (`<unserializable event: {e}>`) is written into
  the same buffer on a serialize error, so the hashed bytes are identical
  to before on both the happy path and the (practically unreachable, and
  already-defensive) error path.

No other part of the function changes: the cursor-accessor half (step 2)
and the final `format!("{:x}", ...)` are untouched.

**No behavior change.** `serde_json::to_writer` and `serde_json::to_string`
share the same serializer, so byte-for-byte identical input produces
byte-for-byte identical hashed bytes; the fingerprint value cannot change.
All 55 `shard_rebalance_unit::*` tests — including the six
`the_fingerprint_*` tests that pin exact fingerprint (in)equality —
pass unmodified, and so does `shard_rebalance_db_tests::*`, which exercises
`history_fingerprint` through a real `verify_target_copy` call against
Postgres.

## 📊 Measurement

Same harness, same machine and session, differing only by the diff above.

### Allocations (`valgrind --tool=dhat`) — the qualifying metric

| dhat | Before | After | Δ |
|---|---|---|---|
| Total blocks | 490,015 | 460,021 | -29,994 (**-6.12%**) |
| Total bytes  | 54,459,322 | 45,500,210 | -8,959,112 (**-16.45%**) |

Bytes allocated drops **-16.45%**, clearing the ≥10%-reduction-in-bytes
floor. Repeated three times post-fix: identical `45,500,210` bytes /
`460,021` blocks every run — dhat's byte/block counters carry no
per-process randomness, unlike the instruction count below.

### Instructions (Ir), `valgrind --tool=callgrind --branch-sim=no --cache-sim=no`

| | Instructions (Ir) |
|---|---|
| Before | 461,455,374 |
| After  | 451,624,485 |
| **Reduction** | **9,830,889 (2.13%)** |

Reported for completeness; **this is not the qualifying metric** — a 2.13%
instruction reduction is below this project's 5% floor on its own. It is
directionally consistent with the allocation win (fewer `malloc`/`free`
calls cost fewer instructions too, just not enough of them here — SHA-256
compression dominates the instruction count and is untouched). Re-run twice
more post-fix to bound noise: `451,624,503` and `451,624,503` — a
`0.000004%` spread, five orders of magnitude below the measured delta.
`build_history`'s own self-cost is byte-for-byte identical before and after
(`2,815,082` both times), confirming the change touches nothing outside
`history_fingerprint`.

### Correctness

* `cargo fmt -p autumn-harvest -- --check` — clean.
* `cargo test -p autumn-harvest --no-default-features --test integration -- shard_rebalance` —
  **55 passed, 0 failed**, including all six `the_fingerprint_*` tests.
* `cargo test -p autumn-harvest --features db,testing --test integration -- shard_rebalance_db_tests` —
  passed against a local Postgres 16, including the `history_fingerprint`
  equality assertion inside a real `verify_target_copy` round trip.
* `cargo clippy` could not be run in this sandbox: the installed clippy
  (0.1.94) predates a `clippy::unused_async_trait_impl` reference already in
  `context.rs` (unrelated pre-existing code, not touched by this change) and
  fails on `unknown-lints` before reaching this diff.
* `python3 docs/audits/comment-hygiene.py --base origin/trunk-dev` — see PR.

## 🔬 Reproduce

```bash
BIN=$(cargo bench -p autumn-harvest --no-default-features --features testing \
  --bench history_fingerprint_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.executable != null) | .executable')

# Allocations (the qualifying metric):
valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"

# Instructions (reported for completeness):
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --threshold=98 cg.out | head -10
```

Full artifacts: `docs/perf-artifacts/history-fingerprint/{before,after}-callgrind-flat.txt`,
`{before,after}-dhat.json`.
