# `derive_timeline`: `AccKey` hashing

Wall-clock timing is not admissible on this machine. Every number below is a
`valgrind --tool=callgrind` instruction count.

## Workload

`GET /workflows/{id}/timeline` calls `derive_timeline` once per request over the
full event history. `autumn-harvest/benches/timeline_profile.rs` drives that
entry point with `TIMELINE_PROFILE_N=400` activities and 1,500 repetitions.

## Profile (baseline, commit before the change)

Total: 2,550,943,985 instructions (second run: 2,546,829,703; spread 0.16%).

| Inclusive cost | Item |
| --- | --- |
| 89.37% | `derive_timeline` |
| 32.86% | `RandomState::hash_one::<&AccKey>` |
| 19.54% | `sip::Hasher::write` (self) |
| 14.59% | `RawTable<(AccKey, usize)>::reserve_rehash` (re-hashes every key) |

All 2,353,568 `hash_one::<&AccKey>` calls come from `derive_timeline`.

## Hypothesis

The derived `Hash` for `AccKey` makes three `SipHash` `write` calls per key:
the variant tag, the slice length prefix of the 16-byte UUID, and the bytes.
One 128-bit write with the tag folded into the id does the same job with one
absorb. Equality still separates namespaces, so behavior is unchanged. The
hasher stays the keyed `RandomState` one, so hash-flooding resistance is kept.
No new crate is needed.

## Change

A manual `Hash` impl for `AccKey` in `autumn-harvest/src/timeline.rs`.

## Measurement (callgrind, same harness, same session)

| Run | Instructions |
| --- | --- |
| Before, run 1 | 2,550,943,985 |
| Before, run 2 | 2,546,829,703 |
| After, run 1 | 2,340,195,633 |
| After, run 2 | 2,334,957,998 |

Delta: -8.3% (run 1 against run 1), -8.3% (mean against mean).

| Inclusive item | Before | After |
| --- | --- | --- |
| `hash_one::<&AccKey>` | 838,188,420 (32.86%) | 632,182,174 (27.01%) |
| `sip::Hasher::write` | 498,500,112 (19.54%, self) | 295,414,812 (12.62%) |

`derive_timeline` is 89% of the harness, so the delta clears the 5% floor on a
benchmark that is the whole workload. The 32 `timeline::tests` cases pass
unchanged.

## Remaining cost

`hash_one::<&AccKey>` is still 27%. `reserve_rehash` is 12.7% because the map
grows without a size hint. A pre-size needs a count of scheduling events, so it
is left for a separate measurement.

## Reproduce

```text
cargo build --release -p autumn-harvest --no-default-features --features testing --bench timeline_profile
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out \
  target/release/deps/timeline_profile-*
callgrind_annotate --inclusive=yes cg.out
```
