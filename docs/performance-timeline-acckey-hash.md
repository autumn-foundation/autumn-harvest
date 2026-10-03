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

## Reproduce

```text
cargo build --release -p autumn-harvest --no-default-features --features testing --bench timeline_profile
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out \
  target/release/deps/timeline_profile-*
callgrind_annotate --inclusive=yes cg.out
```
