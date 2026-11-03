# `project_awaitables` history index: hasher cost

Issue: none. Harness: `autumn-harvest/benches/awaitables_profile.rs`
(`N=400`, `REPS=1500`, 3000 `project_awaitables` calls).

## Baseline (commit 4b6ec95, release build)

| Tool | Metric | Value |
|---|---|---|
| callgrind | instructions | 2,727,202,632 |
| dhat | bytes allocated | 296,304,973 |
| dhat | blocks allocated | 2,608,778 |

`sip::Hasher::write` (14.2%) and `RandomState::hash_one` (13.1%) sum to
27.3% of the instructions. The keys are 16-byte UUID newtypes.

## After: `foldhash::fast::RandomState` on the UUID-keyed index maps

| Tool | Metric | Before | After | Delta |
|---|---|---|---|---|
| callgrind | instructions | 2,727,202,632 | 2,057,342,956 / 2,058,057,109 (2 runs) | -24.5% |
| dhat | bytes allocated | 296,304,973 | 296,304,974 | 0 |
| dhat | blocks allocated | 2,608,778 | 2,608,779 | 0 |

The map keeps a random per-map seed, so it keeps the same flooding
protection class as `RandomState`. Callgrind varies about 0.03% between runs.

Reproduce:

```sh
cargo build --release --features testing -p autumn-harvest --bench awaitables_profile
valgrind --tool=callgrind target/release/deps/awaitables_profile-<hash>
```
