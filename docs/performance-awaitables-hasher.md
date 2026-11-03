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
