# Spike: does the kache compiler cache cut our compile times?

> **Status: measured, not adopted.** One machine, one workload, one sample per
> cell except where noted. The note records what was run so that a later trial
> starts from these numbers instead of repeating them.

Measured 2026-11-04 on `trunk-dev` `429cd9a`.

## 🎯 Question

[kache](https://github.com/kunobi-ninja/kache) is a `RUSTC_WRAPPER` compiler
cache with a GitHub Actions integration,
[kache-action](https://github.com/kunobi-ninja/kache-action). Does it make
local builds or CI builds of this workspace faster?

## 🧪 Setup

- kache 2.0.0 and kache-engine 2.0.0, from `cargo install kache --locked --version 2.0.0`.
- Rust 1.99.1, from `rust-toolchain.toml`. Debug profile.
- 4 vCPU, 15 GiB RAM, Linux 6.18, ext4. No reflink support, so restores copy.
- Workload: the first step of the CI `test` job, without running the tests:

  ```sh
  cargo test -p autumn-harvest --no-default-features --no-run --locked
  ```

  It builds 316 units. The workspace members in it are `autumn-harvest`,
  `autumn-harvest-agent` and `autumn-harvest-macros`.
- The wrapper was set per run with `RUSTC_WRAPPER`, not with `kache init`.
  `kache init` writes `~/.cargo/config.toml`, which would also wrap the
  baseline.
- Each run had its own `CARGO_TARGET_DIR`. Sources were fetched before timing.
  The page cache was not flushed.
- An "edit" appends a unique comment line to `autumn-harvest/src/lib.rs`. A
  plain `touch` is not used: kache keys on content, so a `touch` is a free hit
  for kache and a recompile for cargo.

## 📊 Results

### Local development

| Scenario | Plain cargo | kache | Change |
|---|---|---|---|
| Cold build, empty target, empty store | 124.1 s, 123.8 s | 129.8 s | +5% |
| Empty target, warm store (new worktree, `cargo clean`) | 124 s (a cold build) | **10.1 s** | −92% |
| No-op rebuild | 0.5 s | 0.4 s | none |
| 1st edit of `lib.rs` after a restore | 23.4 s | **95.1 s** | +306% |
| 2nd and 3rd edits | 19.7 s, 20.9 s | 27.9 s, 21.7 s | about even |

The warm row ran with `KACHE_SEED_NEW_TARGETS=0`, so every unit came through
the cache. A cold build left a 5.3 GiB store and a 5.4 GiB target. The plain
target was 7.7 GiB, because kache strips the incremental directory.

The first-edit penalty is kache's adaptive incremental mode. A restored
target has no incremental state. So the first edit of a crate compiles it in
full: about 47 s for the `autumn-harvest` lib, plus its test binaries. Later
edits compile incrementally, like plain cargo.

### CI shape

CI already caches with `Swatinem/rust-cache`. That action saves a target
that holds the dependencies only. It removes the workspace-member artifacts
and sets `CARGO_INCREMENTAL=0`. So the fair comparison is kache against that
target, not against a cold build. All rows use `CARGO_INCREMENTAL=0`.

| Pull request shape | rust-cache target | kache, empty target, warm store | Change |
|---|---|---|---|
| Does not touch the three members above | 70.5 s | **5.3 s** | −92% |
| Edits `autumn-harvest/src/lib.rs` | 71.3 s | 81.2 s | +14% |

One kache run is not in the table: 75.7 s on an untouched PR. Its store came
from the local builds. Cargo gives path crates other `codegen-units` when
incremental is on, so every member key missed. A CI store must come from
builds with the same flags as the job that reads it.

38 of the last 50 commits on `trunk-dev` change the source, manifest or
migrations of one of the three members, or change `Cargo.lock`. This clone
holds only 50 commits. On that mix, the expected gain for this job is about
8 s a run. The common case gets slower, and the rare case gets much faster.

## 🧱 Costs that the timings do not show

- **Cache quota.** One job's store is about 5 GiB before compression. `ci.yml`
  already has 27 `rust-cache` steps, and a matrix job saves one entry per leg. GitHub
  evicts by LRU at the repository quota. More large entries would evict the
  current entries, and the existing jobs would get slower.
- **The kache-action cache key has no job name.** `buildCacheKey` in the
  action builds `kache-<version>-<os>-<arch>-<Cargo.lock hash>`. Every job
  needs its own `cache-key-prefix`. Otherwise the first job to save wins, and
  the other jobs restore a store for another workload.
- **Remote writes need a protected-branch push.** With an S3 remote, kache
  refuses writes from pull requests. A PR then rebuilds its own changes on
  each push, unless a separate `pull_request_prefix` is set up with its own
  bucket policy.
- **A new binary in the build path.** kache 2.0.0 ships two binaries and a
  daemon. A cache bug changes the build output. `cargo` alone has no such
  risk.

## ✅ Outcome

- **CI: do not adopt now.** Most PRs edit the core crate. For those, kache
  is 14% slower than the current `rust-cache` setup. The gain on the other
  PRs does not pay for the quota and the extra moving parts.
- **Local: optional, per developer.** kache helps a developer who often makes
  new worktrees or runs `cargo clean`: 10 s instead of 124 s. It does not help
  the edit-rebuild loop, and the first edit after a restore is slower. It is a
  per-machine choice (`kache init`). It needs no change in the repository.

## 🔁 Reopen when

- A trial moves the jobs that do not build `autumn-harvest` (examples, the
  clients, `autumn-harvest-plugin` shards) to a shared S3 store with
  per-job manifest keys. Those jobs are the untouched-PR case.
- kache can restore incremental state into a CI target, so that the
  first-edit penalty goes away.
- The GitHub Actions cache quota is raised enough to hold a store per job.
