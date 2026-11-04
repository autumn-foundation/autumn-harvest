# Trial: kache in place of rust-cache in CI

> **Status: trial in progress on PR #2551.** CI uses kache through
> `kunobi-ninja/kache-action`. The S3 backend waits for the setup below.
> Until then, each job builds from an empty local store.

Started 2026-11-04 on `trunk-dev` `429cd9a`.

## 🎯 Question

Does [kache](https://github.com/kunobi-ninja/kache) make CI jobs faster than
`Swatinem/rust-cache`? rust-cache saves a target that holds the dependencies
only. Every run then compiles the workspace crates again. kache caches each
`rustc` invocation by content, so an unchanged workspace crate or test binary
is a cache hit.

## ⚙️ What the workflow does

`.github/workflows/ci.yml` runs kache-action in 22 jobs. The comment in the
workflow `env` block gives the reasons. In short:

- `kani`, `fuzz-smoke` and both `harvest-verify` jobs keep rust-cache. They
  cache files that are not `rustc` outputs.
- `CARGO_INCREMENTAL=0` is set for the whole workflow. rust-cache used to set
  it. An incremental build gives path crates another `codegen-units` value.
  Each workspace crate then misses, as a local test showed.
- The kache step runs after any step that deletes `$AGENT_TOOLSDIRECTORY`,
  because the action installs kache there.
- The backend is S3. The GitHub Actions cache is off. The trial measured why;
  see "Results" below.

## 🪣 S3 setup

The S3 backend gives one store that every PR reads. Each push to `trunk-dev`
adds the crates that it compiled, so the store follows `trunk-dev`. The GitHub
Actions cache cannot do that. Its entries are never replaced, so its store
freezes at the first save for each `Cargo.lock`.

1. **Make a bucket.** AWS S3, or an S3-compatible store. Cloudflare R2 has no
   egress fee, and GitHub-hosted runners do not run in AWS. kache writes under
   the `artifacts/` prefix.
2. **Make a credential** that can read and write `artifacts/*` in that bucket:
   `s3:GetObject` and `s3:PutObject` on AWS, or an R2 token with object read
   and write on that bucket.
3. **Add repository variables** (Settings → Secrets and variables → Actions →
   Variables). A repository secret with the same name works too:
   - `KACHE_S3_BUCKET`: the bucket name.
   - `KACHE_S3_REGION`: optional. The default is `us-east-1`. R2 uses `auto`.
   - `KACHE_S3_ENDPOINT`: only for a store that is not AWS. For R2, it is
     `https://<account-id>.r2.cloudflarestorage.com`.
4. **Add repository secrets**: `KACHE_S3_ACCESS_KEY_ID` and
   `KACHE_S3_SECRET_ACCESS_KEY`. A job uses the bucket only when the key secret
   is set. A fork PR gets no secrets, so it builds with no cache.
5. **Protect `trunk-dev`.** On GitHub Actions, kache writes only from a push to
   a branch that GitHub reports as protected (`GITHUB_REF_PROTECTED=true`).
   `KACHE_REMOTE_READONLY=0` does not override that. On 2026-11-04,
   `trunk-dev` had no branch protection and no ruleset, so nothing would be
   written. A branch ruleset that targets `trunk-dev` with only "Restrict
   deletions" and "Block force pushes" is enough, and it does not block an
   ordinary push.
6. **Check the first push run.** After a merge to `trunk-dev`, each kache job
   summary shows the uploads. A job that does not publish says why in the
   `Remote writes` row of `kache doctor`.

Pull request jobs stay read-only. They read what `trunk-dev` published, and
they compile their own changes on each push. kache can give pull requests a
second, writable prefix (`pull_request_prefix`), but the action has no input
for it. Then any PR author can affect the cached artifacts of another PR.

### Hardening to consider

kache's read-only rule is not a security boundary. The bucket policy is.
Same-repository PR jobs get the same secret as `trunk-dev` pushes, so a PR that
edits the workflow can write to the bucket. On AWS, two OIDC roles close that
gap: a write role whose trust policy matches `ref:refs/heads/trunk-dev`, and a
read-only role for every other ref. The daemon reads AWS credentials from the
standard chain, so `aws-actions/configure-aws-credentials` before the kache step
works without key secrets.

R2 has no OIDC trust policy, so it needs two R2 API tokens:

- A token with **Object Read** on the bucket, as the repository secrets above.
  Every job gets it, a PR job included.
- A token with **Object Read & Write**, as secrets of a GitHub environment
  whose deployment branch rule allows `trunk-dev` only. A push job names that
  environment, so only a `trunk-dev` push can read the write token.

A PR that edits the workflow then gets the read token only.

## 📊 Results so far

### Local, before the CI trial

4 vCPU, the CI `test` job's build
(`cargo test -p autumn-harvest --no-default-features --no-run`), one sample
each.

| Case | Plain cargo or rust-cache target | kache |
|---|---|---|
| Empty target, empty store | 124 s | 130 s |
| Empty target, warm store, nothing changed | 70.5 s (rust-cache target) | 5.3 s |
| Empty target, warm store, `autumn-harvest/src/lib.rs` edited | 71.3 s (rust-cache target) | 81.2 s |

An edit to the core crate misses for that crate and every test binary on top
of it. kache then also hashes and stores each large output, so it costs a
little more than rust-cache.

### CI, GitHub Actions cache backend

These runs used the GitHub Actions cache, before the switch to S3. The
baseline is the per-job median of 8 recent rust-cache runs. Only successful
jobs count. One runner can take twice as long as another for the same job,
so a single run is not a fair comparison.

| Set of jobs | rust-cache median | kache cold | kache warm |
|---|---|---|---|
| 17 jobs that finished in both kache runs | 126 job-min | 147 (+17%) | 68 (−45%) |
| The 10 of them whose warm run restored a store | 91 job-min | 99 (+8%) | 30 (−67%) |

The cold run had an empty store, and the warm run changed no Rust source. So
the warm column is the best case. It matches a PR that touches no workspace
crate. A PR that edits `autumn-harvest` misses for that crate and for every
crate and test binary above it.

**The GitHub cache cannot hold the stores.** One run saved 0.4 to 0.5 GB per
job leg, compressed, over 35 legs. That passes the repository quota. GitHub
evicts the least recently used entry first. In the warm run, the 7 jobs whose
stores were saved first all missed, and every job saved later hit. The same
eviction removes the rust-cache entries of other branches. So the workflow no
longer uses the GitHub cache, and the store moves to S3.
