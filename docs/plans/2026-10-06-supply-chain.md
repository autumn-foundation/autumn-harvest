# Plan — Supply chain: advisory scan, SHA pins, Dependabot, signed releases (issue #1826)

Status: implementation plan (TDD: red, green, refactor).

## 1. Brainstorming — candidate approaches

1. **Advisory scan: the `cargo-deny-action` with `command: check advisories`.**
   Rejected. The action keeps its output in the log, so the alert issue cannot
   quote the findings.
2. **Advisory scan: `cargo-deny` 0.20.2 from `taiki-e/install-action`, run by a
   script.** Chosen. 0.20.2 is the version that `cargo-deny-action` v2.1.1
   bundles, so the daily scan and the CI gate agree. A stub test proves the
   script.
3. **Alert: a new issue for each failed run.** Rejected. A daily cron then opens
   one issue a day. The script comments on the open issue instead and closes it
   after a clean run.
4. **SHA pins: a line-based lint only.** Rejected. A flow mapping or an escaped
   key hides a `uses` key from a line scan.
5. **SHA pins: a `docs/audits/` lint that parses the YAML, then reads the raw
   line for the comment.** Chosen. The parser finds every key, and the line
   gives the version comment. It runs in the `lint` job, which also runs on a
   docs-only change.
6. **Renovate.** Rejected. It needs a GitHub App. Dependabot needs a file only,
   and it updates a SHA pin and its version comment together.
7. **Sign each archive in its build job.** Rejected. The build runs third-party
   `build.rs` code. A job that can mint an OIDC token must not run that code.
8. **A separate `sign` job per target.** Chosen. It holds `id-token: write` and
   runs no build.
9. **Dry run on `workflow_dispatch` only.** Rejected as the only path. A change
   to `release.yml` must prove itself before merge. A `pull_request` run on that
   file is a dry run too.

Selected: **2 + 5 + 6 + 8 + 9 (both dry-run paths)**, and 3's "comment, then
close" model.

## 2. Reverse brainstorming — how to make this fail

- **R1. The scan never runs.** A cron in a file that does not parse never fires.
  Foreclosed: `workflow-yaml-parse.py` and a Rust test parse the new file. A
  `pull_request` run on the scan files proves one green run before merge.
- **R2. A red scheduled run is silent.** GitHub tells only the last cron editor.
  Foreclosed: the script opens an issue. A red run with no `reported=true`
  output reports on the same issue. That covers a step timeout too.
- **R3. A hung step cancels the job, so the report step does not run.**
  Foreclosed: each step has a timeout, and the timeouts sum to less than the job
  timeout (the issue #1790 lesson).
- **R4. A `gh` error reads as "clean".** Foreclosed: `set -euo pipefail` stops
  the script on any failed call. A stub test proves it.
- **R5. The alert issue grows to the 65536-character limit.** Foreclosed: the
  script quotes only the tail of the scan output.
- **R6. A pull request run opens an issue.** Foreclosed: the workflow enables the
  alert on `schedule` only.
- **R7. A pin in an unusual form passes the lint.** Foreclosed: the lint finds
  each `uses` key in the parsed YAML tree. A key that is not in the plain
  one-line form is a finding (fail closed).
- **R8. A SHA pin drops the action input that the tag gave.**
  `dtolnay/rust-toolchain@stable` takes its toolchain from the ref. Foreclosed:
  each pin of that action sets `toolchain:` explicitly. A test checks it.
- **R9. Dependabot opens pull requests against `trunk`.** Foreclosed: each entry
  sets `target-branch: trunk-dev`. A test checks it.
- **R10. A fork pull request has no OIDC token, so its dry run is red.**
  Foreclosed: the `sign` job skips a fork pull request.
- **R11. A dry run publishes a release.** Foreclosed: the `release` job runs only
  on a tag push. A test checks it.
- **R12. The signature verifies against the wrong identity.** Foreclosed: the
  `sign` job verifies each bundle against this workflow's exact certificate
  identity and the GitHub OIDC issuer.
- **R13. The SBOM describes a different build.** Foreclosed: the SBOM uses the
  same package, features and target as the build. The SBOM is attested against
  the archive digest.
- **R14. A dry run attestation verifies like a release.** Foreclosed: a dry run
  writes no attestation. The docs pass `--source-ref` and `--signer-workflow`.
- **R15. Third-party code runs with a write token.** Foreclosed: `binaries` and
  `client` hold read-only tokens. The `release` job runs no build and keeps no
  credentials on disk.

## 3. Six hats

- **White (facts).** `cargo deny` runs in CI on code changes only. Every action
  is pinned by tag. There is no Dependabot file. `release.yml` ships the
  TypeScript client only, with no CLI binaries. The repository is public, so
  artifact attestations are available.
- **Red (instinct).** Tag pins are mutable. A moved tag runs new code with the
  workflow's token. Pin first, then automate the updates.
- **Black (risks).** Daily alerts can fatigue readers. Three release targets
  cost runner minutes. Each dry run writes an entry to the public Rekor log.
- **Yellow (upside).** A new RUSTSEC advisory reaches an issue within a day.
  Users can verify each binary with `cosign` or `gh attestation verify`.
  `cargo audit bin` can scan a shipped binary.
- **Green (creativity).** Use one pull request run as the "ran green once"
  evidence. Keep the alert script testable with a stub `gh` and a stub
  `cargo-deny`, as the chaos watchdog does.
- **Blue (process).** Red tests first, then green, then refactor. Then a
  multi-angle review, then an acceptance-criteria audit.

## 4. Design

### 4.1 Daily advisory scan

- `.github/workflows/advisory-scan.yml`: a daily cron, `workflow_dispatch`, and
  `pull_request` on the scan files. Permissions: `contents: read`,
  `issues: write`.
- `.github/ci/advisory-scan.sh`: runs `cargo deny --all-features check
  advisories`. With `ADVISORY_ALERT=true`, a failed scan opens or comments on
  the alert issue, and a clean scan closes it. The exit code is the scan's.
  After it reports, it sets the step output `reported=true`.
- `advisory-scan.sh run-failed` reports a red run with no report: a failed
  setup step, a step timeout or a lost `gh` call.

### 4.2 SHA pins

- Each `uses:` is `owner/repo[/path]@<40-hex SHA> # <ref>`, a `./` path, or a
  `docker://` image with a `sha256` digest.
- `docs/audits/action-sha-pin.py` enforces it, with `--self-test`. The `lint`
  job runs both.

### 4.3 Dependabot

- `.github/dependabot.yml`: `cargo` and `github-actions` on `/`. Weekly.
  `target-branch: trunk-dev`. Minor and patch updates are grouped. `/fuzz` has
  no lockfile and no default CI build, so it is not listed.

### 4.4 Release

- `binaries` (matrix: Linux x86_64, macOS arm64, Windows x86_64):
  `cargo auditable build`, an archive, and a CycloneDX SBOM. The token is
  read-only.
- `sign` (same matrix): `cosign sign-blob` keyless and `cosign verify-blob`.
  On a tag push only, `attest-build-provenance`, and `attest` with the SBOM.
- `client`: the TypeScript client, with a read-only token, because `npm` runs
  third-party install scripts.
- `release`: a tag push only. It uploads the archives, SBOMs and bundles. Its
  checkout keeps no credentials.
- A dry run (`workflow_dispatch`, or a pull request that changes
  `release.yml`) runs `binaries` and `sign`, and uploads the result as a
  workflow artifact. It writes no attestation, because `gh attestation verify
  --repo` alone accepts any run of this repository.

### 4.5 Tests mapped to acceptance criteria

| AC | Test or evidence |
|----|------------------|
| Scheduled advisory job exists and ran green once | `supply_chain_ci::advisory_*` tests, and the green `Advisory scan` run on the pull request |
| Every `uses:` references a SHA, and a lint enforces it | `action-sha-pin.py --self-test` and its full scan in the `lint` job; `supply_chain_ci::lint_job_runs_the_sha_pin_audit` |
| A release dry run produces an SBOM and signature | `supply_chain_ci::release_*` tests, the local `cargo auditable` + `cargo cyclonedx` run, and the `Release` dry run on the pull request |
