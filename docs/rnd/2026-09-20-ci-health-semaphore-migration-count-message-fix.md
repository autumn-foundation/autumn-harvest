# 🚦 Semaphore CI health — the self-contradicting migration-count panic
# message (flagged 09-08, carried forward unfixed at 09-14/09-16) recurred a
# fourth time on a fifth branch and is now fixed, plus today's window census

**Status:** diagnostic-clarity fix, shipped —
`autumn-harvest/tests/integration/sqlite_feasibility_docs.rs`. Not a flake fix
and not a product bug (see Diagnosis). Continues the series from
`docs/rnd/2026-09-18-ci-health-semaphore-corpus-gate-diagnostic-fix.md`.

## 🎯 Verdict path

Same verdict path as the whole series: `ci.yml`'s `pull_request` trigger
against `trunk-dev`, principally `test-db-linux`/`test`/`test-nodb`, plus the
ungated `lint` job's `docs/rnd/sqlite-feasibility.md` guards step, which is
where this report's finding lives. Branch-protection status and cache-usage
API access remain unavailable from this session (re-checked via `ToolSearch`
today; unchanged gap every prior report in this series has logged). No
quarantine ledger exists in this repository (checked again).

## 🌡️ Symptom

### 1. Today's census: window since the 09-18 report's cutoff (2026-09-18T06:36:20Z) through this session's wall clock (2026-09-20T06:06:33Z) — 136 runs: 80 cancelled, 32 success, 24 failure

`list_workflow_runs` with the combined `{event: "pull_request", status:
"completed"}` filter returned identical `total_count` (5013) and an identical
100-run set across two back-to-back identical calls this session — the
09-17 report's finding that this filter is non-deterministic on repeated
calls did **not** reproduce today. Recorded as a data point, not a
retraction: one clean pair of calls does not establish the earlier finding
was wrong, only that it did not recur today. The window needed two pages
(the most recent 100 runs covered only 2026-09-18T19:35:19Z onward; page 2
supplied the remaining 36 runs back to the 09-18 cutoff).

Of the 24 explicit failures, **20 are a single branch**,
`claude/hopeful-pascal-tbijcf`, iterating on `shard_rebalance.rs`-adjacent
work over a ~30-hour span (2026-09-18T23:37:59Z through
2026-09-20T05:34:01Z). Job-logged a sample of 4 of those 20
(`35406455228`, `35449503584`, `35471720583`, `35492002309`, spread across
the window) plus all 4 of the remaining non-`hopeful-pascal-tbijcf`
failures. Every one traces to a distinct, deterministic, own-diff defect —
not a suite-level flake:

| Run | Branch | Signature |
|---|---|---|
| `35406455228` | `claude/hopeful-pascal-tbijcf` | `integration_e2e` legacy-schema `DatabaseError`: `column harvest_workflow_executions.migrated_run_terminal_state does not exist` — the same defect **class** as the 09-16 report's `migrated_run_terminal_at` finding (a hand-maintained legacy-schema SQL fixture lagging a migration-added column), a **different column**, same branch family of change |
| `35449503584` | `claude/hopeful-pascal-tbijcf` | `Clippy autumn-harvest-plugin` failure (own diff) |
| `35471720583` | `claude/hopeful-pascal-tbijcf` | Same `Clippy autumn-harvest-plugin` step, on a same-commit rerun (`run_attempt: 2`, identical `head_sha` as attempt 1) — a clippy compile error is deterministic, so this rerun could not have produced a different result; see the rerun-attempt note below |
| `35492002309` | `claude/hopeful-pascal-tbijcf` | `Clippy autumn-harvest`: `collapsible_if` in `shard_rebalance.rs` (own diff) |
| `35371042002`, `35371731062` | `claude/project-thread-oh1ws0(-1608)` | `E0308` mismatched types, `mcp_tools_http_tests.rs:291` (`Router<AppState>` vs `Router<()>`) — already fixed upstream by `6ed618b` (#1638), merged after these branches forked |
| `35386776482` | `claude/gracious-goldberg-7qvui0` | Clippy `assertions_on_constants` in `hot_swap.rs` (own diff) |
| `35453688571` | `claude/gallant-noether-yp7712` | `sqlite_feasibility_docs::derived_totals_agree_with_the_table_and_the_tree` — **this report's finding, below** |

Not root-caused further this session: the remaining 16 of 20
`hopeful-pascal-tbijcf` failures and the remaining 14 of 18 cancelled runs.
None of the runs checked carry the tracked activity-timeout signature
(issue #1558) or the `corpus::seeded_corpus_is_clean_under_the_syntactic_layer`
signature (closed as a diagnostic-clarity fix, not a flake, per the 09-18
report) — consistent with both staying closed, not a rerun-campaign-grade
confirmation.

**Same-commit reruns:** 4 of 136 runs in the window are `run_attempt > 1`
(`35387845773`, `35414452018`, `35451765344`, `35471720583`). Two are on
`hopeful-pascal-tbijcf` and both are failures; `35471720583` is confirmed
above to be a deterministic clippy compile error re-run without a code
change, which by construction cannot produce a different verdict — a
reflexive rerun on an unread failure, exactly the behavior this role's
charter (Ballast's law) flags as the door a real regression walks through.
At n=2 this is far short of a rerun-click *trend* (Tier 2 evidence needs a
rate over a stated window, not two data points), so it is recorded here,
not actioned.

### 2. `sqlite_feasibility_docs::derived_totals_agree_with_the_table_and_the_tree`: the self-contradicting migration-count message, 4th confirmed occurrence, 2nd distinct branch

`35453688571` (`claude/gallant-noether-yp7712`, 2026-09-19T16:02:43Z) failed
with the exact same self-contradiction the 09-08 report first flagged and
the 09-16 report found recurring 3x on `claude/hopeful-pascal-tbijcf`:

```
the report should state "**107 migrations**"; a live count finds 107 migration directories
```

Both halves of the sentence interpolate the *live* count
(`std::fs::read_dir` over `autumn-harvest/migrations`), so the message can
never show what the report currently states — only ever the live number,
twice. A reader cannot tell from the panic alone whether the report is
stale by one migration or ten, or in which direction. This has now cost at
least 4 CI round-trips across at least 2 branches over 12 days
(09-08 → today) that this series has directly confirmed by log inspection,
plus whatever earlier or unaudited occurrences this series' cancelled-run
and success-run gaps have not sampled.

Checked `docs/rnd/sqlite-feasibility.md` on `trunk-dev` directly: it
currently states `**107 migrations**`, matching the live count on
`trunk-dev` today. This confirms the gate is not stuck red on `trunk-dev`
itself (unlike the 09-13 report's finding, already fixed by PR #1532) — each
occurrence is a feature branch whose own checked-in copy of the doc lags
what its `migrations/` directory contains, the same doc-sync-gate class as
item 2 of the 09-16 report, just recurring on a different branch each time.

## 🔍 Diagnosis

**Test-vs-product verdict, per this role's hard gate:** the *assertion* is
correct and working as designed every time it has fired — a branch whose
migration count and documented count disagree should fail this gate. This
is not a flake and not a product bug. The defect is narrower and entirely
in the test's own diagnostics: `sqlite_feasibility_docs.rs`'s panic message
for this one assertion (unlike every other assertion in the same test
function, which quote both the expected and the live figures side by side)
interpolates only the live count, on both sides of its sentence. This is
the same class of gap this series has now found and fixed twice — the
09-18 report's `corpus.rs` fix (rustc diagnostics dropped on the floor
because the test read `stderr` under `--message-format=json`, which puts
diagnostics on `stdout`) and this one (the panic message never captured
what the document currently says, only ever the value it computes live).
Both cost real investigator and contributor time on every occurrence
without ever being the actual defect a contributor needed to see.

## 🔧 Treatment

Fixed `autumn-harvest/tests/integration/sqlite_feasibility_docs.rs`: added
`stated_migration_count()`, which parses the report text for its current
bold `**N migrations**` figure (scanning back from the ` migrations**`
suffix over a contiguous run of ASCII digits, and requiring the `**` open
marker immediately before them, so it returns `None` rather than a wrong
number if the report's phrasing ever changes). The panic message now reads:

```
the report states "**{stated} migrations**"; a live count finds {migrations}
migration directories. Update the report to "**{migrations} migrations**"
```

(falling back to `no "**N migrations**" figure` if the pattern is absent
entirely, e.g. after an unrelated edit strips the bold figure). This is a
diagnostic-clarity fix, not a test-tolerance change: the assertion condition
(`report.contains(&format!("**{migrations} migrations**"))`) is byte-for-byte
unchanged — nothing that used to pass now fails or vice versa. Only the
panic message improves, from repeating one number twice to naming both the
stale figure and the live one.

## 📊 Measurement

- **Before:** confirmed directly (this session, `35453688571`'s job log, and
  the 09-16 report's three earlier occurrences) that the message always reads
  `"**{live}** migrations**"` on both sides regardless of what the report
  actually states.
- **Revert check (this role's charter, §6):** edited
  `docs/rnd/sqlite-feasibility.md` locally, changing the stated figure from
  `**107 migrations**` to `**106 migrations**` (migrations directory
  untouched, still 107 live). Re-ran
  `cargo test -p autumn-harvest --no-default-features --features testing
  --test integration sqlite_feasibility_docs::derived_totals_agree_with_the_table_and_the_tree
  -- --nocapture`: failed, with the new message reading
  `the report states "**106 migrations**"; a live count finds 107 migration
  directories. Update the report to "**107 migrations**"` — confirms the
  test can still fail (not lobotomized) and that the fix surfaces the actual
  stale figure rather than the live count twice. Reverted
  (`git checkout -- docs/rnd/sqlite-feasibility.md`) and re-ran: clean pass.
- **After, full suite:**
  `cargo test -p autumn-harvest --no-default-features --features testing
  --test integration sqlite_feasibility_docs::` — all 15 tests pass.
  `cargo fmt --all -- --check` clean.
  `docs/audits/comment-hygiene.py --self-test` clean;
  `--base FETCH_HEAD` (against `origin/trunk-dev`) clean, Tier A 0/0/0/0.
  `cargo clippy -p autumn-harvest --all-features --tests -- -D warnings`
  (the same invocation `ci.yml`'s `lint` job runs) caught a real
  `clippy::option_if_let_else` finding on this change's first draft — an
  `if let`/`else` where `Option::map_or_else` was expected. Fixed per
  clippy's own suggestion; re-ran clean (`Finished` with no warnings).
- **Ledger:** no quarantine ledger exists in this repository to update.
  `derived_totals_agree_with_the_table_and_the_tree` was never quarantined
  and needed no retiring — like `corpus.rs`'s test on 09-18, it was
  correctly gating the whole time; only its own diagnostics were unclear.
- **Census (item 1):** not a fix-verification measurement — a window
  census, recorded for the series' continuity. 136 runs, 24 failures, 20
  from one iterating branch, all root-caused to distinct deterministic
  defects on the runs actually job-logged (8 of 24 this session); no
  suite-level flake found or claimed.

## 🔬 Reproduce

```sh
# Today's window census (09-17 report's unfiltered-list workaround was not
# needed today -- the filtered call was deterministic across two identical
# calls; see item 1):
# actions_list(method="list_workflow_runs", resource_id="ci.yml",
#   workflow_runs_filter={event:"pull_request", status:"completed"}, perPage=100)
#   page 1: 2026-09-18T19:35:19Z -> 2026-09-20T06:06:33Z, total_count 5013
#   page 2: 2026-09-16T18:47:27Z -> 2026-09-18T19:33:10Z, total_count 5013 (same)
# Union, filtered to created_at >= 2026-09-18T06:36:20Z: 136 runs
#   (80 cancelled, 32 success, 24 failure)

# Determinism re-check (did NOT reproduce the 09-17 finding today):
# two back-to-back identical actions_list calls -> identical total_count
# (5013) and identical 100-run id sets.

# Per-failure job logs (8 of 24 sampled):
# get_job_logs(run_id=<id>, failed_only=true, return_content=true, tail_lines=40-60)
#   for 35371042002 35371731062 35386776482 35406455228 35449503584
#   35453688571 35471720583 35492002309

# The rerun-attempt check: Counter(r['run_attempt'] for r in <window runs>)
# over the two saved actions_list pages, filtered to the window -> {1: 132, 2: 4}
# 35471720583 confirmed: run_attempt 2, identical head_sha to attempt 1,
# same 'Clippy autumn-harvest-plugin' failure -- a deterministic compile
# error re-run with no code change.

# The message bug, today's occurrence:
grep -n "the report should state" <(curl -sS '<signed log url for job 105925163802>')
# -> "the report should state \"**107 migrations**\"; a live count finds 107
#    migration directories" -- both sides quote the live count.

# docs/rnd/sqlite-feasibility.md on trunk-dev already states 107, confirming
# this is a feature-branch-local staleness, not a trunk-dev regression:
grep -n "migrations\*\*" docs/rnd/sqlite-feasibility.md   # -> "Plus **107 migrations**"

# The fix, and its revert-check:
cd autumn-harvest
cargo test -p autumn-harvest --no-default-features --features testing \
  --test integration sqlite_feasibility_docs::derived_totals_agree_with_the_table_and_the_tree \
  -- --nocapture
# -> test result: ok. 1 passed

sed -i 's/\*\*107 migrations\*\*/\*\*106 migrations\*\*/' ../docs/rnd/sqlite-feasibility.md
cargo test -p autumn-harvest --no-default-features --features testing \
  --test integration sqlite_feasibility_docs::derived_totals_agree_with_the_table_and_the_tree \
  -- --nocapture
# -> FAILED; panic message:
#    the report states "**106 migrations**"; a live count finds 107
#    migration directories. Update the report to "**107 migrations**"

git checkout -- ../docs/rnd/sqlite-feasibility.md
cargo test -p autumn-harvest --no-default-features --features testing \
  --test integration sqlite_feasibility_docs::derived_totals_agree_with_the_table_and_the_tree \
  -- --nocapture
# -> test result: ok. 1 passed

# Full suite + gates:
cargo test -p autumn-harvest --no-default-features --features testing \
  --test integration sqlite_feasibility_docs::            # 15/15 ok
cargo fmt --all -- --check                                # clean
python3 docs/audits/comment-hygiene.py --self-test         # clean
git fetch --no-tags origin trunk-dev
python3 docs/audits/comment-hygiene.py --base FETCH_HEAD   # clean, Tier A 0/0/0/0
cargo clippy -p autumn-harvest --all-features --tests -- -D warnings
# -> first draft: clippy::option_if_let_else on the match in the new
#    assert! call; fixed to Option::map_or_else per its own suggestion;
#    re-run: Finished, no warnings

# Tool-availability re-checks (unchanged from every prior report):
# ToolSearch("branch protection rules github") -> no matching tool
# ToolSearch("actions cache usage github") -> no matching tool
```
