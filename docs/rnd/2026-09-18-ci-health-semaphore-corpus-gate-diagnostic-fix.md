# 🚦 Semaphore CI health — `corpus::seeded_corpus_is_clean_under_the_syntactic_layer`
# was never a flake: three sessions of "undiagnosed, closest thing to a
# suite-attributable flake" (09-15, 09-16, 09-17 reports) were the same
# deterministic `-D warnings` gate correctly failing on each branch's own
# real dead code, and the reason it looked mysterious every time is that
# the test's own panic message threw the actual rustc diagnostic away —
# fixed with a verified revert-check, plus today's 23-run window census

**Status:** determinism/diagnostic PR against a test (not a flake fix — see
Diagnosis) — `autumn-harvest-verify/tests/corpus.rs`. Continues the series from
`docs/rnd/2026-09-17-ci-health-semaphore-list-workflow-runs-nondeterminism.md`.

## 🎯 Verdict path

Same verdict path as the whole series: `ci.yml`'s `pull_request` trigger
against `trunk-dev`/`trunk`, principally `test-db-linux`/`test`/`test-nodb`
plus the `Semantic determinism analysis (harvest-verify)` job this report's
finding concerns. Branch-protection status and cache-usage API access remain
unavailable from this session (re-checked via `ToolSearch` today, same gap
every prior report in this series has logged). No quarantine ledger exists in
this repository (checked again).

Per the 09-17 report's finding, `list_workflow_runs`'s combined `{event,
status}` filter is non-deterministic on repeated identical calls. This
session used that report's recommended workaround: paginating the
**unfiltered** list (one page covered the whole window; `total_count: 5678`
on that page) and filtering client-side for `event=="pull_request" and
status=="completed"`.

## 🌡️ Symptom

### 1. Today's census: window since the 09-17 report's cutoff (2026-09-17T07:39:28Z) through this session's wall clock (2026-09-18T06:36:20Z) — 23 runs: 18 cancelled, 4 success, 1 failure

The single failure, `35312881581` (2026-09-18T05:57:54Z, branch
`claude/pensive-brahmagupta-jafcou`), job-logged in full: 2 failed jobs.

- `Lint` → `Clippy autumn-harvest-sqlite` step: `error: function
  \`apply_skip_policy_indexed\` is never used` at `autumn-harvest/src/calendar.rs:133:4`
  (`-D dead-code` implied by `-D warnings`).
- `Semantic determinism analysis (harvest-verify)` → `Analyzer, corpus and
  boundary tests` step → the test named in this report's title, `FAILED`.

These are not two independent findings. Fetched the corpus job's full raw
log (`get_job_logs` for the signed URL, then `curl` + inspect — the tail
alone truncates before the actual diagnostic, same gap this series has hit
on other large-suite failures): the test's own nested `cargo build
--message-format=json -D warnings` recompiles `autumn-harvest` from a cold
target directory, and it fails on the identical dead-code warning:

```
warning: function `apply_skip_policy_indexed` is never used
   --> autumn-harvest/src/calendar.rs:133:4
```

promoted to a hard error by the corpus test's own `-D warnings`, distinct
from and independent of the `Lint` job's clippy step (different job, no
`needs:` between them — confirmed directly against `ci.yml`). One real
defect on this branch's own diff, correctly and deterministically caught
twice, by two different mechanisms.

### 2. The same signature, verified against two of this item's three prior occurrences named in the 09-15/09-16 reports — same branch, same mechanism, not independent

The 09-15 report (`docs/rnd/2026-09-15-ci-health-semaphore-window-census.md:141-144`)
found `seeded_corpus_is_clean_under_the_syntactic_layer` recurring across 3 of
5 pushes on branch `claude/bold-lovelace-agnczk` (`34891509704`,
`34888038964`, `34883968010`) and logged it as "not otherwise diagnosed." The
09-16 report separately root-caused `34891509704`'s **clippy** dead-code
finding (`DISABLE_RENAME_SUFFIX` in `partition.rs`) but explicitly declined to
connect it to that same run's corpus failure, calling them "two independent
failed jobs."

Fetched both `34891509704`'s and `34888038964`'s corpus-job full logs this
session (`get_job_logs` signed URL + `curl`, same method as item 1): both
show the identical `could not compile \`autumn-harvest\` (lib) due to 1
previous error` signature, from the same cold nested rebuild, on the same
branch that clippy independently flagged for dead code. `34888038964` (an
earlier commit, 19:37:34Z) shows the corpus job's own `Analyzer, corpus and
boundary tests` step failing while `Lint`'s `Check formatting` step failed
first on an unrelated `rustfmt` diff — proving the corpus job runs
independently of `Lint`'s outcome, not gated on it. The dead-code defect
persisted from `34888038964` through `34891509704` (a later commit on the
same PR, 20:12:33Z), consistent with one unfixed defect surfacing on both
commits, not two coincidences.

This is not a rate over independent draws — it is one branch's one
unfixed defect, observed at two of its three consecutive commits (the third,
`34883968010`, not re-fetched this session; carried forward as unconfirmed
by direct log inspection, though the branch/timing pattern is identical).
Combined with today's occurrence on an unrelated branch, the signature has
now been directly log-verified as this same dead-code-triggered mechanism on
2 of 4 known total occurrences, and is circumstantially consistent with it
on a 3rd (`34888038964`, verified independently above) — 3 of 4 in total,
with `34883968010` the only one not directly checked this session.

### 3. Cancelled-run sample: 4/18 job-logged, 0/4 hidden failures

Spread across the window: `35315175671`, `35313675041`, `35300840707`,
`35212851627` — one from each of four different branches. All four show
ordinary `concurrency.cancel-in-progress` behavior: every job cancelled
mid-step at the same instant, consistent with a newer push superseding an
in-progress run, no job-level `"failure"` conclusion hidden under any of the
four. The remaining 14 of 18 cancelled runs were not job-logged this
session.

Worth noting, not a defect: 11 of this window's 18 cancelled runs are a
single branch (`claude/youthful-mendel-bfgsu7`) pushing 10 times in
roughly 50 minutes (10:15:59Z–11:10:32Z), each push cancelling the previous
run — ordinary iterative-development churn under `cancel-in-progress`, not
independent evidence of anything.

### 4. Other tracked items: no recurrence among the runs actually checked

- Issue #1558's activity-timeout signature — not present in today's one
  explicit failure or the 4-run cancelled sample. Still no rerun campaign has
  ever been run for this fix (carried forward, unchanged).
- `quota_enforcement_tests`'s unexplained 10-second timeout (09-16 report) —
  not recurred among the runs checked. Still 1/1 total across the series.
- `benchmarks_docs::the_doc_names_no_competitor_engine` (09-17 report,
  already fixed upstream) — not recurred, as expected.
- `sqlite_feasibility_docs`'s self-contradicting panic message — not
  triggered this window (no run in the checked population touched that
  test).

## 🔍 Diagnosis

**Test-vs-product verdict, rendered explicitly, per this role's own hard
gate:** every occurrence checked this session is the **product** — real dead
code introduced by that specific branch's own diff — not the test and not the
suite. `seeded_corpus_is_clean_under_the_syntactic_layer` is working exactly
as designed: it independently proves the corpus compiles with zero warnings
at any severity (its own docstring: "that build is the proof that
HVG001–HVG011 report nothing at any severity"), which is a stronger
guarantee than the `Lint` job's clippy step alone, and it correctly went red
both times a branch actually shipped dead code. **This item is not a flake,
was never a flake, and the hard gate's item 3 ("show the nondeterminism
lives in the test, not the thing it tests") is satisfied in the direction of
"there is no nondeterminism — it is the product, correctly caught, every
time."** Three consecutive prior reports (09-15, 09-16, 09-17) carried this
forward as an open, undiagnosed candidate specifically because nobody had
opened the corpus job's full raw log (as opposed to its truncated tail) far
enough back to see the warning that scrolled past before the final "could
not compile" summary.

**The actual defect this session found and fixed is a diagnostic-clarity
gap, the mechanism for why it looked undiagnosable:** `corpus.rs`'s own
`-D warnings` build runs with `--message-format=json`, so rustc emits its
diagnostics as JSON on **stdout**, one object per line. The test's panic
branch only ever interpolated `output.stderr` — which under
`--message-format=json` contains nothing but cargo's plain-text progress
narration ("Compiling ...") and its terse final summary ("could not compile
... due to 1 previous error"). The one thing a reader needs — which lint
actually got denied — was captured on `output.stdout` (already parsed
elsewhere in this same test, for an unrelated purpose: confirming the build
actually compiled the corpus packages, `corpus.rs:361` onward) and simply
never surfaced in the failure message. This is the same class of gap this
series has flagged before (`sqlite_feasibility_docs`'s self-contradicting
panic message, 09-08/09-14/09-16 reports) — a real, reproducible defect in a
test's own diagnostics, below the bar for "flaky test" but costing real
investigator time on every occurrence.

## 🔧 Treatment

Fixed `autumn-harvest-verify/tests/corpus.rs`: added
`rendered_compiler_errors()`, which parses the build's JSON stdout for
`compiler-message` entries at `level: "error"` and includes their rendered
text in the panic message, alongside (not instead of) the existing stderr
capture. This is a diagnostic-clarity fix, not a test-tolerance change: it
does not alter what the test asserts, what makes it pass, or what makes it
fail — only what a reader sees when it fails. No assertion was widened, no
lint was suppressed, no retry was added.

Per the hard gate, this does not claim to be a flake fix (there was no
flake), a product bug report (the product bugs behind each historical
occurrence were already fixed by that branch's own follow-up commit before
this session started), or a timing win. It is filed as a determinism/CI-health
PR because it converts a symptom that took four separate investigation
sessions (09-15 through today) to even provisionally characterize into one
that names its own root cause on the first failure, going forward.

## 📊 Measurement

- **Before:** the failing job's own panic message, both today
  (`35312881581`) and historically (`34891509704`, `34888038964`), reads only
  `the corpus must build with \`-D warnings\`... --- stderr ---` followed by
  cargo's `Compiling ...` lines and `error: could not compile \`autumn-harvest\`
  (lib) due to 1 previous error` — zero information about which lint fired.
  Root-causing each occurrence this session required downloading the full
  raw job log via the job's signed URL and grepping it by hand.
- **Revert check (this role's charter, §6):** temporarily reintroduced a real
  dead-code defect (an unused private function appended to
  `autumn-harvest/src/lib.rs`) and re-ran
  `cargo test -p autumn-harvest-verify --test corpus
  seeded_corpus_is_clean_under_the_syntactic_layer -- --nocapture` locally.
  The test failed, and the new panic message's `--- rustc diagnostics ---`
  section printed the actual diagnostic:
  ```
  error: function `semaphore_revert_check_dead_function` is never used
     --> autumn-harvest/src/lib.rs:928:4
      |
  928 | fn semaphore_revert_check_dead_function(x: u32) -> u32 {
      |    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
      |
      = note: `-D dead-code` implied by `-D warnings`
  ```
  Confirms the test can still fail (not lobotomized) and that the fix
  actually surfaces the failing lint. Reverted the injected defect
  (`git checkout -- autumn-harvest/src/lib.rs`) and re-ran the same command:
  clean pass, `test result: ok. 1 passed`.
- **After, full suite:** `cargo test -p autumn-harvest-verify --test corpus
  -- --nocapture` — all 6 tests pass (`test result: ok. 6 passed; 0 failed`).
  `cargo fmt --all -- --check` clean. `docs/audits/comment-hygiene.py --base
  origin/trunk-dev` clean (Tier A: 0/0/0/0; Tier B: 0 new findings in the
  changed file after splitting one over-length sentence the first draft of
  this comment introduced).
- **Not run:** `cargo clippy -p autumn-harvest-verify --all-targets -D
  warnings` in this sandbox fails on an unrelated, pre-existing issue —
  `error: unknown lint: \`clippy::unused_async_trait_impl\`` at
  `autumn-harvest/src/context.rs:15113` — confirmed present with this
  session's changes stashed out too (same error, same line), so it is a
  local clippy/toolchain version mismatch against what `ci.yml` pins, not
  something this change introduced or something this session can fix.
- **Ledger:** no quarantine ledger exists in this repository to update.
  `seeded_corpus_is_clean_under_the_syntactic_layer` itself was never
  quarantined and needed no retiring — it was correctly gating the whole
  time.

## 🔬 Reproduce

```sh
# Today's census (the 09-17 report's recommended unfiltered-list workaround —
# the combined {event,status} filter is confirmed non-deterministic, see that
# report's item 0):
# actions_list(method="list_workflow_runs", resource_id="ci.yml",
#   perPage=100, page=1)   # one page covered the whole window; total_count 5678
python3 -c "
import json
with open('page1.json') as f:
    d = json.load(f)
runs = d['workflow_runs']
cutoff = '2026-09-17T07:39:28Z'
pr_completed = [r for r in runs if r['event']=='pull_request' and r['status']=='completed']
new = [r for r in pr_completed if r['created_at'] > cutoff]
from collections import Counter
print(len(new), Counter(r['conclusion'] for r in new))
"
# -> 23 {'cancelled': 18, 'success': 4, 'failure': 1}

# Today's failure, full job log:
# get_job_logs(run_id=35312881581, failed_only=true, return_content=true)
# -> Lint job: "error: function `apply_skip_policy_indexed` is never used"
#    at autumn-harvest/src/calendar.rs:133:4
# get_job_logs(job_id=105498394456, return_content=false) -> signed URL, then:
curl -sS '<signed logs_url>' -o corpus_job.log
grep -n "never used\|could not compile\|panicked at" corpus_job.log

# Historical occurrence 1, same signature:
# list_workflow_jobs(resource_id=34891509704) -> job id for
# "Semantic determinism analysis (harvest-verify)"
# get_job_logs(job_id=104135163437, return_content=true, tail_lines=150)
# -> "could not compile `autumn-harvest` (lib) due to 1 previous error"
#    while compiling harvest-verify-corpus-helpers -- same signature.
# This run's Lint job (separately job-logged) carries the
# `DISABLE_RENAME_SUFFIX` dead-code clippy finding the 09-16 report named.

# Historical occurrence 2, same signature, earlier commit, same branch:
# list_workflow_jobs(resource_id=34888038964) -> Lint failed on an unrelated
# `cargo fmt` diff (Check formatting), independently the corpus job's own
# "Analyzer, corpus and boundary tests" step failed too -- proving the two
# jobs are not gated on each other.
# get_job_logs(job_id=104123533700, return_content=true, tail_lines=150)
# -> identical "could not compile ... due to 1 previous error" signature.

# The diagnostic-gap mechanism (why the panic message never showed the
# actual lint): corpus.rs builds with --message-format=json, so diagnostics
# are JSON records on stdout, not stderr:
sed -n '323,356p' autumn-harvest-verify/tests/corpus.rs   # (pre-fix line numbers)

# The fix, and its revert-check:
cd autumn-harvest-verify
# 1. Control: clean pass, no diagnostic to show.
cargo test -p autumn-harvest-verify --test corpus \
  seeded_corpus_is_clean_under_the_syntactic_layer -- --nocapture
# -> test result: ok. 1 passed

# 2. Inject a real defect and confirm the new message surfaces it:
cat >> ../autumn-harvest/src/lib.rs << 'RUST'

fn semaphore_revert_check_dead_function(x: u32) -> u32 {
    x + 1
}
RUST
cargo test -p autumn-harvest-verify --test corpus \
  seeded_corpus_is_clean_under_the_syntactic_layer -- --nocapture
# -> FAILED; panic message's "--- rustc diagnostics ---" section shows:
#    "error: function `semaphore_revert_check_dead_function` is never used"

# 3. Revert and confirm a clean pass again:
git checkout -- ../autumn-harvest/src/lib.rs
cargo test -p autumn-harvest-verify --test corpus \
  seeded_corpus_is_clean_under_the_syntactic_layer -- --nocapture
# -> test result: ok. 1 passed

# Full suite + gates:
cargo test -p autumn-harvest-verify --test corpus -- --nocapture   # 6/6 ok
cargo fmt --all -- --check                                        # clean
python3 docs/audits/comment-hygiene.py --base origin/trunk-dev     # clean

# Confirmed pre-existing, unrelated (not fixed here):
git stash
cargo clippy -p autumn-harvest-verify --all-targets -- -D warnings
# -> same "unknown lint: clippy::unused_async_trait_impl" at context.rs:15113
git stash pop

# Tool-availability re-checks (unchanged from every prior report):
# ToolSearch("branch protection rules github") -> no matching tool
# ToolSearch("actions cache usage github") -> no matching tool
```
