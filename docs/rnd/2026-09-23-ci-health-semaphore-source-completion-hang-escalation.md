# 🚦 Semaphore CI health — `completion_trigger_defers_to_outbox_when_target_quota_exceeded`'s SOURCE-completion wait has gone from 2 occurrences in 6 days to 6 more identical-signature occurrences in 26 hours, on 6 differently-named branches, and the 30s timeout widen that shipped for it (PR #1673, 09-21) did not fix it

**Status:** health report — no PR opened against `ci.yml`, `quota_enforcement_tests.rs`,
or `completion_trigger.rs`. This role's hard gate (a located problem, a named
mechanism, a rendered test-vs-product verdict, and a before/after measurement
from a rerun harness) is not met: no Docker daemon in this session's sandbox
(re-checked today — `docker ps` fails, `/var/run/docker.sock` absent, same gap
every report in this series has hit). Continues the series from
`docs/rnd/2026-09-21-ci-health-semaphore-quota-outbox-recurrence.md` and
`docs/rnd/2026-09-23-ci-health-semaphore-shard-0-rebalance.md` (the latter
landed on `claude/fix-shard-0-collision-rebalance-1685`, not yet merged to
`trunk-dev`, so it is not in this session's tree).

**Corrected across five Codex review rounds on this PR.** First round: the
first draft claimed the `QuotaExceeded` arm "never" propagates `Err`/rolls
back the source's transaction, having stopped reading `completion_trigger.rs`
right before the outbox-row insert (that insert's own `.map_err(...)?` can in
fact propagate and roll back — restored as a candidate mechanism); and it
called the six branches "unrelated"/"independent" based only on an
open-PR-listing check, which cannot establish that (downgraded to
"differently-named"). Second round, on the corrected draft: Codex caught that
the "pool exhaustion" example named for the insert-error mechanism is
impossible for this specific insert (it runs on an already-acquired
connection, no new pool checkout) — removed, replaced with the DB-error
causes that connection can actually hit; and that the independence
correction's own admission ("no SHA/diff check done") undercut the
still-unverified claim that all 6 occurrences ran at the new 30s timeout
bound rather than the old 10s one. This session then fetched and checked each
occurrence's actual commit: 5 of 6 confirmed at the 30s bound, 1
(`gallant-dijkstra-a83hyy`) confirmed still on the old 10s bound. **Third
round, on that correction:** Codex caught that this report's "2 clean
passes bound the window, so it's intermittent" claim compared against two
runs whose own diffs were never checked either — checking them directly
found something worse than "unverified": both were **docs-only PRs whose
test steps never ran at all**, so they were never real passes to begin with.
Retracted, and replaced with the one genuine clean execution this session
could find (pre-widen). The same pass, re-checking directly caught a second
thing this report had gotten wrong on its own, unprompted: the 09-21
report's baseline for this signature is 2 occurrences, not 1 (a second one,
in that report's own item 4 cascade, was missed by every earlier draft of
this report too) — corrected throughout.

**Fourth round, on that correction, three more findings.** A "pool
exhaustion" mention survived in a second, near-duplicate Diagnosis bullet
that the second round's sweep had missed — removed there too. The
corrected 2-occurrence baseline (from round three) then exposed that this
report's "spike begins shortly after `56bc205` merged" timing-correlation
claim was already contradicted by its own evidence: both of those 09-21
occurrences happened 11-14 hours *before* `56bc205` merged, the same
day — retracted as a candidate. And the shard-0-collision paragraph's
"every occurrence landed on the shard carrying both heavy suites" claim was
checked against PR #1707's own occurrence under *its own* proposed
21-shard layout, where the two suites land on different shards (`44 % 21 =
2` vs `33 % 21 = 12`) — that occurrence is not explained by the collision,
if anything weakening rather than strengthening it as a candidate.

**Fifth round, two more findings.** This report's "not CI is just slow"
argument assumed the 30s wait covers only the synchronous trigger-evaluation
cycle; Codex pointed out the test's own source shows the wait starts right
after the worker is spawned, and the source workflow was already enqueued
*before* that — so the clock also covers worker startup and task-claim
latency, which a CPU-starved runner could plausibly consume on its own.
Runner slowness is restored as an open candidate, not downgraded. And this
report's advice that mergers "should not treat [the test's] continued
redness as a regression introduced by" PR #1706 or #1707 overreached: predating
both PRs rules out either being the *sole* cause, not either *changing the
rate* — #1706 specifically activates a previously-dormant background-scanner
code path that is new DB work on every poll tick, unexamined as a possible
load contributor to this other test. Softened to "unresolved," with a
same-commit rerun comparison (PR vs. its own base) named as the way to
settle it. All corrections are inline at the point each applies, matching
this series' convention.

## 🎯 Verdict path

Same verdict path as the whole series: `ci.yml`'s `pull_request` trigger
against `trunk-dev`, principally `test-db-linux`. Concretely, in this window,
`Test DB (linux, shard 0)` (11-shard layout) / `Test DB (linux, shard 2)`
(PR #1707's proposed 21-shard layout) — the shard(s) that carry
`quota_enforcement_tests`. Branch-protection and cache-usage API access
remain unavailable (re-checked, unchanged).

## 🌡️ Symptom

### This session's starting point: two fix PRs already open against this exact test, both still red on their own CI

Before running a fresh census, this session found `claude/fix-quota-outbox-scanner-sharded-pool-1685`
(PR #1706, "wire `sharded_pool` into the test's own worker config", targeting
this test's *other* panic signature — `"target row was never created by the
outbox retry"` at `quota_enforcement_tests.rs:3804`) and
`claude/fix-shard-0-collision-rebalance-1685` (PR #1707, an 11→21 shard
rebalance) already open, both citing issue/PR #1685 (this series' 09-21
report). Both PRs' *own* CI runs — pushed hours apart, on top of the fix
commits — still fail `quota_enforcement_tests`:

| Run | Branch | Job | Panic |
|---|---|---|---|
| `35788065837` (job `107008321702`) | PR #1706, `claude/fix-quota-outbox-scanner-sharded-pool-1685` | `Test DB (linux, shard 0)`, 2026-09-23T01:44:40Z | `completion_trigger_defers_to_outbox_when_target_quota_exceeded` panicked at `integration_e2e.rs:1383:6` |
| `35813687699` (job `107035722407`) | PR #1707, `claude/fix-shard-0-collision-rebalance-1685` | `Test DB (linux, shard 2)` (new 21-shard layout), 2026-09-23T03:55:37Z | same test, same panic site |

PR #1706's own description already retracted an earlier draft's claim that
its fix would also explain this panic: `evaluate_triggers_for_execution`'s
`QuotaExceeded` arm never touches the source's own terminal commit *on its
success path* — confirmed this session by reading `completion_trigger.rs:
2078-2100`, the tracing call and metrics record before the outbox insert.
**Correction (post-review, Codex on this PR, two rounds).** First: an earlier
draft of this report stopped reading at line 2100 and claimed "no `Err`/`?`
propagation out of the source's transaction" outright. That is wrong:
continuing to line 2121, the outbox-row insert itself ends
`.map_err(crate::error::database_error)?` (`completion_trigger.rs:2101-2122`).
A database error on *that* insert does propagate `?` out of this function,
and per the function's own doc comment (the whole arm "runs INLINE inside the
SOURCE execution's own terminal transaction"), that would roll back the
source's `WorkflowCompleted` append along with it, reproducing exactly the
pre-fix symptom this test guards against.

Second: that correction's own first draft listed "transient pool exhaustion"
as an example cause of such an error. Also wrong, per a second Codex round —
`evaluate_triggers_for_execution_collecting_with_codecs` receives an
**already-acquired** `&mut AsyncPgConnection` (confirmed by reading its
signature, `completion_trigger.rs:1487-1488`); the outbox insert runs on that
same connection, inside the already-open transaction, and never checks out a
new connection from the pool. So pool exhaustion cannot be this specific
insert's error — it could only affect an *earlier* step (acquiring the
connection this function is handed, before it's ever called), a different
diagnosis branch entirely. The insert can still fail from an error on the
existing connection: a constraint violation, a dropped connection mid-query,
a statement timeout, or a serialization/deadlock error under concurrent
write load.

So #1706's fix landing will not close this on the arm's *ordinary* path
(confirmed unchanged), but a rare error on the outbox insert itself — from
one of the connection-local causes above, not pool exhaustion — is a real,
undismissed candidate mechanism this report had wrongly ruled out. Not
confirmed either way this session (no worker-level logging or DB-error
telemetry available to check whether any of the 6 occurrences actually hit
this insert's error path), added to the diagnosis below.

### Widened census: 4 more branches hit the identical panic site in the same ~26-hour window

Sampling `Test DB (linux, shard 0)` failures from today's `ci.yml` window
(90 `pull_request`/`completed` runs since the 09-21 report's cutoff,
2026-09-21T10:18:51Z: 61 cancelled, 26 failure, 3 success), every
shard-0 failure checked — 4 more beyond the two PRs above, chosen for having
no obvious own-diff compile/lint signature in the run summary — resolves to
the **exact same test, exact same panic site**:

| Run (job) | Branch | Timestamp (UTC) |
|---|---|---|
| `35652570063` (`106647193659`) | `claude/trusting-ritchie-nml6ud` | 2026-09-22T07:15:01Z |
| `35658767845` (`106896752815`) | `claude/gallant-dijkstra-a83hyy` | 2026-09-22T19:29:31Z |
| `35788942436` (`106960979043`) | `claude/confident-babbage-sl0122` | 2026-09-22T22:32:07Z |
| `35835324763` (`107108884704`) | `claude/cool-noether-7dejjb` | 2026-09-23T09:00:21Z |

Every one of these 6 (the 2 PRs above plus these 4) shows the byte-identical
panic:

```
thread 'quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded' panicked at autumn-harvest/tests/integration/integration_e2e.rs:1383:6:
workflow should reach expected state within timeout: Elapsed(())
```

reached from `quota_enforcement_tests.rs`'s call to wait for the source's
`COMPLETED` state — at a 30s bound for 5 of the 6 occurrences, and, per a
correction below, the original 10s bound for the 6th.

Six occurrences, six different branches, spanning 2026-09-22T07:15Z through
2026-09-23T09:00Z — **26 hours**. **Correction (post-review, Codex on this
PR, two rounds).** First round: an earlier draft called these six branches'
failures independent because none of the four new ones has an open PR
touching `quota_enforcement_tests.rs`, `completion_trigger.rs`, or
`execution.rs`. That is too weak a check to support "no shared diff" — an
open-PR listing says nothing about a branch with no PR yet, a branch sharing
commits with another via a common base, or a change to worker/queue/shard/
test-setup code outside those three named files.

Second round, in direct response to that correction's own admission: Codex
correctly pointed out that without a SHA check, the report could not even
support its claim that all 6 occurrences hit the new 30s bound rather than
the old 10s one. This session then fetched each of the 6 runs' actual tested
commit and checked ancestry against `56bc205` (the timeout-widening commit)
directly:

```
git fetch origin <head_sha>:refs/tmp/<head_sha>   # per run, then:
git merge-base --is-ancestor 56bc205 <head_sha> && echo YES || echo NO
```

5 of 6 (`b554b1e6` / PR #1706, `e591ee64` / PR #1707, `79f7d6a4` /
`trusting-ritchie-nml6ud`, `5d77d70e` / `confident-babbage-sl0122`,
`afb756be` / `cool-noether-7dejjb`) do contain `56bc205` and so ran the new
30s bound. The 6th, `58abe747` (`gallant-dijkstra-a83hyy`, run
`35658767845`), does **not** — `git merge-base 56bc205 58abe747` returns a
common ancestor several commits back
(`3d460681`), and that branch's own checked-out copy of
`quota_enforcement_tests.rs` at that SHA still reads the pre-widen
`wait_for_execution_state(&url, source, "COMPLETED").await` (the file's
plain 10s-default helper), confirmed by `git show 58abe747:...`. That
occurrence's timestamp (2026-09-22T19:29:31Z, after `56bc205` merged) placed
it in the post-widen window by clock time alone, which an earlier draft
relied on implicitly — a stale branch that hadn't merged `trunk-dev` recently
can still run on old code long after a fix lands elsewhere, exactly the gap
Codex's comment named.

This is a **correction, not a retraction**: 5 of 6 occurrences are now
directly confirmed at the new 30s bound (not merely timestamped after the
merge), which is if anything stronger support for "the widen didn't help"
than the original unverified claim — and the 6th occurrence, at the old 10s
bound, is consistent with this being the same longstanding flake this
series has tracked since before the widen shipped. The open-PR-based
independence claim remains downgraded regardless: six occurrences on six
differently-named branches, not confirmed to carry otherwise-unrelated
diffs. The two PRs (#1706, #1707) are independently confirmed not to touch
this path (read directly, not inferred from branch naming); the four
additional branches' own full diffs against `trunk-dev` (beyond the one
file checked above) remain an open question for the next session
(`git diff trunk-dev...<branch>` for each) before "independent" is used as
a settled fact.

**Correction (post-review, Codex on this PR): the two "clean passes" this
report cited were never real executions of this test.** An earlier draft
claimed two `Test DB (linux, shard 0)` runs — `35695812531` and
`35826846700` — "passed cleanly" in the same window, offered as evidence
this is intermittent rather than deterministic. Codex questioned comparing
against unverified revisions; checking directly (each run's own "Detect
non-docs changes" job log) shows both are **docs-only PRs**:
`35695812531` changed only `docs/audits/README.md`,
`docs/audits/cli-flag-coverage.py`, and two files under `docs/runbooks/`;
`35826846700` changed only `README.md`. Per `ci.yml`'s own docs-only design
(every expensive step in `Test DB (linux, shard 0)` is gated on
`needs.changes.outputs.code == 'true'`), neither run ever executed
`cargo test`, let alone `quota_enforcement_tests` — both jobs report
`success` because every step inside them was skipped, exactly the "a
skipped job still reports success" behavior `ci.yml`'s own header comment
documents as intentional (for branch-protection purposes, not as a
pass/fail signal for the suite itself). **Both citations are retracted.**

Searching further, this session found one **genuine** clean execution:
run `35640952522` (`claude/zealous-cannon-dsvx8a`, job `106479562749`,
`Test DB (linux, shard 0)`, 2026-09-21T19:32:50Z) — a real, non-skipped run
whose log shows `quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded ... ok`
among 41 other tests in the same suite, all passing. Checked against
`56bc205` the same way as the six failures: this run's SHA (`12f8e939`)
predates the widen (pre-widen, 10s bound). So the honest count from this
session's checking is: **1 confirmed clean pass (pre-widen, 10s bound), 6
confirmed failures (5 post-widen at 30s, 1 pre-widen at 10s), 0 confirmed
clean passes at the 30s bound** — this session did not find one. That is
weaker support for "intermittent" than the retracted claim implied, and it
leaves open whether the 30s bound has passed this test at all recently; the
next session should look specifically for a genuine (non-skipped) pass at
the 30s bound before repeating an intermittency claim. This remains a
sample of convenience, not a formal rerun-rate — the ≥20x same-commit
protocol this role's hard gate requires is still not runnable here
(no Docker).

**One more thing this correction surfaced, independent of Codex's first
comment on this point — then corrected again by a second Codex comment.**
5 of the 6 failures plus the one genuine pass landed on
`Test DB (linux, shard 0)` under the current 11-shard layout, never shard
10 — which is where the 09-21 report's own shard-collision finding placed
`quota_enforcement_tests` (`43 % 11 = 10` at the time). Checked directly
against today's manifest: `quota_enforcement_tests` is now row 44 among
`linux`-osclass rows (`awk '$1=="linux"{print c": "$0; c++}' .github/ci/integration-suites.txt`),
and `integration_e2e` is row 33 — `44 % 11 = 0` and `33 % 11 = 0`. The
manifest gap between them is still 11 rows (unchanged from the 09-21
report's finding), but both row numbers shifted by exactly 11 since then
(the manifest grew), moving the collision from shard 10 to shard 0 — the
same tracked, already-being-fixed defect PR #1707 targets (its own title:
"Rebalance test-db-linux from 11 to 21 shards" /
`fix-shard-0-collision-rebalance-1685`).

**Correction:** an earlier draft of this paragraph said "every occurrence…
landed on the shard carrying both heavy suites," which is wrong for the 6th.
PR #1707's own occurrence (`35813687699`) ran under *its own* proposed
21-shard layout, not the current 11-shard one: `44 % 21 = 2` for
`quota_enforcement_tests`, `33 % 21 = 12` for `integration_e2e` — **different
shards**, matching the table above (`Test DB (linux, shard 2)`) and matching
that PR's own explicit design goal of separating them. So that occurrence is
not explained by this collision at all — if anything it is evidence
*against* the collision being the flake's cause: PR #1707's rebalance
already isolates `quota_enforcement_tests` from `integration_e2e`, and the
SOURCE-completion panic still fired. Restated at the confidence this
evidence actually supports: 5 of 6 occurrences co-occur with the current
11-shard collision (consistent with, not proven caused by, that shard's
outsized duration — sequential execution still rules out literal
concurrent contention, per the 09-21 report); the 6th occurred with the
collision already fixed, which weakens rather than strengthens the case that
the collision explains this flake.

### The 30-second timeout widen (PR #1673, merged 2026-09-21T19:18Z UTC) did not fix this, and its own commit message says the flake was already known and deliberately not root-caused

`quota_enforcement_tests.rs`'s wait for this exact assertion was widened from
the file's usual 10s default to a caller-supplied 30s bound in `56bc205`
(PR #1673, "Fix: backup verify adjudicates lost cross-shard completion-trigger
fires"), whose second commit message states directly: *"This test's own diff,
and completion_trigger.rs's quota-exceeded-to-outbox path, are unchanged from
trunk-dev in this PR -- the flake predates and is unrelated to the fire-verify
work here."* The diff itself confirms: only the timeout argument changed
(`wait_for_execution_state` → `wait_for_execution_state_with_timeout(..., 30s)`),
no assertion, no test logic, no product code in the same hunk.

**This is exactly the "raised timeout as a fix" pattern this role's charter
bans**, applied here to a flake that was observed but not diagnosed at the
time. The evidence this session gathered shows it did not work: 5 of the 6
occurrences above are confirmed (by direct SHA ancestry check, not
timestamp — see the correction above) to have run *at* the new, 3x-larger,
30-second bound and still lost. A timeout that is already 3x the file's own
default and still loses this often is strong evidence against "the runner is
just slow" and for
either a genuine hang (the source workflow's terminal transaction never
actually commits under some condition) or unbounded queueing (the worker's
dispatch loop never picks the task up) — both mechanism categories this
role's hard gate requires naming, and neither confirmed yet.

## 🔍 Diagnosis

**Test-vs-product verdict: not rendered.** Per this role's hard gate
(requirement 3), a fix PR cannot be opened without first showing the
nondeterminism lives in the test rather than the product. This session did
not obtain worker-level tracing or a live repro (no Docker), so it cannot
render that verdict. What this session *did* establish, narrowing the
candidate space:

- **Not `#1706`'s mechanism on the arm's ordinary path — but a related,
  undismissed candidate survives review.** The same-shard `QuotaExceeded`
  arm's happy path (`completion_trigger.rs:2078-2100`) falls through to the
  outbox and metrics recording without touching the source's transaction.
  **Correction (post-review, Codex on this PR, two rounds — this bullet had
  the same error as the census section above and was missed in the first
  sweep).** An earlier draft of this report stopped at line 2100 and claimed
  the whole arm never propagates `Err`. Wrong — the outbox-row insert
  immediately after (`completion_trigger.rs:2101-2122`) ends
  `.map_err(crate::error::database_error)?`, and per the arm's own doc
  comment this whole block runs inline inside the source's terminal
  transaction. A database error on that specific insert would propagate and
  roll back the source's own `WorkflowCompleted` append — the exact pre-fix
  symptom this test exists to catch, on a narrower trigger than the original
  bug (an insert-time error, not every quota-exceeded evaluation). A second
  round then caught that "pool exhaustion" was still listed here as an
  example cause, unchanged from the first mistake: this insert runs on an
  already-acquired `&mut AsyncPgConnection` (`completion_trigger.rs:
  1487-1488`) with no new pool checkout, so pool exhaustion cannot be *this
  insert's* error — corrected to the causes that connection can actually hit
  (a constraint violation, a dropped connection, a statement timeout, or a
  serialization/deadlock error under concurrent write load). Not confirmed
  as what actually happened in any of the 6 occurrences (no DB-error
  telemetry captured), but it is a concrete, previously-unconsidered
  mechanism the next session should check for (e.g. Postgres server-side
  error logs around each occurrence's timestamp) before assuming a pure
  hang.
- **"CI is just slow" is weakened but not ruled out — corrected back from an
  overclaim.** An earlier draft of this bullet said the bound is 3x default
  and "still loses regularly," treating that as evidence against runner
  slowness. **Correction (post-review, Codex on this PR):** the test's own
  source code (`quota_enforcement_tests.rs:3726-3729`) shows `source` is
  enqueued via `start_root` — a direct DB insert — *before* the worker is
  even built and spawned; only then does `wait_for_execution_state_with_timeout`
  start its 30s clock. So that clock covers worker startup and this task's
  claim latency, not only the synchronous trigger-evaluation cycle this
  report's earlier draft described as the whole budget. Under a genuinely
  CPU-starved runner, slow worker startup or delayed task claim could by
  itself consume a meaningful share of 30s, independent of anything in
  `completion_trigger.rs`. This does not favor runner slowness over a
  product-side hang — it only means this report cannot use "30s is way more
  than the decision cycle needs" to rule slowness out. Both remain open,
  undismissed candidates; no timing or tracing data from any of the 6
  occurrences exists to weigh between them.
- **No timing correlation with PR #1673 — retracted.** **Correction
  (post-review, Codex on this PR):** an earlier draft of this bullet said the
  occurrence spike "begins shortly after `56bc205` merged" (2026-09-21T19:18Z),
  framing that commit as a timing suspect. That does not survive this
  report's own corrected baseline: 2 of the occurrences this series has
  documented for this exact signature — `35563198153`
  (2026-09-21T05:04:24Z) and `35576291757` (2026-09-21T08:07:53Z) — happened
  11-14 hours *before* `56bc205` merged, the same day. The flake predates the
  merge by this report's own evidence, so there is no spike-after-merge
  pattern to explain. Retracted as a candidate; `56bc205`'s only confirmed
  connection to this test remains the timeout widen itself (which this
  report separately shows did not fix the flake). Whether `56bc205`'s
  `execution.rs` or other `completion_trigger.rs` changes touch a code path
  this test's worker also exercises is still unchecked, but not for a timing
  reason — `git show 56bc205 -- autumn-harvest/src/execution.rs` and diff
  against what the source workflow's terminal-commit path actually calls
  remains a cheap, independent thing to check if the outbox-insert-error
  candidate above needs ruling in or out.
- **Not investigated this session, for lack of Docker:** whether the source
  workflow's task is even being dispatched at all during the hang (a queue
  visibility/claim bug) versus dispatched-and-stuck (a transaction or lock
  wait) versus never retried (a crashed worker task). Black-box waiting on
  `harvest_workflow_executions.state` cannot distinguish these; the next
  session with Docker should add ad hoc `tracing` output or query
  `harvest_task_queue` mid-hang rather than re-running the black-box wait.

## 🔧 Treatment

None. Per the hard gate, this is correctly a health report, not a fix PR:
no rerun-rate measurement (no Docker), no named mechanism (four candidates
above, none confirmed), no test-vs-product verdict, no before/after
measurement. **Explicitly not recommended:** widening the timeout further.
It is already at 3x default, was widened once for this exact flake already
(citing a single observed occurrence at the time), and 5 of the 6 fresh
occurrences this report found happened at that new, larger bound — a 4th
widen would be pure timeout-bump theater, the exact pattern this role exists
to stop.

**Priority for the next session with Docker or live-CI-dispatch access:**

1. Reproduce this specific panic (not the outbox-retry one #1706 targets)
   using PR #1706's own successful method for the *other* signature: CPU
   oversubscription (its report used 8x on a 4-core box) + N≥20 repeated
   runs of `completion_trigger_defers_to_outbox_when_target_quota_exceeded`
   against unmodified `trunk-dev`. If it reproduces under stress, that is
   this role's Tier-1 rerun-rate evidence and unblocks a real diagnosis.
2. If it reproduces, add tracing (or a direct `harvest_task_queue` /
   `harvest_workflow_executions` query mid-wait) to distinguish "task never
   dispatched" from "dispatched and stuck" from "worker crashed" — the
   black-box wait this test currently does cannot tell these apart, and
   guessing further from outside is not evidence.
3. Diff `56bc205`'s `execution.rs` and remaining `completion_trigger.rs`
   hunks against what this test's worker path actually calls, to check
   (not assume) the commit message's "unrelated" claim.
4. Check whether any of the 6 occurrences hit the outbox-insert error path
   named in Diagnosis (`completion_trigger.rs:2101-2122`'s `.map_err(...)?`
   — a constraint violation, statement timeout, dropped connection, or
   serialization/deadlock error on the connection already in hand, not pool
   exhaustion, per the second correction above) rather than a pure hang —
   Postgres server-side error logs around each occurrence's timestamp, if
   retained, would settle it directly; this session had none.
5. Diff each of the 4 additional branches (`trusting-ritchie-nml6ud`,
   `gallant-dijkstra-a83hyy`, `confident-babbage-sl0122`, `cool-noether-7dejjb`)
   against `trunk-dev` before repeating this report's "independent branches"
   framing as settled — this session checked only the open-PR listing, which
   Codex review on this PR correctly flagged as too weak to support that claim.
6. Both #1706 and #1707 are otherwise complete, reviewed multiple times, and
   blocked on this same panic, which is confirmed to **predate** both PRs
   (occurrences on `trunk-dev`-based branches with neither PR's diff, before
   either PR existed). **Correction (post-review, Codex on this PR):** an
   earlier draft of this item told mergers not to treat the redness as a
   regression introduced by either PR. That overreaches what this session
   checked. Predating the PRs rules out either PR being the *sole* cause; it
   does not rule out either PR *changing the rate*. #1706 specifically
   activates a previously-dormant code path (`enforce_completion_triggers_outbox`
   now finds a real `sharded_pool` instead of no-op'ing on a missing one),
   which is new DB work on every poll tick that did not happen before —
   unexamined as a possible contributor to load/timing on this *other*
   test. #1707 changes shard composition and total shard duration by
   design. Neither was compared against its own base under equivalent
   conditions (e.g. a same-commit rerun count on the PR vs. on `trunk-dev`
   at the PR's parent commit) — this session did not do that comparison, so
   it cannot rule a rate change in or out. Mergers should treat this as
   **unresolved**, not cleared: the panic is not *new* to either PR, but
   whether either PR makes it *worse* is an open question the next session
   should check with a rerun comparison, not assume away.

## 📊 Measurement

- **Before:** none — no rerun protocol executed.
- **Symptom count:** 6 confirmed occurrences of the identical panic
  (`integration_e2e.rs:1383:6`, reached from this test's wait for the
  source's `COMPLETED` state — the 30s bound for 5 of the 6, confirmed by
  SHA ancestry against `56bc205`, and the original 10s bound for the 6th),
  across 6 differently-named branches, not confirmed independent beyond
  that, 2026-09-22T07:15Z–2026-09-23T09:00Z. **Correction (post-review,
  Codex on this PR):** up from **2**, not 1, occurrences of this specific
  site in the 09-21 report — that report's item 2 (run `35576291757`) and,
  missed by an earlier draft of this report, its item 4 (run `35563198153`'s
  shard-10 cascade, which the 09-21 report itself names as including "the
  exact test item 2 tracks... `completion_trigger_defers_to_outbox_when_target_quota_exceeded`"
  among its ~70 failures at the identical `integration_e2e.rs:1383:6` site).
  8 total confirmed occurrences across both reports, not 7.
  **Correction (post-review, Codex on this PR, second round):** the "2
  clean shard-0 passes" bounding the window were retracted — both were
  docs-only PRs whose test steps never ran (see the census section above).
  This session found exactly 1 genuine clean pass (pre-widen, 10s bound) and
  0 genuine clean passes at the 30s bound. This count remains a sample of
  convenience, not a formal rerun-rate.
- **After:** N/A — no fix attempted.
- **Revert check:** N/A — no fix attempted.
- **Ledger:** no quarantine ledger exists in this repository (checked again).

## 🔬 Reproduce

```sh
# Docker check (unchanged gap, re-verified today):
docker ps
# -> failed to connect to the docker API at unix:///var/run/docker.sock:
#    connect: no such file or directory

# The two fix PRs already open against this test family:
# search_pull_requests(query="repo:autumn-foundation/autumn-harvest head:claude/fix-quota-outbox-scanner-sharded-pool-1685")
# -> PR #1706
# search_pull_requests(query="repo:autumn-foundation/autumn-harvest head:claude/fix-shard-0-collision-rebalance-1685")
# -> PR #1707

# Each PR's own CI still failing on quota_enforcement_tests:
# pull_request_read(method="get_check_runs", pullNumber=1706) -> "Test DB (linux, shard 0)" conclusion=failure
# pull_request_read(method="get_check_runs", pullNumber=1707) -> "Test DB (linux, shard 2)" conclusion=failure
# get_job_logs(job_id=107008321702, return_content=false) -> signed URL; curl + grep:
grep -n "panicked at\|quota_enforcement_tests::.*FAILED" pr1706_shard0.log
# -> completion_trigger_defers_to_outbox_when_target_quota_exceeded panicked
#    at integration_e2e.rs:1383:6
# Same for job 107035722407 (PR #1707).

# Today's window census (90 pull_request/completed runs since the 09-21
# report's cutoff):
# actions_list(method="list_workflow_runs", resource_id="ci.yml", perPage=100, page=1)
python3 -c "
import json
d = json.load(open('<saved actions_list response>'))
runs = [r for r in d['workflow_runs'] if r['event']=='pull_request' and r['status']=='completed']
cutoff = '2026-09-21T10:18:51Z'
window = [r for r in runs if r['created_at'] >= cutoff]
from collections import Counter
print(len(window), Counter(r['conclusion'] for r in window))
"
# -> 90 (61 cancelled, 26 failure, 3 success)

# The 4 additional shard-0 failures, each job-logged the same way:
# get_job_logs(run_id=<id>, failed_only=true, tail_lines=15) to find the
# failing job name, then get_job_logs(job_id=<id>, return_content=false) for
# the signed URL, curl + grep "panicked at":
#   35652570063 (job 106647193659) claude/trusting-ritchie-nml6ud
#   35658767845 (job 106896752815) claude/gallant-dijkstra-a83hyy
#   35788942436 (job 106960979043) claude/confident-babbage-sl0122
#   35835324763 (job 107108884704) claude/cool-noether-7dejjb
# -> all 4: identical panic, integration_e2e.rs:1383:6

# RETRACTED (Codex review correction): the two "clean shard-0 passes" this
# report first cited were docs-only PRs whose test steps never ran --
# confirmed by reading each run's own "Detect non-docs changes" job log for
# its changed-files list, not just its conclusion:
# get_job_logs(job_id=106642535687) -> Changed files: docs/audits/README.md,
#   docs/audits/cli-flag-coverage.py, docs/runbooks/harvest-alerts.md,
#   docs/runbooks/nondeterminism-block.md (run 35695812531)
# get_job_logs(job_id=107070415376) -> Changed files: README.md
#   (run 35826846700)
# Both match ci.yml's docs-only filter, so needs.changes.outputs.code=false
# and every step in "Test DB (linux, shard 0)" was skipped -- "success"
# there means "skipped cleanly," not "ran and passed."

# The one GENUINE clean pass this session found instead:
# get_job_logs(job_id=106479562749, return_content=true) -> full log shows
grep -n "quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded" zealous_cannon_shard0.log
# -> "... ok" at 2026-09-21T19:32:50Z (run 35640952522, shard 0, a real
#    non-skipped execution). SHA ancestry check (below) confirms pre-widen.

# The shard-0 manifest collision under the CURRENT 11-shard layout (same
# defect PR #1707 already targets, now at shard 0 not shard 10) -- true for
# 5 of the 6 occurrences:
awk '$1=="linux"{print c": "$0; c++}' .github/ci/integration-suites.txt | grep -n "integration_e2e\|quota_enforcement_tests"
python3 -c "print(33 % 11, 44 % 11)"   # -> 0 0

# PR #1707's own 21-shard layout does NOT collide these two suites --
# confirmed, correcting an earlier draft's "every occurrence" overclaim:
python3 -c "print('quota_enforcement_tests:', 44 % 21, ' integration_e2e:', 33 % 21)"
# -> 2 12 -- different shards, matching that run's own "Test DB (linux,
#    shard 2)" job name. The 6th occurrence is not explained by this
#    collision.

# The timeout widen and its own commit message disclaiming a fix:
git log -S "A wider bound than the usual 10s default" --oneline -- autumn-harvest/tests/integration/quota_enforcement_tests.rs
# -> 56bc205 (#1673)
git show 56bc205 -- autumn-harvest/tests/integration/quota_enforcement_tests.rs
# -> only the wait_for_execution_state -> wait_for_execution_state_with_timeout(30s)
#    change; commit message: "This test's own diff, and
#    completion_trigger.rs's quota-exceeded-to-outbox path, are unchanged
#    from trunk-dev in this PR -- the flake predates and is unrelated to the
#    fire-verify work here."

# #1706's own mechanism (same-shard QuotaExceeded arm) on its happy path
# never aborts the source's transaction -- but the outbox insert right after
# it can, on a DB error (Codex review correction, see Diagnosis):
sed -n '2078,2122p' autumn-harvest/src/completion_trigger.rs
grep -n "fn evaluate_triggers_for_execution_collecting_with_codecs" -A2 autumn-harvest/src/completion_trigger.rs
# -> conn: &'a mut diesel_async::AsyncPgConnection -- already-acquired, no
#    new pool checkout happens inside this function, so a DB error on the
#    outbox insert is not "pool exhaustion" (Codex's 2nd-round correction).

# SHA-ancestry verification (Codex's correction: timestamps alone don't
# prove a branch ran the widened 30s bound):
for sha in <run's head_sha>; do
  git fetch origin "$sha:refs/tmp/$sha"
  git merge-base --is-ancestor 56bc205 "$sha" && echo "$sha: post-widen (30s)" || echo "$sha: pre-widen (10s)"
done
# -> 5 of 6 (b554b1e6/#1706, e591ee64/#1707, 79f7d6a4/trusting-ritchie,
#    5d77d70e/confident-babbage, afb756be/cool-noether): post-widen.
#    58abe747/gallant-dijkstra-a83hyy: pre-widen -- confirmed directly:
git show 58abe747:autumn-harvest/tests/integration/quota_enforcement_tests.rs | grep -n "wait_for_execution_state(&url, source"
# -> still the plain 10s-default call, not wait_for_execution_state_with_timeout
```
