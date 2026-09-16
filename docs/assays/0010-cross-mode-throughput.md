# ⛏️ Prospect: at one shape, on one box, from one tree, how do the three modes compare? (kill: 5.58 workflows/sec against a [7.91, 71.19] validity band, ledger #10)

> Status: **measured.** The Pre-registration lives in
> [`docs/rnd/2026-09-16-cross-mode-throughput-preregistration.md`](../rnd/2026-09-16-cross-mode-throughput-preregistration.md)
> and was committed (`5624f13`) before the apparatus was built or run. Nothing
> in it has been edited since. The Apparatus, Assay, Verdict and Reproduce
> sections below were appended afterward, with the actual numbers.

## 🎯 Question

Restated from the pre-registration: at the canonical 3-activity shape, on one
box, from one tree, in one sitting, what is each mode's sustained
completed-workflows/sec — and does the Redis dispatch channel still buy a
decisive margin over plain Postgres once the control arm is healthy?

This closes the pit ledger #9 named in its own closing line, widened to three
arms and moved to the shape `docs/benchmarks.md` publishes.

## 🔬 Apparatus

[`apparatus/0010-cross-mode-throughput/`](apparatus/0010-cross-mode-throughput/).
Three arms drain the same seeded backlog of the same workflow. The same
`wf_three_activities` function is the workflow handler on every arm, because
the replay engine is backend-neutral, so only persistence and dispatch differ.

**The workload is a port by value, and that claim was checked rather than
asserted.** Six rounds of review (Codex, PR #1617) found five separate places
where this apparatus differed from the published harness it claims to
reproduce, each of which was fixed and re-verified:

| what differed | what the canonical harness does |
|:--|:--|
| handler forwarded the input to all three activities | ignores its input, passes JSON null |
| handler returned the last activity result | returns `{"ok": true}` |
| activity returned JSON null | returns `{"ok": true}` |
| seeded a ~40-byte workflow input | seeds `json!({})` |
| no `LISTEN`/`NOTIFY` on the Postgres control | calls `with_shard_notification_database_urls` |

After the fifth, the whole workload was diffed against `e2e_bench_support.rs`
field by field instead of one finding at a time: names, queue, both handler
bodies, activity results, seeded input, `workflow_info`, `activity_info` and
all thirty-odd `start_params` fields now match. Only `module`, a label, and
the run-scoped `workflow_id`, which must differ, do not.

**Three sweeps were discarded before the one reported here.** The first
measured a Redis arm that had no dispatch hints published at all, so it was
timing the reconcile sweep's recovery path. The second and third predated
workload-parity fixes above. Discarding them was cheaper than explaining them.

## 📐 Assay

Verbatim output is in
[`apparatus/0010-cross-mode-throughput/results/`](apparatus/0010-cross-mode-throughput/results/).
The `postgres` and `redis_pg` rows come from `registered-sweep.md`. The
`sqlite` row comes from `sqlite-corrected.md`, a re-run explained below;
`registered-sweep.md` still carries that arm's withdrawn 3.20, and is kept
unedited rather than rewritten.

Postgres durability this run: `fsync = off`, `synchronous_commit = off`.
Embedded durability is fixed at `journal_mode = WAL`, `synchronous = FULL`.

| arm | mean workflows/sec | per rep | valid reps | correctness |
|:--|--:|:--|--:|:--|
| `sqlite` | **3.19** | 3.19 / 3.19 / 3.19 | 3 | PASS |
| `postgres` | **5.58** | 5.60 / 5.57 / 5.58 | 3 | PASS |
| `redis_pg` | **22.07** | 21.70 / 22.16 / 22.35 | 3 | PASS |

Every repetition of every arm completed all 2,000 executions with exactly
6,000 activity runs, and every Redis repetition left an empty stream, an
empty PEL and an empty marker set.

## 🏁 Verdict

**KILL, on L1, the validity line — and the kill is this assay's own design
error rather than a finding about any engine.**

The `postgres` arm read **5.58 workflows/sec** against a registered validity
band of `[7.91, 71.19]` around `docs/benchmarks.md`'s published 23.73. The
pre-registration says an L1 kill withholds every cross-mode number from this
apparatus, so **L2 and L3 are not graded**, and the apparatus itself refuses
to print them.

**Why it failed is documented in this repository, and predates this assay.**
`docs/benchmarks.md` records why issue #941 chose a bounded closed loop over a
pre-loaded drain:

> a first implementation that pre-loaded a backlog reported 473 workflows/s on
> the middle half of the drain and 55/s over the whole of it, because claim
> cost grows superlinearly with backlog depth ... publishing it would have
> re-published #786's claim-depth curve under an end-to-end label.

This assay pre-registered a 2,000-deep pre-loaded drain and then set its
validity line against a closed-loop number. Those measure different things,
for a reason the repository had already written down. L1 existed to catch
exactly that mismatch, and it did. **The registrant should have read #941's
own note before choosing the shape.**

### What the numbers do support

The three arms all ran the same drain, so the *internal* comparison is not
affected by the shape mismatch — only the comparison to the published figure
is. But the registered lines are the registered lines, and re-reading them
now, after seeing the numbers, in the direction that lets more be claimed is
precisely the goalpost-moving ledger #6 had its verdict reverted for. **L2 and
L3 stay ungraded.** The re-charter below is the correct route.

### 🔭 Post-hoc diagnostic: the Redis margin is a function of backlog depth

Not pre-registered, run after the verdict above was fixed, and reported as a
mechanism rather than as a graded line. One repetition per cell.

| backlog depth | `postgres` | `redis_pg` | ratio |
|--:|--:|--:|--:|
| 250 | 23.65 | 22.41 | 0.95x |
| 500 | 23.90 | 22.21 | 0.93x |
| 1,000 | 14.54 | 22.53 | 1.55x |
| 2,000 | 5.63 | 22.18 | **3.94x** |

**`redis_pg` is flat within 1.4% across an eight-fold depth range** (22.21 to
22.53). `postgres` falls 4.2x over the same range. The 2,000 cell reproduces
the registered sweep closely (5.63 against 5.58, 22.18 against 22.07), so the
curve and the sweep are measuring the same thing.

This explains a result that looked contradictory earlier in the work: at a
400-workflow bring-up check the Redis arm read **0.94x** of Postgres, and at
2,000 it reads nearly 4x. Both are true, of different depths.

The mechanism is not new, and this assay did not discover it. It is the
structural asymmetry `docs/performance.md` documents and ledger #2 measured at
the claim level (18,933 against 29 claims/sec at a 10,000-row backlog): a
consumer-group read is not a function of backlog depth, and a non-indexable
`ORDER BY` is. What is new here is that the asymmetry is now visible **end to
end**, in whole completed workflows, on a deployment-shaped run — and that it
is *absent* below roughly 500 rows of depth, where the channel costs slightly
more than it returns.

**The operator-facing reading**: the Redis dispatch channel is not a general
throughput upgrade. It is insurance against backlog depth. A deployment that
never builds a deep queue gains nothing from it and pays for a second
stateful dependency; a deployment that does build one gains a lot, and gains
more the deeper it gets.

### The embedded arm, and a durability asymmetry that must be stated

`sqlite` read 3.19 workflows/sec, with the three repetitions landing within
0.07% of each other (626.06 s, 626.21 s, 626.50 s). That stability is itself
informative: the arm is bounded by a hard serial constraint, not by anything
noisy on the box.

**This arm was measured twice, and the first number was withdrawn.** A sixth
review round found that the embedded backend runs a caller-supplied callback
rather than `ActivityInfo::handler`, so the earlier fix that made `act_inert`
return the canonical `{"ok": true}` never reached this arm, which went on
persisting JSON null. The first reported figure, 3.20 workflows/sec, therefore
came from a workload that did not match the arms printed beside it. The
re-run above uses the corrected callback. The difference is 0.3%, which is
what a few bytes of payload are worth to an arm that fsyncs on every commit —
but the size of the correction is not what made it necessary.

That defect also survived the field-by-field workload diff this report
describes above, and the reason is worth recording: the diff compared the
*shared* definitions, and the embedded arm is precisely the one that does not
use the shared activity handler. A systematic check missed the one arm its own
method could not see.

**It is not a like-for-like comparison against the Postgres arms, and L3 was
registered without noticing that.** `autumn-harvest-sqlite` hard-codes
`PRAGMA synchronous = FULL` and exposes no way to change it, so the embedded
arm fsyncs on every commit. The Postgres arms ran at `fsync = off` and
`synchronous_commit = off`, the conditions `benchmarks/docker-compose.yml`
documents for the published numbers, and therefore did not fsync at all. A
large part of any gap between them is a durability difference rather than an
engine difference, and this assay cannot say how much. The apparatus now
records both settings in its own output so a later reader cannot mistake one
for the other.

### 🔁 Re-charter

Three questions this assay could not answer, each needing its own
pre-registration rather than a re-reading of this one:

1. **The cross-mode comparison at a valid shape.** A bounded closed loop at
   the published in-flight population, which is what L1 should have registered
   in the first place. This is the assay that can grade L2 and L3.
2. **A durability-matched embedded comparison.** The Postgres arms re-run at
   `fsync = on` and `synchronous_commit = on`, so the embedded arm's number is
   set beside one that pays the same fsync cost.
3. **Where the depth curve's knee actually is.** This diagnostic brackets it
   between 500 and 1,000 rows at one repetition per cell. Its position is the
   number an operator would actually use to decide whether to run Redis.

## Reproduce

```bash
redis-server --daemonize yes --port 6379 --save "" --appendonly no
cargo run --release --manifest-path docs/assays/apparatus/0010-cross-mode-throughput/Cargo.toml
```

See [`apparatus/0010-cross-mode-throughput/README.md`](apparatus/0010-cross-mode-throughput/README.md)
for the environment variables, including `ASSAY10_WORKFLOWS`, which is the
knob the depth diagnostic above sweeps.
