# DESIGN-2579: let a live re-probe count in selection

Issue #2579. Parent: #2369. Follows #2380 and #2577. See
[`DESIGN-2380.md`](DESIGN-2380.md) and [`DESIGN-2577.md`](DESIGN-2577.md).

## Problem

A probe can stub a tool call. The run then re-probes the overlay live on a
fork (#2378). Before this change, the live end stayed in
`ProbeRecord::live`, in the report only. `ProbedCandidate` did not hold it.
So the composer, the selector and the rule pick could not tell a live
failure from a live success. Sequential mode carried each composition,
whatever its live end.

## Decision

The issue gives two options. This design takes option 1: the live end goes
into `ProbedCandidate`, `AnonymisedEvaluation`, the rule order and the
compose context.

### The live end of a candidate

`ProbedCandidate::live` holds the `LiveReprobe` of the probe.
`AnonymisedEvaluation::live` holds an `eval::LiveCheck`. That is the live
end with no identity:

| `LiveReprobe` | `LiveCheck` | A live failure |
|---|---|---|
| `NotNeeded` | `NotNeeded` | no |
| `Skipped` | `Skipped` | no |
| `Failed` | `NoEnd { error }` | no |
| `Ran`, completed | `Completed` | no |
| `Ran`, not completed | `Failed { error }` | yes |

`LiveCheck` holds no fork id, no token count and no cost. A price names a
model. Each error text loses each identity of the pool, as a run end does.
The pool text shows the live end of each candidate on one line.

Only a fork that ended and did not complete is a live failure. A fork that
did not end in time is one. A failed fork or wait is not. It says nothing
about the overlay, so it counts as an unknown end, as a skip does.

### The rule order

The kernel `refine::rule` orders the candidates:

1. A live failure has no key, so the rule never picks it.
2. A completed replay end comes before an end that did not complete.
3. Fewer stubbed tool calls come first.
4. The lowest index comes first. The pool lists the candidates in id
   order, so this is the lowest id.

With no key in the pool, the rule picks none. The select step and the
fallback of the run use the same kernel.

### The selector

The selector reads the live end in `SelectContext::report` and in the text.
A pick of a live failure is refused, as a pick of an id outside the pool
is. The rule pick then stands, with a note. When each candidate failed live,
the select step asks no selector. It picks none, with a note.

### Sequential mode

A composition whose live re-probe failed does not become the current
overlay. The next step starts from the overlay before it. The stage notes
say so.

### Replay of a run in flight

The rule fallback and the sequential carry run in the workflow. The patch
`agent_refine_live_rule` gates both. A run that started before the change
keeps the old rule on replay. The select step is an activity, so its new
rule needs no patch.

`ComposeRequest` and `SelectRequest` hold the new field. A worker matches
an activity by name and not by input, so a run in flight replays. A
request of an older release decodes with no live end.

## Brainstorm

| Question | Options | Choice |
|---|---|---|
| Which option | 1: carry the live end. 2: drop a live failure from the pool. | 1. The composer can learn from a failed proposal. Option 2 hides it. |
| What a live failure is | Each end that is not `Completed`. `Ran` and not completed. | `Ran` and not completed. A failed fork says nothing about the overlay. |
| The rank of a live failure | Last in the order. Out of the pick. | Out of the pick. An overlay that failed with real tools must not reach the gate. |
| A selector pick of a live failure | Keep it. Refuse it. | Refuse it. An auto-adopt policy can take the pick live with no person. |
| A pool with no key | Pick the best failure. Pick none. | Pick none. The run then keeps its current overlay. |
| What the pool shows | The whole `LiveEnd`. A new type with no identity. | A new type. The fork id and the cost are identities. |
| Where the kernel lives | In `compose.rs`. In its own module. | Its own module, `refine/rule.rs`, with a Verus proof. |
| Sequential mode | Carry each composition. Keep the overlay before it. | Keep the overlay before it, as the parallel rule does. |
| A run in flight | No gate. A patch. | A patch. The workflow decides the fallback and the carry. |

## Reverse brainstorm

How can the change pick a bad overlay, leak a name or break a replay?
Each failure has a guard.

| Failure | Guard |
|---|---|
| The rule picks a live failure. | The kernel gives it no key. Verus proves that a keyless entry never wins. |
| A selector picks a live failure. | `run_select` refuses the pick and keeps the rule pick. |
| Sequential mode carries a live failure. | The carry checks the live end of the probe. |
| The fallback rule of the run picks a live failure. | `rule_of` uses the same kernel. |
| A fork error names a model in the pool. | The pool redacts each identity in a live error. |
| The fork id or the cost reaches the pool. | `LiveCheck` has no such field. |
| A flaky fork drops a good overlay. | A failed fork is `NoEnd`, not a live failure. |
| Each candidate failed live, and the run spends on a selector. | The select step asks no selector when the rule has no pick. |
| An old run replays with a new decision. | The patch gates the workflow code. |
| An old request does not decode. | `live` is optional, with a serde default. |
| The pool order of the rule changes. | With no live failure, the key gives the old order. A unit test and the proof check it. |

## Six hats

- **White.** `LiveReprobe` already holds the live end. Only the Postgres
  host forks. SQLite skips each re-probe, so a test needs a scripted
  `RefineSources`. `rule_pick` and `rule_of` held two copies of one rule.
- **Red.** A refined overlay that failed with real tools goes live. That
  is the worst outcome here, so the refusal is a hard rule and not a weight.
- **Black.** A real overlay can fail live for a reason outside it, such as
  a down tool. The run then keeps its current overlay. That costs one
  refine round, and it is safe. A selector loses a choice it had before.
- **Yellow.** The composer reads the live ends, so it can avoid a failed
  edit. One kernel now holds the rule, with a proof.
- **Green.** A later change can weigh `Completed` above `Skipped`. The key
  has room for that.
- **Blue.** Spec and proof first, then red tests, then the code, then the
  gates and a review from several angles.

## Tests

- `refine_run.rs`: a scripted `RefineSources` fails the live re-probe of
  some overlays.
  - The rule picks another composition, and the selection changes.
  - A selector pick of a live failure is refused, with a note.
  - With each composition failed live, the run picks none and asks no
    selector.
  - Sequential mode keeps the overlay before a live failure.
  - The composer reads the live end of each candidate.
- `refine/rule.rs` unit tests and a proptest: the kernel against a model.
- `eval/anonymise.rs` unit tests: the live line of the text, the redaction
  of a live error, and the decode of an entry with no live end.
- `formal/verus/src/refine_rule.rs`: a live failure never wins, the winner
  has the lowest key, and a completed end comes first.
