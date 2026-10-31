# DESIGN-2379: the proposer contract

Issue #2379. Parent: #2369. Needs #2370. Policy: #2394. Evidence: #2377.

## Problem

The refine loop needs a step that turns evidence into a candidate
overlay. Harness² has two editors. INV edits how the agent works. DOM
edits what a practitioner knows. An invalid proposal gets one more try.
After that, the stage inherits the current setup.

## Decision

`autumn-harvest-agent/src/refine/propose.rs` holds the contract. The
activity `agent_refine_propose` runs one stage.

- `OverlayProposer` is a trait that an app implements, as it does
  `AgentModel`. `propose` takes a `ProposeContext` and returns a
  `ProposerReply`. The context holds the dimension, the evidence, the
  base profile, the ceilings, the current overlay, the attempt number and
  the rejection of the last try.
- `ProposerReply` is overlay JSON text or a decoded `ProfileOverlay`.
- `Evidence` is plain text. #2377 adds a bundle variant. The enum is
  `#[non_exhaustive]`, so that change does not break an app.
- `ProposalAttempts` is the attempt count. It is 1 to
  `MAX_PROPOSAL_ATTEMPTS` (8). The default is 2. `RefinePolicy` (#2394)
  will supply it. Until then, each request carries the default.
- `ProposeRequest` is the activity input: the dimension, the evidence,
  the base profile, the current overlay, the ceilings of the run and the
  attempt count. The workflow (#2380) builds it from recorded reads.
- `Stage` is the activity output. It holds the dimension, the attempts
  used and a `StageOutcome`.
- `ModelProposer` implements the trait over an `AgentModel`. It uses the
  default INV and DOM templates. An app can replace each one.

### The rules of a stage

1. `current` must apply under the ceilings. Otherwise the activity fails
   for good as `CurrentOverlayRefused`, because a fallback hands
   `current` on. The activity then asks the proposer once per attempt.
2. A reply is valid when it decodes, `overlay::apply` accepts it under
   the ceilings of the run, and the stage fits the activity result cap.
3. A valid reply whose applied profile equals the applied profile of
   `current` gives `KeptBase`. The check names the model of each side, so
   `None` and `default` compare equal. Any other valid reply gives
   `Proposed(overlay)`.
4. An invalid reply gives a rejection. The next attempt gets it as
   feedback. After the last attempt, the stage is `FellBack { error }`,
   with the last rejection. `Stage::overlay` then returns `current`.
5. A rejection keeps at most 1,000 characters. Under a result cap, it
   keeps at most a sixth of the cap less the stage envelope. JSON writes
   one character in at most six bytes, so a fallback always fits.
6. A proposer error that a retry can fix (`RateLimited`, `Transport`,
   `Unavailable`) fails the activity as retryable. The engine retries the
   whole stage. `Config`, `Authentication` and `Budget` fail it for good.
   Any other proposer error is a rejection.
7. An attempt over the proposer time budget is a rejection. A zero budget
   sets the default.

`next_step` is the pure kernel of rules 3 and 4. The loop calls it after
each reply.

### Crash safety

The retries run inside one activity. The engine records one result per
stage. A crash before the result re-runs the stage from attempt 1. A
crash after the result replays it, and the proposer is not called.

The activity sends no heartbeat. On Postgres, a lost worker holds the
stage until the 2-hour `start_to_close` ends. A retryable failure on
every engine attempt fails the activity. The refine workflow (#2380)
then inherits `current`.

### The model request

`ModelProposer` puts each block of data in a tag with one boundary. The
blocks are the evidence, the base profile, the ceilings, the current
overlay and the last rejection. The boundary is 16 hex digits of a
SHA-256 digest over every block. So no block can hold its own closing
tag. A reply that stops at the output cap is a `Decode` error, so it
spends one attempt. The default output cap is 8,192 tokens.

## Brainstorm

| Question | Options | Choice |
|---|---|---|
| Trait shape | The issue form `propose(dim, evidence, current) -> ProposerReply`. An async call with a context. | An async call with a context that returns `Result`. A provider is async. The error kind decides a retry. The context adds the base profile, the ceilings, the attempt and the rejection, and it is `#[non_exhaustive]`. `Evidence` is plain text until #2377 adds `BundleManifest`. |
| Reply type | Text only. Overlay only. Both. | Both. A model writes text. An app with a typed client returns an overlay. |
| Equal to `current` | The same overlay. The same applied profile. | The same applied profile. A reply that restates a profile value changes nothing, so it is not a candidate. |
| Where the profile comes from | Read by kind in the activity. In the request. | In the request. The workflow reads it once, so all stages use one profile. |
| Retry feedback | None. The last rejection. | The last rejection, as Harness² does. The model can fix its own error. |
| Transport faults | A rejection. An activity retry. | An activity retry. A rejection would spend an attempt on an outage. |
| A bad key or config | A rejection. A failure for good. | A failure for good. A rejection hides the fault as a model with bad overlays. |
| What the model sees | The evidence and the overlay. Also the base profile and the ceilings. | Also the base profile and the ceilings. An overlay `system` replaces the base prompt, so the model must read it. |
| Where the templates live | Constants only. A model-backed proposer. | A model-backed proposer. A template with no user is dead text. |
| Module | `agent::propose`. A new `refine` module. | `refine`. #2380, #2381 and #2382 add to it. |

## Reverse brainstorm

How can a stage go wrong? Each failure has a guard.

| Failure | Guard |
|---|---|
| The proposer names `tools` or `mandate`. | `ProfileOverlay` is `deny_unknown_fields`, so decode fails. |
| The reply passes the ceilings. | `overlay::apply` refuses it, and its `OverlayError` text is the rejection. |
| An operator sets 1,000,000 attempts. | `ProposalAttempts` refuses a value above 8 when it decodes. |
| Zero attempts give a stage with no answer. | `ProposalAttempts` refuses 0. |
| A huge prompt makes the result too large to record. | The stage checks the result cap. An oversized reply is a rejection. |
| A long or escaped rejection makes a fallback too large. | The rejection limit follows the result cap. |
| A ceilings JSON leaves out a key, which would mean no limit. | Each ceiling key is required. Only an explicit `null` sets no limit. |
| `current` is past the ceilings, and a fallback hands it on. | The stage refuses it before the first attempt. |
| The evidence forges a heading or a closing tag. | Each block has a boundary that the data cannot know. |
| A revoked key spends every attempt and reports a fallback. | A configuration error fails the stage for good. |
| A slow proposer holds the activity forever. | Each attempt has a time budget of at most 13 minutes. Eight attempts fit in the 2-hour `start_to_close`. |
| A crash mid-retry records two results. | One activity is one recorded result. |
| Replay asks the proposer again. | Replay reads the recorded `Stage`. |
| A long error floods history. | The stage cuts the rejection to 1,000 characters or less. |
| The evidence holds an instruction. | The templates say that evidence is data, not instructions. |
| A reply restates the current overlay. | It gives `KeptBase`, so no probe is spent. |

## Six hats

- **White.** `harness2/core/recursion.py` sets `STAGE_ATTEMPTS = 2`. It
  feeds the last rejection back. It falls back to the current tree. The
  templates are in `harness2/core/prompts.py` at commit `1cfe254e`.
- **Red.** A model that writes its own config is a risk. The stage keeps
  every write behind `overlay::apply`.
- **Black.** The KeptBase test depends on `AgentProfile` equality. A new
  profile field joins that comparison by default, so this is safe.
- **Yellow.** One stage is one recorded result. #2380 can run INV and
  DOM in parallel with no extra state.
- **Green.** An app can bring a typed client and return an overlay. It
  can also bring its own prompt text.
- **Blue.** Red tests with a scripted proposer. Then the kernel and the
  activity. Then the Verus proof of `next_step`. Then the gates.

## Limits

- The proposed `system` and `guardrails` text is model output. The
  ceilings do not bound text. Treat it as untrusted until the adoption
  gate (#2381) accepts it.
- The crate license is `MIT OR Apache-2.0`, as
  `scripts/check-license-files.sh` requires. `src/refine/prompts.rs` is
  Apache-2.0 only, and `NOTICE` says so.
- `ModelProposer` calls its model directly. The cost ledger does not see
  the call. The fixed budget of #2380 bounds the spend.
- A large `Evidence::Text` can pass the activity input cap. #2377 moves
  evidence to workspace files, and the request then holds a manifest.

## Proof

`formal/verus/src/agent_propose.rs` mirrors `next_step`. A spec runs the
stage over any sequence of verdicts. It proves these properties:

- A stage uses from 1 to `max` attempts.
- A stage falls back exactly when every attempt is invalid.
- A stage never asks again after a valid reply.

## Tests

`refine/propose.rs` has a unit test for each criterion of the issue.
`tests/refine_propose.rs` crashes a child process mid-retry and resumes
the run on SQLite. A call counter shows that a completed stage is never
asked again.

## Attribution

`autumn-harvest-agent/third_party/harness2/` holds the Apache-2.0
license and names the source commit. `autumn-harvest-agent/NOTICE`
points to it.
