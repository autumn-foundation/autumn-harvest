## Agents — The proposer contract (issue #2379, epic #2369)

The new `refine` module turns evidence into a candidate overlay. An app
implements `OverlayProposer` for its provider. `ModelProposer` implements
it over any `AgentModel`, with default INV and DOM templates. An app can
replace each template. `AgentHarness::proposer` installs a proposer, and
`AgentHarness::proposer_timeout` sets the budget of one call (at most 13
minutes).

The activity `agent_refine_propose` runs one stage. It asks the proposer
up to `ProposalAttempts` times (1 to 8, default 2). It checks each reply
with `overlay::apply` under the ceilings of the run. The next attempt gets
the last rejection. The stage returns `Proposed(overlay)`, `KeptBase` or
`FellBack { error }`. A fallback inherits the current overlay. A retryable
proposer error retries the whole stage. The retries run inside the
activity, so one stage is one recorded result. `Ceilings` now encodes as
JSON. `activities()` and `sqlite::register` add the new activity.

The default templates adapt `harness2/core/prompts.py` at commit
`1cfe254e8d25b15f37048c3730134686abe60335` under Apache-2.0.
`autumn-harvest-agent/NOTICE` and `third_party/harness2/` hold the
attribution and the license.

No event, migration or SQL changes. The design is in
`docs/design/DESIGN-2379.md`.

Tests: the unit tests in `refine/propose.rs` cover each criterion of the
issue with a scripted proposer. `tests/refine_propose.rs` aborts a child
process inside a DOM retry and resumes the run on SQLite. A call counter
shows that the completed INV stage is never asked again. History holds one
result per stage, and replay asks no proposer.
`formal/verus/src/agent_propose.rs` mirrors `next_step`. It proves that a
stage uses 1 to `max` attempts, falls back exactly when every attempt is
invalid, and never asks again after a valid reply.
