# Opportunity #1 — Official TypeScript/Python SDKs (issue #955): discovery-gate check

**Verdict: hold.** Not a kill of the underlying job — a kill of #955's current
readiness to enter a build queue. It fails the discovery hard gate on every
one of its six required elements, and the accessible record contains no
admissible evidence for the demand it asserts. A probe is registered below;
nothing ships against this opportunity until it runs.

## 🎯 Struggling moment (as currently sourced)

**Source:** issue #955 itself (labels `spec`, `pm`), authored as a complete
build spec — acceptance criteria, a complexity-tier estimate ("M"), and a
competitor gap table — with no interview, workaround count, or usage
citation behind it. Per this process's own admissibility rule, a feature
request authored this way is data about the author's solution imagination,
not about an excavated struggle. Restated in the issue's own words: "Every
non-Rust team re-invents that glue, badly, and each re-invention is a
support burden and an adoption tax" and "'Is there an npm/PyPI package?' is
the first question every polyglot evaluator asks." Both are asserted, not
cited. No switch-timeline interview, workaround census, or non-consumption
map is referenced or attached anywhere in the issue, its comments, or the
parent roadmap (#968, which lists it as Milestone 6 breadth-over-depth
without additional evidence).

**Segment named:** "non-Rust caller (TypeScript service or Python platform
team) integrating with an embedded Harvest instance." No such caller is
named, quoted, or counted anywhere in this repository's accessible record.

## 🧲 Job statement — status: **not excavated**

A job statement requires reconstruction from switch-timeline interviews and
behavioral evidence, with the four forces documented. None exist for this
backlog item. The nearest artifact is the issue's own user story ("As a
non-Rust caller... I want official, published client SDKs... so that I can
start, signal, update, and await workflows... in minutes, not days") — this
is a job statement *authored at a whiteboard*, the exact pattern this
process treats as a hypothesis wearing a conclusion's clothes, not a passed
gate item. It is carried forward here as the hypothesis to test, not as a
finding:

> *Hypothesized* — When a non-Rust team needs to start, signal, or await a
> Harvest workflow from a TypeScript or Python codebase, they want a typed
> client with the lifecycle ergonomics handled for them, so they can ship
> the integration without hand-deriving HTTP semantics Harvest already
> documents.

Four forces: unknown. No interview has asked what pushes a team off their
current approach, what pulls them toward an official SDK specifically (vs.
generating one themselves), what they're anxious an official SDK would cost
them (a new dependency, a lag behind the Rust engine's release cadence), or
what habit of the old way they'd have to break. All four are guessable;
none are evidenced.

## 📈 Demand evidence

**Tier 1 (behavioral record) — swept, not found.** A full-corpus search of
this repository — issues, merged and open PRs, source comments, and
docs — for any workaround-census signal (hand-rolled non-Rust HTTP clients,
"how do I call Harvest from Python/TS" questions, complaints about the raw
contract) returned zero hits. Every `curl` reference found (40 code-search
matches) is first-party documentation or example code demonstrating the
management API, not a user-authored workaround or complaint. Search terms
covering non-Rust integration pain, hand-written clients, and SDK requests
matched **0** issues beyond #955 itself. This repository is the engine's own
issue tracker, not a support desk or sales-call archive — a `0` here is
*absence of evidence in the only corpus available to this process*, not
proof of absence; see the ledger's methodology note. It is nonetheless the
totality of what this run could check, and it found nothing.

**Tier 2 (market record) — not gathered.** No switcher-to-Harvest interview
exists citing SDK availability as a reason for adopting or rejecting
Harvest.

**Tier 3 (created evidence) — not run.** No fake door, concierge, or
landing-page probe has been executed for this job. See Pre-registration
below for the probe this entry proposes before any further discovery step.

**A gap in the spec's own analysis, found on inspection:** #955 compares
itself only against a hand-rolled raw-HTTP baseline and against five
competitors' *shipped SDK* offerings. It does not mention that this
project's own OpenAPI 3.1 spec (issue #694) already **shipped** (closed,
merged) before #955 was filed. That spec makes today's actual cheapest
current hire not "hand-write raw HTTP" but "run `openapi-generator` (or
`openapi-typescript`) against the already-served/published OpenAPI
document ([`docs/openapi.json`](../openapi.json), per
[`docs/openapi.md`](../openapi.md)) once, locally, for free." Nothing in
#955 measures whether teams have tried that path and
found it insufficient, or whether they've tried it at all. A demand case
for a hand-maintained ergonomic layer that never engages with the free
self-serve alternative it would have to outcompete is not yet a demand
case — it is a solution preference stated without a comparison to its own
nearest substitute.

## 🔁 Current hire

At least two candidates compete for this job today, and #955 evidences
neither:

1. **Hand-rolled raw HTTP against `docs/api-contract.json`** — the baseline
   #955 assumes. Cost, frequency, and prevalence unmeasured.
2. **Self-serve codegen off the shipped OpenAPI spec (#694)** — cheaper than
   either raw HTTP or a hand-maintained official SDK, available today, and
   not acknowledged in #955 at all.
3. **Non-consumption** — no non-Rust integration attempted yet, because no
   named non-Rust adopter has been identified. #955's own "Out of Scope"
   section implicitly concedes this is aimed at a future adoption funnel
   ("meet polyglot callers... the market is won") rather than a presently
   observed switching cost, which is exactly the red-flag case this
   process asks for extraordinary evidence before accepting: betting
   against non-consumption on zero named adopters.

Whichever of these is actually hired, #955 ships something that fires it —
and right now nobody has checked which one it is, or whether anyone has
hired anything at all.

## ⚖️ Pre-registration (RED — committed before any probe runs)

**Probe:** two parts, run together, both honest fake-door/solicitation
instruments (no dark patterns, no money collected, immediate honesty that
the SDKs do not exist yet):

1. **Workaround census by direct solicitation.** Open a pinned GitHub
   Discussion, linked from the top-level `README.md` and
   `docs/management-api.md`: *"Calling Harvest from TypeScript or Python
   today? Tell us what you built to do it, and what specifically got
   awkward."* Count distinct responding organizations/teams (self-identified,
   not anonymous reactions) that describe an existing hand-rolled
   integration or a documented pain point.
2. **Self-serve-path signal, via a distinguishable action, not page
   traffic.** Add one line to `docs/openapi.md`'s existing "Generate a typed
   client in under ten minutes" section and to `docs/management-api.md`,
   pointing at the commands that section already documents and verifies —
   `openapi-typescript` / `openapi-python-client` against the served
   `GET {api_path}/openapi.json` endpoint, or the offline
   [`docs/openapi.json`](../openapi.json) copy for review diffs (**not**
   [`docs/api-contract.json`](../api-contract.json), which is Harvest's
   pre-OpenAPI canonical contract that `docs/openapi.json` is generated
   from, not an OpenAPI document itself, and not something any OpenAPI
   generator can read) — with a direct call to action: *"Tried this? Tell
   us in [the same Discussion] whether it covered your case, or where it
   fell short."* Count replies to that specific prompt as the
   revealed-preference signal. A reply is a distinguishable, attributable
   event; repository page-view or clone counts are not, since nothing on
   this repository's traffic dashboards can attribute a view of that page
   to this newly added line rather than to any other reason someone opened
   it — an unattributable count is not evidence, whatever its size.

**Segment:** teams or organizations embedding Harvest that self-identify as
primarily non-Rust in the Discussion thread.

**Window:** 60 days from the Discussion being linked live from `README.md`.

**Line — pursue:** ≥5 distinct non-Rust orgs/teams report an existing
hand-rolled integration, **or** ≥3 report having tried self-serve OpenAPI
codegen and abandoned or struggled with it over a specifically named gap
this spec lists (long-poll result waiting, typed error branching,
idempotency-key handling, signal-with-start/update-with-start semantics).

**Line — kill:** fewer than 2 such reports in the 60-day window despite live
linkage from `README.md` and `docs/management-api.md` (i.e., real
visibility, not a buried page).

**If it later ships anyway** (build criteria, for whoever runs this probe
and clears the line): kill criteria at the registered endpoint review are
**<10 distinct installing orgs across both packages combined within 90 days
of publish**, or **<30% of first-importers still importing a version at the
90-day mark** — either misses, sunset with a migration note pointing back
at the OpenAPI-codegen self-serve path, which never goes away regardless of
this feature's fate.

## 💸 Agency tax (recorded now so a future "pursue" carries it forward, not resets it)

Two packages published to two public registries (npm, PyPI), forever;
a versioning-compatibility matrix to keep true against every future Harvest
release; a live-instance CI contract suite (testcontainers Postgres + a demo
app) added to the release pipeline and gating every future release on two
more moving parts; published-package supply-chain surface (npm/PyPI
credentials, provenance, a new axis Warden and Ballast both inherit);
quickstart docs to keep current in two more languages than today. This is a
real, ongoing bill — it is the reason the hard gate exists before anyone
schedules the M-tier build.

## 🏁 Verdict

**Hold**, against the registered gate above, not against a probe result
(none has run). #955 stays out of any build milestone until the probe's
line clears in the registered segment and window. If the workaround-census
Discussion or the self-serve-codegen signal instead show the job is already
adequately served by the shipped OpenAPI spec, the correct next step is a
formal kill of #955 with this entry as the citation, not silent archival.

## 🔬 Reproduce

- Workaround-census sweep run for this entry: `search_issues` /
  `search_code` (GitHub) over `autumn-foundation/autumn-harvest` for terms
  covering non-Rust integration pain, hand-written HTTP clients, and SDK
  requests, plus a manual review of every `curl` occurrence in the corpus
  (40 matches, all first-party docs/examples). Zero independent-workaround
  or complaint hits beyond #955 itself.
- Probe configuration: as pre-registered above. Whoever runs it should link
  the Discussion from `README.md` and `docs/management-api.md` in the same
  change that opens it, so the 60-day window and the "real visibility" kill
  condition both start from a verifiable commit.
