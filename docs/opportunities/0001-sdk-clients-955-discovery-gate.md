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

**Tier 1 (behavioral record) — swept, not found.** Two searches, both
reproducible:

1. GitHub issue/PR search, run as **four separate queries** rather than one
   — the tool's own documentation says it does natural-language matching
   rather than exact substring matching, but that claim is the tool
   vendor's, not independently verified here, so this entry does not rest
   the whole result on trusting one phrasing:
   - `non-Rust caller hand-written HTTP client raw HTTP TypeScript Python
     integration management API workaround`
   - `TypeScript client wrapper axios fetch Node calling Harvest workflow
     API`
   - `Python client wrapper requests httpx calling Harvest workflow API
     polling result`
   - `no official SDK npm package PyPI package for management API`

   All scoped to `autumn-foundation/autumn-harvest`, no state filter (open
   and closed issues and PRs both in scope), default ranking, first 20
   results requested. The first three matched **0** issues or PRs; the
   fourth matched exactly **#955 itself** and nothing else.

   **A negative-control test, and what it actually shows.** Rather than
   keep adding queries indefinitely — there is no principled stopping
   point for "enough" phrasings — this entry ran one decisive check
   instead: a query paraphrasing #955's own ask with almost no literal
   term overlap (`developers coding in other languages need a proper
   library instead of writing raw calls by hand to talk to this engine`).
   If the tool's claimed semantic matching is reliable, this should still
   surface #955, a known conceptually-identical match. **It did not** —
   0 results. That means this tool cannot be trusted to catch a real
   complaint phrased far enough from these queries' vocabulary, which is
   a real limit on Tier 1, not a solved problem: the four queries above
   rule out complaints using similar wording to them, and nothing
   stronger. The deterministic, query-independent evidence — search 2
   below — is accordingly the more load-bearing of the two, and Tier 1's
   overall confidence is downgraded to reflect this rather than papered
   over with a fifth or sixth query.
2. `git grep -c curl -- ':!docs/opportunities/*'` (the exhaustive form — an
   initial pass used GitHub's hosted code search, which returned only 40 of
   the corpus's actual matches and undercounted; git grep is the
   reproducible instrument, not the hosted search) finds **203 matches
   across 81 files**. Every one of the 81 files is documentation
   (`docs/**`, `README.md`, `DESIGN-525.md`), a worked example
   (`examples/**`, `autumn-harvest*/examples/**`), a test or CI script
   (`**/tests/**`, `.github/workflows/ci.yml`, `scripts/**`), a source
   doc-comment demonstrating the management API, or a `libcurl`
   build-dependency mention (`Cargo.toml`, the Kafka connector's doc
   comment) — classified by file path and spot-checked by content; none is
   a user-authored workaround, complaint, or support artifact.

**Search 2's actual scope, stated plainly:** `curl` is one common raw-HTTP
idiom in a docs-heavy repo, not a stand-in for every hand-rolled client. A
literal grep for `fetch(`, `axios`, or `requests\.(get|post)` returns zero
hits too, but that is a weak result on its own — this is a Rust codebase
that uses the English word "fetch" constantly for unrelated things (row
fetches, git fetches in CI), so a bare `\bfetch\b` grep returns 257 hits
across ~90 files that are almost entirely noise, and no finite keyword list
covers every client library a workaround might use anyway. Search 2 is
therefore reported for what it actually is: a spot-check that the code
corpus's `curl` idiom is 100% first-party documentation, nothing more, but
it is deterministic and query-independent, unlike search 1 — given the
negative-control result above, search 2 is the more trustworthy of the
two, not merely a supplement to it.

**What this Tier 1 evidence actually supports, stated at the confidence it
earns:** no complaint using vocabulary close to the five tried phrasings
exists in this repository's issues or PRs, and the code corpus's one
checked raw-HTTP idiom (`curl`) is 100% first-party. It does **not**
support a stronger claim that no differently-worded complaint could exist
in this repository, since the negative-control test shows this search
instrument can miss a known match. This repository is also the engine's
own issue tracker, not a support desk or sales-call archive — a `0` here
is *absence of evidence in the only corpus available to this process*, on
top of being a weaker `0` than initially presented; see the ledger's
methodology note. It is nonetheless the totality of what this run could
check, and it found nothing.

**Tier 2 (market record) — not gathered.** No switcher-to-Harvest interview
exists citing SDK availability as a reason for adopting or rejecting
Harvest.

**Tier 3 (created evidence) — not run.** No fake door, concierge, or
landing-page probe has been executed for this job. See Pre-registration
below for the probe this entry proposes before any further discovery step.

**A chronology gap, not an authoring gap, found on inspection:** #955
(filed 2026-07-08) compares itself only against a hand-rolled raw-HTTP
baseline and against five competitors' *shipped SDK* offerings. At filing
time that was a complete comparison: issue #694 (OpenAPI 3.1 spec) was
still open, and did not merge until PR #1404 on 2026-09-07 — two months
*after* #955 was filed. #955's original gap analysis is not at fault for
omitting an alternative that did not exist yet.

But #694 has since shipped, and nothing has revisited #955 in light of
that. Today's actual cheapest current hire is no longer "hand-write raw
HTTP" but "run `openapi-generator` (or `openapi-typescript`) against the
now-served/published OpenAPI document ([`docs/openapi.json`](../openapi.json),
per [`docs/openapi.md`](../openapi.md)) once, locally, for free." Nothing
has measured whether teams have tried that newly-available path and found
it insufficient, or whether they've tried it at all — and the roadmap
(#968) has not been updated either: it still lists #694 as a pending
Milestone 6 item as of its last edit, even though #694 closed before this
entry was written. A demand case for a hand-maintained ergonomic layer
that has never been checked against the free self-serve alternative that
shipped after it was written is not a live demand case yet — it is a
solution preference whose own foundational comparison went stale the
moment #694 merged, and nobody has re-run it since.

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

**Line — kill:** fewer than 2 such reports in the 60-day window, **and** a
minimum *segment-qualified* exposure floor is separately met. Raw thread
traffic does not qualify: a Discussion hitting 200 views or 20 reactions
proves nothing about who saw it — a repo's existing Rust-focused audience
can produce that traffic without a single non-Rust integrator ever reading
the prompt, and scoring that as "we reached them and they didn't answer"
would be a kill built on an uncontrolled sample, not evidence. The floor
that actually qualifies: **at least 2 direct solicitations delivered to
contacts with their own visible evidence of integrating or evaluating
Harvest from a non-Rust environment** — e.g., an account that commented on
#955 or #694, filed an issue describing a Harvest integration attempt, or
authored a public repository (outside this one) that references
`HarvestPlugin`, `harvest_api_router`, or the management API by name. A
starred or forked repo plus a TypeScript/Python-heavy profile is **not**
sufficient on its own — that account need not be integrating Harvest at
all, and soliciting two unrelated people would let the floor pass on
noise. Live linkage from `README.md` and `docs/management-api.md` is
necessary but not sufficient either way.

**What is actually known about this count, stated honestly:** this
repository's own issues/PRs name zero such contacts (the same Tier 1
sweep). But two of the three qualifying paths above — an account that
authored a public repository *outside this one* referencing `HarvestPlugin`
or the management API, or otherwise engaging with Harvest elsewhere on
GitHub — were never searched, because this process's GitHub access is
scoped to `autumn-foundation/autumn-harvest` only and cannot reach or
search any other repository or account. The correct status for that part
of the count is **not searched**, not **zero** — this entry originally
overstated it, and the fix is disclosure, not a claim this process cannot
back up. Finding at least 2 concrete, qualifying contacts (which requires
that broader search) is part of running this probe, not an afterthought,
and should happen before the 60-day window opens, by whoever runs it with
that access. If none can be found at all even with full GitHub search
reach, that absence is itself the finding worth
reporting — it would mean this job cannot yet be probed through any
channel, and closing that harness gap (finding or building a way to reach
the named segment) becomes the next demand-report deliverable, ahead of
any pursue/kill verdict on #955
itself. Short of clearing the 2-contact floor, the window's silence is
**inconclusive**, not a kill.

**If it later ships anyway** (build criteria, for whoever runs this probe
and clears the line): npm/PyPI download counts cannot answer "how many
distinct orgs installed this" or "is a given org still on it 90 days
later" — neither registry exposes installer identity or per-installer
retention, so a kill line phrased in those terms is unfalsifiable and not
usable at endpoint review. The proxy that is actually collectible without
new telemetry: a public code search (GitHub code search or equivalent) for
`@autumn-harvest/client` in `package.json` / `autumn-harvest-client` in
`requirements.txt` or `pyproject.toml`, across distinct repositories and
orgs, run once at day 30 and again at day 90 post-publish. Kill criteria:
**<10 distinct public repos/orgs referencing either package at the 90-day
mark**, or **fewer than half of the day-30 referencing repos/orgs still
referencing a current (non-yanked, non-pre-release) version at day 90**.
This undercounts real adoption (private repos and internal monorepos are
invisible to a public code search) — recorded as a known limitation of the
proxy, not grounds to skip registering a number. Either miss, sunset with
a migration note pointing back at the OpenAPI-codegen self-serve path,
which never goes away regardless of this feature's fate.

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

- Workaround-census sweep run for this entry: `search_issues` (GitHub) over
  `autumn-foundation/autumn-harvest`, the four literal queries recorded in
  Tier 1 above, no state filter, first 20 results each — three returned 0
  hits, the fourth returned only #955 — plus
  `git grep -c curl -- ':!docs/opportunities/*'` (203 matches, 81 files,
  every one classified by path and spot-checked as docs/examples/tests/CI —
  use `git grep`, not GitHub's hosted code search, which undercounted this
  corpus by 5x). Zero independent-workaround or complaint hits beyond #955
  itself.
- Negative-control test on the issue search, also reproducible: query
  `developers coding in other languages need a proper library instead of
  writing raw calls by hand to talk to this engine` (deliberately low
  literal overlap with #955's own title/body) returns **0** results,
  failing to retrieve #955 itself despite being a conceptually identical
  paraphrase. This is the basis for downgrading Tier 1's confidence in the
  issue-search instrument in favor of the deterministic `git grep`.
- Scope limitation on the exposure-floor contact search: this process's
  GitHub access is restricted to `autumn-foundation/autumn-harvest`, so
  the "identifiable non-Rust-adjacent contacts" criterion's
  external-repository and external-account paths were never searched —
  only this repository's own issues/PRs were. That part of the "zero
  known contacts" count should be read as "not searched," not "searched
  and found none."
- Chronology check for this entry: `mcp__github__issue_read` / `pull_request_read`
  on #694 and PR #1404 — #694 merged 2026-09-07, #955 was filed 2026-07-08.
- Probe configuration: as pre-registered above. Whoever runs it should link
  the Discussion from `README.md` and `docs/management-api.md` in the same
  change that opens it, so the 60-day window and the "real visibility" kill
  condition both start from a verifiable commit.
