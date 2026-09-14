# Opportunity ledger

Records of product-discovery verdicts against a job hypothesis, a named current
hire, and a pre-registered evidence line committed *before* a probe runs. This
is the demand-side counterpart to [`docs/assays/README.md`](../assays/README.md):
an assay charters a feasibility question against a numeric performance line;
an opportunity charters a demand question against a numeric behavioral line.

A killed or held entry stands until new evidence — not new enthusiasm —
reopens it. Re-pitching a killed entry without a fresh workaround census,
switch-timeline interview, or probe result is re-litigation, not discovery,
and is out of scope for any entry to accept on its own say-so.

| # | job / backlog item | verdict | report |
|--:|:--|:--|:--|
| 1 | Issue #955 (official TypeScript/Python management-API SDKs): does admissible behavioral evidence exist anywhere in the accessible record for the demand this spec asserts? | **hold** — 0 admissible signals found in a full-corpus workaround-census sweep (issues, PRs, code comments, docs); spec never named or measured the cheaper current hire (#694's already-shipped OpenAPI spec via self-serve codegen) it would need to beat. Probe registered, not yet run. | [0001-sdk-clients-955-discovery-gate.md](0001-sdk-clients-955-discovery-gate.md) |

## Methodology note

Entries in this ledger are produced by mining this repository's own record —
issues, PRs, code, and docs — because that is the only behavioral record this
process has read access to. That is a real limitation, stated up front: a
workflow-engine's own issue tracker is engineering signal, not customer
signal, and a `0` count here means *no evidence surfaced in this corpus*, not
*no demand exists*. Where an entry's job hypothesis concerns people this
corpus cannot see (non-Rust integrators, downstream ops teams, external
adopters), the entry says so and routes the actual probe to a channel that
can reach them (a pinned Discussion, a docs fake-door, real support/sales
archives) rather than treating corpus silence itself as a kill line.
