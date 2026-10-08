# 🏛️ Keystone findings memo: management-API route coupling

**Status:** findings memo — no RFC, no decision required from architecture review.
**Scope examined:** `autumn-harvest/`, `autumn-harvest-plugin/`, `autumn-harvest-cli/`
only (the workspace crates with source history relevant to the management
API and the execution kernel — `autumn-harvest-macros`, `autumn-harvest-redis`
and `autumn-harvest-sqlite` are deliberately excluded from both the query and
the denominator below), full commit history from the first commit
(2026-03-28) through `trunk-dev` HEAD as of 2026-08-27
(`562c7815118e2a486350e8a3ceb879caa28c0212`), 478 commits touching tracked
`*.rs` files in those three crates.

## Reproduce

```sh
# 562c7815118e2a486350e8a3ceb879caa28c0212 is trunk-dev HEAD as of the
# analyzed cutoff (2026-08-27); pinned so this reproduces the documented
# 478-commit denominator even after trunk-dev advances further.
git log --format='@@%H' --name-only 562c7815118e2a486350e8a3ceb879caa28c0212 -- \
  'autumn-harvest/src/*.rs' 'autumn-harvest-plugin/src/*.rs' 'autumn-harvest-cli/src/*.rs' \
  > cochange_raw.txt
# then: parse '@@<sha>' blocks, count per-file touch frequency and pairwise
# co-change frequency across files in the same commit (see script in this
# memo's PR description / session log — omitted here as it's ~30 lines of
# straightforward Python over the log above).
```

## Evidence (tier 2 — repository & delivery record)

Top 10 files by commit-touch frequency, over 478 commits:

| touches | % of commits | file | lines (HEAD) |
|---:|---:|---|---:|
| 212 | 44.4% | `autumn-harvest-plugin/src/api.rs` | 55,185 |
| 163 | 34.1% | `autumn-harvest/src/worker.rs` | 35,926 |
| 163 | 34.1% | `autumn-harvest/src/lib.rs` | 876 |
| 125 | 26.2% | `autumn-harvest/src/context.rs` | 29,178 |
| 89 | 18.6% | `autumn-harvest/src/builder.rs` | — |
| 84 | 17.6% | `autumn-harvest-cli/src/lib.rs` | — |
| 81 | 16.9% | `autumn-harvest-plugin/src/ui.rs` | 15,431 |
| 80 | 16.7% | `autumn-harvest-plugin/src/plugin.rs` | 4,107 |
| 74 | 15.5% | `autumn-harvest/src/telemetry.rs` | — |
| 69 | 14.4% | `autumn-harvest/src/schema.rs` | — |

`api.rs` also anchors the great majority of the strongest pairwise co-change
edges in the whole graph (e.g. it co-changes with `worker.rs` in 79 commits,
with `plugin.rs` in 66, with `ui.rs` in 64, with `audit.rs` in 64, with
`builder.rs` in 56, with `execution.rs` in 54, with `schema.rs` in 52) — i.e.
it is not just individually hot, it is the thing that near-every other hot
file collides with.

`api.rs` size at each tagged release (`git show <tag>:autumn-harvest-plugin/src/api.rs | wc -l`):

| v0.1.0 | v0.2.0 | v0.3.0 | v0.4.0 | v0.5.0 | trunk-dev |
|---:|---:|---:|---:|---:|---:|
| 550 | 777 | 8,541 | 19,821 | 39,127 | 54,425 |

Growth has been rapid and sustained across every release interval, without a
clean accelerating trend: 28 lines/day (v0.1.0→v0.2.0), 518 (v0.2.0→v0.3.0),
313 (v0.3.0→v0.4.0), 522 (v0.4.0→v0.5.0), 450 (v0.5.0→trunk-dev HEAD) — the
most recent interval is slightly below the v0.4.0→v0.5.0 peak, not above it,
but every interval since v0.2.0 has added the file's growth at 300+
lines/day, and no interval shows the growth trending toward zero. At HEAD the
file holds 312 `async
fn` definitions in total (`grep -cE '^\s*(pub(\(.*\))? )?async fn '`: 299 at
top level plus 13 inside `#[cfg(test)]` modules); of those, 168 are wired as
HTTP method handlers (`get`/`post`/`put`/`patch`/`delete`) across 165
`.route()` calls inside a single 670-line `harvest_api_router` function built
as one `Router::new()` chain, with no `Router::merge()` — every
management-API endpoint in the product is registered, and most are handled,
inline in this one file. (The remaining ~130 `async fn`s are shared helpers,
audit/validation plumbing, and inline tests — not separate endpoints.)

## Is this inherent or accidental?

Both are present in the top 10, and they should not be treated the same way:

- **Inherent (core execution kernel):** `worker.rs` and `context.rs` are the
  durable-execution kernel — the poll loop and the deterministic-replay
  context. Every workflow feature touches dispatch and/or replay by
  construction; this is the domain, not an organization defect, and matches
  the example pattern in the Keystone process ("A and B are inherent to the
  domain").
- **Inherent to the current composition-root design (an implementation
  choice, not an ADR'd decision):** `lib.rs`, `builder.rs`, `plugin.rs`,
  `cli/lib.rs` are the re-export surface, the `HarvestBuilder` config
  surface, the plugin registration surface, and the CLI entry point. ADR-0002
  (rust-native execution boundary) decides that workflow/activity authoring
  stays Rust-only and rejects a polyglot worker protocol — it does not
  prescribe a single builder or a centralized config/re-export/plugin
  surface, so it is not evidence that this file set's coupling was a
  deliberate architectural choice, only that today's implementation happens
  to route every new capability through one builder and one re-export
  module. That routing is why touch-frequency is high here too. Unlike
  `api.rs`, though, these files are small (876–4,107 lines), so even
  frequent touches are cheap and low-conflict — the measured cost is much
  lower, not the design pedigree.
- **Accidental, and the outlier:** `api.rs` (and, at smaller scale,
  `ui.rs`) is not inherent. Every other domain concern in
  `autumn-harvest-plugin/src/` is already factored into its own file —
  `dag_retry.rs`, `dag_graph.rs`, `mcp_tools.rs`, `preflight.rs`,
  `shard_health.rs`, `status_summary.rs`, `queue_coverage.rs`,
  `canary.rs`, `webhook_receiver.rs`, `lineage.rs`, `schedule_runs.rs`,
  `config.rs`, `api_token.rs`, and 15 more, none over 120K bytes (~2,500
  lines). The domain logic these route handlers call into is *already*
  well-decomposed. Only the axum route registration and handler-body layer
  was never split the same way — a sampled handler
  (`create_gate_handler`, `api.rs:3773`) is a thin
  validate-then-delegate-to-`admission_gate_db::create_gate` shape, the same
  shape repeated across the file's 168 wired handlers, all in the one file.
  This is the file-size outlier in a codebase that otherwise organizes by domain everywhere else,
  and it is why it collides with nearly every other feature file: any PR
  that adds or changes a management-API endpoint edits this file, regardless
  of which domain the endpoint belongs to.

## Do nothing / decide later

Nothing about this is a one-way door and nothing about it is urgent: the file
compiles, the tests inside it pass, and no external contract changes. Left
alone, the trend (550 → 54,425 lines across 5 tagged releases, sustained at
300+ lines/day since v0.2.0) says the file keeps absorbing every new endpoint
at the same rate features ship, and the 44.4%-of-commits touch rate —
already the highest in the repo — keeps rising with it. The cost is
coordination risk (near every
concurrent feature PR now edits the same file) and reviewability (a 55K-line
file has no natural review boundary), not correctness or an outage risk.

## Why this is not an RFC

Splitting `api.rs` into per-domain route modules (mirroring the pattern
already used for every other concern in the same crate — e.g. a
`routes/dag_retry.rs` colocated with `dag_retry.rs`, each exposing its own
`fn routes() -> Router`, composed in `api.rs` via `Router::merge`) changes no
wire contract, no public API, no schema, and does not cross a team or
ownership boundary in a single-team repository. It is a two-way door:
reversible by reverting the commit(s), at a reversal cost of hours, not
weeks. Keystone's own charter bans deciding two-way doors under ~2
engineer-weeks reversal cost by RFC — that is the implementing team's call in
an ordinary PR, not an architecture-review decision. Writing an RFC to
mandate it would be exactly the "elevating it to architecture review burns
the scarcest resource" case the charter warns against.

## What this memo is

A recorded, reproducible measurement: `api.rs` is the top code-coupling cost
in the repository by a wide margin over the next-largest file (44.4% vs.
34.1% of all commits touching tracked source, and more than 3x the byte size
of the next-largest file in the same directory), it is the accidental outlier
against the codebase's own established per-domain organization, and the fix
is a low-cost, reversible refactor that does not need architecture-review
attention — only someone to do it. No action is required from this memo; it
exists so the next person to feel this file's size has the numbers instead
of a hunch.
