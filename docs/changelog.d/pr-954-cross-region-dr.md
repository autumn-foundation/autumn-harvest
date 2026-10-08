## Phase 5.x — cross-region DR via logical replication with fenced failover (issue #954)

Harvest's availability ceiling was one Postgres region per shard, and the
obvious answer — a cloud cross-region replica — is dangerously incomplete for
an event-sourced engine: after a failover, old-region workers that come back
(or were partitioned, not dead) can still claim tasks and append events against
their local, now-stale database, forking a workflow's history. This ships the
engine half of the answer: **stock Postgres moves the bytes; Harvest makes the
engine fencing-aware.** No new infrastructure in core, no sidecars, no brokers.

**Fencing.** `harvest_shard_generation` holds one row per shard in that shard's
own database: a monotonic write-authority epoch. A worker with `dr_fencing`
enabled provisions and *pins* it at startup, before fleet registration and
before its first poll, and fails **closed** — refusing to start rather than
running unfenced — if it cannot be read. Two structural checks then use the pin:
`claim_task_on_shard` cross-joins the row as one `MATERIALIZED` CTE (no extra
round trip; a fenced worker selects zero candidates and burns no attempt), and
every `store::append_events*` takes the row `FOR SHARE` first. `FOR SHARE` is
the load-bearing detail rather than decoration: the fence bump takes the same
row exclusively, so it cannot commit while an in-flight persist holds it, and
any persist beginning after it commits observes the new epoch and fails — a
commit-order barrier, the same technique the queue-pause hold already uses.
Promoting a standby is then: bump the epoch, and every worker still pinned to
the old one is structurally unable to write, stopping with
`HarvestError::ShardFenced`.

**No new `WorkflowEvent` variant, no change to any existing table, no replay
impact.** A fenced attempt appends nothing at all — not a marker, not a
rejection event. Migration `20260726000000_harvest_shard_generation` adds two
tables, both empty until a DR-enabled worker provisions them. Fencing is
**opt-in and off by default**; with it off the claim SQL is byte-for-byte the
pre-#954 string (pinned by a test) and the persist path issues no statement.

**Measured RPO.** `harvest.replication.lag_seconds{shard}`, plus `lag_bytes`,
`standbys`, `harvest.shard.generation`, and `harvest.shard.fenced`, with three
starter alert rules and their runbook sections. Two findings from building the
drill, both designed for rather than assumed:

- `pg_stat_replication.replay_lag` goes **blind** for logical replication when
  the subscriber's apply worker stalls — it is computed from the subscriber's
  reply messages, and a stuck apply worker stops replying. Measured: with apply
  blocked, byte lag grew monotonically while `replay_lag` never left `NULL`.
  The RPO is therefore measured from an LSN watermark trail
  (`harvest_replication_heartbeat`) and falls back to `replay_lag` only when no
  watermark has been confirmed. The beat also keeps WAL moving on an idle
  primary, so idle deployments report a live RPO rather than a drifting one.
- `pg_replication_slots` / `pg_stat_replication` are **cluster-wide** but a
  Harvest shard is a *database*, so every replication query is scoped to
  `current_database()`. Without that, a cluster hosting two shards reports each
  shard's lag as the worst of both, and any unrelated slot pegs every shard's
  RPO to a stranger.

**Unknown is never zero.** The lag gauge is *absent* when the RPO cannot be
determined, and `harvest_replication_down` keys on `standbys == 0` rather than
on a lag threshold — a lag-based alert produces no series and stays silent
through exactly the outage it exists for.

**Operator surface.** `harvest dr status` (read-only; the RPO you would be
accepting), `harvest dr fence` (requires `--reason` and
`--i-understand-this-stops-the-fleet`), and `harvest dr promote`, which advances
every sequence after promoting a **logical** standby. That last one is a
separate verb on purpose: logical replication copies rows but not sequence
values, so a promoted logical standby holds every replicated `harvest_events`
row while `harvest_events_id_seq` still sits where it started, and the new
primary's first append dies on a duplicate key.

**Docs.** `docs/cross-region-dr.md` (topology, design, and the honest limits)
and `docs/runbooks/cross-region-failover.md` (fence + isolate → promote →
verify → start workers, plus fail-back and the drill). The verify step reuses
`harvest backup verify` verbatim — replayer sample, scanner dry-run, cross-shard
coherence — so no new verification code ships. Both docs state, in a section
CI pins: fencing **cannot** stop a partitioned old region from writing to its
own database, so isolating the old primary is a **mandatory** operator step;
and a fenced worker is recovered by **restarting** it, never by adopting the
new epoch in place, which would re-admit a worker the promoted region just
evicted. Multi-shard skew is named rather than hidden: outbox `*Requested`
without its cross-shard terminal, parent/child skew, and schedule re-fires, with
the same "fence all, verify all, then start workers" discipline as the restore
runbook.

**Test evidence.** 11 DB integration tests in `cross_region_dr_tests` running
against **real logical replication** (publication + pre-created slot +
subscription between two databases): a fenced worker cannot claim or persist and
burns no retry; a promoted standby resumes in-flight work and rejects the old
region with no history fork; the RPO tracks injected lag within the issue's ±5s
tolerance; a disconnected standby reports bytes and an unknown RPO, never zero.
Plus 11 documentation guards in `cross_region_dr_docs`, 5 CLI argument guards in
`dr_cli`, 2 new alert-pack guards, and 13 unit tests across
`replication.rs`/`queue.rs`/`telemetry.rs`.
