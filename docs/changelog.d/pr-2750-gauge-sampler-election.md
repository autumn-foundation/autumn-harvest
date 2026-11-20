## One elected gauge sampler per shard (issue #2679, PR #2750)

Each worker ran every database gauge sampler, and the fleet-wide samplers
read every shard. W workers paid W reads of each shard for one answer.

The eight database gauge samplers now elect one reader per shard through
`harvest_scanner_leases`. The key is `gauge:v1:<sampler>`, plus a digest of
the sampler inputs. The leader reads the shard and writes the read to the
new table `harvest_gauge_samples`. Each other worker uses the row while it is
younger than three sample intervals. Otherwise it reads the shard itself.

- **Every worker still emits each gauge**, with the same value as before. No
  series, alert rule or dashboard query changes. A handover changes who
  reads, not who emits, so it cannot drop or double-count a gauge. A standby
  emits the leader's read, so a gauge that grows with time, such as the
  oldest pending age, is up to one interval behind.
- **Rules that read a time-based gauge.** The alert rule
  `harvest_queue_schedule_to_start_high` reads
  `harvest_queue_oldest_pending_age > 30` as its fallback expression. On a
  standby that gauge is up to one interval old, so the rule can fire up to
  one interval (15 s by default) later. Two dashboard panels read time-based
  gauges the same way: the oldest pending age and the rate-limit tokens. No
  other rule or panel changes. Each reads a value that does not depend on
  the read time.
- The row write needs a live lease of the writer. An outdated leader writes
  nothing.
- `harvest.scanner.pass` gains `scanner="gauge_<sampler>"`. Its non-standby
  rate is the read load per shard.
- A failed lease query, a missing, old or undecodable row, a held shard and a
  fenced process all fail open to a direct read. Under a DR pin, each write
  checks the fence first. `ScannerConfig::elect = false` turns the
  election off.
- A leader that cannot write its row three times in a row gives up its
  lease.
- **Migration.** `20261119232341_harvest_gauge_samples` adds one table. The
  storage role needs `SELECT`, `INSERT`, `UPDATE` and `DELETE`. Preflight
  checks the grants. No `WorkflowEvent` variant, no replay impact.

Evidence: `gauge_election_db_tests`. A worker-driven harness runs three
workers. Each gauge sampler reads the shard about once per interval, not
three times. Each elected worker emits the same values as an unelected
worker. A handover test stops the leader and checks that each survivor keeps
the right value. Design: `docs/design/DESIGN-gauge-sampler-election.md`.
