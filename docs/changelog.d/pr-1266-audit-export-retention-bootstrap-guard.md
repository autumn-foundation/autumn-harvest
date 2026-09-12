## Phase — audit-export retention bootstrap-window guard (issue #1266)

`purge_old_audit_records` already refused to delete an unexported audit row
when a live cursor existed for the shard, or when the sweeping process had a
sink configured. Issue #1266 (a Codex review round 6 follow-up on PR #1261)
found both signals could be absent at once in a split web/worker deployment:
before the worker's first successful tick on a shard, the process running
retention has no sink and no cursor row to read. A retention sweep landing in
that window deleted every retention-aged audit row, including rows the
exporter had not shipped yet.

Adds a third, explicit signal: `RetentionConfig::protect_unexported_audit`
(default `None`, disabled). Set the same way on every process in a split
deployment, it closes the window from the moment export is configured rather
than from the moment the exporter's first tick succeeds — the two cheaper
fixes the issue considered and rejected (seeding the cursor row in a
migration; letting the retention process create it) both fail for the same
reason: a shard's own database cannot know whether an exporter is coming,
and the retention process does not know either.

`purge_old_audit_records` gained a third parameter, `protect_unexported_audit:
bool`, OR'd flatly with the existing `is_configured()` signal — no schema
change. A first draft instead scoped the flag to `NOT EXISTS(any cursor row)`,
so a retired cursor always overrode it; a Codex review (round 1 P1) caught
that this reopened the exact bootstrap window for a shard being **re-enabled**
after decommission, since its cursor stays retired until the worker's first
new tick, and is indistinguishable from a shard meant to stay decommissioned.
The flag now shares `is_configured`'s existing "both steps required" trade
instead: decommissioning a shard does not resume purging there while either
signal stays `true` on the sweeping process, an operational cost documented
next to the pre-existing one for `is_configured`.

The same review round found a second, independent defect in the per-row
pending check: a row already stamped with an `export_seq` was treated as
already acknowledged whenever no cursor row existed at all — exactly backward
from `ensure_cursor_row`'s own handling of a cursor lost to a manual `DELETE`
or a partial restore, which rebuilds it at `last_acked_seq = 0` so every
stamped row is redelivered. The pending check now also treats "no cursor row
for the shard" as pending, matching that rebuild.

A further review round (P1) found that a single process-wide boolean cannot
represent a fleet with more than one shard: decommissioning shard A requires
disabling the flag, which simultaneously strips bootstrap protection from
shard B if B is mid-bootstrap on the same sweep. `RetentionConfig` changed
`protect_unexported_audit` from `bool` to `Option<BTreeSet<ShardId>>` —
`None` disables it, `Some(exempt)` protects every shard not in `exempt`. The
retention sweep now computes the per-shard bool itself
(`RetentionConfig::protects_unexported_audit`) instead of passing one flag
to every shard; `purge_old_audit_records`'s own signature is unaffected,
since it already took a plain `bool` per call. `with_protect_unexported_audit`
keeps its `bool` shape for the common case; a new
`excluding_shard_from_protect_unexported_audit` adds the exemption.

A fourth review round (P1) found that the per-shard fix above was still
unsafe under a supported topology: two logical shards can alias one
physical pool (`ShardedDbPool::from_map`, a pre-split staging shape).
`purge_old_audit_records` issues one unscoped `DELETE` per call, so calling
it once per logical shard let two aliased shards apply two different
decisions to the same physical audit table within one tick — a less
protective decision could commit before a more protective one ever ran.
The sweep now groups shards by pool identity first (`Pool::manager()`
returns a reference into the pool's shared `Arc` allocation, so `ptr::eq`
on it detects aliasing safely, with no private field or unsafe code), and
combines each group's decision with `any`: protect the shared pool
whenever any aliased shard wants protection. This also purges each
physical pool exactly once per tick instead of once per logical shard.

A fifth review round (P1) found the fourth round's fix incomplete:
`ShardedDbPool::from_dsns` builds a separate `Pool` object per shard entry
even when two entries carry the same DSN, so `ptr::eq` on `Pool::manager()`
never sees the alias. `ShardedDbPool` now records each shard's pool-group
number at construction time and exposes it via `pool_groups()`.
`from_map` still groups by `Pool::manager()` identity. `from_dsns` groups
by the DSN string instead, compared before each string is consumed into a
manager. The retention sweep's own grouping logic moved into
`ShardedDbPool::pool_groups()`, so both callers share one grouping.

A sixth review round (P1) found the DSN-string comparison itself too
narrow: two DSNs reaching one physical database can differ in
credentials, an explicit default port, or extra connection parameters,
none of which change which database a connection reaches. `from_dsns`
now compares a canonical key instead — host (lowercased), port (defaulted
to 5432), path, and query string — dropping only credentials, which
never change which database or schema a connection reaches. A DSN that
does not parse as a URL falls back to the raw string, the prior behavior.
A host alias (two hostnames resolving to one address) stays undetected by
design: resolving it needs a DNS lookup, and building a pool must stay a
pure, local operation with no network access.

A seventh review round (P2) caught a mistake in the sixth round's first
version of this key: it dropped the query string along with credentials.
A `?options=-c search_path=...` parameter selects which schema
`harvest_audit_log` resolves to, so two DSNs differing only there could
reach different data yet still collapse into one group. `pool_groups()`
returns one representative pool per group, so the collapsed-away shard's
audit table would silently stop being purged at all — the opposite
failure from the rest of this PR, which is about purging too early, not
too rarely. The key now keeps the query string verbatim.

An eighth review round produced two findings pulling in opposite
directions on the same line. A P1 finding noted that keeping the whole
query string reintroduced the original problem for parameters that carry
no schema meaning: two DSNs differing only in `application_name` or
`sslmode` would no longer collapse, so an exempt shard's unprotected pass
could again run before a colocated protected shard's pass. A P2 finding
argued the opposite for username: dropping it can combine two roles whose
own `search_path` (or PostgreSQL's default, which includes the
connecting user's own schema) differ, again risking the silent
never-purged case above.

`canonical_dsn_key` now keeps only the `options` query parameter — the
one libpq mechanism that can carry `-c search_path=...` — and drops every
other query parameter, including credentials. The username finding is
documented as an accepted, unfixed gap rather than chased further: a
role's own server-side `search_path` is invisible in the DSN regardless
of username, so keeping the username would not fully close the gap; it
would only reopen the sixth round's original bug, since a documented
`from_dsns` use (`harvest shard rebalance`, issue #964) targets one
database under different usernames. Between an accepted, narrow,
documented gap and reopening a P1-severity bug this PR exists to close,
the gap stays.

A ninth review round (P2) found a case the `options`-only key still
missed: a Unix-socket DSN carries no host in its URI authority at all.
libpq instead reads the real endpoint from a `host` or `hostaddr` query
parameter (`postgresql:///harvest?host=%2Frun%2Fpg`), which the key had
never inspected, so two DSNs naming different sockets could still
collapse into one group. `canonical_dsn_key` now falls back to a `host`
or `hostaddr` query parameter when the authority host is empty, or to
`hostaddr` whenever it is given at all, matching libpq's own precedence
between the two. A `port` query parameter is honored the same way.

A tenth review round found two more gaps in the same host-resolution
fix. First (P2): a resolved host was always lowercased, which is correct
for a DNS name but wrong for a Unix-socket path — `/run/PG-A` and
`/run/pg-a` name different sockets on a case-sensitive filesystem, so
lowercasing them could falsely merge two different databases.
`canonical_dsn_key` now lowercases a resolved host only when it does not
start with `/`. Second (P2): a DSN with no path was treated as naming no
database, but libpq defaults an omitted `dbname` to the connecting
username, so two DSNs with no path but different usernames can already
name two different databases today, silently. The key now uses the
username only when the path is empty; an explicit path still ignores the
username, so this does not reopen the sixth round's credentials fix.

An eleventh review round found two further defects, in different files.

The first (P1) found that `url::Url` and `tokio_postgres::Config`
disagree on percent-decoding, on `?dbname=`/`?host=`/`?port=`/
`?hostaddr=` overrides, and on comma-separated multi-host DSNs -- fresh
evidence pointed at `backup_verify.rs`'s own `parse_dsn_identity`
comment, which documents this exact divergence for the guard that keeps
a backup off a live production database. Rather than continue chasing
individual `url`-versus-connector mismatches one at a time, as rounds
six through ten each did, `canonical_dsn_key` now parses with
`tokio_postgres::Config` directly, the same parser `parse_dsn_identity`
uses and the one `diesel_async` actually hands the DSN to at connect
time. This closes the percent-decoding gap by construction, along with
any other divergence class between the two parsers, rather than adding
another special case. Unlike `parse_dsn_identity`, which serves a
different guard with different needs, this key still keeps `options`
(schema-relevant) and drops every other query parameter, and keeps a
resolved Unix-socket path's case instead of folding it, matching the
ninth and tenth rounds' fixes.

The second (P1) found a defect in `audit.rs`, not `shard.rs`. Verifying
it meant tracing the actual SQL: with two colocated shards sharing one
pool, shard A ticked and acknowledged some rows while shard B has no
cursor row yet, a row already acknowledged by A read as fully
acknowledged even though B has acknowledged nothing. The pending check's
own doc comment had assumed "a shard's database holds at most one cursor
row" -- an assumption the fourth through tenth rounds' own colocated-pool
work had already made false. `purge_old_audit_records` gains a fourth
parameter, `colocated_shard_count`, and the pending check now compares
the cursor count against it instead of testing for zero rows. A
single-shard caller passes `1`, unchanged from before. The real caller,
`retention.rs`'s `group_shards_by_pool`, now returns each pool group's
shard count alongside its combined protection decision, sourced from
`ShardedDbPool::pool_groups()`.

New tests:
- `retention_protects_unexported_audit_when_configured_with_no_cursor_and_no_local_sink`
  reproduces the exact bootstrap window (no cursor row anywhere, no sink in
  the sweeping process) and asserts the flag alone keeps every row.
- `retention_decommission_alone_does_not_resume_purging_while_the_flag_is_true`
  and `retention_protects_a_shard_being_re_enabled_after_decommission` pin the
  round 1 P1 fix: the flag survives a retired cursor, and a shard coming back
  from decommission stays protected until the worker ticks it again.
- `retention_protects_a_stamped_row_when_its_cursor_row_is_gone` pins the
  second round 1 P1 fix: a stamped row with no cursor row to check against
  is never treated as acknowledged.
- `retention_still_purges_acknowledged_rows_when_protect_unexported_audit_is_true`
  confirms the flag never blocks purging of rows the exporter already
  shipped, even while a live cursor exists.
- `excluding_a_shard_leaves_every_other_shard_protected` and
  `excluding_a_shard_while_disabled_changes_nothing` pin the per-shard
  exemption: excluding shard 0 never touches shard 1's protection, and
  excluding a shard while the flag is off entirely is a no-op.
- `from_map_groups_cloned_pools_together` pins the fourth round's fix at
  its new home in `shard.rs`. `from_dsns_groups_shards_sharing_one_dsn`
  pins the fifth round's fix: two shards built from one DSN string
  through `from_dsns` still collapse to one group, even though `from_dsns`
  built them as two distinct `Pool` objects.
  `from_dsns_keeps_distinct_dsns_separate` confirms two different DSNs
  never collapse.
- `from_dsns_groups_equivalent_dsns_with_different_credentials_and_port`
  pins the sixth round's fix: two DSNs for one database, differing only
  in credentials and an explicit default port, still collapse to one
  group. `from_dsns_keeps_distinct_dbnames_on_the_same_host_separate`
  confirms two database names on the same host never collapse.
- `from_dsns_keeps_distinct_search_path_options_separate` pins the
  seventh round's fix: two DSNs whose `options` set a different
  `search_path` never collapse into one group, even with the same host,
  port, and database name.
- `from_dsns_ignores_connection_only_parameters` pins the eighth round's
  P1 fix: two DSNs differing only in `application_name` and `sslmode`,
  and in credentials, still collapse into one group.
- `from_dsns_keeps_distinct_unix_socket_hosts_separate` and
  `from_dsns_groups_shards_sharing_one_unix_socket_host` pin the ninth
  round's fix: two Unix-socket DSNs naming different sockets through
  `host` never collapse, and two naming the same socket still do.
- `from_dsns_keeps_distinctly_cased_socket_paths_separate` and
  `from_dsns_groups_shards_sharing_one_hostname_regardless_of_case` pin
  the tenth round's first fix: a socket path's case is significant and
  is never folded, while a DNS hostname's case still is.
- `from_dsns_keeps_distinct_users_with_no_explicit_dbname_separate` and
  `from_dsns_ignores_username_when_dbname_is_explicit` pin the tenth
  round's second fix: two users with no explicit dbname reach different
  databases and must never collapse, while an explicit, shared dbname
  still collapses regardless of username.
- `from_dsns_groups_percent_encoded_and_plain_dbname_spellings` pins the
  eleventh round's first fix: `postgres://db.example/harvest` and
  `postgres://db.example/%68arvest` collapse into one group now that the
  key is parsed with the real connector's parser.
- `retention_protects_rows_until_every_colocated_shard_has_a_cursor` pins
  the eleventh round's second fix: told to expect two colocated shards
  but only one has a cursor row, no stamped row purges even if already
  acknowledged by the shard that has ticked; told to expect only that one
  shard, the same rows purge normally.

**Zero migration, zero engine impact beyond the new parameter.** No new
`WorkflowEvent` variant, no schema change, no change to any existing call
site's behavior when the new flag is left at its default (disabled).
