## Feature — OR filters and live Vantage views (issue #1982)

**What shipped.**

- `GET /workflows` takes a `filter` parameter. It joins predicates with `AND`,
  `OR` and parentheses. `AND` binds tighter than `OR`. Fields are
  `attrs.<key>`, `state`, `workflow_name`, `owner`, `severity` and
  `started_at`. The stalled and history-bloat lists apply it too.
- The Vantage workflow list has a `Filter` field for the same grammar.
- `GET /workflows/changes/stream` is a new admin-gated SSE route. It listens to
  `harvest_events` on each shard and sends a coalesced `changed` frame, at
  most once each second. A frame holds a count only.
- The Vantage list and the detail page of a live run update without a reload.
  `assets/live.js` reads the change stream (list) or the #324 execution stream
  (detail), fetches the page again and swaps the marked regions.

**Design.** `DESIGN-1982.md` holds the planning record. A filter with `OR`
must find each row through an index: each `OR` branch, or the whole filter,
holds an `attrs.*` predicate or `workflow_name` with `=` or `IN`. Postgres
then joins GIN index scans with `BitmapOr`. A filter that breaks the rule
gets `400`. Every key and value is a
bound parameter. Attribute leaves share one renderer with `search_attr_filter`
(issue #506).

**Invariants.** No migration. No new `WorkflowEvent` variant. No engine
change. The page script is same-origin. The pages hold no inline `<script>`
element.

**Tests.**

- `visibility_query` unit tests: precedence, groups, types, limits, the index
  rule, the SQL text and injection text in binds.
- `workflow_filter_integration`: `filter_or_precedence_and_grouping`,
  `filter_errors_return_400`,
  `filter_applies_on_the_stalled_path_and_across_shards`,
  `filter_or_explain_uses_the_search_index` (20 000 seeded rows, the plan has
  `BitmapOr` over `idx_harvest_we_search` and no `Seq Scan`),
  `change_stream_sends_changed_after_an_event` and
  `change_stream_needs_admin_and_a_notification_url`.
- `ui::tests`: the list and detail pages load the script and mark regions. The
  filter field echoes and carries the filter. The server serves the script.
- `workflow_filter_integration` also covers the history-bloat path, the
  Vantage list filter, a live detail page and a closed listener.
