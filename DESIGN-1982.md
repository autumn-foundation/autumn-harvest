# Design — Issue #1982: a visibility filter with OR, and live Vantage views

Issue #1982 asks for two things:

1. A filter grammar for `GET /workflows` and Vantage, with `AND`, `OR` and
   groups, over search attributes and system fields.
2. Live refresh of the Vantage list and detail pages. The pages reuse the
   SSE stream of issue #324.

The grammar must compile to indexed SQL. An `OR` that defeats every index is
worse than no `OR`.

**No migration. No new `WorkflowEvent` variant. No engine change.**

---

## 0. Planning record

### 0.1 Facts found before the plan

- `GET /workflows` parses its query by hand in `parse_workflow_filters`. Each
  parameter adds one `AND` filter to a boxed diesel query. Three loaders use
  the filters: the default list, the stalled list and the history-bloat list.
- `search_attr_filter` (issue #506) gives `eq`, `ne`, `gt`, `gte`, `lt`,
  `lte`, `in` and `exists` over top-level keys. Each predicate uses `@>` or
  `?` on `search_attrs`, so each predicate can use `idx_harvest_we_search`.
- The system-field indexes are partial or composite. `workflow_name` leads
  `idx_harvest_wfx_workflow_identity`. `state` has an index only for
  `RUNNING`. `started_at` has no index that leads with it.
- Vantage is server-rendered maud HTML. It has no `<script>` tag. Tests and
  `docs/vantage-ui.md` state that.
- The SSE stream `GET /executions/{exec_id}/events/stream` covers one run.
  It is admin-gated. It sends one frame for each event, with the event type
  in the `event:` field. It sends `stream-end` when the run ends.
- The browser `EventSource` cannot set a `Last-Event-ID` header on the first
  connect. It also cannot listen to "all event names".
- Each append to `harvest_events` sends `NOTIFY harvest_events`. A list view
  can listen to that channel on each shard.

### 0.2 Brainstorm — how can a caller ask for OR?

| # | Idea | Verdict |
|---|------|---------|
| B1 | A `filter` parameter with a small infix grammar: `attrs.phase = "blocked" OR (state = "RUNNING" AND attrs.amount > 100)`. | **Adopted.** One string, readable, easy to put in a URL and a form field. |
| B2 | A JSON tree in the query string. | Rejected. Hard to type in Vantage. Hard to read in logs. |
| B3 | Repeat `search_attr_filter` with an `or:` prefix. | Rejected. It cannot express groups. |
| B4 | A Temporal-style SQL subset with `ExecutionStatus` names. | Rejected. Harvest field names differ, and a full SQL subset is too large. |
| B5 | Bare names for attributes, with reserved names for system fields. | Rejected. A tenant attribute called `state` would change meaning. The `attrs.` prefix removes the doubt. |
| B6 | Compile the tree to one parenthesized SQL fragment with bound values. | **Adopted.** No value goes into the SQL text. |
| B7 | Reuse the SQL of `search_attr_filter` for each attribute leaf. | **Adopted.** One leaf renderer serves both parameters. |
| B8 | Live list: one `EventSource` per row on the #324 stream. | Rejected. One database `LISTEN` connection for each row. |
| B9 | Live list: a new stream `GET /workflows/changes/stream`. It listens to `harvest_events` on each shard and sends a coalesced `changed` frame. | **Adopted.** It uses the same listener type as #324. The frame holds no id and no payload. |
| B10 | Live detail: read the #324 stream with `fetch`, resume from the last event the page shows. | **Adopted.** `fetch` sets `Last-Event-ID`, so no backfill. It reads every frame name. |
| B11 | On a change, fetch the same page as HTML and swap marked regions. | **Adopted.** One renderer stays on the server. The JavaScript holds no template. |
| B12 | `<meta http-equiv="refresh">` only. | Rejected. It reloads the full page and loses form input. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it fail | Mitigation |
|---|--------------------|------------|
| R1 | An `OR` branch with no indexed predicate forces a full scan. | The compiler rejects the filter (`400`) unless each `OR` branch holds an anchor: an `attrs.*` predicate, or `workflow_name` with `=` or `IN`. |
| R2 | A value goes into the SQL text. | Every value and key is a bound parameter. The operator text comes from a closed enum. |
| R3 | Deep nesting overflows the parser stack. | Depth limit 8. Length limit 2048 bytes. At most 32 predicates and 100 `IN` values. |
| R4 | `a OR b AND c` parses as `(a OR b) AND c`. | `AND` binds tighter than `OR`. Unit tests and database tests prove both forms. |
| R5 | The stalled or bloat list ignores the filter. | All three loaders call one helper. A test covers the stalled path. |
| R6 | A tenant key named like a system field changes meaning. | Attributes always use the `attrs.` prefix. |
| R7 | `"100"` and `100` match the same rows by accident. | Quotes decide the type. `100` is a number. `"100"` is a string. |
| R8 | Live refresh wipes text that an operator types in a form. | A region with focus or with edited input is not swapped. The swap runs later. |
| R9 | Live refresh floods the server. | The server coalesces `changed` frames to one each second. The client fetches at most once each 2 s (list) or 3 s (detail), and stops when the tab is hidden. |
| R10 | The change stream leaks data to a viewer. | The `changed` frame holds a count only. The route is admin-gated, the same as #324. |
| R11 | A non-admin or a failed stream sees a frozen page. | The client falls back to a timed fetch each 10 s. A browser without JavaScript gets the static page. |
| R12 | A long history gives `409 slow_consumer` on the detail stream. | The client sends `Last-Event-ID` with the last event id that the page shows. The backfill is empty. |
| R13 | The detail page keeps polling after the run ends. | A terminal run renders without live mode. `stream-end` triggers one last fetch, then the client stops. |
| R14 | Each change stream holds a database connection for each shard. | Documented. The route is admin-gated. A browser tab holds one stream. |

### 0.4 Six thinking hats

| Hat | Notes |
|-----|-------|
| White | The list query is one boxed diesel query with `AND` filters. GIN supports `@>` and `?`. Postgres can join two bitmap index scans with `BitmapOr`. #324 is admin-gated and covers one run. Vantage has no script. |
| Red | Operators want one search box. A page that updates by itself feels alive. A page that loses typed text feels broken. |
| Black | A grammar is a public contract. A change later breaks callers. A script tag breaks the "no JavaScript" promise in the docs. A change stream is one more route to secure. |
| Yellow | No migration. Attribute leaves reuse the #506 SQL and its index proof. The swap keeps one renderer on the server. A failed stream still leaves a working page. |
| Green | Later: `NOT`, more system fields, the grammar on `GET /workflows/count`, a CLI flag, a server-side filter on the change stream. |
| Blue | TDD order: parser unit tests, then SQL text tests, then database tests for results and `EXPLAIN`, then the stream route, then the UI. Then docs, the changelog fragment and a review. |

### 0.5 Decisions

1. The parameter is `filter`. It is repeatable. Repeats join with `AND`.
   It joins with `AND` to every other parameter.
2. Grammar (keywords are not case-sensitive):

   ```text
   filter    = or_expr
   or_expr   = and_expr { "OR" and_expr }
   and_expr  = term { "AND" term }
   term      = "(" or_expr ")" | predicate
   predicate = field op value
             | field "IN" "(" value { "," value } ")"
             | attr "EXISTS"
   field     = attr | system
   attr      = "attrs." ( ident | string )
   system    = "state" | "workflow_name" | "owner" | "severity" | "started_at"
   op        = "=" | "!=" | ">" | ">=" | "<" | "<="
   value     = string | number | "true" | "false"
   ```

   A string uses single or double quotes. A backslash escapes the next
   character.
3. Operators for each field:

   | Field | Operators | Value |
   |-------|-----------|-------|
   | `attrs.<key>` | `=`, `!=`, `IN`, `EXISTS` | string, number, boolean |
   | `attrs.<key>` | `>`, `>=`, `<`, `<=` | number |
   | `state` | `=`, `!=`, `IN` | a known state name |
   | `workflow_name`, `owner`, `severity` | `=`, `!=`, `IN` | string |
   | `started_at` | `>`, `>=`, `<`, `<=` | RFC 3339 string |

4. An attribute leaf compiles to the #506 SQL. A system leaf compiles to a
   column comparison. All columns are qualified with the table name.
5. Index rule: each branch of each `OR` must hold an anchor. An anchor is an
   `attrs.*` predicate, or `workflow_name` with `=` or `IN`. An `AND` holds an
   anchor when one child holds one. An `OR` holds an anchor when each branch
   holds one. A filter with no `OR` needs no anchor.
6. `GET /workflows/changes/stream` (admin-gated) listens to `harvest_events`
   on each shard. It sends `event: changed` with `{"notifications": n}`, at
   most once each second. It sends `event: stream-error` and ends when a
   listener closes. It returns `503` when no notification URL is set.
7. Vantage list and detail pages load `assets/live.js` with `defer`. The page
   marks swap regions with `data-live-region`. The script:
   - reads the stream with `fetch` and a stream reader,
   - on a frame, fetches the page HTML and swaps each region by its id,
   - falls back to a timed fetch when the stream fails,
   - pauses while the tab is hidden.

   The list form gets a `filter` field. A detail page of a run that ended, or
   a page rendered after a rejected action, has no live script.

### 0.6 Out of scope

- `NOT`. A negated GIN predicate cannot use the index.
- The grammar on `GET /workflows/count` and in the CLI.
- A server-side filter on the change stream.
- Other Vantage pages. They keep the `?refresh=N` meta refresh.

---

## 1. TDD plan and acceptance criteria

| Done-when item | Red test | Green change |
|----------------|----------|--------------|
| The list API accepts an OR filter and returns correct results (precedence and grouping). | `visibility_query` unit tests: precedence, groups, errors, SQL text. `workflow_filter_integration`: `filter_or_precedence_and_grouping`, plus the stalled path and two shards. | `visibility_query` module. `filter` arm in `parse_workflow_filters`. One helper in the three loaders. |
| `EXPLAIN` for a representative OR query uses an index on a seeded fixture. | `filter_or_explain_uses_the_search_index` seeds 20 000 rows and runs `EXPLAIN` on the exact list query. | `explain_workflow_list` test hook. The anchor rule. |
| Vantage list and detail pages update without a manual reload. | `ui_integration`: the pages load `live.js` and mark regions. `workflow_filter_integration`: the change stream sends `changed` after an event. | `GET /workflows/changes/stream`. `assets/live.js`. Region marks. |

## 2. Known limits

- The planner can still choose another plan, for example the
  `(created_at, id)` index for a broad filter. The anchor rule makes an index
  path possible. It does not force one.
- A non-admin viewer gets the timed fetch, not the stream.
- Each open change stream holds one `LISTEN` connection for each shard.
