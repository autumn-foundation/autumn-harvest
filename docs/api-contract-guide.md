# Harvest Management API Contract — Embedder Guide

This guide is for plugin embedders, CLI authors, and UI developers who consume
the Harvest management API.  It explains how to inspect the published contract,
validate your client against it, and understand what counts as a breaking change.

---

## Where to find the contract

`docs/api-contract.json` is the single source of truth.  It is checked into the
repository, so it is always available without running a server or starting a
worker process.  The file version matches the `autumn-harvest-plugin` crate
version (e.g. `"version": "0.4.0"`).

```bash
# Inspect the full route list
jq '.routes[] | {method, path, category, read_only}' docs/api-contract.json

# List only mutating routes
jq '[.routes[] | select(.read_only == false) | {method, path, category}]' \
   docs/api-contract.json

# List only read-only admin routes
jq '[.routes[] | select(.read_only == true and .category == "admin") | .path]' \
   docs/api-contract.json
```

---

## Contract format

| Top-level key | Meaning |
|---|---|
| `version` | Crate version this contract was published with |
| `contract_version` | Contract schema version (currently `"2"`) |
| `compatibility` | Breaking vs. non-breaking change rules |
| `routes` | Array of route entries |

Each route entry:

| Field | Type | Meaning |
|---|---|---|
| `method` | string | HTTP method: `GET`, `POST`, `PATCH`, or `DELETE` |
| `path` | string | Path template using `{param}` placeholders |
| `category` | string | Route group: `workflow`, `dag`, `dlq`, `external_activity`, `worker`, `batch`, `schedule`, `admin`, `health`, `audit` |
| `read_only` | bool | `true` for GET endpoints; `false` for mutating operations |
| `description` | string | Human-readable description |
| `params` | array | Query and path parameters with name, location, required flag, and description |
| `request_body` | object | `required` flag and field schema (null when not applicable) |
| `success_response` | object | HTTP status and response schema description |
| `error_responses` | array | Documented error status codes and conditions |
| `idempotency` | string | (Optional) Idempotency semantics for the route |

---

## Compatibility rules

These rules are stated in the `compatibility` section of the contract:

**Non-breaking (safe to ship without a major contract bump):**
- Adding a new optional response field
- Adding a new optional query parameter
- Adding a new read-only route
- Narrowing an existing error response to be more specific

**Breaking (requires updating the contract, bumping `contract_version`, and a
CHANGELOG entry before release):**
- Removing a response field
- Renaming a response field
- Changing the type of a response field
- Removing a route
- Renaming a route (path or method change)
- Changing a query parameter from optional to required
- Adding a new mutating route without classifying it in the contract

---

## Generating or validating a client

Because the contract is plain JSON, any JSON-aware toolchain can consume it.
A 10-minute workflow to generate a typed Rust client:

```bash
# 1. Extract routes into a simple TSV for code generation scaffolding
jq -r '.routes[] | [.method, .path, .category, (.read_only | tostring)] | @tsv' \
   docs/api-contract.json

# 2. Validate that a hand-written client covers every contract route
#    (example: check that a list of client methods covers all paths)
contract_paths=$(jq -r '.routes[].path' docs/api-contract.json | sort)
# compare against your method list ...
```

For languages with OpenAPI tooling: the contract is not OpenAPI, but its shape
is straightforward to translate.  Each route entry maps 1-to-1 to an OpenAPI
path item; `params` maps to OpenAPI parameters; `request_body.schema` maps to a
`requestBody`.

---

## Using the contract alongside the CLI

The `harvest` CLI uses `api_request()` to translate every subcommand into an
`(method, path, body)` triple.  The contract coverage test in
`autumn-harvest-cli/tests/contract_coverage.rs` asserts that every CLI
subcommand maps to a documented contract route.

To verify a specific CLI command manually:

```bash
# See which route a command uses (dry-run print, no network)
harvest workflow cancel 00000000-0000-0000-0000-000000000001 --dry-run 2>&1 || true

# Cross-reference against the contract
jq '.routes[] | select(.path == "/workflows/{id}/cancel")' docs/api-contract.json
```

---

## Integration with auth posture and audit trail

- **Auth posture (issue #174):** The `category` and `read_only` fields in this
  contract are the input to the auth classification layer.  Read-only routes
  and admin routes can be assigned different auth requirements.  The contract
  records the route surface; auth enforcement is owned by issue #174.

- **Audit trail (issue #158):** Every route where `read_only: false` is
  automatically included in the audit trail when `HarvestPlugin` is active.
  The `GET /admin/audit` endpoint is the read surface for that log.

---

## Regression test

`autumn-harvest-plugin/tests/contract_regression.rs` uses
`management_api_routes()` (from `autumn_harvest_plugin::api`) to compare the
live route set registered in `harvest_api_router` against `docs/api-contract.json`.

**When you add, remove, rename, or change a route:**

1. Update `harvest_api_router` in `autumn-harvest-plugin/src/api.rs`.
2. Update `management_api_routes()` in the same file (keeps the canonical list
   in sync with the router).
3. Update `docs/api-contract.json` to reflect the new route or schema change.
4. Add a CHANGELOG entry under the current version marking the change as
   breaking or non-breaking per the compatibility rules above.
5. Run `cargo test -p autumn-harvest-plugin --test contract_regression` to
   confirm the regression test passes.

The CI job will catch any drift between these three artefacts.
