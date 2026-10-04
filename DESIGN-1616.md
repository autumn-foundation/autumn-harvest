# Design — Issue #1616: non-Rust adoption

Part 1 publishes a generated TypeScript client from the release pipeline.
Part 2 evaluates a standalone `harvest-server` binary. Part 2 is a design
note only. It contains no code.

**No migration. No new `WorkflowEvent` variant. No route change.**

---

## 0. Planning record

Three techniques shape this plan. This note keeps the rejected rows, because
they explain the scope.

### 0.1 Brainstorm — how can a non-Rust consumer get a client?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Keep the status quo: each consumer runs `npm run generate` against a live app. | Rejected. The issue exists to remove this step. |
| B2 | Generate from the in-tree `docs/openapi.json` at release time. | **Adopted.** No running app is necessary. CI already keeps the file current. |
| B3 | Publish to the npm registry. | Deferred. It needs a package name and an `NPM_TOKEN` secret. A maintainer owns both decisions. See §1.4. |
| B4 | Attach an `npm pack` tarball to the GitHub release. | **Adopted.** It needs no new secret. `npm install <url>` installs it. |
| B5 | Check the generated `.ts` file into the tree. | Rejected. It doubles the drift surface. The build makes it from `docs/openapi.json`, which a test already pins. |
| B6 | Ship clients for Python, Go and Java too. | Rejected for now. Each language adds a toolchain to CI. `docs/openapi.md` already shows the generator commands. |
| B7 | Type every response field before the first release. | Rejected. 868 top-level fields on 116 operations have no type. A wrong type is worse than `unknown`, and each type needs proof. |
| B8 | Type the core lifecycle routes now, and prove each type against the real handler. | **Adopted.** See §1.1. |
| B9 | Wrap `openapi-fetch` in a small `createHarvestClient` function. | **Adopted.** The package then gives a client, not only types. |

### 0.2 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | Publish a type that does not match the wire. A consumer then trusts a lie. | `openapi_response_conformance` drives each core route against Postgres. It checks every body against the published schema. |
| R2 | Publish a client whose version does not match the server. | The package version must equal the crate version. `openapi_spec` pins this. The release job fails when the tag differs. |
| R3 | Create the GitHub release, then fail to build the client. | The release job builds the client before it creates the release. |
| R4 | Let the client build break without notice between releases. | The `typescript-client-package` CI job builds and tests it on each pull request. |
| R5 | Add a type name that no generator knows (`int`, `str`), or misspell a key. | The transform rejects a type outside the contract set and an unknown field key. |
| R6 | Remove a type from a core route later. | `openapi_spec::core_client_routes_type_every_response_field` fails. |
| R7 | Publish under an npm name that the project does not own. | No registry publish. See B3. |
| R8 | Make the client need a running app again. | The build reads `docs/openapi.json` only. The CI job has no service container. |

### 0.3 Six thinking hats

| Hat | Finding |
|-----|---------|
| White (facts) | Before this change, 89 of 957 top-level response properties have a type. #1411 typed every parameter. Request bodies list their fields, but most field values have no type. The release pipeline makes a GitHub release only. It publishes no crate. |
| Red (feeling) | A client full of `unknown` feels unfinished. The first call a consumer makes (start, then status) must feel typed. |
| Black (risk) | A wrong type is a silent bug in each consumer. Release-time code runs rarely, so a break there hides for weeks. |
| Yellow (benefit) | Paths, methods and parameters have types today. With typed core responses, a consumer writes a full start-and-poll loop with no casts. |
| Green (ideas) | Allow `nullable` and nested `fields` in the contract, so `execution.state` gets a type. Grow the typed set route by route after this change. |
| Blue (process) | Decide the typing question first, as the issue asks. Then TDD: red tests, green code, refactor. Part 2 stays a note. |

---

## 1. Part 1 — the published client

### 1.1 Decision: response typing

Response typing is a prerequisite **for the core lifecycle routes only**.
It is not a prerequisite for the whole document.

These routes get a type on every response field:

| Route | Use |
|-------|-----|
| `POST /workflows/{workflow_name}/start` | Start a run. |
| `GET /workflows/{id}` | Read the status. |
| `GET /workflows/{id}/result` | Read the outcome. |
| `POST /workflows/{id}/signal/{signal_name}` | Send a signal. |
| `POST /workflows/{id}/cancel` | Cancel a run. |
| `POST /workflows/{id}/terminate` | Terminate a run. |
| `GET /health` | Check that the server is ready. |

Other routes keep open properties. A type moves into the set when a test can
prove it. The list is `CORE_CLIENT_ROUTES` in `autumn_harvest_plugin::openapi`
(`src/openapi.rs`).

A value that the handler passes through, such as workflow input, has the type
`any`. A client sees it as `unknown`. No live test creates an external
hand-off yet, so the elements of `external_handoffs` are `any` too.

### 1.2 Contract additions

A contract field object can now carry:

- `type`: one of `string`, `integer`, `number`, `boolean`, `object`, `array`
  or `any`. `any` publishes `x-harvest-any`. The transform rejects any other
  name.
- `nullable: true`: the field can be JSON `null`. OpenAPI 3.1 writes this as
  `"type": ["string", "null"]`.
- `fields`: the properties of an `object` field, in the same form.
- `items`: the element type of an `array` field.

The transform rejects an unknown key, so a typo fails the build.

### 1.3 Package and pipeline

- `clients/typescript` holds the package `autumn-harvest-client`.
- `scripts/build-typescript-client.sh` generates `src/harvest-api.ts` from
  `docs/openapi.json`, then type-checks, tests, builds and packs it.
- CI runs the script in `typescript-client-package` on each pull request.
- `release.yml` runs the script before it creates the release. It attaches the
  tarball to the release.

### 1.4 Not done

- **npm registry publish.** It needs a package name and an `NPM_TOKEN`
  secret. Add one step after `npm pack` when a maintainer decides both.
- **Types for the other routes.** 797 top-level fields on 109 operations have
  no type. Each one needs its own proof.

---

## 2. Part 2 — a standalone `harvest-server` binary (evaluation)

### 2.1 The question

A server binary separates "run the engine" from "build a Rust web app". The
open question is how a binary registers workflows when it is not the
embedder's own `main`.

### 2.2 What exists

- `harvest-dev` (#525) provisions Postgres, migrates, runs a worker and serves
  the API. It registers only the built-in `dev_greeting` sample. It refuses a
  non-local database and deletes its state on exit.
- `HarvestEmbedding` (#1613) runs the standalone startup sequence on plain Axum.
- `examples/standalone-runner` (#1615) is a production-shaped binary with no
  `autumn-web` entry in its manifest. It has token auth, metrics, webhooks and
  a signal-driven drain.

### 2.3 Options

| # | Option | Verdict |
|---|--------|---------|
| S1 | A prebuilt binary that loads workflows from a Rust `dylib`. | Rejected. Rust has no stable ABI. The `dylib` must use the same compiler and the same crate versions. That is a hidden build coupling. A mismatch causes undefined behavior. The `hot-code-swap` spike (#967, §3) calls `dylib` hosting a permanent no-go. |
| S2 | A prebuilt binary that runs workflows as WASM modules. | Rejected for now. `wasm-activities` (#965) runs activities only. The `hot-code-swap` spike (#967) runs WASM workflows. Its §9 verdict is a no-go for full `WorkflowContext` parity (T3). The sequential-shape tier (T2) is a conditional go, and only after WASM activities ship (T1). ADR 0002 keeps workflow authoring in Rust. |
| S3 | A remote worker protocol for workflows in other languages. | Rejected by ADR 0002. |
| S4 | A documented thin-binary pattern: the embedder writes a small `main` that registers workflows and calls `HarvestEmbedding`. | **Recommended.** `HarvestEmbedding` wraps `HarvestRunner`, so this is the issue's "thin binary around `HarvestRunner`". `examples/standalone-runner` already is this binary. |
| S5 | A `cargo generate` template made from S4. | Recommended as the next step. It gives a non-Rust shop one command to a buildable server crate. |

### 2.4 Recommendation

Do not ship a prebuilt `harvest-server`. Workflows are Rust code, so the same
build must compile the binary and the workflows. The embedder's binary is the
server.

Promote `examples/standalone-runner` as the reference server. The next issue
makes a `cargo generate` template from it. The template removes the example
`standalone_order` and `standalone_shipping` workflows and leaves one
registration point. A non-Rust shop then
writes Rust workflow functions only. It writes no web code and makes no
`autumn-web` decision. It talks to the server through the published client.

### 2.5 Gaps before S5

- `HarvestEmbedding::start` still returns an `autumn-web` error type (#1615
  known limits).
- The webhook config types are `autumn-web` types behind a re-export.
- `harvest preflight` reports `admin_auth_boundary: fail` for the token-only
  posture (#1615 known limits).
