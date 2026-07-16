### Published interaction schema + `/interface` discovery + boundary validation (issue #610)

Extend the #373 workflow input/output schema story to a workflow's **interaction
surface** — its signals, queries, and updates. Each handler can publish an
argument/response JSON Schema and a description:

- `QueryHandlerInfo`/`UpdateHandlerInfo` gain `description`/`arg_schema`/`response_schema`
  fields; a new `SignalHandlerInfo` type (arg-only, no response). Fluent builders
  `with_description`/`with_arg_schema_fn`/`with_response_schema_fn` and, under the
  `schema` feature, `with_schemas::<Arg, Resp>()` (`<Arg>` for signals). The
  `#[query]`/`#[update]`/`#[signal]` macros parse `description = "…"` and emit a
  public `{fn}_info()` alias; `signals![…]` collector added.
- New read-only management route **`GET /workflows/registered/{name}/interface`**
  returns a `WorkflowInterfaceRecord { signals, queries, updates }`, each entry
  `{ name, description?, arg_schema?, response_schema? }`, sorted by name and
  deterministic across calls. `404` for an unregistered workflow name. Signals
  never carry a `response_schema`.
- **Boundary validation:** when a signal or update handler has a published
  `arg_schema`, the payload is validated *before* durable enqueue at the three
  HTTP entry points — the signal-send route, `POST /workflows/{name}/signal-with-start`,
  and the update route. On failure returns `400`
  `{ "error": "…validation failed", "violations": [{ "message", "field_path" }] }`
  (RFC 6901 pointer), reusing #373's `validate_against_schema`. A handler with no
  published schema is not validated (unchanged behavior).

**No new `WorkflowEvent` variant, no migration, no adjacently-tagged JSON change,
no replay impact** — validation runs at the HTTP boundary before enqueue, and
discovery is a read-only projection of the registry. Example
`autumn-harvest/examples/interface_schema_workflow.rs` (`--features schema`).

**Source-visible change:** each `#[query]`/`#[update]`/`#[signal]`-annotated
function now also emits a public `{fn}_info()` symbol (consistent with the
existing `#[workflow]`/`#[activity]` convention). A downstream crate that
already defines a hand-written `foo_info()` beside `#[query] fn foo` would need
to rename one of them.
