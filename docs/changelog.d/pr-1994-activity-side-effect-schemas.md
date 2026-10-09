## Phase — Schemas for activity payloads and side-effect values (issue #1994)

**What shipped.** Activities publish input and output schemas. Side effects
publish a value schema. `harvest schema check` gates them with the issue #794
ruleset.

- `ActivityInfo` gains `input_schema` and `output_schema`, with
  `with_input_schema_fn`, `with_output_schema_fn` and, under the `schema`
  feature, `with_schemas::<I, O>()`.
- `SideEffectInfo` holds the value schema of one `ctx.side_effect(id, ..)`
  call site. `WorkflowInfo::side_effect(id)` builds one with its workflow
  filled in. The key is `(workflow, id)`.
- `WorkflowSchemaContract::with_activities` and `with_side_effects` add two
  contract sections. Each is left out of the file when it is empty.
- `SchemaDelta` and `AcknowledgedBreakingChange` gain `subject` and
  `side_effect`. Both are left out of the JSON for a workflow. The
  acknowledgement identity includes them, so an ack for workflow `charge`
  does not cover activity `charge`.
- New change kinds: `activity_added` and `activity_removed` (compatible),
  `side_effect_added` (compatible) and `side_effect_removed` (breaking). A
  side-effect declaration is not registered with the runtime, so its removal
  must not stop the check in silence.
- The text report labels each subject: `activity:charge.output`,
  `checkout/side_effect:pick.value`.
- `schema check` and `schema update` refuse a `--current` that drops every
  activity or side effect while the baseline has some. A bare
  `GET /workflows/registered` body has neither section.

**Upgrade note.** `contract_version` is now `2`. This build reads a version
`1` baseline. A version `1` binary refuses a version `2` file. The artifact's
`description` and `compatibility` text changed, so `--require-current` asks
for one `harvest schema update`. A struct literal of `ActivityInfo` needs the
two new fields (`input_schema: None, output_schema: None`).

**No migration. No new `WorkflowEvent` variant. No route change. No engine
change.**

**Tests.** `activity_side_effect_schema_tests` covers publishing, the
contract sections, the differ, the acknowledgement identity and the JSON
shape. `schema_check_cli` has the issue's red-first test,
`schema_check_flags_an_incompatible_activity_output_change`, plus the
side-effect break, the dropped-section guard and the escape hatch. The
`schema_feature_tests` unit tests cover `with_schemas` and `with_schema`.
Design record: `DESIGN-1994.md`.
