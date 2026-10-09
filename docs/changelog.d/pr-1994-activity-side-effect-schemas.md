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
  contract sections. The file omits each one when it is empty.
- `SchemaDelta` and `AcknowledgedBreakingChange` gain `subject` and
  `side_effect`. The JSON omits both for a workflow. The acknowledgement
  identity includes them, so an ack for workflow `charge` does not cover
  activity `charge`.
- New change kinds: `activity_added`, `activity_removed`,
  `side_effect_added` and `side_effect_removed`. An addition is compatible.
  A removal is breaking: the contract lists are not tied to the runtime
  registry, so a removal must not stop the check in silence. A side effect
  removed together with its workflow is compatible.
- The text report labels each subject: `activity:charge.output`,
  `checkout/side_effect:pick.value`.
- `schema check` and `schema update` refuse a bare
  `GET /workflows/registered` body as `--current` when the baseline has an
  activity or side-effect section. That body has neither section.

**Upgrade note.** `contract_version` is now `2`. This build reads a version
`1` baseline, and refuses a version `1` label on version `2` content. A
version `1` binary refuses a version `2` file. The artifact's `description`
and `compatibility` text changed, so `--require-current` asks for one
`harvest schema update`. A struct literal of `ActivityInfo` needs the two new
fields. `SchemaRole` and `ChangeKind` have new variants. See
`docs/upgrading/0.8.0.md` section 1.3.

**No migration. No new `WorkflowEvent` variant. No route change. No engine
change.**

**Tests.** `activity_side_effect_schema_tests` covers publishing, the
contract sections, the differ, the acknowledgement identity and the JSON
shape. `schema_check_cli` has the issue's red-first test,
`schema_check_flags_an_incompatible_activity_output_change`, plus the
side-effect break, the bare-body guard, an acknowledged removal, the version
upgrade and the escape hatch. The `schema_feature_tests` unit tests cover
both `with_schemas` methods and a derived activity type end to end.
Design record: `DESIGN-1994.md`.
