# Design — Issue #1994: schemas for activity payloads and side-effect values

Issue #1994 asks for two things:

1. Activities can publish input and output schemas. Side effects can
   publish a value schema.
2. `harvest schema check` flags an incompatible change to an activity
   output type. The test comes first.

Today the gate (issue #794) covers workflow input, output and error only.
Activity results and side-effect values are also read back on replay. A
change to their types can break an in-flight run, and no gate sees it.

**No migration. No new `WorkflowEvent` variant. No route change. No engine
change.** The change adds metadata, extends the contract and extends the
differ's scope.

---

## 0. Planning record

### 0.1 Brainstorm — how can an activity or side effect publish a schema?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Add `input_schema` and `output_schema` to `ActivityInfo`, with `with_input_schema_fn`, `with_output_schema_fn` and `with_schemas::<I, O>()`. | **Adopted.** It is the same shape as `WorkflowInfo` (issue #373). Users learn one pattern. |
| B2 | `#[activity(schema)]` derives the schemas from the signature. | Rejected for now. Workflows do not do this. The macro must know how many parameters encode the input. A follow-up can add it on top of B1. |
| B3 | Add a `side_effect_schemas` list to `WorkflowInfo`. | Rejected. `WorkflowInfo` has 330 struct literals. The churn buys nothing that B4 does not give. |
| B4 | A new `SideEffectInfo { workflow, id, value_schema }`. `WorkflowInfo::side_effect(id)` builds one with the owner filled in. | **Adopted.** A side-effect id is scoped to its workflow, so the key is `(workflow, id)`. No struct gets a new field except `ActivityInfo`. |
| B5 | A typed key: `SideEffectKey<T>` passed to `ctx.side_effect`. | Rejected. It changes a hot API to add metadata. B4 needs no call-site change. |
| B6 | Put activities and side effects into new contract sections, `activities` and `side_effects`. | **Adopted.** The workflow section stays byte-identical. |
| B7 | Reuse `SchemaDelta.workflow` and add a `subject` (`workflow`, `activity`, `side_effect`) and a `side_effect` id. | **Adopted.** The JSON omits `subject` for a workflow delta, so a workflow delta does not change. A JSON consumer must read `subject` before `workflow`. |
| B8 | Serve activity schemas over HTTP. | Rejected for this issue. `GET /activities` (issue #807) lists activities but serves no schemas. The generator is the preferred CI path (see the guide). |

### 0.2 Reverse brainstorm — how can this gate fail to protect a run?

| # | How to make it fail | Mitigation |
|---|---------------------|------------|
| R1 | An ack for workflow `charge` also covers activity `charge`. | The ack identity includes `subject` and the side-effect id. A test pins it. |
| R2 | Feed a bare `GET /workflows/registered` array as `--current`. It has no activity section, so every activity reads as removed. | The CLI refuses a bare body when the baseline has either section. A generated contract is not refused, so an acknowledged removal stays possible. |
| R3 | An old `harvest` binary reads a new artifact and ignores the new sections. | Bump `contract_version` to `2`. An old binary refuses it. A new binary still reads `1`. |
| R4 | Withdraw an activity schema to stop the check. | `SchemaRemoved` stays breaking for every subject. |
| R5 | Two activity registrations share a name. | Last-wins, the same as the runtime and as workflows. |
| R6 | Side effects in two workflows share an id but use different types. | The key is `(workflow, id)`. |
| R7 | A delta for an activity looks like a workflow delta in the text report. | Labels: `activity:charge.output`, `onboarding/side_effect:pick.value`. |
| R8 | A doc-comment edit dirties the new sections. | The new entries go through the same canonicaliser. |
| R9 | The check runs, but the example generator never registers an activity. | The example publishes an activity and a side effect. The CI gate then covers them. |
| R10 | Delete a side-effect declaration to stop the check. The call site stays. | `side_effect_removed` is breaking. The author must acknowledge it. It is compatible only when its workflow goes too. |
| R11 | Drop an activity from the generator, then add it back with a new output type. Both steps read as compatible. | `activity_removed` is breaking (found in review). |
| R12 | Label a version 2 file as version 1, so an old binary skips the new sections. | `parse` refuses a version 1 label on version 2 content (found in review). |
| R13 | The example publishes hand-written schemas, so CI cannot see a change to the example types. | Under the `schema` feature, the example derives the schemas from the types (found in review). |

### 0.3 Six thinking hats

| Hat | Notes |
|-----|-------|
| White | A workflow reads an activity result with `serde_json::from_value` on replay. It reads a side-effect value the same way (`context.rs`, `side_effect`). A retried activity reads its stored input. All three are replay-read. `ActivityInfo` has 170 struct literals. |
| Red | Users want one way to publish a schema. Two patterns for two kinds of payload would feel arbitrary. |
| Black | A new field on `ActivityInfo` breaks a downstream struct literal. A contract version bump makes every embedder regenerate the baseline once. A side-effect id that is built at run time cannot publish a schema. |
| Yellow | The differ is subject-agnostic. Its full ruleset applies to the new payloads. Only the add and remove rules are new. |
| Green | `WorkflowInfo::side_effect(id)` fills the owner, so a user cannot mistype the workflow name. |
| Blue | Red phase: tests for the API, the contract and the CLI fail. Green phase: implement. Refactor phase: docs, artifact, example, gates. Then review from several angles. |

---

## 1. Design

### 1.1 Publishing

```rust
charge_info().with_schemas::<ChargeInput, Receipt>();      // schema feature
charge_info().with_output_schema_fn(receipt_schema);       // no feature
onboarding_info().side_effect("pick_variant").with_schemas::<Variant>();
```

`ActivityInfo` gains `input_schema` and `output_schema`, both
`Option<fn() -> Value>`. `SideEffectInfo` holds `workflow`, `id` and
`value_schema`.

The activity input is the JSON the handler decodes. For more than one
parameter, use the tuple type that the macro encodes.

### 1.2 Contract

```rust
WorkflowSchemaContract::from_infos(version, &workflows)
    .with_activities(&activities)
    .with_side_effects(&side_effects);
```

The contract gains `activities` and `side_effects`. Each section is sorted,
deduplicated last-wins and canonicalised. The JSON omits an empty section.
`contract_version` becomes `2`. `parse` still reads `1`, but refuses a `1`
label on a file with either new section.

### 1.3 Differ

`SchemaDelta` and `AcknowledgedBreakingChange` gain `subject` and
`side_effect`. The differ walks activities (`input`, `output`) and side
effects (`value`, a new `SchemaRole`) with the same rules as workflows.
New change kinds: `activity_added`, `activity_removed`,
`side_effect_added` and `side_effect_removed`.

- An added activity or side effect is compatible. No history exists for it.
- A removed activity is breaking (R11). The generator list is not tied to
  the runtime registry. In-flight runs still decode its recorded results.
- A removed side effect is breaking (R10). A side-effect declaration is not
  registered with the runtime. Its call site can stay after the
  declaration goes, so a removal must not stop the check in silence.
- A side effect removed together with its workflow is compatible. The
  workflow removal is compatible, and issue #520 gates it.

**Limit.** The gate checks the declared type. A workflow call site can decode
a recorded value into another type, and the gate does not see that type.

### 1.4 CLI

The text report labels each subject (R7). The check refuses a bare
`GET /workflows/registered` body when the baseline has a new section (R2).

---

## 2. Test plan

| Test | Phase | Proves |
|------|-------|--------|
| `schema_check_flags_an_incompatible_activity_output_change` (CLI) | Red first | AC 2 |
| `removing_a_field_from_an_activity_output_is_breaking` (core) | Red | The differ walks activities |
| `removing_an_enum_value_from_a_side_effect_is_breaking` (core) | Red | The differ walks side effects |
| `a_workflow_ack_does_not_cover_an_activity_of_the_same_name` (core) | Red | R1 |
| `an_ack_for_one_side_effect_does_not_cover_another` (core) | Review | R1, for the side-effect id |
| `a_current_without_activities_cannot_drop_their_coverage` (CLI) | Red | R2 |
| `removing_the_last_side_effect_can_be_acknowledged` (CLI) | Review | R2 does not block a real removal |
| `a_version_1_contract_still_parses` (core) | Regression | R3 |
| `a_version_1_label_on_version_2_content_is_refused` (core) | Review | R12 |
| `removing_a_side_effect_declaration_is_breaking` (core) | Red | R10 |
| `removing_an_activity_is_breaking` (core) | Review | R11 |
| `an_activity_publishes_input_and_output_schemas` (core) | Red | AC 1 |
| `a_side_effect_publishes_a_value_schema_under_its_workflow` (core) | Red | AC 1 |
| `activity_with_schemas_populates_input_and_output` (unit) | Red | AC 1, `schema` feature |
| `a_derived_activity_output_change_is_breaking` (unit) | Review | AC 2, with derived types |
| `empty_sections_are_left_out_of_the_json` (core) | Regression | B6 |

"Red" tests failed on the stubs of the red commit. "Regression" tests passed
on the stubs, because they pin behaviour that already held. "Review" tests
came from the code review.
