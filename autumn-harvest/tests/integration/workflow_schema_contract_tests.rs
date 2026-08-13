//! Payload-schema compatibility gate (issue #794).
//!
//! These tests are the falsifiable statement of the compatibility ruleset
//! required by AC-3. Every rule gets: a baseline schema, a current schema, the
//! expected verdict, and — for every rule that is a *validation* narrowing — an
//! independent ORACLE check against the engine's own
//! [`autumn_harvest::info::validate_against_schema`].
//!
//! No database, no network: the whole gate is pure `serde_json` analysis.

use autumn_harvest::info::validate_against_schema;
use autumn_harvest::schema_contract::{
    AcknowledgedBreakingChange, ChangeKind, MAX_DELTAS, SCHEMA_CONTRACT_VERSION,
    SchemaContractDiff, SchemaRole, Verdict, WorkflowSchemaContract, WorkflowSchemaEntry,
    canonicalize_schema, dropped_acknowledgements, unacknowledged_breaking,
};
use serde_json::{Value, json};

// ── helpers ──────────────────────────────────────────────────────────────────

/// A contract holding a single workflow whose `input` schema is `schema`.
fn input_contract(schema: Value) -> WorkflowSchemaContract {
    WorkflowSchemaContract::from_entries(
        "0.0.0-test",
        vec![WorkflowSchemaEntry {
            name: "wf".to_string(),
            description: None,
            input_schema: Some(schema),
            output_schema: None,
            error_schema: None,
        }],
    )
}

/// Diff two `input` schemas for the same workflow.
fn diff_input(baseline: Value, current: Value) -> SchemaContractDiff {
    autumn_harvest::schema_contract::diff_schema_contracts(
        &input_contract(baseline),
        &input_contract(current),
    )
}

#[track_caller]
fn assert_breaking(baseline: Value, current: Value, expect: ChangeKind) {
    let diff = diff_input(baseline, current);
    assert!(
        diff.has_breaking(),
        "expected a BREAKING delta ({expect:?}) but got: {:#?}",
        diff.deltas
    );
    assert!(
        diff.deltas
            .iter()
            .any(|d| d.change == expect && d.verdict == Verdict::Breaking),
        "expected a breaking delta of kind {expect:?}, got: {:#?}",
        diff.deltas
    );
}

#[track_caller]
fn assert_compatible(baseline: Value, current: Value) {
    let diff = diff_input(baseline, current);
    assert!(
        !diff.has_breaking(),
        "expected NO breaking delta, got: {:#?}",
        diff.deltas
            .iter()
            .filter(|d| d.verdict == Verdict::Breaking)
            .collect::<Vec<_>>()
    );
}

/// Like [`assert_compatible`], but also pins that the delta was actually
/// *reported*.
///
/// `assert_compatible` alone only says "nothing broke", which stays true if the
/// rule stops emitting anything at all — so a deleted non-breaking rule is
/// invisible to it. Naming the expected [`ChangeKind`] makes the compatible
/// half of the ruleset falsifiable too.
#[track_caller]
fn assert_compatible_delta(baseline: Value, current: Value, expect: ChangeKind) {
    let diff = diff_input(baseline, current);
    assert!(
        !diff.has_breaking(),
        "expected NO breaking delta, got: {:#?}",
        diff.deltas
            .iter()
            .filter(|d| d.verdict == Verdict::Breaking)
            .collect::<Vec<_>>()
    );
    assert!(
        diff.deltas
            .iter()
            .any(|d| d.change == expect && d.verdict == Verdict::Compatible),
        "expected a compatible delta of kind {expect:?}, got: {:#?}",
        diff.deltas
    );
}

/// The single reason string for a diff expected to hold exactly one delta.
#[track_caller]
fn sole_reason(baseline: Value, current: Value) -> String {
    let diff = diff_input(baseline, current);
    assert_eq!(
        diff.deltas.len(),
        1,
        "expected one delta: {:#?}",
        diff.deltas
    );
    diff.deltas[0].reason.clone()
}

/// The ORACLE: an instance that is valid under `baseline` and INVALID under
/// `current` proves the change is objectively breaking, independently of the
/// differ. Uses the engine's own validator — the same code that gates
/// `POST /workflows/{name}/start`.
#[track_caller]
fn assert_oracle_agrees_breaking(baseline: &Value, current: &Value, recorded: &Value) {
    assert!(
        validate_against_schema(baseline, recorded).is_ok(),
        "oracle fixture is wrong: the recorded instance must be VALID under the baseline schema"
    );
    assert!(
        validate_against_schema(current, recorded).is_err(),
        "oracle disagrees: the recorded instance still validates under the current schema, so \
         this change is not a validation narrowing"
    );
}

// ── AC-3: the ruleset, rule by rule ─────────────────────────────────────────

#[test]
fn adding_a_required_property_is_breaking() {
    let baseline = json!({"type":"object","properties":{"a":{"type":"string"}},"required":["a"]});
    let current = json!({
        "type":"object",
        "properties":{"a":{"type":"string"},"b":{"type":"string"}},
        "required":["a","b"]
    });
    assert_breaking(
        baseline.clone(),
        current.clone(),
        ChangeKind::RequiredPropertyAdded,
    );
    assert_oracle_agrees_breaking(&baseline, &current, &json!({"a":"x"}));
}

#[test]
fn adding_an_optional_property_is_compatible() {
    assert_compatible(
        json!({"type":"object","properties":{"a":{"type":"string"}},"required":["a"]}),
        json!({
            "type":"object",
            "properties":{"a":{"type":"string"},"b":{"type":["string","null"]}},
            "required":["a"]
        }),
    );
}

#[test]
fn removing_a_property_is_breaking() {
    // serde tolerates the extra key on read, but the recorded value is silently
    // DROPPED — and this is also how a rename surfaces (remove + add).
    assert_breaking(
        json!({"type":"object","properties":{"a":{"type":"string"},"b":{"type":"string"}},"required":["a"]}),
        json!({"type":"object","properties":{"a":{"type":"string"}},"required":["a"]}),
        ChangeKind::PropertyRemoved,
    );
}

#[test]
fn renaming_a_property_is_breaking_via_both_halves() {
    let baseline =
        json!({"type":"object","properties":{"email":{"type":"string"}},"required":["email"]});
    let current = json!({
        "type":"object",
        "properties":{"email_address":{"type":"string"}},
        "required":["email_address"]
    });
    let diff = diff_input(baseline.clone(), current.clone());
    assert!(diff.has_breaking());
    let kinds: Vec<ChangeKind> = diff.deltas.iter().map(|d| d.change).collect();
    assert!(
        kinds.contains(&ChangeKind::PropertyRemoved),
        "the removed half of the rename must be reported: {kinds:?}"
    );
    assert!(
        kinds.contains(&ChangeKind::RequiredPropertyAdded),
        "the added-required half of the rename must be reported: {kinds:?}"
    );
    // The added-required half is objectively a validation narrowing.
    assert_oracle_agrees_breaking(&baseline, &current, &json!({"email":"a@b.c"}));
}

#[test]
fn making_an_optional_property_required_is_breaking() {
    let baseline = json!({"type":"object","properties":{"a":{"type":"string"}}});
    let current = json!({"type":"object","properties":{"a":{"type":"string"}},"required":["a"]});
    assert_breaking(
        baseline.clone(),
        current.clone(),
        ChangeKind::PropertyBecameRequired,
    );
    assert_oracle_agrees_breaking(&baseline, &current, &json!({}));
}

#[test]
fn making_a_required_property_optional_is_compatible() {
    assert_compatible(
        json!({"type":"object","properties":{"a":{"type":"string"}},"required":["a"]}),
        json!({"type":"object","properties":{"a":{"type":"string"}}}),
    );
}

#[test]
fn narrowing_a_type_is_breaking() {
    let baseline = json!({"type":"object","properties":{"a":{"type":"string"}}});
    let current = json!({"type":"object","properties":{"a":{"type":"integer"}}});
    assert_breaking(baseline.clone(), current.clone(), ChangeKind::TypeNarrowed);
    assert_oracle_agrees_breaking(&baseline, &current, &json!({"a":"x"}));
}

#[test]
fn widening_a_type_is_compatible() {
    // `String` -> `Option<String>` (schemars emits `["string","null"]`).
    assert_compatible(
        json!({"type":"object","properties":{"a":{"type":"string"}}}),
        json!({"type":"object","properties":{"a":{"type":["string","null"]}}}),
    );
    // `i64` -> `f64`: every recorded integer is a valid number.
    assert_compatible(
        json!({"type":"object","properties":{"a":{"type":"integer"}}}),
        json!({"type":"object","properties":{"a":{"type":"number"}}}),
    );
}

#[test]
fn number_to_integer_is_breaking() {
    let baseline = json!({"type":"object","properties":{"a":{"type":"number"}}});
    let current = json!({"type":"object","properties":{"a":{"type":"integer"}}});
    assert_breaking(baseline.clone(), current.clone(), ChangeKind::TypeNarrowed);
    assert_oracle_agrees_breaking(&baseline, &current, &json!({"a":1.5}));
}

#[test]
fn removing_an_enum_variant_is_breaking() {
    // schemars 0.8 emits a unit-only Rust enum as a top-level `enum` array.
    let baseline = json!({"type":"string","enum":["A","B","C"]});
    let current = json!({"type":"string","enum":["A","B"]});
    assert_breaking(
        baseline.clone(),
        current.clone(),
        ChangeKind::EnumValueRemoved,
    );
    assert_oracle_agrees_breaking(&baseline, &current, &json!("C"));
}

#[test]
fn adding_an_enum_variant_is_compatible() {
    assert_compatible(
        json!({"type":"string","enum":["A","B"]}),
        json!({"type":"string","enum":["A","B","C"]}),
    );
}

#[test]
fn removing_a_data_carrying_enum_variant_is_breaking() {
    // schemars 0.8 emits a data-carrying Rust enum as `oneOf`, and collapses ALL
    // unit variants into ONE branch that it emits FIRST — so branches must be
    // matched by discriminator, never by index.
    let baseline = json!({"oneOf":[
        {"type":"string","enum":["C"]},
        {"type":"object","required":["A"],"properties":{"A":{"type":"integer"}}},
        {"type":"object","required":["B"],"properties":{"B":{"type":"string"}}}
    ]});
    let current = json!({"oneOf":[
        {"type":"string","enum":["C"]},
        {"type":"object","required":["A"],"properties":{"A":{"type":"integer"}}}
    ]});
    assert_breaking(baseline, current, ChangeKind::VariantRemoved);
}

#[test]
fn adding_a_data_carrying_enum_variant_is_compatible_even_when_reordered() {
    // Adding the first UNIT variant inserts a branch at index 0 and shifts every
    // existing branch — a positional differ would report a false-positive storm.
    let baseline = json!({"oneOf":[
        {"type":"object","required":["A"],"properties":{"A":{"type":"integer"}}}
    ]});
    let current = json!({"oneOf":[
        {"type":"string","enum":["NewUnit"]},
        {"type":"object","required":["A"],"properties":{"A":{"type":"integer"}}}
    ]});
    assert_compatible(baseline, current);
}

#[test]
fn narrowing_inside_a_matched_oneof_branch_is_breaking() {
    let baseline = json!({"oneOf":[
        {"type":"object","required":["A"],"properties":{"A":{"type":"string"}}}
    ]});
    let current = json!({"oneOf":[
        {"type":"object","required":["A"],"properties":{"A":{"type":"integer"}}}
    ]});
    assert_breaking(baseline, current, ChangeKind::TypeNarrowed);
}

// ── Pre-mortem F2: numeric width lives ONLY in `format` ─────────────────────

#[test]
fn narrowing_an_integer_width_is_breaking() {
    // `i64` -> `i32` changes ONLY `format`; `type` stays "integer". Treating
    // `format` as an ignorable annotation would make this textbook break invisible.
    assert_breaking(
        json!({"type":"object","properties":{"a":{"type":"integer","format":"int64"}}}),
        json!({"type":"object","properties":{"a":{"type":"integer","format":"int32"}}}),
        ChangeKind::NumericFormatNarrowed,
    );
    assert_breaking(
        json!({"type":"object","properties":{"a":{"type":"number","format":"double"}}}),
        json!({"type":"object","properties":{"a":{"type":"number","format":"float"}}}),
        ChangeKind::NumericFormatNarrowed,
    );
}

#[test]
fn widening_an_integer_width_is_compatible() {
    assert_compatible(
        json!({"type":"object","properties":{"a":{"type":"integer","format":"int32"}}}),
        json!({"type":"object","properties":{"a":{"type":"integer","format":"int64"}}}),
    );
    // unsigned -> wider signed still covers every recorded value
    assert_compatible(
        json!({"type":"object","properties":{"a":{"type":"integer","format":"uint32"}}}),
        json!({"type":"object","properties":{"a":{"type":"integer","format":"int64"}}}),
    );
}

#[test]
fn signed_to_unsigned_same_width_is_breaking() {
    assert_breaking(
        json!({"type":"object","properties":{"a":{"type":"integer","format":"int32"}}}),
        json!({"type":"object","properties":{"a":{"type":"integer","format":"uint32"}}}),
        ChangeKind::NumericFormatNarrowed,
    );
}

#[test]
fn non_numeric_formats_are_annotations_and_never_produce_a_delta() {
    // The engine's validator ignores `format` entirely; a string is a string.
    let diff = diff_input(
        json!({"type":"object","properties":{"a":{"type":"string","format":"uuid"}}}),
        json!({"type":"object","properties":{"a":{"type":"string","format":"date-time"}}}),
    );
    assert!(diff.deltas.is_empty(), "got: {:#?}", diff.deltas);
}

// ── Bounds, additionalProperties, maps, tuples ──────────────────────────────

#[test]
fn tightening_a_bound_is_breaking() {
    let baseline = json!({"type":"object","properties":{"a":{"type":"string"}}});
    let current = json!({"type":"object","properties":{"a":{"type":"string","minLength":5}}});
    assert_breaking(
        baseline.clone(),
        current.clone(),
        ChangeKind::BoundTightened,
    );
    assert_oracle_agrees_breaking(&baseline, &current, &json!({"a":"ab"}));

    assert_breaking(
        json!({"type":"integer","minimum":0}),
        json!({"type":"integer","minimum":10}),
        ChangeKind::BoundTightened,
    );
}

#[test]
fn relaxing_a_bound_is_compatible() {
    assert_compatible(
        json!({"type":"object","properties":{"a":{"type":"string","minLength":5}}}),
        json!({"type":"object","properties":{"a":{"type":"string"}}}),
    );
}

#[test]
fn restricting_additional_properties_is_breaking() {
    // `#[serde(deny_unknown_fields)]` — every recorded payload carrying a since-
    // removed key now hard-fails, not silently drops.
    let baseline = json!({"type":"object","properties":{"a":{"type":"string"}}});
    let current =
        json!({"type":"object","properties":{"a":{"type":"string"}},"additionalProperties":false});
    assert_breaking(
        baseline.clone(),
        current.clone(),
        ChangeKind::AdditionalPropertiesRestricted,
    );
    assert_oracle_agrees_breaking(&baseline, &current, &json!({"a":"x","legacy":1}));
}

#[test]
fn relaxing_additional_properties_is_compatible() {
    assert_compatible(
        json!({"type":"object","properties":{"a":{"type":"string"}},"additionalProperties":false}),
        json!({"type":"object","properties":{"a":{"type":"string"}}}),
    );
}

#[test]
fn narrowing_a_map_value_type_is_breaking() {
    // `HashMap<String, i64>` has no `properties` at all — only
    // `additionalProperties`. The differ must recurse into it.
    let baseline = json!({"type":"object","additionalProperties":{"type":"string"}});
    let current = json!({"type":"object","additionalProperties":{"type":"integer"}});
    assert_breaking(baseline, current, ChangeKind::TypeNarrowed);
}

#[test]
fn narrowing_an_array_item_type_is_breaking() {
    let baseline = json!({"type":"array","items":{"type":"string"}});
    let current = json!({"type":"array","items":{"type":"integer"}});
    assert_breaking(baseline, current, ChangeKind::TypeNarrowed);
}

#[test]
fn narrowing_a_tuple_element_type_is_breaking() {
    // schemars emits a Rust tuple struct as array-form `items`.
    let baseline = json!({"type":"array","items":[{"type":"string"},{"type":"integer"}],"minItems":2,"maxItems":2});
    let current = json!({"type":"array","items":[{"type":"integer"},{"type":"integer"}],"minItems":2,"maxItems":2});
    assert_breaking(baseline, current, ChangeKind::TypeNarrowed);
}

#[test]
fn changing_tuple_arity_is_breaking() {
    assert_breaking(
        json!({"type":"array","items":[{"type":"string"}],"minItems":1,"maxItems":1}),
        json!({"type":"array","items":[{"type":"string"},{"type":"integer"}],"minItems":2,"maxItems":2}),
        ChangeKind::TupleArityChanged,
    );
}

// ── $ref resolution + cycle safety ──────────────────────────────────────────

#[test]
fn narrowing_behind_a_ref_is_breaking() {
    // A naive top-level diff sees an unchanged `{"$ref":"#/definitions/Inner"}`
    // on both sides and misses the change inside the definition entirely.
    let baseline = json!({
        "type":"object",
        "properties":{"inner":{"$ref":"#/definitions/Inner"}},
        "definitions":{"Inner":{"type":"object","properties":{"z":{"type":"string"}}}}
    });
    let current = json!({
        "type":"object",
        "properties":{"inner":{"$ref":"#/definitions/Inner"}},
        "definitions":{"Inner":{"type":"object","properties":{"z":{"type":"integer"}}}}
    });
    assert_breaking(baseline, current, ChangeKind::TypeNarrowed);
}

#[test]
fn a_ref_and_its_inlined_equivalent_are_not_a_false_positive() {
    let baseline = json!({
        "type":"object",
        "properties":{"inner":{"$ref":"#/definitions/Inner"}},
        "definitions":{"Inner":{"type":"object","properties":{"z":{"type":"string"}}}}
    });
    let current = json!({
        "type":"object",
        "properties":{"inner":{"type":"object","properties":{"z":{"type":"string"}}}}
    });
    assert_compatible(baseline, current);
}

#[test]
fn a_recursive_ref_cycle_terminates() {
    // schemars emits a genuine cycle for a recursive Rust type.
    let recursive = json!({
        "type":"object",
        "properties":{"children":{"type":"array","items":{"$ref":"#/definitions/R"}}},
        "definitions":{"R":{"type":"object","properties":{
            "children":{"type":"array","items":{"$ref":"#/definitions/R"}}
        }}}
    });
    let diff = diff_input(recursive.clone(), recursive);
    assert!(!diff.has_breaking());
}

#[test]
fn a_deeply_nested_schema_stops_at_the_depth_cap_rather_than_recursing() {
    // Just past the differ's cap — deep enough that the cap must engage, and
    // shallow enough to be safe on every platform's test-thread stack.
    //
    // Deliberately NOT a "survives depth 5000" test: a bare `drop(value)` of a
    // `serde_json::Value` that deep overflows with zero library code on the
    // stack, so no library can defend against it and asserting otherwise would
    // be untestable theatre. The externally-reachable bound is the parse test
    // below; this one pins that the in-process cap fires and *says so*.
    let mut deep = json!({"type":"string"});
    for _ in 0..200 {
        deep = json!({"type":"object","properties":{"n":deep}});
    }
    let diff = diff_input(deep.clone(), deep);
    assert!(
        diff.deltas
            .iter()
            .any(|d| d.change == ChangeKind::DiffDepthCapReached),
        "the depth cap must be reported, never silently truncated: {:#?}",
        diff.deltas
    );
    assert!(
        diff.has_breaking(),
        "an uncompared branch must fail closed, not pass"
    );
}

#[test]
fn a_deeply_nested_schema_from_a_file_is_rejected_at_parse_and_never_silently_truncated() {
    // The externally-reachable path is a *file*, and `serde_json`'s recursive
    // descent parser enforces its own 128-frame limit — so a hostile contract
    // file is refused with a clear error long before it can reach the differ.
    // This is the load-bearing bound for the CLI; the in-process cap above is
    // defence in depth for an embedder building a `Value` by hand.
    let mut nested = String::new();
    for _ in 0..5_000 {
        nested.push_str(r#"{"type":"object","properties":{"n":"#);
    }
    nested.push_str(r#"{"type":"string"}"#);
    for _ in 0..5_000 {
        nested.push_str("}}");
    }
    let doc = format!(
        r#"{{"version":"v","contract_version":"1","workflows":[{{"name":"w","input_schema":{nested}}}]}}"#
    );
    let err = WorkflowSchemaContract::parse(&doc)
        .expect_err("a pathologically nested contract must be refused, never truncated");
    assert!(
        format!("{err}").contains("recursion"),
        "the refusal must name the reason: {err}"
    );
}

// ── Unanalysed constraint keywords: fail closed ─────────────────────────────

#[test]
fn adding_an_unanalysed_constraint_is_breaking_fail_closed() {
    assert_breaking(
        json!({"type":"object","properties":{"a":{"type":"string"}}}),
        json!({"type":"object","properties":{"a":{"type":"string","pattern":"^x"}}}),
        ChangeKind::UnanalysedConstraintChanged,
    );
}

#[test]
fn removing_an_unanalysed_constraint_is_also_breaking_fail_closed() {
    // "Removing a constraint always loosens" is FALSE in JSON Schema: keywords
    // interact. Dropping a `patternProperties` entry while
    // `additionalProperties: false` remains TIGHTENS the schema — the keys that
    // the pattern used to admit become "additional" and are now rejected.
    // The differ does not model those interactions, so it fails closed in both
    // directions and lets the author acknowledge.
    assert_breaking(
        json!({
            "type":"object",
            "patternProperties":{"^x_":{"type":"string"}},
            "additionalProperties":false
        }),
        json!({"type":"object","additionalProperties":false}),
        ChangeKind::UnanalysedConstraintChanged,
    );
}

#[test]
fn schema_containers_are_not_mistaken_for_unanalysed_constraints() {
    // `definitions`/`$defs` are containers resolved through `$ref`, never
    // constraints — a definition that is added (or renamed) while every live
    // `$ref` still resolves must not fail closed.
    assert_compatible(
        json!({
            "type":"object",
            "properties":{"a":{"$ref":"#/definitions/A"}},
            "definitions":{"A":{"type":"string"}}
        }),
        json!({
            "type":"object",
            "properties":{"a":{"$ref":"#/definitions/A"}},
            "definitions":{"A":{"type":"string"},"Unused":{"type":"integer"}}
        }),
    );
}

// ── Annotation churn must never dirty the gate ──────────────────────────────

#[test]
fn annotation_only_changes_produce_zero_deltas() {
    // A doc-comment edit changes schemars' `description`/`title`. It must not
    // produce a delta, and must not force a baseline regeneration.
    let diff = diff_input(
        json!({
            "$schema":"http://json-schema.org/draft-07/schema#",
            "title":"OnboardInput","description":"Old prose.",
            "type":"object",
            "properties":{"a":{"type":"string","description":"Old field prose."}},
            "required":["a"]
        }),
        json!({
            "$schema":"http://json-schema.org/draft-07/schema#",
            "title":"OnboardInput","description":"Brand new prose.",
            "type":"object",
            "properties":{"a":{"type":"string","description":"New field prose.","examples":["x"]}},
            "required":["a"]
        }),
    );
    assert!(diff.deltas.is_empty(), "got: {:#?}", diff.deltas);
}

#[test]
fn documenting_one_unit_enum_variant_is_not_a_catastrophic_false_positive() {
    // MEASURED with schemars 0.8.22: adding a doc comment to ONE unit variant
    // flips the whole representation from a flat `enum` array to a REORDERED
    // `oneOf` of singleton enums. Without normalising the two forms into one, a
    // pure documentation edit reads as "every enum value removed + two branches
    // added" — the single loudest false positive available.
    let diff = diff_input(
        json!({"type":"string","enum":["A","B"]}),
        json!({"oneOf":[
            {"type":"string","enum":["B"]},
            {"description":"doc on A","type":"string","enum":["A"]}
        ]}),
    );
    assert!(diff.deltas.is_empty(), "got: {:#?}", diff.deltas);
}

#[test]
fn the_two_unit_enum_forms_still_detect_a_removed_variant() {
    // Normalising the forms must not blind the rule it exists to protect.
    assert_breaking(
        json!({"oneOf":[{"type":"string","enum":["A"]},{"type":"string","enum":["B"]}]}),
        json!({"type":"string","enum":["A"]}),
        ChangeKind::EnumValueRemoved,
    );
}

#[test]
fn enum_value_reordering_alone_is_not_a_delta() {
    let diff = diff_input(
        json!({"type":"string","enum":["A","B","C"]}),
        json!({"type":"string","enum":["C","A","B"]}),
    );
    assert!(diff.deltas.is_empty(), "got: {:#?}", diff.deltas);
}

#[test]
fn wrapping_a_struct_field_in_option_is_compatible_in_both_representations() {
    // `Option<T>` has TWO schemars representations: `["string","null"]` for a
    // primitive, and `anyOf:[{$ref},{null}]` for a non-primitive. Both are a
    // strict widening.
    assert_compatible(
        json!({
            "type":"object",
            "properties":{"inner":{"$ref":"#/definitions/Inner"}},
            "required":["inner"],
            "definitions":{"Inner":{"type":"object","properties":{"z":{"type":"string"}}}}
        }),
        json!({
            "type":"object",
            "properties":{"inner":{"anyOf":[{"$ref":"#/definitions/Inner"},{"type":"null"}]}},
            "definitions":{"Inner":{"type":"object","properties":{"z":{"type":"string"}}}}
        }),
    );
}

#[test]
fn unwrapping_option_on_a_struct_field_is_breaking() {
    assert_breaking(
        json!({
            "type":"object",
            "properties":{"inner":{"anyOf":[{"$ref":"#/definitions/Inner"},{"type":"null"}]}},
            "definitions":{"Inner":{"type":"object"}}
        }),
        json!({
            "type":"object",
            "properties":{"inner":{"$ref":"#/definitions/Inner"}},
            "required":["inner"],
            "definitions":{"Inner":{"type":"object"}}
        }),
        ChangeKind::PropertyBecameRequired,
    );
}

#[test]
fn constraining_previously_unconstrained_additional_properties_is_breaking() {
    // The 3-point lattice: absent/true (anything) > schema > false.
    // `absent -> schema` is a genuine tightening the `false`-toggle rule misses.
    let baseline = json!({"type":"object","properties":{"a":{"type":"string"}}});
    let current = json!({
        "type":"object",
        "properties":{"a":{"type":"string"}},
        "additionalProperties":{"type":"integer"}
    });
    assert_breaking(
        baseline.clone(),
        current.clone(),
        ChangeKind::AdditionalPropertiesRestricted,
    );
    assert_oracle_agrees_breaking(&baseline, &current, &json!({"a":"x","extra":"str"}));
}

#[test]
fn canonicalize_strips_annotations_but_keeps_numeric_format() {
    let out = canonicalize_schema(&json!({
        "$schema":"http://json-schema.org/draft-07/schema#",
        "title":"T","description":"d","default":0,"readOnly":true,
        "type":"integer","format":"int64","minimum":0
    }));
    assert_eq!(out, json!({"type":"integer","format":"int64","minimum":0}));

    let out2 = canonicalize_schema(&json!({"type":"string","format":"uuid","description":"d"}));
    assert_eq!(
        out2,
        json!({"type":"string"}),
        "a non-numeric `format` is a pure annotation and must be stripped"
    );
}

// ── Workflow-level and schema-level deltas ──────────────────────────────────

#[test]
fn adding_a_workflow_type_is_compatible() {
    let baseline = WorkflowSchemaContract::from_entries("0.0.0-test", vec![]);
    let current = input_contract(json!({"type":"object"}));
    let diff = autumn_harvest::schema_contract::diff_schema_contracts(&baseline, &current);
    assert!(!diff.has_breaking(), "got: {:#?}", diff.deltas);
    assert!(
        diff.deltas
            .iter()
            .any(|d| d.change == ChangeKind::WorkflowAdded)
    );
}

#[test]
fn removing_a_workflow_type_is_compatible_and_defers_to_reachability() {
    // Deliberately NOT breaking: `harvest workflow-types reachability` (#520)
    // already gates handler removal and does it BETTER — it counts actual
    // non-terminal executions of the type. Double-gating with a weaker signal
    // would demand an acknowledgement for something #520 already proved safe,
    // which is pure ack fatigue. The delta is still reported, with a reason that
    // routes the operator to the accurate tool.
    let baseline = input_contract(json!({"type":"object"}));
    let current = WorkflowSchemaContract::from_entries("0.0.0-test", vec![]);
    let diff = autumn_harvest::schema_contract::diff_schema_contracts(&baseline, &current);
    assert!(!diff.has_breaking(), "got: {:#?}", diff.deltas);
    let d = diff
        .deltas
        .iter()
        .find(|d| d.change == ChangeKind::WorkflowRemoved)
        .expect("the removal must still be REPORTED, never silent");
    assert_eq!(d.verdict, Verdict::Compatible);
    assert!(
        d.reason.contains("workflow-types reachability"),
        "the reason must route the operator to the accurate #520 tool, got: {}",
        d.reason
    );
}

#[test]
fn publishing_a_schema_for_the_first_time_is_compatible() {
    let baseline = WorkflowSchemaContract::from_entries(
        "0.0.0-test",
        vec![WorkflowSchemaEntry {
            name: "wf".to_string(),
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
        }],
    );
    let current = input_contract(json!({"type":"object","required":["a"]}));
    let diff = autumn_harvest::schema_contract::diff_schema_contracts(&baseline, &current);
    assert!(!diff.has_breaking(), "got: {:#?}", diff.deltas);
}

#[test]
fn unpublishing_a_schema_is_breaking_coverage_regression() {
    let baseline = input_contract(json!({"type":"object","required":["a"]}));
    let current = WorkflowSchemaContract::from_entries(
        "0.0.0-test",
        vec![WorkflowSchemaEntry {
            name: "wf".to_string(),
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
        }],
    );
    let diff = autumn_harvest::schema_contract::diff_schema_contracts(&baseline, &current);
    assert!(
        diff.has_breaking(),
        "deleting `with_schemas` to silence the gate must not be silent"
    );
    assert!(
        diff.deltas
            .iter()
            .any(|d| d.change == ChangeKind::SchemaRemoved)
    );
}

#[test]
fn every_role_is_diffed_independently() {
    let base = WorkflowSchemaContract::from_entries(
        "0.0.0-test",
        vec![WorkflowSchemaEntry {
            name: "wf".to_string(),
            description: None,
            input_schema: Some(json!({"type":"object","properties":{"a":{"type":"string"}}})),
            output_schema: Some(json!({"type":"object","properties":{"b":{"type":"string"}}})),
            error_schema: Some(json!({"type":"string"})),
        }],
    );
    let cur = WorkflowSchemaContract::from_entries(
        "0.0.0-test",
        vec![WorkflowSchemaEntry {
            name: "wf".to_string(),
            description: None,
            input_schema: Some(json!({"type":"object","properties":{"a":{"type":"integer"}}})),
            output_schema: Some(json!({"type":"object","properties":{"b":{"type":"integer"}}})),
            error_schema: Some(json!({"type":"integer"})),
        }],
    );
    let diff = autumn_harvest::schema_contract::diff_schema_contracts(&base, &cur);
    for role in [SchemaRole::Input, SchemaRole::Output, SchemaRole::Error] {
        assert!(
            diff.deltas
                .iter()
                .any(|d| d.role == Some(role) && d.verdict == Verdict::Breaking),
            "role {role:?} must be diffed: {:#?}",
            diff.deltas
        );
    }
}

// ── AC-1 / AC-4: the artifact and the exit-code contract ────────────────────

#[test]
fn a_contract_round_trips_through_json() {
    let c = input_contract(json!({"type":"object","required":["a"]}));
    let json_text = c.to_json_pretty().expect("serialize");
    let back = WorkflowSchemaContract::parse(&json_text).expect("parse");
    let diff = autumn_harvest::schema_contract::diff_schema_contracts(&c, &back);
    assert!(diff.deltas.is_empty(), "got: {:#?}", diff.deltas);
    assert_eq!(back.contract_version, SCHEMA_CONTRACT_VERSION);
}

#[test]
fn parse_accepts_a_bare_registered_workflow_array() {
    // `curl .../workflows/registered` returns a bare array — it must be usable
    // as `--current` with no post-processing.
    let body = json!([
        {"name":"wf","input_schema":{"type":"object"},"output_schema":null,
         "error_schema":null,"mcp":false}
    ])
    .to_string();
    let c = WorkflowSchemaContract::parse(&body).expect("bare array must parse");
    assert_eq!(c.workflows.len(), 1);
    assert_eq!(c.workflows[0].name, "wf");
    assert!(c.workflows[0].input_schema.is_some());
}

#[test]
fn entries_are_sorted_and_deduplicated_last_wins() {
    // The runtime collapses duplicate workflow names into a HashMap (last wins);
    // the generator must match or it publishes a schema the runtime won't use.
    let c = WorkflowSchemaContract::from_entries(
        "0.0.0-test",
        vec![
            WorkflowSchemaEntry {
                name: "zebra".to_string(),
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
            },
            WorkflowSchemaEntry {
                name: "apple".to_string(),
                description: None,
                input_schema: Some(json!({"type":"string"})),
                output_schema: None,
                error_schema: None,
            },
            WorkflowSchemaEntry {
                name: "apple".to_string(),
                description: None,
                input_schema: Some(json!({"type":"integer"})),
                output_schema: None,
                error_schema: None,
            },
        ],
    );
    let names: Vec<&str> = c.workflows.iter().map(|w| w.name.as_str()).collect();
    assert_eq!(names, vec!["apple", "zebra"]);
    assert_eq!(
        c.workflows[0].input_schema,
        Some(json!({"type":"integer"})),
        "last registration must win, mirroring the runtime's HashMap collapse"
    );
}

#[test]
fn generating_the_contract_is_byte_stable() {
    let a = input_contract(
        json!({"type":"object","properties":{"z":{"type":"string"},"a":{"type":"integer"}}}),
    )
    .to_json_pretty()
    .unwrap();
    let b = input_contract(
        json!({"type":"object","properties":{"z":{"type":"string"},"a":{"type":"integer"}}}),
    )
    .to_json_pretty()
    .unwrap();
    assert_eq!(a, b, "the artifact must be byte-stable or the guard flaps");
}

// ── AC-5: the auditable escape hatch ────────────────────────────────────────

#[test]
fn acknowledging_a_breaking_change_records_an_audit_entry() {
    let baseline =
        input_contract(json!({"type":"object","properties":{"email":{"type":"string"}}}));
    let current = input_contract(
        json!({"type":"object","properties":{"email_address":{"type":"string"}},"required":["email_address"]}),
    );
    let updated = baseline
        .acknowledged_update(
            &current,
            "renamed for GDPR; in-flight runs drained via #148 reset",
            Some("docs/changelog.d/pr-794.md"),
        )
        .expect("acknowledging a breaking change must succeed");

    // The new baseline matches current, so the next check is clean...
    let diff = autumn_harvest::schema_contract::diff_schema_contracts(&updated, &current);
    assert!(diff.deltas.is_empty(), "got: {:#?}", diff.deltas);

    // ...and the acknowledgement is a visible, permanent audit record.
    assert!(
        !updated.acknowledged_breaking_changes.is_empty(),
        "the acknowledgement must be recorded in the artifact, never silent"
    );
    let ack = &updated.acknowledged_breaking_changes[0];
    assert_eq!(ack.workflow, "wf");
    assert!(ack.reason.contains("GDPR"));
    assert_eq!(
        ack.recorded_in.as_deref(),
        Some("docs/changelog.d/pr-794.md")
    );
}

#[test]
fn updating_a_baseline_without_a_reason_is_refused_when_breaking() {
    let baseline =
        input_contract(json!({"type":"object","properties":{"email":{"type":"string"}}}));
    let current = input_contract(
        json!({"type":"object","properties":{"email_address":{"type":"string"}},"required":["email_address"]}),
    );
    assert!(
        baseline.acknowledged_update(&current, "   ", None).is_err(),
        "a blank justification must be refused — an acknowledgement without a reason is a rubber stamp"
    );
}

#[test]
fn a_compatible_update_needs_no_acknowledgement() {
    let baseline = input_contract(json!({"type":"object","properties":{"a":{"type":"string"}}}));
    let current = input_contract(
        json!({"type":"object","properties":{"a":{"type":"string"},"b":{"type":["string","null"]}}}),
    );
    let updated = baseline
        .compatible_update(&current)
        .expect("a compatible update needs no acknowledgement");
    assert!(updated.acknowledged_breaking_changes.is_empty());
    let diff = autumn_harvest::schema_contract::diff_schema_contracts(&updated, &current);
    assert!(diff.deltas.is_empty());
}

#[test]
fn a_breaking_update_without_acknowledgement_is_refused() {
    let baseline = input_contract(
        json!({"type":"object","properties":{"a":{"type":"string"}},"required":["a"]}),
    );
    let current = input_contract(
        json!({"type":"object","properties":{"a":{"type":"string"},"b":{"type":"string"}},"required":["a","b"]}),
    );
    assert!(
        baseline.compatible_update(&current).is_err(),
        "regenerating over a breaking delta must require an explicit acknowledgement"
    );
}

#[test]
fn an_acknowledgement_without_a_reason_is_reported_as_breaking() {
    // A hand-edited artifact must not smuggle in a blank rubber stamp.
    let mut c = input_contract(json!({"type":"object"}));
    c.acknowledged_breaking_changes
        .push(AcknowledgedBreakingChange {
            workflow: "wf".to_string(),
            role: Some(SchemaRole::Input),
            field_path: "/email".to_string(),
            change: ChangeKind::PropertyRemoved,
            reason: String::new(),
            recorded_in: None,
        });
    let diff = autumn_harvest::schema_contract::diff_schema_contracts(&c, &c);
    assert!(
        diff.has_breaking(),
        "a blank acknowledgement reason must fail the gate"
    );
}

// ── SUCCESS METRIC: seeded breaking + seeded compatible fixtures ────────────

#[test]
fn success_metric_seeded_breaking_change_trips_the_gate() {
    let baseline = WorkflowSchemaContract::parse(SEED_BASELINE).expect("seed parses");
    let breaking = WorkflowSchemaContract::parse(SEED_BREAKING).expect("seed parses");
    let diff = autumn_harvest::schema_contract::diff_schema_contracts(&baseline, &breaking);
    assert!(
        diff.has_breaking(),
        "the seeded breaking fixture MUST trip the gate: {:#?}",
        diff.deltas
    );
    assert_eq!(diff.exit_code(), 1);
}

#[test]
fn success_metric_seeded_compatible_change_passes_the_gate() {
    let baseline = WorkflowSchemaContract::parse(SEED_BASELINE).expect("seed parses");
    let compatible = WorkflowSchemaContract::parse(SEED_COMPATIBLE).expect("seed parses");
    let diff = autumn_harvest::schema_contract::diff_schema_contracts(&baseline, &compatible);
    assert!(
        !diff.has_breaking(),
        "the seeded compatible fixture MUST pass the gate: {:#?}",
        diff.deltas
    );
    assert_eq!(diff.exit_code(), 0);
}

const SEED_BASELINE: &str = r#"{
  "version": "0.0.0-test",
  "contract_version": "1",
  "workflows": [
    { "name": "onboarding",
      "input_schema": {
        "type": "object",
        "properties": { "user_id": {"type":"integer","format":"int64"},
                        "email": {"type":"string"} },
        "required": ["user_id","email"] } }
  ]
}"#;

/// `email` renamed to `email_address` — the canonical footgun from the issue.
const SEED_BREAKING: &str = r#"{
  "version": "0.0.0-test",
  "contract_version": "1",
  "workflows": [
    { "name": "onboarding",
      "input_schema": {
        "type": "object",
        "properties": { "user_id": {"type":"integer","format":"int64"},
                        "email_address": {"type":"string"} },
        "required": ["user_id","email_address"] } }
  ]
}"#;

/// A new `Option<String>` field plus a doc-comment edit — both safe on replay.
const SEED_COMPATIBLE: &str = r#"{
  "version": "0.0.0-test",
  "contract_version": "1",
  "workflows": [
    { "name": "onboarding",
      "description": "Now with better prose.",
      "input_schema": {
        "title": "OnboardInput",
        "description": "Edited doc comment.",
        "type": "object",
        "properties": { "user_id": {"type":"integer","format":"int64"},
                        "email": {"type":"string"},
                        "referral_code": {"type":["string","null"]} },
        "required": ["user_id","email"] } }
  ]
}"#;

// ── AC-1: the checked-in baseline artifact ──────────────────────────────────

const CHECKED_IN_BASELINE: &str = include_str!("../../../docs/workflow-schema-contract.json");

#[test]
fn the_checked_in_baseline_parses_and_is_self_consistent() {
    let c = WorkflowSchemaContract::parse(CHECKED_IN_BASELINE)
        .expect("docs/workflow-schema-contract.json must be a valid schema contract");
    assert_eq!(c.contract_version, SCHEMA_CONTRACT_VERSION);
    let diff = autumn_harvest::schema_contract::diff_schema_contracts(&c, &c);
    assert!(
        diff.deltas.is_empty(),
        "the checked-in baseline must diff cleanly against itself: {:#?}",
        diff.deltas
    );
}

#[test]
fn every_acknowledgement_in_the_checked_in_baseline_has_a_justification() {
    let c = WorkflowSchemaContract::parse(CHECKED_IN_BASELINE).expect("parse");
    for ack in &c.acknowledged_breaking_changes {
        assert!(
            !ack.reason.trim().is_empty(),
            "acknowledged breaking change for {} has no recorded justification",
            ack.workflow
        );
    }
}

#[test]
fn the_checked_in_baseline_workflows_are_sorted_and_unique() {
    // Asserted against the RAW json, not against `parse` output: `parse` sorts
    // and de-duplicates as it reads, so checking its result would assert a
    // post-condition the parser just established and would pass for a
    // deliberately unsorted file.
    let raw: Value = serde_json::from_str(CHECKED_IN_BASELINE).expect("baseline is valid json");
    let names: Vec<&str> = raw["workflows"]
        .as_array()
        .expect("workflows array")
        .iter()
        .map(|w| w["name"].as_str().expect("name"))
        .collect();
    let mut want = names.clone();
    want.sort_unstable();
    want.dedup();
    assert_eq!(
        names, want,
        "the checked-in baseline must be stored sorted and unique — regenerate it"
    );
}

#[test]
fn the_checked_in_baseline_schemas_are_canonicalised() {
    // Annotation churn must never dirty the file: the stored schemas already
    // have annotations stripped, so a doc-comment edit is a no-op here.
    // Against the RAW json for the same reason as the sorting test: `parse`
    // canonicalises on the way in, so this would be vacuous on its output.
    let raw: Value = serde_json::from_str(CHECKED_IN_BASELINE).expect("baseline is valid json");
    for w in raw["workflows"].as_array().expect("workflows array") {
        let name = w["name"].as_str().unwrap_or("<unnamed>");
        for role in ["input_schema", "output_schema", "error_schema"] {
            let Some(stored) = w.get(role).filter(|v| !v.is_null()) else {
                continue;
            };
            assert_eq!(
                &canonicalize_schema(stored),
                stored,
                "workflow `{name}` stores a non-canonical `{role}` — regenerate the baseline"
            );
        }
    }
}

// ── review hardening: rules that had implementations but no protection ───────
//
// Every test below was added after a mutation-testing pass found that deleting
// or inverting the rule it covers broke no test. Each one is a real rule the
// artifact documents; several are cases where the differ was outright WRONG.

/// `allOf` is a conjunction, so adding a member narrows the schema — and this
/// case was a genuine false-COMPATIBLE: `allOf` sits in `ANALYSED_KEYWORDS`
/// (which exempts it from the fail-closed sweep) while nothing analysed it.
#[test]
fn introducing_an_all_of_conjunction_is_breaking() {
    assert_breaking(
        json!({"type": "integer"}),
        json!({"allOf": [{"type": "integer"}, {"minimum": 10}]}),
        ChangeKind::AllOfMembersChanged,
    );
    // The oracle proves the narrowing is real, not merely differ opinion.
    assert_oracle_agrees_breaking(
        &json!({"type": "integer"}),
        &json!({"allOf": [{"type": "integer"}, {"minimum": 10}]}),
        &json!(1),
    );
}

/// A change *inside* an `allOf` member is reported at its own path, not as a
/// wholesale replacement.
#[test]
fn tightening_a_bound_inside_an_all_of_member_is_breaking() {
    let diff = diff_input(
        json!({"allOf": [{"type": "integer"}, {"minimum": 1}]}),
        json!({"allOf": [{"type": "integer"}, {"minimum": 10}]}),
    );
    assert!(diff.has_breaking(), "{:#?}", diff.deltas);
    assert!(
        diff.deltas
            .iter()
            .any(|d| d.field_path.contains("allOf[1]")),
        "the delta should point INTO the changed member: {:#?}",
        diff.deltas
    );
}

/// Removing a conjunct looks like a loosening but is reported: `allOf` members
/// interact with the parent's `additionalProperties`, which draft-07 does not
/// let see inside subschemas.
#[test]
fn removing_an_all_of_conjunction_is_reported_fail_closed() {
    assert_breaking(
        json!({"allOf": [{"type": "integer"}, {"minimum": 10}]}),
        json!({"type": "integer"}),
        ChangeKind::AllOfMembersChanged,
    );
}

/// The `schemars` `RemoveRefSiblings` shape — a lone `{"allOf":[{"$ref":…}]}`
/// wrapper, produced merely by adding a doc comment to a struct-typed field —
/// must be a **no-op**. Without the canonicalisation flatten this is the single
/// most common false-BREAKING storm: one `///` line would report every nested
/// field as rewritten.
#[test]
fn a_single_member_all_of_ref_wrapper_is_not_a_delta() {
    let defs = json!({"Inner": {"type": "object", "properties": {"a": {"type": "string"}}}});
    let plain = json!({
        "type": "object",
        "properties": {"inner": {"$ref": "#/definitions/Inner"}},
        "definitions": defs,
    });
    let wrapped = json!({
        "type": "object",
        "properties": {"inner": {"allOf": [{"$ref": "#/definitions/Inner"}]}},
        "definitions": defs,
    });
    let diff = diff_input(plain, wrapped);
    assert!(
        diff.deltas.is_empty(),
        "the RemoveRefSiblings wrapper must canonicalise away: {:#?}",
        diff.deltas
    );
}

/// A single-member `allOf` is only lifted when the parent adds nothing of its
/// own; otherwise the conjunction is real and must still be compared.
#[test]
fn a_single_member_all_of_with_a_sibling_constraint_is_not_flattened_away() {
    assert_breaking(
        json!({"type": "integer"}),
        json!({"allOf": [{"minimum": 10}], "type": "integer"}),
        ChangeKind::AllOfMembersChanged,
    );
}

/// `float -> int32` widens on the integer-range lattice but is breaking on the
/// integrality axis: a recorded `1.5` deserializes into no integer type.
#[test]
fn a_fractional_to_integral_numeric_format_is_breaking() {
    assert_breaking(
        json!({"type": "number", "format": "float"}),
        json!({"type": "number", "format": "int32"}),
        ChangeKind::NumericFormatNarrowed,
    );
}

/// A `required` entry with no `properties` counterpart still changes what
/// recorded JSON must contain.
#[test]
fn a_required_key_with_no_properties_entry_is_still_compared() {
    assert_breaking(
        json!({"type": "object", "required": ["a"]}),
        json!({"type": "object", "required": ["a", "b"]}),
        ChangeKind::RequiredPropertyAdded,
    );
    assert_oracle_agrees_breaking(
        &json!({"type": "object", "required": ["a"]}),
        &json!({"type": "object", "required": ["a", "b"]}),
        &json!({"a": 1}),
    );
}

/// JSON Schema's boolean form: `true` accepts everything, `false` accepts
/// nothing. The node is not an object, so no keyword comparison applies — but a
/// change between them is not "no change".
#[test]
fn a_non_object_schema_node_change_is_reported_not_skipped() {
    assert_breaking(
        json!({"type": "object", "additionalProperties": true, "properties": {"a": true}}),
        json!({"type": "object", "additionalProperties": true, "properties": {"a": false}}),
        ChangeKind::UnanalysedConstraintChanged,
    );
}

/// The delta cap bounds the *report*, never the verdict. A diff whose first
/// `MAX_DELTAS` entries are compatible must still fail on breaking changes past
/// the cap — otherwise exit `0` would mean "nothing breaking that fit".
#[test]
fn breaking_changes_past_the_delta_cap_still_count_and_mark_truncation() {
    let n = autumn_harvest::MAX_DELTAS + 50;
    let mut base = serde_json::Map::new();
    for i in 0..n {
        base.insert(format!("f{i}"), json!({"type": "string"}));
    }
    // Every property removed => n breaking deltas, far past the cap.
    let diff = diff_input(
        json!({"type": "object", "properties": Value::Object(base)}),
        json!({"type": "object", "properties": {}}),
    );
    assert!(diff.truncated, "the report must mark itself incomplete");
    assert_eq!(
        diff.deltas.len(),
        autumn_harvest::MAX_DELTAS,
        "storage is capped"
    );
    assert_eq!(diff.breaking_count, n, "but the TALLY is not capped");
    assert!(diff.has_breaking());
    assert_eq!(diff.exit_code(), 1);
}

// ── the upper-bound / item-count family (previously untested) ────────────────

#[test]
fn tightening_each_upper_bound_is_breaking() {
    for (kw, ty, loose, tight, instance) in [
        ("maximum", "integer", json!(100), json!(10), json!(50)),
        ("maxLength", "string", json!(100), json!(2), json!("abcdef")),
    ] {
        assert_breaking(
            json!({"type": ty, kw: loose}),
            json!({"type": ty, kw: tight}),
            ChangeKind::BoundTightened,
        );
        assert_oracle_agrees_breaking(
            &json!({"type": ty, kw: loose}),
            &json!({"type": ty, kw: tight}),
            &instance,
        );
    }
    // maxItems/minItems are analysed by the differ even though the engine's
    // validator ignores them, so they get no oracle — see the guide's note on
    // the analysed set being a deliberate superset.
    assert_breaking(
        json!({"type": "array", "maxItems": 10}),
        json!({"type": "array", "maxItems": 2}),
        ChangeKind::BoundTightened,
    );
    assert_breaking(
        json!({"type": "array", "minItems": 1}),
        json!({"type": "array", "minItems": 5}),
        ChangeKind::BoundTightened,
    );
}

#[test]
fn relaxing_each_upper_bound_is_compatible() {
    assert_compatible_delta(
        json!({"type": "integer", "maximum": 10}),
        json!({"type": "integer", "maximum": 100}),
        ChangeKind::BoundRelaxed,
    );
    assert_compatible_delta(
        json!({"type": "array", "maxItems": 2}),
        json!({"type": "array", "maxItems": 10}),
        ChangeKind::BoundRelaxed,
    );
}

// ── rules that were implemented but had zero regression protection ───────────

#[test]
fn constraining_a_previously_unconstrained_type_is_breaking() {
    assert_breaking(
        json!({}),
        json!({"type": "string"}),
        ChangeKind::TypeNarrowed,
    );
    assert_oracle_agrees_breaking(&json!({}), &json!({"type": "string"}), &json!(1));
}

#[test]
fn introducing_an_enum_where_the_value_was_unconstrained_is_breaking() {
    assert_breaking(
        json!({"type": "string"}),
        json!({"type": "string", "enum": ["a", "b"]}),
        // `BoundTightened` is the shared slug for "a constraint appeared where
        // the value was unconstrained"; the reason names which constraint.
        ChangeKind::BoundTightened,
    );
    assert_oracle_agrees_breaking(
        &json!({"type": "string"}),
        &json!({"type": "string", "enum": ["a", "b"]}),
        &json!("zzz"),
    );
}

#[test]
fn introducing_an_items_restriction_is_breaking() {
    assert_breaking(
        json!({"type": "array"}),
        json!({"type": "array", "items": {"type": "integer"}}),
        ChangeKind::BoundTightened,
    );
    assert_oracle_agrees_breaking(
        &json!({"type": "array"}),
        &json!({"type": "array", "items": {"type": "integer"}}),
        &json!(["a"]),
    );
}

#[test]
fn introducing_a_numeric_format_is_breaking() {
    assert_breaking(
        json!({"type": "integer"}),
        json!({"type": "integer", "format": "int32"}),
        ChangeKind::NumericFormatNarrowed,
    );
}

#[test]
fn changing_items_between_tuple_and_array_form_is_breaking() {
    assert_breaking(
        json!({"type": "array", "items": [{"type": "string"}, {"type": "integer"}]}),
        json!({"type": "array", "items": {"type": "string"}}),
        // Form changes are fail-closed rather than analysed: tuple and
        // single-schema `items` are not comparable member-by-member.
        ChangeKind::UnanalysedConstraintChanged,
    );
}

/// The fail-closed guard for a `oneOf` container that also carries sibling
/// constraint keywords the branch-set comparison would silently drop.
#[test]
fn a_branch_container_with_sibling_constraints_is_reported() {
    assert_breaking(
        json!({"type": "object", "properties": {"a": {"type": "string"}}}),
        json!({
            "oneOf": [{"type": "object", "properties": {"a": {"type": "string"}}}],
            "minProperties": 2,
        }),
        ChangeKind::UnanalysedConstraintChanged,
    );
}

/// Adding a `oneOf` branch is only safe when nothing recorded can match it.
/// `oneOf` demands *exactly one* match, so a non-disjoint addition turns a
/// previously-valid instance into a two-match failure.
#[test]
fn adding_a_non_disjoint_one_of_variant_is_breaking() {
    // The existing branch is a real externally-tagged variant (`{"A": payload}`),
    // so it keys as discriminated; the ADDED branch carries no tag at all, so it
    // can overlap and turns a single match into two.
    let tagged = json!({
        "type": "object",
        "required": ["A"],
        "properties": {"A": {"type": "string"}},
    });
    assert_breaking(
        json!({"oneOf": [tagged]}),
        json!({"oneOf": [tagged, {"type": "object"}]}),
        ChangeKind::VariantAdded,
    );
}

/// Two branches the differ cannot tell apart must fail closed, not collapse.
///
/// Branches are matched across revisions BY KEY in a map, so two same-keyed
/// branches would collapse and one would be silently dropped — hiding a variant
/// REMOVAL behind a same-keyed survivor and reporting it compatible.
#[test]
fn indistinguishable_one_of_branches_fail_closed_rather_than_collapsing() {
    // Neither branch carries a variant tag, a unit `enum`, or a single-property
    // external tag, so both key identically.
    assert_breaking(
        json!({"oneOf": [
            {"type": "object", "properties": {"a": {"type": "string"}}},
            {"type": "object", "properties": {"b": {"type": "string"}}},
        ]}),
        json!({"oneOf": [
            {"type": "object", "properties": {"a": {"type": "string"}}},
        ]}),
        ChangeKind::UnanalysedConstraintChanged,
    );
}

/// An ordinary struct with one mandatory field is NOT an externally-tagged
/// variant, and two of them are not disjoint (`{"id":…,"name":…}` matches both).
#[test]
fn a_single_required_field_struct_is_not_treated_as_a_disjoint_variant() {
    let one = json!({
        "type": "object",
        "required": ["id"],
        "properties": {"id": {"type": "string"}, "note": {"type": "string"}},
    });
    let two = json!({
        "type": "object",
        "required": ["name"],
        "properties": {"name": {"type": "string"}, "note": {"type": "string"}},
    });
    // Adding `two` alongside `one` under `oneOf` can reject `{"id":…,"name":…}`,
    // which previously matched exactly one branch.
    let diff = diff_input(json!({"oneOf": [one]}), json!({"oneOf": [one, two]}));
    assert!(
        diff.has_breaking(),
        "adding an overlapping non-variant branch must not be compatible: {:#?}",
        diff.deltas
    );
}

// ── compatible rules, asserted as REPORTED rather than merely "not breaking" ──

#[test]
fn adding_an_enum_value_is_reported_compatible() {
    assert_compatible_delta(
        json!({"type": "string", "enum": ["a"]}),
        json!({"type": "string", "enum": ["a", "b"]}),
        ChangeKind::EnumValueAdded,
    );
}

#[test]
fn adding_an_optional_property_is_reported_compatible() {
    assert_compatible_delta(
        json!({"type": "object", "properties": {"a": {"type": "string"}}, "required": ["a"]}),
        json!({
            "type": "object",
            "properties": {"a": {"type": "string"}, "b": {"type": "string"}},
            "required": ["a"],
        }),
        ChangeKind::OptionalPropertyAdded,
    );
}

// ── the remediation guidance IS the feature's user value ─────────────────────

#[test]
fn a_removed_property_reason_names_the_serde_alias_remedy() {
    let reason = sole_reason(
        json!({"type": "object", "properties": {"email": {"type": "string"}}}),
        json!({"type": "object", "properties": {}}),
    );
    assert!(
        reason.contains("serde(alias"),
        "a rename is the usual cause; the fix must be named: {reason}"
    );
}

#[test]
fn a_required_property_add_reason_names_the_option_and_default_remedies() {
    let reason = sole_reason(
        json!({"type": "object", "properties": {}}),
        json!({
            "type": "object",
            "properties": {"email": {"type": "string"}},
            "required": ["email"],
        }),
    );
    assert!(reason.contains("Option<T>"), "{reason}");
    assert!(reason.contains("serde(default)"), "{reason}");
}

// ── canonicalisation primitives, asserted directly ───────────────────────────

#[test]
fn canonicalisation_sorts_enum_values() {
    let out = canonicalize_schema(&json!({"type": "string", "enum": ["c", "a", "b"]}));
    assert_eq!(
        out["enum"],
        json!(["a", "b", "c"]),
        "sorting is what makes a reordering a non-delta"
    );
}

/// The acknowledgement log is an **append-only audit trail**, not a live
/// suppression list. Regenerating over a second breaking change must never
/// quietly erase the record of the first — that record is the only permanent
/// evidence of why an earlier migration was judged safe.
#[test]
fn regenerating_a_baseline_carries_prior_acknowledgements_forward() {
    let v1 = input_contract(json!({"type": "object", "properties": {"a": {"type": "string"}}}));
    let v2 = input_contract(json!({"type": "object", "properties": {}}));
    let after_first = v1
        .acknowledged_update(&v2, "first migration: drained via reset", None)
        .expect("first acknowledgement");
    assert_eq!(after_first.acknowledged_breaking_changes.len(), 1);

    // A second, unrelated breaking change.
    let v3 = input_contract(json!({
        "type": "object",
        "properties": {"b": {"type": "string"}},
        "required": ["b"],
    }));
    let after_second = after_first
        .acknowledged_update(&v3, "second migration: pinned by build routing", None)
        .expect("second acknowledgement");

    let reasons: Vec<&str> = after_second
        .acknowledged_breaking_changes
        .iter()
        .map(|a| a.reason.as_str())
        .collect();
    assert!(
        reasons.iter().any(|r| r.contains("first migration")),
        "the earlier acknowledgement must survive: {reasons:?}"
    );
    assert!(
        reasons.iter().any(|r| r.contains("second migration")),
        "the new acknowledgement must be recorded: {reasons:?}"
    );
}

/// Coverage counters are derived, so they must be recomputed on parse rather
/// than trusted from the file — otherwise a hand-edited artifact could claim
/// coverage it does not have.
#[test]
fn coverage_is_recomputed_on_parse_not_trusted_from_the_file() {
    let mut raw: Value = serde_json::from_str(
        &input_contract(json!({"type": "object"}))
            .to_json_pretty()
            .expect("serialize"),
    )
    .expect("json");
    // Claim coverage that is not there.
    raw["coverage"] = json!({
        "workflows_total": 999,
        "with_input_schema": 999,
        "with_output_schema": 999,
        "with_error_schema": 999,
    });
    let parsed = WorkflowSchemaContract::parse(&raw.to_string())
        .expect("a lying coverage block still parses");
    assert_eq!(
        parsed.coverage.workflows_total, 1,
        "recomputed, not trusted"
    );
    assert_eq!(parsed.coverage.with_input_schema, 1);
    assert_eq!(parsed.coverage.with_output_schema, 0);
}

/// A schema whose definitions cross-reference must not fan out exponentially.
///
/// `visited` is a monotonic memo, not a stack-scoped cycle guard: with the
/// latter, each of N definitions referencing the previous one is re-expanded
/// along every path and a few KB of `$ref`s never finishes. This is a
/// termination test — its value is that it *completes*.
#[test]
fn cross_referencing_definitions_do_not_fan_out_exponentially() {
    const N: usize = 40;
    let mut defs = serde_json::Map::new();
    for i in 0..N {
        // Each level references the next one twice: 2^40 paths without memoing.
        let next = if i + 1 < N {
            json!({
                "type": "object",
                "properties": {
                    "l": {"$ref": format!("#/definitions/D{}", i + 1)},
                    "r": {"$ref": format!("#/definitions/D{}", i + 1)},
                },
            })
        } else {
            json!({"type": "string"})
        };
        defs.insert(format!("D{i}"), next);
    }
    let schema = json!({
        "type": "object",
        "properties": {"root": {"$ref": "#/definitions/D0"}},
        "definitions": Value::Object(defs),
    });
    // Identical on both sides: the point is that it terminates promptly.
    let diff = diff_input(schema.clone(), schema);
    assert!(!diff.has_breaking(), "{:#?}", diff.deltas);
}

// ── review round: sibling clobber, opaque `$ref`, malformed bounds, version ───

/// A node carrying BOTH `enum` and `oneOf` must keep its `enum`.
///
/// `collapse_unit_enum_branches` normalises `schemars`' unit-enum shape
/// (`{"oneOf":[{"enum":["A"]},{"enum":["B"]}]}`) down to a flat `enum`. If it
/// ran on a node that ALSO carries its own `enum`, it would overwrite a real
/// constraint — `validate_against_schema` enforces both — and a later narrowing
/// of that `enum` would read as a widening, or vanish.
#[test]
fn collapsing_unit_enum_branches_never_clobbers_a_sibling_enum() {
    let with_both = |vals: Value| {
        json!({
            "type": "string",
            "enum": vals,
            "oneOf": [{"type": "string", "enum": ["x"]}, {"type": "string", "enum": ["y"]}],
        })
    };
    // The sibling `enum` survives canonicalisation verbatim.
    let canon = canonicalize_schema(&with_both(json!(["a", "b"])));
    assert_eq!(
        canon.get("enum"),
        Some(&json!(["a", "b"])),
        "the node's own `enum` must not be replaced by the branch values: {canon:#?}"
    );

    // ...so narrowing it is still caught.
    assert_breaking(
        with_both(json!(["a", "b"])),
        with_both(json!(["a"])),
        ChangeKind::EnumValueRemoved,
    );
}

/// A parent `type` that disagrees with the branches is left alone.
#[test]
fn collapsing_unit_enum_branches_leaves_a_contradictory_parent_type_alone() {
    let schema = json!({
        "type": "integer",
        "oneOf": [{"type": "string", "enum": ["x"]}, {"type": "string", "enum": ["y"]}],
    });
    let canon = canonicalize_schema(&schema);
    assert_eq!(
        canon.get("type"),
        Some(&json!("integer")),
        "a contradictory parent `type` must not be rewritten: {canon:#?}"
    );
    assert!(
        canon.get("oneOf").is_some(),
        "the node should be left intact rather than half-normalised: {canon:#?}"
    );
}

/// Swapping one *unresolvable* `$ref` for another must not read as no change.
///
/// `$ref` sits in `ANALYSED_KEYWORDS`, which excludes it from
/// `diff_unanalysed`'s fail-closed sweep — so an external or dangling reference
/// that the resolver cannot follow would otherwise be compared as an opaque
/// object whose only differing key is deliberately skipped: a silent PASS on a
/// wholesale type replacement.
#[test]
fn swapping_a_dangling_ref_is_breaking_not_invisible() {
    assert_breaking(
        json!({"type": "object", "properties": {"p": {"$ref": "#/definitions/Gone1"}}}),
        json!({"type": "object", "properties": {"p": {"$ref": "#/definitions/Gone2"}}}),
        ChangeKind::UnanalysedConstraintChanged,
    );
}

#[test]
fn swapping_an_external_ref_is_breaking_not_invisible() {
    assert_breaking(
        json!({"$ref": "https://example.com/schemas/A.json"}),
        json!({"$ref": "https://example.com/schemas/B.json"}),
        ChangeKind::UnanalysedConstraintChanged,
    );
}

/// Replacing a concrete schema with an opaque reference is breaking.
#[test]
fn replacing_a_concrete_schema_with_an_unresolvable_ref_is_breaking() {
    assert_breaking(
        json!({"type": "string"}),
        json!({"type": "string", "$ref": "https://example.com/schemas/A.json"}),
        ChangeKind::UnanalysedConstraintChanged,
    );
}

/// The SAME unresolvable `$ref` on both sides is not a change *at this node*.
///
/// Pins the documented limitation: the gate cannot see behind an external
/// reference, but it also does not manufacture a phantom delta for one.
#[test]
fn an_unchanged_external_ref_is_not_reported() {
    assert_compatible(
        json!({"$ref": "https://example.com/schemas/A.json"}),
        json!({"$ref": "https://example.com/schemas/A.json"}),
    );
}

/// A bound that is present but not a number must not read as "removed".
///
/// `as_f64` collapses `"10"` to `None`, which is indistinguishable from absent
/// — so a malformed value would be reported `BoundRelaxed`/COMPATIBLE even
/// though the constraint is still there.
#[test]
fn a_malformed_bound_is_breaking_not_reported_as_removed() {
    let diff = diff_input(
        json!({"type": "integer", "minimum": 10}),
        json!({"type": "integer", "minimum": "10"}),
    );
    assert!(
        diff.has_breaking(),
        "a numeric bound replaced by a non-number must fail closed, got: {:#?}",
        diff.deltas
    );
    assert!(
        !diff
            .deltas
            .iter()
            .any(|d| d.change == ChangeKind::BoundRelaxed),
        "a malformed bound must never be reported as a relaxation: {:#?}",
        diff.deltas
    );
}

/// Two *different* malformed bounds must not produce zero deltas.
#[test]
fn changing_between_two_malformed_bounds_is_breaking() {
    assert_breaking(
        json!({"type": "integer", "minimum": "10"}),
        json!({"type": "integer", "minimum": "20"}),
        ChangeKind::UnanalysedConstraintChanged,
    );
}

/// An unchanged malformed bound is not a change.
#[test]
fn an_unchanged_malformed_bound_is_not_reported() {
    assert_compatible(
        json!({"type": "integer", "minimum": "10"}),
        json!({"type": "integer", "minimum": "10"}),
    );
}

/// A `contract_version` this build does not implement is refused.
///
/// A future v2 may change what a verdict MEANS. Diffing it silently under v1
/// rules would hand back confident answers computed from the wrong ruleset —
/// the one failure mode a compatibility gate must never have.
#[test]
fn an_unsupported_contract_version_is_refused_not_silently_diffed() {
    let mut v: Value = serde_json::from_str(
        &input_contract(json!({"type": "object"}))
            .to_json_pretty()
            .unwrap(),
    )
    .unwrap();
    v["contract_version"] = json!("999");
    let err = WorkflowSchemaContract::parse(&v.to_string())
        .expect_err("an unknown contract_version must be refused");
    let msg = err.to_string();
    assert!(
        msg.contains("contract_version") && msg.contains("999"),
        "the refusal must name the offending version: {msg}"
    );
    assert!(
        msg.contains(SCHEMA_CONTRACT_VERSION),
        "the refusal must name the version this build implements: {msg}"
    );
}

/// The current version still parses (the guard is not a blanket refusal).
#[test]
fn the_current_contract_version_still_parses() {
    let json = input_contract(json!({"type": "object"}))
        .to_json_pretty()
        .unwrap();
    WorkflowSchemaContract::parse(&json).expect("the current version must parse");
}

// ── Codex review: exact bounds, container keyword, anyOf order, truncated ack ─

/// A bound change smaller than `f64::EPSILON` is still a narrowing.
///
/// `f64::EPSILON` (~2.2e-16) is machine epsilon **at 1.0**; used as an absolute
/// tolerance it swallows every smaller change. `minimum: 1e-20` → `2e-20`
/// rejects values the baseline accepted.
#[test]
fn a_sub_epsilon_bound_tightening_is_still_breaking() {
    assert_breaking(
        json!({"type": "number", "minimum": 1e-20}),
        json!({"type": "number", "minimum": 2e-20}),
        ChangeKind::BoundTightened,
    );
}

/// ...and the relaxation in the same range is still reported.
#[test]
fn a_sub_epsilon_bound_relaxation_is_reported_compatible() {
    assert_compatible_delta(
        json!({"type": "number", "minimum": 2e-20}),
        json!({"type": "number", "minimum": 1e-20}),
        ChangeKind::BoundRelaxed,
    );
}

/// Distinct integers above 2^53 must not collapse through `f64`.
///
/// `f64` has a 53-bit mantissa, so `9007199254740993` and `9007199254740992`
/// become the same value — a bound on an `i64` field could move with no delta.
#[test]
fn a_bound_change_above_the_f64_mantissa_is_not_collapsed() {
    assert_breaking(
        json!({"type": "integer", "format": "int64", "minimum": 9_007_199_254_740_992_i64}),
        json!({"type": "integer", "format": "int64", "minimum": 9_007_199_254_740_993_i64}),
        ChangeKind::BoundTightened,
    );
}

/// An identical bound is still not a delta (the exact path did not over-report).
#[test]
fn an_unchanged_large_integer_bound_is_not_reported() {
    assert_compatible(
        json!({"type": "integer", "minimum": 9_007_199_254_740_993_i64}),
        json!({"type": "integer", "minimum": 9_007_199_254_740_993_i64}),
    );
}

/// `anyOf` → `oneOf` narrows even when every branch is untouched.
///
/// `anyOf` accepts a value matching two or more branches; `oneOf` requires
/// exactly one. A recorded integer matches both an `integer` and a `number`
/// branch, so it is accepted under `anyOf` and rejected under `oneOf`.
#[test]
fn changing_anyof_to_oneof_is_breaking_even_with_identical_branches() {
    let branches = json!([{"type": "integer"}, {"type": "number"}]);
    assert_breaking(
        json!({"anyOf": branches}),
        json!({"oneOf": branches}),
        ChangeKind::BoundTightened,
    );
}

/// `oneOf` → `anyOf` is the widening direction.
#[test]
fn changing_oneof_to_anyof_is_reported_compatible() {
    let branches = json!([{"type": "integer"}, {"type": "number"}]);
    assert_compatible_delta(
        json!({"oneOf": branches}),
        json!({"anyOf": branches}),
        ChangeKind::BoundRelaxed,
    );
}

/// A node carrying BOTH keywords is outside what the branch model covers.
///
/// `branch_set` selects `oneOf` first, so the `anyOf` sibling is ignored — and
/// `anyOf` is in the analysed set, so the fail-closed sweep skips it too.
#[test]
fn a_node_with_both_oneof_and_anyof_fails_closed_on_change() {
    assert_breaking(
        json!({
            "oneOf": [{"type": "object", "required": ["A"], "properties": {"A": {"type": "string"}}}],
            "anyOf": [{"type": "string"}],
        }),
        json!({
            "oneOf": [{"type": "object", "required": ["A"], "properties": {"A": {"type": "string"}}}],
            "anyOf": [{"type": "integer"}],
        }),
        ChangeKind::UnanalysedConstraintChanged,
    );
}

/// Reordering `anyOf` branches silently rebinds recorded data.
///
/// For `#[serde(untagged)]`, serde binds the FIRST matching variant in
/// declaration order. Swapping `Int(i64)` and `Float(f64)` means a recorded
/// integer — which matches both — now deserializes as the float variant. It
/// still parses; it no longer means the same thing.
#[test]
fn reordering_untagged_anyof_variants_is_breaking() {
    assert_breaking(
        json!({"anyOf": [
            {"type": "integer", "format": "int64"},
            {"type": "number", "format": "double"},
        ]}),
        json!({"anyOf": [
            {"type": "number", "format": "double"},
            {"type": "integer", "format": "int64"},
        ]}),
        ChangeKind::UnanalysedConstraintChanged,
    );
}

/// `oneOf` requires exactly one match, so order cannot affect binding there.
#[test]
fn reordering_oneof_branches_is_not_a_delta() {
    let a = json!({"type": "object", "required": ["A"], "properties": {"A": {"type": "string"}}});
    let b = json!({"type": "object", "required": ["B"], "properties": {"B": {"type": "string"}}});
    assert_compatible(json!({"oneOf": [a, b]}), json!({"oneOf": [b, a]}));
}

/// Appending an `anyOf` branch is not a reorder — `T` → `Option<T>` appends a
/// `null` branch and must stay compatible.
#[test]
fn appending_an_anyof_branch_is_not_reported_as_a_reorder() {
    let obj = json!({"type": "object", "required": ["A"], "properties": {"A": {"type": "string"}}});
    assert_compatible(
        json!({"anyOf": [obj]}),
        json!({"anyOf": [obj, {"type": "null"}]}),
    );
}

/// A truncated diff cannot be acknowledged.
///
/// The artifact promises an audit record for every absorbed breaking delta, but
/// the stored listing is capped while the rebase takes the whole contract — so
/// the excess would be absorbed with no record of it.
#[test]
fn acknowledging_a_truncated_diff_is_refused() {
    // One property per workflow, renamed — two breaking deltas each — enough
    // workflows to push the tally past the cap.
    let entries = |suffix: &str| {
        (0..(MAX_DELTAS / 2 + 10))
            .map(|i| WorkflowSchemaEntry {
                name: format!("wf{i:05}"),
                description: None,
                input_schema: Some(json!({
                    "type": "object",
                    "properties": {format!("f{suffix}"): {"type": "string"}},
                    "required": [format!("f{suffix}")],
                })),
                output_schema: None,
                error_schema: None,
            })
            .collect::<Vec<_>>()
    };
    let base = WorkflowSchemaContract::from_entries("0.0.0-test", entries("a"));
    let cur = WorkflowSchemaContract::from_entries("0.0.0-test", entries("b"));

    let diff = autumn_harvest::schema_contract::diff_schema_contracts(&base, &cur);
    assert!(diff.truncated, "fixture must exceed the delta cap");
    assert!(
        diff.breaking_count > diff.deltas.len(),
        "the tally must exceed the stored listing: {} vs {}",
        diff.breaking_count,
        diff.deltas.len()
    );

    let err = base
        .acknowledged_update(&cur, "drained", None)
        .expect_err("a truncated diff must not be acknowledgeable");
    let msg = err.to_string();
    assert!(
        msg.contains("truncated"),
        "the refusal must name truncation as the cause: {msg}"
    );
}

// ── Codex review 2: unchanged ambiguous branches, prepended `anyOf` variants ──

/// An UNCHANGED ambiguous branch set must not be reported breaking.
///
/// Two multi-field object variants of a `#[serde(untagged)]` enum both key as
/// `type:object`, so the collision guard fires — on BOTH sides. Reporting that
/// breaking means the workflow's own baseline can never pass the gate again,
/// permanently blocking every unrelated change to the repository.
#[test]
fn an_unchanged_ambiguous_branch_set_is_not_breaking() {
    let ambiguous = json!({"anyOf": [
        {"type": "object", "properties": {"a": {"type": "string"}, "b": {"type": "string"}}},
        {"type": "object", "properties": {"c": {"type": "string"}, "d": {"type": "string"}}},
    ]});
    assert_compatible(ambiguous.clone(), ambiguous);
}

/// The positional fallback still recurses, so a nested change inside an
/// ambiguous branch is caught rather than waved through.
#[test]
fn a_nested_change_inside_an_ambiguous_branch_is_still_detected() {
    assert_breaking(
        json!({"anyOf": [
            {"type": "object", "properties": {"a": {"type": "string"}, "b": {"type": "string"}}},
            {"type": "object", "properties": {"c": {"type": "string"}, "d": {"type": "string"}}},
        ]}),
        json!({"anyOf": [
            {"type": "object", "properties": {"a": {"type": "string"}, "b": {"type": "string"}}},
            // `c` narrowed from string to integer — recorded strings no longer read.
            {"type": "object", "properties": {"c": {"type": "integer"}, "d": {"type": "string"}}},
        ]}),
        ChangeKind::TypeNarrowed,
    );
}

/// A LENGTH change in an ambiguous set is genuinely unclassifiable: the differ
/// cannot say which branch was dropped, so it must still fail closed.
#[test]
fn an_ambiguous_branch_set_that_changes_length_still_fails_closed() {
    assert_breaking(
        json!({"anyOf": [
            {"type": "object", "properties": {"a": {"type": "string"}, "b": {"type": "string"}}},
            {"type": "object", "properties": {"c": {"type": "string"}, "d": {"type": "string"}}},
        ]}),
        json!({"anyOf": [
            {"type": "object", "properties": {"a": {"type": "string"}, "b": {"type": "string"}}},
        ]}),
        ChangeKind::UnanalysedConstraintChanged,
    );
}

/// For `#[serde(untagged)]`, serde binds the FIRST matching branch. Inserting a
/// permissive variant AHEAD of an existing one silently rebinds recorded data
/// to a different branch — e.g. prepending `Float(f64)` before `Int(i64)`
/// captures every recorded integer.
#[test]
fn prepending_an_anyof_variant_ahead_of_an_existing_branch_is_breaking() {
    assert_breaking(
        json!({"anyOf": [
            {"type": "integer", "format": "int64"},
        ]}),
        json!({"anyOf": [
            {"type": "number", "format": "double"},
            {"type": "integer", "format": "int64"},
        ]}),
        ChangeKind::VariantAdded,
    );
}

/// A variant inserted between two existing branches is equally a rebind risk
/// for every branch after it.
#[test]
fn inserting_an_anyof_variant_between_existing_branches_is_breaking() {
    let a = json!({"type": "object", "required": ["A"], "properties": {"A": {"type": "string"}}});
    let b = json!({"type": "object", "required": ["B"], "properties": {"B": {"type": "string"}}});
    assert_breaking(
        json!({"anyOf": [a, b]}),
        json!({"anyOf": [a, {"type": "object"}, b]}),
        ChangeKind::VariantAdded,
    );
}

/// Appending stays compatible — nothing recorded can rebind to a branch that
/// sits after every pre-existing one. This is the `T` -> `Option<T>` shape.
#[test]
fn appending_an_anyof_variant_after_every_existing_branch_stays_compatible() {
    let a = json!({"type": "object", "required": ["A"], "properties": {"A": {"type": "string"}}});
    let b = json!({"type": "object", "required": ["B"], "properties": {"B": {"type": "string"}}});
    assert_compatible_delta(
        json!({"anyOf": [a, b]}),
        json!({"anyOf": [a, b, {"type": "null"}]}),
        ChangeKind::VariantAdded,
    );
}

/// `oneOf` demands exactly one match, so branch ORDER cannot rebind anything —
/// the position rule is `anyOf`-only and must not leak into `oneOf`.
#[test]
fn prepending_a_disjoint_oneof_variant_stays_compatible() {
    let a = json!({"type": "object", "required": ["A"], "properties": {"A": {"type": "string"}}});
    let b = json!({"type": "object", "required": ["B"], "properties": {"B": {"type": "string"}}});
    assert_compatible_delta(
        json!({"oneOf": [a]}),
        json!({"oneOf": [b, a]}),
        ChangeKind::VariantAdded,
    );
}

// ── Codex review 2: the gate must run when only the BASELINE changes ─────────

const CI_YAML: &str = include_str!("../../../.github/workflows/ci.yml");

/// The gate step must exist in CI at all.
#[test]
fn ci_runs_the_schema_gate() {
    assert!(
        CI_YAML.contains("schema check --current"),
        "ci.yml must run `harvest schema check --current` — without it the gate is decorative"
    );
}

/// A PR that changes ONLY the checked-in baseline must still run the gate.
///
/// The baseline lives under `docs/`, and the matrix is skipped for docs-only
/// changes — so without an explicit carve-out a hand-edited, stale, or malformed
/// baseline merges with no comparison against the generated contract, which is
/// precisely the drift the gate exists to catch.
#[test]
fn the_schema_baseline_is_not_classified_as_a_docs_only_change() {
    let filter = CI_YAML
        .split("Docs-only =")
        .nth(1)
        .expect("ci.yml must document its docs-only classification rule");
    // Bound the window to the classification block so a mention elsewhere in the
    // file cannot satisfy this assertion.
    let block = &filter[..filter.len().min(1200)];
    assert!(
        block.contains("docs/workflow-schema-contract.json"),
        "the docs-only filter must carve out `docs/workflow-schema-contract.json` so a \
         baseline-only PR still runs the schema gate; block was:\n{block}"
    );
}

// ── Codex review 3: a tagged branch is not disjoint from a BROADER sibling ────

/// A tag on the ADDED branch says nothing about the branches already there.
///
/// `oneOf` requires EXACTLY one match. Adding the singleton `{"enum":["x"]}`
/// alongside a broad `{"type":"string"}` makes the recorded value `"x"` match
/// two branches, so it is rejected — even though the new branch is "tagged".
#[test]
fn adding_a_tagged_branch_beside_a_broader_branch_is_breaking() {
    assert_breaking(
        json!({"oneOf": [{"type": "string"}]}),
        json!({"oneOf": [{"type": "string"}, {"type": "string", "enum": ["x"]}]}),
        ChangeKind::VariantAdded,
    );
    // …and the oracle agrees: `"x"` reads under the baseline and not after.
    assert_oracle_agrees_breaking(
        &json!({"oneOf": [{"type": "string"}]}),
        &json!({"oneOf": [{"type": "string"}, {"type": "string", "enum": ["x"]}]}),
        &json!("x"),
    );
}

/// The all-tagged case stays compatible: every branch of a serde
/// externally-tagged enum carries a distinct variant key, and serde's
/// serializer emits exactly one — so a recorded payload matches exactly one
/// branch no matter how many variants are added.
#[test]
fn adding_a_variant_to_an_all_tagged_one_of_stays_compatible() {
    let a = json!({"type": "object", "required": ["A"], "properties": {"A": {"type": "string"}}});
    let b = json!({"type": "object", "required": ["B"], "properties": {"B": {"type": "string"}}});
    let c = json!({"type": "object", "required": ["C"], "properties": {"C": {"type": "string"}}});
    assert_compatible_delta(
        json!({"oneOf": [a, b]}),
        json!({"oneOf": [a, b, c]}),
        ChangeKind::VariantAdded,
    );
}

/// A unit variant mixed with tagged variants is the shape `schemars` emits for
/// `enum Foo { A, B(i32) }` — still all-discriminated, still compatible.
#[test]
fn adding_a_variant_to_a_mixed_unit_and_tagged_one_of_stays_compatible() {
    let unit = json!({"type": "string", "enum": ["A"]});
    let tagged =
        json!({"type": "object", "required": ["B"], "properties": {"B": {"type": "integer"}}});
    let added =
        json!({"type": "object", "required": ["C"], "properties": {"C": {"type": "string"}}});
    assert_compatible_delta(
        json!({"oneOf": [unit, tagged]}),
        json!({"oneOf": [unit, tagged, added]}),
        ChangeKind::VariantAdded,
    );
}

// ── Codex review 3: the escape hatch must not be bypassable by hand-editing ──

/// Fixtures for the bypass: a workflow whose input field is renamed, which is a
/// breaking change from the replay-read perspective.
fn bypass_fixture(field: &str) -> WorkflowSchemaContract {
    WorkflowSchemaContract::from_entries(
        "0.0.0-test",
        vec![WorkflowSchemaEntry {
            name: "onboarding".to_string(),
            description: None,
            input_schema: Some(json!({
                "type": "object",
                "properties": {field: {"type": "string"}},
                "required": [field],
            })),
            output_schema: None,
            error_schema: None,
        }],
    )
}

/// A hand-edited baseline that silently absorbed a breaking change must be
/// caught by comparing against the PREVIOUS revision of the artifact.
///
/// Regenerating the artifact in the same PR makes the ordinary check trivially
/// clean — baseline and generated contract agree — so the *only* signal left is
/// the artifact's own change since the base revision. Without an
/// acknowledgement covering it, the escape hatch has been bypassed.
#[test]
fn a_hand_edited_baseline_absorbing_a_break_is_unacknowledged() {
    let base_artifact = bypass_fixture("email");
    // The contributor renamed the field AND overwrote the artifact by hand, so
    // the head artifact equals the generated contract and no ack was recorded.
    let head_generated = bypass_fixture("email_address");
    let head_artifact = bypass_fixture("email_address");

    // The ordinary check is clean — this is exactly why it cannot catch it.
    assert!(
        !autumn_harvest::schema_contract::diff_schema_contracts(&head_artifact, &head_generated)
            .has_breaking(),
        "the in-PR check is clean by construction; the bypass is invisible to it"
    );

    let diff =
        autumn_harvest::schema_contract::diff_schema_contracts(&base_artifact, &head_generated);
    assert!(diff.has_breaking(), "the change since base IS breaking");
    let missing = unacknowledged_breaking(&diff, &base_artifact, &head_artifact);
    assert!(
        !missing.is_empty(),
        "a breaking change with no acknowledgement must be reported unacknowledged"
    );
}

/// The legitimate path — `schema update --acknowledge` — must pass.
#[test]
fn an_acknowledged_baseline_update_covers_its_breaking_deltas() {
    let base_artifact = bypass_fixture("email");
    let head_generated = bypass_fixture("email_address");
    let head_artifact = base_artifact
        .acknowledged_update(&head_generated, "renamed for GDPR", Some("#794"))
        .expect("acknowledging must succeed");

    let diff =
        autumn_harvest::schema_contract::diff_schema_contracts(&base_artifact, &head_generated);
    assert!(diff.has_breaking(), "fixture must be breaking");
    let missing = unacknowledged_breaking(&diff, &base_artifact, &head_artifact);
    assert!(
        missing.is_empty(),
        "an acknowledged update must leave nothing unacknowledged: {missing:#?}"
    );
}

/// An acknowledgement already present at the base revision was recorded for an
/// EARLIER change. Reusing it would let the same field break twice with one
/// record, so only acknowledgements NEW in this revision count.
#[test]
fn a_pre_existing_acknowledgement_does_not_cover_a_fresh_break() {
    let plain_base = bypass_fixture("email");
    let head_generated = bypass_fixture("email_address");
    let diff = autumn_harvest::schema_contract::diff_schema_contracts(&plain_base, &head_generated);
    assert!(diff.has_breaking(), "fixture must be breaking");

    // Records covering EVERY breaking delta — so the only thing that can make
    // this unacknowledged is the rule that base-carried records do not count.
    let stale: Vec<AcknowledgedBreakingChange> = diff
        .deltas
        .iter()
        .filter(|d| d.verdict == Verdict::Breaking)
        .map(|d| AcknowledgedBreakingChange {
            workflow: d.workflow.clone(),
            role: d.role,
            field_path: d.field_path.clone(),
            change: d.change,
            reason: "an EARLIER, unrelated break".to_string(),
            recorded_in: Some("#111".to_string()),
        })
        .collect();
    assert!(!stale.is_empty(), "fixture must produce records to carry");

    // Base already carries them; head carries exactly the same list and adds
    // nothing, so this revision recorded no justification of its own.
    let mut base_artifact = plain_base;
    base_artifact.acknowledged_breaking_changes = stale.clone();
    let mut head_artifact = bypass_fixture("email_address");
    head_artifact.acknowledged_breaking_changes = stale;

    let missing = unacknowledged_breaking(&diff, &base_artifact, &head_artifact);
    assert_eq!(
        missing.len(),
        diff.breaking_count,
        "records carried over from the base revision must cover nothing: {missing:#?}"
    );
}

/// Nothing breaking since base means nothing to acknowledge.
#[test]
fn a_compatible_baseline_update_needs_no_acknowledgement() {
    let base_artifact = bypass_fixture("email");
    // Widening the field type is compatible.
    let head_generated = WorkflowSchemaContract::from_entries(
        "0.0.0-test",
        vec![WorkflowSchemaEntry {
            name: "onboarding".to_string(),
            description: None,
            input_schema: Some(json!({
                "type": "object",
                "properties": {"email": {}},
                "required": ["email"],
            })),
            output_schema: None,
            error_schema: None,
        }],
    );
    let diff =
        autumn_harvest::schema_contract::diff_schema_contracts(&base_artifact, &head_generated);
    assert!(!diff.has_breaking(), "fixture must be compatible");
    assert!(
        unacknowledged_breaking(&diff, &base_artifact, &head_generated).is_empty(),
        "a compatible change needs no acknowledgement"
    );
}

// ── Codex review 4: degenerate duplicate branches, audit-log integrity ───────

/// Canonicalisation must not dedupe DUPLICATE `oneOf` singleton branches.
///
/// `oneOf` requires exactly one match, so a second identical `{"enum":["x"]}`
/// branch makes the recorded value `"x"` match twice and be rejected. Merging
/// the branches into one flat `enum` and deduping the values erases exactly
/// that difference, so both revisions canonicalise identically.
#[test]
fn duplicating_a_one_of_singleton_branch_is_breaking() {
    assert_breaking(
        json!({"oneOf": [{"type": "string", "enum": ["x"]}]}),
        json!({"oneOf": [
            {"type": "string", "enum": ["x"]},
            {"type": "string", "enum": ["x"]},
        ]}),
        ChangeKind::UnanalysedConstraintChanged,
    );
}

/// `anyOf` is "at least one", so duplicate branches are genuinely harmless and
/// must still collapse — otherwise every unit-only enum churns.
#[test]
fn duplicating_an_any_of_singleton_branch_stays_compatible() {
    assert_compatible(
        json!({"anyOf": [{"type": "string", "enum": ["x"]}]}),
        json!({"anyOf": [
            {"type": "string", "enum": ["x"]},
            {"type": "string", "enum": ["x"]},
        ]}),
    );
}

/// The ordinary unit-enum collapse — distinct values — is untouched, in both
/// containers. This is the shape `schemars` emits for a unit-only Rust enum, so
/// a regression here would churn every such workflow.
#[test]
fn distinct_one_of_singleton_branches_still_collapse() {
    let flat = json!({"type": "string", "enum": ["A", "B"]});
    for kw in ["oneOf", "anyOf"] {
        let branched = json!({
            kw: [{"type": "string", "enum": ["A"]}, {"type": "string", "enum": ["B"]}]
        });
        assert_eq!(
            canonicalize_schema(&branched),
            canonicalize_schema(&flat),
            "distinct singleton `{kw}` branches must still normalise to the flat form"
        );
    }
}

/// The acknowledgement log is promised **append-only**. Retargeting an existing
/// record at a fresh break — editing its workflow/path/change while keeping a
/// plausible reason — would cover the new delta while erasing the old record,
/// so the multiset subtraction alone cannot see it.
#[test]
fn a_rewritten_acknowledgement_record_is_detected() {
    let plain_base = bypass_fixture("email");
    let head_generated = bypass_fixture("email_address");
    let diff = autumn_harvest::schema_contract::diff_schema_contracts(&plain_base, &head_generated);

    // Base carries a record for an unrelated, earlier break.
    let stale = AcknowledgedBreakingChange {
        workflow: "some_other_workflow".to_string(),
        role: Some(SchemaRole::Output),
        field_path: "/legacy".to_string(),
        change: ChangeKind::PropertyRemoved,
        reason: "an EARLIER, unrelated break".to_string(),
        recorded_in: Some("#111".to_string()),
    };
    let mut base_artifact = plain_base;
    base_artifact.acknowledged_breaking_changes = vec![stale];

    // Head RETARGETS that one record at this PR's break instead of appending.
    let mut head_artifact = bypass_fixture("email_address");
    head_artifact.acknowledged_breaking_changes = diff
        .deltas
        .iter()
        .filter(|d| d.verdict == Verdict::Breaking)
        .map(|d| AcknowledgedBreakingChange {
            workflow: d.workflow.clone(),
            role: d.role,
            field_path: d.field_path.clone(),
            change: d.change,
            reason: "retargeted".to_string(),
            recorded_in: Some("#111".to_string()),
        })
        .collect();

    let dropped = dropped_acknowledgements(&base_artifact, &head_artifact);
    assert!(
        !dropped.is_empty(),
        "a record present at the base revision but absent now must be reported"
    );
    assert_eq!(dropped[0].workflow, "some_other_workflow");
}

/// A legitimate acknowledged update APPENDS, so nothing is ever dropped.
#[test]
fn a_legitimate_acknowledged_update_drops_nothing() {
    let mut base_artifact = bypass_fixture("email");
    base_artifact
        .acknowledged_breaking_changes
        .push(AcknowledgedBreakingChange {
            workflow: "some_other_workflow".to_string(),
            role: Some(SchemaRole::Output),
            field_path: "/legacy".to_string(),
            change: ChangeKind::PropertyRemoved,
            reason: "an EARLIER, unrelated break".to_string(),
            recorded_in: Some("#111".to_string()),
        });
    let head_artifact = base_artifact
        .acknowledged_update(
            &bypass_fixture("email_address"),
            "renamed for GDPR",
            Some("#794"),
        )
        .expect("acknowledging must succeed");

    assert!(
        dropped_acknowledgements(&base_artifact, &head_artifact).is_empty(),
        "an append-only update must drop nothing"
    );
}

// ── Codex review 5: optional tags, untagged rebinding ────────────────────────

/// A tag proves disjointness only when the tag property is REQUIRED.
///
/// With an optional `tag` pinned to `"A"`, the empty object `{}` already
/// validates. Adding a branch whose optional `tag` is pinned to `"B"` makes
/// `{}` match twice, and `oneOf` requires exactly one — so a recorded `{}` is
/// rejected. Probed against `schemars` 0.8.22: a real `#[serde(tag = "type")]`
/// enum emits the discriminant in `required`, so this narrowing cannot make a
/// genuine internally-tagged enum churn.
#[test]
fn adding_a_one_of_branch_with_an_optional_tag_is_breaking() {
    assert_breaking(
        json!({"oneOf": [
            {"type": "object", "properties": {"tag": {"enum": ["A"]}}},
        ]}),
        json!({"oneOf": [
            {"type": "object", "properties": {"tag": {"enum": ["A"]}}},
            {"type": "object", "properties": {"tag": {"enum": ["B"]}}},
        ]}),
        ChangeKind::VariantAdded,
    );
}

/// The shape `schemars` actually emits for `#[serde(tag = "type")]` — the tag
/// in `required` — must stay compatible, or every internally-tagged enum churns.
#[test]
fn adding_a_one_of_branch_with_a_required_tag_is_compatible() {
    assert_compatible(
        json!({"oneOf": [
            {"type": "object", "required": ["type"],
             "properties": {"type": {"enum": ["A"]}}},
        ]}),
        json!({"oneOf": [
            {"type": "object", "required": ["type"],
             "properties": {"type": {"enum": ["A"]}}},
            {"type": "object", "required": ["type"],
             "properties": {"type": {"enum": ["B"]}}},
        ]}),
    );
}

/// Widening a non-final `anyOf` branch can rebind recorded data.
///
/// `anyOf` is `#[serde(untagged)]` and serde binds the FIRST matching variant.
/// Both branches here are objects, so neither is provably disjoint from the
/// other; broadening branch 0's `v` to also accept a string lets a recorded
/// `{"v": "x"}` — which bound to variant B — match branch 0 and bind to
/// variant A instead. It still deserializes; it no longer means the same thing.
///
/// The widening is deliberately INSIDE a property so the branch's identity key
/// is unchanged: this must fail because of the rebind rule, not because the
/// branch looked removed-and-re-added.
#[test]
fn widening_a_non_final_untagged_branch_is_breaking() {
    let base = json!({"anyOf": [
        {"type": "object", "required": ["v"],
         "properties": {"v": {"type": "integer"}, "note": {"type": "string"}}},
        {"type": "object", "required": ["w"],
         "properties": {"w": {"type": "string"}, "note": {"type": "string"}}},
    ]});
    let cur = json!({"anyOf": [
        {"type": "object", "required": ["v"],
         "properties": {"v": {"type": ["integer", "string"]}, "note": {"type": "string"}}},
        {"type": "object", "required": ["w"],
         "properties": {"w": {"type": "string"}, "note": {"type": "string"}}},
    ]});
    let diff = diff_input(base, cur);
    assert!(
        diff.deltas.iter().any(|d| d.verdict == Verdict::Breaking
            && d.reason
                .contains("not provably disjoint from a branch declared after it")),
        "widening a non-final untagged branch can rebind recorded data: {:#?}",
        diff.deltas
    );
}

/// The LAST `anyOf` branch has nothing after it to steal from, so widening it
/// cannot rebind anything. Same shape as the test above, widening the other
/// branch — so the two together pin that the guard is position-aware rather
/// than flagging every untagged widening.
#[test]
fn widening_the_final_untagged_branch_is_compatible() {
    assert_compatible(
        json!({"anyOf": [
            {"type": "object", "required": ["v"],
             "properties": {"v": {"type": "integer"}, "note": {"type": "string"}}},
            {"type": "object", "required": ["w"],
             "properties": {"w": {"type": "string"}, "note": {"type": "string"}}},
        ]}),
        json!({"anyOf": [
            {"type": "object", "required": ["v"],
             "properties": {"v": {"type": "integer"}, "note": {"type": "string"}}},
            {"type": "object", "required": ["w"],
             "properties": {"w": {"type": ["string", "integer"]}, "note": {"type": "string"}}},
        ]}),
    );
}

/// The single most common widening must NOT regress: `Option<Struct>` is
/// `anyOf: [{$ref: Struct}, {"type":"null"}]`, so adding an optional field to
/// `Struct` widens the non-final branch 0 — but `object` and `null` are
/// provably disjoint, so nothing can rebind. Verified against `schemars`
/// 0.8.22 output.
#[test]
fn widening_an_option_struct_branch_stays_compatible() {
    let base = json!({
        "definitions": {"Inner": {"type": "object", "required": ["a"],
                                   "properties": {"a": {"type": "integer"}}}},
        "anyOf": [{"$ref": "#/definitions/Inner"}, {"type": "null"}]
    });
    let cur = json!({
        "definitions": {"Inner": {"type": "object", "required": ["a"],
                                   "properties": {"a": {"type": "integer"},
                                                  "b": {"type": ["string", "null"]}}}},
        "anyOf": [{"$ref": "#/definitions/Inner"}, {"type": "null"}]
    });
    let diff = diff_input(base, cur);
    assert!(
        !diff.has_breaking(),
        "adding an optional field behind `Option<T>` must stay compatible: {:#?}",
        diff.deltas
    );
}

/// Adding an OPTIONAL property does not change which instances a branch
/// matches (the schema already accepted the key), so it must not be reported
/// as a rebind even on a non-final, non-disjoint branch.
#[test]
fn adding_an_optional_property_to_a_non_final_untagged_branch_is_compatible() {
    assert_compatible(
        json!({"anyOf": [
            {"type": "object", "required": ["a"],
             "properties": {"a": {"type": "integer"}, "tail": {"type": "string"}}},
            {"type": "object", "required": ["b"],
             "properties": {"b": {"type": "integer"}, "tail": {"type": "string"}}},
        ]}),
        json!({"anyOf": [
            {"type": "object", "required": ["a"],
             "properties": {"a": {"type": "integer"}, "tail": {"type": "string"},
                            "note": {"type": ["string", "null"]}}},
            {"type": "object", "required": ["b"],
             "properties": {"b": {"type": "integer"}, "tail": {"type": "string"}}},
        ]}),
    );
}

/// `oneOf` requires exactly one match, so declaration order cannot affect
/// binding — the rebind guard is `anyOf`-only and must not fire here.
#[test]
fn widening_a_non_final_one_of_branch_is_not_a_rebind() {
    let base = json!({"oneOf": [
        {"type": "object", "required": ["a"], "properties": {"a": {"type": "integer"}}},
        {"type": "string"},
    ]});
    let cur = json!({"oneOf": [
        {"type": "object", "properties": {"a": {"type": "integer"}}},
        {"type": "string"},
    ]});
    let diff = diff_input(base, cur);
    assert!(
        !diff.deltas.iter().any(|d| d
            .reason
            .contains("not provably disjoint from a branch declared after it")),
        "the rebind guard is anyOf-only: {:#?}",
        diff.deltas
    );
}

/// The disjointness escape is what keeps the rebind guard from flagging the
/// most common untagged shape there is.
///
/// `Option<Struct>` is `anyOf: [{$ref: Struct}, {"type":"null"}]`, so ANY
/// change inside `Struct` alters the non-final branch 0. Widening a field's
/// type genuinely changes what branch 0 matches — but `object` and `null` can
/// share no instance, so nothing can rebind and the change must stay
/// compatible. Without the escape this reports breaking, and every widening
/// behind an `Option<T>` would need an acknowledgement.
#[test]
fn widening_a_field_behind_option_struct_stays_compatible() {
    let with_type = |t: Value| {
        json!({
            "definitions": {"Inner": {"type": "object", "required": ["a"],
                                       "properties": {"a": t}}},
            "anyOf": [{"$ref": "#/definitions/Inner"}, {"type": "null"}]
        })
    };
    let diff = diff_input(
        with_type(json!({"type": "integer"})),
        with_type(json!({"type": ["integer", "string"]})),
    );
    assert!(
        !diff.has_breaking(),
        "`object` and `null` are disjoint, so branch 0 can steal nothing: {:#?}",
        diff.deltas
    );
}
