//! Guards for the OpenTelemetry workflow conventions proposal (issue #2015).
//!
//! `docs/otel-workflow-semconv/` holds a draft proposal and its Weaver model.
//! The draft generalizes ADR 0001 for upstream. These guards keep it true to
//! the ADR, to the `ATTR_*` constants and to the upstream model shape:
//!
//! * Each ADR 0001 span maps to a proposed span of the same kind.
//! * Each `ATTR_*` constant maps to a proposed attribute or has a reason.
//! * The model holds no Harvest name, and the page tables match it.
//! * The replay rule survives, and the submission status is recorded.
//! * Each prose sentence and each YAML brief has 25 words or fewer.
//!
//! Pure: no database, no async, no feature gate. Runs on every OS.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde_yaml::Value;

use super::docs_guard_support::{check_stanza, issue_refs, prose_sentences, section, step_stanza};

const PAGE: &str = "docs/otel-workflow-semconv/README.md";
const REGISTRY: &str = "docs/otel-workflow-semconv/model/workflow/registry.yaml";
const SPANS: &str = "docs/otel-workflow-semconv/model/workflow/spans.yaml";
const ADR: &str = "docs/adr/0001-otel-trace-contract.md";
const TELEMETRY_RS: &str = "autumn-harvest/src/telemetry.rs";
const TELEMETRY_PAGE: &str = "docs/telemetry.md";
const FILTER: &str = "--test integration otel_workflow_semconv_docs::";
const MAX_SENTENCE_WORDS: usize = 25;

/// Attributes that a span may reference from the general upstream registry.
const GENERAL_ATTRIBUTES: &[&str] = &["error.type"];

/// The span kinds that upstream accepts.
const SPAN_KINDS: &[&str] = &["internal", "producer", "consumer", "client", "server"];

/// The page sections that the guards read.
const MAPPING_SECTION: &str = "## 7. Mapping from Harvest";
const ATTRIBUTES_SECTION: &str = "## 4. Attributes";
const SPANS_SECTION: &str = "## 5. Spans";
const REPLAY_SECTION: &str = "## 6. Replay";
const ISSUE_TEXT_SECTION: &str = "## 9. Upstream issue text";
const STATUS_PREFIX: &str = "**Submission status:**";

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent")
        .to_path_buf()
}

fn read(relative: &str) -> String {
    let path = repo_root().join(relative);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("cannot read {}: {err}", path.display()))
        .replace("\r\n", "\n")
}

fn page_section<'a>(page: &'a str, heading: &str) -> &'a str {
    section(page, heading, "\n## ").unwrap_or_else(|| panic!("{PAGE} must have `{heading}`"))
}

fn yaml(relative: &str) -> Value {
    serde_yaml::from_str(&read(relative))
        .unwrap_or_else(|err| panic!("{relative} is not valid YAML: {err}"))
}

/// The `groups` list of a Weaver model file.
fn groups(model: &Value, file: &str) -> Vec<Value> {
    model
        .get("groups")
        .and_then(Value::as_sequence)
        .unwrap_or_else(|| panic!("{file} must have a `groups` list"))
        .clone()
}

fn str_field<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
}

/// Every attribute of the registry, by id.
fn registry_attributes() -> BTreeMap<String, Value> {
    groups(&yaml(REGISTRY), REGISTRY)
        .iter()
        .flat_map(|group| {
            group
                .get("attributes")
                .and_then(Value::as_sequence)
                .cloned()
                .unwrap_or_default()
        })
        .map(|attribute| {
            let id = str_field(&attribute, "id")
                .unwrap_or_else(|| panic!("each attribute in {REGISTRY} needs an `id`"))
                .to_owned();
            (id, attribute)
        })
        .collect()
}

/// Every span group, by id.
fn span_groups() -> BTreeMap<String, Value> {
    groups(&yaml(SPANS), SPANS)
        .into_iter()
        .map(|group| {
            let id = str_field(&group, "id")
                .unwrap_or_else(|| panic!("each group in {SPANS} needs an `id`"))
                .to_owned();
            (id, group)
        })
        .collect()
}

/// The `ref` ids of a span group, each with its requirement level.
fn span_refs(group: &Value) -> BTreeMap<String, Value> {
    group
        .get("attributes")
        .and_then(Value::as_sequence)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|attribute| {
            let id = str_field(&attribute, "ref")?.to_owned();
            Some((id, attribute.get("requirement_level")?.clone()))
        })
        .collect()
}

/// The trimmed cells of a Markdown table row, or `None`.
fn cells(line: &str) -> Option<Vec<&str>> {
    let row = line.trim().strip_prefix('|')?.strip_suffix('|')?;
    Some(row.split('|').map(str::trim).collect())
}

/// The text of a cell that is one inline code span, or `None`.
fn code(cell: &str) -> Option<&str> {
    cell.strip_prefix('`')?.strip_suffix('`')
}

/// The first cell of each table row in `text` that is a code span with `prefix`.
fn first_cells(text: &str, prefix: &str) -> BTreeSet<String> {
    text.lines()
        .filter_map(cells)
        .filter_map(|row| code(row.first()?).map(str::to_owned))
        .filter(|name| name.starts_with(prefix))
        .collect()
}

/// The mapping table: each Harvest name with its target cell and its note.
fn mapping(page: &str) -> BTreeMap<String, (String, String)> {
    page_section(page, MAPPING_SECTION)
        .lines()
        .filter_map(cells)
        .filter_map(|row| {
            let name = code(row.first()?)?.strip_prefix("harvest.")?;
            let target = (*row.get(1)?).to_owned();
            let note = row
                .get(2)
                .map_or_else(String::new, |note| (*note).to_owned());
            Some((format!("harvest.{name}"), (target, note)))
        })
        .collect()
}

/// Each ADR 0001 §2 span name with its kind, in lower case.
fn adr_spans(adr: &str) -> BTreeMap<String, String> {
    let catalogue = section(adr, "### 2. Span catalogue", "\n### ")
        .unwrap_or_else(|| panic!("{ADR} must have `### 2. Span catalogue`"));
    catalogue
        .split("\n#### 2.")
        .skip(1)
        .map(|entry| {
            let name = entry
                .split('`')
                .nth(1)
                .unwrap_or_else(|| panic!("no span name in ADR entry: {entry}"));
            let kind = entry
                .lines()
                .filter_map(cells)
                .find(|row| row.first() == Some(&"Kind"))
                .and_then(|row| code(row.get(1)?))
                .unwrap_or_else(|| panic!("ADR span {name} has no `Kind` row"));
            (name.to_owned(), kind.to_ascii_lowercase())
        })
        .collect()
}

/// The value of each `pub const ATTR_*: &str` in `source`.
fn attr_constants(source: &str) -> BTreeSet<String> {
    source
        .lines()
        .filter(|line| line.starts_with("pub const ATTR_"))
        .filter_map(|line| line.split('"').nth(1).map(str::to_owned))
        .collect()
}

/// Upstream attribute and group ids: lower-case dot-separated snake case.
fn is_well_named(id: &str) -> bool {
    id.split('.').count() >= 2
        && id.split('.').all(|part| {
            part.starts_with(|c: char| c.is_ascii_lowercase())
                && part
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        })
}

#[test]
fn proposal_cites_the_issue_and_its_sources() {
    let page = read(PAGE);
    let refs = issue_refs(&page);
    for issue in [2015, 1838] {
        assert!(refs.contains(&issue), "{PAGE} must cite #{issue}");
    }
    assert!(
        page.contains("../adr/0001-otel-trace-contract.md"),
        "{PAGE} must link ADR 0001"
    );
}

#[test]
fn every_adr_span_maps_to_a_proposed_span_of_the_same_kind() {
    let adr = adr_spans(&read(ADR));
    assert_eq!(adr.len(), 8, "{ADR} §2 must list eight spans: {adr:?}");
    let mapping = mapping(&read(PAGE));
    let spans = span_groups();
    for (name, kind) in &adr {
        let (target, _) = mapping
            .get(name)
            .unwrap_or_else(|| panic!("{PAGE} section 7 must map the ADR span `{name}`"));
        let target = code(target)
            .unwrap_or_else(|| panic!("`{name}` must map to a span id, not `{target}`"));
        let group = spans
            .get(target)
            .unwrap_or_else(|| panic!("`{name}` maps to `{target}`, which {SPANS} lacks"));
        assert_eq!(
            str_field(group, "span_kind"),
            Some(kind.as_str()),
            "`{target}` must keep the ADR kind of `{name}`"
        );
    }
}

#[test]
fn every_attr_constant_is_mapped_or_excluded_with_a_reason() {
    let constants = attr_constants(&read(TELEMETRY_RS));
    assert!(
        constants.len() >= 10,
        "ATTR_* extraction rotted: {constants:?}"
    );
    let mapping = mapping(&read(PAGE));
    let registry = registry_attributes();
    for constant in &constants {
        let (target, note) = mapping
            .get(constant)
            .unwrap_or_else(|| panic!("{PAGE} section 7 must map `{constant}`"));
        if let Some(id) = code(target) {
            assert!(
                registry.contains_key(id),
                "`{constant}` maps to `{id}`, which {REGISTRY} lacks"
            );
        } else {
            assert!(
                target.starts_with("Not proposed") && !note.is_empty(),
                "`{constant}` needs a target attribute, or `Not proposed` with a reason"
            );
        }
    }
}

#[test]
fn the_model_is_generalized_and_well_named() {
    for file in [REGISTRY, SPANS] {
        let text = read(file).to_ascii_lowercase();
        assert!(
            !text.contains("harvest"),
            "{file} must not name Harvest. Keep engine names in the page mapping."
        );
    }
    for id in registry_attributes().keys() {
        assert!(
            id.starts_with("workflow."),
            "`{id}` must be in `workflow.*`"
        );
        assert!(
            is_well_named(id),
            "`{id}` must be lower-case dot-separated snake case"
        );
    }
    for id in span_groups().keys() {
        assert!(
            id.starts_with("span.workflow."),
            "`{id}` must start `span.workflow.`"
        );
        assert!(
            is_well_named(id),
            "`{id}` must be lower-case dot-separated snake case"
        );
    }
}

#[test]
fn the_model_has_the_weaver_shape() {
    for group in groups(&yaml(REGISTRY), REGISTRY) {
        assert_eq!(str_field(&group, "type"), Some("attribute_group"));
        let id = str_field(&group, "id").unwrap_or_default();
        assert!(
            id.starts_with("registry.workflow"),
            "bad registry group `{id}`"
        );
        assert!(str_field(&group, "brief").is_some(), "`{id}` needs a brief");
    }
    for (id, attribute) in registry_attributes() {
        assert_eq!(
            str_field(&attribute, "stability"),
            Some("development"),
            "`{id}` must be `development`"
        );
        assert!(
            str_field(&attribute, "brief").is_some(),
            "`{id}` needs a brief"
        );
        let kind = attribute
            .get("type")
            .unwrap_or_else(|| panic!("`{id}` needs a type"));
        match kind.as_str() {
            Some("boolean") => {}
            Some("string" | "int") => assert!(
                attribute.get("examples").is_some(),
                "`{id}` needs `examples`"
            ),
            Some(other) => panic!("`{id}` has an unknown type `{other}`"),
            None => assert!(
                kind.get("members").and_then(Value::as_sequence).is_some(),
                "`{id}` must be a scalar type or an enum with `members`"
            ),
        }
    }

    let registry = registry_attributes();
    for (id, group) in span_groups() {
        assert_eq!(
            str_field(&group, "type"),
            Some("span"),
            "`{id}` must be a span"
        );
        assert_eq!(
            str_field(&group, "stability"),
            Some("development"),
            "`{id}` must be `development`"
        );
        assert!(str_field(&group, "brief").is_some(), "`{id}` needs a brief");
        let kind = str_field(&group, "span_kind").unwrap_or_default();
        assert!(
            SPAN_KINDS.contains(&kind),
            "`{id}` has a bad span_kind `{kind}`"
        );
        assert!(
            id.ends_with(&format!(".{kind}")),
            "`{id}` must end with `.{kind}`"
        );
        let refs = span_refs(&group);
        assert!(!refs.is_empty(), "`{id}` must reference attributes");
        for (attribute, level) in refs {
            assert!(
                registry.contains_key(&attribute)
                    || GENERAL_ATTRIBUTES.contains(&attribute.as_str()),
                "`{id}` refers to `{attribute}`, which no registry defines"
            );
            let valid = match level.as_str() {
                Some(level) => matches!(level, "required" | "recommended" | "opt_in"),
                None => {
                    level.get("conditionally_required").is_some()
                        || level.get("recommended").is_some()
                }
            };
            assert!(valid, "`{id}` gives `{attribute}` a bad requirement_level");
        }
    }
}

#[test]
fn the_page_tables_match_the_model() {
    let page = read(PAGE);
    let attributes = first_cells(page_section(&page, ATTRIBUTES_SECTION), "workflow.");
    let registry: BTreeSet<String> = registry_attributes().into_keys().collect();
    assert_eq!(
        attributes, registry,
        "{PAGE} section 4 must list each registry attribute"
    );
    let spans = first_cells(page_section(&page, SPANS_SECTION), "span.workflow.");
    let model: BTreeSet<String> = span_groups().into_keys().collect();
    assert_eq!(spans, model, "{PAGE} section 5 must list each span group");
}

#[test]
fn the_operation_enum_names_one_span_each() {
    let registry = registry_attributes();
    let operation = registry
        .get("workflow.operation.name")
        .expect("the registry must define `workflow.operation.name`");
    let members: BTreeSet<String> = operation
        .get("type")
        .and_then(|kind| kind.get("members"))
        .and_then(Value::as_sequence)
        .expect("`workflow.operation.name` must be an enum")
        .iter()
        .filter_map(|member| str_field(member, "value").map(str::to_owned))
        .collect();
    let mut operations = BTreeSet::new();
    for (id, group) in span_groups() {
        let operation = id
            .strip_prefix("span.workflow.")
            .and_then(|rest| rest.split('.').next())
            .unwrap_or_default()
            .to_owned();
        assert_eq!(
            span_refs(&group)
                .get("workflow.operation.name")
                .and_then(Value::as_str),
            Some("required"),
            "`{id}` must require `workflow.operation.name`"
        );
        assert!(
            operations.insert(operation),
            "two span groups share an operation: `{id}`"
        );
    }
    assert_eq!(
        members, operations,
        "each enum member must name exactly one span"
    );
}

#[test]
fn the_replay_rule_is_normative() {
    let registry = registry_attributes();
    let replay = registry
        .get("workflow.replay")
        .expect("the registry must define `workflow.replay`");
    assert_eq!(str_field(replay, "type"), Some("boolean"));
    let spans = span_groups();
    let execute = spans
        .get("span.workflow.execute_workflow.internal")
        .expect("the model must define the execute_workflow span");
    assert_eq!(
        span_refs(execute)
            .get("workflow.replay")
            .and_then(Value::as_str),
        Some("required"),
        "the execute_workflow span must require `workflow.replay`"
    );
    let page = read(PAGE);
    let rule = page_section(&page, REPLAY_SECTION);
    for word in ["MUST NOT", "MUST", "link"] {
        assert!(
            rule.contains(word),
            "{PAGE} section 6 must contain `{word}`"
        );
    }
}

#[test]
fn the_submission_status_is_recorded() {
    let page = read(PAGE);
    let status = page
        .lines()
        .find_map(|line| line.strip_prefix(STATUS_PREFIX))
        .unwrap_or_else(|| panic!("{PAGE} must have a `{STATUS_PREFIX}` line"))
        .trim();
    let state = status
        .split(|c: char| !c.is_ascii_alphabetic())
        .next()
        .unwrap_or_default();
    match state {
        "submitted" => assert!(
            status.contains("https://github.com/open-telemetry/"),
            "a submitted status must link the upstream issue or pull request"
        ),
        "declined" => assert!(
            status.split_whitespace().count() > 3,
            "a declined status must give the reason"
        ),
        "pending" => {
            let text = page_section(&page, ISSUE_TEXT_SECTION);
            assert!(
                text.contains("```markdown\n"),
                "a pending status needs the issue text in section 9"
            );
        }
        other => panic!("the status must be `pending`, `submitted` or `declined`, not `{other}`"),
    }
}

#[test]
fn prose_sentences_and_briefs_stay_short() {
    let mut texts = vec![(PAGE.to_owned(), read(PAGE))];
    for (id, attribute) in registry_attributes() {
        texts.push((
            id,
            str_field(&attribute, "brief")
                .unwrap_or_default()
                .to_owned(),
        ));
    }
    for (id, group) in span_groups() {
        for key in ["brief", "note"] {
            texts.push((
                id.clone(),
                str_field(&group, key).unwrap_or_default().to_owned(),
            ));
        }
    }
    let long: Vec<String> = texts
        .iter()
        .flat_map(|(source, text)| {
            prose_sentences(text)
                .into_iter()
                .filter(|sentence| sentence.split_whitespace().count() > MAX_SENTENCE_WORDS)
                .map(move |sentence| format!("{source}: {sentence}"))
        })
        .collect();
    assert!(
        long.is_empty(),
        "sentences over {MAX_SENTENCE_WORDS} words. Split each one:\n{}",
        long.join("\n")
    );
}

#[test]
fn the_adr_and_the_telemetry_page_link_the_proposal() {
    assert!(
        read(ADR).contains("../otel-workflow-semconv/README.md"),
        "{ADR} must link the proposal"
    );
    assert!(
        read(TELEMETRY_PAGE).contains("otel-workflow-semconv/README.md"),
        "{TELEMETRY_PAGE} must link the proposal"
    );
}

/// A docs-only change skips the `test` matrix, so `lint` must run these guards.
#[test]
fn guards_run_on_docs_only_changes() {
    let workflow = read(".github/workflows/ci.yml");
    let lint = workflow
        .find("\n  lint:")
        .expect("ci.yml must define `lint`");
    let test = workflow
        .find("\n  test:")
        .expect("ci.yml must define `test`");
    let stanza = step_stanza(&workflow[lint..test], FILTER).unwrap_or_else(|| {
        panic!(
            "the `lint` job in ci.yml must run these guards. Add this step:\n\
             \x20     - name: Run the OTel workflow conventions proposal guards (also on docs-only PRs)\n\
             \x20       run: \"cargo test -p autumn-harvest --no-default-features --features \
             testing {FILTER}\""
        )
    });
    check_stanza(stanza, FILTER).unwrap_or_else(|reason| panic!("{reason}. Stanza:\n{stanza}"));
}

#[test]
fn helpers_parse_their_inputs() {
    assert_eq!(cells("| `a` | b |"), Some(vec!["`a`", "b"]));
    assert_eq!(cells("not a row"), None);
    assert_eq!(code("`x.y`"), Some("x.y"));
    assert_eq!(code("x.y"), None);

    let page = "## 7. Mapping from Harvest\n\n| Harvest | Target | Note |\n|---|---|---|\n\
                | `harvest.queue` | `workflow.queue.name` | |\n\
                | `harvest.shard.id` | Not proposed. | Engine detail. |\n\
                | `other.name` | `x` | |\n\n## 8. Next\n| `harvest.late` | `y` | |\n";
    let parsed = mapping(page);
    assert_eq!(parsed.len(), 2);
    assert_eq!(parsed["harvest.queue"].0, "`workflow.queue.name`");
    assert_eq!(parsed["harvest.shard.id"].1, "Engine detail.");

    let adr = "### 2. Span catalogue\n\n#### 2.1 `harvest.a.b` — one\n\n| Property | Value |\n\
               |---|---|\n| Kind | `PRODUCER` |\n\n#### 2.2 `harvest.c.d` — two\n\n| Kind | `INTERNAL` |\n\
               \n### 3. Next\n#### 2.9 `harvest.z` — late\n| Kind | `CLIENT` |\n";
    assert_eq!(
        adr_spans(adr),
        BTreeMap::from([
            ("harvest.a.b".to_owned(), "producer".to_owned()),
            ("harvest.c.d".to_owned(), "internal".to_owned()),
        ])
    );

    let source = "pub const ATTR_A: &str = \"harvest.a\";\nconst ATTR_B: &str = \"b\";\n\
                  pub const METRIC_C: &str = \"harvest.c\";\n";
    assert_eq!(
        attr_constants(source),
        BTreeSet::from(["harvest.a".to_owned()])
    );

    assert!(is_well_named("workflow.run.id"));
    assert!(is_well_named("span.workflow.execute_workflow.internal"));
    assert!(!is_well_named("workflow"));
    assert!(!is_well_named("Workflow.name"));
    assert!(!is_well_named("workflow..name"));
    assert!(!is_well_named("workflow.run-id"));
}
