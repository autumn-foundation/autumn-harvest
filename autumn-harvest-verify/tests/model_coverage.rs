//! Coverage guards for the builtin determinism model (`harvest-verify.model.toml`).
//!
//! The model is data, and data rots differently from code: nothing in `src/`
//! stops a `WorkflowContext` method from being added, used across the examples,
//! and never classified. An unclassified ctx method is not a silent pass — the
//! analyzer emits `unknown("unmodeled-ctx-method")` — but an `unknown` verdict
//! is exactly as useless as a wrong one for the success metrics, and it
//! degrades quietly while CI stays green.
//!
//! These guards close that loop in both directions:
//!
//!   * [`every_ctx_method_used_by_the_examples_corpus_is_classified`] works from
//!     the OUTSIDE in — every `ctx.<method>(` this repo's examples and tests
//!     actually call must land in some bucket.
//!   * [`every_pub_method_on_workflow_context_is_classified`] works from the
//!     INSIDE out — it parses `autumn-harvest/src/context.rs` with `syn` and
//!     requires every public method of `impl WorkflowContext` to be classified,
//!     including the ~55 % of the surface no example happens to exercise.
//!
//! Deliberately NOT guarded: whether a classification is *correct*. That is a
//! judgement backed by the `reason` on each row (every one cites the line in
//! `context.rs` that proves it) and by the seeded corpus, not by a test that
//! could only restate the table.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use autumn_harvest_verify::model::{BUILTIN_MODEL_TOML, Model};

/// `<repo>`, i.e. the parent of `CARGO_MANIFEST_DIR` (`<repo>/autumn-harvest-verify`).
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent")
        .to_path_buf()
}

fn builtin() -> Model {
    Model::from_toml(BUILTIN_MODEL_TOML)
        .unwrap_or_else(|err| panic!("the builtin model must parse: {err}"))
}

/// Every method name the model classifies, in any bucket, on any receiver.
fn classified_names(model: &Model) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    let last_segment = |path: &str| path.rsplit("::").next().unwrap_or(path).trim().to_string();
    for r in &model.sink {
        names.insert(last_segment(&r.path));
    }
    for r in model
        .sanctioned
        .iter()
        .chain(&model.non_sink)
        .chain(&model.handler_registration)
    {
        names.insert(last_segment(&r.path));
    }
    for r in &model.source {
        names.insert(last_segment(&r.path));
    }
    names
}

/// Which buckets classify `name` (on any receiver).
fn buckets_for(model: &Model, name: &str) -> Vec<&'static str> {
    let hit = |path: &str| path.rsplit("::").next().unwrap_or(path) == name;
    let mut out = Vec::new();
    if model.sink.iter().any(|r| hit(&r.path)) {
        out.push("sink");
    }
    if model.sanctioned.iter().any(|r| hit(&r.path)) {
        out.push("sanctioned");
    }
    if model.non_sink.iter().any(|r| hit(&r.path)) {
        out.push("non_sink");
    }
    if model.handler_registration.iter().any(|r| hit(&r.path)) {
        out.push("handler_registration");
    }
    if model.source.iter().any(|r| hit(&r.path)) {
        out.push("source");
    }
    out
}

#[test]
fn builtin_model_parses() {
    let model = builtin();

    assert!(
        !model.version.trim().is_empty(),
        "the model must carry a version: it is printed alongside every verdict \
         (\"no non-determinism found, under model M, up to boundaries B\"), so a \
         verdict with no model version cannot be traced to the rules that produced it"
    );
    assert!(
        model.version.starts_with("2026.")
            || model.version.starts_with("2027.")
            || model.version.starts_with("2028."),
        "expected a `YYYY.MM.N` model version, found {:?}",
        model.version
    );

    // Every table must be populated. An empty one is the failure mode that
    // silently turns the analysis into a no-op: no sinks means nothing is ever
    // a finding, no sources means nothing is ever tainted.
    assert!(!model.source.is_empty(), "no `[[source]]` rows");
    assert!(!model.sink.is_empty(), "no `[[sink]]` rows");
    assert!(!model.sanctioned.is_empty(), "no `[[sanctioned]]` rows");
    assert!(!model.non_sink.is_empty(), "no `[[non_sink]]` rows");
    assert!(
        !model.handler_registration.is_empty(),
        "no `[[handler_registration]]` rows — registered handler closures would \
         then be silently unanalyzed, which is the one modelling choice that \
         makes `proven-deterministic` a lie"
    );
    assert!(!model.forbidden.is_empty(), "no `[[forbidden]]` rows");
    assert!(!model.sanitizer.is_empty(), "no `[[sanitizer]]` rows");
    assert!(!model.reduction.is_empty(), "no `[[reduction]]` rows");
    assert!(!model.trusted.is_empty(), "no `[[trusted]]` rows");
    assert!(!model.ambient_type.is_empty(), "no `[[ambient_type]]` rows");

    // A row without a reason is a rule nobody can review. The schema cannot
    // enforce non-emptiness, so the guard does.
    let mut unreasoned: Vec<String> = Vec::new();
    for r in &model.source {
        if r.reason.trim().is_empty() {
            unreasoned.push(format!("source {}", r.path));
        }
    }
    for r in &model.sink {
        if r.reason.trim().is_empty() {
            unreasoned.push(format!("sink {}", r.path));
        }
    }
    for r in model
        .sanctioned
        .iter()
        .chain(&model.non_sink)
        .chain(&model.handler_registration)
    {
        if r.reason.trim().is_empty() {
            unreasoned.push(format!("ctx-rule {}", r.path));
        }
    }
    for r in &model.forbidden {
        if r.reason.trim().is_empty() {
            unreasoned.push(format!("forbidden {}", r.path));
        }
    }
    for r in model.sanitizer.iter().chain(&model.reduction) {
        if r.reason.trim().is_empty() {
            unreasoned.push(format!("sanitizer/reduction {}", r.path));
        }
    }
    for r in &model.trusted {
        if r.reason.trim().is_empty() {
            unreasoned.push(format!("trusted {}", r.name));
        }
    }
    for r in &model.ambient_type {
        if r.reason.trim().is_empty() {
            unreasoned.push(format!("ambient_type {}", r.name));
        }
    }
    assert!(
        unreasoned.is_empty(),
        "every model row must carry a non-empty `reason` — an unexplained rule \
         is how a model rots. Rows missing one:\n  {}",
        unreasoned.join("\n  ")
    );
}

/// `side_effect` is simultaneously a sink (it emits a `RecordSideEffect` command)
/// and a sanctioned source (its recorded result replays verbatim). Getting that
/// backwards in either direction is the single most consequential modelling
/// error available: sink-only turns every correct `ctx.side_effect` in the corpus
/// into a false positive; sanctioned-only drops the control-taint finding that
/// motivates the analysis.
///
/// The same dual shape holds for the recorded-primitive family. This guard pins
/// the dual set exactly, so a future edit that promotes or demotes one of them
/// has to say so here.
#[test]
fn dual_role_rows_are_exactly_the_recorded_primitive_family() {
    const EXPECTED_DUAL: &[&str] = &[
        "business_days_from_now",
        "new_uuid",
        "patched",
        "random_f64",
        "random_range",
        "random_u64",
        "random_uuid",
        "should_continue_as_new",
        "side_effect",
        "system_now",
        "system_time_now",
        "time_until_deadline",
        "version",
    ];

    let model = builtin();
    let sinks: BTreeSet<&str> = model.sink.iter().map(|r| r.path.as_str()).collect();
    let sanctioned: BTreeSet<&str> = model.sanctioned.iter().map(|r| r.path.as_str()).collect();
    let dual: BTreeSet<&str> = sinks.intersection(&sanctioned).copied().collect();
    let expected: BTreeSet<&str> = EXPECTED_DUAL.iter().copied().collect();

    assert_eq!(
        dual, expected,
        "the set of ctx methods that are BOTH a sink and sanctioned changed.\n\
         Only the recorded-primitive family belongs here: the call emits a \
         command (so a tainted argument or a tainted decision to call it is a \
         finding) while the recorded result replays verbatim (so the return is \
         clean). Update EXPECTED_DUAL only alongside the `reason` that justifies it."
    );

    let side_effect = model
        .sink
        .iter()
        .find(|r| r.path == "side_effect")
        .expect("side_effect must be a sink");
    assert_eq!(
        side_effect.opaque_closure_args,
        vec![1],
        "`side_effect(&self, id, f)` — the closure is argument index 1 (zero-based, \
         excluding `self`) and must NOT be descended into: laundering \
         non-determinism inside it is the primitive's entire purpose"
    );
    assert_eq!(
        side_effect.args,
        vec![0],
        "`side_effect`'s checked argument is the id (index 0): a tainted side-effect \
         id diverges the recorded marker even though the closure's result does not"
    );
}

/// Names that the `ctx.<ident>(` grep finds but that are not `WorkflowContext`
/// or `ActivityContext` methods at all.
///
/// * `clone` / `as_ref` are `ctx.clone()` / `ctx.as_ref()` on the context handle
///   itself — universal trait methods, not engine surface.
/// * `read_logs` appears only inside a `//!` doc table in
///   `autumn-harvest/examples/workflow_logs.rs`, which says *there is no such
///   method*; `current_time` appears only inside a string literal in
///   `guardrail_catalog_tests.rs` (the guardrail's suggested-alternative text).
///   Both are grep artifacts, and both are listed rather than filtered by a
///   cleverer regex so the exemption is visible.
const NOT_CTX_METHODS: &[&str] = &["as_ref", "clone", "current_time", "read_logs"];

/// Files whose `ctx.` calls are engine-facing driver code rather than workflow
/// bodies. They are still classified (as `[[non_sink]]`), so this list exists
/// only to document that the grep intentionally includes them.
const HARNESS_ONLY_METHODS: &[&str] = &["drain_commands", "drain_signals"];

fn corpus_files() -> Vec<PathBuf> {
    let root = repo_root();
    let mut files = Vec::new();
    let mut push_dir = |dir: PathBuf, recurse: bool| {
        collect_rs(&dir, recurse, &mut files);
    };
    push_dir(root.join("autumn-harvest/examples"), false);
    push_dir(root.join("autumn-harvest/tests"), true);
    // `examples/*/src/*.rs` — the standalone workspace example crates.
    if let Ok(entries) = std::fs::read_dir(root.join("examples")) {
        for entry in entries.flatten() {
            collect_rs(&entry.path().join("src"), true, &mut files);
        }
    }
    files
}

fn collect_rs(dir: &Path, recurse: bool, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if recurse {
                collect_rs(&path, recurse, out);
            }
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// Every `ctx.<ident>(` in `text`, ignoring whole-line `//` comments.
///
/// Line comments are stripped because two of this repo's files *describe* ctx
/// methods in prose — one doc table says, of a method that deliberately does not
/// exist, "there is no `ctx.read_logs()`". Counting that would make the guard
/// demand a model row for it.
fn ctx_calls(text: &str) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    for line in text.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("//") {
            continue;
        }
        let bytes = line.as_bytes();
        let mut search_from = 0usize;
        while let Some(rel) = line[search_from..].find("ctx.") {
            let at = search_from + rel;
            search_from = at + 4;
            // Require a non-identifier byte before `ctx` so `activity_ctx.` and
            // `wctx.` are still matched but `my_ctx_thing.` is not treated as
            // starting at `ctx`.
            if at > 0 {
                let prev = bytes[at - 1];
                if prev.is_ascii_alphanumeric() && prev != b'_' {
                    continue;
                }
            }
            let rest = &line[at + 4..];
            let ident: String = rest
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            if ident.is_empty() {
                continue;
            }
            if rest[ident.len()..].trim_start().starts_with('(') {
                found.insert(ident);
            }
        }
    }
    found
}

#[test]
fn every_ctx_method_used_by_the_examples_corpus_is_classified() {
    let model = builtin();
    let classified = classified_names(&model);

    let mut used: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for file in corpus_files() {
        let Ok(text) = std::fs::read_to_string(&file) else {
            continue;
        };
        for name in ctx_calls(&text) {
            used.entry(name)
                .or_default()
                .push(file.display().to_string());
        }
    }

    assert!(
        used.len() > 50,
        "the corpus grep found only {} distinct `ctx.` methods — the measured \
         baseline is ~73 across `autumn-harvest/examples`, `examples/*/src` and \
         `autumn-harvest/tests`. A collapse this large means the grep broke, not \
         that the corpus shrank",
        used.len()
    );

    let mut missing: Vec<String> = Vec::new();
    for (name, files) in &used {
        if NOT_CTX_METHODS.contains(&name.as_str()) {
            assert!(
                !classified.contains(name),
                "`{name}` is listed in NOT_CTX_METHODS as a grep artifact, but the \
                 model classifies it. Remove it from one list or the other"
            );
            continue;
        }
        if !classified.contains(name) {
            let first = files.first().map_or("?", String::as_str);
            missing.push(format!("{name}  (e.g. {first})"));
        }
    }

    assert!(
        missing.is_empty(),
        "{} ctx method(s) are called by this repo's own examples/tests but are not \
         classified in harvest-verify.model.toml. Each one makes every workflow \
         that calls it `unknown(\"unmodeled-ctx-method\")`, which counts against \
         the detection metric exactly as a miss does. Add a `[[sink]]`, \
         `[[sanctioned]]`, `[[non_sink]]`, `[[handler_registration]]` or \
         `[[source]]` row (with a `reason` citing the line in context.rs that \
         proves it):\n  {}",
        missing.len(),
        missing.join("\n  ")
    );

    for name in HARNESS_ONLY_METHODS {
        assert!(
            classified.contains(*name),
            "`{name}` is engine-facing driver code, but it is still classified \
             (as `[[non_sink]]`) rather than filtered out, so a reader \
             recomputing the call-site census gets the same number"
        );
    }
}

#[test]
fn every_pub_method_on_workflow_context_is_classified() {
    let context_rs = repo_root().join("autumn-harvest/src/context.rs");
    let text = std::fs::read_to_string(&context_rs)
        .unwrap_or_else(|err| panic!("cannot read {}: {err}", context_rs.display()));
    let file = syn::parse_file(&text)
        .unwrap_or_else(|err| panic!("cannot parse {}: {err}", context_rs.display()));

    let mut methods: BTreeSet<String> = BTreeSet::new();
    for item in &file.items {
        let syn::Item::Impl(item_impl) = item else {
            continue;
        };
        if item_impl.trait_.is_some() {
            continue;
        }
        let syn::Type::Path(type_path) = item_impl.self_ty.as_ref() else {
            continue;
        };
        let is_ctx = type_path
            .path
            .segments
            .last()
            .is_some_and(|s| s.ident == "WorkflowContext");
        if !is_ctx {
            continue;
        }
        for impl_item in &item_impl.items {
            let syn::ImplItem::Fn(method) = impl_item else {
                continue;
            };
            if matches!(method.vis, syn::Visibility::Public(_)) {
                methods.insert(method.sig.ident.to_string());
            }
        }
    }

    assert!(
        methods.len() > 100,
        "expected `impl WorkflowContext` to expose >100 public methods (the \
         audited count is 160); found {}. Either the parse failed or the impl \
         block moved",
        methods.len()
    );

    let model = builtin();
    let mut missing: Vec<String> = Vec::new();
    for name in &methods {
        if buckets_for(&model, name).is_empty() {
            missing.push(name.clone());
        }
    }

    assert!(
        missing.is_empty(),
        "{} public method(s) of `impl WorkflowContext` are unclassified. An \
         unmodelled ctx method yields `unknown(\"unmodeled-ctx-method\")`, so \
         every workflow touching one is un-verifiable. Oracle for the decision: a \
         method is a SINK iff it reaches `WorkflowContext::push_command` \
         transitively AND the command it pushes is matched against recorded \
         history (a push that returns early under `is_replaying()` is \
         replay-suppressed bookkeeping, i.e. `[[non_sink]]`).\n\
         Unclassified:\n  {}",
        missing.len(),
        missing.join("\n  ")
    );
}

/// A `[[trusted]]` crate is modelled as a pure taint-propagator. That is what
/// makes `format!` tractable — and it is also how a source could be silently
/// swallowed: `chrono` is trusted, so if `Utc::now` were not *also* a source row
/// it would propagate cleanly and every clock read through chrono would vanish.
///
/// The precedence rule ("sources match before the trusted default") lives in the
/// matcher, which is GREEN-phase code. What this guard can check structurally,
/// today, is the other half: that the source rows which the precedence rule
/// exists to protect are actually present for every trusted crate that ships a
/// non-deterministic entry point.
#[test]
fn sources_never_shadowed_by_trusted_crates() {
    // (trusted crate, a source/forbidden row that must exist for it)
    const CARVE_OUTS: &[(&str, &[&str])] = &[
        ("chrono", &["Utc::now", "Local::now"]),
        ("uuid", &["Uuid::new_v4", "Uuid::now_v7"]),
        ("rand", &["thread_rng", "random", "Rng::gen_range"]),
        ("std", &["SystemTime::now", "Instant::now", "var"]),
        ("once_cell", &["Lazy::force"]),
    ];

    let model = builtin();
    let trusted: BTreeSet<&str> = model.trusted.iter().map(|c| c.name.as_str()).collect();
    let source_paths: BTreeSet<&str> = model.source.iter().map(|r| r.path.as_str()).collect();
    let mut problems: Vec<String> = Vec::new();
    for (crate_name, required) in CARVE_OUTS {
        if !trusted.contains(crate_name) {
            problems.push(format!(
                "`{crate_name}` is no longer a `[[trusted]]` crate; either restore \
                 it or drop its carve-out row here"
            ));
            continue;
        }
        for path in *required {
            if !source_paths.contains(path) {
                problems.push(format!(
                    "`{crate_name}` is trusted but has no `[[source]]` row for \
                     `{path}` — the trusted default would propagate it cleanly and \
                     the clock/entropy read would disappear from every trace"
                ));
            }
        }
    }

    // `tokio` is the same shape, carved out by `[[forbidden]]` rather than `[[source]]`.
    let forbidden_paths: BTreeSet<&str> = model.forbidden.iter().map(|r| r.path.as_str()).collect();
    if trusted.contains("tokio") {
        for path in ["sleep", "tokio::spawn", "spawn_blocking"] {
            if !forbidden_paths.contains(path) {
                problems.push(format!(
                    "`tokio` is trusted but `{path}` has no `[[forbidden]]` row"
                ));
            }
        }
    }

    assert!(
        problems.is_empty(),
        "trusted-crate carve-outs are incomplete:\n  {}",
        problems.join("\n  ")
    );

    // `indexmap` must stay OUT of the order sources: its deterministic iteration
    // is the recommended fix for a hash-order finding, and a source row for it
    // would turn the fix into a finding of its own.
    assert!(
        !model
            .source
            .iter()
            .any(|r| r.receiver.as_deref() == Some("IndexMap")),
        "`IndexMap` iterates in insertion order and must never be an `Order` \
         source — it is the fix this analyzer recommends"
    );
}

/// The bare `var` suffix problem, pinned.
///
/// MIR prints `std::env::var` as the bare callee `var`, so the row cannot key on
/// a path prefix; a bare `var` rule would match any user function named `var`.
/// The `dest_type` field exists solely to close that, and this guard fails if
/// someone removes the pin.
#[test]
fn ambiguous_bare_suffix_rows_are_pinned_by_dest_type() {
    const MUST_BE_PINNED: &[&str] = &["var", "var_os", "vars", "args", "current_dir", "temp_dir"];

    let model = builtin();

    for path in MUST_BE_PINNED {
        let row = model
            .source
            .iter()
            .find(|r| r.path == *path)
            .unwrap_or_else(|| panic!("`{path}` must be a `[[source]]` row"));
        assert!(
            row.dest_type.as_ref().is_some_and(|t| !t.trim().is_empty()),
            "`[[source]] path = \"{path}\"` is a single bare segment: without a \
             `dest_type` pin it matches any user function of that name. MIR prints \
             callee paths trimmed but `let` declarations fully qualified, which is \
             what `dest_type` reads"
        );
    }

    // Both `sleep` rows are bare suffixes and both must be pinned: rustc trims
    // `tokio::time::sleep` and `std::thread::sleep` to the same one segment, and
    // only their destination types tell them apart (a `Sleep` future vs `()`).
    let sleeps: BTreeSet<Option<&str>> = model
        .forbidden
        .iter()
        .filter(|r| r.path == "sleep")
        .map(|r| r.dest_type.as_deref())
        .collect();
    assert!(
        sleeps.contains(&Some("tokio::time::Sleep")) && sleeps.contains(&Some("()")),
        "the bare `sleep` suffix must carry one pinned row per real target; got \
         {sleeps:?}"
    );
    assert!(
        !sleeps.contains(&None),
        "an unpinned `sleep` row would also match a user-defined `sleep`"
    );

    // `collect` is only a sanitizer when it collects INTO a sorted collection.
    for row in model.sanitizer.iter().filter(|r| r.path == "collect") {
        assert!(
            row.dest_type.is_some(),
            "`collect` clears `Order` only for a sorted destination; an unpinned \
             `collect` row would also clear it for `collect::<Vec<_>>`, which \
             preserves the hash order verbatim"
        );
    }
}

#[test]
fn every_dual_role_sink_that_checks_all_arguments_is_accounted_for() {
    // `Model::classify` returns `Sink` before `Sanctioned` and the analyzer's
    // sink arm is total, so a method in both tables is treated as a sink. A sink
    // row with `args` unset checks *every* argument, which is right for the
    // recorded-primitive family — a tainted day count or a tainted `patched` id
    // really does diverge the recorded history — but it is a decision, not a
    // default. Pinning the set means a *new* dual-role row has to say which of
    // the two it is instead of inheriting "check everything" by accident.
    const CHECKS_EVERY_ARGUMENT: &[&str] = &[
        "business_days_from_now",
        "new_uuid",
        "patched",
        "random_f64",
        "random_range",
        "random_u64",
        "random_uuid",
        "should_continue_as_new",
        "system_now",
        "system_time_now",
        "time_until_deadline",
        "version",
    ];

    let model = builtin();
    let sanctioned: BTreeSet<(&str, &str)> = model
        .sanctioned
        .iter()
        .map(|r| (r.path.as_str(), r.receiver.as_str()))
        .collect();
    let unpinned: BTreeSet<&str> = model
        .sink
        .iter()
        .filter(|r| r.args.is_empty())
        .filter(|r| sanctioned.contains(&(r.path.as_str(), r.receiver.as_str())))
        .map(|r| r.path.as_str())
        .collect();
    let expected: BTreeSet<&str> = CHECKS_EVERY_ARGUMENT.iter().copied().collect();
    assert_eq!(
        unpinned, expected,
        "a `[[sink]]` row that is also `[[sanctioned]]` and leaves `args` unset \
         reports every argument of a primitive the model otherwise calls clean. \
         Either name the checked arguments or add the row here with the reason \
         its whole argument list is history-relevant."
    );
}

#[test]
fn an_overlay_with_an_unknown_table_or_field_is_an_error() {
    // A typo in an overlay used to be silently ignored: the intended rule never
    // entered the model and the run reported `proven` under a model the user
    // believed they had widened.
    let typo_table = Model::from_toml(
        r#"
[[sourcez]]
path = "my_clock"
kind = "value"
reason = "typo in the table name"
"#,
    );
    assert!(
        typo_table.is_err(),
        "an unknown table must be an error, not a silently dropped rule"
    );

    let typo_field = Model::from_toml(
        r#"
[[source]]
path = "my_clock"
kind = "value"
reason = "ok"
recevier = "Clock"
"#,
    );
    assert!(
        typo_field.is_err(),
        "an unknown field must be an error, not a silently dropped constraint"
    );

    assert!(
        Model::from_toml(
            r#"
[[source]]
path = "my_clock"
receiver = "Clock"
kind = "value"
reason = "a well-formed row still parses"
"#
        )
        .is_ok()
    );
}

#[test]
fn model_overlay_merges_rows() {
    // AC4: the sanctioned set must be extensible "without a release". The overlay
    // is that mechanism — `--model extra.toml` unions the rows, and a later row
    // with the same key replaces the earlier one.
    let overlay = Model::from_toml(
        r#"
version = "test-overlay"

[[sanctioned]]
path = "my_recorded_helper"
receiver = "WorkflowContext"
reason = "First-party wrapper that records its own result."

[[source]]
path = "SystemTime::now"
kind = "value"
reason = "Overridden reason: the later row for the same key replaces the earlier one."
"#,
    )
    .expect("the overlay fixture must parse");

    let merged = builtin().merged_with(overlay);

    assert!(
        merged
            .sanctioned
            .iter()
            .any(|r| r.path == "my_recorded_helper"),
        "an overlay row must be added to the merged model"
    );
    assert!(
        merged.sanctioned.iter().any(|r| r.path == "side_effect"),
        "merging must be a UNION: the builtin rows survive an overlay"
    );

    let now_rows: Vec<_> = merged
        .source
        .iter()
        .filter(|r| r.path == "SystemTime::now")
        .collect();
    assert_eq!(
        now_rows.len(),
        1,
        "a later row with the same (table, path, receiver) key must REPLACE the \
         earlier one, not duplicate it"
    );
    assert!(
        now_rows[0].reason.starts_with("Overridden reason:"),
        "the overlay's row must win, not the builtin's"
    );
}

mod api_gap {
    //! Documented API gap, kept as a module note rather than a floating comment.
    //!
    //! Rule *matching* — path-suffix matching, receiver narrowing, `dest_type`
    //! pinning and the source-beats-trusted precedence order — lives in the
    //! analyzer, which is GREEN-phase code. Until it exists there is no API to
    //! assert precedence against, so
    //! [`super::sources_never_shadowed_by_trusted_crates`] asserts the
    //! structural precondition (the carve-out rows exist) rather than the
    //! behaviour (they win).
    //!
    //! When the matcher lands, that test should grow a behavioural half:
    //! resolve `autumn_harvest::chrono::Utc::now` against the full model and
    //! assert the answer is the `[[source]]` row, not the `[[trusted]]` default.
}
