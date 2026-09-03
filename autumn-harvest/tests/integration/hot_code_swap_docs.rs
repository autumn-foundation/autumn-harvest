//! Guards that keep the issue-#967 R&D report honest against the code it audits.
//!
//! `docs/rnd/hot-code-swap.md` is a go/no-go decision document. Its load-bearing
//! claims are not opinions — they are assertions about *this* codebase: that
//! workflow handlers are bare `fn` pointers, that `ctx.build_id()` reports the
//! worker's build rather than the execution's, that `harvest_events` has exactly
//! two sanctioned in-place writers, that the shipped build-routing APIs the
//! design leans on actually exist, and that the spike is invisible to a default
//! build. Every one of those is true the day it is written and quietly false one
//! merge later.
//!
//! These guards make the claims falsifiable. They re-derive each fact from the
//! live tree at test runtime and assert the report agrees. Unlike the spike
//! itself, this suite is **not** feature-gated: a default `cargo test` run must
//! catch a report that has drifted from the code, whether or not wasmtime is
//! compiled.
//!
//! Deliberately *not* covered: the report's prose judgements — the cost tiers,
//! the go/no-go reasoning, the comparison against the do-nothing baseline. Those
//! are argument, not fact, and freezing them would stop the document being
//! revisable. Mirrors the split in `sqlite_feasibility_docs.rs`.

use std::path::{Path, PathBuf};

/// `<repo>`, i.e. the parent of `CARGO_MANIFEST_DIR` (`<repo>/autumn-harvest`).
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent")
        .to_path_buf()
}

/// Read a file with line endings normalised to `\n`.
///
/// Every guard here does substring matching over the document; under a CRLF
/// checkout the raw text carries `\r` that `str::lines()` strips, so a literal
/// containing a newline would never match. Normalising once on read removes the
/// whole class. Mirrors `sqlite_feasibility_docs::read_normalized`.
fn read_normalized(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|err| panic!("cannot read {}: {err}", path.display()))
        .replace("\r\n", "\n")
}

fn report_path() -> PathBuf {
    repo_root().join("docs/rnd/hot-code-swap.md")
}

fn read_report() -> String {
    let path = report_path();
    let body = std::fs::read_to_string(&path).unwrap_or_else(|err| {
        panic!(
            "issue #967 deliverable 1 is missing: could not read {}: {err}\n\
             The R&D report is the primary deliverable of the spike — the success \
             metric requires leadership to reach a go/no-go from the report alone, \
             without reading the spike code.",
            path.display()
        )
    });
    body.replace("\r\n", "\n")
}

fn read_src(relative: &str) -> String {
    read_normalized(&repo_root().join("autumn-harvest").join(relative))
}

// ── deliverable structure ─────────────────────────────────────────────────────

/// Every section the issue's acceptance criteria name, keyed by the AC it
/// serves. A missing heading is a missing deliverable, not a formatting nit.
const REQUIRED_SECTIONS: &[(&str, &str)] = &[
    ("AC1", "## 1. The hosting question"),
    (
        "AC1",
        "## 2. Hard constraints the host boundary must satisfy",
    ),
    ("AC1", "## 3. Option A — dylib hosting (`libloading`)"),
    ("AC1", "## 4. Option B — WebAssembly hosting"),
    ("AC1", "## 5. The recommended host boundary"),
    ("AC2", "## 6. Module registry design"),
    ("AC4", "## 7. Zero replay-surface change"),
    ("AC5", "## 8. Safety analysis"),
    ("AC6", "## 9. Go / no-go"),
    ("AC3", "## 10. What the prototype demonstrates"),
];

#[test]
fn the_report_carries_every_required_section() {
    let report = read_report();
    for (ac, heading) in REQUIRED_SECTIONS {
        assert!(
            report.contains(heading),
            "issue #967 {ac} needs a `{heading}` section in docs/rnd/hot-code-swap.md"
        );
    }
}

#[test]
fn the_report_reaches_an_explicit_verdict() {
    let report = read_report().to_lowercase();
    assert!(
        report.contains("**verdict:**"),
        "the go/no-go section must state a bolded `**Verdict:**` a reader cannot miss"
    );
    assert!(
        report.contains("do-nothing baseline"),
        "AC6 requires the recommendation to compare explicitly against the \
         do-nothing baseline (blue/green fleets + build routing)"
    );
}

// ── constraint inventory vs live code ─────────────────────────────────────────

#[test]
fn the_handler_fn_pointer_constraint_is_still_true() {
    // The report's central constraint: handlers are bare `fn` pointers, which is
    // exactly what a runtime-loaded module cannot produce. If someone ever
    // widens these to `Arc<dyn Fn>`, the report's core argument changes and it
    // must be rewritten rather than silently left standing.
    let info = read_src("src/info.rs");
    assert!(
        info.contains("pub type WorkflowHandlerFn =\n    fn("),
        "WorkflowHandlerFn is no longer a bare `fn` pointer — \
         docs/rnd/hot-code-swap.md §2 is built on that constraint"
    );
    assert!(
        info.contains("pub type ActivityHandlerFn =\n    fn("),
        "ActivityHandlerFn is no longer a bare `fn` pointer — see §2 of the report"
    );
    let report = read_report();
    assert!(
        report.contains("WorkflowHandlerFn") && report.contains("fn` pointer"),
        "§2 must name the `fn`-pointer constraint it is reasoning about"
    );
}

#[test]
fn the_build_id_routing_hazard_is_still_real() {
    // The report warns that `ctx.build_id()` is the WORKER's configured build,
    // not the execution's `assigned_build_id`, and that routing modules on it
    // would drag in-flight work onto new code. That warning is only worth
    // printing while the semantics still hold.
    let context = read_src("src/context.rs");
    assert!(
        context.contains("The worker build ID of the worker executing this workflow."),
        "ctx.build_id()'s documented meaning changed — re-check §6 of the report"
    );
    let report = read_report();
    assert!(
        report.contains("ctx.build_id()") && report.contains("assigned_build_id"),
        "the report must spell out the ctx.build_id() vs assigned_build_id hazard"
    );
}

#[test]
fn the_shipped_routing_apis_the_design_leans_on_exist() {
    // The whole premise is "zero new safety machinery": the design consumes
    // shipped APIs unchanged. A rename here silently turns the design into
    // vapour, so each named symbol is checked against the live module.
    let routing = read_src("src/build_routing.rs");
    for symbol in [
        "pub fn resolve_assigned_build",
        "pub fn is_eligible",
        "pub async fn set_build_ramp",
        "pub async fn clear_build_ramp",
        "pub async fn set_build_policy",
        "pub async fn declare_compat",
        "pub async fn build_reachability",
    ] {
        assert!(
            routing.contains(symbol),
            "build_routing.rs no longer provides `{symbol}`, which \
             docs/rnd/hot-code-swap.md §6 consumes unchanged"
        );
    }
    let report = read_report();
    for cited in [
        "set_build_ramp",
        "clear_build_ramp",
        "BuildCompatibilitySet",
        "build_reachability",
    ] {
        assert!(
            report.contains(cited),
            "§6 must name `{cited}` — it is how the design avoids new machinery"
        );
    }
}

#[test]
fn the_append_only_exception_count_matches_claude_md() {
    // The report asserts it adds no third writer of `harvest_events.event_data`.
    // CLAUDE.md is the authority on how many there are; if a fourth ever lands,
    // this fails and the claim gets re-examined instead of aging into a lie.
    let claude_md = read_normalized(&repo_root().join("CLAUDE.md"));
    assert!(
        claude_md
            .contains("Exactly **two** code paths write `harvest_events.event_data` after insert"),
        "CLAUDE.md's append-only exception count changed — re-check §7 of the report"
    );
    let report = read_report();
    // Asserting merely that the report mentions `harvest_events` was vacuous:
    // the word appears in §2's C7 unconditionally, so the guard would pass a
    // report rewritten to claim this change ADDS a writer. Pin the substantive
    // claim instead.
    assert!(
        report.contains("no third in-place writer of `harvest_events.event_data`"),
        "§7 must state, in those words, that this change adds no third in-place \
         writer of `harvest_events.event_data` — the claim CLAUDE.md's exception \
         count is what makes checkable"
    );
}

// ── the spike is invisible to a default build ─────────────────────────────────

fn manifest() -> String {
    read_normalized(&repo_root().join("autumn-harvest/Cargo.toml"))
}

#[test]
fn the_feature_exists_and_is_not_in_the_default_set() {
    let manifest = manifest();
    assert!(
        manifest.contains("\nhot-code-swap = "),
        "the spike must live behind a `hot-code-swap` Cargo feature (AC7)"
    );
    let default_line = manifest
        .lines()
        .find(|l| l.trim_start().starts_with("default = ["))
        .expect("a `default = [...]` line must exist");
    assert!(
        !default_line.contains("hot-code-swap"),
        "AC7: `hot-code-swap` must never be in the default feature set, got: {default_line}"
    );
}

#[test]
fn the_feature_adds_no_new_dependency_to_the_workspace() {
    // AC7 says the default build is byte-for-byte unaffected; the stronger
    // property the spike actually holds is that it adds no dependency at all,
    // because it reuses the wasmtime embedding issue #965 already vetted.
    let manifest = manifest();
    let feature_line = manifest
        .lines()
        .find(|l| l.trim_start().starts_with("hot-code-swap = "))
        .expect("the feature must be declared");
    assert!(
        !feature_line.contains("dep:"),
        "hot-code-swap must enable no new optional dependency — it reuses the \
         wasm-activities engine. Got: {feature_line}"
    );
    assert!(
        feature_line.contains("wasm-activities"),
        "hot-code-swap must build on the reviewed wasm-activities embedding: {feature_line}"
    );
}

#[test]
fn every_spike_module_is_feature_gated_in_lib_rs() {
    let lib = read_src("src/lib.rs");
    for module in ["hot_swap", "hot_swap_store"] {
        let decl = format!("pub mod {module};");
        let idx = lib
            .find(&decl)
            .unwrap_or_else(|| panic!("lib.rs must declare `{decl}` (the spike's public surface)"));
        let preceding = &lib[..idx];
        assert!(
            preceding
                .lines()
                .rev()
                .take(3)
                .any(|l| l.contains("feature = \"hot-code-swap\"")),
            "`{module}` must be gated on the `hot-code-swap` feature (AC7)"
        );
    }
}

// ── cited evidence actually exists ────────────────────────────────────────────

/// Tests the report points at as its evidence. A report that cites a test which
/// has been renamed or deleted is worse than one that cites nothing.
const CITED_TESTS: &[&str] = &[
    "a_module_hosted_history_is_byte_identical_to_the_statically_linked_one",
    "a_module_hosted_history_replays_clean_under_statically_linked_code",
    "a_statically_linked_history_replays_clean_under_module_hosting",
    "the_module_is_chosen_by_the_executions_build_not_by_the_workers_build_id",
    "two_modules_may_not_claim_one_workflow_name_under_one_build_id",
    "unloading_a_build_drops_its_modules_but_not_a_live_holder",
    "hot_swap_ramp_and_rollback_without_a_restart",
    "a_running_worker_adopts_v2_under_a_new_build_id_without_restarting",
    "a_spinning_guest_is_bounded_by_fuel_and_the_epoch_deadline",
    "a_guest_that_never_completes_is_stopped_by_the_decide_step_cap",
    "syncing_refuses_a_module_whose_stored_bytes_were_tampered_with",
    "a_build_ids_module_binding_is_immutable",
    "a_trapping_guest_is_contained_as_a_workflow_error",
    "hosting_never_introduces_a_new_event_variant",
    "decide_request_serialises_step_first_so_a_wat_guest_can_read_it",
    "the_hosts_encoder_never_reorders_keys_the_way_a_json_value_would",
    "an_activity_failure_is_handed_to_the_guest_rather_than_failing_the_run",
    "only_activity_outcomes_are_handed_to_the_guest",
    "a_signed_module_cannot_be_rebound_under_another_build_id",
    "a_failing_module_leaves_the_whole_build_unbound",
    "a_guest_may_not_schedule_an_activity_the_host_did_not_allow",
    "a_guest_may_not_pick_the_queue_unless_the_host_allows_it",
];

#[test]
fn every_test_the_report_cites_exists() {
    // A report that cites a test which has been renamed or deleted is worse
    // than one that cites nothing, so both directions are checked: the test
    // exists, AND the report actually cites it.
    //
    // Two homes, because some claims are about pure functions and belong in the
    // module's own unit tests rather than the DB-capable integration suite.
    let suite = read_src("tests/integration/hot_code_swap_tests.rs");
    let unit = read_src("src/hot_swap.rs");
    let report = read_report();
    for name in CITED_TESTS {
        let declared = |src: &str| {
            src.contains(&format!("async fn {name}(")) || src.contains(&format!("fn {name}("))
        };
        assert!(
            declared(&suite) || declared(&unit),
            "the report's evidence cites `{name}`, which exists in neither \
             tests/integration/hot_code_swap_tests.rs nor src/hot_swap.rs"
        );
        assert!(
            report.contains(name),
            "`{name}` is part of the spike's evidence but the report never cites it"
        );
    }
}

#[test]
fn the_guest_modules_the_report_points_at_exist() {
    let root = repo_root().join("autumn-harvest/examples/workflow-modules");
    for guest in ["README.md", "pipeline_v1.wat", "pipeline_v2.wat"] {
        assert!(
            root.join(guest).exists(),
            "docs/rnd/hot-code-swap.md points readers at \
             autumn-harvest/examples/workflow-modules/{guest}, which is missing"
        );
    }
    let report = read_report();
    assert!(
        report.contains("examples/workflow-modules"),
        "the report must point at the guest modules CI actually executes"
    );
}

#[test]
fn the_migration_the_registry_design_names_exists() {
    let migrations = repo_root().join("autumn-harvest/migrations");
    let found: Vec<String> = std::fs::read_dir(&migrations)
        .expect("migrations directory")
        .filter_map(std::result::Result::ok)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with("_harvest_workflow_modules"))
        .collect();
    assert_eq!(
        found.len(),
        1,
        "exactly one `*_harvest_workflow_modules` migration must exist, found {found:?}"
    );
    let version = found[0]
        .split('_')
        .next()
        .expect("migration name has a version prefix")
        .to_string();
    assert_eq!(
        version.len(),
        14,
        "second-precision UTC prefix required (CLAUDE.md)"
    );
    assert!(
        !version.ends_with("000000"),
        "CLAUDE.md forbids a day-only prefix with a zeroed time: {version}"
    );
    let report = read_report();
    assert!(
        report.contains("harvest_workflow_modules"),
        "§6 must name the table it designs"
    );
}

// ── the hand-written guests' declared lengths ─────────────────────────────────

/// Every `(data (i32.const OFFSET) "...")` segment in a `.wat` guest, as
/// `(offset, decoded_byte_length)`.
///
/// The only escape the guests use is `\"`, so decoding is a single replacement.
/// A guest that grows a richer escape needs this parser extended — which is a
/// deliberate trip-wire, not a limitation to work around.
fn data_segments(wat: &str) -> Vec<(u32, usize)> {
    let mut out = Vec::new();
    for line in wat.lines() {
        let Some(rest) = line.trim().strip_prefix("(data (i32.const ") else {
            continue;
        };
        let Some((offset, rest)) = rest.split_once(')') else {
            continue;
        };
        let offset: u32 = offset
            .trim()
            .parse()
            .expect("data segment offset is a number");
        let start = rest.find('"').expect("data segment has an opening quote") + 1;
        let end = rest.rfind('"').expect("data segment has a closing quote");
        assert!(start <= end, "malformed data segment: {line}");
        let decoded = rest[start..end].replace("\\\"", "\"");
        assert!(
            !decoded.contains('\\'),
            "unsupported WAT escape in a data segment; extend this parser: {line}"
        );
        out.push((offset, decoded.len()));
    }
    out
}

/// Every `(call $pack (i32.const PTR) (i32.const LEN))` in a `.wat` guest.
fn pack_calls(wat: &str) -> Vec<(u32, usize)> {
    let mut out = Vec::new();
    let mut rest = wat;
    while let Some(idx) = rest.find("(call $pack (i32.const ") {
        let tail = &rest[idx + "(call $pack (i32.const ".len()..];
        let (ptr, tail) = tail.split_once(')').expect("pack pointer arg closes");
        let tail = tail
            .trim_start()
            .strip_prefix("(i32.const ")
            .expect("pack takes a second i32.const length");
        let (len, tail) = tail.split_once(')').expect("pack length arg closes");
        out.push((
            ptr.trim().parse().expect("pack pointer is a number"),
            len.trim().parse().expect("pack length is a number"),
        ));
        rest = tail;
    }
    out
}

#[test]
fn every_guest_returns_exactly_as_many_bytes_as_its_data_segment_holds() {
    // The guests hard-code the byte length of each pre-baked JSON response
    // alongside its data-segment offset. Get one wrong and the host reads a
    // truncated or over-long slice of linear memory and reports an
    // unparseable-response error that says nothing about the real cause — the
    // kind of bug that costs an afternoon. Deriving both sides from the file and
    // comparing them makes it a compile-time-ish failure instead.
    let dir = repo_root().join("autumn-harvest/examples/workflow-modules");
    for guest in ["pipeline_v1.wat", "pipeline_v2.wat"] {
        let wat = read_normalized(&dir.join(guest));
        let segments = data_segments(&wat);
        assert!(
            !segments.is_empty(),
            "{guest} declares no data segments — did the parser or the guest change?"
        );
        let packs = pack_calls(&wat);
        assert_eq!(
            packs.len(),
            segments.len(),
            "{guest}: each response segment must be returned by exactly one $pack call"
        );
        for (ptr, len) in packs {
            let (_, actual) = segments
                .iter()
                .find(|(offset, _)| *offset == ptr)
                .copied()
                .unwrap_or_else(|| {
                    panic!("{guest}: $pack returns pointer {ptr}, which is not a data segment")
                });
            assert_eq!(
                len, actual,
                "{guest}: $pack declares {len} bytes at offset {ptr}, but the data \
                 segment there is {actual} bytes"
            );
        }
    }
}

#[test]
fn every_guest_response_is_valid_json_for_the_decide_abi() {
    // The pre-baked responses are hand-typed JSON. A missing quote would only
    // surface as a runtime "response the host cannot parse" from deep inside a
    // workflow; parsing them here names the offending guest instead.
    let dir = repo_root().join("autumn-harvest/examples/workflow-modules");
    for guest in ["pipeline_v1.wat", "pipeline_v2.wat"] {
        let wat = read_normalized(&dir.join(guest));
        for line in wat.lines() {
            let trimmed = line.trim();
            if !trimmed.starts_with("(data (i32.const ") {
                continue;
            }
            let start = trimmed.find('"').expect("opening quote") + 1;
            let end = trimmed.rfind('"').expect("closing quote");
            let decoded = trimmed[start..end].replace("\\\"", "\"");
            let value: serde_json::Value = serde_json::from_str(&decoded)
                .unwrap_or_else(|e| panic!("{guest}: response is not valid JSON ({e}): {decoded}"));
            let kind = value
                .get("kind")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_else(|| panic!("{guest}: response has no `kind`: {decoded}"));
            assert!(
                matches!(kind, "await" | "complete" | "fail"),
                "{guest}: `{kind}` is not a DecideResponse variant"
            );
        }
    }
}
