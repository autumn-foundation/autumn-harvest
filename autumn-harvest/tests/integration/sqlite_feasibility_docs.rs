//! Guards that keep the issue-#966 R&D report honest against the code it audits.
//!
//! `docs/rnd/sqlite-feasibility.md` is a leadership decision document whose
//! central claim is an *exhaustive* module-by-module inventory of every
//! Postgres-coupled surface in core — the issue words it as "verifiable
//! against a `grep`-level audit". A hand-written inventory of 40-odd modules
//! is exactly the kind of artefact that is true the day it is written and
//! quietly false one merge later: a new module lands with a `SKIP LOCKED`
//! claim, nobody re-reads the R&D doc, and the "exhaustive" claim silently
//! becomes a lie that still renders perfectly.
//!
//! These guards make the claim falsifiable. They re-derive the coupling
//! inventory from `autumn-harvest/src/*.rs` at test runtime and assert the
//! report covers it, in both directions:
//!
//!   * every coupled module the detector finds is inventoried (no silent gap),
//!   * every module the report inventories actually exists and is actually
//!     coupled (no phantom rows surviving a rename or a decoupling),
//!   * every inventory row carries an explicit (a)/(b)/(c) classification, so
//!     a module cannot be listed and then left unjudged,
//!   * the per-mechanism counts quoted in the report match live greps, and
//!   * the evidence the report cites (the cross-backend replay test, the
//!     `default-features = false` seam) actually exists.
//!
//! The detector below is deliberately *precise* rather than generous: a bare
//! `INTERVAL` token matches Rust constants such as `DEFAULT_WORKER_POLL_INTERVAL`,
//! and a bare `diesel` matches the guardrail lint's prose listing forbidden
//! crates. Both were false positives in the first cut of this audit. Matching
//! `INTERVAL '`/`make_interval(` and `diesel::`/`diesel_async` keeps the
//! inventory signal rather than noise — an inventory padded with false
//! positives would discredit the report just as badly as one with holes.
//!
//! Deliberately *not* covered: the report's prose judgements — the seam
//! sizing, the cost estimates, the go/no-go reasoning. Those are argument, not
//! fact, and a guard that froze them would stop the document being revisable.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// `<repo>`, i.e. the parent of `CARGO_MANIFEST_DIR` (`<repo>/autumn-harvest`).
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent")
        .to_path_buf()
}

fn report_path() -> PathBuf {
    repo_root().join("docs/rnd/sqlite-feasibility.md")
}

/// Read a file with line endings normalised to `\n`.
///
/// Every guard here does byte-offset arithmetic over the document (section
/// slicing) or exact cell matching. Under a CRLF checkout — the Windows CI leg
/// — `str::lines()` strips the `\r` while the raw text still contains it, so
/// reconstructing offsets from line lengths under-counts one byte per line.
/// On this report that is a ~90-byte shift by the inventory table, and it can
/// land mid-character (the report contains em dashes and middots), panicking
/// on a non-char-boundary slice. Normalising once on read removes the whole
/// class; it mirrors `performance_docs::read_normalized`.
fn read_normalized(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|err| panic!("cannot read {}: {err}", path.display()))
        .replace("\r\n", "\n")
}

fn read_report() -> String {
    let path = report_path();
    let body = std::fs::read_to_string(&path).unwrap_or_else(|err| {
        panic!(
            "issue #966 deliverable 1 is missing: could not read {}: {err}\n\
             The R&D report is the primary deliverable of the spike — the success \
             metric requires leadership to green-light or kill the direction from \
             the report alone, without reading the spike code.",
            path.display()
        )
    });
    body.replace("\r\n", "\n")
}

/// Every coupling mechanism the issue names, with the precise token that
/// identifies real usage (see the module doc on why these are not bare words).
const MECHANISMS: &[(&str, &[&str])] = &[
    (
        "diesel",
        &["diesel::", "diesel_async", "diesel_migrations", "#[diesel("],
    ),
    ("skip-locked", &["SKIP LOCKED"]),
    // Blocking pessimistic row lock, distinct from the non-blocking claim
    // above. Detected through the Diesel DSL rather than the raw `FOR UPDATE`
    // text: the codebase has no `.skip_locked()` DSL call, so `.for_update()`
    // is unambiguously a blocking lock, and the DSL set is a superset of the
    // modules issuing real raw-SQL row locks (`queue`, `timeout` — both also
    // use the DSL). Matching the raw text instead would be actively wrong: it
    // flags `mutex` for a doc comment reading "No `FOR UPDATE`" and for a unit
    // test asserting `!expr.contains("FOR UPDATE")`, and it cannot separate
    // `FOR UPDATE OF t SKIP LOCKED` from a blocking lock.
    ("row-lock", &[".for_update()"]),
    // Postgres-only raw SQL that Diesel does not abstract: a module can look
    // like plain CRUD through the ORM and still be unportable because of the
    // SQL it embeds. Every token here is genuinely Postgres-only:
    //   `#>>`            JSONB path extract (SQLite JSON1 has no equivalent operator)
    //   `EXTRACT(EPOCH`  (SQLite uses `strftime('%s', …)`)
    //   `JOIN LATERAL`   (SQLite has no LATERAL)
    //   `~ '`            POSIX regex (SQLite has no REGEXP without a UDF)
    //   `::TYPE`         Postgres cast syntax (SQLite uses `CAST(x AS t)`)
    // `JOIN LATERAL` rather than bare `LATERAL` for the same reason the row
    // lock is matched through the DSL: `queue` and `execution` both *talk
    // about* `LATERAL` in doc comments, and a module must not be reported as
    // coupled because it documents a construct.
    //
    // On the cast tokens: an earlier revision listed only UPPERCASE type names
    // and so missed every lowercase spelling of the *same* construct
    // (`'[]'::jsonb`, `$1::bigint`, `payloads::text`). That is a different
    // defect from the open-enumeration problem below — case is a *bounded*
    // dimension, so it is closable, and it is closed here.
    //
    // The first two tokens close it structurally rather than by listing types:
    // a Postgres cast is `<expr>::<type>`, and `'::` / `)::` match on the
    // *cast position* (after a literal or a closing paren) regardless of the
    // type named or its case. Neither is valid Rust syntax, so both are
    // SQL-only by construction. The remaining lowercase type names cover the
    // `$1::bigint` and `ident::text` forms, which have no such anchor.
    //
    // Deliberately NOT matched, because each has a real Rust false positive:
    // bare `::` (every path — `Self::Variant`, `std::fmt`); `::uuid` (the
    // re-exported `autumn_harvest::uuid`, named in a doc comment and in
    // `lib.rs`); `::interval` (`tokio::time::interval`). Case-insensitive
    // matching is likewise rejected: it would flag `diesel::sql_types::Text`
    // and its siblings as Postgres casts.
    (
        "raw-pg-sql",
        &[
            "#>>",
            "@>",
            "EXTRACT(EPOCH",
            "JOIN LATERAL",
            "~ '",
            // Cast position — case- and type-agnostic, SQL-only by syntax.
            "'::",
            ")::",
            // Explicit spellings for casts with no positional anchor.
            "::TEXT",
            "::BIGINT",
            "::INT4",
            "::NUMERIC",
            "::UUID",
            "::TIMESTAMPTZ",
            "::DOUBLE PRECISION",
            "::text",
            "::jsonb",
            "::int8",
            "::bigint",
        ],
    ),
    // Reaches for Diesel's raw-SQL escape hatch at all.
    //
    // This is the *closed* rule, and it exists because the list above is not
    // one. Three review rounds each found another dialect construct the token
    // list missed (the blocking row lock, then JSONB/`LATERAL`/regex, then
    // JSONB `@>`/`||` and `NOW()`), which is the tell that enumerating dialect
    // features cannot terminate: the set of Postgres-only syntax is open, and
    // every miss silently understates the port.
    //
    // Detecting the escape hatch instead cannot miss a construct, because it
    // does not enumerate constructs. It is a weaker claim — raw SQL *may* be
    // portable ANSI — so it is deliberately not evidence of coupling on its
    // own. What it does establish is that the module's portability cannot be
    // read off its Diesel usage, which is precisely what class (a) asserts.
    // `raw_sql_modules_are_never_classified_trivially_traitable` enforces that.
    (
        "raw-sql",
        &[
            "sql::<",
            "sql_query(",
            "define_sql_function",
            "sql_function!",
        ],
    ),
    ("listen/notify", &["pg_notify", "LISTEN "]),
    // Both prefixes are required and, together, are exhaustive: every one of
    // Postgres's ten advisory-lock functions is named `pg_advisory_*` or
    // `pg_try_advisory_*` (session/transaction x shared/exclusive x try/block,
    // plus the two unlocks). An earlier revision matched only `pg_advisory`,
    // which does not substring-match `pg_try_advisory_xact_lock` — `try_` is
    // infixed — and so missed the concurrency-cap claim in `queue` and the
    // fleet-wide registration lock in `scheduler`.
    //
    // Unlike the dialect list above, this family is *closed*: it is a fixed,
    // documented set of Postgres functions, so a prefix pair can cover it
    // exhaustively rather than approximately.
    ("advisory-lock", &["pg_advisory", "pg_try_advisory"]),
    ("to_regclass", &["to_regclass"]),
    ("gen_random_uuid", &["gen_random_uuid"]),
    (
        "interval-sql",
        &["INTERVAL '", "make_interval(", "sql_types::Interval"],
    ),
];

/// Modules deliberately excluded from the core Postgres-coupling audit.
///
/// This report sizes the cost of porting the *engine* to an embedded `SQLite`
/// backend, so "core module" means a production engine surface a port would
/// have to carry. `chaos` is the issue-#940 deterministic fault-injection
/// **test harness**: its `points` half is a zero-cost const catalogue with no
/// Postgres coupling, and its only `diesel::`/`pg_notify` tokens live entirely
/// in the `#[cfg(feature = "chaos")]` controller — a `From<ChaosError> for
/// diesel::result::Error` conversion that *simulates* DB errors at injection
/// points — plus one doc comment. It is never compiled into a production or
/// default build, is never ported to `SQLite`, and is not an engine surface this
/// report exists to judge. Auditing it would list a test scaffold among the
/// modules leadership must decide to port — the opposite of the report's
/// signal. So it is skipped here, exactly like the `bin` directory below, as an
/// *explicit, documented* decision rather than a silent hole (the property
/// `excluded_modules_all_exist` keeps the list from rotting past a rename).
const AUDIT_EXCLUDED_MODULES: &[&str] = &["chaos"];

/// Every auditable core `.rs` module directly under `autumn-harvest/src`, as
/// `(module name, path)`.
///
/// The single source of truth for "core module", shared by the coupling
/// detector and the derived-totals denominator so the two can never disagree
/// about what is in scope. It asserts the tree is still flat (a nested module
/// could carry coupling the inventory never sees) and applies the documented
/// test-only exclusions above.
fn core_module_files() -> Vec<(String, PathBuf)> {
    let src = repo_root().join("autumn-harvest/src");
    let entries = std::fs::read_dir(&src)
        .unwrap_or_else(|err| panic!("cannot read {}: {err}", src.display()));

    let mut files: Vec<(String, PathBuf)> = Vec::new();
    let mut subdirs: Vec<String> = Vec::new();
    for entry in entries {
        let path = entry.expect("readable dir entry").path();
        if path.is_dir() {
            // The scan is deliberately flat: core is a flat module tree, and a
            // flat scan keeps module names unambiguous (`foo.rs` -> `foo`).
            // But a *silently* flat scan would let a nested module carry
            // coupling the inventory never sees, which is the exact rot these
            // guards exist to prevent — so a new subdirectory has to be an
            // explicit decision, not an accident.
            let name = path
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or_default()
                .to_string();
            if name != "bin" {
                subdirs.push(name);
            }
            continue;
        }
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let module = path
            .file_stem()
            .and_then(|s| s.to_str())
            .expect("module file stem")
            .to_string();
        // Documented test-only exclusions — see AUDIT_EXCLUDED_MODULES.
        if AUDIT_EXCLUDED_MODULES.contains(&module.as_str()) {
            continue;
        }
        files.push((module, path));
    }
    assert!(
        subdirs.is_empty(),
        "`autumn-harvest/src` gained subdirectory module(s) {subdirs:?}, which this \
         flat scan does not descend into. A nested module could carry Postgres \
         coupling the inventory never sees — exactly the rot these guards exist to \
         catch. Either make the scan recursive (and decide how nested modules are \
         named in the inventory) or add the directory to the skip list with a \
         reason."
    );
    files
}

/// Re-derives the Postgres-coupling inventory from the core crate's sources.
///
/// Returns `module name -> sorted set of mechanisms`, for every module that
/// exhibits at least one. This is the ground truth the report is checked
/// against; it is computed fresh on every run so the audit cannot go stale.
fn detect_coupled_modules() -> BTreeMap<String, BTreeSet<&'static str>> {
    let mut found: BTreeMap<String, BTreeSet<&'static str>> = BTreeMap::new();
    for (module, path) in core_module_files() {
        let body = read_normalized(&path);

        let mut mechanisms = BTreeSet::new();
        for (label, tokens) in MECHANISMS {
            if tokens.iter().any(|token| body.contains(token)) {
                mechanisms.insert(*label);
            }
        }
        if !mechanisms.is_empty() {
            found.insert(module, mechanisms);
        }
    }
    assert!(
        !found.is_empty(),
        "detector found no Postgres-coupled modules at all — the detector is \
         broken (or the source layout moved), not the report"
    );
    found
}

/// One parsed inventory row: `| `module` | coupling | class | note |`.
struct InventoryRow {
    /// The mechanisms named in the coupling cell, as a set.
    coupling: BTreeSet<String>,
    /// The classification cell, verbatim and trimmed (expected `(a)`/`(b)`/`(c)`).
    class: String,
    /// The whole row, for error messages.
    raw: String,
}

/// The report's inventory rows: Markdown table lines whose first cell is a
/// backticked module name.
///
/// Parses *by cell index* rather than searching the whole row. Whole-row
/// substring matching would let the coupling cell say `diesel` while the note
/// cell happened to contain the word "skip-locked", or let a note reading
/// "not (c)" satisfy a classification check — both pass a containment test and
/// both mean the table is lying.
fn inventory_rows(report: &str) -> BTreeMap<String, InventoryRow> {
    let section = markdown_section(report, "Postgres-coupling inventory").unwrap_or_else(|| {
        panic!(
            "the report must contain a section headed \"Postgres-coupling inventory\" \
             holding the module-by-module table"
        )
    });

    let mut rows: BTreeMap<String, InventoryRow> = BTreeMap::new();
    for line in section.lines() {
        let trimmed = line.trim();
        if !trimmed.starts_with('|') {
            continue;
        }
        // A leading `|` makes `split` yield an empty first element, so the
        // table's cells start at index 1.
        let cells: Vec<&str> = trimmed.split('|').map(str::trim).collect();
        let Some(first_cell) = cells.get(1) else {
            continue;
        };
        // A module cell is exactly a backticked identifier: `queue`.
        let Some(name) = first_cell
            .strip_prefix('`')
            .and_then(|c| c.strip_suffix('`'))
        else {
            continue;
        };
        if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            continue;
        }
        let coupling = cells
            .get(2)
            .unwrap_or(&"")
            .split(',')
            .map(str::trim)
            .filter(|m| !m.is_empty())
            .map(str::to_string)
            .collect();
        let row = InventoryRow {
            coupling,
            class: (*cells.get(3).unwrap_or(&"")).to_string(),
            raw: trimmed.to_string(),
        };
        if let Some(previous) = rows.insert(name.to_string(), row) {
            panic!(
                "the inventory lists `{name}` twice. A duplicate row means one of \
                 the two classifications is invisible to a reader (and to these \
                 guards, which key on module name).\nFirst:  {}\nSecond: {trimmed}",
                previous.raw
            );
        }
    }
    rows
}

/// Extracts the body of a `##`/`###` section by heading substring, up to the
/// next heading of the same-or-shallower depth.
///
/// Offsets come from `split_inclusive('\n')`, which keeps the terminator in the
/// slice, so the running offset is exact for any line ending rather than
/// reconstructed as `line.len() + 1` (which is off by one per line under CRLF
/// and can slice mid-character). A `#` inside a fenced code block is not a
/// heading.
fn markdown_section<'a>(document: &'a str, heading_contains: &str) -> Option<&'a str> {
    let mut start: Option<usize> = None;
    let mut depth = 0usize;
    let mut offset = 0usize;
    let mut in_fence = false;
    for raw in document.split_inclusive('\n') {
        let line = raw.trim_end_matches(['\n', '\r']);
        if line.trim_start().starts_with("```") {
            in_fence = !in_fence;
        } else if !in_fence && line.starts_with('#') {
            let this_depth = line.chars().take_while(|c| *c == '#').count();
            match start {
                // A heading at the same or shallower depth closes the section.
                Some(begin) if this_depth <= depth => return Some(&document[begin..offset]),
                None if line.contains(heading_contains) => {
                    start = Some(offset);
                    depth = this_depth;
                }
                // Deeper heading inside the section, or a heading before it starts.
                Some(_) | None => {}
            }
        }
        offset += raw.len();
    }
    // Unterminated section: runs to end of document.
    start.map(|begin| &document[begin..])
}

#[test]
fn report_exists_and_is_decision_ready() {
    let report = read_report();
    // The sections leadership needs in order to decide without reading code.
    for heading in [
        "Decision",
        "Postgres-coupling inventory",
        "StorageBackend",
        "Cost to the Postgres path",
        "Test-suite portability",
        "Recommendation",
        "hub",
        "Timebox",
        "embedded backends",
    ] {
        assert!(
            markdown_section(&report, heading).is_some(),
            "issue #966 requires the report to cover {heading:?}, but no heading \
             contains that text.\nHeadings present:\n{}",
            report
                .lines()
                .filter(|l| l.starts_with('#'))
                .collect::<Vec<_>>()
                .join("\n")
        );
    }
}

/// The audit's own exceptions must be falsifiable.
///
/// A skip list is only honest while every entry names a module that actually
/// exists — otherwise a rename or deletion leaves a stale exclusion that could
/// silently swallow a *different*, real module later given the same file name.
/// This mirrors `inventory_lists_no_module_that_is_not_actually_coupled`, which
/// forbids the report itself from carrying phantom rows.
#[test]
fn excluded_modules_all_exist() {
    let src = repo_root().join("autumn-harvest/src");
    for excluded in AUDIT_EXCLUDED_MODULES {
        let path = src.join(format!("{excluded}.rs"));
        assert!(
            path.exists(),
            "`AUDIT_EXCLUDED_MODULES` skips `{excluded}`, but {} does not exist. \
             A stale exclusion is a silent hole — remove the entry, or the audit \
             could later miss a real module that reuses the name.",
            path.display()
        );
    }
}

#[test]
fn inventory_covers_every_postgres_coupled_core_module() {
    let report = read_report();
    let detected = detect_coupled_modules();
    let rows = inventory_rows(&report);

    let missing: Vec<String> = detected
        .iter()
        .filter(|(module, _)| !rows.contains_key(*module))
        .map(|(module, mechanisms)| {
            format!(
                "| `{module}` | {} | (?) | |",
                mechanisms.iter().copied().collect::<Vec<_>>().join(", ")
            )
        })
        .collect();

    assert!(
        missing.is_empty(),
        "the report's Postgres-coupling inventory claims to be exhaustive but \
         omits {} module(s). Issue #966 requires an inventory \"verifiable against \
         a grep-level audit\".\nAdd these rows (classification still to fill in):\n{}",
        missing.len(),
        missing.join("\n")
    );
}

#[test]
fn inventory_lists_no_module_that_is_not_actually_coupled() {
    let report = read_report();
    let detected = detect_coupled_modules();
    let rows = inventory_rows(&report);

    let phantom: Vec<&String> = rows
        .keys()
        .filter(|module| !detected.contains_key(*module))
        .collect();

    assert!(
        phantom.is_empty(),
        "the inventory lists {} module(s) that no longer exist or are no longer \
         Postgres-coupled: {phantom:?}\nA stale row is as misleading as a missing \
         one — delete them, or the audit overstates the coupling.",
        phantom.len()
    );
}

#[test]
fn every_inventoried_module_carries_an_abc_classification() {
    let report = read_report();
    let rows = inventory_rows(&report);
    assert!(
        !rows.is_empty(),
        "the inventory table parsed to zero rows — the table format changed and \
         these guards are no longer reading it"
    );

    // The classification *cell*, matched exactly. A whole-row `contains("(a)")`
    // would be satisfied by a note cell reading "not (a)" — the opposite claim.
    let unjudged: Vec<String> = rows
        .iter()
        .filter(|(_, row)| !matches!(row.class.as_str(), "(a)" | "(b)" | "(c)"))
        .map(|(module, row)| format!("`{module}` (class cell: {:?})", row.class))
        .collect();

    assert!(
        unjudged.is_empty(),
        "issue #966 requires each coupled surface to be classified (a) trivially \
         trait-able, (b) needs a semantic substitute, or (c) fundamentally \
         Postgres-shaped, in the third table cell. These rows do not:\n{}",
        unjudged.join("\n")
    );
}

#[test]
fn every_inventoried_module_records_the_mechanisms_grep_finds() {
    let report = read_report();
    let detected = detect_coupled_modules();
    let rows = inventory_rows(&report);

    let mut wrong = Vec::new();
    for (module, mechanisms) in &detected {
        let Some(row) = rows.get(module) else {
            continue; // covered by the exhaustiveness guard
        };
        // Set *equality*, not containment, and against the coupling cell only.
        // Containment would let a row over-claim (naming a mechanism the module
        // does not use, overstating the port cost) as freely as under-claim.
        let expected: BTreeSet<String> = mechanisms.iter().map(|m| (*m).to_string()).collect();
        if row.coupling != expected {
            let missing: Vec<&String> = expected.difference(&row.coupling).collect();
            let extra: Vec<&String> = row.coupling.difference(&expected).collect();
            wrong.push(format!(
                "`{module}`: row says {:?}, audit finds {:?}{}{}",
                row.coupling,
                expected,
                if missing.is_empty() {
                    String::new()
                } else {
                    format!(" — missing {missing:?}")
                },
                if extra.is_empty() {
                    String::new()
                } else {
                    format!(" — over-claims {extra:?}")
                },
            ));
        }
    }

    assert!(
        wrong.is_empty(),
        "each inventory row's coupling cell must name exactly the mechanisms the \
         audit detects for that module, so a reader can see *why* it is coupled \
         and cannot be misled about how much:\n{}",
        wrong.join("\n")
    );
}

#[test]
fn mechanism_counts_quoted_in_the_report_are_current() {
    let report = read_report();
    let detected = detect_coupled_modules();

    let section = markdown_section(&report, "Coupling mechanisms")
        .unwrap_or_else(|| panic!("the report must contain a \"Coupling mechanisms\" section"));

    // Per-mechanism module counts, recomputed live. The report publishes these
    // as `| <mechanism> | <n> modules | ... |`; a stale number here is the
    // classic way an audit rots without anyone noticing.
    for (label, _) in MECHANISMS {
        let count = detected.values().filter(|ms| ms.contains(label)).count();
        // Only table rows: prose elsewhere in the section may legitimately
        // mention a mechanism without publishing its count.
        let row = section
            .lines()
            .map(str::trim)
            .filter(|l| l.starts_with('|'))
            .find(|l| l.contains(label))
            .unwrap_or_else(|| {
                panic!("no table row in \"Coupling mechanisms\" mentions {label:?}")
            });
        // Parse the reach cell as a *number*, not a substring: `contains("13
        // modules")` is satisfied by "113 modules", so a count that grew past a
        // decimal boundary would silently keep passing.
        let reach = row.split('|').nth(2).unwrap_or_default().trim();
        let published: usize = reach
            .split_whitespace()
            .next()
            .and_then(|n| n.parse().ok())
            .unwrap_or_else(|| {
                panic!(
                    "the reach cell for {label:?} must start with a module count; \
                     found {reach:?} in row:\n  {row}"
                )
            });
        assert_eq!(
            published, count,
            "the report says {published} for {label:?}:\n  {row}\nbut a live audit \
             finds {count}."
        );
        // Singular/plural so the guard never forces ungrammatical prose ("1 modules").
        let unit = if count == 1 { "module" } else { "modules" };
        assert!(
            reach.contains(unit),
            "the reach cell for {label:?} should read \"{count} {unit}\"; found {reach:?}"
        );
    }

    let total = detected.len();
    assert!(
        report.contains(&format!("**{total} of the")),
        "the report should state the headline coupling figure as \"**{total} of the \
         N core modules**\"; a live audit currently finds {total} coupled modules"
    );
}

/// A module that hand-writes SQL cannot be "plain CRUD through Diesel".
///
/// This is the guard that ends the whack-a-mole. Class (a) is *defined* in the
/// report as "plain query/CRUD through Diesel with no Postgres-specific
/// semantics" — a claim that is false by construction the moment a module
/// reaches for `sql::<…>` or `sql_query`, because that SQL is not going
/// through Diesel's abstraction at all and its portability has to be read by a
/// human.
///
/// Unlike the `raw-pg-sql` token list, this rule is **closed**: it cannot miss
/// a dialect construct, because it does not enumerate constructs. Three review
/// rounds each found one the token list had missed; this rule independently
/// catches all three classes at once, and would have caught them the first
/// time.
///
/// It deliberately does *not* force a class — raw SQL may be portable ANSI, so
/// (b) is often right. It only forbids the one classification that asserts the
/// module was never hand-written in the first place.
#[test]
fn raw_sql_modules_are_never_classified_trivially_traitable() {
    let report = read_report();
    let detected = detect_coupled_modules();
    let rows = inventory_rows(&report);

    let mislabelled: Vec<&String> = detected
        .iter()
        .filter(|(_, mechanisms)| mechanisms.contains("raw-sql"))
        .filter_map(|(module, _)| rows.get(module).map(|row| (module, row)))
        .filter(|(_, row)| row.class == "(a)")
        .map(|(module, _)| module)
        .collect();

    assert!(
        mislabelled.is_empty(),
        "these modules hand-write SQL through Diesel's escape hatch, so they \
         cannot be class (a) \"plain query/CRUD through Diesel with no \
         Postgres-specific semantics\" — that classification asserts the SQL \
         was never hand-written: {mislabelled:?}\n\
         Read the raw SQL and classify (b) if it is a mechanical rewrite on \
         SQLite, or (c) if porting it drops a capability."
    );
}

/// The report's derived totals must agree with its own table and with the tree.
///
/// The per-mechanism counts are guarded above, but the *derived* figures are
/// the ones a reader actually quotes in a decision: "43 of 93 modules", the
/// (a)/(b)/(c) split, "83 migrations". Each is arithmetic over something that
/// changes without anyone re-reading this document.
#[test]
fn derived_totals_agree_with_the_table_and_the_tree() {
    let report = read_report();
    let rows = inventory_rows(&report);

    // (a)/(b)/(c) split, recomputed from the table the report itself publishes.
    let mut by_class: BTreeMap<&str, usize> = BTreeMap::new();
    for row in rows.values() {
        *by_class.entry(row.class.as_str()).or_default() += 1;
    }
    let (a, b, c) = (
        by_class.get("(a)").copied().unwrap_or_default(),
        by_class.get("(b)").copied().unwrap_or_default(),
        by_class.get("(c)").copied().unwrap_or_default(),
    );
    let totals = format!("**Totals: (a) {a} · (b) {b} · (c) {c}.**");
    assert!(
        report.contains(&totals),
        "the classification totals must match the table. Expected the line:\n  \
         {totals}\nRows counted: {by_class:?} across {} rows.",
        rows.len()
    );
    assert_eq!(
        a + b + c,
        rows.len(),
        "every inventory row must fall in exactly one class; {} rows but \
         {a}+{b}+{c} classified",
        rows.len()
    );

    // Denominator: total core modules, counted live from the *same* source of
    // truth as the coupling detector, so the two can never disagree about scope
    // and the documented test-only exclusions are applied consistently.
    let total_modules = core_module_files().len();
    let headline = format!("**{} of the {total_modules} core modules**", rows.len());
    assert!(
        report.contains(&headline),
        "the headline coupling ratio must be current. Expected:\n  {headline}\n\
         (a live count finds {} coupled of {total_modules} core modules)",
        rows.len()
    );

    // The same figures again, as the *prose* quotes them.
    //
    // The table above is guarded, but a reader making a decision quotes the
    // sentences, not the table — "19 of 43 coupled modules are portable only by
    // dropping a capability" is the line that gets pasted into a design review.
    // Those sentences drifted: they were a coherent snapshot of an earlier audit
    // (43 coupled, 19 class (c), 26 raw-SQL) left behind by a table that had
    // since grown, so the document contradicted itself in the one place it is
    // most likely to be quoted from. Each figure below is pure arithmetic over
    // the inventory rows, so it is derivable and therefore guardable; the
    // estimates that are *not* derivable (`~13 modules` for the scanner rewrite)
    // are deliberately left alone.
    let raw_sql = detect_coupled_modules()
        .values()
        .filter(|ms| ms.contains("raw-sql"))
        .count();
    for phrase in [
        format!("{c} of {} coupled modules", rows.len()),
        format!(
            "That {raw_sql} of the {} coupled modules hand-write SQL",
            rows.len()
        ),
        format!("~{} modules touched", rows.len()),
        format!("**{c} modules are class (c)**"),
        format!("against {a} that are trivially trait-able"),
        format!("**{raw_sql} modules reach for raw SQL**"),
    ] {
        assert!(
            report.contains(&phrase),
            "the report's prose must quote the same figures as its own table; \
             expected to find {phrase:?}. A live audit finds {} coupled modules, \
             {a} class (a), {c} class (c), {raw_sql} reaching for raw SQL.",
            rows.len()
        );
    }

    // Migration count.
    let migrations = std::fs::read_dir(repo_root().join("autumn-harvest/migrations"))
        .expect("migrations dir readable")
        .filter_map(Result::ok)
        .filter(|e| e.path().is_dir())
        .count();
    assert!(
        report.contains(&format!("**{migrations} migrations**")),
        "the report should state \"**{migrations} migrations**\"; a live count \
         finds {migrations} migration directories"
    );
}

#[test]
fn cited_cross_backend_replay_evidence_actually_exists() {
    let report = read_report();
    let cross_backend =
        repo_root().join("autumn-harvest-sqlite/tests/integration/cross_backend.rs");
    let body = std::fs::read_to_string(&cross_backend).unwrap_or_else(|err| {
        panic!(
            "issue #966 requires one cross-backend replay test proving event-log \
             portability; cannot read {}: {err}",
            cross_backend.display()
        )
    });

    // The report must cite a test by name, and that test must exist. Citing
    // evidence that does not exist is the failure mode this guards.
    assert!(
        report.contains("sqlite_history_replays_on_core_replayer"),
        "the report must cite the cross-backend replay test by name so a reader \
         can verify the portability invariant themselves"
    );
    assert!(
        body.contains("fn sqlite_history_replays_on_core_replayer"),
        "the report cites `sqlite_history_replays_on_core_replayer`, but no such \
         test exists in {}",
        cross_backend.display()
    );
}

#[test]
fn zero_regression_to_the_postgres_path_is_structurally_true() {
    // The spike's first invariant: saying "no" stays free, and the Postgres
    // build is untouched. Both halves are checkable from the manifests rather
    // than asserted in prose.
    // Compare against *directives*, not raw text: a comment explaining why core
    // has no rusqlite dependency would fail a raw `contains`, and a comment
    // mentioning `default-features = false` would satisfy one.
    let core_directives = manifest_directives(&repo_root().join("autumn-harvest/Cargo.toml"));
    assert!(
        !core_directives.iter().any(|l| l.contains("rusqlite")),
        "core must not depend on rusqlite — the SQLite backend is a downstream \
         companion crate, never a core dependency"
    );
    assert!(
        !core_directives
            .iter()
            .any(|l| l.contains("autumn-harvest-sqlite")),
        "core must not depend on the SQLite crate; the dependency edge points \
         the other way"
    );

    let sqlite_directives =
        manifest_directives(&repo_root().join("autumn-harvest-sqlite/Cargo.toml"));
    let core_dep = sqlite_directives
        .iter()
        .find(|l| l.starts_with("autumn-harvest") && !l.starts_with("autumn-harvest-"))
        .unwrap_or_else(|| {
            panic!(
                "the SQLite crate must declare a dependency on core; no \
                 `autumn-harvest = ...` directive found"
            )
        });
    assert!(
        core_dep.contains("default-features = false"),
        "the SQLite crate must depend on core with `default-features = false` so \
         it pulls the determinism core without Diesel/Postgres. Found:\n  {core_dep}"
    );
}

/// `markdown_section` must be line-ending agnostic.
///
/// This is pinned directly rather than left to the report's own content,
/// because the failure mode is *content-dependent* and therefore an unreliable
/// canary. The original implementation reconstructed byte offsets as
/// `line.len() + 1` from `str::lines()`, which strips `\r` — so under a CRLF
/// checkout the running offset drifts one byte per line. The drift only
/// *panics* when it happens to land inside a multi-byte character, and only
/// silently mis-slices otherwise.
///
/// That is exactly how it escaped review: the guards passed locally against a
/// CRLF copy of the working-tree report (the drift landed on a char boundary)
/// and panicked on the Windows CI leg against the committed one, at
/// `byte index 14684 ... inside '→'`. A one-line edit anywhere above an em dash
/// flips which of those happens. So the invariant is asserted on a fixture with
/// a known multi-byte character rather than on the live document.
#[test]
fn markdown_section_is_line_ending_agnostic() {
    let lf = "# Title\n\nIntro — with an em dash.\n\n## Wanted\n\nBody → arrow.\n\n## Next\n\nElsewhere.\n";
    let crlf = lf.replace('\n', "\r\n");

    let from_lf = markdown_section(lf, "Wanted").expect("LF section found");
    let from_crlf = markdown_section(&crlf, "Wanted").expect("CRLF section found");

    // Same content modulo line endings, and — the part that panicked — the
    // CRLF slice must be a valid, complete section rather than a shifted one.
    assert_eq!(
        from_crlf.replace("\r\n", "\n"),
        from_lf,
        "the section extracted from a CRLF document must match the LF one"
    );
    assert!(
        from_crlf.starts_with("## Wanted"),
        "CRLF section must start at its heading, not a shifted offset: {from_crlf:?}"
    );
    assert!(
        from_crlf.contains("Body → arrow."),
        "CRLF section must contain its body intact: {from_crlf:?}"
    );
    assert!(
        !from_crlf.contains("Elsewhere"),
        "the section must stop at the next same-depth heading: {from_crlf:?}"
    );
}

/// A `#` inside a fenced code block is not a heading.
///
/// The report is free to include shell snippets, and `# comment` is the most
/// ordinary line in one. Treating it as a heading would silently truncate
/// whichever section contained it.
#[test]
fn markdown_section_ignores_headings_inside_code_fences() {
    let doc = "## Wanted\n\n```bash\n# not a heading\ngrep -r foo\n```\n\nStill in the section.\n\n## Next\n\nElsewhere.\n";
    let section = markdown_section(doc, "Wanted").expect("section found");
    assert!(
        section.contains("Still in the section."),
        "a `#` comment inside a code fence must not close the section: {section:?}"
    );
    assert!(
        !section.contains("Elsewhere"),
        "the real next heading must still close the section: {section:?}"
    );
}

/// These guards must actually execute on a docs-only pull request.
///
/// The change most likely to rot this report is a pull request that edits
/// *only* `docs/rnd/sqlite-feasibility.md` — and that is precisely the change
/// CI would skip: the `changes` job classifies `docs/**` and `*.md` as
/// `code=false`, and every step of the `test` matrix is gated on
/// `code == 'true'`. The `lint` job runs ungated, but Clippy only *compiles*
/// this file.
///
/// So the guards need a `cargo test` step in the ungated `lint` job. This
/// asserts that step is there and has not acquired a condition that would put
/// it back behind the same gate. It mirrors
/// `performance_docs::performance_guards_run_on_docs_only_changes`, which
/// exists because that suite spent three rounds unable to fire on the change
/// class it was written for.
/// The **whole** workflow step stanza containing `needle`, from its `- name:`
/// through to the line before the next step's `- name:` (or the end of the
/// block).
///
/// Bounding at the *next* step rather than at `needle` is the load-bearing
/// part. An earlier revision sliced only up to the matched text, which sits
/// inside the step's `run:` line — so every key written *after* `run:` was
/// invisible. GitHub Actions does not care about key order within a step, so
/// moving an `if:` below the `run:` line was enough to re-gate the guards while
/// the test that exists to prevent exactly that kept passing. A guard that a
/// two-line reordering can evade is worse than no guard, because it also
/// reports success.
fn workflow_step_stanza<'a>(block: &'a str, needle: &str) -> Option<&'a str> {
    const STEP: &str = "\n      - name:";
    let at = block.find(needle)?;
    let start = block[..at].rfind(STEP).unwrap_or(0);
    // Search for the next step from just past this stanza's own `- name:`, so
    // the marker we started from is not rediscovered as the terminator.
    let after_marker = start + STEP.len();
    let end = block[after_marker..]
        .find(STEP)
        .map_or(block.len(), |rel| after_marker + rel);
    Some(&block[start..end])
}

#[test]
fn step_stanza_covers_keys_written_after_run() {
    // `if:` placed *below* `run:` — valid YAML, identical semantics to placing
    // it above, and invisible to a slice that stops at the `run:` line.
    let block = "\n      - name: Some earlier step\n        run: echo earlier\n\
                 \n      - name: Guard step\n        run: cargo test GUARD_FILTER\n\
                 \n        if: needs.changes.outputs.code == 'true'\n\
                 \n      - name: A later step\n        run: echo later\n";

    let stanza = workflow_step_stanza(block, "GUARD_FILTER").expect("stanza is present");

    assert!(
        stanza.contains("\n        if:"),
        "the stanza must extend past `run:` to the next step, or an `if:` \
         written below `run:` re-gates the guards undetected. Stanza:\n{stanza}"
    );
    assert!(
        !stanza.contains("A later step"),
        "the stanza must stop at the next step, not swallow it — otherwise an \
         unrelated neighbour's `if:` would raise a false alarm. Stanza:\n{stanza}"
    );
    assert!(
        !stanza.contains("Some earlier step"),
        "the stanza must start at its own `- name:`, not an earlier step's. \
         Stanza:\n{stanza}"
    );
}

#[test]
fn guards_run_on_docs_only_changes() {
    const FILTER: &str = "--test integration sqlite_feasibility_docs::";
    let workflow = read_normalized(&repo_root().join(".github/workflows/ci.yml"));

    // The step must exist, and must name this module as its filter — a step
    // running some *other* test satisfies a looser check while leaving these
    // guards just as unexecuted.
    let step = workflow
        .lines()
        .find(|line| line.contains(FILTER))
        .unwrap_or_else(|| {
            panic!(
                "ci.yml must run the sqlite_feasibility_docs guards from a step that \
                 is not gated on `changes.outputs.code`, or a docs-only PR — the \
                 change class these guards exist for — skips them entirely"
            )
        });
    assert!(
        step.trim_start().starts_with("run:"),
        "expected the guard invocation to be a step `run:` line, found: {step}"
    );

    // It must live in `lint`, the ungated job. `test` is gated per-step on
    // `changes.outputs.code`, so a step there proves nothing for docs-only PRs.
    let lint_start = workflow
        .find("\n  lint:")
        .expect("ci.yml must define a `lint` job");
    let test_start = workflow
        .find("\n  test:")
        .expect("ci.yml must define a `test` job");
    let step_at = workflow.find(FILTER).expect("located above");
    assert!(
        step_at > lint_start && step_at < test_start,
        "the sqlite_feasibility_docs guard step must live in the ungated `lint` \
         job; a step in the `test` matrix is gated on `changes.outputs.code` and \
         so does not run on a docs-only PR"
    );

    // And it must be unconditional. A step that grew an `if:` is back behind a
    // gate — which is the exact regression this test exists to prevent.
    let block = &workflow[lint_start..test_start];
    let stanza = workflow_step_stanza(block, FILTER)
        .expect("the guard step is inside the lint block, located above");
    assert!(
        !stanza.contains("\n        if:"),
        "the sqlite_feasibility_docs guard step has acquired an `if:` condition. \
         It must run unconditionally: a condition is how guards like these stopped \
         running on docs-only PRs in the first place. Stanza:\n{stanza}"
    );
}

/// Manifest lines with comments and blanks stripped, so guards assert on what
/// Cargo reads rather than on prose that happens to sit in the file.
fn manifest_directives(path: &Path) -> Vec<String> {
    read_normalized(path)
        .lines()
        .map(|line| line.split_once('#').map_or(line, |(code, _)| code).trim())
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect()
}
