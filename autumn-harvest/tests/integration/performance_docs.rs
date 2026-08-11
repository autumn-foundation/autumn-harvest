//! Guards that keep `docs/performance.md`'s prose tied to its own tables.
//!
//! That page is the published baseline for attributing claim-path costs, and
//! its narrative quotes percentages, sample counts and multipliers that are
//! supposed to be read straight off the tables above them. Twice now the
//! tables were regenerated from a fresh benchmark run while the surrounding
//! prose kept the previous run's figures — the second time badly enough that
//! two scenarios swapped which was slower. Nothing caught either drift,
//! because a stale number in prose still renders perfectly.
//!
//! These tests make the tables authoritative. They parse the published tables
//! out of the Markdown and assert that (a) each table's own percentage column
//! is arithmetically consistent with its own latency column, and (b) every
//! per-gate bullet quotes the figure the table publishes for that gate.
//!
//! Deliberately *not* covered: the reproducibility paragraph and the
//! "earlier revision" narrative, which quote other runs on purpose and say so.
//! A guard that forbade those would force the page to hide the run-to-run
//! spread, which is the most operationally honest thing on it.

use std::path::{Path, PathBuf};

fn performance_doc_path() -> PathBuf {
    // CARGO_MANIFEST_DIR is `<repo>/autumn-harvest`; the doc lives at the root.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent")
        .join("docs/performance.md")
}

/// Read a file with line endings normalised to `\n`.
///
/// Every structural helper below locates boundaries with `\n`-anchored needles
/// (`"\n}\n"` for a top-level item's closing brace, `"\n\n"` for a paragraph
/// break, `"\n### "` for a section break). A Windows checkout hands those
/// helpers `\r\n`, so each needle silently misses and the test fails with a
/// "file must define X" panic that has nothing to do with the file's contents.
/// Normalising once here keeps the helpers platform-agnostic rather than
/// spreading `\r?` handling across each of them.
fn read_normalized(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
        .replace("\r\n", "\n")
}

fn read_performance_doc() -> String {
    read_normalized(&performance_doc_path())
}

/// One row of the per-gate attribution table.
#[derive(Debug, Clone)]
struct GateRow {
    gate: String,
    /// The `n` column: how many samples the percentiles describe.
    samples: u32,
    p50_ms: f64,
    /// The `p50 vs` column, as a whole-percent integer (may be negative).
    delta_pct: i64,
    /// The `vs what` column: which row `delta_pct` is measured against.
    comparand: String,
}

/// Strip the ASCII spaces used as thousands separators (`1 324.88`).
fn parse_number(field: &str) -> Option<f64> {
    field.replace(' ', "").parse().ok()
}

/// Parse `**+1383%**` / `+2%` / `−1%` into a signed whole percent.
///
/// The page uses U+2212 MINUS SIGN, not an ASCII hyphen, so both are accepted.
fn parse_percent(field: &str) -> Option<i64> {
    let cleaned = field
        .replace("**", "")
        .replace('\u{2212}', "-")
        .replace(' ', "");
    let digits = cleaned.strip_suffix('%')?;
    digits.parse().ok()
}

/// Pull the gate name out of a cell like ``` `paused_rows` ⚠ ```.
fn parse_gate_name(field: &str) -> Option<String> {
    let start = field.find('`')?;
    let rest = &field[start + 1..];
    let end = rest.find('`')?;
    Some(rest[..end].to_string())
}

/// Split one Markdown table row into its cells, if it is a 7-column row.
///
/// The per-gate table's shape is
/// `gate | seeded rows | claimable | n | p50 ms | p50 vs | vs what`.
fn gate_table_cells(line: &str) -> Option<Vec<&str>> {
    let trimmed = line.trim();
    if !trimmed.starts_with('|') {
        return None;
    }
    let cells: Vec<&str> = trimmed
        .trim_matches('|')
        .split('|')
        .map(str::trim)
        .collect();
    (cells.len() == 7).then_some(cells)
}

fn parse_gate_table(doc: &str) -> Vec<GateRow> {
    let mut rows = Vec::new();
    for line in doc.lines() {
        let Some(cells) = gate_table_cells(line) else {
            continue;
        };
        let (Some(gate), Some(samples), Some(p50)) = (
            parse_gate_name(cells[0]),
            parse_number(cells[3]),
            parse_number(cells[4]),
        ) else {
            continue;
        };
        // `baseline` publishes an em-dash rather than a delta; it is the origin.
        let Some(delta_pct) = parse_percent(cells[5]) else {
            continue;
        };
        let Some(comparand) = parse_gate_name(cells[6]) else {
            continue;
        };
        rows.push(GateRow {
            gate,
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            samples: samples as u32,
            p50_ms: p50,
            delta_pct,
            comparand,
        });
    }
    rows
}

/// The `baseline` row is the table's origin, so it publishes an em-dash rather
/// than a delta and `parse_gate_table` skips it. Read its p50 separately.
fn baseline_p50(doc: &str) -> f64 {
    doc.lines()
        .filter(|line| line.trim_start().starts_with("| `baseline`"))
        .find_map(|line| parse_number(gate_table_cells(line)?[4]))
        .expect("docs/performance.md must publish a `baseline` row in the per-gate table")
}

#[test]
fn gate_table_percentages_agree_with_its_own_latency_column() {
    let doc = read_performance_doc();
    let rows = parse_gate_table(&doc);
    assert!(
        rows.len() >= 7,
        "expected the per-gate table to parse; got {} rows",
        rows.len()
    );

    let baseline = baseline_p50(&doc);
    let by_name: std::collections::BTreeMap<&str, f64> =
        rows.iter().map(|r| (r.gate.as_str(), r.p50_ms)).collect();

    for row in &rows {
        let comparand_p50 = if row.comparand == "baseline" {
            baseline
        } else {
            *by_name.get(row.comparand.as_str()).unwrap_or_else(|| {
                panic!(
                    "`{}` is measured against `{}`, which is not a row of the table",
                    row.gate, row.comparand
                )
            })
        };
        #[allow(clippy::cast_possible_truncation)]
        let computed = ((row.p50_ms / comparand_p50 - 1.0) * 100.0).round() as i64;
        assert_eq!(
            computed, row.delta_pct,
            "`{}` publishes {}% against `{}`, but {:.2} ms / {:.2} ms is {}%. \
             The percentage column and the latency column came from different runs.",
            row.gate, row.delta_pct, row.comparand, row.p50_ms, comparand_p50, computed
        );
    }
}

/// The per-gate bullets under "What each row exercises" must quote the table.
///
/// This is the exact drift a reviewer caught twice: the table was regenerated
/// and these bullets were not, leaving the page asserting two different costs
/// for the same scenario a hundred lines apart.
#[test]
fn per_gate_bullets_quote_the_published_gate_table() {
    let doc = read_performance_doc();
    let rows = parse_gate_table(&doc);
    let by_name: std::collections::BTreeMap<&str, &GateRow> =
        rows.iter().map(|r| (r.gate.as_str(), r)).collect();

    let mut checked = 0_usize;
    for line in doc.lines() {
        let trimmed = line.trim();
        // Shape: * **`gate_name` (+644%)** — prose
        //    or: * **`paused_rows` (+1% vs the control)** — prose
        let Some(rest) = trimmed.strip_prefix("* **`") else {
            continue;
        };
        let Some(name_end) = rest.find('`') else {
            continue;
        };
        let gate = &rest[..name_end];
        let Some(row) = by_name.get(gate) else {
            continue;
        };
        let after = &rest[name_end + 1..];
        let Some(open) = after.find('(') else {
            continue;
        };
        let Some(close) = after.find(')') else {
            continue;
        };
        let inside = &after[open + 1..close];

        let against_control = inside.contains("vs the control");
        let percent_text = inside.replace("vs the control", "");
        let Some(quoted) = parse_percent(percent_text.trim()) else {
            continue;
        };

        assert_eq!(
            quoted, row.delta_pct,
            "the `{gate}` bullet quotes {quoted}%, but the table publishes {}% for it. \
             Regenerate the prose from the table.",
            row.delta_pct
        );

        // A bullet says "vs the control" exactly when the table measures that
        // row against something other than `baseline`. Getting this wrong
        // silently reattributes the number to the wrong comparand.
        let table_uses_control = row.comparand != "baseline";
        assert_eq!(
            against_control,
            table_uses_control,
            "the `{gate}` bullet {} \"vs the control\", but the table measures it against `{}`.",
            if against_control {
                "says"
            } else {
                "does not say"
            },
            row.comparand
        );
        checked += 1;
    }

    assert!(
        checked >= 6,
        "expected to check the per-gate bullets against the table; matched only {checked}. \
         Did the bullet list change shape?"
    );
}

/// Extract the body of a top-level `pub async fn NAME(` from a Rust source file.
///
/// Terminates at the first line that is exactly `}` — the closing brace of a
/// top-level item. Good enough to read one function's call sites out of a file
/// this test does not otherwise need to understand.
fn top_level_fn_body<'a>(src: &'a str, name: &str) -> Option<&'a str> {
    let signature = format!("pub async fn {name}(");
    let start = src.find(&signature)?;
    let rest = &src[start..];
    let end = rest.find("\n}\n")?;
    Some(&rest[..end])
}

/// True when `needle` appears in `haystack` as a whole identifier.
///
/// Plain `contains` is not enough here: `release_claim` is a prefix of
/// `release_claim_if_queue_paused`, so a doc that named only the longer call
/// would look like it had named both.
fn mentions_identifier(haystack: &str, needle: &str) -> bool {
    haystack.match_indices(needle).any(|(idx, _)| {
        let after = haystack[idx + needle.len()..].chars().next();
        !after.is_some_and(|c| c.is_alphanumeric() || c == '_')
    })
}

/// Pull the blank-line-delimited paragraph that opens with `opener`.
fn paragraph_starting_with<'a>(doc: &'a str, opener: &str) -> Option<&'a str> {
    let start = doc.find(opener)?;
    let rest = &doc[start..];
    let end = rest.find("\n\n").unwrap_or(rest.len());
    Some(&rest[..end])
}

/// Pull one `### ` section out of the Markdown, heading excluded.
fn doc_section<'a>(doc: &'a str, heading: &str) -> Option<&'a str> {
    let start = doc.find(heading)? + heading.len();
    let rest = &doc[start..];
    let end = rest.find("\n### ").unwrap_or(rest.len());
    Some(&rest[..end])
}

/// Every statement in the claim transaction must be named in the docs.
///
/// "What is actually timed" publishes a round-trip count, and a reader sizing a
/// remote deployment multiplies their network RTT by it. That count is only
/// trustworthy while the enumeration matches the transaction, and nothing about
/// adding a statement to `claim_task` would otherwise disturb this page.
#[test]
fn claim_transaction_statements_are_all_named_in_the_docs() {
    let queue_src = read_normalized(&Path::new(env!("CARGO_MANIFEST_DIR")).join("src/queue.rs"));
    let body = top_level_fn_body(&queue_src, "claim_task")
        .expect("queue.rs must define `pub async fn claim_task(`");

    let mut called: Vec<&str> = Vec::new();
    for (idx, _) in body.match_indices("crate::queue_pause::") {
        let tail = &body[idx + "crate::queue_pause::".len()..];
        let end = tail
            .find(|c: char| !(c.is_alphanumeric() || c == '_'))
            .unwrap_or(tail.len());
        let name = &tail[..end];
        if !name.is_empty() && !called.contains(&name) {
            called.push(name);
        }
    }
    assert!(
        called.len() >= 2,
        "expected to find the queue-pause statements in `claim_task`; found {called:?}"
    );

    let doc = read_performance_doc();
    let section = doc_section(&doc, "### What is actually timed")
        .expect("docs/performance.md must have a `What is actually timed` section");
    for name in called {
        assert!(
            mentions_identifier(section, name),
            "`claim_task` issues `queue_pause::{name}`, but the \"What is actually \
             timed\" section does not name it. Its published round-trip count is \
             therefore describing a different transaction than the code runs."
        );
    }
}

/// The enqueue caveat must defer to the one authoritative window definition.
///
/// The page described the throughput denominator in two places and they
/// disagreed: the caveat claimed it spanned "warmup and task spawn/join" while
/// the hygiene bullet correctly described a barrier-to-completion span that
/// excludes everything outside the worker closures. Restating the endpoints in
/// two voices is what let them drift, so the caveat now links instead.
#[test]
fn enqueue_throughput_caveat_defers_to_the_authoritative_window_definition() {
    let doc = read_performance_doc();

    assert!(
        doc.contains("earliest resume after the barrier through the last completion"),
        "the authoritative throughput-window definition is missing from Measurement hygiene"
    );

    let caveat = paragraph_starting_with(&doc, "Two caveats on this table.")
        .expect("the enqueue table must carry its `Two caveats on this table.` paragraph");

    assert!(
        caveat.contains("#measurement-hygiene"),
        "the enqueue throughput caveat must link to `#measurement-hygiene` rather \
         than restate the window's endpoints; restating them is what let the two \
         descriptions drift apart. Caveat text:\n{caveat}"
    );

    // The caveat may mention spawn/join — but only to place them *outside* the
    // window. A sentence that mentions them without saying so is the exact
    // regression this guards: it reads as "the denominator includes them".
    let flowed = caveat.replace('\n', " ");
    for sentence in flowed.split(". ") {
        assert!(
            !sentence.contains("spawn") || sentence.contains("outside"),
            "the enqueue throughput caveat mentions task spawn without placing it \
             outside the measured window. The spans are captured at barrier resume \
             and closed inside the worker closure, so spawn and join are not in the \
             denominator. Sentence:\n{sentence}"
        );
    }
}

/// The structural helpers must survive a CRLF checkout.
///
/// Every helper above locates boundaries with `\n`-anchored needles. Git checks
/// this repository out with CRLF on Windows, so reading a file raw hands those
/// helpers `\r\n` and each needle silently misses — surfacing as a bogus "file
/// must define X" panic. That is exactly what happened: the guards passed on
/// Linux and macOS and failed only on `Test (windows-latest)`.
///
/// This test reproduces the platform difference *on every platform* by writing
/// a fixture with explicit CRLF. If `read_normalized` is ever simplified back to
/// a plain `read_to_string`, this fails in the ordinary Linux test run rather
/// than waiting for Windows CI to notice.
#[test]
fn structural_helpers_survive_crlf_line_endings() {
    let dir = tempfile::tempdir().expect("create temp dir");

    // A Rust source fixture whose closing brace is preceded by CRLF.
    let src_path = dir.path().join("crlf_queue.rs");
    std::fs::write(
        &src_path,
        "use std::fmt;\r\n\r\npub async fn claim_task(x: i32) -> i32 {\r\n    \
         crate::queue_pause::try_lock_queue_for_claim(x);\r\n    \
         crate::queue_pause::release_claim_if_queue_paused(x)\r\n}\r\n\r\nfn other() {}\r\n",
    )
    .expect("write CRLF source fixture");

    let src = read_normalized(&src_path);
    let body = top_level_fn_body(&src, "claim_task")
        .expect("top_level_fn_body must find a CRLF-checked-out function body");
    assert!(
        mentions_identifier(body, "try_lock_queue_for_claim"),
        "the extracted body must contain the function's own call sites: {body}"
    );
    assert!(
        !body.contains("fn other"),
        "the body must stop at the closing brace, not run into the next item: {body}"
    );

    // A Markdown fixture with CRLF paragraph and section breaks.
    let doc_path = dir.path().join("crlf_doc.md");
    std::fs::write(
        &doc_path,
        "### What is actually timed\r\n\r\nOpener paragraph.\r\n\r\n\
         Two caveats on this table. First one.\r\nSecond line.\r\n\r\n\
         ### Next section\r\n\r\nUnrelated.\r\n",
    )
    .expect("write CRLF doc fixture");

    let doc = read_normalized(&doc_path);
    let caveat = paragraph_starting_with(&doc, "Two caveats on this table.")
        .expect("paragraph_starting_with must find a CRLF-delimited paragraph");
    assert!(
        caveat.contains("Second line."),
        "the paragraph must span its wrapped lines: {caveat}"
    );
    assert!(
        !caveat.contains("Next section"),
        "the paragraph must stop at the blank line: {caveat}"
    );

    let section = doc_section(&doc, "### What is actually timed")
        .expect("doc_section must find a CRLF-delimited section");
    assert!(
        section.contains("Opener paragraph."),
        "the section must contain its own body: {section}"
    );
    assert!(
        !section.contains("Unrelated."),
        "the section must stop at the next `### ` heading: {section}"
    );
}

/// The page must not attribute the `paused_rows` +1% to the predicate itself.
///
/// `double_backlog` controls for *total* rows, not for the population that
/// reaches the sort: the PAUSED anti-join is a `WHERE` predicate, so it removes
/// half the table before the `ORDER BY` in `paused_rows` and removes nothing in
/// the control (20 000 rows sorted vs 10 000). The +1% is therefore the probe
/// cost minus the sort saving — two effects with opposite signs that this
/// measurement cannot separate — and the page said "the anti-join is free" for
/// several revisions on the strength of it.
///
/// The claim reads so naturally that it came back once already after being
/// narrowed. This pins the narrowing: the page must keep the subsection that
/// explains the confound, and must not re-assert the isolated-cost reading.
#[test]
fn the_paused_rows_delta_is_not_attributed_to_the_predicate() {
    let doc = read_performance_doc();

    assert!(
        doc.contains("#### What that control does *not* establish"),
        "docs/performance.md must keep the subsection explaining why the \
         `double_backlog` control cannot isolate the anti-join predicate. \
         Without it the +1% reads as the predicate's own cost."
    );
    assert!(
        doc.contains("rows fed to the sort"),
        "the confound is the differing sort input (20 000 vs 10 000); the \
         subsection must show it rather than assert it in prose"
    );

    // Phrases that assert the isolated-cost reading the control cannot support.
    for banned in [
        "The anti-join is free",
        "the anti-join is free",
        "Free as a predicate",
        "free as a predicate",
        "the anti-join costs **+1%**",
        "Deleting the predicate would buy you nothing",
    ] {
        assert!(
            !doc.contains(banned),
            "docs/performance.md says \"{banned}\", which attributes the \
             `paused_rows` +1% to the anti-join predicate in isolation. The \
             `double_backlog` control does not support that: it sorts 20 000 \
             rows where `paused_rows` sorts 10 000, so the delta mixes the \
             predicate's probe cost with a sort saving of opposite sign. \
             Publish the depth-controlled finding, not a predicate cost."
        );
    }
}

/// These guards must actually execute on a docs-only pull request.
///
/// The whole point of this file is to catch prose that drifts from the tables
/// in `docs/performance.md`. The change most likely to cause that drift is a
/// pull request that edits *only* that file — and that is precisely the one CI
/// used to skip: the `changes` job classifies `docs/**` as `code=false`, and
/// every step of the `test` matrix is gated on `code == 'true'`. The `lint`
/// job runs ungated, but its Clippy invocation only *compiles* this file.
///
/// So for three rounds these guards could not have fired on the change class
/// they were written for. The fix is a `cargo test` step in the ungated `lint`
/// job; this asserts that step is there and has not acquired a condition that
/// would put it back behind the same gate.
#[test]
fn performance_guards_run_on_docs_only_changes() {
    let workflow = read_normalized(
        &Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("crate directory must have a parent")
            .join(".github/workflows/ci.yml"),
    );

    // The step must exist, and must name this module as its filter — a step
    // that ran some *other* test would satisfy a looser check while leaving
    // these guards just as unexecuted.
    let step = workflow
        .lines()
        .find(|line| line.contains("--test integration performance_docs::"))
        .expect(
            "ci.yml must run the performance_docs guards from a step that is not \
             gated on `changes.outputs.code`, or a docs-only PR — the change class \
             these guards exist for — skips them entirely",
        );
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
    let step_at = workflow
        .find("--test integration performance_docs::")
        .expect("located above");
    assert!(
        step_at > lint_start && step_at < test_start,
        "the performance_docs guard step must live in the ungated `lint` job; \
         a step in the `test` matrix is gated on `changes.outputs.code` and so \
         does not run on a docs-only PR"
    );

    // And it must be unconditional. A step that grew an `if:` is back behind a
    // gate — which is the exact regression this test exists to prevent.
    let block: &str = &workflow[lint_start..test_start];
    let step_idx = block
        .find("--test integration performance_docs::")
        .expect("step is inside the lint block");
    let step_line_start = block[..step_idx].rfind("\n      - name:").unwrap_or(0);
    let stanza = &block[step_line_start..step_idx];
    assert!(
        !stanza.contains("\n        if:"),
        "the performance_docs guard step has acquired an `if:` condition. It must \
         run unconditionally: a condition is how these guards stopped running on \
         docs-only PRs in the first place. Stanza:\n{stanza}"
    );
}

/// The changelog fragment must not publish superseded figures.
///
/// The fragment is collated into the public `CHANGELOG.md`, so its opening
/// entry *is* the release note. Rounds of review appended corrections to the
/// end of it as a development diary — which left the headline still quoting an
/// earlier run's numbers and the attribution that was later retracted, roughly
/// ninety thousand characters before the retraction. A reader of the collated
/// changelog meets the superseded claim first and may never reach the fix.
///
/// So this pins the fragment against the same tables `docs/performance.md`
/// publishes: the figures it quotes must be the current ones, and the rejected
/// isolated-cost reading must not appear as an assertion anywhere in it.
///
/// This guard lives here rather than beside the changelog because
/// `docs/changelog.d/**` is also classified `code=false` — a changelog-only
/// edit is exactly the kind of change that skips the gated test matrix, and the
/// ungated lint step runs *this* module.
#[test]
fn changelog_fragment_quotes_the_published_figures() {
    let doc = read_performance_doc();
    let rows = parse_gate_table(&doc);
    let by_name: std::collections::BTreeMap<&str, &GateRow> =
        rows.iter().map(|r| (r.gate.as_str(), r)).collect();
    let control = by_name
        .get("double_backlog")
        .expect("the control row must exist");

    let fragment = read_normalized(
        &Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("crate directory must have a parent")
            .join("docs/changelog.d/pr-786-claim-throughput-benchmark.md"),
    );

    // The control's multiplier is the one the retracted attribution was built
    // on, so it is the one most likely to be left stale. Read the current value
    // off the table rather than hardcoding it.
    //
    // Scoped to the sentence that introduces the control, NOT to the whole
    // fragment: the figure also appears in the appended round-by-round diary,
    // so a bare `fragment.contains(..)` is satisfied by a *correct* mention in
    // the diary while the *leading* entry — the part that becomes the release
    // note — still quotes a superseded run. That is exactly the drift this
    // guards, so an unscoped check would pass through it. (Verified: it did.)
    let published = format!("+{}%", control.delta_pct);
    let intro = fragment
        .find("ClaimGate::DoubleBacklog")
        .expect("the changelog fragment must introduce the `ClaimGate::DoubleBacklog` control");
    let window_end = (intro + 400).min(fragment.len());
    let window = &fragment[intro..window_end];
    assert!(
        window.contains(&published),
        "the changelog fragment introduces the `double_backlog` control without \
         quoting its published cost ({published}). The fragment is collated into \
         the public CHANGELOG, so a superseded figure here IS the release note \
         while the correction sits thousands of characters below it. Window:\n{window}"
    );

    // Assertions of the reading the control cannot support.
    //
    // A *quoted* occurrence is fine and in fact necessary: the diary retracts
    // these claims by name, so it has to reproduce them. Only an occurrence the
    // fragment states in its own voice is the regression. Distinguished by the
    // character immediately before it — a retraction reads `... the attribution
    // retracted in round 24 ("deleting the predicate would buy nothing") ...`.
    //
    // Without this the guard fails on the very text that fixes the bug, which
    // is how it first behaved: writing the round-26 retraction tripped it.
    for banned in [
        "deleting the predicate would buy nothing",
        "Deleting the predicate would buy nothing",
        "the anti-join costs **−2%**",
        "the anti-join costs **+1%**",
    ] {
        let asserted = fragment.match_indices(banned).any(|(idx, _)| {
            let before = fragment[..idx].chars().next_back();
            !matches!(before, Some('"' | '\u{201c}' | '\u{2018}' | '\''))
        });
        assert!(
            !asserted,
            "the changelog fragment states \"{banned}\" in its own voice, which \
             attributes the `paused_rows` delta to the anti-join predicate in \
             isolation. The `double_backlog` control sorts a different number of \
             rows, so it cannot support that; publish the depth-controlled \
             finding instead. (Quoting the phrase while retracting it is fine — \
             put it in quotation marks.)"
        );
    }
}

/// The harness docs must name the statistic the gate actually asserts.
///
/// The gate switched from p99 to p50 — deliberately, because the headline
/// scenario oversubscribes the box and the tail measures the run queue rather
/// than the claim path. The shared harness's module docs kept describing a p99
/// gate, so a maintainer reading the harness first would calibrate or relax the
/// budget against a statistic nothing asserts.
#[test]
fn harness_docs_name_the_asserted_statistic() {
    let harness = read_normalized(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/integration/claim_bench_support.rs"),
    );
    let module_doc: String = harness
        .lines()
        .take_while(|line| line.starts_with("//!") || line.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n");

    assert!(
        module_doc.contains("fails the build if **p50** exceeds"),
        "the harness module docs must state that the CI gate asserts p50; the \
         gate compares `report.stats.p50_ms` against `HEADLINE_P50_BUDGET_MS`"
    );
    assert!(
        !module_doc.contains("fails the build if p99 exceeds"),
        "the harness module docs claim the gate asserts p99. It asserts p50 — \
         p99 is measured and printed but deliberately not gated, because the \
         headline scenario's tail measures the run queue. A maintainer reading \
         this would tune the budget against the wrong statistic."
    );
}

/// The truncated rows must describe the sample count they actually published.
#[test]
fn truncated_sample_counts_in_prose_match_the_table() {
    let doc = read_performance_doc();
    let rows = parse_gate_table(&doc);
    let by_name: std::collections::BTreeMap<&str, &GateRow> =
        rows.iter().map(|r| (r.gate.as_str(), r)).collect();

    let double_backlog = by_name
        .get("double_backlog")
        .expect("the control row must exist");
    let paused_rows = by_name
        .get("paused_rows")
        .expect("the paused_rows row must exist");

    // The control-vs-anti-join discussion quotes both n values as `n=A/B`.
    let needle = format!("n={}/{}", double_backlog.samples, paused_rows.samples);
    assert!(
        doc.contains(&needle),
        "the control discussion must quote the published sample counts as `{needle}`; \
         the table shows double_backlog n={} and paused_rows n={}.",
        double_backlog.samples,
        paused_rows.samples
    );
}

/// Every unquoted value in `ci.yml` must be a legal YAML plain scalar.
///
/// This exists because the guard above — which asserts the `performance_docs`
/// step is present, ungated, and in the right job — passed happily while the
/// workflow file was **not valid YAML at all**, so GitHub loaded no jobs and
/// every run failed instantly. A guard that asserts a CI step exists is
/// worthless if the file containing it cannot be parsed; the step was there,
/// correct, and dead.
///
/// The specific trap: a plain (unquoted) YAML scalar may not contain a colon
/// that the parser reads as a mapping separator — either `: ` mid-value, or a
/// trailing `:` at end of line. The step this module added ended in
/// `performance_docs::`, so YAML saw a mapping indicator and rejected the file
/// with "mapping values are not allowed here". Quoting the value fixes it.
///
/// This is a targeted rule, not a YAML parser, and it is named for what it
/// checks rather than for "the workflow is valid". It is the right rule for
/// *this* file specifically: `ci.yml` is dominated by long shell one-liners,
/// and cargo test filters (`module::`), URLs (`https://`) and time expressions
/// are exactly the values that acquire colons. A full parse would need a YAML
/// dependency on the core crate, and the only one already in the lockfile is
/// both deprecated and reachable solely through `autumn-web`, which this crate
/// does not depend on.
#[test]
fn ci_yaml_plain_scalars_do_not_contain_a_mapping_colon() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent")
        .join(".github/workflows/ci.yml");
    let workflow = read_normalized(&path);

    for (n, line) in workflow.lines().enumerate() {
        let trimmed = line.trim_start();

        // Only `key: value` lines carry a plain scalar. Comments, list items
        // without a key, and blank lines cannot hit the trap.
        let Some(colon) = trimmed.find(": ") else {
            continue;
        };
        if trimmed.starts_with('#') {
            continue;
        }
        let value = trimmed[colon + 2..].trim();
        if value.is_empty() {
            continue;
        }

        // A quoted value is exempt: quoting is precisely the fix, so a guard
        // that flagged it too would forbid the correct spelling.
        let quoted = (value.starts_with('"') && value.ends_with('"') && value.len() > 1)
            || (value.starts_with('\'') && value.ends_with('\'') && value.len() > 1);
        // Block scalars (`|`, `>`, and their chomping/indent variants) are not
        // plain scalars at all — their content lines are opaque to the parser's
        // mapping rules, which is why every long multi-line `run:` in this file
        // is already safe.
        let block = value.starts_with('|') || value.starts_with('>');
        if quoted || block {
            continue;
        }

        assert!(
            !value.ends_with(':'),
            "ci.yml line {} ends an unquoted value with `:`, which YAML reads as \
             a mapping indicator — the whole file then fails to parse, GitHub \
             loads zero jobs, and every run fails instantly with the workflow \
             named after its own path. Quote the value. Line:\n{}",
            n + 1,
            line
        );
        assert!(
            !value.contains(": "),
            "ci.yml line {} has an unquoted value containing `: `, which YAML \
             reads as a mapping separator. Quote the value. Line:\n{}",
            n + 1,
            line
        );
    }
}

/// The benchmark's *generated report* must not republish the retracted
/// attribution either.
///
/// Round 24 narrowed the claim in `docs/performance.md`; round 26 found the
/// changelog's leading entry still carried it; this covers the third surface —
/// the note `benches/claim_bench.rs` prints under its per-gate table. That note
/// is arguably the worst place for it to survive: the doc is read once, but the
/// bench output is what someone pastes into a PR when they regenerate the
/// numbers, so a stale interpretation there propagates into new writing.
///
/// Scoped to `println!` string content rather than the whole file: the source
/// may freely *discuss* the retraction in comments (this fix does), and a guard
/// that could not tell an explanation from an assertion would forbid explaining
/// it — the same self-trip that
/// [`changelog_fragment_quotes_the_published_figures`] hit and solves by
/// quote-detection.
#[test]
fn the_bench_report_does_not_republish_the_retracted_attribution() {
    let bench =
        read_normalized(&Path::new(env!("CARGO_MANIFEST_DIR")).join("benches/claim_bench.rs"));

    // Everything the bench actually prints, with comments excluded.
    let printed: String = bench
        .lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n");

    for banned in [
        "the delta is the anti-join's cost",
        "the anti-join's cost rather than",
        "so the delta is the anti-join",
    ] {
        assert!(
            !printed.contains(banned),
            "benches/claim_bench.rs prints \"{banned}\", attributing the \
             `paused_rows` delta to the anti-join predicate in isolation. The \
             `double_backlog` control matches total table rows but not the \
             population reaching the sort (20 000 vs 10 000), so the delta is \
             the probe cost minus a sort saving and cannot isolate either. \
             `docs/performance.md` retracted this reading in round 24; the \
             generated report must not keep publishing it."
        );
    }

    // And it must positively carry the correction, so the note cannot simply be
    // deleted and leave the table unqualified.
    assert!(
        printed.contains("not the anti-join's cost in isolation"),
        "benches/claim_bench.rs must state, in the report it prints, that the \
         `paused_rows` delta is not the anti-join's cost in isolation. Dropping \
         the note entirely leaves the per-gate table's `vs` column unqualified, \
         which is how the misreading started."
    );
}
