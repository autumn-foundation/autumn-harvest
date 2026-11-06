//! `--self-test`: the lexer and the rules against their fixtures.

use std::collections::{BTreeSet, HashMap, HashSet};

use super::TIER_B;
use super::fixtures::{
    CODE_SHAPE_TESTS, CONTRACTIONS_EXCLUDED, CONTRACTIONS_EXPECTED, RULE_TESTS, SELF_TESTS,
    SWEEP_KEYWORDS, SWEEP_TAILS,
};
use super::lexer::extract_comments;
use super::patterns::{COMMENTED_CODE_RE, CONTRACTION_RE};
use super::pyre::{repr, repr_list, strip};
use super::ratchet::{
    Tally, compare_tier_b, index_findings, parse_name_status, tally, tier_b_gate,
};
use super::rules::{Finding, findings_for_source};

/// The status tag of one check.
const fn tag(ok: bool) -> &'static str {
    if ok { "ok  " } else { "FAIL" }
}

/// Python `tail.format(n=keyword)` for the sweep templates.
fn format_tail(template: &str, keyword: &str) -> String {
    let mut out = String::new();
    let mut rest = template;
    while !rest.is_empty() {
        if let Some(after) = rest.strip_prefix("{n}") {
            out.push_str(keyword);
            rest = after;
        } else if let Some(after) = rest.strip_prefix("{{") {
            out.push('{');
            rest = after;
        } else if let Some(after) = rest.strip_prefix("}}") {
            out.push('}');
            rest = after;
        } else {
            let c = rest.chars().next().unwrap_or_default();
            out.push(c);
            rest = &rest[c.len_utf8()..];
        }
    }
    out
}

/// Generated prose lines that CH001 must never match.
pub fn prose_sweep() -> Vec<String> {
    SWEEP_KEYWORDS
        .iter()
        .flat_map(|keyword| SWEEP_TAILS.iter().map(|tail| format_tail(tail, keyword)))
        .collect()
}

/// The comment texts that the lexer finds in `source`, empty ones dropped.
pub fn lexed(source: &str) -> Vec<String> {
    extract_comments(source)
        .iter()
        .map(|piece| strip(&piece.text).to_string())
        .filter(|text| !text.is_empty())
        .collect()
}

/// The `(rule, line)` set of the findings in `source`.
pub fn rule_set(source: &str) -> BTreeSet<(&'static str, usize)> {
    findings_for_source("t.rs", source)
        .iter()
        .map(|finding| (finding.rule, finding.line))
        .collect()
}

/// Python `repr` of a sorted list of `(rule, line)` pairs.
fn repr_pairs<'a>(pairs: impl IntoIterator<Item = &'a (&'static str, usize)>) -> String {
    let parts: Vec<String> = pairs
        .into_iter()
        .map(|(rule, line)| format!("({}, {line})", repr(rule)))
        .collect();
    format!("[{}]", parts.join(", "))
}

/// The Tier B findings in `source`, in a file named `f.rs`.
fn tier_b(source: &str) -> Vec<Finding> {
    findings_for_source("f.rs", source)
        .into_iter()
        .filter(|finding| TIER_B.contains(&finding.rule))
        .collect()
}

/// A Tier B failure must name the line that the contributor must fix.
///
/// The hard case is a duplicate. The fingerprint is the comment text, so
/// two identical comments are one fingerprint with a count. A report of the
/// first case then points at the legacy line whenever the merge base
/// already had a copy. That is a gate that tells someone to edit a comment
/// that they never wrote. So each candidate is named, with a count of how
/// many are new.
pub fn ratchet_reporting_test() -> (bool, Vec<String>) {
    let legacy = "// It doesn\u{2019}t matter here.\n";
    let base_source = format!("// filler one\n{legacy}{}", "// filler\n".repeat(12));
    let head_source = format!("{base_source}// tail filler\n{legacy}");
    let head = tier_b(&head_source);
    let base = tier_b(&base_source);
    let scope: HashSet<String> = std::iter::once("f.rs".to_string()).collect();
    let lines = compare_tier_b(
        &tally(&head),
        &tally(&base),
        Some(&scope),
        &HashMap::new(),
        &index_findings(&head),
    );
    let named: BTreeSet<usize> = lines
        .iter()
        .filter(|line| line.starts_with("    f.rs:"))
        .filter_map(|line| line.split(':').nth(1))
        .filter_map(|number| number.parse().ok())
        .collect();
    let ok =
        named == BTreeSet::from([2, 16]) && lines.iter().any(|line| line.contains("1 of these 2"));
    (ok, lines)
}

/// `--tier-a-only` must mean the same thing with and without `--json`.
///
/// The two flags together asked for the absolute gates in a form for
/// machines, and they got a Tier B regression in the exit status. This is
/// not a lexer question, so it cannot be a fixture. It checks the pair of
/// flags on the one function that now answers for both paths.
pub fn tier_a_only_test() -> (bool, Vec<String>, Vec<String>) {
    let legacy = "// It doesn\u{2019}t matter here.\n";
    let head = tier_b(&format!("// filler\n{legacy}"));
    let current: Tally = tally(&head);
    let baseline: Tally = tally(&tier_b("// filler\n"));
    let scope: HashSet<String> = std::iter::once("f.rs".to_string()).collect();
    let renames = HashMap::new();
    let gated = tier_b_gate(&current, &baseline, Some(&scope), &renames, &head, false);
    let skipped = tier_b_gate(&current, &baseline, Some(&scope), &renames, &head, true);
    let ok = !gated.is_empty() && skipped.is_empty();
    (ok, gated, skipped)
}

/// Runs each fixture and prints one line per check. Returns the exit status.
///
/// The Python original also compiled its own source, to catch an escape
/// sequence that Python only warns about. The Rust compiler refuses a bad
/// escape at build time, so that check has no counterpart here.
#[allow(clippy::too_many_lines)]
pub fn self_test() -> u8 {
    let mut failures = 0usize;
    for case in SELF_TESTS {
        let got = lexed(case.source);
        let ok = got
            .iter()
            .map(String::as_str)
            .eq(case.expected.iter().copied());
        failures += usize::from(!ok);
        println!("  [{}] {}", tag(ok), case.name);
        if !ok {
            let expected: Vec<String> = case.expected.iter().map(|s| (*s).to_string()).collect();
            println!(
                "         expected {}\n         got      {}",
                repr_list(&expected),
                repr_list(&got)
            );
        }
    }

    // Line numbers must point at the line where the comment really is.
    let source = "let e = \"a \\\n b\";\n// target\n";
    let pieces: Vec<_> = extract_comments(source)
        .into_iter()
        .filter(|piece| !strip(&piece.text).is_empty())
        .collect();
    let ok = pieces.len() == 1 && pieces[0].line == 3;
    failures += usize::from(!ok);
    println!("  [{}] line number survives a continuation", tag(ok));
    if !ok {
        let got: Vec<String> = pieces
            .iter()
            .map(|piece| format!("({}, {})", piece.line, repr(&piece.text)))
            .collect();
        println!("         expected line 3, got [{}]", got.join(", "));
    }

    // The diff parser. No fixture covered it until a reviewer found that a
    // path outside ASCII left the Tier B scope in silence. This is real
    // `git diff -z` output from a scratch repository with both shapes.
    let listing = "A\0src/\u{e9}.rs\0R100\0src/a.rs\0src/\u{f1}.rs\0M\0src/b.rs\0";
    let (changed, renames) = parse_name_status(listing);
    let want_changed: HashSet<String> = ["src/\u{e9}.rs", "src/\u{f1}.rs", "src/b.rs"]
        .iter()
        .map(|path| (*path).to_string())
        .collect();
    let want_renames: HashMap<String, String> =
        std::iter::once(("src/\u{f1}.rs".to_string(), "src/a.rs".to_string())).collect();
    let ok = changed == want_changed && renames == want_renames;
    failures += usize::from(!ok);
    println!(
        "  [{}] a NUL-delimited diff keeps a non-ASCII path",
        tag(ok)
    );
    if !ok {
        let mut sorted: Vec<String> = changed.into_iter().collect();
        sorted.sort();
        println!(
            "         changed {}\n         renames {renames:?}",
            repr_list(&sorted)
        );
    }

    let missed: Vec<&str> = CONTRACTIONS_EXPECTED
        .iter()
        .copied()
        .filter(|c| !CONTRACTION_RE.is_match(&format!("The row {c} ready.")))
        .collect();
    let wrong: Vec<&str> = CONTRACTIONS_EXCLUDED
        .iter()
        .copied()
        .filter(|c| CONTRACTION_RE.is_match(&format!("The row {c} ready.")))
        .collect();
    failures += missed.len() + wrong.len();
    if missed.is_empty() && wrong.is_empty() {
        println!(
            "  [ok  ] CH006 contraction inventory ({} matched, {} correctly ignored)",
            CONTRACTIONS_EXPECTED.len(),
            CONTRACTIONS_EXCLUDED.len()
        );
    } else {
        println!(
            "  [FAIL] CH006 contraction inventory: {} missed, {} false",
            missed.len(),
            wrong.len()
        );
        for c in &missed {
            println!("         missed: {}", repr(c));
        }
        for c in &wrong {
            println!("         false positive on possessive: {}", repr(c));
        }
    }

    let sweep = prose_sweep();
    let sweep_fps: Vec<&String> = sweep
        .iter()
        .filter(|line| COMMENTED_CODE_RE.match_start(line).is_some())
        .collect();
    failures += sweep_fps.len();
    if sweep_fps.is_empty() {
        println!(
            "  [ok  ] CH001 adversarial prose sweep ({} generated prose lines)",
            sweep.len()
        );
    } else {
        println!(
            "  [FAIL] CH001 adversarial prose sweep: {} false positive(s)",
            sweep_fps.len()
        );
        for line in sweep_fps.iter().take(10) {
            println!("         {}", repr(line));
        }
    }

    let mut shape_failures = 0;
    for &(line, want) in CODE_SHAPE_TESTS {
        if COMMENTED_CODE_RE.match_start(line).is_some() != want {
            shape_failures += 1;
            let kind = if want { "missed" } else { "false positive" };
            println!("  [FAIL] CH001 {kind}: {}", repr(line));
        }
    }
    failures += shape_failures;
    if shape_failures == 0 {
        println!(
            "  [ok  ] CH001 code-vs-prose boundary ({} shapes)",
            CODE_SHAPE_TESTS.len()
        );
    }

    for case in RULE_TESTS {
        let got = rule_set(case.source);
        let expected: BTreeSet<(&str, usize)> = case.expected.iter().copied().collect();
        let ok = got == expected;
        failures += usize::from(!ok);
        println!("  [{}] {}", tag(ok), case.name);
        if !ok {
            println!(
                "         expected {}\n         got      {}",
                repr_pairs(&expected),
                repr_pairs(&got)
            );
        }
    }

    let (ok, lines) = ratchet_reporting_test();
    failures += usize::from(!ok);
    println!(
        "  [{}] a duplicated Tier B finding names every candidate line",
        tag(ok)
    );
    if !ok {
        for line in &lines {
            println!("         {line}");
        }
    }

    let (ok, gated, skipped) = tier_a_only_test();
    failures += usize::from(!ok);
    println!("  [{}] --tier-a-only skips the Tier B ratchet", tag(ok));
    if !ok {
        println!(
            "         gated {}\n         skipped {}",
            repr_list(&gated),
            repr_list(&skipped)
        );
    }

    if failures == 0 {
        println!("\nOK: self-test passed.");
    } else {
        println!("\n{failures} self-test failure(s).");
    }
    u8::from(failures > 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_lexer_fixtures_hold() {
        let failed: Vec<&str> = SELF_TESTS
            .iter()
            .filter(|case| {
                !lexed(case.source)
                    .iter()
                    .map(String::as_str)
                    .eq(case.expected.iter().copied())
            })
            .map(|case| case.name)
            .collect();
        assert!(failed.is_empty(), "{failed:?}");
    }

    #[test]
    fn the_rule_fixtures_hold() {
        let failed: Vec<&str> = RULE_TESTS
            .iter()
            .filter(|case| rule_set(case.source) != case.expected.iter().copied().collect())
            .map(|case| case.name)
            .collect();
        assert!(failed.is_empty(), "{} failed: {failed:?}", failed.len());
    }

    #[test]
    fn the_code_shapes_hold() {
        let failed: Vec<&str> = CODE_SHAPE_TESTS
            .iter()
            .filter(|(line, want)| COMMENTED_CODE_RE.match_start(line).is_some() != *want)
            .map(|(line, _)| *line)
            .collect();
        assert!(failed.is_empty(), "{failed:?}");
    }

    #[test]
    fn the_prose_sweep_has_no_false_positive() {
        let sweep = prose_sweep();
        assert_eq!(sweep.len(), SWEEP_KEYWORDS.len() * SWEEP_TAILS.len());
        assert_eq!(sweep[0], "fn handles the retry path.");
        let hits: Vec<&String> = sweep
            .iter()
            .filter(|line| COMMENTED_CODE_RE.match_start(line).is_some())
            .collect();
        assert!(hits.is_empty(), "{hits:?}");
    }

    #[test]
    fn the_contraction_inventory_holds() {
        for c in CONTRACTIONS_EXPECTED {
            assert!(
                CONTRACTION_RE.is_match(&format!("The row {c} ready.")),
                "{c}"
            );
        }
        for c in CONTRACTIONS_EXCLUDED {
            assert!(
                !CONTRACTION_RE.is_match(&format!("The row {c} ready.")),
                "{c}"
            );
        }
    }

    #[test]
    fn a_line_number_survives_a_continuation() {
        let pieces = extract_comments("let e = \"a \\\n b\";\n// target\n");
        assert_eq!(pieces.len(), 1);
        assert_eq!(pieces[0].line, 3);
    }

    #[test]
    fn the_ratchet_names_each_candidate() {
        let (ok, lines) = ratchet_reporting_test();
        assert!(ok, "{lines:?}");
    }

    #[test]
    fn tier_a_only_skips_the_ratchet() {
        let (ok, gated, skipped) = tier_a_only_test();
        assert!(ok, "{gated:?} {skipped:?}");
    }

    #[test]
    fn format_tail_reads_python_braces() {
        assert_eq!(format_tail("{n} Foo {{ .. }} x", "let"), "let Foo { .. } x");
    }
}
