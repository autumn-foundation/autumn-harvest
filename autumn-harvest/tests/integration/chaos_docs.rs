//! Guards that keep `docs/testing/chaos.md`'s local-iteration example scoped
//! to the `chaos_tests` module (issue #1202, finding P2-2).
//!
//! The doc's "point at an already-migrated local Postgres" example runs the
//! chaos suite against a caller-supplied `HARVEST_TEST_DATABASE_URL`. Chaos
//! tests serialise on a process-wide `DB_BODY_SERIAL` mutex and each test's
//! `scrub()` step issues a global `TRUNCATE` against that database -- but no
//! *other* integration-test module joins that mutex, so an unscoped
//! `cargo test ... --test integration` (no module filter) risks a chaos
//! test's `TRUNCATE` racing a concurrent, unrelated module's assertions
//! against the same shared database. CI's own `.github/workflows/chaos.yml`
//! invocation is already correctly scoped to `chaos_tests::`; these guards
//! keep the *documentation's* worked example in permanent lockstep with it,
//! rather than trusting a human proofread to notice the drift.

use std::path::{Path, PathBuf};

/// `<repo>`, i.e. the parent of `CARGO_MANIFEST_DIR` (`<repo>/autumn-harvest`).
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent")
        .to_path_buf()
}

fn chaos_doc_path() -> PathBuf {
    repo_root().join("docs/testing/chaos.md")
}

/// Read a file with line endings normalised to `\n`.
///
/// The structural helpers below locate boundaries with `\n`-anchored needles
/// (`` ```bash `` fences, `\n  lint:` / `\n  test:` job headers). A Windows
/// checkout hands those helpers `\r\n`, which silently breaks the needles --
/// normalising once here keeps them platform-agnostic (the same rationale as
/// `performance_docs::read_normalized`).
fn read_normalized(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
        .replace("\r\n", "\n")
}

fn read_chaos_doc() -> String {
    read_normalized(&chaos_doc_path())
}

/// Return the body of the first ` ```bash ` fenced block containing `needle`,
/// scanning subsequent blocks if an earlier one doesn't match. Panics with a
/// message naming what was searched for if no such block exists.
fn bash_block_containing<'a>(doc: &'a str, needle: &str) -> &'a str {
    let mut search_from = 0;
    loop {
        let Some(rel_start) = doc[search_from..].find("```bash") else {
            panic!(
                "docs/testing/chaos.md must contain a ```bash fenced block \
                 mentioning {needle:?}"
            );
        };
        let body_start = search_from + rel_start + "```bash".len();
        let Some(rel_end) = doc[body_start..].find("```") else {
            panic!("unterminated ```bash fence in docs/testing/chaos.md");
        };
        let body_end = body_start + rel_end;
        let block = &doc[body_start..body_end];
        if block.contains(needle) {
            return block;
        }
        search_from = body_end + 3;
    }
}

/// Drop every line whose trimmed content starts with `#` from a block of bash
/// source.
///
/// The doc's explanatory comment *about* the `chaos_tests::` filter contains
/// the literal string `` chaos_tests:: `` (as prose: "Scope the run to
/// `chaos_tests::`"), and separately the literal string
/// `HARVEST_TEST_DATABASE_URL` (as prose: "the same `HARVEST_TEST_DATABASE_URL`
/// database"). Checking a *whole* fenced block -- comments included -- for
/// either token is therefore satisfiable by the prose alone: a regression
/// that drops the filter from the actual command line, while leaving the
/// comment describing what the command is *supposed* to do untouched, would
/// pass an assertion that only inspects the raw block text. Stripping
/// comment lines first makes every check below examine what the reader would
/// actually copy-paste and run.
fn strip_comment_lines(block: &str) -> String {
    block
        .lines()
        .filter(|line| !line.trim_start().starts_with('#'))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Split already-comment-stripped command text into distinct commands, where
/// one or more blank lines separate one command from the next.
///
/// A backslash-continued multi-line invocation never contains a blank line
/// internally (the shell would treat one as ending the command anyway), so
/// splitting on blank-line runs cleanly separates the fenced block's several
/// independent `cargo test ...` invocations from one another.
fn split_into_commands(command_only: &str) -> Vec<String> {
    let mut commands = Vec::new();
    let mut current: Vec<&str> = Vec::new();
    for line in command_only.lines() {
        if line.trim().is_empty() {
            if !current.is_empty() {
                commands.push(current.join("\n"));
                current.clear();
            }
        } else {
            current.push(line);
        }
    }
    if !current.is_empty() {
        commands.push(current.join("\n"));
    }
    commands
}

/// The single command (from `split_into_commands`) containing `needle`.
///
/// Panics naming what was searched for on zero or more-than-one matches --
/// either would mean anchoring the check to the wrong (or an ambiguous)
/// command, silently defeating the whole point of splitting per-command in
/// the first place.
fn command_containing<'a>(commands: &'a [String], needle: &str) -> &'a str {
    let matches: Vec<&str> = commands
        .iter()
        .map(String::as_str)
        .filter(|c| c.contains(needle))
        .collect();
    match matches.as_slice() {
        [one] => one,
        [] => panic!(
            "no command (after stripping comments and splitting on blank \
             lines) contains {needle:?}; commands were:\n{commands:#?}"
        ),
        _ => panic!(
            "expected exactly one command to contain {needle:?}, found {}; \
             ambiguous which command the check should anchor to. commands \
             were:\n{commands:#?}",
            matches.len()
        ),
    }
}

#[test]
fn split_into_commands_separates_on_blank_lines_and_keeps_continuations_joined() {
    let text = "FOO=1 cargo test \\\n  --flag a\n\nBAR=2 cargo test \\\n  --flag b\n";
    let commands = split_into_commands(text);

    assert_eq!(commands.len(), 2, "commands were: {commands:#?}");
    assert_eq!(commands[0], "FOO=1 cargo test \\\n  --flag a");
    assert_eq!(commands[1], "BAR=2 cargo test \\\n  --flag b");
}

#[test]
fn command_containing_anchors_to_the_specific_command_not_the_aggregate() {
    // Reproduces the exact regression this helper exists to catch: a doc
    // block with two commands, where the FIRST (unrelated) command happens
    // to mention `chaos_tests::` while the SECOND (the one that actually
    // matters here) does not. A whole-block/aggregated-text check would
    // wrongly pass; anchoring to the specific command must fail.
    let commands = split_into_commands(
        "CHAOS_SEEDS=8 cargo test --test integration \\\n  chaos_tests::chaos_seeded_convergence_sweep\n\n\
         HARVEST_TEST_DATABASE_URL=postgres://x cargo test --test integration",
    );

    let local_db_command = command_containing(&commands, "HARVEST_TEST_DATABASE_URL");
    assert!(
        !local_db_command.contains("chaos_tests::"),
        "the HARVEST_TEST_DATABASE_URL command in this fixture deliberately \
         lacks the filter -- command:\n{local_db_command}"
    );
}

#[test]
#[should_panic(expected = "no command")]
fn command_containing_panics_when_the_needle_appears_in_no_command() {
    let commands = split_into_commands("cargo test --test integration\n");
    command_containing(&commands, "HARVEST_TEST_DATABASE_URL");
}

#[test]
fn strip_comment_lines_removes_comments_but_keeps_commands() {
    let block = "# a comment mentioning chaos_tests::\nreal_command --flag\n# another comment\nmore_command\n";
    let stripped = strip_comment_lines(block);

    assert!(
        !stripped.contains("chaos_tests::"),
        "comment-only content must not survive stripping; got:\n{stripped}"
    );
    assert!(
        stripped.contains("real_command --flag"),
        "actual command lines must survive stripping; got:\n{stripped}"
    );
    assert!(
        stripped.contains("more_command"),
        "actual command lines must survive stripping; got:\n{stripped}"
    );
}

#[test]
fn local_iteration_example_is_scoped_to_chaos_tests_module() {
    let doc = read_chaos_doc();
    let block = bash_block_containing(&doc, "HARVEST_TEST_DATABASE_URL");
    // Comments stripped: checking the raw block would also pass if only the
    // *comment describing* the filter survived while the command itself lost
    // it -- this doc's own explanatory comment happens to contain the
    // literal string `chaos_tests::` as prose, which would otherwise mask
    // exactly the regression this test exists to catch.
    let command_only = strip_comment_lines(block);
    let commands = split_into_commands(&command_only);
    // Anchor the check to the SPECIFIC command that runs against a
    // caller-supplied database, not the whole fenced block's aggregated
    // text. The block also contains an unrelated single-seed replay command;
    // checking the aggregate would still pass if THAT command happened to
    // mention `chaos_tests::` while the HARVEST_TEST_DATABASE_URL command
    // itself lost its filter -- exactly the unsafe-unscoped-command
    // regression this guard exists to catch.
    let local_db_command = command_containing(&commands, "HARVEST_TEST_DATABASE_URL");

    assert!(
        local_db_command.contains("chaos_tests::"),
        "docs/testing/chaos.md's HARVEST_TEST_DATABASE_URL COMMAND \
         specifically (not just some other command in the same fenced \
         block) must scope the run to `chaos_tests::` (the same filter \
         CI's own chaos.yml uses) -- an unscoped `cargo test ... \
         --test integration` invocation risks a chaos test's global TRUNCATE \
         scrub racing a concurrent, unrelated integration-test module \
         against the same shared database.\n\n\
         HARVEST_TEST_DATABASE_URL command:\n{local_db_command}\n\n\
         full block:\n{block}"
    );

    // The whole `integration` binary must never be recommended unscoped
    // against a shared, caller-supplied database -- guard against a
    // regression that drops the filter token while leaving the rest of the
    // invocation's shape untouched (e.g. a rewrite that renames the binary
    // flag but forgets to re-attach the module filter).
    assert!(
        local_db_command.contains("--test integration"),
        "expected the HARVEST_TEST_DATABASE_URL command specifically to \
         invoke the `integration` test binary; command:\n{local_db_command}\n\n\
         full block:\n{block}"
    );
}

/// The end index of `flag` in `command`, only at a real token boundary on
/// BOTH sides.
///
/// A plain substring search has two failure modes. It can match `flag`
/// as a prefix of a longer, different token: `--test integration` inside
/// `--test integration_tests`. It can also match `flag` glued onto a
/// preceding token: inside `test--test integration`. That second shape
/// is one malformed shell argument, not a real flag. Requires a
/// boundary on both sides of `flag`. Keeps searching past a false match
/// instead of accepting the first substring hit.
///
/// A lone `\` right before `flag` is not a real boundary. Bash only
/// treats `\` as a separator inside a `\` + newline continuation, never
/// alone. `cargo test\--test integration` is one glued argument, not a
/// real flag, even though a `\` sits right before it. The boundary
/// before `flag` must be plain whitespace, or the exact two-character
/// `\` + newline ending of a real continuation.
///
/// Panics if TWO OR MORE real occurrences exist, e.g. a `run:` script
/// that echoes the command before executing it. Picking the first one
/// would risk comparing against a stale logged command instead of the
/// one cargo actually runs.
///
/// A lone `\` right after `flag` is not a real boundary either. Bash
/// escapes the next character instead of separating words.
/// `--test integration\ chaos_tests::` glues `integration` and the rest
/// into ONE escaped-space argument, not a flag followed by a filter.
///
/// Known, accepted limitation: the multiple-occurrence check above
/// counts TEXTUAL matches, not executed ones. `echo ... --test
/// integration chaos_tests:: && cargo test --tests chaos_tests::specific`
/// has only one textual `--test integration`, inside the `echo`, so it
/// passes uncaught. Telling an echoed argument from an executed
/// command needs shell-command-boundary parsing (`&&`/`;`/`|`, plus
/// quoting). That is the same class of disproportionate scope as the
/// YAML-folding limitation in `skip_continuation_gap`. chaos.yml's
/// `run:` is a single, unchained command today.
fn find_flag_end(command: &str, flag: &str) -> Option<usize> {
    let before_ok = |prefix: &str| match prefix.chars().next_back() {
        None => true,
        Some('\n') => prefix.ends_with("\\\n"),
        Some(c) => c.is_whitespace(),
    };
    let after_ok = |suffix: &str| match suffix.chars().next() {
        None => true,
        Some('\\') => suffix.starts_with("\\\n"),
        Some(c) => c.is_whitespace(),
    };
    let mut search_from = 0;
    let mut found = None::<usize>;
    while let Some(rel) = command[search_from..].find(flag) {
        let start = search_from + rel;
        let end = start + flag.len();
        if before_ok(&command[..start]) && after_ok(&command[end..]) {
            assert!(
                found.is_none(),
                "multiple {flag:?} occurrences in command, ambiguous which \
                 is the real invocation: {command}"
            );
            found = Some(end);
        }
        search_from = end;
    }
    found
}

/// Skips horizontal whitespace and real `\` + newline continuations from
/// the start of `s`. Stops at a bare newline instead of skipping it.
///
/// A bare newline ends a shell command. If the doc's example ever drops
/// its continuation backslash, the real `cargo test` invocation runs
/// with no filter at all. Treating that newline as skippable whitespace
/// would let this guard extract a filter that could never actually run,
/// masking exactly that regression.
///
/// Known, accepted limitation: chaos.yml's `run:` value is a single
/// physical line today, never a folded YAML block scalar. A folded
/// `>-` scalar would fold its source newlines into spaces before the
/// shell ever runs it. This function cannot see that folding -- it
/// only sees the raw file text. A future folded `run:` would panic
/// here even though the folded command is valid. That is an accepted
/// trade-off: failing loud on an unsupported format beats silently
/// accepting a broken one.
fn skip_continuation_gap(s: &str) -> &str {
    let mut rest = s;
    loop {
        if let Some(next) = rest.strip_prefix(|c: char| c == ' ' || c == '\t') {
            rest = next;
        } else if let Some(next) = rest.strip_prefix("\\\n") {
            rest = next;
        } else {
            return rest;
        }
    }
}

/// The exact filter token following `--test integration` in a command.
///
/// Skips whitespace and backslash continuations between the flag and its
/// argument. The doc wraps its filter onto the next line after one such
/// continuation. This is a token boundary, not a prefix: `chaos_tests::`
/// and `chaos_tests::chaos_seeded_convergence_sweep` extract as different,
/// unequal tokens. A `contains`/prefix check cannot tell them apart.
///
/// Reads the FIRST `--test integration` in `command`. Every call site here
/// passes an already-isolated single command or run line, so one match is
/// the only one expected.
///
/// `cargo test --help` documents `[OPTIONS] [TESTNAME]`: some cargo option
/// could sit between the flag and the filter. This guard does not try to
/// parse cargo options. Some take a value token and some do not, and
/// guessing wrong risks the exact silent mismatch this issue closed. It
/// requires the filter immediately after the flag and panics on anything
/// else, naming the offending token.
///
/// `command.find` alone would match `--test integration` as a prefix of
/// a longer, different target name, e.g. `--test integration_tests`.
/// Requires a token boundary (whitespace, `\`, or end of string) right
/// after `FLAG`, so a same-prefix target name is never mistaken for it.
fn extract_filter_argument(command: &str) -> &str {
    const FLAG: &str = "--test integration";
    let after_flag = find_flag_end(command, FLAG).map_or_else(
        || panic!("command has no {FLAG:?}: {command}"),
        |end| &command[end..],
    );
    let arg_start = skip_continuation_gap(after_flag);
    let arg_end = arg_start
        .find(char::is_whitespace)
        .unwrap_or(arg_start.len());
    let token = &arg_start[..arg_end];
    assert!(
        !token.is_empty(),
        "no filter argument follows {FLAG:?} in command: {command}"
    );
    assert!(
        !token.starts_with('-'),
        "found a cargo option ({token:?}) between the flag and filter -- \
         this guard requires the filter immediately after {FLAG:?}. \
         command: {command}"
    );
    token
}

#[test]
fn extract_filter_argument_reads_the_token_on_the_same_line() {
    let command = "cargo test -p autumn-harvest --features chaos --test integration chaos_tests:: -- --nocapture --test-threads=1";
    assert_eq!(extract_filter_argument(command), "chaos_tests::");
}

#[test]
fn extract_filter_argument_reads_the_token_past_a_line_continuation() {
    // Mirrors the doc's own layout. Its filter sits on the next line,
    // after a backslash continuation, not on the flag's own line.
    let command = "HARVEST_TEST_DATABASE_URL=postgres://x \\\n  CHAOS_SEEDS=8 cargo test --features chaos --test integration \\\n  chaos_tests::";
    assert_eq!(extract_filter_argument(command), "chaos_tests::");
}

#[test]
#[should_panic(expected = "no filter argument follows")]
fn extract_filter_argument_panics_on_a_bare_newline_with_no_continuation_backslash() {
    // Codex finding on PR #1474: a bare newline ends a shell command. If
    // the doc's example ever drops its continuation backslash, the real
    // invocation runs unfiltered. This guard must not treat that dropped
    // backslash as ordinary whitespace and quietly extract a filter that
    // could never actually run.
    let command = "cargo test --features chaos --test integration \n  chaos_tests::";
    extract_filter_argument(command);
}

#[test]
#[should_panic(expected = "command has no")]
fn extract_filter_argument_requires_a_boundary_before_the_flag() {
    // Codex finding on PR #1474: a missing space could glue the flag
    // onto a preceding token, e.g. `test--test integration`. That is
    // one malformed shell argument, not a real `--test` flag. The guard
    // must not accept it just because a boundary follows the match.
    extract_filter_argument("cargo test--test integration chaos_tests::");
}

#[test]
#[should_panic(expected = "command has no")]
fn extract_filter_argument_rejects_a_lone_backslash_glued_to_the_flag() {
    // Codex finding on PR #1474: if a doc continuation after `cargo
    // test\` loses only its trailing newline, the source becomes `cargo
    // test\--test integration ...`. Bash never treats a lone `\` as a
    // separator, only `\` immediately followed by a newline. The guard
    // must not accept the stray backslash as a boundary either.
    extract_filter_argument("cargo test\\--test integration chaos_tests::");
}

#[test]
#[should_panic(expected = "cargo option")]
fn extract_filter_argument_panics_when_the_flag_has_no_following_token() {
    // `--no-run` stands where the filter should be. It is itself a cargo
    // option, not a test name. The guard treats it the same as any other
    // option found there.
    extract_filter_argument("cargo test --test integration --no-run");
}

#[test]
#[should_panic(expected = "no filter argument follows")]
fn extract_filter_argument_panics_when_nothing_follows_the_flag() {
    extract_filter_argument("cargo test --test integration");
}

#[test]
#[should_panic(expected = "command has no")]
fn extract_filter_argument_ignores_a_same_prefix_longer_target_name() {
    // Codex finding on PR #1474: `--test integration` is a substring of
    // `--test integration_tests`, a different, valid cargo target name.
    // A plain `command.find` would match that prefix and misread
    // `_tests` as the filter. The guard requires a token boundary, so no
    // real `--test integration` flag is found here.
    extract_filter_argument("cargo test --test integration_tests chaos_tests::");
}

#[test]
#[should_panic(expected = "multiple")]
fn extract_filter_argument_panics_on_multiple_flag_occurrences() {
    // Codex finding on PR #1474: a run: script could echo the command
    // before executing it. That gives two boundary-valid occurrences of
    // the flag. Picking the first would risk comparing against a stale
    // logged command, not the real one cargo runs. The guard must fail
    // loudly on the ambiguity instead.
    extract_filter_argument(
        "echo cargo test --test integration chaos_tests:: && cargo test --test integration chaos_tests::specific",
    );
}

#[test]
#[should_panic(expected = "command has no")]
fn extract_filter_argument_rejects_a_lone_backslash_after_the_flag() {
    // Codex finding on PR #1474: bash escapes the space after a lone
    // `\`. `--test integration\ chaos_tests::specific` is ONE argument
    // to cargo, not a flag followed by a filter. The guard must not
    // treat that `\` as a boundary just because `skip_continuation_gap`
    // never consumed it.
    extract_filter_argument("cargo test --test integration\\ chaos_tests::specific");
}

#[test]
#[should_panic(expected = "command has no")]
fn extract_filter_argument_panics_when_the_flag_is_absent() {
    extract_filter_argument("cargo test --lib chaos::");
}

#[test]
#[should_panic(expected = "cargo option")]
fn extract_filter_argument_panics_on_a_valueless_cargo_option_before_the_filter() {
    // First Codex finding on PR #1474: `cargo test --help` documents
    // `[OPTIONS] [TESTNAME]`. A short option, e.g. `-q`, is valid between
    // `--test integration` and the filter. Fail loudly here rather than
    // guess past it -- see the value-taking-option test below for why
    // guessing cannot be made complete.
    extract_filter_argument("cargo test --test integration -q chaos_tests::specific");
}

#[test]
#[should_panic(expected = "cargo option")]
fn extract_filter_argument_panics_on_a_value_taking_cargo_option_before_the_filter() {
    // Second Codex finding: `--color always` is a value-taking option in
    // the same documented position. A version that skips single tokens
    // starting with `-` would skip `--color`. It would then wrongly
    // return `always` -- the option's VALUE -- as the filter.
    // Enumerating which options take a value is a losing game. This
    // guard refuses to play it -- it fails loudly on any option here.
    extract_filter_argument("cargo test --test integration --color always chaos_tests::specific");
}

#[test]
fn extract_filter_argument_detects_a_ci_narrowed_filter_the_doc_still_calls_broad() {
    // Reproduces issue #1224 directly: CI narrows to a specific test, but
    // the doc's worked example keeps the broader module filter. A
    // `contains` check on each side independently would pass both. Only
    // an exact-equality comparison catches the divergence.
    let ci_command =
        "cargo test --test integration chaos_tests::chaos_seeded_convergence_sweep -- --nocapture";
    let doc_command = "cargo test --features chaos --test integration \\\n  chaos_tests::";

    let ci_filter = extract_filter_argument(ci_command);
    let doc_filter = extract_filter_argument(doc_command);

    assert!(
        ci_command.contains("chaos_tests::") && doc_command.contains("chaos_tests::"),
        "both commands satisfy the old, insufficient prefix check"
    );
    assert_ne!(
        ci_filter, doc_filter,
        "a narrowed CI filter must be distinguishable from the doc's \
         broader one, not masked by a shared prefix"
    );
}

#[test]
fn extract_filter_argument_detects_a_doc_narrowed_filter_ci_still_calls_broad() {
    // The reverse direction: the doc narrows while CI stays broad.
    let ci_command = "cargo test --test integration chaos_tests:: -- --nocapture";
    let doc_command = "cargo test --features chaos --test integration \\\n  chaos_tests::chaos_seeded_convergence_sweep";

    let ci_filter = extract_filter_argument(ci_command);
    let doc_filter = extract_filter_argument(doc_command);

    assert_ne!(
        ci_filter, doc_filter,
        "a narrowed doc filter must be distinguishable from CI's broader \
         one, not masked by a shared prefix"
    );
}

/// Ties the doc's example filter to CI's own filter, so the two can never
/// silently diverge -- e.g. CI narrows to a smaller sub-filter while the doc
/// keeps recommending the wider (now-stale) one it claims to mirror.
///
/// Compares the exact filter *argument* extracted from each side, not a
/// shared-prefix `contains` check -- see `extract_filter_argument`.
#[test]
fn doc_example_filter_matches_chaos_workflow_filter() {
    // Stripped the same way as the doc side (below). A `#` comment in
    // chaos.yml could name `chaos_tests::` as prose. Left unstripped, it
    // could mis-anchor `workflow_step_stanza` to the wrong step.
    let workflow = strip_comment_lines(&read_normalized(
        &repo_root().join(".github/workflows/chaos.yml"),
    ));
    let ci_stanza = workflow_step_stanza(&workflow, "chaos_tests::").expect(
        ".github/workflows/chaos.yml must have a step running the \
         chaos_tests:: suite",
    );
    // Anchor to the `run:` value specifically, not a single
    // `.lines().find(...)` match and not the whole stanza. A `run:`
    // command can wrap across lines: a `\` continuation, or a folded
    // `>-` block. A single-line match would lose the filter to that
    // break. The doc's own example wraps its filter the same way. The
    // whole stanza also includes the step's `- name:` line, which could
    // itself contain look-alike prose -- see `run_command`.
    let ci_filter = extract_filter_argument(run_command(ci_stanza));

    let doc = read_chaos_doc();
    let block = bash_block_containing(&doc, "HARVEST_TEST_DATABASE_URL");
    let command_only = strip_comment_lines(block);
    let commands = split_into_commands(&command_only);
    let local_db_command = command_containing(&commands, "HARVEST_TEST_DATABASE_URL");
    let doc_filter = extract_filter_argument(local_db_command);

    assert!(
        doc_filter.starts_with("chaos_tests::") && ci_filter.starts_with("chaos_tests::"),
        "both filters must target the chaos_tests module. Equality alone \
         is not enough: if both sides drifted to the same wrong token \
         that merely contains `chaos_tests::` as a substring (e.g. \
         `nonchaos_tests::`), cargo would match zero tests on both sides \
         and the equality check would still pass -- doc filter: \
         {doc_filter:?}, CI filter: {ci_filter:?}"
    );
    assert_eq!(
        doc_filter, ci_filter,
        "docs/testing/chaos.md's local-iteration example must use the \
         exact same filter argument as .github/workflows/chaos.yml, not \
         merely share a `chaos_tests::` prefix -- doc filter: \
         {doc_filter:?}, CI filter: {ci_filter:?}"
    );
}

#[test]
fn doc_example_filter_matches_chaos_workflow_filter_tolerates_a_wrapped_ci_run_line() {
    // Codex finding on PR #1474: an earlier version read only the single
    // physical line containing `--test integration` out of the CI
    // stanza. If chaos.yml ever wraps its filter onto a continuation
    // line, that single line has no filter on it. The doc's own example
    // already does this. The guard panicked even though the real
    // filters still matched. Passing the whole stanza fixes this.
    let workflow = "\n      - name: Run chaos reproducers\n        run: cargo test --features chaos --test integration \\\n          chaos_tests::\n";
    let ci_stanza = workflow_step_stanza(workflow, "chaos_tests::").expect("stanza present");
    assert_eq!(
        extract_filter_argument(run_command(ci_stanza)),
        "chaos_tests::"
    );
}

#[test]
fn doc_and_ci_extraction_pipeline_detects_divergence_end_to_end() {
    // Runs the exact same pipeline as `doc_example_filter_matches_chaos_workflow_filter`
    // above, on synthetic text standing in for chaos.yml and chaos.md. Proves
    // the GUARD catches a divergence, not just `extract_filter_argument` in
    // isolation.
    let workflow = "\n      - uses: actions/checkout@v4\n      - name: Run chaos reproducers\n        run: cargo test --features chaos --test integration chaos_tests::chaos_seeded_convergence_sweep -- --nocapture\n      - name: A later step\n        run: echo later\n";
    let ci_stanza = workflow_step_stanza(workflow, "chaos_tests::").expect("stanza present");
    let ci_filter = extract_filter_argument(run_command(ci_stanza));

    let doc = "```bash\n# Scope the run to `chaos_tests::`:\nHARVEST_TEST_DATABASE_URL=postgres://x \\\n  CHAOS_SEEDS=8 cargo test --features chaos --test integration \\\n  chaos_tests::\n```\n";
    let block = bash_block_containing(doc, "HARVEST_TEST_DATABASE_URL");
    let command_only = strip_comment_lines(block);
    let commands = split_into_commands(&command_only);
    let local_db_command = command_containing(&commands, "HARVEST_TEST_DATABASE_URL");
    let doc_filter = extract_filter_argument(local_db_command);

    assert_ne!(
        doc_filter, ci_filter,
        "a CI filter narrowed to a specific test must be distinguishable \
         from the doc's broader one, through the full extraction pipeline, \
         not just the extract_filter_argument helper alone"
    );
}

#[test]
#[should_panic(expected = "chaos_tests module")]
fn doc_and_ci_extraction_pipeline_rejects_a_shared_wrong_module_drift() {
    // Codex finding on PR #1474: both sides could drift to the SAME
    // wrong token. It only needs to contain `chaos_tests::` as a
    // substring, e.g. `nonchaos_tests::`. The two extracted filters
    // would then be equal to each other. Equality alone would pass,
    // even though cargo matches zero real tests on either side.
    // Reproduces the same module-prefix check
    // `doc_example_filter_matches_chaos_workflow_filter` runs, on
    // synthetic text standing in for this drift.
    let workflow = "\n      - name: Run chaos reproducers\n        run: cargo test --features chaos --test integration nonchaos_tests:: -- --nocapture\n";
    let ci_stanza = workflow_step_stanza(workflow, "chaos_tests::").expect("stanza present");
    let ci_filter = extract_filter_argument(run_command(ci_stanza));

    let doc = "```bash\nHARVEST_TEST_DATABASE_URL=postgres://x \\\n  cargo test --features chaos --test integration \\\n  nonchaos_tests::\n```\n";
    let block = bash_block_containing(doc, "HARVEST_TEST_DATABASE_URL");
    let command_only = strip_comment_lines(block);
    let commands = split_into_commands(&command_only);
    let local_db_command = command_containing(&commands, "HARVEST_TEST_DATABASE_URL");
    let doc_filter = extract_filter_argument(local_db_command);

    assert_eq!(
        doc_filter, ci_filter,
        "the equality check alone deliberately passes here -- that is \
         the whole point of this fixture"
    );
    assert!(
        doc_filter.starts_with("chaos_tests::") && ci_filter.starts_with("chaos_tests::"),
        "both filters must target the chaos_tests module -- doc filter: \
         {doc_filter:?}, CI filter: {ci_filter:?}"
    );
}

/// The **whole** workflow step stanza containing `needle`. Starts at
/// its own `- ` list-item marker. Ends at the line before the next
/// step's `- ` marker, or the end of the block.
///
/// Bounding at the *next* step rather than at `needle` is the load-bearing
/// part. Slicing only up to the matched text sits inside the step's `run:`
/// line, so a key written *after* `run:` (e.g. an `if:` placed on the line
/// below, which is valid YAML with identical semantics to placing it above)
/// is invisible to the check -- ported verbatim from
/// `sqlite_feasibility_docs::workflow_step_stanza`, which exists precisely
/// because that exact reordering once silently re-gated a doc guard while its
/// own "is this unconditional?" test kept reporting success.
///
/// Panics if a SECOND step also contains `needle`. A compile-only step
/// could be scoped with the same filter text as the step that really
/// runs it. Silently picking the first match risks anchoring parity to
/// the wrong step.
///
/// `name:` is optional on a GitHub Actions step -- chaos.yml's own
/// `checkout`/`rust-toolchain`/`rust-cache` steps have none. The marker
/// matches any step list item, not only named ones. An unnamed step
/// now opens its own stanza. It no longer merges silently into the
/// previous named one.
fn workflow_step_stanza<'a>(block: &'a str, needle: &str) -> Option<&'a str> {
    const STEP: &str = "\n      - ";
    let at = block.find(needle)?;
    let start = block[..at].rfind(STEP).unwrap_or(0);
    // Search for the next step from just past this stanza's own `- `, so
    // the marker we started from is not rediscovered as the terminator.
    let after_marker = start + STEP.len();
    let end = block[after_marker..]
        .find(STEP)
        .map_or(block.len(), |rel| after_marker + rel);
    assert!(
        !block[end..].contains(needle),
        "multiple steps contain {needle:?}, ambiguous which is the real \
         one: {block}"
    );
    Some(&block[start..end])
}

#[test]
fn step_stanza_covers_keys_written_after_run() {
    // `if:` placed *below* `run:` -- valid YAML, identical semantics to
    // placing it above, and invisible to a slice that stops at the `run:`
    // line.
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
        "the stanza must stop at the next step, not swallow it -- otherwise \
         an unrelated neighbour's `if:` would raise a false alarm. \
         Stanza:\n{stanza}"
    );
    assert!(
        !stanza.contains("Some earlier step"),
        "the stanza must start at its own `- name:`, not an earlier step's. \
         Stanza:\n{stanza}"
    );
}

#[test]
#[should_panic(expected = "multiple")]
fn workflow_step_stanza_panics_when_the_needle_appears_in_two_steps() {
    // Codex finding on PR #1474: a compile-only step could be scoped
    // with the same filter text as the step that actually executes it.
    // For example, `--test integration chaos_tests:: --no-run`. Picking
    // the FIRST stanza containing the needle would then silently anchor
    // parity to the compile step, not the one CI really runs.
    let block = "\n      - name: Compile chaos suite\n        run: cargo test --test integration chaos_tests:: --no-run\n\n      - name: Run chaos reproducers\n        run: cargo test --test integration chaos_tests::specific\n";
    workflow_step_stanza(block, "chaos_tests::");
}

#[test]
#[should_panic(expected = "multiple")]
fn workflow_step_stanza_treats_an_unnamed_step_as_its_own_boundary() {
    // Codex finding on PR #1474: a step without `name:` is still a real
    // step -- chaos.yml's own checkout/toolchain/cache steps have none.
    // The old marker only recognized named steps, so an unnamed
    // execution step's content stayed inside the PRECEDING named
    // step's stanza. That silently bypassed the multiple-steps check
    // above: the two occurrences looked like one, inside one (wrongly
    // bounded) stanza.
    let block = "\n      - name: Compile chaos suite\n        run: cargo test --test integration chaos_tests:: --no-run\n      - run: cargo test --test integration chaos_tests::specific\n";
    workflow_step_stanza(block, "chaos_tests::");
}

/// The step stanza's `run:` value. Starts at the `run:` key. Ends at
/// the line before the next sibling key at the same indentation, or at
/// the end of the stanza.
///
/// A step's `- name:` line can itself contain text that reads like
/// `--test integration <filter>`, e.g. a name describing the check the
/// step runs. `extract_filter_argument` matches the FIRST occurrence.
/// Passing the whole stanza risks matching that prose instead of the
/// real command. That could mask a real divergence if the `run:` line
/// has since narrowed. Anchoring to `run:` specifically rules that out.
///
/// The end bound matters too: a sibling key after `run:` (`env:`,
/// `if:`, ...) is not part of the run value. A more-indented line is a
/// continuation of `run:`'s own value. A line at the same or shallower
/// indentation is the next key. It ends the slice.
fn run_command(stanza: &str) -> &str {
    let mut offset = 0;
    let mut run: Option<(usize, usize)> = None;
    for line in stanza.split_inclusive('\n') {
        let trimmed = line.trim_start();
        let indent = line.len() - trimmed.len();
        match run {
            None => {
                if trimmed.starts_with("run:") {
                    run = Some((offset + indent, indent));
                }
            }
            Some((start, run_indent)) => {
                if !trimmed.trim_end().is_empty() && indent <= run_indent {
                    return &stanza[start..offset];
                }
            }
        }
        offset += line.len();
    }
    match run {
        Some((start, _)) => &stanza[start..],
        None => panic!("stanza has no `run:` key: {stanza}"),
    }
}

#[test]
fn run_command_skips_a_look_alike_filter_in_the_step_name() {
    // Codex finding on PR #1474: a step name mentioning `--test
    // integration chaos_tests::` as prose must not be mistaken for the
    // real command.
    let stanza = "\n      - name: Run --test integration chaos_tests:: suite\n        run: cargo test --features chaos --test integration chaos_tests::specific\n";
    assert_eq!(
        extract_filter_argument(run_command(stanza)),
        "chaos_tests::specific"
    );
}

#[test]
#[should_panic(expected = "command has no")]
fn run_command_excludes_a_sibling_key_after_run() {
    // Codex finding on PR #1474: a sibling key after `run:` (here
    // `env:`) is not part of the run value. That holds even if its text
    // reads like a broader `--test integration` filter. The real run:
    // command uses an unsupported target name here
    // (`integration_tests`). It must fail loudly on that, not silently
    // succeed by reading past it into the env: block.
    let stanza = "\n      - name: Run chaos reproducers\n        run: cargo test --features chaos --test integration_tests chaos_tests::specific\n        env:\n          NOTE: \"--test integration chaos_tests::\"\n";
    extract_filter_argument(run_command(stanza));
}

/// This module's own guards must run on a docs-only PR, where the `test`
/// matrix (gated on `changes.outputs.code`) is skipped entirely (`chaos.md`
/// lives under `docs/`, so editing only it sets `code=false`). They must
/// therefore be invoked from the ungated `lint` job, exactly like the
/// `performance_docs` / `sqlite_feasibility_docs` precedents this module
/// follows -- and, like both of those, must also be *unconditional* once
/// there: a step re-gated behind an `if:` is functionally identical to no
/// step at all on a docs-only PR, and only checking "does the step exist" /
/// "is it in `lint`" would pass unchanged if one were added.
#[test]
fn chaos_docs_guards_run_on_docs_only_changes() {
    const FILTER: &str = "--test integration chaos_docs::";
    let workflow = read_normalized(&repo_root().join(".github/workflows/ci.yml"));

    let step = workflow.lines().find(|line| line.contains(FILTER)).expect(
        "ci.yml must run the chaos_docs guards from a step that is not \
         gated on `changes.outputs.code` -- a docs-only PR, the change \
         class these guards exist for, skips the entire `test` matrix \
         entirely",
    );
    assert!(
        step.trim_start().starts_with("run:"),
        "expected the guard invocation to be a step `run:` line, found: {step}"
    );

    // It must live in `lint`, the ungated job. `test` is gated per-step on
    // `changes.outputs.code`, so a step there proves nothing for docs-only
    // PRs.
    let lint_start = workflow
        .find("\n  lint:")
        .expect("ci.yml must define a `lint` job");
    let test_start = workflow
        .find("\n  test:")
        .expect("ci.yml must define a `test` job");
    let step_at = workflow.find(FILTER).expect("located above");
    assert!(
        step_at > lint_start && step_at < test_start,
        "the chaos_docs guard step must live in the ungated `lint` job; a \
         step in the `test` matrix is gated on `changes.outputs.code` and so \
         does not run on a docs-only PR"
    );

    // And it must be unconditional. A step that grew an `if:` is back behind
    // a gate -- which is the exact regression this test exists to prevent.
    // (Empirically confirmed exploitable without this check: adding
    // `if: needs.changes.outputs.code == 'true'` to the step left the three
    // assertions above passing unchanged.)
    let block = &workflow[lint_start..test_start];
    let stanza = workflow_step_stanza(block, FILTER)
        .expect("the guard step is inside the lint block, located above");
    assert!(
        !stanza.contains("\n        if:"),
        "the chaos_docs guard step has acquired an `if:` condition. It must \
         run unconditionally: a condition is how guards like these stopped \
         running on docs-only PRs in the first place. Stanza:\n{stanza}"
    );
}
