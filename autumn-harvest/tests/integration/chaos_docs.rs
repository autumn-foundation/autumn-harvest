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

/// Ties the doc's example filter to CI's own filter, so the two can never
/// silently diverge -- e.g. CI narrows to a smaller sub-filter while the doc
/// keeps recommending the wider (now-stale) one it claims to mirror.
#[test]
fn doc_example_filter_matches_chaos_workflow_filter() {
    let workflow = read_normalized(&repo_root().join(".github/workflows/chaos.yml"));
    assert!(
        workflow.contains("--test integration chaos_tests::"),
        ".github/workflows/chaos.yml must run the DB-backed chaos suite \
         scoped to `chaos_tests::` -- this is the exact filter \
         docs/testing/chaos.md's local-iteration example claims to mirror"
    );
}

/// The **whole** workflow step stanza containing `needle`, from its `- name:`
/// through to the line before the next step's `- name:` (or the end of the
/// block).
///
/// Bounding at the *next* step rather than at `needle` is the load-bearing
/// part. Slicing only up to the matched text sits inside the step's `run:`
/// line, so a key written *after* `run:` (e.g. an `if:` placed on the line
/// below, which is valid YAML with identical semantics to placing it above)
/// is invisible to the check -- ported verbatim from
/// `sqlite_feasibility_docs::workflow_step_stanza`, which exists precisely
/// because that exact reordering once silently re-gated a doc guard while its
/// own "is this unconditional?" test kept reporting success.
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
