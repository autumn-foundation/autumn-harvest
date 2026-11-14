//! The fixtures of `--self-test`, as data.
//!
//! The Python original held these cases as lists. They are generated from
//! those lists, so each case keeps its exact source text, expected result
//! and name.

/// A lexer case: Rust source, the comment texts it holds, and a name.
pub struct LexerCase {
    pub source: &'static str,
    pub expected: &'static [&'static str],
    pub name: &'static str,
}

/// A rule case: Rust source, the expected `(rule, line)` set, and a name.
pub struct RuleCase {
    pub source: &'static str,
    pub expected: &'static [(&'static str, usize)],
    pub name: &'static str,
}

/// The forms the lexer must get right.
pub const SELF_TESTS: &[LexerCase] = &[
    LexerCase {
        source: "let n = 1; // TODO: fix\n",
        expected: &["TODO: fix"],
        name: "trailing line comment",
    },
    LexerCase {
        source: "/* TODO: remove */\n",
        expected: &["TODO: remove"],
        name: "block comment",
    },
    LexerCase {
        source: "let s = r#\"\n// TODO: not a comment\n\"#;\n",
        expected: &[],
        name: "raw string contents",
    },
    LexerCase {
        source: "let s = \"// also not a comment\";\n",
        expected: &[],
        name: "string contents",
    },
    LexerCase {
        source: "// real\nlet s = \"not // this\";\n",
        expected: &["real"],
        name: "comment then string",
    },
    LexerCase {
        source: "/* outer /* nested */ still outer */ x();\n",
        expected: &["outer", "nested", "still outer"],
        name: "nested block yields the inner body as its own piece",
    },
    LexerCase {
        source: "let c = '\\''; // after char lit\n",
        expected: &["after char lit"],
        name: "escaped char literal",
    },
    LexerCase {
        source: "fn f<'a>(x: &'a str) {} // after lifetime\n",
        expected: &["after lifetime"],
        name: "lifetime tick",
    },
    LexerCase {
        source: "let s = \"esc \\\" // no\"; // yes\n",
        expected: &["yes"],
        name: "escaped quote",
    },
    LexerCase {
        source: "/// doc\n//! inner\n",
        expected: &["doc", "inner"],
        name: "doc markers",
    },
    LexerCase {
        source: "let s = br#\"// bytes\"#; // trailing\n",
        expected: &["trailing"],
        name: "byte raw string",
    },
    LexerCase {
        source: "let s = r##\"a \"# b\"##; // hashes\n",
        expected: &["hashes"],
        name: "multi-hash raw string",
    },
    LexerCase {
        source: "let e = \"a \\\n b\"; // after continuation\n",
        expected: &["after continuation"],
        name: "line continuation",
    },
];

/// Rule-level fixtures.
///
/// These pin behavior that the lexer alone cannot express: how comment runs
/// bound fence state and prose units.
pub const RULE_TESTS: &[RuleCase] = &[
    RuleCase {
        source: "/// Example:\n/// ```rust\n/// let x = 1;\npub fn a() {}\n\n// TODO: issue required\n// let stale = compute();\npub fn b() {}\n",
        expected: &[("CH001", 7), ("CH002", 6)],
        name: "an unclosed fence does not leak past its own block",
    },
    RuleCase {
        source: "/// Example:\n/// ```rust\n/// let x = compute();\n/// ```\npub fn a() {}\n",
        expected: &[],
        name: "fenced example code stays exempt",
    },
    RuleCase {
        source: "//// let stale = compute();\n",
        expected: &[("CH001", 1)],
        name: "//// is an ordinary comment, not a doc comment",
    },
    RuleCase {
        source: "// fn resolve_call(the caller name appears in diagnostics\n",
        expected: &[],
        name: "prose after an open paren is not a signature",
    },
    RuleCase {
        source: "// fn foo(\n//     a: u8,\n// ) {}\n",
        expected: &[("CH001", 1)],
        name: "a wrapped commented-out signature is caught on its opening line",
    },
    RuleCase {
        source: "/// ```rust\n// TODO: issue required\n",
        expected: &[("CH002", 2)],
        name: "a change of comment marker ends the run",
    },
    RuleCase {
        source: "/* outer /* let stale = compute(); */ */\n",
        expected: &[("CH001", 1)],
        name: "a nested block comment body is inspected",
    },
    RuleCase {
        source: "/// ```rust\n/// ~~~\n/// TODO: fixture placeholder\n/// ```\n",
        expected: &[],
        name: "a ~~~ line inside a ``` fence is content, not a closer",
    },
    RuleCase {
        source: "/// ````rust\n/// ```\n/// TODO: fixture placeholder\n/// ````\n",
        expected: &[],
        name: "a short closer does not close a longer fence",
    },
    RuleCase {
        source: "/// - Example:\n///\n///     ```rust\n///     TODO: fixture placeholder\n///     ```\n",
        expected: &[],
        name: "a fence indented inside a list item is still a fence",
    },
    RuleCase {
        source: "///     ```rust\n/// TODO: issue required\n",
        expected: &[("CH002", 2)],
        name: "four spaces is an indented code line, not a fence opener",
    },
    RuleCase {
        source: "///    ```rust\n///    let x = compute();\n///    ```\n",
        expected: &[],
        name: "three spaces still opens a fence",
    },
    RuleCase {
        source: "/// ```rust\n///     ```\n/// TODO: fixture placeholder\n/// ```\n",
        expected: &[],
        name: "an over-indented ``` inside a fence is content, not a closer",
    },
    RuleCase {
        source: "/// - ```rust\n///   TODO: fixture placeholder\n///   ```\n",
        expected: &[],
        name: "a fence opens on its own list-marker line",
    },
    RuleCase {
        source: "/// - ```rust\n///   let x = compute();\n///   ```\n/// TODO: issue required\n",
        expected: &[("CH002", 4)],
        name: "a fence opened on a list-marker line still closes",
    },
    RuleCase {
        source: "/// > ~~~rust\n/// > TODO: fixture placeholder\n/// > ~~~\n",
        expected: &[],
        name: "a fence inside a block quote is a fence",
    },
    RuleCase {
        source: "/// > > ```rust\n/// > > TODO: fixture placeholder\n/// > > ```\n",
        expected: &[],
        name: "so is one inside a nested block quote",
    },
    RuleCase {
        source: "/// > - Example:\n/// >\n/// >     ```rust\n/// >     TODO: fixture placeholder\n/// >     ```\n",
        expected: &[],
        name: "a quoted list item still opens a container for its fence",
    },
    RuleCase {
        source: "/// > ```rust\n/// > let x = compute();\n/// > ```\n/// > TODO: issue required\n",
        expected: &[("CH002", 4)],
        name: "a quoted fence closes, and quoted prose after it is scanned",
    },
    RuleCase {
        source: "/// > TODO: fix this\n",
        expected: &[("CH002", 1)],
        name: "a quote marker does not exempt the line it carries",
    },
    RuleCase {
        source: "/// 1) alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu nu\n/// 2) xi omicron pi rho sigma tau upsilon phi chi psi omega alpha beta\n",
        expected: &[],
        name: "parenthesized ordered-list items are separate sentences",
    },
    RuleCase {
        source: "// --------------------\n// word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25.\n",
        expected: &[],
        name: "a thematic break is not a word of the sentence below it",
    },
    RuleCase {
        source: "// It pins branch B (index\n// 1) as word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20.\n",
        expected: &[("CH007", 1)],
        name: "a wrapped \"1)\" closing a paren does not split the sentence",
    },
    RuleCase {
        source: "/// -     ```rust\n/// TODO: issue required\n",
        expected: &[("CH002", 2)],
        name: "five spaces after a list marker is indented code, not a fence",
    },
    RuleCase {
        source: "/// - \t\t~~~rust\n/// TODO: issue required\n",
        expected: &[("CH002", 2)],
        name: "list padding is counted in columns, so tabs cannot smuggle a fence",
    },
    RuleCase {
        source: "///     - ~~~rust\n/// TODO: issue required\n",
        expected: &[("CH002", 2)],
        name: "four spaces before a top-level marker is indented code, not a list",
    },
    RuleCase {
        source: "///    - ~~~rust\n///      TODO: fixture placeholder\n///      ~~~\n",
        expected: &[],
        name: "three spaces before a marker is still a list",
    },
    RuleCase {
        source: "/// - ~~~rust\n///   let x = 1;\n/// TODO: issue required\n",
        expected: &[("CH002", 3)],
        name: "an unclosed fence ends when its list item does",
    },
    RuleCase {
        source: "/// > ~~~rust\n/// > let x = 1;\n/// TODO: issue required\n",
        expected: &[("CH002", 3)],
        name: "an unclosed fence ends when its block quote does",
    },
    RuleCase {
        source: "/// - ~~~rust\n///\n///   TODO: fixture placeholder\n///   ~~~\n",
        expected: &[],
        name: "a blank line does not end the list item a fence sits in",
    },
    RuleCase {
        source: "/// -   ~~~rust\n/// \tTODO: fixture placeholder\n/// \t~~~\n",
        expected: &[],
        name: "a tab indenting a fence body is measured in columns, not characters",
    },
    RuleCase {
        source: "/// - outer\n///   - inner\n///   ~~~rust\n///   let x = 1;\n/// TODO: issue required\n",
        expected: &[("CH002", 5)],
        name: "leaving a nested list item lands in its parent, not at top level",
    },
    RuleCase {
        source: "/// - outer\n///   - inner\n///     ~~~rust\n///     TODO: fixture\n///     ~~~\n",
        expected: &[],
        name: "a fence inside a nested list item is still a fence",
    },
    RuleCase {
        source: "/// > - quoted item\n///\n///     ~~~rust\n/// TODO: issue required\n",
        expected: &[("CH002", 4)],
        name: "a list container does not outlive the block quote it opened in",
    },
    RuleCase {
        source: "/// - outer paragraph\n///   2. still the same paragraph\n///      ~~~rust\n///   TODO: fixture placeholder\n///      ~~~\n",
        expected: &[],
        name: "a mid-paragraph \"2.\" does not open a nested container",
    },
    RuleCase {
        source: "/// 1. first item\n/// 2. second item\n///    ~~~rust\n///    TODO: fixture placeholder\n///    ~~~\n",
        expected: &[],
        name: "but \"2.\" after \"1.\" still opens its own item",
    },
    RuleCase {
        source: "/// ~~~rust\n/// let x = 1;\n/// ~~~\n/// 22. item\n///      ~~~rust\n///      TODO: fixture placeholder\n///      ~~~\n",
        expected: &[],
        name: "a fence ends the paragraph before it, so \"22.\" may open an item",
    },
    RuleCase {
        source: "/// - outer\n///   > quoted\n///   ~~~rust\n///   let x = 1;\n/// TODO: issue required\n",
        expected: &[("CH002", 5)],
        name: "a quote nested in a list item does not close the item",
    },
    RuleCase {
        source: "/// ```rust\n/// - ```\n/// TODO: fixture placeholder\n/// ```\n",
        expected: &[],
        name: "a list marker inside a fence is sample text, not a container",
    },
    RuleCase {
        source: "/// -\n///   ~~~rust\n///   let x = 1;\n/// TODO: issue required\n",
        expected: &[("CH002", 4)],
        name: "a marker alone on its line still opens a list item",
    },
    RuleCase {
        source: "/// -   outer\n///     > ```rust\n///     > TODO: fixture placeholder\n///     > ```\n",
        expected: &[],
        name: "a quote marker is indented relative to its list container",
    },
    RuleCase {
        source: "/// ```rust\n/// > ```\n/// TODO: fixture placeholder\n/// ```\n",
        expected: &[],
        name: "a quoted delimiter deeper than the fence is sample text",
    },
    RuleCase {
        source: "/// -   outer\n///     > ```rust\n///     > let x = 1;\n///     > ```\n///     TODO: issue required\n",
        expected: &[("CH002", 5)],
        name: "a quoted fence inside a list item still sees its own closer",
    },
    RuleCase {
        source: "/// - 1. ```rust\n///      TODO: fixture placeholder\n///      ```\n",
        expected: &[],
        name: "two list markers on one line both open containers for the fence",
    },
    RuleCase {
        source: "/// - > ~~~rust\n///   > let x = 1;\n///   > ~~~\n///   TODO: issue required\n",
        expected: &[("CH002", 4)],
        name: "a fence opened behind a list and a quote knows its own depth",
    },
    RuleCase {
        source: "/// # Heading\n/// 22. item\n///     ~~~rust\n///     TODO: fixture placeholder\n///     ~~~\n",
        expected: &[],
        name: "a heading is not a paragraph, so \"22.\" may open an item after it",
    },
    RuleCase {
        source: "/// -----\n/// 22. item\n///     ~~~rust\n///     TODO: fixture placeholder\n///     ~~~\n",
        expected: &[],
        name: "nor is a thematic break",
    },
    RuleCase {
        source: "/// > > ~~~rust\n/// > > let x = 1;\n/// > > ~~~\n/// > TODO: issue required\n",
        expected: &[("CH002", 4)],
        name: "two quote markers in one match count as two levels",
    },
    RuleCase {
        source: "/// - 1. ~~~rust\n///      let x = 1;\n///      ~~~\n///   TODO: issue required\n",
        expected: &[("CH002", 4)],
        name: "a fence inside a nested item is scoped to the inner one",
    },
    RuleCase {
        source: "/**\n```rust\n/* TODO: fixture placeholder */\n```\n*/\npub fn a() {}\n",
        expected: &[],
        name: "a nested comment inside a fenced example is part of the example",
    },
    RuleCase {
        source: "/**\n * Intro\n * /* - inner */\n *     ~~~rust\n *   TODO: issue required\n */\n",
        expected: &[("CH002", 5)],
        name: "a list marker inside a nested comment does not escape it",
    },
    RuleCase {
        source: "/// - let stale = compute();\n",
        expected: &[("CH001", 1)],
        name: "a bullet does not exempt commented-out code",
    },
    RuleCase {
        source: "/// > let stale = compute();\n",
        expected: &[("CH001", 1)],
        name: "and neither does a quote",
    },
    RuleCase {
        source: "/// - a bullet of ordinary prose\n/// - the worker parks;\n",
        expected: &[],
        name: "but ordinary bulleted prose stays clean",
    },
    RuleCase {
        source: "/// - TODO fix this\n",
        expected: &[("CH002", 1)],
        name: "nor does a bullet exempt an unreferenced TODO",
    },
    RuleCase {
        source: "/// > TODO fix this\n",
        expected: &[("CH002", 1)],
        name: "nor a quote",
    },
    RuleCase {
        source: "/// word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20\n/// | w1 w2 w3 w4 w5 w6 w7 w8 w9 w10.\n",
        expected: &[("CH007", 1)],
        name: "one pipe is prose, so the sentence carries on across it",
    },
    RuleCase {
        source: "/// Heading\n/// --\n/// 22. item\n///     ~~~rust\n///     TODO: fixture placeholder\n///     ~~~\n",
        expected: &[],
        name: "a Setext underline may be hyphens",
    },
    RuleCase {
        source: "/// word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21\n/// | while this second pipe-delimited fragment adds another ten words |\n",
        expected: &[("CH007", 1)],
        name: "two pipes are not a table either, without a delimiter row",
    },
    RuleCase {
        source: "/// -   Heading\n///     --\n///     22. item\n///         ~~~rust\n///         TODO: fixture placeholder\n///         ~~~\n",
        expected: &[],
        name: "a Setext underline is measured against its list container",
    },
    RuleCase {
        source: "/// Intro\n/// > Quote\n/// 22. item\n///     ~~~rust\n///     TODO: fixture placeholder\n///     ~~~\n",
        expected: &[],
        name: "a block quote ends the paragraph for containers, not only for prose",
    },
    RuleCase {
        source: "/// word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 | tail\n///     --- | ---\n",
        expected: &[("CH007", 1)],
        name: "an over-indented delimiter row does not make a table",
    },
    RuleCase {
        source: "/// word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 | tail\n/// | | --- |\n",
        expected: &[("CH007", 1)],
        name: "nor does one with an empty cell",
    },
    RuleCase {
        source: "/// | Header |\n/// | --- |\n/// 22. item\n///     ~~~rust\n///     TODO: fixture placeholder\n///     ~~~\n",
        expected: &[],
        name: "but a confirmed table does end the paragraph above the list",
    },
    RuleCase {
        source: "/**\n * Intro /* > inner */\n * 22. item\n *     ~~~rust\n *     TODO: issue required\n */\n",
        expected: &[("CH002", 5)],
        name: "a quote inside a nested comment does not escape it",
    },
    RuleCase {
        source: "/** word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 /* inserted note */ w1 w2 w3 w4 w5 w6 w7 w8 w9 w10. */\npub fn a() {}\n",
        expected: &[("CH007", 1)],
        name: "a sentence crossing an inline nested comment is one sentence",
    },
    RuleCase {
        source: "/// Intro\n/// -\n///     ~~~rust\n///   TODO: issue required\n",
        expected: &[("CH002", 4)],
        name: "a marker with no content cannot interrupt a paragraph",
    },
    RuleCase {
        source: "/// 1234567890. ~~~rust\n///             TODO: issue required\n",
        expected: &[("CH002", 2)],
        name: "ten digits is too many for an ordered marker",
    },
    RuleCase {
        source: "/// 123456789. ~~~rust\n///            TODO: fixture placeholder\n///            ~~~\n",
        expected: &[],
        name: "nine is not",
    },
    RuleCase {
        source: "/// Intro paragraph\n/// 01. item\n///     ~~~rust\n///     TODO: fixture placeholder\n///     ~~~\n",
        expected: &[],
        name: "a marker's value decides interruption, so \"01.\" starts at one",
    },
    RuleCase {
        source: "/// * * *\n///     ~~~rust\n///   TODO: issue required\n",
        expected: &[("CH002", 3)],
        name: "a spaced thematic break is a break, not a list item",
    },
    RuleCase {
        source: "/// #\n/// 22. item\n///     ~~~rust\n///     TODO: fixture placeholder\n///     ~~~\n",
        expected: &[],
        name: "a marker-only heading is still a heading",
    },
    RuleCase {
        source: "/// ===\n/// 22. item\n///     ~~~rust\n///     TODO: issue required\n",
        expected: &[("CH002", 4)],
        name: "a decorative rule is paragraph text, not a thematic break",
    },
    RuleCase {
        source: "/// Heading\n/// ===\n/// 22. item\n///     ~~~rust\n///     TODO: fixture placeholder\n///     ~~~\n",
        expected: &[],
        name: "but the same rule under a paragraph is a Setext heading",
    },
    RuleCase {
        source: "/// >~~~rust\n/// >TODO: fixture placeholder\n/// >~~~\n",
        expected: &[],
        name: "a quote marker needs no space after it",
    },
    RuleCase {
        source: "/// Only defer when it too is\n/// >= end_at and the slot is free.\n",
        expected: &[],
        name: "but \">=\" is still an operator, not a quote",
    },
    RuleCase {
        source: "/// Intro\n///     * * *\n/// 22. item\n///     ~~~rust\n///     TODO: issue required\n",
        expected: &[("CH002", 5)],
        name: "an over-indented thematic break is indented content",
    },
    RuleCase {
        source: "/// Intro\n/// | not a table\n/// 22. item\n///     ~~~rust\n///     TODO: issue required\n",
        expected: &[("CH002", 5)],
        name: "a pipe-prefixed line is prose, not a block that ends a paragraph",
    },
    RuleCase {
        source: "/// - > word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25.\n",
        expected: &[],
        name: "a quote behind a list marker is not a word of the sentence",
    },
    RuleCase {
        source: "/// - 1. word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25.\n",
        expected: &[],
        name: "nor is a second list marker",
    },
    RuleCase {
        source: "/// Intro\n/// > word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25.\n",
        expected: &[],
        name: "entering a block quote ends the paragraph before it",
    },
    RuleCase {
        source: "/// > word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20\n/// w1 w2 w3 w4 w5 w6 w7 w8 w9 w10.\n",
        expected: &[("CH007", 1)],
        name: "but a quoted paragraph continues lazily onto an unmarked line",
    },
    RuleCase {
        source: "/// > word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25\n/// - and a list item here.\n",
        expected: &[],
        name: "leaving a quote onto a block start does flush",
    },
    RuleCase {
        source: "/// -    ```rust\n///      TODO: fixture placeholder\n///      ```\n",
        expected: &[],
        name: "four spaces after a list marker is still valid padding",
    },
    RuleCase {
        source: "// The semaphore lets just one claimant proceed.\n",
        expected: &[],
        name: "\"lets\" as a verb is behaviour, not deliberation",
    },
    RuleCase {
        source: "// The row is held. Lets just skip it.\n",
        expected: &[("CH003", 1)],
        name: "\"Lets just\" opening a sentence is still an aside",
    },
    RuleCase {
        source: "// Short sentence.\n// word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26.\n",
        expected: &[("CH007", 2)],
        name: "a sentence reports its own line, not the paragraph's first",
    },
    RuleCase {
        source: "// todo: fix this\n",
        expected: &[("CH002", 1)],
        name: "the TODO marker is case-insensitive",
    },
    RuleCase {
        source: "// fixme: fix this\n",
        expected: &[("CH002", 1)],
        name: "so is FIXME",
    },
    RuleCase {
        source: "// We\u{2019}ll group stats by queue name.\n",
        expected: &[("CH003", 1), ("CH006", 1)],
        name: "a narrative aside survives a typographic apostrophe",
    },
    RuleCase {
        source: "// Parse the id first (issue #777, Codex P2).\n",
        expected: &[("CH005", 1)],
        name: "a Codex tag with no round number is archaeology",
    },
    RuleCase {
        source: "/// Match the `Codex` literal in the reviewer column.\n",
        expected: &[],
        name: "but the word inside a code span is a literal",
    },
    RuleCase {
        source: "/// ```foo`bar TODO: remove this\n",
        expected: &[("CH002", 1)],
        name: "an invalid fence opener is scanned, not exempted",
    },
    RuleCase {
        source: "/* ```rust */\n/* TODO: issue required */\n",
        expected: &[("CH002", 2)],
        name: "two distinct block comments are two runs",
    },
    RuleCase {
        source: "let x = 1; /* ```rust\nTODO: fixture placeholder\n``` */\n",
        expected: &[],
        name: "a multiline block comment starting after code is one run",
    },
    RuleCase {
        source: "/* outer /* ```rust */ TODO: issue required */\n",
        expected: &[("CH002", 1)],
        name: "a nested comment's fence does not leak into the enclosing one",
    },
    RuleCase {
        source: "/* outer /*! TODO */ */\n",
        expected: &[("CH002", 1)],
        name: "a nested inner-doc marker is stripped too",
    },
    RuleCase {
        source: "fn f() {\n    let a = 1; // the first of two short trailing notes that each stay well under the limit here\n    let b = 2; // the second of two short trailing notes that also stays under it comfortably\n}\n",
        expected: &[],
        name: "adjacent trailing comments are not merged into one sentence",
    },
    RuleCase {
        source: "/// Paragraph.\n///     # indented\n/// 22. item\n///     ```\n///     TODO: issue required\n",
        expected: &[("CH002", 5)],
        name: "a '#' four columns into a paragraph is content, not a heading",
    },
    RuleCase {
        source: "/// Paragraph.\n///   # heading\n/// 22. item\n///     ```\n///     TODO: issue required\n",
        expected: &[],
        name: "a '#' three columns in is still a heading",
    },
    RuleCase {
        source: "/// -   intro paragraph\n///     | h | i |\n///     | --- | --- |\n///     22. item\n///         ```\n///         TODO: issue required\n",
        expected: &[],
        name: "a table nested in a list item is read in its own container",
    },
    RuleCase {
        source: "/* intro paragraph\n/* | h | i |\n| --- | --- | */\n| still prose\n22. item\n     ```\n     TODO: issue required\n*/\n",
        expected: &[("CH002", 7)],
        name: "a table inside a nested comment does not leak out of it",
    },
    RuleCase {
        source: "/// Intro | header |\n/// | - |\n/// 22. item\n///     ```\n///     TODO: issue required\n",
        expected: &[("CH002", 5)],
        name: "a one-cell delimiter under a two-cell header is not a table",
    },
    RuleCase {
        source: "/// | a |\n/// | - |\n/// 22. item\n///     ```\n///     TODO: issue required\n",
        expected: &[],
        name: "one hyphen in a cell is a valid delimiter row",
    },
    RuleCase {
        source: "/// - | a | b |\n///   | - | - |\n///   22. item\n///       ```\n///       TODO: issue required\n",
        expected: &[],
        name: "a list marker on the header row is not a cell",
    },
    RuleCase {
        source: "/// | a \\| b | c |\n/// | - | - |\n/// 22. item\n///     ```\n///     TODO: issue required\n",
        expected: &[],
        name: "an escaped pipe is content, not a cell boundary",
    },
    RuleCase {
        source: "/// - [ ] word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25.\n",
        expected: &[],
        name: "a task-list checkbox is not two words",
    },
    RuleCase {
        source: "/// - [y] word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25.\n",
        expected: &[("CH007", 1)],
        name: "'[y]' is not a task marker, so it stays two words",
    },
    RuleCase {
        source: "/** Intro paragraph\n * /* inserted note */\n * 22. item\n *     ```\n *     TODO: issue required\n */\npub struct A;\n",
        expected: &[("CH002", 5)],
        name: "a gutter before a nested opener is not a blank line",
    },
    RuleCase {
        source: "/** Intro paragraph\n *\n * 22. item\n *     ```\n *     TODO: issue required\n */\npub struct A;\n",
        expected: &[],
        name: "a genuinely blank comment line still ends the paragraph",
    },
    RuleCase {
        source: "/** Intro paragraph\n * | h |\n * /* | --- | */\n * 22. item\n *     ```\n *     TODO: issue required\n */\npub struct A;\n",
        expected: &[("CH002", 6)],
        name: "a delimiter row inside a nested comment is not the outer header's",
    },
    RuleCase {
        source: "/// > word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20\n/// continued | word21 word22 word23 word24 word25 word26.\n",
        expected: &[("CH007", 1)],
        name: "a pipe does not end a lazy quote continuation",
    },
    RuleCase {
        source: "/// > quoted sentence here\n/// | h |\n/// | --- |\n/// 22. item\n///     ```\n///     TODO: issue required\n",
        expected: &[],
        name: "a confirmed table still ends a lazy quote continuation",
    },
    RuleCase {
        source: "/** Outer /* ```rust\n * TODO: issue required\n * ``` */ tail. */\npub struct A;\n",
        expected: &[("CH002", 2)],
        name: "backticks after a nested opener are mid-line, so no fence",
    },
    RuleCase {
        source: "/** Outer /* text\n * ```rust\n * TODO: issue required\n * ``` */ tail. */\npub struct A;\n",
        expected: &[],
        name: "a nested fence that starts its own line still opens",
    },
    RuleCase {
        source: "/** ```rust\n * sample();\n * ``` /* note */\n * TODO: issue required\n * ``` */\npub struct A;\n",
        expected: &[],
        name: "a closer followed by a nested comment does not close",
    },
    RuleCase {
        source: "//! Intro paragraph.\n//! <pre>raw</pre>\n//! 22. item\n//!     ```\n//!     word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26.\n//!     ```\n",
        expected: &[],
        name: "an HTML block ends the paragraph above it",
    },
    RuleCase {
        source: "//! Intro paragraph.\n//! <T> is the type parameter.\n//! 22. item\n//!     ```\n//!     TODO: issue required\n",
        expected: &[("CH002", 5)],
        name: "'<T>' is a type parameter, not an HTML block",
    },
    RuleCase {
        source: "//! | a | b |\n//! | - | - |\n//! 22. item\n//!     ```\n//!     word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26.\n//!     ```\n",
        expected: &[],
        name: "the prose scanner ends the paragraph at a table too",
    },
    RuleCase {
        source: "// Intro e.g. word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 end.\n",
        expected: &[("CH007", 1)],
        name: "'e.g.' does not end a sentence",
    },
    RuleCase {
        source: "// Short one. Short two.\n",
        expected: &[],
        name: "an ordinary period still ends one",
    },
    RuleCase {
        source: "//! | a | b |\n//! | - | - |\n//! ordinary row with no separator\n//! 22. item\n//!     ```\n//!     word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26.\n//!     ```\n",
        expected: &[],
        name: "a table body row needs no pipe",
    },
    RuleCase {
        source: "//! <pre>\n//! word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26.\n//! </pre>\n",
        expected: &[],
        name: "an HTML block runs to its closer",
    },
    RuleCase {
        source: "//! Intro.\n//! <pre>raw</pre>\n//! word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26.\n",
        expected: &[("CH007", 3)],
        name: "an HTML block closed on its own line ends there",
    },
    RuleCase {
        source: "//! <div>\n//! word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26.\n//!\n//! word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26.\n",
        expected: &[("CH007", 4)],
        name: "a tag-name HTML block ends at a blank line",
    },
    RuleCase {
        source: "/// > <pre>\n/// > TODO: fixture placeholder\n/// > </pre>\n",
        expected: &[],
        name: "a quoted HTML block is peeled before it is recognized",
    },
    RuleCase {
        source: "/// <pre>\n/// </style>\n/// TODO: fixture placeholder\n/// </pre>\n",
        expected: &[],
        name: "a verbatim block closes on its own tag and no other",
    },
    RuleCase {
        source: "/// <pre>TODO: fixture placeholder</pre>\n",
        expected: &[],
        name: "a raw block complete on one line exempts that line",
    },
    RuleCase {
        source: "/// <pre>\n/// TODO: one\n/// </pre>\n/// TODO: two\n",
        expected: &[("CH002", 4)],
        name: "and the line after its closer is prose again",
    },
    RuleCase {
        source: "/** ```rust\n * sample();\n * ``` /**/\n * TODO: fixture placeholder\n * ``` */\npub struct A;\n",
        expected: &[],
        name: "an empty nested comment still follows a closing fence",
    },
    RuleCase {
        source: "/// - > intro\n///   22. item\n///       ```\n///       TODO: issue required\n",
        expected: &[],
        name: "quote depth comes from the peel in both scanners",
    },
    RuleCase {
        source: "/// - <pre>\n///   TODO: issue required\n",
        expected: &[],
        name: "a list marker is peeled before the HTML opener too",
    },
    RuleCase {
        source: "/// Intro.\n///\n///     let x = compute();\n///     TODO: fixture placeholder\n",
        expected: &[],
        name: "an indented code block in a doc comment is an example",
    },
    RuleCase {
        source: "// Intro.\n//\n//     let x = compute();\n//     TODO: issue required\n",
        expected: &[("CH001", 3), ("CH002", 4)],
        name: "a plain // comment renders nowhere, so indentation is not code",
    },
    RuleCase {
        source: "/// wrapped line one\n///     word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26.\n",
        expected: &[("CH007", 1)],
        name: "indented code cannot interrupt a paragraph",
    },
    RuleCase {
        source: "/// Intro.\n///\n///     let x = compute();\n///\n/// TODO: issue required\n",
        expected: &[("CH002", 5)],
        name: "and it ends where the indent does",
    },
    RuleCase {
        source: "/// -     let x = compute();\n///       TODO: fixture placeholder\n",
        expected: &[],
        name: "a code block may begin on the list marker's own line",
    },
    RuleCase {
        source: "/// - item\n/// - TODO: issue required\n",
        expected: &[("CH002", 2)],
        name: "an ordinary bullet is not indented code",
    },
    RuleCase {
        source: "/*** not a doc comment\n *\n *     let x = compute();\n *     TODO: issue required\n */\npub struct A;\n",
        expected: &[("CH001", 3), ("CH002", 4)],
        name: "'/***' is an ordinary comment, so its indent is not code",
    },
    RuleCase {
        source: "/** a real doc comment\n *\n *     let x = compute();\n *     TODO: fixture placeholder\n */\npub struct B;\n",
        expected: &[],
        name: "and '/**' is still a doc comment",
    },
    RuleCase {
        source: "// This first sentence ends with **word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 emphasis.** This second sentence has word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 here.\n",
        expected: &[],
        name: "a sentence may end inside emphasis",
    },
    RuleCase {
        source: "// A `?` straight out of any of these returns early from the caller here.\n",
        expected: &[],
        name: "a code span's punctuation is not a sentence end",
    },
    RuleCase {
        source: "// Used to answer \"is `charge_card` held?\" during an incident and word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18.\n",
        expected: &[("CH007", 1)],
        name: "nor is a question mark inside a quotation",
    },
    RuleCase {
        source: "// pub(in crate::foo) fn bar() {\n",
        expected: &[("CH001", 1)],
        name: "pub(in path) is a visibility",
    },
    RuleCase {
        source: "// Parse the `TODO:` prefix from input.\n",
        expected: &[],
        name: "a marker inside a code span is documentation",
    },
    RuleCase {
        source: "// TODO: fix the parser\n",
        expected: &[("CH002", 1)],
        name: "and a bare one is still a commitment",
    },
    RuleCase {
        source: "/** ~~~rust /* note */ `info`\n * TODO: fixture placeholder\n * ~~~\n */\npub struct C;\n",
        expected: &[],
        name: "only a backtick fence forbids a backtick in its info string",
    },
    RuleCase {
        source: "/** ```rust /* note */ `info`\n * TODO: issue required\n */\npub struct D;\n",
        expected: &[("CH002", 2)],
        name: "and a backtick fence still does",
    },
    RuleCase {
        source: "/// - # word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26\n",
        expected: &[],
        name: "a heading on a list-marker line is a heading",
    },
    RuleCase {
        source: "/// word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26\n/// ===\n",
        expected: &[],
        name: "a Setext title is a heading, not a sentence",
    },
    RuleCase {
        source: "// word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26\n// ---------------------------\n",
        expected: &[("CH007", 1)],
        name: "but a banner rule in a // comment underlines nothing",
    },
    RuleCase {
        source: "/// Parse the `let's` token from the input.\n",
        expected: &[],
        name: "a narrative phrase inside a code span is a literal",
    },
    RuleCase {
        source: "// Renew the Let's Encrypt certificate.\n",
        expected: &[],
        name: "a proper name that spells itself with an apostrophe",
    },
    RuleCase {
        source: "// Renew the Let\u{2019}s Encrypt certificate.\n",
        expected: &[],
        name: "in the typographic spelling as well",
    },
    RuleCase {
        source: "// Actually, let's encrypt the payload first.\n",
        expected: &[("CH003", 1), ("CH006", 1)],
        name: "but lower-case encrypt is still deliberation, and a contraction",
    },
    RuleCase {
        source: "// Actually, let's just skip the retry here.\n",
        expected: &[("CH003", 1), ("CH006", 1)],
        name: "and outside one it is still deliberation",
    },
    RuleCase {
        source: "/// Explain the `literal\n/// TODO: marker` syntax.\n",
        expected: &[],
        name: "a code span may wrap onto the next line",
    },
    RuleCase {
        source: "// A stray ` here\n// TODO: fix the parser\n",
        expected: &[("CH002", 2)],
        name: "an unmatched backtick opens no span",
    },
    RuleCase {
        source: "/// A stray ` here\n///\n/// TODO: fix the parser\n",
        expected: &[("CH002", 3)],
        name: "and a blank line ends a paragraph, so it ends a span",
    },
    RuleCase {
        source: "/// Explain `literal TODO: issue required`` suffix.\npub struct A;\n",
        expected: &[("CH002", 1)],
        name: "a one-backtick opener does not close on a two-backtick run",
    },
    RuleCase {
        source: "/// Explain ``TODO: marker`` here.\n",
        expected: &[],
        name: "and a two-backtick span still closes on its own run",
    },
    RuleCase {
        source: "/// Explain the `literal\npub struct B;\n\n/// TODO: issue required` suffix.\npub struct C;\n",
        expected: &[("CH002", 4)],
        name: "a span cannot reach out of its own comment",
    },
    RuleCase {
        source: "/// Parse the `foo. not sure why` token.\n",
        expected: &[],
        name: "a period inside a code span is not a sentence end",
    },
    RuleCase {
        source: "// End here. `%` is the next sentence and it stays whole.\n",
        expected: &[],
        name: "and a sentence that opens with a span keeps it",
    },
    RuleCase {
        source: "/// Explain the \\`literal\n/// TODO: issue required` syntax.\n",
        expected: &[("CH002", 2)],
        name: "an escaped backtick opens no span",
    },
    RuleCase {
        source: "/// The URL parser treats `\\` as a path separator. TODO: issue required\n",
        expected: &[("CH002", 1)],
        name: "but a backslash inside one does not stop it closing",
    },
    RuleCase {
        source: "/// Explain the \\\\`TODO: marker` syntax.\n",
        expected: &[],
        name: "two backslashes escape each other, so the span opens",
    },
    RuleCase {
        source: "// <pre>\n// TODO: issue required\n// let stale = compute();\n// </pre>\n",
        expected: &[("CH001", 3), ("CH002", 2)],
        name: "'<pre>' in a // comment is text, and exempts nothing",
    },
    RuleCase {
        source: "// ```rust\n// TODO: issue required\n// ```\n",
        expected: &[],
        name: "but a fence is an example in any comment",
    },
    RuleCase {
        source: "/// - Explain the `literal\n/// - TODO: issue required` suffix.\n",
        expected: &[("CH002", 2)],
        name: "a code span does not reach across two list items",
    },
    RuleCase {
        source: "/// - Explain the `literal\n///   TODO: marker` syntax.\n",
        expected: &[],
        name: "but it does reach across one item's own lines",
    },
    RuleCase {
        source: "/// Explain the `literal\n/// > TODO: issue required` suffix.\n",
        expected: &[("CH002", 2)],
        name: "nor across the edge of a block quote",
    },
    RuleCase {
        source: "/// > Explain the `literal\n/// > TODO: marker` syntax.\n",
        expected: &[],
        name: "though it does reach within one",
    },
    RuleCase {
        source: "/// Parse the <code>not sure why</code> token literally.\n",
        expected: &[],
        name: "an inline <code> element is code in a doc comment",
    },
    RuleCase {
        source: "// Parse the <code>not sure why</code> token literally.\n",
        expected: &[("CH003", 1)],
        name: "and characters in a // comment",
    },
    RuleCase {
        source: "/// Parse the <code>foo. not sure why</code> token literally.\n",
        expected: &[],
        name: "a period inside an inline <code> element ends no sentence",
    },
    RuleCase {
        source: "// Parse the <code>foo. not sure why</code> token literally.\n",
        expected: &[("CH003", 1)],
        name: "and in a // comment it still does",
    },
    RuleCase {
        source: "/// Explain the `literal\n/// | h |\n/// | - |\n/// TODO: issue required` suffix.\n",
        expected: &[("CH002", 4)],
        name: "a confirmed table ends the paragraph above it, so a span stops",
    },
    RuleCase {
        source: "/// Explain the `literal\n/// Heading\n/// ===\n/// TODO: issue required` suffix.\n",
        expected: &[("CH002", 4)],
        name: "and a Setext underline ends the heading it makes",
    },
    RuleCase {
        source: "/// | a | b |\n/// | - | - |\n/// | x `open | y |\n/// | z TODO: issue required` w |\n",
        expected: &[("CH002", 4)],
        name: "each table row is its own boundary as well",
    },
    RuleCase {
        source: "/// | a |\n/// | - |\n/// | x `open TODO: marker` y |\n",
        expected: &[],
        name: "but within one cell a span still pairs",
    },
    RuleCase {
        source: "// Explain the `literal\n// | h |\n// | - |\n// TODO: issue required` suffix.\n",
        expected: &[],
        name: "neither boundary applies in a // comment, which renders nothing",
    },
    RuleCase {
        source: "/// \\<code>TODO: add retry\\</code>\n",
        expected: &[("CH002", 1)],
        name: "an escaped <code> tag is literal text, not an element",
    },
    RuleCase {
        source: "/// \\\\<code>TODO: add retry</code>\n",
        expected: &[],
        name: "two backslashes escape each other, so the element opens",
    },
    RuleCase {
        source: "/// <code>a\\</code> TODO: add retry\n",
        expected: &[],
        name: "an escaped closer leaves the element open to the block end",
    },
    RuleCase {
        source: "/// <code>a\n/// TODO: add retry\n",
        expected: &[],
        name: "and an unclosed element covers the line under it",
    },
    RuleCase {
        source: "/// <code>a\n///\n/// TODO: issue required\n",
        expected: &[("CH002", 3)],
        name: "though the paragraph's own end closes it",
    },
    RuleCase {
        source: "/// Demonstrates `\n/// let x = compute();\n/// ` inline.\n",
        expected: &[],
        name: "a wrapped code span is an example, not commented-out code",
    },
    RuleCase {
        source: "/// Demonstrates it.\n/// let x = compute();\n",
        expected: &[("CH001", 2)],
        name: "but the same statement outside one still is",
    },
    RuleCase {
        source: "// See #123 for the parser. TODO: add retries\n",
        expected: &[("CH002", 1)],
        name: "a reference belongs to the marker it sits with, not to the line",
    },
    RuleCase {
        source: "// TODO(#123): parser; TODO: add retries\n",
        expected: &[("CH002", 1)],
        name: "so a tracked marker does not cover an untracked one after it",
    },
    RuleCase {
        source: "/// The `#123` syntax. TODO: add retries\n",
        expected: &[("CH002", 1)],
        name: "and a reference inside a code span is syntax, not a reference",
    },
    RuleCase {
        source: "// TODO: add retries (#123)\n",
        expected: &[],
        name: "a marker's own reference may follow it",
    },
    RuleCase {
        source: "// TODO: add retries. (#123)\n",
        expected: &[],
        name: "a citation after the period belongs to the sentence it cites",
    },
    RuleCase {
        source: "// TODO: add retries. [#123]\n",
        expected: &[],
        name: "in square brackets as well as round",
    },
    RuleCase {
        source: "// TODO: add retries.\n// (#123)\n",
        expected: &[],
        name: "and on the line below, because a sentence may wrap",
    },
    RuleCase {
        source: "// TODO: add retries. See #123 for the parser.\n",
        expected: &[("CH002", 1)],
        name: "but a following SENTENCE is not a citation",
    },
    RuleCase {
        source: "// TODO: add retries. (#123) TODO: use them\n",
        expected: &[("CH002", 1)],
        name: "and a citation stops where the next marker starts",
    },
    RuleCase {
        source: "/// TODO: preserve the \"Ready?\" prompt under #123\n",
        expected: &[],
        name: "a quoted question does not end the marker's sentence",
    },
    RuleCase {
        source: "// TODO: fix the (Ready.) prompt under #123\n",
        expected: &[],
        name: "nor does a parenthesised one",
    },
    RuleCase {
        source: "// TODO: *add retries.* See #123 for the parser.\n",
        expected: &[("CH002", 1)],
        name: "but an emphasis marker does not hide the end of one",
    },
    RuleCase {
        source: "/// TODO: print \"ready.\" See #123 for the parser.\n",
        expected: &[("CH002", 1)],
        name: "a closing quote ends a sentence where a capital follows it",
    },
    RuleCase {
        source: "// TODO: print \u{201c}ready.\u{201d} See #123 for the parser.\n",
        expected: &[("CH002", 1)],
        name: "a typographic quote closes one as well as an ASCII quote",
    },
    RuleCase {
        source: "// TODO: preserve the \u{201c}Ready?\u{201d} prompt under #123\n",
        expected: &[],
        name: "and reads the following word the same way",
    },
    RuleCase {
        source: "// TODO: see http://\n",
        expected: &[("CH002", 1)],
        name: "a bare scheme tracks nothing",
    },
    RuleCase {
        source: "// TODO: see https://x.test/i/9\n",
        expected: &[],
        name: "but a URL with a destination does",
    },
    RuleCase {
        source: "// TODO: remove fallback under nothttps://example.com/123\n",
        expected: &[("CH002", 1)],
        name: "and a scheme starts where a scheme may start",
    },
    RuleCase {
        source: "// TODO: see - https://x.test/i/9\n",
        expected: &[],
        name: "which a separator before it does not prevent",
    },
    RuleCase {
        source: "// TODO: add retries #0\n",
        expected: &[("CH002", 1)],
        name: "and neither does issue zero, which numbering never reaches",
    },
    RuleCase {
        source: "// TODO: add retries. (#000)\n",
        expected: &[("CH002", 1)],
        name: "in a citation as well as in the text",
    },
    RuleCase {
        source: "// #0 - TODO: add retries\n",
        expected: &[("CH002", 1)],
        name: "and on the marker's left, where the third pattern reads it",
    },
    RuleCase {
        source: "// #123\n// TODO: remove the fallback\n",
        expected: &[],
        name: "a reference on the line ABOVE tracks the marker below it",
    },
    RuleCase {
        source: "// (#123)\n// TODO: remove the fallback\n",
        expected: &[],
        name: "bracketed there as readily as bare",
    },
    RuleCase {
        source: "// (#123) TODO: remove the fallback\n",
        expected: &[],
        name: "and on the same line, which is the same notation",
    },
    RuleCase {
        source: "// See (#123) for the parser.\n// TODO: remove the fallback\n",
        expected: &[("CH002", 2)],
        name: "but a bracketed reference inside prose still does not abut",
    },
    RuleCase {
        source: "// https://x.test/i/9\n// TODO: remove the fallback\n",
        expected: &[],
        name: "a URL there as well as an issue number",
    },
    RuleCase {
        source: "// #123\n// TODO remove the fallback\n",
        expected: &[],
        name: "and the unpunctuated marker, which only reads at a line start",
    },
    RuleCase {
        source: "// See #123 for the parser.\n// TODO: remove the fallback\n",
        expected: &[("CH002", 2)],
        name: "but prose above it still does not abut the marker",
    },
    RuleCase {
        source: "// #123\n//\n// TODO: remove the fallback\n",
        expected: &[("CH002", 3)],
        name: "a blank line ends the carry from above",
    },
    RuleCase {
        source: "// #123\n// TODO: a\n// TODO: b\n",
        expected: &[("CH002", 3)],
        name: "and one marker keeps the reference from the next",
    },
    RuleCase {
        source: "// TODO: add retries #10\n",
        expected: &[],
        name: "but a zero inside a real number is a digit",
    },
    RuleCase {
        source: "// TODO: fix #1\u{662}\n",
        expected: &[("CH002", 1)],
        name: "an issue number is written in ASCII digits",
    },
    RuleCase {
        source: "// TODO: fix #1\u{ff14}\n",
        expected: &[("CH002", 1)],
        name: "in a fullwidth form as well as an Arabic-Indic one",
    },
    RuleCase {
        source: "// TODO: replace fallback #123abc\n",
        expected: &[("CH002", 1)],
        name: "an issue number ends where the number ends",
    },
    RuleCase {
        source: "// TODO: replace fallback (#1_000)\n",
        expected: &[("CH002", 1)],
        name: "and an underscore is part of a word, not a separator",
    },
    RuleCase {
        source: "// TODO: see HTTPS://x.test/i/9\n",
        expected: &[],
        name: "but a scheme is case-insensitive, as RFC 3986 has it",
    },
    RuleCase {
        source: "// TODO: fix under https://[\n",
        expected: &[("CH002", 1)],
        name: "an IPv6 host needs the bracket that closes it",
    },
    RuleCase {
        source: "// TODO: fix under https://[:]\n",
        expected: &[("CH002", 1)],
        name: "and an address inside it, not only a colon",
    },
    RuleCase {
        source: "// TODO: fix under https://[....]\n",
        expected: &[("CH002", 1)],
        name: "nor only periods",
    },
    RuleCase {
        source: "// https://[::1]/p - TODO: fix\n",
        expected: &[],
        name: "and a closed one is a host in every position",
    },
    RuleCase {
        source: "// HTTPS://x.test/i/9 - TODO: fix\n",
        expected: &[],
        name: "in every position a reference may sit",
    },
    RuleCase {
        source: "// TODO: see https://).\n",
        expected: &[("CH002", 1)],
        name: "a scheme followed by punctuation is not a destination",
    },
    RuleCase {
        source: "// https://) - TODO: x\n",
        expected: &[("CH002", 1)],
        name: "on the marker's left either",
    },
    RuleCase {
        source: "// TODO: see https://[::1]/p\n",
        expected: &[],
        name: "but an IPv6 literal is a host, as a name or an address is",
    },
    RuleCase {
        source: "//! One.\n//!\n//! Two.\n#![allow(dead_code)]\n//!\n//! Three.\n",
        expected: &[],
        name: "an inner doc run survives an inner attribute",
    },
    RuleCase {
        source: "/// One.\n///\n#[allow(dead_code)]\n/// Two.\npub struct S;\n",
        expected: &[],
        name: "and an outer doc run survives an outer one",
    },
    RuleCase {
        source: "/// One.\n///\n#[allow(\n    dead_code\n)]\n/// Two.\npub struct S;\n",
        expected: &[],
        name: "which may wrap over several lines",
    },
    RuleCase {
        source: "/// One.\n///\n#[doc = concat!(\n    \"Inserted ] text.\"\n)]\n/// Two.\npub struct S;\n",
        expected: &[],
        name: "and may carry a bracket inside a string, which is text",
    },
    RuleCase {
        source: "/// One.\n///\n#[doc = concat!(\n    r#\"raw ] text\"#\n)]\n/// Two.\npub struct S;\n",
        expected: &[],
        name: "in a raw string too",
    },
    RuleCase {
        source: "/// One.\n///\n#[allow(\n    // keep this lint off for now\n    dead_code\n)]\n/// Two.\npub struct S;\n",
        expected: &[],
        name: "and a comment written inside it, which belongs to the attribute",
    },
    RuleCase {
        source: "/// One.\n///\n#[cfg(all(/* note */\n    unix\n))]\n/// Two.\npub struct S;\n",
        expected: &[],
        name: "on the line that OPENS it as well as a line within it",
    },
    RuleCase {
        source: "/// One.\n///\n#[allow(\n    /* keep this off */\n    dead_code\n)]\n/// Two.\npub struct S;\n",
        expected: &[],
        name: "a block comment there as well as a line comment",
    },
    RuleCase {
        source: "/// One.\n///\n#[allow(\n    // a ] bracket in the comment\n    dead_code\n)]\n/// Two.\npub struct S;\n",
        expected: &[],
        name: "whose own brackets are text, like a string's",
    },
    RuleCase {
        source: "struct S {\n    #[allow(dead_code)] // why one\n    a: u8,\n    #[allow(dead_code)] // why two\n    b: u8,\n}\n",
        expected: &[],
        name: "but a comment BESIDE a finished attribute is an ordinary trailing one",
    },
    RuleCase {
        source: "/// One.\n///\n\n/// Two.\npub struct S;\n",
        expected: &[],
        name: "an empty source line renders nothing, so it is not a break either",
    },
    RuleCase {
        source: "/// One.\n///\n\n#[allow(dead_code)]\n\n/// Two.\npub struct S;\n",
        expected: &[],
        name: "mixed with attributes in the same gap",
    },
    RuleCase {
        source: "/// One.\n///\n\npub struct S;\n",
        expected: &[("CH004", 2)],
        name: "but with no doc after it the blank line still renders nothing",
    },
    RuleCase {
        source: "// One.\n//\n\n// Two.\n",
        expected: &[("CH004", 2)],
        name: "and a plain // run is joined by nothing at all",
    },
    RuleCase {
        source: "/// One.\n///\n#[allow(dead_code)]\npub struct S;\n",
        expected: &[("CH004", 2)],
        name: "but a blank doc line with no doc after it still renders nothing",
    },
    RuleCase {
        source: "/// One.\n///\n// #[allow(dead_code)]\n/// Two.\npub struct S;\n",
        expected: &[("CH001", 3), ("CH004", 2)],
        name: "and a commented-out attribute is code, not a bridge",
    },
    RuleCase {
        source: "// One.\n//\nlet x = 1;\n// Two.\n",
        expected: &[("CH004", 2)],
        name: "nor does a plain run join across code, which nothing renders",
    },
    RuleCase {
        source: "/// One.\n///\n#[doc = \"Two.\"]\npub struct S;\n",
        expected: &[],
        name: "an explicit doc attribute continues the document past the run",
    },
    RuleCase {
        source: "#[doc = \"Zero.\"]\n///\n/// One.\npub struct S;\n",
        expected: &[],
        name: "and in front of it as well as after it",
    },
    RuleCase {
        source: "//! One.\n//!\n#![doc = \"Two.\"]\n",
        expected: &[],
        name: "the inner form joining an inner run",
    },
    RuleCase {
        source: "//! One.\n//!\n#[doc = \"Two.\"]\npub struct S;\n",
        expected: &[("CH004", 2)],
        name: "but an outer attribute documents the next item, not this module",
    },
    RuleCase {
        source: "/// One.\n///\n#[doc(alias = \"zz\")]\npub struct S;\n",
        expected: &[("CH004", 2)],
        name: "and doc(alias) renders nothing, so the blank line is a real edge",
    },
    RuleCase {
        source: "/// One.\n///\n#[doc = \"Two.\"]\n#[allow(dead_code)]\npub struct S;\n",
        expected: &[],
        name: "reached across an attribute that carries no content of its own",
    },
    RuleCase {
        source: "/// One.\n///\n#[cfg_attr(all(), doc = \"Two.\")]\npub struct S;\n",
        expected: &[],
        name: "a cfg_attr may carry the doc value",
    },
    RuleCase {
        source: "/// One.\n///\n#[cfg_attr(all(), cfg_attr(all(), doc = \"Two.\"))]\npub struct S;\n",
        expected: &[],
        name: "at any nesting, because the attribute is searched and not counted",
    },
    RuleCase {
        source: "/// One.\n///\n#[cfg_attr(\n    all(),\n    doc = \"Two.\"\n)]\npub struct S;\n",
        expected: &[],
        name: "and on a later line, because a wrapped attribute is read whole",
    },
    RuleCase {
        source: "/// One.\n///\n#[cfg_attr(all(), allow(dead_code))]\npub struct S;\n",
        expected: &[("CH004", 2)],
        name: "but a cfg_attr with no doc payload carries nothing",
    },
    RuleCase {
        source: "/// Use `--flag0 --flag1 --flag2 --flag3 --flag4 --flag5 --flag6 --flag7 --flag8 --flag9 --flag10 --flag11 --flag12 --flag13 --flag14 --flag15 --flag16 --flag17 --flag18 --flag19 --flag20 --flag21 --flag22 --flag23 --flag24 --flag25` here.\n",
        expected: &[],
        name: "a code span is one token, however many spaces its content holds",
    },
    RuleCase {
        source: "// TODO: fix token abc#123\n",
        expected: &[("CH002", 1)],
        name: "a hash inside a token routes nowhere and tracks nothing",
    },
    RuleCase {
        source: "// TODO: fix x/#123\n",
        expected: &[("CH002", 1)],
        name: "and a slash with no repository behind it is not the cross-repo form",
    },
    RuleCase {
        source: "// TODO: fix owner/repo#123\n",
        expected: &[],
        name: "though the complete cross-repository form still tracks",
    },
    RuleCase {
        source: "// TODO(#1): a; TODO(#2): b\n",
        expected: &[],
        name: "and two markers each carrying one are both tracked",
    },
    RuleCase {
        source: "/// Contact <code@example.com>. TODO: add retry\n",
        expected: &[("CH002", 1)],
        name: "an e-mail autolink is not a <code> opener",
    },
    RuleCase {
        source: "/// A <code-block> element. TODO: add retry\n",
        expected: &[("CH002", 1)],
        name: "nor is a tag whose name merely starts with code",
    },
    RuleCase {
        source: "/// Spaced <code > here</code>. TODO: add retry\n",
        expected: &[("CH002", 1)],
        name: "a closed element still ends where its closer does",
    },
    RuleCase {
        source: "/// Empty <code/> here. TODO: add retry\n",
        expected: &[],
        name: "but a self-closing tag opens the element HTML gives it",
    },
    RuleCase {
        source: "/// # Explain the `literal\n/// TODO: issue required` suffix.\n",
        expected: &[("CH002", 2)],
        name: "a heading is one line, so the line under it is a new block",
    },
    RuleCase {
        source: "/// # A `literal TODO: marker` heading\n",
        expected: &[],
        name: "but a span within one heading still pairs",
    },
    RuleCase {
        source: "/// Explain the `literal\n/// ***\n/// TODO: issue required` suffix.\n",
        expected: &[("CH002", 3)],
        name: "and a thematic break ends the paragraph on each side of it",
    },
    RuleCase {
        source: "/// Explain <code =bad>TODO: add retry</code> here.\n",
        expected: &[("CH002", 1)],
        name: "an attribute must have a name, so this opens no element",
    },
    RuleCase {
        source: "/// Explain <code class=\"x\">TODO: add retry</code> here.\n",
        expected: &[],
        name: "a quoted attribute value is one",
    },
    RuleCase {
        source: "/// Explain <code data-x=y>TODO: add retry</code> here.\n",
        expected: &[],
        name: "and so is an unquoted one",
    },
    RuleCase {
        source: "/// Explain <code title=\"a>b\">TODO: add retry</code> here.\n",
        expected: &[],
        name: "a quoted value may hold the bracket that would end the tag",
    },
    RuleCase {
        source: "// extern \"C-unwind\" fn stale() {\n",
        expected: &[("CH001", 1)],
        name: "a hyphenated ABI is still a commented-out signature",
    },
    RuleCase {
        source: "// extern fn stale() {\n",
        expected: &[("CH001", 1)],
        name: "and so is one with no ABI string at all",
    },
    RuleCase {
        source: "// Use the extern \"C-unwind\" convention for the callback here.\n",
        expected: &[],
        name: "but prose that names an ABI is prose",
    },
    RuleCase {
        source: "/// > Explain the `literal\n/// TODO: issue required` suffix.\n",
        expected: &[],
        name: "a lazy continuation stays inside the quote, so the span pairs",
    },
    RuleCase {
        source: "/// > Explain the `literal\n///\n/// TODO: issue required` suffix.\n",
        expected: &[("CH002", 3)],
        name: "but a blank line really does leave it",
    },
    RuleCase {
        source: "/// > Explain the `literal\n/// # Heading\n/// TODO: issue required` suffix.\n",
        expected: &[("CH002", 3)],
        name: "and a line that begins a block is no continuation",
    },
    RuleCase {
        source: "// pub use crate::foo;\n",
        expected: &[("CH001", 1)],
        name: "a re-export carries a visibility like any other item",
    },
    RuleCase {
        source: "// pub(crate) use crate::foo;\n",
        expected: &[("CH001", 1)],
        name: "including a restricted one",
    },
    RuleCase {
        source: "// fn r#match() {\n",
        expected: &[("CH001", 1)],
        name: "a raw identifier is an identifier",
    },
    RuleCase {
        source: "// pub struct r#type;\n",
        expected: &[("CH001", 1)],
        name: "in a type name as well as a function name",
    },
    RuleCase {
        source: "// pub(in crate::r#type) fn stale() {\n",
        expected: &[("CH001", 1)],
        name: "and in a visibility path, whose segments are names too",
    },
    RuleCase {
        source: "// fn foo<T>() where T: Copy {\n",
        expected: &[("CH001", 1)],
        name: "a where clause may sit on the signature line",
    },
    RuleCase {
        source: "// pub struct S<T>(T) where T: Copy;\n",
        expected: &[("CH001", 1)],
        name: "on a tuple struct as well as a function",
    },
    RuleCase {
        source: "// impl<T> Q<T> where T: Copy, T: Default {\n",
        expected: &[("CH001", 1)],
        name: "and may carry more than one bound",
    },
    RuleCase {
        source: "// The queue drains where the worker parks\n",
        expected: &[],
        name: "but the word alone is prose",
    },
    RuleCase {
        source: "// fn stale() {}\n",
        expected: &[("CH001", 1)],
        name: "a body that opens and closes on one line is still a body",
    },
    RuleCase {
        source: "// fn stale<T: Into<Vec<u8>>>() {}\n",
        expected: &[("CH001", 1)],
        name: "a generic parameter list nests",
    },
    RuleCase {
        source: "// fn deep<T: Into<Vec<Box<Option<u8>>>>>() {}\n",
        expected: &[("CH001", 1)],
        name: "to any depth, not to a counted one",
    },
    RuleCase {
        source: "// fn ptr<F: Fn(u8) -> u8>(f: F) -> u8 {\n",
        expected: &[("CH001", 1)],
        name: "and may hold the parentheses of a function bound",
    },
    RuleCase {
        source: "// fn stale<T: Into<[u8; 32]>>() {}\n",
        expected: &[("CH001", 1)],
        name: "and the semicolon of an array type, which ends no declaration",
    },
    RuleCase {
        source: "// struct Stale<const N: usize = { 1 + 2 }>;\n",
        expected: &[("CH001", 1)],
        name: "and a const block, which ends none either",
    },
    RuleCase {
        source: "// fn stale<T>() where T: Into<[u8; 32]> {\n",
        expected: &[("CH001", 1)],
        name: "a where clause holds an array type as a generic list does",
    },
    RuleCase {
        source: "// impl<T> Q<T> where T: Into<[u8; 32]> {\n",
        expected: &[("CH001", 1)],
        name: "on an impl header too",
    },
    RuleCase {
        source: "// pub unsafe trait Stale {}\n",
        expected: &[("CH001", 1)],
        name: "and unsafe qualifies a trait, between the visibility and the keyword",
    },
    RuleCase {
        source: "// unsafe impl Send for X {}\n",
        expected: &[("CH001", 1)],
        name: "as it qualifies an impl",
    },
    RuleCase {
        source: "// impl details are described in the module docs {\n",
        expected: &[],
        name: "but an impl-shaped sentence is still prose",
    },
    RuleCase {
        source: "// use foo::{bar::{Baz, Qux}, Quux};\n",
        expected: &[("CH001", 1)],
        name: "a use tree nests as a generic list does",
    },
    RuleCase {
        source: "// extern crate serde;\n",
        expected: &[("CH001", 1)],
        name: "an extern crate is an import item as a use is",
    },
    RuleCase {
        source: "// pub extern crate alloc as alloc2;\n",
        expected: &[("CH001", 1)],
        name: "and carries a visibility and a rename",
    },
    RuleCase {
        source: "// extern crate versions are pinned in the lockfile;\n",
        expected: &[],
        name: "but a sentence that opens with the words is prose",
    },
    RuleCase {
        source: "// extern \"C\" {\n",
        expected: &[("CH001", 1)],
        name: "an extern block opens a body",
    },
    RuleCase {
        source: "// unsafe extern \"C\" {}\n",
        expected: &[("CH001", 1)],
        name: "and closes one, with the qualifier edition 2024 asks for",
    },
    RuleCase {
        source: "// extern {\n",
        expected: &[("CH001", 1)],
        name: "and the ABI string is optional",
    },
    RuleCase {
        source: "// extern symbols resolve at link time {\n",
        expected: &[],
        name: "though no prose reaches the brace, because nothing may precede it",
    },
    RuleCase {
        source: "// extern r\"C\" {\n",
        expected: &[("CH001", 1)],
        name: "an ABI may be a raw string literal",
    },
    RuleCase {
        source: "// unsafe extern r##\"C-unwind\"## {}\n",
        expected: &[("CH001", 1)],
        name: "with any number of hashes",
    },
    RuleCase {
        source: "// extern r#\"C\" {\n",
        expected: &[],
        name: "though the closing run must match the opening one",
    },
    RuleCase {
        source: "// unsafe extern b\"C\" {}\n",
        expected: &[],
        name: "and a byte string is not a string, which rustc says outright",
    },
    RuleCase {
        source: "// pub extern r#\"C\"# fn stale() {}\n",
        expected: &[("CH001", 1)],
        name: "a function ABI reads the same spellings as a block ABI",
    },
    RuleCase {
        source: "// stale! {}\n",
        expected: &[("CH001", 1)],
        name: "a brace-delimited macro call needs no semicolon",
    },
    RuleCase {
        source: "// ::std::compile_error! {\"gone\"}\n",
        expected: &[("CH001", 1)],
        name: "and may start at the crate root",
    },
    RuleCase {
        source: "// Important! {see the note below}\n",
        expected: &[],
        name: "but an exclamation before a brace is English",
    },
    RuleCase {
        source: "// Note! {a, b}\n",
        expected: &[],
        name: "even when it is too short for the four-word guard to see",
    },
    RuleCase {
        source: "// use ::std::fmt;\n",
        expected: &[("CH001", 1)],
        name: "a use tree may start at the crate root",
    },
    RuleCase {
        source: "// use ::{std, core};\n",
        expected: &[("CH001", 1)],
        name: "with a group directly behind it",
    },
    RuleCase {
        source: "// let ::std::option::Option::Some(row) = fetch() else {\n",
        expected: &[("CH001", 1)],
        name: "as may the path of a destructuring pattern",
    },
    RuleCase {
        source: "// #[::core::prelude::v1::derive(Debug)]\n",
        expected: &[("CH001", 1)],
        name: "and the path of an attribute",
    },
    RuleCase {
        source: "// use ::the reader may skip this;\n",
        expected: &[],
        name: "though a root separator does not make a sentence an import",
    },
    RuleCase {
        source: "// } else {\n",
        expected: &[("CH001", 1)],
        name: "a closing brace may carry the other half of a conditional",
    },
    RuleCase {
        source: "// else if let Some(row) = fetch() {\n",
        expected: &[("CH001", 1)],
        name: "and else takes another if",
    },
    RuleCase {
        source: "// else while retries > 0 {\n",
        expected: &[],
        name: "but nothing else, which rustc says outright",
    },
    RuleCase {
        source: "// } else the reader may skip the rest {\n",
        expected: &[],
        name: "and a sentence after else is still a sentence",
    },
    RuleCase {
        source: "// 'outer: loop {\n",
        expected: &[("CH001", 1)],
        name: "a label may sit in front of a loop",
    },
    RuleCase {
        source: "// 'a: {\n",
        expected: &[("CH001", 1)],
        name: "and in front of a bare block",
    },
    RuleCase {
        source: "// 'a: if x > 1 {\n",
        expected: &[],
        name: "but rustc names what may follow a label, and if is not in the set",
    },
    RuleCase {
        source: "// 'a: the worker parks until the queue drains {\n",
        expected: &[],
        name: "nor is a sentence",
    },
    RuleCase {
        source: "// const LIMIT: usize;\n",
        expected: &[("CH001", 1)],
        name: "a const may be declared with no initializer, as in a trait",
    },
    RuleCase {
        source: "// static mut FOREIGN: u8;\n",
        expected: &[("CH001", 1)],
        name: "and a static, as in an extern block",
    },
    RuleCase {
        source: "// const LIMIT: [u8; 32];\n",
        expected: &[("CH001", 1)],
        name: "reading the same type the uninitialized binding reads",
    },
    RuleCase {
        source: "// static analysis: the queue drains before the worker parks;\n",
        expected: &[],
        name: "so three bare words are prose there too",
    },
    RuleCase {
        source: "// type Item;\n",
        expected: &[("CH001", 1)],
        name: "an associated type may be declared with no definition",
    },
    RuleCase {
        source: "// type Iter: Iterator<Item = Self::Item>;\n",
        expected: &[("CH001", 1)],
        name: "with a bound that holds an equals sign of its own",
    },
    RuleCase {
        source: "// type I: Into<[u8; 32]>;\n",
        expected: &[("CH001", 1)],
        name: "and one that holds an array, which ends no declaration",
    },
    RuleCase {
        source: "// type Item where Self: Sized;\n",
        expected: &[("CH001", 1)],
        name: "and a where clause with no bound at all",
    },
    RuleCase {
        source: "// pub type Item;\n",
        expected: &[],
        name: "but E0449 refuses a visibility there, so this rule does too",
    },
    RuleCase {
        source: "// type Item: the reader may skip this;\n",
        expected: &[],
        name: "and three bare words after the colon are a sentence",
    },
    RuleCase {
        source: "// type erasure is documented above;\n",
        expected: &[],
        name: "as is a sentence that merely opens with the keyword",
    },
    RuleCase {
        source: "// type a::b is re-exported for callers;\n",
        expected: &[],
        name: "and a path separator is not a bound, which the prose sweep found",
    },
    RuleCase {
        source: "// type I: ::std::fmt::Debug;\n",
        expected: &[("CH001", 1)],
        name: "though a bound may still start at the crate root",
    },
    RuleCase {
        source: "// fn stale() -> [u8; 32] {\n",
        expected: &[("CH001", 1)],
        name: "an array may be a return type",
    },
    RuleCase {
        source: "// const STALE: [u8; 32] = [0; 32];\n",
        expected: &[("CH001", 1)],
        name: "and the type of an initialized const",
    },
    RuleCase {
        source: "// static mut STALE: [u8; 32] = [0; 32];\n",
        expected: &[("CH001", 1)],
        name: "and of a static",
    },
    RuleCase {
        source: "// let buf: [u8; 32] = [0; 32];\n",
        expected: &[("CH001", 1)],
        name: "and of a binding that has an initializer",
    },
    RuleCase {
        source: "// let (a, b): ([u8; 4], u8) = t;\n",
        expected: &[("CH001", 1)],
        name: "and of one destructured",
    },
    RuleCase {
        source: "// if buf == [0u8; 32] {\n",
        expected: &[("CH001", 1)],
        name: "an array repeat may sit in a condition",
    },
    RuleCase {
        source: "// } else if buf == [0u8; 32] {\n",
        expected: &[("CH001", 1)],
        name: "in the tail of an else as well",
    },
    RuleCase {
        source: "// foo::<[u8; 32]>();\n",
        expected: &[("CH001", 1)],
        name: "and in a turbofish, which is a type position",
    },
    RuleCase {
        source: "// if the queue is paused the worker parks {\n",
        expected: &[],
        name: "but the prose lookahead is untouched by any of it",
    },
    RuleCase {
        source: "// retries = retries + 1;\n",
        expected: &[("CH001", 1)],
        name: "an assignment's right-hand side is an expression, with spaces",
    },
    RuleCase {
        source: "// self.count = self.count + 1;\n",
        expected: &[("CH001", 1)],
        name: "through a field as well",
    },
    RuleCase {
        source: "// retries += retries + 1;\n",
        expected: &[("CH001", 1)],
        name: "and compounded",
    },
    RuleCase {
        source: "// count = the number of rows;\n",
        expected: &[],
        name: "but three bare words after the equals are a sentence",
    },
    RuleCase {
        source: "// return early, before the lock is taken;\n",
        expected: &[],
        name: "and return keeps one token, because that guard does not transfer",
    },
    RuleCase {
        source: "// mode = legacy default;\n",
        expected: &[],
        name: "two bare words after the equals are prose, not an expression",
    },
    RuleCase {
        source: "// retries = count as usize;\n",
        expected: &[("CH001", 1)],
        name: "but a cast is bare words that rustc accepts",
    },
    RuleCase {
        source: "// let retries = count as usize;\n",
        expected: &[("CH001", 1)],
        name: "in a binding as well, from the same fragment",
    },
    RuleCase {
        source: "// const timeout: duration = legacy default;\n",
        expected: &[],
        name: "an initializer is an expression, and two bare words are not one",
    },
    RuleCase {
        source: "// const TIMEOUT: Duration = Duration::from_secs(5);\n",
        expected: &[("CH001", 1)],
        name: "though a real initializer still reports",
    },
    RuleCase {
        source: "// trait object;\n",
        expected: &[],
        name: "rustc answers \"expected {}\" for a trait that ends at a semicolon",
    },
    RuleCase {
        source: "// enum value;\n",
        expected: &[],
        name: "and for an enum",
    },
    RuleCase {
        source: "// union type;\n",
        expected: &[],
        name: "and a union wants where or a brace",
    },
    RuleCase {
        source: "// struct Foo;\n",
        expected: &[("CH001", 1)],
        name: "but a unit struct really does end at one",
    },
    RuleCase {
        source: "// struct Foo(u8, u8);\n",
        expected: &[("CH001", 1)],
        name: "as does a tuple struct, which only a struct may be",
    },
    RuleCase {
        source: "// *counter = counter + 1;\n",
        expected: &[("CH001", 1)],
        name: "an assignment may go through a dereference",
    },
    RuleCase {
        source: "// *self.count += 1;\n",
        expected: &[("CH001", 1)],
        name: "compounded, and through a field",
    },
    RuleCase {
        source: "// * the queue drains = the worker parks;\n",
        expected: &[],
        name: "but a bullet is not a dereference, because the star must abut",
    },
    RuleCase {
        source: "// *(counter) = counter + 1;\n",
        expected: &[("CH001", 1)],
        name: "a dereference target may be parenthesized",
    },
    RuleCase {
        source: "// *(ptr.add(1)) = value;\n",
        expected: &[("CH001", 1)],
        name: "holding a call of its own",
    },
    RuleCase {
        source: "// *(the queue drains) = the worker parks;\n",
        expected: &[],
        name: "though the prose guard still reads the right-hand side",
    },
    RuleCase {
        source: "// *(the queue drains) = x;\n",
        expected: &[],
        name: "and the target, which is where that sentence actually sits",
    },
    RuleCase {
        source: "// let ref stale = String::new();\n",
        expected: &[("CH001", 1)],
        name: "a binding mode may sit in front of the name",
    },
    RuleCase {
        source: "// let ref mut stale = String::new();\n",
        expected: &[("CH001", 1)],
        name: "in that order, which is the only one rustc takes",
    },
    RuleCase {
        source: "// let ref x: String;\n",
        expected: &[("CH001", 1)],
        name: "and on an uninitialized binding, which the report did not name",
    },
    RuleCase {
        source: "// let mut ref x = 1;\n",
        expected: &[],
        name: "but not reversed, because E0658 refuses it",
    },
    RuleCase {
        source: "// let ref counter = the number of rows;\n",
        expected: &[],
        name: "and the prose guard still reads the initializer",
    },
    RuleCase {
        source: "// #[Retry = 3 attempts remaining]\n",
        expected: &[],
        name: "prose after a number is not an attribute value",
    },
    RuleCase {
        source: "// #[deprecated = \"use foo\" instead]\n",
        expected: &[],
        name: "nor after a string, which rustc refuses the same way",
    },
    RuleCase {
        source: "// #[Retry = 3]\n",
        expected: &[("CH001", 1)],
        name: "though a bare number is one",
    },
    RuleCase {
        source: "// #[Retry = 3 + 1]\n",
        expected: &[("CH001", 1)],
        name: "and an operator may follow it",
    },
    RuleCase {
        source: "// #[doc = r#\"raw \"inner\" text\"#]\n",
        expected: &[("CH001", 1)],
        name: "a raw string is a value too, which the opener could not read",
    },
    RuleCase {
        source: "// #[doc = \"text with ] bracket\"]\n",
        expected: &[("CH001", 1)],
        name: "and a bracket inside a string closes nothing",
    },
    RuleCase {
        source: "// async move {};\n",
        expected: &[("CH001", 1)],
        name: "a block expression may be a statement",
    },
    RuleCase {
        source: "// const { let value = 1; };\n",
        expected: &[("CH001", 1)],
        name: "a const block among them",
    },
    RuleCase {
        source: "// unsafe {};\n",
        expected: &[("CH001", 1)],
        name: "and an unsafe block, which the report did not name",
    },
    RuleCase {
        source: "// move {};\n",
        expected: &[],
        name: "but move opens no block, which rustc says outright",
    },
    RuleCase {
        source: "// const {the shard map is stale};\n",
        expected: &[],
        name: "and a braced sentence is still a sentence",
    },
    RuleCase {
        source: "// timeout = 30-second default;\n",
        expected: &[],
        name: "a hyphen ends no atom pair, so this is still two adjacent words",
    },
    RuleCase {
        source: "// x = a - b;\n",
        expected: &[("CH001", 1)],
        name: "but an operator between them makes an expression",
    },
    RuleCase {
        source: "// TODO: fix -/-#1\n",
        expected: &[("CH002", 1)],
        name: "a repository coordinate begins with an alphanumeric",
    },
    RuleCase {
        source: "// TODO: fix -owner/repo#1\n",
        expected: &[("CH002", 1)],
        name: "on the owner side as well",
    },
    RuleCase {
        source: "// #[Retry = 3 + attempts remaining]\n",
        expected: &[],
        name: "an operator satisfies the first atom, not the whole value",
    },
    RuleCase {
        source: "// #[Note (see below) and more]\n",
        expected: &[],
        name: "and the delimiter branch is guarded too",
    },
    RuleCase {
        source: "// Foo::<u8>::bar();\n",
        expected: &[("CH001", 1)],
        name: "a turbofish may sit between path segments",
    },
    RuleCase {
        source: "// self.foo::<u8>();\n",
        expected: &[("CH001", 1)],
        name: "as well as after a method segment",
    },
    RuleCase {
        source: "// ::std::mem::drop(value);\n",
        expected: &[("CH001", 1)],
        name: "a call may start at the crate root, and had no fixture until now",
    },
    RuleCase {
        source: "// <Foo as Bar>::baz();\n",
        expected: &[("CH001", 1)],
        name: "and may name a trait method through a qualified path",
    },
    RuleCase {
        source: "// <b>bold</b> text here;\n",
        expected: &[],
        name: "but a tag is not a qualified path",
    },
    RuleCase {
        source: "// <Vec<u8> as Bar>::baz();\n",
        expected: &[("CH001", 1)],
        name: "and a qualified path may itself be generic",
    },
    RuleCase {
        source: "// timeout = legacy default + jitter;\n",
        expected: &[],
        name: "adjacent atoms are prose wherever they sit, not only at the end",
    },
    RuleCase {
        source: "// msg = format!(\"the queue is paused\");\n",
        expected: &[("CH001", 1)],
        name: "though a code signal makes an expression of any words it holds",
    },
    RuleCase {
        source: "// #[doc = include_str!(\"../README.md\")]\n",
        expected: &[("CH001", 1)],
        name: "and an attribute may hold a macro call",
    },
    RuleCase {
        source: "// #[This section is intentionally blank]\n",
        expected: &[],
        name: "but a sentence in brackets is not an attribute",
    },
    RuleCase {
        source: "// #[]\n",
        expected: &[],
        name: "and neither is nothing at all",
    },
    RuleCase {
        source: "// #[Note, this section is intentionally blank]\n",
        expected: &[],
        name: "nor a comma, which no attribute input may open with",
    },
    RuleCase {
        source: "// #[Note = this section is intentionally blank]\n",
        expected: &[],
        name: "and prose after an = is not an expression",
    },
    RuleCase {
        source: "// #[doc = \"this section is intentionally blank\"]\n",
        expected: &[("CH001", 1)],
        name: "though the same words inside a string are a value",
    },
    RuleCase {
        source: "// type Foo<T> where T: Copy = Vec<T>;\n",
        expected: &[("CH001", 1)],
        name: "and a type alias takes a where clause before its =",
    },
    RuleCase {
        source: "// #[allow[dead_code]]\n",
        expected: &[("CH001", 1)],
        name: "but a bracket delimiter is one",
    },
    RuleCase {
        source: "// #[allow{dead_code}]\n",
        expected: &[("CH001", 1)],
        name: "and so is a brace",
    },
    RuleCase {
        source: "// #[non_exhaustive]\n",
        expected: &[("CH001", 1)],
        name: "while a bare path still is",
    },
    RuleCase {
        source: "// impl<T: Into<Vec<u8>>> Tr<T> for R {\n",
        expected: &[("CH001", 1)],
        name: "on an impl as well as a function",
    },
    RuleCase {
        source: "// Compare a < b and c > d in the queue\n",
        expected: &[],
        name: "but prose that compares two things is prose",
    },
    RuleCase {
        source: "// pub fn one() -> u8 { 1 }\n",
        expected: &[("CH001", 1)],
        name: "with a value in it as well as empty",
    },
    RuleCase {
        source: "// pub struct S {}\n",
        expected: &[("CH001", 1)],
        name: "on a struct as well as a function",
    },
    RuleCase {
        source: "// impl Default for Q { fn default() -> Self { Q } }\n",
        expected: &[("CH001", 1)],
        name: "and however deep the braces go, so long as the line ends at one",
    },
    RuleCase {
        source: "// Returns the shard {0}\n",
        expected: &[],
        name: "but prose ending in a brace is prose",
    },
    RuleCase {
        source: "// match the shard {0}\n",
        expected: &[],
        name: "which is why control flow keeps its opening brace",
    },
    RuleCase {
        source: "// r#type::init();\n",
        expected: &[("CH001", 1)],
        name: "and in the path of a call",
    },
    RuleCase {
        source: "// crate::r#mod::log!(x);\n",
        expected: &[("CH001", 1)],
        name: "and in the path of a macro",
    },
    RuleCase {
        source: "// r#type.field = 1;\n",
        expected: &[("CH001", 1)],
        name: "and on the left of an assignment",
    },
    RuleCase {
        source: "// Use the LATER definition of the shard map here.\n",
        expected: &[],
        name: "but prose that opens with the word is prose",
    },
    RuleCase {
        source: "// pub use the cached resolver when the shard map is warm.\n",
        expected: &[],
        name: "and a path is one word, never a sentence",
    },
    RuleCase {
        source: "// #123 - TODO: remove the legacy fallback\n",
        expected: &[],
        name: "a reference abutting a marker on its left still tracks it",
    },
    RuleCase {
        source: "// https://example.com/x - TODO: remove the legacy fallback\n",
        expected: &[],
        name: "a URL does the same",
    },
    RuleCase {
        source: "// TODO(#1): a; #2 - TODO: b\n",
        expected: &[],
        name: "and a clause separator opens the run, so both markers are tracked",
    },
    RuleCase {
        source: "// Fixes #123. TODO: add retries\n",
        expected: &[("CH002", 1)],
        name: "but a reference in an earlier clause is not adjacent",
    },
    RuleCase {
        source: "// let r#match = parse();\n",
        expected: &[("CH001", 1)],
        name: "a raw identifier binds like any other",
    },
    RuleCase {
        source: "// let r#match: usize;\n",
        expected: &[("CH001", 1)],
        name: "with or without an initializer",
    },
    RuleCase {
        source: "// TODO: #123; TODO: add retries\n",
        expected: &[("CH002", 1)],
        name: "one reference tracks one marker, never the next one too",
    },
    RuleCase {
        source: "// pub use inner::{r#type};\n",
        expected: &[("CH001", 1)],
        name: "a raw identifier inside a grouped use tree",
    },
    RuleCase {
        source: "/// <code>outer <code>inner</code> TODO: add retry</code>\n/// TODO: add retry\n",
        expected: &[("CH002", 2)],
        name: "an inner closer ends the inner element, not the outer one",
    },
    RuleCase {
        source: "/// Explain \\``TODO: issue required` suffix.\n",
        expected: &[],
        name: "an escape takes one backtick, leaving the rest of the run",
    },
    RuleCase {
        source: "/// Intro `literal\n/// 2. TODO: issue required` suffix.\n",
        expected: &[],
        name: "a marker that cannot interrupt a paragraph opens no block",
    },
    RuleCase {
        source: "/// Intro `literal\n///\n/// 2. TODO: issue required` suffix.\n",
        expected: &[("CH002", 3)],
        name: "but after a blank line the same marker does",
    },
    RuleCase {
        source: "// macro_rules! stale { () => {}; }\n// The macro_rules! form is described in the guide below.\n",
        expected: &[("CH001", 1)],
        name: "a macro definition ends at its brace, and prose naming one does not",
    },
    RuleCase {
        source: "/// > Explain the `literal\n/// 2. TODO: issue required` suffix.\n",
        expected: &[("CH002", 2)],
        name: "a list leaving a quote is no lazy continuation of it",
    },
    RuleCase {
        source: "/// > Explain the `literal\n/// - TODO: issue required` suffix.\n",
        expected: &[("CH002", 2)],
        name: "a bullet does the same",
    },
    RuleCase {
        source: "/// | `literal | TODO: issue required` |\n/// | - | - |\n",
        expected: &[("CH002", 1)],
        name: "a code span cannot pair across two cells of a header row",
    },
    RuleCase {
        source: "/// | a | b |\n/// | - | - |\n/// | `literal | TODO: issue required` |\n",
        expected: &[("CH002", 3)],
        name: "nor across two cells of a body row",
    },
    RuleCase {
        source: "/// | a |\n/// | - |\n/// | `code TODO: marker` here |\n",
        expected: &[],
        name: "but within one cell it still pairs",
    },
    RuleCase {
        source: "// | `literal | TODO: issue required` |\n// | - | - |\n",
        expected: &[],
        name: "and a // comment has no table, so the span reaches across",
    },
    RuleCase {
        source: "/// > Explain the `literal\n///     ``` TODO: issue required` suffix.\n",
        expected: &[],
        name: "an over-indented delimiter opens no fence, so the quote carries on",
    },
    RuleCase {
        source: "/// > Explain the `literal\n/// ``` TODO: issue required` suffix.\n",
        expected: &[],
        name: "nor does one whose info string holds a backtick",
    },
    RuleCase {
        source: "/// > Explain the text\n/// ```\n/// let x = 1;\n/// ```\n",
        expected: &[],
        name: "but a real fence does, and exempts its own content",
    },
    RuleCase {
        source: "/// > Explain the text\n/// ```\n/// TODO: issue required\n",
        expected: &[],
        name: "including an unclosed one, which runs to the end of the run",
    },
    RuleCase {
        source: "/// > Explain the `literal\n///     ===\n/// TODO: issue required` suffix.\n",
        expected: &[],
        name: "an over-indented rule is content, so the quote carries on",
    },
    RuleCase {
        source: "/// > Explain the `literal\n/// ===\n/// TODO: issue required` suffix.\n",
        expected: &[],
        name: "and \"===\" underlines no paragraph it does not share a container with",
    },
    RuleCase {
        source: "/// > Explain the `literal\n/// ---\n/// TODO: issue required` suffix.\n",
        expected: &[("CH002", 3)],
        name: "but a thematic break really does end the quote",
    },
    RuleCase {
        source: "/// > Explain the `literal\n///     ---\n/// TODO: issue required` suffix.\n",
        expected: &[],
        name: "unless it is indented past its container",
    },
    RuleCase {
        source: "// > Explain the `literal\n// <pre> TODO: issue required` suffix.\n",
        expected: &[],
        name: "a tag opens no block in a // comment, so the quote carries on",
    },
    RuleCase {
        source: "/// > Actually, explain this behavior\n/// ===\n",
        expected: &[("CH003", 1)],
        name: "and an underline across a quote edge titles nothing",
    },
    RuleCase {
        source: "// Intro `literal\n// <pre>\n// 2. TODO: issue required` suffix.\n",
        expected: &[],
        name: "a tag in a // comment ends no paragraph, so the marker opens no list",
    },
    RuleCase {
        source: "// TODO: implement the retry described in\n// #123\n",
        expected: &[],
        name: "a marker's reference may wrap onto the next line",
    },
    RuleCase {
        source: "// TODO: add retries\n//\n// #123\n",
        expected: &[("CH002", 1)],
        name: "but a blank line ends the sentence, and the reference with it",
    },
    RuleCase {
        source: "// TODO: add retries\n// TODO(#123): other work\n",
        expected: &[("CH002", 1)],
        name: "and the next marker owns everything past itself",
    },
    RuleCase {
        source: "// TODO: add retries.\n// See #123 for parser.\n",
        expected: &[("CH002", 1)],
        name: "a finished sentence carries nothing from the line below it",
    },
    RuleCase {
        source: "// let (r#match, value) = parse();\n",
        expected: &[("CH001", 1)],
        name: "a raw name destructures like any other",
    },
    RuleCase {
        source: "// let [r#match, value] = parse();\n",
        expected: &[("CH001", 1)],
        name: "in a slice pattern as well as a tuple",
    },
    RuleCase {
        source: "// let r#Wrapper(inner) = parse();\n",
        expected: &[("CH001", 1)],
        name: "and in the path of a struct pattern",
    },
    RuleCase {
        source: "// let x: r#Type;\n",
        expected: &[("CH001", 1)],
        name: "and in the type of an uninitialized binding",
    },
    RuleCase {
        source: "// If the row is absent,\n// continue normally;\n",
        expected: &[],
        name: "continue takes a label, so a word after it is prose",
    },
    RuleCase {
        source: "// continue 'outer;\n",
        expected: &[("CH001", 1)],
        name: "but a label after it is the statement",
    },
    RuleCase {
        source: "// break normally;\n",
        expected: &[("CH001", 1)],
        name: "and break really does take a value",
    },
    RuleCase {
        source: "// TODO: add retries. See #123 for parser.\n",
        expected: &[("CH002", 1)],
        name: "a reference in the next sentence is not the marker's own",
    },
    RuleCase {
        source: "// TODO: update foo.rs per #123\n",
        expected: &[],
        name: "but a period inside a token ends no sentence",
    },
    RuleCase {
        source: "// TODO: support v1.2 under #123\n",
        expected: &[],
        name: "nor one inside a version",
    },
    RuleCase {
        source: "// TODO: fix this e.g. per #123\n",
        expected: &[],
        name: "nor one closing an abbreviation",
    },
    RuleCase {
        source: "// TODO: retain approx. 5 retries under #123\n",
        expected: &[],
        name: "nor any other abbreviation, because a digit begins no sentence",
    },
    RuleCase {
        source: "// TODO: use e.g. Postgres for #123\n",
        expected: &[],
        name: "and the guarded ones hold even before a capital",
    },
    RuleCase {
        source: "// TODO: retain approx. 5 retries. See #123 for the parser.\n",
        expected: &[("CH002", 1)],
        name: "but a real next sentence still ends the marker's",
    },
    RuleCase {
        source: "// TODO: ask Dr. Smith about #123\n",
        expected: &[],
        name: "a title before a name is the case the next token cannot read",
    },
    RuleCase {
        source: "// TODO: compare vs. Baseline under #123\n",
        expected: &[],
        name: "and so is any listed abbreviation before a capital",
    },
    RuleCase {
        source: "// TODO: deploy to the U.S. East region under #123\n",
        expected: &[],
        name: "an initialism is read by its shape, not from the list",
    },
    RuleCase {
        source: "// TODO: run at 9 a.m. Then check #123\n",
        expected: &[],
        name: "in lower case as well as capitals",
    },
    RuleCase {
        source: "/// > <pre>\n/// TODO: issue required\n",
        expected: &[("CH002", 2)],
        name: "an HTML block is scoped to the container that opened it",
    },
    RuleCase {
        source: "/// > <pre>\n/// > TODO: exempt, the block never closed\n",
        expected: &[],
        name: "but stays open across a quote that never ends",
    },
    RuleCase {
        source: "/** <pre>\n * sample\n * /* </pre> */\n * TODO: issue required\n */\n",
        expected: &[("CH002", 4)],
        name: "an HTML closer inside a nested comment still closes the block",
    },
    RuleCase {
        source: "/** <pre>\n * sample\n * /* note */\n * TODO: exempt, still inside the block\n */\n",
        expected: &[],
        name: "but only when the nested comment actually carries one",
    },
    RuleCase {
        source: "/// > | h |\n/// > | - |\n/// ordinary paragraph\n/// 22. item\n///     ```\n///     TODO: issue required\n",
        expected: &[("CH002", 6)],
        name: "a table is scoped to the container that opened it",
    },
    RuleCase {
        source: "/// > | h |\n/// > | - |\n/// > 22. item\n/// >     ```\n/// >     TODO: fixture placeholder\n/// >     ```\n",
        expected: &[],
        name: "but a confirmed table still ends the paragraph inside one quote",
    },
    RuleCase {
        source: "/// Intro paragraph\n/// 2. ```rust\n///    TODO: add retry\n",
        expected: &[("CH002", 3)],
        name: "a marker the paragraph refuses is not peeled into a fence",
    },
    RuleCase {
        source: "/// Intro paragraph\n/// 1. ```rust\n///    TODO: fixture placeholder\n///    ```\n",
        expected: &[],
        name: "but a marker of value one still opens one",
    },
    RuleCase {
        source: "/// > <pre>\n/// word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26.\n",
        expected: &[("CH007", 2)],
        name: "the prose scanner scopes an HTML block to its container too",
    },
    RuleCase {
        source: "/// > | h |\n/// > | - |\n/// word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26.\n",
        expected: &[("CH007", 3)],
        name: "and a table there the same way",
    },
    RuleCase {
        source: "/** <pre>\n * sample\n * /* </pre> */\n * word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26.\n */\n",
        expected: &[("CH007", 4)],
        name: "and the prose scanner sees a nested closer too",
    },
    RuleCase {
        source: "/// - <pre>\n/// word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26.\n",
        expected: &[("CH007", 2)],
        name: "an HTML block opened by its own list marker is scoped to it",
    },
    RuleCase {
        source: "/// - item\n///   > | h |\n///   > | - |\n/// word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26.\n",
        expected: &[("CH007", 4)],
        name: "a table's scope survives leaving the quote it opened in, inside a list",
    },
    RuleCase {
        source: "/// - | h |\n///   | - |\n/// word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26.\n",
        expected: &[("CH007", 3)],
        name: "a table opened by its own list marker is scoped to it",
    },
    RuleCase {
        source: "/// Intro paragraph\n/// > 2. ```rust\n/// >    TODO: fixture placeholder\n/// >    ```\n",
        expected: &[],
        name: "a marker right after a quote crossing is not refused the paragraph's veto",
    },
    RuleCase {
        source: "/** <pre>\n * raw /* </pre> */ TODO: issue required\n */\n",
        expected: &[],
        name: "text after the closer on its own physical line stays inside the block",
    },
    RuleCase {
        source: "/** > <pre>\n * > raw /*\n * unquoted text\n * */\n * > TODO: issue required\n */\n",
        expected: &[("CH002", 5)],
        name: "leaving the container inside a nested comment still closes the block",
    },
    RuleCase {
        source: "/** > <pre>\n * > raw /*\n * unquoted text\n * */\n * > word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26.\n */\n",
        expected: &[("CH007", 3)],
        name: "and the prose scanner sees it too",
    },
    RuleCase {
        source: "/// > Intro paragraph\n/// > 2. ```rust\n/// >    TODO: issue required\n",
        expected: &[("CH002", 3)],
        name: "the paragraph veto survives a quote that stays at the same depth",
    },
    RuleCase {
        source: "/** <pre>\n * </pre> /* TODO: inner */ TODO: suffix\n */\n",
        expected: &[],
        name: "a closer that starts its own piece still keeps the rest of its line",
    },
    RuleCase {
        source: "/** <pre>\n * </pre> /* inner */ word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26.\n */\n",
        expected: &[],
        name: "and the prose scanner keeps it too",
    },
    RuleCase {
        source: "/** <div>\n * raw /*\n *\n * */\n * TODO: issue required\n */\n",
        expected: &[("CH002", 5)],
        name: "a blank line closing a type-6 block still closes it past a nested comment",
    },
    RuleCase {
        source: "/** <div>\n * raw /*\n *\n * */\n * word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26.\n */\n",
        expected: &[("CH007", 5)],
        name: "and the prose scanner sees the same close",
    },
    RuleCase {
        source: "/** | h |\n * | - |\n * raw /*\n * # Heading\n * */\n * TODO: issue required\n */\n",
        expected: &[("CH002", 6)],
        name: "a block starting inside a nested comment still ends the table past it",
    },
    RuleCase {
        source: "/** | h |\n * | - |\n * raw /*\n * # Heading\n * */\n * word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26.\n */\n",
        expected: &[("CH007", 6)],
        name: "and the prose scanner sees the same close",
    },
    RuleCase {
        source: "/** <pre></pre> /* TODO: inner */ TODO: suffix\n */\n",
        expected: &[],
        name: "a self-closing opener still keeps the rest of its own line",
    },
    RuleCase {
        source: "/** <pre></pre> /* inner */ word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25 word26.\n */\n",
        expected: &[],
        name: "and the prose scanner keeps it too",
    },
];

/// The boundary of CH001: commented-out Rust, against prose that opens with
/// a Rust keyword.
///
/// False positives landed here more than once, so both sides are pinned. A
/// false positive is the worse failure, because it fails CI on ordinary
/// English. But a rule that catches nothing is not a rule. Each entry is a
/// line, and whether it is commented-out code.
pub const CODE_SHAPE_TESTS: &[(&str, bool)] = &[
    ("let (left, right) = split();", true),
    ("let [a, b] = arr;", true),
    ("let mut (a, b) = t;", true),
    ("let (a, b): (u8, u8) = t;", true),
    ("let Foo { x, y } = value;", true),
    ("let crate::Foo { x } = v;", true),
    ("let Some(v) = opt else {", true),
    ("let Ok(row) = fetch() else {", true),
    ("let (Some(a), Some(b)) = pair else {", true),
    ("let [first, ..] = slice else {", true),
    ("let mut retries: usize;", true),
    ("let buf: Vec<u8>;", true),
    ("let handle: &'a mut Worker;", true),
    ("let T: Send is required here;", false),
    ("let this be the rule: the worker parks;", false),
    ("let the caller decide;", false),
    ("let (or rather, allow) the worker retry;", false),
    ("let us assume the queue is paused;", false),
    ("let Some values be missing here;", false),
    (
        "let the reader see (a) the claim and (b) the release;",
        false,
    ),
    ("fn foo() {", true),
    ("pub fn bar();", true),
    ("fn foo(", true),
    ("fn foo(a: u8,", true),
    ("fn f(x: impl Send + Sync,", true),
    ("fn f(x: [u8; 4],", true),
    ("fn f(&self,", true),
    ("fn foo(the caller name appears, and so on,", false),
    ("struct Foo {", true),
    ("impl Foo {", true),
    ("use a::b;", true),
    ("use a::{b, c};", true),
    ("use crate::x as y;", true),
    ("let x = 1;", true),
    ("let mut v = Vec::new();", true),
    ("type A = B;", true),
    ("}", true),
    ("});", true),
    ("#[derive(Debug)]", true),
    ("assert_eq!(a, b);", true),
    ("anyhow::bail!(\"oops\");", true),
    ("if ready {", true),
    ("for item in items {", true),
    ("while let Some(x) = it.next() {", true),
    ("match state {", true),
    ("impl Foo {", true),
    ("impl<T> Trait for Foo {", true),
    ("value = compute();", true),
    ("self.count = 0;", true),
    ("count += 1;", true),
    ("retries -= 1;", true),
    ("flags |= READY;", true),
    ("bits <<= 2;", true),
    ("vec![1, 2];", true),
    ("my_macro!{ a: 1 };", true),
    ("see vec![1, 2] for the shape;", false),
    ("if the queue is paused, the worker parks {", false),
    (
        "impl the row has already been deleted by retention {",
        false,
    ),
    ("cleanup();", true),
    ("client.send(value).await?;", true),
    ("Type::method::<T>(value);", true),
    ("iter.collect::<Vec<_>>();", true),
    ("foo::<u8>(1)?;", true),
    ("see collect::<Vec<_>>() for the shape;", false),
    ("self.flush()?;", true),
    ("return Err(error);", true),
    ("break;", true),
    ("call cleanup() first, then retry;", false),
    ("see compute() for the details;", false),
    ("fn foo() is called by the wrapper.", false),
    (
        "fn resolve_call(the caller name appears in diagnostics",
        false,
    ),
    ("fn build() constructs the program; see below.", false),
    ("let the caller decide, since the row may be gone;", false),
    ("let x = the value the operator supplied;", false),
    (
        "type x = whatever the operator decided to configure;",
        false,
    ),
    ("use the LATER definition on a duplicate name;", false),
    ("use Foo, which the macro expands to;", false),
    ("struct directly; a malicious body must not flip it.", false),
    ("mod bar is documented in docs/architecture.md.", false),
    ("let bytes: [u8; 32];", true),
    ("let callback: Box<dyn Fn() + Send>;", true),
    ("let function: fn(u8) -> u8;", true),
    ("let ptr: *const u8;", true),
    ("let v: Vec<Box<dyn Error + Send + Sync>>;", true),
    ("let s: &'a [u8];", true),
    ("let a::b is re-exported for callers;", false),
    ("struct CountingLayer(Arc<Mutex<u64>>);", true),
    ("struct Wrapper(u8);", true),
    ("pub struct Foo<T>(T, T);", true),
    ("struct fields are described below;", false),
    ("struct (or enum) definitions live here;", false),
    ("struct Packet([u8; 32]);", true),
];

/// Each English contraction that CH006 must catch.
///
/// CH006 had three defects. Each came from a check by example, not from a
/// full list. They were the negative of "can", the modal perfects and the
/// interrogatives. This is the full list. The typographic apostrophes at
/// the end are there because an editor that substitutes one must not turn a
/// gated contraction into an invisible pass.
pub const CONTRACTIONS_EXPECTED: &[&str] = &[
    "can't",
    "won't",
    "don't",
    "doesn't",
    "didn't",
    "isn't",
    "aren't",
    "wasn't",
    "weren't",
    "hasn't",
    "haven't",
    "hadn't",
    "wouldn't",
    "couldn't",
    "shouldn't",
    "mustn't",
    "needn't",
    "oughtn't",
    "mightn't",
    "shan't",
    "ain't",
    "daren't",
    "it's",
    "that's",
    "there's",
    "here's",
    "what's",
    "who's",
    "how's",
    "where's",
    "when's",
    "why's",
    "he's",
    "she's",
    "let's",
    "we're",
    "they're",
    "you're",
    "we've",
    "they've",
    "you've",
    "i've",
    "should've",
    "could've",
    "would've",
    "must've",
    "might've",
    "we'll",
    "they'll",
    "you'll",
    "i'll",
    "it'll",
    "he'll",
    "she'll",
    "we'd",
    "they'd",
    "you'd",
    "i'd",
    "he'd",
    "she'd",
    "i'm",
    "can\u{2019}t",
    "isn\u{2019}t",
    "we\u{2019}re",
    "it\u{2019}s",
    "should\u{2019}ve",
    "won\u{2019}t",
];

/// Possessives and abbreviations that STE permits. A match is a false
/// positive. `the world's` is here because `world` was once a stem, and it
/// gave a false positive on this ordinary possessive.
pub const CONTRACTIONS_EXCLUDED: &[&str] = &[
    "one's",
    "someone's",
    "everyone's",
    "nobody's",
    "everything's",
    "something's",
    "nothing's",
    "the row's",
    "the queue's",
    "TTL'd",
    "the world's",
];

/// The keywords of the adversarial prose sweep.
///
/// The sweep crosses each Rust keyword that CH001 keys on with the sentence
/// shapes that this corpus writes. None of the results is code, so a match
/// is a false positive. A false positive on a Tier A rule fails CI on
/// ordinary English, which is worse than a missed defect.
///
/// The sweep exists because edits by hand to CH001 gave false positives
/// several times. The generated cross product catches them before a
/// reviewer does. It found six on one pass and four on another, each in a
/// branch that looked correct alone.
pub const SWEEP_KEYWORDS: &[&str] = &[
    "fn", "struct", "enum", "trait", "union", "mod", "const", "static", "type", "impl", "let",
    "use", "pub fn", "async fn", "if", "while", "for", "match", "loop", "unsafe", "return",
    "break", "continue", "self", "ctx", "cleanup", "value", "count",
];

/// The sentence shapes of the sweep, in Python `str.format` syntax.
pub const SWEEP_TAILS: &[&str] = &[
    "{n} handles the retry path.",
    "{n} is called by the wrapper.",
    "{n} resolve_call(the caller name appears in diagnostics",
    "{n} foo() returns a Resolution; see below.",
    "{n} Foo {{ .. }} is the shape we persist.",
    "{n} the caller decide, since the row may be gone;",
    "{n} bar is documented in docs/architecture.md.",
    "{n} Foo, which the macro expands to;",
    "{n} x = the value the operator supplied;",
    "{n} T: Send is required here;",
    "{n} this module owns the sweep;",
    "{n} `Foo` implements Display for the CLI.",
    "{n} x = whatever the operator decided to configure;",
    "{n} a::b is re-exported for callers;",
    "{n} foo(the caller name appears, and so on,",
    "{n} the row, the timer, and the task,",
    "{n} cleanup() first, then retry;",
    "{n} runs before the sweep completes;",
    "{n} early, before the lock is taken;",
    "{n} the queue is paused, the worker parks {{",
    "{n} = the number of rows the sweep deleted;",
    "{n} the row has already been deleted by retention {{",
    "{n} we only ever see this on a cold start {{",
];
