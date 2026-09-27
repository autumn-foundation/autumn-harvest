#!/usr/bin/env python3
"""Quota-lock-ordering clone-class sync check for debounce.rs / throttle.rs.

Deterministic, reproducible on any checkout — no network access, no build.

`autumn-harvest/src/debounce.rs` and `autumn-harvest/src/throttle.rs` each
carry a byte-identical copy of the batch quota-lock-ordering algorithm that
closes the ABBA deadlock issue #1230 Finding 2 describes (two concurrent
scanner transactions claiming disjoint due-row batches that need the same
quota locks in opposite order). PR #1480's review history shows the two
copies co-edited in lockstep across roughly a dozen review rounds — every
fix to the algorithm (the sort-instead-of-prelock redesign, the
hashtext-collision fix, the pure-function split) landed in both files in
the same commit.

An Echo duplication survey (this repo's scheduled clone-detection agent,
issue #1695) confirmed the two copies are still byte-identical and
evaluated merging them into one shared helper. It did not: the clone
class has exactly 2 instances and no missed-fix defect is on record (a
fix landing in one copy and staying stale in the other), so it falls
short of the project's 2-instances-needs-a-missed-fix merge bar.

Falling short of that bar does not make the risk go away — a future fix
applied to one copy and not its sibling would be exactly the missed-fix
defect the bar is designed to catch, and it would fail silently: nothing
else in this crate reads or calls across the two files to notice. This
script is the substitute for the abstraction: it fails CI the moment the
two copies diverge, so the sync work review already had to do by hand
across PR #1480 stays enforced automatically instead of by convention.

Scope is five functions confirmed byte-identical (four outright, one after
normalizing one known field-name difference — see `FIELD_NORMALIZATIONS`
below) by direct diff: `resolve_quota_lock_ids`,
`order_rows_by_quota_lock_id`, `snapshot_quota_policies`,
`order_due_rows_for_deadlock_free_firing` (the wrapper composing the first
three), and `resolve_row_quota_lock_key`. Codex review on PR #1696 found
both the wrapper and the resolver belonged in the tracked set: a change to
how the wrapper composes the other functions, or to the resolver's
policy/key logic, would drift the two copies while every other tracked
function stayed individually unchanged.

`resolve_row_quota_lock_key` reads its row's input from a field each
file's own `FireDueRow` names differently (`last_input` in debounce.rs,
`input` in throttle.rs) — the one structural difference the two scanners'
row shapes actually require. `FIELD_NORMALIZATIONS` substitutes a common
placeholder for that one field access before comparing, so the rest of
the function (the policy lookup, the `has_any_cap` guard, the resolved-key
construction) is still checked byte-for-byte.

Text identity is not the whole invariant: `order_due_rows_for_deadlock_free_firing`
being correct and unchanged does not help if a fire path stops calling it.
Codex review on PR #1696 also found that gap: this script did not check
that either scanner's claim loop still calls the wrapper, so removing or
bypassing that one call site would restore claim-order firing (and its
ABBA deadlock risk) while every tracked function stayed identical.
`CALL_SITE_GUARDS` closes it: for each scanner's `fire_due_on_conn`, it
requires a `let <var> = wrapper_call(...);`-shaped assignment followed by
a `for _ in <var>` loop over that SAME variable — not just the wrapper
call and a loop appearing in the right textual order. Codex review on PR
#1696 found the weaker, order-only check still passed when the wrapper's
result was discarded (`wrapper_call(conn, due_rows.clone()).await?;`,
unused) while the loop kept iterating the untouched original, and when
the wrapper was merely named inside a comment. Tying the loop's variable
to the assignment's, over comment/string-masked text, closes both. A
further round found that tying the NAME alone was still not enough:
shadowing the binding again, reordering the result back in place with a
mutating call, or reaching it through an index/slice projection
(`due_rows[..].reverse()`) all pass a check that only confirms the same
identifier reaches a later loop. Rather than keep enumerating mutating
shapes to reject — the pattern the next two review rounds fell into —
`check_call_site_guard` now WHITELISTS the few read-only queries the real
code needs (`_ALLOWED_READ_ONLY_METHODS`) and fails on any other mention
of the variable at all between the assignment and the loop. A further
round found the assignment and loop patterns themselves were too loose
at their ENDS: `[^;]*?` backtracks through a call chained onto the
wrapper's own result (`.await?.into_iter().rev().collect()`, still
matching because the lazy group can expand past the wrapper's own closing
paren to find some LATER one before the next `;`), and the loop pattern's
`\b` boundary let an adaptor chained onto the loop's iterable
(`due_rows.into_iter().rev()`) still count as "in `due_rows`". Matching
the wrapper's argument list by real paren depth (`find_matching_paren`,
over the same masked text) and requiring the loop's iterable to be
exactly `var {` closes both.

Every fix so far still only looked AFTER the assignment. A later round
found this check finds one correctly ordered assignment/loop pair
anywhere in the function and calls it satisfied — it never checked
whether an EARLIER branch could fire rows first. A special-case branch
that loops over the raw, unordered `due_rows` and returns, before the
ordering assignment further down is ever reached, still passes: some
transactions take that branch and claim-order-fire regardless. Rejecting
any `for _ in var {` loop found before the assignment closes the
concrete case. A follow-up round found a one-line evasion of that fix:
rebinding the pre-assignment variable to a new name first (`let
unordered_rows = due_rows;`) and looping over THAT. Tracking single-hop
aliases this way closes the demonstrated case too, but this is a
declared, deliberate stopping point, not a promise of soundness: chained
aliasing, passing the variable into a helper function, a struct field,
or a tuple destructure all remain undetected, and proving the assignment
dominates every possible control-flow and data-flow path in full
generality would need real static analysis, not text scanning. Past
this point, a finding in this family is a documented limitation of the
approach, not a fix this script can keep absorbing indefinitely. One
explicitly considered and declined case: renaming the wrapper's OUTPUT
binding (`let ordered_rows = wrapper_call(conn, due_rows).await?;`)
would let an early branch loop over the still-unaliased `due_rows`
argument and pass, since only the output binding and its own aliases are
tracked. Tracking the wrapper's input argument as a second,
independently-aliasable name is the same generalization, one hop over —
declined for the same reason, and because neither tracked file ever
renames the result in practice; both always shadow the same name.

`extract_function` itself had a gap at its very first step: it searched
for a tracked function's signature in unmasked text, so a signature-shaped
line inside a preceding block comment or doc-comment code example (typed
out as sample usage) could match instead of the real function further
down — extracting and comparing two copies' documentation examples
instead of their real implementations, with no error to signal it.
Signature and opening-brace search now run over comment/string-masked
text; only the function span actually returned is read back from the
original, so real comments and strings inside a tracked function's body
still compare byte-for-byte as before.

Function extraction matches braces through `find_matching_brace`, not a
raw character count. Codex review on PR #1696 found the raw count could
be desynced by a brace inside a line comment, a block comment, or a
string literal (including a raw string): a stray `}` truncates a tracked
function early, silently hiding a real divergence after it; a stray `{`
never finds its match and runs the scan past the end of the file.
`find_matching_brace` walks the same comment/string lexical structure
Rust itself does, so only a brace the grammar would count changes depth.
A follow-up review round then found the raw-string handling itself
routed a zero-hash `r"..."` (an ordinary, common raw string) through the
escape-aware normal-string scan, so a literal like `r"\"` skipped past
its own closing quote. `_skip_raw_string_literal` never treats `\` as an
escape, regardless of hash count. A further round found char literals
(`'{'`, `'}'`, including a Unicode escape whose own source text contains
braces, like a char literal spelling `{` as `'\\u{{7B}}'`) were not
skipped either; `_try_skip_char_literal`
closes that, while still leaving a lifetime like `'a` as an ordinary
character since it never closes with a matching quote.

Usage:
    python3 docs/audits/quota-lock-ordering-sync.py

Exit code is 1 if either copy is missing a tracked function, if any
tracked function's text has diverged between the two files, or if a
guarded call site no longer calls the ordering wrapper before its firing
loop; 0 otherwise.
"""
import difflib
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DEBOUNCE = REPO_ROOT / "autumn-harvest" / "src" / "debounce.rs"
THROTTLE = REPO_ROOT / "autumn-harvest" / "src" / "throttle.rs"

# The clone class confirmed byte-identical (outright, or after
# FIELD_NORMALIZATIONS) between debounce.rs and throttle.rs (issue #1230
# Finding 2 / PR #1480).
TRACKED_FUNCTIONS = [
    "resolve_quota_lock_ids",
    "order_rows_by_quota_lock_id",
    "snapshot_quota_policies",
    "order_due_rows_for_deadlock_free_firing",
    "resolve_row_quota_lock_key",
]

# Per-function field-name normalization: a tracked function whose two
# copies read one field under a name the row type itself forces to
# differ. Each entry replaces a debounce.rs-side substring and a
# throttle.rs-side substring with the same placeholder before comparing,
# so the rest of the function is still held to a byte-identical bar. A
# function not listed here is compared with no normalization at all.
FIELD_NORMALIZATIONS: dict[str, tuple[str, str]] = {
    "resolve_row_quota_lock_key": ("row.last_input", "row.input"),
}
NORMALIZED_PLACEHOLDER = "row.__normalized_input_field__"

# Each entry names an enclosing function, in both files, that must bind
# `wrapper_call`'s result to a variable and then loop over that SAME
# variable — the invariant that a claimed batch is reordered before any
# row fires, and that the reordered batch (not a discarded clone, not the
# original) is what actually fires. This is a structural check on the
# CALLER, not a text comparison of the wrapper itself.
CALL_SITE_GUARDS = [
    {
        "enclosing_fn": "fire_due_on_conn",
        "wrapper_call": "order_due_rows_for_deadlock_free_firing",
    },
]

FN_SIGNATURE_RE_TEMPLATE = r"\n(?:async )?fn {name}\s*\("

# Matches a raw (or raw byte/raw C) string's opening delimiter: r"..., br"...,
# cr"..., r#"..., etc. Requires the quote immediately after the hashes, so a
# raw identifier like `r#type` (no quote) is never mistaken for one. The
# leading negative lookbehind requires a token boundary before the `b`/`c`/`r`:
# Codex review on PR #1696 found that without it, an identifier merely ending
# in `r` right before an ordinary string — `bar"\"}"`, a valid adjacent macro
# token pair — matched at that trailing `r` and misclassified the following
# normal, escape-aware string as a raw one. A follow-up round found the
# lookbehind's excluded set still missed `'`: `'r"\"}"` tokenizes in Rust as
# the lifetime `'r` followed by an ordinary string, but this scanner treats
# the bare `'` as an ordinary character (correctly, per `_try_skip_char_literal`
# recognizing it as a lifetime, not a char literal) and then matched raw-string
# open at the very next `r`, since a bare `'` is not itself a word character.
# Excluding a preceding `'` too closes that; the rare, contrived case of a
# raw string genuinely and adjacently preceded by a char literal's closing
# quote (`'a'r"..."`, no space) is not worth the ambiguity it would reopen.
_RAW_STRING_OPEN_RE = re.compile(r"(?<![A-Za-z0-9_'])(?:b|c)?r(#*)\"")


def _skip_line_comment(text: str, i: int) -> int:
    j = text.find("\n", i)
    return len(text) if j == -1 else j + 1


def _skip_block_comment(text: str, i: int) -> int:
    """`text[i:i+2]` is `/*`. Return the index just past the matching `*/`,
    honoring Rust's nested block comments."""
    n = len(text)
    depth = 1
    i += 2
    while i < n and depth > 0:
        two = text[i : i + 2]
        if two == "/*":
            depth += 1
            i += 2
        elif two == "*/":
            depth -= 1
            i += 2
        else:
            i += 1
    return i


def _skip_string_literal(text: str, i: int) -> int:
    """`i` is just past the opening quote of a NORMAL (non-raw) string.
    Return the index just past the closing quote. A backslash escapes the
    next character, so an escaped quote never ends the literal early."""
    n = len(text)
    while i < n:
        if text[i] == "\\":
            i += 2
            continue
        if text[i] == '"':
            return i + 1
        i += 1
    return n


def _skip_raw_string_literal(text: str, i: int, hashes: int) -> int:
    """`i` is just past the opening `r#*"` of a raw string. Return the
    index just past its closer (`"` followed by exactly `hashes` `#`s).

    A raw string does no escape processing at all — not even for its own
    quote character — so this never treats `\\` specially. Codex review on
    PR #1696 found that routing a zero-hash raw string (plain `r"..."`,
    the common case) through the escape-aware normal-string scan treated
    its backslashes as escapes, so a literal like `r"\\"` (one backslash,
    a valid, unremarkable raw string) skipped past its own closing quote
    and ran the scan past the function or off the end of the file. `hashes
    == 0` is a normal raw string, not a signal to fall back to escaping.
    """
    n = len(text)
    closer = '"' + "#" * hashes
    j = text.find(closer, i)
    return n if j == -1 else j + len(closer)


def _try_skip_char_literal(text: str, i: int) -> int | None:
    """`text[i]` is `'`. If it opens a genuine char (or byte-char, `b'x'` —
    the leading `b` needs no special handling, since it is scanned as a
    plain character before this ever sees the quote) literal, return the
    index just past its closing `'`. Otherwise (a lifetime like `'a` or
    `'static`, which never closes with a matching quote) return `None` so
    the caller treats `'` as an ordinary character.

    A char literal is exactly one character, or one escape sequence,
    between two `'`s: a simple escape (`\\n`, `\\t`, `\\\\`, `\\'`, `\\"`,
    `\\0`), a byte escape (`\\xNN`), or a Unicode escape (`\\u{...}`, itself
    containing `{`/`}` that must not be counted as block braces either).
    """
    n = len(text)
    j = i + 1
    if j >= n:
        return None
    if text[j] == "\\":
        k = j + 1
        if k >= n:
            return None
        if text[k] == "u" and k + 1 < n and text[k + 1] == "{":
            close = text.find("}", k + 2)
            if close == -1:
                return None
            k = close + 1
        elif text[k] == "x":
            k += 3
        else:
            k += 1
        return k + 1 if k < n and text[k] == "'" else None
    k = j + 1
    return k + 1 if k < n and text[k] == "'" else None


def find_matching_brace(text: str, open_index: int) -> int:
    """Return the index just past the `}` matching the `{` at `open_index`.

    A naive character scan miscounts a brace that appears inside a line
    comment, a block comment, or a string literal — Codex review on PR
    #1696 found this could either truncate a tracked function early (a
    stray `}` in a string silently drops the rest of the function from
    comparison, so a real divergence after it passes as identical) or run
    past the end of the file (a stray `{` in a string never finds its
    match, and the caller's index into `text` goes out of range). This
    walks the same lexical structure rustfmt would, so a brace only counts
    when Rust's own grammar would count it.
    """
    n = len(text)
    depth = 0
    i = open_index
    while i < n:
        c = text[i]
        if c == "/" and i + 1 < n and text[i + 1] == "/":
            i = _skip_line_comment(text, i)
            continue
        if c == "/" and i + 1 < n and text[i + 1] == "*":
            i = _skip_block_comment(text, i)
            continue
        raw_match = _RAW_STRING_OPEN_RE.match(text, i)
        if raw_match:
            i = _skip_raw_string_literal(text, raw_match.end(), len(raw_match.group(1)))
            continue
        if c == '"':
            i = _skip_string_literal(text, i + 1)
            continue
        if c == "'":
            char_end = _try_skip_char_literal(text, i)
            if char_end is not None:
                i = char_end
                continue
            # A lifetime (`'a`, `'static`), not a char literal: fall through
            # and advance past just the `'` like any ordinary character.
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return i + 1
        i += 1
    raise ValueError(f"unterminated brace starting at index {open_index}")


def find_matching_paren(text: str, open_index: int) -> int:
    """Return the index just past the `)` matching the `(` at `open_index`.

    Callers pass text already run through `mask_comments_and_strings`, so
    every `(`/`)` inside a comment, string, or char literal has already
    been blanked out — a plain depth count over the masked text is safe
    without re-doing the lexical skipping `find_matching_brace` needs.
    """
    n = len(text)
    depth = 0
    i = open_index
    while i < n:
        if text[i] == "(":
            depth += 1
        elif text[i] == ")":
            depth -= 1
            if depth == 0:
                return i + 1
        i += 1
    raise ValueError(f"unterminated parenthesis starting at index {open_index}")


def mask_comments_and_strings(text: str) -> str:
    """Return a same-length copy of `text` with every line comment, block
    comment, and string literal (raw strings included) blanked out to
    spaces (newlines kept, to leave line numbers meaningful).

    `check_call_site_guard` searches for a real assignment and a real loop
    in live code. Codex review on PR #1696 found it did not — a mention of
    the wrapper call inside a comment (`// order_due_rows_for_deadlock_free_firing(...)`,
    describing a bypass rather than performing one) would satisfy the same
    regex a real call does. Searching the masked text instead means only
    code the compiler would actually see can match. A follow-up round
    found the char-literal gap applied here too: an unmasked `'"'` (a
    valid char literal holding a quote) was mistaken for the start of a
    string, and the resulting scan for a closing `"` could blank out an
    unrelated, much later part of the function — including the real
    assignment and loop this check exists to find.
    """
    n = len(text)
    out = list(text)

    def blank(lo: int, hi: int) -> None:
        for k in range(lo, hi):
            if out[k] != "\n":
                out[k] = " "

    i = 0
    while i < n:
        c = text[i]
        if c == "/" and i + 1 < n and text[i + 1] == "/":
            j = _skip_line_comment(text, i)
            blank(i, j)
            i = j
            continue
        if c == "/" and i + 1 < n and text[i + 1] == "*":
            j = _skip_block_comment(text, i)
            blank(i, j)
            i = j
            continue
        raw_match = _RAW_STRING_OPEN_RE.match(text, i)
        if raw_match:
            j = _skip_raw_string_literal(text, raw_match.end(), len(raw_match.group(1)))
            blank(i, j)
            i = j
            continue
        if c == '"':
            j = _skip_string_literal(text, i + 1)
            blank(i, j)
            i = j
            continue
        if c == "'":
            char_end = _try_skip_char_literal(text, i)
            if char_end is not None:
                blank(i, char_end)
                i = char_end
                continue
        i += 1
    return "".join(out)


def extract_function(text: str, name: str) -> str | None:
    """Return `name`'s full source (signature through closing brace).

    Finds the signature line, then the first `{`, then matches braces to
    that function's own closing `}` via `find_matching_brace`, so a brace
    inside a comment or string literal in the function body cannot end the
    extraction early or run it off the end of the file. Doc comments and
    attributes above the signature are not included — deliberately, so the
    two copies' shared line-number-dependent `#[cfg(...)]` placement never
    causes a false divergence unrelated to the algorithm itself.

    The signature itself is searched for in comment/string-masked text.
    Codex review on PR #1696 found the earlier unmasked search could match
    a signature-shaped line sitting inside a preceding block comment or
    doc-comment code example (`fn snapshot_quota_policies() { ... }` typed
    out as sample usage) instead of the real function further down, and
    extraction would then follow that fake signature's own (separately
    balanced, so no crash) braces — silently comparing two copies'
    documentation examples instead of their real implementations. Masked
    text has the same length and offsets as `text`, so a match found in it
    locates the real signature and opening brace in the original text
    exactly; only the RETURNED span is read from unmasked `text`, so a
    real comment or string inside the function body is preserved verbatim
    in what gets compared.

    The body's opening brace is found only after skipping the parameter
    list's own balanced parens (`find_matching_paren`, reusing the same
    helper the call-site guard's argument-list matching needs). A
    follow-up round found the earlier "first `{` after the signature"
    search could stop at a brace inside the parameter list itself — a
    const-generic block expression in a parameter's type, `x: [();
    { const N: usize = 1; N }]`, is valid Rust — and extract only the
    signature prefix as if it were the whole function. None of the six
    functions this script actually tracks has anything like that in its
    signature; a brace surviving in a return type or `where` clause after
    the parameter list closes remains unhandled, the same declared,
    bounded stopping point as the alias-tracking limit above.
    """
    masked = mask_comments_and_strings(text)
    sig_re = re.compile(FN_SIGNATURE_RE_TEMPLATE.format(name=re.escape(name)))
    m = sig_re.search(masked)
    if not m:
        return None
    start = m.start() + 1  # skip the leading newline
    params_close = find_matching_paren(masked, m.end() - 1)
    open_brace = masked.index("{", params_close)
    end = find_matching_brace(text, open_brace)
    return text[start:end]


# Vec (and slice) methods that reorder or otherwise mutate a batch in
# place. An intervening call to one of these on the ordering wrapper's
# result, between its assignment and the firing loop, could undo the
# ordering the wrapper just established — the same hazard as skipping the
# wrapper entirely, just one step removed. Read-only calls (`.len()`,
# `.is_empty()`, `.iter()`, ...) are deliberately not in this list.
# The only intervening uses of the ordering wrapper's result variable that
# check_call_site_guard accepts between the assignment and the firing
# loop: plain, argument-free, read-only queries. Everything else —
# rebinding, direct assignment, indexing, a slice projection, or any
# method call not on this list, mutating or not — fails the guard.
#
# This is deliberately a whitelist, not a blacklist of mutating methods.
# Codex review on PR #1696 needed three rounds to close a blacklist
# (`.sort_by(...)`, then `.sort_by_cached_key(...)`, then
# `due_rows[..].reverse()` bypassing the method-name match entirely) --
# each fix just narrowed the next gap. A blacklist can only ever list the
# mutations someone thought of; a whitelist of the few reads the real
# code actually needs rejects everything ELSE by construction, including
# whatever mutating or reordering shape a future Rust API adds.
_ALLOWED_READ_ONLY_METHODS = ("len", "is_empty")


def check_call_site_guard(text: str, file_label: str, enclosing_fn: str, wrapper_call: str) -> str | None:
    """Return a failure message, or `None` if the guard holds.

    Finds `enclosing_fn`'s body, masks its comments and string literals
    (so a mention of `wrapper_call` in a comment cannot count), then
    requires a `let <var> = wrapper_call(...)[.await][?];`-shaped
    assignment followed later by a `for _ in <var>` loop over that SAME
    variable, with every intervening mention of `var` limited to an
    `_ALLOWED_READ_ONLY_METHODS` call. Matching the wrapper call and a
    loop independently, without tying them to one variable, is not
    enough: Codex review on PR #1696 found that calling the wrapper on a
    discarded clone (`wrapper_call(conn, due_rows.clone()).await?;`,
    result unused) still left a textual call before a textual loop over
    the untouched original `due_rows`. Tying the SAME name to both was
    not enough either: shadowing the binding again, reordering the result
    in place with a mutating method, or reaching it through an index or
    slice projection all pass a check that only confirms the identifier
    matches. Whitelisting the few reads the real code needs, and failing
    on anything else, closes the whole class at once rather than one
    mutating shape at a time. A further round found the assignment and
    loop needed anchoring by real structure, not lazy regex spans, to
    reject a transformation chained onto either the assignment
    (`.into_iter().rev().collect()`) or the loop's iterable. Rejects any
    `for _ in var {` loop found BEFORE the assignment too, so a
    special-case branch cannot fire the raw, unordered batch and return
    before the ordering step is ever reached.
    """
    body = extract_function(text, enclosing_fn)
    if body is None:
        return f"{file_label}: enclosing function `{enclosing_fn}` not found"

    masked = mask_comments_and_strings(body)

    call_open_re = re.compile(r"let\s+(?:mut\s+)?(\w+)\s*=\s*" + re.escape(wrapper_call) + r"\s*\(")
    call_open_match = call_open_re.search(masked)
    if call_open_match is None:
        return (
            f"{file_label}::{enclosing_fn}: no `let <var> = {wrapper_call}(...);`-shaped "
            "assignment found — a call whose result isn't bound to a variable doesn't "
            "prove the reordered batch is what fires"
        )

    var = call_open_match.group(1)

    before_assignment = masked[: call_open_match.start()]
    watched_names = {var}
    alias_re = re.compile(r"let\s+(?:mut\s+)?(\w+)\s*=\s*" + re.escape(var) + r"\s*;")
    watched_names.update(m.group(1) for m in alias_re.finditer(before_assignment))
    early_loop_re = re.compile(r"for\s+\w+\s+in\s+(?:" + "|".join(re.escape(n) for n in watched_names) + r")\s*\{")
    early_loop_match = early_loop_re.search(before_assignment)
    if early_loop_match is not None:
        return (
            f"{file_label}::{enclosing_fn}: a `for _ in ... {{` loop fires rows, over "
            f"`{var}` or a direct alias of it ({sorted(watched_names)!r}), before the "
            f"`{wrapper_call}` assignment is even reached — an early branch (e.g. a "
            "special case that fires and returns) can claim-order fire without ever "
            "going through the ordering wrapper"
        )

    call_close = find_matching_paren(masked, call_open_match.end() - 1)
    tail_match = re.compile(r"\s*(?:\.await)?\s*\??\s*;").match(masked, call_close)
    if tail_match is None:
        return (
            f"{file_label}::{enclosing_fn}: `{wrapper_call}`'s call is not immediately "
            "followed by `[.await][?];` — a transformation chained onto its result "
            "(e.g. `.into_iter().rev().collect()`) would not be caught by this check "
            "if it were accepted here"
        )
    assign_end = tail_match.end()

    loop_re = re.compile(r"for\s+\w+\s+in\s+" + re.escape(var) + r"\s*\{")
    loop_match = loop_re.search(masked, assign_end)
    if loop_match is None:
        return (
            f"{file_label}::{enclosing_fn}: `{wrapper_call}`'s result is bound to `{var}`, "
            f"but no `for _ in {var} {{` loop consumes it directly afterward — a further "
            "adaptor chained onto the loop's iterable (e.g. `.into_iter().rev()`) would "
            "not be caught by this check if it were accepted here"
        )

    intervening = masked[assign_end : loop_match.start()]
    allowed_read_re = re.compile(
        r"\b" + re.escape(var) + r"\b\s*\.\s*(?:" + "|".join(_ALLOWED_READ_ONLY_METHODS) + r")\s*\(\s*\)"
    )
    remaining = allowed_read_re.sub("", intervening)
    stray_match = re.search(r"\b" + re.escape(var) + r"\b", remaining)
    if stray_match is not None:
        window = remaining[max(0, stray_match.start() - 20) : stray_match.start() + 20].strip()
        return (
            f"{file_label}::{enclosing_fn}: `{var}` is referenced again between the "
            f"ordering assignment and the loop (near {window!r}) in a way this check "
            f"does not recognize as read-only ({', '.join(_ALLOWED_READ_ONLY_METHODS)} "
            "only), so the loop is not guaranteed to consume the reordered batch"
        )
    return None


def main() -> int:
    debounce_text = DEBOUNCE.read_text(encoding="utf-8")
    throttle_text = THROTTLE.read_text(encoding="utf-8")

    print(
        "Quota-lock-ordering clone-class sync check — "
        f"{len(TRACKED_FUNCTIONS)} tracked functions, "
        f"{DEBOUNCE.relative_to(REPO_ROOT)} vs {THROTTLE.relative_to(REPO_ROOT)}\n"
    )

    failures = 0
    for name in TRACKED_FUNCTIONS:
        a = extract_function(debounce_text, name)
        b = extract_function(throttle_text, name)

        if a is None or b is None:
            failures += 1
            missing_from = []
            if a is None:
                missing_from.append(str(DEBOUNCE.relative_to(REPO_ROOT)))
            if b is None:
                missing_from.append(str(THROTTLE.relative_to(REPO_ROOT)))
            print(f"FAIL {name}: not found in {', '.join(missing_from)}")
            continue

        a_compared, b_compared = a, b
        normalized_note = ""
        if name in FIELD_NORMALIZATIONS:
            debounce_pattern, throttle_pattern = FIELD_NORMALIZATIONS[name]
            a_compared = a.replace(debounce_pattern, NORMALIZED_PLACEHOLDER)
            b_compared = b.replace(throttle_pattern, NORMALIZED_PLACEHOLDER)
            normalized_note = (
                f" (after normalizing `{debounce_pattern}` / `{throttle_pattern}`)"
            )

        if a_compared == b_compared:
            print(f"OK   {name}: byte-identical{normalized_note} ({len(a)} chars)")
            continue

        failures += 1
        print(f"FAIL {name}: copies have diverged{normalized_note}")
        diff = difflib.unified_diff(
            a_compared.splitlines(),
            b_compared.splitlines(),
            fromfile=f"debounce.rs::{name}",
            tofile=f"throttle.rs::{name}",
            lineterm="",
        )
        for line in diff:
            print(f"  {line}")

    print()
    if failures:
        print(
            f"{failures} of {len(TRACKED_FUNCTIONS)} tracked functions failed "
            "(fails CI). A fix to the quota-lock-ordering algorithm (issue "
            "#1230 Finding 2) must be applied identically to both "
            "debounce.rs and throttle.rs, or a tracked function was "
            "renamed/removed — update TRACKED_FUNCTIONS in this script to "
            "match."
        )
    else:
        print("All tracked functions are byte-identical between the two copies.")

    print()
    guard_failures = 0
    for guard in CALL_SITE_GUARDS:
        for label, text in ((str(DEBOUNCE.relative_to(REPO_ROOT)), debounce_text),
                            (str(THROTTLE.relative_to(REPO_ROOT)), throttle_text)):
            error = check_call_site_guard(text, label, guard["enclosing_fn"], guard["wrapper_call"])
            if error is None:
                print(
                    f"OK   {label}::{guard['enclosing_fn']} binds `{guard['wrapper_call']}`'s "
                    "result and loops over it"
                )
            else:
                guard_failures += 1
                print(f"FAIL {error}")

    print()
    if guard_failures:
        print(
            f"{guard_failures} call-site guard(s) failed (fails CI). A fire path "
            "must call its ordering wrapper before iterating the claimed batch — "
            "see CALL_SITE_GUARDS in this script."
        )
    else:
        print("All guarded call sites invoke their ordering wrapper before firing.")

    return 1 if (failures or guard_failures) else 0


if __name__ == "__main__":
    sys.exit(main())
