#!/usr/bin/env python3
"""Form-label association audit for the Vantage dashboard
(`autumn-harvest-plugin/src/ui.rs`).

Deterministic, reproducible on any checkout — no server, browser, or
network access required.

Vantage has no client-side JavaScript for this: an `<input>`/`<select>`/
`<textarea>` gets an accessible name only through a real HTML mechanism,
either a `for="…"` attribute pointing at the control's `id`, or a
`<label>` that wraps the control as a descendant (the implicit-association
form, valid per the HTML spec without any `for`/`id` pair at all). Every
`label { … }` block in this file uses the wrapping form today — `for="`
does not appear anywhere in `ui.rs` — but the audit checks both paths, and
checks the `for` path for real: a `for="typo"` next to an `id="actual"`
resolves to nothing in a browser, so a bare "the attribute is present"
check would pass a label that is still broken. The audit collects every
literal `id="…"` on a labelable control in the file and only counts a
`for` as valid when its value is one of them.

A `label` that only sits *next to* its control (siblings under the same
`form`, not parent/child) associates with nothing. A screen reader then
announces the control with no name — "edit text, blank" instead of "Jump
to event, edit text, blank" — while every sibling field on the same page
announces correctly. That gap is silent: the field still submits, the
sighted operator still sees the adjacent text, and no error, contrast, or
flash-message audit here would ever catch it.

Recognizing a "real control" is not just a word search: `strip_comments`
removes `//`/`/* */` comments first, and `mask_text_strings` blanks every
rendered-text string second, so neither a comment nor a label's own text
node can supply the word or the terminator `CONTROL_OPEN` looks for. Only
an attribute value's own string (following `=`) survives masking, which
is what lets `for`/`id` resolution keep working.

Usage:
    python3 docs/audits/vantage-label-association.py

Run from the repo root. Exits 1 if any `label` block has no descendant
control and no `for="…"` attribute, 0 otherwise — safe to wire into CI as
a gate, or run standalone as a report.
"""
import re
import sys
from pathlib import Path

UI_RS = Path(__file__).resolve().parents[2] / "autumn-harvest-plugin" / "src" / "ui.rs"

# A `label` element's opening tag, from the `label` keyword to the `{` that
# starts its maud block. `(?<![\w-])` keeps this from matching inside some
# other identifier ending in "label" (e.g. `event_label`,
# `dead_letter_bulk_action_label`). The attrs segment is restricted to real
# maud attribute syntax (`ident="literal"` or `ident=(expr)`, repeated) so a
# plain Rust binding like `let label = String::from("Paused");` can never
# satisfy it: there, "label" is followed immediately by a bare `=`, which
# matches neither alternative, so the attrs group matches zero times and the
# mandatory `{` right after is never found in the source. Without this
# restriction, `[^{]*` would happily skip past unrelated `;`, `=`, and `(`
# characters to the next `{` anywhere later in the function, misreading an
# ordinary variable as an HTML element.
ATTR = r'\s+[\w-]+\s*=\s*(?:"[^"]*"|\([^()]*\))'
LABEL_OPEN = re.compile(rf"(?<![\w-])label\b((?:{ATTR})*)\s*\{{")

# An explicit association: `for="some-id"` on the label's own opening tag.
# Captures the target id so it can be checked against a real control below --
# a `for` naming no control on the page (a typo, a stale rename) associates
# with nothing, exactly like having no `for` at all.
FOR_ATTR = re.compile(r'(?<![\w-])for\s*=\s*"([^"]+)"')

# A labelable control's own opening tag, from the tag name to the `;` that
# self-closes it (`input`) or the `{` that starts its block (`select`,
# `textarea`) -- mirrors LABEL_OPEN's brace-based scan, but a control can
# also self-close, so both terminators are accepted.
CONTROL_OPEN = re.compile(r"(?<![\w.-])(?:input|select|textarea)\b[^;{]*?[;{]")

# A literal `id="…"` on a control's own opening tag. Only a literal is
# resolvable statically; a computed `id=(expr)` cannot be, so a `for`
# pointing at one is never counted as resolved -- unverifiable is not the
# same as verified.
ID_ATTR = re.compile(r'(?<![\w-])id\s*=\s*"([^"]+)"')


def strip_comments(src: str) -> str:
    """Blank out `//` line comments (covers `///` and `//!` too, since both
    start with `//`) and `/* … */` block comments, tracking string literals
    so a comment marker inside a quoted value is left alone. Newlines are
    preserved so every other regex's line numbers stay correct.

    Without this, `label`/`input`/`select`/`textarea` appearing as *words*
    in prose -- a code comment explaining a `<select>` two lines below it,
    e.g. `ui.rs:3669`'s "This makes the select echo it back..." -- would
    satisfy a bare word search exactly as well as the real element does
    (Codex review, PR #1485). Stripping comments first, then requiring a
    real element-opening shape (`control_ids`'s `CONTROL_OPEN`) rather than
    a word search, closes that gap: prose can no longer stand in for
    markup."""
    out = []
    i, n = 0, len(src)
    in_string = False
    while i < n:
        ch = src[i]
        if in_string:
            out.append(ch)
            if ch == "\\" and i + 1 < n:
                out.append(src[i + 1])
                i += 2
                continue
            if ch == '"':
                in_string = False
            i += 1
            continue
        if ch == '"':
            in_string = True
            out.append(ch)
            i += 1
            continue
        if ch == "/" and src[i : i + 2] == "//":
            end = src.find("\n", i)
            end = n if end == -1 else end
            out.append(" " * (end - i))
            i = end
            continue
        if ch == "/" and src[i : i + 2] == "/*":
            close = src.find("*/", i + 2)
            end = n if close == -1 else close + 2
            out.extend("\n" if c == "\n" else " " for c in src[i:end])
            i = end
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def mask_text_strings(src: str) -> str:
    """Blank the contents of every string literal that is not an attribute
    value -- a maud rendered-text node (`"Task kind"`) or an ordinary Rust
    string argument (`String::from("Paused")`) -- while leaving an
    attribute value's own quotes and content untouched (`ID_ATTR` and
    `FOR_ATTR` both read real content out of those). A string is an
    attribute value only when the previous non-whitespace character is
    `=`; anything else is text, and gets masked.

    Without this, `CONTROL_OPEN`'s own terminator search can be satisfied
    from *inside* a rendered string. A label whose real control moved to a
    sibling, but which still renders text like `"select one;"`, supplies
    the trailing `;` `CONTROL_OPEN` is looking for from the quoted text
    itself, not from a real element (Codex review, PR #1485, following up
    on the comment-only version of the same gap already closed by
    `strip_comments`). Call this after `strip_comments` so a quote that
    was only ever inside a comment has already been removed and cannot be
    mistaken for the start of a string here."""
    out = list(src)
    i, n = 0, len(src)
    while i < n:
        if src[i] != '"':
            i += 1
            continue
        j = i - 1
        while j >= 0 and src[j] in " \t\n\r":
            j -= 1
        is_attr_value = j >= 0 and src[j] == "="
        k = i + 1
        while k < n and src[k] != '"':
            k += 2 if src[k] == "\\" else 1
        end = min(k, n)
        if not is_attr_value:
            for p in range(i + 1, end):
                if out[p] != "\n":
                    out[p] = " "
        i = end + 1
    return "".join(out)


def control_ids(src: str) -> set[str]:
    ids = set()
    for m in CONTROL_OPEN.finditer(src):
        id_match = ID_ATTR.search(m.group(0))
        if id_match:
            ids.add(id_match.group(1))
    return ids


def line_of(src: str, index: int) -> int:
    return src.count("\n", 0, index) + 1


def find_block_end(src: str, open_brace_index: int) -> int:
    """Return the index just past the `}` matching the `{` at `open_brace_index`,
    skipping brace characters that appear inside `"..."` string literals (maud
    attribute values and rendered text can themselves contain `{`/`}` only
    inside quotes, since raw `{`/`}` outside quotes are always maud syntax)."""
    depth = 0
    in_string = False
    i = open_brace_index
    while i < len(src):
        ch = src[i]
        if in_string:
            if ch == "\\":
                i += 1  # skip the escaped character too
            elif ch == '"':
                in_string = False
        elif ch == '"':
            in_string = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return i + 1
        i += 1
    raise ValueError(f"unbalanced braces starting at index {open_brace_index}")


# The dashboard's inline CSS lives in a Rust raw string and can itself
# contain a `label` *selector* (`.filters label{...}`, `.stat .label{...}`)
# that is not markup at all -- syntactically indistinguishable from a real
# maud element at the regex level, since raw CSS `label{` matches "label"
# immediately followed by `{` just as cleanly as maud's own syntax does.
# Blank the block out before scanning (same extraction span
# `vantage-dashboard-contrast.py` uses) so line numbers elsewhere are
# unaffected but nothing inside it can match.
STYLE_BLOCK = re.compile(r'const STYLE: &str = r#"\n(.*?)\n"#;', re.S)


def blank_style_block(src: str) -> str:
    m = STYLE_BLOCK.search(src)
    if not m:
        raise SystemExit("could not find `const STYLE` block in ui.rs")
    start, end = m.span(1)
    return src[:start] + re.sub(r"[^\n]", " ", src[start:end]) + src[end:]


def main():
    src = mask_text_strings(strip_comments(blank_style_block(UI_RS.read_text())))
    ids = control_ids(src)

    sites = []
    for m in LABEL_OPEN.finditer(src):
        open_tag_attrs = m.group(1)
        open_brace = m.end() - 1
        block_end = find_block_end(src, open_brace)
        block_text = src[open_brace:block_end]

        for_match = FOR_ATTR.search(open_tag_attrs)
        has_for = bool(for_match) and for_match.group(1) in ids
        has_control = bool(CONTROL_OPEN.search(block_text))
        sites.append((line_of(src, m.start()), has_for, has_control))

    sites.sort()

    print(f"Form-label association audit — {UI_RS}")
    print(f"{len(sites)} `label` render sites checked.\n")

    if not sites:
        print("ERROR: found no `label` render sites at all — either the markup")
        print("was rewritten in a spelling this audit doesn't recognize (a silent")
        print("pass would hide that), or every form lost its labels outright.")
        return 1

    missing = [line for line, has_for, has_control in sites if not (has_for or has_control)]
    ok_count = len(sites) - len(missing)

    if missing:
        print(f"BROKEN — {len(missing)}:")
        for line in missing:
            print(f"  ui.rs:{line}  label has no `for=\"…\"` and wraps no input/select/textarea")
    else:
        print("Every `label` render site wraps a control or names one via `for=\"…\"`.")

    print(f"\nCorrect — {ok_count} of {len(sites)} render sites carry a real association.")
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())
