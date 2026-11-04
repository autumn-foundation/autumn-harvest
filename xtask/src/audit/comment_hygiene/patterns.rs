//! Every regular expression the rules use, in one place.
//!
//! The patterns keep the exact text of the Python original. Fingerprints
//! and the Tier B ratchet depend on findings that do not move, so a
//! pattern here changes only on purpose.

use std::fmt::Write as _;
use std::sync::LazyLock;

use super::pyre::{Re, strip_verbose};

/// An apostrophe, ASCII or typographic.
///
/// An editor can replace `'` with U+2019. That must not turn a gated
/// construct into an invisible pass. CH006 and CH003 both had this defect
/// on their own, so the class lives here and not beside either rule.
const APOS: &str = r"[\u2019']";

/// What can follow a literal inside an attribute value.
///
/// This set is narrower than the set after a path. rustc 1.94.1 gives it
/// as "expected one of `.`, `?`, `]`, or an operator". So it has no `!`,
/// no `::` and no `{`. A number is not a macro name, a path segment or a
/// struct literal.
const LITF: &str = r"(?=[.?+*/%&|^<>=,-]|\])";

/// The two-atom prose test of `PROSE`, for a value that ends at a `]`.
///
/// An attribute value ends at a `]`, not at a `;`. An earlier version gave
/// only the first atom of the value a follow set. Then `.*` took the rest,
/// so `\#[Retry = 3 + attempts remaining]` passed. The `+` satisfied the
/// follow set, and the prose came in after it.
///
/// This guard covers the whole alternative. So the delimiter branch is
/// also covered, not only the `=` branch.
const PROSEB: &str = r"(?!.*[^\s\-+*/%&|^<>=!,(\[{:.?](?<!\bas)\s+\w+\s*\]\s*$)";

/// The binding mode in front of an identifier.
///
/// rustc 1.94.1 accepts `let ref stale = ...;` and `let ref mut stale =
/// ...;`. It also accepts `ref` on a binding with no initializer, as in
/// `let ref x: String;`.
///
/// The order is fixed: `ref mut`, never `mut ref`. rustc answers E0658,
/// "mutable by-reference binding", for the second form. To admit it would
/// accept what the language refuses.
///
/// The destructuring rules need nothing. `ref` binds an identifier, so it
/// sits inside the pattern, as in `let (ref a, ref b) = t;`. The pattern
/// class already reads it there, and `let ref (a, b) = t;` is not Rust.
const BIND: &str = r"(?:ref\s+)?(?:mut\s+)?";

/// The keywords that the prose test skips.
///
/// The test asks whether the tail of a declaration is an expression or
/// English. The tail is prose only when both questions below answer yes.
///
/// 1. Does the tail have no code signal? A code signal is a quote, a `!`,
///    a `(`, a `[` or a `::`. Prose in this tree has none. An expression
///    with one is an expression, however its words fall. That keeps
///    `msg = format!("the queue is paused");` reported.
///
/// 2. Are two word atoms adjacent, with only space between them? An
///    expression never has that. `a - b` has an operator between the
///    atoms, and `legacy default` has nothing. Keywords are the exception
///    on either side, as in `count as`, `as usize`, `mut Worker` and
///    `} else`. So the test skips a pair when either half is a keyword.
///
/// Three earlier forms of this guard each read too little. A three-word
/// threshold missed `mode = legacy default;`. A run of bare words missed
/// the hyphen in `30-second default;`. A test of the last atom only missed
/// `legacy default + jitter;`, where the tail ends well and fails in the
/// middle. To read the whole tail fixes all three.
///
/// There is one definition, which the six rules that need it share.
const PROSE_KEYWORD: &str = concat!(
    r"(?:as|mut|const|dyn|impl|move|if|else|match|in|ref|return|break",
    r"|continue|unsafe|async|await|where|for|while|loop|let|true|false",
    r"|crate|self|Self|super)"
);

/// The prose test of a declaration tail. See `PROSE_KEYWORD`.
fn prose() -> String {
    format!(
        r#"(?!(?![^;]*[\"!(\[]|[^;]*::)[^;]*\b(?!{PROSE_KEYWORD}\b)\w+\s+(?!{PROSE_KEYWORD}\b)\w+)"#
    )
}

/// A run that does not stop at the `;` of an array type.
///
/// The `;` that ends a declaration and the `;` inside an array are the same
/// character to a regex. To a reader they are different things. `[u8; 32]`
/// is one type, so a run that reads it must not stop in the middle.
///
/// Three reviews each found this guard missing from a different rule. The
/// generic list, the where clause, the impl header, the return type and the
/// const all lacked it at some time. The guard was written inline, and that
/// let the copies drift. So it is written once here, and every place that
/// can hold a type or an expression uses it. The variants differ only in
/// what else they refuse.
///
/// The prose look-aheads keep their bare `[^;{]*`. A wider run there lets
/// a look-ahead see more text and so refuse more often. That direction
/// under-reports, and prose with an array type in it does not occur.
const NO_SEMI: &str = r"(?:[^;]|;(?=[^;]*\]))";
const NO_SEMI_BRACE: &str = r"(?:[^;{]|;(?=[^;]*\]))";
const NO_SEMI_EQ: &str = r"(?:[^;=]|;(?=[^;]*\]))";
const NO_SEMI_PAREN: &str = r"(?:[^;()]|;(?=[^;]*\]))";

/// A restricted visibility, which seven item alternatives use as a prefix.
///
/// An identifier may be raw. `r#match` is a name, and `\w+` stops at the
/// `#`. So a commented-out `fn r#match() {` was outside an absolute gate.
/// This tree discusses `r#gen` in `det_check.rs`, so the form is real.
///
/// The seven alternatives share one definition. It is one question asked
/// seven times, and all seven once lacked a raw path segment. `r#type` is
/// a legal module name, so each segment of `pub(in crate::r#type)` may be
/// raw, as the name of an item may be.
const VISIBILITY: &str = r"(?:pub(?:\((?:in\s+)?(?:r\#)?\w+(?:::(?:r\#)?\w+)*\))?\s+)?";

/// A `where` clause.
///
/// It can sit on the signature line of anything generic: a function, a
/// struct, an enum, a trait, a union or an impl. A type alias has it before
/// its `=`. The `{` or `;` that ends the declaration bounds it, so it never
/// runs past its own item.
///
/// It has the same array-type guard as `GENERICS`, because a bound can
/// hold an array type. `where T: Into<[u8; 32]>` is one clause.
fn where_clause() -> String {
    format!(r"(?:where\s+{NO_SEMI_BRACE}+?\s*)?")
}

/// What follows the signature of an item.
///
/// That is the `{` that opens its block, or a complete one-line block.
/// `fn stale() {}` is commented-out code as much as `fn stale() {` is. An
/// anchor at the brace alone let each one-line body through an absolute
/// gate. The pattern is anchored at the end of the line, so the closing
/// brace must be the last thing there.
///
/// Only the strongly anchored item forms use it. Control flow does not. Its
/// guard is a look-ahead over the words before the brace, and "match the
/// shard {0}" is prose that ends in a closing brace.
const BODY: &str = r"(?:\{(?:.*\})?)";

/// A generic parameter list, which can nest to any depth.
///
/// A class of "anything but angle brackets" reads `<T>` and refuses
/// `<T: Into<Vec<u8>>>`. That left a whole shape of signature outside an
/// absolute gate. A count of levels only moves the gap one level out.
///
/// So the list is bounded by what cannot appear in it. That is a `;`,
/// unless it is the `;` of an array type. `<T: Into<[u8; 32]>>` is one
/// list, not a declaration that ends in the middle.
///
/// A `{` is not such a character. `<const N: usize = { 1 + 2 }>` is a
/// const-block default, and rustc 1.94.1 accepts it. The greedy run needs
/// no help. It backtracks to the `>` that the surrounding anchor asks for.
/// That anchor is the `(` of a signature, the `=` of an alias or the `{` of
/// a body. So `Fn(u8) -> u8` is admitted, and the form stays anchored on a
/// keyword and a name.
fn generics() -> String {
    format!("<{NO_SEMI}*>")
}

/// A path can start at the crate root.
///
/// rustc 1.94.1 accepts `use ::std::fmt;`, `::std::println!("x")`, `let
/// ::std::option::Option::Some(x) = ...` and
/// `\#[::core::prelude::v1::derive(Debug)]`. Each rule here that reads a
/// path once required an identifier before the first `::`.
const ROOT: &str = r"(?:::\s*)?";

/// The type of a declaration with no initializer, and its terminator.
///
/// Two rules read one: a `let` with no initializer, and a `const` or a
/// `static` with no body.
///
/// With no `=` to anchor on, a type annotation is all that separates "let
/// mut retries: usize;" from "let the reader decide;". So the look-ahead
/// refuses bare words before the `;`. A Rust type needs `;` for an array
/// length, `+` for a trait object, `->` for a function pointer and `*` for
/// a raw pointer. The inner `;` is allowed only where a `]` closes before
/// the next one, so the terminator stays the end of the statement. A
/// hyphen is admitted only as `->`. A bare hyphen let "let a::b is
/// re-exported for callers;" through.
fn decl_type() -> String {
    format!(
        r"{}\s*(?:[\w:<>&'\[\]\s,()+*!?\#]|->|;(?=[^;]*\]))+;\s*$",
        prose()
    )
}

/// The ABI of an `extern` item, for the two rules that read one.
///
/// A plain string literal is not the only form. rustc 1.94.1 accepts
/// `extern r"C" {}` and `extern r##"C"## {}`, and the same raw forms on a
/// function. Only ordinary quotes reached the gate before. So an FFI item
/// with a raw literal got past it.
///
/// The raw delimiter takes any number of hashes, and the closing run must
/// match the opening one. A back-reference says that exactly. Each call
/// site needs its own group name, because one pattern holds two of these,
/// and a repeated name is an error.
///
/// rustc gives the set. For `b"C"`, `c"C"` or `br"C"` it answers
/// "non-string ABI literal". So a byte string, a C string and a raw byte
/// string are all out.
///
/// The ABI name stays `[\w-]+`, which is wider than the set rustc knows.
/// `extern "Q" {}` is E0703, not valid Rust, and this pattern reports it.
/// That over-report cannot occur in practice. `extern` is not an English
/// word, and no prose puts a quoted word between it and a brace.
// The braces hold a placeholder for `str::replace`, as in the Python
// original. They are not format arguments.
#[allow(clippy::literal_string_with_formatting_args)]
fn abi(tag: &str) -> String {
    r#"(?:"[\w-]+"|r(?P<{tag}>\#*)"[\w-]+"(?P={tag}))"#.replace("{tag}", tag)
}

/// The alternatives of CH001, in Python verbose syntax.
///
/// CH001 recognizes commented-out Rust by the shape of the line. Each
/// alternative is anchored and ends at a terminator. So ordinary prose,
/// such as "let the caller decide" or "use the LATER definition", cannot
/// match. Prose does not end in `;` or `{`.
///
/// Each alternative binds its delimiter tight against the identifier. So a
/// prose line that ends in `;` by chance cannot match. `fn item and is
/// therefore typo-proof ...;` is prose. After `fn` comes a name and then a
/// space, never the `(` that a real signature needs.
const CH001_ALTERNATIVES: &[&str] = &[
    // A function. The ABI name can carry a hyphen, because "C-unwind" and
    // its siblings are stable and rustc 1.94.1 compiles them. The ABI can
    // also be absent, because a bare `extern fn` means `extern "C" fn`.
    // Both are ordinary FFI, and `\w+` alone left both outside an absolute
    // gate.
    //
    // The tail has three forms. A complete signature ends at `;` or at a
    // body. A wrapped signature ends with `fn foo(` at the end of the line.
    // A wrapped signature with real parameters ends with a trailing comma.
    r"{VIS}
        (?:async\s+|unsafe\s+|const\s+|extern\s+(?:{ABI_FN}\s+)?)*
            fn\s+(?:r\#)?\w+\s*(?:{GEN})?\s*\(
            (?:
                 .*\)\s*(?:->\s*{NOSB}+?)?\s*{WHERE}(?:;|{BODY})
               | \s*$
               | (?=[^)]*(?::|\bself\b))
                 [\w\s:&'<>\[\](),.+;=*#-]*,\s*$
            )\s*$",
    // A trait. `unsafe` qualifies a trait, and `pub unsafe trait` puts it
    // between the visibility and the keyword. It qualifies an impl too,
    // below. rustc 1.94.1 accepts it on those two and on `fn`, which the
    // function alternative already has.
    //
    // Each kind takes its own terminators, as rustc 1.94.1 gives them. Only
    // a struct can end at `;` or `(`:
    //
    //   struct Foo;    OK          enum E;   expected `{}`, found `;`
    //   struct Foo();  OK          trait T;  expected `{}`, found `;`
    //                              union U;  expected `where` or `{`
    //
    // One shared set let `// trait object;` and `// enum value;` fail an
    // absolute gate on two ordinary English words.
    r"{VIS}(?:unsafe\s+)?trait\s+(?:r\#)?\w+\s*(?:{GEN})?\s*{WHERE}{BODY}\s*$",
    r"{VIS}struct\s+(?:r\#)?\w+\s*(?:{GEN})?\s*{WHERE}(?:[;(]|{BODY})\s*$",
    r"{VIS}(?:enum|union)\s+(?:r\#)?\w+\s*(?:{GEN})?\s*{WHERE}{BODY}\s*$",
    // A tuple struct, with its field list on the line and terminated. It
    // has its own alternative, and it does not relax the one above. That is
    // what the guidance of this module asks for. The name is still anchored
    // hard against `struct`, and the line must end at the `;`.
    //
    // The `;` of "[u8; 32]" is part of an array type, not the end of the
    // statement. This is the same guard that the binding rules use.
    r"{VIS}struct\s+(?:r\#)?\w+\s*(?:{GEN})?
            \s*\({NOSB}*\)\s*{WHERE};\s*$",
    r"{VIS}mod\s+(?:r\#)?\w+\s*(?:;|{BODY})\s*$",
    // An extern block. `unsafe` is optional before edition 2024 and
    // required from it. This tree is edition 2024, so a bare `extern {` is
    // the older form of the same construct. A comment has no edition, so
    // both forms are commented-out FFI, and both belong in an absolute
    // gate.
    //
    // The ABI string needs white space in front of it. `extern"C"` is a
    // reserved prefix, and rustc 1.94.1 answers "prefix `extern` is
    // unknown". A bare `extern {` only warns, and a warning means that the
    // line parses.
    //
    // There is no visibility here. `pub extern "C" {}` is not Rust in any
    // edition.
    //
    // The opener `unsafe extern "C" {` matched before this alternative
    // existed, through the control-flow form, which takes `unsafe` for its
    // own reasons. The complete `unsafe extern "C" {}` did not match,
    // because control flow ends its line at the brace.
    r"(?:unsafe\s+)?extern(?:\s+{ABI_BLOCK})?\s*{BODY}\s*$",
    // A `const` or a `static` with an initializer. The initializer takes
    // the same prose guard as the assignment and the bindings. Without it,
    // `const timeout: duration = legacy default;` was inside an absolute
    // gate.
    r"{VIS}(?:const|static)\s+(?:mut\s+)?(?:r\#)?\w+\s*:{NOSE}+={PROSE}.*[;{]\s*$",
    // An associated type that is declared, not defined. rustc 1.94.1
    // accepts `type Item;`, `type Item<X>;`, `type Item where Self: Sized;`
    // and `type Iter: Iterator<Item = Self::Item>;` in a trait. The alias
    // rule below needs an `=`, so it reached none of them.
    //
    // There is no visibility, and that is the answer of the compiler, not a
    // guess. `pub type Item;` is E0449, "visibility qualifiers are not
    // permitted here". To admit one would accept what Rust refuses.
    //
    // A bound is a trait, so `type Item: [u8; 32];` is "expected a trait,
    // found type". But a bound can hold an array, as `Into<[u8; 32]>` does.
    // So the run has the same array guard as the generic list and the where
    // clause.
    //
    // The bound also takes the prose look-ahead. With no `=` on the line, a
    // colon is all that separates `type Item: Clone;` from "type Item: the
    // reader may skip this;".
    //
    // The bound has one colon. The adversarial prose sweep found the
    // second. "type a::b is re-exported for callers;" put a `:` where the
    // colon of a bound goes. The word look-ahead could not see past it.
    // A single colon starts a bound, and a path separator is two.
    // `type I: ::std::fmt::Debug;` still reports, because a space follows
    // its bound colon.
    r"type\s+(?:r\#)?\w+\s*(?:{GEN})?\s*
            (?::(?!:){PROSE}
                {NOSB}+)?
            \s*{WHERE};\s*$",
    // A type alias. It takes a `where` clause before its `=`. rustc 1.94.1
    // accepts that and warns that the clause is not enforced, which is a
    // lint about meaning and not a syntax error.
    r"{VIS}type\s+(?:r\#)?\w+\s*(?:{GEN})?\s*{WHERE}=
            {PROSE}.*;\s*$",
    // The header of an impl: a trait reference, a type and a `where`
    // clause. A class of what can appear in a header could not hold the
    // `[u8; 32]` of an array bound. So the header is bounded by what cannot
    // appear: the `{` that opens the body, and a `;` outside an array type.
    // The look-ahead is the prose guard, as before.
    r"(?:unsafe\s+)?impl(?:\s*{GEN})?\s+
            (?![^;{]*\b[a-z]+\s+[a-z]+\s+[a-z]+\s+[a-z]+\b)
            {NOSB}+{BODY}\s*$",
    r"let\s+{BIND}(?:r\#)?\w+\s*(?::{NOSE}+)?=
            {PROSE}[^=].*;\s*$",
    // Destructuring bindings: a tuple or slice pattern, or a struct or enum
    // pattern behind a capitalized path. Each is terminated, and English
    // makes none of these shapes. The plain `\w+` form above misses each.
    r"let\s+(?:mut\s+)?[(\[][\w\s,.:&*'()\[\]{}\#]*[)\]]\s*(?::{NOSE}+)?
            =\s*(?:[^=;]|;(?=[^;]*\]))+(?:;|\s+else\s*\{)\s*$",
    r"let\s+{ROOT}(?:(?:r\#)?[\w]+::)*(?:r\#)?[A-Z]\w*\s*(?:\({NOS}*\)|\{{NOS}*\})\s*=\s*(?:[^=;]|;(?=[^;]*\]))+
            (?:;|\s+else\s*\{)\s*$",
    // A binding with no initializer. With no `=` to anchor on, it needs a
    // type annotation. That separates "let mut retries: usize;" from "let
    // the reader decide;". English does not put a colon between two bare
    // words.
    r"let\s+(?:mut\s+)?(?:r\#)?\w+\s*:{DECL_TYPE}",
    // A `const` or a `static` with no initializer. rustc 1.94.1 accepts
    // `const LIMIT: usize;` in a trait and `static FOREIGN: u8;` in an
    // extern block. The rule above needs an `=`, so neither reached the
    // gate. It reads the same type as the binding above, from the same
    // fragment. Two copies of one construct are how two rules came to
    // disagree before.
    r"{VIS}(?:const|static)\s+(?:mut\s+)?(?:r\#)?\w+\s*:{DECL_TYPE}",
    // A typed binding. The type is a Rust type, not a scalar name. An array
    // length needs `;`, a trait object needs `+`, a function pointer needs
    // `->`, and a raw pointer needs `*`. Without them "let bytes: [u8;
    // 32];" is commented-out code that the absolute gate lets through. The
    // inner `;` is allowed only where a `]` closes before the next one, so
    // the terminator stays the end of the statement. A hyphen is admitted
    // only as `->`. A bare one let the prose sweep through "let a::b is
    // re-exported for callers;", and a type has no other use for it.
    r"let\s+{BIND}(?:r\#)?\w+\s*:
            {PROSE}\s*
            (?:[\w:<>&'\[\]\s,()+*!?\#]|->|;(?=[^;]*\]))+;\s*$",
    // A re-export has a visibility like any other item. This alternative
    // once lacked it, and `pub use crate::x;` and `pub(crate) use
    // crate::x;` are the forms this tree writes.
    //
    // A grouped use tree nests, as in `use foo::{bar::{Baz}, Qux};`. So the
    // group is bounded by the `;` that ends the declaration, not by a class
    // of what can sit in it. The reason is the same as for `GENERICS`.
    r"{VIS}
            use\s+{ROOT}(?:(?:r\#)?\w+::)*(?:(?:r\#)?\w+|\*|\{[^;]*\})
            (?:\s+as\s+(?:r\#)?\w+)?;\s*$",
    // `extern crate` is an import item too, and this rule once read only
    // `use`. It takes a visibility, and rustc 1.94.1 accepts `pub extern
    // crate alloc;` and `pub(crate) extern crate core;`. It takes an `as`
    // rename too, `as _` among them, which `\w+` already covers.
    r"{VIS}extern\s+crate\s+(?:r\#)?\w+(?:\s+as\s+(?:r\#)?\w+)?;\s*$",
    // An attribute. The `]` that closes it at the end of the line bounds
    // it, not a class of what can sit inside. A class refused
    // `#[doc = include_str!("../README.md")]` because of the `!`, and the
    // next nested form would need something else.
    //
    // A bound at the end is not enough alone. `#[This section is
    // intentionally blank]` is a sentence in brackets, and `#[]` is nothing
    // at all. An attribute opens with a path, and the grammar says what can
    // follow one.
    //
    // The set comes from rustc 1.94.1, not from memory. An earlier version
    // admitted a comma that no attribute can hold, and refused two
    // delimiters that each attribute can hold. For `#[Note, this section
    // is intentionally blank]` rustc prints the set: "expected one of `(`,
    // `::`, `=`, `[`, `]`, or `{`". The `::` belongs to the path, and the
    // rest is the input.
    //
    // The `=` form takes an expression, and prose is not one. rustc names
    // what can follow the first atom of the value. For `#[Note = this
    // section is intentionally blank]` it prints "expected one of `!`,
    // `.`, `::`, `?`, `]`, `{`, or an operator, found `section`". So a
    // literal opens a value outright. A path opens one only when a member
    // of that set follows. Two bare words in a row are prose in brackets.
    //
    // A literal value has its own follow set, and it is narrower than the
    // set after a path. For `\#[Retry = 3 attempts remaining]` or
    // `\#[deprecated = "use foo" instead]` rustc 1.94.1 answers "expected
    // one of `.`, `?`, `]`, or an operator". A `.*` tail admitted the prose
    // in both.
    //
    // So the literal is matched, not only opened. It is a non-raw string
    // with its escapes, a raw string with balanced hashes, or a number. To
    // match it makes a follow set possible at all. It also fixes an
    // under-report: `\#[doc = r\#"x"\#]` is valid, and the opener could not
    // read `r\#`.
    r#"\#!?\[{PROSEB}\s*{ROOT}(?:r\#)?\w+(?:\s*::\s*(?:r\#)?\w+)*\s*
            (?:
                [(\[{].*
              | =\s*(?:
                      (?:
                          (?:b|br|c)?"(?:[^"\\]|\\.)*"
                        | (?:b|c)?r(?P<attrh>\#*)"(?:(?!"(?P=attrh)).)*"(?P=attrh)
                        | [0-9][0-9_]*(?:\.[0-9][0-9_]*)?
                        | '(?:[^'\\]|\\.)'
                      )\s*{LITF}.*
                    | (?:r\#)?\w+(?:::(?:r\#)?\w+)*\s*
                      (?=[!.?(\[{:+*/%&|^<>=,-]|\])
                      .*
                  )
            )?
            \]\s*$"#,
    r"\}[,;)]*\s*$",
    // A macro invocation. rustc 1.94.1 states the terminator rule itself:
    // "macros that expand to items must be delimited with braces or
    // followed by a semicolon". So a call with braces needs no `;`. To
    // require one left `// stale! {}` outside an absolute gate. The `;`
    // stays mandatory for `(` and `[`, as rustc asks. It is optional after
    // `}`, which is legal in both positions.
    r"{ROOT}(?:(?:r\#)?\w+::)*(?:r\#)?\w+!\s*(?:\(.*\)|\[.*\]|\{.*\})\s*;\s*$",
    // The form with no semicolon. It has its own alternative, because it
    // needs two guards that the terminated form does not. It is the form
    // that prose reaches. English writes an exclamation in front of a
    // brace, and the `;` kept that out.
    //
    // The first guard is the four-lowercase-word look-ahead of the
    // control-flow rule. It refuses "Important! {see the note below}".
    //
    // The second guard is about English, not Rust. rustc 1.94.1 accepts
    // `macro_rules! Stale` and `Stale! {}` with no warning. So a
    // capitalized macro name is legal. But a capitalized word before a `!`
    // is how English writes an exclamation, and each macro in this tree is
    // snake_case. So this branch alone asks for a snake_case name. That
    // refuses "Note! {a, b}" and "Stop! {}", which have too few words for
    // the first guard to see.
    //
    // An upper-case macro name loses the form with no semicolon by that
    // choice. That is an under-report, and the terminated alternative above
    // still reads `Stale!(x);`.
    r"{ROOT}(?:(?:r\#)?\w+::)*(?:r\#)?[a-z_][a-z0-9_]*!\s*
            (?!.*\b[a-z]+\s+[a-z]+\s+[a-z]+\s+[a-z]+\b)\{.*\}\s*$",
    // A macro definition, which ends at its brace and not at a `;`. It is
    // anchored on the keyword, so no prose can reach it.
    r"macro_rules!\s+(?:r\#)?\w+\s*\{.*\}\s*;?\s*$",
    // Control flow that opens a block. The look-ahead refuses a condition
    // of four or more plain words in a row. That is a sentence, not an
    // expression: "if the queue is paused, the worker parks {".
    r"(?:if|while|for|match|loop|unsafe)\b
            (?![^;{]*\b[a-z]+\s+[a-z]+\s+[a-z]+\s+[a-z]+\b){NOS}*\{\s*$",
    // A label. rustc 1.94.1 gives what can follow one: "expected `while`,
    // `for`, `loop` or `{` after a label". So a label attaches to three of
    // the six keywords above and to a bare block. It does not attach to
    // `if`, `match` or `unsafe`.
    //
    // It has its own alternative for that reason. An optional label on the
    // rule above would accept `'a: if x > 1 {`, which the compiler refuses.
    // A bound that admits what the grammar refuses does no work.
    r"'(?:r\#)?\w+\s*:\s*
            (?:(?:while|for|loop)\b
                (?![^;{]*\b[a-z]+\s+[a-z]+\s+[a-z]+\s+[a-z]+\b){NOS}*)?
            \{\s*$",
    // A block expression used as a statement. The control-flow rule above
    // ends its line at the opening brace. These end at a `;`, and nothing
    // read them.
    //
    // rustc 1.94.1 gives the set. `async {}`, `async move {}`, `const {}`
    // and `unsafe {}` all compile as statements. `move {}` is not one
    // ("expected one of `async`, `|`, or `||`"). `try` and `gen` blocks
    // are E0658, experimental. So all three stay out.
    //
    // A bare `{ let x = 1; };` is also valid and is not admitted. With no
    // keyword in front there is nothing to anchor on, and a braced aside in
    // prose would reach it. That is a stated under-report.
    //
    // The four-lowercase-word look-ahead comes too. So
    // `// const {the shard map is stale};` stays prose.
    r"(?:async(?:\s+move)?|const|unsafe)\s*
            (?![^;]*\b[a-z]+\s+[a-z]+\s+[a-z]+\s+[a-z]+\b)
            \{.*\}\s*;\s*$",
    // The other half of a conditional. `else` opens a block like the
    // keywords above. It can also follow the brace that closes the arm
    // before it, and the standalone-brace alternative stops at that brace.
    // Neither form reached `// } else {`, which is as common in
    // commented-out code as the `if` above it.
    //
    // `}` is admitted only in front of `else`. That is a choice about what
    // people write, not about the grammar. `} if x > 1 {` parses, because
    // rustc 1.94.1 reads it as two statements. But nobody closes one block
    // and opens an unrelated `if` on the same line. `else` is the only
    // keyword that continues the item above it.
    //
    // What follows `else` is the grammar, though. rustc takes a block or
    // another `if`, and it answers "expected `{`, found keyword `while`"
    // for anything else. So the tail is not a run of any characters.
    //
    // The prose look-ahead is the same one that the keywords above have.
    // So "else the worker parks until the queue drains {" is still a
    // sentence.
    r"(?:\}\s*)?else\b
            (?![^;{]*\b[a-z]+\s+[a-z]+\s+[a-z]+\s+[a-z]+\b)
            (?:\s+if\b{NOS}*)?\s*\{\s*$",
    // An assignment, plain or compound. Its right side is an expression,
    // and an expression has spaces in it. `retries = retries + 1;` is what
    // people write. A guard that refused white space refused most of the
    // language to keep prose out.
    //
    // The guard belongs on the words, where the other rules put it. Bare
    // words before the terminator are a sentence. That is the same test as
    // the binding with no initializer and the declaration type use.
    //
    // The target can be dereferenced. `*counter = counter + 1;` is an
    // ordinary mutation, and the class could not open on the star.
    //
    // The star must touch its target. A Markdown bullet is `* ` with a
    // space. `strip_containers` already takes a real bullet off this line,
    // so this is a second guard and not the only one. But it costs nothing,
    // and it keeps emphasis out.
    //
    // The target can be in parentheses. `*(counter) = 1;` and
    // `*(ptr.add(1)) = value;` both compile. An earlier look-ahead admitted
    // `(`, and the class behind it could not consume one. So the bound
    // described a form that the rule then refused, which is a bound that
    // does no work.
    //
    // The form in parentheses is allowed only behind a star. That is where
    // it was reported, and there the star already anchors the line. A bare
    // `(x) = 1;` is valid Rust too. To admit it at the start of a comment
    // would open the rule to "(see below) = ..." for no gain.
    //
    // The target in parentheses also takes the prose guard. The right side
    // has one already, but it sees only the right side, and
    // `*(the queue drains) = x;` puts the sentence on the left.
    r"(?:\*+(?=[\w(])
            (?:\((?![^)]*\b[a-z]+\s+[a-z]+\s+[a-z]+\b)[^;=]*\)|[\w.\[\]:\#]+)
          |[\w.\[\]:\#]+)
            \s*(?:[-+*/%&|^]|<<|>>)?=
            {PROSE}\s*{NOS}+;\s*$",
    // `return` and `break` take an expression, so any single token after
    // them is a value. `continue` takes only a lifetime label. rustc
    // answers "expected a label, found an identifier". The unrestricted
    // form reported prose for it: "If the row is absent," that wraps onto
    // "continue normally;" is a sentence, not a statement.
    //
    // `return` and `break` keep the single-token form. An attempt to widen
    // them, as the assignment above was widened, failed the adversarial
    // sweep. Eighteen generated sentences matched, among them "return
    // early, before the lock is taken;" and "break cleanup() first, then
    // retry;".
    //
    // The bare-word look-ahead cannot save these. It is anchored. It fires
    // only when the whole tail is bare words and a `;`. So a comma, a colon
    // or an equals sign in the sentence walks past it. The assignment
    // survives the same widening, because `NAME =` anchors it before the
    // prose starts. `return` is an English verb with no anchor at all.
    //
    // So `// return [0u8; 32];` stays missed. This is recorded because the
    // attempt teaches more than a restatement: this guard does not
    // transfer.
    r"(?:return|break)\b(?:\s+[^\s;{}]+)?\s*;\s*$",
    r"continue\b(?:\s+'\w+)?\s*;\s*$",
    // A statement that calls something. It is anchored hard. The call must
    // open at the very start, so prose that names a function ("call
    // cleanup() first") cannot reach it. The line must end at the `;`.
    // Before it was added, it was measured against all 176k corpus
    // comments. It had one hit, and that was real commented-out code.
    //
    // A turbofish can sit between segments, not only before the call.
    // `Foo::<u8>::bar()` instantiates the type and then names an associated
    // function. The old form allowed one turbofish, at the end of the path.
    //
    // The path can start at the crate root, through `{ROOT}`. A qualified
    // path is allowed too, because `<Foo as Bar>::baz()` names a trait
    // method through the type. A rebuild for turbofish segments once
    // dropped the root prefix. That survived because the form had no
    // fixture. A shape proved in prose is a shape that the next rewrite can
    // break.
    //
    // The qualified path can be generic. `<Vec<u8> as Bar>::baz()`
    // compiles, and a ban on inner angle brackets covered only the plain
    // `<Foo as Bar>`. So the `;` that ends the statement bounds it instead,
    // and the brackets can nest.
    r"(?:{ROOT}|<{NOS}*>::)
            [\w\#]+(?:::[\w\#]+|::<{NOSP}*>)*(?:\.[\w\#]+(?:::<{NOSP}*>)?)*
            \(.*\)(?:\s*\?|\s*\.await\s*\??)*\s*;\s*$",
];

/// The full CH001 pattern, with each fragment in place.
pub fn commented_code_pattern() -> String {
    let template = format!("^(?:{})", CH001_ALTERNATIVES.join("|"));
    let pattern = template
        .replace("{VIS}", VISIBILITY)
        .replace("{WHERE}", &where_clause())
        .replace("{BODY}", BODY)
        .replace("{GEN}", &generics())
        .replace("{ROOT}", ROOT)
        .replace("{DECL_TYPE}", &decl_type())
        .replace("{PROSE}", &prose())
        .replace("{PROSEB}", PROSEB)
        .replace("{BIND}", BIND)
        .replace("{LITF}", LITF)
        .replace("{NOS}", NO_SEMI)
        .replace("{NOSB}", NO_SEMI_BRACE)
        .replace("{NOSE}", NO_SEMI_EQ)
        .replace("{NOSP}", NO_SEMI_PAREN)
        .replace("{ABI_FN}", &abi("abifn"))
        .replace("{ABI_BLOCK}", &abi("abiblock"));
    strip_verbose(&pattern)
}

/// Commented-out Rust, by the shape of the line. See `CH001_ALTERNATIVES`.
///
/// Each alternative ends at one of `;`, `{`, `}`, `(`, `,`, `]` or `)`, and
/// then only white space. The prefilter skips each line that does not, which
/// is most prose.
pub static COMMENTED_CODE_RE: LazyLock<Re> =
    LazyLock::new(|| Re::with_prefilter(&commented_code_pattern(), r"[;{}(,\])]\s*$"));

/// A marker that opens a comment, or a marker with marker punctuation.
///
/// `TODO:` and `FIXME(...)` are markers. Prose that refers to a marker
/// elsewhere is not an untracked commitment itself. An example is "see the
/// marker above", with the marker word in place of "marker".
pub static TODO_RE: LazyLock<Re> =
    LazyLock::new(|| Re::new(r"(?i)^(?:TODO|FIXME|XXX|HACK)\b|\b(?:TODO|FIXME|XXX|HACK)\s*[:(]"));

/// A tracking reference: an issue number, or a URL with a destination.
///
/// `https?://` alone identifies nothing, and CH002 once accepted it. An
/// issue number is positive and has no leading zero, as the tracker writes
/// it. `#0` routes nowhere, because numbers start at one. So it is a
/// placeholder, which is the state CH002 exists to refuse.
///
/// A URL needs a host, not only a character after the scheme.
/// `https://).` is punctuation. A host starts with a letter, a digit, an
/// underscore or the `[` of an IPv6 literal, and never with punctuation.
///
/// This is the one definition of a reference. Three patterns read one:
/// forward from a marker, inside a citation after it, and next to it on
/// the left. Five reviews found five ways for those three to disagree. They
/// were a URL destination, a zero issue, a punctuation host, a bracketed
/// citation, and a token boundary with the case of a scheme. Now only the position of
/// a reference differs between them, which is a real difference.
///
/// A scheme is not case-sensitive, by RFC 3986 and in each browser. It also
/// starts where a scheme can start. `nothttps://example.com` holds the
/// letters of a scheme without being one. RFC 3986 gives the characters of
/// a scheme, so a scheme character in front means that the token is
/// something else.
///
/// An issue number ends where the number ends. `#123abc` and `#1_000` are
/// not issue numbers with a suffix. They are not issue numbers. The digits
/// are ASCII, because the tracker uses ASCII. A Unicode `\d` also matches
/// U+0662 and U+FF14, and `#1` before either routes nowhere.
///
/// The prefix in front of the hash is written out, not tolerated. Either
/// the hash opens a token, or a complete `owner/repo` touches it.
/// `owner/repo#123` is a real cross-repository reference, and the tracker
/// renders it as one. `abc#123` routes nowhere. `x/#123` has the slash and
/// no repository, so it is refused. An owner and a repository each start
/// with a letter or a digit, so `-/-#1` is not a reference.
///
/// An IPv6 literal is a host only when its bracket closes, and only when
/// its content can be an address. `https://[` is an opening bracket.
/// `https://[:]` and `https://[....]` are punctuation in brackets. An
/// address has at least one colon and at least one hex digit. That is the
/// cheapest test that refuses both without a full IPv6 parse.
pub const REFERENCE: &str = concat!(
    r"(?:",
    r"(?<![A-Za-z0-9_./\-])",
    r"(?:[A-Za-z0-9][A-Za-z0-9_.\-]*/[A-Za-z0-9][A-Za-z0-9_.\-]*)?",
    r"#[1-9][0-9]*(?!\w)",
    r"|(?<![A-Za-z0-9+.\-])(?i:https?)://",
    r"(?:\[(?=[0-9A-Fa-f:.]*:)(?=[0-9A-Fa-f:.]*[0-9A-Fa-f])[0-9A-Fa-f:.]+\]|\w)",
    r"[^)\]\s]*",
    r")"
);

/// Text that each reference contains. It is a prefilter only.
const REFERENCE_PREFILTER: &str = r"#[1-9]|(?i:https?)://";

/// A tracking reference anywhere. See `REFERENCE`.
pub static TODO_REF_RE: LazyLock<Re> =
    LazyLock::new(|| Re::with_prefilter(REFERENCE, REFERENCE_PREFILTER));

/// The abbreviations that a capital letter can follow.
///
/// A sentence boundary has each clause of `SENTENCE_SPLIT_RE` and no
/// others. The clauses are terminal punctuation, then any emphasis markers,
/// then a space or the end of the text. Both other clauses are load-bearing. The
/// abbreviation guards keep "e.g." inside its sentence. The space keeps
/// "foo.rs" and "v1.2" inside a word. It also keeps a quoted question
/// inside its sentence: the `?` of `the "Ready?" prompt` has a quote after
/// it, not a boundary.
///
/// A sentence ends where the next one starts. That is one rule, not a list
/// of abbreviations. `add retries. See #123` ends at the period. `retain
/// approx. 5 retries under #123` does not, and neither does `the "Ready?"
/// prompt under #123`. The token after the space separates them. A capital
/// or an opening delimiter starts a sentence. A lower-case word or a digit
/// continues the sentence.
///
/// The known limitations of this audit reject that test for the prose
/// splitter. That splitter must not merge two sentences when the second
/// starts with a lower-case identifier. Here the failure has one side
/// only. A sentence that does not end only reaches further, so a reference
/// is read that perhaps is not the marker's own. A sentence that is cut
/// fails the build.
///
/// A closer between the two belongs to the sentence that it ends. That is
/// emphasis, a quote or a bracket. It can be typographic. An editor that
/// turns "ready." into \u201cready.\u201d changes nothing that a reader
/// sees, and everything that an ASCII class matches.
///
/// An abbreviation before a capital is the one case that the next-token
/// test cannot read. "ask Dr. Smith" and "add retries. See" look the same
/// to it. So a list is kept. A list is the correct tool here, because
/// abbreviations are a lexical set. An entry is necessary only for a form
/// that a capital can follow. `approx. 5` and `etc. and` are read without
/// one.
///
/// Each guard is a fixed-width look-behind, which is all that Python
/// allows. So the guards are generated, not written out.
const ABBREVIATIONS: &[&str] = &[
    "e.g", "E.g", "i.e", "I.e", "cf", "Cf", "vs", "Vs", "viz", "al", "etc", "Etc", "resp",
    "approx", "ca", "Dr", "Mr", "Mrs", "Ms", "Prof", "Sr", "Jr", "St", "Fig", "No", "Ref", "Eq",
    "Sec", "Ch", "Vol",
];

/// An initialism is a shape, not a list entry, so it is read and not
/// listed.
///
/// It is single letters separated by periods, as in `U.S.`, `a.m.` and
/// `Ph.D.`. A capital can follow any of them, and the next-token test
/// cannot see that. This also covers `e.g.` and `i.e.`. Their own entries
/// stay because the audit has always named them.
const INITIALISM_GUARD: &str = r"(?<![A-Za-z]\.[A-Za-z])";

fn sentence_end_pattern() -> String {
    let guards = ABBREVIATIONS.iter().fold(String::new(), |mut out, name| {
        let _ = write!(out, r"(?<!\b{})", regex::escape(name));
        out
    });
    format!(
        "{guards}{INITIALISM_GUARD}{}",
        concat!(
            r"[.!?]",
            r#"[*_\"')\]\u2019\u201d\u00bb]*"#,
            r#"(?=\s+[A-Z\"'(\[\u201c\u2018]|\s*$)"#
        )
    )
}

/// A sentence end for the marker rule. See `ABBREVIATIONS`.
pub static SENTENCE_END_RE: LazyLock<Re> =
    LazyLock::new(|| Re::with_prefilter(&sentence_end_pattern(), r"[.!?]"));

/// A bracketed citation at the end of a sentence.
///
/// "TODO: add retries. (#123)" is an example. A citation is part of the
/// sentence that it cites. So that sentence ends after the citation, not
/// at the period in front of it.
pub static TRAILING_REF_RE: LazyLock<Re> = LazyLock::new(|| {
    Re::with_prefilter(
        &format!(r"[ \t]*[(\[][^)\]]*{REFERENCE}[^)\]]*[)\]]"),
        REFERENCE_PREFILTER,
    )
});

/// A reference that touches the marker on its left.
///
/// It is anchored at the end. It opens at the bound or at a clause
/// separator. So `See #123 for the parser. TODO: x` is still untracked,
/// and `#123 - TODO: x` is not.
///
/// A citation is bracketed as often as it is bare. The opener was admitted
/// and the closer was not, so "(#123)" above a marker failed the build. The
/// two ends are one notation. A `)` or `]` after the reference is
/// punctuation around it, as the `(` or `[` before it is.
pub static ADJACENT_REF_RE: LazyLock<Re> = LazyLock::new(|| {
    Re::with_prefilter(
        &format!(r"(?:^|[;.,(\[])[\s\-\u2010-\u2015:]*({REFERENCE})[\s\-\u2010-\u2015:;,)\]]*$"),
        REFERENCE_PREFILTER,
    )
});

/// A run of backticks.
///
/// Both runs of a code span must be exactly the same length. A bare
/// back-reference let a one-backtick opener close against a "``" pair. It
/// closed against the first tick, and after a guard, against the second.
/// That blanked text that CommonMark leaves literal, and it took an
/// absolute rule off that text. A run is a delimiter only when no backtick
/// touches it.
///
/// The scan does not run this pattern. It is the specification of
/// `spans::backtick_runs`, which is faster, and a test checks that the two agree.
#[cfg(test)]
pub static BACKTICK_RUN_RE: LazyLock<Re> = LazyLock::new(|| Re::new(r"`+"));

/// A blank line, which ends a paragraph and so ends a code span.
pub static BLANK_LINE_RE: LazyLock<Re> = LazyLock::new(|| Re::new(r"\n[ \t]*\n"));

/// White space in HTML.
const HTML_SPACE: &str = r"[ \t\r\n\f]";

/// An inline HTML open tag that renders as code.
///
/// Rustdoc puts "<code>not sure why</code>" in a <code> element, as it
/// does a backtick span. So the absolute rules must not read a narrative
/// phrase or a marker inside one. This applies to doc comments only.
/// Nothing renders a `//` comment, where this is literal text.
///
/// This is the open-tag grammar of CommonMark, not an approximation. Two
/// attempts bounded this pattern by what a tag cannot contain. Each
/// accepted a shape that Rustdoc escapes. `<code@example.com>` is a mail
/// autolink, `<code-block>` is a different element, and `<code =x>` has
/// no attribute name where one is necessary. All three rendered the
/// marker beside them as prose, and the audit masked it.
///
/// The direction is the reason to write the grammar out. Each character
/// that this matcher takes loosely turns an absolute rule off to the end of
/// a block. So a generous pattern does not report too much. It goes
/// silent.
fn code_open_pattern() -> String {
    let attr = format!(
        r#"{HTML_SPACE}+[A-Za-z_:][A-Za-z0-9_.:-]*(?:{HTML_SPACE}*={HTML_SPACE}*(?:[^ \t\r\n\f\"'=<>`]+|'[^']*'|\"[^\"]*\"))?"#
    );
    format!(r"(?i)<code(?:{attr})*{HTML_SPACE}*/?>")
}

/// An inline <code> open tag. See `code_open_pattern`.
pub static CODE_OPEN_RE: LazyLock<Re> = LazyLock::new(|| Re::new(&code_open_pattern()));

/// An inline </code> close tag.
pub static CODE_CLOSE_RE: LazyLock<Re> =
    LazyLock::new(|| Re::new(&format!(r"(?i)</code{HTML_SPACE}*>")));

/// A proper name that has an apostrophe in it.
///
/// "Let's Encrypt" is a certificate authority. So a comment that names it
/// is neither deliberation nor a contraction, and "Let us Encrypt" is a
/// wrong expansion. CH003 is absolute, so the name failed the build.
///
/// The key is the capitalized second word, and it is case-sensitive inside
/// two patterns that are otherwise not. The same contraction before a
/// lower-case "encrypt the payload" is still deliberation and still a
/// contraction. Only the name is exempt.
const PROPER_NAME: &str = r"(?!\s+(?-i:Encrypt)\b)";

/// First-person deliberation.
///
/// "Actually" must open a sentence. In the middle of a sentence it is an
/// ordinary adverb, as in "gated on THIS claimant actually, durably
/// marking the row". "lets just" must also open a sentence, for the
/// opposite reason. It is here as a misspelling of the contraction of "let
/// us just". But "the semaphore lets just one claimant proceed" is an
/// ordinary verb, and it states behavior, not deliberation. The other
/// contractions, such as "we would" and "is not" written short, are for
/// CH006. They are contractions, not always deliberation.
pub static NARRATIVE_RE: LazyLock<Re> = LazyLock::new(|| {
    let pattern = format!(
        concat!(
            r"(?i)(?:^|(?<=[.!?;]\s))\s*(?:actually[,\s]|lets just\b)",
            r"|\b(?:let{apos}s\b{name}|we{apos}ll\b|i think\b",
            r"|i{apos}m not sure\b",
            r#"|not sure (?:if|why|whether)\b|for now,|hmm\b|oops\b(?![\"'])|note to self\b"#,
            r"|as you can see\b|todo later\b)"
        ),
        apos = APOS,
        name = PROPER_NAME
    );
    let prefilter = format!(
        concat!(
            r"(?i)actually|lets just|let{apos}s|we{apos}ll|i think|i{apos}m not sure",
            r"|not sure|for now,|hmm|oops|note to self|as you can see|todo later"
        ),
        apos = APOS
    );
    Re::with_prefilter(&pattern, &prefilter)
});

/// Review-round archaeology.
pub static ARCHAEOLOGY_RE: LazyLock<Re> = LazyLock::new(|| {
    Re::new(r"(?i)\b(?:round[- ]\d+|codex round|review round|P[0-4]\s+(?:fix|finding|review))\b")
});

/// Real contractions only.
///
/// This excludes the possessive and abbreviation apostrophe of this
/// corpus, as in "TTL'd overrides" and "the row's bytes". STE permits
/// those.
///
/// The noun forms with `'s` are not matched on purpose: `one's`,
/// `someone's`, `everything's` and similar. They are ambiguous.
/// "someone's waiting" is a contraction, but "someone's row" is a
/// possessive. Over this corpus all 21 cases are possessives, such as "the
/// previous one's outcome" and "the worst one's". To add those stems
/// would report 21 false positives against correct STE. The stems below
/// are the ones with one meaning. The contractions of "it is", "who is"
/// and "there is" have distinct possessive spellings (`its`, `whose`,
/// `theirs`). So a match is always a contraction.
pub static CONTRACTION_RE: LazyLock<Re> = LazyLock::new(|| {
    let pattern = format!(
        concat!(
            r"(?i)\b(?:ca|is|are|was|were|do|does|did|would|could|should|will|has|have|had",
            r"|must|ai|wo|sha|need|ought|might|dare)n{apos}t\b",
            r"|\b(?:it|that|there|here|what|who|how|where|when|why|let|he|she|we|they",
            r"|you|i){apos}(?:s|ll|re|ve|d|m)\b{name}",
            r"|\b(?:should|could|would|must|might){apos}ve\b"
        ),
        apos = APOS,
        name = PROPER_NAME
    );
    Re::with_prefilter(&pattern, APOS)
});

/// The word limit of ASD-STE100 for descriptive text.
pub const MAX_SENTENCE_WORDS: usize = 25;

/// A fence delimiter.
///
/// The indent is captured, not bounded here. The CommonMark limit of
/// "at most three spaces" is relative to the enclosing block container.
/// So a fence inside a list item can have more indent. `fence_delimiter`
/// applies the limit against the container.
pub static FENCE_RE: LazyLock<Re> = LazyLock::new(|| Re::new(r"^([ \t]*)(`{3,}|~{3,})"));

/// A list marker.
///
/// CommonMark limits an ordered marker to nine digits, and that limit is
/// load-bearing here. A longer run of digits is ordinary text. To accept it
/// opens a list, and a fence allowance, where the rendered document has
/// neither. The digits are ASCII. CommonMark says "1-9 arabic digits", so
/// a Unicode decimal digit opens no list, although a Unicode `\d` matches
/// one.
pub static LIST_MARKER_RE: LazyLock<Re> =
    LazyLock::new(|| Re::new(r"^([ \t]*)((?:[-*+]|[0-9]{1,9}[.)]))([ \t]+|$)"));

/// A block quote marker, or several.
///
/// A block quote is a container too, and Rustdoc uses it. Its marker is not
/// indentation. Content inside the quote starts again at column zero. So
/// the marker is stripped before any container or fence decision. Nested
/// forms and forms with no space ("> >", ">>") both count.
///
/// The look-ahead is load-bearing, and only just wide enough. CommonMark
/// needs no space after ">", so ">~~~rust" opens a quoted fence. But a
/// Rust comment that wraps onto a leading ">=" is an operator. To strip its
/// ">" changes the text that each rule then reads, and the text that a Tier
/// B failure quotes back. So the exception is ">=" and nothing else.
///
/// The scan does not run this pattern. It is the specification of
/// `shapes::blockquote`, which is faster, and a test checks that the two agree.
#[cfg(test)]
pub static BLOCKQUOTE_RE: LazyLock<Re> = LazyLock::new(|| Re::new(r"^([ \t]*)((?:>(?!=)[ \t]?)+)"));

/// One block quote marker. See `BLOCKQUOTE_RE`.
///
/// The scan does not run this pattern. It is the specification of
/// `shapes::one_quote`, which is faster, and a test checks that the two agree.
#[cfg(test)]
pub static ONE_QUOTE_RE: LazyLock<Re> = LazyLock::new(|| Re::new(r"^([ \t]*)>(?!=)[ \t]?"));

/// One cell of a GFM delimiter row.
///
/// A table row is not merely a line that opens with a pipe. One pipe is
/// ordinary prose, because "| foo" wraps a sentence like any other word.
/// A flush there drops the rest of the sentence out of the unit.
pub static DELIM_CELL_RE: LazyLock<Re> = LazyLock::new(|| Re::new(r"^(?::?-+:?)\z"));

/// A cell boundary.
///
/// An escaped pipe is content. Rustdoc renders "| a \\| b | c |" as two
/// columns, and the first holds a pipe.
///
/// The scan does not run this pattern. It is the specification of
/// `markdown::cell_splits`, which is faster, and a test checks that the two agree.
#[cfg(test)]
pub static CELL_SPLIT_RE: LazyLock<Re> = LazyLock::new(|| Re::new(r"(?<!\\)\|"));

/// The start of an explicit doc attribute.
///
/// `\#[doc = "Two."]` and `\#![doc = "Two."]` render a paragraph.
/// `\#[doc(hidden)]` and `\#[doc(alias = "zz")]` render nothing. Group 1
/// tells the outer and inner forms apart.
pub static DOC_VALUE_RE: LazyLock<Re> = LazyLock::new(|| Re::new(r"^\#(!?)\["));

/// The `doc = ...` of an attribute, anywhere inside it.
///
/// The search reads the whole attribute, not its first line. An attribute
/// can wrap and put its payload three lines down.
///
/// `cfg_attr` can hold one. rustdoc 1.94.1 renders two paragraphs for
/// `\#[cfg_attr(all(), doc = "Two.")]` after a blank doc line. It nests, so
/// `cfg_attr(all(), cfg_attr(all(), doc = "Six."))` renders them too. A
/// search of the attribute finds each depth with no count.
///
/// The predicate is not read, on purpose. `\#[cfg_attr(any(), doc =
/// "Four.")]` renders nothing, so to treat it as content is an
/// under-report. But `\#[cfg_attr(feature = "x", doc = "...")]` cannot be
/// decided from the source. The two errors are not equal. A missed blank
/// line is untidy. A Tier A false positive fails the build on correct
/// documentation.
///
/// A `doc =` inside a string cannot reach this. The scan reads the copy
/// from `blank_non_code`, where string bodies are already blank. The `[`
/// in the class matters, because the plain `\#[doc = "Two."]` opens with
/// one.
pub static DOC_PAYLOAD_RE: LazyLock<Re> =
    LazyLock::new(|| Re::new(r"(?:^|[(\[,\s])(?:r\#)?doc\s*="));

/// An ATX heading. The indent is a group, because the indent decides if
/// this is a heading at all. See `heading`.
pub static HEADING_RE: LazyLock<Re> = LazyLock::new(|| Re::new(r"^([ \t]*)#{1,6}(?:[ \t]|$)"));

/// A section rule: one punctuation character, repeated.
///
/// CommonMark writes it "---". This tree also draws section rules with
/// box-drawing characters. Either way it separates blocks, so a sentence
/// never runs across one.
///
/// The scan does not run this pattern. It is the specification of
/// `shapes::separator`, which is faster, and a test checks that the two agree.
#[cfg(test)]
pub static SEPARATOR_RE: LazyLock<Re> = LazyLock::new(|| {
    Re::new("^[ \t]*([-_*=\u{2500}\u{2501}\u{2550}\u{00b7}])(?:[ \t]*\\1){2,}[ \t]*$")
});

/// A thematic break, the CommonMark subset of `SEPARATOR_RE`.
///
/// Only `-`, `_` and `*` make a thematic break. "===" and the box-drawing
/// rules of this tree are ordinary paragraph text to Rustdoc. The broad
/// set still ends a prose unit, because a section rule is not a word of
/// the sentence under it. But only a real break can change container
/// state. Otherwise a decorative line gives the next "22." a container,
/// and its fence an allowance, that the rendered document does not have.
///
/// The indent here has no bound on purpose, the only such pattern here. A
/// section rule is not a word of the sentence at any indent. This pattern
/// only ends a prose unit. It opens no container and closes no paragraph,
/// so a match with too much indent costs a sentence nothing.
///
/// The scan does not run this pattern. It is the specification of
/// `shapes::thematic_break`, which is faster, and a test checks that the two agree.
#[cfg(test)]
pub static THEMATIC_BREAK_RE: LazyLock<Re> =
    LazyLock::new(|| Re::new("^([ \t]*)([-_*])(?:[ \t]*\\2){2,}[ \t]*$"));

/// A Setext underline.
///
/// "=" under a paragraph line makes that paragraph a heading. Position, not
/// shape, decides if a run of "=" is a heading or ordinary text. With a
/// paragraph open it underlines one. At the start of a block it is a
/// decorative rule.
pub static SETEXT_RE: LazyLock<Re> = LazyLock::new(|| Re::new("^([ \t]*)(?:=+|-+)[ \t]*$"));

/// A GFM task-list marker, which Rustdoc renders as a check box.
///
/// It is exactly "[ ]", "[x]" or "[X]", and a space must follow. rustdoc
/// 1.94 renders "[]", "[ ]no space" and "[y]" as literal words.
///
/// The scan does not run this pattern. It is the specification of
/// `shapes::task_marker`, which is faster, and a test checks that the two agree.
#[cfg(test)]
pub static TASK_MARKER_RE: LazyLock<Re> = LazyLock::new(|| Re::new(r"^\[[ xX]\](?=[ \t]|$)"));

/// The HTML block tag names of CommonMark, types 1 and 6.
///
/// A line that opens with one of these is its own block, and it ends the
/// paragraph above it. Rustdoc renders "Intro." then "<pre>raw</pre>" as a
/// paragraph and a block, so the "22." under them starts a list. Type 7
/// (any other complete tag alone on a line) is absent on purpose. It cannot
/// interrupt a paragraph, and "<T>" in a Rust comment is a type parameter,
/// not markup.
const HTML_BLOCK_TAGS: &str = concat!(
    "address|article|aside|base|basefont|blockquote|body|caption|center|col|",
    "colgroup|dd|details|dialog|dir|div|dl|dt|fieldset|figcaption|figure|",
    "footer|form|frame|frameset|h1|h2|h3|h4|h5|h6|head|header|hr|html|iframe|",
    "legend|li|link|main|menu|menuitem|nav|noframes|ol|optgroup|option|p|",
    "param|search|section|summary|table|tbody|td|tfoot|th|thead|title|tr|",
    "track|ul|script|pre|style|textarea"
);

/// The opening line of an HTML block.
pub static HTML_BLOCK_RE: LazyLock<Re> = LazyLock::new(|| {
    Re::new(&format!(
        r"(?i)^([ \t]*)(?:<[/]?(?:{HTML_BLOCK_TAGS})(?:[ \t/>]|$)|<!--|<\?|<![A-Za-z]|<!\[CDATA\[)"
    ))
});

/// The opening line of a verbatim HTML block.
///
/// Types 1 to 5 end on the line that has their closer. That can be the
/// opening line itself, as in "<pre>raw</pre>". Type 6 ends at a blank
/// line, which is not part of it.
pub static HTML_VERBATIM_RE: LazyLock<Re> =
    LazyLock::new(|| Re::new(r"(?i)^[ \t]*<(script|pre|style|textarea)(?:[ \t/>]|$)"));

/// A sentence boundary for the prose rules.
///
/// The two abbreviations that this corpus writes constantly are excluded,
/// because neither ever ends a sentence in English. A split there cuts one
/// sentence in two, and a 27-word sentence with "e.g." then passes CH007.
/// Only those two are excluded. "etc." and "vs." do end sentences, so to
/// exclude them would merge two real sentences and report too much.
///
/// A closing delimiter can sit between the full stop and the space. A
/// sentence that ends in "**strong emphasis.**" puts two asterisks there.
/// A rule that needed white space right after the stop merged it with the
/// next sentence. That reported two compliant sentences as one long one.
///
/// This is emphasis only. Each exclusion below was measured against the
/// corpus, not reasoned about. Each of the other closing delimiters gave
/// false splits in this tree:
///
///   * A backtick: a code span often holds punctuation, such as "`?`",
///     "`.`" and "`foo.bar()`". To admit it cut three hundred sentences at
///     the closing backtick.
///   * A closing parenthesis: this tree writes a parenthesis that ends in
///     an ellipsis, as in "(first is `0`, ...) -- so prefix explicitly".
///     There the ellipsis is in the middle of a sentence.
///   * A quote: "answer \"is `charge_card` held?\" during an incident" is
///     one sentence, and the question mark in the quotation is not its end.
///
/// A closing "**" after a full stop has no such second meaning.
///
/// The scan does not run this pattern. It is the specification of
/// `shapes::sentence_splits`, which is faster, and a test checks that the two agree.
#[cfg(test)]
pub static SENTENCE_SPLIT_RE: LazyLock<Re> =
    LazyLock::new(|| Re::new(r"(?<=[.!?])(?<!e\.g\.)(?<!E\.g\.)(?<!i\.e\.)(?<!I\.e\.)[*_]*\s+"));

/// A raw-string opener: r"", r#""#, br##""##, and the c"" and cr#""# forms.
pub static RAW_OPEN_RE: LazyLock<Re> = LazyLock::new(|| Re::new(r#"^(?:b|c|br|cr|rb)?r(#*)""#));

/// A char literal, as opposed to a lifetime tick.
///
/// `'a'`, `'\n'` and `'\u{1F600}'` are literals. `'a` in `&'a str` is a
/// lifetime, and it must not open a string.
pub static CHAR_LIT_RE: LazyLock<Re> =
    LazyLock::new(|| Re::new(r"^'(?:\\(?:x[0-9a-fA-F]{2}|u\{[0-9a-fA-F]{1,6}\}|[^\n])|[^\\'\n])'"));

/// Compiles each pattern and returns the first error.
pub fn check_all() -> Result<(), String> {
    let all: [&LazyLock<Re>; 23] = [
        &COMMENTED_CODE_RE,
        &TODO_RE,
        &TODO_REF_RE,
        &SENTENCE_END_RE,
        &TRAILING_REF_RE,
        &ADJACENT_REF_RE,
        &BLANK_LINE_RE,
        &CODE_OPEN_RE,
        &CODE_CLOSE_RE,
        &NARRATIVE_RE,
        &ARCHAEOLOGY_RE,
        &CONTRACTION_RE,
        &FENCE_RE,
        &LIST_MARKER_RE,
        &DELIM_CELL_RE,
        &DOC_VALUE_RE,
        &DOC_PAYLOAD_RE,
        &HEADING_RE,
        &SETEXT_RE,
        &HTML_BLOCK_RE,
        &HTML_VERBATIM_RE,
        &RAW_OPEN_RE,
        &CHAR_LIT_RE,
    ];
    all.iter().try_for_each(|re| re.check())
}
