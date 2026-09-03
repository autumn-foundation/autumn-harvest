## Phase 12.4 — dev runtime: the `sslmode` scan reads syntax, not substrings (issue #1286)

`classify_database_url` runs a textual `sslmode` scan *before* handing the DSN
to `tokio_postgres::Config::from_str`, because the client models only
`disable`/`prefer`/`require` — so `verify-ca` and `verify-full`, the two
strongest signals that a DSN points at a remote managed database, would
otherwise arrive as an opaque parse error rather than as the specific thing
they are. That scan was a bare `dsn.find("sslmode=")`, which has no idea
whether it landed on a key or on the inside of a value, so **any** DSN merely
*containing* the text was refused as TLS-requiring:

```
host=localhost password='sslmode=require' dbname=harvest_dev
postgres://u:pw@localhost/app?application_name=sslmode=require
```

Both are legal loopback databases the gate exists to accept, and both came back
`Refused(TlsRequired { sslmode: "require" })` — a message describing a state the
DSN is not in, which is a confusing thing to debug. **Usability, not security:
the gate fails closed, so it refused a database it should have allowed and never
the reverse.**

**The fix is to answer "which syntax is this?" first, and only then look for a
key.** A new private `dev::dsn` module holds the three facts `safety` and
`banner` both need and were each answering for themselves:

- `is_uri_dsn` — the literal, case-sensitive prefix test `Config::from_str`
  itself uses, moved out of `banner` unchanged.
- `keyword_options` — the quote- and escape-aware keyword/value scanner, lifted
  out of `banner::redact_keyword_value`, turned into a lazy iterator and given
  each value's byte span. Lenient by design (a bare token is skipped, an
  unterminated quote ends the scan): both callers already have an authority for
  malformed input, so bailing out would only lose the options that were legible.
- `uri_query_parameters` / `percent_decoded` — split on `&` then the *first*
  `=`, decoding keys and values, so a parameter's value can never be read as the
  query string's own key.

Answering those separately is what produced the bug: `safety` searched the raw
text while `banner`'s scanner two modules over already knew that span was a
password value.

**This is not a second DSN parser.** Nothing in `dev::dsn` decides what gets
connected to; `Config::from_str` and `config.get_ssl_mode()` remain the
authority, unchanged and still running after the scan — see `safety`'s module
docs for why a second opinion on *that* question is a bypass waiting to happen.
The scan may only refuse **more** than the client would, so a span it misses
costs a clearer message and never a connection the gate meant to refuse.

Two behaviour improvements fall out of reading the DSN properly. The scan now
sees `sslmode = verify-full`, `sslmode='verify-ca'` and a percent-encoded
`%73slmode=verify-full` — spellings the substring search missed and
`get_ssl_mode()` cannot name, because the client refuses to parse `verify-*` at
all. And every `sslmode` occurrence is considered rather than the last one the
client would keep, which stays on the failing-closed side of a duplicated key.

`banner::redact_keyword_value` keeps its behaviour exactly (still `password`
alone, still case-insensitive) but now rewrites by splicing over each password
value's byte span, so bytes the scanner merely walked past are unchanged by
construction rather than by reassembly.

**Zero engine impact:** no new `WorkflowEvent` variant, no migration, no schema
change, no public API change. `dev::dsn` is a private module; the only exported
signature that moved is none.

**Test evidence.** Nine new tests in
`autumn-harvest-plugin/tests/dev_runtime_tests.rs` (both reproductions from the
issue, both still-refused cases, `options`/`application_name` values, a URI path
segment, percent-encoded keys and values, an adversarial-syntax sweep asserting
the scan terminates and never slices off a character boundary, and a banner
redaction case with a multi-byte escape) plus thirteen unit tests on the scanner
itself in `autumn-harvest-plugin/src/dev/dsn.rs`, including one asserting the
spans are forward-only and non-overlapping — which is what makes the banner's
splice rewrite safe. `cargo test -p autumn-harvest-plugin --features dev-runtime`
is green; so is `cargo clippy -p autumn-harvest-plugin --all-targets --features
dev-runtime -- -D warnings`, which needed a one-word `clippy::doc_markdown` fix
in `dev/reaper.rs` that was already failing on the base branch and blocking the
lint from reaching any of this.
