## Phase 5.x — dev runtime: the `sslmode` scan reads syntax, not substrings (issue #1286)

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

**Both** of the banner's redaction branches now rewrite by splicing over a
password value's byte span, so bytes the reader merely walked past are unchanged
by construction rather than by reassembly. The keyword/value branch keeps the
key it matches on exactly (`password` alone, case-insensitive). The URI branch
changes in one visible way: it replaces only the *value*, where it used to
rewrite the whole pair, so a percent-encoded key survives as written
(`?%70assword=hunter2` → `?%70assword=***`, previously `?password=***`). The
encoded key is not itself a secret, and not reshaping the DSN is what the banner
promises everywhere else.

**A regression caught in review, and the test that pins it.** Lifting the
scanner from a `Vec<char>` walk to byte indices silently narrowed the option
separator from `char::is_whitespace` to `u8::is_ascii_whitespace`. The client
separates options on the Unicode `White_Space` property
(`tokio_postgres::config`'s `skip_ws`), so
`host=localhost\u{a0}password=hunter2` really is two options to the code that
dials — but the ASCII-only scan read it as one, found no `password` key, and
printed the credential in the one string whose stated purpose is to be safe to
paste into an issue. The same gap deleted real options from the printed DSN
(the splice covered more bytes than the client's token), so the banner could
name a different database than the runtime connected to. The scanner is now
character-based throughout, and
`options_are_separated_the_way_the_client_separates_them` asserts it over eight
separators — including U+000B, which is ASCII but which
`is_ascii_whitespace` excludes.

**Zero engine impact:** no new `WorkflowEvent` variant, no migration, no schema
change, no public API change. `dev::dsn` is a private module; the only exported
signature that moved is none.

**A second regression caught by Codex, and the rule that replaces it.** The URI
branch first took the query to start at the first `?`. It does not:
`tokio_postgres` reads userinfo with `take_until(&['@'])` across the whole
remaining string *before* it looks for anything else, so in
`postgres://u?%73slmode=verify-full&x=y@localhost/app` the `?…` is part of the
**username** and the DSN has no query string at all. Splitting early invented a
parameter out of a credential, and because the invented key percent-decodes to
`sslmode`, the gate refused a loopback database it should have allowed — the
defect this change exists to remove, reintroduced through the other syntax.
`dsn::query_start` now walks it as the client does: credentials to the first
`@`, then the first `?` after them (the host stops at `/` or `?` and a path
stops at `?`, so the two agree from there). The same locator fixes the leaking
direction of the same bug in the banner — `postgres://x?a=1@localhost/db?password=hunter2`
printed the credential, because the real `password=` parameter sat behind what
the old split had already consumed as the whole query.

`query_start` skips leading whitespace before looking for the scheme, because
`is_uri_dsn` skips it for the URI/keyword decision — the two have to make the
same allowance or a space-prefixed DSN goes down the URI branch and is then
reported as having no query string, which left `HARVEST_DEV_DATABASE_URL="  postgres://u@localhost/db?password=hunter2"`
printing its password. Offsets stay absolute, so the whitespace reaches the
banner exactly as it was typed.

**Test evidence.** Seventeen new tests in
`autumn-harvest-plugin/tests/dev_runtime_tests.rs` (both reproductions from the
issue, both still-refused cases, `options`/`application_name` values, a URI path
segment, percent-encoded keys and values, an adversarial-syntax sweep asserting
the scan terminates and never slices off a character boundary, and a banner
redaction case with a multi-byte escape) plus sixteen unit tests on the readers
themselves in `autumn-harvest-plugin/src/dev/dsn.rs`, including one asserting the
spans are forward-only and non-overlapping — which is what makes the banner's
splice rewrite safe. `cargo test -p autumn-harvest-plugin --features dev-runtime`
is green; so is `cargo clippy -p autumn-harvest-plugin --all-targets --features
dev-runtime -- -D warnings`, which needed a one-word `clippy::doc_markdown` fix
in `dev/reaper.rs` that was already failing on the base branch and blocking the
lint from reaching any of this.
