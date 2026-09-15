## Phase — harvest-verify correctness follow-ups from Codex round 6-8 (issue #1296)

Nine correctness findings from three Codex review rounds on #1294 (rounds
6-8, after that PR's review budget was exhausted), all in
`autumn-harvest-verify` — no engine footprint. Each landed TDD-style: a RED
fixture first, then the fix. All nine are false-`proven-deterministic` or
misclassification risks except the two driver.rs crate-attribution findings,
which are precision issues.

**1. Sanitizer kills are now flow-sensitive by block dominance (P1).** A
sanitizer kill (`sort()` clearing `Order` taint) used to apply as a body-wide
blanket clear the instant it was discovered, including to reads that execute
*before* the sanitizer ever runs. `TaintState` no longer strips a fact at
write time; a kill is recorded with the block it happened in, and a read —
taken at its own block via the new `TaintState::read_at` — is filtered only
when the kill's block **dominates** it (`analysis::control::ControlGraph`
gained a forward-dominance table alongside its existing post-dominance one).
MIR always lowers a call as a block terminator, so a sink and a later
sanitizer never share a block; block-level dominance alone settles which side
of the sanitizer a read is on.

**2. The taint fixpoint cap needed a boundary on exhaustion (P1).** The
per-body round loop (`MAX_ROUNDS = 24`) used to stop silently when state was
still changing on the last round, reporting the unconverged partial state as
if it were the true fixpoint. A new `fixpoint-exhausted` boundary is raised
instead, so a body whose dependency chain needs more rounds than the cap
allows reports `unknown`, never `proven-deterministic`.

**3. `--mir` directory walks now propagate I/O errors (P2).**
`collect_mir_paths`/`collect_dir` used to treat an unreadable `--mir`
directory as empty, turning a permissions failure into `analyzed 0` with
exit 0 under a non-strict run. Both now return `Result` and error out;
skipping a symlink or non-regular file stays deliberate and silent.

**4. Overlay-added `[[trusted]]` crates now reach `resolve_call` (P2).**
`Program::resolve_call`'s own trusted-crate check read only the builtin
model (`Model::builtin_ref()`), never the merged model a `--model` overlay
produces, so an overlay-trusted crate's body-less calls could still become
an `external-crate-body` boundary before the (already overlay-aware)
analysis-stage trust check got a say. `Program::build_with_model` threads
the merged model's `[[trusted]]` set through; `Program::build` keeps the
old builtin-only behavior for callers with no overlay to merge.

**5. The per-place taint-fact cap is now per kind, not global (P1).**
`TaintSet::insert` capped at six facts *total*; six `Order` facts could fill
the cap before a later `Value` fact arrived, silently dropping it. The cap
now applies per `TaintKind`.

**6. Body-less function-item callbacks are classified (P1).** A higher-order
call receiving an external function item (`.unwrap_or_else(SystemTime::now)`)
resolved to no targets and was silently ignored, trusting the combinator
with clean arguments. The callback's path now runs through `Model::classify`
exactly like a direct call; an unclassified, untrusted one raises
`external-crate-body`.

**7. Unresolved closure arguments are now a named boundary (P1).** A closure
argument whose span is present but whose body is absent from the analyzed
set (a partial dump, a handler registration never emitted) used to
contribute nothing silently. A new `unresolved-callback` boundary is raised
instead.

**8. Only hexadecimal metadata hashes are stripped from MIR filenames (P2).**
`crate_name_from_stem` used `is_ascii_alphanumeric()` where the
artifact-path helpers already required `is_ascii_hexdigit()`; a hand-supplied
`--mir` file such as `payments-workflows.mir` had `workflows` treated as
rustc's metadata hash. All three call sites now share one `is_metadata_hash`
predicate.

**9. Opaque `-p` package specs are resolved via `cargo pkgid`, not accepted
as "every workspace member" (P2).** A `-p` SPEC this tool's own parser could
not read a bare `name` from (a full package id, a registry/git URL) used to
fall back to accepting artifacts from any workspace member — letting a
dependency's workflows through whenever the selected package had none of its
own. Such a SPEC is now resolved to its exact package id via `cargo pkgid`;
an unresolvable one is a hard error, never "every package".

**Also in this pass:** `Allowlist` gained `#[serde(deny_unknown_fields)]`
(the model structs already had it); the R&D report's boundary table and
Known-imprecisions list were updated to match — fourteen boundary kinds now,
up from twelve, and the three closed imprecisions were removed rather than
left stale.

No new `WorkflowEvent` variant, no migration, no `#[workflow]` macro change —
this PR touches only the build-time verifier.
