#!/usr/bin/env python3
"""Folio corpus harness: config-key / env-var / CLI-flag drift scan for docs/.

Deterministic, reproducible on any checkout — no network access required.
This is the drift-scan Tier-1 audit described in docs/audits/README.md: it
cross-references the docs corpus against the CURRENT public surface (the
`[harvest]` TOML config schema, its env-var overrides, and the `harvest` CLI's
flags) and reports any doc-cited key, env var, or flag that does not actually
exist in source. An answer that used to be true and no longer is is worse
than no answer — the reader cannot detect the difference, and follows it into
a config file or command line that silently does nothing.

Ground truth is extracted mechanically from two files, never hand-maintained:

- `autumn-harvest-plugin/src/config.rs` — the `PartialHarvestRuntimeConfig`
  struct tree (what `autumn.toml`'s `[harvest]` section actually deserializes
  into) gives the TOML config-key paths; `env.var("...")` call sites give the
  `AUTUMN_*` env-var overrides.
- `autumn-harvest-cli/src/lib.rs` — every `#[arg(...)]`-annotated field gives
  a CLI flag (explicit `long = "..."` wins; otherwise the field name in
  kebab-case), and every `env = "..."` on one of those gives a CLI-level env
  var (`HARVEST_*`).

Two doc-side checks run against that ground truth:

1. Config-key check — dotted `harvest.foo.bar` keys, ONLY as written inside a
   fenced ```toml block carrying a `[harvest...]` section header: `[section]`
   headers + `key = value` lines resolve to a dotted path, checked against
   the real schema. (An earlier version also matched a bare inline
   `` `harvest.x.y = value` `` code span outside any TOML block; dropped —
   OTel span attributes and Prometheus metric/alert expressions are written
   the identical way and live in a wholly different namespace, e.g.
   `harvest.replay = true` in a trace excerpt, `harvest.replication.standbys
   == 0` in an alert — so every such hit was a false positive, not drift.)
2. Env-var check — any `AUTUMN_HARVEST[A-Z0-9_]*` token anywhere in the page
   (see ENV_TOKEN_RE below for why this deliberately excludes bare
   `HARVEST_*`), and `--flag-name` tokens on a line that invokes the
   `harvest` binary inside a fenced code block.

KNOWN LIMITATIONS:

- The CLI-flag check is command-unaware: it checks a flag exists SOMEWHERE on
  the `harvest` CLI, not that it exists on the specific subcommand a doc
  example invokes. A flag real on one subcommand but wrongly attached to
  another in a doc example is not caught. Catching that needs a real clap
  parse (or a per-subcommand field map) — bigger scope than this pass; every
  drift instance found so far is a flag absent from the CLI entirely, which
  this catches.
- Config-key extraction only walks `PartialHarvestRuntimeConfig` and structs
  reachable from it. A config surface added outside that tree (a different
  plugin's `[other_plugin]` section) is invisible to this scanner — out of
  scope; this audit is specifically the Harvest config/CLI surface.
- The env-var check is scoped to `AUTUMN_HARVEST*` only (see ENV_TOKEN_RE) —
  a real drift in a bare `HARVEST_*` CLI-declared var (`HARVEST_TOKEN` etc.)
  would not be caught. Accepted: that namespace isn't closed (many unrelated
  tools mint their own `HARVEST_*` vars), so checking it produced only false
  positives, not signal.

Usage:
    python3 docs/audits/config-cli-drift.py [--json]

Exit code is 1 if a drift hit is found in the reader corpus (the same
corpus/process-artifact split as corpus-link-check.py — see
PROCESS_ARTIFACT_PREFIXES there). A hit inside a process-artifact subtree is
reported but does not fail the run.
"""
import argparse
import importlib.util
import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_RS = REPO_ROOT / "autumn-harvest-plugin/src/config.rs"
CLI_RS = REPO_ROOT / "autumn-harvest-cli/src/lib.rs"

# `corpus-link-check.py` can't be `import`ed by its literal (hyphenated) name;
# loaded by path instead so this script shares that one's process-artifact
# reachability closure rather than re-implementing the prefix-only half of it
# (Codex review, PR #1373: a prefix-only check disagrees with
# corpus-link-check.py's convention for a reader-reachable page like
# docs/rnd/determinism-static-analysis.md, which that script promotes into
# the gated corpus and a prefix-only check would not).
_link_check_spec = importlib.util.spec_from_file_location(
    "corpus_link_check", Path(__file__).with_name("corpus-link-check.py")
)
_link_check = importlib.util.module_from_spec(_link_check_spec)
_link_check_spec.loader.exec_module(_link_check)
PROCESS_ARTIFACT_PREFIXES = _link_check.PROCESS_ARTIFACT_PREFIXES
compute_corpus_reachable = _link_check.compute_corpus_reachable


# CLI-level env vars declared directly on `Cli`/subcommand `#[arg(env = ...)]`
# fields use a bare `HARVEST_*` prefix (no `AUTUMN_` — they're read by the
# `harvest` binary itself, not through Autumn's config-file/env layer), so
# they're collected by the same CLI_RS scan as the flags, no separate list
# needed here.

FIELD_RE = re.compile(r"^\s*(?:pub\s+)?(\w+)\s*:\s*([\w:<>]+)\s*,?\s*$")
STRUCT_RE = re.compile(r"struct\s+(\w+)\s*\{(.*?)\n\}", re.DOTALL)


def bare_type_name(ty: str) -> str:
    # `Option<Foo>` -> `Foo` (config fields are only ever bare or
    # single-wrapped in Option in this file — no Vec/HashMap here).
    m = re.match(r"^Option<(.+)>$", ty)
    if m:
        ty = m.group(1)
    return ty.rsplit("::", 1)[-1]


def parse_rust_structs(text: str) -> dict:
    """Map struct name -> [(field_name, bare_type_name), ...] for every
    plain `struct Name { ... }` in the file (no nested `{}` in the body,
    true for every struct in config.rs)."""
    structs = {}
    for m in STRUCT_RE.finditer(text):
        name, body = m.group(1), m.group(2)
        fields = []
        for line in body.splitlines():
            line = line.strip()
            if not line or line.startswith("//") or line.startswith("#["):
                continue
            fm = FIELD_RE.match(line)
            if fm:
                field, ty = fm.group(1), fm.group(2)
                fields.append((field, bare_type_name(ty)))
        structs[name] = fields
    return structs


def config_key_paths(structs: dict, root: str, prefix: str) -> set:
    """Recursively walk the Partial* struct tree into dotted `harvest.x.y`
    paths. A field whose bare type is itself a known struct is a nested
    config section (recurse, no leaf emitted for the field itself); every
    other field is a leaf key."""
    paths = set()
    for field, ty in structs.get(root, []):
        dotted = f"{prefix}.{field}"
        if ty in structs:
            paths |= config_key_paths(structs, ty, dotted)
        else:
            paths.add(dotted)
    return paths


ENV_VAR_CALL_RE = re.compile(r'env\.var\("([A-Z0-9_]+)"\)')


def extract_config_ground_truth():
    text = CONFIG_RS.read_text(encoding="utf-8")
    structs = parse_rust_structs(text)
    config_paths = config_key_paths(structs, "PartialHarvestRuntimeConfig", "harvest")
    env_vars = set(ENV_VAR_CALL_RE.findall(text))
    return config_paths, env_vars


ARG_ATTR_RE = re.compile(r"#\[arg\((.*?)\)\]", re.DOTALL)
LONG_NAME_RE = re.compile(r'long\s*=\s*"([^"]+)"')
BARE_LONG_RE = re.compile(r"(?:^|,)\s*long\s*(?:,|$)")
ENV_NAME_RE = re.compile(r'env\s*=\s*"([A-Z0-9_]+)"')
NEXT_FIELD_RE = re.compile(r"^\s*(?:pub\s+)?(\w+)\s*:")


def extract_cli_ground_truth():
    text = CLI_RS.read_text(encoding="utf-8")
    lines = text.splitlines()

    # Join the whole file so a multi-line #[arg(...)] attribute is captured
    # as one block by ARG_ATTR_RE, then map each match back to a line number
    # so we can look at the field declaration on the lines right after it.
    flags = set()
    env_vars = set()

    for m in ARG_ATTR_RE.finditer(text):
        attr_body = m.group(1)
        if "long" not in attr_body:
            continue  # positional or short-only arg — not a `--flag`
        end_line = text.count("\n", 0, m.end())

        name_m = LONG_NAME_RE.search(attr_body)
        if name_m:
            flags.add(name_m.group(1))
        elif BARE_LONG_RE.search(attr_body):
            # Bare `long` (no explicit name) — the flag is the field name in
            # kebab-case. Scan forward past any doc comments/further
            # attributes to the field declaration.
            for line in lines[end_line : end_line + 6]:
                stripped = line.strip()
                if not stripped or stripped.startswith("///") or stripped.startswith(
                    "#["
                ):
                    continue
                fm = NEXT_FIELD_RE.match(line)
                if fm:
                    flags.add(fm.group(1).replace("_", "-"))
                break

        env_m = ENV_NAME_RE.search(attr_body)
        if env_m:
            env_vars.add(env_m.group(1))

    # clap's derive `Parser`/`Subcommand` adds `--help` and `--version` to
    # every level of the command tree automatically — they're never
    # `#[arg(...)]` fields, so the loop above can't see them. Real unless
    # explicitly suppressed (`disable_help_flag`/`disable_version_flag` on a
    # `#[command(...)]`, absent anywhere in this file — checked), and a
    # fenced `$ harvest --help` example is exactly the kind of doc snippet
    # this scanner should not flag (Codex review, PR #1373:
    # `docs/shipped-work.md` already cites `harvest --help` in prose, and
    # the CLI's own `the_real_binary_*` integration tests invoke it for real).
    flags.add("help")
    flags.add("version")

    return flags, env_vars


BLOCKQUOTE_PREFIX_RE = re.compile(r"^(>[ \t]*)")


def mask_fenced(lines):
    """Return (masked_lines, list_of_(lang, block_lines)) — masked_lines has
    every fenced block blanked (for the inline-code-span scan), block list
    carries each fenced block's language tag and raw lines (for the
    TOML/CLI-invocation scans, which need fence content).

    Recognizes one level of `>` blockquote prefix on every line of the fence
    (open, body, close) — the shape docs/runbooks/triage-pending-tasks-idle
    -workers.md uses for its `harvest workflow diagnose ... --json` example
    (Codex review, PR #1373). Nested/multi-level blockquotes, or a body line
    that omits the prefix (CommonMark's "lazy continuation"), aren't handled
    — real scope beyond what this corpus's one instance needs, same call
    corpus-link-check.py's docstring makes for the identical shape."""
    out = []
    blocks = []
    fence_char = None
    fence_len = 0
    cur_lang = None
    cur_lines = None
    bq_prefix = ""
    for line in lines:
        if fence_char is None:
            bq_m = BLOCKQUOTE_PREFIX_RE.match(line)
            candidate = line[bq_m.end() :] if bq_m else line
            m = re.match(r"^[ \t]{0,3}(`{3,}|~{3,})\s*([\w+-]*)", candidate)
            if m:
                fence_char, fence_len = m.group(1)[0], len(m.group(1))
                cur_lang = m.group(2).lower()
                cur_lines = []
                bq_prefix = bq_m.group(1) if bq_m else ""
                out.append("")
                continue
            out.append(line)
            continue
        body = line[len(bq_prefix) :] if bq_prefix and line.startswith(bq_prefix) else line
        if re.match(
            rf"^[ \t]{{0,3}}{re.escape(fence_char)}{{{fence_len},}}[ \t]*$", body
        ):
            fence_char, fence_len = None, 0
            blocks.append((cur_lang, cur_lines))
            cur_lines = None
            bq_prefix = ""
            out.append("")
            continue
        cur_lines.append(body)
        out.append("")
    return out, blocks


# Scoped to `AUTUMN_HARVEST*` only — NOT a bare `HARVEST_*` prefix. `HARVEST_*`
# is not a namespace this scanner has ground truth for: bench harnesses,
# integration tests, curl-auth runbooks, and dev-setup docs all mint their
# own `HARVEST_*` env vars (`HARVEST_TEST_DATABASE_URL`, `HARVEST_BENCH_*`,
# `HARVEST_STAGING_URL`, `HARVEST_ADMIN_TOKEN`, ...) that are real and
# defined elsewhere in the workspace, not in CLI_RS or CONFIG_RS — an
# earlier version of this scan checked bare `HARVEST_*` too, and every
# resulting hit was one of those real-but-elsewhere-defined vars, not drift.
# `AUTUMN_HARVEST*` is different: CONFIG_RS's own reserved override
# namespace, fully enumerated by extract_config_ground_truth(), so a
# doc-cited `AUTUMN_HARVEST*` var missing from that set is unambiguous.
# `+`, not `*`: at least one char must follow `AUTUMN_HARVEST` for this to be
# a candidate real env var at all (every real one has a `__FIELD` suffix at
# minimum). Using `*` treated the bare namespace notation `AUTUMN_HARVEST*`
# — used in this file's own module docstring and in docs/audits/README.md to
# describe the family, not to cite a specific variable — as itself a drift
# hit (Codex review, PR #1373: this made the scanner fail on its own repo).
ENV_TOKEN_RE = re.compile(r"\b(AUTUMN_HARVEST[A-Z0-9_]+)\b")
SECTION_HEADER_RE = re.compile(r"^\s*\[([\w.]+)\]\s*$")
TOML_KV_RE = re.compile(r"^\s*([\w.]+)\s*=")
# `(?!-)` excludes `harvest-verify` (a distinct binary/cargo-subcommand,
# `autumn-harvest-verify`, invoked as `cargo harvest-verify ...` — its own
# flag set, e.g. `--all-examples`, `--source-root`, is unrelated to the
# `harvest` management CLI this scanner has ground truth for) from being
# read as an invocation of the `harvest` binary just because "harvest" is a
# leading substring of its name.
HARVEST_CMD_LINE_RE = re.compile(r"(?:^|\s|\$)harvest(?!-)\b(.*)$")
FLAG_TOKEN_RE = re.compile(r"--([a-z][a-z0-9-]*)")


def join_shell_continuations(lines):
    """Collapse trailing-backslash shell line continuations into one logical
    line each, so a multi-line invocation's flags aren't invisible just
    because they're not on the line containing `harvest` itself (Codex
    review, PR #1373: `docs/sharding.md`'s `harvest shard rebalance \\` /
    `--shard ... \\` examples put every flag on a continuation line)."""
    logical = []
    buf = None
    for line in lines:
        piece = line if buf is None else buf + " " + line.strip()
        stripped = piece.rstrip()
        if stripped.endswith("\\"):
            buf = stripped[:-1].rstrip()
            continue
        logical.append(piece)
        buf = None
    if buf is not None:
        logical.append(buf)
    return logical


def scan_doc(path: Path, real_config_paths, real_env_vars, real_flags):
    text = path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()
    _masked, blocks = mask_fenced(lines)

    hits = []  # (kind, value)

    # Fenced-block scans: TOML `[section]` + `key = value`, and
    # `harvest ...` CLI invocations. Config keys are checked only inside a
    # fenced ```toml block with a `[harvest...]` section header — an earlier
    # version also matched a bare inline `` `harvest.x.y = value` `` code
    # span, but that shape collides with OTel span attributes and Prometheus
    # metric/alert names written the same way (`harvest.replay = false` in a
    # trace excerpt, `harvest.replication.standbys == 0` in an alert
    # expression) that live in a completely different namespace than the
    # `[harvest]` TOML config — every inline hit in an early version was one
    # of those, not drift.
    for lang, block_lines in blocks:
        if lang in ("toml",) or any(
            SECTION_HEADER_RE.match(l) and l.strip().lstrip("[").startswith("harvest")
            for l in block_lines
        ):
            section = ""
            for line in block_lines:
                sm = SECTION_HEADER_RE.match(line)
                if sm:
                    section = sm.group(1)
                    continue
                code = line.split("#", 1)[0]
                kv = TOML_KV_RE.match(code)
                if not kv:
                    continue
                key_part = kv.group(1)
                full = f"{section}.{key_part}" if section else key_part
                if not full.startswith("harvest"):
                    continue
                if full not in real_config_paths:
                    hits.append(("config-key", full))

        if lang in ("bash", "sh", "shell", "console", "", "text"):
            for line in join_shell_continuations(block_lines):
                cm = HARVEST_CMD_LINE_RE.search(line)
                if not cm or line.strip().startswith("#"):
                    continue
                # Don't chase a `harvest` mention inside a curl URL/string —
                # only lines that actually invoke the binary as a command.
                before = line[: cm.start()]
                if "curl" in before or "http://" in line or "https://" in line:
                    continue
                for fm in FLAG_TOKEN_RE.finditer(cm.group(1)):
                    flag = fm.group(1)
                    if flag not in real_flags:
                        hits.append(("cli-flag", flag))

    # 2. Env-var tokens, anywhere in the page (code or prose — the token
    # shape itself is the signal).
    for m in ENV_TOKEN_RE.finditer(text):
        var = m.group(1)
        if var not in real_env_vars:
            hits.append(("env-var", var))

    # Dedupe, preserve first-seen order.
    seen = set()
    deduped = []
    for kind, value in hits:
        key = (kind, value)
        if key in seen:
            continue
        seen.add(key)
        deduped.append((kind, value))
    return deduped


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    config_paths, config_env_vars = extract_config_ground_truth()
    cli_flags, cli_env_vars = extract_cli_ground_truth()
    real_env_vars = config_env_vars | cli_env_vars

    files = sorted(REPO_ROOT.glob("docs/**/*.md"))

    # Same corpus/process-artifact split corpus-link-check.py uses (a
    # process-artifact page IS still graded corpus if a real corpus page
    # actually links to it) — computed over that script's full file set
    # (docs/**/*.md plus top-level sources like README.md, which participate
    # as link SOURCES even though this script never scans them for drift).
    reachable = compute_corpus_reachable(_link_check.corpus_files())

    def is_process_artifact(p: Path) -> bool:
        return p not in reachable

    corpus_hits = []
    process_hits = []
    for f in files:
        for kind, value in scan_doc(f, config_paths, real_env_vars, cli_flags):
            target = process_hits if is_process_artifact(f) else corpus_hits
            target.append((f, kind, value))

    if args.json:
        print(
            json.dumps(
                {
                    "files_scanned": len(files),
                    "real_config_keys": sorted(config_paths),
                    "real_env_vars": sorted(real_env_vars),
                    "real_cli_flags": sorted(cli_flags),
                    "drift_corpus": [
                        {"source": rel(s), "kind": k, "value": v}
                        for s, k, v in corpus_hits
                    ],
                    "drift_process_artifacts": [
                        {"source": rel(s), "kind": k, "value": v}
                        for s, k, v in process_hits
                    ],
                },
                indent=2,
            )
        )
    else:
        print(
            f"Folio config/CLI drift scan — {len(files)} files scanned "
            f"({len(config_paths)} config keys, {len(real_env_vars)} env vars, "
            f"{len(cli_flags)} CLI flags known)\n"
        )
        print(f"Drift (corpus, fails CI): {len(corpus_hits)}")
        for s, k, v in corpus_hits:
            print(f"  {rel(s)}: {k} `{v}` — not found in current source")
        print(f"\nDrift (process artifacts, reported only): {len(process_hits)}")
        for s, k, v in process_hits:
            print(f"  {rel(s)}: {k} `{v}` — not found in current source")

    return 1 if corpus_hits else 0


if __name__ == "__main__":
    sys.exit(main())
