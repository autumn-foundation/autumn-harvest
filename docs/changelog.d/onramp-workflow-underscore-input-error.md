## Phase X.Y — `#[workflow]` names the fix for a `_` input parameter

`#[workflow]` now rejects a `_` or destructuring input parameter with a
compile error that names the cause and the fix (`_input: ()`). Before, the
macro dropped the pattern and rustc reported `E0061` "takes 5 arguments but 4
arguments were supplied" on the attribute line. That input already failed to
compile, so no working code changes.

Two getting-started snippets used `_: ()` (chapter 8 `incremental_etl`,
chapter 11 `billing_cycle`). Both now use `_input: ()`. The chapter 11 snippet
also passed a `Duration` to `ctx.timer`, which takes whole seconds.

Test evidence: `scripts/check-underscore-input-snippets.sh` (6 compile errors
to 0, wired into CI) and the `workflow_underscore_input` trybuild fixture.
