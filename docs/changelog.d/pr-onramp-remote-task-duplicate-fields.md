## Onramp — fix the engine build on `trunk-dev`

`remote_task::activities` set `input_schema` and `output_schema` twice in one
struct literal. Two merged PRs each added the fields. Git merged them cleanly,
and `autumn-harvest` failed with E0062. Every clean-room `--checkout` run
and every build from `trunk-dev` stopped there. This change removes the second
pair.
