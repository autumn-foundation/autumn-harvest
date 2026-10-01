## Refactor — one shared handler-dispatch builder (issue #1632)

The `#[query]`, `#[update]`, `#[workflow]`, and `#[activity]` macros each
copied one block. The block decodes the handler arguments by arity (0, 1, or
many), calls the handler, and encodes the result as JSON.

`attr_util::build_handler_dispatch` now builds that block. Each macro passes its
own call shape: the context expression, `.await` or nothing, and the error
encoder.

One output changes. The many-parameter decode binding is now `__args` at all
four sites. Before, `#[workflow]` and `#[activity]` used `args`, so a handler
parameter named `args` shadowed the binding and did not compile.

Tests: unit tests for the builder in `attr_util.rs`. Pinned dispatch output for
every arity at all four macros. The `#[query]` and `#[update]` pins are new.
