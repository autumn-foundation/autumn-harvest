## Fix — `WorkflowCancelRequested` in the payload slot matches

`trunk-dev` did not compile. Issue #1984 added `WorkflowCancelRequested`, and
the exhaustive payload slot matches in `event.rs` did not name it. The event
has no payload field, so both matches now return `None` for it.
