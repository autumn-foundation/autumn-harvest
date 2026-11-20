# DESIGN-2762: keep the tracing callsite cache stable in the plugin tests

Issue #2762. The test `stream_logs_hold_no_key_scope_input_or_chunk`
failed once in CI. The `harvest.durable_route` span went disabled in the
middle of the test. The events of the test stayed enabled.

## 1. Facts

tracing-core 0.1.36 is the latest release. It caches an `Interest` for
each callsite. A rebuild sets that cache for all callsites. The flag
`has_just_one` selects how a rebuild finds its dispatchers:

- With the flag off, a rebuild holds a lock on the list of live
  dispatchers. Each new dispatcher takes the write lock, so the rebuilds
  occur one at a time.
- With the flag on, a rebuild asks only the default dispatcher of its own
  thread. It holds no lock.

The flag is on at process start. A new dispatcher sets it again from the
count of live dispatchers. So the flag stays on until two dispatchers are
live at one time.

The race:

1. Thread X has no dispatcher. It hits the span callsite first. The flag
   is on, so X asks only the no-op dispatcher. That answer is `never`.
2. Before X stores `never`, the test thread makes its capture
   dispatcher. That rebuild sets the callsite to `sometimes`.
3. The first call of the test creates the span.
4. X stores `never`. The test cannot see the span again.

`capture()` makes a keeper dispatcher, `KEEP`, to turn the flag off. It
makes `KEEP` lazily, in the first log test. That is too late. A thread
can read the flag before `KEEP` exists and store after it.

## 2. Brainstorming

The chosen option is in bold.

| Option | Result |
|---|---|
| A keeper that answers `sometimes`, with no level hint (the issue) | No effect. With the flag on, a rebuild does not ask the keeper. |
| Rebuild the cache before each call of a test | The window gets smaller. It stays open. |
| Retry a test when its span is missing | Hides the race. Each new log test must do it again. |
| One lock for all log tests | Thread X runs a test that captures no logs. The lock does not stop X. |
| Run each log test in a child process | Slow, and a test with no capture can still race. |
| A fix in tracing-core | Correct, but outside this repository. It takes time. |
| **Two keepers, made by a constructor before `main` of the test binary** | **The flag is off before any test runs. No rebuild runs without the lock.** |

## 3. Reverse brainstorming

"How can a log test lose its span?" Each answer gives a rule.

| Failure | Rule |
|---|---|
| The keeper comes after the first callsite hit. | A constructor makes the keepers before `main`. |
| Only one dispatcher is live, so the flag comes on again. | Two keepers live for the whole process. |
| A keeper changes what a test sees. | Each keeper is a `NoSubscriber`. It answers `never`, so it adds no interest. |
| The race is too rare to catch in CI. | A test holds thread X inside its first registration, so the race occurs each time. |
| A later change removes the keepers. | That test fails when it runs alone. |

## 4. Six thinking hats

- **White (facts).** Section 1. The release that CI uses has the race.
- **Red (feelings).** A retry or a sleep feels like a mask. A
  constructor before `main` feels unusual, but it removes the cause.
- **Black (risks).** Code before `main` must not need the runtime of
  `std`. `Dispatch::new` needs only the allocator, an `Arc` and a
  `RwLock`. The `ctor` crate is a new dev-dependency. It has no other
  dependencies outside its own project, and it is MIT or Apache-2.0.
- **Yellow (benefits).** Each log capture test in the plugin library gets
  the fix: `durable_route`, `process_role`, `mcp_unserved` and `plugin`.
- **Green (ideas).** A test subscriber can block in `register_callsite`.
  That holds thread X at step 1, so the race occurs each time.
- **Blue (process).** Red test first, then the keepers, then remove
  `KEEP` from `capture()`.

## 5. Decision

- `autumn-harvest-plugin/src/trace_keepers.rs` is a test module. A
  `#[ctor]` function makes two `NoSubscriber` dispatchers. A `static`
  keeps them for the whole process.
- `capture()` in `durable_route.rs` loses its `KEEP`.
- Production code does not change. An application sets one global
  dispatcher, so the fast path asks the right dispatcher.

## 6. Acceptance

| Line of the issue | Evidence |
|---|---|
| The span goes disabled in the middle of a test. | `a_late_first_registration_cannot_disable_a_live_capture` fails without the keepers when it runs alone, and passes with them. |
| The keeper of `capture()` does not stop this case. | `KEEP` is gone. The constructor makes the keepers. |
| The first idea of the issue. | Section 2 records why a `sometimes` keeper has no effect. |
