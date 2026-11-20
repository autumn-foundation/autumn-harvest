# DESIGN-2762: keep the tracing callsite cache stable in the plugin tests

Issue #2762. The test `stream_logs_hold_no_key_scope_input_or_chunk`
failed once in CI. The `harvest.durable_route` span went disabled in the
middle of the test. The events of the test stayed enabled.

## 1. Facts

The workspace locks tracing-core 0.1.36, the latest release on
2026-11-20. It caches an `Interest` for each callsite. Two paths set that
cache:

- A new dispatcher rebuilds every callsite under the write lock of the
  list of live dispatchers.
- The first hit of a callsite registers that callsite.

The flag `has_just_one` selects how a registration finds its dispatchers:

- With the flag off, it holds the read lock and asks each live
  dispatcher. So a registration and a rebuild never overlap.
- With the flag on, it asks only the default dispatcher of its own
  thread. It holds no lock.

The flag is on at process start. Only a new dispatcher changes it. The
flag goes off when that dispatcher finds another live dispatcher, and it
goes on when it finds none.

The race:

1. Thread X hits the span callsite first. The flag is on, so X asks only
   its own dispatcher. That is the no-op dispatcher, or a capture at
   `WARN`, as in `mcp_unserved`. Either answers `never` for an `INFO`
   span.
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
| One keeper registers alone, so the flag stays on until the first capture. | The second keeper finds the first one live, so the flag goes off before `main`. |
| A keeper changes what a test sees. | Each keeper is a `NoSubscriber`. It answers `never` and enables nothing. It has no level hint, so the max level stays at TRACE. A test sees the same spans and events. |
| The regression test slows the other tests. | Its gate holds for 250 ms at most. Each new dispatcher waits for that time once. |
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
  The binaries in `autumn-harvest-plugin/tests/` do not link the module.
  None of them captures logs today.
- **Green (ideas).** A test subscriber can block in `register_callsite`.
  That holds thread X at step 1, so the race occurs each time.
- **Blue (process).** Red test first, then the keepers, then remove
  `KEEP` from `capture()`.

## 5. Decision

- `autumn-harvest-plugin/src/trace_keepers.rs` is a test module. A
  `#[ctor]` function makes two `NoSubscriber` dispatchers. A `static`
  keeps them for the whole process.
- `capture()` in `durable_route.rs` loses its `KEEP`.
- Production code does not change. With one global dispatcher, the fast
  path on each thread asks that dispatcher. The race needs a thread whose
  default is not the only live dispatcher. That is an upstream defect.

## 6. Acceptance

| Line of the issue | Evidence |
|---|---|
| The span goes disabled in the middle of a test. | `a_late_first_registration_cannot_disable_a_live_capture` fails without the keepers when it runs alone, and passes with them. |
| The keeper of `capture()` does not stop this case. | `KEEP` is gone. The constructor makes the keepers. |
| The first idea of the issue. | Section 2 records why a `sometimes` keeper has no effect. |
