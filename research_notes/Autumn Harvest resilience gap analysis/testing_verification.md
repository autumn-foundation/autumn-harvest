# Testing and Verification of Resilient Distributed Systems: State of the Art and Autumn Harvest Gap Analysis

Scope: testing and verification only. Current as of 2026-09-30. Codebase facts come from `/home/user/autumn-harvest` at HEAD `937b6553` (committed 2026-09-29). They also draw on read-only GitHub Actions and PR data for `autumn-foundation/autumn-harvest`. Codebase citations use `path:line`, relative to the repository root. Status labels used throughout:

- **absent**: no code or test exists.
- **present but not wired**: the code exists but CI does not execute it.
- **present but untested**: the code runs, but no test targets the property.
- **present and tested**: CI executes a test that targets the property.

## Q1. How do FoundationDB, TigerBeetle, Antithesis, Resonate, S2, Polar Signals and the Rust tools (Turmoil, madsim, Shuttle) do deterministic simulation testing (DST), and what does DST catch that integration tests miss?

### Takeaway
Industrial DST takes control of all four sources of nondeterminism: scheduling, time, randomness and I/O. It then runs the real system code under seeded fault injection, at far more than real-time speed and at large scale. Any failure replays exactly from its seed. There are three ways to get that control:

- **Build-your-own runtime:** FoundationDB with Flow, TigerBeetle, and Polar Signals with state machines.
- **Swap in a simulated runtime:** madsim, Turmoil, and S2's mad-turmoil.
- **Run unmodified containers in a deterministic hypervisor:** Antithesis. This is the only route shown to include a real, unmodified Postgres (WarpStream).

DST finds rare interleavings of faults and timing that ordinary integration tests cannot reach or reproduce. WarpStream reports a bug that tens of thousands of CI hours had missed, which Antithesis found in 233 seconds.

### Cited Findings
- **FoundationDB, simulation scope.** FDB runs a "deterministic simulation of an entire FoundationDB cluster within a single-threaded process". It simulates drives (performance, space, a full disk), the network and time, at about a 10:1 real-to-simulated factor. — [FDB Testing docs](https://apple.github.io/foundationdb/testing.html)
- **FoundationDB, scale and faults.** The team runs "tens of thousands of simulations every night" and estimates "roughly one trillion CPU-hours of simulation". Injected faults include connection failures, machine degradation, and shutdowns or reboots. The "swizzle-clogging" test stops the network connections of random nodes and restores them in random order. It "seems to be particularly good at finding deep issues that only happen in the rarest real-world cases." — [FDB Testing docs](https://apple.github.io/foundationdb/testing.html)
- **TigerBeetle VOPR.** The VOPR runs a real cluster under "network, storage and process faults" at "1000x speed", "24/7 on 1024 cores". It finds "both logical errors in the algorithms and coding bugs." — [TigerBeetle Safety docs](https://docs.tigerbeetle.com/concepts/safety/)
- **S2, the four controls (2025-04-02).** S2 controls "Execution – Single-threaded…; Entropy – All RNGs should have a known seed; Time – No physical clocks; I/O – No dependency on any external IO."
  - The `mad-turmoil` crate overrides `getrandom`/`getentropy` and `clock_gettime`.
  - External services (object storage, metadata) are replaced by in-memory emulators that run as hosts on the simulated network.
  - Determinism check: S2 "reruns the same seed, and compares TRACE-level logs."
  - Cadence: on every PR, on every commit, and in thousands of nightly trials. At least 17 notable bugs found before production.
  — [S2 blog](https://s2.dev/blog/dst)
- **Polar Signals (2025-07-08).** Polar Signals chose single-threaded state machines that talk over a message bus. The bus controls scheduling, time (`tick()`), a seeded PRNG and message-level failure injection. DST found "one causing data loss and another causing data duplication". The team admits "considerable cognitive overhead". — [Polar Signals blog](https://www.polarsignals.com/blog/posts/2025/07/08/dst-rust)
- **Resonate, a durable-execution engine (the closest peer to Harvest).** Resonate uses three layers:
  1. An executable Lean 4 protocol specification with 95 decidable properties.
  2. Differential testing of the server against an in-memory oracle across SQLite, Postgres and MySQL backends, with Porcupine linearizability checking.
  3. DST of the SDK, which controls "whether a message is dropped, duplicated, or delayed" plus worker failures. "CI runs each seed twice and diffs the two logs."
  — [Resonate docs: How Resonate is tested](https://docs.resonatehq.io/evaluate/how-resonate-is-tested)
- **Antithesis.** Antithesis combines fuzzing with injected faults ("network partitions or node kills") inside a deterministic hypervisor, so "every bug we find is perfectly reproducible". It branches a "multiverse" of timelines and checks assertions such as "should always recover after a single node dies". — [Antithesis docs](https://antithesis.com/docs/introduction/how_antithesis_works/)
- **WarpStream on Antithesis (March 2024).** WarpStream ran its whole SaaS in a docker-compose file under Antithesis, **including a real PostgreSQL container** and localstack S3. Results:
  - A data race present since the project's first month was found in 233 seconds, after "10s of thousands of hours" of CI had missed it.
  - A data-loss race needed a sub-microsecond interleaving.
  - 6 wall-clock hours simulated 280 application hours.
  — [WarpStream blog](https://www.warpstream.com/blog/deterministic-simulation-testing-for-our-entire-saas)
- **Turmoil.** Turmoil runs many hosts in one thread and injects network "hardship" (latency, crashes) from a seeded RNG. — [S2 blog](https://s2.dev/blog/dst)
- **madsim.** madsim replaces tokio (and tonic, etcd-client, rdkafka, aws-sdk-s3) under `RUSTFLAGS="--cfg madsim"`. It patches the clock and `getrandom`, and it lists a patched `tokio-postgres`. It is "borrowed from FoundationDB", and RisingWave uses it. — [madsim README](https://github.com/madsim-rs/madsim)
- **Shuttle.** Shuttle is randomized concurrency testing (PCT scheduler) with tokio wrappers. It "is not sound (a passing Shuttle test does not prove the code is correct), but it scales to much larger test cases than Loom." — [awslabs/shuttle](https://github.com/awslabs/shuttle)
- **AWS.** AWS describes deterministic simulation as "a lightweight method widely used at AWS". Each system runs "on a single-threaded simulator with control over all sources of randomness, such as thread scheduling, timing, and message delivery order". — [Systems Correctness Practices at AWS, CACM 68(6), June 2025](https://dl.acm.org/doi/10.1145/3729175) (via search-result abstract)

### Inferences
- Harvest resembles WarpStream (a real Postgres is the coordination substrate) and Resonate (a durable execution engine). Harvest cannot reuse Turmoil or madsim as-is, because neither simulates a Postgres *server*.
- Two credible routes exist. One is a hypervisor-level approach (Antithesis) that runs real Postgres. The other is a Resonate-style split: an in-memory oracle store for the engine DST, plus differential testing that proves the real Postgres store matches the oracle.

### Gaps
- Will Wilson's talks and Phil Eaton's DST notes were not fetched. The FDB claims rely on the official docs only.
- The Resonate server's own DST scope (as opposed to the SDK's) is unclear from the fetched page.

## Q2. How do AWS, Microsoft and Datadog use formal methods, and what is the current state of Rust verifiers (Verus, Kani, Prusti, Creusot)?

### Takeaway
Formal methods in industry are a portfolio, not only proofs:

- design-level model checking (TLA+, P);
- runtime conformance checking of the code against the model (PObserve);
- "lightweight formal methods", meaning executable reference models plus property-based testing and stateless model checking (ShardStore);
- proofs only for small, high-value kernels (Dafny for Cedar, Kani for Firecracker).

For Rust, Kani (bounded model checking) is the production-proven tool. Verus (SMT-based) is maturing quickly, but it covers a chosen subset of Rust. No fetched source shows either tool verifying async, database-backed code of the kind that makes up most of Harvest.

### Cited Findings
- **AWS, CACM 2015 (older source).** "testing is fundamentally inadequate for systems that are required to tolerate faults because fault-tolerant systems must tolerate unusual combinations of faults." TLA+ found a DynamoDB bug that needed "a 35-step sequence of events that would violate the consistency guarantees." — [Newcombe et al., How AWS uses formal methods (PDF)](https://lamport.azurewebsites.net/tla/formal-methods-amazon.pdf)
- **AWS, CACM June 2025: the portfolio.**
  - TLA+.
  - P: models distributed systems as communicating state machines and was used for S3's move to strong read-after-write consistency. Adopted across S3, DynamoDB, EBS, Aurora and EC2 since 2019.
  - Dafny: proves the Cedar authorization engine.
  - Kani: checks Firecracker's security boundaries.
  - Fault Injection Service, property-based testing, deterministic simulation and continuous fuzzing.
  - Quote: "We could identify and eliminate subtle bugs early in development—bugs that would have eluded traditional approaches such as testing."
  — [CACM 2025 (ACM DL)](https://dl.acm.org/doi/10.1145/3729175); [summary by B. Calza](https://brunocalza.me/microblog/2025/06/13/systems-correctness-practices-at-aws.html)
- **AWS ShardStore (SOSP 2021, older source).** "Lightweight formal methods": executable reference models serve as specifications, and correctness is split into properties, each checked with the best-suited tool. "Our work has prevented 16 issues from reaching production, including subtle crash consistency and concurrency problems." — [Amazon Science](https://www.amazon.science/publications/using-lightweight-formal-methods-to-validate-a-key-value-storage-node-in-amazon-s3)
- **Datadog Courier (2024-11-20), a message queue that powers Workflow Automation.** A TLA+ model of Broker, Sender and Receiver passed over 5,515,710 distinct states. Adding a sequencer then exposed a failure in which receivers terminate before delivery. SimPy discrete-event simulations predicted hotspotting. Chaos testing in staging confirmed the simulation. Datadog had earlier used TLA+ for Husky idempotency and for a replication bug in a distributed cron scheduler. — [Datadog engineering blog](https://www.datadoghq.com/blog/engineering/formal-modeling-and-simulation/)
- **Kani (arXiv, 2026-07-01).** Kani compiles MIR to CBMC for bit-precise bounded model checking. It supports function and loop contracts and stubbing. There are "over 16,000 harnesses verified per code change" in the Rust standard-library campaign and "six previously unknown bugs" in industrial case studies. — [arXiv 2607.01504](https://arxiv.org/abs/2607.01504)
- **verify-rust-std.** The campaign started in November 2024 and now runs Kani, ESBMC, VeriFast and Flux in CI. Kani was used for 6 of 9 completed challenges. 29 challenges had been published by March 2026. — [search summary of verify-rust-std sources](https://github.com/model-checking/verify-rust-std/blob/main/README.md)
- **Verus.** "Verification is static: Verus adds no run-time checks". It uses SMT (Z3) plus linear type checking for memory and aliasing. The project states "we do not intend to support all Rust features and libraries". — [Verus guide](https://verus-lang.github.io/verus/guide/)
  - The Verus paper appeared at SOSP 2024. — [Verus paper PDF](https://www.cs.utexas.edu/~hleblanc/pdfs/verus.pdf)
  - The 2025–2026 research wave centres on LLM proof generation: VeruSAGE-Bench has 849 proof tasks drawn from eight Verus-verified systems, and KVerus verified modules of the Asterinas OS kernel. — [VeruSAGE](https://arxiv.org/html/2512.18436v2); [KVerus](https://arxiv.org/html/2605.03822v1)

### Inferences
- A "spec-first with Verus proofs" strategy fits Harvest's *pure* kernels: the retry-delay math, the `splitmix64` plan derivation, failure-signature normalisation, lifecycle-transition tables and codec envelope parsing.
- The same strategy does not fit Harvest's resilience-critical logic. That logic is SQL predicates and transaction boundaries inside async Diesel code. For that, the AWS and Datadog evidence points to TLA+ or P models of the protocol, followed by conformance checking or DST against the code.

### Gaps
- No primary source was fetched for Microsoft's use of P (Windows USB stack, Azure) or for Microsoft's current practice. One search snippet wrongly credits P's origin to AWS. Treat that as unverified.
- The current status of Prusti and Creusot was not researched (no fetched sources).
- It is not confirmed whether Verus supports async/await today. The fetched guide page is silent on it.

## Q3. What has Jepsen found in queues and databases relevant to a workflow engine, and how do Netflix, Gremlin and LitmusChaos frame chaos engineering?

### Takeaway
Jepsen repeatedly finds lost acknowledged writes and split-brain in queues and stream stores (NATS JetStream 2025, Bufstream). It also found serializability violations in PostgreSQL itself (12.3, 2020). The method is history-based: record client operations under faults, then check them with Elle or Knossos-style checkers. Chaos engineering, as defined in the Principles of Chaos, means continuous experiments against a steady-state hypothesis in production, with minimal blast radius. Harvest's opt-in liveness canary is a steady-state signal, but no chaos experiments consume it.

### Cited Findings
- **NATS 2.12.1 (2025-12-08).**
  - JetStream "lost writes if data files were truncated or corrupted on a minority of nodes".
  - Coordinated power failures, or one OS crash combined with network delays, caused lost committed writes and persistent split-brain.
  - JetStream acknowledges before fsync (it flushes every two minutes by default).
  — [Jepsen: NATS 2.12.1](https://jepsen.io/analyses/nats-2.12.1)
- **Bufstream 0.1.0.** Three safety issues and two liveness issues, including lost acknowledged writes in healthy clusters. The same work found Kafka transaction-protocol issues: write loss, aborted read and torn transactions. — [Jepsen (search summary)](https://jepsen.io/blog)
- **PostgreSQL 12.3 (2020, older source).** Elle found G2-item anomalies under SERIALIZABLE: six cases in one two-minute run. The bug had existed since SSI shipped in 2011. PostgreSQL's REPEATABLE READ is snapshot isolation and showed G2-item frequently. — [Jepsen: PostgreSQL 12.3](https://jepsen.io/analyses/postgresql-12.3)
- **Principles of Chaos.** "Chaos Engineering is the discipline of experimenting on a system in order to build confidence in the system's capability to withstand turbulent conditions in production." The advanced principles are: a steady-state hypothesis on measurable output, varied real-world events, running in production, automating to run continuously, and minimising blast radius. — [principlesofchaos.org](https://principlesofchaos.org/)

### Inferences
- Harvest's correctness depends on Postgres isolation semantics: `FOR UPDATE SKIP LOCKED`, CAS updates, `ON CONFLICT`. The PG 12.3 result shows that the database layer itself deserves history-checked tests, at least on the supported Postgres versions.
- CI still pulls `postgres:11-alpine` for some suites (`.github/workflows/ci.yml:1300`, `.github/workflows/ci.yml:1311`), alongside `postgres:16`.

### Gaps
- No Jepsen analysis of Temporal, Restate, Inngest or another workflow engine was found. Treat this as absence of evidence, not evidence of absence.
- No primary Gremlin or LitmusChaos sources were fetched.

## Q4. What does Autumn Harvest actually test for resilience, and what does CI actually run?

### Takeaway
Harvest has a large, Postgres-backed integration suite and several distinctive determinism tools: static and MIR-level determinism analysis, a replay-drift gate and a test generator. It has **no whole-engine deterministic simulation**. Its "chaos" harness is deterministic in its *plan* but not in its *interleaving*.

The most important finding is that the advanced resilience checks are mostly **present but not wired**:

- **Chaos workflow: has never parsed.** `chaos.yml` has a YAML error, so its nightly job has never run (0 schedule runs).
- **Loom workflow: never run.** It is manual-only, with 0 runs ever.
- **Fuzzing: manual-only.**
- **DB suites: about 60 never run.** About 60 DB-gated integration suites are allowlisted as "not yet wired" and never execute in CI.

There are no formal specifications or proofs (no `verus!`, no `#[kani::proof]`, no `.tla`), and no coverage measurement.

### Cited Findings

**Overall scale**

- 4,299 `#[test]`/`#[tokio::test]` attributes in `autumn-harvest/tests/integration/*.rs`, and 3,928 in `autumn-harvest/src/*.rs` (grep count; this counts attributes, not executed tests). — `autumn-harvest/tests/integration/`, `autumn-harvest/src/`

**CI with real Postgres — present and tested**

- The `test-db-linux` job runs `.github/ci/run-suites.sh run linux` over a 21-shard matrix. Postgres comes from testcontainers, with images pre-pulled. — `.github/workflows/ci.yml:1117`, `.github/workflows/ci.yml:1311`, `.github/workflows/ci.yml:1343`
- The manifest is `.github/ci/integration-suites.txt` (249 lines). About 117 core `integration` rows exist. Some suites are re-run against the partitioned `harvest_events` layout (`linuxpart`), for example `integration_e2e`. — `.github/ci/integration-suites.txt:106`, `.github/ci/integration-suites.txt:244`
- CI triggers are push/PR to `trunk` and `trunk-dev`, plus `workflow_dispatch`. — `.github/workflows/ci.yml:17-27`

**DB suites that never run — present but not wired**

- `ci_run_coverage.rs` exists to catch "silently-never-run DB test[s]". It cites `workflow_retry_tests`, where 6 of 9 tests never ran and 3 real bugs hid as a result. — `autumn-harvest/tests/integration/ci_run_coverage.rs:1-9`
- Its `ALLOWLIST` holds 64 entries (36 core, 28 plugin). About 60 carry the "not yet wired … test-coverage debt" reasons, with a soft cap of 75. — `autumn-harvest/tests/integration/ci_run_coverage.rs:183-185`, `autumn-harvest/tests/integration/ci_run_coverage.rs:210-314`, `autumn-harvest/tests/integration/ci_run_coverage.rs:315`
- Resilience-relevant suites that never run in CI:
  - `poison_pill_tests` (`:227`)
  - `scheduler_ha_tests` (`:240`)
  - `signal_tests` (`:241`)
  - `cross_workflow_signal_tests` (`:222`)
  - `cross_workflow_cancel_tests` (`:221`)
  - `transactional_activity_tests`
  - `replayer_integration_tests` and `replay_canary_tests` (`:229-230`)
  - plugin `erase_payloads_integration` (`:260`), which exercises the PII-erasure writer, sanctioned exception #1

**Chaos harness (issue #940)**

- **Design.** The harness injects faults at named points. A seeded plan uses `splitmix64(seed ^ fnv1a(point))`. In a non-`chaos` build it compiles to nothing. — `autumn-harvest/src/chaos.rs:1-38`
- **Catalogue.** There are 9 points (`chaos.rs:130-211`), but only 8 production call sites were found:
  - `queue.rs:4659`
  - `worker.rs:3868`, `worker.rs:22284`, `worker.rs:22347`, `worker.rs:29294`
  - `scheduler.rs:3284`, `scheduler.rs:4769`
  - `poison_pill.rs:901`
  - `NOTIFY_TASK_ENQUEUED` (`chaos.rs:198`) had no `chaos_point!` site in `src` under this grep.
- **Fault primitives.** KILL is a panic inside a spawned tokio task. The others are an injected error, a dropped NOTIFY and a delay. — `autumn-harvest/src/chaos.rs:61-70`
- **Tests.** `chaos_tests.rs` contains 7 targeted reproducers and one seeded convergence sweep:
  - `chaos_tests.rs:386`: #601 lost wake.
  - `chaos_tests.rs:499`: #367 crash orphan reclaimed.
  - `chaos_tests.rs:652`: #1348 terminal metrics survive post-commit cancellation.
  - `chaos_tests.rs:771`: #492 outbox cannot double-deliver.
  - `chaos_tests.rs:934`: #350 crashed fire-claim re-fired exactly once.
  - `chaos_tests.rs:1061`: #350 post-start crash dedupes to exactly one.
  - `chaos_tests.rs:1206`: session lease expiry.
  - `chaos_tests.rs:1432`: the sweep.
- **Sweep scope.** The sweep has a 6-workflow no-op workload and 7 default seeds (`chaos_tests.rs:1330`). It targets only `WORKER_PERSIST_BEFORE_COMMIT` (`chaos_tests.rs:1327`).
- **Convergence oracle.** All executions COMPLETED, no RUNNING task on a dead worker, and no dangling external-signal requests. — `chaos_tests.rs:1571-1613`
- **Runtime.** The suite uses a real multi-thread tokio runtime with 4 workers and a real Postgres (`chaos_tests.rs:1425`). So the seed fixes the fault *plan*, not the thread or transaction interleaving.
- **Stated out of scope.** "network-partition / Jepsen / Antithesis-style testing". — `docs/testing/chaos.md:246-249`
- **The integration suite is not wired (never ran).** It is gated `#[cfg(feature = "chaos")]` (`autumn-harvest/tests/integration/mod.rs:54-55`) and allowlisted on the grounds that it "DOES run in CI" via `chaos.yml` (`ci_run_coverage.rs:199-202`). But `chaos.yml` does not parse:
  - `python3 yaml.safe_load` raises `ScannerError: mapping values are not allowed here … line 57, column 72`. The unquoted `run:` scalars contain `chaos:: ` at `.github/workflows/chaos.yml:57` and `.github/workflows/chaos.yml:63`.
  - GitHub Actions shows every recent `chaos.yml` run as a `push`-event failure, although the file has no push trigger. The `schedule`-event run count is **0**.
  - Open PR #1773 (2026-09-29, unmerged) confirms that "the nightly chaos suite (issue #940) never ran" since the file landed in #1711. The PR also says the suite itself was not verified to compile or pass.
  — [PR #1773](https://github.com/autumn-foundation/autumn-harvest/pull/1773)
- **Harness unit tests: present and tested.** The 20 no-DB lib tests in `chaos.rs` do run in normal CI, via `cargo test -p autumn-harvest --all-features --lib`. — `.github/workflows/ci.yml:875`

**Simulators (component-level only)**

- `WorkflowSimulator` is "a fast, in-memory execution environment for workflows that doesn't require Postgres or worker pools". It runs one workflow function with mocked activities. It has no clock, network, DB or scheduler faults. — `autumn-harvest/src/simulator.rs:1-5`, `autumn-harvest/src/simulator.rs:51-56`
- `DagSimulator` checks DAG trigger rules and ordering in memory. — `autumn-harvest/src/dag_simulator.rs:1-5`
- `WorkflowTestEnv` is a user-facing test environment with mocked activities and a virtual clock that advances with durable timers. — `autumn-harvest/src/testing.rs:5159-5167`, `autumn-harvest/src/testing.rs:5306`
- These are Temporal-style SDK test aids, not engine DST.

**Tool evaluation: DST rejected**

- The project's evaluation says: "the large majority of harvest's concurrency is coordinated through Postgres … None of these three tools can model a Postgres server." — `docs/testing/concurrency-model-checking.md:7-14`
- Turmoil was rejected as a poor fit. — `docs/testing/concurrency-model-checking.md:107-130`
- Shuttle is "backlogged, not shipped". — `docs/testing/concurrency-model-checking.md:66`

**Loom**

- The loom shim routes only `circuit_breaker` and `sessions`. — `autumn-harvest/src/loom_sync.rs:14-24`
- There are 4 models, each with two threads doing at most two operations. — `autumn-harvest/tests/loom_models.rs:32-36`, `:89`, `:138`, `:212`, `:253`
- The loom file itself says "The large majority of harvest's concurrency lives in Postgres … entirely out of loom's reach". — `autumn-harvest/tests/loom_models.rs:25-30`
- `loom.yml` is `workflow_dispatch`-only (`.github/workflows/loom.yml:1-18`). GitHub Actions shows **0 runs ever**. Status: present but not wired.

**Property tests**

- 8 proptest suites cover pure functions only: retry policy, DLQ signature, fairness, task duration, build routing, SSRF URL validation, completion-trigger conditions and event serde. — `autumn-harvest/tests/property/*_props.rs`
- 128 cases by default, and regressions are not persisted. — `autumn-harvest/tests/property/prop_config.rs:5-28`
- They run in CI as an `allos` row with `db`. — `.github/ci/integration-suites.txt:55`
- There is no stateful or model-based property test of engine behaviour. Status: absent.

**Fuzzing**

- 4 libFuzzer targets:
  - `WorkflowEvent` JSON deserialization
  - `det_check` source scanner
  - SSRF URL validator
  - DLQ failure signature
  — `fuzz/fuzz_targets/*.rs`
- The job runs only on manual `workflow_dispatch`, for 30 s per target. — `.github/workflows/ci.yml:1846-1870`
- No target feeds a history into replay or the executor.

**Determinism tooling — present and tested (a real strength)**

- Compile-fail guardrails HVG001–HVG011 cover wall-clock, randomness, env, sleep, spawn, I/O, globals, `select!` and HashMap iteration. — `autumn-harvest/tests/compile_fail/hvg0*.rs`
- The `det_check` static scanner. — `autumn-harvest/src/det_check.rs:1-4`
- The MIR-level taint verifier `autumn-harvest-verify` runs in CI. — `.github/workflows/ci.yml:1752-1781`; `autumn-harvest-verify/README.md`
- The in-flight history sampling and replay-drift gate. — `autumn-harvest/src/replay_sample.rs:1-15`
- `test_generator` turns production histories into regression tests. — `autumn-harvest/src/test_generator.rs:1-4`

**Formal methods — absent**

- A repo-wide grep (excluding `research_notes/`) finds no `verus!`, no `kani::`, no `creusot`, no `prusti`, no `stateright`, and no `.tla`, `.p` or `.qnt` files.

**Coverage measurement — absent**

- There is no `llvm-cov`, `tarpaulin`, `grcov` or `codecov` in `.github/`, `scripts/` or the Cargo manifests.
- So the stated 85–90 % coverage goal has no measurement behind it.

**Production-side steady-state probe — present**

- The opt-in synthetic liveness canary (issue #796, PR #1119) exercises start → activity → timer → complete and emits `harvest.canary.*` metrics. — [issue #796](https://github.com/autumn-foundation/autumn-harvest/issues/796); `autumn-harvest/src/canary.rs`

**Flakiness evidence**

- The recurring `quota_enforcement_tests` hang is still unexplained after several rounds. — `.github/workflows/ci.yml:1165-1175`
- The source-completion wait hang has 8 occurrences on 6 branches, and a timeout widen did not fix it. — `docs/rnd/2026-09-23-ci-health-semaphore-source-completion-hang-escalation.md:1`

### Inferences
- Harvest's resilience evidence rests almost entirely on the per-PR Docker-Postgres integration suite plus hand-written race reproducers. The reproducers are strong regression tests for *known* bugs: #350, #367, #492, #601, #1184 and #1348. But nothing in CI searches for *unknown* fault interleavings. The one component designed to do that (the seeded sweep) has never run in CI.
- The comment in `ci_run_coverage.rs:199-202` claims the chaos suite "DOES run in CI … Not a coverage gap". That claim is false at HEAD. The guard trusts a workflow file it cannot validate.
- The recurring CI hangs are the kind of irreproducible nondeterminism that DST removes by construction.

### Gaps
- It was not possible to confirm whether the chaos suite passes once `chaos.yml` parses. PR #1773 also did not run it.
- GitHub Actions run history for the `fuzz-smoke` job was not queried.
- Build and test were not run (read-only constraint), so no test passes or fails were observed directly.

## Q5. Which critical invariants have tests or proofs, and which do not?

### Takeaway
None of the five invariants has a formal proof or a model-checked specification. Each has at least one real-Postgres integration test, and several have excellent targeted race tests: codec CAS versus erasure, terminal-write claim ownership, and replay fidelity. The main weaknesses are:

- **Append-only enforcement is a source-text grep:** no database-level guard exists.
- **Activity completion is not worker-fenced, and no test was found for a zombie completion after reclaim.**
- **The generic crash-recovery oracle (the chaos sweep) is not executed.**

### Cited Findings

**1. Append-only `harvest_events`**

- The initial migration declares an "append-only log". — `autumn-harvest/migrations/20260409000000_harvest_initial/up.sql:37`
- The guard is a unit test that greps `src/` for `harvest_events::event_data.eq(` and `UPDATE harvest_events SET event_data`, and allows only `erase.rs` and `codec_rotation.rs`. — `autumn-harvest/src/lifecycle.rs:597-614`
  - Status: **present and tested at the source-text level only.**
  - There is no DB trigger or `REVOKE UPDATE`. The only triggers found in migrations are unrelated (`migrations/20260906164501_shard_migration_legal_hold_guard/up.sql:67`).
  - The grep does not see `partition.rs:3783` (`UPDATE harvest_events SET cohort = '-infinity'` during layout disable). That is a third in-place writer, of a different column. The migration comment at `migrations/20260902131705_harvest_shard_rebalancing/up.sql:16` argues that rebalancing is "NOT a fourth exception".
  - The grep also would not catch writes through raw SQL built another way, or through another crate such as `autumn-harvest-plugin` or `autumn-harvest-sqlite`.
- The lifecycle transition table is "not enforced by a database trigger". — `autumn-harvest/src/lifecycle.rs:18-20`

**2. Exactly-once completion (workflow terminal writes)**

- `terminal_write_ownership_tests` is in CI (`.github/ci/integration-suites.txt:143`). It shows that every terminal writer makes no terminal decision when the claim has moved. — `autumn-harvest/tests/integration/terminal_write_ownership_tests.rs:269-609`
- It includes the SKIP LOCKED false-positive case #1184. — `terminal_write_ownership_tests.rs:674`
- The production guard is `claim_still_held_for_update(conn, task_id, worker_id, crash_strikes)`. — `autumn-harvest/src/worker.rs:13530`
- Status: **present and tested.**

**3. Exactly-once completion (activities)**

- `finalize_activity_completion` checks that the activity is still pending in history and that the task state is `RUNNING`, then calls `queue::complete_task`. — `autumn-harvest/src/worker.rs:13043-13083`
- `complete_task` filters only on `state = 'RUNNING'`, with no `worker_id` or attempt fence. — `autumn-harvest/src/queue.rs:2476-2505`
- Other queue paths *are* ownership-guarded on `state = 'RUNNING' AND worker_id = $2`. — `queue.rs:3701`, `queue.rs:3964`
- History-level dedupe prevents a second `ActivityCompleted`. But which attempt's output wins after a reclaim and re-dispatch depends on timing.
- No test was found that targets a zombie worker completing after its task was reclaimed (a grep for zombie, stale-worker and late-completion patterns found none in this path). Status: **present but untested** (a probable gap; hand to the execution-model researcher).

**4. Scheduler exactly-once firing**

- Reproducers `chaos_repro_350_*` exist at `chaos_tests.rs:934` and `chaos_tests.rs:1061`, but chaos never ran.
- `scheduler_ha_tests` is allowlisted and never run. — `ci_run_coverage.rs:240`
- Status: **present but not wired.**

**5. Lease fencing**

- Session lease expiry is covered by the chaos suite (`chaos_tests.rs:1206`), which is not wired.
- Region and shard generation fencing: `cross_region_dr_tests` ("a fenced stale worker cannot claim or persist") is in CI. — `autumn-harvest/tests/integration/cross_region_dr_tests.rs:6`; `.github/ci/integration-suites.txt:162`
- `mutex_tests` covers a mutex fencing token and is in CI. — `.github/ci/integration-suites.txt:112`
- Poison-pill and heartbeat reclaim: `poison_pill_tests` is never run (`ci_run_coverage.rs:227`). `auto_heartbeat_tests` covers spurious reclaim and start-to-close reclaim. — `auto_heartbeat_tests.rs:412`, `auto_heartbeat_tests.rs:624`
- Status: **mixed.**

**6. Replay determinism**

- Static and MIR analysis plus compile-fail guardrails run in CI (see Q4).
- `replay_fidelity_is_byte_identical_across_a_sweep` is at `autumn-harvest/tests/integration/codec_rotation_db_tests.rs:733`, and that suite is in CI (`.github/ci/integration-suites.txt:161`).
- `replay_drift_tests` and `replay_verifier_tests` are `testing`-gated. — `autumn-harvest/tests/integration/mod.rs:190-194`
- `replayer_integration_tests` and `replay_canary_tests` are never run. — `ci_run_coverage.rs:229-230`
- No fuzz target drives replay with arbitrary histories.
- Status: **present and tested, strongest area.**

**7. Codec-rotation CAS**

- The CAS SQL is `UPDATE harvest_events SET event_data = $1 WHERE id = $2 AND event_data = $3`. — `autumn-harvest/src/codec_rotation.rs:1084`
- Tests cover:
  - a tombstone written before the sweep (`codec_rotation_db_tests.rs:1034`);
  - the racing stale-read case `a_stale_read_can_never_overwrite_a_committed_erasure` (`codec_rotation_db_tests.rs:1091`);
  - the unresolved-row accounting (`codec_rotation_db_tests.rs:3198-3218`);
  - sweep resume from cursor (`codec_rotation_db_tests.rs:520`).
- The interleaving is simulated by mutating the row between read and write (`codec_rotation_db_tests.rs:1056`), not by exploring schedules.
- The paired erasure integration suite is never run. — `ci_run_coverage.rs:260`
- Status: **present and tested (scripted interleavings).**

**8. Crash recovery at the process, database and network level**

- A search of `autumn-harvest/tests` and `autumn-harvest-plugin/tests` for Postgres restart, container stop or pause, toxiproxy, SIGKILL or network partition found no fault-injection use.
  - `pg_terminate_backend` appears only in harness cleanup. — `claim_budget_tests.rs:1004`
  - "failover" in `cross_region_dr_tests` means a logical generation bump. — `cross_region_dr_tests.rs:365`
- Status: **absent.** The chaos KILL is an in-process tokio task panic (`chaos.rs:61-63`), not a process or database crash. There are no tests for:
  - a commit that succeeds while the client sees an error (the ambiguous commit);
  - a primary failover mid-transaction;
  - a connection partition during commit.

### Inferences
- The invariants the project names in CLAUDE.md as load-bearing (append-only, exception #1 and exception #3) rely on convention-checking tests (source grep) and scripted race tests. The CLAUDE.md phrase "proven, not asserted" refers to a single fixture-based replay test. That is evidence, not proof in the formal-methods sense.

### Gaps
- Whether `NOTIFY_TASK_ENQUEUED` has an injection site under another macro spelling was not confirmed.
- Whether any plugin-crate test exercises zombie activity completion was not exhaustively checked.

## Q6. Gap ratings and suggested remedies

### Takeaway
There is one Critical gap: the chaos and convergence suite is not executed at all. Four High gaps follow: no engine DST, about 60 never-run DB suites, no protocol-level formal models despite the spec-first stance, and no process-, database- or network-level crash tests. The cheapest large win is wiring what already exists: merge PR #1773, run loom per PR, and shrink the ALLOWLIST. The largest structural gap is the absence of any tool that *searches* interleavings of Postgres-coordinated races.

### Cited Findings (each gap: rating — evidence — remedy)

1. **CRITICAL — the chaos suite and seeded convergence sweep have never run in CI.**
   - Evidence: `chaos.yml` YAML parse error (`.github/workflows/chaos.yml:57`, `:63`); 0 schedule runs; the false "DOES run in CI" allowlist reason at `ci_run_coverage.rs:199-202`; open [PR #1773](https://github.com/autumn-foundation/autumn-harvest/pull/1773).
   - Why Critical: this is the project's only fault-injection oracle for crash convergence and the scheduler exactly-once rules (#350). Its green status is assumed, never observed.
   - Remedy:
     - Merge #1773, including its `workflow-yaml-parse.py` lint gate.
     - Make `ci_run_coverage` also assert that the chaos workflow parses and has a schedule trigger.
     - Alert when no nightly run has succeeded in 48 h.
     - Grow the sweep beyond one point and a no-op workload.
2. **HIGH — no whole-engine deterministic simulation (clock, DB, scheduler, workers).**
   - Evidence: `simulator.rs:1-5` is single-workflow only; the chaos seed controls the plan but not the interleaving (`chaos_tests.rs:1425`); the project explicitly rejected DST tooling (`docs/testing/concurrency-model-checking.md:7-14`, `:107-130`).
   - Why High: industry DST finds bugs that thousands of CI hours miss ([WarpStream](https://www.warpstream.com/blog/deterministic-simulation-testing-for-our-entire-saas); [FDB](https://apple.github.io/foundationdb/testing.html)). Harvest's bugs of record (#350, #367, #492, #601, #1184) are exactly this class.
   - Remedy, as two options:
     - (a) Antithesis-style: run the real engine plus a real Postgres in a deterministic hypervisor. WarpStream shows this works with Postgres.
     - (b) Resonate-style: a store trait with an in-memory oracle backend, used for single-threaded seeded DST of worker, scheduler and reclaimer. Add differential tests proving the Postgres store matches the oracle ([Resonate](https://docs.resonatehq.io/evaluate/how-resonate-is-tested)). `autumn-harvest-sqlite` is a possible starting point but was not assessed.
     - Run each seed twice and diff the logs ([S2](https://s2.dev/blog/dst)).
3. **HIGH — about 60 DB integration suites never execute in CI.**
   - Evidence: `ci_run_coverage.rs:210-314`, including `scheduler_ha_tests`, `poison_pill_tests`, `signal_tests`, `erase_payloads_integration` (the PII-erasure exception) and `replayer_integration_tests`.
   - Remedy: add manifest rows, starting with HA scheduler, poison pill, erasure and signals. Ratchet `ALLOWLIST_MAX_LEN` (75, at `:315`) down each release.
4. **HIGH — no formal specification or proof of any core protocol, despite the owner's stated Verus/spec-first philosophy.**
   - Evidence: zero `verus!`, `kani::`, `.tla` or `.p` files anywhere.
   - Remedy, in cost order:
     - (a) TLA+ or P models of claim → heartbeat → reclaim → completion; terminal-write ownership; codec CAS versus erase; shard-generation fencing. Datadog found a design bug this way in Courier, a workflow-automation queue ([Datadog](https://www.datadoghq.com/blog/engineering/formal-modeling-and-simulation/)).
     - (b) Kani harnesses for pure kernels: `splitmix64` plan derivation, `compute_retry_delay`, `failure_signature` bounds, `lifecycle` TRANSITIONS ([Kani](https://arxiv.org/abs/2607.01504)).
     - (c) Keep Verus for small, pure modules only. Verus targets a chosen subset of Rust ([Verus guide](https://verus-lang.github.io/verus/guide/)).
     - Add PObserve-style trace conformance ([CACM 2025](https://dl.acm.org/doi/10.1145/3729175)) so the models do not drift from the code.
5. **HIGH — no process-, database- or network-level crash-recovery tests.**
   - Evidence: Q5 item 8. The KILL fault is an in-process task panic only (`chaos.rs:61-63`).
   - Remedy: testcontainers-based faults:
     - `pg_terminate_backend` of the worker's connection between `COMMIT` send and ack;
     - Postgres container restart and pause;
     - toxiproxy latency and partition between worker and DB;
     - SIGKILL of a separate worker process.
     - In each case, assert the sweep's convergence oracle (`chaos_tests.rs:1571-1613`).
6. **MEDIUM — activity completion lacks worker/attempt fencing, and no zombie-completion test exists.**
   - Evidence: `queue.rs:2476-2505`, `worker.rs:13043-13083`.
   - Remedy: a DB test in which worker A's lease expires, worker B re-claims and completes, then worker A completes late. Assert a single `ActivityCompleted` event and a defined winner. Consider an attempt or claim-token predicate.
7. **MEDIUM — the append-only invariant is enforced only by a source grep.**
   - Evidence: `lifecycle.rs:597-614`; the unlisted `partition.rs:3783` cohort writer; no DB trigger.
   - Remedy: a Postgres `BEFORE UPDATE` trigger on `harvest_events` that rejects changes to `event_data`, `type`, `event_id` or `created_at` unless a transaction-local GUC is set by the two sanctioned paths. Add a DB test proving an ordinary UPDATE fails.
8. **MEDIUM — no coverage measurement against the stated 85–90 % goal.**
   - Evidence: no coverage tooling anywhere in CI.
   - Remedy: nightly `cargo llvm-cov` over the DB shards. Publish per-module line and branch coverage, and gate on the critical modules: `queue.rs`, `worker.rs`, `scheduler.rs`, `poison_pill.rs`, `codec_rotation.rs`, `erase.rs`.
9. **MEDIUM — no Jepsen-style history checking of client-visible guarantees.**
   - Guarantees at stake: start idempotency, signal delivery, update-with-start, exactly-once schedule fires.
   - Remedy: record concurrent client operation histories under faults and check them with an Elle or Porcupine-style checker. Resonate already uses Porcupine ([Resonate](https://docs.resonatehq.io/evaluate/how-resonate-is-tested)). Include the supported Postgres versions, given the PG 12.3 SSI finding ([Jepsen](https://jepsen.io/analyses/postgresql-12.3)).
10. **MEDIUM — concurrency model checking is tiny and unexecuted.**
    - Evidence: 4 loom models covering 2 modules, 0 runs (`loom.yml` manual); Shuttle backlogged (`concurrency-model-checking.md:66`).
    - Remedy: run loom on every PR (the models take under a second, per `loom.yml`). Adopt Shuttle for the `slot_tuner` semaphore accounting and `heartbeat` mpsc ordering ([Shuttle](https://github.com/awslabs/shuttle)).
11. **MEDIUM — property tests cover only pure functions.**
    - Evidence: 128 cases, no stateful tests (`prop_config.rs:5-28`).
    - Remedy: ShardStore-style model-based PBT ([Amazon Science](https://www.amazon.science/publications/using-lightweight-formal-methods-to-validate-a-key-value-storage-node-in-amazon-s3)). Random sequences of start, claim, heartbeat, reclaim, complete, signal and cancel against an executable reference model of the lifecycle table. Run a nightly deep pass with `PROPTEST_CASES=100000`.
12. **LOW — fuzzing is manual, 30 s per target, with no persisted corpus and no replay target.**
    - Evidence: `ci.yml:1846-1870`.
    - Remedy: a nightly scheduled fuzz job with a cached corpus. Add a structure-aware target (arbitrary `Vec<WorkflowEvent>` → replayer) that asserts no panic and a deterministic verdict.
13. **LOW — CI flakiness is unexplained.**
    - Evidence: the recurring `quota_enforcement_tests` and source-completion hangs (`ci.yml:1165-1175`; `docs/rnd/2026-09-23-…md`).
    - Remedy: capture and replay these under a DST or Antithesis-style harness rather than widening timeouts.

### Inferences
- A defensible priority order:
  1. Wiring fixes: gaps 1, 3 and 10's loom item. These cost days and turn existing assets into evidence.
  2. Process, database and network crash tests plus zombie completion: gaps 5 and 6.
  3. TLA+ or P models of the queue lease protocol: gap 4a.
  4. Engine DST: gap 2, the largest investment.
- The owner's "spec-first with Verus proofs" philosophy is not reflected anywhere in the code. The practical reading of the industry evidence is that TLA+ or P fits Harvest's cross-process protocols better than Verus does.

### Gaps
- Test-execution times and CI budget were not measured, so the cost estimates are qualitative.
- It is not known whether the owner has private or planned Verus work outside this repository. The GitHub issue searches for "formal verification", "Verus", "TLA+" and "simulation testing" returned no results.
