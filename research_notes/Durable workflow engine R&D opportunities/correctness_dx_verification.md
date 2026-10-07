# Correctness, DX, and Verification for Durable Workflow Engines (state as of October 2026)

Scope note: about 35 search and fetch calls. Coverage is broad but shallow in places. Every gap below is either a search that found nothing or a topic not reached in the time budget. Do not read "not found" as "does not exist".

## (a) Workflow versioning and in-flight evolution: what exists, what still hurts, and what tooling detects incompatible changes?

### Takeaway
The industry has converged on **pinning a run to the code version that started it**. Temporal Worker Versioning (GA March 2026), Restate immutable deployments, Vercel Workflow, DBOS version hashes, and Azure's `CurrentOrOlder` matching all do this. In-code patching remains the escape hatch for long-lived runs. **Proving that a code change is compatible with stored histories is still almost entirely unsolved.** The only tooling is replay-testing against sampled histories. The first academic model of upgrade risk appeared in July 2026.

### Cited Findings
**Temporal**
- Worker Versioning reached GA on March 30, 2026. It has two routing behaviors: PINNED keeps a workflow on its starting Worker version for its whole life, and AUTO_UPGRADE moves it to new versions. It also supports ramping. — [Temporal blog](https://temporal.io/blog/ga-worker-versioning-public-preview-upgrade-on-continue-as-new)
- Temporal's own announcement says patching creates "real code complexity and cognitive overhead". Patching is still needed for medium-duration workflows on AUTO_UPGRADE. The announcement mentions no tooling for detecting incompatible changes. — [Temporal blog](https://temporal.io/blog/ga-worker-versioning-public-preview-upgrade-on-continue-as-new)
- Upgrade on Continue-as-New is in Public Preview as an experimental SDK option. Pinned runs stay on their version within a run. The server signals when a new Target Version exists (`target_worker_deployment_version_changed`). The next Continue-as-New run then starts on that version. Temporal names entity workflows, checkpointing batch jobs, and AI agents with long sleeps as the target uses. — [Temporal docs](https://docs.temporal.io/production-deployment/worker-deployments/worker-versioning/upgrade-on-continue-as-new); [Python API: ContinueAsNewVersioningBehavior](https://python.temporal.io/temporalio.workflow.ContinueAsNewVersioningBehavior.html)
- Temporal's safe-deployment guide ranks the tools in this order: Worker Versioning (users "see improved error rates when adopting it"), replay testing with `Replayer`, and patching. It warns that "Eager start does not respect Worker versioning". — [Temporal safe deployments](https://docs.temporal.io/develop/safe-deployments)

**Restate**
- Each registered deployment is immutable. New requests go to the latest version. In-flight invocations stay bound to their original deployment until they complete. — [Restate docs: versioning](https://docs.restate.dev/services/versioning)
- Restate's design argument (Jack Kleeman, Feb 2024) has three parts: immutable deployments, ideally through AWS Lambda versions; handlers kept short (about an hour) so that several versions can run side by side; and implicit versioning on registration. The post admits that "Writing handlers that take weeks to complete is still a hard problem". Keeping very old code alive also has security and infrastructure costs. — [Restate blog](https://restate.dev/blog/solving-durable-executions-immutability-problem)

**DBOS**
- DBOS tags each workflow with an application version. By default the version is a hash of the workflow source code. DBOS does not recover workflows tagged with a different version. It also offers `Patch()`, which returns true for new executions and false for old ones. You remove the patch once no old workflows remain. — [DBOS docs: upgrading workflows (Go)](https://docs.dbos.dev/golang/tutorials/upgrading-workflows); [Python](https://docs.dbos.dev/python/tutorials/upgrading-workflows)

**Inngest**
- Inngest memoizes each step by a hash of its string ID plus a counter. Completed steps never re-execute, even across deploys. In-flight runs execute new steps when they reach them and ignore removed steps. If the step order changes, the SDK **logs a warning instead of failing**. — [Inngest docs: versioning](https://www.inngest.com/docs/learn/versioning); [TS SDK v3 release](https://www.inngest.com/blog/releasing-ts-sdk-3)

**Vercel Workflow DevKit**
- On Vercel, each run is pegged to the deployment that started it. Replay re-runs the whole function and matches steps to cached results **by order**. Correlation IDs come from an RNG seeded with the run ID. Replaying a v1 run on v2 with reordered steps "can quietly produce wrong results". — [useworkflow.dev (Vercel world)](https://useworkflow.dev/worlds/vercel); [Platformatic blog](https://blog.platformatic.dev/durable-workflows-kubernetes-version-safe) (vendor source: Platformatic sells version-safe orchestration for Kubernetes)

**Azure Durable Functions / Durable Task SDKs**
- Built-in orchestration versioning was documented as of 2026-05. Each instance is permanently tagged with a version (`defaultVersion` in `host.json` or on the client). Orchestrator code branches on `context.Version`. The match strategy is `None`, `Strict`, or `CurrentOrOlder` (the default). The failure strategy is `Reject` (back to the queue, the default) or `Fail` (terminal). Instances created before versioning was turned on report a null version. Old workers can slow routing on the Azure Storage and MSSQL backends. — [Microsoft Learn](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-orchestration-versioning)
- Microsoft's guidance is to "preserve the exact orchestrator logic for older versions". Legacy branches can be removed only after a manual check that no instances remain on that version. — [Microsoft Learn](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-orchestration-versioning)

**Golem**
- Golem has two update modes. *Automatic* replays the agent's operation log against the new component. It suits backward-compatible changes. *Manual* (snapshot-based) uses a user-written `save-snapshot` in the old version and `load-snapshot` in the new one. It is required for breaking changes such as renamed or removed functions, changed signatures, or restructured state. — [Golem docs: updating running agents](https://learn.golem.cloud/how-to-guides/common/golem-update-running-agents); [Rust language guide](https://learn.golem.cloud/rust-language-guide/updating); [custom snapshot (Rust)](https://learn.golem.cloud/how-to-guides/rust/golem-custom-snapshot-rust)

**Research on detecting incompatible changes**
- Maraschi and Collina, "A Telemetry-Driven Model for Quantifying Upgrade Risk in Durable Workflow Execution" (arXiv 2607.13617, July 15, 2026). It gives a formal model of event log, replay relation, and determinism contract. It names three failure modes: (1) determinism-contract violation; (2) rehydration failure, where a recorded step payload no longer fits what the new code expects; and (3) behavioral drift, where replay succeeds but future steps behave differently. It proposes a protocol/interface/migration change taxonomy that can be computed from the static diff plus payload telemetry. It proves that a zero backward-risk verdict guarantees safe rehydration. It sorts runs into migrate/review/pin classes and does fleet-level min-cut migration over coupling graphs. It names no specific engine. Both authors are Platformatic founders, a vendor. — [arXiv abstract](https://arxiv.org/abs/2607.13617); [HTML](https://arxiv.org/html/2607.13617v1)

### Inferences
- Every engine uses one of three dials: **pin** (Restate, Vercel, DBOS, Temporal PINNED), **branch in code** (Temporal patch, DBOS `Patch`, Azure `context.Version`), or **migrate state explicitly** (Golem snapshots). No production engine checks *automatically* whether a given in-flight run can move to new code. That leaves pinning costly for long runs and patching risky.
- Inngest's "warn, don't fail" policy trades the loud failure (a non-determinism error) for the silent one (behavioral drift, failure mode 3 in the Maraschi/Collina taxonomy). Engines lack a way to make this policy explicit per run.
- **R&D opportunity: a per-run compatibility checker.** Given a code diff and a stored history, decide statically or by bounded replay whether that history replays under the new code, and whether recorded payloads still deserialize. Output a migrate/pin verdict per run. That is the Maraschi/Collina idea made exact, not probabilistic. A typed, compiled host language like Rust makes payload-schema compatibility much easier to decide than JS or Python.
- **R&D opportunity: Golem-style typed state migration for the event-sourced model.** A function that rewrites a history prefix, or snapshots and resumes, with machine-checked preservation of already-completed side effects.
- **R&D opportunity: patch lifecycle tooling.** Find when no live run can still take a given patch branch, so the patch can be deleted safely. Azure and DBOS today leave this to a manual query.

### Gaps
- Did not find user-survey data, such as a percentage of teams hurt by versioning. Pain evidence is vendor self-reporting (Temporal's "cognitive overhead") and forum threads.
- Did not verify the 2026 details of DBOS versioning (the [April 2026 DBOS features post](https://www.dbos.dev/blog/dbos-new-features-april-2026) was found but not read), or whether Restate added anything for very long handlers after 2024.
- No independent evaluation of the Maraschi/Collina model on real fleets was found.

## (b) Determinism enforcement: static analyzers, sandboxes, runtime detection, and how common are non-determinism incidents?

### Takeaway
Determinism is enforced by a patchwork that differs per language and is always incomplete. Go has `workflowcheck`, a static analyzer. TypeScript and Vercel use V8 isolates with seeded `Math.random`/`Date`. Python has a sandbox that is "not completely isolated". .NET relies on community Roslyn analyzers plus a custom TaskScheduler. Runtime detection, meaning a non-determinism error on replay, is the backstop everywhere. No mainstream engine uses an effect system or a language-level purity guarantee.

### Cited Findings
- **Go:** `workflowcheck` (merged from `temporal-determinist`) transitively flags calls to `time.Now`, `math/rand` global, `crypto/rand.Reader`, `os.Stdin/Stdout/Stderr`, and similar. It "will not catch all cases of non-determinism such as global var mutation". It lives in `sdk-go/contrib/tools/workflowcheck`. — [temporal-determinist on pkg.go.dev](https://pkg.go.dev/github.com/cretz/temporal-determinist); [Temporal community: Go SDK troubleshooting](https://community.temporal.io/t/go-sdk-troubleshooting/4440)
- A community thread asks for compile-time checkers for workflow definitions. This shows demand beyond lint-style tools. — [Temporal community](https://community.temporal.io/t/could-we-add-compile-time-checkers-for-workflow-definition/4766)
- **TypeScript:** the sandbox replaces non-deterministic methods. For example, `Math.random()` becomes a PRNG seeded per workflow execution. Go and Java SDKs have no sandbox. — [Temporal TS durable execution guide](https://docs.temporal.io/dev-guide/typescript/durable-execution)
- **Python:** the sandbox isolates global state and restricts known non-deterministic calls. It "is not completely isolated, and some libraries can internally mutate state". — [Temporal Python sandbox docs](https://docs.temporal.io/develop/python/python-sdk-sandbox)
- **.NET:** Temporal uses a deterministic custom `TaskScheduler`. Many .NET async APIs (`Task.Run` overloads, `Task.Delay`) implicitly use `TaskScheduler.Default`, and .NET does not allow full control over them. Community Roslyn analyzers (`TemporalCommunity.Extensions.Analyzers`) flag these misuses. — [Introducing Temporal .NET](https://temporal.io/blog/introducing-temporal-dotnet); [temporal-dotnet-analyzers](https://github.com/sains1/temporal-dotnet-analyzers); [NuGet](https://www.nuget.org/packages/TemporalCommunity.Extensions.Analyzers/0.3.2)
- **Vercel Workflow:** the `"use workflow"` directive is a compile-time boundary. The bundler packages the workflow into a sandbox that restricts Node.js APIs and provides a seeded `Math.random()` and `Date`. The directive also "allows static analysis of workflow structure". — [Vercel workflow docs: understanding directives (mirror)](https://www.mintlify.com/vercel/workflow/how-it-works/understanding-directives)
- **Failure modes in practice:** Temporal error code TMPRL1100 is the non-determinism error. One example: adding a timeout to `condition()` caused `runReplayHistory` to fail in CI with "Timer machine does not handle this event". — [Temporal community](https://community.temporal.io/t/runreplayhistory-accuses-non-determinism-for-condition-with-timeout/15579). Users also ask how to find non-determinism failures from the Web UI. — [Temporal community](https://community.temporal.io/t/find-non-determinism-issues-from-the-web-ui/12155)
- Common triggers include reordering activities, adding or removing steps without versioning, branching on wall-clock time or random values, and **library upgrades that change internal behavior**. All of these break replay "silently, until a worker tries to resume an in-flight workflow". — [Temporal safe deployments](https://docs.temporal.io/develop/safe-deployments) and the replay-testing guidance summarized in [Keith Tenzer's blog](https://keithtenzer.com/temporal/Altering_Space-Time_Continuum_Testing_for_Determinism/)
- **Category-level classification:** determinism-contract violation, rehydration failure, and behavioral drift. The third is not detected by replay at all. — [arXiv 2607.13617](https://arxiv.org/html/2607.13617v1)
- **WASM sandbox:** Golem runs WASM components. Automatic update replays the oplog against the new component. Determinism comes from the WASM host controlling all imports. — [Golem docs](https://learn.golem.cloud/how-to-guides/common/golem-update-running-agents) (the mechanism claim is partly inference; see Gaps)

### Inferences
- Static checkers are **denylist-based**. They know specific bad functions, not a general property. None of them reasons about shared mutable state, iteration order of hash maps, or transitive third-party library behavior. In Rust, the S2 team hit exactly these problems (HashMap seeding, timestamps embedded by dependencies) in the DST context; see (c).
- **R&D opportunity: allowlist or capability-based determinism for Rust.** Rust's type system could make non-determinism unrepresentable inside workflow bodies. Options include a workflow context that is the *only* source of time, randomness, and I/O; no ambient `std::time`; and a Clippy- or dylint-style lint that forbids `HashMap` iteration, `Instant::now`, `tokio::spawn`, and similar calls within `#[workflow]` functions. No existing tool provides a sound guarantee here.
- **R&D opportunity: catch drift where replay is blind.** Replay checks only the command sequence. Changes to activity inputs, payload schemas, or not-yet-executed steps escape it. Diff-aware checks would close that gap.

### Gaps
- No published incident-rate statistics were found, such as non-determinism errors per deploy or the share of outages caused by them. Evidence is anecdotal (forum threads, a [hashnode post "One Line, One Outage"](https://hashnode.com/@naman-gupta) that was not read).
- Did not verify whether Temporal shipped an official (non-community) .NET analyzer by 2026.
- Did not research language-level effect systems (Koka, OCaml 5 effects, Unison abilities) as applied to durable execution. No source was gathered.
- Did not confirm Golem's specific determinism mechanism from a primary source.

## (c) Testing: time-skipping, replay testing, deterministic simulation testing (DST), fuzzing, property-based testing, Jepsen

### Takeaway
Replay testing against captured histories is the standard user-level practice. Its main operational blocker is encrypted or PII-bearing payloads. DST has become the norm for testing *engines* (Resonate, AWS, Antithesis customers, S2 in Rust). There are no off-the-shelf DST or property-based testing frameworks for *user workflows*, and no public Jepsen analysis of a workflow engine was found.

### Cited Findings
- **Replay testing:** the capture-then-replay pattern stores production or test histories as JSON fixtures, replays them in CI, and fails the build on mismatch. Temporal recommends a two-phase deploy: replay recent real histories against new code, then roll out. — [Temporal safe deployments](https://docs.temporal.io/develop/safe-deployments); [Keith Tenzer](https://keithtenzer.com/temporal/Altering_Space-Time_Continuum_Testing_for_Determinism/)
- Replay against production histories breaks down with encrypted payloads, because the fetched histories may not decrypt, and with PII, which must be scrubbed or avoided. — [Keith Tenzer](https://keithtenzer.com/temporal/Altering_Space-Time_Continuum_Testing_for_Determinism/) (restating Temporal guidance)
- **Resonate** builds its correctness story on three pillars:
  - An executable **Lean 4 spec** of the protocol. It covers 18 of 21 client-reachable handlers (promises, tasks, schedules) and six internal steps (timeouts, drains, lease expiry, retry, schedule firing).
  - **Differential random testing** of the server against an independent in-memory Oracle and SQLite, with optional Postgres/MySQL. It runs up to 200k steps and covers all 22 operation kinds.
  - **DST of the TypeScript SDK**: 10k steps across three simulated workers. Each seed runs twice to confirm determinism, and failing seeds auto-file GitHub issues.

  Concurrent histories are checked for linearizability with Porcupine. A trace checker validates real server traffic against the Lean machine. The page admits that 63 of 95 properties lack full proof (backed only by exhaustive sweeps of scripts up to three steps long), and that the conformance testbed is not public. — [Resonate: How Resonate is tested](https://docs.resonatehq.io/evaluate/how-resonate-is-tested)
- Resonate's Dominik Tornow presented DST at FOSDEM 2025 ("Squashing the Heisenbug with Deterministic Simulation Testing"). — [FOSDEM 2025](https://fosdem.org/schedule/event/fosdem-2025-4279-squashing-the-heisenbug-with-deterministic-simulation-testing)
- **Rust DST (S2, April 2025):** S2 started with **turmoil** (many hosts on one thread, seeded network chaos). Runs were still not reproducible. Causes included timestamps embedded by dependencies, `HashMap` randomization, and third-party time and entropy use. S2 adopted madsim-style libc symbol overrides (`getrandom`, `getentropy`, `clock_gettime`, `CCRandomGenerateBytes`) in the open-source `mad-turmoil` crate. CI "meta tests" compare trace logs byte-for-byte across seed reruns. S2 found 17 notable bugs pre-production. The approach needs compile-time feature gates and dependencies that tolerate symbol overriding. — [S2 blog](https://s2.dev/blog/dst); [mad-turmoil](https://rust-digger.code-maven.com/crates/mad-turmoil)
- turmoil now has companion crates: turmoil-net (simulated socket stack swapped for `tokio::net`), turmoil-fs, and turmoil-io-uring. — [turmoil-net docs](https://docs.rs/turmoil-net)
- **Antithesis** runs whole systems in a deterministic hypervisor that controls network, scheduling, and clocks, so any bug found reproduces exactly. etcd adopted it in 2025. — [Antithesis: how it works](https://antithesis.com/product/how_does_antithesis_work); [etcd blog](https://etcd.io/blog/2025/autonomus_testing_with_antithesis/)
- **AWS:** Brooker and Desai describe AWS's portfolio. It includes P (a state-machine modeling language used on S3, EBS, DynamoDB, Aurora, EC2, and IoT), property-based testing, fuzzing, runtime monitoring, and the "convergent evolution" of deterministic simulation testing. — [CACM: Systems Correctness Practices at AWS](https://cacm.acm.org/practice/systems-correctness-practices-at-amazon-web-services); [ACM Queue](https://queue.acm.org/detail.cfm?id=3712057)

### Inferences
- **R&D opportunity: DST for user workflows, not just engines.** A durable workflow is already a deterministic state machine driven by recorded events. That makes it an unusually good DST target. A harness can inject activity failures, timeouts, duplicate deliveries, signal races, worker crashes at every await point, and version upgrades mid-run, all from a seed. No vendor ships this as a product feature.
- **R&D opportunity: privacy-preserving replay corpora.** Replay with synthesized or redacted payloads that keep control flow but drop PII would remove the main blocker to replaying production histories in CI. This connects to codec and key handling. No tool for it was found.
- **R&D opportunity: a public Jepsen-style analysis of workflow engines and their queue backends.** The safety claims at stake include exactly-once step completion, lease and timeout semantics, and signal ordering. None was found, so a published analysis would be novel and credible.
- The Resonate model of an executable Lean spec, differential testing against an Oracle, and production trace-checking can be replicated for any engine with a well-defined state machine. The public evidence suggests it is the most rigorous design in the space.

### Gaps
- Did not gather primary sources on Temporal's time-skipping test server, Restate's or DBOS's test harnesses, or the Inngest dev server. These features exist per general knowledge, but no URL was collected, so no claims are made.
- **No Jepsen analysis of any workflow engine (Temporal, Cadence, Restate, DBOS, Inngest) was found.** One search returned nothing relevant. A more targeted search on jepsen.io is advisable before asserting absence in the final report.
- No published property-based testing or fuzzing framework aimed at user workflow code was found. This is likely an absence, but it was not exhaustively searched.
- Did not confirm whether Temporal or any other workflow vendor is a public Antithesis customer.

## (d) Debugging and observability: time-travel debugging, history visualization, introspection, OTel

### Takeaway
Engines expose their history (Temporal event history and UI, Restate SQL tables over invocations and journals, DBOS time-travel debugger). Cross-engine standards are immature. **No official OpenTelemetry semantic conventions for workflows exist.** Dapr began codifying conventions with OTel Weaver in 2026. Trace-context propagation across durable boundaries is still being fixed SDK by SDK.

### Cited Findings
- **Restate** exposes SQL introspection tables: `sys_invocation`, `sys_journal`, `sys_inbox`, `sys_keyed_service_status`, `sys_service`, `sys_deployment`, `sys_idempotency`, and `state`. They can be queried through the CLI or HTTP, and the UI is aimed at debugging. — [Restate SQL introspection reference](https://docs.restate.dev/references/sql-introspection.md); [Restate introspection](https://docs.restate.dev/services/introspection)
- **DBOS** time-travel debugging: DBOS Cloud records every step and database change. The debugger rewinds database state to what it was when the selected workflow ran. A VS Code CodeLens lists recent executions to replay locally. Developers can add prints or read queries that return historical results. — [DBOS docs: time-travel debugging](https://docs.dbos.dev/cloud-tutorials/timetravel-debugging); [DBOS Debugger reference](https://docs.dbos.dev/python/reference/dbos-debugger); [DBOS blog: database time travel](https://dbos.dev/blog/database-time-travel)
- **OTel and Dapr (2026):** context broke at the gRPC streaming boundary, so activity code could not join the workflow trace. Workflow, activity, and user spans came out fragmented. Dapr fixed this by embedding `traceparent`/`tracestate` in activity messages in durabletask-go, restoring context in durabletask-java, and tidying parent-child structure. Dapr is adopting **OTel Weaver** to define workflow telemetry attributes in machine-readable form. The post points to no official workflow semconv. — [OpenTelemetry blog: Improving Async Workflow Observability in Dapr](https://opentelemetry.io/blog/2026/dapr-workflow-observability/)
- Vercel's Workflow core ships its own telemetry semantic-convention module (`@workflow/core/telemetry/semantic-conventions`). This suggests each vendor is defining its own attributes. — [unpkg listing](https://app.unpkg.com/eve@0.27.2/files/dist/src/compiled/@workflow/core/telemetry/semantic-conventions.d.ts) (indirect evidence via a compiled bundle; weak source)
- Temporal users ask for ways to find non-determinism failures from the Web UI. This points to a diagnosis gap in history UIs. — [Temporal community](https://community.temporal.io/t/find-non-determinism-issues-from-the-web-ui/12155)

### Inferences
- **R&D opportunity: a workflow OTel semantic convention.** Proposed attributes include run ID, version and deployment, step or activity ID, attempt, replay vs. first execution, and durable-wait spans. The convention should also define a rule for not emitting duplicate spans during replay. Dapr's Weaver work is the nearest effort, and the field is open for a cross-vendor proposal.
- **R&D opportunity: critical-path and cost attribution over event histories.** A history already holds schedule, start, and complete timestamps per step. Computing the critical path, queue-wait vs. execution vs. durable-sleep time, and per-step cost is a mechanical analysis that no surveyed engine advertises.
- **R&D opportunity: a non-determinism diff viewer.** On TMPRL1100-class failures, show the first divergence between the recorded command sequence and the one the new code produced, mapped to source lines.

### Gaps
- Did not collect primary sources on the Temporal UI timeline view, the Inngest dev server, or IDE replay debugging for Temporal. Nothing is claimed about them.
- Found no source on cost or latency attribution features in any engine.
- Did not confirm the date or author of the OTel Dapr post beyond its 2026 URL path.

## (e) Formal methods for engines and for user workflows

### Takeaway
Formal work on workflow *engines* is real but sparse and mostly vendor-internal. Published examples are the Durable Functions OOPSLA 2021 semantics with proofs, Resonate's executable Lean 4 spec, and AWS's P/TLA+ practice for its own services. No public TLA+ or P model of Temporal or Cadence was found. Model checking of *user* workflows, such as saga compensation correctness, appears unaddressed by any shipping product.

### Cited Findings
- Burckhardt et al., "Durable Functions: Semantics for Stateful Serverless", OOPSLA 2021. It gives a formal high-level model based on the untyped lambda calculus. It defines two lower-level execution models with compute/storage separation and record-replay, and proves them equivalent to the high-level model. — [MSR publication page](https://www.microsoft.com/en-us/research/publication/durable-functions-semantics-for-stateful-serverless/); [paper with proofs (PDF)](https://microsoft.com/en-us/research/uploads/prod/2021/10/DF-Semantics-With-Proofs.pdf); [SPLASH 2021](https://2021.splashcon.org/details/splash-2021-oopsla/37/Durable-Functions-Semantics-for-Stateful-Serverless)
- A related MSR paper is "Reliable Actors with Retry Orchestration". — [arXiv 2111.11562](https://arxiv.org/pdf/2111.11562)
- Resonate's Lean 4 spec is normative ("where prose and Lean disagree, the Lean model wins"). It is executable and is used to trace-check production traffic. 63 of 95 properties are not fully proved. — [Resonate docs](https://docs.resonatehq.io/evaluate/how-resonate-is-tested)
- AWS moved many engineers from TLA+ to P because TLA+ was hard to adopt. P models systems as communicating state machines with automated checking backends. AWS also uses theorem proving, deductive verification, model checking, PBT, fuzzing, and runtime monitoring. — [CACM](https://cacm.acm.org/practice/systems-correctness-practices-at-amazon-web-services)
- A systematic literature review covers a decade of industrial TLA+ use. It is useful for finding whether any workflow engine has been specified. — [arXiv 2411.13722](https://arxiv.org/pdf/2411.13722)
- **Upgrade compatibility as a formal property:** the first formal statement found of "safe to upgrade a run" over the event-log/replay model. It includes a proof that a zero backward-risk verdict implies safe rehydration. — [arXiv 2607.13617](https://arxiv.org/abs/2607.13617)
- **Choreographic programming in Rust:**
  - ChoRus (`chorus_lib`, UC Santa Cruz, PLDI 2024 CP track) implements library-level choreographies, with endpoint projection as dependency injection and "choreographic enclaves". — [arXiv 2311.11472](https://ar5iv.labs.arxiv.org/html/2311.11472); [docs.rs/chorus_lib](https://docs.rs/chorus_lib)
  - Suki does choreographed distributed dataflow in Rust (PLDI 2024 CP track). — [PLDI 2024](https://pldi24.sigplan.org/details/cp-2024-papers/4/Suki-Choreographed-Distributed-Dataflow-in-Rust)
  - HasChor is the Haskell origin of library-level choreographies. — [Hackage](https://hackage.haskell.org/package/HasChor-0.1.0.1/docs/README.md)
  - `telltale-choreography` is another Rust crate (v4.0.0). — [docs.rs](https://docs.rs/telltale-choreography/4.0.0)

### Inferences
- **R&D opportunity: a public, checkable model of a Temporal-style engine.** Model the history service, task queues, leases, timers, and versioning routing in TLA+, P, or Lean, plus trace validation of a real implementation against it, in the style of Resonate. Nothing equivalent is public for Temporal or Cadence.
- **R&D opportunity: model checking user workflows.** Durable workflow code is already a restricted, deterministic state machine. Extracting a model from it would allow checks such as: does every forward step that completed have a compensating step on every failure path? Are signal handlers race-free with the main body? Is there a deadlock between child workflows? No product or paper doing this for Temporal/Restate-style code was found.
- **R&D opportunity: choreographies plus durability.** Choreographic programming gives deadlock-freedom by construction across participants. Durable execution gives crash recovery per participant. No surveyed system combines the two. A durable choreography in Rust, with each projected endpoint journaled, is unexplored.
- **R&D opportunity: Rust-specific verification of engine internals.** Verus, Kani, Prusti, or Creusot could check the replay state machine, lease and fencing logic, or CAS-based writers, or bound-check the history codec. No such application to a workflow engine was found, but that topic was not searched specifically.

### Gaps
- **No public TLA+ or P model of Temporal, Cadence, or Azure Durable Task's backends was found.** Two searches returned only generic TLA+ material. Temporal may use formal methods internally without publishing.
- Did not research Verus, Kani, Prusti, or Creusot case studies, nor session-type libraries in Rust such as `rumpsteak` or `ferrite`. No sources were gathered.
- Did not find work that generates TLA+ from workflow code or checks saga compensations. This is likely a real gap, but the search was not exhaustive.

## (f) Programming-model innovation: async/await durability, Rust libraries, compile-time checks, typed messages, sagas, durable promises and streams

### Takeaway
The programming model is shifting from explicit "workflow vs. activity" SDKs toward **annotating ordinary async code**: Vercel `"use workflow"`/`"use step"` compiler directives, DBOS decorators, Restate durable async/await, Resonate durable promises. Rust has many young crates (restate-sdk, duroxide, durare, flawless, Golem's WASM components). None uses the Rust type system to make non-determinism or version-incompatibility a compile error.

### Cited Findings
- **Vercel Workflow DevKit:** the `"use workflow"` and `"use step"` directives form the compile-time semantic boundary that lets workflows suspend, resume, and replay deterministically. The compiler restricts APIs and enables static analysis of workflow structure. — [Vercel workflow docs mirror](https://www.mintlify.com/vercel/workflow/how-it-works/understanding-directives); [unpkg docs (workflow@4.2.0-beta.68)](https://app.unpkg.com/workflow@4.2.0-beta.68/files/docs/how-it-works/understanding-directives.mdx)
- **DBOS** builds on decorators and annotations. Durable workflows, steps, and DB transactions run inside the app process, with versioning, patching, and observability on top. — [DBOS Python upgrading docs](https://docs.dbos.dev/python/tutorials/upgrading-workflows); [DBOS architecture](https://docs.dbos.dev/architecture)
- **Restate** describes itself as "distributed durable async/await". The Rust SDK (`restate-sdk`) "is currently in active development, and might break across releases". — [restate-sdk crate info](https://rust-digger.code-maven.com/crates/restate-sdk)
- **duroxide** is a Rust framework for long-running, code-based workflows with orchestrations, activities, and ContinueAsNew, modeled on Durable Task. A `microsoft/duroxide` path appears in an index, which suggests Microsoft provenance. This was not confirmed at a primary source. — [docs.rs/duroxide](https://docs.rs/duroxide); [tomevault index](https://tomevault.io/tome/microsoft/duroxide)
- **durare** is a Rust library: ordinary async functions, each step checkpointed to your database, unfinished workflows resumed. — [docs.rs/durare](https://docs.rs/crate/durare/0.1.1)
- **flawless** is a durable-execution engine and toolkit for Rust. — [crate info](https://rust-digger.code-maven.com/crates/flawless)
- **Golem** runs WASM components with oplog replay and explicit snapshot-based update functions. — [Golem docs](https://learn.golem.cloud/rust-language-guide/updating)
- **Durable promises:** Resonate's protocol core is promises (create, get, settle, register callback/listener), tasks, and schedules, all specified in Lean. — [Resonate docs](https://docs.resonatehq.io/evaluate/how-resonate-is-tested)
- **Durable streams:** ElectricSQL publishes a Rust durable-streams server (`@electric-ax/durable-streams-server-rust@0.1.1`). — [newreleases.io](https://newreleases.io/project/github/electric-sql/electric/release/@electric-ax%2Fdurable-streams-server-rust@0.1.1)
- **Inngest** identifies steps by string ID and memoizes them, rather than by position. This makes step insertion tolerant by design. — [Inngest docs](https://www.inngest.com/docs/learn/versioning)
- **Temporal Go determinism around goroutines:** Go workflows must use `workflow.Go` and channels rather than native goroutines. — [Temporal Go multithreading best practices](https://docs.temporal.io/develop/go/best-practices/multithreading)

### Inferences
- Two replay-identity models compete. **Positional** matching (Temporal command sequence, Vercel order-based correlation IDs) is strict, so reordering breaks replay. **Named-step** matching (Inngest step IDs, Restate journal entries) is tolerant but invites drift. **R&D opportunity:** a hybrid where step identity is a compile-time-derived stable key (for example, a macro-generated hash of the call site plus a user label). Insertions would then be safe, and reorderings would be detectable at build time by diffing the step graph between versions.
- **R&D opportunity: Rust proc-macro workflows that emit a static step graph.** In the spirit of Vercel's compiler transform, a `#[workflow]` macro could (1) forbid ambient non-determinism through the type system, (2) export the step/signal/update graph as data for version diffing and model checking, and (3) generate typed signal and update handles. No existing Rust crate found claims all three.
- **R&D opportunity: sagas with checked compensation.** A typed DSL where each forward step must declare its compensation, so the compiler or a model checker can guarantee coverage. No Rust or mainstream implementation with such a guarantee was found.
- Structured concurrency inside workflows (scoped child steps and timers with deterministic cancellation) is handled ad hoc per SDK, for example Go's `workflow.Go` and .NET's TaskScheduler work-arounds. Rust's lack of a built-in deterministic executor is both a problem and an opportunity: a purpose-built single-threaded deterministic executor for workflow bodies.

### Gaps
- Did not verify the state of an official Temporal Rust SDK as of October 2026, nor the APIs of restate-sdk-rust, duroxide, or durare in depth.
- Did not research typed signals and updates (Temporal Updates, Nexus), structured-concurrency designs, or saga DSLs in specific engines. No sources were gathered.
- Did not find Replay, Strange Loop, or P99 CONF talks specific to these topics in the time budget.
