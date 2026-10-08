# Market Trends and User Requirements for Durable Workflow Engines (2024 to October 2026)

Research date: 2026-10-07. Scope: cross-cutting requirements, pain points and direction of the category. Single-vendor deep dives are covered by other researchers. Every claim carries a date where the source gave one. Source quality is flagged where a claim comes from a secondary or likely-SEO aggregator.

## Q1. Durable AI agents: what agent frameworks need from durable execution, and who integrated with what (2025-2026)

### Takeaway
By mid-2026 "durable by default" became the expected baseline for production agents, and every major agent framework shipped a durability plug-in point (Pydantic AI, OpenAI Agents SDK via Temporal, Microsoft Agent Framework via Durable Task, Vercel WDK `DurableAgent`, LangGraph checkpointers, Mastra). The concrete requirements that recur are: model calls and tool calls as individually checkpointed steps that are not repeated on replay, suspended human-approval waits that consume no compute, streaming out of durable steps, persistent per-session conversation state (often modeled as an entity/actor), and offloading of large, ever-growing conversation payloads.

### Cited Findings
**Market signal and adoption**
- Temporal raised a $300M Series D led by Andreessen Horowitz at a $5B valuation, announced 17 Feb 2026, doubling from a $2.5B valuation in Oct 2025; coverage ties the round to AI agent workloads; named customers include OpenAI, JPMorgan Chase, Netflix, Snap — [GeekWire, Feb 2026](https://www.geekwire.com/2026/temporal-raises-300m-hits-5b-valuation-as-seattle-infrastructure-startup-rides-ai-wave/)
- The Thoughtworks Technology Radar (Apr 2026) reportedly placed "ignoring durability in agent workflows" in its Caution ring as an anti-pattern, and advised starting with framework-built-in durability (LangGraph, Pydantic AI) then moving to a platform such as Temporal as workflows become critical — [Unico Connect blog citing the Radar](https://unicoconnect.com/blogs/durable-agent-workflows). Note: secondary source; I could not load the primary Radar blip text.
- AWS launched Lambda durable functions (re:Invent, Dec 2025): checkpoint-and-replay, automatic retries, suspended waits up to one year without compute charges, built-in idempotency keyed on execution name; launch region US East (Ohio), Node.js 22/24 and Python 3.13/3.14, open-source JS/TS and Python SDKs; explicitly positioned for "AI workflows" — [AWS Lambda docs](https://docs.aws.amazon.com/en_us/lambda/latest/dg/durable-functions.html); [launch news, 8 Dec 2025](https://ascii.co.uk/news/article/news-20251208-d1074902/aws-lambda-durable-functions-enable-long-running-workflows-w); [Classmethod re:Invent note: retries replay from the beginning, not from the checkpoint](https://dev.classmethod.jp/articles/durable-functions-replay-from-beginning/)
- A secondary roundup (2026) claims that the Temporal + OpenAI Agents SDK, Restate + Vercel AI SDK, DBOS + Pydantic AI and Inngest AgentKit integrations all landed within a single 12-month window, shifting the frame from "you write a durable wrapper" to "the wrapper is included"; it also claims OpenAI Codex web agent, Replit Agent 3 and Cursor run on Temporal — [Reactify Solutions, 2026](https://www.reactify-solutions.com/articles/durable-ai-agents-2026). Note: unverified secondary/marketing source; the deployment claims were not confirmed against primary sources.

**Framework integrations (dated)**
- OpenAI Agents SDK + Temporal: announced 30 Jul 2025 in collaboration with OpenAI. Agent orchestration runs inside the Workflow; each model call runs as an Activity so it retries durably and is not repeated during replay. Python integration is in public preview; TypeScript is pre-release — [BusinessWire, 30 Jul 2025](https://www.businesswire.com/news/home/20250730783559/en/Temporal-and-OpenAI-Launch-Integration-for-Enterprises-Developing-Production-Agents); [Temporal docs](https://docs.temporal.io/develop/python/integrations/openai-agents); [Temporal blog](https://temporal.io/blog/announcing-openai-agents-sdk-integration)
- Temporal added an "OpenAI Agents SDK sandbox integration" in public preview on 16 Apr 2026 — [Temporal changelog](https://temporal.io/changelog)
- Pydantic AI officially supports Temporal, DBOS, Prefect and Restate. Temporal, DBOS and Prefect ship inside Pydantic AI; Restate's lives in the Restate SDK and uses only Pydantic AI's public interface. Durable agents keep "full support for streaming and MCP". The Prefect capability routes model requests, tool calls and MCP communication through Prefect tasks — [Pydantic AI durable execution overview](https://pydantic.dev/docs/ai/integrations/durable_execution/overview/index.md). A newer version of the same docs says Pydantic AI "supports eight durable execution solutions, plus a builder for any other engine" and refers to a "stable durable execution backend builder" for third parties — [Pydantic AI docs (current)](https://pydantic.dev/docs/ai/capabilities/durable_execution/overview/). Note: the two doc versions conflict on the count; the current one implies the list grew in 2026 and that a generic backend-builder API now exists.
- Microsoft Agent Framework: a Durable Task extension makes registered agents durable "without changes to your agent logic": persistent sessions, built-in API endpoints, distributed scaling. Internally each agent session is a durable entity that manages conversation state and checkpointing. Use cases listed: sessions that survive crashes, deterministic multi-agent orchestration, and human-in-the-loop approvals or timed waits lasting "hours, days, or weeks without consuming compute". Hosting on Azure Functions or bring-your-own compute — [Microsoft Learn: durable agents](https://learn.microsoft.com/en-us/azure/durable-task/sdks/durable-agents-microsoft-agent-framework); [Agent Framework durable extension](https://learn.microsoft.com/mt-mt/agent-framework/integrations/durable-extension)
- Vercel Workflow Development Kit (WDK): open-source TypeScript; `"use workflow"` / `"use step"` directives; event-sourced replay so workflows must be deterministic; workflow functions run sandboxed with no native `fetch` or `setTimeout`; steps have full Node access; "streaming, persistence, and resumable runs work out of the box"; a `DurableAgent` primitive for agents — [Vercel workflow examples](https://examples.vercel.com/workflow); [skills.sh summary of WDK docs](https://skills.sh/vercel-labs/vercel-plugin/workflow). Public beta reportedly Oct 2025 — [Reactify (secondary)](https://www.reactify-solutions.com/articles/durable-ai-agents-2026)
- LangGraph: checkpointer-based durability with durability modes `sync`, `async` and `exit`; `PostgresSaver` for production; snapshots organized into threads to support long-running assistants, human-in-the-loop review and fault tolerance — [LangChain reference: Durability](https://reference.langchain.com/python/langgraph-sdk/schema/Durability); [LangChain KB](https://support.langchain.com/articles/1242226068-how-do-i-configure-checkpointing-in-langgraph)
- Mastra: workflow "time travel" re-executes from any step using a stored snapshot or caller-supplied context, with modified `inputData`; supports nested workflows and a `timeTravelStream()` that streams execution events; fails fast with a descriptive error if the workflow definition changed since the run — [Mastra docs](https://mastra.ai/docs/workflows/time-travel/llms.txt)

**Specific agent requirements surfaced**
- Long conversation histories grow each turn; Temporal's External Storage (claim-check offload to e.g. S3) is pitched explicitly for AI agent conversations whose cumulative size degrades workflow performance — [Temporal docs: External Storage](https://docs.temporal.io/external-storage); public preview 14 May 2026 — [Temporal changelog](https://temporal.io/changelog)
- Tool-call side effects: "partial effects, losing-branch residue, stale writes, or irreversible sends" remain after faults, speculative execution or concurrent agents; retries, checkpoints, locks and compensation conflate "which effects must settle together, and when". Atomix proposes progress-aware transactions that classify effects as bufferable, reversible-external or irreversible, with microsecond-scale overhead — [Atomix, arXiv 2602.14849, Feb 2026, rev. May 2026](https://arxiv.org/abs/2602.14849)
- Agent remediation after an AI error (undo/compensation) is identified as an open design area — [Jack Vanlightly, "Remediation: What happens after AI goes wrong?", 28 Jul 2025](https://jack-vanlightly.com/blog/2025/7/28/remediation-what-happens-after-ai-goes-wrong)
- Evaluation criteria for "AI agent fit" in a Dec 2025 comparison: LLM calls, tool usage, human approvals and production replay; one alternative (Kitaru) differentiates on "full-run agent replay" against controlled tool history for regression testing — [ZenML, 17 Dec 2025](https://www.zenml.io/blog/temporal-alternatives)

### Inferences
- The integration pattern is converging: agent loop = workflow/orchestrator; model call = step/activity; tool call = step/activity; session = entity/actor/virtual object; approval = durable signal/event wait. An engine that exposes these four primitives cleanly can host any of the frameworks.
- Pydantic AI's public "backend builder" and Restate's integration through Pydantic AI's public interface suggest a low-cost route for a new engine to become a listed backend: implement the framework's durability interface rather than building a bespoke agent SDK.
- Payload growth from conversation history is a first-order requirement, not an edge case. An engine storing step outputs in Postgres rows needs either a claim-check/offload path or per-step size discipline.
- Exactly-once semantics for irreversible tool effects is still an unsolved research area (Atomix, 2026), so "idempotency keys plus at-least-once steps" remains the practical table stake.

### Gaps
- I found no primary source on Anthropic shipping a durable-execution integration for the Claude Agent SDK, and no primary source for Mastra's or LangGraph's integrations with external engines (beyond their own checkpointers).
- No primary sources found on token/cost accounting or per-tenant LLM concurrency limits as engine features; these appear in vendor marketing but I did not verify any.
- Streaming tokens out of a durable step: claimed by Pydantic AI ("full support for streaming") and Vercel WDK, but I did not fetch primary docs explaining the mechanism (e.g. side-channel streams vs. persisted chunks). Inngest/Restate streaming not verified.
- The OpenAI Agents SDK/Temporal GA status beyond "public preview" (as of the docs fetched) is unconfirmed.

## Q2. Recurring pain points

### Takeaway
The same complaints repeat across 2024-2026: deterministic-replay constraints, upgrading in-flight runs (versioning), payload and history size limits, infrastructure weight/cost of self-hosting Temporal at scale, and weak dashboards/observability in the newer lightweight engines. Postgres-backed engines draw a different set of complaints: vacuum/bloat, independent worker scaling, and "just use a database" growing into a home-made workflow engine.

### Cited Findings
**Determinism**
- Replay-based engines re-execute from the top and reuse stored results; control flow must be deterministic, while side effects run once and are memoized. Subtle bugs arise when control flow depends on mutable state (e.g. points deducted in a previous attempt before a failed email). Durable functions are "progressable workflows with recovery guarantees, not consistency guarantees" — [Jack Vanlightly, "Demystifying Determinism in Durable Execution", 24 Nov 2025](https://jack-vanlightly.com/blog/2025/11/24/demystifying-determinism-in-durable-execution)
- Temporal pain points listed in a Dec 2025 comparison: everything must be serializable between workflows and activities; deterministic replay "creating friction with nondeterministic AI agent behavior"; versioning makes "deployment safety ... a real design concern"; self-hosting needs Postgres plus Kubernetes; Temporal Cloud raises data-residency questions — [ZenML, 17 Dec 2025](https://www.zenml.io/blog/temporal-alternatives)
- Temporal enforces determinism via `Workflow.now()`/`Workflow.random()` and a determinism checker — [foojay.io, "Durable Execution Is a Property, Not a Product"](https://foojay.io/today/durable-execution-is-a-property-not-a-product/) (date not captured)
- AWS Lambda durable functions also replay from the beginning on retry, not from the checkpoint, which surprised early users — [Classmethod, Dec 2025](https://dev.classmethod.jp/articles/durable-functions-replay-from-beginning/)

**Versioning of in-flight runs**
- On DBOS's "Build durable workflows with Postgres" (HN, 8 Aug 2025), user hanikesn: "It seems currently impossible to properly upgrade inflight workflow without manually forking", and marketing is "too handwavy on the idempotency and determinism constraints" — [HN 44840693](https://news.ycombinator.com/item?id=44840693)
- Mastra time travel fails if the workflow definition changed since the run was recorded (e.g. a renamed step) — [Mastra docs](https://mastra.ai/docs/workflows/time-travel/llms.txt)

**Payload and history limits**
- Temporal: 2 MB default payload limit (fixed at 2 MB on Temporal Cloud); 50 MB cumulative event history, after which the server terminates the workflow; every activity input/output persists in history — [Temporal docs: External Storage](https://docs.temporal.io/external-storage); [Temporal troubleshooting](https://docs.temporal.io/troubleshooting/blob-size-limit-error.md); [Temporal community thread](https://community.temporal.io/t/best-practice-to-handle-large-workflow-activity-payload-blob-size-limit-2mb/9814)
- Temporal's fix: External Storage (claim-check offload), announced 8 Apr 2026, public preview 14 May 2026 — [Temporal changelog](https://temporal.io/changelog)
- Cloudflare Workflows limits: 1 MB event payloads; 1 MB state per step and 100 MB-1 GB per instance; 1,024 steps per workflow (raised 15 Jan 2025; sleeps excluded); `waitForEvent` default timeout 24 h; 5 min CPU per step; 4,500 concurrent instances raised to 10,000 (Oct 2025) — [Cloudflare changelog, 15 Jan 2025](https://developers.cloudflare.com/changelog/post/2025-01-15-workflows-more-steps/); [Cloudflare Workers API](https://developers.cloudflare.com/workflows/build/workers-api). Note: several of these limit values came via a third-party skills summary of the docs ([tessl.io](https://tessl.io/registry/skills/github/jezweb/claude-skills/cloudflare-workflows)); verify against current Cloudflare limits page.
- HN user cmdtab (Aug 2025) called Cloudflare Workflows impractical due to a "6 TCP connection" limit and "128 MB ram" — [HN 44840693](https://news.ycombinator.com/item?id=44840693)

**Self-hosting burden and cost**
- On DBOS's "Building Durable Workflows on Postgres" (HN, ~mid 2026, 359 points): temporal_thr123 says moderate-scale Temporal means "you're going to spend _millions_ on infra"; cyberpunk says "Postgres doesn't scale at all for our workload, so you're into cassandra" with 200+ vCPUs for medium deployments; another called Temporal "poorly designed, slow and ridiculously heavy infra wise" — [HN 48313530](https://news.ycombinator.com/item?id=48313530)
- A vendor-authored benchmark (JobRunr) reports Temporal self-hosted at 13.7 s wall and 83.2 CPU-s vs JobRunr-on-Postgres at 8.4 s and 13.3 CPU-s for the same work — [DEV Community, "Durable Workflows on Postgres: What 'You Don't Need Temporal' Actually Buys You"](https://dev.to/contrite42/durable-workflows-on-postgres-what-you-dont-need-temporal-actually-buys-you-3o0f). Note: vendor-adjacent; treat as indicative.

**Postgres-engine-specific complaints**
- joshka: once you need "retries, backoff, timeouts, cancellation, versioning, visibility, task routing ... the 'just use a database' story becomes 'build a poor copy of a workflow engine'"; nulltrace: queue tables suffer dead-tuple pile-up and visibility-map degradation so "planner thinks it's huge"; pirsquare: simple examples undermine crash correctness; epolanski: the post "assumes all steps to be serializable"; sorentwo (Oban author): CockroachDB support needed "feature detection all over" — [HN 48313530](https://news.ycombinator.com/item?id=48313530)
- jumploops asked how to run "a simple worker app that scales independently" (library-embedded engines blur app and worker); cmdtab: DBOS "hosted service UX and frontend can use a lot of work", while Temporal has superior observability but requires rearchitecting; cmdtab migrated from Graphile Worker to DBOS in "half an hour" — [HN 44840693, Aug 2025](https://news.ycombinator.com/item?id=44840693)

**Debugging / recovery**
- DBOS added workflow "fork": a new workflow ID copies inputs and step results up to a chosen step, then re-executes from there, possibly on a newer code version; used to recover from downstream outages or patch bug-failed runs; also manage workflows (search, pause, cancel, resume) via SQL or web UI — [DBOS blog: handling failures with workflow forks (2025)](https://www.dbos.dev/blog/handling-failures-workflow-forks); [DBOS docs: workflow management](https://docs.dbos.dev/golang/tutorials/workflow-management)

### Inferences
- Versioning in-flight runs is the most durable unsolved pain point; the market's answers are (a) Temporal-style worker versioning/patching, (b) fork-from-step onto new code (DBOS, Mastra), and (c) checkpoint-only engines that sidestep replay determinism. An engine should offer at least explicit version pinning plus a fork/restart-from-step operator tool.
- For a Postgres-embedded engine, users will probe vacuum/bloat behavior of hot queue tables, independent worker scaling, and the dashboard. These are the stated weak spots of the Postgres-only cohort.
- Hard size limits are tolerated only when there is a documented offload path. Users expect a claim-check pattern, and Temporal's 2026 move makes it a table stake.

### Gaps
- Local dev experience, testing (time-skipping test servers, replay tests) and polyglot SDK gaps were not covered by primary sources I fetched; evidence here is thin.
- No quantitative survey data (e.g. % of users citing versioning) was found.

## Q3. Table-stakes features buyers check in evaluations

### Takeaway
2025-2026 comparison articles converge on a checklist: durable recovery model, authoring model (code-first vs DSL/BPMN), AI agent fit, deployment options (managed plus self-host), operational responsibility, observability/recovery tooling, data ownership/residency. Enterprise buyers also expect fine-grained RBAC, priority and fairness in queues, and a Kubernetes/serverless worker story; Temporal shipped several of these in 2026, raising the bar.

### Cited Findings
- Evaluation dimensions (Dec 2025): developer experience and local-dev parity; workflow model (replay, checkpoints, DB state, Kubernetes, BPMN); operational model (managed to self-hosted); AI agent fit; state and storage (data ownership, location); observability and recovery — [ZenML, 17 Dec 2025](https://www.zenml.io/blog/temporal-alternatives)
- Evaluation dimensions (Jul 2026): "durable recovery, workflow model, AI and Agent support, deployment options, operational responsibility, and best-fit workload"; teams leave Temporal "not because durability is unimportant, but because they want a different authoring model, operating model, workload focus, or governance layer"; Diagrid positions on governance (identity, MCP policy, verifiable execution, "cloud-to-air-gapped governance") — [Diagrid, 15 Jul 2026](https://www.diagrid.io/infrastructure/10-best-temporal-alternatives-2026). Note: vendor-authored.
- Alternatives and their pitched differentiators (Dec 2025): Restate (virtual objects without Temporal's deterministic model), DBOS (Postgres-native state, transactional guarantees), Inngest (event-driven `step.run()`), Hatchet (queue-based tasks with fine-grained concurrency and worker control), Trigger.dev (checkpoint-resume beyond serverless timeouts), Argo, Azure Durable Functions, Camunda (BPMN, human-in-the-loop) — [ZenML](https://www.zenml.io/blog/temporal-alternatives)
- Temporal's 2026 enterprise feature drops set the bar: Task Queue Priority & Fairness GA (5 May 2026); Worker Controller GA (4 May 2026); Custom Roles pre-release for granular permissions (25 Jun 2026); Projects for organizing Cloud resources (7 Aug 2026); serverless workers on AWS Lambda (public preview, 3 Aug 2026) and Google Cloud Run (6 Aug 2026); Standalone Activities GA across all six SDKs (15 Sep 2026) — [Temporal changelog](https://temporal.io/changelog)
- OTel observability support was cited by an HN user as a DBOS positive; Temporal's observability/UI was cited as superior to the newer engines — [HN 44840693, Aug 2025](https://news.ycombinator.com/item?id=44840693)
- An HN user praised Restate exposing its storage so it could be hooked to Metabase for dashboards, i.e. SQL-queryable state is valued — [HN 48313530](https://news.ycombinator.com/item?id=48313530)
- Lambda durable functions bundle retries, waits up to one year, and idempotent start by execution name as base features — [AWS docs](https://docs.aws.amazon.com/en_us/lambda/latest/dg/durable-functions.html)

### Inferences
- Table stakes in 2026: retries with backoff and timeouts; durable timers and long sleeps; signals/events with timeouts (human approval); schedules/cron; concurrency limits and priority/fairness per key/tenant; idempotent start; cancellation; a web UI with search, pause/resume/cancel and step-level inspection; OTel traces/metrics; payload encryption hooks; managed offering or a credible "runs on your existing Postgres" story.
- Enterprise tier expectations: RBAC with custom roles, SSO, audit trails, data residency/air-gapped options, multi-tenant isolation (namespaces/projects).
- "Queryable by SQL" is a differentiator Postgres-native engines can claim cheaply.

### Gaps
- No primary analyst report (Gartner/Forrester) on durable execution was found. Compliance-specific requirements (SOC 2, HIPAA, PII erasure) were not sourced in this pass.
- Schedules/cron and multi-tenancy were inferred from feature lists rather than explicit buyer-evaluation sources.

## Q4. Emerging differentiators

### Takeaway
The category's frontier in 2025-2026: (1) serverless/push execution where the platform invokes the code (Lambda durable functions, Temporal serverless workers, Cloudflare, Vercel); (2) Postgres-only or "library, not a server" architectures (DBOS, Postgres-native offerings, many Rust crates); (3) fork/time-travel from a step; (4) durable state/actors/entities for agent sessions; (5) transactional semantics extending beyond single steps (DBOS/MIT "AC/DC", Atomix); (6) WASM-based transparent durability (Golem).

### Cited Findings
- Serverless/push: AWS Lambda durable functions (Dec 2025) with suspended waits billed at zero compute — [AWS docs](https://docs.aws.amazon.com/en_us/lambda/latest/dg/durable-functions.html); Temporal serverless workers for Lambda (3 Aug 2026) and Cloud Run (6 Aug 2026), where "Temporal [controls] the scaling" — [Temporal changelog](https://temporal.io/changelog)
- Postgres-only as library: DBOS writes each workflow input and step output into its own tables in your existing Postgres — [HN 48313530](https://news.ycombinator.com/item?id=48313530); [DEV Community](https://dev.to/contrite42/durable-workflows-on-postgres-what-you-dont-need-temporal-actually-buys-you-3o0f)
- Microsoft published "Introducing Durable Functions in PostgreSQL" on the Azure Database for PostgreSQL blog — [Microsoft Tech Community](https://techcommunity.microsoft.com/blog/adforpostgresql/introducing-durable-functions-in-postgresql/4526821). Note: only the title loaded; the content and date are unverified, but the title signals durable execution moving into the database itself.
- Fork/time-travel: DBOS fork-from-step, also from the web UI — [DBOS blog](https://www.dbos.dev/blog/handling-failures-workflow-forks); Mastra `timeTravel` with modified input and streamed events — [Mastra docs](https://mastra.ai/docs/workflows/time-travel/llms.txt); Kitaru "full-run agent replay" against recorded tool history — [ZenML](https://www.zenml.io/blog/temporal-alternatives)
- Durable state/actors: Jack Vanlightly classifies durable functions into three forms: stateless functions, sessions and actors (10 Dec 2025), and describes a "durable function tree" of promises and continuations (4 Dec 2025) — [Vanlightly blog index](https://jack-vanlightly.com/blog); Microsoft Agent Framework models each agent session as a durable entity — [Microsoft Learn](https://learn.microsoft.com/en-us/azure/durable-task/sdks/durable-agents-microsoft-agent-framework); Restate virtual objects are durable handlers plus isolated K/V state, one handler at a time per object — [restate-sdk docs.rs](https://docs.rs/restate-sdk)
- Transactions across workflows: Stonebraker, Zhou, Kraft and Li (CIDR 2026) argue ACID must extend from transactions to whole workflows (atomic, consistent, durable, correct: "AC/DC"); a prototype DB-oriented system supports both physical backout and saga compensation; transactional workflows win under low contention, sagas under contention or long steps — [CIDR 2026 paper](https://www.vldb.org/cidrdb/papers/2026/p9-stonebraker.pdf)
- WASM: Golem provides transparent durability by taking over execution of code compiled to WebAssembly, with exactly-once semantics claimed — [Golem docs](https://learn.golem.cloud/docs/technical-details)
- Coordination-avoiding shared-log architecture is Restate's stated design basis — [Restate blog: architecture](https://restate.dev/tags/architecture)
- Temporal "Standalone Activities" (GA 15 Sep 2026) let activities run without a workflow, i.e. durable job/queue use without the workflow model — [Temporal changelog](https://temporal.io/changelog)

### Inferences
- "Embedded library on your existing Postgres" is a recognized and growing category, validated by DBOS's HN traction and Microsoft's Postgres work. Its winning argument is transactional coupling with application data (exactly-once step plus business write in one transaction, outbox for free).
- Temporal adding standalone activities and priority/fairness signals convergence with queue-first engines (Hatchet, Inngest). A durable engine is now expected to double as a durable task queue.
- Fork-from-step on new code is becoming the practical answer to both debugging and versioning.

### Gaps
- Realtime streaming to clients (Inngest Realtime, Trigger.dev Realtime) and event-driven triggers were not verified with primary sources in this pass.
- No primary evidence gathered on WASM adoption numbers or Golem traction.

## Q5. Rust ecosystem: options in 2026 and what Rust users ask for

### Takeaway
Rust went from "Temporal Core is written in Rust but has no Rust SDK" to a GA Temporal Rust SDK (4 Sep 2026), alongside Restate's Rust SDK, Golem (WASM) and a crowd of small embedded crates. Several of these are embedded, SQL-backed and checkpoint-based, which overlaps directly with a Rust/Postgres-embedded engine, including one that uses Diesel.

### Cited Findings
- Temporal Rust SDK: public preview 7 May 2026; generally available 4 Sep 2026 — [Temporal changelog](https://temporal.io/changelog); [Temporal changelog: Rust SDK public preview](https://temporal.io/changelog/rust-sdk-public-preview). Temporal's Core SDK (Rust) underpins the TypeScript, Python, .NET and Ruby SDKs — [temporalio/sdk-rust](https://github.com/temporalio/sdk-rust); [Temporal blog: Why Rust powers Core SDK](https://temporal.io/blog/why-rust-powers-core-sdk)
- Restate Rust SDK supports services, virtual objects (durable handlers plus isolated K/V state, single-writer per key) and workflows — [docs.rs restate-sdk](https://docs.rs/restate-sdk)
- Golem: durability for Rust (and other languages) compiled to WASM/WASI — [Golem Rust durability guide](https://learn.golem.cloud/docs/rust-language-guide/durability)
- Smaller Rust crates found (Oct 2026 search):
  - flawless: durable execution engine with a `workflow` macro — [crates.io](https://crates.io/crates/flawless); [flawless docs](https://flawless.dev/docs/)
  - ergon: durable execution library inspired by Gunnar Morling's Persistasaurus — [GitHub](https://github.com/richinex/ergon)
  - iopsystems/durable: engine that runs workflows to completion across restarts and updates — [GitHub](https://github.com/iopsystems/durable)
  - Sayiir: "checkpoints after each task ... no deterministic replay, no DSLs, no separate infrastructure" — [GitHub](https://github.com/sayiir)
  - durare: a DBOS-compatible durable-execution SDK for Rust, checkpointing each step to your database — [GitHub](https://github.com/SamuelXing/durare)
  - durable-workflows: workflows, activities, timers, approvals and cron schedules stored as rows in your own MySQL or Postgres via Diesel, with an in-process runtime — [GitHub](https://github.com/steventhanna/durable-workflows)

### Inferences
- The Rust space now has a heavyweight (Temporal Rust SDK GA) and many embedded crates; a Rust/Postgres-embedded engine competes most directly with durare (DBOS-compatible) and durable-workflows (Diesel, Postgres/MySQL). Differentiation will come from operational maturity (UI, versioning, retention, partitioning, encryption, erasure), not from the core idea.
- Two Rust crates (Sayiir, flawless-style macros) market "no deterministic replay" as a feature; checkpoint-per-step models are what Rust users seem to prefer, because idiomatic async Rust code is hard to constrain to deterministic replay.
- DBOS-protocol compatibility (durare) suggests a possible interop expectation: shared system-table schemas across languages.

### Gaps
- I found no primary forum evidence (Reddit/r/rust, users.rust-lang.org, GitHub issues) of what Rust users specifically ask for (tokio integration, no_std/WASM, compile-time determinism checks). These remain unverified hypotheses.
- Maturity, adoption and maintenance status of the smaller crates were not assessed.

## Q6. Research and academia (2023-2026)

### Takeaway
Academic work has moved from "durable execution as a runtime" toward "durable execution plus transactional correctness", especially for agentic tool use. The DBOS/MIT line remains the most cited database-community thread; agent-reliability papers in 2026 explicitly treat durability as a baseline and focus on effect isolation.

### Cited Findings
- Stonebraker, Zhou, Kraft, Li, "Consistency and Correctness in Data-Oriented Workflow Systems", CIDR '26 (18-21 Jan 2026, Santa Cruz): "Although many developers can write and test a saga, few get it right when the server crashes"; durable execution "guarantees exactly-once execution of workflow steps and ensures that compensations actually run"; "durability alone is not sufficient"; proposes AC/DC workflows; each step should be a transaction; serverless PaaS step graphs (Lambda/Step Functions) are the motivating model; AI-agent steps are explicitly in scope — [CIDR 2026 PDF](https://www.vldb.org/cidrdb/papers/2026/p9-stonebraker.pdf)
- DBOS originated as an academic project; time-travel debugging and workflow semantics were carried forward into the DBOS library — [Wikipedia: DBOS](https://en.wikipedia.org/wiki/DBOS)
- Mohammadi, Potamitis, Klein, Arora, Bindschaedler, "Atomix: Timely, Transactional Tool Use for Reliable Agentic Workflows", arXiv 2602.14849 (16 Feb 2026, rev. 29 May 2026): progress-aware transactions that seal and commit agent tool effects only after earlier conflicting work is exhausted — [arXiv](https://arxiv.org/abs/2602.14849)
- Related 2026 arXiv agent-harness papers surfaced by search (not read): "OneDayAgent: Towards a Long-Horizon Harness for Autonomous Agents" — [arXiv 2608.05013](https://arxiv.org/pdf/2608.05013); "The Horizon Gap: Planning, Memory, Execution, Training, and Evaluation for Long-Horizon LLM Agents" — [arXiv 2608.06663](https://arxiv.org/pdf/2608.06663); "Always-On Agents: A Survey of Persistent Memory, State, and Governance in LLM Agents" — [arXiv 2606.30306](https://arxiv.org/pdf/2606.30306); "Mnemosyne: Agentic Transaction Processing for Validating and Repairing AI-generated Workflows" — [arXiv 2607.00269](https://arxiv.org/pdf/2607.00269)
- Practitioner theory: Jack Vanlightly's series "Coordinated Progress" parts 1-4 (11 Jun 2025), "Responsibility Boundaries in the Coordinated Progress Model" (15 Jul 2025), "Demystifying Determinism in Durable Execution" (24 Nov 2025), "The Durable Function Tree" parts 1-2 (4 Dec 2025), "The Three Durable Function Forms" (10 Dec 2025) — [Vanlightly blog](https://jack-vanlightly.com/blog)

### Inferences
- The academic direction (workflow-level transactions, effect isolation) favors engines co-located with the application database. A Postgres-embedded engine can offer "step plus business write in one transaction", which is the primitive these papers build on.

### Gaps
- I did not locate or verify Netherite (Microsoft Research, VLDB 2022, pre-window), a formal "durable functions semantics" paper, or a Restate whitepaper in this pass. No OSDI/SOSP 2024-2026 durable-execution paper was found in my searches.
- The 2026 arXiv agent-harness papers listed above were not read. Their relevance to durable execution is assumed from titles only.
