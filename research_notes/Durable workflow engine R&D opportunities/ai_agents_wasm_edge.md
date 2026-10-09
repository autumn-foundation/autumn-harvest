# Emerging Domains for Durable Execution: AI Agents, WASM, Edge/Serverless (as of Oct 2026)

Source-quality legend used below: **[primary]** = vendor docs/blog/spec/repo; **[secondary]** = third-party blog or aggregator (treat numbers with care); **[competitor claim]** = a vendor talking about a rival. Status tags: **GA**, **Preview**, **Pre-release**, **Experimental**, **Claimed/Hype** (marketing assertion with no evidence I could verify).

## Q1. How are durable-execution vendors and agent frameworks positioning durable execution for AI agents, and what problems do they claim to solve?

### Takeaway
By mid-2026 almost every durable-execution vendor and serverless cloud ships an "agent" integration that works the same way. Each LLM call and each tool call becomes a journaled step or activity. The deterministic orchestration loop replays from that journal. The problems vendors claim to solve are now common ground: crash recovery mid-loop without re-paying for tokens, zero-cost human-in-the-loop waits, long tool calls, and durable token streaming to UIs. Vendors are now competing on large payloads, sandboxes, streaming and serverless hosting, not on basic durability.

### Cited Findings
**Temporal**
- OpenAI and Temporal released an OpenAI Agents SDK integration (first a Public Preview). OpenAI made `Runner` an abstract base class so that Temporal could supply a Runner that runs each agent invocation as a Temporal Activity, with orchestration running as a Workflow. — [Temporal blog, primary](https://temporal.io/blog/announcing-openai-agents-sdk-integration)
- At Replay 2026 (May 6, 2026), Temporal announced: OpenAI Agents SDK integration **GA**, which "integrates with the Agent SDK's new sandbox functionality" so agents run in isolated environments with durability; Google ADK integration **GA** ("LLM calls and tool executions run as Temporal Activities"); **Workflow Streams** (Public Preview), described as "durable streaming that uses Temporal's Signal & Update primitives" for token batches; **External Payload Storage** (Public Preview, Python/Go); **Serverless Workers** on AWS Lambda (Pre-release); **Standalone Activities** (Public Preview Go/Python/.NET); a Rust SDK (Public Preview); and **Principal Attribution** (Pre-release), a "server-derived, non-spoofable field in your Workflow history events" for audit. — [Temporal Replay 2026 announcements, primary](https://temporal.io/blog/replay-2026-product-announcements)
- External Storage uses the claim-check pattern to bypass "the existing 2 MB per-payload size limit" and keep history sizes manageable. It offloads payloads to S3 and keeps a reference token in history, "with zero changes to your application code". — [Temporal changelog, primary](https://temporal.io/changelog/external-storage-public-preview)
- Workflow Streams batching reportedly "defaults to 2 seconds and drops to 100ms for AI integrations". — [byteiota, secondary](https://byteiota.com/temporal-workflow-streams-stream-ai-agent-output-in-real-time/). I could not verify this on a Temporal page.
- Pydantic AI has a native `TemporalAgent` wrapper. It turns an agent's model requests, tool calls and MCP calls into Temporal Activities, because "agent calls aren't deterministic". — [Temporal blog, primary](https://temporal.io/blog/build-durable-ai-agents-pydantic-ai-and-temporal); [Pydantic docs, primary](https://pydantic.dev/docs/ai/capabilities/durable_execution/temporal/)
- Temporal published a post arguing that dynamic, LLM-directed agents can be built on Temporal. It answers critics who say a deterministic workflow cannot host a model-driven loop. — [Temporal blog, primary](https://temporal.io/blog/of-course-you-can-build-dynamic-ai-agents-with-temporal)

**AWS Lambda durable functions** (announced at re:Invent 2025)
- The SDK adds `context.step()`, `context.waitForCallback()` and `context.waitForCondition()`. Each agent invocation is wrapped in a step, so after a failure, replay skips completed agent steps "avoiding redundant token costs and API calls". For human-in-the-loop: "Because the function is fully suspended, it incurs no compute charges during the review window, whether that's 10 minutes or 3 days." The model is replay from the start that skips checkpointed operations. — [AWS Compute blog, Jun 29 2026, primary](https://aws.amazon.com/blogs/compute/building-fault-tolerant-multi-agent-ai-workflows-with-aws-lambda-durable-functions/)
- Java support is reported GA. — [javierinthecloud, secondary](https://www.javierinthecloud.com/p/fcb36ccc-77a8-478a-a7c6-48d380fb47ac/); also see [re:Post deep dive](https://repost.aws/articles/ARc8wmu4l9TKywZCHX-_nn6w/re-invent-2025-deep-dive-on-aws-lambda-durable-functions)

**Vercel Workflow**
- Vercel Workflows is **GA** for TypeScript and Python, with "no orchestrator, no Kubernetes". The `"use workflow"` and `"use step"` directives mark code. `getWritable()` gives a persistent stream that clients can reconnect to and resume "from any point". A `DurableAgent` makes each tool execution "a retryable, observable step". — [Vercel docs, primary](https://vercel.com/docs/workflows); [Vercel blog, primary](https://vercel.com/blog/a-new-programming-model-for-durable-execution); [WorkflowAgent KB, primary](https://vercel.com/kb/guide/what-is-workflowagent)
- Vercel has a guide for running a Claude Managed Agent inside Vercel Workflow. — [Vercel KB, primary](https://vercel.com/kb/guide/claude-managed-agent-vercel)

**Inngest**
- AgentKit provides multi-agent networks, a router, shared network state as memory, and MCP servers as tools. `step.ai.infer` proxies the LLM call so that "a long inference does not burn serverless billable seconds". — [Inngest blog, primary](https://www.inngest.com/blog/ai-orchestration-with-agentkit-step-ai)
- The `useAgent` React hook (Sept 2025) streams durable workflow state to the browser. Inngest also markets "durable token streaming", which decouples execution from the websocket so a dropped connection does not mean "paying for the same tokens twice". — [Inngest blog, primary](https://www.inngest.com/blog/agentkit-useagent-realtime-hook); [Inngest on X, primary](https://x.com/inngest/status/2082156631215710218)

**Cloudflare**
- Agents SDK v0.3.7 (Feb 2026) adds first-class Workflows integration. Agents (Durable Objects) hold WebSocket connections and state. Workflows handle long-running tasks, retries and human-in-the-loop steps. — [Cloudflare changelog, primary](https://developers.cloudflare.com/changelog/post/2026-02-03-agents-workflows-integration)

**DBOS, Restate, Golem, LangGraph, Microsoft**
- DBOS is a library that checkpoints the workflow log into the user's own Postgres. — [HackerNoon comparison, secondary](https://hackernoon.com/durable-execution-for-ai-agents-langgraph-dbos-inngest-and-temporal-compared)
- LangGraph checkpoints graph state at every super-step. It ships InMemorySaver, SqliteSaver and PostgresSaver. — [LangChain docs, primary](https://docs.langchain.com/oss/python/langgraph/use-time-travel); [AWS DynamoDB checkpointer blog, primary](https://aws.amazon.com/blogs/database/build-durable-ai-agents-with-langgraph-and-amazon-dynamodb/)
- Microsoft Agent Framework uses the Pregel/superstep model and snapshots state into a `WorkflowCheckpoint` at each superstep. — [zylos research, secondary](https://zylos.ai/research/2026-04-24-durable-execution-agent-runtimes/). Production durability relies on the Azure Durable Task Extension. — [Diagrid, competitor claim](https://www.diagrid.io/blog/still-not-durable-how-microsoft-agent-framework-and-strands-agents-repeat-the-same-mistake)
- Golem now calls itself "the durable agent runtime". — [golem.cloud, primary](https://golem.cloud/)
- A secondary source says Inngest AgentKit, DBOS, Restate, Vercel Workflow DevKit, Cloudflare Workflows and AWS Bedrock AgentCore "landed in 2025 around the same shape". — [reactify-solutions, secondary](https://www.reactify-solutions.com/articles/durable-ai-agents-2026)

### Inferences
- The integration pattern ("model call = activity, loop = workflow") is now a commodity. A new engine gains nothing from offering it alone. It must differentiate on what that pattern does poorly: history size, streaming, payload size, sandboxing and replay cost.
- Hosted platforms now treat durable streaming as table stakes: Temporal Workflow Streams, Vercel `getWritable()` and Inngest `useAgent`. Temporal builds streaming on Signals/Updates with batching. That suggests streaming is not first-class in its history model. A native, resumable, offset-addressable stream primitive that sits next to the journal and outside it is a design opening.
- OpenAI changed its SDK (Runner as an abstract base class) to allow durable backends. Agent frameworks now expose "runner" seams for durable engines. Shipping adapters for these seams (OpenAI Agents SDK, Pydantic AI, Google ADK, Vercel AI SDK, LangGraph) is how engines get adopted.

### Gaps
- I did not get primary details for Restate's AI agent and Vercel AI SDK integration, Trigger.dev's agent features, Mastra's workflow durability, AWS Bedrock AgentCore's durability model, Google ADK's native session or durability story, or Semantic Kernel's Process Framework. I hit the tool-call budget. These need a follow-up pass.
- I could not verify the reported AWS Lambda durable function maximum duration (often cited as up to 1 year) or checkpoint size limits from a primary page.

## Q2. How do MCP long-running tasks and A2A protocols intersect with durable execution?

### Takeaway
MCP now has a standard async-task primitive, SEP-1686 "Tasks". It shipped as experimental in the 2025-11-25 spec. In the 2026-07-28 spec it became the official extension `io.modelcontextprotocol/tasks`, with non-blocking elicitation and cancellation. The protocol defines a task state machine and polling, but it says nothing about server-side durability, retries or exactly-once execution. That gap is exactly what a durable engine fills. A2A has a similar task model (polling, SSE, push notifications), and engines such as Conductor already put A2A calls behind durable workflow state.

### Cited Findings
- SEP-1686 adds a general Tasks capability for long-running operations, generalized to all JSON-RPC requests. Tasks are bidirectional: either client or server can create them. — [SEP-1686 issue, primary](https://github.com/modelcontextprotocol/modelcontextprotocol/issues/1686)
- The motivation: MCP tool calls are synchronous with a default 60 s request timeout (`DEFAULT_REQUEST_TIMEOUT_MSEC = 60000`). Long builds, log searches and inference were therefore unusable, and it gets worse behind gateways that add their own timeouts. — [claude-code issue #76571](https://github.com/anthropics/claude-code/issues/76571); [claude-code issue #52137](https://github.com/anthropics/claude-code/issues/52137)
- SEP-1686 "shipped experimentally in the 2025-11-25 spec, went through a year of production use, and the SEP itself now carries a Final, Standards Track status". A July 2026 article reports that Tasks gained retry semantics and expiry policies. — [bex.co, secondary](https://bex.co/blog/2026/07/08/mcp-tasks-retry-expiry-async-deploys)
- Changes between 2025-11-25 and 2026-07-28 (AAIF blog, Aug 28 2026): Tasks moved to the extension namespace `io.modelcontextprotocol/tasks`. States are `working`, `input_required` and `completed`, and 2026-07-28 adds `failed` and `cancelled`. A JSON-RPC error means `failed`, but a tool result with `isError: true` is still `completed`. Clients poll `tasks/get`, and servers return a task handle with a TTL. Elicitation is "no longer blocking": input requests come back inline in the `tasks/get` response. Cancellation is "non-blocking and non-guaranteed". The article does **not** mention durability, retry semantics or durable execution engines. — [AAIF blog, primary-ish (Linux Foundation AAIF)](https://aaif.io/blog/handle-long-running-mcp-tool-calls-with-the-mcp-tasks-extension)
- Client support lags. Open feature requests ask Claude Code and Claude Desktop to implement SEP-1686 client support. — [claude-code #18617](https://github.com/anthropics/claude-code/issues/18617); [#52137](https://github.com/anthropics/claude-code/issues/52137). The TypeScript SDK has its own tracking issue. — [typescript-sdk #1060](https://github.com/modelcontextprotocol/typescript-sdk/issues/1060). FastMCP implements background tasks. — [FastMCP docs](https://gofastmcp.com/v2/servers/tasks)
- A2A uses SendMessage/GetTask/CancelTask. Long tasks get updates by polling, SSE streaming, or push notifications to a client-supplied callback. — [Instaclustr, secondary](https://www.instaclustr.com/blog/scaling-agent-systems-with-kafka-and-a2a-part-3-agent2agent-protocol-explained/)
- Conductor OSS documents an A2A integration where "a remote agent call survives a server crash, restart, or redeploy… the call's state lives in the workflow execution, not in a thread". — [Conductor docs, primary](https://docs.conductor-oss.org/devguide/ai/a2a-integration.html)
- Temporal Nexus (GA for Python) offers durable workflow-to-workflow and service-to-service calls across namespaces. It is a proprietary analogue to cross-agent async calls. — [Temporal Replay 2026, primary](https://temporal.io/blog/replay-2026-product-announcements)

### Inferences
- MCP Tasks and A2A tasks map almost one-to-one onto durable workflow concepts. Task id = workflow id. `input_required` = signal/await. `cancelled` = cancellation. TTL = retention. An engine could expose any workflow as an MCP Task server or an A2A agent with no glue code. It could also make outbound MCP/A2A task calls durable awaitables (submit, journal the task id, suspend, resume on poll or push). That is a concrete, near-term R&D opportunity.
- Cancellation is "non-guaranteed" and polling uses TTL handles. The protocol therefore needs idempotent task creation (resubmit after a crash without a duplicate) and durable task storage on the server. A crashed MCP server that loses in-flight tasks breaks the contract. Durable engines can sell "spec-compliant, crash-safe Tasks".
- An `isError: true` result counts as `completed`, so engines must not treat tool-level errors as retryable failures by default. This retry-classification detail matters for adapters.

### Gaps
- I did not read the 2026-07-28 spec text directly. Exact TTL, retry and expiry fields come from secondary summaries.
- I found no primary source showing Temporal, Restate, DBOS or Inngest shipping a first-party "workflow as MCP Task server" adapter.

## Q3. Which emerging needs appear (token/cost accounting, LLM-call caching in the journal, recorded-output replay, eval via replay, sandboxes, durable streaming, fork/time travel, agent memory)?

### Takeaway
Some of these needs are shipped somewhere: durable streaming (Temporal, Vercel, Inngest), sandboxed steps (OpenAI Agents SDK + Temporal), fork/time travel (LangGraph), audit journals (Golem, Temporal Principal Attribution) and large-payload offload (Temporal). Others are mostly unshipped in journal-replay engines: fork-from-step, per-step token/cost accounting, semantic caching of LLM calls in the journal, and evaluation by replaying production histories against new prompts or models. These are the clearest R&D gaps.

### Cited Findings
- **Fork/time travel (shipped in LangGraph)**: LangGraph can resume from a prior checkpoint with modified state, and "resuming past execution produces a new fork in history". Nodes before the checkpoint are not re-executed. Nodes after it re-execute, "including any LLM calls, API requests, and interrupts". — [LangChain docs, primary](https://docs.langchain.com/oss/python/langgraph/use-time-travel)
- Practitioners use LangGraph time travel to debug non-deterministic agents. — [DEV Community, secondary](https://dev.to/sreeni5018/debugging-non-deterministic-llm-agents-implementing-checkpoint-based-state-replay-with-langgraph-5171)
- Temporal's Replay 2026 list includes **no** replay, time-travel or reset announcement. It also has nothing for Pydantic AI, Vercel AI SDK, LangGraph or MCP. — [Temporal Replay 2026, primary](https://temporal.io/blog/replay-2026-product-announcements) (absence noted from my read of the post)
- **Recorded LLM outputs are mandatory for journal replay**: "you must intercept every LLM call and memoize the response, because replaying the same prompt will likely produce a different answer… This memoization layer adds storage and code complexity." — [vadim.blog, Jul 2026, secondary](https://vadim.blog/durable-execution-llm-agents/)
- **Snapshot vs journal trade-off**: the author estimates snapshots at "2–5 KB" per step, "a few hundred kilobytes" over 100 steps, and "tens of milliseconds per event" overhead. He suggests snapshots suit runs under 10 minutes on one node, and journals suit runs over an hour or across nodes. He labels these "rough, un-benchmarked numbers". — [vadim.blog, secondary](https://vadim.blog/durable-execution-llm-agents/)
- Agent workflows contain non-determinism from "LLM outputs, timestamps, randomness, retrieval results, network responses, policy decisions, and tool results". Each must be recorded the first time and reused. — [Medium "Agent Workflows Are Rediscovering Durable Execution", secondary](https://nittikkin.medium.com/agent-workflows-are-rediscovering-durable-execution-be110661ed8c)
- **Sandboxed code steps**: the Temporal + OpenAI Agents SDK GA integration supports the SDK's "sandbox functionality" with durability. — [Temporal, primary](https://temporal.io/blog/replay-2026-product-announcements). Golem pitches per-agent WASM sandboxes, a per-agent filesystem and embedded SQL. — [Golem, primary](https://golem.cloud/blog/the-rise-of-the-agent-runtime/)
- **Agent memory in workflow state**: Inngest AgentKit uses "shared network state as memory". — [Inngest, primary](https://www.inngest.com/blog/ai-orchestration-with-agentkit-step-ai). Golem gives each agent a filesystem and SQLite (TypeScript today), with CozoDB graph memory "shortly after 1.6". — [Golem, primary](https://golem.cloud/blog/the-rise-of-the-agent-runtime/)
- **Cost accounting**: Temporal announced a Billing API (Public Preview, "coming soon") for "chargeback". It is account-level billing, not per-step token accounting. — [Temporal, primary](https://temporal.io/blog/replay-2026-product-announcements). Inngest's `step.ai.infer` is a billing optimization: the LLM wait does not burn serverless seconds. — [Inngest, primary](https://www.inngest.com/blog/ai-orchestration-with-agentkit-step-ai)
- **Audit and compliance**: Golem claims runtime-produced audit journals that meet EU AI Act Article 12. — [Golem, primary, Claimed](https://golem.cloud/blog/the-rise-of-the-agent-runtime/). Temporal Principal Attribution adds a non-spoofable principal to history events (Pre-release). — [Temporal, primary](https://temporal.io/blog/replay-2026-product-announcements). Golem 1.5 adds OpenTelemetry export and oplog-processor plugins that receive each oplog entry exactly once. — [Golem 1.5 OTLP, primary](https://golem.cloud/blog/golem-1-5-features-part-14-opentelemetry/)
- **Replay-resistant authorization**: an Aug 2026 arXiv paper argues that agent actions need durable authorization state so replayed or resumed actions cannot reuse single-use tokens. — [arXiv 2608.01710](https://arxiv.org/pdf/2608.01710). Another paper proposes verified detection of concurrency anomalies in multi-agent LLM systems. — [arXiv 2606.17182](https://arxiv.org/pdf/2606.17182). I read titles only, not the full papers.

### Inferences
- **Fork-from-step on a journal engine (R&D)**: journal engines already store every LLM output and tool result. Branching a new execution from event N, with edited state or a new prompt, is a natural extension. It is less well served in Temporal-style engines than in LangGraph's snapshot model (Temporal has "reset" to a point, but no first-class fork tree was announced in 2026).
- **Replay-as-evaluation (R&D)**: recorded histories are ready-made regression fixtures. Replay with recorded tool outputs but a *live* new model or prompt, then diff the decisions, is a natural capability. I found no engine that ships it as a product feature.
- **Per-step token/cost metering in the journal (R&D)**: no source shows an engine recording tokens, model id and cost as first-class event metadata, or enforcing per-run token or cost budgets. Rate limits and token budgets would need fairness and priority keyed on tokens. Temporal's Priority & Fairness is GA but keyed on tasks.
- **Semantic/content-addressed LLM caching inside the journal (R&D)**: memoization exists only for exact replay of the same run. Cross-run caching (same prompt hash leads to the same recorded output) is unaddressed in the sources.
- **History growth**: agent loops create many large events (full context windows). Claim-check offload treats the symptom. Delta-encoding of context windows, or snapshot-plus-journal hybrids (vadim.blog's crossover), are open design work.

### Gaps
- I found no primary source for semantic caching of LLM calls, per-step token accounting, or replay-based evaluation in any durable engine. That absence is the finding, but a broader search might reveal smaller players.
- I did not find whether Restate, DBOS or Inngest have fork/rewind features.

## Q4. What does WASM-based durable execution enable, and what limits remain?

### Takeaway
Golem is the flagship WASM durable runtime. It records every host-function interaction in an oplog and replays the WASM instance deterministically. This gives "automatic" durability without SDK step boundaries, suspend-to-zero, megabyte-class isolation, and polyglot support through the component model. In 2026 Golem repositioned itself as a full "agent runtime" (sandbox, capabilities, secrets, audit). Polyglot support is uneven, and the remaining limits I could verify are mostly about language and feature coverage.

### Cited Findings
- Golem gives WASM components fault tolerance by recording all host function interactions in a durable oplog, so execution "resume[s] precisely from the point of interruption". — [Golem about, primary](https://golem.cloud/about/)
- A Golem agent is "a stateful sandbox per identity — a WASM instance measured in megabytes, suspend-to-zero (idle agents consume no memory), resumed deterministically by replaying its own oplog". — [Golem "Rise of the Agent Runtime", Jun 10 2026, primary](https://golem.cloud/blog/the-rise-of-the-agent-runtime/)
- Golem's seven runtime primitives are isolation, capability-based authorization ("cards" with upper bounds), host-enforced tool mediation, opaque secret handles, durability, memory (per-agent FS and SQL) and auditability. Its argument: "only capability denial at the host boundary cannot be circumvented by the agent's code." — [Golem, primary](https://golem.cloud/blog/the-rise-of-the-agent-runtime/)
- Golem criticizes others. It says Temporal, Restate and Inngest record actions but cannot make authorization decisions ("Temporal is not a runtime"). It calls Cloudflare Workers' 128 MB isolate cap "hopeless" for agents that must hold working memory and install dependencies. — [Golem, competitor claim](https://golem.cloud/blog/the-rise-of-the-agent-runtime/)
- Limitation: per-agent SQL is "available today to TypeScript agents", and graph memory comes after 1.6, so polyglot parity is incomplete. — [Golem, primary](https://golem.cloud/blog/the-rise-of-the-agent-runtime/)
- Golem 1.5 (end of April 2026) shipped mature oplog-processor plugins. These are agents that receive batches of oplog entries, each delivered exactly once, plus OpenTelemetry export. — [Golem 1.5 Part 14, primary](https://golem.cloud/blog/golem-1-5-features-part-14-opentelemetry/); [vigoo blog, primary](https://blog.vigoo.dev/posts/golem15-part14-otlp/)
- Golem positions itself as an engine for coding agents. — [Golem blog, primary](https://golem.cloud/blog/golem-as-a-coding-agent-engine/)
- An open issue tracks exactly-once semantics for `golem:rdbms`. Transparent host-call durability does not automatically give exactly-once for external database side effects. — [golemcloud/golem #1514, primary](https://github.com/golemcloud/golem/issues/1514)

### Inferences
- WASM's distinctive offers are: (1) no explicit step annotations, because every host call is journaled; (2) a deterministic sandbox, which removes most replay non-determinism bugs; (3) cheap suspend-to-zero; (4) a natural place to enforce capabilities and audit. Points 1 and 4 matter most for *agent-generated code*, where nobody can be trusted to annotate steps correctly.
- Remaining limits I could infer: exactly-once with external side effects still needs per-integration work (issue #1514); polyglot feature parity lags; the component-model toolchain for non-Rust languages is still maturing (not directly verified); and journaling every host call costs more oplog volume than coarse steps.
- R&D opportunity: a hybrid model. Coarse, explicit steps in normal workers, plus a WASM sandbox step type for untrusted, LLM-generated code with host-call journaling and capability limits. That would bring Golem's main benefit to a conventional engine without a full runtime switch.

### Gaps
- I did not research Obelisk, wasmCloud, Fermyon/Spin or any Temporal WASM-worker experiment within budget. I have no verified 2026 status for them.
- I found no independent benchmarks of Golem oplog overhead or replay latency.

## Q5. Edge/serverless durability: how do platforms handle cold starts, zero-cost suspension, and pricing for long sleeps?

### Takeaway
Serverless durability converged on "suspend and pay nothing for compute while waiting". AWS Lambda durable functions, Cloudflare Workflows, Vercel Workflow and Temporal Serverless Workers (pre-release) all work this way. Pricing has moved to **per-operation metering** that bills each step, sleep and wait as an operation, plus storage of checkpoint data. AWS charges $8 per million operations. Cloudflare added per-step billing from Aug 10, 2026, which makes even `sleep` a billed step. For agent loops with many fine-grained steps, the meter now matters.

### Cited Findings
**AWS Lambda durable functions**
- Pricing: durable operations (checkpoints, steps, waits) cost **$8.00 per million**, data written costs **$0.25/GB**, and retention costs **$0.15/GB-month**. Normal Lambda compute applies, including the compute of replays. Waits suspend with no duration charges for on-demand functions. — [hidekazu-konishi guide, secondary](https://hidekazu-konishi.com/entry/aws_lambda_durable_functions_practical_guide.html); [Gunnar Grosch, AWS DevRel](https://gunnargrosch.com/posts/aws-lambda-durable-functions-building-long-running-workflows-in-code); [re:Post on Map/Parallel billing](https://repost.aws/questions/QUSx4ax9k2Te-RcuuX4bI0Kw/billing-rate-for-map-or-parallel-operations-in-a-durable-lambda-function)
- Replay re-runs the handler from the start and skips completed checkpoints. The user pays replay compute but does not re-run completed work. — [same sources]

**Cloudflare Workflows**
- Per-step billing starts "no earlier than August 10, 2026" on Paid plans. A step includes "sleeping or waiting for events". — [Cloudflare Community changelog, primary](https://community.cloudflare.com/t/workflows-workflows-pricing-adds-per-step-billing-step-and-storage-billing-to-start-no-earlier-than-august-10-2026/938295); [Workflows changelog, primary](https://developers.cloudflare.com/changelog/product/workflows/)
- Storage is $0.20/GB-month and CPU is $0.02 per million CPU-ms. The exact per-step rate was not yet published when the article was written. The Free plan keeps a bundled allowance. "A `step.sleep()` that previously cost nothing… will cost a step". At about 450k steps/month, a medium agent pipeline sits "roughly at parity or tipping toward self-hosted". — [bex.co, Aug 17 2026, secondary](https://bex.co/blog/2026/08/17/cloudflare-workflows-per-step-billing-orchestration-meter)
- Limits: the per-instance step limit rose from 1,024 to **25,000** (March 2026). Persisted state is capped at 100 MB (Free) and 1 GB (Paid). A single sleep can last up to 365 days. Sleeping or waiting instances do not count toward concurrency. Instances created on or after Sept 10, 2026 keep completed state 7 days (was 30). — [Cloudflare Workflows changelog, primary](https://developers.cloudflare.com/changelog/product/workflows/) (figures came from the search-result summary of the changelog; I did not open each entry)
- A practitioner analysis covers "retry and step arithmetic" for AI workflows on Cloudflare. — [atyantik, secondary](https://atyantik.com/blog/cloudflare-ai-workflows/)

**Temporal**
- Serverless Workers (Pre-release) run Temporal workers on AWS Lambda with automatic scaling. — [Temporal, primary](https://temporal.io/blog/replay-2026-product-announcements); [The New Stack](https://thenewstack.io/temporal-replay-2026-news/)

**Vercel**
- Workflows are GA, with "no orchestrator… no separate infrastructure". — [Vercel docs, primary](https://vercel.com/docs/workflows)

### Inferences
- Per-step billing turns step granularity into a cost decision. Agent loops create many small steps (each LLM call, tool call, wait), so engines that batch or coalesce steps, or that bill wall-clock suspension at zero and steps cheaply, gain a price advantage. Cloudflare's 25k-step cap also limits long agent loops per instance. That forces "continue-as-new"-style chaining, which complicates agent memory.
- Replay compute is billed on Lambda. Long agent histories therefore raise the cost of every resume. Replay cost is now a visible line item, not a hidden one. Snapshot-plus-journal hybrids or incremental replay are a commercial lever as well as a technical one.
- Shorter default retention (Cloudflare, 30 to 7 days) conflicts with the "replay production histories for eval and audit" use case. Engines that make cheap long-term history archival a first-class feature could serve that need.

### Gaps
- I did not cover Azure Durable Task Scheduler pricing or features, Deno, or Netlify offerings within budget.
- I found no primary data on cold-start latency for resumed durable executions on any platform.
- I could not verify the AWS durable functions maximum execution duration from a primary page.

## Q6. What do practitioners complain is missing, and which concrete unmet needs are R&D opportunities?

### Takeaway
The loudest critique is that agent frameworks (LangGraph, Microsoft Agent Framework, Strands, CrewAI, Google ADK) give "checkpointing, not durability". They save state but leave failure detection, automatic restart, locking and duplicate prevention to the user. On the engine side, practitioners point to the cost and complexity of memoizing non-deterministic LLM calls, growing history and payload size, per-step billing, and protocol-client gaps (MCP Tasks support). Combined with Q1–Q5, these produce a concrete list of R&D opportunities.

### Cited Findings
- Diagrid (Mar 2, 2026): "checkpointing is a storage operation, not a reliability guarantee". In Microsoft Agent Framework, resume needs a manual checkpoint id, there is no failure detection or supervisor, and "concurrent processes can resume the same checkpoint simultaneously without locking". "Superstep-level checkpoints mean partial work within a step is lost on restart". In Strands, "failed graphs reset to the beginning", and FileSessionManager is not thread-safe. LangGraph, CrewAI and ADK leave "failure detection, automatic recovery, and duplicate prevention entirely to you". — [Diagrid, competitor claim (Dapr vendor)](https://www.diagrid.io/blog/still-not-durable-how-microsoft-agent-framework-and-strands-agents-repeat-the-same-mistake)
- LangGraph production concerns include latency, replay and scale of the checkpointer store. — [Aerospike blog, secondary/vendor](https://aerospike.com/blog/langgraph-production-latency-replay-scale/)
- The memoization layer for LLM calls "adds storage and code complexity", and snapshot size grows "linearly with conversation length". — [vadim.blog, secondary](https://vadim.blog/durable-execution-llm-agents/)
- Long-running background agents need hours-long runs that survive restarts. — [AI Engineering Insider, secondary](https://aiengineeringinsider.substack.com/p/long-running-background-agents-and); [Matthew Wong blog, secondary](https://www.matthewswong.com/en/blog/durable-execution-ai-agent-workflows/)
- Users ask MCP clients for SEP-1686 support because 60 s timeouts block long tools. — [claude-code #76571](https://github.com/anthropics/claude-code/issues/76571); [#18617](https://github.com/anthropics/claude-code/issues/18617)
- The Golem essay names the missing primitives as security, isolation, authorization, mediation, secrets and audit, beyond durability. — [Golem, competitor claim](https://golem.cloud/blog/the-rise-of-the-agent-runtime/)
- Cost: per-step metering makes serverless durable agents "a real cost comparison rather than a philosophical one" at about 450k steps/month. — [bex.co, secondary](https://bex.co/blog/2026/08/17/cloudflare-workflows-per-step-billing-orchestration-meter)

### Inferences
**Concrete R&D opportunities, ranked by my estimate of gap size × fit for a durable engine:**
1. **Fork / rewind-and-rerun from step N** on a journal engine, with edited inputs, a new prompt or a new model. The result is a new execution linked to its parent, with shared history before N. LangGraph has it for snapshots. I found no journal engine that markets a fork tree. (Evidence: [LangGraph docs](https://docs.langchain.com/oss/python/langgraph/use-time-travel), and its absence from [Temporal Replay 2026](https://temporal.io/blog/replay-2026-product-announcements).)
2. **Replay-as-evaluation harness**: re-run production histories with recorded tool outputs and a candidate model or prompt, then diff the decisions. I found no shipped product.
3. **First-class LLM-step metadata**: tokens in/out, model, cost and latency recorded in the journal, with per-run, per-tenant and per-step budgets and token-aware rate limiting and fairness. Today only account-level billing APIs exist (Temporal Billing API preview).
4. **Native durable streams**: offset-addressable, resumable output streams stored next to the journal, not in it, so token streams do not bloat replay history. Competitors build this on signals/updates (Temporal) or separate stream stores (Vercel, Inngest).
5. **MCP Tasks / A2A adapters**: expose a workflow as a spec-compliant MCP Task or A2A agent, and make outbound task calls durable awaitables with idempotent submission. The protocol defines the state machine but not crash safety ([AAIF](https://aaif.io/blog/handle-long-running-mcp-tool-calls-with-the-mcp-tasks-extension)).
6. **History compaction for agent loops**: claim-check offload ([Temporal External Storage](https://temporal.io/changelog/external-storage-public-preview)) plus delta-encoding of repeated context windows, or a snapshot-plus-journal hybrid that bounds replay cost (which matters because Lambda bills replay compute).
7. **Cross-run content-addressed LLM cache** in the journal (exact or semantic). Today memoization only works within one run's replay.
8. **Sandboxed step type** for LLM-generated code, with host-call journaling and capability limits. This is Golem's model brought to a conventional engine, and it is related to the OpenAI Agents SDK sandbox + Temporal GA integration.
9. **Supervisor-grade recovery for checkpoint-only frameworks**: an engine that LangGraph, MAF or Strands can use as a backend to add failure detection, auto-resume, leasing and locking. That is the Diagrid critique turned into a product.
10. **Step coalescing and cost-aware granularity**: under per-step billing ([Cloudflare](https://community.cloudflare.com/t/workflows-workflows-pricing-adds-per-step-billing-step-and-storage-billing-to-start-no-earlier-than-august-10-2026/938295), [AWS $8/M ops](https://hidekazu-konishi.com/entry/aws_lambda_durable_functions_practical_guide.html)), engines that batch tiny steps or make sleeps free have a price advantage.
11. **Authorization and audit that survive replay**: non-spoofable principals on events ([Temporal Principal Attribution](https://temporal.io/blog/replay-2026-product-announcements)) and replay-resistant authorization state ([arXiv 2608.01710](https://arxiv.org/pdf/2608.01710)), for EU AI Act-style audit.

### Gaps
- I did not search HN threads directly, so this section leans on vendor and practitioner blogs. A targeted HN or GitHub-issues pass (for example, Temporal community "history size limit" agent complaints, or LangGraph checkpointer issues) would strengthen the evidence on complaints.
- Several cited sources are vendors criticizing competitors (Diagrid, Golem). Treat their claims about rival products as positions, not verified facts.
