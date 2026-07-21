<!--
0.5.0: release notes for 0.5.0 are authored by the release process. See
CHANGELOG.md for the folded 0.5.0 entry and docs/upgrading/0.5.0.md for the
upgrade guide (source-breaking changes, dependency bumps, behavior changes).
This file below is retained for the 0.4.0 and earlier history.
-->

## [0.4.0] - 2026-06-16

### Added

- Add external workflow cancellation primitive (issue #492) (#676)([33595ff](https://github.com/madmax983/autumn-harvest/commit/33595ff5890efeb75c95a71f0f6281ddb0fdc9a8))

- Add soft SLA breach detection for workflow executions (issue #487) (#671)([f85ba1b](https://github.com/madmax983/autumn-harvest/commit/f85ba1b28526f02d060fd5533cbf5aaf236ec00c))

- Add bounded catchup window policy for scheduled workflows (issue #484) (#669)([b11e5a9](https://github.com/madmax983/autumn-harvest/commit/b11e5a9197d17cbe616b990ecf01a448cccbbf99))

- Issue #482: Data-dependent DAG branching with condition predicates (#667)([774079b](https://github.com/madmax983/autumn-harvest/commit/774079bdc522375d29537926dd562945d2a6e2e1))

- Add per-execution context headers (issue #481) (#664)([44f2af3](https://github.com/madmax983/autumn-harvest/commit/44f2af383216cab8c6b52eb165cb6ac7c1d231ad))

- Add stalled-workflow discovery filter to GET /workflows (issue #486) (#663)([3a63370](https://github.com/madmax983/autumn-harvest/commit/3a63370d37635412a0c79f9fc9be6cf0792fb125))

- Implement non-determinism deploy detection (#480) (#652)([53fd0c9](https://github.com/madmax983/autumn-harvest/commit/53fd0c968455f7c10a9524e7494e435017d9c048))

- Add bounded schedule runs: end_at and max_runs (issue #478) (#651)([e4a3479](https://github.com/madmax983/autumn-harvest/commit/e4a3479268fa530b0500ceaaa44de2fdc3d4826d))

- Add update-with-start atomic primitive (issue #479) (#649)([5ac5716](https://github.com/madmax983/autumn-harvest/commit/5ac5716af41b35f34475bdad3c18bd7840622da7))

- **macro:** Implement compile-time determinism check guardrails for #[workflow] (#640)([9a7aafb](https://github.com/madmax983/autumn-harvest/commit/9a7aafb9d8f0489203ce487803bc5608fcc6399e))

- Add signal-or-deadline race primitive (issue #476) (#645)([0d58067](https://github.com/madmax983/autumn-harvest/commit/0d5806799b3cc28bfe7a4211cab2d3a981a7cd5c))

- Add set_current_details() for durable workflow status breadcrumbs (#643)([7254bb1](https://github.com/madmax983/autumn-harvest/commit/7254bb1b182fe30b187604e40a439e2c8f5f304f))

- Add operator pause/resume primitive for workflow executions (#383) (#630)([b56a916](https://github.com/madmax983/autumn-harvest/commit/b56a916d0258ab680b88ae2d9050ac3978485ae1))

- Implement worker capability labels and activity routing (issue #382) (#622)([1ba16d6](https://github.com/madmax983/autumn-harvest/commit/1ba16d68dd993d3470b2e1573fcddaed53efddbb))

- Add deterministic side-effect primitives to WorkflowContext (issue #384) (#628)([c702e68](https://github.com/madmax983/autumn-harvest/commit/c702e6877f98d843d7f850c27c4be587499e8bd2))

- Add DLQ root-cause aggregation API for fast incident triage (#385) (#629)([3783037](https://github.com/madmax983/autumn-harvest/commit/37830378e841ff091fdd19a4975306c51f8eb26a))

- Implement queue & task eligibility explainer for stuck-task triage (#595)([a41087e](https://github.com/madmax983/autumn-harvest/commit/a41087e6d4568d1769ae1d599ee01f7b82893a08))

- Add worker-level retry context: attempt(), previous_failure(), max_attempts() (#616)([6a0834c](https://github.com/madmax983/autumn-harvest/commit/6a0834c8844665e7c11e1314c0b8214f8d19a4a5))

- Add harvest.workflow.terminal counter for per-outcome success-rate SLO (#594)([c9ff68a](https://github.com/madmax983/autumn-harvest/commit/c9ff68a473796d12aa006286cb0de38f7e182ce2))

- Add replay-safe WorkflowLogger and HVG009 guardrail (issue #379) (#587)([0e24d26](https://github.com/madmax983/autumn-harvest/commit/0e24d266fb929eeb9c1320881d65faa58d012944))

- Display timezone next to cron expression in Vantage UI schedules page (#588)([e59293c](https://github.com/madmax983/autumn-harvest/commit/e59293c1a443262eb4c0f39de3f4827ea851579a))

- Add schedule-to-close timeout for cross-retry wall-clock deadlines (#586)([2b1e75a](https://github.com/madmax983/autumn-harvest/commit/2b1e75af87186a7543291ae72b04f99608d531af))

- **dag:** Implement dynamic task mapping (fan-out) for DAGs (#585)([3fbf957](https://github.com/madmax983/autumn-harvest/commit/3fbf957b708f7e42391a10cb624d3200d58a570c))

- Add admission gate primitive for incident-response workflow halts (#579)([7974582](https://github.com/madmax983/autumn-harvest/commit/797458248ed0108761ee1b5020b1256e0aa6cde2))

- Implement declarative completion triggers (issue #517) (#561)([96e82bf](https://github.com/madmax983/autumn-harvest/commit/96e82bfd10fe633c0d3887d0264bf9398053cf7c))

- **metadata:** Implement workflow metadata ownership (#372) (#550)([6b3fb18](https://github.com/madmax983/autumn-harvest/commit/6b3fb18c0ab288bf9a1a8bc774b0e54b20fb2d86))

- Add JSON Schema opt-in for workflow input validation (issue #373) (#556)([7f5323d](https://github.com/madmax983/autumn-harvest/commit/7f5323d9d7fba5004791462e1c1d7d8671416318))

- Add per-activity circuit breaker for downstream outage fast-fail (issue #369) (#549)([c03e1d8](https://github.com/madmax983/autumn-harvest/commit/c03e1d8d2b2908b80d9c834f0a8ecfe5d8fadb7a))

- Implement poison-pill task quarantine (issue #367) (#539)([31c8305](https://github.com/madmax983/autumn-harvest/commit/31c8305c1838875f131896ef9e33587827fec851))

- Add DAG retry-from-failed-node operator surface (issue #366) (#530)([f80154e](https://github.com/madmax983/autumn-harvest/commit/f80154e0e981b5e1147b52b27424239e4936e4f0))

- Auto-pause schedules after consecutive execution failures (issue #360) (#500)([24af11f](https://github.com/madmax983/autumn-harvest/commit/24af11f7819d3a26f49885932ef6dfa33b4a5221))

- Add fan-out / parallel activity execution (issue #359) (#497)([b3535fe](https://github.com/madmax983/autumn-harvest/commit/b3535fe140783f39d78a7ce548d3f0b5ef8ddb2c))

- Add POST /workflows/batch_start endpoint for bulk workflow execution (#496)([2ca0a6d](https://github.com/madmax983/autumn-harvest/commit/2ca0a6dcbea59bb1bb5c79876d865c3d89994959))

- Add HA scheduler claim protocol for multi-replica deployments (issue #350) (#490)([0a3c4f8](https://github.com/madmax983/autumn-harvest/commit/0a3c4f8b3e39de10154c382a509dc70134cfdb72))

- Add transactional activities (issue #352) (#491)([51fe1ab](https://github.com/madmax983/autumn-harvest/commit/51fe1ab36a46517f7b689c1a8094aa001958e630))

- Propagate workflow cancellation to in-flight activities via heartbeat (#483)([246378a](https://github.com/madmax983/autumn-harvest/commit/246378aa82884beaa3b90ccfb4c355fa7ce0429a))

- Add DAGs listing and detail pages to Harvest UI (#424)([8444dc9](https://github.com/madmax983/autumn-harvest/commit/8444dc9c2c168d8ba029248b5c54289a2866d712))

- Add schedule next-fires preview API (issue #348) (#474)([db0387a](https://github.com/madmax983/autumn-harvest/commit/db0387ae729bb59da82cb80d0a285e6193fae1d6))

- Implement detached child workflow spawns with parent-close policies (#453)([17a7036](https://github.com/madmax983/autumn-harvest/commit/17a703651f24f784c96300e7717ae658b894e11d))

- Implement webhooks feature gate with durable signed webhook delivery workflow and activity (#452)([e3f8676](https://github.com/madmax983/autumn-harvest/commit/e3f86762f675b181c1997a7a0ede67895e635311))

- Add Build Routing UI page and API endpoints (issue #362) (#448)([5f4975c](https://github.com/madmax983/autumn-harvest/commit/5f4975c3966be6ccada02269909bd0feebd25ec6))

- Add schedule trigger-now endpoint (issue #343) (#450)([472cc80](https://github.com/madmax983/autumn-harvest/commit/472cc80b41ff28e3184bd3ec8d471b4fa6a28ef5))

- Implement compile-time type-safe workflow client stubs and handles (#341) (#443)([29ec69f](https://github.com/madmax983/autumn-harvest/commit/29ec69f4892f19b22fa109da1077e993b6d52726))

- **await-condition:** Implement await_condition and await_condition_timeout (issue #340) (#439)([9a34b15](https://github.com/madmax983/autumn-harvest/commit/9a34b15b4c4ba08a5377221cde7f550365f52f14))

- **retention:** Implement pre-retention history archival hook (issue #345) (#432)([a4927f7](https://github.com/madmax983/autumn-harvest/commit/a4927f73073e1c5079e037f2e949d3e6b6228d4f))

- Deterministic retry jitter: add `JitterPolicy`, seeded delays, worker seeding, tests, docs and benchmark (#431)([3d24a53](https://github.com/madmax983/autumn-harvest/commit/3d24a53438de369b62135094d12bb36cc87edce4))

- **rate-limit:** Implement time-windowed per-activity downstream API protection (RPS rate limiting) (#332) (#428)([611de9b](https://github.com/madmax983/autumn-harvest/commit/611de9bed73a5ee4d6e083a16cf49da0d94d457c))

- Add DAGs pages to Vantage UI (list + detail) (#426)([a90ffe8](https://github.com/madmax983/autumn-harvest/commit/a90ffe8fc663eb50d183d84461aa24bfe0052a32))

- **signal:** Deterministic Cross-Workflow Signaling (#421)([1c6b996](https://github.com/madmax983/autumn-harvest/commit/1c6b996c80c97889a5184716f6e05d3ea33d2e47))

- Expose worker-pool scaling endpoints for KEDA (#325) (#420)([0aa07df](https://github.com/madmax983/autumn-harvest/commit/0aa07df0fe56e0245e02eac9a18d61c5a2adaa36))

- Implement schedule decision persistence (issue #325) (#419)([66c355d](https://github.com/madmax983/autumn-harvest/commit/66c355dd4f56be4b71d25cde16215d0d2cd36580))

- Persist Schedule Decisions (Issue #325) (#417)([0e5b441](https://github.com/madmax983/autumn-harvest/commit/0e5b44180c42e7a4d9b4916c2a6295bc4ea5aa68))

- Support delayed/future workflow execution start (Issue #322) (#403)([090226b](https://github.com/madmax983/autumn-harvest/commit/090226b99e300bd1f38acd9417938467ad7dadc8))

- Add SSE execution event stream endpoint (issue #324) (#402)([c08b9b0](https://github.com/madmax983/autumn-harvest/commit/c08b9b01551420b4b06a559447a4225a8760f217))

- Add calendar-aware schedule filtering (issue #337) (#399)([f5ca74a](https://github.com/madmax983/autumn-harvest/commit/f5ca74ac2fe0f701460b8de10f6185dd5d5d2a3b))

- Display history event count and continue-as-new threshold on workflow detail page (#398)([805c2f4](https://github.com/madmax983/autumn-harvest/commit/805c2f46a8fab3c5a48aaa03d0dfbcd4130efef0))

- Implement payload size caps for workflow payloads (issue #252) (#395)([2d903b0](https://github.com/madmax983/autumn-harvest/commit/2d903b05d4bbbab02f8d1fc3b8a2ff61f89057b4))

- Add ReplayVerifier batch CI gate for workflow determinism (issue #251) (#393)([a4c1299](https://github.com/madmax983/autumn-harvest/commit/a4c129976be1cb685eb75a7421026782a36e0abc))

- Add task priority support for within-queue ordering (issue #249) (#391)([e37b4f2](https://github.com/madmax983/autumn-harvest/commit/e37b4f2dc0104e5c82d661fa38710585e2469e4d))

- Add timezone-aware cron schedules (Schedule::CronInTimezone) (#389)([ee81805](https://github.com/madmax983/autumn-harvest/commit/ee818052126e7a6b50cb2f35848721bf0017a101))

- Add typed dispatch helpers for activities, workflows, and signals (#387)([68b4ceb](https://github.com/madmax983/autumn-harvest/commit/68b4ceb601f4aee5e43844d82ae031b9bc874afd))

- Add SignalWithStart primitive for atomic webhook handlers (issue #244) (#364)([d3729c4](https://github.com/madmax983/autumn-harvest/commit/d3729c485fe645f5884901b0edab51a5e702e7ec))

- Add per-key concurrency limits for tenant fair-share scheduling (#370)([14240b5](https://github.com/madmax983/autumn-harvest/commit/14240b52a93074755d90d06f267de6b29ec56aae))

- Add ctx.signal_external_workflow for saga choreography (issue #330) (#376)([cde9046](https://github.com/madmax983/autumn-harvest/commit/cde9046102c402be265187b70cbada57a815a4f4))

- Implement workflow execution timeout for SLA enforcement (issue #243) (#374)([5b74436](https://github.com/madmax983/autumn-harvest/commit/5b744368d67b4f3bf26dafd0e04e382f6b7dc03d))

- Add WorkflowTestEnv in-process unit-test harness (issue #250) (#375)([0e56b6e](https://github.com/madmax983/autumn-harvest/commit/0e56b6e1daf1c966bc66e0399a0ea7fe378260c0))

- Add schedule overlap policy for concurrent run collisions (issue #241) (#361)([995c1c4](https://github.com/madmax983/autumn-harvest/commit/995c1c40a37ba69815a79f0f324162b1e137d7de))

- Add TDD DB integration tests for sticky cross-worker routing (spec #35) (#363)([04f723a](https://github.com/madmax983/autumn-harvest/commit/04f723ab46f556192c6378788ab2867d0c2347a0))

- Add workflow detail page with operator actions and event timeline (#354)([1d73748](https://github.com/madmax983/autumn-harvest/commit/1d73748b440d2a728f620e57e15f50929ab13a12))

- Declarative query and update handlers (issue #346) (#353)([dfee5fc](https://github.com/madmax983/autumn-harvest/commit/dfee5fce5abb281ef5fc5ed60c57613745c9bfc4))

- Add deterministic jitter to schedule fire times (#351)([4bdb434](https://github.com/madmax983/autumn-harvest/commit/4bdb434ae49e2ff1f3bcc1c1e96967211cea7b96))

- Add saga cancellation semantics and idempotency contract (#339)([d415ab2](https://github.com/madmax983/autumn-harvest/commit/d415ab29292864baef8f24852025919303cfa9a7))

- Implement sticky cross-worker routing with in-process cache delta-load (issue #235) (#336)([59a10d9](https://github.com/madmax983/autumn-harvest/commit/59a10d92140dfbac3221720a249ff2ef918d6bfc))

- Issue #234: Read-only Query handlers for live workflow state inspection (#328)([ca0e166](https://github.com/madmax983/autumn-harvest/commit/ca0e16618952def10e18fee1f42da78add3ddc36))

- Add schedules management UI page with pause/resume/delete actions (#333)([bc8df91](https://github.com/madmax983/autumn-harvest/commit/bc8df91b604d09f00c6b37740a12fd4fe4df5bd6))

- Unify DAG execution onto workflow path (issue #256 Step 5) (#302)([4e914ca](https://github.com/madmax983/autumn-harvest/commit/4e914ca8484eee2a38f6442c26ef3f8992bc9202))

- Add Claude Code GitHub Workflow (#310)([cb0cb48](https://github.com/madmax983/autumn-harvest/commit/cb0cb48dd294fb75f26653190c5251544427167c))

- Add pause/resume metadata tracking for schedules (issue #229) (#301)([2da1c38](https://github.com/madmax983/autumn-harvest/commit/2da1c382ef5d6eee5b706694a7a9fabf76d72a22))

- Issue #227: Typed activity failure surface with error classification (#299)([511ea8e](https://github.com/madmax983/autumn-harvest/commit/511ea8e44c0e2261cdcd53a87a65cf08c0c7c1d2))


### Fixed

- **plugin:** Require auth for cancel and dead-letter endpoints (#316)([50e7c3e](https://github.com/madmax983/autumn-harvest/commit/50e7c3ef092a1f6b49c3c32ea9720fb5bb5389c3))


### Performance

- Pre-allocate markers vector to reduce heap allocations (#290)([23f0796](https://github.com/madmax983/autumn-harvest/commit/23f0796e6d1d441973cd43645b58257d8229fc06))


### Miscellaneous

- Fix changelog (#300)([512796c](https://github.com/madmax983/autumn-harvest/commit/512796c4ae1c1a365e24b2958d9869f8a787e174))
