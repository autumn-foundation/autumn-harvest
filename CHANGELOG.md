# Changelog

All notable changes to autumn-harvest will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2026-05-11

### Added

- **Versioned management API contract** (`docs/api-contract.json`, issue #175):
  machine-readable JSON describing every route's method, path, category,
  `read_only` flag, request-body field schema, success/error responses,
  pagination params, and idempotency semantics. Contract version `"1"`,
  aligned with crate version `"0.3.0"`.
- `management_api_request_fields()` in `autumn_harvest_plugin::api`: canonical
  registry of request-body field names per mutating route. Compared against the
  contract by the regression test suite; update both together when adding or
  renaming fields.
- Contract regression tests (`autumn-harvest-plugin/tests/contract_regression.rs`):
  six tests assert route-set parity, required metadata fields, version/compat
  metadata, structured `request_body` on all mutating routes, field-registry
  parity, and read-only/method consistency.
- CLI body-field coverage tests (`autumn-harvest-cli/tests/contract_coverage.rs`):
  every CLI subcommand is exercised to confirm (a) it maps to a documented
  contract route and (b) every key it sends in the request body is declared in
  the contract's `fields` list.
- Embedder guide `docs/api-contract-guide.md` with jq inspection recipes,
  compatibility rules, client generation workflow, and developer update checklist.
- Request/response workflow embedding (issue #224): in-process
  `WorkflowHandleClient`/`WorkflowHandle`, compact
  `GET /workflows/{id}/result?wait=...`, and quickstart coverage for awaiting
  `handle.result_raw().await?` from an HTTP route.

### Non-breaking

This release is additive only.  No route, event, or schema was removed or
renamed.  Adding the contract is classified as non-breaking per the
compatibility rules stated in `docs/api-contract.json`.

## [0.2.0] - 2026-04-27

### Added

- Add harvest management cli (#29)([597dcfc](https://github.com/madmax983/autumn-harvest/commit/597dcfca7406dd5554223a843589a31d6cd2647c))
- Add Mermaid.js workflow history exporter (#25)([7226560](https://github.com/madmax983/autumn-harvest/commit/72265609750ef9b02b18f6b9cb49b2d31ddcee91))

### Fixed

- Redis depth handling([76335e6](https://github.com/madmax983/autumn-harvest/commit/76335e65f96643faf3e08c635dc3a7ad5fe2277f))
- Replay determinism([d0a14d7](https://github.com/madmax983/autumn-harvest/commit/d0a14d7eede408bcf4449f1ad68ce04013c47b91))
- Redit and continue-as-new([fd1c024](https://github.com/madmax983/autumn-harvest/commit/fd1c0242a42985e6a8cfc4a1d19b40daf091e88e))
- Prevent silent integer truncation in concurrency limits and event indexing (#45)([68d5225](https://github.com/madmax983/autumn-harvest/commit/68d5225d6d5e97850ba975188d971e86a4bd5ce3))
- Resolve broken intra-doc links for HarvestError (#19)([cd77f63](https://github.com/madmax983/autumn-harvest/commit/cd77f6365848bcd7c9861564ffc53b2c0bf69f08))

### Documentation

- Skills([be33d2e](https://github.com/madmax983/autumn-harvest/commit/be33d2ebcde3c6eeb29540baf2e728ae175bd565))
- Add module-level documentation and executable doc tests for query, dag_export, schema, and signal. (#69)([859bb21](https://github.com/madmax983/autumn-harvest/commit/859bb2101fcbc7f6efbc0b486adb05b6b171e1c4))
- Add Vantage spec for Continue-As-New (#60)([d884143](https://github.com/madmax983/autumn-harvest/commit/d884143fe78da00cf627b11a72ea07445d8b077e))
- Add Vantage specification for Saga Primitives (#18)([0cc01fe](https://github.com/madmax983/autumn-harvest/commit/0cc01fed2dea12ed2ce076ae230a1fd5d0ee3cf4))
- Update CHANGELOG.md for v0.1.1([0dc1ff1](https://github.com/madmax983/autumn-harvest/commit/0dc1ff1e8973c2076bc95c591a0a3883cbc1966c))

### Testing

- **saga:** Add comprehensive unit tests for Saga compensation logic (#44)([72d5760](https://github.com/madmax983/autumn-harvest/commit/72d5760b1217909d79342ac9a5242bc3a672b57d))
- Improve test coverage for Error, Info, and Saga modules (#40)([48f5a37](https://github.com/madmax983/autumn-harvest/commit/48f5a377dd024b65c2782447fca4407ad3577e2a))

### Miscellaneous

- Relase work([7e61ec0](https://github.com/madmax983/autumn-harvest/commit/7e61ec038ee985e93186a72c0cad28d53d8b7ddc))
- Clippy([51b9b6e](https://github.com/madmax983/autumn-harvest/commit/51b9b6ec3626e5804eca98f138ac907f58554fdb))
- PR feedback([6f40114](https://github.com/madmax983/autumn-harvest/commit/6f40114d7ac6109a0a277d98cce469ca8dfefc8f))
- Cleanup([2c59de4](https://github.com/madmax983/autumn-harvest/commit/2c59de42684a19c036e1360584bfc53522eaa4e7))
## [0.1.1] - 2026-04-19

### Documentation

- Update CHANGELOG.md for v0.1.0([b12cfc1](https://github.com/madmax983/autumn-harvest/commit/b12cfc193de46590f7e05eecc8b2063e4b18e675))
## [0.1.0] - 2026-04-19

### Added

- **worker:** Add worker runtime with semaphore-bounded poll loop([47f8291](https://github.com/madmax983/autumn-harvest/commit/47f8291b8f6e7716f6950ff8505105825f67ca3c))
- **pool:** Add separate worker pool with shared ceiling enforcement([c602cbd](https://github.com/madmax983/autumn-harvest/commit/c602cbdb5e086aaecf10a44137b9fc50389696b9))
- **dlq:** Add dead letter queue for permanently failed tasks([07b0ba0](https://github.com/madmax983/autumn-harvest/commit/07b0ba03a5bae5379b8b5d497feb77f1e395a5e7))
- **cache:** Add LRU workflow state cache for replay optimization([6f5eb96](https://github.com/madmax983/autumn-harvest/commit/6f5eb96c1c2db92493863f1648b06d1d5bca212a))
- **timeout:** Add timeout enforcement for heartbeat, start-to-close, and schedule-to-start([f860113](https://github.com/madmax983/autumn-harvest/commit/f860113d6647e8fca33f5286c6dc38dc9b32e960))
- **heartbeat:** Add batched heartbeat flusher with 1s debounce([b563e83](https://github.com/madmax983/autumn-harvest/commit/b563e834ef99e491385af6db591e9afec729f784))
- **executor:** Add workflow executor with replay and suspension detection([b00d056](https://github.com/madmax983/autumn-harvest/commit/b00d056b1cefa6023839d42f8a2fa04ca5d22ca0))
- **notify:** Add LISTEN/NOTIFY wrapper with channel naming and payload types([387dd3a](https://github.com/madmax983/autumn-harvest/commit/387dd3a102d823dccf0ae6f7d8d8103c1d2d4e22))
- **queue:** Add Postgres-backed task queue with SKIP LOCKED claiming([574503f](https://github.com/madmax983/autumn-harvest/commit/574503f2a97de05ef5f8bcb83c67eb45bffc67b9))
- **context:** Implement ActivityContext with heartbeat channel and cancellation([233189a](https://github.com/madmax983/autumn-harvest/commit/233189a1bf368412c6757ec050e0199697f1d4d2))
- **context:** Implement WorkflowContext with replay-aware execute_activity and versioning([3e908bb](https://github.com/madmax983/autumn-harvest/commit/3e908bb22c173de66d784b0d7eae93d9f473b182))
- **replay:** Add HistoryMatcher with activity, timer, and version matching([f79b6ef](https://github.com/madmax983/autumn-harvest/commit/f79b6ef340d2d18ad457203af123b880c517fc4d))
- **store:** Add load_history reader for event replay([921a57f](https://github.com/madmax983/autumn-harvest/commit/921a57f7c81ef856ef03929adff88b1e40e41658))
- **store:** Add event store writer with sequential event IDs([3f7337f](https://github.com/madmax983/autumn-harvest/commit/3f7337f45b250d3978c27cb762a3399238802b0f))
- **macros:** Add workflows![] and activities![] collection macros([d09fd56](https://github.com/madmax983/autumn-harvest/commit/d09fd56e518a949a95af5d379535efc46d856bc1))
- **macros:** Implement #[activity] with retry/timeout/queue attributes([a6ea41a](https://github.com/madmax983/autumn-harvest/commit/a6ea41ae142b7164b61ae34a779b195339387c18))
- **macros:** Implement #[workflow] companion function generator([bd9de34](https://github.com/madmax983/autumn-harvest/commit/bd9de34c630260902e0bf2d4a1132d7b60be100c))
- **builder:** Add HarvestBuilder, WorkerConfig, and prelude([923edbe](https://github.com/madmax983/autumn-harvest/commit/923edbe84551000dbefedfaca4cd24384dce5e67))
- **db:** Add Diesel schema and Queryable/Insertable models for harvest_* tables([cbac2e7](https://github.com/madmax983/autumn-harvest/commit/cbac2e7d4d4486bbc72881177f18e15cdb85161d))
- **migrations:** Add harvest_* Postgres schema (initial migration)([6881762](https://github.com/madmax983/autumn-harvest/commit/6881762d8c64ec409f0a43712514f47fd9df9da2))
- **context:** Add WorkflowContext and ActivityContext skeletons with state access([747a1c4](https://github.com/madmax983/autumn-harvest/commit/747a1c43fdcb7729b6df50ff5db491f206b18fb9))
- **info:** Add WorkflowInfo, ActivityInfo registration types([713f20a](https://github.com/madmax983/autumn-harvest/commit/713f20a62f5ab275b1cebb82c6856ab95eaa4ed9))
- **event:** Add WorkflowEvent enum with serde round-trip([bb2d637](https://github.com/madmax983/autumn-harvest/commit/bb2d6371756cb823334a059e41e6edb261bd9943))
- **policy:** Add RetryPolicy, TriggerRule, Schedule([e22c9ea](https://github.com/madmax983/autumn-harvest/commit/e22c9ea35bb3865d64fb7b1cb346c8795176aecf))
- **error:** Add HarvestError, HarvestResult, TimeoutType, compute_retry_delay([7f6a27e](https://github.com/madmax983/autumn-harvest/commit/7f6a27e335577aabff50b753d5257ed552adc282))
- **types:** Add WorkflowId, ExecutionId, ActivityExecId, TimerId, WorkerId newtypes([e1b26a7](https://github.com/madmax983/autumn-harvest/commit/e1b26a7ba371fcf07c1d1d5143f0bb2b593f277c))

### Fixed

- **plugin:** Explicitly enable autumn-harvest db feature([2603761](https://github.com/madmax983/autumn-harvest/commit/2603761b1a7008077f51ce5023deff0f023c4ef9))
- **migrations:** Remove duplicate harvest initial migration([a3deb05](https://github.com/madmax983/autumn-harvest/commit/a3deb0518745ae99376346ecfcb22aa2115bc944))
- **plugin:** Explicitly enable autumn-harvest db feature([bbf93f3](https://github.com/madmax983/autumn-harvest/commit/bbf93f3bf0c82610071c5b32684be3417954f725))
- **plugin:** Consume MIGRATIONS const instead of cross-crate macro path([cd98e36](https://github.com/madmax983/autumn-harvest/commit/cd98e3656868cb67ee7e1dc26cf8eb0697fefefe))
- **manifests:** Add version to internal workspace path deps([8ed6456](https://github.com/madmax983/autumn-harvest/commit/8ed6456fd812ee06fa4dfecfb6883bf877324fc5))
- **plugin:** Consume MIGRATIONS const instead of cross-crate macro path([43ed6f7](https://github.com/madmax983/autumn-harvest/commit/43ed6f70e94163052e2d3debb2e1ea46aa6c5fd1))
- **manifests:** Add version to internal workspace path deps([3d2254f](https://github.com/madmax983/autumn-harvest/commit/3d2254f1c33889ab3f4466be8397015269ea55ab))
- Resolve clippy warnings in pool/worker and run cargo fmt([28d83ae](https://github.com/madmax983/autumn-harvest/commit/28d83aea5d11a201e09ca5ff43234a4f92d872e7))
- **macros:** Propagate serialization errors and add Debug impls to builder types([c460035](https://github.com/madmax983/autumn-harvest/commit/c460035d8d6d6c2c763f728a337962819932d9a8))
- **models:** Add serde derives and doc comments to all model structs([309f156](https://github.com/madmax983/autumn-harvest/commit/309f1563ace0803b15fb71414a565267deedb6e4))
- **context:** Add production constructors and fix unused_async lint([83645ad](https://github.com/madmax983/autumn-harvest/commit/83645ad69707997e0c5d9b1dca71afe5f58a047b))
- Move compute_retry_delay to policy, fix underflow, AllDone vacuous truth, const type_name, expanded tests([e9d7c46](https://github.com/madmax983/autumn-harvest/commit/e9d7c46715e68a4ffb867783077010e586935820))
- **types:** Const fn on as_uuid, add as_str to TimerId and WorkerId([5fe02cb](https://github.com/madmax983/autumn-harvest/commit/5fe02cb46f3b8114aa35eabf311731513ddbccd6))

### Documentation

- Update CHANGELOG.md for v0.1.0([e117e4d](https://github.com/madmax983/autumn-harvest/commit/e117e4dc56667350af54e271160f63bdc1067081))
- Update CHANGELOG.md for v0.1.0([9aafd65](https://github.com/madmax983/autumn-harvest/commit/9aafd6508b9c42eb82586dce095b5c5e8f447ebb))
- Add README and crates.io metadata for first publish([74dbbb9](https://github.com/madmax983/autumn-harvest/commit/74dbbb912eb4f3d430c209c1ef98fb36e830785e))
- Update CHANGELOG.md for v0.1.0([7ed8a5f](https://github.com/madmax983/autumn-harvest/commit/7ed8a5fdbe5a816bde8ad65567e549f4d5fb34bd))
- Add README and crates.io metadata for first publish([df0d628](https://github.com/madmax983/autumn-harvest/commit/df0d628153bb66641d4affbef34cb61ae45ba453))
- Update CLAUDE.md with Phase 2 completion status([38f506e](https://github.com/madmax983/autumn-harvest/commit/38f506e52a995e85342d5f5aa3ddfebc854b10a8))
- Add CLAUDE.md with architecture overview and development guide([48675e7](https://github.com/madmax983/autumn-harvest/commit/48675e777c46cbfa0701477aaccf18a1044dad7b))

### Testing

- Fix clippy doc_markdown warnings in replay tests([2ee4667](https://github.com/madmax983/autumn-harvest/commit/2ee466788daae3300818a5fda85ee3856ceda493))
- Add E2E integration tests with testcontainers Postgres([c670f8b](https://github.com/madmax983/autumn-harvest/commit/c670f8bc577ba3bd09277b877835bc3095ac0937))
- Add replay engine correctness tests including non-determinism detection([c624524](https://github.com/madmax983/autumn-harvest/commit/c6245246344cc3beea5e95da8910ffa065d9b0f9))
- Add end-to-end integration tests for workflow lifecycle([c81bcc7](https://github.com/madmax983/autumn-harvest/commit/c81bcc75effb9db41f6aee875d3786c1ff2109ba))

### Miscellaneous

- Trigger on trunk-dev push and pull_request([0601dd0](https://github.com/madmax983/autumn-harvest/commit/0601dd092f66b074ccced99c18862e075d93f150))
- Trigger on trunk-dev push and pull_request([7bf8cc0](https://github.com/madmax983/autumn-harvest/commit/7bf8cc0908ed0de336389c506fdc4cca530622be))
- **plugin:** Use published autumn-web 0.2 from crates.io([20aa1cd](https://github.com/madmax983/autumn-harvest/commit/20aa1cd9cfc63e087fb6b732a04e5d1cfec74bf1))
- **plugin:** Use published autumn-web 0.2 from crates.io([15d9eba](https://github.com/madmax983/autumn-harvest/commit/15d9ebaaf32dcd125d3e34a205b07c7f095c7450))
- Sync Phase 3 from autumn monorepo([0ee395e](https://github.com/madmax983/autumn-harvest/commit/0ee395e142d34271b5eaa4c391254f0517235a20))
- Clippy + fmt clean across autumn-harvest Phase 2([9d44e2d](https://github.com/madmax983/autumn-harvest/commit/9d44e2d4f11826338399beffe235de290bd5e80c))
- **lib:** Remove stale task comment from context module declaration([11ce797](https://github.com/madmax983/autumn-harvest/commit/11ce79793270986a5bbbf65c0449a58e2a1bacb4))
- Fix workspace review issues (deadpool version, clippy msrv, gitignore, testcontainers deps)([1d99fc3](https://github.com/madmax983/autumn-harvest/commit/1d99fc3da51bbafb231d6743a51caa501176dae0))
- Initialize autumn-harvest workspace([55f4ceb](https://github.com/madmax983/autumn-harvest/commit/55f4ceb915fd88dc59249db9a98adf1f044c09a7))

