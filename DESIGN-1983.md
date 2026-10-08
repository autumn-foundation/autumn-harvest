# Design — Issue #1983: S3 and GCS adapters, archived-history read-back

Issue #1983 asks for three things:

1. S3 and GCS adapters for `PayloadStore` and `HistoryArchiver`. Tests run
   against emulators (MinIO, fake-gcs-server).
2. With the codec on, the uploaded bytes are ciphertext. A test proves it.
3. The management API and Vantage show an archived history.

**No migration. No new `WorkflowEvent` variant. One new API route.**

---

## 0. Planning record

### 0.1 Facts found before the design

- Cloud clients live in `autumn-harvest-plugin` behind cargo features
  (`sqs`, `aws-kms`). The core crate has no cloud dependency.
  `tests/connector_dependency_graph.rs` enforces this.
- `PayloadOffloader` encodes a field with the codec first. Then it uploads
  the encoded JSON. So a `PayloadStore` blob is a codec envelope already.
- `HistoryArchiver` has `archive` only. Nothing can read an archive back.
- **Defect.** Retention loads the history for the archive with
  `store::load_history`. That loader decodes with the identity-only registry.
  With a real codec, it fails on the first envelope. Retention then skips the
  run on every tick. The archiver never gets a document. The offload path has
  the same defect. So "codec on" plus "archive" never worked.
- The official MinIO images on Docker Hub and `dl.min.io` are gone (HTTP 410
  and "repository does not exist"). `pgsty/minio` is the maintained community
  build. `fsouza/fake-gcs-server` is on Docker Hub.

### 0.2 Brainstorm — where do the adapters go, and how do they work?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Put the adapters in the core crate. | Rejected. It breaks the "no cloud dependency in core" rule. |
| B2 | Use `object_store` (Apache Arrow) for both clouds. | Rejected. It is a large new graph for three calls. |
| B3 | S3 on `aws-sdk-s3`, GCS on the official Google SDK. | Rejected for GCS. The SDK is large and its MSRV is high. |
| B4 | S3 on `aws-sdk-s3`. GCS on the JSON API over the `reqwest` client already in the plugin. One small `ObjectBackend` trait. Generic adapters on top. | **Adopted.** See §1.1. |
| B5 | Encrypt the whole archive document with the codec. | **Adopted** as an option. It hides metadata, for example `workflow_id`. See §1.2. |
| B6 | Add `fetch` to `HistoryArchiver` with a default body. | **Adopted.** Old implementations still compile. See §1.3. |
| B7 | A separate `ArchiveReader` trait. | Rejected. The embedder then registers two objects for one store. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | Upload plaintext when the codec is on. | Emulator tests read the raw object and assert ciphertext. |
| R2 | Decode the payloads before the archive. The archive then holds plaintext. | Retention archives the stored form. A test asserts envelopes in the document. |
| R3 | Change the `store_id` default. Old references then fail to inflate. | The default stays `"default"`, the trait default. `with_store_id` overrides it. |
| R4 | Retire a codec key. Archives encoded under it become unreadable. | The docs say: keep a retired key registered while its archives must stay readable. |
| R5 | Show archived payloads to a non-admin. | The route needs admin. Payload decode uses the same gate as live history. |
| R6 | Let an object key escape the prefix. | Keys come from an `ExecutionId` or a SHA-256 hex digest only. GCS names are percent-encoded. |
| R7 | A missing object returns an error, not "not found". | Both backends map 404 / `NoSuchKey` to `None`. The API returns 404. |
| R8 | Delete fails on a missing blob, and retention GC loops. | Delete of a missing object is success on both backends. |
| R9 | A slow store hangs a request. | The API bounds `fetch` with the retention archival timeout. |
| R10 | CI cannot pull the emulator image. | The CI job pre-pulls both images with retries, as for ElasticMQ. |

### 0.4 Six thinking hats — B4 + B5 + B6

| Hat | Notes |
|-----|-------|
| White | `aws-sdk-s3` 1.122 resolves with Rust 1.88. It adds nine crates, all MIT or Apache-2.0. The GCS JSON API needs three calls: upload, download, delete. |
| Red | One trait and two backends is easy to explain. Embedders stop writing their own adapter. |
| Black | Archives encoded under a key are not re-encrypted by the rotation sweep (R4). GCS auth is a seam: a static token and the GCE metadata server ship. A service-account JSON key needs an embedder token source. |
| Yellow | The fix to retention makes archival work with a codec for all archivers, not only the new ones. The read path reuses the history-export document. Replay tools already read it. |
| Green | B2, B3, B7 in §0.2. A later "tiered history" issue can reuse `ObjectBackend` to export partitions. |
| Blue | Red: tests for the retention defect, the adapters, the emulators, the route and the page. Green: implement. Refactor: docs, contract, CI legs. Then a multi-angle review. |

---

## 1. Design

### 1.1 Adapters (plugin crate)

Features: `object-store` (generic adapters), `s3` and `gcs` (backends).

- `ObjectBackend`: `put(key, bytes, content_type)`, `get(key) -> Option`,
  `delete(key)`. A missing object is `None` on `get` and success on `delete`.
- `ObjectPayloadStore<B>` implements `PayloadStore`. The key is
  `{prefix}{sha256-hex}`, so identical bytes share a key.
- `ObjectHistoryArchiver<B>` implements `HistoryArchiver`. The key is
  `{prefix}{execution_id}.json`. A re-archive overwrites the same key.
- `S3Backend` wraps an `aws_sdk_s3::Client` and a bucket.
- `GcsBackend` uses the JSON API. It takes a base URL, a bucket and a
  `GcsTokenSource` (`NoAuth`, `StaticToken`, `GceMetadataToken`).

### 1.2 Codec

- `PayloadStore`: no change. The offloader uploads the codec envelope.
- `ObjectHistoryArchiver::with_codecs(codecs)` encodes the whole document
  with `PayloadCodecs::encode_payload` before upload. `fetch` decodes it.
  Use the builder's `payload_codecs()`. Its key registry is shared, so a key
  rotation applies at once.
- Retention archives the stored form of each event. It inflates offloaded
  fields but does not decode them. So payload fields stay ciphertext even
  without `with_codecs`.

### 1.3 Read path

- Core: `HistoryArchiver::fetch(&ExecutionId)` returns
  `Result<Option<HistoryExportDocument>, ArchiveFetchError>`. The default
  returns `ArchiveFetchError::Unsupported`.
- API: `GET /workflows/{id}/archived-history`, admin only. 200 with the
  document; 404 when the archive has no such run; 503 when no archiver is
  set, the archiver cannot fetch, or the store fails. Payload fields decode
  under the same gate as `GET /workflows/{id}/history`.
- Vantage: `/ui/workflows/{id}/archived-history` shows the metadata and the
  event table. The "not found" page for a run links to it.

## 2. Test plan

| AC | Test |
|----|------|
| 1 | `object_store_s3_minio` and `object_store_gcs_emulator` suites: put, get, delete, archive, fetch against the emulators. Unit tests with an in-memory backend. |
| 2 | Same suites: with an AES-256-GCM codec, the raw object is a codec envelope with no plaintext marker. Core `retention_archive_keeps_codec_ciphertext`. |
| 3 | `archived_history_api_tests` (200, 404, 503, 401). Vantage render tests. |
