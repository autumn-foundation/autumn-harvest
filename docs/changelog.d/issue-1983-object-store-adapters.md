## Feature — S3 and GCS adapters, archived-history read-back (issue #1983)

- **Adapters.** `autumn-harvest-plugin` ships `ObjectPayloadStore` and
  `ObjectHistoryArchiver` over a small `ObjectBackend` seam. Feature `s3`
  adds `S3Backend` on `aws-sdk-s3`. Feature `gcs` adds `GcsBackend` on the GCS
  JSON API, with `GceMetadataToken`, `StaticToken` and `NoAuth` token sources.
  Feature `object-store` holds the adapters and `MemoryBackend` and adds no
  client. The core crate keeps no cloud dependency.
- **Codec.** An offloaded blob is a codec envelope when a codec is on.
  `ObjectHistoryArchiver::with_codecs` also encodes the whole archive
  document under the active key.
- **Fix: retention archival with a codec.** Retention loaded the history for
  the archive with the identity codec. With a real codec, that load failed.
  Retention then skipped the run on every tick, so it never archived or
  deleted it. Retention now archives the stored form. It inflates offloaded
  fields and does not decode them, so payload fields stay ciphertext.
- **Read path.** `HistoryArchiver::fetch` reads an archive back. Its default
  returns `ArchiveFetchError::Unsupported`, so existing archivers still
  compile. `GET /workflows/{id}/archived-history` (admin) returns the
  document. Vantage shows it at `/ui/workflows/{id}/archived-history`, and the
  "not found" page of a pruned run links there when an archiver is set.

No migration. No new `WorkflowEvent` variant. One new read-only route.

Tests: `object_store_s3_minio` (MinIO, `pgsty/minio`) and
`object_store_gcs_emulator` (`fsouza/fake-gcs-server`) run each adapter, the
ciphertext check and a retention-to-Vantage round trip against an emulator.
`retention_archive_codec_tests` covers the retention fix.
`archived_history_api_tests` covers the route and the page. The
`object_store` unit tests use `MemoryBackend` and a local fake GCS server.
