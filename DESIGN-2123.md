# Design — Issue #2123: Sweep blobs that a rolled-back write uploaded

`append_events_offloaded_with_codecs` uploads blobs before it opens its
transaction (#954). A write that rolls back leaves its blobs in the store with
no `harvest_payload_refs` row. Retention finds blobs only through those rows,
so nothing deletes them. This change adds a periodic sweep of such orphans.

**No migration. No new `WorkflowEvent` variant. No change to the write path.**
`harvest_events` is not touched.

---

## 0. Planning record

### 0.1 Facts found before the plan

- A rolled-back write leaves at most one write's blobs. A content-addressed
  store reuses the key when the same write repeats.
- `harvest_payload_refs` is shard-local. A blob key can be referenced on any
  shard. `batch_blob_keys_still_referenced` already checks one database.
- The retention runtime already runs best-effort passes per physical database
  (`run_terminal_task_pass`, `pools.pool_groups()`).
- `ObjectPayloadStore` uses a new key for each upload. Other stores can reuse
  a key.
- No shipped `PayloadStore` can list. The trait needs an optional method.

### 0.2 Brainstorm — how can a store find its orphans?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Optional `PayloadStore::list` that returns keys with creation times. | **Adopted.** It is the issue proposal. Old backends keep working. |
| B2 | Compensating delete on rollback. | Rejected. It races with a writer of the same bytes (see the issue). |
| B3 | Write a pending-ref row before the upload. | Rejected. It changes the write path and holds the fence design (#954) open. |
| B4 | Tag each blob with an expiry at upload. | Rejected. The trait has no metadata, and a committed blob would need a refresh. |
| B5 | Store lifecycle rules (S3 expiry). | Rejected. A committed blob must live as long as its run. |
| B6 | Page the listing and keep a cursor between ticks. | **Adopted.** One tick does bounded work on a large bucket. |
| B7 | Report `found`, `deleted` and `failed` counters. | **Adopted.** |

### 0.3 Reverse brainstorm — how can the sweep destroy data?

| # | Failure | Guard |
|---|---------|-------|
| R1 | It deletes a blob that a live run needs. | Delete only when no `harvest_payload_refs` row on any database names the key. |
| R2 | One database is down, so its refs are unknown. | A failed reference check aborts the page. The sweep deletes nothing from it. |
| R3 | It deletes a blob a writer just uploaded. | Grace period. The sweep ignores a blob younger than the cutoff, even if the store ignores the filter. |
| R4 | A short grace period makes R3 likely. | `validate` refuses a grace under one hour. The default is one day. |
| R5 | A writer re-uploads an old key and the store keeps the old creation time. | The trait contract says a `put` must reset the creation time. The doc names this. |
| R6 | `list` returns objects that harvest did not write. | The trait contract says `list` returns only blobs from `put`. A store with a shared bucket scopes the listing. |
| R7 | A dry run deletes. | A dry run counts orphans and calls no `delete` and no delete metric. |
| R8 | A fenced region deletes. | The pass runs inside the fenced retention tick. |
| R9 | A delete fails and loops forever. | The failure is counted. The next pass retries. The cursor moves on. |
| R10 | A store that cannot list fails on every tick. | The first `None` disables the sweep. It logs once. |
| R11 | A writer uploads an old orphan key again after the page is listed (review of PR 2191). | Just before each delete, the sweep checks the references of that key and reads its creation time again. The delete is not conditional, so one store round trip stays open. |
| R12 | A hung store holds the tick and its fence guards. | Each `list` and `delete` has the `archival_timeout` and ends at shutdown. |
| R13 | Two deployments with different databases share a store. | The `list` contract forbids it and tells the store to scope its listing. Implementing `list` is the opt-in. |
| R14 | A failing store floods the log. | The sweep logs one warning for each page. Keys go to the debug level. |

### 0.4 Six hats

- **White (facts).** See 0.1. No shipped backend lists, so the sweep is inert
  until an embedder implements `list`.
- **Red (feelings).** An operator fears a janitor that deletes external data.
  The answer is opt-in by capability, a one-day default, a floor, a dry run and
  counters.
- **Black (risks).** See 0.3. The open risk is R5. A store that keeps the old
  creation time on overwrite and also reuses keys is unsafe. The trait doc
  states the rule. Re-checking the references just before the delete shortens
  the window.
- **Yellow (benefits).** Storage stops growing after rolled-back writes. No
  write-path cost. Existing stores keep working.
- **Green (new ideas).** An `ObjectPayloadStore` listing and S3/GCS adapters
  can follow. They need `ObjectBackend::list`. This change keeps the core
  seam only.
- **Blue (process).** Red phase: failing unit and DB tests first. Green phase:
  the smallest code that passes. Refactor: shared helpers, docs, comment
  hygiene. Then a review from several angles.

---

## 1. Design

### 1.1 Trait

```rust
fn list(&self, after: Option<&str>, created_before: DateTime<Utc>, limit: usize)
    -> PayloadStoreFuture<'_, Option<BlobPage>>
```

- The default returns `Ok(None)`. `None` means the store cannot list.
- `BlobPage { blobs: Vec<StoredBlob>, next: Option<String> }`.
- `StoredBlob { key, created_at }`.
- A store returns blobs in key order, after `after`, older than
  `created_before`, at most `limit`. `next` is the cursor for the next page.
  `None` means the end.
- A `put` of an existing key resets its creation time. `delete` of a missing key
  succeeds.
- The issue sketch has a `prefix` argument. A store knows its own prefix, so
  the trait omits it.

### 1.2 Sweep

Module `orphan_blob_sweep`. One pass:

1. Compute `cutoff = now - grace`.
2. List up to `batch_size` blobs, for at most 10 pages per tick.
3. Drop any blob not older than `cutoff`.
4. Ask every physical database which keys it references. Any error aborts.
5. The unreferenced keys are the orphans. Count them as `found`.
6. Outside a dry run, for each orphan: check its references again, read its
   creation time again, then delete it.
7. Count `deleted` and `failed`.

### 1.3 Runtime and config

- `RetentionConfig::orphan_blob_grace_secs: Option<u64>`. Default one day.
  `None` turns the sweep off. Builders: `with_orphan_blob_grace`,
  `without_orphan_blob_sweep`.
- The retention task keeps the cursor and the "listing unsupported" flag as
  local state.
- Metrics: `harvest.retention.orphan_blobs_found`,
  `harvest.retention.orphan_blobs_deleted`,
  `harvest.retention.orphan_blob_delete_failures`.

## 2. Tests

| Test | Proves |
|------|--------|
| `default_list_is_unsupported` | Old backends keep working. |
| `select_orphans_*` (unit) | The cutoff and reference filters. |
| `config_*` (unit) | Default, builder and `validate` floor. |
| `a_rolled_back_append_blob_is_swept_after_the_grace_period` (DB) | Acceptance 1. |
| `a_blob_that_any_shard_references_is_never_deleted` (DB) | Acceptance 2. |
| `a_blob_younger_than_the_grace_period_is_never_deleted` (DB) | Acceptance 3. |
| `a_store_without_listing_disables_the_sweep` (DB) | Acceptance 4. |
| `a_dry_run_deletes_nothing` (DB) | R7. |
| `a_failed_reference_check_deletes_nothing` (DB) | R2. |
| `a_failed_delete_is_counted_and_retried` (DB) | R9. |
| `a_store_that_ignores_the_cutoff_*` (DB) | R3 with a store that ignores the filter. |
| `a_key_uploaded_again_after_the_listing_is_not_deleted` (DB) | R11. |
| `blobs_referenced_on_different_shards_all_survive` (DB) | R1 with a reference on each shard. |
| `a_hung_delete_times_out_and_is_counted` (DB) | R12. |
| `a_failed_listing_*`, `one_tick_reads_at_most_ten_pages` (DB) | Error and page cap. |
