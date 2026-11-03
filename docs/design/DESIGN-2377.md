# DESIGN-2377: the probe evidence bundle

Issue #2377. Parent: #2369. Needs #2375 and #2376. Related: #2274, #2272,
#2379.

## Problem

A proposer or a composer must read the full evidence of a probe. That is
the source run, the run of each candidate, the turn diffs and the current
overlay. `eval::evaluate` returns structs. A prompt-sized text of a long
run cuts it. Harness² serves the evidence as files, and the prompt holds
only a short pointer (`harness2/core/context.py`).

## Decision

`eval::evidence::write_bundle` writes the evidence into one workspace
snapshot (#2274) and returns a `BundleManifest`.

```text
task.md                      the user message of the source run
rollouts/source.md           the full source run
rollouts/<anon id>.md        one per candidate, under #2376 ids
diffs/<anon id>.md           the turn diffs of each candidate
overlay/current.json         the overlay that the editor changes
flow_labels.json             the flow labels of the source run
MISSION.md                   a short pointer for the editor
```

- **Parts.** A file over the part cap splits into
  `<stem>.part-001.md`, `<stem>.part-002.md` and on. A part ends after
  a newline when it can, and always on a UTF-8 boundary. The parts
  joined in order equal the single text byte for byte. Nothing is cut.
  The default cap is 256 KiB.
- **One put.** The writer builds the whole bundle in memory, then calls
  `WorkspaceStore::save` once. That is one blob put. A put that stops
  before its rename leaves no key, so no reader sees a partial bundle.
- **Erasure.** A history with an erasure tombstone gives
  `EvidenceError::Erased` before any render.
- **Flow labels.** `flow_labels.json` holds every label that the source
  run recorded: the task labels, the labels of each model request, tool
  call, model turn and memory snapshot, and the labels of each report.
  `read_bundle` returns them with the files. A run that reads the bundle
  starts with these labels, as a run that continues a transcript does.
- **Anonymous ids.** `eval::anon` gives each candidate an id `c-<hex>`.
  The hex is SHA-256 over the seed, the source run id and the overlay
  digest of the candidate. All ids use the shortest common prefix of at
  least 12 digits that keeps them distinct. The files list the
  candidates in id order, so the input order does not show. #2376 reuses
  this module for `compare_anonymised`.
- **No identity in a file.** The manifest keeps the id map. No file
  holds it. `EvidenceWorkspace::hide` names more text to keep out, such
  as model names and overlay version ids. The writer masks each hidden
  name and each candidate digest in the rendered files. It then checks
  every file again and refuses a leak with `EvidenceError::Leak`.
  `overlay/current.json` is the overlay that the editor changes, so it
  can name its own model. A candidate digest in it is still a leak.
- **The proposer.** `refine::Evidence::Bundle` holds a manifest. Its
  `render` is the text of `MISSION.md`. `ModelProposer` has no
  workspace, so it refuses a bundle as a `Config` error.

## Brainstorm

| Question | Options | Choice |
|---|---|---|
| Where the bytes go | One blob per file. One snapshot. | One snapshot. It is one put, so a crash leaves all or nothing. |
| A file over the cap | Cut it. Split it. | Split it. The issue forbids a cut. |
| Where a part ends | At the cap. After a newline. | After the last newline under the cap, else on the last UTF-8 boundary. A reader sees whole lines and valid text. |
| Where the labels go | In the manifest only. In a file of the snapshot. | In a file. The snapshot then carries them, and a reader of the snapshot gets them back. |
| The id map | In `MISSION.md`. In the manifest. | In the manifest, as an index into the evaluations. An index names no model and no overlay. |
| A hidden name in text | Refuse the bundle. Mask it. | Mask it, then check again. Model text can name a short model key such as `fast`. A refusal would block every probe of that model. |
| A hidden name in `current.json` | Mask it. Refuse. Allow it. | Allow a model name. The editor needs the real overlay. Refuse a candidate digest. |
| Markdown in model text | Write it as is. Fence it. | Fence it with a fence longer than any backtick run inside. Model text then cannot forge a heading of the bundle. |
| Where the manifest type lives | `eval`. `refine`. | `refine::bundle`. `Evidence::Bundle` needs it without the `eval` feature. `eval::evidence` re-exports it. |
| Two candidates with one overlay digest | Refuse. Count them. | Count them. The tie order uses the SHA-256 of each evaluation, so the input order does not change an id. |

## Reverse brainstorm

How can the bundle mislead an editor? Each failure has a guard.

| Failure | Guard |
|---|---|
| A long run loses its tail. | Parts, never a cut. A test joins the parts of a 1 MiB run and compares bytes. |
| A crash leaves half a bundle. | One put. A test stops the put before its rename and finds no key. |
| An erased payload reaches a file. | The tombstone check runs before any decode or render. |
| The editor loses the taint of the source. | `flow_labels.json`, read back by `read_bundle`. |
| A file names the model of a candidate. | The mask and the second check. A test searches every file. |
| The order shows which candidate came first. | The files use id order. A test swaps the input and compares files. |
| Model text forges `## Turn 3` or a closing fence. | Each text sits in a fence that it cannot close. |
| A part splits a UTF-8 character. | A part ends on a character boundary. A proptest checks multibyte text. |
| A cap of 1 byte loops forever. | The cap is at least 4 bytes, so each part holds one character. |
| Two ids collide. | All ids use the prefix length that keeps them distinct. |
| A mask joins with the text into a hidden name. | The second check refuses it. |
| A bundle over the store cap is written in part. | `save` checks the cap before its put. |

## Six hats

- **White.** `WorkspaceStore::save` makes one `put`. `DirWorkspaceBlobs`
  writes a scratch file, then renames it. `Evaluation` holds the turn
  diffs, both run ends and the overlay digest. It holds no model name.
- **Red.** A pointer with no file access is a trap. So `ModelProposer`
  refuses a bundle rather than send an empty pointer.
- **Black.** A mask changes the evidence text. It touches only a name
  that the operator chose to hide, and the source run keeps its bytes
  when nothing is hidden.
- **Yellow.** The editor reads the whole run. The proposer request
  holds a small manifest, so it stays under the activity input cap.
- **Green.** A later issue can add a workspace tool to the proposer and
  read the bundle in place.
- **Blue.** Red tests first. Then the anon ids, the renders, the split
  and the writer. Then the Verus proof of the split. Then the docs and
  the gates.

## Proof

`formal/verus/src/evidence_parts.rs` mirrors `part_end`. A spec runs the
split over any cut choices. It proves that the parts tile the text: each
part is not empty, each part is at most the cap, each part starts where
the last one ended, and the last part ends at the end of the text.

## Acceptance

| Criterion | Test |
|---|---|
| A 1 MiB run splits into parts that join to the single render. | `a_long_source_splits_into_parts_that_join_to_the_single_render` |
| The manifest lists every file, and each one exists. | `the_manifest_lists_every_file_and_each_file_exists` |
| An erased source is refused. | `an_erased_source_is_refused_with_erased` |
| The snapshot carries the flow labels. | `a_labelled_source_reads_back_with_its_labels` |
| No file names a model or an overlay id. | `no_file_names_a_candidate_model_or_overlay_id` |
| One put. A stopped put leaves no bundle. | `the_bundle_is_one_put`, `a_stopped_put_leaves_no_bundle` |

## Limits

- `ModelProposer` cannot read a bundle. A proposer with a workspace tool
  is future work.
- `eval::compare_anonymised` is #2376. This issue adds only the ids that
  it reuses.
- The candidate file shows the decisions of each turn and the run end.
  A candidate tool result is the recorded one or the stub, so the source
  file shows it.
