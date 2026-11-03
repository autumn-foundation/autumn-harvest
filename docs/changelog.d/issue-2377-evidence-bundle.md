## Probe evidence bundle as workspace files (issue #2377)

A proposer or a composer can now read the full evidence of a probe from
files. Parent: #2369. It builds on the overlay candidates of #2375 and the
workspaces of #2274.

- **Writer.** `eval::evidence::write_bundle(workspace, source,
  evaluations, overlay)` writes `task.md`, `rollouts/source.md`, one
  `rollouts/<id>.md` and `diffs/<id>.md` per candidate,
  `overlay/current.json`, `flow_labels.json` and `MISSION.md` into one
  workspace snapshot. It returns a `WrittenBundle`: the `BundleManifest`
  for the editor and the id-to-evaluation map for the caller.
  `read_bundle` loads the files and the flow labels.
- **Parts.** A Markdown file over the part cap splits into numbered parts.
  The parts joined in order equal the single render. Nothing is cut.
  `formal/verus/src/evidence_parts.rs` proves that the parts tile the text.
- **One put.** The bundle is one `WorkspaceStore::save`. A put that stops
  before its rename leaves no key.
- **Erasure and labels.** An erased source gives `EvidenceError::Erased`.
  An encoded payload gives `Undecodable`. `flow_labels.json` carries the
  flow labels of the source run (#2272), mandate and peer labels included.
- **Anonymous ids.** `eval::anon` gives each candidate an id `c-<hex>`
  from a required seed, the source run id and the overlay digest. #2376
  can reuse it. The writer masks each candidate digest and each hidden
  name, then refuses a leak with `EvidenceError::Leak`. A recorded name
  is a JSON string in a code span, so it cannot forge a heading.
- **Proposer.** `refine::Evidence::Bundle` holds a manifest. Its `render`
  is the `MISSION.md` pointer. `ModelProposer` refuses a bundle as a
  `Config` error, because it has no workspace.
- No new `WorkflowEvent` variant, no migration and no SQL.
- Tests: `tests/evidence.rs` covers the criteria on recorded SQLite runs.
  The unit and property tests in `eval/evidence.rs` and `eval/anon.rs`
  cover the split, the ids and the masks. The stopped-put unit test in
  `eval/evidence.rs` stops a real `DirWorkspaceBlobs` put before its
  rename.
