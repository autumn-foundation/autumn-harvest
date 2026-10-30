## Phase 7.x — Durable workspace snapshots (agent workspace store)

`DirWorkspaceBlobs::put` now syncs the scratch file before the rename. It then syncs the folder, and the parent of each folder that it creates. A sync error fails the put. Before, a crash after a committed tool call could lose the snapshot that the history names. Replay does not rerun a completed tool call, so the workspace was lost for good.

Tests: `a_put_syncs_the_file_before_the_rename_and_the_folder_after_it`, `a_put_syncs_the_parent_of_each_folder_that_it_creates` and `a_put_into_an_existing_folder_skips_the_parent_sync`. On Windows the folder sync does nothing, because a directory cannot be opened there.
