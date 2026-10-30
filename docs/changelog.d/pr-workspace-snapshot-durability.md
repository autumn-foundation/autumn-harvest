## Fix — Durable workspace snapshots (agent workspace store)

`DirWorkspaceBlobs::put` now syncs the scratch file before the rename. It then syncs the folder and its parent. Until one put has synced every ancestor of the folder, each put syncs them all, so a retry after a failed first put still makes the new root durable. A sync error fails the put. Before, a crash after a committed tool call could lose the snapshot that the history names. Replay does not rerun a completed tool call, so the workspace was lost for good.

Tests: `a_put_syncs_the_file_before_the_rename_and_the_folder_after_it`, `a_put_syncs_the_parent_of_each_folder_that_it_creates` and `a_retry_after_a_failed_first_put_syncs_the_ancestors`, `a_put_into_an_existing_folder_still_syncs_the_folder_and_its_parent`. On Windows the rename is not durable, because a directory cannot be opened there for a sync.
