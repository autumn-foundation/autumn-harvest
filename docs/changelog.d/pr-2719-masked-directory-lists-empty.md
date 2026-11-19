## A masked directory lists as empty (issue #2719)

`FsIsolation::Mask` mounted a `tmpfs` with mode `000` over a masked
directory. The code got `Permission denied` for a listing. The docs and
criterion 1 of `DESIGN-2697.md` say that the directory lists as empty.

- The mask `tmpfs` now has mode `555`. A listing of the directory succeeds
  and shows no entry. A read of a file under it fails with `ENOENT`.
- The mount stays read-only, `nosuid`, `nodev` and `noexec`, so the code
  cannot write the directory. The mask still shows no host data.
- A file or a socket mask is unchanged. It still binds `/dev/null`.
- No migration. No `WorkflowEvent` variant. No replay impact.
- Tests: `a_masked_directory_lists_as_empty` now checks the exit code of
  `ls`. Its old pipe into `wc -l` printed 0 for an error too.
  `a_masked_directory_hides_its_files` checks `ENOENT`.
  `a_masked_directory_is_read_only` checks mode `555` and a failed write.
