# Release notes

[`CHANGELOG.md`](CHANGELOG.md) is the release history. It has an entry for
each version.

The release workflow (`.github/workflows/release.yml`) uses git-cliff to write
the notes for each GitHub Release. It writes them to this file in the release
runner only, and does not commit them.

For the upgrade steps of a version, see [`docs/upgrading/`](docs/upgrading/).
