# Security policy

## Report a vulnerability

Report a vulnerability in private. Do not open a public issue, pull request or
discussion for it.

Use GitHub private vulnerability reporting:

1. Go to the
   [Security tab](https://github.com/autumn-foundation/autumn-harvest/security).
2. Select **Report a vulnerability**.
3. Complete the form. Only the maintainers can see the report.

The direct link is
<https://github.com/autumn-foundation/autumn-harvest/security/advisories/new>.

## What to include

- The affected crate, client or binary, and its version or commit.
- The configuration and the feature flags that you use.
- The steps or code that reproduce the problem.
- The effect: for example, data disclosure, privilege change or loss of
  workflow history.

## What happens next

1. A maintainer acknowledges the report in the advisory thread.
2. The maintainers confirm the problem and assess its severity.
3. The maintainers prepare a fix in a private fork.
4. The maintainers release the fix and publish a GitHub security advisory.
   The advisory credits the reporter, unless the reporter asks not to be named.

Do not disclose the problem in public before the advisory is published.

## Supported versions

The project is before 1.0. Security fixes go into the newest minor release only.

| Version | Supported |
|---|---|
| 0.7.x | Yes |
| < 0.7 | No |

## Scope

This policy covers the code in this repository: the published crates, the
`harvest` CLI and the TypeScript client.

The management API delegates authentication to the host application. A route
that is open because the host mounts it with no authentication is a deployment
problem, not a vulnerability in Harvest. Read
[`docs/security-posture.md`](docs/security-posture.md) before you deploy.
