# Security policy

## Report a vulnerability

Report a vulnerability in private. Do not open a public issue, pull request or
discussion for it.

Use GitHub private vulnerability reporting:

1. Go to the
   [Security tab](https://github.com/autumn-foundation/autumn-harvest/security).
2. Select **Report a vulnerability**.
3. Complete the form. The report is private to you and the maintainers.

The direct link is
<https://github.com/autumn-foundation/autumn-harvest/security/advisories/new>.

If the form is not available, open a public issue with the title
"Security contact request". Do not put details of the problem in the issue. A
maintainer then gives you a private route.

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
   The advisory credits the reporter, unless the reporter asks for no credit.

Do not disclose the problem in public before the maintainers publish the
advisory.

## Supported versions

The project has not reached version 1.0. Security fixes go into the newest minor
release only. Older minor releases get no fixes. Upgrade to the newest release
to get a fix.

## Scope

This policy covers the code in this repository: the published crates, the
`harvest` CLI and the TypeScript client.

These problems are in scope:

- A bypass of the fail-closed gate. Outside the `dev` profile, a mount with no
  auth layer refuses each mutating route and each `/admin` route with `401`.
- A bypass of the scoped API token checks.
- Any other defect in the code of this repository.

A deployment choice is out of scope. Examples are the `dev` profile, an open
read-only route, or the `allow_unauthenticated_mutations()` opt-out. Read
[`docs/security-posture.md`](docs/security-posture.md) before you deploy.
