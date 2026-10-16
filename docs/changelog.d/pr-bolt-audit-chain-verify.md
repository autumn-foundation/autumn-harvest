## Phase — audit chain verification reuses the keyed HMAC state

`ChainVerifier` sets each key up once and streams the canonical row into the
MAC. Verify-only cost over 5,000 rows drops from 176.8M to 139.2M
instructions (-21.3%) and from 44,999 to 10,000 allocations (-77.8%). No
output, event or schema changes. See `docs/performance-audit-chain-verify.md`.
