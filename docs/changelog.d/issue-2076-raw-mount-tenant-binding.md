## Fixed — tenant binding on a raw router mount (issue #2076)

`harvest_api_router` now installs the tenant binding layer. Before, a raw
mount whose middleware set `VerifiedTenant` did not confine the caller. Tenant
A could read, signal or cancel the run of tenant B by id.

A mount through `HarvestPlugin`, `HarvestEmbedding` or `StandaloneAdminAuth`
runs the layer once. The inner copy skips when the outer copy has run.
