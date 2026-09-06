# Deferred app-backend regression sources

The `.ts.txt` files preserve the interrupted app migration's optional backend
tests and fixture. They are **not executable or verified tests**: they referenced
an app package and shared-delivery Postgres adapters that did not exist.
Phase 1 removes that app package rather than implementing Phase 2 implicitly.

When Phase 2 is authorized, port the useful two-runtime and non-owner cancellation
scenarios onto its real storage Layers. Do not restore the wrapper, silently
enable these through `DATABASE_URL`, or assume FIFO ordering across handler IDs.
Working historical SQL coordinator tests remain in `packages/postgres/test`,
structurally excluded from default tests. No database was accessed here.
