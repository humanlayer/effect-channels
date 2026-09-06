# Channels: native provider modules and shared delivery

Phase 1 implements native Slack plus a shared, typed in-memory mailbox engine.
`packages/app` and the old `packages/channels` facade are removed.

- [`packages/slack`](./packages/slack/) — native operations, schema-backed handles,
  subscriptions, credential Layers, signed routes, and typed ingress handlers.
- [`packages/delivery`](./packages/delivery/) — admission, queue/latest-and-skipped,
  bounded retries, leases, cancellation, memory storage, and scoped runners.
- [`examples/slack-thread-echo`](./examples/slack-thread-echo/) — direct Effect Layer
  composition with memory; no database needed. Effect HTTP and Fetch entrypoints.
- [`examples/slack-multi-tenant`](./examples/slack-multi-tenant/) — existing optional
  Postgres **credential repository**, with memory delivery and subscriptions.
- [`packages/postgres`](./packages/postgres/) — isolated historical SQL code for
  migration/drain work, **not** a new shared-delivery Postgres backend.

## Verification

```sh
bun install
bun run typecheck
bun run test
bun run check
bun run build
```

Commands use the workspace-local `vp` binary. If invoking it directly, use
`./node_modules/.bin/vp test` rather than a separately installed global launcher.

## Slack provider testing

Default tests use supplied memory Layers, real provider/shared-delivery code,
and Vercel Labs `emulate@0.11.0`. No database, workerd, live Slack credentials,
recording sessions, or recorded-traffic replay is required. Vite env-file loading
is disabled, and backend suites are structurally excluded even if `DATABASE_URL`
is already set.

`packages/slack/test/integration` starts isolated Slack emulators and closes each
listener/runtime through Effect scopes. It covers public/private mentions,
subscription follow-ups, DMs, own-bot suppression, other-bot history, files,
edits/deletes/reactions, fallback streaming, cancellation, admission-before-work,
and provider-visible replies. Protocol tests cover signatures, handshakes,
partial-admission failures, replay routing, native streaming/status, and queue
coalescing. Shared-engine tests cover conflicts, bounded retention, retries,
lease recovery, independent mailbox scheduling, and stale/duplicate controls.

Emulate does not implement every Slack API: native streaming and exact pagination
remain synthetic protocol tests, not claimed emulator coverage. Memory recovery
means reconstruction over the same acquired store, not process-crash durability.

Postgres/Redis delivery Layers, built declaration/optional-peer isolation checks,
Alchemy deployment, OAuth, additional delivery modes, and new providers are not
part of Phase 1. No backend tests or deployment were run. Archived incomplete
app-backend test sources are explicitly marked as non-executable Phase 2 work.

`bun run test:live:slack` optionally launches the memory echo server for manual
Slack testing. It contacts real Slack only when deliberately run and is not an
acceptance requirement.
