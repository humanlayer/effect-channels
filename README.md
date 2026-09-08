# Channels: native providers and shared delivery

Native Slack has interchangeable memory, Postgres and Redis storage, with shared
queue, concurrent, debounce, drop and burst delivery. There is no universal
provider facade, app framework, or agent engine.

- [`packages/slack`](./packages/slack/) — native operations, signed ingress,
  ergonomic bot composition, required connection CRUD, and provider-owned state/cache.
- [`packages/delivery`](./packages/delivery/) — one admission, latest-and-skipped,
  retry, lease, cancellation and runner implementation across all storage adapters.
- [`packages/github`](./packages/github/) — native issues/comments and targeted issue/PR discussion mentions, signed ingress,
  App installation authentication and repository-scoped token caching; no Slack dependency.
- [`examples/github`](./examples/github/) and [`examples/slack-github`](./examples/slack-github/)
  — independent and combined memory-backed Effect HTTP/Fetch hosts, verified with provider emulators.
- [`examples/slack-thread-echo`](./examples/slack-thread-echo/) — memory storage,
  configured installation, Effect HTTP and Fetch hosts; no database.
- [`examples/slack-multi-tenant`](./examples/slack-multi-tenant/) — the same bot
  API with library-owned Postgres connections, subscriptions/routing and delivery.
- [`examples/slack-redis`](./examples/slack-redis/) — Redis with local Docker
  Compose, explicit connection seeding, and typed Postgres/mixed storage recipes.

## Verification

```sh
bun install
bun run build
bun run typecheck
bun run test
bun run check
bun run verify:exports
```

Slack, GitHub and delivery have built ESM/declaration
exports. Commands use the local `vp` binary. Normal tests use memory, real native
provider/delivery code and `emulate@0.11.0`; no live credentials, database, workerd,
recordings, or replay. Vite env loading is disabled and backend suites are excluded
structurally, regardless of ambient `DATABASE_URL` / `REDIS_URL`.

Relative imports and re-exports use emitted extensions (`.js`, or `.mjs`/`.cjs`
for `.mts`/`.cts` sources); bundler-only code may use extensionless imports.
Do not import TypeScript source extensions. Both `allowImportingTsExtensions`
and `rewriteRelativeImportExtensions` stay disabled, including in build configs.
Literal source entry paths (package export source conditions, Vite aliases, CLI
entry files) are not module specifiers and retain their source extensions.
`bun run verify:imports` tests compiler/lint rejection, including the compiler's
rewrite-option loophole, and scans active code plus lint tooling. It also runs
before `check` and `lint`. Oxlint's TypeScript plugins are bundled to ignored
JavaScript by `build:lint` during install/prepare and before lint/check; run it
manually after changing plugin sources if invoking `vp` directly.

Optional **disposable Docker** verification:

```sh
bun run test:backend:postgres
bun run test:backend:redis
```

These scripts create a fresh container per suite and remove owned containers and
volumes afterward. They use fixed loopback ports 55432/56379 and refuse a bind
conflict; they do not connect to application databases. Images: `postgres:17-alpine`,
`redis:7-alpine`. See each package's `test-backends/README.md` for direct-suite
commands, assertions and limitations.

`verify:exports` builds/packs actual packages, installs isolated consumers without
optional peers and then with them, checks strict declarations and ESM imports,
bundles real browser output without backend/Node code, and checks shared service
identity and a single Effect runtime. It needs registry access and never acquires
backend Layers. Avoid running it concurrently with another build, since it cleans
generated `dist` output before packing.

## Scope and limitations

The historical `packages/postgres` implementation is removed from the active
workspace; its source and tests remain in Git history at `e7894f0` / `f27947c`.
Removing source does not drain, migrate or delete existing data. New Slack and
delivery adapters do not adopt `channels_conversations`,
`channels_conversation_mailbox`, `channels_user_profile_cache`, or the old
`channels-subscriptions` namespace in `effect_persistence`.

Before switching an old deployment, separately authorize an offline drain or
export/import: stop old ingress and writers, inventory pending mailbox rows and
subscriptions, and back up the old namespace. Use a separately recovered and
validated historical runner for supported events, or export unsupported payloads
unchanged; verify pending counts before switching ingress to new storage. Copying
legacy rows into new delivery state is not a migration. No automated legacy
migration or active legacy runner is provided, and no old tables may be silently
dropped. This is separate from the shared delivery v1→v2 upgrade below.

Memory is volatile. Postgres and Redis tests prove storage/lease/routing semantics,
not power-loss or replica-failover guarantees. Redis requires appropriate persistence,
noeviction and operational headroom; the initial delivery adapter uses one hash slot
and prefix indexes with write amplification. External Slack writes can repeat after
recovery. Credentials are redacted in application values, not encrypted at rest.

See [delivery modes and the v1→v2 upgrade procedure](./packages/delivery/README.md)
before upgrading an existing store. Accepted v1 work remains readable; stop old
writers before deploying code that writes v2. Mixed-version operation is unsupported.

Alchemy/Cloudflare deployment, OAuth, rotating App configuration, interrupt delivery,
GitHub PR management/reviews/workflows, custom App-bot assignment routing and further providers remain unimplemented.
GitHub installation tokens refresh before expiry; App setup remains explicit configuration.
Emulate gaps such as native Slack streaming still
use signed synthetic protocol tests. No live Slack or application database was used.

`bun run test:live:slack` launches the echo server only when deliberately invoked;
it is not automated acceptance coverage.
