# Channels: native providers and shared delivery

Native Slack has interchangeable memory, Postgres and Redis storage, with shared
queue, concurrent, debounce, drop and burst delivery. There is no universal
provider facade, app framework, or agent engine.

- [`packages/slack`](./packages/slack/) — native operations, signed ingress,
  ergonomic bot composition, required connection CRUD, and provider-owned state/cache.
- [`packages/delivery`](./packages/delivery/) — one admission, latest-and-skipped,
  retry, lease, cancellation and runner implementation across all storage adapters.
- [`examples/slack-thread-echo`](./examples/slack-thread-echo/) — memory storage,
  configured installation, Effect HTTP and Fetch hosts; no database.
- [`examples/slack-multi-tenant`](./examples/slack-multi-tenant/) — the same bot
  API with library-owned Postgres connections, subscriptions/routing and delivery.
- [`examples/slack-redis`](./examples/slack-redis/) — Redis with local Docker
  Compose, explicit connection seeding, and typed Postgres/mixed storage recipes.
- [`packages/postgres`](./packages/postgres/) — separate historical SQL implementation,
  not the new delivery adapter. It is preserved for explicit legacy drain/migration
  work; no old tables are dropped or silently adopted by new adapters.

## Verification

```sh
bun install
bun run build
bun run typecheck
bun run test
bun run check
bun run verify:exports
```

The workspace has six projects; Slack and delivery have built ESM/declaration
exports. Commands use the local `vp` binary. Normal tests use memory, real native
provider/delivery code and `emulate@0.11.0`; no live credentials, database, workerd,
recordings, or replay. Vite env loading is disabled and backend suites are excluded
structurally, regardless of ambient `DATABASE_URL` / `REDIS_URL`.

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

Memory is volatile. Postgres and Redis tests prove storage/lease/routing semantics,
not power-loss or replica-failover guarantees. Redis requires appropriate persistence,
noeviction and operational headroom; the initial delivery adapter uses one hash slot
and prefix indexes with write amplification. External Slack writes can repeat after
recovery. Credentials are redacted in application values, not encrypted at rest.

See [delivery modes and the v1→v2 upgrade procedure](./packages/delivery/README.md)
before upgrading an existing store. Accepted v1 work remains readable; stop old
writers before deploying code that writes v2. Mixed-version operation is unsupported.

Alchemy/Cloudflare deployment, OAuth, token rotation, interrupt delivery and other
providers remain unimplemented. Emulate gaps such as native Slack streaming still
use signed synthetic protocol tests. No live Slack or application database was used.

`bun run test:live:slack` launches the echo server only when deliberately invoked;
it is not automated acceptance coverage.
