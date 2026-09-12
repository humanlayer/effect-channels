# Slack + Redis (with Postgres and custom storage recipes)

A standalone Slack echo app built on the Phase 1 native handlers and Phase 2
storage contracts. Bun runs the app on your host; Docker Compose runs **storage
only**. There is no application facade, Redis client in a handler, or custom queue.

## Run locally

Install dependencies and build the workspace packages from the repository root
(their runtime exports point to generated `dist` files):

```sh
bun install
bun run build
```

Then run these commands from `examples/slack-redis`:

```sh
cp .env.example .env
# Edit .env with your Slack signing secret and installation credentials.
docker compose up -d --wait redis
bun run seed
bun run start
```

Use the existing [Slack manifest](../slack-thread-echo/slack-manifest.json) and
[Slack app/credential setup](../slack-thread-echo/README.md#setup). Point its event
and interactivity URLs at your tunnel's:

```text
https://YOUR-TUNNEL/api/v1/integrations/slack/webhook
```

The server listens on port `3000` (`PORT` overrides it). Invite the bot to a
channel and mention it. **Wait for the initial mention's echo/subscription before
testing plain threaded messages**, then reply in that thread. The mention and an
isolated follow-up each receive `Durable echo: …` after the quiet period. Rapid
follow-ups produce only the latest echo. DMs use the same debounce policy. Stop the
app, restart it **without seeding again**, and reply in the subscribed thread;
its connection and subscription remain in Redis (subject to subscription TTL).

`seed` explicitly upserts one installation via `SlackState`. Re-running it
replaces that workspace's saved credentials; normal server startup never
overwrites them from environment variables. After seeding, the server only needs
`REDIS_URL`, `SLACK_SIGNING_SECRET`, and optionally `PORT`. Keep bot credentials
available securely if you need to seed again. This is not OAuth or token rotation.

## The composition

The actual [`src/app.ts`](./src/app.ts) selects non-default delivery:

```ts
import { SlackBot } from '@humanlayer/channels-slack'

import { handlers } from './handlers.js'

export const bot = SlackBot.make({
	namespace: 'slack-redis',
	handlers,
	policy: { mode: 'debounce', quietPeriodMs: 1500 },
})
export const application = bot.layer
```

Within a message mailbox, debounce waits for a 1500 ms quiet period. Each new
eligible message resets that period; duplicate deliveries do not. Active work is
not interrupted. Earlier events in the selected batch are available through
`context.skipped`, but this echo handler ignores that context and replies only to
the latest message. Lifecycle delivery stays serial; it is not debounced.

| File                                                           | Responsibility                                             |
| -------------------------------------------------------------- | ---------------------------------------------------------- |
| [`src/handlers.ts`](./src/handlers.ts)                         | Storage-neutral native Slack callbacks                     |
| [`src/app.ts`](./src/app.ts)                                   | `SlackBot.make`, stable namespace, routes + scoped worker  |
| [`src/storage.ts`](./src/storage.ts)                           | The one storage selection used by **both** server and seed |
| [`src/storage/redis.ts`](./src/storage/redis.ts)               | Complete Redis storage bundle                              |
| [`src/storage/redis-client.ts`](./src/storage/redis-client.ts) | One scoped node-redis client                               |
| [`src/transport.ts`](./src/transport.ts)                       | Storage + outbound HTTP + platform crypto                  |
| [`src/server.ts`](./src/server.ts)                             | HTTP host and graceful shutdown                            |
| [`scripts/seed.ts`](./scripts/seed.ts)                         | Explicit installation bootstrap, no worker or Slack calls  |

The Redis composition is simply:

```ts
import { layer } from '@humanlayer/channels-slack/redis'
import { Layer } from 'effect'
import { redisClient } from './redis-client.js'

export const storage = layer.pipe(Layer.provide(redisClient))
```

That bundle provides `SlackConnectionStore`, `SlackSubscriptions`, `MailboxStore`,
and `MailboxReadiness`. It owns persistence codecs/scripts; Slack owns its
disposable profile cache. `SlackBot.make` owns native service assembly and the
worker. The host acquires one client for the complete bundle, not one per store
or request, and closes it on shutdown.

The client subpath uses **node-redis**, not ioredis. Its current options require
a plain URL string, so `redis-client.ts` unwraps `Config.redacted('REDIS_URL')`
only at that client boundary. Keep URLs and driver diagnostics private. Pure Redis
startup does not import/acquire the Postgres client or read `DATABASE_URL`.

## Swap to Postgres

Change the single export in `src/storage.ts`:

```ts
export { storage } from './storage/postgres.js'
```

Then, from this example directory:

```sh
docker compose --profile postgres up -d --wait postgres
bun run seed
bun run start
```

[`storage/postgres.ts`](./src/storage/postgres.ts) provides the same bundle using
one `PgClient` pool configured with `Config.redacted('DATABASE_URL')`. Standard
versioned migrations run on layer acquisition. Use a database/schema the library
may initialize; this example's Compose database is dedicated to the example.
Redis is not required for this selection.

All recipe files are typechecked. This example includes `@effect/sql-pg` and
`@types/pg` so you can switch without installing anything. A Redis-only app can
omit those dependencies and the Postgres/mixed recipe files. Redis needs
`@effect/platform-node` and `redis`; all Effect packages must match the workspace
pin (`4.0.0-rc.112`).

**Selecting a backend is not data migration.** Stop the old app before switching,
seed the new store, and deliberately drain/migrate any outstanding work and
subscriptions. Keep the namespace and handler IDs stable for existing work.
These recipes never copy records or drop old tables/keys.

## Custom composition: Postgres installations, Redis delivery

Change `src/storage.ts` to:

```ts
export { storage } from './storage/custom.js'
```

Start both stores before seeding/starting:

```sh
docker compose --profile postgres up -d --wait
bun run seed
bun run start
```

The **runnable, typechecked** [custom recipe](./src/storage/custom.ts) is:

```ts
export const storage = Layer.merge(
	connections.pipe(Layer.provide(postgresClient)),
	Layer.merge(subscriptions, delivery).pipe(Layer.provide(redisClient)),
)
```

Here `connections` comes from `@humanlayer/channels-slack/postgres`,
`subscriptions` from `@humanlayer/channels-slack/redis`, and `delivery` from
`@humanlayer/channels-delivery/redis`. Do not merge two complete storage bundles:
they provide overlapping service tags. Select exactly one implementation for
each contract. This mixed recipe is not a distributed transaction between Redis
and Postgres; it puts credentials in Postgres and mailbox/routing state in Redis.

### Bring your own installation repository

Replace only the `connections.pipe(...)` branch with your own
`Layer.effect(SlackConnectionStore, ...)` backed by your repository. Its required
domain contract is:

```ts
get({ workspaceId }): Effect<SlackConnection | undefined, SlackConnectionStoreError>
upsert({ workspaceId, connection }): Effect<void, SlackConnectionStoreError>
remove({ workspaceId }): Effect<void, SlackConnectionStoreError>
```

These signatures describe the adapter contract, not a copy-paste repository
implementation. Parse rows into `SlackConnection`, keep its token `Redacted`,
make upserts atomic and deletes idempotent, and keep reads authoritative. Safely
capture/map database failures to `SlackConnectionStoreError`; do not add a token
cache. Application installation mutations go through `SlackState`, which handles
profile invalidation. The seed command continues to work with your custom layer.
See [the public storage contract](../../packages/slack/README.md#connection-storage-the-required-custom-seam).

If replacing delivery storage too, implement **both** `MailboxStore` and
`MailboxReadiness`: conditional revision writes and readiness must commit
atomically. A generic key/value cache is not sufficient. Prefer the standard
delivery backend unless your platform actually requires a new adapter.

## Redis durability and local infrastructure

[`compose.yaml`](./compose.yaml) uses Redis 7, a named `/data` volume, AOF with
`appendfsync always`, and `maxmemory-policy noeviction`. The 256 MiB Redis limit
is a local example setting, not a sizing recommendation or container RSS limit;
leave headroom for AOF buffers/rewrites and monitor rejected writes. Healthchecks
make `up --wait` wait for readiness. Ports bind only to loopback:

- Redis: `127.0.0.1:56380`
- Optional Postgres 17: `127.0.0.1:55433`

No fixed container names are used. Choose a separate Compose project/ports for
parallel copies. The local Redis has **no password or TLS**; the Postgres password
is a public local placeholder. Do not expose either service publicly. Persisted
Slack tokens are plaintext in standard storage: protect volumes, snapshots and
backups. `Redacted` is not encryption at rest.

These are standalone development containers, not HA infrastructure. AOF is not
a power-loss/failover certification; production deployments need persistence,
backup/replication policy, noeviction, memory headroom and compatible worker clocks.
The initial delivery adapter uses a single Redis hash slot with prefix-index write
amplification. See [delivery guarantees](../../packages/delivery/README.md).

Only the message delivery mode and quiet period are overridden. The remaining
bounded `SlackBot.make` defaults are unchanged: five attempts, 100 ms–30 s retry
backoff, 30 s lease with 5 s heartbeat, and bounded payload/envelope/outcome retention. These are example
defaults, not production capacity recommendations. ACK follows durable admission;
handlers run in the scoped worker. External Slack posts are **not exactly once**
and may repeat after a crash between posting and committing completion.

Stop containers without deleting state:

```sh
docker compose --profile postgres down
```

Only if you deliberately want to erase **all this example's stored tokens,
subscriptions and work**, use `docker compose --profile postgres down -v`.

## Verification

From the repository root:

```sh
bun run --cwd examples/slack-redis typecheck
bun run --cwd examples/slack-redis build
node_modules/.bin/vp test examples/slack-redis/test
docker compose -f examples/slack-redis/compose.yaml config --quiet
```

Default example tests use the actual application, signed webhooks, memory stores
and Emulate, plus isolated seed configuration tests. A TestClock test uses the
actual bot's routes and scoped worker to verify quiet-period resets, duplicate
admission, and a latest-only emulator-visible reply without wall-clock sleeps.
They do not connect to the example's Redis/Postgres or prove disk durability. Optional library backend
contracts use fresh disposable containers, not these application URLs:

```sh
bun run test:backend:redis
bun run test:backend:postgres
```
