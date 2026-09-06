# Slack multi-tenant example — Postgres storage

The same `SlackBot.make` API as the memory example, with **library-owned Postgres
connections, subscriptions/routing and delivery**. The example has no row schema,
repository, codec, migration, or cache invalidation implementation.

## Composition

```ts
// src/app.ts — behavior is independent of storage
export const bot = SlackBot.make({ namespace: 'slack-multi-tenant', handlers })
export const application = bot.layer
```

[`src/handlers.ts`](./src/handlers.ts) contains plain Effect callbacks;
[`src/responses.ts`](./src/responses.ts) owns message/stream formatting.
Native Slack resolves authors automatically, using provider-owned state/cache.

[`src/transport.ts`](./src/transport.ts) provides
`@humanlayer/channels-slack/postgres`'s `layer`, one configured scoped PgClient,
HTTP and crypto. Library migrations finish at acquisition before routes/workers
start. The host's scope owns the pool and shutdown. To use Redis instead, select
`@humanlayer/channels-slack/redis` and provide the neutral Redis client; no handlers
or delivery policies change. See the [service graph](../../packages/slack/README.md#service-graph).

The old `src/store.ts` / `SlackConnectionRow` have been removed. Applications with
existing custom tables may instead implement `SlackConnectionStore`'s domain-level
`get` / `upsert` / `remove`. They still do not implement Slack's cache or hydration.

## Setup

From the repository root after installing/building, select a database you intend
this example to modify. Use the manifest/tunnel setup from the
[echo example](../slack-thread-echo/README.md#setup). One app has one signing secret;
the signed payload's `team_id` selects its installation.

```sh
export DATABASE_URL='postgres://localhost/slack_example'
export SLACK_SIGNING_SECRET='...'
export SLACK_TEAM_ID='T...'
export SLACK_BOT_TOKEN='xoxb-...'
export SLACK_BOT_USER_ID='U...'
export SLACK_BOT_ID='B...'
export PORT=3000

bun run --cwd examples/slack-multi-tenant seed
bun run --cwd examples/slack-multi-tenant start
```

`seed` parses **all** installations before acquiring SQL or writing, then calls
`SlackState.upsertConnection`. The server subsequently requires `DATABASE_URL`
and `SLACK_SIGNING_SECRET`, not the seed variables. Tokens are redacted in domain
values but stored as plaintext credential material; protect database access,
transport, storage and backups. Do not enable query-parameter logging.

For a second installation of the same app, also set all four suffix-`_2` variables:
`SLACK_TEAM_ID_2`, `SLACK_BOT_TOKEN_2`, `SLACK_BOT_USER_ID_2`, `SLACK_BOT_ID_2`.
An absent secondary installation is allowed; partial configuration fails.

Credentials and missing installations are **not TTL-cached**. Upserts/removals are
visible to subsequent inbound/outbound requests across runtimes. Applications
with an installation lifecycle call `SlackState.upsertConnection` /
`removeConnection`; no manual cache invalidation is required. Already-started
requests are not transactionally revoked. Removal does not delete pending work
or subscriptions. OAuth and ownership/authorization endpoints are not implemented.

Set Slack's Request URL to
`https://YOUR-TUNNEL-HOST/api/v1/integrations/slack/webhook`. Invite the bot in each
workspace. Unknown installations are acknowledged and dropped; store failures
return a retryable HTTP error instead.

## Behavior and persistence

Mentions explicitly subscribe and echo; subscribed follow-ups and DMs/MPIMs reply.
`stream` triggers streaming with post/edit fallback; `reaction` adds a check.
Edits, deletes, reactions and Stop have lifecycle handlers with stable IDs.

The [initial delivery policy](../slack-thread-echo/README.md#delivery-limits) is
unchanged. Message queues coalesce pending messages into latest + `context.skipped`;
unrelated registrations and workspaces are independent. Durable stores preserve
accepted work, frozen retries, leases, subscriptions/routing and installations
across runtime reconstruction. Multiple workers share conditional ownership.
All workers need compatible clocks and registration/policy configuration.
External Slack writes can still repeat after recovery; this is not exactly-once.

New tables use `humanlayer_slack_v1_*` and `humanlayer_delivery_v1_*`.
**No automatic migration** from `example_slack_installations_v1`,
`example_slack_connections`, or historical Channels mailbox tables is attempted.
Before switching an existing deployment: stop old ingress, inventory/back up old
work, drain or export it with the matching old runtime, explicitly translate
subscriptions/connections, verify pending counts, then switch ingress and workers.
Do not copy legacy mailbox JSON or drop old tables automatically.

## Verification

```sh
bun run --cwd examples/slack-multi-tenant test
bun run --cwd examples/slack-multi-tenant typecheck
bun run --cwd examples/slack-multi-tenant build
```

Default tests use the actual application graph with memory storage and Emulate:
DM replies, unknown/removed installations, and seed parsing. They require no DB.
Build output is workspace-dependent, not a standalone deployment archive.

Root `bun run test:backend:postgres` and `bun run test:backend:redis` create fresh
disposable containers. They run real adapter contracts and the same bot definition
over each backend, including admission before execution and latest/skipped delivery.
External Slack HTTP is replaced at its actual transport seam for those contracts.

The older optional example-specific outbound credential checks remain separate:

```sh
TEST_DATABASE_URL='postgres://localhost/disposable_slack_test' \
  bun run --cwd examples/slack-multi-tenant test:postgres
```

They create/drop a unique test schema and never fall back to `DATABASE_URL`.
Both checks passed on a separate fresh disposable Postgres container during Phase 2
verification; they are counted separately from the four root adapter contracts.
Normal configurations structurally exclude all backend suites.
