# Slack multi-tenant example

A workspace-aware Slack example with **memory delivery** and an application-owned
Postgres **credential repository**. It is not the Phase 2 Postgres delivery backend.
Use [`slack-thread-echo`](../slack-thread-echo/) if you want no database at all.

## Composition

[`src/app.ts`](./src/app.ts) declares handlers, memory subscriptions/delivery, and
native Slack services. `routes` only admits work; `worker` runs it; `application`
combines them. The separate `transport` Layer uses:

```ts
SlackTenantCredentials.layerWithLookup({ loadConnection: loadSlackConnection })
```

The callback receives `{ workspaceId: SlackTeamId }` and returns a typed
`SlackConnection | undefined`. Slack validates the boundary and caches each
workspace's credentials for one minute. There are no organization gates,
enablement flags, or OAuth endpoints.

[`src/server.ts`](./src/server.ts) provides `transport`, the repository migration,
and one scoped SQL client before serving the combined application. `application`
and `transport` remain separate so tests can use real Slack clients against Emulate
with supplied credential Layers without acquiring a database.

[`src/store.ts`](./src/store.ts) owns `example_slack_installations_v1`. Startup
creates the table if absent but never overwrites credentials. The explicit seed
command upserts records. The old `example_slack_connections` namespace is untouched;
existing installations need an explicitly planned export/reseed, not automatic
migration. No delivery records are written to Postgres. Tokens are redacted in
application values but stored as database text: protect database access and backups.

## Setup

Run commands from the repository root after `bun install`. Use a database you
intend this example to modify, and the same Slack app manifest/tunnel setup as the
[echo example](../slack-thread-echo/README.md#setup). One Slack app has one signing
secret and webhook URL; the signed payload's `team_id` selects its workspace token.

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

`seed` requires a complete primary installation and validates **all** supplied
records before acquiring SQL or writing anything. No secrets are printed. The
server then reads tokens from the repository, not from the seed variables.
`DATABASE_URL` and `SLACK_SIGNING_SECRET` remain necessary for serving requests.

To seed a second installation of the **same app**, also supply all four variables:

```sh
export SLACK_TEAM_ID_2='T...'
export SLACK_BOT_TOKEN_2='xoxb-...'
export SLACK_BOT_USER_ID_2='U...'
export SLACK_BOT_ID_2='B...'
```

Run `seed` again with the primary and secondary configuration. An entirely absent
second installation is allowed; a partial one fails instead of silently skipping.
Credential changes may take up to one minute to pass through the local cache.

Set Slack's Request URL to
`https://YOUR-TUNNEL-HOST/api/v1/integrations/slack/webhook`. Invite the bot to the
channels in each workspace. Unknown installations are acknowledged and dropped;
lookup failures return a retryable HTTP failure instead.

## Behavior and limits

Mentions explicitly subscribe then echo; subscribed follow-ups and DMs/MPIMs also
reply. Include **stream** for streaming (with post/edit fallback), or **reaction**
in a mention for a check reaction. Edits, deletes, typed reactions, and Stop events
have lifecycle handlers. Each registration has a stable ID.

The explicit queue, retry, retention, lease, capacity, and worker settings in
`src/app.ts` match the [echo example](../slack-thread-echo/README.md#delivery-limits).
Mailboxes are installation-scoped; unrelated handlers are independent. Accepted
pending messages can coalesce into the latest message plus `context.skipped`.
There is no exactly-once guarantee for outgoing writes. On process restart,
credentials survive but **delivery, routing, and subscriptions do not**. Run a
single process for this memory example; multiple processes do not share ownership.

## Verification

Normal example tests are database-free:

```sh
bun run --cwd examples/slack-multi-tenant test
bun run --cwd examples/slack-multi-tenant typecheck
bun run --cwd examples/slack-multi-tenant build
```

They exercise the real application graph's DM reply and unknown-installation
handling against Emulate with supplied credential Layers, plus seed-config parsing.
The root `bun run test` includes them. The build produces a workspace-dependent
`dist/server.js`, not a self-contained deployment archive.

The optional repository SQL tests are **separate** and require an explicitly
provided disposable database:

```sh
TEST_DATABASE_URL='postgres://localhost/disposable_slack_test' \
  bun run --cwd examples/slack-multi-tenant test:postgres
```

This command creates and drops unique test schemas. It fails if `TEST_DATABASE_URL`
is missing; it never falls back to `DATABASE_URL` or loads env files. Normal test
commands structurally exclude `test-backends`. Merely setting a database variable
does not activate backend tests. These tests were typechecked but not executed as
part of the example update; they do not test shared-delivery SQL persistence.
