# Slack thread echo — memory delivery

A single-workspace Slack example using the native Slack APIs and shared delivery.
**No database or `packages/app` wrapper is required.**

## What runs where

- [`src/app.ts`](./src/app.ts) declares typed handlers and Layers: `transport`
  supplies Slack credentials/HTTP; `services` supplies native Slack, memory
  subscriptions, a profile cache, and delivery memory. `routes` only verifies and
  admits webhooks; `worker` explicitly runs handlers. `application` combines them.
- [`src/server.ts`](./src/server.ts) provides `transport` to `application` and
  starts one scoped Node HTTP server. SIGINT/SIGTERM close the server and worker.
- [`src/fetch.ts`](./src/fetch.ts) is the **alternative** Fetch entrypoint. It
  exports `handle(request): Promise<Response>` and `close()`, lazily initializes
  one runtime, and returns 503 for new requests after shutdown. Import it once,
  await `close()` when the host stops, and do not also launch `src/server.ts`.

For example, a long-lived Hono host can forward the untouched request:

```ts
import { handle, close } from './src/fetch.ts'

app.post('/api/v1/integrations/slack/webhook', (c) => handle(c.req.raw))
// Call await close() from the host's shutdown hook.
```

Do not parse/re-serialize the request before forwarding it: signatures cover the
original body. Mounting only `routes` requires a separately supervised `worker`
with the same acquired services. These hosts use process memory and background
fibers; this is not yet a durable serverless/Cloudflare deployment example.

## Setup

Run commands below from the repository root. Install workspace dependencies first:

```sh
bun install
```

### 1. Create or update the Slack app

In [Slack's app dashboard](https://api.slack.com/apps), create an app **from a
manifest** using [`slack-manifest.json`](./slack-manifest.json). That file is the
single source for scopes, events, Agent view, and writable Messages-tab settings.
Replace its placeholder webhook URL after starting your tunnel below.

For an existing app, update its manifest and reinstall when scopes change. Under
**App Home → Show Tabs**, enable Messages and allow users to send messages; the
manifest enables this with `messages_tab_read_only_enabled: false`.

### 2. Install it and obtain credentials

Copy the **Signing Secret** from **Basic Information → App Credentials**. Install
or reinstall the app under **OAuth & Permissions**, then copy its **Bot User OAuth
Token** (`xoxb-…`). Do not substitute a client secret, app token, or user token.

Obtain the workspace and bot identities with an explicitly credentialed request:

```sh
export SLACK_BOT_TOKEN='xoxb-...'
curl -sS https://slack.com/api/auth.test \
  -H "Authorization: Bearer $SLACK_BOT_TOKEN" | jq
```

Set `SLACK_BOT_USER_ID` to the response's `user_id`, and `SLACK_BOT_ID` to its
`bot_id`. The workspace `team_id` is useful for proactive API calls. These are
separate IDs; own-bot suppression must not suppress other integrations' messages.

### 3. Configure the server

```sh
export SLACK_SIGNING_SECRET='...'
export SLACK_BOT_TOKEN='xoxb-...'
export SLACK_BOT_USER_ID='U...'
export SLACK_BOT_ID='B...'
export PORT=3000

bun run --cwd examples/slack-thread-echo start
```

`PORT` defaults to 3000. `DATABASE_URL` is not used. The command inherits exported
variables; Bun can also load env files according to its working directory. Keep
secrets untracked. Library tests supply isolated configuration instead.

### 4. Expose the webhook and invite the bot

In another terminal run `ngrok http 3000`. Set Slack's **Event Subscriptions →
Request URL** to:

```text
https://YOUR-TUNNEL-HOST/api/v1/integrations/slack/webhook
```

Wait for Slack's verification success and save. Update the URL whenever the
hostname changes. Invite the bot into a public and private channel for testing.
The manifest subscribes to mentions, channel/private/DM/MPIM messages, reactions,
and `agent_session_stopped`.

## Try it

- Mention the bot: it loads up to 20 preceding channel messages, explicitly
  subscribes, and posts `Echo: …` in the same thread.
- Reply in the subscribed thread without mentioning it: it loads up to 100 recent
  messages and posts `Echo 2: …`.
- Send a DM/MPIM: it replies through the dedicated direct-message handler.
- Include **image** for an in-memory SVG upload, **reaction** for a check reaction,
  **edit** to edit the response, or **delete** to delete it.
- Include **stream** for an Effect stream. Native Slack streaming falls back to
  post-and-edit when unsupported. If both `stream` and `image` are supplied, the
  streaming response takes precedence.
- Edit/delete a message or add a reaction to exercise the lifecycle callbacks.
  Reaction registrations have explicit stable IDs, independent of array order.

History, files/security, DM routing, streaming, cancellation, and explicit
`EphemeralFallbackToDm` behavior have library regression coverage. To manually
exercise extra file combinations and authenticated downloads, temporarily invoke
[`runLiveFileChecks`](./src/live-files.ts) from a handler; it writes real Slack files.

## Delivery limits

This example explicitly selects queue/latest plus `context.skipped`, a 256,000-byte
envelope limit, 1,000 retained envelopes and 10,000 outcomes per mailbox, one-day
dedupe retention, five attempts with 100 ms–30 s backoff, a 30 s lease with 5 s
heartbeat, and 10,000 memory mailboxes. These are example settings, not production
defaults. Failed work counts against capacity; idle mailbox records are not recycled.

Handlers with different IDs are independent: there is no cross-handler order.
Lifecycle events run serially per handler, while message queues coalesce pending
work. Retried external Slack writes can repeat. Restarting loses all mailbox,
routing, and subscription state; Postgres/Redis delivery belongs to a later phase.

## Tests and builds

```sh
bun run --cwd examples/slack-thread-echo test
bun run --cwd examples/slack-thread-echo typecheck
bun run --cwd examples/slack-thread-echo build
```

Tests exercise this example's actual Layer graph against Emulate: admission before
worker startup, the reply, and a subscribed follow-up. They also check Fetch shutdown.
No live credentials or database are used. The build emits `dist/server.js` and
`dist/fetch.js` plus shared chunks; workspace dependencies remain external, so this
is not a standalone deployment archive.

`bun run test:live:slack` is an optional alias that launches this same server for
manual Slack testing—not an automated acceptance test. There is no recording or
replay harness. Run `bun run test` at the root for all normal tests.
