# Alchemy Cloudflare Slack mailbox example

This example receives Slack webhooks in a Cloudflare Worker, stores each event in a mailbox Durable Object, and processes the mailbox from the object's alarm. The sample callbacks subscribe to a thread when the bot is mentioned, then react to each later message.

## Configure secrets

Create the local environment file:

```bash
cd examples/alchemy-cloudflare
cp .env.example .env
```

Copy these values from your Slack app into `.env`:

```dotenv
SLACK_SIGNING_SECRET=...
SLACK_BOT_TOKEN=xoxb-...
```

Alchemy reads these values during Worker construction and creates Cloudflare `secret_text` bindings. The values are not stored in mailbox admissions or Durable Object storage. `.env` is ignored by Git.

## Run

```bash
bun alchemy dev
```

Configure the Slack Events API request URL with the URL printed by Alchemy plus:

```text
/integrations/slack/webhook
```

Deploy with:

```bash
bun alchemy deploy
```

## Current behavior

- The Worker verifies Slack signatures and handles Slack URL verification.
- Valid events are admitted through the typed Durable Object RPC.
- Admissions and processing state use the object's persistent SQLite-backed storage.
- The alarm claims ordered batches and invokes the Slack processor. If a mailbox is still due and has no alarm afterwards, the handler sets one.
- Slack mailboxes use debounce delivery: a batch runs after 2 seconds of quiet, and no later than 10 seconds after its first event.
- On a new mention, the callback subscribes to the thread, shows typing, and posts a confirmation.
- In a subscribed thread, the callback adds an `eyes` reaction to each new message.
- Slack thread subscriptions are stored in Durable Object storage.
- Reaction events resolve the reacted message's thread root through `conversations.history`.

Replace the sample callbacks in `src/Bot.ts` with the application behavior. The delivery mode, lease length and attempt limit are set there too. `src/Worker.ts` and `src/DurableObject.ts` each build their half from that one `bot`.
