# Alchemy Cloudflare Slack mailbox example

This example receives Slack webhooks in a Cloudflare Worker, stores each event in a mailbox Durable Object, and processes the mailbox from the object's alarm. The placeholder callbacks log normalized Slack events.

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
- The alarm claims ordered batches and invokes the Slack processor.
- New-mention and subscribed-thread callbacks log normalized event metadata.
- Slack thread subscriptions are stored in Durable Object storage.
- Reaction events resolve the reacted message's thread root through `conversations.history`.

The placeholder callbacks do not post replies. Replace them in `src/providerDispatcher.ts` with the application behavior.
