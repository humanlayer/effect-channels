# Slack thread echo

A minimal Effect-native Slack app using the same shape as a typical multi-provider chat SDK application:

1. call `createChannelsApp` with `slack()` and `ChannelsStorage.postgres()`;
2. provide Effect handlers using neutral `Thread` and `Message` values;
3. export `app.handle` for Fetch hosts or mount `app.routes` with Effect HTTP.

```ts
const app = createChannelsApp({
	providers: [slack()],
	storage: ChannelsStorage.postgres(),
	onNewMention: (thread, message) =>
		Effect.gen(function* () {
			yield* thread.subscribe()
			yield* thread.post(MarkdownContent.make({ markdown: `Echo: ${message.text}` }))
		}),
})

export const handle = app.handle
export const routes = app.routes
```

The high-level constructor owns provider registration, credentials, persistence, ingress, subscriptions, delivery workers, and the standard HTTP/client layers. Lower-level services remain available for custom composition, but ordinary application code does not wire them.

Both storage constructors return opaque high-level configuration owned by `createChannelsApp`; advanced Effect applications use the low-level exports from `@humanlayer/channels` and `@humanlayer/channels-postgres` directly. `ChannelsStorage.memory()` configures in-process coordination, subscriptions, and an Effect Cache for development. `ChannelsStorage.postgres()` reads `DATABASE_URL`, runs idempotent Channels migrations, and configures durable conversation coordination, subscription persistence, and an eight-day shared profile cache without requiring Redis.

Inbound and provider-history messages arrive with `message.author` hydrated through the provider user directory when available; failed profile lookups safely retain the provider ID-based author, while `Channels.getUser` remains available for explicit lookups.

This storage API intentionally stops at channel mechanics. A multi-tenant Connections facade for installation records and credential callbacks is the next separate design step; this example still uses the existing Config-backed Slack credentials.

The ordinary app is only [`src/app.ts`](./src/app.ts) and [`src/server.ts`](./src/server.ts). Credentialed acceptance and recording code lives under [`test/`](./test/) and is not part of the production example.

## Configure Slack

### 1. Start Postgres

Use an existing Postgres database, or start a disposable local one:

```bash
docker run --rm --name channels-postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=channels_test \
  -p 5432:5432 postgres:16
```

In another terminal:

```bash
export DATABASE_URL='postgres://postgres:postgres@localhost:5432/channels_test'
```

### 2. Create the Slack app

1. Create a [Slack Developer Program sandbox](https://api.slack.com/developer-program), if you do not already have a development workspace.
2. Open [Your Apps](https://api.slack.com/apps), choose **Create New App**, and choose **From an app manifest**.
3. Select the development workspace and paste [`slack-manifest.json`](./slack-manifest.json). The placeholder request URL can remain temporarily; it is replaced after the local server and tunnel are running.
4. Create the app.

### 3. Get the signing secret and bot token

1. In the app settings, open **Basic Information** → **App Credentials**. Reveal and copy **Signing Secret**. This becomes `SLACK_SIGNING_SECRET` and is used to verify that webhook requests really came from Slack.
2. Open **OAuth & Permissions** and click **Install to Workspace** (or **Reinstall to Workspace** after changing scopes).
3. Approve the installation. Back on **OAuth & Permissions**, copy the **Bot User OAuth Token** under **OAuth Tokens for Your Workspace**. It starts with `xoxb-` and becomes `SLACK_BOT_TOKEN`.

Do not use the client secret, verification token, app-level `xapp-` token, or a user `xoxp-` token in place of the bot token.

Use the bot token to obtain the workspace, bot-user, and bot-profile IDs:

```bash
export SLACK_BOT_TOKEN='xoxb-...'

curl -sS https://slack.com/api/auth.test \
  -H "Authorization: Bearer $SLACK_BOT_TOKEN" | jq
```

A bot-token response includes values like:

```json
{
	"ok": true,
	"team_id": "T0123456789",
	"user_id": "U0123456789",
	"bot_id": "B0123456789"
}
```

- `user_id` is the bot's Slack user/member identity. Set it as `SLACK_BOT_USER_ID`. Mentions use this value, for example `<@U0123456789>`.
- `bot_id` is the separate bot-profile identity that can appear as `bot_id` on bot-authored message events. Set it as `SLACK_BOT_ID`. It is optional but recommended so echo suppression recognizes both Slack forms.
- `team_id` is the Slack workspace ID. The SDK reads it from incoming events; it is also the tenant ID used when constructing channels for scheduled outbound work.

### 4. Prepare public and private test channels

Create or choose one public channel and one private channel. Invite the app to both—for example, type `/invite @Channels Thread Echo` in each channel.

You do not need to copy either channel ID. You will exercise the app manually in Slack, and the app receives the channel identity with each Slack event.

### 5. Export the complete configuration

```bash
export DATABASE_URL='postgres://postgres:postgres@localhost:5432/channels_test'
export SLACK_SIGNING_SECRET='...'
export SLACK_BOT_TOKEN='xoxb-...'
export SLACK_BOT_USER_ID='U...'
export SLACK_BOT_ID='B...' # optional, but recommended
export PORT=3000
```

The manifest includes mention and message history access across channel surfaces, channel/user lookup, message posting, Agent status, file read/write, and reaction read/write. It subscribes to `app_mention`, `message.channels`, `message.groups`, `reaction_added`, `reaction_removed`, and `agent_session_stopped`, and enables the current Agent view used by `agents.sessions.setStatus`. Slack does not add newly declared scopes to an existing installation automatically: after changing the manifest, reinstall the app and replace `SLACK_BOT_TOKEN` if Slack issues a new token.

### 6. Start the server and configure ngrok

Start the example:

```bash
bun --cwd examples/slack-thread-echo run src/server.ts
```

In another terminal, expose port 3000:

```bash
ngrok http 3000
```

Copy the HTTPS forwarding URL printed by ngrok, then append the Slack webhook path:

```text
https://YOUR-NGROK-HOST.ngrok.app/api/v1/integrations/slack/webhook
```

In the Slack app settings, open **Event Subscriptions**, enable events, and paste that full URL into **Request URL**. Slack sends a verification request immediately; wait for the green **Verified** result, then save the changes.

If the ngrok hostname changes after a restart, update the Request URL again. If you change scopes, reinstall the app so the bot token receives the new permissions.

Slack permission/authentication failures such as `missing_scope`, `invalid_auth`, and `not_in_channel` are logged once as non-retryable and the conversation mailbox continues in FIFO order; transient transport, rate-limit, and service failures retain bounded-backoff retry.

## Run the example

```bash
bun --cwd examples/slack-thread-echo run src/server.ts
```

A root mention is delivered to `onNewMention`, where the example explicitly subscribes before replying. Later unmentioned thread replies are delivered to `onSubscribedMessage`.

The example also demonstrates neutral file uploads. Include the standalone word **image** in a mention or subscribed thread reply (for example, `@Channels Thread Echo show me an image`) and the response attaches a generated SVG using `FileUpload.make(...)` on `MarkdownContent.files`. The image is generated in memory, so the example does not need a checked-in binary asset. The same content API supports file-only and multiple-file posts; see [`src/live-files.ts`](./src/live-files.ts) for those acceptance checks.

Phase 7 lifecycle APIs are active in the same handler. Include `reaction` to add `Emoji.Check` to your incoming message, `edit` to update the bot response through `SentMessage.edit(...)`, or `delete` to remove it through `SentMessage.delete()`. The app also registers `onMessageUpdated`, `onMessageDeleted`, and a typed `onReaction` filter for `Emoji.ThumbsUp`, `Emoji.Heart`, and `Emoji.Check`.

Include the standalone word `stream` (case-insensitive) to receive an Effect `Stream` response. Slack uses native streaming when available and transparently falls back to one post with throttled edits otherwise.

`src/server.ts` shows the Effect HTTP path: `HttpRouter.serve(routes)` plus the chosen server layer. The same `app.handle(request)` is a Fetch-compatible entry whose runtime and delivery worker are initialized once and owned internally.

## Optional real-Slack smoke check

```bash
bun run test:live:slack
```

While it runs:

1. Mention the bot in a new root message in the configured public channel, then reply in that thread without another mention.
2. Repeat in the configured private channel.
3. Have another bot or integration create a root message, then mention this bot inside that thread.

The live harness is a minimal manual smoke check for credentials and Slack app configuration. The required integration coverage is emulator-first and runs without Slack credentials under `packages/app`; see the [root testing notes](../../README.md#slack-provider-testing). Recordings are not required by the test workflow.

For the optional file-specific acceptance sequence, call `runLiveFileChecks(thread)` from
[`src/live-files.ts`](./src/live-files.ts) in a temporary handler. It verifies text-plus-file,
multiple-file, file-only, and authenticated download behavior without committing binary fixtures.

SIGINT and SIGTERM interrupt the worker and close the HTTP server, Postgres resources, and provider clients through the one Effect scope.
