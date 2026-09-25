# Slack Next

`@humanlayer/channels-slack-next` turns Slack Events API webhooks into durable thread callbacks for `@humanlayer/channels-delivery-next`. It verifies request signatures, stores accepted events in provider-neutral mailboxes, and exposes thread, channel, message, reaction, streaming, and agent-status operations through typed resources and `SlackApi`.

## Create a Slack app

Create an app at [api.slack.com/apps](https://api.slack.com/apps) **From a manifest** and paste [`examples/alchemy-cloudflare/slack-app-manifest.example.json`](../../examples/alchemy-cloudflare/slack-app-manifest.example.json). Replace the app name and set the event request URL to your public webhook route, such as `https://bot.example.com/integrations/slack/webhook`. Then install the app to your workspace and invite the bot to each channel it should read.

The package does not use interactivity, slash commands, OAuth redirects, or Socket Mode.

## Bot token scopes

| Scope                                                              | Used for                                                                                                     |
| ------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------ |
| `app_mentions:read`                                                | `app_mention` events that start a thread.                                                                    |
| `channels:history`, `groups:history`, `im:history`, `mpim:history` | Message events in public channels, private channels, DMs, and group DMs; reading thread and channel history. |
| `channels:read`, `groups:read`, `im:read`, `mpim:read`             | Looking up conversation details.                                                                             |
| `chat:write`                                                       | Posting and streaming replies.                                                                               |
| `chat:write.public`                                                | Posting in public channels the bot has not joined. Optional.                                                 |
| `assistant:write`                                                  | Setting agent session status (`agents.sessions.setStatus`) for typing and progress.                          |
| `reactions:read`, `reactions:write`                                | Receiving reaction events; adding and removing reactions.                                                    |
| `users:read`                                                       | Resolving message authors and the bot's own identity (`users.info`, `bots.info`).                            |
| `files:read`, `files:write`                                        | Downloading and uploading files. Needed once file support lands; harmless before.                            |

Enable Slack's agent features (the manifest's `agent_view`) so agent session status and `agent_session_stopped` work. After changing scopes, reinstall the app.

## Event subscriptions

| Bot event                            | Used for                                                                  |
| ------------------------------------ | ------------------------------------------------------------------------- |
| `app_mention`                        | A mention starts a thread and invokes `onNewMention`.                     |
| `message.channels`, `message.groups` | Follow-up messages in subscribed threads.                                 |
| `message.im`, `message.mpim`         | Direct and group direct messages, which start a thread without a mention. |
| `reaction_added`, `reaction_removed` | Reactions in subscribed threads.                                          |
| `agent_session_stopped`              | The user stopped the agent session in a subscribed thread.                |

Edited and deleted messages arrive as `message` events and are delivered to subscribed threads. Other events are acknowledged and ignored. The bot's own messages are never delivered to callbacks.

## Credentials

```dotenv
SLACK_SIGNING_SECRET=...
SLACK_BOT_TOKEN=xoxb-...
```

- `SLACK_SIGNING_SECRET` (**Basic Information → App Credentials**) verifies webhooks. Pass it to `SlackBot.make` as `signingSecret`.
- `SLACK_BOT_TOKEN` (**OAuth & Permissions → Bot User OAuth Token**) authenticates every API call. `SlackApiLive` reads it.
- `SLACK_BOT_USER_ID` is optional. When unset, `SlackApiLive` resolves the bot's user ID with `auth.test`.

Never commit the signing secret or bot token.

## Configure a provider

```ts
import { DebounceDeliveryMode } from '@humanlayer/channels-delivery-next'
import { SlackBot, SlackContent } from '@humanlayer/channels-slack-next'
import { Config, Effect } from 'effect'

const slack = SlackBot.make({
	signingSecret: Config.redacted('SLACK_SIGNING_SECRET'),
	deliveryMode: DebounceDeliveryMode.make({ quietPeriodMs: 2_000, maxWaitMs: 10_000 }),
	handlers: {
		onNewMention: (event) =>
			Effect.gen(function* () {
				yield* event.thread.subscribe()
				yield* event.thread.post(SlackContent.make({ markdown: 'On it.' }))
			}),
		onSubscribedThreadEvents: (event) => Effect.logInfo('Subscribed thread activity', event.events.length),
	},
})
```

Add `slack` to the providers passed to `Channels.make` or a host-specific wrapper such as `ChannelsCloudflare.make`. See `examples/alchemy-cloudflare` for a complete Worker and Durable Object setup.

## Live API smoke

See [`scripts/README.md`](scripts/README.md).
