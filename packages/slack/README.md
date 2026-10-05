# Slack

`@humanlayer/channels-slack` turns Slack Events API webhooks into durable thread callbacks for `@humanlayer/channels-delivery`. It verifies request signatures, stores accepted events in provider-neutral mailboxes, and exposes thread, channel, message, reaction, file, streaming, and agent-status operations through typed resources and `SlackApi`.

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
| `files:read`                                                       | Downloading message files (`SlackFile.download`, `downloadBytes`).                                           |
| `files:write`                                                      | Uploading files to a channel or thread (`uploadFile`).                                                       |

Enable Slack's agent features (the manifest's `agent_view`) so agent session status and `agent_session_stopped` work. After changing scopes, reinstall the app.

## Event subscriptions

| Bot event                            | Used for                                                                  |
| ------------------------------------ | ------------------------------------------------------------------------- |
| `app_mention`                        | A mention starts a thread and invokes `onNewMention`.                     |
| `message.channels`, `message.groups` | Follow-up messages in subscribed threads.                                 |
| `message.im`, `message.mpim`         | Direct and group direct messages, which start a thread without a mention. |
| `reaction_added`, `reaction_removed` | Reactions in subscribed threads.                                          |
| `agent_session_stopped`              | The user stopped the agent session in a subscribed thread.                |

Edited and deleted messages arrive as `message` events and are delivered to subscribed threads. So do messages that share files (subtype `file_share`). Other events are acknowledged and ignored. The bot's own messages are never delivered to callbacks.

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
import { DebounceDeliveryMode } from '@humanlayer/channels-delivery'
import { SlackBot, SlackContent } from '@humanlayer/channels-slack'
import { Config, Effect } from 'effect'

const slack = SlackBot.make({
	signingSecret: Config.Redacted('SLACK_SIGNING_SECRET'),
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

## Files

Messages carry `files: ReadonlyArray<SlackFile>`, whether they come from a webhook or from `SlackApi` reads. Each `SlackFile` has a stable `ref` (`teamId` and `fileId`) plus a nullable `name`, `contentType`, `size`, and `downloadUrl`. Slack's `url_private_download` wins over `url_private`.

```ts
onSubscribedThreadEvents: (event) =>
	Effect.forEach(
		event.events.flatMap((threadEvent) =>
			Predicate.isTagged(threadEvent, 'SlackMessageReceived') ? threadEvent.message.files : [],
		),
		(file) => file.downloadBytes({ maxBytes: 10 * 1024 * 1024 }),
		{ concurrency: 4 },
	)
```

- `file.download()` streams the bytes without buffering. `file.downloadBytes({ maxBytes })` buffers them and fails with `SlackFileSizeLimitExceeded` from the declared size, `Content-Length`, or the running byte count, before it exceeds the bound.
- Downloads send `SLACK_BOT_TOKEN` only to `https://files.slack.com` (or `https://files.slack-gov.com`). Other URLs and redirects elsewhere fail without a request carrying the token.
- A `401`, a `403`, a redirect to the Slack sign-in page, or an HTML sign-in page instead of the file fails with `SlackFileAuthorizationError` naming `files:read`. It is not retryable, so a callback that lets it escape fails the mailbox batch without retrying.

`SlackChannel.uploadFile(input)` shares a file at the channel root; `SlackThread.uploadFile(input)` shares it as a thread reply. Both return the uploaded `SlackFile`.

```ts
const shareReport = event.thread.uploadFile({
	filename: 'report.csv',
	bytes: new TextEncoder().encode('name,count\nalpha,1\n'),
	title: 'Report',
	initialComment: 'Here is the report.',
})
```

Uploads use Slack's external upload flow: `files.getUploadURLExternal`, a raw-byte `POST` to the returned upload URL, then `files.completeUploadExternal` with `channel_id` and, for threads, `thread_ts`. The bot token goes only to the two Web API calls, never to the upload URL. Inputs hold at most 50 MB in memory. A `missing_scope` response fails with `SlackFileAuthorizationError` naming `files:write`. Other failures are `SlackApiError` values whose `operation` names the stage (`get_file_upload_url`, `upload_file_bytes`, or `complete_file_upload`). The sequence is not retried as a whole, because a failed completion may already have shared the file.

## Live API smoke

See [`scripts/README.md`](scripts/README.md).
