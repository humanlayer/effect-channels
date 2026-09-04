# Slack multi-tenant channels

This example owns a real Postgres repository for its product connection data. The `example_slack_connections` table is deliberately separate from Channels runtime tables; production applications can replace this repository with any database implementation.

```ts
const app = createChannelsApp({
	providers: [slack({ loadConnection: ({ workspaceId }) => loadSlackConnection({ workspaceId }) })],
	storage: ChannelsStorage.postgres({ pool: 'shared' }),
	handlers: {
		onNewMention: (thread, message) =>
			Effect.gen(function* () {
				yield* thread.subscribe()
				yield* thread.post(MarkdownContent.make({ markdown: `Workspace-aware echo: ${message.text}` }))
			}),
	},
})
```

Slack owns the callback input and output schemas. `loadConnection` receives `{ workspaceId: SlackTeamId }` and returns unknown data that is decoded into `{ organizationId, enabled, credentials }`; `createChannelsApp` privately adapts the callback into organization resolution, gating, credentials, and bot identity with one short shared cache. This example intentionally implements no OAuth routes or library-owned installation tables.

The callback preserves its Effect requirements, so `loadSlackConnection` yields the application `SqlClient`; `src/server.ts` supplies one scoped `DATABASE_URL` pool to both the example repository and Channels storage.

`ChannelsStorage.postgres()` is likewise opaque high-level configuration; advanced Effect applications can continue using the low-level Layers exported by `@humanlayer/channels-postgres` directly.

## One-workspace setup

Use the same Slack manifest and scopes as [`../slack-thread-echo`](../slack-thread-echo/README.md). One Slack app has one signing secret and one webhook URL for every workspace installation; the verified payload's `team_id` selects the connection and bot token.

```bash
export DATABASE_URL='postgres://localhost/channels_test'
export SLACK_SIGNING_SECRET='...'
export SLACK_TEAM_ID='T...'
export CHANNELS_ORGANIZATION_ID='local-org'
export SLACK_BOT_TOKEN='xoxb-...'
export SLACK_BOT_USER_ID='U...'
export SLACK_BOT_ID='B...'
export PORT=3000

bun run --cwd examples/slack-multi-tenant seed
bun run --cwd examples/slack-multi-tenant src/server.ts
```

The explicit `seed` command creates `example_slack_connections` if needed and upserts only complete environment records. Normal server startup runs the idempotent table migration but never overwrites connection rows. The token is unwrapped only as a SQL parameter and is never logged.

Expose `http://localhost:$PORT/api/v1/integrations/slack/webhook` through one HTTPS tunnel and configure that URL on the Slack app. Mentioning the bot exercises the real connection callback path even with one workspace.

Set `SLACK_CONNECTION_ENABLED=false` to demonstrate an installed but disabled connection. Unknown workspace deliveries are acknowledged and dropped because the connection lookup returns none; disabled workspace deliveries are acknowledged and rejected by the channel gate.

## Optional second workspace

True two-tenant testing requires installing the **same Slack app** in a second workspace. The signing secret and webhook URL remain unchanged; configure only the second installation record:

```bash
export SLACK_TEAM_ID_2='T...'
export CHANNELS_ORGANIZATION_ID_2='second-org'
export SLACK_BOT_TOKEN_2='xoxb-...'
export SLACK_BOT_USER_ID_2='U...'
export SLACK_BOT_ID_2='B...'
export SLACK_CONNECTION_ENABLED_2=true
```

Run the seed command again after supplying the optional `_2` values. Tokens become `Redacted` immediately after database decoding and are never included in logs.

## Tests

```bash
bun --cwd examples/slack-multi-tenant test
```

With `DATABASE_URL` set, the tests use the real example-owned table and prove separate organization/token resolution for two workspaces, disabled and unknown connection behavior, and that outbound history/post calls select credentials from the workspace encoded in the canonical `ThreadId` rather than a global token. They skip when Postgres is unavailable by configuration.
