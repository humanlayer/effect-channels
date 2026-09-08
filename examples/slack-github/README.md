# Combined Slack and GitHub host

One server mounts independent native providers; there is no provider registry or
universal conversation object. All handlers, composition, transport and test wiring
belong to this example; no source or test imports a sibling example. Both providers
reuse library primitives. GitHub has two independent Delivery registrations: respond on
GitHub and notify a configured Slack channel. One handler's successful admission
does not suppress the other.

Configure `GITHUB_APP_ID`, `GITHUB_PRIVATE_KEY` (multiline RSA PEM),
`GITHUB_INSTALLATION_ID`, `GITHUB_BOT_USER_ID`, `GITHUB_BOT_LOGIN` (e.g. `channels[bot]`),
`GITHUB_WEBHOOK_SECRET` and optional `GITHUB_API_URL` (defaults to the GitHub API), plus
`SLACK_TEAM_ID`, `SLACK_BOT_TOKEN`, `SLACK_BOT_USER_ID`, `SLACK_BOT_ID`,
`SLACK_SIGNING_SECRET`, and `SLACK_NOTIFICATION_CHANNEL_ID`. The Slack bot must be
in the notification channel and have `chat:write`; mentions/subscriptions use the
same Slack scopes and events as the existing Slack examples. `PORT` defaults to 3000. Provider secrets are independent; do not commit them.

The GitHub App needs Issues read/write and Pull requests read, with Issues,
Issue comment, Pull request, Pull request review, Pull request review comment and
Pull request review thread webhooks enabled. New `@channels` / `@channels[bot]`
mentions in issue/PR bodies, discussion comments, reviews or inline comments trigger
both registrations. The GitHub responder subscribes to the parent issue/PR; other
followed activity is silent (no reaction and no Slack notification). Body/comment
edits reply only when introducing a mention. Own-bot and unmentioned unfollowed
events are ignored. There is no App-bot assignment promise, human-account fallback
or native mention-autocomplete guarantee; see the
[provider evidence and exact contract](../../packages/github/README.md#app-bot-targeting-and-assignment-limits).

`GitHubBot.make` infers mixed handler Effects in the library; application authors
do not write `HandlerResult`/`ReturnType` unions. Slack notification configuration is
read directly with Config and existing Slack ID schemas in the handler, with no
NotificationTarget service or custom application schema/type layer.
GitHub subscriptions are supplied explicitly from `@humanlayer/channels-github/memory`.
Each activity callback uses shared serial delivery; mention precedence is local to
that registration and does not suppress the independent Slack notifier. An optional
`onCreation` callback can observe creation and choose to subscribe without a mention.

```sh
# From repository root, after bun install and securely configuring the environment:
bun run build
bun run --cwd examples/slack-github start
```

Routes:

- `POST /api/v1/integrations/slack/webhook`
- `POST /api/v1/integrations/github/webhook`

`src/fetch.ts` exports `makeHost()`: forward untouched requests to `handler` and
await `dispose()` on shutdown. Like the independent example, this is a long-running
memory-backed host with scoped workers, not a serverless/Alchemy/DO deployment.
No permanent polling worker is launched merely by mounting a provider route Layer.

The test runs the actual combined application: signed GitHub issue mention → GitHub comment
and proactive Slack notification; signed Slack mention → native threaded reply.
It inspects both provider emulators, verifies route isolation, and cleans up the
host. Run `./node_modules/.bin/vp test examples/slack-github/test` from the root.

The application owns correlation and notification policy. It does not mirror all
messages, run agents, or implement outbound PR management/review/workflow operations. Notifications contain
repository/issue identifiers, not untrusted markdown or issue bodies. Memory state
is volatile; external side effects are not exactly-once. No deployment or real
provider configuration is included. The provider README documents signed synthetic
review/thread coverage and specific emulator payload gaps; examples never invoke
the emulator's unsupported reaction endpoints.
