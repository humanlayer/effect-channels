# GitHub Next

`@humanlayer/channels-github-next` turns GitHub App webhooks into durable issue and pull-request callbacks. It verifies webhook signatures, stores accepted events in provider-neutral mailboxes, normalizes GitHub payloads, and exposes issue, pull-request, comment, review, and reaction operations through `GitHubApi`.

## Create a GitHub App

Open **GitHub Settings → Developer settings → GitHub Apps → New GitHub App**. If an organization should own the app, create it from that organization's settings.

Configure:

- **GitHub App name:** a globally unique name. GitHub permits the app to use the name of the user or organization that owns it.
- **Homepage URL:** your application or project URL.
- **Webhook:** active.
- **Webhook URL:** your public Channels webhook route, such as `https://bot.example.com/integrations/github/webhook`.
- **Webhook secret:** a strong random value, such as the output of `openssl rand -hex 32`.
- **Where can this GitHub App be installed?:** use **Only on this account** while testing.

OAuth callbacks, user authorization, device flow, and setup URLs are not required.

## Repository permissions

GitHub Apps use repository permissions rather than OAuth scopes. Configure these permissions:

| Permission    | Access         | Required for                                                                                    |
| ------------- | -------------- | ----------------------------------------------------------------------------------------------- |
| Metadata      | Read-only      | Repository identity. GitHub grants this mandatory permission to installed apps.                 |
| Issues        | Read and write | Reading issues and issue comments; creating, editing, deleting, and reacting to issue comments. |
| Pull requests | Read and write | Reading PRs, reviews, and review comments; creating, editing, deleting, replying, and reacting. |
| Checks        | Read-only      | Receiving completed check-run events associated with pull requests.                             |

No Contents, Actions, Administration, organization, or account permissions are required by the current implementation.

If you change permissions after installing the app, approve the new permission request for the installation or reinstall the app before testing again.

Applications that only consume callback data and never call write methods can reduce Issues and Pull requests to read-only. `postComment`, `reply`, `update`, `delete`, `addReaction`, and `removeReaction` require the corresponding write permission.

## Webhook subscriptions

Subscribe the GitHub App to these events:

| GitHub setting              | Webhook header value          | Processed actions                                                                                                                                                                                 |
| --------------------------- | ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Issues                      | `issues`                      | `opened`, `edited`, `closed`, `reopened`, `assigned`, `unassigned`, `labeled`, `unlabeled`                                                                                                        |
| Issue comment               | `issue_comment`               | `created`, `edited`, `deleted`                                                                                                                                                                    |
| Pull request                | `pull_request`                | `opened`, `edited`, `closed`, `reopened`, `synchronize`, `review_requested`, `review_request_removed`, `assigned`, `unassigned`, `labeled`, `unlabeled`, `converted_to_draft`, `ready_for_review` |
| Pull request review         | `pull_request_review`         | `submitted`, `edited`, `dismissed`                                                                                                                                                                |
| Pull request review comment | `pull_request_review_comment` | `created`, `edited`, `deleted`                                                                                                                                                                    |
| Pull request review thread  | `pull_request_review_thread`  | `resolved`, `unresolved`                                                                                                                                                                          |
| Check run                   | `check_run`                   | `completed`                                                                                                                                                                                       |

Other events and actions are acknowledged and ignored. A completed check run is delivered to every pull request listed in its `pull_requests` association. Check runs without an associated pull request are ignored. Do not subscribe to `check_suite`, `push`, or `workflow_run` unless another part of your application needs them.

## Credentials and bot identity

After creating the app:

1. Note its numeric **App ID**.
2. Generate and download a private key from **Private keys**.
3. Install the app on an account or organization and grant it access to the repositories you want to process.
4. Note the app slug. Every GitHub App has a built-in bot with the login `<app-slug>[bot]`.
5. Resolve that bot's numeric user ID:

```bash
curl --fail --silent \
  "https://api.github.com/users/<app-slug>%5Bbot%5D" |
  jq .id
```

An app owned by an organization can use the same base name. For example, organization `acme` and app `acme` are separate from the app's bot, `acme[bot]`. Configure the complete bot login as the mention name so `@acme` does not accidentally activate the bot.

Set these environment variables:

```dotenv
GITHUB_WEBHOOK_SECRET=the-secret-entered-in-the-app-settings
GITHUB_APP_ID=123456
GITHUB_PRIVATE_KEY="-----BEGIN RSA PRIVATE KEY-----
paste-the-complete-downloaded-key-here
-----END RSA PRIVATE KEY-----"
GITHUB_BOT_MENTION_NAME=your-app-slug[bot]
GITHUB_BOT_USER_ID=123456789
```

- `GITHUB_WEBHOOK_SECRET` verifies incoming webhooks.
- `GITHUB_APP_ID` and `GITHUB_PRIVATE_KEY` create short-lived installation tokens for API calls.
- `GITHUB_BOT_MENTION_NAME` controls textual mention activation and does not include a leading `@`.
- `GITHUB_BOT_USER_ID` suppresses self-authored events. `GitHubApiLive` can resolve this identity for reaction removal, but event processing still requires it explicitly.

Never commit the webhook secret or private key.

## Configure a provider

```ts
import { DebounceDeliveryMode } from '@humanlayer/channels-delivery-next'
import { GitHubBot, GitHubId } from '@humanlayer/channels-github-next'
import { Config, Effect } from 'effect'

const github = GitHubBot.make({
	webhookSecret: Config.redacted('GITHUB_WEBHOOK_SECRET'),
	deliveryMode: DebounceDeliveryMode.make({
		quietPeriodMs: 2_000,
		maxWaitMs: 10_000,
	}),
	bot: Config.all({
		mentionNames: Config.string('GITHUB_BOT_MENTION_NAME').pipe(Config.map((name) => [name])),
		botUserId: Config.schema(GitHubId, 'GITHUB_BOT_USER_ID'),
	}),
	handlers: {
		onIssueCreated: (event) =>
			Effect.gen(function* () {
				yield* event.issue.subscribe()
			}),
		onPrCreated: (event) =>
			Effect.gen(function* () {
				yield* event.pullRequest.subscribe()
			}),
		onMentioned: (event) => Effect.logInfo('GitHub bot mentioned', event),
		onSubscribedIssueEvents: (event) => Effect.logInfo('Subscribed issue activity', event),
		onSubscribedPrEvents: (event) => Effect.logInfo('Subscribed pull request activity', event),
	},
})
```

Add `github` to the providers passed to `Channels.make` or the host-specific wrapper such as `ChannelsCloudflare.make`.

## Test and troubleshoot

1. Start or deploy the application at a public HTTPS URL.
2. Put its exact webhook URL in the GitHub App settings.
3. Install the app on the test repository.
4. Open an issue or pull request, or write `@<app-slug>[bot]` in a supported body or comment.
5. Open the GitHub App settings and use **Advanced → Recent Deliveries** to inspect the request, response, and payload or request a redelivery.

A successful admission returns HTTP 200. For common failures:

- **401 from the webhook route:** the application and GitHub App webhook secrets differ.
- **404 from the webhook route:** the URL path is wrong or the GitHub provider is not mounted.
- **403 from a callback API operation:** the app lacks a required permission or the installation cannot access that repository.
- **No delivery in GitHub:** the event is not selected, the app is not installed on the repository, or webhooks are inactive.
- **Delivery succeeds but no callback runs:** the event action may be unsupported, or the issue/PR is not subscribed and did not trigger a configured creation or mention callback.

See `examples/alchemy-cloudflare` for a complete Worker and Durable Object configuration with both GitHub and Slack.
