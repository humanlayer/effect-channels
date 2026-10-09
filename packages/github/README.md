# GitHub

`@humanlayer/channels-github` turns GitHub App webhooks into durable issue and pull-request callbacks. It verifies webhook signatures, stores accepted events in provider-neutral mailboxes, normalizes GitHub payloads, and exposes issue, pull-request, comment, review, label, check-run, GitHub Actions job, and merge operations through typed resource classes and `GitHubApi`.

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

[`examples/alchemy-cloudflare/github-app-manifest.example.json`](../../examples/alchemy-cloudflare/github-app-manifest.example.json) records every permission and event below as a [GitHub App manifest](https://docs.github.com/en/apps/sharing-github-apps/registering-a-github-app-from-a-manifest).

## Repository permissions

GitHub Apps use repository permissions rather than OAuth scopes. Configure these permissions:

| Permission    | Access         | Required for                                                                                                      |
| ------------- | -------------- | ----------------------------------------------------------------------------------------------------------------- |
| Metadata      | Read-only      | Repository identity. GitHub grants this mandatory permission to installed apps.                                   |
| Issues        | Read and write | Reading and changing issues, issue comments, and labels shared by issues and PRs.                                 |
| Pull requests | Read and write | Reading and changing PRs, files, commits, conversation and review comments, reviews, and labels.                  |
| Checks        | Read-only      | Receiving completed check-run events; listing check runs; reading check output and annotations.                   |
| Contents      | Read and write | Merging pull requests. GitHub's merge endpoint specifically requires write access.                                |
| Actions       | Read-only      | Resolving a GitHub Actions-backed check to its workflow job, reading job details, and downloading that job's log. |

No Administration, organization, or account permissions are required.

If you change permissions after installing the app, approve the new permission request for the installation or reinstall the app before testing again.

Applications that only consume callback data and call read methods can reduce Issues and Pull requests to read-only. Comment and reaction mutations, labels, issue state changes, PR state changes, and review-comment creation require the corresponding write permission. Omit Contents if the application cannot merge and Actions if it cannot inspect GitHub Actions jobs or logs.

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

GitHub App bot identities are not native mentionable accounts. For example, GitHub does not autocomplete or link `@acme[bot]`; the `[bot]` login is still needed to resolve the numeric bot user ID and suppress self-authored events. Mention activation in this package is text matching, so configure a human-friendly invocation name such as the app slug and ask users to type `@acme`. The text may remain unlinked. A user can write `[@acme](https://github.com/apps/acme)` when a clickable link is important, and the provider will still recognize it. If the slug matches a real user or organization, choose a distinct invocation name to avoid notifying that account.

Set these environment variables:

```dotenv
GITHUB_WEBHOOK_SECRET=the-secret-entered-in-the-app-settings
GITHUB_APP_ID=123456
GITHUB_PRIVATE_KEY="-----BEGIN RSA PRIVATE KEY-----
paste-the-complete-downloaded-key-here
-----END RSA PRIVATE KEY-----"
GITHUB_BOT_MENTION_NAME=your-app-slug
GITHUB_BOT_USER_ID=123456789
```

- `GITHUB_WEBHOOK_SECRET` verifies incoming webhooks.
- `GITHUB_APP_ID` and `GITHUB_PRIVATE_KEY` create short-lived installation tokens for API calls.
- `GITHUB_BOT_MENTION_NAME` controls textual invocation and does not include a leading `@`; it is not the bot's `[bot]` login.
- `GITHUB_BOT_USER_ID` suppresses self-authored events. `GitHubApiLive` can resolve this identity for reaction removal, but event processing still requires it explicitly.

Never commit the webhook secret or private key.

## Configure a provider

```ts
import { DebounceDeliveryMode } from '@humanlayer/channels-delivery'
import { GitHubBot, GitHubId } from '@humanlayer/channels-github'
import { Config, Effect } from 'effect'

const github = GitHubBot.make({
	webhookSecret: Config.Redacted('GITHUB_WEBHOOK_SECRET'),
	deliveryMode: DebounceDeliveryMode.make({
		quietPeriodMs: 2_000,
		maxWaitMs: 10_000,
	}),
	bot: Config.all({
		mentionNames: Config.String('GITHUB_BOT_MENTION_NAME').pipe(Config.map((name) => [name])),
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

## Resource API

Callbacks provide `GitHubIssue` and `GitHubPullRequest` resources. Their methods require `GitHubApi`, which `GitHubBot.make` provides with `GitHubApiLive` by default. List methods follow GitHub pagination automatically with 100 items per request.

### Issues

In addition to `fetchInfo()`, comment operations, and subscription operations, an issue supports:

| Resource method          | `GitHubApi` operation  | Result and behavior                                                                                     |
| ------------------------ | ---------------------- | ------------------------------------------------------------------------------------------------------- |
| `close(reason)`          | `closeIssue`           | Closes with reason `"completed"` or `"not_planned"`; returns updated issue info.                        |
| `reopen()`               | `reopenIssue`          | Reopens the issue and returns updated issue info.                                                       |
| `listLabels()`           | `listIssueLabels`      | Returns complete `GitHubLabel` records.                                                                 |
| `addLabels(labels)`      | `addIssueLabels`       | Adds labels without removing existing labels; returns the resulting set.                                |
| `setLabels(labels)`      | `setIssueLabels`       | Replaces the complete label set; returns the resulting set.                                             |
| `removeLabel(label)`     | `removeIssueLabel`     | Removes one label by name; returns the resulting set.                                                   |
| `removeAllLabels()`      | `removeAllIssueLabels` | Removes every label.                                                                                    |
| `fetchUserAccess(login)` | `fetchUserAccess`      | Returns the user's access to the repository: `none`, `read`, `triage`, `write`, `maintain`, or `admin`. |

```ts
const updateIssue = Effect.gen(function* () {
	yield* issue.addLabels(['triage', 'agent-reviewed'])
	yield* issue.close('completed')
})
```

Label inputs are arrays of non-empty names. Duplicate is not a supported close reason. GitHub's duplicate-closing API requires the canonical issue's internal database ID and a newer API contract; this package pins GitHub API version `2022-11-28`.

`fetchUserAccess(login)` calls `GET /repos/{owner}/{repo}/collaborators/{username}/permission`, which works with the permissions listed above (checked live: `admin` for an owner, `read` for an outsider on a public repository). It returns the built-in role (`role_name`); for a custom role it falls back to GitHub's legacy `permission`, which counts `maintain` as `write` and `triage` as `read`. Compare levels with `hasGitHubAccess({ access, minimum })` or `GitHubAccessLevelOrder`. A login that is not a GitHub user fails with reason `not_found`. The package does not check authors itself; an application decides whom to act for, as the Cloudflare example does.

### Pull requests

In addition to `fetchInfo()`, conversation comments, reviews, review-comment listing, and subscription operations, a pull request supports:

| Resource method            | `GitHubApi` operation                      | Result and behavior                                                                               |
| -------------------------- | ------------------------------------------ | ------------------------------------------------------------------------------------------------- |
| `listFiles()`              | `listPullRequestFiles`                     | Returns structured added, deleted, modified, renamed, copied, changed, or unchanged file records. |
| `fetchDiff()`              | `fetchPullRequestDiff`                     | Returns the unified diff as text.                                                                 |
| `listCommits()`            | `listPullRequestCommits`                   | Returns the commits belonging to the PR.                                                          |
| `postReviewComment(input)` | `postPullRequestReviewComment`             | Creates one line, range, or whole-file review comment without managing a pending review.          |
| `listLabels()`             | `listPullRequestLabels`                    | Returns complete `GitHubLabel` records through GitHub's Issues API.                               |
| `addLabels(labels)`        | `addPullRequestLabels`                     | Adds labels without removing existing labels; returns the resulting set.                          |
| `setLabels(labels)`        | `setPullRequestLabels`                     | Replaces the complete label set; returns the resulting set.                                       |
| `removeLabel(label)`       | `removePullRequestLabel`                   | Removes one label by name; returns the resulting set.                                             |
| `removeAllLabels()`        | `removeAllPullRequestLabels`               | Removes every label.                                                                              |
| `listCheckRuns()`          | `fetchPullRequest` + `listCheckRunsForRef` | Reads the current head SHA and returns checks for that commit.                                    |
| `listCheckRunsForRef(sha)` | `listCheckRunsForRef`                      | Returns checks for an explicit commit SHA.                                                        |
| `close()`                  | `closePullRequest`                         | Closes the PR and returns updated PR info.                                                        |
| `reopen()`                 | `reopenPullRequest`                        | Reopens the PR and returns updated PR info.                                                       |
| `merge(options)`           | `mergePullRequest`                         | Attempts `"merge"`, `"squash"`, or `"rebase"`; returns `{ merged, sha, message }`.                |
| `fetchUserAccess(login)`   | `fetchUserAccess`                          | Returns the user's access to the repository, as for issues.                                       |

`fetchInfo()` includes `headRepository`, the repository the head branch lives in: a fork's for a pull request from a fork, and `null` once that repository is deleted.

Two `GitHubApi` operations work on a repository rather than one pull request: `listPullRequestsForBranch({ repository, head })` returns the open pull requests from a branch of that repository (not from forks), and `createPullRequest({ repository, head, base, title, body })` opens one. Creating one needs the App's pull requests write permission.

Prefer `listFiles()` when an agent only needs selected changes. GitHub's wire status `removed` is exposed as `deleted`; `copied`, `changed`, and `unchanged` remain distinct statuses rather than being collapsed into `modified`. A file's `sha`, `blobUrl`, and `rawUrl` can be `null`, including for some submodule entries. GitHub limits that endpoint to 3,000 files, may omit `patch` for binary or unusually large files, and limits `listCommits()` to 250 PR commits. `fetchDiff()` loads the complete unified diff into one string.

Review-comment locations are tagged values. `Line` and `Range` line numbers must be positive integers that refer to the pull-request diff; `LEFT` means the old side and `RIGHT` the new side. Use `File` for a whole-file comment. The commit SHA and path must be non-empty.

```ts
const reviewPullRequest = Effect.gen(function* () {
	const info = yield* pullRequest.fetchInfo()
	yield* pullRequest.postReviewComment({
		content: { markdown: 'This can fail when the input is empty.' },
		commitId: info.headSha,
		path: 'src/example.ts',
		location: { _tag: 'Line', line: 42, side: 'RIGHT' },
	})
})
```

A PR has no close-reason field. Post a conversation comment or add a label before `close()` when the reason must be visible.

Merging requires an explicit method and the head SHA the agent actually reviewed. This prevents a later push from being merged accidentally:

```ts
const mergePullRequest = Effect.gen(function* () {
	const info = yield* pullRequest.fetchInfo()
	return yield* pullRequest.merge({
		method: 'squash',
		expectedHeadSha: info.headSha,
		commitTitle: 'Fix the login race',
		commitMessage: 'Prevent overlapping refresh requests.',
	})
})
```

GitHub can reject a merge for a stale head, merge conflict, disabled merge method, required checks or reviews, unresolved conversations, branch protection, repository rules, or a merge queue. Inspect the returned result's `merged` field; rejected HTTP responses fail with `GitHubApiError` as described below.

### Checks and GitHub Actions jobs

`listCheckRuns()` returns lightweight `GitHubCheckRun` resources. Call their methods only when details are needed:

| Resource method                      | `GitHubApi` operation     | Result and behavior                                                                               |
| ------------------------------------ | ------------------------- | ------------------------------------------------------------------------------------------------- |
| `GitHubCheckRun.fetchInfo()`         | `fetchCheckRun`           | Returns status, conclusion, timestamps, output, URLs, suite ID, head SHA, and annotation count.   |
| `GitHubCheckRun.listAnnotations()`   | `listCheckRunAnnotations` | Returns paginated file, line/column, level, message, raw-detail, and blob-URL records.            |
| `GitHubCheckRun.resolveActionsJob()` | `resolveActionsJob`       | Resolves the matching GitHub Actions job, or `null` when no matching Actions job exists.          |
| `GitHubActionsJob.fetchInfo()`       | `fetchActionsJob`         | Returns run ID, status, conclusion, head SHA, workflow metadata, timestamps, URLs, and job steps. |
| `GitHubActionsJob.downloadLog()`     | `downloadActionsJobLog`   | Follows GitHub's short-lived redirect and returns the job log as text.                            |

`detailsUrl` is an opaque provider URL, not a log endpoint. Checks from CircleCI, Buildkite, or another third-party provider can expose output and annotations, but `resolveActionsJob()` returns `null` unless a GitHub Actions job can be matched. Third-party logs require that provider's API and credentials. `GitHubApiLive` follows at most three job-log redirects and does not forward the GitHub authorization header to the signed log host.

`listCheckRuns()` queries the PR head at call time. In a completed-check callback, use the event's `headSha` and `checkRunId` so a newer push cannot change which check you inspect:

```ts
import { Effect } from 'effect'

const inspectCompletedCheck = Effect.gen(function* () {
	const checks = yield* event.pullRequest.listCheckRunsForRef(event.headSha)
	const check = checks.find((candidate) => candidate.ref.id === event.checkRunId)
	if (check === undefined) return

	const info = yield* check.fetchInfo()
	const annotations = yield* check.listAnnotations()
	const job = yield* check.resolveActionsJob()

	if (job !== null) {
		const log = yield* job.downloadLog()
	}
})
```

Listing check runs requests `filter=all`, so rerun attempts are not hidden by GitHub's default latest-only filter. It shows observed statuses, but it does not identify which checks are required by branch protection or repository rules.

### API errors

Every API operation fails with `GitHubApiError`. It includes `operation`, `reason`, and `retryable`, plus `status`, a safe GitHub `message`, and `retryAfterMs` when available. Reasons are `authentication`, `forbidden`, `not_found`, `rate_limited`, `validation`, `stale_head`, `not_mergeable`, `rules_rejected`, `unavailable`, and `invalid_response`. For merge requests, `GitHubApiLive` classifies stable statuses only: `409` is `stale_head`, `405` is `not_mergeable`, non-rate-limited `403` is `forbidden`, and `422` is `validation`. Provider messages are preserved for explanation but are not parsed to infer a reason; `rules_rejected` is reserved for implementations with a stable machine-readable rule signal. `GitHubApiLive` retries once with a fresh installation token after an authentication failure; callers can use `retryable` and `retryAfterMs` for any further retry policy.

## Remote delivery

Every callback receives a `DeliveryContext` and may hand its delivery to a remote worker. The remote worker then drives the output through the delivery API; `GitHubBot` sends it to the issue or pull request.

| Operation                              | Issue or pull request                                                                                                                                         |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `complete` / `fail` (`PresentOutcome`) | a comment when there is Markdown (with `awaitingInput` options listed under it), otherwise nothing; removes `eyes` first when the last activity was `Working` |
| `activity.set` `Working`               | the bot's `eyes` reaction on what started the delivery; the text is not shown                                                                                 |
| `activity.set` `Idle`                  | removes that `eyes` reaction                                                                                                                                  |
| `messages.create`                      | comment                                                                                                                                                       |
| `messages.update` / `delete`           | edit or delete the comment                                                                                                                                    |
| `links.add`                            | nothing                                                                                                                                                       |
| `reactions.set`                        | the bot's reaction on what started the delivery, or on a comment the delivery posted: `thumbs_up` is `+1`, `thumbs_down` is `-1`; the rest keep their names   |
| `plan.put`                             | one plan comment: the first plan comments it, each later plan edits it, and an unchanged plan makes no call; a deleted plan comment is commented again        |

What started the delivery (its activation target) is the mentioning comment or inline review comment, or the issue or pull request itself when it was opened or mentioned the bot in its body. A subscribed batch has none, so its delivery does not list `SetActivity`, and `activity.set` answers 409. `GET /deliveries/<id>` lists what the delivery supports in `supportedOperations`.

Portable reactions can go on what started the delivery, when there is one, and on the delivery's own comments; `GET /deliveries/<id>` lists them in `reactionTargets`. The activity's `eyes` and a portable `eyes` on the activation target are the same GitHub reaction, so `Idle` removes both.

The plan comment shows the plan's title, then one line per item with a mark for its state (⬜ pending, 🔄 in progress, ✅ completed, ❌ failed) and its note. The delivery keeps the plan; `GET /deliveries/<id>` reports it with the revision GitHub last showed.

Reactions converge: adding `eyes` that is already there, or removing it when it is already gone, counts as done. `GitHubApi.addReaction` and `removeReaction` take a `GitHubReactionTarget`: a comment (`Comment`), or an issue or pull request itself (`Discussion`). Comments stay at-least-once: if GitHub accepts a comment but the attempt dies before it is saved, the next attempt comments again. The delivery API never exposes installation, repository, or comment IDs.

## Test and troubleshoot

1. Start or deploy the application at a public HTTPS URL.
2. Put its exact webhook URL in the GitHub App settings.
3. Install the app on the test repository.
4. Open an issue or pull request, or write `@<app-slug>` in a supported body or comment.
5. Open the GitHub App settings and use **Advanced → Recent Deliveries** to inspect the request, response, and payload or request a redelivery.

A successful admission returns HTTP 200. For common failures:

- **401 from the webhook route:** the application and GitHub App webhook secrets differ.
- **404 from the webhook route:** the URL path is wrong or the GitHub provider is not mounted.
- **403 from a callback API operation:** the app lacks a required permission or the installation cannot access that repository.
- **No delivery in GitHub:** the event is not selected, the app is not installed on the repository, or webhooks are inactive.
- **Delivery succeeds but no callback runs:** the event action may be unsupported, the issue/PR is not subscribed and did not trigger a configured creation or mention callback, or it is subscribed and has no subscribed-events callback. A comment that mentions the bot always runs `onMentioned` when it is configured, even in a subscribed issue/PR.

See `examples/alchemy-cloudflare` for a complete Worker and Durable Object configuration with both GitHub and Slack.
