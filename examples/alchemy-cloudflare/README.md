# Alchemy Cloudflare Slack, GitHub, and Linear mailbox example

This example receives Slack, GitHub, and Linear webhooks in a Cloudflare Worker, stores each event in a mailbox Durable Object, and processes mailboxes from Durable Object alarms. All providers are declared in `src/Bot.ts`; `src/Worker.ts` builds webhook ingress and `src/DurableObject.ts` builds persistent processing from that same declaration.

## Linear Application setup

Create a Linear Application for the workspace with the scopes and webhook categories listed in [`packages/linear-next/README.md`](../../packages/linear-next/README.md), and set its webhook URL to:

```text
https://<your-worker-hostname>/integrations/linear/webhook
```

Use the app actor with either an application developer token:

```dotenv
LINEAR_WEBHOOK_SECRET=...
LINEAR_DEVELOPER_TOKEN=...
LINEAR_ORGANIZATION_ID=...
LINEAR_APP_USER_ID=...
```

or OAuth client credentials:

```dotenv
LINEAR_WEBHOOK_SECRET=...
LINEAR_CLIENT_ID=...
LINEAR_CLIENT_SECRET=...
LINEAR_ORGANIZATION_ID=...
LINEAR_APP_USER_ID=...
```

When both authentication forms are configured, `LINEAR_DEVELOPER_TOKEN` takes precedence. The configured organization and app-user IDs are explicit identity expectations, and both authentication paths verify them with a lazy `viewer` query before the first provider operation. Client credentials acquire and renew short-lived tokens automatically; a developer token remains caller-managed. No credential is placed in webhook admissions or Durable Object mailbox state.

`AgentSessionEvent.created` and `AgentSessionEvent.prompted` are the authoritative agent entry points. Created sessions receive an automatic ephemeral thought before application code runs; both example callbacks then emit a terminal response activity so the Linear card completes. The corresponding Inbox Notification mention and assignment events are still authenticated and decoded, but are acknowledged as supplemental signals instead of starting duplicate work.

The automatic thought, and every activity a handed-off session turn posts through the delivery API, carries its own UUID. Linear refuses a second activity with an ID it has seen, and the package counts that refusal as done, so a retry never posts one twice. Activities a callback posts itself (`session.thought`, `session.respond`) let Linear choose the ID and stay at-least-once: a retry after one succeeded can post it again.

For a live check, use a unique `channels-live-p2-<timestamp>` marker: mention the app on one issue and delegate a second issue to it. Each action should create one session-scoped mailbox, show an ephemeral thought within ten seconds, invoke `onAgentSessionCreated` once, and finish with the example's terminal response. Send a follow-up message in the session and confirm `onAgentSessionPrompted` uses the same mailbox and produces one response. Corresponding Inbox Notification deliveries must not invoke the legacy mention or assignment callbacks. Logs include only organization, session, issue, delivery, and prompt activity IDs—never prompts, guidance, credentials, signatures, or payload bodies.

## GitHub App setup

GitHub Apps have an equivalent to Slack manifests, called the [GitHub App Manifest flow](https://docs.github.com/en/apps/sharing-github-apps/registering-a-github-app-from-a-manifest). Unlike Slack's pasteable manifest, GitHub's manifest is submitted by a web page and completed through a callback that exchanges a temporary code for the App ID, private key, and webhook secret. This example does not host that registration callback. `github-app-manifest.example.json` records the exact manifest settings, but the quickest way to test one repository is to create the app manually.

### 1. Start or deploy the Worker

```bash
cd examples/alchemy-cloudflare
cp .env.example .env
bun alchemy dev
```

You need a public HTTPS Worker URL before GitHub can deliver webhooks. The GitHub webhook URL is:

```text
https://<your-worker-hostname>/integrations/github/webhook
```

### 2. Register the GitHub App

Open **GitHub Settings → Developer settings → GitHub Apps → New GitHub App**, then configure:

- **GitHub App name:** any globally unique name.
- **Homepage URL:** your Worker URL or project homepage.
- **Webhook:** active.
- **Webhook URL:** the URL above.
- **Webhook secret:** generate a strong value, for example with `openssl rand -hex 32`.
- **Where can this GitHub App be installed?:** choose **Only on this account** for local testing.

OAuth callback URLs, user authorization, device flow, and post-installation setup URLs are not needed.

### 3. Repository permissions

Configure these permissions under **Repository permissions**:

| Permission    | Access         | Why                                                                                         |
| ------------- | -------------- | ------------------------------------------------------------------------------------------- |
| Metadata      | Read-only      | Repository identity; GitHub grants this mandatory permission to installed apps.             |
| Issues        | Read and write | Read and change issues, issue comments, and labels shared by issues and PRs.                |
| Pull requests | Read and write | Read and change PRs, files, commits, conversation and review comments, reviews, and labels. |
| Checks        | Read-only      | Receive completed check-run events; list check runs; read check output and annotations.     |
| Contents      | Read and write | Merge pull requests. GitHub's merge endpoint specifically requires write access.            |
| Actions       | Read-only      | Resolve GitHub Actions-backed checks to jobs, read job details, and download job logs.      |

No Administration, organization, or account permissions are required.

If you change permissions after installing the app, approve the new permission request for the installation or reinstall the app before testing again.

The example manifest grants every permission needed by the package's resource methods. If you only consume callbacks and call read methods, Issues and Pull requests can be read-only. Omit Contents when the application cannot merge and Actions when it cannot inspect GitHub Actions jobs or logs. The included sample posts comments and reactions, so it needs Issues and Pull requests write access; it does not itself merge PRs or download logs.

### 4. Subscribe to events

Enable exactly these events under **Subscribe to events**:

| GitHub setting              | Webhook name                  | Used for                                                                             |
| --------------------------- | ----------------------------- | ------------------------------------------------------------------------------------ |
| Issues                      | `issues`                      | Open, edit, close, reopen, assignment, and label activity.                           |
| Issue comment               | `issue_comment`               | Issue and PR conversation comments, including mentions.                              |
| Pull request                | `pull_request`                | PR lifecycle, assignment, labels, synchronization, draft state, and review requests. |
| Pull request review         | `pull_request_review`         | Submitted, edited, and dismissed reviews.                                            |
| Pull request review comment | `pull_request_review_comment` | Inline review-comment creation, edits, deletion, and mentions.                       |
| Pull request review thread  | `pull_request_review_thread`  | Review-thread resolution and reopening.                                              |
| Check run                   | `check_run`                   | Completed checks, fanned out to every associated PR.                                 |

The provider ignores unsupported actions, and it ignores check runs that are not associated with a pull request. `check_suite`, `push`, `workflow_run`, and other events are not needed.

### 5. Generate a private key and install the app

After creating the app:

1. On the app settings page, note the numeric **App ID**.
2. Under **Private keys**, generate and download a private key.
3. Open **Install App**, install it on your test account, and grant it access to the repository you want to test.
4. Find the app slug in its settings URL or public URL. The bot login is `<app-slug>[bot]`.
5. Resolve the bot's numeric user ID:

```bash
curl --fail --silent "https://api.github.com/users/<app-slug>%5Bbot%5D" | jq .id
```

### 6. Configure the example

Put these values in `examples/alchemy-cloudflare/.env`:

```dotenv
GITHUB_WEBHOOK_SECRET=the-secret-entered-in-the-github-app-settings
GITHUB_APP_ID=123456
GITHUB_PRIVATE_KEY="-----BEGIN RSA PRIVATE KEY-----
paste-the-complete-downloaded-key-here
-----END RSA PRIVATE KEY-----"
GITHUB_BOT_MENTION_NAME=your-app-slug
GITHUB_BOT_USER_ID=123456789
```

GitHub App bot identities such as `my-reviewer[bot]` are not native mentionable accounts: GitHub does not autocomplete or link `@my-reviewer[bot]`. `GITHUB_BOT_MENTION_NAME` is instead the text invocation name recognized by this provider, without the leading `@`. Use the app slug for a natural command such as `@my-reviewer`. This can still render as plain text; use `[@my-reviewer](https://github.com/apps/my-reviewer)` when a clickable link is important. If the app slug matches a real user or organization, choose a distinct invocation name to avoid notifying that account.

The GitHub provider reads these values while the Worker is constructed, so Alchemy binds them as Cloudflare secrets during deployment and its Durable Objects share the same bindings. The webhook secret verifies incoming requests. The App ID and private key create short-lived installation tokens for API calls. The bot user ID prevents the app from responding to its own events. Secrets are not stored in mailbox admissions or Durable Object storage.

Restart `bun alchemy dev` after changing `.env`. For a deployed stack, deploy the updated secrets with:

```bash
bun alchemy deploy
```

### 7. Test with a repository

Open an issue or pull request in an installed repository. The sample callback reads the discussion and its existing comments (and PR reviews), subscribes the discussion, posts a confirmation comment, and adds an `eyes` reaction to that comment. You can also invoke `@<app-slug>` in an issue body, PR body, issue comment, PR comment, or inline review comment. On every subscribed PR event batch, the example calls `pullRequest.listComments()` and `pullRequest.listReviewComments()`, then logs both current comment counts. Later subscribed comments and inline review comments receive an `eyes` reaction; all subscribed events are logged by the Durable Object. Completed-check callback events include both `headSha` and `checkRunId`, which can be passed to the check-run resource methods without accidentally inspecting a newer push.

In the GitHub App settings, **Advanced → Recent Deliveries** shows each webhook request, response status, and redelivery control. A successful admission returns HTTP 200. If GitHub reports 401, check the webhook secret. If callbacks fail with 403, check the app permissions and make sure the installation includes the repository.

## Slack setup

Create the Slack app from [`slack-app-manifest.example.json`](slack-app-manifest.example.json) after replacing its name and request URL. [`packages/slack-next/README.md`](../../packages/slack-next/README.md) explains each scope and event. Install the app, invite the bot to your test channel, and copy these values into `.env`:

```dotenv
SLACK_SIGNING_SECRET=...
SLACK_BOT_TOKEN=xoxb-...
```

Configure the Slack Events API request URL as:

```text
https://<your-worker-hostname>/integrations/slack/webhook
```

## Whom the example listens to

The example checks authors in its own callbacks (`src/AuthorAccess.ts`); the libraries do not do it for you.

- **GitHub requires write access.** Anyone can comment on a public repository, so every GitHub callback looks up the author's access to the repository with `fetchUserAccess(login)` before it acts. Authors below `write` (`none`, `read`, `triage`) are ignored: no comment, no reaction, and the log line `Example ignored GitHub author without write access` with their login and access level. In a subscribed batch the example acts only on new comments and review comments, so it checks each of those on its own and looks up each author once per batch; it does not look up the authors of events it does nothing with, such as check runs, whose sender is usually an app. If GitHub answers `not_found` (the login is not a user, such as a bot account), the author counts as `none` and the event is ignored and logged. Any other failed lookup fails the callback, which is retried like any other failure; the event never gets through unchecked. The bot's own events are dropped before any callback runs.
- **Slack ignores other workspaces.** In a Slack Connect channel, people from another workspace can mention the bot. Slack marks their messages with `user_team`, which `SlackMessage.authorTeamId` carries. A mention or thread message whose author workspace differs from the installation's is ignored, with the log line `Example ignored Slack author from another workspace`. A message without `user_team` counts as local.

## Fake remote agent

Mention the Slack bot with `handoff [seconds] [flaky]` (for example `@bot handoff 60`; default 60, kept between 5 and 900) to try a durable handoff. The callback starts `FakeRemoteAgent` (`src/FakeRemoteAgentDO.ts`), a Durable Object that stands in for a remote agent host, posts `Handed off <deliveryId>. Finishing in <n>s.`, and returns `delivery.handoff()`. The mailbox stays held while the remote agent waits. When its alarm fires, the remote agent calls `GET /deliveries/<id>`, then `POST /deliveries/<id>/complete` with the final message `Fake remote agent finished after <n>s.`, then `GET /deliveries/<id>` again, all with the delivery's bearer token.

`complete` returns 202 as soon as the result and its `PresentOutcome` output are saved; the second status read shows the delivery `Finishing`, its output not yet applied (`outcome:Pending`, or `outcome:Delivering` if the alarm has already claimed it). The mailbox's alarm then posts the final message to the thread, retires the delivery, and releases the mailbox, so a queued follow-up is answered only after the final message.

While it waits, the remote agent shows what it is doing as the delivery's activity (`PUT /deliveries/<id>/activity`): about a second after it starts it sets `Working` with `Looking into it, about <n>s to go`, and halfway through it sets `Working` again with `Halfway there, …`. Slack shows the text as the thread's status line (`assistant.threads.setStatus`). Just before completing, the remote agent posts one lasting message (`POST /deliveries/<id>/messages`, message ID `summary`): `Summary: the fake remote agent waited <n>s and did no real work.` The result then clears the status line. Activity is for progress that changes; messages are for output that stays. The API never exposes Slack's channel or timestamp.

`react` makes the remote agent use portable reactions (`PUT /deliveries/<id>/reactions/<reaction>`): when it shows its first activity it adds the bot's `rocket` reaction to what started the delivery, halfway through it removes it, and after posting its summary it adds `heart` to the summary. It sends each request twice; the second answers `already_recorded` and changes nothing. It skips a target the delivery does not list in `reactionTargets`, such as a Linear session's messages. `react` works on Slack, GitHub, and Linear, and combines with the other words, as in `handoff 30 react`.

`flaky` makes Slack refuse the final message for 20 seconds (`src/FlakySlackApi.ts` wraps `SlackApiLive` and fails that one post as if Slack were unreachable). Output retries on its own, after waits that double from about a second, and posts once the 20 seconds are up. Neither the callback nor the remote agent runs again.

The remote agent calls the delivery API at the Worker's own public URL, which Alchemy binds at deploy (`Cloudflare.Worker.URL`). If the delivery API cannot be reached, the alarm fails and Cloudflare retries it. If the API refuses the request, the remote agent logs the reason and drops the job. Logs name the delivery ID, stage, and receipt status only, never the token.

### Setup

1. From `examples/alchemy-cloudflare`, run `bun alchemy deploy`, and note the URL it prints.
2. In a second terminal, run `bun alchemy logs --filter IngressWorker --since 10m` and leave it open.

### Checks

In the Slack test channel:

- **Handoff and final message.** Post `@bot handoff 30`. Within a few seconds the bot replies `Handed off delivery:v1:…. Finishing in 30s.` The logs show `Mailbox delivery handed off; waiting for its remote worker`. About 30s later they show `Fake remote agent read delivery status` (stage `ExternalWaiting`), `Fake remote agent completed delivery` (receipt `accepted`), `Fake remote agent read delivery status after completing` (stage `Finishing`, output `outcome:Pending` or `outcome:Delivering`), then `Delivery output started` and `Delivery output applied`. The thread gets `Fake remote agent finished after 30s.`
- **Output retries on its own.** Post `@bot handoff 10 flaky`. After 10s the logs show `Example Slack API refusing a flaky post on purpose` and `Delivery output scheduled for retry` several times, with growing `retry_after_ms`, then `Delivery output applied` about 20s later, and the final message appears. `Slack new mention received` and `Fake remote agent job started` each appear once.
- **Activity and a lasting message.** Post `@bot handoff 30`. About a second after the bot's reply, the thread's status line under the bot shows `Looking into it, about 29s to go`; about 15s later it changes to `Halfway there, about 15s to go`. At 30s the thread gets `Summary: the fake remote agent waited 30s and did no real work.`, then `Fake remote agent finished after 30s.`, and the status line is gone. The logs show `Fake remote agent set its activity` twice and `Fake remote agent posted its summary message` (each `accepted`), and `Delivery output applied` for `SetActivity`, `SetActivity`, `CreateMessage`, then `PresentOutcome`.
- **Queued follow-up.** Post `@bot handoff 60` in a new thread. Right after the bot's reply, post `follow-up` in that thread. The bot doesn't react to the follow-up until the final message is posted; then the subscribed-thread `eyes` reaction appears.
- **Reactions.** Post `@bot handoff 30 react`. About a second after the bot's reply, your mention shows the bot's 🚀 reaction; about 15s later it is gone. At 30s the summary message shows the bot's ❤️. The logs show `Fake remote agent set a reaction` three times, each with `receipt_status=accepted,already_recorded`, and `Delivery output applied` once for each `SetMessageReaction`.
- **Survives a redeploy.** Post `@bot handoff 240`. While it waits, run `bun alchemy deploy --force --yes`. After 240s the logs still show the remote agent's `complete` accepted and the delivery retired.
- **Bad credentials.** Copy the delivery ID from a bot reply, set `WORKER_URL` in your shell to the printed URL, then:

    ```bash
    curl -i -X POST "$WORKER_URL/deliveries/<id>/complete" -H 'content-type: application/json' -d '{}'                                  # 401: no token
    curl -i -X POST "$WORKER_URL/deliveries/<id>/complete" -H 'content-type: application/json' -H 'Authorization: Bearer wrong' -d '{}' # 404
    ```

- Confirm no access token appears in the logs.

### Linear

The same fake remote agent runs Linear Agent Session turns and Linear issue deliveries. It asks each delivery what it supports (`GET /deliveries/<id>`, `supportedOperations`), so it shows activity only where it can.

- **Session.** A session whose prompt context, or whose issue's title or description, says `handoff [seconds] [ask]` hands its turn off, with a link named `Fake remote agent run log` that Linear shows on the session. A reply in the session thread that says `handoff [seconds] [ask]` hands that turn off too. The remote agent's `Working` activity shows as an ephemeral thought, which the next activity replaces; its summary shows as a lasting thought; and the turn ends with one `response` (`Fake remote agent finished after <n>s.`). With `ask` it ends instead with an `elicitation`, `Which environment should I deploy to?`, offering `staging` and `production`; the reply starts a new turn. The agent reads the delivery's status at least every 5 seconds; after Stop it fails the turn with one `error` activity, `Stopped as requested.`, and the stop prompt then reaches `onAgentSessionPrompted` with `signal: stop`, which does nothing more.
- **Issue.** A new issue whose title or description says `issue-handoff [seconds]` is handed off. An issue has no activity, so the agent skips it; the summary and the final message are comments. The keyword differs so that an issue titled `handoff 30` and delegated to the app starts only the session's handoff.
- The run-log link opens `/fake-agent/runs/<deliveryId>` on the Worker, a plain-text line naming the job's step. It never shows the token.

Checks, in the `FAKE` team of the HumanLayer workspace. Use a `channels-live-p4-<timestamp>` marker in each title, and delete the test issues afterwards. People prompt a session by replying in its comment thread; the API does not let a person create activities.

1. **Handoff, activity, a lasting thought, and one response.** Create an issue titled `channels-live-p4-<timestamp> handoff 30` and delegate it to the app. Within 10 seconds the session shows the thought `Working on this…` and the link `Fake remote agent run log`. About a second later the thought `Looking into it, about 29s to go` replaces it; about 15 seconds later `Halfway there, about 15s to go` replaces that. At 30 seconds the session gets the lasting thought `Summary: the fake remote agent waited 30s and did no real work.`, then the response `Fake remote agent finished after 30s.`, and its state is `complete`. No ephemeral thought is left. The logs show `Linear session turn handed off`, `Mailbox delivery handed off; waiting for its remote worker`, then `Delivery output applied` for `AddExternalLink`, `SetActivity`, `SetActivity`, `CreateMessage`, and `PresentOutcome`, in that order, each once.
2. **The run-log link.** Open the link on the session while a turn runs: it reads `Fake remote agent job for delivery:v1:…: step …, about <n>s to go.` After the turn it reads `No job is running here. …`.
3. **The next reply is a new delivery.** Reply `thanks` in the session thread. The logs show `Linear agent session prompted` with a new `delivery_id`, and the session gets the example's local response.
4. **A question ends the turn.** Reply `handoff 20 ask`. After the thoughts and the summary, the session shows the question `Which environment should I deploy to?` with the choices `staging` and `production`, and its state is `awaitingInput`. Pick `staging`: the logs show a new `Linear agent session prompted` delivery, and the session gets the example's local response.
5. **Stop.** Reply `handoff 120`. Once `Looking into it…` shows, press Stop. Within about 5 seconds the logs show `Fake remote agent stopped because the delivery asked it to`, the session shows one `error` activity, `Stopped as requested.`, and then `Linear agent session prompted` with `prompt_signal: stop`, after which nothing more is posted.
6. **Reactions on a session.** Create an issue titled `channels-live-p4-<timestamp> handoff 30 react` and delegate it to the app. About a second after `Looking into it…` shows, the issue has the app's 🚀 reaction; about 15 seconds later it is gone. The summary thought gets no ❤️: a session's messages take no reactions, so the agent skips it. The logs show `Fake remote agent set a reaction` twice, each with `receipt_status=accepted,already_recorded`. (A session started from a comment reacts on that comment instead; automated tests cover it.)
7. **Reactions on an issue delivery.** Create an issue titled `channels-live-p4-<timestamp> issue-handoff 30 react`, not delegated. The issue gets 🚀, which goes about 15 seconds later; the summary comment gets ❤️.
8. **Issue delivery.** Create an issue titled `channels-live-p4-<timestamp> issue-handoff 20`, not delegated. The logs show `Linear issue delivery handed off`. About 20 seconds later the issue gets the comment `Summary: the fake remote agent waited 20s and did no real work.`, then the comment `Fake remote agent finished after 20s.`. The logs show no `Fake remote agent set its activity` for it.
9. Confirm no access token appears in the logs.

### GitHub

The same fake remote agent runs GitHub issue and pull request deliveries. Mention the app with `handoff [seconds] [ask]` in an issue or pull request comment, an inline review comment, or the body of a new issue or pull request (for example `@<app-slug> handoff 30`). The callback subscribes the issue or pull request, starts the job, comments `Handed off <deliveryId>. Finishing in <n>s.`, and hands the delivery off.

- **Activity is `eyes`.** GitHub has no status line, so `Working` adds the bot's `eyes` reaction to what mentioned the bot: the comment, or the issue or pull request itself when the mention was in its body. `Idle` removes it, and so does the result. The text of `Working` is not shown. Adding `eyes` that is already there, or removing it when it is already gone, changes nothing.
- **Messages are comments.** The summary is one comment on the issue or pull request, posted just before the result; the final message is one more comment. With `ask` the final comment is the question, with `- staging` and `- production` listed under it. The delivery API never exposes installation, repository, or comment IDs.
- A later comment in a subscribed issue or pull request, without a new mention, is a subscribed batch. It has nothing that started it to react to, so its delivery does not list `SetActivity`.
- `flaky` is a Slack-only test switch; a GitHub mention ignores it.

Checks, in a test repository where the app is installed (`GITHUB_BOT_MENTION_NAME` is the name after `@`):

1. **Handoff, `eyes` while working, a summary comment, and one final comment.** Open an issue titled `channels-live-p5-<timestamp>` and comment `@<app-slug> handoff 30`. Within a few seconds the app comments `Handed off delivery:v1:…. Finishing in 30s.`, and about a second later your comment shows the app's `eyes` reaction. At 30 seconds the app comments `Summary: the fake remote agent waited 30s and did no real work.`, then `Fake remote agent finished after 30s.`, and the `eyes` reaction is gone. The logs show `GitHub bot mentioned`, `GitHub mention handed off`, `Mailbox delivery handed off; waiting for its remote worker`, then `Delivery output applied` for `SetActivity`, `SetActivity`, `CreateMessage`, and `PresentOutcome`, in that order, each once. Comment edits and deletes are covered by automated tests only.
2. **Pull request.** On a pull request, comment `@<app-slug> handoff 20`. The same happens on the pull request's conversation: `eyes` on your comment while it works, then the summary comment and the final comment, with `eyes` gone at the end.
3. **Mention in a body.** Open an issue whose body is `@<app-slug> handoff 20`. The `eyes` reaction shows on the issue itself, not on a comment, and is gone after the final comment.
4. **A question ends the delivery.** Comment `@<app-slug> handoff 20 ask`. The final comment is `Which environment should I deploy to?` with `- staging` and `- production` under it, and `eyes` is gone.
5. **Queued follow-up.** Comment `@<app-slug> handoff 30`, then right away comment `follow-up` in the same issue. The app's `eyes` reaction on `follow-up` (from the subscribed-events callback) appears only after the final comment.
6. **`flaky` is ignored.** Comment `@<app-slug> handoff 10 flaky`. The final comment is `Fake remote agent finished after 10s.` with no extra text, and the logs show no `Example Slack API refusing a flaky post on purpose`.
7. **Read access gets no response.** From an account with only read access to the repository (on a public repository, any account that is not a collaborator), comment `@<app-slug> handoff 20` on an issue. Nothing is posted and no reaction appears. The logs show `GitHub bot mentioned`, then `Example ignored GitHub author without write access` with that login and `access=read`, and no `GitHub mention handed off`. A plain comment from that account in a subscribed issue gets no `eyes` reaction either.
8. **Reactions.** Comment `@<app-slug> handoff 30 react`. About a second after the `eyes` reaction, your comment also gets the app's 🚀; about 15 seconds later the 🚀 is gone and `eyes` stays. The summary comment gets ❤️. The logs show `Fake remote agent set a reaction` three times, each with `receipt_status=accepted,already_recorded`.
9. Confirm no access token appears in the logs.

- The Worker verifies provider signatures before admitting events through typed Durable Object RPC.
- Admissions, processing state, and subscriptions use persistent SQLite-backed Durable Object storage.
- Slack and GitHub mailboxes use debounce delivery. Linear uses immediate serial delivery so Agent Session acknowledgements can meet Linear's ten-second responsiveness requirement.
- The alarm claims ordered batches and dispatches them to the matching Slack, GitHub, or Linear processor.
- GitHub App installation tokens and resolved bot identity are cached by the live GitHub API layer.
- Linear client-credentials tokens are acquired lazily, identity-verified, and cached only in the processing runtime.

Replace the sample callbacks in `src/Bot.ts` with application behavior. Delivery timing, lease length, and attempt limits are configured there as well.

## Logs

Read recent Worker and Durable Object logs with:

```bash
bun alchemy logs --filter IngressWorker --since 10m
```

Alchemy currently uses Effect's readable multiline logger and `bun alchemy logs` does not have a JSON output option. Effect also provides single-line JSON logging through `Logger.layer([Logger.consoleJson])`, but applying that inside this example does not replace all of Alchemy's own logs or make the command return complete JSON. A consistent JSON view requires logger and JSON-output support in Alchemy itself.
