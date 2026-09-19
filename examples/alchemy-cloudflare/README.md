# Alchemy Cloudflare Slack and GitHub mailbox example

This example receives Slack and GitHub webhooks in a Cloudflare Worker, stores each event in a mailbox Durable Object, and processes mailboxes from Durable Object alarms. Both providers are declared in `src/Bot.ts`; `src/Worker.ts` builds webhook ingress and `src/DurableObject.ts` builds persistent processing from that same declaration.

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

| Permission    | Access         | Why                                                                              |
| ------------- | -------------- | -------------------------------------------------------------------------------- |
| Metadata      | Read-only      | Repository identity; GitHub grants this mandatory permission to installed apps.  |
| Issues        | Read and write | Read issues and issue comments; post, edit, delete, and react to issue comments. |
| Pull requests | Read and write | Read PRs, reviews, and review comments; post, edit, delete, reply, and react.    |
| Checks        | Read-only      | Receive completed check-run events associated with pull requests.                |

No Contents, Actions, Administration, organization, or account permissions are required by the current implementation.

If you change permissions after installing the app, approve the new permission request for the installation or reinstall the app before testing again.

If you only consume callbacks and never use the resource methods that write comments or reactions, Issues and Pull requests can be read-only. The included sample posts comments, so it needs write access.

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
GITHUB_BOT_MENTION_NAME=your-app-slug[bot]
GITHUB_BOT_USER_ID=123456789
```

`GITHUB_BOT_MENTION_NAME` is the bot's complete login without the leading `@`. For example, the login `my-reviewer[bot]` uses `my-reviewer[bot]`. Using the complete login avoids confusing an app with a user or organization that has the same base name.

The Worker reads these values during initialization, so Alchemy binds them as Cloudflare secrets during deployment and its Durable Objects share the same bindings. The webhook secret verifies incoming requests. The App ID and private key create short-lived installation tokens for API calls. The bot user ID prevents the app from responding to its own events. Secrets are not stored in mailbox admissions or Durable Object storage.

Restart `bun alchemy dev` after changing `.env`. For a deployed stack, deploy the updated secrets with:

```bash
bun alchemy deploy
```

### 7. Test with a repository

Open an issue or pull request in an installed repository. The sample callback reads the discussion and its existing comments (and PR reviews), subscribes the discussion, posts a confirmation comment, and adds an `eyes` reaction to that comment. You can also mention `@<app-slug>[bot]` in an issue body, PR body, issue comment, PR comment, or inline review comment. Later subscribed comments and inline review comments receive an `eyes` reaction; all subscribed events are logged by the Durable Object.

In the GitHub App settings, **Advanced → Recent Deliveries** shows each webhook request, response status, and redelivery control. A successful admission returns HTTP 200. If GitHub reports 401, check the webhook secret. If callbacks fail with 403, check the app permissions and make sure the installation includes the repository.

## Slack setup

Copy these values from your Slack app into `.env`:

```dotenv
SLACK_SIGNING_SECRET=...
SLACK_BOT_TOKEN=xoxb-...
```

Configure the Slack Events API request URL as:

```text
https://<your-worker-hostname>/integrations/slack/webhook
```

## Runtime behavior

- The Worker verifies provider signatures before admitting events through typed Durable Object RPC.
- Admissions, processing state, and subscriptions use persistent SQLite-backed Durable Object storage.
- Mailboxes use debounce delivery: a batch runs after 2 seconds of quiet and no later than 10 seconds after its first event.
- The alarm claims ordered batches and dispatches them to the matching Slack or GitHub processor.
- GitHub App installation tokens and resolved bot identity are cached by the live GitHub API layer.

Replace the sample callbacks in `src/Bot.ts` with application behavior. Delivery timing, lease length, and attempt limits are configured there as well.
