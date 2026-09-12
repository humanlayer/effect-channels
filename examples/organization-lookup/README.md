# Organization lookup application

This is a runnable Slack + GitHub bot application. A verified Slack mention or GitHub issue/PR discussion mention is admitted with an application organization ID, and an ordinary local handler replies `Organization: <id>`. It does not start remote agents or introduce example-defined services.

## Where the app lives

- `src/app.ts` creates `SlackBot` and `GitHubBot`, registers the handlers, and supplies the organization Layers to the entire merged routes/worker graph.
- `src/handlers.ts` posts replies using saved `context.organizationId`, without resolving ownership again.
- `src/organizations.ts` supplies the two library lookup callbacks.
- `src/storage.ts` supplies one shared memory delivery store and separate provider subscriptions/routing.
- `src/transport.ts` supplies independently configured credentials, cryptography and HTTP transport.
- `src/server.ts` runs the Node HTTP server and scoped bot workers; `src/fetch.ts` exports `makeHost()` for an in-process Fetch host with the same graph.

All application code belongs to this example; it imports no sibling examples. The structure follows `slack-github`, `github`, and `slack-thread-echo`.

## Supply the callbacks

`src/organizations.ts` contains the complete implementations:

```ts
import { GitHubOrganizations } from '@humanlayer/channels-github'
import { SlackOrganizations } from '@humanlayer/channels-slack'
import { Effect, Layer } from 'effect'

export const slackOrganizations = SlackOrganizations.layer(({ workspaceId }) =>
	Effect.sync(() => {
		if (workspaceId === 'T_NORTH') return { organizationId: 'north' }
		if (workspaceId === 'T_SOUTH') return { organizationId: 'south' }
		return null
	}),
)

export const githubOrganizations = GitHubOrganizations.layer(({ installationId }) =>
	Effect.sync(() => {
		if (installationId === 100) return { organizationId: 'north' }
		if (installationId === 200) return { organizationId: 'south' }
		return null
	}),
)

export const organizations = Layer.merge(slackOrganizations, githubOrganizations)
```

The callbacks receive native identities: branded `SlackTeamId` for Slack and numeric `GitHubId` for GitHub. Replace the sample branches with your application's Effect-returning database/API lookup. Callback dependencies are inferred and captured once when the Layer is built; provide them privately with `Layer.provide` at runtime composition. The service itself does not require those dependencies on each call. Delivery storage and credentials remain independent.

Return `{ organizationId }` or `null` for an unknown identity. The library owns the lookup span, result decoding, safe diagnostic classification, and mapping application failures to the provider's `OrganizationLookupError`. No user spans, error mapping, or Layer constructors are needed. Failures are never absence; null never falls back to a default. Direct custom service Layers and `.fixed` remain supported.

## Run the server

Unlike the old credential-free lookup demo, hosting this application requires real provider credentials supplied through the environment. No credentials are bundled. Replace the sample identity branches with your actual workspace/installation mappings or database/API lookup first: ordinary live identities will otherwise return `null` and be acknowledged without a reply. Organization mapping does not grant provider access; credentials must independently accept that installation.

Required environment variables:

| Provider | Variables                                                                                                                                         |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| Slack    | `SLACK_TEAM_ID`, `SLACK_BOT_TOKEN`, `SLACK_BOT_USER_ID`, `SLACK_BOT_ID`, `SLACK_SIGNING_SECRET`                                                   |
| GitHub   | `GITHUB_APP_ID`, `GITHUB_PRIVATE_KEY` (PEM contents), `GITHUB_INSTALLATION_ID`, `GITHUB_BOT_USER_ID`, `GITHUB_BOT_LOGIN`, `GITHUB_WEBHOOK_SECRET` |
| Host     | Optional `PORT` (default `3000`); optional `GITHUB_API_URL` (default `https://api.github.com`)                                                    |

The sample host configures one Slack connection and one GitHub installation. Multiple installations require separately supplied credential Layers; the lookup callbacks alone do not configure them. Configuration is loaded through Effect `Config`; missing or malformed required configuration fails startup. Secrets remain `Redacted` until provider calls.

From the repository root, with environment variables already exported:

```sh
bun --no-env-file run build
bun --no-env-file run --cwd examples/organization-lookup start
./node_modules/.bin/vp test --run examples/organization-lookup/test
```

The commands deliberately do not load `.env` files. The server mounts `POST /api/v1/integrations/slack/webhook` and `POST /api/v1/integrations/github/webhook` and runs both workers in the same application scope. Provider webhook configuration and publicly reachable HTTPS are host responsibilities; starting a local listener does not configure a Slack/GitHub app. Stop with SIGINT/SIGTERM to interrupt workers and close resources.

For an existing Fetch-capable server, create one `const host = makeHost()` from `src/fetch.ts`, delegate requests to `host.handler(request)`, and call `await host.dispose()` on shutdown. Reuse the host across requests. It owns long-lived in-memory state and scoped polling workers, so this entry is not a durable serverless/Cloudflare deployment recipe.

## Wiring

```ts
import { Layer } from 'effect'
import { organizations } from './organizations.js'

// src/app.ts constructs both bots with the handlers and delivery policy.
const application = Layer.merge(slackBot.layer, githubBot.layer).pipe(Layer.provide(organizations))
```

Provide the lookup Layers once in the graph shared by your provider ingress/routes and workers, alongside the bot's other required Layers for credentials, delivery storage, and transport. Ingress invokes the callbacks and saves the organization association before handler execution; handlers do not resolve it again. You can provide just `slackOrganizations` or `githubOrganizations` for a single provider. For a single organization, use `SlackOrganizations.fixed({ organizationId })` and `GitHubOrganizations.fixed({ organizationId })` instead.

## Explicit conversation lifecycle

1. Slack `onNewMention` calls `event.thread.subscribe()` before replying with the saved organization. Later unmentioned thread messages reach `onSubscribedMessage` and receive the organization reply.
2. GitHub uses the single `handlers` API. Its `onMention` calls `GitHubSubscriptions.subscribe({ namespace: githubNamespace, resource: event.resource })` before replying. The namespace is defined once in `handlers.ts` and shared with the bot registration; the subscription follows the parent issue/PR, not an individual comment.
3. GitHub `onSubscribedEvent` responds only to nonempty human textual activity: opened/edited issue or PR bodies, created/edited discussion or review comments, and submitted/edited review bodies. Labels, assignments, status changes, deleted comments, empty text and bot events do not get organization replies. Followed nontext events may still be admitted and complete without a reply.
4. Send exactly `unsubscribe` in a followed Slack thread or GitHub comment to remove the subscription. Leading/trailing whitespace and command case are ignored. A leading bot mention also works: Slack normalization removes the native `<@BOT_ID>` token; GitHub removes the configured `@GITHUB_BOT_LOGIN` prefix. Extra words such as `please unsubscribe` are ordinary text, not commands.
5. Unsubscribe acknowledges `Unsubscribed. Mention me to engage again.` and does not immediately subscribe again, even when invoked with a mention. Subsequent unmentioned input is not admitted for these handlers. A new ordinary mention subscribes again.

Both reply callbacks use saved `context.organizationId`; neither loads agent settings nor resolves ownership again. Unsubscribe changes future routing, **not cancellation**: already accepted work and frozen routing decisions can still execute. These explicit operations are application behavior using existing APIs, not a new automatic subscription policy.

The app test verifies this lifecycle through both routes and workers, including native reply readback, subscription state, committed completion signals and zero mailbox commits for ignored follow-ups. It uses no sleeps. Focused composition tests also check that nontext/deleted/bot GitHub activity produces no reply.

## Failure behavior and tests

Unknown mappings return HTTP 200 without handler admission. Expected typed lookup failures and malformed lookup results retain HTTP 503, as do existing typed storage failures. A callback defect (`Effect.die`) or synchronous throw before returning its Effect is caught by the factory and produces a sanitized empty HTTP 500 at the actual webhook route. The optional lookup/ingress error `reason: 'unexpected'` carries that distinction; omitted reason retains the existing unavailable behavior and `.make({})` remains valid for lookup errors. No arbitrary error message, cause or callback result is copied into responses/logs.

Factories suspend the callback before invoking it. Installed Effect v4 rc.112 represents combined causes as a flat `reasons` array; `Cause.hasDies` inspects all reasons, so a combined typed failure plus defect is unexpected, not merely the first typed failure. Interruption takes priority: interrupt reasons (including HTTP client-abort annotations) propagate without lookup-failure recovery/logging. Fetch client aborts retain the framework's 499 response, not 500.

`test/App.test.ts` runs the complete app's two routes and workers with fake credentials and local provider emulators, waits for committed completion, and reads native Slack replies/GitHub comments. Slack ingress is signed synthetic input with sample workspace `T_NORTH` mapped to the seeded emulator connection; GitHub ingress is emulator-generated and signed. The provider HTTP adapter rejects unexpected origins. No live accounts or databases are exercised. Composition tests additionally cover both sample organizations, unknown identities and overlapping handler processing with recording outbound services. Provider suites exercise factory defects, synchronous throws, typed 503, unknown 200, no rejected-work posts and client cancellation at actual signed routes.

## Upgrade gate and capacity

Stop old writers before upgrading to mailbox v3. Custom multi-org Layers refuse unattributed legacy work; they do not invent an owner or migrate it automatically. Existing attributed work retains its owner; fixed-org Layers explicitly attribute legacy work to their configured ID. Review any populated deployment's migration separately.

Positive admission decisions are retained without expiry to protect unfinished fan-out. They consume delivery storage and the memory adapter's `maxMailboxes`; exhausted capacity rejects new admission. The sample memory stores are volatile: restarting loses delivery state, attribution and subscriptions. Replace delivery and provider-routing storage independently for a persistent host; choosing a database for organization lookup does not make delivery durable. This example adds no cleanup, OAuth, remote execution, or library-owned application database schema.

The sample caps delivery storage at 10,000 mailboxes. Provider memory defaults allow 10,000 subscriptions and 50,000 frozen routes each. Slack subscriptions expire after 30 days and its frozen DM routes after one day; GitHub subscriptions and frozen routes have no expiry. Unsubscribe removes the subscription, not accepted delivery records or frozen routes, and is not a storage-reclamation strategy. Capacity errors remain errors; no cleanup or eviction policy is added here.
