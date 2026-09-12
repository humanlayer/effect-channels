# Native GitHub issues, PR activity and subscriptions

`@humanlayer/channels-github` is independent of Slack, Node, Alchemy and database
drivers. This checkpoint supports outbound issue/discussion operations and signed
issue/PR activity with application-owned subscriptions. It receives reviews and
inline review activity; it does not implement outbound PR management, review
submission, workflow operations, OAuth or installation lifecycle management.

## Composition

- `GitHub.layer` supplies `createIssue`, `getIssue`, `updateIssue`, `createComment`,
  `updateComment` and a bounded `listComments` page (100 comments; explicit `page`).
  Issue operations use `GitHubRepository` / `GitHubIssueRef`. Comment operations use
  `GitHubDiscussionRef` (issue or `GitHubPullRequestRef`) / `GitHubCommentRef`.
  Discussion reads verify the issue/PR kind before a write; PR handles do not grant
  access to issue-only operations. These are native shared Issues REST comments,
  not PR review comments.
  Comment updates also verify the returned comment ID and exact parent API URL
  before mutation; a matching issue number in another repository is not sufficient.
  Operations require credentials and an Effect `HttpClient`, not an inbox, signing
  secret, subscriptions or worker. They do not automatically retry HTTP writes.
- `GitHub.layer` also supplies outbound `addReaction`, `listReactions` and
  `removeReaction` for issue/PR **bodies** and their shared Issues REST discussion
  comments. These do not subscribe to reaction events or start polling.
- `GitHubCredentials.layer(options)` owns one immutable App configuration and a
  bounded token cache. `layerConfig` reads `GITHUB_APP_ID`, `GITHUB_PRIVATE_KEY`,
  `GITHUB_INSTALLATION_ID`, `GITHUB_BOT_USER_ID` and optional `GITHUB_API_URL`.
  Programmatic configuration accepts multiple explicitly configured installation IDs.
- `GitHubCrypto.layerWebCrypto` implements RS256 and HMAC-SHA256 using host
  WebCrypto, isolated because Effect Crypto does not provide those operations.
  Both unencrypted PKCS#1 and PKCS#8 RSA PEM private keys are accepted. No Node
  crypto import enters the library. Hosts must provide WebCrypto.
- `GitHubIngress.layer({ namespace, policy, handlers })` accepts only `GitHubHandlerRegistration` entries with `id`, `onCreation`, `onMention`, and `onSubscribedEvent`. `acceptActivity({ event, mentioned, own })` saves routing and commits all selected callbacks; `processActivity({ event })` processes at most one ready batch per callback for that resource. `run(options)` owns a scoped polling runner. Admission never runs handlers. Normally the signed route owns normalization and bot identity classification. Every ingress requires an explicitly supplied `GitHubSubscriptionStore`. There is no bare `handler(event)` registration, alternate registration property, mode switch, or legacy `accept`/`process` pipeline.
- `GitHubRoutes.layer({ signingSecret, maxBodyBytes, botLogin })` mounts
  `POST /api/v1/integrations/github/webhook`. `layerConfig` reads
  `GITHUB_WEBHOOK_SECRET` and **required `GITHUB_BOT_LOGIN`**, and limits raw bodies
  to 256,000 bytes. Login is the App bot login, e.g. `channels[bot]`, not a human
  account, installation owner, or App display name. Supply services
  using `Layer.provide`; mount with Effect `HttpRouter.serve` or
  `HttpRouter.toWebHandler`. Forward the untouched request, including in Hono.
- `GitHubBot.make` assembles native operations, ingress, `routes`, `worker` and combined `layer`. Namespace and delivery policy remain explicit. `runner` is optional and accepts `Partial<RunnerOptions>`; omitted fields default to `scanLimit: 100`, `concurrency: 8`, and `pollMs: 25`, matching Slack. These are runner defaults, not changes to delivery policy or mailbox namespaces. Routes alone start no worker; hosts deliberately choose the combined Layer for long-running servers. Heterogeneous callbacks retain their typed errors and infer all required Effect services; handler failures are classified for delivery retry, while ingress exposes `GitHubIngressError`, not arbitrary application errors. Callers need no `ReturnType` union or explicit generic parameters. The library owns that inference and captures the complete handler environment once.

See [the GitHub example](../../examples/github) and
[the combined example](../../examples/slack-github) for complete, typechecked roots.
Each example owns its handlers, configuration, transport and tests; neither imports
another example. The combined notifier reads Config directly at its handler
boundary rather than introducing a NotificationTarget service or custom schema.

## Subscriptions and typed activity handlers

`GitHubSubscriptions` exposes only `subscribe`, `unsubscribe` and `isSubscribed`,
each taking `{ namespace, resource }`. A resource is a native issue or PR reference.
This is application routing state, **not** GitHub's user-notification subscription
API. Installation ID, stable repository ID, issue/PR kind, number and namespace
isolate subscriptions. Repository renames do not change identity. Different Apps
or GitHub origins must use different namespaces.

```ts
import { Effect } from 'effect'
import { GitHub, GitHubBot, GitHubSubscriptions } from '@humanlayer/channels-github'
import { layer as memory } from '@humanlayer/channels-github/memory'

// policy is explicit; runner may be omitted or partially overridden.
const bot = GitHubBot.make({
	namespace: 'agent',
	policy,
	handlers: [
		{
			id: 'respond',
			onCreation: (event) =>
				Effect.gen(function* () {
					// Optional: observe every new issue/PR and choose to follow without a mention.
					if (event.resource.kind === 'github.issue') {
						const subscriptions = yield* GitHubSubscriptions
						yield* subscriptions.subscribe({ namespace: 'agent', resource: event.resource })
					}
				}),
			onMention: (event) =>
				Effect.gen(function* () {
					const subscriptions = yield* GitHubSubscriptions
					yield* subscriptions.subscribe({ namespace: 'agent', resource: event.resource })
					const github = yield* GitHub
					yield* github.createComment({ issue: event.resource, body: 'Received your mention.' })
				}),
			onSubscribedEvent: () => Effect.void,
		},
	],
})
const storage = memory({ maxMailboxes: 10_000, maxSubscriptions: 10_000, maxRoutes: 50_000 })
// Supply storage once alongside credentials/crypto/HTTP at the application root.
```

- `onCreation` receives `GitHubCreationEvent` (`opened` issue or PR), independent
  of mention and subscription. It may run in addition to `onMention`.
- `onMention` receives a typed `GitHubMentionEvent`: new mentions in issue/PR
  bodies, discussion comments, review bodies or inline review comments/replies.
- `onSubscribedEvent` receives the `GitHubActivityEvent` discriminated union below.
  Within one registration an eligible mention callback takes precedence over this
  callback. Independent subscribed-only consumers still receive the same event.
  Creation callbacks cannot retroactively change the already-frozen route for the
  creation delivery; their subscription affects subsequent deliveries.
- Each callback has a shared Delivery `HandlerContext`. Activity callbacks consume
  the existing **serial** policy regardless of the supplied message policy mode:
  lifecycle notifications are not coalesced. Serialization is per callback and
  resource, not global across consumers or between creation/mention/followed work.

| Native event                  | Supported actions / relevant context                                                                                                                                                              |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `issues`                      | `opened`, `edited`, `closed`, `reopened`, `assigned`, `unassigned`, `labeled`, `unlabeled`; issue snapshot, actor, changed assignee/label                                                         |
| `issue_comment`               | `created`, `edited`, `deleted` for issue **and PR** discussions; native comment and parent                                                                                                        |
| `pull_request`                | `opened`, `edited`, `closed`, `reopened`, `synchronize`, `review_requested`, `review_request_removed`, `assigned`, `unassigned`, `labeled`, `unlabeled`, `converted_to_draft`, `ready_for_review` |
| `pull_request_review`         | `submitted` (`approved`, `changes_requested`, `commented`), `edited`, `dismissed`; review ID/node ID, body, actor and commit                                                                      |
| `pull_request_review_comment` | `created`, `edited`, `deleted`, including replies; path, diff hunk, commit IDs, line/side, review ID, comment node ID and parent PR URL                                                           |
| `pull_request_review_thread`  | `resolved`, `unresolved`; native `thread.node_id` and comments; sender may be absent                                                                                                              |

Merged is **`pull_request.closed` with `pull_request.merged: true`**, not an invented
`merged` action. Synchronization retains required `before`/`after` and head/base
refs/SHAs; head SHA must match `after`. Other PR payloads retain head/base/draft/
merged when supplied. Requested reviewers may be users or teams. Assignment events
describe other users; there is no promise that this App's bot is assignable.

`reviewCommentRootId(comment)` returns `in_reply_to_id ?? id`; GitHub's REST reply
target is a top-level review comment, not another reply. A review thread's node ID
is not a numeric comment/review ID. We keep subscriptions and delivery mailboxes at
the parent PR, while retaining these native review identities in the event. There
is no fabricated Slack thread ID or review-thread mutation API.

Schemas/actions were checked against [GitHub's webhook documentation](https://docs.github.com/en/webhooks/webhook-events-and-payloads)
and [official OpenAPI webhook schemas](https://github.com/github/rest-api-description/blob/main/descriptions/api.github.com/api.github.com.json),
including [top-level review reply targets](https://docs.github.com/en/rest/pulls/comments#create-a-reply-for-a-review-comment).
CI/check/workflow correlation, conflict monitoring, merge queues, general push
events and other unlisted actions are deliberately excluded and acknowledged
without dispatch.

### Storage and retry contract

Supply `GitHubSubscriptions.layer` over a replacement `GitHubSubscriptionStore`,
or explicitly import `subscriptions()` from `/memory`. `subscriptionStore()` is
the raw memory store; `subscriptions()` adds the small domain service; `layer()`
also supplies shared Delivery memory. The core export does not choose memory and
outbound-only `GitHub.layer` does not require or bundle subscriptions.

The store's `resolveRoute` **atomically** returns an existing decision or reads the
subscription and saves the complete set of selected callback IDs before any
mailbox admission. Empty decisions are saved too. Retry never reevaluates those
targets after subscribe/unsubscribe or partial fanout; independent mailbox dedupe
fills only missing admissions. Store adapters must serialize this operation with
subscribe/unsubscribe, encode on write, decode on read, preserve namespace/resource
isolation, and retain decisions throughout their supported replay window. Never
evict a partial-fanout decision just because a timer elapsed. Missing configured
targets fail admission rather than pretending fanout completed.

Memory stores are bounded (default 10,000 subscriptions / 50,000 routing decisions),
volatile and **do not automatically expire or evict decisions**. Capacity exhaustion
fails closed with `capacity` (webhook 503). Unsubscribe frees subscription capacity,
not route capacity. A long-running production application needs a durable adapter
with an explicit safe archival/replay policy; restarting memory loses all guarantees
for previous deliveries. There is no disk backend in this slice.

Routing records have schema version `1`; envelopes retain `github.activity` definition version `1`. The rename from `activityHandlers` to `handlers` preserves every current callback mailbox ID exactly: `JSON.stringify([registration.id, 'creation' | 'mention' | 'subscribed'])`. Keep registration IDs and namespaces unchanged to recover current saved activity work and frozen fan-out routes.

**Breaking API cleanup:** older bare `handler(event: GitHubIssueEvent)` work used `github.issue` v1 and different handler IDs. That older legacy API work is **not migrated or processed by this release**. Drain it using the previous release before upgrading, or explicitly handle it operationally; changing a namespace does not migrate it. There is no compatibility alias, second bot/ingress pipeline, migration framework, automatic rollback, or mixed-writer support. The standalone `GitHubIssueEvent` parser remains available, but `issueEventDefinition` is removed. Memory reconstruction proves logical recovery only, not process-crash persistence. External writes can repeat.

## Outbound reactions

```ts
import { Effect } from 'effect'
import { GitHub, type GitHubReactionTarget } from '@humanlayer/channels-github'

// target is an issue.ref, a GitHubPullRequestRef, or a comment.ref.
export const acknowledge = (target: GitHubReactionTarget) =>
	Effect.gen(function* () {
		const github = yield* GitHub
		const added = yield* github.addReaction({ target, content: 'eyes' })
		const page = yield* github.listReactions({ target, page: 1, perPage: 100 })
		yield* github.removeReaction({ reaction: added.ref })
		return page
	})
```

Supply the same outbound credentials/HTTP Layer as other native operations; no
inbox or signing secret is needed. `GitHubReaction` contains a target-scoped
`GitHubReactionRef` and decoded native `data` (`id`, `node_id`, nullable `user`,
`content`, `created_at`). Native content is exactly `+1`, `-1`, `laugh`, `confused`,
`heart`, `hooray`, `rocket`, or `eyes`, not Slack aliases, custom emoji or Unicode.

`listReactions` requires explicit positive safe-integer `page` and `perPage`
(1–100); optional `content` filters the page. It fetches **one page**, rejects an
overfull response, and does not follow Link URLs or imply the array is the entire
collection. Callers own any bounded traversal; concurrent mutations can shift pages.
Removal uses the reaction ID returned by add/list, not its content. GitHub decides
whether the authenticated actor can remove that reaction; a missing reaction is
`not_found`, not silently successful. Refs carry identity, not authorization grants.

Every operation verifies the repository ID, issue/PR kind and number; comment
targets additionally verify the comment ID and exact parent API URL before the
reaction request. This adds two preflight reads for bodies and three for comments.
Credentials remain installation/repository-scoped; parent checks do not remove
the race between a read and provider mutation. There is no cross-origin redirect
or arbitrary reaction URL API.

[Official REST reactions protocol](https://docs.github.com/en/rest/reactions/reactions?apiVersion=2022-11-28):
body operations use `/repos/{owner}/{repo}/issues/{number}/reactions`; discussion
comments use `/repos/{owner}/{repo}/issues/comments/{id}/reactions`. Creates accept
201 (new) or 200 (already exists); lists require 200; deletes require **204 with no
JSON body**. The existing `issues: write` installation-token scope covers these
endpoints; no additional scopes are requested. The adapter retains API version
`2022-11-28`. PR review/inline comments, commit comments, releases and GitHub
Discussions are outside this API.

Failures retain `GitHubError`: authentication (401, invalidates the scoped cache),
forbidden (ordinary 403), not_found (404/410), invalid_input (local validation/422),
response (invalid success status/schema), or unavailable (transport/server/rate
limits, with timing hints when available). Safe internal failure metadata is
captured before narrowing, without payloads or credentials. **No operation is
automatically retried**, including a create whose response was lost.

**Emulator limit:** source inspection and executable probes confirm that installed
`emulate@0.11.0` only emits default GitHub reaction summaries; all twelve
GET/POST/DELETE combinations on emulator-created issue/PR bodies and discussion
comments return 404. Accordingly, provider-visible reaction add/list/remove
cannot be certified with this version. Tests do not install a synthetic success
route and call it emulator evidence. Stateful synthetic `HttpClient` contracts
exercise all four target shapes, request bodies/headers, 200/201/204 handling,
bounded pages, errors, parent checks and overlapping tenant-isolated add/list/remove
through the real native service and credential cache. Emulator tests independently
verify actual targets, route absence, real App authentication and tenancy rejection.
No live credentials were used; end-to-end provider reaction verification remains
blocked on emulator support or a separately authorized live test.

## App-bot targeting and assignment limits

With `botLogin: 'channels[bot]'`, text `@channels` or `@channels[bot]` (case-insensitive,
whole-token boundaries) in an issue/PR body on `opened`, or a discussion comment
on `created`, admits work. On `edited`, the body must newly acquire the mention:
`changes.body.from` must exist and not already match. Unrelated edits, removal,
closure, reopen and deletion do not trigger a **mention** callback. Subscribed
activity callbacks do receive them. New review bodies and inline comments follow
the same text rule. Sender IDs/logins suppress the configured bot's own events;
content-author suppression also applies to opened/created/edited/submitted content,
including someone editing an existing bot comment. Human lifecycle changes on a
bot-authored issue/PR are still observable. Other bots are not blanket-suppressed.

This is literal **webhook body parsing**, not a GitHub mention-notification API or
autocomplete promise. Matching text inside quotes/code also counts; this is not a
Markdown renderer or an authorization decision. Restrict installations/repository
access and add application authorization before privileged actions. No title-only
targeting or native autocomplete behavior is implied.

**Assignment to an arbitrary GitHub App's bot is not a supported routing path in
this slice.** `assigned`/`unassigned` are followed activity, not automatic targeting
or assignment-management operations. There is no human-account fallback or fabricated
bot-assignee success test. An opened issue that is assigned but also mentions this
App still routes because of the mention, not the assignment.

Current research consulted:

- [Official assignee documentation](https://docs.github.com/en/issues/tracking-your-work-with-issues/using-issues/assigning-issues-and-pull-requests-to-other-github-users)
  describes users and a specific Copilot path, not general custom-App-bot eligibility.
- [Official REST assignee endpoints](https://docs.github.com/en/rest/issues/assignees#check-if-a-user-can-be-assigned)
  accept App installation authentication, but that means the App can **manage**
  assignees, not that its bot can **be** an assignee. Eligibility checks return
  204/404 for a particular repository/user; assignment responses can silently omit
  invalid assignees. An HTTP success alone is not evidence of bot assignment.
- [GitHub Community #53504](https://github.com/orgs/community/discussions/53504)
  remains unanswered, including 2026 reports of custom-App limitations and a
  regular-user workaround. That workaround does not meet this application's identity
  requirement and is deliberately not used. This is corroborating community
  evidence, not a universal official prohibition.
- [Official App webhook guide](https://docs.github.com/en/apps/creating-github-apps/registering-a-github-app/using-webhooks-with-github-apps)
  grounds the supported alternative: subscribe to issue, PR and comment payloads
  and inspect their bodies.

No authorized live App/repository was provided for an eligibility probe. General
custom-App-bot assignment is therefore **not verified or promised**, rather than
claimed universally impossible. A future assignment feature needs a real eligibility
check and observed assigned state for the actual bot ID, not a substituted user.

## Identity, admission and recovery

All routes normalize into the native discriminated `GitHubActivityEvent` union and use the same registration/routing pipeline. Without `botLogin`, creation and subscribed activity still work, but mention detection is disabled; there is no all-issue legacy routing fallback. Supported events share installation + stable repository ID + resource kind + number identity. PR bodies and PR discussion comments share the same PR mailbox, distinct from issue mailboxes. Mutable owner/name fields are API routing information, not mailbox identity. Namespace and callback ID independently scope mailboxes. Use different namespaces for different GitHub origins/Apps and keep IDs stable on redeploy.

HMAC verification operates on original bytes before JSON decoding. The bounded
stream reader rejects oversized bodies. Bad signatures receive 401, malformed
eligible bodies 400, unknown configured installations 403, and failed admission 503.
Valid ping/unsupported event families/actions, non-targeted content and configured
self-bot events are deliberately acknowledged without admission. No token is minted
just to acknowledge a webhook. Obtain the App bot's numeric **user ID**, not App ID,
for `botUserId`; incorrect configuration can cause reply loops.

ACK follows all required admissions, not handler completion. Partial fanout returns
503; redelivery idempotently fills missing admissions. Dedupe uses
`X-GitHub-Delivery`, retained according to the shared policy. GitHub signs the body,
**not delivery/event headers**; this is not cryptographic protection against an
attacker replaying a captured valid body with changed headers. Protect secrets and
transport. GitHub does not automatically redeliver every failed webhook: arrange
operational redelivery/monitoring of failures.

All GitHub callbacks use shared `serial` delivery, retaining native lifecycle events without coalescing or interrupting active work. Context retains typed `skipped` events and saved organization attribution. Handler Effects must remain open until their work finishes. Known permanent GitHub failures do not retry; `unavailable` and other application errors use the configured finite handler retry policy. Defects retain Delivery's terminal behavior. Error responses are empty; logs retain safe failure categories, never tokens or payload bodies. Applications should add their own safe diagnostics for custom handler failures.

GitHub 403 responses are not always permission failures: exhausted
`x-ratelimit-remaining`, valid numeric `retry-after`, or bounded inspection of known
GitHub rate-limit error messages classify them as `unavailable`, like 429. Ordinary
permission 403s remain terminal `forbidden`. Raw error bodies are never logged.
`GitHubError.retryAfterMs` preserves the later of the Retry-After seconds and primary
reset deadline when supplied; recognized limits without usable timing default to
60 seconds per GitHub guidance. Outbound callers receive the hint immediately and
own any safe retry policy; the HTTP adapter never replays a mutation.

Delivery has no per-failure retry deadline. GitHub ingress therefore waits
interruptibly inside the handler's existing lease/heartbeat before returning a
retryable failure; Delivery then applies its normal finite backoff. This occupies
a worker slot during the wait, including on the last attempt. It is not a shared
installation-wide limiter or a persisted rate-limit deadline across restart. No
shared Delivery contracts are changed.
Application handler scopes close before this wait, releasing callback resources
while the delivery lease/heartbeat remains active. This applies to every registered callback.

## Authentication and security boundaries

App JWTs backdate issuance 60 seconds and expire in nine minutes. Minted installation
tokens request **only one repository ID and `issues: write`**. Cache capacity is
1,000 entries; keys include installation and repository. App identity, private-key
version and API origin are scoped by the acquired credentials Layer. Concurrent
same-key lookups share work. Success TTL stops at least 60 seconds before provider
expiry, capped at 59 minutes; failed lookups are not cached. An operation's 401
invalidates its entry without automatically repeating the mutation. Rebuild the
credentials Layer on key/installation configuration changes. No live revocation or
installation-removal webhook processing is claimed.

Private keys and tokens are `Redacted` in credential values, not encrypted at rest.
Never expose credential services or log arbitrary HTTP requests to application users.
API origins require HTTPS, except explicit loopback HTTP for local tests.

## Verified boundary

Tests use `emulate@0.11.0` (bundling `@emulators/github@0.11.0`), real App JWT and
token exchange, generated signed deliveries, and provider-visible issues/comments.
The emulator supports narrowed token metadata inspection. Its installation-token
actor is modeled as an installation account, not a faithful App bot identity;
synthetic signed tests cover production self-bot suppression. Synthetic HTTP and
TestClock tests cover failures, cache expiry, concurrency and secret-safe errors.
Live-clock tests are necessary for emulator JWT expiry and Fetch runtime clocks.

The PR example test uses an actual emulator-generated signed PR delivery, real App
token exchange, and provider-visible PR discussion comment create/read/update.
The emulator incorrectly excludes PRs from `GET /repos/:owner/:repo/issues/:number`;
only that read is supplied by a narrow synthetic HTTP response in this test.
Production uses GitHub's documented shared Issues endpoint, not an emulator-specific
fallback. Signed synthetic tests cover PR discussion-comment routing and edit
transitions. They do not prove native App-bot assignment or autocomplete support.

Expanded emulator tests receive actual unmentioned issue/PR creation, subscribe in
the creation handler, observe close/reopen/merge webhooks and inspect native state.
The standalone responder proves an unmentioned followed comment adds no bot reply.
Executable emulator gap probes deliberately reject malformed generated payloads:
reviews use uppercase REST states, review-request events omit the changed reviewer,
and root inline comments contain `in_reply_to_id: null` instead of an absent key.
The production schema is not weakened or payloads re-signed to hide those gaps.
Review-thread events, synchronization and draft transitions are not emitted by this
installed emulator. The complete activity matrix, review replies, edit targeting,
native root identities, own-event suppression and frozen retries use explicitly
synthetic signed protocol tests with real memory/Delivery/ingress services.

Memory mailboxes are volatile. Tests establish logical reconstruction and shared
delivery contracts, **not disk/crash persistence**. External comments and Slack
notifications can repeat after a crash between provider success and mailbox
completion. There is no exactly-once external-write guarantee, live permission
certification, deployment, or automatic retry/redelivery service in this checkpoint.

## Application organization lookup

`GitHubOrganizations` is a separate optional service, supplied through a Layer. `resolve({ installationId })` receives the existing native numeric `GitHubId` validated at ingress and returns `{ organizationId }` or `null`, with `GitHubOrganizationLookupError` for dependency failure. This complements the existing synchronous `GitHubCredentials.acceptsInstallation` check; it does not replace or change credentials, signature verification, or installation authorization.

Without an override, admitted events belong to `default`. `GitHubOrganizations.fixed({ organizationId: 'single-org' })` configures another fixed organization. Use `GitHubOrganizations.layer(({ installationId }) => findOrganization({ installationId }))` with your application's Effect-returning lookup. The callback returns `Effect<GitHubOrganization | null, E, R>`; native input, errors and dependencies are inferred. The factory captures `R` once during Layer construction; provide it privately using `Layer.provide`. It owns the `github.organizations.resolve` span, safe diagnostic classifications, decoding, and mapping application errors to `GitHubOrganizationLookupError`. No user spans or error mapping are required. Direct custom service Layers remain supported. Provide the Layer to the entire bot composition. Configured null/failure never falls back to the default; credentials and delivery storage remain independent.

Unified ingress persists attribution before admitting fan-out. Duplicate delivery and reconstruction reuse that decision, and ordinary handlers receive the saved `context.organizationId`. Unknown mappings return HTTP 200 without admitted handler work; expected typed lookup/storage failures retain HTTP 503. `GitHubOrganizations.layer` suspends the callback and catches causes, including synchronous throws before it returns an Effect. Defects are safely classified as `unexpected_defect` and carried as optional `reason: 'unexpected'` through lookup/ingress errors to an empty HTTP 500 at the unified webhook route. Omitted reason retains unavailable behavior, including existing `GitHubOrganizationLookupError.make({})` callers. Interruption takes priority over failure/defect classification and preserves cancellation without lookup-failure recovery or logging. GitHub does not guarantee automatic redelivery, so applications must arrange redelivery after operational failures where needed.

Custom lookup results are decoded as `GitHubOrganization | null` before any attribution write. Undefined, malformed, or empty-ID results fail rather than mean absence. Diagnostics report fixed classifications, not application error messages or returned data. The exported lookup/organization schemas have same-name inferred interfaces; `fixed` still validates configuration with `makeEffect` and retains its rc.112 `SchemaIssue.Issue` error type. The extracted ingress-attribution effect is package-internal, not an additional public API or service. Organization emulator tests use the live clock specifically for App JWT timestamps checked by the independently running emulator, not simply because they are integration tests.

See [the executable organization lookup recipe](../../examples/organization-lookup/README.md): application callbacks supply both native provider Layers. The library introduces no universal ID, registry, shared installation schema, or cyclic provider dependencies. Provider-visible tests use actual emulator-generated signed requests, real App authentication, and native comment reads; overlapping installation tests additionally exercise one shared acquired ingress with deterministic gates.

Mailbox v3 is a stop-old-writers upgrade. Fixed deployments explicitly attribute legacy unowned work to their configured organization. Custom-directory deployments refuse to dispatch it unless the application deliberately supplies fixed legacy attribution; automatic per-event legacy migration remains deferred. Positive attribution records are conservatively retained and consume delivery-store capacity, including memory `maxMailboxes`; new admissions fail on exhaustion rather than discarding an accepted association. See delivery's README for the full upgrade and retention limitations.
