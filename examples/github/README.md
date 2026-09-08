# GitHub App mention responder (memory, Node/Bun or Fetch)

The standalone app subscribes to an issue/PR on its App bot's text mention and
replies to mentions in bodies, discussion comments, reviews and inline comments.
Unmentioned creations are logged without subscribing. Other followed activity logs
safe native event/action metadata, with no comments or reaction invocation. Handlers are
plain Effect functions in `src/handlers.ts`; `src/app.ts` chooses shared Delivery
limits. Credentials, HTTP and volatile storage belong in `src/transport.ts`.

## Run locally

From the repository root:

```sh
bun install
bun run build
# Supply configuration securely in your shell or secret manager, then:
bun run --cwd examples/github start
```

Required variables: `GITHUB_APP_ID`, `GITHUB_PRIVATE_KEY` (multiline unencrypted RSA
PEM), `GITHUB_INSTALLATION_ID`, `GITHUB_BOT_USER_ID` (numeric App bot user ID),
`GITHUB_BOT_LOGIN` (e.g. `channels[bot]`), and
`GITHUB_WEBHOOK_SECRET`. Optional: `PORT` (3000), `GITHUB_API_URL`
(`https://api.github.com`). Do not commit private keys or copy existing env files.

Configure an installed GitHub App with repository **Issues: read/write** and
**Pull requests: read**, and subscribe to **Issues**, **Issue comment**, **Pull request**,
**Pull request review**, **Pull request review comment** and **Pull request review thread**
events. The webhook path is
`/api/v1/integrations/github/webhook`. A local server is not a public webhook URL;
public HTTPS exposure and real App setup are separately authorized operations.
Only configured installations are accepted. Mention `@channels` or `@channels[bot]`
in a new body/comment, or edit an existing body/comment to introduce the mention.
The app ignores its own sender/content-author identity and unmentioned unfollowed
events. Edits that already contained the mention are followed activity, not another
mention. Incorrect bot identity configuration may create loops. Titles do not target
the bot. Subscription state is supplied explicitly by the GitHub memory subpath.
Creation is the exception to ignoring unmentioned, unfollowed events: it is observed
but does not opt in. Creation callbacks are independent and additive: an opened
issue with a mention is both logged and replied to once. Mentions take precedence
only over the followed callback within this registration.

**App-bot assignment is not implemented or verified:** issue-opened-with-assignees
does not target the bot. `assigned`/`unassigned` for followed issues/PRs are ordinary
silent activity; no human account is substituted or assignment API invoked. This uses
literal webhook-body matching, not native GitHub autocomplete/notifications. See
[the provider targeting contract and evidence](../../packages/github/README.md#app-bot-targeting-and-assignment-limits).

`src/fetch.ts` exports `makeHost()`. Create it once per long-running host, forward
the original `Request` to `host.handler`, and await `host.dispose()` on shutdown.
For Hono, pass `c.req.raw`; never parse/reserialize before forwarding. This example
intentionally starts a scoped worker, so it is **not** a serverless Fetch/DO alarm
implementation. For admission-only hosting, mount `bot.routes` and own the worker
separately. Importing the factory does not start a server or worker.

Proactive callers use `GitHub.layer` without constructing `GitHubBot` or routes;
see the provider tests for create/read/update operations with no signing secret or
mailbox. Activity callbacks use Delivery's existing serial policy: lifecycle events
are not coalesced. Only newly introduced mentions reply on edits; closure/deletion
alone do not post. `onCreation: observeCreation` deliberately leaves opt-in to a
mention; it does not infer labels or assignments. All example code is local to this example
or imported from library packages—there are no imports of sibling examples.

## Activity and explicit operator actions

`src/app.ts` registers the three named effects from `src/handlers.ts`:

```ts
{ id: 'respond', onCreation: observeCreation, onMention: respond, onSubscribedEvent: observeActivity }
```

`observeActivity` discriminates `event` before reading native fields: review
`review.state`, comment IDs and review-thread IDs. A PR `closed` action reads
`pull_request.merged` to distinguish a merge from an unmerged close; `synchronize`
logs `before`/`after` commit IDs. Both observers log event/action, delivery ID,
installation/repository IDs, resource kind and issue number, never
bodies, titles, diff hunks or credentials. No followed event posts a comment.

`src/usage.ts` contains independently callable, typechecked effects—not startup
code or webhook callbacks:

```ts
const wasSubscribed = yield * stopFollowing(resource) // isSubscribed + unsubscribe
const added = yield * acknowledge(target) // addReaction, content: 'eyes'
const firstPage = yield * listAcknowledgements(target) // listReactions, page: 1, perPage: 20
// Only on a later explicit cleanup decision:
yield * removeAcknowledgement(added.ref) // removeReaction
```

Import these functions from `./usage.js` inside this example. Targets are native
issue/PR body refs or discussion-comment refs (not review/inline-comment refs).
The bounded list is one page, not all reactions. Retain the returned reaction ref
for removal. Provide `GitHub` for reaction effects and the application's shared
`GitHubSubscriptions` for cleanup; `bot.services` supplies these when composed with
`transport`. Do not create a second memory store for cleanup. Unsubscribing stops
future followed routing, not already frozen deliveries or future direct mentions;
a later mention subscribes again. These operations have typed failures and no
automatic mutation retries. Calling one reaction operation never calls another.

`wasSubscribed` is a snapshot read before unsubscribe, not an atomic
"removed an existing subscription" result. Concurrent cleanup calls can both
return `true`, and a concurrent mention can subscribe again. Do not use the
boolean as an exclusive claim or as proof of current subscription state.

The installed emulator has **no reaction endpoints** (GET/POST/DELETE return 404).
The main app therefore never invokes these helpers. Their example tests substitute
the `GitHub` service only to check API usage; successful HTTP reaction lifecycle
evidence is synthetic in `packages/github/test/Reactions.test.ts`, not emulator or
live-provider proof.

## Verify without credentials

```sh
./node_modules/.bin/vp test examples/github/test packages/github/test
bun run --cwd examples/github typecheck
```

`test/Handlers.test.ts` checks creation opt-in, subscription cleanup against the
memory layer, native activity discrimination/safe logs and explicit reaction calls.
`test/App.test.ts` exercises the actual app routes with emulator-generated signed issue, PR and comment
deliveries, admit before execution, deduplicate and create provider-visible replies.
Tests use local ephemeral emulators and generated keys, not live GitHub. Memory is
volatile; accepted work disappears on process exit. Provider writes can repeat
after recovery. The PR test substitutes only the emulator's broken shared issue
read; App auth and PR discussion writes use the emulator. The test also verifies
that a followed non-mention creates no extra bot reply. Reviews/inline roots have
known emulator payload incompatibilities; review threads, synchronization and draft
transitions use signed synthetic coverage in the provider tests. See the provider
README for the exact gaps, capacity limits and migration requirements. This example deploys nothing.
