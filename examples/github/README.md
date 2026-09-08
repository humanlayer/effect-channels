# GitHub App mention responder (memory, Node/Bun or Fetch)

The standalone app subscribes to an issue/PR on its App bot's text mention and
replies to mentions in bodies, discussion comments, reviews and inline comments.
Other followed activity is handled silently, with no reaction invocation. Handlers are
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
alone do not post. Add `onCreation` to the activity registration to observe an
unmentioned issue/PR creation and choose whether to subscribe independently of
mentions; see the provider README's typed example. All example code is local to this example
or imported from library packages—there are no imports of sibling examples.

## Verify without credentials

```sh
./node_modules/.bin/vp test examples/github/test packages/github/test
bun run typecheck
bun run build
```

The actual app routes receive emulator-generated signed issue, PR and comment
deliveries, admit before execution, deduplicate and create provider-visible replies.
Tests use local ephemeral emulators and generated keys, not live GitHub. Memory is
volatile; accepted work disappears on process exit. Provider writes can repeat
after recovery. The PR test substitutes only the emulator's broken shared issue
read; App auth and PR discussion writes use the emulator. The test also verifies
that a followed non-mention creates no extra bot reply. Reviews/inline roots have
known emulator payload incompatibilities; review threads, synchronization and draft
transitions use signed synthetic coverage in the provider tests. See the provider
README for the exact gaps, capacity limits and migration requirements. This example deploys nothing.
