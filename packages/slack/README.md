# Native Slack + shared delivery

`@humanlayer/channels-slack` exposes Slack operations, schema-backed handles,
signed webhook routes, and typed handler registration. There is no Channels
facade, provider registry, or app package.

## Outbound only

```ts
import { Slack, SlackClient, SlackTenantCredentials, ThreadId, MarkdownContent } from '@humanlayer/channels-slack'
import { Effect, Layer } from 'effect'
import { FetchHttpClient } from 'effect/unstable/http'

const client = SlackClient.layer.pipe(
	Layer.provide(SlackTenantCredentials.layerFromConfig),
	Layer.provide(FetchHttpClient.layer),
)
const native = Slack.layer.pipe(Layer.provide(client))
const post = Effect.flatMap(Slack, (slack) =>
	slack.post({
		threadId: ThreadId.make('slack:v1:T_WORKSPACE:C_CHANNEL:100.1'),
		content: MarkdownContent.make({ markdown: 'Hello' }),
	}),
).pipe(Effect.provide(native))
```

This needs the bot token and HTTP, not a signing secret, mailbox store,
subscriptions, Node defaults, or a worker. `Thread.post`, `SentMessage.edit`,
reactions, files, history, streaming, and DMs use the same native service.
`Thread.subscribe` separately requires `SlackSubscriptions`. Handles hydrate
history when a `SlackUserDirectory` is available; otherwise they retain native
ID-based authors. Native low-level history remains independently usable.

## Inbound hosting

See [`examples/slack-thread-echo/src/app.ts`](../../examples/slack-thread-echo/src/app.ts)
for the complete application-owned Layer graph:

1. `SlackIngress.layer({ namespace, policy, handlers })` binds stable handler IDs
   and native event schemas to shared delivery.
2. Supply delivery `/memory`, `SlackSubscriptions.layerMemory`, Slack, and its
   disposable user directory.
3. Mount `SlackRoutes.layer` in Effect HTTP. It verifies the original body and
   commits all required admissions before returning success; it starts no worker.
4. Explicitly run `SlackIngress.run({ scanLimit, concurrency, pollMs })` in the
   host's scope. It awaits handlers and finalizes active fibers on shutdown.
   Each handler invocation receives its own scope, including when it uses captured
   application services. Invalid delivery policies fail ingress Layer acquisition.

`HttpRouter.toWebHandler(routes)` supplies a standard
`Request → Promise<Response>` function for Hono or another Fetch host. The host
must separately own the runner, or explicitly combine its worker Layer with the
routes as the echo example does. Await the returned `dispose()` on shutdown.
Do not reconstruct the Layer graph for every request.

Message queue behavior is latest plus `context.skipped`, not FIFO. Lifecycle
callbacks are serial per handler. Different handler IDs are independent; no
cross-handler execution order is promised. Mentions do not implicitly subscribe.
Other bots remain eligible; only this installation's own echoes are suppressed.
Routing decisions are remembered across retries, including mention/message twins
and proactive-DM subscription changes. Memory routes are installation/channel
scoped and retained for one day by default; configure routing retention to cover
your provider replay window. Memory is volatile and capacity bounded.

Ephemeral-to-DM fallback requires explicit `EphemeralFallbackToDm`; it is never
an implicit persistent write. A duplicate Stop cannot cancel a successor attempt.
There is no exactly-once guarantee for external Slack writes.
Explicit stream-source retryability is preserved; a non-retryable source failure
does not become retryable merely because it passed through Slack streaming.

The multi-tenant example supplies an application-owned credential lookup through
`SlackTenantCredentials.layerWithLookup`. It preserves ambient requirements and
caches successful workspace lookups (including missing installations) for one minute.
Failed lookups have zero cache TTL so a recovered repository can serve the next
request. OAuth is not implemented. Profile hydration falls back on expected lookup
failures, but does not turn defects or interruption into successful delivery.
