import { NodeCrypto } from '@effect/platform-node'
import { assert, it } from '@effect/vitest'
import { Clock, ConfigProvider, Context, Effect, Layer, Logger, Queue, Schema } from 'effect'
import { TestClock } from 'effect/testing'
import { HttpRouter } from 'effect/unstable/http'

import { UnknownTenant } from '../src/DomainErrors.js'
import { SlackApiError, SlackTransportError } from '../src/Errors.js'
import { SlackIngress, IngressAccepted, TenantId, type NormalizedReaction } from '../src/index.js'
import { SlackEventCallback } from '../src/Schema.js'
import { SlackRoutes } from '../src/SlackRoutes.js'
import { reactionAddedCallback, signSlackBody, stubSlackClientLayer, testCredentialsLayer } from './support.js'

const failures = [
	{
		name: 'UnknownTenant',
		effect: Effect.fail(UnknownTenant.make({ provider: 'slack', tenant: TenantId.make('T_TEST') })),
		status: 200,
	},
	{
		name: 'SlackTransportError',
		effect: Effect.fail(SlackTransportError.make({ operation: 'conversations.replies', status: 503 })),
		status: 200,
	},
	{
		name: 'SlackApiError',
		effect: Effect.fail(SlackApiError.make({ operation: 'conversations.replies', code: 'message_not_found' })),
		status: 200,
	},
	{ name: 'defect', effect: Effect.die('reaction lookup defect'), status: 500 },
] as const

for (const failure of failures) {
	it.effect(`reaction lookup ${failure.name}: only known failures fall back`, () =>
		Effect.gen(function* () {
			const accepted = yield* Queue.unbounded<NormalizedReaction>()
			const logs: Array<string> = []
			const ingress = Layer.mock(SlackIngress, {
				acceptReaction: (event) =>
					Queue.offer(accepted, event).pipe(
						Effect.as(IngressAccepted.make({ idempotencyKey: event.idempotencyKey })),
					),
			})
			const routeLayer = SlackRoutes.layer.pipe(
				HttpRouter.provideRequest(NodeCrypto.layer),
				HttpRouter.provideRequest(ingress),
				Layer.provide(
					ConfigProvider.layer(
						ConfigProvider.fromUnknown({
							SLACK_SIGNING_SECRET: 'test-signing-secret',
							SLACK_BOT_USER_ID: 'U_BOT',
						}),
					),
				),
				Layer.provide(testCredentialsLayer),
				Layer.provide(stubSlackClientLayer({ replies: () => failure.effect })),
				HttpRouter.provideRequest(
					Logger.layer([Logger.make((entry) => logs.push(JSON.stringify(entry.message)))]),
				),
			)
			const callback = yield* Schema.decodeEffect(SlackEventCallback)(reactionAddedCallback)
			const body = yield* Schema.encodeEffect(Schema.fromJsonString(SlackEventCallback))(callback)
			const now = yield* Clock.currentTimeMillis.pipe(TestClock.withLive)
			const timestamp = Math.floor(now / 1000).toString()
			const signature = yield* signSlackBody(body, timestamp)
			const request = new Request('http://channels.test/api/v1/integrations/slack/webhook', {
				method: 'POST',
				body,
				headers: {
					'content-type': 'application/json',
					'x-slack-request-timestamp': timestamp,
					'x-slack-signature': signature,
				},
			})
			const { handler, dispose } = HttpRouter.toWebHandler(routeLayer, { disableLogger: true })
			yield* Effect.addFinalizer(() => Effect.promise(dispose))
			const response = yield* Effect.promise(() => handler(request, Context.empty()))
			assert.strictEqual(response.status, failure.status)
			assert.strictEqual(yield* Queue.size(accepted), failure.status === 200 ? 1 : 0)
			const fallbackLogs = logs.filter((log) => log.includes('using reacted message as thread root'))
			assert.strictEqual(fallbackLogs.length, failure.status === 200 ? 1 : 0)
			if (failure.status === 200) {
				assert.ok(fallbackLogs[0]?.includes(failure.name))
				assert.strictEqual((yield* Queue.take(accepted)).thread.ref.id, 'slack:v1:T_TEST:C_TEST:100.1')
			}
		}).pipe(Effect.provide(NodeCrypto.layer)),
	)
}
