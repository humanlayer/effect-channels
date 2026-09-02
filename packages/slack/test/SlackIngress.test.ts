import { NodeCrypto } from '@effect/platform-node'
import { assert, it } from '@effect/vitest'
import { Ingress, IngressAccepted, type NormalizedMessage, unimplemented } from '@humanlayer/channels'
import { Clock, Context, Effect, Queue, Schema } from 'effect'
import { TestClock } from 'effect/testing'
import { HttpRouter } from 'effect/unstable/http'

import { SlackEventCallback } from '../src/Schema.ts'
import { SlackRoutes } from '../src/SlackRoutes.ts'
import { appMentionCallback, signSlackBody, signingSecret } from './support.ts'

it.effect('verifies and normalizes a signed Slack request through the Fetch handler', () =>
	Effect.gen(function* () {
		const accepted = yield* Queue.unbounded<NormalizedMessage>()
		const ingress = Ingress.of({
			acceptMessage: (message) =>
				Queue.offer(accepted, message).pipe(
					Effect.as(IngressAccepted.make({ idempotencyKey: message.idempotencyKey })),
				),
			acceptMessageUpdated: () => unimplemented('test.acceptMessageUpdated'),
			acceptMessageDeleted: () => unimplemented('test.acceptMessageDeleted'),
			acceptReaction: () => unimplemented('test.acceptReaction'),
			acceptConversationStopped: () => unimplemented('test.acceptConversationStopped'),
		})
		const callback = yield* Schema.decodeEffect(SlackEventCallback)(appMentionCallback)
		const body = yield* Schema.encodeEffect(Schema.fromJsonString(SlackEventCallback))(callback)
		const currentTime = yield* Clock.currentTimeMillis.pipe(TestClock.withLive)
		const timestamp = Math.floor(currentTime / 1000).toString()
		const signature = yield* signSlackBody(body, timestamp)
		const routeLayer = SlackRoutes.layer({ signingSecret, botUserId: 'U_BOT' }).pipe(
			HttpRouter.provideRequest(NodeCrypto.layer),
		)
		const { dispose, handler } = HttpRouter.toWebHandler(routeLayer, { disableLogger: true })
		yield* Effect.addFinalizer(() => Effect.promise(dispose))
		const response = yield* Effect.promise(() =>
			handler(
				new Request('http://channels.test/api/v1/integrations/slack/webhook', {
					method: 'POST',
					headers: {
						'content-type': 'application/json',
						'x-slack-request-timestamp': timestamp,
						'x-slack-signature': signature,
					},
					body,
				}),
				Context.make(Ingress, ingress),
			),
		)
		const normalized = yield* Queue.take(accepted)

		assert.strictEqual(response.status, 200)
		assert.strictEqual(normalized.thread.ref.id, 'slack:v1:T_TEST:C_TEST:100.1')
		assert.strictEqual(normalized.message.text, 'hello from Slack')
	}).pipe(Effect.provide(NodeCrypto.layer)),
)
