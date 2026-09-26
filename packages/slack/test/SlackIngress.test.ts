import { NodeCrypto } from '@effect/platform-node'
import { assert, it } from '@effect/vitest'
import { DeliveryInterruption, DeliveryQueue, IngressAttributionStore } from '@humanlayer/channels-delivery'
import { Clock, ConfigProvider, Context, Effect, Layer, Queue, Schema } from 'effect'
import { TestClock } from 'effect/testing'
import { HttpRouter } from 'effect/unstable/http'

import {
	SlackIngress as Ingress,
	IngressAccepted,
	type NormalizedConversationStopped,
	type NormalizedMessage,
	type NormalizedReaction,
} from '../src/index'
import { SlackEventCallback } from '../src/Schema'
import { SlackRoutes } from '../src/SlackRoutes'
import { SlackSubscriptions } from '../src/SlackSubscriptions'
import { nativePolicy } from './nativeSupport'
import {
	appMentionCallback,
	makeTestIngress,
	signSlackBody,
	testCredentialsLayer,
	testRouteSlackClientLayer,
	unusedSlack,
} from './support'

const routeLayer = SlackRoutes.layerMounted('/api/v1').pipe(
	HttpRouter.provideRequest(NodeCrypto.layer),
	Layer.provide(
		ConfigProvider.layer(
			ConfigProvider.fromUnknown({
				SLACK_SIGNING_SECRET: 'test-signing-secret',
				SLACK_BOT_USER_ID: 'U_BOT',
			}),
		),
	),
	Layer.provide(testCredentialsLayer),
	Layer.provide(testRouteSlackClientLayer),
)

it.effect('reports the exact application-mounted webhook path', () =>
	Effect.sync(() => {
		assert.strictEqual(SlackRoutes.webhookPath, '/integrations/slack/webhook')
		assert.strictEqual(SlackRoutes.mountedWebhookPath('/api/v1'), '/api/v1/integrations/slack/webhook')
	}),
)

it.effect('mounts webhook admission with addressed services and no mailbox processing services', () =>
	Effect.acquireUseRelease(
		Effect.sync(() => {
			const admission = Ingress.layer({
				namespace: 'route-contract',
				policy: nativePolicy,
				handlers: {},
			}).pipe(
				Layer.provide(unusedSlack),
				Layer.provide(
					Layer.mergeAll(
						Layer.mock(DeliveryQueue, {}),
						Layer.mock(DeliveryInterruption, {}),
						Layer.mock(IngressAttributionStore, {}),
						Layer.mock(SlackSubscriptions, {}),
					),
				),
			)
			return HttpRouter.toWebHandler(routeLayer.pipe(Layer.provide(admission)), { disableLogger: true })
		}),
		({ handler }) =>
			Effect.promise(() =>
				handler(
					new Request('http://localhost/unrouted', { method: 'POST' }),
					Context.make(Ingress, makeTestIngress({})),
				),
			).pipe(Effect.map((response) => assert.strictEqual(response.status, 404))),
		({ dispose }) => Effect.promise(dispose),
	),
)

it.effect('resolves MPIM reaction identities before ingress admission', () =>
	Effect.gen(function* () {
		const accepted = yield* Queue.unbounded<NormalizedReaction>()
		const ingress = makeTestIngress({
			acceptReaction: (event) =>
				Queue.offer(accepted, event).pipe(
					Effect.as(IngressAccepted.make({ idempotencyKey: event.idempotencyKey })),
				),
		})
		const callback = yield* Schema.decodeEffect(SlackEventCallback)({
			type: 'event_callback',
			team_id: 'T_TEST',
			event_id: 'Ev_REACTION_MPIM',
			event_time: 1_788_000_000,
			event: {
				type: 'reaction_added',
				user: 'U_TEST',
				reaction: 'thumbsup',
				item: { type: 'message', channel: 'G_MPIM', ts: '100.1' },
				event_ts: '101.1',
			},
		})
		const request = yield* signedRequest(callback)
		const { dispose, handler } = HttpRouter.toWebHandler(routeLayer, { disableLogger: true })
		yield* Effect.addFinalizer(() => Effect.promise(dispose))
		assert.strictEqual((yield* Effect.promise(() => handler(request, Context.make(Ingress, ingress)))).status, 200)
		const reaction = yield* Queue.take(accepted)
		assert.strictEqual(reaction.thread.ref.id, 'slack:v1:T_TEST:mpim:G_MPIM:100.1')
		assert.strictEqual(reaction.thread.ref.channel.isDm, true)
		assert.strictEqual(reaction.directMessageThread?.id, 'slack:v1:T_TEST:mpim:G_MPIM')
	}).pipe(Effect.provide(NodeCrypto.layer)),
)

const signedRequest = (callback: SlackEventCallback) =>
	Effect.gen(function* () {
		const body = yield* Schema.encodeEffect(Schema.fromJsonString(SlackEventCallback))(callback)
		const currentTime = yield* Clock.currentTimeMillis.pipe(TestClock.withLive)
		const timestamp = Math.floor(currentTime / 1000).toString()
		const signature = yield* signSlackBody(body, timestamp)
		return new Request('http://channels.test/api/v1/integrations/slack/webhook', {
			method: 'POST',
			headers: {
				'content-type': 'application/json',
				'x-slack-request-timestamp': timestamp,
				'x-slack-signature': signature,
			},
			body,
		})
	})

it.effect('verifies and normalizes a signed Slack request through the Fetch handler', () =>
	Effect.gen(function* () {
		const accepted = yield* Queue.unbounded<NormalizedMessage>()
		const ingress = makeTestIngress({
			acceptMessage: (message) =>
				Queue.offer(accepted, message).pipe(
					Effect.as(IngressAccepted.make({ idempotencyKey: message.idempotencyKey })),
				),
		})
		const callback = yield* Schema.decodeEffect(SlackEventCallback)(appMentionCallback)
		const request = yield* signedRequest(callback)
		const { dispose, handler } = HttpRouter.toWebHandler(routeLayer, { disableLogger: true })
		yield* Effect.addFinalizer(() => Effect.promise(dispose))
		const response = yield* Effect.promise(() => handler(request, Context.make(Ingress, ingress)))
		const normalized = yield* Queue.take(accepted)

		assert.strictEqual(response.status, 200)
		assert.strictEqual(normalized.thread.ref.id, 'slack:v1:T_TEST:C_TEST:100.1')
		assert.strictEqual(normalized.message.text, 'hello from Slack')
	}).pipe(Effect.provide(NodeCrypto.layer)),
)

it.effect('normalizes and admits an agent session stop', () =>
	Effect.gen(function* () {
		const accepted = yield* Queue.unbounded<NormalizedConversationStopped>()
		const ingress = makeTestIngress({
			acceptConversationStopped: (event) =>
				Queue.offer(accepted, event).pipe(
					Effect.as(IngressAccepted.make({ idempotencyKey: event.idempotencyKey })),
				),
		})
		const callback = yield* Schema.decodeEffect(SlackEventCallback)({
			type: 'event_callback',
			team_id: 'T_TEST',
			event_id: 'Ev_STOP',
			event_time: 1_788_000_000,
			event: {
				type: 'agent_session_stopped',
				channel: 'G_MPIM',
				thread_ts: '100.1',
				user: 'U_TEST',
				event_ts: '101.1',
				streaming_message_ts: [],
			},
		})
		const request = yield* signedRequest(callback)
		const { dispose, handler } = HttpRouter.toWebHandler(routeLayer, { disableLogger: true })
		yield* Effect.addFinalizer(() => Effect.promise(dispose))
		const response = yield* Effect.promise(() => handler(request, Context.make(Ingress, ingress)))

		assert.strictEqual(response.status, 200)
		const stopped = yield* Queue.take(accepted)
		assert.strictEqual(stopped.threadRef.id, 'slack:v1:T_TEST:mpim:G_MPIM:100.1')
		assert.strictEqual(stopped.threadRef.channel.isDm, true)
		assert.strictEqual(stopped.directMessageThread?.id, 'slack:v1:T_TEST:mpim:G_MPIM')
	}).pipe(Effect.provide(NodeCrypto.layer)),
)
