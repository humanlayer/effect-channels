/**
 * The delivery API end to end: a signed-in-by-token remote worker finishes a handed-off delivery
 * through `Channels.make`'s own routes, with in-memory storage and a real router.
 */
import * as NodeCrypto from '@effect/platform-node/NodeCrypto'
import { describe, it } from '@effect/vitest'
import { Effect, Layer, Option, Queue, Redacted } from 'effect'
import { FetchHttpClient } from 'effect/unstable/http'

import {
	Channels,
	ChannelsMemory,
	DeliveryAdmission,
	PreparedDeliveryInvocation,
	ProviderEventHandled,
	ProviderWebhookEvent,
	QueueDeliveryMode,
	makeDeliveryClient,
	type ChannelsProvider,
	type DeliveryContext,
} from '../src'

const basePath = '/api/channels'

/** Accepts any webhook as one event, named by its `x-event-id` header. Every batch hands itself off. */
const handingOffProvider = (contexts: Queue.Queue<DeliveryContext>): ChannelsProvider => ({
	providerName: 'example',
	deliveryMode: QueueDeliveryMode.make({}),
	webhookProvider: ({ namespace }) =>
		Effect.succeed({
			providerName: 'example',
			handle: ({ headers }) =>
				Effect.succeed(
					ProviderWebhookEvent.make({
						event: DeliveryAdmission.make({
							namespace,
							provider: 'example',
							installationId: 'installation',
							resourceId: 'resource',
							eventId: headers['x-event-id'] ?? 'event',
							payload: null,
						}),
					}),
				),
		}),
	eventProcessor: ({ namespace }) =>
		Effect.succeed({
			namespace,
			providerName: 'example',
			process: (_admissions, execution) =>
				Effect.gen(function* () {
					if (Option.isNone(execution.prepared)) {
						yield* execution.prepare(
							PreparedDeliveryInvocation.make({
								callback: 'onEvent',
								presentationVersion: 1,
								destination: { resource: 'resource' },
								supportedOperations: ['CreateMessage'],
							}),
						)
					}
					yield* Queue.offer(contexts, execution.context)
					yield* execution.context.handoff()
					return ProviderEventHandled.make({})
				}).pipe(Effect.orDie),
		}),
})

const startBot = Effect.gen(function* () {
	const contexts = yield* Queue.unbounded<DeliveryContext>()
	const bot = Channels.make({
		namespace: 'channels-test',
		basePath,
		providers: [handingOffProvider(contexts)],
		eventProcessing: { concurrency: 1, leaseMs: 30_000 },
		storage: ChannelsMemory.make({ polling: { intervalMs: 10 } }),
	})
	const started = yield* Effect.promise(() => bot.start(NodeCrypto.layer, bot.deliveryApi))
	yield* Effect.addFinalizer(() => Effect.promise(started.stop))
	const webhook = (eventId: string) =>
		Effect.promise(() =>
			started.handle(
				new Request(`http://localhost${basePath}/integrations/example/webhook`, {
					method: 'POST',
					headers: { 'x-event-id': eventId },
				}),
			),
		)
	/** The generated client, sending its requests straight to the bot's handler. */
	const client = yield* makeDeliveryClient({ baseUrl: 'http://localhost', basePath }).pipe(
		Effect.provide(
			FetchHttpClient.layer.pipe(
				Layer.provide(
					Layer.succeed(FetchHttpClient.Fetch, (input, init) => started.handle(new Request(input, init))),
				),
			),
		),
	)
	const raw = (path: string, init?: RequestInit) =>
		Effect.promise(() => started.handle(new Request(`http://localhost${basePath}${path}`, init)))
	return { contexts, webhook, client, raw }
})

describe('delivery API', () => {
	it.live('a remote worker finishes a handed-off delivery, and the next event waits until it does', ({ expect }) =>
		Effect.gen(function* () {
			const { contexts, webhook, client } = yield* startBot
			expect((yield* webhook('first')).status).toBe(200)
			const first = yield* Queue.take(contexts)
			expect(first.deliveryId.length).toBeGreaterThan(100)

			yield* webhook('second')
			yield* Effect.sleep('150 millis')
			expect(yield* Queue.size(contexts)).toBe(0)

			const delivery = { deliveryId: first.deliveryId, accessToken: first.accessToken }
			const waiting = yield* client.status(delivery)
			expect(waiting.stage).toBe('ExternalWaiting')
			expect(waiting.supportedOperations).toEqual(['CreateMessage'])
			expect(waiting.interruptRequested).toBe(false)

			expect((yield* client.complete({ ...delivery, payload: { markdown: 'done' } })).status).toBe('accepted')
			expect((yield* client.complete({ ...delivery, payload: { markdown: 'done' } })).status).toBe(
				'already_recorded',
			)
			expect((yield* client.fail(delivery).pipe(Effect.flip))._tag).toBe('DeliveryTerminalConflict')
			const retired = yield* client.status(delivery)
			expect(retired.stage).toBe('Retired')
			expect(retired.outcome?._tag).toBe('Completed')

			const second = yield* Queue.take(contexts)
			expect(second.deliveryId === first.deliveryId).toBe(false)
			expect(second.conversationId).toBe(first.conversationId)
		}),
	)

	it.live('answers 401 without a token and 404 for a wrong token or an unknown delivery', ({ expect }) =>
		Effect.gen(function* () {
			const { contexts, webhook, client, raw } = yield* startBot
			yield* webhook('first')
			const context = yield* Queue.take(contexts)
			const path = `/deliveries/${encodeURIComponent(context.deliveryId)}`

			expect((yield* raw(path)).status).toBe(401)
			expect((yield* raw(`${path}/complete`, { method: 'POST', body: '{}', headers: { 'content-type': 'application/json' } })).status).toBe(401)
			const wrongToken = { headers: { authorization: 'Bearer wrong-token' } }
			expect((yield* raw(path, wrongToken)).status).toBe(404)
			expect((yield* raw('/deliveries/not-a-delivery', wrongToken)).status).toBe(404)

			const wrong = { deliveryId: context.deliveryId, accessToken: Redacted.make('wrong-token') }
			expect((yield* client.complete(wrong).pipe(Effect.flip))._tag).toBe('DeliveryNotFound')
			const right = { deliveryId: context.deliveryId, accessToken: context.accessToken }
			expect((yield* client.status(right)).stage).toBe('ExternalWaiting')
		}),
	)
})
