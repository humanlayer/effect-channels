/**
 * The delivery API end to end: a signed-in-by-token remote worker finishes a handed-off delivery
 * through `Channels.make`'s own routes, with in-memory storage and a real router. The provider's
 * output processor records what it is asked to send.
 */
import * as NodeCrypto from '@effect/platform-node/NodeCrypto'
import { describe, it } from '@effect/vitest'
import { Effect, Layer, Option, Queue, Redacted, Ref, Schedule } from 'effect'
import { FetchHttpClient } from 'effect/unstable/http'

import {
	Channels,
	ChannelsMemory,
	DeliveryAdmission,
	DeliveryOutputApplied,
	DeliveryOutputFailed,
	PreparedDeliveryInvocation,
	ProviderEventHandled,
	ProviderWebhookEvent,
	QueueDeliveryMode,
	makeDeliveryClient,
	type ChannelsProvider,
	type DeliveryClient,
	type DeliveryClientTarget,
	type DeliveryContext,
	type ProviderOutputAttempt,
} from '../src'

const basePath = '/api/channels'

/**
 * Accepts any webhook as one event, named by its `x-event-id` header. Every batch hands itself off.
 * Its output processor records each attempt, and fails the first `failuresLeft` of them.
 */
const handingOffProvider = (
	contexts: Queue.Queue<DeliveryContext>,
	output: Queue.Queue<ProviderOutputAttempt>,
	failuresLeft: Ref.Ref<number>,
): ChannelsProvider => ({
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
	outputProcessor: ({ namespace }) =>
		Effect.succeed({
			namespace,
			providerName: 'example',
			process: (attempt) =>
				Effect.gen(function* () {
					yield* Queue.offer(output, attempt)
					const failing = yield* Ref.modify(failuresLeft, (left) => [left > 0, Math.max(0, left - 1)])
					if (failing) {
						return yield* new DeliveryOutputFailed({
							provider: 'example',
							retryable: true,
							safeCode: 'example_unavailable',
							retryAfterMs: 0,
						})
					}
					return DeliveryOutputApplied.make({ receipt: { sent: attempt.operationId } })
				}),
		}),
})

const startBot = Effect.gen(function* () {
	const contexts = yield* Queue.unbounded<DeliveryContext>()
	const output = yield* Queue.unbounded<ProviderOutputAttempt>()
	const outputFailuresLeft = yield* Ref.make(0)
	const bot = Channels.make({
		namespace: 'channels-test',
		basePath,
		providers: [handingOffProvider(contexts, output, outputFailuresLeft)],
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
	return { contexts, output, outputFailuresLeft, webhook, client, raw }
})

/** Read the delivery's status until it retires. */
const awaitRetired = (client: DeliveryClient, target: DeliveryClientTarget) =>
	client.status(target).pipe(
		Effect.repeat({ until: ({ stage }) => stage === 'Retired', schedule: Schedule.spaced('10 millis') }),
	)

describe('delivery API', () => {
	it.live('a remote worker finishes a handed-off delivery, and the next event waits until its output is sent', ({ expect }) =>
		Effect.gen(function* () {
			const { contexts, output, webhook, client } = yield* startBot
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
			const presented = yield* Queue.take(output)
			expect(presented.deliveryId).toBe(first.deliveryId)
			expect(presented.operation).toEqual({ _tag: 'PresentOutcome', outcome: { _tag: 'Completed' }, markdown: 'done' })
			expect(presented.prepared.destination).toEqual({ resource: 'resource' })
			const retired = yield* awaitRetired(client, delivery)
			expect(retired.outcome?._tag).toBe('Completed')
			expect(retired.output).toEqual([{ operationId: 'outcome', kind: 'PresentOutcome', state: 'Delivered', attempts: 1 }])

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

describe('delivery API output', () => {
	it.live('retries failed output on its own, without running the callback again', ({ expect }) =>
		Effect.gen(function* () {
			const { contexts, output, outputFailuresLeft, webhook, client } = yield* startBot
			yield* webhook('first')
			const context = yield* Queue.take(contexts)
			const delivery = { deliveryId: context.deliveryId, accessToken: context.accessToken }
			yield* Ref.set(outputFailuresLeft, 2)

			expect((yield* client.fail({ ...delivery, payload: { markdown: 'stopped' } })).status).toBe('accepted')
			const attempts = [yield* Queue.take(output), yield* Queue.take(output), yield* Queue.take(output)]
			expect(attempts.map(({ operationId, attempt }) => [operationId, attempt])).toEqual([
				['outcome', 1],
				['outcome', 2],
				['outcome', 3],
			])
			const retired = yield* awaitRetired(client, delivery)
			expect(retired.outcome?._tag).toBe('Failed')
			expect(retired.output).toEqual([{ operationId: 'outcome', kind: 'PresentOutcome', state: 'Delivered', attempts: 3 }])
			expect(yield* Queue.size(contexts)).toBe(0)
		}),
	)

	it.live('adds links: https only, a repeated URL is a replay, and each new one is sent once', ({ expect }) =>
		Effect.gen(function* () {
			const { contexts, output, webhook, client, raw } = yield* startBot
			yield* webhook('first')
			const context = yield* Queue.take(contexts)
			const delivery = { deliveryId: context.deliveryId, accessToken: context.accessToken }
			const pullRequest = { label: 'Pull request', url: 'https://github.com/org/repo/pull/1' }

			expect((yield* client.addLink({ ...delivery, link: pullRequest })).status).toBe('accepted')
			expect((yield* client.addLink({ ...delivery, link: { ...pullRequest, label: 'PR' } })).status).toBe(
				'already_recorded',
			)
			const sent = yield* Queue.take(output)
			expect(sent.operation).toEqual({ _tag: 'AddExternalLink', link: { _tag: 'ExternalLink', ...pullRequest } })

			const plainHttp = yield* raw(`/deliveries/${encodeURIComponent(context.deliveryId)}/links`, {
				method: 'POST',
				headers: {
					'content-type': 'application/json',
					authorization: `Bearer ${Redacted.value(context.accessToken)}`,
				},
				body: JSON.stringify({ label: 'Run', url: 'http://example.com/run' }),
			})
			expect(plainHttp.status).toBe(400)

			yield* client.complete(delivery)
			yield* awaitRetired(client, delivery)
			const late = { label: 'Late', url: 'https://example.com/late' }
			expect((yield* client.addLink({ ...delivery, link: late }).pipe(Effect.flip))._tag).toBe('DeliveryClosed')
			expect((yield* client.addLink({ ...delivery, link: pullRequest })).status).toBe('already_recorded')
		}),
	)
})
