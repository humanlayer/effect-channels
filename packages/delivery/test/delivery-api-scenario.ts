/**
 * The delivery API over a polling store, end to end: `Channels.make` with the store serves a provider
 * webhook and `bot.deliveryApi`, polls the store on its own, and a remote worker finishes a handed-off
 * delivery through the generated client. Postgres and Redis run it in their backend suites.
 */
import * as NodeCrypto from '@effect/platform-node/NodeCrypto'
import { describe, it } from '@effect/vitest'
import { Effect, Layer, Queue, Schedule } from 'effect'
import { FetchHttpClient } from 'effect/http'

import {
	Channels,
	DeliveryAdmission,
	DeliveryOutputApplied,
	PreparedDeliveryInvocation,
	ProviderEventHandled,
	ProviderWebhookEvent,
	QueueDeliveryMode,
	makeDeliveryClient,
	type ChannelsProvider,
	type ChannelsStorage,
	type DeliveryClient,
	type DeliveryClientTarget,
	type DeliveryContext,
	type DeliveryControlBackend,
	type ProviderOutputAttempt,
} from '../src'

/**
 * The store under test.
 *
 * @property client - the store's client; each build is a new connection
 * @property empty - empty the store before the bot starts
 */
export type DeliveryApiStore<StorageError, StorageRequirements, ClientError, EmptyError> = {
	readonly namespace: string
	readonly storage: ChannelsStorage<StorageError, StorageRequirements, DeliveryControlBackend>
	readonly client: Layer.Layer<StorageRequirements, ClientError>
	readonly empty: Effect.Effect<void, EmptyError, StorageRequirements>
}

const basePath = '/api/channels'

/** Accepts any webhook as one event, named by its `x-event-id` header. Every batch hands itself off. */
const handingOffProvider = (
	contexts: Queue.Queue<DeliveryContext>,
	output: Queue.Queue<ProviderOutputAttempt>,
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
					yield* execution.prepare(
						PreparedDeliveryInvocation.make({
							callback: 'onEvent',
							presentationVersion: 1,
							destination: { resource: 'resource' },
							supportedOperations: ['PresentOutcome', 'AddExternalLink'],
						}),
					)
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
				Queue.offer(output, attempt).pipe(
					Effect.as(DeliveryOutputApplied.make({ receipt: { sent: attempt.operationId } })),
				),
		}),
})

const startBot = <StorageError, StorageRequirements, ClientError, EmptyError>(
	store: DeliveryApiStore<StorageError, StorageRequirements, ClientError, EmptyError>,
) =>
	Effect.gen(function* () {
		yield* store.empty.pipe(Effect.provide(Layer.fresh(store.client)))
		const contexts = yield* Queue.unbounded<DeliveryContext>()
		const output = yield* Queue.unbounded<ProviderOutputAttempt>()
		const bot = Channels.make({
			namespace: store.namespace,
			basePath,
			providers: [handingOffProvider(contexts, output)],
			eventProcessing: { concurrency: 2, leaseMs: 30_000 },
			storage: store.storage,
		})
		const started = yield* Effect.promise(() =>
			bot.start(Layer.merge(NodeCrypto.layer, Layer.fresh(store.client)), bot.deliveryApi),
		)
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
		const deliveryClient = yield* makeDeliveryClient({ baseUrl: 'http://localhost', basePath }).pipe(
			Effect.provide(
				FetchHttpClient.layer.pipe(
					Layer.provide(
						Layer.succeed(FetchHttpClient.Fetch, (input, init) => started.handle(new Request(input, init))),
					),
				),
			),
		)
		return { contexts, output, webhook, client: deliveryClient }
	})

/** Read the delivery's status until it retires. */
const awaitRetired = (deliveryClient: DeliveryClient, target: DeliveryClientTarget) =>
	deliveryClient
		.status(target)
		.pipe(Effect.repeat({ until: ({ stage }) => stage === 'Retired', schedule: Schedule.spaced('10 millis') }))

/**
 * Run the scenario against a store.
 *
 * @param name - the store's name in the suite title
 */
export const deliveryApiScenario = <StorageError, StorageRequirements, ClientError, EmptyError>(
	name: string,
	store: DeliveryApiStore<StorageError, StorageRequirements, ClientError, EmptyError>,
) =>
	describe(`${name} delivery API`, () => {
		it.live(
			'a remote worker finishes a handed-off delivery, and the event behind it runs once its output is sent',
			({ expect }) =>
				Effect.gen(function* () {
					const { contexts, output, webhook, client: deliveryClient } = yield* startBot(store)
					expect((yield* webhook('first')).status).toBe(200)
					const first = yield* Queue.take(contexts)
					yield* webhook('second')

					const delivery = { deliveryId: first.deliveryId, accessToken: first.accessToken }
					const waiting = yield* deliveryClient.status(delivery).pipe(
						Effect.repeat({
							until: ({ stage }) => stage === 'ExternalWaiting',
							schedule: Schedule.spaced('10 millis'),
						}),
					)
					expect(waiting.supportedOperations).toEqual(['PresentOutcome', 'AddExternalLink'])
					expect(yield* Queue.size(contexts)).toBe(0)

					const link = { label: 'Run', url: 'https://example.com/run/1' }
					expect((yield* deliveryClient.addLink({ ...delivery, link })).status).toBe('accepted')
					expect(
						(yield* deliveryClient.complete({ ...delivery, payload: { markdown: 'done' } })).status,
					).toBe('accepted')
					expect(
						(yield* deliveryClient.complete({ ...delivery, payload: { markdown: 'done' } })).status,
					).toBe('already_recorded')
					expect((yield* deliveryClient.fail(delivery).pipe(Effect.flip))._tag).toBe(
						'DeliveryTerminalConflict',
					)
					const sent = [yield* Queue.take(output), yield* Queue.take(output)]
					expect(sent.map(({ operation }) => operation._tag)).toEqual(['AddExternalLink', 'PresentOutcome'])

					const retired = yield* awaitRetired(deliveryClient, delivery)
					expect(retired.outcome?._tag).toBe('Completed')
					expect(retired.output.map(({ kind, state }) => `${kind}:${state}`)).toEqual([
						'AddExternalLink:Delivered',
						'PresentOutcome:Delivered',
					])
					const second = yield* Queue.take(contexts)
					expect(second.deliveryId === first.deliveryId).toBe(false)
					expect(second.conversationId).toBe(first.conversationId)
				}).pipe(Effect.scoped),
		)
	})
