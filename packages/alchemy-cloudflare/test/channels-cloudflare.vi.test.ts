import * as NodeCrypto from '@effect/platform-node/NodeCrypto'
import { it } from '@effect/vitest'
import {
	type Channels,
	DeliveryAdmission,
	DeliveryMutationReceipt,
	DeliveryNotFound,
	DeliveryOutcome,
	DeliveryOutputApplied,
	DeliveryStatus,
	PreparedDeliveryInvocation,
	ProviderEventHandled,
	deliveryMailboxKey,
	type ProviderDeliveryExecution,
	ProviderWebhookEvent,
	QueueDeliveryMode,
	type ChannelsProvider,
	type AddLinkPayload,
	type CompleteDeliveryPayload,
	type DeliveryAdmissionBatch,
	type DeliveryOutputOperation,
} from '@humanlayer/channels-delivery-next'
import { Context, Effect, Layer, Option, Predicate, Redacted, Ref, Schema } from 'effect'
import { HttpServerRequest, HttpServerResponse } from 'effect/unstable/http'

import { ChannelsCloudflare, DeliveryMailboxes } from '../src'
import { DurableObjectFake, DurableObjectFakeAlarm } from './DurableObjectFake'

const admission = (namespace: string) =>
	DeliveryAdmission.make({
		namespace,
		provider: 'example',
		installationId: 'installation',
		resourceId: 'thread-1',
		eventId: 'event-1',
		payload: { type: 'example' },
	})

/** The provider parts that accept any webhook as one event. */
const exampleWebhooks: Pick<ChannelsProvider, 'providerName' | 'deliveryMode' | 'webhookProvider'> = {
	providerName: 'example',
	deliveryMode: QueueDeliveryMode.make({}),
	webhookProvider: ({ namespace }) =>
		Effect.succeed({
			providerName: 'example',
			handle: () => Effect.succeed(ProviderWebhookEvent.make({ event: admission(namespace) })),
		}),
}

/** A provider that records the size of each batch it is handed. */
const makeExampleProvider = (batchSizes: Ref.Ref<ReadonlyArray<number>>): ChannelsProvider => ({
	...exampleWebhooks,
	eventProcessor: ({ namespace }) =>
		Effect.succeed({
			namespace,
			providerName: 'example',
			process: (admissions: DeliveryAdmissionBatch) =>
				Ref.update(batchSizes, (sizes) => [...sizes, admissions.length]).pipe(
					Effect.as(ProviderEventHandled.make({})),
				),
		}),
})

/** The delivery a callback handed off, as the remote worker it started would hold it. */
type HandedOff = { readonly deliveryId: string; readonly accessToken: Redacted.Redacted }

/**
 * A provider whose callback hands every batch off and records the delivery it handed off, and whose
 * output processor records each operation it sends.
 */
const makeHandOffProvider = (
	handedOff: Ref.Ref<Option.Option<HandedOff>>,
	sent: Ref.Ref<ReadonlyArray<DeliveryOutputOperation>>,
): ChannelsProvider => ({
	...exampleWebhooks,
	eventProcessor: ({ namespace }) =>
		Effect.succeed({
			namespace,
			providerName: 'example',
			process: (_admissions: DeliveryAdmissionBatch, execution: ProviderDeliveryExecution) =>
				Effect.gen(function* () {
					yield* execution.prepare(
						PreparedDeliveryInvocation.make({
							callback: 'onExample',
							presentationVersion: 1,
							destination: { thread: 'thread-1' },
							supportedOperations: [],
						}),
					)
					yield* execution.context.handoff()
					yield* Ref.set(
						handedOff,
						Option.some({ deliveryId: execution.deliveryId, accessToken: execution.context.accessToken }),
					)
					return ProviderEventHandled.make({})
				}).pipe(Effect.orDie),
		}),
	outputProcessor: ({ namespace }) =>
		Effect.succeed({
			namespace,
			providerName: 'example',
			process: ({ operation }) =>
				Ref.update(sent, (all) => [...all, operation]).pipe(Effect.as(DeliveryOutputApplied.make({}))),
		}),
})

/** Parse a delivery API response body as the schema the route answers with. */
const decodeBody = <S extends Schema.Decoder<unknown>>(schema: S, text: string) =>
	Schema.decodeEffect(Schema.fromJsonString(schema))(text)

const makeOptions = (
	provider: ChannelsProvider,
): Channels.Options<ReadonlyArray<{ readonly build: never; readonly process: never; readonly error: never }>> => ({
	namespace: 'channels-cloudflare-test',
	basePath: '/api/channels',
	providers: [provider],
	eventProcessing: { concurrency: 1, leaseMs: 30_000 },
})

it.effect('ChannelsCloudflare mailbox: deliver sets the alarm and the alarm runs the provider callback', ({ expect }) =>
	Effect.gen(function* () {
		const batchSizes = yield* Ref.make<ReadonlyArray<number>>([])
		const durableObject = yield* Layer.build(DurableObjectFake)
		const mailbox = yield* ChannelsCloudflare.makeMailbox(
			makeOptions(makeExampleProvider(batchSizes)),
			{ rearmAfterMs: 1_000 },
			Layer.succeedContext(durableObject),
		).pipe(Effect.provide(NodeCrypto.layer))
		const alarm = Context.get(durableObject, DurableObjectFakeAlarm)

		const receipt = yield* mailbox.deliver(admission('channels-cloudflare-test'))
		expect(receipt).toEqual({ accepted: true })
		expect(yield* alarm.scheduledAt).toEqual(0)

		yield* mailbox.alarm()

		expect(yield* Ref.get(batchSizes)).toEqual([1])
		expect(yield* alarm.scheduledAt).toEqual(null)
	}),
)

it.effect(
	'ChannelsCloudflare routes: a webhook under the base path reaches the mailbox named by its key',
	({ expect }) =>
		Effect.gen(function* () {
			const delivered = yield* Ref.make<ReadonlyArray<string>>([])
			const bot = ChannelsCloudflare.make(makeOptions(makeExampleProvider(yield* Ref.make<ReadonlyArray<number>>([]))))
			const mailboxes = DeliveryMailboxes.of({
				getByName: (mailboxKey) => ({
					deliver: () =>
						Ref.update(delivered, (keys) => [...keys, mailboxKey]).pipe(Effect.as({ accepted: true })),
					deliveryRequest: () => Effect.die(new Error('this test sends no delivery requests')),
				}),
			})
			const fetch = yield* ChannelsCloudflare.serve(bot.routes).pipe(
				Effect.provide(Layer.merge(NodeCrypto.layer, Layer.succeed(DeliveryMailboxes, mailboxes))),
			)

			const post = (path: string) =>
				fetch.pipe(
					Effect.provideService(
						HttpServerRequest.HttpServerRequest,
						HttpServerRequest.fromWeb(new Request(`http://localhost${path}`, { method: 'POST' })),
					),
					Effect.orElseSucceed(() => HttpServerResponse.empty({ status: 404 })),
				)

			expect((yield* post('/integrations/example/webhook')).status).toEqual(404)
			expect((yield* post('/api/channels/integrations/example/webhook')).status).toEqual(200)
			expect((yield* Ref.get(delivered)).length).toEqual(1)
		}),
)

it.effect(
	'ChannelsCloudflare delivery API: a remote worker reads and completes its delivery through the Worker and its mailbox object',
	({ expect }) =>
		Effect.gen(function* () {
			const handedOff = yield* Ref.make(Option.none<HandedOff>())
			const sent = yield* Ref.make<ReadonlyArray<DeliveryOutputOperation>>([])
			const options = makeOptions(makeHandOffProvider(handedOff, sent))
			const durableObject = yield* Layer.build(DurableObjectFake)
			const mailbox = yield* ChannelsCloudflare.makeMailbox(
				options,
				{ rearmAfterMs: 1_000 },
				Layer.succeedContext(durableObject),
			).pipe(Effect.provide(NodeCrypto.layer))
			const alarm = Context.get(durableObject, DurableObjectFakeAlarm)

			yield* mailbox.deliver(admission('channels-cloudflare-test'))
			yield* mailbox.alarm()
			const { deliveryId, accessToken } = Option.getOrThrow(yield* Ref.get(handedOff))
			expect(yield* alarm.scheduledAt).toEqual(null)

			const routedTo = yield* Ref.make<ReadonlyArray<string>>([])
			const mailboxes = DeliveryMailboxes.of({
				getByName: (mailboxKey) => ({
					deliver: mailbox.deliver,
					deliveryRequest: (request) =>
						Ref.update(routedTo, (keys) => [...keys, mailboxKey]).pipe(
							Effect.andThen(mailbox.deliveryRequest(request)),
						),
				}),
			})
			const bot = ChannelsCloudflare.make(options)
			const fetch = yield* ChannelsCloudflare.serve(Layer.merge(bot.routes, bot.deliveryApi)).pipe(
				Effect.provide(Layer.merge(NodeCrypto.layer, Layer.succeed(DeliveryMailboxes, mailboxes))),
			)

			const call = (input: {
				readonly path: string
				readonly token: string
				readonly payload?: CompleteDeliveryPayload | AddLinkPayload
			}) => {
				const url = `http://localhost/api/channels/deliveries/${deliveryId}${input.path}`
				const headers = { authorization: `Bearer ${input.token}`, 'content-type': 'application/json' }
				const request = Predicate.isUndefined(input.payload)
					? new Request(url, { headers })
					: new Request(url, { method: 'POST', headers, body: JSON.stringify(input.payload) })
				return fetch.pipe(
					Effect.provideService(HttpServerRequest.HttpServerRequest, HttpServerRequest.fromWeb(request)),
					Effect.flatMap((response) => {
						const web = HttpServerResponse.toWeb(response)
						return Effect.promise(() => web.text()).pipe(Effect.map((text) => ({ status: web.status, text })))
					}),
				)
			}
			const token = Redacted.value(accessToken)

			const waiting = yield* call({ path: '', token })
			expect(waiting.status).toEqual(200)
			expect(yield* decodeBody(DeliveryStatus, waiting.text)).toMatchObject({
				deliveryId,
				stage: 'ExternalWaiting',
				interruptRequested: false,
			})

			const link = { label: 'Run', url: 'https://example.com/run/1' }
			expect((yield* call({ path: '/links', token, payload: link })).status).toEqual(202)
			expect(yield* alarm.scheduledAt).toEqual(0)

			const completed = yield* call({ path: '/complete', token, payload: { markdown: 'done' } })
			expect(completed.status).toEqual(202)
			expect(yield* decodeBody(DeliveryMutationReceipt, completed.text)).toMatchObject({
				deliveryId,
				status: 'accepted',
			})
			expect(yield* Ref.get(sent)).toEqual([])
			const finishing = yield* decodeBody(DeliveryStatus, (yield* call({ path: '', token })).text)
			expect(finishing.stage).toEqual('Finishing')

			yield* mailbox.alarm()
			yield* mailbox.alarm()
			expect(yield* Ref.get(sent)).toEqual([
				{ _tag: 'AddExternalLink', link: { _tag: 'ExternalLink', ...link } },
				{ _tag: 'PresentOutcome', outcome: { _tag: 'Completed' }, markdown: 'done' },
			])
			expect(yield* alarm.scheduledAt).toEqual(null)

			const retired = yield* decodeBody(DeliveryStatus, (yield* call({ path: '', token })).text)
			expect(retired.stage).toEqual('Retired')
			expect(retired.outcome).toEqual(DeliveryOutcome.cases.Completed.make({}))
			expect(retired.output.map(({ state }) => state)).toEqual(['Delivered', 'Delivered'])

			const wrongToken = yield* call({ path: '', token: 'wrong-token' })
			expect(wrongToken.status).toEqual(404)
			expect(yield* decodeBody(DeliveryNotFound, wrongToken.text)).toBeInstanceOf(DeliveryNotFound)

			const mailboxKey = deliveryMailboxKey(admission('channels-cloudflare-test'))
			expect(yield* Ref.get(routedTo)).toEqual(Array.from({ length: 6 }, () => mailboxKey))
		}),
)
