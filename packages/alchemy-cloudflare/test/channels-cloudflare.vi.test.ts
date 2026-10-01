import * as NodeCrypto from '@effect/platform-node/NodeCrypto'
import { it } from '@effect/vitest'
import {
	type Channels,
	DeliveryAdmission,
	DeliveryMessageConflict,
	DeliveryMutationReceipt,
	DeliveryNotFound,
	DeliveryOutcome,
	DeliveryOutputApplied,
	DeliveryStatus,
	MessageId,
	PreparedDeliveryInvocation,
	ProviderEventHandled,
	type SetActivityPayload,
	deliveryMailboxKey,
	type ProviderDeliveryExecution,
	ProviderWebhookEvent,
	QueueDeliveryMode,
	type ChannelsProvider,
	type AddLinkPayload,
	type CompleteDeliveryPayload,
	type CreateMessagePayload,
	type DeliveryAdmissionBatch,
	type ProviderOutputOperation,
	type UpdateMessagePayload,
} from '@humanlayer/channels-delivery-next'
import { Clock, Context, Effect, Layer, Option, Predicate, Redacted, Ref, Schema } from 'effect'
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
	sent: Ref.Ref<ReadonlyArray<ProviderOutputOperation>>,
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
							supportedOperations: [
								'PresentOutcome',
								'AddExternalLink',
								'CreateMessage',
								'UpdateMessage',
								'DeleteMessage',
								'SetActivity',
							],
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
				Ref.update(sent, (all) => [...all, operation]).pipe(
					Effect.as(DeliveryOutputApplied.make({ receipt: { posted: operation._tag } })),
				),
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
			const sent = yield* Ref.make<ReadonlyArray<ProviderOutputOperation>>([])
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
			/** The waiting delivery's only due time is the default 24-hour handoff limit. */
			expect(yield* alarm.scheduledAt).toEqual(24 * 60 * 60 * 1_000)

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
				readonly method?: 'POST' | 'PUT' | 'PATCH' | 'DELETE'
				readonly payload?:
					| CompleteDeliveryPayload
					| AddLinkPayload
					| CreateMessagePayload
					| UpdateMessagePayload
					| typeof SetActivityPayload.Encoded
			}) => {
				const url = `http://localhost/api/channels/deliveries/${deliveryId}${input.path}`
				const headers = { authorization: `Bearer ${input.token}`, 'content-type': 'application/json' }
				const body = Predicate.isUndefined(input.payload) ? undefined : JSON.stringify(input.payload)
				const method = input.method ?? (Predicate.isUndefined(body) ? 'GET' : 'POST')
				const request = new Request(url, Predicate.isUndefined(body) ? { method, headers } : { method, headers, body })
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
			const activity = { activity: { _tag: 'Working', message: 'Running tests' } } as const
			expect((yield* call({ path: '/activity', token, method: 'PUT', payload: activity })).status).toEqual(202)
			const progress = MessageId.make('progress')
			const created = yield* call({ path: '/messages', token, payload: { messageId: progress, markdown: 'Working' } })
			expect(created.status).toEqual(202)
			const patch = { path: '/messages/progress', token, method: 'PATCH', payload: { markdown: 'Almost' } } as const
			expect((yield* call(patch)).status).toEqual(202)
			expect((yield* call({ path: '/messages/progress', token, method: 'DELETE' })).status).toEqual(202)
			const conflict = yield* call({ path: '/messages', token, payload: { messageId: progress, markdown: 'Other' } })
			expect(conflict.status).toEqual(409)
			expect(yield* decodeBody(DeliveryMessageConflict, conflict.text)).toBeInstanceOf(DeliveryMessageConflict)

			const completed = yield* call({ path: '/complete', token, payload: { markdown: 'done' } })
			expect(completed.status).toEqual(202)
			expect(yield* decodeBody(DeliveryMutationReceipt, completed.text)).toMatchObject({
				deliveryId,
				status: 'accepted',
			})
			expect(yield* Ref.get(sent)).toEqual([])
			const finishing = yield* decodeBody(DeliveryStatus, (yield* call({ path: '', token })).text)
			expect(finishing.stage).toEqual('Finishing')

			for (let pass = 0; pass < 6; pass++) yield* mailbox.alarm()
			const reference = { posted: 'CreateMessage' }
			expect(yield* Ref.get(sent)).toEqual([
				{ _tag: 'AddExternalLink', link: { _tag: 'ExternalLink', ...link } },
				{ _tag: 'SetActivity', ...activity },
				{ _tag: 'CreateMessage', messageId: progress, markdown: 'Working' },
				{ _tag: 'UpdateMessage', messageId: progress, markdown: 'Almost', reference },
				{ _tag: 'DeleteMessage', messageId: progress, reference },
				{ _tag: 'PresentOutcome', outcome: { _tag: 'Completed' }, markdown: 'done', clearActivity: true },
			])
			expect(yield* alarm.scheduledAt).toEqual(null)

			const retired = yield* decodeBody(DeliveryStatus, (yield* call({ path: '', token })).text)
			expect(retired.stage).toEqual('Retired')
			expect(retired.outcome).toEqual(DeliveryOutcome.cases.Completed.make({}))
			expect(retired.output.map(({ state }) => state)).toEqual(Array.from({ length: 6 }, () => 'Delivered'))

			const wrongToken = yield* call({ path: '', token: 'wrong-token' })
			expect(wrongToken.status).toEqual(404)
			expect(yield* decodeBody(DeliveryNotFound, wrongToken.text)).toBeInstanceOf(DeliveryNotFound)

			const mailboxKey = deliveryMailboxKey(admission('channels-cloudflare-test'))
			expect(yield* Ref.get(routedTo)).toEqual(Array.from({ length: 11 }, () => mailboxKey))
		}),
)

/**
 * A provider whose callback hands off every batch and records each delivery, and whose output
 * processor runs `duringCreate` while it sends a `CreateMessage`, as a remote worker's request can
 * arrive while the mailbox object waits on Slack.
 */
const makeInterleavingProvider = (
	handedOff: Ref.Ref<ReadonlyArray<HandedOff>>,
	sent: Ref.Ref<ReadonlyArray<string>>,
	duringCreate: Ref.Ref<Effect.Effect<void>>,
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
							supportedOperations: ['PresentOutcome', 'CreateMessage', 'SetActivity'],
						}),
					)
					yield* execution.context.handoff()
					yield* Ref.update(handedOff, (all) => [
						...all,
						{ deliveryId: execution.deliveryId, accessToken: execution.context.accessToken },
					])
					return ProviderEventHandled.make({})
				}).pipe(Effect.orDie),
		}),
	outputProcessor: ({ namespace }) =>
		Effect.succeed({
			namespace,
			providerName: 'example',
			process: ({ operation }) =>
				Effect.gen(function* () {
					if (Predicate.isTagged(operation, 'CreateMessage')) yield* Effect.flatten(Ref.getAndSet(duringCreate, Effect.void))
					yield* Ref.update(sent, (all) => [...all, operation._tag])
					return DeliveryOutputApplied.make({ receipt: { posted: operation._tag } })
				}),
		}),
})

const interleaving = (request: { readonly path: string; readonly method: 'POST' | 'PUT'; readonly body: unknown }) =>
	Effect.gen(function* () {
		const handedOff = yield* Ref.make<ReadonlyArray<HandedOff>>([])
		const sent = yield* Ref.make<ReadonlyArray<string>>([])
		const duringCreate = yield* Ref.make<Effect.Effect<void>>(Effect.void)
		const options = makeOptions(makeInterleavingProvider(handedOff, sent, duringCreate))
		const durableObject = yield* Layer.build(DurableObjectFake)
		const mailbox = yield* ChannelsCloudflare.makeMailbox(
			options,
			{ rearmAfterMs: 1_000 },
			Layer.succeedContext(durableObject),
		).pipe(Effect.provide(NodeCrypto.layer))
		const alarm = Context.get(durableObject, DurableObjectFakeAlarm)
		const mailboxes = DeliveryMailboxes.of({
			getByName: () => ({ deliver: mailbox.deliver, deliveryRequest: mailbox.deliveryRequest }),
		})
		const bot = ChannelsCloudflare.make(options)
		const fetch = yield* ChannelsCloudflare.serve(Layer.merge(bot.routes, bot.deliveryApi)).pipe(
			Effect.provide(Layer.merge(NodeCrypto.layer, Layer.succeed(DeliveryMailboxes, mailboxes))),
		)
		const call = (target: HandedOff, input: { readonly path: string; readonly method: string; readonly body?: unknown }) => {
			const url = `http://localhost/api/channels/deliveries/${target.deliveryId}${input.path}`
			const headers = {
				authorization: `Bearer ${Redacted.value(target.accessToken)}`,
				'content-type': 'application/json',
			}
			const init = Predicate.isUndefined(input.body)
				? { method: input.method, headers }
				: { method: input.method, headers, body: JSON.stringify(input.body) }
			return fetch.pipe(
				Effect.provideService(HttpServerRequest.HttpServerRequest, HttpServerRequest.fromWeb(new Request(url, init))),
				Effect.map((response) => HttpServerResponse.toWeb(response).status),
				Effect.orDie,
			)
		}

		yield* mailbox.deliver(admission('channels-cloudflare-test'))
		yield* mailbox.alarm()
		const [first] = yield* Ref.get(handedOff)
		if (first === undefined) return yield* Effect.die(new Error('the first delivery did not hand off'))
		const summary = { messageId: 'summary', markdown: 'Summary' }
		const created = yield* call(first, { path: '/messages', method: 'POST', body: summary })
		yield* mailbox.deliver(DeliveryAdmission.make({ ...admission('channels-cloudflare-test'), eventId: 'event-2' }))
		const statuses = yield* Ref.make<ReadonlyArray<number>>([])
		yield* Ref.set(
			duringCreate,
			call(first, request).pipe(
				Effect.flatMap((code) => Ref.update(statuses, (all) => [...all, code])),
				Effect.scoped,
			),
		)

		yield* alarm.clearAsCloudflareDoesBeforeTheHandler
		yield* mailbox.alarm()
		return { first, created, handedOff, sent, statuses, alarm, call }
	})

it.effect(
	'ChannelsCloudflare delivery API: a result accepted while an earlier output is being sent runs in the same alarm, retires the delivery, and runs the next event',
	({ expect }) =>
		Effect.gen(function* () {
			const { first, created, handedOff, sent, statuses, alarm, call } = yield* interleaving({
				path: '/complete',
				method: 'POST',
				body: { markdown: 'done' },
			})
			expect([created, ...(yield* Ref.get(statuses))]).toEqual([202, 202])
			expect(yield* Ref.get(sent)).toEqual(['CreateMessage', 'PresentOutcome'])
			const deliveries = yield* Ref.get(handedOff)
			expect(deliveries).toHaveLength(2)
			expect(deliveries[1]?.deliveryId === first.deliveryId).toBe(false)
			expect(yield* call(first, { path: '', method: 'GET' })).toEqual(200)
			const left = yield* alarm.scheduledAt
			expect(left === null || left > (yield* Clock.currentTimeMillis)).toBe(true)
		}),
)

it.effect(
	'ChannelsCloudflare delivery API: activity accepted while an earlier output is being sent runs in the same alarm',
	({ expect }) =>
		Effect.gen(function* () {
			const { created, statuses, sent, handedOff } = yield* interleaving({
				path: '/activity',
				method: 'PUT',
				body: { activity: { _tag: 'Working', message: 'Running tests' } },
			})
			expect([created, ...(yield* Ref.get(statuses))]).toEqual([202, 202])
			expect(yield* Ref.get(sent)).toEqual(['CreateMessage', 'SetActivity'])
			expect(yield* Ref.get(handedOff)).toHaveLength(1)
		}),
)
