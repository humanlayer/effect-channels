import * as NodeCrypto from '@effect/platform-node/NodeCrypto'
import { it } from '@effect/vitest'
import {
	type Channels,
	DeliveryAdmission,
	ProviderEventHandled,
	ProviderWebhookEvent,
	QueueDeliveryMode,
	type ChannelsProvider,
	type DeliveryAdmissionBatch,
} from '@humanlayer/channels-delivery-next'
import { Context, Effect, Layer, Ref } from 'effect'
import { HttpServerRequest, HttpServerResponse } from 'effect/unstable/http'

import { ChannelsCloudflare } from '../src'
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

/** A provider that accepts any webhook as one event and records the size of each batch it is handed. */
const makeExampleProvider = (batchSizes: Ref.Ref<ReadonlyArray<number>>): ChannelsProvider => ({
	providerName: 'example',
	deliveryMode: QueueDeliveryMode.make({}),
	webhookProvider: ({ namespace }) =>
		Effect.succeed({
			providerName: 'example',
			handle: () => Effect.succeed(ProviderWebhookEvent.make({ event: admission(namespace) })),
		}),
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

const makeOptions = (
	batchSizes: Ref.Ref<ReadonlyArray<number>>,
): Channels.Options<ReadonlyArray<{ readonly build: never; readonly process: never }>> => ({
	namespace: 'channels-cloudflare-test',
	basePath: '/api/channels',
	providers: [makeExampleProvider(batchSizes)],
	eventProcessing: { concurrency: 1, leaseMs: 30_000 },
})

it.effect('ChannelsCloudflare mailbox: deliver sets the alarm and the alarm runs the provider callback', ({ expect }) =>
	Effect.gen(function* () {
		const batchSizes = yield* Ref.make<ReadonlyArray<number>>([])
		const durableObject = yield* Layer.build(DurableObjectFake)
		const mailbox = yield* ChannelsCloudflare.makeMailbox(
			makeOptions(batchSizes),
			{ rearmAfterMs: 1_000 },
			Layer.succeedContext(durableObject),
		)
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
	'ChannelsCloudflare ingress: a webhook under the base path reaches the mailbox named by its key',
	({ expect }) =>
		Effect.gen(function* () {
			const delivered = yield* Ref.make<ReadonlyArray<string>>([])
			const bot = ChannelsCloudflare.make(makeOptions(yield* Ref.make<ReadonlyArray<number>>([])))
			const fetch = yield* bot
				.ingress({
					getByName: (mailboxKey) => ({
						deliver: () =>
							Ref.update(delivered, (keys) => [...keys, mailboxKey]).pipe(Effect.as({ accepted: true })),
					}),
				})
				.fetch.pipe(Effect.provide(NodeCrypto.layer))

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
